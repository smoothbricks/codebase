/**
 * OpGroup Creation - Class-based architecture for Phase 2
 *
 * Creates OpGroups as class instances with ops as own properties.
 * Enables V8 hidden class optimization and cleaner API (`.opName` not `.ops.opName`).
 *
 * Why class-based:
 * - Ops as own properties enable direct access and predictable object shape
 * - prefix()/mapColumns() create new instances with same SpanBufferClass (shared stats)
 * - Only immutable remap metadata differs between original and mapped op instances
 *
 * Internal properties (_logSchema, _flags, etc.) are prefixed with underscore to avoid
 * collision with user-defined op names. Public interfaces hide these from intellisense.
 *
 * @module opContext/createOpGroup
 */

import {
  type ColumnMapping,
  defineFields,
  type Field,
  type FieldSchema,
  type LocalColumns,
  type MappedColumns,
  type MappedFields,
  type PrefixedColumns,
} from '@smoothbricks/arrow-builder';
import { createRemapDescriptor } from '../library.js';
import type { RemapDescriptor } from '../logBinding.js';
import { type AnyOp, Op } from '../op.js';
import type { LogSchema } from '../schema/LogSchema.js';
import { SYSTEM_SCHEMA_FIELD_NAMES } from '../schema/systemSchema.js';
import type { SchemaFields } from '../schema/types.js';
import type {
  MappedOpGroup,
  MappedOpGroupInternal,
  OpGroup,
  OpGroupInternal,
  OpGroupOps,
  SchemaFieldsOf,
} from './opGroupTypes.js';
import { validateOpName } from './opGroupTypes.js';
import type { OpContext } from './types.js';

//#region smoo/lmao!n/column-mapping-api
// 01l "Dependencies with Column Mapping" + 01e "Column Mapping API": .prefix(name) and
// .mapColumns({ field: 'new' | 'shared' | null }) are arrow-builder's column composition algebra over the group's
// own fields. Each call clones the OpGroup's ops with one immutable descriptor rendered from the mapping; child
// buffers remain canonical and Arrow interprets the descriptor only on its cold traversal path.
// =============================================================================
// INTERNAL HELPERS
// =============================================================================

function assertFieldsOf<F extends SchemaFields>(fields: Record<string, Field>, schema: LogSchema): asserts fields is F {
  for (const name of Object.keys(fields)) {
    if (!(name in schema.fields)) throw new TypeError(`field '${name}' is not a field of the group's log schema`);
  }
}

/**
 * A group's own fields as its library's field schema. System columns are not a library's: every application has
 * its own, so they take no part in a column mapping.
 */
function libraryFields<Ctx extends OpContext>(
  logSchema: Ctx['logSchema'],
): FieldSchema<string, SchemaFieldsOf<Ctx['logSchema']>> {
  const own: Record<string, Field> = {};
  for (const [name, field] of logSchema._columns) {
    if (!SYSTEM_SCHEMA_FIELD_NAMES.has(name)) own[name] = field;
  }
  assertFieldsOf<SchemaFieldsOf<Ctx['logSchema']>>(own, logSchema);
  return defineFields(logSchema.library, own);
}

/**
 * Render a column mapping as the Arrow output-to-canonical-source lookup of the library's span buffers.
 *
 * A span buffer reads one source column per output column, so two fields of one library cannot be one output.
 *
 * @throws Error naming both fields when a mapping gives two of the library's fields one column.
 */
function isColumns(value: unknown): value is Readonly<Record<string, string | null>> {
  return (
    typeof value === 'object' &&
    value !== null &&
    Object.values(value).every((column) => column === null || typeof column === 'string')
  );
}

function outputToSource(library: string, columns: unknown): Record<string, string> {
  if (!isColumns(columns)) throw new TypeError(`library '${library}': a column mapping holds column names or null`);
  const sources: Record<string, string> = {};
  for (const [field, column] of Object.entries(columns)) {
    if (column === null) continue;
    const earlier = sources[column];
    if (earlier !== undefined) {
      throw new Error(
        `Schema conflict: fields '${earlier}' and '${field}' of library '${library}' are both mapped to column '${column}'. ` +
          'A span row has one value per column: give each field its own column.',
      );
    }
    sources[column] = field;
  }
  return sources;
}

function isOpInstance<Ctx extends OpContext>(value: unknown): value is AnyOp<Ctx> {
  return value instanceof Op;
}

function copyOpsWithDescriptor<Ctx extends OpContext>(
  source: OpGroupOps<Ctx>,
  target: Record<string, unknown>,
  remapDescriptor: RemapDescriptor,
) {
  for (const [key, value] of Object.entries(source)) {
    if (isOpInstance<Ctx>(value)) {
      target[key] = new Op(
        value.metadata,
        value.callsitePlan.SpanBufferClass,
        value.fn,
        remapDescriptor,
        value._opContextBinding,
        value.callsitePlan.runtimeHint,
        value.callsitePlan.eagerColumns.names,
        value.callsitePlan.localMessageDictionary,
      );
    }
  }
}

function assertGroupCarriesOps<Ctx extends OpContext, Ops extends OpGroupOps<Ctx>>(
  group: Record<string, unknown>,
  ops: Ops,
): asserts group is Record<string, unknown> & Ops {
  for (const name of Object.keys(ops)) {
    if (!isOpInstance<Ctx>(group[name])) {
      throw new TypeError(`Expected OpGroup to expose op '${name}' as an Op instance`);
    }
  }
}

function cloneOpsWithDescriptor<Ctx extends OpContext, Ops extends OpGroupOps<Ctx>>(
  source: Ops,
  remapDescriptor: RemapDescriptor,
): Ops {
  const cloned: Record<string, unknown> = {};
  copyOpsWithDescriptor<Ctx>(source, cloned, remapDescriptor);
  assertGroupCarriesOps<Ctx, Ops>(cloned, source);
  return cloned;
}

/**
 * Validate all op names in a record.
 * Throws if any name starts with underscore or is a reserved name.
 */
function validateAllOpNames(ops: Record<string, unknown>): void {
  for (const name of Object.keys(ops)) {
    validateOpName(name);
  }
}

/** The group of `ops` wired with `mapped`: the same ops, each cloned with the mapping's remap descriptor. */
function mappedGroup<Ctx extends OpContext, Columns, Ops extends OpGroupOps<Ctx>>(
  logSchema: Ctx['logSchema'],
  flags: Ctx['flags'],
  ops: Ops,
  mapped: MappedFields<string, SchemaFieldsOf<Ctx['logSchema']>, Columns>,
): MappedOpGroup<Ctx, Columns, Ops> {
  const remapDescriptor = createRemapDescriptor(logSchema, outputToSource(mapped.schema.library, mapped.columns));
  const group = new MappedOpGroupImpl<Ctx, Columns, Ops>(
    logSchema,
    flags,
    cloneOpsWithDescriptor<Ctx, Ops>(ops, remapDescriptor),
    mapped,
  );
  assertGroupCarriesOps<Ctx, Ops>(group, ops);
  return group;
}

// =============================================================================
// OP GROUP CLASS - Phase 2 Architecture
// =============================================================================

/**
 * OpGroup class - Holds ops as own properties.
 *
 * Why class-based:
 * 1. Private fields don't pollute object shape (no hidden class impact)
 * 2. Ops spread as own properties for direct access (`.request` not `.ops.request`)
 * 3. Prototype methods (prefix/mapColumns) shared across instances
 * 4. Same SpanBufferClass preserved across prefix() calls (shared schema + stats)
 * 5. Only immutable remap metadata differs between original and mapped ops
 *
 * Internal properties use underscore prefix (_logSchema, _flags) to avoid
 * collision with user-defined op names.
 */
class OpGroupImpl<Ctx extends OpContext, Ops extends OpGroupOps<Ctx>> implements OpGroupInternal<Ctx, Ops> {
  // Allow dynamic op properties (ops spread as own properties in constructor)
  [key: string]: unknown;

  readonly #ops: Ops;
  readonly _columns: FieldSchema<string, SchemaFieldsOf<Ctx['logSchema']>>;

  constructor(
    readonly _logSchema: Ctx['logSchema'],
    readonly _flags: Ctx['flags'],
    ops: Ops,
  ) {
    this.#ops = ops;
    this._columns = libraryFields<Ctx>(_logSchema);

    // Validate op names before spreading
    validateAllOpNames(ops);

    // Spread ops onto this instance as own properties
    // Why spread: Enables direct access (ctx.deps.http.request)
    for (const [name, op] of Object.entries(ops)) {
      this[name] = op;
    }
  }

  prefix<P extends string>(
    p: P,
  ): MappedOpGroup<Ctx, PrefixedColumns<LocalColumns<SchemaFieldsOf<Ctx['logSchema']>>, P>, Ops> {
    return mappedGroup<Ctx, PrefixedColumns<LocalColumns<SchemaFieldsOf<Ctx['logSchema']>>, P>, Ops>(
      this._logSchema,
      this._flags,
      this.#ops,
      this._columns.prefix(p),
    );
  }

  mapColumns<const M extends ColumnMapping<SchemaFieldsOf<Ctx['logSchema']>>>(
    mapping: M,
  ): MappedOpGroup<Ctx, MappedColumns<LocalColumns<SchemaFieldsOf<Ctx['logSchema']>>, M>, Ops> {
    return mappedGroup<Ctx, MappedColumns<LocalColumns<SchemaFieldsOf<Ctx['logSchema']>>, M>, Ops>(
      this._logSchema,
      this._flags,
      this.#ops,
      this._columns.mapColumns(mapping),
    );
  }
}

// =============================================================================
// MAPPED OP GROUP CLASS
// =============================================================================

/**
 * MappedOpGroup class - OpGroup with column mapping applied.
 *
 * Why separate class: TypeScript needs distinct type for mapped groups
 * to track the Columns type parameter.
 *
 * Internal properties use underscore prefix to avoid collision with op names.
 */
class MappedOpGroupImpl<Ctx extends OpContext, Columns, Ops extends OpGroupOps<Ctx>>
  implements MappedOpGroupInternal<Ctx, Columns, Ops>
{
  // Allow dynamic op properties (ops spread as own properties in constructor)
  [key: string]: unknown;

  readonly #ops: Ops;

  constructor(
    readonly _logSchema: Ctx['logSchema'],
    readonly _flags: Ctx['flags'],
    ops: Ops,
    readonly _columns: MappedFields<string, SchemaFieldsOf<Ctx['logSchema']>, Columns>,
  ) {
    this.#ops = ops;

    // Validate op names before spreading
    validateAllOpNames(ops);

    // Spread ops onto this instance
    for (const [name, op] of Object.entries(ops)) {
      this[name] = op;
    }
  }

  prefix<P extends string>(p: P): MappedOpGroup<Ctx, PrefixedColumns<Columns, P>, Ops> {
    return mappedGroup<Ctx, PrefixedColumns<Columns, P>, Ops>(
      this._logSchema,
      this._flags,
      this.#ops,
      this._columns.prefix(p),
    );
  }

  mapColumns<const M extends ColumnMapping<SchemaFieldsOf<Ctx['logSchema']>>>(
    mapping: M,
  ): MappedOpGroup<Ctx, MappedColumns<Columns, M>, Ops> {
    return mappedGroup<Ctx, MappedColumns<Columns, M>, Ops>(
      this._logSchema,
      this._flags,
      this.#ops,
      this._columns.mapColumns(mapping),
    );
  }
}

// =============================================================================
// OP GROUP CREATION
// =============================================================================

/**
 * Create an OpGroup from a schema, flags, and ops
 *
 * Why class-based: V8 hidden class optimization - all OpGroups with same ops
 * have same object shape (private fields don't affect hidden class).
 *
 * The ops passed to this function already have a SpanBufferClass attached.
 * All ops in the same context share the SAME SpanBufferClass instance, which carries:
 * - Static schema property (shared by all instances)
 * - Static stats property (shared mutable stats for self-tuning)
 *
 * The returned OpGroup can be:
 * - Used directly as a dependency (ops accessible via ctx.deps.name.opName)
 * - Prefixed with `.prefix('http')` to namespace all columns
 * - Mapped with `.mapColumns({ query: 'query', _internal: null })` for fine control
 *
 * @param logSchema - The LogSchema defining columns for this group's ops
 * @param flags - Feature flag schema
 * @param ops - Record of Op instances belonging to this group
 * @returns OpGroup with prefix() and mapColumns() methods, ops as own properties
 *
 * @example
 * ```typescript
 * const httpOps = createOpGroup(httpLogSchema, httpFlags, {
 *   fetch: fetchOp,
 *   post: postOp,
 * });
 *
 * // Direct access (Phase 2):
 * await ctx.deps.http.fetch(url);
 *
 * // Wire with prefix:
 * deps: { http: httpOps.prefix('http') }
 * // Columns become: http_status, http_url, http_duration, etc.
 * ```
 */
export function createOpGroup<Ctx extends OpContext, Ops extends OpGroupOps<Ctx>>(
  logSchema: Ctx['logSchema'],
  flags: Ctx['flags'],
  ops: Ops,
): OpGroup<Ctx, Ops> {
  const group = new OpGroupImpl<Ctx, Ops>(logSchema, flags, ops);
  assertGroupCarriesOps<Ctx, Ops>(group, ops);
  return group;
}
//#endregion smoo/lmao!n/column-mapping-api
