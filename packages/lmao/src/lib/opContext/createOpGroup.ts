/**
 * OpGroup Creation - Class-based architecture for Phase 2
 *
 * Creates OpGroups as class instances with ops as own properties.
 * Enables V8 hidden class optimization and cleaner API (`.opName` not `.ops.opName`).
 *
 * Why class-based:
 * - Private fields (#columnMapping) don't affect hidden class (truly hidden, no enumeration)
 * - Ops as own properties enable direct access and predictable object shape
 * - prefix()/mapColumns() create new instances with same SpanBufferClass (shared stats)
 * - Only remappedViewClass differs between original and prefixed op instances
 *
 * Internal properties (_logSchema, _flags, etc.) are prefixed with underscore to avoid
 * collision with user-defined op names. Public interfaces hide these from intellisense.
 *
 * @module opContext/createOpGroup
 */

import { generateRemappedBufferViewClass } from '../library.js';
import { Op } from '../op.js';
import { SYSTEM_SCHEMA_FIELD_NAMES } from '../schema/systemSchema.js';
import type { SchemaFields } from '../schema/types.js';
import type {
  ColumnMapping,
  MappedOpGroup,
  MappedOpGroupInternal,
  MappedSchema,
  OpGroup,
  OpGroupInternal,
  OpGroupOps,
  PrefixedSchema,
  SchemaFieldsOf,
} from './opGroupTypes.js';
import { validateOpName } from './opGroupTypes.js';
import type { OpContext } from './types.js';

// =============================================================================
// INTERNAL HELPERS
// =============================================================================

function getSchemaFields<Schema extends { readonly fields: SchemaFields }>(fieldsSchema: Schema): Schema['fields'] {
  return fieldsSchema.fields;
}

/**
 * Create reverse mapping for RemappedBufferView.
 * Input: clean -> prefixed mapping (e.g., { status: 'http_status' })
 * Output: prefixed -> clean mapping (e.g., { http_status: 'status' })
 *
 * Why reverse: RemappedBufferView wraps child buffer at registration time.
 * Arrow conversion asks for 'http_status', view needs to find 'status' in raw buffer.
 */
function createReverseMapping(mapping: Record<string, string | null | undefined>): Record<string, string> {
  const reverse: Record<string, string> = {};
  for (const [cleanName, prefixedName] of Object.entries(mapping)) {
    if (typeof prefixedName === 'string') {
      reverse[prefixedName] = cleanName;
    }
  }
  return reverse;
}

function isOpInstance<Ctx extends OpContext>(value: unknown): value is Op<Ctx, unknown[], unknown, unknown> {
  return value instanceof Op;
}

function copyOpsWithView<Ctx extends OpContext>(
  source: OpGroupOps<Ctx>,
  target: Record<string, unknown>,
  remappedViewClass: ReturnType<typeof generateRemappedBufferViewClass>,
) {
  for (const [key, value] of Object.entries(source)) {
    if (isOpInstance<Ctx>(value)) {
      target[key] = new Op(value.metadata, value.SpanBufferClass, value.fn, remappedViewClass, value._opContextBinding);
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

function cloneOpsWithView<Ctx extends OpContext, Ops extends OpGroupOps<Ctx>>(
  source: Ops,
  remappedViewClass: ReturnType<typeof generateRemappedBufferViewClass>,
): Ops {
  const cloned: Record<string, unknown> = {};
  copyOpsWithView<Ctx>(source, cloned, remappedViewClass);
  assertGroupCarriesOps<Ctx, Ops>(cloned, source);
  return cloned;
}

function prefixContributedSchema<T extends SchemaFields, P extends string>(fields: T, prefix: P): PrefixedSchema<T, P> {
  const result: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(fields)) {
    result[`${prefix}_${key}`] = value;
  }
  assertMappedSchema<PrefixedSchema<T, P>>(result);
  return result;
}

function assertMappedSchema<T extends SchemaFields>(value: Record<string, unknown>): asserts value is T {
  for (const schema of Object.values(value)) {
    if (typeof schema !== 'object' || schema === null) {
      throw new TypeError('Mapped schema must contain schema metadata objects');
    }
  }
}

/**
 * Build a prefix mapping for all schema fields
 *
 * Why prefix all fields: Ensures no column name collisions when wiring multiple libraries.
 * Each library's columns become `${prefix}_${field}` in app schema.
 *
 * @param fieldNames - The field names from the schema
 * @param prefix - The prefix to apply
 * @returns Column mapping where each field becomes `${prefix}_${field}`
 */
function buildPrefixMapping<T extends SchemaFields, P extends string>(
  fieldNames: readonly string[],
  prefix: P,
): ColumnMapping<T> {
  const mapping: Record<string, string> = {};
  for (const name of fieldNames) {
    // Skip system columns - they should NOT be prefixed
    if (SYSTEM_SCHEMA_FIELD_NAMES.has(name)) continue;
    mapping[name] = `${prefix}_${name}`;
  }
  return mapping as ColumnMapping<T>;
}

/**
 * Apply a prefix to an existing mapping
 *
 * For each entry in the mapping:
 * - If mapped to a string, prefix that string: 'status' -> 'http_status'
 * - If mapped to null (dropped), keep as null
 *
 * Why: Enables chaining `.prefix('http').prefix('api')` to build composite prefixes.
 *
 * @param mapping - Existing column mapping
 * @param prefix - Prefix to apply
 * @returns New mapping with prefixed target names
 */
function prefixMapping<T extends SchemaFields, P extends string>(
  mapping: ColumnMapping<T>,
  prefix: P,
): ColumnMapping<T> {
  const result: Record<string, string | null> = {};
  for (const [key, value] of Object.entries(mapping)) {
    if (value === null) {
      result[key] = null;
    } else if (typeof value === 'string') {
      result[key] = `${prefix}_${value}`;
    }
  }
  return result as ColumnMapping<T>;
}

/**
 * Build the contributed schema object from the original schema and mapping
 *
 * The contributed schema reflects what columns this group adds to the app's schema
 * after mapping is applied.
 *
 * Why track contributed schema: Type system needs to know what columns are available
 * after prefix/mapping transformations for compile-time safety.
 *
 * @param fields - Original schema fields
 * @param mapping - Column mapping
 * @returns The contributed schema with renamed keys
 */
function buildContributedSchema<T extends SchemaFields, M extends ColumnMapping<T>>(
  fields: T,
  mapping: M,
): MappedSchema<T, M> {
  const result: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(fields)) {
    const mappedName = mapping[key as keyof M];
    if (mappedName === null) {
      // Dropped column - not included in contributed schema
    } else if (typeof mappedName === 'string') {
      // Explicitly mapped - use the mapped name
      result[mappedName] = value;
    } else {
      // Not in mapping - keep original name
      result[key] = value;
    }
  }
  assertMappedSchema<MappedSchema<T, M>>(result);
  return result;
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

// =============================================================================
// OP GROUP CLASS - Phase 2 Architecture
// =============================================================================

/**
 * OpGroup class - Holds ops as own properties with private field for mapping state.
 *
 * Why class-based:
 * 1. Private fields don't pollute object shape (no hidden class impact)
 * 2. Ops spread as own properties for direct access (`.request` not `.ops.request`)
 * 3. Prototype methods (prefix/mapColumns) shared across instances
 * 4. Same SpanBufferClass preserved across prefix() calls (shared schema + stats)
 * 5. Only remappedViewClass differs between original and prefixed ops
 *
 * Structure enables V8 optimization:
 * - All OpGroup instances with same ops have same hidden class
 * - Private fields invisible to property enumeration
 * - Method dispatch via prototype (no per-instance copies)
 *
 * Internal properties use underscore prefix (_logSchema, _flags) to avoid
 * collision with user-defined op names.
 */
class OpGroupImpl<Ctx extends OpContext, Ops extends OpGroupOps<Ctx>> implements OpGroupInternal<Ctx, Ops> {
  // Allow dynamic op properties (ops spread as own properties in constructor)
  [key: string]: unknown;

  // Private field - truly hidden, doesn't affect hidden class or enumeration
  readonly #columnMapping: ColumnMapping<SchemaFieldsOf<Ctx['logSchema']>>;
  readonly #ops: Ops;

  constructor(
    readonly _logSchema: Ctx['logSchema'],
    readonly _flags: Ctx['flags'],
    ops: Ops,
    columnMapping?: ColumnMapping<SchemaFieldsOf<Ctx['logSchema']>>,
  ) {
    this.#columnMapping = columnMapping ?? {};
    this.#ops = ops;

    // Validate op names before spreading
    validateAllOpNames(ops);

    // Spread ops onto this instance as own properties
    // Why spread: Enables direct access (ctx.deps.http.request)
    for (const [name, op] of Object.entries(ops)) {
      this[name] = op;
    }
  }

  prefix<P extends string>(p: P): MappedOpGroup<Ctx, PrefixedSchema<Ctx['logSchema']['fields'], P>, Ops> {
    // Apply prefix to the current mapping
    // Check if mapping is non-empty (empty object {} is truthy but has no entries)
    const hasMapping = this.#columnMapping && Object.keys(this.#columnMapping).length > 0;
    const newMapping = hasMapping
      ? prefixMapping(this.#columnMapping, p)
      : buildPrefixMapping(this._logSchema._columnNames, p);

    // Generate RemappedBufferView for this prefix
    // Why reverse mapping: Arrow conversion asks for 'http_status', needs to find 'status'
    const reverseMapping = createReverseMapping(newMapping);
    const remappedViewClass = generateRemappedBufferViewClass(reverseMapping);

    // Create new OpGroup with prefixed ops
    const newGroup = new MappedOpGroupImpl<Ctx, PrefixedSchema<Ctx['logSchema']['fields'], P>, Ops>(
      this._logSchema,
      this._flags,
      cloneOpsWithView<Ctx, Ops>(this.#ops, remappedViewClass),
      newMapping,
      prefixContributedSchema<Ctx['logSchema']['fields'], P>(getSchemaFields(this._logSchema), p),
    );

    assertGroupCarriesOps<Ctx, Ops>(newGroup, this.#ops);
    return newGroup;
  }

  mapColumns<M extends ColumnMapping<Ctx['logSchema']['fields']>>(
    mapping: M,
  ): MappedOpGroup<Ctx, MappedSchema<Ctx['logSchema']['fields'], M>, Ops> {
    // Override/extend the existing mapping with new mapping
    const newMapping = { ...this.#columnMapping, ...mapping };

    // Generate RemappedBufferView for this mapping
    const reverseMapping = createReverseMapping(newMapping);
    const remappedViewClass = generateRemappedBufferViewClass(reverseMapping);

    // Build contributed schema from original fields with new mapping
    const newContributed = buildContributedSchema<Ctx['logSchema']['fields'], M>(
      getSchemaFields(this._logSchema),
      newMapping,
    );

    // Create new OpGroup with mapped ops
    const newGroup = new MappedOpGroupImpl<Ctx, MappedSchema<Ctx['logSchema']['fields'], M>, Ops>(
      this._logSchema,
      this._flags,
      cloneOpsWithView<Ctx, Ops>(this.#ops, remappedViewClass),
      newMapping,
      newContributed,
    );

    assertGroupCarriesOps<Ctx, Ops>(newGroup, this.#ops);
    return newGroup;
  }
}

// =============================================================================
// MAPPED OP GROUP CLASS
// =============================================================================

/**
 * MappedOpGroup class - OpGroup with column mapping applied.
 *
 * Why separate class: TypeScript needs distinct type for mapped groups
 * to track contributedSchema type parameter.
 *
 * Internal properties use underscore prefix to avoid collision with op names.
 */
class MappedOpGroupImpl<Ctx extends OpContext, ContributedSchema extends SchemaFields, Ops extends OpGroupOps<Ctx>>
  implements MappedOpGroupInternal<Ctx, ContributedSchema, Ops>
{
  // Allow dynamic op properties (ops spread as own properties in constructor)
  [key: string]: unknown;

  readonly #columnMapping: Record<string, string | null | undefined>;
  readonly #ops: Ops;

  constructor(
    readonly _logSchema: Ctx['logSchema'],
    readonly _flags: Ctx['flags'],
    ops: Ops,
    readonly _columnMapping: ColumnMapping<SchemaFieldsOf<Ctx['logSchema']>>,
    readonly _contributedSchema: ContributedSchema,
  ) {
    this.#columnMapping = _columnMapping;
    this.#ops = ops;

    // Validate op names before spreading
    validateAllOpNames(ops);

    // Spread ops onto this instance
    for (const [name, op] of Object.entries(ops)) {
      this[name] = op;
    }
  }

  prefix<P extends string>(p: P): MappedOpGroup<Ctx, PrefixedSchema<ContributedSchema, P>, Ops> {
    // Apply prefix to the current mapping's target names
    const newMapping = prefixMapping(this.#columnMapping, p);

    // Generate RemappedBufferView
    const reverseMapping = createReverseMapping(newMapping);
    const remappedViewClass = generateRemappedBufferViewClass(reverseMapping);

    // Build new contributed schema by prefixing current contributed schema
    const newContributed = prefixContributedSchema(this._contributedSchema, p);

    // Create new MappedOpGroup
    const newGroup = new MappedOpGroupImpl<Ctx, PrefixedSchema<ContributedSchema, P>, Ops>(
      this._logSchema,
      this._flags,
      cloneOpsWithView<Ctx, Ops>(this.#ops, remappedViewClass),
      newMapping,
      newContributed,
    );

    assertGroupCarriesOps<Ctx, Ops>(newGroup, this.#ops);
    return newGroup;
  }

  mapColumns<M extends ColumnMapping<ContributedSchema>>(
    mapping: M,
  ): MappedOpGroup<Ctx, MappedSchema<ContributedSchema, M>, Ops> {
    // Override/extend the existing mapping
    const newMapping = { ...this.#columnMapping, ...mapping };

    // Generate RemappedBufferView
    const reverseMapping = createReverseMapping(newMapping);
    const remappedViewClass = generateRemappedBufferViewClass(reverseMapping);

    // Build contributed schema from current contributed fields so chained maps stay typed to the public surface.
    const newContributed = buildContributedSchema(this._contributedSchema, mapping);

    // Create new MappedOpGroup
    const newGroup = new MappedOpGroupImpl<Ctx, MappedSchema<ContributedSchema, M>, Ops>(
      this._logSchema,
      this._flags,
      cloneOpsWithView<Ctx, Ops>(this.#ops, remappedViewClass),
      newMapping,
      newContributed,
    );

    assertGroupCarriesOps<Ctx, Ops>(newGroup, this.#ops);
    return newGroup;
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
