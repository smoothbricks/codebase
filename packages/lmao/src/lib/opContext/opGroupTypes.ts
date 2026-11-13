/**
 * OpGroup Types
 *
 * Type definitions for OpGroups - collections of Ops that can be mapped
 * and wired as dependencies. These types enable:
 * - Column mapping (prefix, rename, share, drop), through arrow-builder's column composition algebra
 * - Schema contribution tracking
 * - Type-safe dependency resolution
 *
 * Public interfaces (OpGroup, MappedOpGroup) hide internal properties from
 * intellisense - users only see ops and methods they need.
 * Internal interfaces (OpGroupInternal, MappedOpGroupInternal) expose all
 * properties for library implementation.
 *
 * @module opContext/opGroupTypes
 */

import type {
  ColumnFields,
  ColumnMapping,
  ComposedPart,
  FieldSchema,
  LocalColumns,
  MappedColumns,
  MappedFields,
  PrefixedColumns,
} from '@smoothbricks/arrow-builder';
import type { AnyOp } from '../op.js';
import type { LogSchema } from '../schema/LogSchema.js';
import type { SchemaFields } from '../schema/types.js';
import type { OpContext } from './types.js';

export type { ColumnMapping } from '@smoothbricks/arrow-builder';

/**
 * Extract the schema fields from a LogSchema
 */
export type SchemaFieldsOf<T> = T extends LogSchema<infer Fields> ? Fields : never;

// =============================================================================
// RESERVED OP NAMES
// =============================================================================

/**
 * Reserved names that cannot be used as op names.
 * These conflict with OpGroup methods or internal properties.
 */
export const RESERVED_OP_NAMES = ['prefix', 'mapColumns'] as const;

/**
 * Validate that an op name is allowed.
 * Throws if the name is reserved or starts with underscore.
 *
 * @param name - The op name to validate
 * @throws Error if name is invalid
 */
export function validateOpName(name: string): void {
  if (name.startsWith('_')) {
    throw new Error(
      `Op name "${name}" cannot start with underscore (reserved for internal properties). ` +
        `Consider using "${name.slice(1)}" instead.`,
    );
  }
  if ((RESERVED_OP_NAMES as readonly string[]).includes(name)) {
    throw new Error(
      `Op name "${name}" is reserved (conflicts with OpGroup method). ` +
        `Consider using a more descriptive name like "${name}Op" or "${name}Operation".`,
    );
  }
}

// =============================================================================
// PUBLIC OP GROUP INTERFACES (visible to users in intellisense)
// =============================================================================

export type OpGroupOps<Ctx extends OpContext> = Record<string, AnyOp<Ctx>>;
type NoOpMembers = Record<never, never>;

/**
 * A group of Ops that can be mapped and wired as a dependency.
 *
 * Created by defineOps() - represents a library's exported operations.
 * Libraries declare their schema but don't know how the app will wire them.
 *
 * Note: Ops are accessed directly as properties (`.opName` not `.ops.opName`)
 * for V8 hidden class optimization and cleaner API ergonomics.
 *
 * Internal properties (_logSchema, _flags) are hidden from intellisense
 * but accessible at runtime for library implementation.
 */
interface OpGroupMethods<Ctx extends OpContext, Ops extends object> {
  /**
   * Apply a prefix to all schema columns.
   *
   * E.g., `httpOps.prefix('http')` transforms 'status' -> 'http_status'
   *
   * The prefixed columns become part of the combined app schema:
   * `appSchema & { http_status: S.number(), http_url: S.category() }`
   *
   * @param prefix - Prefix to apply to all schema columns
   * @returns MappedOpGroup with prefixed columns
   */
  prefix<P extends string>(
    prefix: P,
  ): MappedOpGroup<Ctx, PrefixedColumns<LocalColumns<SchemaFieldsOf<Ctx['logSchema']>>, P>, Ops>;

  /**
   * Map the library's fields to columns of the app's schema.
   *
   * Use this for:
   * - Sharing: `{ query: 'query' }` - several libraries write one column
   * - Renaming: `{ rows: 'pg_row_count' }`
   * - Dropping: `{ _internal: null }` - the field has no column; its writes are not emitted
   * - Mixed: `{ query: 'query', rows: 'pg_rows', _debug: null }`
   *
   * A field the mapping does not name keeps its column. Two fields share a column only when each was mapped
   * there and their types are equal; defineOpContext refuses any other meeting of two fields on one column.
   *
   * @param mapping - Maps the library's field names to app column names (or null to drop)
   * @returns MappedOpGroup with the mapped columns
   *
   * @example
   * ```typescript
   * // Two DB libs share 'query' column, each has unique columns, drop debug
   * deps: {
   *   pg: postgresOps.mapColumns({ query: 'query', rows: 'pg_rows', _debug: null }),
   *   mysql: mysqlOps.mapColumns({ query: 'query', duration: 'mysql_duration' }),
   * }
   * ```
   */
  mapColumns<const M extends ColumnMapping<SchemaFieldsOf<Ctx['logSchema']>>>(
    mapping: M,
  ): MappedOpGroup<Ctx, MappedColumns<LocalColumns<SchemaFieldsOf<Ctx['logSchema']>>, M>, Ops>;
}

export type OpGroup<Ctx extends OpContext, Ops extends object = NoOpMembers> = OpGroupMethods<Ctx, Ops> & Ops;

/**
 * An OpGroup with column mapping applied.
 *
 * Wiring composes the mapping into immutable metadata interpreted only by cold
 * Arrow conversion; library child buffers keep their canonical storage layout.
 *
 * Note: Ops are accessed directly as properties (`.opName` not `.ops.opName`)
 * for V8 hidden class optimization and cleaner API ergonomics.
 *
 * Internal properties (_logSchema, _flags, _columns) are hidden from intellisense but accessible at runtime.
 *
 * @template Ctx - OpContext (bundled type with logSchema, flags, deps, userCtx)
 * @template Columns - The column of each of the library's fields (field name → column name, or null)
 */
interface MappedOpGroupMethods<Ctx extends OpContext, Columns, Ops extends object> {
  /** Chain with prefix (prefixes every current column) */
  prefix<P extends string>(prefix: P): MappedOpGroup<Ctx, PrefixedColumns<Columns, P>, Ops>;

  /** Override the columns of single fields, named by the library's field names */
  mapColumns<const M extends ColumnMapping<SchemaFieldsOf<Ctx['logSchema']>>>(
    mapping: M,
  ): MappedOpGroup<Ctx, MappedColumns<Columns, M>, Ops>;
}

export type MappedOpGroup<Ctx extends OpContext, Columns, Ops extends object = NoOpMembers> = MappedOpGroupMethods<
  Ctx,
  Columns,
  Ops
> &
  Ops;

// =============================================================================
// INTERNAL OP GROUP INTERFACES (for library implementation)
// =============================================================================

/**
 * Internal OpGroup interface with all properties exposed.
 * Used by library implementation code that needs access to schema/flags.
 */
export interface OpGroupInternal<Ctx extends OpContext, Ops extends object = NoOpMembers>
  extends OpGroupMethods<Ctx, Ops> {
  /** The log schema for this group's ops (internal) */
  readonly _logSchema: Ctx['logSchema'];

  /** The feature flag schema (internal) */
  readonly _flags: Ctx['flags'];

  /** The library's own fields, under their local names (internal) */
  readonly _columns: FieldSchema<string, SchemaFieldsOf<Ctx['logSchema']>>;
}

/**
 * Internal MappedOpGroup interface with all properties exposed.
 * Used by library implementation code that needs access to mapping details.
 */
export interface MappedOpGroupInternal<Ctx extends OpContext, Columns, Ops extends object = NoOpMembers>
  extends MappedOpGroupMethods<Ctx, Columns, Ops> {
  /** The log schema for this group's ops (internal) */
  readonly _logSchema: Ctx['logSchema'];

  /** The feature flag schema (internal) */
  readonly _flags: Ctx['flags'];

  /** The library's fields with the columns the wiring gave them (internal) */
  readonly _columns: MappedFields<string, SchemaFieldsOf<Ctx['logSchema']>, Columns>;
}

// =============================================================================
// DEPENDENCY CONFIG TYPES
// =============================================================================

/**
 * Minimal structural interface for any OpGroup, mapped or not (internal use only).
 *
 * This captures only the internal properties dependency wiring reads. They exist at runtime but are
 * hidden from the public TypeScript interfaces.
 *
 * Note: No `.ops` property - ops are accessed directly as own properties.
 */
export interface AnyOpGroup {
  /** The log schema - read-only access for wiring (internal property) */
  readonly _logSchema: LogSchema;
  /** The group's part in a column composition: its own fields, mapped or under their local names */
  readonly _columns: ComposedPart;
}

/**
 * Minimal public interface for OpGroups used in deps config.
 * Accepts any OpGroup or MappedOpGroup without requiring internal properties.
 * Internal properties are accessed at runtime via type assertion.
 */
export interface AnyOpGroupPublic {
  /** Apply a prefix to all schema columns */
  prefix(prefix: string): AnyOpGroupPublic;
  /** Map schema columns to different names */
  mapColumns(mapping: ColumnMapping<SchemaFields>): AnyOpGroupPublic;
}

/**
 * Type for deps config - maps names to OpGroups (optionally mapped)
 *
 * This is used as a constraint (`Deps extends DepsConfig`) so it needs
 * to be wide enough to accept any specific deps configuration.
 *
 * Uses AnyOpGroupPublic to accept OpGroup/MappedOpGroup without requiring
 * internal properties in the type signature.
 */
export type DepsConfig = Record<string, AnyOpGroupPublic>;

// =============================================================================
// SCHEMA COMBINATION TYPES
// =============================================================================

/**
 * Extract the contributed schema from a dep (for combining into app schema): column name → field
 */
export type ContributedSchemaOf<D> =
  D extends MappedOpGroup<infer Ctx, infer Columns, infer _Ops>
    ? ColumnFields<SchemaFieldsOf<Ctx['logSchema']>, Columns>
    : D extends OpGroup<infer Ctx, infer _Ops>
      ? SchemaFieldsOf<Ctx['logSchema']>
      : never;

/**
 * Combine all contributed schemas from deps into a union of all their fields.
 * App's effective schema = appSchema & pg_contributed & mysql_contributed & ...
 *
 * This is used to type the combined LogSchema that has access to all columns
 * from both the app and all wired dependencies.
 */
export type CombinedDepsSchema<D extends DepsConfig> = UnionToIntersection<
  { [K in keyof D]: ContributedSchemaOf<D[K]> }[keyof D]
>;

/**
 * Helper: Convert union to intersection
 * `A | B | C` → `A & B & C`
 */
export type UnionToIntersection<U> = (U extends unknown ? (k: U) => void : never) extends (k: infer I) => void
  ? I
  : never;

/**
 * The effective schema for an OpContext: app schema + all dep contributed schemas.
 */
export type EffectiveSchema<T extends SchemaFields, D extends DepsConfig> = T & CombinedDepsSchema<D>;

/**
 * Extract the typed deps interface from a DepsConfig.
 * Each dep becomes accessible via ctx.deps.name
 */
export type ResolvedDeps<D extends DepsConfig> = {
  readonly [K in keyof D]: D[K] extends OpGroup<infer Ctx, infer Ops>
    ? OpGroup<Ctx, Ops>
    : D[K] extends MappedOpGroup<infer Ctx, infer _CS, infer Ops>
      ? OpGroup<Ctx, Ops>
      : never;
};
