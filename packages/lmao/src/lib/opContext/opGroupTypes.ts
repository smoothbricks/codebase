/**
 * OpGroup Types
 *
 * Type definitions for OpGroups - collections of Ops that can be mapped
 * and wired as dependencies. These types enable:
 * - Column mapping (prefix, rename, drop)
 * - Schema contribution tracking
 * - Type-safe dependency resolution
 *
 * @module opContext/opGroupTypes
 */

import type { LogSchema } from '../schema/LogSchema.js';
import type { SchemaFields } from '../schema/types.js';
import type { Op } from './opTypes.js';
import type { OpContext } from './types.js';

// =============================================================================
// COLUMN MAPPING TYPES
// =============================================================================

/**
 * Column mapping for wiring OpGroups.
 *
 * Maps the library's column names to the app's column names:
 * - `string` → map to that column name (must be type-compatible)
 * - `null` → drop the column (library writes are ignored)
 *
 * @example
 * ```typescript
 * // Map query to shared column, rows to prefixed, drop internal column
 * { query: 'query', rows: 'pg_rows', _internal: null }
 * ```
 */
export type ColumnMapping<LibSchema extends SchemaFields = SchemaFields> = {
  [K in keyof LibSchema]?: string | null;
};

/**
 * Apply a prefix to all schema field names.
 * `{ status: S.number() }` with prefix 'http' → `{ http_status: S.number() }`
 */
export type PrefixedSchema<T extends SchemaFields, P extends string> = {
  [K in keyof T as `${P}_${K & string}`]: T[K];
};

/**
 * Extract the schema fields from a LogSchema
 */
export type SchemaFieldsOf<T> = T extends LogSchema<infer Fields> ? Fields : never;

/**
 * Apply column mapping to schema.
 * - Mapped columns get new names
 * - Columns mapped to null are dropped
 * - Unmapped columns keep original names (with prefix if using prefix())
 */
export type MappedSchema<T extends SchemaFields, M extends ColumnMapping<T>> = {
  // For each key in mapping that maps to a string, use that string as the new key
  [K in keyof T as M[K] extends string ? M[K] : M[K] extends null ? never : K]: T[K];
};

// =============================================================================
// OP GROUP INTERFACES
// =============================================================================

/**
 * A group of Ops that can be mapped and wired as a dependency.
 *
 * Created by defineOps() - represents a library's exported operations.
 * Libraries declare their schema but don't know how the app will wire them.
 */
export interface OpGroup<Ctx extends OpContext> {
  /** The log schema for this group's ops */
  readonly logSchema: Ctx['logSchema'];

  /** The feature flag schema */
  readonly flags: Ctx['flags'];

  /** The ops in this group (loosely typed - actual types preserved in intersection) */
  readonly ops: Record<string, Op<Ctx, unknown[], unknown, unknown>>;

  /**
   * Apply a prefix to all schema columns.
   *
   * Shorthand for mapColumns where each column gets the prefix.
   * E.g., `httpOps.prefix('http')` transforms 'status' -> 'http_status'
   *
   * The prefixed columns become part of the combined app schema:
   * `appSchema & { http_status: S.number(), http_url: S.category() }`
   *
   * @param prefix - Prefix to apply to all schema columns
   * @returns MappedOpGroup with prefixed schema contribution
   */
  prefix<P extends string>(prefix: P): MappedOpGroup<Ctx, PrefixedSchema<SchemaFieldsOf<Ctx['logSchema']>, P>>;

  /**
   * Map schema columns to different names in the app's schema.
   *
   * Use this for:
   * - Deduplication: `{ query: 'query' }` - multiple libs share same column
   * - Renaming: `{ rows: 'pg_row_count' }`
   * - Dropping: `{ _internal: null }` - ignore library's internal column
   * - Mixed: `{ query: 'query', rows: 'pg_rows', _debug: null }`
   *
   * Type-safety: Target columns must have compatible schema types.
   *
   * @param mapping - Maps library column names to app column names (or null to drop)
   * @returns MappedOpGroup with mapped schema contribution
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
  mapColumns<M extends ColumnMapping<SchemaFieldsOf<Ctx['logSchema']>>>(
    mapping: M,
  ): MappedOpGroup<Ctx, MappedSchema<SchemaFieldsOf<Ctx['logSchema']>, M>>;
}

/**
 * An OpGroup with column mapping applied.
 *
 * The mapping is used during wiring to create RemappedBufferView
 * that translates the library's column writes to the app's column names.
 *
 * @template Ctx - OpContext (bundled type with logSchema, flags, deps, userCtx)
 * @template ContributedSchema - Schema fields this group contributes to app (after mapping)
 */
export interface MappedOpGroup<Ctx extends OpContext, ContributedSchema extends SchemaFields> {
  /** The log schema for this group's ops */
  readonly logSchema: Ctx['logSchema'];

  /** The feature flag schema */
  readonly flags: Ctx['flags'];

  /** The schema fields this group contributes to the app's combined schema */
  readonly contributedSchema: ContributedSchema;

  /** The column mapping (library column -> app column, or null to drop) */
  readonly columnMapping: ColumnMapping<SchemaFieldsOf<Ctx['logSchema']>>;

  /** The ops in this group */
  readonly ops: Record<string, Op<Ctx, unknown[], unknown, unknown>>;

  /** Chain with prefix (applies prefix to current mapping) */
  prefix<P extends string>(prefix: P): MappedOpGroup<Ctx, PrefixedSchema<ContributedSchema, P>>;

  /** Override/extend the mapping */
  mapColumns<M extends ColumnMapping<ContributedSchema>>(
    mapping: M,
  ): MappedOpGroup<Ctx, MappedSchema<ContributedSchema, M>>;
}

// =============================================================================
// DEPENDENCY CONFIG TYPES
// =============================================================================

/**
 * Any OpGroup (mapped or not) that can be used as a dependency
 */
export type AnyOpGroup = OpGroup<OpContext> | MappedOpGroup<OpContext, SchemaFields>;

/**
 * Type for deps config - maps names to OpGroups (optionally mapped)
 */
export type DepsConfig = Record<string, AnyOpGroup>;

// =============================================================================
// SCHEMA COMBINATION TYPES
// =============================================================================

/**
 * Extract the contributed schema from a dep (for combining into app schema)
 */
export type ContributedSchemaOf<D> = D extends MappedOpGroup<infer _Ctx, infer CS>
  ? CS
  : D extends OpGroup<infer Ctx>
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
  readonly [K in keyof D]: D[K] extends OpGroup<infer Ctx>
    ? OpGroup<Ctx>
    : D[K] extends MappedOpGroup<infer Ctx, infer _CS>
      ? OpGroup<Ctx>
      : never;
};
