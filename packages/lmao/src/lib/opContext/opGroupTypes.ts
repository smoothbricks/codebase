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
import type { FeatureFlagSchema } from './featureFlagTypes.js';
import type { Op } from './opTypes.js';

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
export interface OpGroup<
  T extends LogSchema = LogSchema,
  FF extends FeatureFlagSchema = FeatureFlagSchema,
  UserCtx extends Record<string, unknown> = Record<string, unknown>,
> {
  /** The log schema for this group's ops */
  readonly logSchema: T;

  /** The feature flag schema */
  readonly flags: FF;

  /** The ops in this group (loosely typed - actual types preserved in intersection) */
  // biome-ignore lint/suspicious/noExplicitAny: Ops stored with any deps - actual Op types come from defineOps intersection
  readonly ops: Record<string, Op<T, FF, any, UserCtx, unknown[], unknown>>;

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
  prefix<P extends string>(prefix: P): MappedOpGroup<T, FF, UserCtx, PrefixedSchema<SchemaFieldsOf<T>, P>>;

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
  mapColumns<M extends ColumnMapping<SchemaFieldsOf<T>>>(
    mapping: M,
  ): MappedOpGroup<T, FF, UserCtx, MappedSchema<SchemaFieldsOf<T>, M>>;
}

/**
 * An OpGroup with column mapping applied.
 *
 * The mapping is used during wiring to create RemappedBufferView
 * that translates the library's column writes to the app's column names.
 *
 * @template T - Original library LogSchema
 * @template FF - Feature flag schema
 * @template UserCtx - User context type
 * @template ContributedSchema - Schema fields this group contributes to app (after mapping)
 */
export interface MappedOpGroup<
  T extends LogSchema,
  FF extends FeatureFlagSchema,
  UserCtx extends Record<string, unknown>,
  ContributedSchema extends SchemaFields = SchemaFields,
> {
  /** The original log schema (library's column names) */
  readonly logSchema: T;

  /** The feature flag schema */
  readonly flags: FF;

  /** The ops in this group */
  // biome-ignore lint/suspicious/noExplicitAny: Ops stored with any deps - actual Op types come from defineOps intersection
  readonly ops: Record<string, Op<T, FF, any, UserCtx, unknown[], unknown>>;

  /** The column mapping (library column -> app column, or null to drop) */
  readonly _columnMapping: ColumnMapping<SchemaFieldsOf<T>>;

  /** The schema fields this group contributes to the app's combined schema */
  readonly _contributedSchema: ContributedSchema;

  /** Chain with prefix (applies prefix to current mapping) */
  prefix<P extends string>(prefix: P): MappedOpGroup<T, FF, UserCtx, PrefixedSchema<ContributedSchema, P>>;

  /** Override/extend the mapping */
  mapColumns<M extends ColumnMapping<SchemaFieldsOf<T>>>(
    mapping: M,
  ): MappedOpGroup<T, FF, UserCtx, MappedSchema<SchemaFieldsOf<T>, M>>;
}

// =============================================================================
// DEPENDENCY CONFIG TYPES
// =============================================================================

/**
 * Any OpGroup (mapped or not) that can be used as a dependency
 */
// biome-ignore lint/suspicious/noExplicitAny: Generic dep type needs any for flexibility
export type AnyOpGroup = OpGroup<any, any, any> | MappedOpGroup<any, any, any, any>;

/**
 * Type for deps config - maps names to OpGroups (optionally mapped)
 */
export type DepsConfig = Record<string, AnyOpGroup>;

/**
 * Empty deps config (no dependencies)
 */
export type EmptyDeps = Record<string, never>;

// =============================================================================
// SCHEMA COMBINATION TYPES
// =============================================================================

/**
 * Extract the contributed schema from a dep (for combining into app schema)
 */
export type ContributedSchemaOf<D> = D extends MappedOpGroup<infer _T, infer _FF, infer _UC, infer CS>
  ? CS
  : D extends OpGroup<infer T, infer _FF, infer _UC>
    ? SchemaFieldsOf<T>
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
  readonly [K in keyof D]: D[K] extends OpGroup<infer T, infer FF, infer UC>
    ? OpGroup<T, FF, UC>
    : D[K] extends MappedOpGroup<infer T, infer FF, infer UC, infer _CS>
      ? OpGroup<T, FF, UC>
      : never;
};
