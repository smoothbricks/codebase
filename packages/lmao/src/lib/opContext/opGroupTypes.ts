/**
 * OpGroup Types
 *
 * Type definitions for OpGroups - collections of Ops that can be mapped
 * and wired as dependencies. These types enable:
 * - Column mapping (prefix, rename, drop)
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

import type { Op } from '../op.js';
import type { LogSchema } from '../schema/LogSchema.js';
import type { SchemaFields } from '../schema/types.js';
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

export type OpGroupOps<Ctx extends OpContext> = Record<string, Op<Ctx, unknown[], unknown, unknown>>;
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
   * Shorthand for mapColumns where each column gets the prefix.
   * E.g., `httpOps.prefix('http')` transforms 'status' -> 'http_status'
   *
   * The prefixed columns become part of the combined app schema:
   * `appSchema & { http_status: S.number(), http_url: S.category() }`
   *
   * @param prefix - Prefix to apply to all schema columns
   * @returns MappedOpGroup with prefixed schema contribution
   */
  prefix<P extends string>(prefix: P): MappedOpGroup<Ctx, PrefixedSchema<SchemaFieldsOf<Ctx['logSchema']>, P>, Ops>;

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
  ): MappedOpGroup<Ctx, MappedSchema<SchemaFieldsOf<Ctx['logSchema']>, M>, Ops>;
}

export type OpGroup<Ctx extends OpContext, Ops extends object = NoOpMembers> = OpGroupMethods<Ctx, Ops> & Ops;

/**
 * An OpGroup with column mapping applied.
 *
 * The mapping is used during wiring to create RemappedBufferView
 * that translates the library's column writes to the app's column names.
 *
 * Note: Ops are accessed directly as properties (`.opName` not `.ops.opName`)
 * for V8 hidden class optimization and cleaner API ergonomics.
 *
 * Internal properties (_logSchema, _flags, _contributedSchema, _columnMapping)
 * are hidden from intellisense but accessible at runtime.
 *
 * @template Ctx - OpContext (bundled type with logSchema, flags, deps, userCtx)
 * @template ContributedSchema - Schema fields this group contributes to app (after mapping)
 */
interface MappedOpGroupMethods<Ctx extends OpContext, ContributedSchema extends SchemaFields, Ops extends object> {
  /** Chain with prefix (applies prefix to current mapping) */
  prefix<P extends string>(prefix: P): MappedOpGroup<Ctx, PrefixedSchema<ContributedSchema, P>, Ops>;

  /** Override/extend the mapping */
  mapColumns<M extends ColumnMapping<ContributedSchema>>(
    mapping: M,
  ): MappedOpGroup<Ctx, MappedSchema<ContributedSchema, M>, Ops>;
}

export type MappedOpGroup<
  Ctx extends OpContext,
  ContributedSchema extends SchemaFields,
  Ops extends object = NoOpMembers,
> = MappedOpGroupMethods<Ctx, ContributedSchema, Ops> & Ops;

// =============================================================================
// INTERNAL OP GROUP INTERFACES (for library implementation)
// =============================================================================

/**
 * Internal OpGroup interface with all properties exposed.
 * Used by library implementation code that needs access to schema/flags.
 *
 * NOT exported from public API - use type assertion to access:
 * `(opGroup as OpGroupInternal<Ctx>)._logSchema`
 */
export interface OpGroupInternal<Ctx extends OpContext, Ops extends object = NoOpMembers>
  extends OpGroupMethods<Ctx, Ops> {
  /** The log schema for this group's ops (internal) */
  readonly _logSchema: Ctx['logSchema'];

  /** The feature flag schema (internal) */
  readonly _flags: Ctx['flags'];
}

/**
 * Internal MappedOpGroup interface with all properties exposed.
 * Used by library implementation code that needs access to mapping details.
 */
export interface MappedOpGroupInternal<
  Ctx extends OpContext,
  ContributedSchema extends SchemaFields,
  Ops extends object = NoOpMembers,
> extends MappedOpGroupMethods<Ctx, ContributedSchema, Ops> {
  /** The log schema for this group's ops (internal) */
  readonly _logSchema: Ctx['logSchema'];

  /** The feature flag schema (internal) */
  readonly _flags: Ctx['flags'];

  /** The schema fields this group contributes to the app's combined schema (internal) */
  readonly _contributedSchema: ContributedSchema;

  /** The column mapping (library column -> app column, or null to drop) (internal) */
  readonly _columnMapping: ColumnMapping<SchemaFieldsOf<Ctx['logSchema']>>;
}

// =============================================================================
// DEPENDENCY CONFIG TYPES
// =============================================================================

/**
 * Minimal structural interface for any OpGroup (internal use only).
 *
 * This captures only the internal properties we need to read from an OpGroup
 * in dependency wiring. The internal properties exist at runtime but are
 * hidden from the public TypeScript interfaces.
 *
 * Note: No `.ops` property - ops are accessed directly as own properties.
 */
export interface AnyOpGroup {
  /** The log schema - read-only access for wiring (internal property) */
  readonly _logSchema: LogSchema;
}

/**
 * Minimal structural interface for a MappedOpGroup (internal use only).
 * Used for dependency wiring when we need access to contributed schema.
 */
export interface AnyMappedOpGroup extends AnyOpGroup {
  /** The contributed schema after mapping (internal property) */
  readonly _contributedSchema: SchemaFields;
}

/**
 * Type guard to check if an OpGroup has been mapped (has _contributedSchema)
 */
export function isMappedOpGroup(dep: AnyOpGroup): dep is AnyMappedOpGroup {
  return '_contributedSchema' in dep;
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
  mapColumns(mapping: ColumnMapping): AnyOpGroupPublic;
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
 * Extract the contributed schema from a dep (for combining into app schema)
 */
export type ContributedSchemaOf<D> =
  D extends MappedOpGroup<infer _Ctx, infer CS, infer _Ops>
    ? CS
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
