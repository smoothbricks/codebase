/**
 * Op-Centric API Type Definitions Hub
 *
 * This is the central hub for all Op-centric API types. Consumers should import
 * from this file rather than individual type files.
 *
 * Organized into logical groups:
 * - Context utilities (reserved props, null/undefined key extraction, validation)
 * - Feature flags (schema, evaluator, bound flags)
 * - OpGroup (groups, mapping, deps, schema combination)
 * - Op (metadata, function signature, instance)
 * - SpanContext (tag writer, logger, span function, full context)
 *
 * @module opContext/types
 */

// =============================================================================
// RE-EXPORTS FROM EXTRACTED TYPE FILES
// =============================================================================

// FlagEvaluator interface is in schema/evaluator.ts
export type { FlagEvaluator, SpanContextWithoutFf } from '../schema/evaluator.js';
// Context type utilities (reserved props, null/undefined key extraction, validation)
export {
  type DefaultKeys,
  type NullKeys,
  type OptionalContextParams,
  RESERVED_CONTEXT_PROPS,
  type RequiredContextParams,
  type ReservedContextProp,
  type ResolvedContext,
  type TraceContextParams,
  type UndefinedKeys,
  type ValidateUserContext,
} from './contextTypes.js';
// Feature flag types
export type { BoundFeatureFlags, FeatureFlagSchema } from './featureFlagTypes.js';
// OpGroup types (groups, mapping, deps, schema combination)
export type {
  AnyOpGroup,
  ColumnMapping,
  CombinedDepsSchema,
  ContributedSchemaOf,
  DepsConfig,
  EffectiveSchema,
  MappedOpGroup,
  MappedSchema,
  OpGroup,
  PrefixedSchema,
  ResolvedDeps,
  SchemaFieldsOf,
  UnionToIntersection,
} from './opGroupTypes.js';
// Op types (metadata, function signature, instance)
export type { Op, OpFn, OpMetadata, OpsFromRecord } from './opTypes.js';
// SpanContext types (tag writer, logger, span function, full context)
export type { FluentLogEntry, SpanContext, SpanFn, SpanLogger, TagWriter } from './spanContextTypes.js';

// =============================================================================
// CONVENIENCE RE-EXPORTS
// =============================================================================

export { LogSchema } from '../schema/LogSchema.js';
export type { FeatureFlagDefinition, InferSchema, SchemaFields } from '../schema/types.js';

// =============================================================================
// OP CONTEXT TYPE SYMBOL
// =============================================================================

/**
 * Unique symbol to carry OpContext type on OpContextFactory.
 * Allows extracting the bundled type without passing 4 separate params.
 * Must be a runtime value (not just `declare const`) so it can be used as property key.
 */
const opContextType: unique symbol = Symbol('opContextType');

/**
 * Helper to extract OpContext from a factory.
 * @example
 * const { defineOp } = defineOpContext({ ... });
 * type MyCtx = OpContextOf<typeof factory>; // OpContext<T, FF, Deps, UserCtx>
 */
export type OpContextOf<F> = F extends { readonly [opContextType]: infer Ctx } ? Ctx : never;

export { opContextType };

// =============================================================================
// OP CONTEXT BUNDLE TYPE
// =============================================================================

/**
 * Bundled type parameters for Op context.
 *
 * Created by defineOpContext() and shared by all Ops from that factory.
 * Reduces type parameter count: Op<Ctx, Args, S, E> instead of Op<T, FF, Deps, UserCtx, Args, R>
 *
 * @template T - LogSchema type
 * @template FF - Feature flag schema
 * @template Deps - Dependencies config
 * @template UserCtx - User context properties
 */
export type OpContext<
  T extends LogSchema = LogSchema,
  FF extends FeatureFlagSchema = {},
  Deps extends DepsConfig = {},
  UserCtx extends Record<string, unknown> = {},
> = {
  readonly logSchema: T;
  readonly flags: FF;
  readonly deps: Deps;
  readonly userCtx: UserCtx;
};

// =============================================================================
// OP CONTEXT CONFIGURATION
// =============================================================================

import type { LogSchema } from '../schema/LogSchema.js';
import type { SchemaFields } from '../schema/types.js';
import type { ValidateUserContext } from './contextTypes.js';
import type { FeatureFlagSchema } from './featureFlagTypes.js';
import type { DepsConfig, EffectiveSchema, OpGroup, SchemaFieldsOf } from './opGroupTypes.js';
import type { Op, OpFn, OpMetadata, OpsFromRecord } from './opTypes.js';

/**
 * Configuration for defineOpContext
 *
 * This is the main configuration interface for creating an Op context factory.
 * It defines:
 * - The app's own schema (logSchema)
 * - Dependencies from other OpGroups (deps)
 * - Feature flag schema (flags)
 * - User-defined context properties (ctx)
 *
 * @template T - LogSchema fields (app's own schema)
 * @template FF - Feature flag schema
 * @template Deps - Dependency record (other OpGroups with mappings)
 * @template UserCtx - User-defined context properties
 *
 * The effective schema for ops = `T & CombinedDepsSchema<Deps>`.
 * This means ops can write to columns from both the app's schema AND
 * all columns contributed by wired dependencies (after mapping/prefixing).
 */
export interface OpContextConfig<
  T extends SchemaFields,
  FF extends FeatureFlagSchema,
  Deps extends DepsConfig,
  UserCtx extends Record<string, unknown>,
> {
  /**
   * Log schema defining the app's own columns.
   *
   * The effective schema for ops is this PLUS columns from deps.
   * E.g., if you wire `http: httpOps.prefix('http')`, ops can also
   * write to `http_status`, `http_url`, etc.
   */
  readonly logSchema: LogSchema<T>;

  /**
   * Dependencies - other OpGroups that can be invoked via ctx.deps
   *
   * Each dep's columns are added to the effective schema based on its mapping:
   * - `.prefix('http')` → columns become `http_status`, `http_url`, etc.
   * - `.mapColumns({ query: 'query' })` → shares the `query` column
   * - `.mapColumns({ _internal: null })` → drops the `_internal` column
   *
   * @example
   * deps: {
   *   pg: postgresOps.mapColumns({ query: 'query', rows: 'pg_rows' }),
   *   mysql: mysqlOps.mapColumns({ query: 'query', duration: 'mysql_duration' }),
   *   http: httpOps.prefix('http'),
   * }
   * // Effective schema includes: query, pg_rows, mysql_duration, http_status, http_url, ...
   */
  readonly deps?: Deps;

  /**
   * Feature flag schema - defines available flags and their types
   *
   * @example
   * flags: defineFeatureFlags({
   *   newCheckout: S.boolean().default(false).async(),
   *   maxRetries: S.number().default(3).sync(),
   * })
   */
  readonly flags?: FF;

  /**
   * User-defined context properties
   *
   * ALL properties must be declared here for V8 hidden class optimization.
   * Properties set to `null` must be provided when calling createTrace().
   * Properties with values have defaults and can be optionally overridden.
   *
   * @example
   * ctx: {
   *   env: null as CFWorkerEnv,       // Must provide at createTrace()
   *   config: { retryCount: 3 },       // Has default, can override
   *   requestId: null as string,       // Must provide at createTrace()
   * }
   */
  readonly ctx?: ValidateUserContext<UserCtx>;
}

// =============================================================================
// OP CONTEXT BINDING (For Tracer)
// =============================================================================

/**
 * Runtime binding from defineOpContext for use with Tracer.
 *
 * Contains everything a Tracer needs to create traces:
 * - logBinding: Schema and capacity stats for buffer creation
 * - ctxDefaults: User context defaults (with null sentinels for required fields)
 * - deps: Wired dependencies for ctx.deps
 *
 * Carries phantom type via [opContextType] for full OpContext inference.
 * This allows `new TestTracer(ctx)` to infer T from ctx.logBinding without
 * explicit type parameters.
 *
 * @template T - LogSchema type
 * @template FF - Feature flag schema
 * @template Deps - Dependencies config
 * @template UserCtx - User context properties
 *
 * @example
 * ```typescript
 * const ctx = defineOpContext({
 *   logSchema: defineLogSchema({ userId: S.category() }),
 *   ctx: { env: null as Env },
 * });
 *
 * // Tracer infers types from binding - no explicit type params needed
 * const { trace } = new TestTracer(ctx);
 * ```
 */
export interface OpContextBinding<
  T extends LogSchema = LogSchema,
  FF extends FeatureFlagSchema = {},
  Deps extends DepsConfig = {},
  UserCtx extends Record<string, unknown> = {},
> {
  /**
   * Phantom type - extract via OpContextOf<typeof binding>
   *
   * Carries the bundled OpContext type for type inference.
   * Tracers use this to infer the full context type without explicit parameters.
   *
   * @internal
   */
  readonly [opContextType]: OpContext<T, FF, Deps, UserCtx>;

  /**
   * LogBinding for this context (schema, capacity stats, optional prefix view).
   * Used by Tracer to create SpanBuffers and SpanContexts.
   * Typed with schema T for Op type safety.
   */
  readonly logBinding: import('../logBinding.js').LogBinding<T>;

  /**
   * User context defaults from ctx config.
   * Used by Tracer to merge with trace-level overrides.
   * Properties with null values are required at trace creation.
   */
  readonly ctxDefaults: UserCtx;

  /**
   * Dependencies wired at defineOpContext time.
   * Used by Tracer to populate ctx.deps on root spans.
   */
  readonly deps: Deps;
}

// =============================================================================
// OP CONTEXT FACTORY
// =============================================================================

/**
 * Return type of defineOpContext
 *
 * This is the factory returned by defineOpContext that provides:
 * - `defineOp`: Define a single Op with explicit name
 * - `defineOps`: Define multiple Ops and create an OpGroup
 * - `logSchema`: The combined effective schema
 * - `flags`: The feature flag schema
 *
 * Extends OpContextBinding to provide everything Tracer needs.
 * Uses unique symbol [opContextType] to carry the bundled OpContext type
 * for extraction via OpContextOf<typeof factory>.
 *
 * @template T - The app's LogSchema
 * @template FF - Feature flag schema
 * @template Deps - Wired dependencies
 * @template UserCtx - User context properties
 *
 * The effective schema for ops is `T & CombinedDepsSchema<Deps>` - the app's
 * schema combined with all columns contributed by wired dependencies.
 */
export interface OpContextFactory<
  T extends LogSchema,
  FF extends FeatureFlagSchema,
  Deps extends DepsConfig,
  UserCtx extends Record<string, unknown>,
> extends OpContextBinding<T, FF, Deps, UserCtx> {
  /**
   * The combined log schema (app schema + dep contributions).
   *
   * This is the effective schema that ops can write to. It includes:
   * - All columns from the app's logSchema
   * - All mapped/prefixed columns from dependencies
   *
   * External consumers can use this to see the full schema shape.
   */
  readonly logSchema: LogSchema<EffectiveSchema<SchemaFieldsOf<T>, Deps>>;

  /**
   * The feature flag schema (for external consumers)
   */
  readonly flags: FF;

  /**
   * Define a single Op with explicit name.
   *
   * Creates an Op instance that wraps your function with metadata for tracing.
   * The Op can be invoked via ctx.span() or exported for use by other modules.
   *
   * @param name - Op name (used for default span naming and metrics)
   * @param fn - Op function receiving SpanContext and returning Result<S, E>
   * @param metadata - Optional metadata override (package_name, package_file, etc.)
   * @returns Op instance with type Op<OpContext<T, FF, Deps, UserCtx>, Args, S, E>
   *
   * @example
   * ```typescript
   * const fetchUser = defineOp('fetchUser', async (ctx, userId: string) => {
   *   ctx.tag.userId(userId);
   *   const user = await db.getUser(userId);
   *   if (!user) return ctx.err('NOT_FOUND', { userId });
   *   return ctx.ok(user);
   * });
   *
   * // Invoke via span:
   * const result = await ctx.span('get-user', fetchUser, 'user-123');
   * ```
   */
  defineOp<Args extends unknown[], S, E>(
    name: string,
    fn: OpFn<OpContext<T, FF, Deps, UserCtx>, Args, S, E>,
    metadata?: Partial<OpMetadata>,
  ): Op<OpContext<T, FF, Deps, UserCtx>, Args, S, E>;

  /**
   * Define multiple Ops at once and create an OpGroup.
   *
   * Property keys become Op names. Supports `this` binding for calling
   * sibling ops within the same batch. Returns an OpGroup that can be:
   * - Prefixed with `.prefix('http')` for column namespacing
   * - Column-mapped with `.mapColumns({ query: 'query' })` for sharing
   * - Wired as a dependency in another module's `deps` config
   *
   * @param definitions - Record of Op functions or existing Op instances
   * @returns OpGroup with all defined Ops accessible as properties
   *
   * @example
   * ```typescript
   * export const userOps = defineOps({
   *   // Reuse existing Op
   *   fetchUser,
   *
   *   // Define new op with method syntax
   *   async createUser(ctx, data: UserData) {
   *     ctx.tag.operation('INSERT');
   *     const user = await db.createUser(data);
   *     return ctx.ok(user);
   *   },
   *
   *   // Call sibling op via this.opName
   *   async createAndFetch(ctx, data: UserData) {
   *     const created = await ctx.span('create', this.createUser, data);
   *     if (!created.success) return created;
   *     return ctx.span('fetch', this.fetchUser, created.value.id);
   *   },
   * });
   *
   * // Wire as dependency with prefix:
   * deps: { users: userOps.prefix('user') }
   *
   * // Invoke from parent context:
   * await ctx.span('create-user', userOps.createUser, userData);
   * ```
   */
  defineOps<
    Defs extends Record<
      string,
      | OpFn<OpContext<T, FF, Deps, UserCtx>, unknown[], unknown, unknown>
      | Op<OpContext<T, FF, Deps, UserCtx>, unknown[], unknown, unknown>
    >,
  >(definitions: Defs): OpGroup<OpContext<T, FF, Deps, UserCtx>> & OpsFromRecord<OpContext<T, FF, Deps, UserCtx>, Defs>;
}
