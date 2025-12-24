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
  EmptyDeps,
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
export type { FluentLogEntry, InvocableOp, SpanContext, SpanFn, SpanLogger, TagWriter } from './spanContextTypes.js';

// =============================================================================
// CONVENIENCE RE-EXPORTS
// =============================================================================

export { LogSchema } from '../schema/LogSchema.js';
export type { FeatureFlagDefinition, InferSchema, SchemaFields } from '../schema/types.js';

// =============================================================================
// OP CONTEXT CONFIGURATION
// =============================================================================

import type { FlagEvaluator } from '../schema/evaluator.js';
import type { LogSchema } from '../schema/LogSchema.js';
import type { SchemaFields } from '../schema/types.js';
import type { TraceContextParams, ValidateUserContext } from './contextTypes.js';
import type { FeatureFlagSchema } from './featureFlagTypes.js';
import type { DepsConfig, EffectiveSchema, OpGroup, SchemaFieldsOf } from './opGroupTypes.js';
import type { Op, OpFn, OpsFromRecord } from './opTypes.js';
import type { SpanContext } from './spanContextTypes.js';

/**
 * Configuration for defineOpContext
 *
 * This is the main configuration interface for creating an Op context factory.
 * It defines:
 * - The app's own schema (logSchema)
 * - Dependencies from other OpGroups (deps)
 * - Feature flags and their evaluator (flags, flagEvaluator)
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
   * Feature flag evaluator
   * Provides getSync/getAsync/forContext methods to resolve flag values.
   * Receives SpanContext (minus `ff`) for logging, child spans, and user context.
   *
   * @example
   * flagEvaluator: new InMemoryFlagEvaluator(flags.schema, { darkMode: true }),
   *
   * // Or implement FlagEvaluator interface for LaunchDarkly:
   * flagEvaluator: {
   *   getSync: (ctx, flag) => { ctx.log.debug(`Eval ${flag}`); return ldClient.variationSync(...); },
   *   getAsync: (ctx, flag) => ldClient.variation(flag, { user: ctx.scope.userId }),
   *   forContext: (ctx) => new FeatureFlagEvaluator(schema, ctx, this),
   * }
   */
  readonly flagEvaluator?: FlagEvaluator<LogSchema<T>, FF, unknown>;

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
// OP CONTEXT FACTORY
// =============================================================================

/**
 * Return type of defineOpContext
 *
 * This is the factory returned by defineOpContext that provides:
 * - `defineOp`: Define a single Op with explicit name
 * - `defineOps`: Define multiple Ops and create an OpGroup
 * - `createTrace`: Create a root trace context for a request/invocation
 * - `logSchema`: The combined effective schema
 * - `flags`: The feature flag schema
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
> {
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
   * LogBinding for this context (schema, capacity stats, optional prefix view).
   * Used by Tracer to create SpanBuffers and SpanContexts.
   */
  readonly logBinding: import('../logBinding.js').LogBinding;

  /**
   * User context defaults from ctx config.
   * Used by Tracer to merge with trace-level overrides.
   */
  readonly ctxDefaults: UserCtx;

  /**
   * Feature flag evaluator (optional).
   * Used by Tracer to bind feature flags to SpanContexts.
   */
  readonly flagEvaluator?: FlagEvaluator<T, FF, UserCtx>;

  /**
   * Define a single Op with explicit name
   *
   * @param name - Op name for metrics and default span naming
   * @param fn - Op function
   * @returns Op instance
   *
   * @example
   * const fetchUser = defineOp('fetchUser', async (ctx, userId: string) => {
   *   ctx.tag.userId(userId);
   *   return ctx.ok(await db.getUser(userId));
   * });
   */
  defineOp<Args extends unknown[], R>(
    name: string,
    fn: OpFn<T, FF, Deps, UserCtx, Args, R>,
  ): Op<T, FF, Deps, UserCtx, Args, R>;

  /**
   * Define multiple Ops at once and create an OpGroup
   *
   * Property key becomes the Op name.
   * Supports `this` binding for calling other ops in the batch.
   * Returns an OpGroup that can be prefixed and wired as a dependency.
   *
   * @param definitions - Record of Op functions or existing Ops
   * @returns OpGroup containing all defined Ops
   *
   * @example
   * export const userOps = defineOps({
   *   fetchUser,  // Existing Op
   *   updateUser: async (ctx, userId: string, data: UserData) => {
   *     await ctx.span('validate', this.fetchUser, userId);
   *     // ...
   *   },
   * });
   *
   * // Later, wire with prefix:
   * deps: { users: userOps.prefix('user') }
   */
  defineOps<
    Defs extends Record<
      string,
      Op<T, FF, Deps, UserCtx, unknown[], unknown> | OpFn<T, FF, Deps, UserCtx, unknown[], unknown>
    >,
  >(
    definitions: Defs & ThisType<OpsFromRecord<T, FF, Deps, UserCtx, Defs>>,
  ): OpGroup<T, FF, UserCtx> & OpsFromRecord<T, FF, Deps, UserCtx, Defs>;

  /**
   * Create a trace context for a new request/invocation
   *
   * Used by application code or adapters (Lambda, CF Worker, Browser, etc.)
   * to create the root context for a trace.
   *
   * Properties that were `null` in ctx config MUST be provided here.
   * Properties with defaults can optionally be overridden.
   *
   * @param params - Context params (required nulls + optional overrides)
   * @returns Root SpanContext
   *
   * @example
   * // Given ctx: {
   * //   env: null as Env,                        // required
   * //   userId: undefined as string | undefined, // optional
   * //   config: { retry: 3 },                    // has default
   * // }
   * const ctx = createTrace({
   *   env: workerEnv,         // Required - must provide
   *   userId: user?.id,       // Optional - can omit or provide
   *   // config not provided - uses default
   * });
   */
  createTrace(params: CreateTraceParams<UserCtx>): SpanContext<T, FF, Deps, UserCtx>;
}

// =============================================================================
// CREATE TRACE PARAMS
// =============================================================================

/**
 * Parameters for creating a new trace
 *
 * Combines:
 * - Required params (null-sentinel properties from ctx config)
 * - Optional params (undefined-sentinel and default properties from ctx config)
 * - Optional traceId override
 */
export type CreateTraceParams<UserCtx> = TraceContextParams<UserCtx> & {
  /** Trace ID (generated if not provided) */
  traceId?: string;
};
