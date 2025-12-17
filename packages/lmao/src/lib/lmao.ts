/**
 * Main LMAO integration - Context creation and task wrapper system
 *
 * This module ties together:
 * - Feature flags with automatic analytics
 * - Environment configuration
 * - Tag attributes with columnar storage
 * - Task wrappers with span buffers
 *
 * **V8 Hidden Class Optimization**
 *
 * This module uses prototype-based context creation to ensure stable hidden classes:
 * - SpanContextProto defines methods ONCE on a shared prototype
 * - createTraceContext() creates root contexts with Object.create(SpanContextProto)
 * - ctx.span() creates child contexts with Object.create(this) to inherit user props
 * - No object spreads ({...ctx}) - avoids hidden class pollution
 * - Direct property assignments maintain V8 optimization
 */

import {
  createResultWriter,
  createTagWriter,
  type ResultWriter,
  type TagWriter,
} from './codegen/fixedPositionWriterGenerator.js';
import { createScope, createScopeWithInheritance, type GeneratedScope } from './codegen/scopeGenerator.js';
import { type BaseSpanLogger, createSpanLogger } from './codegen/spanLoggerGenerator.js';
import { ModuleContext } from './moduleContext.js';
import type { EvaluationContext, FeatureFlagSchema, InferFeatureFlags } from './schema/defineFeatureFlags.js';
import { FeatureFlagEvaluator, type FlagColumnWriters, type FlagEvaluator } from './schema/evaluator.js';
import { mergeWithSystemSchema } from './schema/systemSchema.js';
import type { InferTagAttributes, TagAttributeSchema } from './schema/types.js';
import { createChildSpanBuffer, createNextBuffer, createSpanBuffer } from './spanBuffer.js';
import { TaskContext } from './taskContext.js';
import { getTimestampNanos } from './timestamp.js';
import { generateTraceId, type TraceId } from './traceId.js';
import type { BufferCapacityStats, SpanBuffer } from './types.js';

// Re-export TagWriter and ResultWriter types for external use
export type { ResultWriter, TagWriter } from './codegen/fixedPositionWriterGenerator.js';

/**
 * Discriminated union representing a successful operation result.
 *
 * Use with {@link ErrorResult} and {@link Result} for type-safe error handling
 * without exceptions. The `success: true` literal enables TypeScript narrowing.
 *
 * @typeParam V - The type of the success value
 *
 * @example
 * ```typescript
 * function parseUserId(input: string): Result<number, string> {
 *   const id = parseInt(input);
 *   if (isNaN(id)) {
 *     return { success: false, error: { code: 'INVALID_ID', details: 'Not a number' } };
 *   }
 *   return { success: true, value: id };
 * }
 * ```
 *
 * @see {@link SpanContext.ok} - Create success result with trace logging
 */
export type SuccessResult<V> = { success: true; value: V };

/**
 * Discriminated union representing a failed operation result.
 *
 * Use with {@link SuccessResult} and {@link Result} for type-safe error handling
 * without exceptions. The `success: false` literal enables TypeScript narrowing.
 *
 * @typeParam E - The type of the error details
 *
 * @see {@link SpanContext.err} - Create error result with trace logging
 */
export type ErrorResult<E> = { success: false; error: { code: string; details: E } };

/**
 * Union type for operation results - either success or error.
 *
 * Provides type-safe error handling without exceptions. Use with `ctx.ok()` and
 * `ctx.err()` methods in span contexts for automatic trace logging.
 *
 * @typeParam V - The type of the success value
 * @typeParam E - The type of the error details (defaults to unknown)
 *
 * @example
 * ```typescript
 * async function createUser(ctx: SpanContext, data: UserData): Promise<Result<User, ValidationError>> {
 *   if (!data.email) {
 *     return ctx.err('MISSING_EMAIL', { field: 'email' });
 *   }
 *   const user = await db.insert(data);
 *   return ctx.ok(user);
 * }
 *
 * // Type narrowing works automatically
 * const result = await createUser(ctx, data);
 * if (result.success) {
 *   console.log(result.value); // TypeScript knows this is User
 * } else {
 *   console.log(result.error.code); // 'MISSING_EMAIL'
 * }
 * ```
 */
export type Result<V, E = unknown> = SuccessResult<V> | ErrorResult<E>;

/**
 * Fluent result builder for ctx.ok()/ctx.err()
 * Allows chaining attributes and message before returning result
 *
 * Per specs/01h_entry_types_and_logging_primitives.md:
 * - Writes span-ok or span-err entry to buffer
 * - Supports .with() for attributes and individual setters
 * - Returns final result after writing to buffer
 *
 * This class uses ResultWriter internally for attribute writing while
 * maintaining proper TypeScript type narrowing via SuccessResult interface.
 */
class FluentSuccessResult<V, T extends TagAttributeSchema> implements SuccessResult<V> {
  readonly success = true as const;
  readonly value: V;
  private _writer: ResultWriter<T>;

  constructor(buffer: SpanBuffer<T>, value: V, schema: T) {
    this.value = value;

    // Overwrite the pre-initialized span-exception with span-ok
    buffer.operations[1] = ENTRY_TYPE_SPAN_OK;

    // Write timestamp (nanoseconds since epoch)
    buffer.timestamps[1] = getTimestampNanos();

    // Create ResultWriter for fluent attribute setting (writes to position 1)
    // Type assertion needed because createResultWriter expects SpanBuffer (non-generic)
    this._writer = createResultWriter(schema, buffer as unknown as SpanBuffer, value, false);

    // Note: writeIndex is NOT incremented - row 1 is reserved, events start at row 2
  }

  /**
   * Set multiple attributes on the result entry
   * Example: ctx.ok(result).with({ userId: 'u1', operation: 'CREATE' })
   */
  with(attributes: Partial<InferTagAttributes<T>>): this {
    this._writer.with(attributes);
    return this;
  }

  /**
   * Set a message on the result entry
   * Example: ctx.ok(result).message('User created successfully')
   */
  message(text: string): this {
    // Use the unified message column via the writer's message setter
    // ResultWriter generates a message() method from the systemSchema
    const writer = this._writer as ResultWriter<T, V, never> & { message?: (v: string) => unknown };
    if (typeof writer.message === 'function') {
      writer.message(text);
    }
    return this;
  }

  /**
   * Set the source code line number for this result entry
   * Example: ctx.ok(result).line(42)
   */
  line(lineNumber: number): this {
    // Use the writer's setter if available, otherwise fallback
    const writer = this._writer as ResultWriter<T, V, never> & { lineNumber?: (v: number) => unknown };
    if (typeof writer.lineNumber === 'function') {
      writer.lineNumber(lineNumber);
    }
    return this;
  }
}

/**
 * Fluent error result with chaining support
 *
 * Uses ResultWriter internally for attribute writing while
 * maintaining proper TypeScript type narrowing via ErrorResult interface.
 */
class FluentErrorResult<E, T extends TagAttributeSchema> implements ErrorResult<E> {
  readonly success = false as const;
  readonly error: { code: string; details: E };
  private _writer: ResultWriter<T>;

  constructor(buffer: SpanBuffer<T>, code: string, details: E, schema: T) {
    this.error = { code, details };

    // Overwrite the pre-initialized span-exception with span-err
    buffer.operations[1] = ENTRY_TYPE_SPAN_ERR;

    // Write timestamp (nanoseconds since epoch)
    buffer.timestamps[1] = getTimestampNanos();

    // Create ResultWriter for fluent attribute setting (writes to position 1)
    // Type assertion needed because createResultWriter expects SpanBuffer (non-generic)
    this._writer = createResultWriter(schema, buffer as unknown as SpanBuffer, details, true);

    // Write error code using the writer if available
    const writer = this._writer as ResultWriter<T, never, E> & { errorCode?: (v: string) => unknown };
    if (typeof writer.errorCode === 'function') {
      writer.errorCode(code);
    }

    // Note: writeIndex is NOT incremented - row 1 is reserved, events start at row 2
  }

  /**
   * Set multiple attributes on the result entry
   * Example: ctx.err('ERROR', details).with({ userId: 'u1' })
   */
  with(attributes: Partial<InferTagAttributes<T>>): this {
    this._writer.with(attributes);
    return this;
  }

  /**
   * Set a message on the result entry
   * Example: ctx.err('ERROR', details).message('Operation failed')
   */
  message(text: string): this {
    // Use the unified message column via the writer's message setter
    // ResultWriter generates a message() method from the systemSchema
    const writer = this._writer as ResultWriter<T, never, E> & { message?: (v: string) => unknown };
    if (typeof writer.message === 'function') {
      writer.message(text);
    }
    return this;
  }

  /**
   * Set the source code line number for this result entry
   * Example: ctx.err('ERROR', details).line(42)
   */
  line(lineNumber: number): this {
    // Use the writer's setter if available
    const writer = this._writer as ResultWriter<T, never, E> & { lineNumber?: (v: number) => unknown };
    if (typeof writer.lineNumber === 'function') {
      writer.lineNumber(lineNumber);
    }
    return this;
  }
}

/**
 * Check if capacity should be tuned based on usage patterns
 *
 * Per specs/01b_columnar_buffer_architecture.md:
 * - Increase if >15% writes overflow
 * - Decrease if <5% writes overflow with many buffers
 * - Bounded growth: 8-1024 entries
 */
function shouldTuneCapacity(stats: BufferCapacityStats): boolean {
  const minSamples = 100; // Need enough data
  if (stats.totalWrites < minSamples) return false;

  const overflowRatio = stats.overflowWrites / stats.totalWrites;

  // Increase if >15% writes overflow
  if (overflowRatio > 0.15 && stats.currentCapacity < 1024) {
    const newCapacity = Math.min(stats.currentCapacity * 2, 1024);

    // TODO: Use system tracer for self-tracing capacity tuning events

    stats.currentCapacity = newCapacity;
    resetStats(stats);
    return true;
  }

  // Decrease if <5% writes overflow and we have many buffers
  if (overflowRatio < 0.05 && stats.totalBuffersCreated >= 10 && stats.currentCapacity > 8) {
    const newCapacity = Math.max(8, stats.currentCapacity / 2);

    // TODO: Use system tracer for self-tracing capacity tuning events

    stats.currentCapacity = newCapacity;
    resetStats(stats);
    return true;
  }

  return false;
}

/**
 * Reset stats after capacity tuning
 */
function resetStats(stats: BufferCapacityStats): void {
  stats.totalWrites = 0;
  stats.overflowWrites = 0;
  stats.totalBuffersCreated = 0;
}

/**
 * Request context created at the HTTP request boundary.
 *
 * This is the root context for a distributed trace, created once per incoming
 * request via {@link createRequestContext}. It provides:
 *
 * - **Trace correlation**: Unique `traceId` links all spans in a request
 * - **Feature flags**: Type-safe flag access with automatic analytics tracking
 * - **Environment config**: Application configuration (API keys, endpoints, etc.)
 * - **Time anchor**: High-precision timestamp base for microsecond accuracy
 *
 * The time anchor is created ONCE per request - all subsequent timestamps use
 * efficient delta calculation from this anchor, avoiding repeated system calls.
 *
 * @typeParam FF - Feature flag schema type for type-safe flag access
 * @typeParam Env - Environment configuration type
 *
 * @example
 * ```typescript
 * // In your request handler middleware
 * const ctx = createRequestContext(
 *   { requestId: req.headers['x-request-id'], userId: req.user?.id },
 *   featureFlags,
 *   flagEvaluator,
 *   { apiUrl: process.env.API_URL }
 * );
 *
 * // Access feature flags with full type safety
 * if (ctx.ff.newCheckout) {
 *   // Flag value is typed based on schema
 * }
 *
 * // Access environment config
 * const response = await fetch(ctx.env.apiUrl);
 * ```
 *
 * @see {@link createRequestContext} - Factory function to create this context
 * @see {@link SpanContext} - Extended context with logging capabilities
 */
export interface RequestContext<FF extends FeatureFlagSchema = FeatureFlagSchema, Env = Record<string, unknown>> {
  /** Unique identifier for this HTTP request (e.g., from load balancer or UUID) */
  readonly requestId: string;

  /** Optional user identifier for user-scoped operations and flag targeting */
  readonly userId?: string;

  /** Unique trace ID linking all spans in this request's distributed trace */
  readonly traceId: TraceId;

  /**
   * Feature flag evaluator with typed flag access.
   *
   * Access flags as properties (e.g., `ctx.ff.myFlag`) for type-safe values.
   * Analytics are automatically tracked when flags are first accessed.
   */
  readonly ff: FeatureFlagEvaluator<FF> & InferFeatureFlags<FF>;

  /**
   * Environment configuration object.
   * Contains application-level config like API endpoints, secrets, etc.
   */
  readonly env: Env;
}

/**
 * System options for trace context creation.
 *
 * @typeParam FF - Feature flag schema type
 * @typeParam Env - Environment configuration type
 */
export interface TraceContextSystemOpts<FF extends FeatureFlagSchema, Env extends Record<string, unknown>> {
  /** Feature flag configuration */
  ff: {
    /** Feature flag schema created with `defineFeatureFlags()` */
    schema: { schema: FF };
    /** Backend evaluator that resolves flag values */
    evaluator: FlagEvaluator;
  };
  /** Environment configuration object */
  env: Env;
  /** Optional trace ID (auto-generated if not provided) */
  traceId?: TraceId;
}

/**
 * Creates a request context at the HTTP request boundary.
 *
 * Call this once at the start of each incoming request to establish:
 * - A unique trace ID for distributed tracing
 * - A high-precision time anchor for microsecond timestamps
 * - Feature flag evaluation with automatic analytics
 * - Type-safe environment configuration access
 *
 * The returned context is passed to module tasks via {@link ModuleContextBuilder.task}
 * wrappers, which extend it with logging capabilities.
 *
 * @typeParam FF - Feature flag schema type (inferred from featureFlagSchema)
 * @typeParam Env - Environment configuration type (inferred from environmentConfig)
 *
 * @param params - Request identification parameters
 * @param params.requestId - Unique request identifier (from load balancer, UUID, etc.)
 * @param params.userId - Optional user ID for flag targeting and trace correlation
 * @param featureFlagSchema - Feature flag schema created with `defineFeatureFlags()`
 * @param evaluator - Backend evaluator that resolves flag values
 * @param environmentConfig - Application configuration object
 *
 * @returns A RequestContext for use with module task wrappers
 *
 * @example
 * ```typescript
 * // Define your feature flags
 * const flags = defineFeatureFlags({
 *   newCheckout: S.boolean(),
 *   maxItems: S.number(),
 * });
 *
 * // Create evaluator (e.g., LaunchDarkly adapter)
 * const evaluator: FlagEvaluator = {
 *   evaluate: (name, context) => launchDarkly.variation(name, context),
 * };
 *
 * // In request handler
 * app.use((req, res, next) => {
 *   req.ctx = createRequestContext(
 *     { requestId: req.headers['x-request-id'], userId: req.user?.id },
 *     flags,
 *     evaluator,
 *     { apiUrl: process.env.API_URL, maxRetries: 3 }
 *   );
 *   next();
 * });
 * ```
 *
 * @see {@link RequestContext} - The returned context type
 * @see {@link createModuleContext} - Create module context with task wrappers
 * @see {@link createTraceContext} - Newer prototype-based API
 */
export function createRequestContext<FF extends FeatureFlagSchema, Env extends Record<string, unknown>>(
  params: {
    requestId: string;
    userId?: string;
    [key: string]: unknown;
  },
  featureFlagSchema: { schema: FF },
  evaluator: FlagEvaluator,
  environmentConfig: Env,
): RequestContext<FF, Env> {
  // Use the new createTraceContext internally
  return createTraceContext<FF, Env>(
    {
      ff: { schema: featureFlagSchema, evaluator },
      env: environmentConfig,
    },
    params,
  );
}

// =============================================================================
// Prototype-based Context System for V8 Hidden Class Optimization
// =============================================================================
// Per specs/01c_context_flow_and_task_wrappers.md:
// - Methods are defined ONCE on a shared prototype
// - Object.create() is used for inheritance (no object spreads)
// - Properties are assigned directly for stable hidden classes
// =============================================================================

/**
 * Internal symbol to mark contexts as prototype-based
 * Used for type guards and instanceof-like checks
 */
const TRACE_CONTEXT_MARKER = Symbol.for('lmao.TraceContext');

/**
 * Base prototype for all trace contexts.
 *
 * Contains shared properties that all contexts inherit.
 * This is used internally and should not be accessed directly.
 */
interface TraceContextBase<FF extends FeatureFlagSchema = FeatureFlagSchema, Env = Record<string, unknown>> {
  /** Marker for prototype chain detection */
  readonly [TRACE_CONTEXT_MARKER]: true;
  /** Trace ID for this context */
  traceId: TraceId;
  /** Feature flag evaluator */
  ff: FeatureFlagEvaluator<FF> & InferFeatureFlags<FF>;
  /** Environment configuration */
  env: Env;
  /** Allow dynamic user properties */
  [key: string]: unknown;
}

/**
 * The shared prototype object for TraceContext.
 *
 * Per V8 optimization guidelines:
 * - Define methods ONCE on prototype, not per-instance
 * - Use Object.create() for inheritance chains
 * - Avoid object spreads which break hidden classes
 */
const TraceContextProto: TraceContextBase = {
  [TRACE_CONTEXT_MARKER]: true,
  traceId: undefined as unknown as TraceId,
  ff: undefined as unknown as FeatureFlagEvaluator<FeatureFlagSchema> & InferFeatureFlags<FeatureFlagSchema>,
  env: undefined as unknown as Record<string, unknown>,
};

/**
 * Creates a trace context using prototype-based inheritance for V8 optimization.
 *
 * This is the recommended API for creating request contexts. It uses:
 * - Object.create() for prototype chain (stable hidden classes)
 * - Direct property assignment (no object spreads)
 * - Shared prototype methods (single allocation)
 *
 * User properties are assigned directly to the context and are accessible
 * on child spans through prototype inheritance.
 *
 * @typeParam FF - Feature flag schema type
 * @typeParam Env - Environment configuration type
 * @typeParam UserProps - User-defined properties type
 *
 * @param systemOpts - System configuration (ff, env, optional traceId)
 * @param userProps - User-defined properties (requestId, userId, etc.)
 *
 * @returns A RequestContext with user props accessible directly
 *
 * @example
 * ```typescript
 * const ctx = createTraceContext(
 *   {
 *     ff: { schema: featureFlags, evaluator: flagEvaluator },
 *     env: { apiUrl: 'https://api.example.com' },
 *   },
 *   { requestId: 'req-123', userId: 'user-456', customProp: 'value' }
 * );
 *
 * // User props are directly accessible
 * console.log(ctx.requestId); // 'req-123'
 * console.log(ctx.customProp); // 'value'
 * ```
 *
 * @see {@link createRequestContext} - Backward-compatible wrapper
 */
export function createTraceContext<
  FF extends FeatureFlagSchema,
  Env extends Record<string, unknown>,
  UserProps extends Record<string, unknown> = { requestId: string; userId?: string },
>(systemOpts: TraceContextSystemOpts<FF, Env>, userProps?: UserProps): RequestContext<FF, Env> & UserProps {
  // Create evaluation context from user props
  const evaluationContext: EvaluationContext = {
    userId: (userProps as Record<string, unknown>)?.userId as string | undefined,
    requestId: (userProps as Record<string, unknown>)?.requestId as string | undefined,
  };

  // Create feature flag evaluator (buffer will be set by task wrapper)
  const ffEvaluator = new FeatureFlagEvaluator(
    systemOpts.ff.schema.schema,
    evaluationContext,
    systemOpts.ff.evaluator,
    undefined, // Column writers set later when span context is created
  ) as FeatureFlagEvaluator<FF> & InferFeatureFlags<FF>;

  // Create context with prototype inheritance
  // Use type assertion for initial assignment (properties are readonly after construction)
  const ctx = Object.create(TraceContextProto) as TraceContextBase<FF, Env> & UserProps;

  // Assign system properties directly (stable hidden class)
  ctx.traceId = systemOpts.traceId ?? generateTraceId();
  ctx.ff = ffEvaluator;
  ctx.env = systemOpts.env;

  // Assign user properties directly (accessible via prototype chain in children)
  if (userProps) {
    Object.assign(ctx, userProps);
  }

  // Return as readonly RequestContext (properties are readonly after construction)
  return ctx as unknown as RequestContext<FF, Env> & UserProps;
}

// =============================================================================
// SpanContext Prototype for V8 Hidden Class Optimization
// =============================================================================

/**
 * Internal type for mutable SpanContext during construction.
 * After construction, the context is returned as readonly SpanContext.
 */
interface MutableSpanContext<
  T extends TagAttributeSchema,
  FF extends FeatureFlagSchema,
  Env = Record<string, unknown>,
> {
  [TRACE_CONTEXT_MARKER]: true;
  traceId: TraceId;
  ff: FeatureFlagEvaluator<FF> & InferFeatureFlags<FF>;
  env: Env;
  tag: TagWriter<T>;
  log: SpanLogger<T>;
  _buffer: SpanBuffer<T>;
  _schema: T;
  _spanLogger: BaseSpanLogger<T>;
  buffer: SpanBuffer<T>;
  scope: (attributes: Partial<InferTagAttributes<T>>) => void;
  ok: <V>(value: V) => FluentSuccessResult<V, T>;
  err: <E>(code: string, error: E) => FluentErrorResult<E, T>;
  span: <R>(name: string, fn: (ctx: SpanContext<T, FF, Env>) => Promise<R>, line?: number) => Promise<R>;
  [key: string]: unknown;
}

/**
 * Create the SpanContext prototype with shared methods.
 *
 * These methods are defined ONCE and inherited by all span contexts,
 * avoiding per-instance function allocations.
 *
 * @internal
 */
function createSpanContextProto<
  T extends TagAttributeSchema,
  FF extends FeatureFlagSchema,
  Env = Record<string, unknown>,
>(schemaOnly: T, taskContext: TaskContext): object {
  return {
    [TRACE_CONTEXT_MARKER]: true,

    // Buffer getter - returns _buffer
    get buffer(): SpanBuffer<T> {
      return (this as MutableSpanContext<T, FF, Env>)._buffer;
    },

    // Scope method - delegates to spanLogger._setScope
    scope(this: MutableSpanContext<T, FF, Env>, attributes: Partial<InferTagAttributes<T>>): void {
      this._spanLogger._setScope(attributes);
    },

    // Ok method - creates FluentSuccessResult
    ok<V>(this: MutableSpanContext<T, FF, Env>, value: V): FluentSuccessResult<V, T> {
      return new FluentSuccessResult<V, T>(this._buffer, value, this._schema);
    },

    // Err method - creates FluentErrorResult
    err<E>(this: MutableSpanContext<T, FF, Env>, code: string, error: E): FluentErrorResult<E, T> {
      return new FluentErrorResult<E, T>(this._buffer, code, error, this._schema);
    },

    // Span method - creates child span with Object.create(this) for inheritance
    span<R>(
      this: MutableSpanContext<T, FF, Env>,
      childName: string,
      childFn: (ctx: SpanContext<T, FF, Env>) => Promise<R>,
      line?: number,
    ): Promise<R> {
      // Create child span buffer with Arrow builders
      const childBuffer = createChildSpanBuffer(this._buffer, taskContext);

      // Write span-start for child span (row 0) and pre-initialize span-end (row 1)
      writeSpanStart(childBuffer, childName);

      // Write line number to row 0 if provided
      if (line !== undefined) {
        childBuffer.lineNumber(0, line);
      }

      // Inherit scoped values from parent span via Scope instance
      // Per specs/01i_span_scope_attributes.md - child spans inherit parent's scoped attributes
      const parentScopeValues = this._spanLogger._getScope()._getScopeValues();

      // Create child context with its own logger (with inherited scope values)
      const childLogger = createSpanLoggerWithScope(schemaOnly, childBuffer, parentScopeValues);

      // Create tag writer for child span attributes (writes to row 0)
      const childTagAPI = createTagWriter(schemaOnly, childBuffer);

      // Create a new feature flag evaluator bound to the CHILD buffer.
      // This ensures ff-access/ff-usage entries are logged to the correct span.
      // Type assertion needed because withBuffer expects SpanBuffer (non-generic)
      const childFf = this.ff.withBuffer(childBuffer as unknown as SpanBuffer) as FeatureFlagEvaluator<FF> &
        InferFeatureFlags<FF>;

      // Use Object.create(this) for prototype inheritance
      // Child inherits all user properties from parent via prototype chain
      const childContext = Object.create(this) as MutableSpanContext<T, FF, Env>;

      // Assign child-specific properties directly (stable hidden class)
      childContext.ff = childFf;
      childContext.tag = childTagAPI;
      childContext.log = childLogger as SpanLogger<T>;
      childContext._buffer = childBuffer;
      childContext._spanLogger = childLogger;

      // Execute child span with exception handling and return the promise directly
      return (async () => {
        try {
          return await childFn(childContext as unknown as SpanContext<T, FF, Env>);
        } catch (error) {
          // Write span-exception to row 1 (fixed layout)
          // Row 1 was pre-initialized as exception, just update timestamp
          childBuffer.timestamps[1] = getTimestampNanos();

          // Write exception details to row 1
          const errorMessage = error instanceof Error ? error.message : String(error);
          const errorStack = error instanceof Error ? error.stack : undefined;

          childBuffer.message(1, errorMessage);
          if (errorStack) {
            childBuffer.exceptionStack(1, errorStack);
          }

          // Re-throw to propagate
          throw error;
        }
      })();
    },
  };
}

/**
 * Fluent builder for log entries - supports .line() chaining
 *
 * Returned by ctx.log.info(), ctx.log.warn(), etc. to enable:
 * ctx.log.info('message').line(42)
 */
export interface FluentLogEntry {
  /**
   * Set the source code line number for this log entry.
   *
   * Per specs/01c_context_flow_and_task_wrappers.md "Line Number System":
   * - TypeScript transformer injects these calls at compile time
   * - No runtime overhead - just a method call with literal number
   *
   * @param lineNumber - Source line number (0-65535)
   *
   * @example
   * ctx.log.info('Processing user').line(42);
   */
  line(lineNumber: number): void;
}

/**
 * Span logger context - provides logging API for spans
 *
 * This is what's available as `ctx.log` in task wrappers.
 * For span attributes, use `ctx.tag` directly (not `ctx.log.tag`).
 * For scoped attributes, use `ctx.scope` directly (not `ctx.log.scope`).
 *
 * Logging methods return `this` for fluent chaining of attribute setters:
 * @example
 * ```typescript
 * ctx.log.info('Processing user').userId('u123');
 * ctx.log.warn('Slow operation');
 * ```
 */
/**
 * SpanLogger type - just an alias for BaseSpanLogger which already includes
 * schema-specific setter methods via ColumnWriter<T>.
 */
export type SpanLogger<T extends TagAttributeSchema> = BaseSpanLogger<T>;

/**
 * Span context provided to task functions
 *
 * This is what's provided to task functions and child spans.
 * Extends RequestContext with:
 * - `tag` - Chainable span attribute API (writes to row 0)
 * - `log` - Logging API (info, debug, warn, error)
 * - `scope` - Set scoped attributes that propagate to all entries
 * - `ok`/`err` - Result helpers with fluent API
 * - `span` - Child span creation
 *
 * @example
 * ```typescript
 * const createUser = task('create-user', async (ctx, userData) => {
 *   // Scoped attributes (propagate to all log entries)
 *   ctx.scope({ userId: userData.id });
 *
 *   // Span attributes (writes to span-start row)
 *   ctx.tag.operation('INSERT');
 *
 *   // Logging (includes scoped attributes)
 *   ctx.log.info('Creating user').line(42);
 *
 *   // Results
 *   return ctx.ok(user);
 * });
 * ```
 */
export interface SpanContext<T extends TagAttributeSchema, FF extends FeatureFlagSchema, Env = Record<string, unknown>>
  extends RequestContext<FF, Env> {
  /**
   * Chainable span attribute API
   *
   * Writes attributes to row 0 (span-start) with overwrite semantics.
   * All methods return the tag API for chaining.
   *
   * @example
   * ctx.tag.userId('u1').requestId('r1').operation('INSERT');
   * ctx.tag.with({ userId: 'u1', requestId: 'r1' });
   */
  readonly tag: TagWriter<T>;

  /**
   * Logging API for structured log messages
   *
   * Use for logging events during span execution.
   * Logs are appended to the buffer (row 2+).
   *
   * @example
   * ctx.log.info('Processing request').line(42);
   */
  readonly log: SpanLogger<T>;

  /**
   * Set scoped attributes that auto-propagate to all subsequent log entries
   *
   * Scoped attributes are automatically included in all log entries
   * and inherited by child spans.
   *
   * @param attributes - Attributes to scope to this span
   *
   * @example
   * ctx.scope({ requestId: req.id, userId: req.user?.id });
   * ctx.log.info('Processing'); // Includes requestId and userId
   */
  scope(attributes: Partial<InferTagAttributes<T>>): void;

  /**
   * Create a success result with optional attributes
   *
   * Writes span-ok entry to row 1 (span-end).
   * Supports fluent chaining with .with() and .message().
   *
   * @param value - The success value
   * @returns Fluent result builder
   *
   * @example
   * return ctx.ok(user).with({ userId: user.id });
   */
  ok<V>(value: V): FluentSuccessResult<V, T>;

  /**
   * Create an error result with optional attributes
   *
   * Writes span-err entry to row 1 (span-end).
   * Supports fluent chaining with .with() and .message().
   *
   * @param code - Error code string
   * @param error - Error details
   * @returns Fluent result builder
   *
   * @example
   * return ctx.err('NOT_FOUND', { userId }).message('User not found');
   */
  err<E>(code: string, error: E): FluentErrorResult<E, T>;

  /**
   * Create a child span with its own buffer
   *
   * Child spans inherit scoped attributes from the parent.
   * The child function receives a new SpanContext.
   * Supports fluent .line() for source code location.
   *
   * @param name - Child span name
   * @param fn - Async function to execute in child span
   * @returns Fluent span builder with .line() support
   *
   * @example
   * const result = await ctx.span('validate', async (childCtx) => {
   *   childCtx.tag.step('validation');
   *   return childCtx.ok({ valid: true });
   * }).line(42);
   */
  span<R>(name: string, fn: (ctx: SpanContext<T, FF, Env>) => Promise<R>, line?: number): Promise<R>;

  /**
   * The underlying SpanBuffer for this context.
   *
   * Useful for Arrow table conversion after task completion.
   * The buffer contains all trace data written during this span's execution.
   *
   * @example
   * ```typescript
   * const result = await myTask(requestCtx, args);
   * const table = convertToArrowTable(ctx.buffer);
   * ```
   */
  readonly buffer: SpanBuffer;
}

/**
 * Type signature for task functions wrapped by module contexts.
 *
 * Task functions receive a {@link SpanContext} as their first argument,
 * followed by any custom arguments. They must return a Promise.
 *
 * @typeParam Args - Tuple type of additional arguments after ctx
 * @typeParam Result - Return type of the task (unwrapped from Promise)
 * @typeParam T - Tag attribute schema for the module
 * @typeParam FF - Feature flag schema
 * @typeParam Env - Environment configuration type
 *
 * @example
 * ```typescript
 * // A task function with typed arguments
 * const myTaskFn: TaskFunction<[string, number], User, typeof schema, typeof flags> =
 *   async (ctx, userId, limit) => {
 *     ctx.log.tag.userId(userId).limit(limit);
 *     return await fetchUser(userId);
 *   };
 * ```
 */
export type TaskFunction<
  Args extends unknown[],
  Result,
  T extends TagAttributeSchema,
  FF extends FeatureFlagSchema,
  Env = Record<string, unknown>,
> = (ctx: SpanContext<T, FF, Env>, ...args: Args) => Promise<Result>;

/**
 * Builder interface returned by {@link createModuleContext}.
 *
 * Provides the `task()` method to wrap functions with automatic span
 * creation, logging context, and trace correlation.
 *
 * @typeParam T - Tag attribute schema for this module
 * @typeParam FF - Feature flag schema
 * @typeParam Env - Environment configuration type
 *
 * @example
 * ```typescript
 * const userModule = createModuleContext({
 *   moduleMetadata: { gitSha: 'abc123', filePath: 'user/service.ts', moduleName: 'user' },
 *   tagAttributes: userSchema,
 * });
 *
 * // Create wrapped task
 * const getUser = userModule.task('getUser', async (ctx, userId: string) => {
 *   ctx.tag.userId(userId);
 *   return await db.findUser(userId);
 * });
 *
 * // Call the task with request context
 * const user = await getUser(requestCtx, 'u123');
 * ```
 *
 * @see {@link createModuleContext} - Factory function
 */
export interface ModuleContextBuilder<
  T extends TagAttributeSchema,
  FF extends FeatureFlagSchema,
  Env = Record<string, unknown>,
> {
  /**
   * Wraps a function with automatic span tracking and logging context.
   *
   * The returned function:
   * - Creates a span buffer for the execution
   * - Writes span-start on entry, span-end on completion
   * - Handles exceptions with span-exception logging
   * - Provides full {@link SpanContext} to the wrapped function
   *
   * @typeParam Args - Additional arguments after the context
   * @typeParam Result - Return type of the wrapped function
   *
   * @param name - Span name (e.g., 'getUser', 'processPayment')
   * @param fn - Task function to wrap
   * @returns Wrapped function that accepts RequestContext + args
   *
   * @example
   * ```typescript
   * const processOrder = orderModule.task('processOrder',
   *   async (ctx, orderId: string, options: OrderOptions) => {
   *     ctx.log.scope({ orderId });
   *     ctx.tag.status('processing');
   *     // ... implementation
   *     return ctx.ok({ processed: true });
   *   }
   * );
   *
   * // Usage
   * const result = await processOrder(requestCtx, 'order-123', { rush: true });
   * ```
   */
  task<Args extends unknown[], Result>(
    name: string,
    fn: TaskFunction<Args, Result, T, FF, Env>,
    line?: number,
  ): (ctx: RequestContext<FF, Env>, ...args: Args) => Promise<Result>;

  /**
   * Buffer capacity stats for this module (for testing and monitoring).
   *
   * Exposes the self-tuning capacity statistics:
   * - `currentCapacity`: Current tuned buffer capacity
   * - `totalWrites`: Total writes since last tuning
   * - `overflowWrites`: Writes that caused buffer overflow
   * - `totalBuffersCreated`: Number of buffers created
   */
  readonly spanBufferCapacityStats: import('@smoothbricks/arrow-builder').BufferCapacityStats;
}

/**
 * Entry type constants for trace event categorization.
 *
 * These numeric codes are stored in the `operations` Uint8Array column,
 * enabling efficient filtering and aggregation in columnar storage.
 * Each constant maps to a human-readable name in {@link ENTRY_TYPE_NAMES}.
 *
 * @see {@link ENTRY_TYPE_NAMES} for string representations
 */

// =============================================================================
// Entry Type Constants
// =============================================================================
// Ordered by frequency/importance:
// 1-4: Span lifecycle (most common - every span has start + end)
// 5-9: Logging levels (ordered by verbosity: trace < debug < info < warn < error)
// 10-11: Feature flags (least common)
// =============================================================================

/** Span start event - written at row 0 when a span begins */
export const ENTRY_TYPE_SPAN_START = 1;

/** Span success completion - written to row 1 via ctx.ok() */
export const ENTRY_TYPE_SPAN_OK = 2;

/** Span error completion - written to row 1 via ctx.err() */
export const ENTRY_TYPE_SPAN_ERR = 3;

/** Span exception - written to row 1 when an unhandled exception occurs */
export const ENTRY_TYPE_SPAN_EXCEPTION = 4;

/** Trace log level entry - via ctx.log.trace() (most verbose) */
export const ENTRY_TYPE_TRACE = 5;

/** Debug log level entry - via ctx.log.debug() */
export const ENTRY_TYPE_DEBUG = 6;

/** Info log level entry - via ctx.log.info() */
export const ENTRY_TYPE_INFO = 7;

/** Warning log level entry - via ctx.log.warn() */
export const ENTRY_TYPE_WARN = 8;

/** Error log level entry - via ctx.log.error() */
export const ENTRY_TYPE_ERROR = 9;

/** Feature flag access event - logged when a flag value is first read in a span */
export const ENTRY_TYPE_FF_ACCESS = 10;

/** Feature flag usage event - logged when a flag influences a code path decision */
export const ENTRY_TYPE_FF_USAGE = 11;

/**
 * Human-readable names for entry types, indexed by entry type code.
 *
 * Used for Arrow dictionary encoding and display purposes.
 * Index 0 is unused (entry types start at 1).
 *
 * @example
 * ```typescript
 * const entryType = buffer.operations[index];
 * const typeName = ENTRY_TYPE_NAMES[entryType]; // e.g., 'span-ok'
 * ```
 */
export const ENTRY_TYPE_NAMES = [
  '', // 0 - unused
  'span-start', // 1
  'span-ok', // 2
  'span-err', // 3
  'span-exception', // 4
  'trace', // 5 (most verbose)
  'debug', // 6
  'info', // 7
  'warn', // 8
  'error', // 9
  'ff-access', // 10
  'ff-usage', // 11
] as const;

/**
 * Create column writers for feature flag analytics
 * Writes to TypedArray columnar buffers in memory (hot path)
 *
 * Per specs/01b1_buffer_performance_optimizations.md:
 * - String interning for category columns
 * - Direct TypedArray writes (no allocations)
 */
function createFlagColumnWriters<T extends TagAttributeSchema>(buffer: SpanBuffer<T>): FlagColumnWriters {
  return {
    writeEntryType(type: 'ff-access' | 'ff-usage'): void {
      // Write entry type code to operation column
      const typeCode = type === 'ff-access' ? ENTRY_TYPE_FF_ACCESS : ENTRY_TYPE_FF_USAGE;
      buffer.operations[buffer.writeIndex] = typeCode;

      // Write timestamp (nanoseconds since epoch)
      buffer.timestamps[buffer.writeIndex] = getTimestampNanos();
    },

    writeFfName(name: string): void {
      // Write to unified message column (feature flag name)
      buffer.message(buffer.writeIndex, name);
    },

    writeFfValue(value: string | number | boolean | null): void {
      // Write to ffValue column (raw string, no interning)
      // Cast to record for dynamic access to internal column storage
      const bufferRecord = buffer as unknown as Record<string, unknown>;
      const column = bufferRecord['ffValue_values'] as string[] | undefined;
      if (column && Array.isArray(column)) {
        const strValue = value === null ? 'null' : String(value);
        column[buffer.writeIndex] = strValue;
      }
    },

    // writeAction and writeOutcome removed - these should be user-defined scope attributes
    // The evaluator generator still calls these but they're no-ops
    writeAction(_action?: string): void {
      // No-op: action should be a user-defined scope attribute, not a system column
    },

    writeOutcome(_outcome?: string): void {
      // No-op: outcome should be a user-defined scope attribute, not a system column
    },

    writeContextAttributes(_context: EvaluationContext): void {
      // userId and requestId removed - these should be user-defined scope attributes
      // Just increment write index to complete the entry
      buffer.writeIndex++;
    },
  };
}

/**
 * Get buffer with space function type
 * @deprecated Use createNextBuffer directly via ColumnWriter overflow handling
 */
export type GetBufferWithSpaceFn = (buffer: SpanBuffer) => { buffer: SpanBuffer; didOverflow: boolean };

/**
 * Find buffer with space, creating chained buffer if needed
 *
 * Per specs/01b_columnar_buffer_architecture.md:
 * - Buffer chaining handles overflow gracefully
 * - Tracks overflow stats for self-tuning
 * - CPU branch predictor friendly
 *
 * @deprecated Use createNextBuffer directly via ColumnWriter overflow handling.
 * This function is kept for backward compatibility with library.ts.
 */
export function getBufferWithSpace(inputBuffer: SpanBuffer): { buffer: SpanBuffer; didOverflow: boolean } {
  const originalBuffer = inputBuffer;
  let currentBuffer = inputBuffer;
  let didOverflow = false;

  // Find buffer with space (CPU branch predictor friendly)
  while (currentBuffer.writeIndex >= currentBuffer.capacity) {
    if (!currentBuffer.next) {
      currentBuffer.next = createNextBuffer(currentBuffer);
    }
    // Type assertion: createNextBuffer always returns SpanBuffer
    currentBuffer = currentBuffer.next as SpanBuffer;
    didOverflow = true;
  }

  // Track stats for self-tuning
  const stats = originalBuffer.task.module.spanBufferCapacityStats;
  stats.totalWrites++;
  if (didOverflow) {
    stats.overflowWrites++;
  }

  // Check if capacity should be tuned
  shouldTuneCapacity(stats);

  return { buffer: currentBuffer, didOverflow };
}

/**
 * Write span-start entry to buffer at row 0 (fixed layout)
 * Pre-initialize row 1 as span-exception (will be overwritten by ok/err)
 * Set writeIndex to 2 (events start after reserved rows)
 *
 * Per specs/01h_entry_types_and_logging_primitives.md:
 * - Row 0: span-start (written here)
 * - Row 1: span-end (pre-initialized as exception, overwritten by ok/err)
 * - Row 2+: events (ctx.log.* appends here)
 */
function writeSpanStart<T extends TagAttributeSchema>(buffer: SpanBuffer<T>, spanName: string): void {
  // Row 0: span-start (fixed layout)
  buffer.operations[0] = ENTRY_TYPE_SPAN_START;
  buffer.timestamps[0] = getTimestampNanos();
  buffer.message(0, spanName); // Unified message column for span name

  // Row 1: pre-initialize as span-exception (will be overwritten on ok/err)
  buffer.operations[1] = ENTRY_TYPE_SPAN_EXCEPTION;
  buffer.timestamps[1] = 0n; // Will be set on completion

  // Events start at row 2
  buffer.writeIndex = 2;
}

/**
 * Helper function to create a SpanLogger with inherited scope values.
 *
 * Uses the imported createSpanLogger from spanLoggerGenerator.js and
 * handles scope creation and inheritance.
 *
 * @param schema - Tag attribute schema with field definitions
 * @param buffer - SpanBuffer to write entries to (per-span instance)
 * @param inheritedScopeValues - Scoped values inherited from parent span (from _getScopeValues())
 * @returns SpanLogger with typed methods matching schema
 */
function createSpanLoggerWithScope<T extends TagAttributeSchema>(
  schema: T,
  buffer: SpanBuffer<T>,
  inheritedScopeValues?: Record<string, unknown>,
): BaseSpanLogger<T> {
  // Create Scope instance (separate from column storage)
  const scopeInstance: GeneratedScope =
    inheritedScopeValues && Object.keys(inheritedScopeValues).length > 0
      ? createScopeWithInheritance(schema, inheritedScopeValues)
      : createScope(schema);

  // Create the SpanLogger using the imported function
  const logger = createSpanLogger(schema, buffer, scopeInstance, createNextBuffer);

  // If scoped values were inherited, pre-fill the buffer
  // The _setScope() method will update both the Scope instance and pre-fill buffer
  if (inheritedScopeValues && Object.keys(inheritedScopeValues).length > 0) {
    logger._setScope(inheritedScopeValues as Partial<InferTagAttributes<T>>);
  }

  return logger as BaseSpanLogger<T>;
}

/**
 * Creates a module context with typed tag attributes and task wrappers.
 *
 * This is the primary entry point for instrumenting application modules.
 * Each module calls this once to create a context builder, then uses
 * the `task()` method to wrap individual operations with automatic tracing.
 *
 * **What it does:**
 * - Merges your tag attributes with system schema (timestamps, entry types, etc.)
 * - Creates efficient columnar storage configuration
 * - Provides typed task wrapper generation
 *
 * **Per-module benefits:**
 * - Separate capacity tuning per module (self-adjusting buffer sizes)
 * - Module-specific tag attributes
 * - Isolated buffer management
 *
 * @typeParam TInput - Input tag attribute schema type
 * @typeParam T - Resolved tag attribute schema (inferred)
 * @typeParam FF - Feature flag schema type
 * @typeParam Env - Environment configuration type
 *
 * @param options - Module configuration
 * @param options.moduleMetadata - Module identification for tracing
 * @param options.moduleMetadata.gitSha - Git SHA for version tracking
 * @param options.moduleMetadata.filePath - Source file path for debugging
 * @param options.moduleMetadata.moduleName - Human-readable module name
 * @param options.tagAttributes - Schema from `defineTagAttributes()` or plain object
 *
 * @returns Module context builder with `task()` method
 *
 * @example
 * ```typescript
 * // Define your module's tag attributes
 * const orderSchema = defineTagAttributes({
 *   orderId: S.category(),
 *   status: S.enum(['pending', 'processing', 'complete', 'failed']),
 *   itemCount: S.number(),
 *   errorMessage: S.text(),
 * });
 *
 * // Create module context
 * const orderModule = createModuleContext({
 *   moduleMetadata: {
 *     gitSha: process.env.GIT_SHA || 'dev',
 *     filePath: 'orders/service.ts',
 *     moduleName: 'orders',
 *   },
 *   tagAttributes: orderSchema,
 * });
 *
 * // Create task wrappers
 * export const processOrder = orderModule.task('processOrder',
 *   async (ctx, orderId: string) => {
 *     ctx.log.scope({ orderId });
 *     ctx.tag.status('processing');
 *
 *     const items = await fetchItems(orderId);
 *     ctx.tag.itemCount(items.length);
 *
 *     return ctx.ok({ orderId, items });
 *   }
 * );
 * ```
 *
 * @see {@link defineTagAttributes} - Create typed tag attribute schemas
 * @see {@link ModuleContextBuilder} - Returned builder interface
 * @see {@link SpanContext} - Context provided to task functions
 */
export function createModuleContext<
  T extends TagAttributeSchema,
  FF extends FeatureFlagSchema = FeatureFlagSchema,
  Env = Record<string, unknown>,
>(options: {
  moduleMetadata: {
    gitSha: string;
    filePath: string;
    moduleName: string;
  };
  tagAttributes: T;
}): ModuleContextBuilder<T, FF, Env> {
  const { moduleMetadata, tagAttributes } = options;

  // Extract only the schema fields, removing methods like validate, extend, etc.
  const userSchemaOnly = Object.keys(tagAttributes as Record<string, unknown>).reduce(
    (acc, key) => {
      const value = (tagAttributes as Record<string, unknown>)[key];
      // Only include non-function properties
      if (typeof value !== 'function') {
        acc[key] = value;
      }
      return acc;
    },
    {} as Record<string, unknown>,
  );

  // Merge with system schema to get all required columns
  // Per specs/01h and 01f - system columns always included
  const schemaOnly = mergeWithSystemSchema(userSchemaOnly) as T;

  // Create module context - moduleId is deprecated, will be removed later
  // UTF-8 pre-encoding now happens in ModuleContext constructor
  const moduleContext = new ModuleContext(
    0, // moduleId deprecated, use utf8FilePath instead
    moduleMetadata.gitSha,
    moduleMetadata.filePath,
    schemaOnly,
  );

  return {
    // Expose capacity stats for testing and monitoring
    spanBufferCapacityStats: moduleContext.spanBufferCapacityStats,

    task<Args extends unknown[], Result>(
      name: string,
      fn: TaskFunction<Args, Result, T, FF, Env>,
      line?: number,
    ): (ctx: RequestContext<FF, Env>, ...args: Args) => Promise<Result> {
      return async (requestCtx: RequestContext<FF, Env>, ...args: Args): Promise<Result> => {
        // Create task context with span name
        const taskContext = new TaskContext(
          moduleContext,
          name, // span name - UTF-8 pre-encoded in TaskContext constructor
          line ?? 0, // lineNumber from transformer injection
        );

        // Create span buffer with traceId from request context
        // Per specs/01b - traceId is constant across all spans in a trace
        // Note: RequestContext.traceId is typed as string for API flexibility,
        // but it's actually a TraceId (generated by generateTraceId or validated externally)
        const spanBuffer = createSpanBuffer(schemaOnly, taskContext, requestCtx.traceId as TraceId);

        // Connect feature flag evaluator to buffer for analytics
        // The evaluator is a FeatureFlagEvaluator instance, we need to set columnWriters
        if (requestCtx.ff instanceof FeatureFlagEvaluator) {
          (requestCtx.ff as FeatureFlagEvaluator<FF>)['columnWriters'] = createFlagColumnWriters(spanBuffer);
        }

        // Write span-start entry (row 0) and pre-initialize span-end (row 1)
        // writeIndex is set to 2 after this call
        writeSpanStart(spanBuffer, name);

        // Write line number to row 0 (span-start) if provided by transformer
        if (line !== undefined) {
          spanBuffer.lineNumber(0, line);
        }

        // Inherit scoped values from parent context if available
        // Per specs/01i_span_scope_attributes.md - tasks inherit scoped attributes from calling context
        const parentLogger = (requestCtx as RequestContext<FF, Env> & { log?: SpanLogger<T> }).log;
        const inheritedScopeValues = parentLogger?._getScope?.()._getScopeValues() || {};

        // Create span logger with typed logging methods (with inherited scope values)
        const spanLogger = createSpanLoggerWithScope(schemaOnly, spanBuffer, inheritedScopeValues);

        // Create tag writer for span attributes (writes to row 0)
        const tagAPI = createTagWriter(schemaOnly, spanBuffer);

        // Create SpanContext prototype for this task (methods defined once per task wrapper)
        const spanContextProto = createSpanContextProto<T, FF, Env>(schemaOnly, taskContext);

        // Create span context using prototype-based inheritance
        // This avoids object spreads and maintains stable V8 hidden classes
        const spanContext = Object.create(spanContextProto) as MutableSpanContext<T, FF, Env>;

        // Copy user properties from requestCtx to spanContext
        // Use Object.keys to get own properties (not inherited from prototype)
        // Exclude internal properties that might conflict with SpanContext methods
        const RESERVED_KEYS = new Set([
          'ff',
          'tag',
          'log',
          '_buffer',
          '_schema',
          '_spanLogger',
          'buffer',
          'scope',
          'ok',
          'err',
          'span',
          // SpanBuffer properties that might be present on parent contexts
          'message',
          'lineNumber',
          'errorCode',
          'exceptionStack',
          'ffValue',
          'timestamps',
          'operations',
          'writeIndex',
          'capacity',
          'next',
          'spanId',
          'traceId',
          'hasParent',
          'parentSpanId',
          'parent',
          'children',
          'task',
          '_system',
          '_identity',
          '_timestamps',
          '_operations',
          '_writeIndex',
          '_capacity',
          '_next',
        ]);
        const requestCtxAny = requestCtx as unknown as Record<string, unknown>;
        const requestCtxKeys = Object.keys(requestCtxAny);
        for (const key of requestCtxKeys) {
          if (!RESERVED_KEYS.has(key)) {
            (spanContext as Record<string, unknown>)[key] = requestCtxAny[key];
          }
        }

        // Also copy any properties from the prototype chain of requestCtx
        // This handles cases where requestCtx was created with createTraceContext
        for (const key in requestCtxAny) {
          if (!RESERVED_KEYS.has(key) && !(key in spanContext) && !Object.hasOwn(requestCtxAny, key)) {
            // Property is inherited from prototype, copy it
            (spanContext as Record<string, unknown>)[key] = requestCtxAny[key];
          }
        }

        // Assign span-specific properties directly (stable hidden class)
        spanContext.traceId = requestCtx.traceId;
        spanContext.ff = requestCtx.ff;
        spanContext.env = requestCtx.env;
        spanContext.tag = tagAPI;
        spanContext.log = spanLogger as SpanLogger<T>;
        spanContext._buffer = spanBuffer;
        spanContext._schema = schemaOnly;
        spanContext._spanLogger = spanLogger;

        // Execute task function with exception handling
        try {
          // Cast to SpanContext for the task function call
          return await fn(spanContext as unknown as SpanContext<T, FF, Env>, ...args);
        } catch (error) {
          // Write span-exception to row 1 (fixed layout)
          // Row 1 was pre-initialized as exception, just update timestamp
          spanBuffer.timestamps[1] = getTimestampNanos();

          // Write exception details to row 1
          const errorMessage = error instanceof Error ? error.message : String(error);
          const errorStack = error instanceof Error ? error.stack : undefined;

          spanBuffer.message(1, errorMessage);
          if (errorStack) {
            spanBuffer.exceptionStack(1, errorStack);
          }

          // Re-throw to propagate
          throw error;
        }
      };
    },
  };
}
