/**
 * Main LMAO integration - Context creation and task wrapper system
 *
 * This module ties together:
 * - Feature flags with automatic analytics
 * - Environment configuration
 * - Tag attributes with columnar storage
 * - Task wrappers with span buffers
 */

import { type BaseSpanLogger, createSpanLoggerClass } from './codegen/spanLoggerGenerator.js';
import type { EvaluationContext, FeatureFlagSchema, InferFeatureFlags } from './schema/defineFeatureFlags.js';
import { FeatureFlagEvaluator, type FlagColumnWriters, type FlagEvaluator } from './schema/evaluator.js';
import { mergeWithSystemSchema } from './schema/systemSchema.js';
import type { InferTagAttributes, TagAttributeSchema } from './schema/types.js';
import { createChildSpanBuffer, createNextBuffer, createSpanBuffer } from './spanBuffer.js';
import { createTimeAnchor, getTimestampMicros } from './timestamp.js';
import type { BufferCapacityStats, ModuleContext, SpanBuffer, TaskContext } from './types.js';

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
 * - Supports .with() for attributes and .message() for text
 * - Returns final result after writing to buffer
 *
 * This class extends the base Result type to support method chaining
 * while maintaining proper TypeScript type narrowing.
 */
class FluentSuccessResult<V, T extends TagAttributeSchema> implements SuccessResult<V> {
  readonly success = true as const;
  readonly value: V;
  private buffer: SpanBuffer;
  private entryIndex: number;

  constructor(
    buffer: SpanBuffer,
    value: V,
    _schema: T, // Needed for generic type inference
    anchorEpochMicros: number,
    anchorPerfNow: number,
  ) {
    this.value = value;

    // Row 1 is ALWAYS the span-end row (fixed layout)
    // No need to find buffer with space - row 1 is pre-allocated
    this.buffer = buffer;
    this.entryIndex = 1; // ALWAYS row 1

    // Overwrite the pre-initialized span-exception with span-ok
    this.buffer.operations[1] = ENTRY_TYPE_SPAN_OK;

    // Write timestamp using anchor for high precision
    this.buffer.timestamps[1] = getTimestampMicros(anchorEpochMicros, anchorPerfNow);

    // Note: writeIndex is NOT incremented - row 1 is reserved, events start at row 2
  }

  /**
   * Set multiple attributes on the result entry
   * Example: ctx.ok(result).with({ userId: 'u1', operation: 'CREATE' })
   */
  with(attributes: Partial<InferTagAttributes<T>>): this {
    // Write each attribute to its column
    for (const [key, value] of Object.entries(attributes)) {
      const columnName = `attr_${key}`;
      writeToColumn(this.buffer, columnName, value, this.entryIndex);
    }
    return this;
  }

  /**
   * Set a message on the result entry
   * Example: ctx.ok(result).message('User created successfully')
   */
  message(text: string): this {
    writeToColumn(this.buffer, 'attr_resultMessage', text, this.entryIndex);
    return this;
  }
}

/**
 * Fluent error result with chaining support
 */
class FluentErrorResult<E, T extends TagAttributeSchema> implements ErrorResult<E> {
  readonly success = false as const;
  readonly error: { code: string; details: E };
  private buffer: SpanBuffer;
  private entryIndex: number;

  constructor(
    buffer: SpanBuffer,
    code: string,
    details: E,
    _schema: T, // Needed for generic type inference
    anchorEpochMicros: number,
    anchorPerfNow: number,
  ) {
    this.error = { code, details };

    // Row 1 is ALWAYS the span-end row (fixed layout)
    // No need to find buffer with space - row 1 is pre-allocated
    this.buffer = buffer;
    this.entryIndex = 1; // ALWAYS row 1

    // Overwrite the pre-initialized span-exception with span-err
    this.buffer.operations[1] = ENTRY_TYPE_SPAN_ERR;

    // Write timestamp using anchor for high precision
    this.buffer.timestamps[1] = getTimestampMicros(anchorEpochMicros, anchorPerfNow);

    // Write error code
    writeToColumn(this.buffer, 'attr_errorCode', code, this.entryIndex);

    // Note: writeIndex is NOT incremented - row 1 is reserved, events start at row 2
  }

  /**
   * Set multiple attributes on the result entry
   * Example: ctx.err('ERROR', details).with({ userId: 'u1' })
   */
  with(attributes: Partial<InferTagAttributes<T>>): this {
    // Write each attribute to its column
    for (const [key, value] of Object.entries(attributes)) {
      const columnName = `attr_${key}`;
      writeToColumn(this.buffer, columnName, value, this.entryIndex);
    }
    return this;
  }

  /**
   * Set a message on the result entry
   * Example: ctx.err('ERROR', details).message('Operation failed')
   */
  message(text: string): this {
    writeToColumn(this.buffer, 'attr_resultMessage', text, this.entryIndex);
    return this;
  }
}

/**
 * Generate unique trace ID
 */
function generateTraceId(): string {
  // Simple implementation - can be replaced with more sophisticated ID generation
  return `trace-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
}

/**
 * String interning for category columns
 *
 * Per specs/01b1_buffer_performance_optimizations.md:
 * - Store strings once, reference by index
 * - Fast integer comparison vs string comparison
 * - Direct Arrow dictionary creation
 * - Cache-friendly integer storage
 */
class StringInterner {
  private strings: string[] = [];
  private indices = new Map<string, number>();

  /**
   * Intern a string and return its index
   * O(1) lookup via Map, O(1) insertion
   */
  intern(str: string): number {
    let idx = this.indices.get(str);

    if (idx === undefined) {
      idx = this.strings.length;
      this.strings.push(str);
      this.indices.set(str, idx);
    }

    return idx;
  }

  /**
   * Get string by index
   * Used during Arrow conversion
   */
  getString(idx: number): string | undefined {
    return this.strings[idx];
  }

  /**
   * Get all strings for Arrow dictionary
   */
  getStrings(): readonly string[] {
    return this.strings;
  }

  /**
   * Get count of unique strings
   */
  size(): number {
    return this.strings.length;
  }
}

/**
 * Global string interners
 * One per string type to keep dictionaries separate
 *
 * Exported for Arrow table conversion
 */
export const categoryInterner = new StringInterner();
export const moduleIdInterner = new StringInterner();
export const spanNameInterner = new StringInterner();

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
    // For now, removed console.log to avoid hot path overhead

    stats.currentCapacity = newCapacity;
    resetStats(stats);
    return true;
  }

  // Decrease if <5% writes overflow and we have many buffers
  if (overflowRatio < 0.05 && stats.totalBuffersCreated >= 10 && stats.currentCapacity > 8) {
    const newCapacity = Math.max(8, stats.currentCapacity / 2);

    // TODO: Use system tracer for self-tracing capacity tuning events
    // For now, removed console.log to avoid hot path overhead

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
  readonly traceId: string;

  /**
   * Epoch time in microseconds when the request started.
   * Used as anchor point for high-precision timestamp calculations.
   * @internal Combined with anchorPerfNow for efficient timestamp generation
   */
  readonly anchorEpochMicros: number;

  /**
   * High-resolution performance counter value when request started.
   * Combined with anchorEpochMicros for microsecond-precision timestamps.
   * @internal Used internally for timestamp delta calculations
   */
  readonly anchorPerfNow: number;

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
  const evaluationContext: EvaluationContext = {
    userId: params.userId,
    requestId: params.requestId,
  };

  // Create feature flag evaluator (buffer will be set by task wrapper)
  const ffEvaluator = new FeatureFlagEvaluator(
    featureFlagSchema.schema,
    evaluationContext,
    evaluator,
    undefined, // Column writers set later when span context is created
  ) as FeatureFlagEvaluator<FF> & InferFeatureFlags<FF>;

  // Create time anchor ONCE per request for high-precision timestamps
  const { anchorEpochMicros, anchorPerfNow } = createTimeAnchor();

  return {
    requestId: params.requestId,
    userId: params.userId,
    traceId: generateTraceId(),
    anchorEpochMicros,
    anchorPerfNow,
    ff: ffEvaluator,
    env: environmentConfig,
  };
}

/**
 * Chainable tag API type for span attributes
 *
 * Writes to row 0 (span-start) with overwrite semantics.
 * All methods return `this` for zero-allocation chaining.
 *
 * @example
 * ```typescript
 * // Direct attribute methods (chainable)
 * ctx.tag.userId('u1').requestId('r1').operation('INSERT');
 *
 * // Bulk attribute setting
 * ctx.tag.with({ userId: 'u1', requestId: 'r1' });
 * ```
 */
export type ChainableTagAPI<T extends TagAttributeSchema> = {
  /**
   * Set multiple attributes at once (chainable)
   *
   * @param attributes - Object with attribute values to set
   * @returns The tag API for chaining
   *
   * @example
   * ctx.tag.with({ userId: 'u1', requestId: 'r1' }).operation('INSERT');
   */
  with(attributes: Partial<InferTagAttributes<T>>): ChainableTagAPI<T>;
} & {
  /**
   * Set individual attributes (chainable)
   *
   * Each attribute from the schema gets a typed method.
   *
   * @example
   * ctx.tag.userId('u1').requestId('r1').operation('INSERT');
   */
  [K in keyof InferTagAttributes<T>]: (value: InferTagAttributes<T>[K]) => ChainableTagAPI<T>;
};

/**
 * Span logger context - provides logging API for spans
 *
 * This is what's available as `ctx.log` in task wrappers.
 * For span attributes, use `ctx.tag` directly (not `ctx.log.tag`).
 *
 * Per specs/01i_span_scope_attributes.md:
 * - scope() sets attributes that auto-propagate to all subsequent entries
 *
 * @example
 * ```typescript
 * ctx.log.info('Processing user');
 * ctx.log.scope({ requestId: 'req-1' }); // All subsequent logs include requestId
 * ctx.log.warn('Slow operation');
 * ```
 */
export interface SpanLogger<T extends TagAttributeSchema> {
  /**
   * Internal tag API - use `ctx.tag` instead
   * @internal Exposed on ctx.tag for cleaner API
   */
  readonly tag: ChainableTagAPI<T>;

  /**
   * Set scoped attributes that auto-propagate to all subsequent log entries
   *
   * Scoped attributes are automatically included in all log entries
   * and inherited by child spans.
   *
   * @param attributes - Attributes to scope to this span
   *
   * @example
   * ctx.log.scope({ requestId: req.id, userId: req.user?.id });
   * ctx.log.info('Processing'); // Includes requestId and userId
   */
  scope(attributes: Partial<InferTagAttributes<T>>): void;

  /**
   * Get current scoped attributes (for inheritance)
   * @internal Used internally for scope inheritance to child spans
   */
  getScopedAttributes(): Record<string, unknown>;

  /**
   * Log a message entry with specified level
   *
   * @param level - Log level (info, debug, warn, error)
   * @param message - Log message text
   */
  message(level: 'info' | 'debug' | 'warn' | 'error', message: string): void;

  /**
   * Log an info message
   * @param message - Log message text
   */
  info(message: string): void;

  /**
   * Log a debug message
   * @param message - Log message text
   */
  debug(message: string): void;

  /**
   * Log a warning message
   * @param message - Log message text
   */
  warn(message: string): void;

  /**
   * Log an error message
   * @param message - Log message text
   */
  error(message: string): void;
}

/**
 * Span context provided to task functions
 *
 * This is what's provided to task functions and child spans.
 * Extends RequestContext with:
 * - `tag` - Chainable span attribute API (writes to row 0)
 * - `log` - Logging API (info, debug, warn, error, scope)
 * - `ok`/`err` - Result helpers with fluent API
 * - `span` - Child span creation
 *
 * @example
 * ```typescript
 * const createUser = task('create-user', async (ctx, userData) => {
 *   // Span attributes (writes to span-start row)
 *   ctx.tag.userId(userData.id).operation('INSERT');
 *
 *   // Logging
 *   ctx.log.info('Creating user');
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
  readonly tag: ChainableTagAPI<T>;

  /**
   * Logging API for structured log messages
   *
   * Use for logging events during span execution.
   * Logs are appended to the buffer (row 2+).
   *
   * @example
   * ctx.log.info('Processing request');
   * ctx.log.scope({ requestId: 'req-1' }); // Set scoped attributes
   */
  readonly log: SpanLogger<T>;

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
   *
   * @param name - Child span name
   * @param fn - Async function to execute in child span
   * @returns Promise resolving to child function result
   *
   * @example
   * const result = await ctx.span('validate', async (childCtx) => {
   *   childCtx.tag.step('validation');
   *   return childCtx.ok({ valid: true });
   * });
   */
  span<R>(name: string, fn: (ctx: SpanContext<T, FF, Env>) => Promise<R>): Promise<R>;
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
  ): (ctx: RequestContext<FF, Env>, ...args: Args) => Promise<Result>;
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

/** Feature flag access event - logged when a flag value is first read in a span */
export const ENTRY_TYPE_FF_ACCESS = 1;

/** Feature flag usage event - logged when a flag influences a code path decision */
export const ENTRY_TYPE_FF_USAGE = 2;

/** Tag attribute entry - logged via ctx.tag API */
export const ENTRY_TYPE_TAG = 3;

/**
 * Generic message entry type.
 * @deprecated Use level-specific types: ENTRY_TYPE_INFO, ENTRY_TYPE_DEBUG, ENTRY_TYPE_WARN, or ENTRY_TYPE_ERROR
 */
export const ENTRY_TYPE_MESSAGE = 4;

/** Span start event - written at row 0 when a span begins */
export const ENTRY_TYPE_SPAN_START = 5;

/** Span success completion - written to row 1 via ctx.ok() */
export const ENTRY_TYPE_SPAN_OK = 6;

/** Span error completion - written to row 1 via ctx.err() */
export const ENTRY_TYPE_SPAN_ERR = 7;

/** Span exception - written to row 1 when an unhandled exception occurs */
export const ENTRY_TYPE_SPAN_EXCEPTION = 8;

/** Info log level entry - via ctx.log.info() */
export const ENTRY_TYPE_INFO = 9;

/** Debug log level entry - via ctx.log.debug() */
export const ENTRY_TYPE_DEBUG = 10;

/** Warning log level entry - via ctx.log.warn() */
export const ENTRY_TYPE_WARN = 11;

/** Error log level entry - via ctx.log.error() */
export const ENTRY_TYPE_ERROR = 12;

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
  'ff-access', // 1
  'ff-usage', // 2
  'tag', // 3
  'message', // 4
  'span-start', // 5
  'span-ok', // 6
  'span-err', // 7
  'span-exception', // 8
  'info', // 9
  'debug', // 10
  'warn', // 11
  'error', // 12
] as const;

/**
 * Create column writers for feature flag analytics
 * Writes to TypedArray columnar buffers in memory (hot path)
 *
 * Per specs/01b1_buffer_performance_optimizations.md:
 * - String interning for category columns
 * - Direct TypedArray writes (no allocations)
 */
function createFlagColumnWriters(
  buffer: SpanBuffer,
  anchorEpochMicros: number,
  anchorPerfNow: number,
): FlagColumnWriters {
  return {
    writeEntryType(type: 'ff-access' | 'ff-usage'): void {
      // Write entry type code to operation column
      const typeCode = type === 'ff-access' ? ENTRY_TYPE_FF_ACCESS : ENTRY_TYPE_FF_USAGE;
      buffer.operations[buffer.writeIndex] = typeCode;

      // Write timestamp using anchor for high precision (microseconds)
      buffer.timestamps[buffer.writeIndex] = getTimestampMicros(anchorEpochMicros, anchorPerfNow);
    },

    writeFfName(name: string): void {
      // Write to attr_ffName column with string interning
      const column = buffer['attr_ffName' as keyof SpanBuffer];
      if (column && column instanceof Uint32Array) {
        column[buffer.writeIndex] = categoryInterner.intern(name);
      }
    },

    writeFfValue(value: string | number | boolean | null): void {
      // Write to attr_ffValue column
      // For mixed types, serialize to string and intern
      const column = buffer['attr_ffValue' as keyof SpanBuffer];
      if (column && column instanceof Uint32Array) {
        const strValue = value === null ? 'null' : String(value);
        column[buffer.writeIndex] = categoryInterner.intern(strValue);
      }
    },

    writeAction(action?: string): void {
      // Write to attr_action column
      const column = buffer['attr_action' as keyof SpanBuffer];
      if (column && column instanceof Uint32Array) {
        const idx = buffer.writeIndex;
        if (action) {
          column[idx] = categoryInterner.intern(action);
          // Mark as non-null in bitmap
          const nullBitmap = buffer.nullBitmaps['attr_action'];
          if (nullBitmap) {
            const byteIndex = Math.floor(idx / 8);
            const bitOffset = idx % 8;
            nullBitmap[byteIndex] |= 1 << bitOffset;
          }
        }
      }
    },

    writeOutcome(outcome?: string): void {
      // Write to attr_outcome column
      const column = buffer['attr_outcome' as keyof SpanBuffer];
      if (column && column instanceof Uint32Array) {
        const idx = buffer.writeIndex;
        if (outcome) {
          column[idx] = categoryInterner.intern(outcome);
          // Mark as non-null in bitmap
          const nullBitmap = buffer.nullBitmaps['attr_outcome'];
          if (nullBitmap) {
            const byteIndex = Math.floor(idx / 8);
            const bitOffset = idx % 8;
            nullBitmap[byteIndex] |= 1 << bitOffset;
          }
        }
      }
    },

    writeContextAttributes(context: EvaluationContext): void {
      const idx = buffer.writeIndex;

      // Write context attributes to their respective columns with string interning
      if (context.userId) {
        const column = buffer['attr_contextUserId' as keyof SpanBuffer];
        if (column && column instanceof Uint32Array) {
          column[idx] = categoryInterner.intern(context.userId);
          // Mark as non-null in bitmap
          const nullBitmap = buffer.nullBitmaps['attr_contextUserId'];
          if (nullBitmap) {
            const byteIndex = Math.floor(idx / 8);
            const bitOffset = idx % 8;
            nullBitmap[byteIndex] |= 1 << bitOffset;
          }
        }
      }

      if (context.requestId) {
        const column = buffer['attr_contextRequestId' as keyof SpanBuffer];
        if (column && column instanceof Uint32Array) {
          column[idx] = categoryInterner.intern(context.requestId);
          // Mark as non-null in bitmap
          const nullBitmap = buffer.nullBitmaps['attr_contextRequestId'];
          if (nullBitmap) {
            const byteIndex = Math.floor(idx / 8);
            const bitOffset = idx % 8;
            nullBitmap[byteIndex] |= 1 << bitOffset;
          }
        }
      }

      // Increment write index after all writes
      buffer.writeIndex++;
    },
  };
}

/**
 * Find buffer with space, creating chained buffer if needed
 *
 * Per specs/01b_columnar_buffer_architecture.md:
 * - Buffer chaining handles overflow gracefully
 * - Tracks overflow stats for self-tuning
 * - CPU branch predictor friendly
 */
function getBufferWithSpace(inputBuffer: SpanBuffer): { buffer: SpanBuffer; didOverflow: boolean } {
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
function writeSpanStart(buffer: SpanBuffer, spanName: string, anchorEpochMicros: number, anchorPerfNow: number): void {
  // Row 0: span-start (fixed layout)
  buffer.operations[0] = ENTRY_TYPE_SPAN_START;
  buffer.timestamps[0] = getTimestampMicros(anchorEpochMicros, anchorPerfNow);
  writeToColumn(buffer, 'attr_spanName', spanName, 0);

  // Row 1: pre-initialize as span-exception (will be overwritten on ok/err)
  buffer.operations[1] = ENTRY_TYPE_SPAN_EXCEPTION;
  buffer.timestamps[1] = 0; // Will be set on completion

  // Events start at row 2
  buffer.writeIndex = 2;
}

/**
 * Text string storage - raw strings without interning
 * Separate from category interning to avoid dictionary overhead for unique strings
 */
class TextStringStorage {
  private strings: string[] = [];

  /**
   * Store a text string and return its index
   * No deduplication - every string gets a new index
   */
  store(str: string): number {
    const idx = this.strings.length;
    this.strings.push(str);
    return idx;
  }

  /**
   * Get string by index
   */
  getString(idx: number): string | undefined {
    return this.strings[idx];
  }

  /**
   * Get all strings for Arrow column
   */
  getStrings(): readonly string[] {
    return this.strings;
  }
}

/**
 * Global text string storage
 * One instance for all text columns
 *
 * Exported for Arrow table conversion
 */
export const textStringStorage = new TextStringStorage();

/**
 * Write a value to a TypedArray column
 * Handles type conversion for different column types
 *
 * Per specs/01b1_buffer_performance_optimizations.md and 01a_trace_schema_system.md:
 * - THREE DISTINCT STRING TYPES:
 *   1. enum: Uint8Array with compile-time enum values (0-255)
 *   2. category: Uint32Array with runtime string interning
 *   3. text: Uint32Array with raw string storage (no interning)
 * - Null bitmap management per Arrow spec (1 = valid, 0 = null)
 * - Direct TypedArray writes (hot path)
 */
function writeToColumn(buffer: SpanBuffer, columnName: string, value: unknown, index: number): void {
  const column = buffer[columnName as keyof SpanBuffer];

  if (!column || !ArrayBuffer.isView(column)) return;

  // Get null bitmap for this column (Arrow format: 1 Uint8Array per column)
  const nullBitmap = buffer.nullBitmaps[columnName as `attr_${string}`];

  // Handle null/undefined - store 0 and clear null bitmap bit
  if (value === null || value === undefined) {
    if (column instanceof Uint8Array) {
      column[index] = 0;
    } else if (column instanceof Uint16Array) {
      column[index] = 0;
    } else if (column instanceof Uint32Array) {
      column[index] = 0;
    } else if (column instanceof Float64Array) {
      column[index] = 0;
    }

    // Clear bit in bitmap (Arrow format: 0 = null, 1 = valid)
    if (nullBitmap) {
      const byteIndex = Math.floor(index / 8);
      const bitOffset = index % 8;
      nullBitmap[byteIndex] &= ~(1 << bitOffset);
    }
    return;
  }

  // Mark as non-null in bitmap (Arrow format: 1 = valid)
  if (nullBitmap) {
    const byteIndex = Math.floor(index / 8);
    const bitOffset = index % 8;
    nullBitmap[byteIndex] |= 1 << bitOffset;
  }

  // Get schema metadata to determine type
  const fieldName = columnName.replace('attr_', '');
  const schema = buffer.task.module.tagAttributes;
  const fieldSchema = schema[fieldName];
  const schemaWithMetadata = fieldSchema as import('./schema/types.js').SchemaWithMetadata;
  const lmaoType = schemaWithMetadata?.__lmao_type;

  // Write based on column type and schema metadata
  if (column instanceof Uint8Array) {
    // Boolean or small enum types (0-255 values)
    if (typeof value === 'boolean') {
      column[index] = value ? 1 : 0;
    } else if (typeof value === 'number') {
      column[index] = value;
    } else if (typeof value === 'string' && lmaoType === 'enum') {
      // Enum: map string to index using enum values from schema
      const enumSchema = schemaWithMetadata as import('./schema/types.js').EnumSchemaWithMetadata;
      const enumValues = enumSchema?.__lmao_enum_values;

      if (enumValues) {
        const enumIndex = enumValues.indexOf(value);
        column[index] = enumIndex >= 0 ? enumIndex : 0;
      } else {
        column[index] = 0;
      }
    }
  } else if (column instanceof Uint16Array) {
    // Medium enum types (256-65535 values)
    if (typeof value === 'number') {
      column[index] = value;
    } else if (typeof value === 'string' && lmaoType === 'enum') {
      const enumSchema = schemaWithMetadata as import('./schema/types.js').EnumSchemaWithMetadata;
      const enumValues = enumSchema?.__lmao_enum_values;

      if (enumValues) {
        const enumIndex = enumValues.indexOf(value);
        column[index] = enumIndex >= 0 ? enumIndex : 0;
      } else {
        column[index] = 0;
      }
    }
  } else if (column instanceof Uint32Array) {
    // Category or text types (both use Uint32Array but different storage)
    if (typeof value === 'string') {
      // CRITICAL: Check __lmao_type metadata to distinguish category vs text
      if (lmaoType === 'text') {
        // TEXT: raw storage without interning (specs/01a)
        column[index] = textStringStorage.store(value);
      } else if (lmaoType === 'category') {
        // CATEGORY: string interning for repeated values (specs/01a)
        column[index] = categoryInterner.intern(value);
      } else {
        // Fallback: treat as category (safe default)
        column[index] = categoryInterner.intern(value);
      }
    } else if (typeof value === 'number') {
      column[index] = value;
    }
  } else if (column instanceof Float64Array) {
    // Number types - full precision
    column[index] = typeof value === 'number' ? value : 0;
  }
}

/**
 * Get buffer with space function type
 */
type GetBufferWithSpaceFn = (buffer: SpanBuffer) => { buffer: SpanBuffer; didOverflow: boolean };

/**
 * Cache for generated SpanLogger classes
 * Per-schema cache to avoid regenerating the same class
 */
const spanLoggerClassCache = new WeakMap<
  TagAttributeSchema,
  new (
    buffer: SpanBuffer,
    categoryInterner: StringInterner,
    textStorage: TextStringStorage,
    getBufferWithSpace: GetBufferWithSpaceFn,
    anchorEpochMicros: number,
    anchorPerfNow: number,
    initialScopedAttributes?: Record<string, unknown>,
  ) => BaseSpanLogger<TagAttributeSchema>
>();

/**
 * Create span logger with typed tag methods and method chaining
 * Writes to TypedArray columnar buffers in memory (hot path)
 *
 * Per specs/01g_trace_context_api_codegen.md and 01j_module_context_and_spanlogger_generation.md:
 * - Uses runtime class generation with new Function() for zero-overhead prototype methods
 * - Tag getter creates a new entry and returns a chainable API
 * - All chained methods write to the SAME row
 * - Zero allocations: returns same object instance
 *
 * @param schema - Tag attribute schema with field definitions
 * @param buffer - SpanBuffer to write entries to (per-span instance)
 * @param inheritedScopedAttributes - Scoped attributes inherited from parent span
 * @param anchorEpochMicros - Epoch time in microseconds when anchor was created
 * @param anchorPerfNow - High-precision time when anchor was created
 * @returns SpanLogger with typed methods matching schema
 */
function createSpanLogger<T extends TagAttributeSchema>(
  schema: T,
  buffer: SpanBuffer,
  inheritedScopedAttributes?: Record<string, unknown>,
  anchorEpochMicros?: number,
  anchorPerfNow?: number,
): SpanLogger<T> {
  // Get or create the generated SpanLogger class (cold path - happens once per schema)
  let SpanLoggerClass = spanLoggerClassCache.get(schema);

  if (!SpanLoggerClass) {
    SpanLoggerClass = createSpanLoggerClass(schema);
    spanLoggerClassCache.set(schema, SpanLoggerClass);
  }

  // TypeScript doesn't know the WeakMap guarantees non-null after set
  // So we add an assertion here
  if (!SpanLoggerClass) {
    throw new Error('Failed to create SpanLogger class');
  }

  // Create instance (hot path - happens once per span)
  const logger = new SpanLoggerClass(
    buffer,
    categoryInterner,
    textStringStorage,
    getBufferWithSpace,
    anchorEpochMicros ?? 0,
    anchorPerfNow ?? 0,
    inheritedScopedAttributes || {},
  );

  // If scoped attributes were inherited, pre-fill the buffer
  if (inheritedScopedAttributes && Object.keys(inheritedScopedAttributes).length > 0) {
    logger.scope(inheritedScopedAttributes);
  }

  return logger as SpanLogger<T>;
}

/**
 * Extract just the schema fields from an object, removing methods
 * This allows us to accept objects with additional methods like validate, extend, etc.
 *
 * This type recursively picks all properties that are not functions from intersections
 *
 * IMPORTANT: This must properly filter out methods added by defineTagAttributes like:
 * - validate
 * - parse
 * - safeParse
 * - extend
 */
type ExtractSchemaFields<T> = {
  [K in keyof T as T[K] extends Function ? never : K]: T[K];
};

/**
 * Type predicate to check if extracted fields match TagAttributeSchema
 */
type IsValidTagSchema<T> = ExtractSchemaFields<T> extends TagAttributeSchema
  ? ExtractSchemaFields<T>
  : TagAttributeSchema;

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
  TInput,
  T extends TagAttributeSchema = IsValidTagSchema<TInput>,
  FF extends FeatureFlagSchema = FeatureFlagSchema,
  Env = Record<string, unknown>,
>(options: {
  moduleMetadata: {
    gitSha: string;
    filePath: string;
    moduleName: string;
  };
  tagAttributes: TInput;
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

  // Create module context with string-interned module ID
  // Module ID is the file path, interned for efficient storage
  const moduleContext: ModuleContext = {
    moduleId: moduleIdInterner.intern(moduleMetadata.filePath),
    gitSha: moduleMetadata.gitSha,
    filePath: moduleMetadata.filePath,
    tagAttributes: schemaOnly,
    spanBufferCapacityStats: {
      currentCapacity: 64, // Start with cache-friendly size (see specs/01b_columnar_buffer_architecture.md)
      totalWrites: 0,
      overflowWrites: 0,
      totalBuffersCreated: 0,
    },
  };

  return {
    task<Args extends unknown[], Result>(
      name: string,
      fn: TaskFunction<Args, Result, T, FF, Env>,
    ): (ctx: RequestContext<FF, Env>, ...args: Args) => Promise<Result> {
      return async (requestCtx: RequestContext<FF, Env>, ...args: Args): Promise<Result> => {
        // Create task context with string-interned span name
        const taskContext: TaskContext = {
          module: moduleContext,
          spanNameId: spanNameInterner.intern(name),
          lineNumber: 0, // Would be set by code generation
        };

        // Create span buffer with traceId from request context
        // Per specs/01b - traceId is constant across all spans in a trace
        const spanBuffer = createSpanBuffer(schemaOnly, taskContext, requestCtx.traceId);

        // Connect feature flag evaluator to buffer for analytics
        // The evaluator is a FeatureFlagEvaluator instance, we need to set columnWriters
        if (requestCtx.ff instanceof FeatureFlagEvaluator) {
          (requestCtx.ff as FeatureFlagEvaluator<FF>)['columnWriters'] = createFlagColumnWriters(
            spanBuffer,
            requestCtx.anchorEpochMicros,
            requestCtx.anchorPerfNow,
          );
        }

        // Write span-start entry (row 0) and pre-initialize span-end (row 1)
        // writeIndex is set to 2 after this call
        writeSpanStart(spanBuffer, name, requestCtx.anchorEpochMicros, requestCtx.anchorPerfNow);

        // Inherit scoped attributes from parent context if available
        // Per specs/01i_span_scope_attributes.md - tasks inherit scoped attributes from calling context
        const inheritedScopedAttributes =
          (requestCtx as RequestContext<FF, Env> & { log?: SpanLogger<T> }).log?.getScopedAttributes() || {};

        // Create span logger with typed tag methods (with inherited scoped attributes)
        const spanLogger = createSpanLogger(
          schemaOnly,
          spanBuffer,
          inheritedScopedAttributes,
          requestCtx.anchorEpochMicros,
          requestCtx.anchorPerfNow,
        );

        // Create span context
        // Note: spanLogger.tag returns the chainable tag API
        // We expose it directly on ctx.tag for cleaner API: ctx.tag.userId() instead of ctx.log.tag.userId()
        const spanContext: SpanContext<T, FF, Env> = {
          ...requestCtx,
          tag: spanLogger.tag as ChainableTagAPI<T>,
          log: spanLogger,

          ok<V>(value: V): FluentSuccessResult<V, T> {
            return new FluentSuccessResult<V, T>(
              spanBuffer,
              value,
              schemaOnly,
              requestCtx.anchorEpochMicros,
              requestCtx.anchorPerfNow,
            );
          },

          err<E>(code: string, error: E): FluentErrorResult<E, T> {
            return new FluentErrorResult<E, T>(
              spanBuffer,
              code,
              error,
              schemaOnly,
              requestCtx.anchorEpochMicros,
              requestCtx.anchorPerfNow,
            );
          },

          async span<R>(childName: string, childFn: (ctx: SpanContext<T, FF, Env>) => Promise<R>): Promise<R> {
            // Create child span buffer with Arrow builders
            const childBuffer = createChildSpanBuffer(spanBuffer, taskContext);

            // Write span-start for child span (row 0) and pre-initialize span-end (row 1)
            writeSpanStart(childBuffer, childName, requestCtx.anchorEpochMicros, requestCtx.anchorPerfNow);

            // Inherit scoped attributes from parent span
            // Per specs/01i_span_scope_attributes.md - child spans inherit parent's scoped attributes
            const parentScopedAttributes = spanLogger.getScopedAttributes();

            // Create child context with its own logger (with inherited scoped attributes)
            const childLogger = createSpanLogger(
              schemaOnly,
              childBuffer,
              parentScopedAttributes,
              requestCtx.anchorEpochMicros,
              requestCtx.anchorPerfNow,
            );

            // Create a new feature flag evaluator bound to the CHILD buffer.
            // This ensures ff-access/ff-usage entries are logged to the correct span.
            // The withBuffer() method creates a fresh evaluator with:
            // - Same schema and evaluation context
            // - New buffer reference (child buffer)
            // - Fresh accessedFlags set (so first access in child logs ff-access)
            // - Fresh flagCache (child span gets its own cache)
            const childFf = spanContext.ff.withBuffer(childBuffer) as FeatureFlagEvaluator<FF> & InferFeatureFlags<FF>;

            const childContext: SpanContext<T, FF, Env> = {
              ...spanContext,
              ff: childFf,
              tag: childLogger.tag as ChainableTagAPI<T>,
              log: childLogger,
            };

            // Execute child span with exception handling
            try {
              return await childFn(childContext);
            } catch (error) {
              // Write span-exception to row 1 (fixed layout)
              // Row 1 was pre-initialized as exception, just update timestamp
              childBuffer.timestamps[1] = getTimestampMicros(requestCtx.anchorEpochMicros, requestCtx.anchorPerfNow);

              // Write exception details to row 1
              const errorMessage = error instanceof Error ? error.message : String(error);
              const errorStack = error instanceof Error ? error.stack : undefined;

              writeToColumn(childBuffer, 'attr_exceptionMessage', errorMessage, 1);
              if (errorStack) {
                writeToColumn(childBuffer, 'attr_exceptionStack', errorStack, 1);
              }

              // Re-throw to propagate
              throw error;
            }
          },
        };

        // Execute task function with exception handling
        try {
          return await fn(spanContext, ...args);
        } catch (error) {
          // Write span-exception to row 1 (fixed layout)
          // Row 1 was pre-initialized as exception, just update timestamp
          spanBuffer.timestamps[1] = getTimestampMicros(requestCtx.anchorEpochMicros, requestCtx.anchorPerfNow);

          // Write exception details to row 1
          const errorMessage = error instanceof Error ? error.message : String(error);
          const errorStack = error instanceof Error ? error.stack : undefined;

          writeToColumn(spanBuffer, 'attr_exceptionMessage', errorMessage, 1);
          if (errorStack) {
            writeToColumn(spanBuffer, 'attr_exceptionStack', errorStack, 1);
          }

          // Re-throw to propagate
          throw error;
        }
      };
    },
  };
}
