/**
 * Main LMAO integration - Context creation and task wrapper system
 *
 * This module ties together:
 * - Feature flags with automatic analytics
 * - Environment configuration
 * - Tag attributes with columnar storage
 * - Task wrappers with span buffers
 */

import { type BaseSpanLogger, type ChainableTagAPI, createSpanLoggerClass } from './codegen/spanLoggerGenerator.js';
import { ModuleContext } from './moduleContext.js';
import type { EvaluationContext, FeatureFlagSchema, InferFeatureFlags } from './schema/defineFeatureFlags.js';
import { FeatureFlagEvaluator, type FlagColumnWriters, type FlagEvaluator } from './schema/evaluator.js';
import { mergeWithSystemSchema } from './schema/systemSchema.js';
import type { InferTagAttributes, TagAttributeSchema } from './schema/types.js';
import { createChildSpanBuffer, createNextBuffer, createSpanBuffer } from './spanBuffer.js';
import { TaskContext } from './taskContext.js';
import { getTimestampNanos } from './timestamp.js';
import type { BufferCapacityStats, SpanBuffer } from './types.js';

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
  ) {
    this.value = value;

    // Row 1 is ALWAYS the span-end row (fixed layout)
    // No need to find buffer with space - row 1 is pre-allocated
    this.buffer = buffer;
    this.entryIndex = 1; // ALWAYS row 1

    // Overwrite the pre-initialized span-exception with span-ok
    this.buffer.operations[1] = ENTRY_TYPE_SPAN_OK;

    // Write timestamp (nanoseconds since epoch)
    this.buffer.timestamps[1] = getTimestampNanos();

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
  ) {
    this.error = { code, details };

    // Row 1 is ALWAYS the span-end row (fixed layout)
    // No need to find buffer with space - row 1 is pre-allocated
    this.buffer = buffer;
    this.entryIndex = 1; // ALWAYS row 1

    // Overwrite the pre-initialized span-exception with span-err
    this.buffer.operations[1] = ENTRY_TYPE_SPAN_ERR;

    // Write timestamp (nanoseconds since epoch)
    this.buffer.timestamps[1] = getTimestampNanos();

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
 * Global string interners for SYSTEM columns only
 *
 * - USER attribute columns (category/text) store strings directly in string[] arrays
 * - SYSTEM columns (moduleId, spanName) use interning for efficiency
 *
 * Exported for Arrow table conversion
 */
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
  readonly traceId: string;

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

  return {
    requestId: params.requestId,
    userId: params.userId,
    traceId: generateTraceId(),
    ff: ffEvaluator,
    env: environmentConfig,
  };
}

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
   * Get the Scope instance directly
   * @internal Used internally for scope inheritance via _getScopeValues()
   */
  _getScope(): import('./codegen/scopeGenerator.js').GeneratedScope;

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
 * Prefer level-specific types: ENTRY_TYPE_INFO, ENTRY_TYPE_DEBUG, ENTRY_TYPE_WARN, or ENTRY_TYPE_ERROR
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
function createFlagColumnWriters(buffer: SpanBuffer): FlagColumnWriters {
  return {
    writeEntryType(type: 'ff-access' | 'ff-usage'): void {
      // Write entry type code to operation column
      const typeCode = type === 'ff-access' ? ENTRY_TYPE_FF_ACCESS : ENTRY_TYPE_FF_USAGE;
      buffer.operations[buffer.writeIndex] = typeCode;

      // Write timestamp (nanoseconds since epoch)
      buffer.timestamps[buffer.writeIndex] = getTimestampNanos();
    },

    writeFfName(name: string): void {
      // Write to attr_ffName column (raw string, no interning)
      const column = buffer['attr_ffName' as keyof SpanBuffer] as string[] | undefined;
      if (column && Array.isArray(column)) {
        column[buffer.writeIndex] = name;
      }
    },

    writeFfValue(value: string | number | boolean | null): void {
      // Write to attr_ffValue column (raw string, no interning)
      const column = buffer['attr_ffValue' as keyof SpanBuffer] as string[] | undefined;
      if (column && Array.isArray(column)) {
        const strValue = value === null ? 'null' : String(value);
        column[buffer.writeIndex] = strValue;
      }
    },

    writeAction(action?: string): void {
      // Write to attr_action column (raw string, no interning)
      const column = buffer['attr_action' as keyof SpanBuffer] as string[] | undefined;
      if (column && Array.isArray(column)) {
        const idx = buffer.writeIndex;
        if (action) {
          column[idx] = action;
          // Mark as non-null in bitmap
          const nullBitmap = buffer.attr_action_nulls;
          if (nullBitmap) {
            const byteIndex = Math.floor(idx / 8);
            const bitOffset = idx % 8;
            nullBitmap[byteIndex] |= 1 << bitOffset;
          }
        }
      }
    },

    writeOutcome(outcome?: string): void {
      // Write to attr_outcome column (raw string, no interning)
      const column = buffer['attr_outcome' as keyof SpanBuffer] as string[] | undefined;
      if (column && Array.isArray(column)) {
        const idx = buffer.writeIndex;
        if (outcome) {
          column[idx] = outcome;
          // Mark as non-null in bitmap
          const nullBitmap = buffer.attr_outcome_nulls;
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

      // Write context attributes to their respective columns (raw strings, no interning)
      if (context.userId) {
        const column = buffer['attr_contextUserId' as keyof SpanBuffer] as string[] | undefined;
        if (column && Array.isArray(column)) {
          column[idx] = context.userId;
          // Mark as non-null in bitmap
          const nullBitmap = buffer.attr_contextUserId_nulls;
          if (nullBitmap) {
            const byteIndex = Math.floor(idx / 8);
            const bitOffset = idx % 8;
            nullBitmap[byteIndex] |= 1 << bitOffset;
          }
        }
      }

      if (context.requestId) {
        const column = buffer['attr_contextRequestId' as keyof SpanBuffer] as string[] | undefined;
        if (column && Array.isArray(column)) {
          column[idx] = context.requestId;
          // Mark as non-null in bitmap
          const nullBitmap = buffer.attr_contextRequestId_nulls;
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
function writeSpanStart(buffer: SpanBuffer, spanName: string): void {
  // Row 0: span-start (fixed layout)
  buffer.operations[0] = ENTRY_TYPE_SPAN_START;
  buffer.timestamps[0] = getTimestampNanos();
  writeToColumn(buffer, 'attr_spanName', spanName, 0);

  // Row 1: pre-initialize as span-exception (will be overwritten on ok/err)
  buffer.operations[1] = ENTRY_TYPE_SPAN_EXCEPTION;
  buffer.timestamps[1] = 0n; // Will be set on completion

  // Events start at row 2
  buffer.writeIndex = 2;
}

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
  const column = buffer[(columnName + '_values') as keyof SpanBuffer];

  if (!column || !ArrayBuffer.isView(column)) return;

  // Get null bitmap for this column (Arrow format: 1 Uint8Array per column)
  const nullBitmap = buffer[(columnName + '_nulls') as keyof SpanBuffer] as Uint8Array | undefined;

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
  const lmaoType = schemaWithMetadata?.__schema_type;

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
      const enumValues = enumSchema?.__enum_values;

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
      const enumValues = enumSchema?.__enum_values;

      if (enumValues) {
        const enumIndex = enumValues.indexOf(value);
        column[index] = enumIndex >= 0 ? enumIndex : 0;
      } else {
        column[index] = 0;
      }
    }
  } else if (column instanceof Uint32Array) {
    // Numeric indices for enum columns
    if (typeof value === 'number') {
      column[index] = value;
    } else if (typeof value === 'string') {
      // Should not happen - enums are mapped to numbers in generated code
      // But handle gracefully by writing 0
      column[index] = 0;
    }
  } else if (Array.isArray(column)) {
    // Category or text types (string arrays with new storage design)
    if (typeof value === 'string') {
      // Write raw string (no interning on hot path)
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
    getBufferWithSpace: GetBufferWithSpaceFn,
    scopeInstance: import('./codegen/scopeGenerator.js').GeneratedScope,
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
 * Per user requirements:
 * - Uses separate generated Scope class for attribute inheritance
 * - Scope instance is created per span context
 * - Child spans inherit parent's scope values via _getScopeValues()
 *
 * @param schema - Tag attribute schema with field definitions
 * @param buffer - SpanBuffer to write entries to (per-span instance)
 * @param inheritedScopeValues - Scoped values inherited from parent span (from _getScopeValues())
 * @returns SpanLogger with typed methods matching schema
 */
function createSpanLogger<T extends TagAttributeSchema>(
  schema: T,
  buffer: SpanBuffer,
  inheritedScopeValues?: Record<string, unknown>,
): SpanLogger<T> {
  // Import Scope generator (dynamic import for tree-shaking)
  const { createScope, createScopeWithInheritance } = require('./codegen/scopeGenerator.js');

  // Create Scope instance (separate from column storage)
  const scopeInstance =
    inheritedScopeValues && Object.keys(inheritedScopeValues).length > 0
      ? createScopeWithInheritance(schema, inheritedScopeValues)
      : createScope(schema);

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
    getBufferWithSpace,
    scopeInstance, // Pass Scope instance instead of plain object
  );

  // If scoped values were inherited, pre-fill the buffer
  // The scope() method will update both the Scope instance and pre-fill buffer
  if (inheritedScopeValues && Object.keys(inheritedScopeValues).length > 0) {
    logger.scope(inheritedScopeValues);
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
  const moduleContext = new ModuleContext(
    moduleIdInterner.intern(moduleMetadata.filePath),
    moduleMetadata.gitSha,
    moduleMetadata.filePath,
    schemaOnly,
  );

  return {
    task<Args extends unknown[], Result>(
      name: string,
      fn: TaskFunction<Args, Result, T, FF, Env>,
    ): (ctx: RequestContext<FF, Env>, ...args: Args) => Promise<Result> {
      return async (requestCtx: RequestContext<FF, Env>, ...args: Args): Promise<Result> => {
        // Create task context with string-interned span name
        const taskContext = new TaskContext(
          moduleContext,
          spanNameInterner.intern(name),
          0, // lineNumber would be set by code generation
        );

        // Create span buffer with traceId from request context
        // Per specs/01b - traceId is constant across all spans in a trace
        const spanBuffer = createSpanBuffer(schemaOnly, taskContext, requestCtx.traceId);

        // Connect feature flag evaluator to buffer for analytics
        // The evaluator is a FeatureFlagEvaluator instance, we need to set columnWriters
        if (requestCtx.ff instanceof FeatureFlagEvaluator) {
          (requestCtx.ff as FeatureFlagEvaluator<FF>)['columnWriters'] = createFlagColumnWriters(spanBuffer);
        }

        // Write span-start entry (row 0) and pre-initialize span-end (row 1)
        // writeIndex is set to 2 after this call
        writeSpanStart(spanBuffer, name);

        // Inherit scoped values from parent context if available
        // Per specs/01i_span_scope_attributes.md - tasks inherit scoped attributes from calling context
        const parentLogger = (requestCtx as RequestContext<FF, Env> & { log?: SpanLogger<T> }).log;
        const inheritedScopeValues = parentLogger?._getScope?.()._getScopeValues() || {};

        // Create span logger with typed tag methods (with inherited scope values)
        const spanLogger = createSpanLogger(schemaOnly, spanBuffer, inheritedScopeValues);

        // Create span context
        // Note: spanLogger.tag returns the chainable tag API
        // We expose it directly on ctx.tag for cleaner API: ctx.tag.userId() instead of ctx.log.tag.userId()
        const spanContext: SpanContext<T, FF, Env> = {
          ...requestCtx,
          tag: spanLogger.tag as ChainableTagAPI<T>,
          log: spanLogger,

          ok<V>(value: V): FluentSuccessResult<V, T> {
            return new FluentSuccessResult<V, T>(spanBuffer, value, schemaOnly);
          },

          err<E>(code: string, error: E): FluentErrorResult<E, T> {
            return new FluentErrorResult<E, T>(spanBuffer, code, error, schemaOnly);
          },

          async span<R>(childName: string, childFn: (ctx: SpanContext<T, FF, Env>) => Promise<R>): Promise<R> {
            // Create child span buffer with Arrow builders
            const childBuffer = createChildSpanBuffer(spanBuffer, taskContext);

            // Write span-start for child span (row 0) and pre-initialize span-end (row 1)
            writeSpanStart(childBuffer, childName);

            // Inherit scoped values from parent span via Scope instance
            // Per specs/01i_span_scope_attributes.md - child spans inherit parent's scoped attributes
            const parentScopeValues = spanLogger._getScope()._getScopeValues();

            // Create child context with its own logger (with inherited scope values)
            const childLogger = createSpanLogger(schemaOnly, childBuffer, parentScopeValues);

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
              childBuffer.timestamps[1] = getTimestampNanos();

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
          spanBuffer.timestamps[1] = getTimestampNanos();

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
