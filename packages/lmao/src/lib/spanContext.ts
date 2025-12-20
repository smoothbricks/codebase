/**
 * SpanContext - Context provided to op functions during span execution
 *
 * Per specs/01c_context_flow_and_op_wrappers.md and specs/01l_module_builder_pattern.md:
 * - SpanContext combines built-in properties with user-extensible Extra
 * - Uses prototype-based inheritance for V8 hidden class optimization
 * - Child spans inherit scoped attributes from parents
 */

import type { TagWriter } from './codegen/fixedPositionWriterGenerator.js';
import { createTagWriter } from './codegen/fixedPositionWriterGenerator.js';
import {
  type BaseSpanLogger,
  createSpanLogger as createSpanLoggerFromGenerator,
} from './codegen/spanLoggerGenerator.js';
import type { Op, OpBrand } from './op.js';
import { FluentErrorResult, FluentSuccessResult } from './result.js';
import type { FeatureFlagSchema } from './schema/defineFeatureFlags.js';
import type { FeatureFlagEvaluator, InferFeatureFlagsWithContext } from './schema/evaluator.js';
import { ENTRY_TYPE_SPAN_EXCEPTION, ENTRY_TYPE_SPAN_START } from './schema/systemSchema.js';
import type { InferSchema, LogSchema } from './schema/types.js';
import { createChildSpanBuffer, createNextBuffer } from './spanBuffer.js';
import { getTimestampNanos } from './timestamp.js';
import type { TraceContext } from './traceContext.js';
import type { TraceId } from './traceId.js';
import type { ModuleContext, SpanBuffer } from './types.js';

// =============================================================================
// SpanContext Symbol Marker
// =============================================================================

/**
 * Internal symbol to mark span contexts
 * Used for type guards and prototype chain detection
 */
export const SPAN_CONTEXT_MARKER = Symbol.for('lmao.SpanContext');

// =============================================================================
// FluentLogEntry Type
// =============================================================================

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
   * Per specs/01c_context_flow_and_op_wrappers.md "Line Number System":
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

// =============================================================================
// SpanLogger Type Alias
// =============================================================================

/**
 * SpanLogger type - alias for BaseSpanLogger which includes
 * schema-specific setter methods via ColumnWriter<T>.
 */
export type SpanLogger<T extends LogSchema> = BaseSpanLogger<T>;

/**
 * Internal SpanLogger type with FF methods (not on public type).
 * Methods exist at runtime but are hidden from TypeScript users.
 */
export type SpanLoggerInternal<T extends LogSchema> = SpanLogger<T> & {
  /**
   * Write feature flag access entry (internal, not on public type).
   * Called by FeatureFlagEvaluator to log flag access.
   */
  ffAccess(flagName: string, value: unknown): void;
  /**
   * Write feature flag usage entry (internal, not on public type).
   * Called by FeatureFlagEvaluator to log flag usage.
   */
  ffUsage(flagName: string, context?: Record<string, unknown>): void;
};

// =============================================================================
// SpanFn Types
// =============================================================================

/**
 * SpanFn type for ctx.span() - creates a child span
 *
 * Per spec 01l lines 474-511 and 01o lines 34-72:
 * Supports 6 overloads for Op invocation and inline closures, with/without line numbers.
 *
 * The transformer injects line numbers as the first argument, but runtime supports both.
 */
export type SpanFn<T extends LogSchema, FF extends FeatureFlagSchema, Env> = {
  // Overload 1: With line number, with ctx override
  <R, Args extends unknown[], Ctx extends SpanContext<T, FF, Env>>(
    line: number,
    name: string,
    ctx: Ctx & { [OpBrand]?: never },
    op: Op<Ctx, Args, R>,
    ...args: Args
  ): Promise<R>;

  // Overload 2: With line number, without ctx override
  <R, Args extends unknown[]>(
    line: number,
    name: string,
    op: Op<SpanContext<T, FF, Env>, Args, R>,
    ...args: Args
  ): Promise<R>;

  // Overload 3: With line number, inline closure
  <R>(
    line: number,
    name: string,
    fn: ((ctx: SpanContext<T, FF, Env>) => Promise<R>) & { [OpBrand]?: never },
  ): Promise<R>;

  // Overload 4: Without line number, with ctx override
  <R, Args extends unknown[], Ctx extends SpanContext<T, FF, Env>>(
    name: string,
    ctx: Ctx & { [OpBrand]?: never },
    op: Op<Ctx, Args, R>,
    ...args: Args
  ): Promise<R>;

  // Overload 5: Without line number, without ctx override (most common)
  <R, Args extends unknown[]>(name: string, op: Op<SpanContext<T, FF, Env>, Args, R>, ...args: Args): Promise<R>;

  // Overload 6: Without line number, inline closure
  <R>(name: string, fn: ((ctx: SpanContext<T, FF, Env>) => Promise<R>) & { [OpBrand]?: never }): Promise<R>;
};

// =============================================================================
// SpanContext Interface
// =============================================================================

/**
 * SpanContext - what ops receive when invoked
 *
 * Per spec 01l lines 747-762:
 * Combines built-in properties with user-extensible Extra via intersection at usage site.
 *
 * @typeParam T - Tag attribute schema for this module
 * @typeParam FF - Feature flag schema
 * @typeParam Env - Environment configuration type
 *
 * @example
 * ```typescript
 * const createUser = task('create-user', async (ctx, userData) => {
 *   // Scoped attributes (propagate to all log entries)
 *   ctx.setScope({ userId: userData.id });
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
export interface SpanContext<T extends LogSchema, FF extends FeatureFlagSchema, Env = Record<string, unknown>> {
  /** Marker for prototype chain detection */
  readonly [SPAN_CONTEXT_MARKER]: true;

  /** Trace ID for this span's trace */
  readonly traceId: TraceId;

  /**
   * Feature flags (logs access to current span)
   */
  readonly ff: FeatureFlagEvaluator<FF> & InferFeatureFlagsWithContext<FF>;

  /**
   * Environment configuration
   */
  readonly env: Env;

  /**
   * Dependencies - can be destructured!
   *
   * Per spec 01l lines 757-758:
   * - Plain object references to dependency modules/ops
   * - Zero-allocation deps (not closures)
   *
   * @example
   * const { retry, auth } = deps;
   * await span('retry', retry, 1);
   */
  readonly deps: Record<string, unknown>;

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
   * and inherited by child spans. Uses merge semantics - pass null to clear a value.
   *
   * @param attributes - Attributes to scope to this span (null clears a value)
   *
   * @example
   * ctx.setScope({ requestId: req.id, userId: req.user?.id });
   * ctx.log.info('Processing'); // Includes requestId and userId
   * ctx.setScope({ userId: null }); // Clear userId from scope
   */
  setScope(attributes: Partial<InferSchema<T> | null>): void;

  /**
   * Read-only view of current scoped attributes
   *
   * Returns the frozen scope object containing all currently scoped values.
   * Use setScope() to modify scope values.
   *
   * @example
   * ctx.setScope({ requestId: 'r1' });
   * console.log(ctx.scope.requestId); // 'r1'
   */
  readonly scope: Readonly<Partial<InferSchema<T>>>;

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
   * @param line - Optional source line number (injected by transformer)
   * @returns Promise resolving to the child function's return value
   *
   * @example
   * const result = await ctx.span('validate', async (childCtx) => {
   *   childCtx.tag.step('validation');
   *   return childCtx.ok({ valid: true });
   * });
   */
  span: SpanFn<T, FF, Env>;

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

// =============================================================================
// MutableSpanContext (internal)
// =============================================================================

/**
 * Internal type for mutable SpanContext during construction.
 * After construction, the context is returned as readonly SpanContext.
 *
 * @internal
 */
export interface MutableSpanContext<T extends LogSchema, FF extends FeatureFlagSchema, Env = Record<string, unknown>> {
  [SPAN_CONTEXT_MARKER]: true;
  traceId: TraceId;
  ff: FeatureFlagEvaluator<FF> & InferFeatureFlagsWithContext<FF>;
  env: Env;
  deps: Record<string, unknown>;
  tag: TagWriter<T>;
  log: SpanLogger<T>;
  _buffer: SpanBuffer<T>;
  _schema: T;
  _spanLogger: BaseSpanLogger<T>;
  _traceCtx?: TraceContext<FF, Record<string, unknown>>; // Hidden reference to root trace context for Op invocation
  buffer: SpanBuffer<T>;
  setScope: (attributes: Partial<InferSchema<T> | null>) => void;
  scope: Readonly<Partial<InferSchema<T>>>;
  ok: <V>(value: V) => FluentSuccessResult<V, T>;
  err: <E>(code: string, error: E) => FluentErrorResult<E, T>;
  span: SpanFn<T, FF, Env>;
  span_op: <R, Args extends unknown[]>(
    line: number,
    name: string,
    ctx: SpanContext<T, FF, Env>,
    op: Op<SpanContext<T, FF, Env>, Args, R>,
    ...args: Args
  ) => Promise<R>;
  span_fn: <R>(
    line: number,
    name: string,
    ctx: SpanContext<T, FF, Env>,
    fn: (ctx: SpanContext<T, FF, Env>) => Promise<R>,
  ) => Promise<R>;
  [key: string]: unknown;
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Write span-start entry to buffer at row 0 (fixed layout)
 * Pre-initialize row 1 as span-exception (will be overwritten by ok/err)
 * Set writeIndex to 2 (events start after reserved rows)
 *
 * Per specs/01h_entry_types_and_logging_primitives.md:
 * - Row 0: span-start (written here)
 * - Row 1: span-end (pre-initialized as exception, overwritten by ok/err)
 * - Row 2+: events (ctx.log.* appends here)
 *
 * @param buffer - SpanBuffer to write to
 * @param spanName - Name for this span
 */
export function writeSpanStart<T extends LogSchema>(buffer: SpanBuffer<T>, spanName: string): void {
  // Row 0: span-start (fixed layout)
  buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
  buffer.timestamp[0] = getTimestampNanos();
  buffer.message(0, spanName); // Unified message column for span name

  // Row 1: pre-initialize as span-exception (will be overwritten on ok/err)
  buffer.entry_type[1] = ENTRY_TYPE_SPAN_EXCEPTION;
  buffer.timestamp[1] = 0n; // Will be set on completion

  // Events start at row 2
  buffer._writeIndex = 2;
}

/**
 * Create a SpanLogger for the given buffer.
 *
 * Per specs/01i_span_scope_attributes.md: Scope values are stored directly on buffer._scopeValues.
 * The SpanLogger reads/writes scope via buffer._scopeValues - no separate Scope class needed.
 *
 * @param schema - Tag attribute schema with field definitions
 * @param buffer - SpanBuffer to write entries to (per-span instance)
 * @returns SpanLogger with typed methods matching schema
 */
export function createSpanLogger<T extends LogSchema>(schema: T, buffer: SpanBuffer<T>): BaseSpanLogger<T> {
  // Create the SpanLogger - it will read/write scope via buffer._scopeValues
  return createSpanLoggerFromGenerator(schema, buffer, createNextBuffer) as BaseSpanLogger<T>;
}

/**
 * Create the SpanContext prototype with shared methods.
 *
 * These methods are defined ONCE and inherited by all span contexts,
 * avoiding per-instance function allocations.
 *
 * Per V8 optimization guidelines:
 * - Define methods ONCE on prototype, not per-instance
 * - Use Object.create() for inheritance chains
 * - Avoid object spreads which break hidden classes
 *
 * @param schemaOnly - Tag attribute schema
 * @param taskContext - Task context with module metadata
 * @returns Prototype object for SpanContext instances
 *
 * @internal
 */
export function createSpanContextProto<
  T extends LogSchema,
  FF extends FeatureFlagSchema,
  Env = Record<string, unknown>,
>(schemaOnly: T, moduleContext: ModuleContext): Record<string | symbol, unknown> {
  return {
    [SPAN_CONTEXT_MARKER]: true as const,

    // Buffer getter - returns _buffer
    get buffer(): SpanBuffer<T> {
      return (this as unknown as MutableSpanContext<T, FF, Env>)._buffer;
    },

    // Scope getter - returns current scope values as frozen object
    get scope(): Readonly<Partial<InferSchema<T>>> {
      const buffer = (this as unknown as MutableSpanContext<T, FF, Env>)._buffer;
      return (buffer._scopeValues as Readonly<Partial<InferSchema<T>>>) ?? Object.freeze({});
    },

    // setScope method - delegates to spanLogger._setScope
    setScope(this: MutableSpanContext<T, FF, Env>, attributes: Partial<InferSchema<T> | null>): void {
      // Cast needed because _setScope expects Partial<InferSchema<T>> but we accept null values
      this._spanLogger._setScope(attributes as Partial<InferSchema<T>>);
    },

    // Ok method - creates FluentSuccessResult
    ok<V>(this: MutableSpanContext<T, FF, Env>, value: V): FluentSuccessResult<V, T> {
      return new FluentSuccessResult<V, T>(this._buffer, value, this._schema);
    },

    // Err method - creates FluentErrorResult
    err<E>(this: MutableSpanContext<T, FF, Env>, code: string, error: E): FluentErrorResult<E, T> {
      return new FluentErrorResult<E, T>(this._buffer, code, error, this._schema);
    },

    // Monomorphic span_op - Op invocation (per spec 01o lines 46-49)
    span_op<R, Args extends unknown[]>(
      this: MutableSpanContext<T, FF, Env>,
      line: number,
      name: string,
      ctx: SpanContext<T, FF, Env>,
      op: Op<SpanContext<T, FF, Env>, Args, R>,
      ...args: Args
    ): Promise<R> {
      // Direct property access - V8 optimized (no prototype walk!)
      // ctx parameter always has _traceCtx set directly when created
      const traceCtx = (ctx as unknown as MutableSpanContext<T, FF, Env>)._traceCtx;
      if (!traceCtx) {
        throw new Error('TraceContext not found - cannot invoke Op');
      }

      // Call op._invoke with proper parameters
      // callsiteModule is the current span's module (buffer._module)
      // Cast needed: SpanBuffer<T> -> SpanBuffer (TypeScript variance limitation with index signatures)
      return op._invoke(
        this._traceCtx as TraceContext<FF, Record<string, unknown>>,
        this._buffer as SpanBuffer,
        moduleContext,
        name,
        line,
        args,
      );
    },

    // Monomorphic span_fn - inline closure (per spec 01o lines 51-53)
    async span_fn<R>(
      this: MutableSpanContext<T, FF, Env>,
      line: number,
      name: string,
      ctx: SpanContext<T, FF, Env>,
      fn: (ctx: SpanContext<T, FF, Env>) => Promise<R>,
    ): Promise<R> {
      // Create child span buffer
      // Uses same module as parent - span_fn creates child spans within same module context
      const childBuffer = createChildSpanBuffer(this._buffer as SpanBuffer, moduleContext, name) as SpanBuffer<T>;

      // Explicit registration with parent's children array
      this._buffer._children.push(childBuffer as SpanBuffer);

      // Write span-start for child span (row 0) and pre-initialize span-end (row 1)
      writeSpanStart(childBuffer, name);

      // Write line number to row 0
      childBuffer.lineNumber(0, line);

      // Create child span logger (scope inheritance handled by buffer constructor)
      const childLogger = createSpanLogger(schemaOnly, childBuffer);

      // Create tag writer for child span attributes (writes to row 0)
      const childTagAPI = createTagWriter(schemaOnly, childBuffer);

      // Use Object.create(ctx) for prototype inheritance (ctx may be overridden)
      const childContext = Object.create(ctx) as MutableSpanContext<T, FF, Env>;

      // Assign child-specific properties directly (stable hidden class)
      childContext.tag = childTagAPI as SpanContext<T, FF, Env>['tag'];
      childContext.log = childLogger as SpanLogger<T>;
      childContext._buffer = childBuffer as SpanBuffer<T>;
      childContext._spanLogger = childLogger;
      // ALWAYS copy _traceCtx directly (V8 optimization - no prototype access)
      childContext._traceCtx = (ctx as unknown as MutableSpanContext<T, FF, Env>)._traceCtx;

      // Create a new feature flag evaluator bound to the CHILD span context
      // Must be after childContext is created since forContext receives the full SpanContext
      const childFf = this.ff.forContext?.(
        childContext as unknown as SpanContext<T, FF, Env>,
      ) as unknown as FeatureFlagEvaluator<FF> & InferFeatureFlagsWithContext<FF>;
      childContext.ff = childFf;

      // Execute child span with exception handling (direct async, no IIFE)
      try {
        return await fn(childContext as unknown as SpanContext<T, FF, Env>);
      } catch (error) {
        // Write span-exception to row 1 (fixed layout)
        childBuffer.timestamp[1] = getTimestampNanos();

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
    },

    // Polymorphic span dispatcher - fallback without transformer (per spec 01o lines 55-71)
    span(this: MutableSpanContext<T, FF, Env>, lineOrName: number | string, ...rest: unknown[]): Promise<unknown> {
      // Parse arguments to determine: line, name, override?, op|fn, args
      const hasLine = typeof lineOrName === 'number';
      const line = hasLine ? lineOrName : 0;
      const name = hasLine ? (rest[0] as string) : (lineOrName as string);
      const restAfterName = hasLine ? rest.slice(1) : rest;

      // Check for context override (second arg is object literal, not Op/fn)
      let ctxOverride: SpanContext<T, FF, Env> | undefined;
      let opOrFn:
        | Op<SpanContext<T, FF, Env>, unknown[], unknown>
        | ((ctx: SpanContext<T, FF, Env>) => Promise<unknown>)
        | undefined;
      let args: unknown[];

      if (
        restAfterName.length > 0 &&
        typeof restAfterName[0] === 'object' &&
        restAfterName[0] !== null &&
        !('_invoke' in restAfterName[0])
      ) {
        // Context override detected
        ctxOverride = Object.assign(Object.create(this), restAfterName[0]) as SpanContext<T, FF, Env>;
        // CRITICAL: Copy _traceCtx directly onto the new object for V8 optimization
        (ctxOverride as unknown as MutableSpanContext<T, FF, Env>)._traceCtx = (
          this as MutableSpanContext<T, FF, Env>
        )._traceCtx;
        opOrFn = restAfterName[1] as
          | Op<SpanContext<T, FF, Env>, unknown[], unknown>
          | ((ctx: SpanContext<T, FF, Env>) => Promise<unknown>);
        args = restAfterName.slice(2);
      } else {
        // No context override
        opOrFn = restAfterName[0] as
          | Op<SpanContext<T, FF, Env>, unknown[], unknown>
          | ((ctx: SpanContext<T, FF, Env>) => Promise<unknown>);
        args = restAfterName.slice(1);
      }

      // Determine if op or fn
      const ctxToUse = ctxOverride ?? (this as unknown as SpanContext<T, FF, Env>);

      if (opOrFn && '_invoke' in opOrFn) {
        // It's an Op
        return this.span_op(line, name, ctxToUse, opOrFn as Op<SpanContext<T, FF, Env>, unknown[], unknown>, ...args);
      }
      if (typeof opOrFn === 'function') {
        // It's a function
        return this.span_fn(line, name, ctxToUse, opOrFn as (ctx: SpanContext<T, FF, Env>) => Promise<unknown>);
      }

      throw new Error('Invalid span() call - expected Op or function');
    },
  };
}

// =============================================================================
// Type Guard
// =============================================================================

/**
 * Type guard to check if a value is a SpanContext
 */
export function isSpanContext<T extends LogSchema, FF extends FeatureFlagSchema, Env>(
  value: unknown,
): value is SpanContext<T, FF, Env> {
  return (
    typeof value === 'object' &&
    value !== null &&
    SPAN_CONTEXT_MARKER in value &&
    (value as Record<symbol, unknown>)[SPAN_CONTEXT_MARKER] === true
  );
}
