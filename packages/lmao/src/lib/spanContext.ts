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
import { Op } from './op.js';
import { FluentErr, FluentOk } from './result.js';
import type { FeatureFlagSchema } from './schema/defineFeatureFlags.js';
import type { FeatureFlagEvaluator, InferFeatureFlagsWithContext } from './schema/evaluator.js';
import { ENTRY_TYPE_SPAN_EXCEPTION, ENTRY_TYPE_SPAN_START } from './schema/systemSchema.js';
import type { InferSchema, LogSchema } from './schema/types.js';
import { createChildSpanBuffer, createOverflowBuffer } from './spanBuffer.js';
import { getTimestampNanos } from './timestamp.js';
import type { LogBinding, ModuleContext, SpanBuffer } from './types.js';

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

  /** Set error code for this entry */
  error_code(code: string): void;

  /** Set exception stack for this entry */
  exception_stack(stack: string): void;

  /** Set feature flag value for this entry */
  ff_value(value: string): void;

  /** Set uint64 value for this entry */
  uint64_value(value: bigint): void;
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
    ctx: Ctx,
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
  <R>(line: number, name: string, fn: (ctx: SpanContext<T, FF, Env>) => Promise<R>): Promise<R>;

  // Overload 4: Without line number, with ctx override
  <R, Args extends unknown[], Ctx extends SpanContext<T, FF, Env>>(
    name: string,
    ctx: Ctx,
    op: Op<Ctx, Args, R>,
    ...args: Args
  ): Promise<R>;

  // Overload 5: Without line number, without ctx override (most common)
  <R, Args extends unknown[]>(name: string, op: Op<SpanContext<T, FF, Env>, Args, R>, ...args: Args): Promise<R>;

  // Overload 6: Without line number, inline closure
  <R>(name: string, fn: (ctx: SpanContext<T, FF, Env>) => Promise<R>): Promise<R>;
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
  ok<V>(value: V): FluentOk<V, T>;

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
  err<E>(code: string, error: E): FluentErr<E, T>;

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
  readonly buffer: SpanBuffer<T>;
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
export interface MutableSpanContext<T extends LogSchema, FF extends FeatureFlagSchema, Env = Record<string, unknown>>
  extends SpanContext<T, FF, Env> {
  [SPAN_CONTEXT_MARKER]: true;
  _logBinding: ModuleContext;
  callee_package: string;
  callee_file: string;
  callee_line: number;
  callee_git_sha: string;
  ff: FeatureFlagEvaluator<FF> & InferFeatureFlagsWithContext<FF>;
  env: Env;
  deps: Record<string, unknown>;
  tag: TagWriter<T>;
  log: SpanLogger<T>;
  _buffer: SpanBuffer<T>;
  _schema: T;
  _spanLogger: BaseSpanLogger<T>;
  buffer: SpanBuffer<T>;
  setScope: (attributes: Partial<InferSchema<T> | null>) => void;
  scope: Readonly<Partial<InferSchema<T>>>;
  ok: <V>(value: V) => FluentOk<V, T>;
  err: <E>(code: string, error: E) => FluentErr<E, T>;
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
  buffer.timestamp[0] = getTimestampNanos(buffer._traceRoot.anchorEpochNanos, buffer._traceRoot.anchorPerfNow);
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
  return createSpanLoggerFromGenerator(schema, buffer, createOverflowBuffer) as BaseSpanLogger<T>;
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
 * @param logBinding - LogBinding with schema and stats (ModuleContext alias)
 * @returns Prototype object for SpanContext instances
 *
 * @internal
 */
export function createSpanContextProto<
  T extends LogSchema,
  FF extends FeatureFlagSchema,
  Env = Record<string, unknown>,
>(schemaOnly: T, logBinding: LogBinding): Record<string | symbol, unknown> {
  return {
    [SPAN_CONTEXT_MARKER]: true as const,

    // OpMetadata getter - returns _buffer._opMetadata
    get module(): ModuleContext {
      return (this as any).buffer._opMetadata;
    },
    get callee_package(): string {
      return (this as any).buffer._opMetadata.package_name;
    },
    get callee_file(): string {
      return (this as any).buffer._opMetadata.package_file;
    },
    get callee_line(): number {
      return (this as any).buffer.line(0);
    },
    get callee_git_sha(): string {
      return (this as any).buffer._opMetadata.git_sha;
    },

    // Buffer getter - returns _buffer
    get buffer(): SpanBuffer<T> {
      return (this as unknown as MutableSpanContext<T, FF, Env>)._buffer;
    },

    // Scope getter - returns current scope values (always a frozen object, never undefined)
    // Buffer._scopeValues is initialized to EMPTY_SCOPE and only replaced with frozen objects
    get scope(): Readonly<Partial<InferSchema<T>>> {
      const buffer = (this as unknown as MutableSpanContext<T, FF, Env>)._buffer;
      return buffer._scopeValues as Readonly<Partial<InferSchema<T>>>;
    },

    // setScope method - delegates to spanLogger._setScope
    setScope(this: MutableSpanContext<T, FF, Env>, attributes: Partial<InferSchema<T> | null>): void {
      // Cast needed because _setScope expects Partial<InferSchema<T>> but we accept null values
      this._spanLogger._setScope(attributes as Partial<InferSchema<T>>);
    },

    // Ok method - creates FluentOk
    ok<V>(this: MutableSpanContext<T, FF, Env>, value: V): FluentOk<V, T> {
      return new FluentOk<V, T>(this._buffer, value, this._schema);
    },

    // Err method - creates FluentErr
    err<E>(this: MutableSpanContext<T, FF, Env>, code: string, error: E): FluentErr<E, T> {
      return new FluentErr<E, T>(this._buffer, code, error, this._schema);
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
      // Call op.fn with parent context (ctx parameter) after creating child span
      // The Op will use Object.create(ctx) to inherit user properties via prototype chain
      // callsiteMetadata is the CURRENT span's OpMetadata - where span() was invoked from
      // TODO: span_op should create child span buffer and call op.fn() directly
      // For now, cast to any to call fn (the class has fn, not _invoke)
      return (op as unknown as { fn: (ctx: any, ...args: Args) => Promise<R> }).fn(
        ctx as SpanContext<LogSchema, FF, Record<string, unknown>>,
        ...args,
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
      // Uses same logBinding as parent - span_fn creates child spans within same logging context
      // callsiteMetadata is the CURRENT op's metadata (for row 0 attribution)
      const childBuffer = createChildSpanBuffer(
        this._buffer,
        logBinding,
        name,
        this._buffer._opMetadata, // callsiteMetadata - current op's metadata for row 0
      ) as SpanBuffer<T>;

      // Explicit registration with parent's children array
      this._buffer._children.push(childBuffer);

      // Write span-start for child span (row 0) and pre-initialize span-end (row 1)
      writeSpanStart(childBuffer, name);

      // Write line number to row 0
      childBuffer.line(0, line);

      // Create child span logger (scope inheritance handled by buffer constructor)
      const childLogger = createSpanLogger(schemaOnly, childBuffer);

      // Create tag writer for child span attributes (writes to row 0)
      const childTagAPI = createTagWriter(schemaOnly, childBuffer);

      // Use Object.create(ctx) for prototype inheritance (ctx may be overridden)
      const childContext = Object.create(ctx) as MutableSpanContext<T, FF, Env>;

      // Assign child-specific properties directly (stable hidden class)
      // User properties (env, deps, etc.) are inherited via prototype chain from ctx
      childContext.tag = childTagAPI as SpanContext<T, FF, Env>['tag'];
      childContext.log = childLogger as SpanLogger<T>;
      childContext._buffer = childBuffer as SpanBuffer<T>;
      childContext._spanLogger = childLogger;

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
        childBuffer.timestamp[1] = getTimestampNanos(
          childBuffer._traceRoot.anchorEpochNanos,
          childBuffer._traceRoot.anchorPerfNow,
        );

        // Write exception details to row 1
        const errorMessage = error instanceof Error ? error.message : String(error);
        const errorStack = error instanceof Error ? error.stack : undefined;

        childBuffer.message(1, errorMessage);
        if (errorStack) {
          childBuffer.exception_stack(1, errorStack);
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
        !(restAfterName[0] instanceof Op)
      ) {
        // Context override detected - create child context inheriting from this via prototype
        // Override properties are spread onto the new object as own properties
        // User properties (env, deps, etc.) still inherited via prototype chain
        ctxOverride = Object.assign(Object.create(this), restAfterName[0]) as SpanContext<T, FF, Env>;
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

      if (opOrFn instanceof Op) {
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
