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
  createSpanLogger as createSpanLoggerFromGenerator,
  type SpanLoggerImpl,
} from './codegen/spanLoggerGenerator.js';
import { Op } from './op.js';
import type { SpanContext, SpanFn } from './opContext/types.js';
import { FluentErr, FluentOk, type Result } from './result.js';
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
// Type Exports
// =============================================================================

// Public SpanLogger type is defined in opContext/spanContextTypes.ts
// It extends BaseSpanLogger (core logging methods) with ColumnWriter (schema methods)
// This file defines SpanLoggerInternal which extends SpanLoggerImpl (includes internal methods)

/**
 * Internal SpanLogger type with FF methods and internal methods exposed.
 * Used by FeatureFlagEvaluator and internal span management code.
 */
export type SpanLoggerInternal<T extends LogSchema> = SpanLoggerImpl<T> & {
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

// SpanFn and SpanContext are defined in opContext/spanContextTypes.ts
// Import them for use in this implementation file

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
  _spanLogger: SpanLoggerImpl<T>;
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
export function createSpanLogger<T extends LogSchema>(schema: T, buffer: SpanBuffer<T>): SpanLoggerImpl<T> {
  // Create the SpanLogger - it will read/write scope via buffer._scopeValues
  return createSpanLoggerFromGenerator(schema, buffer, createOverflowBuffer) as SpanLoggerImpl<T>;
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

// =============================================================================
// Re-export public types from opContext/types
// =============================================================================

export type { FluentLogEntry, SpanContext, SpanFn, SpanLogger } from './opContext/types.js';
