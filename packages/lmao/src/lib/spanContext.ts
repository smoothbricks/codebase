/**
 * SpanContext - Context provided to op functions during span execution
 *
 * Per specs/01c_context_flow_and_op_wrappers.md and specs/01l_module_builder_pattern.md:
 * - SpanContext combines built-in properties with user-extensible Extra
 * - Uses prototype-based inheritance for V8 hidden class optimization
 * - Child spans inherit scoped attributes from parents
 *
 * ## Class-Based Design
 *
 * SpanContext is implemented as a class factory pattern:
 * - `createSpanContextClass()` returns a class that closes over schema/logBinding
 * - Child contexts use `Object.create(parent)` for prototype inheritance
 * - User properties (deps, ff, env) are inherited via prototype chain
 * - Context overrides work by setting own properties that shadow inherited ones
 *
 * ## Method Binding for Destructuring
 *
 * Methods that users commonly destructure (span, ok, err, setScope) are defined
 * as arrow function properties. This means:
 * - They're own properties on each instance (slight memory overhead)
 * - But destructuring works without explicit .bind() calls
 * - const { span, ok, err } = ctx; // Just works!
 */

import type { TagWriter } from './codegen/fixedPositionWriterGenerator.js';
import { createTagWriter } from './codegen/fixedPositionWriterGenerator.js';
import {
  createSpanLogger as createSpanLoggerFromGenerator,
  type SpanLoggerImpl,
} from './codegen/spanLoggerGenerator.js';
import type { RemappedViewConstructor } from './logBinding.js';
import { Op } from './op.js';
import type { OpContext, OpMetadata, SpanContext, SpanFn, SpanLogger } from './opContext/types.js';
import { FluentErr, FluentOk, type Result } from './result.js';
import type { FeatureFlagEvaluator, InferFeatureFlagsWithContext } from './schema/evaluator.js';
import { ENTRY_TYPE_SPAN_EXCEPTION, ENTRY_TYPE_SPAN_START } from './schema/systemSchema.js';
import type { InferSchema, LogSchema } from './schema/types.js';
import { createChildSpanBuffer, createOverflowBuffer, type SpanBufferConstructor } from './spanBuffer.js';
import { getTimestampNanos } from './timestamp.js';
import type { LogBinding, SpanBuffer } from './types.js';

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
  return createSpanLoggerFromGenerator(schema, buffer, createOverflowBuffer) as SpanLoggerImpl<T>;
}

// =============================================================================
// SpanContext Class Types
// =============================================================================

/**
 * Constructor type for SpanContext classes created by createSpanContextClass.
 * Takes buffer, schema, spanLogger, tag directly (no temp object allocation).
 */
export type SpanContextClass<Ctx extends OpContext> = new (
  buffer: SpanBuffer<Ctx['logSchema']>,
  schema: Ctx['logSchema'],
  spanLogger: SpanLoggerImpl<Ctx['logSchema']>,
  tag: TagWriter<Ctx['logSchema']>,
) => SpanContextInstance<Ctx>;

/**
 * Instance type of SpanContext class - what ops actually receive.
 * This is a type alias that extends the public SpanContext with internal properties.
 */
export type SpanContextInstance<Ctx extends OpContext> = SpanContext<Ctx> & {
  // Internal properties
  _buffer: SpanBuffer<Ctx['logSchema']>;
  _schema: Ctx['logSchema'];
  _spanLogger: SpanLoggerImpl<Ctx['logSchema']>;
  _logBinding: LogBinding;

  // Internal methods
  _newCtx0(): SpanContextInstance<Ctx>;
  _newCtx1(overrides: Record<string, unknown>): SpanContextInstance<Ctx>;
  _spanPre(
    childCtx: SpanContextInstance<Ctx>,
    line: number,
    name: string,
    SpanBufferClass: SpanBufferConstructor,
    remappedViewClass: import('./logBinding.js').RemappedViewConstructor | undefined,
    opMetadata: import('./opContext/opTypes.js').OpMetadata,
  ): SpanContextInstance<Ctx>;
  _spanException(buffer: SpanBuffer<Ctx['logSchema']>, error: unknown): void;

  // Monomorphic span methods (span0-span8)
  // Transformer emits: ctx.span0(line, name, ctx, SpanBufferClass, remappedViewClass, opMetadata, fn, ...args)
  span0<S, E>(
    line: number,
    name: string,
    childCtx: SpanContext<Ctx>,
    SpanBufferClass: SpanBufferConstructor,
    remappedViewClass: import('./logBinding.js').RemappedViewConstructor | undefined,
    opMetadata: import('./opContext/opTypes.js').OpMetadata,
    fn: (ctx: SpanContext<Ctx>) => Result<S, E> | Promise<Result<S, E>>,
  ): Promise<Result<S, E>>;
  span1<S, E, A1>(
    line: number,
    name: string,
    childCtx: SpanContext<Ctx>,
    SpanBufferClass: SpanBufferConstructor,
    remappedViewClass: RemappedViewConstructor | undefined,
    opMetadata: OpMetadata,
    fn: (ctx: SpanContext<Ctx>, a1: A1) => Result<S, E> | Promise<Result<S, E>>,
    a1: A1,
  ): Promise<Result<S, E>>;
  span2<S, E, A1, A2>(
    line: number,
    name: string,
    childCtx: SpanContext<Ctx>,
    SpanBufferClass: SpanBufferConstructor,
    remappedViewClass: RemappedViewConstructor | undefined,
    opMetadata: OpMetadata,
    fn: (ctx: SpanContext<Ctx>, a1: A1, a2: A2) => Result<S, E> | Promise<Result<S, E>>,
    a1: A1,
    a2: A2,
  ): Promise<Result<S, E>>;
  span3<S, E, A1, A2, A3>(
    line: number,
    name: string,
    childCtx: SpanContext<Ctx>,
    SpanBufferClass: SpanBufferConstructor,
    remappedViewClass: RemappedViewConstructor | undefined,
    opMetadata: OpMetadata,
    fn: (ctx: SpanContext<Ctx>, a1: A1, a2: A2, a3: A3) => Result<S, E> | Promise<Result<S, E>>,
    a1: A1,
    a2: A2,
    a3: A3,
  ): Promise<Result<S, E>>;
  span4<S, E, A1, A2, A3, A4>(
    line: number,
    name: string,
    childCtx: SpanContext<Ctx>,
    SpanBufferClass: SpanBufferConstructor,
    remappedViewClass: RemappedViewConstructor | undefined,
    opMetadata: OpMetadata,
    fn: (ctx: SpanContext<Ctx>, a1: A1, a2: A2, a3: A3, a4: A4) => Result<S, E> | Promise<Result<S, E>>,
    a1: A1,
    a2: A2,
    a3: A3,
    a4: A4,
  ): Promise<Result<S, E>>;
  span5<S, E, A1, A2, A3, A4, A5>(
    line: number,
    name: string,
    childCtx: SpanContext<Ctx>,
    SpanBufferClass: SpanBufferConstructor,
    remappedViewClass: RemappedViewConstructor | undefined,
    opMetadata: OpMetadata,
    fn: (ctx: SpanContext<Ctx>, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5) => Result<S, E> | Promise<Result<S, E>>,
    a1: A1,
    a2: A2,
    a3: A3,
    a4: A4,
    a5: A5,
  ): Promise<Result<S, E>>;
  span6<S, E, A1, A2, A3, A4, A5, A6>(
    line: number,
    name: string,
    childCtx: SpanContext<Ctx>,
    SpanBufferClass: SpanBufferConstructor,
    remappedViewClass: RemappedViewConstructor | undefined,
    opMetadata: OpMetadata,
    fn: (ctx: SpanContext<Ctx>, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6) => Result<S, E> | Promise<Result<S, E>>,
    a1: A1,
    a2: A2,
    a3: A3,
    a4: A4,
    a5: A5,
    a6: A6,
  ): Promise<Result<S, E>>;
  span7<S, E, A1, A2, A3, A4, A5, A6, A7>(
    line: number,
    name: string,
    childCtx: SpanContext<Ctx>,
    SpanBufferClass: SpanBufferConstructor,
    remappedViewClass: RemappedViewConstructor | undefined,
    opMetadata: OpMetadata,
    fn: (
      ctx: SpanContext<Ctx>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
      a5: A5,
      a6: A6,
      a7: A7,
    ) => Result<S, E> | Promise<Result<S, E>>,
    a1: A1,
    a2: A2,
    a3: A3,
    a4: A4,
    a5: A5,
    a6: A6,
    a7: A7,
  ): Promise<Result<S, E>>;
  span8<S, E, A1, A2, A3, A4, A5, A6, A7, A8>(
    line: number,
    name: string,
    childCtx: SpanContext<Ctx>,
    SpanBufferClass: SpanBufferConstructor,
    remappedViewClass: RemappedViewConstructor | undefined,
    opMetadata: OpMetadata,
    fn: (
      ctx: SpanContext<Ctx>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
      a5: A5,
      a6: A6,
      a7: A7,
      a8: A8,
    ) => Result<S, E> | Promise<Result<S, E>>,
    a1: A1,
    a2: A2,
    a3: A3,
    a4: A4,
    a5: A5,
    a6: A6,
    a7: A7,
    a8: A8,
  ): Promise<Result<S, E>>;
};

// =============================================================================
// SpanContext Class Factory
// =============================================================================

/**
 * Create a SpanContext class for the given schema and logBinding.
 *
 * The returned class:
 * - Closes over schema/logBinding (available to all methods)
 * - Constructor takes buffer, spanLogger, tag directly (no temp object allocation)
 * - Uses prototype methods for internal methods (_spanPre, _spanException, span0-span8)
 * - Supports context inheritance via Object.create(parent) for child spans
 * - Supports context overrides via own properties shadowing inherited ones
 *
 * @param schemaOnly - Tag attribute schema
 * @param logBinding - LogBinding with schema and stats
 * @returns Class constructor for SpanContext instances
 */
export function createSpanContextClass<Ctx extends OpContext>(
  schemaOnly: Ctx['logSchema'],
  logBinding: LogBinding,
): SpanContextClass<Ctx> {
  /**
   * SpanContext implementation class.
   *
   * Constructor takes core values directly so arrow functions can close over them,
   * avoiding both temp object allocation and property lookups in hot paths.
   */
  class SpanContextImpl {
    // Core properties set via constructor
    _buffer: SpanBuffer<Ctx['logSchema']>;
    _schema: Ctx['logSchema'];
    _spanLogger: SpanLoggerImpl<Ctx['logSchema']>;
    _logBinding: LogBinding = logBinding;

    // User-facing properties
    tag: TagWriter<Ctx['logSchema']>;
    log: SpanLogger<Ctx['logSchema']>;
    ff!: FeatureFlagEvaluator<Ctx> & InferFeatureFlagsWithContext<Ctx>;
    deps!: Record<string, unknown>;

    // Index signature for user context properties (env, requestId, etc.)
    [key: string]: unknown;

    // Arrow functions that close over constructor args directly
    setScope: (attributes: Partial<InferSchema<Ctx['logSchema']>> | null) => void;
    ok: <V>(value: V) => FluentOk<V, Ctx['logSchema']>;
    err: <E>(code: string, error: E) => FluentErr<E, Ctx['logSchema']>;
    span: SpanFn<Ctx>;

    constructor(
      buffer: SpanBuffer<Ctx['logSchema']>,
      schema: Ctx['logSchema'],
      spanLogger: SpanLoggerImpl<Ctx['logSchema']>,
      tag: TagWriter<Ctx['logSchema']>,
    ) {
      this._buffer = buffer;
      this._schema = schema;
      this._spanLogger = spanLogger;
      this.tag = tag;
      this.log = spanLogger as unknown as SpanLogger<Ctx['logSchema']>;

      // Regular functions close over constructor args directly - no property lookups
      // Using regular function (not arrow) allows destructuring while closing over args
      this.setScope = (attributes: Partial<InferSchema<Ctx['logSchema']>> | null): void => {
        spanLogger._setScope(attributes as Partial<InferSchema<Ctx['logSchema']>>);
      };

      this.ok = <V>(value: V): FluentOk<V, Ctx['logSchema']> =>
        new FluentOk<V, Ctx['logSchema']>(buffer, value, schema);

      this.err = <E>(code: string, error: E): FluentErr<E, Ctx['logSchema']> =>
        new FluentErr<E, Ctx['logSchema']>(buffer, code, error, schema);

      // span uses regular function to access `arguments` (no ...rest spread allocation)
      // Closes over `self` for calling prototype methods
      // Named function for better stack traces
      const self = this;
      this.span = function span(nameOrLine: string | number): Promise<Result<unknown, unknown>> {
        // Use arguments.length instead of ...rest to avoid array allocation
        const len = arguments.length;

        // Detect call pattern:
        // - span(name, fn) - len=2, typeof arg0 === 'string'
        // - span(line, name, fn) - len=3, typeof arg0 === 'number'
        // - span(name, overrides, fn) - len=3, typeof arg0 === 'string', typeof arg1 === 'object'
        // - span(line, name, overrides, fn) - len=4, typeof arg0 === 'number', typeof arg2 === 'object'
        // Plus additional args for span arguments

        const hasLine = typeof nameOrLine === 'number';
        const spanLine = hasLine ? (nameOrLine as number) : 0;
        const spanName = hasLine ? (arguments[1] as string) : (nameOrLine as string);

        // Index where fn/op or overrides starts
        const checkIdx = hasLine ? 2 : 1;
        const maybeOverrides = arguments[checkIdx];

        // Check if it's an overrides object (not Op, not function)
        const hasOverrides =
          maybeOverrides !== null &&
          typeof maybeOverrides === 'object' &&
          !(maybeOverrides instanceof Op) &&
          typeof maybeOverrides !== 'function';

        const fnIdx = checkIdx + (hasOverrides ? 1 : 0);
        const fnOrOp = arguments[fnIdx];
        // Why extract all properties: Ops provide their own buffer class and metadata for cross-module calls
        const isOp = fnOrOp instanceof Op;

        // Why Op's class: Contains correct schema for tag methods and shared stats for self-tuning
        const SpanBufferClass = isOp
          ? (fnOrOp as Op<Ctx, unknown[], unknown, unknown>).SpanBufferClass
          : (self._buffer.constructor as SpanBufferConstructor);

        // Why wrap buffer: RemappedBufferView translates prefixed column names during Arrow conversion
        const remappedViewClass = isOp ? (fnOrOp as Op<Ctx, unknown[], unknown, unknown>).remappedViewClass : undefined;

        // Why Op's metadata: Identifies which op is executing (for rows 1+)
        const opMetadata = isOp ? (fnOrOp as Op<Ctx, unknown[], unknown, unknown>).metadata : self._buffer._opMetadata;
        // Extract function from Op or use directly
        const fn = isOp
          ? (fnOrOp as Op<Ctx, unknown[], unknown, unknown>).fn
          : (fnOrOp as (ctx: SpanContext<Ctx>, ...args: unknown[]) => Promise<Result<unknown, unknown>>);

        // Create child context - use _newCtx0 or _newCtx1 based on overrides
        const childCtx = hasOverrides ? self._newCtx1(maybeOverrides as Record<string, unknown>) : self._newCtx0();

        // Dispatch to monomorphic methods with all Op properties extracted
        const argCount = len - fnIdx - 1;
        switch (argCount) {
          case 0:
            return self.span0(spanLine, spanName, childCtx, SpanBufferClass, remappedViewClass, opMetadata, fn);
          case 1:
            return self.span1(
              spanLine,
              spanName,
              childCtx,
              SpanBufferClass,
              remappedViewClass,
              opMetadata,
              fn,
              arguments[fnIdx + 1],
            );
          case 2:
            return self.span2(
              spanLine,
              spanName,
              childCtx,
              SpanBufferClass,
              remappedViewClass,
              opMetadata,
              fn,
              arguments[fnIdx + 1],
              arguments[fnIdx + 2],
            );
          case 3:
            return self.span3(
              spanLine,
              spanName,
              childCtx,
              SpanBufferClass,
              remappedViewClass,
              opMetadata,
              fn,
              arguments[fnIdx + 1],
              arguments[fnIdx + 2],
              arguments[fnIdx + 3],
            );
          case 4:
            return self.span4(
              spanLine,
              spanName,
              childCtx,
              SpanBufferClass,
              remappedViewClass,
              opMetadata,
              fn,
              arguments[fnIdx + 1],
              arguments[fnIdx + 2],
              arguments[fnIdx + 3],
              arguments[fnIdx + 4],
            );
          case 5:
            return self.span5(
              spanLine,
              spanName,
              childCtx,
              SpanBufferClass,
              remappedViewClass,
              opMetadata,
              fn,
              arguments[fnIdx + 1],
              arguments[fnIdx + 2],
              arguments[fnIdx + 3],
              arguments[fnIdx + 4],
              arguments[fnIdx + 5],
            );
          case 6:
            return self.span6(
              spanLine,
              spanName,
              childCtx,
              SpanBufferClass,
              remappedViewClass,
              opMetadata,
              fn,
              arguments[fnIdx + 1],
              arguments[fnIdx + 2],
              arguments[fnIdx + 3],
              arguments[fnIdx + 4],
              arguments[fnIdx + 5],
              arguments[fnIdx + 6],
            );
          case 7:
            return self.span7(
              spanLine,
              spanName,
              childCtx,
              SpanBufferClass,
              remappedViewClass,
              opMetadata,
              fn,
              arguments[fnIdx + 1],
              arguments[fnIdx + 2],
              arguments[fnIdx + 3],
              arguments[fnIdx + 4],
              arguments[fnIdx + 5],
              arguments[fnIdx + 6],
              arguments[fnIdx + 7],
            );
          case 8:
            return self.span8(
              spanLine,
              spanName,
              childCtx,
              SpanBufferClass,
              remappedViewClass,
              opMetadata,
              fn,
              arguments[fnIdx + 1],
              arguments[fnIdx + 2],
              arguments[fnIdx + 3],
              arguments[fnIdx + 4],
              arguments[fnIdx + 5],
              arguments[fnIdx + 6],
              arguments[fnIdx + 7],
              arguments[fnIdx + 8],
            );
          default:
            throw new Error(`span() supports up to 8 arguments, got ${argCount}`);
        }
      } as SpanFn<Ctx>;
    }

    // =========================================================================
    // Getters (on prototype, inherited by children)
    // =========================================================================

    get [SPAN_CONTEXT_MARKER](): true {
      return true;
    }

    get module(): import('./opContext/opTypes.js').OpMetadata {
      return this._buffer._opMetadata;
    }

    get callee_package(): string {
      return this._buffer._opMetadata.package_name;
    }

    get callee_file(): string {
      return this._buffer._opMetadata.package_file;
    }

    get callee_line(): number {
      // Access the line_values array directly to read line number at row 0
      // line_values is a Float64Array per the system schema (LazyNumberSchema)
      return this._buffer.line_values[0];
    }

    get callee_git_sha(): string {
      return this._buffer._opMetadata.git_sha;
    }

    get buffer(): SpanBuffer<Ctx['logSchema']> {
      return this._buffer;
    }

    get scope(): Readonly<Partial<InferSchema<Ctx['logSchema']>>> {
      return this._buffer._scopeValues as Readonly<Partial<InferSchema<Ctx['logSchema']>>>;
    }

    // =========================================================================
    // Internal methods (on prototype)
    // =========================================================================

    /**
     * Create a new child context with prototype chain inheritance.
     * Called by transformer for spans without overrides.
     *
     * Transformer emits: ctx.span0(line, name, ctx._newCtx0(), fn)
     */
    _newCtx0(): SpanContextInstance<Ctx> {
      return Object.create(this) as SpanContextInstance<Ctx>;
    }

    /**
     * Create a new child context with prototype chain inheritance + user overrides.
     * Called by transformer for spans with overrides.
     *
     * Transformer emits: ctx.span0(line, name, ctx._newCtx1({ requestId }), fn)
     */
    _newCtx1(overrides: Record<string, unknown>): SpanContextInstance<Ctx> {
      return Object.assign(Object.create(this), overrides) as SpanContextInstance<Ctx>;
    }

    /**
     * Complete child span context setup - creates buffer, registers with parent.
     * Called by span0-span8 after _newCtx0/_newCtx1 has created the prototype chain.
     *
     * Properties are set in HOT→COLD order per specs/01b1_buffer_performance_optimizations.md
     * to ensure frequently-accessed properties get V8 in-object slots.
     *
     * @param childCtx - Child context created by _newCtx0 or _newCtx1
     * @param line - Source line number (0 if not provided by transformer)
     * @param name - Child span name
     * @param SpanBufferClass - Buffer class to use (Op's class or parent's class for plain functions)
     * @param remappedViewClass - Optional view class for prefixed ops (wraps buffer for Arrow conversion)
     * @param opMetadata - Metadata for the executing op (Op's metadata or parent's for plain functions)
     */
    _spanPre(
      childCtx: SpanContextInstance<Ctx>,
      line: number,
      name: string,
      SpanBufferClass: SpanBufferConstructor,
      remappedViewClass: RemappedViewConstructor | undefined,
      opMetadata: OpMetadata,
    ): SpanContextInstance<Ctx> {
      // Why use Op's class: Child buffer gets correct schema for tag methods and shared stats for self-tuning
      const childBuffer = createChildSpanBuffer<Ctx['logSchema']>(
        this._buffer, // parentBuffer
        SpanBufferClass, // Op's class or parent's class
        name, // spanName
        this._buffer._opMetadata, // callsiteMetadata - WHO called span() (for row 0)
        opMetadata, // opMetadata - WHICH op is executing (for rows 1+)
      );

      // Why wrap buffer: RemappedBufferView translates prefixed column names during Arrow conversion
      // Parent sees remapped names, child sees unprefixed names (transparent to child)
      const bufferOrView = remappedViewClass ? new remappedViewClass(childBuffer) : childBuffer;

      // Why push wrapped buffer: Parent's _children see remapped names for Arrow conversion
      this._buffer._children.push(bufferOrView);

      // Write span-start for child span (row 0) and pre-initialize span-end (row 1)
      writeSpanStart(childBuffer, name);

      // Write line number to row 0 (line() takes pos and value)
      childBuffer.line(0, line);

      // Create child span logger and tag writer using Op's schema (from SpanBufferClass.schema)
      // Why Op's schema: Cross-module spans need the Op's tag methods (e.g., http lib needs .status())
      const childSchema = (SpanBufferClass as any).schema as Ctx['logSchema'];
      const childLogger = createSpanLogger(childSchema, childBuffer);
      const childTagAPI = createTagWriter(childSchema, childBuffer);

      // Cast to impl for property assignment
      const ctx = childCtx as unknown as SpanContextImpl;

      // ═══════════════════════════════════════════════════════════════════
      // PROPERTY ASSIGNMENT ORDER: HOT → COLD (V8 in-object slots optimization)
      // First ~10-12 properties get fast in-object slots per spec
      // ═══════════════════════════════════════════════════════════════════

      // SLOT 1-4: HOTTEST - accessed on every span operation
      ctx._buffer = childBuffer;
      ctx._spanLogger = childLogger;
      ctx.tag = childTagAPI as SpanContext<Ctx>['tag'];
      ctx.log = childLogger as unknown as SpanLogger<Ctx['logSchema']>;

      // SLOT 5-7: HOT - reserved keys copied from parent for O(1) access
      ctx.deps = this.deps;
      // Create child-bound FF evaluator so flag access logs to child buffer
      // If no evaluator (ff is EMPTY_SCOPE), just copy the reference
      ctx.ff = this.ff.forContext ? this.ff.forContext(ctx) : this.ff;
      // Note: user-defined extras (env, requestId, etc.) inherited via prototype chain

      // SLOT 8-11: WARM - function properties (created per-context for destructuring)
      // Regular functions close over local variables directly - no property lookups
      ctx.span = function span(nameOrLine: string | number): Promise<Result<unknown, unknown>> {
        const len = arguments.length;
        const hasLine = typeof nameOrLine === 'number';
        const spanLine = hasLine ? (nameOrLine as number) : 0;
        const spanName = hasLine ? (arguments[1] as string) : (nameOrLine as string);
        const checkIdx = hasLine ? 2 : 1;
        const maybeOverrides = arguments[checkIdx];
        const hasOverrides =
          maybeOverrides !== null &&
          typeof maybeOverrides === 'object' &&
          !(maybeOverrides instanceof Op) &&
          typeof maybeOverrides !== 'function';
        const fnIdx = checkIdx + (hasOverrides ? 1 : 0);
        const fnOrOp = arguments[fnIdx];

        // Why extract all properties: Ops provide their own buffer class and metadata for cross-module calls
        const isOp = fnOrOp instanceof Op;

        // Why Op's class: Contains correct schema for tag methods and shared stats for self-tuning
        const bufferClass = isOp
          ? (fnOrOp as Op<Ctx, unknown[], unknown, unknown>).SpanBufferClass
          : (childBuffer.constructor as SpanBufferConstructor);

        // Why wrap buffer: RemappedBufferView translates prefixed column names during Arrow conversion
        const viewClass = isOp ? (fnOrOp as Op<Ctx, unknown[], unknown, unknown>).remappedViewClass : undefined;

        // Why Op's metadata: Identifies which op is executing (for rows 1+)
        const metadata = isOp ? (fnOrOp as Op<Ctx, unknown[], unknown, unknown>).metadata : childBuffer._opMetadata;

        const fn = isOp
          ? (fnOrOp as Op<Ctx, unknown[], unknown, unknown>).fn
          : (fnOrOp as (ctx: SpanContext<Ctx>, ...args: unknown[]) => Promise<Result<unknown, unknown>>);

        // Create child context - use _newCtx0 or _newCtx1 based on overrides
        const newChildCtx = hasOverrides ? ctx._newCtx1(maybeOverrides as Record<string, unknown>) : ctx._newCtx0();
        const argCount = len - fnIdx - 1;
        switch (argCount) {
          case 0:
            return ctx.span0(spanLine, spanName, newChildCtx, bufferClass, viewClass, metadata, fn);
          case 1:
            return ctx.span1(
              spanLine,
              spanName,
              newChildCtx,
              bufferClass,
              viewClass,
              metadata,
              fn,
              arguments[fnIdx + 1],
            );
          case 2:
            return ctx.span2(
              spanLine,
              spanName,
              newChildCtx,
              bufferClass,
              viewClass,
              metadata,
              fn,
              arguments[fnIdx + 1],
              arguments[fnIdx + 2],
            );
          case 3:
            return ctx.span3(
              spanLine,
              spanName,
              newChildCtx,
              bufferClass,
              viewClass,
              metadata,
              fn,
              arguments[fnIdx + 1],
              arguments[fnIdx + 2],
              arguments[fnIdx + 3],
            );
          case 4:
            return ctx.span4(
              spanLine,
              spanName,
              newChildCtx,
              bufferClass,
              viewClass,
              metadata,
              fn,
              arguments[fnIdx + 1],
              arguments[fnIdx + 2],
              arguments[fnIdx + 3],
              arguments[fnIdx + 4],
            );
          case 5:
            return ctx.span5(
              spanLine,
              spanName,
              newChildCtx,
              bufferClass,
              viewClass,
              metadata,
              fn,
              arguments[fnIdx + 1],
              arguments[fnIdx + 2],
              arguments[fnIdx + 3],
              arguments[fnIdx + 4],
              arguments[fnIdx + 5],
            );
          case 6:
            return ctx.span6(
              spanLine,
              spanName,
              newChildCtx,
              bufferClass,
              viewClass,
              metadata,
              fn,
              arguments[fnIdx + 1],
              arguments[fnIdx + 2],
              arguments[fnIdx + 3],
              arguments[fnIdx + 4],
              arguments[fnIdx + 5],
              arguments[fnIdx + 6],
            );
          case 7:
            return ctx.span7(
              spanLine,
              spanName,
              newChildCtx,
              bufferClass,
              viewClass,
              metadata,
              fn,
              arguments[fnIdx + 1],
              arguments[fnIdx + 2],
              arguments[fnIdx + 3],
              arguments[fnIdx + 4],
              arguments[fnIdx + 5],
              arguments[fnIdx + 6],
              arguments[fnIdx + 7],
            );
          case 8:
            return ctx.span8(
              spanLine,
              spanName,
              newChildCtx,
              bufferClass,
              viewClass,
              metadata,
              fn,
              arguments[fnIdx + 1],
              arguments[fnIdx + 2],
              arguments[fnIdx + 3],
              arguments[fnIdx + 4],
              arguments[fnIdx + 5],
              arguments[fnIdx + 6],
              arguments[fnIdx + 7],
              arguments[fnIdx + 8],
            );
          default:
            throw new Error(`span() supports up to 8 arguments, got ${argCount}`);
        }
      } as SpanFn<Ctx>;

      ctx.ok = function ok<V>(value: V): FluentOk<V, Ctx['logSchema']> {
        return new FluentOk<V, Ctx['logSchema']>(childBuffer, value, childSchema);
      };
      ctx.err = function err<E>(code: string, error: E): FluentErr<E, Ctx['logSchema']> {
        return new FluentErr<E, Ctx['logSchema']>(childBuffer, code, error, childSchema);
      };
      ctx.setScope = function setScope(attributes: Partial<InferSchema<Ctx['logSchema']>> | null): void {
        childLogger._setScope(attributes as Partial<InferSchema<Ctx['logSchema']>>);
      };

      // SLOT 12+: COLD - internal/rarely accessed
      ctx._schema = childSchema;
      ctx._logBinding = logBinding;

      return ctx as SpanContextInstance<Ctx>;
    }

    /**
     * Handle span exception - writes exception details to row 1.
     * Called in catch blocks of span methods.
     */
    _spanException(childBuffer: SpanBuffer<Ctx['logSchema']>, error: unknown): void {
      childBuffer.timestamp[1] = getTimestampNanos(
        childBuffer._traceRoot.anchorEpochNanos,
        childBuffer._traceRoot.anchorPerfNow,
      );
      const errorMessage = error instanceof Error ? error.message : String(error);
      const errorStack = error instanceof Error ? error.stack : undefined;
      childBuffer.message(1, errorMessage);
      if (errorStack) {
        childBuffer.exception_stack(1, errorStack);
      }
    }

    // =========================================================================
    // Monomorphic span methods (for transformer output)
    // Transformer emits: ctx.span0(line, name, ctx, fn) or
    //                    ctx.span0(line, name, Object.assign(Object.create(ctx), overrides), fn)
    // childCtx inherits from parent via prototype chain, may have user overrides as own properties
    // =========================================================================

    async span0<S, E>(
      line: number,
      name: string,
      childCtx: SpanContext<Ctx>,
      SpanBufferClass: SpanBufferConstructor,
      remappedViewClass: RemappedViewConstructor | undefined,
      opMetadata: OpMetadata,
      fn: (ctx: SpanContext<Ctx>) => Result<S, E> | Promise<Result<S, E>>,
    ): Promise<Result<S, E>> {
      const ctx = this._spanPre(
        childCtx as unknown as SpanContextInstance<Ctx>,
        line,
        name,
        SpanBufferClass,
        remappedViewClass,
        opMetadata,
      );
      ctx._buffer._traceRoot.tracer.onSpanStart(ctx._buffer);
      try {
        return await fn(ctx as unknown as SpanContext<Ctx>);
      } catch (error) {
        this._spanException(ctx._buffer, error);
        throw error;
      } finally {
        ctx._buffer._traceRoot.tracer.onSpanEnd(ctx._buffer);
      }
    }

    async span1<S, E, A1>(
      line: number,
      name: string,
      childCtx: SpanContext<Ctx>,
      SpanBufferClass: SpanBufferConstructor,
      remappedViewClass: RemappedViewConstructor | undefined,
      opMetadata: OpMetadata,
      fn: (ctx: SpanContext<Ctx>, a1: A1) => Result<S, E> | Promise<Result<S, E>>,
      a1: A1,
    ): Promise<Result<S, E>> {
      const ctx = this._spanPre(
        childCtx as unknown as SpanContextInstance<Ctx>,
        line,
        name,
        SpanBufferClass,
        remappedViewClass,
        opMetadata,
      );
      ctx._buffer._traceRoot.tracer.onSpanStart(ctx._buffer);
      try {
        return await fn(ctx as unknown as SpanContext<Ctx>, a1);
      } catch (error) {
        this._spanException(ctx._buffer, error);
        throw error;
      } finally {
        ctx._buffer._traceRoot.tracer.onSpanEnd(ctx._buffer);
      }
    }

    async span2<S, E, A1, A2>(
      line: number,
      name: string,
      childCtx: SpanContext<Ctx>,
      SpanBufferClass: SpanBufferConstructor,
      remappedViewClass: RemappedViewConstructor | undefined,
      opMetadata: OpMetadata,
      fn: (ctx: SpanContext<Ctx>, a1: A1, a2: A2) => Result<S, E> | Promise<Result<S, E>>,
      a1: A1,
      a2: A2,
    ): Promise<Result<S, E>> {
      const ctx = this._spanPre(
        childCtx as unknown as SpanContextInstance<Ctx>,
        line,
        name,
        SpanBufferClass,
        remappedViewClass,
        opMetadata,
      );
      try {
        return await fn(ctx as unknown as SpanContext<Ctx>, a1, a2);
      } catch (error) {
        this._spanException(ctx._buffer, error);
        throw error;
      } finally {
        ctx._buffer._traceRoot.tracer.onSpanEnd(ctx._buffer);
      }
    }

    async span3<S, E, A1, A2, A3>(
      line: number,
      name: string,
      childCtx: SpanContext<Ctx>,
      SpanBufferClass: SpanBufferConstructor,
      remappedViewClass: RemappedViewConstructor | undefined,
      opMetadata: OpMetadata,
      fn: (ctx: SpanContext<Ctx>, a1: A1, a2: A2, a3: A3) => Result<S, E> | Promise<Result<S, E>>,
      a1: A1,
      a2: A2,
      a3: A3,
    ): Promise<Result<S, E>> {
      const ctx = this._spanPre(
        childCtx as unknown as SpanContextInstance<Ctx>,
        line,
        name,
        SpanBufferClass,
        remappedViewClass,
        opMetadata,
      );
      try {
        return await fn(ctx as unknown as SpanContext<Ctx>, a1, a2, a3);
      } catch (error) {
        this._spanException(ctx._buffer, error);
        throw error;
      } finally {
        ctx._buffer._traceRoot.tracer.onSpanEnd(ctx._buffer);
      }
    }

    async span4<S, E, A1, A2, A3, A4>(
      line: number,
      name: string,
      childCtx: SpanContext<Ctx>,
      SpanBufferClass: SpanBufferConstructor,
      remappedViewClass: RemappedViewConstructor | undefined,
      opMetadata: OpMetadata,
      fn: (ctx: SpanContext<Ctx>, a1: A1, a2: A2, a3: A3, a4: A4) => Result<S, E> | Promise<Result<S, E>>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
    ): Promise<Result<S, E>> {
      const ctx = this._spanPre(
        childCtx as unknown as SpanContextInstance<Ctx>,
        line,
        name,
        SpanBufferClass,
        remappedViewClass,
        opMetadata,
      );
      try {
        return await fn(ctx as unknown as SpanContext<Ctx>, a1, a2, a3, a4);
      } catch (error) {
        this._spanException(ctx._buffer, error);
        throw error;
      } finally {
        ctx._buffer._traceRoot.tracer.onSpanEnd(ctx._buffer);
      }
    }

    async span5<S, E, A1, A2, A3, A4, A5>(
      line: number,
      name: string,
      childCtx: SpanContext<Ctx>,
      SpanBufferClass: SpanBufferConstructor,
      remappedViewClass: RemappedViewConstructor | undefined,
      opMetadata: OpMetadata,
      fn: (ctx: SpanContext<Ctx>, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5) => Result<S, E> | Promise<Result<S, E>>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
      a5: A5,
    ): Promise<Result<S, E>> {
      const ctx = this._spanPre(
        childCtx as unknown as SpanContextInstance<Ctx>,
        line,
        name,
        SpanBufferClass,
        remappedViewClass,
        opMetadata,
      );
      try {
        return await fn(ctx as unknown as SpanContext<Ctx>, a1, a2, a3, a4, a5);
      } catch (error) {
        this._spanException(ctx._buffer, error);
        throw error;
      } finally {
        ctx._buffer._traceRoot.tracer.onSpanEnd(ctx._buffer);
      }
    }

    async span6<S, E, A1, A2, A3, A4, A5, A6>(
      line: number,
      name: string,
      childCtx: SpanContext<Ctx>,
      SpanBufferClass: SpanBufferConstructor,
      remappedViewClass: RemappedViewConstructor | undefined,
      opMetadata: OpMetadata,
      fn: (
        ctx: SpanContext<Ctx>,
        a1: A1,
        a2: A2,
        a3: A3,
        a4: A4,
        a5: A5,
        a6: A6,
      ) => Result<S, E> | Promise<Result<S, E>>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
      a5: A5,
      a6: A6,
    ): Promise<Result<S, E>> {
      const ctx = this._spanPre(
        childCtx as unknown as SpanContextInstance<Ctx>,
        line,
        name,
        SpanBufferClass,
        remappedViewClass,
        opMetadata,
      );
      try {
        return await fn(ctx as unknown as SpanContext<Ctx>, a1, a2, a3, a4, a5, a6);
      } catch (error) {
        this._spanException(ctx._buffer, error);
        throw error;
      } finally {
        ctx._buffer._traceRoot.tracer.onSpanEnd(ctx._buffer);
      }
    }

    async span7<S, E, A1, A2, A3, A4, A5, A6, A7>(
      line: number,
      name: string,
      childCtx: SpanContext<Ctx>,
      SpanBufferClass: SpanBufferConstructor,
      remappedViewClass: RemappedViewConstructor | undefined,
      opMetadata: OpMetadata,
      fn: (
        ctx: SpanContext<Ctx>,
        a1: A1,
        a2: A2,
        a3: A3,
        a4: A4,
        a5: A5,
        a6: A6,
        a7: A7,
      ) => Result<S, E> | Promise<Result<S, E>>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
      a5: A5,
      a6: A6,
      a7: A7,
    ): Promise<Result<S, E>> {
      const ctx = this._spanPre(
        childCtx as unknown as SpanContextInstance<Ctx>,
        line,
        name,
        SpanBufferClass,
        remappedViewClass,
        opMetadata,
      );
      try {
        return await fn(ctx as unknown as SpanContext<Ctx>, a1, a2, a3, a4, a5, a6, a7);
      } catch (error) {
        this._spanException(ctx._buffer, error);
        throw error;
      } finally {
        ctx._buffer._traceRoot.tracer.onSpanEnd(ctx._buffer);
      }
    }

    async span8<S, E, A1, A2, A3, A4, A5, A6, A7, A8>(
      line: number,
      name: string,
      childCtx: SpanContext<Ctx>,
      SpanBufferClass: SpanBufferConstructor,
      remappedViewClass: RemappedViewConstructor | undefined,
      opMetadata: OpMetadata,
      fn: (
        ctx: SpanContext<Ctx>,
        a1: A1,
        a2: A2,
        a3: A3,
        a4: A4,
        a5: A5,
        a6: A6,
        a7: A7,
        a8: A8,
      ) => Result<S, E> | Promise<Result<S, E>>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
      a5: A5,
      a6: A6,
      a7: A7,
      a8: A8,
    ): Promise<Result<S, E>> {
      const ctx = this._spanPre(
        childCtx as unknown as SpanContextInstance<Ctx>,
        line,
        name,
        SpanBufferClass,
        remappedViewClass,
        opMetadata,
      );
      try {
        return await fn(ctx as unknown as SpanContext<Ctx>, a1, a2, a3, a4, a5, a6, a7, a8);
      } catch (error) {
        this._spanException(ctx._buffer, error);
        throw error;
      } finally {
        ctx._buffer._traceRoot.tracer.onSpanEnd(ctx._buffer);
      }
    }
  }

  return SpanContextImpl as unknown as SpanContextClass<Ctx>;
}

// =============================================================================
// Type Guard
// =============================================================================

/**
 * Type guard to check if a value is a SpanContext
 */
export function isSpanContext<Ctx extends OpContext>(value: unknown): value is SpanContext<Ctx> {
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
