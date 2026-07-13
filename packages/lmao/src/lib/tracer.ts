/**
 * Tracer - Abstract base class for trace collection
 *
 * Per specs/lmao/01c_context_flow_and_op_wrappers.md:
 * - Tracer owns trace() function for creating root spans
 * - Collects SpanBuffers in pendingBuffers array
 * - Subclasses implement flush() to define persistence strategy
 *
 * ## Design Philosophy
 *
 * Tracer separates two concerns:
 * 1. **BufferStrategy**: HOW buffers are allocated (JS TypedArrays vs WASM memory)
 * 2. **Sink behavior** (lifecycle hooks): WHAT happens with completed buffers
 *
 * Tracer is ABSTRACT - subclasses implement lifecycle hooks:
 * - `onTraceEnd()` - called when root trace completes
 * - `onSpanEnd()` - called when child span completes
 * - `flush()` - optional, for batching strategies
 *
 * BufferStrategy is INJECTED via constructor:
 * - `JsBufferStrategy` - GC-managed TypedArrays, zero-copy Arrow
 * - `WasmBufferStrategy` - WASM memory with freelist, Arrow built in WASM
 *
 * The sink controls WHEN to convert/release:
 * - StdioTracer: prints then releases immediately
 * - TestTracer: holds buffers until clear()
 * - SqsTracer: accumulates, converts on flush(), then releases
 *
 * @example
 * ```typescript
 * // Concrete implementation - Cloudflare Queue
 * class QueueTracer extends Tracer {
 *   constructor(binding, strategy, options, private queue: Queue) {
 *     super(binding, strategy, options);
 *   }
 *
 *   async flush() {
 *     const buffers = [...this.pending];
 *     this.pending.length = 0;
 *     for (const buffer of buffers) {
 *       const table = await this.bufferStrategy.toArrowTable(buffer);
 *       await this.queue.send(table);
 *       this.bufferStrategy.releaseBuffer(buffer);
 *     }
 *   }
 * }
 *
 * // Usage
 * const ctx = defineOpContext({ logSchema, ctx: { env: null as Env } });
 * const strategy = new JsBufferStrategy();
 * const { trace, flush } = new QueueTracer(ctx, strategy, options, env.TRACES);
 * await trace('fetch', { env: myEnv }, async (ctx) => {
 *   ctx.tag.userId('user-123');
 *   return ctx.ok('done');
 * });
 * executionContext.waitUntil(flush());
 * ```
 *
 * @module tracer
 */

import type { BufferStrategy } from './bufferStrategy.js';
import type { LogBinding } from './logBinding.js';
import { Op } from './op.js';
import { createOpMetadata } from './opContext/defineOp.js';
import type { OpContext, OpContextBinding, OpContextOf, SpanContext } from './opContext/types.js';
import {
  getPhysicalLayoutPlan,
  sealCallsitePlan,
  type CallsitePlan,
  type PhysicalLayoutPlan,
} from './physicalLayoutPlan.js';
import { Err, Ok, type Result } from './result.js';
import { createFeatureFlagEvaluator, type FlagEvaluator, InMemoryFlagEvaluator } from './schema/evaluator.js';
import { ENTRY_TYPE_SPAN_EXCEPTION, ENTRY_TYPE_SPAN_OK } from './schema/systemSchema.js';
import { EMPTY_SCOPE, getSpanBufferClass } from './spanBuffer.js';
import {
  createSpanContextClass,
  isPhysicalLayoutPlanForContext,
  type SpanContextClass,
  type SpanContextInstance,
  writeSpanEnd,
  writeSpanStart,
} from './spanContext.js';
import { generateTraceId, isValidTraceId, type TraceId } from './traceId.js';
import type { TraceRootFactory, TracerLifecycleHooks } from './traceRoot.js';
import type { SpanBuffer } from './types.js';

// =============================================================================
// Types
// =============================================================================

/**
 * Base tracer options (can be extended by subclasses)
 *
 * Flag evaluator typing is intentionally loose - the evaluator just needs to
 * implement the FlagEvaluator interface. Type safety for flag access comes from
 * the OpContext's flags schema, not from the evaluator's type parameter.
 */
export interface TracerOptions<
  T extends import('./schema/LogSchema.js').LogSchema = import('./schema/LogSchema.js').LogSchema,
> {
  /**
   * Buffer strategy for memory management and Arrow conversion.
   *
   * Controls HOW buffers are allocated:
   * - JsBufferStrategy: GC-managed TypedArrays, zero-copy Arrow
   * - WasmBufferStrategy: WASM memory with freelist, Arrow built in WASM
   */
  bufferStrategy: BufferStrategy<T>;

  /**
   * Factory for creating platform-specific TraceRoot instances.
   * Import from '@smoothbricks/lmao/node' or '@smoothbricks/lmao/es'.
   */
  createTraceRoot: TraceRootFactory<T>;

  /** Feature flag evaluator - provides flag values at runtime */
  flagEvaluator?: FlagEvaluator<OpContext>;
}

function isTraceOverridesArg(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !(value instanceof Op);
}

function isTraceFn<Ctx extends OpContext>(value: unknown): value is (ctx: SpanContext<Ctx>) => unknown {
  return typeof value === 'function';
}

function isPromiseResult<R>(value: R | Promise<R>): value is Promise<R> {
  return (
    value !== null &&
    (typeof value === 'object' || typeof value === 'function') &&
    typeof Reflect.get(value, 'then') === 'function'
  );
}

/**
 * Trace overrides - userCtx values plus optional trace_id.
 *
 * Like span() overrides, but with optional trace_id for distributed tracing.
 * The trace_id must be a branded TraceId (use createTraceId() to validate).
 *
 * When UserCtx is empty (Record<string, never>), only trace_id is available.
 * Otherwise, allows partial userCtx values plus trace_id.
 *
 * We use a conditional type to avoid issues with Partial<Record<string, never>>
 * which creates an index signature where all values must be undefined.
 *
 * @typeParam UserCtx - User context type from OpContext
 */
export type TraceOverrides<UserCtx> = [UserCtx] extends [Record<string, never>]
  ? { trace_id?: TraceId }
  : Partial<UserCtx> & { trace_id?: TraceId };

// =============================================================================
// TraceFn Type - Overloaded trace function signatures
// =============================================================================

/**
 * TraceFn - overloaded trace creation function.
 *
 * Per specs/lmao/01c_context_flow_and_op_wrappers.md:
 * Creates root spans with type-safe Op invocation or inline closures.
 * Mirrors SpanFn but for root traces.
 *
 * Pattern:
 * - With/without line numbers (transformer injection)
 * - Always requires name (unlike span which can use op.metadata.name)
 * - With/without overrides (userCtx + optional trace_id)
 * - With/without Op arguments (0-8 args supported)
 *
 * @template Ctx - Full OpContext type from the tracer's binding
 */
export type TraceFn<Ctx extends OpContext> = {
  // ===========================================================================
  // Op invocations WITH line number (transformer-injected)
  // ===========================================================================

  /**
   * Trace Op with line + name + args.
   */
  <S, E, Args extends unknown[]>(
    line: number,
    name: string,
    op: Op<Ctx, Args, S, E>,
    ...args: Args
  ): Promise<Result<S, E>>;

  /**
   * Trace Op with line + name + overrides + args.
   */
  <S, E, Args extends unknown[]>(
    line: number,
    name: string,
    overrides: TraceOverrides<Ctx['userCtx']>,
    op: Op<Ctx, Args, S, E>,
    ...args: Args
  ): Promise<Result<S, E>>;

  /**
   * Trace inline function with line + name.
   */
  <R>(line: number, name: string, fn: (ctx: SpanContext<Ctx>) => R): R;

  /**
   * Trace inline function with line + name + overrides.
   */
  <R>(line: number, name: string, overrides: TraceOverrides<Ctx['userCtx']>, fn: (ctx: SpanContext<Ctx>) => R): R;

  // ===========================================================================
  // Op invocations WITHOUT line number (fallback/manual)
  // ===========================================================================

  /**
   * Trace Op with name + args.
   */
  <S, E, Args extends unknown[]>(name: string, op: Op<Ctx, Args, S, E>, ...args: Args): Promise<Result<S, E>>;

  /**
   * Trace Op with name + overrides + args.
   */
  <S, E, Args extends unknown[]>(
    name: string,
    overrides: TraceOverrides<Ctx['userCtx']>,
    op: Op<Ctx, Args, S, E>,
    ...args: Args
  ): Promise<Result<S, E>>;

  /**
   * Trace inline function with name.
   */
  <R>(name: string, fn: (ctx: SpanContext<Ctx>) => R): R;

  /**
   * Trace inline function with name + overrides.
   */
  <R>(name: string, overrides: TraceOverrides<Ctx['userCtx']>, fn: (ctx: SpanContext<Ctx>) => R): R;
};

// =============================================================================
// Tracer Class
// =============================================================================

//#region smoo/lmao!n/tracer-architecture #contract
/**
 * Tracer - Abstract base class for trace collection
 *
 * Provides trace execution with lifecycle hooks for buffer management.
 * Subclasses implement onTraceStart, onTraceEnd, onSpanStart, onSpanEnd to define
 * how spans are collected and processed.
 *
 * BufferStrategy is injected to control memory management and Arrow conversion.
 *
 * @typeParam B - OpContextBinding type (inferred from constructor argument)
 */
export abstract class Tracer<B extends OpContextBinding = OpContextBinding>
  implements TracerLifecycleHooks<B['logBinding']['logSchema']> {
  private readonly logBinding: LogBinding<B['logBinding']['logSchema']>;
  private readonly SpanContextClass: SpanContextClass<OpContextOf<B>>;
  private readonly physicalLayoutPlan: PhysicalLayoutPlan<B['logBinding']['logSchema'], OpContextOf<B>>;
  private readonly flagEvaluator: FlagEvaluator<OpContextOf<B>>;
  private readonly ctxDefaults: Record<string, unknown>;
  private readonly requiredCtxKeys: readonly string[];
  private readonly deps: Record<string, unknown>;
  private readonly createTraceRoot: TraceRootFactory<B['logBinding']['logSchema']>;

  /**
   * Buffer strategy for memory management and Arrow conversion.
   * Public because SpanContext needs access for child span creation.
   * Also accessible to subclasses for toArrowTable/releaseBuffer calls.
   */
  readonly bufferStrategy: BufferStrategy<B['logBinding']['logSchema']>;

  constructor(binding: B, options: TracerOptions<B['logBinding']['logSchema']>) {
    this.logBinding = binding.logBinding;
    this.bufferStrategy = options.bufferStrategy;
    this.ctxDefaults = binding.ctxDefaults ?? EMPTY_SCOPE;
    const userContextKeys = Object.keys(this.ctxDefaults).sort();
    const requiredCtxKeys: string[] = [];
    for (const key of userContextKeys) {
      if (this.ctxDefaults[key] === null) requiredCtxKeys.push(key);
    }
    this.requiredCtxKeys = requiredCtxKeys;
    this.deps = binding.deps ?? EMPTY_SCOPE;
    this.flagEvaluator = this._createFlagEvaluator(binding.flags, options.flagEvaluator);
    this.createTraceRoot = options.createTraceRoot;

    // Create SpanContext class for all contexts created by this tracer
    // The class closes over schema/logBinding and provides typed methods
    this.SpanContextClass = createSpanContextClass<OpContextOf<B>>(
      binding.logBinding.logSchema,
      binding.logBinding,
      undefined,
      userContextKeys,
    );
    this.physicalLayoutPlan = getPhysicalLayoutPlan(
      getSpanBufferClass(binding.logBinding.logSchema),
      0,
      this.SpanContextClass,
      undefined,
      'strategy-selected',
      userContextKeys.join('\u0000'),
    );

    // Bind methods for destructuring (per AGENTS.md - "Always destructure")
    this.trace = this.trace.bind(this);
    this.trace_op = this.trace_op.bind(this);
    this.trace_fn = this.trace_fn.bind(this);
    this.flush = this.flush.bind(this);
  }

  getFlagEvaluatorForContext(): FlagEvaluator<OpContextOf<B>> {
    return this.flagEvaluator;
  }

  private _createFlagEvaluator(
    flags: B['flags'],
    evaluator: FlagEvaluator<OpContext> | undefined,
  ): FlagEvaluator<OpContextOf<B>> {
    const delegate = evaluator ?? new InMemoryFlagEvaluator<OpContextOf<B>>(flags);
    const boundEvaluator: FlagEvaluator<OpContextOf<B>> = {
      getSync: (ctx, flag) => delegate.getSync(ctx, flag),
      getAsync: (ctx, flag) => delegate.getAsync(ctx, flag),
      forContext: (ctx) => createFeatureFlagEvaluator<OpContextOf<B>>(flags, ctx, boundEvaluator),
    };

    return boundEvaluator;
  }

  // ===========================================================================
  // Lifecycle Hooks (abstract - subclasses MUST implement)
  // ===========================================================================

  /**
   * Called when a root trace starts (before fn execution).
   * Only called for root spans created via trace(), not child spans.
   * Subclasses implement to handle trace lifecycle events (e.g., collect for batching).
   */
  abstract onTraceStart(rootBuffer: SpanBuffer<B['logBinding']['logSchema']>): void;

  /**
   * Called when a root trace ends (after fn completes, in finally).
   * Always called, even if fn threw an exception.
   * Only called for root spans created via trace(), not child spans.
   * Subclasses implement to handle trace completion (e.g., queue for sending).
   */
  abstract onTraceEnd(rootBuffer: SpanBuffer<B['logBinding']['logSchema']>): void;

  /**
   * Called when a child span starts (before fn execution).
   * Only called for child spans created via ctx.span(), not root traces.
   * Subclasses implement if child span lifecycle needs special handling.
   */
  abstract onSpanStart(childBuffer: SpanBuffer<B['logBinding']['logSchema']>): void;

  /**
   * Called when a child span ends (after fn completes, in finally).
   * Always called, even if fn threw an exception.
   * Only called for child spans created via ctx.span(), not root traces.
   * Subclasses implement if child span lifecycle needs special handling.
   */
  abstract onSpanEnd(childBuffer: SpanBuffer<B['logBinding']['logSchema']>): void;

  /**
   * Called before stats are reset during capacity tuning.
   * Allows tracer to capture stats for observability before they're lost.
   *
   * The buffer provides all necessary context:
   * - buffer._stats → SpanBufferStats about to be reset
   * - buffer._opMetadata → which Op/module these stats belong to
   * - buffer.constructor → SpanBufferClass (schema info)
   *
   * @param buffer - The buffer that triggered overflow
   */
  abstract onStatsWillResetFor(buffer: SpanBuffer<B['logBinding']['logSchema']>): void;

  /**
   * Flush any pending data. Default is no-op.
   * Subclasses override if they batch data (e.g., collecting in onTraceEnd then flushing here).
   */
  async flush(): Promise<void> {
    // Default no-op - subclasses override if needed
  }

  // ===========================================================================
  // trace() - Polymorphic dispatcher (like span())
  // ===========================================================================

  //#region smoo/lmao!n/tracer-architecture.trace-overloads
  /**
   * Polymorphic trace dispatcher.
   *
   * Parses overloaded arguments and dispatches to monomorphic methods.
   * Uses argument type detection for clean pattern matching.
   *
   * Patterns supported:
   * - (name, fn)
   * - (name, overrides, fn)
   * - (name, op, ...args)
   * - (name, overrides, op, ...args)
   * - Plus line number prefix variants for transformer
   *
   * @see TraceFn for type-level documentation of all overloads
   */
  trace<S, E, Args extends unknown[]>(
    line: number,
    name: string,
    op: Op<OpContextOf<B>, Args, S, E>,
    ...args: Args
  ): Promise<Result<S, E>>;
  trace<S, E, Args extends unknown[]>(
    line: number,
    name: string,
    overrides: TraceOverrides<OpContextOf<B>['userCtx']>,
    op: Op<OpContextOf<B>, Args, S, E>,
    ...args: Args
  ): Promise<Result<S, E>>;
  trace<R>(line: number, name: string, fn: (ctx: SpanContext<OpContextOf<B>>) => R): R;
  trace<R>(
    line: number,
    name: string,
    overrides: TraceOverrides<OpContextOf<B>['userCtx']>,
    fn: (ctx: SpanContext<OpContextOf<B>>) => R,
  ): R;
  trace<S, E, Args extends unknown[]>(
    name: string,
    op: Op<OpContextOf<B>, Args, S, E>,
    ...args: Args
  ): Promise<Result<S, E>>;
  trace<S, E, Args extends unknown[]>(
    name: string,
    overrides: TraceOverrides<OpContextOf<B>['userCtx']>,
    op: Op<OpContextOf<B>, Args, S, E>,
    ...args: Args
  ): Promise<Result<S, E>>;
  trace<R>(name: string, fn: (ctx: SpanContext<OpContextOf<B>>) => R): R;
  trace<R>(
    name: string,
    overrides: TraceOverrides<OpContextOf<B>['userCtx']>,
    fn: (ctx: SpanContext<OpContextOf<B>>) => R,
  ): R;
  trace(...args: [number | string, ...unknown[]]): unknown {
    const len = args.length;
    if (len === 0) throw new Error('trace() requires at least 1 argument');

    // Detect if first arg is a line number (transformer-injected)
    const firstArg = args[0];
    const hasLine = typeof firstArg === 'number';
    const line = hasLine ? firstArg : 0;

    // Name is always required and comes after optional line
    const nameIdx = hasLine ? 1 : 0;
    const name = args[nameIdx];
    if (typeof name !== 'string') {
      throw new Error('trace() requires a trace name string');
    }

    // Check if next arg is overrides (object but not Op or function)
    const nextIdx = nameIdx + 1;
    const nextArg = args[nextIdx];
    const hasOverrides = isTraceOverridesArg(nextArg) && !isTraceFn<OpContextOf<B>>(nextArg);

    const overrides = hasOverrides ? nextArg : EMPTY_SCOPE;
    const fnIdx = nextIdx + (hasOverrides ? 1 : 0);
    const fnOrOp = args[fnIdx];
    const argCount = len - fnIdx - 1;

    // Dispatch to monomorphic methods
    if (fnOrOp instanceof Op) {
      switch (argCount) {
        case 0:
          return this._trace_unknown_op(line, name, overrides, fnOrOp.callsitePlan, fnOrOp.fn);
        case 1:
          return this._trace_unknown_op(line, name, overrides, fnOrOp.callsitePlan, fnOrOp.fn, args[fnIdx + 1]);
        case 2:
          return this._trace_unknown_op(
            line,
            name,
            overrides,
            fnOrOp.callsitePlan,
            fnOrOp.fn,
            args[fnIdx + 1],
            args[fnIdx + 2],
          );
        case 3:
          return this._trace_unknown_op(
            line,
            name,
            overrides,
            fnOrOp.callsitePlan,
            fnOrOp.fn,
            args[fnIdx + 1],
            args[fnIdx + 2],
            args[fnIdx + 3],
          );
        case 4:
          return this._trace_unknown_op(
            line,
            name,
            overrides,
            fnOrOp.callsitePlan,
            fnOrOp.fn,
            args[fnIdx + 1],
            args[fnIdx + 2],
            args[fnIdx + 3],
            args[fnIdx + 4],
          );
        case 5:
          return this._trace_unknown_op(
            line,
            name,
            overrides,
            fnOrOp.callsitePlan,
            fnOrOp.fn,
            args[fnIdx + 1],
            args[fnIdx + 2],
            args[fnIdx + 3],
            args[fnIdx + 4],
            args[fnIdx + 5],
          );
        case 6:
          return this._trace_unknown_op(
            line,
            name,
            overrides,
            fnOrOp.callsitePlan,
            fnOrOp.fn,
            args[fnIdx + 1],
            args[fnIdx + 2],
            args[fnIdx + 3],
            args[fnIdx + 4],
            args[fnIdx + 5],
            args[fnIdx + 6],
          );
        case 7:
          return this._trace_unknown_op(
            line,
            name,
            overrides,
            fnOrOp.callsitePlan,
            fnOrOp.fn,
            args[fnIdx + 1],
            args[fnIdx + 2],
            args[fnIdx + 3],
            args[fnIdx + 4],
            args[fnIdx + 5],
            args[fnIdx + 6],
            args[fnIdx + 7],
          );
        case 8:
          return this._trace_unknown_op(
            line,
            name,
            overrides,
            fnOrOp.callsitePlan,
            fnOrOp.fn,
            args[fnIdx + 1],
            args[fnIdx + 2],
            args[fnIdx + 3],
            args[fnIdx + 4],
            args[fnIdx + 5],
            args[fnIdx + 6],
            args[fnIdx + 7],
            args[fnIdx + 8],
          );
        default:
          throw new Error(`trace() supports up to 8 op arguments, got ${argCount}`);
      }
    }

    // Function path - no args allowed
    if (argCount > 0) {
      throw new Error('trace() with inline function does not accept additional arguments');
    }
    if (!isTraceFn<OpContextOf<B>>(fnOrOp)) {
      throw new Error('trace() requires an Op or function callback');
    }
    return this._trace_unknown_fn(line, name, overrides, fnOrOp);
  }
  //#endregion smoo/lmao!n/tracer-architecture.trace-overloads

  // ===========================================================================
  // Monomorphic trace methods (for transformer optimization)
  // ===========================================================================

  /**
   * Execute an Op as a root trace (internal monomorphic method).
   * Supports 0-8 arguments. The transformer rewrites trace() calls to this.
   */
  private _trace_op<S, E, Args extends unknown[]>(
    line: number,
    name: string,
    overrides: Record<string, unknown>,
    op: Op<OpContextOf<B>, Args, S, E>,
    ...opArgs: Args
  ): Result<S, E> | Promise<Result<S, E>> {
    if (!isPhysicalLayoutPlanForContext<OpContextOf<B>>(op.callsitePlan, this.logBinding.logSchema)) {
      throw new TypeError('Op physical layout plan does not match this tracer context');
    }
    const ctx = this._createRootContext(line, name, overrides, op.callsitePlan);
    return this._executeResultWithContext(ctx, (c) => op.fn(c, ...opArgs));
  }

  private _trace_unknown_op(
    line: number,
    name: string,
    overrides: Record<string, unknown>,
    callsitePlan: CallsitePlan<B['logBinding']['logSchema'], OpContextOf<B>>,
    fn: (
      ctx: SpanContext<OpContextOf<B>>,
      ...opArgs: unknown[]
    ) => Result<unknown, unknown> | Promise<Result<unknown, unknown>>,
    ...opArgs: unknown[]
  ): Result<unknown, unknown> | Promise<Result<unknown, unknown>> {
    const ctx = this._createRootContext(line, name, overrides, callsitePlan);
    return this._executeResultWithContext(ctx, (childCtx) => fn(childCtx, ...opArgs));
  }

  /**
   * Execute a function as a root trace (internal monomorphic method).
   * The transformer rewrites trace() calls to this.
   */
  private _trace_fn<R>(
    line: number,
    name: string,
    overrides: Record<string, unknown>,
    fn: (ctx: SpanContext<OpContextOf<B>>) => Promise<R>,
  ): Promise<R>;
  private _trace_fn<R>(
    line: number,
    name: string,
    overrides: Record<string, unknown>,
    fn: (ctx: SpanContext<OpContextOf<B>>) => R,
  ): R;
  private _trace_fn(
    line: number,
    name: string,
    overrides: Record<string, unknown>,
    fn: (ctx: SpanContext<OpContextOf<B>>) => unknown,
  ): unknown {
    const metadata = Object.freeze({
      ...createOpMetadata('root', 'tracer', 'runtime', 'unknown', 0),
      _physicalLayoutPlan: this.physicalLayoutPlan,
    });
    const ctx = this._createRootContext(
      line,
      name,
      overrides,
      sealCallsitePlan(this.physicalLayoutPlan, metadata),
    );
    return this._executeUnknownWithContext(ctx, fn);
  }

  private _trace_unknown_fn(
    line: number,
    name: string,
    overrides: Record<string, unknown>,
    fn: (ctx: SpanContext<OpContextOf<B>>) => unknown,
  ): unknown {
    return this._trace_fn(line, name, overrides, fn);
  }

  // ===========================================================================
  // Public monomorphic methods (for transformer direct calls)
  // ===========================================================================

  /**
   * Execute an Op as a root trace (monomorphic - for transformer optimization).
   * Public API for transformer to call directly, bypassing polymorphic dispatch.
   */
  trace_op<S, E, Args extends unknown[]>(
    line: number,
    name: string,
    overrides: TraceOverrides<OpContextOf<B>['userCtx']>,
    op: Op<OpContextOf<B>, Args, S, E>,
    ...opArgs: Args
  ): Result<S, E> | Promise<Result<S, E>> {
    return this._trace_op(line, name, overrides, op, ...opArgs);
  }

  /**
   * Execute a function as a root trace (monomorphic - for transformer optimization).
   * Public API for transformer to call directly, bypassing polymorphic dispatch.
   */
  trace_fn<R>(
    line: number,
    name: string,
    overrides: TraceOverrides<OpContextOf<B>['userCtx']>,
    fn: (ctx: SpanContext<OpContextOf<B>>) => Promise<R>,
  ): Promise<R>;
  trace_fn<R>(
    line: number,
    name: string,
    overrides: TraceOverrides<OpContextOf<B>['userCtx']>,
    fn: (ctx: SpanContext<OpContextOf<B>>) => R,
  ): R;
  trace_fn(
    line: number,
    name: string,
    overrides: TraceOverrides<OpContextOf<B>['userCtx']>,
    fn: (ctx: SpanContext<OpContextOf<B>>) => unknown,
  ): unknown {
    return this._trace_fn(line, name, overrides, fn);
  }

  // ===========================================================================
  // Internal helpers
  // ===========================================================================

  private _createRootContext(
    _line: number,
    name: string,
    overrides: Record<string, unknown>,
    callsitePlan: CallsitePlan<B['logBinding']['logSchema'], OpContextOf<B>>,
  ): SpanContextInstance<OpContextOf<B>> {
    // Extract trace_id from overrides if present
    const traceId: TraceId = isValidTraceId(overrides.trace_id) ? overrides.trace_id : generateTraceId();
    const schema = this.logBinding.logSchema;

    // Validate required null-sentinel fields before allocating the trace root or buffer.
    this._validateRequiredUserContext(overrides);

    // Create TraceRoot via platform-specific factory
    const traceRoot = this.createTraceRoot(traceId, this);

    // Create root SpanBuffer with pre-built TraceRoot via strategy
    const buffer = this.bufferStrategy.createSpanBuffer(
      schema,
      traceRoot,
      callsitePlan.metadata,
      callsitePlan.capacityTier,
      callsitePlan.SpanBufferClass,
    );

    // Write span-start entry (row 0)
    writeSpanStart(buffer, name);

    return new callsitePlan.SpanContextClass(
      buffer,
      schema,
      callsitePlan,
      this.ctxDefaults,
      overrides,
      this.deps,
      this.flagEvaluator,
    );
  }

  /**
   * Validate null-sentinel required fields without materializing a merged
   * user-context carrier.
   *
   * Per the ctx config pattern:
   * - Properties with null values in ctxDefaults must be provided in overrides
   * - Properties with default values can optionally be overridden
   * - undefined is allowed for optional fields
   */
  private _validateRequiredUserContext(overrides: Record<string, unknown>): void {
    for (const key of this.requiredCtxKeys) {
      if (!Object.prototype.propertyIsEnumerable.call(overrides, key)) {
        throw new Error(
          `Required context parameter '${key}' must be provided. ` +
            'Properties with null values in ctx config are required.',
        );
      }
      const providedValue = overrides[key];
      if (providedValue === null || providedValue === undefined) {
        throw new Error(
          `Required context parameter '${key}' must be provided. ` +
            'Properties with null values in ctx config are required.',
        );
      }
    }
  }


  /**
   * Execute function with context and handle span-ok/span-err/span-exception writes
   * Promise-agnostic: returns sync for sync fn, Promise for async fn
   *
   * Writes span-end entry (entry_type, timestamp, error_code) and applies deferred tags
   * when the function returns an Ok/Err result.
   */
  private _executeResultWithContext<S, E>(
    ctx: SpanContextInstance<OpContextOf<B>>,
    fn: (ctx: SpanContext<OpContextOf<B>>) => Result<S, E> | Promise<Result<S, E>>,
  ): Result<S, E> | Promise<Result<S, E>> {
    const buffer = ctx._spanBuffer;

    this.onTraceStart(buffer);

    let isAsync = false;
    try {
      const result = fn(ctx);

      if (isPromiseResult(result)) {
        isAsync = true;
        return result
          .then(
            (resolved) => {
              writeSpanEnd(buffer, resolved);
              return resolved;
            },
            (error: unknown) => {
              buffer._traceRoot._writeSpanEnd(buffer._traceRoot, buffer, ENTRY_TYPE_SPAN_EXCEPTION);

              const errorMessage = error instanceof Error ? error.message : String(error);
              const errorStack = error instanceof Error ? error.stack : undefined;

              buffer.message(1, errorMessage);
              if (errorStack) {
                buffer.exception_stack(1, errorStack);
              }

              throw error;
            },
          )
          .finally(() => this.onTraceEnd(buffer));
      }

      writeSpanEnd(buffer, result);
      return result;
    } catch (error) {
      buffer._traceRoot._writeSpanEnd(buffer._traceRoot, buffer, ENTRY_TYPE_SPAN_EXCEPTION);

      const errorMessage = error instanceof Error ? error.message : String(error);
      const errorStack = error instanceof Error ? error.stack : undefined;

      buffer.message(1, errorMessage);
      if (errorStack) {
        buffer.exception_stack(1, errorStack);
      }

      throw error;
    } finally {
      if (!isAsync) {
        this.onTraceEnd(buffer);
      }
    }
  }

  private _executeUnknownWithContext(
    ctx: SpanContextInstance<OpContextOf<B>>,
    fn: (ctx: SpanContext<OpContextOf<B>>) => unknown,
  ): unknown {
    const buffer = ctx._spanBuffer;

    // Call trace start hook
    this.onTraceStart(buffer);

    let isAsync = false;
    try {
      const result = fn(ctx);

      if (isPromiseResult(result)) {
        isAsync = true;
        return result
          .then(
            (resolved) => {
              if (resolved instanceof Ok || resolved instanceof Err) {
                writeSpanEnd(buffer, resolved);
              } else {
                buffer._traceRoot._writeSpanEnd(buffer._traceRoot, buffer, ENTRY_TYPE_SPAN_OK);
              }
              return resolved;
            },
            (error: unknown) => {
              buffer._traceRoot._writeSpanEnd(buffer._traceRoot, buffer, ENTRY_TYPE_SPAN_EXCEPTION);

              const errorMessage = error instanceof Error ? error.message : String(error);
              const errorStack = error instanceof Error ? error.stack : undefined;

              buffer.message(1, errorMessage);
              if (errorStack) {
                buffer.exception_stack(1, errorStack);
              }

              throw error;
            },
          )
          .finally(() => this.onTraceEnd(buffer));
      }

      // Sync path - write span-end
      if (result instanceof Ok || result instanceof Err) {
        writeSpanEnd(buffer, result);
      } else {
        buffer._traceRoot._writeSpanEnd(buffer._traceRoot, buffer, ENTRY_TYPE_SPAN_OK);
      }

      return result;
    } catch (error) {
      // Sync exception path
      buffer._traceRoot._writeSpanEnd(buffer._traceRoot, buffer, ENTRY_TYPE_SPAN_EXCEPTION);

      // Write exception details
      const errorMessage = error instanceof Error ? error.message : String(error);
      const errorStack = error instanceof Error ? error.stack : undefined;

      buffer.message(1, errorMessage);
      if (errorStack) {
        buffer.exception_stack(1, errorStack);
      }

      throw error;
    } finally {
      // Only call for sync path - async uses .finally() on promise
      if (!isAsync) {
        this.onTraceEnd(buffer);
      }
    }
  }
}
//#endregion smoo/lmao!n/tracer-architecture
