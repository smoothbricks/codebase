/**
 * Tracer - Abstract base class for trace collection
 *
 * Per specs/01c_context_flow_and_op_wrappers.md:
 * - Tracer owns trace() function for creating root spans
 * - Collects SpanBuffers in pendingBuffers array
 * - Subclasses implement flush() to define persistence strategy
 *
 * ## Design Philosophy
 *
 * Tracer is ABSTRACT - no sink, no persistence assumptions:
 * - `trace()` executes ops and collects buffers
 * - `flush()` is abstract - subclasses decide what "flush" means
 * - Flushing is always a hint (batching, rate limiting, async queues)
 *
 * Concrete implementations might:
 * - Send to queue (Cloudflare Queue, SQS, Kafka)
 * - Write to disk/database
 * - Send to observability backend (DataDog, Honeycomb)
 * - Accumulate in memory for testing
 * - Drop buffers (dev/testing no-op)
 *
 * @example
 * ```typescript
 * // Concrete implementation - Cloudflare Queue
 * class QueueTracer extends Tracer {
 *   constructor(config, private queue: Queue) {
 *     super(config);
 *   }
 *
 *   async flush() {
 *     const buffers = this.takePendingBuffers();
 *     for (const buffer of buffers) {
 *       const table = convertSpanTreeToArrowTable(buffer);
 *       await this.queue.send(table);
 *     }
 *   }
 * }
 *
 * // Usage
 * const { trace, flush } = new QueueTracer({ logBinding }, env.TRACES);
 * await trace('fetch', async (ctx) => {
 *   ctx.tag.userId('user-123');
 *   return new Response('ok');
 * });
 * executionContext.waitUntil(flush());
 * ```
 *
 * @module tracer
 */

import type { TagWriter } from './codegen/fixedPositionWriterGenerator.js';
import { createTagWriter } from './codegen/fixedPositionWriterGenerator.js';
import { createSpanLogger as createSpanLoggerFromGenerator } from './codegen/spanLoggerGenerator.js';
import { convertSpanTreeToArrowTable } from './convertToArrow.js';
import type { LogBinding } from './logBinding.js';
import { Op } from './op.js';
import type { TraceContextParams } from './opContext/contextTypes.js';
import { createOpMetadata } from './opContext/defineOp.js';
import type { OpContext, SpanContext } from './opContext/types.js';
import { FluentErr, FluentOk, type Result } from './result.js';
import type { FlagEvaluator } from './schema/evaluator.js';
import { ENTRY_TYPE_SPAN_EXCEPTION, ENTRY_TYPE_SPAN_OK } from './schema/systemSchema.js';

import { createOverflowBuffer, createSpanBuffer, EMPTY_SCOPE } from './spanBuffer.js';
import {
  createSpanContextClass,
  type SpanContextClass,
  type SpanContextInstance,
  writeSpanStart,
} from './spanContext.js';
import { getTimestampNanos } from './timestamp.js';
import { generateTraceId, type TraceId } from './traceId.js';
import type { AnySpanBuffer, SpanBuffer } from './types.js';

// =============================================================================
// Types
// =============================================================================

/**
 * Sink function type - receives Arrow table for export
 */
export type TraceSink = (table: import('apache-arrow').Table) => Promise<void> | void;

/**
 * Tracer configuration - runtime concerns only
 *
 * @typeParam Ctx - Bundled OpContext type (logSchema, flags, deps, userCtx)
 */
export interface TracerConfig<Ctx extends OpContext> {
  /**
   * LogBinding from defineOpContext - contains schema and capacity stats
   * Type matches OpContextFactory.logBinding for Op type safety
   */
  logBinding: LogBinding<Ctx['logSchema']>;

  /**
   * Sink function to export Arrow tables
   * Called during flush() with converted trace data
   */
  sink: TraceSink;

  /**
   * Optional feature flag evaluator
   *
   * Provides getSync/getAsync/forContext methods to resolve flag values at runtime.
   * If not provided, ctx.ff will be an empty object.
   *
   * @example
   * ```typescript
   * flagEvaluator: new InMemoryFlagEvaluator(flags.schema, { darkMode: true })
   * ```
   */
  flagEvaluator?: FlagEvaluator<Ctx>;

  /**
   * User context defaults from defineOpContext ctx config.
   *
   * Properties with null values are required at trace creation.
   * Properties with other values are defaults that can be overridden.
   *
   * @example
   * ```typescript
   * ctxDefaults: { env: null, config: { retryCount: 3 } }
   * ```
   */
  ctxDefaults?: Ctx['userCtx'];

  /**
   * Dependencies wired at defineOpContext time.
   *
   * These are passed to ctx.deps on root spans, allowing ops to
   * access library operations via ctx.span('name', ctx.deps.http.request).
   *
   * @example
   * ```typescript
   * deps: { http: httpOps.prefix('http'), auth: authOps.prefix('auth') }
   * ```
   */
  deps?: Ctx['deps'];
}

/**
 * Trace overrides - passed per-trace
 *
 * @typeParam UserCtx - User context type from OpContext
 */
export interface TraceOptions<UserCtx = Record<string, unknown>> {
  /** Use existing trace ID (e.g., from x-trace-id header) */
  traceId?: string | TraceId;
  /** Parent span ID for distributed tracing */
  parentSpanId?: string;
  /** User context params - required props from null sentinels, optional overrides for defaults */
  ctx?: TraceContextParams<UserCtx>;
}

// =============================================================================
// Tracer Class
// =============================================================================

/**
 * Tracer - Concrete class for trace collection
 *
 * Provides trace execution, buffer collection, and flushing to sink.
 *
 * @typeParam Ctx - Bundled OpContext type (logSchema, flags, deps, userCtx)
 */
export class Tracer<Ctx extends OpContext> {
  protected pendingBuffers: AnySpanBuffer[] = [];
  private readonly logBinding: LogBinding<Ctx['logSchema']>;
  private readonly sink: TraceSink;
  private readonly SpanContextClass: SpanContextClass<Ctx>;
  private readonly flagEvaluator?: FlagEvaluator<Ctx>;
  private readonly ctxDefaults: Ctx['userCtx'];
  private readonly deps: Ctx['deps'];

  constructor(config: TracerConfig<Ctx>) {
    this.logBinding = config.logBinding;
    this.sink = config.sink;
    this.flagEvaluator = config.flagEvaluator;
    this.ctxDefaults = config.ctxDefaults ?? (EMPTY_SCOPE as Ctx['userCtx']);
    this.deps = config.deps ?? (EMPTY_SCOPE as Ctx['deps']);

    // Create SpanContext class for all contexts created by this tracer
    // The class closes over schema/logBinding and provides typed methods
    this.SpanContextClass = createSpanContextClass<Ctx>(config.logBinding.logSchema, config.logBinding);

    // Bind methods for destructuring (per AGENTS.md - "Always destructure")
    this.trace = this.trace.bind(this);
    this.trace_op = this.trace_op.bind(this);
    this.trace_fn = this.trace_fn.bind(this);
    this.flush = this.flush.bind(this);
    this.pendingCount = this.pendingCount.bind(this);
  }

  // ===========================================================================
  // trace() - Polymorphic dispatcher (like span())
  // ===========================================================================

  // Overload 1: trace(name, op) - execute Op directly
  trace<S, E>(name: string, op: Op<Ctx, [], S, E>): Promise<Result<S, E>>;

  // Overload 2: trace(name, fn) - execute function (returns any value R, sync or async)
  trace<R>(name: string, fn: (ctx: SpanContext<Ctx>) => R): R;

  // Overload 3: trace(name, options, op)
  trace<S, E>(name: string, options: TraceOptions<Ctx['userCtx']>, op: Op<Ctx, [], S, E>): Promise<Result<S, E>>;

  // Overload 4: trace(name, options, fn) - execute function (returns any value R, sync or async)
  trace<R>(name: string, options: TraceOptions<Ctx['userCtx']>, fn: (ctx: SpanContext<Ctx>) => R): R;

  // Implementation
  trace(
    name: string,
    optionsOrOpOrFn:
      | TraceOptions<Ctx['userCtx']>
      | Op<Ctx, [], unknown, unknown>
      | ((ctx: SpanContext<Ctx>) => unknown),
    maybeOpOrFn?: Op<Ctx, [], unknown, unknown> | ((ctx: SpanContext<Ctx>) => unknown),
  ): unknown {
    // Parse overloaded arguments - dispatch to trace_op or trace_fn
    // Pattern matches span() dispatcher in spanContext.ts
    let options: TraceOptions<Ctx['userCtx']>;
    let opOrFn: Op<Ctx, [], unknown, unknown> | ((ctx: SpanContext<Ctx>) => unknown);

    if (optionsOrOpOrFn instanceof Op) {
      options = {};
      opOrFn = optionsOrOpOrFn;
    } else if (typeof optionsOrOpOrFn === 'function') {
      options = {};
      opOrFn = optionsOrOpOrFn;
    } else {
      options = optionsOrOpOrFn;
      opOrFn = maybeOpOrFn!;
    }

    // Dispatch to monomorphic methods
    // TODO: lmao-transformer should rewrite trace(name, op) → trace_op(name, {}, op)
    if (opOrFn instanceof Op) {
      return this.trace_op(name, options, opOrFn);
    }
    return this.trace_fn(name, options, opOrFn);
  }

  // ===========================================================================
  // trace_op - Monomorphic method for Op execution (Promise-agnostic)
  // ===========================================================================

  /**
   * Execute an Op as a root trace (monomorphic - for transformer optimization)
   * Returns Result<S, E> (sync) or Promise<Result<S, E>> (async) - preserves sync/async
   */
  trace_op<S, E>(
    name: string,
    options: TraceOptions<Ctx['userCtx']>,
    op: Op<Ctx, [], S, E>,
  ): Result<S, E> | Promise<Result<S, E>> {
    const ctx = this._createRootContext(name, options, op.metadata);
    return this._executeWithContext(ctx, (c) => op.fn(c));
  }

  // ===========================================================================
  // trace_fn - Monomorphic method for function execution (Promise-agnostic)
  // ===========================================================================

  /**
   * Execute a function as a root trace (monomorphic - for transformer optimization)
   * Returns R (sync) or Promise<R> (async) - preserves sync/async
   */
  trace_fn<R>(name: string, options: TraceOptions<Ctx['userCtx']>, fn: (ctx: SpanContext<Ctx>) => R): R {
    const ctx = this._createRootContext(name, options, createOpMetadata('root', 'tracer', 'runtime', 'unknown', 0));
    return this._executeWithContext(ctx, fn);
  }

  // ===========================================================================
  // Internal helpers
  // ===========================================================================

  private _createRootContext(
    name: string,
    options: TraceOptions<Ctx['userCtx']>,
    metadata: ReturnType<typeof createOpMetadata>,
  ): SpanContextInstance<Ctx> {
    const traceId: TraceId = options.traceId ? (options.traceId as TraceId) : generateTraceId();
    const schema = this.logBinding.logSchema as Ctx['logSchema'];

    // Validate null-sentinel required fields and merge user context
    const resolvedUserCtx = this._resolveUserContext(options.ctx);

    // Create root SpanBuffer
    const buffer = createSpanBuffer(
      schema,
      name, // spanName
      traceId, // trace_id
      metadata, // opMetadata (for both callsite and op at root level)
      // capacity omitted - uses default from class stats
    ) as SpanBuffer<Ctx['logSchema']>;

    // Register with pendingBuffers for collection
    this.pendingBuffers.push(buffer);

    // Initialize scope values to shared frozen empty object (avoid allocation)
    buffer._scopeValues = EMPTY_SCOPE;

    // Set _opMetadata for Arrow conversion
    buffer._opMetadata = metadata;

    // Write span-start entry (row 0)
    writeSpanStart(buffer, name);

    // Create tag writer and span logger (needed for constructor)
    const tagWriter = createTagWriter(schema, buffer) as TagWriter<Ctx['logSchema']>;
    const spanLogger = createSpanLoggerFromGenerator(schema, buffer, createOverflowBuffer);

    // Instantiate SpanContext class with direct arguments (no temp object allocation)
    const ctx = new this.SpanContextClass(buffer, schema, spanLogger, tagWriter) as SpanContextInstance<Ctx>;

    // Set up deps from config (may be EMPTY_SCOPE if none provided)
    ctx.deps = this.deps;

    // Set up feature flags if evaluator provided
    if (this.flagEvaluator) {
      // biome-ignore lint/suspicious/noExplicitAny: FF types are complex, forContext handles it
      ctx.ff = (this.flagEvaluator as any).forContext(ctx);
    } else {
      // biome-ignore lint/suspicious/noExplicitAny: Empty object as fallback when no evaluator
      ctx.ff = EMPTY_SCOPE as any;
    }

    // Spread resolved user context onto SpanContext
    for (const [key, value] of Object.entries(resolvedUserCtx)) {
      (ctx as Record<string, unknown>)[key] = value;
    }

    return ctx;
  }

  /**
   * Validate null-sentinel required fields and merge with provided params.
   *
   * Per the ctx config pattern:
   * - Properties with null values must be provided at trace creation
   * - Properties with default values can optionally be overridden
   * - undefined is allowed for optional fields
   */
  private _resolveUserContext(params?: TraceContextParams<Ctx['userCtx']>): Record<string, unknown> {
    const ctxDefaults = this.ctxDefaults as Record<string, unknown>;

    // Check that all null-sentinel keys are provided in params
    for (const key of Object.keys(ctxDefaults)) {
      const defaultValue = ctxDefaults[key];
      if (defaultValue === null) {
        const providedValue = (params as Record<string, unknown> | undefined)?.[key];
        if (providedValue === null || providedValue === undefined) {
          throw new Error(
            `Required context parameter '${key}' must be provided. ` +
              'Properties with null values in ctx config are required.',
          );
        }
      }
    }

    // Merge defaults with provided params (provided values win)
    const resolvedUserCtx: Record<string, unknown> = { ...ctxDefaults };
    if (params) {
      for (const key of Object.keys(params)) {
        const value = (params as Record<string, unknown>)[key];
        if (value !== undefined) {
          resolvedUserCtx[key] = value;
        }
      }
    }

    return resolvedUserCtx;
  }

  /**
   * Execute function with context and handle span-ok/span-err/span-exception writes
   * Promise-agnostic: returns sync for sync fn, Promise for async fn
   *
   * NOTE: If the function returns a FluentOk/FluentErr (from ctx.ok()/ctx.err()),
   * the entry type was already written by the constructor. We detect this via
   * instanceof and skip overwriting.
   */
  private _executeWithContext<R>(ctx: SpanContextInstance<Ctx>, fn: (ctx: SpanContext<Ctx>) => R): R {
    const buffer = ctx._buffer;

    try {
      const result = fn(ctx as SpanContext<Ctx>);

      // Check if result is a Promise
      if (result && typeof (result as { then?: unknown }).then === 'function') {
        // Async path - use .then to write span-ok/err after Promise resolves
        return (result as unknown as Promise<unknown>).then(
          (resolved) => {
            // If result is FluentOk/FluentErr (from ctx.ok()/ctx.err()), entry type was already
            // written by the constructor. Skip overwriting.
            if (resolved instanceof FluentOk || resolved instanceof FluentErr) {
              return resolved;
            }
            // Write span-ok to row 1 (fallback for non-FluentResult returns)
            buffer.entry_type[1] = ENTRY_TYPE_SPAN_OK;
            buffer.timestamp[1] = getTimestampNanos(
              buffer._traceRoot.anchorEpochNanos,
              buffer._traceRoot.anchorPerfNow,
            );
            return resolved;
          },
          (error: unknown) => {
            // Write span-exception to row 1
            buffer.entry_type[1] = ENTRY_TYPE_SPAN_EXCEPTION;
            buffer.timestamp[1] = getTimestampNanos(
              buffer._traceRoot.anchorEpochNanos,
              buffer._traceRoot.anchorPerfNow,
            );

            // Write exception details
            const errorMessage = error instanceof Error ? error.message : String(error);
            const errorStack = error instanceof Error ? error.stack : undefined;

            buffer.message(1, errorMessage);
            if (errorStack) {
              buffer.exception_stack(1, errorStack);
            }

            throw error;
          },
        ) as R;
      }

      // Sync path
      // If result is FluentOk/FluentErr (from ctx.ok()/ctx.err()), entry type was already
      // written by the constructor. Skip overwriting.
      if (result instanceof FluentOk || result instanceof FluentErr) {
        return result;
      }
      // Write span-ok immediately (fallback for non-FluentResult returns)
      buffer.entry_type[1] = ENTRY_TYPE_SPAN_OK;
      buffer.timestamp[1] = getTimestampNanos(buffer._traceRoot.anchorEpochNanos, buffer._traceRoot.anchorPerfNow);

      return result;
    } catch (error) {
      // Sync exception path
      buffer.entry_type[1] = ENTRY_TYPE_SPAN_EXCEPTION;
      buffer.timestamp[1] = getTimestampNanos(buffer._traceRoot.anchorEpochNanos, buffer._traceRoot.anchorPerfNow);

      // Write exception details
      const errorMessage = error instanceof Error ? error.message : String(error);
      const errorStack = error instanceof Error ? error.stack : undefined;

      buffer.message(1, errorMessage);
      if (errorStack) {
        buffer.exception_stack(1, errorStack);
      }

      throw error;
    }
  }

  /**
   * Flush pending buffers to sink
   *
   * @example
   * ```typescript
   * const { trace, flush } = new Tracer({ logBinding, sink });
   * executionContext.waitUntil(flush());
   * ```
   */
  async flush(): Promise<void> {
    const buffers = this.pendingBuffers;
    this.pendingBuffers = [];

    for (const buffer of buffers) {
      try {
        const table = convertSpanTreeToArrowTable(buffer);
        await this.sink(table);
      } catch (error) {
        console.error('Error flushing trace buffer:', error);
      }
    }
  }

  /**
   * Get count of pending buffers (for testing/monitoring)
   *
   * Note: This is a method (not a getter) so it can be safely destructured
   * along with trace() and flush().
   */
  pendingCount(): number {
    return this.pendingBuffers.length;
  }
}
