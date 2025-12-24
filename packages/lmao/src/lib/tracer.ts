/**
 * Tracer - Entry point for creating traces with automatic buffer collection
 *
 * Per specs/01c_context_flow_and_op_wrappers.md:
 * - Tracer owns trace() function for creating root spans
 * - Collects SpanBuffers for flushing to external sinks
 * - Bound to application-level OpContext
 *
 * ## Design Rationale
 *
 * Each ESM module has its own `defineOpContext` with its own `LogBinding`.
 * We can't wire a single collector into all those at build time. Instead,
 * the Tracer is created at the application root and `trace()` is the entry
 * point that captures the root SpanBuffer.
 *
 * The Tracer separates concerns:
 * 1. `defineOpContext` - Creates Op factory, schema binding, LogBinding (per-module)
 * 2. `Tracer` - Owns trace(), collects buffers, handles flushing (application-level)
 *
 * @example
 * ```typescript
 * const appContext = defineOpContext({
 *   logSchema: appSchema,
 *   ctx: { env: null as Env },
 * });
 *
 * const tracer = new Tracer(appContext, {
 *   sink: async (table) => { await queue.send(table); },
 * });
 *
 * // In fetch handler
 * const response = await tracer.trace('fetch', { env }, async (ctx) => {
 *   ctx.tag.method('GET');
 *   return new Response('ok');
 * });
 *
 * executionContext.waitUntil(tracer.flush());
 * ```
 *
 * @module tracer
 */

import type { Table } from 'apache-arrow';
import type { TagWriter } from './codegen/fixedPositionWriterGenerator.js';
import { createTagWriter } from './codegen/fixedPositionWriterGenerator.js';
import { createSpanLogger as createSpanLoggerFromGenerator } from './codegen/spanLoggerGenerator.js';
import { convertSpanTreeToArrowTable } from './convertToArrow.js';
import { createOpMetadata } from './opContext/defineOp.js';
import type { DepsConfig, FeatureFlagSchema, OpContextFactory, SpanContext } from './opContext/types.js';
import type { LogSchema } from './schema/LogSchema.js';
import { ENTRY_TYPE_SPAN_EXCEPTION, ENTRY_TYPE_SPAN_OK } from './schema/systemSchema.js';
import type { SchemaFields } from './schema/types.js';
import { createOverflowBuffer, createSpanBuffer } from './spanBuffer.js';
import { createSpanContextProto, type MutableSpanContext, writeSpanStart } from './spanContext.js';
import { getTimestampNanos } from './timestamp.js';
import { generateTraceId, type TraceId } from './traceId.js';
import type { AnySpanBuffer, SpanBuffer } from './types.js';

// =============================================================================
// Types
// =============================================================================

/**
 * Sink function type - receives Arrow table for export
 */
export type TraceSink = (table: Table) => Promise<void> | void;

/**
 * Tracer configuration
 */
export interface TracerConfig {
  /**
   * Sink function to export Arrow tables
   * Called during flush() with converted trace data
   */
  sink: TraceSink;
}

/**
 * Override parameters for trace()
 *
 * Combines optional trace ID with user context properties.
 * Required properties (null-sentinel) from ctx config must be provided here.
 */
export type TraceOverrides<UserCtx extends Record<string, unknown>> = Partial<UserCtx> & {
  /** Use existing trace ID (e.g., from x-trace-id header) */
  traceId?: string | TraceId;
  /** Parent span ID for distributed tracing (our root becomes their child) */
  parentSpanId?: string;
};

// =============================================================================
// Tracer Class
// =============================================================================

/**
 * Tracer - Entry point for creating traces
 *
 * Per spec 01c_context_flow_and_op_wrappers.md:
 * - Owns trace() function for creating root spans
 * - Collects SpanBuffers for flushing
 * - Bound to application-level OpContext
 *
 * @typeParam T - Schema fields type
 * @typeParam FF - Feature flag schema
 * @typeParam Deps - Dependencies config
 * @typeParam UserCtx - User context properties (env, requestId, etc.)
 */
export class Tracer<
  T extends SchemaFields,
  FF extends FeatureFlagSchema,
  Deps extends DepsConfig,
  UserCtx extends Record<string, unknown>,
> {
  private pendingBuffers: AnySpanBuffer[] = [];
  private readonly sink: TraceSink;
  private readonly factory: OpContextFactory<LogSchema<T>, FF, Deps, UserCtx>;
  private readonly spanContextProto: Record<string | symbol, unknown>;

  constructor(factory: OpContextFactory<LogSchema<T>, FF, Deps, UserCtx>, config: TracerConfig) {
    this.factory = factory;
    this.sink = config.sink;

    // Create shared prototype for all SpanContexts created by this tracer
    // This avoids recreating prototype methods per-trace
    this.spanContextProto = createSpanContextProto<LogSchema<T>, FF, UserCtx>(factory.logSchema, factory.logBinding);
  }

  /**
   * Create a new trace with root span
   *
   * Consistent API with defineOp/span: name first, overrides second, body last.
   * Returns exactly what the body returns (no Result wrapper).
   *
   * @param name - Root span name
   * @param fn - Async function to execute in trace context
   * @returns Promise resolving to fn's return value
   */
  trace<R>(name: string, fn: (ctx: SpanContext<LogSchema<T>, FF, Deps, UserCtx>) => Promise<R>): Promise<R>;

  /**
   * Create a new trace with root span and overrides
   *
   * @param name - Root span name
   * @param overrides - Trace ID and/or user context overrides
   * @param fn - Async function to execute in trace context
   * @returns Promise resolving to fn's return value
   */
  trace<R>(
    name: string,
    overrides: TraceOverrides<UserCtx>,
    fn: (ctx: SpanContext<LogSchema<T>, FF, Deps, UserCtx>) => Promise<R>,
  ): Promise<R>;

  async trace<R>(
    name: string,
    overridesOrFn: TraceOverrides<UserCtx> | ((ctx: SpanContext<LogSchema<T>, FF, Deps, UserCtx>) => Promise<R>),
    maybeFn?: (ctx: SpanContext<LogSchema<T>, FF, Deps, UserCtx>) => Promise<R>,
  ): Promise<R> {
    // Parse overloaded arguments
    const hasOverrides = typeof overridesOrFn !== 'function';
    const overrides = hasOverrides ? (overridesOrFn as TraceOverrides<UserCtx>) : ({} as TraceOverrides<UserCtx>);
    const fn = hasOverrides
      ? maybeFn!
      : (overridesOrFn as (ctx: SpanContext<LogSchema<T>, FF, Deps, UserCtx>) => Promise<R>);

    // Generate or use provided trace ID
    const traceId: TraceId = overrides.traceId ? (overrides.traceId as TraceId) : generateTraceId();

    // Get logBinding from factory - contains the MERGED schema (user + system columns)
    const logBinding = this.factory.logBinding;
    // IMPORTANT: Use logBinding.logSchema (merged schema) not factory.logSchema (user schema)
    // The merged schema includes system columns like message(), line(), etc.
    const schema = logBinding.logSchema as LogSchema<T>;

    // Create root SpanBuffer
    const buffer = createSpanBuffer(schema, logBinding, name, traceId) as SpanBuffer<LogSchema<T>>;

    // Register with pendingBuffers for collection
    this.pendingBuffers.push(buffer);

    // Initialize scope values to empty frozen object
    buffer._scopeValues = Object.freeze({});

    // Set _opMetadata for Arrow conversion (root spans use a default/runtime metadata)
    // For root spans created via Tracer, we don't have transformer-injected metadata,
    // so we create a runtime placeholder
    buffer._opMetadata = createOpMetadata('root', 'tracer', 'runtime', 0);

    // Write span-start entry (row 0) and pre-initialize span-end (row 1)
    writeSpanStart(buffer, name);

    // Merge user context: factory defaults + overrides
    const ctxDefaults = this.factory.ctxDefaults ?? ({} as UserCtx);
    const resolvedUserCtx: Record<string, unknown> = { ...ctxDefaults };

    // Override with provided params (excluding traceId/parentSpanId which are trace-level)
    for (const key of Object.keys(overrides) as (keyof typeof overrides)[]) {
      if (key !== 'traceId' && key !== 'parentSpanId') {
        const value = overrides[key];
        if (value !== undefined) {
          resolvedUserCtx[key as string] = value;
        }
      }
    }

    // Create SpanContext using prototype inheritance
    const ctx = Object.create(this.spanContextProto) as MutableSpanContext<LogSchema<T>, FF, UserCtx>;

    // Set instance properties
    ctx._buffer = buffer;
    ctx._schema = schema;
    ctx._logBinding = logBinding;

    // Create tag writer and span logger
    ctx.tag = createTagWriter(schema, buffer) as TagWriter<LogSchema<T>>;
    ctx.log = createSpanLoggerFromGenerator(schema, buffer, createOverflowBuffer);
    ctx._spanLogger = ctx.log;

    // Spread user context properties onto context
    for (const [key, value] of Object.entries(resolvedUserCtx)) {
      (ctx as Record<string, unknown>)[key] = value;
    }

    // Set up deps (empty for root, ops provide their own)
    ctx.deps = {};

    // Set up feature flags if evaluator provided
    const flagEvaluator = this.factory.flagEvaluator;
    if (flagEvaluator) {
      // forContext expects SpanContext without ff (to prevent recursion)
      // It returns a bound evaluator that becomes ctx.ff
      // biome-ignore lint/suspicious/noExplicitAny: Type erasure needed for FlagEvaluator generics
      ctx.ff = (flagEvaluator as any).forContext(ctx);
    } else {
      // No-op ff if no evaluator - empty object that won't error on access
      // biome-ignore lint/suspicious/noExplicitAny: Type erasure for empty ff
      ctx.ff = {} as any;
    }

    // Execute fn with try/catch
    try {
      const result = await fn(ctx as unknown as SpanContext<LogSchema<T>, FF, Deps, UserCtx>);

      // Write span-ok to row 1 (fixed layout)
      buffer.entry_type[1] = ENTRY_TYPE_SPAN_OK;
      buffer.timestamp[1] = getTimestampNanos(buffer._traceRoot.anchorEpochNanos, buffer._traceRoot.anchorPerfNow);

      return result;
    } catch (error) {
      // Write span-exception to row 1 (fixed layout)
      buffer.entry_type[1] = ENTRY_TYPE_SPAN_EXCEPTION;
      buffer.timestamp[1] = getTimestampNanos(buffer._traceRoot.anchorEpochNanos, buffer._traceRoot.anchorPerfNow);

      // Write exception details
      const errorMessage = error instanceof Error ? error.message : String(error);
      const errorStack = error instanceof Error ? error.stack : undefined;

      buffer.message(1, errorMessage);
      if (errorStack) {
        buffer.exception_stack(1, errorStack);
      }

      // Re-throw - user handles errors
      throw error;
    }
  }

  /**
   * Flush all pending buffers to sink
   *
   * Call this at the end of request handling to ensure all traces are exported.
   * Integrates with CF Worker's waitUntil for background processing.
   *
   * @example
   * executionContext.waitUntil(tracer.flush());
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
   * Hint that flush should happen soon (non-blocking)
   *
   * Can be called to indicate the tracer should flush when convenient.
   * Implementation may batch or delay actual flushing.
   */
  hintFlush(): void {
    // For now, just schedule a microtask to flush
    // Future: could implement batching, debouncing, etc.
    queueMicrotask(() => {
      this.flush().catch((error) => {
        console.error('Error in hinted flush:', error);
      });
    });
  }

  /**
   * Get count of pending buffers (for testing/monitoring)
   */
  get pendingCount(): number {
    return this.pendingBuffers.length;
  }
}
