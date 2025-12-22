/**
 * Op - Traced operation class
 *
 * Per specs/01l_module_builder_pattern.md and specs/01c_context_flow_and_op_wrappers.md:
 * - Wraps a user function with automatic span tracking
 * - Captures module metadata for attribution
 * - Has _invoke method called by span()
 * - Op name is used for metrics tracking (separate from span names)
 *
 * This file breaks the circular dependency between traceContext.ts and defineModule.ts
 * by providing the Op class implementation in its own module.
 */

import { createTagWriter } from './codegen/fixedPositionWriterGenerator.js';
import type { ModuleContext } from './moduleContext.js';
import type { FeatureFlagSchema } from './schema/defineFeatureFlags.js';
import type { FeatureFlagEvaluator, InferFeatureFlagsWithContext } from './schema/evaluator.js';
import type { LogSchema } from './schema/types.js';
import { createChildSpanBuffer, createSpanBuffer } from './spanBuffer.js';
import {
  createSpanContextProto,
  createSpanLogger,
  type MutableSpanContext,
  type SpanLogger,
  writeSpanStart,
} from './spanContext.js';
import { getTimestampNanos } from './timestamp.js';
import type { TraceContext, TraceContextBase } from './traceContext.js';

import type { SpanBuffer } from './types.js';

// =============================================================================
// Op Brand Symbol
// =============================================================================

/**
 * Brand symbol to uniquely identify Op instances for overload discrimination
 *
 * Per spec 01o_typescript_transformer.md and experiment exp7-full-ctx.ts:
 * Used in SpanFn overloads to distinguish Op instances from context objects and closures
 */
export const OpBrand: unique symbol = Symbol('OpBrand');

// =============================================================================
// Reserved Context Keys
// =============================================================================

/**
 * Reserved keys that cannot be used as Extra properties in TraceContext/SpanContext
 *
 * These are system properties that are always present and should not be overridden.
 * Created once at module load time for V8 optimization (not per invocation).
 */
const RESERVED_CONTEXT_KEYS = new Set<string>([
  'ff',
  'tag',
  'log',
  'buffer',
  'scope',
  'ok',
  'err',
  'span',
  'anchorEpochMicros',
  'anchorPerfNow',
]);

// =============================================================================
// Op Class
// =============================================================================

/**
 * Op - Traced operation
 *
 * Per spec 01l lines 436-453 and 01c lines 292-362:
 * - Wraps a user function with automatic span tracking
 * - Captures module metadata for attribution
 * - Has _invoke method called by span()
 *
 * Type parameter order matches function signature: (ctx: Ctx, ...args: Args) => Promise<Result>
 *
 * Op has TWO names:
 * - `name`: The Op's name for metrics (invocations, errors, duration tracking)
 * - Span names are provided at CALL SITE: `await span('contextual-name', myOp, args)`
 *
 * Op captures the module for source attribution:
 * - When span() invokes this Op, the Op's module becomes buffer._module (for rows 1+)
 * - The caller's module becomes buffer._callsiteModule (for row 0)
 *
 * @typeParam Ctx - Required context type (contravariant position)
 * @typeParam Args - Tuple of argument types (excluding ctx)
 * @typeParam Result - Return type
 */
export class Op<Ctx, Args extends unknown[], Result> {
  /** Brand to distinguish Op from other objects in overload resolution */
  declare readonly [OpBrand]: true;

  constructor(
    /** The Op's name for metrics (invocations, errors, duration) */
    readonly name: string,
    /** The module where this Op was defined - for git_sha/package_name/package_file attribution */
    readonly module: ModuleContext,
    /** The user function to execute */
    private fn: (ctx: Ctx, ...args: Args) => Promise<Result>,
    /** Line number where op was defined (for transformer support) */
    readonly definitionLine?: number,
  ) {}

  /**
   * Internal invocation - called by span()
   *
   * Per spec 01c lines 309-362:
   * @param traceCtx - The root trace context
   * @param parentBuffer - Parent span's buffer (null for root)
   * @param callsiteModule - The CALLER's module (where span() was invoked)
   * @param spanName - Name decided by caller
   * @param lineNumber - Line number where span() was called
   * @param args - User arguments to the op
   */
  async _invoke(
    traceCtx: TraceContext<FeatureFlagSchema, Record<string, unknown>>,
    parentBuffer: SpanBuffer | null,
    _callsiteModule: ModuleContext,
    spanName: string,
    lineNumber: number,
    args: Args,
  ): Promise<Result> {
    // 1. Get schema from module
    const schemaOnly = this.module.logSchema;

    // 2. Create SpanBuffer with module reference directly (no TaskContext wrapper)
    let spanBuffer: SpanBuffer<LogSchema>;
    if (parentBuffer) {
      // Child span - use createChildSpanBuffer which handles parent chain, callsiteModule, scope
      spanBuffer = createChildSpanBuffer(parentBuffer, this.module, spanName);

      // Register with parent's children array (RemappedBufferView if prefixed)
      // Only check for RemappedBufferView when registering with parent (child spans only)
      // Root spans don't need RemappedBufferView - they're not in any parent's children array
      if (this.module.remappedViewClass) {
        // Module has prefix - wrap buffer in RemappedBufferView for parent's tree traversal
        const view = new this.module.remappedViewClass(spanBuffer);
        parentBuffer._children.push(view);
      } else {
        // No prefix - push raw buffer directly
        parentBuffer._children.push(spanBuffer);
      }
    } else {
      // Root span - uses trace_id from TraceContext
      spanBuffer = createSpanBuffer(schemaOnly, this.module, spanName, traceCtx.trace_id);
    }

    // 4. Write span-start entry (row 0) and pre-initialize span-end (row 1)
    writeSpanStart(spanBuffer, spanName);

    // Write line number to row 0 if provided
    if (lineNumber > 0) {
      spanBuffer.line(0, lineNumber);
    }

    // 5. Create span logger with typed logging methods
    const spanLogger = createSpanLogger(schemaOnly, spanBuffer as SpanBuffer<LogSchema>);

    // 6. Create tag writer for span attributes (writes to row 0)
    const tagAPI = createTagWriter(schemaOnly, spanBuffer);

    // 7. Create SpanContext prototype for this invocation
    const spanContextProto = createSpanContextProto(schemaOnly, this.module);

    // 8. Create span context using prototype-based inheritance
    const spanContext = Object.create(spanContextProto) as MutableSpanContext<
      LogSchema,
      FeatureFlagSchema,
      Record<string, unknown>
    >;

    // 9. Copy user properties from traceCtx to spanContext
    const traceCtxAny = traceCtx as unknown as TraceContextBase;
    const extraKeys = traceCtxAny._extraKeys;
    if (extraKeys) {
      for (let i = 0; i < extraKeys.length; i++) {
        const key = extraKeys[i];
        (spanContext as Record<string, unknown>)[key] = (traceCtxAny as any)[key];
      }
    } else {
      // Fallback for non-prototype contexts (unlikely in production)
      for (const key of Object.keys(traceCtxAny)) {
        if (!RESERVED_CONTEXT_KEYS.has(key)) {
          (spanContext as Record<string, unknown>)[key] = (traceCtxAny as any)[key];
        }
      }
    }

    spanContext.tag = tagAPI;
    spanContext.log = spanLogger as SpanLogger<LogSchema>;
    spanContext._buffer = spanBuffer;
    spanContext._schema = schemaOnly;
    spanContext._spanLogger = spanLogger;
    // Store traceCtx reference for Op invocation via span_op
    spanContext._traceCtx = traceCtx;

    // 11. Create feature flag evaluator bound to this span context
    // Must be after spanContext is created since forContext receives the full SpanContext
    // forContext() creates a span-bound evaluator with typed getters
    const spanFf = traceCtx.ff.forContext?.(spanContext);
    spanContext.ff = spanFf as unknown as FeatureFlagEvaluator<FeatureFlagSchema> &
      InferFeatureFlagsWithContext<FeatureFlagSchema>;

    // 12. Execute op function with exception handling
    try {
      return await this.fn(spanContext as unknown as Ctx, ...args);
    } catch (error) {
      // Write span-exception to row 1 (fixed layout)
      spanBuffer.timestamp[1] = getTimestampNanos();

      // Write exception details to row 1
      const errorMessage = error instanceof Error ? error.message : String(error);
      const errorStack = error instanceof Error ? error.stack : undefined;

      spanBuffer.message(1, errorMessage);
      if (errorStack) {
        spanBuffer.exception_stack(1, errorStack);
      }

      // Re-throw to propagate
      throw error;
    }
  }
}
