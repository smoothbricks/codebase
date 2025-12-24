/**
 * TraceContext Types and Prototype System
 *
 * Per specs/01c_context_flow_and_op_wrappers.md and specs/01l_module_builder_pattern.md:
 * - TraceContext is created via module.traceContext() at request entry
 * - Uses prototype-based inheritance for V8 hidden class optimization
 * - Contains system properties (traceId, timestamps, threadId) + user Extra
 */

import type { OpContext } from './opContext/types.js';
import type { Result } from './result.js';
import type { FeatureFlagSchema } from './schema/defineFeatureFlags.js';
import type { FlagEvaluator } from './schema/evaluator.js';
import type { LogSchema } from './schema/types.js';
import type { TraceId } from './traceId.js';

// =============================================================================
// Reserved Keys
// =============================================================================

/**
 * Reserved keys that Extra cannot contain (compile-time enforcement)
 *
 * Per spec 01l lines 379-386
 */
export type ReservedTraceContextKeys = 'traceId' | 'anchorEpochMicros' | 'anchorPerfNow' | 'threadId' | 'ff' | 'span';

// =============================================================================
// Op Type (imported from op.ts)
// =============================================================================

/**
 * Op type - traced operation
 *
 * Imported from op.ts to break circular dependency.
 * The actual Op class is defined in op.ts.
 */
import type { Op } from './op.js';

export type { Op } from './op.js';

// =============================================================================
// RootSpanFn Type
// =============================================================================

/**
 * Function type for ctx.span() at the root level
 *
 * Per spec 01c lines 243-244 and experiment exp7-full-ctx.ts lines 253-259:
 * Supports both with and without line number (transformer injects line number)
 *
 * The first argument (lineNumber) is injected by the transformer.
 */
export type RootSpanFn = {
  // Overload 1: With line number (transformer output)
  <Ctx extends OpContext, Args extends unknown[], S, E>(
    line: number,
    name: string,
    op: Op<Ctx, Args, S, E>,
    ...args: Args
  ): Promise<Result<S, E>>;
  // Overload 2: Without line number (user writes)
  <Ctx extends OpContext, Args extends unknown[], S, E>(
    name: string,
    op: Op<Ctx, Args, S, E>,
    ...args: Args
  ): Promise<Result<S, E>>;
};

// =============================================================================
// TraceContext Interface
// =============================================================================

/**
 * TraceContext system properties - root context created via module.traceContext()
 *
 * Per spec 01c lines 206-216 and 01l lines 369-377:
 * - System properties are auto-generated and cannot be overridden via Extra
 *
 * @typeParam FF - Feature flag schema type
 * @typeParam T - LogSchema type (for typed evaluation context)
 * @typeParam Env - Environment type
 */
export interface TraceContextSystem<
  FF extends FeatureFlagSchema,
  T extends LogSchema = LogSchema,
  Env extends Record<string, unknown> = Record<string, unknown>,
> {
  // System properties (always present, auto-generated)
  readonly anchorEpochMicros: number; // Nanoseconds.now() / 1000n at trace root (microsecond precision)
  readonly anchorPerfNow: number; // performance.now() at trace root
  /** Root flag evaluator - use forContext(spanCtx) to get span-bound evaluator */
  readonly ff: FlagEvaluator<OpContext<T, FF, {}, Env>, FF>;
  readonly span: RootSpanFn;
}

/**
 * TraceContext - system properties + user Extra
 *
 * Per spec 01c lines 215-216:
 * ```typescript
 * interface TraceContext<FF, Extra> { ... } & Extra;
 * ```
 *
 * Extra properties are spread via intersection at usage site.
 *
 * @typeParam FF - Feature flag schema type
 * @typeParam Extra - User-defined properties (e.g., requestId, userId, env)
 */
export type TraceContext<FF extends FeatureFlagSchema, Extra extends Record<string, unknown>> = TraceContextSystem<FF> &
  Extra;

// =============================================================================
// Prototype System for V8 Hidden Class Optimization
// =============================================================================

/**
 * Internal symbol to mark contexts as prototype-based
 * Used for type guards and instanceof-like checks
 */
export const TRACE_CONTEXT_MARKER = Symbol.for('lmao.TraceContext');

/**
 * Base prototype interface for all trace contexts.
 *
 * Per specs/01c_context_flow_and_op_wrappers.md:
 * - Methods are defined ONCE on a shared prototype
 * - Object.create() is used for inheritance (no object spreads)
 * - Properties are assigned directly for stable hidden classes
 *
 * This is used internally and should not be accessed directly.
 */
export interface TraceContextBase {
  /** Marker for prototype chain detection */
  readonly [TRACE_CONTEXT_MARKER]: true;
  /** Trace ID for this context */
  trace_id: TraceId;
  /** Epoch microseconds when trace started */
  anchorEpochMicros: number;
  /** performance.now() when trace started */
  anchorPerfNow: number;
  /** Feature flag evaluator */
  ff: unknown;
  /** Root span function */
  span: unknown;
  /** Pre-computed Extra keys for V8 optimization (avoid Object.keys in hot path) */
  _extraKeys?: string[];
}

/**
 * The shared prototype object for TraceContext.
 *
 * Per V8 optimization guidelines:
 * - Define methods ONCE on prototype, not per-instance
 * - Use Object.create() for inheritance chains
 * - Avoid object spreads which break hidden classes
 */
export const TraceContextProto: TraceContextBase = {
  [TRACE_CONTEXT_MARKER]: true,
  trace_id: undefined as unknown as TraceId,
  anchorEpochMicros: undefined as unknown as number,
  anchorPerfNow: undefined as unknown as number,
  ff: undefined,
  span: undefined,
  _extraKeys: undefined,
};

// =============================================================================
// Type Guard
// =============================================================================

/**
 * Type guard to check if a value is a TraceContext
 */
export function isTraceContext(value: unknown): value is TraceContext<FeatureFlagSchema, Record<string, unknown>> {
  return (
    typeof value === 'object' &&
    value !== null &&
    TRACE_CONTEXT_MARKER in value &&
    (value as Record<symbol, unknown>)[TRACE_CONTEXT_MARKER] === true
  );
}
