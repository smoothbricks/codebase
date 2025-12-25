/**
 * NoOpTracer - A tracer that executes ops but discards all trace data
 *
 * All lifecycle hooks are no-ops - traces execute normally but produce no output.
 * Useful for:
 * - Tests that don't need to inspect trace output
 * - Disabling tracing in production without code changes
 * - Baseline performance testing (measures op execution cost without tracing overhead)
 *
 * @example
 * ```typescript
 * const { trace } = new NoOpTracer({ logBinding });
 *
 * // Trace executes, result returned, no side effects
 * const result = await trace('fetch', fetchOp);
 * ```
 */

import type { OpContext } from '../opContext/types.js';
import { Tracer } from '../tracer.js';
import type { SpanBuffer } from '../types.js';

/**
 * No-op tracer - executes ops normally but discards all trace data
 *
 * All lifecycle hooks are empty - no processing, no output.
 * Useful for testing and measuring op execution cost without trace overhead.
 *
 * @typeParam Ctx - OpContext type (logSchema, flags, deps, userCtx)
 */
export class NoOpTracer<Ctx extends OpContext> extends Tracer<Ctx> {
  /**
   * No-op hook for trace start.
   */
  onTraceStart(_rootBuffer: SpanBuffer<Ctx['logSchema']>): void {
    // No-op
  }

  /**
   * No-op hook for trace end.
   */
  onTraceEnd(_rootBuffer: SpanBuffer<Ctx['logSchema']>): void {
    // No-op
  }

  /**
   * No-op hook for span start.
   */
  onSpanStart(_childBuffer: SpanBuffer<Ctx['logSchema']>): void {
    // No-op
  }

  /**
   * No-op hook for span end.
   */
  onSpanEnd(_childBuffer: SpanBuffer<Ctx['logSchema']>): void {
    // No-op
  }

  /**
   * No-op hook for stats reset.
   */
  onStatsWillResetFor(_buffer: SpanBuffer<Ctx['logSchema']>): void {
    // No-op
  }
}
