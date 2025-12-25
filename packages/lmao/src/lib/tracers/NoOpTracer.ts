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
 * const ctx = defineOpContext({ logSchema });
 * const { trace } = new NoOpTracer(ctx);
 *
 * // Trace executes, result returned, no side effects
 * const result = await trace('fetch', fetchOp);
 * ```
 */

import type { OpContextBinding } from '../opContext/types.js';
import { Tracer } from '../tracer.js';
import type { SpanBuffer } from '../types.js';

/**
 * No-op tracer - executes ops normally but discards all trace data
 *
 * All lifecycle hooks are empty - no processing, no output.
 * Useful for testing and measuring op execution cost without trace overhead.
 *
 * @typeParam B - OpContextBinding type
 */
export class NoOpTracer<B extends OpContextBinding = OpContextBinding> extends Tracer<B> {
  /**
   * No-op hook for trace start.
   */
  onTraceStart(_rootBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    // No-op
  }

  /**
   * No-op hook for trace end.
   */
  onTraceEnd(_rootBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    // No-op
  }

  /**
   * No-op hook for span start.
   */
  onSpanStart(_childBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    // No-op
  }

  /**
   * No-op hook for span end.
   */
  onSpanEnd(_childBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    // No-op
  }

  /**
   * No-op hook for stats reset.
   */
  onStatsWillResetFor(_buffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    // No-op
  }
}
