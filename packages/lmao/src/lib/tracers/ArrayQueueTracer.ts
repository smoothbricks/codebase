import type { OpContext } from '../opContext/types.js';
import { Tracer } from '../tracer.js';
import type { AnySpanBuffer } from '../types.js';

/**
 * Tracer that batches completed root traces in an in-memory queue.
 *
 * Use `drain()` to consume all queued buffers and clear the queue.
 * Designed for production scenarios where traces should be batched
 * before sending to a backend.
 *
 * @example
 * ```typescript
 * const tracer = new ArrayQueueTracer({ logBinding });
 * const { trace } = tracer;
 *
 * // Process requests, traces accumulate
 * await trace('request-1', handleRequest, req1);
 * await trace('request-2', handleRequest, req2);
 *
 * // Periodically drain and process
 * const batch = tracer.drain();
 * for (const buf of batch) {
 *   const table = convertSpanTreeToArrowTable(buf);
 *   await sendToBackend(table);
 * }
 * ```
 */
export class ArrayQueueTracer<Ctx extends OpContext = OpContext> extends Tracer<Ctx> {
  /**
   * Queue of completed root trace buffers.
   * Use `drain()` to consume and clear.
   */
  readonly queue: AnySpanBuffer[] = [];

  onTraceStart(_rootBuffer: AnySpanBuffer): void {
    // No-op
  }

  onTraceEnd(rootBuffer: AnySpanBuffer): void {
    this.queue.push(rootBuffer);
  }

  onSpanStart(_childBuffer: AnySpanBuffer): void {
    // No-op
  }

  onSpanEnd(_childBuffer: AnySpanBuffer): void {
    // No-op
  }

  onStatsWillResetFor(_buffer: AnySpanBuffer): void {
    // No-op - override in subclass to capture stats
  }

  /**
   * Consume all queued buffers and clear the queue.
   * Returns an independent copy of the queue contents.
   *
   * @returns Array of root buffers (with child spans in _children tree)
   */
  drain(): AnySpanBuffer[] {
    const buffers = [...this.queue];
    this.queue.length = 0;
    return buffers;
  }
}
