import type { OpContextBinding } from '../opContext/types.js';
import { Tracer } from '../tracer.js';
import type { SpanBuffer } from '../types.js';

/**
 * Tracer that batches completed root traces in an in-memory queue.
 *
 * Use `drain()` to consume all queued buffers and clear the queue.
 * Designed for production scenarios where traces should be batched
 * before sending to a backend.
 *
 * @example
 * ```typescript
 * import { createTraceRoot } from '@smoothbricks/lmao/node';
 * import { JsBufferStrategy } from '@smoothbricks/lmao';
 *
 * const ctx = defineOpContext({ logSchema });
 * const tracer = new ArrayQueueTracer(ctx, {
 *   bufferStrategy: new JsBufferStrategy(),
 *   createTraceRoot,
 * });
 * const { trace } = tracer;
 *
 * // Process requests, traces accumulate
 * await trace('request-1', handleRequest, req1);
 * await trace('request-2', handleRequest, req2);
 *
 * // Periodically drain and process
 * const batch = tracer.drain();
 * for (const buf of batch) {
 *   const table = await tracer.bufferStrategy.toArrowTable(buf);
 *   await sendToBackend(table);
 *   tracer.bufferStrategy.releaseBuffer(buf);
 * }
 * ```
 */
export class ArrayQueueTracer<B extends OpContextBinding = OpContextBinding> extends Tracer<B> {
  /**
   * Queue of completed root trace buffers.
   * Use `drain()` to consume and clear.
   * Public for test inspection.
   */
  readonly queue: SpanBuffer<B['logBinding']['logSchema']>[] = [];

  onTraceStart(_rootBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    // No-op
  }

  onTraceEnd(rootBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    this.queue.push(rootBuffer);
  }

  onSpanStart(_childBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    // No-op
  }

  onSpanEnd(_childBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    // No-op
  }

  onStatsWillResetFor(_buffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    // No-op - override in subclass to capture stats
  }

  /**
   * Consume all queued buffers and clear the queue.
   * Returns an independent copy of the queue contents.
   *
   * @returns Array of root buffers (with child spans in _children tree)
   */
  drain(): SpanBuffer<B['logBinding']['logSchema']>[] {
    const buffers = [...this.queue];
    this.queue.length = 0;
    return buffers;
  }
}
