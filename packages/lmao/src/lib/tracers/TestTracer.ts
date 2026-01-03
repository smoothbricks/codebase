/**
 * TestTracer - A tracer that accumulates completed root trace buffers for test inspection.
 *
 * Child spans are accessible via the buffer's `_children` tree - no need to track them separately.
 * This is the primary tracer for tests that need to inspect trace output, convert to Arrow tables,
 * or verify buffer contents.
 *
 * @module tracers/TestTracer
 */

import type { OpContextBinding } from '../opContext/types.js';
import type { LogSchema } from '../schema/LogSchema.js';
import type { TracerOptions } from '../tracer.js';
import { Tracer } from '../tracer.js';
import type { SpanBuffer } from '../types.js';

/**
 * Snapshot of buffer stats captured before reset during capacity tuning.
 */
export interface StatsSnapshot<T extends LogSchema = LogSchema> {
  buffer: SpanBuffer<T>;
  totalWrites: number;
  overflowWrites: number;
  totalCreated: number;
  capacity: number;
}

/**
 * Test tracer that accumulates root buffers for inspection.
 *
 * Collects all completed root trace buffers in `rootBuffers` array.
 * Child spans are accessible via the buffer's `_children` tree -
 * no need to track them separately.
 *
 * Also tracks capacity tuning events via `statsSnapshots` to verify
 * that `onStatsWillResetFor` is called before stats are reset.
 *
 * @example
 * ```typescript
 * const ctx = defineOpContext({ logSchema });
 * const tracer = new TestTracer(ctx);
 * const { trace } = tracer;
 *
 * await trace('my-op', myOp);
 *
 * // Inspect buffer
 * expect(tracer.rootBuffers).toHaveLength(1);
 * expect(tracer.rootBuffers[0]._spanName).toBe('my-op');
 *
 * // Convert to Arrow for detailed inspection
 * const table = convertSpanTreeToArrowTable(tracer.rootBuffers[0]);
 * expect(table.numRows).toBe(2); // span-start + span-ok
 *
 * // Verify capacity tuning was captured
 * expect(tracer.statsSnapshots.length).toBeGreaterThan(0);
 * ```
 */
export class TestTracer<B extends OpContextBinding = OpContextBinding> extends Tracer<B> {
  /**
   * All completed root trace buffers.
   * Child spans are accessible via each buffer's `_children` tree.
   */
  readonly rootBuffers: SpanBuffer<B['logBinding']['logSchema']>[] = [];

  /**
   * Stats snapshots captured before reset during capacity tuning.
   * Used to verify onStatsWillResetFor hook is called correctly.
   */
  readonly statsSnapshots: StatsSnapshot<B['logBinding']['logSchema']>[] = [];

  constructor(binding: B, options: TracerOptions) {
    super(binding, options);
  }

  // ===========================================================================
  // Lifecycle hook implementations
  // ===========================================================================

  onTraceStart(_rootBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    // No-op - we collect on end, not start
  }

  onTraceEnd(rootBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    // Collect the root buffer when trace completes
    this.rootBuffers.push(rootBuffer);
  }

  onSpanStart(_childBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    // No-op - children are in tree, accessed via rootBuffer._children
  }

  onSpanEnd(_childBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    // No-op - children are in tree, accessed via rootBuffer._children
  }

  onStatsWillResetFor(buffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    // Capture stats snapshot before they're reset
    const stats = buffer._stats;
    this.statsSnapshots.push({
      buffer,
      totalWrites: stats.totalWrites,
      overflowWrites: stats.overflowWrites,
      totalCreated: stats.totalCreated,
      capacity: stats.capacity,
    });
  }

  // ===========================================================================
  // Test utilities
  // ===========================================================================

  /**
   * Clear all collected buffers and stats snapshots.
   * Useful for cleanup between tests.
   */
  clear(): void {
    this.rootBuffers.length = 0;
    this.statsSnapshots.length = 0;
  }
}
