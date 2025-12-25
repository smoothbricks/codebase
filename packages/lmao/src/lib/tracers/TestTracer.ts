/**
 * TestTracer - A tracer that accumulates completed root trace buffers for test inspection.
 *
 * Child spans are accessible via the buffer's `_children` tree - no need to track them separately.
 * This is the primary tracer for tests that need to inspect trace output, convert to Arrow tables,
 * or verify buffer contents.
 *
 * @module tracers/TestTracer
 */

import { Tracer, type TracerConfig } from '../tracer.js';
import type { AnySpanBuffer, SpanBuffer } from '../types.js';

/**
 * TestTracer configuration - same as base TracerConfig but without sink
 *
 * @typeParam T - LogSchema type
 * @typeParam FF - Feature flag schema (defaults to empty)
 * @typeParam Deps - Dependencies config (defaults to empty)
 * @typeParam UserCtx - User context properties (defaults to empty)
 */
export type TestTracerConfig<Ctx extends import('../opContext/types.js').OpContext> = Omit<TracerConfig<Ctx>, 'sink'>;

/**
 * Test tracer that accumulates root buffers for inspection.
 *
 * Collects all completed root trace buffers in `rootBuffers` array.
 * Child spans are accessible via the buffer's `_children` tree -
 * no need to track them separately.
 *
 * @example
 * ```typescript
 * const tracer = new TestTracer({ logBinding });
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
 * ```
 */
export class TestTracer<Ctx extends import('../opContext/types.js').OpContext> extends Tracer<Ctx> {
  /**
   * All completed root trace buffers.
   * Child spans are accessible via each buffer's `_children` tree.
   */
  readonly rootBuffers: AnySpanBuffer[] = [];

  constructor(config: TestTracerConfig<Ctx>) {
    super(config);
  }

  // ===========================================================================
  // Lifecycle hook implementations
  // ===========================================================================

  protected onTraceStart(_rootBuffer: SpanBuffer<Ctx['logSchema']>): void {
    // No-op - we collect on end, not start
  }

  protected onTraceEnd(rootBuffer: SpanBuffer<Ctx['logSchema']>): void {
    // Collect the root buffer when trace completes
    this.rootBuffers.push(rootBuffer);
  }

  protected onSpanStart(_childBuffer: SpanBuffer<Ctx['logSchema']>): void {
    // No-op - children are in tree, accessed via rootBuffer._children
  }

  protected onSpanEnd(_childBuffer: SpanBuffer<Ctx['logSchema']>): void {
    // No-op - children are in tree, accessed via rootBuffer._children
  }

  // ===========================================================================
  // Test utilities
  // ===========================================================================

  /**
   * Clear all collected buffers.
   * Useful for cleanup between tests.
   */
  clear(): void {
    this.rootBuffers.length = 0;
  }
}
