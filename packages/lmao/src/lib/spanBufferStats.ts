/**
 * SpanBufferStats - Self-tuning buffer statistics
 *
 * Mutable stats shared across all SpanBuffer instances from the same defineOpContext.
 * Stored as a static property on the generated SpanBuffer class for optimal memory usage.
 *
 * Per specs/01b2_buffer_self_tuning.md:
 * - Capacity grows when overflow ratio > 15%
 * - Capacity shrinks when overflow ratio < 5% with enough buffers
 * - Stats reset after each adjustment to measure new capacity effectiveness
 *
 * ## Key Architectural Decision
 *
 * Stats are NO LONGER on LogBinding. They are static properties on the generated
 * SpanBuffer class. This reduces per-instance memory (schema stored once per class,
 * stats shared across all instances from same defineOpContext).
 *
 * **Property names have NO `sb_` prefix** (clean names for new architecture).
 * The `sb_` prefix was only needed when these were on LogBinding to avoid conflicts.
 *
 * @module spanBufferStats
 */

/**
 * SpanBufferStats interface - mutable self-tuning statistics
 *
 * Created once per `defineOpContext`, stored as static property on generated
 * SpanBuffer class. All buffers from same context share these stats.
 */
export interface SpanBufferStats {
  /** Current buffer capacity for new buffers (grows/shrinks based on overflow ratio) */
  capacity: number;

  /** Total entries written across all buffers */
  totalWrites: number;

  /** How many times an overflow occurred (buffer count = 1 + overflows) */
  overflowWrites: number;

  /** Total buffers created (root + children + chains) */
  totalCreated: number;

  /** Number of overflow events (incremented when buffer fills) */
  overflows: number;
}
