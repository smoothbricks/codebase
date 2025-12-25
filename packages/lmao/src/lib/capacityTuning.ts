/**
 * Capacity Tuning - Self-tuning buffer capacity based on usage patterns
 *
 * Per specs/01b2_buffer_self_tuning.md:
 * - Track overflow ratio (overflowWrites / totalWrites)
 * - Increase capacity if >15% writes overflow
 * - Decrease capacity if <5% writes overflow with many buffers
 * - Maintain power-of-2 capacities between 8 and 1024
 *
 * ## Architecture Note
 *
 * Stats are on SpanBufferClass.stats (static property), NOT on LogBinding.
 * This reduces per-instance memory and avoids sync bugs from having two stat objects.
 * See agent-todo/opgroup-refactor.md lines 58-70, 525-547 for details.
 */

import type { SpanBufferStats } from './spanBufferStats.js';

/**
 * Track overflow and check if capacity should be tuned.
 *
 * Called when a buffer overflows and needs to chain to a new buffer.
 * Updates overflow stats and triggers capacity tuning if thresholds are met.
 *
 * @param stats - SpanBufferStats from SpanBufferClass.stats
 * @internal
 */
export function trackOverflowAndTune(stats: SpanBufferStats): void {
  stats.overflowWrites++;
  stats.overflows++;
  shouldTuneCapacity(stats);
}

/**
 * Reset stats after capacity tuning.
 *
 * Called after a tuning decision to start fresh measurement window.
 * This ensures the tuning algorithm responds to recent behavior, not cumulative history.
 *
 * Note: In the future, Tracer.onStatsWillResetFor() will be called BEFORE this reset
 * to capture stats for observability. See agent-todo/tracer-architecture-refactor.plan.md.
 */
function resetStats(stats: SpanBufferStats): void {
  stats.totalWrites = 0;
  stats.overflowWrites = 0;
  stats.totalCreated = 0;
}

/**
 * Check if capacity should be tuned based on usage patterns.
 *
 * Algorithm:
 * - Requires minimum 100 samples before tuning
 * - Increase capacity (×2, max 1024) if overflow ratio > 15%
 * - Decrease capacity (÷2, min 8) if overflow ratio < 5% AND totalCreated >= 10
 * - Reset stats after tuning to start fresh measurement window
 *
 * @param stats - SpanBufferStats from SpanBufferClass.stats
 * @internal Exported for testing
 */
export function shouldTuneCapacity(stats: SpanBufferStats): void {
  const minSamples = 100;
  if (stats.totalWrites < minSamples) return;

  const overflowRatio = stats.overflowWrites / stats.totalWrites;

  // Increase if >15% writes overflow
  if (overflowRatio > 0.15 && stats.capacity < 1024) {
    stats.capacity = Math.min(stats.capacity * 2, 1024);
    resetStats(stats);
    return;
  }

  // Decrease if <5% writes overflow and we have many buffers
  if (overflowRatio < 0.05 && stats.totalCreated >= 10 && stats.capacity > 8) {
    stats.capacity = Math.max(8, stats.capacity / 2);
    resetStats(stats);
  }
}
