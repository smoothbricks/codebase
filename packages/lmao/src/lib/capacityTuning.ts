/**
 * Capacity Tuning - Self-tuning buffer capacity based on utilization
 *
 * Per specs/lmao/01b2_buffer_self_tuning.md:
 * - Track row utilization = totalWrites / (spansCreated * usableRowsPerSpan)
 * - Increase capacity if utilization > 150% (regularly overflowing)
 * - Decrease capacity if utilization < 50% (wasting space)
 * - Maintain power-of-2 capacities between 8 and 1024
 *
 * ## Utilization Model
 *
 * - 100% = exactly filling first buffer per span (ideal)
 * - 150% = filling 1.5x first buffer (some spans overflow)
 * - 200% = filling 2x first buffer (every span overflows once)
 * - 50% = only using half the first buffer (wasting space)
 *
 * The 3x gap between shrink (50%) and grow (150%) thresholds prevents flip-flopping:
 * - After grow: utilization halves → lands in stable zone
 * - After shrink: utilization doubles → lands in stable zone
 *
 * ## Architecture Note
 *
 * Stats are on SpanBufferClass.stats (static property), NOT on LogBinding.
 * This reduces per-instance memory and avoids sync bugs from having two stat objects.
 */

import type { SpanBufferStats } from './spanBufferStats.js';

/** Minimum spans needed before tuning decisions */
const MIN_SPANS_FOR_TUNING = 10;

/** Grow capacity when utilization exceeds this (150% = regularly overflowing) */
const GROW_THRESHOLD = 1.5;

/** Shrink capacity when utilization falls below this (50% = wasting space) */
const SHRINK_THRESHOLD = 0.5;

/** Minimum capacity (must be power of 2, >= 8 for alignment) */
const MIN_CAPACITY = 8;

/** Maximum capacity (must be power of 2) */
const MAX_CAPACITY = 1024;

/**
 * Check if capacity should be tuned after an overflow event.
 *
 * Called when a buffer overflows and needs to chain to a new buffer.
 *
 * @param stats - SpanBufferStats from SpanBufferClass.stats
 * @internal
 */
export function checkCapacityTuning(stats: SpanBufferStats): void {
  shouldTuneCapacity(stats);
}

/**
 * Reset stats after capacity tuning.
 *
 * Called after a tuning decision to start fresh measurement window.
 * This ensures the tuning algorithm responds to recent behavior, not cumulative history.
 *
 * Note: Tracer.onStatsWillResetFor() is called BEFORE this reset (in spanLoggerGenerator's
 * _getNextBuffer method) to capture stats for observability before they're lost.
 */
function resetStats(stats: SpanBufferStats): void {
  stats.totalWrites = 0;
  stats.spansCreated = 0;
}

/**
 * Check if capacity should be tuned based on utilization.
 *
 * Algorithm:
 * - Requires minimum 10 spans before tuning
 * - Compute utilization = totalWrites / (spansCreated * usableRowsPerSpan)
 * - Increase capacity (×2, max 1024) if utilization > 150%
 * - Decrease capacity (÷2, min 8) if utilization < 50%
 * - Reset stats after tuning to start fresh measurement window
 *
 * @param stats - SpanBufferStats from SpanBufferClass.stats
 * @internal Exported for testing
 */
export function shouldTuneCapacity(stats: SpanBufferStats): void {
  if (stats.spansCreated < MIN_SPANS_FOR_TUNING) return;

  // Usable rows per span = capacity - 2 (rows 0-1 reserved for span-start/end)
  const usableRowsPerSpan = stats.capacity - 2;
  const utilization = stats.totalWrites / (stats.spansCreated * usableRowsPerSpan);

  // Grow if utilization > 150% (regularly overflowing)
  if (utilization > GROW_THRESHOLD && stats.capacity < MAX_CAPACITY) {
    stats.capacity = Math.min(stats.capacity * 2, MAX_CAPACITY);
    resetStats(stats);
    return;
  }

  // Shrink if utilization < 50% (wasting space)
  if (utilization < SHRINK_THRESHOLD && stats.capacity > MIN_CAPACITY) {
    stats.capacity = Math.max(MIN_CAPACITY, stats.capacity / 2);
    resetStats(stats);
  }
}
