/**
 * Capacity Tuning - Self-tuning buffer capacity based on usage patterns
 *
 * Per specs/01b2_buffer_self_tuning.md:
 * - Track overflow ratio (overflowWrites / totalWrites)
 * - Increase capacity if >15% writes overflow
 * - Decrease capacity if <5% writes overflow with many buffers
 * - Maintain power-of-2 capacities between 8 and 1024
 */

import type { LogBinding } from './logBinding.js';

/**
 * Track overflow and check if capacity should be tuned.
 *
 * Called when a buffer overflows and needs to chain to a new buffer.
 * Updates overflow stats and triggers capacity tuning if thresholds are met.
 *
 * @param logBinding - LogBinding with sb_* capacity statistics
 * @internal
 */
export function trackOverflowAndTune(logBinding: LogBinding): void {
  logBinding.sb_overflowWrites++;
  logBinding.sb_overflows++;
  shouldTuneCapacity(logBinding);
}

/**
 * Check if capacity should be tuned based on usage patterns and update stats if needed.
 *
 * Algorithm:
 * - Requires minimum 100 samples before tuning
 * - Increase capacity (×2, max 1024) if overflow ratio > 15%
 * - Decrease capacity (÷2, min 8) if overflow ratio < 5% AND totalCreated >= 10
 * - Reset stats after tuning to start fresh measurement period
 *
 * @param logBinding - LogBinding with sb_* capacity statistics
 * @internal Exported for testing
 */
export function shouldTuneCapacity(logBinding: LogBinding): void {
  const minSamples = 100;
  if (logBinding.sb_totalWrites < minSamples) return;

  const overflowRatio = logBinding.sb_overflowWrites / logBinding.sb_totalWrites;

  // Increase if >15% writes overflow
  if (overflowRatio > 0.15 && logBinding.sb_capacity < 1024) {
    logBinding.sb_capacity = Math.min(logBinding.sb_capacity * 2, 1024);
    logBinding.sb_totalWrites = 0;
    logBinding.sb_overflowWrites = 0;
    logBinding.sb_totalCreated = 0;
    return;
  }

  // Decrease if <5% writes overflow and we have many buffers
  if (overflowRatio < 0.05 && logBinding.sb_totalCreated >= 10 && logBinding.sb_capacity > 8) {
    logBinding.sb_capacity = Math.max(8, logBinding.sb_capacity / 2);
    logBinding.sb_totalWrites = 0;
    logBinding.sb_overflowWrites = 0;
    logBinding.sb_totalCreated = 0;
  }
}
