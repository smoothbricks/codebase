import type { Nanoseconds } from '@smoothbricks/arrow-builder';

/**
 * Browser/ES timestamp implementation.
 *
 * Uses per-trace anchored timestamps for consistent trace-wide time reference.
 * Combines epoch anchor with performance.now() delta for microsecond precision (~5-20μs).
 * Last 3 digits of nanoseconds are always 000.
 *
 * For Node.js with true nanosecond precision, import from `@smoothbricks/lmao/node`.
 */
export function getTimestampNanos(anchorEpochNanos: bigint, anchorPerfNow: number): Nanoseconds {
  // Calculate elapsed time since anchor using high-resolution timer
  const elapsedMs = performance.now() - anchorPerfNow;
  // Convert to nanoseconds (last 3 digits = 000 due to microsecond precision)
  const elapsedNanos = BigInt(Math.floor(elapsedMs * 1000)) * 1000n;
  // Add to epoch anchor for absolute time
  return (anchorEpochNanos + elapsedNanos) as Nanoseconds;
}

/**
 * Create anchor values for a new trace.
 * Call this once at trace creation, then pass the anchors to getTimestampNanos.
 */
export function createTimestampAnchor(): { anchorEpochNanos: bigint; anchorPerfNow: number } {
  return {
    anchorEpochNanos: BigInt(Date.now()) * 1_000_000n,
    anchorPerfNow: performance.now(),
  };
}
