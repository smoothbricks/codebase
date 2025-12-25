import { Nanoseconds } from '@smoothbricks/arrow-builder';

/**
 * Node.js timestamp implementation.
 *
 * Uses per-trace anchored timestamps for consistent trace-wide time reference.
 * Combines epoch anchor with process.hrtime.bigint() delta for true nanosecond precision.
 *
 * **How it works:**
 * - At trace creation: capture `Date.now()` and `process.hrtime.bigint()` together
 * - For each timestamp: add hrtime delta to the epoch anchor
 * - Result: absolute epoch time with nanosecond precision
 *
 * **Why per-trace anchoring:**
 * - Each trace has fresh anchor - no long-running drift issues
 * - NTP corrections between traces are isolated
 * - Trace is self-contained unit with consistent time reference
 */

/**
 * Get current timestamp in nanoseconds since Unix epoch.
 * True nanosecond precision via `process.hrtime.bigint()`.
 *
 * @param anchorEpochNanos - Epoch time in nanoseconds when trace was created
 * @param anchorPerfNow - Anchor value from TraceRoot (process.hrtime.bigint() converted to number)
 */
export function getTimestampNanos(anchorEpochNanos: bigint, anchorPerfNow: number): Nanoseconds {
  // anchorPerfNow is Number(process.hrtime.bigint()) from trace creation
  // Convert back to bigint (safe - no precision loss for the conversion itself)
  const anchorHrtime = BigInt(Math.round(anchorPerfNow));
  const currentHrtime = process.hrtime.bigint();
  const elapsedNanos = currentHrtime - anchorHrtime;
  return Nanoseconds.unsafe(anchorEpochNanos + elapsedNanos);
}

/**
 * Create anchor values for a new trace.
 * Call this once at trace creation, then pass the anchors to getTimestampNanos.
 *
 * Uses process.hrtime.bigint() for true nanosecond precision.
 */
export function createTimestampAnchor(): { anchorEpochNanos: bigint; anchorPerfNow: number } {
  return {
    anchorEpochNanos: BigInt(Date.now()) * 1_000_000n,
    // Store as number for consistency with browser API (performance.now() returns number)
    anchorPerfNow: Number(process.hrtime.bigint()),
  };
}
