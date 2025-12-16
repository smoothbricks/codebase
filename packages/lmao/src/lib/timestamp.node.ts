import { Nanoseconds } from '@smoothbricks/arrow-builder';

/**
 * High-precision timestamp system (Node.js implementation)
 *
 * Provides true nanosecond-precision timestamps using `process.hrtime.bigint()`
 * anchored to epoch time. This is the most precise timestamp available on Node.js.
 *
 * **How it works:**
 * - On module load: Capture `Date.now()` and `process.hrtime.bigint()` together
 * - For each timestamp: Use hrtime delta from anchor point
 * - Add delta to epoch anchor for absolute time with nanosecond precision
 *
 * **Why anchor once?**
 * - `Date.now()` has millisecond precision and can drift with NTP adjustments
 * - `process.hrtime.bigint()` has nanosecond precision but measures monotonic time
 * - By anchoring once, we get both absolute epoch time AND nanosecond precision
 *
 * @module timestamp.node
 *
 * @example
 * ```typescript
 * import { getTimestampNanos } from '@smoothbricks/lmao/node';
 *
 * // Get current timestamp in nanoseconds
 * buffer.timestamps[idx] = getTimestampNanos();
 * ```
 */

// Anchor point: captured once at module load
// This gives us the relationship between epoch time and hrtime
const anchorEpochNanos = BigInt(Date.now()) * 1_000_000n;
const anchorHrtime = process.hrtime.bigint();

/**
 * Gets the current timestamp in nanoseconds since Unix epoch.
 *
 * Uses `process.hrtime.bigint()` delta from a module-load anchor point.
 * This provides true nanosecond precision on Node.js.
 *
 * **Performance**: Single BigInt subtraction + addition, minimal overhead.
 *
 * @returns Current timestamp in nanoseconds since Unix epoch
 *
 * @example
 * ```typescript
 * // Store timestamp in trace buffer
 * buffer.timestamps[idx] = getTimestampNanos();
 * ```
 */
export function getTimestampNanos(): Nanoseconds {
  const hrtimeDelta = process.hrtime.bigint() - anchorHrtime;
  return Nanoseconds.unsafe(anchorEpochNanos + hrtimeDelta);
}

// Re-export Nanoseconds for convenience
export { Nanoseconds };
