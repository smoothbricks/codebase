import { Nanoseconds } from '@smoothbricks/arrow-builder';

/**
 * High-precision timestamp system (Browser implementation)
 *
 * Provides microsecond-precision timestamps using `performance.timeOrigin + performance.now()`.
 * This is the simplified timestamp API that doesn't require manual anchoring.
 *
 * **How it works:**
 * - `performance.timeOrigin` gives the epoch time (ms) when the page loaded
 * - `performance.now()` gives high-precision time since page load
 * - Combined, they give absolute epoch time with sub-millisecond precision
 *
 * **Precision:**
 * - Browsers typically provide 5-20 microsecond precision
 * - Returned nanoseconds will have 000 in last 3 digits (microsecond precision)
 *
 * @module timestamp
 *
 * @example
 * ```typescript
 * import { getTimestampNanos } from '@smoothbricks/lmao';
 *
 * // Get current timestamp in nanoseconds
 * buffer.timestamps[idx] = getTimestampNanos();
 * ```
 */

/**
 * Gets the current timestamp in nanoseconds since Unix epoch.
 *
 * Uses `performance.timeOrigin + performance.now()` for efficient, high-precision
 * timestamp generation. No anchoring required.
 *
 * **Performance**: Single function call, minimal overhead on hot path.
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
  return Nanoseconds.now();
}

// Re-export Nanoseconds for convenience
export { Nanoseconds };
