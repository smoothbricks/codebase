/**
 * High-precision timestamp system (Node.js implementation)
 *
 * Provides nanosecond-precision timestamps using `process.hrtime.bigint()` for
 * Node.js environments. For browsers and generic environments, import from
 * `'@smoothbricks/lmao'` instead.
 *
 * **Why use this over the generic implementation?**
 * - Node.js `process.hrtime.bigint()` provides nanosecond precision
 * - Browser `performance.now()` typically provides microsecond precision
 * - For server-side tracing, nanosecond precision captures more detail
 *
 * **How it works:**
 * 1. At request start: Capture `Date.now()` (absolute) and `process.hrtime.bigint()` (precise)
 * 2. For each timestamp: Calculate delta from hrtime anchor (in nanoseconds)
 * 3. Convert to microseconds and add to epoch anchor
 *
 * @module timestamp.node
 *
 * @example
 * ```typescript
 * // Import from node entry point
 * import { createTimeAnchor, getTimestampMicros } from '@smoothbricks/lmao/node';
 *
 * // At request start (once)
 * const { anchorEpochMicros, anchorPerfNow } = createTimeAnchor();
 *
 * // Later, get nanosecond-precision timestamps
 * const timestamp = getTimestampMicros(anchorEpochMicros, anchorPerfNow);
 * ```
 */

/**
 * Creates a time anchor at the trace root (called ONCE per request).
 *
 * This captures both the absolute epoch time and the high-resolution time counter
 * at the same instant using Node.js's nanosecond-precision `process.hrtime.bigint()`.
 *
 * **Performance**: Returns flat primitives instead of a nested object for
 * zero-allocation spread when passing to child functions.
 *
 * @returns Object with anchor values for timestamp calculations
 * @returns {number} anchorEpochMicros - Epoch time in microseconds when anchor was created
 * @returns {number} anchorPerfNow - High-precision hrtime value in microseconds
 *
 * @example
 * ```typescript
 * // In request context creation
 * const { anchorEpochMicros, anchorPerfNow } = createTimeAnchor();
 *
 * // Pass to child contexts via spread
 * const childContext = { ...parentContext, anchorEpochMicros, anchorPerfNow };
 * ```
 */
export function createTimeAnchor(): {
  anchorEpochMicros: number;
  anchorPerfNow: number;
} {
  const epochMicros = Date.now() * 1000;
  const hrtimeNanos = process.hrtime.bigint();
  const perfNowMicros = Number(hrtimeNanos / 1000n); // Convert ns to microseconds

  return {
    anchorEpochMicros: epochMicros,
    anchorPerfNow: perfNowMicros,
  };
}

/**
 * Gets the current timestamp in microseconds since Unix epoch.
 *
 * Uses delta calculation from the anchor point with Node.js nanosecond-precision
 * `process.hrtime.bigint()`. While hrtime provides nanoseconds, the output is
 * microseconds for consistency with Arrow timestamp columns.
 *
 * **Precision**: Nanosecond precision from hrtime, truncated to microseconds
 *
 * @param anchorEpochMicros - Epoch time in microseconds when anchor was created
 * @param anchorPerfNow - High-precision hrtime value when anchor was created (microseconds)
 * @returns Current timestamp in microseconds since Unix epoch
 *
 * @example
 * ```typescript
 * // Store timestamp in trace buffer
 * buffer.timestamps[idx] = getTimestampMicros(anchorEpochMicros, anchorPerfNow);
 * ```
 */
export function getTimestampMicros(anchorEpochMicros: number, anchorPerfNow: number): number {
  const hrtimeNanos = process.hrtime.bigint();
  const nowMicros = Number(hrtimeNanos / 1000n); // Convert ns to microseconds
  return anchorEpochMicros + (nowMicros - anchorPerfNow);
}
