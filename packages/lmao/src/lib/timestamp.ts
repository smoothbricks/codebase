/**
 * High-precision timestamp system (Generic/Browser implementation)
 *
 * Provides microsecond-precision timestamps using `performance.now()` for browsers
 * and generic JavaScript environments. For Node.js with nanosecond precision,
 * import from `'@smoothbricks/lmao/node'` instead.
 *
 * **Why anchoring?**
 * - `Date.now()` has millisecond precision and can drift with system clock adjustments
 * - `performance.now()` has sub-millisecond precision but measures relative time
 * - By anchoring once per request, we get both absolute epoch time AND high precision
 *
 * **How it works:**
 * 1. At request start: Capture `Date.now()` (absolute) and `performance.now()` (precise)
 * 2. For each timestamp: Calculate delta from `performance.now()` anchor
 * 3. Add delta to epoch anchor for high-precision absolute timestamp
 *
 * @module timestamp
 *
 * @example
 * ```typescript
 * // At request start (once)
 * const { anchorEpochMicros, anchorPerfNow } = createTimeAnchor();
 *
 * // Later, get high-precision timestamps
 * const timestamp1 = getTimestampMicros(anchorEpochMicros, anchorPerfNow);
 * await doSomeWork();
 * const timestamp2 = getTimestampMicros(anchorEpochMicros, anchorPerfNow);
 *
 * console.log(`Work took ${timestamp2 - timestamp1} microseconds`);
 * ```
 */

/**
 * Creates a time anchor at the trace root (called ONCE per request).
 *
 * This captures both the absolute epoch time and the high-resolution performance
 * counter at the same instant. All subsequent timestamp calculations use these
 * anchor values for efficient delta calculation.
 *
 * **Performance**: Returns flat primitives instead of a nested object for
 * zero-allocation spread when passing to child functions.
 *
 * @returns Object with anchor values for timestamp calculations
 * @returns {number} anchorEpochMicros - Epoch time in microseconds when anchor was created
 * @returns {number} anchorPerfNow - High-precision performance.now() value in microseconds
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
  const perfNowMicros = performance.now() * 1000; // Convert ms to microseconds

  return {
    anchorEpochMicros: epochMicros,
    anchorPerfNow: perfNowMicros,
  };
}

/**
 * Gets the current timestamp in microseconds since Unix epoch.
 *
 * Uses delta calculation from the anchor point for high precision without
 * repeated system calls. This function is designed for the hot path - it
 * performs ZERO allocations, just arithmetic operations.
 *
 * **Precision**: Sub-millisecond precision (typically 5-20 microseconds depending on browser)
 *
 * @param anchorEpochMicros - Epoch time in microseconds when anchor was created
 * @param anchorPerfNow - High-precision performance.now() value when anchor was created (microseconds)
 * @returns Current timestamp in microseconds since Unix epoch
 *
 * @example
 * ```typescript
 * // Store timestamp in trace buffer
 * buffer.timestamps[idx] = getTimestampMicros(anchorEpochMicros, anchorPerfNow);
 * ```
 */
export function getTimestampMicros(anchorEpochMicros: number, anchorPerfNow: number): number {
  const nowMicros = performance.now() * 1000;
  return anchorEpochMicros + (nowMicros - anchorPerfNow);
}
