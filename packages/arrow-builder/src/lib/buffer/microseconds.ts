/**
 * Microseconds branded type for type-safe timestamp handling
 *
 * Prevents accidental unit mixing (milliseconds vs microseconds) and provides
 * a centralized location for timestamp conversion utilities.
 *
 * This is a generic buffer concept used for high-precision time-series data.
 *
 * **Benefits:**
 * - Type safety prevents mixing milliseconds and microseconds
 * - Self-documenting code - units clear from type
 * - Zero runtime overhead (erased at compile time)
 * - Conversion functions centralized and tested
 *
 * **Precision Guarantees:**
 * - JavaScript Number is IEEE 754 double (53-bit mantissa)
 * - Can represent integers up to 2^53 - 1 (9,007,199,254,740,991)
 * - Microseconds since epoch (Jan 1 1970) will exceed this in year 2255
 * - For current use (2020-2100), microseconds are SAFE
 *
 * @module microseconds
 *
 * @example
 * ```typescript
 * import { Microseconds } from '@smoothbricks/arrow-builder';
 *
 * // Convert from milliseconds
 * const timestamp = Microseconds.fromMillis(Date.now());
 *
 * // Convert from nanoseconds (Node.js)
 * const precise = Microseconds.fromNanos(process.hrtime.bigint());
 *
 * // Store in buffer
 * buffer.timestamp[idx] = timestamp; // Type-safe!
 * buffer.timestamp[idx] = Date.now(); // ❌ Type error!
 * ```
 */

/**
 * Microseconds branded type
 *
 * A `number` with a compile-time brand that prevents accidental mixing of time units.
 * The brand is erased at runtime, so there is zero overhead.
 */
declare const microsecondsBrand: unique symbol;

export type Microseconds = number & { readonly [microsecondsBrand]: 'Microseconds' };

function toMicroseconds(value: number): Microseconds;
function toMicroseconds(value: number): number {
  return value;
}

/**
 * Utility functions for working with Microseconds
 */
export namespace Microseconds {
  /**
   * Convert milliseconds to microseconds.
   *
   * **Precision:** Milliseconds have 3 decimal places, microseconds have 6.
   * Conversion multiplies by 1000, preserving all precision.
   *
   * @param millis - Time in milliseconds (e.g., from Date.now())
   * @returns Time in microseconds
   *
   * @example
   * ```typescript
   * const epochMicros = Microseconds.fromMillis(Date.now());
   * ```
   */
  export function fromMillis(millis: number): Microseconds {
    return toMicroseconds(millis * 1000);
  }

  /**
   * Convert nanoseconds (BigInt) to microseconds.
   *
   * **Precision:** Nanoseconds have 9 decimal places, microseconds have 6.
   * Conversion divides by 1000, truncating the last 3 digits (sub-microsecond precision).
   *
   * **Safety:** The division happens in BigInt space before conversion to Number,
   * ensuring we don't overflow JavaScript's Number precision limits.
   *
   * @param nanos - Time in nanoseconds (e.g., from process.hrtime.bigint())
   * @returns Time in microseconds
   *
   * @example
   * ```typescript
   * const hrtimeMicros = Microseconds.fromNanos(process.hrtime.bigint());
   * ```
   */
  export function fromNanos(nanos: bigint): Microseconds {
    return toMicroseconds(Number(nanos / 1000n));
  }

  /**
   * Cast a raw number to Microseconds (use with caution).
   *
   * **Warning:** This bypasses type safety. Only use when you're certain
   * the value is already in microseconds (e.g., from arithmetic on existing
   * Microseconds values).
   *
   * @param value - Raw number value in microseconds
   * @returns Branded Microseconds value
   *
   * @example
   * ```typescript
   * // Calculate delta between two Microseconds values
   * const start = Microseconds.fromMillis(Date.now());
   * const end = Microseconds.fromMillis(Date.now());
   * const delta = Microseconds.unsafe(end - start); // Already in microseconds
   * ```
   */
  export function unsafe(value: number): Microseconds {
    return toMicroseconds(value);
  }

  /**
   * Convert microseconds to milliseconds.
   *
   * **Precision:** Microseconds have 6 decimal places, milliseconds have 3.
   * Conversion divides by 1000, truncating sub-millisecond precision.
   *
   * @param micros - Time in microseconds
   * @returns Time in milliseconds
   *
   * @example
   * ```typescript
   * const timestamp = Microseconds.fromMillis(Date.now());
   * const asMillis = Microseconds.toMillis(timestamp);
   * ```
   */
  export function toMillis(micros: Microseconds): number {
    return micros / 1000;
  }

  /**
   * Get current timestamp in microseconds using anchored approach.
   *
   * **Why anchoring?**
   * - `Date.now()` has millisecond precision and can drift with system clock adjustments
   * - `performance.now()` has sub-millisecond precision but measures relative time
   * - By anchoring once, we get both absolute epoch time AND high precision
   *
   * **How it works:**
   * 1. At request start: Capture epoch anchor and performance anchor
   * 2. For each timestamp: Calculate delta from performance anchor
   * 3. Add delta to epoch anchor for high-precision absolute timestamp
   *
   * @param anchorEpochMicros - Epoch anchor in microseconds (from createTimeAnchor)
   * @param anchorPerfNow - Performance anchor in microseconds (from createTimeAnchor)
   * @param perfNowMicros - Current performance.now() * 1000
   * @returns Current timestamp in microseconds since Unix epoch
   *
   * @example
   * ```typescript
   * // In browser/generic environment
   * const anchor = { epoch: Microseconds.fromMillis(Date.now()), perf: Microseconds.fromMillis(performance.now()) };
   * const timestamp = Microseconds.now(anchor.epoch, anchor.perf, Microseconds.fromMillis(performance.now()));
   * ```
   */
  export function now(
    anchorEpochMicros: Microseconds,
    anchorPerfNow: Microseconds,
    perfNowMicros: Microseconds,
  ): Microseconds {
    return unsafe(anchorEpochMicros + (perfNowMicros - anchorPerfNow));
  }
}
