/**
 * Nanoseconds branded type for type-safe timestamp handling
 *
 * Prevents accidental unit mixing (milliseconds vs nanoseconds) and provides
 * a centralized location for timestamp conversion utilities.
 *
 * This is a generic buffer concept used for high-precision time-series data.
 *
 * **Benefits:**
 * - Type safety prevents mixing time units
 * - Self-documenting code - units clear from type
 * - Zero runtime overhead on hot path (BigInt operations are fast)
 * - Conversion functions centralized and tested
 * - Works with Arrow's TimestampNanosecond type
 *
 * **Precision:**
 * - BigInt can represent arbitrarily large integers
 * - Nanoseconds since epoch will never overflow
 * - Node.js: Native nanosecond precision via process.hrtime.bigint()
 * - Browser: Microsecond precision via performance.now() (last 3 digits are 000)
 *
 * @module nanoseconds
 *
 * @example
 * ```typescript
 * import { Nanoseconds } from '@smoothbricks/arrow-builder';
 *
 * // Node.js: Native nanoseconds
 * const timestamp = Nanoseconds.now();
 *
 * // Store in buffer (BigInt64Array)
 * buffer.timestamps[idx] = timestamp; // Type-safe!
 * ```
 */

/**
 * Nanoseconds branded type
 *
 * A `bigint` with a compile-time brand that prevents accidental mixing of time units.
 * The brand is erased at runtime, so there is zero overhead.
 */
export type Nanoseconds = bigint & { readonly __brand: 'Nanoseconds' };

/**
 * Utility functions for working with Nanoseconds
 */
export namespace Nanoseconds {
  /**
   * Get current timestamp in nanoseconds since Unix epoch.
   *
   * Uses the most efficient method available:
   * - Browser/generic: performance.timeOrigin + performance.now() (microsecond precision)
   * - Browsers typically provide 5-20 microsecond precision
   *
   * For Node.js with true nanosecond precision, use the /node entry point.
   *
   * **Precision handling:**
   * - Epoch milliseconds (~1.7 trillion) is safe as Number
   * - Multiply by 1000 to get microseconds (~1.7 quadrillion) - still safe
   * - Convert to BigInt, then multiply by 1000n for nanoseconds
   *
   * @returns Current timestamp in nanoseconds since Unix epoch
   *
   * @example
   * ```typescript
   * buffer.timestamps[idx] = Nanoseconds.now();
   * ```
   */
  export function now(): Nanoseconds {
    // performance.timeOrigin + performance.now() gives epoch time in milliseconds
    // with sub-millisecond precision (e.g., 1702789123456.789)
    // Convert to microseconds first (safe as Number), then to nanoseconds as BigInt
    const epochMicros = Math.round((performance.timeOrigin + performance.now()) * 1000);
    return (BigInt(epochMicros) * 1000n) as Nanoseconds;
  }

  /**
   * Convert milliseconds to nanoseconds.
   *
   * @param millis - Time in milliseconds (e.g., from Date.now())
   * @returns Time in nanoseconds
   *
   * @example
   * ```typescript
   * const epochNanos = Nanoseconds.fromMillis(Date.now());
   * ```
   */
  export function fromMillis(millis: number): Nanoseconds {
    return (BigInt(Math.trunc(millis)) * 1_000_000n) as Nanoseconds;
  }

  /**
   * Cast a raw bigint to Nanoseconds (use with caution).
   *
   * **Warning:** This bypasses type safety. Only use when you're certain
   * the value is already in nanoseconds.
   *
   * @param value - Raw bigint value in nanoseconds
   * @returns Branded Nanoseconds value
   */
  export function unsafe(value: bigint): Nanoseconds {
    return value as Nanoseconds;
  }

  /**
   * Convert nanoseconds to milliseconds.
   *
   * @param nanos - Time in nanoseconds
   * @returns Time in milliseconds (as number)
   */
  export function toMillis(nanos: Nanoseconds): number {
    return Number(nanos / 1_000_000n);
  }

  /**
   * Convert nanoseconds to microseconds.
   *
   * @param nanos - Time in nanoseconds
   * @returns Time in microseconds (as bigint)
   */
  export function toMicros(nanos: Nanoseconds): bigint {
    return nanos / 1_000n;
  }
}
