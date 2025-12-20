/**
 * Nanoseconds branded type for type-safe timestamp handling.
 *
 * Prevents accidental unit mixing (milliseconds vs nanoseconds) at compile time.
 *
 * ## Precision by Platform
 *
 * | Platform | Precision | Method |
 * |----------|-----------|--------|
 * | Browser  | ~5-20 microseconds | `performance.timeOrigin + performance.now()` |
 * | Node.js  | 1 nanosecond | `process.hrtime.bigint()` anchored to epoch |
 *
 * Browser timestamps have last 3 digits as 000 (microsecond granularity).
 * For Node.js nanosecond precision, import from the `/node` entry point.
 *
 * ## Usage
 *
 * ```typescript
 * // Browser (default)
 * import { Nanoseconds } from '@smoothbricks/arrow-builder';
 *
 * // Node.js (true nanosecond precision)
 * import { Nanoseconds } from '@smoothbricks/lmao/node';
 *
 * buffer.timestamp[idx] = Nanoseconds.now();
 * ```
 *
 * @module nanoseconds
 */

/**
 * Nanoseconds branded type.
 *
 * A `bigint` with a compile-time brand that prevents accidental mixing of time units.
 * The brand is erased at runtime - zero overhead.
 */
export type Nanoseconds = bigint & { readonly __brand: 'Nanoseconds' };

/**
 * Nanoseconds utilities.
 *
 * The `now` method is mutable - importing from `/node` entry point replaces it
 * with the Node.js implementation using `process.hrtime.bigint()`.
 */
export const Nanoseconds = {
  /**
   * Get current timestamp in nanoseconds since Unix epoch.
   *
   * **Must be set by importing a timestamp module.**
   * - Browser: `import '@smoothbricks/lmao'` sets this to use `performance` API
   * - Node.js: `import '@smoothbricks/lmao/node'` sets this to use `process.hrtime.bigint()`
   *
   * Throws if called before a timestamp module is imported.
   *
   * @returns Current time as nanoseconds since Unix epoch
   */
  now(): Nanoseconds {
    throw new Error('Nanoseconds.now not initialized. Import @smoothbricks/lmao or @smoothbricks/lmao/node first.');
  },

  /**
   * Convert milliseconds to nanoseconds.
   *
   * @param millis - Milliseconds (e.g., from `Date.now()`)
   * @returns Nanoseconds (last 6 digits are 000000)
   */
  fromMillis(millis: number): Nanoseconds {
    return (BigInt(Math.trunc(millis)) * 1_000_000n) as Nanoseconds;
  },

  /**
   * Cast raw bigint to Nanoseconds. Use when you're certain the value is in nanoseconds.
   *
   * @param value - Raw bigint in nanoseconds
   */
  unsafe(value: bigint): Nanoseconds {
    return value as Nanoseconds;
  },

  /**
   * Convert nanoseconds to milliseconds.
   *
   * @param nanos - Nanoseconds value
   * @returns Milliseconds as number (truncated, not rounded)
   */
  toMillis(nanos: Nanoseconds): number {
    return Number(nanos / 1_000_000n);
  },

  /**
   * Convert nanoseconds to microseconds.
   *
   * @param nanos - Nanoseconds value
   * @returns Microseconds as bigint (truncated)
   */
  toMicros(nanos: Nanoseconds): bigint {
    return nanos / 1_000n;
  },
};
