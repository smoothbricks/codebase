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
 *
 * ## Usage
 *
 * For timestamp functionality, import from platform-specific entry points:
 * - Node.js: `import { ... } from '@smoothbricks/lmao/node'`
 * - Browser: `import { ... } from '@smoothbricks/lmao/es'`
 *
 * The base `@smoothbricks/lmao` export does NOT include timestamp implementation
 * to enable tree-shaking.
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
 * Nanoseconds utilities for conversion between time units.
 */
export const Nanoseconds = {
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
