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
declare const nanosecondsBrand: unique symbol;

export type Nanoseconds = bigint & { readonly [nanosecondsBrand]: 'Nanoseconds' };

function toNanoseconds(value: bigint): Nanoseconds;
function toNanoseconds(value: bigint): bigint {
  return value;
}

/**
 * Nanoseconds utilities for conversion between time units.
 */
export namespace Nanoseconds {
  /**
   * Convert milliseconds to nanoseconds.
   *
   * @param millis - Milliseconds (e.g., from `Date.now()`)
   * @returns Nanoseconds (last 6 digits are 000000)
   */
  export function fromMillis(millis: number): Nanoseconds {
    return toNanoseconds(BigInt(Math.trunc(millis)) * 1_000_000n);
  }

  /**
   * Cast raw bigint to Nanoseconds. Use when you're certain the value is in nanoseconds.
   *
   * @param value - Raw bigint in nanoseconds
   */
  export function unsafe(value: bigint): Nanoseconds {
    return toNanoseconds(value);
  }

  /**
   * Convert nanoseconds to milliseconds.
   *
   * @param nanos - Nanoseconds value
   * @returns Milliseconds as number (truncated, not rounded)
   */
  export function toMillis(nanos: Nanoseconds): number {
    return Number(nanos / 1_000_000n);
  }

  /**
   * Convert nanoseconds to microseconds.
   *
   * @param nanos - Nanoseconds value
   * @returns Microseconds as bigint (truncated)
   */
  export function toMicros(nanos: Nanoseconds): bigint {
    return nanos / 1_000n;
  }
}
