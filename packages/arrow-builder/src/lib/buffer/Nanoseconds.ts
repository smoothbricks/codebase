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

import {
  EpochMillis,
  EpochNanos,
  epochMillisToNanos,
  epochNanosToMicros,
  epochNanosToMillis,
} from '@smoothbricks/time';

/**
 * Nanoseconds since the Unix epoch: `@smoothbricks/time`'s `EpochNanos`, the one nanosecond brand.
 */
export type Nanoseconds = EpochNanos;

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
    return epochMillisToNanos(EpochMillis(Math.trunc(millis)));
  }

  /**
   * Cast raw bigint to Nanoseconds. Use when you're certain the value is in nanoseconds.
   *
   * @param value - Raw bigint in nanoseconds
   */
  export function unsafe(value: bigint): Nanoseconds {
    return EpochNanos(value);
  }

  /**
   * Convert nanoseconds to milliseconds.
   *
   * @param nanos - Nanoseconds value
   * @returns The millisecond the instant lies in
   */
  export function toMillis(nanos: Nanoseconds): number {
    return epochNanosToMillis(nanos);
  }

  /**
   * Convert nanoseconds to microseconds.
   *
   * @param nanos - Nanoseconds value
   * @returns The microsecond the instant lies in
   */
  export function toMicros(nanos: Nanoseconds): bigint {
    return epochNanosToMicros(nanos);
  }
}
