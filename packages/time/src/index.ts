/**
 * @smoothbricks/time
 *
 * Branded timestamp types and named conversion functions for compile-time
 * safety between microsecond and millisecond precision domains.
 * Zero runtime dependencies. Works on all runtimes (Bun, Node, browser, Expo, AWS, Cloudflare).
 *
 * @example
 * ```typescript
 * import { EpochMicros, EpochMillis, epochMicrosToMillis, nowMicros } from '@smoothbricks/time';
 *
 * const now = nowMicros();                   // branded bigint microseconds
 * const ms = epochMicrosToMillis(now);       // branded number milliseconds
 * const date = new Date(ms);                 // JS Date from branded millis
 * ```
 *
 * @packageDocumentation
 */

export {
  dateToMicros,
  dateToMillis,
  epochMicrosToMillis,
  epochMillisToMicros,
  microsToDate,
  microsToISODate,
  millisToDate,
  nowMicros,
  nowMillis,
} from './conversions.js';
export { isTimeBoundary, TIME_BOUNDARIES, type TimeBoundary } from './time-boundaries.js';
export { EpochMicros, EpochMillis } from './types.js';
