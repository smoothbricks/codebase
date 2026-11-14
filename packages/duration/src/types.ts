/**
 * Duration Types
 *
 * Type definitions for duration parsing.
 */

/**
 * A string representing a duration in human-readable format.
 *
 * Supported formats:
 * - Milliseconds: "N ms", "N millisecond(s)"
 * - Seconds: "N s", "N second(s)"
 * - Minutes: "N m", "N min", "N minute(s)"
 * - Hours: "N h", "N hour(s)"
 * - Days: "N d", "N day(s)"
 * - Weeks: "N w", "N week(s)"
 *
 * Where N can be an integer or decimal number.
 *
 * @example
 * "5 minutes"
 * "24 hours"
 * "100 ms"
 * "1.5 hours"
 */
export type DurationString = string;

/**
 * A duration expressed in milliseconds.
 *
 * This is the canonical internal representation of duration.
 */
export type Milliseconds = number;

// Phantom brands: a span of time carries its unit in its type, as an instant does in `@smoothbricks/time`.
declare const durationUnit: unique symbol;
type DurationBrand<T, U> = T & { readonly [durationUnit]: U };

function brandBigint<U>(value: bigint): DurationBrand<bigint, U>;
function brandBigint(value: bigint): bigint {
  return value;
}

function brandNumber<U>(value: number): DurationBrand<number, U>;
function brandNumber(value: number): number {
  return value;
}

/** A span of time in bigint nanoseconds -- an Arrow `Duration(ns)` */
export type DurationNanos = DurationBrand<bigint, 'ns'>;
export function DurationNanos(value: bigint): DurationNanos {
  return brandBigint<'ns'>(value);
}

/**
 * A span of time in whole microseconds as a safe-integer number -- an Arrow `Duration(us)`. Throws a RangeError for a
 * value that counts no whole microsecond or lies past the safe-integer range (about 285 years); negative zero reads as
 * no span.
 */
export type DurationMicros = DurationBrand<number, 'us'>;
export function DurationMicros(value: number): DurationMicros {
  if (!Number.isSafeInteger(value)) {
    throw new RangeError(`DurationMicros: ${value} is not a safe integer count of microseconds`);
  }
  // `+ 0` turns -0 into 0 and leaves every other integer as it is.
  return brandNumber<'us'>(value + 0);
}

/** A span of time in number milliseconds -- an Arrow `Duration(ms)` */
export type DurationMillis = DurationBrand<number, 'ms'>;
export function DurationMillis(value: number): DurationMillis {
  return brandNumber<'ms'>(value);
}

/** A span of time in number seconds -- an Arrow `Duration(s)` */
export type DurationSeconds = DurationBrand<number, 's'>;
export function DurationSeconds(value: number): DurationSeconds {
  return brandNumber<'s'>(value);
}
