/**
 * Branded time types for compile-time safety between precision domains.
 *
 * Every instant is a native JS primitive with a phantom brand. A nanosecond instant is a `bigint`: nanoseconds since the
 * epoch are about 1.7e18 today, past what a number holds exactly. A microsecond, millisecond or second instant is a
 * safe-integer `number`: microseconds since the epoch are about 1.7e15 today and stay under 2^53 until the year 2255,
 * and a number adds and compares without the heap value a `bigint` is
 * (specs/lmao/01a_trace_schema_system.md#smoo/lmao!n/schema-time-units).
 */

// Inline Brand to avoid circular dependencies.
// Phantom brand pattern for compile-time safety.
declare const __brand: unique symbol;
type Brand<T, B> = T & { readonly [__brand]: B };

function brandBigint<B>(value: bigint): Brand<bigint, B>;
function brandBigint(value: bigint): bigint {
  return value;
}

function brandNumber<B>(value: number): Brand<number, B>;
function brandNumber(value: number): number {
  return value;
}

/**
 * Microseconds since the epoch as a safe-integer number: a business instant, when something happened or is due in a
 * domain. Some platforms give no nanoseconds; the event and trace clock is `EpochNanos`.
 */
export type EpochMicros = Brand<number, 'EpochMicros'>;

/**
 * Constructor. Throws a RangeError for a value that counts no whole microsecond (a fraction, NaN, an infinity) or lies
 * past the safe-integer range (more than about 285 years from 1970): a caller holding such a value has a defect. A value
 * from outside the program is judged by its own schema first (a `Timestamp(us)` field's `validate`). Negative zero
 * reads as the epoch.
 */
export function EpochMicros(value: number): EpochMicros {
  if (!Number.isSafeInteger(value)) {
    throw new RangeError(`EpochMicros: ${value} is not a safe integer count of microseconds`);
  }
  // `+ 0` turns -0 into 0 and leaves every other integer as it is.
  return brandNumber<'EpochMicros'>(value + 0);
}

/** Bigint nanoseconds since epoch -- the unit of an Arrow `Timestamp(ns)` and of a span's clock */
export type EpochNanos = Brand<bigint, 'EpochNanos'>;

/** Constructor */
export function EpochNanos(value: bigint): EpochNanos {
  return brandBigint<'EpochNanos'>(value);
}
/** Number milliseconds since epoch -- JS Date.getTime() domain */
export type EpochMillis = Brand<number, 'EpochMillis'>;

/** Constructor */
export function EpochMillis(value: number): EpochMillis {
  return brandNumber<'EpochMillis'>(value);
}

/** Number seconds since epoch -- the Unix `time_t` domain */
export type EpochSeconds = Brand<number, 'EpochSeconds'>;

/** Constructor */
export function EpochSeconds(value: number): EpochSeconds {
  return brandNumber<'EpochSeconds'>(value);
}
