/**
 * Named, greppable, typed conversion functions between timestamp precision domains.
 *
 * These replace the 122+ inline arithmetic conversion sites scattered across
 * platform runtimes, and EventLog backends.
 */

import { EpochMicros, EpochMillis, EpochNanos } from './types.js';

/**
 * Floored division: the unit an instant lies in. Bigint `/` truncates toward zero, which before the epoch names the
 * unit after the instant.
 */
function floorDiv(value: bigint, unit: bigint): bigint {
  const quotient = value / unit;
  return value % unit < 0n ? quotient - 1n : quotient;
}
/** Convert EpochMillis to EpochMicros: millis * 1000 */
export function epochMillisToMicros(ms: EpochMillis): EpochMicros {
  return EpochMicros(BigInt(ms) * 1000n);
}

/** Convert EpochMicros to EpochMillis: the millisecond the instant lies in (floors, before the epoch too) */
export function epochMicrosToMillis(us: EpochMicros): EpochMillis {
  return EpochMillis(Number(floorDiv(us, 1000n)));
}

/** Convert Date to EpochMicros */
export function dateToMicros(d: Date): EpochMicros {
  return EpochMicros(BigInt(d.getTime()) * 1000n);
}

/** Convert Date to EpochMillis */
export function dateToMillis(d: Date): EpochMillis {
  return EpochMillis(d.getTime());
}

/** Convert EpochMicros to Date */
export function microsToDate(us: EpochMicros): Date {
  return new Date(Number(floorDiv(us, 1000n)));
}

/** Convert EpochMillis to Date */
export function millisToDate(ms: EpochMillis): Date {
  return new Date(ms);
}

/** Current time as EpochMicros -- replaces `BigInt(Date.now()) * 1000n` */
export function nowMicros(): EpochMicros {
  return EpochMicros(BigInt(Date.now()) * 1000n);
}

/** Current time as EpochMillis -- replaces bare `Date.now()` for timestamps */
export function nowMillis(): EpochMillis {
  return EpochMillis(Date.now());
}

/** Convert EpochMicros to ISO date string (YYYY-MM-DD) -- replaces duplicated accountingDateFromTimestamp */
export function microsToISODate(us: EpochMicros): string {
  return microsToDate(us).toISOString().slice(0, 10);
}

/** Convert EpochMicros to a full ISO-8601 timestamp — display edge only. */
export function microsToISOString(us: EpochMicros): string {
  return microsToDate(us).toISOString();
}

/** Convert EpochMillis to EpochNanos: millis * 1_000_000 */
export function epochMillisToNanos(ms: EpochMillis): EpochNanos {
  return EpochNanos(BigInt(ms) * 1_000_000n);
}

/** Convert EpochMicros to EpochNanos: micros * 1000 */
export function epochMicrosToNanos(us: EpochMicros): EpochNanos {
  return EpochNanos(us * 1000n);
}

/** Convert EpochNanos to EpochMicros: the microsecond the instant lies in (floors) */
export function epochNanosToMicros(ns: EpochNanos): EpochMicros {
  return EpochMicros(floorDiv(ns, 1000n));
}

/** Convert EpochNanos to EpochMillis: the millisecond the instant lies in (floors) */
export function epochNanosToMillis(ns: EpochNanos): EpochMillis {
  return EpochMillis(Number(floorDiv(ns, 1_000_000n)));
}

/** Convert Date to EpochNanos */
export function dateToNanos(d: Date): EpochNanos {
  return EpochNanos(BigInt(d.getTime()) * 1_000_000n);
}

/** Convert EpochNanos to Date: the millisecond the instant lies in (floors) */
export function nanosToDate(ns: EpochNanos): Date {
  return new Date(Number(floorDiv(ns, 1_000_000n)));
}

/** Current time as EpochNanos, at the wall clock's millisecond resolution */
export function nowNanos(): EpochNanos {
  return EpochNanos(BigInt(Date.now()) * 1_000_000n);
}

/** Convert EpochNanos to ISO date string (YYYY-MM-DD) */
export function nanosToISODate(ns: EpochNanos): string {
  return nanosToDate(ns).toISOString().slice(0, 10);
}

/** Convert EpochNanos to an RFC 3339 instant with all nine fraction digits, so the text names the instant exactly. */
export function nanosToISOString(ns: EpochNanos): string {
  const seconds = floorDiv(ns, 1_000_000_000n);
  const fraction = (ns - seconds * 1_000_000_000n).toString().padStart(9, '0');
  return `${new Date(Number(seconds) * 1000).toISOString().slice(0, 19)}.${fraction}Z`;
}
