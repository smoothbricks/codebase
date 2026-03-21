/**
 * Named, greppable, typed conversion functions between timestamp precision domains.
 *
 * These replace the 122+ inline arithmetic conversion sites scattered across
 * platform runtimes, and EventLog backends.
 */

import { EpochMicros, EpochMillis } from './types.js';

/** Convert EpochMillis to EpochMicros: millis * 1000 */
export function epochMillisToMicros(ms: EpochMillis): EpochMicros {
  return EpochMicros(BigInt(ms) * 1000n);
}

/** Convert EpochMicros to EpochMillis: micros / 1000 (truncates sub-ms) */
export function epochMicrosToMillis(us: EpochMicros): EpochMillis {
  return EpochMillis(Number(us / 1000n));
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
  return new Date(Number(us / 1000n));
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
