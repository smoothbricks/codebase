/**
 * Civil time-boundary computation in the EpochMicros domain.
 *
 * Calendar/TZ math uses Date internally; the public contract is µs in / µs out.
 */

import { dateToMicros, microsToDate } from './conversions.js';
import type { TimeBoundary } from './time-boundaries.js';
import type { EpochMicros } from './types.js';

type NormalizedBoundary = 'EOD' | 'EOW' | 'EOM' | 'EOQ' | 'EOY';
type LocalDate = { year: number; month: number; day: number };

const LONG_TO_SHORT: Record<string, NormalizedBoundary> = {
  'end of day': 'EOD',
  'end of week': 'EOW',
  'end of month': 'EOM',
  'end of quarter': 'EOQ',
  'end of year': 'EOY',
};

function normalize(boundary: TimeBoundary): NormalizedBoundary {
  switch (boundary) {
    case 'EOD':
    case 'EOW':
    case 'EOM':
    case 'EOQ':
    case 'EOY':
      return boundary;
    case 'end of day':
    case 'end of week':
    case 'end of month':
    case 'end of quarter':
    case 'end of year':
      return LONG_TO_SHORT[boundary];
  }
}

function setEndOfDayUTC(date: Date): Date {
  const d = new Date(date);
  d.setUTCHours(23, 59, 59, 999);
  return d;
}

function computeUTC(now: Date, boundary: NormalizedBoundary): Date {
  const year = now.getUTCFullYear();
  const month = now.getUTCMonth();

  switch (boundary) {
    case 'EOD': {
      return setEndOfDayUTC(now);
    }

    case 'EOW': {
      const day = now.getUTCDay();
      const daysUntilSunday = day === 0 ? 0 : 7 - day;
      const sunday = new Date(now);
      sunday.setUTCDate(now.getUTCDate() + daysUntilSunday);
      return setEndOfDayUTC(sunday);
    }

    case 'EOM': {
      const lastDay = new Date(Date.UTC(year, month + 1, 0));
      return setEndOfDayUTC(lastDay);
    }

    case 'EOQ': {
      const quarterEndMonth = Math.floor(month / 3) * 3 + 2;
      const lastDay = new Date(Date.UTC(year, quarterEndMonth + 1, 0));
      return setEndOfDayUTC(lastDay);
    }

    case 'EOY': {
      const dec31 = new Date(Date.UTC(year, 11, 31));
      return setEndOfDayUTC(dec31);
    }
  }
}

function getLocalComponents(now: Date, timezone: string): LocalDate {
  const formatter = new Intl.DateTimeFormat('en-US', {
    timeZone: timezone,
    year: 'numeric',
    month: 'numeric',
    day: 'numeric',
    hour12: false,
  });
  const parts = formatter.formatToParts(now);

  let year = 0;
  let month = 0;
  let day = 0;
  for (const part of parts) {
    if (part.type === 'year') year = Number(part.value);
    if (part.type === 'month') month = Number(part.value);
    if (part.type === 'day') day = Number(part.value);
  }

  return { year, month, day };
}

function getMinute(parts: Intl.DateTimeFormatPart[]): number {
  for (const part of parts) {
    if (part.type === 'minute') return Number(part.value);
  }
  return 0;
}

function localToUTC(
  year: number,
  month: number,
  day: number,
  hour: number,
  minute: number,
  second: number,
  ms: number,
  timezone: string,
): Date {
  const guess = new Date(Date.UTC(year, month - 1, day, hour, minute, second, ms));
  const guessLocal = getLocalComponents(guess, timezone);

  const guessFormatter = new Intl.DateTimeFormat('en-US', {
    timeZone: timezone,
    year: 'numeric',
    month: 'numeric',
    day: 'numeric',
    hour: 'numeric',
    minute: 'numeric',
    second: 'numeric',
    hour12: false,
  });
  const guessParts = guessFormatter.formatToParts(guess);

  let guessHour = 0;
  for (const part of guessParts) {
    if (part.type === 'hour') guessHour = Number(part.value);
  }

  const dayDiff = day - guessLocal.day;
  const hourDiff = hour - guessHour;
  const offsetMs = (dayDiff * 24 + hourDiff) * 3600 * 1000 + (minute - getMinute(guessParts)) * 60 * 1000;

  return new Date(guess.getTime() + offsetMs);
}

function computeWithTimezone(now: Date, boundary: NormalizedBoundary, timezone: string): Date {
  const local = getLocalComponents(now, timezone);

  let targetYear = local.year;
  let targetMonth = local.month;
  let targetDay = local.day;

  switch (boundary) {
    case 'EOD': {
      break;
    }

    case 'EOW': {
      const localDate = new Date(Date.UTC(local.year, local.month - 1, local.day, 12, 0, 0));
      const dayOfWeek = localDate.getUTCDay();
      const daysUntilSunday = dayOfWeek === 0 ? 0 : 7 - dayOfWeek;
      targetDay = local.day + daysUntilSunday;
      const normalized = new Date(Date.UTC(targetYear, targetMonth - 1, targetDay));
      targetYear = normalized.getUTCFullYear();
      targetMonth = normalized.getUTCMonth() + 1;
      targetDay = normalized.getUTCDate();
      break;
    }

    case 'EOM': {
      const lastDay = new Date(Date.UTC(local.year, local.month, 0));
      targetDay = lastDay.getUTCDate();
      break;
    }

    case 'EOQ': {
      const m0 = local.month - 1;
      const quarterEndMonth = Math.floor(m0 / 3) * 3 + 2;
      const lastDay = new Date(Date.UTC(local.year, quarterEndMonth + 1, 0));
      targetMonth = quarterEndMonth + 1;
      targetDay = lastDay.getUTCDate();
      break;
    }

    case 'EOY': {
      targetMonth = 12;
      targetDay = 31;
      break;
    }
  }

  return localToUTC(targetYear, targetMonth, targetDay, 23, 59, 59, 999, timezone);
}

/**
 * Compute an absolute EpochMicros for a civil time boundary.
 *
 * @param now - Current time in epoch microseconds
 * @param boundary - EOD / EOW / EOM / EOQ / EOY or long forms
 * @param timezone - Optional IANA timezone. UTC if omitted.
 */
export function computeTimeBoundary(now: EpochMicros, boundary: TimeBoundary, timezone?: string): EpochMicros {
  const civil = microsToDate(now);
  const normalized = normalize(boundary);
  const result = timezone ? computeWithTimezone(civil, normalized, timezone) : computeUTC(civil, normalized);
  return dateToMicros(result);
}
