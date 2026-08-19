/**
 * Add Duration - add a duration string to a clock value.
 *
 * Date and EpochMicros overloads share parseDuration. The micros arm stays in
 * the µs domain (duration ms × 1000) so decide clocks never drop to Date.
 */

import { EpochMicros } from '@smoothbricks/time';
import { parseDuration } from './parse-duration.js';
import type { DurationString } from './types.js';

/**
 * Convert a duration string to microseconds.
 *
 * parseDuration is millisecond-valued; this scales the duration, not a timestamp.
 */
export function durationToMicros(duration: DurationString): bigint {
  return BigInt(Math.round(parseDuration(duration) * 1000));
}

/**
 * Parse a duration string and add it to a clock value.
 *
 * Supports: "N ms", "N milliseconds", "N seconds", "N minutes", "N hours", "N days", "N weeks"
 *
 * @param date - Base date to add duration to
 * @param duration - Duration string (e.g., "5 minutes", "24 hours", "100 ms")
 * @returns New Date at date + duration
 * @throws Error if duration format is invalid or negative
 *
 * @example
 * addDuration(new Date('2024-01-01T00:00:00Z'), '5 minutes');
 * // Returns Date('2024-01-01T00:05:00Z')
 *
 * addDuration(new Date('2024-01-01T00:00:00Z'), '1 hour');
 * // Returns Date('2024-01-01T01:00:00Z')
 */
export function addDuration(date: Date, duration: DurationString): Date;
/**
 * Add a duration to a branded microsecond clock, staying in the µs domain.
 *
 * @param now - Base timestamp, typically `nowMicros()` or a decide clock
 * @param duration - Duration string (e.g., "5 minutes", "24 hours", "100 ms")
 * @returns New EpochMicros at now + duration
 * @throws Error if duration format is invalid or negative
 *
 * @example
 * addDuration(EpochMicros(1_704_067_200_000_000n), '5 minutes');
 * // Returns EpochMicros(1_704_067_500_000_000n)
 */
export function addDuration(now: EpochMicros, duration: DurationString): EpochMicros;
export function addDuration(base: Date | EpochMicros, duration: DurationString): Date | EpochMicros {
  if (typeof base === 'bigint') {
    return EpochMicros(base + durationToMicros(duration));
  }
  return new Date(base.getTime() + parseDuration(duration));
}
