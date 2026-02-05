/**
 * Add Duration - Date manipulation with duration strings
 *
 * Convenience wrapper around parseDuration for adding duration to dates.
 */

import { parseDuration } from './parse-duration.js';
import type { DurationString } from './types.js';

/**
 * Parse duration string and add to date.
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
export function addDuration(date: Date, duration: DurationString): Date {
  return new Date(date.getTime() + parseDuration(duration));
}
