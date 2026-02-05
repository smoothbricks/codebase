/**
 * Parse Duration - Core duration string parsing
 *
 * Parses human-readable duration strings into milliseconds.
 */

import type { DurationString, Milliseconds } from './types.js';

/**
 * Regular expression for parsing duration strings.
 *
 * Matches patterns like:
 * - "5 minutes" or "5minutes"
 * - "24 hours" or "24h"
 * - "100 ms" or "100ms"
 * - "1.5 hours" (decimal values supported)
 */
const DURATION_REGEX = /^(\d+(?:\.\d+)?)\s*(ms|milliseconds?|seconds?|s|minutes?|min|m|hours?|h|days?|d|weeks?|w)$/i;

/**
 * Parse duration string to milliseconds.
 *
 * Supports: "N ms", "N milliseconds", "N seconds", "N minutes", "N hours", "N days", "N weeks"
 *
 * @param duration - Duration string (e.g., "5 minutes", "24 hours", "100 ms")
 * @returns Duration in milliseconds
 * @throws Error if duration format is invalid or negative
 *
 * @example
 * parseDuration('5 minutes');  // 300000
 * parseDuration('24 hours');   // 86400000
 * parseDuration('100 ms');     // 100
 * parseDuration('1.5 hours');  // 5400000
 */
export function parseDuration(duration: DurationString): Milliseconds {
  const match = duration.match(DURATION_REGEX);
  if (!match) {
    throw new Error(
      `Invalid duration format: "${duration}". Expected "N unit" (e.g., "5 minutes", "24 hours", "100 ms")`,
    );
  }

  const value = Number.parseFloat(match[1]);
  const unit = match[2].toLowerCase();

  // Reject negative durations (Phase 20 requirement)
  if (value < 0) {
    throw new Error(`Invalid duration: negative values are not allowed. Got "${duration}"`);
  }

  switch (unit) {
    case 'ms':
    case 'millisecond':
    case 'milliseconds':
      return value;
    case 's':
    case 'second':
    case 'seconds':
      return value * 1000;
    case 'm':
    case 'min':
    case 'minute':
    case 'minutes':
      return value * 60 * 1000;
    case 'h':
    case 'hour':
    case 'hours':
      return value * 60 * 60 * 1000;
    case 'd':
    case 'day':
    case 'days':
      return value * 24 * 60 * 60 * 1000;
    case 'w':
    case 'week':
    case 'weeks':
      return value * 7 * 24 * 60 * 60 * 1000;
    default:
      throw new Error(`Unknown duration unit: ${unit}`);
  }
}
