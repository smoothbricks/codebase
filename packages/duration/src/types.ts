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
