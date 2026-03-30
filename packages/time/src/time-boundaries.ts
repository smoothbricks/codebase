/**
 * Shared time-boundary labels used by scheduling helpers.
 *
 * Kept in `@smoothbricks/time` because the labels themselves are pure time-domain
 * data and do not depend on runtime code.
 */

export const TIME_BOUNDARIES = [
  'EOD',
  'EOW',
  'EOM',
  'EOQ',
  'EOY',
  'end of day',
  'end of week',
  'end of month',
  'end of quarter',
  'end of year',
] as const;

export type TimeBoundary = (typeof TIME_BOUNDARIES)[number];

const timeBoundarySet: ReadonlySet<string> = new Set(TIME_BOUNDARIES);

export function isTimeBoundary(value: string): value is TimeBoundary {
  return timeBoundarySet.has(value);
}
