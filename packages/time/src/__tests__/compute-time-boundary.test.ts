import { describe, expect, it } from 'bun:test';
import { computeTimeBoundary } from '../compute-time-boundary.js';
import { dateToMicros, microsToISOString } from '../conversions.js';
import type { TimeBoundary } from '../time-boundaries.js';

// [now, boundary, timezone, the last millisecond of the boundary]
const CASES: readonly (readonly [string, TimeBoundary, string | undefined, string])[] = [
  ['2026-02-06T14:30:00Z', 'EOD', undefined, '2026-02-06T23:59:59.999000Z'],
  ['2026-02-06T14:30:00Z', 'end of day', undefined, '2026-02-06T23:59:59.999000Z'],
  // Wednesday and Sunday both end on that Sunday.
  ['2026-02-04T10:00:00Z', 'EOW', undefined, '2026-02-08T23:59:59.999000Z'],
  ['2026-02-08T10:00:00Z', 'EOW', undefined, '2026-02-08T23:59:59.999000Z'],
  ['2026-02-04T10:00:00Z', 'end of week', undefined, '2026-02-08T23:59:59.999000Z'],
  // February of a common year and of a leap year.
  ['2026-02-15T10:00:00Z', 'EOM', undefined, '2026-02-28T23:59:59.999000Z'],
  ['2024-02-15T10:00:00Z', 'EOM', undefined, '2024-02-29T23:59:59.999000Z'],
  ['2026-02-15T10:00:00Z', 'end of month', undefined, '2026-02-28T23:59:59.999000Z'],
  ['2026-02-15T10:00:00Z', 'EOQ', undefined, '2026-03-31T23:59:59.999000Z'],
  ['2026-05-15T10:00:00Z', 'EOQ', undefined, '2026-06-30T23:59:59.999000Z'],
  ['2026-08-15T10:00:00Z', 'EOQ', undefined, '2026-09-30T23:59:59.999000Z'],
  ['2026-11-15T10:00:00Z', 'EOQ', undefined, '2026-12-31T23:59:59.999000Z'],
  ['2026-02-15T10:00:00Z', 'end of quarter', undefined, '2026-03-31T23:59:59.999000Z'],
  ['2026-06-15T10:00:00Z', 'EOY', undefined, '2026-12-31T23:59:59.999000Z'],
  ['2026-06-15T10:00:00Z', 'end of year', undefined, '2026-12-31T23:59:59.999000Z'],
  // New York in winter (UTC-5), and the day of the March 2026 spring-forward.
  ['2026-02-06T20:00:00Z', 'EOD', 'America/New_York', '2026-02-07T04:59:59.999000Z'],
  ['2026-03-08T10:00:00Z', 'EOD', 'America/New_York', '2026-03-09T03:59:59.999000Z'],
];

describe('computeTimeBoundary', () => {
  it('names_the_last_millisecond_of_each_civil_boundary', () => {
    for (const [now, boundary, timezone, end] of CASES) {
      expect([
        now,
        boundary,
        microsToISOString(computeTimeBoundary(dateToMicros(new Date(now)), boundary, timezone)),
      ]).toEqual([now, boundary, end]);
    }
  });
});
