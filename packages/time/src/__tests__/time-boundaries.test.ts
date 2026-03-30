import { describe, expect, it } from 'bun:test';

import { isTimeBoundary, TIME_BOUNDARIES } from '../index.js';

describe('time boundaries', () => {
  it('exports the full boundary label set', () => {
    expect(TIME_BOUNDARIES).toEqual([
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
    ]);
  });

  it('accepts every supported boundary label', () => {
    for (const boundary of TIME_BOUNDARIES) {
      expect(isTimeBoundary(boundary)).toBe(true);
    }
  });

  it('rejects unsupported labels', () => {
    expect(isTimeBoundary('tomorrow')).toBe(false);
    expect(isTimeBoundary('eod')).toBe(false);
  });
});
