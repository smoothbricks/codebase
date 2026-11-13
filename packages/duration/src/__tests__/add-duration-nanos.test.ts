import { describe, expect, it } from 'bun:test';
import { EpochNanos } from '@smoothbricks/time';
import { addDurationNanos, durationToMicros, durationToNanos } from '../add-duration.js';

describe('durationToNanos', () => {
  it('is the microsecond duration scaled by a thousand, for every unit', () => {
    for (const duration of ['1 ms', '500 ms', '30 seconds', '5 minutes', '2 hours', '7 days', '2 weeks'] as const) {
      expect(durationToNanos(duration)).toBe(durationToMicros(duration) * 1000n);
    }
  });

  it('stays exact past the span a number holds in nanoseconds', () => {
    // 2^53 ns is about 104 days; 520 weeks is 35 times that.
    expect(durationToNanos('520 weeks')).toBe(520n * 7n * 86_400n * 1_000_000_000n);
  });
});

describe('addDurationNanos', () => {
  it('adds in the nanosecond domain and keeps the sub-millisecond digits', () => {
    const now = EpochNanos(1_794_601_417_993_000_123n);
    expect(addDurationNanos(now, '5 minutes')).toBe(EpochNanos(1_794_601_717_993_000_123n));
  });
});
