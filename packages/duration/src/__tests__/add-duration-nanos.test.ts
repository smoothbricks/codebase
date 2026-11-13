import { describe, expect, it } from 'bun:test';
import { EpochNanos } from '@smoothbricks/time';
import { addDurationNanos, durationToMicros, durationToNanos } from '../add-duration.js';
import { DurationMicros, DurationMillis, DurationNanos, DurationSeconds } from '../types.js';

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

describe('duration unit brands', () => {
  it('refuse each other and a bare value, by type', () => {
    const nanos: DurationNanos = DurationNanos(1_000n);
    // @ts-expect-error a microsecond span takes no nanosecond value
    const asMicros: DurationMicros = nanos;
    // @ts-expect-error a millisecond span takes no bare number
    const bare: DurationMillis = 5;
    // @ts-expect-error a seconds span takes no millisecond value
    const asSeconds: DurationSeconds = DurationMillis(5);
    expect([asMicros, bare, asSeconds].length).toBe(3);
  });
});
