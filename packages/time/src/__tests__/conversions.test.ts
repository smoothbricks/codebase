import { describe, expect, it } from 'bun:test';

import {
  dateToMicros,
  dateToMillis,
  EpochMicros,
  EpochMillis,
  epochMicrosToMillis,
  epochMillisToMicros,
  microsToDate,
  microsToISODate,
  microsToISOString,
  millisToDate,
  nowMicros,
  nowMillis,
} from '../index.js';

describe('EpochMicros constructor', () => {
  it('wraps a bigint value', () => {
    const us = EpochMicros(1_000_000n);
    expect(us).toBe(EpochMicros(1_000_000n));
  });

  it('works with zero', () => {
    expect(EpochMicros(0n)).toBe(EpochMicros(0n));
  });

  it('works with negative (pre-epoch)', () => {
    expect(EpochMicros(-1_000_000n)).toBe(EpochMicros(-1_000_000n));
  });
});

describe('EpochMillis constructor', () => {
  it('wraps a number value', () => {
    expect(EpochMillis(1000)).toBe(EpochMillis(1000));
  });

  it('works with zero', () => {
    expect(EpochMillis(0)).toBe(EpochMillis(0));
  });

  it('works with negative (pre-epoch)', () => {
    expect(EpochMillis(-1000)).toBe(EpochMillis(-1000));
  });
});

// [milliseconds, the same instant in microseconds]: a second, zero, before the epoch, 2024-01-01, 2100-01-01.
const MILLIS_AND_MICROS = [
  [1000, 1_000_000n],
  [0, 0n],
  [-500, -500_000n],
  [-86_400_000, -86_400_000_000n],
  [1_704_067_200_000, 1_704_067_200_000_000n],
  [4_102_444_800_000, 4_102_444_800_000_000n],
] as const;

describe('epochMillisToMicros and epochMicrosToMillis', () => {
  it('convert whole milliseconds both ways exactly', () => {
    for (const [ms, us] of MILLIS_AND_MICROS) {
      expect([ms, epochMillisToMicros(EpochMillis(ms))]).toEqual([ms, EpochMicros(us)]);
      expect([us, epochMicrosToMillis(EpochMicros(us))]).toEqual([us, EpochMillis(ms)]);
      expect(epochMicrosToMillis(epochMillisToMicros(EpochMillis(ms)))).toBe(EpochMillis(ms));
    }
  });

  it('truncates sub-millisecond precision', () => {
    // 1500 microseconds = 1.5 milliseconds, truncated to 1
    expect(epochMicrosToMillis(EpochMicros(1_500n))).toBe(EpochMillis(1));
  });
});

describe('dateToMicros / microsToDate round-trip', () => {
  it('preserves millisecond precision through round-trip', () => {
    const original = new Date('2024-06-15T12:30:45.123Z');
    const us = dateToMicros(original);
    const roundTripped = microsToDate(us);
    expect(roundTripped.getTime()).toBe(original.getTime());
  });

  it('handles epoch (1970-01-01)', () => {
    const epoch = new Date(0);
    const us = dateToMicros(epoch);
    expect(us).toBe(EpochMicros(0n));
    expect(microsToDate(us).getTime()).toBe(0);
  });

  it('handles pre-epoch dates', () => {
    const preEpoch = new Date('1969-07-20T20:17:00.000Z');
    const us = dateToMicros(preEpoch);
    expect(us < 0n).toBe(true);
    expect(microsToDate(us).getTime()).toBe(preEpoch.getTime());
  });
});

describe('dateToMillis / millisToDate round-trip', () => {
  it('preserves exact millisecond timestamp', () => {
    const original = new Date('2024-06-15T12:30:45.123Z');
    const ms = dateToMillis(original);
    const roundTripped = millisToDate(ms);
    expect(roundTripped.getTime()).toBe(original.getTime());
  });

  it('handles epoch', () => {
    const epoch = new Date(0);
    const ms = dateToMillis(epoch);
    expect(ms).toBe(EpochMillis(0));
    expect(millisToDate(ms).getTime()).toBe(0);
  });
});

describe('nowMicros', () => {
  it('returns a bigint', () => {
    const us = nowMicros();
    expect(typeof us).toBe('bigint');
  });

  it('returns a plausible timestamp (after 2024-01-01, before 2100-01-01)', () => {
    const us = nowMicros();
    const min = 1_704_067_200_000_000n; // 2024-01-01
    const max = 4_102_444_800_000_000n; // 2100-01-01
    expect(us >= min).toBe(true);
    expect(us <= max).toBe(true);
  });
});

describe('nowMillis', () => {
  it('returns a number', () => {
    const ms = nowMillis();
    expect(typeof ms).toBe('number');
  });

  it('returns a plausible timestamp (after 2024-01-01, before 2100-01-01)', () => {
    const ms = nowMillis();
    const min = 1_704_067_200_000; // 2024-01-01
    const max = 4_102_444_800_000; // 2100-01-01
    expect(ms >= min).toBe(true);
    expect(ms <= max).toBe(true);
  });
});

describe('microsToISODate', () => {
  it('names the UTC calendar day of an instant', () => {
    // A known instant, the last millisecond of a year, and a far year.
    for (const [iso, day] of [
      ['2024-06-15T12:30:45.123Z', '2024-06-15'],
      ['2024-12-31T23:59:59.999Z', '2024-12-31'],
      ['2099-12-31T00:00:00.000Z', '2099-12-31'],
    ] as const) {
      expect([iso, microsToISODate(dateToMicros(new Date(iso)))]).toEqual([iso, day]);
    }
    expect(microsToISODate(EpochMicros(0n))).toBe('1970-01-01');
  });
});

describe('microsToISOString', () => {
  it('preserves the full UTC timestamp', () => {
    const d = new Date('2024-06-15T12:30:45.123Z');
    expect(microsToISOString(dateToMicros(d))).toBe('2024-06-15T12:30:45.123Z');
  });
});
