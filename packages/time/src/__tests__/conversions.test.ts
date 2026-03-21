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

describe('epochMillisToMicros', () => {
  it('converts milliseconds to microseconds', () => {
    const us = epochMillisToMicros(EpochMillis(1000));
    expect(us).toBe(EpochMicros(1_000_000n));
  });

  it('converts zero', () => {
    expect(epochMillisToMicros(EpochMillis(0))).toBe(EpochMicros(0n));
  });

  it('converts negative (pre-epoch)', () => {
    expect(epochMillisToMicros(EpochMillis(-500))).toBe(EpochMicros(-500_000n));
  });

  it('converts a realistic timestamp', () => {
    // 2024-01-01T00:00:00.000Z = 1704067200000 ms
    const us = epochMillisToMicros(EpochMillis(1_704_067_200_000));
    expect(us).toBe(EpochMicros(1_704_067_200_000_000n));
  });
});

describe('epochMicrosToMillis', () => {
  it('converts microseconds to milliseconds', () => {
    const ms = epochMicrosToMillis(EpochMicros(1_000_000n));
    expect(ms).toBe(EpochMillis(1000));
  });

  it('truncates sub-millisecond precision', () => {
    // 1500 microseconds = 1.5 milliseconds, truncated to 1
    const ms = epochMicrosToMillis(EpochMicros(1_500n));
    expect(ms).toBe(EpochMillis(1));
  });

  it('converts zero', () => {
    expect(epochMicrosToMillis(EpochMicros(0n))).toBe(EpochMillis(0));
  });

  it('converts negative (pre-epoch)', () => {
    expect(epochMicrosToMillis(EpochMicros(-500_000n))).toBe(EpochMillis(-500));
  });
});

describe('round-trip: millis -> micros -> millis', () => {
  it('preserves integer milliseconds', () => {
    const original = EpochMillis(1_704_067_200_000);
    const roundTripped = epochMicrosToMillis(epochMillisToMicros(original));
    expect(roundTripped).toBe(original);
  });

  it('preserves zero', () => {
    const original = EpochMillis(0);
    expect(epochMicrosToMillis(epochMillisToMicros(original))).toBe(EpochMillis(0));
  });

  it('preserves negative timestamps', () => {
    const original = EpochMillis(-86_400_000);
    expect(epochMicrosToMillis(epochMillisToMicros(original))).toBe(EpochMillis(-86_400_000));
  });

  it('preserves large timestamps (year 2100+)', () => {
    // 2100-01-01T00:00:00Z = 4102444800000 ms
    const original = EpochMillis(4_102_444_800_000);
    expect(epochMicrosToMillis(epochMillisToMicros(original))).toBe(EpochMillis(4_102_444_800_000));
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
  it('produces correct YYYY-MM-DD for a known timestamp', () => {
    // 2024-06-15T12:30:45.123Z
    const d = new Date('2024-06-15T12:30:45.123Z');
    const us = dateToMicros(d);
    expect(microsToISODate(us)).toBe('2024-06-15');
  });

  it('handles epoch', () => {
    expect(microsToISODate(EpochMicros(0n))).toBe('1970-01-01');
  });

  it('handles end-of-day boundary (UTC)', () => {
    // 2024-12-31T23:59:59.999Z -- still Dec 31 in UTC
    const d = new Date('2024-12-31T23:59:59.999Z');
    const us = dateToMicros(d);
    expect(microsToISODate(us)).toBe('2024-12-31');
  });

  it('handles very large timestamps', () => {
    // 2099-12-31T00:00:00.000Z
    const d = new Date('2099-12-31T00:00:00.000Z');
    const us = dateToMicros(d);
    expect(microsToISODate(us)).toBe('2099-12-31');
  });
});
