import { describe, expect, it } from 'bun:test';

import {
  dateToMicros,
  dateToMillis,
  EpochMicros,
  EpochMillis,
  EpochNanos,
  epochMicrosToMillis,
  epochMicrosToNanos,
  epochMillisToMicros,
  epochNanosToMicros,
  microsToDate,
  microsToISODate,
  microsToISOString,
  millisToDate,
  nowMicros,
  nowMillis,
} from '../index.js';

// 2024-06-15T12:30:45.123456Z: a current instant with every microsecond digit set.
const INSTANT = 1_718_454_645_123_456;

//#region smoo/lmao!n/schema-time-units
describe('EpochMicros', () => {
  it('a_microsecond_instant_is_a_safe_integer_number', () => {
    const us = EpochMicros(INSTANT);
    expect(typeof us).toBe('number');
    expect(Number.isSafeInteger(us)).toBe(true);
    expect(us).toBe(EpochMicros(INSTANT));
    expect(EpochMicros(0)).toBe(EpochMicros(0));
    expect(EpochMicros(-1_000_000)).toBe(EpochMicros(-1_000_000));
  });

  it('holds_the_whole_safe_range', () => {
    expect(EpochMicros(Number.MAX_SAFE_INTEGER)).toBe(EpochMicros(Number.MAX_SAFE_INTEGER));
    expect(EpochMicros(Number.MIN_SAFE_INTEGER)).toBe(EpochMicros(Number.MIN_SAFE_INTEGER));
  });

  it('refuses_a_value_that_counts_no_whole_microsecond', () => {
    expect(() => EpochMicros(1.5)).toThrow(RangeError);
    expect(() => EpochMicros(Number.NaN)).toThrow(RangeError);
    expect(() => EpochMicros(Number.POSITIVE_INFINITY)).toThrow(RangeError);
    expect(() => EpochMicros(Number.MAX_SAFE_INTEGER + 1)).toThrow(RangeError);
    expect(() => EpochMicros(Number.MIN_SAFE_INTEGER - 1)).toThrow(RangeError);
    expect(() => EpochMicros(1.5)).toThrow('EpochMicros: 1.5 is not a safe integer count of microseconds');
  });

  it('reads_negative_zero_as_the_epoch', () => {
    expect(Object.is(EpochMicros(-0), 0)).toBe(true);
  });

  it('refuses_a_value_of_another_unit_by_type', () => {
    // @ts-expect-error a microsecond instant takes no millisecond value
    const fromMillis: EpochMicros = EpochMillis(1);
    // @ts-expect-error a microsecond instant takes no bare number
    const bare: EpochMicros = 1;
    // @ts-expect-error a microsecond instant takes no bigint
    const wide: EpochMicros = EpochNanos(1n);
    expect([fromMillis, bare, wide].length).toBe(3);
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
  it('is_exact', () => {
    expect(epochMillisToMicros(EpochMillis(1000))).toBe(EpochMicros(1_000_000));
    expect(epochMillisToMicros(EpochMillis(0))).toBe(EpochMicros(0));
    expect(epochMillisToMicros(EpochMillis(-500))).toBe(EpochMicros(-500_000));
    // 2024-01-01T00:00:00.000Z
    expect(epochMillisToMicros(EpochMillis(1_704_067_200_000))).toBe(EpochMicros(1_704_067_200_000_000));
  });

  it('refuses_a_millisecond_past_the_microsecond_range', () => {
    // The last millisecond whose microseconds a number holds exactly, and the one after it (year 2255).
    expect(epochMillisToMicros(EpochMillis(9_007_199_254_740))).toBe(EpochMicros(9_007_199_254_740_000));
    expect(() => epochMillisToMicros(EpochMillis(9_007_199_254_741))).toThrow(RangeError);
  });
});

describe('epochMicrosToMillis', () => {
  it('floors_to_the_millisecond_the_instant_lies_in', () => {
    expect(epochMicrosToMillis(EpochMicros(1_000_000))).toBe(EpochMillis(1000));
    expect(epochMicrosToMillis(EpochMicros(1_500))).toBe(EpochMillis(1));
    expect(epochMicrosToMillis(EpochMicros(0))).toBe(EpochMillis(0));
    expect(epochMicrosToMillis(EpochMicros(-500_000))).toBe(EpochMillis(-500));
    expect(epochMicrosToMillis(EpochMicros(-1))).toBe(EpochMillis(-1));
    expect(epochMicrosToMillis(EpochMicros(-1_001))).toBe(EpochMillis(-2));
  });

  it('floors_exactly_at_the_top_of_the_range', () => {
    // One microsecond before a millisecond, where the quotient's distance to the next integer is smallest.
    expect(epochMicrosToMillis(EpochMicros(9_007_199_254_739_999))).toBe(EpochMillis(9_007_199_254_739));
    expect(epochMicrosToMillis(EpochMicros(Number.MAX_SAFE_INTEGER))).toBe(EpochMillis(9_007_199_254_740));
    expect(epochMicrosToMillis(EpochMicros(Number.MIN_SAFE_INTEGER))).toBe(EpochMillis(-9_007_199_254_741));
  });

  it('round_trips_every_millisecond', () => {
    for (const ms of [1_704_067_200_000, 0, -86_400_000, 4_102_444_800_000]) {
      expect(epochMicrosToMillis(epochMillisToMicros(EpochMillis(ms)))).toBe(EpochMillis(ms));
    }
  });
});

describe('epochMicrosToNanos / epochNanosToMicros', () => {
  it('scales_up_exactly_over_the_whole_range', () => {
    expect(epochMicrosToNanos(EpochMicros(INSTANT))).toBe(EpochNanos(1_718_454_645_123_456_000n));
    expect(epochMicrosToNanos(EpochMicros(Number.MAX_SAFE_INTEGER))).toBe(EpochNanos(9_007_199_254_740_991_000n));
    expect(epochMicrosToNanos(EpochMicros(-1))).toBe(EpochNanos(-1_000n));
  });

  it('floors_nanoseconds_to_the_microsecond_they_lie_in', () => {
    expect(epochNanosToMicros(EpochNanos(1_718_454_645_123_456_999n))).toBe(EpochMicros(INSTANT));
    expect(epochNanosToMicros(EpochNanos(-1n))).toBe(EpochMicros(-1));
    expect(epochNanosToMicros(EpochNanos(-1_001n))).toBe(EpochMicros(-2));
  });

  it('refuses_a_nanosecond_instant_past_the_microsecond_range', () => {
    expect(() => epochNanosToMicros(EpochNanos(9_007_199_254_740_992_000n))).toThrow(RangeError);
    expect(() => epochNanosToMicros(EpochNanos(-9_007_199_254_740_992_000n))).toThrow(RangeError);
  });
});

describe('dateToMicros / microsToDate', () => {
  it('scales_a_date_exactly', () => {
    const original = new Date('2024-06-15T12:30:45.123Z');
    expect(dateToMicros(original)).toBe(EpochMicros(1_718_454_645_123_000));
    expect(microsToDate(dateToMicros(original)).getTime()).toBe(original.getTime());
    expect(dateToMicros(new Date(0))).toBe(EpochMicros(0));
    const preEpoch = new Date('1969-07-20T20:17:00.000Z');
    expect(dateToMicros(preEpoch) < 0).toBe(true);
    expect(microsToDate(dateToMicros(preEpoch)).getTime()).toBe(preEpoch.getTime());
  });

  it('floors_to_the_millisecond_a_date_holds', () => {
    expect(microsToDate(EpochMicros(INSTANT)).toISOString()).toBe('2024-06-15T12:30:45.123Z');
    expect(microsToDate(EpochMicros(-1)).getTime()).toBe(-1);
  });

  it('refuses_a_date_past_the_microsecond_range_or_invalid', () => {
    expect(() => dateToMicros(new Date(8_640_000_000_000_000))).toThrow(RangeError);
    expect(() => dateToMicros(new Date(Number.NaN))).toThrow(RangeError);
  });
});

describe('dateToMillis / millisToDate round-trip', () => {
  it('preserves exact millisecond timestamp', () => {
    const original = new Date('2024-06-15T12:30:45.123Z');
    expect(millisToDate(dateToMillis(original)).getTime()).toBe(original.getTime());
  });

  it('handles epoch', () => {
    expect(dateToMillis(new Date(0))).toBe(EpochMillis(0));
    expect(millisToDate(EpochMillis(0)).getTime()).toBe(0);
  });
});

describe('nowMicros', () => {
  it('reads_the_wall_clock_as_a_safe_integer_number', () => {
    const before = Date.now() * 1000;
    const us = nowMicros();
    expect(typeof us).toBe('number');
    expect(Number.isSafeInteger(us)).toBe(true);
    expect(us >= before).toBe(true);
    expect(us <= Date.now() * 1000).toBe(true);
  });
});

describe('nowMillis', () => {
  it('returns a number', () => {
    expect(typeof nowMillis()).toBe('number');
  });
});

describe('microsToISOString / microsToISODate', () => {
  it('writes_every_microsecond_digit_of_an_instant', () => {
    expect(microsToISOString(EpochMicros(INSTANT))).toBe('2024-06-15T12:30:45.123456Z');
    expect(microsToISOString(EpochMicros(0))).toBe('1970-01-01T00:00:00.000000Z');
    expect(microsToISOString(EpochMicros(-1))).toBe('1969-12-31T23:59:59.999999Z');
    expect(microsToISOString(EpochMicros(Number.MAX_SAFE_INTEGER))).toBe('2255-06-05T23:47:34.740991Z');
  });

  it('names_the_utc_date_an_instant_lies_in', () => {
    expect(microsToISODate(EpochMicros(INSTANT))).toBe('2024-06-15');
    expect(microsToISODate(EpochMicros(0))).toBe('1970-01-01');
    expect(microsToISODate(EpochMicros(-1))).toBe('1969-12-31');
    expect(microsToISODate(dateToMicros(new Date('2024-12-31T23:59:59.999Z')))).toBe('2024-12-31');
  });
});
//#endregion smoo/lmao!n/schema-time-units
