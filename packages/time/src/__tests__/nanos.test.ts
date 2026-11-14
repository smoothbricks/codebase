import { describe, expect, it } from 'bun:test';

import {
  computeTimeBoundary,
  computeTimeBoundaryNanos,
  dateToNanos,
  EpochMicros,
  EpochMillis,
  EpochNanos,
  EpochSeconds,
  epochMicrosToMillis,
  epochMicrosToNanos,
  epochMillisToNanos,
  epochNanosToMicros,
  epochNanosToMillis,
  nanosToDate,
  nanosToISODate,
  nanosToISOString,
  nowNanos,
} from '../index.js';

// A current instant with all nine fraction digits set: past 2^53, so no number holds it.
const INSTANT = EpochNanos(1_794_601_417_993_000_123n);

describe('EpochNanos', () => {
  it('is exact where a number is not', () => {
    expect(BigInt(Number(INSTANT))).not.toBe(INSTANT);
    expect(epochMicrosToNanos(epochNanosToMicros(INSTANT)) + 123n).toBe(INSTANT);
  });

  it('converts up from coarser units without loss', () => {
    expect(epochMillisToNanos(EpochMillis(1_794_601_417_993))).toBe(EpochNanos(1_794_601_417_993_000_000n));
    expect(epochMicrosToNanos(EpochMicros(1_794_601_417_993_000n))).toBe(EpochNanos(1_794_601_417_993_000_000n));
    expect(dateToNanos(new Date(1_794_601_417_993))).toBe(EpochNanos(1_794_601_417_993_000_000n));
  });

  it('converts down by flooring, before the epoch too', () => {
    expect(epochNanosToMicros(INSTANT)).toBe(EpochMicros(1_794_601_417_993_000n));
    expect(epochNanosToMillis(INSTANT)).toBe(EpochMillis(1_794_601_417_993));
    expect(nanosToDate(INSTANT).getTime()).toBe(1_794_601_417_993);
    // One nanosecond before the epoch lies in the last microsecond and millisecond before it.
    expect(epochNanosToMicros(EpochNanos(-1n))).toBe(EpochMicros(-1n));
    expect(epochNanosToMillis(EpochNanos(-1n))).toBe(EpochMillis(-1));
    expect(epochMicrosToMillis(EpochMicros(-1n))).toBe(EpochMillis(-1));
  });

  it('writes every nanosecond digit of an instant', () => {
    expect(nanosToISOString(INSTANT)).toBe('2026-11-13T20:23:37.993000123Z');
    expect(nanosToISOString(EpochNanos(0n))).toBe('1970-01-01T00:00:00.000000000Z');
    expect(nanosToISOString(EpochNanos(-1n))).toBe('1969-12-31T23:59:59.999999999Z');
    expect(nanosToISODate(INSTANT)).toBe('2026-11-13');
  });

  it('reads the wall clock in the nanosecond domain', () => {
    const before = BigInt(Date.now()) * 1_000_000n;
    const now = nowNanos();
    expect(now >= before).toBe(true);
    expect(now <= BigInt(Date.now()) * 1_000_000n).toBe(true);
  });

  it('computes the same civil boundary as the microsecond clock', () => {
    const micros = epochNanosToMicros(INSTANT);
    for (const boundary of ['EOD', 'EOW', 'EOM', 'EOQ', 'EOY'] as const) {
      for (const timezone of [undefined, 'America/New_York']) {
        expect(computeTimeBoundaryNanos(INSTANT, boundary, timezone)).toBe(
          epochMicrosToNanos(computeTimeBoundary(micros, boundary, timezone)),
        );
      }
    }
  });
});

describe('unit brands refuse each other', () => {
  it('holds by type', () => {
    const nanos: EpochNanos = INSTANT;
    // @ts-expect-error a microsecond field takes no nanosecond value
    const asMicros: EpochMicros = nanos;
    // @ts-expect-error a nanosecond field takes no microsecond value
    const asNanos: EpochNanos = EpochMicros(1n);
    // @ts-expect-error a nanosecond field takes no bare bigint
    const bare: EpochNanos = 1n;
    expect([asMicros, asNanos, bare].length).toBe(3);
  });
});

describe('EpochSeconds', () => {
  it('is its own unit', () => {
    const seconds: EpochSeconds = EpochSeconds(1_794_601_417);
    // @ts-expect-error a seconds field takes no millisecond value
    const fromMillis: EpochSeconds = EpochMillis(1_794_601_417_993);
    expect([seconds, fromMillis].length).toBe(2);
  });
});
