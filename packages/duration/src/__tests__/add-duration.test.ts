import { describe, expect, it } from 'bun:test';
import { dateToMicros, EpochMicros } from '@smoothbricks/time';
import { addDuration, durationToMicros } from '../add-duration.js';
import { DurationMicros } from '../types.js';

describe('addDuration', () => {
  // Use a fixed base date for consistent testing
  const baseDate = new Date('2024-01-01T00:00:00.000Z');

  describe('basic operations', () => {
    it('adds milliseconds to date', () => {
      const result = addDuration(baseDate, '100 ms');
      expect(result.toISOString()).toBe('2024-01-01T00:00:00.100Z');
    });

    it('adds seconds to date', () => {
      const result = addDuration(baseDate, '30 seconds');
      expect(result.toISOString()).toBe('2024-01-01T00:00:30.000Z');
    });

    it('adds minutes to date', () => {
      const result = addDuration(baseDate, '5 minutes');
      expect(result.toISOString()).toBe('2024-01-01T00:05:00.000Z');
    });

    it('adds hours to date', () => {
      const result = addDuration(baseDate, '2 hours');
      expect(result.toISOString()).toBe('2024-01-01T02:00:00.000Z');
    });

    it('adds days to date', () => {
      const result = addDuration(baseDate, '3 days');
      expect(result.toISOString()).toBe('2024-01-04T00:00:00.000Z');
    });

    it('adds weeks to date', () => {
      const result = addDuration(baseDate, '2 weeks');
      expect(result.toISOString()).toBe('2024-01-15T00:00:00.000Z');
    });
  });

  describe('decimal values', () => {
    it('adds decimal hours', () => {
      const result = addDuration(baseDate, '1.5 hours');
      expect(result.toISOString()).toBe('2024-01-01T01:30:00.000Z');
    });

    it('adds decimal minutes', () => {
      const result = addDuration(baseDate, '2.5 minutes');
      expect(result.toISOString()).toBe('2024-01-01T00:02:30.000Z');
    });
  });

  describe('does not mutate original date', () => {
    it('returns a new Date instance', () => {
      const original = new Date('2024-01-01T00:00:00.000Z');
      const result = addDuration(original, '1 hour');

      expect(result).not.toBe(original);
      expect(original.toISOString()).toBe('2024-01-01T00:00:00.000Z');
      expect(result.toISOString()).toBe('2024-01-01T01:00:00.000Z');
    });
  });

  describe('zero duration', () => {
    it('returns same time for zero duration', () => {
      const result = addDuration(baseDate, '0 seconds');
      expect(result.toISOString()).toBe(baseDate.toISOString());
    });
  });

  describe('error propagation', () => {
    it('throws on invalid duration format', () => {
      expect(() => addDuration(baseDate, 'invalid')).toThrow('Invalid duration format');
    });
  });

  describe('shorthand units', () => {
    it('supports short unit formats', () => {
      expect(addDuration(baseDate, '5m').toISOString()).toBe('2024-01-01T00:05:00.000Z');
      expect(addDuration(baseDate, '2h').toISOString()).toBe('2024-01-01T02:00:00.000Z');
      expect(addDuration(baseDate, '1d').toISOString()).toBe('2024-01-02T00:00:00.000Z');
    });
  });

  //#region smoo/lmao!n/schema-time-units
  describe('EpochMicros clock', () => {
    const base = dateToMicros(baseDate);

    it('adds_minutes_in_the_microsecond_domain', () => {
      expect(addDuration(base, '5 minutes')).toBe(EpochMicros(base + 5 * 60 * 1_000_000));
    });

    it('keeps_the_sub_millisecond_digits', () => {
      const odd = EpochMicros(base + 250);
      expect(addDuration(odd, '1 second')).toBe(EpochMicros(odd + 1_000_000));
    });

    it('scales_fractional_milliseconds_to_whole_micros', () => {
      expect(durationToMicros('0.5 ms')).toBe(DurationMicros(500));
      expect(addDuration(base, '0.5 ms')).toBe(EpochMicros(base + 500));
    });

    it('stays_exact_at_the_top_of_the_range_and_refuses_past_it', () => {
      const last = EpochMicros(Number.MAX_SAFE_INTEGER - 1_000_000);
      expect(addDuration(last, '1 second')).toBe(EpochMicros(Number.MAX_SAFE_INTEGER));
      expect(() => addDuration(last, '2 seconds')).toThrow(RangeError);
    });
  });
});

describe('DurationMicros', () => {
  it('a_microsecond_span_is_a_safe_integer_number', () => {
    const span = durationToMicros('5 minutes');
    expect(typeof span).toBe('number');
    expect(span).toBe(DurationMicros(300_000_000));
  });

  it('refuses_a_value_that_counts_no_whole_microsecond', () => {
    expect(() => DurationMicros(1.5)).toThrow(RangeError);
    expect(() => DurationMicros(Number.NaN)).toThrow(RangeError);
    expect(() => DurationMicros(Number.MAX_SAFE_INTEGER + 1)).toThrow(RangeError);
    expect(Object.is(DurationMicros(-0), 0)).toBe(true);
  });

  it('refuses_a_duration_string_past_the_microsecond_range', () => {
    // 2^53 µs is about 285 years.
    expect(() => durationToMicros('20000 weeks')).toThrow(RangeError);
  });
});
//#endregion smoo/lmao!n/schema-time-units
