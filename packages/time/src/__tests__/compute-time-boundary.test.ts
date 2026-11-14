import { describe, expect, it } from 'bun:test';
import { computeTimeBoundary } from '../compute-time-boundary.js';
import { dateToMicros, microsToISOString } from '../conversions.js';

describe('computeTimeBoundary', () => {
  describe('EOD (end of day)', () => {
    it('returns 23:59:59.999 UTC for current day', () => {
      const now = dateToMicros(new Date('2026-02-06T14:30:00Z'));
      expect(microsToISOString(computeTimeBoundary(now, 'EOD'))).toBe('2026-02-06T23:59:59.999Z');
    });

    it('long form "end of day" works the same', () => {
      const now = dateToMicros(new Date('2026-02-06T14:30:00Z'));
      expect(microsToISOString(computeTimeBoundary(now, 'end of day'))).toBe('2026-02-06T23:59:59.999Z');
    });
  });

  describe('EOW (end of week)', () => {
    it('given Wednesday, returns Sunday 23:59:59.999', () => {
      const now = dateToMicros(new Date('2026-02-04T10:00:00Z'));
      expect(microsToISOString(computeTimeBoundary(now, 'EOW'))).toBe('2026-02-08T23:59:59.999Z');
    });

    it('given Sunday, returns same Sunday 23:59:59.999', () => {
      const now = dateToMicros(new Date('2026-02-08T10:00:00Z'));
      expect(microsToISOString(computeTimeBoundary(now, 'EOW'))).toBe('2026-02-08T23:59:59.999Z');
    });

    it('long form "end of week" works the same', () => {
      const now = dateToMicros(new Date('2026-02-04T10:00:00Z'));
      expect(microsToISOString(computeTimeBoundary(now, 'end of week'))).toBe('2026-02-08T23:59:59.999Z');
    });
  });

  describe('EOM (end of month)', () => {
    it('February 2026 (28 days)', () => {
      const now = dateToMicros(new Date('2026-02-15T10:00:00Z'));
      expect(microsToISOString(computeTimeBoundary(now, 'EOM'))).toBe('2026-02-28T23:59:59.999Z');
    });

    it('February 2024 leap year (29 days)', () => {
      const now = dateToMicros(new Date('2024-02-15T10:00:00Z'));
      expect(microsToISOString(computeTimeBoundary(now, 'EOM'))).toBe('2024-02-29T23:59:59.999Z');
    });

    it('long form "end of month" works the same', () => {
      const now = dateToMicros(new Date('2026-02-15T10:00:00Z'));
      expect(microsToISOString(computeTimeBoundary(now, 'end of month'))).toBe('2026-02-28T23:59:59.999Z');
    });
  });

  describe('EOQ (end of quarter)', () => {
    it('Q1: February -> March 31', () => {
      const now = dateToMicros(new Date('2026-02-15T10:00:00Z'));
      expect(microsToISOString(computeTimeBoundary(now, 'EOQ'))).toBe('2026-03-31T23:59:59.999Z');
    });

    it('Q2: May -> June 30', () => {
      const now = dateToMicros(new Date('2026-05-15T10:00:00Z'));
      expect(microsToISOString(computeTimeBoundary(now, 'EOQ'))).toBe('2026-06-30T23:59:59.999Z');
    });

    it('Q3: August -> September 30', () => {
      const now = dateToMicros(new Date('2026-08-15T10:00:00Z'));
      expect(microsToISOString(computeTimeBoundary(now, 'EOQ'))).toBe('2026-09-30T23:59:59.999Z');
    });

    it('Q4: November -> December 31', () => {
      const now = dateToMicros(new Date('2026-11-15T10:00:00Z'));
      expect(microsToISOString(computeTimeBoundary(now, 'EOQ'))).toBe('2026-12-31T23:59:59.999Z');
    });

    it('long form "end of quarter" works the same', () => {
      const now = dateToMicros(new Date('2026-02-15T10:00:00Z'));
      expect(microsToISOString(computeTimeBoundary(now, 'end of quarter'))).toBe('2026-03-31T23:59:59.999Z');
    });
  });

  describe('EOY (end of year)', () => {
    it('returns December 31 23:59:59.999', () => {
      const now = dateToMicros(new Date('2026-06-15T10:00:00Z'));
      expect(microsToISOString(computeTimeBoundary(now, 'EOY'))).toBe('2026-12-31T23:59:59.999Z');
    });

    it('long form "end of year" works the same', () => {
      const now = dateToMicros(new Date('2026-06-15T10:00:00Z'));
      expect(microsToISOString(computeTimeBoundary(now, 'end of year'))).toBe('2026-12-31T23:59:59.999Z');
    });
  });

  describe('timezone support', () => {
    it('EOD in America/New_York (EST, UTC-5)', () => {
      const now = dateToMicros(new Date('2026-02-06T20:00:00Z'));
      expect(microsToISOString(computeTimeBoundary(now, 'EOD', 'America/New_York'))).toBe('2026-02-07T04:59:59.999Z');
    });

    it('EOD near DST spring-forward boundary (March 2026)', () => {
      const now = dateToMicros(new Date('2026-03-08T10:00:00Z'));
      expect(microsToISOString(computeTimeBoundary(now, 'EOD', 'America/New_York'))).toBe('2026-03-09T03:59:59.999Z');
    });
  });
});
