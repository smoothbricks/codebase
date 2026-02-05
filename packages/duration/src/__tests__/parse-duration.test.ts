import { describe, expect, it } from 'bun:test';
import { parseDuration } from '../parse-duration.js';

describe('parseDuration', () => {
  describe('milliseconds', () => {
    it('parses "ms" unit', () => {
      expect(parseDuration('100 ms')).toBe(100);
      expect(parseDuration('100ms')).toBe(100);
    });

    it('parses "millisecond" unit', () => {
      expect(parseDuration('1 millisecond')).toBe(1);
    });

    it('parses "milliseconds" unit', () => {
      expect(parseDuration('500 milliseconds')).toBe(500);
    });
  });

  describe('seconds', () => {
    it('parses "s" unit', () => {
      expect(parseDuration('5 s')).toBe(5000);
      expect(parseDuration('5s')).toBe(5000);
    });

    it('parses "second" unit', () => {
      expect(parseDuration('1 second')).toBe(1000);
    });

    it('parses "seconds" unit', () => {
      expect(parseDuration('30 seconds')).toBe(30000);
    });
  });

  describe('minutes', () => {
    it('parses "m" unit', () => {
      expect(parseDuration('5 m')).toBe(300000);
      expect(parseDuration('5m')).toBe(300000);
    });

    it('parses "min" unit', () => {
      expect(parseDuration('5 min')).toBe(300000);
    });

    it('parses "minute" unit', () => {
      expect(parseDuration('1 minute')).toBe(60000);
    });

    it('parses "minutes" unit', () => {
      expect(parseDuration('5 minutes')).toBe(300000);
    });
  });

  describe('hours', () => {
    it('parses "h" unit', () => {
      expect(parseDuration('2 h')).toBe(7200000);
      expect(parseDuration('2h')).toBe(7200000);
    });

    it('parses "hour" unit', () => {
      expect(parseDuration('1 hour')).toBe(3600000);
    });

    it('parses "hours" unit', () => {
      expect(parseDuration('24 hours')).toBe(86400000);
    });
  });

  describe('days', () => {
    it('parses "d" unit', () => {
      expect(parseDuration('1 d')).toBe(86400000);
      expect(parseDuration('1d')).toBe(86400000);
    });

    it('parses "day" unit', () => {
      expect(parseDuration('1 day')).toBe(86400000);
    });

    it('parses "days" unit', () => {
      expect(parseDuration('7 days')).toBe(604800000);
    });
  });

  describe('weeks', () => {
    it('parses "w" unit', () => {
      expect(parseDuration('1 w')).toBe(604800000);
      expect(parseDuration('1w')).toBe(604800000);
    });

    it('parses "week" unit', () => {
      expect(parseDuration('1 week')).toBe(604800000);
    });

    it('parses "weeks" unit', () => {
      expect(parseDuration('2 weeks')).toBe(1209600000);
    });
  });

  describe('decimal values', () => {
    it('parses decimal hours', () => {
      expect(parseDuration('1.5 hours')).toBe(5400000);
    });

    it('parses decimal minutes', () => {
      expect(parseDuration('2.5 minutes')).toBe(150000);
    });

    it('parses decimal seconds', () => {
      expect(parseDuration('0.5 seconds')).toBe(500);
    });
  });

  describe('case insensitivity', () => {
    it('handles uppercase units', () => {
      expect(parseDuration('5 MINUTES')).toBe(300000);
      expect(parseDuration('24 HOURS')).toBe(86400000);
    });

    it('handles mixed case units', () => {
      expect(parseDuration('5 Minutes')).toBe(300000);
      expect(parseDuration('24 Hours')).toBe(86400000);
    });
  });

  describe('zero values', () => {
    it('parses zero duration', () => {
      expect(parseDuration('0 seconds')).toBe(0);
      expect(parseDuration('0 minutes')).toBe(0);
      expect(parseDuration('0 hours')).toBe(0);
    });
  });

  describe('error handling', () => {
    it('throws on invalid format', () => {
      expect(() => parseDuration('five minutes')).toThrow('Invalid duration format');
      expect(() => parseDuration('minutes 5')).toThrow('Invalid duration format');
      expect(() => parseDuration('')).toThrow('Invalid duration format');
      expect(() => parseDuration('5')).toThrow('Invalid duration format');
    });

    it('throws on unknown unit', () => {
      // Unknown units won't match the regex, so they throw invalid format
      expect(() => parseDuration('5 years')).toThrow('Invalid duration format');
      expect(() => parseDuration('5 months')).toThrow('Invalid duration format');
    });

    it('throws on negative values', () => {
      // Negative values won't match the regex (no minus sign in pattern)
      expect(() => parseDuration('-5 minutes')).toThrow('Invalid duration format');
    });
  });
});
