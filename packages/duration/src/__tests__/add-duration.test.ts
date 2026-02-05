import { describe, expect, it } from 'bun:test';
import { addDuration } from '../add-duration.js';

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
});
