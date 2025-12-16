import { describe, expect, it } from 'bun:test';

import { getTimestampNanos, Nanoseconds } from '../timestamp.js';

describe('ES Timestamp (performance.now)', () => {
  describe('getTimestampNanos', () => {
    it('should return a bigint', () => {
      const ts = getTimestampNanos();
      expect(typeof ts).toBe('bigint');
    });

    it('should return increasing timestamps', async () => {
      const ts1 = getTimestampNanos();
      await new Promise((r) => setTimeout(r, 10));
      const ts2 = getTimestampNanos();

      expect(ts2).toBeGreaterThan(ts1);
      // Should be roughly 10_000_000 nanoseconds apart (10ms)
      const diff = ts2 - ts1;
      expect(diff).toBeGreaterThan(5_000_000n); // > 5ms
      expect(diff).toBeLessThan(50_000_000n); // < 50ms
    });

    it('should return timestamps close to epoch time in nanoseconds', () => {
      const ts = getTimestampNanos();
      const expectedNanos = BigInt(Date.now()) * 1_000_000n;
      // Should be within 1 second of Date.now()
      const diff = ts > expectedNanos ? ts - expectedNanos : expectedNanos - ts;
      expect(diff).toBeLessThan(1_000_000_000n); // within 1 second
    });

    it('should be efficient (no allocations beyond BigInt)', () => {
      // Just verify it doesn't throw and returns bigint
      for (let i = 0; i < 1000; i++) {
        const ts = getTimestampNanos();
        expect(typeof ts).toBe('bigint');
      }
    });
  });

  describe('Nanoseconds namespace', () => {
    it('should export now() function', () => {
      const ts = Nanoseconds.now();
      expect(typeof ts).toBe('bigint');
    });

    it('should export fromMillis()', () => {
      const ms = 1000;
      const ns = Nanoseconds.fromMillis(ms);
      expect(ns).toBe(Nanoseconds.unsafe(1_000_000_000n));
    });

    it('should export toMillis()', () => {
      const ns = Nanoseconds.fromMillis(1234);
      const ms = Nanoseconds.toMillis(ns);
      expect(ms).toBe(1234);
    });

    it('should export unsafe()', () => {
      const raw = 123456789n;
      const ns = Nanoseconds.unsafe(raw);
      expect(ns).toBe(Nanoseconds.unsafe(raw));
    });
  });
});
