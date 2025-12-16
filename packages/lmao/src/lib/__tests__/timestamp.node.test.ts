import { describe, expect, it } from 'bun:test';

import { getTimestampNanos, Nanoseconds } from '../timestamp.node.js';

describe('Node.js Timestamp (process.hrtime.bigint)', () => {
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

    it('should have sub-millisecond precision (nanoseconds)', async () => {
      const timestamps: bigint[] = [];

      // Rapid-fire timestamps
      for (let i = 0; i < 100; i++) {
        timestamps.push(getTimestampNanos());
      }

      // Should see sub-millisecond differences (< 1_000_000 nanoseconds between some)
      // Node.js hrtime has true nanosecond precision
      let hasSubMillisecond = false;
      for (let i = 1; i < timestamps.length; i++) {
        const diff = timestamps[i] - timestamps[i - 1];
        if (diff > 0n && diff < 1_000_000n) {
          hasSubMillisecond = true;
          break;
        }
      }
      expect(hasSubMillisecond).toBe(true);
    });
  });

  describe('Nanoseconds re-export', () => {
    it('should re-export Nanoseconds from arrow-builder', () => {
      expect(typeof Nanoseconds.now).toBe('function');
      expect(typeof Nanoseconds.fromMillis).toBe('function');
      expect(typeof Nanoseconds.toMillis).toBe('function');
    });
  });
});
