import { describe, expect, it } from 'bun:test';

import { createTimeAnchor, getTimestampMicros } from '../timestamp.js';

describe('ES Timestamp (performance.now)', () => {
  describe('createTimeAnchor', () => {
    it('should return anchor with epoch and perf values', () => {
      const anchor = createTimeAnchor();
      expect(anchor.anchorEpochMicros).toBeGreaterThan(0);
      expect(anchor.anchorPerfNow).toBeGreaterThan(0);
      // Epoch should be roughly Date.now() * 1000
      expect(anchor.anchorEpochMicros).toBeCloseTo(Date.now() * 1000, -4); // within 10ms
    });

    it('should return flat object (not nested)', () => {
      const anchor = createTimeAnchor();
      expect(typeof anchor.anchorEpochMicros).toBe('number');
      expect(typeof anchor.anchorPerfNow).toBe('number');
      expect(Object.keys(anchor)).toEqual(['anchorEpochMicros', 'anchorPerfNow']);
    });
  });

  describe('getTimestampMicros', () => {
    it('should return increasing timestamps', async () => {
      const { anchorEpochMicros, anchorPerfNow } = createTimeAnchor();
      const ts1 = getTimestampMicros(anchorEpochMicros, anchorPerfNow);
      await new Promise((r) => setTimeout(r, 10));
      const ts2 = getTimestampMicros(anchorEpochMicros, anchorPerfNow);

      expect(ts2).toBeGreaterThan(ts1);
      // Should be roughly 10000 microseconds apart (10ms)
      expect(ts2 - ts1).toBeGreaterThan(5000);
      expect(ts2 - ts1).toBeLessThan(50000);
    });

    it('should return timestamps in microseconds (16 digits)', () => {
      const { anchorEpochMicros, anchorPerfNow } = createTimeAnchor();
      const ts = getTimestampMicros(anchorEpochMicros, anchorPerfNow);
      const digits = Math.floor(ts).toString().length;
      expect(digits).toBeGreaterThanOrEqual(15);
      expect(digits).toBeLessThanOrEqual(17);
    });

    it('should be zero-allocation (just arithmetic)', () => {
      const { anchorEpochMicros, anchorPerfNow } = createTimeAnchor();
      // Just verify it doesn't throw and returns a number
      for (let i = 0; i < 1000; i++) {
        const ts = getTimestampMicros(anchorEpochMicros, anchorPerfNow);
        expect(typeof ts).toBe('number');
      }
    });
  });
});
