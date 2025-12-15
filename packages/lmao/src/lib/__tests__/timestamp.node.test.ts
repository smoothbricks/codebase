import { describe, expect, it } from 'bun:test';

import { createTimeAnchor, getTimestampMicros } from '../timestamp.node.js';

describe('Node.js Timestamp (process.hrtime.bigint)', () => {
  describe('createTimeAnchor', () => {
    it('should return anchor with epoch and perf values', () => {
      const anchor = createTimeAnchor();
      expect(anchor.anchorEpochMicros).toBeGreaterThan(0);
      expect(anchor.anchorPerfNow).toBeGreaterThan(0);
      // Epoch should be roughly Date.now() * 1000
      expect(anchor.anchorEpochMicros).toBeCloseTo(Date.now() * 1000, -4);
    });

    it('should return flat object (not nested)', () => {
      const anchor = createTimeAnchor();
      expect(typeof anchor.anchorEpochMicros).toBe('number');
      expect(typeof anchor.anchorPerfNow).toBe('number');
      expect(Object.keys(anchor)).toEqual(['anchorEpochMicros', 'anchorPerfNow']);
    });

    it('should use hrtime (anchorPerfNow should be positive monotonic value)', () => {
      const anchor1 = createTimeAnchor();
      const anchor2 = createTimeAnchor();
      // hrtime is monotonic, so second anchor should have >= perf value
      expect(anchor2.anchorPerfNow).toBeGreaterThanOrEqual(anchor1.anchorPerfNow);
      // Must be positive
      expect(anchor1.anchorPerfNow).toBeGreaterThan(0);
    });
  });

  describe('getTimestampMicros', () => {
    it('should return increasing timestamps', async () => {
      const { anchorEpochMicros, anchorPerfNow } = createTimeAnchor();
      const ts1 = getTimestampMicros(anchorEpochMicros, anchorPerfNow);
      await new Promise((r) => setTimeout(r, 10));
      const ts2 = getTimestampMicros(anchorEpochMicros, anchorPerfNow);

      expect(ts2).toBeGreaterThan(ts1);
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

    it('should have sub-millisecond precision', async () => {
      const { anchorEpochMicros, anchorPerfNow } = createTimeAnchor();
      const timestamps: number[] = [];

      // Rapid-fire timestamps
      for (let i = 0; i < 100; i++) {
        timestamps.push(getTimestampMicros(anchorEpochMicros, anchorPerfNow));
      }

      // Should see sub-millisecond differences (< 1000 microseconds between some)
      let hasSubMillisecond = false;
      for (let i = 1; i < timestamps.length; i++) {
        const diff = timestamps[i] - timestamps[i - 1];
        if (diff > 0 && diff < 1000) {
          hasSubMillisecond = true;
          break;
        }
      }
      expect(hasSubMillisecond).toBe(true);
    });
  });
});
