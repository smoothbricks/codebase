import { describe, expect, it } from 'bun:test';

import { getTimestampNanos, Nanoseconds } from '../timestamp.node.js';

describe('Node.js Timestamp (process.hrtime.bigint)', () => {
  it('should return a bigint', () => {
    const ts = getTimestampNanos();
    expect(typeof ts).toBe('bigint');
  });

  it('should return increasing timestamps', async () => {
    const ts1 = getTimestampNanos();
    await new Promise((r) => setTimeout(r, 10));
    const ts2 = getTimestampNanos();

    expect(ts2).toBeGreaterThan(ts1);
    // Should be roughly 10ms apart
    const diff = ts2 - ts1;
    expect(diff).toBeGreaterThan(5_000_000n); // > 5ms
    expect(diff).toBeLessThan(50_000_000n); // < 50ms
  });

  it('should be within 1ms of Date.now()', () => {
    // Sample multiple times and check they're all close
    // Node.js anchor may be up to 1ms off due to Date.now() millisecond precision
    for (let i = 0; i < 10; i++) {
      const dateNowMs = Date.now();
      const ts = getTimestampNanos();
      const tsMs = Nanoseconds.toMillis(ts);

      const diffMs = Math.abs(tsMs - dateNowMs);
      expect(diffMs).toBeLessThanOrEqual(1);
    }
  });

  it('should have sub-millisecond precision (true nanoseconds)', () => {
    const timestamps: bigint[] = [];

    // Rapid-fire timestamps
    for (let i = 0; i < 100; i++) {
      timestamps.push(getTimestampNanos());
    }

    // Should see sub-millisecond differences (< 1_000_000 nanoseconds)
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

  it('should set Nanoseconds.now', () => {
    expect(Nanoseconds.now).toBe(getTimestampNanos);
  });
});
