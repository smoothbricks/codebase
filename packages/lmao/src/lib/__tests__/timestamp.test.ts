import { describe, expect, it } from 'bun:test';

import { Nanoseconds } from '@smoothbricks/arrow-builder';

describe('Node.js Timestamp (process.hrtime.bigint)', () => {
  it('should return a bigint', async () => {
    const nodeModule = await import('../timestamp.node.js');
    const { anchorEpochNanos, anchorPerfNow } = nodeModule.createTimestampAnchor();
    const ts = nodeModule.getTimestampNanos(anchorEpochNanos, anchorPerfNow);
    expect(typeof ts).toBe('bigint');
  });

  it('should return increasing timestamps', async () => {
    const nodeModule = await import('../timestamp.node.js');
    const { anchorEpochNanos, anchorPerfNow } = nodeModule.createTimestampAnchor();

    const ts1 = nodeModule.getTimestampNanos(anchorEpochNanos, anchorPerfNow);
    await new Promise((r) => setTimeout(r, 10));
    const ts2 = nodeModule.getTimestampNanos(anchorEpochNanos, anchorPerfNow);

    expect(ts2).toBeGreaterThan(ts1);
    // Should be roughly 10ms apart
    const diff = ts2 - ts1;
    expect(diff).toBeGreaterThan(5_000_000n); // > 5ms
    expect(diff).toBeLessThan(50_000_000n); // < 50ms
  });

  it('should be within 1ms of Date.now()', async () => {
    const nodeModule = await import('../timestamp.node.js');
    // Sample multiple times and check they're all close
    // Node.js anchor may be up to 1ms off due to Date.now() millisecond precision
    for (let i = 0; i < 10; i++) {
      const { anchorEpochNanos, anchorPerfNow } = nodeModule.createTimestampAnchor();
      const dateNowMs = Date.now();
      const ts = nodeModule.getTimestampNanos(anchorEpochNanos, anchorPerfNow);
      const tsMs = Nanoseconds.toMillis(ts);

      const diffMs = Math.abs(tsMs - dateNowMs);
      expect(diffMs).toBeLessThanOrEqual(1);
    }
  });

  it('should have sub-millisecond precision (true nanoseconds)', async () => {
    const nodeModule = await import('../timestamp.node.js');
    const { anchorEpochNanos, anchorPerfNow } = nodeModule.createTimestampAnchor();
    const timestamps: bigint[] = [];

    // Rapid-fire timestamps
    for (let i = 0; i < 100; i++) {
      timestamps.push(nodeModule.getTimestampNanos(anchorEpochNanos, anchorPerfNow));
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
});

describe('Browser Timestamp (performance.now)', () => {
  it('should return a bigint', async () => {
    const browserModule = await import('../timestamp.js');
    const { anchorEpochNanos, anchorPerfNow } = browserModule.createTimestampAnchor();
    const ts = browserModule.getTimestampNanos(anchorEpochNanos, anchorPerfNow);
    expect(typeof ts).toBe('bigint');
  });

  it('should return increasing timestamps', async () => {
    const browserModule = await import('../timestamp.js');
    const { anchorEpochNanos, anchorPerfNow } = browserModule.createTimestampAnchor();

    const ts1 = browserModule.getTimestampNanos(anchorEpochNanos, anchorPerfNow);
    await new Promise((r) => setTimeout(r, 10));
    const ts2 = browserModule.getTimestampNanos(anchorEpochNanos, anchorPerfNow);

    expect(ts2).toBeGreaterThan(ts1);
    // Should be roughly 10ms apart
    const diff = ts2 - ts1;
    expect(diff).toBeGreaterThan(5_000_000n); // > 5ms
    expect(diff).toBeLessThan(50_000_000n); // < 50ms
  });

  it('should be within 1ms of Date.now()', async () => {
    const browserModule = await import('../timestamp.js');
    // Sample multiple times and check they're all close
    for (let i = 0; i < 10; i++) {
      const { anchorEpochNanos, anchorPerfNow } = browserModule.createTimestampAnchor();
      const dateNowMs = Date.now();
      const ts = browserModule.getTimestampNanos(anchorEpochNanos, anchorPerfNow);
      const tsMs = Nanoseconds.toMillis(ts);

      const diffMs = Math.abs(tsMs - dateNowMs);
      // Browser performance API should be very close to Date.now()
      expect(diffMs).toBeLessThanOrEqual(1);
    }
  });
});

describe('Nanoseconds utilities', () => {
  it('fromMillis converts correctly', () => {
    const ns = Nanoseconds.fromMillis(1000);
    expect(ns).toBe(Nanoseconds.unsafe(1_000_000_000n));
  });

  it('toMillis converts correctly', () => {
    const ns = Nanoseconds.fromMillis(1234);
    expect(Nanoseconds.toMillis(ns)).toBe(1234);
  });

  it('toMicros converts correctly', () => {
    const ns = Nanoseconds.fromMillis(1);
    expect(Nanoseconds.toMicros(ns)).toBe(1000n);
  });

  it('unsafe casts bigint', () => {
    const raw = 123456789n;
    const ns = Nanoseconds.unsafe(raw);
    expect(ns).toBe(Nanoseconds.unsafe(raw));
  });
});
