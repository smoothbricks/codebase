import { describe, expect, it } from 'bun:test';

import { Nanoseconds } from '@smoothbricks/arrow-builder';

// Import browser module at top level to simulate it being cached (like in "Browser Timestamp" tests)
// This tests that the cache-busting fix works even when the module is already cached
await import('../timestamp.js');

describe('Timestamp implementations and Nanoseconds.now assignment', () => {
  it('should set Nanoseconds.now to node implementation when node module is imported', async () => {
    // Dynamically import node module
    const nodeModule = await import('../timestamp.node.js');
    const nodeGetTimestamp = nodeModule.getTimestampNanos;

    // Verify Nanoseconds.now is set to the node implementation
    expect(Nanoseconds.now).toBe(nodeGetTimestamp);

    // Test basic functionality
    const ts = Nanoseconds.now();
    expect(typeof ts).toBe('bigint');
    expect(ts).toBeGreaterThan(0n);
  });

  it('should update Nanoseconds.now to browser implementation when browser module is imported after node', async () => {
    // First import node to set initial state
    // Use cache-busting query parameter to force fresh import (bypasses module cache)
    // This ensures the module's top-level code executes and sets Nanoseconds.now
    const nodeModule = await import(`../timestamp.node.js?v=${Date.now()}`);
    const nodeGetTimestamp = nodeModule.getTimestampNanos;
    expect(Nanoseconds.now).toBe(nodeGetTimestamp);

    // Then import browser - should replace Nanoseconds.now
    // Use cache-busting query parameter to force fresh import (bypasses module cache)
    // This ensures the module's top-level code executes and sets Nanoseconds.now
    // Needed because if the browser module was already imported (e.g., in "Browser Timestamp" tests),
    // the cached version won't re-execute its top-level code
    const browserModule = await import(`../timestamp.js?v=${Date.now()}`);
    const browserGetTimestamp = browserModule.getTimestampNanos;

    // Verify Nanoseconds.now is now set to the browser implementation
    expect(Nanoseconds.now).toBe(browserGetTimestamp);
    expect(Nanoseconds.now).not.toBe(nodeGetTimestamp);

    // Test basic functionality
    const ts = Nanoseconds.now();
    expect(typeof ts).toBe('bigint');
    expect(ts).toBeGreaterThan(0n);
  });
});

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
