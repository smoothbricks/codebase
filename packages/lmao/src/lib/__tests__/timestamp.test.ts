import { describe, expect, it } from 'bun:test';

import { getTimestampNanos, Nanoseconds } from '../timestamp.js';

describe('Browser Timestamp (performance.now)', () => {
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
    for (let i = 0; i < 10; i++) {
      const dateNowMs = Date.now();
      const ts = getTimestampNanos();
      const tsMs = Nanoseconds.toMillis(ts);

      const diffMs = Math.abs(tsMs - dateNowMs);
      // Browser performance API should be very close to Date.now()
      expect(diffMs).toBeLessThanOrEqual(1);
    }
  });

  it('should set Nanoseconds.now', () => {
    // Verify that Nanoseconds.now is set to the browser's getTimestampNanos function
    // This ensures the browser timestamp implementation is correctly assigned
    // Force module evaluation by calling getTimestampNanos to ensure assignment happened
    getTimestampNanos();
    expect(Nanoseconds.now).toBe(getTimestampNanos);
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
