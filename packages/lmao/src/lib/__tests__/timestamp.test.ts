import { describe, expect, it } from 'bun:test';

import { Nanoseconds } from '@smoothbricks/arrow-builder';

describe('Node.js TraceRoot Timestamps (process.hrtime.bigint)', () => {
  it('should return a bigint from getTimestampNanos()', async () => {
    const { createTraceRoot } = await import('../traceRoot.node.js');
    const mockTracer = {
      onTraceStart: () => {},
      onTraceEnd: () => {},
      onSpanStart: () => {},
      onSpanEnd: () => {},
      onStatsWillResetFor: () => {},
      bufferStrategy: {
        createChildSpanBuffer: () => ({}) as any,
        createOverflowBuffer: () => ({}) as any,
      },
    };
    const traceRoot = createTraceRoot('test-trace', mockTracer);

    const ts = traceRoot.getTimestampNanos();
    expect(typeof ts).toBe('bigint');
  });

  it('should return increasing timestamps', async () => {
    const { createTraceRoot } = await import('../traceRoot.node.js');
    const mockTracer = {
      onTraceStart: () => {},
      onTraceEnd: () => {},
      onSpanStart: () => {},
      onSpanEnd: () => {},
      onStatsWillResetFor: () => {},
      bufferStrategy: {
        createChildSpanBuffer: () => ({}) as any,
        createOverflowBuffer: () => ({}) as any,
      },
    };
    const traceRoot = createTraceRoot('test-trace', mockTracer);

    const ts1 = traceRoot.getTimestampNanos();
    await new Promise((r) => setTimeout(r, 10));
    const ts2 = traceRoot.getTimestampNanos();

    expect(ts2).toBeGreaterThan(ts1);
    // Should be roughly 10ms apart
    const diff = ts2 - ts1;
    expect(diff).toBeGreaterThan(5_000_000n); // > 5ms
    expect(diff).toBeLessThan(50_000_000n); // < 50ms
  });

  it('should be within 1ms of Date.now()', async () => {
    const { createTraceRoot } = await import('../traceRoot.node.js');
    const mockTracer = {
      onTraceStart: () => {},
      onTraceEnd: () => {},
      onSpanStart: () => {},
      onSpanEnd: () => {},
      onStatsWillResetFor: () => {},
      bufferStrategy: {
        createChildSpanBuffer: () => ({}) as any,
        createOverflowBuffer: () => ({}) as any,
      },
    };

    // Sample multiple times and check they're all close
    for (let i = 0; i < 10; i++) {
      const traceRoot = createTraceRoot('test-trace', mockTracer);
      const dateNowMs = Date.now();
      const ts = traceRoot.getTimestampNanos();
      const tsMs = Nanoseconds.toMillis(ts);

      const diffMs = Math.abs(tsMs - dateNowMs);
      expect(diffMs).toBeLessThanOrEqual(1);
    }
  });

  it('should have sub-millisecond precision (true nanoseconds)', async () => {
    const { createTraceRoot } = await import('../traceRoot.node.js');
    const mockTracer = {
      onTraceStart: () => {},
      onTraceEnd: () => {},
      onSpanStart: () => {},
      onSpanEnd: () => {},
      onStatsWillResetFor: () => {},
      bufferStrategy: {
        createChildSpanBuffer: () => ({}) as any,
        createOverflowBuffer: () => ({}) as any,
      },
    };
    const traceRoot = createTraceRoot('test-trace', mockTracer);
    const timestamps: bigint[] = [];

    // Rapid-fire timestamps
    for (let i = 0; i < 100; i++) {
      timestamps.push(traceRoot.getTimestampNanos());
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

  it('should preserve nanosecond deltas in JS fallback above Number.MAX_SAFE_INTEGER', async () => {
    const originalHrtimeBigint = process.hrtime.bigint;
    const base = 9_007_199_254_740_993n; // Number.MAX_SAFE_INTEGER + 2
    const sequence = [base, base + 1n, base + 2n];
    let idx = 0;

    process.hrtime.bigint = () => sequence[idx++] ?? sequence[sequence.length - 1];

    try {
      const { createTraceRoot } = await import('../traceRoot.node.js');
      const mockTracer = {
        onTraceStart: () => {},
        onTraceEnd: () => {},
        onSpanStart: () => {},
        onSpanEnd: () => {},
        onStatsWillResetFor: () => {},
        bufferStrategy: {
          createChildSpanBuffer: () => ({}) as any,
          createOverflowBuffer: () => ({}) as any,
        },
      };

      const traceRoot = createTraceRoot('test-trace', mockTracer);
      const ts0 = traceRoot.getTimestampNanos();
      const ts1 = traceRoot.getTimestampNanos();

      expect(ts1 - ts0).toBe(1n);
    } finally {
      process.hrtime.bigint = originalHrtimeBigint;
    }
  });
});

describe('Browser TraceRoot Timestamps (performance.now)', () => {
  it('should return a bigint from getTimestampNanos()', async () => {
    const { createTraceRoot } = await import('../traceRoot.es.js');
    const mockTracer = {
      onTraceStart: () => {},
      onTraceEnd: () => {},
      onSpanStart: () => {},
      onSpanEnd: () => {},
      onStatsWillResetFor: () => {},
      bufferStrategy: {
        createChildSpanBuffer: () => ({}) as any,
        createOverflowBuffer: () => ({}) as any,
      },
    };
    const traceRoot = createTraceRoot('test-trace', mockTracer);

    const ts = traceRoot.getTimestampNanos();
    expect(typeof ts).toBe('bigint');
  });

  it('should return increasing timestamps', async () => {
    const { createTraceRoot } = await import('../traceRoot.es.js');
    const mockTracer = {
      onTraceStart: () => {},
      onTraceEnd: () => {},
      onSpanStart: () => {},
      onSpanEnd: () => {},
      onStatsWillResetFor: () => {},
      bufferStrategy: {
        createChildSpanBuffer: () => ({}) as any,
        createOverflowBuffer: () => ({}) as any,
      },
    };
    const traceRoot = createTraceRoot('test-trace', mockTracer);

    const ts1 = traceRoot.getTimestampNanos();
    await new Promise((r) => setTimeout(r, 10));
    const ts2 = traceRoot.getTimestampNanos();

    expect(ts2).toBeGreaterThan(ts1);
    // Should be roughly 10ms apart
    const diff = ts2 - ts1;
    expect(diff).toBeGreaterThan(5_000_000n); // > 5ms
    expect(diff).toBeLessThan(50_000_000n); // < 50ms
  });

  it('should be within 1ms of Date.now()', async () => {
    const { createTraceRoot } = await import('../traceRoot.es.js');
    const mockTracer = {
      onTraceStart: () => {},
      onTraceEnd: () => {},
      onSpanStart: () => {},
      onSpanEnd: () => {},
      onStatsWillResetFor: () => {},
      bufferStrategy: {
        createChildSpanBuffer: () => ({}) as any,
        createOverflowBuffer: () => ({}) as any,
      },
    };

    // Sample multiple times and check they're all close
    for (let i = 0; i < 10; i++) {
      const traceRoot = createTraceRoot('test-trace', mockTracer);
      const dateNowMs = Date.now();
      const ts = traceRoot.getTimestampNanos();
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

describe('Platform entry points export createTraceRoot', () => {
  it('should export createTraceRoot from /node', async () => {
    const nodeModule = await import('../../node.js');
    expect(nodeModule.createTraceRoot).toBeDefined();
    expect(typeof nodeModule.createTraceRoot).toBe('function');
  });

  it('should export createTraceRoot from /es', async () => {
    const esModule = await import('../../es.js');
    expect(esModule.createTraceRoot).toBeDefined();
    expect(typeof esModule.createTraceRoot).toBe('function');
  });
});
