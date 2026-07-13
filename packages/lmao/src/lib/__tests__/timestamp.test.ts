import { describe, expect, it } from 'bun:test';

import { Nanoseconds } from '@smoothbricks/arrow-builder';
import fc from 'fast-check';
import { JsBufferStrategy } from '../JsBufferStrategy.js';
import { ENTRY_TYPE_INFO, ENTRY_TYPE_SPAN_OK, ENTRY_TYPE_SPAN_START } from '../schema/systemSchema.js';
import { createTraceId } from '../traceId.js';
import { createTraceRoot as createEsTraceRoot, TraceRoot as EsTraceRoot } from '../traceRoot.es.js';
import type { TracerLifecycleHooks } from '../traceRoot.js';
import { createTraceRoot as createNodeTraceRoot, TraceRoot as NodeTraceRoot } from '../traceRoot.node.js';
import type { SpanBuffer } from '../types.js';
import { createTestSpanBuffer } from './test-helpers.js';

const mockBuffer = createTestSpanBuffer({}).spanBuffer;
type MockLogSchema = (typeof mockBuffer)['_logSchema'];

function createMockSpanBuffer(): SpanBuffer<MockLogSchema> {
  return createTestSpanBuffer({}).spanBuffer;
}

function createMockTracer(): TracerLifecycleHooks<MockLogSchema> {
  return {
    onTraceStart: () => {},
    onTraceEnd: () => {},
    onSpanStart: () => {},
    onSpanEnd: () => {},
    onStatsWillResetFor: () => {},
    getFlagEvaluatorForContext: () => undefined,
    bufferStrategy: new JsBufferStrategy<MockLogSchema>(),
  };
}

function withPerformanceNow<T>(now: () => number, run: () => T): T {
  const original = performance.now;
  Object.defineProperty(performance, 'now', { configurable: true, value: now });
  try {
    return run();
  } finally {
    Object.defineProperty(performance, 'now', { configurable: true, value: original });
  }
}

function appendLifecycle(root: NodeTraceRoot | EsTraceRoot, buffer: SpanBuffer<MockLogSchema>): void {
  root._writeSpanStart(root, buffer, 'timestamp-contract');
  root._appendLogEntry(root, buffer, ENTRY_TYPE_INFO);
  root._appendLogEntry(root, buffer, ENTRY_TYPE_INFO);
  root._writeSpanEnd(root, buffer, ENTRY_TYPE_SPAN_OK);
}

describe('Node.js TraceRoot Timestamps (process.hrtime.bigint)', () => {
  it('should return a bigint from getTimestampNanos()', async () => {
    const { createTraceRoot } = await import('../traceRoot.node.js');
    const mockTracer = createMockTracer();
    const traceRoot = createTraceRoot('test-trace', mockTracer);

    const ts = traceRoot.getTimestampNanos();
    expect(typeof ts).toBe('bigint');
  });

  it('should return increasing timestamps', async () => {
    const { createTraceRoot } = await import('../traceRoot.node.js');
    const mockTracer = createMockTracer();
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
    const mockTracer = createMockTracer();

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
    const mockTracer = createMockTracer();
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
      const mockTracer = createMockTracer();

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
    const mockTracer = createMockTracer();
    const traceRoot = createTraceRoot('test-trace', mockTracer);

    const ts = traceRoot.getTimestampNanos();
    expect(typeof ts).toBe('bigint');
  });

  it('should return increasing timestamps', async () => {
    const { createTraceRoot } = await import('../traceRoot.es.js');
    const mockTracer = createMockTracer();
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
    const mockTracer = createMockTracer();

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

describe('Platform timestamp append contract', () => {
  it('anchors public Node and ES factories to the Unix epoch without exact wall-clock equality', () => {
    const before = BigInt(Date.now()) * 1_000_000n;
    const node = createNodeTraceRoot('node-epoch', createMockTracer());
    const es = createEsTraceRoot('es-epoch', createMockTracer());
    const after = BigInt(Date.now()) * 1_000_000n;
    const tolerance = 50_000_000n;

    for (const root of [node, es]) {
      const buffer = createMockSpanBuffer();
      root._writeSpanStart(root, buffer, 'epoch');
      expect(buffer.timestamp[0]).toBeGreaterThanOrEqual(before - tolerance);
      expect(buffer.timestamp[0]).toBeLessThanOrEqual(after + tolerance);
    }
  });

  it('keeps lifecycle appends strictly monotonic when the wrapped clock stalls or rolls back', () => {
    const epoch = 1_700_000_000_000_000_000n;
    const anchor = 10_000_000n;
    const ticks = [anchor, anchor, anchor - 50n, anchor + 1n, anchor - 1_000n];
    const buffer = createMockSpanBuffer();
    let index = 0;
    const original = process.hrtime.bigint;
    process.hrtime.bigint = () => ticks[index++] ?? ticks[ticks.length - 1];

    try {
      const root = new NodeTraceRoot(createTraceId('rollback'), epoch, Number(anchor), anchor, createMockTracer());
      appendLifecycle(root, buffer);
      const chronological = [buffer.timestamp[0], buffer.timestamp[2], buffer.timestamp[3], buffer.timestamp[1]];
      for (let i = 1; i < chronological.length; i++) {
        expect(chronological[i]).toBeGreaterThan(chronological[i - 1]);
      }
    } finally {
      process.hrtime.bigint = original;
    }
  });

  it('advances ES lifecycle appends by one microsecond when performance.now stalls or rolls back', () => {
    const epoch = 1_700_000_000_000_000_000n;
    const buffer = createMockSpanBuffer();
    const ticks = [10, 10, 9, 10.0001];
    let index = 0;
    const root = new EsTraceRoot(createTraceId('es-rollback'), epoch, 10, createMockTracer());

    withPerformanceNow(
      () => ticks[index++] ?? ticks[ticks.length - 1],
      () => appendLifecycle(root, buffer),
    );

    const chronological = [buffer.timestamp[0], buffer.timestamp[2], buffer.timestamp[3], buffer.timestamp[1]];
    expect(chronological[0]).toBe(epoch);
    for (let i = 1; i < chronological.length; i++) {
      expect(chronological[i] - chronological[i - 1]).toBe(1_000n);
    }
  });

  it('preserves exact nanosecond deltas after a long Node monotonic-clock gap', () => {
    const epoch = 1_700_000_000_000_000_000n;
    const anchor = 9_007_199_254_740_993n;
    const gap = 365n * 24n * 60n * 60n * 1_000_000_000n;
    const ticks = [anchor + gap, anchor + gap + 1n, anchor + gap + 2n, anchor + gap + 3n];
    const buffer = createMockSpanBuffer();
    let index = 0;
    const original = process.hrtime.bigint;
    process.hrtime.bigint = () => ticks[index++] ?? ticks[ticks.length - 1];

    try {
      const root = new NodeTraceRoot(createTraceId('long-gap'), epoch, Number(anchor), anchor, createMockTracer());
      appendLifecycle(root, buffer);
      expect(buffer.timestamp[0]).toBe(epoch + gap);
      expect(buffer.timestamp[2] - buffer.timestamp[0]).toBe(1n);
      expect(buffer.timestamp[3] - buffer.timestamp[2]).toBe(1n);
      expect(buffer.timestamp[1] - buffer.timestamp[3]).toBe(1n);
    } finally {
      process.hrtime.bigint = original;
    }
  });

  it('re-anchors each new trace while an existing trace ignores wall-clock rollback', () => {
    const originalDateNow = Date.now;
    const originalHrtime = process.hrtime.bigint;
    const wallTimes = [1_700_000_000_000, 1_699_999_000_000];
    const ticks = [100n, 110n, 200n, 220n, 120n];
    const firstStart = createMockSpanBuffer();
    const secondStart = createMockSpanBuffer();
    let wallIndex = 0;
    let tickIndex = 0;
    Date.now = () => wallTimes[wallIndex++] ?? wallTimes[wallTimes.length - 1];
    process.hrtime.bigint = () => ticks[tickIndex++] ?? ticks[ticks.length - 1];

    try {
      const first = createNodeTraceRoot('first-anchor', createMockTracer());
      first._writeSpanStart(first, firstStart, 'first');
      const second = createNodeTraceRoot('second-anchor', createMockTracer());
      second._writeSpanStart(second, secondStart, 'second');
      const firstLater = first._timestampNow(first);

      expect(firstStart.timestamp[0]).toBe(BigInt(wallTimes[0]) * 1_000_000n + 10n);
      expect(secondStart.timestamp[0]).toBe(BigInt(wallTimes[1]) * 1_000_000n + 20n);
      expect(firstLater).toBeGreaterThan(firstStart.timestamp[0]);
    } finally {
      Date.now = originalDateNow;
      process.hrtime.bigint = originalHrtime;
    }
  });

  it('keeps Node and ES lifecycle output equivalent at shared microsecond ticks', () => {
    const epoch = 1_700_000_000_000_000_000n;
    const nodeAnchor = 5_000_000n;
    const elapsedMicros = [0, 1, 2, 3];
    const nodeBuffer = createMockSpanBuffer();
    const esBuffer = createMockSpanBuffer();
    let nodeIndex = 0;
    const original = process.hrtime.bigint;
    process.hrtime.bigint = () => nodeAnchor + BigInt(elapsedMicros[nodeIndex++] ?? 3) * 1_000n;

    try {
      const node = new NodeTraceRoot(
        createTraceId('node-parity'),
        epoch,
        Number(nodeAnchor),
        nodeAnchor,
        createMockTracer(),
      );
      let esIndex = 0;
      const es = new EsTraceRoot(createTraceId('es-parity'), epoch, 10, createMockTracer());
      appendLifecycle(node, nodeBuffer);
      withPerformanceNow(
        () => 10 + (elapsedMicros[esIndex++] ?? 3) / 1_000,
        () => appendLifecycle(es, esBuffer),
      );

      expect(Array.from(esBuffer.entry_type.slice(0, 4))).toEqual(Array.from(nodeBuffer.entry_type.slice(0, 4)));
      expect(Array.from(esBuffer.timestamp.slice(0, 4))).toEqual(Array.from(nodeBuffer.timestamp.slice(0, 4)));
      expect(nodeBuffer.entry_type[0]).toBe(ENTRY_TYPE_SPAN_START);
      expect(nodeBuffer.entry_type[1]).toBe(ENTRY_TYPE_SPAN_OK);
    } finally {
      process.hrtime.bigint = original;
    }
  });

  it('keeps generated rapid-write sequences strictly monotonic through the real append primitive', () => {
    fc.assert(
      fc.property(fc.array(fc.integer({ min: -5, max: 5 }), { minLength: 4, maxLength: 40 }), (steps) => {
        const epoch = 1_700_000_000_000_000_000n;
        const anchor = 1_000_000n;
        let tick = anchor;
        let index = 0;
        const ticks = steps.map((step) => (tick += BigInt(step)));
        const buffer = createMockSpanBuffer();
        const original = process.hrtime.bigint;
        process.hrtime.bigint = () => ticks[index++] ?? ticks[ticks.length - 1];
        try {
          const root = new NodeTraceRoot(createTraceId('property'), epoch, Number(anchor), anchor, createMockTracer());
          root._writeSpanStart(root, buffer, 'property');
          for (let i = 1; i < steps.length; i++) root._appendLogEntry(root, buffer, ENTRY_TYPE_INFO);
          const written = Array.from(buffer.timestamp.slice(0, buffer._writeIndex)).filter((value) => value !== 0n);
          for (let i = 1; i < written.length; i++) expect(written[i]).toBeGreaterThan(written[i - 1]);
        } finally {
          process.hrtime.bigint = original;
        }
      }),
      { numRuns: 80 },
    );
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
