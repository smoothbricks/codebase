import { describe, expect, it } from 'bun:test';

import { Nanoseconds } from '@smoothbricks/arrow-builder';
import fc from 'fast-check';
import { LOG_STAMP_REFRESH } from '../coarseClock.js';
import { JsBufferStrategy } from '../JsBufferStrategy.js';
import { resolveEntryType } from '../resolveMessage.js';
import { ENTRY_TYPE_INFO, ENTRY_TYPE_SPAN_OK, ENTRY_TYPE_SPAN_START } from '../schema/systemSchema.js';
import { createTraceId } from '../traceId.js';
import { createTraceRoot as createEsTraceRoot, TraceRoot as EsTraceRoot } from '../traceRoot.es.js';
import type { TracerLifecycleHooks } from '../traceRoot.js';
import { createTraceRoot as createNodeTraceRoot, TraceRoot as NodeTraceRoot } from '../traceRoot.node.js';
import type { SpanBuffer } from '../types.js';
import { WallClock } from '../wallClock.js';
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

/** Run `run` with `performance.timeOrigin`, `performance.now()` and `Date.now()` answering as given. */
function withWallClock<T>(clock: { origin: number; now: number; date: () => number }, run: () => T): T {
  const originalDateNow = Date.now;
  const ownOrigin = Object.getOwnPropertyDescriptor(performance, 'timeOrigin');
  Object.defineProperty(performance, 'timeOrigin', { configurable: true, get: () => clock.origin });
  Date.now = clock.date;
  try {
    return withPerformanceNow(() => clock.now, run);
  } finally {
    Date.now = originalDateNow;
    if (ownOrigin === undefined) Reflect.deleteProperty(performance, 'timeOrigin');
    else Object.defineProperty(performance, 'timeOrigin', ownOrigin);
  }
}

/**
 * Run `run` with `process.hrtime.bigint()` answering from `now`. The Node root reads it through the global on every
 * call, so the substitution is the clock the stamps use.
 */
function withHrtime<T>(now: () => bigint, run: () => T): T {
  const original = process.hrtime.bigint;
  process.hrtime.bigint = now;
  try {
    return run();
  } finally {
    process.hrtime.bigint = original;
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

  it('should return increasing timestamps', () => {
    const mockTracer = createMockTracer();
    // The test owns the monotonic clock: it advances exactly 10 ms between the two reads, so the delta is exact.
    let monotonic = 1_000_000_000n;
    withHrtime(
      () => monotonic,
      () => {
        const traceRoot = createNodeTraceRoot('test-trace', mockTracer);
        const ts1 = traceRoot.getTimestampNanos();
        monotonic += 10_000_000n;
        const ts2 = traceRoot.getTimestampNanos();
        expect(ts2 - ts1).toBe(10_000_000n);
      },
    );
  });

  it('stamps the wall-clock millisecond that Date.now() reports', () => {
    const mockTracer = createMockTracer();
    const millisecond = 1_700_000_000_000;
    // Date.now(), timeOrigin, performance.now() and hrtime are injected and advance together by one second per root,
    // so the anchor is the platform estimate (origin + now) and the first stamp is that anchor exactly.
    for (let i = 0; i < 10; i++) {
      const wallMs = millisecond + i * 1_000;
      const monotonic = BigInt(i) * 1_000_000_000n + 5_000_000n;
      const ts = withHrtime(
        () => monotonic,
        () =>
          withWallClock({ origin: millisecond - 1_000, now: 1_000.25 + i * 1_000, date: () => wallMs }, () =>
            createNodeTraceRoot('test-trace', mockTracer).getTimestampNanos(),
          ),
      );
      expect(ts).toBe(Nanoseconds.unsafe(BigInt(wallMs) * 1_000_000n + 250_000n));
    }
  });

  it('should have sub-millisecond precision (true nanoseconds)', () => {
    const mockTracer = createMockTracer();
    // The test owns the monotonic clock and steps it 333_333 ns, a width no millisecond or microsecond grid divides:
    // every delta is exact, so the stamps carry true nanoseconds rather than a coarsened clock.
    let monotonic = 5_000_000n;
    withHrtime(
      () => monotonic,
      () => {
        const traceRoot = createNodeTraceRoot('test-trace', mockTracer);
        const timestamps: bigint[] = [];
        for (let i = 0; i < 100; i++) {
          monotonic += 333_333n;
          timestamps.push(traceRoot.getTimestampNanos());
        }
        for (let i = 1; i < timestamps.length; i++) {
          expect(timestamps[i] - timestamps[i - 1]).toBe(333_333n);
        }
      },
    );
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

  it('should return increasing timestamps', () => {
    const mockTracer = createMockTracer();
    // The test owns performance.now(): it advances exactly 10 ms between the two reads, so the delta is exact.
    let nowMs = 1_000;
    withPerformanceNow(
      () => nowMs,
      () => {
        const traceRoot = createEsTraceRoot('test-trace', mockTracer);
        const ts1 = traceRoot.getTimestampNanos();
        nowMs += 10;
        const ts2 = traceRoot.getTimestampNanos();
        expect(ts2 - ts1).toBe(10_000_000n);
      },
    );
  });

  it('stamps the wall-clock millisecond that Date.now() reports', () => {
    const mockTracer = createMockTracer();
    const millisecond = 1_700_000_000_000;
    // Date.now(), timeOrigin and performance.now() are injected and advance together by one second per root, so the
    // anchor is the platform estimate (origin + now) and the first stamp is that anchor exactly.
    for (let i = 0; i < 10; i++) {
      const wallMs = millisecond + i * 1_000;
      const ts = withWallClock({ origin: millisecond - 1_000, now: 1_000.25 + i * 1_000, date: () => wallMs }, () =>
        createEsTraceRoot('test-trace', mockTracer).getTimestampNanos(),
      );
      expect(ts).toBe(Nanoseconds.unsafe(BigInt(wallMs) * 1_000_000n + 250_000n));
    }
  });
});

describe('Platform timestamp append contract', () => {
  it('anchors public Node and ES factories to the injected Unix epoch wall clock', () => {
    // Date.now(), timeOrigin, performance.now() and hrtime are held by the test, so each factory's anchor and its
    // first stamp are the injected wall clock exactly: origin + now is the epoch millisecond plus 0.25 ms.
    const millisecond = 1_700_000_000_000;
    withHrtime(
      () => 5_000_000n,
      () =>
        withWallClock({ origin: millisecond - 1_000, now: 1_000.25, date: () => millisecond }, () => {
          for (const create of [createNodeTraceRoot, createEsTraceRoot]) {
            const root = create('epoch', createMockTracer());
            const buffer = createMockSpanBuffer();
            root._writeSpanStart(root, buffer, 'epoch');
            expect(buffer.timestamp[0]).toBe(BigInt(millisecond) * 1_000_000n + 250_000n);
          }
        }),
    );
  });

  it('keeps lifecycle appends non-decreasing and boundaries strictly monotonic when the clock stalls or rolls back', () => {
    // Log rows ride the coarse cache, so the row contract is non-decreasing.
    // The two boundaries always read fresh, so they absorb stalls and rollbacks
    // through the guard and stay strictly monotonic — that is the pair
    // execution-duration metrics derive from.
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
        expect(chronological[i]).toBeGreaterThanOrEqual(chronological[i - 1]);
      }
      expect(buffer.timestamp[1]).toBeGreaterThan(buffer.timestamp[0]);
      // Only the two boundaries read the clock.
      expect(index).toBe(2);
    } finally {
      process.hrtime.bigint = original;
    }
  });

  it('advances ES lifecycle boundaries by one microsecond when performance.now stalls or rolls back', () => {
    const epoch = 1_700_000_000_000_000_000n;
    const buffer = createMockSpanBuffer();
    // Two boundary reads: a stalled clock and then a rolled-back one.
    const ticks = [10, 9];
    let index = 0;
    const root = new EsTraceRoot(createTraceId('es-rollback'), epoch, 10, createMockTracer());

    withPerformanceNow(
      () => ticks[index++] ?? ticks[ticks.length - 1],
      () => appendLifecycle(root, buffer),
    );

    expect(buffer.timestamp[0]).toBe(epoch);
    // The log rows share the span-start stamp; completion is bumped one
    // microsecond past it because the clock went backwards.
    expect(buffer.timestamp[2]).toBe(epoch);
    expect(buffer.timestamp[3]).toBe(epoch);
    expect(buffer.timestamp[1]).toBe(epoch + 1_000n);
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
      // Above Number.MAX_SAFE_INTEGER the arithmetic must stay in bigint, so the
      // boundary pair still resolves a 1 ns duration a year after the anchor.
      expect(buffer.timestamp[0]).toBe(epoch + gap);
      expect(buffer.timestamp[1] - buffer.timestamp[0]).toBe(1n);
      // The log rows in between carry the span-start stamp exactly.
      expect(buffer.timestamp[2]).toBe(epoch + gap);
      expect(buffer.timestamp[3]).toBe(epoch + gap);
    } finally {
      process.hrtime.bigint = original;
    }
  });

  it('re-anchors each new trace while an existing trace ignores wall-clock rollback', () => {
    const originalHrtime = process.hrtime.bigint;
    // Each root reads the wall clock before and after its monotonic read.
    const wallTimes = [1_700_000_000_000, 1_700_000_000_000, 1_699_999_000_000, 1_699_999_000_000];
    const ticks = [100n, 110n, 200n, 220n, 120n];
    const firstStart = createMockSpanBuffer();
    const secondStart = createMockSpanBuffer();
    let wallIndex = 0;
    let tickIndex = 0;
    process.hrtime.bigint = () => ticks[tickIndex++] ?? ticks[ticks.length - 1];

    try {
      // The platform's sub-millisecond estimate stays where the first wall clock was: 0.25 ms into its millisecond.
      const [first, second] = withWallClock(
        { origin: wallTimes[0], now: 0.25, date: () => wallTimes[wallIndex++] ?? wallTimes[wallTimes.length - 1] },
        () => {
          const first = createNodeTraceRoot('first-anchor', createMockTracer());
          first._writeSpanStart(first, firstStart, 'first');
          const second = createNodeTraceRoot('second-anchor', createMockTracer());
          second._writeSpanStart(second, secondStart, 'second');
          return [first, second];
        },
      );
      const firstLater = first._timestampNow(first);

      expect(firstStart.timestamp[0]).toBe(BigInt(wallTimes[0]) * 1_000_000n + 250_000n + 10n);
      // The wall clock stepped back 1000 s: the second trace anchors on it, and the estimate it disproves is held
      // inside the millisecond the wall clock names.
      expect(secondStart.timestamp[0]).toBe(BigInt(wallTimes[2]) * 1_000_000n + 999_999n + 20n);
      expect(firstLater).toBeGreaterThan(firstStart.timestamp[0]);
      expect(second.anchorEpochNanos).toBe(BigInt(wallTimes[2]) * 1_000_000n + 999_999n);
    } finally {
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

      expect(Array.from({ length: 4 }, (_, row) => resolveEntryType(esBuffer, row))).toEqual(
        Array.from({ length: 4 }, (_, row) => resolveEntryType(nodeBuffer, row)),
      );
      expect(Array.from(esBuffer.timestamp.slice(0, 4))).toEqual(Array.from(nodeBuffer.timestamp.slice(0, 4)));
      expect(resolveEntryType(nodeBuffer, 0)).toBe(ENTRY_TYPE_SPAN_START);
      expect(resolveEntryType(nodeBuffer, 1)).toBe(ENTRY_TYPE_SPAN_OK);
    } finally {
      process.hrtime.bigint = original;
    }
  });

  it('keeps generated rapid-write sequences non-decreasing with row-bounded staleness', () => {
    fc.assert(
      fc.property(
        fc.array(fc.integer({ min: -5, max: 5 }), { minLength: 4, maxLength: 40 }),
        fc.integer({ min: 1, max: 48 }),
        (steps, logs) => {
          const epoch = 1_700_000_000_000_000_000n;
          const anchor = 1_000_000n;
          let tick = anchor;
          let index = 0;
          const ticks = steps.map((step) => (tick += BigInt(step)));
          // Wide enough that the row count can cross several refresh blocks.
          const buffer = createTestSpanBuffer({}, { capacity: 64 }).spanBuffer;
          const original = process.hrtime.bigint;
          process.hrtime.bigint = () => ticks[index++] ?? ticks[ticks.length - 1];
          try {
            const root = new NodeTraceRoot(
              createTraceId('property'),
              epoch,
              Number(anchor),
              anchor,
              createMockTracer(),
            );
            root._writeSpanStart(root, buffer, 'property');
            for (let i = 0; i < logs; i++) root._appendLogEntry(root, buffer, ENTRY_TYPE_INFO);
            root._writeSpanEnd(root, buffer, ENTRY_TYPE_SPAN_OK);

            // Rows are non-decreasing: a rolled-back clock can never move a row
            // backwards, because rows never read the clock at all except at a
            // refresh, where the boundary guard applies.
            const rows = Array.from(buffer.timestamp.slice(2, 2 + logs));
            for (let i = 1; i < rows.length; i++) expect(rows[i]).toBeGreaterThanOrEqual(rows[i - 1]);

            // Staleness is bounded by rows: at most LOG_STAMP_REFRESH share a stamp.
            let run = 0;
            let previous = rows[0];
            for (const stamp of rows) {
              run = stamp === previous ? run + 1 : 1;
              previous = stamp;
              expect(run).toBeLessThanOrEqual(LOG_STAMP_REFRESH);
            }

            // The boundary pair a duration comes from is strictly increasing.
            expect(buffer.timestamp[1]).toBeGreaterThan(buffer.timestamp[0]);
            expect(rows[0]).toBeGreaterThanOrEqual(buffer.timestamp[0]);
            expect(buffer.timestamp[1]).toBeGreaterThanOrEqual(rows[rows.length - 1]);
          } finally {
            process.hrtime.bigint = original;
          }
        },
      ),
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

describe('Trace root wall-clock anchor', () => {
  const millisecond = 1_700_000_000_000;
  const millisecondNanos = BigInt(millisecond) * 1_000_000n;

  it('anchors at the platform sub-millisecond estimate, not the truncated Date.now() millisecond', () => {
    const anchored = new WallClock().anchor(5_000n, millisecond, millisecond, millisecondNanos + 437_500n);
    expect(anchored).toBe(millisecondNanos + 437_500n);
  });

  it('anchors Node and ES roots on the sub-millisecond wall clock', () => {
    const [node, es] = withWallClock({ origin: millisecond - 1_000, now: 1_000.4375, date: () => millisecond }, () => [
      createNodeTraceRoot('node-sub-ms', createMockTracer()),
      createEsTraceRoot('es-sub-ms', createMockTracer()),
    ]);
    expect(node.anchorEpochNanos).toBe(millisecondNanos + 437_500n);
    expect(es.anchorEpochNanos).toBe(millisecondNanos + 437_500n);
  });

  it('holds an estimate the Date.now() reads disprove inside the millisecond they name', () => {
    // `performance.timeOrigin` measured 200–320 µs early under bun test.
    const early = new WallClock().anchor(0n, millisecond, millisecond, millisecondNanos - 300_000n);
    expect(early).toBe(millisecondNanos);
    const late = new WallClock().anchor(0n, millisecond, millisecond, millisecondNanos + 1_300_000n);
    expect(late).toBe(millisecondNanos + 999_999n);
  });

  it('learns the offset from anchors that fall at different points of a millisecond', () => {
    // The true wall clock is the monotonic clock plus `offset`; there is no platform estimate.
    const offset = millisecondNanos + 400_000n;
    const clock = new WallClock();
    const read = (monotonic: bigint) => {
      const wallMs = Number((monotonic + offset) / 1_000_000n);
      return clock.anchor(monotonic, wallMs, wallMs, undefined) - monotonic;
    };
    // One read leaves the whole millisecond open.
    expect(read(0n)).toBe(millisecondNanos + 499_999n);
    // Reads just past and just before a millisecond boundary close it on the offset, less what the host could have
    // slewed the wall clock in the 1.6 ms between them (500 ppm).
    read(600_000n);
    const learned = read(1_599_999n);
    expect(learned).toBeGreaterThanOrEqual(offset - 800n);
    expect(learned).toBeLessThanOrEqual(offset);
  });

  it('starts over when the wall clock is stepped', () => {
    const clock = new WallClock();
    clock.anchor(0n, millisecond, millisecond, millisecondNanos + 100_000n);
    const stepped = millisecond - 10_000;
    const anchored = clock.anchor(1_000n, stepped, stepped, undefined);
    expect(anchored).toBeGreaterThanOrEqual(BigInt(stepped) * 1_000_000n);
    expect(anchored).toBeLessThan(BigInt(stepped + 1) * 1_000_000n);
  });

  it('anchors each root on the injected sub-millisecond wall clock, in every millisecond', () => {
    // Date.now(), timeOrigin, performance.now() and hrtime are injected. Each root moves the wall clock and the
    // monotonic clocks together by one millisecond, so the bounds stay consistent and the anchor is exactly the
    // platform estimate (origin + now). The sub-millisecond digits k/16 ms are k * 62_500 ns.
    for (let i = 0; i < 64; i++) {
      const wallMs = millisecond + i;
      const k = i % 16;
      const monotonic = BigInt(i) * 1_000_000n + 5_000_000n;
      for (const create of [createNodeTraceRoot, createEsTraceRoot]) {
        const root = withHrtime(
          () => monotonic,
          () =>
            withWallClock({ origin: millisecond - 1_000, now: 1_000 + i + k / 16, date: () => wallMs }, () =>
              create('real-anchor', createMockTracer()),
            ),
        );
        expect(root.anchorEpochNanos).toBe(BigInt(wallMs) * 1_000_000n + BigInt(k) * 62_500n);
      }
    }
  });
});
