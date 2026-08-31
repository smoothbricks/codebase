/**
 * Row-stamp coarsening contract.
 *
 * The property most likely to break silently is that a span BOUNDARY still
 * reads the clock fresh. If a refactor routes `_writeSpanStart`/`_writeSpanEnd`
 * through the row cache, every log row keeps working, every ordering assertion
 * keeps passing, and `axe_execution_duration_seconds` quietly reports zero for
 * every span short enough to fit inside one refresh block. These tests exist to
 * make that mistake loud.
 */

import { describe, expect, it } from 'bun:test';
import { LOG_STAMP_REFRESH } from '../coarseClock.js';
import { JsBufferStrategy } from '../JsBufferStrategy.js';
import { ENTRY_TYPE_INFO, ENTRY_TYPE_SPAN_OK } from '../schema/systemSchema.js';
import { createTraceId } from '../traceId.js';
import { TraceRoot as EsTraceRoot } from '../traceRoot.es.js';
import type { TracerLifecycleHooks } from '../traceRoot.js';
import { TraceRoot as NodeTraceRoot } from '../traceRoot.node.js';
import type { SpanBuffer } from '../types.js';
import { createTestSpanBuffer } from './test-helpers.js';

const mockBuffer = createTestSpanBuffer({}).spanBuffer;
type MockLogSchema = (typeof mockBuffer)['_logSchema'];

/** Wide enough for a whole refresh block plus both boundary rows. */
const WIDE_CAPACITY = 64;

function createBuffer(capacity = WIDE_CAPACITY): SpanBuffer<MockLogSchema> {
  return createTestSpanBuffer({}, { capacity }).spanBuffer;
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

const EPOCH = 1_700_000_000_000_000_000n;
const ANCHOR = 1_000_000n;

interface ClockProbe {
  /** Every value the substituted clock handed out, in order. */
  readonly reads: bigint[];
}

/**
 * Substitute `process.hrtime.bigint` with a clock that advances a fixed step per
 * read and records every read. The count of recorded reads is the assertion that
 * matters most: a cached row performs no clock read, so it constructs no BigInt
 * cell and allocates nothing — the per-row allocation claim reduces to this
 * count.
 */
function withNodeClock<T>(step: bigint, run: (probe: ClockProbe) => T): T {
  const reads: bigint[] = [];
  let tick = ANCHOR;
  const original = process.hrtime.bigint;
  process.hrtime.bigint = () => {
    tick += step;
    reads.push(tick);
    return tick;
  };
  try {
    return run({ reads });
  } finally {
    process.hrtime.bigint = original;
  }
}

function createNodeRoot(): NodeTraceRoot<MockLogSchema> {
  return new NodeTraceRoot(createTraceId('coarse'), EPOCH, Number(ANCHOR), ANCHOR, createMockTracer());
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

describe('coarse row stamps — span boundary precision', () => {
  it('keeps a span duration exact when the whole span fits inside one refresh block', () => {
    // The span's real duration is one clock step. A coarsened completion would
    // reuse the start stamp and report a duration of zero.
    const STEP = 137n;
    const buffer = createBuffer();
    const reads = withNodeClock(STEP, (probe) => {
      const root = createNodeRoot();
      root._writeSpanStart(root, buffer, 'tiny');
      for (let i = 0; i < LOG_STAMP_REFRESH / 2; i++) root._appendLogEntry(root, buffer, ENTRY_TYPE_INFO);
      root._writeSpanEnd(root, buffer, ENTRY_TYPE_SPAN_OK);
      return probe.reads;
    });

    // Exactly two clock reads: the two boundaries. Every log row rode the cache.
    expect(reads).toHaveLength(2);
    expect(buffer.timestamp[1] - buffer.timestamp[0]).toBe(STEP);
  });

  it('keeps a span duration exact when the span straddles a refresh boundary', () => {
    const STEP = 4_096n;
    const logs = LOG_STAMP_REFRESH + 4;
    const buffer = createBuffer();
    const reads = withNodeClock(STEP, (probe) => {
      const root = createNodeRoot();
      root._writeSpanStart(root, buffer, 'straddle');
      for (let i = 0; i < logs; i++) root._appendLogEntry(root, buffer, ENTRY_TYPE_INFO);
      root._writeSpanEnd(root, buffer, ENTRY_TYPE_SPAN_OK);
      return probe.reads;
    });

    // start, one mid-span refresh, completion.
    expect(reads).toHaveLength(3);
    // The duration is start-to-completion, unaffected by the refresh in between.
    expect(buffer.timestamp[1] - buffer.timestamp[0]).toBe(STEP * 2n);

    // The first LOG_STAMP_REFRESH rows share the start stamp; the rows after the
    // refresh share the refreshed one. Row 0 is the span start, row 1 completion.
    const rows = Array.from(buffer.timestamp.slice(2, 2 + logs));
    const firstBlock = rows.slice(0, LOG_STAMP_REFRESH);
    const secondBlock = rows.slice(LOG_STAMP_REFRESH);
    expect(new Set(firstBlock).size).toBe(1);
    expect(new Set(secondBlock).size).toBe(1);
    expect(firstBlock[0]).toBe(buffer.timestamp[0]);
    expect(secondBlock[0]).toBeGreaterThan(firstBlock[0]);
    expect(secondBlock).toHaveLength(4);
  });

  it('bounds staleness by rows, never by wall time', () => {
    // A stalled lane must not accumulate stale stamps in proportion to how long
    // it stalled: the bound is LOG_STAMP_REFRESH rows regardless of the step.
    const buffer = createBuffer();
    const logs = LOG_STAMP_REFRESH * 3;
    const reads = withNodeClock(1_000_000_000n, (probe) => {
      const root = createNodeRoot();
      root._writeSpanStart(root, buffer, 'stall');
      for (let i = 0; i < logs; i++) root._appendLogEntry(root, buffer, ENTRY_TYPE_INFO);
      root._writeSpanEnd(root, buffer, ENTRY_TYPE_SPAN_OK);
      return probe.reads;
    });

    // One read per boundary plus one per completed block of LOG_STAMP_REFRESH rows.
    expect(reads).toHaveLength(2 + logs / LOG_STAMP_REFRESH - 1);

    const rows = Array.from(buffer.timestamp.slice(2, 2 + logs));
    let rowsSinceRead = 0;
    let previous = rows[0];
    for (const stamp of rows) {
      rowsSinceRead = stamp === previous ? rowsSinceRead + 1 : 1;
      previous = stamp;
      expect(rowsSinceRead).toBeLessThanOrEqual(LOG_STAMP_REFRESH);
    }
  });

  it('performs no clock read at all on a cached row, so a cached row allocates nothing', () => {
    const buffer = createBuffer();
    const reads = withNodeClock(1n, (probe) => {
      const root = createNodeRoot();
      root._writeSpanStart(root, buffer, 'alloc');
      const afterStart = probe.reads.length;
      for (let i = 0; i < LOG_STAMP_REFRESH - 1; i++) root._appendLogEntry(root, buffer, ENTRY_TYPE_INFO);
      expect(probe.reads.length).toBe(afterStart);
      return probe.reads;
    });
    expect(reads).toHaveLength(1);
  });

  it('forces a fresh read for the first log row of a trace rather than stamping a stale zero', () => {
    const buffer = createBuffer();
    withNodeClock(11n, (probe) => {
      const root = createNodeRoot();
      // No span start: the very first thing this trace does is append a log row.
      root._appendLogEntry(root, buffer, ENTRY_TYPE_INFO);
      expect(probe.reads).toHaveLength(1);
      expect(buffer.timestamp[0]).toBeGreaterThan(0n);
    });
  });

  it('keeps rows non-decreasing and boundaries strictly increasing when the clock rolls back', () => {
    // Stalls and rollbacks are absorbed by the boundary guard. Rows share a
    // stamp by design, so the row contract is non-decreasing, not strict.
    const buffer = createBuffer();
    const ticks = [ANCHOR, ANCHOR, ANCHOR - 50n, ANCHOR + 1n, ANCHOR - 1_000n];
    let index = 0;
    const original = process.hrtime.bigint;
    process.hrtime.bigint = () => ticks[index++] ?? ticks[ticks.length - 1];
    try {
      const root = createNodeRoot();
      root._writeSpanStart(root, buffer, 'rollback');
      root._appendLogEntry(root, buffer, ENTRY_TYPE_INFO);
      root._appendLogEntry(root, buffer, ENTRY_TYPE_INFO);
      root._writeSpanEnd(root, buffer, ENTRY_TYPE_SPAN_OK);

      const rows = [buffer.timestamp[0], buffer.timestamp[2], buffer.timestamp[3], buffer.timestamp[1]];
      for (let i = 1; i < rows.length; i++) expect(rows[i]).toBeGreaterThanOrEqual(rows[i - 1]);
      expect(buffer.timestamp[1]).toBeGreaterThan(buffer.timestamp[0]);
    } finally {
      process.hrtime.bigint = original;
    }
  });

  it('stays fully determined by a substituted clock', () => {
    const run = (): bigint[] => {
      const buffer = createBuffer();
      return withNodeClock(7n, () => {
        const root = createNodeRoot();
        root._writeSpanStart(root, buffer, 'deterministic');
        for (let i = 0; i < LOG_STAMP_REFRESH + 3; i++) root._appendLogEntry(root, buffer, ENTRY_TYPE_INFO);
        root._writeSpanEnd(root, buffer, ENTRY_TYPE_SPAN_OK);
        return Array.from(buffer.timestamp.slice(0, LOG_STAMP_REFRESH + 5));
      });
    };
    expect(run()).toEqual(run());
  });
});

describe('coarse row stamps — ES lane', () => {
  // The substituted `performance.now` returns a value the test advances by hand
  // rather than one that steps per call: the bun test harness reads
  // `performance.now` itself, and a per-call step would silently attribute the
  // harness's reads to the lane under test.
  it('keeps a span duration exact when the whole span fits inside one refresh block', () => {
    const buffer = createBuffer();
    const root = new EsTraceRoot(createTraceId('coarse-es'), EPOCH, 10, createMockTracer());
    let nowMillis = 10;
    withPerformanceNow(
      () => nowMillis,
      () => {
        root._writeSpanStart(root, buffer, 'tiny');
        // Advance far more than the span's real duration. If a log row read the
        // clock the row stamps would jump; they must not.
        nowMillis = 60;
        for (let i = 0; i < LOG_STAMP_REFRESH / 2; i++) root._appendLogEntry(root, buffer, ENTRY_TYPE_INFO);
        nowMillis = 10.137;
        root._writeSpanEnd(root, buffer, ENTRY_TYPE_SPAN_OK);
      },
    );

    expect(buffer.timestamp[0]).toBe(EPOCH);
    // 137 microseconds, the ES lane's resolution, not the 50ms the rows saw.
    expect(buffer.timestamp[1] - buffer.timestamp[0]).toBe(137_000n);
    const rows = Array.from(buffer.timestamp.slice(2, 2 + LOG_STAMP_REFRESH / 2));
    expect(new Set(rows)).toEqual(new Set([buffer.timestamp[0]]));
  });

  it('refreshes mid-span and keeps the duration start-to-completion', () => {
    const buffer = createBuffer();
    const root = new EsTraceRoot(createTraceId('straddle-es'), EPOCH, 10, createMockTracer());
    const logs = LOG_STAMP_REFRESH + 4;
    let nowMillis = 10;
    withPerformanceNow(
      () => nowMillis,
      () => {
        root._writeSpanStart(root, buffer, 'straddle');
        nowMillis = 10.5;
        for (let i = 0; i < logs; i++) root._appendLogEntry(root, buffer, ENTRY_TYPE_INFO);
        nowMillis = 11;
        root._writeSpanEnd(root, buffer, ENTRY_TYPE_SPAN_OK);
      },
    );

    expect(buffer.timestamp[1] - buffer.timestamp[0]).toBe(1_000_000n);
    const rows = Array.from(buffer.timestamp.slice(2, 2 + logs));
    // Sixteen rows on the span-start stamp, then the refresh picks up 10.5ms.
    expect(new Set(rows.slice(0, LOG_STAMP_REFRESH))).toEqual(new Set([EPOCH]));
    expect(new Set(rows.slice(LOG_STAMP_REFRESH))).toEqual(new Set([EPOCH + 500_000n]));
  });
});

describe('coarsening policy', () => {
  it('is a quarter of the 64-row span capacity every lane shares', () => {
    // The constant is a fraction of buffer capacity by design, so a full buffer
    // forces several fresh reads. If SPAN_CAPACITY moves, this moves with it.
    expect(LOG_STAMP_REFRESH).toBe(WIDE_CAPACITY / 4);
  });
});
