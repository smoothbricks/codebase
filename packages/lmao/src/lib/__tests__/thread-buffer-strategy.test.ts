import { beforeAll, describe, expect, it } from 'bun:test';
import { defineOpContext } from '../defineOpContext.js';
import { Ok } from '../result.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { ENTRY_TYPE_INFO, ENTRY_TYPE_SPAN_OK, ENTRY_TYPE_SPAN_START } from '../schema/systemSchema.js';
import { ThreadBufferStrategy } from '../ThreadBufferStrategy.js';
import { createTraceRoot } from '../traceRoot.node.js';
import { TestTracer } from '../tracers/TestTracer.js';
import { isThreadSpanView } from '../wasm/threadSpanView.js';

const schema = defineLogSchema({
  count: S.number(),
  user: S.category(),
});

const opContext = defineOpContext({ logSchema: schema });

describe('ThreadBufferStrategy', () => {
  let strategy: ThreadBufferStrategy<typeof schema>;

  beforeAll(async () => {
    strategy = await ThreadBufferStrategy.create({ capacity: 8 });
  });

  it('writes root and child spans through the ThreadSpanBuffer binding', () => {
    const tracer = new TestTracer(opContext, {
      bufferStrategy: strategy,
      createTraceRoot,
    });

    tracer.trace_fn(12, 'root', {}, (ctx) => {
      ctx.log.info('hello').count(3).user('ada');
      ctx.setScope({ user: 'scoped' });
      ctx.span('child', (child) => {
        child.log.info('nested');
        return child.ok(1);
      });
      return ctx.ok('done');
    });

    expect(tracer.rootBuffers).toHaveLength(1);
    const root = tracer.rootBuffers[0];
    expect(isThreadSpanView(root)).toBe(true);
    if (!isThreadSpanView(root)) return;

    expect(strategy.runtime.rowCount(root.binding)).toBeGreaterThanOrEqual(4);
    expect(strategy.runtime.readHeader(root.binding, root.startRow) & 0xff).toBe(ENTRY_TYPE_SPAN_START);
    expect(strategy.runtime.readSpanId(root.binding, root.startRow)).toBe(root.spanId);
    expect(strategy.runtime.readMessage(root.binding, root.startRow)).toBe('root');

    const logRow = [...root.fakeToReal.values()][0];
    expect(logRow).toBeDefined();
    if (logRow === undefined) return;
    expect(strategy.runtime.readHeader(root.binding, logRow) & 0xff).toBe(ENTRY_TYPE_INFO);
    expect(strategy.runtime.readMessage(root.binding, logRow)).toBe('hello');
    expect(strategy.runtime.readTimestamp(root.binding, root.startRow)).not.toBe(0n);
  });

  it('opens a span and ends ok without the generated per-span TypedArray store', () => {
    const tracer = new TestTracer(opContext, {
      bufferStrategy: strategy,
      createTraceRoot,
    });
    const result = tracer.trace_fn(0, 'solo', {}, (ctx) => ctx.ok(true));
    expect(result).toBeInstanceOf(Ok);
    const root = tracer.rootBuffers[0];
    expect(isThreadSpanView(root)).toBe(true);
    if (!isThreadSpanView(root)) return;
    expect(strategy.runtime.readHeader(root.binding, root.completionRow) & 0xff).toBe(ENTRY_TYPE_SPAN_OK);
  });

  it('applies latest setScope across an overflow chain at materialize', () => {
    const tracer = new TestTracer(opContext, {
      bufferStrategy: strategy,
      createTraceRoot,
    });
    tracer.trace_fn(0, 'overflow-scope', {}, (ctx) => {
      for (let i = 0; i < 12; i++) ctx.log.info(`row-${i}`);
      ctx.setScope({ user: 'late' });
      return ctx.ok(1);
    });
    const root = tracer.rootBuffers[0];
    expect(isThreadSpanView(root)).toBe(true);
    if (!isThreadSpanView(root)) return;
    const rows = strategy.runtime.rowCount(root.binding);
    expect(rows).toBeGreaterThan(8);
    strategy.runtime.materializeScope(root.binding, 0, rows);
    const userOrdinal = root.ordinals.get('user');
    expect(userOrdinal).toBeDefined();
    if (userOrdinal === undefined) return;
    const cell = strategy.runtime.readAttr(root.binding, rows - 1, userOrdinal);
    expect(cell).toBeDefined();
    if (cell === undefined) return;
    expect(strategy.runtime.readInterned(root.binding, Number(cell.value))).toBe('late');
  });

  it('coarsens log-row stamps but never a span duration', () => {
    const tracer = new TestTracer(opContext, {
      bufferStrategy: strategy,
      createTraceRoot,
    });
    // Two rows would ride one cached stamp; 40 crosses the refresh boundary
    // more than once, so this pins the refresh as well as the sharing.
    const rowCount = 40;
    tracer.trace_fn(0, 'stamps', {}, (ctx) => {
      for (let i = 0; i < rowCount; i++) ctx.log.info(`row-${i}`);
      return ctx.ok(1);
    });
    const root = tracer.rootBuffers[0];
    expect(isThreadSpanView(root)).toBe(true);
    if (!isThreadSpanView(root)) return;

    const start = strategy.runtime.readTimestamp(root.binding, root.startRow);
    const completion = strategy.runtime.readTimestamp(root.binding, root.completionRow);
    // Boundaries always read fresh: a duration derived from these two never
    // collapses, however many rows shared a cached stamp in between.
    expect(completion).toBeGreaterThan(start);

    const stamps = [...root.fakeToReal.values()].map((row) => strategy.runtime.readTimestamp(root.binding, row));
    expect(stamps).toHaveLength(rowCount);
    for (const stamp of stamps) {
      expect(stamp).toBeGreaterThanOrEqual(start);
      expect(stamp).toBeLessThanOrEqual(completion);
    }
    for (let i = 1; i < stamps.length; i++) {
      const previous = stamps[i - 1] ?? 0n;
      const current = stamps[i] ?? 0n;
      expect(current).toBeGreaterThanOrEqual(previous);
    }
    // Bounded staleness, not a frozen clock: 40 rows span more than two
    // refresh windows, so the cache must have been re-read.
    expect(new Set(stamps).size).toBeGreaterThan(1);
    expect(new Set(stamps).size).toBeLessThan(rowCount);
  });
});
