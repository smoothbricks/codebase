import { beforeAll, describe, expect, it } from 'bun:test';
import { defineOpContext } from '../defineOpContext.js';
import { Ok } from '../result.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import {
  ENTRY_TYPE_INFO,
  ENTRY_TYPE_SPAN_OK,
  ENTRY_TYPE_SPAN_START,
  THREAD_ATTRIBUTE_KINDS,
} from '../schema/systemSchema.js';
import { ThreadBufferStrategy, type ThreadSpanBufferProvider } from '../ThreadBufferStrategy.js';
import { createTraceRoot } from '../traceRoot.node.js';
import { TestTracer } from '../tracers/TestTracer.js';
import type { ThreadSpanBufferBinding } from '../wasm/threadSpanBuffer.js';
import {
  createThreadBufferStrategy,
  createThreadSpanBufferRuntime,
  type ThreadSpanBufferRuntime,
} from '../wasm/threadSpanBufferHost.js';
import { isThreadSpanView } from '../wasm/threadSpanView.js';
import { runtimeModuleGraph } from './moduleGraph.js';

const schema = defineLogSchema({
  count: S.number(),
  user: S.category(),
});

const opContext = defineOpContext({ logSchema: schema });

/**
 * allocator.wasm's row stores behind a backend that refuses to open any span
 * named in `refused`, the way a host store refuses a span it cannot place.
 * Every lifecycle call that reaches a store is recorded by the span it names,
 * so a test can say exactly what a refused span sent.
 */
function refusingBackend(runtime: ThreadSpanBufferRuntime, refused: ReadonlySet<string>) {
  const calls: string[] = [];
  const stores: ThreadSpanBufferBinding[] = [];
  const provider: ThreadSpanBufferProvider = {
    createBinding(threadId, capacity, schema) {
      const store = runtime.createBinding(threadId, capacity, schema);
      stores.push(store);
      const names = new Map<number, string>();
      return {
        capacity: store.capacity,
        free: () => store.free(),
        reset: () => store.reset(),
        get rowGeneration() {
          return store.rowGeneration;
        },
        spanStartRow: (spanId) => store.spanStartRow(spanId),
        intern(text) {
          const ordinal = store.intern(text);
          names.set(ordinal, text);
          return ordinal;
        },
        openSpan(traceId, parentThreadId, parentSpanId, nameOrdinal, timestamp, line) {
          const name = names.get(nameOrdinal) ?? '';
          calls.push(`open ${name}`);
          if (refused.has(name)) return 0n;
          return store.openSpan(traceId, parentThreadId, parentSpanId, nameOrdinal, timestamp, line);
        },
        openSpanStatic(traceId, parentThreadId, parentSpanId, nameId, timestamp, line) {
          calls.push(`open static ${nameId}`);
          return store.openSpanStatic(traceId, parentThreadId, parentSpanId, nameId, timestamp, line);
        },
        end(spanId, entryType, timestamp) {
          calls.push(`end ${spanId}`);
          return store.end(spanId, entryType, timestamp);
        },
        appendLog(spanId, entryType, messageOrdinal, timestamp, line) {
          calls.push(`log ${spanId}`);
          return store.appendLog(spanId, entryType, messageOrdinal, timestamp, line);
        },
        appendLogStatic(spanId, entryType, messageId, timestamp, line) {
          calls.push(`log ${spanId}`);
          return store.appendLogStatic(spanId, entryType, messageId, timestamp, line);
        },
        setScope(spanId, ordinal, kind, value) {
          calls.push(`scope ${spanId}`);
          return store.setScope(spanId, ordinal, kind, value);
        },
        setCompletionMessage(spanId, message) {
          calls.push(`complete ${spanId}`);
          return store.setCompletionMessage(spanId, message);
        },
        attributeCells: (block) => store.attributeCells(block),
      };
    },
    toArrowTable() {
      throw new Error('these tests read the stores, never a conversion');
    },
  };
  return { provider, calls, stores };
}

describe('ThreadBufferStrategy', () => {
  let strategy: ThreadBufferStrategy<typeof schema, ThreadSpanBufferRuntime>;

  beforeAll(async () => {
    strategy = await createThreadBufferStrategy({ capacity: 8 });
  });

  it('reaches no allocator.wasm loader, so a bundle over another provider names no node: module', async () => {
    const graph = await runtimeModuleGraph('lib/ThreadBufferStrategy.ts');

    expect(
      [...graph.modules].filter(
        (module) => module === 'lib/wasm/wasmAllocator.ts' || module === 'lib/wasm/threadSpanBufferHost.ts',
      ),
    ).toEqual([]);
    expect([...graph.bare.keys()].filter((specifier) => specifier.startsWith('node:'))).toEqual([]);
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

    expect(strategy.provider.rowCount(root.binding)).toBeGreaterThanOrEqual(4);
    expect(strategy.provider.readHeader(root.binding, root.startRow) & 0xff).toBe(ENTRY_TYPE_SPAN_START);
    expect(strategy.provider.readSpanId(root.binding, root.startRow)).toBe(root.spanId);
    expect(strategy.provider.readMessage(root.binding, root.startRow)).toBe('root');

    const logRow = [...root.fakeToReal.values()][0];
    expect(logRow).toBeDefined();
    if (logRow === undefined) return;
    expect(strategy.provider.readHeader(root.binding, logRow) & 0xff).toBe(ENTRY_TYPE_INFO);
    expect(strategy.provider.readMessage(root.binding, logRow)).toBe('hello');
    expect(strategy.provider.readTimestamp(root.binding, root.startRow)).not.toBe(0n);
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
    expect(strategy.provider.readHeader(root.binding, root.completionRow) & 0xff).toBe(ENTRY_TYPE_SPAN_OK);
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
    const rows = strategy.provider.rowCount(root.binding);
    expect(rows).toBeGreaterThan(8);
    strategy.provider.materializeScope(root.binding, 0, rows);
    const userOrdinal = root.ordinals.get('user');
    expect(userOrdinal).toBeDefined();
    if (userOrdinal === undefined) return;
    const cell = strategy.provider.readAttr(root.binding, rows - 1, userOrdinal);
    expect(cell).toBeDefined();
    if (cell === undefined) return;
    expect(strategy.provider.readInterned(root.binding, Number(cell.value))).toBe('late');
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

    const start = strategy.provider.readTimestamp(root.binding, root.startRow);
    const completion = strategy.provider.readTimestamp(root.binding, root.completionRow);
    // Boundaries always read fresh: a duration derived from these two never
    // collapses, however many rows shared a cached stamp in between.
    expect(completion).toBeGreaterThan(start);

    const stamps = [...root.fakeToReal.values()].map((row) => strategy.provider.readTimestamp(root.binding, row));
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

  it("stores an attribute into the row store's own cells, with no call and no copy", () => {
    const tracer = new TestTracer(opContext, {
      bufferStrategy: strategy,
      createTraceRoot,
    });
    tracer.trace_fn(0, 'cells', {}, (ctx) => {
      ctx.tag.count(7);
      ctx.log.info('row').user('ada');
      return ctx.ok(1);
    });
    const root = tracer.rootBuffers[0];
    if (!isThreadSpanView(root)) throw new Error('expected a thread-lane span');
    const count = root.fields.get('count');
    const user = root.fields.get('user');
    const logRow = [...root.fakeToReal.values()][0];
    if (count === undefined || user === undefined || logRow === undefined) throw new Error('missing schema fields');

    // The TypedArrays the writes used are views of linear memory itself, at the
    // offset the store exported for the block — not a JS copy of it.
    const views = root.cells.views(root.startRow);
    const exported = root.binding.attributeCells(Math.floor(root.startRow / strategy.capacity));
    if (exported === undefined) throw new Error('the span start block has no attribute cells');
    expect(views.f64.buffer).toBe(strategy.provider.memory.buffer);
    expect(views.f64.byteOffset).toBe(exported.byteOffset);

    // The store reads exactly what the stores wrote.
    const tag = strategy.provider.readAttr(root.binding, root.startRow, count.ordinal);
    expect(tag?.kind).toBe(THREAD_ATTRIBUTE_KINDS[0].discriminant);
    expect(new Float64Array(new BigUint64Array([tag?.value ?? 0n]).buffer)[0]).toBe(7);
    const text = strategy.provider.readAttr(root.binding, logRow, user.ordinal);
    expect(strategy.provider.readInterned(root.binding, Number(text?.value))).toBe('ada');

    // And the batch the lane converts carries them as columns.
    const table = strategy.toArrowTable(root);
    expect(table.getChild('count')?.at(root.startRow)).toBe(7);
    expect(table.getChild('user')?.at(logRow)).toBe('ada');
  });

  it('re-interns a name after a reset reclaims the arena, so it never writes a stale ordinal', async () => {
    const reclaiming: ThreadBufferStrategy<typeof schema, ThreadSpanBufferRuntime> = await createThreadBufferStrategy({
      capacity: 8,
    });
    const tracer = new TestTracer(opContext, { bufferStrategy: reclaiming, createTraceRoot });
    tracer.trace_fn(0, 'kept-name', {}, (ctx) => ctx.ok(1));
    // Distinct names until the arena passes its reclaim threshold (8 MiB).
    const filler = 'x'.repeat(4096);
    for (let name = 0; name < 2100; name += 1) {
      tracer.trace_fn(0, `${name}-${filler}`, {}, (ctx) => ctx.ok(1));
    }
    reclaiming.reset();
    // The fresh arena hands out low ordinals again; a binding still holding
    // 'kept-name' -> its old ordinal would now name this text instead.
    tracer.trace_fn(0, 'other-name', {}, (ctx) => ctx.ok(1));
    tracer.trace_fn(0, 'kept-name', {}, (ctx) => ctx.ok(1));

    const root = tracer.rootBuffers[tracer.rootBuffers.length - 1];
    if (!isThreadSpanView(root)) throw new Error('expected a thread-lane span');
    expect(reclaiming.provider.readMessage(root.binding, root.startRow)).toBe('kept-name');
  });

  it("never lands a span's write after its store released its rows on another span's row", async () => {
    const moving: ThreadBufferStrategy<typeof schema, ThreadSpanBufferRuntime> = await createThreadBufferStrategy({
      capacity: 8,
    });
    const tracer = new TestTracer(opContext, { bufferStrategy: moving, createTraceRoot });
    let resume: () => void = () => undefined;
    const held = tracer.trace('held', async (ctx) => {
      ctx.tag.count(1);
      await new Promise<void>((resolve) => {
        resume = resolve;
      });
      // The store released this span's rows while it waited; the rows it read
      // at open are another span's now.
      ctx.tag.count(2);
      ctx.log.info('after the release').count(3);
      return ctx.ok('held');
    });
    moving.reset();
    // Opens on the rows the reset released, the held span's included.
    tracer.trace_fn(0, 'later', {}, (ctx) => {
      ctx.log.info('untagged');
      return ctx.ok(2);
    });
    const later = tracer.rootBuffers[tracer.rootBuffers.length - 1];
    if (!isThreadSpanView(later)) throw new Error('expected a thread-lane span');
    const count = later.ordinals.get('count');
    if (count === undefined) throw new Error('missing schema field count');
    const rows = moving.provider.rowCount(later.binding);

    resume();
    // The released span writes nothing further, and fails nothing it traces.
    expect((await held).value).toBe('held');
    expect(moving.provider.rowCount(later.binding)).toBe(rows);
    for (let row = 0; row < rows; row += 1) {
      expect(moving.provider.readAttr(later.binding, row, count)).toBeUndefined();
    }
  });
});

describe('a span the row store refuses to open', () => {
  let runtime: ThreadSpanBufferRuntime;

  beforeAll(async () => {
    runtime = await createThreadSpanBufferRuntime();
  });

  it('runs its body, answers the body its own result, and leaves the store as it was', () => {
    const backend = refusingBackend(runtime, new Set(['refused']));
    const strategy = ThreadBufferStrategy.fromProvider<typeof schema, ThreadSpanBufferProvider>(backend.provider, {
      capacity: 8,
    });
    const tracer = new TestTracer(opContext, { bufferStrategy: strategy, createTraceRoot });
    // A span the store did open, holding rows 0 and 1: the rows a span with no
    // rows of its own would land on if it wrote anyway.
    tracer.trace_fn(0, 'kept', {}, (ctx) => {
      ctx.tag.count(7);
      return ctx.ok(1);
    });
    const kept = tracer.rootBuffers[0];
    const store = backend.stores[0];
    if (!isThreadSpanView(kept) || store === undefined) throw new Error('expected a thread-lane span in one store');
    const count = kept.ordinals.get('count');
    if (count === undefined) throw new Error('missing schema field count');
    const rows = runtime.rowCount(store);
    backend.calls.length = 0;

    const result = tracer.trace_fn(0, 'refused', {}, (ctx) => {
      ctx.tag.count(99);
      ctx.log.info('dropped').count(5).user('ada');
      ctx.setScope({ user: 'scoped' });
      ctx.span('child', (child) => {
        child.tag.count(98);
        child.log.info('nested');
        return child.ok(2);
      });
      return ctx.ok('own');
    });

    expect(result).toBeInstanceOf(Ok);
    expect(result.value).toBe('own');
    // The one refused open is all that crossed: no row, end, scope or
    // completion for a span the store holds no record of, and its child was
    // never offered as a root of its own.
    expect(backend.calls).toEqual(['open refused']);
    expect(runtime.rowCount(store)).toBe(rows);
    const tag = runtime.readAttr(store, kept.startRow, count);
    expect(new Float64Array(new BigUint64Array([tag?.value ?? 0n]).buffer)[0]).toBe(7);
    expect(strategy.refusedSpans).toBe(1);

    // The store is not wedged: the next root opens and ends as any other.
    backend.calls.length = 0;
    tracer.trace_fn(0, 'after', {}, (ctx) => ctx.ok(3));
    const after = tracer.rootBuffers[tracer.rootBuffers.length - 1];
    if (!isThreadSpanView(after)) throw new Error('expected a thread-lane span');
    expect(backend.calls).toEqual(['open after', `end ${after.spanId}`]);
    expect(strategy.refusedSpans).toBe(1);
  });

  it("answers an async body's result, and lets the body's own throw through unchanged", async () => {
    const backend = refusingBackend(runtime, new Set(['refused']));
    const strategy = ThreadBufferStrategy.fromProvider<typeof schema, ThreadSpanBufferProvider>(backend.provider, {
      capacity: 8,
    });
    const tracer = new TestTracer(opContext, { bufferStrategy: strategy, createTraceRoot });

    const result = await tracer.trace('refused', async (ctx) => {
      await Promise.resolve();
      ctx.log.info('after the await').count(1);
      return ctx.ok('own');
    });
    expect(result.value).toBe('own');

    expect(() =>
      tracer.trace_fn(0, 'refused', {}, () => {
        throw new Error('the body failed');
      }),
    ).toThrow('the body failed');
    expect(backend.calls).toEqual(['open refused', 'open refused']);
    expect(strategy.refusedSpans).toBe(2);
  });

  it('drops the refused child and everything under it, and its parent writes on', async () => {
    const backend = refusingBackend(runtime, new Set(['refused-child']));
    const strategy = ThreadBufferStrategy.fromProvider<typeof schema, ThreadSpanBufferProvider>(backend.provider, {
      capacity: 8,
    });
    const tracer = new TestTracer(opContext, { bufferStrategy: strategy, createTraceRoot });

    const result = await tracer.trace('parent', async (ctx) => {
      const child = await ctx.span('refused-child', async (refused) => {
        await refused.span('grandchild', (grandchild) => grandchild.ok(1));
        return refused.ok('child-own');
      });
      ctx.log.info('parent goes on');
      if (!(child instanceof Ok)) throw new Error('the refused child answered no Ok');
      return ctx.ok(child.value);
    });

    expect(result.value).toBe('child-own');
    const parent = tracer.rootBuffers[0];
    if (!isThreadSpanView(parent)) throw new Error('expected a thread-lane span');
    expect(backend.calls).toEqual([
      'open parent',
      'open refused-child',
      `log ${parent.spanId}`,
      `end ${parent.spanId}`,
    ]);
    expect(strategy.refusedSpans).toBe(1);
  });
});
