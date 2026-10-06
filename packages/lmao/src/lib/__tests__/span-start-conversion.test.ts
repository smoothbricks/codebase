import { describe, expect, it } from 'bun:test';
import { type Table, tableFromIPC, tableToIPC } from '@uwdata/flechette';
import { convertSpanStartToArrowTable, convertToArrowTable } from '../convertToArrow.js';
import { defineOpContext } from '../defineOpContext.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { createTraceRoot } from '../traceRoot.node.js';
import { TestTracer } from '../tracers/TestTracer.js';
import type { SpanBuffer } from '../types.js';
import { createThreadBufferStrategy } from '../wasm/threadSpanBufferHost.js';
import { createTestTracerOptions } from './test-helpers.js';

const schema = defineLogSchema({
  user: S.category(),
  attempt: S.number(),
});
const opContext = defineOpContext({ logSchema: schema });
type Schema = (typeof opContext)['logBinding']['logSchema'];

/**
 * A table's rows as a reader of its IPC stream gets them, every 64-bit id and nanosecond
 * timestamp exact.
 */
function rowsOf(table: Table): Record<string, unknown>[] {
  const stream = tableToIPC(table, { format: 'stream' });
  if (stream === null) throw new Error('the table serialized to nothing');
  return tableFromIPC(stream, { useBigInt: true, useBigIntTimestamp: true }).toArray();
}

/** What a host that hands spans over as they open sees, captured at the open hooks. */
interface Opened {
  readonly buffer: SpanBuffer<Schema>;
  readonly start: Record<string, unknown>[];
}

class OpeningTracer extends TestTracer<typeof opContext> {
  readonly opened: Opened[] = [];

  override onTraceStart(buffer: SpanBuffer<Schema>): void {
    this.opened.push({ buffer, start: rowsOf(convertSpanStartToArrowTable(buffer)) });
    super.onTraceStart(buffer);
  }

  override onSpanStart(buffer: SpanBuffer<Schema>): void {
    this.opened.push({ buffer, start: rowsOf(convertSpanStartToArrowTable(buffer)) });
    super.onSpanStart(buffer);
  }
}

const child = opContext.defineOp('child', (ctx) => {
  ctx.tag.user('ada');
  // More rows than a fresh buffer holds, so the child's rows run into an overflow buffer.
  for (let attempt = 0; attempt < 40; attempt++) ctx.log.info('retry').attempt(attempt);
  return ctx.ok('done');
});

const root = opContext.defineOp('root', async (ctx) => {
  await ctx.span('child-call', child);
  return ctx.ok('root');
});

describe('convertSpanStartToArrowTable', () => {
  it('converts an open span to its start row alone, never its pre-armed completion', async () => {
    const tracer = new OpeningTracer(opContext, createTestTracerOptions());
    await tracer.trace('root', root);

    const [opened, openedChild] = tracer.opened;
    if (opened === undefined || openedChild === undefined) throw new Error('both spans open through the hooks');
    expect(tracer.opened.map(({ start }) => start.length)).toEqual([1, 1]);
    const [rootStart] = opened.start;
    const [childStart] = openedChild.start;
    expect(rootStart).toMatchObject({
      entry_type: 'span-start',
      message: 'root',
      span_id: opened.buffer.span_id,
      parent_span_id: null,
      timestamp: opened.buffer.timestamp[0],
    });
    expect(childStart).toMatchObject({
      entry_type: 'span-start',
      message: 'child-call',
      span_id: openedChild.buffer.span_id,
      parent_span_id: opened.buffer.span_id,
      timestamp: openedChild.buffer.timestamp[0],
    });
    // The row as it read when the span opened: the child tags its user only afterwards, so
    // the attributes nobody had written yet are null, not a zeroed lane read as a value.
    expect(childStart).toMatchObject({ user: null, attempt: null });
  });

  it('converts an ended span to the start row its whole conversion leads with, and leaves the buffer whole', async () => {
    const tracer = new OpeningTracer(opContext, createTestTracerOptions());
    await tracer.trace('root', root);

    const childBuffer = tracer.opened[1]?.buffer;
    if (childBuffer === undefined) throw new Error('the child opens through the hook');
    expect(childBuffer._overflow).toBeDefined();
    for (const { buffer } of tracer.opened) {
      const whole = rowsOf(convertToArrowTable(buffer));
      expect(rowsOf(convertSpanStartToArrowTable(buffer))).toEqual(whole.slice(0, 1));
      expect(rowsOf(convertToArrowTable(buffer))).toEqual(whole);
    }
    expect(rowsOf(convertSpanStartToArrowTable(childBuffer))).toMatchObject([
      { entry_type: 'span-start', message: 'child-call', user: 'ada' },
    ]);
  });

  it('reads an attribute a buffer never wrote as null, in its own rows and in its overflow', async () => {
    const tracer = new OpeningTracer(opContext, createTestTracerOptions());
    await tracer.trace('root', root);

    const [opened, openedChild] = tracer.opened;
    if (opened === undefined || openedChild === undefined) throw new Error('both spans open through the hooks');
    // The root writes neither attribute: its category column has an empty dictionary, so a
    // row read as valid would name index 0 of nothing, which an Arrow reader refuses.
    for (const table of [convertSpanStartToArrowTable(opened.buffer), convertToArrowTable(opened.buffer)]) {
      expect(table.getChild('user').nullCount).toBe(table.numRows);
      expect(table.getChild('attempt').nullCount).toBe(table.numRows);
    }
    // The child tags its user on its start row alone; its overflow buffer never allocates the
    // column, and its rows are null like every other row the tag did not write.
    expect(openedChild.buffer._overflow).toBeDefined();
    const users = rowsOf(convertToArrowTable(openedChild.buffer)).map((row) => row.user);
    expect(users[0]).toBe('ada');
    expect(users.slice(1).every((user) => user === null)).toBe(true);
  });

  it('refuses a thread-lane view, whose rows only its row store can read', async () => {
    const strategy = await createThreadBufferStrategy<Schema>({ capacity: 8 });
    const tracer = new TestTracer(opContext, { bufferStrategy: strategy, createTraceRoot });
    tracer.trace_fn(0, 'solo', {}, (ctx) => ctx.ok(true));
    const view = tracer.rootBuffers[0];
    if (view === undefined) throw new Error('the trace leaves its root buffer');

    expect(() => convertSpanStartToArrowTable(view)).toThrow(TypeError);
  });
});
