/**
 * CompositeTracer tests
 *
 * CompositeTracer fans one trace run out to several delegate tracers. The case
 * that matters in practice — and the one the examples and the convert-to-Arrow
 * guide document — is stacking StdioTracer (human-readable output) with
 * ArrayQueueTracer (retains completed root buffers for Arrow conversion), so a
 * single run both prints and exports.
 */

// Configure Node.js timestamp implementation - MUST be first import
import '../../__tests__/test-helpers.js';

import { describe, expect, it } from 'bun:test';
import { Writable } from 'node:stream';
import { createTestTracerOptions } from '../../__tests__/test-helpers.js';
import { convertSpanTreeToArrowTable } from '../../convertToArrow.js';
import { defineLogSchema, defineOpContext, S } from '../../defineOpContext.js';
import { resolveMessage } from '../../resolveMessage.js';
import { ArrayQueueTracer } from '../ArrayQueueTracer.js';
import { CompositeTracer } from '../CompositeTracer.js';
import { StdioTracer, type StdioWritable } from '../StdioTracer.js';

type MockStream = { stream: StdioWritable; output: string[] };

function createMockStream(): MockStream {
  const output: string[] = [];
  const writable = new Writable({
    write(chunk, _encoding, callback) {
      output.push(chunk.toString());
      callback();
    },
  });
  const stream: StdioWritable = {
    write(chunk: string): boolean {
      return writable.write(chunk);
    },
  };
  return { stream, output };
}

describe('CompositeTracer', () => {
  const testSchema = defineLogSchema({
    userId: S.category(),
  });

  const ctx = defineOpContext({
    logSchema: testSchema,
  });
  const { defineOp } = ctx;

  // `CompositeTracerOptions` carries `delegates: Tracer<B>[]`, which pins B to this
  // op context — so the shared tracer options must be built for the same concrete
  // log schema rather than the loose `LogSchema` default.
  type TestLogSchema = (typeof ctx)['logBinding']['logSchema'];

  /** Stdio + ArrayQueue behind one composite, sharing the same tracer options. */
  function createStackedTracer() {
    const { stream: out, output } = createMockStream();
    const { stream: err } = createMockStream();
    const options = createTestTracerOptions<TestLogSchema>();

    const stdio = new StdioTracer(ctx, { ...options, out, err, colorEnabled: false });
    const queued = new ArrayQueueTracer(ctx, { ...options });
    const tracer = new CompositeTracer(ctx, { ...options, delegates: [stdio, queued] });

    return { tracer, queued, output };
  }

  describe('stdio + ArrayQueue stacking', () => {
    it('should print to stdout and retain the root buffer from one trace run', async () => {
      const { tracer, queued, output } = createStackedTracer();

      const testOp = defineOp('test', (ctx) => ctx.ok('done'));
      await tracer.trace('stacked-trace', testOp);

      // StdioTracer delegate printed the span tree...
      expect(output.some((line) => line.includes('stacked-trace'))).toBe(true);

      // ...and the ArrayQueueTracer delegate kept the completed root buffer.
      expect(queued.queue).toHaveLength(1);
      expect(resolveMessage(queued.queue[0], 0)).toBe('stacked-trace');
    });

    it('should convert the retained buffer to an Arrow table', async () => {
      const { tracer, queued } = createStackedTracer();

      const testOp = defineOp('test', (ctx) => ctx.ok('done'));
      await tracer.trace('exported-trace', testOp);

      const tables = queued.drain().map((rootBuffer) => convertSpanTreeToArrowTable(rootBuffer));

      expect(tables).toHaveLength(1);
      // span-start + span-ok
      expect(tables[0]?.numRows).toBe(2);
      expect(tables[0]?.names).toContain('entry_type');
      expect(tables[0]?.names).toContain('userId');
    });

    it('should leave the queue empty after draining so the next batch is clean', async () => {
      const { tracer, queued } = createStackedTracer();

      const testOp = defineOp('test', (ctx) => ctx.ok('done'));
      await tracer.trace('first', testOp);

      expect(queued.drain()).toHaveLength(1);
      expect(queued.queue).toHaveLength(0);

      await tracer.trace('second', testOp);

      const batch = queued.drain();
      expect(batch).toHaveLength(1);
      expect(resolveMessage(batch[0], 0)).toBe('second');
    });

    it('should capture child spans in both delegates', async () => {
      const { tracer, queued, output } = createStackedTracer();

      const childOp = defineOp('child', (ctx) => ctx.ok('child-done'));
      const parentOp = defineOp('parent', async (ctx) => {
        await ctx.span('child-span', childOp);
        return ctx.ok('parent-done');
      });

      await tracer.trace('with-children', parentOp);

      expect(output.some((line) => line.includes('child-span'))).toBe(true);

      const table = convertSpanTreeToArrowTable(queued.drain()[0]);
      // parent span-start/ok + child span-start/ok
      expect(table.numRows).toBe(4);
    });
  });

  describe('delegate fan-out', () => {
    it('should keep delegates independent — draining one does not affect the other', async () => {
      const { tracer, queued, output } = createStackedTracer();

      const testOp = defineOp('test', (ctx) => ctx.ok('done'));
      await tracer.trace('independent', testOp);

      const printedBefore = output.length;
      queued.drain();

      // Draining the queue must not retroactively change what stdio already wrote.
      expect(output).toHaveLength(printedBefore);
      expect(output.some((line) => line.includes('independent'))).toBe(true);
    });
  });
});
