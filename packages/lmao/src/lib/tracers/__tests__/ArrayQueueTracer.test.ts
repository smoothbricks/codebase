// Configure Node.js timestamp implementation - MUST be first import
import '../../__tests__/test-helpers.js';

import { describe, expect, it } from 'bun:test';
import { convertSpanTreeToArrowTable } from '../../convertToArrow.js';
import { defineLogSchema, defineOpContext, type OpContextOf, S } from '../../defineOpContext.js';
import { ArrayQueueTracer } from '../ArrayQueueTracer.js';

describe('ArrayQueueTracer', () => {
  const testSchema = defineLogSchema({
    userId: S.category(),
  });

  const opContext = defineOpContext({
    logSchema: testSchema,
  });
  type Ctx = OpContextOf<typeof opContext>;
  const { logBinding, defineOp } = opContext;

  describe('queue accumulation', () => {
    it('should start with empty queue', () => {
      const tracer = new ArrayQueueTracer<Ctx>({ logBinding });
      expect(tracer.queue).toHaveLength(0);
    });

    it('should queue root buffer after trace completes', async () => {
      const tracer = new ArrayQueueTracer<Ctx>({ logBinding });
      const { trace } = tracer;

      const testOp = defineOp('test', (ctx) => ctx.ok('done'));
      await trace('queued-trace', testOp);

      expect(tracer.queue).toHaveLength(1);
      expect(tracer.queue[0]._spanName).toBe('queued-trace');
    });

    it('should queue multiple traces in order', async () => {
      const tracer = new ArrayQueueTracer<Ctx>({ logBinding });
      const { trace } = tracer;

      const testOp = defineOp('test', (ctx) => ctx.ok('done'));

      await trace('first', testOp);
      await trace('second', testOp);
      await trace('third', testOp);

      expect(tracer.queue).toHaveLength(3);
      expect(tracer.queue[0]._spanName).toBe('first');
      expect(tracer.queue[1]._spanName).toBe('second');
      expect(tracer.queue[2]._spanName).toBe('third');
    });

    it('should queue buffer even if trace throws', async () => {
      const tracer = new ArrayQueueTracer<Ctx>({ logBinding });
      const { trace } = tracer;

      const failOp = defineOp('fail', async () => {
        throw new Error('boom');
      });

      await expect(trace('failing', failOp)).rejects.toThrow('boom');

      expect(tracer.queue).toHaveLength(1);
      expect(tracer.queue[0]._spanName).toBe('failing');
    });
  });

  describe('drain()', () => {
    it('should return all queued buffers', async () => {
      const tracer = new ArrayQueueTracer<Ctx>({ logBinding });
      const { trace } = tracer;

      const testOp = defineOp('test', (ctx) => ctx.ok('done'));

      await trace('a', testOp);
      await trace('b', testOp);

      const drained = tracer.drain();

      expect(drained).toHaveLength(2);
      expect(drained[0]._spanName).toBe('a');
      expect(drained[1]._spanName).toBe('b');
    });

    it('should clear queue after drain', async () => {
      const tracer = new ArrayQueueTracer<Ctx>({ logBinding });
      const { trace } = tracer;

      const testOp = defineOp('test', (ctx) => ctx.ok('done'));

      await trace('x', testOp);
      await trace('y', testOp);

      tracer.drain();

      expect(tracer.queue).toHaveLength(0);
    });

    it('should return empty array when queue is empty', () => {
      const tracer = new ArrayQueueTracer<Ctx>({ logBinding });

      const drained = tracer.drain();

      expect(drained).toHaveLength(0);
      expect(Array.isArray(drained)).toBe(true);
    });

    it('should allow new traces after drain', async () => {
      const tracer = new ArrayQueueTracer<Ctx>({ logBinding });
      const { trace } = tracer;

      const testOp = defineOp('test', (ctx) => ctx.ok('done'));

      await trace('before', testOp);
      tracer.drain();
      await trace('after', testOp);

      expect(tracer.queue).toHaveLength(1);
      expect(tracer.queue[0]._spanName).toBe('after');
    });

    it('should return independent array (not reference to internal queue)', async () => {
      const tracer = new ArrayQueueTracer<Ctx>({ logBinding });
      const { trace } = tracer;

      const testOp = defineOp('test', (ctx) => ctx.ok('done'));

      await trace('test', testOp);

      const drained = tracer.drain();

      // Modify returned array
      drained.push(drained[0]);

      // Internal queue should be unaffected
      expect(tracer.queue).toHaveLength(0);
    });
  });

  describe('production pattern: batch processing', () => {
    it('should support batch-then-process pattern', async () => {
      const tracer = new ArrayQueueTracer<Ctx>({ logBinding });
      const { trace } = tracer;

      const testOp = defineOp('test', (ctx) => ctx.ok('done'));

      // Simulate batching traces
      await trace('req-1', testOp);
      await trace('req-2', testOp);
      await trace('req-3', testOp);

      // Process batch
      const batch = tracer.drain();
      const tables = batch.map((buf) => convertSpanTreeToArrowTable(buf));

      expect(tables).toHaveLength(3);
      for (const table of tables) {
        expect(table.numRows).toBe(2); // span-start + span-ok
      }

      // Queue is now empty, ready for next batch
      expect(tracer.queue).toHaveLength(0);
    });
  });

  describe('child spans', () => {
    it('should include child spans in queued buffer tree', async () => {
      const tracer = new ArrayQueueTracer<Ctx>({ logBinding });
      const { trace } = tracer;

      const childOp = defineOp('child', (ctx) => ctx.ok('c'));
      const parentOp = defineOp('parent', async (ctx) => {
        await ctx.span('child-span', childOp);
        return ctx.ok('p');
      });

      await trace('root', parentOp);

      const [buffer] = tracer.drain();
      expect(buffer._children).toHaveLength(1);
      expect(buffer._children[0]._spanName).toBe('child-span');
    });
  });
});
