/**
 * TestTracer tests
 *
 * Tests for the TestTracer class - a tracer that accumulates root buffers for test inspection.
 */

import { describe, expect, it } from 'bun:test';
// Must import test-helpers first to initialize timestamp implementation
import '../../__tests__/test-helpers.js';
import { createTestTracerOptions } from '../../__tests__/test-helpers.js';
import { convertSpanTreeToArrowTable } from '../../convertToArrow.js';
import { defineOpContext } from '../../defineOpContext.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import { TestTracer } from '../TestTracer.js';

describe('TestTracer', () => {
  const testSchema = defineLogSchema({
    userId: S.category(),
    count: S.number(),
  });

  const ctx = defineOpContext({
    logSchema: testSchema,
  });

  describe('rootBuffers accumulation', () => {
    it('should start with empty rootBuffers', () => {
      const tracer = new TestTracer(ctx, { ...createTestTracerOptions() });
      expect(tracer.rootBuffers).toHaveLength(0);
    });

    it('should accumulate root buffer after trace completes', async () => {
      const tracer = new TestTracer(ctx, { ...createTestTracerOptions() });
      const { trace } = tracer;

      await trace('my-trace', async (ctx) => ctx.ok('done'));

      expect(tracer.rootBuffers).toHaveLength(1);
      expect(tracer.rootBuffers[0].message_values[0]).toBe('my-trace');
    });

    it('should accumulate multiple traces', async () => {
      const tracer = new TestTracer(ctx, { ...createTestTracerOptions() });
      const { trace } = tracer;

      await trace('trace-1', async (ctx) => ctx.ok('done'));
      await trace('trace-2', async (ctx) => ctx.ok('done'));
      await trace('trace-3', async (ctx) => ctx.ok('done'));

      expect(tracer.rootBuffers).toHaveLength(3);
      expect(tracer.rootBuffers[0].message_values[0]).toBe('trace-1');
      expect(tracer.rootBuffers[1].message_values[0]).toBe('trace-2');
      expect(tracer.rootBuffers[2].message_values[0]).toBe('trace-3');
    });

    it('should accumulate buffer even if trace throws', async () => {
      const tracer = new TestTracer(ctx, { ...createTestTracerOptions() });
      const { trace } = tracer;

      await expect(
        trace('failing-trace', async () => {
          throw new Error('intentional');
        }),
      ).rejects.toThrow('intentional');

      // Buffer should still be collected
      expect(tracer.rootBuffers).toHaveLength(1);
      expect(tracer.rootBuffers[0].message_values[0]).toBe('failing-trace');
    });
  });

  describe('child span access', () => {
    it('should include child spans in buffer tree', async () => {
      const tracer = new TestTracer(ctx, { ...createTestTracerOptions() });
      const { trace } = tracer;

      await trace('root-trace', async (ctx) => {
        await ctx.span('child-span', async (childCtx) => {
          return childCtx.ok('child-done');
        });
        return ctx.ok('parent-done');
      });

      expect(tracer.rootBuffers).toHaveLength(1);
      const rootBuffer = tracer.rootBuffers[0];

      // Child spans are in _children array
      expect(rootBuffer._children).toHaveLength(1);
      expect(rootBuffer._children[0].message_values[0]).toBe('child-span');
    });

    it('should support deeply nested spans', async () => {
      const tracer = new TestTracer(ctx, { ...createTestTracerOptions() });
      const { trace } = tracer;

      await trace('root', async (ctx) => {
        await ctx.span('span-l2', async (ctx2) => {
          await ctx2.span('span-l3', async (ctx3) => {
            return ctx3.ok('l3');
          });
          return ctx2.ok('l2');
        });
        return ctx.ok('l1');
      });

      const root = tracer.rootBuffers[0];
      expect(root._children).toHaveLength(1);
      expect(root._children[0].message_values[0]).toBe('span-l2');
      expect(root._children[0]._children).toHaveLength(1);
      expect(root._children[0]._children[0].message_values[0]).toBe('span-l3');
    });
  });

  describe('clear()', () => {
    it('should clear all accumulated buffers', async () => {
      const tracer = new TestTracer(ctx, { ...createTestTracerOptions() });
      const { trace } = tracer;

      await trace('trace-1', async (ctx) => ctx.ok('done'));
      await trace('trace-2', async (ctx) => ctx.ok('done'));

      expect(tracer.rootBuffers).toHaveLength(2);

      tracer.clear();

      expect(tracer.rootBuffers).toHaveLength(0);
    });

    it('should allow new traces after clear', async () => {
      const tracer = new TestTracer(ctx, { ...createTestTracerOptions() });
      const { trace } = tracer;

      await trace('before-clear', async (ctx) => ctx.ok('done'));
      tracer.clear();
      await trace('after-clear', async (ctx) => ctx.ok('done'));

      expect(tracer.rootBuffers).toHaveLength(1);
      expect(tracer.rootBuffers[0].message_values[0]).toBe('after-clear');
    });
  });

  describe('Arrow conversion', () => {
    it('should produce valid Arrow table from root buffer', async () => {
      const tracer = new TestTracer(ctx, { ...createTestTracerOptions() });
      const { trace } = tracer;

      await trace('arrow-test', async (ctx) => {
        ctx.tag.userId('user-123');
        return ctx.ok('done');
      });

      const table = convertSpanTreeToArrowTable(tracer.rootBuffers[0]);

      // Root span has 2 rows: span-start (row 0) and span-ok (row 1)
      expect(table.numRows).toBe(2);
    });

    it('should include child spans in Arrow table', async () => {
      const tracer = new TestTracer(ctx, { ...createTestTracerOptions() });
      const { trace } = tracer;

      await trace('root', async (ctx) => {
        await ctx.span('child1', async (ctx1) => ctx1.ok('child'));
        await ctx.span('child2', async (ctx2) => ctx2.ok('child'));
        return ctx.ok('parent');
      });

      const table = convertSpanTreeToArrowTable(tracer.rootBuffers[0]);

      // Root: 2 rows, Child1: 2 rows, Child2: 2 rows = 6 rows total
      expect(table.numRows).toBe(6);
    });
  });

  describe('tag values', () => {
    it('should preserve tag values in buffer', async () => {
      const tracer = new TestTracer(ctx, { ...createTestTracerOptions() });
      const { trace } = tracer;

      await trace('tag-test', async (ctx) => {
        ctx.tag.userId('user-456');
        ctx.tag.count(42);
        return ctx.ok('done');
      });

      const buffer = tracer.rootBuffers[0];
      // Tag values are written to row 0 (span-start row)
      // Cast to any to access typed properties since AnySpanBuffer doesn't have index signatures
      // biome-ignore lint/suspicious/noExplicitAny: Test access to typed buffer properties
      const typedBuffer = buffer as any;
      expect(typedBuffer.userId_values[0]).toBe('user-456');
      expect(typedBuffer.count_values[0]).toBe(42);
    });
  });
});
