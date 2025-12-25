/**
 * NoOpTracer tests
 *
 * Tests for the NoOpTracer class - a tracer that executes ops but
 * produces no output (no-op lifecycle hooks).
 */

import { describe, expect, it } from 'bun:test';
// Initialize Node.js timestamp implementation for SpanLogger
import { setTimestampNanosImpl } from '../../codegen/spanLoggerGenerator.js';
import { getTimestampNanos } from '../../timestamp.node.js';

setTimestampNanosImpl(getTimestampNanos);

import { defineLogSchema, defineOpContext, S } from '../../defineOpContext.js';
import { NoOpTracer } from '../NoOpTracer.js';

// Test schema
const testSchema = defineLogSchema({
  userId: S.category(),
});

describe('NoOpTracer', () => {
  describe('instantiation', () => {
    it('should create a NoOpTracer instance', () => {
      const ctx = defineOpContext({
        logSchema: testSchema,
      });
      const tracer = new NoOpTracer(ctx);
      expect(tracer).toBeDefined();
      expect(tracer.trace).toBeDefined();
      expect(tracer.flush).toBeDefined();
    });

    it('should allow destructuring trace and flush', () => {
      const ctx = defineOpContext({
        logSchema: testSchema,
      });
      const { trace, flush } = new NoOpTracer(ctx);
      expect(typeof trace).toBe('function');
      expect(typeof flush).toBe('function');
    });
  });

  describe('trace execution', () => {
    it('should execute functions and return result', async () => {
      const ctx = defineOpContext({
        logSchema: testSchema,
      });
      const { trace } = new NoOpTracer(ctx);

      const result = await trace('test-trace', async (ctx) => {
        ctx.tag.userId('user-123');
        return { success: true, value: 'sync-result' };
      });

      expect(result).toEqual({ success: true, value: 'sync-result' });
    });

    it('should execute async functions and return result', async () => {
      const ctx = defineOpContext({
        logSchema: testSchema,
      });
      const { trace } = new NoOpTracer(ctx);

      const result = await trace('test-trace', async (ctx) => {
        await Promise.resolve();
        ctx.tag.userId('user-456');
        return 'async-result';
      });

      expect(result).toBe('async-result');
    });

    it('should propagate exceptions from traced functions', async () => {
      const ctx = defineOpContext({
        logSchema: testSchema,
      });
      const { trace } = new NoOpTracer(ctx);

      await expect(
        trace('test-trace', async () => {
          throw new Error('test-error');
        }),
      ).rejects.toThrow('test-error');
    });

    it('should support nested spans', async () => {
      const ctx = defineOpContext({
        logSchema: testSchema,
      });
      const { defineOp } = ctx;
      const { trace } = new NoOpTracer(ctx);

      const childOp = defineOp('child', (ctx) => {
        return ctx.ok('child-done');
      });

      const result = await trace('parent-trace', async (ctx) => {
        // biome-ignore lint/suspicious/noExplicitAny: Test setup requires flexible typing
        const childResult = await (ctx as any).span('child-span', childOp);
        expect(childResult.success).toBe(true);
        return 'parent-done';
      });

      expect(result).toBe('parent-done');
    });
  });

  describe('flush', () => {
    it('should be a no-op that returns resolved promise', async () => {
      const ctx = defineOpContext({
        logSchema: testSchema,
      });
      const { flush } = new NoOpTracer(ctx);

      // Should not throw, should resolve immediately
      await expect(flush()).resolves.toBeUndefined();
    });

    it('should clear pending buffers without processing', async () => {
      const ctx = defineOpContext({
        logSchema: testSchema,
      });
      const { trace, flush } = new NoOpTracer(ctx);

      // Create a trace
      await trace('test-trace', async () => {
        return 'done';
      });

      // Flush should clear without doing anything
      await flush();
    });
  });

  describe('lifecycle hooks', () => {
    it('should not throw when hooks are called (they are no-ops)', async () => {
      const ctx = defineOpContext({
        logSchema: testSchema,
      });
      const { defineOp } = ctx;
      const { trace } = new NoOpTracer(ctx);

      const childOp1 = defineOp('c1', (c) => c.ok(1));
      const childOp2 = defineOp('c2', (c) => c.ok(2));

      // Should complete without any errors
      const result = await trace('test-trace', async (ctx) => {
        // Create multiple nested spans to trigger all hooks
        // biome-ignore lint/suspicious/noExplicitAny: Test setup requires flexible typing
        await (ctx as any).span('child1', childOp1);
        // biome-ignore lint/suspicious/noExplicitAny: Test setup requires flexible typing
        await (ctx as any).span('child2', childOp2);
        return 'done';
      });

      expect(result).toBe('done');
    });
  });

  describe('use cases', () => {
    it('should work for tests that do not need trace inspection', async () => {
      // This demonstrates using NoOpTracer for testing business logic
      // without caring about trace output
      const ctx = defineOpContext({
        logSchema: testSchema,
      });
      const { trace } = new NoOpTracer(ctx);

      const result = await trace('math-test', async () => {
        return 2 + 2;
      });

      expect(result).toBe(4);
    });

    it('should support ctx.ok() for successful results', async () => {
      const ctx = defineOpContext({
        logSchema: testSchema,
      });
      const { defineOp } = ctx;
      const { trace } = new NoOpTracer(ctx);

      const successOp = defineOp('success', (ctx) => {
        return ctx.ok({ status: 'success' });
      });

      // biome-ignore lint/suspicious/noExplicitAny: Test setup requires flexible typing
      const result = await (trace as any)('ok-test', successOp);
      expect(result.success).toBe(true);
    });

    it('should support ctx.err() for error results', async () => {
      const ctx = defineOpContext({
        logSchema: testSchema,
      });
      const { defineOp } = ctx;
      const { trace } = new NoOpTracer(ctx);

      const failingOp = defineOp('failing', (ctx) => {
        return ctx.err('VALIDATION_ERROR', { field: 'email' });
      });

      // biome-ignore lint/suspicious/noExplicitAny: Test setup requires flexible typing
      const result = await (trace as any)('err-test', failingOp);
      expect(result.success).toBe(false);
    });
  });
});
