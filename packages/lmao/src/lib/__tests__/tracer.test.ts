/**
 * Tracer tests
 *
 * Tests for the Tracer class - the entry point for creating traces.
 */

import { describe, expect, it } from 'bun:test';
import type { Table } from 'apache-arrow';
import { defineOpContext } from '../defineOpContext.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { createTraceId } from '../traceId.js';
import { Tracer, type TraceSink } from '../tracer.js';

// Test schema
const testSchema = defineLogSchema({
  userId: S.category(),
  requestId: S.category(),
  method: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
});

// Test environment type
interface TestEnv {
  API_KEY: string;
}

describe('Tracer', () => {
  describe('basic functionality', () => {
    it('should create a trace and return the body result', async () => {
      const factory = defineOpContext({
        logSchema: testSchema,
        ctx: { env: null as unknown as TestEnv },
      });

      const tables: Table[] = [];
      const sink: TraceSink = (table) => {
        tables.push(table);
      };

      const tracer = new Tracer(factory, { sink });

      const result = await tracer.trace('test-trace', { env: { API_KEY: 'test' } }, async (ctx) => {
        ctx.tag.userId('user-123');
        ctx.tag.method('GET');
        return { success: true, data: 'hello' };
      });

      expect(result.success).toBe(true);
      expect(result.value).toEqual({ success: true, data: 'hello' });
      expect(tracer.pendingCount).toBe(1);
    });

    it('should flush pending buffers to sink', async () => {
      const factory = defineOpContext({
        logSchema: testSchema,
      });

      const tables: Table[] = [];
      const sink: TraceSink = async (table) => {
        tables.push(table);
      };

      const tracer = new Tracer(factory, { sink });

      await tracer.trace('trace-1', async (ctx) => {
        ctx.tag.userId('user-1');
        return 'result-1';
      });

      await tracer.trace('trace-2', async (ctx) => {
        ctx.tag.userId('user-2');
        return 'result-2';
      });

      expect(tracer.pendingCount).toBe(2);

      await tracer.flush();

      expect(tracer.pendingCount).toBe(0);
      expect(tables.length).toBe(2);
    });

    it('should handle errors and re-throw', async () => {
      const factory = defineOpContext({
        logSchema: testSchema,
      });

      const tables: Table[] = [];
      const tracer = new Tracer(factory, {
        sink: (t) => {
          tables.push(t);
        },
      });

      const error = new Error('test error');

      await expect(
        tracer.trace('error-trace', async () => {
          throw error;
        }),
      ).rejects.toThrow('test error');

      // Buffer should still be registered for flushing
      expect(tracer.pendingCount).toBe(1);
    });

    it('should accept optional trace ID', async () => {
      const factory = defineOpContext({
        logSchema: testSchema,
      });

      const tracer = new Tracer(factory, { sink: () => {} });

      const customTraceId = createTraceId('custom-trace-id-12345');

      await tracer.trace('trace-with-id', { traceId: customTraceId }, async (ctx) => {
        // Access the trace ID from buffer
        expect(ctx.buffer.trace_id).toBe(customTraceId);
        return 'done';
      });
    });

    it('should merge ctx defaults with overrides', async () => {
      const factory = defineOpContext({
        logSchema: testSchema,
        ctx: {
          env: null as unknown as TestEnv, // Required
          config: { timeout: 5000 }, // Has default
        },
      });

      const tracer = new Tracer(factory, { sink: () => {} });

      await tracer.trace('ctx-test', { env: { API_KEY: 'secret' } }, async (ctx) => {
        // env should be from overrides
        // biome-ignore lint/suspicious/noExplicitAny: Test access to dynamic ctx property
        expect((ctx as any).env.API_KEY).toBe('secret');
        // config should use default
        // biome-ignore lint/suspicious/noExplicitAny: Test access to dynamic ctx property
        expect((ctx as any).config.timeout).toBe(5000);
        return 'done';
      });

      // Can override config too
      // biome-ignore lint/suspicious/noExplicitAny: Test access with extra override
      await tracer.trace('ctx-test-2', { env: { API_KEY: 'key2' }, config: { timeout: 10000 } } as any, async (ctx) => {
        // biome-ignore lint/suspicious/noExplicitAny: Test access to dynamic ctx property
        expect((ctx as any).config.timeout).toBe(10000);
        return 'done';
      });
    });
  });

  describe('overload signatures', () => {
    it('should work without overrides (name, fn)', async () => {
      const factory = defineOpContext({
        logSchema: testSchema,
      });

      const tracer = new Tracer(factory, { sink: () => {} });

      const result = await tracer.trace('simple', async () => {
        return 42;
      });

      expect(result).toBe(42);
    });

    it('should work with overrides (name, overrides, fn)', async () => {
      const factory = defineOpContext({
        logSchema: testSchema,
        ctx: { value: 0 },
      });

      const tracer = new Tracer(factory, { sink: () => {} });

      const result = await tracer.trace('with-overrides', { value: 100 }, async (ctx) => {
        // biome-ignore lint/suspicious/noExplicitAny: Test access to dynamic ctx property
        return (ctx as any).value;
      });

      expect(result).toBe(100);
    });
  });

  describe('hintFlush', () => {
    it('should schedule flush on microtask', async () => {
      const factory = defineOpContext({
        logSchema: testSchema,
      });

      let flushed = false;
      const tracer = new Tracer(factory, {
        sink: () => {
          flushed = true;
        },
      });

      await tracer.trace('hint-test', async () => 'done');

      tracer.hintFlush();

      // Not flushed yet (still on microtask queue)
      expect(flushed).toBe(false);

      // Wait for microtask
      await new Promise((resolve) => queueMicrotask(resolve));
      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(flushed).toBe(true);
    });
  });
});
