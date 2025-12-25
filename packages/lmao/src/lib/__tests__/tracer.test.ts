/**
 * Tracer tests
 *
 * Tests for the Tracer class - the entry point for creating traces.
 */

import { describe, expect, it } from 'bun:test';
// Must import test-helpers first to initialize timestamp implementation
import './test-helpers.js';
import { convertSpanTreeToArrowTable } from '../convertToArrow.js';
import { defineOpContext } from '../defineOpContext.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { createTraceId } from '../traceId.js';

import { TestTracer } from '../tracers/TestTracer.js';

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

      const tracer = new TestTracer({
        logBinding: factory.logBinding,
        ctxDefaults: factory.ctxDefaults,
      });
      const { trace } = tracer;

      const result = await trace('test-trace', { ctx: { env: { API_KEY: 'test' } } }, async (ctx) => {
        ctx.tag.userId('user-123');
        ctx.tag.method('GET');
        return { success: true, data: 'hello' };
      });

      expect(result).toEqual({ success: true, data: 'hello' });
      expect(tracer.rootBuffers.length).toBe(1);
    });

    it('should collect multiple traces in rootBuffers', async () => {
      const factory = defineOpContext({
        logSchema: testSchema,
      });

      const tracer = new TestTracer({ logBinding: factory.logBinding });
      const { trace } = tracer;

      await trace('trace-1', async (ctx) => {
        ctx.tag.userId('user-1');
        return 'result-1';
      });

      await trace('trace-2', async (ctx) => {
        ctx.tag.userId('user-2');
        return 'result-2';
      });

      expect(tracer.rootBuffers.length).toBe(2);

      // Convert each buffer to Arrow table to verify data
      const table1 = convertSpanTreeToArrowTable(tracer.rootBuffers[0]);
      const table2 = convertSpanTreeToArrowTable(tracer.rootBuffers[1]);

      expect(table1.numRows).toBeGreaterThan(0);
      expect(table2.numRows).toBeGreaterThan(0);
    });

    it('should handle errors and re-throw', async () => {
      const factory = defineOpContext({
        logSchema: testSchema,
      });

      const tracer = new TestTracer({
        logBinding: factory.logBinding,
      });
      const { trace } = tracer;

      const error = new Error('test error');

      await expect(
        trace('error-trace', async () => {
          throw error;
        }),
      ).rejects.toThrow('test error');

      // Buffer should still be registered for flushing
      expect(tracer.rootBuffers.length).toBe(1);
    });

    it('should accept optional trace ID', async () => {
      const factory = defineOpContext({
        logSchema: testSchema,
      });

      const { trace } = new TestTracer({ logBinding: factory.logBinding });

      const customTraceId = createTraceId('custom-trace-id-12345');

      await trace('trace-with-id', { traceId: customTraceId }, async (ctx) => {
        // Access the trace ID from buffer
        expect(ctx.buffer.trace_id).toBe(customTraceId);
        return 'done';
      });
    });

    it('should merge ctx defaults with overrides', async () => {
      const factory = defineOpContext({
        logSchema: testSchema,
        ctx: {
          env: null as unknown as TestEnv, // Required (null sentinel)
          config: { timeout: 5000 }, // Has default
        },
      });

      const { trace } = new TestTracer({
        logBinding: factory.logBinding,
        ctxDefaults: factory.ctxDefaults,
      });

      await trace('ctx-test', { ctx: { env: { API_KEY: 'secret' } } }, async (ctx) => {
        // env should be from overrides
        // biome-ignore lint/suspicious/noExplicitAny: Test access to dynamic ctx property
        expect((ctx as any).env.API_KEY).toBe('secret');
        // config should use default
        // biome-ignore lint/suspicious/noExplicitAny: Test access to dynamic ctx property
        expect((ctx as any).config.timeout).toBe(5000);
        return 'done';
      });

      // Can override config too
      await trace(
        'ctx-test-2',
        // biome-ignore lint/suspicious/noExplicitAny: Test access with extra override
        { ctx: { env: { API_KEY: 'key2' }, config: { timeout: 10000 } } as any },
        async (ctx) => {
          // biome-ignore lint/suspicious/noExplicitAny: Test access to dynamic ctx property
          expect((ctx as any).config.timeout).toBe(10000);
          return 'done';
        },
      );
    });
  });

  describe('overload signatures', () => {
    it('should work without overrides (name, fn)', async () => {
      const factory = defineOpContext({
        logSchema: testSchema,
      });

      const { trace } = new TestTracer({ logBinding: factory.logBinding });

      const result = await trace('simple', async () => {
        return 42;
      });

      expect(result).toBe(42);
    });

    it('should work with overrides (name, overrides, fn)', async () => {
      const factory = defineOpContext({
        logSchema: testSchema,
        ctx: { value: 0 },
      });

      const { trace } = new TestTracer({
        logBinding: factory.logBinding,
        ctxDefaults: factory.ctxDefaults,
      });

      const result = await trace('with-overrides', { ctx: { value: 100 } }, async (ctx) => {
        // biome-ignore lint/suspicious/noExplicitAny: Test access to dynamic ctx property
        return (ctx as any).value;
      });

      expect(result).toBe(100);
    });
  });
});
