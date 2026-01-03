/**
 * Tests for synchronous and asynchronous trace execution
 *
 * Verifies that Tracer.trace() correctly handles both sync and async functions,
 * writing span-ok or span-exception entries at the correct time (immediately for sync,
 * after Promise resolution for async).
 *
 * Per specs/01_trace_logging_system.md and tracer.ts implementation:
 * - Sync functions: span-ok/span-exception written immediately after execution
 * - Async functions: span-ok/span-exception written after Promise resolves/rejects
 */

import { describe, expect, it } from 'bun:test';
// Must import test-helpers first to initialize timestamp implementation
import './test-helpers.js';
import { defineOpContext } from '../defineOpContext.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { ENTRY_TYPE_SPAN_EXCEPTION, ENTRY_TYPE_SPAN_OK, ENTRY_TYPE_SPAN_START } from '../schema/systemSchema.js';
import { createTraceRoot } from '../traceRoot.node.js';
import { TestTracer } from '../tracers/TestTracer.js';
import type { AnySpanBuffer } from '../types.js';

// Test schema
const testSchema = defineLogSchema({
  userId: S.category(),
  requestId: S.category(),
});

describe('Sync/Async Trace Execution', () => {
  describe('Sync trace execution', () => {
    it('should write span-ok immediately for sync function', () => {
      const ctx = defineOpContext({
        logSchema: testSchema,
      });

      let capturedBuffer: AnySpanBuffer | undefined;

      const tracer = new TestTracer(ctx, { createTraceRoot });

      // Execute sync function - trace() returns sync result
      const result = tracer.trace('sync-trace', (ctx) => {
        capturedBuffer = ctx.buffer;

        // At this point, span-start is written (row 0)
        expect(ctx.buffer.entry_type[0]).toBe(ENTRY_TYPE_SPAN_START);

        // Row 1 not yet written (happens after function returns)
        return { success: true, value: 42 };
      });

      // For sync function, result should be returned immediately (not a Promise)
      expect(result).toEqual({ success: true, value: 42 });

      // span-ok should be written to row 1 immediately after sync function returns
      expect(capturedBuffer).toBeDefined();
      expect(capturedBuffer?.entry_type[0]).toBe(ENTRY_TYPE_SPAN_START);
      expect(capturedBuffer?.entry_type[1]).toBe(ENTRY_TYPE_SPAN_OK);

      // Timestamp should be written
      expect(capturedBuffer?.timestamp[1]).toBeGreaterThan(0n);
    });

    it('should write span-exception immediately for sync function that throws', () => {
      const ctx = defineOpContext({
        logSchema: testSchema,
      });

      let capturedBuffer: AnySpanBuffer | undefined;

      const tracer = new TestTracer(ctx, { createTraceRoot });

      const testError = new Error('Sync error');

      // Execute sync function that throws - should throw immediately
      expect(() => {
        tracer.trace('sync-error-trace', (ctx) => {
          capturedBuffer = ctx.buffer;

          // span-start written at row 0
          expect(ctx.buffer.entry_type[0]).toBe(ENTRY_TYPE_SPAN_START);

          throw testError;
        });
      }).toThrow('Sync error');

      // span-exception should be written to row 1 immediately
      expect(capturedBuffer).toBeDefined();
      expect(capturedBuffer?.entry_type[0]).toBe(ENTRY_TYPE_SPAN_START);
      expect(capturedBuffer?.entry_type[1]).toBe(ENTRY_TYPE_SPAN_EXCEPTION);

      // Exception details should be written
      expect(capturedBuffer?.timestamp[1]).toBeGreaterThan(0n);

      // message should contain error message
      // Note: System columns (message, exception_stack) use method call, not direct array access
      // We verify via conversion later if needed, here we just check entry_type
    });
  });

  describe('Async trace execution', () => {
    it('should write span-ok after Promise resolves', async () => {
      const ctx = defineOpContext({
        logSchema: testSchema,
      });

      let capturedBuffer: AnySpanBuffer | undefined;
      let promiseResolved = false;

      const tracer = new TestTracer(ctx, { createTraceRoot });

      // Execute async function - trace() returns Promise
      const resultPromise = tracer.trace('async-trace', async (ctx) => {
        capturedBuffer = ctx.buffer;

        // span-start is written at row 0
        expect(ctx.buffer.entry_type[0]).toBe(ENTRY_TYPE_SPAN_START);

        // Row 1 not yet written (will be written after Promise resolves)
        // At this point, entry_type[1] may be 0 or pre-initialized

        // Simulate async work
        await new Promise((resolve) => setTimeout(resolve, 10));

        promiseResolved = true;

        return { success: true, value: 'async result' };
      });

      // Result should be a Promise
      expect(resultPromise).toBeInstanceOf(Promise);

      // Before awaiting, span-ok not yet written
      expect(promiseResolved).toBe(false);

      // Await the Promise
      const result = await resultPromise;

      // Promise should have resolved
      expect(promiseResolved).toBe(true);
      expect(result).toEqual({ success: true, value: 'async result' });

      // After Promise resolves, span-ok should be written to row 1
      expect(capturedBuffer).toBeDefined();
      expect(capturedBuffer?.entry_type[0]).toBe(ENTRY_TYPE_SPAN_START);
      expect(capturedBuffer?.entry_type[1]).toBe(ENTRY_TYPE_SPAN_OK);

      // Timestamp should be written
      expect(capturedBuffer?.timestamp[1]).toBeGreaterThan(0n);
    });

    it('should write span-exception after Promise rejects', async () => {
      const ctx = defineOpContext({
        logSchema: testSchema,
      });

      let capturedBuffer: AnySpanBuffer | undefined;
      let promiseRejected = false;

      const tracer = new TestTracer(ctx, { createTraceRoot });

      const testError = new Error('Async error');

      // Execute async function that throws
      const resultPromise = tracer.trace('async-error-trace', async (ctx) => {
        capturedBuffer = ctx.buffer;

        // span-start written at row 0
        expect(ctx.buffer.entry_type[0]).toBe(ENTRY_TYPE_SPAN_START);

        // Simulate async work before error
        await new Promise((resolve) => setTimeout(resolve, 10));

        promiseRejected = true;

        throw testError;
      });

      // Result should be a Promise
      expect(resultPromise).toBeInstanceOf(Promise);

      // Before awaiting, exception not yet thrown
      expect(promiseRejected).toBe(false);

      // Await and expect rejection
      await expect(resultPromise).rejects.toThrow('Async error');

      // Promise should have rejected
      expect(promiseRejected).toBe(true);

      // After Promise rejects, span-exception should be written to row 1
      expect(capturedBuffer).toBeDefined();
      expect(capturedBuffer?.entry_type[0]).toBe(ENTRY_TYPE_SPAN_START);
      expect(capturedBuffer?.entry_type[1]).toBe(ENTRY_TYPE_SPAN_EXCEPTION);

      // Timestamp should be written
      expect(capturedBuffer?.timestamp[1]).toBeGreaterThan(0n);
    });
  });

  describe('Mixed sync/async traces', () => {
    it('should handle multiple sync traces correctly', () => {
      const ctx = defineOpContext({
        logSchema: testSchema,
      });

      const buffers: AnySpanBuffer[] = [];

      const tracer = new TestTracer(ctx, { createTraceRoot });

      // Execute multiple sync traces
      const result1 = tracer.trace('sync-1', (ctx) => {
        buffers.push(ctx.buffer);
        ctx.tag.userId('user-1');
        return 'result-1';
      });

      const result2 = tracer.trace('sync-2', (ctx) => {
        buffers.push(ctx.buffer);
        ctx.tag.userId('user-2');
        return 'result-2';
      });

      // Both should return immediately
      expect(result1).toBe('result-1');
      expect(result2).toBe('result-2');

      // Both buffers should have span-ok written
      expect(buffers.length).toBe(2);
      expect(buffers[0].entry_type[1]).toBe(ENTRY_TYPE_SPAN_OK);
      expect(buffers[1].entry_type[1]).toBe(ENTRY_TYPE_SPAN_OK);

      // Both traces should be collected
      expect(tracer.rootBuffers.length).toBe(2);
    });

    it('should handle multiple async traces correctly', async () => {
      const ctx = defineOpContext({
        logSchema: testSchema,
      });

      const buffers: AnySpanBuffer[] = [];

      const tracer = new TestTracer(ctx, { createTraceRoot });

      // Execute multiple async traces in parallel
      const promise1 = tracer.trace('async-1', async (ctx) => {
        buffers.push(ctx.buffer);
        ctx.tag.userId('user-1');
        await new Promise((resolve) => setTimeout(resolve, 5));
        return 'result-1';
      });

      const promise2 = tracer.trace('async-2', async (ctx) => {
        buffers.push(ctx.buffer);
        ctx.tag.userId('user-2');
        await new Promise((resolve) => setTimeout(resolve, 5));
        return 'result-2';
      });

      // Both should return Promises
      expect(promise1).toBeInstanceOf(Promise);
      expect(promise2).toBeInstanceOf(Promise);

      // Await both
      const [result1, result2] = await Promise.all([promise1, promise2]);

      expect(result1).toBe('result-1');
      expect(result2).toBe('result-2');

      // Both buffers should have span-ok written
      expect(buffers.length).toBe(2);
      expect(buffers[0].entry_type[1]).toBe(ENTRY_TYPE_SPAN_OK);
      expect(buffers[1].entry_type[1]).toBe(ENTRY_TYPE_SPAN_OK);

      // Both traces should be collected
      expect(tracer.rootBuffers.length).toBe(2);
    });

    it('should handle interleaved sync and async traces', async () => {
      const ctx = defineOpContext({
        logSchema: testSchema,
      });

      const buffers: AnySpanBuffer[] = [];

      const tracer = new TestTracer(ctx, { createTraceRoot });

      // Sync trace
      const syncResult = tracer.trace('sync', (ctx) => {
        buffers.push(ctx.buffer);
        return 'sync-done';
      });

      // Async trace
      const asyncPromise = tracer.trace('async', async (ctx) => {
        buffers.push(ctx.buffer);
        await new Promise((resolve) => setTimeout(resolve, 5));
        return 'async-done';
      });

      // Another sync trace
      const syncResult2 = tracer.trace('sync-2', (ctx) => {
        buffers.push(ctx.buffer);
        return 'sync-done-2';
      });

      // Sync results available immediately
      expect(syncResult).toBe('sync-done');
      expect(syncResult2).toBe('sync-done-2');

      // Async result available after await
      const asyncResult = await asyncPromise;
      expect(asyncResult).toBe('async-done');

      // All three buffers should have span-ok
      expect(buffers.length).toBe(3);
      expect(buffers[0].entry_type[1]).toBe(ENTRY_TYPE_SPAN_OK);
      expect(buffers[1].entry_type[1]).toBe(ENTRY_TYPE_SPAN_OK);
      expect(buffers[2].entry_type[1]).toBe(ENTRY_TYPE_SPAN_OK);

      // All three traces should be collected
      expect(tracer.rootBuffers.length).toBe(3);
    });
  });

  describe('Trace with tags and logging', () => {
    it('should support tags in sync trace', () => {
      const ctx = defineOpContext({
        logSchema: testSchema,
      });

      let capturedBuffer: AnySpanBuffer | undefined;

      const tracer = new TestTracer(ctx, { createTraceRoot });

      const result = tracer.trace('sync-with-tags', (ctx) => {
        capturedBuffer = ctx.buffer;

        // Write tags
        ctx.tag.userId('user-123');
        ctx.tag.requestId('req-456');

        return { id: 123 };
      });

      expect(result).toEqual({ id: 123 });

      // span-ok should be written
      expect(capturedBuffer?.entry_type[1]).toBe(ENTRY_TYPE_SPAN_OK);
    });

    it('should support tags in async trace', async () => {
      const ctx = defineOpContext({
        logSchema: testSchema,
      });

      let capturedBuffer: AnySpanBuffer | undefined;

      const tracer = new TestTracer(ctx, { createTraceRoot });

      const result = await tracer.trace('async-with-tags', async (ctx) => {
        capturedBuffer = ctx.buffer;

        // Write tags
        ctx.tag.userId('user-123');
        ctx.tag.requestId('req-456');

        await new Promise((resolve) => setTimeout(resolve, 5));

        return { id: 123 };
      });

      expect(result).toEqual({ id: 123 });

      // span-ok should be written after Promise resolves
      expect(capturedBuffer?.entry_type[1]).toBe(ENTRY_TYPE_SPAN_OK);
    });

    it('should support logging in sync trace', () => {
      const ctx = defineOpContext({
        logSchema: testSchema,
      });

      let capturedBuffer: AnySpanBuffer | undefined;

      const tracer = new TestTracer(ctx, { createTraceRoot });

      const result = tracer.trace('sync-with-logging', (ctx) => {
        capturedBuffer = ctx.buffer;

        // Log events (should go to rows 2+)
        ctx.log.info('Starting operation');
        ctx.log.debug('Debug info');

        return 'done';
      });

      expect(result).toBe('done');

      // span-ok at row 1
      expect(capturedBuffer?.entry_type[1]).toBe(ENTRY_TYPE_SPAN_OK);

      // Events logged at rows 2+ (verified by successful completion)
    });

    it('should support logging in async trace', async () => {
      const ctx = defineOpContext({
        logSchema: testSchema,
      });

      let capturedBuffer: AnySpanBuffer | undefined;

      const tracer = new TestTracer(ctx, { createTraceRoot });

      const result = await tracer.trace('async-with-logging', async (ctx) => {
        capturedBuffer = ctx.buffer;

        // Log events
        ctx.log.info('Starting async operation');

        await new Promise((resolve) => setTimeout(resolve, 5));

        ctx.log.info('Completed async operation');

        return 'done';
      });

      expect(result).toBe('done');

      // span-ok at row 1 (written after Promise resolves)
      expect(capturedBuffer?.entry_type[1]).toBe(ENTRY_TYPE_SPAN_OK);
    });
  });
});
