/**
 * Retry Integration Tests for LMAO Op
 *
 * Verifies end-to-end retry behavior in LMAO's span execution:
 * - TransientError triggers retry based on error.policy
 * - BlockedError returns immediately without retry
 * - Non-transient errors return immediately
 * - All backoff strategies work correctly
 * - Policy override per-call works
 * - span-retry entries are written for observability
 *
 * IMPORTANT: Retry logic is implemented in span execution (ctx.span()), not at tracer.trace() level.
 * Root traces do NOT have retry. Tests use ctx.span() to trigger retry behavior.
 *
 * Note: Tests use very short delays (0-1ms) with jitter disabled to run quickly.
 * The retry loop is tested with real time, not fake timers.
 */

import { describe, expect, it } from 'bun:test';
import { createTestTracerOptions } from '../../__tests__/test-helpers.js';
import { defineOpContext } from '../../defineOpContext.js';
import { Blocked } from '../../errors/Blocked.js';
import { Code } from '../../errors/CodeError.js';
import { exponentialBackoff, fixedDelay, linearBackoff, Transient, TransientError } from '../../errors/Transient.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import { ENTRY_TYPE_SPAN_RETRY } from '../../schema/systemSchema.js';
import { TestTracer } from '../../tracers/TestTracer.js';

// Test schema
const testSchema = defineLogSchema({
  operation: S.category(),
  service: S.category(),
});

// Test op context
const testOpContext = defineOpContext({
  logSchema: testSchema,
});

const { defineOp } = testOpContext;

// Define transient error codes with SHORT delays for fast tests
// Using jitter: false and 0-1ms delays to avoid slow tests
const SERVICE_UNAVAILABLE = Transient<{ status?: number }>('SERVICE_UNAVAILABLE', {
  backoff: 'exponential',
  maxAttempts: 3,
  baseDelayMs: 0,
  jitter: false,
});
const NETWORK_ERROR = Transient<{ service: string }>('NETWORK_ERROR', {
  backoff: 'linear',
  maxAttempts: 3,
  baseDelayMs: 0,
  jitter: false,
});
// Non-transient error codes
const VALIDATION_ERROR = Code<{ field: string }>('VALIDATION_ERROR');

describe('LMAO Op Retry', () => {
  // Helper to test retry behavior via child span
  // Retry logic is in span execution (ctx.span()), not tracer.trace()
  const executeWithRetry = async (
    tracer: TestTracer<typeof testOpContext>,
    spanName: string,
    opFn: (
      ctx: import('../../opContext/types.js').SpanContext<
        import('../../opContext/types.js').OpContextOf<typeof testOpContext>
      >,
    ) =>
      | import('../../result.js').Result<unknown, unknown>
      | Promise<import('../../result.js').Result<unknown, unknown>>,
  ) => {
    return tracer.trace('root', async (ctx) => {
      // Use ctx.span() to trigger span execution with retry logic
      return ctx.span(spanName, opFn);
    });
  };

  describe('TransientError triggers retry', () => {
    it('should retry on TransientError and succeed on second attempt', async () => {
      let attempts = 0;

      const tracer = new TestTracer(testOpContext, createTestTracerOptions());

      const result = await executeWithRetry(tracer, 'testOp', async (ctx) => {
        attempts++;
        if (attempts < 2) {
          return ctx.err(SERVICE_UNAVAILABLE({ status: 503 }));
        }
        return ctx.ok({ success: true });
      });

      expect(attempts).toBe(2);
      expect(result.success).toBe(true);
    });

    it('should exhaust retries and return final TransientError', async () => {
      let attempts = 0;

      const tracer = new TestTracer(testOpContext, createTestTracerOptions());

      const result = await executeWithRetry(tracer, 'testOp', async (ctx) => {
        attempts++;
        return ctx.err(NETWORK_ERROR({ service: 'payment-api' }));
      });

      expect(attempts).toBe(3); // maxAttempts from NETWORK_ERROR
      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error).toBeInstanceOf(TransientError);
      }
    });

    it('should verify error is instanceof TransientError', async () => {
      const tracer = new TestTracer(testOpContext, createTestTracerOptions());

      await executeWithRetry(tracer, 'testOp', async (ctx) => {
        const error = SERVICE_UNAVAILABLE({ status: 503 });
        expect(error).toBeInstanceOf(TransientError);
        expect(error).toBeInstanceOf(Error);
        return ctx.err(error);
      });
    });
  });

  describe('BlockedError does not trigger retry', () => {
    it('should return immediately without retry for Blocked.service', async () => {
      let attempts = 0;

      const tracer = new TestTracer(testOpContext, createTestTracerOptions());

      const result = await executeWithRetry(tracer, 'testOp', async (ctx) => {
        attempts++;
        return ctx.err(Blocked.service('payment-api'));
      });

      expect(attempts).toBe(1); // No retries
      expect(result.success).toBe(false);
    });

    it('should return immediately for Blocked.index', async () => {
      let attempts = 0;

      const tracer = new TestTracer(testOpContext, createTestTracerOptions());

      const result = await executeWithRetry(tracer, 'testOp', async (ctx) => {
        attempts++;
        return ctx.err(Blocked.index('orders-index'));
      });

      expect(attempts).toBe(1); // No retries
      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error).toBeInstanceOf(Blocked);
      }
    });

    it('should return immediately for Blocked.ended', async () => {
      let attempts = 0;

      const tracer = new TestTracer(testOpContext, createTestTracerOptions());

      const result = await executeWithRetry(tracer, 'testOp', async (ctx) => {
        attempts++;
        return ctx.err(Blocked.ended('execution-123'));
      });

      expect(attempts).toBe(1); // No retries
      expect(result.success).toBe(false);
      if (!result.success) {
        const error = result.error as Blocked;
        expect(error).toBeInstanceOf(Blocked);
        expect(error.reason.type).toBe('ended');
      }
    });
  });

  describe('Non-transient errors do not trigger retry', () => {
    it('should return error immediately for regular CodeError', async () => {
      let attempts = 0;

      const tracer = new TestTracer(testOpContext, createTestTracerOptions());

      const result = await executeWithRetry(tracer, 'testOp', async (ctx) => {
        attempts++;
        return ctx.err(VALIDATION_ERROR({ field: 'email' }));
      });

      expect(attempts).toBe(1); // No retries
      expect(result.success).toBe(false);
    });

    it('should return error immediately for plain object error', async () => {
      let attempts = 0;

      const tracer = new TestTracer(testOpContext, createTestTracerOptions());

      const result = await executeWithRetry(tracer, 'testOp', async (ctx) => {
        attempts++;
        // biome-ignore lint/suspicious/noExplicitAny: Testing plain object errors
        return ctx.err({ code: 'PLAIN_ERROR', detail: 'something' } as any);
      });

      expect(attempts).toBe(1); // No retries
      expect(result.success).toBe(false);
    });
  });

  describe('Backoff strategies', () => {
    it('should use exponential backoff', async () => {
      let attempts = 0;

      const expBackoffError = Transient<void>('EXP_ERROR', {
        backoff: 'exponential',
        maxAttempts: 4,
        baseDelayMs: 0,
        jitter: false,
      });

      const tracer = new TestTracer(testOpContext, createTestTracerOptions());

      const result = await executeWithRetry(tracer, 'testOp', async (ctx) => {
        attempts++;
        if (attempts < 4) {
          return ctx.err(expBackoffError());
        }
        return ctx.ok({ success: true });
      });

      expect(result.success).toBe(true);
      expect(attempts).toBe(4);
    });

    it('should use linear backoff', async () => {
      let attempts = 0;

      const linearError = Transient<void>('LINEAR_ERROR', {
        backoff: 'linear',
        maxAttempts: 4,
        baseDelayMs: 0,
        jitter: false,
      });

      const tracer = new TestTracer(testOpContext, createTestTracerOptions());

      const result = await executeWithRetry(tracer, 'testOp', async (ctx) => {
        attempts++;
        if (attempts < 4) {
          return ctx.err(linearError());
        }
        return ctx.ok({ success: true });
      });

      expect(result.success).toBe(true);
      expect(attempts).toBe(4);
    });

    it('should use fixed delay', async () => {
      let attempts = 0;

      const fixedError = Transient<void>('FIXED_ERROR', {
        backoff: 'fixed',
        maxAttempts: 4,
        baseDelayMs: 0,
        jitter: false,
      });

      const tracer = new TestTracer(testOpContext, createTestTracerOptions());

      const result = await executeWithRetry(tracer, 'testOp', async (ctx) => {
        attempts++;
        if (attempts < 4) {
          return ctx.err(fixedError());
        }
        return ctx.ok({ success: true });
      });

      expect(result.success).toBe(true);
      expect(attempts).toBe(4);
    });

    it('should respect maxDelayMs cap', async () => {
      let attempts = 0;

      // Exponential with low cap: would grow but is capped at maxDelayMs
      const cappedError = Transient<void>('CAPPED_ERROR', {
        backoff: 'exponential',
        maxAttempts: 4,
        baseDelayMs: 10,
        maxDelayMs: 10, // Cap at 10ms
        jitter: false,
      });

      const tracer = new TestTracer(testOpContext, createTestTracerOptions());

      const result = await executeWithRetry(tracer, 'testOp', async (ctx) => {
        attempts++;
        if (attempts < 4) {
          return ctx.err(cappedError());
        }
        return ctx.ok({ success: true });
      });

      expect(result.success).toBe(true);
      expect(attempts).toBe(4);
    });

    it('should verify policy helpers create correct backoff types', () => {
      // Verify exponentialBackoff helper
      const expPolicy = exponentialBackoff(5);
      expect(expPolicy.backoff).toBe('exponential');
      expect(expPolicy.maxAttempts).toBe(5);
      expect(expPolicy.jitter).toBe(true);

      // Verify linearBackoff helper
      const linPolicy = linearBackoff(3, 200);
      expect(linPolicy.backoff).toBe('linear');
      expect(linPolicy.maxAttempts).toBe(3);
      expect(linPolicy.baseDelayMs).toBe(200);

      // Verify fixedDelay helper
      const fixPolicy = fixedDelay(2, 5000);
      expect(fixPolicy.backoff).toBe('fixed');
      expect(fixPolicy.maxAttempts).toBe(2);
      expect(fixPolicy.baseDelayMs).toBe(5000);
      expect(fixPolicy.jitter).toBe(false);
    });
  });

  describe('Policy override', () => {
    it('should allow per-call policy override to reduce maxAttempts', async () => {
      let attempts = 0;

      // Default policy has 3 attempts
      const OVERRIDE_ERROR = Transient<void>('OVERRIDE_ERROR', {
        backoff: 'exponential',
        maxAttempts: 3,
        baseDelayMs: 0,
        jitter: false,
      });

      const tracer = new TestTracer(testOpContext, createTestTracerOptions());

      const result = await executeWithRetry(tracer, 'testOp', async (ctx) => {
        attempts++;
        // Override to only 1 attempt (immediate failure)
        return ctx.err(OVERRIDE_ERROR({ maxAttempts: 1 }));
      });

      expect(attempts).toBe(1); // Override to 1 attempt
      expect(result.success).toBe(false);
    });

    it('should allow per-call policy override to change backoff strategy', async () => {
      let attempts = 0;

      // Default is exponential
      const STRATEGY_ERROR = Transient<{ info: string }>('STRATEGY_ERROR', {
        backoff: 'exponential',
        maxAttempts: 3,
        baseDelayMs: 0,
        jitter: false,
      });

      const tracer = new TestTracer(testOpContext, createTestTracerOptions());

      const result = await executeWithRetry(tracer, 'testOp', async (ctx) => {
        attempts++;
        if (attempts < 3) {
          // Override to fixed delay
          return ctx.err(STRATEGY_ERROR({ info: 'retry' }, { backoff: 'fixed', baseDelayMs: 0 }));
        }
        return ctx.ok({ success: true });
      });

      expect(result.success).toBe(true);
      expect(attempts).toBe(3);
    });

    it('should allow complete policy replacement', async () => {
      let attempts = 0;

      const BASE_ERROR = Transient<void>('BASE_ERROR', exponentialBackoff(5));

      const tracer = new TestTracer(testOpContext, createTestTracerOptions());

      const result = await executeWithRetry(tracer, 'testOp', async (ctx) => {
        attempts++;
        // Replace entire policy with 1 attempt = no retry
        return ctx.err(BASE_ERROR({ maxAttempts: 1, baseDelayMs: 0, jitter: false }));
      });

      expect(attempts).toBe(1);
      expect(result.success).toBe(false);
    });
  });

  describe('Distinguishing TransientError from BlockedError', () => {
    it('TransientError triggers retry, BlockedError does not', async () => {
      let transientAttempts = 0;
      let blockedAttempts = 0;

      const tracer = new TestTracer(testOpContext, createTestTracerOptions());

      // Execute blocked - should return immediately
      const blockedResult = await tracer.trace('blocked-trace', async (ctx) => {
        return ctx.span('blocked-span', async (spanCtx) => {
          blockedAttempts++;
          return spanCtx.err(Blocked.service('dependency'));
        });
      });
      expect(blockedAttempts).toBe(1);
      expect(blockedResult.success).toBe(false);

      // Execute transient - should retry
      const transientResult = await tracer.trace('transient-trace', async (ctx) => {
        return ctx.span('transient-span', async (spanCtx) => {
          transientAttempts++;
          return spanCtx.err(SERVICE_UNAVAILABLE({ status: 503 }));
        });
      });

      expect(transientAttempts).toBe(3); // maxAttempts: 3
      expect(transientResult.success).toBe(false);
    });

    it('different error types have correct instanceof checks', () => {
      const transientErr = SERVICE_UNAVAILABLE({ status: 503 });
      const blockedErr = Blocked.service('api');
      const codeErr = VALIDATION_ERROR({ field: 'email' });

      // TransientError checks
      expect(transientErr instanceof TransientError).toBe(true);
      expect(transientErr instanceof Blocked).toBe(false);
      expect(transientErr instanceof Error).toBe(true);

      // Blocked checks
      expect(blockedErr instanceof Blocked).toBe(true);
      expect(blockedErr instanceof TransientError).toBe(false);
      expect(blockedErr instanceof Error).toBe(true);

      // CodeError checks
      expect(codeErr instanceof TransientError).toBe(false);
      expect(codeErr instanceof Blocked).toBe(false);
      expect(codeErr instanceof Error).toBe(true);
    });
  });

  describe('span-retry entries', () => {
    it('should write span-retry entries for each retry attempt', async () => {
      let attempts = 0;

      const tracer = new TestTracer(testOpContext, createTestTracerOptions());

      const result = await tracer.trace('root', async (ctx) => {
        return ctx.span('retryObservability', async (spanCtx) => {
          attempts++;
          if (attempts < 3) {
            return spanCtx.err(SERVICE_UNAVAILABLE({ status: 503 }));
          }
          return spanCtx.ok({ success: true });
        });
      });

      expect(result.success).toBe(true);

      // Verify span-retry entries were written
      expect(tracer.rootBuffers.length).toBe(1);
      const rootBuffer = tracer.rootBuffers[0];

      // Get child span buffer (the span execution)
      const childBuffers = rootBuffer._children;
      expect(childBuffers.length).toBe(1);
      const spanBuffer = childBuffers[0];

      // Check entry_type for ENTRY_TYPE_SPAN_RETRY (value 5)
      // Row 0: span-start
      // Row 1: span-ok/span-err
      // Row 2+: log entries and span-retry entries
      const entryTypes = spanBuffer.entry_type;
      const retryEntries: number[] = [];
      for (let i = 2; i < spanBuffer._writeIndex; i++) {
        if (entryTypes[i] === ENTRY_TYPE_SPAN_RETRY) {
          retryEntries.push(i);
        }
      }

      // Should have 2 retry entries (attempts 1 and 2 failed)
      expect(retryEntries.length).toBe(2);

      // Verify message format: retry:op:{opName}
      // Note: For inline functions, the opMetadata name comes from the parent context
      // which in this test is 'root' (the root trace name)
      const messages = spanBuffer.getColumnIfAllocated('message') as string[];
      for (const idx of retryEntries) {
        expect(messages[idx]).toBe('retry:op:root');
      }
    });

    it('should not write span-retry entries when no retries occur', async () => {
      const tracer = new TestTracer(testOpContext, createTestTracerOptions());

      await tracer.trace('root', async (ctx) => {
        return ctx.span('noRetry', async (spanCtx) => {
          return spanCtx.ok({ success: true });
        });
      });

      expect(tracer.rootBuffers.length).toBe(1);
      const rootBuffer = tracer.rootBuffers[0];
      const childBuffers = rootBuffer._children;
      expect(childBuffers.length).toBe(1);
      const spanBuffer = childBuffers[0];

      // Check for any retry entries
      const entryTypes = spanBuffer.entry_type;
      let retryCount = 0;
      for (let i = 0; i < spanBuffer._writeIndex; i++) {
        if (entryTypes[i] === ENTRY_TYPE_SPAN_RETRY) {
          retryCount++;
        }
      }

      // No retries occurred, so no span-retry entries
      expect(retryCount).toBe(0);
    });

    it('should write span-retry entries even when retries are exhausted', async () => {
      const tracer = new TestTracer(testOpContext, createTestTracerOptions());

      const result = await tracer.trace('root', async (ctx) => {
        return ctx.span('exhaustedRetries', async (spanCtx) => {
          return spanCtx.err(SERVICE_UNAVAILABLE({ status: 503 }));
        });
      });

      expect(result.success).toBe(false);

      const rootBuffer = tracer.rootBuffers[0];
      const spanBuffer = rootBuffer._children[0];

      // Check for retry entries
      const entryTypes = spanBuffer.entry_type;
      let retryCount = 0;
      for (let i = 0; i < spanBuffer._writeIndex; i++) {
        if (entryTypes[i] === ENTRY_TYPE_SPAN_RETRY) {
          retryCount++;
        }
      }

      // maxAttempts: 3 means 3 attempts total, 2 retries
      // Each failed attempt before exhaustion writes a retry entry
      expect(retryCount).toBe(2);
    });

    it('should include error code in span-retry entry', async () => {
      let attempts = 0;

      const tracer = new TestTracer(testOpContext, createTestTracerOptions());

      await tracer.trace('root', async (ctx) => {
        return ctx.span('retryErrorCode', async (spanCtx) => {
          attempts++;
          if (attempts < 2) {
            return spanCtx.err(SERVICE_UNAVAILABLE({ status: 503 }));
          }
          return spanCtx.ok({ success: true });
        });
      });

      const rootBuffer = tracer.rootBuffers[0];
      const spanBuffer = rootBuffer._children[0];

      // Find retry entry
      const entryTypes = spanBuffer.entry_type;
      let retryIdx = -1;
      for (let i = 2; i < spanBuffer._writeIndex; i++) {
        if (entryTypes[i] === ENTRY_TYPE_SPAN_RETRY) {
          retryIdx = i;
          break;
        }
      }

      expect(retryIdx).toBeGreaterThan(-1);
      // Check error_code was written
      const errorCodes = spanBuffer.getColumnIfAllocated('error_code') as string[];
      expect(errorCodes[retryIdx]).toBe('SERVICE_UNAVAILABLE');
    });
  });

  describe('Edge cases', () => {
    it('should handle immediate success (no retries needed)', async () => {
      let attempts = 0;

      const tracer = new TestTracer(testOpContext, createTestTracerOptions());

      const result = await executeWithRetry(tracer, 'immediateSuccess', async (ctx) => {
        attempts++;
        return ctx.ok({ immediate: true });
      });

      expect(attempts).toBe(1);
      expect(result.success).toBe(true);
      if (result.success) {
        const value = result.value as { immediate: boolean };
        expect(value.immediate).toBe(true);
      }
    });

    it('should handle maxAttempts of 1 (no retry)', async () => {
      let attempts = 0;

      const noRetryError = Transient<void>('NO_RETRY', {
        backoff: 'fixed',
        maxAttempts: 1,
        baseDelayMs: 0,
        jitter: false,
      });

      const tracer = new TestTracer(testOpContext, createTestTracerOptions());

      const result = await executeWithRetry(tracer, 'noRetryOp', async (ctx) => {
        attempts++;
        return ctx.err(noRetryError());
      });

      expect(attempts).toBe(1); // No retries with maxAttempts=1
      expect(result.success).toBe(false);
    });

    it('should preserve tags written before retry', async () => {
      let attempts = 0;

      const tracer = new TestTracer(testOpContext, createTestTracerOptions());

      const result = await tracer.trace('root', async (ctx) => {
        return ctx.span('tagPreservation', async (spanCtx) => {
          attempts++;
          // Write tag on first attempt
          if (attempts === 1) {
            spanCtx.tag.operation('INSERT').service('payment-api');
          }
          if (attempts < 2) {
            return spanCtx.err(SERVICE_UNAVAILABLE({ status: 503 }));
          }
          return spanCtx.ok({ success: true });
        });
      });

      expect(result.success).toBe(true);

      // Verify tags were preserved through retry
      const rootBuffer = tracer.rootBuffers[0];
      const spanBuffer = rootBuffer._children[0];

      // Tags are written at row 0 (span-start)
      const operations = spanBuffer.getColumnIfAllocated('operation') as string[];
      const services = spanBuffer.getColumnIfAllocated('service') as string[];
      expect(operations[0]).toBe('INSERT');
      expect(services[0]).toBe('payment-api');
    });

    it('should work with nested child spans containing retry logic', async () => {
      let childAttempts = 0;

      const tracer = new TestTracer(testOpContext, createTestTracerOptions());

      const result = await tracer.trace('root', async (rootCtx) => {
        return rootCtx.span('parent', async (parentCtx) => {
          const childResult = await parentCtx.span('childWithRetry', async (childCtx) => {
            childAttempts++;
            if (childAttempts < 2) {
              return childCtx.err(SERVICE_UNAVAILABLE({ status: 503 }));
            }
            return childCtx.ok({ child: true });
          });

          if (!childResult.success) {
            // biome-ignore lint/suspicious/noExplicitAny: Testing error propagation
            return parentCtx.err({ code: 'CHILD_FAILED' } as any);
          }

          return parentCtx.ok({ parent: true, childResult: childResult.value });
        });
      });

      expect(result.success).toBe(true);
      expect(childAttempts).toBe(2);
    });

    it('should handle multiple retries', async () => {
      let attempts = 0;

      // Use 5 retries to fit within default buffer capacity of 8
      // (span-start, span-ok/err, 4 retry entries = 6 rows)
      const multiRetryError = Transient<void>('MULTI_RETRY', {
        backoff: 'fixed',
        maxAttempts: 5,
        baseDelayMs: 0,
        jitter: false,
      });

      const tracer = new TestTracer(testOpContext, createTestTracerOptions());

      const result = await tracer.trace('root', async (ctx) => {
        return ctx.span('multiRetries', async (spanCtx) => {
          attempts++;
          if (attempts < 5) {
            return spanCtx.err(multiRetryError());
          }
          return spanCtx.ok({ success: true });
        });
      });

      expect(result.success).toBe(true);
      expect(attempts).toBe(5);

      // Verify 4 retry entries (attempts 1-4 failed)
      const spanBuffer = tracer.rootBuffers[0]._children[0];
      const entryTypes = spanBuffer.entry_type;
      let retryCount = 0;
      for (let i = 0; i < spanBuffer._writeIndex; i++) {
        if (entryTypes[i] === ENTRY_TYPE_SPAN_RETRY) {
          retryCount++;
        }
      }
      expect(retryCount).toBe(4);
    });

    it('should work with defineOp inside span', async () => {
      let attempts = 0;

      const retryOp = defineOp('retryOp', async (ctx) => {
        attempts++;
        if (attempts < 2) {
          return ctx.err(SERVICE_UNAVAILABLE({ status: 503 }));
        }
        return ctx.ok({ done: true });
      });

      const tracer = new TestTracer(testOpContext, createTestTracerOptions());

      const result = await tracer.trace('root', async (ctx) => {
        // Using Op via ctx.span triggers the retry logic
        return ctx.span('op-span', retryOp);
      });

      expect(attempts).toBe(2);
      expect(result.success).toBe(true);
    });
  });
});
