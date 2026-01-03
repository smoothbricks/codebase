/**
 * Tests for TypeScript type narrowing with FluentResult
 * Ensures that result.success properly narrows types
 */

import { describe, expect, it } from 'bun:test';
import { defineOpContext } from '../defineOpContext.js';
import { defineCodeError } from '../result.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { createTraceRoot } from '../traceRoot.node.js';
import { TestTracer } from '../tracers/TestTracer.js';

// Error code factories for tests
const TEST_ERROR = defineCodeError('TEST_ERROR')<{ field: string; reason: string }>();
const VALIDATION_ERROR = defineCodeError('VALIDATION_ERROR')<{ message: string }>();

const testSchema = defineLogSchema({
  userId: S.category(),
  customField: S.category(),
});

// Create op context factory
const opContext = defineOpContext({
  logSchema: testSchema,
  ctx: {
    requestId: undefined as string | undefined,
  },
});

const { defineOp } = opContext;

// Create a properly typed tracer - new API passes opContext directly
function createTestTracer() {
  return new TestTracer(opContext, { createTraceRoot });
}

describe('Type Narrowing with FluentResult', () => {
  it('should properly narrow success result type', async () => {
    const { trace } = createTestTracer();

    const testOp = defineOp('testOp', async (ctx) => {
      const result = ctx.ok({ id: 123, name: 'test' });

      // Type narrowing should work with FluentResult
      if (result.success) {
        // result.value should be accessible and typed correctly
        const value = result.value;
        expect(value.id).toBe(123);
        expect(value.name).toBe('test');
        return result;
      }

      return ctx.ok(null);
    });

    const output = await trace('testOp', testOp);
    expect(output.success).toBe(true);
    if (output.success) {
      expect(output.value).toEqual({ id: 123, name: 'test' });
    }
  });

  it('should properly narrow error result type', async () => {
    const { trace } = createTestTracer();

    const testOp = defineOp('testOp', async (ctx) => {
      const result = ctx.err(TEST_ERROR({ field: 'email', reason: 'invalid' }));

      // Type narrowing should work with FluentResult
      if (!result.success) {
        // result.error should be accessible and typed correctly
        const error = result.error;
        expect(error.code).toBe('TEST_ERROR');
        expect(error.field).toBe('email');
        expect(error.reason).toBe('invalid');
        return ctx.ok(error.code);
      }

      return ctx.ok(null);
    });

    const output = await trace('testOp', testOp);
    expect(output.success).toBe(true);
    if (output.success) {
      expect(output.value).toBe('TEST_ERROR');
    }
  });

  it('should support chaining before type check', async () => {
    const { trace } = createTestTracer();

    const testOp = defineOp('testOp', async (ctx) => {
      const result = ctx.ok({ id: 456 }).with({ userId: 'user1' }).message('Success');

      // Type narrowing should still work after chaining
      if (result.success) {
        expect(result.value.id).toBe(456);
        return ctx.ok(result.value.id);
      }

      return ctx.ok(0);
    });

    const output = await trace('testOp', testOp);
    expect(output.success).toBe(true);
    if (output.success) {
      expect(output.value).toBe(456);
    }
  });

  it('should handle error result with chaining', async () => {
    const { trace } = createTestTracer();

    const testOp = defineOp('testOp', async (ctx) => {
      const result = ctx
        .err(VALIDATION_ERROR({ message: 'Invalid input' }))
        .with({ userId: 'user1' })
        .message('Validation failed');

      // Type narrowing should work after chaining
      if (!result.success) {
        expect(result.error.code).toBe('VALIDATION_ERROR');
        expect(result.error.message).toBe('Invalid input');
        return ctx.ok(result.error.code);
      }

      return ctx.ok('OK');
    });

    const output = await trace('testOp', testOp);
    expect(output.success).toBe(true);
    if (output.success) {
      expect(output.value).toBe('VALIDATION_ERROR');
    }
  });
});
