/**
 * Tests for TypeScript type narrowing with FluentResult
 * Ensures that result.success properly narrows types
 */

import { describe, expect, it } from 'bun:test';
import { defineModule } from '../defineModule.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';

const testSchema = defineLogSchema({
  userId: S.category(),
  customField: S.category(),
});

// Create a simple module for testing
function createTestModule() {
  return defineModule({
    metadata: {
      git_sha: 'abc123',
      package_name: '@test/pkg',
      package_file: '/test/module.ts',
    },
    logSchema: testSchema,
  })
    .ctx<{ requestId?: string }>({
      requestId: undefined,
    })
    .make();
}

describe('Type Narrowing with FluentResult', () => {
  it('should properly narrow success result type', async () => {
    const testModule = createTestModule();

    const testOp = testModule.op('testOp', async (ctx) => {
      const result = ctx.ok({ id: 123, name: 'test' });

      // Type narrowing should work with FluentResult
      if (result.success) {
        // result.value should be accessible and typed correctly
        const value = result.value;
        expect(value.id).toBe(123);
        expect(value.name).toBe('test');
        return value;
      }

      return null;
    });

    const traceCtx = testModule.traceContext({ requestId: 'req1' });

    const output = await traceCtx.span('testOp', testOp);
    expect(output).toEqual({ id: 123, name: 'test' });
  });

  it('should properly narrow error result type', async () => {
    const testModule = createTestModule();

    const testOp = testModule.op('testOp', async (ctx) => {
      const result = ctx.err('TEST_ERROR', { field: 'email', reason: 'invalid' });

      // Type narrowing should work with FluentResult
      if (!result.success) {
        // result.error should be accessible and typed correctly
        const error = result.error;
        expect(error.code).toBe('TEST_ERROR');
        expect(error.details.field).toBe('email');
        expect(error.details.reason).toBe('invalid');
        return error.code;
      }

      return null;
    });

    const traceCtx = testModule.traceContext({ requestId: 'req1' });

    const output = await traceCtx.span('testOp', testOp);
    expect(output).toBe('TEST_ERROR');
  });

  it('should support chaining before type check', async () => {
    const testModule = createTestModule();

    const testOp = testModule.op('testOp', async (ctx) => {
      const result = ctx.ok({ id: 456 }).with({ userId: 'user1' }).message('Success');

      // Type narrowing should still work after chaining
      if (result.success) {
        expect(result.value.id).toBe(456);
        return result.value.id;
      }

      return 0;
    });

    const traceCtx = testModule.traceContext({ requestId: 'req1' });

    const output = await traceCtx.span('testOp', testOp);
    expect(output).toBe(456);
  });

  it('should handle error result with chaining', async () => {
    const testModule = createTestModule();

    const testOp = testModule.op('testOp', async (ctx) => {
      const result = ctx
        .err('VALIDATION_ERROR', { message: 'Invalid input' })
        .with({ userId: 'user1' })
        .message('Validation failed');

      // Type narrowing should work after chaining
      if (!result.success) {
        expect(result.error.code).toBe('VALIDATION_ERROR');
        expect(result.error.details.message).toBe('Invalid input');
        return result.error.code;
      }

      return 'OK';
    });

    const traceCtx = testModule.traceContext({ requestId: 'req1' });

    const output = await traceCtx.span('testOp', testOp);
    expect(output).toBe('VALIDATION_ERROR');
  });
});
