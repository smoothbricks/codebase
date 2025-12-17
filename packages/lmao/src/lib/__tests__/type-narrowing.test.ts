/**
 * Tests for TypeScript type narrowing with FluentResult
 * Ensures that result.success properly narrows types
 */

import { describe, expect, it } from 'bun:test';
import { createModuleContext, createRequestContext } from '../lmao.js';
import { S } from '../schema/builder.js';
import { defineFeatureFlags } from '../schema/defineFeatureFlags.js';
import { defineTagAttributes } from '../schema/defineTagAttributes.js';
import { InMemoryFlagEvaluator } from '../schema/evaluator.js';

const testSchema = defineTagAttributes({
  userId: S.category(),
  customField: S.category(),
});

const testFlags = defineFeatureFlags({
  testFlag: S.boolean().default(true).sync(),
});

const mockEvaluator = new InMemoryFlagEvaluator({
  testFlag: true,
});

describe('Type Narrowing with FluentResult', () => {
  it('should properly narrow success result type', async () => {
    const moduleCtx = createModuleContext({
      moduleMetadata: {
        gitSha: 'abc123',
        packageName: '@test/pkg',
        packagePath: '/test/module.ts',
      },
      tagAttributes: testSchema,
    });

    const task = moduleCtx.task('testTask', async (ctx) => {
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

    const requestCtx = createRequestContext({ requestId: 'req1' }, testFlags, mockEvaluator, {});

    const output = await task(requestCtx);
    expect(output).toEqual({ id: 123, name: 'test' });
  });

  it('should properly narrow error result type', async () => {
    const moduleCtx = createModuleContext({
      moduleMetadata: {
        gitSha: 'abc123',
        packageName: '@test/pkg',
        packagePath: '/test/module.ts',
      },
      tagAttributes: testSchema,
    });

    const task = moduleCtx.task('testTask', async (ctx) => {
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

    const requestCtx = createRequestContext({ requestId: 'req1' }, testFlags, mockEvaluator, {});

    const output = await task(requestCtx);
    expect(output).toBe('TEST_ERROR');
  });

  it('should support chaining before type check', async () => {
    const moduleCtx = createModuleContext({
      moduleMetadata: {
        gitSha: 'abc123',
        packageName: '@test/pkg',
        packagePath: '/test/module.ts',
      },
      tagAttributes: testSchema,
    });

    const task = moduleCtx.task('testTask', async (ctx) => {
      const result = ctx.ok({ id: 456 }).with({ userId: 'user1' }).message('Success');

      // Type narrowing should still work after chaining
      if (result.success) {
        expect(result.value.id).toBe(456);
        return result.value.id;
      }

      return 0;
    });

    const requestCtx = createRequestContext({ requestId: 'req1' }, testFlags, mockEvaluator, {});

    const output = await task(requestCtx);
    expect(output).toBe(456);
  });

  it('should handle error result with chaining', async () => {
    const moduleCtx = createModuleContext({
      moduleMetadata: {
        gitSha: 'abc123',
        packageName: '@test/pkg',
        packagePath: '/test/module.ts',
      },
      tagAttributes: testSchema,
    });

    const task = moduleCtx.task('testTask', async (ctx) => {
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

    const requestCtx = createRequestContext({ requestId: 'req1' }, testFlags, mockEvaluator, {});

    const output = await task(requestCtx);
    expect(output).toBe('VALIDATION_ERROR');
  });
});
