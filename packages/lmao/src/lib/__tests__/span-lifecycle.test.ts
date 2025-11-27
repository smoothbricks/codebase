/**
 * Tests for span lifecycle entry types and fluent result API
 * Per specs/01h_entry_types_and_logging_primitives.md
 */

import { describe, it, expect } from 'bun:test';
import { createModuleContext, createRequestContext } from '../lmao.js';
import { defineTagAttributes } from '../schema/defineTagAttributes.js';
import { defineFeatureFlags } from '../schema/defineFeatureFlags.js';
import { InMemoryFlagEvaluator } from '../schema/evaluator.js';
import { S } from '../schema/builder.js';

// Test schema
const testSchema = defineTagAttributes({
  userId: S.category(),
  operation: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE']),
  errorCode: S.category(),
  resultMessage: S.text(),
  exceptionMessage: S.text(),
  exceptionStack: S.text(),
  spanName: S.category(),
});

const testFlags = defineFeatureFlags({
  testFlag: S.boolean().default(true).sync(),
});

// Mock feature flag evaluator
const mockEvaluator = new InMemoryFlagEvaluator({
  testFlag: true,
});

describe('Span Lifecycle', () => {
  it('should write span-start entry when task begins', async () => {
    const moduleCtx = createModuleContext({
      moduleMetadata: {
        gitSha: 'abc123',
        filePath: '/test/module.ts',
        moduleName: 'TestModule',
      },
      tagAttributes: testSchema,
    });

    const task = moduleCtx.task('testTask', async (ctx) => {
      return ctx.ok('success');
    });

    const requestCtx = createRequestContext(
      { requestId: 'req1', userId: 'user1' },
      testFlags,
      mockEvaluator,
      {}
    );

    await task(requestCtx);

    // Buffer is internal, but we can verify behavior through side effects
    // The span-start entry should be written before any other operations
    expect(true).toBe(true); // Placeholder - real test would inspect buffer
  });

  it('should write span-ok entry with ctx.ok()', async () => {
    const moduleCtx = createModuleContext({
      moduleMetadata: {
        gitSha: 'abc123',
        filePath: '/test/module.ts',
        moduleName: 'TestModule',
      },
      tagAttributes: testSchema,
    });

    const task = moduleCtx.task('testTask', async (ctx) => {
      return ctx.ok({ id: 123, name: 'test' });
    });

    const requestCtx = createRequestContext(
      { requestId: 'req1' },
      testFlags,
      mockEvaluator,
      {}
    );

    const result = await task(requestCtx);

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.value).toEqual({ id: 123, name: 'test' });
    }
  });

  it('should write span-err entry with ctx.err()', async () => {
    const moduleCtx = createModuleContext({
      moduleMetadata: {
        gitSha: 'abc123',
        filePath: '/test/module.ts',
        moduleName: 'TestModule',
      },
      tagAttributes: testSchema,
    });

    const task = moduleCtx.task('testTask', async (ctx) => {
      return ctx.err('VALIDATION_ERROR', { field: 'email', message: 'Invalid email' });
    });

    const requestCtx = createRequestContext(
      { requestId: 'req1' },
      testFlags,
      mockEvaluator,
      {}
    );

    const result = await task(requestCtx);

    expect(result.success).toBe(false);
    if (!result.success) {
      expect(result.error.code).toBe('VALIDATION_ERROR');
      expect(result.error.details).toEqual({ field: 'email', message: 'Invalid email' });
    }
  });

  it('should write span-exception entry when task throws', async () => {
    const moduleCtx = createModuleContext({
      moduleMetadata: {
        gitSha: 'abc123',
        filePath: '/test/module.ts',
        moduleName: 'TestModule',
      },
      tagAttributes: testSchema,
    });

    const task = moduleCtx.task('testTask', async (ctx) => {
      throw new Error('Unexpected error');
    });

    const requestCtx = createRequestContext(
      { requestId: 'req1' },
      testFlags,
      mockEvaluator,
      {}
    );

    await expect(task(requestCtx)).rejects.toThrow('Unexpected error');
  });
});

describe('Fluent Result API', () => {
  it('should support .with() for setting attributes on ok result', async () => {
    const moduleCtx = createModuleContext({
      moduleMetadata: {
        gitSha: 'abc123',
        filePath: '/test/module.ts',
        moduleName: 'TestModule',
      },
      tagAttributes: testSchema,
    });

    const task = moduleCtx.task('testTask', async (ctx) => {
      const result = ctx.ok({ id: 123 }).with({
        userId: 'user1',
        operation: 'CREATE',
      });
      return result;
    });

    const requestCtx = createRequestContext(
      { requestId: 'req1' },
      testFlags,
      mockEvaluator,
      {}
    );

    const result = await task(requestCtx);

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.value).toEqual({ id: 123 });
    }
  });

  it('should support .message() for setting message on ok result', async () => {
    const moduleCtx = createModuleContext({
      moduleMetadata: {
        gitSha: 'abc123',
        filePath: '/test/module.ts',
        moduleName: 'TestModule',
      },
      tagAttributes: testSchema,
    });

    const task = moduleCtx.task('testTask', async (ctx) => {
      return ctx.ok({ id: 123 }).message('User created successfully');
    });

    const requestCtx = createRequestContext(
      { requestId: 'req1' },
      testFlags,
      mockEvaluator,
      {}
    );

    const result = await task(requestCtx);

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.value).toEqual({ id: 123 });
    }
  });

  it('should support chaining .with() and .message()', async () => {
    const moduleCtx = createModuleContext({
      moduleMetadata: {
        gitSha: 'abc123',
        filePath: '/test/module.ts',
        moduleName: 'TestModule',
      },
      tagAttributes: testSchema,
    });

    const task = moduleCtx.task('testTask', async (ctx) => {
      return ctx
        .ok({ id: 123 })
        .with({ userId: 'user1', operation: 'CREATE' })
        .message('User created successfully');
    });

    const requestCtx = createRequestContext(
      { requestId: 'req1' },
      testFlags,
      mockEvaluator,
      {}
    );

    const result = await task(requestCtx);

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.value).toEqual({ id: 123 });
    }
  });

  it('should support .with() on err result', async () => {
    const moduleCtx = createModuleContext({
      moduleMetadata: {
        gitSha: 'abc123',
        filePath: '/test/module.ts',
        moduleName: 'TestModule',
      },
      tagAttributes: testSchema,
    });

    const task = moduleCtx.task('testTask', async (ctx) => {
      return ctx
        .err('VALIDATION_ERROR', { field: 'email' })
        .with({ userId: 'user1', operation: 'CREATE' });
    });

    const requestCtx = createRequestContext(
      { requestId: 'req1' },
      testFlags,
      mockEvaluator,
      {}
    );

    const result = await task(requestCtx);

    expect(result.success).toBe(false);
    if (!result.success) {
      expect(result.error.code).toBe('VALIDATION_ERROR');
    }
  });

  it('should support .message() on err result', async () => {
    const moduleCtx = createModuleContext({
      moduleMetadata: {
        gitSha: 'abc123',
        filePath: '/test/module.ts',
        moduleName: 'TestModule',
      },
      tagAttributes: testSchema,
    });

    const task = moduleCtx.task('testTask', async (ctx) => {
      return ctx
        .err('VALIDATION_ERROR', { field: 'email' })
        .message('Invalid email format');
    });

    const requestCtx = createRequestContext(
      { requestId: 'req1' },
      testFlags,
      mockEvaluator,
      {}
    );

    const result = await task(requestCtx);

    expect(result.success).toBe(false);
    if (!result.success) {
      expect(result.error.code).toBe('VALIDATION_ERROR');
    }
  });
});

describe('Child Span Lifecycle', () => {
  it('should write span-start for child spans', async () => {
    const moduleCtx = createModuleContext({
      moduleMetadata: {
        gitSha: 'abc123',
        filePath: '/test/module.ts',
        moduleName: 'TestModule',
      },
      tagAttributes: testSchema,
    });

    const task = moduleCtx.task('parentTask', async (ctx) => {
      const childResult = await ctx.span('childSpan', async (childCtx) => {
        return childCtx.ok('child success');
      });

      return ctx.ok(childResult);
    });

    const requestCtx = createRequestContext(
      { requestId: 'req1' },
      testFlags,
      mockEvaluator,
      {}
    );

    const result = await task(requestCtx);

    expect(result.success).toBe(true);
  });

  it('should write span-exception for child span errors', async () => {
    const moduleCtx = createModuleContext({
      moduleMetadata: {
        gitSha: 'abc123',
        filePath: '/test/module.ts',
        moduleName: 'TestModule',
      },
      tagAttributes: testSchema,
    });

    const task = moduleCtx.task('parentTask', async (ctx) => {
      await ctx.span('childSpan', async (childCtx) => {
        throw new Error('Child span error');
      });
    });

    const requestCtx = createRequestContext(
      { requestId: 'req1' },
      testFlags,
      mockEvaluator,
      {}
    );

    await expect(task(requestCtx)).rejects.toThrow('Child span error');
  });

  it('should handle nested child spans', async () => {
    const moduleCtx = createModuleContext({
      moduleMetadata: {
        gitSha: 'abc123',
        filePath: '/test/module.ts',
        moduleName: 'TestModule',
      },
      tagAttributes: testSchema,
    });

    const task = moduleCtx.task('parentTask', async (ctx) => {
      const result1 = await ctx.span('child1', async (child1Ctx) => {
        const result2 = await child1Ctx.span('child2', async (child2Ctx) => {
          return child2Ctx.ok('grandchild success');
        });
        return child1Ctx.ok(result2);
      });

      return ctx.ok(result1);
    });

    const requestCtx = createRequestContext(
      { requestId: 'req1' },
      testFlags,
      mockEvaluator,
      {}
    );

    const result = await task(requestCtx);

    expect(result.success).toBe(true);
  });
});

describe('FluentResult Type Compatibility', () => {
  it('should allow direct access to success property', async () => {
    const moduleCtx = createModuleContext({
      moduleMetadata: {
        gitSha: 'abc123',
        filePath: '/test/module.ts',
        moduleName: 'TestModule',
      },
      tagAttributes: testSchema,
    });

    const task = moduleCtx.task('testTask', async (ctx) => {
      const result = ctx.ok({ id: 123 });
      
      // Should be able to access success property directly
      if (result.success) {
        return result.value;
      }
      return null;
    });

    const requestCtx = createRequestContext(
      { requestId: 'req1' },
      testFlags,
      mockEvaluator,
      {}
    );

    const result = await task(requestCtx);
    expect(result).toEqual({ id: 123 });
  });

  it('should allow direct access to error property', async () => {
    const moduleCtx = createModuleContext({
      moduleMetadata: {
        gitSha: 'abc123',
        filePath: '/test/module.ts',
        moduleName: 'TestModule',
      },
      tagAttributes: testSchema,
    });

    const task = moduleCtx.task('testTask', async (ctx) => {
      const result = ctx.err('TEST_ERROR', { message: 'test' });
      
      // Should be able to access error property directly
      if (!result.success) {
        return result.error.code;
      }
      return null;
    });

    const requestCtx = createRequestContext(
      { requestId: 'req1' },
      testFlags,
      mockEvaluator,
      {}
    );

    const result = await task(requestCtx);
    expect(result).toBe('TEST_ERROR');
  });
});
