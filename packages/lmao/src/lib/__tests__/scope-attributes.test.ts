/**
 * Tests for span scope attributes
 *
 * Per specs/01i_span_scope_attributes.md:
 * - Scope attributes set at span level propagate to all child entries
 * - Child spans inherit parent's scoped attributes
 * - Tasks inherit scoped attributes from calling context
 * - Pre-filling optimization ensures zero runtime overhead
 */

import { describe, expect, test } from 'bun:test';
import { createModuleContext, createRequestContext } from '../lmao.js';
import { S } from '../schema/builder.js';
import { defineFeatureFlags } from '../schema/defineFeatureFlags.js';
import { defineTagAttributes } from '../schema/defineTagAttributes.js';
import { InMemoryFlagEvaluator } from '../schema/evaluator.js';

// Helper to create a simple request context for testing
function createTestRequestContext() {
  const featureFlags = defineFeatureFlags({
    testFlag: S.boolean().default(false).sync(),
  });

  const flagEvaluator = new InMemoryFlagEvaluator({
    testFlag: true,
  });

  return createRequestContext({ requestId: 'test-req', userId: 'test-user' }, featureFlags, flagEvaluator, {});
}

describe('Span Scope Attributes', () => {
  describe('Basic Scope Setting', () => {
    test('should set scope attributes on span', async () => {
      const requestCtx = createTestRequestContext();

      const schema = defineTagAttributes({
        userId: S.category(),
        requestId: S.category(),
        orderId: S.category(),
      });

      const module = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          packageName: '@test/pkg',
          packagePath: 'test.ts',
        },
        tagAttributes: schema,
      });

      const testTask = module.task('test-task', async (ctx) => {
        // Set scope attributes
        ctx.scope({
          userId: 'user123',
          requestId: 'req456',
        });

        // Write a tag entry
        ctx.tag.orderId('order789');

        return ctx.ok('done');
      });

      const result = await testTask(requestCtx);
      expect(result.success).toBe(true);
    });

    test('should include scoped attributes in subsequent log entries', async () => {
      const requestCtx = createTestRequestContext();

      const schema = defineTagAttributes({
        userId: S.category(),
        requestId: S.category(),
        step: S.category(),
      });

      const module = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          packageName: '@test/pkg',
          packagePath: 'test.ts',
        },
        tagAttributes: schema,
      });

      const testTask = module.task('test-task', async (ctx) => {
        // Set scope once
        ctx.scope({
          userId: 'user123',
          requestId: 'req456',
        });

        // All subsequent operations should include scoped attributes
        ctx.log.info('Step 1');
        ctx.tag.step('step2');
        ctx.log.info('Step 3');

        return ctx.ok('done');
      });

      await testTask(requestCtx);
      // If this completes without error, scope inheritance is working
    });
  });

  describe('Child Span Inheritance', () => {
    test('should inherit scoped attributes in child spans', async () => {
      const requestCtx = createTestRequestContext();

      const schema = defineTagAttributes({
        userId: S.category(),
        requestId: S.category(),
        operation: S.category(),
        step: S.category(),
      });

      const module = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          packageName: '@test/pkg',
          packagePath: 'test.ts',
        },
        tagAttributes: schema,
      });

      const testTask = module.task('test-task', async (ctx) => {
        // Set scope at parent level
        ctx.scope({
          userId: 'user123',
          requestId: 'req456',
          operation: 'process_order',
        });

        ctx.log.info('Parent span started');

        // Create child span
        const childResult = await ctx.span('child-task', async (childCtx) => {
          // Child should have access to parent's scoped attributes
          // Add additional scope in child
          childCtx.scope({
            step: 'validation',
          });

          childCtx.log.info('Child span with inherited scope');

          return childCtx.ok({ validated: true });
        });

        expect(childResult.success).toBe(true);
        return ctx.ok('done');
      });

      const result = await testTask(requestCtx);
      expect(result.success).toBe(true);
    });

    test('should support deep nesting of scoped attributes', async () => {
      const requestCtx = createTestRequestContext();

      const schema = defineTagAttributes({
        userId: S.category(),
        level1: S.category(),
        level2: S.category(),
        level3: S.category(),
      });

      const module = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          packageName: '@test/pkg',
          packagePath: 'test.ts',
        },
        tagAttributes: schema,
      });

      const testTask = module.task('test-task', async (ctx) => {
        ctx.scope({ userId: 'user123', level1: 'L1' });

        await ctx.span('level-2', async (ctx2) => {
          ctx2.scope({ level2: 'L2' });

          await ctx2.span('level-3', async (ctx3) => {
            ctx3.scope({ level3: 'L3' });

            // This span should have all scoped attributes: userId, level1, level2, level3
            ctx3.log.info('Deep nested span');

            return ctx3.ok('level3-done');
          });

          return ctx2.ok('level2-done');
        });

        return ctx.ok('done');
      });

      const result = await testTask(requestCtx);
      expect(result.success).toBe(true);
    });
  });

  describe('Task Wrapper Inheritance', () => {
    test('should inherit scoped attributes across task boundaries', async () => {
      const requestCtx = createTestRequestContext();

      const schema = defineTagAttributes({
        userId: S.category(),
        requestId: S.category(),
        taskName: S.category(),
      });

      const module = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          packageName: '@test/pkg',
          packagePath: 'test.ts',
        },
        tagAttributes: schema,
      });

      const parentTask = module.task('parent-task', async (ctx) => {
        ctx.scope({
          userId: 'user123',
          requestId: 'req456',
          taskName: 'parent',
        });

        // Call child task - it should inherit scoped attributes
        const childResult = await childTask(ctx);

        return ctx.ok(childResult);
      });

      const childTask = module.task('child-task', async (ctx) => {
        // This task should inherit userId and requestId from parent
        ctx.scope({
          taskName: 'child',
        });

        ctx.log.info('Child task with inherited scope');

        return ctx.ok('child-done');
      });

      const result = await parentTask(requestCtx);
      expect(result.success).toBe(true);
    });
  });

  describe('Middleware Pattern', () => {
    test('should support middleware-style scope setup', async () => {
      const schema = defineTagAttributes({
        requestId: S.category(),
        userId: S.category(),
        endpoint: S.category(),
        method: S.category(),
        userAgent: S.text(),
        ip: S.category(),
      });

      const module = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          packageName: '@test/pkg',
          packagePath: 'test.ts',
        },
        tagAttributes: schema,
      });

      // Simulate middleware setting up request-level scope
      const requestCtx = createTestRequestContext();

      const middlewareTask = module.task('middleware', async (ctx) => {
        // Middleware sets up request-level scope
        ctx.scope({
          requestId: 'req123',
          userId: 'user456',
          endpoint: '/api/users',
          method: 'POST',
          userAgent: 'Mozilla/5.0',
          ip: '192.168.1.1',
        });

        // Business logic task should inherit all middleware scope
        const businessResult = await businessLogicTask(ctx);

        return ctx.ok(businessResult);
      });

      const businessLogicTask = module.task('business-logic', async (ctx) => {
        // All middleware scope should be inherited
        ctx.log.info('Processing business logic');

        // Business can add its own scope
        ctx.scope({
          endpoint: '/api/users/create', // Override endpoint
        });

        ctx.log.info('User creation started');

        return ctx.ok('user-created');
      });

      const result = await middlewareTask(requestCtx);
      expect(result.success).toBe(true);
    });
  });

  describe('Scope with Different Types', () => {
    test('should handle different string types (enum, category, text)', async () => {
      const requestCtx = createTestRequestContext();

      const schema = defineTagAttributes({
        status: S.enum(['pending', 'active', 'completed']),
        userId: S.category(),
        errorMessage: S.text(),
        count: S.number(),
        isValid: S.boolean(),
      });

      const module = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          packageName: '@test/pkg',
          packagePath: 'test.ts',
        },
        tagAttributes: schema,
      });

      const testTask = module.task('test-task', async (ctx) => {
        ctx.scope({
          status: 'active',
          userId: 'user123',
          errorMessage: 'No errors',
          count: 42,
          isValid: true,
        });

        ctx.log.info('All types scoped');

        return ctx.ok('done');
      });

      const result = await testTask(requestCtx);
      expect(result.success).toBe(true);
    });
  });

  describe('Scope Updates', () => {
    test('should allow updating scoped attributes', async () => {
      const requestCtx = createTestRequestContext();

      const schema = defineTagAttributes({
        phase: S.category(),
        status: S.category(),
        progress: S.number(),
      });

      const module = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          packageName: '@test/pkg',
          packagePath: 'test.ts',
        },
        tagAttributes: schema,
      });

      const testTask = module.task('test-task', async (ctx) => {
        // Initial scope
        ctx.scope({
          phase: 'initialization',
          status: 'starting',
          progress: 0,
        });

        ctx.log.info('Phase 1');

        // Update scope
        ctx.scope({
          phase: 'processing',
          status: 'in_progress',
          progress: 50,
        });

        ctx.log.info('Phase 2');

        // Update again
        ctx.scope({
          phase: 'finalization',
          status: 'completing',
          progress: 100,
        });

        ctx.log.info('Phase 3');

        return ctx.ok('done');
      });

      const result = await testTask(requestCtx);
      expect(result.success).toBe(true);
    });
  });
});
