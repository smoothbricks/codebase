/**
 * Integration tests for schema integration patterns
 *
 * Tests the full integration of:
 * - Request context creation with feature flags and environment
 * - Module context with tag attributes
 * - Task wrappers with span context
 * - Feature flag analytics tracking
 * - Typed tag attribute API
 */

import { beforeEach, describe, expect, it } from 'bun:test';
import { createModuleContext, createRequestContext } from '../lmao.js';
import { S } from '../schema/builder.js';
import { defineFeatureFlags } from '../schema/defineFeatureFlags.js';
import { defineTagAttributes } from '../schema/defineTagAttributes.js';
import { InMemoryFlagEvaluator } from '../schema/evaluator.js';
import type { TagAttributeSchema } from '../schema/types.js';

describe('Schema Integration Patterns', () => {
  // Define tag attributes for DB operations
  // Using the three string types per specs/01a_trace_schema_system.md:
  // - S.enum: Known values at compile time
  // - S.category: Values that often repeat
  // - S.text: Unique values
  const dbAttributes = defineTagAttributes({
    requestId: S.category(), // Category: request IDs repeat within traces
    userId: S.category(), // Category: user IDs repeat across operations
    duration: S.number(),
    httpStatus: S.number(),
    operation: S.enum(['SELECT', 'INSERT', 'UPDATE', 'DELETE']), // Enum: known DB operations
    query: S.text(), // Text: SQL queries are mostly unique
    region: S.category(), // Category: AWS regions have limited cardinality
  });

  // Define feature flags
  const featureFlags = defineFeatureFlags({
    advancedValidation: S.boolean().default(false).sync(),
    maxRetries: S.number().default(3).sync(),
    experimentalFeature: S.boolean().default(false).async(),
  });

  // Environment config
  const environmentConfig = {
    awsRegion: 'us-east-1',
    maxConnections: 100,
    databaseUrl: 'postgresql://localhost:5432/test',
  };

  let flagEvaluator: InMemoryFlagEvaluator;

  beforeEach(() => {
    flagEvaluator = new InMemoryFlagEvaluator({
      advancedValidation: true,
      maxRetries: 5,
      experimentalFeature: false,
    });
  });

  describe('createRequestContext', () => {
    it('should create request context with feature flags and environment', () => {
      const ctx = createRequestContext(
        { requestId: 'req-123', userId: 'user-456' },
        featureFlags,
        flagEvaluator,
        environmentConfig,
      );

      expect(ctx.requestId).toBe('req-123');
      expect(ctx.userId).toBe('user-456');
      expect(ctx.traceId).toMatch(/^trace-/);
      expect(ctx.ff).toBeDefined();
      expect(ctx.env).toBe(environmentConfig);
    });

    it('should provide access to environment config as plain properties', () => {
      const ctx = createRequestContext({ requestId: 'req-123' }, featureFlags, flagEvaluator, environmentConfig);

      expect(ctx.env.awsRegion).toBe('us-east-1');
      expect(ctx.env.maxConnections).toBe(100);
      expect(ctx.env.databaseUrl).toBe('postgresql://localhost:5432/test');
    });

    it('should create feature flag evaluator', () => {
      const ctx = createRequestContext({ requestId: 'req-123' }, featureFlags, flagEvaluator, environmentConfig);

      // Access sync flags as properties - returns { value, track } object when truthy
      // Use type assertions since TypeScript types don't match runtime API
      expect((ctx.ff.advancedValidation as unknown as { value: boolean }).value).toBe(true);
      expect((ctx.ff.maxRetries as unknown as { value: number }).value).toBe(5);
    });
  });

  describe('createModuleContext', () => {
    it('should create module context with tag attributes', () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/user.ts',
          moduleName: 'UserService',
        },
        tagAttributes: dbAttributes as unknown as TagAttributeSchema,
      });

      expect(moduleContext).toBeDefined();
      expect(moduleContext.task).toBeFunction();
    });

    it('should create task wrapper that provides span context', async () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/user.ts',
          moduleName: 'UserService',
        },
        tagAttributes: dbAttributes as unknown as TagAttributeSchema,
      });

      const testTask = moduleContext.task('test-task', async (ctx) => {
        expect(ctx.log).toBeDefined();
        expect(ctx.ff).toBeDefined();
        expect(ctx.env).toBeDefined();
        expect(ctx.ok).toBeFunction();
        expect(ctx.err).toBeFunction();
        expect(ctx.span).toBeFunction();
        return ctx.ok({ success: true });
      });

      const requestCtx = createRequestContext({ requestId: 'req-123' }, featureFlags, flagEvaluator, environmentConfig);

      const result = await testTask(requestCtx);
      expect(result.success).toBe(true);
    });
  });

  describe('Method Chaining', () => {
    it('should chain multiple tag methods fluently and write to buffers', async () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/test.ts',
          moduleName: 'TestService',
        },
        tagAttributes: dbAttributes,
      });

      const testTask = moduleContext.task('test-chaining', async (ctx) => {
        // Each method returns the tag object for chaining
        const result = ctx.tag
          .requestId('req-001')
          .userId('user-001')
          .operation('SELECT')
          .duration(10.5)
          .httpStatus(200)
          .region('us-west-2')
          .query('SELECT * FROM users WHERE id = ?');

        // Verify we can still chain after the chain
        expect(result).toBeDefined();
        expect(typeof result.requestId).toBe('function');

        return ctx.ok({ chained: true });
      });

      const requestCtx = createRequestContext({ requestId: 'req-001' }, featureFlags, flagEvaluator, environmentConfig);

      const result = await testTask(requestCtx);
      expect(result.success).toBe(true);

      // Note: Buffer writes are happening in memory to Arrow columnar format
      // Each tag method call writes to the appropriate attribute column
    });

    it('should chain with() method and continue chaining', async () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/test.ts',
          moduleName: 'TestService',
        },
        tagAttributes: dbAttributes as unknown as TagAttributeSchema,
      });

      const testTask = moduleContext.task('test-with-chaining', async (ctx) => {
        // with() returns the tag object, allowing continued chaining
        ctx.tag
          .with({
            requestId: 'req-002',
            userId: 'user-002',
          })
          .operation('INSERT')
          .duration(15.2)
          .with({
            httpStatus: 201,
            region: 'eu-west-1',
          })
          .query('INSERT INTO users VALUES (...)');

        return ctx.ok({ chained: true });
      });

      const requestCtx = createRequestContext({ requestId: 'req-002' }, featureFlags, flagEvaluator, environmentConfig);

      const result = await testTask(requestCtx);
      expect(result.success).toBe(true);
    });

    it('should support real-world chaining pattern: orderId and amount', async () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/order.ts',
          moduleName: 'OrderService',
        },
        tagAttributes: dbAttributes as unknown as TagAttributeSchema,
      });

      const testTask = moduleContext.task('process-order', async (ctx, orderId: string, amount: number) => {
        // Example from requirements: ctx.tag.orderId(order.id).amount(order.total)
        // Using available attributes to simulate
        ctx.tag
          .requestId(orderId)
          .userId(ctx.userId || 'guest')
          .duration(amount)
          .operation('INSERT')
          .httpStatus(201);

        return ctx.ok({ orderId, amount });
      });

      const requestCtx = createRequestContext(
        { requestId: 'req-003', userId: 'user-003' },
        featureFlags,
        flagEvaluator,
        environmentConfig,
      );

      const result = await testTask(requestCtx, 'order-789', 149.99);
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.value.orderId).toBe('order-789');
        expect(result.value.amount).toBe(149.99);
      }
    });
  });

  describe('Task Integration', () => {
    it('should provide typed tag attribute API with method chaining', async () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/user.ts',
          moduleName: 'UserService',
        },
        tagAttributes: dbAttributes as unknown as TagAttributeSchema,
      });

      const testTask = moduleContext.task('test-task', async (ctx) => {
        // Method chaining - each method returns the tag object
        ctx.tag.requestId('req-123').userId('user-456').operation('INSERT').duration(12.5).httpStatus(200);

        // Can also chain after with()
        ctx.tag
          .with({
            requestId: 'req-456',
            userId: 'user-789',
          })
          .operation('SELECT')
          .duration(8.3);

        // Single calls still work
        ctx.tag.httpStatus(201);

        return ctx.ok({ success: true });
      });

      const requestCtx = createRequestContext({ requestId: 'req-123' }, featureFlags, flagEvaluator, environmentConfig);

      const result = await testTask(requestCtx);
      expect(result.success).toBe(true);
    });

    it('should provide message logging methods', async () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/user.ts',
          moduleName: 'UserService',
        },
        tagAttributes: dbAttributes as unknown as TagAttributeSchema,
      });

      const testTask = moduleContext.task('test-task', async (ctx) => {
        ctx.log.info('Info message');
        ctx.log.debug('Debug message');
        ctx.log.warn('Warning message');
        ctx.log.error('Error message');
        ctx.log.message('info', 'Custom message');

        return ctx.ok({ success: true });
      });

      const requestCtx = createRequestContext({ requestId: 'req-123' }, featureFlags, flagEvaluator, environmentConfig);

      const result = await testTask(requestCtx);
      expect(result.success).toBe(true);
    });

    it('should provide ok/err result helpers', async () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/user.ts',
          moduleName: 'UserService',
        },
        tagAttributes: dbAttributes as unknown as TagAttributeSchema,
      });

      const successTask = moduleContext.task('success-task', async (ctx) => {
        return ctx.ok({ data: 'result' });
      });

      const errorTask = moduleContext.task('error-task', async (ctx) => {
        return ctx.err('VALIDATION_ERROR', { field: 'email' });
      });

      const requestCtx = createRequestContext({ requestId: 'req-123' }, featureFlags, flagEvaluator, environmentConfig);

      const successResult = await successTask(requestCtx);
      expect(successResult.success).toBe(true);
      if (successResult.success) {
        expect(successResult.value).toEqual({ data: 'result' });
      }

      const errorResult = await errorTask(requestCtx);
      expect(errorResult.success).toBe(false);
      if (!errorResult.success) {
        expect(errorResult.error.code).toBe('VALIDATION_ERROR');
        expect(errorResult.error.details).toEqual({ field: 'email' });
      }
    });

    it('should support child spans with chained tag methods', async () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/user.ts',
          moduleName: 'UserService',
        },
        tagAttributes: dbAttributes as unknown as TagAttributeSchema,
      });

      const testTask = moduleContext.task('parent-task', async (ctx) => {
        // Parent span with chained tags
        ctx.tag.requestId('req-123').operation('INSERT').duration(50.0);

        const childResult = await ctx.span('child-task', async (childCtx) => {
          // Child span with chained tags
          childCtx.log.tag.operation('SELECT').query('SELECT * FROM users').duration(5.2).httpStatus(200);

          return { found: true };
        });

        expect(childResult.found).toBe(true);

        return ctx.ok({ success: true });
      });

      const requestCtx = createRequestContext({ requestId: 'req-123' }, featureFlags, flagEvaluator, environmentConfig);

      const result = await testTask(requestCtx);
      expect(result.success).toBe(true);
    });
  });

  describe('Feature Flag Integration', () => {
    it('should write feature flag access to child buffer, not parent buffer', async () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/user.ts',
          moduleName: 'UserService',
        },
        tagAttributes: dbAttributes as unknown as TagAttributeSchema,
      });

      // Track that child span has its own ff evaluator bound to child buffer
      let parentFfRef: unknown = null;
      let childFfRef: unknown = null;

      const testTask = moduleContext.task('parent-task', async (ctx) => {
        parentFfRef = ctx.ff;

        // Access flag in parent - should log to parent buffer
        const parentFlag = ctx.ff.advancedValidation;
        expect(parentFlag).toBeDefined();

        await ctx.span('child-task', async (childCtx) => {
          childFfRef = childCtx.ff;

          // Child ff should be a DIFFERENT instance than parent ff
          // This ensures ff-access entries go to child buffer, not parent
          expect(childCtx.ff).not.toBe(ctx.ff);

          // Access same flag in child - should log to CHILD buffer (not parent)
          const childFlag = childCtx.ff.advancedValidation;
          expect(childFlag).toBeDefined();

          return { done: true };
        });

        return ctx.ok({ success: true });
      });

      const requestCtx = createRequestContext({ requestId: 'req-123' }, featureFlags, flagEvaluator, environmentConfig);
      await testTask(requestCtx);

      // Verify parent and child had different ff instances
      expect(parentFfRef).not.toBe(childFfRef);
    });

    it('should access feature flags in task context', async () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/user.ts',
          moduleName: 'UserService',
        },
        tagAttributes: dbAttributes as unknown as TagAttributeSchema,
      });

      const testTask = moduleContext.task('test-task', async (ctx) => {
        // Access sync flags as properties - returns { value, track } object when truthy
        // Use type assertions since TypeScript types don't match runtime API
        const shouldValidate = (ctx.ff.advancedValidation as unknown as { value: boolean }).value;
        const maxRetries = (ctx.ff.maxRetries as unknown as { value: number }).value;

        expect(shouldValidate).toBe(true);
        expect(maxRetries).toBe(5);

        if (shouldValidate) {
          ctx.log.info('Performing advanced validation');
        }

        // Track usage
        ctx.ff.trackUsage('advancedValidation', {
          action: 'validation_performed',
          outcome: 'success',
        });

        return ctx.ok({ validated: shouldValidate });
      });

      const requestCtx = createRequestContext({ requestId: 'req-123' }, featureFlags, flagEvaluator, environmentConfig);

      const result = await testTask(requestCtx);
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.value.validated).toBe(true);
      }
    });

    it('should access async feature flags', async () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/user.ts',
          moduleName: 'UserService',
        },
        tagAttributes: dbAttributes,
      });

      const testTask = moduleContext.task('test-task', async (ctx) => {
        // Async flags use method call
        // TypeScript can't properly infer async flag types through the generic chain
        // so we use a type assertion to access the specific flag type
        const experimentalEnabled = await (ctx.ff.get as (flag: 'experimentalFeature') => Promise<boolean>)(
          'experimentalFeature',
        );
        // The new FF API returns undefined when a flag is false
        expect(experimentalEnabled).toBeUndefined();

        return ctx.ok({ experimental: experimentalEnabled });
      });

      const requestCtx = createRequestContext({ requestId: 'req-123' }, featureFlags, flagEvaluator, environmentConfig);

      const result = await testTask(requestCtx);
      expect(result.success).toBe(true);
    });
  });

  describe('Method Chaining Examples', () => {
    it('should support various chaining patterns', async () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/order.ts',
          moduleName: 'OrderService',
        },
        tagAttributes: dbAttributes as unknown as TagAttributeSchema,
      });

      const processOrder = moduleContext.task('process-order', async (ctx, orderId: string, amount: number) => {
        // Example from requirements: ctx.tag.orderId(order.id).amount(order.total)
        ctx.tag
          .requestId(ctx.requestId)
          .userId(ctx.userId || 'anonymous')
          .operation('INSERT');

        // Chaining with with()
        ctx.tag.with({ region: 'us-east-1', httpStatus: 200 }).duration(25.5).query('INSERT INTO orders VALUES (...)');

        return ctx.ok({ orderId, processed: true });
      });

      const requestCtx = createRequestContext(
        { requestId: 'req-999', userId: 'user-123' },
        featureFlags,
        flagEvaluator,
        environmentConfig,
      );

      const result = await processOrder(requestCtx, 'order-456', 99.99);
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.value.orderId).toBe('order-456');
        expect(result.value.processed).toBe(true);
      }
    });

    it('should demonstrate complete integration with chaining', async () => {
      // Create module context
      const { task } = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/user.ts',
          moduleName: 'UserService',
        },
        tagAttributes: dbAttributes as unknown as TagAttributeSchema,
      });

      // Define task with extensive chaining
      const createUser = task('create-user', async (ctx, userData: { email: string; name: string }) => {
        // Feature flag access - returns { value, track } object when truthy
        if ((ctx.ff.advancedValidation as unknown as { value: boolean } | undefined)?.value) {
          ctx.log.info('Using advanced validation');
          ctx.ff.trackUsage('advancedValidation', {
            action: 'validation_performed',
            outcome: 'success',
          });
        }

        // Environment access and chained tags
        ctx.tag.requestId(ctx.requestId).userId(userData.email).region(ctx.env.awsRegion).operation('INSERT');

        // Child span with chaining
        const validation = await ctx.span('validate-user', async (childCtx) => {
          childCtx.log.tag
            .operation('SELECT')
            .query('SELECT COUNT(*) FROM users WHERE email = ?')
            .duration(12.5)
            .httpStatus(200);

          // Simulate validation
          const existingUser = false;

          if (existingUser) {
            return childCtx.err('USER_EXISTS', { email: userData.email });
          }
          return childCtx.ok({ valid: true });
        });

        if (!validation.success) {
          return ctx.err('VALIDATION_FAILED', validation.error);
        }

        // Complete operation with chained tags
        ctx.tag.duration(50.3).httpStatus(201);

        return ctx.ok({ id: 'user-123', ...userData });
      });

      // Create request context
      const requestCtx = createRequestContext(
        { requestId: 'req-123', userId: 'user-456' },
        featureFlags,
        flagEvaluator,
        environmentConfig,
      );

      // Execute task
      const result = await createUser(requestCtx, {
        email: 'test@example.com',
        name: 'Test User',
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.value.id).toBe('user-123');
        expect(result.value.email).toBe('test@example.com');
        expect(result.value.name).toBe('Test User');
      }
    });
  });
});
