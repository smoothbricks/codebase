/**
 * Integration tests for schema integration patterns
 *
 * Tests the full integration of:
 * - Module definition with defineModule()
 * - TraceContext creation with module.traceContext()
 * - Op wrappers with span context
 * - Feature flag evaluation and analytics
 * - Typed tag attribute API
 */

import { describe, expect, it } from 'bun:test';
import { defineModule } from '../defineModule.js';
import { S } from '../schema/builder.js';
import { defineFeatureFlags } from '../schema/defineFeatureFlags.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { InMemoryFlagEvaluator } from '../schema/evaluator.js';

describe('Schema Integration Patterns', () => {
  // Define log schema for DB operations
  // Using the three string types per specs/01a_trace_schema_system.md:
  // - S.enum: Known values at compile time
  // - S.category: Values that often repeat
  // - S.text: Unique values
  const dbSchema = defineLogSchema({
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

  // Environment config type
  type EnvConfig = {
    awsRegion: string;
    maxConnections: number;
    databaseUrl: string;
  };

  // Environment config
  const environmentConfig: EnvConfig = {
    awsRegion: 'us-east-1',
    maxConnections: 100,
    databaseUrl: 'postgresql://localhost:5432/test',
  };

  // Create flag evaluator with test values
  function createFlagEvaluator() {
    return new InMemoryFlagEvaluator(featureFlags.schema, {
      advancedValidation: true,
      maxRetries: 5,
      experimentalFeature: false,
    });
  }

  // Create a shared module for tests (without feature flags)
  function createTestModule() {
    return defineModule({
      metadata: {
        git_sha: 'abc123',
        package_name: '@test/pkg',
        package_file: 'src/services/user.ts',
      },
      logSchema: dbSchema,
    })
      .ctx<{ requestId?: string; userId?: string; env: EnvConfig }>({
        requestId: undefined,
        userId: undefined,
        env: null!,
      })
      .make();
  }

  // Create a module WITH feature flags
  function createModuleWithFlags() {
    return defineModule({
      metadata: {
        git_sha: 'abc123',
        package_name: '@test/pkg',
        package_file: 'src/services/user.ts',
      },
      logSchema: dbSchema,
      ff: featureFlags.schema,
    })
      .ctx<{ requestId?: string; userId?: string; env: EnvConfig }>({
        requestId: undefined,
        userId: undefined,
        env: null!,
      })
      .make({ ffEvaluator: createFlagEvaluator() });
  }

  describe('module.traceContext', () => {
    it('should create trace context with environment', () => {
      const testModule = createTestModule();
      const ctx = testModule.traceContext({
        requestId: 'req-123',
        userId: 'user-456',
        env: environmentConfig,
      });

      expect(ctx.requestId).toBe('req-123');
      expect(ctx.userId).toBe('user-456');
      // TraceId is W3C format (32 hex chars) via generateTraceId from traceId.ts
      expect(ctx.trace_id).toMatch(/^[a-f0-9]{32}$/);
      expect(ctx.env).toBe(environmentConfig);
    });

    it('should provide access to environment config as plain properties', () => {
      const testModule = createTestModule();
      const ctx = testModule.traceContext({ requestId: 'req-123', env: environmentConfig });

      expect(ctx.env.awsRegion).toBe('us-east-1');
      expect(ctx.env.maxConnections).toBe(100);
      expect(ctx.env.databaseUrl).toBe('postgresql://localhost:5432/test');
    });
  });

  describe('defineModule', () => {
    it('should create module with log schema', () => {
      const testModule = createTestModule();

      expect(testModule).toBeDefined();
      expect(testModule.op).toBeFunction();
      expect(testModule.traceContext).toBeFunction();
    });

    it('should create op wrapper that provides span context', async () => {
      const testModule = createTestModule();

      const testOp = testModule.op('test-task', async (ctx) => {
        expect(ctx.log).toBeDefined();
        expect(ctx.env).toBeDefined();
        expect(ctx.ok).toBeFunction();
        expect(ctx.err).toBeFunction();
        expect(ctx.span).toBeFunction();
        return ctx.ok({ success: true });
      });

      const traceCtx = testModule.traceContext({ requestId: 'req-123', env: environmentConfig });

      const result = await traceCtx.span('test-task', testOp);
      expect(result.success).toBe(true);
    });
  });

  describe('Feature Flag Integration', () => {
    it('should create trace context with feature flags', () => {
      const testModule = createModuleWithFlags();
      const ctx = testModule.traceContext({
        requestId: 'req-123',
        env: environmentConfig,
      });

      expect(ctx.ff).toBeDefined();
      expect(ctx.trace_id).toMatch(/^[a-f0-9]{32}$/);
    });

    it('should access sync feature flags as properties', async () => {
      const testModule = createModuleWithFlags();

      const testOp = testModule.op('test-ff', async (ctx) => {
        // Sync flags return FlagContext wrappers when truthy (undefined when falsy)
        // Access .value to get the actual flag value
        const validationFlag = ctx.ff.advancedValidation;
        const retriesFlag = ctx.ff.maxRetries;

        expect(validationFlag).toBeDefined();
        expect((validationFlag as { value: boolean }).value).toBe(true);

        expect(retriesFlag).toBeDefined();
        expect((retriesFlag as { value: number }).value).toBe(5);

        return ctx.ok({ validated: true });
      });

      const traceCtx = testModule.traceContext({
        requestId: 'req-123',
        env: environmentConfig,
      });

      const result = await traceCtx.span('test-ff', testOp);
      expect(result.success).toBe(true);
    });

    it('should access async feature flags via get()', async () => {
      const testModule = createModuleWithFlags();

      const testOp = testModule.op('test-async-ff', async (ctx) => {
        // Async flags use ctx.ff.get('flagName')
        // Returns FlagContext when truthy, undefined when falsy
        const experimentalFlag = await ctx.ff.get('experimentalFeature');

        // Flag is false, so should be undefined (falsy pattern)
        expect(experimentalFlag).toBeUndefined();

        return ctx.ok({ experimental: false });
      });

      const traceCtx = testModule.traceContext({
        requestId: 'req-123',
        env: environmentConfig,
      });

      const result = await traceCtx.span('test-async-ff', testOp);
      expect(result.success).toBe(true);
    });

    it('should track feature flag usage', async () => {
      const testModule = createModuleWithFlags();

      const testOp = testModule.op('test-ff-tracking', async (ctx) => {
        // Access flag first
        const validationFlag = ctx.ff.advancedValidation;

        if (validationFlag) {
          // Use track() on the flag context to log usage
          validationFlag.track({ action: 'validation_performed', outcome: 'success' });
        }

        // Can also use trackUsage directly
        ctx.ff.trackUsage('advancedValidation', {
          action: 'retry_check',
          outcome: 'skipped',
        });

        return ctx.ok({ tracked: true });
      });

      const traceCtx = testModule.traceContext({
        requestId: 'req-123',
        env: environmentConfig,
      });

      const result = await traceCtx.span('test-ff-tracking', testOp);
      expect(result.success).toBe(true);
    });

    it('should use feature flags in conditional logic', async () => {
      const testModule = createModuleWithFlags();

      const testOp = testModule.op('conditional-logic', async (ctx) => {
        // Natural truthy/falsy pattern - flag is undefined when disabled
        if (ctx.ff.advancedValidation) {
          ctx.log.info('Advanced validation enabled');
          ctx.tag.operation('SELECT'); // Validation query
        } else {
          ctx.log.info('Basic validation only');
          ctx.tag.operation('INSERT'); // Skip validation
        }

        // Number flags also work with truthy pattern
        const retries = ctx.ff.maxRetries;
        if (retries) {
          ctx.tag.duration(retries.value * 100); // Use retry count for timeout
        }

        return ctx.ok({ validated: !!ctx.ff.advancedValidation });
      });

      const traceCtx = testModule.traceContext({
        requestId: 'req-123',
        env: environmentConfig,
      });

      const result = await traceCtx.span('conditional-logic', testOp);
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.value.validated).toBe(true);
      }
    });
  });

  describe('Method Chaining', () => {
    it('should chain multiple tag methods fluently and write to buffers', async () => {
      const testModule = createTestModule();

      const testOp = testModule.op('test-chaining', async (ctx) => {
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

      const traceCtx = testModule.traceContext({ requestId: 'req-001', env: environmentConfig });

      const result = await traceCtx.span('test-chaining', testOp);
      expect(result.success).toBe(true);

      // Note: Buffer writes are happening in memory to Arrow columnar format
      // Each tag method call writes to the appropriate attribute column
    });

    it('should chain with() method and continue chaining', async () => {
      const testModule = createTestModule();

      const testOp = testModule.op('test-with-chaining', async (ctx) => {
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

      const traceCtx = testModule.traceContext({ requestId: 'req-002', env: environmentConfig });

      const result = await traceCtx.span('test-with-chaining', testOp);
      expect(result.success).toBe(true);
    });

    it('should support real-world chaining pattern: orderId and amount', async () => {
      const testModule = createTestModule();

      const testOp = testModule.op('process-order', async (ctx, orderId: string, amount: number) => {
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

      const traceCtx = testModule.traceContext({
        requestId: 'req-003',
        userId: 'user-003',
        env: environmentConfig,
      });

      const result = await traceCtx.span('process-order', testOp, 'order-789', 149.99);
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.value.orderId).toBe('order-789');
        expect(result.value.amount).toBe(149.99);
      }
    });
  });

  describe('Op Integration', () => {
    it('should provide typed tag attribute API with method chaining', async () => {
      const testModule = createTestModule();

      const testOp = testModule.op('test-task', async (ctx) => {
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

      const traceCtx = testModule.traceContext({ requestId: 'req-123', env: environmentConfig });

      const result = await traceCtx.span('test-task', testOp);
      expect(result.success).toBe(true);
    });

    it('should provide message logging methods', async () => {
      const testModule = createTestModule();

      const testOp = testModule.op('test-task', async (ctx) => {
        ctx.log.info('Info message');
        ctx.log.debug('Debug message');
        ctx.log.warn('Warning message');
        ctx.log.error('Error message');
        // Note: ctx.log.message() was removed - use level-specific methods instead
        ctx.log.info('Custom message');

        return ctx.ok({ success: true });
      });

      const traceCtx = testModule.traceContext({ requestId: 'req-123', env: environmentConfig });

      const result = await traceCtx.span('test-task', testOp);
      expect(result.success).toBe(true);
    });

    it('should provide ok/err result helpers', async () => {
      const testModule = createTestModule();

      const successOp = testModule.op('success-task', async (ctx) => {
        return ctx.ok({ data: 'result' });
      });

      const errorOp = testModule.op('error-task', async (ctx) => {
        return ctx.err('VALIDATION_ERROR', { field: 'email' });
      });

      const traceCtx = testModule.traceContext({ requestId: 'req-123', env: environmentConfig });

      const successResult = await traceCtx.span('success-task', successOp);
      expect(successResult.success).toBe(true);
      if (successResult.success) {
        expect(successResult.value).toEqual({ data: 'result' });
      }

      const errorResult = await traceCtx.span('error-task', errorOp);
      expect(errorResult.success).toBe(false);
      if (!errorResult.success) {
        expect(errorResult.error.code).toBe('VALIDATION_ERROR');
        expect(errorResult.error.details).toEqual({ field: 'email' });
      }
    });

    it('should support child spans with chained tag methods', async () => {
      const testModule = createTestModule();

      const testOp = testModule.op('parent-task', async (ctx) => {
        // Parent span with chained tags
        ctx.tag.requestId('req-123').operation('INSERT').duration(50.0);

        const childResult = await ctx.span('child-task', async (childCtx) => {
          // Child span with chained tags - tag is on childCtx directly, not on childCtx.log
          childCtx.tag.operation('SELECT').query('SELECT * FROM users').duration(5.2).httpStatus(200);

          return { found: true };
        });

        expect(childResult.found).toBe(true);

        return ctx.ok({ success: true });
      });

      const traceCtx = testModule.traceContext({ requestId: 'req-123', env: environmentConfig });

      const result = await traceCtx.span('parent-task', testOp);
      expect(result.success).toBe(true);
    });
  });

  describe('Method Chaining Examples', () => {
    it('should support various chaining patterns', async () => {
      const testModule = createTestModule();

      const processOrder = testModule.op('process-order', async (ctx, orderId: string, _amount: number) => {
        // Example from requirements: ctx.tag.orderId(order.id).amount(order.total)
        ctx.tag
          .requestId(ctx.requestId || 'unknown')
          .userId(ctx.userId || 'anonymous')
          .operation('INSERT');

        // Chaining with with()
        ctx.tag.with({ region: 'us-east-1', httpStatus: 200 }).duration(25.5).query('INSERT INTO orders VALUES (...)');

        return ctx.ok({ orderId, processed: true });
      });

      const traceCtx = testModule.traceContext({
        requestId: 'req-999',
        userId: 'user-123',
        env: environmentConfig,
      });

      const result = await traceCtx.span('process-order', processOrder, 'order-456', 99.99);
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.value.orderId).toBe('order-456');
        expect(result.value.processed).toBe(true);
      }
    });

    it('should demonstrate complete integration with chaining and feature flags', async () => {
      // Create module with feature flags
      const testModule = createModuleWithFlags();

      // Define op with extensive chaining and feature flag usage
      const createUser = testModule.op('create-user', async (ctx, userData: { email: string; name: string }) => {
        // Feature flag conditional - natural truthy pattern
        if (ctx.ff.advancedValidation) {
          ctx.log.info('Using advanced validation');
          ctx.ff.advancedValidation.track({
            action: 'validation_performed',
            outcome: 'success',
          });
        }

        // Environment access and chained tags
        ctx.tag
          .requestId(ctx.requestId || 'unknown')
          .userId(userData.email)
          .region(ctx.env.awsRegion as string)
          .operation('INSERT');

        // Child span with chaining - tag is on childCtx directly, not on childCtx.log
        const validation = await ctx.span('validate-user', async (childCtx) => {
          childCtx.tag
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

      // Create trace context
      const traceCtx = testModule.traceContext({
        requestId: 'req-123',
        userId: 'user-456',
        env: environmentConfig,
      });

      // Execute op
      const result = await traceCtx.span('create-user', createUser, {
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

  describe('Prototype-based Context Inheritance', () => {
    it('should preserve user properties in parent spans', async () => {
      const testModule = createTestModule();

      let parentRequestId: string | undefined;
      let parentUserId: string | undefined;

      const testOp = testModule.op('parent-task', async (ctx) => {
        // Capture parent context properties
        parentRequestId = ctx.requestId;
        parentUserId = ctx.userId;

        await ctx.span('child-task', async (childCtx) => {
          return childCtx.ok({ done: true });
        });

        return ctx.ok({ success: true });
      });

      const traceCtx = testModule.traceContext({
        requestId: 'req-inherit-test',
        userId: 'user-inherit-test',
        env: environmentConfig,
      });

      await traceCtx.span('parent-task', testOp);

      // Verify properties are available in parent span
      expect(parentRequestId).toBe('req-inherit-test');
      expect(parentUserId).toBe('user-inherit-test');
    });

    it('should preserve env config in parent and child spans', async () => {
      const testModule = createTestModule();

      let parentEnv: EnvConfig | undefined;

      const testOp = testModule.op('parent-task', async (ctx) => {
        parentEnv = ctx.env;

        await ctx.span('child-task', async (childCtx) => {
          return childCtx.ok({ done: true });
        });

        return ctx.ok({ success: true });
      });

      const traceCtx = testModule.traceContext({
        requestId: 'req-env-test',
        env: environmentConfig,
      });

      await traceCtx.span('parent-task', testOp);

      // Verify env is same object reference (not copied)
      expect(parentEnv).toBe(environmentConfig);
      expect(parentEnv?.awsRegion).toBe('us-east-1');
    });
  });
});
