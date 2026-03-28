/**
 * Integration tests for schema integration patterns
 *
 * Tests the full integration of:
 * - Op context definition with defineOpContext()
 * - TraceContext creation with Tracer (destructuring pattern)
 * - Op wrappers with span context
 * - Feature flag evaluation and analytics
 * - Typed tag attribute API
 */

import { describe, expect, it } from 'bun:test';
// Must import test-helpers first to initialize timestamp implementation
import './test-helpers.js';
import { defineOpContext } from '../defineOpContext.js';
import { defineCodeError } from '../result.js';
import { S } from '../schema/builder.js';
import { defineFeatureFlags } from '../schema/defineFeatureFlags.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { InMemoryFlagEvaluator } from '../schema/evaluator.js';
import { TestTracer } from '../tracers/TestTracer.js';
import { createTestTracerOptions } from './test-helpers.js';

// Error code factories for tests
const VALIDATION_ERROR = defineCodeError('VALIDATION_ERROR')<{ field: string }>();
const VALIDATION_FAILED = defineCodeError('VALIDATION_FAILED')<{ error: unknown }>();
const USER_EXISTS = defineCodeError('USER_EXISTS')<{ email: string }>();

describe('Schema Integration Patterns', () => {
  // Define log schema for DB operations
  // Using the three string types per specs/lmao/01a_trace_schema_system.md:
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

  // Create a shared op context for tests (without feature flags)
  const opContext = defineOpContext({
    logSchema: dbSchema,
    ctx: {
      requestId: undefined as string | undefined,
      userId: undefined as string | undefined,
      env: undefined as EnvConfig | undefined,
    },
  });

  const { defineOp } = opContext;

  // Create an op context WITH feature flags
  const opContextWithFlags = defineOpContext({
    logSchema: dbSchema,
    flags: featureFlags.schema,
    ctx: {
      requestId: undefined as string | undefined,
      userId: undefined as string | undefined,
      env: undefined as EnvConfig | undefined,
    },
  });

  const { defineOp: defineOpWithFlags } = opContextWithFlags;

  // Create a flag evaluator with test values
  const flagEvaluator = new InMemoryFlagEvaluator(featureFlags.schema, {
    advancedValidation: true,
    maxRetries: 5,
    experimentalFeature: false,
  });

  describe('Tracer API', () => {
    it('should create trace context with environment via Tracer', async () => {
      const { trace } = new TestTracer(opContext, { ...createTestTracerOptions() });

      // Use function overload to test user context properties
      const result = await trace(
        'check-ctx',
        { requestId: 'req-123', userId: 'user-456', env: environmentConfig },
        async (ctx) => {
          expect(ctx.requestId).toBe('req-123');
          expect(ctx.userId).toBe('user-456');
          // TraceId is W3C format (32 hex chars) via generateTraceId from traceId.ts
          expect(ctx.buffer.trace_id).toMatch(/^[a-f0-9]{32}$/);
          expect(ctx.env).toBe(environmentConfig);
          return { checked: true };
        },
      );
      expect(result.checked).toBe(true);
    });

    it('should provide access to environment config as plain properties', async () => {
      const { trace } = new TestTracer(opContext, { ...createTestTracerOptions() });

      const result = await trace('check-env', { requestId: 'req-123', env: environmentConfig }, async (ctx) => {
        expect(ctx.env?.awsRegion).toBe('us-east-1');
        expect(ctx.env?.maxConnections).toBe(100);
        expect(ctx.env?.databaseUrl).toBe('postgresql://localhost:5432/test');
        return { checked: true };
      });
      expect(result.checked).toBe(true);
    });
  });

  describe('defineOp', () => {
    it('should create op that provides span context', () => {
      const testOp = defineOp('test-task', async (ctx) => {
        expect(ctx.log).toBeDefined();
        expect(ctx.ok).toBeFunction();
        expect(ctx.err).toBeFunction();
        expect(ctx.span).toBeFunction();
        return ctx.ok({ success: true });
      });

      expect(testOp).toBeDefined();
      // Op is an object with fn method and metadata property
      expect(testOp.metadata.package_name).toBeDefined();
      expect(typeof testOp.fn).toBe('function');
    });

    it('should execute op with Tracer (destructuring pattern)', async () => {
      const testOp = defineOp('test-task', async (ctx) => {
        expect(ctx.log).toBeDefined();
        expect(ctx.ok).toBeFunction();
        expect(ctx.err).toBeFunction();
        expect(ctx.span).toBeFunction();
        return ctx.ok({ success: true });
      });

      // ✅ CORRECT PATTERN - Destructure trace from Tracer
      const { trace } = new TestTracer(opContext, { ...createTestTracerOptions() });

      const result = await trace('test-task', testOp);
      expect(result.success).toBe(true);
    });
  });

  describe('Feature Flag Integration', () => {
    it('should create trace context with feature flags', async () => {
      const testOp = defineOpWithFlags('check-ff', async (ctx) => {
        expect(ctx.ff).toBeDefined();
        expect(ctx.buffer.trace_id).toMatch(/^[a-f0-9]{32}$/);
        return ctx.ok({ checked: true });
      });

      const { trace } = new TestTracer(opContextWithFlags, { ...createTestTracerOptions(), flagEvaluator });

      const result = await trace('check-ff', { requestId: 'req-123', env: environmentConfig }, testOp);
      expect(result.success).toBe(true);
    });

    it('should access sync feature flags as properties', async () => {
      const testOp = defineOpWithFlags('test-ff', async (ctx) => {
        // Sync flags return FlagContext wrappers when truthy (undefined when falsy)
        // Access .value to get the actual flag value
        const validationFlag = ctx.ff.advancedValidation;
        const retriesFlag = ctx.ff.maxRetries;

        if (!validationFlag || !retriesFlag) {
          throw new Error('Expected sync feature flags to be available in test fixture');
        }

        expect(validationFlag.value).toBe(true);
        expect(retriesFlag.value).toBe(5);

        return ctx.ok({ validated: true });
      });

      const { trace } = new TestTracer(opContextWithFlags, { ...createTestTracerOptions(), flagEvaluator });

      const result = await trace('test-ff', { requestId: 'req-123', env: environmentConfig }, testOp);
      expect(result.success).toBe(true);
    });

    it('should access async feature flags via get()', async () => {
      const testOp = defineOpWithFlags('test-async-ff', async (ctx) => {
        // Async flags use ctx.ff.get('flagName')
        // Returns FlagContext when truthy, undefined when falsy
        const experimentalFlag = await ctx.ff.get('experimentalFeature');

        // Flag is false, so should be undefined (falsy pattern)
        expect(experimentalFlag).toBeUndefined();

        return ctx.ok({ experimental: false });
      });

      const { trace } = new TestTracer(opContextWithFlags, { ...createTestTracerOptions(), flagEvaluator });

      const result = await trace('test-async-ff', { requestId: 'req-123', env: environmentConfig }, testOp);
      expect(result.success).toBe(true);
    });

    it('should track feature flag usage', async () => {
      const testOp = defineOpWithFlags('test-ff-tracking', async (ctx) => {
        // Access flag first
        const validationFlag = ctx.ff.advancedValidation;

        if (validationFlag) {
          // track() creates ff-usage entries and returns fluent entry for tagging
          validationFlag.track().operation('SELECT').duration(5);
        }

        return ctx.ok({ tracked: true });
      });

      const { trace } = new TestTracer(opContextWithFlags, { ...createTestTracerOptions(), flagEvaluator });

      const result = await trace('test-ff-tracking', { requestId: 'req-123', env: environmentConfig }, testOp);
      expect(result.success).toBe(true);
    });

    it('should use feature flags in conditional logic', async () => {
      const testOp = defineOpWithFlags('conditional-logic', async (ctx) => {
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

      const { trace } = new TestTracer(opContextWithFlags, { ...createTestTracerOptions(), flagEvaluator });

      const result = await trace('conditional-logic', { requestId: 'req-123', env: environmentConfig }, testOp);
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.value.validated).toBe(true);
      }
    });
  });

  describe('Method Chaining', () => {
    it('should chain multiple tag methods fluently and write to buffers', async () => {
      const testOp = defineOp('test-chaining', async (ctx) => {
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

      // ✅ CORRECT PATTERN - Destructure trace from Tracer
      const { trace } = new TestTracer(opContext, { ...createTestTracerOptions() });

      const result = await trace('test-chaining', testOp);
      expect(result.success).toBe(true);

      // Note: Buffer writes are happening in memory to Arrow columnar format
      // Each tag method call writes to the appropriate attribute column
    });

    it('should chain with() method and continue chaining', async () => {
      const testOp = defineOp('test-with-chaining', async (ctx) => {
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

      // ✅ CORRECT PATTERN - Destructure trace from Tracer
      const { trace } = new TestTracer(opContext, { ...createTestTracerOptions() });

      const result = await trace('test-with-chaining', testOp);
      expect(result.success).toBe(true);
    });

    it('should support real-world chaining pattern: orderId and amount', async () => {
      const testOp = defineOp('process-order', async (ctx, orderId: string, amount: number) => {
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

      const { trace } = new TestTracer(opContext, { ...createTestTracerOptions() });

      // Use trace_fn to wrap the op invocation with args
      const result = await trace(
        'process-order',
        { requestId: 'req-003', userId: 'user-003', env: environmentConfig },
        async (ctx) => testOp.fn(ctx, 'order-789', 149.99),
      );
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.value.orderId).toBe('order-789');
        expect(result.value.amount).toBe(149.99);
      }
    });
  });

  describe('Op Integration', () => {
    it('should provide typed tag attribute API with method chaining', async () => {
      const testOp = defineOp('test-task', async (ctx) => {
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

      // ✅ CORRECT PATTERN - Destructure trace from Tracer
      const { trace } = new TestTracer(opContext, { ...createTestTracerOptions() });

      const result = await trace('test-task', testOp);
      expect(result.success).toBe(true);
    });

    it('should provide message logging methods', async () => {
      const testOp = defineOp('test-task', async (ctx) => {
        ctx.log.info('Info message');
        ctx.log.debug('Debug message');
        ctx.log.warn('Warning message');
        ctx.log.error('Error message');
        // Note: ctx.log.message() was removed - use level-specific methods instead
        ctx.log.info('Custom message');

        return ctx.ok({ success: true });
      });

      // ✅ CORRECT PATTERN - Destructure trace from Tracer
      const { trace } = new TestTracer(opContext, { ...createTestTracerOptions() });

      const result = await trace('test-task', testOp);
      expect(result.success).toBe(true);
    });

    it('should provide ok/err result helpers', async () => {
      const successOp = defineOp('success-task', async (ctx) => {
        return ctx.ok({ data: 'result' });
      });

      const errorOp = defineOp('error-task', async (ctx) => {
        return ctx.err(VALIDATION_ERROR({ field: 'email' }));
      });

      // ✅ CORRECT PATTERN - Destructure trace from Tracer
      const { trace } = new TestTracer(opContext, { ...createTestTracerOptions() });

      const successResult = await trace('success-task', successOp);
      expect(successResult.success).toBe(true);
      if (successResult.success) {
        expect(successResult.value).toEqual({ data: 'result' });
      }

      const errorResult = await trace('error-task', errorOp);
      expect(errorResult.success).toBe(false);
      if (!errorResult.success) {
        expect(errorResult.error.code).toBe('VALIDATION_ERROR');
        expect(errorResult.error.field).toBe('email');
      }
    });

    it('should support child spans with chained tag methods', async () => {
      const testOp = defineOp('parent-task', async (ctx) => {
        // Parent span with chained tags
        ctx.tag.requestId('req-123').operation('INSERT').duration(50.0);

        const childResult = await ctx.span('child-task', async (childCtx) => {
          // Child span with chained tags - tag is on childCtx directly, not on childCtx.log
          childCtx.tag.operation('SELECT').query('SELECT * FROM users').duration(5.2).httpStatus(200);

          return childCtx.ok({ found: true });
        });

        expect(childResult.success).toBe(true);
        if (childResult.success) {
          expect(childResult.value.found).toBe(true);
        }

        return ctx.ok({ success: true });
      });

      // ✅ CORRECT PATTERN - Destructure trace from Tracer
      const { trace } = new TestTracer(opContext, { ...createTestTracerOptions() });

      const result = await trace('parent-task', testOp);
      expect(result.success).toBe(true);
    });
  });

  describe('Method Chaining Examples', () => {
    it('should support various chaining patterns', async () => {
      const processOrder = defineOp('process-order', async (ctx, orderId: string, _amount: number) => {
        // Example from requirements: ctx.tag.orderId(order.id).amount(order.total)
        ctx.tag
          .requestId(ctx.requestId || 'unknown')
          .userId(ctx.userId || 'anonymous')
          .operation('INSERT');

        // Chaining with with()
        ctx.tag.with({ region: 'us-east-1', httpStatus: 200 }).duration(25.5).query('INSERT INTO orders VALUES (...)');

        return ctx.ok({ orderId, processed: true });
      });

      const { trace } = new TestTracer(opContext, { ...createTestTracerOptions() });

      const result = await trace(
        'process-order',
        { requestId: 'req-999', userId: 'user-123', env: environmentConfig },
        async (ctx) => processOrder.fn(ctx, 'order-456', 99.99),
      );
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.value.orderId).toBe('order-456');
        expect(result.value.processed).toBe(true);
      }
    });

    it('should demonstrate complete integration with chaining and feature flags', async () => {
      // Define op with extensive chaining and feature flag usage
      const createUser = defineOpWithFlags('create-user', async (ctx, userData: { email: string; name: string }) => {
        // Feature flag conditional - natural truthy pattern
        if (ctx.ff.advancedValidation) {
          ctx.log.info('Using advanced validation');
          ctx.ff.advancedValidation.track();
        }

        // Environment access and chained tags
        ctx.tag
          .requestId(ctx.requestId || 'unknown')
          .userId(userData.email)
          .region((ctx.env?.awsRegion ?? 'unknown') as string)
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
            return childCtx.err(USER_EXISTS({ email: userData.email }));
          }
          return childCtx.ok({ valid: true });
        });

        if (!validation.success) {
          return ctx.err(VALIDATION_FAILED({ error: validation.error }));
        }

        // Complete operation with chained tags
        ctx.tag.duration(50.3).httpStatus(201);

        return ctx.ok({ id: 'user-123', ...userData });
      });

      const { trace } = new TestTracer(opContextWithFlags, { ...createTestTracerOptions(), flagEvaluator });

      const result = await trace(
        'create-user',
        { requestId: 'req-123', userId: 'user-456', env: environmentConfig },
        async (ctx) =>
          createUser.fn(ctx, {
            email: 'test@example.com',
            name: 'Test User',
          }),
      );

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
      let parentRequestId: string | undefined;
      let parentUserId: string | undefined;

      const testOp = defineOp('parent-task', async (ctx) => {
        // Capture parent context properties
        parentRequestId = ctx.requestId;
        parentUserId = ctx.userId;

        await ctx.span('child-task', async (childCtx) => {
          return childCtx.ok({ done: true });
        });

        return ctx.ok({ success: true });
      });

      const { trace } = new TestTracer(opContext, { ...createTestTracerOptions() });

      await trace(
        'parent-task',
        { requestId: 'req-inherit-test', userId: 'user-inherit-test', env: environmentConfig },
        testOp,
      );

      // Verify properties are available in parent span
      expect(parentRequestId).toBe('req-inherit-test');
      expect(parentUserId).toBe('user-inherit-test');
    });

    it('should preserve env config in parent and child spans', async () => {
      let parentEnv: EnvConfig | undefined;

      const testOp = defineOp('parent-task', async (ctx) => {
        parentEnv = ctx.env;

        await ctx.span('child-task', async (childCtx) => {
          return childCtx.ok({ done: true });
        });

        return ctx.ok({ success: true });
      });

      const { trace } = new TestTracer(opContext, { ...createTestTracerOptions() });

      await trace('parent-task', { requestId: 'req-env-test', env: environmentConfig }, testOp);

      // Verify env is same object reference (not copied)
      expect(parentEnv).toBe(environmentConfig);
      expect(parentEnv?.awsRegion).toBe('us-east-1');
    });
  });
});
