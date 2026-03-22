import { describe, expect, test } from 'bun:test';
// Must import test-helpers first to initialize timestamp implementation
import '../../__tests__/test-helpers.js';
import { createTestTracerOptions } from '../../__tests__/test-helpers.js';
import { defineOpContext } from '../../defineOpContext.js';
import { TestTracer } from '../../tracers/TestTracer.js';
import type { AnySpanBuffer } from '../../types.js';
import { S } from '../builder.js';
import { defineFeatureFlags } from '../defineFeatureFlags.js';
import { defineLogSchema } from '../defineLogSchema.js';
import { InMemoryFlagEvaluator } from '../evaluator.js';
import { ENTRY_TYPE_FF_ACCESS, ENTRY_TYPE_FF_USAGE } from '../systemSchema.js';

/**
 * Helper to count entries of a specific type in the buffer
 */
function countEntryType(buffer: AnySpanBuffer, entryType: number): number {
  let count = 0;
  let current: AnySpanBuffer | undefined = buffer;
  while (current) {
    const writeIndex = current._writeIndex;
    for (let i = 0; i < writeIndex; i++) {
      if (current.entry_type[i] === entryType) {
        count++;
      }
    }
    current = current._overflow;
  }
  return count;
}

describe('Feature Flags', () => {
  // Define a minimal log schema for tests
  const testLogSchema = defineLogSchema({});

  test('defines feature flags with sync/async markers', () => {
    const flags = defineFeatureFlags({
      debugMode: S.boolean().default(false).sync(),
      maxRetries: S.number().default(3).sync(),
      userSpecificLimit: S.number().default(100).async(),
    });

    expect(flags.schema).toBeDefined();
    expect(flags.syncFlags).toContain('debugMode');
    expect(flags.syncFlags).toContain('maxRetries');
    expect(flags.asyncFlags).toContain('userSpecificLimit');
  });

  test('evaluator returns FlagContext for truthy flags', async () => {
    const flags = defineFeatureFlags({
      debugMode: S.boolean().default(false).sync(),
      maxRetries: S.number().default(3).sync(),
    });

    const ctx = defineOpContext({
      logSchema: testLogSchema,
      flags: flags.schema,
    });

    const flagEvaluator = new InMemoryFlagEvaluator(flags.schema, {
      debugMode: true,
      maxRetries: 5,
    });

    const { trace } = new TestTracer(ctx, { ...createTestTracerOptions(), flagEvaluator });
    const result = await trace('test-span', async (ctx) => {
      const ff = ctx.ff;

      expect(ff.debugMode).toBeDefined();
      expect(ff.debugMode?.value).toBe(true);
      expect(typeof ff.debugMode?.track).toBe('function');

      expect(ff.maxRetries).toBeDefined();
      expect(ff.maxRetries?.value).toBe(5);
      expect(typeof ff.maxRetries?.track).toBe('function');

      return ctx.ok(null);
    });

    expect(result.success).toBe(true);
  });

  test('feature-flag runtime stays worker-safe', async () => {
    const flags = defineFeatureFlags({
      debugMode: S.boolean().default(false).sync(),
    });

    const ctx = defineOpContext({
      logSchema: testLogSchema,
      flags: flags.schema,
    });

    const flagEvaluator = new InMemoryFlagEvaluator(flags.schema, { debugMode: true });
    const tracer = new TestTracer(ctx, { ...createTestTracerOptions(), flagEvaluator });

    await tracer.trace('worker-safe-evaluator', async (spanCtx) => {
      expect(spanCtx.ff.debugMode?.value).toBe(true);
      expect(spanCtx.ff.constructor.toString()).not.toContain('new Function');
      return spanCtx.ok(null);
    });
  });

  test('evaluator returns undefined for falsy flags', async () => {
    const flags = defineFeatureFlags({
      debugMode: S.boolean().default(false).sync(),
      userLimit: S.number().default(100).async(),
    });

    const ctx = defineOpContext({
      logSchema: testLogSchema,
      flags: flags.schema,
    });

    const flagEvaluator = new InMemoryFlagEvaluator(flags.schema, {}); // No flags set → null values

    const { trace } = new TestTracer(ctx, { ...createTestTracerOptions(), flagEvaluator });
    await trace('test-span', async (ctx) => {
      const ff = ctx.ff;
      expect(ff.debugMode).toBeUndefined();
      return ctx.ok(null);
    });
  });

  test('async flags return FlagContext via get method', async () => {
    const flags = defineFeatureFlags({
      userSpecificLimit: S.number().default(100).async(),
      dynamicProvider: S.enum(['stripe', 'paypal']).default('stripe').async(),
    });

    const ctx = defineOpContext({
      logSchema: testLogSchema,
      flags: flags.schema,
    });

    const flagEvaluator = new InMemoryFlagEvaluator(flags.schema, {
      userSpecificLimit: 200,
      dynamicProvider: 'paypal',
    });

    const { trace } = new TestTracer(ctx, { ...createTestTracerOptions(), flagEvaluator });
    await trace('test-span', async (ctx) => {
      // Async flags accessed via get() return FlagContext
      const limit = await ctx.ff.get('userSpecificLimit');
      expect(limit).toBeDefined();
      expect(limit?.value).toBe(200);

      const provider = await ctx.ff.get('dynamicProvider');
      expect(provider).toBeDefined();
      expect(provider?.value).toBe('paypal');

      return ctx.ok(null);
    });
  });

  test('track() on FlagContext logs usage events', async () => {
    const flags = defineFeatureFlags({
      advancedValidation: S.boolean().default(false).sync(),
    });

    const ctx = defineOpContext({
      logSchema: testLogSchema,
      flags: flags.schema,
    });

    const flagEvaluator = new InMemoryFlagEvaluator(flags.schema, {
      advancedValidation: true,
    });

    const { trace } = new TestTracer(ctx, { ...createTestTracerOptions(), flagEvaluator });
    await trace('test-span', async (ctx) => {
      const flag = ctx.ff.advancedValidation;

      // track() returns fluent log entry so additional fields can be tagged
      flag?.track({ action: 'validation_performed', outcome: 'success' }).ff_value('used').line(123);

      // Check buffer for ff-usage entry
      const usageCount = countEntryType(ctx.buffer, ENTRY_TYPE_FF_USAGE);
      expect(usageCount).toBe(1);

      return ctx.ok(null);
    });
  });

  test('sync flag access is logged only once per span', async () => {
    const flags = defineFeatureFlags({
      debugMode: S.boolean().default(false).sync(),
    });

    const ctx = defineOpContext({
      logSchema: testLogSchema,
      flags: flags.schema,
    });

    const flagEvaluator = new InMemoryFlagEvaluator(flags.schema, { debugMode: true });

    const { trace } = new TestTracer(ctx, { ...createTestTracerOptions(), flagEvaluator });
    await trace('test-span', async (ctx) => {
      // Access the flag multiple times
      const value1 = ctx.ff.debugMode;
      const value2 = ctx.ff.debugMode;
      const value3 = ctx.ff.debugMode;

      // All accesses should return same value (flag is truthy)
      expect(value1?.value).toBe(true);
      expect(value2?.value).toBe(true);
      expect(value3?.value).toBe(true);

      // Only ONE ff-access log should be written (deduplication via buffer scan)
      const accessCount = countEntryType(ctx.buffer, ENTRY_TYPE_FF_ACCESS);
      expect(accessCount).toBe(1);

      return ctx.ok(null);
    });
  });

  test('async flag access is logged', async () => {
    const flags = defineFeatureFlags({
      userLimit: S.number().default(100).async(),
    });

    const ctx = defineOpContext({
      logSchema: testLogSchema,
      flags: flags.schema,
    });

    const flagEvaluator = new InMemoryFlagEvaluator(flags.schema, { userLimit: 200 });

    const { trace } = new TestTracer(ctx, { ...createTestTracerOptions(), flagEvaluator });
    await trace('test-span', async (ctx) => {
      const value = await ctx.ff.get('userLimit');
      expect(value?.value).toBe(200);

      // Check buffer for ff-access entry
      const accessCount = countEntryType(ctx.buffer, ENTRY_TYPE_FF_ACCESS);
      expect(accessCount).toBe(1);

      return ctx.ok(null);
    });
  });

  test('S.enum validates enum values', () => {
    const schema = defineFeatureFlags({
      environment: S.enum(['dev', 'staging', 'prod'] as const)
        .default('dev')
        .sync(),
    });

    expect(schema.schema.environment).toBeDefined();
    expect(schema.schema.environment.defaultValue).toBe('dev');
    expect(schema.schema.environment.evaluationType).toBe('sync');
  });

  test('S.enum throws on empty values', () => {
    expect(() => {
      S.enum([]);
    }).toThrow('Enum must have at least one value');
  });

  test('multiple sync and async flags work together', async () => {
    const flags = defineFeatureFlags({
      // Sync flags
      enableFeatureX: S.boolean().default(false).sync(),
      maxConnectionPool: S.number().default(10).sync(),
      logLevel: S.enum(['debug', 'info', 'warn', 'error'] as const)
        .default('info')
        .sync(),

      // Async flags
      userTier: S.category().default('free').async(),
      customLimit: S.number().default(100).async(),
    });

    const ctx = defineOpContext({
      logSchema: testLogSchema,
      flags: flags.schema,
    });

    const flagEvaluator = new InMemoryFlagEvaluator(flags.schema, {
      enableFeatureX: true,
      maxConnectionPool: 20,
      logLevel: 'debug',
      userTier: 'premium',
      customLimit: 500,
    });

    const { trace } = new TestTracer(ctx, { ...createTestTracerOptions(), flagEvaluator });
    await trace('test-span', async (ctx) => {
      // Test sync flags
      expect(ctx.ff.enableFeatureX?.value).toBe(true);
      expect(ctx.ff.maxConnectionPool?.value).toBe(20);
      expect(ctx.ff.logLevel?.value).toBe('debug');

      // Test async flags
      const userTier = await ctx.ff.get('userTier');
      expect(userTier?.value).toBe('premium');

      const customLimit = await ctx.ff.get('customLimit');
      expect(customLimit?.value).toBe(500);

      return ctx.ok(null);
    });
  });

  test('InMemoryFlagEvaluator setFlag updates values', async () => {
    const schema = defineFeatureFlags({
      testFlag: S.category().default('initial').sync(),
    });
    const evaluator = new InMemoryFlagEvaluator(schema.schema, {
      testFlag: 'initial',
    });

    // getSync receives ctx as first param (can be empty object for simple evaluator)
    const { trace } = new TestTracer(
      defineOpContext({
        logSchema: testLogSchema,
        flags: schema.schema,
      }),
      { ...createTestTracerOptions(), flagEvaluator: evaluator },
    );
    await trace('sync-read-initial', async (ctx) => {
      expect(evaluator.getSync(ctx, 'testFlag')).toBe('initial');
      return ctx.ok(null);
    });

    evaluator.setFlag('testFlag', 'updated');

    await trace('sync-read-updated', async (ctx) => {
      expect(evaluator.getSync(ctx, 'testFlag')).toBe('updated');
      return ctx.ok(null);
    });
  });

  test('InMemoryFlagEvaluator works with async', async () => {
    const schema = defineFeatureFlags({
      asyncFlag: S.number().default(0).async(),
    });
    const evaluator = new InMemoryFlagEvaluator(schema.schema, {
      asyncFlag: 42,
    });

    const { trace } = new TestTracer(
      defineOpContext({
        logSchema: testLogSchema,
        flags: schema.schema,
      }),
      { ...createTestTracerOptions(), flagEvaluator: evaluator },
    );
    await trace('async-read', async (ctx) => {
      expect(await evaluator.getAsync(ctx, 'asyncFlag')).toBe(42);
      return ctx.ok(null);
    });
  });

  test('forContext creates child evaluator bound to child SpanContext', async () => {
    const flags = defineFeatureFlags({
      debugMode: S.boolean().default(false).sync(),
    });

    const ctx = defineOpContext({
      logSchema: testLogSchema,
      flags: flags.schema,
    });

    const flagEvaluator = new InMemoryFlagEvaluator(flags.schema, { debugMode: true });

    const { trace } = new TestTracer(ctx, { ...createTestTracerOptions(), flagEvaluator });
    await trace('parent-span', async (parentCtx) => {
      // Access flag in parent
      expect(parentCtx.ff.debugMode?.value).toBe(true);

      const parentAccessCount = countEntryType(parentCtx.buffer, ENTRY_TYPE_FF_ACCESS);
      expect(parentAccessCount).toBe(1);

      // Create child span - child gets its own ff evaluator via forContext
      await parentCtx.span('child-span', async (childCtx) => {
        expect(childCtx.ff.debugMode?.value).toBe(true);

        // Access in child should log to child buffer
        const childAccessCount = countEntryType(childCtx.buffer, ENTRY_TYPE_FF_ACCESS);
        expect(childAccessCount).toBe(1);

        return childCtx.ok(null);
      });

      return parentCtx.ok(null);
    });
  });

  test('usage example from spec: undefined/truthy pattern', async () => {
    const flags = defineFeatureFlags({
      darkMode: S.boolean().default(false).sync(),
      advancedSearch: S.boolean().default(false).sync(),
    });

    // Scenario 1: darkMode is enabled
    const enabledCtx = defineOpContext({
      logSchema: testLogSchema,
      flags: flags.schema,
    });

    const enabledFlagEvaluator = new InMemoryFlagEvaluator(flags.schema, { darkMode: true });
    const { trace: enabledTrace } = new TestTracer(enabledCtx, {
      ...createTestTracerOptions(),
      flagEvaluator: enabledFlagEvaluator,
    });

    await enabledTrace('test-enabled', async (ctx) => {
      const darkMode = ctx.ff.darkMode;

      // Truthy check works naturally with undefined/truthy semantics
      if (darkMode) {
        expect(darkMode.value).toBe(true);
        darkMode.track({ action: 'applied' });
      }

      return ctx.ok(null);
    });

    // Scenario 2: darkMode is disabled
    const disabledCtx = defineOpContext({
      logSchema: testLogSchema,
      flags: flags.schema,
    });

    const disabledFlagEvaluator = new InMemoryFlagEvaluator(flags.schema, { darkMode: false });
    const { trace: disabledTrace } = new TestTracer(disabledCtx, {
      ...createTestTracerOptions(),
      flagEvaluator: disabledFlagEvaluator,
    });

    await disabledTrace('test-disabled', async (ctx) => {
      const darkModeDisabled = ctx.ff.darkMode;

      // Falsy check works - undefined is falsy
      expect(darkModeDisabled).toBeUndefined();
      if (!darkModeDisabled) {
        expect(true).toBe(true);
      }

      return ctx.ok(null);
    });
  });
});
