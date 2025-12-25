import { describe, expect, test } from 'bun:test';
// Must import test-helpers first to initialize timestamp implementation
import '../../__tests__/test-helpers.js';
import { defineOpContext } from '../../defineOpContext.js';
import { TestTracer } from '../../tracers/TestTracer.js';
import type { AnySpanBuffer } from '../../types.js';
import { S } from '../builder.js';
import { defineFeatureFlags } from '../defineFeatureFlags.js';
import { defineLogSchema } from '../defineLogSchema.js';
import { type BooleanFlagContext, InMemoryFlagEvaluator, type VariantFlagContext } from '../evaluator.js';
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
    }) as any;

    const { trace } = new TestTracer(ctx, { flagEvaluator });
    const result = await trace('test-span', async (ctx) => {
      // Cast ff to access typed properties (type inference limitation)
      const ff = ctx.ff as unknown as {
        debugMode: BooleanFlagContext | undefined;
        maxRetries: { value: number; track: () => void } | undefined;
      };

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

  test('evaluator returns undefined for falsy flags', async () => {
    const flags = defineFeatureFlags({
      debugMode: S.boolean().default(false).sync(),
      userLimit: S.number().default(100).async(),
    });

    const ctx = defineOpContext({
      logSchema: testLogSchema,
      flags: flags.schema,
    });

    const flagEvaluator = new InMemoryFlagEvaluator(flags.schema, {}) as any; // No flags set → null values

    const { trace } = new TestTracer(ctx, { flagEvaluator });
    await trace('test-span', async (ctx) => {
      const ff = ctx.ff as unknown as { debugMode: BooleanFlagContext | undefined };
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
    }) as any;

    const { trace } = new TestTracer(ctx, { flagEvaluator });
    await trace('test-span', async (ctx) => {
      // Async flags accessed via get() return FlagContext
      const limit = (await ctx.ff.get('userSpecificLimit')) as { value: number; track: () => void };
      expect(limit).toBeDefined();
      expect(limit.value).toBe(200);

      const provider = (await ctx.ff.get('dynamicProvider')) as VariantFlagContext<string>;
      expect(provider).toBeDefined();
      expect(provider.value).toBe('paypal');

      return ctx.ok(null);
    });
  });

  test('trackUsage logs usage events to buffer', async () => {
    const flags = defineFeatureFlags({
      advancedValidation: S.boolean().default(false).sync(),
    });

    const ctx = defineOpContext({
      logSchema: testLogSchema,
      flags: flags.schema,
    });

    const flagEvaluator = new InMemoryFlagEvaluator(flags.schema, {
      advancedValidation: true,
    }) as any;

    const { trace } = new TestTracer(ctx, { flagEvaluator });
    await trace('test-span', async (ctx) => {
      (ctx.ff as { trackUsage: (flag: string, context: object) => void }).trackUsage('advancedValidation', {
        action: 'validation_performed',
        outcome: 'success',
      });

      // Check buffer for ff-usage entry
      const usageCount = countEntryType(ctx.buffer, ENTRY_TYPE_FF_USAGE);
      expect(usageCount).toBe(1);

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
    }) as any;

    const { trace } = new TestTracer(ctx, { flagEvaluator });
    await trace('test-span', async (ctx) => {
      const ff = ctx.ff as unknown as { advancedValidation: BooleanFlagContext | undefined };
      const flag = ff.advancedValidation;

      // Use track() on the flag context
      flag?.track({ action: 'validation_performed', outcome: 'success' });

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

    const flagEvaluator = new InMemoryFlagEvaluator(flags.schema, { debugMode: true }) as any;

    const { trace } = new TestTracer(ctx, { flagEvaluator });
    await trace('test-span', async (ctx) => {
      const ff = ctx.ff as unknown as { debugMode: BooleanFlagContext | undefined };

      // Access the flag multiple times
      const value1 = ff.debugMode;
      const value2 = ff.debugMode;
      const value3 = ff.debugMode;

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

    const flagEvaluator = new InMemoryFlagEvaluator(flags.schema, { userLimit: 200 }) as any;

    const { trace } = new TestTracer(ctx, { flagEvaluator });
    await trace('test-span', async (ctx) => {
      const value = (await ctx.ff.get('userLimit')) as { value: number };
      expect(value.value).toBe(200);

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
    }) as any;

    const { trace } = new TestTracer(ctx, { flagEvaluator });
    await trace('test-span', async (ctx) => {
      const ff = ctx.ff as unknown as {
        enableFeatureX: BooleanFlagContext | undefined;
        maxConnectionPool: { value: number; track: () => void } | undefined;
        logLevel: VariantFlagContext<string> | undefined;
        get(flag: string): Promise<unknown>;
      };

      // Test sync flags
      expect(ff.enableFeatureX?.value).toBe(true);
      expect(ff.maxConnectionPool?.value).toBe(20);
      expect(ff.logLevel?.value).toBe('debug');

      // Test async flags
      const userTier = (await ff.get('userTier')) as VariantFlagContext<string>;
      expect(userTier.value).toBe('premium');

      const customLimit = (await ff.get('customLimit')) as { value: number };
      expect(customLimit.value).toBe(500);

      return ctx.ok(null);
    });
  });

  test('InMemoryFlagEvaluator setFlag updates values', () => {
    const schema = defineFeatureFlags({
      testFlag: S.category().default('initial').sync(),
    });
    const evaluator = new InMemoryFlagEvaluator(schema.schema, {
      testFlag: 'initial',
    }) as any;

    // getSync receives ctx as first param (can be empty object for simple evaluator)
    expect(evaluator.getSync({} as Parameters<typeof evaluator.getSync>[0], 'testFlag')).toBe('initial');

    evaluator.setFlag('testFlag', 'updated');

    expect(evaluator.getSync({} as Parameters<typeof evaluator.getSync>[0], 'testFlag')).toBe('updated');
  });

  test('InMemoryFlagEvaluator works with async', async () => {
    const schema = defineFeatureFlags({
      asyncFlag: S.number().default(0).async(),
    });
    const evaluator = new InMemoryFlagEvaluator(schema.schema, {
      asyncFlag: 42,
    }) as any;

    expect(await evaluator.getAsync({} as Parameters<typeof evaluator.getAsync>[0], 'asyncFlag')).toBe(42);
  });

  test('forContext creates child evaluator bound to child SpanContext', async () => {
    const flags = defineFeatureFlags({
      debugMode: S.boolean().default(false).sync(),
    });

    const ctx = defineOpContext({
      logSchema: testLogSchema,
      flags: flags.schema,
    });

    const flagEvaluator = new InMemoryFlagEvaluator(flags.schema, { debugMode: true }) as any;

    const { trace } = new TestTracer(ctx, { flagEvaluator });
    await trace('parent-span', async (parentCtx) => {
      const parentFf = parentCtx.ff as unknown as { debugMode: BooleanFlagContext | undefined };

      // Access flag in parent
      expect(parentFf.debugMode?.value).toBe(true);

      const parentAccessCount = countEntryType(parentCtx.buffer, ENTRY_TYPE_FF_ACCESS);
      expect(parentAccessCount).toBe(1);

      // Create child span - child gets its own ff evaluator via forContext
      await parentCtx.span('child-span', async (childCtx) => {
        const childFf = childCtx.ff as unknown as { debugMode: BooleanFlagContext | undefined };
        expect(childFf.debugMode?.value).toBe(true);

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

    const enabledFlagEvaluator = new InMemoryFlagEvaluator(flags.schema, { darkMode: true }) as any;
    const { trace: enabledTrace } = new TestTracer(enabledCtx, { flagEvaluator: enabledFlagEvaluator });

    await enabledTrace('test-enabled', async (ctx) => {
      const ff = ctx.ff as unknown as { darkMode: BooleanFlagContext | undefined };
      const darkMode = ff.darkMode;

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

    const disabledFlagEvaluator = new InMemoryFlagEvaluator(flags.schema, { darkMode: false }) as any;
    const { trace: disabledTrace } = new TestTracer(disabledCtx, { flagEvaluator: disabledFlagEvaluator });

    await disabledTrace('test-disabled', async (ctx) => {
      const ff = ctx.ff as unknown as { darkMode: BooleanFlagContext | undefined };
      const darkModeDisabled = ff.darkMode;

      // Falsy check works - undefined is falsy
      expect(darkModeDisabled).toBeUndefined();
      if (!darkModeDisabled) {
        expect(true).toBe(true);
      }

      return ctx.ok(null);
    });
  });
});
