import { describe, expect, test } from 'bun:test';
import { createTestSchema, createTestSpanBuffer } from '../../__tests__/test-helpers.js';
import { createSpanLogger } from '../../codegen/spanLoggerGenerator.js';
import type { SpanContext } from '../../spanContext.js';
import type { SpanBuffer } from '../../types.js';
import { S } from '../builder.js';
import { defineFeatureFlags } from '../defineFeatureFlags.js';
import {
  type BooleanFlagContext,
  FeatureFlagEvaluator,
  InMemoryFlagEvaluator,
  type VariantFlagContext,
} from '../evaluator.js';
import { ENTRY_TYPE_FF_ACCESS, ENTRY_TYPE_FF_USAGE } from '../systemSchema.js';
import type { LogSchema } from '../types.js';

/**
 * Create a mock SpanContext for testing FeatureFlagEvaluator
 */
function createMockSpanContext<T extends LogSchema>(spanBuffer: SpanBuffer<T>): SpanContext<T, any, any> {
  // Create a real SpanLogger for the buffer using the schema from module
  const schema = spanBuffer.module.logSchema as T;
  const logger = createSpanLogger(schema, spanBuffer);

  const mockCtx = {
    _buffer: spanBuffer,
    get buffer() {
      return spanBuffer;
    },
    log: logger,
    traceId: spanBuffer._traceId,
    ff: null as any,
    env: {},
    deps: {},
    tag: {} as any,
    scope: spanBuffer.scopeValues || {},
    setScope: (attrs: any) => {
      if (!spanBuffer.scopeValues) {
        spanBuffer.scopeValues = {};
      }
      Object.assign(spanBuffer.scopeValues, attrs);
    },
    ok: () => ({ success: true, value: undefined }),
    err: () => ({ success: false, error: 'error' }),
    span: () => Promise.resolve(undefined),
    span_op: () => Promise.resolve(undefined),
    span_fn: () => Promise.resolve(undefined),
  } as unknown as SpanContext<T, any, any>;

  return mockCtx;
}

/**
 * Helper to count entries of a specific type in the buffer
 */
function countEntryType(buffer: SpanBuffer<any>, entryType: number): number {
  let count = 0;
  let current: SpanBuffer<any> | null = buffer;
  while (current) {
    const writeIndex = current.writeIndex;
    for (let i = 0; i < writeIndex; i++) {
      if (current._operations[i] === entryType) {
        count++;
      }
    }
    current = current.next;
  }
  return count;
}

describe('Feature Flags', () => {
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

  test('evaluator returns FlagContext for truthy flags', () => {
    const schema = defineFeatureFlags({
      debugMode: S.boolean().default(false).sync(),
      maxRetries: S.number().default(3).sync(),
    });
    const logSchema = createTestSchema({});
    const { spanBuffer } = createTestSpanBuffer(logSchema, { spanName: 'test-span' });
    const mockCtx = createMockSpanContext(spanBuffer);

    const evaluator = new InMemoryFlagEvaluator(schema.schema, {
      debugMode: true,
      maxRetries: 5,
    });

    const ff = new FeatureFlagEvaluator(schema.schema, mockCtx, evaluator);

    // New API: truthy flags return FlagContext wrappers
    type FfWithFlags = typeof ff & {
      debugMode: BooleanFlagContext | undefined;
      maxRetries: { value: number; track: () => void } | undefined;
    };

    const debugMode = (ff as FfWithFlags).debugMode;
    expect(debugMode).toBeDefined();
    expect(debugMode?.value).toBe(true);
    expect(typeof debugMode?.track).toBe('function');

    const maxRetries = (ff as FfWithFlags).maxRetries;
    expect(maxRetries).toBeDefined();
    expect(maxRetries?.value).toBe(5);
    expect(typeof maxRetries?.track).toBe('function');
  });

  test('evaluator returns undefined for falsy flags', () => {
    const schema = defineFeatureFlags({
      debugMode: S.boolean().default(false).sync(),
      userLimit: S.number().default(100).async(),
    });
    const logSchema = createTestSchema({});
    const { spanBuffer } = createTestSpanBuffer(logSchema, { spanName: 'test-span' });
    const mockCtx = createMockSpanContext(spanBuffer);

    const evaluator = new InMemoryFlagEvaluator(schema.schema, {}); // No flags set → null values

    const ff = new FeatureFlagEvaluator(schema.schema, mockCtx, evaluator);

    // New API: falsy flags (including false defaults) return undefined
    type FfWithFlags = typeof ff & { debugMode: BooleanFlagContext | undefined };
    expect((ff as FfWithFlags).debugMode).toBeUndefined();
  });

  test('async flags return FlagContext via get method', async () => {
    const schema = defineFeatureFlags({
      userSpecificLimit: S.number().default(100).async(),
      dynamicProvider: S.enum(['stripe', 'paypal']).default('stripe').async(),
    });
    const logSchema = createTestSchema({});
    const { spanBuffer } = createTestSpanBuffer(logSchema, { spanName: 'test-span' });
    const mockCtx = createMockSpanContext(spanBuffer);

    const evaluator = new InMemoryFlagEvaluator(schema.schema, {
      userSpecificLimit: 200,
      dynamicProvider: 'paypal',
    });

    const ff = new FeatureFlagEvaluator(schema.schema, mockCtx, evaluator);

    // Async flags accessed via get() return FlagContext
    const limit = (await ff.get('userSpecificLimit')) as { value: number; track: () => void };
    expect(limit).toBeDefined();
    expect(limit.value).toBe(200);

    const provider = (await ff.get('dynamicProvider')) as VariantFlagContext<string>;
    expect(provider).toBeDefined();
    expect(provider.value).toBe('paypal');
  });

  test('trackUsage logs usage events to buffer', () => {
    const schema = defineFeatureFlags({
      advancedValidation: S.boolean().default(false).sync(),
    });
    const logSchema = createTestSchema({});
    const { spanBuffer } = createTestSpanBuffer(logSchema, { spanName: 'test-span' });
    const mockCtx = createMockSpanContext(spanBuffer);

    const evaluator = new InMemoryFlagEvaluator(schema.schema, {
      advancedValidation: true,
    });

    const ff = new FeatureFlagEvaluator(schema.schema, mockCtx, evaluator);

    ff.trackUsage('advancedValidation', {
      action: 'validation_performed',
      outcome: 'success',
    });

    // Check buffer for ff-usage entry
    const usageCount = countEntryType(spanBuffer, ENTRY_TYPE_FF_USAGE);
    expect(usageCount).toBe(1);
  });

  test('track() on FlagContext logs usage events', () => {
    const schema = defineFeatureFlags({
      advancedValidation: S.boolean().default(false).sync(),
    });
    const logSchema = createTestSchema({});
    const { spanBuffer } = createTestSpanBuffer(logSchema, { spanName: 'test-span' });
    const mockCtx = createMockSpanContext(spanBuffer);

    const evaluator = new InMemoryFlagEvaluator(schema.schema, {
      advancedValidation: true,
    });

    const ff = new FeatureFlagEvaluator(schema.schema, mockCtx, evaluator);

    // Access flag to get FlagContext
    type FfWithFlags = typeof ff & { advancedValidation: BooleanFlagContext | undefined };
    const flag = (ff as FfWithFlags).advancedValidation;

    // Use track() on the flag context
    flag?.track({ action: 'validation_performed', outcome: 'success' });

    // Check buffer for ff-usage entry (access is logged automatically, usage is separate)
    const usageCount = countEntryType(spanBuffer, ENTRY_TYPE_FF_USAGE);
    expect(usageCount).toBe(1);
  });

  test('sync flag access is logged only once per span', () => {
    const schema = defineFeatureFlags({
      debugMode: S.boolean().default(false).sync(),
    });
    const logSchema = createTestSchema({});
    const { spanBuffer } = createTestSpanBuffer(logSchema, { spanName: 'test-span' });
    const mockCtx = createMockSpanContext(spanBuffer);

    const evaluator = new InMemoryFlagEvaluator(schema.schema, { debugMode: true });

    const ff = new FeatureFlagEvaluator(schema.schema, mockCtx, evaluator);

    // Access the flag multiple times
    type FfWithFlags = typeof ff & { debugMode: BooleanFlagContext | undefined };
    const value1 = (ff as FfWithFlags).debugMode;
    const value2 = (ff as FfWithFlags).debugMode;
    const value3 = (ff as FfWithFlags).debugMode;

    // All accesses should return same value (flag is truthy)
    expect(value1?.value).toBe(true);
    expect(value2?.value).toBe(true);
    expect(value3?.value).toBe(true);

    // Only ONE ff-access log should be written (deduplication via buffer scan)
    const accessCount = countEntryType(spanBuffer, ENTRY_TYPE_FF_ACCESS);
    expect(accessCount).toBe(1);
  });

  test('async flag access is logged', async () => {
    const schema = defineFeatureFlags({
      userLimit: S.number().default(100).async(),
    });
    const logSchema = createTestSchema({});
    const { spanBuffer } = createTestSpanBuffer(logSchema, { spanName: 'test-span' });
    const mockCtx = createMockSpanContext(spanBuffer);

    const evaluator = new InMemoryFlagEvaluator(schema.schema, { userLimit: 200 });

    const ff = new FeatureFlagEvaluator(schema.schema, mockCtx, evaluator);

    const value = (await ff.get('userLimit')) as { value: number };

    expect(value.value).toBe(200);

    // Check buffer for ff-access entry
    const accessCount = countEntryType(spanBuffer, ENTRY_TYPE_FF_ACCESS);
    expect(accessCount).toBe(1);
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
    const schema = defineFeatureFlags({
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
    const logSchema = createTestSchema({});
    const { spanBuffer } = createTestSpanBuffer(logSchema, { spanName: 'test-span' });
    const mockCtx = createMockSpanContext(spanBuffer);

    const evaluator = new InMemoryFlagEvaluator(schema.schema, {
      enableFeatureX: true,
      maxConnectionPool: 20,
      logLevel: 'debug',
      userTier: 'premium',
      customLimit: 500,
    });

    const ff = new FeatureFlagEvaluator(schema.schema, mockCtx, evaluator);

    // Test sync flags - now return FlagContext wrappers
    type FfWithFlags = typeof ff & {
      enableFeatureX: BooleanFlagContext | undefined;
      maxConnectionPool: { value: number; track: () => void } | undefined;
      logLevel: VariantFlagContext<string> | undefined;
    };

    const enableFeatureX = (ff as FfWithFlags).enableFeatureX;
    expect(enableFeatureX?.value).toBe(true);

    const maxConnectionPool = (ff as FfWithFlags).maxConnectionPool;
    expect(maxConnectionPool?.value).toBe(20);

    const logLevel = (ff as FfWithFlags).logLevel;
    expect(logLevel?.value).toBe('debug');

    // Test async flags
    const userTier = (await ff.get('userTier')) as VariantFlagContext<string>;
    expect(userTier.value).toBe('premium');

    const customLimit = (await ff.get('customLimit')) as { value: number };
    expect(customLimit.value).toBe(500);
  });

  test('InMemoryFlagEvaluator setFlag updates values', () => {
    const schema = defineFeatureFlags({
      testFlag: S.category().default('initial').sync(),
    });
    const evaluator = new InMemoryFlagEvaluator(schema.schema, {
      testFlag: 'initial',
    });

    expect(evaluator.getSync('testFlag', {})).toBe('initial');

    evaluator.setFlag('testFlag', 'updated');

    expect(evaluator.getSync('testFlag', {})).toBe('updated');
  });

  test('InMemoryFlagEvaluator works with async', async () => {
    const schema = defineFeatureFlags({
      asyncFlag: S.number().default(0).async(),
    });
    const evaluator = new InMemoryFlagEvaluator(schema.schema, {
      asyncFlag: 42,
    });

    expect(await evaluator.getAsync('asyncFlag', {})).toBe(42);
  });

  test('forContext creates child evaluator bound to child SpanContext', () => {
    const schema = defineFeatureFlags({
      debugMode: S.boolean().default(false).sync(),
    });
    const logSchema = createTestSchema({});
    const { spanBuffer } = createTestSpanBuffer(logSchema, { spanName: 'test-span' });
    const mockCtx = createMockSpanContext(spanBuffer);

    const evaluator = new InMemoryFlagEvaluator(schema.schema, { debugMode: true });

    const ff = new FeatureFlagEvaluator(schema.schema, mockCtx, evaluator);

    // Create child SpanContext
    const { spanBuffer: childBuffer } = createTestSpanBuffer(logSchema, { spanName: 'child-span' });
    const childCtx = createMockSpanContext(childBuffer);

    // Create child evaluator bound to child context
    const childFf = ff.forContext(childCtx);

    // Both should have the flag accessible
    type FfWithFlags = typeof ff & { debugMode: BooleanFlagContext | undefined };
    expect((ff as FfWithFlags).debugMode?.value).toBe(true);
    expect((childFf as unknown as FfWithFlags).debugMode?.value).toBe(true);

    // Access in child should log to child buffer
    const childAccessCount = countEntryType(childBuffer, ENTRY_TYPE_FF_ACCESS);
    expect(childAccessCount).toBe(1);

    // Parent buffer should have its own access log from earlier access
    const parentAccessCount = countEntryType(spanBuffer, ENTRY_TYPE_FF_ACCESS);
    expect(parentAccessCount).toBe(1);
  });

  test('usage example from spec: undefined/truthy pattern', () => {
    const schema = defineFeatureFlags({
      darkMode: S.boolean().default(false).sync(),
      advancedSearch: S.boolean().default(false).sync(),
    });
    const logSchema = createTestSchema({});

    // Scenario 1: darkMode is enabled
    const { spanBuffer: enabledBuffer } = createTestSpanBuffer(logSchema, { spanName: 'test-enabled' });
    const enabledCtx = createMockSpanContext(enabledBuffer);
    const enabledEvaluator = new InMemoryFlagEvaluator(schema.schema, { darkMode: true });
    const ffEnabled = new FeatureFlagEvaluator(schema.schema, enabledCtx, enabledEvaluator);

    type FfWithFlags = typeof ffEnabled & {
      darkMode: BooleanFlagContext | undefined;
      advancedSearch: BooleanFlagContext | undefined;
    };

    const darkMode = (ffEnabled as FfWithFlags).darkMode;

    // Truthy check works naturally with undefined/truthy semantics
    if (darkMode) {
      // darkMode is BooleanFlagContext here
      expect(darkMode.value).toBe(true);
      // Can track usage
      darkMode.track({ action: 'applied' });
    }

    // Scenario 2: darkMode is disabled
    const { spanBuffer: disabledBuffer } = createTestSpanBuffer(logSchema, { spanName: 'test-disabled' });
    const disabledCtx = createMockSpanContext(disabledBuffer);
    const disabledEvaluator = new InMemoryFlagEvaluator(schema.schema, { darkMode: false });
    const ffDisabled = new FeatureFlagEvaluator(schema.schema, disabledCtx, disabledEvaluator);

    const darkModeDisabled = (ffDisabled as FfWithFlags).darkMode;

    // Falsy check works - undefined is falsy
    expect(darkModeDisabled).toBeUndefined();
    if (!darkModeDisabled) {
      // This branch executes for disabled flags
      expect(true).toBe(true);
    }
  });
});
