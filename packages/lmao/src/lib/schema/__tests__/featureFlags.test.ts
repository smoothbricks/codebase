import { describe, expect, test } from 'bun:test';
import { S } from '../builder.js';
import { defineFeatureFlags, type EvaluationContext } from '../defineFeatureFlags.js';
import {
  type BooleanFlagContext,
  FeatureFlagEvaluator,
  type FlagTrackContext,
  type FlagValue,
  InMemoryFlagEvaluator,
  type VariantFlagContext,
} from '../evaluator.js';

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

    const evaluator = new InMemoryFlagEvaluator({
      debugMode: true,
      maxRetries: 5,
    });

    const ff = new FeatureFlagEvaluator(schema.schema, { userId: 'user-123' }, evaluator);

    // New API: truthy flags return FlagContext wrappers
    type FfWithFlags = typeof ff & {
      debugMode: BooleanFlagContext | undefined;
      maxRetries: { value: number; track: (ctx?: FlagTrackContext) => void } | undefined;
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

    const evaluator = new InMemoryFlagEvaluator({}); // No flags set → null values

    const ff = new FeatureFlagEvaluator(schema.schema, { userId: 'user-123' }, evaluator);

    // New API: falsy flags (including false defaults) return undefined
    type FfWithFlags = typeof ff & { debugMode: BooleanFlagContext | undefined };
    expect((ff as FfWithFlags).debugMode).toBeUndefined();
  });

  test('async flags return FlagContext via get method', async () => {
    const schema = defineFeatureFlags({
      userSpecificLimit: S.number().default(100).async(),
      dynamicProvider: S.enum(['stripe', 'paypal']).default('stripe').async(),
    });

    const evaluator = new InMemoryFlagEvaluator({
      userSpecificLimit: 200,
      dynamicProvider: 'paypal',
    });

    const ff = new FeatureFlagEvaluator(schema.schema, { userId: 'user-123' }, evaluator);

    // Async flags accessed via get() return FlagContext
    const limit = (await ff.get('userSpecificLimit')) as { value: number; track: () => void };
    expect(limit).toBeDefined();
    expect(limit.value).toBe(200);

    const provider = (await ff.get('dynamicProvider')) as VariantFlagContext<string>;
    expect(provider).toBeDefined();
    expect(provider.value).toBe('paypal');
  });

  test('trackUsage logs usage events via FlagColumnWriters', () => {
    const schema = defineFeatureFlags({
      advancedValidation: S.boolean().default(false).sync(),
    });

    const evaluator = new InMemoryFlagEvaluator({
      advancedValidation: true,
    });

    type LogEntry = Record<string, unknown>;
    const logs: LogEntry[] = [];
    const mockWriters = {
      writeEntryType: (type: string) => logs.push({ type }),
      writeFfName: (name: string) => logs.push({ name }),
      writeFfValue: (value: FlagValue) => logs.push({ value }),
      writeAction: (action?: string) => logs.push({ action }),
      writeOutcome: (outcome?: string) => logs.push({ outcome }),
      writeContextAttributes: (ctx: EvaluationContext) => logs.push({ context: ctx }),
    };

    const ff = new FeatureFlagEvaluator(schema.schema, { userId: 'user-123' }, evaluator, mockWriters);

    ff.trackUsage('advancedValidation', {
      action: 'validation_performed',
      outcome: 'success',
    });

    expect(logs).toContainEqual({ type: 'ff-usage' });
    expect(logs).toContainEqual({ name: 'advancedValidation' });
    expect(logs).toContainEqual({ action: 'validation_performed' });
    expect(logs).toContainEqual({ outcome: 'success' });
  });

  test('track() on FlagContext logs usage events', () => {
    const schema = defineFeatureFlags({
      advancedValidation: S.boolean().default(false).sync(),
    });

    const evaluator = new InMemoryFlagEvaluator({
      advancedValidation: true,
    });

    type LogEntry = Record<string, unknown>;
    const logs: LogEntry[] = [];
    const mockWriters = {
      writeEntryType: (type: string) => logs.push({ type }),
      writeFfName: (name: string) => logs.push({ name }),
      writeFfValue: (value: FlagValue) => logs.push({ value }),
      writeAction: (action?: string) => logs.push({ action }),
      writeOutcome: (outcome?: string) => logs.push({ outcome }),
      writeContextAttributes: (ctx: EvaluationContext) => logs.push({ context: ctx }),
    };

    const ff = new FeatureFlagEvaluator(schema.schema, { userId: 'user-123' }, evaluator, mockWriters);

    // Access flag to get FlagContext
    type FfWithFlags = typeof ff & { advancedValidation: BooleanFlagContext | undefined };
    const flag = (ff as FfWithFlags).advancedValidation;

    // Clear logs from access
    logs.length = 0;

    // Use track() on the flag context
    flag?.track({ action: 'validation_performed', outcome: 'success' });

    expect(logs).toContainEqual({ type: 'ff-usage' });
    expect(logs).toContainEqual({ name: 'advancedValidation' });
    expect(logs).toContainEqual({ action: 'validation_performed' });
    expect(logs).toContainEqual({ outcome: 'success' });
  });

  test('sync flag access is logged only once per span', () => {
    const schema = defineFeatureFlags({
      debugMode: S.boolean().default(false).sync(),
    });

    const evaluator = new InMemoryFlagEvaluator({ debugMode: true });

    type LogEntry = Record<string, unknown>;
    const logs: LogEntry[] = [];
    const mockWriters = {
      writeEntryType: (type: string) => logs.push({ type }),
      writeFfName: (name: string) => logs.push({ name }),
      writeFfValue: (value: FlagValue) => logs.push({ value }),
      writeAction: () => {},
      writeOutcome: () => {},
      writeContextAttributes: (ctx: EvaluationContext) => logs.push({ context: ctx }),
    };

    const ff = new FeatureFlagEvaluator(schema.schema, { userId: 'user-123' }, evaluator, mockWriters);

    // Access the flag multiple times
    type FfWithFlags = typeof ff & { debugMode: BooleanFlagContext | undefined };
    const value1 = (ff as FfWithFlags).debugMode;
    const value2 = (ff as FfWithFlags).debugMode;
    const value3 = (ff as FfWithFlags).debugMode;

    // All accesses should return same cached value
    expect(value1).toBe(value2);
    expect(value2).toBe(value3);
    expect(value1?.value).toBe(true);

    // Only ONE ff-access log should be written (deduplication)
    const accessLogs = logs.filter((l) => l.type === 'ff-access');
    expect(accessLogs).toHaveLength(1);
  });

  test('async flag access is logged', async () => {
    const schema = defineFeatureFlags({
      userLimit: S.number().default(100).async(),
    });

    const evaluator = new InMemoryFlagEvaluator({ userLimit: 200 });

    type LogEntry = Record<string, unknown>;
    const logs: LogEntry[] = [];
    const mockWriters = {
      writeEntryType: (type: string) => logs.push({ type }),
      writeFfName: (name: string) => logs.push({ name }),
      writeFfValue: (value: FlagValue) => logs.push({ value }),
      writeAction: () => {},
      writeOutcome: () => {},
      writeContextAttributes: (ctx: EvaluationContext) => logs.push({ context: ctx }),
    };

    const ff = new FeatureFlagEvaluator(schema.schema, { userId: 'user-123' }, evaluator, mockWriters);

    const value = (await ff.get('userLimit')) as { value: number };

    expect(value.value).toBe(200);
    expect(logs).toContainEqual({ type: 'ff-access' });
    expect(logs).toContainEqual({ name: 'userLimit' });
    expect(logs).toContainEqual({ value: 200 });
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

    const evaluator = new InMemoryFlagEvaluator({
      enableFeatureX: true,
      maxConnectionPool: 20,
      logLevel: 'debug',
      userTier: 'premium',
      customLimit: 500,
    });

    const ff = new FeatureFlagEvaluator(schema.schema, { userId: 'user-123' }, evaluator);

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
    const evaluator = new InMemoryFlagEvaluator({
      testFlag: 'initial',
    });

    expect(evaluator.getSync('testFlag', {})).toBe('initial');

    evaluator.setFlag('testFlag', 'updated');

    expect(evaluator.getSync('testFlag', {})).toBe('updated');
  });

  test('InMemoryFlagEvaluator works with async', async () => {
    const evaluator = new InMemoryFlagEvaluator({
      asyncFlag: 42,
    });

    expect(await evaluator.getAsync('asyncFlag', {})).toBe(42);
  });

  test('forContext creates child evaluator with additional context', () => {
    const schema = defineFeatureFlags({
      debugMode: S.boolean().default(false).sync(),
    });

    const evaluator = new InMemoryFlagEvaluator({ debugMode: true });

    const ff = new FeatureFlagEvaluator(schema.schema, { userId: 'user-123' }, evaluator);

    // Create child evaluator with additional context
    const childFf = ff.forContext({ requestId: 'req-456' });

    // Both should have the flag accessible
    type FfWithFlags = typeof ff & { debugMode: BooleanFlagContext | undefined };
    expect((ff as FfWithFlags).debugMode?.value).toBe(true);
    expect((childFf as unknown as FfWithFlags).debugMode?.value).toBe(true);

    // Child should have merged context
    expect(childFf.getContext().userId).toBe('user-123');
    expect(childFf.getContext().requestId).toBe('req-456');
  });

  test('usage example from spec: undefined/truthy pattern', () => {
    const schema = defineFeatureFlags({
      darkMode: S.boolean().default(false).sync(),
      advancedSearch: S.boolean().default(false).sync(),
    });

    // Scenario 1: darkMode is enabled
    const enabledEvaluator = new InMemoryFlagEvaluator({ darkMode: true });
    const ffEnabled = new FeatureFlagEvaluator(schema.schema, { userId: 'user-123' }, enabledEvaluator);

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
    const disabledEvaluator = new InMemoryFlagEvaluator({ darkMode: false });
    const ffDisabled = new FeatureFlagEvaluator(schema.schema, { userId: 'user-123' }, disabledEvaluator);

    const darkModeDisabled = (ffDisabled as FfWithFlags).darkMode;

    // Falsy check works - undefined is falsy
    expect(darkModeDisabled).toBeUndefined();
    if (!darkModeDisabled) {
      // This branch executes for disabled flags
      expect(true).toBe(true);
    }
  });
});
