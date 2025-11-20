import { describe, expect, test } from 'bun:test';
import { defineFeatureFlags, type EvaluationContext } from '../defineFeatureFlags.js';
import { FeatureFlagEvaluator, InMemoryFlagEvaluator, type FlagValue } from '../evaluator.js';
import { S } from '../builder.js';

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

  test('evaluator provides sync flags as properties', () => {
    const schema = defineFeatureFlags({
      debugMode: S.boolean().default(false).sync(),
      maxRetries: S.number().default(3).sync(),
    });

    const evaluator = new InMemoryFlagEvaluator({
      debugMode: true,
      maxRetries: 5,
    });

    const ff = new FeatureFlagEvaluator(
      schema.schema,
      { userId: 'user-123' },
      evaluator
    );

    // Sync flags accessed as properties
    type FfWithSyncFlags = typeof ff & { debugMode: boolean; maxRetries: number };
    expect((ff as FfWithSyncFlags).debugMode).toBe(true);
    expect((ff as FfWithSyncFlags).maxRetries).toBe(5);
  });

  test('evaluator provides async flags via get method', async () => {
    const schema = defineFeatureFlags({
      userSpecificLimit: S.number().default(100).async(),
      dynamicProvider: S.enum(['stripe', 'paypal'])
        .default('stripe')
        .async(),
    });

    const evaluator = new InMemoryFlagEvaluator({
      userSpecificLimit: 200,
      dynamicProvider: 'paypal',
    });

    const ff = new FeatureFlagEvaluator(
      schema.schema,
      { userId: 'user-123' },
      evaluator
    );

    // Async flags accessed via get()
    const limit = await ff.get('userSpecificLimit');
    const provider = await ff.get('dynamicProvider');

    expect(limit).toBe(200);
    expect(provider).toBe('paypal');
  });

  test('returns default values when flag not set', async () => {
    const schema = defineFeatureFlags({
      debugMode: S.boolean().default(false).sync(),
      userLimit: S.number().default(100).async(),
    });

    const evaluator = new InMemoryFlagEvaluator({}); // No flags set

    const ff = new FeatureFlagEvaluator(
      schema.schema,
      { userId: 'user-123' },
      evaluator
    );

    type FfWithSyncFlags = typeof ff & { debugMode: boolean };
    expect((ff as FfWithSyncFlags).debugMode).toBe(false);
    expect(await ff.get('userLimit')).toBe(100);
  });

  test('trackUsage logs usage events', () => {
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

    const ff = new FeatureFlagEvaluator(
      schema.schema,
      { userId: 'user-123' },
      evaluator,
      mockWriters
    );

    ff.trackUsage('advancedValidation', {
      action: 'validation_performed',
      outcome: 'success',
    });

    expect(logs).toContainEqual({ type: 'ff-usage' });
    expect(logs).toContainEqual({ name: 'advancedValidation' });
    expect(logs).toContainEqual({ action: 'validation_performed' });
    expect(logs).toContainEqual({ outcome: 'success' });
  });

  test('sync flag access is logged', () => {
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

    const ff = new FeatureFlagEvaluator(
      schema.schema,
      { userId: 'user-123' },
      evaluator,
      mockWriters
    );

    // Access the flag
    type FfWithSyncFlags = typeof ff & { debugMode: boolean };
    const value = (ff as FfWithSyncFlags).debugMode;

    expect(value).toBe(true);
    expect(logs).toContainEqual({ type: 'ff-access' });
    expect(logs).toContainEqual({ name: 'debugMode' });
    expect(logs).toContainEqual({ value: true });
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

    const ff = new FeatureFlagEvaluator(
      schema.schema,
      { userId: 'user-123' },
      evaluator,
      mockWriters
    );

    const value = await ff.get('userLimit');

    expect(value).toBe(200);
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
      userTier: S.string().default('free').async(),
      customLimit: S.number().default(100).async(),
    });

    const evaluator = new InMemoryFlagEvaluator({
      enableFeatureX: true,
      maxConnectionPool: 20,
      logLevel: 'debug',
      userTier: 'premium',
      customLimit: 500,
    });

    const ff = new FeatureFlagEvaluator(
      schema.schema,
      { userId: 'user-123' },
      evaluator
    );

    // Test sync flags
    type FfWithSyncFlags = typeof ff & {
      enableFeatureX: boolean;
      maxConnectionPool: number;
      logLevel: 'info' | 'debug' | 'warn';
    };
    expect((ff as FfWithSyncFlags).enableFeatureX).toBe(true);
    expect((ff as FfWithSyncFlags).maxConnectionPool).toBe(20);
    expect((ff as FfWithSyncFlags).logLevel).toBe('debug');

    // Test async flags
    expect(await ff.get('userTier')).toBe('premium');
    expect(await ff.get('customLimit')).toBe(500);
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
});
