import { describe, expect, test } from 'bun:test';
import { S } from '../../schema/builder.js';
import { defineFeatureFlags, type EvaluationContext } from '../../schema/defineFeatureFlags.js';
import { type BooleanFlagContext, type FlagValue, InMemoryFlagEvaluator } from '../../schema/evaluator.js';
import { createEvaluatorClass, type GeneratedEvaluatorState, generateEvaluatorClass } from '../evaluatorGenerator.js';

// Mock dependencies for testing the generated class
const mockValidateFlagValue = <T>(value: unknown, _schema: unknown, defaultValue: T): T => {
  if (value === null || value === undefined) {
    return defaultValue;
  }
  return value as T;
};

const mockGetTimestampNanos = (): bigint => BigInt(Date.now()) * 1_000_000n;

const MOCK_FF_ACCESS = 7;
const MOCK_FF_USAGE = 8;

describe('EvaluatorGenerator', () => {
  describe('generateEvaluatorClass', () => {
    test('generates valid JavaScript class code', () => {
      const schema = defineFeatureFlags({
        debugMode: S.boolean().default(false).sync(),
        maxRetries: S.number().default(3).sync(),
      });

      const code = generateEvaluatorClass(schema.schema);

      // Should be a valid IIFE wrapper
      expect(code).toContain('(function(validateFlagValue, getTimestampNanos');
      expect(code).toContain("'use strict'");
      expect(code).toContain('class GeneratedEvaluator');

      // Should have getters for each flag
      expect(code).toContain('get debugMode()');
      expect(code).toContain('get maxRetries()');

      // Should have core methods
      expect(code).toContain('forContext(additional)');
      expect(code).toContain('withBuffer(buffer)');
      expect(code).toContain('getContext()');
      expect(code).toContain('async get(flag)');
      expect(code).toContain('trackUsage(flag, context)');
    });

    test('generates unique getter for each flag', () => {
      const schema = defineFeatureFlags({
        featureA: S.boolean().default(false).sync(),
        featureB: S.boolean().default(false).sync(),
        featureC: S.number().default(0).sync(),
        featureD: S.enum(['a', 'b', 'c'] as const)
          .default('a')
          .sync(),
      });

      const code = generateEvaluatorClass(schema.schema);

      expect(code).toContain('get featureA()');
      expect(code).toContain('get featureB()');
      expect(code).toContain('get featureC()');
      expect(code).toContain('get featureD()');

      // Each getter should call #getFlag with the flag name
      expect(code).toContain("this.#getFlag('featureA')");
      expect(code).toContain("this.#getFlag('featureB')");
      expect(code).toContain("this.#getFlag('featureC')");
      expect(code).toContain("this.#getFlag('featureD')");
    });
  });

  describe('createEvaluatorClass', () => {
    test('creates a functional class constructor', () => {
      const schema = defineFeatureFlags({
        debugMode: S.boolean().default(false).sync(),
      });

      const GeneratedClass = createEvaluatorClass(
        schema.schema,
        mockValidateFlagValue,
        mockGetTimestampNanos,
        MOCK_FF_ACCESS,
        MOCK_FF_USAGE,
      );

      expect(typeof GeneratedClass).toBe('function');

      const evaluator = new InMemoryFlagEvaluator({ debugMode: true });
      const state: GeneratedEvaluatorState<typeof schema.schema> = {
        schema: schema.schema,
        evaluationContext: { userId: 'test' },
        evaluator,
        buffer: null,

        accessedFlags: new Set(),
        flagCache: new Map(),
      };

      const instance = new GeneratedClass(state);
      expect(instance).toBeDefined();
    });

    test('caches generated classes by schema', () => {
      const schema = defineFeatureFlags({
        testFlag: S.boolean().default(false).sync(),
      });

      const Class1 = createEvaluatorClass(
        schema.schema,
        mockValidateFlagValue,
        mockGetTimestampNanos,
        MOCK_FF_ACCESS,
        MOCK_FF_USAGE,
      );

      const Class2 = createEvaluatorClass(
        schema.schema,
        mockValidateFlagValue,
        mockGetTimestampNanos,
        MOCK_FF_ACCESS,
        MOCK_FF_USAGE,
      );

      // Same schema should return same class (cached)
      expect(Class1).toBe(Class2);
    });

    test('generates different classes for different schemas', () => {
      const schema1 = defineFeatureFlags({
        flagA: S.boolean().default(false).sync(),
      });

      const schema2 = defineFeatureFlags({
        flagB: S.boolean().default(false).sync(),
      });

      const Class1 = createEvaluatorClass(
        schema1.schema,
        mockValidateFlagValue,
        mockGetTimestampNanos,
        MOCK_FF_ACCESS,
        MOCK_FF_USAGE,
      );

      const Class2 = createEvaluatorClass(
        schema2.schema,
        mockValidateFlagValue,
        mockGetTimestampNanos,
        MOCK_FF_ACCESS,
        MOCK_FF_USAGE,
      );

      // Different schemas should generate different classes
      expect(Class1).not.toBe(Class2);
    });
  });

  describe('Generated class functionality', () => {
    test('flag getters return FlagContext for truthy values', () => {
      const schema = defineFeatureFlags({
        debugMode: S.boolean().default(false).sync(),
      });

      const GeneratedClass = createEvaluatorClass(
        schema.schema,
        mockValidateFlagValue,
        mockGetTimestampNanos,
        MOCK_FF_ACCESS,
        MOCK_FF_USAGE,
      );

      const evaluator = new InMemoryFlagEvaluator({ debugMode: true });
      const state: GeneratedEvaluatorState<typeof schema.schema> = {
        schema: schema.schema,
        evaluationContext: { userId: 'test' },
        evaluator,
        buffer: null,

        accessedFlags: new Set(),
        flagCache: new Map(),
      };

      const instance = new GeneratedClass(state);

      // Access the flag via generated getter
      const flag = (instance as unknown as { debugMode: BooleanFlagContext | undefined }).debugMode;

      expect(flag).toBeDefined();
      expect(flag?.value).toBe(true);
      expect(typeof flag?.track).toBe('function');
    });

    test('flag getters return undefined for falsy values', () => {
      const schema = defineFeatureFlags({
        debugMode: S.boolean().default(false).sync(),
      });

      const GeneratedClass = createEvaluatorClass(
        schema.schema,
        mockValidateFlagValue,
        mockGetTimestampNanos,
        MOCK_FF_ACCESS,
        MOCK_FF_USAGE,
      );

      const evaluator = new InMemoryFlagEvaluator({ debugMode: false });
      const state: GeneratedEvaluatorState<typeof schema.schema> = {
        schema: schema.schema,
        evaluationContext: { userId: 'test' },
        evaluator,
        buffer: null,

        accessedFlags: new Set(),
        flagCache: new Map(),
      };

      const instance = new GeneratedClass(state);

      const flag = (instance as unknown as { debugMode: BooleanFlagContext | undefined }).debugMode;
      expect(flag).toBeUndefined();
    });

    test('forContext creates new instance with merged context', () => {
      const schema = defineFeatureFlags({
        debugMode: S.boolean().default(false).sync(),
      });

      const GeneratedClass = createEvaluatorClass(
        schema.schema,
        mockValidateFlagValue,
        mockGetTimestampNanos,
        MOCK_FF_ACCESS,
        MOCK_FF_USAGE,
      );

      const evaluator = new InMemoryFlagEvaluator({ debugMode: true });
      const state: GeneratedEvaluatorState<typeof schema.schema> = {
        schema: schema.schema,
        evaluationContext: { userId: 'user-123' },
        evaluator,
        buffer: null,

        accessedFlags: new Set(),
        flagCache: new Map(),
      };

      const instance = new GeneratedClass(state);
      const childInstance = instance.forContext({ requestId: 'req-456' });

      expect(childInstance.getContext().userId).toBe('user-123');
      expect(childInstance.getContext().requestId).toBe('req-456');
    });

    test('flag access is cached (deduplication)', () => {
      const schema = defineFeatureFlags({
        debugMode: S.boolean().default(false).sync(),
      });

      let evalCount = 0;
      const trackingEvaluator = {
        getSync: () => {
          evalCount++;
          return true;
        },
        getAsync: async () => true,
      };

      const GeneratedClass = createEvaluatorClass(
        schema.schema,
        mockValidateFlagValue,
        mockGetTimestampNanos,
        MOCK_FF_ACCESS,
        MOCK_FF_USAGE,
      );

      const state: GeneratedEvaluatorState<typeof schema.schema> = {
        schema: schema.schema,
        evaluationContext: { userId: 'test' },
        evaluator: trackingEvaluator,
        buffer: null,

        accessedFlags: new Set(),
        flagCache: new Map(),
      };

      const instance = new GeneratedClass(state);

      // Access flag multiple times
      (instance as unknown as { debugMode: BooleanFlagContext | undefined }).debugMode;
      (instance as unknown as { debugMode: BooleanFlagContext | undefined }).debugMode;
      (instance as unknown as { debugMode: BooleanFlagContext | undefined }).debugMode;

      // Should only evaluate once (cached)
      expect(evalCount).toBe(1);
    });

    test('async get method works correctly', async () => {
      const schema = defineFeatureFlags({
        asyncFlag: S.number().default(100).async(),
      });

      const GeneratedClass = createEvaluatorClass(
        schema.schema,
        mockValidateFlagValue,
        mockGetTimestampNanos,
        MOCK_FF_ACCESS,
        MOCK_FF_USAGE,
      );

      const evaluator = new InMemoryFlagEvaluator({ asyncFlag: 200 });
      const state: GeneratedEvaluatorState<typeof schema.schema> = {
        schema: schema.schema,
        evaluationContext: { userId: 'test' },
        evaluator,
        buffer: null,

        accessedFlags: new Set(),
        flagCache: new Map(),
      };

      const instance = new GeneratedClass(state);
      const result = (await instance.get('asyncFlag')) as { value: number; track: () => void };

      expect(result).toBeDefined();
      expect(result.value).toBe(200);
    });

    test('trackUsage logs to column writers', () => {
      const schema = defineFeatureFlags({
        debugMode: S.boolean().default(false).sync(),
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

      const GeneratedClass = createEvaluatorClass(
        schema.schema,
        mockValidateFlagValue,
        mockGetTimestampNanos,
        MOCK_FF_ACCESS,
        MOCK_FF_USAGE,
      );

      const evaluator = new InMemoryFlagEvaluator({ debugMode: true });
      const state: GeneratedEvaluatorState<typeof schema.schema> = {
        schema: schema.schema,
        evaluationContext: { userId: 'test' },
        evaluator,
        buffer: null,

        columnWriters: mockWriters,
        accessedFlags: new Set(),
        flagCache: new Map(),
      };

      const instance = new GeneratedClass(state);
      instance.trackUsage('debugMode', { action: 'test_action', outcome: 'success' });

      expect(logs).toContainEqual({ type: 'ff-usage' });
      expect(logs).toContainEqual({ name: 'debugMode' });
      expect(logs).toContainEqual({ action: 'test_action' });
      expect(logs).toContainEqual({ outcome: 'success' });
    });
  });

  describe('V8 optimization verification', () => {
    test('generated class has stable shape (all instances have same properties)', () => {
      const schema = defineFeatureFlags({
        flag1: S.boolean().default(false).sync(),
        flag2: S.number().default(0).sync(),
      });

      const GeneratedClass = createEvaluatorClass(
        schema.schema,
        mockValidateFlagValue,
        mockGetTimestampNanos,
        MOCK_FF_ACCESS,
        MOCK_FF_USAGE,
      );

      const evaluator = new InMemoryFlagEvaluator({});

      const createInstance = () => {
        const state: GeneratedEvaluatorState<typeof schema.schema> = {
          schema: schema.schema,
          evaluationContext: { userId: 'test' },
          evaluator,
          buffer: null,

          accessedFlags: new Set(),
          flagCache: new Map(),
        };
        return new GeneratedClass(state);
      };

      const instance1 = createInstance();
      const instance2 = createInstance();

      // Both instances should have the same property names (stable hidden class)
      const props1 = Object.getOwnPropertyNames(Object.getPrototypeOf(instance1));
      const props2 = Object.getOwnPropertyNames(Object.getPrototypeOf(instance2));

      expect(props1).toEqual(props2);

      // Should have getters as own properties on prototype
      expect(props1).toContain('flag1');
      expect(props1).toContain('flag2');
    });

    test('generated getters are real property descriptors (not proxy traps)', () => {
      const schema = defineFeatureFlags({
        testFlag: S.boolean().default(false).sync(),
      });

      const GeneratedClass = createEvaluatorClass(
        schema.schema,
        mockValidateFlagValue,
        mockGetTimestampNanos,
        MOCK_FF_ACCESS,
        MOCK_FF_USAGE,
      );

      // Check the prototype for getter descriptors
      const descriptor = Object.getOwnPropertyDescriptor(GeneratedClass.prototype, 'testFlag');

      expect(descriptor).toBeDefined();
      expect(typeof descriptor?.get).toBe('function');
      expect(descriptor?.set).toBeUndefined();
      expect(descriptor?.enumerable).toBe(false);
      expect(descriptor?.configurable).toBe(true);
    });

    test('no Proxy in generated class', () => {
      const schema = defineFeatureFlags({
        testFlag: S.boolean().default(false).sync(),
      });

      const code = generateEvaluatorClass(schema.schema);

      // Generated code should not contain Proxy
      expect(code).not.toContain('new Proxy');
      expect(code).not.toContain('Proxy(');
    });
  });
});

describe('generateEvaluatorClass snapshots', () => {
  test('snapshot: single boolean flag', () => {
    const schema = defineFeatureFlags({
      debugMode: S.boolean().default(false).sync(),
    });
    const code = generateEvaluatorClass(schema.schema);
    expect(code).toMatchSnapshot();
  });

  test('snapshot: multiple flags of different types', () => {
    const schema = defineFeatureFlags({
      debugMode: S.boolean().default(false).sync(),
      maxRetries: S.number().default(3).sync(),
      logLevel: S.enum(['debug', 'info', 'warn', 'error'] as const)
        .default('info')
        .sync(),
    });
    const code = generateEvaluatorClass(schema.schema);
    expect(code).toMatchSnapshot();
  });

  test('snapshot: async flags', () => {
    const schema = defineFeatureFlags({
      asyncFeature: S.boolean().default(false).async(),
      asyncLimit: S.number().default(100).async(),
    });
    const code = generateEvaluatorClass(schema.schema);
    expect(code).toMatchSnapshot();
  });

  test('snapshot: custom class name', () => {
    const schema = defineFeatureFlags({
      testFlag: S.boolean().default(false).sync(),
    });
    const code = generateEvaluatorClass(schema.schema, 'CustomFeatureFlagEvaluator');
    expect(code).toMatchSnapshot();
  });

  test('snapshot: many flags', () => {
    const schema = defineFeatureFlags({
      flag1: S.boolean().default(false).sync(),
      flag2: S.boolean().default(true).sync(),
      flag3: S.number().default(0).sync(),
      flag4: S.number().default(100).sync(),
      flag5: S.enum(['a', 'b', 'c'] as const)
        .default('a')
        .sync(),
    });
    const code = generateEvaluatorClass(schema.schema);
    expect(code).toMatchSnapshot();
  });
});
