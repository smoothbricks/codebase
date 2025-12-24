import { describe, expect, test } from 'bun:test';
import { createTestSchema, createTestSpanBuffer } from '../../__tests__/test-helpers.js';
import { S } from '../../schema/builder.js';
import { defineFeatureFlags } from '../../schema/defineFeatureFlags.js';
import { type BooleanFlagContext, InMemoryFlagEvaluator } from '../../schema/evaluator.js';
import { ENTRY_TYPE_FF_ACCESS, ENTRY_TYPE_FF_USAGE } from '../../schema/systemSchema.js';
import type { LogSchema } from '../../schema/types.js';
import type { AnySpanBuffer, SpanBuffer } from '../../types.js';
import { createEvaluatorClass, generateEvaluatorClass } from '../evaluatorGenerator.js';
import { createSpanLogger } from '../spanLoggerGenerator.js';

/**
 * Create a mock SpanContext for testing FeatureFlagEvaluator
 *
 * The evaluator needs:
 * - _buffer with scopeValues and writeIndex
 * - log with ffAccess, ffUsage, and _writeIndex
 * - buffer getter that returns _buffer
 *
 * Note: Uses explicit 'any' cast since this is a test utility and the actual
 * SpanContext type requires OpContext (not just LogSchema). The runtime
 * structure is what matters for these tests.
 */
// biome-ignore lint/suspicious/noExplicitAny: Test utility - mock context for evaluator testing
function createMockSpanContext<T extends LogSchema>(spanBuffer: SpanBuffer<T>): any {
  // Create a real SpanLogger for the buffer using the schema from module
  const schema = spanBuffer._logBinding.logSchema as T;
  const logger = createSpanLogger(schema, spanBuffer);

  // Mock SpanContext with the essential properties
  return {
    _buffer: spanBuffer,
    get buffer() {
      return spanBuffer;
    },
    log: logger,
    // Other properties not needed by evaluator tests
    ff: null,
    env: {},
    deps: {},
    tag: {},
    scope: spanBuffer._scopeValues || {},
    setScope: (attrs: Record<string, unknown>) => {
      if (!spanBuffer._scopeValues) {
        spanBuffer._scopeValues = {};
      }
      Object.assign(spanBuffer._scopeValues, attrs);
    },
    ok: () => ({ success: true, value: undefined }),
    err: () => ({ success: false, error: 'error' }),
    span: () => Promise.resolve(undefined),
    span_op: () => Promise.resolve(undefined),
    span_fn: () => Promise.resolve(undefined),
  };
}

describe('EvaluatorGenerator', () => {
  describe('generateEvaluatorClass', () => {
    test('generates valid JavaScript class code', () => {
      const schema = defineFeatureFlags({
        debugMode: S.boolean().default(false).sync(),
        maxRetries: S.number().default(3).sync(),
      });

      const code = generateEvaluatorClass(schema.schema);

      // Should be a valid IIFE wrapper
      expect(code).toContain('(function(validateFlagValue, ENTRY_TYPE_FF_ACCESS, SCHEMA)');
      expect(code).toContain("'use strict'");
      expect(code).toContain('class GeneratedEvaluator');

      // Should have getters for each flag
      expect(code).toContain('get debugMode()');
      expect(code).toContain('get maxRetries()');

      // Should have new API methods (not old ones)
      expect(code).toContain('forContext(ctx)');
      expect(code).toContain('async get(flag)');
      expect(code).toContain('trackUsage(flag, context)');

      // Should NOT have old API methods
      expect(code).not.toContain('withBuffer(');
      expect(code).not.toContain('getContext()');
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
      const ffSchema = defineFeatureFlags({
        debugMode: S.boolean().default(false).sync(),
      });
      const logSchema = createTestSchema({});
      const { spanBuffer } = createTestSpanBuffer(logSchema, { spanName: 'test-span' });
      const mockCtx = createMockSpanContext(spanBuffer);

      const GeneratedClass = createEvaluatorClass(
        ffSchema.schema,
        (value, _schema, def) => value ?? def,
        ENTRY_TYPE_FF_ACCESS,
      );

      expect(typeof GeneratedClass).toBe('function');

      const evaluator = new InMemoryFlagEvaluator(ffSchema.schema, { debugMode: true });
      const instance = new GeneratedClass(mockCtx, evaluator);
      expect(instance).toBeDefined();
    });

    test('caches generated classes by schema', () => {
      const schema = defineFeatureFlags({
        testFlag: S.boolean().default(false).sync(),
      });

      const Class1 = createEvaluatorClass(schema.schema, (value, _schema, def) => value ?? def, ENTRY_TYPE_FF_ACCESS);

      const Class2 = createEvaluatorClass(schema.schema, (value, _schema, def) => value ?? def, ENTRY_TYPE_FF_ACCESS);

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

      const Class1 = createEvaluatorClass(schema1.schema, (value, _schema, def) => value ?? def, ENTRY_TYPE_FF_ACCESS);

      const Class2 = createEvaluatorClass(schema2.schema, (value, _schema, def) => value ?? def, ENTRY_TYPE_FF_ACCESS);

      // Different schemas should generate different classes
      expect(Class1).not.toBe(Class2);
    });
  });

  describe('Generated class functionality', () => {
    test('flag getters return FlagContext for truthy values', () => {
      const ffSchema = defineFeatureFlags({
        debugMode: S.boolean().default(false).sync(),
      });
      const logSchema = createTestSchema({});
      const { spanBuffer } = createTestSpanBuffer(logSchema, { spanName: 'test-span' });
      const mockCtx = createMockSpanContext(spanBuffer);

      const GeneratedClass = createEvaluatorClass(
        ffSchema.schema,
        (value, _schema, def) => value ?? def,
        ENTRY_TYPE_FF_ACCESS,
      );

      const evaluator = new InMemoryFlagEvaluator(ffSchema.schema, { debugMode: true });
      const instance = new GeneratedClass(mockCtx, evaluator);

      // Access the flag via generated getter
      const flag = (instance as unknown as { debugMode: BooleanFlagContext | undefined }).debugMode;

      expect(flag).toBeDefined();
      expect(flag?.value).toBe(true);
      expect(typeof flag?.track).toBe('function');
    });

    test('flag getters return undefined for falsy values', () => {
      const ffSchema = defineFeatureFlags({
        debugMode: S.boolean().default(false).sync(),
      });
      const logSchema = createTestSchema({});
      const { spanBuffer } = createTestSpanBuffer(logSchema, { spanName: 'test-span' });
      const mockCtx = createMockSpanContext(spanBuffer);

      const GeneratedClass = createEvaluatorClass(
        ffSchema.schema,
        (value, _schema, def) => value ?? def,
        ENTRY_TYPE_FF_ACCESS,
      );

      const evaluator = new InMemoryFlagEvaluator(ffSchema.schema, { debugMode: false });
      const instance = new GeneratedClass(mockCtx, evaluator);

      const flag = (instance as unknown as { debugMode: BooleanFlagContext | undefined }).debugMode;
      expect(flag).toBeUndefined();
    });

    test('forContext creates new instance bound to new SpanContext', () => {
      const ffSchema = defineFeatureFlags({
        debugMode: S.boolean().default(false).sync(),
      });
      const logSchema = createTestSchema({});
      const { spanBuffer } = createTestSpanBuffer(logSchema, { spanName: 'test-span' });
      const mockCtx = createMockSpanContext(spanBuffer);

      const GeneratedClass = createEvaluatorClass(
        ffSchema.schema,
        (value, _schema, def) => value ?? def,
        ENTRY_TYPE_FF_ACCESS,
      );

      const evaluator = new InMemoryFlagEvaluator(ffSchema.schema, { debugMode: true });
      const instance = new GeneratedClass(mockCtx, evaluator);

      // Create a new SpanContext for child span
      const { spanBuffer: childBuffer } = createTestSpanBuffer(logSchema, { spanName: 'child-span' });
      const childCtx = createMockSpanContext(childBuffer);

      const childInstance = instance.forContext(childCtx);

      // Child should have flag accessible
      const childFlag = (childInstance as unknown as { debugMode: BooleanFlagContext | undefined }).debugMode;
      expect(childFlag?.value).toBe(true);
    });

    test('flag access is deduplicated via buffer scan', () => {
      const ffSchema = defineFeatureFlags({
        debugMode: S.boolean().default(false).sync(),
      });
      const logSchema = createTestSchema({});
      const { spanBuffer } = createTestSpanBuffer(logSchema, { spanName: 'test-span' });
      const mockCtx = createMockSpanContext(spanBuffer);

      let evalCount = 0;
      const trackingEvaluator = {
        getSync: (_ctx: unknown, _flag: string) => {
          evalCount++;
          return true;
        },
        getAsync: async (_ctx: unknown, _flag: string) => true,
        forContext: () => trackingEvaluator,
      };

      const GeneratedClass = createEvaluatorClass(
        ffSchema.schema,
        (value, _schema, def) => value ?? def,
        ENTRY_TYPE_FF_ACCESS,
      );

      const instance = new GeneratedClass(mockCtx, trackingEvaluator as any);

      // Access flag multiple times
      (instance as unknown as { debugMode: BooleanFlagContext | undefined }).debugMode;
      (instance as unknown as { debugMode: BooleanFlagContext | undefined }).debugMode;
      (instance as unknown as { debugMode: BooleanFlagContext | undefined }).debugMode;

      // Evaluator should be called each time (no local cache)
      expect(evalCount).toBe(3);

      // But ff-access should only be logged once (buffer deduplication)
      const accessCount = countEntryType(spanBuffer, ENTRY_TYPE_FF_ACCESS);
      expect(accessCount).toBe(1);
    });

    test('async get method works correctly', async () => {
      const ffSchema = defineFeatureFlags({
        asyncFlag: S.number().default(100).async(),
      });
      const logSchema = createTestSchema({});
      const { spanBuffer } = createTestSpanBuffer(logSchema, { spanName: 'test-span' });
      const mockCtx = createMockSpanContext(spanBuffer);

      const GeneratedClass = createEvaluatorClass(
        ffSchema.schema,
        (value, _schema, def) => value ?? def,
        ENTRY_TYPE_FF_ACCESS,
      );

      const evaluator = new InMemoryFlagEvaluator(ffSchema.schema, { asyncFlag: 200 });
      const instance = new GeneratedClass(mockCtx, evaluator);
      const result = (await instance.get('asyncFlag')) as { value: number; track: () => void };

      expect(result).toBeDefined();
      expect(result.value).toBe(200);
    });

    test('trackUsage logs ff-usage entry via logger', () => {
      const ffSchema = defineFeatureFlags({
        debugMode: S.boolean().default(false).sync(),
      });
      const logSchema = createTestSchema({});
      const { spanBuffer } = createTestSpanBuffer(logSchema, { spanName: 'test-span' });
      const mockCtx = createMockSpanContext(spanBuffer);

      const GeneratedClass = createEvaluatorClass(
        ffSchema.schema,
        (value, _schema, def) => value ?? def,
        ENTRY_TYPE_FF_ACCESS,
      );

      const evaluator = new InMemoryFlagEvaluator(ffSchema.schema, { debugMode: true });
      const instance = new GeneratedClass(mockCtx, evaluator);

      // Cast needed because createEvaluatorClass returns generic OpContext-typed class
      // The actual runtime instance is correctly typed, but TypeScript can't infer it
      // biome-ignore lint/suspicious/noExplicitAny: Test-specific type workaround
      (instance as any).trackUsage('debugMode', { action: 'test_action', outcome: 'success' });

      // Check buffer for ff-usage entry
      const usageCount = countEntryType(spanBuffer, ENTRY_TYPE_FF_USAGE);
      expect(usageCount).toBe(1);
    });
  });

  describe('V8 optimization verification', () => {
    test('generated class has stable shape (all instances have same properties)', () => {
      const schema = defineFeatureFlags({
        flag1: S.boolean().default(false).sync(),
        flag2: S.number().default(0).sync(),
      });
      const logSchema = createTestSchema({});

      const GeneratedClass = createEvaluatorClass(
        schema.schema,
        (value, _s, def) => value ?? def,
        ENTRY_TYPE_FF_ACCESS,
      );

      const mockEvaluator = new InMemoryFlagEvaluator(schema.schema, {});

      const createInstance = () => {
        const { spanBuffer } = createTestSpanBuffer(logSchema, { spanName: 'test-span' });
        const mockCtx = createMockSpanContext(spanBuffer);
        return new GeneratedClass(mockCtx, mockEvaluator);
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
        (value, _s, def) => value ?? def,
        ENTRY_TYPE_FF_ACCESS,
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
