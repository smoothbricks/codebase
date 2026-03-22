import { describe, expect, test } from 'bun:test';
import { createBuffer, createTestLogBinding, createTestSchema } from '../../__tests__/test-helpers.js';
import type { OpContext } from '../../opContext/types.js';
import { S } from '../../schema/builder.js';
import type { FeatureFlagSchema } from '../../schema/defineFeatureFlags.js';
import { defineFeatureFlags } from '../../schema/defineFeatureFlags.js';
import { InMemoryFlagEvaluator } from '../../schema/evaluator.js';
import { ENTRY_TYPE_FF_ACCESS, ENTRY_TYPE_FF_USAGE } from '../../schema/systemSchema.js';
import type { LogSchema } from '../../schema/types.js';
import { createSpanContextClass, type SpanContextInstance } from '../../spanContext.js';
import type { AnySpanBuffer, SpanBuffer } from '../../types.js';
import { createEvaluatorClass, generateEvaluatorClass } from '../evaluatorGenerator.js';
import { createTagWriter } from '../fixedPositionWriterGenerator.js';
import { createSpanLogger } from '../spanLoggerGenerator.js';

/**
 * Create a mock SpanContext for testing FeatureFlagEvaluator
 *
 * The evaluator needs:
 * - _buffer with scopeValues and writeIndex
 * - log with ffAccess, ffUsage, and _writeIndex
 * - buffer getter that returns _buffer
 *
 */
type EvaluatorTestContext<TLogSchema extends LogSchema, TFlags extends FeatureFlagSchema> = OpContext<
  TLogSchema,
  TFlags,
  Record<string, never>,
  Record<string, never>
>;

function createMockSpanContext<TLogSchema extends LogSchema, TFlags extends FeatureFlagSchema>(
  schema: TLogSchema,
  spanBuffer: SpanBuffer<TLogSchema>,
): SpanContextInstance<EvaluatorTestContext<TLogSchema, TFlags>> {
  const logger = createSpanLogger(schema, spanBuffer);
  const tagWriter = createTagWriter(schema, spanBuffer);
  const SpanContextClass = createSpanContextClass<EvaluatorTestContext<TLogSchema, TFlags>>(
    schema,
    createTestLogBinding(schema),
  );
  const ctx = new SpanContextClass(spanBuffer, schema, logger, tagWriter);
  ctx.deps = {};
  return ctx;
}

describe('EvaluatorGenerator', () => {
  describe('generateEvaluatorClass', () => {
    test('generates valid JavaScript class code', () => {
      const schema = defineFeatureFlags({
        debugMode: S.boolean().default(false).sync(),
        maxRetries: S.number().default(3).sync(),
      });

      const code = generateEvaluatorClass(schema.schema);

      expect(code).toContain('class GeneratedEvaluator extends WorkerSafeGeneratedEvaluator');
      expect(code).toContain('Object.defineProperties(GeneratedEvaluator.prototype');

      // Should describe the generated getters for each flag
      expect(code).toContain("return this.getFlag('debugMode')");
      expect(code).toContain("return this.getFlag('maxRetries')");

      // Should have new API methods (not old ones)
      expect(code).toContain('forContext(ctx)');
      expect(code).toContain('super(spanContext, evaluator, SCHEMA, validateFlagValue, ENTRY_TYPE_FF_ACCESS);');

      // Should NOT have old API methods
      expect(code).not.toContain('withBuffer(');
      expect(code).not.toContain('getContext()');
      expect(code).not.toContain('new Function');
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

      expect(code).toContain('featureA: {');
      expect(code).toContain('featureB: {');
      expect(code).toContain('featureC: {');
      expect(code).toContain('featureD: {');

      // Each getter should call getFlag with the flag name
      expect(code).toContain("this.getFlag('featureA')");
      expect(code).toContain("this.getFlag('featureB')");
      expect(code).toContain("this.getFlag('featureC')");
      expect(code).toContain("this.getFlag('featureD')");
    });
  });

  describe('createEvaluatorClass', () => {
    test('creates a functional class constructor', () => {
      const ffSchema = defineFeatureFlags({
        debugMode: S.boolean().default(false).sync(),
      });
      const logSchema = createTestSchema({});
      const spanBuffer = createBuffer(logSchema);
      type Ctx = EvaluatorTestContext<typeof logSchema, typeof ffSchema.schema>;
      const mockCtx = createMockSpanContext<typeof logSchema, typeof ffSchema.schema>(logSchema, spanBuffer);

      const GeneratedClass = createEvaluatorClass<Ctx>(
        ffSchema.schema,
        (value, _schema, def) => value ?? def,
        ENTRY_TYPE_FF_ACCESS,
      );

      expect(typeof GeneratedClass).toBe('function');

      const evaluator = new InMemoryFlagEvaluator<Ctx>(ffSchema.schema, { debugMode: true });
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
      const spanBuffer = createBuffer(logSchema);
      type Ctx = EvaluatorTestContext<typeof logSchema, typeof ffSchema.schema>;
      const mockCtx = createMockSpanContext<typeof logSchema, typeof ffSchema.schema>(logSchema, spanBuffer);

      const GeneratedClass = createEvaluatorClass<Ctx>(
        ffSchema.schema,
        (value, _schema, def) => value ?? def,
        ENTRY_TYPE_FF_ACCESS,
      );

      const evaluator = new InMemoryFlagEvaluator<Ctx>(ffSchema.schema, { debugMode: true });
      const instance = new GeneratedClass(mockCtx, evaluator);

      // Access the flag via generated getter
      const flag = Reflect.get(instance, 'debugMode');

      expect(flag).toBeDefined();
      expect(flag?.value).toBe(true);
      expect(typeof flag?.track).toBe('function');
    });

    test('flag getters return undefined for falsy values', () => {
      const ffSchema = defineFeatureFlags({
        debugMode: S.boolean().default(false).sync(),
      });
      const logSchema = createTestSchema({});
      const spanBuffer = createBuffer(logSchema);
      type Ctx = EvaluatorTestContext<typeof logSchema, typeof ffSchema.schema>;
      const mockCtx = createMockSpanContext<typeof logSchema, typeof ffSchema.schema>(logSchema, spanBuffer);

      const GeneratedClass = createEvaluatorClass<Ctx>(
        ffSchema.schema,
        (value, _schema, def) => value ?? def,
        ENTRY_TYPE_FF_ACCESS,
      );

      const evaluator = new InMemoryFlagEvaluator<Ctx>(ffSchema.schema, { debugMode: false });
      const instance = new GeneratedClass(mockCtx, evaluator);

      const flag = Reflect.get(instance, 'debugMode');
      expect(flag).toBeUndefined();
    });

    test('forContext creates new instance bound to new SpanContext', () => {
      const ffSchema = defineFeatureFlags({
        debugMode: S.boolean().default(false).sync(),
      });
      const logSchema = createTestSchema({});
      const spanBuffer = createBuffer(logSchema);
      type Ctx = EvaluatorTestContext<typeof logSchema, typeof ffSchema.schema>;
      const mockCtx = createMockSpanContext<typeof logSchema, typeof ffSchema.schema>(logSchema, spanBuffer);

      const GeneratedClass = createEvaluatorClass<Ctx>(
        ffSchema.schema,
        (value, _schema, def) => value ?? def,
        ENTRY_TYPE_FF_ACCESS,
      );

      const evaluator = new InMemoryFlagEvaluator<Ctx>(ffSchema.schema, { debugMode: true });
      const instance = new GeneratedClass(mockCtx, evaluator);

      // Create a new SpanContext for child span
      const childBuffer = createBuffer(logSchema);
      const childCtx = createMockSpanContext<typeof logSchema, typeof ffSchema.schema>(logSchema, childBuffer);

      const childInstance = instance.forContext(childCtx);

      // Child should have flag accessible
      const childFlag = Reflect.get(childInstance, 'debugMode');
      expect(childFlag?.value).toBe(true);
    });

    test('flag access is deduplicated via buffer scan', () => {
      const ffSchema = defineFeatureFlags({
        debugMode: S.boolean().default(false).sync(),
      });
      const logSchema = createTestSchema({});
      const spanBuffer = createBuffer(logSchema);
      type Ctx = EvaluatorTestContext<typeof logSchema, typeof ffSchema.schema>;
      const mockCtx = createMockSpanContext<typeof logSchema, typeof ffSchema.schema>(logSchema, spanBuffer);

      let evalCount = 0;
      const trackingEvaluator = new InMemoryFlagEvaluator<Ctx>(ffSchema.schema, { debugMode: true });
      const baseGetSync = trackingEvaluator.getSync.bind(trackingEvaluator);
      trackingEvaluator.getSync = (ctx, flag) => {
        evalCount++;
        return baseGetSync(ctx, flag);
      };

      const GeneratedClass = createEvaluatorClass<Ctx>(
        ffSchema.schema,
        (value, _schema, def) => value ?? def,
        ENTRY_TYPE_FF_ACCESS,
      );

      const instance = new GeneratedClass(mockCtx, trackingEvaluator);

      // Access flag multiple times
      Reflect.get(instance, 'debugMode');
      Reflect.get(instance, 'debugMode');
      Reflect.get(instance, 'debugMode');

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
      const spanBuffer = createBuffer(logSchema);
      type Ctx = EvaluatorTestContext<typeof logSchema, typeof ffSchema.schema>;
      const mockCtx = createMockSpanContext<typeof logSchema, typeof ffSchema.schema>(logSchema, spanBuffer);

      const GeneratedClass = createEvaluatorClass<Ctx>(
        ffSchema.schema,
        (value, _schema, def) => value ?? def,
        ENTRY_TYPE_FF_ACCESS,
      );

      const evaluator = new InMemoryFlagEvaluator<Ctx>(ffSchema.schema, { asyncFlag: 200 });
      const instance = new GeneratedClass(mockCtx, evaluator);
      const result = await instance.get('asyncFlag');

      expect(result).toBeDefined();
      if (!result) {
        throw new Error('Expected asyncFlag to produce a flag context');
      }
      expect(result.value).toBe(200);
    });

    test('track() logs ff-usage entry via logger', () => {
      const ffSchema = defineFeatureFlags({
        debugMode: S.boolean().default(false).sync(),
      });
      const logSchema = createTestSchema({
        action: S.category(),
        outcome: S.category(),
      });
      const spanBuffer = createBuffer(logSchema);
      type Ctx = EvaluatorTestContext<typeof logSchema, typeof ffSchema.schema>;
      const mockCtx = createMockSpanContext<typeof logSchema, typeof ffSchema.schema>(logSchema, spanBuffer);

      const GeneratedClass = createEvaluatorClass<Ctx>(
        ffSchema.schema,
        (value, _schema, def) => value ?? def,
        ENTRY_TYPE_FF_ACCESS,
      );

      const evaluator = new InMemoryFlagEvaluator<Ctx>(ffSchema.schema, { debugMode: true });
      const instance = new GeneratedClass(mockCtx, evaluator);

      const flag = Reflect.get(instance, 'debugMode');
      flag?.track({ action: 'test_action', outcome: 'success' });

      // Check buffer for ff-usage entry
      const usageCount = countEntryType(spanBuffer, ENTRY_TYPE_FF_USAGE);
      expect(usageCount).toBe(1);

      // Context should be applied via with(context)
      let usageIdx = -1;
      for (let i = 0; i < spanBuffer._writeIndex; i++) {
        if (spanBuffer.entry_type[i] === ENTRY_TYPE_FF_USAGE) {
          usageIdx = i;
          break;
        }
      }
      expect(usageIdx).toBeGreaterThanOrEqual(0);
      expect(spanBuffer.action_values?.[usageIdx]).toBe('test_action');
      expect(spanBuffer.outcome_values?.[usageIdx]).toBe('success');
    });
  });

  describe('V8 optimization verification', () => {
    test('generated class has stable shape (all instances have same properties)', () => {
      const schema = defineFeatureFlags({
        flag1: S.boolean().default(false).sync(),
        flag2: S.number().default(0).sync(),
      });
      const logSchema = createTestSchema({});
      type Ctx = EvaluatorTestContext<typeof logSchema, typeof schema.schema>;

      const GeneratedClass = createEvaluatorClass<Ctx>(
        schema.schema,
        (value, _s, def) => value ?? def,
        ENTRY_TYPE_FF_ACCESS,
      );

      const mockEvaluator = new InMemoryFlagEvaluator<Ctx>(schema.schema, {});

      const createInstance = () => {
        const spanBuffer = createBuffer(logSchema);
        const mockCtx = createMockSpanContext<typeof logSchema, typeof schema.schema>(logSchema, spanBuffer);
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

    test('runtime path stays worker-safe', () => {
      const schema = defineFeatureFlags({
        testFlag: S.boolean().default(false).sync(),
      });

      const GeneratedClass = createEvaluatorClass(
        schema.schema,
        (value, _schema, def) => value ?? def,
        ENTRY_TYPE_FF_ACCESS,
      );

      expect(() => GeneratedClass).not.toThrow();
      expect(GeneratedClass.toString()).not.toContain('new Function');
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
