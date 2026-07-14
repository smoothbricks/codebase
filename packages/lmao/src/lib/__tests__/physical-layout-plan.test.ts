import { describe, expect, it } from 'bun:test';
import { getResultWriterClass, getTagWriterClass } from '../codegen/fixedPositionWriterGenerator.js';
import { createSpanLoggerClass } from '../codegen/spanLoggerGenerator.js';
import { defineOpContext } from '../defineOpContext.js';
import { createRemapDescriptor } from '../library.js';
import type { OpContext } from '../opContext/types.js';
import { getPhysicalLayoutPlan, PHYSICAL_LAYOUT_VERSION } from '../physicalLayoutPlan.js';
import { resolveMessage } from '../resolveMessage.js';
import {
  RUNTIME_HINT_ANALYZED_VALID,
  RUNTIME_HINT_LOG,
  RUNTIME_HINT_MESSAGE_LAYOUT_STATIC_ONLY,
  RUNTIME_HINT_RESULT,
  RUNTIME_HINT_TAG,
} from '../runtimeHint.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { getSpanBufferClass } from '../spanBuffer.js';
import { createSpanContextClass, isPhysicalLayoutPlanForContext } from '../spanContext.js';
import { TestTracer } from '../tracers/TestTracer.js';
import { iterateSpanChildren } from '../traceTopology.js';
import { createTestTracerOptions } from './test-helpers.js';

const CAPACITY_TIER = 17;
const HOT_CHILD_CAPABILITIES = RUNTIME_HINT_TAG | RUNTIME_HINT_LOG | RUNTIME_HINT_RESULT;
const HOT_CHILD_HINT =
  RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_TAG | RUNTIME_HINT_LOG | RUNTIME_HINT_RESULT | CAPACITY_TIER;

describe('PhysicalLayoutPlan', () => {
  it('reuses one immutable plan per schema, capability set, context layout, remap, and backend', () => {
    const schema = defineLogSchema({ userId: S.category() });
    const otherSchema = defineLogSchema({ userId: S.category(), duration: S.number() });
    const SpanBufferClass = getSpanBufferClass(schema);
    const logBinding = { logSchema: schema, remapDescriptor: undefined };
    const SpanContextClass = createSpanContextClass<OpContext<typeof schema>>(
      schema,
      logBinding,
      HOT_CHILD_CAPABILITIES,
    );
    const differentCapabilityContextClass = createSpanContextClass<OpContext<typeof schema>>(
      schema,
      logBinding,
      HOT_CHILD_CAPABILITIES ^ RUNTIME_HINT_LOG,
    );
    const differentLayoutContextClass = createSpanContextClass<OpContext<typeof schema>>(
      schema,
      logBinding,
      HOT_CHILD_CAPABILITIES,
      ['requestId'],
    );
    const otherSchemaContextClass = createSpanContextClass<OpContext<typeof otherSchema>>(
      otherSchema,
      { logSchema: otherSchema, remapDescriptor: undefined },
      HOT_CHILD_CAPABILITIES,
    );
    const layout = createRemapDescriptor(schema, { app_user_id: 'userId' });
    const otherLayout = createRemapDescriptor(schema, { lib_user_id: 'userId' });

    const first = getPhysicalLayoutPlan(SpanBufferClass, HOT_CHILD_HINT, SpanContextClass, layout, 'js-heap');
    const identical = getPhysicalLayoutPlan(SpanBufferClass, HOT_CHILD_HINT, SpanContextClass, layout, 'js-heap');
    const differentCapabilities = getPhysicalLayoutPlan(
      SpanBufferClass,
      HOT_CHILD_HINT ^ RUNTIME_HINT_LOG,
      differentCapabilityContextClass,
      layout,
      'js-heap',
    );
    const differentBackend = getPhysicalLayoutPlan(SpanBufferClass, HOT_CHILD_HINT, SpanContextClass, layout, 'wasm');
    const differentRemap = getPhysicalLayoutPlan(
      SpanBufferClass,
      HOT_CHILD_HINT,
      SpanContextClass,
      otherLayout,
      'js-heap',
    );
    const differentContextLayout = getPhysicalLayoutPlan(
      SpanBufferClass,
      HOT_CHILD_HINT,
      differentLayoutContextClass,
      layout,
      'js-heap',
      'requestId',
    );
    const differentSchema = getPhysicalLayoutPlan(
      getSpanBufferClass(otherSchema),
      HOT_CHILD_HINT,
      otherSchemaContextClass,
      undefined,
      'js-heap',
    );

    expect(identical).toBe(first);
    expect(Object.isFrozen(first)).toBe(true);
    expect(first.SpanContextClass).toBe(SpanContextClass);
    expect(first.remapDescriptor).toBe(layout);
    expect(differentCapabilities).not.toBe(first);
    expect(differentBackend).not.toBe(first);
    expect(differentRemap).not.toBe(first);
    expect(differentContextLayout).not.toBe(first);
    expect(differentSchema).not.toBe(first);
    expect(differentCapabilities.capabilities).toBe(first.capabilities ^ RUNTIME_HINT_LOG);
    expect(differentCapabilities.SpanContextClass).toBe(differentCapabilityContextClass);
    expect(differentBackend.backendKind).toBe('wasm');
    expect(differentRemap.remapDescriptor).toBe(otherLayout);
    expect(differentContextLayout.contextLayoutKey).toBe('requestId');
    expect(differentContextLayout.SpanContextClass).toBe(differentLayoutContextClass);
    expect(differentSchema.schema).toBe(otherSchema);
  });

  it('owns the schema-specific constructors, capacity, and appenders', () => {
    const schema = defineLogSchema({ userId: S.category() });
    const SpanBufferClass = getSpanBufferClass(schema);
    const SpanContextClass = createSpanContextClass<OpContext<typeof schema>>(
      schema,
      { logSchema: schema, remapDescriptor: undefined },
      HOT_CHILD_CAPABILITIES,
    );
    const plan = getPhysicalLayoutPlan(SpanBufferClass, HOT_CHILD_HINT, SpanContextClass);
    const reusedPlan = getPhysicalLayoutPlan(SpanBufferClass, HOT_CHILD_HINT, SpanContextClass);

    expect(plan.version).toBe(PHYSICAL_LAYOUT_VERSION);
    expect(plan.schema).toBe(schema);
    expect(plan.SpanContextClass).toBe(SpanContextClass);
    expect(plan.SpanBufferClass).toBe(SpanBufferClass);
    expect(plan.SpanLoggerClass).toBe(createSpanLoggerClass(schema));
    expect(plan.TagWriterClass).toBe(getTagWriterClass(schema));
    expect(plan.ResultWriterClass).toBe(getResultWriterClass(schema));
    expect(plan.capacityTier).toBe(CAPACITY_TIER);
    expect(plan.clock.kind).toBe('trace-root');
    expect(plan.poolRef).toBeNull();
    expect(reusedPlan.clock).toBe(plan.clock);
    expect(reusedPlan.appenders).toBe(plan.appenders);
    expect(Object.isFrozen(plan.clock)).toBe(true);
    expect(Object.isFrozen(plan.appenders)).toBe(true);
  });

  it('reuses matching SpanBuffer metadata and materializes one constructor for a mismatch', () => {
    const schema = defineLogSchema({ userId: S.category() });
    const opContext = defineOpContext({ logSchema: schema });
    const incomingClass = getSpanBufferClass(opContext.logBinding.logSchema);
    const matchingHint = RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_RESULT;
    const mismatchedHint = matchingHint | RUNTIME_HINT_MESSAGE_LAYOUT_STATIC_ONLY;

    const originalDescriptor = Object.getOwnPropertyDescriptor(globalThis, 'Function');
    if (!originalDescriptor) throw new Error('Expected global Function descriptor');
    const originalFunction = globalThis.Function;
    let functionCalls = 0;
    const functionProbe = new Proxy(originalFunction, {
      apply(target, thisArgument, argumentList) {
        functionCalls++;
        return Reflect.apply(target, thisArgument, argumentList);
      },
      construct(target, argumentList, newTarget) {
        functionCalls++;
        return Reflect.construct(target, argumentList, newTarget);
      },
    });
    Object.defineProperty(globalThis, 'Function', { ...originalDescriptor, value: functionProbe });
    try {
      const matching = opContext.defineOp('matching-layout', (ctx) => ctx.ok('matched'), undefined, {
        runtimeHint: matchingHint,
      });
      expect(matching.callsitePlan.SpanBufferClass).toBe(incomingClass);
      expect(functionCalls).toBe(0);

      const mismatched = opContext.defineOp('mismatched-layout', (ctx) => ctx.ok('generated'), undefined, {
        runtimeHint: mismatchedHint,
      });
      const selectedClass = mismatched.callsitePlan.SpanBufferClass;
      expect(selectedClass).not.toBe(incomingClass);
      expect(selectedClass.messageLayoutFamily).toBe('static-only');
      expect(functionCalls).toBe(1);

      const reused = opContext.defineOp('reused-layout', (ctx) => ctx.ok('reused'), undefined, {
        runtimeHint: mismatchedHint,
      });
      expect(reused.callsitePlan.SpanBufferClass).toBe(selectedClass);
      expect(functionCalls).toBe(1);
    } finally {
      Object.defineProperty(globalThis, 'Function', originalDescriptor);
    }
  });

  it('consumes the same plan for repeated hot child creation without mutating it', async () => {
    const schema = defineLogSchema({ userId: S.category() });
    const opContext = defineOpContext({ logSchema: schema });
    const writtenUsers: unknown[] = [];
    const writtenMessages: unknown[] = [];

    const childOp = opContext.defineOp(
      'planned-child',
      (ctx) => {
        ctx.tag.userId('planned-user');
        ctx.log.info('planned-log');
        writtenUsers.push(ctx.buffer.userId_values[0]);
        writtenMessages.push(resolveMessage(ctx.buffer, 2));
        return ctx.ok('done');
      },
      undefined,
      { runtimeHint: HOT_CHILD_HINT },
    );
    const plan = childOp.callsitePlan;
    const childSchema = plan.SpanBufferClass.schema;
    if (!isPhysicalLayoutPlanForContext<OpContext<typeof childSchema>>(plan, childSchema)) {
      throw new TypeError('Expected child Op to own a schema-specific physical layout plan');
    }
    const parentOp = opContext.defineOp('parent', async (ctx) => {
      await ctx.span('first-child', childOp);
      await ctx.span('second-child', childOp);
      return ctx.ok('done');
    });

    const ownedLogger = plan.SpanLoggerClass;
    const ownedTagWriter = plan.TagWriterClass;
    const ownedAppenders = plan.appenders;
    const tracer = new TestTracer(opContext, createTestTracerOptions());

    await tracer.trace('root', parentOp);

    const children = Array.from(iterateSpanChildren(tracer.rootBuffers[0]));
    expect(children).toHaveLength(2);
    expect(children.every((buffer) => buffer instanceof plan.SpanBufferClass)).toBe(true);
    expect(children.map((buffer) => buffer._capacity)).toEqual([CAPACITY_TIER, CAPACITY_TIER]);
    const clockBuffer = children[0];
    const beforePlanClock = clockBuffer._traceRoot.getTimestampNanos();
    const planTimestamp = plan.clock.now(clockBuffer);
    const afterPlanClock = clockBuffer._traceRoot.getTimestampNanos();
    expect(planTimestamp >= beforePlanClock && planTimestamp <= afterPlanClock).toBe(true);
    expect(writtenUsers).toEqual(['planned-user', 'planned-user']);
    expect(writtenMessages).toEqual(['planned-log', 'planned-log']);
    expect(plan.SpanLoggerClass).toBe(ownedLogger);
    expect(plan.TagWriterClass).toBe(ownedTagWriter);
    expect(plan.appenders).toBe(ownedAppenders);
  });
  it('reuses one monomorphic CallsitePlan across sync and async calls while separating capability and schema plans', async () => {
    const schema = defineLogSchema({ userId: S.category() });
    const opContext = defineOpContext({ logSchema: schema, ctx: { requestId: 'default-request' } });
    const resultContexts: object[] = [];
    const resultOnly = opContext.defineOp(
      'result-only-callsite',
      (ctx) => {
        resultContexts.push(ctx);
        return ctx.ok(resultContexts.length);
      },
      undefined,
      { runtimeHint: RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_RESULT | 4 },
    );
    const logResult = opContext.defineOp(
      'log-result-callsite',
      (ctx) => {
        ctx.log.info('planned-log');
        return ctx.ok('logged');
      },
      undefined,
      { runtimeHint: RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_LOG | RUNTIME_HINT_RESULT | 4 },
    );
    const tagResult = opContext.defineOp(
      'tag-result-callsite',
      (ctx) => {
        ctx.tag.userId('planned-user');
        return ctx.ok('tagged');
      },
      undefined,
      { runtimeHint: RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_TAG | RUNTIME_HINT_RESULT | 4 },
    );
    const otherSchema = defineLogSchema({ duration: S.number() });
    const otherContext = defineOpContext({ logSchema: otherSchema });
    const otherSchemaOp = otherContext.defineOp('other-schema-callsite', (ctx) => ctx.ok('other'), undefined, {
      runtimeHint: RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_RESULT | 4,
    });

    const plan = resultOnly.callsitePlan;
    expect(Object.isFrozen(plan)).toBe(true);
    expect(plan.schema).toBe(opContext.logBinding.logSchema);
    expect(plan.metadata.name).toBe('result-only-callsite');
    expect(plan.metadata).toBe(resultOnly.metadata);
    expect(plan.runtimeHint).toBe(RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_RESULT | 4);
    expect(plan.newSpanLogger).toBeUndefined();
    expect(plan.SpanLoggerClass).toBeUndefined();
    expect(plan.newTagWriter).toBeUndefined();
    expect(typeof plan.newCtx0).toBe('function');
    expect(typeof plan.newCtx1).toBe('function');
    const inherited = { requestId: 'default-request' };
    const noOverrides = plan.newCtx0(inherited);
    const withOverrides = plan.newCtx1(inherited, { requestId: 'override-request' });
    expect(noOverrides).toBe(inherited);
    expect(withOverrides).not.toBe(inherited);
    expect(Object.getPrototypeOf(withOverrides)).toBe(Object.prototype);
    expect(Reflect.ownKeys(withOverrides)).toEqual(['requestId']);
    expect(Reflect.get(withOverrides, 'requestId')).toBe('override-request');

    expect(logResult.callsitePlan).not.toBe(plan);
    expect(logResult.callsitePlan.SpanContextClass).not.toBe(plan.SpanContextClass);
    expect(logResult.callsitePlan.SpanBufferClass).toBe(plan.SpanBufferClass);
    expect(typeof logResult.callsitePlan.newSpanLogger).toBe('function');
    expect(logResult.callsitePlan.SpanLoggerClass).toBeDefined();
    expect(logResult.callsitePlan.newTagWriter).toBeUndefined();
    expect(tagResult.callsitePlan).not.toBe(plan);
    expect(tagResult.callsitePlan.SpanBufferClass).toBe(plan.SpanBufferClass);
    expect(tagResult.callsitePlan.newSpanLogger).toBeUndefined();
    expect(typeof tagResult.callsitePlan.newTagWriter).toBe('function');
    expect(otherSchemaOp.callsitePlan).not.toBe(plan);
    expect(otherSchemaOp.callsitePlan.SpanBufferClass).not.toBe(plan.SpanBufferClass);

    const parent = opContext.defineOp('callsite-parent', async (ctx) => {
      const syncResult = ctx.spanSync('sync-result', resultOnly);
      const asyncResult = await ctx.span('async-result', resultOnly);
      if (!syncResult.success || !asyncResult.success) throw new Error('expected repeated child calls to succeed');
      return ctx.ok([syncResult.value, asyncResult.value]);
    });
    const tracer = new TestTracer(opContext, createTestTracerOptions());
    const result = await tracer.trace('callsite-root', parent);

    expect(result.success).toBe(true);
    if (!result.success) throw new Error('expected parent result to succeed');
    expect(result.value).toEqual([1, 2]);
    expect(resultOnly.callsitePlan).toBe(plan);
    expect(resultContexts).toHaveLength(2);
    expect(resultContexts.every((ctx) => ctx instanceof plan.SpanContextClass)).toBe(true);
    const rootBuffer = tracer.rootBuffers[0];
    if (!rootBuffer) throw new Error('expected repeated child buffers');
    const children = Array.from(iterateSpanChildren(rootBuffer));
    expect(children).toHaveLength(2);
    expect(children.every((buffer) => buffer instanceof plan.SpanBufferClass)).toBe(true);
  });
});
