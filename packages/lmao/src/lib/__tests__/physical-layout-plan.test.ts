import { describe, expect, it } from 'bun:test';
import { getResultWriterClass, getTagWriterClass } from '../codegen/fixedPositionWriterGenerator.js';
import { createSpanLoggerClass } from '../codegen/spanLoggerGenerator.js';
import { defineOpContext } from '../defineOpContext.js';
import { createRemapDescriptor } from '../library.js';
import { getPhysicalLayoutPlan, PHYSICAL_LAYOUT_VERSION } from '../physicalLayoutPlan.js';
import {
  RUNTIME_HINT_ANALYZED_VALID,
  RUNTIME_HINT_LOG,
  RUNTIME_HINT_RESULT,
  RUNTIME_HINT_TAG,
} from '../runtimeHint.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { getSpanBufferClass } from '../spanBuffer.js';
import { TestTracer } from '../tracers/TestTracer.js';
import { createTestTracerOptions } from './test-helpers.js';

const CAPACITY_TIER = 17;
const HOT_CHILD_HINT =
  RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_TAG | RUNTIME_HINT_LOG | RUNTIME_HINT_RESULT | CAPACITY_TIER;

describe('PhysicalLayoutPlan', () => {
  it('reuses one immutable plan per schema, capability set, and backend', () => {
    const schema = defineLogSchema({ userId: S.category() });
    const otherSchema = defineLogSchema({ userId: S.category(), duration: S.number() });
    const SpanBufferClass = getSpanBufferClass(schema);
    const layout = createRemapDescriptor(schema, { app_user_id: 'userId' });
    const otherLayout = createRemapDescriptor(schema, { lib_user_id: 'userId' });

    const first = getPhysicalLayoutPlan(SpanBufferClass, HOT_CHILD_HINT, layout, 'js-heap');
    const identical = getPhysicalLayoutPlan(SpanBufferClass, HOT_CHILD_HINT, layout, 'js-heap');
    const differentCapabilities = getPhysicalLayoutPlan(
      SpanBufferClass,
      HOT_CHILD_HINT ^ RUNTIME_HINT_LOG,
      layout,
      'js-heap',
    );
    const differentBackend = getPhysicalLayoutPlan(SpanBufferClass, HOT_CHILD_HINT, layout, 'wasm');
    const differentLayout = getPhysicalLayoutPlan(SpanBufferClass, HOT_CHILD_HINT, otherLayout, 'js-heap');
    const differentSchema = getPhysicalLayoutPlan(
      getSpanBufferClass(otherSchema),
      HOT_CHILD_HINT,
      undefined,
      'js-heap',
    );

    expect(identical).toBe(first);
    expect(Object.isFrozen(first)).toBe(true);
    expect(first.remapDescriptor).toBe(layout);
    expect(differentCapabilities).not.toBe(first);
    expect(differentBackend).not.toBe(first);
    expect(differentLayout).not.toBe(first);
    expect(differentSchema).not.toBe(first);
    expect(differentCapabilities.capabilities).toBe(first.capabilities ^ RUNTIME_HINT_LOG);
    expect(differentBackend.backendKind).toBe('wasm');
    expect(differentLayout.remapDescriptor).toBe(otherLayout);
    expect(differentSchema.schema).toBe(otherSchema);
  });

  it('owns the schema-specific constructors, capacity, appenders, and reserved vocabulary slot', () => {
    const schema = defineLogSchema({ userId: S.category() });
    const SpanBufferClass = getSpanBufferClass(schema);
    const plan = getPhysicalLayoutPlan(SpanBufferClass, HOT_CHILD_HINT);
    const reusedPlan = getPhysicalLayoutPlan(SpanBufferClass, HOT_CHILD_HINT);

    expect(plan.version).toBe(PHYSICAL_LAYOUT_VERSION);
    expect(plan.schema).toBe(schema);
    expect(plan.SpanBufferClass).toBe(SpanBufferClass);
    expect(plan.SpanLoggerClass).toBe(createSpanLoggerClass(schema));
    expect(plan.TagWriterClass).toBe(getTagWriterClass(schema));
    expect(plan.ResultWriterClass).toBe(getResultWriterClass(schema));
    expect(plan.capacityTier).toBe(CAPACITY_TIER);
    expect(plan.clock.kind).toBe('trace-root');
    expect(plan.vocabularyBindingSlot).toBeNull();
    expect(plan.poolRef).toBeNull();
    expect(reusedPlan.clock).toBe(plan.clock);
    expect(reusedPlan.appenders).toBe(plan.appenders);
    expect(Object.isFrozen(plan.clock)).toBe(true);
    expect(Object.isFrozen(plan.appenders)).toBe(true);
  });

  it('consumes the same plan for repeated hot child creation without mutating it', async () => {
    const schema = defineLogSchema({ userId: S.category() });
    const opContext = defineOpContext({ logSchema: schema });
    const ownershipChecks: boolean[] = [];
    const writtenUsers: unknown[] = [];
    const writtenMessages: unknown[] = [];
    const clockChecks: boolean[] = [];

    const childOp = opContext.defineOp(
      'planned-child',
      (ctx) => {
        ownershipChecks.push(
          ctx.buffer instanceof childOp.physicalLayoutPlan.SpanBufferClass &&
            ctx.log instanceof childOp.physicalLayoutPlan.SpanLoggerClass &&
            ctx.tag instanceof childOp.physicalLayoutPlan.TagWriterClass,
        );
        const beforePlanClock = ctx.buffer._traceRoot.getTimestampNanos();
        const planTimestamp = childOp.physicalLayoutPlan.clock.now(ctx.buffer);
        const afterPlanClock = ctx.buffer._traceRoot.getTimestampNanos();
        clockChecks.push(planTimestamp >= beforePlanClock && planTimestamp <= afterPlanClock);
        ctx.tag.userId('planned-user');
        ctx.log.info('planned-log');
        writtenUsers.push(ctx.buffer.userId_values[0]);
        writtenMessages.push(ctx.buffer.message_values[2]);
        return ctx.ok('done');
      },
      undefined,
      { runtimeHint: HOT_CHILD_HINT, logTemplateIds: [] },
    );
    const parentOp = opContext.defineOp('parent', async (ctx) => {
      await ctx.span('first-child', childOp);
      await ctx.span('second-child', childOp);
      return ctx.ok('done');
    });

    const plan = childOp.physicalLayoutPlan;
    const ownedLogger = plan.SpanLoggerClass;
    const ownedTagWriter = plan.TagWriterClass;
    const ownedAppenders = plan.appenders;
    const tracer = new TestTracer(opContext, createTestTracerOptions());

    await tracer.trace('root', parentOp);

    const children = tracer.rootBuffers[0]._children;
    expect(children).toHaveLength(2);
    expect(ownershipChecks).toEqual([true, true]);
    expect(clockChecks).toEqual([true, true]);
    expect(children.map((buffer) => buffer._capacity)).toEqual([CAPACITY_TIER, CAPACITY_TIER]);
    expect(writtenUsers).toEqual(['planned-user', 'planned-user']);
    expect(writtenMessages).toEqual(['planned-log', 'planned-log']);
    expect(childOp.metadata._physicalLayoutPlan).toBe(plan);
    expect(childOp.physicalLayoutPlan).toBe(plan);
    expect(plan.SpanLoggerClass).toBe(ownedLogger);
    expect(plan.TagWriterClass).toBe(ownedTagWriter);
    expect(plan.appenders).toBe(ownedAppenders);
  });
});
