import { describe, expect, it } from 'bun:test';
import './test-helpers.js';
import { convertSpanTreeToArrowTable } from '../convertToArrow.js';
import { defineOpContext } from '../defineOpContext.js';
import { resolveEnumLookupDescriptor } from '../enumMetadata.js';
import { createRemapDescriptor } from '../library.js';
import { getPhysicalLayoutPlan } from '../physicalLayoutPlan.js';
import type { OpContext } from '../opContext/types.js';
import {
  RUNTIME_HINT_ANALYZED_VALID,
  RUNTIME_HINT_LOG,
  RUNTIME_HINT_RESULT,
  RUNTIME_HINT_SPAN,
  RUNTIME_HINT_TAG,
} from '../runtimeHint.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { getSpanBufferClass } from '../spanBuffer.js';
import { createSpanContextClass } from '../spanContext.js';
import { TestTracer } from '../tracers/TestTracer.js';
import { iterateSpanTree } from '../traceTopology.js';
import { requireColumn } from './arrow-test-helpers.js';
import { createTestTracerOptions } from './test-helpers.js';

function makeValues(count: number): string[] {
  return Array.from({ length: count }, (_, index) => `VALUE_${index}`);
}

function invokeEnumWriter(writer: object, value: string): void {
  const method = Reflect.get(writer, 'operation');
  if (typeof method !== 'function') throw new TypeError('Expected an operation enum writer');
  Reflect.apply(method, writer, [value]);
}

const CHILD_HINT = RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_TAG | RUNTIME_HINT_LOG | RUNTIME_HINT_RESULT | 4;
const PARENT_HINT = RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_SPAN | RUNTIME_HINT_RESULT | 4;

describe('startup-hoisted enum lookup metadata', () => {
  it.each([
    [255, Uint8Array],
    [256, Uint8Array],
    [65_535, Uint16Array],
    [65_536, Uint16Array],
    [65_537, Uint32Array],
  ])('preserves exact index width and boundary ordinal for %d values', (count, expectedConstructor) => {
    const values = makeValues(count);
    const schema = defineLogSchema({ boundary: S.enum(values) });
    const descriptor = resolveEnumLookupDescriptor(schema).byField.boundary;

    expect(descriptor.indexArrayConstructor).toBe(expectedConstructor);
    expect(descriptor.values).toEqual(values);
    expect(Object.isFrozen(descriptor.values)).toBe(true);
    expect(descriptor.encode(values[0])).toBe(0);
    expect(descriptor.encode(values[count - 1])).toBe(count - 1);
    expect(descriptor.encode('NOT_IN_THE_ENUM')).toBe(0);
  });

  it('resolves once and reuses one descriptor through repeated trace, span, tag, log, result, overflow, and Arrow paths', async () => {
    let valueMetadataReads = 0;
    const values = new Proxy(['READ', 'WRITE'], {
      get(target, property, receiver) {
        if (property === Symbol.iterator || (typeof property === 'string' && /^\d+$/.test(property))) {
          valueMetadataReads++;
        }
        return Reflect.get(target, property, receiver);
      },
    });
    const schema = defineLogSchema({ operation: S.enum(values) });
    const context = defineOpContext({ logSchema: schema });

    valueMetadataReads = 0;
    const child = context.defineOp(
      'enum-child',
      (ctx) => {
        ctx.tag.operation('WRITE');
        for (let index = 0; index < 10; index++) {
          ctx.log.info(`enum-${index}`).operation(index % 2 === 0 ? 'READ' : 'WRITE');
        }
        return ctx.ok('done').with({ operation: 'WRITE' });
      },
      undefined,
      { runtimeHint: CHILD_HINT },
    );
    const firstResolutionReads = valueMetadataReads;
    const lookup = child.callsitePlan.enumLookup;
    expect(firstResolutionReads).toBeGreaterThan(0);
    expect(resolveEnumLookupDescriptor(child.callsitePlan.schema)).toBe(lookup);
    const encode = lookup.byField.operation.encode;

    valueMetadataReads = 0;
    const parent = context.defineOp(
      'enum-parent',
      async (ctx) => {
        for (let index = 0; index < 3; index++) await ctx.span(`child-${index}`, child);
        return ctx.ok('done');
      },
      undefined,
      { runtimeHint: PARENT_HINT },
    );
    expect(parent.callsitePlan.enumLookup).toBe(lookup);

    const tracer = new TestTracer(context, createTestTracerOptions());
    for (let index = 0; index < 4; index++) await tracer.trace(`trace-${index}`, parent);

    expect(child.callsitePlan.enumLookup).toBe(lookup);
    expect(parent.callsitePlan.enumLookup).toBe(lookup);
    expect(valueMetadataReads).toBe(0);

    let overflowCount = 0;
    for (const root of tracer.rootBuffers) {
      for (const segment of iterateSpanTree(root)) {
        if (segment._overflow !== undefined) overflowCount++;
      }
      const table = convertSpanTreeToArrowTable(root);
      const operations = requireColumn(table, 'operation');
      expect(Array.from({ length: operations.length }, (_, row) => operations.get(row)).filter(Boolean)).toContain('WRITE');
    }
    expect(overflowCount).toBeGreaterThan(0);
    expect(resolveEnumLookupDescriptor(child.callsitePlan.schema)).toBe(lookup);
    expect(child.callsitePlan.enumLookup.byField.operation.encode).toBe(encode);
  });

  it('keeps invalid-value fallback and dictionary decoding exact for tag, log, and result writers', async () => {
    const schema = defineLogSchema({ operation: S.enum(['READ', 'WRITE']) });
    const context = defineOpContext({ logSchema: schema });
    const op = context.defineOp('invalid-enum', (ctx) => {
      invokeEnumWriter(ctx.tag, 'INVALID');
      invokeEnumWriter(ctx.log.info('invalid-log'), 'INVALID');
      return ctx.ok('done').with({ operation: JSON.parse('"INVALID"') });
    });
    const tracer = new TestTracer(context, createTestTracerOptions());

    await tracer.trace('invalid-enum-root', op);

    const root = tracer.rootBuffers[0];
    if (!root) throw new Error('Expected invalid enum trace root');
    const raw = root.operation_values;
    expect([raw[0], raw[1], raw[2]]).toEqual([0, 0, 0]);
    const decoded = requireColumn(convertSpanTreeToArrowTable(root), 'operation');
    expect([decoded.get(0), decoded.get(1), decoded.get(2)]).toEqual(['READ', 'READ', 'READ']);
    expect(op.callsitePlan.enumLookup.byField.operation.encode('INVALID')).toBe(0);
  });

  it('separates extended schemas while remapped plans reuse the extended schema descriptor', () => {
    const base = defineLogSchema({ operation: S.enum(['READ', 'WRITE']) });
    const extended = base.extend({ phase: S.enum(['QUEUED', 'RUNNING', 'DONE']) });
    const baseLookup = resolveEnumLookupDescriptor(base);
    const extendedLookup = resolveEnumLookupDescriptor(extended);

    expect(baseLookup.ordered.map((descriptor) => descriptor.fieldName)).toEqual(['operation']);
    expect(extendedLookup.ordered.map((descriptor) => descriptor.fieldName)).toEqual(['operation', 'phase']);
    expect(extendedLookup).not.toBe(baseLookup);
    expect(resolveEnumLookupDescriptor(base)).toBe(baseLookup);
    expect(resolveEnumLookupDescriptor(extended)).toBe(extendedLookup);

    const remap = createRemapDescriptor(extended, {
      library_operation: 'operation',
      library_phase: 'phase',
    });
    const SpanBufferClass = getSpanBufferClass(extended);
    const SpanContextClass = createSpanContextClass<OpContext<typeof extended>>(
      extended,
      { logSchema: extended, remapDescriptor: undefined },
      RUNTIME_HINT_TAG | RUNTIME_HINT_LOG | RUNTIME_HINT_RESULT,
    );
    const basePlan = getPhysicalLayoutPlan(SpanBufferClass, CHILD_HINT, SpanContextClass);
    const remappedPlan = getPhysicalLayoutPlan(SpanBufferClass, CHILD_HINT, SpanContextClass, remap);

    expect(basePlan.enumLookup).toBe(extendedLookup);
    expect(remappedPlan.enumLookup).toBe(extendedLookup);
    expect(remappedPlan.remapDescriptor).toBe(remap);
    expect(remappedPlan.enumLookup.byField.operation.values).toEqual(['READ', 'WRITE']);
    expect(remappedPlan.enumLookup.byField.phase.values).toEqual(['QUEUED', 'RUNNING', 'DONE']);
  });
});
