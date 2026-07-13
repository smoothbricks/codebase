import { describe, expect, it } from 'bun:test';
import fc from 'fast-check';
import { createRemapDescriptor } from '../library.js';
import {
  getPhysicalLayoutPlan,
  resolveEagerColumns,
  type EagerColumnDescriptor,
} from '../physicalLayoutPlan.js';
import type { OpContext } from '../opContext/types.js';
import { RUNTIME_HINT_ANALYZED_VALID, RUNTIME_HINT_LOG, RUNTIME_HINT_RESULT } from '../runtimeHint.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { LogSchema } from '../schema/LogSchema.js';
import {
  createChildSpanBuffer,
  createOverflowBuffer,
  createSpanBuffer,
  getSpanBufferClass,
} from '../spanBuffer.js';
import { createSpanContextClass } from '../spanContext.js';
import type { AnySpanBuffer } from '../types.js';
import { createTestOpMetadata, createTestTraceRoot } from './test-helpers.js';

const HINT = RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_LOG | RUNTIME_HINT_RESULT | 8;

function descriptorBytes(descriptor: EagerColumnDescriptor): Uint8Array {
  const bytes = new Uint8Array(descriptor.words.length * Uint32Array.BYTES_PER_ELEMENT);
  const view = new DataView(bytes.buffer);
  for (let index = 0; index < descriptor.words.length; index++) {
    view.setUint32(index * Uint32Array.BYTES_PER_ELEMENT, descriptor.words[index], true);
  }
  return bytes;
}


function expectCompilerEagerStorage(buffer: AnySpanBuffer, name: string): void {
  expect(Object.hasOwn(buffer, `_${name}_values`)).toBe(true);
  expect(Object.hasOwn(buffer, `_${name}_nulls`)).toBe(true);
  expect(Reflect.get(buffer, `_${name}_values`)).toBeDefined();
  expect(Reflect.get(buffer, `_${name}_nulls`)).toBeInstanceOf(Uint8Array);
  expect(Object.hasOwn(buffer, `${name}_values`)).toBe(false);
  expect(Object.hasOwn(buffer, `${name}_nulls`)).toBe(false);
}

function expectLazyStorage(buffer: AnySpanBuffer, name: string): void {
  expect(Reflect.get(buffer, `_${name}_values`)).toBeUndefined();
  expect(Reflect.get(buffer, `_${name}_nulls`)).toBeUndefined();
  expect(buffer.getColumnIfAllocated(name)).toBeUndefined();
  expect(buffer.getNullsIfAllocated(name)).toBeUndefined();
}

describe('compiler-proven eager user columns', () => {
  it('encodes exact schema-order descriptor words and bytes beyond 32 fields', () => {
    const schema = new LogSchema({
      f00: S.number(), f01: S.number(), f02: S.number(), f03: S.number(), f04: S.number(),
      f05: S.number(), f06: S.number(), f07: S.number(), f08: S.number(), f09: S.number(),
      f10: S.number(), f11: S.number(), f12: S.number(), f13: S.number(), f14: S.number(),
      f15: S.number(), f16: S.number(), f17: S.number(), f18: S.number(), f19: S.number(),
      f20: S.number(), f21: S.number(), f22: S.number(), f23: S.number(), f24: S.number(),
      f25: S.number(), f26: S.number(), f27: S.number(), f28: S.number(), f29: S.number(),
      f30: S.number(), f31: S.number(), f32: S.number(), f33: S.number(), f34: S.number(),
      f35: S.number(), f36: S.number(), f37: S.number(), f38: S.number(), f39: S.number(),
    });

    const descriptor = resolveEagerColumns(schema, ['f39', 'f00', 'f32', 'f00']);

    expect(descriptor.names).toEqual(['f00', 'f32', 'f39']);
    expect(descriptor.words).toEqual([0x00000001, 0x00000081]);
    expect(descriptor.key).toBe('0000000100000081');
    expect([...descriptorBytes(descriptor)]).toEqual([1, 0, 0, 0, 129, 0, 0, 0]);
    expect(Object.isFrozen(descriptor)).toBe(true);
    expect(Object.isFrozen(descriptor.names)).toBe(true);
    expect(Object.isFrozen(descriptor.words)).toBe(true);
    expect(() => resolveEagerColumns(schema, ['missing'])).toThrow('Unknown eager column: missing');

    fc.assert(
      fc.property(
        fc.uniqueArray(fc.integer({ min: 0, max: 39 }), { maxLength: 40 }),
        fc.boolean(),
        (ordinals, reverse) => {
          const requested = ordinals.map((ordinal) => `f${ordinal.toString().padStart(2, '0')}`);
          if (reverse) requested.reverse();
          const resolved = resolveEagerColumns(schema, requested);
          const expectedWords = [0, 0];
          for (const ordinal of ordinals) {
            expectedWords[ordinal >>> 5] = (expectedWords[ordinal >>> 5] | (1 << (ordinal & 31))) >>> 0;
          }
          while (expectedWords.length > 0 && expectedWords[expectedWords.length - 1] === 0) expectedWords.pop();
          expect(resolved.words).toEqual(expectedWords);
          expect(resolved.names).toEqual(schema._columnNames.filter((name) => requested.includes(name)));
          expect(descriptorBytes(resolved).byteLength).toBe(resolved.words.length * 4);
        },
      ),
      { numRuns: 80 },
    );
  });

  it('selects one immutable plan and preallocates only proven nullable lanes across root, child, and overflow', () => {
    const schema = defineLogSchema({
      provenNumber: S.number(),
      lazyString: S.category(),
      provenString: S.category(),
      lazyBoolean: S.boolean(),
    });
    const baseClass = getSpanBufferClass(schema);
    const logBinding = { logSchema: schema, remapDescriptor: undefined };
    const SpanContextClass = createSpanContextClass<OpContext<typeof schema>>(schema, logBinding, RUNTIME_HINT_LOG | RUNTIME_HINT_RESULT);
    const remap = createRemapDescriptor(schema, { local_number: 'provenNumber' });
    const plan = getPhysicalLayoutPlan(
      baseClass,
      HINT,
      SpanContextClass,
      undefined,
      'js-heap',
      '',
      ['provenString', 'provenNumber', 'provenString'],
    );
    const identical = getPhysicalLayoutPlan(
      baseClass,
      HINT,
      SpanContextClass,
      undefined,
      'js-heap',
      '',
      ['provenNumber', 'provenString'],
    );
    const lazyPlan = getPhysicalLayoutPlan(baseClass, HINT, SpanContextClass, undefined, 'js-heap');
    const remapped = getPhysicalLayoutPlan(
      baseClass,
      HINT,
      SpanContextClass,
      remap,
      'js-heap',
      '',
      ['provenNumber', 'provenString'],
    );

    expect(identical).toBe(plan);
    expect(lazyPlan).not.toBe(plan);
    expect(remapped.eagerColumns).toBe(plan.eagerColumns);
    expect(plan.eagerColumns.names).toEqual(['provenNumber', 'provenString']);
    expect(plan.SpanBufferClass.eagerColumns).toBe(plan.eagerColumns);
    expect(plan.wasmLayout.eagerColumns).toBe(plan.eagerColumns);

    plan.SpanBufferClass.stats.capacity = 8;
    const metadata = createTestOpMetadata();
    const root = createSpanBuffer(schema, createTestTraceRoot('eager-root'), metadata, 8, plan.SpanBufferClass);
    const child = createChildSpanBuffer(root, plan.SpanBufferClass, metadata, metadata, 8);
    const overflow = createOverflowBuffer(child);

    for (const buffer of [root, child, overflow]) {
      expectCompilerEagerStorage(buffer, 'provenNumber');
      expectCompilerEagerStorage(buffer, 'provenString');
      expectLazyStorage(buffer, 'lazyString');
      expectLazyStorage(buffer, 'lazyBoolean');
      expect(buffer.constructor).toBe(plan.SpanBufferClass);
    }
    expect(child._parent).toBe(root);
    expect(overflow._parent).toBe(root);
    expect(overflow._identity).toBe(child._identity);
    expect(child._identity).not.toBe(root._identity);

    root.provenNumber(0, 42);
    root.provenNumber(1, null);
    child.provenString(2, 'child');
    overflow.provenNumber(3, null);
    root.lazyString(4, 'materialized lazily');

    expect(root.getColumnIfAllocated('provenNumber')?.[0]).toBe(42);
    const rootNumberNulls = root.getNullsIfAllocated('provenNumber');
    if (!rootNumberNulls) throw new Error('expected provenNumber null bitmap');
    expect(rootNumberNulls[0] & 0b00000001).toBe(1);
    expect(rootNumberNulls[0] & 0b00000010).toBe(0);
    expect(child.getColumnIfAllocated('provenString')?.[2]).toBe('child');
    const childStringNulls = child.getNullsIfAllocated('provenString');
    if (!childStringNulls) throw new Error('expected provenString null bitmap');
    expect(childStringNulls[0] & 0b00000100).toBe(0b00000100);
    const overflowNumberNulls = overflow.getNullsIfAllocated('provenNumber');
    if (!overflowNumberNulls) throw new Error('expected overflow provenNumber null bitmap');
    expect(overflowNumberNulls[0] & 0b00001000).toBe(0);
    expect(root.getColumnIfAllocated('lazyString')?.[4]).toBe('materialized lazily');
    expect(root.getColumnIfAllocated('lazyBoolean')).toBeUndefined();
  });

  it('keeps plugin-off metadata all-lazy', () => {
    const schema = defineLogSchema({ field: S.category() });
    const SpanBufferClass = getSpanBufferClass(schema);
    const SpanContextClass = createSpanContextClass<OpContext<typeof schema>>(
      schema,
      { logSchema: schema, remapDescriptor: undefined },
      RUNTIME_HINT_LOG | RUNTIME_HINT_RESULT,
    );
    const plan = getPhysicalLayoutPlan(SpanBufferClass, HINT, SpanContextClass);
    const buffer = createSpanBuffer(schema, createTestTraceRoot('plugin-off'), createTestOpMetadata(), 8, plan.SpanBufferClass);

    expect(plan.eagerColumns.names).toEqual([]);
    expect(plan.eagerColumns.words).toEqual([]);
    expect(plan.eagerColumns.key).toBe('');
    expectLazyStorage(buffer, 'field');
  });
});
