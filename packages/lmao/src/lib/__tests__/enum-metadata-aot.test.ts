import { describe, expect, it } from 'bun:test';
import './test-helpers.js';
import { resolveEnumLookupDescriptor } from '../enumMetadata.js';
import { createRemapDescriptor } from '../library.js';
import type { OpContext } from '../opContext/types.js';
import { getPhysicalLayoutPlan } from '../physicalLayoutPlan.js';
import {
  RUNTIME_HINT_ANALYZED_VALID,
  RUNTIME_HINT_LOG,
  RUNTIME_HINT_RESULT,
  RUNTIME_HINT_TAG,
} from '../runtimeHint.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { getSpanBufferClass } from '../spanBuffer.js';
import { createSpanContextClass } from '../spanContext.js';

const ENUM_CAPABILITIES = RUNTIME_HINT_TAG | RUNTIME_HINT_LOG | RUNTIME_HINT_RESULT;
const ENUM_CALLSITE_HINT = RUNTIME_HINT_ANALYZED_VALID | ENUM_CAPABILITIES | 4;

function countFunctionConstructorCalls(run: () => void): number {
  const originalDescriptor = Object.getOwnPropertyDescriptor(globalThis, 'Function');
  if (!originalDescriptor) throw new Error('Expected global Function descriptor');

  const originalFunction = globalThis.Function;
  let calls = 0;
  const functionProbe = new Proxy(originalFunction, {
    apply(target, thisArgument, argumentList) {
      calls++;
      return Reflect.apply(target, thisArgument, argumentList);
    },
    construct(target, argumentList, newTarget) {
      calls++;
      return Reflect.construct(target, argumentList, newTarget);
    },
  });

  Object.defineProperty(globalThis, 'Function', {
    configurable: originalDescriptor.configurable,
    enumerable: originalDescriptor.enumerable,
    value: functionProbe,
    writable: originalDescriptor.writable,
  });
  try {
    run();
  } finally {
    Object.defineProperty(globalThis, 'Function', originalDescriptor);
  }
  return calls;
}

describe('AOT-safe enum metadata', () => {
  it('encodes declaration order and every storage-width boundary without compiling a Function', () => {
    const functionConstructorCalls = countFunctionConstructorCalls(() => {
      const orderedValues = ['ZERO', '__proto__', 'constructor', 'toString', 'OMEGA'];
      const orderedSchema = defineLogSchema({ state: S.enum(orderedValues) });
      const ordered = resolveEnumLookupDescriptor(orderedSchema).byField.state;

      expect(ordered.values).toEqual(orderedValues);
      expect(orderedValues.map((value) => ordered.encode(value))).toEqual([0, 1, 2, 3, 4]);
      expect([ordered.encode('MISSING'), ordered.encode('valueOf'), ordered.encode('')]).toEqual([0, 0, 0]);

      const boundaries = [
        { count: 1, indexArrayConstructor: Uint8Array },
        { count: 256, indexArrayConstructor: Uint8Array },
        { count: 257, indexArrayConstructor: Uint16Array },
        { count: 65_536, indexArrayConstructor: Uint16Array },
        { count: 65_537, indexArrayConstructor: Uint32Array },
      ];
      for (const { count, indexArrayConstructor } of boundaries) {
        const values = Array.from({ length: count }, (_, index) => `VALUE_${index}`);
        const schema = defineLogSchema({ boundary: S.enum(values) });
        const descriptor = resolveEnumLookupDescriptor(schema).byField.boundary;
        const midpoint = Math.floor((count - 1) / 2);

        expect(descriptor.indexArrayConstructor).toBe(indexArrayConstructor);
        expect(descriptor.values).toEqual(values);
        expect([
          descriptor.encode('VALUE_0'),
          descriptor.encode(`VALUE_${midpoint}`),
          descriptor.encode(`VALUE_${count - 1}`),
          descriptor.encode(`VALUE_${count}`),
        ]).toEqual([0, midpoint, count - 1, 0]);
      }
    });

    expect(functionConstructorCalls).toBe(0);
  });

  it('maps non-string values to zero without coercion or Function compilation', () => {
    const functionConstructorCalls = countFunctionConstructorCalls(() => {
      const schema = defineLogSchema({
        outcome: S.enum(['ZERO', '1', '[object Object]', 'Symbol(value)']),
      });
      const encode = resolveEnumLookupDescriptor(schema).byField.outcome.encode;
      const invalidValues = [1, {}, Symbol('value')];

      expect(invalidValues.map((value) => Reflect.apply(encode, undefined, [value]))).toEqual([0, 0, 0]);
    });

    expect(functionConstructorCalls).toBe(0);
  });

  it('reuses schema descriptors and field cores without compiling a Function', () => {
    const functionConstructorCalls = countFunctionConstructorCalls(() => {
      const sharedField = S.enum(['QUEUED', 'RUNNING', 'DONE']);
      const primarySchema = defineLogSchema({ state: sharedField });
      const renamedSchema = defineLogSchema({ renamedState: sharedField });

      const primaryLookup = resolveEnumLookupDescriptor(primarySchema);
      const primaryDescriptor = primaryLookup.byField.state;
      const renamedDescriptor = resolveEnumLookupDescriptor(renamedSchema).byField.renamedState;

      expect(resolveEnumLookupDescriptor(primarySchema)).toBe(primaryLookup);
      expect(resolveEnumLookupDescriptor(primarySchema).byField.state).toBe(primaryDescriptor);
      expect(renamedDescriptor).not.toBe(primaryDescriptor);
      expect(renamedDescriptor.fieldName).toBe('renamedState');
      expect(renamedDescriptor.values).toBe(primaryDescriptor.values);
      expect(renamedDescriptor.encode).toBe(primaryDescriptor.encode);
      expect(renamedDescriptor.encode('RUNNING')).toBe(1);
      expect(renamedDescriptor.encode('UNKNOWN')).toBe(0);
    });

    expect(functionConstructorCalls).toBe(0);
  });

  it('shares the schema descriptor with remapped physical plans without compiling a Function', () => {
    const schema = defineLogSchema({ operation: S.enum(['READ', 'WRITE']) });
    const functionConstructorCalls = countFunctionConstructorCalls(() => {
      const firstLookup = resolveEnumLookupDescriptor(schema);
      expect(firstLookup.byField.operation.encode('WRITE')).toBe(1);
      expect(firstLookup.byField.operation.encode('INVALID')).toBe(0);
    });

    expect(functionConstructorCalls).toBe(0);

    const lookup = resolveEnumLookupDescriptor(schema);
    const remap = createRemapDescriptor(schema, { library_operation: 'operation' });
    const SpanBufferClass = getSpanBufferClass(schema);
    const SpanContextClass = createSpanContextClass<OpContext<typeof schema>>(
      schema,
      { logSchema: schema, remapDescriptor: undefined },
      ENUM_CAPABILITIES,
    );

    const basePlan = getPhysicalLayoutPlan(SpanBufferClass, ENUM_CALLSITE_HINT, SpanContextClass);
    const remappedPlan = getPhysicalLayoutPlan(SpanBufferClass, ENUM_CALLSITE_HINT, SpanContextClass, remap);

    expect(basePlan.enumLookup).toBe(lookup);
    expect(remappedPlan.enumLookup).toBe(lookup);
    expect(remappedPlan.enumLookup.byField.operation).toBe(lookup.byField.operation);
  });
});
