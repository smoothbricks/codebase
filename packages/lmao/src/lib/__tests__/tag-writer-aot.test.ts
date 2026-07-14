import { describe, expect, it } from 'bun:test';
import './test-helpers.js';
import { createTagWriter, getTagWriterClass } from '../codegen/fixedPositionWriterGenerator.js';
import { resolveEnumLookupDescriptor } from '../enumMetadata.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { createSpanBuffer } from '../spanBuffer.js';
import type { AnySpanBuffer } from '../types.js';
import { createTestOpMetadata, createTestSchema, createTestSpanContext, createTestTraceRoot } from './test-helpers.js';

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

function arrayColumn(buffer: AnySpanBuffer, name: string): unknown[] {
  const column = buffer.getColumnIfAllocated(name);
  if (!Array.isArray(column)) {
    throw new Error(`${name} column was not allocated as an array`);
  }
  return column;
}

function float64Column(buffer: AnySpanBuffer, name: string): Float64Array {
  const column = buffer.getColumnIfAllocated(name);
  if (!(column instanceof Float64Array)) {
    throw new Error(`${name} column was not allocated as a Float64Array`);
  }
  return column;
}

function uint8Column(buffer: AnySpanBuffer, name: string): Uint8Array {
  const column = buffer.getColumnIfAllocated(name);
  if (!(column instanceof Uint8Array)) {
    throw new Error(`${name} column was not allocated as a Uint8Array`);
  }
  return column;
}

function bigUint64Column(buffer: AnySpanBuffer, name: string): BigUint64Array {
  const column = buffer.getColumnIfAllocated(name);
  if (!(column instanceof BigUint64Array)) {
    throw new Error(`${name} column was not allocated as a BigUint64Array`);
  }
  return column;
}

function invokeWriterMethod(writer: object, methodName: string, value: unknown): unknown {
  const method = Reflect.get(writer, methodName);
  if (typeof method !== 'function') {
    throw new Error(`TagWriter is missing ${methodName}()`);
  }
  return Reflect.apply(method, writer, [value]);
}

function setupWriter() {
  const schema = createTestSchema({
    categoryValue: S.category(),
    textValue: S.text(),
    numberValue: S.number(),
    booleanValue: S.boolean(),
    enumValue: S.enum(['FIRST', 'SECOND']),
  });
  const buffer = createSpanBuffer(schema, createTestTraceRoot('tag-writer-aot'), createTestOpMetadata(), 8);
  const context = createTestSpanContext(schema, buffer);
  return { buffer, schema, writer: createTagWriter(schema, context) };
}

describe('AOT-safe TagWriter class creation', () => {
  it('creates and caches a constructor for a fresh schema without calling global Function', () => {
    const schema = defineLogSchema({ status: S.enum(['QUEUED', 'DONE']) });
    const enumLookup = resolveEnumLookupDescriptor(schema);

    const functionConstructorCalls = countFunctionConstructorCalls(() => {
      getTagWriterClass(schema, ['status'], enumLookup);
    });

    expect(functionConstructorCalls).toBe(0);
  });

  it('caches by schema identity and ordered eager-column key', () => {
    const schema = defineLogSchema({ alpha: S.number(), beta: S.number() });
    const sameEagerKey = getTagWriterClass(schema, ['alpha']);

    expect(getTagWriterClass(schema, ['alpha'])).toBe(sameEagerKey);
    expect(getTagWriterClass(schema, ['beta'])).not.toBe(sameEagerKey);
    expect(getTagWriterClass(schema, ['alpha', 'beta'])).not.toBe(getTagWriterClass(schema, ['beta', 'alpha']));

    const structurallyEqualSchema = defineLogSchema({ alpha: S.number(), beta: S.number() });
    expect(getTagWriterClass(structurallyEqualSchema, ['alpha'])).not.toBe(sameEagerKey);
  });
});

describe('TagWriter row-0 semantics', () => {
  it('uses exact schema setter names, writes every scalar lane at row 0, and preserves fluent identity', () => {
    const { buffer, writer } = setupWriter();

    expect(writer.categoryValue('category-0')).toBe(writer);
    expect(writer.textValue('text-0')).toBe(writer);
    expect(writer.numberValue(42.5)).toBe(writer);
    expect(writer.booleanValue(true)).toBe(writer);
    expect(writer.enumValue('SECOND')).toBe(writer);
    expect(writer.uint64_value(18_446_744_073_709_551_615n)).toBe(writer);

    expect(arrayColumn(buffer, 'categoryValue')[0]).toBe('category-0');
    expect(arrayColumn(buffer, 'textValue')[0]).toBe('text-0');
    expect(float64Column(buffer, 'numberValue')[0]).toBe(42.5);
    expect((uint8Column(buffer, 'booleanValue')[0] & 0b0000_0001) !== 0).toBe(true);
    expect(uint8Column(buffer, 'enumValue')[0]).toBe(1);
    expect(bigUint64Column(buffer, 'uint64_value')[0]).toBe(18_446_744_073_709_551_615n);
  });

  it('encodes an invalid enum value as ordinal zero', () => {
    const { buffer, writer } = setupWriter();

    expect(invokeWriterMethod(writer, 'enumValue', 'INVALID')).toBe(writer);
    expect(uint8Column(buffer, 'enumValue')[0]).toBe(0);
  });

  it('with skips nullish values, evaluates attributes in schema order, and returns the same writer', () => {
    const schema = createTestSchema({
      first: S.number(),
      nullValue: S.text(),
      second: S.category(),
      undefinedValue: S.number(),
      third: S.boolean(),
    });
    const buffer = createSpanBuffer(schema, createTestTraceRoot('tag-writer-with-order'), createTestOpMetadata(), 8);
    const context = createTestSpanContext(schema, buffer);
    const writer = createTagWriter(schema, context);
    const firstAccesses: string[] = [];
    const accessed = new Set<string>();
    const recordFirstAccess = (name: string): void => {
      if (!accessed.has(name)) {
        accessed.add(name);
        firstAccesses.push(name);
      }
    };
    const attributes = Object.defineProperties(
      {},
      {
        third: {
          enumerable: true,
          get() {
            recordFirstAccess('third');
            return true;
          },
        },
        undefinedValue: {
          enumerable: true,
          get() {
            recordFirstAccess('undefinedValue');
            return undefined;
          },
        },
        second: {
          enumerable: true,
          get() {
            recordFirstAccess('second');
            return 'second-0';
          },
        },
        nullValue: {
          enumerable: true,
          get() {
            recordFirstAccess('nullValue');
            return null;
          },
        },
        first: {
          enumerable: true,
          get() {
            recordFirstAccess('first');
            return 1;
          },
        },
      },
    );

    expect(invokeWriterMethod(writer, 'with', attributes)).toBe(writer);
    expect(firstAccesses).toEqual(['first', 'nullValue', 'second', 'undefinedValue', 'third']);
    expect(float64Column(buffer, 'first')[0]).toBe(1);
    expect(arrayColumn(buffer, 'second')[0]).toBe('second-0');
    expect((uint8Column(buffer, 'third')[0] & 0b0000_0001) !== 0).toBe(true);
    expect(buffer.getColumnIfAllocated('nullValue')).toBeUndefined();
    expect(buffer.getColumnIfAllocated('undefinedValue')).toBeUndefined();
  });
});
