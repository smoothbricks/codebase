/**
 * Compiled vs closure-composed fixed-position writer parity.
 *
 * getResultWriterClass selects between the compiled renderer (new Function)
 * and the closure-composed materializer via activeMaterializerMode(). This
 * suite drives identical write scripts through both classes against real
 * SpanBuffers and byte-compares every touched lane (values, null bitmaps,
 * message lanes). TagWriter ships closure-only, so its closure class is
 * compared against a compiled reference built from the still-exported
 * generateFixedPositionWriterClass source.
 *
 * Both materializers run under bun via the explicit override hook
 * (setMaterializerModeOverride); afterEach restores probe-based selection.
 */

import { afterEach, describe, expect, it } from 'bun:test';
import { activeMaterializerMode, bufferHelpers, setMaterializerModeOverride } from '@smoothbricks/arrow-builder';
import {
  createTestOpMetadata,
  createTestSchema,
  createTestSpanContext,
  createTestTraceRoot,
} from '../../__tests__/test-helpers.js';
import { resolveEnumLookupDescriptor } from '../../enumMetadata.js';
import type { MessageLayoutFamily } from '../../runtimeHint.js';
import { S } from '../../schema/builder.js';
import type { LogSchema, SchemaFields } from '../../schema/types.js';
import { createSpanBuffer, getSpanBufferClass } from '../../spanBuffer.js';
import type { AnySpanBuffer } from '../../types.js';
import {
  generateFixedPositionWriterClass,
  getResultWriterClass,
  getTagWriterClass,
  type WriterState,
} from '../fixedPositionWriterGenerator.js';

afterEach(() => {
  setMaterializerModeOverride(undefined);
});

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// >256 values force the Uint16 enum index lane, the widest special case.
const wideEnumValues = Array.from({ length: 300 }, (_, i) => `value_${i}`);

// Every value family the writer generator can route to a SpanBuffer setter.
function createParityFields() {
  return {
    status: S.enum(['ok', 'err']),
    wide: S.enum(wideEnumValues),
    cat: S.category(),
    txt: S.text(),
    num: S.number(),
    bool: S.boolean(),
    bin: S.binary(),
    obj: S.object<{ a: number }>(),
  };
}

const USER_LANES = ['status', 'wide', 'cat', 'txt', 'num', 'bool', 'bin', 'obj'] as const;
const SYSTEM_LANES = ['uint64_value', 'line'] as const;

// SpanBuffer classes still require string codegen, so buffers and contexts are
// created under the default (compiled) mode; only writer-class creation runs
// under the closure override.
function createBufferPair<T extends SchemaFields>(schema: LogSchema<T>, family: MessageLayoutFamily) {
  const BufferClass = getSpanBufferClass(schema, family);
  const bufferA = createSpanBuffer(schema, createTestTraceRoot('a'), createTestOpMetadata(), 8, BufferClass);
  const bufferB = createSpanBuffer(schema, createTestTraceRoot('b'), createTestOpMetadata(), 8, BufferClass);
  return {
    bufferA,
    bufferB,
    contextA: createTestSpanContext(schema, bufferA),
    contextB: createTestSpanContext(schema, bufferB),
  };
}

function withClosureMode<T>(create: () => T): T {
  setMaterializerModeOverride('closure');
  try {
    return create();
  } finally {
    setMaterializerModeOverride(undefined);
  }
}

// ---------------------------------------------------------------------------
// Reflect helpers (schema-dynamic methods; repo lint bans unsafe assertions)
// ---------------------------------------------------------------------------

function constructWriter(ctor: unknown, state: WriterState): object {
  if (typeof ctor !== 'function') throw new Error('writer constructor is not callable');
  const instance: unknown = Reflect.construct(ctor, [state]);
  if (typeof instance !== 'object' || instance === null) throw new Error('writer construction failed');
  return instance;
}

function call(writer: object, method: string, value: unknown): unknown {
  const fn = Reflect.get(writer, method);
  if (typeof fn !== 'function') throw new Error(`missing writer method ${JSON.stringify(method)}`);
  return Reflect.apply(fn, writer, [value]);
}

function prototypeOf(ctor: unknown): object {
  if (typeof ctor !== 'function') throw new Error('expected a constructor');
  const proto: unknown = Reflect.get(ctor, 'prototype');
  if (typeof proto !== 'object' || proto === null) throw new Error('expected a prototype object');
  return proto;
}

function nameOf(ctor: unknown): string {
  if (typeof ctor !== 'function') throw new Error('expected a constructor');
  return ctor.name;
}

// ---------------------------------------------------------------------------
// Write scripts (identical for both materializers)
// ---------------------------------------------------------------------------

function runFieldScript(writer: object): void {
  // Invalid enum values encode to ordinal 0 before the valid overwrite.
  expect(call(writer, 'status', 'not-a-value')).toBe(writer);
  call(writer, 'status', 'err');
  call(writer, 'wide', 'value_299');
  call(writer, 'cat', 'checkout');
  call(writer, 'txt', 'unique text \u2713');
  call(writer, 'num', 42.5);
  call(writer, 'bool', true);
  call(writer, 'bin', Uint8Array.of(1, 2, 3));
  call(writer, 'obj', { a: 7 });
  call(writer, 'uint64_value', 123456789n);
  // Bulk set: null/undefined entries are skipped, unknown keys ignored.
  call(writer, 'with', { status: 'ok', num: 7, cat: null, txt: undefined, notAColumn: 'ignored' });
}

function runResultScript(writer: object): void {
  runFieldScript(writer);
  call(writer, 'message', 'span message');
  call(writer, 'line', 1234);
}

// ---------------------------------------------------------------------------
// Lane comparison
// ---------------------------------------------------------------------------

function expectIdenticalLanes(actual: AnySpanBuffer, expected: AnySpanBuffer, lanes: readonly string[]): void {
  for (const lane of lanes) {
    const actualValues = actual.getColumnIfAllocated(lane);
    const expectedValues = expected.getColumnIfAllocated(lane);
    expect(actualValues?.constructor).toBe(expectedValues?.constructor);
    expect(actualValues).toEqual(expectedValues);
    expect(actual.getNullsIfAllocated(lane)).toEqual(expected.getNullsIfAllocated(lane));
  }
}

// ---------------------------------------------------------------------------
// ResultWriter parity (compiled vs closure)
// ---------------------------------------------------------------------------

describe('ResultWriter compiled/closure parity', () => {
  const families: readonly MessageLayoutFamily[] = ['mixed', 'dynamic-only', 'static-only'];

  for (const family of families) {
    it(`writes byte-identical lanes for the ${family} message layout family`, () => {
      expect(activeMaterializerMode()).toBe('compiled');
      const schema = createTestSchema(createParityFields());
      const { bufferA, bufferB, contextA, contextB } = createBufferPair(schema, family);

      const Compiled = getResultWriterClass(schema, family, 'current');
      const Closure = withClosureMode(() => getResultWriterClass(schema, family, 'current'));
      expect(Closure).not.toBe(Compiled);
      expect(nameOf(Closure)).toBe(nameOf(Compiled));

      const compiledWriter = constructWriter(Compiled, contextA);
      const closureWriter = constructWriter(Closure, contextB);
      expect(Reflect.ownKeys(closureWriter)).toEqual(Reflect.ownKeys(compiledWriter));

      runResultScript(compiledWriter);
      runResultScript(closureWriter);

      expectIdenticalLanes(bufferB, bufferA, [...USER_LANES, ...SYSTEM_LANES]);
      if (family === 'static-only') {
        expect(bufferA._terminalMessage).toBe('span message');
        expect(bufferB._terminalMessage).toBe(bufferA._terminalMessage);
        expect(bufferB.message_values).toBeUndefined();
        expect(bufferA.message_values).toBeUndefined();
      } else {
        expect(bufferA.message_values?.[1]).toBe('span message');
        expect(bufferB.message_values).toEqual(bufferA.message_values);
        expect(bufferB._terminalMessage).toBe(bufferA._terminalMessage);
      }
    });
  }

  it('installs class-member-equivalent prototype descriptors', () => {
    const schema = createTestSchema(createParityFields());
    const Compiled = getResultWriterClass(schema, 'mixed', 'current');
    const Closure = withClosureMode(() => getResultWriterClass(schema, 'mixed', 'current'));

    const compiledProto = prototypeOf(Compiled);
    const closureProto = prototypeOf(Closure);
    const members = [...schema._columns.map(([fieldName]) => fieldName), 'with', 'message', 'line', 'uint64_value'];

    for (const member of members) {
      const compiledDescriptor = Object.getOwnPropertyDescriptor(compiledProto, member);
      const closureDescriptor = Object.getOwnPropertyDescriptor(closureProto, member);
      if (!compiledDescriptor || !closureDescriptor) {
        throw new Error(`missing prototype member ${JSON.stringify(member)}`);
      }
      expect(closureDescriptor.writable).toBe(compiledDescriptor.writable);
      expect(closureDescriptor.configurable).toBe(compiledDescriptor.configurable);
      expect(closureDescriptor.enumerable).toBe(compiledDescriptor.enumerable);
      expect(typeof closureDescriptor.value).toBe(typeof compiledDescriptor.value);
      if (typeof closureDescriptor.value === 'function' && typeof compiledDescriptor.value === 'function') {
        expect(closureDescriptor.value.name).toBe(compiledDescriptor.value.name);
      }
    }
  });

  it('caches classes per materializer mode and restores on override reset', () => {
    const schema = createTestSchema({ n: S.number() });
    const compiled = getResultWriterClass(schema);
    expect(getResultWriterClass(schema)).toBe(compiled);

    setMaterializerModeOverride('closure');
    const closure = getResultWriterClass(schema);
    expect(getResultWriterClass(schema)).toBe(closure);
    expect(closure).not.toBe(compiled);

    setMaterializerModeOverride(undefined);
    expect(getResultWriterClass(schema)).toBe(compiled);
  });
});

// ---------------------------------------------------------------------------
// TagWriter parity (shipped closure class vs compiled-source reference)
// ---------------------------------------------------------------------------

describe('TagWriter closure vs compiled-source reference parity', () => {
  it('writes byte-identical row-0 lanes', () => {
    const schema = createTestSchema(createParityFields());
    const enumLookup = resolveEnumLookupDescriptor(schema);
    const { bufferA, bufferB, contextA, contextB } = createBufferPair(schema, 'mixed');

    // Compiled reference assembled here (tests run under bun where string
    // codegen is allowed); production tag writers never eval.
    const classCode = generateFixedPositionWriterClass(
      schema,
      0,
      'GeneratedTagWriter',
      undefined,
      [],
      enumLookup,
    ).trim();
    const factory = new Function('helpers', 'enumLookup', classCode);
    const CompiledTag: unknown = factory(bufferHelpers, enumLookup);

    const ClosureTag = withClosureMode(() => getTagWriterClass(schema));
    expect(getTagWriterClass(schema)).toBe(ClosureTag);
    expect(nameOf(ClosureTag)).toBe(nameOf(CompiledTag));

    const compiledWriter = constructWriter(CompiledTag, contextA);
    const closureWriter = constructWriter(ClosureTag, contextB);
    expect(Reflect.ownKeys(closureWriter)).toEqual(Reflect.ownKeys(compiledWriter));

    runFieldScript(compiledWriter);
    runFieldScript(closureWriter);

    expectIdenticalLanes(bufferB, bufferA, [...USER_LANES, 'uint64_value']);
  });
});
