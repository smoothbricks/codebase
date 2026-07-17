/**
 * Parity suite: the compiled (new Function) and closure-composed (no-eval)
 * materializers MUST produce byte-identical buffers for the same schema and
 * write script. The closure path is what production workerd runs (string
 * codegen is forbidden there), so any divergence here is a production bug.
 *
 * Both materializers run under bun via the explicit override hook
 * (setMaterializerModeOverride).
 */

import { afterEach, describe, expect, it } from 'bun:test';
import { S } from '../../schema/builder.js';
import { ColumnSchema } from '../../schema/ColumnSchema.js';
import {
  activeMaterializerMode,
  isStringCodegenSupported,
  type MaterializerMode,
  setMaterializerModeOverride,
} from '../codegenCapability.js';
import { createGeneratedColumnBuffer, getColumnBufferClass } from '../columnBufferGenerator.js';
import { createColumnWriter, getColumnWriterClass } from '../columnWriterGenerator.js';
import type { AnyColumnBuffer } from '../types.js';

afterEach(() => {
  setMaterializerModeOverride(undefined);
});

function withMode<T>(mode: MaterializerMode, fn: () => T): T {
  setMaterializerModeOverride(mode);
  try {
    return fn();
  } finally {
    setMaterializerModeOverride(undefined);
  }
}

function callMethod(target: object, name: string, ...args: unknown[]): unknown {
  const method = Reflect.get(target, name);
  if (typeof method !== 'function') throw new Error(`Missing method: ${name}`);
  return Reflect.apply(method, target, args);
}

// ============================================================================
// Schema corpus (mirrors the fixtures used across the existing suites)
// ============================================================================

type FieldKind = 'enum' | 'category' | 'text' | 'number' | 'boolean' | 'bigUint64' | 'binary';

interface FieldSpec {
  name: string;
  kind: FieldKind;
  enumValues?: readonly string[];
  /** Never written — asserts allocation-laziness parity. */
  skip?: boolean;
}

interface ParityCase {
  name: string;
  schema: ColumnSchema;
  fields: readonly FieldSpec[];
  preallocatedColumns?: readonly string[];
}

const WIDE_ENUM = Array.from({ length: 300 }, (_, i) => `v${i}`);

const CASES: readonly ParityCase[] = [
  {
    name: 'all lazy types',
    schema: new ColumnSchema({
      status: S.enum(['pending', 'active', 'completed'] as const),
      userId: S.category(),
      errorMsg: S.text(),
      count: S.number(),
      isActive: S.boolean(),
      timestamp: S.bigUint64(),
      payload: S.binary(),
      untouched: S.number(),
    }),
    fields: [
      { name: 'status', kind: 'enum', enumValues: ['pending', 'active', 'completed'] },
      { name: 'userId', kind: 'category' },
      { name: 'errorMsg', kind: 'text' },
      { name: 'count', kind: 'number' },
      { name: 'isActive', kind: 'boolean' },
      { name: 'timestamp', kind: 'bigUint64' },
      { name: 'payload', kind: 'binary' },
      { name: 'untouched', kind: 'number', skip: true },
    ],
  },
  {
    name: 'all eager types',
    schema: new ColumnSchema({
      status: S.enum(['x', 'y'] as const).eager(),
      userId: S.category().eager(),
      errorMsg: S.text().eager(),
      count: S.number().eager(),
      isActive: S.boolean().eager(),
      timestamp: S.bigUint64().eager(),
      payload: S.binary().eager(),
    }),
    fields: [
      { name: 'status', kind: 'enum', enumValues: ['x', 'y'] },
      { name: 'userId', kind: 'category' },
      { name: 'errorMsg', kind: 'text' },
      { name: 'count', kind: 'number' },
      { name: 'isActive', kind: 'boolean' },
      { name: 'timestamp', kind: 'bigUint64' },
      { name: 'payload', kind: 'binary' },
    ],
  },
  {
    name: 'mixed with preallocated columns',
    schema: new ColumnSchema({
      status: S.enum(['a', 'b', 'c'] as const),
      userId: S.category(),
      count: S.number(),
      isActive: S.boolean(),
      payload: S.binary(),
      message: S.category().eager(),
    }),
    fields: [
      { name: 'status', kind: 'enum', enumValues: ['a', 'b', 'c'] },
      { name: 'userId', kind: 'category' },
      { name: 'count', kind: 'number' },
      { name: 'isActive', kind: 'boolean' },
      { name: 'payload', kind: 'binary' },
      { name: 'message', kind: 'category' },
    ],
    preallocatedColumns: ['status', 'userId', 'count', 'isActive', 'payload'],
  },
  {
    name: 'wide enum uses Uint16 lanes',
    schema: new ColumnSchema({
      wide: S.enum(WIDE_ENUM),
      count: S.number(),
    }),
    fields: [
      { name: 'wide', kind: 'enum', enumValues: WIDE_ENUM },
      { name: 'count', kind: 'number' },
    ],
  },
];

// ============================================================================
// Deterministic write scripts
// ============================================================================

const CAPACITY = 16;
const ROWS = 10;

function bufferValueFor(field: FieldSpec, pos: number): unknown {
  switch (field.kind) {
    case 'enum':
      return pos % (field.enumValues?.length ?? 1);
    case 'category':
      return `cat-${pos % 3}`;
    case 'text':
      return `text-${pos}`;
    case 'number':
      return pos * 1.5;
    case 'boolean':
      return pos % 2 === 0;
    case 'bigUint64':
      return BigInt(pos) * 1000n;
    case 'binary':
      return pos % 2 === 0 ? new Uint8Array([pos, pos + 1]) : { idx: pos, tag: `bin-${pos}` };
  }
}

function writerValueFor(field: FieldSpec, pos: number): unknown {
  if (field.kind === 'enum') {
    const enumValues = field.enumValues ?? [];
    return enumValues[pos % enumValues.length];
  }
  return bufferValueFor(field, pos);
}

/** Direct setter script: dense rows, then explicit null writes at 2 and 5. */
function fillViaSetters(buffer: AnyColumnBuffer, fields: readonly FieldSpec[]): void {
  for (let pos = 0; pos < ROWS; pos++) {
    for (const field of fields) {
      if (field.skip) continue;
      callMethod(buffer, field.name, pos, bufferValueFor(field, pos));
    }
  }
  for (const field of fields) {
    if (field.skip) continue;
    callMethod(buffer, field.name, 2, null);
    callMethod(buffer, field.name, 5, null);
  }
}

/** Fluent writer script over `rows` rows (spills into `_overflow` chains). */
function fillViaWriter(
  schema: ColumnSchema,
  buffer: AnyColumnBuffer,
  fields: readonly FieldSpec[],
  rows: number,
): void {
  const writer = createColumnWriter(schema, buffer);
  for (let pos = 0; pos < rows; pos++) {
    callMethod(writer, 'nextRow');
    for (const field of fields) {
      if (field.skip) continue;
      callMethod(writer, field.name, writerValueFor(field, pos));
    }
  }
}

// ============================================================================
// Buffer snapshots (byte-level for TypedArray lanes)
// ============================================================================

function snapshotColumnValues(values: unknown): unknown {
  if (values === undefined) return undefined;
  if (Array.isArray(values)) {
    return values.map((v) => (v instanceof Uint8Array ? { bytes: Array.from(v) } : v));
  }
  if (ArrayBuffer.isView(values) && !(values instanceof DataView)) {
    return {
      ctor: values.constructor.name,
      bytes: Array.from(new Uint8Array(values.buffer, values.byteOffset, values.byteLength)),
    };
  }
  return { unexpected: String(values) };
}

function snapshotBuffer(buffer: AnyColumnBuffer, fields: readonly FieldSpec[]): Record<string, unknown> {
  const snapshot: Record<string, unknown> = {
    capacity: buffer._capacity,
    alignedCapacity: Reflect.get(buffer, '_alignedCapacity'),
  };
  for (const field of fields) {
    const nulls = buffer.getNullsIfAllocated(field.name);
    snapshot[`${field.name}.allocated`] = buffer.getColumnIfAllocated(field.name) !== undefined;
    snapshot[`${field.name}.nulls`] = nulls === undefined ? undefined : Array.from(nulls);
    snapshot[`${field.name}.values`] = snapshotColumnValues(buffer.getColumnIfAllocated(field.name));
    snapshot[`${field.name}.enumValues`] = Reflect.get(buffer, `${field.name}_enumValues`);
    snapshot[`${field.name}.setterIsFunction`] = typeof Reflect.get(buffer, field.name) === 'function';
  }
  return snapshot;
}

function materializeAndFill(mode: MaterializerMode, testCase: ParityCase): Record<string, unknown> {
  return withMode(mode, () => {
    const extension = testCase.preallocatedColumns ? { preallocatedColumns: testCase.preallocatedColumns } : undefined;
    const buffer = createGeneratedColumnBuffer(testCase.schema, CAPACITY, extension);
    fillViaSetters(buffer, testCase.fields);
    return snapshotBuffer(buffer, testCase.fields);
  });
}

// ============================================================================
// Capability probe
// ============================================================================

describe('codegen capability probe', () => {
  it('detects string codegen support under bun and defaults to the compiled materializer', () => {
    expect(isStringCodegenSupported()).toBe(true);
    expect(activeMaterializerMode()).toBe('compiled');
  });

  it('honors the override hook and restores probe-based selection', () => {
    setMaterializerModeOverride('closure');
    expect(activeMaterializerMode()).toBe('closure');
    setMaterializerModeOverride(undefined);
    expect(activeMaterializerMode()).toBe('compiled');
  });

  it('caches classes per materializer mode', () => {
    const schema = new ColumnSchema({ count: S.number() });
    const Compiled = withMode('compiled', () => getColumnBufferClass(schema));
    const Closure = withMode('closure', () => getColumnBufferClass(schema));
    expect(Compiled).not.toBe(Closure);
    expect(withMode('compiled', () => getColumnBufferClass(schema))).toBe(Compiled);
    expect(withMode('closure', () => getColumnBufferClass(schema))).toBe(Closure);

    const CompiledWriter = withMode('compiled', () => getColumnWriterClass(schema));
    const ClosureWriter = withMode('closure', () => getColumnWriterClass(schema));
    expect(CompiledWriter).not.toBe(ClosureWriter);
    expect(withMode('closure', () => getColumnWriterClass(schema))).toBe(ClosureWriter);
  });
});

// ============================================================================
// ColumnBuffer parity
// ============================================================================

describe('ColumnBuffer materializer parity', () => {
  for (const testCase of CASES) {
    it(`produces byte-identical buffers: ${testCase.name}`, () => {
      const compiled = materializeAndFill('compiled', testCase);
      const closure = materializeAndFill('closure', testCase);
      expect(closure).toEqual(compiled);
    });
  }

  it('keeps untouched lazy columns unallocated in both materializers', () => {
    const testCase = CASES[0];
    for (const mode of ['compiled', 'closure'] as const) {
      withMode(mode, () => {
        const buffer = createGeneratedColumnBuffer(testCase.schema, CAPACITY);
        expect(buffer.getColumnIfAllocated('untouched')).toBeUndefined();
        expect(buffer.getNullsIfAllocated('untouched')).toBeUndefined();
      });
    }
  });

  it('shares one prototype across closure-composed instances of a schema', () => {
    const schema = new ColumnSchema({ count: S.number() });
    withMode('closure', () => {
      const BufferClass = getColumnBufferClass(schema);
      const a = new BufferClass(8);
      const b = new BufferClass(8);
      expect(Object.getPrototypeOf(a)).toBe(Object.getPrototypeOf(b));
      expect(Object.getPrototypeOf(a)).toBe(BufferClass.prototype);
    });
  });

  it('rejects code-string extensions in the closure materializer with a descriptive error', () => {
    const schema = new ColumnSchema({ count: S.number() });
    withMode('closure', () => {
      expect(() => getColumnBufferClass(schema, { methods: 'ping() { return 1; }' })).toThrow(/methods/);
      expect(() => getColumnBufferClass(schema, { constructorCode: 'this._x = 1;' })).toThrow(/string codegen/);
      expect(() => getColumnWriterClass(schema, { classPreamble: 'const X = 1;' })).toThrow(/classPreamble/);
    });
    // The same extension still compiles where string codegen is allowed.
    withMode('compiled', () => {
      expect(() => getColumnBufferClass(schema, { methods: 'ping() { return 1; }' })).not.toThrow();
    });
  });
});

// ============================================================================
// ColumnWriter parity
// ============================================================================

describe('ColumnWriter materializer parity', () => {
  for (const testCase of CASES) {
    it(`produces byte-identical buffers through the fluent writer: ${testCase.name}`, () => {
      const run = (mode: MaterializerMode) =>
        withMode(mode, () => {
          const extension = testCase.preallocatedColumns
            ? { preallocatedColumns: testCase.preallocatedColumns }
            : undefined;
          const buffer = createGeneratedColumnBuffer(testCase.schema, CAPACITY, extension);
          fillViaWriter(testCase.schema, buffer, testCase.fields, ROWS);
          return snapshotBuffer(buffer, testCase.fields);
        });
      expect(run('closure')).toEqual(run('compiled'));
    });
  }

  it('handles overflow chaining identically', () => {
    const testCase = CASES[0];
    const run = (mode: MaterializerMode) =>
      withMode(mode, () => {
        const first = createGeneratedColumnBuffer(testCase.schema, 8);
        const second = createGeneratedColumnBuffer(testCase.schema, 8);
        first._overflow = second;
        fillViaWriter(testCase.schema, first, testCase.fields, 12);
        return [snapshotBuffer(first, testCase.fields), snapshotBuffer(second, testCase.fields)];
      });
    expect(run('closure')).toEqual(run('compiled'));
  });

  it('throws the identical error for invalid enum values', () => {
    const schema = new ColumnSchema({ status: S.enum(['ok', 'error'] as const) });
    const messageFor = (mode: MaterializerMode) =>
      withMode(mode, () => {
        const buffer = createGeneratedColumnBuffer(schema, 8);
        const writer = createColumnWriter(schema, buffer);
        callMethod(writer, 'nextRow');
        try {
          callMethod(writer, 'status', 'bogus');
        } catch (error) {
          return error instanceof Error ? error.message : String(error);
        }
        throw new Error('expected the enum setter to throw');
      });
    expect(messageFor('closure')).toBe(messageFor('compiled'));
    expect(messageFor('closure')).toContain('Invalid enum value "bogus" for field "status"');
  });

  it('freezes binary object payloads in both materializers', () => {
    const schema = new ColumnSchema({ payload: S.binary() });
    for (const mode of ['compiled', 'closure'] as const) {
      withMode(mode, () => {
        const buffer = createGeneratedColumnBuffer(schema, 8);
        const writer = createColumnWriter(schema, buffer);
        callMethod(writer, 'nextRow');
        const payload = { a: 1 };
        callMethod(writer, 'payload', payload);
        expect(Object.isFrozen(payload)).toBe(true);
        const raw = new Uint8Array([1, 2, 3]);
        callMethod(writer, 'payload', raw);
        expect(Object.isFrozen(raw)).toBe(false);
      });
    }
  });

  it('lets a closure-composed writer drive a compiled buffer', () => {
    const testCase = CASES[0];
    const compiledBuffer = withMode('compiled', () => createGeneratedColumnBuffer(testCase.schema, CAPACITY));
    withMode('closure', () => {
      fillViaWriter(testCase.schema, compiledBuffer, testCase.fields, ROWS);
    });
    const reference = withMode('compiled', () => {
      const buffer = createGeneratedColumnBuffer(testCase.schema, CAPACITY);
      fillViaWriter(testCase.schema, buffer, testCase.fields, ROWS);
      return snapshotBuffer(buffer, testCase.fields);
    });
    expect(snapshotBuffer(compiledBuffer, testCase.fields)).toEqual(reference);
  });
});
