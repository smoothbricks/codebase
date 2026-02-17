import { describe, expect, it } from 'bun:test';
import { S } from '../../schema/builder.js';
import { ColumnSchema } from '../../schema/ColumnSchema.js';
import { createGeneratedColumnBuffer } from '../columnBufferGenerator.js';
import {
  type ColumnWriter,
  createColumnWriter,
  generateColumnWriterClass,
  getColumnWriterClass,
} from '../columnWriterGenerator.js';

// Test schemas with proper metadata markers
// Cast to SchemaFields to work around strict type checking in tests
// In real usage, these would be proper Sury schemas with __schema_type metadata
const testSchema = new ColumnSchema({
  userId: S.category(),
  status: S.enum(['ok', 'error'] as const),
  count: S.number(),
  enabled: S.boolean(),
  message: S.text(),
});

describe('generateColumnWriterClass', () => {
  it('generates valid class code', () => {
    const code = generateColumnWriterClass(testSchema);
    expect(code).toContain('class GeneratedColumnWriter');
    expect(code).toContain('nextRow()');
    expect(code).toContain('_getNextBuffer()');
    expect(code).toContain('userId(value)');
    expect(code).toContain('status(value)');
    expect(code).toContain('count(value)');
    expect(code).toContain('enabled(value)');
    expect(code).toContain('message(value)');
  });

  it('generates code with extension classPreamble', () => {
    const code = generateColumnWriterClass(testSchema, 'TestWriter', {
      classPreamble: 'const FOO = 42;',
    });
    expect(code).toContain('const FOO = 42;');
    // classPreamble should appear before class definition
    const classPreambleIdx = code.indexOf('const FOO = 42;');
    const classIdx = code.indexOf('class TestWriter');
    expect(classPreambleIdx).toBeLessThan(classIdx);
  });

  it('generates code with extension constructor params', () => {
    const code = generateColumnWriterClass(testSchema, 'TestWriter', {
      constructorParams: 'scope, context',
      constructorCode: 'this._scope = scope;',
    });
    expect(code).toContain('constructor(buffer, scope, context)');
    expect(code).toContain('this._scope = scope;');
  });

  it('generates code with extension methods', () => {
    const code = generateColumnWriterClass(testSchema, 'TestWriter', {
      methods: `
info() {
  return this;
}`,
    });
    expect(code).toContain('info()');
  });
});

describe('getColumnWriterClass', () => {
  it('returns a class constructor', () => {
    const WriterClass = getColumnWriterClass(testSchema);
    expect(typeof WriterClass).toBe('function');
  });

  it('caches class for same schema', () => {
    const WriterClass1 = getColumnWriterClass(testSchema);
    const WriterClass2 = getColumnWriterClass(testSchema);
    expect(WriterClass1).toBe(WriterClass2);
  });

  it('creates different classes for different schemas', () => {
    const schema1 = new ColumnSchema({ foo: S.number() });
    const schema2 = new ColumnSchema({ bar: S.number() });
    const WriterClass1 = getColumnWriterClass(schema1);
    const WriterClass2 = getColumnWriterClass(schema2);
    expect(WriterClass1).not.toBe(WriterClass2);
  });
});

describe('createColumnWriter', () => {
  it('creates a writer instance', () => {
    const buffer = createGeneratedColumnBuffer(testSchema, 64);
    const writer = createColumnWriter(testSchema, buffer);
    expect(writer._buffer).toBe(buffer);
    expect(writer._writeIndex).toBe(-1);
  });

  it('writes to buffer with nextRow and setters', () => {
    const buffer = createGeneratedColumnBuffer(testSchema, 64);
    const writer = createColumnWriter(testSchema, buffer);

    // First row
    writer.nextRow();
    writer.userId('user1');
    writer.count(42);

    expect(writer._writeIndex).toBe(0);
    expect(buffer.userId_values[0]).toBe('user1');
    expect(buffer.count_values[0]).toBe(42);

    // Second row
    writer.nextRow();
    writer.userId('user2');
    writer.count(100);

    expect(writer._writeIndex).toBe(1);
    expect(buffer.userId_values[1]).toBe('user2');
    expect(buffer.count_values[1]).toBe(100);
  });

  it('supports fluent chaining', () => {
    const buffer = createGeneratedColumnBuffer(testSchema, 64);
    const writer = createColumnWriter(testSchema, buffer);

    const result = writer.nextRow().userId('user1').status('ok').count(42).message('hello');

    expect(result).toBe(writer);
  });

  it('handles boolean bit-packing', () => {
    const buffer = createGeneratedColumnBuffer(testSchema, 64);
    const writer = createColumnWriter(testSchema, buffer);

    // Write multiple booleans to test bit-packing
    for (let i = 0; i < 16; i++) {
      writer.nextRow();
      // biome-ignore lint/suspicious/noExplicitAny: testing dynamic methods
      (writer as any).enabled(i % 2 === 0); // alternating true/false
    }

    const enabledValues = (buffer as unknown as { enabled_values: Uint8Array }).enabled_values;

    // First byte should have bits 0, 2, 4, 6 set (even indices = true)
    // Bit pattern: 01010101 = 0x55
    expect(enabledValues[0]).toBe(0x55);

    // Second byte should have bits 8, 10, 12, 14 set (indices 8-15, even = true)
    // Bit pattern: 01010101 = 0x55
    expect(enabledValues[1]).toBe(0x55);
  });

  it('handles overflow with _getNextBuffer', () => {
    const buffer1 = createGeneratedColumnBuffer(testSchema, 2);
    const buffer2 = createGeneratedColumnBuffer(testSchema, 2);
    buffer1._overflow = buffer2;

    const writer = createColumnWriter(testSchema, buffer1);

    // Fill first buffer
    writer.nextRow(); // index 0
    writer.nextRow(); // index 1

    // Next should overflow to buffer2
    writer.nextRow(); // should trigger overflow

    expect(writer._buffer).toBe(buffer2);
    expect(writer._writeIndex).toBe(0);
  });

  it('throws on overflow without next buffer', () => {
    const buffer = createGeneratedColumnBuffer(testSchema, 2);
    const writer = createColumnWriter(testSchema, buffer);

    writer.nextRow(); // index 0
    writer.nextRow(); // index 1

    expect(() => writer.nextRow()).toThrow('Buffer overflow');
  });

  it('supports extension constructor params', () => {
    const buffer = createGeneratedColumnBuffer(testSchema, 64);

    const writer = createColumnWriter(
      testSchema,
      buffer,
      {
        constructorParams: 'customValue',
        constructorCode: 'this._custom = customValue;',
      },
      'test-value',
    );

    expect((writer as ColumnWriter<typeof testSchema> & { _custom: string })._custom).toBe('test-value');
  });

  it('supports extension dependencies', () => {
    const buffer = createGeneratedColumnBuffer(testSchema, 64);
    const transformFn = (s: string) => s.toUpperCase();

    const writer = createColumnWriter(testSchema, buffer, {
      classPreamble: '',
      methods: `
transformed(value) {
  const idx = this._writeIndex;
  this._buffer.message_nulls[idx >>> 3] |= 1 << (idx & 7);
  this._buffer.message_values[idx] = transform(value);
  return this;
}`,
      dependencies: { transform: transformFn },
    });

    writer.nextRow();
    // biome-ignore lint/suspicious/noExplicitAny: testing dynamic methods
    (writer as any).transformed('hello');

    expect((buffer as unknown as { message_values: string[] }).message_values[0]).toBe('HELLO');
  });

  it('writes eager columns without accessing null bitmaps', () => {
    const eagerSchema = new ColumnSchema({
      message: S.text().eager(),
      enabled: S.boolean().eager(),
    });
    const buffer = createGeneratedColumnBuffer(eagerSchema, 16);
    const writer = createColumnWriter(eagerSchema, buffer);

    writer.nextRow().message('hello').enabled(true);
    writer.nextRow().message('world').enabled(false);

    expect(buffer.message_values[0]).toBe('hello');
    expect(buffer.message_values[1]).toBe('world');
    expect(buffer.enabled_values[0] & 0b01).toBe(0b01);
    expect(buffer.enabled_values[0] & 0b10).toBe(0);
  });
});

describe('enum O(1) Map lookup', () => {
  it('uses Map.get instead of indexOf for enum writes', () => {
    const code = generateColumnWriterClass(testSchema);
    // WHY: Verify the generated code uses O(1) Map lookup, not O(N) indexOf
    expect(code).toContain('_enumLookup.get(value)');
    expect(code).not.toContain('indexOf(value)');
  });

  it('generates enum lookup Map in constructor', () => {
    const code = generateColumnWriterClass(testSchema);
    // The constructor should build a Map from enumValues
    expect(code).toContain('_status_enumLookup');
    expect(code).toContain('new Map');
  });

  it('correctly writes enum values via Map lookup', () => {
    const buffer = createGeneratedColumnBuffer(testSchema, 64);
    const writer = createColumnWriter(testSchema, buffer);

    writer.nextRow().status('ok');
    writer.nextRow().status('error');

    expect(buffer.status_values[0]).toBe(0); // 'ok' is index 0
    expect(buffer.status_values[1]).toBe(1); // 'error' is index 1
  });

  it('throws on invalid enum value with Map lookup', () => {
    const buffer = createGeneratedColumnBuffer(testSchema, 64);
    const writer = createColumnWriter(testSchema, buffer);

    writer.nextRow();
    // biome-ignore lint/suspicious/noExplicitAny: testing invalid value
    expect(() => writer.status('invalid' as any)).toThrow('Invalid enum value');
  });

  it('handles high-cardinality enum (64 values) via Map lookup', () => {
    // WHY: Verifies O(1) Map lookup works for high-cardinality enums
    // where indexOf would be measurably slow
    const values = Array.from({ length: 64 }, (_, i) => `value_${i}`) as [string, ...string[]];
    const highCardSchema = new ColumnSchema({
      status: S.enum(values),
    });
    const buffer = createGeneratedColumnBuffer(highCardSchema, 64);
    const writer = createColumnWriter(highCardSchema, buffer);

    // Write every value
    for (let i = 0; i < 64; i++) {
      writer.nextRow();
      // biome-ignore lint/suspicious/noExplicitAny: testing dynamic methods
      (writer as any).status(`value_${i}`);
    }

    // Verify each value got the correct index
    for (let i = 0; i < 64; i++) {
      expect(buffer.status_values[i]).toBe(i);
    }
  });
});

describe('preamble contract split', () => {
  it('classPreamble appears before class definition in ColumnWriter', () => {
    const code = generateColumnWriterClass(testSchema, 'TestWriter', {
      classPreamble: 'const MAGIC = 99;',
    });
    const preambleIdx = code.indexOf('const MAGIC = 99;');
    const classIdx = code.indexOf('class TestWriter');
    expect(preambleIdx).toBeGreaterThan(-1);
    expect(classIdx).toBeGreaterThan(-1);
    expect(preambleIdx).toBeLessThan(classIdx);
  });

  it('constructorPreamble appears inside constructor body in ColumnWriter', () => {
    const code = generateColumnWriterClass(testSchema, 'TestWriter', {
      constructorPreamble: 'const SETUP = true;',
    });
    const preambleIdx = code.indexOf('const SETUP = true;');
    const constructorIdx = code.indexOf('constructor(');
    const firstBrace = code.indexOf('{', constructorIdx);
    expect(preambleIdx).toBeGreaterThan(firstBrace);
  });
});

describe('null bitmap handling', () => {
  it('sets null bit when writing value', () => {
    const buffer = createGeneratedColumnBuffer(testSchema, 64);
    const writer = createColumnWriter(testSchema, buffer);

    writer.nextRow();
    // biome-ignore lint/suspicious/noExplicitAny: testing dynamic methods
    (writer as any).userId('test');

    const nullBitmap = (buffer as unknown as { userId_nulls: Uint8Array }).userId_nulls;
    // Bit 0 should be set (first row written)
    expect(nullBitmap[0] & 0x01).toBe(1);
  });

  it('leaves null bit unset for unwritten values', () => {
    const buffer = createGeneratedColumnBuffer(testSchema, 64);
    const writer = createColumnWriter(testSchema, buffer);

    writer.nextRow();
    // Only write userId, not status
    // biome-ignore lint/suspicious/noExplicitAny: testing dynamic methods
    (writer as any).userId('test');

    const statusNulls = (buffer as unknown as { status_nulls: Uint8Array }).status_nulls;
    // Bit 0 should be unset (status was not written)
    expect(statusNulls[0] & 0x01).toBe(0);
  });
});
