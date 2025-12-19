import { describe, expect, it } from 'bun:test';
import { S } from '../../schema/builder.js';
import { ColumnSchema } from '../../schema/ColumnSchema.js';
import type { SchemaFields } from '../../schema-types.js';
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

  it('generates code with extension preamble', () => {
    const code = generateColumnWriterClass(testSchema, 'TestWriter', {
      preamble: 'const FOO = 42;',
    });
    expect(code).toContain('const FOO = 42;');
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
    (writer as ColumnWriter & { userId(v: string): ColumnWriter }).userId('user1');
    (writer as ColumnWriter & { count(v: number): ColumnWriter }).count(42);

    expect(writer._writeIndex).toBe(0);
    expect((buffer as unknown as { userId_values: string[] }).userId_values[0]).toBe('user1');
    expect((buffer as unknown as { count_values: Float64Array }).count_values[0]).toBe(42);

    // Second row
    writer.nextRow();
    (writer as ColumnWriter & { userId(v: string): ColumnWriter }).userId('user2');
    (writer as ColumnWriter & { count(v: number): ColumnWriter }).count(100);

    expect(writer._writeIndex).toBe(1);
    expect((buffer as unknown as { userId_values: string[] }).userId_values[1]).toBe('user2');
    expect((buffer as unknown as { count_values: Float64Array }).count_values[1]).toBe(100);
  });

  it('supports fluent chaining', () => {
    const buffer = createGeneratedColumnBuffer(testSchema, 64);
    const writer = createColumnWriter(testSchema, buffer);

    // Use explicit type assertions for dynamic methods
    // biome-ignore lint/suspicious/noExplicitAny: testing dynamic methods
    const w = writer as any;
    const result = w.nextRow().userId('user1').status(0).count(42).message('hello');

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
    buffer1._next = buffer2;

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

    expect((writer as ColumnWriter & { _custom: string })._custom).toBe('test-value');
  });

  it('supports extension dependencies', () => {
    const buffer = createGeneratedColumnBuffer(testSchema, 64);
    const transformFn = (s: string) => s.toUpperCase();

    const writer = createColumnWriter(testSchema, buffer, {
      preamble: '',
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
