/**
 * Arrow Binary Compatibility Tests
 *
 * This test suite verifies that arrow-builder's TypedArray-based buffers produce
 * Arrow-compatible data that:
 * 1. Can be serialized to IPC binary format using apache-arrow
 * 2. Can be deserialized back to identical data
 * 3. Matches the expected null bitmap format (1=valid, 0=null)
 * 4. Works correctly with both eager and lazy columns
 * 5. Handles all supported types (enum, category, text, number, boolean)
 *
 * This is the arrow-builder package's comprehensive test for Arrow format correctness.
 * Unlike lmao's tests which focus on span-specific conversion, these tests focus on
 * the low-level buffer compat-arrow format.
 */

import { describe, expect, it } from 'bun:test';
import * as arrow from 'apache-arrow';
import { S } from '../../schema/builder.js';
import { createGeneratedColumnBuffer } from '../columnBufferGenerator.js';

/**
 * Helper to create Arrow vector from buffer column data
 */
function createEnumVector(
  indices: Uint8Array,
  length: number,
  enumValues: readonly string[],
  nullBitmap?: Uint8Array,
): arrow.Vector {
  const encoder = new TextEncoder();

  // Calculate offsets
  const offsets = new Int32Array(enumValues.length + 1);
  offsets[0] = 0;
  let totalBytes = 0;
  for (let i = 0; i < enumValues.length; i++) {
    totalBytes += encoder.encode(enumValues[i]).length;
    offsets[i + 1] = totalBytes;
  }

  // Build concatenated UTF-8 data
  const data = new Uint8Array(totalBytes);
  let offset = 0;
  for (const value of enumValues) {
    const encoded = encoder.encode(value);
    data.set(encoded, offset);
    offset += encoded.length;
  }

  // Create dictionary data
  const dictData = arrow.makeData({
    type: new arrow.Utf8(),
    offset: 0,
    length: enumValues.length,
    nullCount: 0,
    valueOffsets: offsets,
    data,
  });

  // Count nulls
  let nullCount = 0;
  if (nullBitmap) {
    for (let i = 0; i < length; i++) {
      const byteIndex = i >>> 3;
      const bitOffset = i & 7;
      if ((nullBitmap[byteIndex] & (1 << bitOffset)) === 0) {
        nullCount++;
      }
    }
  }

  // Create dictionary-encoded data
  const enumData = arrow.makeData({
    type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint8()),
    offset: 0,
    length,
    nullCount,
    data: indices.subarray(0, length),
    nullBitmap: nullBitmap ? nullBitmap.subarray(0, Math.ceil(length / 8)) : undefined,
    dictionary: arrow.makeVector(dictData),
  });

  return arrow.makeVector(enumData);
}

function createCategoryVector(values: string[], length: number, nullBitmap?: Uint8Array): arrow.Vector {
  const encoder = new TextEncoder();

  // Build dictionary from unique values (sorted for category)
  const uniqueValues = [...new Set(values.slice(0, length).filter((v) => v != null))].sort();
  const valueToIndex = new Map(uniqueValues.map((v, i) => [v, i]));

  // Calculate offsets
  const offsets = new Int32Array(uniqueValues.length + 1);
  offsets[0] = 0;
  let totalBytes = 0;
  for (let i = 0; i < uniqueValues.length; i++) {
    totalBytes += encoder.encode(uniqueValues[i]).length;
    offsets[i + 1] = totalBytes;
  }

  // Build concatenated UTF-8 data
  const data = new Uint8Array(totalBytes);
  let offset = 0;
  for (const value of uniqueValues) {
    const encoded = encoder.encode(value);
    data.set(encoded, offset);
    offset += encoded.length;
  }

  // Create dictionary data
  const dictData = arrow.makeData({
    type: new arrow.Utf8(),
    offset: 0,
    length: uniqueValues.length,
    nullCount: 0,
    valueOffsets: offsets,
    data,
  });

  // Build indices
  const indices = new Uint32Array(length);
  let nullCount = 0;
  for (let i = 0; i < length; i++) {
    const value = values[i];
    if (value != null) {
      indices[i] = valueToIndex.get(value) ?? 0;
    } else {
      indices[i] = 0;
      nullCount++;
    }
  }

  // Count nulls from bitmap if provided
  if (nullBitmap) {
    nullCount = 0;
    for (let i = 0; i < length; i++) {
      const byteIndex = i >>> 3;
      const bitOffset = i & 7;
      if ((nullBitmap[byteIndex] & (1 << bitOffset)) === 0) {
        nullCount++;
      }
    }
  }

  const categoryData = arrow.makeData({
    type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint32()),
    offset: 0,
    length,
    nullCount,
    data: indices,
    nullBitmap: nullBitmap ? nullBitmap.subarray(0, Math.ceil(length / 8)) : undefined,
    dictionary: arrow.makeVector(dictData),
  });

  return arrow.makeVector(categoryData);
}

function createNumberVector(values: Float64Array, length: number, nullBitmap?: Uint8Array): arrow.Vector {
  let nullCount = 0;
  if (nullBitmap) {
    for (let i = 0; i < length; i++) {
      const byteIndex = i >>> 3;
      const bitOffset = i & 7;
      if ((nullBitmap[byteIndex] & (1 << bitOffset)) === 0) {
        nullCount++;
      }
    }
  }

  const numberData = arrow.makeData({
    type: new arrow.Float64(),
    offset: 0,
    length,
    nullCount,
    data: values.subarray(0, length),
    nullBitmap: nullBitmap ? nullBitmap.subarray(0, Math.ceil(length / 8)) : undefined,
  });

  return arrow.makeVector(numberData);
}

function createBooleanVector(values: Uint8Array, length: number, nullBitmap?: Uint8Array): arrow.Vector {
  let nullCount = 0;
  if (nullBitmap) {
    for (let i = 0; i < length; i++) {
      const byteIndex = i >>> 3;
      const bitOffset = i & 7;
      if ((nullBitmap[byteIndex] & (1 << bitOffset)) === 0) {
        nullCount++;
      }
    }
  }

  const boolData = arrow.makeData({
    type: new arrow.Bool(),
    offset: 0,
    length,
    nullCount,
    data: values.subarray(0, Math.ceil(length / 8)),
    nullBitmap: nullBitmap ? nullBitmap.subarray(0, Math.ceil(length / 8)) : undefined,
  });

  return arrow.makeVector(boolData);
}

function roundTripVerify(table: arrow.Table): arrow.Table {
  // Serialize to IPC format
  const ipcBytes = arrow.tableToIPC(table);

  // Deserialize back
  const roundTripped = arrow.tableFromIPC(ipcBytes);

  // Verify schema matches
  expect(roundTripped.schema.fields.length).toBe(table.schema.fields.length);
  for (let i = 0; i < table.schema.fields.length; i++) {
    expect(roundTripped.schema.fields[i].name).toBe(table.schema.fields[i].name);
    expect(roundTripped.schema.fields[i].typeId).toBe(table.schema.fields[i].typeId);
    expect(roundTripped.schema.fields[i].nullable).toBe(table.schema.fields[i].nullable);
  }

  // Verify row count matches
  expect(roundTripped.numRows).toBe(table.numRows);

  return roundTripped;
}

describe('Arrow Binary Compatibility - Enum Columns', () => {
  it('writes and reads enum values correctly', () => {
    const schema = {
      status: S.enum(['pending', 'active', 'completed'] as const),
    };

    const buffer = createGeneratedColumnBuffer(schema, 16);
    const enumValues = ['pending', 'active', 'completed'] as const;

    // Write some values using numeric indices (buffer expects indices, not strings)
    // The TagWriter/SpanLogger layer handles string→index conversion
    buffer.status(0, 0); // pending = 0
    buffer.status(1, 1); // active = 1
    buffer.status(2, 2); // completed = 2
    buffer.status(3, 0); // pending = 0
    buffer.status(4, 1); // active = 1

    const length = 5;
    const vector = createEnumVector(buffer.status_values as Uint8Array, length, enumValues, buffer.status_nulls);

    const arrowSchema = new arrow.Schema([
      arrow.Field.new({
        name: 'status',
        type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint8()),
        nullable: true,
      }),
    ]);

    const batch = new arrow.RecordBatch(
      arrowSchema,
      arrow.makeData({
        type: new arrow.Struct(arrowSchema.fields),
        length,
        nullCount: 0,
        children: [vector.data[0]],
      }),
    );

    const table = new arrow.Table([batch]);
    const roundTripped = roundTripVerify(table);

    // Verify values
    expect(roundTripped.get(0)?.toJSON().status).toBe('pending');
    expect(roundTripped.get(1)?.toJSON().status).toBe('active');
    expect(roundTripped.get(2)?.toJSON().status).toBe('completed');
    expect(roundTripped.get(3)?.toJSON().status).toBe('pending');
    expect(roundTripped.get(4)?.toJSON().status).toBe('active');
  });

  // TODO: S.enum().eager() not yet implemented
  it.skip('handles eager enum columns', () => {
    // When S.enum().eager() is implemented, uncomment and test
    expect(true).toBe(true);
  });
});

describe('Arrow Binary Compatibility - Category Columns', () => {
  it('writes and reads category values correctly', () => {
    const schema = {
      userId: S.category(),
    };

    const buffer = createGeneratedColumnBuffer(schema, 16);

    buffer.userId(0, 'user-alice');
    buffer.userId(1, 'user-bob');
    buffer.userId(2, 'user-alice'); // Duplicate
    buffer.userId(3, 'user-charlie');
    buffer.userId(4, 'user-bob'); // Duplicate

    const length = 5;
    const vector = createCategoryVector(buffer.userId_values as string[], length, buffer.userId_nulls);

    const arrowSchema = new arrow.Schema([
      arrow.Field.new({
        name: 'userId',
        type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint32()),
        nullable: true,
      }),
    ]);

    const batch = new arrow.RecordBatch(
      arrowSchema,
      arrow.makeData({
        type: new arrow.Struct(arrowSchema.fields),
        length,
        nullCount: 0,
        children: [vector.data[0]],
      }),
    );

    const table = new arrow.Table([batch]);
    const roundTripped = roundTripVerify(table);

    expect(roundTripped.get(0)?.toJSON().userId).toBe('user-alice');
    expect(roundTripped.get(1)?.toJSON().userId).toBe('user-bob');
    expect(roundTripped.get(2)?.toJSON().userId).toBe('user-alice');
    expect(roundTripped.get(3)?.toJSON().userId).toBe('user-charlie');
    expect(roundTripped.get(4)?.toJSON().userId).toBe('user-bob');
  });

  it('handles eager category columns', () => {
    const schema = {
      message: S.category().eager(),
    };

    const buffer = createGeneratedColumnBuffer(schema, 16);

    // Eager columns are pre-allocated as arrays
    expect(Array.isArray(buffer.message_values)).toBe(true);

    buffer.message(0, 'hello');
    buffer.message(1, 'world');

    expect(buffer.message_values[0]).toBe('hello');
    expect(buffer.message_values[1]).toBe('world');

    // Eager columns don't have null bitmap
    expect(buffer.getNullsIfAllocated('message')).toBeUndefined();
  });
});

describe('Arrow Binary Compatibility - Number Columns', () => {
  it('writes and reads number values correctly', () => {
    const schema = {
      count: S.number(),
    };

    const buffer = createGeneratedColumnBuffer(schema, 16);

    buffer.count(0, 42.5);
    buffer.count(1, 100);
    buffer.count(2, -3.14159);
    buffer.count(3, 0);
    buffer.count(4, Number.MAX_SAFE_INTEGER);

    const length = 5;
    const vector = createNumberVector(buffer.count_values as Float64Array, length, buffer.count_nulls);

    const arrowSchema = new arrow.Schema([
      arrow.Field.new({ name: 'count', type: new arrow.Float64(), nullable: true }),
    ]);

    const batch = new arrow.RecordBatch(
      arrowSchema,
      arrow.makeData({
        type: new arrow.Struct(arrowSchema.fields),
        length,
        nullCount: 0,
        children: [vector.data[0]],
      }),
    );

    const table = new arrow.Table([batch]);
    const roundTripped = roundTripVerify(table);

    expect(roundTripped.get(0)?.toJSON().count).toBe(42.5);
    expect(roundTripped.get(1)?.toJSON().count).toBe(100);
    expect(roundTripped.get(2)?.toJSON().count).toBe(-3.14159);
    expect(roundTripped.get(3)?.toJSON().count).toBe(0);
    expect(roundTripped.get(4)?.toJSON().count).toBe(Number.MAX_SAFE_INTEGER);
  });

  // TODO: S.number().eager() not yet implemented
  it.skip('handles eager number columns', () => {
    // When S.number().eager() is implemented, uncomment and test
    expect(true).toBe(true);
  });
});

describe('Arrow Binary Compatibility - Boolean Columns', () => {
  it('writes and reads boolean values correctly (bit-packed)', () => {
    const schema = {
      active: S.boolean(),
    };

    const buffer = createGeneratedColumnBuffer(schema, 16);

    buffer.active(0, true);
    buffer.active(1, false);
    buffer.active(2, true);
    buffer.active(3, true);
    buffer.active(4, false);
    buffer.active(5, true);
    buffer.active(6, false);
    buffer.active(7, true);
    buffer.active(8, true); // Second byte

    const length = 9;
    const vector = createBooleanVector(buffer.active_values as Uint8Array, length, buffer.active_nulls);

    const arrowSchema = new arrow.Schema([arrow.Field.new({ name: 'active', type: new arrow.Bool(), nullable: true })]);

    const batch = new arrow.RecordBatch(
      arrowSchema,
      arrow.makeData({
        type: new arrow.Struct(arrowSchema.fields),
        length,
        nullCount: 0,
        children: [vector.data[0]],
      }),
    );

    const table = new arrow.Table([batch]);
    const roundTripped = roundTripVerify(table);

    expect(roundTripped.get(0)?.toJSON().active).toBe(true);
    expect(roundTripped.get(1)?.toJSON().active).toBe(false);
    expect(roundTripped.get(2)?.toJSON().active).toBe(true);
    expect(roundTripped.get(3)?.toJSON().active).toBe(true);
    expect(roundTripped.get(4)?.toJSON().active).toBe(false);
    expect(roundTripped.get(5)?.toJSON().active).toBe(true);
    expect(roundTripped.get(6)?.toJSON().active).toBe(false);
    expect(roundTripped.get(7)?.toJSON().active).toBe(true);
    expect(roundTripped.get(8)?.toJSON().active).toBe(true);
  });

  // TODO: S.boolean().eager() not yet implemented
  it.skip('handles eager boolean columns', () => {
    // When S.boolean().eager() is implemented, uncomment and test
    expect(true).toBe(true);
  });
});

describe('Arrow Binary Compatibility - Text Columns', () => {
  it('writes and reads text values correctly', () => {
    const schema = {
      message: S.text(),
    };

    const buffer = createGeneratedColumnBuffer(schema, 16);

    buffer.message(0, 'First message');
    buffer.message(1, 'Second message');
    buffer.message(2, 'Third message');

    const length = 3;
    const vector = createCategoryVector(buffer.message_values as string[], length, buffer.message_nulls);

    const arrowSchema = new arrow.Schema([
      arrow.Field.new({
        name: 'message',
        type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint32()),
        nullable: true,
      }),
    ]);

    const batch = new arrow.RecordBatch(
      arrowSchema,
      arrow.makeData({
        type: new arrow.Struct(arrowSchema.fields),
        length,
        nullCount: 0,
        children: [vector.data[0]],
      }),
    );

    const table = new arrow.Table([batch]);
    const roundTripped = roundTripVerify(table);

    expect(roundTripped.get(0)?.toJSON().message).toBe('First message');
    expect(roundTripped.get(1)?.toJSON().message).toBe('Second message');
    expect(roundTripped.get(2)?.toJSON().message).toBe('Third message');
  });

  it('handles eager text columns', () => {
    const schema = {
      log: S.text().eager(),
    };

    const buffer = createGeneratedColumnBuffer(schema, 16);

    expect(Array.isArray(buffer.log_values)).toBe(true);

    buffer.log(0, 'Log entry 1');
    buffer.log(1, 'Log entry 2');

    expect(buffer.log_values[0]).toBe('Log entry 1');
    expect(buffer.log_values[1]).toBe('Log entry 2');

    expect(buffer.getNullsIfAllocated('log')).toBeUndefined();
  });
});

describe('Arrow Binary Compatibility - Mixed Schema', () => {
  it('handles all column types together', () => {
    const schema = {
      status: S.enum(['pending', 'active', 'completed'] as const),
      userId: S.category(),
      errorMsg: S.text(),
      count: S.number(),
      active: S.boolean(),
    };

    const buffer = createGeneratedColumnBuffer(schema, 16);

    // Write first row (status uses numeric index)
    buffer.status(0, 0); // pending = 0
    buffer.userId(0, 'user-1').errorMsg(0, 'No error').count(0, 42).active(0, true);

    // Write second row (status uses numeric index)
    buffer.status(1, 1); // active = 1
    buffer.userId(1, 'user-2').errorMsg(1, 'Some error').count(1, 100).active(1, false);

    // Verify all columns have data
    expect(buffer.status_values[0]).toBe(0); // pending = 0
    expect(buffer.status_values[1]).toBe(1); // active = 1
    expect(buffer.userId_values[0]).toBe('user-1');
    expect(buffer.userId_values[1]).toBe('user-2');
    expect(buffer.errorMsg_values[0]).toBe('No error');
    expect(buffer.errorMsg_values[1]).toBe('Some error');
    expect(buffer.count_values[0]).toBe(42);
    expect(buffer.count_values[1]).toBe(100);
    // Boolean bit-packed: bit 0 set = true, bit 1 clear = false = 0b00000001 = 1
    expect((buffer.active_values as Uint8Array)[0]).toBe(1);
  });

  // TODO: S.number().eager() not yet implemented (S.category().eager() works)
  it.skip('handles mixed eager and lazy columns', () => {
    // When S.number().eager() is implemented, uncomment and test
    expect(true).toBe(true);
  });
});

describe('Arrow Binary Compatibility - Null Bitmap Format', () => {
  it('uses Arrow format (1=valid, 0=null) for lazy columns', () => {
    const schema = {
      value: S.number(),
    };

    const buffer = createGeneratedColumnBuffer(schema, 16);

    // Write to positions 0, 2, 4 (skip 1, 3)
    buffer.value(0, 1.0);
    buffer.value(2, 2.0);
    buffer.value(4, 3.0);

    // Null bitmap should have bits 0, 2, 4 set
    // 0b00010101 = 21
    expect(buffer.value_nulls[0]).toBe(21);
  });

  it('handles null bitmap across byte boundaries', () => {
    const schema = {
      value: S.category(),
    };

    const buffer = createGeneratedColumnBuffer(schema, 32);

    // Write to positions 0, 7, 8, 15
    buffer.value(0, 'a');
    buffer.value(7, 'b');
    buffer.value(8, 'c');
    buffer.value(15, 'd');

    // First byte: bits 0, 7 set = 0b10000001 = 129
    expect(buffer.value_nulls[0]).toBe(129);
    // Second byte: bits 0, 7 set = 0b10000001 = 129
    expect(buffer.value_nulls[1]).toBe(129);
  });
});

describe('Arrow Binary Compatibility - Large Datasets', () => {
  it('handles large number of rows', () => {
    const schema = {
      value: S.number(),
    };

    const capacity = 1024;
    const buffer = createGeneratedColumnBuffer(schema, capacity);

    // Write 1000 values
    for (let i = 0; i < 1000; i++) {
      buffer.value(i, i * 1.5);
    }

    // Verify first and last values
    expect(buffer.value_values[0]).toBe(0);
    expect(buffer.value_values[999]).toBe(999 * 1.5);

    // Create Arrow table and round-trip
    const length = 1000;
    const vector = createNumberVector(buffer.value_values as Float64Array, length, buffer.value_nulls);

    const arrowSchema = new arrow.Schema([
      arrow.Field.new({ name: 'value', type: new arrow.Float64(), nullable: true }),
    ]);

    const batch = new arrow.RecordBatch(
      arrowSchema,
      arrow.makeData({
        type: new arrow.Struct(arrowSchema.fields),
        length,
        nullCount: 0,
        children: [vector.data[0]],
      }),
    );

    const table = new arrow.Table([batch]);
    const roundTripped = roundTripVerify(table);

    expect(roundTripped.get(0)?.toJSON().value).toBe(0);
    expect(roundTripped.get(999)?.toJSON().value).toBe(999 * 1.5);
  });

  it('handles large enum dictionary', () => {
    // Create enum with many values
    const enumValues = Array.from({ length: 100 }, (_, i) => `status_${i}`) as [string, ...string[]];
    const schema = {
      status: S.enum(enumValues),
    };

    const buffer = createGeneratedColumnBuffer(schema, 256);

    // Write various enum values using numeric indices
    // (buffer expects indices, TagWriter/SpanLogger handles string→index conversion)
    for (let i = 0; i < 100; i++) {
      buffer.status(i, i); // index i corresponds to `status_${i}`
    }

    // Verify indices
    for (let i = 0; i < 100; i++) {
      expect(buffer.status_values[i]).toBe(i);
    }
  });
});
