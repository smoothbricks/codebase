/**
 * Arrow IPC compatibility tests for column buffers.
 */

import { describe, expect, it } from 'bun:test';
import { tableFromIPC, tableToIPC } from '@uwdata/flechette';
import {
  createBoolData,
  createDictionary8Data,
  createDictionary32Data,
  createFloat64Data,
  createTableFromBatches,
  createUtf8Data,
} from '../../arrow/data.js';
import { S } from '../../schema/builder.js';
import { ColumnSchema } from '../../schema/ColumnSchema.js';
import { createGeneratedColumnBuffer } from '../columnBufferGenerator.js';
import { expose } from '../types.js';

function utf8Encode(values: readonly string[]) {
  const encoder = new TextEncoder();
  const bytes = values.map((value) => encoder.encode(value));
  const offsets = new Int32Array(values.length + 1);

  let total = 0;
  for (let i = 0; i < bytes.length; i++) {
    offsets[i] = total;
    total += bytes[i].length;
  }
  offsets[values.length] = total;

  const data = new Uint8Array(total);
  let offset = 0;
  for (const chunk of bytes) {
    data.set(chunk, offset);
    offset += chunk.length;
  }

  return { data, offsets };
}

function createEnumColumn(indices: Uint8Array, length: number, enumValues: readonly string[], nullBitmap?: Uint8Array) {
  const { data, offsets } = utf8Encode(enumValues);
  return createDictionary8Data(indices, data, offsets, length, nullBitmap);
}

function createCategoryColumn(values: string[], length: number, nullBitmap?: Uint8Array) {
  const uniqueValues = [...new Set(values.slice(0, length).filter((value) => value != null))].sort();
  const valueToIndex = new Map(uniqueValues.map((value, index) => [value, index]));
  const indices = new Uint32Array(length);

  for (let i = 0; i < length; i++) {
    const value = values[i];
    indices[i] = value != null ? (valueToIndex.get(value) ?? 0) : 0;
  }

  const { data, offsets } = utf8Encode(uniqueValues);
  return createDictionary32Data(indices, data, offsets, length, nullBitmap);
}

function createTextColumn(values: string[], length: number, nullBitmap?: Uint8Array) {
  const sliced = values.slice(0, length);
  const { data, offsets } = utf8Encode(sliced);
  return createUtf8Data(data, offsets, length, nullBitmap);
}

function roundTripVerify(table: ReturnType<typeof createTableFromBatches>) {
  const ipc = tableToIPC(table, { format: 'file' });
  if (!ipc) throw new Error('Failed to serialize Arrow table');
  const roundTripped = tableFromIPC(ipc);

  expect(roundTripped.schema.fields.length).toBe(table.schema.fields.length);
  for (let i = 0; i < table.schema.fields.length; i++) {
    expect(roundTripped.schema.fields[i].name).toBe(table.schema.fields[i].name);
    expect(roundTripped.schema.fields[i].type.typeId).toBe(table.schema.fields[i].type.typeId);
    expect(roundTripped.schema.fields[i].nullable).toBe(table.schema.fields[i].nullable);
  }
  expect(roundTripped.numRows).toBe(table.numRows);

  return roundTripped;
}

describe('Arrow IPC Compatibility - Enum Columns', () => {
  it('writes and reads enum values correctly', () => {
    const schema = new ColumnSchema({ status: S.enum(['pending', 'active', 'completed'] as const) });
    const buffer = createGeneratedColumnBuffer(schema, 16);
    const enumValues = ['pending', 'active', 'completed'] as const;

    buffer.status(0, 0);
    buffer.status(1, 1);
    buffer.status(2, 2);
    buffer.status(3, 0);
    buffer.status(4, 1);

    const length = 5;
    const exposed = expose(buffer);
    const statusColumn = createEnumColumn(
      exposed.status_values as Uint8Array,
      length,
      enumValues,
      exposed.status_nulls,
    );
    const table = createTableFromBatches({ status: statusColumn });
    const roundTripped = roundTripVerify(table);

    expect(roundTripped.at(0).status).toBe('pending');
    expect(roundTripped.at(1).status).toBe('active');
    expect(roundTripped.at(2).status).toBe('completed');
    expect(roundTripped.at(3).status).toBe('pending');
    expect(roundTripped.at(4).status).toBe('active');
  });

  it('handles eager enum columns', () => {
    const enumValues = ['PENDING', 'ACTIVE', 'DONE'] as const;
    const schema = new ColumnSchema({ status: S.enum(enumValues).eager() });
    const buffer = createGeneratedColumnBuffer(schema, 16);

    expect(buffer.status_values instanceof Uint8Array).toBe(true);
    buffer.status(0, 0);
    buffer.status(1, 1);
    buffer.status(2, 2);
    buffer.status(3, 1);

    const table = createTableFromBatches({
      status: createEnumColumn(buffer.status_values as Uint8Array, 4, enumValues),
    });
    const roundTripped = roundTripVerify(table);

    expect(roundTripped.at(0).status).toBe('PENDING');
    expect(roundTripped.at(1).status).toBe('ACTIVE');
    expect(roundTripped.at(2).status).toBe('DONE');
    expect(roundTripped.at(3).status).toBe('ACTIVE');
  });
});

describe('Arrow IPC Compatibility - Category Columns', () => {
  it('writes and reads category values correctly', () => {
    const schema = new ColumnSchema({ userId: S.category() });
    const buffer = createGeneratedColumnBuffer(schema, 16);

    buffer.userId(0, 'user-alice');
    buffer.userId(1, 'user-bob');
    buffer.userId(2, 'user-alice');
    buffer.userId(3, 'user-charlie');
    buffer.userId(4, 'user-bob');

    const table = createTableFromBatches({
      userId: createCategoryColumn(buffer.userId_values as string[], 5, buffer.userId_nulls),
    });
    const roundTripped = roundTripVerify(table);

    expect(roundTripped.at(0).userId).toBe('user-alice');
    expect(roundTripped.at(1).userId).toBe('user-bob');
    expect(roundTripped.at(2).userId).toBe('user-alice');
    expect(roundTripped.at(3).userId).toBe('user-charlie');
    expect(roundTripped.at(4).userId).toBe('user-bob');
  });

  it('handles eager category columns', () => {
    const schema = new ColumnSchema({ message: S.category().eager() });
    const buffer = createGeneratedColumnBuffer(schema, 16);

    expect(Array.isArray(buffer.message_values)).toBe(true);
    buffer.message(0, 'hello');
    buffer.message(1, 'world');

    expect(buffer.message_values[0]).toBe('hello');
    expect(buffer.message_values[1]).toBe('world');
    expect(buffer.getNullsIfAllocated('message')).toBeUndefined();
  });
});

describe('Arrow IPC Compatibility - Number Columns', () => {
  it('writes and reads number values correctly', () => {
    const schema = new ColumnSchema({ count: S.number() });
    const buffer = createGeneratedColumnBuffer(schema, 16);

    buffer.count(0, 42.5);
    buffer.count(1, 100);
    buffer.count(2, -Math.PI);
    buffer.count(3, 0);
    buffer.count(4, Number.MAX_SAFE_INTEGER);

    const table = createTableFromBatches({
      count: createFloat64Data(buffer.count_values as Float64Array, 5, buffer.count_nulls),
    });
    const roundTripped = roundTripVerify(table);

    expect(roundTripped.at(0).count).toBe(42.5);
    expect(roundTripped.at(1).count).toBe(100);
    expect(roundTripped.at(2).count).toBe(-Math.PI);
    expect(roundTripped.at(3).count).toBe(0);
    expect(roundTripped.at(4).count).toBe(Number.MAX_SAFE_INTEGER);
  });

  it('handles eager number columns', () => {
    const schema = new ColumnSchema({ count: S.number().eager() });
    const buffer = createGeneratedColumnBuffer(schema, 16);

    expect(buffer.count_values instanceof Float64Array).toBe(true);
    buffer.count(0, 1.5);
    buffer.count(1, 2.5);
    buffer.count(2, 3.5);

    const table = createTableFromBatches({ count: createFloat64Data(buffer.count_values as Float64Array, 3) });
    const roundTripped = roundTripVerify(table);

    expect(roundTripped.at(0).count).toBe(1.5);
    expect(roundTripped.at(1).count).toBe(2.5);
    expect(roundTripped.at(2).count).toBe(3.5);
  });
});

describe('Arrow IPC Compatibility - Boolean Columns', () => {
  it('writes and reads boolean values correctly (bit-packed)', () => {
    const schema = new ColumnSchema({ active: S.boolean() });
    const buffer = createGeneratedColumnBuffer(schema, 16);

    buffer.active(0, true);
    buffer.active(1, false);
    buffer.active(2, true);
    buffer.active(3, true);
    buffer.active(4, false);
    buffer.active(5, true);
    buffer.active(6, false);
    buffer.active(7, true);
    buffer.active(8, true);

    const table = createTableFromBatches({
      active: createBoolData(buffer.active_values as Uint8Array, 9, buffer.active_nulls),
    });
    const roundTripped = roundTripVerify(table);

    expect(roundTripped.at(0).active).toBe(true);
    expect(roundTripped.at(1).active).toBe(false);
    expect(roundTripped.at(2).active).toBe(true);
    expect(roundTripped.at(3).active).toBe(true);
    expect(roundTripped.at(4).active).toBe(false);
    expect(roundTripped.at(5).active).toBe(true);
    expect(roundTripped.at(6).active).toBe(false);
    expect(roundTripped.at(7).active).toBe(true);
    expect(roundTripped.at(8).active).toBe(true);
  });

  it('handles eager boolean columns', () => {
    const schema = new ColumnSchema({ active: S.boolean().eager() });
    const buffer = createGeneratedColumnBuffer(schema, 16);

    expect(buffer.active_values instanceof Uint8Array).toBe(true);
    buffer.active(0, true);
    buffer.active(1, false);
    buffer.active(2, true);

    const table = createTableFromBatches({ active: createBoolData(buffer.active_values as Uint8Array, 3) });
    const roundTripped = roundTripVerify(table);

    expect(roundTripped.at(0).active).toBe(true);
    expect(roundTripped.at(1).active).toBe(false);
    expect(roundTripped.at(2).active).toBe(true);
  });
});

describe('Arrow IPC Compatibility - Text Columns', () => {
  it('writes and reads text values correctly', () => {
    const schema = new ColumnSchema({ message: S.text() });
    const buffer = createGeneratedColumnBuffer(schema, 16);

    buffer.message(0, 'First message');
    buffer.message(1, 'Second message');
    buffer.message(2, 'Third message');

    const table = createTableFromBatches({
      message: createTextColumn(buffer.message_values as string[], 3, buffer.message_nulls),
    });
    const roundTripped = roundTripVerify(table);

    expect(roundTripped.at(0).message).toBe('First message');
    expect(roundTripped.at(1).message).toBe('Second message');
    expect(roundTripped.at(2).message).toBe('Third message');
  });

  it('handles eager text columns', () => {
    const schema = new ColumnSchema({ log: S.text().eager() });
    const buffer = createGeneratedColumnBuffer(schema, 16);

    expect(Array.isArray(buffer.log_values)).toBe(true);
    buffer.log(0, 'Log entry 1');
    buffer.log(1, 'Log entry 2');

    expect(buffer.log_values[0]).toBe('Log entry 1');
    expect(buffer.log_values[1]).toBe('Log entry 2');
    expect(buffer.getNullsIfAllocated('log')).toBeUndefined();
  });
});

describe('Arrow IPC Compatibility - Mixed Schema', () => {
  it('handles all column types together', () => {
    const schema = new ColumnSchema({
      status: S.enum(['pending', 'active', 'completed'] as const),
      userId: S.category(),
      errorMsg: S.text(),
      count: S.number(),
      active: S.boolean(),
    });

    const buffer = createGeneratedColumnBuffer(schema, 16);
    buffer.status(0, 0);
    buffer.userId(0, 'user-1').errorMsg(0, 'No error').count(0, 42).active(0, true);
    buffer.status(1, 1);
    buffer.userId(1, 'user-2').errorMsg(1, 'Some error').count(1, 100).active(1, false);

    expect(buffer.status_values[0]).toBe(0);
    expect(buffer.status_values[1]).toBe(1);
    expect(buffer.userId_values[0]).toBe('user-1');
    expect(buffer.userId_values[1]).toBe('user-2');
    expect(buffer.errorMsg_values[0]).toBe('No error');
    expect(buffer.errorMsg_values[1]).toBe('Some error');
    expect(buffer.count_values[0]).toBe(42);
    expect(buffer.count_values[1]).toBe(100);
    expect((buffer.active_values as Uint8Array)[0]).toBe(1);
  });

  it('handles mixed eager and lazy columns', () => {
    const schema = new ColumnSchema({
      message: S.text().eager(),
      userId: S.category(),
      count: S.number().eager(),
    });

    const buffer = createGeneratedColumnBuffer(schema, 16);
    expect(buffer.message_values).toBeDefined();
    expect(buffer.count_values).toBeDefined();
    expect(buffer.getColumnIfAllocated('userId')).toBeUndefined();

    buffer.message(0, 'hello');
    buffer.userId(0, 'user-1');
    buffer.count(0, 42);
    buffer.message(1, 'world');
    buffer.count(1, 100);

    expect(buffer.getColumnIfAllocated('userId')).toBeDefined();
    expect(buffer.message_values[0]).toBe('hello');
    expect(buffer.message_values[1]).toBe('world');
    expect(buffer.userId_values[0]).toBe('user-1');
    expect(buffer.count_values[0]).toBe(42);
    expect(buffer.count_values[1]).toBe(100);
    expect(buffer.userId_nulls[0] & 0b11).toBe(0b01);
  });
});

describe('Arrow IPC Compatibility - Null Bitmap Format', () => {
  it('uses Arrow format (1=valid, 0=null) for lazy columns', () => {
    const schema = new ColumnSchema({ value: S.number() });
    const buffer = createGeneratedColumnBuffer(schema, 16);

    buffer.value(0, 1.0);
    buffer.value(2, 2.0);
    buffer.value(4, 3.0);

    expect(buffer.value_nulls[0]).toBe(21);
  });

  it('handles null bitmap across byte boundaries', () => {
    const schema = new ColumnSchema({ value: S.category() });
    const buffer = createGeneratedColumnBuffer(schema, 32);

    buffer.value(0, 'a');
    buffer.value(7, 'b');
    buffer.value(8, 'c');
    buffer.value(15, 'd');

    expect(buffer.value_nulls[0]).toBe(129);
    expect(buffer.value_nulls[1]).toBe(129);
  });
});

describe('Arrow IPC Compatibility - Large Datasets', () => {
  it('handles large number of rows', () => {
    const schema = new ColumnSchema({ value: S.number() });
    const buffer = createGeneratedColumnBuffer(schema, 1024);

    for (let i = 0; i < 1000; i++) {
      buffer.value(i, i * 1.5);
    }

    const table = createTableFromBatches({
      value: createFloat64Data(buffer.value_values as Float64Array, 1000, buffer.value_nulls),
    });
    const roundTripped = roundTripVerify(table);

    expect(roundTripped.at(0).value).toBe(0);
    expect(roundTripped.at(999).value).toBe(999 * 1.5);
  });

  it('handles large enum dictionary', () => {
    const generatedEnumValues = Array.from({ length: 100 }, (_, i) => `status_${i}`);
    const firstEnumValue = generatedEnumValues[0];
    if (firstEnumValue === undefined) {
      throw new Error('Expected non-empty enum values');
    }
    const enumValues: [string, ...string[]] = [firstEnumValue, ...generatedEnumValues.slice(1)];
    const schema = new ColumnSchema({ status: S.enum(enumValues) });
    const buffer = createGeneratedColumnBuffer(schema, 256);

    for (let i = 0; i < 100; i++) {
      buffer.status(i, i);
    }

    for (let i = 0; i < 100; i++) {
      expect(buffer.status_values[i]).toBe(i);
    }
  });
});

describe('Arrow IPC Compatibility - Format Verification', () => {
  it('produces valid Arrow IPC binary format', () => {
    const schema = new ColumnSchema({
      id: S.number(),
      name: S.category(),
      status: S.enum(['active', 'inactive'] as const),
      flag: S.boolean(),
    });

    const buffer = createGeneratedColumnBuffer(schema, 16);
    buffer.id(0, 1).name(0, 'Alice').status(0, 0).flag(0, true);
    buffer.id(1, 2).name(1, 'Bob').status(1, 1).flag(1, false);

    const table = createTableFromBatches({
      id: createFloat64Data(buffer.id_values as Float64Array, 2, buffer.id_nulls),
      name: createCategoryColumn(buffer.name_values as string[], 2, buffer.name_nulls),
      status: createEnumColumn(buffer.status_values as Uint8Array, 2, ['active', 'inactive'], buffer.status_nulls),
      flag: createBoolData(buffer.flag_values as Uint8Array, 2, buffer.flag_nulls),
    });

    const ipc = tableToIPC(table, { format: 'file' });
    if (!ipc) throw new Error('Failed to serialize Arrow table');
    expect(ipc).toBeInstanceOf(Uint8Array);
    expect(ipc.length).toBeGreaterThan(0);

    const roundTripped = tableFromIPC(ipc);
    expect(roundTripped.numRows).toBe(2);
    expect(roundTripped.schema.fields.length).toBe(4);

    const row0 = roundTripped.at(0);
    const row1 = roundTripped.at(1);
    expect(row0.id).toBe(1);
    expect(row0.name).toBe('Alice');
    expect(row0.status).toBe('active');
    expect(row0.flag).toBe(true);
    expect(row1.id).toBe(2);
    expect(row1.name).toBe('Bob');
    expect(row1.status).toBe('inactive');
    expect(row1.flag).toBe(false);
  });
});
