import { afterAll, describe, expect, it } from 'bun:test';
import {
  binary,
  bool,
  float64,
  int64,
  nullType,
  tableFromArrays,
  tableFromIPC,
  tableToIPC,
  uint32,
  utf8,
} from '@uwdata/flechette';

import { CompactEncodingError, loadParseBackend } from '../parse-backend.js';
import type { CompactBatch, CompactColumn, EncodedArrowSchema } from '../pipeline.js';

const ROW_COUNT = 300;
const FIELD_NAMES = ['all_null', 'u32_value', 'f64_value', 'i64_value', 'bool_value', 'binary_value', 'utf8_value'];
const NULL_ROWS: readonly number[] = [0, 7, 8, 9, 257];

function schemaMessage(): EncodedArrowSchema {
  const table = tableFromArrays(
    {
      all_null: [null],
      u32_value: [0],
      f64_value: [0],
      i64_value: [0n],
      bool_value: [false],
      binary_value: [new Uint8Array()],
      utf8_value: [''],
    },
    {
      types: {
        all_null: nullType(),
        u32_value: uint32(),
        f64_value: float64(),
        i64_value: int64(),
        bool_value: bool(),
        binary_value: binary(),
        utf8_value: utf8(),
      },
    },
  );
  const stream = tableToIPC(table, { format: 'stream' });
  if (stream === null) {
    throw new Error('Flechette did not return in-memory Arrow IPC bytes');
  }
  const view = new DataView(stream.buffer, stream.byteOffset, stream.byteLength);
  if (view.getUint32(0, true) !== 0xffff_ffff) {
    throw new Error('Flechette schema message is not continuation-prefixed');
  }
  const messageLength = 8 + view.getUint32(4, true);
  return {
    schemaBytes: stream.slice(0, messageLength),
    fieldMetadata: new Uint8Array([0, 1, 0, 0, 1, 1, 0, 0, 2, 1, 0, 0, 6, 1, 0, 0, 5, 1, 0, 0, 3, 1, 0, 0, 4, 1, 0, 0]),
  };
}

function getAt<T>(values: readonly T[], index: number, label: string): T {
  const value = values[index];
  if (value === undefined) {
    throw new RangeError(`${label} is missing index ${index}`);
  }
  return value;
}

function bitmapSubview(rowCount: number, initialValue: boolean): Uint8Array {
  const byteLength = Math.ceil(rowCount / 8);
  const backing = new Uint8Array(byteLength + 2);
  const bitmap = backing.subarray(1, byteLength + 1);
  if (initialValue) {
    bitmap.fill(0xff);
    if (rowCount % 8 !== 0) {
      bitmap[byteLength - 1] = (1 << (rowCount % 8)) - 1;
    }
  }
  return bitmap;
}

function setBit(bitmap: Uint8Array, row: number, value: boolean): void {
  const byte = row >>> 3;
  const current = bitmap[byte];
  if (current === undefined) {
    throw new RangeError(`bitmap is missing row ${row}`);
  }
  const mask = 1 << (row & 7);
  bitmap[byte] = value ? current | mask : current & ~mask;
}

function nullableValidity(rowCount: number): Uint8Array {
  const validity = bitmapSubview(rowCount, true);
  for (const row of NULL_ROWS) {
    if (row < rowCount) {
      setBit(validity, row, false);
    }
  }
  return validity;
}

function variableColumn(values: readonly (Uint8Array | null)[]): {
  readonly offsets: Uint32Array;
  readonly data: Uint8Array;
} {
  let dataLength = 0;
  for (const value of values) {
    dataLength += value?.byteLength ?? 0;
  }
  const offsetsBacking = new Uint32Array(values.length + 3);
  const offsets = offsetsBacking.subarray(1, values.length + 2);
  const dataBacking = new Uint8Array(dataLength + 2);
  const data = dataBacking.subarray(1, dataLength + 1);
  let offset = 0;
  for (let row = 0; row < values.length; row += 1) {
    offsets[row] = offset;
    const value = values[row];
    if (value !== null) {
      data.set(value, offset);
      offset += value.byteLength;
    }
  }
  offsets[values.length] = offset;
  return { offsets, data };
}

function mixedBatch(schema: EncodedArrowSchema): CompactBatch {
  const u32Backing = new Uint32Array(ROW_COUNT + 2);
  const u32Values = u32Backing.subarray(1, ROW_COUNT + 1);
  for (let row = 0; row < ROW_COUNT; row += 1) {
    u32Values[row] = row;
  }
  u32Values[1] = 0xffff_ffff;
  u32Values[2] = 0x8000_0000;

  const f64Backing = new Float64Array(ROW_COUNT + 2);
  const f64Values = f64Backing.subarray(1, ROW_COUNT + 1);
  for (let row = 0; row < ROW_COUNT; row += 1) {
    f64Values[row] = row + 0.25;
  }
  f64Values[1] = Number.NaN;
  f64Values[2] = Number.POSITIVE_INFINITY;
  f64Values[3] = -0;

  const i64Backing = new BigInt64Array(ROW_COUNT + 2);
  const i64Values = i64Backing.subarray(1, ROW_COUNT + 1);
  for (let row = 0; row < ROW_COUNT; row += 1) {
    i64Values[row] = BigInt(row);
  }
  i64Values[1] = -(1n << 63n);
  i64Values[2] = (1n << 63n) - 1n;

  const boolValues = bitmapSubview(ROW_COUNT, false);
  for (let row = 0; row < ROW_COUNT; row += 1) {
    setBit(boolValues, row, row % 3 === 0);
  }

  const encoder = new TextEncoder();
  const binaryValues: Array<Uint8Array | null> = [];
  const utf8Values: Array<Uint8Array | null> = [];
  for (let row = 0; row < ROW_COUNT; row += 1) {
    const isNull = NULL_ROWS.some((nullRow) => nullRow === row);
    binaryValues.push(isNull ? null : new Uint8Array([row & 0xff, 0, (row * 7) & 0xff]));
    utf8Values.push(isNull ? null : encoder.encode(`row-${row}-π`));
  }
  const binaryBuffers = variableColumn(binaryValues);
  const utf8Buffers = variableColumn(utf8Values);

  const columns: readonly CompactColumn[] = [
    { kind: 'null' },
    { kind: 'u32', data: u32Values, validity: nullableValidity(ROW_COUNT) },
    { kind: 'f64', data: f64Values, validity: nullableValidity(ROW_COUNT) },
    { kind: 'i64', data: i64Values, validity: nullableValidity(ROW_COUNT) },
    { kind: 'bool', data: boolValues, validity: nullableValidity(ROW_COUNT) },
    { kind: 'binary', ...binaryBuffers, validity: nullableValidity(ROW_COUNT) },
    { kind: 'utf8', ...utf8Buffers, validity: nullableValidity(ROW_COUNT) },
  ];

  for (const column of columns) {
    if ('data' in column) {
      expect(column.data.byteOffset).toBeGreaterThan(0);
    }
    if ('validity' in column && column.validity !== undefined) {
      expect(column.validity.byteOffset).toBeGreaterThan(0);
    }
    if ('offsets' in column) {
      expect(column.offsets.byteOffset).toBeGreaterThan(0);
    }
  }
  return { rowCount: ROW_COUNT, schema, columns };
}

const backend = await loadParseBackend();
const encodedSchema = schemaMessage();

afterAll(() => backend.dispose());

describe('Compact real event_processor.wasm', () => {
  it('encodes a mixed nullable batch beyond processor parse capacity for an independent Arrow reader', () => {
    const ipc = backend.encode(mixedBatch(encodedSchema));
    const table = tableFromIPC(ipc, { useBigInt: true });
    const columns = table.toColumns();

    expect(table.numRows).toBe(ROW_COUNT);
    expect(table.numRows).toBeGreaterThan(256);
    expect(table.names).toEqual(FIELD_NAMES);
    expect(table.schema.fields.map((field) => field.name)).toEqual(FIELD_NAMES);
    expect(table.schema.fields.map((field) => field.nullable)).toEqual(new Array(7).fill(true));
    expect(table.schema.fields.map((field) => field.type.typeId)).toEqual([1, 2, 3, 2, 6, 4, 5]);
    expect(getAt(table.schema.fields, 1, 'schema field').type).toMatchObject({ bitWidth: 32, signed: false });
    expect(getAt(table.schema.fields, 3, 'schema field').type).toMatchObject({ bitWidth: 64, signed: true });

    expect(columns.all_null).toEqual(new Array(ROW_COUNT).fill(null));
    for (const row of NULL_ROWS) {
      expect(columns.u32_value[row]).toBeNull();
      expect(columns.f64_value[row]).toBeNull();
      expect(columns.i64_value[row]).toBeNull();
      expect(columns.bool_value[row]).toBeNull();
      expect(columns.binary_value[row]).toBeNull();
      expect(columns.utf8_value[row]).toBeNull();
    }

    expect(columns.u32_value[1]).toBe(0xffff_ffff);
    expect(columns.u32_value[2]).toBe(0x8000_0000);
    expect(Number.isNaN(columns.f64_value[1])).toBe(true);
    expect(columns.f64_value[2]).toBe(Number.POSITIVE_INFINITY);
    expect(Object.is(columns.f64_value[3], -0)).toBe(true);
    expect(columns.i64_value[1]).toBe(-(1n << 63n));
    expect(columns.i64_value[2]).toBe((1n << 63n) - 1n);
    expect(columns.bool_value[3]).toBe(true);
    expect(columns.bool_value[4]).toBe(false);
    expect(columns.binary_value[299]).toEqual(new Uint8Array([43, 0, 45]));
    expect(columns.utf8_value[299]).toBe('row-299-π');
  });

  it('encodes the same seven-field schema as an empty batch', () => {
    const ipc = backend.encode({
      rowCount: 0,
      schema: encodedSchema,
      columns: [
        { kind: 'null' },
        { kind: 'u32', data: new Uint32Array() },
        { kind: 'f64', data: new Float64Array() },
        { kind: 'i64', data: new BigInt64Array() },
        { kind: 'bool', data: new Uint8Array() },
        { kind: 'binary', offsets: new Uint32Array([0]), data: new Uint8Array() },
        { kind: 'utf8', offsets: new Uint32Array([0]), data: new Uint8Array() },
      ],
    });
    const table = tableFromIPC(ipc, { useBigInt: true });
    expect(table.numRows).toBe(0);
    expect(table.names).toEqual(FIELD_NAMES);
    expect(table.schema.fields.map((field) => field.type.typeId)).toEqual([1, 2, 3, 2, 6, 4, 5]);
  });

  it('rejects metadata that disagrees with the real logical Arrow schema', async () => {
    const mismatchBackend = await loadParseBackend();
    const fieldMetadata = encodedSchema.fieldMetadata.slice();
    fieldMetadata[4] = 2;
    try {
      mismatchBackend.encode({
        rowCount: 0,
        schema: { schemaBytes: encodedSchema.schemaBytes, fieldMetadata },
        columns: [
          { kind: 'null' },
          { kind: 'f64', data: new Float64Array() },
          { kind: 'f64', data: new Float64Array() },
          { kind: 'i64', data: new BigInt64Array() },
          { kind: 'bool', data: new Uint8Array() },
          { kind: 'binary', offsets: new Uint32Array([0]), data: new Uint8Array() },
          { kind: 'utf8', offsets: new Uint32Array([0]), data: new Uint8Array() },
        ],
      });
      throw new Error('expected native schema validation to reject mismatched metadata');
    } catch (error) {
      expect(error).toBeInstanceOf(CompactEncodingError);
      if (!(error instanceof CompactEncodingError)) {
        throw error;
      }
      expect(error.status).toBe(7);
      expect(error.code).toBe('SCHEMA_MISMATCH');
    } finally {
      mismatchBackend.dispose();
    }
  });
});
