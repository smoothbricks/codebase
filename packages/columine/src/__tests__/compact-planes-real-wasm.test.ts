import { afterAll, describe, expect, it } from 'bun:test';
import {
  binary,
  bool,
  type DataType,
  dateDay,
  dateMillisecond,
  decimal128,
  decimal256,
  duration,
  fixedSizeBinary,
  float16,
  float32,
  float64,
  IntervalUnit,
  int8,
  int16,
  int32,
  int64,
  interval,
  largeBinary,
  largeUtf8,
  nullType,
  TimeUnit,
  tableFromArrays,
  tableFromIPC,
  tableToIPC,
  timeMicrosecond,
  timeMillisecond,
  timestamp,
  uint8,
  uint16,
  uint32,
  uint64,
  utf8,
} from '@uwdata/flechette';

import { loadParseBackend } from '../parse-backend.js';
import type { CompactColumn, EncodedArrowSchema } from '../pipeline.js';

const backend = await loadParseBackend();
afterAll(() => backend.dispose());

function schemaMessage(name: string, sample: unknown, type: DataType, tag: number, typeParam = 0): EncodedArrowSchema {
  const table = tableFromArrays({ [name]: [sample] }, { types: { [name]: type } });
  const stream = tableToIPC(table, { format: 'stream' });
  if (stream === null) {
    throw new Error('Flechette did not return in-memory Arrow IPC bytes');
  }
  const view = new DataView(stream.buffer, stream.byteOffset, stream.byteLength);
  if (view.getUint32(0, true) !== 0xffff_ffff) {
    throw new Error('Flechette schema message is not continuation-prefixed');
  }
  const messageLength = 8 + view.getUint32(4, true);
  const fieldMetadata = new Uint8Array(4);
  fieldMetadata[0] = tag;
  fieldMetadata[1] = 1;
  fieldMetadata[2] = typeParam & 0xff;
  fieldMetadata[3] = (typeParam >>> 8) & 0xff;
  return { schemaBytes: stream.slice(0, messageLength), fieldMetadata };
}

function readColumn(ipc: Uint8Array, name: string) {
  const table = tableFromIPC(ipc, { useBigInt: true, useDecimalInt: true });
  expect(table.numRows).toBeGreaterThan(0);
  const found = Object.entries(table.toColumns()).find(([key]) => key === name);
  if (found === undefined) {
    throw new Error(`missing column ${name}`);
  }
  return found[1];
}

function expectTypedValues(
  values: unknown,
  ctor: new (length: number) => ArrayLike<number>,
  expected: readonly number[],
): void {
  if (!(values instanceof ctor)) {
    throw new TypeError(`expected ${ctor.name}, got ${Object.prototype.toString.call(values)}`);
  }
  expect(Array.from(values)).toEqual([...expected]);
}

function leInteger(width: number, value: bigint): Uint8Array {
  const bytes = new Uint8Array(width);
  let remaining = value < 0n ? (1n << BigInt(width * 8)) + value : value;
  for (let index = 0; index < width; index += 1) {
    bytes[index] = Number(remaining & 0xffn);
    remaining >>= 8n;
  }
  return bytes;
}

function monthDayNano(months: number, days: number, nanos: bigint): Uint8Array {
  const bytes = new Uint8Array(16);
  const view = new DataView(bytes.buffer);
  view.setInt32(0, months, true);
  view.setInt32(4, days, true);
  view.setBigInt64(8, nanos, true);
  return bytes;
}

function encodeOne(name: string, schema: EncodedArrowSchema, column: CompactColumn) {
  const ipc = backend.encode({ rowCount: 2, schema, columns: [column] });
  return readColumn(ipc, name);
}

function expectNumeric(actual: unknown, expected: bigint): void {
  if (typeof actual === 'bigint') {
    expect(actual).toBe(expected);
    return;
  }
  if (typeof actual === 'number') {
    expect(BigInt(actual)).toBe(expected);
    return;
  }
  throw new Error(`expected a numeric column value, got ${typeof actual}`);
}

describe('Compact real-wasm flat planes', () => {
  it('null', () => {
    const schema = schemaMessage('v', null, nullType(), 0);
    const values = encodeOne('v', schema, { kind: 'null' });
    expect(values).toEqual([null, null]);
  });

  it('i32 signed bits', () => {
    const schema = schemaMessage('v', 0, int32(), 1);
    const values = encodeOne('v', schema, { kind: 'i32', data: new Int32Array([-1, 0x7fff_ffff]) });
    expect(values[0]).toBe(-1);
    expect(values[1]).toBe(0x7fff_ffff);
  });

  it('u32 unsigned bits', () => {
    const schema = schemaMessage('v', 0, uint32(), 11);
    const values = encodeOne('v', schema, { kind: 'u32', data: new Uint32Array([0xffff_ffff, 0x8000_0000]) });
    expect(values[0]).toBe(4_294_967_295);
    expect(values[1]).toBe(2_147_483_648);
  });

  it('i8', () => {
    const schema = schemaMessage('v', 0, int8(), 7);
    expectTypedValues(encodeOne('v', schema, { kind: 'i8', data: new Int8Array([-128, 127]) }), Int8Array, [-128, 127]);
  });

  it('i16', () => {
    const schema = schemaMessage('v', 0, int16(), 8);
    expectTypedValues(
      encodeOne('v', schema, { kind: 'i16', data: new Int16Array([-32768, 32767]) }),
      Int16Array,
      [-32768, 32767],
    );
  });

  it('u8', () => {
    const schema = schemaMessage('v', 0, uint8(), 9);
    expectTypedValues(encodeOne('v', schema, { kind: 'u8', data: new Uint8Array([0, 255]) }), Uint8Array, [0, 255]);
  });

  it('u16', () => {
    const schema = schemaMessage('v', 0, uint16(), 10);
    expectTypedValues(
      encodeOne('v', schema, { kind: 'u16', data: new Uint16Array([0, 0xffff]) }),
      Uint16Array,
      [0, 65535],
    );
  });

  it('u64', () => {
    const schema = schemaMessage('v', 0n, uint64(), 12);
    const values = encodeOne('v', schema, { kind: 'u64', data: new BigUint64Array([0n, (1n << 64n) - 1n]) });
    expect(values[0]).toBe(0n);
    expect(values[1]).toBe((1n << 64n) - 1n);
  });

  it('i64', () => {
    const schema = schemaMessage('v', 0n, int64(), 6);
    const values = encodeOne('v', schema, { kind: 'i64', data: new BigInt64Array([-(1n << 63n), (1n << 63n) - 1n]) });
    expect(values[0]).toBe(-(1n << 63n));
    expect(values[1]).toBe((1n << 63n) - 1n);
  });

  it('f16', () => {
    const schema = schemaMessage('v', 0, float16(), 13);
    const values = encodeOne('v', schema, { kind: 'f16', data: new Uint16Array([0x3c00, 0xbc00]) });
    expect(values[0]).toBe(1);
    expect(values[1]).toBe(-1);
  });

  it('f32', () => {
    const schema = schemaMessage('v', 0, float32(), 14);
    const values = encodeOne('v', schema, { kind: 'f32', data: new Float32Array([-0, Number.POSITIVE_INFINITY]) });
    expect(Object.is(values[0], -0)).toBe(true);
    expect(values[1]).toBe(Number.POSITIVE_INFINITY);
  });

  it('f64', () => {
    const schema = schemaMessage('v', 0, float64(), 2);
    const values = encodeOne('v', schema, { kind: 'f64', data: new Float64Array([Number.NaN, -0]) });
    expect(Number.isNaN(values[0])).toBe(true);
    expect(Object.is(values[1], -0)).toBe(true);
  });

  it('bool', () => {
    const schema = schemaMessage('v', false, bool(), 5);
    expect(encodeOne('v', schema, { kind: 'bool', data: new Uint8Array([0b01]) })).toEqual([true, false]);
  });

  it('binary', () => {
    const schema = schemaMessage('v', new Uint8Array(), binary(), 3);
    const values = encodeOne('v', schema, {
      kind: 'binary',
      offsets: new Uint32Array([0, 1, 3]),
      data: new Uint8Array([9, 8, 7]),
    });
    expect(values[0]).toEqual(new Uint8Array([9]));
    expect(values[1]).toEqual(new Uint8Array([8, 7]));
  });

  it('utf8', () => {
    const schema = schemaMessage('v', '', utf8(), 4);
    const encoder = new TextEncoder();
    const hello = encoder.encode('hi');
    const pi = encoder.encode('π');
    const values = encodeOne('v', schema, {
      kind: 'utf8',
      offsets: new Uint32Array([0, hello.byteLength, hello.byteLength + pi.byteLength]),
      data: new Uint8Array([...hello, ...pi]),
    });
    expect(values).toEqual(['hi', 'π']);
  });

  it('largeBinary', () => {
    const schema = schemaMessage('v', new Uint8Array(), largeBinary(), 17);
    const values = encodeOne('v', schema, {
      kind: 'largeBinary',
      offsets: new BigInt64Array([0n, 1n, 3n]),
      data: new Uint8Array([9, 8, 7]),
    });
    expect(values[0]).toEqual(new Uint8Array([9]));
    expect(values[1]).toEqual(new Uint8Array([8, 7]));
  });

  it('largeUtf8', () => {
    const schema = schemaMessage('v', '', largeUtf8(), 18);
    const encoder = new TextEncoder();
    const hello = encoder.encode('hi');
    const pi = encoder.encode('π');
    const values = encodeOne('v', schema, {
      kind: 'largeUtf8',
      offsets: new BigInt64Array([0n, BigInt(hello.byteLength), BigInt(hello.byteLength + pi.byteLength)]),
      data: new Uint8Array([...hello, ...pi]),
    });
    expect(values).toEqual(['hi', 'π']);
  });

  it('fixedSizeBinary', () => {
    const schema = schemaMessage('v', new Uint8Array(3), fixedSizeBinary(3), 19, 3);
    const values = encodeOne('v', schema, { kind: 'fixedSizeBinary', data: new Uint8Array([1, 2, 3, 4, 5, 6]) });
    expect(values[0]).toEqual(new Uint8Array([1, 2, 3]));
    expect(values[1]).toEqual(new Uint8Array([4, 5, 6]));
  });

  it('decimal128', () => {
    // flechette's DEFAULT decimal path is a lossy Float64Array, and `useBigInt`
    // does not govern decimals — the option a reader would reach for. Measured
    // on decimal128(38,0) values 2^53+1, 10^38-1, -1:
    //   {} / { useBigInt: true } -> 9007199254740992 | 1e+38 | -1  (lossy)
    //   { useDecimalInt: true }  -> 9007199254740993n | 999...9n | -1n (exact)
    // `readColumn` therefore passes useDecimalInt. Full precision is also
    // covered independently by crates/columine-arrow/tests/pyarrow_oracle.rs.
    const aboveMantissa = (1n << 53n) + 1n;
    const thirtyEightNines = 10n ** 38n - 1n;
    const schema = schemaMessage('v', 0n, decimal128(38, 0), 15);
    const ipc = backend.encode({
      rowCount: 3,
      schema,
      columns: [
        {
          kind: 'decimal128',
          data: new Uint8Array([
            ...leInteger(16, aboveMantissa),
            ...leInteger(16, thirtyEightNines),
            ...leInteger(16, -1n),
          ]),
        },
      ],
    });
    const values = readColumn(ipc, 'v');
    expect(values[0]).toBe(aboveMantissa);
    expect(values[1]).toBe(thirtyEightNines);
    expect(values[2]).toBe(-1n);
  });

  it('decimal256', () => {
    const aboveMantissa = (1n << 53n) + 1n;
    const schema = schemaMessage('v', 0n, decimal256(76, 0), 16);
    const values = encodeOne('v', schema, {
      kind: 'decimal256',
      data: new Uint8Array([...leInteger(32, aboveMantissa), ...leInteger(32, -1n)]),
    });
    expect(values[0]).toBe(aboveMantissa);
    expect(values[1]).toBe(-1n);
  });

  it('date32 rides i32', () => {
    const schema = schemaMessage('v', 0, dateDay(), 1);
    const values = encodeOne('v', schema, { kind: 'i32', data: new Int32Array([0, 19_000]) });
    expect(values[0]).toBe(0);
    expect(values[1]).toBe(19_000 * 86_400_000);
  });

  it('date64 rides i64', () => {
    const schema = schemaMessage('v', 0n, dateMillisecond(), 6);
    const values = encodeOne('v', schema, { kind: 'i64', data: new BigInt64Array([0n, 86_400_000n]) });
    expectNumeric(values[0], 0n);
    expectNumeric(values[1], 86_400_000n);
  });

  it('time32 rides i32', () => {
    const schema = schemaMessage('v', 0, timeMillisecond(), 1);
    expectTypedValues(
      encodeOne('v', schema, { kind: 'i32', data: new Int32Array([0, 1_000]) }),
      Int32Array,
      [0, 1_000],
    );
  });

  it('time64 rides i64', () => {
    const schema = schemaMessage('v', 0n, timeMicrosecond(), 6);
    const values = encodeOne('v', schema, { kind: 'i64', data: new BigInt64Array([0n, 1_000_000n]) });
    expectNumeric(values[0], 0n);
    expectNumeric(values[1], 1_000_000n);
  });

  it('timestamp rides i64', () => {
    const schema = schemaMessage('v', 0n, timestamp(TimeUnit.MILLISECOND), 6);
    const values = encodeOne('v', schema, { kind: 'i64', data: new BigInt64Array([0n, 1_700_000_000_000n]) });
    expectNumeric(values[0], 0n);
    expectNumeric(values[1], 1_700_000_000_000n);
  });

  it('duration rides i64', () => {
    const schema = schemaMessage('v', 0n, duration(TimeUnit.MILLISECOND), 6);
    const values = encodeOne('v', schema, { kind: 'i64', data: new BigInt64Array([0n, 5_000n]) });
    expectNumeric(values[0], 0n);
    expectNumeric(values[1], 5_000n);
  });

  it('intervalYearMonth', () => {
    const schema = schemaMessage('v', 0, interval(IntervalUnit.YEAR_MONTH), 20);
    expectTypedValues(
      encodeOne('v', schema, { kind: 'intervalYearMonth', data: new Int32Array([13, -1]) }),
      Int32Array,
      [13, -1],
    );
  });

  it('intervalDayTime', () => {
    const schema = schemaMessage('v', [0, 0], interval(IntervalUnit.DAY_TIME), 21);
    const values = encodeOne('v', schema, { kind: 'intervalDayTime', data: new Int32Array([1, 2, 3, 4]) });
    expect(values[0]).toEqual(Int32Array.of(1, 2));
    expect(values[1]).toEqual(Int32Array.of(3, 4));
  });

  it('intervalMonthDayNano', () => {
    const schema = schemaMessage('v', [0, 0, 0n], interval(IntervalUnit.MONTH_DAY_NANO), 22);
    const first = monthDayNano(1, 2, 3n);
    const second = monthDayNano(-1, 0, 4n);
    const values = encodeOne('v', schema, {
      kind: 'intervalMonthDayNano',
      data: new Uint8Array([...first, ...second]),
    });
    expect(values[0]).toEqual(Float64Array.of(1, 2, 3));
    expect(values[1]).toEqual(Float64Array.of(-1, 0, 4));
  });
});
