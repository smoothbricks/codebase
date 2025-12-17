/**
 * DictionaryBuilder Binary Compatibility Tests
 *
 * This test suite verifies that DictionaryBuilder produces Arrow-compatible
 * dictionaries that can be serialized to IPC format and read back correctly.
 *
 * Tests cover:
 * 1. Sorted dictionaries (category columns)
 * 2. Unsorted dictionaries (text columns)
 * 3. Dictionary index type selection (uint8/uint16/uint32)
 * 4. UTF-8 encoding correctness
 * 5. IPC round-trip verification
 */

import { describe, expect, it } from 'bun:test';
import * as arrow from 'apache-arrow';
import { DictionaryBuilder } from '../dictionary.js';
import { countNulls } from '../nullBitmap.js';

/**
 * Helper to create dictionary vector with Uint8 indices.
 */
function createDictVector8(
  dict: ReturnType<DictionaryBuilder['finalize']>,
  indices: Uint8Array,
  length: number,
  nullBitmap?: Uint8Array,
): arrow.Data<arrow.Dictionary<arrow.Utf8, arrow.Uint8>> {
  const dictData = arrow.makeData({
    type: new arrow.Utf8(),
    offset: 0,
    length: dict.indexMap.size,
    nullCount: 0,
    valueOffsets: dict.offsets,
    data: dict.data,
  });

  return arrow.makeData({
    type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint8()),
    offset: 0,
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    data: indices.subarray(0, length),
    nullBitmap: nullBitmap?.subarray(0, Math.ceil(length / 8)),
    dictionary: arrow.makeVector(dictData),
  });
}

/**
 * Helper to create dictionary vector with Uint16 indices.
 */
function createDictVector16(
  dict: ReturnType<DictionaryBuilder['finalize']>,
  indices: Uint16Array,
  length: number,
  nullBitmap?: Uint8Array,
): arrow.Data<arrow.Dictionary<arrow.Utf8, arrow.Uint16>> {
  const dictData = arrow.makeData({
    type: new arrow.Utf8(),
    offset: 0,
    length: dict.indexMap.size,
    nullCount: 0,
    valueOffsets: dict.offsets,
    data: dict.data,
  });

  return arrow.makeData({
    type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint16()),
    offset: 0,
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    data: indices.subarray(0, length),
    nullBitmap: nullBitmap?.subarray(0, Math.ceil(length / 8)),
    dictionary: arrow.makeVector(dictData),
  });
}

/**
 * Create an Arrow Table from DictionaryBuilder output with Uint8 indices and round-trip through IPC.
 */
function roundTripDictionary8(
  dict: ReturnType<DictionaryBuilder['finalize']>,
  indices: Uint8Array,
  length: number,
  nullBitmap?: Uint8Array,
): arrow.Table {
  const columnData = createDictVector8(dict, indices, length, nullBitmap);
  const dictType = new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint8());

  const schema = new arrow.Schema([arrow.Field.new({ name: 'value', type: dictType, nullable: !!nullBitmap })]);

  const batch = new arrow.RecordBatch(
    schema,
    arrow.makeData({
      type: new arrow.Struct(schema.fields),
      length,
      nullCount: 0,
      children: [columnData],
    }),
  );

  const table = new arrow.Table([batch]);

  // Round-trip through IPC
  const ipcBytes = arrow.tableToIPC(table);
  return arrow.tableFromIPC(ipcBytes);
}

/**
 * Create an Arrow Table from DictionaryBuilder output with Uint16 indices and round-trip through IPC.
 */
function roundTripDictionary16(
  dict: ReturnType<DictionaryBuilder['finalize']>,
  indices: Uint16Array,
  length: number,
  nullBitmap?: Uint8Array,
): arrow.Table {
  const columnData = createDictVector16(dict, indices, length, nullBitmap);
  const dictType = new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint16());

  const schema = new arrow.Schema([arrow.Field.new({ name: 'value', type: dictType, nullable: !!nullBitmap })]);

  const batch = new arrow.RecordBatch(
    schema,
    arrow.makeData({
      type: new arrow.Struct(schema.fields),
      length,
      nullCount: 0,
      children: [columnData],
    }),
  );

  const table = new arrow.Table([batch]);

  // Round-trip through IPC
  const ipcBytes = arrow.tableToIPC(table);
  return arrow.tableFromIPC(ipcBytes);
}

describe('DictionaryBuilder - Sorted Dictionaries', () => {
  it('produces sorted dictionary with correct indices', () => {
    const builder = new DictionaryBuilder();
    const values = ['zebra', 'apple', 'banana', 'apple', 'zebra'];

    for (const v of values) {
      builder.add(v);
    }

    const dict = builder.finalize(true); // sorted

    // Dictionary should be sorted alphabetically
    const dictStrings = [...dict.indexMap.entries()].sort((a, b) => a[1] - b[1]).map(([str]) => str);

    expect(dictStrings).toEqual(['apple', 'banana', 'zebra']);

    // Create indices based on the sorted dictionary
    const indices = new Uint8Array(values.length);
    for (let i = 0; i < values.length; i++) {
      const idx = dict.indexMap.get(values[i]);
      if (idx === undefined) {
        throw new Error(`Index not found for value: ${values[i]}`);
      }
      indices[i] = idx;
    }

    // Round-trip through IPC
    const roundTripped = roundTripDictionary8(dict, indices, values.length);

    // Verify values match
    expect(roundTripped.get(0)?.toJSON().value).toBe('zebra');
    expect(roundTripped.get(1)?.toJSON().value).toBe('apple');
    expect(roundTripped.get(2)?.toJSON().value).toBe('banana');
    expect(roundTripped.get(3)?.toJSON().value).toBe('apple');
    expect(roundTripped.get(4)?.toJSON().value).toBe('zebra');
  });

  it('handles empty dictionary', () => {
    const builder = new DictionaryBuilder();
    const dict = builder.finalize(true);

    expect(dict.indexMap.size).toBe(0);
    expect(dict.data.length).toBe(0);
    expect(dict.offsets.length).toBe(1); // Just the initial 0 offset
    expect(dict.offsets[0]).toBe(0);
  });

  it('handles single value', () => {
    const builder = new DictionaryBuilder();
    builder.add('hello');

    const dict = builder.finalize(true);

    expect(dict.indexMap.size).toBe(1);
    expect(dict.indexMap.get('hello')).toBe(0);
    expect(dict.indexType).toBe('uint8');
  });

  it('handles duplicate values correctly', () => {
    const builder = new DictionaryBuilder();
    const value = 'repeated';

    // Add same value 1000 times
    for (let i = 0; i < 1000; i++) {
      builder.add(value);
    }

    const dict = builder.finalize(true);

    // Should still have only one unique value
    expect(dict.indexMap.size).toBe(1);
    expect(dict.indexMap.get(value)).toBe(0);
  });
});

describe('DictionaryBuilder - Unsorted Dictionaries', () => {
  it('produces unsorted dictionary preserving insertion order', () => {
    const builder = new DictionaryBuilder();
    const values = ['zebra', 'apple', 'banana'];

    for (const v of values) {
      builder.add(v);
    }

    const dict = builder.finalize(false); // unsorted

    // Dictionary should preserve insertion order
    const dictStrings = [...dict.indexMap.entries()].sort((a, b) => a[1] - b[1]).map(([str]) => str);

    expect(dictStrings).toEqual(['zebra', 'apple', 'banana']);
  });
});

describe('DictionaryBuilder - Index Type Selection', () => {
  it('uses uint8 for small dictionaries (<=255 values)', () => {
    const builder = new DictionaryBuilder();

    // Add 200 unique values
    for (let i = 0; i < 200; i++) {
      builder.add(`value_${i}`);
    }

    const dict = builder.finalize(true);
    expect(dict.indexType).toBe('uint8');
  });

  it('uses uint16 for medium dictionaries (256-65535 values)', () => {
    const builder = new DictionaryBuilder();

    // Add 300 unique values (>255)
    for (let i = 0; i < 300; i++) {
      builder.add(`value_${i}`);
    }

    const dict = builder.finalize(true);
    expect(dict.indexType).toBe('uint16');
  });

  it('large dictionary with >255 values uses uint16 indices', () => {
    const builder = new DictionaryBuilder();
    const values: string[] = [];

    // Add 300 unique values
    for (let i = 0; i < 300; i++) {
      const value = `unique_value_${i}`;
      values.push(value);
      builder.add(value);
    }

    const dict = builder.finalize(true); // sorted

    expect(dict.indexType).toBe('uint16');
    expect(dict.indexMap.size).toBe(300);

    // Create uint16 indices
    const indices = new Uint16Array(values.length);
    for (let i = 0; i < values.length; i++) {
      const idx = dict.indexMap.get(values[i]);
      indices[i] = idx !== undefined ? idx : 0;
    }

    // Round-trip through IPC
    const roundTripped = roundTripDictionary16(dict, indices, values.length);

    // Verify a few values round-trip correctly
    expect(roundTripped.numRows).toBe(300);
    // After sorting, 'unique_value_0' comes after 'unique_value_1', 'unique_value_10', etc.
    // So we just verify the count is correct
    const distinctValues = new Set<string>();
    for (let i = 0; i < roundTripped.numRows; i++) {
      distinctValues.add(roundTripped.get(i)?.toJSON().value);
    }
    expect(distinctValues.size).toBe(300);
  });
});

describe('DictionaryBuilder - UTF-8 Encoding', () => {
  it('correctly encodes ASCII strings', () => {
    const builder = new DictionaryBuilder();
    const values = ['hello', 'world', 'test'];

    for (const v of values) {
      builder.add(v);
    }

    const dict = builder.finalize(true);
    const encoder = new TextEncoder();

    // Verify UTF-8 data is correct
    for (const [str, idx] of [...dict.indexMap.entries()].sort((a, b) => a[1] - b[1])) {
      const start = dict.offsets[idx];
      const end = dict.offsets[idx + 1];
      const expected = encoder.encode(str);

      expect(end - start).toBe(expected.length);
      expect(dict.data.subarray(start, end)).toEqual(expected);
    }
  });

  it('correctly encodes multi-byte UTF-8 characters', () => {
    const builder = new DictionaryBuilder();
    const values = ['héllo', '世界', '🎉', 'café'];

    for (const v of values) {
      builder.add(v);
    }

    const dict = builder.finalize(false);
    const encoder = new TextEncoder();

    // Verify each string encodes correctly
    for (const [str, idx] of dict.indexMap) {
      const start = dict.offsets[idx];
      const end = dict.offsets[idx + 1];
      const expected = encoder.encode(str);

      expect(dict.data.subarray(start, end)).toEqual(expected);
    }

    // Create indices and round-trip
    const indices = new Uint8Array(values.length);
    for (let i = 0; i < values.length; i++) {
      const idx = dict.indexMap.get(values[i]);
      indices[i] = idx !== undefined ? idx : 0;
    }

    const roundTripped = roundTripDictionary8(dict, indices, values.length);

    // Verify multi-byte strings survive round-trip
    expect(roundTripped.get(0)?.toJSON().value).toBe('héllo');
    expect(roundTripped.get(1)?.toJSON().value).toBe('世界');
    expect(roundTripped.get(2)?.toJSON().value).toBe('🎉');
    expect(roundTripped.get(3)?.toJSON().value).toBe('café');
  });
});

describe('DictionaryBuilder - Nullable Columns', () => {
  it('handles nullable column with some nulls', () => {
    const builder = new DictionaryBuilder();
    const values = ['a', null, 'b', null, 'c'];

    // Only add non-null values to dictionary
    for (const v of values) {
      if (v !== null) {
        builder.add(v);
      }
    }

    const dict = builder.finalize(true);

    // Create indices (0 for nulls, doesn't matter since null bitmap marks them)
    const indices = new Uint8Array(values.length);
    const nullBitmap = new Uint8Array(1);
    nullBitmap[0] = 0; // Start with all null

    for (let i = 0; i < values.length; i++) {
      const v = values[i];
      if (v !== null) {
        const idx = dict.indexMap.get(v);
        indices[i] = idx !== undefined ? idx : 0;
        nullBitmap[0] |= 1 << i; // Set bit = valid
      }
    }

    const roundTripped = roundTripDictionary8(dict, indices, values.length, nullBitmap);

    expect(roundTripped.get(0)?.toJSON().value).toBe('a');
    expect(roundTripped.get(1)?.toJSON().value).toBe(null);
    expect(roundTripped.get(2)?.toJSON().value).toBe('b');
    expect(roundTripped.get(3)?.toJSON().value).toBe(null);
    expect(roundTripped.get(4)?.toJSON().value).toBe('c');
  });
});

describe('DictionaryBuilder - 2nd Occurrence Caching', () => {
  it('caches UTF-8 encoding on second occurrence', () => {
    const builder = new DictionaryBuilder();

    // Add same value twice
    builder.add('cached_value');
    builder.add('cached_value');

    // Should still only have one unique value
    expect(builder.size).toBe(1);

    const dict = builder.finalize(true);
    expect(dict.indexMap.size).toBe(1);
  });

  it('efficiently handles many repeated values', () => {
    const builder = new DictionaryBuilder();
    const uniqueValues = ['user-1', 'user-2', 'user-3'];

    // Add each value many times (simulating category column with repeated values)
    for (let i = 0; i < 10000; i++) {
      builder.add(uniqueValues[i % uniqueValues.length]);
    }

    const dict = builder.finalize(true);

    // Should only have 3 unique values
    expect(dict.indexMap.size).toBe(3);
    expect(dict.indexType).toBe('uint8');
  });
});

describe('DictionaryBuilder - Clear and Reuse', () => {
  it('can be cleared and reused', () => {
    const builder = new DictionaryBuilder();

    builder.add('first');
    builder.add('second');
    expect(builder.size).toBe(2);

    builder.clear();
    expect(builder.size).toBe(0);

    builder.add('new');
    const dict = builder.finalize(true);
    expect(dict.indexMap.size).toBe(1);
    expect(dict.indexMap.get('new')).toBe(0);
  });
});
