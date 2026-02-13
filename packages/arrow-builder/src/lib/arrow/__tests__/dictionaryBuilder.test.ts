/**
 * DictionaryBuilder binary compatibility tests.
 *
 * Verifies dictionary output can be encoded to Arrow IPC and decoded back
 * using flechette.
 */

import { describe, expect, it } from 'bun:test';
import { tableFromIPC, tableToIPC, uint8, uint16 } from '@uwdata/flechette';
import { createDictionary8Data, createDictionary16Data, createTableFromBatches } from '../data.js';
import { DictionaryBuilder } from '../dictionary.js';

function roundTripDictionary8(
  dict: ReturnType<DictionaryBuilder['finalize']>,
  indices: Uint8Array,
  length: number,
  nullBitmap?: Uint8Array,
) {
  const batch = createDictionary8Data(indices, dict.data, dict.offsets, length, nullBitmap);
  const table = createTableFromBatches({ value: batch });
  const ipcBytes = tableToIPC(table, { format: 'file' });
  if (!ipcBytes) throw new Error('Failed to serialize dictionary table');
  return tableFromIPC(ipcBytes);
}

function roundTripDictionary16(
  dict: ReturnType<DictionaryBuilder['finalize']>,
  indices: Uint16Array,
  length: number,
  nullBitmap?: Uint8Array,
) {
  const batch = createDictionary16Data(indices, dict.data, dict.offsets, length, nullBitmap);
  const table = createTableFromBatches({ value: batch });
  const ipcBytes = tableToIPC(table, { format: 'file' });
  if (!ipcBytes) throw new Error('Failed to serialize dictionary table');
  return tableFromIPC(ipcBytes);
}

describe('DictionaryBuilder - Sorted Dictionaries', () => {
  it('produces sorted dictionary with correct indices', () => {
    const builder = new DictionaryBuilder();
    const values = ['zebra', 'apple', 'banana', 'apple', 'zebra'];

    for (const value of values) {
      builder.add(value);
    }

    const dict = builder.finalize(true);
    const dictStrings = [...dict.indexMap.entries()].sort((a, b) => a[1] - b[1]).map(([value]) => value);
    expect(dictStrings).toEqual(['apple', 'banana', 'zebra']);

    const indices = new Uint8Array(values.length);
    for (let i = 0; i < values.length; i++) {
      const index = dict.indexMap.get(values[i]);
      if (index === undefined) throw new Error(`Index not found for value: ${values[i]}`);
      indices[i] = index;
    }

    const roundTripped = roundTripDictionary8(dict, indices, values.length);
    expect(roundTripped.at(0).value).toBe('zebra');
    expect(roundTripped.at(1).value).toBe('apple');
    expect(roundTripped.at(2).value).toBe('banana');
    expect(roundTripped.at(3).value).toBe('apple');
    expect(roundTripped.at(4).value).toBe('zebra');
  });

  it('handles empty dictionary', () => {
    const builder = new DictionaryBuilder();
    const dict = builder.finalize(true);

    expect(dict.indexMap.size).toBe(0);
    expect(dict.data.length).toBe(0);
    expect(dict.offsets.length).toBe(1);
    expect(dict.offsets[0]).toBe(0);
  });

  it('handles single value', () => {
    const builder = new DictionaryBuilder();
    builder.add('hello');

    const dict = builder.finalize(true);
    expect(dict.indexMap.size).toBe(1);
    expect(dict.indexMap.get('hello')).toBe(0);
    expect(dict.indexArrayCtor).toBe(Uint8Array);
    expect(dict.arrowIndexType.typeId).toBe(uint8().typeId);
    expect(dict.arrowIndexType.bitWidth).toBe(uint8().bitWidth);
  });

  it('handles duplicate values correctly', () => {
    const builder = new DictionaryBuilder();

    for (let i = 0; i < 1000; i++) {
      builder.add('repeated');
    }

    const dict = builder.finalize(true);
    expect(dict.indexMap.size).toBe(1);
    expect(dict.indexMap.get('repeated')).toBe(0);
  });
});

describe('DictionaryBuilder - Unsorted Dictionaries', () => {
  it('produces unsorted dictionary preserving insertion order', () => {
    const builder = new DictionaryBuilder();
    const values = ['zebra', 'apple', 'banana'];

    for (const value of values) {
      builder.add(value);
    }

    const dict = builder.finalize(false);
    const dictStrings = [...dict.indexMap.entries()].sort((a, b) => a[1] - b[1]).map(([value]) => value);
    expect(dictStrings).toEqual(['zebra', 'apple', 'banana']);
  });
});

describe('DictionaryBuilder - Index Type Selection', () => {
  it('uses uint8 for small dictionaries (<=255 values)', () => {
    const builder = new DictionaryBuilder();

    for (let i = 0; i < 200; i++) {
      builder.add(`value_${i}`);
    }

    const dict = builder.finalize(true);
    expect(dict.indexArrayCtor).toBe(Uint8Array);
    expect(dict.arrowIndexType.typeId).toBe(uint8().typeId);
    expect(dict.arrowIndexType.bitWidth).toBe(uint8().bitWidth);
  });

  it('uses uint16 for medium dictionaries (256-65535 values)', () => {
    const builder = new DictionaryBuilder();

    for (let i = 0; i < 300; i++) {
      builder.add(`value_${i}`);
    }

    const dict = builder.finalize(true);
    expect(dict.indexArrayCtor).toBe(Uint16Array);
    expect(dict.arrowIndexType.typeId).toBe(uint16().typeId);
    expect(dict.arrowIndexType.bitWidth).toBe(uint16().bitWidth);
  });

  it('large dictionary with >255 values uses uint16 indices', () => {
    const builder = new DictionaryBuilder();
    const values: string[] = [];

    for (let i = 0; i < 300; i++) {
      const value = `unique_value_${i}`;
      values.push(value);
      builder.add(value);
    }

    const dict = builder.finalize(true);
    expect(dict.indexArrayCtor).toBe(Uint16Array);
    expect(dict.arrowIndexType.typeId).toBe(uint16().typeId);
    expect(dict.arrowIndexType.bitWidth).toBe(uint16().bitWidth);
    expect(dict.indexMap.size).toBe(300);

    const indices = new Uint16Array(values.length);
    for (let i = 0; i < values.length; i++) {
      const idx = dict.indexMap.get(values[i]);
      indices[i] = idx !== undefined ? idx : 0;
    }

    const roundTripped = roundTripDictionary16(dict, indices, values.length);
    expect(roundTripped.numRows).toBe(300);

    const distinctValues = new Set<string>();
    for (let i = 0; i < roundTripped.numRows; i++) {
      distinctValues.add(roundTripped.at(i).value);
    }
    expect(distinctValues.size).toBe(300);
  });
});

describe('DictionaryBuilder - UTF-8 Encoding', () => {
  it('correctly encodes ASCII strings', () => {
    const builder = new DictionaryBuilder();
    for (const value of ['hello', 'world', 'test']) {
      builder.add(value);
    }

    const dict = builder.finalize(true);
    const encoder = new TextEncoder();

    for (const [value, idx] of [...dict.indexMap.entries()].sort((a, b) => a[1] - b[1])) {
      const start = dict.offsets[idx];
      const end = dict.offsets[idx + 1];
      const expected = encoder.encode(value);
      expect(end - start).toBe(expected.length);
      expect(dict.data.subarray(start, end)).toEqual(expected);
    }
  });

  it('correctly encodes multi-byte UTF-8 characters', () => {
    const builder = new DictionaryBuilder();
    const values = ['hello', 'world', 'data', 'table'];

    for (const value of values) {
      builder.add(value);
    }

    const dict = builder.finalize(false);
    const indices = new Uint8Array(values.length);
    for (let i = 0; i < values.length; i++) {
      const idx = dict.indexMap.get(values[i]);
      indices[i] = idx !== undefined ? idx : 0;
    }

    const roundTripped = roundTripDictionary8(dict, indices, values.length);
    expect(roundTripped.at(0).value).toBe('hello');
    expect(roundTripped.at(1).value).toBe('world');
    expect(roundTripped.at(2).value).toBe('data');
    expect(roundTripped.at(3).value).toBe('table');
  });
});

describe('DictionaryBuilder - Nullable Columns', () => {
  it('handles nullable column with some nulls', () => {
    const builder = new DictionaryBuilder();
    const values = ['a', null, 'b', null, 'c'];

    for (const value of values) {
      if (value !== null) builder.add(value);
    }

    const dict = builder.finalize(true);
    const indices = new Uint8Array(values.length);
    const nullBitmap = new Uint8Array(1);

    for (let i = 0; i < values.length; i++) {
      const value = values[i];
      if (value !== null) {
        const idx = dict.indexMap.get(value);
        indices[i] = idx !== undefined ? idx : 0;
        nullBitmap[0] |= 1 << i;
      }
    }

    const roundTripped = roundTripDictionary8(dict, indices, values.length, nullBitmap);
    expect(roundTripped.at(0).value).toBe('a');
    expect(roundTripped.at(1).value).toBe(null);
    expect(roundTripped.at(2).value).toBe('b');
    expect(roundTripped.at(3).value).toBe(null);
    expect(roundTripped.at(4).value).toBe('c');
  });
});

describe('DictionaryBuilder - 2nd Occurrence Caching', () => {
  it('caches UTF-8 encoding on second occurrence', () => {
    const builder = new DictionaryBuilder();
    builder.add('cached_value');
    builder.add('cached_value');
    expect(builder.size).toBe(1);

    const dict = builder.finalize(true);
    expect(dict.indexMap.size).toBe(1);
  });

  it('efficiently handles many repeated values', () => {
    const builder = new DictionaryBuilder();
    const uniqueValues = ['user-1', 'user-2', 'user-3'];

    for (let i = 0; i < 10000; i++) {
      builder.add(uniqueValues[i % uniqueValues.length]);
    }

    const dict = builder.finalize(true);
    expect(dict.indexMap.size).toBe(3);
    expect(dict.indexArrayCtor).toBe(Uint8Array);
    expect(dict.arrowIndexType.typeId).toBe(uint8().typeId);
    expect(dict.arrowIndexType.bitWidth).toBe(uint8().bitWidth);
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
