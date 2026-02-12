/**
 * Arrow Data creation utilities.
 *
 * Creates flechette Columns directly from TypedArrays - no builder pattern.
 */

import {
  type Batch,
  batchType,
  bool,
  Column,
  dictionary,
  float64,
  int32,
  uint8,
  uint16,
  uint32,
  utf8,
} from '@uwdata/flechette';
import { countNulls } from './nullBitmap.js';

function createValidity(length: number, nullBitmap?: Uint8Array): Uint8Array {
  return nullBitmap?.subarray(0, Math.ceil(length / 8)) ?? new Uint8Array(0);
}

type ArrowColumn = Column<unknown>;

type DictionaryBatchLike = Batch<string> & {
  setDictionary(dictionaryColumn: Column<string>): void;
};

/** Create Arrow Column for Uint8 column */
export function createUint8Data(values: Uint8Array, length: number, nullBitmap?: Uint8Array): ArrowColumn {
  const BatchCtor = batchType(uint8());
  const batch = new BatchCtor({
    type: uint8(),
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    values: values.subarray(0, length),
    validity: createValidity(length, nullBitmap),
  });

  return new Column([batch]);
}

/** Create Arrow Column for Uint16 column */
export function createUint16Data(values: Uint16Array, length: number, nullBitmap?: Uint8Array): ArrowColumn {
  const BatchCtor = batchType(uint16());
  const batch = new BatchCtor({
    type: uint16(),
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    values: values.subarray(0, length),
    validity: createValidity(length, nullBitmap),
  });

  return new Column([batch]);
}

/** Create Arrow Column for Uint32 column */
export function createUint32Data(values: Uint32Array, length: number, nullBitmap?: Uint8Array): ArrowColumn {
  const BatchCtor = batchType(uint32());
  const batch = new BatchCtor({
    type: uint32(),
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    values: values.subarray(0, length),
    validity: createValidity(length, nullBitmap),
  });

  return new Column([batch]);
}

/** Create Arrow Column for Int32 column */
export function createInt32Data(values: Int32Array, length: number, nullBitmap?: Uint8Array): ArrowColumn {
  const BatchCtor = batchType(int32());
  const batch = new BatchCtor({
    type: int32(),
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    values: values.subarray(0, length),
    validity: createValidity(length, nullBitmap),
  });

  return new Column([batch]);
}

/** Create Arrow Column for Float64 column */
export function createFloat64Data(values: Float64Array, length: number, nullBitmap?: Uint8Array): ArrowColumn {
  const BatchCtor = batchType(float64());
  const batch = new BatchCtor({
    type: float64(),
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    values: values.subarray(0, length),
    validity: createValidity(length, nullBitmap),
  });

  return new Column([batch]);
}

/** Create Arrow Column for Bool column (bit-packed) */
export function createBoolData(values: Uint8Array, length: number, nullBitmap?: Uint8Array): ArrowColumn {
  const BatchCtor = batchType(bool());
  const batch = new BatchCtor({
    type: bool(),
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    values: values.subarray(0, Math.ceil(length / 8)),
    validity: createValidity(length, nullBitmap),
  });

  return new Column([batch]);
}

/** Create Arrow Column for Utf8 (string) column */
export function createUtf8Data(
  data: Uint8Array,
  offsets: Int32Array,
  length: number,
  nullBitmap?: Uint8Array,
): ArrowColumn {
  const BatchCtor = batchType(utf8());
  const batch = new BatchCtor({
    type: utf8(),
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    values: data,
    offsets,
    validity: createValidity(length, nullBitmap),
  });

  return new Column([batch]);
}

/** Create Arrow Column for Dictionary column with Uint8 indices */
export function createDictionary8Data(
  indices: Uint8Array,
  dictData: Uint8Array,
  dictOffsets: Int32Array,
  length: number,
  nullBitmap?: Uint8Array,
): ArrowColumn {
  const dictLength = dictOffsets.length - 1;
  const Utf8BatchCtor = batchType(utf8());
  const utf8Batch = new Utf8BatchCtor({
    type: utf8(),
    length: dictLength,
    nullCount: 0,
    values: dictData,
    offsets: dictOffsets,
    validity: new Uint8Array(0),
  });
  const dictColumn = new Column([utf8Batch]) as Column<string>;

  const dictionaryType = dictionary(utf8(), uint8());
  const DictionaryBatchCtor = batchType(dictionaryType);
  const dictBatch = new DictionaryBatchCtor({
    type: dictionaryType,
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    values: indices.subarray(0, length),
    validity: createValidity(length, nullBitmap),
  });
  (dictBatch as DictionaryBatchLike).setDictionary(dictColumn);

  return new Column([dictBatch]);
}

/** Create Arrow Column for Dictionary column with Uint16 indices */
export function createDictionary16Data(
  indices: Uint16Array,
  dictData: Uint8Array,
  dictOffsets: Int32Array,
  length: number,
  nullBitmap?: Uint8Array,
): ArrowColumn {
  const dictLength = dictOffsets.length - 1;
  const Utf8BatchCtor = batchType(utf8());
  const utf8Batch = new Utf8BatchCtor({
    type: utf8(),
    length: dictLength,
    nullCount: 0,
    values: dictData,
    offsets: dictOffsets,
    validity: new Uint8Array(0),
  });
  const dictColumn = new Column([utf8Batch]) as Column<string>;

  const dictionaryType = dictionary(utf8(), uint16());
  const DictionaryBatchCtor = batchType(dictionaryType);
  const dictBatch = new DictionaryBatchCtor({
    type: dictionaryType,
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    values: indices.subarray(0, length),
    validity: createValidity(length, nullBitmap),
  });
  (dictBatch as DictionaryBatchLike).setDictionary(dictColumn);

  return new Column([dictBatch]);
}

/** Create Arrow Column for Dictionary column with Uint32 indices */
export function createDictionary32Data(
  indices: Uint32Array,
  dictData: Uint8Array,
  dictOffsets: Int32Array,
  length: number,
  nullBitmap?: Uint8Array,
): ArrowColumn {
  const dictLength = dictOffsets.length - 1;
  const Utf8BatchCtor = batchType(utf8());
  const utf8Batch = new Utf8BatchCtor({
    type: utf8(),
    length: dictLength,
    nullCount: 0,
    values: dictData,
    offsets: dictOffsets,
    validity: new Uint8Array(0),
  });
  const dictColumn = new Column([utf8Batch]) as Column<string>;

  const dictionaryType = dictionary(utf8(), uint32());
  const DictionaryBatchCtor = batchType(dictionaryType);
  const dictBatch = new DictionaryBatchCtor({
    type: dictionaryType,
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    values: indices.subarray(0, length),
    validity: createValidity(length, nullBitmap),
  });
  (dictBatch as DictionaryBatchLike).setDictionary(dictColumn);

  return new Column([dictBatch]);
}
