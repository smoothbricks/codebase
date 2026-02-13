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
  tableFromColumns,
  uint8,
  uint16,
  uint32,
  utf8,
} from '@uwdata/flechette';
import { countNulls } from './nullBitmap.js';

const EMPTY_VALIDITY = new Uint8Array(0);

const UINT8_TYPE = uint8();
const UINT16_TYPE = uint16();
const UINT32_TYPE = uint32();
const INT32_TYPE = int32();
const FLOAT64_TYPE = float64();
const BOOL_TYPE = bool();
const UTF8_TYPE = utf8();

const UINT8_BATCH = batchType(UINT8_TYPE);
const UINT16_BATCH = batchType(UINT16_TYPE);
const UINT32_BATCH = batchType(UINT32_TYPE);
const INT32_BATCH = batchType(INT32_TYPE);
const FLOAT64_BATCH = batchType(FLOAT64_TYPE);
const BOOL_BATCH = batchType(BOOL_TYPE);
const UTF8_BATCH = batchType(UTF8_TYPE);

const DICT8_BATCH = batchType(dictionary(UTF8_TYPE, UINT8_TYPE));
const DICT16_BATCH = batchType(dictionary(UTF8_TYPE, UINT16_TYPE));
const DICT32_BATCH = batchType(dictionary(UTF8_TYPE, UINT32_TYPE));

function createValidity(length: number, nullBitmap?: Uint8Array): Uint8Array {
  return nullBitmap?.subarray(0, Math.ceil(length / 8)) ?? EMPTY_VALIDITY;
}

export type ArrowBatch = Batch<unknown>;

type DictionaryBatchLike = Batch<string> & {
  dictionary?: Column<string>;
};

function attachDictionaryWithoutEagerCache(batch: DictionaryBatchLike, dictionaryColumn: Column<string>): void {
  // Avoid DictionaryBatch.setDictionary(), which eagerly materializes dictionary.cache().
  // We only need dictionary metadata for encoding in this path.
  batch.dictionary = dictionaryColumn;
}

/** Create Arrow Batch for Uint8 column */
export function createUint8Data(values: Uint8Array, length: number, nullBitmap?: Uint8Array): ArrowBatch {
  const batch = new UINT8_BATCH({
    type: UINT8_TYPE,
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    values: values.subarray(0, length),
    validity: createValidity(length, nullBitmap),
  });

  return batch;
}

/** Create Arrow Batch for Uint16 column */
export function createUint16Data(values: Uint16Array, length: number, nullBitmap?: Uint8Array): ArrowBatch {
  const batch = new UINT16_BATCH({
    type: UINT16_TYPE,
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    values: values.subarray(0, length),
    validity: createValidity(length, nullBitmap),
  });

  return batch;
}

/** Create Arrow Batch for Uint32 column */
export function createUint32Data(values: Uint32Array, length: number, nullBitmap?: Uint8Array): ArrowBatch {
  const batch = new UINT32_BATCH({
    type: UINT32_TYPE,
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    values: values.subarray(0, length),
    validity: createValidity(length, nullBitmap),
  });

  return batch;
}

/** Create Arrow Batch for Int32 column */
export function createInt32Data(values: Int32Array, length: number, nullBitmap?: Uint8Array): ArrowBatch {
  const batch = new INT32_BATCH({
    type: INT32_TYPE,
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    values: values.subarray(0, length),
    validity: createValidity(length, nullBitmap),
  });

  return batch;
}

/** Create Arrow Batch for Float64 column */
export function createFloat64Data(values: Float64Array, length: number, nullBitmap?: Uint8Array): ArrowBatch {
  const batch = new FLOAT64_BATCH({
    type: FLOAT64_TYPE,
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    values: values.subarray(0, length),
    validity: createValidity(length, nullBitmap),
  });

  return batch;
}

/** Create Arrow Batch for Bool column (bit-packed) */
export function createBoolData(values: Uint8Array, length: number, nullBitmap?: Uint8Array): ArrowBatch {
  const batch = new BOOL_BATCH({
    type: BOOL_TYPE,
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    values: values.subarray(0, Math.ceil(length / 8)),
    validity: createValidity(length, nullBitmap),
  });

  return batch;
}

/** Create Arrow Batch for Utf8 (string) column */
export function createUtf8Data(
  data: Uint8Array,
  offsets: Int32Array,
  length: number,
  nullBitmap?: Uint8Array,
): ArrowBatch {
  const batch = new UTF8_BATCH({
    type: UTF8_TYPE,
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    values: data,
    offsets,
    validity: createValidity(length, nullBitmap),
  });

  return batch;
}

/** Create Arrow Batch for Dictionary column with Uint8 indices */
export function createDictionary8Data(
  indices: Uint8Array,
  dictData: Uint8Array,
  dictOffsets: Int32Array,
  length: number,
  nullBitmap?: Uint8Array,
): ArrowBatch {
  const dictionaryValueType = utf8();
  const dictLength = dictOffsets.length - 1;
  const utf8Batch = new UTF8_BATCH({
    type: dictionaryValueType,
    length: dictLength,
    nullCount: 0,
    values: dictData,
    offsets: dictOffsets,
    validity: EMPTY_VALIDITY,
  });
  const dictColumn = new Column([utf8Batch]) as Column<string>;

  const dictionaryType = dictionary(dictionaryValueType, UINT8_TYPE);
  const dictBatch = new DICT8_BATCH({
    type: dictionaryType,
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    values: indices.subarray(0, length),
    validity: createValidity(length, nullBitmap),
  });
  attachDictionaryWithoutEagerCache(dictBatch as DictionaryBatchLike, dictColumn);

  return dictBatch;
}

/** Create Arrow Batch for Dictionary column with Uint16 indices */
export function createDictionary16Data(
  indices: Uint16Array,
  dictData: Uint8Array,
  dictOffsets: Int32Array,
  length: number,
  nullBitmap?: Uint8Array,
): ArrowBatch {
  const dictionaryValueType = utf8();
  const dictLength = dictOffsets.length - 1;
  const utf8Batch = new UTF8_BATCH({
    type: dictionaryValueType,
    length: dictLength,
    nullCount: 0,
    values: dictData,
    offsets: dictOffsets,
    validity: EMPTY_VALIDITY,
  });
  const dictColumn = new Column([utf8Batch]) as Column<string>;

  const dictionaryType = dictionary(dictionaryValueType, UINT16_TYPE);
  const dictBatch = new DICT16_BATCH({
    type: dictionaryType,
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    values: indices.subarray(0, length),
    validity: createValidity(length, nullBitmap),
  });
  attachDictionaryWithoutEagerCache(dictBatch as DictionaryBatchLike, dictColumn);

  return dictBatch;
}

/** Create Arrow Batch for Dictionary column with Uint32 indices */
export function createDictionary32Data(
  indices: Uint32Array,
  dictData: Uint8Array,
  dictOffsets: Int32Array,
  length: number,
  nullBitmap?: Uint8Array,
): ArrowBatch {
  const dictionaryValueType = utf8();
  const dictLength = dictOffsets.length - 1;
  const utf8Batch = new UTF8_BATCH({
    type: dictionaryValueType,
    length: dictLength,
    nullCount: 0,
    values: dictData,
    offsets: dictOffsets,
    validity: EMPTY_VALIDITY,
  });
  const dictColumn = new Column([utf8Batch]) as Column<string>;

  const dictionaryType = dictionary(dictionaryValueType, UINT32_TYPE);
  const dictBatch = new DICT32_BATCH({
    type: dictionaryType,
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    values: indices.subarray(0, length),
    validity: createValidity(length, nullBitmap),
  });
  attachDictionaryWithoutEagerCache(dictBatch as DictionaryBatchLike, dictColumn);

  return dictBatch;
}

/** Create flechette Table from a record of single-batch columns. */
export function createTableFromBatches(batches: Record<string, ArrowBatch>) {
  const columns: Record<string, Column<unknown>> = {};
  for (const [name, batch] of Object.entries(batches)) {
    columns[name] = new Column([batch]);
  }
  return tableFromColumns(columns);
}
