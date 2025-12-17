/**
 * Arrow Data creation utilities.
 *
 * Creates arrow.Data directly from TypedArrays - no builder pattern.
 * Following the pattern from arrow-js-ffi.
 */

import {
  Bool,
  type Data,
  Dictionary,
  Float64,
  Int32,
  makeData,
  makeVector,
  Uint8,
  Uint16,
  Uint32,
  Utf8,
} from 'apache-arrow';
import { countNulls } from './nullBitmap.js';

/** Create Arrow Data for Uint8 column */
export function createUint8Data(values: Uint8Array, length: number, nullBitmap?: Uint8Array): Data<Uint8> {
  return makeData({
    type: new Uint8(),
    offset: 0,
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    data: values.subarray(0, length),
    nullBitmap: nullBitmap?.subarray(0, Math.ceil(length / 8)),
  });
}

/** Create Arrow Data for Uint16 column */
export function createUint16Data(values: Uint16Array, length: number, nullBitmap?: Uint8Array): Data<Uint16> {
  return makeData({
    type: new Uint16(),
    offset: 0,
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    data: values.subarray(0, length),
    nullBitmap: nullBitmap?.subarray(0, Math.ceil(length / 8)),
  });
}

/** Create Arrow Data for Uint32 column */
export function createUint32Data(values: Uint32Array, length: number, nullBitmap?: Uint8Array): Data<Uint32> {
  return makeData({
    type: new Uint32(),
    offset: 0,
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    data: values.subarray(0, length),
    nullBitmap: nullBitmap?.subarray(0, Math.ceil(length / 8)),
  });
}

/** Create Arrow Data for Int32 column */
export function createInt32Data(values: Int32Array, length: number, nullBitmap?: Uint8Array): Data<Int32> {
  return makeData({
    type: new Int32(),
    offset: 0,
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    data: values.subarray(0, length),
    nullBitmap: nullBitmap?.subarray(0, Math.ceil(length / 8)),
  });
}

/** Create Arrow Data for Float64 column */
export function createFloat64Data(values: Float64Array, length: number, nullBitmap?: Uint8Array): Data<Float64> {
  return makeData({
    type: new Float64(),
    offset: 0,
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    data: values.subarray(0, length),
    nullBitmap: nullBitmap?.subarray(0, Math.ceil(length / 8)),
  });
}

/** Create Arrow Data for Bool column (bit-packed) */
export function createBoolData(values: Uint8Array, length: number, nullBitmap?: Uint8Array): Data<Bool> {
  return makeData({
    type: new Bool(),
    offset: 0,
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    data: values.subarray(0, Math.ceil(length / 8)),
    nullBitmap: nullBitmap?.subarray(0, Math.ceil(length / 8)),
  });
}

/** Create Arrow Data for Utf8 (string) column */
export function createUtf8Data(
  data: Uint8Array,
  offsets: Int32Array,
  length: number,
  nullBitmap?: Uint8Array,
): Data<Utf8> {
  return makeData({
    type: new Utf8(),
    offset: 0,
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    data,
    valueOffsets: offsets,
    nullBitmap: nullBitmap?.subarray(0, Math.ceil(length / 8)),
  });
}

/** Create Arrow Data for Dictionary column with Uint8 indices */
export function createDictionary8Data(
  indices: Uint8Array,
  dictData: Uint8Array,
  dictOffsets: Int32Array,
  length: number,
  nullBitmap?: Uint8Array,
): Data<Dictionary<Utf8, Uint8>> {
  const dictLength = dictOffsets.length - 1;
  const utf8DictData = createUtf8Data(dictData, dictOffsets, dictLength);

  return makeData({
    type: new Dictionary(new Utf8(), new Uint8()),
    offset: 0,
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    data: indices.subarray(0, length),
    nullBitmap: nullBitmap?.subarray(0, Math.ceil(length / 8)),
    dictionary: makeVector(utf8DictData),
  });
}

/** Create Arrow Data for Dictionary column with Uint16 indices */
export function createDictionary16Data(
  indices: Uint16Array,
  dictData: Uint8Array,
  dictOffsets: Int32Array,
  length: number,
  nullBitmap?: Uint8Array,
): Data<Dictionary<Utf8, Uint16>> {
  const dictLength = dictOffsets.length - 1;
  const utf8DictData = createUtf8Data(dictData, dictOffsets, dictLength);

  return makeData({
    type: new Dictionary(new Utf8(), new Uint16()),
    offset: 0,
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    data: indices.subarray(0, length),
    nullBitmap: nullBitmap?.subarray(0, Math.ceil(length / 8)),
    dictionary: makeVector(utf8DictData),
  });
}

/** Create Arrow Data for Dictionary column with Uint32 indices */
export function createDictionary32Data(
  indices: Uint32Array,
  dictData: Uint8Array,
  dictOffsets: Int32Array,
  length: number,
  nullBitmap?: Uint8Array,
): Data<Dictionary<Utf8, Uint32>> {
  const dictLength = dictOffsets.length - 1;
  const utf8DictData = createUtf8Data(dictData, dictOffsets, dictLength);

  return makeData({
    type: new Dictionary(new Utf8(), new Uint32()),
    offset: 0,
    length,
    nullCount: nullBitmap ? countNulls(nullBitmap, length) : 0,
    data: indices.subarray(0, length),
    nullBitmap: nullBitmap?.subarray(0, Math.ceil(length / 8)),
    dictionary: makeVector(utf8DictData),
  });
}
