/**
 * Zero-copy conversion from SpanBuffer to Apache Arrow tables
 *
 * Per specs/01f_arrow_table_structure.md:
 * - Enum columns: Dictionary with compile-time values
 * - Category columns: Dictionary with runtime-built values
 * - Text columns: Plain strings without dictionary
 * - Zero-copy wrap TypedArrays as Arrow vectors
 */

import type { TypedArray } from '@smoothbricks/arrow-builder';
import * as arrow from 'apache-arrow';
import { ENTRY_TYPE_NAMES } from './lmao.js';
import { getEnumUtf8, getEnumValues, getLmaoSchemaType } from './schema/typeGuards.js';
import type { SpanBuffer } from './types.js';
import { globalUtf8Cache } from './utf8Cache.js';

/**
 * String interner interface (matches lmao's implementation)
 */
export interface StringInterner {
  getStrings(): readonly string[];
  getString(idx: number): string | undefined;
}

/**
 * Zero-copy helper: Create Arrow Data from TypedArray without copying
 */
export function createZeroCopyData<T extends arrow.DataType>(
  type: T,
  data: TypedArray,
  length: number,
  nullBitmap?: Uint8Array,
): arrow.Data<T> {
  const slicedData = data.subarray(0, length);
  const nullCount = nullBitmap ? countNulls(nullBitmap, length) : 0;
  const slicedNullBitmap = nullBitmap ? nullBitmap.subarray(0, Math.ceil(length / 8)) : undefined;

  return arrow.makeData({
    type,
    offset: 0,
    length,
    nullCount,
    data: slicedData,
    nullBitmap: slicedNullBitmap,
  } as any) as arrow.Data<T>;
}

function countNulls(nullBitmap: Uint8Array, length: number): number {
  let count = 0;
  for (let i = 0; i < length; i++) {
    const byteIndex = Math.floor(i / 8);
    const bitOffset = i % 8;
    const isNotNull = (nullBitmap[byteIndex] & (1 << bitOffset)) !== 0;
    if (!isNotNull) count++;
  }
  return count;
}

/**
 * Clear a range of bits in a null bitmap (set them to 0 = null).
 *
 * PRECONDITION: startBit must be byte-aligned (startBit % 8 === 0).
 * This is guaranteed because buffer capacities are always multiples of 8,
 * and this function is called with rowOffset which accumulates by writeIndex
 * where writeIndex <= capacity for full buffers.
 */
function clearBitRange(bitmap: Uint8Array, startBit: number, count: number): void {
  if (count === 0) return;

  const startByte = startBit >>> 3;
  const fullBytes = count >>> 3;
  const remainingBits = count & 7;

  // Clear full bytes
  if (fullBytes > 0) {
    bitmap.fill(0, startByte, startByte + fullBytes);
  }

  // Clear remaining bits in the last partial byte
  if (remainingBits > 0) {
    const mask = (1 << remainingBits) - 1; // Bits 0 to remainingBits-1
    bitmap[startByte + fullBytes] &= ~mask;
  }
}

function encodeUtf8Strings(strings: readonly string[]): Uint8Array {
  const encoder = new TextEncoder();
  const encoded = strings.map((s) => encoder.encode(s));
  const totalLength = encoded.reduce((sum, arr) => sum + arr.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const arr of encoded) {
    result.set(arr, offset);
    offset += arr.length;
  }
  return result;
}

function calculateUtf8Offsets(strings: readonly string[]): Int32Array {
  const encoder = new TextEncoder();
  const offsets = new Int32Array(strings.length + 1);
  offsets[0] = 0;
  for (let i = 0; i < strings.length; i++) {
    const byteLength = encoder.encode(strings[i]).length;
    offsets[i + 1] = offsets[i] + byteLength;
  }
  return offsets;
}

function concatenateTypedArrays<T extends TypedArray>(arrays: T[]): T {
  if (arrays.length === 0) throw new Error('Cannot concatenate empty array list');
  if (arrays.length === 1) return arrays[0];

  const totalLength = arrays.reduce((sum, arr) => sum + arr.length, 0);
  const result = new (arrays[0].constructor as any)(totalLength);
  let offset = 0;
  for (const arr of arrays) {
    result.set(arr, offset);
    offset += arr.length;
  }
  return result;
}

function concatenateNullBitmaps(
  buffers: SpanBuffer[],
  columnName: `attr_${string}`,
): { nullBitmap: Uint8Array | undefined; nullCount: number } {
  const nullsName = `${columnName}_nulls` as const;
  const hasAnyNulls = buffers.some((buf) => buf[nullsName] !== undefined);

  if (!hasAnyNulls) return { nullBitmap: undefined, nullCount: 0 };

  const totalRows = buffers.reduce((sum, buf) => sum + buf.writeIndex, 0);
  const bitmapBytes = Math.ceil(totalRows / 8);
  const nullBitmap = new Uint8Array(bitmapBytes);
  nullBitmap.fill(0xff); // Default all valid

  // Buffer chains: all buffers except the last are full (writeIndex == capacity).
  // If capacity is a multiple of 8, each buffer starts at a byte boundary.
  let rowOffset = 0;
  let nullCount = 0;

  for (const buf of buffers) {
    const sourceBitmap = buf[nullsName];
    const rowCount = buf.writeIndex;

    if (sourceBitmap) {
      const byteOffset = rowOffset >>> 3; // rowOffset / 8
      const fullBytes = rowCount >>> 3;
      const remainingBits = rowCount & 7;

      // Bulk copy full bytes
      if (fullBytes > 0) {
        nullBitmap.set(sourceBitmap.subarray(0, fullBytes), byteOffset);
      }
      // Handle remaining bits in last partial byte
      if (remainingBits > 0) {
        const srcLastByte = sourceBitmap[fullBytes];
        const mask = (1 << remainingBits) - 1;
        nullBitmap[byteOffset + fullBytes] = (nullBitmap[byteOffset + fullBytes] & ~mask) | (srcLastByte & mask);
      }
      // Count nulls (bits that are 0)
      for (let i = 0; i < rowCount; i++) {
        const byte = i >>> 3;
        const bit = i & 7;
        if ((sourceBitmap[byte] & (1 << bit)) === 0) nullCount++;
      }
    }
    // If no sourceBitmap, leave as 0xff (all valid)

    rowOffset += rowCount;
  }

  return { nullBitmap, nullCount };
}

function getArrowFieldName(fieldName: string): string {
  if (fieldName === 'logMessage') return 'message';
  return fieldName;
}

function buildSortedCategoryDictionary(
  buffers: SpanBuffer[],
  columnName: `attr_${string}`,
): { dictionary: string[]; indices: Uint32Array; nullBitmap: Uint8Array | undefined; nullCount: number } {
  const valuesName = `${columnName}_values` as const;
  const totalRows = buffers.reduce((sum, buf) => sum + buf.writeIndex, 0);

  const uniqueStrings = new Set<string>();
  for (const buf of buffers) {
    const column = buf[valuesName];
    if (column && Array.isArray(column)) {
      for (let i = 0; i < buf.writeIndex; i++) {
        const value = column[i];
        if (value != null) uniqueStrings.add(value);
      }
    }
  }

  const dictionary = Array.from(uniqueStrings).sort();
  const stringToIndex = new Map(dictionary.map((s, i) => [s, i]));

  const indices = new Uint32Array(totalRows);
  const { nullBitmap, nullCount } = concatenateNullBitmaps(buffers, columnName);

  let rowOffset = 0;
  for (const buf of buffers) {
    const column = buf[valuesName];
    if (column && Array.isArray(column)) {
      for (let i = 0; i < buf.writeIndex; i++) {
        const value = column[i];
        indices[rowOffset + i] = value != null ? (stringToIndex.get(value) ?? 0) : 0;
      }
    }
    rowOffset += buf.writeIndex;
  }

  return { dictionary, indices, nullBitmap, nullCount };
}

function buildTextDictionary(
  buffers: SpanBuffer[],
  columnName: `attr_${string}`,
): { dictionary: string[]; indices: Uint32Array; nullBitmap: Uint8Array | undefined; nullCount: number } | null {
  const valuesName = `${columnName}_values` as const;
  const totalRows = buffers.reduce((sum, buf) => sum + buf.writeIndex, 0);

  const frequencyMap = new Map<string, number>();
  for (const buf of buffers) {
    const column = buf[valuesName];
    if (column && Array.isArray(column)) {
      for (let i = 0; i < buf.writeIndex; i++) {
        const value = column[i];
        if (value != null) frequencyMap.set(value, (frequencyMap.get(value) ?? 0) + 1);
      }
    }
  }

  if (frequencyMap.size === 0) {
    return { dictionary: [], indices: new Uint32Array(totalRows), nullBitmap: undefined, nullCount: totalRows };
  }

  const dictionary = Array.from(frequencyMap.keys());
  const stringToIndex = new Map(dictionary.map((s, i) => [s, i]));

  const indices = new Uint32Array(totalRows);
  const { nullBitmap, nullCount } = concatenateNullBitmaps(buffers, columnName);

  let rowOffset = 0;
  for (const buf of buffers) {
    const column = buf[valuesName];
    if (column && Array.isArray(column)) {
      for (let i = 0; i < buf.writeIndex; i++) {
        const value = column[i];
        indices[rowOffset + i] = value != null ? (stringToIndex.get(value) ?? 0) : 0;
      }
    }
    rowOffset += buf.writeIndex;
  }

  return { dictionary, indices, nullBitmap, nullCount };
}

export type SystemColumnBuilder = (
  buffer: SpanBuffer,
  buffers: SpanBuffer[],
  totalRows: number,
) => { fields: arrow.Field[]; vectors: arrow.Vector[] };

/**
 * Convert SpanBuffer (and its overflow chain) to Arrow RecordBatch
 */
export function convertToRecordBatch(
  buffer: SpanBuffer,
  moduleIdInterner: StringInterner,
  spanNameInterner: StringInterner,
  systemColumnBuilder?: SystemColumnBuilder,
): arrow.RecordBatch {
  const buffers: SpanBuffer[] = [];
  let currentBuffer: SpanBuffer | undefined = buffer;

  while (currentBuffer) {
    buffers.push(currentBuffer);
    currentBuffer = currentBuffer.next as SpanBuffer | undefined;
  }

  return convertBuffersToRecordBatch(buffers, moduleIdInterner, spanNameInterner, systemColumnBuilder);
}

function convertBuffersToRecordBatch(
  buffers: SpanBuffer[],
  moduleIdInterner: StringInterner,
  spanNameInterner: StringInterner,
  systemColumnBuilder?: SystemColumnBuilder,
): arrow.RecordBatch {
  if (buffers.length === 0) return new arrow.RecordBatch({});

  const totalRows = buffers.reduce((sum, buf) => sum + buf.writeIndex, 0);
  if (totalRows === 0) return new arrow.RecordBatch({});

  const schema = buffers[0].task.module.tagAttributes;
  const fields: arrow.Field[] = [];
  let systemVectors: arrow.Vector[] = [];

  if (systemColumnBuilder) {
    const systemColumns = systemColumnBuilder(buffers[0], buffers, totalRows);
    fields.push(...systemColumns.fields);
    systemVectors = systemColumns.vectors;
  } else {
    fields.push(arrow.Field.new({ name: 'timestamp', type: new arrow.TimestampNanosecond() }));
    fields.push(arrow.Field.new({ name: 'trace_id', type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()) }));
    // Span ID columns (separate columns instead of struct)
    fields.push(arrow.Field.new({ name: 'thread_id', type: new arrow.Uint64() }));
    fields.push(arrow.Field.new({ name: 'span_id', type: new arrow.Uint32() }));
    fields.push(arrow.Field.new({ name: 'parent_thread_id', type: new arrow.Uint64(), nullable: true }));
    fields.push(arrow.Field.new({ name: 'parent_span_id', type: new arrow.Uint32(), nullable: true }));
    fields.push(
      arrow.Field.new({ name: 'entry_type', type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int8()) }),
    );
    fields.push(arrow.Field.new({ name: 'module', type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()) }));
    fields.push(
      arrow.Field.new({ name: 'span_name', type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()) }),
    );
  }

  for (const [fieldName, fieldSchema] of Object.entries(schema)) {
    const lmaoType = getLmaoSchemaType(fieldSchema);
    const arrowFieldName = getArrowFieldName(fieldName);

    if (lmaoType === 'enum') {
      fields.push(
        arrow.Field.new({
          name: arrowFieldName,
          type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint8()),
          nullable: true,
        }),
      );
    } else if (lmaoType === 'category') {
      fields.push(
        arrow.Field.new({
          name: arrowFieldName,
          type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint32()),
          nullable: true,
        }),
      );
    } else if (lmaoType === 'text') {
      fields.push(
        arrow.Field.new({
          name: arrowFieldName,
          type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint32()),
          nullable: true,
        }),
      );
    } else if (lmaoType === 'number') {
      fields.push(arrow.Field.new({ name: arrowFieldName, type: new arrow.Float64(), nullable: true }));
    } else if (lmaoType === 'boolean') {
      fields.push(arrow.Field.new({ name: arrowFieldName, type: new arrow.Bool(), nullable: true }));
    }
  }

  const arrowSchema = new arrow.Schema(fields);
  const vectors: arrow.Vector[] = [];

  if (systemColumnBuilder) {
    vectors.push(...systemVectors);
  } else {
    buildDefaultSystemVectors(buffers, vectors, moduleIdInterner, spanNameInterner);
  }

  for (const [fieldName, fieldSchema] of Object.entries(schema)) {
    const lmaoType = getLmaoSchemaType(fieldSchema);
    const columnName = `attr_${fieldName}` as `attr_${string}`;

    if (lmaoType === 'enum') {
      const enumValues = getEnumValues(fieldSchema) || [];
      const enumUtf8 = getEnumUtf8(fieldSchema);
      const valuesName = `${columnName}_values` as const;
      const valueArrays: Uint8Array[] = [];

      for (const buf of buffers) {
        const column = buf[valuesName];
        if (column && column instanceof Uint8Array) {
          valueArrays.push(column.subarray(0, buf.writeIndex));
        } else {
          valueArrays.push(new Uint8Array(buf.writeIndex));
        }
      }

      const allIndices = concatenateTypedArrays(valueArrays);
      const { nullBitmap, nullCount } = concatenateNullBitmaps(buffers, columnName);

      const enumDictData = arrow.makeData({
        type: new arrow.Utf8(),
        offset: 0,
        length: enumValues.length,
        nullCount: 0,
        valueOffsets: enumUtf8?.offsets ?? calculateUtf8Offsets(enumValues as readonly string[]),
        data: enumUtf8?.concatenated ?? encodeUtf8Strings(enumValues as readonly string[]),
      });

      const enumData = arrow.makeData({
        type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint8()),
        offset: 0,
        length: totalRows,
        nullCount,
        data: allIndices,
        nullBitmap,
        dictionary: arrow.makeVector(enumDictData),
      });

      vectors.push(arrow.makeVector(enumData));
    } else if (lmaoType === 'category') {
      const { dictionary, indices, nullBitmap, nullCount } = buildSortedCategoryDictionary(buffers, columnName);
      const { data: categoryUtf8Data, offsets: categoryUtf8Offsets } = globalUtf8Cache.encodeMany(dictionary);

      const categoryDictData = arrow.makeData({
        type: new arrow.Utf8(),
        offset: 0,
        length: dictionary.length,
        nullCount: 0,
        valueOffsets: categoryUtf8Offsets,
        data: categoryUtf8Data,
      });

      const categoryData = arrow.makeData({
        type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint32()),
        offset: 0,
        length: totalRows,
        nullCount,
        data: indices,
        nullBitmap,
        dictionary: arrow.makeVector(categoryDictData),
      });

      vectors.push(arrow.makeVector(categoryData));
    } else if (lmaoType === 'text') {
      const result = buildTextDictionary(buffers, columnName);
      if (result) {
        const { dictionary, indices, nullBitmap, nullCount } = result;
        const { data: textUtf8Data, offsets: textUtf8Offsets } = globalUtf8Cache.encodeMany(dictionary);

        const textDictData = arrow.makeData({
          type: new arrow.Utf8(),
          offset: 0,
          length: dictionary.length,
          nullCount: 0,
          valueOffsets: textUtf8Offsets,
          data: textUtf8Data,
        });

        const textData = arrow.makeData({
          type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint32()),
          offset: 0,
          length: totalRows,
          nullCount,
          data: indices,
          nullBitmap,
          dictionary: arrow.makeVector(textDictData),
        });

        vectors.push(arrow.makeVector(textData));
      }
    } else if (lmaoType === 'number') {
      const valuesName = `${columnName}_values` as const;
      const valueArrays: Float64Array[] = [];

      for (const buf of buffers) {
        const column = buf[valuesName];
        if (column && column instanceof Float64Array) {
          valueArrays.push(column.subarray(0, buf.writeIndex));
        } else {
          valueArrays.push(new Float64Array(buf.writeIndex));
        }
      }

      const allValues = concatenateTypedArrays(valueArrays);
      const { nullBitmap, nullCount } = concatenateNullBitmaps(buffers, columnName);

      const numberData = arrow.makeData({
        type: new arrow.Float64(),
        offset: 0,
        length: totalRows,
        nullCount,
        data: allValues,
        nullBitmap,
      });

      vectors.push(arrow.makeVector(numberData));
    } else if (lmaoType === 'boolean') {
      const valuesName = `${columnName}_values` as const;
      const valueArrays: Uint8Array[] = [];

      for (const buf of buffers) {
        const column = buf[valuesName];
        if (column && column instanceof Uint8Array) {
          const requiredBytes = Math.ceil(buf.writeIndex / 8);
          valueArrays.push(column.subarray(0, requiredBytes));
        } else {
          const requiredBytes = Math.ceil(buf.writeIndex / 8);
          valueArrays.push(new Uint8Array(requiredBytes));
        }
      }

      const allValues = concatenateTypedArrays(valueArrays);
      const { nullBitmap, nullCount } = concatenateNullBitmaps(buffers, columnName);

      const boolData = arrow.makeData({
        type: new arrow.Bool(),
        offset: 0,
        length: totalRows,
        nullCount,
        data: allValues,
        nullBitmap,
      });

      vectors.push(arrow.makeVector(boolData));
    }
  }

  const data = arrow.makeData({
    type: new arrow.Struct(arrowSchema.fields),
    length: totalRows,
    nullCount: 0,
    children: vectors.map((v) => v.data[0]),
  });

  return new arrow.RecordBatch(arrowSchema, data);
}

/**
 * Convert SpanBuffer (and its overflow chain) to Arrow Table
 */
export function convertToArrowTable(
  buffer: SpanBuffer,
  moduleIdInterner: StringInterner,
  spanNameInterner: StringInterner,
  systemColumnBuilder?: SystemColumnBuilder,
): arrow.Table {
  const batch = convertToRecordBatch(buffer, moduleIdInterner, spanNameInterner, systemColumnBuilder);
  if (batch.numRows === 0) return new arrow.Table();
  return new arrow.Table([batch]);
}

function buildDefaultSystemVectors(
  buffers: SpanBuffer[],
  vectors: arrow.Vector[],
  moduleIdInterner: StringInterner,
  spanNameInterner: StringInterner,
): void {
  const totalRows = buffers.reduce((sum, buf) => sum + buf.writeIndex, 0);

  // Timestamp - BigInt64Array with nanoseconds
  const allTimestamps = new BigInt64Array(totalRows);
  let timestampOffset = 0;
  for (const buf of buffers) {
    // Use set() with subarray - bulk copy
    allTimestamps.set(buf.timestamps.subarray(0, buf.writeIndex), timestampOffset);
    timestampOffset += buf.writeIndex;
  }
  vectors.push(
    arrow.makeVector(
      arrow.makeData({
        type: new arrow.TimestampNanosecond(),
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: allTimestamps,
      }),
    ),
  );

  // Trace ID
  const traceIdSet = new Set<string>();
  for (const buf of buffers) traceIdSet.add(buf.traceId);
  const traceIdArray = Array.from(traceIdSet);
  const traceIdMap = new Map(traceIdArray.map((id, idx) => [id, idx]));

  const traceIdIndices = new Int32Array(totalRows);
  let rowOffset = 0;
  for (const buf of buffers) {
    const traceIdIndex = traceIdMap.get(buf.traceId)!;
    // Use fill() - constant value per buffer
    traceIdIndices.fill(traceIdIndex, rowOffset, rowOffset + buf.writeIndex);
    rowOffset += buf.writeIndex;
  }

  const traceIdDictData = arrow.makeData({
    type: new arrow.Utf8(),
    offset: 0,
    length: traceIdArray.length,
    nullCount: 0,
    valueOffsets: calculateUtf8Offsets(traceIdArray),
    data: encodeUtf8Strings(traceIdArray),
  });

  vectors.push(
    arrow.makeVector(
      arrow.makeData({
        type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()),
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: traceIdIndices,
        dictionary: arrow.makeVector(traceIdDictData),
      }),
    ),
  );

  // thread_id (Uint64) - separate column
  // threadId is constant per buffer, use fill()
  const threadIds = new BigUint64Array(totalRows);
  rowOffset = 0;
  for (const buf of buffers) {
    threadIds.fill(buf.threadId, rowOffset, rowOffset + buf.writeIndex);
    rowOffset += buf.writeIndex;
  }
  vectors.push(
    arrow.makeVector(
      arrow.makeData({
        type: new arrow.Uint64(),
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: threadIds,
      }),
    ),
  );

  // span_id (Uint32) - separate column
  const spanIds = new Uint32Array(totalRows);
  rowOffset = 0;
  for (const buf of buffers) {
    // Use fill() - constant value per buffer
    spanIds.fill(buf.spanId, rowOffset, rowOffset + buf.writeIndex);
    rowOffset += buf.writeIndex;
  }
  vectors.push(
    arrow.makeVector(
      arrow.makeData({
        type: new arrow.Uint32(),
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: spanIds,
      }),
    ),
  );

  // parent_thread_id (Uint64, nullable) - separate column
  // parentThreadId is constant per buffer (from parent pointer), use fill()
  const parentThreadIds = new BigUint64Array(totalRows);
  const parentThreadIdNulls = new Uint8Array(Math.ceil(totalRows / 8));
  parentThreadIdNulls.fill(0xff);
  let parentThreadIdNullCount = 0;
  rowOffset = 0;
  for (const buf of buffers) {
    if (buf.parent) {
      parentThreadIds.fill(buf.parent.threadId, rowOffset, rowOffset + buf.writeIndex);
    } else {
      clearBitRange(parentThreadIdNulls, rowOffset, buf.writeIndex);
      parentThreadIdNullCount += buf.writeIndex;
    }
    rowOffset += buf.writeIndex;
  }
  vectors.push(
    arrow.makeVector(
      arrow.makeData({
        type: new arrow.Uint64(),
        offset: 0,
        length: totalRows,
        nullCount: parentThreadIdNullCount,
        data: parentThreadIds,
        nullBitmap: parentThreadIdNullCount > 0 ? parentThreadIdNulls : undefined,
      }),
    ),
  );

  // parent_span_id (Uint32, nullable) - separate column
  const parentSpanIds = new Uint32Array(totalRows);
  const parentSpanIdNulls = new Uint8Array(Math.ceil(totalRows / 8));
  parentSpanIdNulls.fill(0xff);
  let parentSpanIdNullCount = 0;
  rowOffset = 0;
  for (const buf of buffers) {
    if (buf.hasParent) {
      // Use fill() - constant value per buffer
      parentSpanIds.fill(buf.parentSpanId, rowOffset, rowOffset + buf.writeIndex);
    } else {
      clearBitRange(parentSpanIdNulls, rowOffset, buf.writeIndex);
      parentSpanIdNullCount += buf.writeIndex;
    }
    rowOffset += buf.writeIndex;
  }
  vectors.push(
    arrow.makeVector(
      arrow.makeData({
        type: new arrow.Uint32(),
        offset: 0,
        length: totalRows,
        nullCount: parentSpanIdNullCount,
        data: parentSpanIds,
        nullBitmap: parentSpanIdNullCount > 0 ? parentSpanIdNulls : undefined,
      }),
    ),
  );

  // Entry type
  const entryTypeIndices = new Int8Array(totalRows);
  rowOffset = 0;
  for (const buf of buffers) {
    // Use set() with subarray - bulk copy
    // Note: operations is Uint8Array but entryTypeIndices is Int8Array, same underlying representation
    entryTypeIndices.set(buf.operations.subarray(0, buf.writeIndex), rowOffset);
    rowOffset += buf.writeIndex;
  }
  const entryTypeDictData = arrow.makeData({
    type: new arrow.Utf8(),
    offset: 0,
    length: ENTRY_TYPE_NAMES.length,
    nullCount: 0,
    valueOffsets: calculateUtf8Offsets(ENTRY_TYPE_NAMES),
    data: encodeUtf8Strings(ENTRY_TYPE_NAMES),
  });
  vectors.push(
    arrow.makeVector(
      arrow.makeData({
        type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int8()),
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: entryTypeIndices,
        dictionary: arrow.makeVector(entryTypeDictData),
      }),
    ),
  );

  // Module
  const moduleIdSet = new Set<number>();
  for (const buf of buffers) moduleIdSet.add(buf.task.module.moduleId);
  const moduleIdArray = Array.from(moduleIdSet);
  const moduleNameArray = moduleIdArray.map((id) => moduleIdInterner.getString(id) || 'unknown');
  const moduleIdMap = new Map(moduleIdArray.map((id, idx) => [id, idx]));

  const moduleIndices = new Int32Array(totalRows);
  rowOffset = 0;
  for (const buf of buffers) {
    const moduleIndex = moduleIdMap.get(buf.task.module.moduleId)!;
    // Use fill() - constant value per buffer
    moduleIndices.fill(moduleIndex, rowOffset, rowOffset + buf.writeIndex);
    rowOffset += buf.writeIndex;
  }

  const moduleDictData = arrow.makeData({
    type: new arrow.Utf8(),
    offset: 0,
    length: moduleNameArray.length,
    nullCount: 0,
    valueOffsets: calculateUtf8Offsets(moduleNameArray),
    data: encodeUtf8Strings(moduleNameArray),
  });
  vectors.push(
    arrow.makeVector(
      arrow.makeData({
        type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()),
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: moduleIndices,
        dictionary: arrow.makeVector(moduleDictData),
      }),
    ),
  );

  // Span name
  const spanNameIdSet = new Set<number>();
  for (const buf of buffers) spanNameIdSet.add(buf.task.spanNameId);
  const spanNameIdArray = Array.from(spanNameIdSet);
  const spanNameArray = spanNameIdArray.map((id) => spanNameInterner.getString(id) || 'unknown');
  const spanNameIdMap = new Map(spanNameIdArray.map((id, idx) => [id, idx]));

  const spanNameIndices = new Int32Array(totalRows);
  rowOffset = 0;
  for (const buf of buffers) {
    const spanNameIndex = spanNameIdMap.get(buf.task.spanNameId)!;
    // Use fill() - constant value per buffer
    spanNameIndices.fill(spanNameIndex, rowOffset, rowOffset + buf.writeIndex);
    rowOffset += buf.writeIndex;
  }

  const spanNameDictData = arrow.makeData({
    type: new arrow.Utf8(),
    offset: 0,
    length: spanNameArray.length,
    nullCount: 0,
    valueOffsets: calculateUtf8Offsets(spanNameArray),
    data: encodeUtf8Strings(spanNameArray),
  });
  vectors.push(
    arrow.makeVector(
      arrow.makeData({
        type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()),
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: spanNameIndices,
        dictionary: arrow.makeVector(spanNameDictData),
      }),
    ),
  );
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tree Conversion with Shared Dictionaries
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Calculate UTF-8 byte length without allocating
 */
function utf8ByteLength(str: string): number {
  let bytes = 0;
  for (let i = 0; i < str.length; i++) {
    const c = str.charCodeAt(i);
    if (c < 0x80) bytes += 1;
    else if (c < 0x800) bytes += 2;
    else if (c < 0xd800 || c >= 0xe000) bytes += 3;
    else {
      i++;
      bytes += 4;
    } // surrogate pair
  }
  return bytes;
}

/**
 * Dictionary builder for string columns.
 *
 * Pass 1: Collect unique strings, track total UTF-8 byte length,
 *         cache UTF-8 encoding on second occurrence.
 * Finalize: Sort (for category), pre-allocate exact-size buffer,
 *           encode remaining strings with encodeInto (zero allocation).
 */
class ColumnDictionary {
  private strings = new Map<string, { utf8?: Uint8Array; count: number }>();
  private totalUtf8Bytes = 0;
  private encoder = new TextEncoder();

  add(str: string): void {
    const entry = this.strings.get(str);
    if (entry) {
      entry.count++;
      if (entry.count === 2 && !entry.utf8) {
        // Cache UTF-8 on second occurrence
        entry.utf8 = this.encoder.encode(str);
      }
    } else {
      // First occurrence - just track byte length (no allocation)
      this.totalUtf8Bytes += utf8ByteLength(str);
      this.strings.set(str, { count: 1 });
    }
  }

  get size(): number {
    return this.strings.size;
  }

  finalize(sort: boolean): { data: Uint8Array; offsets: Int32Array; indexMap: Map<string, number> } {
    const uniqueCount = this.strings.size;

    // Pre-allocate array with exact size
    const keys = new Array<string>(uniqueCount);
    let i = 0;
    for (const key of this.strings.keys()) {
      keys[i++] = key;
    }
    if (sort) keys.sort();

    // Pre-allocate exact-size buffer
    const data = new Uint8Array(this.totalUtf8Bytes);
    const offsets = new Int32Array(uniqueCount + 1);
    const indexMap = new Map<string, number>();

    let offset = 0;
    for (let i = 0; i < uniqueCount; i++) {
      const str = keys[i];
      const entry = this.strings.get(str)!;

      offsets[i] = offset;
      indexMap.set(str, i);

      if (entry.utf8) {
        // Cached - just copy
        data.set(entry.utf8, offset);
        offset += entry.utf8.length;
      } else {
        // Encode directly into final buffer (zero intermediate allocation)
        const result = this.encoder.encodeInto(str, data.subarray(offset));
        offset += result.written!;
      }
    }
    offsets[uniqueCount] = offset;

    return { data, offsets, indexMap };
  }
}

/**
 * Walk a SpanBuffer tree (depth-first pre-order), including overflow chains.
 */
function walkSpanTree(root: SpanBuffer, visitor: (buffer: SpanBuffer) => void): void {
  let current: SpanBuffer | undefined = root;
  while (current) {
    visitor(current);
    current = current.next as SpanBuffer | undefined;
  }
  for (const child of root.children) {
    walkSpanTree(child, visitor);
  }
}

/**
 * Convert SpanBuffer tree to Arrow Table
 *
 * Two-pass conversion with shared dictionaries:
 * - Pass 1: Walk tree, build dictionaries (collect unique strings, cache UTF-8)
 * - Pass 2: Walk tree, convert each buffer to RecordBatch using shared dictionaries
 */
export function convertSpanTreeToArrowTable(
  rootBuffer: SpanBuffer,
  moduleIdInterner: StringInterner,
  spanNameInterner: StringInterner,
  _systemColumnBuilder?: SystemColumnBuilder,
): arrow.Table {
  const schema = rootBuffer.task.module.tagAttributes;

  // ═══════════════════════════════════════════════════════════════════════════
  // PASS 1: Build dictionaries
  // ═══════════════════════════════════════════════════════════════════════════
  const categoryBuilders = new Map<string, ColumnDictionary>();
  const textBuilders = new Map<string, ColumnDictionary>();

  for (const [fieldName, fieldSchema] of Object.entries(schema)) {
    const lmaoType = getLmaoSchemaType(fieldSchema);
    if (lmaoType === 'category') categoryBuilders.set(fieldName, new ColumnDictionary());
    if (lmaoType === 'text') textBuilders.set(fieldName, new ColumnDictionary());
  }

  // System column dictionaries
  const traceIdBuilder = new ColumnDictionary();
  const moduleBuilder = new ColumnDictionary();
  const spanNameBuilder = new ColumnDictionary();

  let totalRows = 0;
  walkSpanTree(rootBuffer, (buffer) => {
    totalRows += buffer.writeIndex;
    traceIdBuilder.add(buffer.traceId);

    // Add module and span name to their shared dictionaries
    const moduleName = moduleIdInterner.getString(buffer.task.module.moduleId) || 'unknown';
    const spanName = spanNameInterner.getString(buffer.task.spanNameId) || 'unknown';
    moduleBuilder.add(moduleName);
    spanNameBuilder.add(spanName);

    for (const [fieldName, builder] of categoryBuilders) {
      const col = buffer[`attr_${fieldName}_values` as keyof SpanBuffer] as string[] | undefined;
      if (col) {
        for (let i = 0; i < buffer.writeIndex; i++) {
          if (col[i] != null) builder.add(col[i]);
        }
      }
    }

    for (const [fieldName, builder] of textBuilders) {
      const col = buffer[`attr_${fieldName}_values` as keyof SpanBuffer] as string[] | undefined;
      if (col) {
        for (let i = 0; i < buffer.writeIndex; i++) {
          if (col[i] != null) builder.add(col[i]);
        }
      }
    }
  });

  if (totalRows === 0) return new arrow.Table();

  // Finalize dictionaries
  const traceIdDict = traceIdBuilder.finalize(false);
  const moduleDict = moduleBuilder.finalize(true); // sorted
  const spanNameDict = spanNameBuilder.finalize(true); // sorted
  const categoryDicts = new Map<string, FinalizedDict>();
  const textDicts = new Map<string, FinalizedDict>();

  for (const [name, builder] of categoryBuilders) {
    categoryDicts.set(name, builder.finalize(true)); // sorted for binary search
  }
  for (const [name, builder] of textBuilders) {
    textDicts.set(name, builder.finalize(false)); // not sorted
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Build shared Arrow schema with stable dictionary IDs
  // ═══════════════════════════════════════════════════════════════════════════
  const arrowFields: arrow.Field[] = [];

  // System columns - create types with explicit dictionary IDs
  arrowFields.push(arrow.Field.new({ name: 'timestamp', type: new arrow.TimestampNanosecond() }));
  arrowFields.push(
    arrow.Field.new({ name: 'trace_id', type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32(), 0) }),
  );
  // Span ID columns (separate columns instead of struct)
  arrowFields.push(arrow.Field.new({ name: 'thread_id', type: new arrow.Uint64() }));
  arrowFields.push(arrow.Field.new({ name: 'span_id', type: new arrow.Uint32() }));
  arrowFields.push(arrow.Field.new({ name: 'parent_thread_id', type: new arrow.Uint64(), nullable: true }));
  arrowFields.push(arrow.Field.new({ name: 'parent_span_id', type: new arrow.Uint32(), nullable: true }));
  arrowFields.push(
    arrow.Field.new({ name: 'entry_type', type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int8(), 1) }),
  );
  arrowFields.push(
    arrow.Field.new({ name: 'module', type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32(), 2) }),
  );
  arrowFields.push(
    arrow.Field.new({ name: 'span_name', type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32(), 3) }),
  );

  // Attribute columns - assign dictionary IDs starting at 4
  let nextDictId = 4;
  const attrDictIds = new Map<string, number>();
  for (const [fieldName, fieldSchema] of Object.entries(schema)) {
    const lmaoType = getLmaoSchemaType(fieldSchema);
    const arrowFieldName = getArrowFieldName(fieldName);

    if (lmaoType === 'enum') {
      attrDictIds.set(fieldName, nextDictId);
      arrowFields.push(
        arrow.Field.new({
          name: arrowFieldName,
          type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint8(), nextDictId++),
          nullable: true,
        }),
      );
    } else if (lmaoType === 'category') {
      attrDictIds.set(fieldName, nextDictId);
      arrowFields.push(
        arrow.Field.new({
          name: arrowFieldName,
          type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint32(), nextDictId++),
          nullable: true,
        }),
      );
    } else if (lmaoType === 'text') {
      attrDictIds.set(fieldName, nextDictId);
      arrowFields.push(
        arrow.Field.new({
          name: arrowFieldName,
          type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint32(), nextDictId++),
          nullable: true,
        }),
      );
    } else if (lmaoType === 'number') {
      arrowFields.push(arrow.Field.new({ name: arrowFieldName, type: new arrow.Float64(), nullable: true }));
    } else if (lmaoType === 'boolean') {
      arrowFields.push(arrow.Field.new({ name: arrowFieldName, type: new arrow.Bool(), nullable: true }));
    }
  }

  const arrowSchema = new arrow.Schema(arrowFields);

  // ═══════════════════════════════════════════════════════════════════════════
  // PASS 2: Collect all buffers with non-zero rows
  // ═══════════════════════════════════════════════════════════════════════════
  const allBuffers: SpanBuffer[] = [];

  walkSpanTree(rootBuffer, (buffer) => {
    if (buffer.writeIndex > 0) {
      allBuffers.push(buffer);
    }
  });

  if (allBuffers.length === 0) return new arrow.Table();

  // Build a single RecordBatch from all buffers
  const batch = convertBuffersWithSharedDicts(
    allBuffers,
    arrowSchema,
    traceIdDict,
    moduleDict,
    spanNameDict,
    categoryDicts,
    textDicts,
    attrDictIds,
    moduleIdInterner,
    spanNameInterner,
    schema,
  );

  return new arrow.Table([batch]);
}

type FinalizedDict = ReturnType<ColumnDictionary['finalize']>;

/**
 * Convert multiple buffers to a single RecordBatch using pre-built shared dictionaries
 */
function convertBuffersWithSharedDicts(
  buffers: SpanBuffer[],
  arrowSchema: arrow.Schema,
  traceIdDict: FinalizedDict,
  moduleDict: FinalizedDict,
  spanNameDict: FinalizedDict,
  categoryDicts: Map<string, FinalizedDict>,
  textDicts: Map<string, FinalizedDict>,
  _attrDictIds: Map<string, number>,
  moduleIdInterner: StringInterner,
  spanNameInterner: StringInterner,
  lmaoSchema: Record<string, unknown>,
): arrow.RecordBatch {
  const totalRows = buffers.reduce((sum, buf) => sum + buf.writeIndex, 0);
  const vectors: arrow.Vector[] = [];

  // Get types from the shared schema
  const timestampType = arrowSchema.fields[0].type as arrow.TimestampNanosecond;
  const traceIdType = arrowSchema.fields[1].type as arrow.Dictionary<arrow.Utf8, arrow.Int32>;
  // Span ID columns: thread_id (2), span_id (3), parent_thread_id (4), parent_span_id (5)
  const entryTypeType = arrowSchema.fields[6].type as arrow.Dictionary<arrow.Utf8, arrow.Int8>;
  const moduleType = arrowSchema.fields[7].type as arrow.Dictionary<arrow.Utf8, arrow.Int32>;
  const spanNameType = arrowSchema.fields[8].type as arrow.Dictionary<arrow.Utf8, arrow.Int32>;

  // ═══════════════════════════════════════════════════════════════════════════
  // System columns - concatenate data from all buffers
  // ═══════════════════════════════════════════════════════════════════════════

  // Timestamp - BigInt64Array with nanoseconds (zero-copy compatible)
  const allTimestamps = new BigInt64Array(totalRows);
  let offset = 0;
  for (const buf of buffers) {
    // Use set() with subarray - bulk copy
    allTimestamps.set(buf.timestamps.subarray(0, buf.writeIndex), offset);
    offset += buf.writeIndex;
  }
  vectors.push(
    arrow.makeVector(
      arrow.makeData({
        type: timestampType,
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: allTimestamps,
      }),
    ),
  );

  // Trace ID (using shared dictionary)
  const traceIdIndices = new Int32Array(totalRows);
  offset = 0;
  for (const buf of buffers) {
    const idx = traceIdDict.indexMap.get(buf.traceId) ?? 0;
    // Use fill() - constant value per buffer
    traceIdIndices.fill(idx, offset, offset + buf.writeIndex);
    offset += buf.writeIndex;
  }
  vectors.push(
    arrow.makeVector(
      arrow.makeData({
        type: traceIdType,
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: traceIdIndices,
        dictionary: arrow.makeVector(
          arrow.makeData({
            type: new arrow.Utf8(),
            offset: 0,
            length: traceIdDict.indexMap.size,
            nullCount: 0,
            valueOffsets: traceIdDict.offsets,
            data: traceIdDict.data,
          }),
        ),
      }),
    ),
  );

  // thread_id (Uint64) - separate column
  // threadId is constant per buffer, use fill()
  const threadIds = new BigUint64Array(totalRows);
  offset = 0;
  for (const buf of buffers) {
    threadIds.fill(buf.threadId, offset, offset + buf.writeIndex);
    offset += buf.writeIndex;
  }
  vectors.push(
    arrow.makeVector(
      arrow.makeData({
        type: new arrow.Uint64(),
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: threadIds,
      }),
    ),
  );

  // span_id (Uint32) - separate column
  const spanIds = new Uint32Array(totalRows);
  offset = 0;
  for (const buf of buffers) {
    // Use fill() - constant value per buffer
    spanIds.fill(buf.spanId, offset, offset + buf.writeIndex);
    offset += buf.writeIndex;
  }
  vectors.push(
    arrow.makeVector(
      arrow.makeData({
        type: new arrow.Uint32(),
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: spanIds,
      }),
    ),
  );

  // parent_thread_id (Uint64, nullable) - separate column
  // parentThreadId is constant per buffer (from parent pointer), use fill()
  const parentThreadIds = new BigUint64Array(totalRows);
  const parentThreadIdNulls = new Uint8Array(Math.ceil(totalRows / 8));
  parentThreadIdNulls.fill(0xff);
  let parentThreadIdNullCount = 0;
  offset = 0;
  for (const buf of buffers) {
    if (buf.parent) {
      parentThreadIds.fill(buf.parent.threadId, offset, offset + buf.writeIndex);
    } else {
      clearBitRange(parentThreadIdNulls, offset, buf.writeIndex);
      parentThreadIdNullCount += buf.writeIndex;
    }
    offset += buf.writeIndex;
  }
  vectors.push(
    arrow.makeVector(
      arrow.makeData({
        type: new arrow.Uint64(),
        offset: 0,
        length: totalRows,
        nullCount: parentThreadIdNullCount,
        data: parentThreadIds,
        nullBitmap: parentThreadIdNullCount > 0 ? parentThreadIdNulls : undefined,
      }),
    ),
  );

  // parent_span_id (Uint32, nullable) - separate column
  // Uses hasParent and parentSpanId directly on buffer
  const parentSpanIds = new Uint32Array(totalRows);
  const parentSpanIdNulls = new Uint8Array(Math.ceil(totalRows / 8));
  parentSpanIdNulls.fill(0xff);
  let parentSpanIdNullCount = 0;
  offset = 0;
  for (const buf of buffers) {
    if (buf.hasParent) {
      // Use fill() - constant value per buffer
      parentSpanIds.fill(buf.parentSpanId, offset, offset + buf.writeIndex);
    } else {
      clearBitRange(parentSpanIdNulls, offset, buf.writeIndex);
      parentSpanIdNullCount += buf.writeIndex;
    }
    offset += buf.writeIndex;
  }
  vectors.push(
    arrow.makeVector(
      arrow.makeData({
        type: new arrow.Uint32(),
        offset: 0,
        length: totalRows,
        nullCount: parentSpanIdNullCount,
        data: parentSpanIds,
        nullBitmap: parentSpanIdNullCount > 0 ? parentSpanIdNulls : undefined,
      }),
    ),
  );

  // Entry type
  const entryTypeIndices = new Int8Array(totalRows);
  offset = 0;
  for (const buf of buffers) {
    // Use set() with subarray - bulk copy
    // Note: operations is Uint8Array but entryTypeIndices is Int8Array, same underlying representation
    entryTypeIndices.set(buf.operations.subarray(0, buf.writeIndex), offset);
    offset += buf.writeIndex;
  }
  const entryTypeDictData = arrow.makeData({
    type: new arrow.Utf8(),
    offset: 0,
    length: ENTRY_TYPE_NAMES.length,
    nullCount: 0,
    valueOffsets: calculateUtf8Offsets(ENTRY_TYPE_NAMES),
    data: encodeUtf8Strings(ENTRY_TYPE_NAMES),
  });
  vectors.push(
    arrow.makeVector(
      arrow.makeData({
        type: entryTypeType,
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: entryTypeIndices,
        dictionary: arrow.makeVector(entryTypeDictData),
      }),
    ),
  );

  // Module (using shared dictionary)
  const moduleIndices = new Int32Array(totalRows);
  offset = 0;
  for (const buf of buffers) {
    const moduleName = moduleIdInterner.getString(buf.task.module.moduleId) || 'unknown';
    const idx = moduleDict.indexMap.get(moduleName) ?? 0;
    // Use fill() - constant value per buffer
    moduleIndices.fill(idx, offset, offset + buf.writeIndex);
    offset += buf.writeIndex;
  }
  vectors.push(
    arrow.makeVector(
      arrow.makeData({
        type: moduleType,
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: moduleIndices,
        dictionary: arrow.makeVector(
          arrow.makeData({
            type: new arrow.Utf8(),
            offset: 0,
            length: moduleDict.indexMap.size,
            nullCount: 0,
            valueOffsets: moduleDict.offsets,
            data: moduleDict.data,
          }),
        ),
      }),
    ),
  );

  // Span name (using shared dictionary)
  const spanNameIndices = new Int32Array(totalRows);
  offset = 0;
  for (const buf of buffers) {
    const spanName = spanNameInterner.getString(buf.task.spanNameId) || 'unknown';
    const idx = spanNameDict.indexMap.get(spanName) ?? 0;
    // Use fill() - constant value per buffer
    spanNameIndices.fill(idx, offset, offset + buf.writeIndex);
    offset += buf.writeIndex;
  }
  vectors.push(
    arrow.makeVector(
      arrow.makeData({
        type: spanNameType,
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: spanNameIndices,
        dictionary: arrow.makeVector(
          arrow.makeData({
            type: new arrow.Utf8(),
            offset: 0,
            length: spanNameDict.indexMap.size,
            nullCount: 0,
            valueOffsets: spanNameDict.offsets,
            data: spanNameDict.data,
          }),
        ),
      }),
    ),
  );

  // ═══════════════════════════════════════════════════════════════════════════
  // Attribute columns - concatenate from all buffers
  // ═══════════════════════════════════════════════════════════════════════════
  // System fields: timestamp, trace_id, thread_id, span_id, parent_thread_id, parent_span_id, entry_type, module, span_name
  const SYSTEM_FIELDS_COUNT = 9;
  let fieldIdx = SYSTEM_FIELDS_COUNT;

  for (const [fieldName, fieldSchema] of Object.entries(lmaoSchema)) {
    const lmaoType = getLmaoSchemaType(fieldSchema);
    const columnName = `attr_${fieldName}` as `attr_${string}`;
    const valuesKey = `${columnName}_values` as keyof SpanBuffer;
    const nullsKey = `${columnName}_nulls` as keyof SpanBuffer;

    // Get the type from the shared schema
    const fieldType = arrowSchema.fields[fieldIdx].type;

    if (lmaoType === 'category') {
      const dict = categoryDicts.get(fieldName)!;
      const indices = new Uint32Array(totalRows);
      const nullBitmap = new Uint8Array(Math.ceil(totalRows / 8));
      let nullCount = 0;
      let rowOffset = 0;

      for (const buf of buffers) {
        const col = buf[valuesKey] as string[] | undefined;
        if (col) {
          for (let i = 0; i < buf.writeIndex; i++) {
            const v = col[i];
            const rowIdx = rowOffset + i;
            if (v != null) {
              indices[rowIdx] = dict.indexMap.get(v) ?? 0;
              nullBitmap[rowIdx >>> 3] |= 1 << (rowIdx & 7);
            } else {
              nullCount++;
            }
          }
        } else {
          nullCount += buf.writeIndex;
        }
        rowOffset += buf.writeIndex;
      }

      vectors.push(
        arrow.makeVector(
          arrow.makeData({
            type: fieldType as arrow.Dictionary<arrow.Utf8, arrow.Uint32>,
            offset: 0,
            length: totalRows,
            nullCount,
            data: indices,
            nullBitmap: nullCount > 0 ? nullBitmap : undefined,
            dictionary: arrow.makeVector(
              arrow.makeData({
                type: new arrow.Utf8(),
                offset: 0,
                length: dict.indexMap.size,
                nullCount: 0,
                valueOffsets: dict.offsets,
                data: dict.data,
              }),
            ),
          }),
        ),
      );
    } else if (lmaoType === 'text') {
      const dict = textDicts.get(fieldName)!;
      const indices = new Uint32Array(totalRows);
      const nullBitmap = new Uint8Array(Math.ceil(totalRows / 8));
      let nullCount = 0;
      let rowOffset = 0;

      for (const buf of buffers) {
        const col = buf[valuesKey] as string[] | undefined;
        if (col) {
          for (let i = 0; i < buf.writeIndex; i++) {
            const v = col[i];
            const rowIdx = rowOffset + i;
            if (v != null) {
              indices[rowIdx] = dict.indexMap.get(v) ?? 0;
              nullBitmap[rowIdx >>> 3] |= 1 << (rowIdx & 7);
            } else {
              nullCount++;
            }
          }
        } else {
          nullCount += buf.writeIndex;
        }
        rowOffset += buf.writeIndex;
      }

      vectors.push(
        arrow.makeVector(
          arrow.makeData({
            type: fieldType as arrow.Dictionary<arrow.Utf8, arrow.Uint32>,
            offset: 0,
            length: totalRows,
            nullCount,
            data: indices,
            nullBitmap: nullCount > 0 ? nullBitmap : undefined,
            dictionary: arrow.makeVector(
              arrow.makeData({
                type: new arrow.Utf8(),
                offset: 0,
                length: dict.indexMap.size,
                nullCount: 0,
                valueOffsets: dict.offsets,
                data: dict.data,
              }),
            ),
          }),
        ),
      );
    } else if (lmaoType === 'number') {
      const allValues = new Float64Array(totalRows);
      const nullBitmap = new Uint8Array(Math.ceil(totalRows / 8));
      nullBitmap.fill(0xff);
      let nullCount = 0;
      let rowOffset = 0;

      for (const buf of buffers) {
        const col = buf[valuesKey];
        const srcNulls = buf[nullsKey] as Uint8Array | undefined;

        if (col instanceof Float64Array) {
          allValues.set(col.subarray(0, buf.writeIndex), rowOffset);
          if (srcNulls) {
            // Copy null bits - still need per-bit loop for non-aligned copy
            for (let i = 0; i < buf.writeIndex; i++) {
              const srcByte = i >>> 3;
              const srcBit = i & 7;
              const isValid = (srcNulls[srcByte] & (1 << srcBit)) !== 0;
              if (!isValid) {
                const rowIdx = rowOffset + i;
                nullBitmap[rowIdx >>> 3] &= ~(1 << (rowIdx & 7));
                nullCount++;
              }
            }
          }
        } else {
          // Column doesn't exist for this buffer - all nulls
          clearBitRange(nullBitmap, rowOffset, buf.writeIndex);
          nullCount += buf.writeIndex;
        }
        rowOffset += buf.writeIndex;
      }

      vectors.push(
        arrow.makeVector(
          arrow.makeData({
            type: fieldType as arrow.Float64,
            offset: 0,
            length: totalRows,
            nullCount,
            data: allValues,
            nullBitmap: nullCount > 0 ? nullBitmap : undefined,
          }),
        ),
      );
    } else if (lmaoType === 'boolean') {
      const requiredBytes = Math.ceil(totalRows / 8);
      const allValues = new Uint8Array(requiredBytes);
      const nullBitmap = new Uint8Array(requiredBytes);
      nullBitmap.fill(0xff);
      let nullCount = 0;
      let rowOffset = 0;

      for (const buf of buffers) {
        const col = buf[valuesKey];
        const srcNulls = buf[nullsKey] as Uint8Array | undefined;

        if (col instanceof Uint8Array) {
          // Copy boolean values bit by bit - can't avoid loop for bit-level operations
          for (let i = 0; i < buf.writeIndex; i++) {
            const srcByte = i >>> 3;
            const srcBit = i & 7;
            const value = (col[srcByte] & (1 << srcBit)) !== 0;
            const rowIdx = rowOffset + i;
            if (value) {
              allValues[rowIdx >>> 3] |= 1 << (rowIdx & 7);
            }
            if (srcNulls) {
              const isValid = (srcNulls[srcByte] & (1 << srcBit)) !== 0;
              if (!isValid) {
                nullBitmap[rowIdx >>> 3] &= ~(1 << (rowIdx & 7));
                nullCount++;
              }
            }
          }
        } else {
          // Column doesn't exist for this buffer - all nulls
          clearBitRange(nullBitmap, rowOffset, buf.writeIndex);
          nullCount += buf.writeIndex;
        }
        rowOffset += buf.writeIndex;
      }

      vectors.push(
        arrow.makeVector(
          arrow.makeData({
            type: fieldType as arrow.Bool,
            offset: 0,
            length: totalRows,
            nullCount,
            data: allValues,
            nullBitmap: nullCount > 0 ? nullBitmap : undefined,
          }),
        ),
      );
    } else if (lmaoType === 'enum') {
      const enumValues = getEnumValues(fieldSchema) || [];
      const enumUtf8 = getEnumUtf8(fieldSchema);
      const allIndices = new Uint8Array(totalRows);
      const nullBitmap = new Uint8Array(Math.ceil(totalRows / 8));
      nullBitmap.fill(0xff);
      let nullCount = 0;
      let rowOffset = 0;

      for (const buf of buffers) {
        const col = buf[valuesKey];
        const srcNulls = buf[nullsKey] as Uint8Array | undefined;

        if (col instanceof Uint8Array) {
          allIndices.set(col.subarray(0, buf.writeIndex), rowOffset);
          if (srcNulls) {
            for (let i = 0; i < buf.writeIndex; i++) {
              const srcByte = i >>> 3;
              const srcBit = i & 7;
              const isValid = (srcNulls[srcByte] & (1 << srcBit)) !== 0;
              if (!isValid) {
                const rowIdx = rowOffset + i;
                nullBitmap[rowIdx >>> 3] &= ~(1 << (rowIdx & 7));
                nullCount++;
              }
            }
          }
        } else {
          for (let i = 0; i < buf.writeIndex; i++) {
            const rowIdx = rowOffset + i;
            nullBitmap[rowIdx >>> 3] &= ~(1 << (rowIdx & 7));
            nullCount++;
          }
        }
        rowOffset += buf.writeIndex;
      }

      const enumDictData = arrow.makeData({
        type: new arrow.Utf8(),
        offset: 0,
        length: enumValues.length,
        nullCount: 0,
        valueOffsets: enumUtf8?.offsets ?? calculateUtf8Offsets(enumValues as readonly string[]),
        data: enumUtf8?.concatenated ?? encodeUtf8Strings(enumValues as readonly string[]),
      });

      vectors.push(
        arrow.makeVector(
          arrow.makeData({
            type: fieldType as arrow.Dictionary<arrow.Utf8, arrow.Uint8>,
            offset: 0,
            length: totalRows,
            nullCount,
            data: allIndices,
            nullBitmap: nullCount > 0 ? nullBitmap : undefined,
            dictionary: arrow.makeVector(enumDictData),
          }),
        ),
      );
    }

    fieldIdx++;
  }

  const structData = arrow.makeData({
    type: new arrow.Struct(arrowSchema.fields),
    length: totalRows,
    nullCount: 0,
    children: vectors.map((v) => v.data[0]),
  });

  return new arrow.RecordBatch(arrowSchema, structData);
}
