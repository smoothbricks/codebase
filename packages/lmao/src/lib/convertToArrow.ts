/**
 * Zero-copy conversion from SpanBuffer to Apache Arrow tables
 *
 * Per specs/01f_arrow_table_structure.md:
 * - Enum columns: Dictionary with compile-time values
 * - Category columns: Dictionary with runtime-built values
 * - Text columns: Plain strings without dictionary
 * - Zero-copy wrap TypedArrays as Arrow vectors
 */

import {
  clearBitRange,
  compareStrings,
  countNulls,
  createSortedDictionary,
  DictionaryBuilder,
  type FinalizedDictionary,
  getMaskTransform,
  type PreEncodedEntry,
  sortInPlace,
} from '@smoothbricks/arrow-builder';
import {
  Bool,
  type Data,
  Dictionary,
  Field,
  Float64,
  Int8,
  Int32,
  makeData,
  makeVector,
  RecordBatch,
  Schema,
  Struct,
  Table,
  TimestampNanosecond,
  Uint8,
  Uint16,
  Uint32,
  Uint64,
  Utf8,
  type Vector,
} from 'apache-arrow';
import { ENTRY_TYPE_NAMES } from './lmao.js';
import type { ModuleContext } from './moduleContext.js';
import { getEnumUtf8, getEnumValues, getLmaoSchemaType } from './schema/typeGuards.js';
import type { SpanBuffer } from './types.js';
import { globalUtf8Cache } from './utf8Cache.js';

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

/**
 * Concatenate Uint8 arrays without type casting.
 */
function concatenateUint8Arrays(arrays: Uint8Array[]): Uint8Array {
  if (arrays.length === 0) throw new Error('Cannot concatenate empty array list');
  if (arrays.length === 1) return arrays[0];
  const totalLength = arrays.reduce((sum, arr) => sum + arr.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const arr of arrays) {
    result.set(arr, offset);
    offset += arr.length;
  }
  return result;
}

/**
 * Concatenate Float64 arrays without type casting.
 */
function concatenateFloat64Arrays(arrays: Float64Array[]): Float64Array {
  if (arrays.length === 0) throw new Error('Cannot concatenate empty array list');
  if (arrays.length === 1) return arrays[0];
  const totalLength = arrays.reduce((sum, arr) => sum + arr.length, 0);
  const result = new Float64Array(totalLength);
  let offset = 0;
  for (const arr of arrays) {
    result.set(arr, offset);
    offset += arr.length;
  }
  return result;
}

function concatenateNullBitmaps(
  buffers: SpanBuffer[],
  columnName: string,
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
      // Count nulls using countNulls from arrow-builder
      nullCount += countNulls(sourceBitmap, rowCount);
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
  columnName: string,
  maskTransform?: (value: string) => string,
): { dictionary: string[]; indices: Uint32Array; nullBitmap: Uint8Array | undefined; nullCount: number } {
  const valuesName = `${columnName}_values` as const;
  const totalRows = buffers.reduce((sum, buf) => sum + buf.writeIndex, 0);

  // Build mapping from original value to masked value (for dictionary lookup)
  // and collect unique masked values for the dictionary
  const uniqueMaskedStrings = new Set<string>();
  const originalToMasked = new Map<string, string>();

  for (const buf of buffers) {
    const column = buf[valuesName];
    if (column && Array.isArray(column)) {
      for (let i = 0; i < buf.writeIndex; i++) {
        const value = column[i];
        if (value != null && !originalToMasked.has(value)) {
          const maskedValue = maskTransform ? maskTransform(value) : value;
          originalToMasked.set(value, maskedValue);
          uniqueMaskedStrings.add(maskedValue);
        }
      }
    }
  }

  const dictionary = Array.from(uniqueMaskedStrings).sort();
  const maskedToIndex = new Map(dictionary.map((s, i) => [s, i]));

  const indices = new Uint32Array(totalRows);
  const { nullBitmap, nullCount } = concatenateNullBitmaps(buffers, columnName);

  let rowOffset = 0;
  for (const buf of buffers) {
    const column = buf[valuesName];
    if (column && Array.isArray(column)) {
      for (let i = 0; i < buf.writeIndex; i++) {
        const value = column[i];
        if (value != null) {
          const maskedValue = originalToMasked.get(value) ?? value;
          indices[rowOffset + i] = maskedToIndex.get(maskedValue) ?? 0;
        } else {
          indices[rowOffset + i] = 0;
        }
      }
    }
    rowOffset += buf.writeIndex;
  }

  return { dictionary, indices, nullBitmap, nullCount };
}

function buildTextDictionary(
  buffers: SpanBuffer[],
  columnName: string,
  maskTransform?: (value: string) => string,
): { dictionary: string[]; indices: Uint32Array; nullBitmap: Uint8Array | undefined; nullCount: number } | null {
  const valuesName = `${columnName}_values` as const;
  const totalRows = buffers.reduce((sum, buf) => sum + buf.writeIndex, 0);

  // Build mapping from original value to masked value and track frequency of masked values
  const frequencyMap = new Map<string, number>();
  const originalToMasked = new Map<string, string>();

  for (const buf of buffers) {
    const column = buf[valuesName];
    if (column && Array.isArray(column)) {
      for (let i = 0; i < buf.writeIndex; i++) {
        const value = column[i];
        if (value != null) {
          let maskedValue: string;
          if (originalToMasked.has(value)) {
            const masked = originalToMasked.get(value);
            if (masked === undefined) {
              throw new Error(`Masked value not found for: ${value}`);
            }
            maskedValue = masked;
          } else {
            maskedValue = maskTransform ? maskTransform(value) : value;
            originalToMasked.set(value, maskedValue);
          }
          frequencyMap.set(maskedValue, (frequencyMap.get(maskedValue) ?? 0) + 1);
        }
      }
    }
  }

  if (frequencyMap.size === 0) {
    return { dictionary: [], indices: new Uint32Array(totalRows), nullBitmap: undefined, nullCount: totalRows };
  }

  const dictionary = Array.from(frequencyMap.keys());
  const maskedToIndex = new Map(dictionary.map((s, i) => [s, i]));

  const indices = new Uint32Array(totalRows);
  const { nullBitmap, nullCount } = concatenateNullBitmaps(buffers, columnName);

  let rowOffset = 0;
  for (const buf of buffers) {
    const column = buf[valuesName];
    if (column && Array.isArray(column)) {
      for (let i = 0; i < buf.writeIndex; i++) {
        const value = column[i];
        if (value != null) {
          const maskedValue = originalToMasked.get(value) ?? value;
          indices[rowOffset + i] = maskedToIndex.get(maskedValue) ?? 0;
        } else {
          indices[rowOffset + i] = 0;
        }
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
) => { fields: Field[]; vectors: Vector[] };

/**
 * Convert SpanBuffer (and its overflow chain) to Arrow RecordBatch
 */
export function convertToRecordBatch(buffer: SpanBuffer, systemColumnBuilder?: SystemColumnBuilder): RecordBatch {
  const buffers: SpanBuffer[] = [];
  let currentBuffer: SpanBuffer | undefined = buffer;

  while (currentBuffer) {
    buffers.push(currentBuffer);
    currentBuffer = currentBuffer.next as SpanBuffer | undefined;
  }

  return convertBuffersToRecordBatch(buffers, systemColumnBuilder);
}

function convertBuffersToRecordBatch(buffers: SpanBuffer[], systemColumnBuilder?: SystemColumnBuilder): RecordBatch {
  if (buffers.length === 0) return new RecordBatch({});

  const totalRows = buffers.reduce((sum, buf) => sum + buf.writeIndex, 0);
  if (totalRows === 0) return new RecordBatch({});

  const schema = buffers[0].task.module.tagAttributes;
  const fields: Field[] = [];
  let systemVectors: Vector[] = [];

  if (systemColumnBuilder) {
    const systemColumns = systemColumnBuilder(buffers[0], buffers, totalRows);
    fields.push(...systemColumns.fields);
    systemVectors = systemColumns.vectors;
  } else {
    fields.push(Field.new({ name: 'timestamp', type: new TimestampNanosecond() }));
    fields.push(Field.new({ name: 'trace_id', type: new Dictionary(new Utf8(), new Int32()) }));
    // Span ID columns (separate columns instead of struct)
    fields.push(Field.new({ name: 'thread_id', type: new Uint64() }));
    fields.push(Field.new({ name: 'span_id', type: new Uint32() }));
    fields.push(Field.new({ name: 'parent_thread_id', type: new Uint64(), nullable: true }));
    fields.push(Field.new({ name: 'parent_span_id', type: new Uint32(), nullable: true }));
    fields.push(Field.new({ name: 'entry_type', type: new Dictionary(new Utf8(), new Int8()) }));
    fields.push(Field.new({ name: 'package_name', type: new Dictionary(new Utf8(), new Int32()) }));
    fields.push(Field.new({ name: 'package_path', type: new Dictionary(new Utf8(), new Int32()) }));
    fields.push(Field.new({ name: 'git_sha', type: new Dictionary(new Utf8(), new Int32()) }));
    fields.push(Field.new({ name: 'span_name', type: new Dictionary(new Utf8(), new Int32()) }));
  }

  for (const [fieldName, fieldSchema] of Object.entries(schema)) {
    const lmaoType = getLmaoSchemaType(fieldSchema);
    const arrowFieldName = getArrowFieldName(fieldName);

    if (lmaoType === 'enum') {
      fields.push(
        Field.new({
          name: arrowFieldName,
          type: new Dictionary(new Utf8(), new Uint8()),
          nullable: true,
        }),
      );
    } else if (lmaoType === 'category') {
      fields.push(
        Field.new({
          name: arrowFieldName,
          type: new Dictionary(new Utf8(), new Uint32()),
          nullable: true,
        }),
      );
    } else if (lmaoType === 'text') {
      fields.push(
        Field.new({
          name: arrowFieldName,
          type: new Dictionary(new Utf8(), new Uint32()),
          nullable: true,
        }),
      );
    } else if (lmaoType === 'number') {
      fields.push(Field.new({ name: arrowFieldName, type: new Float64(), nullable: true }));
    } else if (lmaoType === 'boolean') {
      fields.push(Field.new({ name: arrowFieldName, type: new Bool(), nullable: true }));
    }
  }

  const arrowSchema = new Schema(fields);
  const vectors: Vector[] = [];

  if (systemColumnBuilder) {
    vectors.push(...systemVectors);
  } else {
    buildDefaultSystemVectors(buffers, vectors);
  }

  for (const [fieldName, fieldSchema] of Object.entries(schema)) {
    const lmaoType = getLmaoSchemaType(fieldSchema);
    const columnName = fieldName; // User columns have no prefix

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

      const allIndices = concatenateUint8Arrays(valueArrays);
      const { nullBitmap, nullCount } = concatenateNullBitmaps(buffers, columnName);

      const enumDictData = makeData({
        type: new Utf8(),
        offset: 0,
        length: enumValues.length,
        nullCount: 0,
        valueOffsets: enumUtf8?.offsets ?? calculateUtf8Offsets(enumValues as readonly string[]),
        data: enumUtf8?.concatenated ?? encodeUtf8Strings(enumValues as readonly string[]),
      });

      const enumData = makeData({
        type: new Dictionary(new Utf8(), new Uint8()),
        offset: 0,
        length: totalRows,
        nullCount,
        data: allIndices,
        nullBitmap,
        dictionary: makeVector(enumDictData),
      });

      vectors.push(makeVector(enumData));
    } else if (lmaoType === 'category') {
      // Get mask transform from schema metadata (if present)
      const maskTransform = getMaskTransform(fieldSchema);
      const { dictionary, indices, nullBitmap, nullCount } = buildSortedCategoryDictionary(
        buffers,
        columnName,
        maskTransform,
      );
      const { data: categoryUtf8Data, offsets: categoryUtf8Offsets } = globalUtf8Cache.encodeMany(dictionary);

      const categoryDictData = makeData({
        type: new Utf8(),
        offset: 0,
        length: dictionary.length,
        nullCount: 0,
        valueOffsets: categoryUtf8Offsets,
        data: categoryUtf8Data,
      });

      const categoryData = makeData({
        type: new Dictionary(new Utf8(), new Uint32()),
        offset: 0,
        length: totalRows,
        nullCount,
        data: indices,
        nullBitmap,
        dictionary: makeVector(categoryDictData),
      });

      vectors.push(makeVector(categoryData));
    } else if (lmaoType === 'text') {
      // Get mask transform from schema metadata (if present)
      const maskTransform = getMaskTransform(fieldSchema);
      const result = buildTextDictionary(buffers, columnName, maskTransform);
      if (result) {
        const { dictionary, indices, nullBitmap, nullCount } = result;
        const { data: textUtf8Data, offsets: textUtf8Offsets } = globalUtf8Cache.encodeMany(dictionary);

        const textDictData = makeData({
          type: new Utf8(),
          offset: 0,
          length: dictionary.length,
          nullCount: 0,
          valueOffsets: textUtf8Offsets,
          data: textUtf8Data,
        });

        const textData = makeData({
          type: new Dictionary(new Utf8(), new Uint32()),
          offset: 0,
          length: totalRows,
          nullCount,
          data: indices,
          nullBitmap,
          dictionary: makeVector(textDictData),
        });

        vectors.push(makeVector(textData));
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

      const allValues = concatenateFloat64Arrays(valueArrays);
      const { nullBitmap, nullCount } = concatenateNullBitmaps(buffers, columnName);

      const numberData = makeData({
        type: new Float64(),
        offset: 0,
        length: totalRows,
        nullCount,
        data: allValues,
        nullBitmap,
      });

      vectors.push(makeVector(numberData));
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

      const allValues = concatenateUint8Arrays(valueArrays);
      const { nullBitmap, nullCount } = concatenateNullBitmaps(buffers, columnName);

      const boolData = makeData({
        type: new Bool(),
        offset: 0,
        length: totalRows,
        nullCount,
        data: allValues,
        nullBitmap,
      });

      vectors.push(makeVector(boolData));
    }
  }

  const data = makeData({
    type: new Struct(arrowSchema.fields),
    length: totalRows,
    nullCount: 0,
    children: vectors.map((v) => v.data[0]),
  });

  return new RecordBatch(arrowSchema, data);
}

/**
 * Convert SpanBuffer (and its overflow chain) to Arrow Table
 */
export function convertToArrowTable(buffer: SpanBuffer, systemColumnBuilder?: SystemColumnBuilder): Table {
  const batch = convertToRecordBatch(buffer, systemColumnBuilder);
  if (batch.numRows === 0) return new Table();
  return new Table([batch]);
}

function buildDefaultSystemVectors(buffers: SpanBuffer[], vectors: Vector[]): void {
  const totalRows = buffers.reduce((sum, buf) => sum + buf.writeIndex, 0);

  // Timestamp - BigInt64Array with nanoseconds
  const allTimestamps = new BigInt64Array(totalRows);
  let timestampOffset = 0;
  for (const buf of buffers) {
    // Use set() with subarray - bulk copy
    // Defensive check: timestamps should always exist, but guard against corruption
    if (!buf.timestamps) {
      throw new Error(`Buffer missing timestamps property (writeIndex: ${buf.writeIndex})`);
    }
    allTimestamps.set(buf.timestamps.subarray(0, buf.writeIndex), timestampOffset);
    timestampOffset += buf.writeIndex;
  }
  vectors.push(
    makeVector(
      makeData({
        type: new TimestampNanosecond(),
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
    const traceIdIndex = traceIdMap.get(buf.traceId);
    if (traceIdIndex === undefined) {
      throw new Error(`TraceId index not found for: ${buf.traceId}`);
    }
    // Use fill() - constant value per buffer
    traceIdIndices.fill(traceIdIndex, rowOffset, rowOffset + buf.writeIndex);
    rowOffset += buf.writeIndex;
  }

  const traceIdDictData = makeData({
    type: new Utf8(),
    offset: 0,
    length: traceIdArray.length,
    nullCount: 0,
    valueOffsets: calculateUtf8Offsets(traceIdArray),
    data: encodeUtf8Strings(traceIdArray),
  });

  vectors.push(
    makeVector(
      makeData({
        type: new Dictionary(new Utf8(), new Int32()),
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: traceIdIndices,
        dictionary: makeVector(traceIdDictData),
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
    makeVector(
      makeData({
        type: new Uint64(),
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
    makeVector(
      makeData({
        type: new Uint32(),
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
    makeVector(
      makeData({
        type: new Uint64(),
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
    makeVector(
      makeData({
        type: new Uint32(),
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
    // Defensive check: operations should always exist, but guard against corruption
    if (!buf.operations) {
      throw new Error(`Buffer missing operations property (writeIndex: ${buf.writeIndex})`);
    }
    entryTypeIndices.set(buf.operations.subarray(0, buf.writeIndex), rowOffset);
    rowOffset += buf.writeIndex;
  }
  const entryTypeDictData = makeData({
    type: new Utf8(),
    offset: 0,
    length: ENTRY_TYPE_NAMES.length,
    nullCount: 0,
    valueOffsets: calculateUtf8Offsets(ENTRY_TYPE_NAMES),
    data: encodeUtf8Strings(ENTRY_TYPE_NAMES),
  });
  vectors.push(
    makeVector(
      makeData({
        type: new Dictionary(new Utf8(), new Int8()),
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: entryTypeIndices,
        dictionary: makeVector(entryTypeDictData),
      }),
    ),
  );

  // Package - using direct string access via buf.task.module.packageName
  const packageSet = new Set<string>();
  for (const buf of buffers) packageSet.add(buf.task.module.packageName);
  const packageArray = Array.from(packageSet);
  const packageMap = new Map(packageArray.map((name, idx) => [name, idx]));

  const packageIndices = new Int32Array(totalRows);
  rowOffset = 0;
  for (const buf of buffers) {
    const packageIndex = packageMap.get(buf.task.module.packageName);
    if (packageIndex === undefined) {
      throw new Error(`Package index not found for: ${buf.task.module.packageName}`);
    }
    // Use fill() - constant value per buffer
    packageIndices.fill(packageIndex, rowOffset, rowOffset + buf.writeIndex);
    rowOffset += buf.writeIndex;
  }

  const packageDictData = makeData({
    type: new Utf8(),
    offset: 0,
    length: packageArray.length,
    nullCount: 0,
    valueOffsets: calculateUtf8Offsets(packageArray),
    data: encodeUtf8Strings(packageArray),
  });
  vectors.push(
    makeVector(
      makeData({
        type: new Dictionary(new Utf8(), new Int32()),
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: packageIndices,
        dictionary: makeVector(packageDictData),
      }),
    ),
  );

  // Module Path - using direct string access via buf.task.module.packagePath
  const modulePathSet = new Set<string>();
  for (const buf of buffers) modulePathSet.add(buf.task.module.packagePath);
  const modulePathArray = Array.from(modulePathSet);
  const modulePathMap = new Map(modulePathArray.map((name, idx) => [name, idx]));

  const packagePathIndices = new Int32Array(totalRows);
  rowOffset = 0;
  for (const buf of buffers) {
    const modulePathIndex = modulePathMap.get(buf.task.module.packagePath);
    if (modulePathIndex === undefined) {
      throw new Error(`Module path index not found for: ${buf.task.module.packagePath}`);
    }
    // Use fill() - constant value per buffer
    packagePathIndices.fill(modulePathIndex, rowOffset, rowOffset + buf.writeIndex);
    rowOffset += buf.writeIndex;
  }

  const packagePathDictData = makeData({
    type: new Utf8(),
    offset: 0,
    length: modulePathArray.length,
    nullCount: 0,
    valueOffsets: calculateUtf8Offsets(modulePathArray),
    data: encodeUtf8Strings(modulePathArray),
  });
  vectors.push(
    makeVector(
      makeData({
        type: new Dictionary(new Utf8(), new Int32()),
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: packagePathIndices,
        dictionary: makeVector(packagePathDictData),
      }),
    ),
  );

  // Git SHA - extracted from module context
  const gitShaSet = new Set<string>();
  for (const buf of buffers) gitShaSet.add(buf.task.module.gitSha);
  const gitShaArray = Array.from(gitShaSet);
  const gitShaMap = new Map(gitShaArray.map((sha, idx) => [sha, idx]));

  const gitShaIndices = new Int32Array(totalRows);
  rowOffset = 0;
  for (const buf of buffers) {
    const gitShaIndex = gitShaMap.get(buf.task.module.gitSha);
    if (gitShaIndex === undefined) {
      throw new Error(`GitSha index not found for: ${buf.task.module.gitSha}`);
    }
    // Use fill() - constant value per buffer
    gitShaIndices.fill(gitShaIndex, rowOffset, rowOffset + buf.writeIndex);
    rowOffset += buf.writeIndex;
  }

  const gitShaDictData = makeData({
    type: new Utf8(),
    offset: 0,
    length: gitShaArray.length,
    nullCount: 0,
    valueOffsets: calculateUtf8Offsets(gitShaArray),
    data: encodeUtf8Strings(gitShaArray),
  });
  vectors.push(
    makeVector(
      makeData({
        type: new Dictionary(new Utf8(), new Int32()),
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: gitShaIndices,
        dictionary: makeVector(gitShaDictData),
      }),
    ),
  );

  // Span name - now using direct string access
  const spanNameSet = new Set<string>();
  for (const buf of buffers) spanNameSet.add(buf.task.spanName);
  const spanNameArray = Array.from(spanNameSet);
  const spanNameMap = new Map(spanNameArray.map((name, idx) => [name, idx]));

  const spanNameIndices = new Int32Array(totalRows);
  rowOffset = 0;
  for (const buf of buffers) {
    const spanNameIndex = spanNameMap.get(buf.task.spanName);
    if (spanNameIndex === undefined) {
      throw new Error(`SpanName index not found for: ${buf.task.spanName}`);
    }
    // Use fill() - constant value per buffer
    spanNameIndices.fill(spanNameIndex, rowOffset, rowOffset + buf.writeIndex);
    rowOffset += buf.writeIndex;
  }

  const spanNameDictData = makeData({
    type: new Utf8(),
    offset: 0,
    length: spanNameArray.length,
    nullCount: 0,
    valueOffsets: calculateUtf8Offsets(spanNameArray),
    data: encodeUtf8Strings(spanNameArray),
  });
  vectors.push(
    makeVector(
      makeData({
        type: new Dictionary(new Utf8(), new Int32()),
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: spanNameIndices,
        dictionary: makeVector(spanNameDictData),
      }),
    ),
  );
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tree Conversion with Shared Dictionaries
// ═══════════════════════════════════════════════════════════════════════════════

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
export function convertSpanTreeToArrowTable(rootBuffer: SpanBuffer, _systemColumnBuilder?: SystemColumnBuilder): Table {
  const schema = rootBuffer.task.module.tagAttributes;

  // ═══════════════════════════════════════════════════════════════════════════
  // PASS 1: Build dictionaries using DictionaryBuilder from arrow-builder
  // ═══════════════════════════════════════════════════════════════════════════
  const categoryBuilders = new Map<string, DictionaryBuilder>();
  const textBuilders = new Map<string, DictionaryBuilder>();
  // Store mask transforms and original->masked mappings per column
  const categoryMaskTransforms = new Map<string, ((value: string) => string) | undefined>();
  const textMaskTransforms = new Map<string, ((value: string) => string) | undefined>();
  const categoryOriginalToMasked = new Map<string, Map<string, string>>();
  const textOriginalToMasked = new Map<string, Map<string, string>>();

  for (const [fieldName, fieldSchema] of Object.entries(schema)) {
    const lmaoType = getLmaoSchemaType(fieldSchema);
    if (lmaoType === 'category') {
      categoryBuilders.set(fieldName, new DictionaryBuilder(globalUtf8Cache));
      categoryMaskTransforms.set(fieldName, getMaskTransform(fieldSchema));
      categoryOriginalToMasked.set(fieldName, new Map());
    }
    if (lmaoType === 'text') {
      textBuilders.set(fieldName, new DictionaryBuilder(globalUtf8Cache));
      textMaskTransforms.set(fieldName, getMaskTransform(fieldSchema));
      textOriginalToMasked.set(fieldName, new Map());
    }
  }

  // System column dictionaries
  // For traceId and spanName, use DictionaryBuilder (values not pre-encoded)
  const traceIdBuilder = new DictionaryBuilder(globalUtf8Cache);
  const spanNameBuilder = new DictionaryBuilder(globalUtf8Cache);

  // For package, modulePath, gitSha - collect pre-encoded entries for direct dictionary creation
  // Use a Set to deduplicate by ModuleContext identity (same module = same entries)
  const uniqueModules = new Set<ModuleContext>();

  let totalRows = 0;
  walkSpanTree(rootBuffer, (buffer) => {
    totalRows += buffer.writeIndex;
    traceIdBuilder.add(buffer.traceId);
    spanNameBuilder.add(buffer.task.spanName);

    // Collect unique ModuleContexts (already have pre-encoded entries)
    uniqueModules.add(buffer.task.module);

    for (const [fieldName, builder] of categoryBuilders) {
      const col = buffer.getColumnIfAllocated(fieldName) as string[] | undefined;
      const maskTransform = categoryMaskTransforms.get(fieldName);
      const originalToMasked = categoryOriginalToMasked.get(fieldName);
      if (!originalToMasked) {
        throw new Error(`Category originalToMasked map not found for field: ${fieldName}`);
      }
      if (col) {
        for (let i = 0; i < buffer.writeIndex; i++) {
          const originalValue = col[i];
          if (originalValue != null) {
            let maskedValue = originalToMasked.get(originalValue);
            if (maskedValue === undefined) {
              maskedValue = maskTransform ? maskTransform(originalValue) : originalValue;
              originalToMasked.set(originalValue, maskedValue);
            }
            builder.add(maskedValue);
          }
        }
      }
    }

    for (const [fieldName, builder] of textBuilders) {
      const col = buffer.getColumnIfAllocated(fieldName) as string[] | undefined;
      const maskTransform = textMaskTransforms.get(fieldName);
      const originalToMasked = textOriginalToMasked.get(fieldName);
      if (!originalToMasked) {
        throw new Error(`Text originalToMasked map not found for field: ${fieldName}`);
      }
      if (col) {
        for (let i = 0; i < buffer.writeIndex; i++) {
          const originalValue = col[i];
          if (originalValue != null) {
            let maskedValue = originalToMasked.get(originalValue);
            if (maskedValue === undefined) {
              maskedValue = maskTransform ? maskTransform(originalValue) : originalValue;
              originalToMasked.set(originalValue, maskedValue);
            }
            builder.add(maskedValue);
          }
        }
      }
    }
  });

  if (totalRows === 0) return new Table();

  // Finalize dictionaries
  const traceIdDict = traceIdBuilder.finalize(false);

  // Build dictionaries from pre-encoded ModuleContext entries
  // Preallocate arrays, single iteration of set
  const moduleCount = uniqueModules.size;
  const packageEntries: PreEncodedEntry[] = new Array(moduleCount);
  const packagePathEntries: PreEncodedEntry[] = new Array(moduleCount);
  const gitShaEntries: PreEncodedEntry[] = new Array(moduleCount);
  let idx = 0;
  for (const m of uniqueModules) {
    packageEntries[idx] = m.packageEntry;
    packagePathEntries[idx] = m.packagePathEntry;
    gitShaEntries[idx] = m.gitShaEntry;
    idx++;
  }

  const cmp = (a: PreEncodedEntry, b: PreEncodedEntry) => compareStrings(a.str, b.str);
  const packageDict = createSortedDictionary(sortInPlace(packageEntries, cmp));
  const packagePathDict = createSortedDictionary(sortInPlace(packagePathEntries, cmp));
  const gitShaDict = createSortedDictionary(sortInPlace(gitShaEntries, cmp));
  const spanNameDict = spanNameBuilder.finalize(true); // sorted
  const categoryDicts = new Map<string, FinalizedDictionary>();
  const textDicts = new Map<string, FinalizedDictionary>();

  for (const [name, builder] of categoryBuilders) {
    categoryDicts.set(name, builder.finalize(true)); // sorted for binary search
  }
  for (const [name, builder] of textBuilders) {
    textDicts.set(name, builder.finalize(false)); // not sorted
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Build shared Arrow schema with stable dictionary IDs
  // ═══════════════════════════════════════════════════════════════════════════
  const arrowFields: Field[] = [];

  // System columns - create types with explicit dictionary IDs
  arrowFields.push(Field.new({ name: 'timestamp', type: new TimestampNanosecond() }));
  arrowFields.push(Field.new({ name: 'trace_id', type: new Dictionary(new Utf8(), new Int32(), 0) }));
  // Span ID columns (separate columns instead of struct)
  arrowFields.push(Field.new({ name: 'thread_id', type: new Uint64() }));
  arrowFields.push(Field.new({ name: 'span_id', type: new Uint32() }));
  arrowFields.push(Field.new({ name: 'parent_thread_id', type: new Uint64(), nullable: true }));
  arrowFields.push(Field.new({ name: 'parent_span_id', type: new Uint32(), nullable: true }));
  arrowFields.push(Field.new({ name: 'entry_type', type: new Dictionary(new Utf8(), new Int8(), 1) }));
  arrowFields.push(Field.new({ name: 'package_name', type: new Dictionary(new Utf8(), new Int32(), 2) }));
  arrowFields.push(Field.new({ name: 'package_path', type: new Dictionary(new Utf8(), new Int32(), 3) }));
  arrowFields.push(Field.new({ name: 'git_sha', type: new Dictionary(new Utf8(), new Int32(), 4) }));
  arrowFields.push(Field.new({ name: 'span_name', type: new Dictionary(new Utf8(), new Int32(), 5) }));

  // Attribute columns - assign dictionary IDs starting at 6
  let nextDictId = 6;
  const attrDictIds = new Map<string, number>();
  for (const [fieldName, fieldSchema] of Object.entries(schema)) {
    const lmaoType = getLmaoSchemaType(fieldSchema);
    const arrowFieldName = getArrowFieldName(fieldName);

    if (lmaoType === 'enum') {
      attrDictIds.set(fieldName, nextDictId);
      arrowFields.push(
        Field.new({
          name: arrowFieldName,
          type: new Dictionary(new Utf8(), new Uint8(), nextDictId++),
          nullable: true,
        }),
      );
    } else if (lmaoType === 'category') {
      attrDictIds.set(fieldName, nextDictId);
      arrowFields.push(
        Field.new({
          name: arrowFieldName,
          type: new Dictionary(new Utf8(), new Uint32(), nextDictId++),
          nullable: true,
        }),
      );
    } else if (lmaoType === 'text') {
      attrDictIds.set(fieldName, nextDictId);
      arrowFields.push(
        Field.new({
          name: arrowFieldName,
          type: new Dictionary(new Utf8(), new Uint32(), nextDictId++),
          nullable: true,
        }),
      );
    } else if (lmaoType === 'number') {
      arrowFields.push(Field.new({ name: arrowFieldName, type: new Float64(), nullable: true }));
    } else if (lmaoType === 'boolean') {
      arrowFields.push(Field.new({ name: arrowFieldName, type: new Bool(), nullable: true }));
    }
  }

  const arrowSchema = new Schema(arrowFields);

  // ═══════════════════════════════════════════════════════════════════════════
  // PASS 2: Collect all buffers with non-zero rows
  // ═══════════════════════════════════════════════════════════════════════════
  const allBuffers: SpanBuffer[] = [];

  walkSpanTree(rootBuffer, (buffer) => {
    if (buffer.writeIndex > 0) {
      allBuffers.push(buffer);
    }
  });

  if (allBuffers.length === 0) return new Table();

  // Build a single RecordBatch from all buffers
  const batch = convertBuffersWithSharedDicts(
    allBuffers,
    arrowSchema,
    traceIdDict,
    packageDict,
    packagePathDict,
    gitShaDict,
    spanNameDict,
    categoryDicts,
    textDicts,
    attrDictIds,
    schema,
    categoryOriginalToMasked,
    textOriginalToMasked,
  );

  return new Table([batch]);
}

// Use FinalizedDictionary type from arrow-builder

/**
 * Convert multiple buffers to a single RecordBatch using pre-built shared dictionaries
 */
function convertBuffersWithSharedDicts(
  buffers: SpanBuffer[],
  arrowSchema: Schema,
  traceIdDict: FinalizedDictionary,
  packageDict: FinalizedDictionary,
  packagePathDict: FinalizedDictionary,
  gitShaDict: FinalizedDictionary,
  spanNameDict: FinalizedDictionary,
  categoryDicts: Map<string, FinalizedDictionary>,
  textDicts: Map<string, FinalizedDictionary>,
  _attrDictIds: Map<string, number>,
  lmaoSchema: Record<string, unknown>,
  categoryOriginalToMasked: Map<string, Map<string, string>>,
  textOriginalToMasked: Map<string, Map<string, string>>,
): RecordBatch {
  const totalRows = buffers.reduce((sum, buf) => sum + buf.writeIndex, 0);
  const vectors: Vector[] = [];

  // Get types from the shared schema
  // Schema order: timestamp(0), trace_id(1), thread_id(2), span_id(3), parent_thread_id(4), parent_span_id(5),
  //               entry_type(6), package(7), module_path(8), git_sha(9), span_name(10)
  const timestampType = arrowSchema.fields[0].type as TimestampNanosecond;
  const traceIdType = arrowSchema.fields[1].type as Dictionary<Utf8, Int32>;
  // Span ID columns: thread_id (2), span_id (3), parent_thread_id (4), parent_span_id (5)
  const entryTypeType = arrowSchema.fields[6].type as Dictionary<Utf8, Int8>;
  const packageType = arrowSchema.fields[7].type as Dictionary<Utf8, Int32>;
  const packagePathType = arrowSchema.fields[8].type as Dictionary<Utf8, Int32>;
  const gitShaType = arrowSchema.fields[9].type as Dictionary<Utf8, Int32>;
  const spanNameType = arrowSchema.fields[10].type as Dictionary<Utf8, Int32>;

  // ═══════════════════════════════════════════════════════════════════════════
  // System columns - concatenate data from all buffers
  // ═══════════════════════════════════════════════════════════════════════════

  // Timestamp - BigInt64Array with nanoseconds (zero-copy compatible)
  const allTimestamps = new BigInt64Array(totalRows);
  let offset = 0;
  for (const buf of buffers) {
    // Use set() with subarray - bulk copy
    // Defensive check: timestamps should always exist, but guard against corruption
    if (!buf.timestamps) {
      throw new Error(`Buffer missing timestamps property (writeIndex: ${buf.writeIndex})`);
    }
    allTimestamps.set(buf.timestamps.subarray(0, buf.writeIndex), offset);
    offset += buf.writeIndex;
  }
  vectors.push(
    makeVector(
      makeData({
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
    makeVector(
      makeData({
        type: traceIdType,
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: traceIdIndices,
        dictionary: makeVector(
          makeData({
            type: new Utf8(),
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
    makeVector(
      makeData({
        type: new Uint64(),
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
    makeVector(
      makeData({
        type: new Uint32(),
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
    makeVector(
      makeData({
        type: new Uint64(),
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
    makeVector(
      makeData({
        type: new Uint32(),
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
    // Defensive check: operations should always exist, but guard against corruption
    if (!buf.operations) {
      throw new Error(`Buffer missing operations property (writeIndex: ${buf.writeIndex})`);
    }
    entryTypeIndices.set(buf.operations.subarray(0, buf.writeIndex), offset);
    offset += buf.writeIndex;
  }
  const entryTypeDictData = makeData({
    type: new Utf8(),
    offset: 0,
    length: ENTRY_TYPE_NAMES.length,
    nullCount: 0,
    valueOffsets: calculateUtf8Offsets(ENTRY_TYPE_NAMES),
    data: encodeUtf8Strings(ENTRY_TYPE_NAMES),
  });
  vectors.push(
    makeVector(
      makeData({
        type: entryTypeType,
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: entryTypeIndices,
        dictionary: makeVector(entryTypeDictData),
      }),
    ),
  );

  // Package (using shared dictionary)
  const packageIndices = new Int32Array(totalRows);
  offset = 0;
  for (const buf of buffers) {
    const pkgIdx = packageDict.indexMap.get(buf.task.module.packageName) ?? 0;
    packageIndices.fill(pkgIdx, offset, offset + buf.writeIndex);
    offset += buf.writeIndex;
  }
  vectors.push(
    makeVector(
      makeData({
        type: packageType,
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: packageIndices,
        dictionary: makeVector(
          makeData({
            type: new Utf8(),
            offset: 0,
            length: packageDict.indexMap.size,
            nullCount: 0,
            valueOffsets: packageDict.offsets,
            data: packageDict.data,
          }),
        ),
      }),
    ),
  );

  // Module path (using shared dictionary)
  const packagePathIndices = new Int32Array(totalRows);
  offset = 0;
  for (const buf of buffers) {
    const pathIdx = packagePathDict.indexMap.get(buf.task.module.packagePath) ?? 0;
    packagePathIndices.fill(pathIdx, offset, offset + buf.writeIndex);
    offset += buf.writeIndex;
  }
  vectors.push(
    makeVector(
      makeData({
        type: packagePathType,
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: packagePathIndices,
        dictionary: makeVector(
          makeData({
            type: new Utf8(),
            offset: 0,
            length: packagePathDict.indexMap.size,
            nullCount: 0,
            valueOffsets: packagePathDict.offsets,
            data: packagePathDict.data,
          }),
        ),
      }),
    ),
  );

  // Git SHA (using shared dictionary)
  const gitShaIndices2 = new Int32Array(totalRows);
  offset = 0;
  for (const buf of buffers) {
    const gitSha = buf.task.module.gitSha;
    const idx = gitShaDict.indexMap.get(gitSha) ?? 0;
    // Use fill() - constant value per buffer
    gitShaIndices2.fill(idx, offset, offset + buf.writeIndex);
    offset += buf.writeIndex;
  }
  vectors.push(
    makeVector(
      makeData({
        type: gitShaType,
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: gitShaIndices2,
        dictionary: makeVector(
          makeData({
            type: new Utf8(),
            offset: 0,
            length: gitShaDict.indexMap.size,
            nullCount: 0,
            valueOffsets: gitShaDict.offsets,
            data: gitShaDict.data,
          }),
        ),
      }),
    ),
  );

  // Span name (using shared dictionary) - direct string access via buf.task.spanName
  const spanNameIndices = new Int32Array(totalRows);
  offset = 0;
  for (const buf of buffers) {
    const idx = spanNameDict.indexMap.get(buf.task.spanName) ?? 0;
    // Use fill() - constant value per buffer
    spanNameIndices.fill(idx, offset, offset + buf.writeIndex);
    offset += buf.writeIndex;
  }
  vectors.push(
    makeVector(
      makeData({
        type: spanNameType,
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: spanNameIndices,
        dictionary: makeVector(
          makeData({
            type: new Utf8(),
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
  const SYSTEM_FIELDS_COUNT = 10;
  let fieldIdx = SYSTEM_FIELDS_COUNT;

  for (const [fieldName, fieldSchema] of Object.entries(lmaoSchema)) {
    const lmaoType = getLmaoSchemaType(fieldSchema);
    const columnName = fieldName; // User columns have no prefix
    // Get the type from the shared schema
    const fieldType = arrowSchema.fields[fieldIdx].type;

    if (lmaoType === 'category') {
      const dict = categoryDicts.get(fieldName);
      const originalToMasked = categoryOriginalToMasked.get(fieldName);
      if (!dict || !originalToMasked) {
        throw new Error(`Category dictionary or mapping not found for field: ${fieldName}`);
      }
      const indices = new Uint32Array(totalRows);
      const nullBitmap = new Uint8Array(Math.ceil(totalRows / 8));
      let nullCount = 0;
      let rowOffset = 0;

      for (const buf of buffers) {
        const col = buf.getColumnIfAllocated(columnName) as string[] | undefined;
        if (col) {
          for (let i = 0; i < buf.writeIndex; i++) {
            const v = col[i];
            const rowIdx = rowOffset + i;
            if (v != null) {
              // Look up the masked value, then find its index in the dictionary
              const maskedValue = originalToMasked.get(v) ?? v;
              indices[rowIdx] = dict.indexMap.get(maskedValue) ?? 0;
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
        makeVector(
          makeData({
            type: fieldType as Dictionary<Utf8, Uint32>,
            offset: 0,
            length: totalRows,
            nullCount,
            data: indices,
            nullBitmap: nullCount > 0 ? nullBitmap : undefined,
            dictionary: makeVector(
              makeData({
                type: new Utf8(),
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
      const dict = textDicts.get(fieldName);
      const originalToMasked = textOriginalToMasked.get(fieldName);
      if (!dict || !originalToMasked) {
        throw new Error(`Text dictionary or mapping not found for field: ${fieldName}`);
      }
      const indices = new Uint32Array(totalRows);
      const nullBitmap = new Uint8Array(Math.ceil(totalRows / 8));
      let nullCount = 0;
      let rowOffset = 0;

      for (const buf of buffers) {
        const col = buf.getColumnIfAllocated(columnName) as string[] | undefined;
        if (col) {
          for (let i = 0; i < buf.writeIndex; i++) {
            const v = col[i];
            const rowIdx = rowOffset + i;
            if (v != null) {
              // Look up the masked value, then find its index in the dictionary
              const maskedValue = originalToMasked.get(v) ?? v;
              indices[rowIdx] = dict.indexMap.get(maskedValue) ?? 0;
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
        makeVector(
          makeData({
            type: fieldType as Dictionary<Utf8, Uint32>,
            offset: 0,
            length: totalRows,
            nullCount,
            data: indices,
            nullBitmap: nullCount > 0 ? nullBitmap : undefined,
            dictionary: makeVector(
              makeData({
                type: new Utf8(),
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
        const col = buf.getColumnIfAllocated(columnName);
        const srcNulls = buf.getNullsIfAllocated(columnName);

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
        makeVector(
          makeData({
            type: fieldType as Float64,
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
        const col = buf.getColumnIfAllocated(columnName);
        const srcNulls = buf.getNullsIfAllocated(columnName);

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
        makeVector(
          makeData({
            type: fieldType as Bool,
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
        const col = buf.getColumnIfAllocated(columnName);
        const srcNulls = buf.getNullsIfAllocated(columnName);

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

      const enumDictData = makeData({
        type: new Utf8(),
        offset: 0,
        length: enumValues.length,
        nullCount: 0,
        valueOffsets: enumUtf8?.offsets ?? calculateUtf8Offsets(enumValues as readonly string[]),
        data: enumUtf8?.concatenated ?? encodeUtf8Strings(enumValues as readonly string[]),
      });

      vectors.push(
        makeVector(
          makeData({
            type: fieldType as Dictionary<Utf8, Uint8>,
            offset: 0,
            length: totalRows,
            nullCount,
            data: allIndices,
            nullBitmap: nullCount > 0 ? nullBitmap : undefined,
            dictionary: makeVector(enumDictData),
          }),
        ),
      );
    }

    fieldIdx++;
  }

  const structData = makeData({
    type: new Struct(arrowSchema.fields),
    length: totalRows,
    nullCount: 0,
    children: vectors.map((v) => v.data[0]),
  });

  return new RecordBatch(arrowSchema, structData);
}
