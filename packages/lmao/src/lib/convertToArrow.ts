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
  createSortedDictionary,
  DictionaryBuilder,
  type FinalizedDictionary,
  getMaskTransform,
  type PreEncodedEntry,
  sortInPlace,
} from '@smoothbricks/arrow-builder';
import {
  Bool,
  Dictionary,
  Field,
  Float64,
  Int8,
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
import { type CapacityStatsEntry, createCapacityStatsRecordBatch } from './arrow/capacityStats.js';
import { buildSortedCategoryDictionary, buildTextDictionary } from './arrow/dictionaries.js';
import {
  calculateUtf8Offsets,
  concatenateFloat64Arrays,
  concatenateNullBitmaps,
  concatenateUint8Arrays,
  encodeUtf8Strings,
  getArrowFieldName,
  walkSpanTree,
} from './arrow/utils.js';
import { ENTRY_TYPE_NAMES, SYSTEM_SCHEMA_FIELD_NAMES } from './schema/systemSchema.js';
import { getEnumUtf8, getEnumValues, getSchemaType } from './schema/typeGuards.js';
import type { LogSchema } from './schema/types.js';
import type { AnySpanBuffer, OpMetadata } from './types.js';
import { globalUtf8Cache } from './utf8Cache.js';

export type SystemColumnBuilder = (
  buffer: AnySpanBuffer,
  buffers: AnySpanBuffer[],
  totalRows: number,
) => { fields: Field[]; vectors: Vector[] };

/**
 * Convert SpanBuffer (and its overflow chain) to Arrow RecordBatch.
 *
 * **Dictionary handling**: Each RecordBatch has its own dictionary data.
 * Dictionaries are built from the data in this RecordBatch only and are not shared
 * with other RecordBatches, even when combined into a Table.
 */
export function convertToRecordBatch(buffer: AnySpanBuffer, systemColumnBuilder?: SystemColumnBuilder): RecordBatch {
  const buffers: AnySpanBuffer[] = [];
  let currentBuffer: AnySpanBuffer | undefined = buffer;

  while (currentBuffer) {
    buffers.push(currentBuffer);
    currentBuffer = currentBuffer._overflow;
  }

  return convertBuffersToRecordBatch(buffers, systemColumnBuilder);
}

/**
 * Convert multiple buffers to a single RecordBatch.
 *
 * **Dictionary handling**: Each RecordBatch has its own dictionary data.
 * Dictionaries are built from the data in this RecordBatch only and are not shared
 * with other RecordBatches, even when combined into a Table.
 */
function convertBuffersToRecordBatch(buffers: AnySpanBuffer[], systemColumnBuilder?: SystemColumnBuilder): RecordBatch {
  if (buffers.length === 0) return new RecordBatch({});

  const totalRows = buffers.reduce((sum, buf) => sum + buf._writeIndex, 0);
  if (totalRows === 0) return new RecordBatch({});

  const schema: LogSchema = buffers[0]._logSchema;

  // Build vectors first, then derive schema from them
  // This ensures Field types and vector data types are identical (Arrow IPC requirement)
  const fields: Field[] = [];
  const vectors: Vector[] = [];

  if (systemColumnBuilder) {
    const systemColumns = systemColumnBuilder(buffers[0], buffers, totalRows);
    fields.push(...systemColumns.fields);
    vectors.push(...systemColumns.vectors);
  } else {
    // Build metadata columns - returns both fields and vectors with matching types
    const metadataResult = buildMetadataColumnsWithFields(buffers, totalRows);
    fields.push(...metadataResult.fields);
    vectors.push(...metadataResult.vectors);

    // System attribute column: message (eager category)
    const maskTransform = undefined; // message has no masking
    let { dictionary, indices, arrowIndexTypeCtor, nullBitmap } = buildSortedCategoryDictionary(
      buffers,
      'message',
      maskTransform,
    );
    // Ensure dictionary has at least one entry (empty string) if no messages were written
    if (dictionary.length === 0) {
      dictionary = [''];
    }
    // Ensure nullBitmap is all 1s (all valid) for eager column
    if (!nullBitmap) {
      const bitmapBytes = Math.ceil(totalRows / 8);
      nullBitmap = new Uint8Array(bitmapBytes);
      nullBitmap.fill(0xff);
    }

    // CRITICAL: Create ONE Dictionary type instance for both Field AND makeData
    // Arrow Schema validates that fields with same dictionary ID have identical type references
    const messageDictType = new Dictionary(new Utf8(), new arrowIndexTypeCtor());
    fields.push(Field.new({ name: 'message', type: messageDictType }));

    const { data: messageUtf8Data, offsets: messageUtf8Offsets } = globalUtf8Cache.encodeMany(dictionary);

    const messageDictData = makeData({
      type: new Utf8(),
      offset: 0,
      length: dictionary.length,
      nullCount: 0,
      valueOffsets: messageUtf8Offsets,
      data: messageUtf8Data,
    });

    const messageData = makeData({
      type: messageDictType,
      offset: 0,
      length: totalRows,
      nullCount: 0, // message is eager, never null
      data: indices,
      nullBitmap, // Always include nullBitmap for message (even though nullCount is 0)
      dictionary: makeVector(messageDictData),
    });

    vectors.push(makeVector(messageData));
  }

  // Build user attribute vectors
  // Skip system schema fields - they are handled separately as system columns
  // (message is handled above, line/error_code/exception_stack/ff_value/uint64_value below)
  const userSchemaFields = schema._columns.filter(([fieldName]) => !SYSTEM_SCHEMA_FIELD_NAMES.has(fieldName));
  for (const [fieldName, fieldSchema] of userSchemaFields) {
    const lmaoType = getSchemaType(fieldSchema);
    const arrowFieldName = getArrowFieldName(fieldName);
    const columnName = fieldName; // User columns have no prefix

    if (lmaoType === 'enum') {
      const enumValues = getEnumValues(fieldSchema) || [];
      const enumUtf8 = getEnumUtf8(fieldSchema);
      // Get constructors from schema metadata
      const enumSchema = fieldSchema as { __index_array_ctor?: unknown; __arrow_index_type_ctor?: unknown };
      const indexArrayCtor =
        (enumSchema.__index_array_ctor as Uint8ArrayConstructor | Uint16ArrayConstructor | Uint32ArrayConstructor) ??
        Uint8Array;
      const arrowIndexTypeCtor =
        (enumSchema.__arrow_index_type_ctor as typeof Uint8 | typeof Uint16 | typeof Uint32) ?? Uint8;
      // Collect value arrays - need to handle different TypedArray types
      const valueArrays: (Uint8Array | Uint16Array | Uint32Array)[] = [];
      for (const buf of buffers) {
        const column = buf.getColumnIfAllocated(columnName);
        if (column && column instanceof indexArrayCtor) {
          valueArrays.push(column.subarray(0, buf._writeIndex));
        } else {
          valueArrays.push(new indexArrayCtor(buf._writeIndex));
        }
      }

      // Concatenate arrays based on type
      let allIndices: Uint8Array | Uint16Array | Uint32Array;
      if (indexArrayCtor === Uint8Array) {
        allIndices = concatenateUint8Arrays(valueArrays as Uint8Array[]);
      } else {
        const totalLength = valueArrays.reduce((sum, arr) => sum + arr.length, 0);
        allIndices = new indexArrayCtor(totalLength);
        let offset = 0;
        for (const arr of valueArrays) {
          allIndices.set(arr, offset);
          offset += arr.length;
        }
      }

      const { nullBitmap, nullCount } = concatenateNullBitmaps(buffers, columnName);

      // CRITICAL: Create ONE Dictionary type instance for both Field AND makeData
      const enumDictType = new Dictionary(new Utf8(), new arrowIndexTypeCtor());
      fields.push(Field.new({ name: arrowFieldName, type: enumDictType, nullable: true }));

      const enumDictData = makeData({
        type: new Utf8(),
        offset: 0,
        length: enumValues.length,
        nullCount: 0,
        valueOffsets: enumUtf8?.offsets ?? calculateUtf8Offsets(enumValues as readonly string[]),
        data: enumUtf8?.concatenated ?? encodeUtf8Strings(enumValues as readonly string[]),
      });

      const enumData = makeData({
        type: enumDictType,
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
      const { dictionary, indices, arrowIndexTypeCtor, nullBitmap, nullCount } = buildSortedCategoryDictionary(
        buffers,
        columnName,
        maskTransform,
      );

      // CRITICAL: Create ONE Dictionary type instance for both Field AND makeData
      const categoryDictType = new Dictionary(new Utf8(), new arrowIndexTypeCtor());
      fields.push(Field.new({ name: arrowFieldName, type: categoryDictType, nullable: true }));

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
        type: categoryDictType,
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
        const { dictionary, indices, arrowIndexTypeCtor, nullBitmap, nullCount } = result;

        // CRITICAL: Create ONE Dictionary type instance for both Field AND makeData
        const textDictType = new Dictionary(new Utf8(), new arrowIndexTypeCtor());
        fields.push(Field.new({ name: arrowFieldName, type: textDictType, nullable: true }));

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
          type: textDictType,
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
      const valueArrays: Float64Array[] = [];

      for (const buf of buffers) {
        const column = buf.getColumnIfAllocated(columnName);
        if (column && column instanceof Float64Array) {
          valueArrays.push(column.subarray(0, buf._writeIndex));
        } else {
          valueArrays.push(new Float64Array(buf._writeIndex));
        }
      }

      const allValues = concatenateFloat64Arrays(valueArrays);
      const { nullBitmap, nullCount } = concatenateNullBitmaps(buffers, columnName);

      const numberType = new Float64();
      fields.push(Field.new({ name: arrowFieldName, type: numberType, nullable: true }));

      const numberData = makeData({
        type: numberType,
        offset: 0,
        length: totalRows,
        nullCount,
        data: allValues,
        nullBitmap,
      });

      vectors.push(makeVector(numberData));
    } else if (lmaoType === 'boolean') {
      const valueArrays: Uint8Array[] = [];

      for (const buf of buffers) {
        const column = buf.getColumnIfAllocated(columnName);
        if (column && column instanceof Uint8Array) {
          const requiredBytes = Math.ceil(buf._writeIndex / 8);
          valueArrays.push(column.subarray(0, requiredBytes));
        } else {
          const requiredBytes = Math.ceil(buf._writeIndex / 8);
          valueArrays.push(new Uint8Array(requiredBytes));
        }
      }

      const allValues = concatenateUint8Arrays(valueArrays);
      const { nullBitmap, nullCount } = concatenateNullBitmaps(buffers, columnName);

      const boolType = new Bool();
      fields.push(Field.new({ name: arrowFieldName, type: boolType, nullable: true }));

      const boolData = makeData({
        type: boolType,
        offset: 0,
        length: totalRows,
        nullCount,
        data: allValues,
        nullBitmap,
      });

      vectors.push(makeVector(boolData));
    }
  }

  const arrowSchema = new Schema(fields);
  const data = makeData({
    type: new Struct(arrowSchema.fields),
    length: totalRows,
    nullCount: 0,
    children: vectors.map((v) => v.data[0]),
  });

  return new RecordBatch(arrowSchema, data);
}

/**
 * Build metadata columns with matching Field definitions.
 * Returns both fields and vectors with identical Dictionary type instances.
 * This is required for Arrow IPC serialization - Schema validates that
 * fields with same dictionary ID have identical type object references.
 */
function buildMetadataColumnsWithFields(
  buffers: AnySpanBuffer[],
  totalRows: number,
): { fields: Field[]; vectors: Vector[] } {
  const fields: Field[] = [];
  const vectors: Vector[] = [];

  // Core system column: Timestamp
  const timestampType = new TimestampNanosecond();
  fields.push(Field.new({ name: 'timestamp', type: timestampType }));

  const allTimestamps = new BigInt64Array(totalRows);
  let timestampOffset = 0;
  for (const buf of buffers) {
    if (!buf.timestamp) {
      throw new Error(`Buffer missing timestamps property (_writeIndex: ${buf._writeIndex})`);
    }
    allTimestamps.set(buf.timestamp.subarray(0, buf._writeIndex), timestampOffset);
    timestampOffset += buf._writeIndex;
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

  // Trace ID
  const traceIdSet = new Set<string>();
  for (const buf of buffers) traceIdSet.add(buf.trace_id);
  const traceIdArray = Array.from(traceIdSet);
  const traceIdMap = new Map(traceIdArray.map((id, idx) => [id, idx]));
  const traceIdUniqueCount = traceIdArray.length;
  const traceIdIndexArrayCtor =
    traceIdUniqueCount <= 255 ? Uint8Array : traceIdUniqueCount <= 65535 ? Uint16Array : Uint32Array;
  const traceIdArrowIndexTypeCtor = traceIdUniqueCount <= 255 ? Uint8 : traceIdUniqueCount <= 65535 ? Uint16 : Uint32;

  const traceIdIndices = new traceIdIndexArrayCtor(totalRows);
  let rowOffset = 0;
  for (const buf of buffers) {
    const traceIdIndex = traceIdMap.get(buf.trace_id);
    if (traceIdIndex === undefined) {
      throw new Error(`TraceId index not found for: ${buf.trace_id}`);
    }
    traceIdIndices.fill(traceIdIndex, rowOffset, rowOffset + buf._writeIndex);
    rowOffset += buf._writeIndex;
  }

  const traceIdDictType = new Dictionary(new Utf8(), new traceIdArrowIndexTypeCtor());
  fields.push(Field.new({ name: 'trace_id', type: traceIdDictType }));

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
        type: traceIdDictType,
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: traceIdIndices,
        dictionary: makeVector(traceIdDictData),
      }),
    ),
  );

  // thread_id
  const threadIdType = new Uint64();
  fields.push(Field.new({ name: 'thread_id', type: threadIdType }));
  const threadIds = new BigUint64Array(totalRows);
  rowOffset = 0;
  for (const buf of buffers) {
    threadIds.fill(buf.thread_id, rowOffset, rowOffset + buf._writeIndex);
    rowOffset += buf._writeIndex;
  }
  vectors.push(
    makeVector(makeData({ type: threadIdType, offset: 0, length: totalRows, nullCount: 0, data: threadIds })),
  );

  // span_id
  const spanIdType = new Uint32();
  fields.push(Field.new({ name: 'span_id', type: spanIdType }));
  const spanIds = new Uint32Array(totalRows);
  rowOffset = 0;
  for (const buf of buffers) {
    spanIds.fill(buf.span_id, rowOffset, rowOffset + buf._writeIndex);
    rowOffset += buf._writeIndex;
  }
  vectors.push(makeVector(makeData({ type: spanIdType, offset: 0, length: totalRows, nullCount: 0, data: spanIds })));

  // parent_thread_id (nullable)
  const parentThreadIdType = new Uint64();
  fields.push(Field.new({ name: 'parent_thread_id', type: parentThreadIdType, nullable: true }));
  const parentThreadIds = new BigUint64Array(totalRows);
  const parentThreadIdNulls = new Uint8Array(Math.ceil(totalRows / 8));
  parentThreadIdNulls.fill(0xff);
  let parentThreadIdNullCount = 0;
  rowOffset = 0;
  for (const buf of buffers) {
    if (buf._parent) {
      parentThreadIds.fill(buf.parent_thread_id, rowOffset, rowOffset + buf._writeIndex);
    } else {
      clearBitRange(parentThreadIdNulls, rowOffset, buf._writeIndex);
      parentThreadIdNullCount += buf._writeIndex;
    }
    rowOffset += buf._writeIndex;
  }
  vectors.push(
    makeVector(
      makeData({
        type: parentThreadIdType,
        offset: 0,
        length: totalRows,
        nullCount: parentThreadIdNullCount,
        data: parentThreadIds,
        nullBitmap: parentThreadIdNullCount > 0 ? parentThreadIdNulls : undefined,
      }),
    ),
  );

  // parent_span_id (nullable)
  const parentSpanIdType = new Uint32();
  fields.push(Field.new({ name: 'parent_span_id', type: parentSpanIdType, nullable: true }));
  const parentSpanIds = new Uint32Array(totalRows);
  const parentSpanIdNulls = new Uint8Array(Math.ceil(totalRows / 8));
  parentSpanIdNulls.fill(0xff);
  let parentSpanIdNullCount = 0;
  rowOffset = 0;
  for (const buf of buffers) {
    if (buf._hasParent) {
      parentSpanIds.fill(buf.parent_span_id, rowOffset, rowOffset + buf._writeIndex);
    } else {
      clearBitRange(parentSpanIdNulls, rowOffset, buf._writeIndex);
      parentSpanIdNullCount += buf._writeIndex;
    }
    rowOffset += buf._writeIndex;
  }
  vectors.push(
    makeVector(
      makeData({
        type: parentSpanIdType,
        offset: 0,
        length: totalRows,
        nullCount: parentSpanIdNullCount,
        data: parentSpanIds,
        nullBitmap: parentSpanIdNullCount > 0 ? parentSpanIdNulls : undefined,
      }),
    ),
  );

  // Entry type
  const entryTypeDictType = new Dictionary(new Utf8(), new Int8());
  fields.push(Field.new({ name: 'entry_type', type: entryTypeDictType }));
  const entryTypeIndices = new Int8Array(totalRows);
  rowOffset = 0;
  for (const buf of buffers) {
    if (!buf.entry_type) {
      throw new Error(`Buffer missing operations property (_writeIndex: ${buf._writeIndex})`);
    }
    entryTypeIndices.set(buf.entry_type.subarray(0, buf._writeIndex), rowOffset);
    rowOffset += buf._writeIndex;
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
        type: entryTypeDictType,
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: entryTypeIndices,
        dictionary: makeVector(entryTypeDictData),
      }),
    ),
  );

  // Package name
  const packageSet = new Set<string>();
  for (const buf of buffers) {
    packageSet.add(buf._opMetadata.package_name);
    if (buf._callsiteMetadata) packageSet.add(buf._callsiteMetadata.package_name);
  }
  const packageArray = Array.from(packageSet);
  const packageMap = new Map(packageArray.map((name, idx) => [name, idx]));
  const packageUniqueCount = packageArray.length;
  const packageIndexArrayCtor =
    packageUniqueCount <= 255 ? Uint8Array : packageUniqueCount <= 65535 ? Uint16Array : Uint32Array;
  const packageArrowIndexTypeCtor = packageUniqueCount <= 255 ? Uint8 : packageUniqueCount <= 65535 ? Uint16 : Uint32;

  const packageDictType = new Dictionary(new Utf8(), new packageArrowIndexTypeCtor());
  fields.push(Field.new({ name: 'package_name', type: packageDictType }));

  const packageIndices = new packageIndexArrayCtor(totalRows);
  rowOffset = 0;
  for (const buf of buffers) {
    const callsitePkgName = buf._callsiteMetadata?.package_name ?? buf._opMetadata.package_name;
    const callsitePkgIdx = packageMap.get(callsitePkgName) ?? 0;
    const opPkgIdx = packageMap.get(buf._opMetadata.package_name) ?? 0;
    packageIndices[rowOffset] = callsitePkgIdx;
    if (buf._writeIndex > 1) {
      packageIndices.fill(opPkgIdx, rowOffset + 1, rowOffset + buf._writeIndex);
    }
    rowOffset += buf._writeIndex;
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
        type: packageDictType,
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: packageIndices,
        dictionary: makeVector(packageDictData),
      }),
    ),
  );

  // Package file
  const modulePathSet = new Set<string>();
  for (const buf of buffers) {
    modulePathSet.add(buf._opMetadata.package_file);
    if (buf._callsiteMetadata) modulePathSet.add(buf._callsiteMetadata.package_file);
  }
  const modulePathArray = Array.from(modulePathSet);
  const modulePathMap = new Map(modulePathArray.map((name, idx) => [name, idx]));
  const packagePathUniqueCount = modulePathArray.length;
  const packagePathIndexArrayCtor =
    packagePathUniqueCount <= 255 ? Uint8Array : packagePathUniqueCount <= 65535 ? Uint16Array : Uint32Array;
  const packagePathArrowIndexTypeCtor =
    packagePathUniqueCount <= 255 ? Uint8 : packagePathUniqueCount <= 65535 ? Uint16 : Uint32;

  const packagePathDictType = new Dictionary(new Utf8(), new packagePathArrowIndexTypeCtor());
  fields.push(Field.new({ name: 'package_file', type: packagePathDictType }));

  const packagePathIndices = new packagePathIndexArrayCtor(totalRows);
  rowOffset = 0;
  for (const buf of buffers) {
    const callsitePathName = buf._callsiteMetadata?.package_file ?? buf._opMetadata.package_file;
    const callsitePathIdx = modulePathMap.get(callsitePathName) ?? 0;
    const opPathIdx = modulePathMap.get(buf._opMetadata.package_file) ?? 0;
    packagePathIndices[rowOffset] = callsitePathIdx;
    if (buf._writeIndex > 1) {
      packagePathIndices.fill(opPathIdx, rowOffset + 1, rowOffset + buf._writeIndex);
    }
    rowOffset += buf._writeIndex;
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
        type: packagePathDictType,
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: packagePathIndices,
        dictionary: makeVector(packagePathDictData),
      }),
    ),
  );

  // Git SHA
  const gitShaSet = new Set<string>();
  for (const buf of buffers) {
    gitShaSet.add(buf._opMetadata.git_sha);
    if (buf._callsiteMetadata) gitShaSet.add(buf._callsiteMetadata.git_sha);
  }
  const gitShaArray = Array.from(gitShaSet);
  const gitShaMap = new Map(gitShaArray.map((sha, idx) => [sha, idx]));
  const gitShaUniqueCount = gitShaArray.length;
  const gitShaIndexArrayCtor =
    gitShaUniqueCount <= 255 ? Uint8Array : gitShaUniqueCount <= 65535 ? Uint16Array : Uint32Array;
  const gitShaArrowIndexTypeCtor = gitShaUniqueCount <= 255 ? Uint8 : gitShaUniqueCount <= 65535 ? Uint16 : Uint32;

  const gitShaDictType = new Dictionary(new Utf8(), new gitShaArrowIndexTypeCtor());
  fields.push(Field.new({ name: 'git_sha', type: gitShaDictType }));

  const gitShaIndices = new gitShaIndexArrayCtor(totalRows);
  rowOffset = 0;
  for (const buf of buffers) {
    const callsiteGitSha = buf._callsiteMetadata?.git_sha ?? buf._opMetadata.git_sha;
    const callsiteGitShaIdx = gitShaMap.get(callsiteGitSha) ?? 0;
    const opGitShaIdx = gitShaMap.get(buf._opMetadata.git_sha) ?? 0;
    gitShaIndices[rowOffset] = callsiteGitShaIdx;
    if (buf._writeIndex > 1) {
      gitShaIndices.fill(opGitShaIdx, rowOffset + 1, rowOffset + buf._writeIndex);
    }
    rowOffset += buf._writeIndex;
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
        type: gitShaDictType,
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: gitShaIndices,
        dictionary: makeVector(gitShaDictData),
      }),
    ),
  );

  return { fields, vectors };
}

/**
 * Convert SpanBuffer (and its overflow chain) to Arrow Table
 */
export function convertToArrowTable<T extends LogSchema = LogSchema>(
  buffer: AnySpanBuffer,
  systemColumnBuilder?: SystemColumnBuilder,
): Table {
  const batch = convertToRecordBatch(buffer, systemColumnBuilder);
  if (batch.numRows === 0) return new Table();
  return new Table([batch]);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tree Conversion with Shared Dictionaries
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Convert SpanBuffer tree to Arrow Table
 *
 * Two-pass conversion with shared dictionaries:
 * - Pass 1: Walk tree, build dictionaries (collect unique strings, cache UTF-8)
 * - Pass 2: Walk tree, convert each buffer to RecordBatch using shared dictionaries
 */
export function convertSpanTreeToArrowTable(
  rootBuffer: AnySpanBuffer,
  _systemColumnBuilder?: SystemColumnBuilder,
  modulesToLogStats?: CapacityStatsEntry[],
  periodStartNs?: bigint,
): Table {
  // ═══════════════════════════════════════════════════════════════════════════
  // PASS 0: Collect ALL unique schema fields from ALL buffers in the tree
  // Per specs/01k_tree_walker_and_arrow_conversion.md - child spans may have different schemas
  // (e.g., library prefixed schemas like db_query, http_status, etc.)
  // ═══════════════════════════════════════════════════════════════════════════
  const mergedSchemaFields = new Map<string, unknown>();

  walkSpanTree(rootBuffer, (buffer) => {
    // Use buffer._columns directly - works for both SpanBuffer and RemappedBufferView
    const fields = buffer._columns;
    for (const [fieldName, fieldSchema] of fields) {
      // Skip system schema fields - they are handled separately as system columns
      // (message, line, error_code, exception_stack, ff_value, uint64_value)
      if (SYSTEM_SCHEMA_FIELD_NAMES.has(fieldName)) {
        continue;
      }
      // Only add if not already present (first buffer with this field wins)
      if (!mergedSchemaFields.has(fieldName)) {
        mergedSchemaFields.set(fieldName, fieldSchema);
      }
    }
  });

  // Convert merged schema fields to array format for iteration
  const schemaFields: Array<[string, unknown]> = Array.from(mergedSchemaFields.entries());

  // Create merged schema object from collected fields (for capacity stats and other uses)
  const mergedSchema = Object.fromEntries(mergedSchemaFields) as Record<string, unknown>;

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

  // Use cached schemaFields to filter out methods (extend, validate, parse, safeParse)
  for (const [fieldName, fieldSchema] of schemaFields) {
    const lmaoType = getSchemaType(fieldSchema);
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

  // System attribute column: message (eager category, not in user schema)
  // message is always present (eager allocation), so we need to handle it explicitly
  categoryBuilders.set('message', new DictionaryBuilder(globalUtf8Cache));
  categoryMaskTransforms.set('message', undefined); // message has no masking
  categoryOriginalToMasked.set('message', new Map());

  // Metadata column dictionaries
  // For traceId (metadata), use DictionaryBuilder (values not pre-encoded)
  const traceIdBuilder = new DictionaryBuilder(globalUtf8Cache);

  // For package, modulePath, gitSha - collect pre-encoded entries for direct dictionary creation
  // Use a Set to deduplicate by OpMetadata identity (same module = same entries)
  const uniqueModules = new Set<OpMetadata>();

  let spanRows = 0;
  walkSpanTree(rootBuffer, (buffer) => {
    spanRows += buffer._writeIndex;
    traceIdBuilder.add(buffer.trace_id);

    // Collect unique OpMetadata (for package_name, package_file, git_sha)
    // Include both buffer._opMetadata (for rows 1+) and callsiteMetadata (for row 0) if present
    uniqueModules.add(buffer._opMetadata);
    if (buffer._callsiteMetadata) {
      uniqueModules.add(buffer._callsiteMetadata);
    }

    for (const [fieldName, builder] of categoryBuilders) {
      const col = buffer.getColumnIfAllocated(fieldName) as string[] | undefined;
      const maskTransform = categoryMaskTransforms.get(fieldName);
      const originalToMasked = categoryOriginalToMasked.get(fieldName);
      if (!originalToMasked) {
        throw new Error(`Category originalToMasked map not found for field: ${fieldName}`);
      }
      if (col) {
        for (let i = 0; i < buffer._writeIndex; i++) {
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
      // Also add scope values to dictionary (they may not be in columns)
      // Per specs/01i_span_scope_attributes.md: scope values fill NULL cells at Arrow conversion
      const scopeValue = buffer._scopeValues?.[fieldName] as string | undefined;
      if (scopeValue !== undefined) {
        let maskedScopeValue = originalToMasked.get(scopeValue);
        if (maskedScopeValue === undefined) {
          maskedScopeValue = maskTransform ? maskTransform(scopeValue) : scopeValue;
          originalToMasked.set(scopeValue, maskedScopeValue);
        }
        builder.add(maskedScopeValue);
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
        for (let i = 0; i < buffer._writeIndex; i++) {
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
      // Also add scope values to dictionary (they may not be in columns)
      // Per specs/01i_span_scope_attributes.md: scope values fill NULL cells at Arrow conversion
      const scopeValue = buffer._scopeValues?.[fieldName] as string | undefined;
      if (scopeValue !== undefined) {
        let maskedScopeValue = originalToMasked.get(scopeValue);
        if (maskedScopeValue === undefined) {
          maskedScopeValue = maskTransform ? maskTransform(scopeValue) : scopeValue;
          originalToMasked.set(scopeValue, maskedScopeValue);
        }
        builder.add(maskedScopeValue);
      }
      // Scope values are now pre-filled in columns at allocation time
      // (via lazy getters and _prefillScopedAttributes on overflow),
      // so they'll be added to dictionary via the column iteration above
    }
  });

  if (spanRows === 0 && (!modulesToLogStats || modulesToLogStats.length === 0)) {
    return new Table();
  }

  // Finalize dictionaries
  const traceIdDict = traceIdBuilder.finalize(false);

  // Build dictionaries from OpMetadata entries
  // Use pre-encoded entries from OpMetadata (interned at Op definition time)
  const moduleCount = uniqueModules.size;
  const packageEntries: PreEncodedEntry[] = new Array(moduleCount);
  const packagePathEntries: PreEncodedEntry[] = new Array(moduleCount);
  const gitShaEntries: PreEncodedEntry[] = new Array(moduleCount);
  let idx = 0;
  for (const m of uniqueModules) {
    packageEntries[idx] = m.package_name_entry;
    packagePathEntries[idx] = m.package_file_entry;
    gitShaEntries[idx] = m.git_sha_entry;
    idx++;
  }

  const cmp = (a: PreEncodedEntry, b: PreEncodedEntry) => compareStrings(a.str, b.str);
  const packageDict = createSortedDictionary(sortInPlace(packageEntries, cmp));
  const packagePathDict = createSortedDictionary(sortInPlace(packagePathEntries, cmp));
  const gitShaDict = createSortedDictionary(sortInPlace(gitShaEntries, cmp));
  const categoryDicts = new Map<string, FinalizedDictionary>();
  const textDicts = new Map<string, FinalizedDictionary>();

  for (const [name, builder] of categoryBuilders) {
    categoryDicts.set(name, builder.finalize(true)); // sorted for binary search
  }
  for (const [name, builder] of textBuilders) {
    textDicts.set(name, builder.finalize(false)); // not sorted
  }

  // Get messageDict from categoryDicts (message is handled as a category column)
  // If no messages were written, create an empty dictionary
  let messageDict = categoryDicts.get('message');
  if (!messageDict) {
    const messageBuilder = new DictionaryBuilder(globalUtf8Cache);
    messageBuilder.add(''); // Single empty string entry
    messageDict = messageBuilder.finalize(true); // sorted
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Build shared Arrow schema with stable dictionary IDs
  // ═══════════════════════════════════════════════════════════════════════════
  const arrowFields: Field[] = [];

  // Core system columns and metadata columns - create types with explicit dictionary IDs
  // Use dynamically determined index types from dictionaries (Uint8/Uint16/Uint32)

  // Core system column: timestamp (from _system ArrayBuffer)
  arrowFields.push(Field.new({ name: 'timestamp', type: new TimestampNanosecond() }));

  // Metadata columns (computed from buffer properties):
  arrowFields.push(
    Field.new({ name: 'trace_id', type: new Dictionary(new Utf8(), new traceIdDict.arrowIndexTypeCtor(), 0) }),
  );
  // Span ID columns (separate columns instead of struct)
  arrowFields.push(Field.new({ name: 'thread_id', type: new Uint64() }));
  arrowFields.push(Field.new({ name: 'span_id', type: new Uint32() }));
  arrowFields.push(Field.new({ name: 'parent_thread_id', type: new Uint64(), nullable: true }));
  arrowFields.push(Field.new({ name: 'parent_span_id', type: new Uint32(), nullable: true }));

  // Core system column: entry_type (from _system ArrayBuffer)
  arrowFields.push(Field.new({ name: 'entry_type', type: new Dictionary(new Utf8(), new Int8(), 1) }));

  // Metadata columns (from task._module):
  arrowFields.push(
    Field.new({ name: 'package_name', type: new Dictionary(new Utf8(), new packageDict.arrowIndexTypeCtor(), 2) }),
  );
  arrowFields.push(
    Field.new({ name: 'package_file', type: new Dictionary(new Utf8(), new packagePathDict.arrowIndexTypeCtor(), 3) }),
  );
  arrowFields.push(
    Field.new({ name: 'git_sha', type: new Dictionary(new Utf8(), new gitShaDict.arrowIndexTypeCtor(), 4) }),
  );

  // System attribute column: message (handled as category column, but positioned here in schema)
  arrowFields.push(
    Field.new({ name: 'message', type: new Dictionary(new Utf8(), new messageDict.arrowIndexTypeCtor(), 5) }),
  );

  // System attribute column: uint64_value (for buffer metrics - op durations, counts, etc.)
  arrowFields.push(Field.new({ name: 'uint64_value', type: new Uint64(), nullable: true }));

  // User attribute columns - assign dictionary IDs starting at 6
  let nextDictId = 6;
  const attrDictIds = new Map<string, number>();
  // Use cached schemaFields to filter out methods (extend, validate, parse, safeParse)
  for (const [fieldName, fieldSchema] of schemaFields) {
    const lmaoType = getSchemaType(fieldSchema);
    const arrowFieldName = getArrowFieldName(fieldName);

    if (lmaoType === 'enum') {
      // Get constructors from schema metadata
      const enumSchema = fieldSchema as { __arrow_index_type_ctor?: unknown };
      const arrowIndexTypeCtor =
        (enumSchema.__arrow_index_type_ctor as typeof Uint8 | typeof Uint16 | typeof Uint32) ?? Uint8;
      attrDictIds.set(fieldName, nextDictId);
      arrowFields.push(
        Field.new({
          name: arrowFieldName,
          type: new Dictionary(new Utf8(), new arrowIndexTypeCtor(), nextDictId++),
          nullable: true,
        }),
      );
    } else if (lmaoType === 'category') {
      const dict = categoryDicts.get(fieldName);
      if (!dict) {
        throw new Error(`Category dictionary not found for field: ${fieldName}`);
      }
      attrDictIds.set(fieldName, nextDictId);
      arrowFields.push(
        Field.new({
          name: arrowFieldName,
          type: new Dictionary(new Utf8(), new dict.arrowIndexTypeCtor(), nextDictId++),
          nullable: true,
        }),
      );
    } else if (lmaoType === 'text') {
      const dict = textDicts.get(fieldName);
      if (!dict) {
        throw new Error(`Text dictionary not found for field: ${fieldName}`);
      }
      attrDictIds.set(fieldName, nextDictId);
      arrowFields.push(
        Field.new({
          name: arrowFieldName,
          type: new Dictionary(new Utf8(), new dict.arrowIndexTypeCtor(), nextDictId++),
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
  const allBuffers: AnySpanBuffer[] = [];

  walkSpanTree(rootBuffer, (buffer) => {
    if (buffer._writeIndex > 0) {
      allBuffers.push(buffer);
    }
  });

  // Build a single RecordBatch from all buffers
  const batch =
    allBuffers.length > 0
      ? convertBuffersWithSharedDicts(
          allBuffers,
          arrowSchema,
          traceIdDict,
          packageDict,
          packagePathDict,
          gitShaDict,
          categoryDicts,
          textDicts,
          attrDictIds,
          mergedSchema,
          schemaFields,
          categoryOriginalToMasked,
          textOriginalToMasked,
        )
      : undefined;

  // Create capacity stats RecordBatch if needed
  if (modulesToLogStats && modulesToLogStats.length > 0) {
    const hasSpanData = batch !== undefined;
    // periodStartNs must be provided by the caller (from FlushScheduler's tracked period start)
    // If not provided, use 0n as a fallback (indicates period tracking not enabled)
    const effectivePeriodStartNs = periodStartNs ?? 0n;
    const capacityStatsBatch = createCapacityStatsRecordBatch(
      modulesToLogStats,
      effectivePeriodStartNs,
      arrowSchema,
      mergedSchema,
      hasSpanData,
    );
    if (batch) {
      // Use the first batch's schema as the Table schema (Apache Arrow requirement)
      // The capacity stats batch schema should match, but if it doesn't, use batch's schema
      return new Table(batch.schema, [batch, capacityStatsBatch]);
    }
    return new Table([capacityStatsBatch]);
  }

  if (batch) {
    return new Table([batch]);
  }
  return new Table();
}

/**
 * Convert multiple buffers to a single RecordBatch using pre-built dictionaries.
 *
 * **Dictionary handling**: Each RecordBatch has its own dictionary data.
 * While dictionaries are built in a first pass from all buffers, they are used to create
 * a single RecordBatch. The resulting RecordBatch contains its own dictionary data and
 * is not shared with other RecordBatches, even when combined into a Table.
 */
function convertBuffersWithSharedDicts(
  buffers: AnySpanBuffer[],
  arrowSchema: Schema,
  traceIdDict: FinalizedDictionary,
  packageDict: FinalizedDictionary,
  packagePathDict: FinalizedDictionary,
  gitShaDict: FinalizedDictionary,
  categoryDicts: Map<string, FinalizedDictionary>,
  textDicts: Map<string, FinalizedDictionary>,
  _attrDictIds: Map<string, number>,
  _lmaoSchema: Record<string, unknown>,
  schemaFields: Array<[string, unknown]>,
  categoryOriginalToMasked: Map<string, Map<string, string>>,
  textOriginalToMasked: Map<string, Map<string, string>>,
): RecordBatch {
  const totalRows = buffers.reduce((sum, buf) => sum + buf._writeIndex, 0);
  const vectors: Vector[] = [];

  // Get types from the shared schema
  // Schema order: timestamp(0), trace_id(1), thread_id(2), span_id(3), parent_thread_id(4), parent_span_id(5),
  //               entry_type(6), package_name(7), package_file(8), git_sha(9), message(10)
  const timestampType = arrowSchema.fields[0].type as TimestampNanosecond;
  const traceIdType = arrowSchema.fields[1].type as Dictionary<Utf8>;
  // Span ID columns: thread_id (2), span_id (3), parent_thread_id (4), parent_span_id (5)
  const entryTypeType = arrowSchema.fields[6].type as Dictionary<Utf8, Int8>;
  const packageType = arrowSchema.fields[7].type as Dictionary<Utf8>;
  const packagePathType = arrowSchema.fields[8].type as Dictionary<Utf8>;
  const gitShaType = arrowSchema.fields[9].type as Dictionary<Utf8>;

  // ═══════════════════════════════════════════════════════════════════════════
  // Core system columns and metadata columns - concatenate data from all buffers
  // ═══════════════════════════════════════════════════════════════════════════

  // Core system column: Timestamp - BigInt64Array with nanoseconds (from _system ArrayBuffer, zero-copy compatible)
  const allTimestamps = new BigInt64Array(totalRows);
  let offset = 0;
  for (const buf of buffers) {
    // Use set() with subarray - bulk copy
    // Defensive check: timestamps should always exist, but guard against corruption
    if (!buf.timestamp) {
      throw new Error(`Buffer missing timestamps property (_writeIndex: ${buf._writeIndex})`);
    }
    allTimestamps.set(buf.timestamp.subarray(0, buf._writeIndex), offset);
    offset += buf._writeIndex;
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

  // Metadata column: Trace ID (computed from _identity, using shared dictionary)
  const traceIdIndices = new traceIdDict.indexArrayCtor(totalRows);
  offset = 0;
  for (const buf of buffers) {
    const idx = traceIdDict.indexMap.get(buf.trace_id) ?? 0;
    // Use fill() - constant value per buffer
    traceIdIndices.fill(idx, offset, offset + buf._writeIndex);
    offset += buf._writeIndex;
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

  // Metadata column: thread_id (Uint64) - computed from _identity
  // threadId is constant per buffer, use fill()
  const threadIds = new BigUint64Array(totalRows);
  offset = 0;
  for (const buf of buffers) {
    threadIds.fill(buf.thread_id, offset, offset + buf._writeIndex);
    offset += buf._writeIndex;
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

  // Metadata column: span_id (Uint32) - computed from _identity
  const spanIds = new Uint32Array(totalRows);
  offset = 0;
  for (const buf of buffers) {
    // Use fill() - constant value per buffer
    spanIds.fill(buf.span_id, offset, offset + buf._writeIndex);
    offset += buf._writeIndex;
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

  // Metadata column: parent_thread_id (Uint64, nullable) - computed from _identity
  // parentThreadId is constant per buffer (from parent pointer), use fill()
  const parentThreadIds = new BigUint64Array(totalRows);
  const parentThreadIdNulls = new Uint8Array(Math.ceil(totalRows / 8));
  parentThreadIdNulls.fill(0xff);
  let parentThreadIdNullCount = 0;
  offset = 0;
  for (const buf of buffers) {
    if (buf._parent) {
      parentThreadIds.fill(buf.parent_thread_id, offset, offset + buf._writeIndex);
    } else {
      clearBitRange(parentThreadIdNulls, offset, buf._writeIndex);
      parentThreadIdNullCount += buf._writeIndex;
    }
    offset += buf._writeIndex;
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

  // Metadata column: parent_span_id (Uint32, nullable) - computed from _identity
  // Uses hasParent and parentSpanId directly on buffer
  const parentSpanIds = new Uint32Array(totalRows);
  const parentSpanIdNulls = new Uint8Array(Math.ceil(totalRows / 8));
  parentSpanIdNulls.fill(0xff);
  let parentSpanIdNullCount = 0;
  offset = 0;
  for (const buf of buffers) {
    if (buf._hasParent) {
      // Use fill() - constant value per buffer
      parentSpanIds.fill(buf.parent_span_id, offset, offset + buf._writeIndex);
    } else {
      clearBitRange(parentSpanIdNulls, offset, buf._writeIndex);
      parentSpanIdNullCount += buf._writeIndex;
    }
    offset += buf._writeIndex;
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

  // Core system column: Entry type (from _system ArrayBuffer, buf.entry_type)
  const entryTypeIndices = new Int8Array(totalRows);
  offset = 0;
  for (const buf of buffers) {
    // Use set() with subarray - bulk copy
    // Note: operations is Uint8Array but entryTypeIndices is Int8Array, same underlying representation
    // Defensive check: operations should always exist, but guard against corruption
    if (!buf.entry_type) {
      throw new Error(`Buffer missing operations property (_writeIndex: ${buf._writeIndex})`);
    }
    entryTypeIndices.set(buf.entry_type.subarray(0, buf._writeIndex), offset);
    offset += buf._writeIndex;
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

  // Metadata column: Package name (from task._module.packageName, with callsiteModule for row 0)
  // Per specs/01c_context_flow_and_op_wrappers.md:
  // - Row 0 (span-start): uses callsiteModule for gitSha/packageName/packagePath
  // - Rows 1+ (logs, span-end): uses task._module
  const packageIndices = new packageDict.indexArrayCtor(totalRows);
  offset = 0;
  for (const buf of buffers) {
    // Row 0: use callsiteModule if available, else fall back to task._module
    const callsitePkgName = buf._callsiteMetadata?.package_name ?? buf._opMetadata.package_name;
    const callsitePkgIdx = packageDict.indexMap.get(callsitePkgName) ?? 0;

    // Op's module (task._module) for rows 1+
    const opPkgIdx = packageDict.indexMap.get(buf._opMetadata.package_name) ?? 0;

    // Set row 0 (span-start) to callsite module
    packageIndices[offset] = callsitePkgIdx;

    // Set rows 1+ (span-end, logs) to op's module
    if (buf._writeIndex > 1) {
      packageIndices.fill(opPkgIdx, offset + 1, offset + buf._writeIndex);
    }
    offset += buf._writeIndex;
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

  // Metadata column: Package path (from task._module.packagePath, with callsiteModule for row 0)
  const packagePathIndices = new packagePathDict.indexArrayCtor(totalRows);
  offset = 0;
  for (const buf of buffers) {
    // Row 0: use callsiteModule if available, else fall back to task._module
    const callsitePathName = buf._callsiteMetadata?.package_file ?? buf._opMetadata.package_file;
    const callsitePathIdx = packagePathDict.indexMap.get(callsitePathName) ?? 0;

    // Op's module (task._module) for rows 1+
    const opPathIdx = packagePathDict.indexMap.get(buf._opMetadata.package_file) ?? 0;

    // Set row 0 (span-start) to callsite module
    packagePathIndices[offset] = callsitePathIdx;

    // Set rows 1+ (span-end, logs) to op's module
    if (buf._writeIndex > 1) {
      packagePathIndices.fill(opPathIdx, offset + 1, offset + buf._writeIndex);
    }
    offset += buf._writeIndex;
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

  // Metadata column: Git SHA (from task._module.gitSha, with callsiteModule for row 0)
  const gitShaIndices2 = new gitShaDict.indexArrayCtor(totalRows);
  offset = 0;
  for (const buf of buffers) {
    // Row 0: use callsiteModule if available, else fall back to task._module
    const callsiteGitSha = buf._callsiteMetadata?.git_sha ?? buf._opMetadata.git_sha;
    const callsiteGitShaIdx = gitShaDict.indexMap.get(callsiteGitSha) ?? 0;

    // Op's module (task._module) for rows 1+
    const opGitShaIdx = gitShaDict.indexMap.get(buf._opMetadata.git_sha) ?? 0;

    // Set row 0 (span-start) to callsite module
    gitShaIndices2[offset] = callsiteGitShaIdx;

    // Set rows 1+ (span-end, logs) to op's module
    if (buf._writeIndex > 1) {
      gitShaIndices2.fill(opGitShaIdx, offset + 1, offset + buf._writeIndex);
    }
    offset += buf._writeIndex;
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

  // ═══════════════════════════════════════════════════════════════════════════
  // System attribute and user attribute columns - concatenate from all buffers
  // ═══════════════════════════════════════════════════════════════════════════
  // Schema order: Core system + metadata columns + system attribute columns:
  //   Core system: timestamp(0), entry_type(6)
  //   Metadata: trace_id(1), thread_id(2), span_id(3), parent_thread_id(4), parent_span_id(5),
  //             package_name(7), package_file(8), git_sha(9)
  //   System attribute: message(10), uint64_value(11)
  // Total: 12 columns before user attributes
  const METADATA_AND_SYSTEM_COLUMNS_COUNT = 12;
  let fieldIdx = METADATA_AND_SYSTEM_COLUMNS_COUNT;

  // Validate that arrowSchema has the expected number of fields
  // Note: message is NOT in schemaFields (it's a system column), so we need to account for it separately
  const expectedFieldCount = METADATA_AND_SYSTEM_COLUMNS_COUNT + schemaFields.length;
  if (arrowSchema.fields.length !== expectedFieldCount) {
    throw new Error(
      `Schema mismatch: arrowSchema has ${arrowSchema.fields.length} fields, expected ${expectedFieldCount} (${METADATA_AND_SYSTEM_COLUMNS_COUNT} metadata+system + ${schemaFields.length} schema fields). ` +
        `Schema fields: ${schemaFields.map(([name]) => name).join(', ')}. ` +
        `Arrow fields: ${arrowSchema.fields.map((f) => f.name).join(', ')}`,
    );
  }

  // System attribute column: message (at fixed position 10, before user attributes)
  const messageDict = categoryDicts.get('message');
  const messageOriginalToMasked = categoryOriginalToMasked.get('message');
  if (!messageDict || !messageOriginalToMasked) {
    throw new Error('Message dictionary or mapping not found');
  }
  const messageFieldType = arrowSchema.fields[10].type as Dictionary<Utf8, Uint8 | Uint16 | Uint32>;
  const messageIndices = new messageDict.indexArrayCtor(totalRows);
  const messageNullBitmap = new Uint8Array(Math.ceil(totalRows / 8));
  let messageRowOffset = 0;
  for (const buf of buffers) {
    const col = buf.getColumnIfAllocated('message') as string[] | undefined;
    if (col) {
      for (let i = 0; i < buf._writeIndex; i++) {
        const v = col[i];
        const rowIdx = messageRowOffset + i;
        if (v != null) {
          const maskedValue = messageOriginalToMasked.get(v) ?? v;
          messageIndices[rowIdx] = messageDict.indexMap.get(maskedValue) ?? 0;
          messageNullBitmap[rowIdx >>> 3] |= 1 << (rowIdx & 7);
        }
      }
    }
    messageRowOffset += buf._writeIndex;
  }
  vectors.push(
    makeVector(
      makeData({
        type: messageFieldType,
        offset: 0,
        length: totalRows,
        nullCount: 0, // message is eager, never null
        data: messageIndices,
        nullBitmap: messageNullBitmap,
        dictionary: makeVector(
          makeData({
            type: new Utf8(),
            offset: 0,
            length: messageDict.indexMap.size,
            nullCount: 0,
            valueOffsets: messageDict.offsets,
            data: messageDict.data,
          }),
        ),
      }),
    ),
  );

  // System attribute column: uint64_value (for buffer metrics - op durations, counts, etc.)
  // For span data, this is all null - only buffer metrics use it
  const uint64ValueNullBitmap = new Uint8Array(Math.ceil(totalRows / 8));
  // All zeros = all null (Arrow null bitmap: 1 = valid, 0 = null)
  vectors.push(
    makeVector(
      makeData({
        type: new Uint64(),
        offset: 0,
        length: totalRows,
        nullCount: totalRows,
        data: new BigUint64Array(totalRows),
        nullBitmap: uint64ValueNullBitmap,
      }),
    ),
  );

  for (const [fieldName, fieldSchema] of schemaFields) {
    const lmaoType = getSchemaType(fieldSchema);
    const columnName = fieldName; // User columns have no prefix

    // Get the type from the shared schema (user attributes start after system columns)
    if (fieldIdx >= arrowSchema.fields.length) {
      throw new Error(
        `Field index ${fieldIdx} out of bounds for field '${fieldName}'. ` +
          `Arrow schema has ${arrowSchema.fields.length} fields, ` +
          `schema has ${schemaFields.length} fields. ` +
          `Processed ${fieldIdx - METADATA_AND_SYSTEM_COLUMNS_COUNT} user fields so far.`,
      );
    }
    const fieldType = arrowSchema.fields[fieldIdx].type;

    if (lmaoType === 'category') {
      const dict = categoryDicts.get(fieldName);
      const originalToMasked = categoryOriginalToMasked.get(fieldName);
      if (!dict || !originalToMasked) {
        throw new Error(`Category dictionary or mapping not found for field: ${fieldName}`);
      }
      const indices = new dict.indexArrayCtor(totalRows);
      const nullBitmap = new Uint8Array(Math.ceil(totalRows / 8));
      let nullCount = 0;
      let rowOffset = 0;

      // Per specs/01i_span_scope_attributes.md: scope values fill NULL cells at Arrow conversion
      for (const buf of buffers) {
        const col = buf.getColumnIfAllocated(columnName) as string[] | undefined;
        // Check if this buffer has a scope value for this field
        const scopeValue = buf._scopeValues?.[fieldName] as string | undefined;
        let scopeEncodedValue: number | undefined;
        if (scopeValue !== undefined) {
          const maskedScopeValue = originalToMasked.get(scopeValue) ?? scopeValue;
          scopeEncodedValue = dict.indexMap.get(maskedScopeValue);
        }

        if (col) {
          for (let i = 0; i < buf._writeIndex; i++) {
            const v = col[i];
            const rowIdx = rowOffset + i;
            if (v != null) {
              // Direct write wins
              const maskedValue = originalToMasked.get(v) ?? v;
              indices[rowIdx] = dict.indexMap.get(maskedValue) ?? 0;
              nullBitmap[rowIdx >>> 3] |= 1 << (rowIdx & 7);
            } else if (scopeEncodedValue !== undefined) {
              // No direct write, but have scope value - use it
              indices[rowIdx] = scopeEncodedValue;
              nullBitmap[rowIdx >>> 3] |= 1 << (rowIdx & 7);
            } else {
              nullCount++;
            }
          }
        } else if (scopeEncodedValue !== undefined) {
          // Column not allocated but we have scope - fill all rows with scope value
          for (let i = 0; i < buf._writeIndex; i++) {
            const rowIdx = rowOffset + i;
            indices[rowIdx] = scopeEncodedValue;
            nullBitmap[rowIdx >>> 3] |= 1 << (rowIdx & 7);
          }
        } else {
          // No column and no scope - all nulls
          nullCount += buf._writeIndex;
        }
        rowOffset += buf._writeIndex;
      }

      vectors.push(
        makeVector(
          makeData({
            type: fieldType as Dictionary<Utf8, Uint8 | Uint16 | Uint32>,
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
      const indices = new dict.indexArrayCtor(totalRows);
      const nullBitmap = new Uint8Array(Math.ceil(totalRows / 8));
      let nullCount = 0;
      let rowOffset = 0;

      for (const buf of buffers) {
        const col = buf.getColumnIfAllocated(columnName) as string[] | undefined;
        // Check if this buffer has a scope value for this field
        const scopeValue = buf._scopeValues?.[fieldName] as string | undefined;
        let scopeEncodedValue: number | undefined;
        if (scopeValue !== undefined) {
          const maskedScopeValue = originalToMasked.get(scopeValue) ?? scopeValue;
          scopeEncodedValue = dict.indexMap.get(maskedScopeValue);
        }

        if (col) {
          for (let i = 0; i < buf._writeIndex; i++) {
            const v = col[i];
            const rowIdx = rowOffset + i;
            if (v != null) {
              // Direct write wins - look up the masked value
              const maskedValue = originalToMasked.get(v) ?? v;
              indices[rowIdx] = dict.indexMap.get(maskedValue) ?? 0;
              nullBitmap[rowIdx >>> 3] |= 1 << (rowIdx & 7);
            } else if (scopeEncodedValue !== undefined) {
              // No direct write, but have scope value - use it
              indices[rowIdx] = scopeEncodedValue;
              nullBitmap[rowIdx >>> 3] |= 1 << (rowIdx & 7);
            } else {
              nullCount++;
            }
          }
        } else if (scopeEncodedValue !== undefined) {
          // Column not allocated but we have scope - fill all rows with scope value
          for (let i = 0; i < buf._writeIndex; i++) {
            const rowIdx = rowOffset + i;
            indices[rowIdx] = scopeEncodedValue;
            nullBitmap[rowIdx >>> 3] |= 1 << (rowIdx & 7);
          }
        } else {
          // No column and no scope - all nulls
          nullCount += buf._writeIndex;
        }
        rowOffset += buf._writeIndex;
      }

      vectors.push(
        makeVector(
          makeData({
            type: fieldType as Dictionary<Utf8, Uint8 | Uint16 | Uint32>,
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
      // Start with all nulls (bits cleared) - we'll set bits as we find values
      let nullCount = 0;
      let rowOffset = 0;

      for (const buf of buffers) {
        const col = buf.getColumnIfAllocated(columnName);
        const srcNulls = buf.getNullsIfAllocated(columnName);
        // Check if this buffer has a scope value for this field
        const scopeValue = buf._scopeValues?.[fieldName] as number | undefined;

        if (col instanceof Float64Array) {
          allValues.set(col.subarray(0, buf._writeIndex), rowOffset);
          if (srcNulls) {
            // Process each row - direct write wins, then scope, then null
            for (let i = 0; i < buf._writeIndex; i++) {
              const srcByte = i >>> 3;
              const srcBit = i & 7;
              const isValid = (srcNulls[srcByte] & (1 << srcBit)) !== 0;
              const rowIdx = rowOffset + i;
              if (isValid) {
                // Direct write - mark as valid
                nullBitmap[rowIdx >>> 3] |= 1 << (rowIdx & 7);
              } else if (scopeValue !== undefined) {
                // No direct write, but have scope value - use it
                allValues[rowIdx] = scopeValue;
                nullBitmap[rowIdx >>> 3] |= 1 << (rowIdx & 7);
              } else {
                // No direct write and no scope - null
                nullCount++;
              }
            }
          } else {
            // No null bitmap means column was allocated but no writes - check scope
            for (let i = 0; i < buf._writeIndex; i++) {
              const rowIdx = rowOffset + i;
              if (scopeValue !== undefined) {
                allValues[rowIdx] = scopeValue;
                nullBitmap[rowIdx >>> 3] |= 1 << (rowIdx & 7);
              } else {
                nullCount++;
              }
            }
          }
        } else if (scopeValue !== undefined) {
          // Column not allocated but we have scope - fill all rows with scope value
          for (let i = 0; i < buf._writeIndex; i++) {
            const rowIdx = rowOffset + i;
            allValues[rowIdx] = scopeValue;
            nullBitmap[rowIdx >>> 3] |= 1 << (rowIdx & 7);
          }
        } else {
          // Column doesn't exist for this buffer and no scope - all nulls
          nullCount += buf._writeIndex;
        }
        rowOffset += buf._writeIndex;
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
      // Start with all nulls (bits cleared) - we'll set bits as we find values
      let nullCount = 0;
      let rowOffset = 0;

      for (const buf of buffers) {
        const col = buf.getColumnIfAllocated(columnName);
        const srcNulls = buf.getNullsIfAllocated(columnName);
        // Check if this buffer has a scope value for this field
        const scopeValue = buf._scopeValues?.[fieldName] as boolean | undefined;

        if (col instanceof Uint8Array) {
          // Copy boolean values bit by bit - can't avoid loop for bit-level operations
          for (let i = 0; i < buf._writeIndex; i++) {
            const srcByte = i >>> 3;
            const srcBit = i & 7;
            const rowIdx = rowOffset + i;

            if (srcNulls) {
              const isValid = (srcNulls[srcByte] & (1 << srcBit)) !== 0;
              if (isValid) {
                // Direct write - copy the value
                const value = (col[srcByte] & (1 << srcBit)) !== 0;
                if (value) {
                  allValues[rowIdx >>> 3] |= 1 << (rowIdx & 7);
                }
                nullBitmap[rowIdx >>> 3] |= 1 << (rowIdx & 7);
              } else if (scopeValue !== undefined) {
                // No direct write, but have scope value - use it
                if (scopeValue) {
                  allValues[rowIdx >>> 3] |= 1 << (rowIdx & 7);
                }
                nullBitmap[rowIdx >>> 3] |= 1 << (rowIdx & 7);
              } else {
                // No direct write and no scope - null
                nullCount++;
              }
            } else {
              // No null bitmap - check scope
              if (scopeValue !== undefined) {
                if (scopeValue) {
                  allValues[rowIdx >>> 3] |= 1 << (rowIdx & 7);
                }
                nullBitmap[rowIdx >>> 3] |= 1 << (rowIdx & 7);
              } else {
                nullCount++;
              }
            }
          }
        } else if (scopeValue !== undefined) {
          // Column not allocated but we have scope - fill all rows with scope value
          for (let i = 0; i < buf._writeIndex; i++) {
            const rowIdx = rowOffset + i;
            if (scopeValue) {
              allValues[rowIdx >>> 3] |= 1 << (rowIdx & 7);
            }
            nullBitmap[rowIdx >>> 3] |= 1 << (rowIdx & 7);
          }
        } else {
          // Column doesn't exist for this buffer and no scope - all nulls
          nullCount += buf._writeIndex;
        }
        rowOffset += buf._writeIndex;
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
      // Get constructors from schema metadata
      const enumSchema = fieldSchema as { __index_array_ctor?: unknown; __arrow_index_type_ctor?: unknown };
      const indexArrayCtor =
        (enumSchema.__index_array_ctor as Uint8ArrayConstructor | Uint16ArrayConstructor | Uint32ArrayConstructor) ??
        Uint8Array;
      const arrowIndexTypeCtor =
        (enumSchema.__arrow_index_type_ctor as typeof Uint8 | typeof Uint16 | typeof Uint32) ?? Uint8;
      const allIndices = new indexArrayCtor(totalRows);
      const nullBitmap = new Uint8Array(Math.ceil(totalRows / 8));
      // Start with all nulls (bits cleared) - we'll set bits as we find values
      let nullCount = 0;
      let rowOffset = 0;

      // Build enum value to index mapping for scope lookup
      const enumValueToIndex = new Map<string, number>();
      for (let i = 0; i < enumValues.length; i++) {
        enumValueToIndex.set(enumValues[i] as string, i);
      }

      for (const buf of buffers) {
        const col = buf.getColumnIfAllocated(columnName);
        const srcNulls = buf.getNullsIfAllocated(columnName);
        // Check if this buffer has a scope value for this field
        const scopeValue = buf._scopeValues?.[fieldName] as string | undefined;
        let scopeEncodedValue: number | undefined;
        if (scopeValue !== undefined) {
          scopeEncodedValue = enumValueToIndex.get(scopeValue);
        }

        if (col instanceof indexArrayCtor) {
          allIndices.set(col.subarray(0, buf._writeIndex), rowOffset);
          if (srcNulls) {
            for (let i = 0; i < buf._writeIndex; i++) {
              const srcByte = i >>> 3;
              const srcBit = i & 7;
              const isValid = (srcNulls[srcByte] & (1 << srcBit)) !== 0;
              const rowIdx = rowOffset + i;
              if (isValid) {
                // Direct write - mark as valid
                nullBitmap[rowIdx >>> 3] |= 1 << (rowIdx & 7);
              } else if (scopeEncodedValue !== undefined) {
                // No direct write, but have scope value - use it
                allIndices[rowIdx] = scopeEncodedValue;
                nullBitmap[rowIdx >>> 3] |= 1 << (rowIdx & 7);
              } else {
                // No direct write and no scope - null
                nullCount++;
              }
            }
          } else {
            // No null bitmap - check scope for each row
            for (let i = 0; i < buf._writeIndex; i++) {
              const rowIdx = rowOffset + i;
              if (scopeEncodedValue !== undefined) {
                allIndices[rowIdx] = scopeEncodedValue;
                nullBitmap[rowIdx >>> 3] |= 1 << (rowIdx & 7);
              } else {
                nullCount++;
              }
            }
          }
        } else if (scopeEncodedValue !== undefined) {
          // Column not allocated but we have scope - fill all rows with scope value
          for (let i = 0; i < buf._writeIndex; i++) {
            const rowIdx = rowOffset + i;
            allIndices[rowIdx] = scopeEncodedValue;
            nullBitmap[rowIdx >>> 3] |= 1 << (rowIdx & 7);
          }
        } else {
          // Column doesn't exist for this buffer and no scope - all nulls
          nullCount += buf._writeIndex;
        }
        rowOffset += buf._writeIndex;
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
            type: new Dictionary(new Utf8(), new arrowIndexTypeCtor()),
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
