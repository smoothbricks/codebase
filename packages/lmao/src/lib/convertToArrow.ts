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
import { createCapacityStatsRecordBatch } from './arrow/capacityStats.js';
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
import { ENTRY_TYPE_NAMES } from './lmao.js';
import type { ModuleContext } from './moduleContext.js';
import { getEnumUtf8, getEnumValues, getSchemaType } from './schema/typeGuards.js';
import { getSchemaFields } from './schema/types.js';
import type { SpanBuffer } from './types.js';
import { globalUtf8Cache } from './utf8Cache.js';

export type SystemColumnBuilder = (
  buffer: SpanBuffer,
  buffers: SpanBuffer[],
  totalRows: number,
) => { fields: Field[]; vectors: Vector[] };

/**
 * Convert SpanBuffer (and its overflow chain) to Arrow RecordBatch.
 *
 * **Dictionary handling**: Each RecordBatch has its own dictionary data.
 * Dictionaries are built from the data in this RecordBatch only and are not shared
 * with other RecordBatches, even when combined into a Table.
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

/**
 * Convert multiple buffers to a single RecordBatch.
 *
 * **Dictionary handling**: Each RecordBatch has its own dictionary data.
 * Dictionaries are built from the data in this RecordBatch only and are not shared
 * with other RecordBatches, even when combined into a Table.
 */
function convertBuffersToRecordBatch(buffers: SpanBuffer[], systemColumnBuilder?: SystemColumnBuilder): RecordBatch {
  if (buffers.length === 0) return new RecordBatch({});

  const totalRows = buffers.reduce((sum, buf) => sum + buf.writeIndex, 0);
  if (totalRows === 0) return new RecordBatch({});

  const schema = buffers[0].task.module.tagAttributes;
  const fields: Field[] = [];
  let systemVectors: Vector[] = [];

  // Build metadata columns first to determine correct index types
  let traceIdArrowIndexTypeCtor: typeof Uint8 | typeof Uint16 | typeof Uint32 = Uint32;
  let packageNameArrowIndexTypeCtor: typeof Uint8 | typeof Uint16 | typeof Uint32 = Uint32;
  let packagePathArrowIndexTypeCtor: typeof Uint8 | typeof Uint16 | typeof Uint32 = Uint32;
  let gitShaArrowIndexTypeCtor: typeof Uint8 | typeof Uint16 | typeof Uint32 = Uint32;

  if (!systemColumnBuilder) {
    // Determine index types by building dictionaries first
    const traceIdSet = new Set<string>();
    for (const buf of buffers) traceIdSet.add(buf.traceId);
    const traceIdUniqueCount = traceIdSet.size;
    traceIdArrowIndexTypeCtor = traceIdUniqueCount <= 255 ? Uint8 : traceIdUniqueCount <= 65535 ? Uint16 : Uint32;

    const packageSet = new Set<string>();
    for (const buf of buffers) packageSet.add(buf.task.module.packageName);
    const packageUniqueCount = packageSet.size;
    packageNameArrowIndexTypeCtor = packageUniqueCount <= 255 ? Uint8 : packageUniqueCount <= 65535 ? Uint16 : Uint32;

    const modulePathSet = new Set<string>();
    for (const buf of buffers) modulePathSet.add(buf.task.module.packagePath);
    const packagePathUniqueCount = modulePathSet.size;
    packagePathArrowIndexTypeCtor =
      packagePathUniqueCount <= 255 ? Uint8 : packagePathUniqueCount <= 65535 ? Uint16 : Uint32;

    const gitShaSet = new Set<string>();
    for (const buf of buffers) gitShaSet.add(buf.task.module.gitSha || '');
    const gitShaUniqueCount = gitShaSet.size;
    gitShaArrowIndexTypeCtor = gitShaUniqueCount <= 255 ? Uint8 : gitShaUniqueCount <= 65535 ? Uint16 : Uint32;
  }

  if (systemColumnBuilder) {
    const systemColumns = systemColumnBuilder(buffers[0], buffers, totalRows);
    fields.push(...systemColumns.fields);
    systemVectors = systemColumns.vectors;
  } else {
    fields.push(Field.new({ name: 'timestamp', type: new TimestampNanosecond() }));
    fields.push(Field.new({ name: 'trace_id', type: new Dictionary(new Utf8(), new traceIdArrowIndexTypeCtor()) }));
    // Span ID columns (separate columns instead of struct)
    fields.push(Field.new({ name: 'thread_id', type: new Uint64() }));
    fields.push(Field.new({ name: 'span_id', type: new Uint32() }));
    fields.push(Field.new({ name: 'parent_thread_id', type: new Uint64(), nullable: true }));
    fields.push(Field.new({ name: 'parent_span_id', type: new Uint32(), nullable: true }));
    fields.push(Field.new({ name: 'entry_type', type: new Dictionary(new Utf8(), new Int8()) }));
    fields.push(
      Field.new({ name: 'package_name', type: new Dictionary(new Utf8(), new packageNameArrowIndexTypeCtor()) }),
    );
    fields.push(
      Field.new({ name: 'package_path', type: new Dictionary(new Utf8(), new packagePathArrowIndexTypeCtor()) }),
    );
    fields.push(Field.new({ name: 'git_sha', type: new Dictionary(new Utf8(), new gitShaArrowIndexTypeCtor()) }));
    // System attribute column: message (eager category)
    fields.push(Field.new({ name: 'message', type: new Dictionary(new Utf8(), new Uint32()) }));
  }

  for (const [fieldName, fieldSchema] of Object.entries(schema)) {
    const lmaoType = getSchemaType(fieldSchema);
    const arrowFieldName = getArrowFieldName(fieldName);

    if (lmaoType === 'enum') {
      // Get constructors from schema metadata
      const enumSchema = fieldSchema as { __arrow_index_type_ctor?: unknown };
      const arrowIndexTypeCtor =
        (enumSchema.__arrow_index_type_ctor as typeof Uint8 | typeof Uint16 | typeof Uint32) ?? Uint8;
      fields.push(
        Field.new({
          name: arrowFieldName,
          type: new Dictionary(new Utf8(), new arrowIndexTypeCtor()),
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

  const vectors: Vector[] = [];

  if (systemColumnBuilder) {
    vectors.push(...systemVectors);
  } else {
    buildMetadataColumns(buffers, vectors);

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

    // Update message field type to match actual dictionary index type
    const messageFieldIndex = fields.findIndex((f) => f.name === 'message');
    if (messageFieldIndex !== -1) {
      fields[messageFieldIndex] = Field.new({
        name: 'message',
        type: new Dictionary(new Utf8(), new arrowIndexTypeCtor()),
      });
    }

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
      type: new Dictionary(new Utf8(), new arrowIndexTypeCtor()),
      offset: 0,
      length: totalRows,
      nullCount: 0, // message is eager, never null
      data: indices,
      nullBitmap, // Always include nullBitmap for message (even though nullCount is 0)
      dictionary: makeVector(messageDictData),
    });

    vectors.push(makeVector(messageData));
  }

  // Build user attribute vectors and update schema field types BEFORE creating schema
  for (const [fieldName, fieldSchema] of Object.entries(schema)) {
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
      const valuesName = `${columnName}_values` as const;

      // Collect value arrays - need to handle different TypedArray types
      const valueArrays: (Uint8Array | Uint16Array | Uint32Array)[] = [];
      for (const buf of buffers) {
        const column = buf[valuesName];
        if (column && column instanceof indexArrayCtor) {
          valueArrays.push(column.subarray(0, buf.writeIndex));
        } else {
          valueArrays.push(new indexArrayCtor(buf.writeIndex));
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

      const enumDictData = makeData({
        type: new Utf8(),
        offset: 0,
        length: enumValues.length,
        nullCount: 0,
        valueOffsets: enumUtf8?.offsets ?? calculateUtf8Offsets(enumValues as readonly string[]),
        data: enumUtf8?.concatenated ?? encodeUtf8Strings(enumValues as readonly string[]),
      });

      const enumData = makeData({
        type: new Dictionary(new Utf8(), new arrowIndexTypeCtor()),
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

      // Update field type to match actual dictionary index type (BEFORE Schema creation)
      const fieldIndex = fields.findIndex((f) => f.name === arrowFieldName);
      if (fieldIndex !== -1) {
        fields[fieldIndex] = Field.new({
          name: arrowFieldName,
          type: new Dictionary(new Utf8(), new arrowIndexTypeCtor()),
          nullable: true,
        });
      }

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
        type: new Dictionary(new Utf8(), new arrowIndexTypeCtor()),
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

        // Update field type to match actual dictionary index type (BEFORE Schema creation)
        const fieldIndex = fields.findIndex((f) => f.name === arrowFieldName);
        if (fieldIndex !== -1) {
          fields[fieldIndex] = Field.new({
            name: arrowFieldName,
            type: new Dictionary(new Utf8(), new arrowIndexTypeCtor()),
            nullable: true,
          });
        }

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
          type: new Dictionary(new Utf8(), new arrowIndexTypeCtor()),
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
 * Convert SpanBuffer (and its overflow chain) to Arrow Table
 */
export function convertToArrowTable(buffer: SpanBuffer, systemColumnBuilder?: SystemColumnBuilder): Table {
  const batch = convertToRecordBatch(buffer, systemColumnBuilder);
  if (batch.numRows === 0) return new Table();
  return new Table([batch]);
}

/**
 * Build metadata columns (derived/computed from buffer properties) and core system columns (from _system ArrayBuffer).
 *
 * Metadata columns are computed from buffer properties:
 * - trace_id, thread_id, span_id, parent_thread_id, parent_span_id: from _identity
 * - package_name, package_path, git_sha: from task.module
 *
 * Core system columns are stored in _system ArrayBuffer:
 * - timestamp: from buf.timestamps
 * - entry_type: from buf.operations
 */
function buildMetadataColumns(buffers: SpanBuffer[], vectors: Vector[]): void {
  const totalRows = buffers.reduce((sum, buf) => sum + buf.writeIndex, 0);

  // Core system column: Timestamp - BigInt64Array with nanoseconds (from _system ArrayBuffer)
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

  // Metadata column: Trace ID (computed from _identity)
  const traceIdSet = new Set<string>();
  for (const buf of buffers) traceIdSet.add(buf.traceId);
  const traceIdArray = Array.from(traceIdSet);
  const traceIdMap = new Map(traceIdArray.map((id, idx) => [id, idx]));

  // Determine index type based on dictionary size
  const traceIdUniqueCount = traceIdArray.length;
  const traceIdIndexArrayCtor =
    traceIdUniqueCount <= 255 ? Uint8Array : traceIdUniqueCount <= 65535 ? Uint16Array : Uint32Array;
  const traceIdArrowIndexTypeCtor = traceIdUniqueCount <= 255 ? Uint8 : traceIdUniqueCount <= 65535 ? Uint16 : Uint32;

  const traceIdIndices = new traceIdIndexArrayCtor(totalRows);
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
        type: new Dictionary(new Utf8(), new traceIdArrowIndexTypeCtor()),
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

  // Metadata column: parent_span_id (Uint32, nullable) - computed from _identity
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

  // Metadata column: Package name (from task.module.packageName)
  const packageSet = new Set<string>();
  for (const buf of buffers) packageSet.add(buf.task.module.packageName);
  const packageArray = Array.from(packageSet);
  const packageMap = new Map(packageArray.map((name, idx) => [name, idx]));

  // Determine index type based on dictionary size
  const packageUniqueCount = packageArray.length;
  const packageIndexArrayCtor =
    packageUniqueCount <= 255 ? Uint8Array : packageUniqueCount <= 65535 ? Uint16Array : Uint32Array;
  const packageArrowIndexTypeCtor = packageUniqueCount <= 255 ? Uint8 : packageUniqueCount <= 65535 ? Uint16 : Uint32;

  const packageIndices = new packageIndexArrayCtor(totalRows);
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
        type: new Dictionary(new Utf8(), new packageArrowIndexTypeCtor()),
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: packageIndices,
        dictionary: makeVector(packageDictData),
      }),
    ),
  );

  // Metadata column: Package path (from task.module.packagePath)
  const modulePathSet = new Set<string>();
  for (const buf of buffers) modulePathSet.add(buf.task.module.packagePath);
  const modulePathArray = Array.from(modulePathSet);
  const modulePathMap = new Map(modulePathArray.map((name, idx) => [name, idx]));

  // Determine index type based on dictionary size
  const packagePathUniqueCount = modulePathArray.length;
  const packagePathIndexArrayCtor =
    packagePathUniqueCount <= 255 ? Uint8Array : packagePathUniqueCount <= 65535 ? Uint16Array : Uint32Array;
  const packagePathArrowIndexTypeCtor =
    packagePathUniqueCount <= 255 ? Uint8 : packagePathUniqueCount <= 65535 ? Uint16 : Uint32;

  const packagePathIndices = new packagePathIndexArrayCtor(totalRows);
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
        type: new Dictionary(new Utf8(), new packagePathArrowIndexTypeCtor()),
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: packagePathIndices,
        dictionary: makeVector(packagePathDictData),
      }),
    ),
  );

  // Metadata column: Git SHA (from task.module.gitSha)
  const gitShaSet = new Set<string>();
  for (const buf of buffers) gitShaSet.add(buf.task.module.gitSha);
  const gitShaArray = Array.from(gitShaSet);
  const gitShaMap = new Map(gitShaArray.map((sha, idx) => [sha, idx]));

  // Determine index type based on dictionary size
  const gitShaUniqueCount = gitShaArray.length;
  const gitShaIndexArrayCtor =
    gitShaUniqueCount <= 255 ? Uint8Array : gitShaUniqueCount <= 65535 ? Uint16Array : Uint32Array;
  const gitShaArrowIndexTypeCtor = gitShaUniqueCount <= 255 ? Uint8 : gitShaUniqueCount <= 65535 ? Uint16 : Uint32;

  const gitShaIndices = new gitShaIndexArrayCtor(totalRows);
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
        type: new Dictionary(new Utf8(), new gitShaArrowIndexTypeCtor()),
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: gitShaIndices,
        dictionary: makeVector(gitShaDictData),
      }),
    ),
  );
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
  rootBuffer: SpanBuffer,
  _systemColumnBuilder?: SystemColumnBuilder,
  modulesToLogStats?: Set<ModuleContext>,
): Table {
  const schema = rootBuffer.task.module.tagAttributes;

  // Cache schema fields once - reuse throughout conversion
  const schemaFields = getSchemaFields(schema);

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
  // Use a Set to deduplicate by ModuleContext identity (same module = same entries)
  const uniqueModules = new Set<ModuleContext>();

  let spanRows = 0;
  walkSpanTree(rootBuffer, (buffer) => {
    spanRows += buffer.writeIndex;
    traceIdBuilder.add(buffer.traceId);

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

  if (spanRows === 0 && (!modulesToLogStats || modulesToLogStats.size === 0)) {
    return new Table();
  }

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

  // Metadata columns (from task.module):
  arrowFields.push(
    Field.new({ name: 'package_name', type: new Dictionary(new Utf8(), new packageDict.arrowIndexTypeCtor(), 2) }),
  );
  arrowFields.push(
    Field.new({ name: 'package_path', type: new Dictionary(new Utf8(), new packagePathDict.arrowIndexTypeCtor(), 3) }),
  );
  arrowFields.push(
    Field.new({ name: 'git_sha', type: new Dictionary(new Utf8(), new gitShaDict.arrowIndexTypeCtor(), 4) }),
  );

  // System attribute column: message (handled as category column, but positioned here in schema)
  arrowFields.push(
    Field.new({ name: 'message', type: new Dictionary(new Utf8(), new messageDict.arrowIndexTypeCtor(), 5) }),
  );

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
  const allBuffers: SpanBuffer[] = [];

  walkSpanTree(rootBuffer, (buffer) => {
    if (buffer.writeIndex > 0) {
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
          schema,
          schemaFields,
          categoryOriginalToMasked,
          textOriginalToMasked,
        )
      : undefined;

  // Create capacity stats RecordBatch if needed
  if (modulesToLogStats && modulesToLogStats.size > 0) {
    const hasSpanData = batch !== undefined;
    const capacityStatsBatch = createCapacityStatsRecordBatch(modulesToLogStats, arrowSchema, schema, hasSpanData);
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
  buffers: SpanBuffer[],
  arrowSchema: Schema,
  traceIdDict: FinalizedDictionary,
  packageDict: FinalizedDictionary,
  packagePathDict: FinalizedDictionary,
  gitShaDict: FinalizedDictionary,
  categoryDicts: Map<string, FinalizedDictionary>,
  textDicts: Map<string, FinalizedDictionary>,
  _attrDictIds: Map<string, number>,
  lmaoSchema: Record<string, unknown>,
  schemaFields: Array<[string, unknown]>,
  categoryOriginalToMasked: Map<string, Map<string, string>>,
  textOriginalToMasked: Map<string, Map<string, string>>,
): RecordBatch {
  const totalRows = buffers.reduce((sum, buf) => sum + buf.writeIndex, 0);
  const vectors: Vector[] = [];

  // Get types from the shared schema
  // Schema order: timestamp(0), trace_id(1), thread_id(2), span_id(3), parent_thread_id(4), parent_span_id(5),
  //               entry_type(6), package_name(7), package_path(8), git_sha(9), message(10)
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

  // Metadata column: Trace ID (computed from _identity, using shared dictionary)
  const traceIdIndices = new traceIdDict.indexArrayCtor(totalRows);
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

  // Metadata column: thread_id (Uint64) - computed from _identity
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

  // Metadata column: span_id (Uint32) - computed from _identity
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

  // Metadata column: parent_thread_id (Uint64, nullable) - computed from _identity
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

  // Metadata column: parent_span_id (Uint32, nullable) - computed from _identity
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

  // Core system column: Entry type (from _system ArrayBuffer, buf.operations)
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

  // Metadata column: Package name (from task.module.packageName, using shared dictionary)
  const packageIndices = new packageDict.indexArrayCtor(totalRows);
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

  // Metadata column: Package path (from task.module.packagePath, using shared dictionary)
  const packagePathIndices = new packagePathDict.indexArrayCtor(totalRows);
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

  // Metadata column: Git SHA (from task.module.gitSha, using shared dictionary)
  const gitShaIndices2 = new gitShaDict.indexArrayCtor(totalRows);
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

  // ═══════════════════════════════════════════════════════════════════════════
  // System attribute and user attribute columns - concatenate from all buffers
  // ═══════════════════════════════════════════════════════════════════════════
  // Schema order: Core system + metadata columns + system attribute columns:
  //   Core system: timestamp(0), entry_type(6)
  //   Metadata: trace_id(1), thread_id(2), span_id(3), parent_thread_id(4), parent_span_id(5),
  //             package_name(7), package_path(8), git_sha(9)
  //   System attribute: message(10)
  // Total: 11 columns before user attributes
  const METADATA_AND_SYSTEM_COLUMNS_COUNT = 11;
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
      for (let i = 0; i < buf.writeIndex; i++) {
        const v = col[i];
        const rowIdx = messageRowOffset + i;
        if (v != null) {
          const maskedValue = messageOriginalToMasked.get(v) ?? v;
          messageIndices[rowIdx] = messageDict.indexMap.get(maskedValue) ?? 0;
          messageNullBitmap[rowIdx >>> 3] |= 1 << (rowIdx & 7);
        }
      }
    }
    messageRowOffset += buf.writeIndex;
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
      // Get constructors from schema metadata
      const enumSchema = fieldSchema as { __index_array_ctor?: unknown; __arrow_index_type_ctor?: unknown };
      const indexArrayCtor =
        (enumSchema.__index_array_ctor as Uint8ArrayConstructor | Uint16ArrayConstructor | Uint32ArrayConstructor) ??
        Uint8Array;
      const arrowIndexTypeCtor =
        (enumSchema.__arrow_index_type_ctor as typeof Uint8 | typeof Uint16 | typeof Uint32) ?? Uint8;
      const allIndices = new indexArrayCtor(totalRows);
      const nullBitmap = new Uint8Array(Math.ceil(totalRows / 8));
      nullBitmap.fill(0xff);
      let nullCount = 0;
      let rowOffset = 0;

      for (const buf of buffers) {
        const col = buf.getColumnIfAllocated(columnName);
        const srcNulls = buf.getNullsIfAllocated(columnName);

        if (col instanceof indexArrayCtor) {
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
