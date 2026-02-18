/**
 * Zero-copy conversion from SpanBuffer to Flechette tables
 *
 * Per specs/lmao/01f_arrow_table_structure.md:
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
  batchType,
  binary,
  bool,
  Column,
  dictionary,
  float64,
  type IntType,
  int8,
  type Table,
  TimeUnit,
  tableFromColumns,
  timestamp,
  uint8,
  uint16,
  uint32,
  uint64,
  utf8,
} from '@uwdata/flechette';
import { type CapacityStatsEntry, createCapacityStatsTable } from './arrow/capacityStats.js';
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
import { getBinaryEncoder, getEnumUtf8, getEnumValues, getSchemaType } from './schema/typeGuards.js';
import type { LogSchema } from './schema/types.js';
import type { AnySpanBuffer, OpMetadata } from './types.js';
import { globalUtf8Cache } from './utf8Cache.js';

const DictBuilder = DictionaryBuilder;
const F64Array = Float64Array;

export type SystemColumnBuilder = (
  buffer: AnySpanBuffer,
  buffers: AnySpanBuffer[],
  totalRows: number,
) => { fields: string[]; vectors: Column<unknown>[] };

const EMPTY_VALIDITY = new Uint8Array(0);
const BINARY_TYPE = binary();
const BOOL_TYPE_ID = (bool() as { typeId?: unknown }).typeId;

function buildData<T extends Record<string, unknown>>(data: T): T {
  return data;
}

function isBoolType(value: unknown): boolean {
  return (value as { typeId?: unknown })?.typeId === BOOL_TYPE_ID;
}

function buildColumn(data: {
  type: unknown;
  length: number;
  nullCount: number;
  data?: unknown;
  nullBitmap?: Uint8Array;
  valueOffsets?: Int32Array;
  dictionary?: Column<unknown>;
}): Column<unknown> {
  const validity = data.nullBitmap ?? EMPTY_VALIDITY;

  if (data.dictionary) {
    const dictType = data.type as ReturnType<typeof dictionary>;
    const batch = new (batchType(dictType))({
      type: dictType,
      length: data.length,
      nullCount: data.nullCount,
      validity,
      values: data.data,
    }) as {
      setDictionary(dictionaryColumn: Column<unknown>): void;
    };
    batch.setDictionary(data.dictionary);
    return new Column([batch as never]);
  }

  if (data.valueOffsets) {
    const utf8Type = data.type as ReturnType<typeof utf8>;
    const batch = new (batchType(utf8Type))({
      type: utf8Type,
      length: data.length,
      nullCount: data.nullCount,
      validity,
      offsets: data.valueOffsets,
      values: data.data,
    });
    return new Column([batch]);
  }

  if (isBoolType(data.type)) {
    const boolType = data.type as ReturnType<typeof bool>;
    const batch = new (batchType(boolType))({
      type: boolType,
      length: data.length,
      nullCount: data.nullCount,
      validity,
      values: data.data,
    });
    return new Column([batch]);
  }

  const batch = new (batchType(data.type as never))({
    type: data.type,
    length: data.length,
    nullCount: data.nullCount,
    validity,
    values: data.data,
  });
  return new Column([batch]);
}

function buildTable(fields: string[], vectors: Column<unknown>[]): Table {
  if (fields.length === 0) return tableFromColumns({});
  const entries: [string, Column<unknown>][] = fields.map((name, index) => [name, vectors[index] as Column<unknown>]);
  return tableFromColumns(entries);
}

function appendTables(base: Table, extra: Table): Table {
  const entries: [string, Column<unknown>][] = [];
  for (let i = 0; i < base.numCols; i++) {
    const name = base.names[i] as string;
    const left = base.getChildAt(i);
    const right = extra.getChild(name);
    if (!left || !right) {
      throw new Error(`Cannot append tables: missing column '${name}'`);
    }
    const batches = [...(left.data as unknown[]), ...(right.data as unknown[])];
    entries.push([name, new Column(batches as never[])]);
  }
  return tableFromColumns(entries);
}

function indexTypeForCount(count: number): IntType {
  if (count <= 255) return uint8();
  if (count <= 65535) return uint16();
  return uint32();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Shared metadata column helpers (used by both buildMetadataColumnsWithFields
// and convertBuffersWithSharedDicts to avoid duplication)
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Build non-dictionary identity columns: timestamp, thread_id, span_id, parent_thread_id, parent_span_id.
 * These are identical in both single-batch and shared-dict paths.
 *
 * Callers are responsible for inserting trace_id (dictionary column) at the correct position
 * and entry_type (dictionary column) after these columns.
 */
function buildNonDictIdentityColumns(
  buffers: AnySpanBuffer[],
  totalRows: number,
  fields: string[],
  vectors: Column<unknown>[],
): void {
  // thread_id (BigUint64Array, constant per buffer)
  const threadIds = new BigUint64Array(totalRows);
  let offset = 0;
  for (const buf of buffers) {
    threadIds.fill(buf.thread_id, offset, offset + buf._writeIndex);
    offset += buf._writeIndex;
  }
  fields.push('thread_id');
  vectors.push(buildColumn(buildData({ type: uint64(), offset: 0, length: totalRows, nullCount: 0, data: threadIds })));

  // span_id (Uint32Array, constant per buffer)
  const spanIds = new Uint32Array(totalRows);
  offset = 0;
  for (const buf of buffers) {
    spanIds.fill(buf.span_id, offset, offset + buf._writeIndex);
    offset += buf._writeIndex;
  }
  fields.push('span_id');
  vectors.push(buildColumn(buildData({ type: uint32(), offset: 0, length: totalRows, nullCount: 0, data: spanIds })));

  // parent_thread_id (nullable BigUint64Array)
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
  fields.push('parent_thread_id');
  vectors.push(
    buildColumn(
      buildData({
        type: uint64(),
        offset: 0,
        length: totalRows,
        nullCount: parentThreadIdNullCount,
        data: parentThreadIds,
        nullBitmap: parentThreadIdNullCount > 0 ? parentThreadIdNulls : undefined,
      }),
    ),
  );

  // parent_span_id (nullable Uint32Array)
  const parentSpanIds = new Uint32Array(totalRows);
  const parentSpanIdNulls = new Uint8Array(Math.ceil(totalRows / 8));
  parentSpanIdNulls.fill(0xff);
  let parentSpanIdNullCount = 0;
  offset = 0;
  for (const buf of buffers) {
    if (buf._hasParent) {
      parentSpanIds.fill(buf.parent_span_id, offset, offset + buf._writeIndex);
    } else {
      clearBitRange(parentSpanIdNulls, offset, buf._writeIndex);
      parentSpanIdNullCount += buf._writeIndex;
    }
    offset += buf._writeIndex;
  }
  fields.push('parent_span_id');
  vectors.push(
    buildColumn(
      buildData({
        type: uint32(),
        offset: 0,
        length: totalRows,
        nullCount: parentSpanIdNullCount,
        data: parentSpanIds,
        nullBitmap: parentSpanIdNullCount > 0 ? parentSpanIdNulls : undefined,
      }),
    ),
  );
}

/**
 * Build timestamp column from buffers.
 */
function buildTimestampColumn(
  buffers: AnySpanBuffer[],
  totalRows: number,
  fields: string[],
  vectors: Column<unknown>[],
): void {
  const allTimestamps = new BigInt64Array(totalRows);
  let offset = 0;
  for (const buf of buffers) {
    if (!buf.timestamp) {
      throw new Error(`Buffer missing timestamps property (_writeIndex: ${buf._writeIndex})`);
    }
    allTimestamps.set(buf.timestamp.subarray(0, buf._writeIndex), offset);
    offset += buf._writeIndex;
  }
  fields.push('timestamp');
  vectors.push(
    buildColumn(
      buildData({
        type: timestamp(TimeUnit.NANOSECOND),
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: allTimestamps,
      }),
    ),
  );
}

/**
 * Build entry_type dictionary column.
 */
function buildEntryTypeColumn(
  buffers: AnySpanBuffer[],
  totalRows: number,
  fields: string[],
  vectors: Column<unknown>[],
  dictType?: ReturnType<typeof dictionary>,
): void {
  const entryTypeIndices = new Int8Array(totalRows);
  let offset = 0;
  for (const buf of buffers) {
    if (!buf.entry_type) {
      throw new Error(`Buffer missing operations property (_writeIndex: ${buf._writeIndex})`);
    }
    entryTypeIndices.set(buf.entry_type.subarray(0, buf._writeIndex), offset);
    offset += buf._writeIndex;
  }
  const entryTypeDictData = buildData({
    type: utf8(),
    offset: 0,
    length: ENTRY_TYPE_NAMES.length,
    nullCount: 0,
    valueOffsets: calculateUtf8Offsets(ENTRY_TYPE_NAMES),
    data: encodeUtf8Strings(ENTRY_TYPE_NAMES),
  });
  fields.push('entry_type');
  vectors.push(
    buildColumn(
      buildData({
        type: dictType ?? dictionary(utf8(), int8()),
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: entryTypeIndices,
        dictionary: buildColumn(entryTypeDictData),
      }),
    ),
  );
}

/**
 * Build a per-buffer metadata dictionary column (package_name, package_file, git_sha).
 * Row 0 uses callsiteMetadata, rows 1+ use opMetadata (per context flow spec).
 */
function buildPerBufferDictColumn(
  buffers: AnySpanBuffer[],
  totalRows: number,
  fieldName: string,
  metadataGetter: (m: OpMetadata) => string,
  dictType: ReturnType<typeof dictionary>,
  dictValues: string[],
  dictMap: Map<string, number>,
  dictUtf8Offsets: Int32Array,
  dictUtf8Data: Uint8Array,
  fields: string[],
  vectors: Column<unknown>[],
): void {
  const uniqueCount = dictValues.length;
  const IndexArrayCtor = uniqueCount <= 255 ? Uint8Array : uniqueCount <= 65535 ? Uint16Array : Uint32Array;
  const indices = new IndexArrayCtor(totalRows);
  let offset = 0;
  for (const buf of buffers) {
    const callsiteValue = metadataGetter(buf._callsiteMetadata ?? buf._opMetadata);
    const callsiteIdx = dictMap.get(callsiteValue) ?? 0;
    const opIdx = dictMap.get(metadataGetter(buf._opMetadata)) ?? 0;
    indices[offset] = callsiteIdx;
    if (buf._writeIndex > 1) {
      indices.fill(opIdx, offset + 1, offset + buf._writeIndex);
    }
    offset += buf._writeIndex;
  }
  fields.push(fieldName);
  vectors.push(
    buildColumn(
      buildData({
        type: dictType,
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: indices,
        dictionary: buildColumn(
          buildData({
            type: utf8(),
            offset: 0,
            length: dictValues.length,
            nullCount: 0,
            valueOffsets: dictUtf8Offsets,
            data: dictUtf8Data,
          }),
        ),
      }),
    ),
  );
}

/**
 * Build a binary Arrow column from SpanBuffer data.
 * Shared by both convertBuffersToTable and convertBuffersWithSharedDicts.
 *
 * Binary columns store arbitrary bytes (raw Uint8Array or encoder-encoded objects).
 * At flush time, the encoder (if present) transforms stored values into Uint8Array.
 */
function buildBinaryColumnFromBuffers(
  buffers: AnySpanBuffer[],
  columnName: string,
  totalRows: number,
  fieldSchema: unknown,
): { column: Column<unknown>; nullCount: number } {
  const encoder = getBinaryEncoder(fieldSchema);

  // Collect all values from buffers, encode each if encoder is present
  const allValues: (Uint8Array | null)[] = [];
  for (const buf of buffers) {
    const col = buf.getColumnIfAllocated(columnName) as unknown[] | undefined;
    const srcNulls = buf.getNullsIfAllocated(columnName);
    for (let i = 0; i < buf._writeIndex; i++) {
      if (srcNulls) {
        const isValid = (srcNulls[i >>> 3] & (1 << (i & 7))) !== 0;
        if (isValid && col) {
          const raw = col[i];
          allValues.push(encoder ? encoder.encode(raw) : (raw as Uint8Array));
        } else {
          allValues.push(null);
        }
      } else if (col) {
        // No null bitmap -- column allocated but check for undefined/null values
        const raw = col[i];
        if (raw != null) {
          allValues.push(encoder ? encoder.encode(raw) : (raw as Uint8Array));
        } else {
          allValues.push(null);
        }
      } else {
        allValues.push(null);
      }
    }
  }

  // Build offsets and concatenated data buffer
  const offsets = new Int32Array(totalRows + 1);
  let dataLength = 0;
  for (let i = 0; i < allValues.length; i++) {
    offsets[i] = dataLength;
    const value = allValues[i];
    if (value !== null) {
      dataLength += value.length;
    }
  }
  offsets[allValues.length] = dataLength;

  const data = new Uint8Array(dataLength);
  let dataOffset = 0;
  let nullCount = 0;
  const nullBitmapSize = Math.ceil(totalRows / 8);
  const nullBitmap = new Uint8Array(nullBitmapSize);
  for (let i = 0; i < allValues.length; i++) {
    const val = allValues[i];
    if (val !== null) {
      data.set(val, dataOffset);
      dataOffset += val.length;
      // Mark valid in null bitmap
      nullBitmap[i >>> 3] |= 1 << (i & 7);
    } else {
      nullCount++;
    }
  }

  // Build Arrow Binary column via flechette
  const batch = new (batchType(BINARY_TYPE))({
    type: BINARY_TYPE,
    length: totalRows,
    nullCount,
    validity: nullCount > 0 ? nullBitmap : EMPTY_VALIDITY,
    offsets,
    values: data,
  });
  return { column: new Column([batch]), nullCount };
}

/**
 * Convert SpanBuffer (and its overflow chain) to an Arrow table.
 *
 * **Dictionary handling**: Each table batch has its own dictionary data.
 * Dictionaries are built from the data in this batch only and are not shared
 * with other batches, even when combined into a Table.
 */
export function convertToTable(buffer: AnySpanBuffer, systemColumnBuilder?: SystemColumnBuilder): Table {
  const buffers: AnySpanBuffer[] = [];
  let currentBuffer: AnySpanBuffer | undefined = buffer;

  while (currentBuffer) {
    buffers.push(currentBuffer);
    currentBuffer = currentBuffer._overflow;
  }

  return convertBuffersToTable(buffers, systemColumnBuilder);
}

/**
 * Convert multiple buffers to a single table.
 *
 * **Dictionary handling**: Each table batch has its own dictionary data.
 * Dictionaries are built from the data in this batch only and are not shared
 * with other batches, even when combined into a Table.
 */
function convertBuffersToTable(buffers: AnySpanBuffer[], systemColumnBuilder?: SystemColumnBuilder): Table {
  if (buffers.length === 0) return tableFromColumns({});

  const totalRows = buffers.reduce((sum, buf) => sum + buf._writeIndex, 0);
  if (totalRows === 0) return tableFromColumns({});

  const schema: LogSchema = buffers[0]._logSchema;

  // Build vectors first, then derive schema from them
  // This ensures Field types and vector data types are identical (Arrow IPC requirement)
  const fields: string[] = [];
  const vectors: Column<unknown>[] = [];

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
    let {
      dictionary: dictValues,
      indices,
      arrowIndexType,
      nullBitmap,
    } = buildSortedCategoryDictionary(buffers, 'message', maskTransform);
    // Ensure dictionary has at least one entry (empty string) if no messages were written
    if (dictValues.length === 0) {
      dictValues = [''];
    }
    // Ensure nullBitmap is all 1s (all valid) for eager column
    if (!nullBitmap) {
      const bitmapBytes = Math.ceil(totalRows / 8);
      nullBitmap = new Uint8Array(bitmapBytes);
      nullBitmap.fill(0xff);
    }

    // CRITICAL: Create ONE Dictionary type instance for field and batch data
    // Arrow Schema validates that fields with same dictionary ID have identical type references
    const messageDictType = dictionary(utf8(), arrowIndexType);
    fields.push('message');

    const { data: messageUtf8Data, offsets: messageUtf8Offsets } = globalUtf8Cache.encodeMany(dictValues);

    const messageDictData = buildData({
      type: utf8(),
      offset: 0,
      length: dictValues.length,
      nullCount: 0,
      valueOffsets: messageUtf8Offsets,
      data: messageUtf8Data,
    });

    const messageData = buildData({
      type: messageDictType,
      offset: 0,
      length: totalRows,
      nullCount: 0, // message is eager, never null
      data: indices,
      nullBitmap, // Always include nullBitmap for message (even though nullCount is 0)
      dictionary: buildColumn(messageDictData),
    });

    vectors.push(buildColumn(messageData));
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
      const enumSchema = fieldSchema as { __index_array_ctor?: unknown; __arrow_index_type?: unknown };
      const indexArrayCtor =
        (enumSchema.__index_array_ctor as Uint8ArrayConstructor | Uint16ArrayConstructor | Uint32ArrayConstructor) ??
        Uint8Array;
      const arrowIndexType = (enumSchema.__arrow_index_type as IntType) ?? uint8();
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

      // CRITICAL: Create ONE Dictionary type instance for field and batch data
      const enumDictType = dictionary(utf8(), arrowIndexType);
      fields.push(arrowFieldName);

      const enumDictData = buildData({
        type: utf8(),
        offset: 0,
        length: enumValues.length,
        nullCount: 0,
        valueOffsets: enumUtf8?.offsets ?? calculateUtf8Offsets(enumValues as readonly string[]),
        data: enumUtf8?.concatenated ?? encodeUtf8Strings(enumValues as readonly string[]),
      });

      const enumData = buildData({
        type: enumDictType,
        offset: 0,
        length: totalRows,
        nullCount,
        data: allIndices,
        nullBitmap,
        dictionary: buildColumn(enumDictData),
      });

      vectors.push(buildColumn(enumData));
    } else if (lmaoType === 'category') {
      // Get mask transform from schema metadata (if present)
      const maskTransform = getMaskTransform(fieldSchema);
      const {
        dictionary: dictValues,
        indices,
        arrowIndexType,
        nullBitmap,
        nullCount,
      } = buildSortedCategoryDictionary(buffers, columnName, maskTransform);

      // CRITICAL: Create ONE Dictionary type instance for field and batch data
      const categoryDictType = dictionary(utf8(), arrowIndexType);
      fields.push(arrowFieldName);

      const { data: categoryUtf8Data, offsets: categoryUtf8Offsets } = globalUtf8Cache.encodeMany(dictValues);

      const categoryDictData = buildData({
        type: utf8(),
        offset: 0,
        length: dictValues.length,
        nullCount: 0,
        valueOffsets: categoryUtf8Offsets,
        data: categoryUtf8Data,
      });

      const categoryData = buildData({
        type: categoryDictType,
        offset: 0,
        length: totalRows,
        nullCount,
        data: indices,
        nullBitmap,
        dictionary: buildColumn(categoryDictData),
      });

      vectors.push(buildColumn(categoryData));
    } else if (lmaoType === 'text') {
      // Get mask transform from schema metadata (if present)
      const maskTransform = getMaskTransform(fieldSchema);
      const result = buildTextDictionary(buffers, columnName, maskTransform);
      if (result) {
        const { dictionary: dictValues, indices, arrowIndexType, nullBitmap, nullCount } = result;

        // CRITICAL: Create ONE Dictionary type instance for field and batch data
        const textDictType = dictionary(utf8(), arrowIndexType);
        fields.push(arrowFieldName);

        const { data: textUtf8Data, offsets: textUtf8Offsets } = globalUtf8Cache.encodeMany(dictValues);

        const textDictData = buildData({
          type: utf8(),
          offset: 0,
          length: dictValues.length,
          nullCount: 0,
          valueOffsets: textUtf8Offsets,
          data: textUtf8Data,
        });

        const textData = buildData({
          type: textDictType,
          offset: 0,
          length: totalRows,
          nullCount,
          data: indices,
          nullBitmap,
          dictionary: buildColumn(textDictData),
        });

        vectors.push(buildColumn(textData));
      }
    } else if (lmaoType === 'number') {
      const valueArrays: Float64Array[] = [];

      for (const buf of buffers) {
        const column = buf.getColumnIfAllocated(columnName);
        if (column && column instanceof Float64Array) {
          valueArrays.push(column.subarray(0, buf._writeIndex));
        } else {
          valueArrays.push(new F64Array(buf._writeIndex));
        }
      }

      const allValues = concatenateFloat64Arrays(valueArrays);
      const { nullBitmap, nullCount } = concatenateNullBitmaps(buffers, columnName);

      const numberType = float64();
      fields.push(arrowFieldName);

      const numberData = buildData({
        type: numberType,
        offset: 0,
        length: totalRows,
        nullCount,
        data: allValues,
        nullBitmap,
      });

      vectors.push(buildColumn(numberData));
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

      const boolType = bool();
      fields.push(arrowFieldName);

      const boolData = buildData({
        type: boolType,
        offset: 0,
        length: totalRows,
        nullCount,
        data: allValues,
        nullBitmap,
      });

      vectors.push(buildColumn(boolData));
    } else if (lmaoType === 'binary') {
      // Binary columns: raw Uint8Array or encoder-wrapped values (e.g. msgpack)
      const { column } = buildBinaryColumnFromBuffers(buffers, columnName, totalRows, fieldSchema);
      fields.push(arrowFieldName);
      vectors.push(column);
    }
  }

  return buildTable(fields, vectors);
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
): { fields: string[]; vectors: Column<unknown>[] } {
  const fields: string[] = [];
  const vectors: Column<unknown>[] = [];

  // Column order: timestamp, trace_id, thread_id, span_id, parent_thread_id, parent_span_id,
  //               entry_type, package_name, package_file, git_sha

  // 1. Timestamp
  buildTimestampColumn(buffers, totalRows, fields, vectors);

  // 2. Trace ID dictionary (built inline from buffers)
  const traceIdSet = new Set<string>();
  for (const buf of buffers) traceIdSet.add(buf.trace_id);
  const traceIdArray = Array.from(traceIdSet);
  const traceIdMap = new Map(traceIdArray.map((id, idx) => [id, idx]));
  const traceIdArrowIndexType = indexTypeForCount(traceIdArray.length);
  const TraceIdIndexArrayCtor =
    traceIdArray.length <= 255 ? Uint8Array : traceIdArray.length <= 65535 ? Uint16Array : Uint32Array;

  const traceIdIndices = new TraceIdIndexArrayCtor(totalRows);
  let rowOffset = 0;
  for (const buf of buffers) {
    const traceIdIndex = traceIdMap.get(buf.trace_id);
    if (traceIdIndex === undefined) {
      throw new Error(`TraceId index not found for: ${buf.trace_id}`);
    }
    traceIdIndices.fill(traceIdIndex, rowOffset, rowOffset + buf._writeIndex);
    rowOffset += buf._writeIndex;
  }
  const traceIdDictType = dictionary(utf8(), traceIdArrowIndexType);
  fields.push('trace_id');
  vectors.push(
    buildColumn(
      buildData({
        type: traceIdDictType,
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: traceIdIndices,
        dictionary: buildColumn(
          buildData({
            type: utf8(),
            offset: 0,
            length: traceIdArray.length,
            nullCount: 0,
            valueOffsets: calculateUtf8Offsets(traceIdArray),
            data: encodeUtf8Strings(traceIdArray),
          }),
        ),
      }),
    ),
  );

  // 3-6. thread_id, span_id, parent_thread_id, parent_span_id
  buildNonDictIdentityColumns(buffers, totalRows, fields, vectors);

  // 7. Entry type
  buildEntryTypeColumn(buffers, totalRows, fields, vectors);

  // 8-10. Per-buffer metadata columns (package_name, package_file, git_sha)
  for (const [fieldName, metadataGetter] of [
    ['package_name', (m: OpMetadata) => m.package_name],
    ['package_file', (m: OpMetadata) => m.package_file],
    ['git_sha', (m: OpMetadata) => m.git_sha],
  ] as [string, (m: OpMetadata) => string][]) {
    const valueSet = new Set<string>();
    for (const buf of buffers) {
      valueSet.add(metadataGetter(buf._opMetadata));
      if (buf._callsiteMetadata) valueSet.add(metadataGetter(buf._callsiteMetadata));
    }
    const dictValues = Array.from(valueSet);
    const dictMap = new Map(dictValues.map((v, i) => [v, i]));
    const arrowIndexType = indexTypeForCount(dictValues.length);
    const dictType = dictionary(utf8(), arrowIndexType);

    buildPerBufferDictColumn(
      buffers,
      totalRows,
      fieldName,
      metadataGetter,
      dictType,
      dictValues,
      dictMap,
      calculateUtf8Offsets(dictValues),
      encodeUtf8Strings(dictValues),
      fields,
      vectors,
    );
  }

  return { fields, vectors };
}

/**
 * Convert SpanBuffer (and its overflow chain) to Arrow Table
 */
export function convertToArrowTable<_T extends LogSchema = LogSchema>(
  buffer: AnySpanBuffer,
  systemColumnBuilder?: SystemColumnBuilder,
): Table {
  const batch = convertToTable(buffer, systemColumnBuilder);
  if (batch.numRows === 0) return tableFromColumns({});
  return batch;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tree Conversion with Shared Dictionaries
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Convert SpanBuffer tree to Arrow Table
 *
 * Two-pass conversion with shared dictionaries:
 * - Pass 1: Walk tree, build dictionaries (collect unique strings, cache UTF-8)
 * - Pass 2: Walk tree, convert each buffer to a table using shared dictionaries
 */
export function convertSpanTreeToArrowTable(
  rootBuffer: AnySpanBuffer,
  _systemColumnBuilder?: SystemColumnBuilder,
  modulesToLogStats?: CapacityStatsEntry[],
  periodStartNs?: bigint,
): Table {
  // ═══════════════════════════════════════════════════════════════════════════
  // PASS 0: Collect ALL unique schema fields from ALL buffers in the tree
  // Per specs/lmao/01k_tree_walker_and_arrow_conversion.md - child spans may have different schemas
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
      categoryBuilders.set(fieldName, new DictBuilder(globalUtf8Cache));
      categoryMaskTransforms.set(fieldName, getMaskTransform(fieldSchema));
      categoryOriginalToMasked.set(fieldName, new Map());
    }
    if (lmaoType === 'text') {
      textBuilders.set(fieldName, new DictBuilder(globalUtf8Cache));
      textMaskTransforms.set(fieldName, getMaskTransform(fieldSchema));
      textOriginalToMasked.set(fieldName, new Map());
    }
  }

  // System attribute column: message (eager category, not in user schema)
  // message is always present (eager allocation), so we need to handle it explicitly
  categoryBuilders.set('message', new DictBuilder(globalUtf8Cache));
  categoryMaskTransforms.set('message', undefined); // message has no masking
  categoryOriginalToMasked.set('message', new Map());

  // Metadata column dictionaries
  // For traceId (metadata), use DictionaryBuilder (values not pre-encoded)
  const traceIdBuilder = new DictBuilder(globalUtf8Cache);

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
      // Per specs/lmao/01i_span_scope_attributes.md: scope values fill NULL cells at Arrow conversion
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
      // Per specs/lmao/01i_span_scope_attributes.md: scope values fill NULL cells at Arrow conversion
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
    return tableFromColumns({});
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
    const messageBuilder = new DictBuilder(globalUtf8Cache);
    messageBuilder.add(''); // Single empty string entry
    messageDict = messageBuilder.finalize(true); // sorted
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // PASS 2: Collect all buffers with non-zero rows
  // ═══════════════════════════════════════════════════════════════════════════
  const allBuffers: AnySpanBuffer[] = [];

  walkSpanTree(rootBuffer, (buffer) => {
    if (buffer._writeIndex > 0) {
      allBuffers.push(buffer);
    }
  });

  // Build a single table from all buffers
  const batch =
    allBuffers.length > 0
      ? convertBuffersWithSharedDicts(
          allBuffers,
          traceIdDict,
          packageDict,
          packagePathDict,
          gitShaDict,
          categoryDicts,
          textDicts,
          schemaFields,
          categoryOriginalToMasked,
          textOriginalToMasked,
        )
      : undefined;

  // Create capacity stats table if needed
  if (modulesToLogStats && modulesToLogStats.length > 0) {
    // periodStartNs must be provided by the caller (from FlushScheduler's tracked period start)
    // If not provided, use 0n as a fallback (indicates period tracking not enabled)
    const effectivePeriodStartNs = periodStartNs ?? 0n;
    const capacityStatsBatch = createCapacityStatsTable(modulesToLogStats, effectivePeriodStartNs, mergedSchema);
    if (batch) {
      return appendTables(batch, capacityStatsBatch);
    }
    return capacityStatsBatch;
  }

  if (batch) {
    return batch;
  }
  return tableFromColumns({});
}

/**
 * Convert multiple buffers to a single table using pre-built dictionaries.
 *
 * **Dictionary handling**: Each table batch has its own dictionary data.
 * While dictionaries are built in a first pass from all buffers, they are used to create
 * a single table. The resulting batches contain their own dictionary data and
 * are not shared with other batches, even when combined into a Table.
 */
function convertBuffersWithSharedDicts(
  buffers: AnySpanBuffer[],
  traceIdDict: FinalizedDictionary,
  packageDict: FinalizedDictionary,
  packagePathDict: FinalizedDictionary,
  gitShaDict: FinalizedDictionary,
  categoryDicts: Map<string, FinalizedDictionary>,
  textDicts: Map<string, FinalizedDictionary>,
  schemaFields: Array<[string, unknown]>,
  categoryOriginalToMasked: Map<string, Map<string, string>>,
  textOriginalToMasked: Map<string, Map<string, string>>,
): Table {
  const totalRows = buffers.reduce((sum, buf) => sum + buf._writeIndex, 0);
  const fields: string[] = [];
  const vectors: Column<unknown>[] = [];

  // Dictionary types with explicit IDs for IPC serialization
  const traceIdType = dictionary(utf8(), traceIdDict.arrowIndexType, false, 0);
  const entryTypeDictType = dictionary(utf8(), int8(), false, 1);
  const packageType = dictionary(utf8(), packageDict.arrowIndexType, false, 2);
  const packagePathType = dictionary(utf8(), packagePathDict.arrowIndexType, false, 3);
  const gitShaType = dictionary(utf8(), gitShaDict.arrowIndexType, false, 4);

  // Column order: timestamp, trace_id, thread_id, span_id, parent_thread_id, parent_span_id,
  //               entry_type, package_name, package_file, git_sha

  // 1. Timestamp
  buildTimestampColumn(buffers, totalRows, fields, vectors);

  // 2. Trace ID (using shared pre-built dictionary)
  const traceIdIndices = new traceIdDict.indexArrayCtor(totalRows);
  let offset = 0;
  for (const buf of buffers) {
    const idx = traceIdDict.indexMap.get(buf.trace_id) ?? 0;
    traceIdIndices.fill(idx, offset, offset + buf._writeIndex);
    offset += buf._writeIndex;
  }
  fields.push('trace_id');
  vectors.push(
    buildColumn(
      buildData({
        type: traceIdType,
        offset: 0,
        length: totalRows,
        nullCount: 0,
        data: traceIdIndices,
        dictionary: buildColumn(
          buildData({
            type: utf8(),
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

  // 3-6. thread_id, span_id, parent_thread_id, parent_span_id
  buildNonDictIdentityColumns(buffers, totalRows, fields, vectors);

  // 7. Entry type (with explicit dictionary ID for IPC)
  buildEntryTypeColumn(buffers, totalRows, fields, vectors, entryTypeDictType);

  // 8-10. Per-buffer metadata columns using shared pre-built dictionaries
  // Per specs/lmao/01c_context_flow_and_op_wrappers.md:
  // - Row 0 (span-start): uses callsiteModule for gitSha/packageName/packagePath
  // - Rows 1+ (logs, span-end): uses task._module
  for (const [fieldName, metadataGetter, dictType, dict] of [
    ['package_name', (m: OpMetadata) => m.package_name, packageType, packageDict],
    ['package_file', (m: OpMetadata) => m.package_file, packagePathType, packagePathDict],
    ['git_sha', (m: OpMetadata) => m.git_sha, gitShaType, gitShaDict],
  ] as [string, (m: OpMetadata) => string, ReturnType<typeof dictionary>, FinalizedDictionary][]) {
    const dictValues = Array.from(dict.indexMap.keys());
    buildPerBufferDictColumn(
      buffers,
      totalRows,
      fieldName,
      metadataGetter,
      dictType,
      dictValues,
      dict.indexMap,
      dict.offsets,
      dict.data,
      fields,
      vectors,
    );
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // System attribute and user attribute columns - concatenate from all buffers
  // ═══════════════════════════════════════════════════════════════════════════

  // System attribute column: message (at fixed position 10, before user attributes)
  const messageDict = categoryDicts.get('message');
  const messageOriginalToMasked = categoryOriginalToMasked.get('message');
  if (!messageDict || !messageOriginalToMasked) {
    throw new Error('Message dictionary or mapping not found');
  }
  const messageFieldType = dictionary(utf8(), messageDict.arrowIndexType, false, 5);
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
    buildColumn(
      buildData({
        type: messageFieldType,
        offset: 0,
        length: totalRows,
        nullCount: 0, // message is eager, never null
        data: messageIndices,
        nullBitmap: messageNullBitmap,
        dictionary: buildColumn(
          buildData({
            type: utf8(),
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
  fields.push('message');

  // System attribute column: uint64_value (for buffer metrics - op durations, counts, etc.)
  // For span data, this is all null - only buffer metrics use it
  const uint64ValueNullBitmap = new Uint8Array(Math.ceil(totalRows / 8));
  // All zeros = all null (Arrow null bitmap: 1 = valid, 0 = null)
  vectors.push(
    buildColumn(
      buildData({
        type: uint64(),
        offset: 0,
        length: totalRows,
        nullCount: totalRows,
        data: new BigUint64Array(totalRows),
        nullBitmap: uint64ValueNullBitmap,
      }),
    ),
  );
  fields.push('uint64_value');

  for (const [fieldName, fieldSchema] of schemaFields) {
    const lmaoType = getSchemaType(fieldSchema);
    const columnName = fieldName; // User columns have no prefix
    const arrowFieldName = getArrowFieldName(fieldName);

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

      // Per specs/lmao/01i_span_scope_attributes.md: scope values fill NULL cells at Arrow conversion
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
        buildColumn(
          buildData({
            type: dictionary(utf8(), dict.arrowIndexType),
            offset: 0,
            length: totalRows,
            nullCount,
            data: indices,
            nullBitmap: nullCount > 0 ? nullBitmap : undefined,
            dictionary: buildColumn(
              buildData({
                type: utf8(),
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
      fields.push(arrowFieldName);
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
        buildColumn(
          buildData({
            type: dictionary(utf8(), dict.arrowIndexType),
            offset: 0,
            length: totalRows,
            nullCount,
            data: indices,
            nullBitmap: nullCount > 0 ? nullBitmap : undefined,
            dictionary: buildColumn(
              buildData({
                type: utf8(),
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
      fields.push(arrowFieldName);
    } else if (lmaoType === 'number') {
      const allValues = new F64Array(totalRows);
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
        buildColumn(
          buildData({
            type: float64(),
            offset: 0,
            length: totalRows,
            nullCount,
            data: allValues,
            nullBitmap: nullCount > 0 ? nullBitmap : undefined,
          }),
        ),
      );
      fields.push(arrowFieldName);
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
        buildColumn(
          buildData({
            type: bool(),
            offset: 0,
            length: totalRows,
            nullCount,
            data: allValues,
            nullBitmap: nullCount > 0 ? nullBitmap : undefined,
          }),
        ),
      );
      fields.push(arrowFieldName);
    } else if (lmaoType === 'enum') {
      const enumValues = getEnumValues(fieldSchema) || [];
      const enumUtf8 = getEnumUtf8(fieldSchema);
      // Get constructors from schema metadata
      const enumSchema = fieldSchema as { __index_array_ctor?: unknown; __arrow_index_type?: unknown };
      const indexArrayCtor =
        (enumSchema.__index_array_ctor as Uint8ArrayConstructor | Uint16ArrayConstructor | Uint32ArrayConstructor) ??
        Uint8Array;
      const arrowIndexType = (enumSchema.__arrow_index_type as IntType) ?? uint8();
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

      const enumDictData = buildData({
        type: utf8(),
        offset: 0,
        length: enumValues.length,
        nullCount: 0,
        valueOffsets: enumUtf8?.offsets ?? calculateUtf8Offsets(enumValues as readonly string[]),
        data: enumUtf8?.concatenated ?? encodeUtf8Strings(enumValues as readonly string[]),
      });

      vectors.push(
        buildColumn(
          buildData({
            type: dictionary(utf8(), arrowIndexType),
            offset: 0,
            length: totalRows,
            nullCount,
            data: allIndices,
            nullBitmap: nullCount > 0 ? nullBitmap : undefined,
            dictionary: buildColumn(enumDictData),
          }),
        ),
      );
      fields.push(arrowFieldName);
    } else if (lmaoType === 'binary') {
      // Binary columns: raw Uint8Array or encoder-wrapped values (e.g. msgpack)
      // Binary columns don't use dictionaries and are not scope-fillable
      const { column } = buildBinaryColumnFromBuffers(buffers, columnName, totalRows, fieldSchema);
      fields.push(arrowFieldName);
      vectors.push(column);
    }
  }

  return buildTable(fields, vectors);
}
