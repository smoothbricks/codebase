/**
 * Capacity statistics RecordBatch creation
 */

import {
  compareStrings,
  createSortedDictionary,
  DictionaryBuilder,
  type PreEncodedEntry,
  sortInPlace,
} from '@smoothbricks/arrow-builder';
import {
  type Bool,
  Dictionary,
  Field,
  type Float64,
  Int8,
  makeData,
  makeVector,
  RecordBatch,
  Schema,
  Struct,
  TimestampNanosecond,
  Uint8,
  type Uint16,
  Uint32,
  Uint64,
  Utf8,
  type Vector,
} from 'apache-arrow';
import { ENTRY_TYPE_CAPACITY_STATS, ENTRY_TYPE_NAMES } from '../lmao.js';
import type { ModuleContext } from '../moduleContext.js';
import { getEnumUtf8, getEnumValues, getSchemaType } from '../schema/typeGuards.js';
import { globalUtf8Cache } from '../utf8Cache.js';
import { calculateUtf8Offsets, encodeUtf8Strings } from './utils.js';

/**
 * Create a RecordBatch with capacity stats entries for the given modules.
 * Each module gets one row with its capacity statistics serialized as JSON in the message column.
 *
 * **Dictionary handling**: Each RecordBatch has its own dictionary data.
 * This function builds dictionaries containing only the modules that have capacity stats,
 * not all modules from the span tree. This ensures the RecordBatch only includes dictionary
 * entries for data it actually uses, avoiding unnecessary data duplication.
 *
 * **Schema handling**: If `hasSpanData` is false (no span data in the tree), builds a minimal
 * schema with only the columns needed for capacity stats:
 * - timestamp, entry_type, package_name, package_path, git_sha, message (if in schema)
 * - NO trace_id, thread_id, span_id, parent_thread_id, parent_span_id
 * - NO custom attribute columns (except message)
 *
 * If `hasSpanData` is true, uses the full `arrowSchema` so RecordBatches can be combined.
 */
export function createCapacityStatsRecordBatch(
  modulesToLogStats: Set<ModuleContext>,
  arrowSchema: Schema,
  lmaoSchema: Record<string, unknown>,
  hasSpanData = true,
): RecordBatch {
  const capacityStatsRows = modulesToLogStats.size;
  if (capacityStatsRows === 0) {
    return new RecordBatch({});
  }

  const modulesArray = Array.from(modulesToLogStats);
  const vectors: Vector[] = [];

  // Build dictionaries containing only modules with capacity stats
  // This ensures the RecordBatch only includes dictionary entries it actually uses
  const moduleCount = modulesArray.length;
  const packageEntries: PreEncodedEntry[] = new Array(moduleCount);
  const packagePathEntries: PreEncodedEntry[] = new Array(moduleCount);
  const gitShaEntries: PreEncodedEntry[] = new Array(moduleCount);
  for (let i = 0; i < moduleCount; i++) {
    const module = modulesArray[i];
    packageEntries[i] = module.packageEntry;
    packagePathEntries[i] = module.packagePathEntry;
    gitShaEntries[i] = module.gitShaEntry;
  }

  const cmp = (a: PreEncodedEntry, b: PreEncodedEntry) => compareStrings(a.str, b.str);
  const capacityStatsPackageDict = createSortedDictionary(sortInPlace(packageEntries, cmp));
  const capacityStatsPackagePathDict = createSortedDictionary(sortInPlace(packagePathEntries, cmp));
  const capacityStatsGitShaDict = createSortedDictionary(sortInPlace(gitShaEntries, cmp));

  // Create minimal dictionaries for metadata columns that don't have meaningful values
  // Trace ID (metadata column) - capacity stats aren't part of any trace
  const traceIdBuilder = new DictionaryBuilder(globalUtf8Cache);
  traceIdBuilder.add(''); // Single empty string entry
  const capacityStatsTraceIdDict = traceIdBuilder.finalize(false);

  // Build schema: minimal if no span data, full if combining with span data
  let capacityStatsSchema: Schema;
  let timestampType: TimestampNanosecond;
  let traceIdType: Dictionary<Utf8> | undefined;
  let entryTypeType: Dictionary<Utf8, Int8>;
  let packageType: Dictionary<Utf8>;
  let packagePathType: Dictionary<Utf8>;
  let gitShaType: Dictionary<Utf8>;
  let messageFieldIndex: number | undefined;

  if (hasSpanData) {
    // We'll create a new schema from the vectors after they're built to ensure dictionary IDs match
    // For now, store reference to arrowSchema for field order and types
    capacityStatsSchema = arrowSchema;
    timestampType = arrowSchema.fields[0].type as TimestampNanosecond;
    traceIdType = arrowSchema.fields[1].type as Dictionary<Utf8>;
    entryTypeType = arrowSchema.fields[6].type as Dictionary<Utf8, Int8>;
    packageType = arrowSchema.fields[7].type as Dictionary<Utf8>;
    packagePathType = arrowSchema.fields[8].type as Dictionary<Utf8>;
    gitShaType = arrowSchema.fields[9].type as Dictionary<Utf8>;
    // Find message field index
    for (let i = 0; i < arrowSchema.fields.length; i++) {
      if (arrowSchema.fields[i].name === 'message' || arrowSchema.fields[i].name === 'attr_message') {
        messageFieldIndex = i;
        break;
      }
    }
  } else {
    // Build minimal schema: only columns needed for capacity stats
    const fields: Field[] = [];
    fields.push(Field.new({ name: 'timestamp', type: new TimestampNanosecond() }));
    fields.push(Field.new({ name: 'entry_type', type: new Dictionary(new Utf8(), new Int8()) }));
    fields.push(
      Field.new({
        name: 'package_name',
        type: new Dictionary(new Utf8(), new capacityStatsPackageDict.arrowIndexTypeCtor()),
      }),
    );
    fields.push(
      Field.new({
        name: 'package_path',
        type: new Dictionary(new Utf8(), new capacityStatsPackagePathDict.arrowIndexTypeCtor()),
      }),
    );
    fields.push(
      Field.new({
        name: 'git_sha',
        type: new Dictionary(new Utf8(), new capacityStatsGitShaDict.arrowIndexTypeCtor()),
      }),
    );

    // Message is always included as a system column (even if not in user schema)
    // Message will be dictionary-encoded, but we'll determine the type after building the dict
    // For now, use Uint8 as placeholder (will be updated after building messageDict)
    fields.push(Field.new({ name: 'message', type: new Dictionary(new Utf8(), new Uint8()) }));
    messageFieldIndex = fields.length - 1;

    capacityStatsSchema = new Schema(fields);
    timestampType = new TimestampNanosecond();
    entryTypeType = new Dictionary(new Utf8(), new Int8());
    packageType = new Dictionary(new Utf8(), new capacityStatsPackageDict.arrowIndexTypeCtor());
    packagePathType = new Dictionary(new Utf8(), new capacityStatsPackagePathDict.arrowIndexTypeCtor());
    gitShaType = new Dictionary(new Utf8(), new capacityStatsGitShaDict.arrowIndexTypeCtor());
  }

  // Build capacity stats JSON strings
  // Create a separate dictionary just for capacity stats messages
  // This doesn't pollute the main message dictionary cache
  const capacityStatsMessages: string[] = [];
  const nowNs = BigInt(Date.now()) * 1_000_000n;

  for (const module of modulesArray) {
    const stats = module.spanBufferCapacityStats;
    const efficiency = stats.totalWrites / (stats.totalBuffersCreated * stats.currentCapacity);
    const overflowRatio = stats.totalWrites > 0 ? stats.overflowWrites / stats.totalWrites : 0;

    const statsJson = JSON.stringify({
      currentCapacity: stats.currentCapacity,
      totalWrites: stats.totalWrites,
      overflowWrites: stats.overflowWrites,
      totalBuffers: stats.totalBuffersCreated,
      efficiency,
      overflowRatio,
    });

    capacityStatsMessages.push(statsJson);
  }

  // Create a separate dictionary just for capacity stats messages
  // This doesn't pollute the main message dictionary cache
  const capacityStatsMessageBuilder = new DictionaryBuilder(globalUtf8Cache);
  for (const msg of capacityStatsMessages) {
    capacityStatsMessageBuilder.add(msg);
  }
  const capacityStatsMessageDict = capacityStatsMessageBuilder.finalize(false); // not sorted

  // Timestamp - all current time
  const timestamps = new BigInt64Array(capacityStatsRows);
  timestamps.fill(nowNs);
  vectors.push(
    makeVector(
      makeData({
        type: timestampType,
        offset: 0,
        length: capacityStatsRows,
        nullCount: 0,
        data: timestamps,
      }),
    ),
  );

  // Span-specific columns: only include if combining with span data
  if (hasSpanData) {
    if (!traceIdType) {
      throw new Error('traceIdType is required when hasSpanData is true');
    }
    // Trace ID - capacity stats aren't part of any trace, use minimal dictionary
    const traceIdIndices = new capacityStatsTraceIdDict.indexArrayCtor(capacityStatsRows);
    traceIdIndices.fill(0); // All point to index 0 (empty string)
    vectors.push(
      makeVector(
        makeData({
          type: traceIdType,
          offset: 0,
          length: capacityStatsRows,
          nullCount: 0,
          data: traceIdIndices,
          dictionary: makeVector(
            makeData({
              type: new Utf8(),
              offset: 0,
              length: capacityStatsTraceIdDict.indexMap.size,
              nullCount: 0,
              valueOffsets: capacityStatsTraceIdDict.offsets,
              data: capacityStatsTraceIdDict.data,
            }),
          ),
        }),
      ),
    );

    // thread_id - all 0
    const threadIds = new BigUint64Array(capacityStatsRows);
    threadIds.fill(0n);
    vectors.push(
      makeVector(
        makeData({
          type: new Uint64(),
          offset: 0,
          length: capacityStatsRows,
          nullCount: 0,
          data: threadIds,
        }),
      ),
    );

    // span_id - all 0
    const spanIds = new Uint32Array(capacityStatsRows);
    spanIds.fill(0);
    vectors.push(
      makeVector(
        makeData({
          type: new Uint32(),
          offset: 0,
          length: capacityStatsRows,
          nullCount: 0,
          data: spanIds,
        }),
      ),
    );

    // parent_thread_id - all null
    const parentThreadIds = new BigUint64Array(capacityStatsRows);
    const parentThreadIdNulls = new Uint8Array(Math.ceil(capacityStatsRows / 8));
    parentThreadIdNulls.fill(0); // All null
    vectors.push(
      makeVector(
        makeData({
          type: new Uint64(),
          offset: 0,
          length: capacityStatsRows,
          nullCount: capacityStatsRows,
          data: parentThreadIds,
          nullBitmap: parentThreadIdNulls,
        }),
      ),
    );

    // parent_span_id - all null
    const parentSpanIds = new Uint32Array(capacityStatsRows);
    const parentSpanIdNulls = new Uint8Array(Math.ceil(capacityStatsRows / 8));
    parentSpanIdNulls.fill(0); // All null
    vectors.push(
      makeVector(
        makeData({
          type: new Uint32(),
          offset: 0,
          length: capacityStatsRows,
          nullCount: capacityStatsRows,
          data: parentSpanIds,
          nullBitmap: parentSpanIdNulls,
        }),
      ),
    );
  }

  // Entry type - all CAPACITY_STATS
  const entryTypeIndices = new Int8Array(capacityStatsRows);
  entryTypeIndices.fill(ENTRY_TYPE_CAPACITY_STATS);
  vectors.push(
    makeVector(
      makeData({
        type: entryTypeType,
        offset: 0,
        length: capacityStatsRows,
        nullCount: 0,
        data: entryTypeIndices,
        dictionary: makeVector(
          makeData({
            type: new Utf8(),
            offset: 0,
            length: ENTRY_TYPE_NAMES.length,
            nullCount: 0,
            valueOffsets: calculateUtf8Offsets(ENTRY_TYPE_NAMES),
            data: encodeUtf8Strings(ENTRY_TYPE_NAMES),
          }),
        ),
      }),
    ),
  );

  // Package name - from modules (using capacity stats dictionary)
  const packageIndices = new capacityStatsPackageDict.indexArrayCtor(capacityStatsRows);
  for (let i = 0; i < capacityStatsRows; i++) {
    const module = modulesArray[i];
    const pkgIdx = capacityStatsPackageDict.indexMap.get(module.packageName) ?? 0;
    packageIndices[i] = pkgIdx;
  }
  vectors.push(
    makeVector(
      makeData({
        type: packageType,
        offset: 0,
        length: capacityStatsRows,
        nullCount: 0,
        data: packageIndices,
        dictionary: makeVector(
          makeData({
            type: new Utf8(),
            offset: 0,
            length: capacityStatsPackageDict.indexMap.size,
            nullCount: 0,
            valueOffsets: capacityStatsPackageDict.offsets,
            data: capacityStatsPackageDict.data,
          }),
        ),
      }),
    ),
  );

  // Package path - from modules (using capacity stats dictionary)
  const packagePathIndices = new capacityStatsPackagePathDict.indexArrayCtor(capacityStatsRows);
  for (let i = 0; i < capacityStatsRows; i++) {
    const module = modulesArray[i];
    const pathIdx = capacityStatsPackagePathDict.indexMap.get(module.packagePath) ?? 0;
    packagePathIndices[i] = pathIdx;
  }
  vectors.push(
    makeVector(
      makeData({
        type: packagePathType,
        offset: 0,
        length: capacityStatsRows,
        nullCount: 0,
        data: packagePathIndices,
        dictionary: makeVector(
          makeData({
            type: new Utf8(),
            offset: 0,
            length: capacityStatsPackagePathDict.indexMap.size,
            nullCount: 0,
            valueOffsets: capacityStatsPackagePathDict.offsets,
            data: capacityStatsPackagePathDict.data,
          }),
        ),
      }),
    ),
  );

  // Git SHA - from modules (using capacity stats dictionary)
  const gitShaIndices = new capacityStatsGitShaDict.indexArrayCtor(capacityStatsRows);
  for (let i = 0; i < capacityStatsRows; i++) {
    const module = modulesArray[i];
    const idx = capacityStatsGitShaDict.indexMap.get(module.gitSha) ?? 0;
    gitShaIndices[i] = idx;
  }
  vectors.push(
    makeVector(
      makeData({
        type: gitShaType,
        offset: 0,
        length: capacityStatsRows,
        nullCount: 0,
        data: gitShaIndices,
        dictionary: makeVector(
          makeData({
            type: new Utf8(),
            offset: 0,
            length: capacityStatsGitShaDict.indexMap.size,
            nullCount: 0,
            valueOffsets: capacityStatsGitShaDict.offsets,
            data: capacityStatsGitShaDict.data,
          }),
        ),
      }),
    ),
  );

  // System attribute column: message (index 10) - handle separately before user attributes
  if (hasSpanData && messageFieldIndex !== undefined) {
    const messageField = arrowSchema.fields[messageFieldIndex];
    if (messageField && messageField.name === 'message') {
      const messageFieldType = messageField.type as Dictionary<Utf8, Uint8 | Uint16 | Uint32>;
      const indices = new capacityStatsMessageDict.indexArrayCtor(capacityStatsRows);
      for (let i = 0; i < capacityStatsRows; i++) {
        indices[i] = capacityStatsMessageDict.indexMap.get(capacityStatsMessages[i]) ?? 0;
      }

      vectors.push(
        makeVector(
          makeData({
            type: messageFieldType,
            offset: 0,
            length: capacityStatsRows,
            nullCount: 0,
            data: indices,
            dictionary: makeVector(
              makeData({
                type: new Utf8(),
                offset: 0,
                length: capacityStatsMessageDict.indexMap.size,
                nullCount: 0,
                valueOffsets: capacityStatsMessageDict.offsets,
                data: capacityStatsMessageDict.data,
              }),
            ),
          }),
        ),
      );
    }
  }

  // User attribute columns - only include if combining with span data
  if (hasSpanData) {
    // Iterate through attribute columns that actually exist in the span RecordBatch
    // (lazy columns that were never written to won't be in arrowSchema)
    // Schema order: Core system + metadata columns + system attribute columns:
    //   Core system: timestamp(0), entry_type(6)
    //   Metadata: trace_id(1), thread_id(2), span_id(3), parent_thread_id(4), parent_span_id(5),
    //             package_name(7), package_path(8), git_sha(9)
    //   System attribute: message(10)
    // Total: 11 columns before user attributes
    const METADATA_AND_SYSTEM_COLUMNS_COUNT = 11;

    // Iterate through arrowSchema fields (starting after metadata+system columns) to get only columns that exist
    for (let i = METADATA_AND_SYSTEM_COLUMNS_COUNT; i < arrowSchema.fields.length; i++) {
      const arrowField = arrowSchema.fields[i];
      const fieldType = arrowField.type;

      // Map arrow field name back to lmao schema field name
      // Arrow field names match lmao names (getArrowFieldName just maps 'logMessage' -> 'message')
      const lmaoFieldName = arrowField.name === 'message' ? 'message' : arrowField.name;

      const fieldSchema = lmaoSchema[lmaoFieldName];
      if (!fieldSchema) {
        // Skip if not in lmaoSchema (shouldn't happen, but defensive)
        continue;
      }

      const lmaoType = getSchemaType(fieldSchema);

      // Skip message as it's already handled above
      if (lmaoFieldName === 'message') {
        continue;
      }

      if (lmaoType === 'category') {
        // Since all values are null, create a minimal dictionary with just one empty string
        // This avoids copying the entire dictionary from the span tree
        const minimalBuilder = new DictionaryBuilder(globalUtf8Cache);
        minimalBuilder.add(''); // Single empty string entry
        const minimalDict = minimalBuilder.finalize(false);

        const indices = new minimalDict.indexArrayCtor(capacityStatsRows);
        indices.fill(0); // All point to index 0 (empty string)
        const nullBitmap = new Uint8Array(Math.ceil(capacityStatsRows / 8));
        nullBitmap.fill(0); // All null
        vectors.push(
          makeVector(
            makeData({
              type: fieldType as Dictionary<Utf8, Uint8 | Uint16 | Uint32>,
              offset: 0,
              length: capacityStatsRows,
              nullCount: capacityStatsRows,
              data: indices,
              nullBitmap,
              dictionary: makeVector(
                makeData({
                  type: new Utf8(),
                  offset: 0,
                  length: minimalDict.indexMap.size,
                  nullCount: 0,
                  valueOffsets: minimalDict.offsets,
                  data: minimalDict.data,
                }),
              ),
            }),
          ),
        );
      } else if (lmaoType === 'text') {
        // Since all values are null, create a minimal dictionary with just one empty string
        // This avoids copying the entire dictionary from the span tree
        const minimalBuilder = new DictionaryBuilder(globalUtf8Cache);
        minimalBuilder.add(''); // Single empty string entry
        const minimalDict = minimalBuilder.finalize(false);

        const indices = new minimalDict.indexArrayCtor(capacityStatsRows);
        indices.fill(0); // All point to index 0 (empty string)
        const nullBitmap = new Uint8Array(Math.ceil(capacityStatsRows / 8));
        nullBitmap.fill(0); // All null
        vectors.push(
          makeVector(
            makeData({
              type: fieldType as Dictionary<Utf8, Uint8 | Uint16 | Uint32>,
              offset: 0,
              length: capacityStatsRows,
              nullCount: capacityStatsRows,
              data: indices,
              nullBitmap,
              dictionary: makeVector(
                makeData({
                  type: new Utf8(),
                  offset: 0,
                  length: minimalDict.indexMap.size,
                  nullCount: 0,
                  valueOffsets: minimalDict.offsets,
                  data: minimalDict.data,
                }),
              ),
            }),
          ),
        );
      } else if (lmaoType === 'number') {
        const allValues = new Float64Array(capacityStatsRows);
        const nullBitmap = new Uint8Array(Math.ceil(capacityStatsRows / 8));
        nullBitmap.fill(0); // All null
        vectors.push(
          makeVector(
            makeData({
              type: fieldType as Float64,
              offset: 0,
              length: capacityStatsRows,
              nullCount: capacityStatsRows,
              data: allValues,
              nullBitmap,
            }),
          ),
        );
      } else if (lmaoType === 'boolean') {
        const allValues = new Uint8Array(Math.ceil(capacityStatsRows / 8));
        const nullBitmap = new Uint8Array(Math.ceil(capacityStatsRows / 8));
        nullBitmap.fill(0); // All null
        vectors.push(
          makeVector(
            makeData({
              type: fieldType as Bool,
              offset: 0,
              length: capacityStatsRows,
              nullCount: capacityStatsRows,
              data: allValues,
              nullBitmap,
            }),
          ),
        );
      } else if (lmaoType === 'enum') {
        const enumValues = getEnumValues(fieldSchema) || [];
        const enumUtf8 = getEnumUtf8(fieldSchema);
        const enumSchema = fieldSchema as { __index_array_ctor?: unknown; __arrow_index_type_ctor?: unknown };
        const indexArrayCtor =
          (enumSchema.__index_array_ctor as Uint8ArrayConstructor | Uint16ArrayConstructor | Uint32ArrayConstructor) ??
          Uint8Array;
        const arrowIndexTypeCtor =
          (enumSchema.__arrow_index_type_ctor as typeof Uint8 | typeof Uint16 | typeof Uint32) ?? Uint8;
        const allIndices = new indexArrayCtor(capacityStatsRows);
        const nullBitmap = new Uint8Array(Math.ceil(capacityStatsRows / 8));
        nullBitmap.fill(0); // All null
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
              length: capacityStatsRows,
              nullCount: capacityStatsRows,
              data: allIndices,
              nullBitmap,
              dictionary: makeVector(enumDictData),
            }),
          ),
        );
      }
    } // end for loop
  } else {
    // Minimal schema: only add message column if it exists
    if (messageFieldIndex !== undefined) {
      const indices = new capacityStatsMessageDict.indexArrayCtor(capacityStatsRows);
      for (let i = 0; i < capacityStatsRows; i++) {
        indices[i] = capacityStatsMessageDict.indexMap.get(capacityStatsMessages[i]) ?? 0;
      }

      // Update schema field type with correct index type
      const messageField = capacityStatsSchema.fields[messageFieldIndex];
      const updatedMessageField = Field.new({
        name: messageField.name,
        type: new Dictionary(new Utf8(), new capacityStatsMessageDict.arrowIndexTypeCtor()),
      });
      capacityStatsSchema = new Schema([
        ...capacityStatsSchema.fields.slice(0, messageFieldIndex),
        updatedMessageField,
        ...capacityStatsSchema.fields.slice(messageFieldIndex + 1),
      ]);

      vectors.push(
        makeVector(
          makeData({
            type: new Dictionary(new Utf8(), new capacityStatsMessageDict.arrowIndexTypeCtor()),
            offset: 0,
            length: capacityStatsRows,
            nullCount: 0,
            data: indices,
            dictionary: makeVector(
              makeData({
                type: new Utf8(),
                offset: 0,
                length: capacityStatsMessageDict.indexMap.size,
                nullCount: 0,
                valueOffsets: capacityStatsMessageDict.offsets,
                data: capacityStatsMessageDict.data,
              }),
            ),
          }),
        ),
      );
    }
  }

  // When hasSpanData is true, rebuild schema from the actual vector types
  // This ensures dictionary IDs match exactly (Apache Arrow requires exact schema equivalence)
  let finalSchema = capacityStatsSchema;
  if (hasSpanData) {
    if (vectors.length !== capacityStatsSchema.fields.length) {
      throw new Error(
        `Vector count (${vectors.length}) doesn't match schema field count (${capacityStatsSchema.fields.length})`,
      );
    }
    // Rebuild schema from the actual vector types to ensure dictionary IDs match
    const fields: Field[] = [];
    for (let i = 0; i < vectors.length; i++) {
      const vector = vectors[i];
      const originalField = capacityStatsSchema.fields[i];
      if (!originalField) {
        throw new Error(`Missing field at index ${i} in capacity stats schema`);
      }
      // Use the actual type from the vector to ensure dictionary IDs match
      const vectorType = vector.type;
      fields.push(Field.new({ name: originalField.name, type: vectorType, nullable: originalField.nullable }));
    }
    finalSchema = new Schema(fields);
  }

  const structData = makeData({
    type: new Struct(finalSchema.fields),
    length: capacityStatsRows,
    nullCount: 0,
    children: vectors.map((v) => v.data[0]),
  });

  return new RecordBatch(finalSchema, structData);
}
