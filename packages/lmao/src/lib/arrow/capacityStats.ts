import {
  bool,
  type Column,
  dictionary,
  float64,
  type IntType,
  int8,
  type Table,
  TimeUnit,
  tableFromColumns,
  timestamp,
  uint8,
  uint32,
  uint64,
  utf8,
} from '@uwdata/flechette';
import { SYSTEM_SCHEMA_FIELD_NAMES } from '../schema/systemSchema.js';
import { getSchemaType } from '../schema/typeGuards.js';
import type { SpanBufferConstructor } from '../spanBuffer.js';
import type { OpMetadata } from '../types.js';
import { hasArrowIndexType, makeArrowColumn } from './flechette.js';
import { getArrowFieldName } from './utils.js';

export interface CapacityStatsEntry {
  bufferClass: SpanBufferConstructor;
  metadata: OpMetadata;
}

const F64Array = Float64Array;

type Utf8Encoded = {
  data: Uint8Array;
  offsets: Int32Array;
};

function encodeUtf8(strings: string[]): Utf8Encoded {
  const enc = new TextEncoder();
  const bytes = strings.map((s) => enc.encode(s));
  const total = bytes.reduce((n, b) => n + b.length, 0);
  const data = new Uint8Array(total);
  const offsets = new Int32Array(strings.length + 1);
  let pos = 0;
  for (let i = 0; i < bytes.length; i++) {
    data.set(bytes[i], pos);
    offsets[i] = pos;
    pos += bytes[i].length;
  }
  offsets[strings.length] = pos;
  return { data, offsets };
}

function indexTypeForCount(count: number): IntType {
  return count <= 255 ? uint8() : uint32();
}

function append(entries: [string, Column<unknown>][], name: string, column: Column<unknown>): void {
  entries.push([name, column]);
}

export function createCapacityStatsTable(
  entriesToLog: CapacityStatsEntry[],
  periodStartNs: bigint,
  mergedSchema: Record<string, unknown>,
): Table {
  if (entriesToLog.length === 0) return tableFromColumns({});

  const rowsPerModule = 4;
  const totalRows = entriesToLog.length * rowsPerModule;

  const entryTypeStrings = ['period-start', 'buffer-writes', 'buffer-spans', 'buffer-capacity'];
  const [periodStartIdx, writesIdx, spansIdx, capacityIdx] = [0, 1, 2, 3];

  const traceIdStrings = [''];
  const packageNameStrings = Array.from(new Set(entriesToLog.map((e) => e.metadata.package_name)));
  const packageFileStrings = Array.from(new Set(entriesToLog.map((e) => e.metadata.package_file)));
  const gitShaStrings = Array.from(new Set(entriesToLog.map((e) => e.metadata.git_sha)));
  const messageStrings = [''];

  const packageNameMap = new Map(packageNameStrings.map((v, i) => [v, i]));
  const packageFileMap = new Map(packageFileStrings.map((v, i) => [v, i]));
  const gitShaMap = new Map(gitShaStrings.map((v, i) => [v, i]));

  const entryTypes = new Int8Array(totalRows);
  const traceIds = new Uint8Array(totalRows);
  const packageNames = new Uint32Array(totalRows);
  const packageFiles = new Uint32Array(totalRows);
  const gitShas = new Uint32Array(totalRows);
  const messages = new Uint8Array(totalRows);

  const timestamps = new BigInt64Array(totalRows);
  const threadIds = new BigUint64Array(totalRows);
  const spanIds = new Uint32Array(totalRows);
  const parentThreadIds = new BigUint64Array(totalRows);
  const parentSpanIds = new Uint32Array(totalRows);
  const uint64Values = new BigUint64Array(totalRows);

  const parentThreadValidity = new Uint8Array(Math.ceil(totalRows / 8));
  const parentSpanValidity = new Uint8Array(Math.ceil(totalRows / 8));

  for (let m = 0; m < entriesToLog.length; m++) {
    const base = m * rowsPerModule;
    const { bufferClass, metadata } = entriesToLog[m];

    const pName = packageNameMap.get(metadata.package_name) ?? 0;
    const pFile = packageFileMap.get(metadata.package_file) ?? 0;
    const gSha = gitShaMap.get(metadata.git_sha) ?? 0;

    const stats = bufferClass.stats;
    const values = [periodStartNs, BigInt(stats.totalWrites), BigInt(stats.spansCreated), BigInt(stats.capacity)];
    const kinds = [periodStartIdx, writesIdx, spansIdx, capacityIdx];

    for (let i = 0; i < rowsPerModule; i++) {
      const row = base + i;
      entryTypes[row] = kinds[i];
      traceIds[row] = 0;
      packageNames[row] = pName;
      packageFiles[row] = pFile;
      gitShas[row] = gSha;
      messages[row] = 0;
      uint64Values[row] = values[i];
      timestamps[row] = periodStartNs;
      threadIds[row] = 0n;
      spanIds[row] = 0;
      parentThreadIds[row] = 0n;
      parentSpanIds[row] = 0;
    }
  }

  const entryTypeUtf8 = encodeUtf8(entryTypeStrings);
  const traceIdUtf8 = encodeUtf8(traceIdStrings);
  const packageNameUtf8 = encodeUtf8(packageNameStrings);
  const packageFileUtf8 = encodeUtf8(packageFileStrings);
  const gitShaUtf8 = encodeUtf8(gitShaStrings);
  const messageUtf8 = encodeUtf8(messageStrings);

  const cols: [string, Column<unknown>][] = [];

  append(
    cols,
    'timestamp',
    makeArrowColumn({ type: timestamp(TimeUnit.NANOSECOND), length: totalRows, nullCount: 0, values: timestamps }),
  );

  append(
    cols,
    'trace_id',
    makeArrowColumn({
      type: dictionary(utf8(), indexTypeForCount(traceIdStrings.length), false, 0),
      length: totalRows,
      nullCount: 0,
      values: traceIds,
      dictionary: makeArrowColumn({
        type: utf8(),
        length: traceIdStrings.length,
        nullCount: 0,
        values: traceIdUtf8.data,
        offsets: traceIdUtf8.offsets,
      }),
    }),
  );

  append(cols, 'thread_id', makeArrowColumn({ type: uint64(), length: totalRows, nullCount: 0, values: threadIds }));
  append(cols, 'span_id', makeArrowColumn({ type: uint32(), length: totalRows, nullCount: 0, values: spanIds }));
  append(
    cols,
    'parent_thread_id',
    makeArrowColumn({
      type: uint64(),
      length: totalRows,
      nullCount: totalRows,
      values: parentThreadIds,
      validity: parentThreadValidity,
    }),
  );
  append(
    cols,
    'parent_span_id',
    makeArrowColumn({
      type: uint32(),
      length: totalRows,
      nullCount: totalRows,
      values: parentSpanIds,
      validity: parentSpanValidity,
    }),
  );

  append(
    cols,
    'entry_type',
    makeArrowColumn({
      type: dictionary(utf8(), int8(), false, 1),
      length: totalRows,
      nullCount: 0,
      values: entryTypes,
      dictionary: makeArrowColumn({
        type: utf8(),
        length: entryTypeStrings.length,
        nullCount: 0,
        values: entryTypeUtf8.data,
        offsets: entryTypeUtf8.offsets,
      }),
    }),
  );

  append(
    cols,
    'package_name',
    makeArrowColumn({
      type: dictionary(utf8(), indexTypeForCount(packageNameStrings.length), false, 2),
      length: totalRows,
      nullCount: 0,
      values: packageNames,
      dictionary: makeArrowColumn({
        type: utf8(),
        length: packageNameStrings.length,
        nullCount: 0,
        values: packageNameUtf8.data,
        offsets: packageNameUtf8.offsets,
      }),
    }),
  );
  append(
    cols,
    'package_file',
    makeArrowColumn({
      type: dictionary(utf8(), indexTypeForCount(packageFileStrings.length), false, 3),
      length: totalRows,
      nullCount: 0,
      values: packageFiles,
      dictionary: makeArrowColumn({
        type: utf8(),
        length: packageFileStrings.length,
        nullCount: 0,
        values: packageFileUtf8.data,
        offsets: packageFileUtf8.offsets,
      }),
    }),
  );
  append(
    cols,
    'git_sha',
    makeArrowColumn({
      type: dictionary(utf8(), indexTypeForCount(gitShaStrings.length), false, 4),
      length: totalRows,
      nullCount: 0,
      values: gitShas,
      dictionary: makeArrowColumn({
        type: utf8(),
        length: gitShaStrings.length,
        nullCount: 0,
        values: gitShaUtf8.data,
        offsets: gitShaUtf8.offsets,
      }),
    }),
  );
  append(
    cols,
    'message',
    makeArrowColumn({
      type: dictionary(utf8(), indexTypeForCount(messageStrings.length), false, 5),
      length: totalRows,
      nullCount: 0,
      values: messages,
      dictionary: makeArrowColumn({
        type: utf8(),
        length: messageStrings.length,
        nullCount: 0,
        values: messageUtf8.data,
        offsets: messageUtf8.offsets,
      }),
    }),
  );
  append(
    cols,
    'uint64_value',
    makeArrowColumn({ type: uint64(), length: totalRows, nullCount: 0, values: uint64Values }),
  );

  for (const [fieldName, fieldSchema] of Object.entries(mergedSchema)) {
    if (SYSTEM_SCHEMA_FIELD_NAMES.has(fieldName)) continue;
    const lmaoType = getSchemaType(fieldSchema);
    const arrowFieldName = getArrowFieldName(fieldName);

    if (lmaoType === 'number') {
      append(
        cols,
        arrowFieldName,
        makeArrowColumn({
          type: float64(),
          length: totalRows,
          nullCount: totalRows,
          values: new F64Array(totalRows),
          validity: new Uint8Array(Math.ceil(totalRows / 8)),
        }),
      );
      continue;
    }

    if (lmaoType === 'boolean') {
      append(
        cols,
        arrowFieldName,
        makeArrowColumn({
          type: bool(),
          length: totalRows,
          nullCount: totalRows,
          values: new Uint8Array(Math.ceil(totalRows / 8)),
          validity: new Uint8Array(Math.ceil(totalRows / 8)),
        }),
      );
      continue;
    }

    let idxType: IntType = uint8();
    if (lmaoType === 'enum') {
      idxType = hasArrowIndexType(fieldSchema) ? fieldSchema.__arrow_index_type : uint8();
    }
    const dictType = dictionary(utf8(), idxType);
    append(
      cols,
      arrowFieldName,
      makeArrowColumn({
        type: dictType,
        length: totalRows,
        nullCount: totalRows,
        values: new Uint8Array(totalRows),
        validity: new Uint8Array(Math.ceil(totalRows / 8)),
        dictionary: makeArrowColumn({
          type: utf8(),
          length: 1,
          nullCount: 0,
          values: new Uint8Array(0),
          offsets: new Int32Array([0, 0]),
        }),
      }),
    );
  }

  return tableFromColumns(cols);
}
