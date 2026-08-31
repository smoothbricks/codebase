/**
 * Convert a ThreadSpanView's live window into a flechette Table whose columns
 * match lmao-arrow's system ∪ schema batch (scope materialized at flush).
 */

import type { Table } from '@uwdata/flechette';
import { float64, TimeUnit, tableFromColumns, timestamp, uint32, uint64, utf8 } from '@uwdata/flechette';
import { THREAD_ATTRIBUTE_KINDS } from '../schema/systemSchema.js';
import { schemaAttributeOrdinals } from './schemaBlob.js';
import type { ThreadSpanView } from './threadSpanView.js';

const KIND_NUMBER = THREAD_ATTRIBUTE_KINDS[0].discriminant;
const KIND_UINT64 = THREAD_ATTRIBUTE_KINDS[1].discriminant;
const KIND_BOOLEAN = THREAD_ATTRIBUTE_KINDS[2].discriminant;
const KIND_TEXT = THREAD_ATTRIBUTE_KINDS[3].discriminant;
const KIND_ENUM = THREAD_ATTRIBUTE_KINDS[4].discriminant;

const bits = new DataView(new ArrayBuffer(8));

function bitsToF64(value: bigint): number {
  bits.setBigUint64(0, value, true);
  return bits.getFloat64(0, true);
}

export function convertThreadViewToArrowTable(view: ThreadSpanView): Table {
  const { runtime, binding } = view;
  const rowCount = runtime.rowCount(binding);
  if (rowCount === 0) return tableFromColumns({});
  runtime.materializeScope(binding, 0, rowCount);

  const timestamps = new BigInt64Array(rowCount);
  const spanIds = new Uint32Array(rowCount);
  const parentSpanIds = new Uint32Array(rowCount);
  const lines = new Uint32Array(rowCount);
  const headers = new Uint32Array(rowCount);
  const traceIds: string[] = new Array(rowCount);
  const messages: (string | null)[] = new Array(rowCount);

  for (let row = 0; row < rowCount; row++) {
    timestamps[row] = runtime.readTimestamp(binding, row);
    spanIds[row] = runtime.readSpanId(binding, row);
    parentSpanIds[row] = runtime.readParentSpanId(binding, row);
    lines[row] = runtime.readLine(binding, row);
    headers[row] = runtime.readHeader(binding, row);
    traceIds[row] = runtime.readTraceId(binding, row);
    const message = runtime.readMessage(binding, row);
    messages[row] = message.length === 0 ? null : message;
  }

  const columns: Record<string, unknown> = {
    timestamp: { type: timestamp(TimeUnit.NANOSECOND), data: timestamps },
    trace_id: traceIds,
    thread_id: { type: uint64(), data: new BigUint64Array(rowCount).fill(view.thread_id) },
    span_id: { type: uint32(), data: spanIds },
    parent_span_id: { type: uint32(), data: parentSpanIds },
    entry_type: { type: uint32(), data: Uint32Array.from(headers, (header) => header & 0xff) },
    line: { type: uint32(), data: lines },
    message: messages,
  };

  const ordinals = schemaAttributeOrdinals(view._logSchema);
  for (const [name, ordinal] of ordinals) {
    const values: unknown[] = new Array(rowCount);
    let kind = 0;
    for (let row = 0; row < rowCount; row++) {
      const cell = runtime.readAttr(binding, row, ordinal);
      if (cell === undefined) {
        values[row] = null;
        continue;
      }
      kind = cell.kind;
      if (kind === KIND_NUMBER) values[row] = bitsToF64(cell.value);
      else if (kind === KIND_UINT64) values[row] = cell.value;
      else if (kind === KIND_BOOLEAN) values[row] = cell.value !== 0n;
      else if (kind === KIND_ENUM) values[row] = Number(cell.value);
      else if (kind === KIND_TEXT) values[row] = runtime.readInterned(binding, Number(cell.value));
      else values[row] = null;
    }
    if (kind === KIND_NUMBER) {
      const data = new Float64Array(rowCount);
      for (let row = 0; row < rowCount; row++) {
        const value = values[row];
        data[row] = typeof value === 'number' ? value : 0;
      }
      columns[name] = { type: float64(), data };
    } else {
      columns[name] = values;
    }
  }

  return tableFromColumns(columns);
}
