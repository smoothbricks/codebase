/**
 * Convert a ThreadSpanView's live window into a flechette Table whose columns
 * match lmao-arrow's system ∪ schema batch (scope materialized at flush).
 */

import type { Column, Table } from '@uwdata/flechette';
import {
  columnFromArray,
  columnFromValues,
  TimeUnit,
  tableFromColumns,
  timestamp,
  uint32,
  uint64,
  utf8,
} from '@uwdata/flechette';
import { makeArrowColumn } from '../arrow/flechette.js';
import { ENTRY_TYPE_NAMES, THREAD_ATTRIBUTE_KINDS } from '../schema/systemSchema.js';
import { getEnumValues } from '../schema/typeGuards.js';
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

  // tableFromColumns needs Column values; a bare `{ type, data }` descriptor
  // has no `length`, which made every non-empty flush on this lane throw
  // "All columns must have the same length" before it produced a row.
  const columns: Record<string, Column<unknown>> = {
    // flechette's value builder multiplies by a unit factor, which throws on
    // BigInt input, so nanosecond timestamps are handed over as raw values.
    timestamp: makeArrowColumn({
      type: timestamp(TimeUnit.NANOSECOND),
      length: rowCount,
      nullCount: 0,
      values: timestamps,
    }),
    trace_id: columnFromArray(traceIds, utf8()),
    thread_id: columnFromArray(new BigUint64Array(rowCount).fill(view.thread_id), uint64()),
    span_id: columnFromArray(spanIds, uint32()),
    parent_span_id: columnFromArray(parentSpanIds, uint32()),
    // Same surface as the JS-heap converter: entry_type reaches Arrow as the
    // ENTRY_TYPE_NAMES string, not the wire discriminant, so queries and the
    // canonical semantic snapshot read one vocabulary across lanes.
    entry_type: columnFromArray(
      Array.from(headers, (header) => ENTRY_TYPE_NAMES[header & 0xff] ?? String(header & 0xff)),
      utf8(),
    ),
    line: columnFromArray(lines, uint32()),
    message: columnFromArray(messages, utf8()),
  };

  const ordinals = schemaAttributeOrdinals(view._logSchema);
  for (const [name, ordinal] of ordinals) {
    // Enum cells cross the ABI as variant indices; the Arrow surface speaks
    // variant names, same as the js-heap converter.
    const enumValues = getEnumValues(view._logSchema.fields[name]);
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
      else if (kind === KIND_ENUM) values[row] = enumValues?.[Number(cell.value)] ?? Number(cell.value);
      else if (kind === KIND_TEXT) values[row] = runtime.readInterned(binding, Number(cell.value));
      else values[row] = null;
    }
    // Number cells stay nullable: a dense Float64Array would coerce an unset
    // cell to 0, which reads as a written value on the query surface.
    columns[name] = columnFromValues(values);
  }

  return tableFromColumns(columns);
}
