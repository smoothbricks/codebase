/**
 * Trace-row conversion for the Cloudflare sink lanes.
 *
 * Per specs/lmao/01u_cloudflare_trace_segments.md §Collection Lanes: spans are sent as ROWS
 * (JSON-serializable records), not pre-built Arrow chunks — Pipelines owns batching and Parquet
 * encoding for the raw tier, and the collector DO reduces the same flat row shape.
 *
 * The row shape mirrors the SQLite `spans` table (01a schema system): system columns
 * (trace_id, span_id, parent_span_id, row_index, entry_type, timestamp_ns, message) plus the
 * buffer's user schema columns, flattened so downstream SQL transforms need no unnesting.
 */

import { readSpanFieldValue, walkSpanSegments } from '../sqlite/sqlite-common.js';
import type { AnySpanBuffer } from '../types.js';

/** JSON-serializable scalar — the only value kinds a trace row may carry across the sink boundary. */
export type TraceRowValue = string | number | boolean | null;

/** One flat, JSON-serializable trace record — the payload unit of both delivery lanes. */
export type TraceRow = Record<string, TraceRowValue>;

function toTraceRowValue(value: unknown): TraceRowValue {
  if (value === undefined || value === null) return null;
  switch (typeof value) {
    case 'string':
    case 'number':
    case 'boolean':
      return value;
    case 'bigint':
      return Number(value);
    default:
      // Non-scalar buffer values (binary payloads etc.) have no JSON row representation.
      return null;
  }
}

/** Flatten a completed root span-buffer tree into JSON-serializable trace rows. */
export function spanBufferToTraceRows(rootBuffer: AnySpanBuffer): TraceRow[] {
  const rows: TraceRow[] = [];
  for (const segment of walkSpanSegments(rootBuffer)) {
    const { buffer, traceId, spanId, parentSpanId, rowOffset } = segment;
    const fieldNames = buffer._logSchema._columnNames;
    for (let row = 0; row < buffer._writeIndex; row++) {
      const record: TraceRow = {
        trace_id: traceId,
        span_id: spanId,
        parent_span_id: parentSpanId,
        row_index: rowOffset + row,
        entry_type: buffer.entry_type[row],
        timestamp_ns: Number(buffer.timestamp[row]),
        message: buffer.message_values[row] ?? null,
      };
      for (const fieldName of fieldNames) {
        record[fieldName] = toTraceRowValue(readSpanFieldValue(buffer, fieldName, row));
      }
      rows.push(record);
    }
  }
  return rows;
}
