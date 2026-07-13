//#region smoo/lmao!n/es-trace-root
/**
 * TraceRoot - Browser/ES implementation.
 *
 * Uses performance.now() for timing with microsecond precision (~5-20μs).
 * Last 3 digits of nanoseconds are always 000.
 *
 * For Node.js with NAPI-optimized writes, use traceRoot.node.ts instead.
 *
 * @module traceRoot.es
 */

import { Nanoseconds } from '@smoothbricks/arrow-builder';
import { ENTRY_TYPE_SPAN_EXCEPTION, ENTRY_TYPE_SPAN_START } from './schema/systemSchema.js';
import { createTraceId, type TraceId } from './traceId.js';
import {
  type ITraceRoot,
  TRACE_ROOT_ANCHOR_EPOCH_OFFSET,
  TRACE_ROOT_ANCHOR_PERF_OFFSET,
  TRACE_ROOT_TRACE_ID_LEN_OFFSET,
  TRACE_ROOT_TRACE_ID_OFFSET,
  type SpanEndPrimitive,
  type SpanStartPrimitive,
  type TimestampAppendPrimitive,
  type TimestampNowPrimitive,
  type TracerLifecycleHooks,
} from './traceRoot.js';
import type { AnySpanBuffer } from './types.js';

/** Shared TextEncoder for trace_id encoding (stateless, reusable) */
const textEncoder = new TextEncoder();

/** Shared TextDecoder for trace_id decoding (stateless, reusable) */
const textDecoder = new TextDecoder();

/**
 * TraceRoot - Browser implementation with performance.now() timing.
 *
 * Stores anchor data in _system ArrayBuffer for consistency with Node.js layout,
 * but all timestamp calculations are done in pure JS.
 */
function nextTimestamp(root: TraceRoot): Nanoseconds {
  const elapsedNanos = BigInt(Math.floor((performance.now() - root._anchorPerformanceNow) * 1000)) * 1000n;
  let timestamp = root._anchorEpochNanos + elapsedNanos;
  if (timestamp <= root._lastTimestampNanos) timestamp = root._lastTimestampNanos + 1000n;
  root._lastTimestampNanos = timestamp;
  return Nanoseconds.unsafe(timestamp);
}

const timestampNow: TimestampNowPrimitive = (traceRoot) => nextTimestamp(traceRoot as TraceRoot);

const appendLogEntry: TimestampAppendPrimitive = (traceRoot, buffer, entryType) => {
  const idx = buffer._writeIndex;
  buffer.timestamp[idx] = nextTimestamp(traceRoot as TraceRoot);
  buffer.entry_type[idx] = entryType;
  buffer._writeIndex = idx + 1;
  return idx;
};

const writeSpanStartPrimitive: SpanStartPrimitive = (traceRoot, buffer, spanName) => {
  buffer.timestamp[0] = nextTimestamp(traceRoot as TraceRoot);
  buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
  if (buffer.message_values) buffer.message(0, spanName);
  buffer.entry_type[1] = ENTRY_TYPE_SPAN_EXCEPTION;
  buffer.timestamp[1] = 0n;
  buffer._writeIndex = 2;
};

const writeSpanEndPrimitive: SpanEndPrimitive = (traceRoot, buffer, entryType) => {
  buffer.timestamp[1] = nextTimestamp(traceRoot as TraceRoot);
  buffer.entry_type[1] = entryType;
  buffer._sealStatsChain();
};

export class TraceRoot implements ITraceRoot {
  /**
   * Raw backing buffer containing anchor timestamps and trace_id.
   */
  readonly _system: ArrayBuffer;

  /**
   * Tracer reference for lifecycle hooks and event callbacks.
   */
  readonly tracer: TracerLifecycleHooks;
  readonly _timestampNow = timestampNow;
  readonly _appendLogEntry = appendLogEntry;
  readonly _writeSpanStart = writeSpanStartPrimitive;
  readonly _writeSpanEnd = writeSpanEndPrimitive;
  readonly _anchorEpochNanos: bigint;
  readonly _anchorPerformanceNow: number;
  _lastTimestampNanos = 0n;

  /**
   * Cached TypedArray views for fast JS access.
   */
  private readonly _epochView: BigInt64Array;
  private readonly _perfView: Float64Array;

  constructor(trace_id: TraceId, anchorEpochNanos: bigint, anchorPerfNow: number, tracer: TracerLifecycleHooks) {
    // Allocate buffer: 17 bytes header + trace_id length
    // trace_id is validated to be ASCII (1 byte per char) so length === byte length
    this._system = new ArrayBuffer(TRACE_ROOT_TRACE_ID_OFFSET + trace_id.length);
    this.tracer = tracer;
    this._anchorEpochNanos = anchorEpochNanos;
    this._anchorPerformanceNow = anchorPerfNow;

    // Create cached views (one-time allocation)
    this._epochView = new BigInt64Array(this._system, TRACE_ROOT_ANCHOR_EPOCH_OFFSET, 1);
    this._perfView = new Float64Array(this._system, TRACE_ROOT_ANCHOR_PERF_OFFSET, 1);

    // Write anchor data directly
    this._epochView[0] = anchorEpochNanos;
    this._perfView[0] = anchorPerfNow;

    // Encode trace_id directly into buffer (no intermediate allocation)
    const u8View = new Uint8Array(this._system, TRACE_ROOT_TRACE_ID_LEN_OFFSET);
    const traceIdDest = new Uint8Array(this._system, TRACE_ROOT_TRACE_ID_OFFSET, trace_id.length);
    const { written } = textEncoder.encodeInto(trace_id, traceIdDest);
    u8View[0] = written;
  }

  /**
   * Epoch time in nanoseconds when trace was created.
   */
  get anchorEpochNanos(): bigint {
    return this._epochView[0];
  }

  /**
   * High-resolution timer anchor when trace was created (performance.now() value).
   */
  get anchorPerfNow(): number {
    return this._perfView[0];
  }

  /**
   * Trace ID for this trace.
   */
  get trace_id(): TraceId {
    const len = new Uint8Array(this._system, TRACE_ROOT_TRACE_ID_LEN_OFFSET, 1)[0];
    return createTraceId(textDecoder.decode(new Uint8Array(this._system, TRACE_ROOT_TRACE_ID_OFFSET, len)));
  }

  //#region smoo/lmao!n/trace-root-timestamps #es
  /**
   * Get current timestamp in nanoseconds since Unix epoch.
   * Uses performance.now() - microsecond precision (last 3 digits = 000).
   */
  getTimestampNanos(): Nanoseconds {
    return this._timestampNow(this);
  }
  //#endregion smoo/lmao!n/trace-root-timestamps

  /**
   * Write span-start entry to buffer at row 0.
   *
   * Pure JS implementation - writes timestamp, entry_type, message, and sets _writeIndex.
   */
  writeSpanStart(buffer: AnySpanBuffer, spanName: string): void {
    this._writeSpanStart(this, buffer, spanName);
  }

  /**
   * Write span-end entry to buffer at row 1.
   * Writes both timestamp and entry_type.
   */
  writeSpanEnd(buffer: AnySpanBuffer, entryType: number): void {
    this._writeSpanEnd(this, buffer, entryType);
  }

  /**
   * Write log entry: bump writeIndex, write timestamp + entry_type, return idx.
   * SpanLogger uses returned idx for string column writes.
   */
  writeLogEntry(buffer: AnySpanBuffer, entryType: number): number {
    return this._appendLogEntry(this, buffer, entryType);
  }
}

/**
 * Factory function for creating browser TraceRoot instances.
 * Pass this to Tracer constructor for browser/ES environments.
 */
export function createTraceRoot(trace_id: string, tracer: TracerLifecycleHooks): TraceRoot {
  const anchorEpochNanos = BigInt(Date.now()) * 1_000_000n;
  const anchorPerfNow = performance.now();
  return new TraceRoot(createTraceId(trace_id), anchorEpochNanos, anchorPerfNow, tracer);
}
//#endregion smoo/lmao!n/es-trace-root
