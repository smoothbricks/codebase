/// <reference types="node" />

/**
 * TraceRoot - Node.js implementation.
 *
 * Uses process.hrtime.bigint() for true nanosecond precision and writes directly
 * to SpanBuffer state from JavaScript.
 *
 * @module traceRoot.node
 */

import { Nanoseconds } from '@smoothbricks/arrow-builder';
import type { LogSchema } from './schema/LogSchema.js';
import { ENTRY_TYPE_SPAN_EXCEPTION, ENTRY_TYPE_SPAN_START } from './schema/systemSchema.js';
import { createTraceId, type TraceId } from './traceId.js';
import {
  type ITraceRoot,
  type SpanEndPrimitive,
  type SpanStartPrimitive,
  type TimestampAppendPrimitive,
  type TimestampNowPrimitive,
  TRACE_ROOT_ANCHOR_EPOCH_OFFSET,
  TRACE_ROOT_ANCHOR_PERF_OFFSET,
  TRACE_ROOT_TRACE_ID_LEN_OFFSET,
  TRACE_ROOT_TRACE_ID_OFFSET,
  type TracerLifecycleHooks,
} from './traceRoot.js';
import { TraceTopology } from './traceTopology.js';
import type { AnySpanBuffer } from './types.js';

/** Shared TextEncoder for trace_id encoding (stateless, reusable) */
const textEncoder = new TextEncoder();

//#region smoo/lmao!n/trace-root-timestamps #node
/**
 * TraceRoot - Node.js implementation.
 *
 * Stores anchor data in _system ArrayBuffer and writes timestamps directly from JS.
 */
function nextTimestamp(root: TraceRoot): Nanoseconds {
  let timestamp = root._epochHrtimeOffset + process.hrtime.bigint();
  if (timestamp <= root._lastTimestampNanos) timestamp = root._lastTimestampNanos + 1n;
  root._lastTimestampNanos = timestamp;
  return Nanoseconds.unsafe(timestamp);
}

const timestampNow: TimestampNowPrimitive = (traceRoot) => {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- Node primitive table is only installed on TraceRoot.node instances.
  return nextTimestamp(traceRoot as TraceRoot);
};

const appendLogEntry: TimestampAppendPrimitive = (traceRoot, buffer, entryType) => {
  const entryTypes = buffer.entry_type;
  if (entryTypes === undefined) throw new TypeError('Split timestamp appender requires entry_type storage');
  const idx = buffer._writeIndex;
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- Node primitive table is only installed on TraceRoot.node instances.
  buffer.timestamp[idx] = nextTimestamp(traceRoot as TraceRoot);
  entryTypes[idx] = entryType;
  buffer._writeIndex = idx + 1;
  return idx;
};

const writeSpanStartPrimitive: SpanStartPrimitive = (traceRoot, buffer, spanName) => {
  const entryTypes = buffer.entry_type;
  if (entryTypes === undefined) throw new TypeError('Split span-start appender requires entry_type storage');
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- Node primitive table is only installed on TraceRoot.node instances.
  buffer.timestamp[0] = nextTimestamp(traceRoot as TraceRoot);
  entryTypes[0] = ENTRY_TYPE_SPAN_START;
  if (buffer.message_values) buffer.message(0, spanName);
  entryTypes[1] = ENTRY_TYPE_SPAN_EXCEPTION;
  buffer.timestamp[1] = 0n;
  buffer._writeIndex = 2;
};

const writeSpanEndPrimitive: SpanEndPrimitive = (traceRoot, buffer, entryType) => {
  const entryTypes = buffer.entry_type;
  if (entryTypes === undefined) throw new TypeError('Split span-end appender requires entry_type storage');
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- Node primitive table is only installed on TraceRoot.node instances.
  buffer.timestamp[1] = nextTimestamp(traceRoot as TraceRoot);
  entryTypes[1] = entryType;
  buffer._sealStatsChain();
};

export class TraceRoot<T extends LogSchema = LogSchema> implements ITraceRoot<T> {
  /**
   * Raw backing buffer containing anchor timestamps and trace_id.
   */
  readonly _system: ArrayBuffer;
  readonly _traceIdBytes: Uint8Array;
  readonly trace_id: TraceId;

  /**
   * Tracer reference for lifecycle hooks and event callbacks.
   */
  readonly tracer: TracerLifecycleHooks<T>;
  readonly _topology: TraceTopology;
  readonly _timestampNow = timestampNow;
  readonly _appendLogEntry = appendLogEntry;
  readonly _writeSpanStart = writeSpanStartPrimitive;
  readonly _writeSpanEnd = writeSpanEndPrimitive;

  /**
   * Cached TypedArray views for fast JS access.
   */
  private readonly _epochView: BigInt64Array;
  private readonly _perfView: Float64Array;

  /** Epoch offset makes each exact timestamp one clock read plus one BigInt addition. */
  readonly _epochHrtimeOffset: bigint;
  _lastTimestampNanos = 0n;

  constructor(
    trace_id: TraceId,
    anchorEpochNanos: bigint,
    anchorPerfNow: number,
    anchorHrtimeBigInt: bigint,
    tracer: TracerLifecycleHooks<T>,
  ) {
    // Allocate buffer: 17 bytes header + trace_id length
    // trace_id is validated to be ASCII (1 byte per char) so length === byte length
    this._system = new ArrayBuffer(TRACE_ROOT_TRACE_ID_OFFSET + trace_id.length);
    this._traceIdBytes = new Uint8Array(this._system, TRACE_ROOT_TRACE_ID_OFFSET, trace_id.length);
    this.trace_id = trace_id;
    this.tracer = tracer;
    this._topology = new TraceTopology();

    // Create cached views (one-time allocation)
    this._epochView = new BigInt64Array(this._system, TRACE_ROOT_ANCHOR_EPOCH_OFFSET, 1);
    this._perfView = new Float64Array(this._system, TRACE_ROOT_ANCHOR_PERF_OFFSET, 1);

    // Write anchor data directly
    this._epochView[0] = anchorEpochNanos;
    this._perfView[0] = anchorPerfNow;

    this._epochHrtimeOffset = anchorEpochNanos - anchorHrtimeBigInt;

    // Encode once into the canonical trace-owned view used by root span identities.
    const { written } = textEncoder.encodeInto(trace_id, this._traceIdBytes);
    new Uint8Array(this._system, TRACE_ROOT_TRACE_ID_LEN_OFFSET, 1)[0] = written;
  }

  /**
   * Epoch time in nanoseconds when trace was created.
   */
  get anchorEpochNanos(): bigint {
    return this._epochView[0];
  }

  /**
   * High-resolution timer anchor when trace was created.
   */
  get anchorPerfNow(): number {
    return this._perfView[0];
  }

  /**
   * Get current timestamp in nanoseconds since Unix epoch.
   * Uses process.hrtime.bigint() for true nanosecond precision.
   */
  getTimestampNanos(): Nanoseconds {
    return this._timestampNow(this);
  }

  /**
   * Write span-start entry to buffer at row 0.
   *
   */
  writeSpanStart(buffer: AnySpanBuffer, spanName: string): void {
    this._writeSpanStart(this, buffer, spanName);
  }

  /**
   * Write span-end entry to buffer at row 1.
   * Writes both timestamp and entry_type.
   *
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
 * Factory function for creating Node.js TraceRoot instances.
 * Pass this to Tracer constructor for Node.js environments.
 */
export function createTraceRoot<T extends LogSchema>(trace_id: string, tracer: TracerLifecycleHooks<T>): TraceRoot<T> {
  const anchorEpochNanos = BigInt(Date.now()) * 1_000_000n;
  const anchorHrtimeBigInt = process.hrtime.bigint();
  // Also store as f64 in the shared _system layout (WASM reads it without BigInt extraction)
  const anchorPerfNow = Number(anchorHrtimeBigInt);
  return new TraceRoot(createTraceId(trace_id), anchorEpochNanos, anchorPerfNow, anchorHrtimeBigInt, tracer);
}
//#endregion smoo/lmao!n/trace-root-timestamps
