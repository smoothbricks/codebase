/// <reference types="node" />

/**
 * TraceRoot - Node.js implementation.
 *
 * Uses process.hrtime.bigint() for true nanosecond precision and writes directly
 * to SpanBuffer state from JavaScript.
 *
 * @module traceRoot.node
 */

import type { Nanoseconds } from '@smoothbricks/arrow-builder';
import { ENTRY_TYPE_SPAN_EXCEPTION, ENTRY_TYPE_SPAN_START } from './schema/systemSchema.js';
import type { TraceId } from './traceId.js';
import {
  type ITraceRoot,
  TRACE_ROOT_ANCHOR_EPOCH_OFFSET,
  TRACE_ROOT_ANCHOR_PERF_OFFSET,
  TRACE_ROOT_TRACE_ID_LEN_OFFSET,
  TRACE_ROOT_TRACE_ID_OFFSET,
  type TracerLifecycleHooks,
} from './traceRoot.js';
import type { AnySpanBuffer } from './types.js';

/** Shared TextEncoder for trace_id encoding (stateless, reusable) */
const textEncoder = new TextEncoder();

/** Shared TextDecoder for trace_id decoding (stateless, reusable) */
const textDecoder = new TextDecoder();

/**
 * TraceRoot - Node.js implementation.
 *
 * Stores anchor data in _system ArrayBuffer and writes timestamps directly from JS.
 */
export class TraceRoot implements ITraceRoot {
  /**
   * Raw backing buffer containing anchor timestamps and trace_id.
     */
  readonly _system: ArrayBuffer;

  /**
   * Tracer reference for lifecycle hooks and event callbacks.
   */
  readonly tracer: TracerLifecycleHooks;

  /**
   * Cached TypedArray views for fast JS access.
   */
  private readonly _epochView: BigInt64Array;
  private readonly _perfView: Float64Array;

  /**
   * Cached anchor hrtime as BigInt for JS timestamp fallback.
   * Only used if NAPI addon is not available.
   */
  private readonly _anchorHrtimeBigInt: bigint;

  constructor(
    trace_id: TraceId,
    anchorEpochNanos: bigint,
    anchorPerfNow: number,
    anchorHrtimeBigInt: bigint,
    tracer: TracerLifecycleHooks,
  ) {
    // Allocate buffer: 17 bytes header + trace_id length
    // trace_id is validated to be ASCII (1 byte per char) so length === byte length
    this._system = new ArrayBuffer(TRACE_ROOT_TRACE_ID_OFFSET + trace_id.length);
    this.tracer = tracer;

    // Create cached views (one-time allocation)
    this._epochView = new BigInt64Array(this._system, TRACE_ROOT_ANCHOR_EPOCH_OFFSET, 1);
    this._perfView = new Float64Array(this._system, TRACE_ROOT_ANCHOR_PERF_OFFSET, 1);

    // Write anchor data directly
    this._epochView[0] = anchorEpochNanos;
    this._perfView[0] = anchorPerfNow;

    // Keep exact monotonic anchor for JS fallback (avoid Number precision loss after ~104 days uptime)
    this._anchorHrtimeBigInt = anchorHrtimeBigInt;

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
   * High-resolution timer anchor when trace was created.
     */
  get anchorPerfNow(): number {
    return this._perfView[0];
  }

  /**
   * Trace ID for this trace.
   */
  get trace_id(): TraceId {
    const len = new Uint8Array(this._system, TRACE_ROOT_TRACE_ID_LEN_OFFSET, 1)[0];
    return textDecoder.decode(new Uint8Array(this._system, TRACE_ROOT_TRACE_ID_OFFSET, len)) as TraceId;
  }

  /**
   * Get current timestamp in nanoseconds since Unix epoch.
   * Uses process.hrtime.bigint() for true nanosecond precision.
   */
  getTimestampNanos(): Nanoseconds {
    const currentHrtime = process.hrtime.bigint();
    const elapsedNanos = currentHrtime - this._anchorHrtimeBigInt;
    return (this._epochView[0] + elapsedNanos) as Nanoseconds;
  }

  /**
   * Write span-start entry to buffer at row 0.
   *
   */
  writeSpanStart(buffer: AnySpanBuffer, spanName: string): void {
    buffer.timestamp[0] = this.getTimestampNanos();
    buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
    buffer.message(0, spanName);

    // Pre-initialize row 1 as span-exception
    buffer.entry_type[1] = ENTRY_TYPE_SPAN_EXCEPTION;
    buffer.timestamp[1] = 0n;

    buffer._writeIndex = 2;
  }

  /**
   * Write span-end entry to buffer at row 1.
   * Writes both timestamp and entry_type.
   *
   */
  writeSpanEnd(buffer: AnySpanBuffer, entryType: number): void {
    buffer.timestamp[1] = this.getTimestampNanos();
    buffer.entry_type[1] = entryType;
  }

  /**
   * Write log entry: bump writeIndex, write timestamp + entry_type, return idx.
   * SpanLogger uses returned idx for string column writes.
   */
  writeLogEntry(buffer: AnySpanBuffer, entryType: number): number {
    const idx = buffer._writeIndex;
    buffer.timestamp[idx] = this.getTimestampNanos();
    buffer.entry_type[idx] = entryType;
    buffer._writeIndex = idx + 1;
    return idx;
  }
}

/**
 * Factory function for creating Node.js TraceRoot instances.
 * Pass this to Tracer constructor for Node.js environments.
 */
export function createTraceRoot(trace_id: string, tracer: TracerLifecycleHooks): TraceRoot {
  const anchorEpochNanos = BigInt(Date.now()) * 1_000_000n;
  const anchorHrtimeBigInt = process.hrtime.bigint();
  // Store as number for NAPI to read as f64
  const anchorPerfNow = Number(anchorHrtimeBigInt);
  return new TraceRoot(trace_id as TraceId, anchorEpochNanos, anchorPerfNow, anchorHrtimeBigInt, tracer);
}
