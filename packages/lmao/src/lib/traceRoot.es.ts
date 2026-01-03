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
 * TraceRoot - Browser implementation with performance.now() timing.
 *
 * Stores anchor data in _system ArrayBuffer for consistency with Node.js layout,
 * but all timestamp calculations are done in pure JS.
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

  constructor(trace_id: TraceId, anchorEpochNanos: bigint, anchorPerfNow: number, tracer: TracerLifecycleHooks) {
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
    return textDecoder.decode(new Uint8Array(this._system, TRACE_ROOT_TRACE_ID_OFFSET, len)) as TraceId;
  }

  /**
   * Get current timestamp in nanoseconds since Unix epoch.
   * Uses performance.now() - microsecond precision (last 3 digits = 000).
   */
  getTimestampNanos(): Nanoseconds {
    const elapsedMs = performance.now() - this._perfView[0];
    // Convert to nanoseconds (last 3 digits = 000 due to microsecond precision)
    const elapsedNanos = BigInt(Math.floor(elapsedMs * 1000)) * 1000n;
    return (this._epochView[0] + elapsedNanos) as Nanoseconds;
  }

  /**
   * Write span-start entry to buffer at row 0.
   *
   * Pure JS implementation - writes timestamp, entry_type, message, and sets _writeIndex.
   */
  writeSpanStart(buffer: AnySpanBuffer, spanName: string): void {
    // Row 0: span-start
    buffer.timestamp[0] = this.getTimestampNanos();
    buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
    buffer.message(0, spanName);

    // Row 1: pre-initialize as span-exception (will be overwritten on ok/err)
    buffer.entry_type[1] = ENTRY_TYPE_SPAN_EXCEPTION;
    buffer.timestamp[1] = 0n;

    // Events start at row 2
    buffer._writeIndex = 2;
  }

  /**
   * Write span-end entry to buffer at row 1.
   * Writes both timestamp and entry_type.
   */
  writeSpanEnd(buffer: AnySpanBuffer, entryType: number): void {
    buffer.timestamp[1] = this.getTimestampNanos();
    buffer.entry_type[1] = entryType;
  }

  /**
   * Write log entry timestamp and entry_type at the given index.
   * Used by SpanLogger for info/debug/warn/error/trace/ff entries.
   */
  writeLogEntry(buffer: AnySpanBuffer, idx: number, entryType: number): void {
    buffer.timestamp[idx] = this.getTimestampNanos();
    buffer.entry_type[idx] = entryType;
  }
}

/**
 * Factory function for creating browser TraceRoot instances.
 * Pass this to Tracer constructor for browser/ES environments.
 */
export function createTraceRoot(trace_id: string, tracer: TracerLifecycleHooks): TraceRoot {
  const anchorEpochNanos = BigInt(Date.now()) * 1_000_000n;
  const anchorPerfNow = performance.now();
  return new TraceRoot(trace_id as TraceId, anchorEpochNanos, anchorPerfNow, tracer);
}
