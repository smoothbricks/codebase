/**
 * TraceRoot - Platform-agnostic interface for trace anchoring and timestamp writes.
 *
 * TraceRoot manages:
 * - Trace ID storage
 * - Timestamp anchoring (epoch + high-res timer reference)
 * - Writing span-start/span-end to SpanBuffer's _system columns
 *
 * Platform-specific implementations:
 * - TraceRoot.es.ts: Browser - uses performance.now() for timing
 * - TraceRoot.node.ts: Node.js - delegates to NAPI addon for optimized writes
 *
 * @module traceRoot
 */

import type { Nanoseconds } from '@smoothbricks/arrow-builder';
import type { AnySpanBuffer } from './types.js';

export type { SpanIdentity, TraceId } from './traceId.js';
// Re-export TraceId types from traceId.ts
export { createTraceId, extractSpanIdentity, generateTraceId, isValidTraceId, MAX_TRACE_ID_LENGTH } from './traceId.js';

/**
 * Tracer lifecycle hooks interface.
 * Extracted to avoid circular dependency - Tracer implements this.
 */
export interface TracerLifecycleHooks {
  onTraceStart(buffer: unknown): void;
  onTraceEnd(buffer: unknown): void;
  onSpanStart(buffer: unknown): void;
  onSpanEnd(buffer: unknown): void;
  onStatsWillResetFor(buffer: unknown): void;
}

/**
 * TraceRoot._system memory layout (little-endian):
 *
 * Offset  Size   Field
 * 0       8      anchorEpochNanos (i64) - epoch time in nanoseconds
 * 8       8      anchorPerfNow (f64) - high-res timer anchor
 * 16      1      trace_id_len (u8) - length of trace_id UTF-8 bytes
 * 17      N      trace_id (UTF-8 bytes)
 *
 * Total: 17 + trace_id.length bytes
 *
 * This layout enables NAPI/WASM to read anchors directly without BigInt extraction.
 */
export const TRACE_ROOT_ANCHOR_EPOCH_OFFSET = 0;
export const TRACE_ROOT_ANCHOR_PERF_OFFSET = 8;
export const TRACE_ROOT_TRACE_ID_LEN_OFFSET = 16;
export const TRACE_ROOT_TRACE_ID_OFFSET = 17;

/**
 * TraceRoot interface - implemented by platform-specific classes.
 *
 * Each implementation handles timestamp calculation and span writes differently:
 * - Browser: Pure JS with performance.now()
 * - Node.js: NAPI addon with mach_absolute_time() / clock_gettime()
 */
/**
 * Factory function type for creating platform-specific TraceRoot instances.
 * Passed to Tracer constructor to enable tree-shaking of unused platform code.
 *
 * The factory creates anchor timestamps internally using platform-specific APIs.
 */
export type TraceRootFactory = (trace_id: string, tracer: TracerLifecycleHooks) => ITraceRoot;

export interface ITraceRoot {
  /**
   * Raw backing buffer containing anchor timestamps and trace_id.
   * NAPI/WASM can read this directly without BigInt extraction.
   */
  readonly _system: ArrayBuffer;

  /**
   * Tracer reference for lifecycle hooks and event callbacks.
   */
  readonly tracer: TracerLifecycleHooks;

  /**
   * Trace ID for this trace.
   */
  readonly trace_id: string;

  /**
   * Epoch time in nanoseconds when trace was created.
   * Used by SpanLogger for log entry timestamps.
   */
  readonly anchorEpochNanos: bigint;

  /**
   * High-resolution timer anchor when trace was created.
   * Browser: performance.now() value (number)
   * Node.js: stored as number for NAPI compatibility, but represents hrtime
   */
  readonly anchorPerfNow: number;

  /**
   * Get current timestamp in nanoseconds since Unix epoch.
   * Platform-specific implementation.
   */
  getTimestampNanos(): Nanoseconds;

  /**
   * Write span-start entry to buffer at row 0.
   *
   * Writes:
   * - timestamp[0] = current time
   * - entry_type[0] = SPAN_START
   * - entry_type[1] = SPAN_EXCEPTION (pre-initialized for crash safety)
   * - timestamp[1] = 0 (will be set on completion)
   * - message[0] = spanName
   * - _writeIndex = 2
   *
   * @param buffer - SpanBuffer to write to
   * @param spanName - Name for this span
   */
  writeSpanStart(buffer: AnySpanBuffer, spanName: string): void;

  /**
   * Write span-end entry to buffer at row 1.
   * Writes both timestamp and entry_type.
   *
   * @param buffer - SpanBuffer to write to
   * @param entryType - Entry type (SPAN_OK, SPAN_ERR, or SPAN_EXCEPTION)
   */
  writeSpanEnd(buffer: AnySpanBuffer, entryType: number): void;

  /**
   * Write log entry timestamp and entry_type at the given index.
   * Used by SpanLogger for info/debug/warn/error/trace/ff entries.
   *
   * @param buffer - SpanBuffer to write to
   * @param idx - Row index to write at
   * @param entryType - Entry type (INFO, DEBUG, WARN, ERROR, TRACE, FF_ACCESS, FF_USAGE)
   */
  writeLogEntry(buffer: AnySpanBuffer, idx: number, entryType: number): void;
}
