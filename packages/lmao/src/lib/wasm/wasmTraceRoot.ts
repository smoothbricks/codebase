/**
 * WASM-backed TraceRoot that writes span lifecycle events via WASM allocator.
 *
 * Unlike NodeTraceRoot which writes to JS ArrayBuffers, this writes directly
 * to WASM memory using the allocator's span lifecycle functions.
 *
 * Implements ITraceRoot for compatibility with SpanContext and Tracer.
 *
 * @module wasmTraceRoot
 */

import { Nanoseconds } from '@smoothbricks/arrow-builder';
import { ENTRY_TYPE_SPAN_EXCEPTION, ENTRY_TYPE_SPAN_START } from '../schema/systemSchema.js';
import { createTraceId, type TraceId } from '../traceId.js';
import type { ITraceRoot, TracerLifecycleHooks } from '../traceRoot.js';
import type { AnySpanBuffer } from '../types.js';
import type { WasmAllocator } from './wasmAllocator.js';

// =============================================================================
// WASM Buffer Detection
// =============================================================================

/**
 * Interface for WASM-backed span buffers.
 * These have a _systemPtr that points to WASM memory.
 */
export interface WasmSpanBufferLike {
  /** Byte offset into WASM memory for system columns (timestamp + entry_type) */
  readonly _systemPtr: number;
  /** Byte offset into WASM memory for identity block (writeIndex, span_id, trace_id) */
  readonly _identityPtr: number;
  /** Message column - string array for span names and log messages */
  readonly _message: string[];
  /** Current write position (getter/setter reads from WASM identity block) */
  _writeIndex: number;
}

/**
 * Type guard to check if a buffer is WASM-backed.
 */
export function isWasmSpanBuffer(buffer: unknown): buffer is WasmSpanBufferLike {
  return typeof buffer === 'object' && buffer !== null && typeof Reflect.get(buffer, '_systemPtr') === 'number';
}

// =============================================================================
// WasmTraceRoot Implementation
// =============================================================================

/**
 * WASM-backed TraceRoot implementation.
 *
 * Implements ITraceRoot so it's compatible with SpanContext and the base Tracer.
 * Writes timestamps and entry types to WASM memory for WASM-backed buffers.
 *
 * Memory Layout at _traceRootPtr (16 bytes):
 * - Offset 0-7: anchorEpochNanos (i64) - wall clock time in nanoseconds
 * - Offset 8-15: anchorPerfNow (f64) - performance.now() anchor value
 *
 * The WASM allocator's initTraceRoot() captures these values at trace start,
 * and span lifecycle methods use them to calculate timestamps.
 */
export class WasmTraceRoot implements ITraceRoot {
  /** Byte offset in WASM memory where TraceRoot data lives */
  readonly _traceRootPtr: number;

  /** The WASM allocator instance */
  readonly allocator: WasmAllocator;

  /** Trace ID string */
  readonly trace_id: TraceId;

  /** Tracer reference for lifecycle hooks */
  readonly tracer: TracerLifecycleHooks;

  /**
   * Raw backing buffer - not used for WASM, but required by ITraceRoot.
   * We create a minimal ArrayBuffer just to satisfy the interface.
   */
  readonly _system: ArrayBuffer;

  constructor(allocator: WasmAllocator, trace_id: TraceId, tracer: TracerLifecycleHooks) {
    this.allocator = allocator;
    this.trace_id = trace_id;
    this.tracer = tracer;

    // Create minimal _system buffer (not actually used, but required by interface)
    this._system = new ArrayBuffer(16);

    // Allocate an 8B column block for TraceRoot data (16 bytes needed, 8B block is large enough)
    // The 8B column block size is: ceil(capacity/8) + capacity*8 = 8 + 512 = 520 bytes for capacity 64
    // We only need 16 bytes, so this works fine
    this._traceRootPtr = allocator.alloc8B();

    // Initialize wall clock and monotonic start times in WASM memory
    allocator.initTraceRoot(this._traceRootPtr);
  }

  /**
   * Epoch time in nanoseconds when trace was created.
   * Read from WASM memory at _traceRootPtr offset 0.
   */
  get anchorEpochNanos(): bigint {
    // i64 view is indexed by 8-byte chunks
    return this.allocator.i64[this._traceRootPtr / 8];
  }

  /**
   * High-resolution timer anchor when trace was created.
   * Read from WASM memory at _traceRootPtr offset 8.
   */
  get anchorPerfNow(): number {
    // f64 view is indexed by 8-byte chunks
    return this.allocator.f64[(this._traceRootPtr + 8) / 8];
  }

  /**
   * Get current timestamp in nanoseconds since Unix epoch.
   *
   * Uses the same calculation as NodeTraceRoot:
   * currentNanos = anchorEpochNanos + (currentPerfNow - anchorPerfNow) * 1_000_000
   *
   * Note: In the browser, performance.now() returns milliseconds as a float.
   * We convert to nanoseconds for consistency with the trace format.
   */
  getTimestampNanos(): Nanoseconds {
    const elapsedMs = performance.now() - this.anchorPerfNow;
    const elapsedNanos = BigInt(Math.round(elapsedMs * 1_000_000));
    return Nanoseconds.unsafe(this.anchorEpochNanos + elapsedNanos);
  }

  // ===========================================================================
  // ITraceRoot implementation (takes buffer parameter)
  // These are the methods used by SpanContext and Tracer
  // ===========================================================================

  /**
   * Write span-start entry to buffer at row 0.
   *
   * For WASM buffers: writes via WASM allocator to WASM memory
   * Sets row 0 = span-start, row 1 = span-exception (crash safety).
   * Sets _writeIndex = 2.
   *
   * @param buffer - SpanBuffer to write to (can be WASM or JS buffer)
   * @param spanName - Name for this span
   */
  writeSpanStart(buffer: AnySpanBuffer, spanName: string): void {
    if (isWasmSpanBuffer(buffer)) {
      // WASM path: delegate to WASM allocator
      // spanStart writes row 0 (span-start) and row 1 (span-exception placeholder)
      // Also sets writeIndex to 2 in the identity block
      this.allocator.spanStart(buffer._systemPtr, buffer._identityPtr, this._traceRootPtr);
      // Write span name to message column (JS string array)
      buffer._message[0] = spanName;
      // Note: _writeIndex is set by WASM spanStart to 2, but the getter reads from WASM
      // so we don't need to set it here explicitly (the setter would write to WASM anyway)
    } else {
      // JS path: write directly to TypedArrays (fallback for mixed usage)
      buffer.timestamp[0] = this.getTimestampNanos();
      buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      buffer.message(0, spanName);
      buffer.entry_type[1] = ENTRY_TYPE_SPAN_EXCEPTION;
      buffer.timestamp[1] = 0n;
      buffer._writeIndex = 2;
    }
  }

  /**
   * Write span-end entry to buffer at row 1.
   * Writes both timestamp and entry_type.
   *
   * @param buffer - SpanBuffer to write to
   * @param entryType - Entry type (SPAN_OK, SPAN_ERR, or SPAN_EXCEPTION)
   */
  writeSpanEnd(buffer: AnySpanBuffer, entryType: number): void {
    if (isWasmSpanBuffer(buffer)) {
      // WASM path: use appropriate end function based on entry type
      // Note: WASM spanEndOk/spanEndErr write to row 1
      if (entryType === 2) {
        // SPAN_OK
        this.allocator.spanEndOk(buffer._systemPtr, this._traceRootPtr);
      } else if (entryType === 3) {
        // SPAN_ERR
        this.allocator.spanEndErr(buffer._systemPtr, this._traceRootPtr);
      } else {
        // SPAN_EXCEPTION or other - write via generic log entry
        // For now, use spanEndErr as fallback (entry_type will be overwritten)
        this.allocator.spanEndErr(buffer._systemPtr, this._traceRootPtr);
        // Overwrite entry_type if needed (for EXCEPTION)
        if (entryType !== 3) {
          buffer.entry_type[1] = entryType;
        }
      }
    } else {
      // JS path
      buffer.timestamp[1] = this.getTimestampNanos();
      buffer.entry_type[1] = entryType;
    }
  }

  /**
   * Write log entry: bump writeIndex, write timestamp + entry_type, return idx.
   * SpanLogger uses returned idx for string column writes.
   *
   * @param buffer - SpanBuffer to write to
   * @param entryType - Entry type (INFO, DEBUG, WARN, ERROR, TRACE, FF_ACCESS, FF_USAGE)
   * @returns The row index where entry was written
   */
  writeLogEntry(buffer: AnySpanBuffer, entryType: number): number {
    if (isWasmSpanBuffer(buffer)) {
      // WASM path: bump writeIndex, write timestamp + entry_type, return idx
      return this.allocator.writeLogEntry(buffer._systemPtr, buffer._identityPtr, this._traceRootPtr, entryType);
    }
    // JS path: bump writeIndex, write directly, return idx
    const idx = buffer._writeIndex;
    buffer.timestamp[idx] = this.getTimestampNanos();
    buffer.entry_type[idx] = entryType;
    buffer._writeIndex = idx + 1;
    return idx;
  }

  // ===========================================================================
  // Low-level WASM pointer-based methods (for direct WASM access and tests)
  // These are used by the existing tests and for low-level buffer manipulation
  // ===========================================================================

  /**
   * Write span-start entry to buffer's system block using pointer.
   * Sets row 0 = span-start, row 1 = span-exception (crash safety).
   * Sets _writeIndex = 2 in the identity block.
   *
   * @param systemPtr - Byte offset of buffer's system block in WASM memory
   * @param identityPtr - Byte offset of buffer's identity block in WASM memory
   */
  writeSpanStartPtr(systemPtr: number, identityPtr: number): void {
    this.allocator.spanStart(systemPtr, identityPtr, this._traceRootPtr);
  }

  /**
   * Write span-ok to row 1 using pointer.
   * @param systemPtr - Byte offset of buffer's system block in WASM memory
   */
  writeSpanEndOkPtr(systemPtr: number): void {
    this.allocator.spanEndOk(systemPtr, this._traceRootPtr);
  }

  /**
   * Write span-err to row 1 using pointer.
   * @param systemPtr - Byte offset of buffer's system block in WASM memory
   */
  writeSpanEndErrPtr(systemPtr: number): void {
    this.allocator.spanEndErr(systemPtr, this._traceRootPtr);
  }

  /**
   * Write a log entry at current write index using pointer.
   * Returns the row index where entry was written.
   *
   * @param systemPtr - Byte offset of buffer's system block in WASM memory
   * @param identityPtr - Byte offset of buffer's identity block in WASM memory
   * @param entryType - Entry type constant (ENTRY_TYPE_INFO, etc.)
   * @returns Row index where entry was written
   */
  writeLogEntryPtr(systemPtr: number, identityPtr: number, entryType: number): number {
    return this.allocator.writeLogEntry(systemPtr, identityPtr, this._traceRootPtr, entryType);
  }

  /**
   * Read current write index from buffer's identity block.
   * @param identityPtr - Byte offset of buffer's identity block in WASM memory
   */
  readWriteIndex(identityPtr: number): number {
    return this.allocator.readWriteIndex(identityPtr);
  }

  /**
   * Free the TraceRoot's memory block when trace completes.
   */
  free(): void {
    this.allocator.free8B(this._traceRootPtr);
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a WasmTraceRoot instance directly.
 * Used by tests and low-level code.
 *
 * @param allocator - WASM allocator instance
 * @param trace_id - Trace ID for this trace
 * @param tracer - Tracer lifecycle hooks
 */
export function createWasmTraceRoot(
  allocator: WasmAllocator,
  trace_id: string,
  tracer: TracerLifecycleHooks,
): WasmTraceRoot {
  return new WasmTraceRoot(allocator, createTraceId(trace_id), tracer);
}

/**
 * Factory function to create WasmTraceRoot factory.
 *
 * This returns a factory function that can be passed to Tracer constructor.
 *
 * @param allocator - WASM allocator instance
 * @returns Factory function compatible with TraceRootFactory type
 */
export function createWasmTraceRootFactory(allocator: WasmAllocator) {
  return (trace_id: string, tracer: TracerLifecycleHooks): WasmTraceRoot => {
    return new WasmTraceRoot(allocator, createTraceId(trace_id), tracer);
  };
}
