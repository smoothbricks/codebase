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
import type { LogSchema } from '../schema/LogSchema.js';
import { ENTRY_TYPE_SPAN_EXCEPTION, ENTRY_TYPE_SPAN_START } from '../schema/systemSchema.js';
import { createTraceId, type TraceId } from '../traceId.js';
import {
  consumeSpanStartedAtAllocation,
  type ITraceRoot,
  type SpanEndPrimitive,
  type SpanStartPrimitive,
  type TimestampAppendPrimitive,
  type TimestampNowPrimitive,
  type TraceRootFactory,
  type TracerLifecycleHooks,
} from '../traceRoot.js';
import { TraceTopology } from '../traceTopology.js';
import type { AnySpanBuffer } from '../types.js';
import type { WasmAllocator } from './wasmAllocator.js';

const traceIdEncoder = new TextEncoder();

// Spec link (88): realizes specs/lmao/01q_wasm_memory_architecture.md#smoo/lmao!n/wasm-mem (span lifecycle writer).
//#region smoo/lmao!n/wasm-mem.trace-root

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
  /** Concrete per-op capacity; may differ from the allocator default. */
  readonly _capacity: number;
  /** Byte offset into WASM memory for identity block (writeIndex, span_id, trace_id) */
  readonly _identityPtr: number;
  /** Message column - string array for span names and log messages */
  readonly _message: string[];
  /** Current write position (getter/setter reads from WASM identity block) */
  _writeIndex: number;
  readonly _identityOwner: boolean;
  readonly timestamp: BigInt64Array;
  readonly entry_type: Uint8Array;
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
const timestampNow: TimestampNowPrimitive = (traceRoot) => {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- WASM primitive table is only installed on WasmTraceRoot instances.
  const root = traceRoot as WasmTraceRoot;
  root._assertLive();
  const elapsedNanos = BigInt(Math.round((performance.now() - root.anchorPerfNow) * 1_000_000));
  return Nanoseconds.unsafe(root.anchorEpochNanos + elapsedNanos);
};

const appendLogEntry: TimestampAppendPrimitive = (traceRoot, buffer, entryType) => {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- WASM primitive table is only installed on WasmTraceRoot instances.
  const root = traceRoot as WasmTraceRoot;
  root._assertLive();
  if (isWasmSpanBuffer(buffer) && buffer._messagePhysicalLayout !== 'packed' && buffer._identityOwner) {
    return root.allocator.writeLogEntry(
      buffer._systemPtr,
      buffer._identityPtr,
      root._traceRootPtr,
      entryType,
      buffer._capacity,
    );
  }
  const idx = buffer._writeIndex;
  const entryTypes = buffer.entry_type;
  if (entryTypes === undefined) throw new TypeError('Split WASM appender requires entry_type storage');
  buffer.timestamp[idx] = timestampNow(root);
  entryTypes[idx] = entryType;
  buffer._writeIndex = idx + 1;
  return idx;
};

const writeSpanStartPrimitive: SpanStartPrimitive = (traceRoot, buffer, spanName) => {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- WASM primitive table is only installed on WasmTraceRoot instances.
  const root = traceRoot as WasmTraceRoot;
  root._assertLive();
  if (isWasmSpanBuffer(buffer) && buffer._messagePhysicalLayout !== 'packed') {
    if (!consumeSpanStartedAtAllocation(buffer)) {
      root.allocator.spanStart(buffer._systemPtr, buffer._identityPtr, root._traceRootPtr, buffer._capacity);
    }
    buffer._message[0] = spanName;
    return;
  }
  const entryTypes = buffer.entry_type;
  if (entryTypes === undefined) throw new TypeError('Split WASM span-start appender requires entry_type storage');
  buffer.timestamp[0] = timestampNow(root);
  entryTypes[0] = ENTRY_TYPE_SPAN_START;
  buffer.message(0, spanName);
  entryTypes[1] = ENTRY_TYPE_SPAN_EXCEPTION;
  buffer.timestamp[1] = 0n;
  buffer._writeIndex = 2;
};

const writeSpanEndPrimitive: SpanEndPrimitive = (traceRoot, buffer, entryType) => {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- WASM primitive table is only installed on WasmTraceRoot instances.
  const root = traceRoot as WasmTraceRoot;
  root._assertLive();
  if (isWasmSpanBuffer(buffer) && buffer._messagePhysicalLayout !== 'packed') {
    if (entryType === 2) root.allocator.spanEndOk(buffer._systemPtr, root._traceRootPtr, buffer._capacity);
    else root.allocator.spanEndErr(buffer._systemPtr, root._traceRootPtr, buffer._capacity);
    if (entryType !== 2 && entryType !== 3) {
      const splitEntryTypes = buffer.entry_type;
      if (splitEntryTypes === undefined) throw new TypeError('Split WASM buffer is missing entry_type storage');
      splitEntryTypes[1] = entryType;
    }
    return;
  }
  const entryTypes = buffer.entry_type;
  if (entryTypes === undefined) throw new TypeError('Split WASM span-end appender requires entry_type storage');
  buffer.timestamp[1] = timestampNow(root);
  entryTypes[1] = entryType;
};

export class WasmTraceRoot<T extends LogSchema = LogSchema> implements ITraceRoot<T> {
  /** Byte offset in WASM memory where TraceRoot data lives */
  readonly _traceRootPtr: number;
  private _state: 'live' | 'freed' = 'live';

  /** The WASM allocator instance */
  readonly allocator: WasmAllocator;

  /** Trace ID string */
  readonly trace_id: TraceId;
  readonly _traceIdBytes: Uint8Array;

  /** Tracer reference for lifecycle hooks */
  readonly tracer: TracerLifecycleHooks<T>;
  readonly _topology: TraceTopology;
  readonly _timestampNow = timestampNow;
  readonly _appendLogEntry = appendLogEntry;
  readonly _writeSpanStart = writeSpanStartPrimitive;
  readonly _writeSpanEnd = writeSpanEndPrimitive;

  /**
   * Raw backing buffer - not used for WASM, but required by ITraceRoot.
   * We create a minimal ArrayBuffer just to satisfy the interface.
   */
  readonly _system: ArrayBuffer;

  constructor(allocator: WasmAllocator, trace_id: TraceId, tracer: TracerLifecycleHooks<T>) {
    this.allocator = allocator;
    this.trace_id = trace_id;
    this._traceIdBytes = traceIdEncoder.encode(trace_id);
    this.tracer = tracer;
    this._topology = new TraceTopology();

    this._system = new ArrayBuffer(16);
    // Native TraceRoot is exactly two aligned 8-byte fields.
    const traceRootPtr = allocator.allocExact(16, 8);
    if (traceRootPtr === 0) {
      throw new Error('WASM trace-root allocation failed');
    }
    this._traceRootPtr = traceRootPtr;
    allocator.initTraceRoot(this._traceRootPtr);
  }
  _assertLive(): void {
    if (this._state !== 'live') throw new Error('WASM trace root has been released');
  }

  /**
   * Epoch time in nanoseconds when trace was created.
   * Read from WASM memory at _traceRootPtr offset 0.
   */
  get anchorEpochNanos(): bigint {
    this._assertLive();
    // i64 view is indexed by 8-byte chunks
    return this.allocator.i64[this._traceRootPtr / 8];
  }

  /**
   * High-resolution timer anchor when trace was created.
   * Read from WASM memory at _traceRootPtr offset 8.
   */
  get anchorPerfNow(): number {
    this._assertLive();
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
    return this._timestampNow(this);
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
    this._writeSpanStart(this, buffer, spanName);
  }

  /**
   * Write span-end entry to buffer at row 1.
   * Writes both timestamp and entry_type.
   *
   * @param buffer - SpanBuffer to write to
   * @param entryType - Entry type (SPAN_OK, SPAN_ERR, or SPAN_EXCEPTION)
   */
  writeSpanEnd(buffer: AnySpanBuffer, entryType: number): void {
    this._writeSpanEnd(this, buffer, entryType);
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
    return this._appendLogEntry(this, buffer, entryType);
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
    this._assertLive();
    this.allocator.spanStart(systemPtr, identityPtr, this._traceRootPtr);
  }

  /**
   * Write span-ok to row 1 using pointer.
   * @param systemPtr - Byte offset of buffer's system block in WASM memory
   */
  writeSpanEndOkPtr(systemPtr: number): void {
    this._assertLive();
    this.allocator.spanEndOk(systemPtr, this._traceRootPtr);
  }

  /**
   * Write span-err to row 1 using pointer.
   * @param systemPtr - Byte offset of buffer's system block in WASM memory
   */
  writeSpanEndErrPtr(systemPtr: number): void {
    this._assertLive();
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
    this._assertLive();
    return this.allocator.writeLogEntry(systemPtr, identityPtr, this._traceRootPtr, entryType);
  }

  /**
   * Read current write index from buffer's identity block.
   * @param identityPtr - Byte offset of buffer's identity block in WASM memory
   */
  readWriteIndex(identityPtr: number): number {
    this._assertLive();
    return this.allocator.readWriteIndex(identityPtr);
  }

  /** Mark the handle stale when its allocator is reset wholesale. */
  invalidate(): void {
    this._state = 'freed';
  }

  /** Free the TraceRoot allocation exactly once. */
  free(): void {
    if (this._state === 'freed') return;
    this.allocator.freeExact(this._traceRootPtr, 16, 8);
    this._state = 'freed';
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
export function createWasmTraceRoot<T extends LogSchema = LogSchema>(
  allocator: WasmAllocator,
  trace_id: string,
  tracer: TracerLifecycleHooks<T>,
): WasmTraceRoot<T> {
  return new WasmTraceRoot<T>(allocator, createTraceId(trace_id), tracer);
}

/**
 * Factory function to create WasmTraceRoot factory.
 *
 * This returns a factory function that can be passed to Tracer constructor.
 *
 * @param allocator - WASM allocator instance
 * @returns Factory function compatible with TraceRootFactory type
 */
export function createWasmTraceRootFactory<T extends LogSchema = LogSchema>(
  allocator: WasmAllocator,
): TraceRootFactory<T> {
  return (trace_id: string, tracer: TracerLifecycleHooks<T>): WasmTraceRoot<T> =>
    new WasmTraceRoot<T>(allocator, createTraceId(trace_id), tracer);
}
//#endregion smoo/lmao!n/wasm-mem.trace-root
