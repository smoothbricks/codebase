/**
 * BufferStrategy - Abstraction for SpanBuffer memory management and Arrow conversion.
 *
 * This interface separates memory management concerns from sink concerns (Tracer):
 * - BufferStrategy: HOW buffers are allocated and converted (JS TypedArrays vs WASM)
 * - Tracer (sink): WHAT happens with completed buffers (stdio, queue, test accumulation)
 *
 * Key responsibilities:
 * 1. Buffer creation (root, child, overflow)
 * 2. Arrow conversion (strategy knows optimal path)
 * 3. Buffer release (GC for JS, freelist for WASM)
 *
 * The sink (Tracer subclass) controls WHEN to convert and release:
 * - StdioTracer: prints then releases immediately
 * - TestTracer: holds buffers until clear()
 * - SqsTracer: accumulates, converts on flush(), then releases
 *
 * @module bufferStrategy
 */

import type { Table } from '@uwdata/flechette';
import type { OpMetadata } from './opContext/opTypes.js';
import type { LogSchema } from './schema/LogSchema.js';
import type { ITraceRoot } from './traceRoot.js';
import type { AnySpanBuffer, SpanBuffer } from './types.js';

/**
 * BufferStrategy interface for memory management and Arrow conversion.
 *
 * Implementations:
 * - JsBufferStrategy: GC-managed JS TypedArrays, zero-copy Arrow via flechette
 * - WasmBufferStrategy: WASM memory with freelist, Arrow built in WASM
 * - (future) NapiBufferStrategy: Native code with SharedArrayBuffer
 *
 * @typeParam T - LogSchema type for type-safe buffer access
 */
export interface BufferStrategy<T extends LogSchema = LogSchema> {
  /**
   * Create a root SpanBuffer for a new trace.
   *
   * @param schema - LogSchema defining column types
   * @param spanName - Name of the root span
   * @param traceRoot - Platform-specific trace root with timestamp anchors
   * @param opMetadata - Metadata for the executing op
   * @param capacity - Optional capacity override
   * @returns New SpanBuffer for the root span
   */
  createSpanBuffer(
    schema: T,
    spanName: string,
    traceRoot: ITraceRoot,
    opMetadata: OpMetadata,
    capacity?: number,
  ): SpanBuffer<T>;

  /**
   * Create a child span buffer linked to a parent.
   *
   * @param parentBuffer - Parent buffer for tree linkage
   * @param spanName - Name for this span
   * Note: Caller must call writeSpanStart() after this to set span name in message_values[0].
   *
   * @param callsiteMetadata - Metadata for WHERE span() was called (row 0)
   * @param opMetadata - Metadata for WHAT op is executing (rows 1+)
   * @param capacity - Optional capacity override
   * @param schema - Optional schema for the child (if different from parent, e.g., library ops)
   * @returns New SpanBuffer linked to parent
   */
  createChildSpanBuffer(
    parentBuffer: SpanBuffer<T>,
    callsiteMetadata: OpMetadata,
    opMetadata: OpMetadata,
    capacity?: number,
    schema?: T,
  ): SpanBuffer<T>;

  /**
   * Create an overflow buffer when current buffer is full.
   *
   * @param buffer - Full buffer that needs overflow storage
   * @returns New SpanBuffer linked via buffer._overflow
   */
  createOverflowBuffer(buffer: SpanBuffer<T>): SpanBuffer<T>;

  /**
   * Convert a SpanBuffer tree to Arrow Table.
   *
   * Each strategy knows the optimal conversion path:
   * - JS: Zero-copy wrap TypedArrays via flechette
   * - WASM: Build Arrow in WASM memory, return table
   * - NAPI: Call into native code for parallel conversion
   *
   * @param buffer - Root buffer to convert (includes children and overflow chain)
   * @returns Arrow Table (sync) or Promise<Table> (async)
   */
  toArrowTable(buffer: AnySpanBuffer): Table | Promise<Table>;

  /**
   * Release a SpanBuffer's memory resources.
   *
   * Called by the sink when it's done with a buffer.
   *
   * - JsBufferStrategy: No-op (GC handles it)
   * - WasmBufferStrategy: Returns blocks to freelist
   *
   * The sink decides WHEN to release:
   * - After printing (StdioTracer)
   * - On clear() (TestTracer)
   * - After Arrow conversion (SqsTracer)
   *
   * @param buffer - Buffer to release (and its entire tree)
   */
  releaseBuffer(buffer: AnySpanBuffer): void;
}
