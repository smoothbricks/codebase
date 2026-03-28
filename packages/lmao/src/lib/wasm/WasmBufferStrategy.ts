/**
 * WasmBufferStrategy - Buffer strategy using WASM memory with freelist allocation.
 *
 * Uses WebAssembly memory for numeric columns with O(1) freelist allocation/deallocation.
 * String columns remain in JavaScript.
 *
 * Memory characteristics:
 * - Numeric data lives in contiguous WASM memory (better cache locality)
 * - Freelist enables O(1) memory reuse across traces
 * - Explicit release required (not GC-managed)
 * - Arrow conversion reads from WASM memory views
 *
 * @module WasmBufferStrategy
 */

import type { Table } from '@uwdata/flechette';
import type { BufferStrategy } from '../bufferStrategy.js';
import { convertSpanTreeToArrowTable } from '../convertToArrow.js';
import type { OpMetadata } from '../opContext/opTypes.js';
import type { LogSchema } from '../schema/LogSchema.js';
import { EMPTY_SCOPE } from '../spanBuffer.js';
import type { ITraceRoot } from '../traceRoot.js';
import type { AnySpanBuffer, SpanBuffer } from '../types.js';
import { createWasmAllocator, type WasmAllocator, type WasmAllocatorOptions } from './wasmAllocator.js';
import {
  createWasmChildSpanBuffer,
  createWasmOverflowBuffer,
  createWasmSpanBuffer,
  isWasmSpanBufferInstance,
  type WasmSpanBufferInstance,
} from './wasmSpanBuffer.js';

function requireWasmSpanBuffer(buffer: AnySpanBuffer): WasmSpanBufferInstance {
  if (!isWasmSpanBufferInstance(buffer)) {
    throw new Error('Expected WASM-backed span buffer');
  }
  return buffer;
}

/**
 * Options for WasmBufferStrategy.
 */
export interface WasmBufferStrategyOptions extends WasmAllocatorOptions {
  /** Pre-created allocator. If not provided, one will be created. */
  allocator?: WasmAllocator;
}

type WasmAllocatorStats = {
  allocCount: number;
  freeCount: number;
  bumpPtr: number;
  capacity: number;
};

/**
 * WasmBufferStrategy - Buffer strategy using WASM memory with freelist allocation.
 *
 * This strategy is optimized for high-throughput scenarios:
 * - Numeric columns allocated from WASM freelists (O(1) alloc/free)
 * - Better cache locality from contiguous WASM memory
 * - Explicit memory management via releaseBuffer()
 *
 * @example
 * ```typescript
 * const strategy = await WasmBufferStrategy.create<MySchema>();
 * const tracer = new StdioTracer(binding, { bufferStrategy: strategy });
 * ```
 */
export class WasmBufferStrategy<T extends LogSchema = LogSchema> implements BufferStrategy<T> {
  /**
   * The WASM allocator instance.
   * Shared across all buffers created by this strategy.
   */
  readonly allocator: WasmAllocator;

  /**
   * Private constructor - use static create() method.
   */
  private constructor(allocator: WasmAllocator) {
    this.allocator = allocator;
  }

  /**
   * Create a WasmBufferStrategy instance.
   *
   * Async because WASM module loading may be asynchronous.
   *
   * @param options - Configuration options
   * @returns Promise<WasmBufferStrategy>
   */
  static async create<T extends LogSchema = LogSchema>(
    options?: WasmBufferStrategyOptions,
  ): Promise<WasmBufferStrategy<T>> {
    const allocator = options?.allocator ?? (await createWasmAllocator(options));
    return new WasmBufferStrategy<T>(allocator);
  }

  createSpanBuffer(schema: T, traceRoot: ITraceRoot, opMetadata: OpMetadata, capacity?: number): SpanBuffer<T> {
    const effectiveCapacity = capacity ?? this.allocator.capacity;

    // Create WASM buffer
    const wasmBuffer = createWasmSpanBuffer(
      schema,
      {
        allocator: this.allocator,
        capacity: effectiveCapacity,
        trace_id: traceRoot.trace_id,
        // thread_id and span_id come from WASM (global header and allocator respectively)
        thread_id: 0n, // Will be read from WASM header
        span_id: 0, // Will be assigned by WASM allocator
      },
      traceRoot, // _traceRoot
      EMPTY_SCOPE, // _scopeValues
      opMetadata, // _opMetadata
      opMetadata, // _callsiteMetadata (same as opMetadata for root)
    );

    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- generated WASM buffers implement the SpanBuffer contract for the same schema.
    return wasmBuffer as unknown as SpanBuffer<T>;
  }

  createChildSpanBuffer(
    parentBuffer: SpanBuffer<T>,
    callsiteMetadata: OpMetadata,
    opMetadata: OpMetadata,
    capacity?: number,
    schema?: T,
  ): SpanBuffer<T> {
    const wasmParent = requireWasmSpanBuffer(parentBuffer);
    const effectiveCapacity = capacity ?? wasmParent._capacity;

    // Use provided schema (for cross-library calls) or parent's schema
    const childSchema = schema ?? (parentBuffer._logSchema as T);

    const child = createWasmChildSpanBuffer(
      wasmParent,
      {
        allocator: this.allocator,
        capacity: effectiveCapacity,
        thread_id: 0n, // Will be read from WASM header
        span_id: 0, // Will be assigned by WASM allocator
        schema: childSchema, // Pass schema for correct buffer class
      },
      parentBuffer._traceRoot, // _traceRoot (inherit from parent)
      parentBuffer._scopeValues, // _scopeValues (inherit from parent)
      opMetadata, // _opMetadata
      callsiteMetadata, // _callsiteMetadata
    );

    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- child buffers preserve the requested schema contract.
    return child as unknown as SpanBuffer<T>;
  }

  createOverflowBuffer(buffer: SpanBuffer<T>): SpanBuffer<T> {
    const wasmBuffer = requireWasmSpanBuffer(buffer);
    const overflow = createWasmOverflowBuffer(
      wasmBuffer,
      buffer._traceRoot, // _traceRoot (same as original)
      buffer._scopeValues, // _scopeValues (same as original)
      buffer._opMetadata, // _opMetadata (same as original)
      buffer._callsiteMetadata ?? buffer._opMetadata, // _callsiteMetadata (same as original, fallback to opMetadata)
    );

    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- overflow buffers are schema-compatible continuations of the same span.
    return overflow as unknown as SpanBuffer<T>;
  }

  toArrowTable(buffer: AnySpanBuffer): Table {
    // Uses existing tree conversion with shared dictionaries
    return convertSpanTreeToArrowTable(buffer);
  }

  releaseBuffer(buffer: AnySpanBuffer): void {
    // Walk the span tree and free all WASM memory
    this.freeSpanTree(requireWasmSpanBuffer(buffer));
  }

  /**
   * Recursively free all WASM memory for a span tree.
   */
  private freeSpanTree(buffer: WasmSpanBufferInstance): void {
    // Free children first (depth-first)
    if (buffer._children) {
      for (const child of buffer._children) {
        this.freeSpanTree(child);
      }
    }

    // Free overflow chain
    if (buffer._overflow) {
      this.freeSpanTree(buffer._overflow);
    }

    // Free this buffer's WASM memory
    if (typeof buffer.free === 'function') {
      buffer.free();
    }
  }

  /**
   * Reset the allocator (for testing/benchmarking).
   * WARNING: This invalidates ALL buffers created by this strategy.
   */
  reset(): void {
    this.allocator.reset();
  }

  /**
   * Get allocator statistics for debugging/monitoring.
   */
  getStats(): WasmAllocatorStats {
    return {
      allocCount: this.allocator.getAllocCount(),
      freeCount: this.allocator.getFreeCount(),
      bumpPtr: this.allocator.getBumpPtr(),
      capacity: this.allocator.capacity,
    };
  }
}
