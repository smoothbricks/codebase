/**
 * JsBufferStrategy - Default buffer strategy using JavaScript TypedArrays.
 *
 * Uses GC-managed memory with zero-copy Arrow conversion via apache-arrow-js.
 *
 * Memory characteristics:
 * - Buffers are regular JS objects with TypedArray properties
 * - GC handles deallocation (no explicit release needed)
 * - Arrow conversion shares ArrayBuffer references (zero-copy)
 *
 * @module JsBufferStrategy
 */

import type { RecordBatch, Table } from 'apache-arrow';
import type { BufferStrategy } from './bufferStrategy.js';
import { convertSpanTreeToArrowTable, convertToRecordBatch } from './convertToArrow.js';
import type { OpMetadata } from './opContext/opTypes.js';
import type { LogSchema } from './schema/LogSchema.js';
import {
  createChildSpanBuffer as createChildSpanBufferImpl,
  createOverflowBuffer as createOverflowBufferImpl,
  createSpanBuffer as createSpanBufferImpl,
  getSpanBufferClass,
  type SpanBufferConstructor,
} from './spanBuffer.js';
import type { ITraceRoot } from './traceRoot.js';
import type { AnySpanBuffer, SpanBuffer } from './types.js';

/**
 * JsBufferStrategy - Default buffer strategy using GC-managed TypedArrays.
 *
 * This is the standard strategy for most use cases:
 * - Simple memory model (GC handles cleanup)
 * - Zero-copy Arrow conversion (shares ArrayBuffer references)
 * - No explicit release needed (but releaseBuffer is a no-op)
 *
 * @example
 * ```typescript
 * const strategy = new JsBufferStrategy<MySchema>();
 * const tracer = new StdioTracer(binding, strategy, options);
 * ```
 */
export class JsBufferStrategy<T extends LogSchema = LogSchema> implements BufferStrategy<T> {
  /**
   * Cache of SpanBuffer classes per schema.
   * Populated lazily on first buffer creation.
   */
  private spanBufferClassCache = new WeakMap<T, SpanBufferConstructor>();

  /**
   * Get or create SpanBuffer class for a schema.
   */
  private getSpanBufferClassForSchema(schema: T): SpanBufferConstructor {
    let cached = this.spanBufferClassCache.get(schema);
    if (!cached) {
      cached = getSpanBufferClass(schema);
      this.spanBufferClassCache.set(schema, cached);
    }
    return cached;
  }

  createSpanBuffer(
    schema: T,
    spanName: string,
    traceRoot: ITraceRoot,
    opMetadata: OpMetadata,
    capacity?: number,
  ): SpanBuffer<T> {
    return createSpanBufferImpl(schema, spanName, traceRoot, opMetadata, capacity);
  }

  createChildSpanBuffer(
    parentBuffer: SpanBuffer<T>,
    callsiteMetadata: OpMetadata,
    opMetadata: OpMetadata,
    capacity?: number,
    schema?: T,
  ): SpanBuffer<T> {
    // Use provided schema (for cross-library calls) or parent's schema
    const childSchema = schema ?? (parentBuffer._logSchema as T);
    const SpanBufferClass = this.getSpanBufferClassForSchema(childSchema);
    return createChildSpanBufferImpl(parentBuffer, SpanBufferClass, callsiteMetadata, opMetadata, capacity);
  }

  createOverflowBuffer(buffer: SpanBuffer<T>): SpanBuffer<T> {
    return createOverflowBufferImpl(buffer);
  }

  toArrowRecordBatch(buffer: AnySpanBuffer): RecordBatch {
    // Uses existing zero-copy conversion
    return convertToRecordBatch(buffer);
  }

  toArrowTable(buffer: AnySpanBuffer): Table {
    // Uses existing tree conversion with shared dictionaries
    return convertSpanTreeToArrowTable(buffer);
  }

  releaseBuffer(_buffer: AnySpanBuffer): void {
    // No-op for JS strategy - GC handles memory
    // The buffer will be collected when no references remain
  }
}
