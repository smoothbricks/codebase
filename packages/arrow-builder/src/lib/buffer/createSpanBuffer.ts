/**
 * Create SpanBuffer with native TypedArrays
 * 
 * Per specs/01b_columnar_buffer_architecture.md:
 * - Cache-aligned TypedArrays (64-byte boundaries)
 * - Null bitmap management (one Uint8Array per nullable column per Arrow spec)
 * - Per-span buffers (no traceId/spanId arrays needed)
 * - Direct TypedArray writes in hot path
 * - Arrow conversion in cold path (background processing)
 */

import type { SpanBuffer, TaskContext, TypedArray } from './types.js';
import type { TagAttributeSchema } from '../schema-types.js';
import { createAttributeColumns } from './createBuilders.js';

let nextGlobalSpanId = 1;

/**
 * Get cache-aligned capacity
 * 
 * Aligns to 64-byte cache line boundaries for optimal CPU performance.
 * Uses 1-byte element size (worst case) to ensure ALL array types are aligned.
 */
function getCacheAlignedCapacity(elementCount: number): number {
  const CACHE_LINE_SIZE = 64; // Cache line size in bytes
  const totalBytes = elementCount * 1; // Use 1 byte (worst case - Uint8Array)
  const alignedBytes = Math.ceil(totalBytes / CACHE_LINE_SIZE) * CACHE_LINE_SIZE;
  return alignedBytes; // Return byte count which becomes element count for Uint8Array
}

/**
 * Create empty SpanBuffer with native TypedArrays
 * 
 * Per specs/01b_columnar_buffer_architecture.md:
 * - Each span gets its own buffer
 * - traceId and spanId are constant per buffer (stored as properties)
 * - All TypedArrays have equal length (columnar storage requirement)
 * - Null bitmaps: one Uint8Array per nullable column (Arrow format)
 * 
 * @param spanId - Unique span identifier
 * @param traceId - Trace ID from request context (passed through all spans)
 * @param schema - Tag attribute schema
 * @param taskContext - Task context with module metadata
 * @param parentBuffer - Optional parent buffer for tree structure
 * @param requestedCapacity - Requested buffer capacity
 */
export function createEmptySpanBuffer(
  spanId: number,
  traceId: string,
  schema: TagAttributeSchema,
  taskContext: TaskContext,
  parentBuffer?: SpanBuffer,
  requestedCapacity: number = 64
): SpanBuffer {
  // Cache-align capacity for all arrays
  const alignedCapacity = getCacheAlignedCapacity(requestedCapacity);
  
  // Create core columns
  const timestamps = new Float64Array(alignedCapacity);
  const operations = new Uint8Array(alignedCapacity);
  
  // Create null bitmaps - one Uint8Array per nullable column (Arrow format)
  // Each bitmap stores 8 rows per byte (1 bit per row)
  // Length = Math.ceil(alignedCapacity / 8) bytes
  const nullBitmaps: Record<`attr_${string}`, Uint8Array> = {};
  const bitmapByteLength = Math.ceil(alignedCapacity / 8);
  
  for (const fieldName of Object.keys(schema)) {
    const columnName = `attr_${fieldName}` as `attr_${string}`;
    nullBitmaps[columnName] = new Uint8Array(bitmapByteLength);
  }
  
  // Create attribute columns from schema
  const attributeColumns = createAttributeColumns(schema, alignedCapacity);
  
  // Type-safe spread of attribute columns
  // We know attributeColumns has keys like attr_${string}, which matches SpanBuffer's index signature
  const buffer: SpanBuffer = {
    spanId,
    traceId, // TraceId from request context (constant across all spans in trace)
    timestamps,
    operations,
    nullBitmaps,
    ...(attributeColumns as Record<`attr_${string}`, TypedArray>),
    children: [],
    parent: parentBuffer,
    task: taskContext,
    writeIndex: 0,
    capacity: requestedCapacity, // Logical capacity for overflow detection
    next: undefined
  };
  
  taskContext.module.spanBufferCapacityStats.totalBuffersCreated++;
  
  return buffer;
}

/**
 * Create root SpanBuffer for new trace
 * 
 * @param schema - Tag attribute schema
 * @param taskContext - Task context with module metadata
 * @param traceId - Trace ID from request context
 * @param capacity - Optional buffer capacity
 */
export function createSpanBuffer(
  schema: TagAttributeSchema,
  taskContext: TaskContext,
  traceId: string,
  capacity?: number
): SpanBuffer {
  const spanId = nextGlobalSpanId++;
  return createEmptySpanBuffer(spanId, traceId, schema, taskContext, undefined, capacity);
}

/**
 * Create child SpanBuffer
 * Inherits traceId from parent (all spans in same trace share traceId)
 */
export function createChildSpanBuffer(
  parentBuffer: SpanBuffer,
  taskContext: TaskContext
): SpanBuffer {
  const spanId = nextGlobalSpanId++;
  const schema = parentBuffer.task.module.tagAttributes;
  const capacity = parentBuffer.capacity;
  
  const childBuffer = createEmptySpanBuffer(
    spanId,
    parentBuffer.traceId, // Inherit traceId from parent
    schema,
    taskContext,
    parentBuffer,
    capacity
  );
  
  parentBuffer.children.push(childBuffer);
  
  return childBuffer;
}

/**
 * Create next buffer in chain for overflow handling
 * 
 * Per specs/01b_columnar_buffer_architecture.md:
 * - Buffer chaining is part of self-tuning mechanism
 * - Chained buffer inherits spanId and traceId (continuation)
 * - Same parent, same schema, same task context
 */
export function createNextBuffer(buffer: SpanBuffer): SpanBuffer {
  const schema = buffer.task.module.tagAttributes;
  const capacity = buffer.task.module.spanBufferCapacityStats.currentCapacity;
  
  const nextBuffer = createEmptySpanBuffer(
    buffer.spanId,     // Same logical span
    buffer.traceId,    // Same trace (continuation)
    schema,
    buffer.task,       // Same task context
    buffer.parent,     // Same parent
    capacity
  );
  
  // Link current buffer to next
  buffer.next = nextBuffer;
  
  return nextBuffer;
}
