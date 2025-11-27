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
 */
export function createEmptySpanBuffer(
  spanId: number,
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
    traceId: parentBuffer?.traceId || `trace-${spanId}`, // Inherit from parent or generate new
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
 */
export function createSpanBuffer(
  schema: TagAttributeSchema,
  taskContext: TaskContext,
  capacity?: number
): SpanBuffer {
  const spanId = nextGlobalSpanId++;
  return createEmptySpanBuffer(spanId, schema, taskContext, undefined, capacity);
}

/**
 * Create child SpanBuffer
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
    schema,
    taskContext,
    parentBuffer,
    capacity
  );
  
  parentBuffer.children.push(childBuffer);
  
  return childBuffer;
}
