/**
 * SpanBuffer creation functions for LMAO trace logging
 *
 * These functions extend the generic ColumnBuffer from arrow-builder
 * with span-specific fields (spanId, traceId, parent, children, task).
 *
 * Per specs/01b_columnar_buffer_architecture.md:
 * - Each span gets its own buffer
 * - traceId and spanId are constant per buffer (stored as properties)
 * - Buffer chaining handles overflow gracefully
 */

import { createColumnBuffer } from '@smoothbricks/arrow-builder';
import type { TagAttributeSchema } from './schema/types.js';
import type { SpanBuffer, TaskContext } from './types.js';

let nextGlobalSpanId = 1;

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
  requestedCapacity = 64,
): SpanBuffer {
  // Create generic column buffer first
  const columnBuffer = createColumnBuffer(schema, requestedCapacity);

  // Extend with span-specific fields
  const buffer: SpanBuffer = {
    ...columnBuffer,
    spanId,
    traceId, // TraceId from request context (constant across all spans in trace)
    children: [],
    parent: parentBuffer,
    task: taskContext,
  };

  taskContext.module.spanBufferCapacityStats.totalBuffersCreated++;

  return buffer;
}

/**
 * Create root SpanBuffer for new trace
 *
 * @param schema - Tag attribute schema
 * @param taskContext - Task context with module metadata
 * @param traceId - Trace ID from request context (defaults to auto-generated if not provided)
 * @param capacity - Optional buffer capacity
 */
export function createSpanBuffer(
  schema: TagAttributeSchema,
  taskContext: TaskContext,
  traceId?: string | number,
  capacity?: number,
): SpanBuffer {
  const spanId = nextGlobalSpanId++;

  // Handle the case where traceId might be a number (capacity) or omitted
  let actualTraceId: string;
  let actualCapacity: number | undefined;

  if (typeof traceId === 'number') {
    // traceId was omitted, this is actually the capacity
    actualCapacity = traceId;
    actualTraceId = `trace-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  } else if (traceId === undefined) {
    // Both traceId and capacity omitted
    actualTraceId = `trace-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    actualCapacity = capacity;
  } else {
    // Normal case: traceId provided as string
    actualTraceId = traceId;
    actualCapacity = capacity;
  }

  return createEmptySpanBuffer(spanId, actualTraceId, schema, taskContext, undefined, actualCapacity);
}

/**
 * Create child SpanBuffer
 * Inherits traceId from parent (all spans in same trace share traceId)
 */
export function createChildSpanBuffer(parentBuffer: SpanBuffer, taskContext: TaskContext): SpanBuffer {
  const spanId = nextGlobalSpanId++;
  const schema = parentBuffer.task.module.tagAttributes;
  const capacity = parentBuffer.capacity;

  const childBuffer = createEmptySpanBuffer(
    spanId,
    parentBuffer.traceId, // Inherit traceId from parent
    schema,
    taskContext,
    parentBuffer,
    capacity,
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
    buffer.spanId, // Same logical span
    buffer.traceId, // Same trace (continuation)
    schema,
    buffer.task, // Same task context
    buffer.parent, // Same parent
    capacity,
  );

  // Link current buffer to next
  buffer.next = nextBuffer;

  return nextBuffer;
}
