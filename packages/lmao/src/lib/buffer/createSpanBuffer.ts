/**
 * Create SpanBuffer with Arrow builders
 * 
 * Arrow builders handle:
 * - Cache-aligned memory allocation
 * - Null bitmap management
 * - Automatic resizing
 * - Zero-copy conversion to Arrow vectors
 */

import * as arrow from 'apache-arrow';
import type { SpanBuffer, TaskContext } from './types.js';
import type { TagAttributeSchema } from '../schema/types.js';
import { createAttributeBuilders } from './createBuilders.js';

let nextGlobalSpanId = 1;

/**
 * Create empty SpanBuffer with Arrow builders
 * 
 * Arrow builders handle:
 * - Cache-aligned memory allocation
 * - Null bitmap management
 * - Automatic resizing
 * - Zero-copy conversion to Arrow vectors
 */
export function createEmptySpanBuffer(
  spanId: number,
  schema: TagAttributeSchema,
  taskContext: TaskContext,
  parentBuffer?: SpanBuffer,
  capacity: number = 64
): SpanBuffer {
  // Create core column builders
  const timestampBuilder = new arrow.Float64Builder({
    type: new arrow.Float64(),
    nullValues: [null, undefined]
  });
  
  const operationBuilder = new arrow.Uint8Builder({
    type: new arrow.Uint8(),
    nullValues: [null, undefined]
  });
  
  // Create attribute builders from schema
  const attributeBuilders = createAttributeBuilders(schema, capacity);
  
  const buffer: SpanBuffer = {
    spanId,
    timestampBuilder,
    operationBuilder,
    attributeBuilders,
    children: [],
    parent: parentBuffer,
    task: taskContext,
    writeIndex: 0,
    capacity,
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
