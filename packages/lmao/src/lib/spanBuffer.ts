/**
 * SpanBuffer creation functions for LMAO trace logging.
 *
 * This module provides factory functions for creating span-specific buffers that
 * extend the generic ColumnBuffer from arrow-builder with tracing metadata.
 *
 * **Why SpanBuffer vs ColumnBuffer?**
 *
 * The arrow-builder package provides generic columnar storage. SpanBuffer extends
 * this with span-specific metadata:
 * - `spanId`: Unique identifier for this span
 * - `traceId`: Shared across all spans in a request (from RequestContext)
 * - `parent`/`children`: Tree structure for nested spans
 * - `task`: Link to module context for schema and capacity stats
 *
 * **Buffer Memory Layout**
 *
 * LMAO uses a fixed row layout (per specs/01h_entry_types_and_logging_primitives.md):
 * - Row 0: `span-start` entry (reserved)
 * - Row 1: `span-end` entry (pre-initialized as `span-exception`, overwritten by ok/err)
 * - Row 2+: Event entries (ctx.log.*, ctx.tag.*, etc.)
 *
 * **Usage Pattern**
 *
 * ```typescript
 * // 1. Create buffer (writeIndex starts at 0)
 * const buffer = createSpanBuffer(schema, taskContext, traceId);
 *
 * // 2. Initialize fixed row layout (REQUIRED - sets writeIndex to 2)
 * writeSpanStart(buffer, spanName, anchorEpochMicros, anchorPerfNow);
 *
 * // 3. Log events (writeIndex increments from 2)
 * ctx.log.info('Processing...');
 * ctx.tag.userId('u123');
 *
 * // 4. Complete span (writes to row 1)
 * return ctx.ok(result); // or ctx.err('CODE', details)
 * ```
 *
 * **Buffer Chaining**
 *
 * When a buffer fills up, {@link createNextBuffer} creates a continuation buffer.
 * The chained buffer shares the same spanId and traceId (it's the same logical span,
 * just more storage). Overflow stats are tracked for self-tuning capacity.
 *
 * @module spanBuffer
 *
 * @see {@link createSpanBuffer} - Create a root span buffer
 * @see {@link createChildSpanBuffer} - Create a child span buffer
 * @see {@link createNextBuffer} - Create overflow continuation buffer
 */

import { createColumnBuffer } from '@smoothbricks/arrow-builder';
import type { TagAttributeSchema } from './schema/types.js';
import { getThreadId } from './threadId.js';
import type { SpanBuffer, TaskContext } from './types.js';

let nextSpanId = 1;

/**
 * Creates an empty SpanBuffer with native TypedArrays.
 *
 * This is a low-level factory function that creates the raw buffer structure.
 * Most code should use {@link createSpanBuffer} or {@link createChildSpanBuffer} instead.
 *
 * **Buffer Properties:**
 * - Cache-aligned TypedArrays for all columns (64-byte alignment)
 * - Null bitmaps per nullable column (Arrow format: 1 bit per row)
 * - Span metadata (threadId, spanId, traceId, parent/children links)
 *
 * **Important**: Returns buffer with `writeIndex = 0`. Callers MUST call
 * `writeSpanStart()` from lmao.ts to initialize the fixed row layout.
 *
 * @param spanId - Span identifier (auto-generated incrementing number within process)
 * @param threadId - 64-bit thread/worker ID for distributed tracing
 * @param traceId - Trace ID from request context (shared across all spans in trace)
 * @param schema - Tag attribute schema defining column types
 * @param taskContext - Task context with module metadata and capacity stats
 * @param parentBuffer - Optional parent buffer for building span tree
 * @param requestedCapacity - Initial buffer capacity (default: 64, may be adjusted)
 *
 * @returns SpanBuffer with writeIndex = 0 (requires initialization)
 *
 * @internal Use {@link createSpanBuffer} for root spans, {@link createChildSpanBuffer} for children
 */
export function createEmptySpanBuffer(
  spanId: number,
  threadId: bigint,
  traceId: string,
  schema: TagAttributeSchema,
  taskContext: TaskContext,
  parentBuffer?: SpanBuffer,
  requestedCapacity = 64,
): SpanBuffer {
  // Create generic column buffer first
  const columnBuffer = createColumnBuffer(schema, requestedCapacity);

  // Extend the column buffer with span-specific fields
  // IMPORTANT: Use Object.assign to preserve getters/prototype chain!
  // Spreading {...columnBuffer} would evaluate getters and lose lazy allocation
  const buffer = columnBuffer as SpanBuffer;
  buffer.threadId = threadId;
  buffer.spanId = spanId;
  buffer.traceId = traceId; // TraceId from request context (constant across all spans in trace)
  buffer.children = [];
  buffer.parent = parentBuffer;
  buffer.task = taskContext;

  taskContext.module.spanBufferCapacityStats.totalBuffersCreated++;

  return buffer;
}

/**
 * Creates a root SpanBuffer for a new trace or task.
 *
 * This is the primary factory function for creating span buffers. It assigns
 * a unique spanId and either uses the provided traceId or auto-generates one.
 *
 * **When to use:**
 * - Starting a new task from `moduleContext.task()`
 * - Creating a standalone span for testing
 *
 * **After creation:**
 * Call `writeSpanStart()` to initialize the fixed row layout before logging.
 *
 * @param schema - Tag attribute schema defining column types
 * @param taskContext - Task context with module metadata and capacity stats
 * @param traceId - Trace ID from request context (auto-generated if omitted)
 * @param capacity - Buffer capacity (uses module's tuned capacity if omitted)
 *
 * @returns SpanBuffer ready for initialization via `writeSpanStart()`
 *
 * @example
 * ```typescript
 * // With traceId from request context
 * const buffer = createSpanBuffer(schema, taskContext, requestCtx.traceId);
 *
 * // Auto-generate traceId (for testing or standalone spans)
 * const buffer = createSpanBuffer(schema, taskContext);
 * ```
 */
export function createSpanBuffer(
  schema: TagAttributeSchema,
  taskContext: TaskContext,
  traceId?: string | number,
  capacity?: number,
): SpanBuffer {
  const spanId = nextSpanId++;
  const threadId = getThreadId();

  // Handle the case where traceId might be a number (capacity) or omitted
  let actualTraceId: string;
  let actualCapacity: number | undefined;

  if (typeof traceId === 'number') {
    // traceId was omitted, this is actually the capacity
    actualCapacity = traceId;
    actualTraceId = `trace-${Date.now()}-${Math.random().toString(36).substring(2, 11)}`;
  } else if (traceId === undefined) {
    // Both traceId and capacity omitted
    actualTraceId = `trace-${Date.now()}-${Math.random().toString(36).substring(2, 11)}`;
    actualCapacity = capacity;
  } else {
    // Normal case: traceId provided as string
    actualTraceId = traceId;
    actualCapacity = capacity;
  }

  return createEmptySpanBuffer(spanId, threadId, actualTraceId, schema, taskContext, undefined, actualCapacity);
}

/**
 * Creates a child SpanBuffer for nested span operations.
 *
 * Child spans inherit the traceId from their parent, maintaining trace correlation
 * across the entire request. The child is automatically linked to the parent's
 * `children` array for tree traversal during Arrow conversion.
 *
 * **When to use:**
 * - Called by `ctx.span()` to create nested operations
 * - Building span hierarchies (e.g., request → database → query)
 *
 * **Inheritance:**
 * - traceId: Inherited from parent (same distributed trace)
 * - threadId: Fresh from getThreadId() (same process, but allows cross-worker spans)
 * - spanId: Newly generated (unique per span within process)
 * - capacity: Inherited from parent
 * - schema: Inherited from parent's module
 *
 * @param parentBuffer - Parent span's buffer (for traceId and tree structure)
 * @param taskContext - Task context (may differ from parent if calling across modules)
 *
 * @returns Child SpanBuffer linked to parent, ready for `writeSpanStart()`
 *
 * @example
 * ```typescript
 * // Inside ctx.span() implementation
 * const childBuffer = createChildSpanBuffer(parentBuffer, taskContext);
 * writeSpanStart(childBuffer, childName, anchorEpochMicros, anchorPerfNow);
 * ```
 */
export function createChildSpanBuffer(parentBuffer: SpanBuffer, taskContext: TaskContext): SpanBuffer {
  const spanId = nextSpanId++;
  const threadId = getThreadId();
  const schema = parentBuffer.task.module.tagAttributes;
  const capacity = parentBuffer.capacity;

  const childBuffer = createEmptySpanBuffer(
    spanId,
    threadId,
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
 * Creates a continuation buffer when the current buffer overflows.
 *
 * This is part of LMAO's self-tuning buffer system. When a span needs more
 * entries than the current buffer capacity, a chained buffer is created to
 * continue the same logical span.
 *
 * **Key characteristics:**
 * - Same threadId + spanId: The chained buffer is the same logical span
 * - Same traceId: Still part of the same distributed trace
 * - Linked via `next`: Forms a singly-linked list of buffers
 * - Uses tuned capacity: Takes the module's current optimized capacity
 *
 * **Self-tuning mechanism:**
 * Overflow events are tracked in `module.spanBufferCapacityStats`. When
 * overflow rate exceeds thresholds, future buffers get larger capacity.
 *
 * @param buffer - The full buffer that needs overflow handling
 *
 * @returns New SpanBuffer linked via `buffer.next`, ready for writes
 *
 * @example
 * ```typescript
 * // In getBufferWithSpace()
 * while (currentBuffer.writeIndex >= currentBuffer.capacity) {
 *   if (!currentBuffer.next) {
 *     currentBuffer.next = createNextBuffer(currentBuffer);
 *   }
 *   currentBuffer = currentBuffer.next;
 * }
 * ```
 */
export function createNextBuffer(buffer: SpanBuffer): SpanBuffer {
  const schema = buffer.task.module.tagAttributes;
  const capacity = buffer.task.module.spanBufferCapacityStats.currentCapacity;

  const nextBuffer = createEmptySpanBuffer(
    buffer.spanId, // Same logical span
    buffer.threadId, // Same thread ID (continuation of same span)
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
