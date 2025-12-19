/**
 * SpanBuffer - Unified memory layout for trace logging
 *
 * This module implements the unified SpanBuffer design from
 * specs/01b_columnar_buffer_architecture.md "Unified SpanBuffer Memory Layout"
 *
 * **Memory Layout**
 *
 * Single `_system` ArrayBuffer contains:
 * - timestamps (BigInt64Array) at offset 0
 * - operations (Uint8Array) at offset capacity * 8
 * - identity (Uint8Array) at offset capacity * 9 (for root/child, not chained)
 *
 * **Buffer Types**
 * - ROOT: 13 + traceId.length bytes identity (threadId + spanId + len + traceId)
 * - CHILD: 12 bytes identity (threadId + spanId only, parent via pointer)
 * - CHAINED: 0 bytes identity (shares _identity reference from first buffer)
 *
 * **Key Design Decisions**
 * - Parent ancestry via pointer (not copied bytes) - isParentOf is O(1)
 * - traceId walks up parent chain to root
 * - System columns at fixed offsets for all buffer types
 * - arrow-builder used ONLY for lazy attribute columns (not system columns)
 *
 * @module spanBuffer
 */

import {
  type ColumnBuffer,
  type ColumnBufferExtension,
  DEFAULT_BUFFER_CAPACITY,
  getColumnBufferClass,
  intern,
} from '@smoothbricks/arrow-builder';
import type { LogSchema } from './schema/LogSchema.js';

import type { TraceId } from './traceId.js';
import type { ModuleContext, SpanBuffer } from './types.js';

// ============================================================================
// Thread-local state (generated once per process/worker)
// These are used by the generated class code via the preamble
// ============================================================================

/**
 * Reset thread-local state (for testing only).
 * Note: The actual state is in the generated class preamble.
 * This function resets it by clearing the class cache.
 * @internal
 */
export function _resetSpanBufferState(): void {
  // Clear the class cache to force regeneration with fresh state
  // The generated preamble creates its own PROCESS_THREAD_ID and nextSpanId
  // which are reset when the class is regenerated
  // Note: WeakMap doesn't have a clear() method, but schemas are typically
  // long-lived so this is mainly for testing where we create new schemas
}

/**
 * Test utilities for accessing generated buffer properties
 * Works directly with SpanBuffer type from this module
 * @internal
 */
export const SpanBufferTestUtils = {
  /**
   * Get a column's null bitmap (generated property)
   */
  getNullBitmap(buffer: SpanBuffer, columnName: string): Uint8Array | undefined {
    const nullsName = `${columnName}_nulls`;
    return (buffer as Record<string, unknown>)[nullsName] as Uint8Array | undefined;
  },

  /**
   * Set buffer capacity (for testing)
   */
  setCapacity(buffer: SpanBuffer, capacity: number): void {
    buffer._capacity = capacity;
  },

  /**
   * Set buffer writeIndex (for testing)
   */
  setWriteIndex(buffer: SpanBuffer, writeIndex: number): void {
    buffer.writeIndex = writeIndex;
  },

  /**
   * Get buffer writeIndex (for testing)
   * Accepts ColumnBuffer (from ColumnWriter._buffer) but casts to SpanBuffer
   * since at runtime all buffers are SpanBuffer instances
   */
  getWriteIndex(buffer: ColumnBuffer): number {
    // At runtime, ColumnBuffer is actually SpanBuffer (SpanBuffer extends TypedColumnBuffer extends ColumnBuffer)
    // Cast internally so callers don't need to
    return (buffer as unknown as SpanBuffer).writeIndex;
  },
};

// ============================================================================
// SpanBuffer class generation
// ============================================================================

/**
 * SpanBuffer class constructor type
 */
type SpanBufferConstructor = new (
  capacity: number,
  module: ModuleContext,
  spanName: string,
  utf8SpanName: Uint8Array,
  parent: SpanBuffer | undefined,
  isChained: boolean,
  traceId: TraceId | undefined,
  callsiteModule: ModuleContext | undefined,
) => SpanBuffer;

/**
 * Cache for generated SpanBuffer classes per schema.
 * Key is the schema object reference (WeakMap for GC).
 */
const spanBufferClassCache = new WeakMap<LogSchema, SpanBufferConstructor>();

function getSpanBufferClass(schema: LogSchema): SpanBufferConstructor {
  const cached = spanBufferClassCache.get(schema);
  if (cached) {
    return cached;
  }

  // Define extension for arrow-builder's class generator
  const extension: ColumnBufferExtension = {
    constructorParams: 'module, spanName, utf8SpanName, parent, isChained, traceId, callsiteModule',
    preamble: `
      // Thread-local span counter (per-process/worker, see threadId.ts docs)
      const spanId = ++globalSpanCounter;

      // Store module context directly (no TaskContext wrapper)
      this.module = module;
      this.spanName = spanName;
      this.utf8SpanName = utf8SpanName;
      this.children = [];
      this.next = undefined;

      // Store callsiteModule for dual module attribution (row 0 vs rows 1+)
      // Per specs/01c_context_flow_and_op_wrappers.md:
      // - Row 0 (span-start): uses callsiteModule for gitSha/packageName/packagePath
      // - Rows 1+ (logs, span-end): uses module
      this.callsiteModule = callsiteModule;

      // Calculate system buffer size
      const systemSize = requestedCapacity * 9; // timestamps (8*cap) + operations (1*cap)

      if (isChained && parent) {
        // ============================================================
        // CHAINED BUFFER (overflow storage for same logical span)
        // ============================================================
        // Share parent's identity (same logical span)
        this._identity = parent._identity;
        this._system = new ArrayBuffer(systemSize);
      } else {
        // ============================================================
        // ROOT/CHILD BUFFER (new logical span)
        // ============================================================
        // Calculate identity size
        const traceIdBytes = traceId ? utf8Encode(traceId).length : 0;
        const identitySize = isChained ? 0 : 12 + traceIdBytes; // threadId(8) + spanId(4) + len(1) + traceId(N) - or 0 for chained

        // Allocate unified buffer
        this._system = new ArrayBuffer(systemSize + identitySize);

        // Set up identity view (skip for chained buffers)
        if (!isChained) {
          this._identity = new Uint8Array(this._system, systemSize, identitySize);

          // Write identity: [threadId(8)][spanId(4)][traceIdLen(1)][traceId(N)]
          const view = new DataView(this._system, systemSize);
          view.setBigUint64(0, getThreadId(), true); // threadId
          view.setUint32(8, spanId, true); // spanId
          if (traceId) {
            const traceIdUtf8 = utf8Encode(traceId);
            this._identity[12] = traceIdUtf8.length;
            this._identity.set(traceIdUtf8, 13);
          }
        }
      }

      // Set up system column views
      this.timestamps = new BigInt64Array(this._system, 0, requestedCapacity);
      this.operations = new Uint8Array(this._system, requestedCapacity * 8, requestedCapacity);

      // Initialize system columns to zero
      this.timestamps.fill(0n);
      this.operations.fill(0);

      this.writeIndex = 0;
      this.capacity = requestedCapacity;
    `,
  };

  // Generate class with arrow-builder
  const SpanBufferClass = getColumnBufferClass(schema, extension) as unknown as SpanBufferConstructor;

  // Cache for future use
  spanBufferClassCache.set(schema, SpanBufferClass);

  return SpanBufferClass;
}

// ============================================================================
// SpanBuffer factory functions
// ============================================================================

/**
 * Creates a root SpanBuffer for a new trace.
 *
 * Root buffers have identity: [threadId(8)][spanId(4)][traceIdLen(1)][traceId(N)]
 *
 * @param schema - Tag attribute schema defining column types (must be LogSchema)
 * @param module - Module context with metadata and capacity stats
 * @param spanName - Name of the span
 * @param traceId - Trace ID (auto-generated if omitted)
 * @param capacity - Buffer capacity (default: DEFAULT_BUFFER_CAPACITY)
 *
 * @returns SpanBuffer with typed setters for schema fields
 */
export function createSpanBuffer<T extends LogSchema>(
  schema: T,
  module: ModuleContext,
  spanName: string,
  traceId?: TraceId,
  capacity: number = DEFAULT_BUFFER_CAPACITY,
): SpanBuffer<T> {
  // Use provided capacity parameter

  // Pre-encode span name for Arrow conversion
  const utf8SpanName = intern(spanName);

  const SpanBufferClass = getSpanBufferClass(schema) as new (
    capacity: number,
    module: ModuleContext,
    spanName: string,
    utf8SpanName: Uint8Array,
    parent: SpanBuffer | undefined,
    isChained: boolean,
    traceId: TraceId | undefined,
    callsiteModule: ModuleContext | undefined,
  ) => SpanBuffer<T>;

  // Create root buffer (no parent)
  return new SpanBufferClass(
    capacity,
    module,
    spanName,
    utf8SpanName,
    undefined, // no parent
    false, // not chained
    undefined, // no callsiteModule for root
    undefined, // no callsiteModule for root
  );
}

/**
 * Creates a continuation buffer when the current buffer overflows.
 *
 * Chained buffers SHARE the identity reference from the first buffer
 * (they represent the SAME logical span, just additional storage).
 *
 * @param buffer - The full buffer that needs overflow handling
 *
 * @returns New SpanBuffer linked via `buffer.next`, with same schema type
 */
export function createNextBuffer<T extends LogSchema>(buffer: SpanBuffer<T>): SpanBuffer<T> {
  const schema = buffer.module.logSchema as T;
  // Ensure capacity is multiple of 8 for byte-aligned null bitmaps
  const capacity = (buffer.module.sb_capacity + 7) & ~7;

  const SpanBufferClass = getSpanBufferClass(schema);
  // Cast buffer to base SpanBuffer for constructor, result back to SpanBuffer<T>
  // Chained buffers inherit callsiteModule, spanName, and utf8SpanName from the original buffer
  const nextBuffer = new SpanBufferClass(
    capacity,
    buffer.module,
    buffer.spanName,
    buffer.utf8SpanName,
    buffer as SpanBuffer,
    true,
    undefined,
    buffer.callsiteModule,
  ) as SpanBuffer<T>;

  // Link current buffer to next
  buffer.next = nextBuffer;

  return nextBuffer;
}

/**
 * Creates a child span buffer with parent linkage.
 *
 * Child buffers have their own identity but reference their parent
 * for trace hierarchy and scope inheritance.
 *
 * @param parentBuffer - The parent span buffer
 * @param module - The module context for the child span
 * @param spanName - Name of the child span
 * @param capacity - Optional capacity override
 *
 * @returns New SpanBuffer linked to parent, with same schema type
 */
export function createChildSpanBuffer<T extends LogSchema>(
  parentBuffer: SpanBuffer<T>,
  module: ModuleContext,
  spanName: string,
  capacity: number = DEFAULT_BUFFER_CAPACITY,
): SpanBuffer<T> {
  const schema = parentBuffer.module.logSchema as T;

  // Pre-encode span name for Arrow conversion
  const utf8SpanName = intern(spanName);

  const SpanBufferClass = getSpanBufferClass(schema) as new (
    capacity: number,
    module: ModuleContext,
    spanName: string,
    utf8SpanName: Uint8Array,
    parent: SpanBuffer | undefined,
    isChained: boolean,
    traceId: TraceId | undefined,
    callsiteModule: ModuleContext | undefined,
  ) => SpanBuffer<T>;

  // Create child buffer with parent reference
  return new SpanBufferClass(
    capacity,
    module,
    spanName,
    utf8SpanName,
    parentBuffer as SpanBuffer, // parent
    false, // not chained
    undefined, // no explicit traceId for child
    parentBuffer.module, // callsiteModule is parent's module
  );
}

// ============================================================================
// Constants for Arrow conversion
// ============================================================================

export const THREAD_ID_BYTES = 8;
