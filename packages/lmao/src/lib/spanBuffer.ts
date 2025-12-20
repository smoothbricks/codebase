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
} from '@smoothbricks/arrow-builder';
import type { LogSchema } from './schema/LogSchema.js';
import { textEncoder } from './spanBufferHelpers.js';
import { writeThreadIdToUint64Array } from './threadId.js';
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
    buffer._writeIndex = writeIndex;
  },

  /**
   * Get buffer writeIndex (for testing)
   * Accepts ColumnBuffer (from ColumnWriter._buffer) but casts to SpanBuffer
   * since at runtime all buffers are SpanBuffer instances
   */
  getWriteIndex(buffer: ColumnBuffer): number {
    // At runtime, ColumnBuffer is actually SpanBuffer (SpanBuffer extends TypedColumnBuffer extends ColumnBuffer)
    // Cast internally so callers don't need to
    return (buffer as unknown as SpanBuffer)._writeIndex;
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
    constructorParams: 'module, spanName, parent, isChained, traceId, callsiteModule',
    dependencies: {
      writeThreadIdToUint64Array,
      textEncoder,
    },
    preamble: `
      // Thread-local span counter (per-process/worker, see threadId.ts docs)
      if (typeof globalThis.globalSpanCounter === 'undefined') {
        globalThis.globalSpanCounter = 0;
      }
      const spanId = ++globalThis.globalSpanCounter;

      // Store module context directly (no TaskContext wrapper)
      this._module = module;
      this._spanName = spanName;
       this._children = [];
      this._next = undefined;

      // Store callsiteModule for dual module attribution (row 0 vs rows 1+)
      // Per specs/01c_context_flow_and_op_wrappers.md:
      // - Row 0 (span-start): uses callsiteModule for gitSha/packageName/packagePath
      // - Rows 1+ (logs, span-end): uses module
      this._callsiteModule = callsiteModule;

      // Calculate system buffer size
      const systemSize = requestedCapacity * 9; // _timestamps (8*cap) + _operations (1*cap)

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
          const threadIdArray = new BigUint64Array(this._system, systemSize, 1);
          writeThreadIdToUint64Array(threadIdArray, 0); // threadId
          view.setUint32(8, spanId, true); // spanId
          if (traceId) {
            const traceIdUtf8 = textEncoder.encode(traceId);
            this._identity[12] = traceIdUtf8.length;
            this._identity.set(traceIdUtf8, 13);
          }
        }
      }

      // Set up system column views
  this.timestamp = new BigInt64Array(this._system, 0, requestedCapacity);
  this.entry_type = new Uint8Array(this._system, requestedCapacity * 8, requestedCapacity);

      // Create internal aliases for backwards compatibility
      this._timestamps = this.timestamp;
      this._operations = this.entry_type;

      // Initialize system columns to zero
      this.timestamp.fill(0n);
      this.entry_type.fill(0);

      this._writeIndex = 0;
      this._capacity = requestedCapacity;
    `,
    methods: `
    get span_id() {
      return this._identity ? new DataView(this._identity.buffer, this._identity.byteOffset + 8).getUint32(0, true) : 0;
    }

    get thread_id() {
      return this._identity ? new DataView(this._identity.buffer, this._identity.byteOffset).getBigUint64(0, true) : 0n;
    }

    get trace_id() {
      // Walk up parent chain to find root span with trace_id
      let current = this;
      while (current._parent) {
        current = current._parent;
      }
      if (!current._identity) {
        return undefined;
      }
      const len = current._identity[12];
      const traceIdBytes = current._identity.subarray(13, 13 + len);
      return new TextDecoder().decode(traceIdBytes);
    }

    get _hasParent() {
      return this._parent !== undefined;
    }

    get parent_span_id() {
      return this._parent?.span_id ?? 0;
    }

    get parent_thread_id() {
      return this._parent?.thread_id ?? 0n;
    }

    isParentOf(other) {
      return this === other._parent;
    }

    isChildOf(other) {
      return this._parent === other;
    }

    copyThreadIdTo(dest, offset) {
      if (this._identity) {
        const view = new DataView(this._identity.buffer, this._identity.byteOffset);
        const threadId = view.getBigUint64(0, true);
        new DataView(dest.buffer, dest.byteOffset + offset).setBigUint64(0, threadId, true);
      } else {
        new DataView(dest.buffer, dest.byteOffset + offset).setBigUint64(0, 0n, true);
      }
    }

    copyParentThreadIdTo(dest, offset) {
      this._parent?.copyThreadIdTo(dest, offset) ?? new DataView(dest.buffer, dest.byteOffset + offset).setBigUint64(0, 0n, true);
    }
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

  const SpanBufferClass = getSpanBufferClass(schema) as new (
    capacity: number,
    module: ModuleContext,
    spanName: string,
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
 * @returns New SpanBuffer linked via `buffer._next`, with same schema type
 */
export function createNextBuffer<T extends LogSchema>(buffer: SpanBuffer<T>): SpanBuffer<T> {
  const schema = buffer._module.logSchema as T;
  // Ensure capacity is multiple of 8 for byte-aligned null bitmaps
  const capacity = (buffer._module.sb_capacity + 7) & ~7;

  const SpanBufferClass = getSpanBufferClass(schema);
  // Cast buffer to base SpanBuffer for constructor, result back to SpanBuffer<T>
  // Chained buffers inherit callsiteModule and spanName from the original buffer
  const nextBuffer = new SpanBufferClass(
    capacity,
    buffer._module,
    buffer._spanName,
    buffer as SpanBuffer,
    true,
    undefined,
    buffer._callsiteModule,
  ) as SpanBuffer<T>;

  // Link current buffer to next
  buffer._next = nextBuffer;

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
export function createChildSpanBuffer(
  parentBuffer: SpanBuffer,
  module: ModuleContext,
  spanName: string,
  capacity: number = DEFAULT_BUFFER_CAPACITY,
): SpanBuffer {
  const schema = module.logSchema;

  const SpanBufferClass = getSpanBufferClass(schema) as new (
    capacity: number,
    module: ModuleContext,
    spanName: string,
    parent: SpanBuffer | undefined,
    isChained: boolean,
    traceId: TraceId | undefined,
    callsiteModule: ModuleContext | undefined,
  ) => SpanBuffer;

  // Create child buffer with parent reference
  return new SpanBufferClass(
    capacity,
    module,
    spanName,
    parentBuffer as SpanBuffer, // parent
    false, // not chained
    undefined, // no explicit traceId for child
    module, // callsiteModule
  );
}

// ============================================================================
// Constants for Arrow conversion
// ============================================================================

export const THREAD_ID_BYTES = 8;
