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

import { type ColumnBufferExtension, getColumnBufferClass } from '@smoothbricks/arrow-builder';
import type { TagAttributeSchema } from './schema/types.js';
import { spanBufferHelpers } from './spanBufferHelpers.js';
import { createTraceId, generateTraceId, type TraceId } from './traceId.js';
import type { SpanBuffer, TaskContext } from './types.js';

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

// ============================================================================
// SpanBuffer class generation
// ============================================================================

/**
 * SpanBuffer class constructor type
 */
type SpanBufferConstructor = new (
  capacity: number,
  task: TaskContext,
  parent: SpanBuffer | undefined,
  isChained: boolean,
  traceId: string | undefined,
) => SpanBuffer;

/**
 * Cache for generated SpanBuffer classes per schema.
 * Key is the schema object reference (WeakMap for GC).
 */
const spanBufferClassCache = new WeakMap<TagAttributeSchema, SpanBufferConstructor>();

/**
 * Get or create a SpanBuffer class for the given schema.
 *
 * Uses arrow-builder's extension mechanism to inject:
 * - System columns (_system ArrayBuffer with timestamps, operations, identity)
 * - Span-specific properties (parent, children, task)
 * - Identity getters (spanId, traceId, hasParent, parentSpanId)
 */
function getSpanBufferClass(schema: TagAttributeSchema): SpanBufferConstructor {
  const cached = spanBufferClassCache.get(schema);
  if (cached) {
    return cached;
  }

  // Define extension for arrow-builder's class generator
  const extension: ColumnBufferExtension = {
    constructorParams: 'task, parent, isChained, traceId',
    preamble: `
        // Thread-local span counter (per-process/worker, see threadId.ts docs)
        // This MUST stay in preamble - it's per-class state incremented during construction
        let nextSpanId = 1;
      `,
    constructorCode: `
        // Store task context
        this.task = task;
        this.children = [];
        this.next = undefined;
        
        // Calculate system buffer size
        const systemSize = requestedCapacity * 9; // timestamps (8*cap) + operations (1*cap)
        
        if (isChained && parent) {
          // CHAINED: share identity, only allocate system columns
          // parent here is the buffer we're chaining FROM (to share identity)
          // but our tree parent should be the same as that buffer's parent
          this.parent = parent.parent;
          this._system = new ArrayBuffer(systemSize);
          this._identity = parent._identity; // Shared reference!
        } else if (parent) {
          // CHILD: parent is our tree parent
          this.parent = parent;
          // CHILD: own 12-byte identity (threadId + spanId)
          const identitySize = 12;
          this._system = new ArrayBuffer(systemSize + identitySize);
          this._identity = new Uint8Array(this._system, systemSize, identitySize);
          
          // Set threadId (bytes 0-7) via TaskContext (singleton per process/worker)
          task.copyThreadIdTo(this._identity, 0);
          
          // Set spanId (bytes 8-11, little-endian) using injected helper
          sbHelpers.writeSpanId(this._identity, 8, nextSpanId++);
          
          // Link to parent
          parent.children.push(this);
        } else {
          // ROOT: identity with traceId
          const traceBytes = traceId ? sbHelpers.textEncoder.encode(traceId) : new Uint8Array(0);
          const identitySize = 13 + traceBytes.length;
          this._system = new ArrayBuffer(systemSize + identitySize);
          this._identity = new Uint8Array(this._system, systemSize, identitySize);
          
          // Set threadId (bytes 0-7) via TaskContext (singleton per process/worker)
          task.copyThreadIdTo(this._identity, 0);
          
          // Set spanId (bytes 8-11, little-endian) using injected helper
          sbHelpers.writeSpanId(this._identity, 8, nextSpanId++);
          
          // Set traceId length and bytes
          this._identity[12] = traceBytes.length;
          this._identity.set(traceBytes, 13);
        }
        
        // System columns at FIXED offsets (same for ALL buffer types)
        // These override the ColumnBuffer's _timestamps/_operations with our unified layout
        this._timestamps = new BigInt64Array(this._system, 0, requestedCapacity);
        this._operations = new Uint8Array(this._system, requestedCapacity * 8, requestedCapacity);
        
        // Direct property aliases for system columns (lmao code uses these)
        // These are direct assignments, not getters - V8 hidden class friendly
        this.timestamps = this._timestamps;
        this.operations = this._operations;
        
        // Track buffer creation
        task.module.spanBufferCapacityStats.totalBuffersCreated++;
      `,
    methods: `
        // Aliases for buffer management properties (native getters - V8 optimized)
        get writeIndex() { return this._writeIndex; }
        set writeIndex(v) { this._writeIndex = v; }
        get capacity() { return this._capacity; }
        get next() { return this._next; }
        set next(v) { this._next = v; }
        
        // spanId getter - uses injected helper to read from identity bytes 8-11
        get spanId() {
          return sbHelpers.readSpanId(this._identity, 8);
        }
        
        // traceId getter - walks up parent chain to root, uses helper for decoding
        get traceId() {
          if (this.parent) {
            return this.parent.traceId;
          }
          // Root: decode from identity bytes using helper
          return sbHelpers.decodeTraceId(this._identity, 12);
        }
        
        // hasParent getter
        get hasParent() {
          return this.parent !== undefined;
        }
        
        // parentSpanId getter
        get parentSpanId() {
          return this.parent ? this.parent.spanId : 0;
        }
        
        // isParentOf - O(1) pointer comparison
        isParentOf(other) {
          return this === other.parent;
        }
        
        // isChildOf - O(1) pointer comparison  
        isChildOf(other) {
          return this.parent === other;
        }
        
        // Copy threadId bytes (8 bytes) to destination
        copyThreadIdTo(dest, offset) {
          dest.set(this._identity.subarray(0, 8), offset);
        }
        
        // Copy parent's threadId bytes to destination
        copyParentThreadIdTo(dest, offset) {
          if (this.parent) {
            this.parent.copyThreadIdTo(dest, offset);
          } else {
            for (let i = 0; i < 8; i++) {
              dest[offset + i] = 0;
            }
          }
        }
        
        // Get threadId as BigInt (for Arrow conversion)
        get threadId() {
          const b = this._identity;
          return BigInt(b[0]) | (BigInt(b[1]) << 8n) | (BigInt(b[2]) << 16n) | (BigInt(b[3]) << 24n) |
                 (BigInt(b[4]) << 32n) | (BigInt(b[5]) << 40n) | (BigInt(b[6]) << 48n) | (BigInt(b[7]) << 56n);
        }
      `,
    // Inject helpers as dependency - available as 'sbHelpers' in generated code
    dependencies: {
      sbHelpers: spanBufferHelpers,
    },
  };

  // Generate class using arrow-builder (provides lazy attribute columns)
  const GeneratedClass = getColumnBufferClass(schema, extension);
  const SpanBufferClass = GeneratedClass as unknown as SpanBufferConstructor;

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
 * @param schema - Tag attribute schema defining column types
 * @param taskContext - Task context with module metadata and capacity stats
 * @param traceId - Trace ID (auto-generated if omitted)
 * @param capacity - Buffer capacity (default: 64)
 *
 * @returns SpanBuffer ready for initialization via `writeSpanStart()`
 */
export function createSpanBuffer(
  schema: TagAttributeSchema,
  taskContext: TaskContext,
  traceId?: TraceId | string,
  capacity = 64,
): SpanBuffer {
  // Ensure capacity is multiple of 8 for byte-aligned null bitmaps
  const alignedCapacity = (capacity + 7) & ~7;

  // Resolve traceId
  let resolvedTraceId: string;
  if (traceId === undefined) {
    resolvedTraceId = generateTraceId();
  } else if (typeof traceId === 'string') {
    resolvedTraceId = createTraceId(traceId);
  } else {
    resolvedTraceId = traceId;
  }

  const SpanBufferClass = getSpanBufferClass(schema);
  return new SpanBufferClass(alignedCapacity, taskContext, undefined, false, resolvedTraceId);
}

/**
 * Creates a child SpanBuffer for nested span operations.
 *
 * Child buffers have identity: [threadId(8)][spanId(4)] (12 bytes)
 * Parent reference is via `parent` property pointer.
 *
 * @param parentBuffer - Parent span's buffer
 * @param taskContext - Task context (may differ from parent if calling across modules)
 *
 * @returns Child SpanBuffer linked to parent
 */
export function createChildSpanBuffer(parentBuffer: SpanBuffer, taskContext: TaskContext): SpanBuffer {
  const schema = parentBuffer.task.module.tagAttributes;
  const capacity = parentBuffer.capacity;

  const SpanBufferClass = getSpanBufferClass(schema);
  return new SpanBufferClass(capacity, taskContext, parentBuffer, false, undefined);
}

/**
 * Creates a continuation buffer when the current buffer overflows.
 *
 * Chained buffers SHARE the identity reference from the first buffer
 * (they represent the SAME logical span, just additional storage).
 *
 * @param buffer - The full buffer that needs overflow handling
 *
 * @returns New SpanBuffer linked via `buffer.next`
 */
export function createNextBuffer(buffer: SpanBuffer): SpanBuffer {
  const schema = buffer.task.module.tagAttributes;
  // Ensure capacity is multiple of 8 for byte-aligned null bitmaps
  const capacity = (buffer.task.module.spanBufferCapacityStats.currentCapacity + 7) & ~7;

  const SpanBufferClass = getSpanBufferClass(schema);
  const nextBuffer = new SpanBufferClass(capacity, buffer.task, buffer, true, undefined);

  // Link current buffer to next
  buffer.next = nextBuffer;

  return nextBuffer;
}

// ============================================================================
// Constants for Arrow conversion
// ============================================================================

export const THREAD_ID_BYTES = 8;
