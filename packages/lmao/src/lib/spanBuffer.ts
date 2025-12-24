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
  type AnyColumnBuffer,
  type ColumnBufferExtension,
  DEFAULT_BUFFER_CAPACITY,
  getColumnBufferClass,
} from '@smoothbricks/arrow-builder';
import type { OpMetadata } from './opContext/opTypes.js';
import type { LogSchema } from './schema/LogSchema.js';
import { textEncoder } from './spanBufferHelpers.js';
import { writeThreadIdToUint64Array } from './threadId.js';
import type { TraceId, TraceRoot } from './traceId.js';
import type { AnySpanBuffer, LogBinding, SpanBuffer } from './types.js';

// Re-export TraceRoot for external consumers (and to satisfy linter - it's used in generated code)
export type { TraceRoot };

/**
 * Empty frozen scope object - shared singleton to avoid allocations.
 * Used as default _scopeValues for all buffers until setScope() is called.
 */
export const EMPTY_SCOPE: Readonly<Record<string, unknown>> = Object.freeze({});

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
  getNullBitmap(buffer: AnySpanBuffer, columnName: string): Uint8Array | undefined {
    return buffer.getNullsIfAllocated(columnName);
  },

  /**
   * Set buffer capacity (for testing)
   */
  setCapacity(buffer: AnySpanBuffer, capacity: number): void {
    // Cast to mutable for test manipulation
    (buffer as { _capacity: number })._capacity = capacity;
  },

  /**
   * Set buffer writeIndex (for testing)
   */
  setWriteIndex(buffer: AnySpanBuffer, writeIndex: number): void {
    buffer._writeIndex = writeIndex;
  },

  /**
   * Get buffer writeIndex (for testing)
   * Accepts ColumnBuffer (from ColumnWriter._buffer) but casts to AnySpanBuffer
   * since at runtime all buffers are AnySpanBuffer instances
   */
  getWriteIndex(buffer: AnyColumnBuffer): number {
    // At runtime, ColumnBuffer is actually AnySpanBuffer (SpanBuffer extends TypedSpanBuffer extends AnySpanBuffer)
    // Cast internally so callers don't need to
    return (buffer as unknown as AnySpanBuffer)._writeIndex;
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
  logBinding: LogBinding,
  spanName: string,
  parent: AnySpanBuffer | undefined,
  isChained: boolean,
  trace_id: TraceId | undefined,
  callsiteMetadata: OpMetadata | undefined,
) => AnySpanBuffer;

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

  // ==========================================================================
  // GENERATED CODE EXTENSION
  // ==========================================================================
  // The preamble and methods strings below are passed to new Function() by
  // arrow-builder's columnBufferGenerator. They MUST be pure JavaScript:
  // - NO TypeScript type annotations (: Type)
  // - NO non-null assertions (!)
  // - NO comments (waste of runtime parsing)
  //
  // V8 IN-OBJECT PROPERTY OPTIMIZATION:
  // Property assignment order determines which properties get fast in-object slots.
  // First ~10-12 properties are stored directly on the object (fastest access).
  // Remaining properties go to backing store (extra indirection).
  // Therefore: ASSIGN HOTTEST PROPERTIES FIRST!
  //
  // Hot path analysis (from buffer performance profiling):
  // 1. _writeIndex - incremented on EVERY write
  // 2. _capacity - compared on EVERY write (overflow check)
  // 3. _overflow - checked when overflow occurs
  // 4. timestamp/entry_type - TypedArray refs accessed on EVERY write
  // 5. _children/_parent - modified/accessed during span creation
  // 6. _traceRoot - accessed for timestamp anchors on EVERY write
  // 7. _identity/_logBinding - accessed during span operations
  // 8-N. Cold properties - only accessed during Arrow conversion
  // ==========================================================================

  // Define extension for arrow-builder's class generator
  const extension: ColumnBufferExtension = {
    constructorParams: 'logBinding, spanName, parent, isChained, trace_id, callsiteModule',
    dependencies: {
      writeThreadIdToUint64Array,
      textEncoder,
      EMPTY_SCOPE,
    },
    // Thread-local span counter (per-process/worker, see threadId.ts docs)
    preamble:
      `if (typeof globalThis.globalSpanCounter === 'undefined') {
         globalThis.globalSpanCounter = 0;
       }
       const spanId = ++globalThis.globalSpanCounter;
` +
      // Calculate system buffer size: timestamps (8 bytes * cap) + entry_type (1 byte * cap)
      // Align to 8 bytes so identity's BigUint64Array offset is aligned
      `const rawSystemSize = requestedCapacity * 9;
      const systemSize = (rawSystemSize + 7) & ~7;
      let systemBuffer;
      let identityView;
` +
      // CHAINED BUFFER: overflow storage for same logical span - shares parent identity
      // ROOT/CHILD BUFFER: new logical span - gets own identity section
      // Identity layout: [threadId(8)][spanId(4)][traceIdLen(1)][traceId(N)]
      `if (isChained && parent) {
        systemBuffer = new ArrayBuffer(systemSize);
        identityView = parent._identity;
      } else {
        const traceIdBytes = trace_id ? textEncoder.encode(trace_id).length : 0;
        const identitySize = 13 + traceIdBytes;
        systemBuffer = new ArrayBuffer(systemSize + identitySize);
        identityView = new Uint8Array(systemBuffer, systemSize, identitySize);
        const view = new DataView(systemBuffer, systemSize);
        const threadIdArray = new BigUint64Array(systemBuffer, systemSize, 1);
        writeThreadIdToUint64Array(threadIdArray, 0);
        view.setUint32(8, spanId, true);
        if (trace_id) {
          const traceIdUtf8 = textEncoder.encode(trace_id);
          identityView[12] = traceIdUtf8.length;
          identityView.set(traceIdUtf8, 13);
        }
      }
` +
      // Create TypedArray views for timestamps and entry_type
      `const timestampView = new BigInt64Array(systemBuffer, 0, requestedCapacity);
      const entryTypeView = new Uint8Array(systemBuffer, requestedCapacity * 8, requestedCapacity);
      timestampView.fill(0n);
      entryTypeView.fill(0);
` +
      // Create or copy TraceRoot (per-trace anchoring data)
      // Child/chained spans share parent's traceRoot (O(1) reference copy)
      // Root spans create new TraceRoot with fresh timestamp anchors
      // Platform-specific: Node.js uses process.hrtime.bigint(), Browser uses performance.now()
      `let traceRoot;
      if (parent) {
        traceRoot = parent._traceRoot;
      } else {
        const anchorEpochNanos = BigInt(Date.now()) * 1_000_000n;
        let anchorPerfNow;
        if (typeof process !== 'undefined' && process.hrtime) {
          anchorPerfNow = Number(process.hrtime.bigint());
        } else {
          anchorPerfNow = performance.now();
        }
        const threadId = new DataView(identityView.buffer, identityView.byteOffset).getBigUint64(0, true);
        traceRoot = {
          trace_id: trace_id,
          thread_id: threadId,
          anchorEpochNanos,
          anchorPerfNow,
        };
      }
` +
      // Assign properties in optimal order for V8 in-object slots
      // Slots 1-3: HOTTEST (every log entry), Slots 4-5: HOT (TypedArray refs)
      // Slots 6-7: WARM (tree structure), Slots 8-10: WARM (context)
      // Slots 10+: COLD (Arrow conversion only)
      `this._writeIndex = 0;
      this._capacity = requestedCapacity;
      this._overflow = undefined;
      this.timestamp = timestampView;
      this.entry_type = entryTypeView;
      this._children = [];
      this._parent = isChained ? parent._parent : parent;
      this._traceRoot = traceRoot;
      this._identity = identityView;
      this._logBinding = logBinding;
      this._system = systemBuffer;
      this._spanName = spanName;
      this._scopeValues = parent ? parent._scopeValues : EMPTY_SCOPE;
      this._callsiteMetadata = callsiteModule;
    `,
    // Generated methods for SpanBuffer - no comments in output
    // span_id: extract from identity bytes [8:12]
    // thread_id: extract from identity bytes [0:8]
    // trace_id: walk up parent chain to root, decode trace_id from identity
    // parent_span_id/parent_thread_id: delegate to parent
    // isParentOf/isChildOf: pointer comparison
    // copyThreadIdTo/copyParentThreadIdTo: copy thread_id bytes to destination
    methods: `get span_id() {
      return this._identity ? new DataView(this._identity.buffer, this._identity.byteOffset + 8).getUint32(0, true) : 0;
    }
    get thread_id() {
      return this._identity ? new DataView(this._identity.buffer, this._identity.byteOffset).getBigUint64(0, true) : 0n;
    }
    get trace_id() {
      let current = this;
      while (current._parent) { current = current._parent; }
      if (!current._identity) { return undefined; }
      const len = current._identity[12];
      const traceIdBytes = current._identity.subarray(13, 13 + len);
      return new TextDecoder().decode(traceIdBytes);
    }
    get _hasParent() { return this._parent !== undefined; }
    get parent_span_id() { return this._parent?.span_id ?? 0; }
    get parent_thread_id() { return this._parent?.thread_id ?? 0n; }
    isParentOf(other) { return this === other._parent; }
    isChildOf(other) { return this._parent === other; }
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
 * @param logBinding - LogBinding with schema and capacity stats
 * @param spanName - Name of the span
 * @param traceId - Trace ID (auto-generated if omitted)
 * @param capacity - Buffer capacity (default: DEFAULT_BUFFER_CAPACITY)
 *
 * @returns SpanBuffer with typed setters for schema fields
 */
export function createSpanBuffer<T extends LogSchema>(
  schema: T,
  logBinding: LogBinding,
  spanName: string,
  trace_id?: TraceId,
  capacity: number = DEFAULT_BUFFER_CAPACITY,
): SpanBuffer<T> {
  // Use provided capacity parameter

  const SpanBufferClass = getSpanBufferClass(schema) as new (
    capacity: number,
    logBinding: LogBinding,
    spanName: string,
    parent: AnySpanBuffer | undefined,
    isChained: boolean,
    trace_id: TraceId | undefined,
    callsiteMetadata: any,
  ) => SpanBuffer<T>;

  // Create root buffer (no parent)
  return new SpanBufferClass(
    capacity,
    logBinding,
    spanName,
    undefined, // no parent
    false, // not chained
    trace_id, // trace_id for root
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
 * @returns New SpanBuffer linked via `buffer._overflow`, with same schema type
 */
export function createOverflowBuffer<T extends LogSchema>(buffer: SpanBuffer<T>): SpanBuffer<T> {
  const schema = buffer._logBinding.logSchema as T;
  // Ensure capacity is multiple of 8 for byte-aligned null bitmaps
  const capacity = (buffer._logBinding.sb_capacity + 7) & ~7;

  const SpanBufferClass = getSpanBufferClass(schema);
  // Chained buffers inherit callsiteMetadata and spanName from the original buffer
  // Pass buffer as parent - used for identity sharing; _parent set to buffer._parent in constructor
  const nextBuffer = new SpanBufferClass(
    capacity,
    buffer._logBinding,
    buffer._spanName,
    buffer, // Used for identity sharing, _parent set to buffer._parent in constructor
    true,
    undefined,
    buffer._callsiteMetadata,
  ) as SpanBuffer<T>;

  // Inherit _opMetadata from original buffer (chained buffers are same logical span)
  nextBuffer._opMetadata = buffer._opMetadata;

  // Link current buffer to next
  buffer._overflow = nextBuffer;

  // Track buffer creation for capacity tuning
  buffer._logBinding.sb_totalCreated++;

  return nextBuffer;
}

/**
 * Creates a child span buffer with parent linkage.
 *
 * Child buffers have their own identity but reference their parent
 * for trace hierarchy and scope inheritance.
 *
 * @param parentBuffer - The parent span buffer
 * @param logBinding - The LogBinding for the child span
 * @param spanName - Name of the child span
 * @param capacity - Optional capacity override
 *
 * @returns New SpanBuffer linked to parent, with same schema type
 */
export function createChildSpanBuffer(
  parentBuffer: AnySpanBuffer,
  logBinding: LogBinding,
  spanName: string,
  callsiteMetadata: OpMetadata,
  capacity: number = DEFAULT_BUFFER_CAPACITY,
): AnySpanBuffer {
  const schema = logBinding.logSchema;

  const SpanBufferClass = getSpanBufferClass(schema) as new (
    capacity: number,
    logBinding: LogBinding,
    spanName: string,
    parent: AnySpanBuffer | undefined,
    isChained: boolean,
    trace_id: TraceId | undefined,
    callsiteMetadata: OpMetadata | undefined,
  ) => AnySpanBuffer;

  // Create child buffer with parent reference
  return new SpanBufferClass(
    capacity,
    logBinding,
    spanName,
    parentBuffer, // parent
    false, // not chained
    undefined, // no trace_id for child (walk up parent chain instead)
    callsiteMetadata, // callsiteMetadata - CALLER's op metadata (for row 0)
  );
}

// ============================================================================
// Constants for Arrow conversion
// ============================================================================

export const THREAD_ID_BYTES = 8;
