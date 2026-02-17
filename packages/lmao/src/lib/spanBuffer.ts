/**
 * SpanBuffer - Unified memory layout for trace logging
 *
 * This module implements the unified SpanBuffer design from
 * specs/lmao/01b_columnar_buffer_architecture.md "Unified SpanBuffer Memory Layout"
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
import { checkCapacityTuning } from './capacityTuning.js';
import type { OpMetadata } from './opContext/opTypes.js';
import type { LogSchema } from './schema/LogSchema.js';
import { textEncoder } from './spanBufferHelpers.js';
import type { SpanBufferStats } from './spanBufferStats.js';
import { writeThreadIdToUint64Array } from './threadId.js';
import type { ITraceRoot } from './traceRoot.js';
import type { AnySpanBuffer, SpanBuffer } from './types.js';

// Re-export ITraceRoot for external consumers (and to satisfy linter - it's used in generated code)
export type { ITraceRoot };

/**
 * Empty frozen scope object - shared singleton to avoid allocations.
 * Used as default _scopeValues for all buffers until setScope() is called.
 */
export const EMPTY_SCOPE: Readonly<Record<string, unknown>> = Object.freeze({});

const traceIdDecoder = new TextDecoder();

// ============================================================================
// Thread-local state (generated once per process/worker)
// These are used by the generated class code via the constructorPreamble
// ============================================================================

/**
 * Reset thread-local state (for testing only).
 * Note: The actual state is in the generated class constructorPreamble.
 * This function resets it by clearing the class cache.
 * @internal
 */
export function _resetSpanBufferState(): void {
  // Clear the class cache to force regeneration with fresh state
  // The generated constructorPreamble creates its own PROCESS_THREAD_ID and nextSpanId
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
 * SpanBuffer constructor type - accepts all parameters for buffer creation.
 * Exported for use in Op class and span context creation.
 *
 * The generated class has static properties:
 * - schema: LogSchema - The schema used to generate this class
 * - stats: SpanBufferStats - Shared mutable stats for all instances
 *
 * **Constructor cases:**
 * - ROOT: traceRoot provided (freshly created), parent undefined
 * - CHILD: traceRoot provided (from parent._traceRoot), parent provided, isChained=false
 * - CHAINED: traceRoot provided (from parent._traceRoot), parent provided, isChained=true
 *
 * traceRoot is ALWAYS provided - factories extract it from parent when needed.
 */
export interface SpanBufferConstructor {
  new (
    capacity: number,
    stats: SpanBufferStats,
    parent: AnySpanBuffer | undefined,
    isChained: boolean,
    callsiteMetadata: OpMetadata | undefined,
    opMetadata: OpMetadata | undefined,
    traceRoot: ITraceRoot,
  ): AnySpanBuffer;

  // Static properties added after class generation
  readonly schema: LogSchema;
  readonly stats: SpanBufferStats;
}

/**
 * Cache for generated SpanBuffer classes per schema.
 * Key is the schema object reference (WeakMap for GC).
 */
const spanBufferClassCache = new WeakMap<LogSchema, SpanBufferConstructor>();

/**
 * Get or create a SpanBuffer class for the given schema.
 *
 * The generated class has static `schema` and `stats` properties:
 * - `schema`: The LogSchema used to generate this class
 * - `stats`: Shared SpanBufferStats for all instances from same defineOpContext
 *
 * Classes are cached per schema (WeakMap keyed by schema object).
 *
 * @param schema - LogSchema defining the buffer structure
 * @returns SpanBufferConstructor with static schema and stats properties
 */
export function getSpanBufferClass(schema: LogSchema): SpanBufferConstructor {
  const cached = spanBufferClassCache.get(schema);
  if (cached) {
    return cached;
  }

  // ==========================================================================
  // GENERATED CODE EXTENSION
  // ==========================================================================
  // The constructorPreamble and methods strings below are passed to new Function() by
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
    constructorParams: 'stats, parent, isChained, callsiteMetadata, opMetadata, traceRoot',
    dependencies: {
      writeThreadIdToUint64Array,
      textDecoder: traceIdDecoder,
      textEncoder,
      EMPTY_SCOPE,
      checkCapacityTuning,
    },
    // ==========================================================================
    // GENERATED CONSTRUCTOR PREAMBLE
    // ==========================================================================
    // The constructorPreamble below is passed to new Function() by arrow-builder's
    // columnBufferGenerator. It MUST be pure JavaScript:
    // - NO TypeScript type annotations (: Type)
    // - NO non-null assertions (!)
    // - NO comments (waste of runtime parsing)
    //
    // BUFFER TYPES (determined by constructor arguments):
    // - ROOT: traceRoot provided, parent undefined
    //   → Creates identity with traceId: [threadId(8)][spanId(4)][traceIdLen(1)][traceId(N)]
    // - CHILD: traceRoot undefined (uses parent._traceRoot), parent provided, isChained=false
    //   → Creates identity without traceId: [threadId(8)][spanId(4)]
    // - CHAINED: traceRoot undefined (uses parent._traceRoot), parent provided, isChained=true
    //   → Shares parent._identity (no new identity allocation)
    //
    // MEMORY LAYOUT:
    // - systemBuffer: timestamps (8 bytes * cap) + entry_type (1 byte * cap) + identity
    // - System size aligned to 8 bytes so identity's BigUint64Array offset is aligned
    // - Identity layout: [threadId(8)][spanId(4)][traceIdLen(1)?][traceId(N)?]
    //
    // V8 IN-OBJECT PROPERTY OPTIMIZATION:
    // Property assignment order determines which properties get fast in-object slots.
    // First ~10-12 properties are stored directly on the object (fastest access).
    // Slots 1-3: HOTTEST (every log entry) - _writeIndex, _capacity, _overflow
    // Slots 4-5: HOT (TypedArray refs) - timestamp, entry_type
    // Slots 6-7: WARM (tree structure) - _children, _parent
    // Slots 8-10: WARM (context) - _traceRoot, _scopeValues, _identity
    // Slots 11: WARM (system) - _system
    // Slots 12+: COLD (Arrow conversion only) - _callsiteMetadata, _opMetadata
    // ==========================================================================
    constructorPreamble:
      // Thread-local span counter (per-process/worker, see threadId.ts docs)
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
      `if (isChained) {
          systemBuffer = new ArrayBuffer(systemSize);
          identityView = parent._identity;
        }` +
      // CHILD BUFFER: new span in existing trace - gets identity without traceId
      // (traceId accessed via parent chain walk in trace_id getter)
      ` else if (parent) {
         const identitySize = 12;
         systemBuffer = new ArrayBuffer(systemSize + identitySize);
         identityView = new Uint8Array(systemBuffer, systemSize, identitySize);
         const threadIdArray = new BigUint64Array(systemBuffer, systemSize, 1);
         writeThreadIdToUint64Array(threadIdArray, 0);
         new DataView(systemBuffer, systemSize).setUint32(8, spanId, true);
       }` +
      // ROOT BUFFER: new trace - gets identity with traceId from TraceRoot
      ` else {
          const traceIdUtf8 = textEncoder.encode(traceRoot.trace_id);
          const identitySize = 13 + traceIdUtf8.length;
          systemBuffer = new ArrayBuffer(systemSize + identitySize);
          identityView = new Uint8Array(systemBuffer, systemSize, identitySize);
          const view = new DataView(systemBuffer, systemSize);
         const threadIdArray = new BigUint64Array(systemBuffer, systemSize, 1);
         writeThreadIdToUint64Array(threadIdArray, 0);
          view.setUint32(8, spanId, true);
          identityView[12] = traceIdUtf8.length;
          identityView.set(traceIdUtf8, 13);
        }
` +
      // Create TypedArray views for timestamps and entry_type
      `const timestampView = new BigInt64Array(systemBuffer, 0, requestedCapacity);
       const entryTypeView = new Uint8Array(systemBuffer, requestedCapacity * 8, requestedCapacity);
       timestampView.fill(0n);
       entryTypeView.fill(0);
` +
      // Assign properties in optimal order for V8 in-object slots (see comment above)
      `this._writeIndex = 0;
       this._capacity = requestedCapacity;
       this._overflow = undefined;
       this.timestamp = timestampView;
       this.entry_type = entryTypeView;
       this._children = [];
       this._parent = isChained ? parent._parent : parent;
       this._traceRoot = traceRoot;
       this._scopeValues = parent ? parent._scopeValues : EMPTY_SCOPE;
        this._identity = identityView;
        this._system = systemBuffer;
        this._callsiteMetadata = callsiteMetadata;
        this._opMetadata = opMetadata;
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
      return textDecoder.decode(traceIdBytes);
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
    get _logSchema() { return this.constructor.schema; }
    get _columns() { return this.constructor.schema._columns; }
    get _stats() { return this.constructor.stats; }
    getOrCreateOverflow() {
      if (this._overflow) return this._overflow;
      const tracer = this._traceRoot.tracer;
      tracer.onStatsWillResetFor(this);
      checkCapacityTuning(this.constructor.stats);
      return tracer.bufferStrategy.createOverflowBuffer(this);
    }
    `,
  };

  // Generate class with arrow-builder
  const SpanBufferClass = getColumnBufferClass(schema, extension) as unknown as SpanBufferConstructor;

  // Add static properties to the generated class
  // These are shared across all instances from the same defineOpContext
  (SpanBufferClass as any).schema = schema;
  (SpanBufferClass as any).stats = {
    capacity: DEFAULT_BUFFER_CAPACITY,
    totalWrites: 0,
    spansCreated: 0,
  } satisfies SpanBufferStats;

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
 * @param spanName - Name of the span
 * @param traceRoot - Pre-built ITraceRoot with trace_id, anchors, and tracer
 * @param opMetadata - Op metadata for attribution (package_name, package_file, git_sha, line)
 * @param capacity - Buffer capacity (optional, uses class.stats.capacity if omitted)
 *
 * @returns SpanBuffer with typed setters for schema fields
 */
const MIN_CAPACITY = 8;

export function createSpanBuffer<T extends LogSchema>(
  schema: T,
  spanName: string,
  traceRoot: ITraceRoot,
  opMetadata: OpMetadata,
  capacity?: number,
): SpanBuffer<T> {
  const SpanBufferClass = getSpanBufferClass(schema);
  const stats = SpanBufferClass.stats;

  // Use provided capacity or default from class stats, enforce minimum
  const actualCapacity = Math.max(MIN_CAPACITY, capacity ?? stats.capacity);

  // Track non-chained buffer creation for capacity tuning
  stats.spansCreated++;

  // Create root buffer (no parent)
  // Root spans use same metadata for both callsite and op (no distinction at root)
  return new SpanBufferClass(
    actualCapacity,
    stats,
    undefined, // no parent
    false, // not chained
    opMetadata, // callsiteMetadata for row 0
    opMetadata, // opMetadata for rows 1+ (same as callsite for root)
    traceRoot, // pre-built TraceRoot with trace_id, anchors, tracer
  ) as SpanBuffer<T>;
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
  const SpanBufferClass = buffer.constructor as SpanBufferConstructor;
  const stats = SpanBufferClass.stats;

  // Ensure capacity is multiple of 8 for byte-aligned null bitmaps
  const capacity = (stats.capacity + 7) & ~7;

  // Chained buffers inherit callsiteMetadata, opMetadata, and spanName from the original buffer
  // Pass buffer as parent - used for identity sharing; _parent set to buffer._parent in constructor
  const nextBuffer = new SpanBufferClass(
    capacity,
    stats,
    buffer, // Used for identity sharing, _parent set to buffer._parent in constructor
    true, // isChained
    buffer._callsiteMetadata,
    buffer._opMetadata,
    buffer._traceRoot, // traceRoot from parent
  ) as SpanBuffer<T>;

  // Link current buffer to next
  buffer._overflow = nextBuffer;

  return nextBuffer;
}

/**
 * Creates a child span buffer with parent linkage.
 *
 * Child buffers have their own identity but reference their parent
 * for trace hierarchy and scope inheritance.
 *
 * Note: Caller must call writeSpanStart() after this to set span name in message_values[0].
 *
 * @param parentBuffer - The parent span buffer
 * @param SpanBufferClass - The SpanBuffer class to instantiate (has static schema + stats)
 * @param callsiteMetadata - Caller's op metadata (for row 0 attribution)
 * @param opMetadata - Op metadata for this span (for rows 1+ attribution)
 * @param capacity - Optional capacity override (uses class.stats.capacity if omitted)
 *
 * @returns New SpanBuffer linked to parent
 */
export function createChildSpanBuffer<T extends LogSchema>(
  parentBuffer: AnySpanBuffer,
  SpanBufferClass: SpanBufferConstructor,
  callsiteMetadata: OpMetadata,
  opMetadata: OpMetadata,
  capacity?: number,
): SpanBuffer<T> {
  const stats = (SpanBufferClass as any).stats as SpanBufferStats;
  const actualCapacity = capacity ?? stats.capacity;

  // Track non-chained buffer creation for capacity tuning
  stats.spansCreated++;

  // Create child buffer with parent reference
  const childBuffer = new SpanBufferClass(
    actualCapacity,
    stats,
    parentBuffer, // parent
    false, // not chained
    callsiteMetadata, // callsiteMetadata - CALLER's op metadata (for row 0)
    opMetadata, // opMetadata - EXECUTING op metadata (for rows 1+)
    parentBuffer._traceRoot, // traceRoot from parent
  ) as SpanBuffer<T>;

  return childBuffer;
}

// ============================================================================
// Constants for Arrow conversion
// ============================================================================

export const THREAD_ID_BYTES = 8;
