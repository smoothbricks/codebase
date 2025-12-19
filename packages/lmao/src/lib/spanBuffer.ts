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
import { spanBufferHelpers } from './spanBufferHelpers.js';
import { generateTraceId, type TraceId } from './traceId.js';
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
  traceId: string | undefined,
  callsiteModule: ModuleContext | undefined,
) => SpanBuffer;

/**
 * Cache for generated SpanBuffer classes per schema.
 * Key is the schema object reference (WeakMap for GC).
 */
const spanBufferClassCache = new WeakMap<LogSchema, SpanBufferConstructor>();

/**
 * Get or create a SpanBuffer class for the given schema.
 *
 * Uses arrow-builder's extension mechanism to inject:
 * - System columns (_system ArrayBuffer with timestamps, operations, identity)
 * - Span-specific properties (parent, children, task)
 * - Identity getters (spanId, traceId, hasParent, parentSpanId)
 */
function getSpanBufferClass<T extends LogSchema>(schema: T): SpanBufferConstructor {
  const cached = spanBufferClassCache.get(schema);
  if (cached) {
    return cached;
  }

  // Define extension for arrow-builder's class generator
  const extension: ColumnBufferExtension = {
    constructorParams: 'module, spanName, utf8SpanName, parent, isChained, traceId, callsiteModule',
    preamble: `
        // Thread-local span counter (per-process/worker, see threadId.ts docs)
        // This MUST stay in preamble - it's per-class state incremented during construction
        let nextSpanId = 1;
      `,
    constructorCode: `
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
          // IMPORTANT: 'parent' parameter here is the PREVIOUS BUFFER IN THE CHAIN
          // (the one that overflowed), NOT the tree parent!
          //
          // We extract THREE things from the chain predecessor:
          //   1. parent.parent    → tree parent (for traceId resolution)
          //   2. parent._identity → shared identity (same spanId)
          //   3. parent.scopeValues → shared scope (same logical span)
          //
          // Why parent.parent for tree structure?
          //   The traceId getter walks: this.parent.traceId → ... → root._identity
          //   Chained buffers share _identity, so they need the SAME tree parent
          //   to resolve traceId consistently.
          // ============================================================
          this.parent = parent.parent;
          this._system = new ArrayBuffer(systemSize);
          this._identity = parent._identity; // Shared with chain predecessor!
        } else if (parent) {
          // CHILD: parent is our tree parent
          this.parent = parent;
          // CHILD: own 12-byte identity (threadId + spanId)
          const identitySize = 12;
          this._system = new ArrayBuffer(systemSize + identitySize);
          this._identity = new Uint8Array(this._system, systemSize, identitySize);
          
          // Set threadId (bytes 0-7) via injected helper (singleton per process/worker)
          sbHelpers.copyThreadIdTo(this._identity, 0);
          
          // Set spanId (bytes 8-11, little-endian) using injected helper
          sbHelpers.writeSpanId(this._identity, 8, nextSpanId++);
          
          // Parent reference kept for identity resolution (traceId walks up parent chain)
          // Note: Registration with parent.children is EXPLICIT at call sites (not here)
          // See specs/01k_tree_walker_and_arrow_conversion.md "Explicit Child Registration"
        } else {
          // ROOT: identity with traceId
          const traceBytes = traceId ? sbHelpers.textEncoder.encode(traceId) : new Uint8Array(0);
          const identitySize = 13 + traceBytes.length;
          this._system = new ArrayBuffer(systemSize + identitySize);
          this._identity = new Uint8Array(this._system, systemSize, identitySize);
          
          // Set threadId (bytes 0-7) via injected helper (singleton per process/worker)
          sbHelpers.copyThreadIdTo(this._identity, 0);
          
          // Set spanId (bytes 8-11, little-endian) using injected helper
          sbHelpers.writeSpanId(this._identity, 8, nextSpanId++);
          
          // Set traceId length and bytes
          this._identity[12] = traceBytes.length;
          this._identity.set(traceBytes, 13);
        }
        
        // Scope values initialization per specs/01i_span_scope_attributes.md:
        // - ROOT: allocate empty frozen scope (single allocation per trace)
        // - CHILD: inherit from tree parent by reference (zero cost)
        // - CHAINED: inherit from chain predecessor (same logical span, NOT tree parent!)
        if (isChained && parent) {
          // CHAINED: 'parent' is the chain predecessor, NOT tree parent!
          // We want scope from the buffer we're chaining from (same logical span).
          this.scopeValues = parent.scopeValues;
        } else if (parent) {
          // CHILD: 'parent' IS the tree parent here
          this.scopeValues = parent.scopeValues;
        } else {
          this.scopeValues = Object.freeze({});
        }
        
        // System columns at FIXED offsets (same for ALL buffer types)
        // These override the ColumnBuffer's _timestamps/_operations with our unified layout
        this._timestamps = new BigInt64Array(this._system, 0, requestedCapacity);
        this._operations = new Uint8Array(this._system, requestedCapacity * 8, requestedCapacity);
        
        // Direct property aliases for system columns (lmao code uses these)
        // These are direct assignments, not getters - V8 hidden class friendly
        this.timestamps = this._timestamps;
        this.operations = this._operations;
        
        // Write index for tracking current position
        // NOTE: Per architecture, _writeIndex should be tracked by ColumnWriter (SpanLogger),
        // not by ColumnBuffer. However, SpanBuffer maintains this for backwards compatibility
        // with existing tests and code that directly access buffer.writeIndex.
        // TODO: Migrate to SpanLogger-only write tracking when ready.
        this._writeIndex = 0;
        
        // Track buffer creation
        module.sb_totalCreated++;
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
  capacity = DEFAULT_BUFFER_CAPACITY,
): SpanBuffer<T> {
  // Ensure capacity is multiple of 8 for byte-aligned null bitmaps
  const alignedCapacity = (capacity + 7) & ~7;

  // Use provided TraceId or generate a new one
  const resolvedTraceId: string = traceId ?? generateTraceId();

  // Pre-encode span name for Arrow conversion
  const utf8SpanName = intern(spanName);

  const SpanBufferClass = getSpanBufferClass(schema);
  // Root spans have no callsiteModule (they're the entry point)
  return new SpanBufferClass(
    alignedCapacity,
    module,
    spanName,
    utf8SpanName,
    undefined,
    false,
    resolvedTraceId,
    undefined,
  ) as SpanBuffer<T>;
}

/**
 * Creates a child SpanBuffer for nested span operations.
 *
 * Child buffers have identity: [threadId(8)][spanId(4)] (12 bytes)
 * Parent reference is via `parent` property pointer.
 *
 * Per specs/01c_context_flow_and_op_wrappers.md:
 * - `callsiteModule` is the caller's module (where span() was invoked)
 * - `module` is the op's module (where code executes)
 * - Row 0 uses callsiteModule for gitSha/packageName/packagePath
 * - Rows 1+ use module for gitSha/packageName/packagePath
 *
 * **IMPORTANT**: The child buffer uses the CHILD module's schema (module.logSchema),
 * not the parent's schema. This allows cross-module calls where each module has
 * its own schema with different columns (e.g., httpModule calling dbModule).
 *
 * @param parentBuffer - Parent span's buffer (provides parent reference for tree structure)
 * @param module - Module context for the child span (provides schema and metadata)
 * @param spanName - Name of the child span
 *
 * @returns Child SpanBuffer linked to parent, with the child module's schema
 */
export function createChildSpanBuffer(parentBuffer: SpanBuffer, module: ModuleContext, spanName: string): SpanBuffer {
  // IMPORTANT: Use CHILD module's schema, NOT parent's!
  // This allows cross-module calls where modules have different schemas.
  const schema = module.logSchema;
  const capacity = parentBuffer.capacity;

  // The caller's module (parent's module) becomes callsiteModule
  // This records WHERE span() was invoked from
  const callsiteModule = parentBuffer.module;

  // Pre-encode span name for Arrow conversion
  const utf8SpanName = intern(spanName);

  const SpanBufferClass = getSpanBufferClass(schema);
  // Cast parentBuffer to base SpanBuffer for constructor, result back to SpanBuffer<T>
  return new SpanBufferClass(
    capacity,
    module,
    spanName,
    utf8SpanName,
    parentBuffer as SpanBuffer,
    false,
    undefined,
    callsiteModule,
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

// ============================================================================
// Constants for Arrow conversion
// ============================================================================

export const THREAD_ID_BYTES = 8;
