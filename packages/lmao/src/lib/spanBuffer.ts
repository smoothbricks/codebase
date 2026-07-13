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
  type ColumnBufferExtension,
  DEFAULT_BUFFER_CAPACITY,
  getColumnBufferClass,
  type SchemaWithMetadata,
} from '@smoothbricks/arrow-builder';
import { checkCapacityTuning } from './capacityTuning.js';
import type { OpMetadata } from './opContext/opTypes.js';
import type { EagerColumnDescriptor } from './physicalLayoutPlan.js';
import { LogSchema } from './schema/LogSchema.js';
import type { MessageLayoutFamily } from './runtimeHint.js';
import type { SpanBufferStats } from './spanBufferStats.js';
import { copyThreadIdTo, getThreadId } from './threadId.js';
import type { ITraceRoot } from './traceRoot.js';
import type { AnySpanBuffer, SpanBuffer } from './types.js';
import { getVocabularyGeneration, type VocabularyGeneration } from './vocabularyRegistry.js';

// Re-export ITraceRoot for external consumers (and to satisfy linter - it's used in generated code)
export type { ITraceRoot };

/**
 * Empty frozen scope object - shared singleton to avoid allocations.
 * Used as default _scopeValues for all buffers until setScope() is called.
 */
export const EMPTY_SCOPE: Readonly<Record<string, unknown>> = Object.freeze({});


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
  getWriteIndex(buffer: AnySpanBuffer): number {
    return buffer._writeIndex;
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
//#region smoo/lmao!n/spanbuffer-layout.class-signature
export interface SpanBufferConstructor<T extends LogSchema = LogSchema> {
  new (
    capacity: number,
    stats: SpanBufferStats,
    parent: AnySpanBuffer | undefined,
    isChained: boolean,
    callsiteMetadata: OpMetadata | undefined,
    opMetadata: OpMetadata | undefined,
    traceRoot: ITraceRoot,
    vocabularyGeneration: VocabularyGeneration,
  ): SpanBuffer<T>;

  // Static properties added after class generation
  readonly schema: T;
  readonly stats: SpanBufferStats;
  readonly messageLayoutFamily: MessageLayoutFamily;
  readonly eagerColumns: EagerColumnDescriptor;
}
//#endregion smoo/lmao!n/spanbuffer-layout.class-signature

type GeneratedSpanBufferClass<T extends LogSchema> = new (
  capacity: number,
  stats: SpanBufferStats,
  parent: AnySpanBuffer | undefined,
  isChained: boolean,
  callsiteMetadata: OpMetadata | undefined,
  opMetadata: OpMetadata | undefined,
  traceRoot: ITraceRoot,
  vocabularyGeneration: VocabularyGeneration,
) => SpanBuffer<T>;

function isGeneratedSpanBufferClass<T extends LogSchema>(
  ctor: new (...args: never[]) => unknown,
): ctor is GeneratedSpanBufferClass<T> {
  const prototype = Reflect.get(ctor, 'prototype');
  if (typeof prototype !== 'object' || prototype === null) {
    return false;
  }

  return (
    typeof Reflect.get(prototype, 'getOrCreateOverflow') === 'function' &&
    typeof Reflect.get(prototype, '_sealStats') === 'function' &&
    typeof Reflect.get(prototype, '_sealStatsChain') === 'function' &&
    typeof Reflect.get(prototype, 'isParentOf') === 'function' &&
    typeof Reflect.get(prototype, 'isChildOf') === 'function'
  );
}

function isSpanBufferConstructorForSchema<T extends LogSchema>(
  ctor: unknown,
  schema: T,
): ctor is SpanBufferConstructor<T> {
  return (
    typeof ctor === 'function' &&
    Reflect.get(ctor, 'schema') === schema &&
    typeof Reflect.get(ctor, 'stats') === 'object'
  );
}

function getSpanBufferConstructorForBuffer<T extends LogSchema>(buffer: SpanBuffer<T>): SpanBufferConstructor<T> {
  const ctor = buffer.constructor;
  if (!isSpanBufferConstructorForSchema(ctor, buffer._logSchema)) {
    throw new TypeError('Buffer constructor does not match buffer schema');
  }
  return ctor;
}

function createSpanBufferConstructor<T extends LogSchema>(
  schema: T,
  messageLayoutFamily: MessageLayoutFamily,
  generatedClass: GeneratedSpanBufferClass<T>,
  eagerColumns: EagerColumnDescriptor,
): SpanBufferConstructor<T> {
  return Object.assign(generatedClass, {
    schema,
    messageLayoutFamily,
    eagerColumns,
    stats: {
      capacity: DEFAULT_BUFFER_CAPACITY,
      totalWrites: 0,
      spansCreated: 0,
    } satisfies SpanBufferStats,
  });
}

/**
 * Cache for generated SpanBuffer classes per schema.
 * Key is the schema object reference (WeakMap for GC).
 */
const spanBufferClassCache = new WeakMap<LogSchema, Map<string, object>>();
const staticStorageSchemas = new WeakMap<LogSchema, LogSchema>();
const EMPTY_EAGER_COLUMNS: EagerColumnDescriptor = Object.freeze({
  names: Object.freeze([]),
  words: Object.freeze([]),
  key: '',
});

function getStorageSchema(schema: LogSchema, messageLayoutFamily: MessageLayoutFamily): LogSchema {
  if (messageLayoutFamily !== 'static-only') return schema;
  let storageSchema = staticStorageSchemas.get(schema);
  if (storageSchema === undefined) {
    const fields: Record<string, SchemaWithMetadata> = {};
    for (const [name, field] of schema._columns) {
      if (name !== 'message') fields[name] = field;
    }
    storageSchema = new LogSchema(fields);
    staticStorageSchemas.set(schema, storageSchema);
  }
  return storageSchema;
}

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
//#region smoo/lmao!n/spanbuffer-layout.constructor
export function getSpanBufferClass<T extends LogSchema>(
  schema: T,
  messageLayoutFamily: MessageLayoutFamily = 'mixed',
  eagerColumns: EagerColumnDescriptor = EMPTY_EAGER_COLUMNS,
): SpanBufferConstructor<T> {
  const cacheKey = `${messageLayoutFamily}:${eagerColumns.key}`;
  let familyClasses = spanBufferClassCache.get(schema);
  const cached = familyClasses?.get(cacheKey);
  if (isSpanBufferConstructorForSchema(cached, schema) && cached.messageLayoutFamily === messageLayoutFamily) {
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
  // 5. _nodeIndex/_parent - modified/accessed during span creation
  // 6. _traceRoot - accessed for timestamp anchors on EVERY write
  // 7. _identity/_logBinding - accessed during span operations
  // 8-N. Cold properties - only accessed during Arrow conversion
  // ==========================================================================

  // Define extension for arrow-builder's class generator
  const extension: ColumnBufferExtension = {
    preallocatedColumns: eagerColumns.names,
    constructorParams: 'stats, parent, isChained, callsiteMetadata, opMetadata, traceRoot, vocabularyGeneration',
    dependencies: {
      copyThreadIdTo,
      getThreadId,
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
    // Slots 6-7: WARM (tree structure) - _nodeIndex, _parent
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
       const threadId = isChained ? parent.thread_id : getThreadId();
` +
      // Calculate exact system storage. Dynamic-only has no capacity-sized packed-header lane.
      (messageLayoutFamily === 'dynamic-only'
        ? `const systemSize = (requestedCapacity * 9 + 7) & ~7;
       let systemBuffer;
       let identityView;
`
        : `const logHeaderOffset = (requestedCapacity * 9 + 3) & ~3;
       const rawSystemSize = logHeaderOffset + requestedCapacity * 4;
       const systemSize = (rawSystemSize + 7) & ~7;
       let systemBuffer;
       let identityView;
`) +
      // CHAINED BUFFER: overflow storage for same logical span - shares parent identity
      `if (isChained) {
          systemBuffer = new ArrayBuffer(systemSize);
          identityView = parent._identity;
        }` +
      // CHILD BUFFER: new span in existing trace - gets identity without traceId
      ` else if (parent) {
         const identitySize = 12;
         systemBuffer = new ArrayBuffer(systemSize + identitySize);
         identityView = new Uint8Array(systemBuffer, systemSize, identitySize);
         copyThreadIdTo(identityView, 0);
         identityView[8] = spanId;
         identityView[9] = spanId >>> 8;
         identityView[10] = spanId >>> 16;
         identityView[11] = spanId >>> 24;
       }` +
      // ROOT BUFFER: new trace - copies the trace-owned canonical bytes once into its public identity.
      ` else {
          const traceIdBytes = traceRoot._traceIdBytes;
          const identitySize = 13 + traceIdBytes.length;
          systemBuffer = new ArrayBuffer(systemSize + identitySize);
          identityView = new Uint8Array(systemBuffer, systemSize, identitySize);
          copyThreadIdTo(identityView, 0);
          identityView[8] = spanId;
          identityView[9] = spanId >>> 8;
          identityView[10] = spanId >>> 16;
          identityView[11] = spanId >>> 24;
          identityView[12] = traceIdBytes.length;
          identityView.set(traceIdBytes, 13);
        }
` +
      // Create exact family views. Headerless buffers never create a header view.
      `const timestampView = new BigInt64Array(systemBuffer, 0, requestedCapacity);
       const entryTypeView = new Uint8Array(systemBuffer, requestedCapacity * 8, requestedCapacity);
       timestampView.fill(0n);
       entryTypeView.fill(0);
` +
      (messageLayoutFamily === 'dynamic-only'
        ? ''
        : `       const logHeaderView = new Uint32Array(systemBuffer, logHeaderOffset, requestedCapacity);
       logHeaderView.fill(0);
`) +
      // Assign properties in optimal order for V8 in-object slots (see comment above)
      `this._writeIndex = 0;
       this._capacity = requestedCapacity;
       this._overflow = undefined;
       this.timestamp = timestampView;
       this.entry_type = entryTypeView;
` +
      (messageLayoutFamily === 'dynamic-only' ? '' : `       this._logHeaders = logHeaderView;
`) +
      `       this._vocabularyGeneration = vocabularyGeneration;
` +
      (messageLayoutFamily === 'static-only'
        ? `       this._spanName = undefined;
       this._terminalMessage = undefined;
`
        : messageLayoutFamily === 'dynamic-only'
          ? `       this._spanName = undefined;
`
          : '') +
      `       this._nodeIndex = 4294967295;
       this._topologyGeneration = 0;
       this._parent = isChained ? parent._parent : parent;
       this._traceRoot = traceRoot;
       this._scopeValues = parent ? parent._scopeValues : EMPTY_SCOPE;
       this._threadId = threadId;
        this._identity = identityView;
        this._system = systemBuffer;
        this._callsiteMetadata = callsiteMetadata;
        this._opMetadata = opMetadata;
        this._statsSealed = false;
        this._statsReservedRows = isChained ? 0 : 2;
    `,
    // Generated identity methods use canonical primitives/direct offsets without temporary views.
    methods: `get span_id() {
      const b = this._identity;
      return b ? (b[8] | (b[9] << 8) | (b[10] << 16) | (b[11] << 24)) >>> 0 : 0;
    }
    get thread_id() {
      return this._threadId;
    }
    get trace_id() {
      return this._traceRoot.trace_id;
    }
    get _spanStartTime() {
      return this.timestamp[0];
    }
    get _lastLoggedTime() {
      const chain = [];
      let current = this;
      while (current) {
        chain.push(current);
        current = current._overflow;
      }
      for (let i = chain.length - 1; i >= 0; i--) {
        const buffer = chain[i];
        for (let row = buffer._writeIndex - 1; row >= 0; row--) {
          const ts = buffer.timestamp[row];
          if (ts !== 0n) {
            return ts;
          }
        }
      }
      return null;
    }
    get _hasParent() { return this._parent !== undefined; }
    get parent_span_id() { return this._parent?.span_id ?? 0; }
    get parent_thread_id() { return this._parent?.thread_id ?? 0n; }
    isParentOf(other) { return this === other._parent; }
    isChildOf(other) { return this._parent === other; }
    copyThreadIdTo(dest, offset) {
      const source = this._identity;
      if (source) {
        dest[offset] = source[0];
        dest[offset + 1] = source[1];
        dest[offset + 2] = source[2];
        dest[offset + 3] = source[3];
        dest[offset + 4] = source[4];
        dest[offset + 5] = source[5];
        dest[offset + 6] = source[6];
        dest[offset + 7] = source[7];
      } else {
        dest.fill(0, offset, offset + 8);
      }
    }
    copyParentThreadIdTo(dest, offset) {
      if (this._parent) this._parent.copyThreadIdTo(dest, offset);
      else dest.fill(0, offset, offset + 8);
    }
    get _logSchema() { return this.constructor.schema; }
    get _messageLayoutFamily() { return this.constructor.messageLayoutFamily; }
    get _columns() { return this.constructor.schema._columns; }
    get _stats() { return this.constructor.stats; }
    _sealStats() {
      if (this._statsSealed) return;
      const completedRows = this._writeIndex - this._statsReservedRows;
      if (completedRows > 0) this.constructor.stats.totalWrites += completedRows;
      this._statsSealed = true;
    }
    _sealStatsChain() {
      let current = this;
      while (current) {
        current._sealStats();
        current = current._overflow;
      }
    }
    getOrCreateOverflow() {
      if (this._overflow) return this._overflow;
      this._sealStats();
      const tracer = this._traceRoot.tracer;
      tracer.onStatsWillResetFor(this);
      checkCapacityTuning(this.constructor.stats);
      return tracer.bufferStrategy.createOverflowBuffer(this);
    }
    ${messageLayoutFamily === 'static-only' ? `message(pos, val) {
      if (pos === 0) this._spanName = val;
      else if (pos === 1) this._terminalMessage = val;
      else throw new RangeError('Static-only buffers only accept raw system messages at rows 0 and 1');
      return this;
    }` : ''}
    `,
  };

  // Generate class with arrow-builder
  const generatedClass = getColumnBufferClass(getStorageSchema(schema, messageLayoutFamily), extension);
  if (!isGeneratedSpanBufferClass<T>(generatedClass)) {
    throw new TypeError('Generated column buffer is missing SpanBuffer methods');
  }

  // Add static properties to the generated class
  // These are shared across all instances from the same defineOpContext
  const SpanBufferClass = createSpanBufferConstructor(schema, messageLayoutFamily, generatedClass, eagerColumns);

  // Cache for future use
  familyClasses ??= new Map();
  familyClasses.set(cacheKey, SpanBufferClass);
  spanBufferClassCache.set(schema, familyClasses);

  return SpanBufferClass;
}
//#endregion smoo/lmao!n/spanbuffer-layout.constructor

// ============================================================================
// SpanBuffer factory functions
// ============================================================================

/**
 * Creates a root SpanBuffer for a new trace.
 *
 * Root buffers have identity: [threadId(8)][spanId(4)][traceIdLen(1)][traceId(N)]
 *
 * @param schema - Tag attribute schema defining column types (must be LogSchema)
 * @param traceRoot - Pre-built ITraceRoot with trace_id, anchors, and tracer
 * @param opMetadata - Op metadata for attribution (package_name, package_file, git_sha, line)
 * @param capacity - Buffer capacity (optional, uses class.stats.capacity if omitted)
 *
 * @returns SpanBuffer with typed setters for schema fields
 */
const MIN_CAPACITY = 8;

//#region smoo/lmao!n/spanbuffer-layout.create-root
export function createSpanBuffer<T extends LogSchema>(
  schema: T,
  traceRoot: ITraceRoot,
  opMetadata: OpMetadata,
  capacity?: number,
  plannedClass?: SpanBufferConstructor<T>,
): SpanBuffer<T> {
  const SpanBufferClass = plannedClass ?? getSpanBufferClass(schema);
  const stats = SpanBufferClass.stats;

  // Use provided capacity or default from class stats, enforce minimum
  const actualCapacity = Math.max(MIN_CAPACITY, capacity ?? stats.capacity);

  // Track non-chained buffer creation for capacity tuning
  stats.spansCreated++;

  // Create root buffer (no parent), then register its stable logical node.
  const buffer = new SpanBufferClass(
    actualCapacity,
    stats,
    undefined,
    false,
    opMetadata,
    opMetadata,
    traceRoot,
    opMetadata._physicalLayoutPlan?.vocabularyGeneration ?? getVocabularyGeneration(),
  );
  traceRoot._topology.registerRoot(buffer);
  return buffer;
}
//#endregion smoo/lmao!n/spanbuffer-layout.create-root

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
//#region smoo/lmao!n/spanbuffer-layout.create-overflow
export function createOverflowBuffer<T extends LogSchema>(buffer: SpanBuffer<T>): SpanBuffer<T> {
  const SpanBufferClass = getSpanBufferConstructorForBuffer(buffer);
  const stats = SpanBufferClass.stats;

  // Ensure capacity is multiple of 8 for byte-aligned null bitmaps
  const capacity = (stats.capacity + 7) & ~7;

  // Chained buffers inherit callsiteMetadata and opMetadata from the original buffer
  // Pass buffer as parent - used for identity sharing; _parent set to buffer._parent in constructor
  const nextBuffer = new SpanBufferClass(
    capacity,
    stats,
    buffer, // Used for identity sharing, _parent set to buffer._parent in constructor
    true, // isChained
    buffer._callsiteMetadata,
    buffer._opMetadata,
    buffer._traceRoot, // traceRoot from parent
    buffer._vocabularyGeneration,
  );

  // Link current buffer to next
  buffer._overflow = nextBuffer;
  buffer._traceRoot._topology.adoptOverflow(buffer, nextBuffer);

  return nextBuffer;
}
//#endregion smoo/lmao!n/spanbuffer-layout.create-overflow

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
//#region smoo/lmao!n/spanbuffer-layout.create-child
export function createChildSpanBuffer<T extends LogSchema>(
  parentBuffer: AnySpanBuffer,
  SpanBufferClass: SpanBufferConstructor<T>,
  callsiteMetadata: OpMetadata,
  opMetadata: OpMetadata,
  capacity?: number,
): SpanBuffer<T> {
  const stats = SpanBufferClass.stats;
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
    opMetadata._physicalLayoutPlan?.vocabularyGeneration ?? getVocabularyGeneration(),
  );

  parentBuffer._traceRoot._topology.registerChild(parentBuffer, childBuffer);
  return childBuffer;
}
//#endregion smoo/lmao!n/spanbuffer-layout.create-child

// ============================================================================
// Constants for Arrow conversion
// ============================================================================

export const THREAD_ID_BYTES = 8;
