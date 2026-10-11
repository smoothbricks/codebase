# SpanBuffer Memory Layout <a id="smoo/lmao!n/spanbuffer-layout"></a>

> **Part of [Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**
>
> This document details the unified memory layout for SpanBuffer, combining identity and system columns into a single
> ArrayBuffer for maximum cache efficiency. For span identification concepts, see
> [Span Identity](./01b4_span_identity.md).

## Physical Layout and Ownership Contract <a id="smoo/lmao!n/spanbuffer-layout-contract"></a>

`PhysicalLayoutPlan` is the versioned startup artifact that fixes column offsets, widths, alignment, eager/lazy policy,
and contiguous-versus-chunked Arrow exposure for one generated buffer shape. A compatible `VocabularyBinding` may add
compiler-emitted vocabulary at startup; it MUST NOT mutate an instantiated plan or live SpanBuffer hidden class.

- **Buffer-owned:** writable `_system`, identity, lazy-column blocks, string-reference arrays, and overflow links from
  span setup until freeze.
- **Lease-owned borrowing:** conversion may expose frozen contiguous JS-heap TypedArray regions through an `ArrowLease`.
  The lease holds the trace topology, the source buffers, and their vocabulary generations, so `releaseBuffer()` defers
  recycling the tree until the last lease is released. WASM-backed columns are always copied into Arrow-owned storage,
  because linear-memory growth cannot keep borrowed views valid; the WASM memory epoch is pinned only during the
  synchronous build.
- **Arrow-owned:** dictionary/UTF-8/offset buffers, synthesized null bitmaps, serialized output, and any contiguous
  concatenation required by a consumer.
- **Chunked boundary:** overflow buffers remain separate physical chunks. Prefer Arrow-native chunks; concatenate only
  where contiguity is required, and report the allocation and copied bytes.

Effective memory includes SpanBuffer objects/trees, TypedArray views, JS/external backing stores, committed WASM pages
and fragmentation, Arrow output, active-lease pinned bytes, temporary conversion buffers, and cache/allocator overhead.
Report logical payload and peak/retained effective bytes per request.

Startup creates the plan/generated class; span setup allocates the object and base storage; warmed fixed-capacity writes
allocate zero new heap memory with fixed monomorphic stores and one predictable not-overflow branch; first lazy column
and overflow are slow paths; per-request Arrow flush is separately measured.

## Unified SpanBuffer Memory Layout <a id="smoo/lmao!n/spanbuffer-layout-unified"></a>

**Purpose**: Combine identity and system columns into one base backing allocation for cache efficiency and fixed system
column offsets. This reduces allocations; it does not make span setup or the complete request allocation-free.

### Design Principles

1. **Single `_system` ArrayBuffer** per buffer containing timestamp, entry_type, and identity (for non-chained)
2. **System columns FIRST** - timestamp and entry_type at fixed offsets 0 and `capacity * 8`
3. **Identity AFTER system columns** - variable size depending on buffer type (root vs child vs chained)
4. **Parent pointer for ancestry** - no copied parent identity bytes, just walk the `parent` reference
5. **Chained buffers share identity** - overflow buffers point to the same `_identity` view as their root

### Buffer Type Layouts

```
┌──────────────────────────────────────────────────────────────────────────────-───┐
│ ROOT SPAN BUFFER                                                                 │
│                                                                                  │
│ _system: ArrayBuffer                                                             │
│ ┌────────────────────────┬──────────────────┬──────────────────────────────────┐ │
│ │ timestamp              │ entry_type       │ identity                         │ │
│ │ BigInt64Array          │ Uint8Array       │ [thread_id][span_id][len][trace]   │
│ │ 8 * capacity bytes     │ 1 * capacity     │ 8 + 4 + 1 + trace_id.length      │ │
│ └────────────────────────┴──────────────────┴──────────────────────────────────┘ │
│  offset: 0                capacity * 8       capacity * 9                        │
│                                                                                  │
│ Views:                                                                           │
│   timestamp ──► BigInt64Array(this._system, 0, capacity)                         │
│   entry_type ──► Uint8Array(this._system, capacity * 8, capacity)                │
│   _identity  ──► Uint8Array(this._system, capacity * 9, 13 + trace_id.length)    │
│                                                                                  │
│ Identity layout (13 + trace_id.length bytes):                                    │
│   [0-7]   thread_id    (8 bytes, crypto-secure random, same for all spans)       │
│   [8-11]  span_id      (4 bytes, Uint32, incrementing counter)                   │
│   [12]    trace_idLen  (1 byte, length of trace_id string)                       │
│   [13+]   trace_id     (1-128 bytes, ASCII string)                               │
│                                                                                  │
│ Properties:                                                                      │
│   _parent: undefined (root has no parent)                                        │
│   _nodeIndex: number (this span's node in _traceRoot._topology)                  │
│   _callsiteMetadata: OpMetadata (caller's module for row 0 metadata)             │
│   _opMetadata: OpMetadata (Op's module for rows 1+ metadata)                     │
│   spanName: string (per-span data)                                               │
│   NOTE: lineNumber is in lineNumber_values TypedArray, NOT a property            │
└──────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│ CHILD SPAN BUFFER                                                               │
│                                                                                 │
│ _system: ArrayBuffer                                                            │
│ ┌────────────────────────┬──────────────────┬─────────────────┐                 │
│ │ timestamp              │ entry_type       │ identity        │                 │
│ │ BigInt64Array          │ Uint8Array       │ [thread_id][span]│                 │
│ │ 8 * capacity bytes     │ 1 * capacity     │ 8 + 4 = 12 bytes│                 │
│ └────────────────────────┴──────────────────┴─────────────────┘                 │
│  offset: 0                capacity * 8       capacity * 9                       │
│                                                                                 │
│ Views:                                                                          │
│   timestamp ──► BigInt64Array(this._system, 0, capacity)                        │
│   entry_type ──► Uint8Array(this._system, capacity * 8, capacity)               │
│   _identity  ──► Uint8Array(this._system, capacity * 9, 12)                     │
│                                                                                 │
│ Identity layout (12 bytes):                                                     │
│   [0-7]   thread_id    (8 bytes, same as process thread_id)                     │
│   [8-11]  span_id      (4 bytes, Uint32, incrementing counter)                  │
│                                                                                 │
│ Properties:                                                                     │
│   _parent ─────────────► (parent SpanBuffer - for trace_id + parentSpanId)      │
│   _nodeIndex: number (this span's node in _traceRoot._topology)                 │
│   _callsiteMetadata: OpMetadata (caller's module for row 0 metadata)            │
│   _opMetadata: OpMetadata (Op's module for rows 1+ metadata)                    │
│   spanName: string (per-span data)                                              │
│   NOTE: lineNumber is in lineNumber_values TypedArray, NOT a property           │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│ CHAINED SPAN BUFFER (overflow)                                                  │
│                                                                                 │
│ _system: ArrayBuffer (NO identity - smallest allocation!)                       │
│ ┌────────────────────────┬──────────────────┐                                   │
│ │ timestamp              │ entry_type       │                                   │
│ │ BigInt64Array          │ Uint8Array       │                                   │
│ │ 8 * capacity bytes     │ 1 * capacity     │                                   │
│ └────────────────────────┴──────────────────┘                                   │
│  offset: 0                capacity * 8                                          │
│                                                                                 │
│ Views:                                                                          │
│   timestamp ──► BigInt64Array(this._system, 0, capacity)                        │
│   entry_type ──► Uint8Array(this._system, capacity * 8, capacity)               │
│   _identity  ──────────────► (first buffer's _identity - shared reference!)     │
│                                                                                 │
│ Properties:                                                                     │
│   _parent ─────────────► (same as first buffer's parent)                        │
│   _nodeIndex: number (same logical node as the first buffer)                    │
│   _callsiteMetadata: OpMetadata (shared from first buffer)                      │
│   _opMetadata: OpMetadata (Op's module, shared reference)                       │
│   spanName: string (per-span data)                                              │
│   NOTE: lineNumber is in lineNumber_values TypedArray, NOT a property           │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Memory Savings

| Buffer Type | Separate Allocations (alternative)                        | Unified \_system (chosen)              |
| ----------- | --------------------------------------------------------- | -------------------------------------- |
| Root        | 25 bytes identity + timestamp AB + entry_type AB          | Single AB: capacity\*9 + 13 + traceLen |
| Child       | 25 bytes identity + timestamp AB + entry_type AB          | Single AB: capacity\*9 + 12            |
| Chained     | 25 bytes identity (copied) + timestamp AB + entry_type AB | Single AB: capacity\*9 (no identity!)  |

**Key savings from unified approach:**

- Root/Child: 2 fewer ArrayBuffer allocations per span
- Chained: 12-141 identity bytes avoided; the existing identity view is shared by reference
- All: Better cache locality (identity adjacent to hot system columns)

### Why System Columns BEFORE Identity

Placing timestamp and entry_type at the START of the buffer means:

1. **Fixed offsets for ALL buffer types** - timestamp always at 0, entry_type always at `capacity * 8`
2. **No per-write layout conditional** - root, child, and chained buffers use the same fixed system-column offsets
3. **Chained is just truncated** - same layout prefix, just shorter (no identity suffix)

```typescript
// SAME code for root, child, AND chained:
this.timestamp = new BigInt64Array(this._system, 0, capacity);
this.entry_type = new Uint8Array(this._system, capacity * 8, capacity);

// Only non-chained buffers have identity after system columns:
if (!isChained) {
  const identityOffset = capacity * 9;
  this._identity = new Uint8Array(this._system, identityOffset, identitySize);
}
```

### Parent-Based Ancestry (No Copied Parent Identity)

Instead of copying 12 bytes of parent identity into each child, we use the existing `parent` reference:

```typescript
get hasParent(): boolean {
  return this._parent !== undefined;
}

get parentSpanId(): number {
  return this._parent?.span_id ?? 0;
}

get trace_id(): string {
  if (this._parent) {
    return this._parent.trace_id;  // Walk up to root
  }
  // Root: decode from _identity
  const len = this._identity[12];
  return String.fromCharCode(...this._identity.subarray(13, 13 + len));
}

// MASSIVE WIN: isParentOf becomes pointer comparison!
isParentOf(other: SpanBuffer): boolean {
  return this === other._parent;
}

isChildOf(other: SpanBuffer): boolean {
  return this._parent === other;
}
```

**Benefits:**

- `isParentOf` is now O(1) pointer comparison instead of 12-byte loop
- No 12 bytes copied per child span
- `trace_id` walks to root (spans rarely deep, typically 3-5 levels)

### span_id at Fixed Offset

The `span_id` getter works identically for root and child because thread_id and span_id are at the same offsets:

```typescript
// Identity layout for both root and child:
//   [0-7]   thread_id (8 bytes)
//   [8-11]  span_id (4 bytes)
//   ... (root has trace_idLen + trace_id after, child stops here)

get span_id(): number {
  const b = this._identity;
  return b[8] | (b[9] << 8) | (b[10] << 16) | (b[11] << 24);
}
```

### Thread ID Generation <a id="smoo/lmao!n/spanbuffer-layout.thread-id"></a>

Thread ID bytes are cached once per worker/process and copied into newly allocated root/child identity storage. The
cache avoids regeneration and intermediate encoding; `Uint8Array.set()` still copies 8 bytes during span setup.

```typescript
// Module-level singleton generated once per process/worker
let thread_idBytes: Uint8Array | null = null;

// Span-setup API: copy cached bytes into owned identity storage
function copyThreadIdTo(dest: Uint8Array, offset: number): void {
  ensureInitialized();
  dest.set(thread_idBytes!, offset);
}
```

### Constructor Implementation <a id="smoo/lmao!n/spanbuffer-layout.constructor"></a>

The constructor is generated. `getSpanBufferClass` (`packages/lmao/src/lib/spanBuffer.ts`) builds the
`constructorPreamble` string passed to arrow-builder's `getColumnBufferClass`, and `SpanBufferConstructor` (same file)
is the parameter list:
`(capacity, stats, parent, isChained, callsiteMetadata, opMetadata, traceRoot, vocabularyGeneration)`. `spanName` is
written to `message_values[0]` by `writeSpanStart`, and `trace_id` comes from `traceRoot._traceIdBytes`. The class below
shows the unified `_system` layout and the identity layout (`[threadId 8][spanId 4][traceIdLen 1][traceId N]`).

```typescript
class SpanBuffer {
  readonly _system: ArrayBuffer;
  readonly _identity: Uint8Array;
  readonly timestamp: BigInt64Array;
  readonly entry_type: Uint8Array;

  _parent?: SpanBuffer;
  _nodeIndex: number; // Node in _traceRoot._topology; child links live in the topology arena
  _topologyGeneration: number;
  _overflow?: SpanBuffer;

  // Per-span invocation data
  // NOTE: lineNumber is in lineNumber_values TypedArray, NOT a property on SpanBuffer
  _callsiteMetadata?: OpMetadata; // Caller's metadata (for row 0's git_sha/package_name/package_file)
  _opMetadata: OpMetadata; // Op's metadata (for rows 1+ git_sha/package_name/package_file)
  _traceRoot: ITraceRoot;
  _vocabularyGeneration: VocabularyGeneration;

  _writeIndex: number;
  _capacity: number;

  constructor(
    requestedCapacity: number,
    stats: SpanBufferStats,
    parent: SpanBuffer | undefined,
    isChained: boolean,
    callsiteMetadata: OpMetadata | undefined, // Caller's metadata for row 0
    opMetadata: OpMetadata | undefined, // Op's metadata
    traceRoot: ITraceRoot,
    vocabularyGeneration: VocabularyGeneration
  ) {
    // Store metadata and trace root
    this._callsiteMetadata = callsiteMetadata;
    this._opMetadata = opMetadata;
    this._traceRoot = traceRoot;
    this._vocabularyGeneration = vocabularyGeneration;
    this._nodeIndex = NO_NODE; // Set by _traceRoot._topology.registerRoot/registerChild/adoptOverflow
    this._topologyGeneration = 0;
    this._overflow = undefined;

    // Calculate system buffer size
    const systemSize = requestedCapacity * 9; // timestamp (8*cap) + entry_type (1*cap)
    // Thread-local span counter (per-process/worker, see threadId.ts docs)
    const spanId = ++globalThis.globalSpanCounter;

    if (isChained && parent) {
      // CHAINED: share identity, only allocate system columns
      this._parent = parent._parent;
      this._system = new ArrayBuffer(systemSize);
      this._identity = parent._identity; // Shared reference!
    } else if (parent) {
      // CHILD: own 12-byte identity (thread_id + span_id)
      this._parent = parent;
      const identitySize = 12;
      this._system = new ArrayBuffer(systemSize + identitySize);
      this._identity = new Uint8Array(this._system, systemSize, identitySize);

      // Set thread_id via thread_id.ts module-level singleton (cached bytes)
      copyThreadIdTo(this._identity, 0);

      // Set span_id (bytes 8-11, little-endian) from the thread-local counter
      this._identity[8] = spanId;
      this._identity[9] = spanId >>> 8;
      this._identity[10] = spanId >>> 16;
      this._identity[11] = spanId >>> 24;
    } else {
      // ROOT: identity with trace_id
      const traceIdBytes = traceRoot._traceIdBytes;
      const identitySize = 13 + traceIdBytes.length;
      this._system = new ArrayBuffer(systemSize + identitySize);
      this._identity = new Uint8Array(this._system, systemSize, identitySize);

      // Set thread_id via thread_id.ts module-level singleton (cached bytes)
      copyThreadIdTo(this._identity, 0);

      // Set span_id
      this._identity[8] = spanId;
      this._identity[9] = spanId >>> 8;
      this._identity[10] = spanId >>> 16;
      this._identity[11] = spanId >>> 24;

      // Set trace_id length and bytes
      this._identity[12] = traceIdBytes.length;
      this._identity.set(traceIdBytes, 13);
    }

    // System columns at FIXED offsets (same for ALL buffer types)
    this.timestamp = new BigInt64Array(this._system, 0, requestedCapacity);
    this.entry_type = new Uint8Array(this._system, requestedCapacity * 8, requestedCapacity);

    this._writeIndex = 0;
  }
}
```

### V8 In-Object Property Optimization

**Contract: property assignment order and shape stability are performance requirements.**

The generated constructor MUST initialize every hot own-property in the same order for every instance of a generated
class. Warmed setters observe one receiver shape and value representation per call site; no schema-name lookup, property
addition/deletion, or JS/WASM receiver mixing is allowed there. Benchmark evidence MUST detect hidden-class transitions
and polymorphic/megamorphic ICs when diagnostics exist. The common capacity branch is biased to not-overflow; lazy
allocation and overflow branch into explicit slow helpers.

**SpanBuffer Property Ordering (Hottest → Coldest):**

```typescript
// SLOT 1-3: HOTTEST - Read/written on EVERY log entry
this._writeIndex = 0;          // Incremented every write
this._capacity = requestedCapacity;  // Compared every write (overflow check)
this._overflow = undefined;    // Checked on overflow

// SLOT 4-5: HOT - TypedArray refs accessed every entry
this.timestamp = timestampView;      // Written every entry
this.entry_type = entryTypeView;     // Written every entry

// SLOT 6-7: WARM - Tree structure (span creation/navigation)
this._nodeIndex = NO_NODE;     // Topology handle, set when the factory registers the span
this._parent = parent;         // Walked for trace_id lookup

// SLOT 8-9: WARM - Identity and binding (accessed during span ops)
this._identity = identityView; // Accessed for span_id/thread_id
this._opMetadata = opMetadata; // Accessed for schema/capacity stats

// SLOT 10+: COLD - Only accessed during Arrow conversion or rarely
this._system = systemBuffer;   // Underlying ArrayBuffer (accessed via views)
this.message_values[0] = spanName; // Span name written to message row 0
this._scopeValues = ...;       // Scope inheritance
this._callsiteMetadata = ...;  // Row 0 metadata
```

**Access Frequency Analysis:**

| Property                                                   | Access Pattern           | Frequency                |
| ---------------------------------------------------------- | ------------------------ | ------------------------ |
| `_writeIndex`                                              | Read + increment         | Every log entry          |
| `_capacity`                                                | Read (overflow check)    | Every log entry          |
| `_overflow`                                                | Read (buffer chain)      | On overflow              |
| `timestamp` / `entry_type`                                 | Array indexing           | Every log entry          |
| `_nodeIndex` / `_parent`                                   | Tree navigation          | Span creation/completion |
| `_identity` / `_opMetadata`                                | Metadata access          | Span operations          |
| `_system`                                                  | Never after construction | Constructor only         |
| `message_values[0]` / `_scopeValues` / `_callsiteMetadata` | Arrow conversion         | Cold path only           |

**Why This Matters:**

- In-object property access: **1-2 CPU cycles** (inline cached)
- Out-of-object property access: **5-10 CPU cycles** (extra indirection through backing store)
- On hot path (millions of log entries), this 3-8 cycle difference per property access compounds significantly

**See Also:** [Buffer Performance Optimizations](./01b1_buffer_performance_optimizations.md) for detailed V8
optimization patterns.

### Getters (Cold Path - Lazy DataView)

```typescript
// span_id at fixed offset 8-11 for both root and child
get span_id(): number {
  const b = this._identity;
  return b[8] | (b[9] << 8) | (b[10] << 16) | (b[11] << 24);
}

get hasParent(): boolean {
  return this._parent !== undefined;
}

get parentSpanId(): number {
  return this._parent?.span_id ?? 0;
}

get trace_id(): string {
  if (this._parent) {
    return this._parent.trace_id;  // Walk up
  }
  // Root: decode from identity
  const len = this._identity[12];
  return String.fromCharCode(...this._identity.subarray(13, 13 + len));
}

// Copy thread_id bytes for Arrow conversion
copyThreadIdTo(dest: Uint8Array, offset: number): void {
  dest.set(this._identity.subarray(0, 8), offset);
}
```

### External Prototype Methods

Methods are defined externally and assigned to prototype for smaller generated code:

```typescript
// Defined once, shared by all SpanBuffer instances
const spanBufferMethods = {
  isParentOf(this: SpanBuffer, other: SpanBuffer): boolean {
    return this === other._parent; // Pointer comparison!
  },

  isChildOf(this: SpanBuffer, other: SpanBuffer): boolean {
    return this._parent === other; // Pointer comparison!
  },

  copyThreadIdTo(this: SpanBuffer, dest: Uint8Array, offset: number): void {
    dest.set(this._identity.subarray(0, 8), offset);
  },
};

// Assign to prototype
Object.assign(SpanBuffer.prototype, spanBufferMethods);
```

### Performance Characteristics

Construction counts below are **span-setup** costs. Warmed-write allocation claims exclude them. Effective-memory
reporting includes object/tree/view overhead, backing stores, lazy columns, and lease-pinned/Arrow-owned output.

**Construction (root span):**

- 1 ArrayBuffer allocation (systemSize + 13 + trace_id.length)
- 3 TypedArray view creations (timestamp, entry_type, \_identity)
- 1 Uint8Array.set() for thread_id (8 bytes)
- 4 byte writes for span_id
- 1 byte write for trace_id length
- 1 Uint8Array.set() for trace_id bytes

**Construction (child span):**

- 1 ArrayBuffer allocation (systemSize + 12)
- 3 TypedArray view creations
- 1 Uint8Array.set() for thread_id (8 bytes)
- 4 byte writes for span_id
- 1 pointer assignment to parent

**Construction (chained overflow):**

- 1 ArrayBuffer allocation (systemSize only - smallest!)
- 2 TypedArray view creations (timestamp, entry_type)
- 1 pointer assignment for \_identity (shared!)
- 1 pointer assignment for parent

**Comparison:**

- `isParentOf`: O(1) pointer comparison
- `isChildOf`: O(1) pointer comparison
- No byte loops, no DataView creation

**trace_id access:**

- Root: O(1) decode from \_identity
- Child depth N: O(N) pointer walks (typically N=3-5)

## Complete SpanBuffer Interface <a id="smoo/lmao!n/spanbuffer-layout.interface"></a>

**Purpose**: Provide the authoritative interface definition that can be extended with schema-generated columns.

**PACKAGE**: This interface is defined in **lmao** (`packages/lmao/src/lib/types.ts`). It extends arrow-builder's
`AnyColumnBuffer` with span-specific properties (tree structure, identity, context).

**CRITICAL**: Column properties are **direct properties** on the SpanBuffer via lazy getters (no nested
`columns: Record<...>`). This design provides zero indirection for hot path access.

```typescript
// PACKAGE: lmao - SpanBuffer extends arrow-builder's AnyColumnBuffer
interface SpanBuffer {
  // Core columns - always present (allocated immediately in constructor)
  timestamp: BigInt64Array; // Every operation appends timestamp (nanoseconds)
  entry_type: Uint8Array; // Operation type: tag, ok, err, etc.
  lineNumber_values: Int32Array; // Line numbers for each entry
  lineNumber_nulls: Uint8Array; // Null bitmap for line numbers

  // Attribute columns - DIRECT PROPERTIES with LAZY GETTERS (no nested Record!)
  // Each attribute has TWO properties sharing ONE ArrayBuffer:
  // - X_nulls: Uint8Array for null bitmap (Arrow format: 1=valid, 0=null)
  // - X_values: TypedArray for actual values
  // Schema-generated via new Function() at module creation time
  userId_nulls: Uint8Array; // Lazy getter
  userId_values: Uint32Array; // Lazy getter
  userId: Uint32Array; // Shorthand getter (alias for _values)

  requestId_nulls: Uint8Array;
  requestId_values: Uint32Array;
  requestId: Uint32Array;

  http_status_nulls: Uint8Array; // Prefixed library column
  http_status_values: Uint16Array;
  http_status: Uint16Array;
  // ... same pattern for all schema attributes

  // Tree structure
  _nodeIndex: number; // Node in _traceRoot._topology (child links live there)
  _parent?: SpanBuffer; // Reference to parent SpanBuffer

  // Dual metadata references for accurate source attribution
  _callsiteMetadata?: OpMetadata; // Caller's metadata (for row 0)
  _opMetadata: OpMetadata; // Op's metadata (for rows 1+)
  spanName: string; // Span name for this invocation

  // Buffer management
  _writeIndex: number; // Current write position (0 to capacity-1)
  _capacity: number; // Logical capacity for bounds checking
  _overflow?: SpanBuffer; // Chain to next buffer when overflow

  // Span Identification (see Span Identity section above)
  thread_id: bigint; // 64-bit random ID per worker
  span_id: number; // 32-bit thread-local counter
  trace_id: string; // Root trace ID (constant per span)

  // Helpers (don't trigger allocation)
  getColumnIfAllocated(columnName: string): TypedArray | undefined;
  getNullsIfAllocated(columnName: string): Uint8Array | undefined;
}

// OpMetadata - per-op source attribution, one per defineOp
interface OpMetadata {
  name: string; // Op name for metrics
  package_name: string; // npm package name from package.json
  package_file: string; // Path within package, relative to package.json
  git_sha: string; // Git SHA at build time
  line: number; // Line of the defineOp call

  // Pre-encoded UTF-8 for Arrow dictionary building (interned once at defineOp time)
  package_name_entry: PreEncodedEntry;
  package_file_entry: PreEncodedEntry;
  git_sha_entry: PreEncodedEntry;

  // Startup-resolved physical layout used by span setup; absent only on raw fallback metadata
  _physicalLayoutPlan?: {
    SpanBufferClass: SpanBufferConstructor;
    eagerColumns: EagerColumnDescriptor;
    vocabularyGeneration: VocabularyGeneration;
    localMessageDictionary: readonly number[];
    encodeLocalMessage: (globalDenseIndex: number) => number;
    wasmLayout: WasmLayoutTemplate;
  };
}

// The log schema lives on the op context's LogBinding; capacity stats live on the generated class
// (SpanBufferClass.stats, see Capacity Statistics below).
```

### Access Patterns

```typescript
// Op metadata (row 0 uses _callsiteMetadata, rows 1+ use _opMetadata)
buffer._callsiteMetadata?.package_name; // Caller's '@mycompany/http' (for row 0)
buffer._opMetadata.package_name; // Op's metadata (for rows 1+)
buffer.spanName; // Span name (direct property)

// Self-tuning stats (static on the generated class, shared by its buffers)
buffer._stats.capacity; // 8 (default, self-tuning adapts)
buffer._stats.totalWrites; // 1234

// Direct property access - zero indirection:
buffer.userId_values[idx] = value; // ✅ Direct TypedArray access
buffer.userId_nulls[byteIdx] |= bitmask; // ✅ Direct bitmap access

// Check allocation without triggering it:
const values = buffer.getColumnIfAllocated('userId');
```

## Buffer Creation Functions <a id="smoo/lmao!n/spanbuffer-layout-creation"></a>

**Purpose**: Factory functions for creating SpanBuffer instances with correct identity and hierarchy.

**PACKAGE**: All buffer creation functions are in **lmao** (`packages/lmao/src/lib/spanBuffer.ts`).

### createSpanBuffer - Root Buffer Creation <a id="smoo/lmao!n/spanbuffer-layout.create-root"></a>

Creates a root SpanBuffer for a new trace with full identity including trace_id.

```typescript
function createSpanBuffer<T extends LogSchema>(
  schema: T,
  spanName: string,
  traceRoot: TraceRoot,
  opMetadata: OpMetadata,
  capacity?: number
): SpanBuffer<T>;
```

**Parameters:**

- `schema` - Tag attribute schema defining column types (must be LogSchema)
- `spanName` - Name of the span
- `traceRoot` - Pre-built TraceRoot with trace_id, timestamp anchors, and tracer reference
- `opMetadata` - Op metadata for attribution (package_name, package_file, git_sha, line)
- `capacity` - Optional buffer capacity (uses class.stats.capacity if omitted)

**Returns:** SpanBuffer with typed setters for schema fields

**Identity Layout:** `[threadId(8)][spanId(4)][traceIdLen(1)][traceId(N)]`

**Memory:** Single ArrayBuffer containing timestamps, entry_type, and identity (13 + traceId.length bytes)

### createChildSpanBuffer - Child Buffer Creation <a id="smoo/lmao!n/spanbuffer-layout.create-child"></a>

Creates a child span buffer with parent linkage and simplified identity (no trace_id bytes).

```typescript
function createChildSpanBuffer<T extends LogSchema>(
  parentBuffer: AnySpanBuffer,
  SpanBufferClass: SpanBufferConstructor,
  spanName: string,
  callsiteMetadata: OpMetadata,
  opMetadata: OpMetadata,
  capacity?: number
): SpanBuffer<T>;
```

**Parameters:**

- `parentBuffer` - The parent span buffer (for tree hierarchy and traceRoot)
- `SpanBufferClass` - The SpanBuffer class to instantiate (has static schema + stats)
- `spanName` - Name of the child span
- `callsiteMetadata` - Caller's op metadata (for row 0 attribution - who called span())
- `opMetadata` - Op metadata for this span (for rows 1+ attribution - what's executing)
- `capacity` - Optional capacity override (uses class.stats.capacity if omitted)

**Returns:** New SpanBuffer linked to parent

**Identity Layout:** `[threadId(8)][spanId(4)]` (12 bytes total)

**Memory:** Single ArrayBuffer containing timestamps, entry_type, and simplified identity (12 bytes)

**Note:** Child buffers access trace_id by walking parent chain to root (typically 3-5 levels).

### createOverflowBuffer - Buffer Chaining <a id="smoo/lmao!n/spanbuffer-layout.create-overflow"></a>

Creates a continuation chunk when the current chunk overflows. This slow path allocates system storage/views and links
the chunk; it shares the immutable identity view with the first buffer. Overflow is excluded from the warmed
fixed-capacity zero-allocation claim.

```typescript
function createOverflowBuffer<T extends LogSchema>(buffer: SpanBuffer<T>): SpanBuffer<T>;
```

**Parameters:**

- `buffer` - The full buffer that needs overflow handling

**Returns:** New SpanBuffer linked via `buffer._overflow`, with same schema type

**Identity Layout:** Shares `_identity` from the original buffer (zero additional identity payload bytes).

**Memory:** Allocates one system backing store plus views. Arrow keeps it chunked where possible or performs one
explicitly accounted contiguous copy.

**Note:** Capacity is rounded up to multiple of 8 for byte-aligned null bitmaps.

### TraceRoot Interface <a id="smoo/lmao!n/spanbuffer-layout-traceroot"></a>

TraceRoot is created once per trace and shared by reference across all spans. It provides timestamp anchoring and tracer
lifecycle hooks.

`ITraceRoot` (`packages/lmao/src/lib/traceRoot.ts`) is fenced under the high-precision-timestamp node
`smoo/lmao!n/trace-root-timestamps` ([01b3](./01b3_high_precision_timestamps.md)), so this heading shares that fence.
The interface also carries `_system` (backing anchor buffer) and the `writeSpanStart`/`writeSpanEnd`/`writeLogEntry`/
`getTimestampNanos` write API beyond the four read-only fields sketched below; `tracer` is `TracerLifecycleHooks`, which
carries `onStatsWillResetFor` (the [Buffer Self-Tuning](./01b2_buffer_self_tuning.md) observability hook).

```typescript
interface TraceRoot {
  /**
   * Trace ID for this trace.
   * Stored here for quick access without walking buffer parent chain.
   */
  readonly trace_id: TraceId;

  /**
   * Wall-clock epoch time in nanoseconds when trace was created, to sub-millisecond precision:
   * `wallClockNanos(performance.now(), Date.now())` at trace root (01b3).
   */
  readonly anchorEpochNanos: bigint;

  /**
   * High-resolution timer value when trace was created.
   * Captured via performance.now() (browser) or process.hrtime.bigint() (Node.js).
   */
  readonly anchorPerfNow: number;

  /**
   * Tracer reference for lifecycle hooks and event callbacks.
   * Provides onTraceStart, onTraceEnd, onSpanStart, onSpanEnd, onStatsWillResetFor.
   */
  readonly tracer: TracerLifecycleHooks;
}
```

**Memory Layout:**

- Created ONCE per trace (root span creation)
- Shared by ALL spans in trace (copied by reference, 8 bytes per child span)
- Total overhead: ~40 bytes per trace (object header + 4 properties)

**Why per-trace anchoring:**

- Each trace has fresh anchor - no long-running drift issues
- NTP corrections between traces are isolated
- Trace is self-contained unit with consistent time reference

See [High-Precision Timestamps](./01b3_high_precision_timestamps.md) for timestamp design details.

## Capacity Statistics <a id="smoo/lmao!n/spanbuffer-layout.stats"></a>

Each `SpanBufferClass` (generated per schema) has a static `stats` property for capacity tuning.

```typescript
interface SpanBufferStats {
  capacity: number; // Current capacity for new buffers
  totalWrites: number; // Completed user/event rows sealed across segments (rows 0/1 excluded once per span)
  spansCreated: number; // Non-chained spans created (root + child, not overflow buffers)
}

// Access via class
const SpanBufferClass = getSpanBufferClass(schema);
console.log(SpanBufferClass.stats.capacity); // Current capacity

// Access via instance
buffer._stats; // Getter returns this.constructor.stats
```

### Why Static?

Stats are per-schema, not per-buffer:

- All buffers from same schema share capacity learning
- Utilization computed across all buffers of the same class
- Enables cross-request capacity optimization

### Stats Updates

| Event                                | Stats Updated                                          |
| ------------------------------------ | ------------------------------------------------------ |
| Root or child span created (factory) | `spansCreated++`                                       |
| Buffer sealed (overflow or span end) | `totalWrites += completed rows`                        |
| Overflow triggered                   | buffer sealed, tracer hook, then the tuning check      |
| Capacity adjusted                    | `capacity` updated, `totalWrites`/`spansCreated` reset |

### Benchmark Acceptance

For each `PhysicalLayoutPlan`, Mitata scenarios MUST follow
[01b1 §Required Mitata Protocol](./01b1_buffer_performance_optimizations.md#smoo/lmao!n/buffer-perf-benchmark-protocol):
plugin-off, plugin-on/current, and candidates use identical semantic checksums, position-balanced ordering,
machine-readable raw samples, and p50/p95/p99/p99.9 for startup, span setup, warmed fixed-capacity writes, first lazy
allocation, overflow, and per-request Arrow flush. Allocation/GC, effective-memory, active-lease, copied-byte, chunk,
shape/IC, and branch observations are auxiliary instrumentation alongside Mitata and may be unavailable. Reject shape or
checksum changes, warmed-write allocations, polymorphic hot ICs, unstable common branches, lease-lifetime violations, or
regressions beyond the predeclared noise threshold.
