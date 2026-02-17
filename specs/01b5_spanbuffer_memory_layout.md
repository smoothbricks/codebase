# SpanBuffer Memory Layout

> **Part of [Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**
>
> This document details the unified memory layout for SpanBuffer, combining identity and system columns into a single
> ArrayBuffer for maximum cache efficiency. For span identification concepts, see
> [Span Identity](./01b4_span_identity.md).

## Unified SpanBuffer Memory Layout

**Purpose**: Combine identity and system columns into a single ArrayBuffer allocation for maximum cache efficiency,
minimal allocations, and zero conditional logic for system column access.

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
│   parent: undefined (root has no parent)                                         │
│   _children: SpanBuffer[]                                                        │
│   callsiteModule: ModuleContext (caller's module for row 0 metadata)             │
│   module: ModuleContext (Op's module for rows 1+ metadata)                       │
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
│   parent ──────────────► (parent SpanBuffer - for trace_id + parentSpanId)      │
│   _children: SpanBuffer[]                                                       │
│   callsiteModule: ModuleContext (caller's module for row 0 metadata)            │
│   module: ModuleContext (Op's module for rows 1+ metadata)                      │
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
│   parent ──────────────► (same as first buffer's parent)                        │
│   _children: [] (only root buffer tracks _children)                             │
│   callsiteModule: ModuleContext (shared from first buffer)                      │
│   module: ModuleContext (Op's module, shared reference)                         │
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
- Chained: 12-141 bytes saved (no identity bytes at all) + zero copy
- All: Better cache locality (identity adjacent to hot system columns)

### Why System Columns BEFORE Identity

Placing timestamp and entry_type at the START of the buffer means:

1. **Fixed offsets for ALL buffer types** - timestamp always at 0, entry_type always at `capacity * 8`
2. **Zero conditional logic** - same view creation code for root, child, and chained
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
  return this.parent !== undefined;
}

get parentSpanId(): number {
  return this.parent?.span_id ?? 0;
}

get trace_id(): string {
  if (this.parent) {
    return this.parent.trace_id;  // Walk up to root
  }
  // Root: decode from _identity
  const len = this._identity[12];
  return String.fromCharCode(...this._identity.subarray(13, 13 + len));
}

// MASSIVE WIN: isParentOf becomes pointer comparison!
isParentOf(other: SpanBuffer): boolean {
  return this === other.parent;
}

isChildOf(other: SpanBuffer): boolean {
  return this.parent === other;
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

### Thread ID Generation

Thread ID is cached as raw bytes at module level (thread_id.ts) for zero-copy writes:

````typescript
// Module-level singleton (thread_id.ts)
// Generated once per process/worker, cached as Uint8Array for zero-copy writes
let thread_idBytes: Uint8Array | null = null;

function ensureInitialized(): void {
  if (thread_idBytes !== null) return;
  thread_idBytes = new Uint8Array(8);
  crypto.getRandomValues(thread_idBytes); // Crypto-secure, generated once
}

// Hot-path API: copy cached bytes directly (zero-copy)
function copyThreadIdTo(dest: Uint8Array, offset: number): void {
  ensureInitialized();
  dest.set(thread_idBytes!, offset); // Direct copy of cached bytes
}

### Constructor Implementation

```typescript
class SpanBuffer {
  readonly _system: ArrayBuffer;
  readonly _identity: Uint8Array;
  readonly timestamp: BigInt64Array;
  readonly entry_type: Uint8Array;

  parent?: SpanBuffer;
  _children: SpanBuffer[];
  _next?: SpanBuffer;

  // Per-span invocation data
  // NOTE: lineNumber is in lineNumber_values TypedArray, NOT a property on SpanBuffer
  callsiteModule?: ModuleContext; // Caller's module (for row 0's gitSha/packageName/packagePath)
  module: ModuleContext; // Op's module (for rows 1+ gitSha/packageName/packagePath)
  spanName: string;

  _writeIndex: number;
  _capacity: number;

  constructor(
    requestedCapacity: number,
    module: ModuleContext, // Op's module context
    spanName: string, // Span name
    parent?: SpanBuffer,
    isChained = false,
    trace_id?: string, // Only for root spans
    callsiteModule?: ModuleContext // Caller's module for row 0 metadata
  ) {
    // Store module and spanName (flattened from TaskContext)
    this.module = module;
    this.spanName = spanName;
    this._children = [];
    this._next = undefined;
    this.callsiteModule = callsiteModule;

    // Calculate system buffer size
    const systemSize = requestedCapacity * 9; // timestamp (8*cap) + entry_type (1*cap)

    if (isChained && parent) {
      // CHAINED: share identity, only allocate system columns
      this.parent = parent.parent;
      this._system = new ArrayBuffer(systemSize);
      this._identity = parent._identity; // Shared reference!
    } else if (parent) {
      // CHILD: own 12-byte identity (thread_id + span_id)
      this.parent = parent;
      const identitySize = 12;
      this._system = new ArrayBuffer(systemSize + identitySize);
      this._identity = new Uint8Array(this._system, systemSize, identitySize);

      // Set thread_id via thread_id.ts module-level singleton (cached bytes)
      copyThreadIdTo(this._identity, 0);

      // Set span_id (bytes 8-11, little-endian) - accesses module-level nextSpanId via closure
      sbHelpers.writeSpanId(this._identity, 8, nextSpanId++);
    } else {
      // ROOT: identity with trace_id
      const traceBytes = trace_id ? sbHelpers.textEncoder.encode(trace_id) : new Uint8Array(0);
      const identitySize = 13 + traceBytes.length;
      this._system = new ArrayBuffer(systemSize + identitySize);
      this._identity = new Uint8Array(this._system, systemSize, identitySize);

      // Set thread_id via thread_id.ts module-level singleton (cached bytes)
      copyThreadIdTo(this._identity, 0);

      // Set span_id
      sbHelpers.writeSpanId(this._identity, 8, nextSpanId++);

      // Set trace_id length and bytes
      this._identity[12] = traceBytes.length;
      this._identity.set(traceBytes, 13);
    }

    // System columns at FIXED offsets (same for ALL buffer types)
    this._timestamp = new BigInt64Array(this._system, 0, requestedCapacity);
    this._entry_type = new Uint8Array(this._system, requestedCapacity * 8, requestedCapacity);

    // Direct property aliases for system columns (V8 hidden class friendly)
    this.timestamp = this._timestamp;
    this.entry_type = this._operations;

    this._writeIndex = 0;

    // Track buffer creation
    module.sb_totalCreated++;
  }
}
````

### V8 In-Object Property Optimization

**CRITICAL: Property Assignment Order Matters for Performance!**

V8 distinguishes between **in-object properties** (stored directly on the object, ~8-12 properties) and **out-of-object
properties** (stored in a backing store with extra indirection). The first properties assigned in the constructor get
fast in-object slots.

**SpanBuffer Property Ordering (Hottest → Coldest):**

```typescript
// SLOT 1-3: HOTTEST - Read/written on EVERY log entry
this._writeIndex = 0;          // Incremented every write
this._capacity = requestedCapacity;  // Compared every write (overflow check)
this._next = undefined;        // Checked on overflow

// SLOT 4-5: HOT - TypedArray refs accessed every entry
this.timestamp = timestampView;      // Written every entry
this.entry_type = entryTypeView;     // Written every entry

// SLOT 6-7: WARM - Tree structure (span creation/navigation)
this._children = [];           // Pushed to when creating child spans
this._parent = parent;         // Walked for trace_id lookup

// SLOT 8-9: WARM - Identity and binding (accessed during span ops)
this._identity = identityView; // Accessed for span_id/thread_id
this._logBinding = module;     // Accessed for schema/capacity stats

// SLOT 10+: COLD - Only accessed during Arrow conversion or rarely
this._system = systemBuffer;   // Underlying ArrayBuffer (accessed via views)
this._spanName = spanName;     // Metadata for Arrow conversion
this._scopeValues = ...;       // Scope inheritance
this._callsiteMetadata = ...;  // Row 0 metadata
```

**Access Frequency Analysis:**

| Property                                           | Access Pattern           | Frequency                |
| -------------------------------------------------- | ------------------------ | ------------------------ |
| `_writeIndex`                                      | Read + increment         | Every log entry          |
| `_capacity`                                        | Read (overflow check)    | Every log entry          |
| `_next`                                            | Read (buffer chain)      | On overflow              |
| `timestamp` / `entry_type`                         | Array indexing           | Every log entry          |
| `_children` / `_parent`                            | Tree navigation          | Span creation/completion |
| `_identity` / `_logBinding`                        | Metadata access          | Span operations          |
| `_system`                                          | Never after construction | Constructor only         |
| `_spanName` / `_scopeValues` / `_callsiteMetadata` | Arrow conversion         | Cold path only           |

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
  return this.parent !== undefined;
}

get parentSpanId(): number {
  return this.parent?.span_id ?? 0;
}

get trace_id(): string {
  if (this.parent) {
    return this.parent.trace_id;  // Walk up
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
    return this === other.parent; // Pointer comparison!
  },

  isChildOf(this: SpanBuffer, other: SpanBuffer): boolean {
    return this.parent === other; // Pointer comparison!
  },

  copyThreadIdTo(this: SpanBuffer, dest: Uint8Array, offset: number): void {
    dest.set(this._identity.subarray(0, 8), offset);
  },
};

// Assign to prototype
Object.assign(SpanBuffer.prototype, spanBufferMethods);
```

### Performance Characteristics

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

## Complete SpanBuffer Interface

**Purpose**: Provide the authoritative interface definition that can be extended with schema-generated columns.

**PACKAGE**: This interface is defined in **lmao** (`packages/lmao/src/lib/types.ts`). It extends arrow-builder's
`TypedColumnBuffer` with span-specific properties (tree structure, identity, context).

**CRITICAL**: Column properties are **direct properties** on the SpanBuffer via lazy getters (no nested
`columns: Record<...>`). This design provides zero indirection for hot path access.

```typescript
// PACKAGE: lmao - SpanBuffer extends arrow-builder's TypedColumnBuffer
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
  _children: SpanBuffer[];
  parent?: SpanBuffer; // Reference to parent SpanBuffer

  // Dual module references for accurate source attribution
  callsiteModule?: ModuleContext; // Caller's module (for row 0 metadata)
  module: ModuleContext; // Op's module (for rows 1+ metadata)
  spanName: string; // Span name for this invocation

  // Buffer management
  _writeIndex: number; // Current write position (0 to capacity-1)
  _capacity: number; // Logical capacity for bounds checking
  _next?: SpanBuffer; // Chain to next buffer when overflow

  // Span Identification (see Span Identity section above)
  thread_id: bigint; // 64-bit random ID per worker
  span_id: number; // 32-bit thread-local counter
  trace_id: string; // Root trace ID (constant per span)

  // Helpers (don't trigger allocation)
  getColumnIfAllocated(columnName: string): TypedArray | undefined;
  getNullsIfAllocated(columnName: string): Uint8Array | undefined;
}

// ModuleContext - module-level metadata with flattened stats
interface ModuleContext {
  packageName: string; // npm package name from package.json
  packagePath: string; // Path within package, relative to package.json
  gitSha: string; // Git SHA at build time
  prefix?: string; // Optional prefix for library integration
  logSchema: LogSchema; // Schema definition for this module

  // Self-tuning buffer capacity stats (flattened, not nested)
  sb_capacity: number; // Current buffer capacity
  sb_totalWrites: number; // Total writes for tuning decisions
  sb_overflows: number; // Overflow count for tuning decisions
  sb_totalCreated: number; // Total buffers created
}
```

### Access Patterns

```typescript
// Module metadata (row 0 uses callsiteModule, rows 1+ use module)
buffer.callsiteModule?.packageName; // Caller's '@mycompany/http' (for row 0)
buffer.module.packageName; // Op's module (for rows 1+)
buffer.spanName; // Span name (direct property)

// Self-tuning stats (flattened on module)
buffer.module.sb_capacity; // 8 (default, self-tuning adapts)
buffer.module.sb_totalWrites; // 1234

// Direct property access - zero indirection:
buffer.userId_values[idx] = value; // ✅ Direct TypedArray access
buffer.userId_nulls[byteIdx] |= bitmask; // ✅ Direct bitmap access

// Check allocation without triggering it:
const values = buffer.getColumnIfAllocated('userId');
```

## Buffer Creation Functions

**Purpose**: Factory functions for creating SpanBuffer instances with correct identity and hierarchy.

**PACKAGE**: All buffer creation functions are in **lmao** (`packages/lmao/src/lib/spanBuffer.ts`).

### createSpanBuffer - Root Buffer Creation

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

### createChildSpanBuffer - Child Buffer Creation

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

### createOverflowBuffer - Buffer Chaining

Creates a continuation buffer when the current buffer overflows. Chained buffers SHARE the identity reference from the
first buffer (they represent the SAME logical span, just additional storage).

```typescript
function createOverflowBuffer<T extends LogSchema>(buffer: SpanBuffer<T>): SpanBuffer<T>;
```

**Parameters:**

- `buffer` - The full buffer that needs overflow handling

**Returns:** New SpanBuffer linked via `buffer._overflow`, with same schema type

**Identity Layout:** Shares `_identity` reference from original buffer (zero bytes allocated)

**Memory:** Single ArrayBuffer containing ONLY timestamps and entry_type (smallest allocation!)

**Note:** Capacity is rounded up to multiple of 8 for byte-aligned null bitmaps.

### TraceRoot Interface

TraceRoot is created once per trace and shared by reference across all spans. It provides timestamp anchoring and tracer
lifecycle hooks.

```typescript
interface TraceRoot {
  /**
   * Trace ID for this trace.
   * Stored here for quick access without walking buffer parent chain.
   */
  readonly trace_id: TraceId;

  /**
   * Epoch time in nanoseconds when trace was created.
   * Captured via Date.now() * 1_000_000n at trace root.
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

## Capacity Statistics

Each `SpanBufferClass` (generated per schema) has a static `stats` property for capacity tuning:

```typescript
interface SpanBufferStats {
  capacity: number; // Current capacity for new buffers
  totalWrites: number; // Total entries written across all buffers
  overflowWrites: number; // Number of overflow events (incremented once per overflow)
  totalCreated: number; // Total buffers created (root + children + chains)
  overflows: number; // Number of overflow events
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
- Overflow ratio computed across all buffers of same type
- Enables cross-request capacity optimization

### Stats Updates

| Event              | Stats Updated                     |
| ------------------ | --------------------------------- |
| Buffer created     | `totalCreated++`                  |
| Entry written      | `totalWrites++`                   |
| Overflow triggered | `overflows++`, `overflowWrites++` |
| Capacity adjusted  | `capacity` updated, stats reset   |
