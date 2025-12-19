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

1. **Single `_system` ArrayBuffer** per buffer containing timestamps, operations, and identity (for non-chained)
2. **System columns FIRST** - timestamps and operations at fixed offsets 0 and `capacity * 8`
3. **Identity AFTER system columns** - variable size depending on buffer type (root vs child vs chained)
4. **Parent pointer for ancestry** - no copied parent identity bytes, just walk the `parent` reference
5. **Chained buffers share identity** - overflow buffers point to the same `_identity` view as their root

### Buffer Type Layouts

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ ROOT SPAN BUFFER                                                                │
│                                                                                 │
│ _system: ArrayBuffer                                                            │
│ ┌────────────────────────┬──────────────────┬────────────────────────────────┐  │
│ │ timestamps             │ operations       │ identity                       │  │
│ │ BigInt64Array          │ Uint8Array       │ [threadId][spanId][len][trace] │  │
│ │ 8 * capacity bytes     │ 1 * capacity     │ 8 + 4 + 1 + traceId.length     │  │
│ └────────────────────────┴──────────────────┴────────────────────────────────┘  │
│  offset: 0                capacity * 8       capacity * 9                       │
│                                                                                 │
│ Views:                                                                          │
│   timestamps ──► BigInt64Array(this._system, 0, capacity)                       │
│   operations ──► Uint8Array(this._system, capacity * 8, capacity)               │
│   _identity  ──► Uint8Array(this._system, capacity * 9, 13 + traceId.length)    │
│                                                                                 │
│ Identity layout (13 + traceId.length bytes):                                    │
│   [0-7]   threadId    (8 bytes, crypto-secure random, same for all spans)       │
│   [8-11]  spanId      (4 bytes, Uint32, incrementing counter)                   │
│   [12]    traceIdLen  (1 byte, length of traceId string)                        │
│   [13+]   traceId     (1-128 bytes, ASCII string)                               │
│                                                                                 │
│ Properties:                                                                     │
│   parent: undefined (root has no parent)                                        │
│   children: SpanBuffer[]                                                        │
│   callsiteModule: ModuleContext (caller's module for row 0 metadata)            │
│   module: ModuleContext (Op's module for rows 1+ metadata)                      │
│   spanName: string (per-span data)                                              │
│   NOTE: lineNumber is in lineNumber_values TypedArray, NOT a property           │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│ CHILD SPAN BUFFER                                                               │
│                                                                                 │
│ _system: ArrayBuffer                                                            │
│ ┌────────────────────────┬──────────────────┬─────────────────┐                 │
│ │ timestamps             │ operations       │ identity        │                 │
│ │ BigInt64Array          │ Uint8Array       │ [threadId][span]│                 │
│ │ 8 * capacity bytes     │ 1 * capacity     │ 8 + 4 = 12 bytes│                 │
│ └────────────────────────┴──────────────────┴─────────────────┘                 │
│  offset: 0                capacity * 8       capacity * 9                       │
│                                                                                 │
│ Views:                                                                          │
│   timestamps ──► BigInt64Array(this._system, 0, capacity)                       │
│   operations ──► Uint8Array(this._system, capacity * 8, capacity)               │
│   _identity  ──► Uint8Array(this._system, capacity * 9, 12)                     │
│                                                                                 │
│ Identity layout (12 bytes):                                                     │
│   [0-7]   threadId    (8 bytes, same as process threadId)                       │
│   [8-11]  spanId      (4 bytes, Uint32, incrementing counter)                   │
│                                                                                 │
│ Properties:                                                                     │
│   parent ──────────────► (parent SpanBuffer - for traceId + parentSpanId)       │
│   children: SpanBuffer[]                                                        │
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
│ │ timestamps             │ operations       │                                   │
│ │ BigInt64Array          │ Uint8Array       │                                   │
│ │ 8 * capacity bytes     │ 1 * capacity     │                                   │
│ └────────────────────────┴──────────────────┘                                   │
│  offset: 0                capacity * 8                                          │
│                                                                                 │
│ Views:                                                                          │
│   timestamps ──► BigInt64Array(this._system, 0, capacity)                       │
│   operations ──► Uint8Array(this._system, capacity * 8, capacity)               │
│   _identity  ──────────────► (first buffer's _identity - shared reference!)     │
│                                                                                 │
│ Properties:                                                                     │
│   parent ──────────────► (same as first buffer's parent)                        │
│   children: [] (only root buffer tracks children)                               │
│   callsiteModule: ModuleContext (shared from first buffer)                      │
│   module: ModuleContext (Op's module, shared reference)                         │
│   spanName: string (per-span data)                                              │
│   NOTE: lineNumber is in lineNumber_values TypedArray, NOT a property           │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Memory Savings

| Buffer Type | Separate Allocations (alternative)                  | Unified \_system (chosen)              |
| ----------- | --------------------------------------------------- | -------------------------------------- |
| Root        | 25 bytes identity + timestamps AB + ops AB          | Single AB: capacity\*9 + 13 + traceLen |
| Child       | 25 bytes identity + timestamps AB + ops AB          | Single AB: capacity\*9 + 12            |
| Chained     | 25 bytes identity (copied) + timestamps AB + ops AB | Single AB: capacity\*9 (no identity!)  |

**Key savings from unified approach:**

- Root/Child: 2 fewer ArrayBuffer allocations per span
- Chained: 12-141 bytes saved (no identity bytes at all) + zero copy
- All: Better cache locality (identity adjacent to hot system columns)

### Why System Columns BEFORE Identity

Placing timestamps and operations at the START of the buffer means:

1. **Fixed offsets for ALL buffer types** - timestamps always at 0, ops always at `capacity * 8`
2. **Zero conditional logic** - same view creation code for root, child, and chained
3. **Chained is just truncated** - same layout prefix, just shorter (no identity suffix)

```typescript
// SAME code for root, child, AND chained:
this.timestamps = new BigInt64Array(this._system, 0, capacity);
this.operations = new Uint8Array(this._system, capacity * 8, capacity);

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
  return this.parent?.spanId ?? 0;
}

get traceId(): string {
  if (this.parent) {
    return this.parent.traceId;  // Walk up to root
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
- `traceId` walks to root (spans rarely deep, typically 3-5 levels)

### spanId at Fixed Offset

The `spanId` getter works identically for root and child because threadId and spanId are at the same offsets:

```typescript
// Identity layout for both root and child:
//   [0-7]   threadId (8 bytes)
//   [8-11]  spanId (4 bytes)
//   ... (root has traceIdLen + traceId after, child stops here)

get spanId(): number {
  const b = this._identity;
  return b[8] | (b[9] << 8) | (b[10] << 16) | (b[11] << 24);
}
```

### Thread ID Generation

Thread ID is cached as raw bytes at module level (threadId.ts) for zero-copy writes:

````typescript
// Module-level singleton (threadId.ts)
// Generated once per process/worker, cached as Uint8Array for zero-copy writes
let threadIdBytes: Uint8Array | null = null;

function ensureInitialized(): void {
  if (threadIdBytes !== null) return;
  threadIdBytes = new Uint8Array(8);
  crypto.getRandomValues(threadIdBytes); // Crypto-secure, generated once
}

// Hot-path API: copy cached bytes directly (zero-copy)
function copyThreadIdTo(dest: Uint8Array, offset: number): void {
  ensureInitialized();
  dest.set(threadIdBytes!, offset); // Direct copy of cached bytes
}

### Constructor Implementation

```typescript
class SpanBuffer {
  readonly _system: ArrayBuffer;
  readonly _identity: Uint8Array;
  readonly timestamps: BigInt64Array;
  readonly operations: Uint8Array;

  parent?: SpanBuffer;
  children: SpanBuffer[];
  next?: SpanBuffer;

  // Per-span invocation data
  // NOTE: lineNumber is in lineNumber_values TypedArray, NOT a property on SpanBuffer
  callsiteModule?: ModuleContext; // Caller's module (for row 0's gitSha/packageName/packagePath)
  module: ModuleContext; // Op's module (for rows 1+ gitSha/packageName/packagePath)
  spanName: string;

  writeIndex: number;
  capacity: number;

  constructor(
    requestedCapacity: number,
    module: ModuleContext, // Op's module context
    spanName: string, // Span name
    parent?: SpanBuffer,
    isChained = false,
    traceId?: string, // Only for root spans
    callsiteModule?: ModuleContext // Caller's module for row 0 metadata
  ) {
    // Store module and spanName (flattened from TaskContext)
    this.module = module;
    this.spanName = spanName;
    this.children = [];
    this.next = undefined;
    this.callsiteModule = callsiteModule;

    // Calculate system buffer size
    const systemSize = requestedCapacity * 9; // timestamps (8*cap) + operations (1*cap)

    if (isChained && parent) {
      // CHAINED: share identity, only allocate system columns
      this.parent = parent.parent;
      this._system = new ArrayBuffer(systemSize);
      this._identity = parent._identity; // Shared reference!
    } else if (parent) {
      // CHILD: own 12-byte identity (threadId + spanId)
      this.parent = parent;
      const identitySize = 12;
      this._system = new ArrayBuffer(systemSize + identitySize);
      this._identity = new Uint8Array(this._system, systemSize, identitySize);

      // Set threadId via threadId.ts module-level singleton (cached bytes)
      copyThreadIdTo(this._identity, 0);

      // Set spanId (bytes 8-11, little-endian) - accesses module-level nextSpanId via closure
      sbHelpers.writeSpanId(this._identity, 8, nextSpanId++);
    } else {
      // ROOT: identity with traceId
      const traceBytes = traceId ? sbHelpers.textEncoder.encode(traceId) : new Uint8Array(0);
      const identitySize = 13 + traceBytes.length;
      this._system = new ArrayBuffer(systemSize + identitySize);
      this._identity = new Uint8Array(this._system, systemSize, identitySize);

      // Set threadId via threadId.ts module-level singleton (cached bytes)
      copyThreadIdTo(this._identity, 0);

      // Set spanId
      sbHelpers.writeSpanId(this._identity, 8, nextSpanId++);

      // Set traceId length and bytes
      this._identity[12] = traceBytes.length;
      this._identity.set(traceBytes, 13);
    }

    // System columns at FIXED offsets (same for ALL buffer types)
    this._timestamps = new BigInt64Array(this._system, 0, requestedCapacity);
    this._operations = new Uint8Array(this._system, requestedCapacity * 8, requestedCapacity);

    // Direct property aliases for system columns (V8 hidden class friendly)
    this.timestamps = this._timestamps;
    this.operations = this._operations;

    this._writeIndex = 0;

    // Track buffer creation
    module.sb_totalCreated++;
  }
}
````

### Getters (Cold Path - Lazy DataView)

```typescript
// spanId at fixed offset 8-11 for both root and child
get spanId(): number {
  const b = this._identity;
  return b[8] | (b[9] << 8) | (b[10] << 16) | (b[11] << 24);
}

get hasParent(): boolean {
  return this.parent !== undefined;
}

get parentSpanId(): number {
  return this.parent?.spanId ?? 0;
}

get traceId(): string {
  if (this.parent) {
    return this.parent.traceId;  // Walk up
  }
  // Root: decode from identity
  const len = this._identity[12];
  return String.fromCharCode(...this._identity.subarray(13, 13 + len));
}

// Copy threadId bytes for Arrow conversion
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

- 1 ArrayBuffer allocation (systemSize + 13 + traceId.length)
- 3 TypedArray view creations (timestamps, operations, \_identity)
- 1 Uint8Array.set() for threadId (8 bytes)
- 4 byte writes for spanId
- 1 byte write for traceId length
- 1 Uint8Array.set() for traceId bytes

**Construction (child span):**

- 1 ArrayBuffer allocation (systemSize + 12)
- 3 TypedArray view creations
- 1 Uint8Array.set() for threadId (8 bytes)
- 4 byte writes for spanId
- 1 pointer assignment to parent

**Construction (chained overflow):**

- 1 ArrayBuffer allocation (systemSize only - smallest!)
- 2 TypedArray view creations (timestamps, operations)
- 1 pointer assignment for \_identity (shared!)
- 1 pointer assignment for parent

**Comparison:**

- `isParentOf`: O(1) pointer comparison
- `isChildOf`: O(1) pointer comparison
- No byte loops, no DataView creation

**traceId access:**

- Root: O(1) decode from \_identity
- Child depth N: O(N) pointer walks (typically N=3-5)
