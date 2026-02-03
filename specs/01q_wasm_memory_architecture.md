# 01q: WASM Memory Architecture for SpanBuffer Storage

## Overview

This spec describes a WASM-based memory architecture for SpanBuffer columnar storage. The goal is to consolidate numeric
column data into WebAssembly memory for improved cache locality and explicit memory management, while keeping string
data in JavaScript.

## Motivation

### Current Architecture

Each SpanBuffer owns separate JavaScript TypedArrays:

- `timestamp`: `BigInt64Array`
- `entry_type`: `Uint8Array`
- Per-column: `Float64Array`, `Uint32Array`, etc.
- Per-column nulls: `Uint8Array`

**Issues:**

- Each TypedArray is a separate GC-managed object
- Memory fragmentation across heap
- 64KB minimum for WASM Memory means per-SpanBuffer WASM is wasteful

### Proposed Architecture

One `WebAssembly.Memory` per `OpContext`:

- All numeric columns for all spans from that OpContext share one memory
- Memory grows based on OpContext usage patterns
- Freelists enable memory reuse across traces
- String columns remain in JS (cold-path dictionary building)

## Design Principles

1. **WASM per OpContext, not per SpanBuffer** - Amortizes 64KB minimum across many spans
2. **Freelist per size class** - Enables O(1) allocation/deallocation without fragmentation
3. **Strings stay in JS** - Dictionary building and UTF-8 encoding are cold-path operations
4. **Remap only on read** - Library column IDs are local; remapping happens at Arrow conversion via existing
   `RemappedBufferView`
5. **Capacity-sized blocks** - All blocks in a freelist are identical size (capacity × element_size)

## Memory Layout

### OpContext WASM Memory

```
┌─────────────────────────────────────────────────────────────────┐
│ Header (192 bytes, 3 cache lines)                                │
│ - bump_ptr, span_id_counter, alloc/free counts                   │
│ - 28 tiered freelists (7 tiers × 4 size classes)                 │
│ - thread_id, freelist_identity                                   │
├─────────────────────────────────────────────────────────────────┤
│ Allocated Blocks                                                 │
│ ┌─────────────┬─────────────┬─────────────┬─────────────┐       │
│ │ Span System │ Span System │ 8B Column   │ 4B Column   │ ...   │
│ │ (Trace A)   │ (Trace B)   │ (Trace A)   │ (Trace A)   │       │
│ └─────────────┴─────────────┴─────────────┴─────────────┘       │
├─────────────────────────────────────────────────────────────────┤
│ Free Space (bump allocation area)                                │
│                                            ↑ bump_ptr            │
└─────────────────────────────────────────────────────────────────┘
```

### Header Structure (192 bytes, 3 cache lines)

The header uses tiered freelists for buddy allocation. Each size class has 7 tiers corresponding to capacities 8, 16,
32, 64, 128, 256, 512.

```
Offset  Size  Field
------  ----  -----
0       4     bump_ptr          // Next free byte for bump allocation
4       4     span_id_counter   // Global span ID counter (monotonically increasing)
8       4     alloc_count       // Total allocations (debug)
12      4     free_count        // Total frees (debug)
16      4     freelist_identity // Head of Identity block freelist (fixed size)
20      4     _pad0             // Alignment padding
24      8     thread_id         // Process/worker thread ID (set once from JS)
32      112   freelists[28]     // 7 tiers × 4 size classes = 28 freelists (112 bytes)
                                // Index = size_class * 7 + tier
                                // Size classes: 0=span_system, 1=col_1b, 2=col_4b, 3=col_8b
                                // Tiers: 0=cap8, 1=cap16, 2=cap32, 3=cap64, 4=cap128, 5=cap256, 6=cap512
144     1     thread_id_set     // 1 if thread_id has been set, 0 otherwise
145     47    _reserved         // Pad to 192 bytes (3 cache lines)
```

**Capacity per-call API**: Capacity is NOT stored in the header. Each `alloc_*` and `free_*` call takes capacity as a
parameter, allowing different OpContexts to share the same WASM memory with different capacities.

### Size Classes

Each column block contains **both null bitmap and values** - they're always allocated together:

| Size Class  | Block Size                        | Used For                                          |
| ----------- | --------------------------------- | ------------------------------------------------- |
| Span System | `capacity × 9`                    | timestamp (8B) + entry_type (1B) per row          |
| 1B Column   | `ceil(capacity/8) + capacity × 1` | Null bitmap + Uint8 values (enum, boolean)        |
| 4B Column   | `ceil(capacity/8) + capacity × 4` | Null bitmap + Uint32/Float32 values               |
| 8B Column   | `ceil(capacity/8) + capacity × 8` | Null bitmap + Float64/BigInt64 values             |
| Identity    | 128 bytes (fixed)                 | write_index, span_id, trace_id (for root buffers) |

All capacity-dependent blocks are sized to enable **buddy allocation** - when capacity halves, block size halves
exactly:

- `span_system(C) = C × 9` → `span_system(C/2) = C/2 × 9 = span_system(C) / 2` ✓
- `col_1b(C) ≈ C × 1.125` → scales linearly with capacity ✓
- `col_4b(C) ≈ C × 4.125` → scales linearly with capacity ✓
- `col_8b(C) ≈ C × 8.125` → scales linearly with capacity ✓

**Note**: `writeIndex` was moved from span_system to the identity block to make span_system size exactly `C × 9`,
enabling clean buddy splits.

Block layout (column blocks):

```
┌─────────────────────────────────────────────────────────┐
│ Null bitmap: ceil(capacity/8) bytes                     │
├─────────────────────────────────────────────────────────┤
│ Values: capacity × element_size bytes                   │
└─────────────────────────────────────────────────────────┘
```

### Identity Block Layout

Each span has an identity block (128 bytes, fixed size) that stores per-span metadata:

```
Offset  Size  Field
------  ----  -----
0       4     write_index     // Current write position (hot path, at offset 0 for fast access)
4       4     span_id         // Unique span ID (from global counter)
8       1     trace_id_len    // Length of trace_id (0 for child buffers)
9       119   trace_id        // UTF-8 trace_id bytes (only for root buffers)
                              // Total: 4 + 4 + 1 + 119 = 128 bytes
```

**Root buffers** store the trace_id in their identity block. **Child buffers** have `trace_id_len = 0` and inherit
trace_id from their parent via the `_parent` buffer reference.

**Thread ID** is stored globally in the header (not per-span) since all spans in a process/worker share the same thread
ID.

## Buddy Allocation

### Why Buddy Allocation?

When capacity changes (e.g., self-tuning adjusts from 64 to 32 rows), blocks in the freelist become the wrong size.
Buddy allocation allows:

1. **Splitting**: A capacity-64 block can split into two capacity-32 blocks
2. **Merging**: Two adjacent capacity-32 blocks can merge into one capacity-64 block

This eliminates memory waste when capacity changes and enables memory compaction.

### Tiered Freelists

The allocator maintains 28 freelists organized by size class and capacity tier:

- **7 capacity tiers**: 8, 16, 32, 64, 128, 256, 512 (tier index 0-6)
- **4 size classes**: span_system, col_1b, col_4b, col_8b
- **Total**: 7 × 4 = 28 freelists

Freelist index: `size_class * NUM_TIERS + tier`

### Buddy Relationship

For capacity `C`, blocks can split/merge with capacity `C/2`:

| Size Class  | C=64 | C=32 | C=16 | Relationship                |
| ----------- | ---- | ---- | ---- | --------------------------- |
| span_system | 576  | 288  | 144  | `size(C) = 2 × size(C/2)` ✓ |
| col_1b      | 72   | 36   | 18   | `size(C) = 2 × size(C/2)` ✓ |
| col_4b      | 264  | 132  | 66   | `size(C) = 2 × size(C/2)` ✓ |
| col_8b      | 520  | 260  | 130  | `size(C) = 2 × size(C/2)` ✓ |

### Split Operation (allocAtTier)

When allocating a block for capacity tier `T` but freelist is empty:

1. Recursively try to allocate from tier `T+1`
2. If tier `T+1` returns a block, split it:
   - First half returned to caller
   - Second half pushed to tier `T` freelist
3. If all tiers exhausted, bump allocate at current tier

### Merge Operation (freeAtTier) - Address-Based

When freeing a block at offset `O` with block size `B`:

1. **Check right neighbor** at `O + B`:
   - If found in freelist at same tier, remove it
   - Merge: freed block + right neighbor → parent at `O`
   - Recursively try to merge at tier `T+1`

2. **Check left neighbor** at `O - B`:
   - If found in freelist at same tier, remove it
   - Merge: left neighbor + freed block → parent at left neighbor's offset
   - Recursively try to merge at tier `T+1`

3. If no merge possible, push to freelist at tier `T`

### Address-Based Buddy Identification (Implemented)

Buddies are identified by address arithmetic, not stored pointers:

- **Right buddy** of block at offset `O`: `O + block_size`
- **Left buddy** of block at offset `O`: `O - block_size`

This works for ALL adjacent blocks, not just those from the same split. When freeing, we scan the freelist for neighbors
at `O ± block_size`. If found, we merge.

**O(n) freelist scan**: `findAndRemoveByOffset` scans the freelist to find a neighbor. Freelists are expected to be
short in practice (typically 0-10 blocks), so O(n) is acceptable.

### Cascading Merge Behavior

When blocks are allocated by splitting from the max tier (512), sibling blocks are left at every intermediate tier. When
freeing, merges cascade all the way back up:

```
Alloc(32) → bump alloc at tier 6 (512), split to:
  tier 5 (256): sibling pushed to freelist
  tier 4 (128): sibling pushed to freelist
  tier 3 (64):  sibling pushed to freelist
  tier 2 (32):  returned to caller

Free(32) → merge with tier-2 sibling → tier 3
         → merge with tier-3 sibling → tier 4
         → merge with tier-4 sibling → tier 5
         → merge with tier-5 sibling → tier 6 (max, stop)
```

The final merged block ends up at max tier (512).

## Freelist Implementation

### Zero-Overhead Freelists

The freelist uses the **block's own memory** to store the next pointer. When a block is free, its first 4 bytes hold the
next pointer. When a block is in use, those same bytes hold valid data (null bitmap). There is **no per-block
overhead**.

This works because:

- A block is either **on the freelist** (free) OR **held by a SpanBuffer** (in use), never both
- When free, the null bitmap / values are garbage anyway
- All blocks are ≥ 4 bytes (minimum: 1B column with capacity=4 → 1 + 4 = 5 bytes)

### Block Lifecycle Diagram

```
═══════════════════════════════════════════════════════════════════════════════
STEP 1: BUMP ALLOCATE (first time, freelist empty)
═══════════════════════════════════════════════════════════════════════════════

    freelist_1b: 0 (empty)

    WASM Memory:
    ┌─────────────────────────────────────┐
    │ Block at offset 100                 │
    │ [uninitialized...................]  │
    └─────────────────────────────────────┘

    SpanBuffer._columnOffsets[colId] = 100   ← Caller holds the reference

═══════════════════════════════════════════════════════════════════════════════
STEP 2: BLOCK IN USE (SpanBuffer writes data)
═══════════════════════════════════════════════════════════════════════════════

    freelist_1b: 0 (empty, block is NOT on freelist)

    WASM Memory:
    ┌─────────────────────────────────────┐
    │ Block at offset 100                 │
    │ [nulls: 0x03][values: 1,2,0,0,...]  │  ← Valid data
    └─────────────────────────────────────┘

    SpanBuffer._columnOffsets[colId] = 100   ← Caller still holds reference

═══════════════════════════════════════════════════════════════════════════════
STEP 3: TRACE COMPLETES - BLOCK FREED
═══════════════════════════════════════════════════════════════════════════════

    free(100, SIZE_CLASS_1B) is called

    freelist_1b: 100 ──────────────────┐
                                       │
    WASM Memory:                       │
    ┌──────────────────────────────────▼──┐
    │ Block at offset 100                 │
    │ [next: 0][garbage...............]   │  ← First 4 bytes = next ptr
    └─────────────────────────────────────┘

    SpanBuffer is gone (trace completed)

═══════════════════════════════════════════════════════════════════════════════
STEP 4: ANOTHER BLOCK FREED (offset 200)
═══════════════════════════════════════════════════════════════════════════════

    free(200, SIZE_CLASS_1B) is called - pushes onto front of list

    freelist_1b: 200 ──────────────────┐
                                       │
    WASM Memory:                       │
    ┌──────────────────────────────────▼──┐
    │ Block at offset 200                 │
    │ [next: 100][garbage.............]   │ ─┐
    ├─────────────────────────────────────┤  │
    │ Block at offset 100                 │  │
    │ [next: 0][garbage...............]   │◄─┘
    └─────────────────────────────────────┘

    Freelist chain: 200 → 100 → 0 (end)

═══════════════════════════════════════════════════════════════════════════════
STEP 5: ALLOCATE (new trace starts, pops from freelist)
═══════════════════════════════════════════════════════════════════════════════

    alloc(SIZE_CLASS_1B) returns 200, updates head to 100

    freelist_1b: 100 ──────────────────┐
                                       │
    WASM Memory:                       │
    ┌─────────────────────────────────────┐
    │ Block at offset 200                 │
    │ [nulls: 0x00][values: 0,0,0,...]    │  ← Now in use (will be written)
    ├──────────────────────────────────▼──┤
    │ Block at offset 100                 │
    │ [next: 0][garbage...............]   │  ← Still on freelist
    └─────────────────────────────────────┘

    SpanBuffer._columnOffsets[colId] = 200   ← New caller holds reference
```

### Key Insight

**A block is never on the freelist AND in use simultaneously.**

- **In use**: SpanBuffer.\_columnOffsets[colId] holds the offset. Freelist doesn't know about it.
- **Free**: Freelist head (or another free block's next pointer) holds the offset. No SpanBuffer references it.

The first 4 bytes serve dual purpose:

- When **free**: `next` pointer to chain free blocks, followed by cascading stats
- When **in use**: Start of null bitmap (overwritten with valid data)

### Minimum Capacity Requirement

For zero-overhead freelists to work, all blocks must be ≥ 4 bytes to hold the next pointer.

Smallest block is a 1B column: `ceil(capacity/8) + capacity × 1`

- capacity=4: `1 + 4 = 5 bytes` ✓
- capacity=3: `1 + 3 = 4 bytes` ✓ (exactly fits)
- capacity=2: `1 + 2 = 3 bytes` ✗ (too small!)

**Constraint: capacity ≥ 4** (which is reasonable - a span with fewer than 4 rows is unusual).

### Allocation Algorithm

```
fn alloc(size_class: SizeClass) -> u32:
    head = freelist_heads[size_class]

    if head != 0:
        // Pop from freelist
        freelist_heads[size_class] = memory[head]  // read next pointer
        return head

    // Freelist empty - bump allocate
    block_size = getBlockSize(size_class)
    ptr = header.bump_ptr
    header.bump_ptr += block_size

    // Grow memory if needed
    if header.bump_ptr > memory.size():
        pages_needed = ceil((header.bump_ptr - memory.size()) / 65536)
        memory.grow(pages_needed)

    return ptr
```

### Deallocation Algorithm

```
fn free(offset: u32, size_class: SizeClass):
    // Push onto freelist
    old_head = freelist_heads[size_class]
    memory[offset] = old_head  // write next pointer
    freelist_heads[size_class] = offset
```

### Freelist Statistics (Zero-Overhead)

Since freed blocks have unused space after the `next` pointer, we store cascading statistics that aggregate as blocks
are pushed/popped. This is the `FreeBlock` struct (20 bytes):

```
Freed Block Layout (FreeBlock, 20 bytes):
Offset  Size  Field
------  ----  -----
0       4     next_ptr            // Pointer to next free block (0 = end of list)
4       4     freelist_len        // Number of blocks in freelist (cascading sum)
8       4     reuse_count         // Times allocated from freelist (cascading sum)
12      4     split_count         // Times this size class was split from larger (cascading)
16      4     merge_count         // Times blocks merged to larger (cascading)
20+     ...   unused              // Rest of block unused when free
```

**Cascading Aggregation**: Each freed block carries forward the stats from the entire chain.

On **pushToFreelist(offset, is_merge)**:

```zig
block.next = old_head
if (old_head != 0) {
    block.freelist_len = old_head.freelist_len + 1
    block.reuse_count = old_head.reuse_count
    block.split_count = old_head.split_count
    block.merge_count = old_head.merge_count + (is_merge ? 1 : 0)
} else {
    block.freelist_len = 1
    block.reuse_count = 0
    block.split_count = 0
    block.merge_count = is_merge ? 1 : 0
}
HEAD = block
```

On **allocAtTier() from freelist**:

```zig
block = HEAD
HEAD = block.next
// Cascade stats to new head and increment reuse count
if (HEAD != 0) {
    HEAD.freelist_len = block.freelist_len - 1
    HEAD.reuse_count = block.reuse_count + 1
    HEAD.split_count = block.split_count
    HEAD.merge_count = block.merge_count
}
return block
```

**O(1) Stats Access**: Read directly from `HEAD`:

- `freelist_len`: `HEAD.freelist_len` — blocks available for reuse
- `reuse_count`: `HEAD.reuse_count` — times allocated from freelist
- `split_count`: `HEAD.split_count` — buddy splits performed
- `merge_count`: `HEAD.merge_count` — buddy merges performed

If freelist is empty (`HEAD == 0`), all stats return 0.

**Derived Metrics**:

- `bump_allocs = header.alloc_count - total_reuse_count`
- `reuse_ratio = total_reuse_count / header.alloc_count`
- `fragmentation_indicator`: high `freelist_len` with low `reuse_ratio` suggests fragmentation

### Freelist Growth

Freelists start empty and grow dynamically:

1. **First trace**: All allocations bump allocate
2. **Trace completes**: Blocks returned to freelists
3. **Second trace**: Allocations pop from freelists
4. **Usage spike**: If more blocks needed than in freelist, bump allocate more
5. **Steady state**: Freelists satisfy all allocations

This naturally adapts to the OpContext's usage pattern without pre-reservation.

## What WASM Manages

| Data                   | Storage     | Notes                                                   |
| ---------------------- | ----------- | ------------------------------------------------------- |
| Span System            | WASM block  | timestamp (8B × capacity) + entry_type (1B × capacity)  |
| Identity               | WASM block  | writeIndex, span_id, trace_id (144 bytes fixed)         |
| Numeric columns (lazy) | WASM block  | Null bitmap + values together, allocated on first write |
| Thread ID              | WASM header | Global for all spans (set once per worker/process)      |
| Span ID counter        | WASM header | Monotonically increasing, used to assign span_id        |

## What JS Manages

| Data                            | Storage      | Notes                                             |
| ------------------------------- | ------------ | ------------------------------------------------- |
| String columns (category, text) | `string[]`   | No nulls array - `undefined` = null               |
| SpanBuffer object               | JS heap      | Holds offsets, children, metadata                 |
| `_columnOffsets`                | `Int32Array` | Maps column ID → WASM offset (-1 = not allocated) |

### Why Strings Stay in JS

1. **Dictionary building is cold-path** - Happens at Arrow conversion, not hot-path writes
2. **UTF-8 encoding is cold-path** - Only needed for Arrow output
3. **No null bitmap needed** - `undefined` in `string[]` naturally represents null
4. **Avoids WASM string complexity** - No need for string table management

## SpanBuffer Changes

### Current SpanBuffer Properties

```typescript
// Per-column TypedArrays (current)
this._userId_values: Float64Array;
this._userId_nulls: Uint8Array;
```

### Proposed SpanBuffer Properties

```typescript
// Offset into OpContext's WASM memory
this._systemOffset: number;           // Points to Span System block
this._columnOffsets: Int32Array;      // Per-column: WASM offset or -1

// String columns remain as JS arrays
this._userId_values: string[];        // For category/text columns
```

### Write Path (Numeric Column)

```typescript
// Generated SpanLogger method for numeric column
latency(value: number) {
    const COL_ID = 3;  // Baked at codegen
    let offset = this._buffer._columnOffsets[COL_ID];

    if (offset < 0) {
        // Lazy allocation - single block contains nulls + values
        offset = this._opContextMemory.alloc8B();
        this._buffer._columnOffsets[COL_ID] = offset;
    }

    // Block layout: [nulls: ceil(cap/8) bytes][values: cap×8 bytes]
    const nullsSize = Math.ceil(this._buffer._capacity / 8);
    const idx = this._buffer._writeIndex;

    // Set null bit (nulls at start of block)
    const byteIdx = offset + (idx >>> 3);
    this._opContextMemory.u8View[byteIdx] |= (1 << (idx & 7));

    // Write value (values after nulls)
    const valuesOffset = offset + nullsSize;
    this._opContextMemory.f64View[(valuesOffset >>> 3) + idx] = value;

    return this;
}
```

### Write Path (String Column)

```typescript
// Generated SpanLogger method for category/text column
userId(value: string) {
    const COL_ID = 5;  // Baked at codegen

    // Lazy allocation (JS array)
    if (!this._buffer._stringCols[COL_ID]) {
        this._buffer._stringCols[COL_ID] = new Array(this._buffer._capacity);
    }

    // Write to JS array (undefined = null, no bitmap needed)
    this._buffer._stringCols[COL_ID][this._buffer._writeIndex] = value;

    return this;
}
```

## Library Integration (No Write-Time Remapping)

### Current Flow

Library defines ops with its own schema. When composed into app with `prefix()`:

- `RemappedSpanLogger` generated with prefixed column names baked in
- `RemappedBufferView` generated for Arrow conversion

### Proposed Flow

Same as current - **no change to remapping**:

1. **Library writes to its own columns** using library column IDs (0, 1, 2...)
2. **Library columns stored in library's OpContext WASM memory**
3. **At Arrow conversion**, `RemappedBufferView` maps prefixed names to library column names
4. **Remap is read-only** - happens at cold path, not write path

This works because each OpContext (library vs app) has its own WASM memory. Library code writes to library memory with
library column IDs. No cross-OpContext coordination needed at write time.

## Lifecycle

### Span Start

```
1. Alloc Span System block from freelist_span (or bump)
2. Initialize _systemOffset
3. Initialize _columnOffsets = Int32Array filled with -1
4. String columns: leave as undefined (lazy)
```

### Lazy Column Allocation

```
On first write to numeric column:
1. Alloc column block from appropriate size-class freelist
   (block contains both null bitmap and values)
2. Store offset in _columnOffsets
3. Write value and null bit
```

### Trace Completion

```
Walk span tree depth-first:
For each span:
  1. Return Span System block to freelist_span
  2. For each allocated numeric column:
     - Return column block to size-class freelist
  3. String arrays: left for GC (or pooled)
```

## Capacity Tuning Considerations

If OpContext capacity changes (e.g., 32 → 64):

- Old blocks in freelists are wrong size
- **Simple solution**: Discard old freelist contents (blocks become orphaned)
- Memory waste during tuning phase, but capacity stabilizes quickly
- Alternative: Maintain separate freelists per capacity (more complex)

## Concurrent Traces

Multiple traces can be in-flight within same OpContext (async/await):

```
Trace A: rows allocated at offsets [0, 256, 512]
Trace B: rows allocated at offsets [768, 1024]
Trace A resumes: writes to its pre-allocated offsets
Trace B resumes: writes to its pre-allocated offsets
```

Single-threaded JS ensures no race conditions. Each span has its own allocated blocks, writes are isolated.

## Memory Growth and Reclamation

### Growth

- WASM memory grows in 64KB pages via `memory.grow()`
- Bump allocator carves small blocks from pages
- Growth happens when freelists empty AND bump area exhausted

### Reclamation

- Blocks returned to freelists on trace completion
- Freelists enable reuse without compaction
- WASM memory never shrinks (spec limitation)
- Memory naturally stabilizes at peak usage level

### Long-Running Processes

For servers handling many requests:

- Freelists accumulate blocks matching usage patterns
- Peak memory = max concurrent traces × blocks per trace
- No unbounded growth if traces complete and return blocks

## Performance Characteristics

### Cache Locality

- All numeric data for an OpContext in contiguous WASM memory
- Span System blocks (timestamp + entry_type) accessed together
- Better L1/L2 cache utilization than scattered TypedArrays

### Allocation Cost

- Freelist pop: O(1) - read next pointer, update head
- Freelist push: O(1) - write next pointer, update head
- Bump allocate: O(1) - increment pointer, maybe grow memory
- No GC pressure for numeric data

### Write Cost

- Same as current: array index write
- One extra indirection: `_columnOffsets[colId]` lookup
- Offset cached after first write per column

## Implementation Status

**All phases complete and tested** - WASM memory architecture is fully integrated:

- ✅ Phase 1: WASM Allocator Module (Zig + TypeScript wrapper)
- ✅ Phase 2: OpContext Integration (WasmBufferStrategy)
- ✅ Phase 3: SpanBuffer Migration (WasmSpanBuffer with lazy allocation)
- ✅ Phase 4: SpanLogger Codegen (numeric/string setters)
- ✅ Phase 5: Trace Completion (block return to freelists)
- ✅ Phase 6: Benchmarking (comprehensive JS vs WASM benchmarks)

**Total implementation**: ~2,910 lines (Zig: 971 lines, TypeScript: 1,939 lines)

The WASM architecture is production-ready and works seamlessly with:

- `TestTracer`, `StdioTracer`, `ArrayQueueTracer` via `WasmBufferStrategy`
- `defineOpContext()` and the entire logging API (tag, log, ok, err, span)
- Arrow conversion via existing `convertSpanTreeToArrowTable()`
- Schema generation (S.enum, S.category, S.text, S.number, S.boolean)

### Phase 1: WASM Allocator Module ✅ COMPLETE

Implemented in Zig (`packages/lmao/src/lib/wasm/allocator.zig`):

- Header initialization: `init()`, `reset()`
- Tiered allocation: `alloc_span_system(capacity)`, `alloc_col_1b(capacity)`, `alloc_col_4b(capacity)`,
  `alloc_col_8b(capacity)`
- Tiered deallocation with buddy merge: `free_span_system(offset, capacity)`, `free_col_1b(offset, capacity)`, etc.
- Identity block allocation: `alloc_identity_root(trace_id_ptr, trace_id_len)`, `alloc_identity_child()`,
  `free_identity(offset)`
- Span lifecycle: `span_start()`, `span_end_ok()`, `span_end_err()`, `write_log_entry()`
- Column writes: `write_col_f64()`, `write_col_u32()`, `write_col_u8()`
- Thread ID management: `set_thread_id()`, `get_thread_id_high()`, `get_thread_id_low()`
- Freelist statistics: `get_freelist_len()`, `get_freelist_reuse_count()`, `get_freelist_split_count()`,
  `get_freelist_merge_count()`

TypeScript wrapper (`packages/lmao/src/lib/wasm/wasmAllocator.ts`):

- `createWasmAllocator(options)` - async factory
- `createWasmAllocatorSync(wasmModule, options)` - sync factory
- All WASM functions exposed with TypeScript types
- Capacity defaults to allocator's configured capacity but can be overridden per-call

### Phase 2: OpContext Integration ✅ COMPLETE

Implemented as `WasmBufferStrategy` (`packages/lmao/src/lib/wasm/WasmBufferStrategy.ts`):

- `WasmBufferStrategy` owns `WebAssembly.Memory` + allocator instance
- Exposes typed views: `u8`, `u32`, `f64`, `i64` (cached, recreated after grow)
- Async factory `WasmBufferStrategy.create(options)` with allocator reuse support
- Implements `BufferStrategy` interface for full Tracer compatibility
- Handles memory growth via allocator's bump pointer mechanism

### Phase 3: SpanBuffer Migration ✅ COMPLETE

Implemented in `WasmSpanBuffer` (`packages/lmao/src/lib/wasm/wasmSpanBuffer.ts`, 942 lines):

- `WasmSpanBufferInstance` extends `AnySpanBuffer` for full API compatibility
- Per-column storage replaced with `_columnPtrs: Int32Array` (WASM offsets, -1 = not allocated)
- String columns remain as `string[]` in JavaScript arrays (`undefined` = null)
- Factory functions: `createWasmSpanBuffer()`, `createWasmChildSpanBuffer()`, `createWasmOverflowBuffer()`
- Runtime class generation with schema-specific column methods
- Identity block allocation (root stores trace_id, child inherits from parent)
- System columns stored in WASM via `_systemPtr`
- Full test coverage in `wasmSpanBuffer.test.ts` (52 tests)

### Phase 4: SpanLogger Codegen ✅ COMPLETE

Code generation in `wasmSpanBuffer.ts`:

- `generateNumericSetter()` - generates column writers that:
  - Lazy allocate columns from size-class freelist on first write
  - Write values directly to WASM memory at column offset
  - Set null bits in null bitmap (at start of column block)
  - Call cached WASM exports for hot-path writes (`write_col_f64`, `write_col_u32`, `write_col_u8`)
- `generateStringSetter()` - generates string writers to JS arrays
- Column methods generated at class construction via `new Function()`
- Fluent API preserved (all methods return `this`)
- Supports both numeric (float64, uint32, uint8) and string (category, text) column types

### Phase 5: Trace Completion ✅ COMPLETE

Implemented in `WasmBufferStrategy` (`packages/lmao/src/lib/wasm/WasmBufferStrategy.ts`):

- `releaseBuffer(buf)` - called when trace completes
- `releaseSpanTree(buf)` - recursively walks span tree depth-first
- Returns Span System blocks to freelist via `freeSpanSystem()`
- Returns each allocated numeric block to its size-class freelist (1B, 4B, 8B)
- Returns identity blocks to identity freelist
- String arrays left for JavaScript GC
- Freelist merges enabled by address-based buddy allocation
- Supports capacity tuning by discarding orphaned blocks (simple, effective)

### Phase 6: Benchmarking ✅ COMPLETE

Implemented in `benchmarks/js-vs-wasm.bench.ts`:

- Comprehensive benchmarks comparing JS (`JsBufferStrategy`) vs WASM (`WasmBufferStrategy`)
- Cold start scenarios: simple trace, trace with tags, multiple log entries
- Warm/steady-state scenarios: simple trace, tags, nested spans, multiple entries, memory reuse
- Uses `mitata` benchmarking framework
- Measures throughput (ops/second) for realistic workloads
- Integration tests in `wasm-integration.test.ts` (full end-to-end test suite)
- `WasmTracer` disabled tests available (wasmTracer.test.ts.disabled) for future work

## Open Questions (Resolved/Deferred)

1. ~~**Capacity tuning**: Accept orphaned blocks, or track blocks by capacity?~~ ✅ **RESOLVED**: Accept orphaned blocks
   during capacity tuning - simple, effective, capacity stabilizes quickly in practice.

2. **String column pooling**: Should we pool `string[]` arrays too? 🚧 **DEFERRED**: Not implemented. String arrays left
   for GC. Could be future optimization but not critical since strings are cold-path.

3. ~~**View invalidation**: How to handle `memory.grow()` invalidating TypedArray views?~~ ✅ **RESOLVED**: Views cached
   in `WasmAllocator` but allocator grows memory conservatively for benchmarks. Memory grows only when freelists empty
   and bump area exhausted.

4. ~~**Overflow buffers**: Do overflow buffers allocate from same WASM memory?~~ ✅ **RESOLVED**: Yes -
   `createWasmOverflowBuffer()` allocates from the same allocator, enabling memory reuse across overflow buffers.

## Future Optimizations (Not Currently Implemented)

- **String array pooling**: Pool `string[]` arrays to reduce GC pressure in long-running processes
- **Per-capacity freelists**: Track blocks by capacity to avoid orphaning during self-tuning (more complex)
- **Hot path optimizations**: Further inline WASM exports, elide bounds checking via capacity constraints

## References

- [01b_columnar_buffer_architecture.md](./01b_columnar_buffer_architecture.md) - Current buffer architecture
- [01b6_buffer_codegen_extension.md](./01b6_buffer_codegen_extension.md) - Buffer code generation
- [01e_library_integration_pattern.md](./01e_library_integration_pattern.md) - RemappedBufferView pattern
- [01l_op_context_pattern.md](./01l_op_context_pattern.md) - OpContext and defineOpContext
