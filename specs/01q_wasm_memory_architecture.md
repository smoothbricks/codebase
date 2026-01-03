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
│ Header (64 bytes, cache-aligned)                                 │
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

### Header Structure

```
Offset  Size  Field
------  ----  -----
0       4     bump_ptr          // Next free byte for bump allocation
4       4     capacity          // Block sizing (rows per span)
8       4     freelist_span     // Head of Span System freelist
12      4     freelist_1b       // Head of 1-byte column freelist (nulls + values)
16      4     freelist_4b       // Head of 4-byte column freelist (nulls + values)
20      4     freelist_8b       // Head of 8-byte column freelist (nulls + values)
24      40    reserved          // Pad to 64 bytes (cache line)
```

### Size Classes

Each column block contains **both null bitmap and values** - they're always allocated together:

| Size Class  | Block Size                        | Used For                                                |
| ----------- | --------------------------------- | ------------------------------------------------------- |
| Span System | `capacity × 9 + identity_size`    | timestamp (8B) + entry_type (1B) per row, plus identity |
| 1B Column   | `ceil(capacity/8) + capacity × 1` | Null bitmap + Uint8 values (enum, boolean)              |
| 4B Column   | `ceil(capacity/8) + capacity × 4` | Null bitmap + Uint32/Float32 values                     |
| 8B Column   | `ceil(capacity/8) + capacity × 8` | Null bitmap + Float64/BigInt64 values                   |

All blocks within a size class are **identical size** because capacity is fixed per OpContext.

Block layout:

```
┌─────────────────────────────────────────────────────────┐
│ Null bitmap: ceil(capacity/8) bytes                     │
├─────────────────────────────────────────────────────────┤
│ Values: capacity × element_size bytes                   │
└─────────────────────────────────────────────────────────┘
```

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

- When **free**: `next` pointer to chain free blocks
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

### Freelist Growth

Freelists start empty and grow dynamically:

1. **First trace**: All allocations bump allocate
2. **Trace completes**: Blocks returned to freelists
3. **Second trace**: Allocations pop from freelists
4. **Usage spike**: If more blocks needed than in freelist, bump allocate more
5. **Steady state**: Freelists satisfy all allocations

This naturally adapts to the OpContext's usage pattern without pre-reservation.

## What WASM Manages

| Data                   | Storage    | Notes                                                   |
| ---------------------- | ---------- | ------------------------------------------------------- |
| Span System            | WASM block | timestamp + entry_type + identity per span              |
| Numeric columns (lazy) | WASM block | Null bitmap + values together, allocated on first write |

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

## Implementation Phases

### Phase 1: WASM Allocator Module

Implement in Zig:

- Header initialization
- `allocSpanSystem()`, `alloc1B()`, `alloc4B()`, `alloc8B()` (each includes nulls + values)
- `freeSpanSystem()`, `free1B()`, `free4B()`, `free8B()`
- `init(capacity)`, `reset()`

### Phase 2: OpContext Integration

- OpContext owns `WebAssembly.Memory` + allocator instance
- Expose typed views: `u8View`, `u32View`, `f64View`, `i64View`
- Handle memory growth (recreate views)

### Phase 3: SpanBuffer Migration

- Replace per-column TypedArrays with WASM offsets
- Keep string columns as JS arrays
- Update `createSpanBuffer`, `createChildSpanBuffer`

### Phase 4: SpanLogger Codegen

- Generate numeric writers that use WASM offsets
- Generate string writers that use JS arrays
- Maintain fluent API (return `this`)

### Phase 5: Trace Completion

- Implement block return to freelists
- Walk span tree, collect all allocated blocks
- Return in reverse allocation order (optional optimization)

### Phase 6: Benchmarking

- Compare write throughput: current vs WASM
- Measure memory usage patterns
- Profile cache behavior with perf/cachegrind

## Open Questions

1. **Capacity tuning**: Accept orphaned blocks, or track blocks by capacity?
2. **String column pooling**: Should we pool `string[]` arrays too?
3. **View invalidation**: How to handle `memory.grow()` invalidating TypedArray views?
4. **Overflow buffers**: Do overflow buffers allocate from same WASM memory?

## References

- [01b_columnar_buffer_architecture.md](./01b_columnar_buffer_architecture.md) - Current buffer architecture
- [01b6_buffer_codegen_extension.md](./01b6_buffer_codegen_extension.md) - Buffer code generation
- [01e_library_integration_pattern.md](./01e_library_integration_pattern.md) - RemappedBufferView pattern
- [01l_op_context_pattern.md](./01l_op_context_pattern.md) - OpContext and defineOpContext
