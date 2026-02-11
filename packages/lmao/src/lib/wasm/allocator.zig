// =============================================================================
// Freelist Allocator for SpanBuffer Storage
// =============================================================================
//
// Implements spec 01q: WASM Memory Architecture for SpanBuffer Storage
//
// Memory Layout:
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │ Header (64 bytes, cache-aligned)                                            │
// ├─────────────────────────────────────────────────────────────────────────────┤
// │ Allocated Blocks (spans + columns)                                          │
// ├─────────────────────────────────────────────────────────────────────────────┤
// │ Free Space (bump allocation area) ↑ bump_ptr                                │
// └─────────────────────────────────────────────────────────────────────────────┘
//
// Size Classes:
// - Span System:  capacity × 9 bytes (timestamp + entry_type)
// - Identity:     128 bytes (writeIndex + span_id + trace_id)
// - 1B Column:    ceil(capacity/8) + capacity × 1 (nulls + uint8/bool values)
// - 4B Column:    ceil(capacity/8) + capacity × 4 (nulls + uint32/float32 values)
// - 8B Column:    ceil(capacity/8) + capacity × 8 (nulls + float64/bigint64 values)
//
// Freelist: Zero-overhead, uses block's memory for next pointer + cascading stats when free.
//

const std = @import("std");

// =============================================================================
// Packed Structs for Memory Layout
// =============================================================================

// =============================================================================
// Capacity Tiers for Buddy Allocation
// =============================================================================
// Tiers: 8, 16, 32, 64, 128, 256, 512 (7 tiers, index 0-6)
// Buddy relationship: tier[i] can split into 2× tier[i-1], merge into tier[i+1]

const MIN_CAPACITY: u32 = 8;
const MAX_CAPACITY: u32 = 512;
const NUM_TIERS: usize = 7; // log2(512/8) + 1 = 7
const NUM_SIZE_CLASSES: usize = 4;
const NUM_FREELISTS: usize = NUM_SIZE_CLASSES * NUM_TIERS; // 28

/// Convert capacity to tier index (0=8, 1=16, 2=32, 3=64, 4=128, 5=256, 6=512)
inline fn capacityToTier(capacity: u32) usize {
    // capacity must be power of 2 between 8 and 512
    return @ctz(capacity) - @ctz(MIN_CAPACITY);
}

/// Convert tier index to capacity
inline fn tierToCapacity(tier: usize) u32 {
    return MIN_CAPACITY << @intCast(tier);
}

/// Header at offset 0 of WASM memory (192 bytes, 3 cache lines)
/// Uses extern struct for stable ABI layout
/// Hot fields in first cache line (64 bytes)
const Header = extern struct {
    // === First cache line (hot) ===
    bump_ptr: u32, // offset 0: Next free byte for bump allocation
    span_id_counter: u32, // offset 4: Global span ID counter
    alloc_count: u32, // offset 8: Total allocations
    free_count: u32, // offset 12: Total frees
    freelist_identity: u32, // offset 16: Identity freelist head
    _pad0: u32, // offset 20: Align thread_id to 8 bytes
    thread_id: u64, // offset 24: Process/worker thread ID

    // Freelists start at offset 32 (inside first cache line)
    // 4 size classes × 7 tiers = 28 freelists (112 bytes)
    // Index: size_class * NUM_TIERS + tier
    // Size classes: 0=span_system, 1=col_1b, 2=col_4b, 3=col_8b
    freelists: [NUM_FREELISTS]u32, // offset 32-143

    // === Cold fields ===
    thread_id_set: u8, // offset 144: 1 if thread_id has been set

    // Padding to 192 bytes (192 - 145 = 47 bytes)
    _reserved: [47]u8,

    comptime {
        std.debug.assert(@sizeOf(Header) == 192);
    }
};

/// Identity block (128 bytes fixed) - per-span metadata
/// Uses extern struct for stable ABI layout with array field
const Identity = extern struct {
    write_index: u32, // Current write position (hot path, at offset 0)
    span_id: u32, // Unique span ID (from global counter)
    trace_id_len: u8, // Length of trace_id (0 for child buffers)
    trace_id: [119]u8, // UTF-8 trace_id bytes (only for root buffers)

    comptime {
        std.debug.assert(@sizeOf(Identity) == 128);
    }
};

/// TraceRoot timing data (16 bytes)
/// Uses extern struct for stable ABI layout
const TraceRoot = extern struct {
    wall_clock_nanos: i64, // Wall clock start time in nanoseconds
    monotonic_ms: f64, // Monotonic start time in milliseconds

    comptime {
        std.debug.assert(@sizeOf(TraceRoot) == 16);
    }
};

/// Freelist block header - overlays the first 20 bytes of any freed block
/// Stores cascading statistics that aggregate as blocks are pushed/popped
/// Uses extern struct for stable ABI layout
const FreeBlock = extern struct {
    next_ptr: u32, // Pointer to next free block (0 = end of list)
    freelist_len: u32, // Number of blocks in freelist (cascading sum)
    reuse_count: u32, // Times allocated from freelist (cascading sum)
    split_count: u32, // Times this size class was split (cascading)
    merge_count: u32, // Times blocks merged (cascading)

    comptime {
        std.debug.assert(@sizeOf(FreeBlock) == 20);
    }
};

// =============================================================================
// Size Classes
// =============================================================================

const SizeClass = enum(u8) {
    span_system = 0,
    col_1b = 1,
    col_4b = 2,
    col_8b = 3,
};

// Identity is separate (fixed size, not capacity-dependent)
const IDENTITY_SIZE_CLASS: u8 = 4;

// =============================================================================
// Entry Type Constants (per specs/01h and systemSchema.ts)
// =============================================================================

const ENTRY_TYPE_SPAN_START: u8 = 1;
const ENTRY_TYPE_SPAN_OK: u8 = 2;
const ENTRY_TYPE_SPAN_ERR: u8 = 3;
const ENTRY_TYPE_SPAN_EXCEPTION: u8 = 4;

// No global capacity state - capacity is passed to each alloc/free call.
// This allows different OpContexts to have different capacities sharing the same WASM memory.

// =============================================================================
// Memory Access Helpers
// =============================================================================

/// Get typed pointer to WASM memory at offset.
/// Uses allowzero because WASM memory starts at address 0.
inline fn ptrAt(comptime T: type, offset: u32) *allowzero T {
    return @ptrFromInt(offset);
}

/// Get byte slice pointer at offset
inline fn bytesAt(offset: u32) [*]allowzero u8 {
    return @ptrFromInt(offset);
}

/// Get the global header at offset 0
inline fn header() *allowzero Header {
    return ptrAt(Header, 0);
}

// =============================================================================
// Size Class Helpers
// =============================================================================

/// Get freelist index for a size class and capacity tier
inline fn freelistIndex(sc: SizeClass, tier: usize) usize {
    return @intFromEnum(sc) * NUM_TIERS + tier;
}

/// Get freelist head for a size class at specific tier
inline fn freelistHeadAtTier(sc: SizeClass, tier: usize) u32 {
    return header().freelists[freelistIndex(sc, tier)];
}

/// Set freelist head for a size class at specific tier
inline fn setFreelistHeadAtTier(sc: SizeClass, tier: usize, value: u32) void {
    header().freelists[freelistIndex(sc, tier)] = value;
}

/// Get block size for a size class at a given capacity
inline fn blockSizeForCapacity(sc: SizeClass, capacity: u32) u32 {
    const null_bitmap_size = (capacity + 7) >> 3;
    return switch (sc) {
        .span_system => capacity * 9,
        .col_1b => null_bitmap_size + capacity,
        .col_4b => null_bitmap_size + capacity * 4,
        .col_8b => null_bitmap_size + capacity * 8,
    };
}

// =============================================================================
// Allocator Core
// =============================================================================

/// Allocate a block from the given size class at specific tier.
fn allocAtTier(sc: SizeClass, tier: usize) u32 {
    const h = header();
    const head_offset = freelistHeadAtTier(sc, tier);

    if (head_offset != 0) {
        // Pop from freelist
        const free_block = ptrAt(FreeBlock, head_offset);
        const next = free_block.next_ptr;
        setFreelistHeadAtTier(sc, tier, next);

        // Cascade stats forward: copy from popped block to new head, then increment
        // This ensures the new head has the aggregate stats from the entire chain
        if (next != 0) {
            const next_block = ptrAt(FreeBlock, next);
            next_block.freelist_len = free_block.freelist_len - 1;
            next_block.reuse_count = free_block.reuse_count + 1;
            next_block.split_count = free_block.split_count;
            next_block.merge_count = free_block.merge_count;
        }

        h.alloc_count += 1;
        return head_offset;
    }

    // Freelist empty - try buddy split from larger tier
    if (tier + 1 < NUM_TIERS) {
        const parent_offset = allocAtTier(sc, tier + 1);
        if (parent_offset != 0) {
            // Split parent into two blocks of current tier size
            const child_size = blockSizeForCapacity(sc, tierToCapacity(tier));
            const second_child = parent_offset + child_size;

            // Push second child to freelist (no merge attempt - it's a fresh split)
            pushToFreelist(second_child, sc, tier, false);

            // Update split count in the freelist head
            const new_head = freelistHeadAtTier(sc, tier);
            if (new_head != 0) {
                ptrAt(FreeBlock, new_head).split_count += 1;
            }

            // Return first child
            return parent_offset;
        }
    }

    // No buddy available - bump allocate
    const size = blockSizeForCapacity(sc, tierToCapacity(tier));
    const current_bump = h.bump_ptr;

    // Align to 8 bytes
    const aligned = (current_bump + 7) & ~@as(u32, 7);
    const new_bump = aligned + size;

    // Grow memory if needed
    const current_size = @wasmMemorySize(0) * 65536; // pages * 64KB
    if (new_bump > current_size) {
        const needed = new_bump - current_size;
        const pages_needed = (needed + 65535) / 65536; // ceil division
        const result = @wasmMemoryGrow(0, pages_needed);
        if (result == -1) {
            return 0; // Out of memory
        }
    }

    h.bump_ptr = new_bump;
    h.alloc_count += 1;

    return aligned;
}

/// Free a block back to its size class freelist at specific tier.
/// Attempts to merge with adjacent neighbor blocks (address-based buddy merge).
fn freeAtTier(offset: u32, sc: SizeClass, tier: usize) void {
    // Try to merge with adjacent blocks if not at max tier
    if (tier + 1 < NUM_TIERS) {
        const block_size = blockSizeForCapacity(sc, tierToCapacity(tier));

        // Check right neighbor (block at offset + block_size)
        const right_neighbor = offset + block_size;
        if (findAndRemoveByOffset(sc, tier, right_neighbor)) {
            // Merge: this block + right neighbor → parent at offset
            freeAtTierWithMerge(offset, sc, tier + 1);
            return;
        }

        // Check left neighbor (block at offset - block_size)
        if (offset >= block_size) {
            const left_neighbor = offset - block_size;
            if (findAndRemoveByOffset(sc, tier, left_neighbor)) {
                // Merge: left neighbor + this block → parent at left_neighbor
                freeAtTierWithMerge(left_neighbor, sc, tier + 1);
                return;
            }
        }
    }

    // No merge possible - just push to freelist
    pushToFreelist(offset, sc, tier, false);
}

/// Free a block after a merge (increments merge count).
fn freeAtTierWithMerge(offset: u32, sc: SizeClass, tier: usize) void {
    // Try to cascade merge at higher tier
    if (tier + 1 < NUM_TIERS) {
        const block_size = blockSizeForCapacity(sc, tierToCapacity(tier));

        // Check right neighbor
        const right_neighbor = offset + block_size;
        if (findAndRemoveByOffset(sc, tier, right_neighbor)) {
            freeAtTierWithMerge(offset, sc, tier + 1);
            return;
        }

        // Check left neighbor
        if (offset >= block_size) {
            const left_neighbor = offset - block_size;
            if (findAndRemoveByOffset(sc, tier, left_neighbor)) {
                freeAtTierWithMerge(left_neighbor, sc, tier + 1);
                return;
            }
        }
    }

    // No further merge - push with merge count increment
    pushToFreelist(offset, sc, tier, true);
}

/// Push a block onto the freelist with cascading stats.
fn pushToFreelist(offset: u32, sc: SizeClass, tier: usize, is_merge: bool) void {
    const h = header();
    const old_head = freelistHeadAtTier(sc, tier);
    const free_block = ptrAt(FreeBlock, offset);

    // Push onto freelist
    free_block.next_ptr = old_head;

    // Cascading stats: aggregate from old head (if any) + delta
    if (old_head != 0) {
        const old_block = ptrAt(FreeBlock, old_head);
        free_block.freelist_len = old_block.freelist_len + 1;
        free_block.reuse_count = old_block.reuse_count;
        free_block.split_count = old_block.split_count;
        free_block.merge_count = old_block.merge_count + @as(u32, if (is_merge) 1 else 0);
    } else {
        free_block.freelist_len = 1;
        free_block.reuse_count = 0;
        free_block.split_count = 0;
        free_block.merge_count = if (is_merge) 1 else 0;
    }

    setFreelistHeadAtTier(sc, tier, offset);
    h.free_count += 1;
}

/// Find and remove a block at a specific offset from the freelist.
/// Returns true if found and removed, false otherwise.
/// O(n) scan - freelists are expected to be short in practice.
fn findAndRemoveByOffset(sc: SizeClass, tier: usize, target_offset: u32) bool {
    const head_offset = freelistHeadAtTier(sc, tier);
    if (head_offset == 0) return false;

    // Check if HEAD is the target
    if (head_offset == target_offset) {
        const head_block = ptrAt(FreeBlock, head_offset);
        setFreelistHeadAtTier(sc, tier, head_block.next_ptr);

        // Cascade stats to new head
        if (head_block.next_ptr != 0) {
            const new_head = ptrAt(FreeBlock, head_block.next_ptr);
            new_head.freelist_len = head_block.freelist_len - 1;
            new_head.reuse_count = head_block.reuse_count;
            new_head.split_count = head_block.split_count;
            new_head.merge_count = head_block.merge_count;
        }
        return true;
    }

    // Scan freelist for target
    var prev_offset = head_offset;
    var current_offset = ptrAt(FreeBlock, head_offset).next_ptr;

    while (current_offset != 0) {
        if (current_offset == target_offset) {
            // Found target - remove from list
            const prev_block = ptrAt(FreeBlock, prev_offset);
            const current_block = ptrAt(FreeBlock, current_offset);
            prev_block.next_ptr = current_block.next_ptr;

            // Update freelist length in head
            const head_block = ptrAt(FreeBlock, freelistHeadAtTier(sc, tier));
            head_block.freelist_len -= 1;

            return true;
        }
        prev_offset = current_offset;
        current_offset = ptrAt(FreeBlock, current_offset).next_ptr;
    }

    return false;
}

// =============================================================================
// Debug exports (temporary for troubleshooting buddy merge)
// =============================================================================

/// Debug: get freelist head offset for a size class at given capacity
export fn debug_get_freelist_head(size_class: u8, capacity: u32) u32 {
    const sc: SizeClass = @enumFromInt(size_class);
    const tier = capacityToTier(capacity);
    return freelistHeadAtTier(sc, tier);
}

/// Debug: read next_ptr from a FreeBlock at given offset
export fn debug_read_next_ptr(offset: u32) u32 {
    if (offset == 0) return 0;
    return ptrAt(FreeBlock, offset).next_ptr;
}

// =============================================================================
// JS Imports (for timestamp calculation)
// =============================================================================

extern "env" fn performanceNow() f64;
extern "env" fn dateNow() f64;

// =============================================================================
// Timestamp Calculation
// =============================================================================

inline fn getTimestampNanos(trace_root_ptr: u32) i64 {
    const root = ptrAt(TraceRoot, trace_root_ptr);
    const current_ms = performanceNow();
    const elapsed_ms = current_ms - root.monotonic_ms;
    const elapsed_nanos: i64 = @intFromFloat(elapsed_ms * 1_000_000.0);
    return root.wall_clock_nanos + elapsed_nanos;
}

// =============================================================================
// Exported Functions - Initialization
// =============================================================================

/// Initialize the allocator header. Call once before first allocation.
/// Idempotent - subsequent calls are no-ops if already initialized.
export fn init() void {
    const h = header();
    const is_first_init = h.bump_ptr == 0;

    // Only initialize header on first call
    if (is_first_init) {
        h.bump_ptr = @sizeOf(Header);
        h.alloc_count = 0;
        h.free_count = 0;
        h.span_id_counter = 0;

        // Clear all freelists (28 capacity-tiered + 1 identity)
        for (&h.freelists) |*fl| {
            fl.* = 0;
        }
        h.freelist_identity = 0;
    }
    // Don't reset thread_id - it persists across init calls
}

/// Reset all freelists (for testing/benchmarking).
/// WARNING: This leaks all allocated blocks. Use free() for proper cleanup.
export fn reset() void {
    const h = header();
    h.bump_ptr = @sizeOf(Header);
    h.alloc_count = 0;
    h.free_count = 0;
    h.span_id_counter = 0;

    // Clear all freelists
    for (&h.freelists) |*fl| {
        fl.* = 0;
    }
    h.freelist_identity = 0;
    // Don't reset thread_id - it persists across reset calls
}

// =============================================================================
// Exported Functions - Debug/Stats
// =============================================================================

export fn get_bump_ptr() u32 {
    return header().bump_ptr;
}

export fn get_alloc_count() u32 {
    return header().alloc_count;
}

export fn get_free_count() u32 {
    return header().free_count;
}

/// Get span system block size for given capacity.
export fn get_span_system_size(capacity: u32) u32 {
    return blockSizeForCapacity(.span_system, capacity);
}

/// Get 1-byte column block size for given capacity.
export fn get_col_1b_size(capacity: u32) u32 {
    return blockSizeForCapacity(.col_1b, capacity);
}

/// Get 4-byte column block size for given capacity.
export fn get_col_4b_size(capacity: u32) u32 {
    return blockSizeForCapacity(.col_4b, capacity);
}

/// Get 8-byte column block size for given capacity.
export fn get_col_8b_size(capacity: u32) u32 {
    return blockSizeForCapacity(.col_8b, capacity);
}

// =============================================================================
// Exported Functions - Thread ID
// =============================================================================

/// Set the thread ID (called once from JS on module init).
export fn set_thread_id(high: u32, low: u32) void {
    const h = header();
    h.thread_id = (@as(u64, high) << 32) | @as(u64, low);
    h.thread_id_set = 1;
}

export fn get_thread_id_high() u32 {
    return @truncate(header().thread_id >> 32);
}

export fn get_thread_id_low() u32 {
    return @truncate(header().thread_id);
}

export fn is_thread_id_set() u8 {
    return header().thread_id_set;
}

export fn get_span_id_counter() u32 {
    return header().span_id_counter;
}

// =============================================================================
// Exported Functions - Freelist Statistics
// =============================================================================

/// Get freelist length for a size class at given capacity (O(1) - read from HEAD)
export fn get_freelist_len(size_class: u8, capacity: u32) u32 {
    const sc: SizeClass = @enumFromInt(size_class);
    const tier = capacityToTier(capacity);
    const head_offset = freelistHeadAtTier(sc, tier);
    if (head_offset == 0) return 0;
    return ptrAt(FreeBlock, head_offset).freelist_len;
}

/// Get total reuse count for a size class at given capacity (O(1) - read from HEAD)
export fn get_freelist_reuse_count(size_class: u8, capacity: u32) u32 {
    const sc: SizeClass = @enumFromInt(size_class);
    const tier = capacityToTier(capacity);
    const head_offset = freelistHeadAtTier(sc, tier);
    if (head_offset == 0) return 0;
    return ptrAt(FreeBlock, head_offset).reuse_count;
}

/// Get split count for a size class at given capacity (O(1) - read from HEAD)
export fn get_freelist_split_count(size_class: u8, capacity: u32) u32 {
    const sc: SizeClass = @enumFromInt(size_class);
    const tier = capacityToTier(capacity);
    const head_offset = freelistHeadAtTier(sc, tier);
    if (head_offset == 0) return 0;
    return ptrAt(FreeBlock, head_offset).split_count;
}

/// Get merge count for a size class at given capacity (O(1) - read from HEAD)
export fn get_freelist_merge_count(size_class: u8, capacity: u32) u32 {
    const sc: SizeClass = @enumFromInt(size_class);
    const tier = capacityToTier(capacity);
    const head_offset = freelistHeadAtTier(sc, tier);
    if (head_offset == 0) return 0;
    return ptrAt(FreeBlock, head_offset).merge_count;
}

// =============================================================================
// Exported Functions - Identity Block Operations
// =============================================================================

/// Allocate an identity block (fixed 128 bytes, not capacity-tiered)
fn allocIdentity() u32 {
    const h = header();
    const head_offset = h.freelist_identity;

    if (head_offset != 0) {
        // Pop from freelist
        const free_block = ptrAt(FreeBlock, head_offset);
        h.freelist_identity = free_block.next_ptr;

        if (free_block.next_ptr != 0) {
            ptrAt(FreeBlock, free_block.next_ptr).reuse_count += 1;
        }

        h.alloc_count += 1;
        return head_offset;
    }

    // Bump allocate
    const current_bump = h.bump_ptr;
    const aligned = (current_bump + 7) & ~@as(u32, 7);
    const new_bump = aligned + @sizeOf(Identity);

    h.bump_ptr = new_bump;
    h.alloc_count += 1;

    return aligned;
}

/// Free an identity block
fn freeIdentity(offset: u32) void {
    const h = header();
    const old_head = h.freelist_identity;
    const free_block = ptrAt(FreeBlock, offset);

    free_block.next_ptr = old_head;

    if (old_head != 0) {
        const old_block = ptrAt(FreeBlock, old_head);
        free_block.freelist_len = old_block.freelist_len + 1;
        free_block.reuse_count = old_block.reuse_count;
        free_block.split_count = old_block.split_count;
        free_block.merge_count = old_block.merge_count;
    } else {
        free_block.freelist_len = 1;
        free_block.reuse_count = 0;
        free_block.split_count = 0;
        free_block.merge_count = 0;
    }

    h.freelist_identity = offset;
    h.free_count += 1;
}

/// Allocate identity block and provide offset where JS should write trace_id bytes.
/// Returns packed u64: (identity_offset << 32) | trace_id_field_offset
/// JS can write bytes directly to memory[trace_id_field_offset..trace_id_field_offset+len]
export fn alloc_identity_root_for_js_write(trace_id_len: u32) u64 {
    const max_len = @sizeOf(@TypeOf(@as(Identity, undefined).trace_id));
    if (trace_id_len > max_len) {
        return 0; // trace_id too long
    }

    const offset = allocIdentity();
    const identity = ptrAt(Identity, offset);

    // Initialize identity (same as alloc_identity_root)
    const h = header();
    h.span_id_counter += 1;
    identity.span_id = h.span_id_counter;
    identity.write_index = 0;
    identity.trace_id_len = @truncate(trace_id_len);

    // Calculate absolute byte offset to trace_id field
    // Cast to u32 explicitly to ensure proper type handling
    const field_offset_in_struct: u32 = @offsetOf(Identity, "trace_id");
    const trace_id_field_offset: u32 = offset + field_offset_in_struct;

    // Pack both offsets into u64: upper 32 bits = identity offset, lower 32 bits = trace_id field offset
    return (@as(u64, offset) << 32) | @as(u64, trace_id_field_offset);
}

/// Allocate and initialize a CHILD identity block.
export fn alloc_identity_child() u32 {
    const offset = allocIdentity();
    const identity = ptrAt(Identity, offset);

    const h = header();
    h.span_id_counter += 1;
    identity.span_id = h.span_id_counter;
    identity.write_index = 0;
    identity.trace_id_len = 0; // Child uses parent's trace_id

    return offset;
}

export fn free_identity(offset: u32) void {
    freeIdentity(offset);
}

export fn read_identity_span_id(identity_ptr: u32) u32 {
    return ptrAt(Identity, identity_ptr).span_id;
}

export fn read_identity_trace_id_len(identity_ptr: u32) u32 {
    return ptrAt(Identity, identity_ptr).trace_id_len;
}

export fn get_identity_trace_id_ptr(identity_ptr: u32) u32 {
    // trace_id starts at offset 9 within Identity struct
    return identity_ptr + @offsetOf(Identity, "trace_id");
}

export fn read_write_index(identity_ptr: u32) u32 {
    return ptrAt(Identity, identity_ptr).write_index;
}

// =============================================================================
// Exported Functions - Block Allocation (capacity-aware)
// =============================================================================
// Each OpContext may have different capacity, so capacity is passed to each call.
// Capacity must be a power of 2 between 8 and 512.

/// Allocate a span system block (timestamp + entry_type for capacity rows).
export fn alloc_span_system(capacity: u32) u32 {
    return allocWithCapacity(.span_system, capacity);
}

/// Free a span system block.
export fn free_span_system(offset: u32, capacity: u32) void {
    freeWithCapacity(offset, .span_system, capacity);
}

/// Allocate a 1-byte column block (for enum/boolean).
export fn alloc_col_1b(capacity: u32) u32 {
    return allocWithCapacity(.col_1b, capacity);
}

/// Free a 1-byte column block.
export fn free_col_1b(offset: u32, capacity: u32) void {
    freeWithCapacity(offset, .col_1b, capacity);
}

/// Allocate a 4-byte column block (for u32/i32/f32).
export fn alloc_col_4b(capacity: u32) u32 {
    return allocWithCapacity(.col_4b, capacity);
}

/// Free a 4-byte column block.
export fn free_col_4b(offset: u32, capacity: u32) void {
    freeWithCapacity(offset, .col_4b, capacity);
}

/// Allocate an 8-byte column block (for f64/i64).
export fn alloc_col_8b(capacity: u32) u32 {
    return allocWithCapacity(.col_8b, capacity);
}

/// Free an 8-byte column block.
export fn free_col_8b(offset: u32, capacity: u32) void {
    freeWithCapacity(offset, .col_8b, capacity);
}

/// Allocate with explicit capacity (converts to tier internally).
fn allocWithCapacity(sc: SizeClass, capacity: u32) u32 {
    const tier = capacityToTier(capacity);
    return allocAtTier(sc, tier);
}

/// Free with explicit capacity (converts to tier internally).
fn freeWithCapacity(offset: u32, sc: SizeClass, capacity: u32) void {
    const tier = capacityToTier(capacity);
    freeAtTier(offset, sc, tier);
}

// =============================================================================
// Exported Functions - TraceRoot Operations
// =============================================================================

/// Initialize a TraceRoot at the given offset.
/// Sets wall clock and monotonic start times for timestamp calculation.
export fn init_trace_root(trace_root_ptr: u32) void {
    const root = ptrAt(TraceRoot, trace_root_ptr);
    const ms: i64 = @intFromFloat(dateNow());
    root.wall_clock_nanos = ms * 1_000_000;
    root.monotonic_ms = performanceNow();
}

// =============================================================================
// Exported Functions - Span Lifecycle
// =============================================================================

/// Write span-start to a Span System block.
/// Sets row 0 (span-start) and row 1 (span-exception placeholder).
/// Sets write_index = 2 in identity block.
export fn span_start(system_ptr: u32, identity_ptr: u32, trace_root_ptr: u32, capacity: u32) void {
    const base = bytesAt(system_ptr);

    // Span system layout: [timestamp: i64 × capacity][entry_type: u8 × capacity]
    const ts_ptr: [*]allowzero i64 = @ptrCast(@alignCast(base));
    const et_ptr: [*]allowzero u8 = base + capacity * 8;

    // Row 0: span-start
    ts_ptr[0] = getTimestampNanos(trace_root_ptr);
    et_ptr[0] = ENTRY_TYPE_SPAN_START;

    // Row 1: pre-initialize as span-exception
    ts_ptr[1] = 0;
    et_ptr[1] = ENTRY_TYPE_SPAN_EXCEPTION;

    // Set write_index = 2
    ptrAt(Identity, identity_ptr).write_index = 2;
}

/// Write span-ok to row 1 (successful completion).
export fn span_end_ok(system_ptr: u32, trace_root_ptr: u32, capacity: u32) void {
    const base = bytesAt(system_ptr);
    const ts_ptr: [*]allowzero i64 = @ptrCast(@alignCast(base));
    const et_ptr: [*]allowzero u8 = base + capacity * 8;

    et_ptr[1] = ENTRY_TYPE_SPAN_OK;
    ts_ptr[1] = getTimestampNanos(trace_root_ptr);
}

/// Write span-err to row 1 (error result, not exception).
export fn span_end_err(system_ptr: u32, trace_root_ptr: u32, capacity: u32) void {
    const base = bytesAt(system_ptr);
    const ts_ptr: [*]allowzero i64 = @ptrCast(@alignCast(base));
    const et_ptr: [*]allowzero u8 = base + capacity * 8;

    et_ptr[1] = ENTRY_TYPE_SPAN_ERR;
    ts_ptr[1] = getTimestampNanos(trace_root_ptr);
}

/// Write a log entry (info/debug/warn/error).
/// Bumps write_index, writes timestamp+entry_type, returns the idx written to.
/// SpanLogger uses returned idx for string column writes.
export fn write_log_entry(system_ptr: u32, identity_ptr: u32, trace_root_ptr: u32, entry_type: u8, capacity: u32) u32 {
    const identity = ptrAt(Identity, identity_ptr);
    const idx = identity.write_index;

    const base = bytesAt(system_ptr);
    const ts_ptr: [*]allowzero i64 = @ptrCast(@alignCast(base));
    const et_ptr: [*]allowzero u8 = base + capacity * 8;

    ts_ptr[idx] = getTimestampNanos(trace_root_ptr);
    et_ptr[idx] = entry_type;

    identity.write_index = idx + 1;
    return idx;
}

// =============================================================================
// Exported Functions - Column Write Operations
// =============================================================================

/// Write an f64 value to a column.
/// Returns: column offset (same as input if already allocated, or newly allocated)
export fn write_col_f64(col_offset: u32, row_idx: u32, value: f64, capacity: u32) u32 {
    var offset = col_offset;
    if (offset == 0) {
        offset = allocWithCapacity(.col_8b, capacity);
    }

    const base = bytesAt(offset);
    const null_bitmap_size = (capacity + 7) >> 3;

    // Write value (after null bitmap)
    const val_ptr: *allowzero align(1) f64 = @ptrCast(base + null_bitmap_size + row_idx * 8);
    val_ptr.* = value;

    // Set null bit (mark as valid)
    base[row_idx >> 3] |= @as(u8, 1) << @intCast(row_idx & 7);

    return offset;
}

/// Write a u32 value to a column.
/// Returns: column offset (same as input if already allocated, or newly allocated)
export fn write_col_u32(col_offset: u32, row_idx: u32, value: u32, capacity: u32) u32 {
    var offset = col_offset;
    if (offset == 0) {
        offset = allocWithCapacity(.col_4b, capacity);
    }

    const base = bytesAt(offset);
    const null_bitmap_size = (capacity + 7) >> 3;
    const val_ptr: *allowzero align(1) u32 = @ptrCast(base + null_bitmap_size + row_idx * 4);
    val_ptr.* = value;
    base[row_idx >> 3] |= @as(u8, 1) << @intCast(row_idx & 7);

    return offset;
}

/// Write a u8 value to a column (for enum/boolean).
/// Returns: column offset (same as input if already allocated, or newly allocated)
export fn write_col_u8(col_offset: u32, row_idx: u32, value: u8, capacity: u32) u32 {
    var offset = col_offset;
    if (offset == 0) {
        offset = allocWithCapacity(.col_1b, capacity);
    }

    const base = bytesAt(offset);
    const null_bitmap_size = (capacity + 7) >> 3;
    base[null_bitmap_size + row_idx] = value;
    base[row_idx >> 3] |= @as(u8, 1) << @intCast(row_idx & 7);

    return offset;
}

// =============================================================================
// Exported Functions - Debug/Read Operations
// =============================================================================

/// Get current performance.now() value (for debugging).
export fn get_performance_now() f64 {
    return performanceNow();
}

/// Compute a timestamp relative to trace root (for debugging).
export fn debug_compute_timestamp(trace_root_ptr: u32) i64 {
    return getTimestampNanos(trace_root_ptr);
}

/// Read timestamp at given row from a Span System block.
export fn read_timestamp(system_ptr: u32, row_idx: u32) i64 {
    const base = bytesAt(system_ptr);
    const ts_ptr: [*]allowzero const i64 = @ptrCast(@alignCast(base));
    return ts_ptr[row_idx];
}

/// Read entry_type at given row from a Span System block.
export fn read_entry_type(system_ptr: u32, row_idx: u32, capacity: u32) u8 {
    const base = bytesAt(system_ptr);
    const et_ptr: [*]allowzero const u8 = base + capacity * 8;
    return et_ptr[row_idx];
}

/// Read f64 value from a column.
export fn read_col_f64(col_offset: u32, row_idx: u32, capacity: u32) f64 {
    const base = bytesAt(col_offset);
    const null_bitmap_size = (capacity + 7) >> 3;
    const val_ptr: *allowzero align(1) const f64 = @ptrCast(base + null_bitmap_size + row_idx * 8);
    return val_ptr.*;
}

/// Check if null bit is set for a row (1 = valid, 0 = null).
export fn read_col_is_valid(col_offset: u32, row_idx: u32) u8 {
    const base = bytesAt(col_offset);
    const byte = base[row_idx >> 3];
    const bit: u8 = @as(u8, 1) << @intCast(row_idx & 7);
    return if ((byte & bit) != 0) 1 else 0;
}
