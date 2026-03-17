// =============================================================================
// Columine Reducer Bytecode VM - Columnar Batch Processing with SIMD
// =============================================================================
//
// NEW ARCHITECTURE (v2):
//   - ALL state lives in a single contiguous buffer (WebAssembly.Memory)
//   - JS creates the Memory, WASM operates on it, JS GC cleans it up
//   - State layout defined by slot metadata at known offsets
//   - Input columns copied to end of state buffer before execution
//
// Memory Layout:
//   [Header]        32 bytes
//   [Slot Meta]     24 bytes × numSlots
//   [Slot Data]     variable, 8-byte aligned
//   [Input Region]  at end, for Arrow columns
//
// Slot Metadata (24 bytes each):
//   offset: u32     - byte offset to slot data
//   capacity: u32   - for hashmap/hashset (power of 2)
//   size: u32       - current element count
//   type: u32       - SlotType (1=HashMap, 2=HashSet, 3=Aggregate)
//   aggType: u32    - AggType (1=Sum, 2=Count, 3=Min, 4=Max)
//   reserved: u32
//
// Slot Data:
//   HashMap: keys(u32[cap]) + values(u32[cap]) [+ timestamps(f64[cap])]
//   HashSet: keys(u32[cap])
//   Aggregate: value(f64) + count(u64)

const std = @import("std");
const builtin = @import("builtin");
const rawr = @import("rawr");
const types = @import("types.zig");
pub const nested = @import("nested.zig");
pub const hash_table = @import("hash_table.zig");
pub const hashmap_ops = @import("hashmap_ops.zig");
pub const hashset_ops = @import("hashset_ops.zig");
pub const slot_growth = @import("slot_growth.zig");
pub const aggregates = @import("aggregates.zig");
pub const undo_log = @import("undo_log.zig");
pub const struct_map = @import("struct_map.zig");

// Pull in test blocks from sub-modules during test compilation
comptime {
    if (@import("builtin").is_test) {
        _ = @import("nested.zig");
        _ = @import("hash_table.zig");
        _ = @import("hashmap_ops.zig");
        _ = @import("hashset_ops.zig");
        _ = @import("slot_growth.zig");
        _ = @import("aggregates.zig");
        _ = @import("undo_log.zig");
        _ = @import("struct_map.zig");
    }
}
const RoaringBitmap = rawr.RoaringBitmap;
const FrozenBitmap = rawr.FrozenBitmap;
const bitmap_allocator = if (builtin.cpu.arch == .wasm32 or builtin.cpu.arch == .wasm64)
    std.heap.wasm_allocator
else
    std.heap.smp_allocator;

var g_bitmap_scratch_ptr: usize = 0;
var g_bitmap_scratch_len: u32 = 0;
var g_bitmap_scratch_fba = std.heap.FixedBufferAllocator.init(&[_]u8{});
var g_bitmap_last_error: u32 = 0;
var g_bitmap_store_temp: ?[]u8 = null;
var g_algebra_result_buf: ?[]u8 = null;

fn ensureReusableBuffer(
    allocator: std.mem.Allocator,
    buffer: *?[]u8,
    min_len: usize,
    oom_error_code: u32,
    other_error_code: u32,
) ?[]u8 {
    if (min_len == 0) return &[_]u8{};

    if (buffer.*) |existing| {
        if (existing.len >= min_len) return existing[0..min_len];

        const grown = allocator.realloc(existing, min_len) catch |err| {
            if (err == error.OutOfMemory)
                g_bitmap_last_error = oom_error_code
            else
                g_bitmap_last_error = other_error_code;
            return null;
        };
        buffer.* = grown;
        return grown[0..min_len];
    }

    const allocated = allocator.alloc(u8, min_len) catch |err| {
        if (err == error.OutOfMemory)
            g_bitmap_last_error = oom_error_code
        else
            g_bitmap_last_error = other_error_code;
        return null;
    };
    buffer.* = allocated;
    return allocated[0..min_len];
}

inline fn bitmapBackingAllocator() std.mem.Allocator {
    if (g_bitmap_scratch_ptr != 0 and g_bitmap_scratch_len != 0) {
        // FBA is already initialised by vm_set_rbmp_scratch — just return its allocator
        return g_bitmap_scratch_fba.allocator();
    }
    return bitmap_allocator;
}

// NOTE: RETE import removed — columine is the pure reducer VM.

// =============================================================================
// Re-exports from types.zig — all public types, constants, enums
// =============================================================================

pub const STATE_MAGIC = types.STATE_MAGIC;
pub const PROGRAM_MAGIC = types.PROGRAM_MAGIC;
pub const RETE_MAGIC = types.RETE_MAGIC;
pub const STATE_HEADER_SIZE = types.STATE_HEADER_SIZE;
pub const PROGRAM_HASH_PREFIX = types.PROGRAM_HASH_PREFIX;
pub const PROGRAM_HEADER_SIZE = types.PROGRAM_HEADER_SIZE;
pub const RETE_HEADER_SIZE = types.RETE_HEADER_SIZE;
pub const STATE_FORMAT_VERSION = types.STATE_FORMAT_VERSION;
pub const StateHeaderOffset = types.StateHeaderOffset;
pub const StateFlags = types.StateFlags;
pub const SLOT_META_SIZE = types.SLOT_META_SIZE;
pub const SlotMetaOffset = types.SlotMetaOffset;
pub const DurationUnit = types.DurationUnit;
pub const ChangeFlag = types.ChangeFlag;
pub const SlotType = types.SlotType;
pub const SlotTypeFlags = types.SlotTypeFlags;
pub const AggType = types.AggType;
pub const StructFieldType = types.StructFieldType;
pub const EvictionEntry = types.EvictionEntry;
pub const ConditionTreeState = types.ConditionTreeState;
pub const CONDITION_TREE_MATCHER_PLAN_VERSION = types.CONDITION_TREE_MATCHER_PLAN_VERSION;
pub const CT_NODE_EQ = types.CT_NODE_EQ;
pub const CT_NODE_NEQ = types.CT_NODE_NEQ;
pub const CT_NODE_GT = types.CT_NODE_GT;
pub const CT_NODE_GTE = types.CT_NODE_GTE;
pub const CT_NODE_LT = types.CT_NODE_LT;
pub const CT_NODE_LTE = types.CT_NODE_LTE;
pub const CT_NODE_IN = types.CT_NODE_IN;
pub const CT_NODE_RANGE = types.CT_NODE_RANGE;
pub const CT_NODE_BOOLEAN = types.CT_NODE_BOOLEAN;
pub const CT_NODE_NOT = types.CT_NODE_NOT;
pub const CT_NODE_DESTINATION = types.CT_NODE_DESTINATION;
pub const CONDITION_TREE_STATE_BYTES = types.CONDITION_TREE_STATE_BYTES;
pub const Opcode = types.Opcode;
pub const ErrorCode = types.ErrorCode;
pub const SlotMeta = types.SlotMeta;
pub const V4f64 = types.V4f64;

// Internal aliases
const EMPTY_KEY = types.EMPTY_KEY;
const TOMBSTONE = types.TOMBSTONE;
const BITMAP_SERIALIZED_LEN_BYTES = types.BITMAP_SERIALIZED_LEN_BYTES;
const BITMAP_BYTES_PER_CAPACITY = types.BITMAP_BYTES_PER_CAPACITY;
const BITMAP_BASE_BYTES = types.BITMAP_BASE_BYTES;
pub const hashKey = types.hashKey;
pub const getSlotMeta = types.getSlotMeta;
pub const clearAllChangeFlags = types.clearAllChangeFlags;
pub const hasRelevantChanges = types.hasRelevantChanges;

// Internal aliases for functions from types.zig used throughout vm.zig
const setChangeFlag = types.setChangeFlag;

// =============================================================================
// Runtime globals (NOT types — runtime state that stays in vm.zig)
// =============================================================================

// Tracks which slot triggered CAPACITY_EXCEEDED so JS can grow it.
// Set by executeBatchImpl, read by vm_get_needs_growth_slot.
var g_needs_growth_slot: u8 = 0xFF;

/// Configure optional bitmap scratch workspace for the next VM call.
pub export fn vm_set_rbmp_scratch(ptr: u32, len: u32) void {
    g_bitmap_scratch_ptr = @as(usize, ptr);
    g_bitmap_scratch_len = len;
    if (ptr != 0 and len != 0) {
        const scratch_ptr: [*]u8 = @ptrFromInt(@as(usize, ptr));
        g_bitmap_scratch_fba = std.heap.FixedBufferAllocator.init(scratch_ptr[0..len]);
    }
}

inline fn clearBitmapScratch() void {
    g_bitmap_scratch_ptr = 0;
    g_bitmap_scratch_len = 0;
}

pub export fn vm_get_rbmp_last_error() u32 {
    return g_bitmap_last_error;
}

pub export fn vm_get_rbmp_scratch_len() u32 {
    return g_bitmap_scratch_len;
}

pub export fn vm_get_rbmp_scratch_ptr() u32 {
    return @intCast(g_bitmap_scratch_ptr);
}

// =============================================================================
// Flat-Buffer Undo Log Infrastructure
// =============================================================================
//
// Global undo log for speculative execution rollback.
// Operates on the flat-buffer state model (vm.zig), NOT the high-level
// state.zig model that undo_log.zig targets.
//
// Pre-allocated in WASM BSS — no dynamic allocation on the hot path.
// WASM is single-threaded, so global state is safe.

pub const FlatUndoOp = enum(u8) {
    MAP_INSERT = 1, // Rollback: tombstone key, decrement size
    MAP_UPDATE = 2, // Rollback: restore prev value + timestamp
    MAP_DELETE = 3, // Rollback: restore key + value + timestamp, increment size
    SET_INSERT = 4, // Rollback: tombstone elem, decrement size
    SET_DELETE = 5, // Rollback: restore elem, increment size
    AGG_UPDATE = 6, // Rollback: restore prev value (f64/i64) + count (u64) — 16-byte slots
    COUNT_UPDATE = 11, // Rollback: restore prev count (u64) — 8-byte COUNT slot
    FACT_INSERT_NEW = 7, // Rollback: tombstone derived fact key
    FACT_INSERT_UPDATE = 8, // Rollback: restore prev derived fact values
    FACT_RETRACT = 9, // Rollback: restore derived fact key + values
    LIST_APPEND_UNDO = 10, // Rollback: restore list count to prev_value
};

pub const FlatUndoEntry = extern struct {
    op: FlatUndoOp,
    slot: u8, // Slot index (0xFF = derived fact)
    _pad1: u8,
    _pad2: u8,
    key: u32, // HashMap/HashSet key or derived fact combined_key
    prev_value: u32, // Previous value (for updates/deletes), or slot_index for facts
    aux: u64, // Previous timestamp bits (f64), or packed prev values for facts
};

pub const FlatDeltaEntry = extern struct {
    undo: FlatUndoEntry,
    redo: FlatUndoEntry,
};

const UNDO_CAPACITY: u32 = 16384;
var g_undo_entries: [UNDO_CAPACITY]FlatUndoEntry = undefined;
var g_redo_entries: [UNDO_CAPACITY]FlatUndoEntry = undefined;
var g_undo_count: u32 = 0;
var g_delta_count: u32 = 0;
var g_undo_overflow: bool = false;
pub var g_undo_enabled: bool = false;

// Shadow buffer for lazy overflow snapshot — only used when undo log exceeds capacity.
// WASM: static 1MB buffer (wasm_allocator.alloc grows memory, detaching JS ArrayBuffer views).
// Native: dynamically allocated via page_allocator for any state size.
const UNDO_SHADOW_CAPACITY: u32 = 1 * 1024 * 1024;
var g_undo_shadow_static: if (builtin.cpu.arch == .wasm32) [UNDO_SHADOW_CAPACITY]u8 else [0]u8 = undefined;
var g_undo_shadow_dynamic: ?[]u8 = null;
var g_undo_shadow_active: bool = false;
var g_undo_overflow_entry: FlatUndoEntry = undefined;
var g_redo_overflow_entry: FlatUndoEntry = undefined;
var g_undo_has_overflow_entry: bool = false;
var g_undo_state_size: u32 = 0;
// Stored at vm_undo_enable time — WASM is single-threaded so this is safe
var g_undo_state_base: [*]u8 = undefined;

const native_shadow_allocator = std.heap.page_allocator;

// Saved change flags for rollback (max 256 slots + 1 derived facts flag)
var g_saved_change_flags: [257]u8 = undefined;
var g_saved_change_flags_count: u32 = 0;

var g_delta_export_start: u32 = 0;
var g_delta_export_count: u32 = 0;
var g_delta_export_overflow: bool = false;

pub fn undoAppend(entry: FlatUndoEntry) void {
    if (g_undo_count < UNDO_CAPACITY) {
        g_undo_entries[g_undo_count] = entry;
        g_undo_count += 1;
    } else if (!g_undo_overflow) {
        // First overflow: snapshot current state so rollback can restore
        // un-logged mutations that happen after this point.
        if (comptime builtin.cpu.arch == .wasm32) {
            // WASM: static shadow buffer (avoids memory.grow which detaches JS ArrayBuffer views)
            if (g_undo_state_size <= UNDO_SHADOW_CAPACITY) {
                @memcpy(g_undo_shadow_static[0..g_undo_state_size], g_undo_state_base[0..g_undo_state_size]);
                g_undo_shadow_active = true;
            }
        } else {
            // Native: dynamically allocate exact-size buffer (no fixed cap)
            const shadow = native_shadow_allocator.alloc(u8, g_undo_state_size) catch null;
            if (shadow) |s| {
                @memcpy(s, g_undo_state_base[0..g_undo_state_size]);
                g_undo_shadow_dynamic = shadow;
                g_undo_shadow_active = true;
            }
        }
        g_undo_overflow_entry = entry;
        g_undo_has_overflow_entry = true;
        g_undo_overflow = true;
    }
    // If already overflowed, silently drop — shadow buffer covers subsequent mutations
}

pub fn undoAppendPair(undo_entry: FlatUndoEntry, redo_entry: FlatUndoEntry) void {
    if (g_undo_count < UNDO_CAPACITY) {
        g_undo_entries[g_undo_count] = undo_entry;
        g_redo_entries[g_undo_count] = redo_entry;
        g_undo_count += 1;
        g_delta_count = g_undo_count;
    } else if (!g_undo_overflow) {
        // First overflow: snapshot current state so rollback can restore
        // un-logged mutations that happen after this point.
        if (comptime builtin.cpu.arch == .wasm32) {
            // WASM: static shadow buffer (avoids memory.grow which detaches JS ArrayBuffer views)
            if (g_undo_state_size <= UNDO_SHADOW_CAPACITY) {
                @memcpy(g_undo_shadow_static[0..g_undo_state_size], g_undo_state_base[0..g_undo_state_size]);
                g_undo_shadow_active = true;
            }
        } else {
            // Native: dynamically allocate exact-size buffer (no fixed cap)
            const shadow = native_shadow_allocator.alloc(u8, g_undo_state_size) catch null;
            if (shadow) |s| {
                @memcpy(s, g_undo_state_base[0..g_undo_state_size]);
                g_undo_shadow_dynamic = shadow;
                g_undo_shadow_active = true;
            }
        }
        g_undo_overflow_entry = undo_entry;
        g_redo_overflow_entry = redo_entry;
        g_undo_has_overflow_entry = true;
        g_undo_overflow = true;
    }
    // If already overflowed, silently drop — shadow buffer covers subsequent mutations
}

pub inline fn appendMutation(comptime delta_mode: bool, undo_entry: FlatUndoEntry, redo_entry: FlatUndoEntry) void {
    if (delta_mode) {
        undoAppendPair(undo_entry, redo_entry);
    } else {
        undoAppend(undo_entry);
    }
}

/// Save all slot change_flags + derived_facts_change_flag at checkpoint time.
/// Restored during rollback to undo flag-level side effects.
fn saveChangeFlags(state_base: [*]u8) void {
    const num_slots = state_base[StateHeaderOffset.NUM_SLOTS];
    var i: u32 = 0;
    while (i < num_slots) : (i += 1) {
        const meta_offset = STATE_HEADER_SIZE + i * SLOT_META_SIZE;
        g_saved_change_flags[i] = state_base[meta_offset + SlotMetaOffset.CHANGE_FLAGS];
    }
    // Save derived facts change flag
    g_saved_change_flags[num_slots] = state_base[StateHeaderOffset.DERIVED_FACTS_CHANGE_FLAG];
    g_saved_change_flags_count = num_slots + 1;
}

/// Restore all slot change_flags + derived_facts_change_flag from saved state.
fn restoreChangeFlags(state_base: [*]u8) void {
    if (g_saved_change_flags_count == 0) return;
    const num_slots = g_saved_change_flags_count - 1;
    var i: u32 = 0;
    while (i < num_slots) : (i += 1) {
        const meta_offset = STATE_HEADER_SIZE + i * SLOT_META_SIZE;
        state_base[meta_offset + SlotMetaOffset.CHANGE_FLAGS] = g_saved_change_flags[i];
    }
    state_base[StateHeaderOffset.DERIVED_FACTS_CHANGE_FLAG] = g_saved_change_flags[num_slots];
}

/// Find the array slot for a given key via hash probing.
/// Returns the slot index, or capacity if not found.

/// Find a TOMBSTONE or EMPTY slot at the hash position for restoring a deleted key.
/// Used by MAP_DELETE/SET_DELETE rollback to re-insert at the correct probe position.

// probeUpsertSlot removed — replaced by FlatHashTable.findInsert() in hash_table.zig

/// Roll back a single undo entry by reversing its mutation on the flat state buffer.
fn rollbackEntry(state_base: [*]u8, entry: FlatUndoEntry) void {
    switch (entry.op) {
        .MAP_INSERT => {
            const meta = getSlotMeta(state_base, entry.slot);
            if (undo_log.rollbackMapInsert(state_base, meta, entry.key)) {
                if (meta.hasTTL()) removeTTLEntriesForKey(state_base, meta, entry.key);
            }
        },
        .MAP_UPDATE => {
            const meta = getSlotMeta(state_base, entry.slot);
            if (undo_log.rollbackMapUpdate(state_base, meta, entry.key, entry.prev_value, entry.aux)) {
                if (meta.hasTTL()) {
                    removeTTLEntriesForKey(state_base, meta, entry.key);
                    restoreTTLEntry(state_base, meta, entry.key, @bitCast(entry.aux));
                }
            }
        },
        .MAP_DELETE => {
            const meta = getSlotMeta(state_base, entry.slot);
            if (undo_log.rollbackMapDelete(state_base, meta, entry.key, entry.prev_value, entry.aux)) {
                if (meta.hasTTL()) {
                    removeTTLEntriesForKey(state_base, meta, entry.key);
                    restoreTTLEntry(state_base, meta, entry.key, @bitCast(entry.aux));
                }
            }
        },
        .SET_INSERT => {
            const meta = getSlotMeta(state_base, entry.slot);
            if (meta.slotType() == .BITMAP) {
                // Bitmap rollback stays inline (uses roaring library)
                const storage = getBitmapStorage(state_base, meta);
                var bitmap = bitmapLoad(storage) orelse return;
                defer bitmap.deinit();
                const removed = bitmap.value.remove(entry.key) catch false;
                if (removed) {
                    const cardinality: u32 = @intCast(bitmap.value.cardinality());
                    if (bitmapStore(storage, &bitmap.value) != .OK) return;
                    meta.size_ptr.* = cardinality;
                    if (meta.hasTTL()) removeTTLEntriesForKey(state_base, meta, entry.key);
                }
            } else {
                if (undo_log.rollbackSetInsert(state_base, meta, entry.key)) {
                    if (meta.hasTTL()) removeTTLEntriesForKey(state_base, meta, entry.key);
                }
            }
        },
        .SET_DELETE => {
            const meta = getSlotMeta(state_base, entry.slot);
            if (meta.slotType() == .BITMAP) {
                const storage = getBitmapStorage(state_base, meta);
                var bitmap = bitmapLoad(storage) orelse return;
                defer bitmap.deinit();
                _ = bitmap.value.add(entry.key) catch false;
                const cardinality: u32 = @intCast(bitmap.value.cardinality());
                if (cardinality <= meta.capacity and bitmapStore(storage, &bitmap.value) == .OK) {
                    meta.size_ptr.* = cardinality;
                }
                if (meta.hasTTL()) {
                    removeTTLEntriesForKey(state_base, meta, entry.key);
                    restoreTTLEntry(state_base, meta, entry.key, @bitCast(entry.aux));
                }
            } else {
                if (undo_log.rollbackSetDelete(state_base, meta, entry.key)) {
                    if (meta.hasTTL()) {
                        removeTTLEntriesForKey(state_base, meta, entry.key);
                        restoreTTLEntry(state_base, meta, entry.key, @bitCast(entry.aux));
                    }
                }
            }
        },
        .AGG_UPDATE => {
            // Undo aggregate update: restore prev value (f64/i64) and prev count (u64)
            // aux stores the previous value bits, prev_value stores previous count (truncated to u32)
            const meta = getSlotMeta(state_base, entry.slot);
            const val_ptr: *u64 = @ptrCast(@alignCast(state_base + meta.offset));
            const count_ptr: *u64 = @ptrCast(@alignCast(state_base + meta.offset + 8));
            val_ptr.* = entry.aux;
            count_ptr.* = entry.prev_value;
        },
        .COUNT_UPDATE => {
            // Undo count update: restore prev count — COUNT slot is 8 bytes (u64 only)
            const meta = getSlotMeta(state_base, entry.slot);
            const count_ptr: *u64 = @ptrCast(@alignCast(state_base + meta.offset));
            count_ptr.* = entry.prev_value;
        },
        .FACT_INSERT_NEW => {
            // Undo derived fact new insertion: tombstone the key
            // entry.slot = 0xFF sentinel, entry.prev_value = HashMap slot index
            const derived_offset = getDerivedFactsOffset(state_base);
            const capacity = getDerivedFactsCapacity(state_base);
            const fact_data = state_base + derived_offset;
            const keys: [*]u32 = @ptrCast(@alignCast(fact_data));
            const slot_idx = entry.prev_value;
            if (slot_idx < capacity) {
                keys[slot_idx] = TOMBSTONE;
            }
        },
        .FACT_INSERT_UPDATE => {
            // Undo derived fact update: restore previous values_lo and values_hi
            // entry.prev_value = HashMap slot index, entry.aux = packed (lo << 32 | hi)
            const derived_offset = getDerivedFactsOffset(state_base);
            const capacity = getDerivedFactsCapacity(state_base);
            const fact_data = state_base + derived_offset;
            const values_lo: [*]u32 = @ptrCast(@alignCast(fact_data + @as(u32, capacity) * 4));
            const values_hi: [*]u32 = @ptrCast(@alignCast(fact_data + @as(u32, capacity) * 8));
            const slot_idx = entry.prev_value;
            if (slot_idx < capacity) {
                values_lo[slot_idx] = @truncate(entry.aux >> 32);
                values_hi[slot_idx] = @truncate(entry.aux);
            }
        },
        .FACT_RETRACT => {
            // Undo derived fact retract: restore key + values_lo + values_hi
            // entry.key = combined_key, entry.prev_value = HashMap slot index
            // entry.aux = packed (lo << 32 | hi)
            const derived_offset = getDerivedFactsOffset(state_base);
            const capacity = getDerivedFactsCapacity(state_base);
            const fact_data = state_base + derived_offset;
            const keys: [*]u32 = @ptrCast(@alignCast(fact_data));
            const values_lo: [*]u32 = @ptrCast(@alignCast(fact_data + @as(u32, capacity) * 4));
            const values_hi: [*]u32 = @ptrCast(@alignCast(fact_data + @as(u32, capacity) * 8));
            const slot_idx = entry.prev_value;
            if (slot_idx < capacity) {
                keys[slot_idx] = entry.key;
                values_lo[slot_idx] = @truncate(entry.aux >> 32);
                values_hi[slot_idx] = @truncate(entry.aux);
            }
        },
        .LIST_APPEND_UNDO => {
            // Undo list append: restore count to previous value
            const meta_off = STATE_HEADER_SIZE + @as(u32, entry.slot) * SLOT_META_SIZE;
            std.mem.writeInt(u32, state_base[meta_off + 8 ..][0..4], entry.prev_value, .little);
        },
    }
}

// =============================================================================
// TTL Eviction Operations
// =============================================================================

/// Get pointer to eviction index array for a TTL slot
pub inline fn getEvictionIndex(state_base: [*]u8, meta: SlotMeta) [*]EvictionEntry {
    return @ptrCast(@alignCast(state_base + meta.eviction_index_offset));
}

/// Get pointer to evicted buffer (for RETE rule triggers)
inline fn getEvictedBuffer(state_base: [*]u8, meta: SlotMeta) [*]EvictionEntry {
    return @ptrCast(@alignCast(state_base + meta.evicted_buffer_offset));
}

/// Binary search for insertion position in sorted eviction index
fn binarySearchEvictionPos(index: [*]const EvictionEntry, size: u32, timestamp: f64) u32 {
    var left: u32 = 0;
    var right: u32 = size;

    while (left < right) {
        const mid = left + (right - left) / 2;
        if (index[mid].timestamp < timestamp) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    return left;
}

/// Shift eviction index entries left (remove from front)
fn shiftEvictionLeft(index: [*]EvictionEntry, count: u32, size: u32) void {
    if (count >= size) return;
    const remaining = size - count;
    var i: u32 = 0;
    while (i < remaining) : (i += 1) {
        index[i] = index[i + count];
    }
}

/// Shift eviction index entries right (make room for insert)
fn shiftEvictionRight(index: [*]EvictionEntry, pos: u32, size: u32) void {
    if (size == 0) return;
    var i: u32 = size;
    while (i > pos) {
        index[i] = index[i - 1];
        i -= 1;
    }
}

/// Remove all eviction-index entries for a key.
/// Returns number of removed entries.
fn removeEvictionEntriesForKey(index: [*]EvictionEntry, size: u32, key: u32) u32 {
    var write_idx: u32 = 0;
    var read_idx: u32 = 0;
    var removed: u32 = 0;

    while (read_idx < size) : (read_idx += 1) {
        const entry = index[read_idx];
        if (entry.key_or_idx == key) {
            removed += 1;
            continue;
        }
        if (write_idx != read_idx) {
            index[write_idx] = entry;
        }
        write_idx += 1;
    }

    return removed;
}

pub fn findLatestEvictionTimestampForKey(index: [*]const EvictionEntry, size: u32, key: u32) ?f64 {
    if (size == 0) return null;
    var i: u32 = size;
    while (i > 0) {
        i -= 1;
        const entry = index[i];
        if (entry.key_or_idx == key) {
            return entry.timestamp;
        }
    }
    return null;
}

fn removeTTLEntriesForKey(state_base: [*]u8, meta: SlotMeta, key: u32) void {
    if (!meta.hasTTL()) return;

    const eviction_index = getEvictionIndex(state_base, meta);
    const eviction_size = meta.eviction_index_size_ptr.*;
    const removed = removeEvictionEntriesForKey(eviction_index, eviction_size, key);
    if (removed > 0) {
        meta.eviction_index_size_ptr.* = eviction_size - removed;
    }
}

fn restoreTTLEntry(state_base: [*]u8, meta: SlotMeta, key: u32, timestamp: f64) void {
    if (!meta.hasTTL()) return;
    const insert_result = insertWithTTL(state_base, meta, key, timestamp);
    std.debug.assert(insert_result == .OK);
}

/// Returns true when the eviction index entry still represents the current
/// value for the key (no newer entry for the same key later in the sorted list).
fn isEvictionEntryCurrent(index: [*]const EvictionEntry, size: u32, entry_idx: u32, key: u32) bool {
    var i = entry_idx + 1;
    while (i < size) : (i += 1) {
        if (index[i].key_or_idx == key) {
            return false;
        }
    }
    return true;
}

/// Evict all entries with timestamp older than cutoff.
/// Returns number of entries evicted.
/// If has_evict_trigger is set, evicted entries are copied to evicted buffer for RETE.
/// Note: JS layer applies startOf truncation to 'now' before calling this function.
/// cutoff = now - ttl_seconds - grace_seconds
pub fn evictExpired(state_base: [*]u8, meta: SlotMeta, slot_idx: u8, now: f64) u32 {
    if (!meta.hasTTL()) return 0;

    const cutoff = meta.cutoff(now);
    const eviction_index = getEvictionIndex(state_base, meta);
    const eviction_size = meta.eviction_index_size_ptr.*;

    var processed_count: u32 = 0;
    var removed_count: u32 = 0;

    // Scan from front (oldest entries first)
    while (processed_count < eviction_size) {
        const entry = eviction_index[processed_count];
        if (entry.timestamp >= cutoff) break; // Done - rest are newer

        // Skip stale index entries (e.g. older timestamp for a key that was
        // updated later in the same window).
        if (!isEvictionEntryCurrent(eviction_index, eviction_size, processed_count, entry.key_or_idx)) {
            processed_count += 1;
            continue;
        }

        // Remove from primary storage (HashMap/HashSet/Array). If entry is no
        // longer present, this was effectively stale and should not be emitted.
        if (g_undo_enabled) {
            switch (meta.slotType()) {
                .HASHMAP => {
                    const tbl = hash_table.FlatHashTable(u32).bindSlot(state_base, meta);
                    if (tbl.find(entry.key_or_idx)) |idx| {
                        const ts: [*]const f64 = @ptrCast(@alignCast(state_base + meta.offset + meta.capacity * 8));
                        appendMutation(
                            false,
                            .{ .op = .MAP_DELETE, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = entry.key_or_idx, .prev_value = tbl.entries[idx], .aux = @bitCast(ts[idx]) },
                            .{ .op = .MAP_INSERT, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = entry.key_or_idx, .prev_value = 0, .aux = 0 },
                        );
                    }
                },
                .HASHSET, .BITMAP => {
                    appendMutation(
                        false,
                        .{
                            .op = .SET_DELETE,
                            .slot = slot_idx,
                            ._pad1 = 0,
                            ._pad2 = 0,
                            .key = entry.key_or_idx,
                            .prev_value = 0,
                            .aux = @bitCast(entry.timestamp),
                        },
                        .{
                            .op = .SET_INSERT,
                            .slot = slot_idx,
                            ._pad1 = 0,
                            ._pad2 = 0,
                            .key = entry.key_or_idx,
                            .prev_value = 0,
                            .aux = 0,
                        },
                    );
                },
                else => {},
            }
        }

        if (removeEntryByKey(state_base, meta, entry.key_or_idx)) {
            // Record for RETE rule firing if has_evict_trigger
            if (meta.hasEvictTrigger()) {
                const evicted_buffer = getEvictedBuffer(state_base, meta);
                const evicted_count = meta.evicted_count_ptr.*;
                evicted_buffer[evicted_count] = entry;
                meta.evicted_count_ptr.* += 1;
            }

            removed_count += 1;
        }

        processed_count += 1;
    }

    // Shift sorted array to remove processed entries
    if (processed_count > 0) {
        shiftEvictionLeft(eviction_index, processed_count, eviction_size);
        meta.eviction_index_size_ptr.* = eviction_size - processed_count;

        if (removed_count > 0) {
            // Update slot size only for entries actually removed from storage.
            meta.size_ptr.* -= removed_count;

            // Set change flag
            setChangeFlag(meta, ChangeFlag.EVICTED);
        }
    }

    return removed_count;
}

/// Remove entry from primary storage by key (for HashMap/HashSet) or index (for Array)
fn removeEntryByKey(state_base: [*]u8, meta: SlotMeta, key: u32) bool {
    switch (meta.slotType()) {
        .HASHMAP => {
            const tbl = hash_table.FlatHashTable(u32).bindSlot(state_base, meta);
            const pos = tbl.find(key) orelse return false;
            tbl.keys[pos] = TOMBSTONE;
            return true;
        },
        .HASHSET => {
            const tbl = hash_table.HashSet.bindSlot(state_base, meta);
            const pos = tbl.find(key) orelse return false;
            tbl.keys[pos] = TOMBSTONE;
            return true;
        },
        .BITMAP => {
            const storage = getBitmapStorage(state_base, meta);
            var bitmap = bitmapLoad(storage) orelse return false;
            defer bitmap.deinit();

            const removed = bitmap.value.remove(key) catch return false;
            if (!removed) return false;
            const cardinality: u32 = @intCast(bitmap.value.cardinality());
            if (bitmapStore(storage, &bitmap.value) != .OK) return false;
            meta.size_ptr.* = cardinality;
            return true;
        },
        .ARRAY => {
            // For arrays, key_or_idx is the array index
            // We mark as tombstone (for sparse arrays) or shift (for dense)
            // For now, use tombstone approach
            const data_ptr = state_base + meta.offset;
            const keys: [*]u32 = @ptrCast(@alignCast(data_ptr));
            if (key < meta.capacity) {
                if (keys[key] == EMPTY_KEY or keys[key] == TOMBSTONE) return false;
                keys[key] = TOMBSTONE;
                return true;
            }
            return false;
        },
        .AGGREGATE => {
            // Aggregates don't support individual entry removal
            // TTL for aggregates means the whole aggregate expires
            return false;
        },
        .CONDITION_TREE => {
            removeConditionTreeEntry(state_base, meta, key);
            return true;
        },
        .STRUCT_MAP => {
            // TTL eviction not yet supported for struct maps
            return false;
        },
        .ORDERED_LIST => {
            // TTL eviction not supported for ordered lists
            return false;
        },
        .SCALAR => {
            // Scalar slots don't support key-based removal
            return false;
        },
        .NESTED => {
            // Nested slot TTL eviction not yet supported
            return false;
        },
    }
}

inline fn getConditionTreeStatePtr(state_base: [*]u8, meta: SlotMeta) *ConditionTreeState {
    return @ptrCast(@alignCast(state_base + meta.offset));
}

fn removeConditionTreeEntry(state_base: [*]u8, meta: SlotMeta, key: u32) void {
    const tree_state = getConditionTreeStatePtr(state_base, meta);
    tree_state.lifecycle_generation +%= 1;
    tree_state.last_removed_key = key;
}

/// Insert entry with TTL tracking (maintains sorted eviction index)
pub fn insertWithTTL(state_base: [*]u8, meta: SlotMeta, key: u32, timestamp: f64) ErrorCode {
    if (!meta.hasTTL()) return .OK;

    const eviction_index = getEvictionIndex(state_base, meta);
    const eviction_size_initial = meta.eviction_index_size_ptr.*;
    const removed_for_key = removeEvictionEntriesForKey(eviction_index, eviction_size_initial, key);
    const eviction_size = eviction_size_initial - removed_for_key;
    meta.eviction_index_size_ptr.* = eviction_size;

    // Check capacity
    if (eviction_size >= meta.eviction_index_capacity) {
        // Deterministic overflow handling: signal growth requirement through the
        // same CAPACITY_EXCEEDED path used by primary slot storage.
        return .CAPACITY_EXCEEDED;
    }

    // Binary search for insert position (sorted by timestamp)
    const pos = binarySearchEvictionPos(eviction_index, eviction_size, timestamp);

    const entry_value: u32 = switch (meta.slotType()) {
        .HASHMAP => blk: {
            const tbl = hash_table.FlatHashTable(u32).bindSlot(state_base, meta);
            break :blk if (tbl.get(key)) |v| v.* else 0;
        },
        .HASHSET, .BITMAP => key,
        else => 0,
    };

    // Shift right and insert
    shiftEvictionRight(eviction_index, pos, eviction_size);
    eviction_index[pos] = .{ .timestamp = timestamp, .key_or_idx = key, .value = entry_value };
    meta.eviction_index_size_ptr.* = eviction_size + 1;
    return .OK;
}

/// Clear evicted buffer (call after RETE rules have processed evictions)
pub fn clearEvictedBuffer(meta: SlotMeta) void {
    meta.evicted_count_ptr.* = 0;
}

/// Evict expired entries for all TTL-enabled slots.
/// Call at the start of each batch.
pub export fn vm_evict_all_expired(state_base: [*]u8, now: f64) u32 {
    // Validate state header
    const state_magic: *const u32 = @ptrCast(@alignCast(state_base));
    if (state_magic.* != STATE_MAGIC) return @intFromEnum(ErrorCode.INVALID_STATE);

    const num_slots = state_base[StateHeaderOffset.NUM_SLOTS];
    var total_evicted: u32 = 0;

    var i: u8 = 0;
    while (i < num_slots) : (i += 1) {
        const meta = getSlotMeta(state_base, i);
        if (meta.hasTTL()) {
            // Clear evicted buffer from previous batch
            clearEvictedBuffer(meta);
            // Evict expired entries
            total_evicted += evictExpired(state_base, meta, i, now);
        }
    }

    return total_evicted;
}

// =============================================================================
// HashMap Operations
// =============================================================================


// =============================================================================
// HashSet Operations
// =============================================================================

const BitmapStorage = struct {
    serialized_len_ptr: *u32,
    payload_ptr: [*]u8,
    payload_capacity: u32,
};

const BitmapSlot = struct {
    meta: SlotMeta,
    storage: BitmapStorage,
};

const LoadedBitmap = struct {
    value: RoaringBitmap,

    fn deinit(self: *LoadedBitmap) void {
        self.value.deinit();
    }
};

inline fn bitmapPayloadCapacity(slot_capacity: u32) u32 {
    return slot_capacity * BITMAP_BYTES_PER_CAPACITY + BITMAP_BASE_BYTES;
}

inline fn getBitmapStorage(state_base: [*]u8, meta: SlotMeta) BitmapStorage {
    const data_ptr = state_base + meta.offset;
    const serialized_len_ptr: *u32 = @ptrCast(@alignCast(data_ptr));
    return .{
        .serialized_len_ptr = serialized_len_ptr,
        .payload_ptr = data_ptr + BITMAP_SERIALIZED_LEN_BYTES,
        .payload_capacity = bitmapPayloadCapacity(meta.capacity),
    };
}

inline fn getBitmapSlotByOffset(state_base: [*]u8, slot_offset: u32) ?BitmapSlot {
    const meta = findSlotMetaByOffset(state_base, slot_offset) orelse return null;
    if (meta.slotType() != .BITMAP) return null;
    return .{ .meta = meta, .storage = getBitmapStorage(state_base, meta) };
}

inline fn bitmapFrozen(storage: BitmapStorage) ?FrozenBitmap {
    const serialized_len = storage.serialized_len_ptr.*;
    if (serialized_len == 0 or serialized_len > storage.payload_capacity) {
        return null;
    }
    return FrozenBitmap.init(storage.payload_ptr[0..serialized_len]) catch return null;
}

fn bitmapLoad(storage: BitmapStorage) ?LoadedBitmap {
    const alloc = bitmapBackingAllocator();
    const scratch_active = g_bitmap_scratch_ptr != 0 and g_bitmap_scratch_len != 0;

    const serialized_len = storage.serialized_len_ptr.*;
    if (serialized_len == 0) {
        const empty = RoaringBitmap.init(alloc) catch |err| blk: {
            if (err == error.OutOfMemory and scratch_active) {
                break :blk RoaringBitmap.init(bitmap_allocator) catch |fallback_err| {
                    if (fallback_err == error.OutOfMemory) g_bitmap_last_error = 100 else g_bitmap_last_error = 109;
                    return null;
                };
            }
            if (err == error.OutOfMemory) g_bitmap_last_error = 100 else g_bitmap_last_error = 109;
            return null;
        };
        return .{ .value = empty };
    }
    if (serialized_len > storage.payload_capacity) {
        return null;
    }

    const data = storage.payload_ptr[0..serialized_len];
    const restored = RoaringBitmap.deserialize(alloc, data) catch |err| blk: {
        if (err == error.OutOfMemory and scratch_active) {
            break :blk RoaringBitmap.deserialize(bitmap_allocator, data) catch |fallback_err| {
                if (fallback_err == error.OutOfMemory)
                    g_bitmap_last_error = 101
                else if (fallback_err == error.InvalidFormat)
                    g_bitmap_last_error = 102
                else
                    g_bitmap_last_error = 103;
                return null;
            };
        }

        if (err == error.OutOfMemory)
            g_bitmap_last_error = 101
        else if (err == error.InvalidFormat)
            g_bitmap_last_error = 102
        else
            g_bitmap_last_error = 103;
        return null;
    };
    return .{ .value = restored };
}

fn bitmapStore(storage: BitmapStorage, bitmap: *RoaringBitmap) ErrorCode {
    // Optimize container encoding (array → run where beneficial) before serialization
    _ = bitmap.runOptimize() catch {};

    const serialized_size_needed = bitmap.serializedSizeInBytes();
    if (serialized_size_needed > storage.payload_capacity) {
        g_bitmap_last_error = 60;
        return .CAPACITY_EXCEEDED;
    }

    // Two-phase commit: serialize to temp buffer first, then copy into slot storage.
    // serializeIntoBuffer may partially write on failure; this keeps slot bytes unmodified
    // when we need to return CAPACITY_EXCEEDED/INVALID_STATE and retry after growth.
    const scratch_active = g_bitmap_scratch_ptr != 0 and g_bitmap_scratch_len != 0;
    const temp = if (scratch_active)
        bitmapBackingAllocator().alloc(u8, serialized_size_needed) catch |err| {
            if (err == error.OutOfMemory) {
                g_bitmap_last_error = 60;
                return .CAPACITY_EXCEEDED;
            }
            g_bitmap_last_error = 61;
            return .INVALID_STATE;
        }
    else
        ensureReusableBuffer(bitmap_allocator, &g_bitmap_store_temp, serialized_size_needed, 60, 61) orelse {
            return if (g_bitmap_last_error == 61) .INVALID_STATE else .CAPACITY_EXCEEDED;
        };

    const serialized_size_usize = bitmap.serializeIntoBuffer(temp) catch |err| {
        if (err == error.NoSpaceLeft or err == error.OutOfMemory) {
            // OOM in scratch FBA during serialization temp-buffer alloc → treat as
            // capacity exceeded so the JS growth loop doubles the slot (and scratch).
            g_bitmap_last_error = 60;
            return .CAPACITY_EXCEEDED;
        }
        g_bitmap_last_error = 61;
        return .INVALID_STATE;
    };

    const serialized_size: u32 = @intCast(serialized_size_usize);
    if (serialized_size > storage.payload_capacity) {
        return .CAPACITY_EXCEEDED;
    }

    storage.serialized_len_ptr.* = serialized_size;
    @memcpy(storage.payload_ptr[0..serialized_size], temp[0..serialized_size]);
    if (serialized_size < storage.payload_capacity) {
        @memset(storage.payload_ptr[serialized_size..storage.payload_capacity], 0);
    }
    return .OK;
}

fn bitmapSelect(storage: BitmapStorage, rank: u32) ?u32 {
    const frozen = bitmapFrozen(storage) orelse return null;
    var iterator = frozen.iterator();
    var idx: u32 = 0;
    while (iterator.next()) |element| {
        if (idx == rank) return element;
        idx += 1;
    }
    return null;
}

pub fn batchBitmapAdd(
    comptime delta_mode: bool,
    state_base: [*]u8,
    meta: SlotMeta,
    slot_idx: u8,
    elem_col: [*]const u32,
    ts_col: ?[*]const f64,
    batch_len: u32,
) ErrorCode {
    g_bitmap_last_error = 0;
    const storage = getBitmapStorage(state_base, meta);
    var bitmap = bitmapLoad(storage) orelse {
        // OOM during deserialize (error 100/101) → scratch FBA exhausted, trigger growth
        if (g_bitmap_last_error == 100 or g_bitmap_last_error == 101) return .CAPACITY_EXCEEDED;
        if (g_bitmap_last_error == 0) g_bitmap_last_error = 1;
        return .INVALID_STATE;
    };
    defer bitmap.deinit();

    const original_size = meta.size_ptr.*;
    var cardinality: u32 = @intCast(bitmap.value.cardinality());
    if (cardinality != original_size) {
        meta.size_ptr.* = cardinality;
    }
    var had_insert = false;

    var i: u32 = 0;
    while (i < batch_len) : (i += 1) {
        const elem = elem_col[i];
        if (elem == EMPTY_KEY or elem == TOMBSTONE) continue;
        const ts = if (meta.hasTTL()) ts_col.?[i] else 0;

        const already_present = bitmap.value.contains(elem);
        if (!already_present and cardinality >= meta.capacity) {
            if (had_insert) {
                const flush_result = bitmapStore(storage, &bitmap.value);
                if (flush_result != .OK) {
                    g_bitmap_last_error = 2;
                    meta.size_ptr.* = original_size;
                    return flush_result;
                }
                meta.size_ptr.* = cardinality;
                setChangeFlag(meta, ChangeFlag.INSERTED);
            } else {
                meta.size_ptr.* = original_size;
            }
            return .CAPACITY_EXCEEDED;
        }

        const inserted = if (already_present)
            false
        else
            bitmap.value.add(elem) catch {
                // OOM in scratch FBA during container growth → flush and trigger growth
                g_bitmap_last_error = 3;
                if (had_insert) {
                    const flush_result = bitmapStore(storage, &bitmap.value);
                    if (flush_result != .OK) {
                        g_bitmap_last_error = 4;
                        meta.size_ptr.* = original_size;
                        return flush_result;
                    }
                    setChangeFlag(meta, ChangeFlag.INSERTED);
                }
                meta.size_ptr.* = cardinality;
                return .CAPACITY_EXCEEDED;
            };

        if (!inserted) {
            if (meta.hasTTL()) {
                const ttl_result = insertWithTTL(state_base, meta, elem, ts);
                if (ttl_result != .OK) {
                    if (had_insert) {
                        const flush_result = bitmapStore(storage, &bitmap.value);
                        if (flush_result != .OK) {
                            meta.size_ptr.* = original_size;
                            return flush_result;
                        }
                        setChangeFlag(meta, ChangeFlag.INSERTED);
                    }
                    meta.size_ptr.* = cardinality;
                    return ttl_result;
                }
            }
            continue;
        }

        if (g_undo_enabled) {
            meta.size_ptr.* = cardinality;
            appendMutation(
                delta_mode,
                .{
                    .op = .SET_INSERT,
                    .slot = slot_idx,
                    ._pad1 = 0,
                    ._pad2 = 0,
                    .key = elem,
                    .prev_value = 0,
                    .aux = 0,
                },
                .{
                    .op = .SET_DELETE,
                    .slot = slot_idx,
                    ._pad1 = 0,
                    ._pad2 = 0,
                    .key = elem,
                    .prev_value = 0,
                    .aux = 0,
                },
            );
        }

        cardinality += 1;
        had_insert = true;

        if (meta.hasTTL()) {
            const ttl_result = insertWithTTL(state_base, meta, elem, ts);
            if (ttl_result != .OK) {
                const flush_result = bitmapStore(storage, &bitmap.value);
                if (flush_result != .OK) {
                    g_bitmap_last_error = 5;
                    meta.size_ptr.* = original_size;
                    return flush_result;
                }
                meta.size_ptr.* = cardinality;
                if (had_insert) setChangeFlag(meta, ChangeFlag.INSERTED);
                return ttl_result;
            }
        }
    }

    const store_result = bitmapStore(storage, &bitmap.value);
    if (store_result != .OK) {
        if (g_bitmap_last_error == 0) g_bitmap_last_error = 6;
        meta.size_ptr.* = original_size;
        return store_result;
    }

    meta.size_ptr.* = cardinality;
    if (had_insert) setChangeFlag(meta, ChangeFlag.INSERTED);
    return .OK;
}

pub fn batchBitmapRemove(
    comptime delta_mode: bool,
    state_base: [*]u8,
    meta: SlotMeta,
    slot_idx: u8,
    elem_col: [*]const u32,
    batch_len: u32,
) void {
    const storage = getBitmapStorage(state_base, meta);
    var bitmap = bitmapLoad(storage) orelse return;
    defer bitmap.deinit();

    var cardinality: u32 = @intCast(bitmap.value.cardinality());
    var had_remove = false;

    var i: u32 = 0;
    while (i < batch_len) : (i += 1) {
        const elem = elem_col[i];
        if (cardinality == 0) break;

        const removed = bitmap.value.remove(elem) catch false;
        if (!removed) {
            continue;
        }

        if (g_undo_enabled) {
            var prev_ts_bits: u64 = 0;
            if (meta.hasTTL()) {
                const eviction_index = getEvictionIndex(state_base, meta);
                const eviction_size = meta.eviction_index_size_ptr.*;
                if (findLatestEvictionTimestampForKey(eviction_index, eviction_size, elem)) |prev_ts| {
                    prev_ts_bits = @bitCast(prev_ts);
                }
            }
            meta.size_ptr.* = cardinality;
            appendMutation(
                delta_mode,
                .{
                    .op = .SET_DELETE,
                    .slot = slot_idx,
                    ._pad1 = 0,
                    ._pad2 = 0,
                    .key = elem,
                    .prev_value = 0,
                    .aux = prev_ts_bits,
                },
                .{
                    .op = .SET_INSERT,
                    .slot = slot_idx,
                    ._pad1 = 0,
                    ._pad2 = 0,
                    .key = elem,
                    .prev_value = 0,
                    .aux = 0,
                },
            );
        }

        cardinality -= 1;
        had_remove = true;
        if (meta.hasTTL()) {
            removeTTLEntriesForKey(state_base, meta, elem);
        }
    }

    if (!had_remove) {
        meta.size_ptr.* = cardinality;
        return;
    }

    if (bitmapStore(storage, &bitmap.value) != .OK) {
        return;
    }

    meta.size_ptr.* = cardinality;
    if (had_remove) setChangeFlag(meta, ChangeFlag.REMOVED);
}

// =============================================================================
// In-place bitmap set algebra
// =============================================================================

const BitmapAlgebraOp = enum { AND, OR, AND_NOT, XOR };

/// Force an undo snapshot so bulk bitmap mutations are covered by shadow rollback.
/// Same mechanism as natural overflow — captures full state at this point.
fn forceUndoSnapshot() void {
    if (comptime builtin.cpu.arch == .wasm32) {
        if (g_undo_state_size <= UNDO_SHADOW_CAPACITY) {
            @memcpy(g_undo_shadow_static[0..g_undo_state_size], g_undo_state_base[0..g_undo_state_size]);
            g_undo_shadow_active = true;
        }
    } else {
        const shadow = native_shadow_allocator.alloc(u8, g_undo_state_size) catch null;
        if (shadow) |s| {
            @memcpy(s, g_undo_state_base[0..g_undo_state_size]);
            g_undo_shadow_dynamic = shadow;
            g_undo_shadow_active = true;
        }
    }
    g_undo_overflow = true;
}

/// In-place set algebra: mutate target slot's bitmap with source bitmap data.
/// Uses rawr's in-place operations to avoid allocating a new result bitmap.
/// Undo uses snapshot-based rollback (not per-element tracking).
fn batchBitmapAlgebra(
    comptime op: BitmapAlgebraOp,
    state_base: [*]u8,
    target_meta: SlotMeta,
    source_data: []const u8,
) ErrorCode {
    const target_storage = getBitmapStorage(state_base, target_meta);
    const original_size = target_meta.size_ptr.*;

    // Empty-source identities
    if (source_data.len == 0) {
        switch (op) {
            .AND => {
                // AND with empty = clear target
                if (g_undo_enabled and !g_undo_overflow) forceUndoSnapshot();
                target_storage.serialized_len_ptr.* = 0;
                @memset(target_storage.payload_ptr[0..target_storage.payload_capacity], 0);
                target_meta.size_ptr.* = 0;
                if (original_size != 0) setChangeFlag(target_meta, ChangeFlag.SIZE_CHANGED);
                return .OK;
            },
            // OR/ANDNOT/XOR with empty = no change
            .OR, .AND_NOT, .XOR => return .OK,
        }
    }

    // Force undo snapshot before bulk mutation (individual element tracking impractical)
    if (g_undo_enabled and !g_undo_overflow) forceUndoSnapshot();

    var target = bitmapLoad(target_storage) orelse return .INVALID_STATE;
    defer target.deinit();

    // Deserialize source into arena (freed on return)
    var arena = std.heap.ArenaAllocator.init(bitmap_allocator);
    defer arena.deinit();
    var source = RoaringBitmap.deserialize(arena.allocator(), source_data) catch {
        g_bitmap_last_error = 80;
        return .INVALID_STATE;
    };

    // In-place operation
    switch (op) {
        .AND => target.value.bitwiseAndInPlace(&source) catch {
            g_bitmap_last_error = 81;
            target_meta.size_ptr.* = original_size;
            return .INVALID_STATE;
        },
        .OR => target.value.bitwiseOrInPlace(&source) catch {
            g_bitmap_last_error = 82;
            target_meta.size_ptr.* = original_size;
            return .INVALID_STATE;
        },
        .AND_NOT => target.value.bitwiseDifferenceInPlace(&source) catch {
            g_bitmap_last_error = 83;
            target_meta.size_ptr.* = original_size;
            return .INVALID_STATE;
        },
        .XOR => target.value.bitwiseXorInPlace(&source) catch {
            g_bitmap_last_error = 84;
            target_meta.size_ptr.* = original_size;
            return .INVALID_STATE;
        },
    }

    // Store back
    const store_result = bitmapStore(target_storage, &target.value);
    if (store_result != .OK) {
        target_meta.size_ptr.* = original_size;
        return store_result;
    }

    const new_card: u32 = @intCast(target.value.cardinality());
    target_meta.size_ptr.* = new_card;
    if (new_card != original_size) setChangeFlag(target_meta, ChangeFlag.SIZE_CHANGED);

    return .OK;
}



// =============================================================================
// Single-Element Operations (for block-based dispatch in FOR_EACH_EVENT)
// =============================================================================
// Each function processes ONE key/value at a time. The batch loop lives in
// FOR_EACH_EVENT; these are the loop bodies.  Existing BATCH_* handlers are
// preserved unchanged for backward compat with pre-block bytecode.









const StructUpsertResult = struct {
    err: ErrorCode,
    pos: u32, // hash table position where key was placed (only valid if err == .OK)
};

/// Single-element struct map upsert (last-wins). Reads scalar field values from
/// columns at the given element index, writes packed row. Returns the hash
/// position so the caller can write array fields to the same row.
fn singleStructMapUpsertLast(
    comptime delta_mode: bool,
    state_base: [*]u8,
    slot_idx: u8,
    key: u32,
    val_cols: []const u8,
    field_idxs: []const u8,
    num_vals: u8,
    col_ptrs: [*]const [*]const u8,
    element_idx: u32,
) StructUpsertResult {
    if (key == EMPTY_KEY or key == TOMBSTONE) return .{ .err = .OK, .pos = 0 };

    const smap = struct_map.StructMapSlot.bind(state_base, slot_idx);
    const result = smap.upsert(key) orelse {
        if (comptime delta_mode) {
            const meta_base = STATE_HEADER_SIZE + @as(u32, slot_idx) * SLOT_META_SIZE;
            state_base[meta_base + 14] |= 0x01;
        }
        g_needs_growth_slot = slot_idx;
        return .{ .err = .CAPACITY_EXCEEDED, .pos = 0 };
    };

    const row = smap.rowPtr(result.pos);
    smap.clearBitset(row);

    // Write each provided scalar field value via typed accessor
    for (0..num_vals) |vi| {
        smap.writeScalarField(result.pos, field_idxs[vi], col_ptrs, val_cols[vi], element_idx);
    }

    if (comptime delta_mode) {
        const meta_base = STATE_HEADER_SIZE + @as(u32, slot_idx) * SLOT_META_SIZE;
        state_base[meta_base + 14] |= 0x01;
    }
    return .{ .err = .OK, .pos = result.pos };
}

/// Write array field values into a struct map row's arena.
/// Each array field uses CSR encoding: offsets_col[child_idx] .. offsets_col[child_idx+1]
/// gives the range in values_col. Data is appended to the per-slot arena; (offset, length)
/// is written to the row's field slot.
/// Returns .ARENA_OVERFLOW if arena is full (triggers NEEDS_GROWTH).
fn writeStructMapArrayFields(
    state_base: [*]u8,
    slot_idx: u8,
    row_pos: u32,
    array_offsets_cols: []const u8,
    array_values_cols: []const u8,
    array_field_idxs: []const u8,
    num_array_vals: u8,
    col_ptrs: [*]const [*]const u8,
    child_idx: u32,
) ErrorCode {
    if (num_array_vals == 0) return .OK;

    const meta_base = STATE_HEADER_SIZE + @as(u32, slot_idx) * SLOT_META_SIZE;
    const slot_offset = std.mem.readInt(u32, state_base[meta_base..][0..4], .little);
    const capacity = std.mem.readInt(u32, state_base[meta_base + 4 ..][0..4], .little);
    const num_fields = state_base[meta_base + 13];
    const row_size: u32 = std.mem.readInt(u16, state_base[meta_base + 16 ..][0..2], .little);
    const arena_header_off = std.mem.readInt(u32, state_base[meta_base + 20 ..][0..4], .little);

    if (arena_header_off == 0) return .OK; // No arena (shouldn't happen if we have array fields)

    const field_types_ptr: [*]u8 = state_base + slot_offset;
    const descriptor_size = align8(@as(u32, num_fields));
    const keys_offset = slot_offset + descriptor_size;
    const rows_base = keys_offset + capacity * 4;
    const row_ptr = state_base + rows_base + row_pos * row_size;

    // Read arena state
    const arena_capacity = std.mem.readInt(u32, state_base[arena_header_off..][0..4], .little);
    var arena_used = std.mem.readInt(u32, state_base[arena_header_off + 4 ..][0..4], .little);
    const arena_data_base = arena_header_off + ARENA_HEADER_SIZE;

    for (0..num_array_vals) |vi| {
        const field_idx = array_field_idxs[vi];
        const field_type: StructFieldType = @enumFromInt(field_types_ptr[field_idx]);
        const f_offset = structFieldOffset(num_fields, field_types_ptr, field_idx);
        const elem_size = arenaElemSize(field_type);

        // Read CSR offsets to get array element range
        const offsets: [*]const u32 = @ptrCast(@alignCast(col_ptrs[array_offsets_cols[vi]]));
        const arr_start = offsets[child_idx];
        const arr_end = offsets[child_idx + 1];
        const arr_len = arr_end - arr_start;
        const byte_len = arr_len * elem_size;

        // Check arena capacity
        if (arena_used + byte_len > arena_capacity) {
            g_needs_growth_slot = slot_idx;
            return .ARENA_OVERFLOW;
        }

        // Set bit in bitset
        row_ptr[field_idx / 8] |= @as(u8, 1) << @as(u3, @truncate(field_idx % 8));

        // Write (offset, length) to row field
        std.mem.writeInt(u32, row_ptr[f_offset..][0..4], arena_used, .little);
        std.mem.writeInt(u32, row_ptr[f_offset + 4 ..][0..4], arr_len, .little);

        // Copy array elements to arena
        if (byte_len > 0) {
            const src_base = col_ptrs[array_values_cols[vi]];
            const src_off = arr_start * elem_size;
            const dst_off = arena_data_base + arena_used;
            @memcpy(state_base[dst_off .. dst_off + byte_len], src_base[src_off .. src_off + byte_len]);
        }

        arena_used += byte_len;
    }

    // Update arena_used
    std.mem.writeInt(u32, state_base[arena_header_off + 4 ..][0..4], arena_used, .little);
    return .OK;
}

// =============================================================================
// SIMD Aggregate Operations
// =============================================================================




// =============================================================================
// Masked SIMD Aggregates (for FOR_EACH_EVENT type filtering)
// =============================================================================






// =============================================================================
// Comptime aggregate dispatch — DRY handler for f64/i64 × sum/min/max
// =============================================================================

// AggKind moved to aggregates.zig
const AggKind = aggregates.AggKind;

const TypeMask = aggregates.TypeMask;

/// Reduce a column to a single value using SIMD for f64, scalar for i64.
/// type_mask: only include rows where type_data[i] == type_id (FOR_EACH_EVENT).
/// pred_col: only include rows where pred[i] != 0 (_IF variants).

/// Unified aggregate handler for main dispatch, FOR_EACH_EVENT, and _IF variants.
/// Uses SIMD for f64 when no predicate column; scalar otherwise.
fn execAgg(
    comptime T: type,
    comptime kind: AggKind,
    comptime delta_mode: bool,
    state_base: [*]u8,
    slot: u8,
    vals: [*]const T,
    batch_len: u32,
    type_mask: ?TypeMask,
    pred_col: ?[*]const u32,
) void {
    const meta = getSlotMeta(state_base, slot);
    const agg_ptr: *T = @ptrCast(@alignCast(state_base + meta.offset));
    const count_ptr: *u64 = @ptrCast(@alignCast(state_base + meta.offset + 8));
    const old_val = agg_ptr.*;

    const new_val = aggregates.reduceCol(T, kind, vals, batch_len, old_val, type_mask, pred_col);

    if (new_val != old_val) {
        if (g_undo_enabled) appendMutation(
            delta_mode,
            .{ .op = .AGG_UPDATE, .slot = slot, ._pad1 = 0, ._pad2 = 0, .key = 0, .prev_value = @truncate(count_ptr.*), .aux = @bitCast(old_val) },
            .{ .op = .AGG_UPDATE, .slot = slot, ._pad1 = 0, ._pad2 = 0, .key = 0, .prev_value = @truncate(count_ptr.*), .aux = @bitCast(new_val) },
        );
        agg_ptr.* = new_val;
        setChangeFlag(meta, ChangeFlag.SIZE_CHANGED);
    }
}

/// Read a column as [*]const T from the column pointer array.
fn getColAs(comptime T: type, col_ptrs: anytype, col: u8) [*]const T {
    return @ptrCast(@alignCast(col_ptrs[col]));
}

// =============================================================================
// Main Execution - NEW SIMPLIFIED API
// =============================================================================

/// Execute reduce bytecode on state buffer.
///
/// Parameters:
///   state_base: Pointer to state buffer (header + slot meta + slot data)
///   program_ptr: Pointer to program bytecode
///   program_len: Length of program bytecode
///   col_ptrs_ptr: Pointer to array of column data pointers
///   num_cols: Number of input columns
///   batch_len: Number of rows in batch
///
/// Returns: ErrorCode
fn executeBatchImpl(
    comptime delta_mode: bool,
    state_base: [*]u8,
    program_ptr: [*]const u8,
    program_len: u32,
    col_ptrs_ptr: [*]const [*]const u8,
    num_cols: u32,
    batch_len: u32,
) u32 {
    _ = num_cols;

    // Validate state header
    const state_magic: *const u32 = @ptrCast(@alignCast(state_base));
    if (state_magic.* != STATE_MAGIC) return @intFromEnum(ErrorCode.INVALID_STATE);

    // Validate program (layout: [0..31] hash, [32..45] content header, [46..] init+reduce)
    const program = program_ptr[0..program_len];
    if (program_len < PROGRAM_HEADER_SIZE) return @intFromEnum(ErrorCode.INVALID_PROGRAM);

    const content = program[PROGRAM_HASH_PREFIX..];
    const prog_magic = @as(u32, content[0]) | (@as(u32, content[1]) << 8) | (@as(u32, content[2]) << 16) | (@as(u32, content[3]) << 24);
    if (prog_magic != PROGRAM_MAGIC) return @intFromEnum(ErrorCode.INVALID_PROGRAM);

    // Parse header (content header is 14 bytes: magic(4) version(2) numSlots(1) numInputs(1) reserved(2) initLen(2) reduceLen(2))
    const init_len: u16 = @as(u16, content[10]) | (@as(u16, content[11]) << 8);
    const reduce_len: u16 = @as(u16, content[12]) | (@as(u16, content[13]) << 8);
    const code_len = init_len + reduce_len;
    if (PROGRAM_HEADER_SIZE + code_len > program_len) return @intFromEnum(ErrorCode.INVALID_PROGRAM);

    // Execute only reduce section (init section parsed by JS)
    const code = content[14 + init_len .. 14 + init_len + reduce_len];

    // Helper to get column as u32 array
    const getColU32 = struct {
        fn f(ptrs: [*]const [*]const u8, idx: u8) [*]const u32 {
            return @ptrCast(@alignCast(ptrs[idx]));
        }
    }.f;

    // Helper to get column as f64 array
    const getColF64 = struct {
        fn f(ptrs: [*]const [*]const u8, idx: u8) [*]const f64 {
            return @ptrCast(@alignCast(ptrs[idx]));
        }
    }.f;

    const getColI64 = struct {
        fn f(ptrs: [*]const [*]const u8, idx: u8) [*]const i64 {
            return @ptrCast(@alignCast(ptrs[idx]));
        }
    }.f;

    // Helper: convert CAPACITY_EXCEEDED to NEEDS_GROWTH, tracking the slot index
    const signalGrowth = struct {
        fn check(slot_idx: u8, result: ErrorCode) u32 {
            if (result == .CAPACITY_EXCEEDED) {
                g_needs_growth_slot = slot_idx;
                return @intFromEnum(ErrorCode.NEEDS_GROWTH);
            }
            return @intFromEnum(result);
        }
    }.check;

    var pc: usize = 0;
    while (pc < code.len) {
        const op: Opcode = @enumFromInt(code[pc]);
        pc += 1;

        switch (op) {
            .HALT => break,

            .BATCH_MAP_UPSERT_LATEST, .BATCH_MAP_UPSERT_LATEST_TTL => {
                const slot = code[pc];
                const key_col = code[pc + 1];
                const val_col = code[pc + 2];
                const ts_col = code[pc + 3];
                pc += 4;
                const meta = getSlotMeta(state_base, slot);
                const result = hashmap_ops.batchMapUpsert(.latest, delta_mode, state_base, meta, slot, getColU32(col_ptrs_ptr, key_col), getColU32(col_ptrs_ptr, val_col), getColF64(col_ptrs_ptr, ts_col), batch_len);
                if (result != .OK) return signalGrowth(slot, result);
            },

            .BATCH_MAP_UPSERT_FIRST => {
                const slot = code[pc];
                const key_col = code[pc + 1];
                const val_col = code[pc + 2];
                pc += 3;
                const meta = getSlotMeta(state_base, slot);
                const result = hashmap_ops.batchMapUpsert(.first, delta_mode, state_base, meta, slot, getColU32(col_ptrs_ptr, key_col), getColU32(col_ptrs_ptr, val_col), null, batch_len);
                if (result != .OK) return signalGrowth(slot, result);
            },

            .BATCH_MAP_UPSERT_LAST => {
                const slot = code[pc];
                const key_col = code[pc + 1];
                const val_col = code[pc + 2];
                pc += 3;
                const meta = getSlotMeta(state_base, slot);
                const result = hashmap_ops.batchMapUpsert(.last, delta_mode, state_base, meta, slot, getColU32(col_ptrs_ptr, key_col), getColU32(col_ptrs_ptr, val_col), if (meta.hasTTL()) getColF64(col_ptrs_ptr, meta.timestamp_field_idx) else null, batch_len);
                if (result != .OK) return signalGrowth(slot, result);
            },

            .BATCH_MAP_UPSERT_LAST_TTL => {
                const slot = code[pc];
                const key_col = code[pc + 1];
                const val_col = code[pc + 2];
                const ts_col = code[pc + 3];
                pc += 4;
                const meta = getSlotMeta(state_base, slot);
                const result = hashmap_ops.batchMapUpsert(.last, delta_mode, state_base, meta, slot, getColU32(col_ptrs_ptr, key_col), getColU32(col_ptrs_ptr, val_col), getColF64(col_ptrs_ptr, ts_col), batch_len);
                if (result != .OK) return signalGrowth(slot, result);
            },

            .BATCH_MAP_REMOVE => {
                const slot = code[pc];
                const key_col = code[pc + 1];
                pc += 2;
                const meta = getSlotMeta(state_base, slot);
                hashmap_ops.batchMapRemove(delta_mode, state_base, meta, slot, getColU32(col_ptrs_ptr, key_col), batch_len);
            },

            .BATCH_MAP_UPSERT_MAX => {
                const slot = code[pc];
                const key_col = code[pc + 1];
                const val_col = code[pc + 2];
                const cmp_col = code[pc + 3];
                pc += 4;
                const meta = getSlotMeta(state_base, slot);
                const result = hashmap_ops.batchMapUpsert(.max, delta_mode, state_base, meta, slot, getColU32(col_ptrs_ptr, key_col), getColU32(col_ptrs_ptr, val_col), getColF64(col_ptrs_ptr, cmp_col), batch_len);
                if (result != .OK) return signalGrowth(slot, result);
            },

            .BATCH_MAP_UPSERT_MIN => {
                const slot = code[pc];
                const key_col = code[pc + 1];
                const val_col = code[pc + 2];
                const cmp_col = code[pc + 3];
                pc += 4;
                const meta = getSlotMeta(state_base, slot);
                const result = hashmap_ops.batchMapUpsert(.min, delta_mode, state_base, meta,
                    slot,
                    getColU32(col_ptrs_ptr, key_col),
                    getColU32(col_ptrs_ptr, val_col),
                    getColF64(col_ptrs_ptr, cmp_col),
                    batch_len,
                );
                if (result != .OK) return signalGrowth(slot, result);
            },

            .BATCH_SET_INSERT => {
                const slot = code[pc];
                const elem_col = code[pc + 1];
                pc += 2;

                const meta = getSlotMeta(state_base, slot);
                const result = hashset_ops.batchSetInsert(
                    delta_mode,
                    state_base,
                    meta,
                    slot,
                    getColU32(col_ptrs_ptr, elem_col),
                    if (meta.hasTTL()) getColF64(col_ptrs_ptr, meta.timestamp_field_idx) else null,
                    batch_len,
                );
                if (result != .OK) return signalGrowth(slot, result);
            },

            .BATCH_SET_INSERT_TTL => {
                const slot = code[pc];
                const elem_col = code[pc + 1];
                const ts_col = code[pc + 2];
                pc += 3;

                const meta = getSlotMeta(state_base, slot);
                const result = hashset_ops.batchSetInsert(
                    delta_mode,
                    state_base,
                    meta,
                    slot,
                    getColU32(col_ptrs_ptr, elem_col),
                    getColF64(col_ptrs_ptr, ts_col),
                    batch_len,
                );
                if (result != .OK) return signalGrowth(slot, result);
            },

            .BATCH_SET_REMOVE => {
                const slot = code[pc];
                const elem_col = code[pc + 1];
                pc += 2;

                const meta = getSlotMeta(state_base, slot);
                hashset_ops.batchSetRemove(delta_mode, state_base, meta, slot, getColU32(col_ptrs_ptr, elem_col), batch_len);
            },

            .BATCH_BITMAP_ADD => {
                const slot = code[pc];
                const elem_col = code[pc + 1];
                pc += 2;

                const meta = getSlotMeta(state_base, slot);
                const result = batchBitmapAdd(
                    delta_mode,
                    state_base,
                    meta,
                    slot,
                    getColU32(col_ptrs_ptr, elem_col),
                    if (meta.hasTTL()) getColF64(col_ptrs_ptr, meta.timestamp_field_idx) else null,
                    batch_len,
                );
                if (result != .OK) return signalGrowth(slot, result);
            },

            .BATCH_BITMAP_REMOVE => {
                const slot = code[pc];
                const elem_col = code[pc + 1];
                pc += 2;

                const meta = getSlotMeta(state_base, slot);
                batchBitmapRemove(delta_mode, state_base, meta, slot, getColU32(col_ptrs_ptr, elem_col), batch_len);
            },

            // Bitmap in-place set algebra (slot × slot)
            .BATCH_BITMAP_AND, .BATCH_BITMAP_OR, .BATCH_BITMAP_ANDNOT, .BATCH_BITMAP_XOR => {
                const target_slot = code[pc];
                const source_slot = code[pc + 1];
                pc += 2;

                const target_meta = getSlotMeta(state_base, target_slot);
                const source_storage = getBitmapStorage(state_base, getSlotMeta(state_base, source_slot));
                const source_len = source_storage.serialized_len_ptr.*;
                const source_data: []const u8 = if (source_len > 0) source_storage.payload_ptr[0..source_len] else &[_]u8{};

                const result = switch (op) {
                    .BATCH_BITMAP_AND => batchBitmapAlgebra(.AND, state_base, target_meta, source_data),
                    .BATCH_BITMAP_OR => batchBitmapAlgebra(.OR, state_base, target_meta, source_data),
                    .BATCH_BITMAP_ANDNOT => batchBitmapAlgebra(.AND_NOT, state_base, target_meta, source_data),
                    .BATCH_BITMAP_XOR => batchBitmapAlgebra(.XOR, state_base, target_meta, source_data),
                    else => unreachable,
                };
                if (result == .CAPACITY_EXCEEDED) return signalGrowth(target_slot, result);
                if (result != .OK) return @intFromEnum(result);
            },

            // Bitmap in-place set algebra (slot × scratch)
            .BATCH_BITMAP_AND_SCRATCH, .BATCH_BITMAP_OR_SCRATCH, .BATCH_BITMAP_ANDNOT_SCRATCH, .BATCH_BITMAP_XOR_SCRATCH => {
                const target_slot = code[pc];
                pc += 1;

                const target_meta = getSlotMeta(state_base, target_slot);
                const source_data: []const u8 = if (g_algebra_result_len > 0)
                    @as([*]const u8, @ptrFromInt(g_algebra_result_ptr))[0..g_algebra_result_len]
                else
                    &[_]u8{};

                const result = switch (op) {
                    .BATCH_BITMAP_AND_SCRATCH => batchBitmapAlgebra(.AND, state_base, target_meta, source_data),
                    .BATCH_BITMAP_OR_SCRATCH => batchBitmapAlgebra(.OR, state_base, target_meta, source_data),
                    .BATCH_BITMAP_ANDNOT_SCRATCH => batchBitmapAlgebra(.AND_NOT, state_base, target_meta, source_data),
                    .BATCH_BITMAP_XOR_SCRATCH => batchBitmapAlgebra(.XOR, state_base, target_meta, source_data),
                    else => unreachable,
                };
                if (result == .CAPACITY_EXCEEDED) return signalGrowth(target_slot, result);
                if (result != .OK) return @intFromEnum(result);
            },

            .BATCH_AGG_SUM => {
                const slot = code[pc];
                const val_col = code[pc + 1];
                pc += 2;
                execAgg(f64, .sum, delta_mode, state_base, slot, getColAs(f64, col_ptrs_ptr, val_col), batch_len, null, null);
            },

            .BATCH_AGG_COUNT => {
                const slot = code[pc];
                pc += 1;

                const meta = getSlotMeta(state_base, slot);
                // COUNT slot is 8 bytes: u64 count at meta.offset directly
                const count_ptr: *u64 = @ptrCast(@alignCast(state_base + meta.offset));
                if (batch_len > 0) {
                    const prev_count = count_ptr.*;
                    const next_count = prev_count + batch_len;
                    if (g_undo_enabled) appendMutation(
                        delta_mode,
                        .{ .op = .COUNT_UPDATE, .slot = slot, ._pad1 = 0, ._pad2 = 0, .key = 0, .prev_value = @truncate(prev_count), .aux = 0 },
                        .{ .op = .COUNT_UPDATE, .slot = slot, ._pad1 = 0, ._pad2 = 0, .key = 0, .prev_value = @truncate(next_count), .aux = 0 },
                    );
                    count_ptr.* = next_count;
                    setChangeFlag(meta, ChangeFlag.SIZE_CHANGED);
                }
            },

            .BATCH_AGG_MIN => {
                const slot = code[pc];
                const val_col = code[pc + 1];
                pc += 2;
                execAgg(f64, .min, delta_mode, state_base, slot, getColAs(f64, col_ptrs_ptr, val_col), batch_len, null, null);
            },

            .BATCH_AGG_MAX => {
                const slot = code[pc];
                const val_col = code[pc + 1];
                pc += 2;
                execAgg(f64, .max, delta_mode, state_base, slot, getColAs(f64, col_ptrs_ptr, val_col), batch_len, null, null);
            },

            // i64 aggregate ops — delegate to comptime-generic execAgg
            .BATCH_AGG_SUM_I64 => {
                const slot = code[pc];
                const val_col = code[pc + 1];
                pc += 2;
                execAgg(i64, .sum, delta_mode, state_base, slot, getColAs(i64, col_ptrs_ptr, val_col), batch_len, null, null);
            },
            .BATCH_AGG_MIN_I64 => {
                const slot = code[pc];
                const val_col = code[pc + 1];
                pc += 2;
                execAgg(i64, .min, delta_mode, state_base, slot, getColAs(i64, col_ptrs_ptr, val_col), batch_len, null, null);
            },
            .BATCH_AGG_MAX_I64 => {
                const slot = code[pc];
                const val_col = code[pc + 1];
                pc += 2;
                execAgg(i64, .max, delta_mode, state_base, slot, getColAs(i64, col_ptrs_ptr, val_col), batch_len, null, null);
            },

            .BATCH_SCALAR_LATEST => {
                // Store value from event with highest comparison timestamp.
                // AggType subtype determines how the 8-byte value field is interpreted.
                const slot = code[pc];
                const val_col = code[pc + 1];
                const cmp_col = code[pc + 2];
                pc += 3;

                const meta = getSlotMeta(state_base, slot);
                const data_base = state_base + meta.offset;
                const cmp_ptr: *f64 = @ptrCast(@alignCast(data_base + 8));
                const cmp_vals = getColF64(col_ptrs_ptr, cmp_col);

                const scalar_type = meta.agg_type;
                switch (scalar_type) {
                    .SCALAR_U32 => {
                        const val_ptr: *u32 = @ptrCast(@alignCast(data_base));
                        const val_vals = getColU32(col_ptrs_ptr, val_col);
                        var i: u32 = 0;
                        while (i < batch_len) : (i += 1) {
                            const ts = cmp_vals[i];
                            if (ts > cmp_ptr.* and val_vals[i] != EMPTY_KEY) {
                                val_ptr.* = val_vals[i];
                                cmp_ptr.* = ts;
                                setChangeFlag(meta, ChangeFlag.UPDATED);
                            }
                        }
                    },
                    .SCALAR_F64 => {
                        const val_ptr: *f64 = @ptrCast(@alignCast(data_base));
                        const val_vals = getColF64(col_ptrs_ptr, val_col);
                        var i: u32 = 0;
                        while (i < batch_len) : (i += 1) {
                            const ts = cmp_vals[i];
                            if (ts > cmp_ptr.*) {
                                val_ptr.* = val_vals[i];
                                cmp_ptr.* = ts;
                                setChangeFlag(meta, ChangeFlag.UPDATED);
                            }
                        }
                    },
                    .SCALAR_I64 => {
                        const val_ptr: *i64 = @ptrCast(@alignCast(data_base));
                        const val_vals = getColI64(col_ptrs_ptr, val_col);
                        var i: u32 = 0;
                        while (i < batch_len) : (i += 1) {
                            const ts = cmp_vals[i];
                            if (ts > cmp_ptr.*) {
                                val_ptr.* = val_vals[i];
                                cmp_ptr.* = ts;
                                setChangeFlag(meta, ChangeFlag.UPDATED);
                            }
                        }
                    },
                    else => return @intFromEnum(ErrorCode.INVALID_PROGRAM),
                }
            },

            .BATCH_STRUCT_MAP_UPSERT_LAST => {
                const slot = code[pc];
                const key_col = code[pc + 1];
                const num_vals = code[pc + 2];
                pc += 3;

                // Read (val_col, field_idx) pairs — max 32 fields
                var val_cols: [32]u8 = undefined;
                var field_idxs: [32]u8 = undefined;
                for (0..num_vals) |vi| {
                    val_cols[vi] = code[pc];
                    field_idxs[vi] = code[pc + 1];
                    pc += 2;
                }

                // Skip array field section (not supported in batch path)
                const num_array_vals_batch = code[pc];
                pc += 1 + @as(usize, num_array_vals_batch) * 3;

                const meta_base = STATE_HEADER_SIZE + @as(u32, slot) * SLOT_META_SIZE;
                const slot_offset = std.mem.readInt(u32, state_base[meta_base..][0..4], .little);
                const capacity = std.mem.readInt(u32, state_base[meta_base + 4 ..][0..4], .little);
                var current_size = std.mem.readInt(u32, state_base[meta_base + 8 ..][0..4], .little);
                const num_fields = state_base[meta_base + 13];
                const bitset_bytes_val: u32 = state_base[meta_base + 15];
                const row_size: u32 = std.mem.readInt(u16, state_base[meta_base + 16 ..][0..2], .little);

                // Read field types from slot data prefix
                const field_types_ptr: [*]u8 = state_base + slot_offset;
                const descriptor_size = align8(@as(u32, num_fields));

                const keys_offset = slot_offset + descriptor_size;
                const keys: [*]u32 = @ptrCast(@alignCast(&state_base[keys_offset]));
                const rows_base = keys_offset + capacity * 4;

                const max_size = (capacity * 7) / 10;

                const key_data = getColU32(col_ptrs_ptr, key_col);

                var i: u32 = 0;
                while (i < batch_len) : (i += 1) {
                    const key = key_data[i];
                    if (key == EMPTY_KEY or key == TOMBSTONE) continue;

                    // Hash probe
                    var pos = hashKey(key, capacity);
                    var found = false;
                    while (true) {
                        const k = keys[pos];
                        if (k == EMPTY_KEY) break; // insert at empty
                        if (k == key) {
                            found = true;
                            break;
                        } // update existing
                        if (k == TOMBSTONE) break; // reuse tombstone
                        pos = (pos + 1) & (capacity - 1);
                    }

                    if (!found) {
                        if (current_size >= max_size) {
                            // Commit progress so far
                            std.mem.writeInt(u32, state_base[meta_base + 8 ..][0..4], current_size, .little);
                            if (comptime delta_mode) {
                                state_base[meta_base + 14] |= 0x01;
                            }
                            g_needs_growth_slot = slot;
                            return @intFromEnum(ErrorCode.NEEDS_GROWTH);
                        }
                        keys[pos] = key;
                        current_size += 1;
                    }

                    // Write row data
                    const row_ptr = state_base + rows_base + pos * row_size;

                    // Clear bitset
                    @memset(row_ptr[0..bitset_bytes_val], 0);

                    // Write each provided field value
                    for (0..num_vals) |vi| {
                        const field_idx = field_idxs[vi];
                        const field_type: StructFieldType = @enumFromInt(field_types_ptr[field_idx]);
                        const f_offset = structFieldOffset(num_fields, field_types_ptr, field_idx);

                        // Set bit in bitset
                        row_ptr[field_idx / 8] |= @as(u8, 1) << @as(u3, @truncate(field_idx % 8));

                        // Write value based on field type
                        switch (field_type) {
                            .UINT32, .STRING => {
                                const col = getColU32(col_ptrs_ptr, val_cols[vi]);
                                std.mem.writeInt(u32, row_ptr[f_offset..][0..4], col[i], .little);
                            },
                            .INT64 => {
                                const col: [*]const u64 = @ptrCast(@alignCast(col_ptrs_ptr[val_cols[vi]]));
                                std.mem.writeInt(u64, row_ptr[f_offset..][0..8], col[i], .little);
                            },
                            .FLOAT64 => {
                                const col: [*]const f64 = @ptrCast(@alignCast(col_ptrs_ptr[val_cols[vi]]));
                                const bits: u64 = @bitCast(col[i]);
                                std.mem.writeInt(u64, row_ptr[f_offset..][0..8], bits, .little);
                            },
                            .BOOL => {
                                const col = getColU32(col_ptrs_ptr, val_cols[vi]);
                                row_ptr[f_offset] = if (col[i] != 0) 1 else 0;
                            },
                            // Array fields handled via block-based path (FOR_EACH_EVENT + writeStructMapArrayFields)
                            .ARRAY_U32, .ARRAY_I64, .ARRAY_F64, .ARRAY_STRING, .ARRAY_BOOL => {},
                        }
                    }
                }

                // Commit size and change flags
                std.mem.writeInt(u32, state_base[meta_base + 8 ..][0..4], current_size, .little);
                if (comptime delta_mode) {
                    state_base[meta_base + 14] |= 0x01; // change_flags
                }
            },

            .FOR_EACH_EVENT => {
                // Header: type_col:u8, type_id:u32(LE), body_len:u16(LE)
                const type_col = code[pc];
                const type_id = @as(u32, code[pc + 1]) |
                    (@as(u32, code[pc + 2]) << 8) |
                    (@as(u32, code[pc + 3]) << 16) |
                    (@as(u32, code[pc + 4]) << 24);
                const body_len = @as(u16, code[pc + 5]) | (@as(u16, code[pc + 6]) << 8);
                pc += 7;

                const body = code[pc .. pc + body_len];
                pc += body_len;

                const type_data = getColU32(col_ptrs_ptr, type_col);

                // Pass 1: batch-level aggregates with type mask (SIMD)
                const agg_result = executeBatchAggregates(
                    delta_mode,
                    state_base,
                    body,
                    col_ptrs_ptr,
                    batch_len,
                    type_data,
                    type_id,
                );
                if (agg_result != @intFromEnum(ErrorCode.OK)) return agg_result;

                // Pass 2: per-element scalar operations (hash/struct/set)
                var ei: u32 = 0;
                while (ei < batch_len) : (ei += 1) {
                    if (type_data[ei] != type_id) continue;
                    const elem_result = executeElementOpcodes(
                        delta_mode,
                        state_base,
                        body,
                        col_ptrs_ptr,
                        ei, // child_idx
                        ei, // parent_idx (same at top level)
                        0xFF, // no parent_ts_col at top level
                    );
                    if (elem_result != @intFromEnum(ErrorCode.OK)) return elem_result;
                }
            },

            else => return @intFromEnum(ErrorCode.INVALID_PROGRAM),
        }
    }

    return @intFromEnum(ErrorCode.OK);
}

// =============================================================================
// Block-based execution helpers (FOR_EACH_EVENT / FLAT_MAP)
// =============================================================================

/// Returns true if the opcode byte is an aggregate op (0x40-0x4F).
inline fn isAggregateOp(op_byte: u8) bool {
    return (op_byte & 0xF0) == 0x40;
}

/// Returns the bytecode length (including opcode) for an aggregate op.
inline fn aggOpLen(op_byte: u8) u32 {
    return switch (op_byte) {
        0x40 => 3, // AGG_SUM: opcode + slot + val_col
        0x41 => 2, // AGG_COUNT: opcode + slot
        0x42 => 3, // AGG_MIN: opcode + slot + val_col
        0x43 => 3, // AGG_MAX: opcode + slot + val_col
        0x44 => 4, // AGG_SUM_IF: opcode + slot + val_col + pred_col
        0x45 => 3, // AGG_COUNT_IF: opcode + slot + pred_col
        0x46 => 4, // AGG_MIN_IF: opcode + slot + val_col + pred_col
        0x47 => 4, // AGG_MAX_IF: opcode + slot + val_col + pred_col
        0x48 => 4, // BATCH_SCALAR_LATEST: opcode + slot + val_col + cmp_col
        0x49 => 3, // BATCH_AGG_SUM_I64: opcode + slot + val_col
        0x4a => 3, // BATCH_AGG_MIN_I64: opcode + slot + val_col
        0x4b => 3, // BATCH_AGG_MAX_I64: opcode + slot + val_col
        else => 2, // conservative fallback
    };
}

/// Returns the bytecode length (including opcode) for a non-aggregate body
/// opcode.  Used to skip non-agg ops in the aggregate pass.
inline fn bodyOpLen(code: []const u8, pc_offset: usize) u32 {
    const op_byte = code[pc_offset];
    return switch (op_byte) {
        // MAP ops
        0x20 => 5, // MAP_UPSERT_LATEST: op + slot + key_col + val_col + ts_col
        0x21 => 4, // MAP_UPSERT_FIRST: op + slot + key_col + val_col
        0x22 => 4, // MAP_UPSERT_LAST: op + slot + key_col + val_col
        0x23 => 3, // MAP_REMOVE: op + slot + key_col
        0x24 => 5, // MAP_UPSERT_LATEST_TTL
        0x25 => 5, // MAP_UPSERT_LAST_TTL
        0x26 => 5, // MAP_UPSERT_MAX: op + slot + key_col + val_col + cmp_col
        0x27 => 5, // MAP_UPSERT_MIN: op + slot + key_col + val_col + cmp_col
        0x28 => 6, // MAP_UPSERT_LATEST_IF: op + slot + key_col + val_col + ts_col + pred_col
        0x29 => 5, // MAP_UPSERT_FIRST_IF: op + slot + key_col + val_col + pred_col
        0x2A => 5, // MAP_UPSERT_LAST_IF: op + slot + key_col + val_col + pred_col
        0x2B => 4, // MAP_REMOVE_IF: op + slot + key_col + pred_col
        0x2C => 6, // MAP_UPSERT_MAX_IF: op + slot + key_col + val_col + cmp_col + pred_col
        0x2D => 6, // MAP_UPSERT_MIN_IF: op + slot + key_col + val_col + cmp_col + pred_col
        // SET ops
        0x30 => 3, // SET_INSERT: op + slot + elem_col
        0x31 => 3, // SET_REMOVE: op + slot + elem_col
        0x32 => 4, // SET_INSERT_TTL: op + slot + elem_col + ts_col
        0x33 => 4, // SET_INSERT_IF: op + slot + elem_col + pred_col
        // AGG ops (shouldn't be called here but handle anyway)
        0x40 => 3, // AGG_SUM
        0x41 => 2, // AGG_COUNT
        0x42 => 3, // AGG_MIN
        0x43 => 3, // AGG_MAX
        0x44 => 4, // AGG_SUM_IF
        0x45 => 3, // AGG_COUNT_IF
        0x46 => 4, // AGG_MIN_IF
        0x47 => 4, // AGG_MAX_IF
        0x48 => 4, // SCALAR_LATEST: op + slot + val_col + cmp_col
        0x49 => 3, // AGG_SUM_I64: op + slot + val_col
        0x4a => 3, // AGG_MIN_I64: op + slot + val_col
        0x4b => 3, // AGG_MAX_I64: op + slot + val_col
        // STRUCT_MAP_UPSERT_LAST: variable length (scalar fields + optional array fields)
        0x80 => blk: {
            // op + slot + key_col + num_vals + (val_col + field_idx) × num_vals
            // + num_array_vals + (offsets_col + values_col + field_idx) × num_array_vals
            const num_vals: u32 = code[pc_offset + 3];
            const scalar_end: u32 = 4 + num_vals * 2;
            const num_array_vals: u32 = code[pc_offset + scalar_end];
            break :blk scalar_end + 1 + num_array_vals * 3;
        },
        // LIST_APPEND: op + slot + val_col
        0x84 => 3,
        // LIST_APPEND_STRUCT: variable length
        0x85 => blk: {
            // op + slot + num_vals + num_vals * 2
            const num_vals: u32 = code[pc_offset + 2];
            break :blk 3 + num_vals * 2;
        },
        // FLAT_MAP: variable length (includes inner body)
        0xE1 => blk: {
            // op + offsets_col + parent_ts_col + inner_body_len(u16LE) + inner_body
            const inner_len = @as(u32, code[pc_offset + 3]) | (@as(u32, code[pc_offset + 4]) << 8);
            break :blk 5 + inner_len;
        },
        // NESTED_SET_INSERT: op + slot + outer_key_col + elem_col
        0x90 => 4,
        // NESTED_MAP_UPSERT_LAST: op + slot + outer_key_col + inner_key_col + val_col
        0x92 => 5,
        // NESTED_AGG_UPDATE: op + slot + outer_key_col + val_col
        0x95 => 4,
        else => 1, // unknown — advance 1 byte to avoid infinite loop
    };
}

/// Pass 1: scan body bytecode for aggregate ops and execute them with type-
/// masked SIMD.  Non-aggregate ops are skipped.
fn executeBatchAggregates(
    comptime delta_mode: bool,
    state_base: [*]u8,
    body: []const u8,
    col_ptrs: [*]const [*]const u8,
    batch_len: u32,
    type_data: [*]const u32,
    type_id: u32,
) u32 {
    const getColF64Inner = struct {
        fn f(ptrs: [*]const [*]const u8, idx: u8) [*]const f64 {
            return @ptrCast(@alignCast(ptrs[idx]));
        }
    }.f;

    var bpc: usize = 0;
    while (bpc < body.len) {
        const op_byte = body[bpc];

        if (!isAggregateOp(op_byte)) {
            // Skip non-aggregate ops
            bpc += bodyOpLen(body, bpc);
            continue;
        }

        switch (op_byte) {
            0x40 => { // AGG_SUM (f64 SIMD via maskedAggSum)
                const slot = body[bpc + 1];
                const val_col = body[bpc + 2];
                bpc += 3;
                execAgg(f64, .sum, delta_mode, state_base, slot, getColAs(f64, col_ptrs, val_col), batch_len, .{ .data = type_data, .id = type_id }, null);
            },
            0x41 => { // AGG_COUNT — 8-byte slot (u64 at meta.offset)
                const slot = body[bpc + 1];
                bpc += 2;

                const meta = getSlotMeta(state_base, slot);
                const count_ptr: *u64 = @ptrCast(@alignCast(state_base + meta.offset));
                const matched = aggregates.maskedAggCount(type_data, type_id, batch_len);
                if (matched > 0) {
                    const prev_count = count_ptr.*;
                    const next_count = prev_count + matched;
                    if (g_undo_enabled) appendMutation(
                        delta_mode,
                        .{ .op = .COUNT_UPDATE, .slot = slot, ._pad1 = 0, ._pad2 = 0, .key = 0, .prev_value = @truncate(prev_count), .aux = 0 },
                        .{ .op = .COUNT_UPDATE, .slot = slot, ._pad1 = 0, ._pad2 = 0, .key = 0, .prev_value = @truncate(next_count), .aux = 0 },
                    );
                    count_ptr.* = next_count;
                    setChangeFlag(meta, ChangeFlag.SIZE_CHANGED);
                }
            },
            0x42 => { // AGG_MIN (f64 SIMD via maskedAggMin)
                const slot = body[bpc + 1];
                const val_col = body[bpc + 2];
                bpc += 3;
                execAgg(f64, .min, delta_mode, state_base, slot, getColAs(f64, col_ptrs, val_col), batch_len, .{ .data = type_data, .id = type_id }, null);
            },
            0x43 => { // AGG_MAX (f64 SIMD via maskedAggMax)
                const slot = body[bpc + 1];
                const val_col = body[bpc + 2];
                bpc += 3;
                execAgg(f64, .max, delta_mode, state_base, slot, getColAs(f64, col_ptrs, val_col), batch_len, .{ .data = type_data, .id = type_id }, null);
            },
            0x44 => { // AGG_SUM_IF
                const slot = body[bpc + 1];
                const val_col = body[bpc + 2];
                const pred_col = body[bpc + 3];
                bpc += 4;
                execAgg(f64, .sum, delta_mode, state_base, slot, getColAs(f64, col_ptrs, val_col), batch_len, .{ .data = type_data, .id = type_id }, getColAs(u32, col_ptrs, pred_col));
            },
            0x45 => { // AGG_COUNT_IF — 8-byte slot (u64 at meta.offset)
                const slot = body[bpc + 1];
                const pred_col = body[bpc + 2];
                bpc += 3;

                const meta = getSlotMeta(state_base, slot);
                const count_ptr: *u64 = @ptrCast(@alignCast(state_base + meta.offset));
                const preds = @as([*]const u32, @ptrCast(@alignCast(col_ptrs[pred_col])));
                var matched: u64 = 0;
                var i: u32 = 0;
                while (i < batch_len) : (i += 1) {
                    if (type_data[i] == type_id and preds[i] != 0) matched += 1;
                }
                if (matched > 0) {
                    const prev_count = count_ptr.*;
                    const next_count = prev_count + matched;
                    if (g_undo_enabled) appendMutation(
                        delta_mode,
                        .{ .op = .COUNT_UPDATE, .slot = slot, ._pad1 = 0, ._pad2 = 0, .key = 0, .prev_value = @truncate(prev_count), .aux = 0 },
                        .{ .op = .COUNT_UPDATE, .slot = slot, ._pad1 = 0, ._pad2 = 0, .key = 0, .prev_value = @truncate(next_count), .aux = 0 },
                    );
                    count_ptr.* = next_count;
                    setChangeFlag(meta, ChangeFlag.SIZE_CHANGED);
                }
            },
            0x46 => { // AGG_MIN_IF
                const slot = body[bpc + 1];
                const val_col = body[bpc + 2];
                const pred_col = body[bpc + 3];
                bpc += 4;
                execAgg(f64, .min, delta_mode, state_base, slot, getColAs(f64, col_ptrs, val_col), batch_len, .{ .data = type_data, .id = type_id }, getColAs(u32, col_ptrs, pred_col));
            },
            0x47 => { // AGG_MAX_IF
                const slot = body[bpc + 1];
                const val_col = body[bpc + 2];
                const pred_col = body[bpc + 3];
                bpc += 4;
                execAgg(f64, .max, delta_mode, state_base, slot, getColAs(f64, col_ptrs, val_col), batch_len, .{ .data = type_data, .id = type_id }, getColAs(u32, col_ptrs, pred_col));
            },
            0x48 => { // BATCH_SCALAR_LATEST — store value from event with highest comparison timestamp
                const slot = body[bpc + 1];
                const val_col = body[bpc + 2];
                const cmp_col = body[bpc + 3];
                bpc += 4;

                const meta = getSlotMeta(state_base, slot);
                const data_base = state_base + meta.offset;
                const cmp_ptr: *f64 = @ptrCast(@alignCast(data_base + 8));
                const cmp_vals = getColF64Inner(col_ptrs, cmp_col);

                const scalar_type = meta.agg_type;
                switch (scalar_type) {
                    .SCALAR_U32 => {
                        const val_ptr: *u32 = @ptrCast(@alignCast(data_base));
                        const val_vals: [*]const u32 = @ptrCast(@alignCast(col_ptrs[val_col]));
                        var i: u32 = 0;
                        while (i < batch_len) : (i += 1) {
                            if (type_data[i] == type_id and cmp_vals[i] > cmp_ptr.* and val_vals[i] != EMPTY_KEY) {
                                val_ptr.* = val_vals[i];
                                cmp_ptr.* = cmp_vals[i];
                                setChangeFlag(meta, ChangeFlag.UPDATED);
                            }
                        }
                    },
                    .SCALAR_F64 => {
                        const val_ptr: *f64 = @ptrCast(@alignCast(data_base));
                        const val_vals = getColF64Inner(col_ptrs, val_col);
                        var i: u32 = 0;
                        while (i < batch_len) : (i += 1) {
                            if (type_data[i] == type_id and cmp_vals[i] > cmp_ptr.*) {
                                val_ptr.* = val_vals[i];
                                cmp_ptr.* = cmp_vals[i];
                                setChangeFlag(meta, ChangeFlag.UPDATED);
                            }
                        }
                    },
                    .SCALAR_I64 => {
                        const val_ptr: *i64 = @ptrCast(@alignCast(data_base));
                        const val_vals: [*]const i64 = @ptrCast(@alignCast(col_ptrs[val_col]));
                        var i: u32 = 0;
                        while (i < batch_len) : (i += 1) {
                            if (type_data[i] == type_id and cmp_vals[i] > cmp_ptr.*) {
                                val_ptr.* = val_vals[i];
                                cmp_ptr.* = cmp_vals[i];
                                setChangeFlag(meta, ChangeFlag.UPDATED);
                            }
                        }
                    },
                    else => {},
                }
            },
            // i64 aggregate ops — delegate to comptime-generic execAgg with type mask
            0x49 => { // BATCH_AGG_SUM_I64
                const slot = body[bpc + 1];
                const val_col = body[bpc + 2];
                bpc += 3;
                execAgg(i64, .sum, delta_mode, state_base, slot, getColAs(i64, col_ptrs, val_col), batch_len, .{ .data = type_data, .id = type_id }, null);
            },
            0x4a => { // BATCH_AGG_MIN_I64
                const slot = body[bpc + 1];
                const val_col = body[bpc + 2];
                bpc += 3;
                execAgg(i64, .min, delta_mode, state_base, slot, getColAs(i64, col_ptrs, val_col), batch_len, .{ .data = type_data, .id = type_id }, null);
            },
            0x4b => { // BATCH_AGG_MAX_I64
                const slot = body[bpc + 1];
                const val_col = body[bpc + 2];
                bpc += 3;
                execAgg(i64, .max, delta_mode, state_base, slot, getColAs(i64, col_ptrs, val_col), batch_len, .{ .data = type_data, .id = type_id }, null);
            },
            else => {
                bpc += aggOpLen(op_byte);
            },
        }
    }
    return @intFromEnum(ErrorCode.OK);
}

/// Pass 2: execute non-aggregate body opcodes for a single element.
/// Handles FLAT_MAP nesting recursively.
fn executeElementOpcodes(
    comptime delta_mode: bool,
    state_base: [*]u8,
    body: []const u8,
    col_ptrs: [*]const [*]const u8,
    child_idx: u32,
    parent_idx: u32,
    parent_ts_col: u8, // 0xFF = no parent timestamp; otherwise column index for parent event timestamp
) u32 {
    const getColU32Inner = struct {
        fn f(ptrs: [*]const [*]const u8, idx: u8) [*]const u32 {
            return @ptrCast(@alignCast(ptrs[idx]));
        }
    }.f;
    const getColF64Inner = struct {
        fn f(ptrs: [*]const [*]const u8, idx: u8) [*]const f64 {
            return @ptrCast(@alignCast(ptrs[idx]));
        }
    }.f;

    var bpc: usize = 0;
    while (bpc < body.len) {
        const op_byte = body[bpc];

        // Skip aggregate ops (handled in batch pass)
        if (isAggregateOp(op_byte)) {
            bpc += aggOpLen(op_byte);
            continue;
        }

        switch (op_byte) {
            // MAP_UPSERT_LATEST (0x20): slot, key_col, val_col, ts_col
            0x20 => {
                const slot = body[bpc + 1];
                const key_col_idx = body[bpc + 2];
                const val_col_idx = body[bpc + 3];
                const ts_col_idx = body[bpc + 4];
                bpc += 5;

                // Inside FLAT_MAP with parent_ts_col set: use parent event's timestamp
                const ts = if (parent_ts_col != 0xFF)
                    getColF64Inner(col_ptrs, parent_ts_col)[parent_idx]
                else
                    getColF64Inner(col_ptrs, ts_col_idx)[child_idx];

                const meta = getSlotMeta(state_base, slot);
                const result = hashmap_ops.singleMapUpsert(.latest,
                    delta_mode,
                    state_base,
                    meta,
                    slot,
                    getColU32Inner(col_ptrs, key_col_idx)[child_idx],
                    getColU32Inner(col_ptrs, val_col_idx)[child_idx],
                    ts,
                );
                if (result == .CAPACITY_EXCEEDED) {
                    g_needs_growth_slot = slot;
                    return @intFromEnum(ErrorCode.NEEDS_GROWTH);
                }
            },

            // MAP_UPSERT_FIRST (0x21): slot, key_col, val_col
            0x21 => {
                const slot = body[bpc + 1];
                const key_col_idx = body[bpc + 2];
                const val_col_idx = body[bpc + 3];
                bpc += 4;

                const meta = getSlotMeta(state_base, slot);
                const result = hashmap_ops.singleMapUpsert(.first,
                    delta_mode,
                    state_base,
                    meta,
                    slot,
                    getColU32Inner(col_ptrs, key_col_idx)[child_idx],
                    getColU32Inner(col_ptrs, val_col_idx)[child_idx],
                    0,
                );
                if (result == .CAPACITY_EXCEEDED) {
                    g_needs_growth_slot = slot;
                    return @intFromEnum(ErrorCode.NEEDS_GROWTH);
                }
            },

            // MAP_UPSERT_LAST (0x22): slot, key_col, val_col
            0x22 => {
                const slot = body[bpc + 1];
                const key_col_idx = body[bpc + 2];
                const val_col_idx = body[bpc + 3];
                bpc += 4;

                const meta = getSlotMeta(state_base, slot);
                const result = hashmap_ops.singleMapUpsert(.last,
                    delta_mode,
                    state_base,
                    meta,
                    slot,
                    getColU32Inner(col_ptrs, key_col_idx)[child_idx],
                    getColU32Inner(col_ptrs, val_col_idx)[child_idx],
                    if (meta.hasTTL()) getColF64Inner(col_ptrs, meta.timestamp_field_idx)[child_idx] else 0,
                );
                if (result == .CAPACITY_EXCEEDED) {
                    g_needs_growth_slot = slot;
                    return @intFromEnum(ErrorCode.NEEDS_GROWTH);
                }
            },

            // MAP_UPSERT_LATEST_TTL (0x24): slot, key_col, val_col, ts_col
            0x24 => {
                const slot = body[bpc + 1];
                const key_col_idx = body[bpc + 2];
                const val_col_idx = body[bpc + 3];
                const ts_col_idx = body[bpc + 4];
                bpc += 5;

                const ts = if (parent_ts_col != 0xFF)
                    getColF64Inner(col_ptrs, parent_ts_col)[parent_idx]
                else
                    getColF64Inner(col_ptrs, ts_col_idx)[child_idx];

                const meta = getSlotMeta(state_base, slot);
                const result = hashmap_ops.singleMapUpsert(.latest,
                    delta_mode,
                    state_base,
                    meta,
                    slot,
                    getColU32Inner(col_ptrs, key_col_idx)[child_idx],
                    getColU32Inner(col_ptrs, val_col_idx)[child_idx],
                    ts,
                );
                if (result == .CAPACITY_EXCEEDED) {
                    g_needs_growth_slot = slot;
                    return @intFromEnum(ErrorCode.NEEDS_GROWTH);
                }
            },

            // MAP_UPSERT_LAST_TTL (0x25): slot, key_col, val_col, ts_col
            0x25 => {
                const slot = body[bpc + 1];
                const key_col_idx = body[bpc + 2];
                const val_col_idx = body[bpc + 3];
                const ts_col_idx = body[bpc + 4];
                bpc += 5;

                const ts = if (parent_ts_col != 0xFF)
                    getColF64Inner(col_ptrs, parent_ts_col)[parent_idx]
                else
                    getColF64Inner(col_ptrs, ts_col_idx)[child_idx];

                const meta = getSlotMeta(state_base, slot);
                const result = hashmap_ops.singleMapUpsert(.last,
                    delta_mode,
                    state_base,
                    meta,
                    slot,
                    getColU32Inner(col_ptrs, key_col_idx)[child_idx],
                    getColU32Inner(col_ptrs, val_col_idx)[child_idx],
                    ts,
                );
                if (result == .CAPACITY_EXCEEDED) {
                    g_needs_growth_slot = slot;
                    return @intFromEnum(ErrorCode.NEEDS_GROWTH);
                }
            },

            // MAP_REMOVE (0x23): slot, key_col
            0x23 => {
                const slot = body[bpc + 1];
                const key_col_idx = body[bpc + 2];
                bpc += 3;

                const meta = getSlotMeta(state_base, slot);
                hashmap_ops.singleMapRemove(
                    delta_mode,
                    state_base,
                    meta,
                    slot,
                    getColU32Inner(col_ptrs, key_col_idx)[child_idx],
                );
            },

            // MAP_UPSERT_MAX (0x26): slot, key_col, val_col, cmp_col
            0x26 => {
                const slot = body[bpc + 1];
                const key_col_idx = body[bpc + 2];
                const val_col_idx = body[bpc + 3];
                const cmp_col_idx = body[bpc + 4];
                bpc += 5;

                const meta = getSlotMeta(state_base, slot);
                const result = hashmap_ops.singleMapUpsert(.max,
                    delta_mode,
                    state_base,
                    meta,
                    slot,
                    getColU32Inner(col_ptrs, key_col_idx)[child_idx],
                    getColU32Inner(col_ptrs, val_col_idx)[child_idx],
                    getColF64Inner(col_ptrs, cmp_col_idx)[child_idx],
                );
                if (result == .CAPACITY_EXCEEDED) {
                    g_needs_growth_slot = slot;
                    return @intFromEnum(ErrorCode.NEEDS_GROWTH);
                }
            },

            // MAP_UPSERT_MIN (0x27): slot, key_col, val_col, cmp_col
            0x27 => {
                const slot = body[bpc + 1];
                const key_col_idx = body[bpc + 2];
                const val_col_idx = body[bpc + 3];
                const cmp_col_idx = body[bpc + 4];
                bpc += 5;

                const meta = getSlotMeta(state_base, slot);
                const result = hashmap_ops.singleMapUpsert(.min,
                    delta_mode,
                    state_base,
                    meta,
                    slot,
                    getColU32Inner(col_ptrs, key_col_idx)[child_idx],
                    getColU32Inner(col_ptrs, val_col_idx)[child_idx],
                    getColF64Inner(col_ptrs, cmp_col_idx)[child_idx],
                );
                if (result == .CAPACITY_EXCEEDED) {
                    g_needs_growth_slot = slot;
                    return @intFromEnum(ErrorCode.NEEDS_GROWTH);
                }
            },

            // MAP_UPSERT_LATEST_IF (0x28): slot, key_col, val_col, ts_col, pred_col
            0x28 => {
                const slot = body[bpc + 1];
                const key_col_idx = body[bpc + 2];
                const val_col_idx = body[bpc + 3];
                const ts_col_idx = body[bpc + 4];
                const pred_col_idx = body[bpc + 5];
                bpc += 6;

                if (getColU32Inner(col_ptrs, pred_col_idx)[child_idx] == 0) continue;

                const ts = if (parent_ts_col != 0xFF)
                    getColF64Inner(col_ptrs, parent_ts_col)[parent_idx]
                else
                    getColF64Inner(col_ptrs, ts_col_idx)[child_idx];

                const meta = getSlotMeta(state_base, slot);
                const result = hashmap_ops.singleMapUpsert(.latest,
                    delta_mode,
                    state_base,
                    meta,
                    slot,
                    getColU32Inner(col_ptrs, key_col_idx)[child_idx],
                    getColU32Inner(col_ptrs, val_col_idx)[child_idx],
                    ts,
                );
                if (result == .CAPACITY_EXCEEDED) {
                    g_needs_growth_slot = slot;
                    return @intFromEnum(ErrorCode.NEEDS_GROWTH);
                }
            },

            // MAP_UPSERT_FIRST_IF (0x29): slot, key_col, val_col, pred_col
            0x29 => {
                const slot = body[bpc + 1];
                const key_col_idx = body[bpc + 2];
                const val_col_idx = body[bpc + 3];
                const pred_col_idx = body[bpc + 4];
                bpc += 5;

                if (getColU32Inner(col_ptrs, pred_col_idx)[child_idx] == 0) continue;

                const meta = getSlotMeta(state_base, slot);
                const result = hashmap_ops.singleMapUpsert(.first,
                    delta_mode,
                    state_base,
                    meta,
                    slot,
                    getColU32Inner(col_ptrs, key_col_idx)[child_idx],
                    getColU32Inner(col_ptrs, val_col_idx)[child_idx],
                    0,
                );
                if (result == .CAPACITY_EXCEEDED) {
                    g_needs_growth_slot = slot;
                    return @intFromEnum(ErrorCode.NEEDS_GROWTH);
                }
            },

            // MAP_UPSERT_LAST_IF (0x2A): slot, key_col, val_col, pred_col
            0x2A => {
                const slot = body[bpc + 1];
                const key_col_idx = body[bpc + 2];
                const val_col_idx = body[bpc + 3];
                const pred_col_idx = body[bpc + 4];
                bpc += 5;

                if (getColU32Inner(col_ptrs, pred_col_idx)[child_idx] == 0) continue;

                const meta = getSlotMeta(state_base, slot);
                const result = hashmap_ops.singleMapUpsert(.last,
                    delta_mode,
                    state_base,
                    meta,
                    slot,
                    getColU32Inner(col_ptrs, key_col_idx)[child_idx],
                    getColU32Inner(col_ptrs, val_col_idx)[child_idx],
                    if (meta.hasTTL()) getColF64Inner(col_ptrs, meta.timestamp_field_idx)[child_idx] else 0,
                );
                if (result == .CAPACITY_EXCEEDED) {
                    g_needs_growth_slot = slot;
                    return @intFromEnum(ErrorCode.NEEDS_GROWTH);
                }
            },

            // MAP_REMOVE_IF (0x2B): slot, key_col, pred_col
            0x2B => {
                const slot = body[bpc + 1];
                const key_col_idx = body[bpc + 2];
                const pred_col_idx = body[bpc + 3];
                bpc += 4;

                if (getColU32Inner(col_ptrs, pred_col_idx)[child_idx] == 0) continue;

                const meta = getSlotMeta(state_base, slot);
                hashmap_ops.singleMapRemove(
                    delta_mode,
                    state_base,
                    meta,
                    slot,
                    getColU32Inner(col_ptrs, key_col_idx)[child_idx],
                );
            },

            // MAP_UPSERT_MAX_IF (0x2C): slot, key_col, val_col, cmp_col, pred_col
            0x2C => {
                const slot = body[bpc + 1];
                const key_col_idx = body[bpc + 2];
                const val_col_idx = body[bpc + 3];
                const cmp_col_idx = body[bpc + 4];
                const pred_col_idx = body[bpc + 5];
                bpc += 6;

                if (getColU32Inner(col_ptrs, pred_col_idx)[child_idx] == 0) continue;

                const meta = getSlotMeta(state_base, slot);
                const result = hashmap_ops.singleMapUpsert(.max,
                    delta_mode,
                    state_base,
                    meta,
                    slot,
                    getColU32Inner(col_ptrs, key_col_idx)[child_idx],
                    getColU32Inner(col_ptrs, val_col_idx)[child_idx],
                    getColF64Inner(col_ptrs, cmp_col_idx)[child_idx],
                );
                if (result == .CAPACITY_EXCEEDED) {
                    g_needs_growth_slot = slot;
                    return @intFromEnum(ErrorCode.NEEDS_GROWTH);
                }
            },

            // MAP_UPSERT_MIN_IF (0x2D): slot, key_col, val_col, cmp_col, pred_col
            0x2D => {
                const slot = body[bpc + 1];
                const key_col_idx = body[bpc + 2];
                const val_col_idx = body[bpc + 3];
                const cmp_col_idx = body[bpc + 4];
                const pred_col_idx = body[bpc + 5];
                bpc += 6;

                if (getColU32Inner(col_ptrs, pred_col_idx)[child_idx] == 0) continue;

                const meta = getSlotMeta(state_base, slot);
                const result = hashmap_ops.singleMapUpsert(.min,
                    delta_mode,
                    state_base,
                    meta,
                    slot,
                    getColU32Inner(col_ptrs, key_col_idx)[child_idx],
                    getColU32Inner(col_ptrs, val_col_idx)[child_idx],
                    getColF64Inner(col_ptrs, cmp_col_idx)[child_idx],
                );
                if (result == .CAPACITY_EXCEEDED) {
                    g_needs_growth_slot = slot;
                    return @intFromEnum(ErrorCode.NEEDS_GROWTH);
                }
            },

            // SET_INSERT (0x30): slot, elem_col
            0x30 => {
                const slot = body[bpc + 1];
                const elem_col_idx = body[bpc + 2];
                bpc += 3;

                const meta = getSlotMeta(state_base, slot);
                const result = hashset_ops.singleSetInsert(
                    delta_mode,
                    state_base,
                    meta,
                    slot,
                    getColU32Inner(col_ptrs, elem_col_idx)[child_idx],
                    if (meta.hasTTL()) getColF64Inner(col_ptrs, meta.timestamp_field_idx)[child_idx] else 0,
                );
                if (result == .CAPACITY_EXCEEDED) {
                    g_needs_growth_slot = slot;
                    return @intFromEnum(ErrorCode.NEEDS_GROWTH);
                }
            },

            // SET_INSERT_TTL (0x32): slot, elem_col, ts_col
            0x32 => {
                const slot = body[bpc + 1];
                const elem_col_idx = body[bpc + 2];
                const ts_col_idx = body[bpc + 3];
                bpc += 4;

                const ts = if (parent_ts_col != 0xFF)
                    getColF64Inner(col_ptrs, parent_ts_col)[parent_idx]
                else
                    getColF64Inner(col_ptrs, ts_col_idx)[child_idx];

                const meta = getSlotMeta(state_base, slot);
                const result = hashset_ops.singleSetInsert(
                    delta_mode,
                    state_base,
                    meta,
                    slot,
                    getColU32Inner(col_ptrs, elem_col_idx)[child_idx],
                    ts,
                );
                if (result == .CAPACITY_EXCEEDED) {
                    g_needs_growth_slot = slot;
                    return @intFromEnum(ErrorCode.NEEDS_GROWTH);
                }
            },

            // SET_INSERT_IF (0x33): slot, elem_col, pred_col
            0x33 => {
                const slot = body[bpc + 1];
                const elem_col_idx = body[bpc + 2];
                const pred_col_idx = body[bpc + 3];
                bpc += 4;

                if (getColU32Inner(col_ptrs, pred_col_idx)[child_idx] == 0) continue;

                const meta = getSlotMeta(state_base, slot);
                const result = hashset_ops.singleSetInsert(
                    delta_mode,
                    state_base,
                    meta,
                    slot,
                    getColU32Inner(col_ptrs, elem_col_idx)[child_idx],
                    if (meta.hasTTL()) getColF64Inner(col_ptrs, meta.timestamp_field_idx)[child_idx] else 0,
                );
                if (result == .CAPACITY_EXCEEDED) {
                    g_needs_growth_slot = slot;
                    return @intFromEnum(ErrorCode.NEEDS_GROWTH);
                }
            },

            // SET_REMOVE (0x31): slot, elem_col
            0x31 => {
                const slot = body[bpc + 1];
                const elem_col_idx = body[bpc + 2];
                bpc += 3;

                const meta = getSlotMeta(state_base, slot);
                hashset_ops.singleSetRemove(
                    delta_mode,
                    state_base,
                    meta,
                    slot,
                    getColU32Inner(col_ptrs, elem_col_idx)[child_idx],
                );
            },

            // BITMAP_ADD (0x34): slot, elem_col
            0x34 => {
                const slot = body[bpc + 1];
                const elem_col_idx = body[bpc + 2];
                bpc += 3;

                const meta = getSlotMeta(state_base, slot);
                const result = hashset_ops.singleSetInsert(
                    delta_mode,
                    state_base,
                    meta,
                    slot,
                    getColU32Inner(col_ptrs, elem_col_idx)[child_idx],
                    if (meta.hasTTL()) getColF64Inner(col_ptrs, meta.timestamp_field_idx)[child_idx] else 0,
                );
                if (result == .CAPACITY_EXCEEDED) {
                    g_needs_growth_slot = slot;
                    return @intFromEnum(ErrorCode.NEEDS_GROWTH);
                }
            },

            // BITMAP_REMOVE (0x35): slot, elem_col
            0x35 => {
                const slot = body[bpc + 1];
                const elem_col_idx = body[bpc + 2];
                bpc += 3;

                const meta = getSlotMeta(state_base, slot);
                hashset_ops.singleSetRemove(
                    delta_mode,
                    state_base,
                    meta,
                    slot,
                    getColU32Inner(col_ptrs, elem_col_idx)[child_idx],
                );
            },

            // STRUCT_MAP_UPSERT_LAST (0x80): slot, key_col, num_vals, [(val_col, field_idx) × N],
            //   num_array_vals, [(offsets_col, values_col, field_idx) × N]
            0x80 => {
                const slot = body[bpc + 1];
                const key_col_idx = body[bpc + 2];
                const num_vals = body[bpc + 3];
                bpc += 4;

                var vc: [32]u8 = undefined;
                var fi: [32]u8 = undefined;
                for (0..num_vals) |vi| {
                    vc[vi] = body[bpc];
                    fi[vi] = body[bpc + 1];
                    bpc += 2;
                }

                // Parse array field entries
                const num_array_vals = body[bpc];
                bpc += 1;
                var aoc: [16]u8 = undefined; // array offsets cols
                var avc: [16]u8 = undefined; // array values cols
                var afi: [16]u8 = undefined; // array field indices
                for (0..num_array_vals) |ai| {
                    aoc[ai] = body[bpc];
                    avc[ai] = body[bpc + 1];
                    afi[ai] = body[bpc + 2];
                    bpc += 3;
                }

                const key = getColU32Inner(col_ptrs, key_col_idx)[child_idx];
                const result = singleStructMapUpsertLast(
                    delta_mode,
                    state_base,
                    slot,
                    key,
                    vc[0..num_vals],
                    fi[0..num_vals],
                    num_vals,
                    col_ptrs,
                    child_idx,
                );
                if (result.err == .CAPACITY_EXCEEDED) {
                    g_needs_growth_slot = slot;
                    return @intFromEnum(ErrorCode.NEEDS_GROWTH);
                }

                // Write array fields to arena (if any)
                if (num_array_vals > 0) {
                    const arr_result = writeStructMapArrayFields(
                        state_base,
                        slot,
                        result.pos,
                        aoc[0..num_array_vals],
                        avc[0..num_array_vals],
                        afi[0..num_array_vals],
                        num_array_vals,
                        col_ptrs,
                        child_idx,
                    );
                    if (arr_result == .ARENA_OVERFLOW) {
                        g_needs_growth_slot = slot;
                        return @intFromEnum(ErrorCode.NEEDS_GROWTH);
                    }
                }
            },

            // LIST_APPEND (0x84): slot, val_col — append scalar value
            0x84 => {
                const slot = body[bpc + 1];
                const val_col_idx = body[bpc + 2];
                bpc += 3;

                // Read metadata directly (getSlotMeta can't handle ORDERED_LIST's repurposed fields)
                const meta_base_off = STATE_HEADER_SIZE + @as(u32, slot) * SLOT_META_SIZE;
                const slot_offset = std.mem.readInt(u32, state_base[meta_base_off..][0..4], .little);
                const capacity = std.mem.readInt(u32, state_base[meta_base_off + 4 ..][0..4], .little);
                var count = std.mem.readInt(u32, state_base[meta_base_off + 8 ..][0..4], .little);
                const elem_size: u32 = std.mem.readInt(u16, state_base[meta_base_off + 16 ..][0..2], .little);

                if (count >= capacity) {
                    g_needs_growth_slot = slot;
                    return @intFromEnum(ErrorCode.NEEDS_GROWTH);
                }

                if (g_undo_enabled) {
                    appendMutation(
                        delta_mode,
                        .{ .op = .LIST_APPEND_UNDO, .slot = slot, ._pad1 = 0, ._pad2 = 0, .key = 0, .prev_value = count, .aux = 0 },
                        .{ .op = .LIST_APPEND_UNDO, .slot = slot, ._pad1 = 0, ._pad2 = 0, .key = 0, .prev_value = count + 1, .aux = 0 },
                    );
                }

                // Write value at position count
                const write_off = slot_offset + count * elem_size;
                if (elem_size == 4) {
                    const val = getColU32Inner(col_ptrs, val_col_idx)[child_idx];
                    std.mem.writeInt(u32, state_base[write_off..][0..4], val, .little);
                } else if (elem_size == 8) {
                    const col: [*]const u64 = @ptrCast(@alignCast(col_ptrs[val_col_idx]));
                    std.mem.writeInt(u64, state_base[write_off..][0..8], col[child_idx], .little);
                } else {
                    // BOOL (1 byte)
                    const val = getColU32Inner(col_ptrs, val_col_idx)[child_idx];
                    state_base[write_off] = if (val != 0) 1 else 0;
                }

                count += 1;
                std.mem.writeInt(u32, state_base[meta_base_off + 8 ..][0..4], count, .little);
                // Set change flag
                state_base[meta_base_off + 14] |= ChangeFlag.INSERTED;
            },

            // LIST_APPEND_STRUCT (0x85): slot, num_vals, [(val_col, field_idx) × N]
            0x85 => {
                const slot = body[bpc + 1];
                const num_vals = body[bpc + 2];
                bpc += 3;

                var vc: [32]u8 = undefined;
                var fi: [32]u8 = undefined;
                for (0..num_vals) |vi| {
                    vc[vi] = body[bpc];
                    fi[vi] = body[bpc + 1];
                    bpc += 2;
                }

                const meta_base_off = STATE_HEADER_SIZE + @as(u32, slot) * SLOT_META_SIZE;
                const slot_offset = std.mem.readInt(u32, state_base[meta_base_off..][0..4], .little);
                const capacity = std.mem.readInt(u32, state_base[meta_base_off + 4 ..][0..4], .little);
                var count = std.mem.readInt(u32, state_base[meta_base_off + 8 ..][0..4], .little);
                const num_fields = state_base[meta_base_off + 13];
                const bitset_bytes_val: u32 = state_base[meta_base_off + 15];
                const row_size: u32 = std.mem.readInt(u16, state_base[meta_base_off + 16 ..][0..2], .little);

                if (count >= capacity) {
                    g_needs_growth_slot = slot;
                    return @intFromEnum(ErrorCode.NEEDS_GROWTH);
                }

                if (g_undo_enabled) {
                    appendMutation(
                        delta_mode,
                        .{ .op = .LIST_APPEND_UNDO, .slot = slot, ._pad1 = 0, ._pad2 = 0, .key = 0, .prev_value = count, .aux = 0 },
                        .{ .op = .LIST_APPEND_UNDO, .slot = slot, ._pad1 = 0, ._pad2 = 0, .key = 0, .prev_value = count + 1, .aux = 0 },
                    );
                }

                // Write row at position count
                const field_types_ptr: [*]u8 = state_base + slot_offset;
                const descriptor_size = align8(@as(u32, num_fields));
                const rows_base = slot_offset + descriptor_size;
                const row_ptr = state_base + rows_base + count * row_size;

                // Clear bitset
                @memset(row_ptr[0..bitset_bytes_val], 0);

                for (0..num_vals) |vi| {
                    const field_idx = fi[vi];
                    const field_type: StructFieldType = @enumFromInt(field_types_ptr[field_idx]);
                    const f_offset = structFieldOffset(num_fields, field_types_ptr, field_idx);

                    // Set bit
                    row_ptr[field_idx / 8] |= @as(u8, 1) << @as(u3, @truncate(field_idx % 8));

                    switch (field_type) {
                        .UINT32, .STRING => {
                            const col = getColU32Inner(col_ptrs, vc[vi]);
                            std.mem.writeInt(u32, row_ptr[f_offset..][0..4], col[child_idx], .little);
                        },
                        .INT64 => {
                            const col: [*]const u64 = @ptrCast(@alignCast(col_ptrs[vc[vi]]));
                            std.mem.writeInt(u64, row_ptr[f_offset..][0..8], col[child_idx], .little);
                        },
                        .FLOAT64 => {
                            const col: [*]const f64 = @ptrCast(@alignCast(col_ptrs[vc[vi]]));
                            const bits: u64 = @bitCast(col[child_idx]);
                            std.mem.writeInt(u64, row_ptr[f_offset..][0..8], bits, .little);
                        },
                        .BOOL => {
                            const col = getColU32Inner(col_ptrs, vc[vi]);
                            row_ptr[f_offset] = if (col[child_idx] != 0) 1 else 0;
                        },
                        // Array fields in ordered list struct rows — not yet supported
                        .ARRAY_U32, .ARRAY_I64, .ARRAY_F64, .ARRAY_STRING, .ARRAY_BOOL => {},
                    }
                }

                count += 1;
                std.mem.writeInt(u32, state_base[meta_base_off + 8 ..][0..4], count, .little);
                state_base[meta_base_off + 14] |= ChangeFlag.INSERTED;
            },

            // FLAT_MAP (0xE1): offsets_col, parent_ts_col, inner_body_len(u16LE)
            0xE1 => {
                const offsets_col_idx = body[bpc + 1];
                const inner_parent_ts_col = body[bpc + 2]; // 0xFF = unused, else column idx for parent timestamp
                const inner_body_len = @as(u16, body[bpc + 3]) | (@as(u16, body[bpc + 4]) << 8);
                bpc += 5;

                const inner_body = body[bpc .. bpc + inner_body_len];
                bpc += inner_body_len;

                const offsets = getColU32Inner(col_ptrs, offsets_col_idx);
                const start = offsets[child_idx];
                const end = offsets[child_idx + 1];

                var j = start;
                while (j < end) : (j += 1) {
                    const result = executeElementOpcodes(
                        delta_mode,
                        state_base,
                        inner_body,
                        col_ptrs,
                        j, // child_idx in child columns
                        child_idx, // parent_idx stays the same
                        inner_parent_ts_col, // pass through for LATEST strategy
                    );
                    if (result != @intFromEnum(ErrorCode.OK)) return result;
                }
            },

            // NESTED_SET_INSERT (0x90): slot, outer_key_col, elem_col
            0x90 => {
                const slot = body[bpc + 1];
                const outer_key_col = body[bpc + 2];
                const elem_col = body[bpc + 3];
                bpc += 4;

                const meta = getSlotMeta(state_base, slot);
                const result = nested.nestedSetInsert(
                    state_base,
                    meta,
                    getColU32Inner(col_ptrs, outer_key_col)[child_idx],
                    getColU32Inner(col_ptrs, elem_col)[child_idx],
                );
                if (result == .CAPACITY_EXCEEDED) {
                    g_needs_growth_slot = slot;
                    return @intFromEnum(ErrorCode.NEEDS_GROWTH);
                }
            },

            // NESTED_MAP_UPSERT_LAST (0x92): slot, outer_key_col, inner_key_col, val_col
            0x92 => {
                const slot = body[bpc + 1];
                const outer_key_col = body[bpc + 2];
                const inner_key_col = body[bpc + 3];
                const val_col = body[bpc + 4];
                bpc += 5;

                const meta = getSlotMeta(state_base, slot);
                const result = nested.nestedMapUpsertLast(
                    state_base,
                    meta,
                    getColU32Inner(col_ptrs, outer_key_col)[child_idx],
                    getColU32Inner(col_ptrs, inner_key_col)[child_idx],
                    getColU32Inner(col_ptrs, val_col)[child_idx],
                );
                if (result == .CAPACITY_EXCEEDED) {
                    g_needs_growth_slot = slot;
                    return @intFromEnum(ErrorCode.NEEDS_GROWTH);
                }
            },

            // NESTED_AGG_UPDATE (0x95): slot, outer_key_col, val_col
            0x95 => {
                const slot = body[bpc + 1];
                const outer_key_col = body[bpc + 2];
                const val_col = body[bpc + 3];
                bpc += 4;

                const meta = getSlotMeta(state_base, slot);
                const val_data: [*]const f64 = @ptrCast(@alignCast(col_ptrs[val_col]));
                const result = nested.nestedAggUpdate(
                    state_base,
                    meta,
                    getColU32Inner(col_ptrs, outer_key_col)[child_idx],
                    @bitCast(val_data[child_idx]),
                );
                if (result == .CAPACITY_EXCEEDED) {
                    g_needs_growth_slot = slot;
                    return @intFromEnum(ErrorCode.NEEDS_GROWTH);
                }
            },

            else => {
                // Unknown body opcode - skip 1 byte to avoid infinite loop
                bpc += 1;
            },
        }
    }
    return @intFromEnum(ErrorCode.OK);
}

pub export fn vm_execute_batch(
    state_base: [*]u8,
    program_ptr: [*]const u8,
    program_len: u32,
    col_ptrs_ptr: [*]const [*]const u8,
    num_cols: u32,
    batch_len: u32,
) u32 {
    defer clearBitmapScratch();
    return executeBatchImpl(false, state_base, program_ptr, program_len, col_ptrs_ptr, num_cols, batch_len);
}

pub export fn vm_execute_batch_delta(
    state_base: [*]u8,
    program_ptr: [*]const u8,
    program_len: u32,
    col_ptrs_ptr: [*]const [*]const u8,
    num_cols: u32,
    batch_len: u32,
) u32 {
    defer clearBitmapScratch();
    return executeBatchImpl(true, state_base, program_ptr, program_len, col_ptrs_ptr, num_cols, batch_len);
}

// =============================================================================
// Derived Facts Header Access
// =============================================================================

/// Read derived_facts_offset from state header (u32 at offset 13, little-endian)
pub fn getDerivedFactsOffset(state_base: [*]u8) u32 {
    return @as(u32, state_base[StateHeaderOffset.DERIVED_FACTS_OFFSET]) |
        (@as(u32, state_base[StateHeaderOffset.DERIVED_FACTS_OFFSET + 1]) << 8) |
        (@as(u32, state_base[StateHeaderOffset.DERIVED_FACTS_OFFSET + 2]) << 16) |
        (@as(u32, state_base[StateHeaderOffset.DERIVED_FACTS_OFFSET + 3]) << 24);
}

/// Read derived_facts_capacity from state header (u16 at offset 17, little-endian)
pub fn getDerivedFactsCapacity(state_base: [*]u8) u16 {
    return @as(u16, state_base[StateHeaderOffset.DERIVED_FACTS_CAPACITY]) |
        (@as(u16, state_base[StateHeaderOffset.DERIVED_FACTS_CAPACITY + 1]) << 8);
}

/// Read num_derived_fact_schemas from state header
pub fn getNumDerivedFactSchemas(state_base: [*]u8) u8 {
    return state_base[StateHeaderOffset.NUM_DERIVED_FACT_SCHEMAS];
}

/// Get/set derived facts change flag
pub fn getDerivedFactsChangeFlag(state_base: [*]u8) u8 {
    return state_base[StateHeaderOffset.DERIVED_FACTS_CHANGE_FLAG];
}

pub fn setDerivedFactsChangeFlag(state_base: [*]u8, flag: u8) void {
    state_base[StateHeaderOffset.DERIVED_FACTS_CHANGE_FLAG] |= flag;
}

pub fn clearDerivedFactsChangeFlag(state_base: [*]u8) void {
    state_base[StateHeaderOffset.DERIVED_FACTS_CHANGE_FLAG] = 0;
}

/// Write derived facts header fields (called during RETE program loading)
pub fn writeDerivedFactsHeader(
    state_base: [*]u8,
    derived_offset: u32,
    capacity: u16,
    num_schemas: u8,
) void {
    // Write offset as little-endian u32
    state_base[StateHeaderOffset.DERIVED_FACTS_OFFSET] = @truncate(derived_offset);
    state_base[StateHeaderOffset.DERIVED_FACTS_OFFSET + 1] = @truncate(derived_offset >> 8);
    state_base[StateHeaderOffset.DERIVED_FACTS_OFFSET + 2] = @truncate(derived_offset >> 16);
    state_base[StateHeaderOffset.DERIVED_FACTS_OFFSET + 3] = @truncate(derived_offset >> 24);

    // Write capacity as little-endian u16
    state_base[StateHeaderOffset.DERIVED_FACTS_CAPACITY] = @truncate(capacity);
    state_base[StateHeaderOffset.DERIVED_FACTS_CAPACITY + 1] = @truncate(capacity >> 8);

    // Write schema count
    state_base[StateHeaderOffset.NUM_DERIVED_FACT_SCHEMAS] = num_schemas;

    // Clear change flag
    state_base[StateHeaderOffset.DERIVED_FACTS_CHANGE_FLAG] = 0;
}

// =============================================================================
// Utility Exports
// =============================================================================

// =============================================================================
// State Size Calculation and Initialization
// =============================================================================

inline fn align8(n: u32) u32 {
    return (n + 7) & ~@as(u32, 7);
}

inline fn nextPowerOf2(n: u32) u32 {
    if (n <= 16) return 16;
    var v = n - 1;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    return v + 1;
}

/// Calculate state buffer size from program bytecode.
/// Returns size in bytes, or 0 if program is invalid.
pub export fn vm_calculate_state_size(
    program_ptr: [*]const u8,
    program_len: u32,
) u32 {
    if (program_len < PROGRAM_HEADER_SIZE) return 0;

    const program = program_ptr[0..program_len];
    const content = program[PROGRAM_HASH_PREFIX..];

    // Check magic
    const magic = @as(u32, content[0]) | (@as(u32, content[1]) << 8) | (@as(u32, content[2]) << 16) | (@as(u32, content[3]) << 24);
    if (magic != PROGRAM_MAGIC) return 0;

    const num_slots = content[6];
    const init_len: u16 = @as(u16, content[10]) | (@as(u16, content[11]) << 8);

    if (PROGRAM_HEADER_SIZE + init_len > program_len) return 0;

    // Start with header + slot metadata (now 48 bytes per slot)
    var size: u32 = STATE_HEADER_SIZE + @as(u32, num_slots) * SLOT_META_SIZE;
    size = align8(size);

    // Parse init section to calculate slot data sizes (content header 14 bytes, then init)
    const init_code = content[14 .. 14 + init_len];
    var pc: usize = 0;

    while (pc < init_code.len) {
        const op: Opcode = @enumFromInt(init_code[pc]);
        pc += 1;

        switch (op) {
            // New unified slot definition
            .SLOT_DEF => {
                // slot:u8, type_flags:u8, cap_lo:u8, cap_hi:u8 [, ttl:f32, ts_field:u8]
                const type_flags = SlotTypeFlags.fromByte(init_code[pc + 1]);
                const cap_lo = init_code[pc + 2];
                const cap_hi = init_code[pc + 3];
                var capacity: u32 = (@as(u32, cap_hi) << 8) | cap_lo;
                const is_fixed_size = type_flags.slot_type == .AGGREGATE or type_flags.slot_type == .SCALAR or type_flags.slot_type == .CONDITION_TREE;
                if (!is_fixed_size and capacity == 0) {
                    capacity = 1024;
                }
                if (!is_fixed_size) {
                    capacity = nextPowerOf2(capacity * 2); // 2x for load factor
                }

                // Skip base bytes
                pc += 4;

                if (type_flags.slot_type == .HASHMAP and type_flags.has_ttl and type_flags.no_hashmap_timestamps) {
                    return 0;
                }

                // Skip TTL params if present (10 bytes: f32 ttl + f32 grace + u8 ts_field + u8 start_of)
                if (type_flags.has_ttl) {
                    pc += 10;
                }

                // Calculate primary storage size based on slot type
                switch (type_flags.slot_type) {
                    .HASHMAP => {
                        // keys (u32) + values (u32) [+ timestamps (f64)]
                        size += capacity * 4 + capacity * 4;
                        if (!type_flags.no_hashmap_timestamps) {
                            size += capacity * 8;
                        }
                    },
                    .HASHSET => {
                        // keys (u32)
                        size += capacity * 4;
                    },
                    .BITMAP => {
                        size += BITMAP_SERIALIZED_LEN_BYTES + bitmapPayloadCapacity(capacity);
                    },
                    .AGGREGATE => {
                        // COUNT: u64 only (8 bytes). Others: value + count = 16 bytes.
                        // cap_lo holds AggType for AGGREGATE/SCALAR slots.
                        size += if (cap_lo == @intFromEnum(AggType.COUNT)) 8 else 16;
                    },
                    .SCALAR => {
                        // value ([8]u8) + cmp_ts (f64) = 16 bytes
                        size += 16;
                    },
                    .ARRAY => {
                        // values (u32) + timestamps (f64)
                        size += capacity * 4 + capacity * 8;
                    },
                    .CONDITION_TREE => {
                        size += CONDITION_TREE_STATE_BYTES;
                    },
                    .STRUCT_MAP => {
                        // STRUCT_MAP uses its own SLOT_STRUCT_MAP opcode, not SLOT_DEF
                        // This branch should not be reached, but handle gracefully
                    },
                    .ORDERED_LIST => {
                        // ORDERED_LIST uses its own SLOT_ORDERED_LIST opcode, not SLOT_DEF
                    },
                    .NESTED => {
                        // NESTED uses its own SLOT_NESTED opcode, not SLOT_DEF
                    },
                }
                size = align8(size);

                // Add eviction index and evicted buffer if TTL enabled
                if (type_flags.has_ttl) {
                    // EvictionEntry is 16 bytes (f64 timestamp + u32 key + u32 value)
                    // Eviction index: same capacity as primary storage
                    size += capacity * @sizeOf(EvictionEntry);
                    size = align8(size);

                    // Evicted buffer (for RETE triggers): max ~1000 entries per batch
                    if (type_flags.has_evict_trigger) {
                        const evicted_buffer_capacity: u32 = 1024;
                        size += evicted_buffer_capacity * @sizeOf(EvictionEntry);
                        size = align8(size);
                    }
                }
            },

            .SLOT_ARRAY => {
                const cap_lo = init_code[pc + 1];
                const cap_hi = init_code[pc + 2];
                var capacity: u32 = (@as(u32, cap_hi) << 8) | cap_lo;
                if (capacity == 0) capacity = 1024;
                capacity = nextPowerOf2(capacity * 2);
                pc += 4;

                // values (u32) + timestamps (f64)
                size += capacity * 4 + capacity * 8;
                size = align8(size);
            },

            .SLOT_STRUCT_MAP => {
                const type_flags = SlotTypeFlags.fromByte(init_code[pc + 1]);
                const cap_lo = init_code[pc + 2];
                const cap_hi = init_code[pc + 3];
                const num_fields = init_code[pc + 4];
                pc += 5;

                var capacity: u32 = (@as(u32, cap_hi) << 8) | cap_lo;
                if (capacity == 0) capacity = 1024;
                capacity = nextPowerOf2(capacity * 2);

                // Read field types from bytecode
                const field_types_ptr: [*]const u8 = @ptrCast(init_code[pc..].ptr);
                pc += num_fields;

                const layout = computeStructRowLayout(num_fields, field_types_ptr);
                size += getStructMapSlotDataSize(layout.descriptor_size, capacity, layout.row_size, false);

                // Arena for array fields
                if (hasArrayFields(num_fields, field_types_ptr)) {
                    size += ARENA_HEADER_SIZE + arenaInitialCapacity(capacity);
                }
                size = align8(size);

                // TTL eviction storage (if applicable)
                if (type_flags.has_ttl) {
                    size += capacity * @sizeOf(EvictionEntry);
                    size = align8(size);
                    if (type_flags.has_evict_trigger) {
                        const evicted_buffer_capacity: u32 = 1024;
                        size += evicted_buffer_capacity * @sizeOf(EvictionEntry);
                        size = align8(size);
                    }
                }
            },

            .SLOT_ORDERED_LIST => {
                // slot:u8, type_flags:u8, cap_lo:u8, cap_hi:u8, elem_type:u8
                // (if elem_type == 0xFF/STRUCT: num_fields:u8, [field_type:u8 × num_fields])
                const cap_lo = init_code[pc + 2];
                const cap_hi = init_code[pc + 3];
                const elem_type = init_code[pc + 4];
                pc += 5;

                var capacity: u32 = (@as(u32, cap_hi) << 8) | cap_lo;
                if (capacity == 0) capacity = 1024;
                capacity = nextPowerOf2(capacity);

                if (elem_type == 0xFF) {
                    // Struct list: descriptor + rows
                    const num_fields = init_code[pc];
                    const field_types_ptr: [*]const u8 = @ptrCast(init_code[pc + 1 ..].ptr);
                    pc += 1 + num_fields;

                    const layout = computeStructRowLayout(num_fields, field_types_ptr);
                    size += layout.descriptor_size + capacity * layout.row_size;
                } else {
                    // Scalar list: values only
                    const elem_size: u32 = structFieldSize(@enumFromInt(elem_type));
                    size += capacity * elem_size;
                }
                size = align8(size);
            },

            .SLOT_NESTED => {
                // SLOT_NESTED: slot:u8, outer_type_flags:u8, outer_cap_lo:u8, outer_cap_hi:u8,
                //              inner_type:u8, inner_cap_lo:u8, inner_cap_hi:u8, inner_agg_type:u8
                const outer_cap_lo = init_code[pc + 2];
                const outer_cap_hi = init_code[pc + 3];
                const inner_type_byte = init_code[pc + 4];
                const inner_cap_lo = init_code[pc + 5];
                const inner_cap_hi = init_code[pc + 6];
                const inner_agg_type_byte = init_code[pc + 7];
                pc += 8;

                var outer_cap: u32 = (@as(u32, outer_cap_hi) << 8) | outer_cap_lo;
                if (outer_cap == 0) outer_cap = 1024;
                outer_cap = nextPowerOf2(outer_cap * 2);

                var inner_initial_cap: u32 = (@as(u32, inner_cap_hi) << 8) | inner_cap_lo;
                if (inner_initial_cap == 0) inner_initial_cap = 16;
                const inner_type: SlotType = @enumFromInt(@as(u4, @truncate(inner_type_byte)));
                // Only interpret as AggType for AGGREGATE inner containers; default to SUM otherwise
                const inner_agg_type: AggType = if (inner_type == .AGGREGATE and inner_agg_type_byte >= 1)
                    @enumFromInt(inner_agg_type_byte)
                else
                    .SUM;

                size += nested.nestedSlotDataSize(outer_cap, nextPowerOf2(inner_initial_cap), inner_type, inner_agg_type);
                size = align8(size);
            },

            .HALT => break,
            else => break,
        }
    }

    return size;
}

/// Helper to write slot metadata to state buffer (new 48-byte layout)
fn writeSlotMeta(
    state_ptr: [*]u8,
    slot: u8,
    data_offset: u32,
    capacity: u32,
    type_flags: SlotTypeFlags,
    agg_type: AggType,
    ttl_seconds: f32,
    grace_seconds: f32,
    timestamp_field_idx: u8,
    start_of: DurationUnit,
    eviction_index_offset: u32,
    eviction_index_capacity: u32,
    evicted_buffer_offset: u32,
) void {
    const meta_offset = STATE_HEADER_SIZE + @as(u32, slot) * SLOT_META_SIZE;
    const meta_bytes = state_ptr + meta_offset;
    const meta_ptr: [*]u32 = @ptrCast(@alignCast(meta_bytes));

    // Core fields
    meta_ptr[0] = data_offset; // offset: u32
    meta_ptr[1] = capacity; // capacity: u32
    meta_ptr[2] = 0; // size: u32 (initially 0)

    // Byte-sized fields
    meta_bytes[SlotMetaOffset.TYPE_FLAGS] = type_flags.toByte();
    meta_bytes[SlotMetaOffset.AGG_TYPE] = @truncate(@intFromEnum(agg_type));
    meta_bytes[SlotMetaOffset.CHANGE_FLAGS] = 0;
    meta_bytes[SlotMetaOffset.TIMESTAMP_FIELD_IDX] = timestamp_field_idx;

    // TTL fields
    const ttl_ptr: *f32 = @ptrCast(@alignCast(meta_bytes + SlotMetaOffset.TTL_SECONDS));
    ttl_ptr.* = ttl_seconds;
    const grace_ptr: *f32 = @ptrCast(@alignCast(meta_bytes + SlotMetaOffset.GRACE_SECONDS));
    grace_ptr.* = grace_seconds;

    meta_ptr[6] = eviction_index_offset; // eviction_index_offset (offset 24)
    meta_ptr[7] = eviction_index_capacity; // eviction_index_capacity (offset 28)
    meta_ptr[8] = 0; // eviction_index_size (initially 0) (offset 32)
    meta_ptr[9] = evicted_buffer_offset; // evicted_buffer_offset (offset 36)
    meta_ptr[10] = 0; // evicted_count (initially 0) (offset 40)

    // start_of at offset 44
    meta_bytes[SlotMetaOffset.START_OF] = @intFromEnum(start_of);

    // Clear reserved bytes (45-47)
    meta_bytes[45] = 0;
    meta_bytes[46] = 0;
    meta_bytes[47] = 0;
}

/// Initialize state buffer from program bytecode.
/// state_ptr must point to a buffer of at least vm_calculate_state_size() bytes.
/// Returns 0 on success, non-zero on error.
pub export fn vm_init_state(
    state_ptr: [*]u8,
    program_ptr: [*]const u8,
    program_len: u32,
) u32 {
    if (program_len < PROGRAM_HEADER_SIZE) return @intFromEnum(ErrorCode.INVALID_PROGRAM);

    const program = program_ptr[0..program_len];
    const content = program[PROGRAM_HASH_PREFIX..];

    // Check magic
    const magic = @as(u32, content[0]) | (@as(u32, content[1]) << 8) | (@as(u32, content[2]) << 16) | (@as(u32, content[3]) << 24);
    if (magic != PROGRAM_MAGIC) return @intFromEnum(ErrorCode.INVALID_PROGRAM);

    const num_slots = content[6];
    const init_len: u16 = @as(u16, content[10]) | (@as(u16, content[11]) << 8);

    if (PROGRAM_HEADER_SIZE + init_len > program_len) return @intFromEnum(ErrorCode.INVALID_PROGRAM);

    // Write state header (new format v2)
    // Layout: magic(4) + format_version(1) + program_version(2) + ruleset_version(2) +
    //         num_slots(1) + num_vars(1) + num_bitvecs(1) + flags(1) + reserved(19)
    const state_u32: [*]u32 = @ptrCast(@alignCast(state_ptr));
    state_u32[0] = STATE_MAGIC;

    // Byte-level access for the rest of the header
    state_ptr[StateHeaderOffset.FORMAT_VERSION] = STATE_FORMAT_VERSION;
    // Program version from content header (bytes 4-5)
    state_ptr[StateHeaderOffset.PROGRAM_VERSION] = content[4];
    state_ptr[StateHeaderOffset.PROGRAM_VERSION + 1] = content[5];
    // Ruleset version defaults to 0 (no RETE program loaded yet)
    state_ptr[StateHeaderOffset.RULESET_VERSION] = 0;
    state_ptr[StateHeaderOffset.RULESET_VERSION + 1] = 0;
    state_ptr[StateHeaderOffset.NUM_SLOTS] = num_slots;
    state_ptr[StateHeaderOffset.NUM_VARS] = 0; // Set when RETE program loaded
    state_ptr[StateHeaderOffset.NUM_BITVECS] = 0; // Set when RETE program loaded
    state_ptr[StateHeaderOffset.FLAGS] = 0; // No RETE by default

    // Clear reserved bytes (13-31)
    for (13..32) |i| {
        state_ptr[i] = 0;
    }

    // Calculate where slot data starts (after 48-byte slot metadata)
    var data_offset: u32 = align8(STATE_HEADER_SIZE + @as(u32, num_slots) * SLOT_META_SIZE);

    // Parse init section and initialize slots (content header 14 bytes, then init)
    const init_code = content[14 .. 14 + init_len];
    var pc: usize = 0;

    while (pc < init_code.len) {
        const op: Opcode = @enumFromInt(init_code[pc]);
        pc += 1;

        switch (op) {
            // New unified slot definition
            .SLOT_DEF => {
                const slot = init_code[pc];
                const type_flags = SlotTypeFlags.fromByte(init_code[pc + 1]);
                const cap_lo = init_code[pc + 2];
                const cap_hi = init_code[pc + 3];
                var capacity: u32 = (@as(u32, cap_hi) << 8) | cap_lo;
                const is_fixed_size = type_flags.slot_type == .AGGREGATE or type_flags.slot_type == .SCALAR or type_flags.slot_type == .CONDITION_TREE;
                if (!is_fixed_size and capacity == 0) {
                    capacity = 1024;
                }
                if (!is_fixed_size) {
                    capacity = nextPowerOf2(capacity * 2);
                }
                // For AGGREGATE/SCALAR: cap_lo encodes the AggType subtype
                const is_subtyped = type_flags.slot_type == .AGGREGATE or type_flags.slot_type == .SCALAR;
                const agg_type: AggType = if (is_subtyped and cap_lo > 0) @enumFromInt(cap_lo) else .SUM;
                pc += 4;

                if (type_flags.slot_type == .HASHMAP and type_flags.has_ttl and type_flags.no_hashmap_timestamps) {
                    return @intFromEnum(ErrorCode.INVALID_PROGRAM);
                }

                // Read TTL params if present (10 bytes: f32 ttl + f32 grace + u8 ts_field + u8 start_of)
                var ttl_seconds: f32 = 0.0;
                var grace_seconds: f32 = 0.0;
                var timestamp_field_idx: u8 = 0;
                var start_of: DurationUnit = .NONE;
                if (type_flags.has_ttl) {
                    const ttl_bits: u32 =
                        @as(u32, init_code[pc]) |
                        (@as(u32, init_code[pc + 1]) << 8) |
                        (@as(u32, init_code[pc + 2]) << 16) |
                        (@as(u32, init_code[pc + 3]) << 24);
                    ttl_seconds = @bitCast(ttl_bits);
                    const grace_bits: u32 =
                        @as(u32, init_code[pc + 4]) |
                        (@as(u32, init_code[pc + 5]) << 8) |
                        (@as(u32, init_code[pc + 6]) << 16) |
                        (@as(u32, init_code[pc + 7]) << 24);
                    grace_seconds = @bitCast(grace_bits);
                    timestamp_field_idx = init_code[pc + 8];
                    start_of = @enumFromInt(init_code[pc + 9]);
                    pc += 10;
                }

                const primary_data_offset = data_offset;

                // Initialize primary storage based on slot type
                switch (type_flags.slot_type) {
                    .HASHMAP => {
                        // Init keys to EMPTY_KEY via typed FlatHashTable, skip values (zero-init)
                        const slot_meta_off = STATE_HEADER_SIZE + @as(u32, slot) * SLOT_META_SIZE;
                        const size_ptr: *align(1) u32 = @ptrCast(state_ptr + slot_meta_off + 8);
                        _ = hash_table.FlatHashTable(u32).initExternal(state_ptr + data_offset, capacity, size_ptr);
                        data_offset += capacity * 4 + capacity * 4;

                        if (!type_flags.no_hashmap_timestamps) {
                            const ts_ptr: [*]f64 = @ptrCast(@alignCast(state_ptr + data_offset));
                            for (0..capacity) |i| ts_ptr[i] = -std.math.inf(f64);
                            data_offset += capacity * 8;
                        }
                    },
                    .CONDITION_TREE => {
                        const tree_state: *ConditionTreeState = @ptrCast(@alignCast(state_ptr + data_offset));
                        tree_state.* = .{ .lifecycle_generation = 1, .last_removed_key = EMPTY_KEY };
                        data_offset += CONDITION_TREE_STATE_BYTES;
                    },
                    .HASHSET => {
                        const slot_meta_off = STATE_HEADER_SIZE + @as(u32, slot) * SLOT_META_SIZE;
                        const size_ptr: *align(1) u32 = @ptrCast(state_ptr + slot_meta_off + 8);
                        _ = hash_table.HashSet.initExternal(state_ptr + data_offset, capacity, size_ptr);
                        data_offset += capacity * 4;
                    },
                    .BITMAP => {
                        const storage_size = BITMAP_SERIALIZED_LEN_BYTES + bitmapPayloadCapacity(capacity);
                        @memset(state_ptr[data_offset .. data_offset + storage_size], 0);
                        data_offset += storage_size;
                    },
                    .AGGREGATE => {
                        data_offset += aggregates.initAggSlot(state_ptr, data_offset, agg_type);
                    },
                    .SCALAR => {
                        // value ([8]u8) + cmp_ts (f64) = 16 bytes
                        @memset(state_ptr[data_offset .. data_offset + 8], 0);
                        const cmp_ptr: *f64 = @ptrCast(@alignCast(state_ptr + data_offset + 8));
                        cmp_ptr.* = -std.math.inf(f64);
                        data_offset += 16;
                    },
                    .ARRAY => {
                        // values (u32) + timestamps (f64)
                        const vals_ptr: [*]u32 = @ptrCast(@alignCast(state_ptr + data_offset));
                        for (0..capacity) |i| {
                            vals_ptr[i] = EMPTY_KEY;
                        }

                        const ts_offset = data_offset + capacity * 4;
                        const ts_ptr: [*]f64 = @ptrCast(@alignCast(state_ptr + ts_offset));
                        for (0..capacity) |i| {
                            ts_ptr[i] = -std.math.inf(f64);
                        }

                        data_offset += capacity * 4 + capacity * 8;
                    },
                    .STRUCT_MAP => {
                        // STRUCT_MAP uses its own SLOT_STRUCT_MAP opcode, not SLOT_DEF
                    },
                    .ORDERED_LIST => {
                        // ORDERED_LIST uses its own SLOT_ORDERED_LIST opcode, not SLOT_DEF
                    },
                    .NESTED => {
                        // NESTED uses its own SLOT_NESTED opcode, not SLOT_DEF
                    },
                }
                data_offset = align8(data_offset);

                // Initialize eviction index and evicted buffer if TTL enabled
                var eviction_index_offset: u32 = 0;
                var eviction_index_capacity: u32 = 0;
                var evicted_buffer_offset: u32 = 0;

                if (type_flags.has_ttl) {
                    eviction_index_offset = data_offset;
                    eviction_index_capacity = capacity;

                    // Zero out eviction index
                    const eviction_size = capacity * @sizeOf(EvictionEntry);
                    @memset(state_ptr[data_offset .. data_offset + eviction_size], 0);
                    data_offset += eviction_size;
                    data_offset = align8(data_offset);

                    if (type_flags.has_evict_trigger) {
                        evicted_buffer_offset = data_offset;
                        const evicted_capacity: u32 = 1024;
                        const evicted_size = evicted_capacity * @sizeOf(EvictionEntry);
                        @memset(state_ptr[data_offset .. data_offset + evicted_size], 0);
                        data_offset += evicted_size;
                        data_offset = align8(data_offset);
                    }
                }

                // Write slot metadata
                writeSlotMeta(
                    state_ptr,
                    slot,
                    primary_data_offset,
                    capacity,
                    type_flags,
                    agg_type,
                    ttl_seconds,
                    grace_seconds,
                    timestamp_field_idx,
                    start_of,
                    eviction_index_offset,
                    eviction_index_capacity,
                    evicted_buffer_offset,
                );
            },

            .SLOT_ARRAY => {
                const slot = init_code[pc];
                const cap_lo = init_code[pc + 1];
                const cap_hi = init_code[pc + 2];
                var capacity: u32 = (@as(u32, cap_hi) << 8) | cap_lo;
                if (capacity == 0) capacity = 1024;
                capacity = nextPowerOf2(capacity * 2);
                pc += 4;

                const type_flags = SlotTypeFlags{
                    .slot_type = .ARRAY,
                    .has_ttl = false,
                    .has_evict_trigger = false,
                };

                // values (u32) + timestamps (f64)
                const vals_ptr: [*]u32 = @ptrCast(@alignCast(state_ptr + data_offset));
                for (0..capacity) |i| {
                    vals_ptr[i] = EMPTY_KEY;
                }

                const ts_offset = data_offset + capacity * 4;
                const ts_ptr: [*]f64 = @ptrCast(@alignCast(state_ptr + ts_offset));
                for (0..capacity) |i| {
                    ts_ptr[i] = -std.math.inf(f64);
                }

                const primary_offset = data_offset;
                data_offset += capacity * 4 + capacity * 8;
                data_offset = align8(data_offset);

                writeSlotMeta(state_ptr, slot, primary_offset, capacity, type_flags, .SUM, 0.0, 0.0, 0, .NONE, 0, 0, 0);
            },

            .SLOT_STRUCT_MAP => {
                const slot = init_code[pc];
                const type_flags = SlotTypeFlags.fromByte(init_code[pc + 1]);
                const cap_lo = init_code[pc + 2];
                const cap_hi = init_code[pc + 3];
                const num_fields = init_code[pc + 4];
                pc += 5;

                var capacity: u32 = (@as(u32, cap_hi) << 8) | cap_lo;
                if (capacity == 0) capacity = 1024;
                capacity = nextPowerOf2(capacity * 2);

                // Read field types from bytecode
                const field_types_ptr: [*]const u8 = @ptrCast(init_code[pc..].ptr);
                pc += num_fields;

                const layout = computeStructRowLayout(num_fields, field_types_ptr);

                const meta_base = STATE_HEADER_SIZE + @as(u32, slot) * SLOT_META_SIZE;

                // Write slot metadata
                std.mem.writeInt(u32, state_ptr[meta_base..][0..4], data_offset, .little);
                std.mem.writeInt(u32, state_ptr[meta_base + 4 ..][0..4], capacity, .little);
                std.mem.writeInt(u32, state_ptr[meta_base + 8 ..][0..4], 0, .little); // size = 0
                state_ptr[meta_base + 12] = type_flags.toByte();
                state_ptr[meta_base + 13] = num_fields; // reuse AGG_TYPE byte for num_fields
                state_ptr[meta_base + 14] = 0; // change_flags
                state_ptr[meta_base + 15] = @truncate(layout.bitset_bytes); // reuse TIMESTAMP_FIELD_IDX for bitset_bytes
                std.mem.writeInt(u16, state_ptr[meta_base + 16 ..][0..2], @truncate(layout.row_size), .little); // reuse TTL_SECONDS low bytes for row_size
                state_ptr[meta_base + 18] = 0; // has_timestamps (0 for UPSERT_LAST)
                // Clear remaining metadata bytes
                for (19..SLOT_META_SIZE) |off| {
                    state_ptr[meta_base + off] = 0;
                }

                // Write field types as prefix of slot data
                @memcpy(state_ptr[data_offset .. data_offset + num_fields], field_types_ptr[0..num_fields]);

                // Initialize keys to EMPTY_KEY
                const keys_offset = data_offset + layout.descriptor_size;
                const keys: [*]u32 = @ptrCast(@alignCast(&state_ptr[keys_offset]));
                for (0..capacity) |ki| {
                    keys[ki] = EMPTY_KEY;
                }

                // Zero the rows region
                const rows_offset = keys_offset + capacity * 4;
                @memset(state_ptr[rows_offset .. rows_offset + capacity * layout.row_size], 0);

                data_offset += getStructMapSlotDataSize(layout.descriptor_size, capacity, layout.row_size, false);

                // Initialize arena for array fields
                if (hasArrayFields(num_fields, field_types_ptr)) {
                    const arena_cap = arenaInitialCapacity(capacity);
                    // Store arena header offset in metadata[20..24]
                    std.mem.writeInt(u32, state_ptr[meta_base + 20 ..][0..4], data_offset, .little);
                    // Arena header: [capacity:u32][used:u32]
                    std.mem.writeInt(u32, state_ptr[data_offset..][0..4], arena_cap, .little);
                    std.mem.writeInt(u32, state_ptr[data_offset + 4 ..][0..4], 0, .little); // used = 0
                    // Zero the arena data region
                    @memset(state_ptr[data_offset + ARENA_HEADER_SIZE .. data_offset + ARENA_HEADER_SIZE + arena_cap], 0);
                    data_offset += ARENA_HEADER_SIZE + arena_cap;
                }
                data_offset = align8(data_offset);
            },

            .SLOT_ORDERED_LIST => {
                const slot = init_code[pc];
                const type_flags_byte = init_code[pc + 1];
                const cap_lo = init_code[pc + 2];
                const cap_hi = init_code[pc + 3];
                const elem_type = init_code[pc + 4];
                pc += 5;

                var capacity: u32 = (@as(u32, cap_hi) << 8) | cap_lo;
                if (capacity == 0) capacity = 1024;
                capacity = nextPowerOf2(capacity);

                const meta_base = STATE_HEADER_SIZE + @as(u32, slot) * SLOT_META_SIZE;

                if (elem_type == 0xFF) {
                    // Struct list
                    const num_fields = init_code[pc];
                    const field_types_ptr: [*]const u8 = @ptrCast(init_code[pc + 1 ..].ptr);
                    pc += 1 + num_fields;

                    const layout = computeStructRowLayout(num_fields, field_types_ptr);

                    // Write metadata
                    std.mem.writeInt(u32, state_ptr[meta_base..][0..4], data_offset, .little);
                    std.mem.writeInt(u32, state_ptr[meta_base + 4 ..][0..4], capacity, .little);
                    std.mem.writeInt(u32, state_ptr[meta_base + 8 ..][0..4], 0, .little); // count = 0
                    state_ptr[meta_base + 12] = type_flags_byte;
                    state_ptr[meta_base + 13] = num_fields;
                    state_ptr[meta_base + 14] = 0; // change_flags
                    state_ptr[meta_base + 15] = @truncate(layout.bitset_bytes);
                    std.mem.writeInt(u16, state_ptr[meta_base + 16 ..][0..2], @truncate(layout.row_size), .little);
                    state_ptr[meta_base + 18] = elem_type; // 0xFF = struct
                    for (19..SLOT_META_SIZE) |moff| {
                        state_ptr[meta_base + moff] = 0;
                    }

                    // Write field types as descriptor prefix
                    @memcpy(state_ptr[data_offset .. data_offset + num_fields], field_types_ptr[0..num_fields]);

                    // Zero the rows region
                    const rows_offset = data_offset + layout.descriptor_size;
                    @memset(state_ptr[rows_offset .. rows_offset + capacity * layout.row_size], 0);

                    data_offset += layout.descriptor_size + capacity * layout.row_size;
                } else {
                    // Scalar list
                    const elem_size: u32 = structFieldSize(@enumFromInt(elem_type));

                    std.mem.writeInt(u32, state_ptr[meta_base..][0..4], data_offset, .little);
                    std.mem.writeInt(u32, state_ptr[meta_base + 4 ..][0..4], capacity, .little);
                    std.mem.writeInt(u32, state_ptr[meta_base + 8 ..][0..4], 0, .little); // count = 0
                    state_ptr[meta_base + 12] = type_flags_byte;
                    state_ptr[meta_base + 13] = 0; // no fields for scalar
                    state_ptr[meta_base + 14] = 0; // change_flags
                    state_ptr[meta_base + 15] = 0;
                    std.mem.writeInt(u16, state_ptr[meta_base + 16 ..][0..2], @truncate(elem_size), .little); // row_size = elem_size
                    state_ptr[meta_base + 18] = elem_type;
                    for (19..SLOT_META_SIZE) |moff| {
                        state_ptr[meta_base + moff] = 0;
                    }

                    // Zero the values region
                    @memset(state_ptr[data_offset .. data_offset + capacity * elem_size], 0);

                    data_offset += capacity * elem_size;
                }
                data_offset = align8(data_offset);
            },

            .SLOT_NESTED => {
                // SLOT_NESTED: slot:u8, outer_type_flags:u8, outer_cap_lo:u8, outer_cap_hi:u8,
                //              inner_type:u8, inner_cap_lo:u8, inner_cap_hi:u8, inner_agg_type:u8
                const slot_idx = init_code[pc];
                const outer_type_flags_byte = init_code[pc + 1];
                const outer_cap_lo = init_code[pc + 2];
                const outer_cap_hi = init_code[pc + 3];
                const inner_type_byte = init_code[pc + 4];
                const inner_cap_lo = init_code[pc + 5];
                const inner_cap_hi = init_code[pc + 6];
                const inner_agg_type_byte = init_code[pc + 7];
                pc += 8;

                var outer_cap: u32 = (@as(u32, outer_cap_hi) << 8) | outer_cap_lo;
                if (outer_cap == 0) outer_cap = 1024;
                outer_cap = nextPowerOf2(outer_cap * 2);

                var inner_initial_cap: u32 = (@as(u32, inner_cap_hi) << 8) | inner_cap_lo;
                if (inner_initial_cap == 0) inner_initial_cap = 16;
                const inner_cap = nextPowerOf2(inner_initial_cap);
                const inner_type: SlotType = @enumFromInt(@as(u4, @truncate(inner_type_byte)));
                const inner_agg_type: AggType = if (inner_type == .AGGREGATE and inner_agg_type_byte >= 1)
                    @enumFromInt(inner_agg_type_byte)
                else
                    .SUM;

                // Write slot metadata
                const meta_base = STATE_HEADER_SIZE + @as(u32, slot_idx) * SLOT_META_SIZE;
                std.mem.writeInt(u32, state_ptr[meta_base..][0..4], data_offset, .little); // offset
                std.mem.writeInt(u32, state_ptr[meta_base + 4 ..][0..4], outer_cap, .little); // capacity
                std.mem.writeInt(u32, state_ptr[meta_base + 8 ..][0..4], 0, .little); // size = 0
                state_ptr[meta_base + SlotMetaOffset.TYPE_FLAGS] = outer_type_flags_byte;
                state_ptr[meta_base + SlotMetaOffset.AGG_TYPE] = @intFromEnum(inner_agg_type);
                state_ptr[meta_base + SlotMetaOffset.CHANGE_FLAGS] = 0;

                // Write nested prefix at slot data start
                nested.writeNestedPrefix(state_ptr, data_offset, .{
                    .inner_type = inner_type,
                    .inner_initial_cap = @truncate(inner_initial_cap),
                    .inner_agg_type = inner_agg_type,
                    .depth = 1,
                });

                // Initialize outer keys to EMPTY_KEY
                const keys_off = nested.outerKeysOffset(data_offset);
                const keys: [*]u32 = @ptrCast(@alignCast(state_ptr + keys_off));
                for (0..outer_cap) |i| keys[i] = EMPTY_KEY;

                // Initialize arena header
                const arena_hdr = nested.arenaHeaderOffset(data_offset, outer_cap);
                const arena_start = nested.arenaDataOffset(data_offset, outer_cap);
                const slot_data_size = nested.nestedSlotDataSize(outer_cap, inner_cap, inner_type, inner_agg_type);
                const arena_cap = slot_data_size - (arena_start - data_offset);
                std.mem.writeInt(u32, state_ptr[arena_hdr..][0..4], arena_cap, .little);
                std.mem.writeInt(u32, state_ptr[arena_hdr + 4 ..][0..4], 0, .little); // used = 0

                data_offset += slot_data_size;
                data_offset = align8(data_offset);
            },

            .HALT => break,
            else => break,
        }
    }

    return @intFromEnum(ErrorCode.OK);
}

/// Reset state to initial values (re-initialize without reallocating).
pub export fn vm_reset_state(
    state_ptr: [*]u8,
    program_ptr: [*]const u8,
    program_len: u32,
) u32 {
    // Just re-initialize - same as init
    return vm_init_state(state_ptr, program_ptr, program_len);
}

// =============================================================================
// Utility Exports
// =============================================================================

/// Look up key in hashmap, returns value or EMPTY_KEY if not found
pub export fn vm_map_get(
    state_base: [*]u8,
    slot_offset: u32,
    capacity: u32,
    key: u32,
) u32 {
    const data_ptr = state_base + slot_offset;
    const keys: [*]const u32 = @ptrCast(@alignCast(data_ptr));
    const values: [*]const u32 = @ptrCast(@alignCast(data_ptr + capacity * 4));

    var slot = hashKey(key, capacity);
    var probes: u32 = 0;

    while (probes < capacity) : (probes += 1) {
        const k = keys[slot];
        if (k == EMPTY_KEY) return EMPTY_KEY;
        if (k == key) return values[slot];
        slot = (slot + 1) & (capacity - 1);
    }
    return EMPTY_KEY;
}

/// Check if element exists in hashset
pub export fn vm_set_contains(
    state_base: [*]u8,
    slot_offset: u32,
    capacity: u32,
    elem: u32,
) u32 {
    if (getBitmapSlotByOffset(state_base, slot_offset)) |slot| {
        const frozen = bitmapFrozen(slot.storage) orelse return 0;
        return if (frozen.contains(elem)) 1 else 0;
    }

    const data_ptr = state_base + slot_offset;
    const keys: [*]const u32 = @ptrCast(@alignCast(data_ptr));

    var slot = hashKey(elem, capacity);
    var probes: u32 = 0;

    while (probes < capacity) : (probes += 1) {
        const k = keys[slot];
        if (k == EMPTY_KEY) return 0;
        if (k == elem) return 1;
        slot = (slot + 1) & (capacity - 1);
    }
    return 0;
}

pub export fn vm_rbmp_export_len(state_base: [*]u8, slot_offset: u32) u32 {
    const slot = getBitmapSlotByOffset(state_base, slot_offset) orelse return 0;
    const serialized_len = slot.storage.serialized_len_ptr.*;
    if (serialized_len > slot.storage.payload_capacity) return 0;
    return serialized_len;
}

pub export fn vm_rbmp_export_copy(
    state_base: [*]u8,
    slot_offset: u32,
    out_ptr: [*]u8,
    out_capacity: u32,
) u32 {
    const slot = getBitmapSlotByOffset(state_base, slot_offset) orelse return @intFromEnum(ErrorCode.INVALID_SLOT);
    const serialized_len = slot.storage.serialized_len_ptr.*;
    if (serialized_len > slot.storage.payload_capacity) return @intFromEnum(ErrorCode.INVALID_STATE);
    if (serialized_len > out_capacity) return @intFromEnum(ErrorCode.CAPACITY_EXCEEDED);
    if (serialized_len > 0) {
        @memcpy(out_ptr[0..serialized_len], slot.storage.payload_ptr[0..serialized_len]);
    }
    return @intFromEnum(ErrorCode.OK);
}

pub export fn vm_rbmp_import_copy(
    state_base: [*]u8,
    slot_offset: u32,
    in_ptr: [*]const u8,
    in_len: u32,
) u32 {
    const slot = getBitmapSlotByOffset(state_base, slot_offset) orelse return @intFromEnum(ErrorCode.INVALID_SLOT);
    if (in_len > slot.storage.payload_capacity) {
        return @intFromEnum(ErrorCode.CAPACITY_EXCEEDED);
    }

    if (in_len == 0) {
        slot.storage.serialized_len_ptr.* = 0;
        @memset(slot.storage.payload_ptr[0..slot.storage.payload_capacity], 0);
        slot.meta.size_ptr.* = 0;
        return @intFromEnum(ErrorCode.OK);
    }

    const frozen = FrozenBitmap.init(in_ptr[0..in_len]) catch return @intFromEnum(ErrorCode.INVALID_STATE);
    const card = frozen.cardinality();
    if (card > slot.meta.capacity) {
        return @intFromEnum(ErrorCode.CAPACITY_EXCEEDED);
    }

    @memcpy(slot.storage.payload_ptr[0..in_len], in_ptr[0..in_len]);
    slot.storage.serialized_len_ptr.* = in_len;
    if (in_len < slot.storage.payload_capacity) {
        @memset(slot.storage.payload_ptr[in_len..slot.storage.payload_capacity], 0);
    }
    slot.meta.size_ptr.* = @intCast(card);
    return @intFromEnum(ErrorCode.OK);
}

/// Zero-allocation intersection cardinality using FrozenBitmap's container-level ops.
fn deserializedIntersectCount(left_data: []const u8, right_data: []const u8) u32 {
    const left_frozen = FrozenBitmap.init(left_data) catch return 0;
    const right_frozen = FrozenBitmap.init(right_data) catch return 0;
    const card = left_frozen.andCardinality(&right_frozen);
    return if (card > std.math.maxInt(u32)) std.math.maxInt(u32) else @intCast(card);
}

/// Zero-allocation intersection check using FrozenBitmap's container-level ops.
fn deserializedIntersects(left_data: []const u8, right_data: []const u8) bool {
    const left_frozen = FrozenBitmap.init(left_data) catch return false;
    const right_frozen = FrozenBitmap.init(right_data) catch return false;
    return left_frozen.intersects(&right_frozen);
}

/// Get serialized byte slice from a bitmap slot (returns null if empty/invalid).
inline fn slotSerializedData(storage: BitmapStorage) ?[]const u8 {
    const serialized_len = storage.serialized_len_ptr.*;
    if (serialized_len == 0 or serialized_len > storage.payload_capacity) return null;
    return storage.payload_ptr[0..serialized_len];
}

pub export fn vm_rbmp_intersect_any_slots(state_base: [*]u8, left_slot_offset: u32, right_slot_offset: u32) u32 {
    const left = getBitmapSlotByOffset(state_base, left_slot_offset) orelse return 0;
    const right = getBitmapSlotByOffset(state_base, right_slot_offset) orelse return 0;
    const left_data = slotSerializedData(left.storage) orelse return 0;
    const right_data = slotSerializedData(right.storage) orelse return 0;
    return if (deserializedIntersects(left_data, right_data)) 1 else 0;
}

pub export fn vm_rbmp_intersect_count_slots(state_base: [*]u8, left_slot_offset: u32, right_slot_offset: u32) u32 {
    const left = getBitmapSlotByOffset(state_base, left_slot_offset) orelse return 0;
    const right = getBitmapSlotByOffset(state_base, right_slot_offset) orelse return 0;
    const left_data = slotSerializedData(left.storage) orelse return 0;
    const right_data = slotSerializedData(right.storage) orelse return 0;
    return deserializedIntersectCount(left_data, right_data);
}

pub export fn vm_rbmp_intersect_any_serialized(
    left_ptr: [*]const u8,
    left_len: u32,
    right_ptr: [*]const u8,
    right_len: u32,
) u32 {
    if (left_len == 0 or right_len == 0) return 0;
    return if (deserializedIntersects(left_ptr[0..left_len], right_ptr[0..right_len])) 1 else 0;
}

pub export fn vm_rbmp_intersect_count_serialized(
    left_ptr: [*]const u8,
    left_len: u32,
    right_ptr: [*]const u8,
    right_len: u32,
) u32 {
    if (left_len == 0 or right_len == 0) return 0;
    return deserializedIntersectCount(left_ptr[0..left_len], right_ptr[0..right_len]);
}

// =============================================================================
// Set Algebra Exports (decision-function-side)
// =============================================================================

/// Global result location for set algebra operations.
/// Backed by dedicated VM-owned storage so later scratch allocations cannot clobber it.
var g_algebra_result_ptr: u32 = 0;
var g_algebra_result_len: u32 = 0;

pub export fn vm_rbmp_algebra_result_ptr() u32 {
    return g_algebra_result_ptr;
}

pub export fn vm_rbmp_algebra_result_len() u32 {
    return g_algebra_result_len;
}

/// Get a bitmap slot's serialized data pointer (no copy).
pub export fn vm_rbmp_slot_data_ptr(state_base: [*]u8, slot_offset: u32) u32 {
    const slot = getBitmapSlotByOffset(state_base, slot_offset) orelse return 0;
    return @intCast(@intFromPtr(slot.storage.payload_ptr));
}

/// Get a bitmap slot's serialized data length (no copy).
pub export fn vm_rbmp_slot_data_len(state_base: [*]u8, slot_offset: u32) u32 {
    const slot = getBitmapSlotByOffset(state_base, slot_offset) orelse return 0;
    const serialized_len = slot.storage.serialized_len_ptr.*;
    if (serialized_len > slot.storage.payload_capacity) return 0;
    return serialized_len;
}

fn setAlgebraResult(src: []const u8, oom_error_code: u32) u32 {
    if (src.len == 0) {
        g_algebra_result_ptr = 0;
        g_algebra_result_len = 0;
        return @intFromEnum(ErrorCode.OK);
    }

    const out = ensureReusableBuffer(bitmap_allocator, &g_algebra_result_buf, src.len, oom_error_code, oom_error_code) orelse {
        return @intFromEnum(ErrorCode.CAPACITY_EXCEEDED);
    };
    @memcpy(out, src);
    g_algebra_result_ptr = @intCast(@intFromPtr(out.ptr));
    g_algebra_result_len = @intCast(src.len);
    return @intFromEnum(ErrorCode.OK);
}

const AlgebraOp = enum { AND, OR, AND_NOT, XOR };

/// Core set algebra: deserialize into arena, compute op, serialize result into VM-owned buffer.
fn rbmpSetAlgebra(
    left_ptr: [*]const u8,
    left_len: u32,
    right_ptr: [*]const u8,
    right_len: u32,
    comptime op: AlgebraOp,
) u32 {
    g_algebra_result_ptr = 0;
    g_algebra_result_len = 0;

    // Empty-set identities — copy survivor directly into VM-owned algebra storage
    if (left_len == 0 and right_len == 0) return @intFromEnum(ErrorCode.OK);
    if (left_len == 0) return switch (op) {
        .AND, .AND_NOT => @intFromEnum(ErrorCode.OK), // empty result
        .OR, .XOR => setAlgebraResult(right_ptr[0..right_len], 70),
    };
    if (right_len == 0) return switch (op) {
        .AND => @intFromEnum(ErrorCode.OK), // empty result
        .OR, .AND_NOT, .XOR => setAlgebraResult(left_ptr[0..left_len], 70),
    };

    // Arena for deserialization temporaries — bulk freed on return
    var arena = std.heap.ArenaAllocator.init(bitmap_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var left = RoaringBitmap.deserialize(alloc, left_ptr[0..left_len]) catch {
        g_bitmap_last_error = 71;
        return @intFromEnum(ErrorCode.INVALID_STATE);
    };
    var right = RoaringBitmap.deserialize(alloc, right_ptr[0..right_len]) catch {
        g_bitmap_last_error = 72;
        return @intFromEnum(ErrorCode.INVALID_STATE);
    };
    var result = switch (op) {
        .AND => left.bitwiseAnd(alloc, &right),
        .OR => left.bitwiseOr(alloc, &right),
        .AND_NOT => left.bitwiseDifference(alloc, &right),
        .XOR => left.bitwiseXor(alloc, &right),
    } catch {
        g_bitmap_last_error = 73;
        return @intFromEnum(ErrorCode.INVALID_STATE);
    };

    _ = result.runOptimize() catch {};

    // Serialize result into VM-owned buffer.
    const size = result.serializedSizeInBytes();
    const out = ensureReusableBuffer(bitmap_allocator, &g_algebra_result_buf, size, 74, 74) orelse {
        return @intFromEnum(ErrorCode.CAPACITY_EXCEEDED);
    };
    _ = result.serializeIntoBuffer(out) catch {
        g_bitmap_last_error = 75;
        return @intFromEnum(ErrorCode.INVALID_STATE);
    };

    g_algebra_result_ptr = @intCast(@intFromPtr(out.ptr));
    g_algebra_result_len = @intCast(size);
    return @intFromEnum(ErrorCode.OK);
}

pub export fn vm_rbmp_and(lp: [*]const u8, ll: u32, rp: [*]const u8, rl: u32) u32 {
    return rbmpSetAlgebra(lp, ll, rp, rl, .AND);
}

pub export fn vm_rbmp_or(lp: [*]const u8, ll: u32, rp: [*]const u8, rl: u32) u32 {
    return rbmpSetAlgebra(lp, ll, rp, rl, .OR);
}

pub export fn vm_rbmp_andnot(lp: [*]const u8, ll: u32, rp: [*]const u8, rl: u32) u32 {
    return rbmpSetAlgebra(lp, ll, rp, rl, .AND_NOT);
}

pub export fn vm_rbmp_xor(lp: [*]const u8, ll: u32, rp: [*]const u8, rl: u32) u32 {
    return rbmpSetAlgebra(lp, ll, rp, rl, .XOR);
}

// =============================================================================
// Query Exports for Scratch-Resident or Slot-Resident Serialized Data
// =============================================================================

/// Check if a serialized bitmap (in scratch or slot storage) contains a value.
pub export fn vm_rbmp_contains_serialized(ptr: [*]const u8, len: u32, value: u32) u32 {
    if (len == 0) return 0;
    const frozen = FrozenBitmap.init(ptr[0..len]) catch return 0;
    return if (frozen.contains(value)) 1 else 0;
}

/// Get cardinality of a serialized bitmap (in scratch or slot storage).
pub export fn vm_rbmp_cardinality_serialized(ptr: [*]const u8, len: u32) u32 {
    if (len == 0) return 0;
    const frozen = FrozenBitmap.init(ptr[0..len]) catch return 0;
    const card = frozen.cardinality();
    return if (card > std.math.maxInt(u32)) std.math.maxInt(u32) else @intCast(card);
}

/// Extract all values from a serialized bitmap into an output buffer.
/// Returns the number of values written (capped at out_capacity).
pub export fn vm_rbmp_extract_serialized(
    data_ptr: [*]const u8,
    data_len: u32,
    out_ptr: [*]u32,
    out_capacity: u32,
) u32 {
    if (data_len == 0) return 0;
    const frozen = FrozenBitmap.init(data_ptr[0..data_len]) catch return 0;
    var iter = frozen.iterator();
    var count: u32 = 0;
    while (iter.next()) |v| {
        if (count >= out_capacity) break;
        out_ptr[count] = v;
        count += 1;
    }
    return count;
}

// =============================================================================
// HashMap/HashSet Iteration Exports
// =============================================================================

/// Start iteration over HashMap entries.
/// Returns first valid slot index, or capacity if empty.
pub export fn vm_map_iter_start(
    state_base: [*]u8,
    slot_offset: u32,
    capacity: u32,
) u32 {
    // Typed table binding — size_ptr unused for iteration
    var dummy_size: u32 = 0;
    const tbl = hash_table.HashMap.bindExternal(state_base + slot_offset, capacity, &dummy_size);

    var i: u32 = 0;
    while (i < tbl.cap) : (i += 1) {
        const k = tbl.keys[i];
        if (k != EMPTY_KEY and k != TOMBSTONE) {
            return i; // First valid entry
        }
    }
    return capacity; // No entries (signals end)
}

/// Get next valid slot index after current position.
/// Returns next valid slot index, or capacity if no more entries.
pub export fn vm_map_iter_next(
    state_base: [*]u8,
    slot_offset: u32,
    capacity: u32,
    current: u32,
) u32 {
    var dummy_size: u32 = 0;
    const tbl = hash_table.HashMap.bindExternal(state_base + slot_offset, capacity, &dummy_size);

    var i: u32 = current + 1;
    while (i < tbl.cap) : (i += 1) {
        const k = tbl.keys[i];
        if (k != EMPTY_KEY and k != TOMBSTONE) {
            return i;
        }
    }
    return capacity; // No more entries
}

/// Get key and value at iterator position.
/// Returns key in lower 32 bits, value in upper 32 bits (as u64).
pub export fn vm_map_iter_get(
    state_base: [*]u8,
    slot_offset: u32,
    capacity: u32,
    pos: u32,
) u64 {
    var dummy_size: u32 = 0;
    const tbl = hash_table.HashMap.bindExternal(state_base + slot_offset, capacity, &dummy_size);

    const key = tbl.keys[pos];
    const val = tbl.entries[pos];
    return (@as(u64, val) << 32) | key;
}

/// Start iteration over HashSet elements.
/// Returns first valid slot index, or capacity if empty.
pub export fn vm_set_iter_start(
    state_base: [*]u8,
    slot_offset: u32,
    capacity: u32,
) u32 {
    if (findSlotMetaByOffset(state_base, slot_offset)) |meta| {
        if (meta.slotType() == .BITMAP) {
            return if (meta.size_ptr.* == 0) capacity else 0;
        }
    }

    // Same as vm_map_iter_start - sets use same key array structure
    return vm_map_iter_start(state_base, slot_offset, capacity);
}

/// Get next valid element index in HashSet.
/// Returns next valid slot index, or capacity if no more elements.
pub export fn vm_set_iter_next(
    state_base: [*]u8,
    slot_offset: u32,
    capacity: u32,
    current: u32,
) u32 {
    if (findSlotMetaByOffset(state_base, slot_offset)) |meta| {
        if (meta.slotType() == .BITMAP) {
            const next = current + 1;
            return if (next < meta.size_ptr.*) next else capacity;
        }
    }

    return vm_map_iter_next(state_base, slot_offset, capacity, current);
}

/// Get element at iterator position (just the key for sets).
pub export fn vm_set_iter_get(
    state_base: [*]u8,
    slot_offset: u32,
    pos: u32,
) u32 {
    if (findSlotMetaByOffset(state_base, slot_offset)) |meta| {
        if (meta.slotType() == .BITMAP) {
            const storage = getBitmapStorage(state_base, meta);
            return bitmapSelect(storage, pos) orelse EMPTY_KEY;
        }
    }

    const data_ptr = state_base + slot_offset;
    const keys: [*]const u32 = @ptrCast(@alignCast(data_ptr));
    return keys[pos];
}

fn findSlotMetaByOffset(state_base: [*]u8, slot_offset: u32) ?SlotMeta {
    const num_slots = state_base[StateHeaderOffset.NUM_SLOTS];
    var slot: u8 = 0;
    while (slot < num_slots) : (slot += 1) {
        const meta = getSlotMeta(state_base, slot);
        if (meta.offset == slot_offset) return meta;
    }
    return null;
}

// =============================================================================
// Struct Map Read/Iteration Exports
// =============================================================================

/// Look up a key in a struct map. Returns the absolute byte offset of the row
/// (from state_base start) or 0xFFFFFFFF if not found.
/// JS side reads num_fields, row_size from slot metadata and passes them in.
pub export fn vm_struct_map_get_row_ptr(
    state_base_ptr: [*]const u8,
    slot_offset: u32,
    capacity: u32,
    num_fields: u32,
    row_size: u32,
    key: u32,
) u32 {
    const descriptor_size = align8(num_fields);
    const keys_offset = slot_offset + descriptor_size;
    const keys: [*]const u32 = @ptrCast(@alignCast(&state_base_ptr[keys_offset]));
    const rows_base = keys_offset + capacity * 4;

    var pos = hashKey(key, capacity);
    while (true) {
        const k = keys[pos];
        if (k == EMPTY_KEY) return 0xFFFFFFFF;
        if (k == key) return rows_base + pos * row_size;
        if (k == TOMBSTONE) {
            pos = (pos + 1) & (capacity - 1);
            continue;
        }
        pos = (pos + 1) & (capacity - 1);
    }
}

/// Struct map iteration — find first occupied slot index.
/// Returns slot index or capacity (end sentinel).
pub export fn vm_struct_map_iter_start(
    state_base_ptr: [*]const u8,
    slot_offset: u32,
    capacity: u32,
    num_fields: u32,
) u32 {
    const descriptor_size = align8(num_fields);
    const keys_offset = slot_offset + descriptor_size;
    const keys: [*]const u32 = @ptrCast(@alignCast(&state_base_ptr[keys_offset]));
    var pos: u32 = 0;
    while (pos < capacity) : (pos += 1) {
        if (keys[pos] != EMPTY_KEY and keys[pos] != TOMBSTONE) return pos;
    }
    return capacity; // end sentinel
}

/// Struct map iteration — advance to next occupied slot index.
/// Returns next slot index or capacity (end sentinel).
pub export fn vm_struct_map_iter_next(
    state_base_ptr: [*]const u8,
    slot_offset: u32,
    capacity: u32,
    num_fields: u32,
    current: u32,
) u32 {
    const descriptor_size = align8(num_fields);
    const keys_offset = slot_offset + descriptor_size;
    const keys: [*]const u32 = @ptrCast(@alignCast(&state_base_ptr[keys_offset]));
    var pos = current + 1;
    while (pos < capacity) : (pos += 1) {
        if (keys[pos] != EMPTY_KEY and keys[pos] != TOMBSTONE) return pos;
    }
    return capacity;
}

/// Get key at struct map iterator position.
pub export fn vm_struct_map_iter_key(
    state_base_ptr: [*]const u8,
    slot_offset: u32,
    num_fields: u32,
    pos: u32,
) u32 {
    const descriptor_size = align8(num_fields);
    const keys_offset = slot_offset + descriptor_size;
    const keys: [*]const u32 = @ptrCast(@alignCast(&state_base_ptr[keys_offset]));
    return keys[pos];
}

// =============================================================================
// Undo Log WASM Exports
// =============================================================================

/// Enable undo logging and save change flags. Call before speculative execution.
/// Resets the undo log to empty state. Stores state_base and state_size for
/// lazy shadow buffer snapshot on overflow.
pub export fn vm_undo_enable(state_base: [*]u8, state_size: u32) void {
    g_undo_enabled = true;
    g_undo_count = 0;
    g_delta_count = 0;
    g_undo_overflow = false;
    g_undo_shadow_active = false;
    g_undo_has_overflow_entry = false;
    // Free any leftover dynamic shadow from a prior undo session (native only)
    if (comptime builtin.cpu.arch != .wasm32) {
        if (g_undo_shadow_dynamic) |s| {
            native_shadow_allocator.free(s);
            g_undo_shadow_dynamic = null;
        }
    }
    g_undo_state_base = state_base;
    g_undo_state_size = state_size;
    saveChangeFlags(state_base);
}

/// Save current undo log position. Returns position as u32.
/// The returned position can be passed to vm_undo_rollback or vm_undo_commit.
pub export fn vm_undo_checkpoint(_state_base: [*]u8) u32 {
    _ = _state_base;
    return g_undo_count;
}

/// Rollback all mutations since the given checkpoint position.
/// If overflow occurred: restore shadow buffer (undoes un-logged mutations after
/// overflow), then replay undo log in reverse (undoes logged mutations before overflow).
/// If no overflow: just replay undo log in reverse.
pub export fn vm_undo_rollback(state_base: [*]u8, checkpoint_pos: u32) void {
    // Validate state identity in native FFI (prevents cross-handle aliasing bugs).
    // WASM always uses the same stateRegionOffset so the check is redundant there.
    if (comptime builtin.cpu.arch != .wasm32) {
        std.debug.assert(state_base == g_undo_state_base);
    }

    if (g_undo_overflow and g_undo_shadow_active) {
        // Step 1: Restore shadow buffer — undoes all mutations after the overflow point
        // that weren't captured in the undo log
        if (comptime builtin.cpu.arch == .wasm32) {
            if (g_undo_state_size <= UNDO_SHADOW_CAPACITY) {
                @memcpy(state_base[0..g_undo_state_size], g_undo_shadow_static[0..g_undo_state_size]);
            }
        } else {
            if (g_undo_shadow_dynamic) |shadow| {
                @memcpy(state_base[0..g_undo_state_size], shadow);
                native_shadow_allocator.free(shadow);
                g_undo_shadow_dynamic = null;
            }
        }
        g_undo_shadow_active = false;
        // Replay the overflow-triggering entry that was dropped from the log.
        // The shadow includes this mutation's effect, so we must undo it.
        if (g_undo_has_overflow_entry) {
            rollbackEntry(state_base, g_undo_overflow_entry);
            g_undo_has_overflow_entry = false;
        }
    }
    // Step 2: Replay undo log in reverse — undoes logged mutations before overflow
    var i = g_undo_count;
    while (i > checkpoint_pos) {
        i -= 1;
        rollbackEntry(state_base, g_undo_entries[i]);
    }
    g_undo_count = checkpoint_pos;
    if (g_delta_count > g_undo_count) {
        g_delta_count = g_undo_count;
    }
    restoreChangeFlags(state_base);
}

/// Commit (discard) undo entries and disable undo logging.
/// Call after speculative execution succeeds.
pub export fn vm_undo_commit(state_base: [*]u8, _checkpoint_pos: u32) void {
    _ = _checkpoint_pos;
    // Validate state identity in native FFI (WASM always uses same stateRegionOffset)
    if (comptime builtin.cpu.arch != .wasm32) {
        std.debug.assert(state_base == g_undo_state_base);
    }

    // Free dynamic shadow if present (native only)
    if (comptime builtin.cpu.arch != .wasm32) {
        if (g_undo_shadow_dynamic) |s| {
            native_shadow_allocator.free(s);
            g_undo_shadow_dynamic = null;
        }
    }
    g_undo_shadow_active = false;
    g_undo_has_overflow_entry = false;
    g_undo_count = 0;
    g_delta_count = 0;
    g_undo_overflow = false;
    g_undo_enabled = false;
}

/// Check if undo log overflowed during speculation.
/// Returns 1 if overflow occurred, 0 otherwise.
/// Overflow is now handled internally via shadow buffer — this export is kept for debugging/testing.
pub export fn vm_undo_has_overflow() u32 {
    return if (g_undo_overflow) @as(u32, 1) else @as(u32, 0);
}

pub export fn vm_delta_export_segment(_state_base: [*]u8, from_pos: u32, to_pos: u32) u32 {
    _ = _state_base;
    const end = @min(to_pos, g_delta_count);
    const start = @min(from_pos, end);
    g_delta_export_start = start;
    g_delta_export_count = end - start;
    g_delta_export_overflow = g_undo_overflow;
    return g_delta_export_count;
}

pub export fn vm_delta_export_undo_ptr() u32 {
    return @truncate(@intFromPtr(&g_undo_entries[g_delta_export_start]));
}

pub export fn vm_delta_export_redo_ptr() u32 {
    return @truncate(@intFromPtr(&g_redo_entries[g_delta_export_start]));
}

pub export fn vm_delta_export_len_bytes() u32 {
    return g_delta_export_count * @as(u32, @sizeOf(FlatUndoEntry));
}

pub export fn vm_delta_export_entry_size() u32 {
    return @sizeOf(FlatUndoEntry);
}

pub export fn vm_delta_export_overflow() u32 {
    return if (g_delta_export_overflow) @as(u32, 1) else @as(u32, 0);
}

pub export fn vm_delta_apply_rollback_segment(
    state_base: [*]u8,
    undo_segment_ptr: [*]const u8,
    segment_len_bytes: u32,
    entry_size: u32,
) void {
    if (entry_size != @sizeOf(FlatUndoEntry) or segment_len_bytes % entry_size != 0) return;
    const count = segment_len_bytes / entry_size;
    const entries: [*]align(1) const FlatUndoEntry = @ptrCast(undo_segment_ptr);
    var i = count;
    while (i > 0) {
        i -= 1;
        rollbackEntry(state_base, entries[i]);
    }
}

pub export fn vm_delta_apply_rollforward_segment(
    state_base: [*]u8,
    redo_segment_ptr: [*]const u8,
    segment_len_bytes: u32,
    entry_size: u32,
) void {
    if (entry_size != @sizeOf(FlatUndoEntry) or segment_len_bytes % entry_size != 0) return;
    const count = segment_len_bytes / entry_size;
    const entries: [*]align(1) const FlatUndoEntry = @ptrCast(redo_segment_ptr);
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        rollbackEntry(state_base, entries[i]);
    }
}

// =============================================================================
// Slot Growth
// =============================================================================
//
// When a HashMap or HashSet exceeds 70% load during executeBatch, the VM returns
// NEEDS_GROWTH (5). JS then:
//   1. Calls vm_get_needs_growth_slot() to learn which slot overflowed
//   2. Calls vm_calculate_grown_state_size() to compute the new buffer size
//   3. Allocates a new buffer of that size
//   4. Calls vm_grow_state() to copy data and rehash the grown slot
//   5. Retries the batch on the new state

/// Returns the slot index that triggered NEEDS_GROWTH, or 0xFF if none.
pub export fn vm_get_needs_growth_slot() u32 {
    return @as(u32, g_needs_growth_slot);
}

// getSlotDataSize removed — replaced by slot_growth.slotDataSize (typed SlotType enum)

fn getTTLSideBufferSize(has_ttl: bool, has_evict_trigger: bool, capacity: u32) u32 {
    if (!has_ttl) return 0;

    var size: u32 = align8(capacity * @sizeOf(EvictionEntry));
    if (has_evict_trigger) {
        size += align8(1024 * @sizeOf(EvictionEntry));
    }
    return size;
}

/// Compute data region size for a STRUCT_MAP slot.
/// Layout: [field_types x num_fields padded to 4] + [keys u32 x capacity] + [rows x capacity]
fn getStructMapSlotDataSize(descriptor_size: u32, capacity: u32, row_size: u32, has_timestamps: bool) u32 {
    return descriptor_size + capacity * 4 + capacity * row_size + if (has_timestamps) capacity * 8 else 0;
}

fn structFieldSize(ft: StructFieldType) u32 {
    return switch (ft) {
        .UINT32, .STRING => 4,
        .INT64, .FLOAT64 => 8,
        .BOOL => 1,
        // Array fields store (offset:u32, length:u32) in-row — 8 bytes
        .ARRAY_U32, .ARRAY_I64, .ARRAY_F64, .ARRAY_STRING, .ARRAY_BOOL => 8,
    };
}

/// Element size for array types in the arena
fn arenaElemSize(ft: StructFieldType) u32 {
    return switch (ft) {
        .ARRAY_U32, .ARRAY_STRING => 4,
        .ARRAY_I64, .ARRAY_F64 => 8,
        .ARRAY_BOOL => 1,
        else => unreachable,
    };
}

fn isArrayFieldType(ft: StructFieldType) bool {
    return @intFromEnum(ft) >= 5;
}

/// Check if any field in the descriptor is an array type
fn hasArrayFields(num_fields: u8, field_types_ptr: [*]const u8) bool {
    for (0..num_fields) |i| {
        if (field_types_ptr[i] >= 5) return true;
    }
    return false;
}

const ARENA_HEADER_SIZE: u32 = 8; // [arena_capacity:u32][arena_used:u32]

/// Heuristic initial arena capacity: 64 bytes per hash table entry
fn arenaInitialCapacity(hash_capacity: u32) u32 {
    return hash_capacity * 64;
}

/// Compute row size (bitset + field data) from field types stored in slot data prefix.
/// Returns: { row_size, bitset_bytes, descriptor_size }
fn computeStructRowLayout(num_fields: u8, field_types_ptr: [*]const u8) struct { row_size: u32, bitset_bytes: u32, descriptor_size: u32 } {
    const bitset_bytes: u32 = ((@as(u32, num_fields) + 7) / 8);
    var row_data: u32 = bitset_bytes;
    for (0..num_fields) |i| {
        row_data += structFieldSize(@enumFromInt(field_types_ptr[i]));
    }
    // Align row to 4 bytes for clean addressing
    const row_size = (row_data + 3) & ~@as(u32, 3);
    // Descriptor: num_fields bytes of field types, aligned to 4
    const descriptor_size = align8(@as(u32, num_fields));
    return .{ .row_size = row_size, .bitset_bytes = bitset_bytes, .descriptor_size = descriptor_size };
}

/// Compute byte offset of a field within a row (after bitset).
fn structFieldOffset(num_fields: u8, field_types_ptr: [*]const u8, target_field: u8) u32 {
    const bitset_bytes: u32 = ((@as(u32, num_fields) + 7) / 8);
    var offset: u32 = bitset_bytes;
    for (0..target_field) |i| {
        offset += structFieldSize(@enumFromInt(field_types_ptr[i]));
    }
    return offset;
}

/// Compute state size with 2× capacity for the slot at `grown_slot_idx`.
/// Reads capacity from old state metadata (not program bytecode), so it
/// handles states that have already been grown.
pub export fn vm_calculate_grown_state_size(
    old_state_ptr: [*]const u8,
    _program_ptr: [*]const u8,
    _program_len: u32,
    grown_slot_idx: u32,
) u32 {
    _ = _program_ptr;
    _ = _program_len;
    const num_slots: u32 = old_state_ptr[9];
    var total_size: u32 = STATE_HEADER_SIZE + num_slots * SLOT_META_SIZE;
    total_size = (total_size + 7) & ~@as(u32, 7);

    var slot_i: u32 = 0;
    while (slot_i < num_slots) : (slot_i += 1) {
        const meta_base = STATE_HEADER_SIZE + slot_i * SLOT_META_SIZE;
        const old_cap = std.mem.readInt(u32, old_state_ptr[meta_base + 4 ..][0..4], .little);
        const type_flags_byte = old_state_ptr[meta_base + 12];
        const slot_type: u4 = @truncate(type_flags_byte & 0x0F);
        const has_ttl = (type_flags_byte & 0x10) != 0;
        const has_evict_trigger = (type_flags_byte & 0x20) != 0;
        const has_hashmap_timestamps = (slot_type != @intFromEnum(SlotType.HASHMAP)) or ((type_flags_byte & 0x40) == 0);
        const agg_type_byte = old_state_ptr[meta_base + 13]; // dual-purpose: AggType for agg/scalar, num_fields for struct_map

        const cap = if (slot_i == grown_slot_idx) nextPowerOf2(old_cap * 2) else old_cap;
        var slot_size: u32 = 0;
        if (slot_type == @intFromEnum(SlotType.STRUCT_MAP)) {
            // STRUCT_MAP: byte 13 = num_fields
            const nf: u32 = agg_type_byte;
            const rs: u32 = std.mem.readInt(u16, old_state_ptr[meta_base + 16 ..][0..2], .little);
            const has_ts = old_state_ptr[meta_base + 18] != 0;
            const desc_size = align8(nf);
            slot_size += getStructMapSlotDataSize(desc_size, cap, rs, has_ts);
            // Arena: if present, double arena capacity on growth; keep same on non-growth
            const arena_hdr_off = std.mem.readInt(u32, old_state_ptr[meta_base + 20 ..][0..4], .little);
            if (arena_hdr_off != 0) {
                const old_arena_cap = std.mem.readInt(u32, old_state_ptr[arena_hdr_off..][0..4], .little);
                const new_arena_cap = if (slot_i == grown_slot_idx) old_arena_cap * 2 else old_arena_cap;
                slot_size += ARENA_HEADER_SIZE + new_arena_cap;
            }
        } else if (slot_type == @intFromEnum(SlotType.ORDERED_LIST)) {
            // ORDERED_LIST: scalar or struct
            const elem_type_byte = old_state_ptr[meta_base + 18];
            const rs: u32 = std.mem.readInt(u16, old_state_ptr[meta_base + 16 ..][0..2], .little);
            if (elem_type_byte == 0xFF) {
                // Struct list: descriptor + rows
                const nf: u32 = old_state_ptr[meta_base + 13];
                slot_size += align8(nf) + cap * rs;
            } else {
                // Scalar list
                slot_size += cap * rs;
            }
        } else {
            slot_size += slot_growth.slotDataSize(SlotTypeFlags.fromByte(type_flags_byte).slot_type, cap, has_hashmap_timestamps, agg_type_byte);
        }

        slot_size += getTTLSideBufferSize(has_ttl, has_evict_trigger, cap);
        total_size += slot_size;
        total_size = align8(total_size);
    }

    return total_size;
}

/// Copy state from old buffer to new buffer, rehashing the grown slot.
/// new_state_ptr must point to a zeroed buffer of vm_calculate_grown_state_size() bytes.
/// Returns 0 on success.
pub export fn vm_grow_state(
    old_state_ptr: [*]const u8,
    new_state_ptr: [*]u8,
    _program_ptr: [*]const u8,
    _program_len: u32,
    grown_slot_idx: u32,
) u32 {
    _ = _program_ptr;
    _ = _program_len;
    const num_slots: u32 = old_state_ptr[9];

    // Copy header verbatim
    @memcpy(new_state_ptr[0..STATE_HEADER_SIZE], old_state_ptr[0..STATE_HEADER_SIZE]);

    // Compute new data offsets and build metadata
    var data_cursor: u32 = STATE_HEADER_SIZE + num_slots * SLOT_META_SIZE;
    data_cursor = (data_cursor + 7) & ~@as(u32, 7);

    var slot_i: u32 = 0;
    while (slot_i < num_slots) : (slot_i += 1) {
        const meta_base = STATE_HEADER_SIZE + slot_i * SLOT_META_SIZE;
        const old_offset = std.mem.readInt(u32, old_state_ptr[meta_base..][0..4], .little);
        const old_cap = std.mem.readInt(u32, old_state_ptr[meta_base + 4 ..][0..4], .little);
        const type_flags_byte = old_state_ptr[meta_base + 12];
        const slot_type: u4 = @truncate(type_flags_byte & 0x0F);
        const has_ttl = (type_flags_byte & 0x10) != 0;
        const has_evict_trigger = (type_flags_byte & 0x20) != 0;
        const has_hashmap_timestamps = (slot_type != @intFromEnum(SlotType.HASHMAP)) or ((type_flags_byte & 0x40) == 0);
        const agg_type_byte2 = old_state_ptr[meta_base + 13];

        const new_cap = if (slot_i == grown_slot_idx) nextPowerOf2(old_cap * 2) else old_cap;
        const new_offset = data_cursor;

        // Compute primary data size (STRUCT_MAP and ORDERED_LIST need metadata-based calculation)
        const new_primary_size = if (slot_type == @intFromEnum(SlotType.STRUCT_MAP)) blk: {
            // STRUCT_MAP
            const nf: u32 = old_state_ptr[meta_base + 13];
            const rs: u32 = std.mem.readInt(u16, old_state_ptr[meta_base + 16 ..][0..2], .little);
            const has_ts = old_state_ptr[meta_base + 18] != 0;
            break :blk getStructMapSlotDataSize(align8(nf), new_cap, rs, has_ts);
        } else if (slot_type == @intFromEnum(SlotType.ORDERED_LIST)) blk: {
            // ORDERED_LIST
            const elem_type_byte = old_state_ptr[meta_base + 18];
            const rs: u32 = std.mem.readInt(u16, old_state_ptr[meta_base + 16 ..][0..2], .little);
            if (elem_type_byte == 0xFF) {
                const nf: u32 = old_state_ptr[meta_base + 13];
                break :blk align8(nf) + new_cap * rs;
            } else {
                break :blk new_cap * rs;
            }
        } else slot_growth.slotDataSize(SlotTypeFlags.fromByte(type_flags_byte).slot_type, new_cap, has_hashmap_timestamps, agg_type_byte2);

        const eviction_index_offset = if (has_ttl) align8(new_offset + new_primary_size) else 0;
        const eviction_index_capacity = if (has_ttl) new_cap else 0;
        const evicted_buffer_offset = if (has_ttl and has_evict_trigger)
            align8(eviction_index_offset + eviction_index_capacity * @sizeOf(EvictionEntry))
        else
            0;

        // Write metadata: offset, capacity, size (size updated below for grown)
        std.mem.writeInt(u32, new_state_ptr[meta_base..][0..4], new_offset, .little);
        std.mem.writeInt(u32, new_state_ptr[meta_base + 4 ..][0..4], new_cap, .little);
        // Copy remaining metadata fields (type_flags, agg_type, change_flags, TTL, etc.)
        @memcpy(new_state_ptr[meta_base + 8 .. meta_base + SLOT_META_SIZE], old_state_ptr[meta_base + 8 .. meta_base + SLOT_META_SIZE]);
        std.mem.writeInt(u32, new_state_ptr[meta_base + SlotMetaOffset.EVICTION_INDEX_OFFSET ..][0..4], eviction_index_offset, .little);
        std.mem.writeInt(u32, new_state_ptr[meta_base + SlotMetaOffset.EVICTION_INDEX_CAPACITY ..][0..4], eviction_index_capacity, .little);
        std.mem.writeInt(u32, new_state_ptr[meta_base + SlotMetaOffset.EVICTED_BUFFER_OFFSET ..][0..4], evicted_buffer_offset, .little);

        if (slot_i == grown_slot_idx) {
            if (slot_type == @intFromEnum(SlotType.HASHMAP)) {
                // HASHMAP: rehash keys + values + optional timestamps via typed helper
                const has_ts = (type_flags_byte & 0x40) == 0;
                const rehashed_size = slot_growth.growHashMap(old_state_ptr, new_state_ptr, old_offset, new_offset, old_cap, new_cap, has_ts);
                std.mem.writeInt(u32, new_state_ptr[meta_base + 8 ..][0..4], rehashed_size, .little);
            } else if (slot_type == @intFromEnum(SlotType.HASHSET)) {
                // HASHSET: rehash keys via typed helper
                const rehashed_size = slot_growth.growHashSet(old_state_ptr, new_state_ptr, old_offset, new_offset, old_cap, new_cap);
                std.mem.writeInt(u32, new_state_ptr[meta_base + 8 ..][0..4], rehashed_size, .little);
            } else if (slot_type == @intFromEnum(SlotType.BITMAP)) {
                // BITMAP: copy serialized roaring payload as-is into larger slot.
                const old_storage_size = BITMAP_SERIALIZED_LEN_BYTES + bitmapPayloadCapacity(old_cap);
                const new_storage_size = BITMAP_SERIALIZED_LEN_BYTES + bitmapPayloadCapacity(new_cap);
                @memset(new_state_ptr[new_offset .. new_offset + new_storage_size], 0);
                @memcpy(
                    new_state_ptr[new_offset .. new_offset + old_storage_size],
                    old_state_ptr[old_offset .. old_offset + old_storage_size],
                );
            } else if (slot_type == @intFromEnum(SlotType.STRUCT_MAP)) {
                // STRUCT_MAP: rehash keys + rows via typed helper
                const nf: u32 = old_state_ptr[meta_base + 13];
                const rs: u32 = std.mem.readInt(u16, old_state_ptr[meta_base + 16 ..][0..2], .little);
                const desc_size = align8(nf);
                const new_keys_off = new_offset + desc_size;
                const new_keys: [*]u32 = @ptrCast(@alignCast(&new_state_ptr[new_keys_off]));
                const new_rows_base = new_keys_off + new_cap * 4;

                const rehashed_size = slot_growth.growStructMap(old_state_ptr, new_state_ptr, old_offset, new_offset, old_cap, new_cap, nf, rs);
                std.mem.writeInt(u32, new_state_ptr[meta_base + 8 ..][0..4], rehashed_size, .little);

                // Arena compaction: compact live array data into new arena
                const old_arena_hdr_off = std.mem.readInt(u32, old_state_ptr[meta_base + 20 ..][0..4], .little);
                if (old_arena_hdr_off != 0) {
                    const old_arena_cap = std.mem.readInt(u32, old_state_ptr[old_arena_hdr_off..][0..4], .little);
                    const new_arena_cap = if (slot_i == grown_slot_idx) old_arena_cap * 2 else old_arena_cap;
                    const struct_data_size = getStructMapSlotDataSize(desc_size, new_cap, rs, old_state_ptr[meta_base + 18] != 0);
                    const new_arena_hdr_off = new_offset + struct_data_size;

                    // Write new arena header
                    std.mem.writeInt(u32, new_state_ptr[new_arena_hdr_off..][0..4], new_arena_cap, .little);

                    // Update metadata to point to new arena header location
                    std.mem.writeInt(u32, new_state_ptr[meta_base + 20 ..][0..4], new_arena_hdr_off, .little);

                    // Compact: scan all live rows, copy their array data sequentially
                    const old_arena_data_base = old_arena_hdr_off + ARENA_HEADER_SIZE;
                    const new_arena_data_base = new_arena_hdr_off + ARENA_HEADER_SIZE;
                    const field_types_ptr: [*]const u8 = @ptrCast(new_state_ptr + new_offset);
                    var new_arena_used: u32 = 0;

                    for (0..new_cap) |ki| {
                        if (new_keys[ki] != EMPTY_KEY and new_keys[ki] != TOMBSTONE) {
                            const row_ptr = new_state_ptr + new_rows_base + @as(u32, @truncate(ki)) * rs;
                            const bitset_bytes_val = (nf + 7) / 8;

                            for (0..nf) |fi| {
                                const ft: StructFieldType = @enumFromInt(field_types_ptr[fi]);
                                if (!isArrayFieldType(ft)) continue;

                                // Check if field is set in bitset
                                const bit_set = (row_ptr[fi / 8] & (@as(u8, 1) << @as(u3, @truncate(fi % 8)))) != 0;
                                if (!bit_set) continue;

                                const f_off = structFieldOffset(@truncate(nf), field_types_ptr, @truncate(fi));
                                const old_offset_val = std.mem.readInt(u32, row_ptr[f_off..][0..4], .little);
                                const arr_len = std.mem.readInt(u32, row_ptr[f_off + 4 ..][0..4], .little);

                                if (arr_len == 0) continue;

                                const elem_sz = arenaElemSize(ft);
                                const byte_len = arr_len * elem_sz;

                                // Copy array data from old arena to new arena
                                const src = old_arena_data_base + old_offset_val;
                                const dst = new_arena_data_base + new_arena_used;
                                @memcpy(new_state_ptr[dst .. dst + byte_len], old_state_ptr[src .. src + byte_len]);

                                // Update row's (offset, length) to point to new location
                                std.mem.writeInt(u32, row_ptr[f_off..][0..4], new_arena_used, .little);
                                _ = bitset_bytes_val;
                                // length stays the same

                                new_arena_used += byte_len;
                            }
                        }
                    }

                    std.mem.writeInt(u32, new_state_ptr[new_arena_hdr_off + 4 ..][0..4], new_arena_used, .little);
                }
            } else if (slot_type == @intFromEnum(SlotType.ORDERED_LIST)) {
                // ORDERED_LIST: memcpy existing entries (no rehash needed)
                const elem_type_byte = old_state_ptr[meta_base + 18];
                const rs: u32 = std.mem.readInt(u16, old_state_ptr[meta_base + 16 ..][0..2], .little);
                const count = std.mem.readInt(u32, old_state_ptr[meta_base + 8 ..][0..4], .little);

                if (elem_type_byte == 0xFF) {
                    // Struct list: copy descriptor + existing rows
                    const nf: u32 = old_state_ptr[meta_base + 13];
                    const desc_size = align8(nf);
                    @memcpy(new_state_ptr[new_offset .. new_offset + nf], old_state_ptr[old_offset .. old_offset + nf]);
                    const old_rows = old_offset + desc_size;
                    const new_rows = new_offset + desc_size;
                    const copy_bytes = count * rs;
                    if (copy_bytes > 0) {
                        @memcpy(new_state_ptr[new_rows .. new_rows + copy_bytes], old_state_ptr[old_rows .. old_rows + copy_bytes]);
                    }
                } else {
                    // Scalar list: copy existing values
                    const copy_bytes = count * rs;
                    if (copy_bytes > 0) {
                        @memcpy(new_state_ptr[new_offset .. new_offset + copy_bytes], old_state_ptr[old_offset .. old_offset + copy_bytes]);
                    }
                }
            } else {
                // Non-hash slot: copy data (aggregates/condition trees shouldn't be grown)
                const old_data_size = slot_growth.slotDataSize(SlotTypeFlags.fromByte(type_flags_byte).slot_type, old_cap, has_hashmap_timestamps, agg_type_byte2);
                const copy_len = @min(old_data_size, new_primary_size);
                if (copy_len > 0) {
                    @memcpy(new_state_ptr[new_offset .. new_offset + copy_len], old_state_ptr[old_offset .. old_offset + copy_len]);
                }
            }
        } else {
            // Non-grown slot: memcpy data as-is (struct map data + arena if present)
            const primary_size = if (slot_type == @intFromEnum(SlotType.STRUCT_MAP)) blk: {
                // STRUCT_MAP
                const nf: u32 = old_state_ptr[meta_base + 13];
                const rs: u32 = std.mem.readInt(u16, old_state_ptr[meta_base + 16 ..][0..2], .little);
                const has_ts = old_state_ptr[meta_base + 18] != 0;
                var sz = getStructMapSlotDataSize(align8(nf), old_cap, rs, has_ts);
                // Include arena in copy
                const arena_hdr = std.mem.readInt(u32, old_state_ptr[meta_base + 20 ..][0..4], .little);
                if (arena_hdr != 0) {
                    const arena_cap = std.mem.readInt(u32, old_state_ptr[arena_hdr..][0..4], .little);
                    sz += ARENA_HEADER_SIZE + arena_cap;
                }
                break :blk sz;
            } else if (slot_type == @intFromEnum(SlotType.ORDERED_LIST)) blk: {
                // ORDERED_LIST
                const etb = old_state_ptr[meta_base + 18];
                const rs: u32 = std.mem.readInt(u16, old_state_ptr[meta_base + 16 ..][0..2], .little);
                if (etb == 0xFF) {
                    const nf: u32 = old_state_ptr[meta_base + 13];
                    break :blk align8(nf) + old_cap * rs;
                } else {
                    break :blk old_cap * rs;
                }
            } else slot_growth.slotDataSize(SlotTypeFlags.fromByte(type_flags_byte).slot_type, old_cap, has_hashmap_timestamps, agg_type_byte2);
            if (primary_size > 0) {
                @memcpy(new_state_ptr[new_offset .. new_offset + primary_size], old_state_ptr[old_offset .. old_offset + primary_size]);
            }
            // Update arena header offset in metadata for non-grown struct maps
            if (slot_type == @intFromEnum(SlotType.STRUCT_MAP)) {
                const old_arena_hdr = std.mem.readInt(u32, old_state_ptr[meta_base + 20 ..][0..4], .little);
                if (old_arena_hdr != 0) {
                    // Arena moved with the slot data — offset shifted by (new_offset - old_offset)
                    const nf2: u32 = old_state_ptr[meta_base + 13];
                    const rs2: u32 = std.mem.readInt(u16, old_state_ptr[meta_base + 16 ..][0..2], .little);
                    const has_ts2 = old_state_ptr[meta_base + 18] != 0;
                    const struct_sz = getStructMapSlotDataSize(align8(nf2), old_cap, rs2, has_ts2);
                    const new_arena_hdr = new_offset + struct_sz;
                    std.mem.writeInt(u32, new_state_ptr[meta_base + 20 ..][0..4], new_arena_hdr, .little);
                }
            }
        }

        if (has_ttl) {
            const old_eviction_index_offset = std.mem.readInt(u32, old_state_ptr[meta_base + SlotMetaOffset.EVICTION_INDEX_OFFSET ..][0..4], .little);
            const old_eviction_index_size = std.mem.readInt(u32, old_state_ptr[meta_base + SlotMetaOffset.EVICTION_INDEX_SIZE ..][0..4], .little);
            var copied_eviction_size: u32 = 0;

            if (old_eviction_index_offset != 0 and eviction_index_offset != 0 and old_eviction_index_size > 0) {
                copied_eviction_size = @min(old_eviction_index_size, eviction_index_capacity);
                const copy_bytes = copied_eviction_size * @sizeOf(EvictionEntry);
                @memcpy(
                    new_state_ptr[eviction_index_offset .. eviction_index_offset + copy_bytes],
                    old_state_ptr[old_eviction_index_offset .. old_eviction_index_offset + copy_bytes],
                );
            }
            std.mem.writeInt(u32, new_state_ptr[meta_base + SlotMetaOffset.EVICTION_INDEX_SIZE ..][0..4], copied_eviction_size, .little);

            if (has_evict_trigger) {
                const old_evicted_offset = std.mem.readInt(u32, old_state_ptr[meta_base + SlotMetaOffset.EVICTED_BUFFER_OFFSET ..][0..4], .little);
                const old_evicted_count = std.mem.readInt(u32, old_state_ptr[meta_base + SlotMetaOffset.EVICTED_COUNT ..][0..4], .little);
                var copied_evicted_count: u32 = 0;
                if (old_evicted_offset != 0 and evicted_buffer_offset != 0 and old_evicted_count > 0) {
                    copied_evicted_count = @min(old_evicted_count, 1024);
                    const copy_bytes = copied_evicted_count * @sizeOf(EvictionEntry);
                    @memcpy(
                        new_state_ptr[evicted_buffer_offset .. evicted_buffer_offset + copy_bytes],
                        old_state_ptr[old_evicted_offset .. old_evicted_offset + copy_bytes],
                    );
                }
                std.mem.writeInt(u32, new_state_ptr[meta_base + SlotMetaOffset.EVICTED_COUNT ..][0..4], copied_evicted_count, .little);
            } else {
                std.mem.writeInt(u32, new_state_ptr[meta_base + SlotMetaOffset.EVICTED_COUNT ..][0..4], 0, .little);
            }
        }

        var slot_total_size = new_primary_size;
        if (slot_type == @intFromEnum(SlotType.STRUCT_MAP)) {
            const arena_hdr_off = std.mem.readInt(u32, old_state_ptr[meta_base + 20 ..][0..4], .little);
            if (arena_hdr_off != 0) {
                const old_arena_cap = std.mem.readInt(u32, old_state_ptr[arena_hdr_off..][0..4], .little);
                const new_arena_cap = if (slot_i == grown_slot_idx) old_arena_cap * 2 else old_arena_cap;
                slot_total_size += ARENA_HEADER_SIZE + new_arena_cap;
            }
        }
        slot_total_size += getTTLSideBufferSize(has_ttl, has_evict_trigger, new_cap);
        data_cursor = align8(new_offset + slot_total_size);
    }

    return @intFromEnum(ErrorCode.OK);
}

// =============================================================================
// Tests — Slot Growth
// =============================================================================

/// Build a minimal program with one HashMap slot (BATCH_MAP_UPSERT_LAST)
/// and one Aggregate slot (BATCH_AGG_COUNT).
fn buildTestProgram(comptime cap_lo: u8, comptime cap_hi: u8) [64]u8 {
    var prog = [_]u8{0} ** 64;
    const content = prog[PROGRAM_HASH_PREFIX..];
    // Magic "CLM1" stored as bytes: 'A','X','E','1' → reads as LE u32 = 0x314D4C43
    content[0] = 0x41; // 'A'
    content[1] = 0x58; // 'X'
    content[2] = 0x45; // 'E'
    content[3] = 0x31; // '1'
    // Version: 1.0
    content[4] = 1;
    content[5] = 0;
    // num_slots = 2, num_inputs = 2
    content[6] = 2;
    content[7] = 2;
    // reserved
    content[8] = 0;
    content[9] = 0;
    // Init section: SLOT_DEF(5 bytes) × 2 = 10 bytes
    const init_len: u16 = 10;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);
    // Reduce section: BATCH_MAP_UPSERT_LAST(4 bytes) + BATCH_AGG_COUNT(2 bytes) = 6 bytes
    const reduce_len: u16 = 6;
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    // Init section starts at content[14]
    var off: usize = 14;
    // SLOT_DEF slot=0, type_flags=0x00 (HASHMAP), cap_lo, cap_hi
    content[off] = 0x10; // SLOT_DEF opcode
    content[off + 1] = 0; // slot index
    content[off + 2] = 0x00; // type_flags: HASHMAP=0, no TTL
    content[off + 3] = cap_lo;
    content[off + 4] = cap_hi;
    off += 5;
    // SLOT_DEF slot=1, type_flags=0x02 (AGGREGATE), cap_lo=2 (COUNT), cap_hi=0
    content[off] = 0x10; // SLOT_DEF opcode
    content[off + 1] = 1; // slot index
    content[off + 2] = 0x02; // type_flags: AGGREGATE=2
    content[off + 3] = 2; // AggType.COUNT
    content[off + 4] = 0;

    // Reduce section starts at content[14 + init_len]
    const reduce_start = 14 + init_len;
    // BATCH_MAP_UPSERT_LAST slot=0, key_col=0, val_col=1
    content[reduce_start] = 0x22;
    content[reduce_start + 1] = 0;
    content[reduce_start + 2] = 0;
    content[reduce_start + 3] = 1;
    // BATCH_AGG_COUNT slot=1
    content[reduce_start + 4] = 0x41;
    content[reduce_start + 5] = 1;

    return prog;
}

fn buildNoTimestampMapProgram(comptime cap_lo: u8, comptime cap_hi: u8, comptime map_opcode: u8, comptime needs_ts_col: bool) [80]u8 {
    var prog = [_]u8{0} ** 80;
    const content = prog[PROGRAM_HASH_PREFIX..];
    content[0] = 0x41; // 'A'
    content[1] = 0x58; // 'X'
    content[2] = 0x45; // 'E'
    content[3] = 0x31; // '1'
    content[4] = 1;
    content[5] = 0;
    content[6] = 1; // num_slots
    content[7] = if (needs_ts_col) 3 else 2;
    content[8] = 0;
    content[9] = 0;

    const init_len: u16 = 5;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);
    const reduce_len: u16 = if (needs_ts_col) 5 else 4;
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    var off: usize = 14;
    content[off] = 0x10; // SLOT_DEF
    content[off + 1] = 0;
    content[off + 2] = 0x40; // HASHMAP + NO_HASHMAP_TIMESTAMPS
    content[off + 3] = cap_lo;
    content[off + 4] = cap_hi;
    off += 5;

    content[off] = map_opcode;
    content[off + 1] = 0;
    content[off + 2] = 0;
    content[off + 3] = 1;
    if (needs_ts_col) {
        content[off + 4] = 2;
    }
    return prog;
}

fn writeF32LE(dst: []u8, value: f32) void {
    std.mem.writeInt(u32, dst[0..4], @bitCast(value), .little);
}

/// Build a minimal TTL HashMap program with one slot and one
/// BATCH_MAP_UPSERT_LATEST reducer opcode.
fn buildTTLMapProgram(comptime cap_lo: u8, comptime cap_hi: u8, comptime has_evict_trigger: bool) [96]u8 {
    var prog = [_]u8{0} ** 96;
    const content = prog[PROGRAM_HASH_PREFIX..];

    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    content[4] = 1;
    content[5] = 0;
    content[6] = 1; // num_slots
    content[7] = 3; // num_inputs (key, val, ts)
    content[8] = 0;
    content[9] = 0;

    // SLOT_DEF with TTL payload: 1(op)+1(slot)+1(type_flags)+1(cap_lo)+1(cap_hi)+10(ttl payload)
    const init_len: u16 = 15;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);

    // BATCH_MAP_UPSERT_LATEST slot,key,val,ts
    const reduce_len: u16 = 5;
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    var off: usize = 14;
    const ttl_flag: u8 = 0x10;
    const evict_flag: u8 = if (has_evict_trigger) 0x20 else 0;
    content[off] = 0x10; // SLOT_DEF
    content[off + 1] = 0;
    content[off + 2] = 0x00 | ttl_flag | evict_flag; // HASHMAP + flags
    content[off + 3] = cap_lo;
    content[off + 4] = cap_hi;
    writeF32LE(content[off + 5 .. off + 9], 10.0); // ttl_seconds
    writeF32LE(content[off + 9 .. off + 13], 0.0); // grace_seconds
    content[off + 13] = 2; // timestamp_field_idx
    content[off + 14] = @intFromEnum(DurationUnit.NONE);
    off += 15;

    content[off] = 0x20; // BATCH_MAP_UPSERT_LATEST
    content[off + 1] = 0; // slot
    content[off + 2] = 0; // key col
    content[off + 3] = 1; // val col
    content[off + 4] = 2; // ts col

    return prog;
}

/// Build a minimal TTL HashSet program with one slot and one BATCH_SET_INSERT reducer opcode.
fn buildTTLSetProgram(comptime cap_lo: u8, comptime cap_hi: u8) [96]u8 {
    var prog = [_]u8{0} ** 96;
    const content = prog[PROGRAM_HASH_PREFIX..];

    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    content[4] = 1;
    content[5] = 0;
    content[6] = 1; // num_slots
    content[7] = 2; // num_inputs (elem, ts)
    content[8] = 0;
    content[9] = 0;

    const init_len: u16 = 15;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);

    const reduce_len: u16 = 3;
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    var off: usize = 14;
    content[off] = 0x10; // SLOT_DEF
    content[off + 1] = 0;
    content[off + 2] = 0x01 | 0x10; // HASHSET + has_ttl
    content[off + 3] = cap_lo;
    content[off + 4] = cap_hi;
    writeF32LE(content[off + 5 .. off + 9], 10.0); // ttl_seconds
    writeF32LE(content[off + 9 .. off + 13], 0.0); // grace_seconds
    content[off + 13] = 1; // timestamp_field_idx
    content[off + 14] = @intFromEnum(DurationUnit.NONE);
    off += 15;

    content[off] = 0x30; // BATCH_SET_INSERT
    content[off + 1] = 0; // slot
    content[off + 2] = 0; // elem col

    return prog;
}

test "hashmap no-timestamp slot reduces state size" {
    var with_ts = buildNoTimestampMapProgram(4, 0, 0x22, false);
    const no_ts = buildNoTimestampMapProgram(4, 0, 0x22, false);
    with_ts[PROGRAM_HASH_PREFIX + 16] &= ~@as(u8, 0x40);

    const with_ts_size = vm_calculate_state_size(@ptrCast(&with_ts), with_ts.len);
    const no_ts_size = vm_calculate_state_size(@ptrCast(&no_ts), no_ts.len);

    // Effective capacity is nextPowerOf2(4 * 2) = 16, saving one f64 per entry.
    try std.testing.expectEqual(@as(u32, 16 * 8), with_ts_size - no_ts_size);
}

test "hashmap no-timestamp supports first and last semantics" {
    const first_prog = buildNoTimestampMapProgram(4, 0, 0x21, false);
    const last_prog = buildNoTimestampMapProgram(4, 0, 0x22, false);

    const size_first = vm_calculate_state_size(@ptrCast(&first_prog), first_prog.len);
    const size_last = vm_calculate_state_size(@ptrCast(&last_prog), last_prog.len);

    var first_buf = try std.testing.allocator.alignedAlloc(u8, std.mem.Alignment.of(u64), @intCast(size_first));
    defer std.testing.allocator.free(first_buf);
    @memset(first_buf, 0);
    var last_buf = try std.testing.allocator.alignedAlloc(u8, std.mem.Alignment.of(u64), @intCast(size_last));
    defer std.testing.allocator.free(last_buf);
    @memset(last_buf, 0);

    const first_state: [*]u8 = @ptrCast(first_buf.ptr);
    const last_state: [*]u8 = @ptrCast(last_buf.ptr);
    try std.testing.expectEqual(@as(u32, 0), vm_init_state(first_state, @ptrCast(&first_prog), first_prog.len));
    try std.testing.expectEqual(@as(u32, 0), vm_init_state(last_state, @ptrCast(&last_prog), last_prog.len));

    var keys = [3]u32{ 7, 7, 7 };
    var vals = [3]u32{ 10, 20, 30 };
    const col_ptrs = [2][*]const u8{ @ptrCast(&keys), @ptrCast(&vals) };

    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.OK)), vm_execute_batch(first_state, @ptrCast(&first_prog), first_prog.len, @ptrCast(&col_ptrs), 2, 3));
    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.OK)), vm_execute_batch(last_state, @ptrCast(&last_prog), last_prog.len, @ptrCast(&col_ptrs), 2, 3));

    const first_meta = STATE_HEADER_SIZE;
    const first_offset = std.mem.readInt(u32, first_buf[first_meta..][0..4], .little);
    const first_cap = std.mem.readInt(u32, first_buf[first_meta + 4 ..][0..4], .little);
    const last_offset = std.mem.readInt(u32, last_buf[first_meta..][0..4], .little);
    const last_cap = std.mem.readInt(u32, last_buf[first_meta + 4 ..][0..4], .little);

    try std.testing.expectEqual(@as(u32, 10), vm_map_get(first_state, first_offset, first_cap, 7));
    try std.testing.expectEqual(@as(u32, 30), vm_map_get(last_state, last_offset, last_cap, 7));
}

test "hashmap no-timestamp rejects latest/max/min opcodes" {
    const latest_prog = buildNoTimestampMapProgram(4, 0, 0x20, true);
    const max_prog = buildNoTimestampMapProgram(4, 0, 0x26, true);
    const min_prog = buildNoTimestampMapProgram(4, 0, 0x27, true);

    const progs = [_][80]u8{ latest_prog, max_prog, min_prog };
    for (progs) |prog| {
        const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
        const state_buf = try std.testing.allocator.alignedAlloc(u8, std.mem.Alignment.of(u64), @intCast(state_size));
        defer std.testing.allocator.free(state_buf);
        @memset(state_buf, 0);
        const state_ptr: [*]u8 = @ptrCast(state_buf.ptr);
        try std.testing.expectEqual(@as(u32, 0), vm_init_state(state_ptr, @ptrCast(&prog), prog.len));

        var keys = [1]u32{1};
        var vals = [1]u32{42};
        var cmp = [1]f64{123};
        const col_ptrs = [3][*]const u8{ @ptrCast(&keys), @ptrCast(&vals), @ptrCast(&cmp) };

        const result = vm_execute_batch(state_ptr, @ptrCast(&prog), prog.len, @ptrCast(&col_ptrs), 3, 1);
        try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.INVALID_PROGRAM)), result);
    }
}

test "hashmap no-timestamp growth preserves entries" {
    const prog = buildNoTimestampMapProgram(4, 0, 0x22, false);
    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    const state_buf = try std.testing.allocator.alignedAlloc(u8, std.mem.Alignment.of(u64), @intCast(state_size));
    defer std.testing.allocator.free(state_buf);
    @memset(state_buf, 0);
    const state_ptr: [*]u8 = @ptrCast(state_buf.ptr);
    try std.testing.expectEqual(@as(u32, 0), vm_init_state(state_ptr, @ptrCast(&prog), prog.len));

    var keys = [_]u32{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    var vals = [_]u32{ 0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 };
    const batch_ptrs = [2][*]const u8{ @ptrCast(&keys), @ptrCast(&vals) };
    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.OK)), vm_execute_batch(state_ptr, @ptrCast(&prog), prog.len, @ptrCast(&batch_ptrs), 2, 11));

    var overflow_key = [1]u32{100};
    var overflow_val = [1]u32{1000};
    const overflow_ptrs = [2][*]const u8{ @ptrCast(&overflow_key), @ptrCast(&overflow_val) };
    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.NEEDS_GROWTH)), vm_execute_batch(state_ptr, @ptrCast(&prog), prog.len, @ptrCast(&overflow_ptrs), 2, 1));

    const grown_size = vm_calculate_grown_state_size(state_ptr, @ptrCast(&prog), prog.len, 0);
    var grown_buf = try std.testing.allocator.alignedAlloc(u8, std.mem.Alignment.of(u64), @intCast(grown_size));
    defer std.testing.allocator.free(grown_buf);
    @memset(grown_buf, 0);
    const grown_ptr: [*]u8 = @ptrCast(grown_buf.ptr);
    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.OK)), vm_grow_state(state_ptr, grown_ptr, @ptrCast(&prog), prog.len, 0));
    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.OK)), vm_execute_batch(grown_ptr, @ptrCast(&prog), prog.len, @ptrCast(&overflow_ptrs), 2, 1));

    const meta_base = STATE_HEADER_SIZE;
    const cap = std.mem.readInt(u32, grown_buf[meta_base + 4 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 32), cap);
    for (0..11) |i| {
        try std.testing.expectEqual(vals[i], vm_map_get(grown_ptr, std.mem.readInt(u32, grown_buf[meta_base..][0..4], .little), cap, @intCast(i)));
    }
    try std.testing.expectEqual(@as(u32, 1000), vm_map_get(grown_ptr, std.mem.readInt(u32, grown_buf[meta_base..][0..4], .little), cap, 100));
}

test "hashmap invalid no-timestamp+ttl flag combination is rejected" {
    var prog = buildNoTimestampMapProgram(4, 0, 0x22, false);
    const content = prog[PROGRAM_HASH_PREFIX..];
    // Turn on has_ttl while keeping no_hashmap_timestamps.
    content[16] |= 0x10;
    // Extend init_len by 10 bytes and provide TTL payload.
    const init_len: u16 = 15;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);
    writeF32LE(content[19..23], 10.0);
    writeF32LE(content[23..27], 0.0);
    content[27] = 1;
    content[28] = @intFromEnum(DurationUnit.NONE);

    try std.testing.expectEqual(@as(u32, 0), vm_calculate_state_size(@ptrCast(&prog), prog.len));

    var state_buf = [_]u8{0} ** 256;
    const init_result = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);
    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.INVALID_PROGRAM)), init_result);
}

test "ttl hashmap - insert and evict through live reducer path" {
    const prog = buildTTLMapProgram(4, 0, true);
    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf = try std.testing.allocator.alignedAlloc(u8, std.mem.Alignment.of(u64), @intCast(state_size));
    defer std.testing.allocator.free(state_buf);
    @memset(state_buf, 0);
    const state_ptr: [*]u8 = @ptrCast(state_buf.ptr);

    const init_result = vm_init_state(state_ptr, @ptrCast(&prog), prog.len);
    try std.testing.expectEqual(@as(u32, 0), init_result);

    var key_col = [1]u32{42};
    var val_col = [1]u32{7};
    var ts_col = [1]f64{100.0};
    const col_ptrs = [3][*]const u8{ @ptrCast(&key_col), @ptrCast(&val_col), @ptrCast(&ts_col) };

    const exec_result = vm_execute_batch(state_ptr, @ptrCast(&prog), prog.len, @ptrCast(&col_ptrs), 3, 1);
    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.OK)), exec_result);

    const meta_base = STATE_HEADER_SIZE;
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const slot_cap = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    const before = vm_map_get(state_ptr, slot_offset, slot_cap, 42);
    try std.testing.expectEqual(@as(u32, 7), before);

    const evict_none = vm_evict_all_expired(state_ptr, 105.0);
    try std.testing.expectEqual(@as(u32, 0), evict_none);
    try std.testing.expectEqual(@as(u32, 7), vm_map_get(state_ptr, slot_offset, slot_cap, 42));

    const evict_one = vm_evict_all_expired(state_ptr, 111.0);
    try std.testing.expectEqual(@as(u32, 1), evict_one);
    try std.testing.expectEqual(EMPTY_KEY, vm_map_get(state_ptr, slot_offset, slot_cap, 42));
}

test "ttl hashmap - stale eviction entries do not evict newer values" {
    const prog = buildTTLMapProgram(4, 0, false);
    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf = try std.testing.allocator.alignedAlloc(u8, std.mem.Alignment.of(u64), @intCast(state_size));
    defer std.testing.allocator.free(state_buf);
    @memset(state_buf, 0);
    const state_ptr: [*]u8 = @ptrCast(state_buf.ptr);

    try std.testing.expectEqual(@as(u32, 0), vm_init_state(state_ptr, @ptrCast(&prog), prog.len));

    var key_col_a = [1]u32{9};
    var val_col_a = [1]u32{100};
    var ts_col_a = [1]f64{100.0};
    const ptrs_a = [3][*]const u8{ @ptrCast(&key_col_a), @ptrCast(&val_col_a), @ptrCast(&ts_col_a) };
    try std.testing.expectEqual(@as(u32, 0), vm_execute_batch(state_ptr, @ptrCast(&prog), prog.len, @ptrCast(&ptrs_a), 3, 1));

    var key_col_b = [1]u32{9};
    var val_col_b = [1]u32{200};
    var ts_col_b = [1]f64{200.0};
    const ptrs_b = [3][*]const u8{ @ptrCast(&key_col_b), @ptrCast(&val_col_b), @ptrCast(&ts_col_b) };
    try std.testing.expectEqual(@as(u32, 0), vm_execute_batch(state_ptr, @ptrCast(&prog), prog.len, @ptrCast(&ptrs_b), 3, 1));

    const meta_base = STATE_HEADER_SIZE;
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const slot_cap = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);

    // cutoff=140, so the stale ts=100 index entry is expired. It must not evict
    // the current key value with ts=200.
    const evicted = vm_evict_all_expired(state_ptr, 150.0);
    try std.testing.expectEqual(@as(u32, 0), evicted);
    try std.testing.expectEqual(@as(u32, 200), vm_map_get(state_ptr, slot_offset, slot_cap, 9));
}

test "ttl hashset - reinsertion refreshes ttl and evicts deterministically" {
    const prog = buildTTLSetProgram(4, 0);
    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf = try std.testing.allocator.alignedAlloc(u8, std.mem.Alignment.of(u64), @intCast(state_size));
    defer std.testing.allocator.free(state_buf);
    @memset(state_buf, 0);
    const state_ptr: [*]u8 = @ptrCast(state_buf.ptr);

    try std.testing.expectEqual(@as(u32, 0), vm_init_state(state_ptr, @ptrCast(&prog), prog.len));

    var elem_1 = [1]u32{77};
    var ts_1 = [1]f64{100.0};
    const ptrs_1 = [2][*]const u8{ @ptrCast(&elem_1), @ptrCast(&ts_1) };
    try std.testing.expectEqual(@as(u32, 0), vm_execute_batch(state_ptr, @ptrCast(&prog), prog.len, @ptrCast(&ptrs_1), 2, 1));

    var elem_2 = [1]u32{77};
    var ts_2 = [1]f64{200.0};
    const ptrs_2 = [2][*]const u8{ @ptrCast(&elem_2), @ptrCast(&ts_2) };
    try std.testing.expectEqual(@as(u32, 0), vm_execute_batch(state_ptr, @ptrCast(&prog), prog.len, @ptrCast(&ptrs_2), 2, 1));

    const meta_base = STATE_HEADER_SIZE;
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const slot_cap = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);

    try std.testing.expect(vm_set_contains(state_ptr, slot_offset, slot_cap, 77) == 1);

    // cutoff=140; refreshed timestamp=200 keeps element alive
    try std.testing.expectEqual(@as(u32, 0), vm_evict_all_expired(state_ptr, 150.0));
    try std.testing.expect(vm_set_contains(state_ptr, slot_offset, slot_cap, 77) == 1);

    // cutoff=201; refreshed timestamp now expires
    try std.testing.expectEqual(@as(u32, 1), vm_evict_all_expired(state_ptr, 211.0));
    try std.testing.expect(vm_set_contains(state_ptr, slot_offset, slot_cap, 77) == 0);
}

test "ttl eviction index overflow - returns NEEDS_GROWTH instead of dropping" {
    const prog = buildTTLMapProgram(4, 0, false);
    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf = try std.testing.allocator.alignedAlloc(u8, std.mem.Alignment.of(u64), @intCast(state_size));
    defer std.testing.allocator.free(state_buf);
    @memset(state_buf, 0);
    const state_ptr: [*]u8 = @ptrCast(state_buf.ptr);

    try std.testing.expectEqual(@as(u32, 0), vm_init_state(state_ptr, @ptrCast(&prog), prog.len));

    // Force tiny eviction index capacity to trigger deterministic overflow on
    // the second insert (policy path under test).
    const meta_base = STATE_HEADER_SIZE;
    std.mem.writeInt(u32, state_buf[meta_base + SlotMetaOffset.EVICTION_INDEX_CAPACITY ..][0..4], 1, .little);

    var key_col = [2]u32{ 1, 2 };
    var val_col = [2]u32{ 10, 20 };
    var ts_col = [2]f64{ 100.0, 101.0 };
    const ptrs = [3][*]const u8{ @ptrCast(&key_col), @ptrCast(&val_col), @ptrCast(&ts_col) };

    const result = vm_execute_batch(state_ptr, @ptrCast(&prog), prog.len, @ptrCast(&ptrs), 3, 2);
    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.NEEDS_GROWTH)), result);
    try std.testing.expectEqual(@as(u32, 0), vm_get_needs_growth_slot());
}

test "slot growth - hashmap capacity exceeded triggers NEEDS_GROWTH" {
    // Build program with tiny capacity (cap_lo=4, cap_hi=0 → effective capacity = nextPowerOf2(4*2) = 16)
    const prog = buildTestProgram(4, 0);

    // Calculate state size and allocate
    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [4096]u8 align(8) = [_]u8{0} ** 4096;
    std.debug.assert(state_size <= 4096);

    // Init state
    const init_result = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);
    try std.testing.expectEqual(@as(u32, 0), init_result);

    // Verify capacity is 16 (nextPowerOf2(4*2) = 16)
    const meta_base = STATE_HEADER_SIZE;
    const cap = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 16), cap);

    // Insert 11 unique keys (max_size = (16*7)/10 = 11, so first 11 fit exactly)
    var i: u32 = 0;
    while (i < 11) : (i += 1) {
        var key_col = [1]u32{i};
        var val_col = [1]u32{i * 10};
        const key_ptr: [*]const u8 = @ptrCast(&key_col);
        const val_ptr: [*]const u8 = @ptrCast(&val_col);
        const col_ptrs = [2][*]const u8{ key_ptr, val_ptr };
        const result = vm_execute_batch(
            @ptrCast(&state_buf),
            @ptrCast(&prog),
            prog.len,
            @ptrCast(&col_ptrs),
            2,
            1,
        );
        try std.testing.expectEqual(@as(u32, 0), result);
    }

    // Verify size = 11
    const size_after_11 = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 11), size_after_11);

    // 12th unique key should trigger NEEDS_GROWTH (size=11 >= max_size=11)
    var key_col_12 = [1]u32{100};
    var val_col_12 = [1]u32{1000};
    const key_ptr_12: [*]const u8 = @ptrCast(&key_col_12);
    const val_ptr_12: [*]const u8 = @ptrCast(&val_col_12);
    const col_ptrs_12 = [2][*]const u8{ key_ptr_12, val_ptr_12 };
    const result_12 = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs_12),
        2,
        1,
    );
    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.NEEDS_GROWTH)), result_12);

    // Verify g_needs_growth_slot is slot 0
    try std.testing.expectEqual(@as(u32, 0), vm_get_needs_growth_slot());
}

test "slot growth - grow preserves hashmap entries and aggregate" {
    const prog = buildTestProgram(4, 0);

    // Init state
    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [4096]u8 align(8) = [_]u8{0} ** 4096;
    std.debug.assert(state_size <= 4096);

    const init_result = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);
    try std.testing.expectEqual(@as(u32, 0), init_result);

    // Insert 11 unique keys
    var i: u32 = 0;
    while (i < 11) : (i += 1) {
        var key_col = [1]u32{i};
        var val_col = [1]u32{i * 10};
        const key_ptr: [*]const u8 = @ptrCast(&key_col);
        const val_ptr: [*]const u8 = @ptrCast(&val_col);
        const col_ptrs = [2][*]const u8{ key_ptr, val_ptr };
        _ = vm_execute_batch(
            @ptrCast(&state_buf),
            @ptrCast(&prog),
            prog.len,
            @ptrCast(&col_ptrs),
            2,
            1,
        );
    }

    // Verify aggregate count = 11 (11 batches of 1 row each)
    const agg_meta_base = STATE_HEADER_SIZE + SLOT_META_SIZE; // slot 1 meta
    const agg_data_offset = std.mem.readInt(u32, state_buf[agg_meta_base..][0..4], .little);
    const count_val = std.mem.readInt(u64, state_buf[agg_data_offset..][0..8], .little);
    try std.testing.expectEqual(@as(u64, 11), count_val);

    // Grow slot 0
    const grown_size = vm_calculate_grown_state_size(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        0,
    );
    var new_state_buf: [16384]u8 align(8) = [_]u8{0} ** 16384;
    std.debug.assert(grown_size <= 16384);

    const grow_result = vm_grow_state(
        @ptrCast(&state_buf),
        @ptrCast(&new_state_buf),
        @ptrCast(&prog),
        prog.len,
        0,
    );
    try std.testing.expectEqual(@as(u32, 0), grow_result);

    // Verify new capacity doubled: 16 → 32
    const new_meta_base = STATE_HEADER_SIZE;
    const new_cap = std.mem.readInt(u32, new_state_buf[new_meta_base + 4 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 32), new_cap);

    // Verify size preserved (11 entries rehashed)
    const new_size = std.mem.readInt(u32, new_state_buf[new_meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 11), new_size);

    // Verify all entries preserved via vm_map_get
    const new_data_offset = std.mem.readInt(u32, new_state_buf[new_meta_base..][0..4], .little);
    i = 0;
    while (i < 11) : (i += 1) {
        const result = vm_map_get(@ptrCast(&new_state_buf), new_data_offset, 32, i);
        try std.testing.expectEqual(i * 10, result);
    }

    // Verify aggregate count preserved (copied, not re-executed)
    const new_agg_meta_base = STATE_HEADER_SIZE + SLOT_META_SIZE;
    const new_agg_data_offset = std.mem.readInt(u32, new_state_buf[new_agg_meta_base..][0..4], .little);
    const new_count_val = std.mem.readInt(u64, new_state_buf[new_agg_data_offset..][0..8], .little);
    try std.testing.expectEqual(@as(u64, 11), new_count_val);

    // Verify we can now insert more entries without NEEDS_GROWTH
    // (new max_size = (32*7)/10 = 22, currently at 11)
    var key_col_new = [1]u32{100};
    var val_col_new = [1]u32{1000};
    const key_ptr_new: [*]const u8 = @ptrCast(&key_col_new);
    const val_ptr_new: [*]const u8 = @ptrCast(&val_col_new);
    const col_ptrs_new = [2][*]const u8{ key_ptr_new, val_ptr_new };
    const insert_result = vm_execute_batch(
        @ptrCast(&new_state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs_new),
        2,
        1,
    );
    try std.testing.expectEqual(@as(u32, 0), insert_result);

    // Verify the new entry is findable
    const lookup = vm_map_get(@ptrCast(&new_state_buf), new_data_offset, 32, 100);
    try std.testing.expectEqual(@as(u32, 1000), lookup);
}

test "slot growth - hashset growth preserves elements" {
    // Build program with one HashSet slot
    var prog = [_]u8{0} ** 56;
    const content = prog[PROGRAM_HASH_PREFIX..];
    content[0] = 0x41; // 'A'
    content[1] = 0x58; // 'X'
    content[2] = 0x45; // 'E'
    content[3] = 0x31; // '1'
    content[4] = 1;
    content[5] = 0;
    content[6] = 1; // num_slots = 1
    content[7] = 1; // num_inputs = 1
    content[8] = 0;
    content[9] = 0;
    const init_len: u16 = 5;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);
    const reduce_len: u16 = 3;
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    // SLOT_DEF slot=0, type_flags=0x01 (HASHSET), cap_lo=4, cap_hi=0 → cap=16
    content[14] = 0x10;
    content[15] = 0;
    content[16] = 0x01; // HASHSET
    content[17] = 4;
    content[18] = 0;
    // BATCH_SET_INSERT slot=0, elem_col=0
    const reduce_start = 14 + init_len;
    content[reduce_start] = 0x30;
    content[reduce_start + 1] = 0;
    content[reduce_start + 2] = 0;

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [4096]u8 align(8) = [_]u8{0} ** 4096;
    std.debug.assert(state_size <= 4096);

    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // Insert 11 elements
    var i: u32 = 0;
    while (i < 11) : (i += 1) {
        var elem_col = [1]u32{i + 1000}; // avoid EMPTY_KEY/TOMBSTONE
        const elem_ptr: [*]const u8 = @ptrCast(&elem_col);
        const col_ptrs = [1][*]const u8{elem_ptr};
        _ = vm_execute_batch(
            @ptrCast(&state_buf),
            @ptrCast(&prog),
            prog.len,
            @ptrCast(&col_ptrs),
            1,
            1,
        );
    }

    // Verify 12th triggers growth
    var elem_col_12 = [1]u32{2000};
    const elem_ptr_12: [*]const u8 = @ptrCast(&elem_col_12);
    const col_ptrs_12 = [1][*]const u8{elem_ptr_12};
    const result_12 = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs_12),
        1,
        1,
    );
    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.NEEDS_GROWTH)), result_12);

    // Grow
    const grown_size = vm_calculate_grown_state_size(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        0,
    );
    var new_state_buf: [16384]u8 align(8) = [_]u8{0} ** 16384;
    std.debug.assert(grown_size <= 16384);

    _ = vm_grow_state(
        @ptrCast(&state_buf),
        @ptrCast(&new_state_buf),
        @ptrCast(&prog),
        prog.len,
        0,
    );

    // Verify new capacity = 32
    const meta_base = STATE_HEADER_SIZE;
    const new_cap = std.mem.readInt(u32, new_state_buf[meta_base + 4 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 32), new_cap);

    // Verify all 11 elements preserved via vm_set_contains
    const new_data_offset = std.mem.readInt(u32, new_state_buf[meta_base..][0..4], .little);
    i = 0;
    while (i < 11) : (i += 1) {
        const contained = vm_set_contains(@ptrCast(&new_state_buf), new_data_offset, 32, i + 1000);
        try std.testing.expectEqual(@as(u32, 1), contained);
    }

    // Verify can insert 12th element now
    const insert_result = vm_execute_batch(
        @ptrCast(&new_state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs_12),
        1,
        1,
    );
    try std.testing.expectEqual(@as(u32, 0), insert_result);
}

// =============================================================================
// Tests — Struct Map
// =============================================================================

/// Build a program with one STRUCT_MAP slot (2 fields: UINT32 + STRING)
/// and BATCH_STRUCT_MAP_UPSERT_LAST that maps both fields.
fn buildStructMapTestProgram(comptime cap_lo: u8, comptime cap_hi: u8) [80]u8 {
    var prog = [_]u8{0} ** 80;
    const content = prog[PROGRAM_HASH_PREFIX..];
    // Magic "CLM1"
    content[0] = 0x41; // 'A'
    content[1] = 0x58; // 'X'
    content[2] = 0x45; // 'E'
    content[3] = 0x31; // '1'
    // Version: 1.0
    content[4] = 1;
    content[5] = 0;
    // num_slots = 1, num_inputs = 3
    content[6] = 1;
    content[7] = 3;
    // reserved
    content[8] = 0;
    content[9] = 0;
    // Init section: SLOT_STRUCT_MAP opcode(1) + slot(1) + type_flags(1) + cap_lo(1) + cap_hi(1)
    //               + num_fields(1) + field_types(2) = 8 bytes
    const init_len: u16 = 8;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);
    // Reduce section: BATCH_STRUCT_MAP_UPSERT_LAST opcode(1) + slot(1) + key_col(1) + num_vals(1)
    //                 + 2x (val_col(1) + field_idx(1)) + num_array_vals(1) = 9 bytes
    const reduce_len: u16 = 9;
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    // Init section at content[14]
    var off: usize = 14;
    // SLOT_STRUCT_MAP
    content[off] = 0x18; // SLOT_STRUCT_MAP opcode
    content[off + 1] = 0; // slot index
    content[off + 2] = @intFromEnum(SlotType.STRUCT_MAP); // type_flags: STRUCT_MAP, no TTL
    content[off + 3] = cap_lo;
    content[off + 4] = cap_hi;
    content[off + 5] = 2; // num_fields = 2
    content[off + 6] = 0; // field 0: UINT32
    content[off + 7] = 4; // field 1: STRING (interned u32)
    off += 8;

    // Reduce section at content[14 + init_len]
    const reduce_start = 14 + init_len;
    // BATCH_STRUCT_MAP_UPSERT_LAST slot=0, key_col=0, num_vals=2
    content[reduce_start] = 0x80;
    content[reduce_start + 1] = 0; // slot
    content[reduce_start + 2] = 0; // key_col
    content[reduce_start + 3] = 2; // num_vals
    // val_col=1, field_idx=0 (UINT32 field)
    content[reduce_start + 4] = 1;
    content[reduce_start + 5] = 0;
    // val_col=2, field_idx=1 (STRING field)
    content[reduce_start + 6] = 2;
    content[reduce_start + 7] = 1;
    // num_array_vals = 0
    content[reduce_start + 8] = 0;

    return prog;
}

test "struct map - init, upsert, and read back" {
    var prog = buildStructMapTestProgram(4, 0);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    try std.testing.expect(state_size > 0);

    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    std.debug.assert(state_size <= 8192);

    const init_result = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);
    try std.testing.expectEqual(@as(u32, 0), init_result);

    // Verify slot metadata
    const meta_base = STATE_HEADER_SIZE;
    const capacity = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    try std.testing.expect(capacity >= 16); // nextPowerOf2(4*2) = 16

    const num_fields = state_buf[meta_base + 13];
    try std.testing.expectEqual(@as(u8, 2), num_fields);

    const row_size: u32 = std.mem.readInt(u16, state_buf[meta_base + 16 ..][0..2], .little);
    // 2 fields: bitset=1 byte, UINT32=4 bytes, STRING=4 bytes => 9 bytes, aligned to 4 => 12
    try std.testing.expectEqual(@as(u32, 12), row_size);

    // Insert 3 entries: key=100 (val=42, str=1001), key=200 (val=99, str=1002), key=300 (val=7, str=1003)
    var key_col = [3]u32{ 100, 200, 300 };
    var val_col = [3]u32{ 42, 99, 7 };
    var str_col = [3]u32{ 1001, 1002, 1003 };
    const key_ptr: [*]const u8 = @ptrCast(&key_col);
    const val_ptr: [*]const u8 = @ptrCast(&val_col);
    const str_ptr: [*]const u8 = @ptrCast(&str_col);
    const col_ptrs = [3][*]const u8{ key_ptr, val_ptr, str_ptr };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        3,
        3,
    );
    try std.testing.expectEqual(@as(u32, 0), exec_result);

    // Verify size = 3
    const size_after = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 3), size_after);

    // Read back via vm_struct_map_get_row_ptr
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const row_off_100 = vm_struct_map_get_row_ptr(
        @ptrCast(&state_buf),
        slot_offset,
        capacity,
        num_fields,
        row_size,
        100,
    );
    try std.testing.expect(row_off_100 != 0xFFFFFFFF);

    // Check bitset: both fields set => byte 0 should have bits 0 and 1 set = 0x03
    try std.testing.expectEqual(@as(u8, 0x03), state_buf[row_off_100]);
    // Field 0 (UINT32) at offset 1 (after 1-byte bitset) => value 42
    const f0_val = std.mem.readInt(u32, state_buf[row_off_100 + 1 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 42), f0_val);
    // Field 1 (STRING) at offset 5 => value 1001
    const f1_val = std.mem.readInt(u32, state_buf[row_off_100 + 5 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 1001), f1_val);

    // Check key=200
    const row_off_200 = vm_struct_map_get_row_ptr(
        @ptrCast(&state_buf),
        slot_offset,
        capacity,
        num_fields,
        row_size,
        200,
    );
    try std.testing.expect(row_off_200 != 0xFFFFFFFF);
    const f0_val_200 = std.mem.readInt(u32, state_buf[row_off_200 + 1 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 99), f0_val_200);

    // Check non-existent key returns NOT_FOUND
    const row_off_missing = vm_struct_map_get_row_ptr(
        @ptrCast(&state_buf),
        slot_offset,
        capacity,
        num_fields,
        row_size,
        999,
    );
    try std.testing.expectEqual(@as(u32, 0xFFFFFFFF), row_off_missing);
}

test "struct map - upsert overwrites existing key" {
    var prog = buildStructMapTestProgram(4, 0);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    const meta_base = STATE_HEADER_SIZE;
    const capacity = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    const num_fields = state_buf[meta_base + 13];
    const row_size: u32 = std.mem.readInt(u16, state_buf[meta_base + 16 ..][0..2], .little);
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    _ = state_size;

    // Insert key=100, val=42, str=1001
    {
        var key_col = [1]u32{100};
        var val_col = [1]u32{42};
        var str_col = [1]u32{1001};
        const col_ptrs = [3][*]const u8{
            @ptrCast(&key_col),
            @ptrCast(&val_col),
            @ptrCast(&str_col),
        };
        _ = vm_execute_batch(
            @ptrCast(&state_buf),
            @ptrCast(&prog),
            prog.len,
            @ptrCast(&col_ptrs),
            3,
            1,
        );
    }

    // Overwrite key=100 with val=99, str=2002
    {
        var key_col = [1]u32{100};
        var val_col = [1]u32{99};
        var str_col = [1]u32{2002};
        const col_ptrs = [3][*]const u8{
            @ptrCast(&key_col),
            @ptrCast(&val_col),
            @ptrCast(&str_col),
        };
        _ = vm_execute_batch(
            @ptrCast(&state_buf),
            @ptrCast(&prog),
            prog.len,
            @ptrCast(&col_ptrs),
            3,
            1,
        );
    }

    // Size should still be 1 (upsert, not double-insert)
    const size_after = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 1), size_after);

    // Values should be overwritten
    const row_off = vm_struct_map_get_row_ptr(
        @ptrCast(&state_buf),
        slot_offset,
        capacity,
        num_fields,
        row_size,
        100,
    );
    try std.testing.expect(row_off != 0xFFFFFFFF);
    const f0_val = std.mem.readInt(u32, state_buf[row_off + 1 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 99), f0_val);
    const f1_val = std.mem.readInt(u32, state_buf[row_off + 5 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 2002), f1_val);
}

test "struct map - iteration" {
    var prog = buildStructMapTestProgram(4, 0);
    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    const meta_base = STATE_HEADER_SIZE;
    const capacity = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    const num_fields = state_buf[meta_base + 13];
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);

    // Insert 3 entries
    var key_col = [3]u32{ 100, 200, 300 };
    var val_col = [3]u32{ 10, 20, 30 };
    var str_col = [3]u32{ 1, 2, 3 };
    const col_ptrs = [3][*]const u8{
        @as([*]const u8, @ptrCast(&key_col)),
        @as([*]const u8, @ptrCast(&val_col)),
        @as([*]const u8, @ptrCast(&str_col)),
    };
    _ = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        3,
        3,
    );

    // Iterate and collect keys
    var found_keys: [3]u32 = undefined;
    var count: u32 = 0;
    var pos = vm_struct_map_iter_start(@ptrCast(&state_buf), slot_offset, capacity, num_fields);
    while (pos < capacity) {
        found_keys[count] = vm_struct_map_iter_key(@ptrCast(&state_buf), slot_offset, num_fields, pos);
        count += 1;
        pos = vm_struct_map_iter_next(@ptrCast(&state_buf), slot_offset, capacity, num_fields, pos);
    }
    try std.testing.expectEqual(@as(u32, 3), count);

    // All 3 keys should be present (order may vary due to hashing)
    var has_100 = false;
    var has_200 = false;
    var has_300 = false;
    for (0..count) |i| {
        if (found_keys[i] == 100) has_100 = true;
        if (found_keys[i] == 200) has_200 = true;
        if (found_keys[i] == 300) has_300 = true;
    }
    try std.testing.expect(has_100);
    try std.testing.expect(has_200);
    try std.testing.expect(has_300);
}

test "struct map - growth preserves entries" {
    // Use small capacity (cap=4 -> actual=16) to force growth
    var prog = buildStructMapTestProgram(4, 0);
    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    const meta_base = STATE_HEADER_SIZE;
    const num_fields = state_buf[meta_base + 13];
    const row_size: u32 = std.mem.readInt(u16, state_buf[meta_base + 16 ..][0..2], .little);

    // Insert 11 entries (70% of 16 = 11.2, so 12th should trigger growth)
    var i: u32 = 0;
    while (i < 11) : (i += 1) {
        var key_col = [1]u32{i + 1000};
        var val_col = [1]u32{i * 10};
        var str_col = [1]u32{i + 5000};
        const col_ptrs = [3][*]const u8{
            @ptrCast(&key_col),
            @ptrCast(&val_col),
            @ptrCast(&str_col),
        };
        const res = vm_execute_batch(
            @ptrCast(&state_buf),
            @ptrCast(&prog),
            prog.len,
            @ptrCast(&col_ptrs),
            3,
            1,
        );
        try std.testing.expectEqual(@as(u32, 0), res);
    }

    // 12th entry should trigger NEEDS_GROWTH
    var key_col_12 = [1]u32{2000};
    var val_col_12 = [1]u32{999};
    var str_col_12 = [1]u32{9999};
    const col_ptrs_12 = [3][*]const u8{
        @ptrCast(&key_col_12),
        @ptrCast(&val_col_12),
        @ptrCast(&str_col_12),
    };
    const result_12 = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs_12),
        3,
        1,
    );
    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.NEEDS_GROWTH)), result_12);

    // Grow
    const grown_size = vm_calculate_grown_state_size(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        0,
    );
    var new_state_buf: [32768]u8 align(8) = [_]u8{0} ** 32768;
    std.debug.assert(grown_size <= 32768);

    const grow_result = vm_grow_state(
        @ptrCast(&state_buf),
        @ptrCast(&new_state_buf),
        @ptrCast(&prog),
        prog.len,
        0,
    );
    try std.testing.expectEqual(@as(u32, 0), grow_result);

    // Verify new capacity = 32
    const new_cap = std.mem.readInt(u32, new_state_buf[meta_base + 4 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 32), new_cap);

    // Verify all 11 entries preserved
    const new_slot_offset = std.mem.readInt(u32, new_state_buf[meta_base..][0..4], .little);
    i = 0;
    while (i < 11) : (i += 1) {
        const row_off = vm_struct_map_get_row_ptr(
            @ptrCast(&new_state_buf),
            new_slot_offset,
            new_cap,
            num_fields,
            row_size,
            i + 1000,
        );
        try std.testing.expect(row_off != 0xFFFFFFFF);
        // Check UINT32 field value
        const f0 = std.mem.readInt(u32, new_state_buf[row_off + 1 ..][0..4], .little);
        try std.testing.expectEqual(i * 10, f0);
        // Check STRING field value
        const f1 = std.mem.readInt(u32, new_state_buf[row_off + 5 ..][0..4], .little);
        try std.testing.expectEqual(i + 5000, f1);
    }

    // Verify can insert 12th element now
    const insert_result = vm_execute_batch(
        @ptrCast(&new_state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs_12),
        3,
        1,
    );
    try std.testing.expectEqual(@as(u32, 0), insert_result);
}

// =============================================================================
// Tests — FOR_EACH_EVENT block-based execution
// =============================================================================

/// Build a program with one HashMap slot (HASHMAP, LAST strategy) and a
/// FOR_EACH_EVENT block wrapping MAP_UPSERT_LAST + AGG_COUNT.
/// Columns: 0=type_col(u32), 1=key_col(u32), 2=val_col(u32)
/// Slot 0: HashMap (cap_lo=4 → cap=16), Slot 1: Aggregate COUNT
fn buildForEachEventTestProgram(type_id: u32) [96]u8 {
    var prog = [_]u8{0} ** 96;
    const content = prog[PROGRAM_HASH_PREFIX..];
    // Magic "CLM1"
    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    // Version 1.0
    content[4] = 1;
    content[5] = 0;
    // num_slots=2, num_inputs=3
    content[6] = 2;
    content[7] = 3;
    content[8] = 0;
    content[9] = 0;

    // Init section: 2 slot defs
    // SLOT_DEF(HashMap) = 5 bytes + SLOT_DEF(Agg COUNT) = 5 bytes = 10
    const init_len: u16 = 10;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);

    // Reduce section: FOR_EACH_EVENT header(8) + body: MAP_UPSERT_LAST(4) + AGG_COUNT(2) = 14
    const body_len: u16 = 6; // MAP_UPSERT_LAST(4) + AGG_COUNT(2)
    const reduce_len: u16 = 8 + body_len; // FOR_EACH_EVENT header(8) + body
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    // Init section at content[14]
    var off: usize = 14;
    // Slot 0: HASHMAP, cap_lo=4, cap_hi=0
    content[off] = 0x10; // SLOT_DEF
    content[off + 1] = 0; // slot 0
    content[off + 2] = 0x00; // type_flags: HASHMAP=0
    content[off + 3] = 4; // cap_lo
    content[off + 4] = 0; // cap_hi
    off += 5;
    // Slot 1: AGGREGATE COUNT
    content[off] = 0x10; // SLOT_DEF
    content[off + 1] = 1; // slot 1
    content[off + 2] = 0x02; // type_flags: AGGREGATE=2
    content[off + 3] = @intFromEnum(AggType.COUNT); // aggType in cap_lo
    content[off + 4] = 0; // cap_hi
    off += 5;

    // Reduce section
    const rs = 14 + init_len;
    // FOR_EACH_EVENT opcode
    content[rs] = 0xE0;
    // type_col=0
    content[rs + 1] = 0;
    // type_id as u32 LE
    content[rs + 2] = @truncate(type_id);
    content[rs + 3] = @truncate(type_id >> 8);
    content[rs + 4] = @truncate(type_id >> 16);
    content[rs + 5] = @truncate(type_id >> 24);
    // body_len as u16 LE
    content[rs + 6] = @truncate(body_len);
    content[rs + 7] = @truncate(body_len >> 8);

    // Body: MAP_UPSERT_LAST(0x22) slot=0 key_col=1 val_col=2
    const body_start = rs + 8;
    content[body_start] = 0x22;
    content[body_start + 1] = 0; // slot
    content[body_start + 2] = 1; // key_col
    content[body_start + 3] = 2; // val_col

    // Body: AGG_COUNT(0x41) slot=1
    content[body_start + 4] = 0x41;
    content[body_start + 5] = 1; // slot

    return prog;
}

test "FOR_EACH_EVENT - basic type filtering" {
    const TYPE_A: u32 = 1001;
    const TYPE_B: u32 = 1002;

    var prog = buildForEachEventTestProgram(TYPE_A);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    std.debug.assert(state_size <= 8192);

    const init_result = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);
    try std.testing.expectEqual(@as(u32, 0), init_result);

    // Batch with mixed types: 2 events of TYPE_A, 1 event of TYPE_B
    // Only TYPE_A events should be processed (type_id in program = TYPE_A)
    var type_col = [3]u32{ TYPE_A, TYPE_B, TYPE_A };
    var key_col = [3]u32{ 100, 200, 300 };
    var val_col = [3]u32{ 10, 20, 30 };
    const col_ptrs = [3][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&key_col)),
        @as([*]const u8, @ptrCast(&val_col)),
    };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        3,
        3,
    );
    try std.testing.expectEqual(@as(u32, 0), exec_result);

    // HashMap (slot 0): should have 2 entries (keys 100, 300) — key 200 was TYPE_B
    const meta_base = STATE_HEADER_SIZE;
    const map_size = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 2), map_size);

    // Verify key 100 → val 10
    const data_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const cap = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 10), vm_map_get(@ptrCast(&state_buf), data_offset, cap, 100));
    // Verify key 300 → val 30
    try std.testing.expectEqual(@as(u32, 30), vm_map_get(@ptrCast(&state_buf), data_offset, cap, 300));
    // Verify key 200 NOT present (EMPTY_KEY = 0 returned for missing)
    try std.testing.expectEqual(EMPTY_KEY, vm_map_get(@ptrCast(&state_buf), data_offset, cap, 200));

    // Aggregate COUNT (slot 1): should be 2 (only TYPE_A events counted)
    // COUNT is compact 8-byte slot: u64 count at offset 0 (no f64 value prefix)
    const agg_meta_base = STATE_HEADER_SIZE + SLOT_META_SIZE;
    const agg_offset = std.mem.readInt(u32, state_buf[agg_meta_base..][0..4], .little);
    const agg_count: u64 = std.mem.readInt(u64, state_buf[agg_offset..][0..8], .little);
    try std.testing.expectEqual(@as(u64, 2), agg_count);
}

test "FOR_EACH_EVENT - all events match" {
    const TYPE_A: u32 = 1001;
    var prog = buildForEachEventTestProgram(TYPE_A);

    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // All 3 events match TYPE_A
    var type_col = [3]u32{ TYPE_A, TYPE_A, TYPE_A };
    var key_col = [3]u32{ 10, 20, 30 };
    var val_col = [3]u32{ 100, 200, 300 };
    const col_ptrs = [3][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&key_col)),
        @as([*]const u8, @ptrCast(&val_col)),
    };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        3,
        3,
    );
    try std.testing.expectEqual(@as(u32, 0), exec_result);

    // All 3 should be in the map
    const meta_base = STATE_HEADER_SIZE;
    const map_size = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 3), map_size);

    // Count should be 3
    const agg_meta_base = STATE_HEADER_SIZE + SLOT_META_SIZE;
    const agg_offset = std.mem.readInt(u32, state_buf[agg_meta_base..][0..4], .little);
    const agg_count: u64 = std.mem.readInt(u64, state_buf[agg_offset..][0..8], .little);
    try std.testing.expectEqual(@as(u64, 3), agg_count);
}

test "FOR_EACH_EVENT - no events match" {
    const TYPE_A: u32 = 1001;
    const TYPE_B: u32 = 1002;
    var prog = buildForEachEventTestProgram(TYPE_A);

    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // No events match — all are TYPE_B
    var type_col = [2]u32{ TYPE_B, TYPE_B };
    var key_col = [2]u32{ 10, 20 };
    var val_col = [2]u32{ 100, 200 };
    const col_ptrs = [3][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&key_col)),
        @as([*]const u8, @ptrCast(&val_col)),
    };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        3,
        2,
    );
    try std.testing.expectEqual(@as(u32, 0), exec_result);

    // Map should be empty
    const meta_base = STATE_HEADER_SIZE;
    const map_size = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 0), map_size);

    // Count should be 0
    const agg_meta_base = STATE_HEADER_SIZE + SLOT_META_SIZE;
    const agg_offset = std.mem.readInt(u32, state_buf[agg_meta_base..][0..4], .little);
    const agg_count: u64 = std.mem.readInt(u64, state_buf[agg_offset..][0..8], .little);
    try std.testing.expectEqual(@as(u64, 0), agg_count);
}

// =============================================================================
// Tests — FOR_EACH_EVENT with STRUCT_MAP
// =============================================================================

/// Build a program with:
///   Slot 0: STRUCT_MAP (2 fields: UINT32 + STRING, cap=16)
/// Reduce: FOR_EACH_EVENT(type_col=0, type_id) { STRUCT_MAP_UPSERT_LAST }
/// Columns: 0=type, 1=key, 2=uint32_val, 3=string_val
fn buildBlockStructMapTestProgram(type_id: u32) [96]u8 {
    var prog = [_]u8{0} ** 96;
    const content = prog[PROGRAM_HASH_PREFIX..];
    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    content[4] = 1;
    content[5] = 0;
    content[6] = 1; // num_slots
    content[7] = 4; // num_inputs
    content[8] = 0;
    content[9] = 0;

    // Init: SLOT_STRUCT_MAP (8 bytes)
    const init_len: u16 = 8;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);

    // Reduce: FOR_EACH_EVENT(8) + body: STRUCT_MAP_UPSERT_LAST(9) = 17
    const body_len: u16 = 9; // op(1)+slot(1)+key_col(1)+num_vals(1)+2*(val_col+field_idx)+num_array_vals(1)
    const reduce_len: u16 = 8 + body_len;
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    // Init section
    var off: usize = 14;
    content[off] = 0x18; // SLOT_STRUCT_MAP
    content[off + 1] = 0;
    content[off + 2] = @intFromEnum(SlotType.STRUCT_MAP); // STRUCT_MAP
    content[off + 3] = 4; // cap_lo
    content[off + 4] = 0;
    content[off + 5] = 2; // num_fields
    content[off + 6] = 0; // UINT32
    content[off + 7] = 4; // STRING
    off += 8;

    // Reduce section
    const rs = 14 + init_len;
    content[rs] = 0xE0; // FOR_EACH_EVENT
    content[rs + 1] = 0; // type_col
    content[rs + 2] = @truncate(type_id);
    content[rs + 3] = @truncate(type_id >> 8);
    content[rs + 4] = @truncate(type_id >> 16);
    content[rs + 5] = @truncate(type_id >> 24);
    content[rs + 6] = @truncate(body_len);
    content[rs + 7] = @truncate(body_len >> 8);

    // Body: STRUCT_MAP_UPSERT_LAST
    const bs = rs + 8;
    content[bs] = 0x80;
    content[bs + 1] = 0; // slot
    content[bs + 2] = 1; // key_col
    content[bs + 3] = 2; // num_vals
    content[bs + 4] = 2; // val_col=2, field_idx=0
    content[bs + 5] = 0;
    content[bs + 6] = 3; // val_col=3, field_idx=1
    content[bs + 7] = 1;
    content[bs + 8] = 0; // num_array_vals = 0

    return prog;
}

test "FOR_EACH_EVENT - struct map upsert with type filtering" {
    const TYPE_A: u32 = 1001;
    const TYPE_B: u32 = 1002;
    var prog = buildBlockStructMapTestProgram(TYPE_A);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    std.debug.assert(state_size <= 8192);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // 3 events: TYPE_A, TYPE_B, TYPE_A — only 2 should be inserted
    var type_col = [3]u32{ TYPE_A, TYPE_B, TYPE_A };
    var key_col = [3]u32{ 100, 200, 300 };
    var val_col = [3]u32{ 42, 99, 7 };
    var str_col = [3]u32{ 1001, 1002, 1003 };
    const col_ptrs = [4][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&key_col)),
        @as([*]const u8, @ptrCast(&val_col)),
        @as([*]const u8, @ptrCast(&str_col)),
    };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        4,
        3,
    );
    try std.testing.expectEqual(@as(u32, 0), exec_result);

    // Struct map should have 2 entries (keys 100, 300)
    const meta_base = STATE_HEADER_SIZE;
    const sm_size = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 2), sm_size);

    // Verify key 100 present
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const capacity = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    const num_fields = state_buf[meta_base + 13];
    const row_size: u32 = std.mem.readInt(u16, state_buf[meta_base + 16 ..][0..2], .little);

    const row_off = vm_struct_map_get_row_ptr(
        @ptrCast(&state_buf),
        slot_offset,
        capacity,
        num_fields,
        row_size,
        100,
    );
    try std.testing.expect(row_off != 0xFFFFFFFF);
    const f0_val = std.mem.readInt(u32, state_buf[row_off + 1 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 42), f0_val);

    // Verify key 200 NOT present (was TYPE_B)
    const row_off_200 = vm_struct_map_get_row_ptr(
        @ptrCast(&state_buf),
        slot_offset,
        capacity,
        num_fields,
        row_size,
        200,
    );
    try std.testing.expectEqual(@as(u32, 0xFFFFFFFF), row_off_200);
}

// =============================================================================
// Tests — FLAT_MAP
// =============================================================================

/// Build a program with:
///   Slot 0: STRUCT_MAP (2 fields: UINT32 + STRING, cap=16)
/// Reduce: FOR_EACH_EVENT(type_col=0, type_id) {
///            FLAT_MAP(offsets_col=1, parent_ts_col=0xFF) {
///              STRUCT_MAP_UPSERT_LAST(key_col=2, ...)
///            }
///          }
/// Columns: 0=type, 1=offsets(u32), 2=child_key(u32), 3=child_val0(u32), 4=child_val1(u32)
fn buildFlatMapTestProgram(type_id: u32) [112]u8 {
    var prog = [_]u8{0} ** 112;
    const content = prog[PROGRAM_HASH_PREFIX..];
    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    content[4] = 1;
    content[5] = 0;
    content[6] = 1; // num_slots
    content[7] = 5; // num_inputs
    content[8] = 0;
    content[9] = 0;

    const init_len: u16 = 8; // SLOT_STRUCT_MAP
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);

    // Body of FLAT_MAP: STRUCT_MAP_UPSERT_LAST = 9 bytes (8 + num_array_vals)
    const inner_body_len: u16 = 9;
    // FLAT_MAP header: op(1)+offsets_col(1)+parent_ts_col(1)+inner_body_len(2) = 5
    const flat_map_total: u16 = 5 + inner_body_len;
    // FOR_EACH_EVENT header: 8 bytes, body = flat_map_total
    const reduce_len: u16 = 8 + flat_map_total;
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    // Init section
    var off: usize = 14;
    content[off] = 0x18;
    content[off + 1] = 0;
    content[off + 2] = 0x05;
    content[off + 3] = 4; // cap_lo
    content[off + 4] = 0;
    content[off + 5] = 2; // num_fields
    content[off + 6] = 0; // UINT32
    content[off + 7] = 4; // STRING
    off += 8;

    // Reduce section
    const rs = 14 + init_len;
    // FOR_EACH_EVENT
    content[rs] = 0xE0;
    content[rs + 1] = 0; // type_col
    content[rs + 2] = @truncate(type_id);
    content[rs + 3] = @truncate(type_id >> 8);
    content[rs + 4] = @truncate(type_id >> 16);
    content[rs + 5] = @truncate(type_id >> 24);
    content[rs + 6] = @truncate(flat_map_total);
    content[rs + 7] = @truncate(flat_map_total >> 8);

    // FLAT_MAP
    const fm = rs + 8;
    content[fm] = 0xE1; // FLAT_MAP
    content[fm + 1] = 1; // offsets_col
    content[fm + 2] = 0xFF; // parent_ts_col (unused)
    content[fm + 3] = @truncate(inner_body_len);
    content[fm + 4] = @truncate(inner_body_len >> 8);

    // Inner body: STRUCT_MAP_UPSERT_LAST
    const ib = fm + 5;
    content[ib] = 0x80;
    content[ib + 1] = 0; // slot
    content[ib + 2] = 2; // key_col (child column)
    content[ib + 3] = 2; // num_vals
    content[ib + 4] = 3; // val_col=3, field_idx=0
    content[ib + 5] = 0;
    content[ib + 6] = 4; // val_col=4, field_idx=1
    content[ib + 7] = 1;
    content[ib + 8] = 0; // num_array_vals = 0

    return prog;
}

test "FLAT_MAP - basic: 2 parents with children" {
    const TYPE_A: u32 = 1001;
    var prog = buildFlatMapTestProgram(TYPE_A);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    std.debug.assert(state_size <= 8192);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // 2 parent events (both TYPE_A):
    //   Parent 0: 2 children (offsets[0]=0, offsets[1]=2)
    //   Parent 1: 1 child   (offsets[1]=2, offsets[2]=3)
    var type_col = [2]u32{ TYPE_A, TYPE_A };
    // Offsets: [0, 2, 3] — need batch_len+1 entries
    var offsets_col = [3]u32{ 0, 2, 3 };
    // Child columns (3 total children)
    var child_key_col = [3]u32{ 100, 200, 300 };
    var child_val0_col = [3]u32{ 10, 20, 30 };
    var child_val1_col = [3]u32{ 1001, 1002, 1003 };

    const col_ptrs = [5][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&offsets_col)),
        @as([*]const u8, @ptrCast(&child_key_col)),
        @as([*]const u8, @ptrCast(&child_val0_col)),
        @as([*]const u8, @ptrCast(&child_val1_col)),
    };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        5,
        2, // batch_len = 2 parent events
    );
    try std.testing.expectEqual(@as(u32, 0), exec_result);

    // Should have 3 entries in struct map
    const meta_base = STATE_HEADER_SIZE;
    const sm_size = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 3), sm_size);

    // Verify all 3 child keys present
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const capacity = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    const num_fields = state_buf[meta_base + 13];
    const row_size: u32 = std.mem.readInt(u16, state_buf[meta_base + 16 ..][0..2], .little);

    // Key 100 → val0=10, val1=1001
    const row_100 = vm_struct_map_get_row_ptr(@ptrCast(&state_buf), slot_offset, capacity, num_fields, row_size, 100);
    try std.testing.expect(row_100 != 0xFFFFFFFF);
    try std.testing.expectEqual(@as(u32, 10), std.mem.readInt(u32, state_buf[row_100 + 1 ..][0..4], .little));
    try std.testing.expectEqual(@as(u32, 1001), std.mem.readInt(u32, state_buf[row_100 + 5 ..][0..4], .little));

    // Key 200 → val0=20, val1=1002
    const row_200 = vm_struct_map_get_row_ptr(@ptrCast(&state_buf), slot_offset, capacity, num_fields, row_size, 200);
    try std.testing.expect(row_200 != 0xFFFFFFFF);
    try std.testing.expectEqual(@as(u32, 20), std.mem.readInt(u32, state_buf[row_200 + 1 ..][0..4], .little));

    // Key 300 → val0=30, val1=1003
    const row_300 = vm_struct_map_get_row_ptr(@ptrCast(&state_buf), slot_offset, capacity, num_fields, row_size, 300);
    try std.testing.expect(row_300 != 0xFFFFFFFF);
    try std.testing.expectEqual(@as(u32, 30), std.mem.readInt(u32, state_buf[row_300 + 1 ..][0..4], .little));
}

test "FLAT_MAP - empty parent (zero children)" {
    const TYPE_A: u32 = 1001;
    var prog = buildFlatMapTestProgram(TYPE_A);

    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // 2 parents: first has 0 children, second has 1 child
    var type_col = [2]u32{ TYPE_A, TYPE_A };
    var offsets_col = [3]u32{ 0, 0, 1 }; // parent 0: [0,0), parent 1: [0,1)
    var child_key_col = [1]u32{500};
    var child_val0_col = [1]u32{55};
    var child_val1_col = [1]u32{9999};

    const col_ptrs = [5][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&offsets_col)),
        @as([*]const u8, @ptrCast(&child_key_col)),
        @as([*]const u8, @ptrCast(&child_val0_col)),
        @as([*]const u8, @ptrCast(&child_val1_col)),
    };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        5,
        2,
    );
    try std.testing.expectEqual(@as(u32, 0), exec_result);

    // Should have 1 entry (only from parent 1's child)
    const meta_base = STATE_HEADER_SIZE;
    const sm_size = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 1), sm_size);
}

test "FLAT_MAP - key collision: last child wins" {
    const TYPE_A: u32 = 1001;
    var prog = buildFlatMapTestProgram(TYPE_A);

    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // 1 parent with 2 children that have the SAME key
    var type_col = [1]u32{TYPE_A};
    var offsets_col = [2]u32{ 0, 2 };
    var child_key_col = [2]u32{ 100, 100 }; // same key
    var child_val0_col = [2]u32{ 10, 99 }; // different values — last wins
    var child_val1_col = [2]u32{ 1001, 2002 };

    const col_ptrs = [5][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&offsets_col)),
        @as([*]const u8, @ptrCast(&child_key_col)),
        @as([*]const u8, @ptrCast(&child_val0_col)),
        @as([*]const u8, @ptrCast(&child_val1_col)),
    };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        5,
        1,
    );
    try std.testing.expectEqual(@as(u32, 0), exec_result);

    // Should have 1 entry (both children had same key)
    const meta_base = STATE_HEADER_SIZE;
    const sm_size = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 1), sm_size);

    // Last child wins: val0=99, val1=2002
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const capacity = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    const num_fields = state_buf[meta_base + 13];
    const row_size: u32 = std.mem.readInt(u16, state_buf[meta_base + 16 ..][0..2], .little);
    const row_off = vm_struct_map_get_row_ptr(@ptrCast(&state_buf), slot_offset, capacity, num_fields, row_size, 100);
    try std.testing.expect(row_off != 0xFFFFFFFF);
    try std.testing.expectEqual(@as(u32, 99), std.mem.readInt(u32, state_buf[row_off + 1 ..][0..4], .little));
    try std.testing.expectEqual(@as(u32, 2002), std.mem.readInt(u32, state_buf[row_off + 5 ..][0..4], .little));
}

test "FLAT_MAP - type filtering skips non-matching parents" {
    const TYPE_A: u32 = 1001;
    const TYPE_B: u32 = 1002;
    var prog = buildFlatMapTestProgram(TYPE_A);

    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // 3 parents: TYPE_A (2 children), TYPE_B (1 child), TYPE_A (1 child)
    // TYPE_B parent's children should be skipped
    var type_col = [3]u32{ TYPE_A, TYPE_B, TYPE_A };
    var offsets_col = [4]u32{ 0, 2, 3, 4 };
    // 4 total children, but child at index 2 belongs to TYPE_B parent
    var child_key_col = [4]u32{ 100, 200, 999, 300 };
    var child_val0_col = [4]u32{ 10, 20, 99, 30 };
    var child_val1_col = [4]u32{ 1001, 1002, 9999, 1003 };

    const col_ptrs = [5][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&offsets_col)),
        @as([*]const u8, @ptrCast(&child_key_col)),
        @as([*]const u8, @ptrCast(&child_val0_col)),
        @as([*]const u8, @ptrCast(&child_val1_col)),
    };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        5,
        3,
    );
    try std.testing.expectEqual(@as(u32, 0), exec_result);

    // Should have 3 entries (keys 100, 200, 300) — NOT key 999
    const meta_base = STATE_HEADER_SIZE;
    const sm_size = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 3), sm_size);

    // Verify key 999 NOT present
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const capacity = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    const num_fields = state_buf[meta_base + 13];
    const row_size: u32 = std.mem.readInt(u16, state_buf[meta_base + 16 ..][0..2], .little);
    const row_999 = vm_struct_map_get_row_ptr(@ptrCast(&state_buf), slot_offset, capacity, num_fields, row_size, 999);
    try std.testing.expectEqual(@as(u32, 0xFFFFFFFF), row_999);

    // Verify key 300 IS present
    const row_300 = vm_struct_map_get_row_ptr(@ptrCast(&state_buf), slot_offset, capacity, num_fields, row_size, 300);
    try std.testing.expect(row_300 != 0xFFFFFFFF);
}

test "FLAT_MAP - growth: small capacity triggers NEEDS_GROWTH" {
    const TYPE_A: u32 = 1001;
    // Minimum struct map capacity = 16 (nextPowerOf2 floors at 16), max_size = 16*7/10 = 11
    // Need 12 unique keys to trigger NEEDS_GROWTH
    var prog = buildFlatMapTestProgram(TYPE_A); // cap=16 (from cap_lo=4 → nextPowerOf2(8)=16)

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    std.debug.assert(state_size <= 8192);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // 1 parent with 12 children (12 unique keys → exceeds max_size=11)
    var type_col = [1]u32{TYPE_A};
    var offsets_col = [2]u32{ 0, 12 };
    var child_key_col = [12]u32{ 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200 };
    var child_val0_col = [12]u32{ 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120 };
    var child_val1_col = [12]u32{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };

    const col_ptrs = [5][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&offsets_col)),
        @as([*]const u8, @ptrCast(&child_key_col)),
        @as([*]const u8, @ptrCast(&child_val0_col)),
        @as([*]const u8, @ptrCast(&child_val1_col)),
    };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        5,
        1,
    );
    // Should trigger NEEDS_GROWTH (error code 5) because 12 unique keys > max_size=11
    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.NEEDS_GROWTH)), exec_result);
}

// =============================================================================
// Tests — FLAT_MAP + event-level ops in same FOR_EACH_EVENT block
// =============================================================================

/// Build a program with:
///   Slot 0: STRUCT_MAP (2 fields: UINT32 + STRING, cap=16) — updated via FLAT_MAP
///   Slot 1: AGGREGATE SUM — updated at event level (non-flat)
/// FOR_EACH_EVENT block body: AGG_SUM(slot=1, val_col=2) + FLAT_MAP { STRUCT_MAP_UPSERT_LAST }
/// Columns: 0=type, 1=offsets(u32), 2=parent_amount(f64), 3=child_key(u32), 4=child_val0(u32), 5=child_val1(u32)
fn buildMixedBlockTestProgram(type_id: u32) [160]u8 {
    var prog = [_]u8{0} ** 160;
    const content = prog[PROGRAM_HASH_PREFIX..];
    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    content[4] = 1;
    content[5] = 0;
    content[6] = 2; // num_slots (struct map + aggregate)
    content[7] = 6; // num_inputs
    content[8] = 0;
    content[9] = 0;

    // Init section: SLOT_STRUCT_MAP(8) + SLOT_DEF for aggregate(5)
    const init_len: u16 = 8 + 5;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);

    // Body = AGG_SUM(3) + FLAT_MAP(5 + 9)
    const inner_body_len: u16 = 9; // STRUCT_MAP_UPSERT_LAST (8 + num_array_vals)
    const flat_map_total: u16 = 5 + inner_body_len;
    const agg_op_len: u16 = 3; // AGG_SUM: op + slot + val_col
    const reduce_len: u16 = 8 + agg_op_len + flat_map_total; // FOR_EACH_EVENT header + body
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    // Init section
    var off: usize = 14;
    // Slot 0: SLOT_STRUCT_MAP
    content[off] = 0x18;
    content[off + 1] = 0; // slot
    content[off + 2] = @intFromEnum(SlotType.STRUCT_MAP); // STRUCT_MAP
    content[off + 3] = 4; // cap_lo
    content[off + 4] = 0;
    content[off + 5] = 2; // num_fields
    content[off + 6] = 0; // UINT32
    content[off + 7] = 4; // STRING
    off += 8;

    // Slot 1: SLOT_DEF for AGGREGATE
    content[off] = @intFromEnum(Opcode.SLOT_DEF);
    content[off + 1] = 1; // slot
    content[off + 2] = @intFromEnum(SlotType.AGGREGATE); // type_flags: AGGREGATE=3
    content[off + 3] = 0; // cap_lo (unused for aggregate)
    content[off + 4] = 0; // cap_hi
    off += 5;

    // Reduce section
    const rs = 14 + init_len;
    // FOR_EACH_EVENT header
    content[rs] = 0xE0;
    content[rs + 1] = 0; // type_col
    content[rs + 2] = @truncate(type_id);
    content[rs + 3] = @truncate(type_id >> 8);
    content[rs + 4] = @truncate(type_id >> 16);
    content[rs + 5] = @truncate(type_id >> 24);
    const body_total = agg_op_len + flat_map_total;
    content[rs + 6] = @truncate(body_total);
    content[rs + 7] = @truncate(body_total >> 8);

    // Body starts after FOR_EACH_EVENT header
    const body_start = rs + 8;

    // AGG_SUM: op(0x40) + slot(1) + val_col(2, parent_amount as f64)
    content[body_start] = 0x40; // AGG_SUM
    content[body_start + 1] = 1; // slot 1
    content[body_start + 2] = 2; // val_col = 2 (parent_amount)

    // FLAT_MAP
    const fm = body_start + 3;
    content[fm] = 0xE1; // FLAT_MAP
    content[fm + 1] = 1; // offsets_col
    content[fm + 2] = 0xFF; // parent_ts_col (unused)
    content[fm + 3] = @truncate(inner_body_len);
    content[fm + 4] = @truncate(inner_body_len >> 8);

    // Inner: STRUCT_MAP_UPSERT_LAST
    const ib = fm + 5;
    content[ib] = 0x80;
    content[ib + 1] = 0; // slot 0
    content[ib + 2] = 3; // key_col (child column)
    content[ib + 3] = 2; // num_vals
    content[ib + 4] = 4; // val_col=4, field_idx=0
    content[ib + 5] = 0;
    content[ib + 6] = 5; // val_col=5, field_idx=1
    content[ib + 7] = 1;
    content[ib + 8] = 0; // num_array_vals = 0

    return prog;
}

test "FLAT_MAP + event-level AGG_SUM in same FOR_EACH_EVENT block" {
    const TYPE_A: u32 = 1001;
    var prog = buildMixedBlockTestProgram(TYPE_A);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    std.debug.assert(state_size <= 8192);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // 2 parents with amounts, each with children
    var type_col = [2]u32{ TYPE_A, TYPE_A };
    var offsets_col = [3]u32{ 0, 2, 3 }; // parent 0: 2 children, parent 1: 1 child
    // Parent amounts (f64) — slot 1 AGG_SUM sums these
    var parent_amounts: [2]f64 = .{ 100.0, 250.0 };
    // Child columns
    var child_key_col = [3]u32{ 10, 20, 30 };
    var child_val0_col = [3]u32{ 1, 2, 3 };
    var child_val1_col = [3]u32{ 11, 22, 33 };

    const col_ptrs = [6][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&offsets_col)),
        @as([*]const u8, @ptrCast(&parent_amounts)),
        @as([*]const u8, @ptrCast(&child_key_col)),
        @as([*]const u8, @ptrCast(&child_val0_col)),
        @as([*]const u8, @ptrCast(&child_val1_col)),
    };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        6,
        2,
    );
    try std.testing.expectEqual(@as(u32, 0), exec_result);

    // Check struct map: 3 entries from FLAT_MAP
    const meta0_base = STATE_HEADER_SIZE;
    const sm_size = std.mem.readInt(u32, state_buf[meta0_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 3), sm_size);

    // Check aggregate: sum of parent amounts = 100.0 + 250.0 = 350.0
    const meta1_base = STATE_HEADER_SIZE + SLOT_META_SIZE;
    const agg_offset = std.mem.readInt(u32, state_buf[meta1_base..][0..4], .little);
    const agg_val: f64 = @bitCast(std.mem.readInt(u64, state_buf[agg_offset..][0..8], .little));
    try std.testing.expectApproxEqAbs(@as(f64, 350.0), agg_val, 0.001);
}

// =============================================================================
// Tests — Nested FLAT_MAP (depth-2)
// =============================================================================

/// Build a program with nested FLAT_MAP (depth-2):
///   FOR_EACH_EVENT { FLAT_MAP(outer) { FLAT_MAP(inner) { STRUCT_MAP_UPSERT_LAST } } }
///
///   Slot 0: STRUCT_MAP (2 fields: UINT32 + STRING, cap=16)
///   Columns: 0=type, 1=outer_offsets, 2=inner_offsets, 3=leaf_key, 4=leaf_val0, 5=leaf_val1
///
///   Event → outer children (groups) → inner children (items) → struct map
fn buildNestedFlatMapTestProgram(type_id: u32) [128]u8 {
    var prog = [_]u8{0} ** 128;
    const content = prog[PROGRAM_HASH_PREFIX..];
    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    content[4] = 1;
    content[5] = 0;
    content[6] = 1; // num_slots
    content[7] = 6; // num_inputs
    content[8] = 0;
    content[9] = 0;

    const init_len: u16 = 8; // SLOT_STRUCT_MAP

    // Inner body: STRUCT_MAP_UPSERT_LAST = 9 bytes (8 + num_array_vals)
    const inner_body_len: u16 = 9;
    // Inner FLAT_MAP: op(1)+offsets_col(1)+parent_ts_col(1)+inner_body_len(2) = 5 + inner_body_len
    const inner_flat_map_total: u16 = 5 + inner_body_len;
    // Outer FLAT_MAP: op(1)+offsets_col(1)+parent_ts_col(1)+inner_body_len(2) = 5 + inner_flat_map_total
    const outer_flat_map_total: u16 = 5 + inner_flat_map_total;
    // FOR_EACH_EVENT: header(8) + outer_flat_map_total
    const reduce_len: u16 = 8 + outer_flat_map_total;

    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    // Init section
    var off: usize = 14;
    content[off] = 0x18; // SLOT_STRUCT_MAP
    content[off + 1] = 0;
    content[off + 2] = 0x05;
    content[off + 3] = 4; // cap_lo → capacity = nextPowerOf2(8) = 8
    content[off + 4] = 0;
    content[off + 5] = 2; // num_fields
    content[off + 6] = 0; // UINT32
    content[off + 7] = 4; // STRING
    off += 8;

    // Reduce section
    const rs: usize = 14 + init_len;
    // FOR_EACH_EVENT
    content[rs] = 0xE0;
    content[rs + 1] = 0; // type_col
    content[rs + 2] = @truncate(type_id);
    content[rs + 3] = @truncate(type_id >> 8);
    content[rs + 4] = @truncate(type_id >> 16);
    content[rs + 5] = @truncate(type_id >> 24);
    content[rs + 6] = @truncate(outer_flat_map_total);
    content[rs + 7] = @truncate(outer_flat_map_total >> 8);

    // Outer FLAT_MAP
    const ofm = rs + 8;
    content[ofm] = 0xE1;
    content[ofm + 1] = 1; // outer offsets_col
    content[ofm + 2] = 0xFF; // parent_ts_col (unused)
    content[ofm + 3] = @truncate(inner_flat_map_total);
    content[ofm + 4] = @truncate(inner_flat_map_total >> 8);

    // Inner FLAT_MAP (body of outer)
    const ifm = ofm + 5;
    content[ifm] = 0xE1;
    content[ifm + 1] = 2; // inner offsets_col
    content[ifm + 2] = 0xFF; // parent_ts_col (unused)
    content[ifm + 3] = @truncate(inner_body_len);
    content[ifm + 4] = @truncate(inner_body_len >> 8);

    // Inner body: STRUCT_MAP_UPSERT_LAST
    const ib = ifm + 5;
    content[ib] = 0x80;
    content[ib + 1] = 0; // slot
    content[ib + 2] = 3; // key_col (leaf key)
    content[ib + 3] = 2; // num_vals
    content[ib + 4] = 4; // val_col=4
    content[ib + 5] = 0; // field_idx=0
    content[ib + 6] = 5; // val_col=5
    content[ib + 7] = 1; // field_idx=1
    content[ib + 8] = 0; // num_array_vals = 0

    return prog;
}

test "Nested FLAT_MAP - depth-2 (groups → items → struct map)" {
    const TYPE_A: u32 = 1001;
    var prog = buildNestedFlatMapTestProgram(TYPE_A);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    std.debug.assert(state_size <= 8192);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // 1 parent event with 2 groups (outer children):
    //   Group 0: 2 items (inner children)
    //   Group 1: 1 item
    // So: 1 event → 2 groups → 3 items total
    var type_col = [1]u32{TYPE_A};

    // Outer offsets: event 0 → groups [0, 2)
    var outer_offsets = [2]u32{ 0, 2 };

    // Inner offsets: group 0 → items [0, 2), group 1 → items [2, 3)
    var inner_offsets = [3]u32{ 0, 2, 3 };

    // Leaf columns (3 items)
    var leaf_key_col = [3]u32{ 100, 200, 300 };
    var leaf_val0_col = [3]u32{ 10, 20, 30 };
    var leaf_val1_col = [3]u32{ 1001, 1002, 1003 };

    const col_ptrs = [6][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&outer_offsets)),
        @as([*]const u8, @ptrCast(&inner_offsets)),
        @as([*]const u8, @ptrCast(&leaf_key_col)),
        @as([*]const u8, @ptrCast(&leaf_val0_col)),
        @as([*]const u8, @ptrCast(&leaf_val1_col)),
    };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        6,
        1,
    );
    try std.testing.expectEqual(@as(u32, 0), exec_result);

    // Should have 3 entries in struct map
    const meta_base = STATE_HEADER_SIZE;
    const sm_size = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 3), sm_size);

    // Verify all 3 leaf keys present with correct values
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const capacity = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    const num_fields = state_buf[meta_base + 13];
    const row_size: u32 = std.mem.readInt(u16, state_buf[meta_base + 16 ..][0..2], .little);

    // Key 100 from group 0, item 0
    const row_100 = vm_struct_map_get_row_ptr(@ptrCast(&state_buf), slot_offset, capacity, num_fields, row_size, 100);
    try std.testing.expect(row_100 != 0xFFFFFFFF);
    try std.testing.expectEqual(@as(u32, 10), std.mem.readInt(u32, state_buf[row_100 + 1 ..][0..4], .little));

    // Key 200 from group 0, item 1
    const row_200 = vm_struct_map_get_row_ptr(@ptrCast(&state_buf), slot_offset, capacity, num_fields, row_size, 200);
    try std.testing.expect(row_200 != 0xFFFFFFFF);
    try std.testing.expectEqual(@as(u32, 20), std.mem.readInt(u32, state_buf[row_200 + 1 ..][0..4], .little));

    // Key 300 from group 1, item 0
    const row_300 = vm_struct_map_get_row_ptr(@ptrCast(&state_buf), slot_offset, capacity, num_fields, row_size, 300);
    try std.testing.expect(row_300 != 0xFFFFFFFF);
    try std.testing.expectEqual(@as(u32, 30), std.mem.readInt(u32, state_buf[row_300 + 1 ..][0..4], .little));
}

// =============================================================================
// Tests — FLAT_MAP with LATEST strategy (parent timestamp inheritance)
// =============================================================================

/// Build a program with MAP_UPSERT_LATEST inside FLAT_MAP, using parent_ts_col:
///   Slot 0: HASHMAP (cap=16, with timestamps)
///   FOR_EACH_EVENT { FLAT_MAP(offsets_col=1, parent_ts_col=2) { MAP_UPSERT_LATEST(slot=0, key_col=3, val_col=4, ts_col=5) } }
///
/// The ts_col=5 is the "fallback" ts column, but parent_ts_col=2 overrides it inside FLAT_MAP.
/// Columns: 0=type, 1=offsets, 2=parent_ts(f64), 3=child_key, 4=child_val, 5=child_ts(f64, ignored)
fn buildFlatMapLatestTestProgram(type_id: u32) [112]u8 {
    var prog = [_]u8{0} ** 112;
    const content = prog[PROGRAM_HASH_PREFIX..];
    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    content[4] = 1;
    content[5] = 0;
    content[6] = 1; // num_slots
    content[7] = 6; // num_inputs
    content[8] = 0;
    content[9] = 0;

    // Init: SLOT_DEF for HASHMAP with timestamps = 5 bytes
    const init_len: u16 = 5;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);

    // Body: MAP_UPSERT_LATEST = 5 bytes (op + slot + key_col + val_col + ts_col)
    const inner_body_len: u16 = 5;
    const flat_map_total: u16 = 5 + inner_body_len; // FLAT_MAP header + body
    const reduce_len: u16 = 8 + flat_map_total; // FOR_EACH_EVENT + body
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    // Init section
    var off: usize = 14;
    content[off] = @intFromEnum(Opcode.SLOT_DEF);
    content[off + 1] = 0; // slot
    content[off + 2] = @intFromEnum(SlotType.HASHMAP); // type_flags: HASHMAP=0
    content[off + 3] = 8; // cap_lo=8 → capacity = nextPowerOf2(16) = 16
    content[off + 4] = 0; // cap_hi
    off += 5;

    // Reduce section
    const rs = 14 + init_len;
    content[rs] = 0xE0; // FOR_EACH_EVENT
    content[rs + 1] = 0; // type_col
    content[rs + 2] = @truncate(type_id);
    content[rs + 3] = @truncate(type_id >> 8);
    content[rs + 4] = @truncate(type_id >> 16);
    content[rs + 5] = @truncate(type_id >> 24);
    content[rs + 6] = @truncate(flat_map_total);
    content[rs + 7] = @truncate(flat_map_total >> 8);

    // FLAT_MAP with parent_ts_col = 2 (uses parent event's timestamp)
    const fm = rs + 8;
    content[fm] = 0xE1; // FLAT_MAP
    content[fm + 1] = 1; // offsets_col
    content[fm + 2] = 2; // parent_ts_col = col 2 (parent timestamp!)
    content[fm + 3] = @truncate(inner_body_len);
    content[fm + 4] = @truncate(inner_body_len >> 8);

    // Inner body: MAP_UPSERT_LATEST
    const ib = fm + 5;
    content[ib] = 0x20; // MAP_UPSERT_LATEST
    content[ib + 1] = 0; // slot
    content[ib + 2] = 3; // key_col (child)
    content[ib + 3] = 4; // val_col (child)
    content[ib + 4] = 5; // ts_col (child — will be OVERRIDDEN by parent_ts_col)

    return prog;
}

test "FLAT_MAP with LATEST strategy - parent timestamp overrides child ts_col" {
    const TYPE_A: u32 = 1001;
    var prog = buildFlatMapLatestTestProgram(TYPE_A);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    std.debug.assert(state_size <= 8192);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // 2 parent events:
    //   Parent 0 (ts=100.0): 1 child → key=42, val=10, child_ts=999.0 (should be IGNORED)
    //   Parent 1 (ts=200.0): 1 child → key=42, val=20, child_ts=1.0 (should be IGNORED)
    // Because parent_ts_col is set, the MAP_UPSERT_LATEST uses parent ts.
    // Parent 1 has ts=200.0 > parent 0 ts=100.0, so val=20 should win.
    var type_col = [2]u32{ TYPE_A, TYPE_A };
    var offsets_col = [3]u32{ 0, 1, 2 };
    var parent_ts: [2]f64 = .{ 100.0, 200.0 };
    var child_key = [2]u32{ 42, 42 }; // same key
    var child_val = [2]u32{ 10, 20 };
    var child_ts: [2]f64 = .{ 999.0, 1.0 }; // should be IGNORED because parent_ts_col overrides

    const col_ptrs = [6][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&offsets_col)),
        @as([*]const u8, @ptrCast(&parent_ts)),
        @as([*]const u8, @ptrCast(&child_key)),
        @as([*]const u8, @ptrCast(&child_val)),
        @as([*]const u8, @ptrCast(&child_ts)),
    };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        6,
        2,
    );
    try std.testing.expectEqual(@as(u32, 0), exec_result);

    // Key 42 should have val=20 (parent 1 wins because parent ts 200.0 > 100.0)
    // If child_ts were used, val=10 would win (child_ts 999.0 > 1.0)
    const meta_base = STATE_HEADER_SIZE;
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const capacity = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    const data_ptr = state_buf[slot_offset..];
    const keys: [*]const u32 = @ptrCast(@alignCast(data_ptr.ptr));
    const values: [*]const u32 = @ptrCast(@alignCast(data_ptr[capacity * 4 ..].ptr));

    // Find key 42
    var found = false;
    var val: u32 = 0;
    var pos: u32 = 0;
    while (pos < capacity) : (pos += 1) {
        if (keys[pos] == 42) {
            found = true;
            val = values[pos];
            break;
        }
    }
    try std.testing.expect(found);
    try std.testing.expectEqual(@as(u32, 20), val); // parent ts wins, so val=20
}

// =============================================================================
// Tests — Ordered List (scalar)
// =============================================================================

/// Build a program with:
///   Slot 0: ORDERED_LIST (scalar UINT32, capacity hint=8)
///   FOR_EACH_EVENT { LIST_APPEND(slot=0, val_col=1) }
/// Columns: 0=type, 1=value(u32)
fn buildScalarListTestProgram(type_id: u32) [80]u8 {
    var prog = [_]u8{0} ** 80;
    const content = prog[PROGRAM_HASH_PREFIX..];
    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    content[4] = 1;
    content[5] = 0;
    content[6] = 1; // num_slots
    content[7] = 2; // num_inputs
    content[8] = 0;
    content[9] = 0;

    // Init: SLOT_ORDERED_LIST(scalar) = 6 bytes (op + slot + type_flags + cap_lo + cap_hi + elem_type)
    const init_len: u16 = 6;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);

    // Reduce: FOR_EACH_EVENT(8) + LIST_APPEND(3) = 11
    const body_len: u16 = 3;
    const reduce_len: u16 = 8 + body_len;
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    // Init section
    var off: usize = 14;
    content[off] = 0x19; // SLOT_ORDERED_LIST
    content[off + 1] = 0; // slot
    content[off + 2] = 6; // type_flags: ORDERED_LIST(6)
    content[off + 3] = 8; // cap_lo → capacity = nextPowerOf2(8) = 8
    content[off + 4] = 0; // cap_hi
    content[off + 5] = 0; // elem_type: UINT32(0)
    off += 6;

    // Reduce section
    const rs: usize = 14 + init_len;
    content[rs] = 0xE0; // FOR_EACH_EVENT
    content[rs + 1] = 0; // type_col
    content[rs + 2] = @truncate(type_id);
    content[rs + 3] = @truncate(type_id >> 8);
    content[rs + 4] = @truncate(type_id >> 16);
    content[rs + 5] = @truncate(type_id >> 24);
    content[rs + 6] = @truncate(body_len);
    content[rs + 7] = @truncate(body_len >> 8);

    // Body: LIST_APPEND
    const bo = rs + 8;
    content[bo] = 0x84; // LIST_APPEND
    content[bo + 1] = 0; // slot
    content[bo + 2] = 1; // val_col

    return prog;
}

test "ORDERED_LIST - scalar append" {
    const TYPE_A: u32 = 1001;
    var prog = buildScalarListTestProgram(TYPE_A);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [4096]u8 align(8) = [_]u8{0} ** 4096;
    std.debug.assert(state_size <= 4096);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // 3 events with values
    var type_col = [3]u32{ TYPE_A, TYPE_A, TYPE_A };
    var val_col = [3]u32{ 100, 200, 300 };

    const col_ptrs = [2][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&val_col)),
    };

    const result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        2,
        3,
    );
    try std.testing.expectEqual(@as(u32, 0), result);

    // Verify count = 3
    const meta_base = STATE_HEADER_SIZE;
    const count = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 3), count);

    // Verify values [100, 200, 300]
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const values: [*]const u32 = @ptrCast(@alignCast(&state_buf[slot_offset]));
    try std.testing.expectEqual(@as(u32, 100), values[0]);
    try std.testing.expectEqual(@as(u32, 200), values[1]);
    try std.testing.expectEqual(@as(u32, 300), values[2]);
}

test "ORDERED_LIST - scalar growth triggers NEEDS_GROWTH" {
    const TYPE_A: u32 = 1001;
    var prog = buildScalarListTestProgram(TYPE_A);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [4096]u8 align(8) = [_]u8{0} ** 4096;
    std.debug.assert(state_size <= 4096);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // Capacity = nextPowerOf2(8) = 8 (minimum might be higher, let's check)
    const meta_base = STATE_HEADER_SIZE;
    const capacity = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);

    // Fill to capacity
    var type_col: [64]u32 = undefined;
    var val_col: [64]u32 = undefined;
    for (0..capacity) |i| {
        type_col[i] = TYPE_A;
        val_col[i] = @truncate(i + 1);
    }

    const col_ptrs = [2][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&val_col)),
    };

    // Fill to capacity — should succeed
    const result_fill = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        2,
        capacity,
    );
    try std.testing.expectEqual(@as(u32, 0), result_fill);

    // One more should trigger NEEDS_GROWTH
    var type_col2 = [1]u32{TYPE_A};
    var val_col2 = [1]u32{999};
    const col_ptrs2 = [2][*]const u8{
        @as([*]const u8, @ptrCast(&type_col2)),
        @as([*]const u8, @ptrCast(&val_col2)),
    };

    const result_overflow = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs2),
        2,
        1,
    );
    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.NEEDS_GROWTH)), result_overflow);
}

// =============================================================================
// Tests — Ordered List (struct)
// =============================================================================

/// Build a program with:
///   Slot 0: ORDERED_LIST (struct, 2 fields: UINT32 + STRING, capacity hint=8)
///   FOR_EACH_EVENT { LIST_APPEND_STRUCT(slot=0, vals from cols 1,2) }
/// Columns: 0=type, 1=val0(u32), 2=val1(u32)
fn buildStructListTestProgram(type_id: u32) [96]u8 {
    var prog = [_]u8{0} ** 96;
    const content = prog[PROGRAM_HASH_PREFIX..];
    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    content[4] = 1;
    content[5] = 0;
    content[6] = 1; // num_slots
    content[7] = 3; // num_inputs
    content[8] = 0;
    content[9] = 0;

    // Init: SLOT_ORDERED_LIST(struct) = op(1) + slot(1) + type_flags(1) + cap_lo(1) + cap_hi(1)
    //       + elem_type(1=0xFF) + num_fields(1) + field_types(2) = 9 bytes
    const init_len: u16 = 9;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);

    // Reduce: FOR_EACH_EVENT(8) + LIST_APPEND_STRUCT(3 + 2*2 = 7) = 15
    const body_len: u16 = 7;
    const reduce_len: u16 = 8 + body_len;
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    // Init section
    var off: usize = 14;
    content[off] = 0x19; // SLOT_ORDERED_LIST
    content[off + 1] = 0; // slot
    content[off + 2] = @intFromEnum(SlotType.ORDERED_LIST); // type_flags: ORDERED_LIST
    content[off + 3] = 8; // cap_lo
    content[off + 4] = 0; // cap_hi
    content[off + 5] = 0xFF; // elem_type: STRUCT
    content[off + 6] = 2; // num_fields
    content[off + 7] = 0; // UINT32
    content[off + 8] = 4; // STRING
    off += 9;

    // Reduce section
    const rs: usize = 14 + init_len;
    content[rs] = 0xE0; // FOR_EACH_EVENT
    content[rs + 1] = 0; // type_col
    content[rs + 2] = @truncate(type_id);
    content[rs + 3] = @truncate(type_id >> 8);
    content[rs + 4] = @truncate(type_id >> 16);
    content[rs + 5] = @truncate(type_id >> 24);
    content[rs + 6] = @truncate(body_len);
    content[rs + 7] = @truncate(body_len >> 8);

    // Body: LIST_APPEND_STRUCT
    const bo = rs + 8;
    content[bo] = 0x85; // LIST_APPEND_STRUCT
    content[bo + 1] = 0; // slot
    content[bo + 2] = 2; // num_vals
    content[bo + 3] = 1; // val_col=1
    content[bo + 4] = 0; // field_idx=0
    content[bo + 5] = 2; // val_col=2
    content[bo + 6] = 1; // field_idx=1

    return prog;
}

test "ORDERED_LIST - struct append" {
    const TYPE_A: u32 = 1001;
    var prog = buildStructListTestProgram(TYPE_A);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [4096]u8 align(8) = [_]u8{0} ** 4096;
    std.debug.assert(state_size <= 4096);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // 2 events
    var type_col = [2]u32{ TYPE_A, TYPE_A };
    var val0_col = [2]u32{ 42, 99 };
    var val1_col = [2]u32{ 1001, 2002 };

    const col_ptrs = [3][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&val0_col)),
        @as([*]const u8, @ptrCast(&val1_col)),
    };

    const result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        3,
        2,
    );
    try std.testing.expectEqual(@as(u32, 0), result);

    // Verify count = 2
    const meta_base = STATE_HEADER_SIZE;
    const count = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 2), count);

    // Read slot metadata for row access
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const num_fields = state_buf[meta_base + 13];
    const row_size: u32 = std.mem.readInt(u16, state_buf[meta_base + 16 ..][0..2], .little);
    const descriptor_size = align8(@as(u32, num_fields));
    const rows_base = slot_offset + descriptor_size;

    // Row 0: val0=42, val1=1001
    const row0 = state_buf[rows_base..];
    // bitset is 1 byte (2 fields < 8), field 0 at offset 1 (bitset=1B), field 1 at offset 5
    const bitset0 = row0[0];
    try std.testing.expectEqual(@as(u8, 0x03), bitset0); // both bits set
    const field0_0 = std.mem.readInt(u32, row0[1..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 42), field0_0);
    const field0_1 = std.mem.readInt(u32, row0[5..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 1001), field0_1);

    // Row 1: val0=99, val1=2002
    const row1 = state_buf[rows_base + row_size ..];
    const field1_0 = std.mem.readInt(u32, row1[1..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 99), field1_0);
    const field1_1 = std.mem.readInt(u32, row1[5..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 2002), field1_1);
}

test "ORDERED_LIST - undo restores count" {
    const TYPE_A: u32 = 1001;
    var prog = buildScalarListTestProgram(TYPE_A);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [4096]u8 align(8) = [_]u8{0} ** 4096;
    std.debug.assert(state_size <= 4096);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // Insert 2 items
    var type_col = [2]u32{ TYPE_A, TYPE_A };
    var val_col = [2]u32{ 100, 200 };
    const col_ptrs = [2][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&val_col)),
    };

    const result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        2,
        2,
    );
    try std.testing.expectEqual(@as(u32, 0), result);

    // Verify count = 2
    const meta_base = STATE_HEADER_SIZE;
    try std.testing.expectEqual(@as(u32, 2), std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little));

    // Enable undo and insert 1 more
    const state_size2 = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    vm_undo_enable(@ptrCast(&state_buf), state_size2);
    const checkpoint = vm_undo_checkpoint(@ptrCast(&state_buf));
    var type_col2 = [1]u32{TYPE_A};
    var val_col2 = [1]u32{300};
    const col_ptrs2 = [2][*]const u8{
        @as([*]const u8, @ptrCast(&type_col2)),
        @as([*]const u8, @ptrCast(&val_col2)),
    };
    const result2 = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs2),
        2,
        1,
    );
    try std.testing.expectEqual(@as(u32, 0), result2);

    // count should be 3
    try std.testing.expectEqual(@as(u32, 3), std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little));

    // Rollback
    vm_undo_rollback(@ptrCast(&state_buf), checkpoint);

    // count should be back to 2
    try std.testing.expectEqual(@as(u32, 2), std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little));
}

test "ORDERED_LIST - inside FLAT_MAP" {
    const TYPE_A: u32 = 1001;
    // Build a program: FOR_EACH_EVENT { FLAT_MAP { LIST_APPEND(slot=0, val_col=2) } }
    // Slot 0: ORDERED_LIST (scalar UINT32, cap=8)
    // Columns: 0=type, 1=offsets, 2=child_val(u32)
    var prog = [_]u8{0} ** 96;
    const content = prog[PROGRAM_HASH_PREFIX..];
    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    content[4] = 1;
    content[5] = 0;
    content[6] = 1; // num_slots
    content[7] = 3; // num_inputs
    content[8] = 0;
    content[9] = 0;

    const init_len: u16 = 6; // SLOT_ORDERED_LIST scalar
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);

    const inner_body_len: u16 = 3; // LIST_APPEND
    const flat_map_total: u16 = 5 + inner_body_len;
    const reduce_len: u16 = 8 + flat_map_total;
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    // Init
    var off: usize = 14;
    content[off] = 0x19; // SLOT_ORDERED_LIST
    content[off + 1] = 0; // slot
    content[off + 2] = @intFromEnum(SlotType.ORDERED_LIST); // type_flags: ORDERED_LIST
    content[off + 3] = 8; // cap_lo
    content[off + 4] = 0;
    content[off + 5] = 0; // UINT32
    off += 6;

    // Reduce
    const rs = 14 + init_len;
    content[rs] = 0xE0;
    content[rs + 1] = 0; // type_col
    content[rs + 2] = @truncate(TYPE_A);
    content[rs + 3] = @truncate(TYPE_A >> 8);
    content[rs + 4] = @truncate(TYPE_A >> 16);
    content[rs + 5] = @truncate(TYPE_A >> 24);
    content[rs + 6] = @truncate(flat_map_total);
    content[rs + 7] = @truncate(flat_map_total >> 8);

    const fm = rs + 8;
    content[fm] = 0xE1; // FLAT_MAP
    content[fm + 1] = 1; // offsets_col
    content[fm + 2] = 0xFF; // parent_ts_col
    content[fm + 3] = @truncate(inner_body_len);
    content[fm + 4] = @truncate(inner_body_len >> 8);

    const ib = fm + 5;
    content[ib] = 0x84; // LIST_APPEND
    content[ib + 1] = 0; // slot
    content[ib + 2] = 2; // val_col

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [4096]u8 align(8) = [_]u8{0} ** 4096;
    std.debug.assert(state_size <= 4096);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // 2 parents: parent 0 has 2 children, parent 1 has 1 child
    var type_col = [2]u32{ TYPE_A, TYPE_A };
    var offsets_col = [3]u32{ 0, 2, 3 };
    var child_val = [3]u32{ 10, 20, 30 };

    const col_ptrs = [3][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&offsets_col)),
        @as([*]const u8, @ptrCast(&child_val)),
    };

    const result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        3,
        2,
    );
    try std.testing.expectEqual(@as(u32, 0), result);

    // Verify count = 3 and values = [10, 20, 30]
    const meta_base = STATE_HEADER_SIZE;
    const cnt = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 3), cnt);

    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const values: [*]const u32 = @ptrCast(@alignCast(&state_buf[slot_offset]));
    try std.testing.expectEqual(@as(u32, 10), values[0]);
    try std.testing.expectEqual(@as(u32, 20), values[1]);
    try std.testing.expectEqual(@as(u32, 30), values[2]);
}

// =============================================================================
// Tests — Array Fields in Struct Map Rows
// =============================================================================

/// Build a program with a STRUCT_MAP slot that has 1 scalar + 1 array field:
///   Field 0: UINT32 (scalar)
///   Field 1: ARRAY_U32 (array — stored as offset+length in row, data in arena)
///
/// Columns: 0=type, 1=key(u32), 2=scalar_val(u32), 3=array_offsets(u32), 4=array_values(u32)
fn buildArrayFieldTestProgram(type_id: u32) [128]u8 {
    var prog = [_]u8{0} ** 128;
    const content = prog[PROGRAM_HASH_PREFIX..];
    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    content[4] = 1;
    content[5] = 0;
    content[6] = 1; // num_slots
    content[7] = 5; // num_inputs
    content[8] = 0;
    content[9] = 0;

    // Init: SLOT_STRUCT_MAP = 8 bytes (op + slot + type_flags + cap_lo + cap_hi + num_fields + 2 field types)
    const init_len: u16 = 8;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);

    // Reduce: FOR_EACH_EVENT(8) + body: STRUCT_MAP_UPSERT_LAST
    //   Scalar: op(1)+slot(1)+key_col(1)+num_vals(1)+1*(val_col+field_idx) = 6
    //   Array: num_array_vals(1)+1*(offsets_col+values_col+field_idx) = 4
    //   Total body = 6 + 4 = 10
    const body_len: u16 = 10;
    const reduce_len: u16 = 8 + body_len;
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    // Init section
    var off: usize = 14;
    content[off] = 0x18; // SLOT_STRUCT_MAP
    content[off + 1] = 0; // slot index
    content[off + 2] = @intFromEnum(SlotType.STRUCT_MAP); // type_flags: STRUCT_MAP
    content[off + 3] = 4; // cap_lo
    content[off + 4] = 0; // cap_hi
    content[off + 5] = 2; // num_fields
    content[off + 6] = 0; // field 0: UINT32
    content[off + 7] = 5; // field 1: ARRAY_U32
    off += 8;

    // Reduce section
    const rs = 14 + init_len;
    content[rs] = 0xE0; // FOR_EACH_EVENT
    content[rs + 1] = 0; // type_col
    content[rs + 2] = @truncate(type_id);
    content[rs + 3] = @truncate(type_id >> 8);
    content[rs + 4] = @truncate(type_id >> 16);
    content[rs + 5] = @truncate(type_id >> 24);
    content[rs + 6] = @truncate(body_len);
    content[rs + 7] = @truncate(body_len >> 8);

    // Body: STRUCT_MAP_UPSERT_LAST with 1 scalar + 1 array field
    const bs = rs + 8;
    content[bs] = 0x80; // STRUCT_MAP_UPSERT_LAST
    content[bs + 1] = 0; // slot
    content[bs + 2] = 1; // key_col
    content[bs + 3] = 1; // num_scalar_vals
    content[bs + 4] = 2; // val_col=2, field_idx=0 (UINT32)
    content[bs + 5] = 0;
    // Array section
    content[bs + 6] = 1; // num_array_vals
    content[bs + 7] = 3; // offsets_col
    content[bs + 8] = 4; // values_col
    content[bs + 9] = 1; // field_idx=1 (ARRAY_U32)

    return prog;
}

test "struct map array field - basic write and read back" {
    const TYPE_A: u32 = 1001;
    var prog = buildArrayFieldTestProgram(TYPE_A);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [16384]u8 align(8) = [_]u8{0} ** 16384;
    std.debug.assert(state_size <= 16384);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // Insert 2 rows:
    //   key=100, scalar=42, array=[10, 20, 30]
    //   key=200, scalar=99, array=[40, 50]
    var type_col = [2]u32{ TYPE_A, TYPE_A };
    var key_col = [2]u32{ 100, 200 };
    var scalar_col = [2]u32{ 42, 99 };
    // CSR offsets: row0 has 3 elements [0..3), row1 has 2 elements [3..5)
    var offsets_col = [3]u32{ 0, 3, 5 };
    var values_col = [5]u32{ 10, 20, 30, 40, 50 };

    var col_ptrs = [5][*]const u8{
        @ptrCast(&type_col),
        @ptrCast(&key_col),
        @ptrCast(&scalar_col),
        @ptrCast(&offsets_col),
        @ptrCast(&values_col),
    };

    const result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        5,
        2,
    );
    try std.testing.expectEqual(@as(u32, 0), result);

    // Verify: read struct map metadata
    const meta_base = STATE_HEADER_SIZE;
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const capacity = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    const current_size = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 2), current_size);

    const num_fields: u32 = state_buf[meta_base + 13];
    try std.testing.expectEqual(@as(u32, 2), num_fields);
    const row_size: u32 = std.mem.readInt(u16, state_buf[meta_base + 16 ..][0..2], .little);

    // Arena should be initialized
    const arena_hdr_off = std.mem.readInt(u32, state_buf[meta_base + 20 ..][0..4], .little);
    try std.testing.expect(arena_hdr_off != 0);
    const arena_used = std.mem.readInt(u32, state_buf[arena_hdr_off + 4 ..][0..4], .little);
    // 3 + 2 = 5 elements × 4 bytes = 20 bytes
    try std.testing.expectEqual(@as(u32, 20), arena_used);

    // Find key=100 row and verify
    const field_types_ptr: [*]const u8 = @ptrCast(&state_buf[slot_offset]);
    const desc_size = align8(num_fields);
    const keys_offset = slot_offset + desc_size;
    const keys: [*]const u32 = @ptrCast(@alignCast(&state_buf[keys_offset]));
    const rows_base = keys_offset + capacity * 4;
    const arena_data_base = arena_hdr_off + ARENA_HEADER_SIZE;

    // Find key=100
    var pos100: ?u32 = null;
    var pos200: ?u32 = null;
    for (0..capacity) |ki| {
        if (keys[ki] == 100) pos100 = @truncate(ki);
        if (keys[ki] == 200) pos200 = @truncate(ki);
    }
    try std.testing.expect(pos100 != null);
    try std.testing.expect(pos200 != null);

    // Verify key=100: scalar=42, array=[10, 20, 30]
    {
        const row_ptr: [*]const u8 = @ptrCast(&state_buf[rows_base + pos100.? * row_size]);
        // Bitset: both fields set
        try std.testing.expect((row_ptr[0] & 0x03) == 0x03);
        // Scalar field at offset = bitset(1)
        const scalar_off = structFieldOffset(@truncate(num_fields), field_types_ptr, 0);
        const scalar_val = std.mem.readInt(u32, row_ptr[scalar_off..][0..4], .little);
        try std.testing.expectEqual(@as(u32, 42), scalar_val);
        // Array field at offset after scalar (bitset(1) + u32(4) = 5)
        const arr_off = structFieldOffset(@truncate(num_fields), field_types_ptr, 1);
        const arr_arena_offset = std.mem.readInt(u32, row_ptr[arr_off..][0..4], .little);
        const arr_len = std.mem.readInt(u32, row_ptr[arr_off + 4 ..][0..4], .little);
        try std.testing.expectEqual(@as(u32, 3), arr_len);
        // Read array elements from arena
        const arr_data: [*]const u32 = @ptrCast(@alignCast(&state_buf[arena_data_base + arr_arena_offset]));
        try std.testing.expectEqual(@as(u32, 10), arr_data[0]);
        try std.testing.expectEqual(@as(u32, 20), arr_data[1]);
        try std.testing.expectEqual(@as(u32, 30), arr_data[2]);
    }

    // Verify key=200: scalar=99, array=[40, 50]
    {
        const row_ptr: [*]const u8 = @ptrCast(&state_buf[rows_base + pos200.? * row_size]);
        const scalar_off = structFieldOffset(@truncate(num_fields), field_types_ptr, 0);
        const scalar_val = std.mem.readInt(u32, row_ptr[scalar_off..][0..4], .little);
        try std.testing.expectEqual(@as(u32, 99), scalar_val);
        const arr_off = structFieldOffset(@truncate(num_fields), field_types_ptr, 1);
        const arr_arena_offset = std.mem.readInt(u32, row_ptr[arr_off..][0..4], .little);
        const arr_len = std.mem.readInt(u32, row_ptr[arr_off + 4 ..][0..4], .little);
        try std.testing.expectEqual(@as(u32, 2), arr_len);
        const arr_data: [*]const u32 = @ptrCast(@alignCast(&state_buf[arena_data_base + arr_arena_offset]));
        try std.testing.expectEqual(@as(u32, 40), arr_data[0]);
        try std.testing.expectEqual(@as(u32, 50), arr_data[1]);
    }
}

test "struct map array field - empty array" {
    const TYPE_A: u32 = 1001;
    var prog = buildArrayFieldTestProgram(TYPE_A);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [16384]u8 align(8) = [_]u8{0} ** 16384;
    std.debug.assert(state_size <= 16384);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // key=100, scalar=42, empty array (offsets[0]=0, offsets[1]=0 → length=0)
    var type_col = [1]u32{TYPE_A};
    var key_col = [1]u32{100};
    var scalar_col = [1]u32{42};
    var offsets_col = [2]u32{ 0, 0 }; // empty array
    var values_col = [1]u32{0}; // unused

    var col_ptrs = [5][*]const u8{
        @ptrCast(&type_col),
        @ptrCast(&key_col),
        @ptrCast(&scalar_col),
        @ptrCast(&offsets_col),
        @ptrCast(&values_col),
    };

    const result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        5,
        1,
    );
    try std.testing.expectEqual(@as(u32, 0), result);

    // Verify: array has length 0
    const meta_base = STATE_HEADER_SIZE;
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const capacity = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    const num_fields: u32 = state_buf[meta_base + 13];
    const row_size: u32 = std.mem.readInt(u16, state_buf[meta_base + 16 ..][0..2], .little);

    const field_types_ptr: [*]const u8 = @ptrCast(&state_buf[slot_offset]);
    const desc_size = align8(num_fields);
    const keys: [*]const u32 = @ptrCast(@alignCast(&state_buf[slot_offset + desc_size]));
    const rows_base = slot_offset + desc_size + capacity * 4;

    // Find key=100
    var pos: ?u32 = null;
    for (0..capacity) |ki| {
        if (keys[ki] == 100) pos = @truncate(ki);
    }
    try std.testing.expect(pos != null);

    const row_ptr: [*]const u8 = @ptrCast(&state_buf[rows_base + pos.? * row_size]);
    const arr_off = structFieldOffset(@truncate(num_fields), field_types_ptr, 1);
    const arr_len = std.mem.readInt(u32, row_ptr[arr_off + 4 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 0), arr_len);

    // Arena should have 0 bytes used
    const arena_hdr_off = std.mem.readInt(u32, state_buf[meta_base + 20 ..][0..4], .little);
    const arena_used = std.mem.readInt(u32, state_buf[arena_hdr_off + 4 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 0), arena_used);
}

test "struct map array field - overwrite (last wins, old arena data abandoned)" {
    const TYPE_A: u32 = 1001;
    var prog = buildArrayFieldTestProgram(TYPE_A);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [16384]u8 align(8) = [_]u8{0} ** 16384;
    std.debug.assert(state_size <= 16384);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // First write: key=100, scalar=42, array=[10, 20, 30]
    {
        var type_col = [1]u32{TYPE_A};
        var key_col = [1]u32{100};
        var scalar_col = [1]u32{42};
        var offsets_col = [2]u32{ 0, 3 };
        var values_col = [3]u32{ 10, 20, 30 };

        var col_ptrs = [5][*]const u8{
            @ptrCast(&type_col),
            @ptrCast(&key_col),
            @ptrCast(&scalar_col),
            @ptrCast(&offsets_col),
            @ptrCast(&values_col),
        };

        const result = vm_execute_batch(
            @ptrCast(&state_buf),
            @ptrCast(&prog),
            prog.len,
            @ptrCast(&col_ptrs),
            5,
            1,
        );
        try std.testing.expectEqual(@as(u32, 0), result);
    }

    // Overwrite: key=100, scalar=99, array=[40, 50]
    {
        var type_col = [1]u32{TYPE_A};
        var key_col = [1]u32{100};
        var scalar_col = [1]u32{99};
        var offsets_col = [2]u32{ 0, 2 };
        var values_col = [2]u32{ 40, 50 };

        var col_ptrs = [5][*]const u8{
            @ptrCast(&type_col),
            @ptrCast(&key_col),
            @ptrCast(&scalar_col),
            @ptrCast(&offsets_col),
            @ptrCast(&values_col),
        };

        const result = vm_execute_batch(
            @ptrCast(&state_buf),
            @ptrCast(&prog),
            prog.len,
            @ptrCast(&col_ptrs),
            5,
            1,
        );
        try std.testing.expectEqual(@as(u32, 0), result);
    }

    // Verify: should see new values, size still 1
    const meta_base = STATE_HEADER_SIZE;
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const capacity = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    const current_size = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 1), current_size);

    const num_fields: u32 = state_buf[meta_base + 13];
    const row_size: u32 = std.mem.readInt(u16, state_buf[meta_base + 16 ..][0..2], .little);
    const field_types_ptr: [*]const u8 = @ptrCast(&state_buf[slot_offset]);
    const desc_size = align8(num_fields);
    const keys: [*]const u32 = @ptrCast(@alignCast(&state_buf[slot_offset + desc_size]));
    const rows_base = slot_offset + desc_size + capacity * 4;
    const arena_hdr_off = std.mem.readInt(u32, state_buf[meta_base + 20 ..][0..4], .little);
    const arena_data_base = arena_hdr_off + ARENA_HEADER_SIZE;

    // Arena used should be 12+8=20 (old data abandoned but still in arena)
    const arena_used = std.mem.readInt(u32, state_buf[arena_hdr_off + 4 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 20), arena_used); // 3*4 + 2*4 = 20

    // Find key=100 and verify new values
    var pos: ?u32 = null;
    for (0..capacity) |ki| {
        if (keys[ki] == 100) pos = @truncate(ki);
    }
    try std.testing.expect(pos != null);

    const row_ptr: [*]const u8 = @ptrCast(&state_buf[rows_base + pos.? * row_size]);
    // Scalar = 99
    const scalar_off = structFieldOffset(@truncate(num_fields), field_types_ptr, 0);
    try std.testing.expectEqual(@as(u32, 99), std.mem.readInt(u32, row_ptr[scalar_off..][0..4], .little));
    // Array = [40, 50] (at offset 12, length 2)
    const arr_off = structFieldOffset(@truncate(num_fields), field_types_ptr, 1);
    const arr_arena_offset = std.mem.readInt(u32, row_ptr[arr_off..][0..4], .little);
    const arr_len = std.mem.readInt(u32, row_ptr[arr_off + 4 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 12), arr_arena_offset); // after first write's 12 bytes
    try std.testing.expectEqual(@as(u32, 2), arr_len);
    const arr_data: [*]const u32 = @ptrCast(@alignCast(&state_buf[arena_data_base + arr_arena_offset]));
    try std.testing.expectEqual(@as(u32, 40), arr_data[0]);
    try std.testing.expectEqual(@as(u32, 50), arr_data[1]);
}
