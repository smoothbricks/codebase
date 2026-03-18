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
pub const bitmap_ops = @import("bitmap_ops.zig");
pub const state_init = @import("state_init.zig");

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
        _ = @import("bitmap_ops.zig");
        _ = @import("state_init.zig");
    }
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
const align8 = types.align8;
const nextPowerOf2 = types.nextPowerOf2;
const EMPTY_KEY = types.EMPTY_KEY;
const TOMBSTONE = types.TOMBSTONE;
pub const BITMAP_SERIALIZED_LEN_BYTES = types.BITMAP_SERIALIZED_LEN_BYTES;
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
pub var g_needs_growth_slot: u8 = 0xFF;

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
pub var g_undo_overflow: bool = false;
pub var g_undo_enabled: bool = false;

// Shadow buffer for lazy overflow snapshot — only used when undo log exceeds capacity.
// WASM: static 1MB buffer (wasm_allocator.alloc grows memory, detaching JS ArrayBuffer views).
// Native: dynamically allocated via page_allocator for any state size.
pub const UNDO_SHADOW_CAPACITY: u32 = 1 * 1024 * 1024;
pub var g_undo_shadow_static: if (builtin.cpu.arch == .wasm32) [UNDO_SHADOW_CAPACITY]u8 else [0]u8 = undefined;
pub var g_undo_shadow_dynamic: ?[]u8 = null;
pub var g_undo_shadow_active: bool = false;
var g_undo_overflow_entry: FlatUndoEntry = undefined;
var g_redo_overflow_entry: FlatUndoEntry = undefined;
var g_undo_has_overflow_entry: bool = false;
pub var g_undo_state_size: u32 = 0;
// Stored at vm_undo_enable time — WASM is single-threaded so this is safe
pub var g_undo_state_base: [*]u8 = undefined;

pub const native_shadow_allocator = std.heap.page_allocator;

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

pub fn removeTTLEntriesForKey(state_base: [*]u8, meta: SlotMeta, key: u32) void {
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
// Bitmap types/ops — re-exported from bitmap_ops.zig for internal use
// =============================================================================

const BitmapStorage = bitmap_ops.BitmapStorage;
const getBitmapStorage = bitmap_ops.getBitmapStorage;
const getBitmapSlotByOffset = bitmap_ops.getBitmapSlotByOffset;
const bitmapFrozen = bitmap_ops.bitmapFrozen;
const bitmapLoad = bitmap_ops.bitmapLoad;
const bitmapStore = bitmap_ops.bitmapStore;
const bitmapSelect = bitmap_ops.bitmapSelect;
const BitmapAlgebraOp = bitmap_ops.BitmapAlgebraOp;
const batchBitmapAlgebra = bitmap_ops.batchBitmapAlgebra;
const clearBitmapScratch = bitmap_ops.clearBitmapScratch;
pub const bitmapPayloadCapacity = bitmap_ops.bitmapPayloadCapacity;
pub const batchBitmapAdd = bitmap_ops.batchBitmapAdd;
pub const batchBitmapRemove = bitmap_ops.batchBitmapRemove;

// Re-exports from state_init.zig for sub-modules (nested.zig, vm_test.zig)
pub const vm_calculate_state_size = state_init.vm_calculate_state_size;
pub const vm_init_state = state_init.vm_init_state;
pub const vm_reset_state = state_init.vm_reset_state;
pub const vm_calculate_grown_state_size = state_init.vm_calculate_grown_state_size;
pub const vm_grow_state = state_init.vm_grow_state;
pub const getStructMapSlotDataSize = state_init.getStructMapSlotDataSize;
pub const ARENA_HEADER_SIZE = state_init.ARENA_HEADER_SIZE;
pub const arenaInitialCapacity = state_init.arenaInitialCapacity;
pub const vm_get_needs_growth_slot = state_init.vm_get_needs_growth_slot;
const structFieldOffset = state_init.structFieldOffset;
const arenaElemSize = state_init.arenaElemSize;
const isArrayFieldType = state_init.isArrayFieldType;
const computeStructRowLayout = state_init.computeStructRowLayout;
const structFieldSize = state_init.structFieldSize;
const hasArrayFields = state_init.hasArrayFields;
const getTTLSideBufferSize = state_init.getTTLSideBufferSize;

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
                const source_data: []const u8 = if (bitmap_ops.g_algebra_result_len > 0)
                    @as([*]const u8, @ptrFromInt(bitmap_ops.g_algebra_result_ptr))[0..bitmap_ops.g_algebra_result_len]
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

    // Non-bitmap path: bind typed set and read key at position
    var dummy_size: u32 = 0;
    // Capacity not needed for direct key read — use a safe upper bound.
    // HashSet.bindExternal only uses cap for entries layout (void for sets).
    const tbl = hash_table.HashSet.bindExternal(state_base + slot_offset, 0, &dummy_size);
    return tbl.keys[pos];
}

pub fn findSlotMetaByOffset(state_base: [*]u8, slot_offset: u32) ?SlotMeta {
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

/// Construct a StructMapSlot from raw WASM export parameters.
/// Used by iteration/read exports that receive layout params directly from JS
/// instead of a slot index. Fields unused by the caller (size_ptr, field_types,
/// bitset_bytes) are set to safe defaults since iteration is read-only.
fn buildStructMapSlot(state_base: [*]u8, slot_offset: u32, capacity: u32, num_fields: u32, row_size: u32) struct_map.StructMapSlot {
    const nf: u8 = @truncate(num_fields);
    const descriptor_size = align8(num_fields);
    const keys_offset = slot_offset + descriptor_size;
    return .{
        .state_base = state_base,
        .slot_offset = slot_offset,
        .capacity = capacity,
        .size_ptr = @ptrCast(@alignCast(state_base)), // unused by read-only iteration
        .num_fields = nf,
        .bitset_bytes = 0, // unused by iteration
        .row_size = row_size,
        .descriptor_size = descriptor_size,
        .field_types = state_base + slot_offset, // unused by iteration
        .keys = @ptrCast(@alignCast(state_base + keys_offset)),
        .rows_base = keys_offset + capacity * 4,
    };
}

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
    // Construct typed StructMapSlot from raw WASM params (read-only iteration)
    const smap = buildStructMapSlot(@constCast(state_base_ptr), slot_offset, capacity, num_fields, row_size);
    return smap.getRowPtrByKey(key);
}

/// Struct map iteration — find first occupied slot index.
/// Returns slot index or capacity (end sentinel).
pub export fn vm_struct_map_iter_start(
    state_base_ptr: [*]const u8,
    slot_offset: u32,
    capacity: u32,
    num_fields: u32,
) u32 {
    const smap = buildStructMapSlot(@constCast(state_base_ptr), slot_offset, capacity, num_fields, 0);
    var pos: u32 = 0;
    while (pos < smap.capacity) : (pos += 1) {
        if (smap.keys[pos] != EMPTY_KEY and smap.keys[pos] != TOMBSTONE) return pos;
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
    const smap = buildStructMapSlot(@constCast(state_base_ptr), slot_offset, capacity, num_fields, 0);
    var pos = current + 1;
    while (pos < smap.capacity) : (pos += 1) {
        if (smap.keys[pos] != EMPTY_KEY and smap.keys[pos] != TOMBSTONE) return pos;
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
    const smap = buildStructMapSlot(@constCast(state_base_ptr), slot_offset, 0, num_fields, 0);
    return smap.keys[pos];
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



// Integration tests moved to vm_test.zig
comptime {
    if (@import("builtin").is_test) {
        _ = @import("vm_test.zig");
    }
}
