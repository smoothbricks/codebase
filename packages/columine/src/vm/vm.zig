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
//   HashMap: keys(u32[cap]) + values(u32[cap]) + timestamps(f64[cap])
//   HashSet: keys(u32[cap])
//   Aggregate: value(f64) + count(u64)

const std = @import("std");
const builtin = @import("builtin");

// NOTE: RETE import removed — columine is the pure reducer VM.

// =============================================================================
// Constants
// =============================================================================

pub const STATE_MAGIC: u32 = 0x53544154; // "STAT"
pub const PROGRAM_MAGIC: u32 = 0x314D4C43; // "CLM1"
pub const RETE_MAGIC: u32 = 0x45544552; // "RETE"
pub const STATE_HEADER_SIZE: u32 = 32;
// NOTE: SLOT_META_SIZE moved below with expanded TTL fields (now 48 bytes)
pub const PROGRAM_HEADER_SIZE: u32 = 14;
pub const RETE_HEADER_SIZE: u32 = 16;

// State format version - increment when header layout changes
pub const STATE_FORMAT_VERSION: u8 = 2;

const EMPTY_KEY: u32 = 0xFFFFFFFF;
const TOMBSTONE: u32 = 0xFFFFFFFE;

// =============================================================================
// State Header Layout (32 bytes)
// =============================================================================
// Offset | Size | Field
// -------|------|------------------
//   0    |  4   | magic (0x53544154)
//   4    |  1   | format_version (2)
//   5    |  2   | program_version (reducer schema version)
//   7    |  2   | ruleset_version (RETE rules version)
//   9    |  1   | num_slots
//  10    |  1   | num_vars (RETE variable count)
//  11    |  1   | num_bitvecs (RETE bitvector count)
//  12    |  1   | flags (bit 0: has_rete)
//  13    |  4   | derived_facts_offset (byte offset to derived facts HashMap)
//  17    |  2   | derived_facts_capacity (max entries, power of 2)
//  19    |  1   | num_derived_fact_schemas (number of .fact(N) schemas)
//  20    |  1   | derived_facts_change_flag (set when derived facts modified)
//  21    |  11  | reserved (zero)
// =============================================================================

pub const StateHeaderOffset = struct {
    pub const MAGIC: u32 = 0;
    pub const FORMAT_VERSION: u32 = 4;
    pub const PROGRAM_VERSION: u32 = 5;
    pub const RULESET_VERSION: u32 = 7;
    pub const NUM_SLOTS: u32 = 9;
    pub const NUM_VARS: u32 = 10;
    pub const NUM_BITVECS: u32 = 11;
    pub const FLAGS: u32 = 12;
    pub const DERIVED_FACTS_OFFSET: u32 = 13;
    pub const DERIVED_FACTS_CAPACITY: u32 = 17;
    pub const NUM_DERIVED_FACT_SCHEMAS: u32 = 19;
    pub const DERIVED_FACTS_CHANGE_FLAG: u32 = 20;
};

pub const StateFlags = struct {
    pub const HAS_RETE: u8 = 0x01;
};

// =============================================================================
// Slot Metadata Layout (48 bytes each)
// =============================================================================
// Offset | Size | Field
// -------|------|------------------
//   0    |  4   | offset (byte offset to slot data)
//   4    |  4   | capacity (for hashmap/hashset/array)
//   8    |  4   | size (current element count)
//  12    |  1   | type_flags (SlotTypeFlags - slot_type + ttl + evict bits)
//  13    |  1   | agg_type (AggType enum, for aggregates)
//  14    |  1   | change_flags (set by reducer ops)
//  15    |  1   | timestamp_field_idx (column index for timestamp, if TTL)
//  16    |  4   | ttl_seconds (f32, TTL window duration)
//  20    |  4   | grace_seconds (f32, grace period for late events)
//  24    |  4   | eviction_index_offset (byte offset to EvictionEntry array)
//  28    |  4   | eviction_index_capacity (max entries in eviction index)
//  32    |  4   | eviction_index_size (current entries in eviction index)
//  36    |  4   | evicted_buffer_offset (offset to evicted entries for RETE)
//  40    |  4   | evicted_count (entries evicted this batch, for RETE)
//  44    |  1   | start_of (DurationUnit for truncation, see 10d-ax-expression-language.md)
//  45    |  3   | reserved
// =============================================================================
// Note: startOf truncation + timezone are applied at JS layer before calling VM.
// JS computes truncate(now, startOf, timezone) and passes result as 'now' param.

pub const SLOT_META_SIZE: u32 = 48;

pub const SlotMetaOffset = struct {
    pub const OFFSET: u32 = 0;
    pub const CAPACITY: u32 = 4;
    pub const SIZE: u32 = 8;
    pub const TYPE_FLAGS: u32 = 12;
    pub const AGG_TYPE: u32 = 13;
    pub const CHANGE_FLAGS: u32 = 14;
    pub const TIMESTAMP_FIELD_IDX: u32 = 15;
    pub const TTL_SECONDS: u32 = 16;
    pub const GRACE_SECONDS: u32 = 20;
    pub const EVICTION_INDEX_OFFSET: u32 = 24;
    pub const EVICTION_INDEX_CAPACITY: u32 = 28;
    pub const EVICTION_INDEX_SIZE: u32 = 32;
    pub const EVICTED_BUFFER_OFFSET: u32 = 36;
    pub const EVICTED_COUNT: u32 = 40;
    pub const START_OF: u32 = 44;
    // 45-47: reserved
};

// DurationUnit - bytecode encoding for duration units from ax expression language
// The JS compiler normalizes long forms to short: 'month'/'months' -> 'M', etc.
// Used for startOf truncation in TTL slots.
// Encoding: 0=none, 1='s', 2='m', 3='h', 4='d', 5='w', 6='M', 7='Q', 8='y'
pub const DurationUnit = enum(u8) {
    NONE = 0,
    s = 1, // second, seconds
    m = 2, // minute, minutes
    h = 3, // hour, hours
    d = 4, // day, days
    w = 5, // week, weeks
    M = 6, // month, months (calendar-aware, variable length)
    Q = 7, // quarter, quarters (calendar-aware)
    y = 8, // year, years (calendar-aware)
};

// Change flags - set by reducer ops, cleared after RETE execution
pub const ChangeFlag = struct {
    pub const INSERTED: u8 = 0x01; // New key inserted
    pub const UPDATED: u8 = 0x02; // Existing value updated
    pub const REMOVED: u8 = 0x04; // Key removed
    pub const SIZE_CHANGED: u8 = 0x08; // Size changed (for aggregates)
    pub const EVICTED: u8 = 0x10; // Entries evicted due to TTL
};

// =============================================================================
// Slot Types and TTL Flags
// =============================================================================
//
// TTL is a FLAG on any slot type, not a separate type. This avoids explosion
// of//
// SlotTypeFlags byte layout:
// ┌─────────────────────────────────────┐
// │ 7 │ 6 │ 5 │ 4 │ 3 │ 2 │ 1 │ 0 │
// │rsv│rsv│evict│ttl│   slot_type   │
// └─────────────────────────────────────┘
// bits 0-3: slot_type (0-15, supports 16 slot types)
// bit 4:    has_ttl (if 1, TTL params in bytecode)
// bit 5:    has_evict_trigger (if 1, fire RETE rules on eviction)
// bits 6-7: reserved

pub const SlotType = enum(u4) {
    HASHMAP = 0,
    HASHSET = 1,
    AGGREGATE = 2,
    ARRAY = 3, // For `.within()` without keyBy - stores array of events
    CONDITION_TREE = 4, // Condition router tree (VM handlers: Phase 37)
    // Room for 11 more types (5-15) without needing TTL variants
};

pub const SlotTypeFlags = packed struct(u8) {
    slot_type: SlotType, // bits 0-3
    has_ttl: bool, // bit 4: TTL params follow in bytecode
    has_evict_trigger: bool, // bit 5: fire RETE rules on eviction
    reserved: u2 = 0, // bits 6-7

    pub fn fromByte(b: u8) SlotTypeFlags {
        return @bitCast(b);
    }

    pub fn toByte(self: SlotTypeFlags) u8 {
        return @bitCast(self);
    }
};

pub const AggType = enum(u32) {
    SUM = 1,
    COUNT = 2,
    MIN = 3,
    MAX = 4,
};

// =============================================================================
// Eviction Index Entry (for TTL slots)
// =============================================================================
// Sorted by timestamp ascending - oldest entries at front for O(expired) eviction

pub const EvictionEntry = packed struct {
    timestamp: f64, // Event timestamp (8 bytes)
    key_or_idx: u32, // Key (for Map/Set) or index (for Array) (4 bytes)
    // Total: 12 bytes per entry
};

// =============================================================================
// Opcodes - Each processes entire batch
// =============================================================================

pub const Opcode = enum(u8) {
    HALT = 0x00,

    // Slot definitions (init section - parsed by JS, not executed)
    // New unified format: SLOT_DEF + slot_idx + type_flags + capacity_lo + capacity_hi [+ TTL params]
    // See SlotTypeFlags for type_flags encoding (slot_type + has_ttl + has_evict_trigger)
    // TTL params (10 bytes): ttl_seconds:f32, grace_seconds:f32, ts_field_idx:u8, start_of:u8
    SLOT_DEF = 0x10, // slot:u8, type_flags:u8, cap_lo:u8, cap_hi:u8 [, ttl:f32, grace:f32, ts_field:u8, start_of:u8]
    // Superseded by SLOT_DEF (still supported for existing bytecode)
    SLOT_HASHMAP = 0x11,
    SLOT_HASHSET = 0x12,
    SLOT_AGGREGATE = 0x13,
    SLOT_ARRAY = 0x14, // For .within() without keyBy - stores array of events

    // HashMap batch ops - process entire column
    BATCH_MAP_UPSERT_LATEST = 0x20, // slot:u8, key_col:u8, val_col:u8, ts_col:u8
    BATCH_MAP_UPSERT_FIRST = 0x21, // slot:u8, key_col:u8, val_col:u8
    BATCH_MAP_UPSERT_LAST = 0x22, // slot:u8, key_col:u8, val_col:u8
    BATCH_MAP_REMOVE = 0x23, // slot:u8, key_col:u8
    // TTL-aware versions (track insertion in eviction index)
    BATCH_MAP_UPSERT_LATEST_TTL = 0x24, // slot:u8, key_col:u8, val_col:u8, ts_col:u8
    BATCH_MAP_UPSERT_LAST_TTL = 0x25, // slot:u8, key_col:u8, val_col:u8, ts_col:u8
    // Max/Min pick strategies (keep row with highest/lowest comparison column value)
    BATCH_MAP_UPSERT_MAX = 0x26, // slot:u8, key_col:u8, val_col:u8, cmp_col:u8
    BATCH_MAP_UPSERT_MIN = 0x27, // slot:u8, key_col:u8, val_col:u8, cmp_col:u8

    // HashSet batch ops
    BATCH_SET_INSERT = 0x30, // slot:u8, elem_col:u8
    BATCH_SET_REMOVE = 0x31, // slot:u8, elem_col:u8
    BATCH_SET_INSERT_TTL = 0x32, // slot:u8, elem_col:u8, ts_col:u8

    // Array batch ops (for .within() without keyBy)
    BATCH_ARRAY_PUSH = 0x38, // slot:u8, val_col:u8, ts_col:u8
    BATCH_ARRAY_PUSH_TTL = 0x39, // slot:u8, val_col:u8, ts_col:u8 (tracks in eviction index)

    // Aggregate batch ops (SIMD accelerated)
    BATCH_AGG_SUM = 0x40, // slot:u8, val_col:u8
    BATCH_AGG_COUNT = 0x41, // slot:u8
    BATCH_AGG_MIN = 0x42, // slot:u8, val_col:u8
    BATCH_AGG_MAX = 0x43, // slot:u8, val_col:u8

    _,
};

// =============================================================================
// Error Codes
// =============================================================================

pub const ErrorCode = enum(u32) {
    OK = 0,
    CAPACITY_EXCEEDED = 1,
    INVALID_PROGRAM = 2,
    INVALID_SLOT = 3,
    INVALID_STATE = 4,
};

// =============================================================================
// Hash Function
// =============================================================================

pub inline fn hashKey(key: u32, cap: u32) u32 {
    var h: u64 = key;
    h ^= h >> 16;
    h *%= 0x85ebca6b;
    h ^= h >> 13;
    h *%= 0xc2b2ae35;
    h ^= h >> 16;
    return @intCast(h & (cap - 1));
}

// =============================================================================
// SIMD Types
// =============================================================================

const V4f64 = @Vector(4, f64);

// =============================================================================
// Slot Metadata Access
// =============================================================================

const SlotMeta = struct {
    // Core fields
    offset: u32,
    capacity: u32,
    size_ptr: *u32,
    type_flags: SlotTypeFlags,
    agg_type: AggType,
    change_flags_ptr: *u8,

    // TTL fields (only meaningful if type_flags.has_ttl == true)
    timestamp_field_idx: u8,
    ttl_seconds: f32,
    grace_seconds: f32,
    start_of: DurationUnit,
    eviction_index_offset: u32,
    eviction_index_capacity: u32,
    eviction_index_size_ptr: *u32,
    evicted_buffer_offset: u32,
    evicted_count_ptr: *u32,

    /// Helper to get slot_type from type_flags
    pub fn slotType(self: SlotMeta) SlotType {
        return self.type_flags.slot_type;
    }

    /// Helper to check if TTL is enabled
    pub fn hasTTL(self: SlotMeta) bool {
        return self.type_flags.has_ttl;
    }

    /// Helper to check if eviction triggers RETE rules
    pub fn hasEvictTrigger(self: SlotMeta) bool {
        return self.type_flags.has_evict_trigger;
    }

    /// Calculate cutoff time for eviction (called from JS after applying startOf/timezone)
    /// cutoff = now - ttl_seconds - grace_seconds
    pub fn cutoff(self: SlotMeta, now: f64) f64 {
        return now - @as(f64, self.ttl_seconds) - @as(f64, self.grace_seconds);
    }
};

pub fn getSlotMeta(state_base: [*]u8, slot: u8) SlotMeta {
    const meta_offset = STATE_HEADER_SIZE + @as(u32, slot) * SLOT_META_SIZE;
    const meta_bytes = state_base + meta_offset;
    const meta_ptr: [*]u32 = @ptrCast(@alignCast(meta_bytes));

    // Read byte-sized fields
    const type_flags = SlotTypeFlags.fromByte(meta_bytes[SlotMetaOffset.TYPE_FLAGS]);
    const agg_type: AggType = @enumFromInt(meta_bytes[SlotMetaOffset.AGG_TYPE]);
    const change_flags_ptr: *u8 = @ptrCast(meta_bytes + SlotMetaOffset.CHANGE_FLAGS);
    const timestamp_field_idx = meta_bytes[SlotMetaOffset.TIMESTAMP_FIELD_IDX];
    const start_of: DurationUnit = @enumFromInt(meta_bytes[SlotMetaOffset.START_OF]);

    // Read f32 fields
    const ttl_ptr: *const f32 = @ptrCast(@alignCast(meta_bytes + SlotMetaOffset.TTL_SECONDS));
    const grace_ptr: *const f32 = @ptrCast(@alignCast(meta_bytes + SlotMetaOffset.GRACE_SECONDS));

    return SlotMeta{
        .offset = meta_ptr[0], // offset 0
        .capacity = meta_ptr[1], // offset 4
        .size_ptr = @ptrCast(@alignCast(meta_bytes + SlotMetaOffset.SIZE)), // offset 8
        .type_flags = type_flags,
        .agg_type = agg_type,
        .change_flags_ptr = change_flags_ptr,
        .timestamp_field_idx = timestamp_field_idx,
        .ttl_seconds = ttl_ptr.*,
        .grace_seconds = grace_ptr.*,
        .start_of = start_of,
        .eviction_index_offset = meta_ptr[6], // offset 24
        .eviction_index_capacity = meta_ptr[7], // offset 28
        .eviction_index_size_ptr = @ptrCast(@alignCast(meta_bytes + SlotMetaOffset.EVICTION_INDEX_SIZE)),
        .evicted_buffer_offset = meta_ptr[9], // offset 36
        .evicted_count_ptr = @ptrCast(@alignCast(meta_bytes + SlotMetaOffset.EVICTED_COUNT)),
    };
}

/// Set change flag on slot metadata
inline fn setChangeFlag(meta: SlotMeta, flag: u8) void {
    meta.change_flags_ptr.* |= flag;
}

/// Clear all change flags for all slots
pub fn clearAllChangeFlags(state_base: [*]u8, num_slots: u8) void {
    var i: u8 = 0;
    while (i < num_slots) : (i += 1) {
        const meta_offset = STATE_HEADER_SIZE + @as(u32, i) * SLOT_META_SIZE;
        const change_flags_ptr: *u8 = @ptrCast(state_base + meta_offset + SlotMetaOffset.CHANGE_FLAGS);
        change_flags_ptr.* = 0;
    }
}

/// Check if any slot has relevant changes for RETE
pub fn hasRelevantChanges(state_base: [*]u8, num_slots: u8) bool {
    var i: u8 = 0;
    while (i < num_slots) : (i += 1) {
        const meta_offset = STATE_HEADER_SIZE + @as(u32, i) * SLOT_META_SIZE;
        const change_flags: u8 = (state_base + meta_offset + SlotMetaOffset.CHANGE_FLAGS)[0];
        if (change_flags != 0) return true;
    }
    return false;
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
    AGG_UPDATE = 6, // Rollback: restore prev value + count
    FACT_INSERT_NEW = 7, // Rollback: tombstone derived fact key
    FACT_INSERT_UPDATE = 8, // Rollback: restore prev derived fact values
    FACT_RETRACT = 9, // Rollback: restore derived fact key + values
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

const UNDO_CAPACITY: u32 = 16384;
var g_undo_entries: [UNDO_CAPACITY]FlatUndoEntry = undefined;
var g_undo_count: u32 = 0;
var g_undo_overflow: bool = false;
pub var g_undo_enabled: bool = false;

// Shadow buffer for lazy overflow snapshot — only used when undo log exceeds capacity.
// 2MB covers typical state sizes (each HashMap slot ~32KB, so ~60 slots at 1K capacity).
const UNDO_SHADOW_CAPACITY: u32 = 2 * 1024 * 1024;
var g_undo_shadow: [UNDO_SHADOW_CAPACITY]u8 = undefined;
var g_undo_state_size: u32 = 0;
// Stored at vm_undo_enable time — WASM is single-threaded so this is safe
var g_undo_state_base: [*]u8 = undefined;

// Saved change flags for rollback (max 256 slots + 1 derived facts flag)
var g_saved_change_flags: [257]u8 = undefined;
var g_saved_change_flags_count: u32 = 0;

pub fn undoAppend(entry: FlatUndoEntry) void {
    if (g_undo_count < UNDO_CAPACITY) {
        g_undo_entries[g_undo_count] = entry;
        g_undo_count += 1;
    } else if (!g_undo_overflow) {
        // First overflow: snapshot current state into shadow buffer so rollback
        // can restore un-logged mutations that happen after this point
        if (g_undo_state_size <= UNDO_SHADOW_CAPACITY) {
            @memcpy(g_undo_shadow[0..g_undo_state_size], g_undo_state_base[0..g_undo_state_size]);
        }
        g_undo_overflow = true;
    }
    // If already overflowed, silently drop — shadow buffer covers subsequent mutations
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
fn findKeyInMap(keys: [*]u32, capacity: u32, key: u32) u32 {
    var slot = hashKey(key, capacity);
    var probes: u32 = 0;
    while (probes < capacity) : (probes += 1) {
        if (keys[slot] == key) return slot;
        if (keys[slot] == EMPTY_KEY) return capacity; // Not found
        slot = (slot + 1) & (capacity - 1);
    }
    return capacity; // Not found
}

/// Find a TOMBSTONE or EMPTY slot at the hash position for restoring a deleted key.
/// Used by MAP_DELETE/SET_DELETE rollback to re-insert at the correct probe position.
fn findInsertSlot(keys: [*]u32, capacity: u32, key: u32) u32 {
    var slot = hashKey(key, capacity);
    var probes: u32 = 0;
    while (probes < capacity) : (probes += 1) {
        const k = keys[slot];
        if (k == EMPTY_KEY or k == TOMBSTONE) return slot;
        if (k == key) return slot; // Key already exists (shouldn't happen, but safe)
        slot = (slot + 1) & (capacity - 1);
    }
    return capacity; // Full (shouldn't happen with proper load factor)
}

/// Roll back a single undo entry by reversing its mutation on the flat state buffer.
fn rollbackEntry(state_base: [*]u8, entry: FlatUndoEntry) void {
    switch (entry.op) {
        .MAP_INSERT => {
            // Undo insert: tombstone the key, decrement size
            // Use TOMBSTONE (not EMPTY_KEY) to preserve probe chains
            const meta = getSlotMeta(state_base, entry.slot);
            const data_ptr = state_base + meta.offset;
            const keys: [*]u32 = @ptrCast(@alignCast(data_ptr));
            const idx = findKeyInMap(keys, meta.capacity, entry.key);
            if (idx < meta.capacity) {
                keys[idx] = TOMBSTONE;
                meta.size_ptr.* -= 1;
            }
        },
        .MAP_UPDATE => {
            // Undo update: restore previous value and timestamp
            const meta = getSlotMeta(state_base, entry.slot);
            const data_ptr = state_base + meta.offset;
            const keys: [*]u32 = @ptrCast(@alignCast(data_ptr));
            const values: [*]u32 = @ptrCast(@alignCast(data_ptr + meta.capacity * 4));
            const timestamps: [*]f64 = @ptrCast(@alignCast(data_ptr + meta.capacity * 8));
            const idx = findKeyInMap(keys, meta.capacity, entry.key);
            if (idx < meta.capacity) {
                values[idx] = entry.prev_value;
                timestamps[idx] = @bitCast(entry.aux);
            }
        },
        .MAP_DELETE => {
            // Undo delete: restore key + value + timestamp, increment size
            const meta = getSlotMeta(state_base, entry.slot);
            const data_ptr = state_base + meta.offset;
            const keys: [*]u32 = @ptrCast(@alignCast(data_ptr));
            const values: [*]u32 = @ptrCast(@alignCast(data_ptr + meta.capacity * 4));
            const timestamps: [*]f64 = @ptrCast(@alignCast(data_ptr + meta.capacity * 8));
            const idx = findInsertSlot(keys, meta.capacity, entry.key);
            if (idx < meta.capacity) {
                keys[idx] = entry.key;
                values[idx] = entry.prev_value;
                timestamps[idx] = @bitCast(entry.aux);
                meta.size_ptr.* += 1;
            }
        },
        .SET_INSERT => {
            // Undo insert: tombstone the element, decrement size
            const meta = getSlotMeta(state_base, entry.slot);
            const data_ptr = state_base + meta.offset;
            const keys: [*]u32 = @ptrCast(@alignCast(data_ptr));
            const idx = findKeyInMap(keys, meta.capacity, entry.key);
            if (idx < meta.capacity) {
                keys[idx] = TOMBSTONE;
                meta.size_ptr.* -= 1;
            }
        },
        .SET_DELETE => {
            // Undo delete: restore element, increment size
            const meta = getSlotMeta(state_base, entry.slot);
            const data_ptr = state_base + meta.offset;
            const keys: [*]u32 = @ptrCast(@alignCast(data_ptr));
            const idx = findInsertSlot(keys, meta.capacity, entry.key);
            if (idx < meta.capacity) {
                keys[idx] = entry.key;
                meta.size_ptr.* += 1;
            }
        },
        .AGG_UPDATE => {
            // Undo aggregate update: restore prev f64 value and prev u64 count
            // aux stores the previous f64 value bits, prev_value stores previous count (truncated to u32)
            const meta = getSlotMeta(state_base, entry.slot);
            const agg_ptr: *f64 = @ptrCast(@alignCast(state_base + meta.offset));
            const count_ptr: *u64 = @ptrCast(@alignCast(state_base + meta.offset + 8));
            agg_ptr.* = @bitCast(entry.aux);
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
    }
}

// =============================================================================
// TTL Eviction Operations
// =============================================================================

/// Get pointer to eviction index array for a TTL slot
inline fn getEvictionIndex(state_base: [*]u8, meta: SlotMeta) [*]EvictionEntry {
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

/// Evict all entries with timestamp older than cutoff.
/// Returns number of entries evicted.
/// If has_evict_trigger is set, evicted entries are copied to evicted buffer for RETE.
/// Note: JS layer applies startOf truncation to 'now' before calling this function.
/// cutoff = now - ttl_seconds - grace_seconds
pub fn evictExpired(state_base: [*]u8, meta: SlotMeta, now: f64) u32 {
    if (!meta.hasTTL()) return 0;

    const cutoff = meta.cutoff(now);
    const eviction_index = getEvictionIndex(state_base, meta);
    const eviction_size = meta.eviction_index_size_ptr.*;

    var evict_count: u32 = 0;

    // Scan from front (oldest entries first)
    while (evict_count < eviction_size) {
        const entry = eviction_index[evict_count];
        if (entry.timestamp >= cutoff) break; // Done - rest are newer

        // Record for RETE rule firing if has_evict_trigger
        if (meta.hasEvictTrigger()) {
            const evicted_buffer = getEvictedBuffer(state_base, meta);
            const evicted_count = meta.evicted_count_ptr.*;
            evicted_buffer[evicted_count] = entry;
            meta.evicted_count_ptr.* += 1;
        }

        // Remove from primary storage (HashMap/HashSet/Array)
        removeEntryByKey(state_base, meta, entry.key_or_idx);

        evict_count += 1;
    }

    // Shift sorted array to remove processed entries
    if (evict_count > 0) {
        shiftEvictionLeft(eviction_index, evict_count, eviction_size);
        meta.eviction_index_size_ptr.* = eviction_size - evict_count;

        // Update slot size
        meta.size_ptr.* -= evict_count;

        // Set change flag
        setChangeFlag(meta, ChangeFlag.EVICTED);
    }

    return evict_count;
}

/// Remove entry from primary storage by key (for HashMap/HashSet) or index (for Array)
fn removeEntryByKey(state_base: [*]u8, meta: SlotMeta, key: u32) void {
    switch (meta.slotType()) {
        .HASHMAP => {
            const data_ptr = state_base + meta.offset;
            const keys: [*]u32 = @ptrCast(@alignCast(data_ptr));

            var slot = hashKey(key, meta.capacity);
            var probes: u32 = 0;

            while (probes < meta.capacity) : (probes += 1) {
                const k = keys[slot];
                if (k == EMPTY_KEY) return;
                if (k == key) {
                    keys[slot] = TOMBSTONE;
                    return;
                }
                slot = (slot + 1) & (meta.capacity - 1);
            }
        },
        .HASHSET => {
            const data_ptr = state_base + meta.offset;
            const keys: [*]u32 = @ptrCast(@alignCast(data_ptr));

            var slot = hashKey(key, meta.capacity);
            var probes: u32 = 0;

            while (probes < meta.capacity) : (probes += 1) {
                const k = keys[slot];
                if (k == EMPTY_KEY) return;
                if (k == key) {
                    keys[slot] = TOMBSTONE;
                    return;
                }
                slot = (slot + 1) & (meta.capacity - 1);
            }
        },
        .ARRAY => {
            // For arrays, key_or_idx is the array index
            // We mark as tombstone (for sparse arrays) or shift (for dense)
            // For now, use tombstone approach
            const data_ptr = state_base + meta.offset;
            const keys: [*]u32 = @ptrCast(@alignCast(data_ptr));
            if (key < meta.capacity) {
                keys[key] = TOMBSTONE;
            }
        },
        .AGGREGATE => {
            // Aggregates don't support individual entry removal
            // TTL for aggregates means the whole aggregate expires
        },
        .CONDITION_TREE => {
            // Placeholder until VM-native tree handlers (Phase 37)
        },
    }
}

/// Insert entry with TTL tracking (maintains sorted eviction index)
pub fn insertWithTTL(state_base: [*]u8, meta: SlotMeta, key: u32, timestamp: f64) void {
    if (!meta.hasTTL()) return;

    const eviction_index = getEvictionIndex(state_base, meta);
    const eviction_size = meta.eviction_index_size_ptr.*;

    // Check capacity
    if (eviction_size >= meta.eviction_index_capacity) {
        // Eviction index full - would need to grow or reject
        // For now, skip tracking (entry won't be auto-evicted correctly)
        return;
    }

    // Binary search for insert position (sorted by timestamp)
    const pos = binarySearchEvictionPos(eviction_index, eviction_size, timestamp);

    // Shift right and insert
    shiftEvictionRight(eviction_index, pos, eviction_size);
    eviction_index[pos] = .{ .timestamp = timestamp, .key_or_idx = key };
    meta.eviction_index_size_ptr.* = eviction_size + 1;
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
            total_evicted += evictExpired(state_base, meta, now);
        }
    }

    return total_evicted;
}

// =============================================================================
// HashMap Operations
// =============================================================================

fn batchMapUpsertLatest(
    state_base: [*]u8,
    meta: SlotMeta,
    slot_idx: u8,
    key_col: [*]const u32,
    val_col: [*]const u32,
    ts_col: [*]const f64,
    batch_len: u32,
) ErrorCode {
    const data_ptr = state_base + meta.offset;
    const keys: [*]u32 = @ptrCast(@alignCast(data_ptr));
    const values: [*]u32 = @ptrCast(@alignCast(data_ptr + meta.capacity * 4));
    const timestamps: [*]f64 = @ptrCast(@alignCast(data_ptr + meta.capacity * 8));

    var size = meta.size_ptr.*;
    const max_size = (meta.capacity * 7) / 10;
    var had_insert = false;
    var had_update = false;

    var i: u32 = 0;
    while (i < batch_len) : (i += 1) {
        const key = key_col[i];
        const val = val_col[i];
        const ts = ts_col[i];

        var slot = hashKey(key, meta.capacity);
        var probes: u32 = 0;

        while (probes < meta.capacity) : (probes += 1) {
            const k = keys[slot];
            if (k == EMPTY_KEY or k == TOMBSTONE) {
                if (size >= max_size) {
                    meta.size_ptr.* = size;
                    if (had_insert) setChangeFlag(meta, ChangeFlag.INSERTED);
                    if (had_update) setChangeFlag(meta, ChangeFlag.UPDATED);
                    return .CAPACITY_EXCEEDED;
                }
                // Undo: record new insertion so rollback can tombstone it
                if (g_undo_enabled) undoAppend(.{
                    .op = .MAP_INSERT, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0,
                    .key = key, .prev_value = 0, .aux = 0,
                });
                keys[slot] = key;
                values[slot] = val;
                timestamps[slot] = ts;
                size += 1;
                had_insert = true;
                break;
            } else if (k == key) {
                if (ts > timestamps[slot]) {
                    // Undo: save previous value + timestamp before overwrite
                    if (g_undo_enabled) undoAppend(.{
                        .op = .MAP_UPDATE, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0,
                        .key = key, .prev_value = values[slot], .aux = @bitCast(timestamps[slot]),
                    });
                    values[slot] = val;
                    timestamps[slot] = ts;
                    had_update = true;
                }
                break;
            }
            slot = (slot + 1) & (meta.capacity - 1);
        }
    }

    meta.size_ptr.* = size;
    if (had_insert) setChangeFlag(meta, ChangeFlag.INSERTED);
    if (had_update) setChangeFlag(meta, ChangeFlag.UPDATED);
    return .OK;
}

fn batchMapUpsertFirst(
    state_base: [*]u8,
    meta: SlotMeta,
    slot_idx: u8,
    key_col: [*]const u32,
    val_col: [*]const u32,
    batch_len: u32,
) ErrorCode {
    const data_ptr = state_base + meta.offset;
    const keys: [*]u32 = @ptrCast(@alignCast(data_ptr));
    const values: [*]u32 = @ptrCast(@alignCast(data_ptr + meta.capacity * 4));

    var size = meta.size_ptr.*;
    const max_size = (meta.capacity * 7) / 10;
    var had_insert = false;

    var i: u32 = 0;
    while (i < batch_len) : (i += 1) {
        const key = key_col[i];
        const val = val_col[i];

        var slot = hashKey(key, meta.capacity);
        var probes: u32 = 0;

        while (probes < meta.capacity) : (probes += 1) {
            const k = keys[slot];
            if (k == EMPTY_KEY or k == TOMBSTONE) {
                if (size >= max_size) {
                    meta.size_ptr.* = size;
                    if (had_insert) setChangeFlag(meta, ChangeFlag.INSERTED);
                    return .CAPACITY_EXCEEDED;
                }
                // Undo: record new insertion (first wins has no update path)
                if (g_undo_enabled) undoAppend(.{
                    .op = .MAP_INSERT, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0,
                    .key = key, .prev_value = 0, .aux = 0,
                });
                keys[slot] = key;
                values[slot] = val;
                size += 1;
                had_insert = true;
                break;
            } else if (k == key) {
                // First wins - don't update
                break;
            }
            slot = (slot + 1) & (meta.capacity - 1);
        }
    }

    meta.size_ptr.* = size;
    if (had_insert) setChangeFlag(meta, ChangeFlag.INSERTED);
    return .OK;
}

fn batchMapUpsertLast(
    state_base: [*]u8,
    meta: SlotMeta,
    slot_idx: u8,
    key_col: [*]const u32,
    val_col: [*]const u32,
    batch_len: u32,
) ErrorCode {
    const data_ptr = state_base + meta.offset;
    const keys: [*]u32 = @ptrCast(@alignCast(data_ptr));
    const values: [*]u32 = @ptrCast(@alignCast(data_ptr + meta.capacity * 4));

    var size = meta.size_ptr.*;
    const max_size = (meta.capacity * 7) / 10;
    var had_insert = false;
    var had_update = false;

    var i: u32 = 0;
    while (i < batch_len) : (i += 1) {
        const key = key_col[i];
        const val = val_col[i];

        var slot = hashKey(key, meta.capacity);
        var probes: u32 = 0;

        while (probes < meta.capacity) : (probes += 1) {
            const k = keys[slot];
            if (k == EMPTY_KEY or k == TOMBSTONE) {
                if (size >= max_size) {
                    meta.size_ptr.* = size;
                    if (had_insert) setChangeFlag(meta, ChangeFlag.INSERTED);
                    if (had_update) setChangeFlag(meta, ChangeFlag.UPDATED);
                    return .CAPACITY_EXCEEDED;
                }
                // Undo: record new insertion so rollback can tombstone it
                if (g_undo_enabled) undoAppend(.{
                    .op = .MAP_INSERT, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0,
                    .key = key, .prev_value = 0, .aux = 0,
                });
                keys[slot] = key;
                values[slot] = val;
                size += 1;
                had_insert = true;
                break;
            } else if (k == key) {
                // Undo: save previous value before overwrite (no timestamp for Last)
                if (g_undo_enabled) undoAppend(.{
                    .op = .MAP_UPDATE, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0,
                    .key = key, .prev_value = values[slot], .aux = 0,
                });
                // Last wins - always update
                values[slot] = val;
                had_update = true;
                break;
            }
            slot = (slot + 1) & (meta.capacity - 1);
        }
    }

    meta.size_ptr.* = size;
    if (had_insert) setChangeFlag(meta, ChangeFlag.INSERTED);
    if (had_update) setChangeFlag(meta, ChangeFlag.UPDATED);
    return .OK;
}

/// Batch map upsert with MAX strategy: keep row with highest comparison value.
/// Uses timestamps array to store comparison values for tracking.
fn batchMapUpsertMax(
    state_base: [*]u8,
    meta: SlotMeta,
    slot_idx: u8,
    key_col: [*]const u32,
    val_col: [*]const u32,
    cmp_col: [*]const f64,
    batch_len: u32,
) ErrorCode {
    const data_ptr = state_base + meta.offset;
    const keys: [*]u32 = @ptrCast(@alignCast(data_ptr));
    const values: [*]u32 = @ptrCast(@alignCast(data_ptr + meta.capacity * 4));
    // Repurpose timestamps array to store comparison values
    const cmp_vals: [*]f64 = @ptrCast(@alignCast(data_ptr + meta.capacity * 8));

    var size = meta.size_ptr.*;
    const max_size = (meta.capacity * 7) / 10;
    var had_insert = false;
    var had_update = false;

    var i: u32 = 0;
    while (i < batch_len) : (i += 1) {
        const key = key_col[i];
        const val = val_col[i];
        const cmp = cmp_col[i];

        var slot = hashKey(key, meta.capacity);
        var probes: u32 = 0;

        while (probes < meta.capacity) : (probes += 1) {
            const k = keys[slot];
            if (k == EMPTY_KEY or k == TOMBSTONE) {
                if (size >= max_size) {
                    meta.size_ptr.* = size;
                    if (had_insert) setChangeFlag(meta, ChangeFlag.INSERTED);
                    if (had_update) setChangeFlag(meta, ChangeFlag.UPDATED);
                    return .CAPACITY_EXCEEDED;
                }
                // Undo: record new insertion so rollback can tombstone it
                if (g_undo_enabled) undoAppend(.{
                    .op = .MAP_INSERT, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0,
                    .key = key, .prev_value = 0, .aux = 0,
                });
                keys[slot] = key;
                values[slot] = val;
                cmp_vals[slot] = cmp;
                size += 1;
                had_insert = true;
                break;
            } else if (k == key) {
                // MAX: only update if new comparison value is greater
                if (cmp > cmp_vals[slot]) {
                    // Undo: save previous value + cmp value before overwrite
                    if (g_undo_enabled) undoAppend(.{
                        .op = .MAP_UPDATE, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0,
                        .key = key, .prev_value = values[slot], .aux = @bitCast(cmp_vals[slot]),
                    });
                    values[slot] = val;
                    cmp_vals[slot] = cmp;
                    had_update = true;
                }
                break;
            }
            slot = (slot + 1) & (meta.capacity - 1);
        }
    }

    meta.size_ptr.* = size;
    if (had_insert) setChangeFlag(meta, ChangeFlag.INSERTED);
    if (had_update) setChangeFlag(meta, ChangeFlag.UPDATED);
    return .OK;
}

/// Batch map upsert with MIN strategy: keep row with lowest comparison value.
/// Uses timestamps array to store comparison values for tracking.
fn batchMapUpsertMin(
    state_base: [*]u8,
    meta: SlotMeta,
    slot_idx: u8,
    key_col: [*]const u32,
    val_col: [*]const u32,
    cmp_col: [*]const f64,
    batch_len: u32,
) ErrorCode {
    const data_ptr = state_base + meta.offset;
    const keys: [*]u32 = @ptrCast(@alignCast(data_ptr));
    const values: [*]u32 = @ptrCast(@alignCast(data_ptr + meta.capacity * 4));
    // Repurpose timestamps array to store comparison values
    const cmp_vals: [*]f64 = @ptrCast(@alignCast(data_ptr + meta.capacity * 8));

    var size = meta.size_ptr.*;
    const max_size = (meta.capacity * 7) / 10;
    var had_insert = false;
    var had_update = false;

    var i: u32 = 0;
    while (i < batch_len) : (i += 1) {
        const key = key_col[i];
        const val = val_col[i];
        const cmp = cmp_col[i];

        var slot = hashKey(key, meta.capacity);
        var probes: u32 = 0;

        while (probes < meta.capacity) : (probes += 1) {
            const k = keys[slot];
            if (k == EMPTY_KEY or k == TOMBSTONE) {
                if (size >= max_size) {
                    meta.size_ptr.* = size;
                    if (had_insert) setChangeFlag(meta, ChangeFlag.INSERTED);
                    if (had_update) setChangeFlag(meta, ChangeFlag.UPDATED);
                    return .CAPACITY_EXCEEDED;
                }
                // Undo: record new insertion so rollback can tombstone it
                if (g_undo_enabled) undoAppend(.{
                    .op = .MAP_INSERT, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0,
                    .key = key, .prev_value = 0, .aux = 0,
                });
                keys[slot] = key;
                values[slot] = val;
                cmp_vals[slot] = cmp;
                size += 1;
                had_insert = true;
                break;
            } else if (k == key) {
                // MIN: only update if new comparison value is smaller
                if (cmp < cmp_vals[slot]) {
                    // Undo: save previous value + cmp value before overwrite
                    if (g_undo_enabled) undoAppend(.{
                        .op = .MAP_UPDATE, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0,
                        .key = key, .prev_value = values[slot], .aux = @bitCast(cmp_vals[slot]),
                    });
                    values[slot] = val;
                    cmp_vals[slot] = cmp;
                    had_update = true;
                }
                break;
            }
            slot = (slot + 1) & (meta.capacity - 1);
        }
    }

    meta.size_ptr.* = size;
    if (had_insert) setChangeFlag(meta, ChangeFlag.INSERTED);
    if (had_update) setChangeFlag(meta, ChangeFlag.UPDATED);
    return .OK;
}

fn batchMapRemove(
    state_base: [*]u8,
    meta: SlotMeta,
    slot_idx: u8,
    key_col: [*]const u32,
    batch_len: u32,
) void {
    const data_ptr = state_base + meta.offset;
    const keys: [*]u32 = @ptrCast(@alignCast(data_ptr));
    const values: [*]u32 = @ptrCast(@alignCast(data_ptr + meta.capacity * 4));
    const timestamps: [*]f64 = @ptrCast(@alignCast(data_ptr + meta.capacity * 8));

    var size = meta.size_ptr.*;
    var had_remove = false;

    var i: u32 = 0;
    while (i < batch_len) : (i += 1) {
        const key = key_col[i];
        var slot = hashKey(key, meta.capacity);
        var probes: u32 = 0;

        while (probes < meta.capacity) : (probes += 1) {
            const k = keys[slot];
            if (k == EMPTY_KEY) break;
            if (k == key) {
                // Undo: save key + value + timestamp so rollback can restore
                if (g_undo_enabled) undoAppend(.{
                    .op = .MAP_DELETE, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0,
                    .key = key, .prev_value = values[slot], .aux = @bitCast(timestamps[slot]),
                });
                keys[slot] = TOMBSTONE;
                size -= 1;
                had_remove = true;
                break;
            }
            slot = (slot + 1) & (meta.capacity - 1);
        }
    }

    meta.size_ptr.* = size;
    if (had_remove) setChangeFlag(meta, ChangeFlag.REMOVED);
}

// =============================================================================
// HashSet Operations
// =============================================================================

fn batchSetInsert(
    state_base: [*]u8,
    meta: SlotMeta,
    slot_idx: u8,
    elem_col: [*]const u32,
    batch_len: u32,
) ErrorCode {
    const data_ptr = state_base + meta.offset;
    const keys: [*]u32 = @ptrCast(@alignCast(data_ptr));

    var size = meta.size_ptr.*;
    const max_size = (meta.capacity * 7) / 10;
    var had_insert = false;

    var i: u32 = 0;
    while (i < batch_len) : (i += 1) {
        const elem = elem_col[i];
        var slot = hashKey(elem, meta.capacity);
        var probes: u32 = 0;

        while (probes < meta.capacity) : (probes += 1) {
            const k = keys[slot];
            if (k == EMPTY_KEY or k == TOMBSTONE) {
                if (size >= max_size) {
                    meta.size_ptr.* = size;
                    if (had_insert) setChangeFlag(meta, ChangeFlag.INSERTED);
                    return .CAPACITY_EXCEEDED;
                }
                // Undo: record new set insertion so rollback can tombstone it
                if (g_undo_enabled) undoAppend(.{
                    .op = .SET_INSERT, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0,
                    .key = elem, .prev_value = 0, .aux = 0,
                });
                keys[slot] = elem;
                size += 1;
                had_insert = true;
                break;
            } else if (k == elem) {
                break;
            }
            slot = (slot + 1) & (meta.capacity - 1);
        }
    }

    meta.size_ptr.* = size;
    if (had_insert) setChangeFlag(meta, ChangeFlag.INSERTED);
    return .OK;
}

fn batchSetRemove(
    state_base: [*]u8,
    meta: SlotMeta,
    slot_idx: u8,
    elem_col: [*]const u32,
    batch_len: u32,
) void {
    const data_ptr = state_base + meta.offset;
    const keys: [*]u32 = @ptrCast(@alignCast(data_ptr));

    var size = meta.size_ptr.*;
    var had_remove = false;

    var i: u32 = 0;
    while (i < batch_len) : (i += 1) {
        const elem = elem_col[i];
        var slot = hashKey(elem, meta.capacity);
        var probes: u32 = 0;

        while (probes < meta.capacity) : (probes += 1) {
            const k = keys[slot];
            if (k == EMPTY_KEY) break;
            if (k == elem) {
                // Undo: save element so rollback can restore it
                if (g_undo_enabled) undoAppend(.{
                    .op = .SET_DELETE, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0,
                    .key = elem, .prev_value = 0, .aux = 0,
                });
                keys[slot] = TOMBSTONE;
                size -= 1;
                had_remove = true;
                break;
            }
            slot = (slot + 1) & (meta.capacity - 1);
        }
    }

    meta.size_ptr.* = size;
    if (had_remove) setChangeFlag(meta, ChangeFlag.REMOVED);
}

// =============================================================================
// SIMD Aggregate Operations
// =============================================================================

fn batchAggSum(val_col: [*]const f64, batch_len: u32) f64 {
    var sum_vec: V4f64 = @splat(0.0);
    var i: u32 = 0;

    while (i + 4 <= batch_len) : (i += 4) {
        const v: V4f64 = .{ val_col[i], val_col[i + 1], val_col[i + 2], val_col[i + 3] };
        sum_vec += v;
    }

    var result = @reduce(.Add, sum_vec);

    while (i < batch_len) : (i += 1) {
        result += val_col[i];
    }

    return result;
}

fn batchAggMin(val_col: [*]const f64, batch_len: u32, current_min: f64) f64 {
    if (batch_len == 0) return current_min;

    var min_vec: V4f64 = @splat(current_min);
    var i: u32 = 0;

    while (i + 4 <= batch_len) : (i += 4) {
        const v: V4f64 = .{ val_col[i], val_col[i + 1], val_col[i + 2], val_col[i + 3] };
        min_vec = @min(min_vec, v);
    }

    var result = @reduce(.Min, min_vec);

    while (i < batch_len) : (i += 1) {
        result = @min(result, val_col[i]);
    }

    return result;
}

fn batchAggMax(val_col: [*]const f64, batch_len: u32, current_max: f64) f64 {
    if (batch_len == 0) return current_max;

    var max_vec: V4f64 = @splat(current_max);
    var i: u32 = 0;

    while (i + 4 <= batch_len) : (i += 4) {
        const v: V4f64 = .{ val_col[i], val_col[i + 1], val_col[i + 2], val_col[i + 3] };
        max_vec = @max(max_vec, v);
    }

    var result = @reduce(.Max, max_vec);

    while (i < batch_len) : (i += 1) {
        result = @max(result, val_col[i]);
    }

    return result;
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
pub export fn vm_execute_batch(
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

    // Validate program
    const program = program_ptr[0..program_len];
    if (program_len < PROGRAM_HEADER_SIZE) return @intFromEnum(ErrorCode.INVALID_PROGRAM);

    const prog_magic = @as(u32, program[0]) | (@as(u32, program[1]) << 8) | (@as(u32, program[2]) << 16) | (@as(u32, program[3]) << 24);
    if (prog_magic != PROGRAM_MAGIC) return @intFromEnum(ErrorCode.INVALID_PROGRAM);

    // Parse header
    const init_len: u16 = @as(u16, program[10]) | (@as(u16, program[11]) << 8);
    const reduce_len: u16 = @as(u16, program[12]) | (@as(u16, program[13]) << 8);
    const code_len = init_len + reduce_len;
    if (PROGRAM_HEADER_SIZE + code_len > program_len) return @intFromEnum(ErrorCode.INVALID_PROGRAM);

    // Execute only reduce section (init section parsed by JS)
    const code = program[PROGRAM_HEADER_SIZE + init_len .. PROGRAM_HEADER_SIZE + init_len + reduce_len];

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

    var pc: usize = 0;
    while (pc < code.len) {
        const op: Opcode = @enumFromInt(code[pc]);
        pc += 1;

        switch (op) {
            .HALT => break,

            .BATCH_MAP_UPSERT_LATEST => {
                const slot = code[pc];
                const key_col = code[pc + 1];
                const val_col = code[pc + 2];
                const ts_col = code[pc + 3];
                pc += 4;

                const meta = getSlotMeta(state_base, slot);
                const result = batchMapUpsertLatest(
                    state_base,
                    meta,
                    slot,
                    getColU32(col_ptrs_ptr, key_col),
                    getColU32(col_ptrs_ptr, val_col),
                    getColF64(col_ptrs_ptr, ts_col),
                    batch_len,
                );
                if (result != .OK) return @intFromEnum(result);
            },

            .BATCH_MAP_UPSERT_FIRST => {
                const slot = code[pc];
                const key_col = code[pc + 1];
                const val_col = code[pc + 2];
                pc += 3;

                const meta = getSlotMeta(state_base, slot);
                const result = batchMapUpsertFirst(
                    state_base,
                    meta,
                    slot,
                    getColU32(col_ptrs_ptr, key_col),
                    getColU32(col_ptrs_ptr, val_col),
                    batch_len,
                );
                if (result != .OK) return @intFromEnum(result);
            },

            .BATCH_MAP_UPSERT_LAST => {
                const slot = code[pc];
                const key_col = code[pc + 1];
                const val_col = code[pc + 2];
                pc += 3;

                const meta = getSlotMeta(state_base, slot);
                const result = batchMapUpsertLast(
                    state_base,
                    meta,
                    slot,
                    getColU32(col_ptrs_ptr, key_col),
                    getColU32(col_ptrs_ptr, val_col),
                    batch_len,
                );
                if (result != .OK) return @intFromEnum(result);
            },

            .BATCH_MAP_REMOVE => {
                const slot = code[pc];
                const key_col = code[pc + 1];
                pc += 2;

                const meta = getSlotMeta(state_base, slot);
                batchMapRemove(state_base, meta, slot, getColU32(col_ptrs_ptr, key_col), batch_len);
            },

            .BATCH_MAP_UPSERT_MAX => {
                const slot = code[pc];
                const key_col = code[pc + 1];
                const val_col = code[pc + 2];
                const cmp_col = code[pc + 3];
                pc += 4;

                const meta = getSlotMeta(state_base, slot);
                const result = batchMapUpsertMax(
                    state_base,
                    meta,
                    slot,
                    getColU32(col_ptrs_ptr, key_col),
                    getColU32(col_ptrs_ptr, val_col),
                    getColF64(col_ptrs_ptr, cmp_col),
                    batch_len,
                );
                if (result != .OK) return @intFromEnum(result);
            },

            .BATCH_MAP_UPSERT_MIN => {
                const slot = code[pc];
                const key_col = code[pc + 1];
                const val_col = code[pc + 2];
                const cmp_col = code[pc + 3];
                pc += 4;

                const meta = getSlotMeta(state_base, slot);
                const result = batchMapUpsertMin(
                    state_base,
                    meta,
                    slot,
                    getColU32(col_ptrs_ptr, key_col),
                    getColU32(col_ptrs_ptr, val_col),
                    getColF64(col_ptrs_ptr, cmp_col),
                    batch_len,
                );
                if (result != .OK) return @intFromEnum(result);
            },

            .BATCH_SET_INSERT => {
                const slot = code[pc];
                const elem_col = code[pc + 1];
                pc += 2;

                const meta = getSlotMeta(state_base, slot);
                const result = batchSetInsert(state_base, meta, slot, getColU32(col_ptrs_ptr, elem_col), batch_len);
                if (result != .OK) return @intFromEnum(result);
            },

            .BATCH_SET_REMOVE => {
                const slot = code[pc];
                const elem_col = code[pc + 1];
                pc += 2;

                const meta = getSlotMeta(state_base, slot);
                batchSetRemove(state_base, meta, slot, getColU32(col_ptrs_ptr, elem_col), batch_len);
            },

            .BATCH_AGG_SUM => {
                const slot = code[pc];
                const val_col = code[pc + 1];
                pc += 2;

                const meta = getSlotMeta(state_base, slot);
                const agg_ptr: *f64 = @ptrCast(@alignCast(state_base + meta.offset));
                const count_ptr: *u64 = @ptrCast(@alignCast(state_base + meta.offset + 8));
                // Undo: save previous f64 value (aux) and count (prev_value) before mutation
                if (g_undo_enabled) undoAppend(.{
                    .op = .AGG_UPDATE, .slot = slot, ._pad1 = 0, ._pad2 = 0,
                    .key = 0, .prev_value = @truncate(count_ptr.*), .aux = @bitCast(agg_ptr.*),
                });
                const old_val = agg_ptr.*;
                agg_ptr.* += batchAggSum(getColF64(col_ptrs_ptr, val_col), batch_len);
                if (agg_ptr.* != old_val) setChangeFlag(meta, ChangeFlag.SIZE_CHANGED);
            },

            .BATCH_AGG_COUNT => {
                const slot = code[pc];
                pc += 1;

                const meta = getSlotMeta(state_base, slot);
                const agg_ptr: *f64 = @ptrCast(@alignCast(state_base + meta.offset));
                const count_ptr: *u64 = @ptrCast(@alignCast(state_base + meta.offset + 8));
                if (batch_len > 0) {
                    // Undo: save previous f64 value (aux) and count (prev_value) before mutation
                    if (g_undo_enabled) undoAppend(.{
                        .op = .AGG_UPDATE, .slot = slot, ._pad1 = 0, ._pad2 = 0,
                        .key = 0, .prev_value = @truncate(count_ptr.*), .aux = @bitCast(agg_ptr.*),
                    });
                    count_ptr.* += batch_len;
                    setChangeFlag(meta, ChangeFlag.SIZE_CHANGED);
                }
            },

            .BATCH_AGG_MIN => {
                const slot = code[pc];
                const val_col = code[pc + 1];
                pc += 2;

                const meta = getSlotMeta(state_base, slot);
                const agg_ptr: *f64 = @ptrCast(@alignCast(state_base + meta.offset));
                const count_ptr: *u64 = @ptrCast(@alignCast(state_base + meta.offset + 8));
                const old_val = agg_ptr.*;
                const new_val = batchAggMin(getColF64(col_ptrs_ptr, val_col), batch_len, agg_ptr.*);
                if (new_val != old_val) {
                    // Undo: save previous f64 value (aux) and count (prev_value) before mutation
                    if (g_undo_enabled) undoAppend(.{
                        .op = .AGG_UPDATE, .slot = slot, ._pad1 = 0, ._pad2 = 0,
                        .key = 0, .prev_value = @truncate(count_ptr.*), .aux = @bitCast(old_val),
                    });
                    agg_ptr.* = new_val;
                    setChangeFlag(meta, ChangeFlag.SIZE_CHANGED);
                }
            },

            .BATCH_AGG_MAX => {
                const slot = code[pc];
                const val_col = code[pc + 1];
                pc += 2;

                const meta = getSlotMeta(state_base, slot);
                const agg_ptr: *f64 = @ptrCast(@alignCast(state_base + meta.offset));
                const count_ptr: *u64 = @ptrCast(@alignCast(state_base + meta.offset + 8));
                const old_val = agg_ptr.*;
                const new_val = batchAggMax(getColF64(col_ptrs_ptr, val_col), batch_len, agg_ptr.*);
                if (new_val != old_val) {
                    // Undo: save previous f64 value (aux) and count (prev_value) before mutation
                    if (g_undo_enabled) undoAppend(.{
                        .op = .AGG_UPDATE, .slot = slot, ._pad1 = 0, ._pad2 = 0,
                        .key = 0, .prev_value = @truncate(count_ptr.*), .aux = @bitCast(old_val),
                    });
                    agg_ptr.* = new_val;
                    setChangeFlag(meta, ChangeFlag.SIZE_CHANGED);
                }
            },

            else => return @intFromEnum(ErrorCode.INVALID_PROGRAM),
        }
    }

    return @intFromEnum(ErrorCode.OK);
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

    // Check magic
    const magic = @as(u32, program[0]) | (@as(u32, program[1]) << 8) | (@as(u32, program[2]) << 16) | (@as(u32, program[3]) << 24);
    if (magic != PROGRAM_MAGIC) return 0;

    const num_slots = program[6];
    const init_len: u16 = @as(u16, program[10]) | (@as(u16, program[11]) << 8);

    if (PROGRAM_HEADER_SIZE + init_len > program_len) return 0;

    // Start with header + slot metadata (now 48 bytes per slot)
    var size: u32 = STATE_HEADER_SIZE + @as(u32, num_slots) * SLOT_META_SIZE;
    size = align8(size);

    // Parse init section to calculate slot data sizes
    const init_code = program[PROGRAM_HEADER_SIZE .. PROGRAM_HEADER_SIZE + init_len];
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
                if (capacity == 0) capacity = 1024;
                capacity = nextPowerOf2(capacity * 2); // 2x for load factor

                // Skip base bytes
                pc += 4;

                // Skip TTL params if present (10 bytes: f32 ttl + f32 grace + u8 ts_field + u8 start_of)
                if (type_flags.has_ttl) {
                    pc += 10;
                }

                // Calculate primary storage size based on slot type
                switch (type_flags.slot_type) {
                    .HASHMAP => {
                        // keys (u32) + values (u32) + timestamps (f64)
                        size += capacity * 4 + capacity * 4 + capacity * 8;
                    },
                    .HASHSET => {
                        // keys (u32)
                        size += capacity * 4;
                    },
                    .AGGREGATE => {
                        // value (f64) + count (u64)
                        size += 16;
                    },
                    .ARRAY => {
                        // values (u32) + timestamps (f64)
                        size += capacity * 4 + capacity * 8;
                    },
                    .CONDITION_TREE => {
                        // Placeholder until Phase 37 (tree layout TBD)
                    },
                }
                size = align8(size);

                // Add eviction index and evicted buffer if TTL enabled
                if (type_flags.has_ttl) {
                    // EvictionEntry is 12 bytes (f64 timestamp + u32 key_or_idx)
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

            // Superseded by SLOT_DEF (still supported for existing bytecode)
            .SLOT_HASHMAP => {
                const cap_lo = init_code[pc + 1];
                const cap_hi = init_code[pc + 2];
                var capacity: u32 = (@as(u32, cap_hi) << 8) | cap_lo;
                if (capacity == 0) capacity = 1024;
                capacity = nextPowerOf2(capacity * 2);
                pc += 5;

                // keys (u32) + values (u32) + timestamps (f64)
                size += capacity * 4 + capacity * 4 + capacity * 8;
                size = align8(size);
            },
            .SLOT_HASHSET => {
                const cap_lo = init_code[pc + 1];
                const cap_hi = init_code[pc + 2];
                var capacity: u32 = (@as(u32, cap_hi) << 8) | cap_lo;
                if (capacity == 0) capacity = 1024;
                capacity = nextPowerOf2(capacity * 2);
                pc += 4;

                // keys (u32)
                size += capacity * 4;
                size = align8(size);
            },
            .SLOT_AGGREGATE => {
                pc += 2;
                // value (f64) + count (u64)
                size += 16;
                size = align8(size);
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

    // Check magic
    const magic = @as(u32, program[0]) | (@as(u32, program[1]) << 8) | (@as(u32, program[2]) << 16) | (@as(u32, program[3]) << 24);
    if (magic != PROGRAM_MAGIC) return @intFromEnum(ErrorCode.INVALID_PROGRAM);

    const num_slots = program[6];
    const init_len: u16 = @as(u16, program[10]) | (@as(u16, program[11]) << 8);

    if (PROGRAM_HEADER_SIZE + init_len > program_len) return @intFromEnum(ErrorCode.INVALID_PROGRAM);

    // Write state header (new format v2)
    // Layout: magic(4) + format_version(1) + program_version(2) + ruleset_version(2) +
    //         num_slots(1) + num_vars(1) + num_bitvecs(1) + flags(1) + reserved(19)
    const state_u32: [*]u32 = @ptrCast(@alignCast(state_ptr));
    state_u32[0] = STATE_MAGIC;

    // Byte-level access for the rest of the header
    state_ptr[StateHeaderOffset.FORMAT_VERSION] = STATE_FORMAT_VERSION;
    // Program version from program header (bytes 4-5)
    state_ptr[StateHeaderOffset.PROGRAM_VERSION] = program[4];
    state_ptr[StateHeaderOffset.PROGRAM_VERSION + 1] = program[5];
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

    // Parse init section and initialize slots
    const init_code = program[PROGRAM_HEADER_SIZE .. PROGRAM_HEADER_SIZE + init_len];
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
                if (capacity == 0) capacity = 1024;
                capacity = nextPowerOf2(capacity * 2);
                pc += 4;

                // Read TTL params if present (10 bytes: f32 ttl + f32 grace + u8 ts_field + u8 start_of)
                var ttl_seconds: f32 = 0.0;
                var grace_seconds: f32 = 0.0;
                var timestamp_field_idx: u8 = 0;
                var start_of: DurationUnit = .NONE;
                if (type_flags.has_ttl) {
                    const ttl_bytes = init_code[pc .. pc + 4];
                    const ttl_ptr: *const f32 = @ptrCast(@alignCast(ttl_bytes.ptr));
                    ttl_seconds = ttl_ptr.*;
                    const grace_bytes = init_code[pc + 4 .. pc + 8];
                    const grace_ptr: *const f32 = @ptrCast(@alignCast(grace_bytes.ptr));
                    grace_seconds = grace_ptr.*;
                    timestamp_field_idx = init_code[pc + 8];
                    start_of = @enumFromInt(init_code[pc + 9]);
                    pc += 10;
                }

                const primary_data_offset = data_offset;

                // Initialize primary storage based on slot type
                switch (type_flags.slot_type) {
                    .HASHMAP => {
                        // keys (u32) + values (u32) + timestamps (f64)
                        const keys_ptr: [*]u32 = @ptrCast(@alignCast(state_ptr + data_offset));
                        for (0..capacity) |i| {
                            keys_ptr[i] = EMPTY_KEY;
                        }

                        const ts_offset = data_offset + capacity * 8;
                        const ts_ptr: [*]f64 = @ptrCast(@alignCast(state_ptr + ts_offset));
                        for (0..capacity) |i| {
                            ts_ptr[i] = -std.math.inf(f64);
                        }

                        data_offset += capacity * 4 + capacity * 4 + capacity * 8;
                    },
                    .CONDITION_TREE => {
                        // Placeholder until Phase 37
                    },
                    .HASHSET => {
                        const keys_ptr: [*]u32 = @ptrCast(@alignCast(state_ptr + data_offset));
                        for (0..capacity) |i| {
                            keys_ptr[i] = EMPTY_KEY;
                        }
                        data_offset += capacity * 4;
                    },
                    .AGGREGATE => {
                        const agg_ptr: *f64 = @ptrCast(@alignCast(state_ptr + data_offset));
                        agg_ptr.* = 0.0; // Will be set properly below
                        const count_ptr: *u64 = @ptrCast(@alignCast(state_ptr + data_offset + 8));
                        count_ptr.* = 0;
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
                    .SUM, // agg_type not used for non-aggregates
                    ttl_seconds,
                    grace_seconds,
                    timestamp_field_idx,
                    start_of,
                    eviction_index_offset,
                    eviction_index_capacity,
                    evicted_buffer_offset,
                );
            },

            // Superseded by SLOT_DEF (still supported for existing bytecode)
            .SLOT_HASHMAP => {
                const slot = init_code[pc];
                const cap_lo = init_code[pc + 1];
                const cap_hi = init_code[pc + 2];
                var capacity: u32 = (@as(u32, cap_hi) << 8) | cap_lo;
                if (capacity == 0) capacity = 1024;
                capacity = nextPowerOf2(capacity * 2);
                pc += 5;

                const type_flags = SlotTypeFlags{
                    .slot_type = .HASHMAP,
                    .has_ttl = false,
                    .has_evict_trigger = false,
                };

                // Initialize keys to EMPTY_KEY
                const keys_ptr: [*]u32 = @ptrCast(@alignCast(state_ptr + data_offset));
                for (0..capacity) |i| {
                    keys_ptr[i] = EMPTY_KEY;
                }

                // Timestamps start after values - initialize to -Infinity
                const ts_offset = data_offset + capacity * 8;
                const ts_ptr: [*]f64 = @ptrCast(@alignCast(state_ptr + ts_offset));
                for (0..capacity) |i| {
                    ts_ptr[i] = -std.math.inf(f64);
                }

                const primary_offset = data_offset;
                data_offset += capacity * 4 + capacity * 4 + capacity * 8;
                data_offset = align8(data_offset);

                writeSlotMeta(state_ptr, slot, primary_offset, capacity, type_flags, .SUM, 0.0, 0.0, 0, .NONE, 0, 0, 0);
            },
            .SLOT_HASHSET => {
                const slot = init_code[pc];
                const cap_lo = init_code[pc + 1];
                const cap_hi = init_code[pc + 2];
                var capacity: u32 = (@as(u32, cap_hi) << 8) | cap_lo;
                if (capacity == 0) capacity = 1024;
                capacity = nextPowerOf2(capacity * 2);
                pc += 4;

                const type_flags = SlotTypeFlags{
                    .slot_type = .HASHSET,
                    .has_ttl = false,
                    .has_evict_trigger = false,
                };

                // Initialize keys to EMPTY_KEY
                const keys_ptr: [*]u32 = @ptrCast(@alignCast(state_ptr + data_offset));
                for (0..capacity) |i| {
                    keys_ptr[i] = EMPTY_KEY;
                }

                const primary_offset = data_offset;
                data_offset += capacity * 4;
                data_offset = align8(data_offset);

                writeSlotMeta(state_ptr, slot, primary_offset, capacity, type_flags, .SUM, 0.0, 0.0, 0, .NONE, 0, 0, 0);
            },
            .SLOT_AGGREGATE => {
                const slot = init_code[pc];
                const agg_type: AggType = @enumFromInt(init_code[pc + 1]);
                pc += 2;

                const type_flags = SlotTypeFlags{
                    .slot_type = .AGGREGATE,
                    .has_ttl = false,
                    .has_evict_trigger = false,
                };

                // Initialize aggregate value
                const agg_ptr: *f64 = @ptrCast(@alignCast(state_ptr + data_offset));
                agg_ptr.* = switch (agg_type) {
                    .MIN => std.math.inf(f64),
                    .MAX => -std.math.inf(f64),
                    else => 0.0,
                };

                // Initialize count to 0
                const count_ptr: *u64 = @ptrCast(@alignCast(state_ptr + data_offset + 8));
                count_ptr.* = 0;

                const primary_offset = data_offset;
                data_offset += 16;
                data_offset = align8(data_offset);

                writeSlotMeta(state_ptr, slot, primary_offset, 0, type_flags, agg_type, 0.0, 0.0, 0, .NONE, 0, 0, 0);
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
    const data_ptr = state_base + slot_offset;
    const keys: [*]const u32 = @ptrCast(@alignCast(data_ptr));

    var i: u32 = 0;
    while (i < capacity) : (i += 1) {
        const k = keys[i];
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
    const data_ptr = state_base + slot_offset;
    const keys: [*]const u32 = @ptrCast(@alignCast(data_ptr));

    var i: u32 = current + 1;
    while (i < capacity) : (i += 1) {
        const k = keys[i];
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
    const data_ptr = state_base + slot_offset;
    const keys: [*]const u32 = @ptrCast(@alignCast(data_ptr));
    const values: [*]const u32 = @ptrCast(@alignCast(data_ptr + capacity * 4));

    const key = keys[pos];
    const val = values[pos];
    return (@as(u64, val) << 32) | key;
}

/// Start iteration over HashSet elements.
/// Returns first valid slot index, or capacity if empty.
pub export fn vm_set_iter_start(
    state_base: [*]u8,
    slot_offset: u32,
    capacity: u32,
) u32 {
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
    return vm_map_iter_next(state_base, slot_offset, capacity, current);
}

/// Get element at iterator position (just the key for sets).
pub export fn vm_set_iter_get(
    state_base: [*]u8,
    slot_offset: u32,
    pos: u32,
) u32 {
    const data_ptr = state_base + slot_offset;
    const keys: [*]const u32 = @ptrCast(@alignCast(data_ptr));
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
    g_undo_overflow = false;
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
    if (g_undo_overflow and g_undo_state_size <= UNDO_SHADOW_CAPACITY) {
        // Step 1: Restore shadow buffer — undoes all mutations after the overflow point
        // that weren't captured in the undo log
        @memcpy(state_base[0..g_undo_state_size], g_undo_shadow[0..g_undo_state_size]);
    }
    // Step 2: Replay undo log in reverse — undoes logged mutations before overflow
    var i = g_undo_count;
    while (i > checkpoint_pos) {
        i -= 1;
        rollbackEntry(state_base, g_undo_entries[i]);
    }
    g_undo_count = checkpoint_pos;
    restoreChangeFlags(state_base);
}

/// Commit (discard) undo entries and disable undo logging.
/// Call after speculative execution succeeds.
pub export fn vm_undo_commit(_state_base: [*]u8, _checkpoint_pos: u32) void {
    _ = _state_base;
    _ = _checkpoint_pos;
    g_undo_count = 0;
    g_undo_overflow = false;
    g_undo_enabled = false;
}

/// Check if undo log overflowed during speculation.
/// Returns 1 if overflow occurred, 0 otherwise.
/// Overflow is now handled internally via shadow buffer — this export is kept for debugging/testing.
pub export fn vm_undo_has_overflow() u32 {
    return if (g_undo_overflow) @as(u32, 1) else @as(u32, 0);
}
