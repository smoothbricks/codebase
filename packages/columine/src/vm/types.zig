// =============================================================================
// Columine VM Types — Shared constants, enums, and layout definitions
// =============================================================================
//
// Foundational types imported by all other VM sub-modules.
// Contains NO logic — only type definitions, constants, and layout structs.

const std = @import("std");

// =============================================================================
// Constants
// =============================================================================

pub const STATE_MAGIC: u32 = 0x53544154; // "STAT"
pub const PROGRAM_MAGIC: u32 = 0x314D4C43; // "CLM1"
pub const RETE_MAGIC: u32 = 0x45544552; // "RETE"
pub const STATE_HEADER_SIZE: u32 = 32;
// Program layout: [0..31] hash, [32..45] content header (magic, version, init_len, reduce_len), [46..] init+reduce
pub const PROGRAM_HASH_PREFIX: u32 = 32;
pub const PROGRAM_HEADER_SIZE: u32 = 46; // 32 + 14 (content header)
pub const RETE_HEADER_SIZE: u32 = 16;

// State format version - increment when header layout changes
pub const STATE_FORMAT_VERSION: u8 = 2;

pub const EMPTY_KEY: u32 = 0xFFFFFFFF;
pub const TOMBSTONE: u32 = 0xFFFFFFFE;

pub const BITMAP_SERIALIZED_LEN_BYTES: u32 = 4;
pub const BITMAP_BYTES_PER_CAPACITY: u32 = 8;
pub const BITMAP_BASE_BYTES: u32 = 256;

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
//
// SlotTypeFlags byte layout:
// ┌─────────────────────────────────────┐
// │ 7 │ 6 │ 5 │ 4 │ 3 │ 2 │ 1 │ 0 │
// │rsv│no_ts│evict│ttl│  slot_type   │
// └─────────────────────────────────────┘
// bits 0-3: slot_type (0-15, supports 16 slot types)
// bit 4:    has_ttl (if 1, TTL params in bytecode)
// bit 5:    has_evict_trigger (if 1, fire RETE rules on eviction)
// bit 6:    no_hashmap_timestamps (if 1, HASHMAP omits f64 side array)
// bit 7:    reserved

pub const SlotType = enum(u4) {
    HASHMAP = 0,
    HASHSET = 1,
    AGGREGATE = 2,
    ARRAY = 3, // For `.within()` without keyBy - stores array of events
    CONDITION_TREE = 4, // Condition router tree (VM handlers: Phase 37)
    SCALAR = 5, // Single typed value + comparison timestamp for latest()
    STRUCT_MAP = 6, // Multi-field hash map with per-row bitset
    ORDERED_LIST = 7, // Append-only sequential storage (scalar or struct rows)
    BITMAP = 8, // Roaring-style reducer membership represented as u32 set semantics
    NESTED = 9, // Arena-allocated nested containers: Map<K, Set/Map/Agg/List>
};

pub const SlotTypeFlags = packed struct(u8) {
    slot_type: SlotType, // bits 0-3
    has_ttl: bool, // bit 4: TTL params follow in bytecode
    has_evict_trigger: bool, // bit 5: fire RETE rules on eviction
    no_hashmap_timestamps: bool = false, // bit 6: HASHMAP storage optimization
    reserved: u1 = 0, // bit 7

    pub fn fromByte(b: u8) SlotTypeFlags {
        return @bitCast(b);
    }

    pub fn toByte(self: SlotTypeFlags) u8 {
        return @bitCast(self);
    }
};

/// Subtype for AGGREGATE and SCALAR slots. Stored in metadata byte 13.
/// Values 1-5: f64 aggregate operations (for AGGREGATE slots)
/// Values 6-7: reserved
/// Values 8-10: scalar value types (for SCALAR slots)
/// Values 11-13: i64 aggregate operations (lossless integer precision)
pub const AggType = enum(u8) {
    SUM = 1,
    COUNT = 2,
    MIN = 3,
    MAX = 4,
    AVG = 5,
    // 6-7: reserved
    SCALAR_U32 = 8, // Interned string ID
    SCALAR_F64 = 9, // 64-bit float
    SCALAR_I64 = 10, // 64-bit signed integer (bigint, timestamps)
    SUM_I64 = 11, // Lossless i64 sum (S.bigint(), S.timestamp())
    MIN_I64 = 12, // Lossless i64 min
    MAX_I64 = 13, // Lossless i64 max
};

pub const StructFieldType = enum(u8) {
    UINT32 = 0, // 4 bytes
    INT64 = 1, // 8 bytes
    FLOAT64 = 2, // 8 bytes
    BOOL = 3, // 1 byte (stored as u8)
    STRING = 4, // 4 bytes (interned u32)
    // Array types — in-row storage is offset:u32 + length:u32 = 8 bytes
    // Actual array data lives in per-slot arena
    ARRAY_U32 = 5, // arena element: 4 bytes
    ARRAY_I64 = 6, // arena element: 8 bytes
    ARRAY_F64 = 7, // arena element: 8 bytes
    ARRAY_STRING = 8, // arena element: 4 bytes (interned u32)
    ARRAY_BOOL = 9, // arena element: 1 byte
};

// =============================================================================
// Eviction Index Entry (for TTL slots)
// =============================================================================
// Sorted by timestamp ascending - oldest entries at front for O(expired) eviction

pub const EvictionEntry = packed struct {
    timestamp: f64, // Event timestamp (8 bytes)
    key_or_idx: u32, // Key (for Map/Set) or index (for Array) (4 bytes)
    value: u32, // Evicted value snapshot for RETE :evicted bindings (4 bytes)
    // Total: 16 bytes per entry
};

pub const ConditionTreeState = extern struct {
    lifecycle_generation: u32,
    last_removed_key: u32,
};

// Version marker for condition-tree matcher payloads consumed by ax_eval.zig.
// Kept in columine so state/matcher evolution stays synchronized.
pub const CONDITION_TREE_MATCHER_PLAN_VERSION: u16 = 1;

// Condition tree node type constants (must match TypeScript condition node types
pub const CT_NODE_EQ: u8 = 1;
pub const CT_NODE_NEQ: u8 = 2;
pub const CT_NODE_GT: u8 = 3;
pub const CT_NODE_GTE: u8 = 4;
pub const CT_NODE_LT: u8 = 5;
pub const CT_NODE_LTE: u8 = 6;
pub const CT_NODE_IN: u8 = 7;
pub const CT_NODE_RANGE: u8 = 8;
pub const CT_NODE_BOOLEAN: u8 = 9;
pub const CT_NODE_NOT: u8 = 10;
pub const CT_NODE_DESTINATION: u8 = 11;

// CONDITION_TREE slots keep explicit lifecycle bytes in reducer state so
// the runtime can detect stale plans after restore without re-hashing.
pub const CONDITION_TREE_STATE_BYTES: u32 = @sizeOf(ConditionTreeState);

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
    // For AGGREGATE: cap_lo=aggType, cap_hi=0
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
    BATCH_MAP_UPSERT_LATEST_IF = 0x28, // slot:u8, key_col:u8, val_col:u8, ts_col:u8, pred_col:u8
    BATCH_MAP_UPSERT_FIRST_IF = 0x29, // slot:u8, key_col:u8, val_col:u8, pred_col:u8
    BATCH_MAP_UPSERT_LAST_IF = 0x2A, // slot:u8, key_col:u8, val_col:u8, pred_col:u8
    BATCH_MAP_REMOVE_IF = 0x2B, // slot:u8, key_col:u8, pred_col:u8
    BATCH_MAP_UPSERT_MAX_IF = 0x2C, // slot:u8, key_col:u8, val_col:u8, cmp_col:u8, pred_col:u8
    BATCH_MAP_UPSERT_MIN_IF = 0x2D, // slot:u8, key_col:u8, val_col:u8, cmp_col:u8, pred_col:u8

    // HashSet batch ops
    BATCH_SET_INSERT = 0x30, // slot:u8, elem_col:u8
    BATCH_SET_REMOVE = 0x31, // slot:u8, elem_col:u8
    BATCH_SET_INSERT_TTL = 0x32, // slot:u8, elem_col:u8, ts_col:u8
    BATCH_SET_INSERT_IF = 0x33, // slot:u8, elem_col:u8, pred_col:u8

    // Bitmap batch ops
    BATCH_BITMAP_ADD = 0x34, // slot:u8, elem_col:u8
    BATCH_BITMAP_REMOVE = 0x35, // slot:u8, elem_col:u8

    // Bitmap in-place set algebra (slot × slot)
    BATCH_BITMAP_AND = 0x36, // target_slot:u8, source_slot:u8
    BATCH_BITMAP_OR = 0x37, // target_slot:u8, source_slot:u8
    BATCH_BITMAP_ANDNOT = 0x38, // target_slot:u8, source_slot:u8
    BATCH_BITMAP_XOR = 0x39, // target_slot:u8, source_slot:u8

    // Bitmap in-place set algebra (slot × scratch result)
    BATCH_BITMAP_AND_SCRATCH = 0x3A, // target_slot:u8
    BATCH_BITMAP_OR_SCRATCH = 0x3B, // target_slot:u8
    BATCH_BITMAP_ANDNOT_SCRATCH = 0x3C, // target_slot:u8
    BATCH_BITMAP_XOR_SCRATCH = 0x3D, // target_slot:u8

    // Aggregate batch ops (SIMD accelerated)
    BATCH_AGG_SUM = 0x40, // slot:u8, val_col:u8
    BATCH_AGG_COUNT = 0x41, // slot:u8
    BATCH_AGG_MIN = 0x42, // slot:u8, val_col:u8
    BATCH_AGG_MAX = 0x43, // slot:u8, val_col:u8
    BATCH_AGG_SUM_IF = 0x44, // slot:u8, val_col:u8, pred_col:u8
    BATCH_AGG_COUNT_IF = 0x45, // slot:u8, pred_col:u8
    BATCH_AGG_MIN_IF = 0x46, // slot:u8, val_col:u8, pred_col:u8
    BATCH_AGG_MAX_IF = 0x47, // slot:u8, val_col:u8, pred_col:u8

    // Scalar batch ops — store latest value (highest comparison timestamp wins)
    // AggType subtype (SCALAR_U32/SCALAR_F64/SCALAR_I64) is in slot metadata.
    BATCH_SCALAR_LATEST = 0x48, // slot:u8, val_col:u8, cmp_col:u8

    // i64 aggregate ops — lossless integer accumulation for S.bigint()/S.i64()/S.timestamp()
    // Same 16-byte slot layout as f64 aggregates but value stored as i64.
    BATCH_AGG_SUM_I64 = 0x49, // slot:u8, val_col:u8
    BATCH_AGG_MIN_I64 = 0x4a, // slot:u8, val_col:u8
    BATCH_AGG_MAX_I64 = 0x4b, // slot:u8, val_col:u8

    // Struct map init (variable-length: base 6 bytes + num_fields)
    SLOT_STRUCT_MAP = 0x18, // slot:u8, type_flags:u8, cap_lo:u8, cap_hi:u8, num_fields:u8, [field_type:u8 x num_fields]

    // Struct map batch ops
    BATCH_STRUCT_MAP_UPSERT_LAST = 0x80, // slot:u8, key_col:u8, num_vals:u8, [val_col:u8, field_idx:u8] x num_vals

    // Ordered list ops (body opcodes inside FOR_EACH_EVENT/FLAT_MAP blocks)
    LIST_APPEND = 0x84, // slot:u8, val_col:u8
    LIST_APPEND_STRUCT = 0x85, // slot:u8, num_vals:u8, [(val_col:u8, field_idx:u8) × N]

    // Ordered list init
    SLOT_ORDERED_LIST = 0x19, // slot:u8, type_flags:u8, cap_lo:u8, cap_hi:u8 [, num_fields:u8, field_type:u8 × num_fields]

    // Nested container init
    SLOT_NESTED = 0x1A, // slot:u8, outer_type_flags:u8, outer_cap_lo:u8, outer_cap_hi:u8, inner_type:u8, inner_cap_lo:u8, inner_cap_hi:u8, inner_agg_type:u8

    // Nested container ops (body opcodes inside FOR_EACH_EVENT blocks)
    // These operate per-element, dispatched within the FOR_EACH_EVENT loop.
    NESTED_SET_INSERT = 0x90, // slot:u8, outer_key_col:u8, elem_col:u8
    NESTED_MAP_UPSERT_LAST = 0x92, // slot:u8, outer_key_col:u8, inner_key_col:u8, val_col:u8
    NESTED_AGG_UPDATE = 0x95, // slot:u8, outer_key_col:u8, val_col:u8

    // Block-based reduce section opcodes (4a)
    // FOR_EACH_EVENT wraps body opcodes that process one element at a time.
    // Body uses the same opcode byte values as BATCH_* but dispatched per-element.
    FOR_EACH_EVENT = 0xE0, // type_col:u8, type_id_lo:u8, type_id_hi_0:u8, type_id_hi_1:u8, type_id_hi_2:u8, body_len_lo:u8, body_len_hi:u8
    FLAT_MAP = 0xE1, // offsets_col:u8, parent_ts_col:u8, inner_body_len_lo:u8, inner_body_len_hi:u8

    // --- More planned opcodes in opcodes.zig ---

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
    NEEDS_GROWTH = 5,
    ARENA_OVERFLOW = 6, // Per-slot arena full (array fields in struct map)
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

pub const V4f64 = @Vector(4, f64);
pub const V4u32 = @Vector(4, u32);
pub const V2i64 = @Vector(2, i64);

// =============================================================================
// Slot Metadata Access
// =============================================================================

pub const SlotMeta = struct {
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

    /// HASHMAP timestamp/comparison side-array availability.
    pub fn hasHashMapTimestampStorage(self: SlotMeta) bool {
        return self.slotType() == .HASHMAP and !self.type_flags.no_hashmap_timestamps;
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
pub inline fn setChangeFlag(meta: SlotMeta, flag: u8) void {
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
// Utility functions
// =============================================================================

pub inline fn align8(n: u32) u32 {
    return (n + 7) & ~@as(u32, 7);
}

pub inline fn nextPowerOf2(n: u32) u32 {
    if (n <= 16) return 16;
    var v = n - 1;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    return v + 1;
}

pub fn structFieldSize(ft: StructFieldType) u32 {
    return switch (ft) {
        .UINT32 => 4,
        .INT64 => 8,
        .FLOAT64 => 8,
        .BOOL => 1,
        .STRING => 4,
        .ARRAY_U32, .ARRAY_I64, .ARRAY_F64, .ARRAY_STRING, .ARRAY_BOOL => 8,
    };
}

pub fn arenaElemSize(ft: StructFieldType) u32 {
    return switch (ft) {
        .ARRAY_U32, .ARRAY_STRING => 4,
        .ARRAY_I64, .ARRAY_F64 => 8,
        .ARRAY_BOOL => 1,
        else => 0,
    };
}

pub fn isArrayFieldType(ft: StructFieldType) bool {
    return switch (ft) {
        .ARRAY_U32, .ARRAY_I64, .ARRAY_F64, .ARRAY_STRING, .ARRAY_BOOL => true,
        else => false,
    };
}

pub fn hasArrayFields(num_fields: u8, field_types_ptr: [*]const u8) bool {
    for (0..num_fields) |i| {
        const ft: StructFieldType = @enumFromInt(field_types_ptr[i]);
        if (isArrayFieldType(ft)) return true;
    }
    return false;
}

pub fn arenaInitialCapacity(hash_capacity: u32) u32 {
    // 4× hash capacity → covers moderate array sizes without immediate growth
    return hash_capacity * 4;
}

pub fn computeStructRowLayout(num_fields: u8, field_types_ptr: [*]const u8) struct { row_size: u32, bitset_bytes: u32, descriptor_size: u32 } {
    const bitset_bytes: u32 = (num_fields + 7) / 8;
    var row_size: u32 = bitset_bytes;
    for (0..num_fields) |i| {
        const ft: StructFieldType = @enumFromInt(field_types_ptr[i]);
        row_size += structFieldSize(ft);
    }
    const descriptor_size = align8(@as(u32, num_fields));
    return .{ .row_size = row_size, .bitset_bytes = bitset_bytes, .descriptor_size = descriptor_size };
}

pub fn structFieldOffset(num_fields: u8, field_types_ptr: [*]const u8, target_field: u8) u32 {
    const bitset_bytes = (num_fields + 7) / 8;
    var offset: u32 = bitset_bytes;
    for (0..target_field) |i| {
        const ft: StructFieldType = @enumFromInt(field_types_ptr[i]);
        offset += structFieldSize(ft);
    }
    return offset;
}

// =============================================================================
// Column access helpers
// =============================================================================

pub inline fn getColU32(ptrs: [*]const [*]const u8, idx: u8) [*]const u32 {
    return @ptrCast(@alignCast(ptrs[idx]));
}

pub inline fn getColF64(ptrs: [*]const [*]const u8, idx: u8) [*]const f64 {
    return @ptrCast(@alignCast(ptrs[idx]));
}

pub inline fn getColI64(ptrs: [*]const [*]const u8, idx: u8) [*]const i64 {
    return @ptrCast(@alignCast(ptrs[idx]));
}

pub inline fn getColAs(comptime T: type, ptrs: [*]const [*]const u8, idx: u8) [*]const T {
    return @ptrCast(@alignCast(ptrs[idx]));
}
