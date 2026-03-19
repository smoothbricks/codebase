// =============================================================================
// =============================================================================
//
// This file is the single source of truth for all bytecode opcodes, slot types,
// aggregate types, and struct field types used by the columine VM.
//
// STATUS: This is a registry/specification file. It documents all implemented
// and planned opcodes. vm.zig currently uses inline hex values — this file
// does NOT replace the dispatch tables in vm.zig yet, but serves as the
// canonical catalog for documentation, tooling, and future migration.
//
// The VM processes events in columnar batches (Arrow RecordBatch). Each opcode
// operates on entire columns at once (SIMD-accelerated where possible).
//
// Path addressing (planned):
//   - Root slots are addressed by slot index (u8)
//   - Nested values use key path: slot[key1][key2][key3]...
//   - Path stored as: depth:u8, key_input_0:u8, key_input_1:u8, ...

pub const Opcode = enum(u8) {
    // ═══════════════════════════════════════════════════════════════════
    // Control Flow
    // ═══════════════════════════════════════════════════════════════════

    /// Stop execution. Terminates both init and reduce sections.
    HALT = 0x00, // ✓ implemented

    // ═══════════════════════════════════════════════════════════════════
    // State Slot Creation (init section — executed once at state init)
    // ═══════════════════════════════════════════════════════════════════
    //
    // Unified slot definition opcode. SlotTypeFlags byte encodes slot
    // type (4 bits) + TTL/eviction flags (4 bits). For AGGREGATE/SCALAR
    // slots, cap_lo encodes the AggType subtype.
    //
    // TTL params (10 bytes, present when has_ttl flag set):
    //   ttl_seconds:f32, grace_seconds:f32, ts_field_idx:u8, start_of:u8

    /// Unified slot definition — covers HASHMAP, HASHSET, AGGREGATE, ARRAY,
    /// CONDITION_TREE, SCALAR, BITMAP slot types.
    SLOT_DEF = 0x10, // ✓ implemented — slot:u8, type_flags:u8, cap_lo:u8, cap_hi:u8 [, ttl params]

    /// Array slot for .within() without keyBy — stores array of events.
    SLOT_ARRAY = 0x14, // ✓ implemented — slot:u8, type_flags:u8, cap_lo:u8, cap_hi:u8

    /// Struct map slot — multi-field hash map with per-row field presence bitset.
    /// Variable-length init: base 6 bytes + num_fields field type bytes.
    SLOT_STRUCT_MAP = 0x18, // ✓ implemented — slot:u8, type_flags:u8, cap_lo:u8, cap_hi:u8, num_fields:u8, [field_type:u8 × num_fields]

    /// Ordered list slot — append-only sequential storage (scalar or struct rows).
    /// If num_fields > 0, followed by field type bytes (struct rows).
    SLOT_ORDERED_LIST = 0x19, // ✓ implemented — slot:u8, type_flags:u8, cap_lo:u8, cap_hi:u8 [, num_fields:u8, field_type:u8 × num_fields]

    // ─── Planned slot types ────────────────────────────────────────────

    // Map<K, Map<...>> — nested containers
    // SLOT_MAP_OF_MAP = 0x15, // planned — slot:u8, cap:u16, key_type:u8, nested_slot_template:u8
    // SLOT_MAP_OF_SET = 0x16, // planned — slot:u8, cap:u16, key_type:u8, elem_type:u8
    // SLOT_MAP_OF_AGG = 0x17, // planned — slot:u8, cap:u16, key_type:u8, agg_type:u8

    // ═══════════════════════════════════════════════════════════════════
    // Batch HashMap Operations — process entire column per opcode
    // ═══════════════════════════════════════════════════════════════════

    /// keyBy(field).keepValue(latest('timestamp'))
    /// cmp_type: 0=u32, 1=f64, 2=i64 — typed comparison for the ts_col column
    BATCH_MAP_UPSERT_LATEST = 0x20, // ✓ implemented — slot:u8, key_col:u8, val_col:u8, ts_col:u8, cmp_type:u8
    /// keyBy(field).keepValue(first)
    BATCH_MAP_UPSERT_FIRST = 0x21, // ✓ implemented — slot:u8, key_col:u8, val_col:u8
    /// keyBy(field).keepValue(last)
    BATCH_MAP_UPSERT_LAST = 0x22, // ✓ implemented — slot:u8, key_col:u8, val_col:u8
    /// .removeKeys(stream)
    BATCH_MAP_REMOVE = 0x23, // ✓ implemented — slot:u8, key_col:u8

    /// TTL-aware latest upsert (tracks insertion in eviction index)
    BATCH_MAP_UPSERT_LATEST_TTL = 0x24, // ✓ implemented — slot:u8, key_col:u8, val_col:u8, ts_col:u8, cmp_type:u8
    /// TTL-aware last upsert (tracks insertion in eviction index)
    BATCH_MAP_UPSERT_LAST_TTL = 0x25, // ✓ implemented — slot:u8, key_col:u8, val_col:u8, ts_col:u8

    /// keyBy(field).keepValue(max('field'))
    /// cmp_type: 0=u32, 1=f64, 2=i64 — typed comparison for the cmp_col column
    BATCH_MAP_UPSERT_MAX = 0x26, // ✓ implemented — slot:u8, key_col:u8, val_col:u8, cmp_col:u8, cmp_type:u8
    /// keyBy(field).keepValue(min('field'))
    /// cmp_type: 0=u32, 1=f64, 2=i64 — typed comparison for the cmp_col column
    BATCH_MAP_UPSERT_MIN = 0x27, // ✓ implemented — slot:u8, key_col:u8, val_col:u8, cmp_col:u8, cmp_type:u8

    // ─── Conditional variants (_IF suffix = predicate-filtered) ────────

    /// Conditional latest upsert — only process rows where pred_col is truthy
    BATCH_MAP_UPSERT_LATEST_IF = 0x28, // ✓ implemented — slot:u8, key_col:u8, val_col:u8, ts_col:u8, cmp_type:u8, pred_col:u8
    /// Conditional first upsert
    BATCH_MAP_UPSERT_FIRST_IF = 0x29, // ✓ implemented — slot:u8, key_col:u8, val_col:u8, pred_col:u8
    /// Conditional last upsert
    BATCH_MAP_UPSERT_LAST_IF = 0x2A, // ✓ implemented — slot:u8, key_col:u8, val_col:u8, pred_col:u8
    /// Conditional remove
    BATCH_MAP_REMOVE_IF = 0x2B, // ✓ implemented — slot:u8, key_col:u8, pred_col:u8
    /// Conditional max upsert
    BATCH_MAP_UPSERT_MAX_IF = 0x2C, // ✓ implemented — slot:u8, key_col:u8, val_col:u8, cmp_col:u8, cmp_type:u8, pred_col:u8
    /// Conditional min upsert
    BATCH_MAP_UPSERT_MIN_IF = 0x2D, // ✓ implemented — slot:u8, key_col:u8, val_col:u8, cmp_col:u8, cmp_type:u8, pred_col:u8

    // ═══════════════════════════════════════════════════════════════════
    // Batch HashSet Operations
    // ═══════════════════════════════════════════════════════════════════

    /// .unique() — insert element into set
    BATCH_SET_INSERT = 0x30, // ✓ implemented — slot:u8, elem_col:u8
    /// .remove() — remove element from set
    BATCH_SET_REMOVE = 0x31, // ✓ implemented — slot:u8, elem_col:u8
    /// TTL-aware set insert (tracks insertion in eviction index)
    BATCH_SET_INSERT_TTL = 0x32, // ✓ implemented — slot:u8, elem_col:u8, ts_col:u8
    /// Conditional set insert
    BATCH_SET_INSERT_IF = 0x33, // ✓ implemented — slot:u8, elem_col:u8, pred_col:u8

    // ═══════════════════════════════════════════════════════════════════
    // Batch Bitmap Operations (u32 ordinal membership, Roaring-style)
    // ═══════════════════════════════════════════════════════════════════

    /// Add ordinals to bitmap
    BATCH_BITMAP_ADD = 0x34, // ✓ implemented — slot:u8, elem_col:u8
    /// Remove ordinals from bitmap
    BATCH_BITMAP_REMOVE = 0x35, // ✓ implemented — slot:u8, elem_col:u8

    // ─── Bitmap in-place set algebra (slot × slot) ─────────────────────

    /// target &= source
    BATCH_BITMAP_AND = 0x36, // ✓ implemented — target_slot:u8, source_slot:u8
    /// target |= source
    BATCH_BITMAP_OR = 0x37, // ✓ implemented — target_slot:u8, source_slot:u8
    /// target &= ~source
    BATCH_BITMAP_ANDNOT = 0x38, // ✓ implemented — target_slot:u8, source_slot:u8
    /// target ^= source
    BATCH_BITMAP_XOR = 0x39, // ✓ implemented — target_slot:u8, source_slot:u8

    // ─── Bitmap in-place set algebra (slot × scratch result) ───────────

    /// target &= scratch
    BATCH_BITMAP_AND_SCRATCH = 0x3A, // ✓ implemented — target_slot:u8
    /// target |= scratch
    BATCH_BITMAP_OR_SCRATCH = 0x3B, // ✓ implemented — target_slot:u8
    /// target &= ~scratch
    BATCH_BITMAP_ANDNOT_SCRATCH = 0x3C, // ✓ implemented — target_slot:u8
    /// target ^= scratch
    BATCH_BITMAP_XOR_SCRATCH = 0x3D, // ✓ implemented — target_slot:u8

    // ═══════════════════════════════════════════════════════════════════
    // Batch Aggregate Operations (f64, SIMD-accelerated)
    // ═══════════════════════════════════════════════════════════════════

    /// Sum all values in column (f64 SIMD via maskedAggSum)
    BATCH_AGG_SUM = 0x40, // ✓ implemented — slot:u8, val_col:u8
    /// Count rows (u64 counter, 8-byte slot)
    BATCH_AGG_COUNT = 0x41, // ✓ implemented — slot:u8
    /// Track minimum value (f64 SIMD via maskedAggMin)
    BATCH_AGG_MIN = 0x42, // ✓ implemented — slot:u8, val_col:u8
    /// Track maximum value (f64 SIMD via maskedAggMax)
    BATCH_AGG_MAX = 0x43, // ✓ implemented — slot:u8, val_col:u8

    // ─── Conditional aggregate variants ────────────────────────────────

    BATCH_AGG_SUM_IF = 0x44, // ✓ implemented — slot:u8, val_col:u8, pred_col:u8
    BATCH_AGG_COUNT_IF = 0x45, // ✓ implemented — slot:u8, pred_col:u8
    BATCH_AGG_MIN_IF = 0x46, // ✓ implemented — slot:u8, val_col:u8, pred_col:u8
    BATCH_AGG_MAX_IF = 0x47, // ✓ implemented — slot:u8, val_col:u8, pred_col:u8

    // ═══════════════════════════════════════════════════════════════════
    // Batch Scalar Operations
    // ═══════════════════════════════════════════════════════════════════
    //
    // SCALAR slots store a single typed value + comparison timestamp.
    // AggType subtype (SCALAR_U32/SCALAR_F64/SCALAR_I64) is in slot metadata.
    // Layout: value([8]u8) + cmp_ts(f64) = 16 bytes.

    /// Store value from event with highest comparison timestamp
    BATCH_SCALAR_LATEST = 0x48, // ✓ implemented — slot:u8, val_col:u8, cmp_col:u8

    // ═══════════════════════════════════════════════════════════════════
    // Batch i64 Aggregate Operations (lossless integer precision)
    // ═══════════════════════════════════════════════════════════════════
    //
    // Same 16-byte slot layout as f64 aggregates but value field is i64.
    // Used for S.bigint(), S.i64(), S.timestamp() — the DSL auto-detects
    // these types and emits i64 opcodes.

    /// Lossless i64 sum
    BATCH_AGG_SUM_I64 = 0x49, // ✓ implemented — slot:u8, val_col:u8
    /// Lossless i64 min
    BATCH_AGG_MIN_I64 = 0x4A, // ✓ implemented — slot:u8, val_col:u8
    /// Lossless i64 max
    BATCH_AGG_MAX_I64 = 0x4B, // ✓ implemented — slot:u8, val_col:u8

    // ═══════════════════════════════════════════════════════════════════
    // Struct Map Batch Operations (0x80+ range)
    // ═══════════════════════════════════════════════════════════════════
    //
    // Struct maps are multi-field hash maps with per-row field presence
    // bitset and typed columns. Variable-length encoding.

    /// Upsert into struct map — last-write-wins per key.
    /// Encoding: slot, key_col, num_vals, [val_col, field_idx] × num_vals,
    ///           num_array_vals, [(offsets_col, values_col, field_idx) × num_array_vals]
    BATCH_STRUCT_MAP_UPSERT_LAST = 0x80, // ✓ implemented

    // ═══════════════════════════════════════════════════════════════════
    // Ordered List Operations (body opcodes inside blocks)
    // ═══════════════════════════════════════════════════════════════════

    /// Append scalar value to ordered list
    LIST_APPEND = 0x84, // ✓ implemented — slot:u8, val_col:u8
    /// Append struct row to ordered list
    LIST_APPEND_STRUCT = 0x85, // ✓ implemented — slot:u8, num_vals:u8, [(val_col:u8, field_idx:u8) × N]

    // ═══════════════════════════════════════════════════════════════════
    // Block-Based Reduce Opcodes (0xE0+ range)
    // ═══════════════════════════════════════════════════════════════════
    //
    // FOR_EACH wraps body opcodes that process one element at a time,
    // dispatched only for rows matching one or more type discriminators.
    // Body opcodes reuse the same byte values as BATCH_* but are
    // dispatched per-element within the block.

    /// Type-discriminated event loop with multi-match support.
    /// When match_count == 1, processes rows where col == match_id (backward compat).
    /// When match_count > 1, processes rows matching ANY of the match_ids.
    FOR_EACH = 0xE0, // ✓ implemented — col:u8, match_count:u8, match_ids:u32le[match_count], body_len:u16le
    /// Flat-map expansion: iterates nested array offsets, running inner body per element.
    FLAT_MAP = 0xE1, // ✓ implemented — offsets_col:u8, parent_ts_col:u8, inner_body_len:u16le(2 bytes)

    // ═══════════════════════════════════════════════════════════════════
    // Time Filtering — planned (applied to input before other ops)
    // ═══════════════════════════════════════════════════════════════════

    // .within('4 minutes ago')
    // FILTER_WITHIN = 0x50, // planned — mask_reg:u8, ts_input:u8, threshold_ms:i64

    // .before(timestamp)
    // FILTER_BEFORE = 0x51, // planned — mask_reg:u8, ts_input:u8, threshold_ms:i64

    // .after(timestamp)
    // FILTER_AFTER = 0x52, // planned — mask_reg:u8, ts_input:u8, threshold_ms:i64

    // .between(start, end)
    // FILTER_BETWEEN = 0x53, // planned — mask_reg:u8, ts_input:u8, start_ms:i64, end_ms:i64
    // Columine reducer opcodes end at 0x4B (except struct map at 0x80+, blocks at 0xE0+).
    // Time filters may be relocated if RETE claims this range.

    // ═══════════════════════════════════════════════════════════════════
    // Expressions — planned (for predicates, computed transforms)
    // ═══════════════════════════════════════════════════════════════════

    // EXPR_CONST_I64 = 0x60, // planned — result_reg:u8, value:i64
    // EXPR_CONST_F64 = 0x61, // planned — result_reg:u8, value:f64
    // EXPR_CMP_GT = 0x62, // planned — result_reg:u8, left:u8, right:u8
    // EXPR_CMP_LT = 0x63, // planned — result_reg:u8, left:u8, right:u8
    // EXPR_CMP_EQ = 0x64, // planned — result_reg:u8, left:u8, right:u8
    // EXPR_CMP_GTE = 0x65, // planned — result_reg:u8, left:u8, right:u8
    // EXPR_CMP_LTE = 0x66, // planned — result_reg:u8, left:u8, right:u8
    // EXPR_AND = 0x67, // planned — result_reg:u8, left:u8, right:u8
    // EXPR_OR = 0x68, // planned — result_reg:u8, left:u8, right:u8
    // EXPR_NOT = 0x69, // planned — result_reg:u8, input:u8

    // ═══════════════════════════════════════════════════════════════════
    // JS Callbacks — planned (for e.apply())
    // ═══════════════════════════════════════════════════════════════════

    // Pause VM, call JS function, resume with result
    // CALLBACK = 0x70, // planned — result_reg:u8, callback_id:u8, arg_count:u8, arg_regs...:u8[]

    // Batched callback - call JS once with arrays, get array result
    // CALLBACK_BATCH = 0x71, // planned — result_input:u8, callback_id:u8, arg_count:u8, arg_inputs...:u8[]

    // ═══════════════════════════════════════════════════════════════════
    // Nested Map Operations — planned
    // ═══════════════════════════════════════════════════════════════════
    //
    // Design supports arbitrary nesting: Map<K1, Map<K2, Map<K3, ...V>>>
    // Each nested map/set/aggregate is addressed by a path of keys.

    // NESTED_UPSERT_LATEST = 0x90, // planned — slot:u8, depth:u8, key_inputs[depth]:u8[], val:u8, ts:u8
    // NESTED_UPSERT_FIRST = 0x91, // planned — slot:u8, depth:u8, key_inputs[depth]:u8[], val:u8
    // NESTED_UPSERT_LAST = 0x92, // planned — slot:u8, depth:u8, key_inputs[depth]:u8[], val:u8
    // NESTED_SET_INSERT = 0x93, // planned — slot:u8, depth:u8, key_inputs[depth]:u8[], elem:u8
    // NESTED_SET_REMOVE = 0x94, // planned — slot:u8, depth:u8, key_inputs[depth]:u8[], elem:u8
    // NESTED_AGG_UPDATE = 0x95, // planned — slot:u8, depth:u8, key_inputs[depth]:u8[], val:u8
    // NESTED_REMOVE = 0x96, // planned — slot:u8, depth:u8, key_inputs[depth]:u8[]

    /// Catch-all for unknown/future opcodes — allows non-exhaustive dispatch.
    _,
};

// =============================================================================
// Aggregate / Scalar Subtype
// =============================================================================
//
// Subtype for AGGREGATE and SCALAR slots. Stored in slot metadata byte 13.
// Values 1-5: f64 aggregate operations (for AGGREGATE slots)
// Values 6-7: reserved
// Values 8-10: scalar value types (for SCALAR slots)
// Values 11-13: i64 aggregate operations (lossless integer precision)

pub const AggType = enum(u8) {
    // ─── f64 aggregates ────────────────────────────────────────────────
    SUM = 1,
    COUNT = 2,
    MIN = 3,
    MAX = 4,
    AVG = 5,

    // 6-7: reserved

    // ─── Scalar subtypes (SCALAR slot type) ────────────────────────────
    SCALAR_U32 = 8, // Interned string ID (4 bytes stored in [8]u8)
    SCALAR_F64 = 9, // 64-bit float
    SCALAR_I64 = 10, // 64-bit signed integer (bigint, timestamps)

    // ─── i64 aggregates (lossless integer precision) ───────────────────
    SUM_I64 = 11, // Lossless i64 sum (S.bigint(), S.timestamp())
    MIN_I64 = 12, // Lossless i64 min
    MAX_I64 = 13, // Lossless i64 max
};

// =============================================================================
// Slot Types
// =============================================================================
//
// SlotTypeFlags byte layout:
// ┌─────────────────────────────────────┐
// │ 7 │ 6 │ 5 │ 4 │ 3 │ 2 │ 1 │ 0 │
// │rsv│no_ts│evict│ttl│  slot_type   │
// └─────────────────────────────────────┘
// bits 0-3: slot_type (0-15, supports 16 slot types)
// bit 4:    has_ttl (if 1, TTL params follow in bytecode — 10 bytes)
// bit 5:    has_evict_trigger (if 1, fire RETE rules on eviction)
// bit 6:    no_hashmap_timestamps (HASHMAP only; omit f64 timestamp side-array)
// bit 7:    reserved

pub const SlotType = enum(u4) {
    HASHMAP = 0, // ✓ implemented — key-value with optional timestamp + TTL
    HASHSET = 1, // ✓ implemented — unique u32 membership + TTL
    AGGREGATE = 2, // ✓ implemented — f64/i64 accumulators (SUM/COUNT/MIN/MAX/AVG)
    ARRAY = 3, // ✓ implemented — for .within() without keyBy, stores array of events
    CONDITION_TREE = 4, // ✓ implemented — condition router tree (VM handlers: Phase 37)
    SCALAR = 5, // ✓ implemented — single typed value + comparison timestamp for latest()
    STRUCT_MAP = 6, // ✓ implemented — multi-field hash map with per-row field bitset
    ORDERED_LIST = 7, // ✓ implemented — append-only sequential storage (scalar or struct rows)
    BITMAP = 8, // ✓ implemented — Roaring-style u32 set semantics
};

// =============================================================================
// Struct Field Types (for STRUCT_MAP and ORDERED_LIST struct rows)
// =============================================================================

pub const StructFieldType = enum(u8) {
    UINT32 = 0, // 4 bytes
    INT64 = 1, // 8 bytes
    FLOAT64 = 2, // 8 bytes
    BOOL = 3, // 1 byte (stored as u8)
    STRING = 4, // 4 bytes (interned u32)
    // Array types — in-row: 8 bytes (offset:u32 + length:u32 into per-slot arena)
    ARRAY_U32 = 5, // arena element: 4 bytes
    ARRAY_I64 = 6, // arena element: 8 bytes
    ARRAY_F64 = 7, // arena element: 8 bytes
    ARRAY_STRING = 8, // arena element: 4 bytes (interned u32)
    ARRAY_BOOL = 9, // arena element: 1 byte
};

// =============================================================================
// Duration Unit (for TTL startOf truncation)
// =============================================================================
//
// Encoding: 0=none, 1='s', 2='m', 3='h', 4='d', 5='w', 6='M', 7='Q', 8='y'
// The JS compiler normalizes long forms to short: 'month'/'months' -> 'M', etc.

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

// =============================================================================
// Program Header
// =============================================================================
//
// Program layout: [0..31] SHA-256 hash prefix, [32..45] content header, [46..] init + reduce bytecode
// Content header (14 bytes): magic(4) + version(2) + num_slots(1) + num_inputs(1) + num_callbacks(1) + flags(1) + init_code_len(2) + reduce_code_len(2)

/// "CLM1" in little-endian
pub const PROGRAM_MAGIC: u32 = 0x314D4C43;

/// Reserved bytes at program start for SHA-256 hash
pub const PROGRAM_HASH_PREFIX: u32 = 32;

/// Total header size: hash prefix (32) + content header (14)
pub const PROGRAM_HEADER_SIZE: u32 = 46;

pub const ProgramHeader = packed struct {
    magic: u32, // Must be PROGRAM_MAGIC ("CLM1")
    version: u16, // Protocol/schema version
    num_slots: u8, // Number of state slots
    num_inputs: u8, // Number of input columns
    num_callbacks: u8, // Number of JS callbacks (planned)
    flags: u8, // Feature flags
    init_code_len: u16, // Length of init bytecode (slot creation)
    reduce_code_len: u16, // Length of reduce bytecode (per-event ops)
    // Followed by:
    //   - init bytecode (init_code_len bytes)
    //   - reduce bytecode (reduce_code_len bytes)
};

// =============================================================================
// State Header
// =============================================================================
//
// State layout: 32-byte header + (num_slots × 48-byte slot metadata) + slot data
//
// State header (32 bytes):
// Offset | Size | Field
// -------|------|------------------
//   0    |  4   | magic (0x53544154 "STAT")
//   4    |  1   | format_version (2)
//   5    |  2   | program_version
//   7    |  2   | ruleset_version (RETE)
//   9    |  1   | num_slots
//  10    |  1   | num_vars (RETE)
//  11    |  1   | num_bitvecs (RETE)
//  12    |  1   | flags (bit 0: has_rete)
//  13    |  4   | derived_facts_offset
//  17    |  2   | derived_facts_capacity
//  19    |  1   | num_derived_fact_schemas
//  20    |  1   | derived_facts_change_flag
//  21    |  11  | reserved (zero)

/// "STAT" in little-endian
pub const STATE_MAGIC: u32 = 0x53544154;
pub const STATE_HEADER_SIZE: u32 = 32;
pub const STATE_FORMAT_VERSION: u8 = 2;

// =============================================================================
// Slot Metadata (48 bytes per slot)
// =============================================================================
//
// Offset | Size | Field
// -------|------|------------------
//   0    |  4   | offset (byte offset to slot data)
//   4    |  4   | capacity (for hashmap/hashset/array)
//   8    |  4   | size (current element count)
//  12    |  1   | type_flags (SlotTypeFlags)
//  13    |  1   | agg_type (AggType enum)
//  14    |  1   | change_flags (set by reducer ops)
//  15    |  1   | timestamp_field_idx
//  16    |  4   | ttl_seconds (f32)
//  20    |  4   | grace_seconds (f32)
//  24    |  4   | eviction_index_offset
//  28    |  4   | eviction_index_capacity
//  32    |  4   | eviction_index_size
//  36    |  4   | evicted_buffer_offset
//  40    |  4   | evicted_count
//  44    |  1   | start_of (DurationUnit)
//  45    |  3   | reserved

pub const SLOT_META_SIZE: u32 = 48;

// =============================================================================
// Change Flags (set by reducer ops, cleared after RETE execution)
// =============================================================================

pub const ChangeFlag = struct {
    pub const INSERTED: u8 = 0x01; // New key inserted
    pub const UPDATED: u8 = 0x02; // Existing value updated
    pub const REMOVED: u8 = 0x04; // Key removed
    pub const SIZE_CHANGED: u8 = 0x08; // Size changed (for aggregates)
    pub const EVICTED: u8 = 0x10; // Entries evicted due to TTL
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
