// =============================================================================
// State Size Calculation, Initialization, and Growth
// =============================================================================
//
// Handles program bytecode -> state buffer lifecycle:
//   vm_calculate_state_size   -- compute required buffer size from program
//   vm_init_state             -- initialize state buffer from program bytecode
//   vm_reset_state            -- re-initialize state to initial values
//   vm_calculate_grown_state_size -- compute buffer size after slot growth
//   vm_grow_state             -- copy state to larger buffer with rehashed grown slot
//
// Struct layout helpers used by both init and execution are also housed here:
//   getStructMapSlotDataSize, structFieldSize, structFieldOffset,
//   computeStructRowLayout, arenaElemSize, isArrayFieldType, hasArrayFields,
//   arenaInitialCapacity, ARENA_HEADER_SIZE, getTTLSideBufferSize.

const std = @import("std");
const types = @import("types.zig");
const vm = @import("vm.zig");
const hash_table = @import("hash_table.zig");
const aggregates = @import("aggregates.zig");
const nested = @import("nested.zig");
const slot_growth = @import("slot_growth.zig");

// Type / constant aliases from types.zig
const align8 = types.align8;
const nextPowerOf2 = types.nextPowerOf2;
const STATE_MAGIC = types.STATE_MAGIC;
const PROGRAM_MAGIC = types.PROGRAM_MAGIC;
const STATE_HEADER_SIZE = types.STATE_HEADER_SIZE;
const PROGRAM_HASH_PREFIX = types.PROGRAM_HASH_PREFIX;
const PROGRAM_HEADER_SIZE = types.PROGRAM_HEADER_SIZE;
const STATE_FORMAT_VERSION = types.STATE_FORMAT_VERSION;
const SLOT_META_SIZE = types.SLOT_META_SIZE;
const SlotType = types.SlotType;
const SlotTypeFlags = types.SlotTypeFlags;
const AggType = types.AggType;
const StructFieldType = types.StructFieldType;
const ErrorCode = types.ErrorCode;
const Opcode = types.Opcode;
const DurationUnit = types.DurationUnit;
const StateHeaderOffset = types.StateHeaderOffset;
const SlotMetaOffset = types.SlotMetaOffset;
const EvictionEntry = types.EvictionEntry;
const ConditionTreeState = types.ConditionTreeState;
const CONDITION_TREE_STATE_BYTES = types.CONDITION_TREE_STATE_BYTES;
const EMPTY_KEY = types.EMPTY_KEY;
const TOMBSTONE = types.TOMBSTONE;
const hashKey = types.hashKey;
const BITMAP_SERIALIZED_LEN_BYTES = types.BITMAP_SERIALIZED_LEN_BYTES;

// Runtime references from vm.zig
const bitmapPayloadCapacity = vm.bitmapPayloadCapacity;

// =============================================================================
// Struct Layout Helpers
// =============================================================================

pub fn structFieldSize(ft: StructFieldType) u32 {
    return switch (ft) {
        .UINT32, .STRING => 4,
        .INT64, .FLOAT64 => 8,
        .BOOL => 1,
        // Array fields store (offset:u32, length:u32) in-row -- 8 bytes
        .ARRAY_U32, .ARRAY_I64, .ARRAY_F64, .ARRAY_STRING, .ARRAY_BOOL => 8,
    };
}

/// Element size for array types in the arena
pub fn arenaElemSize(ft: StructFieldType) u32 {
    return switch (ft) {
        .ARRAY_U32, .ARRAY_STRING => 4,
        .ARRAY_I64, .ARRAY_F64 => 8,
        .ARRAY_BOOL => 1,
        else => unreachable,
    };
}

pub fn isArrayFieldType(ft: StructFieldType) bool {
    return @intFromEnum(ft) >= 5;
}

/// Check if any field in the descriptor is an array type
pub fn hasArrayFields(num_fields: u8, field_types_ptr: [*]const u8) bool {
    for (0..num_fields) |i| {
        if (field_types_ptr[i] >= 5) return true;
    }
    return false;
}

pub const ARENA_HEADER_SIZE: u32 = 8; // [arena_capacity:u32][arena_used:u32]

/// Heuristic initial arena capacity: 64 bytes per hash table entry
pub fn arenaInitialCapacity(hash_capacity: u32) u32 {
    return hash_capacity * 64;
}

/// Compute row size (bitset + field data) from field types stored in slot data prefix.
/// Returns: { row_size, bitset_bytes, descriptor_size }
pub fn computeStructRowLayout(num_fields: u8, field_types_ptr: [*]const u8) struct { row_size: u32, bitset_bytes: u32, descriptor_size: u32 } {
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
pub fn structFieldOffset(num_fields: u8, field_types_ptr: [*]const u8, target_field: u8) u32 {
    const bitset_bytes: u32 = ((@as(u32, num_fields) + 7) / 8);
    var offset: u32 = bitset_bytes;
    for (0..target_field) |i| {
        offset += structFieldSize(@enumFromInt(field_types_ptr[i]));
    }
    return offset;
}

/// Compute data region size for a STRUCT_MAP slot.
/// Layout: [field_types x num_fields padded to 4] + [keys u32 x capacity] + [rows x capacity]
pub fn getStructMapSlotDataSize(descriptor_size: u32, capacity: u32, row_size: u32, has_timestamps: bool) u32 {
    return descriptor_size + capacity * 4 + capacity * row_size + if (has_timestamps) capacity * 8 else 0;
}

fn getTTLSideBufferSize(has_ttl: bool, has_evict_trigger: bool, capacity: u32) u32 {
    if (!has_ttl) return 0;

    var size: u32 = align8(capacity * @sizeOf(EvictionEntry));
    if (has_evict_trigger) {
        size += align8(1024 * @sizeOf(EvictionEntry));
    }
    return size;
}

// =============================================================================
// State Size Calculation
// =============================================================================

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
                        // Derived facts HashMap: keys[cap]*4 + values_lo[cap]*4 + values_hi[cap]*4
                        // capacity from bytecode is the derived facts HashMap capacity
                        if (capacity > 0) {
                            size = align8(size);
                            size += capacity * 12;
                        }
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
                // (if elem_type == 0xFF/STRUCT: num_fields:u8, [field_type:u8 x num_fields])
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

// =============================================================================
// State Initialization
// =============================================================================

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
    std.mem.writeInt(u32, state_ptr[0..4], STATE_MAGIC, .little);

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

                        // Allocate and initialize derived facts HashMap region
                        // capacity from bytecode = derived facts HashMap capacity
                        if (capacity > 0) {
                            data_offset = align8(data_offset);
                            const derived_facts_offset = data_offset;

                            // Initialize keys to EMPTY_KEY sentinel
                            const keys: [*]u32 = @ptrCast(@alignCast(state_ptr + data_offset));
                            for (0..capacity) |i| keys[i] = EMPTY_KEY;
                            data_offset += capacity * 4;

                            // Zero-init values_lo and values_hi
                            @memset(state_ptr[data_offset .. data_offset + capacity * 4], 0);
                            data_offset += capacity * 4;
                            @memset(state_ptr[data_offset .. data_offset + capacity * 4], 0);
                            data_offset += capacity * 4;

                            // Write derived facts layout into state header
                            std.mem.writeInt(u32, state_ptr[StateHeaderOffset.DERIVED_FACTS_OFFSET..][0..4], derived_facts_offset, .little);
                            std.mem.writeInt(u16, state_ptr[StateHeaderOffset.DERIVED_FACTS_CAPACITY..][0..2], @intCast(capacity), .little);
                            // Set HAS_RETE flag
                            state_ptr[StateHeaderOffset.FLAGS] |= types.StateFlags.HAS_RETE;
                        }
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
    return @as(u32, vm.g_needs_growth_slot);
}

/// Compute state size with 2x capacity for the slot at `grown_slot_idx`.
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
                    // Arena moved with the slot data -- offset shifted by (new_offset - old_offset)
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

        // Update derived facts offset in state header when CONDITION_TREE slot moves
        if (slot_type == @intFromEnum(SlotType.CONDITION_TREE) and new_cap > 0) {
            const new_derived_offset = align8(new_offset + CONDITION_TREE_STATE_BYTES);
            std.mem.writeInt(u32, new_state_ptr[StateHeaderOffset.DERIVED_FACTS_OFFSET..][0..4], new_derived_offset, .little);
        }
    }

    return @intFromEnum(ErrorCode.OK);
}

// =============================================================================
// Tests
// =============================================================================

const testing = std.testing;

// ---------------------------------------------------------------------------
// Helper: build a minimal program with a single slot defined by SLOT_DEF
// ---------------------------------------------------------------------------
fn buildSingleSlotProgram(type_flags_byte: u8, cap_lo: u8, cap_hi: u8) [64]u8 {
    var prog = [_]u8{0} ** 64;
    const content = prog[PROGRAM_HASH_PREFIX..];
    // Magic "CLM1" little-endian
    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    content[4] = 1; // version lo
    content[5] = 0; // version hi
    content[6] = 1; // num_slots
    content[7] = 0; // body_len_lo (unused for init-only)
    content[8] = 0;
    content[9] = 0;
    // init_len = 6 (1 opcode + 1 slot + 1 flags + 1 cap_lo + 1 cap_hi + 1 HALT)
    const init_len: u16 = 6;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);
    content[12] = 0; // reduce_len lo
    content[13] = 0; // reduce_len hi
    // Init section at content[14]
    content[14] = @intFromEnum(Opcode.SLOT_DEF);
    content[15] = 0; // slot index
    content[16] = type_flags_byte;
    content[17] = cap_lo;
    content[18] = cap_hi;
    content[19] = @intFromEnum(Opcode.HALT);
    return prog;
}

// ---------------------------------------------------------------------------
// 1. computeStructRowLayout
// ---------------------------------------------------------------------------

test "computeStructRowLayout — 2 fields (UINT32, STRING)" {
    var fields = [_]u8{ @intFromEnum(StructFieldType.UINT32), @intFromEnum(StructFieldType.STRING) };
    const layout = computeStructRowLayout(2, &fields);
    // bitset = (2+7)/8 = 1, row_data = 1+4+4 = 9, padded to 4 = 12
    try testing.expectEqual(@as(u32, 1), layout.bitset_bytes);
    try testing.expectEqual(@as(u32, 12), layout.row_size);
    try testing.expectEqual(@as(u32, 8), layout.descriptor_size);
}

test "computeStructRowLayout — 3 fields (FLOAT64, BOOL, INT64)" {
    var fields = [_]u8{ @intFromEnum(StructFieldType.FLOAT64), @intFromEnum(StructFieldType.BOOL), @intFromEnum(StructFieldType.INT64) };
    const layout = computeStructRowLayout(3, &fields);
    // bitset = 1, row_data = 1+8+1+8 = 18, padded to 4 = 20
    try testing.expectEqual(@as(u32, 1), layout.bitset_bytes);
    try testing.expectEqual(@as(u32, 20), layout.row_size);
    try testing.expectEqual(@as(u32, 8), layout.descriptor_size);
}

test "computeStructRowLayout — 8 fields (all UINT32)" {
    var fields = [_]u8{@intFromEnum(StructFieldType.UINT32)} ** 8;
    const layout = computeStructRowLayout(8, &fields);
    // bitset = (8+7)/8 = 1, row_data = 1+8*4 = 33, padded to 4 = 36
    try testing.expectEqual(@as(u32, 1), layout.bitset_bytes);
    try testing.expectEqual(@as(u32, 36), layout.row_size);
    try testing.expectEqual(@as(u32, 8), layout.descriptor_size);
}

// ---------------------------------------------------------------------------
// 2. structFieldOffset
// ---------------------------------------------------------------------------

test "structFieldOffset — 3 fields (UINT32=4, FLOAT64=8, STRING=4)" {
    var fields = [_]u8{ @intFromEnum(StructFieldType.UINT32), @intFromEnum(StructFieldType.FLOAT64), @intFromEnum(StructFieldType.STRING) };
    const bitset_bytes: u32 = 1; // (3+7)/8
    // field 0: starts at bitset_bytes
    try testing.expectEqual(bitset_bytes, structFieldOffset(3, &fields, 0));
    // field 1: bitset + UINT32(4) = 1+4 = 5
    try testing.expectEqual(bitset_bytes + 4, structFieldOffset(3, &fields, 1));
    // field 2: bitset + UINT32(4) + FLOAT64(8) = 1+4+8 = 13
    try testing.expectEqual(bitset_bytes + 4 + 8, structFieldOffset(3, &fields, 2));
}

// ---------------------------------------------------------------------------
// 3. structFieldSize
// ---------------------------------------------------------------------------

test "structFieldSize — all types" {
    try testing.expectEqual(@as(u32, 4), structFieldSize(.UINT32));
    try testing.expectEqual(@as(u32, 8), structFieldSize(.INT64));
    try testing.expectEqual(@as(u32, 8), structFieldSize(.FLOAT64));
    try testing.expectEqual(@as(u32, 1), structFieldSize(.BOOL));
    try testing.expectEqual(@as(u32, 4), structFieldSize(.STRING));
    // Array types: in-row (offset:u32 + length:u32) = 8
    try testing.expectEqual(@as(u32, 8), structFieldSize(.ARRAY_U32));
    try testing.expectEqual(@as(u32, 8), structFieldSize(.ARRAY_I64));
    try testing.expectEqual(@as(u32, 8), structFieldSize(.ARRAY_F64));
    try testing.expectEqual(@as(u32, 8), structFieldSize(.ARRAY_STRING));
    try testing.expectEqual(@as(u32, 8), structFieldSize(.ARRAY_BOOL));
}

// ---------------------------------------------------------------------------
// 4. arenaInitialCapacity
// ---------------------------------------------------------------------------

test "arenaInitialCapacity — equals hash_capacity * 64" {
    try testing.expectEqual(@as(u32, 64 * 64), arenaInitialCapacity(64));
    try testing.expectEqual(@as(u32, 16 * 64), arenaInitialCapacity(16));
    try testing.expectEqual(@as(u32, 256 * 64), arenaInitialCapacity(256));
}

// ---------------------------------------------------------------------------
// 5. getStructMapSlotDataSize
// ---------------------------------------------------------------------------

test "getStructMapSlotDataSize — without timestamps" {
    // descriptor=8, cap=32, row_size=12 → 8 + 32*4 + 32*12 = 8+128+384 = 520
    try testing.expectEqual(@as(u32, 520), getStructMapSlotDataSize(8, 32, 12, false));
}

test "getStructMapSlotDataSize — with timestamps" {
    // descriptor=8, cap=32, row_size=12 → 8 + 32*4 + 32*12 + 32*8 = 8+128+384+256 = 776
    try testing.expectEqual(@as(u32, 776), getStructMapSlotDataSize(8, 32, 12, true));
}

// ---------------------------------------------------------------------------
// 6. getTTLSideBufferSize
// ---------------------------------------------------------------------------

test "getTTLSideBufferSize — no TTL returns 0" {
    try testing.expectEqual(@as(u32, 0), getTTLSideBufferSize(false, false, 32));
}

test "getTTLSideBufferSize — TTL only (no evict trigger)" {
    // cap=32, EvictionEntry=16 bytes → align8(32*16) = align8(512) = 512
    try testing.expectEqual(align8(32 * @sizeOf(EvictionEntry)), getTTLSideBufferSize(true, false, 32));
}

test "getTTLSideBufferSize — TTL + evict trigger" {
    const cap: u32 = 32;
    const expected = align8(cap * @sizeOf(EvictionEntry)) + align8(1024 * @sizeOf(EvictionEntry));
    try testing.expectEqual(expected, getTTLSideBufferSize(true, true, cap));
}

// ---------------------------------------------------------------------------
// 7. vm_calculate_state_size — HASHMAP slot
// ---------------------------------------------------------------------------

test "vm_calculate_state_size — HASHMAP" {
    // type_flags = HASHMAP (0x00), cap_lo=8, cap_hi=0 → requested=8, effective=nextPowerOf2(8*2)=16
    const prog = buildSingleSlotProgram(0x00, 8, 0);
    const size = vm_calculate_state_size(&prog, prog.len);
    try testing.expect(size > 0);

    const cap: u32 = 16; // nextPowerOf2(8*2)
    // header(32) + 1 slot meta(48) = 80, align8 = 80
    var expected: u32 = align8(STATE_HEADER_SIZE + 1 * SLOT_META_SIZE);
    // HASHMAP: keys(cap*4) + values(cap*4) + timestamps(cap*8)
    expected += cap * 4 + cap * 4 + cap * 8;
    expected = align8(expected);
    try testing.expectEqual(expected, size);
}

// ---------------------------------------------------------------------------
// 8. vm_calculate_state_size — AGGREGATE (COUNT)
// ---------------------------------------------------------------------------

test "vm_calculate_state_size — AGGREGATE COUNT" {
    // type_flags = AGGREGATE (0x02), cap_lo = AggType.COUNT (2), cap_hi = 0
    const prog = buildSingleSlotProgram(0x02, @intFromEnum(AggType.COUNT), 0);
    const size = vm_calculate_state_size(&prog, prog.len);
    try testing.expect(size > 0);

    var expected: u32 = align8(STATE_HEADER_SIZE + 1 * SLOT_META_SIZE);
    // COUNT aggregate: 8 bytes (not 16)
    expected += 8;
    expected = align8(expected);
    try testing.expectEqual(expected, size);
}

// ---------------------------------------------------------------------------
// 9. vm_init_state — HASHMAP (magic, keys, size, timestamps)
// ---------------------------------------------------------------------------

test "vm_init_state — HASHMAP keys=EMPTY_KEY, size=0, timestamps=-inf" {
    const prog = buildSingleSlotProgram(0x00, 8, 0);
    const size = vm_calculate_state_size(&prog, prog.len);
    try testing.expect(size > 0);

    var state_buf: [16384]u8 align(8) = [_]u8{0} ** 16384;
    const rc = vm_init_state(&state_buf, &prog, prog.len);
    try testing.expectEqual(@as(u32, 0), rc); // OK

    // Check magic
    const magic = std.mem.readInt(u32, state_buf[0..4], .little);
    try testing.expectEqual(STATE_MAGIC, magic);

    // Read slot meta
    const meta = types.getSlotMeta(&state_buf, 0);
    try testing.expectEqual(@as(u32, 0), meta.size_ptr.*); // size == 0

    const cap: u32 = 16; // nextPowerOf2(8*2)
    try testing.expectEqual(cap, meta.capacity);

    // Keys should be EMPTY_KEY
    const keys: [*]const u32 = @ptrCast(@alignCast(state_buf[meta.offset..]));
    for (0..cap) |i| {
        try testing.expectEqual(EMPTY_KEY, keys[i]);
    }

    // Timestamps should be -inf
    const ts_offset = meta.offset + cap * 4 + cap * 4;
    const ts: [*]const f64 = @ptrCast(@alignCast(state_buf[ts_offset..]));
    for (0..cap) |i| {
        try testing.expect(ts[i] == -std.math.inf(f64));
    }
}

// ---------------------------------------------------------------------------
// 10. vm_init_state — AGGREGATE (SUM → value=0.0, count=0; MIN → value=+inf)
// ---------------------------------------------------------------------------

test "vm_init_state — AGGREGATE SUM value=0, count=0" {
    const prog = buildSingleSlotProgram(0x02, @intFromEnum(AggType.SUM), 0);
    const size = vm_calculate_state_size(&prog, prog.len);
    try testing.expect(size > 0);

    var state_buf: [16384]u8 align(8) = [_]u8{0} ** 16384;
    const rc = vm_init_state(&state_buf, &prog, prog.len);
    try testing.expectEqual(@as(u32, 0), rc);

    const meta = types.getSlotMeta(&state_buf, 0);
    const slot = aggregates.AggSlot(.SUM).bind(&state_buf, meta.offset);
    try testing.expectApproxEqAbs(@as(f64, 0.0), slot.value(), 0.001);
    try testing.expectEqual(@as(u64, 0), slot.count());
}

test "vm_init_state — AGGREGATE MIN value=+inf" {
    const prog = buildSingleSlotProgram(0x02, @intFromEnum(AggType.MIN), 0);
    const size = vm_calculate_state_size(&prog, prog.len);
    try testing.expect(size > 0);

    var state_buf: [16384]u8 align(8) = [_]u8{0} ** 16384;
    const rc = vm_init_state(&state_buf, &prog, prog.len);
    try testing.expectEqual(@as(u32, 0), rc);

    const meta = types.getSlotMeta(&state_buf, 0);
    const slot = aggregates.AggSlot(.MIN).bind(&state_buf, meta.offset);
    try testing.expect(slot.value() == std.math.inf(f64));
    try testing.expectEqual(@as(u64, 0), slot.count());
}

// ---------------------------------------------------------------------------
// 11. vm_init_state — HASHSET (keys=EMPTY_KEY)
// ---------------------------------------------------------------------------

test "vm_init_state — HASHSET keys=EMPTY_KEY" {
    // HASHSET = 1
    const type_flags = SlotTypeFlags{ .slot_type = .HASHSET, .has_ttl = false, .has_evict_trigger = false };
    const prog = buildSingleSlotProgram(type_flags.toByte(), 8, 0);
    const size = vm_calculate_state_size(&prog, prog.len);
    try testing.expect(size > 0);

    var state_buf: [16384]u8 align(8) = [_]u8{0} ** 16384;
    const rc = vm_init_state(&state_buf, &prog, prog.len);
    try testing.expectEqual(@as(u32, 0), rc);

    const meta = types.getSlotMeta(&state_buf, 0);
    const cap: u32 = 16; // nextPowerOf2(8*2)
    try testing.expectEqual(cap, meta.capacity);

    const keys: [*]const u32 = @ptrCast(@alignCast(state_buf[meta.offset..]));
    for (0..cap) |i| {
        try testing.expectEqual(EMPTY_KEY, keys[i]);
    }
}

// ---------------------------------------------------------------------------
// 12. vm_calculate_grown_state_size — HASHMAP cap doubles
// ---------------------------------------------------------------------------

test "vm_calculate_grown_state_size — HASHMAP slot cap doubles" {
    const prog = buildSingleSlotProgram(0x00, 8, 0);
    const size = vm_calculate_state_size(&prog, prog.len);
    try testing.expect(size > 0);

    var state_buf: [16384]u8 align(8) = [_]u8{0} ** 16384;
    const rc = vm_init_state(&state_buf, &prog, prog.len);
    try testing.expectEqual(@as(u32, 0), rc);

    // Grow slot 0: old_cap=16 → new_cap=nextPowerOf2(16*2)=32
    const grown_size = vm_calculate_grown_state_size(&state_buf, &prog, prog.len, 0);
    try testing.expect(grown_size > size);

    const new_cap: u32 = 32; // nextPowerOf2(16*2)
    var expected: u32 = align8(STATE_HEADER_SIZE + 1 * SLOT_META_SIZE);
    // HASHMAP with timestamps: keys(cap*4) + values(cap*4) + timestamps(cap*8)
    expected += new_cap * 4 + new_cap * 4 + new_cap * 8;
    expected = align8(expected);
    try testing.expectEqual(expected, grown_size);
}
