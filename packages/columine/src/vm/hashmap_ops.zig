// =============================================================================
// HashMap Batch Operations — generic over upsert strategy
// =============================================================================
//
// All HashMap batch operations follow the same pattern:
//   1. Probe for key (find existing or insertion slot)
//   2. If new: capacity check → undo log → write key+value → size++ → TTL
//   3. If existing: strategy check → undo log → write value → TTL
//   4. Flush size + change flags
//
// The only varying part is the strategy: should we update an existing entry?
//   - Latest: update if new timestamp > existing timestamp
//   - First: never update existing entries
//   - Last: always update existing entries
//   - Max: update if new comparison value > existing
//   - Min: update if new comparison value < existing
//
// This module uses FlatHashTable for typed probing, eliminating raw @ptrCast.

const std = @import("std");
const types = @import("types.zig");
const ht = @import("hash_table.zig");

const EMPTY_KEY = types.EMPTY_KEY;
const TOMBSTONE = types.TOMBSTONE;
const SlotMeta = types.SlotMeta;
const ErrorCode = types.ErrorCode;
const ChangeFlag = types.ChangeFlag;
const setChangeFlag = types.setChangeFlag;

// Import undo log infrastructure from vm.zig
// These are module-level mutable globals — we access them via @import("vm.zig")
const vm = @import("vm.zig");

const HashMap = ht.FlatHashTable(u32);

pub const Strategy = enum {
    latest, // Update if ts > existing ts
    first, // Never update existing
    last, // Always update existing
    max, // Update if cmp > existing cmp
    min, // Update if cmp < existing cmp
};

/// Comparison type for latest/max/min strategies.
/// Determines how the 8-byte comparison column is interpreted.
/// Encoded as 1 byte in the opcode arguments (cmp_type field).
pub const CmpType = enum(u8) {
    /// Compare as unsigned 32-bit integers (string intern IDs, ordinals).
    /// Input column is [*]const u32; stored zero-extended in 8-byte slot.
    u32 = 0,
    /// Compare as 64-bit IEEE 754 floats (numeric fields, timestamps-as-f64).
    /// Current default behavior.
    f64 = 1,
    /// Compare as signed 64-bit integers (bigint timestamps).
    /// Input column is [*]const i64.
    i64 = 2,
};

/// Bind a top-level HashMap slot's keys+values arrays via SlotMeta.
/// The VM's SoA layout: [keys: u32 × cap][values: u32 × cap][timestamps: f64 × cap]
/// We use HashMap (FlatHashTable(u32)) for the keys+values portion.
fn bindSlotMap(state_base: [*]u8, meta: SlotMeta) HashMap {
    return HashMap.bindExternal(state_base + meta.offset, meta.capacity, meta.size_ptr);
}

/// Get the comparison side-array for a slot (after keys + values).
/// Physically 8 bytes per entry; interpretation depends on CmpType.
fn getCmpSlots(state_base: [*]u8, meta: SlotMeta) [*]u64 {
    return @ptrCast(@alignCast(state_base + meta.offset + meta.capacity * 8));
}

/// Legacy accessor — returns the same memory reinterpreted as [*]f64.
/// Used by callers that always deal with f64 timestamps (remove, TTL, undo).
fn getTimestamps(state_base: [*]u8, meta: SlotMeta) [*]f64 {
    return @ptrCast(@alignCast(state_base + meta.offset + meta.capacity * 8));
}

/// Read a comparison value from a raw input column pointer at position `i`.
/// For u32: reads 4 bytes, zero-extends to u64.
/// For f64: reads 8 bytes, bitcasts to u64.
/// For i64: reads 8 bytes, bitcasts to u64.
pub inline fn readCmpValue(cmp_col: [*]const u8, i: u32, cmp_type: CmpType) u64 {
    return switch (cmp_type) {
        .u32 => @as(u64, @as([*]const u32, @ptrCast(@alignCast(cmp_col)))[i]),
        .f64 => @bitCast(@as([*]const f64, @ptrCast(@alignCast(cmp_col)))[i]),
        .i64 => @bitCast(@as([*]const i64, @ptrCast(@alignCast(cmp_col)))[i]),
    };
}

/// Compare two stored comparison values (already in u64 storage format).
/// Returns true if `a > b` using the comparison semantics of cmp_type.
inline fn cmpGt(a: u64, b: u64, cmp_type: CmpType) bool {
    return switch (cmp_type) {
        .u32 => @as(u32, @truncate(a)) > @as(u32, @truncate(b)),
        .f64 => @as(f64, @bitCast(a)) > @as(f64, @bitCast(b)),
        .i64 => @as(i64, @bitCast(a)) > @as(i64, @bitCast(b)),
    };
}

/// Compare two stored comparison values. Returns true if `a < b`.
inline fn cmpLt(a: u64, b: u64, cmp_type: CmpType) bool {
    return switch (cmp_type) {
        .u32 => @as(u32, @truncate(a)) < @as(u32, @truncate(b)),
        .f64 => @as(f64, @bitCast(a)) < @as(f64, @bitCast(b)),
        .i64 => @as(i64, @bitCast(a)) < @as(i64, @bitCast(b)),
    };
}

/// Convert a stored u64 comparison value to f64 for TTL timestamp use.
/// TTL always operates on f64 timestamps regardless of cmp_type.
inline fn cmpToF64(val: u64, cmp_type: CmpType) f64 {
    return switch (cmp_type) {
        .u32 => @floatFromInt(@as(u32, @truncate(val))),
        .f64 => @bitCast(val),
        .i64 => @floatFromInt(@as(i64, @bitCast(val))),
    };
}

/// Generic batch upsert for all strategies. Comptime-parameterized by strategy and delta_mode.
///
/// `cmp_col_raw` is the raw byte pointer to the comparison column. Interpretation
/// depends on `cmp_type`: u32 columns have 4-byte stride, f64/i64 have 8-byte stride.
/// Pass null for strategies that don't need comparison (first, last without TTL).
pub fn batchMapUpsert(
    comptime strategy: Strategy,
    comptime delta_mode: bool,
    state_base: [*]u8,
    meta: SlotMeta,
    slot_idx: u8,
    key_col: [*]const u32,
    val_col: [*]const u32,
    cmp_col_raw: ?[*]const u8, // raw bytes; typed via cmp_type
    cmp_type: CmpType,
    batch_len: u32,
) ErrorCode {
    // Latest/max/min require timestamp storage; first/last don't
    const needs_timestamps = comptime (strategy == .latest or strategy == .max or strategy == .min);
    if (needs_timestamps and !meta.hasHashMapTimestampStorage()) return .INVALID_PROGRAM;

    const tbl = bindSlotMap(state_base, meta);
    const cmp_slots: [*]u64 = if (needs_timestamps) getCmpSlots(state_base, meta) else undefined;

    var local_size = tbl.size();
    const max_load = tbl.maxLoad();
    var had_insert = false;
    var had_update = false;

    var i: u32 = 0;
    while (i < batch_len) : (i += 1) {
        const key = key_col[i];
        const val = val_col[i];

        const probe = tbl.findInsert(key) orelse continue; // skip EMPTY_KEY/TOMBSTONE keys

        // Read typed comparison value from input column
        const new_cmp: u64 = if (needs_timestamps) readCmpValue(cmp_col_raw.?, i, cmp_type) else 0;

        if (!probe.found) {
            // New key — check capacity
            if (local_size >= max_load) {
                tbl.size_ptr.* = local_size;
                if (had_insert) setChangeFlag(meta, ChangeFlag.INSERTED);
                if (had_update) setChangeFlag(meta, ChangeFlag.UPDATED);
                return .CAPACITY_EXCEEDED;
            }

            // Undo log
            if (vm.g_undo_enabled) {
                tbl.size_ptr.* = local_size;
                vm.appendMutation(
                    delta_mode,
                    .{ .op = .MAP_INSERT, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = key, .prev_value = 0, .aux = 0 },
                    .{ .op = .MAP_DELETE, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = key, .prev_value = val, .aux = if (needs_timestamps) @bitCast(new_cmp) else 0 },
                );
            }

            tbl.keys[probe.pos] = key;
            tbl.entries[probe.pos] = val;
            if (needs_timestamps) cmp_slots[probe.pos] = new_cmp;
            local_size += 1;
            had_insert = true;

            if (meta.hasTTL()) {
                const ts = if (cmp_col_raw != null) cmpToF64(new_cmp, cmp_type) else 0;
                const ttl_result = vm.insertWithTTL(state_base, meta, key, ts);
                if (ttl_result != .OK) {
                    tbl.size_ptr.* = local_size;
                    if (had_insert) setChangeFlag(meta, ChangeFlag.INSERTED);
                    if (had_update) setChangeFlag(meta, ChangeFlag.UPDATED);
                    return ttl_result;
                }
            }
            continue;
        }

        // Existing key — apply strategy with typed comparison
        const pos = probe.pos;
        const should_update = switch (strategy) {
            .first => false, // never update
            .last => true, // always update
            .latest => cmpGt(new_cmp, cmp_slots[pos], cmp_type),
            .max => cmpGt(new_cmp, cmp_slots[pos], cmp_type),
            .min => cmpLt(new_cmp, cmp_slots[pos], cmp_type),
        };

        if (should_update) {
            if (vm.g_undo_enabled) {
                tbl.size_ptr.* = local_size;
                vm.appendMutation(
                    delta_mode,
                    .{ .op = .MAP_UPDATE, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = key, .prev_value = tbl.entries[pos], .aux = if (needs_timestamps) @bitCast(cmp_slots[pos]) else 0 },
                    .{ .op = .MAP_UPDATE, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = key, .prev_value = val, .aux = if (needs_timestamps) @bitCast(new_cmp) else 0 },
                );
            }
            tbl.entries[pos] = val;
            if (needs_timestamps) cmp_slots[pos] = new_cmp;
            had_update = true;

            if (meta.hasTTL()) {
                const ts = if (cmp_col_raw != null) cmpToF64(new_cmp, cmp_type) else 0;
                const ttl_result = vm.insertWithTTL(state_base, meta, key, ts);
                if (ttl_result != .OK) {
                    tbl.size_ptr.* = local_size;
                    if (had_insert) setChangeFlag(meta, ChangeFlag.INSERTED);
                    if (had_update) setChangeFlag(meta, ChangeFlag.UPDATED);
                    return ttl_result;
                }
            }
        }
    }

    tbl.size_ptr.* = local_size;
    if (had_insert) setChangeFlag(meta, ChangeFlag.INSERTED);
    if (had_update) setChangeFlag(meta, ChangeFlag.UPDATED);
    return .OK;
}

/// Batch remove: tombstone keys in the hashmap.
pub fn batchMapRemove(
    comptime delta_mode: bool,
    state_base: [*]u8,
    meta: SlotMeta,
    slot_idx: u8,
    key_col: [*]const u32,
    batch_len: u32,
) void {
    const tbl = bindSlotMap(state_base, meta);
    const has_ts = meta.hasHashMapTimestampStorage();
    const timestamps: [*]f64 = if (has_ts) getTimestamps(state_base, meta) else undefined;
    var had_remove = false;

    var i: u32 = 0;
    while (i < batch_len) : (i += 1) {
        const key = key_col[i];
        const pos = tbl.find(key) orelse continue;

        if (vm.g_undo_enabled) {
            vm.appendMutation(
                delta_mode,
                .{ .op = .MAP_DELETE, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = key, .prev_value = tbl.entries[pos], .aux = if (has_ts) @bitCast(timestamps[pos]) else 0 },
                .{ .op = .MAP_INSERT, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = key, .prev_value = 0, .aux = 0 },
            );
        }

        tbl.keys[pos] = TOMBSTONE;
        tbl.size_ptr.* -= 1;
        had_remove = true;
    }

    if (had_remove) setChangeFlag(meta, ChangeFlag.REMOVED);
}

// =============================================================================
// Single-element operations (for FOR_EACH body dispatch)
// =============================================================================

/// Single-element upsert — same as batch with batch_len=1 but takes element index.
/// `cmp` is the raw 8-byte comparison value (use readCmpValue or manual bitcast).
pub fn singleMapUpsert(
    comptime strategy: Strategy,
    comptime delta_mode: bool,
    state_base: [*]u8,
    meta: SlotMeta,
    slot_idx: u8,
    key: u32,
    val: u32,
    cmp: u64, // raw comparison value (typed by cmp_type)
    cmp_type: CmpType,
) ErrorCode {
    const needs_timestamps = comptime (strategy == .latest or strategy == .max or strategy == .min);
    if (needs_timestamps and !meta.hasHashMapTimestampStorage()) return .INVALID_PROGRAM;

    const tbl = bindSlotMap(state_base, meta);
    const cmp_slots: [*]u64 = if (needs_timestamps) getCmpSlots(state_base, meta) else undefined;

    const probe = tbl.findInsert(key) orelse return .OK; // skip invalid keys

    if (!probe.found) {
        if (tbl.size() >= tbl.maxLoad()) return .CAPACITY_EXCEEDED;

        if (vm.g_undo_enabled) {
            vm.appendMutation(
                delta_mode,
                .{ .op = .MAP_INSERT, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = key, .prev_value = 0, .aux = 0 },
                .{ .op = .MAP_DELETE, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = key, .prev_value = val, .aux = if (needs_timestamps) @bitCast(cmp) else 0 },
            );
        }

        tbl.keys[probe.pos] = key;
        tbl.entries[probe.pos] = val;
        if (needs_timestamps) cmp_slots[probe.pos] = cmp;
        tbl.size_ptr.* += 1;
        setChangeFlag(meta, ChangeFlag.INSERTED);
        return .OK;
    }

    const should_update = switch (strategy) {
        .first => false,
        .last => true,
        .latest => cmpGt(cmp, cmp_slots[probe.pos], cmp_type),
        .max => cmpGt(cmp, cmp_slots[probe.pos], cmp_type),
        .min => cmpLt(cmp, cmp_slots[probe.pos], cmp_type),
    };

    if (should_update) {
        if (vm.g_undo_enabled) {
            vm.appendMutation(
                delta_mode,
                .{ .op = .MAP_UPDATE, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = key, .prev_value = tbl.entries[probe.pos], .aux = if (needs_timestamps) @bitCast(cmp_slots[probe.pos]) else 0 },
                .{ .op = .MAP_UPDATE, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = key, .prev_value = val, .aux = if (needs_timestamps) @bitCast(cmp) else 0 },
            );
        }
        tbl.entries[probe.pos] = val;
        if (needs_timestamps) cmp_slots[probe.pos] = cmp;
        setChangeFlag(meta, ChangeFlag.UPDATED);
    }

    return .OK;
}

/// Single-element remove.
pub fn singleMapRemove(
    comptime delta_mode: bool,
    state_base: [*]u8,
    meta: SlotMeta,
    slot_idx: u8,
    key: u32,
) void {
    const tbl = bindSlotMap(state_base, meta);
    const pos = tbl.find(key) orelse return;
    const has_ts = meta.hasHashMapTimestampStorage();
    const timestamps: [*]f64 = if (has_ts) getTimestamps(state_base, meta) else undefined;

    if (vm.g_undo_enabled) {
        vm.appendMutation(
            delta_mode,
            .{ .op = .MAP_DELETE, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = key, .prev_value = tbl.entries[pos], .aux = if (has_ts) @bitCast(timestamps[pos]) else 0 },
            .{ .op = .MAP_INSERT, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = key, .prev_value = 0, .aux = 0 },
        );
    }

    tbl.keys[pos] = TOMBSTONE;
    tbl.size_ptr.* -= 1;
    setChangeFlag(meta, ChangeFlag.REMOVED);
}

// =============================================================================
// Tests
// =============================================================================

const testing = std.testing;

test "batchMapUpsert — last strategy inserts and overwrites" {
    // Build a minimal state buffer with a HASHMAP slot
    var state: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const cap: u32 = 16;
    const slot_offset: u32 = types.STATE_HEADER_SIZE + types.SLOT_META_SIZE; // after 1 slot metadata

    // Write slot metadata manually
    const meta_base = types.STATE_HEADER_SIZE;
    std.mem.writeInt(u32, state[meta_base..][0..4], slot_offset, .little); // offset
    std.mem.writeInt(u32, state[meta_base + 4 ..][0..4], cap, .little); // capacity
    std.mem.writeInt(u32, state[meta_base + 8 ..][0..4], 0, .little); // size
    state[meta_base + 12] = 0x00; // type_flags: HASHMAP, no TTL, no_timestamps=false

    // Initialize keys to EMPTY_KEY
    const keys: [*]u32 = @ptrCast(@alignCast(&state[slot_offset]));
    for (0..cap) |j| keys[j] = EMPTY_KEY;

    const meta = types.getSlotMeta(&state, 0);

    // Batch insert 3 entries
    var key_data = [_]u32{ 10, 20, 30 };
    var val_data = [_]u32{ 100, 200, 300 };
    const result = batchMapUpsert(.last, false, &state, meta, 0, &key_data, &val_data, null, .f64, 3);
    try testing.expectEqual(ErrorCode.OK, result);
    try testing.expectEqual(@as(u32, 3), meta.size_ptr.*);

    // Verify values via typed lookup
    const tbl = bindSlotMap(&state, meta);
    try testing.expectEqual(@as(u32, 100), tbl.get(10).?.*);
    try testing.expectEqual(@as(u32, 200), tbl.get(20).?.*);
    try testing.expectEqual(@as(u32, 300), tbl.get(30).?.*);

    // Overwrite key 20
    var key_data2 = [_]u32{20};
    var val_data2 = [_]u32{999};
    _ = batchMapUpsert(.last, false, &state, meta, 0, &key_data2, &val_data2, null, .f64, 1);
    try testing.expectEqual(@as(u32, 999), tbl.get(20).?.*);
    try testing.expectEqual(@as(u32, 3), meta.size_ptr.*); // size unchanged (overwrite)
}

test "batchMapUpsert — first strategy skips existing" {
    var state: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const cap: u32 = 16;
    const slot_offset: u32 = types.STATE_HEADER_SIZE + types.SLOT_META_SIZE;

    const meta_base = types.STATE_HEADER_SIZE;
    std.mem.writeInt(u32, state[meta_base..][0..4], slot_offset, .little);
    std.mem.writeInt(u32, state[meta_base + 4 ..][0..4], cap, .little);
    std.mem.writeInt(u32, state[meta_base + 8 ..][0..4], 0, .little);
    state[meta_base + 12] = 0x00;

    const keys: [*]u32 = @ptrCast(@alignCast(&state[slot_offset]));
    for (0..cap) |j| keys[j] = EMPTY_KEY;

    const meta = types.getSlotMeta(&state, 0);
    const tbl = bindSlotMap(&state, meta);

    // Insert key 10 = 100
    var k1 = [_]u32{10};
    var v1 = [_]u32{100};
    _ = batchMapUpsert(.first, false, &state, meta, 0, &k1, &v1, null, .f64, 1);
    try testing.expectEqual(@as(u32, 100), tbl.get(10).?.*);

    // Try to overwrite — first strategy should keep original
    var v2 = [_]u32{999};
    _ = batchMapUpsert(.first, false, &state, meta, 0, &k1, &v2, null, .f64, 1);
    try testing.expectEqual(@as(u32, 100), tbl.get(10).?.*); // still 100
}

test "batchMapUpsert — latest strategy uses timestamp comparison (f64)" {
    var state: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const cap: u32 = 16;
    const slot_offset: u32 = types.STATE_HEADER_SIZE + types.SLOT_META_SIZE;

    const meta_base = types.STATE_HEADER_SIZE;
    std.mem.writeInt(u32, state[meta_base..][0..4], slot_offset, .little);
    std.mem.writeInt(u32, state[meta_base + 4 ..][0..4], cap, .little);
    std.mem.writeInt(u32, state[meta_base + 8 ..][0..4], 0, .little);
    state[meta_base + 12] = 0x00; // HASHMAP with timestamps

    const keys: [*]u32 = @ptrCast(@alignCast(&state[slot_offset]));
    for (0..cap) |j| keys[j] = EMPTY_KEY;
    // Initialize comparison slots to -inf (as f64 bitcast to u64)
    const cmp: [*]u64 = @ptrCast(@alignCast(&state[slot_offset + cap * 8]));
    for (0..cap) |j| cmp[j] = @bitCast(-std.math.inf(f64));

    const meta = types.getSlotMeta(&state, 0);
    const tbl = bindSlotMap(&state, meta);

    // Insert key 10 with ts=100
    var k = [_]u32{10};
    var v = [_]u32{100};
    var t = [_]f64{100.0};
    _ = batchMapUpsert(.latest, false, &state, meta, 0, &k, &v, @ptrCast(&t), .f64, 1);
    try testing.expectEqual(@as(u32, 100), tbl.get(10).?.*);

    // Try to overwrite with older timestamp — should keep original
    var v2 = [_]u32{999};
    var t2 = [_]f64{50.0};
    _ = batchMapUpsert(.latest, false, &state, meta, 0, &k, &v2, @ptrCast(&t2), .f64, 1);
    try testing.expectEqual(@as(u32, 100), tbl.get(10).?.*); // still 100

    // Overwrite with newer timestamp — should update
    var v3 = [_]u32{200};
    var t3 = [_]f64{200.0};
    _ = batchMapUpsert(.latest, false, &state, meta, 0, &k, &v3, @ptrCast(&t3), .f64, 1);
    try testing.expectEqual(@as(u32, 200), tbl.get(10).?.*); // updated
}

test "batchMapRemove — removes and tombstones" {
    var state: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const cap: u32 = 16;
    const slot_offset: u32 = types.STATE_HEADER_SIZE + types.SLOT_META_SIZE;

    const meta_base = types.STATE_HEADER_SIZE;
    std.mem.writeInt(u32, state[meta_base..][0..4], slot_offset, .little);
    std.mem.writeInt(u32, state[meta_base + 4 ..][0..4], cap, .little);
    std.mem.writeInt(u32, state[meta_base + 8 ..][0..4], 0, .little);
    state[meta_base + 12] = 0x00;

    const keys: [*]u32 = @ptrCast(@alignCast(&state[slot_offset]));
    for (0..cap) |j| keys[j] = EMPTY_KEY;

    const meta = types.getSlotMeta(&state, 0);
    const tbl = bindSlotMap(&state, meta);

    // Insert 3 entries
    var k = [_]u32{ 10, 20, 30 };
    var v = [_]u32{ 100, 200, 300 };
    _ = batchMapUpsert(.last, false, &state, meta, 0, &k, &v, null, .f64, 3);
    try testing.expectEqual(@as(u32, 3), tbl.size());

    // Remove key 20
    var rk = [_]u32{20};
    batchMapRemove(false, &state, meta, 0, &rk, 1);
    try testing.expectEqual(@as(u32, 2), tbl.size());
    try testing.expect(tbl.get(20) == null);
    try testing.expect(tbl.get(10) != null);
    try testing.expect(tbl.get(30) != null);
}

test "batchMapUpsert — CAPACITY_EXCEEDED at 70% load" {
    var state: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const cap: u32 = 16;
    const slot_offset: u32 = types.STATE_HEADER_SIZE + types.SLOT_META_SIZE;

    const meta_base = types.STATE_HEADER_SIZE;
    std.mem.writeInt(u32, state[meta_base..][0..4], slot_offset, .little);
    std.mem.writeInt(u32, state[meta_base + 4 ..][0..4], cap, .little);
    std.mem.writeInt(u32, state[meta_base + 8 ..][0..4], 0, .little);
    state[meta_base + 12] = 0x40; // HASHMAP, no_timestamps=true

    const keys: [*]u32 = @ptrCast(@alignCast(&state[slot_offset]));
    for (0..cap) |j| keys[j] = EMPTY_KEY;

    const meta = types.getSlotMeta(&state, 0);

    // Insert 11 keys (70% of 16 = 11.2, so first 11 fit)
    var i: u32 = 1;
    while (i <= 11) : (i += 1) {
        var k = [_]u32{i};
        var v = [_]u32{i * 10};
        const r = batchMapUpsert(.last, false, &state, meta, 0, &k, &v, null, .f64, 1);
        try testing.expectEqual(ErrorCode.OK, r);
    }
    try testing.expectEqual(@as(u32, 11), meta.size_ptr.*);

    // 12th should trigger CAPACITY_EXCEEDED
    var k12 = [_]u32{12};
    var v12 = [_]u32{120};
    const r12 = batchMapUpsert(.last, false, &state, meta, 0, &k12, &v12, null, .f64, 1);
    try testing.expectEqual(ErrorCode.CAPACITY_EXCEEDED, r12);
}

test "batchMapUpsert — max strategy with u32 comparison (string intern IDs)" {
    var state: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const cap: u32 = 16;
    const slot_offset: u32 = types.STATE_HEADER_SIZE + types.SLOT_META_SIZE;

    const meta_base = types.STATE_HEADER_SIZE;
    std.mem.writeInt(u32, state[meta_base..][0..4], slot_offset, .little);
    std.mem.writeInt(u32, state[meta_base + 4 ..][0..4], cap, .little);
    std.mem.writeInt(u32, state[meta_base + 8 ..][0..4], 0, .little);
    state[meta_base + 12] = 0x00; // HASHMAP with timestamps

    const keys: [*]u32 = @ptrCast(@alignCast(&state[slot_offset]));
    for (0..cap) |j| keys[j] = EMPTY_KEY;
    const cmp: [*]u64 = @ptrCast(@alignCast(&state[slot_offset + cap * 8]));
    for (0..cap) |j| cmp[j] = 0;

    const meta = types.getSlotMeta(&state, 0);
    const tbl = bindSlotMap(&state, meta);

    // Insert key 10 with u32 cmp=100
    var k = [_]u32{10};
    var v = [_]u32{42};
    var c = [_]u32{100};
    _ = batchMapUpsert(.max, false, &state, meta, 0, &k, &v, @ptrCast(&c), .u32, 1);
    try testing.expectEqual(@as(u32, 42), tbl.get(10).?.*);

    // Try with lower u32 cmp=50 — should not update
    var v2 = [_]u32{99};
    var c2 = [_]u32{50};
    _ = batchMapUpsert(.max, false, &state, meta, 0, &k, &v2, @ptrCast(&c2), .u32, 1);
    try testing.expectEqual(@as(u32, 42), tbl.get(10).?.*); // still 42

    // Try with higher u32 cmp=200 — should update
    var v3 = [_]u32{77};
    var c3 = [_]u32{200};
    _ = batchMapUpsert(.max, false, &state, meta, 0, &k, &v3, @ptrCast(&c3), .u32, 1);
    try testing.expectEqual(@as(u32, 77), tbl.get(10).?.*); // updated

    // Verify stored comparison value is correct (u32 zero-extended to u64)
    const pos = tbl.find(10).?;
    try testing.expectEqual(@as(u64, 200), cmp[pos]);
}

test "batchMapUpsert — latest strategy with i64 comparison (bigint timestamps)" {
    var state: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const cap: u32 = 16;
    const slot_offset: u32 = types.STATE_HEADER_SIZE + types.SLOT_META_SIZE;

    const meta_base = types.STATE_HEADER_SIZE;
    std.mem.writeInt(u32, state[meta_base..][0..4], slot_offset, .little);
    std.mem.writeInt(u32, state[meta_base + 4 ..][0..4], cap, .little);
    std.mem.writeInt(u32, state[meta_base + 8 ..][0..4], 0, .little);
    state[meta_base + 12] = 0x00; // HASHMAP with timestamps

    const keys: [*]u32 = @ptrCast(@alignCast(&state[slot_offset]));
    for (0..cap) |j| keys[j] = EMPTY_KEY;
    // Initialize comparison slots to minimum i64
    const cmp: [*]u64 = @ptrCast(@alignCast(&state[slot_offset + cap * 8]));
    for (0..cap) |j| cmp[j] = @bitCast(@as(i64, std.math.minInt(i64)));

    const meta = types.getSlotMeta(&state, 0);
    const tbl = bindSlotMap(&state, meta);

    // Insert key 10 with i64 cmp=1000
    var k = [_]u32{10};
    var v = [_]u32{42};
    var c = [_]i64{1000};
    _ = batchMapUpsert(.latest, false, &state, meta, 0, &k, &v, @ptrCast(&c), .i64, 1);
    try testing.expectEqual(@as(u32, 42), tbl.get(10).?.*);

    // Try with older i64 cmp=500 — should not update
    var v2 = [_]u32{99};
    var c2 = [_]i64{500};
    _ = batchMapUpsert(.latest, false, &state, meta, 0, &k, &v2, @ptrCast(&c2), .i64, 1);
    try testing.expectEqual(@as(u32, 42), tbl.get(10).?.*); // still 42

    // Try with newer i64 cmp=2000 — should update
    var v3 = [_]u32{77};
    var c3 = [_]i64{2000};
    _ = batchMapUpsert(.latest, false, &state, meta, 0, &k, &v3, @ptrCast(&c3), .i64, 1);
    try testing.expectEqual(@as(u32, 77), tbl.get(10).?.*); // updated

    // Verify stored comparison value is correct
    const pos = tbl.find(10).?;
    try testing.expectEqual(@as(i64, 2000), @as(i64, @bitCast(cmp[pos])));
}

test "batchMapUpsert — min strategy with i64 comparison (negative values)" {
    var state: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const cap: u32 = 16;
    const slot_offset: u32 = types.STATE_HEADER_SIZE + types.SLOT_META_SIZE;

    const meta_base = types.STATE_HEADER_SIZE;
    std.mem.writeInt(u32, state[meta_base..][0..4], slot_offset, .little);
    std.mem.writeInt(u32, state[meta_base + 4 ..][0..4], cap, .little);
    std.mem.writeInt(u32, state[meta_base + 8 ..][0..4], 0, .little);
    state[meta_base + 12] = 0x00; // HASHMAP with timestamps

    const keys: [*]u32 = @ptrCast(@alignCast(&state[slot_offset]));
    for (0..cap) |j| keys[j] = EMPTY_KEY;
    // Initialize comparison slots to max i64 (so any real value is smaller)
    const cmp: [*]u64 = @ptrCast(@alignCast(&state[slot_offset + cap * 8]));
    for (0..cap) |j| cmp[j] = @bitCast(@as(i64, std.math.maxInt(i64)));

    const meta = types.getSlotMeta(&state, 0);
    const tbl = bindSlotMap(&state, meta);

    // Insert key 10 with i64 cmp=-100
    var k = [_]u32{10};
    var v = [_]u32{42};
    var c = [_]i64{-100};
    _ = batchMapUpsert(.min, false, &state, meta, 0, &k, &v, @ptrCast(&c), .i64, 1);
    try testing.expectEqual(@as(u32, 42), tbl.get(10).?.*);

    // Try with higher i64 cmp=50 — should not update (min keeps smaller)
    var v2 = [_]u32{99};
    var c2 = [_]i64{50};
    _ = batchMapUpsert(.min, false, &state, meta, 0, &k, &v2, @ptrCast(&c2), .i64, 1);
    try testing.expectEqual(@as(u32, 42), tbl.get(10).?.*); // still 42

    // Try with lower i64 cmp=-200 — should update
    var v3 = [_]u32{77};
    var c3 = [_]i64{-200};
    _ = batchMapUpsert(.min, false, &state, meta, 0, &k, &v3, @ptrCast(&c3), .i64, 1);
    try testing.expectEqual(@as(u32, 77), tbl.get(10).?.*); // updated

    // Verify stored comparison value
    const pos = tbl.find(10).?;
    try testing.expectEqual(@as(i64, -200), @as(i64, @bitCast(cmp[pos])));
}

test "singleMapUpsert — u32 comparison mode" {
    var state: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const cap: u32 = 16;
    const slot_offset: u32 = types.STATE_HEADER_SIZE + types.SLOT_META_SIZE;

    const meta_base = types.STATE_HEADER_SIZE;
    std.mem.writeInt(u32, state[meta_base..][0..4], slot_offset, .little);
    std.mem.writeInt(u32, state[meta_base + 4 ..][0..4], cap, .little);
    std.mem.writeInt(u32, state[meta_base + 8 ..][0..4], 0, .little);
    state[meta_base + 12] = 0x00;

    const keys: [*]u32 = @ptrCast(@alignCast(&state[slot_offset]));
    for (0..cap) |j| keys[j] = EMPTY_KEY;
    const cmp: [*]u64 = @ptrCast(@alignCast(&state[slot_offset + cap * 8]));
    for (0..cap) |j| cmp[j] = 0;

    const meta = types.getSlotMeta(&state, 0);
    const tbl = bindSlotMap(&state, meta);

    // Insert with u32 cmp=10
    const r1 = singleMapUpsert(.max, false, &state, meta, 0, 5, 42, @as(u64, 10), .u32);
    try testing.expectEqual(ErrorCode.OK, r1);
    try testing.expectEqual(@as(u32, 42), tbl.get(5).?.*);

    // Try with lower cmp=5 — should not update
    const r2 = singleMapUpsert(.max, false, &state, meta, 0, 5, 99, @as(u64, 5), .u32);
    try testing.expectEqual(ErrorCode.OK, r2);
    try testing.expectEqual(@as(u32, 42), tbl.get(5).?.*);

    // Update with higher cmp=20
    const r3 = singleMapUpsert(.max, false, &state, meta, 0, 5, 77, @as(u64, 20), .u32);
    try testing.expectEqual(ErrorCode.OK, r3);
    try testing.expectEqual(@as(u32, 77), tbl.get(5).?.*);
}

test "singleMapUpsert — i64 comparison mode" {
    var state: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const cap: u32 = 16;
    const slot_offset: u32 = types.STATE_HEADER_SIZE + types.SLOT_META_SIZE;

    const meta_base = types.STATE_HEADER_SIZE;
    std.mem.writeInt(u32, state[meta_base..][0..4], slot_offset, .little);
    std.mem.writeInt(u32, state[meta_base + 4 ..][0..4], cap, .little);
    std.mem.writeInt(u32, state[meta_base + 8 ..][0..4], 0, .little);
    state[meta_base + 12] = 0x00;

    const keys: [*]u32 = @ptrCast(@alignCast(&state[slot_offset]));
    for (0..cap) |j| keys[j] = EMPTY_KEY;
    const cmp: [*]u64 = @ptrCast(@alignCast(&state[slot_offset + cap * 8]));
    for (0..cap) |j| cmp[j] = @bitCast(@as(i64, std.math.minInt(i64)));

    const meta = types.getSlotMeta(&state, 0);
    const tbl = bindSlotMap(&state, meta);

    // Insert with i64 cmp=-50
    const r1 = singleMapUpsert(.latest, false, &state, meta, 0, 5, 42, @bitCast(@as(i64, -50)), .i64);
    try testing.expectEqual(ErrorCode.OK, r1);
    try testing.expectEqual(@as(u32, 42), tbl.get(5).?.*);

    // Try with older i64 cmp=-100 — should not update
    const r2 = singleMapUpsert(.latest, false, &state, meta, 0, 5, 99, @bitCast(@as(i64, -100)), .i64);
    try testing.expectEqual(ErrorCode.OK, r2);
    try testing.expectEqual(@as(u32, 42), tbl.get(5).?.*);

    // Update with newer i64 cmp=100
    const r3 = singleMapUpsert(.latest, false, &state, meta, 0, 5, 77, @bitCast(@as(i64, 100)), .i64);
    try testing.expectEqual(ErrorCode.OK, r3);
    try testing.expectEqual(@as(u32, 77), tbl.get(5).?.*);
}
