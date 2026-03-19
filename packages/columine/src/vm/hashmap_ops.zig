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

/// Bind a top-level HashMap slot's keys+values arrays via SlotMeta.
/// The VM's SoA layout: [keys: u32 × cap][values: u32 × cap][timestamps: f64 × cap]
/// We use HashMap (FlatHashTable(u32)) for the keys+values portion.
fn bindSlotMap(state_base: [*]u8, meta: SlotMeta) HashMap {
    return HashMap.bindExternal(state_base + meta.offset, meta.capacity, meta.size_ptr);
}

/// Get the timestamps side-array for a slot (after keys + values).
fn getTimestamps(state_base: [*]u8, meta: SlotMeta) [*]f64 {
    return @ptrCast(@alignCast(state_base + meta.offset + meta.capacity * 8));
}

/// Generic batch upsert for all strategies. Comptime-parameterized by strategy and delta_mode.
pub fn batchMapUpsert(
    comptime strategy: Strategy,
    comptime delta_mode: bool,
    state_base: [*]u8,
    meta: SlotMeta,
    slot_idx: u8,
    key_col: [*]const u32,
    val_col: [*]const u32,
    cmp_col: ?[*]const f64, // timestamps for latest, comparison values for max/min
    batch_len: u32,
) ErrorCode {
    // Latest/max/min require timestamp storage; first/last don't
    const needs_timestamps = comptime (strategy == .latest or strategy == .max or strategy == .min);
    if (needs_timestamps and !meta.hasHashMapTimestampStorage()) return .INVALID_PROGRAM;

    const tbl = bindSlotMap(state_base, meta);
    const timestamps: [*]f64 = if (needs_timestamps) getTimestamps(state_base, meta) else undefined;

    var local_size = tbl.size();
    const max_load = tbl.maxLoad();
    var had_insert = false;
    var had_update = false;

    var i: u32 = 0;
    while (i < batch_len) : (i += 1) {
        const key = key_col[i];
        const val = val_col[i];

        const probe = tbl.findInsert(key) orelse continue; // skip EMPTY_KEY/TOMBSTONE keys

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
                    .{ .op = .MAP_DELETE, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = key, .prev_value = val, .aux = if (needs_timestamps) @bitCast(cmp_col.?[i]) else 0 },
                );
            }

            tbl.keys[probe.pos] = key;
            tbl.entries[probe.pos] = val;
            if (needs_timestamps) timestamps[probe.pos] = cmp_col.?[i];
            local_size += 1;
            had_insert = true;

            if (meta.hasTTL()) {
                const ts = if (cmp_col) |c| c[i] else 0;
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

        // Existing key — apply strategy
        const pos = probe.pos;
        const should_update = switch (strategy) {
            .first => false, // never update
            .last => true, // always update
            .latest => cmp_col.?[i] > timestamps[pos],
            .max => cmp_col.?[i] > timestamps[pos],
            .min => cmp_col.?[i] < timestamps[pos],
        };

        if (should_update) {
            if (vm.g_undo_enabled) {
                tbl.size_ptr.* = local_size;
                vm.appendMutation(
                    delta_mode,
                    .{ .op = .MAP_UPDATE, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = key, .prev_value = tbl.entries[pos], .aux = if (needs_timestamps) @bitCast(timestamps[pos]) else 0 },
                    .{ .op = .MAP_UPDATE, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = key, .prev_value = val, .aux = if (needs_timestamps) @bitCast(cmp_col.?[i]) else 0 },
                );
            }
            tbl.entries[pos] = val;
            if (needs_timestamps) timestamps[pos] = cmp_col.?[i];
            had_update = true;

            if (meta.hasTTL()) {
                const ts = if (cmp_col) |c| c[i] else 0;
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
pub fn singleMapUpsert(
    comptime strategy: Strategy,
    comptime delta_mode: bool,
    state_base: [*]u8,
    meta: SlotMeta,
    slot_idx: u8,
    key: u32,
    val: u32,
    cmp: f64, // timestamp or comparison value
) ErrorCode {
    const needs_timestamps = comptime (strategy == .latest or strategy == .max or strategy == .min);
    if (needs_timestamps and !meta.hasHashMapTimestampStorage()) return .INVALID_PROGRAM;

    const tbl = bindSlotMap(state_base, meta);
    const timestamps: [*]f64 = if (needs_timestamps) getTimestamps(state_base, meta) else undefined;

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
        if (needs_timestamps) timestamps[probe.pos] = cmp;
        tbl.size_ptr.* += 1;
        setChangeFlag(meta, ChangeFlag.INSERTED);
        return .OK;
    }

    const should_update = switch (strategy) {
        .first => false,
        .last => true,
        .latest => cmp > timestamps[probe.pos],
        .max => cmp > timestamps[probe.pos],
        .min => cmp < timestamps[probe.pos],
    };

    if (should_update) {
        if (vm.g_undo_enabled) {
            vm.appendMutation(
                delta_mode,
                .{ .op = .MAP_UPDATE, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = key, .prev_value = tbl.entries[probe.pos], .aux = if (needs_timestamps) @bitCast(timestamps[probe.pos]) else 0 },
                .{ .op = .MAP_UPDATE, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = key, .prev_value = val, .aux = if (needs_timestamps) @bitCast(cmp) else 0 },
            );
        }
        tbl.entries[probe.pos] = val;
        if (needs_timestamps) timestamps[probe.pos] = cmp;
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
    const result = batchMapUpsert(.last, false, &state, meta, 0, &key_data, &val_data, null, 3);
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
    _ = batchMapUpsert(.last, false, &state, meta, 0, &key_data2, &val_data2, null, 1);
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
    _ = batchMapUpsert(.first, false, &state, meta, 0, &k1, &v1, null, 1);
    try testing.expectEqual(@as(u32, 100), tbl.get(10).?.*);

    // Try to overwrite — first strategy should keep original
    var v2 = [_]u32{999};
    _ = batchMapUpsert(.first, false, &state, meta, 0, &k1, &v2, null, 1);
    try testing.expectEqual(@as(u32, 100), tbl.get(10).?.*); // still 100
}

test "batchMapUpsert — latest strategy uses timestamp comparison" {
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
    // Initialize timestamps to -inf
    const ts: [*]f64 = @ptrCast(@alignCast(&state[slot_offset + cap * 8]));
    for (0..cap) |j| ts[j] = -std.math.inf(f64);

    const meta = types.getSlotMeta(&state, 0);
    const tbl = bindSlotMap(&state, meta);

    // Insert key 10 with ts=100
    var k = [_]u32{10};
    var v = [_]u32{100};
    var t = [_]f64{100.0};
    _ = batchMapUpsert(.latest, false, &state, meta, 0, &k, &v, &t, 1);
    try testing.expectEqual(@as(u32, 100), tbl.get(10).?.*);

    // Try to overwrite with older timestamp — should keep original
    var v2 = [_]u32{999};
    var t2 = [_]f64{50.0};
    _ = batchMapUpsert(.latest, false, &state, meta, 0, &k, &v2, &t2, 1);
    try testing.expectEqual(@as(u32, 100), tbl.get(10).?.*); // still 100

    // Overwrite with newer timestamp — should update
    var v3 = [_]u32{200};
    var t3 = [_]f64{200.0};
    _ = batchMapUpsert(.latest, false, &state, meta, 0, &k, &v3, &t3, 1);
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
    _ = batchMapUpsert(.last, false, &state, meta, 0, &k, &v, null, 3);
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
        const r = batchMapUpsert(.last, false, &state, meta, 0, &k, &v, null, 1);
        try testing.expectEqual(ErrorCode.OK, r);
    }
    try testing.expectEqual(@as(u32, 11), meta.size_ptr.*);

    // 12th should trigger CAPACITY_EXCEEDED
    var k12 = [_]u32{12};
    var v12 = [_]u32{120};
    const r12 = batchMapUpsert(.last, false, &state, meta, 0, &k12, &v12, null, 1);
    try testing.expectEqual(ErrorCode.CAPACITY_EXCEEDED, r12);
}
