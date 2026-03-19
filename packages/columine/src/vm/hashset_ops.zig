// =============================================================================
// HashSet Batch Operations — using FlatHashTable(void)
// =============================================================================
//
// HashSet is FlatHashTable(void): keys only, no per-entry value.
// Insert = add key if absent. Remove = tombstone key.
// Supports TTL tracking and undo logging.
//
// BITMAP fallback: if the slot type is BITMAP, delegates to bitmap ops in vm.zig.

const std = @import("std");
const types = @import("types.zig");
const ht = @import("hash_table.zig");

const EMPTY_KEY = types.EMPTY_KEY;
const TOMBSTONE = types.TOMBSTONE;
const SlotMeta = types.SlotMeta;
const ErrorCode = types.ErrorCode;
const ChangeFlag = types.ChangeFlag;
const setChangeFlag = types.setChangeFlag;

const vm = @import("vm.zig");
const HashSet = ht.HashSet;

fn bindSlotSet(state_base: [*]u8, meta: SlotMeta) HashSet {
    return HashSet.bindExternal(state_base + meta.offset, meta.capacity, meta.size_ptr);
}

/// Batch insert elements into a HashSet slot.
pub fn batchSetInsert(
    comptime delta_mode: bool,
    state_base: [*]u8,
    meta: SlotMeta,
    slot_idx: u8,
    elem_col: [*]const u32,
    ts_col: ?[*]const f64,
    batch_len: u32,
) ErrorCode {
    // BITMAP fallback
    if (meta.slotType() == .BITMAP) {
        return vm.batchBitmapAdd(delta_mode, state_base, meta, slot_idx, elem_col, ts_col, batch_len);
    }

    const tbl = bindSlotSet(state_base, meta);
    var local_size = tbl.size();
    const max_load = tbl.maxLoad();
    var had_insert = false;

    var i: u32 = 0;
    while (i < batch_len) : (i += 1) {
        const elem = elem_col[i];
        const ts = if (meta.hasTTL()) ts_col.?[i] else 0;

        const probe = tbl.findInsert(elem) orelse continue; // skip EMPTY_KEY/TOMBSTONE

        if (!probe.found) {
            if (local_size >= max_load) {
                tbl.size_ptr.* = local_size;
                if (had_insert) setChangeFlag(meta, ChangeFlag.INSERTED);
                return .CAPACITY_EXCEEDED;
            }

            if (vm.g_undo_enabled) {
                tbl.size_ptr.* = local_size;
                vm.appendMutation(
                    delta_mode,
                    .{ .op = .SET_INSERT, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = elem, .prev_value = 0, .aux = 0 },
                    .{ .op = .SET_DELETE, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = elem, .prev_value = 0, .aux = 0 },
                );
            }

            tbl.keys[probe.pos] = elem;
            local_size += 1;
            had_insert = true;

            if (meta.hasTTL()) {
                const ttl_result = vm.insertWithTTL(state_base, meta, elem, ts);
                if (ttl_result != .OK) {
                    tbl.size_ptr.* = local_size;
                    if (had_insert) setChangeFlag(meta, ChangeFlag.INSERTED);
                    return ttl_result;
                }
            }
            continue;
        }

        // Already present — just refresh TTL if applicable
        if (meta.hasTTL()) {
            const ttl_result = vm.insertWithTTL(state_base, meta, elem, ts);
            if (ttl_result != .OK) {
                tbl.size_ptr.* = local_size;
                if (had_insert) setChangeFlag(meta, ChangeFlag.INSERTED);
                return ttl_result;
            }
        }
    }

    tbl.size_ptr.* = local_size;
    if (had_insert) setChangeFlag(meta, ChangeFlag.INSERTED);
    return .OK;
}

/// Batch remove elements from a HashSet slot.
pub fn batchSetRemove(
    comptime delta_mode: bool,
    state_base: [*]u8,
    meta: SlotMeta,
    slot_idx: u8,
    elem_col: [*]const u32,
    batch_len: u32,
) void {
    if (meta.slotType() == .BITMAP) {
        vm.batchBitmapRemove(delta_mode, state_base, meta, slot_idx, elem_col, batch_len);
        return;
    }

    const tbl = bindSlotSet(state_base, meta);
    var had_remove = false;

    var i: u32 = 0;
    while (i < batch_len) : (i += 1) {
        const elem = elem_col[i];
        const pos = tbl.find(elem) orelse continue;

        if (vm.g_undo_enabled) {
            // For undo, capture TTL timestamp if present
            var prev_ts_bits: u64 = 0;
            if (meta.hasTTL()) {
                const eviction_index = vm.getEvictionIndex(state_base, meta);
                const eviction_size = meta.eviction_index_size_ptr.*;
                if (vm.findLatestEvictionTimestampForKey(eviction_index, eviction_size, elem)) |prev_ts| {
                    prev_ts_bits = @bitCast(prev_ts);
                }
            }
            vm.appendMutation(
                delta_mode,
                .{ .op = .SET_DELETE, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = elem, .prev_value = 0, .aux = prev_ts_bits },
                .{ .op = .SET_INSERT, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = elem, .prev_value = 0, .aux = 0 },
            );
        }

        tbl.keys[pos] = TOMBSTONE;
        tbl.size_ptr.* -= 1;
        had_remove = true;
    }

    if (had_remove) setChangeFlag(meta, ChangeFlag.REMOVED);
}

// =============================================================================
// Single-element operations (FOR_EACH body dispatch)
// =============================================================================

pub fn singleSetInsert(
    comptime delta_mode: bool,
    state_base: [*]u8,
    meta: SlotMeta,
    slot_idx: u8,
    elem: u32,
    ts: f64,
) ErrorCode {
    if (meta.slotType() == .BITMAP) {
        var elem_arr = [_]u32{elem};
        var ts_arr = [_]f64{ts};
        const ts_ptr: ?[*]const f64 = if (meta.hasTTL()) &ts_arr else null;
        return vm.batchBitmapAdd(delta_mode, state_base, meta, slot_idx, &elem_arr, ts_ptr, 1);
    }

    const tbl = bindSlotSet(state_base, meta);
    const probe = tbl.findInsert(elem) orelse return .OK;

    if (!probe.found) {
        if (tbl.size() >= tbl.maxLoad()) return .CAPACITY_EXCEEDED;

        if (vm.g_undo_enabled) {
            vm.appendMutation(
                delta_mode,
                .{ .op = .SET_INSERT, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = elem, .prev_value = 0, .aux = 0 },
                .{ .op = .SET_DELETE, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = elem, .prev_value = 0, .aux = 0 },
            );
        }

        tbl.keys[probe.pos] = elem;
        tbl.size_ptr.* += 1;
        setChangeFlag(meta, ChangeFlag.INSERTED);

        if (meta.hasTTL()) {
            const ttl_result = vm.insertWithTTL(state_base, meta, elem, ts);
            if (ttl_result != .OK) return ttl_result;
        }
        return .OK;
    }

    // Already present — refresh TTL
    if (meta.hasTTL()) {
        const ttl_result = vm.insertWithTTL(state_base, meta, elem, ts);
        if (ttl_result != .OK) return ttl_result;
    }
    return .OK;
}

pub fn singleSetRemove(
    comptime delta_mode: bool,
    state_base: [*]u8,
    meta: SlotMeta,
    slot_idx: u8,
    elem: u32,
) void {
    if (meta.slotType() == .BITMAP) {
        var elem_arr = [_]u32{elem};
        vm.batchBitmapRemove(delta_mode, state_base, meta, slot_idx, &elem_arr, 1);
        return;
    }

    const tbl = bindSlotSet(state_base, meta);
    const pos = tbl.find(elem) orelse return;

    if (vm.g_undo_enabled) {
        vm.appendMutation(
            delta_mode,
            .{ .op = .SET_DELETE, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = elem, .prev_value = 0, .aux = 0 },
            .{ .op = .SET_INSERT, .slot = slot_idx, ._pad1 = 0, ._pad2 = 0, .key = elem, .prev_value = 0, .aux = 0 },
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

test "batchSetInsert — insert and dedup" {
    var state: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const cap: u32 = 16;
    const slot_offset: u32 = types.STATE_HEADER_SIZE + types.SLOT_META_SIZE;
    const meta_base = types.STATE_HEADER_SIZE;

    std.mem.writeInt(u32, state[meta_base..][0..4], slot_offset, .little);
    std.mem.writeInt(u32, state[meta_base + 4 ..][0..4], cap, .little);
    std.mem.writeInt(u32, state[meta_base + 8 ..][0..4], 0, .little);
    state[meta_base + 12] = 0x01; // HASHSET type_flags

    const keys: [*]u32 = @ptrCast(@alignCast(&state[slot_offset]));
    for (0..cap) |j| keys[j] = EMPTY_KEY;

    const meta = types.getSlotMeta(&state, 0);
    const tbl = bindSlotSet(&state, meta);

    var elems = [_]u32{ 10, 20, 10, 30, 20 }; // 10 and 20 are duplicates
    const result = batchSetInsert(false, &state, meta, 0, &elems, null, 5);
    try testing.expectEqual(ErrorCode.OK, result);
    try testing.expectEqual(@as(u32, 3), tbl.size()); // 3 unique elements
    try testing.expect(tbl.contains(10));
    try testing.expect(tbl.contains(20));
    try testing.expect(tbl.contains(30));
    try testing.expect(!tbl.contains(40));
}

test "batchSetRemove — remove existing" {
    var state: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const cap: u32 = 16;
    const slot_offset: u32 = types.STATE_HEADER_SIZE + types.SLOT_META_SIZE;
    const meta_base = types.STATE_HEADER_SIZE;

    std.mem.writeInt(u32, state[meta_base..][0..4], slot_offset, .little);
    std.mem.writeInt(u32, state[meta_base + 4 ..][0..4], cap, .little);
    std.mem.writeInt(u32, state[meta_base + 8 ..][0..4], 0, .little);
    state[meta_base + 12] = 0x01;

    const keys: [*]u32 = @ptrCast(@alignCast(&state[slot_offset]));
    for (0..cap) |j| keys[j] = EMPTY_KEY;

    const meta = types.getSlotMeta(&state, 0);
    const tbl = bindSlotSet(&state, meta);

    // Insert 3 elements
    var elems = [_]u32{ 10, 20, 30 };
    _ = batchSetInsert(false, &state, meta, 0, &elems, null, 3);
    try testing.expectEqual(@as(u32, 3), tbl.size());

    // Remove 20
    var rm = [_]u32{20};
    batchSetRemove(false, &state, meta, 0, &rm, 1);
    try testing.expectEqual(@as(u32, 2), tbl.size());
    try testing.expect(!tbl.contains(20));
    try testing.expect(tbl.contains(10));
    try testing.expect(tbl.contains(30));
}

test "singleSetInsert — CAPACITY_EXCEEDED at load factor" {
    var state: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const cap: u32 = 16;
    const slot_offset: u32 = types.STATE_HEADER_SIZE + types.SLOT_META_SIZE;
    const meta_base = types.STATE_HEADER_SIZE;

    std.mem.writeInt(u32, state[meta_base..][0..4], slot_offset, .little);
    std.mem.writeInt(u32, state[meta_base + 4 ..][0..4], cap, .little);
    std.mem.writeInt(u32, state[meta_base + 8 ..][0..4], 0, .little);
    state[meta_base + 12] = 0x01;

    const keys: [*]u32 = @ptrCast(@alignCast(&state[slot_offset]));
    for (0..cap) |j| keys[j] = EMPTY_KEY;

    const meta = types.getSlotMeta(&state, 0);

    // Insert 11 elements (70% of 16)
    var i: u32 = 1;
    while (i <= 11) : (i += 1) {
        try testing.expectEqual(ErrorCode.OK, singleSetInsert(false, &state, meta, 0, i, 0));
    }
    try testing.expectEqual(@as(u32, 11), meta.size_ptr.*);

    // 12th should fail
    try testing.expectEqual(ErrorCode.CAPACITY_EXCEEDED, singleSetInsert(false, &state, meta, 0, 12, 0));
}
