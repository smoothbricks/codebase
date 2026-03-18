// =============================================================================
// Undo Log — Typed rollback helpers via FlatHashTable
// =============================================================================
//
// Provides typed rollback functions that use FlatHashTable instead of raw
// pointer arithmetic. Called by rollbackEntry in vm.zig.
//
// Bitmap and TTL rollback remain in vm.zig (depend on roaring + eviction index).

const std = @import("std");
const types = @import("types.zig");
const ht = @import("hash_table.zig");

const TOMBSTONE = types.TOMBSTONE;
const SlotMeta = types.SlotMeta;
const HashMap = ht.FlatHashTable(u32);
const HashSet = ht.HashSet;

/// Timestamp side-array (after keys + values in SoA layout).
fn getTimestamps(state_base: [*]u8, meta: SlotMeta) [*]f64 {
    return @ptrCast(@alignCast(state_base + meta.offset + meta.capacity * 8));
}

// =============================================================================
// HashMap rollback
// =============================================================================

/// Rollback MAP_INSERT: tombstone key, decrement size.
pub fn rollbackMapInsert(state_base: [*]u8, meta: SlotMeta, key: u32) bool {
    const tbl = HashMap.bindSlot(state_base, meta);
    const pos = tbl.find(key) orelse return false;
    tbl.keys[pos] = TOMBSTONE;
    tbl.size_ptr.* -= 1;
    return true;
}

/// Rollback MAP_UPDATE: restore previous value + optional timestamp.
pub fn rollbackMapUpdate(state_base: [*]u8, meta: SlotMeta, key: u32, prev_value: u32, prev_ts_bits: u64) bool {
    const tbl = HashMap.bindSlot(state_base, meta);
    const pos = tbl.find(key) orelse return false;
    tbl.entries[pos] = prev_value;
    if (meta.hasHashMapTimestampStorage()) {
        getTimestamps(state_base, meta)[pos] = @bitCast(prev_ts_bits);
    }
    return true;
}

/// Rollback MAP_DELETE: restore key + value + optional timestamp, increment size.
pub fn rollbackMapDelete(state_base: [*]u8, meta: SlotMeta, key: u32, prev_value: u32, prev_ts_bits: u64) bool {
    const tbl = HashMap.bindSlot(state_base, meta);
    const probe = tbl.findInsert(key) orelse return false;
    if (probe.found) return false;
    tbl.keys[probe.pos] = key;
    tbl.entries[probe.pos] = prev_value;
    if (meta.hasHashMapTimestampStorage()) {
        getTimestamps(state_base, meta)[probe.pos] = @bitCast(prev_ts_bits);
    }
    tbl.size_ptr.* += 1;
    return true;
}

// =============================================================================
// Aggregate rollback
// =============================================================================

/// Rollback AGG_UPDATE: restore previous value (f64/i64) and count.
/// Layout: [value: u64 @ offset] [count: u64 @ offset+8] — 16-byte SUM/MIN/MAX/AVG slot.
/// prev_value_bits holds the previous count (truncated to u32, extended back to u64).
/// prev_aux holds the previous value bits (f64/i64 bitcast to u64).
pub fn rollbackAggUpdate(state_base: [*]u8, meta: SlotMeta, prev_count: u32, prev_val_bits: u64) void {
    const val_ptr: *u64 = @ptrCast(@alignCast(state_base + meta.offset));
    const count_ptr: *u64 = @ptrCast(@alignCast(state_base + meta.offset + 8));
    val_ptr.* = prev_val_bits;
    count_ptr.* = prev_count;
}

/// Rollback COUNT_UPDATE: restore previous count.
/// Layout: [count: u64 @ offset] — 8-byte COUNT-only slot.
pub fn rollbackCountUpdate(state_base: [*]u8, meta: SlotMeta, prev_count: u32) void {
    const count_ptr: *u64 = @ptrCast(@alignCast(state_base + meta.offset));
    count_ptr.* = prev_count;
}

// =============================================================================
// HashSet rollback
// =============================================================================

/// Rollback SET_INSERT: tombstone element, decrement size.
pub fn rollbackSetInsert(state_base: [*]u8, meta: SlotMeta, elem: u32) bool {
    const tbl = HashSet.bindSlot(state_base, meta);
    const pos = tbl.find(elem) orelse return false;
    tbl.keys[pos] = TOMBSTONE;
    tbl.size_ptr.* -= 1;
    return true;
}

/// Rollback SET_DELETE: restore element, increment size.
pub fn rollbackSetDelete(state_base: [*]u8, meta: SlotMeta, elem: u32) bool {
    const tbl = HashSet.bindSlot(state_base, meta);
    const probe = tbl.findInsert(elem) orelse return false;
    if (probe.found) return false;
    tbl.keys[probe.pos] = elem;
    tbl.size_ptr.* += 1;
    return true;
}

// =============================================================================
// Tests
// =============================================================================

const testing = std.testing;
const EMPTY_KEY = types.EMPTY_KEY;

test "rollbackMapInsert — tombstones key and decrements size" {
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
    const tbl = HashMap.bindSlot(&state, meta);

    _ = tbl.upsert(42, 100);
    try testing.expectEqual(@as(u32, 1), tbl.size());

    try testing.expect(rollbackMapInsert(&state, meta, 42));
    try testing.expectEqual(@as(u32, 0), tbl.size());
    try testing.expect(!tbl.contains(42));
}

test "rollbackMapUpdate — restores previous value" {
    var state: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const cap: u32 = 16;
    const slot_offset: u32 = types.STATE_HEADER_SIZE + types.SLOT_META_SIZE;
    const meta_base = types.STATE_HEADER_SIZE;

    std.mem.writeInt(u32, state[meta_base..][0..4], slot_offset, .little);
    std.mem.writeInt(u32, state[meta_base + 4 ..][0..4], cap, .little);
    std.mem.writeInt(u32, state[meta_base + 8 ..][0..4], 0, .little);
    state[meta_base + 12] = 0x40;

    const keys: [*]u32 = @ptrCast(@alignCast(&state[slot_offset]));
    for (0..cap) |j| keys[j] = EMPTY_KEY;

    const meta = types.getSlotMeta(&state, 0);
    const tbl = HashMap.bindSlot(&state, meta);

    _ = tbl.upsert(42, 100);
    _ = tbl.upsert(42, 200);
    try testing.expectEqual(@as(u32, 200), tbl.get(42).?.*);

    try testing.expect(rollbackMapUpdate(&state, meta, 42, 100, 0));
    try testing.expectEqual(@as(u32, 100), tbl.get(42).?.*);
}

test "rollbackMapDelete — restores key+value and increments size" {
    var state: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const cap: u32 = 16;
    const slot_offset: u32 = types.STATE_HEADER_SIZE + types.SLOT_META_SIZE;
    const meta_base = types.STATE_HEADER_SIZE;

    std.mem.writeInt(u32, state[meta_base..][0..4], slot_offset, .little);
    std.mem.writeInt(u32, state[meta_base + 4 ..][0..4], cap, .little);
    std.mem.writeInt(u32, state[meta_base + 8 ..][0..4], 0, .little);
    state[meta_base + 12] = 0x40;

    const keys: [*]u32 = @ptrCast(@alignCast(&state[slot_offset]));
    for (0..cap) |j| keys[j] = EMPTY_KEY;

    const meta = types.getSlotMeta(&state, 0);
    const tbl = HashMap.bindSlot(&state, meta);

    // Insert then manually tombstone (simulating what the VM does on delete)
    _ = tbl.upsert(42, 100);
    const pos = tbl.find(42).?;
    tbl.keys[pos] = TOMBSTONE;
    tbl.size_ptr.* -= 1;
    try testing.expect(!tbl.contains(42));

    // Rollback the delete — should restore the key+value
    try testing.expect(rollbackMapDelete(&state, meta, 42, 100, 0));
    try testing.expect(tbl.contains(42));
    try testing.expectEqual(@as(u32, 100), tbl.get(42).?.*);
    try testing.expectEqual(@as(u32, 1), tbl.size());
}

test "rollbackSetInsert — tombstones element" {
    var state: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const cap: u32 = 16;
    const slot_offset: u32 = types.STATE_HEADER_SIZE + types.SLOT_META_SIZE;
    const meta_base = types.STATE_HEADER_SIZE;

    std.mem.writeInt(u32, state[meta_base..][0..4], slot_offset, .little);
    std.mem.writeInt(u32, state[meta_base + 4 ..][0..4], cap, .little);
    std.mem.writeInt(u32, state[meta_base + 8 ..][0..4], 0, .little);
    state[meta_base + 12] = @intFromEnum(types.SlotType.HASHSET);

    const keys: [*]u32 = @ptrCast(@alignCast(&state[slot_offset]));
    for (0..cap) |j| keys[j] = EMPTY_KEY;

    const meta = types.getSlotMeta(&state, 0);
    const tbl = HashSet.bindSlot(&state, meta);

    _ = tbl.insertKey(42);
    try testing.expect(tbl.contains(42));

    try testing.expect(rollbackSetInsert(&state, meta, 42));
    try testing.expect(!tbl.contains(42));
    try testing.expectEqual(@as(u32, 0), tbl.size());
}

test "rollback — AGG_UPDATE restores previous f64 value and count" {
    var state: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const meta_base = types.STATE_HEADER_SIZE;
    // AGGREGATE SUM slot: 16 bytes (8 value + 8 count), offset just past metadata
    const slot_offset: u32 = types.STATE_HEADER_SIZE + types.SLOT_META_SIZE;

    // Write slot metadata: offset, capacity (unused for agg, set 0), size (unused), type_flags
    std.mem.writeInt(u32, state[meta_base..][0..4], slot_offset, .little);
    std.mem.writeInt(u32, state[meta_base + 4 ..][0..4], 0, .little);
    std.mem.writeInt(u32, state[meta_base + 8 ..][0..4], 0, .little);
    state[meta_base + 12] = @intFromEnum(types.SlotType.AGGREGATE);

    // Write initial value=10.0 at offset, count=5 at offset+8
    const val_ptr: *u64 = @ptrCast(@alignCast(&state[slot_offset]));
    const count_ptr: *u64 = @ptrCast(@alignCast(&state[slot_offset + 8]));
    const initial_val: f64 = 10.0;
    val_ptr.* = @bitCast(initial_val);
    count_ptr.* = 5;

    // Save previous state for rollback
    const prev_val_bits: u64 = @bitCast(initial_val);
    const prev_count: u32 = 5;

    // Simulate an aggregate update — overwrite to new values
    const new_val: f64 = 25.0;
    val_ptr.* = @bitCast(new_val);
    count_ptr.* = 8;

    // Verify the overwrite took effect
    const read_val: f64 = @bitCast(val_ptr.*);
    try testing.expectApproxEqAbs(@as(f64, 25.0), read_val, 0.001);
    try testing.expectEqual(@as(u64, 8), count_ptr.*);

    // Rollback using the helper
    const meta = types.getSlotMeta(&state, 0);
    rollbackAggUpdate(&state, meta, prev_count, prev_val_bits);

    // Verify value restored to 10.0 and count to 5
    const restored_val: f64 = @bitCast(val_ptr.*);
    try testing.expectApproxEqAbs(@as(f64, 10.0), restored_val, 0.001);
    try testing.expectEqual(@as(u64, 5), count_ptr.*);
}

test "rollback — COUNT_UPDATE restores previous u64 count" {
    var state: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const meta_base = types.STATE_HEADER_SIZE;
    // COUNT slot: 8 bytes (u64 count only), offset just past metadata
    const slot_offset: u32 = types.STATE_HEADER_SIZE + types.SLOT_META_SIZE;

    // Write slot metadata
    std.mem.writeInt(u32, state[meta_base..][0..4], slot_offset, .little);
    std.mem.writeInt(u32, state[meta_base + 4 ..][0..4], 0, .little);
    std.mem.writeInt(u32, state[meta_base + 8 ..][0..4], 0, .little);
    state[meta_base + 12] = @intFromEnum(types.SlotType.AGGREGATE);
    state[meta_base + 13] = @intFromEnum(types.AggType.COUNT);

    // Write initial count=42 at offset
    const count_ptr: *u64 = @ptrCast(@alignCast(&state[slot_offset]));
    count_ptr.* = 42;

    const prev_count: u32 = 42;

    // Simulate a count update — overwrite to new value
    count_ptr.* = 99;
    try testing.expectEqual(@as(u64, 99), count_ptr.*);

    // Rollback using the helper
    const meta = types.getSlotMeta(&state, 0);
    rollbackCountUpdate(&state, meta, prev_count);

    // Verify count restored to 42
    try testing.expectEqual(@as(u64, 42), count_ptr.*);
}
