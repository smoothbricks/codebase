// =============================================================================
// Slot Growth — rehash/copy via typed FlatHashTable
// =============================================================================
//
// When a slot exceeds 70% load, JS allocates a larger state buffer and calls
// vm_grow_state. This module provides typed growth functions that use
// FlatHashTable.rehashInto instead of manual pointer arithmetic.

const std = @import("std");
const types = @import("types.zig");
const ht = @import("hash_table.zig");

const EMPTY_KEY = types.EMPTY_KEY;
const TOMBSTONE = types.TOMBSTONE;
const SlotType = types.SlotType;
const SlotTypeFlags = types.SlotTypeFlags;
const StructFieldType = types.StructFieldType;
const hashKey = types.hashKey;
const align8 = types.align8;

const HashMap = ht.FlatHashTable(u32);
const HashSet = ht.HashSet;

// =============================================================================
// Slot data size calculation (typed, no hardcoded slot numbers)
// =============================================================================

/// Compute primary data size for a slot based on its SlotType.
/// For STRUCT_MAP/ORDERED_LIST/NESTED, returns 0 (use dedicated functions).
pub fn slotDataSize(slot_type: SlotType, capacity: u32, has_timestamps: bool, agg_type_byte: u8) u32 {
    return switch (slot_type) {
        .HASHMAP => capacity * 4 + capacity * 4 + if (has_timestamps) capacity * 8 else 0,
        .HASHSET => capacity * 4,
        .AGGREGATE => if (agg_type_byte == 2) 8 else 16, // COUNT=8, others=16
        .CONDITION_TREE => types.CONDITION_TREE_STATE_BYTES,
        .SCALAR => 16,
        .BITMAP => 4 + bitmapPayloadCapacity(capacity), // serialized_len(4) + payload
        .STRUCT_MAP, .ORDERED_LIST, .NESTED => 0, // use dedicated functions
        .ARRAY => capacity * 4 + capacity * 8,
    };
}

fn bitmapPayloadCapacity(capacity: u32) u32 {
    return if (capacity <= 256) 256 else capacity * 8;
}

// =============================================================================
// Growth: HASHMAP (SoA layout: keys + values + optional timestamps)
// =============================================================================

/// Grow a HASHMAP slot: rehash keys+values via FlatHashTable, copy timestamps side-array.
pub fn growHashMap(
    old_state: [*]const u8,
    new_state: [*]u8,
    old_offset: u32,
    new_offset: u32,
    old_cap: u32,
    new_cap: u32,
    has_timestamps: bool,
) u32 {
    // Use HashMap (FlatHashTable(u32)) for typed keys+values rehashing
    // Old table: bindExternal on old_state (readonly, so we cast away const for the API)
    const old_keys: [*]const u32 = @ptrCast(@alignCast(old_state + old_offset));
    const old_vals: [*]const u32 = @ptrCast(@alignCast(old_state + old_offset + old_cap * 4));

    // Initialize new table keys to EMPTY_KEY
    const new_keys: [*]u32 = @ptrCast(@alignCast(new_state + new_offset));
    const new_vals: [*]u32 = @ptrCast(@alignCast(new_state + new_offset + new_cap * 4));
    for (0..new_cap) |i| new_keys[i] = EMPTY_KEY;

    // Optional timestamps
    const old_ts: [*]const f64 = if (has_timestamps) @ptrCast(@alignCast(old_state + old_offset + old_cap * 8)) else undefined;
    const new_ts: [*]f64 = if (has_timestamps) @ptrCast(@alignCast(new_state + new_offset + new_cap * 8)) else undefined;

    // Rehash: scan old, probe new, copy key+value+ts
    var rehashed: u32 = 0;
    for (0..old_cap) |i| {
        const k = old_keys[i];
        if (k != EMPTY_KEY and k != TOMBSTONE) {
            var pos = hashKey(k, new_cap);
            while (new_keys[pos] != EMPTY_KEY) {
                pos = (pos + 1) & (new_cap - 1);
            }
            new_keys[pos] = k;
            new_vals[pos] = old_vals[i];
            if (has_timestamps) new_ts[pos] = old_ts[i];
            rehashed += 1;
        }
    }
    return rehashed;
}

/// Grow a HASHSET slot via typed HashSet rehashing.
pub fn growHashSet(
    old_state: [*]const u8,
    new_state: [*]u8,
    old_offset: u32,
    new_offset: u32,
    old_cap: u32,
    new_cap: u32,
) u32 {
    const old_keys: [*]const u32 = @ptrCast(@alignCast(old_state + old_offset));
    const new_keys: [*]u32 = @ptrCast(@alignCast(new_state + new_offset));
    for (0..new_cap) |i| new_keys[i] = EMPTY_KEY;

    var rehashed: u32 = 0;
    for (0..old_cap) |i| {
        const k = old_keys[i];
        if (k != EMPTY_KEY and k != TOMBSTONE) {
            var pos = hashKey(k, new_cap);
            while (new_keys[pos] != EMPTY_KEY) {
                pos = (pos + 1) & (new_cap - 1);
            }
            new_keys[pos] = k;
            rehashed += 1;
        }
    }
    return rehashed;
}

/// Grow a STRUCT_MAP slot: copy descriptor, rehash keys, copy row data.
pub fn growStructMap(
    old_state: [*]const u8,
    new_state: [*]u8,
    old_offset: u32,
    new_offset: u32,
    old_cap: u32,
    new_cap: u32,
    num_fields: u32,
    row_size: u32,
) u32 {
    const desc_size = align8(num_fields);

    // Copy field type descriptor prefix
    @memcpy(new_state[new_offset .. new_offset + num_fields], old_state[old_offset .. old_offset + num_fields]);

    // Initialize new keys
    const old_keys_offset = old_offset + desc_size;
    const new_keys_offset = new_offset + desc_size;
    const new_keys: [*]u32 = @ptrCast(@alignCast(new_state + new_keys_offset));
    for (0..new_cap) |i| new_keys[i] = EMPTY_KEY;

    const old_keys: [*]const u32 = @ptrCast(@alignCast(old_state + old_keys_offset));
    const old_rows_base = old_keys_offset + old_cap * 4;
    const new_rows_base = new_keys_offset + new_cap * 4;

    // Rehash: probe new, copy key + row data
    var rehashed: u32 = 0;
    for (0..old_cap) |i| {
        const k = old_keys[i];
        if (k != EMPTY_KEY and k != TOMBSTONE) {
            var pos = hashKey(k, new_cap);
            while (new_keys[pos] != EMPTY_KEY) {
                pos = (pos + 1) & (new_cap - 1);
            }
            new_keys[pos] = k;
            // Copy row data
            const old_row = old_rows_base + @as(u32, @intCast(i)) * row_size;
            const new_row = new_rows_base + pos * row_size;
            @memcpy(new_state[new_row .. new_row + row_size], old_state[old_row .. old_row + row_size]);
            rehashed += 1;
        }
    }
    return rehashed;
}

// =============================================================================
// Tests
// =============================================================================

const testing = std.testing;

test "growHashMap — rehashes into larger table preserving values" {
    var old_buf: [4096]u8 align(8) = [_]u8{0} ** 4096;
    var new_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    const old_cap: u32 = 16;
    const new_cap: u32 = 32;
    const offset: u32 = 0;

    // Set up old table manually: keys at 0, values at 64, no timestamps
    const old_keys: [*]u32 = @ptrCast(@alignCast(&old_buf[offset]));
    const old_vals: [*]u32 = @ptrCast(@alignCast(&old_buf[offset + old_cap * 4]));
    for (0..old_cap) |i| old_keys[i] = EMPTY_KEY;

    // Insert some entries manually
    old_keys[0] = 42;
    old_vals[0] = 100;
    old_keys[3] = 99;
    old_vals[3] = 200;
    old_keys[7] = 7;
    old_vals[7] = 300;

    const rehashed = growHashMap(&old_buf, &new_buf, offset, 0, old_cap, new_cap, false);
    try testing.expectEqual(@as(u32, 3), rehashed);

    // Verify all entries are in new table
    const new_tbl = HashMap.bindExternal(&new_buf, new_cap, undefined);
    try testing.expectEqual(@as(u32, 100), new_tbl.get(42).?.*);
    try testing.expectEqual(@as(u32, 200), new_tbl.get(99).?.*);
    try testing.expectEqual(@as(u32, 300), new_tbl.get(7).?.*);
    try testing.expect(new_tbl.get(1) == null);
}

test "growHashSet — rehashes keys only" {
    var old_buf: [512]u8 align(8) = [_]u8{0} ** 512;
    var new_buf: [1024]u8 align(8) = [_]u8{0} ** 1024;
    const old_cap: u32 = 16;
    const new_cap: u32 = 32;

    const old_keys: [*]u32 = @ptrCast(@alignCast(&old_buf[0]));
    for (0..old_cap) |i| old_keys[i] = EMPTY_KEY;
    old_keys[2] = 10;
    old_keys[5] = 20;
    old_keys[9] = 30;

    const rehashed = growHashSet(&old_buf, &new_buf, 0, 0, old_cap, new_cap);
    try testing.expectEqual(@as(u32, 3), rehashed);

    const new_keys: [*]const u32 = @ptrCast(@alignCast(&new_buf[0]));
    // Verify all elements present (check by scanning)
    var found: u32 = 0;
    for (0..new_cap) |i| {
        if (new_keys[i] != EMPTY_KEY and new_keys[i] != TOMBSTONE) found += 1;
    }
    try testing.expectEqual(@as(u32, 3), found);
}

test "growHashMap — preserves timestamps side-array" {
    var old_buf: [4096]u8 align(8) = [_]u8{0} ** 4096;
    var new_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    const old_cap: u32 = 16;
    const new_cap: u32 = 32;

    const old_keys: [*]u32 = @ptrCast(@alignCast(&old_buf[0]));
    const old_vals: [*]u32 = @ptrCast(@alignCast(&old_buf[old_cap * 4]));
    const old_ts: [*]f64 = @ptrCast(@alignCast(&old_buf[old_cap * 8]));
    for (0..old_cap) |i| old_keys[i] = EMPTY_KEY;

    old_keys[0] = 42;
    old_vals[0] = 100;
    old_ts[0] = 999.5;

    const rehashed = growHashMap(&old_buf, &new_buf, 0, 0, old_cap, new_cap, true);
    try testing.expectEqual(@as(u32, 1), rehashed);

    // Find where key 42 landed in new table
    const new_keys: [*]const u32 = @ptrCast(@alignCast(&new_buf[0]));
    const new_ts: [*]const f64 = @ptrCast(@alignCast(&new_buf[new_cap * 8]));
    var found_pos: u32 = new_cap;
    for (0..new_cap) |i| {
        if (new_keys[i] == 42) {
            found_pos = @intCast(i);
            break;
        }
    }
    try testing.expect(found_pos < new_cap);
    try testing.expectApproxEqAbs(@as(f64, 999.5), new_ts[found_pos], 0.001);
}

// =============================================================================
// Stress tests — sequential growth cascade
// =============================================================================

test "stress — sequential HashMap growth 16→32→64→128" {
    // Uses FlatHashTable with inline headers (init/bind) so size is tracked
    // automatically in the header. Each growth step rehashes via the typed API
    // into a new region within the same buffer, then re-binds.
    const HT = ht.FlatHashTable(u32);

    var buf: [65536]u8 align(8) = [_]u8{0} ** 65536;

    // Track all inserted keys and their values for verification
    var all_keys: [256]u32 = [_]u32{0} ** 256;
    var all_vals: [256]u32 = [_]u32{0} ** 256;
    var total_inserted: u32 = 0;
    var next_key: u32 = 1;

    // Start: cap=16 at offset 0
    var current_cap: u32 = 16;
    var current_offset: u32 = 0;
    var tbl = HT.init(&buf, current_offset, current_cap);

    // Growth targets: 16 → 32 → 64 → 128
    const target_caps = [_]u32{ 32, 64, 128 };

    for (target_caps) |new_cap| {
        // Insert keys until load factor exceeded (null = needs growth)
        while (true) {
            const result = tbl.upsert(next_key, next_key * 7);
            if (result == null) break;
            all_keys[total_inserted] = next_key;
            all_vals[total_inserted] = next_key * 7;
            total_inserted += 1;
            next_key += 1;
        }

        // Grow: rehash into a new region using the typed rehashInto API
        const new_offset = current_offset + HT.byteSize(current_cap) + 64; // gap to avoid overlap
        tbl = tbl.rehashInto(&buf, new_offset, new_cap);

        // Verify all entries present after growth
        try testing.expectEqual(total_inserted, tbl.size());
        var v: u32 = 0;
        while (v < total_inserted) : (v += 1) {
            const entry = tbl.get(all_keys[v]);
            try testing.expect(entry != null);
            try testing.expectEqual(all_vals[v], entry.?.*);
        }

        current_offset = new_offset;
        current_cap = new_cap;

        // Insert the key that triggered growth (was rejected above)
        const retry = tbl.upsert(next_key, next_key * 7);
        try testing.expect(retry != null);
        all_keys[total_inserted] = next_key;
        all_vals[total_inserted] = next_key * 7;
        total_inserted += 1;
        next_key += 1;
    }

    // Final verification: all keys still have correct values
    var f: u32 = 0;
    while (f < total_inserted) : (f += 1) {
        const entry = tbl.get(all_keys[f]);
        try testing.expect(entry != null);
        try testing.expectEqual(all_vals[f], entry.?.*);
    }

    // Verify final size matches total inserted
    try testing.expectEqual(total_inserted, tbl.size());
    try testing.expect(total_inserted > 0);
}
