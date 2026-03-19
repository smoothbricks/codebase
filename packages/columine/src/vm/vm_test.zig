// =============================================================================
// VM Integration Tests — full pipeline (program → state → execute → growth)
// =============================================================================
//
// These tests build raw bytecode programs and exercise the complete VM pipeline:
// vm_calculate_state_size → vm_init_state → vm_execute_batch → vm_grow_state.
// They test slot types, block dispatch, growth, TTL, struct maps, and ordered lists.

const std = @import("std");
const vm = @import("vm.zig");
const types = @import("types.zig");
const hash_table = @import("hash_table.zig");
const aggregates = @import("aggregates.zig");
const slot_growth = @import("slot_growth.zig");

// Re-import needed symbols
const STATE_HEADER_SIZE = types.STATE_HEADER_SIZE;
const SLOT_META_SIZE = types.SLOT_META_SIZE;
const SlotMetaOffset = types.SlotMetaOffset;
const PROGRAM_HASH_PREFIX = types.PROGRAM_HASH_PREFIX;
const SlotType = types.SlotType;
const SlotTypeFlags = types.SlotTypeFlags;
const AggType = types.AggType;
const StructFieldType = types.StructFieldType;
const ErrorCode = types.ErrorCode;
const EMPTY_KEY = types.EMPTY_KEY;
const EvictionEntry = types.EvictionEntry;
const ConditionTreeState = types.ConditionTreeState;
const CONDITION_TREE_STATE_BYTES = types.CONDITION_TREE_STATE_BYTES;
const align8 = types.align8;
const nextPowerOf2 = types.nextPowerOf2;
const hashKey = types.hashKey;
const getSlotMeta = types.getSlotMeta;
const structFieldSize = types.structFieldSize;
const structFieldOffset = types.structFieldOffset;
const computeStructRowLayout = types.computeStructRowLayout;
const isArrayFieldType = types.isArrayFieldType;
const hasArrayFields = types.hasArrayFields;
const DurationUnit = types.DurationUnit;
const Opcode = types.Opcode;

// VM exports used by tests
const vm_calculate_state_size = vm.vm_calculate_state_size;
const vm_init_state = vm.vm_init_state;
const vm_execute_batch = vm.vm_execute_batch;
const vm_execute_batch_delta = vm.vm_execute_batch_delta;
const vm_calculate_grown_state_size = vm.vm_calculate_grown_state_size;
const vm_grow_state = vm.vm_grow_state;
const vm_get_needs_growth_slot = vm.vm_get_needs_growth_slot;
const vm_map_get = vm.vm_map_get;
const vm_struct_map_get_row_ptr = vm.vm_struct_map_get_row_ptr;
const vm_struct_map_iter_start = vm.vm_struct_map_iter_start;
const vm_struct_map_iter_next = vm.vm_struct_map_iter_next;
const vm_struct_map_iter_key = vm.vm_struct_map_iter_key;
const vm_map_iter_start = vm.vm_map_iter_start;
const vm_map_iter_next = vm.vm_map_iter_next;
const vm_map_iter_get = vm.vm_map_iter_get;
const vm_set_iter_start = vm.vm_set_iter_start;
const vm_set_iter_next = vm.vm_set_iter_next;
const vm_set_iter_get = vm.vm_set_iter_get;
const vm_undo_enable = vm.vm_undo_enable;
const vm_undo_checkpoint = vm.vm_undo_checkpoint;
const vm_undo_rollback = vm.vm_undo_rollback;
const vm_undo_commit = vm.vm_undo_commit;
const vm_evict_all_expired = vm.vm_evict_all_expired;
const vm_reset_state = vm.vm_reset_state;
const vm_set_contains = vm.vm_set_contains;

// Bitmap helpers
const BITMAP_SERIALIZED_LEN_BYTES = vm.BITMAP_SERIALIZED_LEN_BYTES;
const bitmapPayloadCapacity = vm.bitmapPayloadCapacity;
const getStructMapSlotDataSize = vm.getStructMapSlotDataSize;
const ARENA_HEADER_SIZE = vm.ARENA_HEADER_SIZE;
const arenaInitialCapacity = vm.arenaInitialCapacity;

// =============================================================================
// Tests — Slot Growth
// =============================================================================

/// Build a minimal program with one HashMap slot (BATCH_MAP_UPSERT_LAST)
/// and one Aggregate slot (BATCH_AGG_COUNT).
fn buildTestProgram(comptime cap_lo: u8, comptime cap_hi: u8) [64]u8 {
    var prog = [_]u8{0} ** 64;
    const content = prog[PROGRAM_HASH_PREFIX..];
    // Magic "CLM1" stored as bytes: 'A','X','E','1' → reads as LE u32 = 0x314D4C43
    content[0] = 0x41; // 'A'
    content[1] = 0x58; // 'X'
    content[2] = 0x45; // 'E'
    content[3] = 0x31; // '1'
    // Version: 1.0
    content[4] = 1;
    content[5] = 0;
    // num_slots = 2, num_inputs = 2
    content[6] = 2;
    content[7] = 2;
    // reserved
    content[8] = 0;
    content[9] = 0;
    // Init section: SLOT_DEF(5 bytes) × 2 = 10 bytes
    const init_len: u16 = 10;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);
    // Reduce section: BATCH_MAP_UPSERT_LAST(4 bytes) + BATCH_AGG_COUNT(2 bytes) = 6 bytes
    const reduce_len: u16 = 6;
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    // Init section starts at content[14]
    var off: usize = 14;
    // SLOT_DEF slot=0, type_flags=0x00 (HASHMAP), cap_lo, cap_hi
    content[off] = 0x10; // SLOT_DEF opcode
    content[off + 1] = 0; // slot index
    content[off + 2] = 0x00; // type_flags: HASHMAP=0, no TTL
    content[off + 3] = cap_lo;
    content[off + 4] = cap_hi;
    off += 5;
    // SLOT_DEF slot=1, type_flags=0x02 (AGGREGATE), cap_lo=2 (COUNT), cap_hi=0
    content[off] = 0x10; // SLOT_DEF opcode
    content[off + 1] = 1; // slot index
    content[off + 2] = 0x02; // type_flags: AGGREGATE=2
    content[off + 3] = 2; // AggType.COUNT
    content[off + 4] = 0;

    // Reduce section starts at content[14 + init_len]
    const reduce_start = 14 + init_len;
    // BATCH_MAP_UPSERT_LAST slot=0, key_col=0, val_col=1
    content[reduce_start] = 0x22;
    content[reduce_start + 1] = 0;
    content[reduce_start + 2] = 0;
    content[reduce_start + 3] = 1;
    // BATCH_AGG_COUNT slot=1
    content[reduce_start + 4] = 0x41;
    content[reduce_start + 5] = 1;

    return prog;
}

fn buildNoTimestampMapProgram(comptime cap_lo: u8, comptime cap_hi: u8, comptime map_opcode: u8, comptime needs_ts_col: bool) [80]u8 {
    var prog = [_]u8{0} ** 80;
    const content = prog[PROGRAM_HASH_PREFIX..];
    content[0] = 0x41; // 'A'
    content[1] = 0x58; // 'X'
    content[2] = 0x45; // 'E'
    content[3] = 0x31; // '1'
    content[4] = 1;
    content[5] = 0;
    content[6] = 1; // num_slots
    content[7] = if (needs_ts_col) 3 else 2;
    content[8] = 0;
    content[9] = 0;

    const init_len: u16 = 5;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);
    const reduce_len: u16 = if (needs_ts_col) 5 else 4;
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    var off: usize = 14;
    content[off] = 0x10; // SLOT_DEF
    content[off + 1] = 0;
    content[off + 2] = 0x40; // HASHMAP + NO_HASHMAP_TIMESTAMPS
    content[off + 3] = cap_lo;
    content[off + 4] = cap_hi;
    off += 5;

    content[off] = map_opcode;
    content[off + 1] = 0;
    content[off + 2] = 0;
    content[off + 3] = 1;
    if (needs_ts_col) {
        content[off + 4] = 2;
    }
    return prog;
}

fn writeF32LE(dst: []u8, value: f32) void {
    std.mem.writeInt(u32, dst[0..4], @bitCast(value), .little);
}

/// Build a minimal TTL HashMap program with one slot and one
/// BATCH_MAP_UPSERT_LATEST reducer opcode.
fn buildTTLMapProgram(comptime cap_lo: u8, comptime cap_hi: u8, comptime has_evict_trigger: bool) [96]u8 {
    var prog = [_]u8{0} ** 96;
    const content = prog[PROGRAM_HASH_PREFIX..];

    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    content[4] = 1;
    content[5] = 0;
    content[6] = 1; // num_slots
    content[7] = 3; // num_inputs (key, val, ts)
    content[8] = 0;
    content[9] = 0;

    // SLOT_DEF with TTL payload: 1(op)+1(slot)+1(type_flags)+1(cap_lo)+1(cap_hi)+10(ttl payload)
    const init_len: u16 = 15;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);

    // BATCH_MAP_UPSERT_LATEST slot,key,val,ts
    const reduce_len: u16 = 5;
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    var off: usize = 14;
    const ttl_flag: u8 = 0x10;
    const evict_flag: u8 = if (has_evict_trigger) 0x20 else 0;
    content[off] = 0x10; // SLOT_DEF
    content[off + 1] = 0;
    content[off + 2] = 0x00 | ttl_flag | evict_flag; // HASHMAP + flags
    content[off + 3] = cap_lo;
    content[off + 4] = cap_hi;
    writeF32LE(content[off + 5 .. off + 9], 10.0); // ttl_seconds
    writeF32LE(content[off + 9 .. off + 13], 0.0); // grace_seconds
    content[off + 13] = 2; // timestamp_field_idx
    content[off + 14] = @intFromEnum(DurationUnit.NONE);
    off += 15;

    content[off] = 0x20; // BATCH_MAP_UPSERT_LATEST
    content[off + 1] = 0; // slot
    content[off + 2] = 0; // key col
    content[off + 3] = 1; // val col
    content[off + 4] = 2; // ts col

    return prog;
}

/// Build a minimal TTL HashSet program with one slot and one BATCH_SET_INSERT reducer opcode.
fn buildTTLSetProgram(comptime cap_lo: u8, comptime cap_hi: u8) [96]u8 {
    var prog = [_]u8{0} ** 96;
    const content = prog[PROGRAM_HASH_PREFIX..];

    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    content[4] = 1;
    content[5] = 0;
    content[6] = 1; // num_slots
    content[7] = 2; // num_inputs (elem, ts)
    content[8] = 0;
    content[9] = 0;

    const init_len: u16 = 15;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);

    const reduce_len: u16 = 3;
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    var off: usize = 14;
    content[off] = 0x10; // SLOT_DEF
    content[off + 1] = 0;
    content[off + 2] = 0x01 | 0x10; // HASHSET + has_ttl
    content[off + 3] = cap_lo;
    content[off + 4] = cap_hi;
    writeF32LE(content[off + 5 .. off + 9], 10.0); // ttl_seconds
    writeF32LE(content[off + 9 .. off + 13], 0.0); // grace_seconds
    content[off + 13] = 1; // timestamp_field_idx
    content[off + 14] = @intFromEnum(DurationUnit.NONE);
    off += 15;

    content[off] = 0x30; // BATCH_SET_INSERT
    content[off + 1] = 0; // slot
    content[off + 2] = 0; // elem col

    return prog;
}

test "hashmap no-timestamp slot reduces state size" {
    var with_ts = buildNoTimestampMapProgram(4, 0, 0x22, false);
    const no_ts = buildNoTimestampMapProgram(4, 0, 0x22, false);
    with_ts[PROGRAM_HASH_PREFIX + 16] &= ~@as(u8, 0x40);

    const with_ts_size = vm_calculate_state_size(@ptrCast(&with_ts), with_ts.len);
    const no_ts_size = vm_calculate_state_size(@ptrCast(&no_ts), no_ts.len);

    // Effective capacity is nextPowerOf2(4 * 2) = 16, saving one f64 per entry.
    try std.testing.expectEqual(@as(u32, 16 * 8), with_ts_size - no_ts_size);
}

test "hashmap no-timestamp supports first and last semantics" {
    const first_prog = buildNoTimestampMapProgram(4, 0, 0x21, false);
    const last_prog = buildNoTimestampMapProgram(4, 0, 0x22, false);

    const size_first = vm_calculate_state_size(@ptrCast(&first_prog), first_prog.len);
    const size_last = vm_calculate_state_size(@ptrCast(&last_prog), last_prog.len);

    var first_buf = try std.testing.allocator.alignedAlloc(u8, std.mem.Alignment.of(u64), @intCast(size_first));
    defer std.testing.allocator.free(first_buf);
    @memset(first_buf, 0);
    var last_buf = try std.testing.allocator.alignedAlloc(u8, std.mem.Alignment.of(u64), @intCast(size_last));
    defer std.testing.allocator.free(last_buf);
    @memset(last_buf, 0);

    const first_state: [*]u8 = @ptrCast(first_buf.ptr);
    const last_state: [*]u8 = @ptrCast(last_buf.ptr);
    try std.testing.expectEqual(@as(u32, 0), vm_init_state(first_state, @ptrCast(&first_prog), first_prog.len));
    try std.testing.expectEqual(@as(u32, 0), vm_init_state(last_state, @ptrCast(&last_prog), last_prog.len));

    var keys = [3]u32{ 7, 7, 7 };
    var vals = [3]u32{ 10, 20, 30 };
    const col_ptrs = [2][*]const u8{ @ptrCast(&keys), @ptrCast(&vals) };

    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.OK)), vm_execute_batch(first_state, @ptrCast(&first_prog), first_prog.len, @ptrCast(&col_ptrs), 2, 3));
    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.OK)), vm_execute_batch(last_state, @ptrCast(&last_prog), last_prog.len, @ptrCast(&col_ptrs), 2, 3));

    const first_meta = STATE_HEADER_SIZE;
    const first_offset = std.mem.readInt(u32, first_buf[first_meta..][0..4], .little);
    const first_cap = std.mem.readInt(u32, first_buf[first_meta + 4 ..][0..4], .little);
    const last_offset = std.mem.readInt(u32, last_buf[first_meta..][0..4], .little);
    const last_cap = std.mem.readInt(u32, last_buf[first_meta + 4 ..][0..4], .little);

    try std.testing.expectEqual(@as(u32, 10), vm_map_get(first_state, first_offset, first_cap, 7));
    try std.testing.expectEqual(@as(u32, 30), vm_map_get(last_state, last_offset, last_cap, 7));
}

test "hashmap no-timestamp rejects latest/max/min opcodes" {
    const latest_prog = buildNoTimestampMapProgram(4, 0, 0x20, true);
    const max_prog = buildNoTimestampMapProgram(4, 0, 0x26, true);
    const min_prog = buildNoTimestampMapProgram(4, 0, 0x27, true);

    const progs = [_][80]u8{ latest_prog, max_prog, min_prog };
    for (progs) |prog| {
        const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
        const state_buf = try std.testing.allocator.alignedAlloc(u8, std.mem.Alignment.of(u64), @intCast(state_size));
        defer std.testing.allocator.free(state_buf);
        @memset(state_buf, 0);
        const state_ptr: [*]u8 = @ptrCast(state_buf.ptr);
        try std.testing.expectEqual(@as(u32, 0), vm_init_state(state_ptr, @ptrCast(&prog), prog.len));

        var keys = [1]u32{1};
        var vals = [1]u32{42};
        var cmp = [1]f64{123};
        const col_ptrs = [3][*]const u8{ @ptrCast(&keys), @ptrCast(&vals), @ptrCast(&cmp) };

        const result = vm_execute_batch(state_ptr, @ptrCast(&prog), prog.len, @ptrCast(&col_ptrs), 3, 1);
        try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.INVALID_PROGRAM)), result);
    }
}

test "hashmap no-timestamp growth preserves entries" {
    const prog = buildNoTimestampMapProgram(4, 0, 0x22, false);
    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    const state_buf = try std.testing.allocator.alignedAlloc(u8, std.mem.Alignment.of(u64), @intCast(state_size));
    defer std.testing.allocator.free(state_buf);
    @memset(state_buf, 0);
    const state_ptr: [*]u8 = @ptrCast(state_buf.ptr);
    try std.testing.expectEqual(@as(u32, 0), vm_init_state(state_ptr, @ptrCast(&prog), prog.len));

    var keys = [_]u32{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    var vals = [_]u32{ 0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 };
    const batch_ptrs = [2][*]const u8{ @ptrCast(&keys), @ptrCast(&vals) };
    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.OK)), vm_execute_batch(state_ptr, @ptrCast(&prog), prog.len, @ptrCast(&batch_ptrs), 2, 11));

    var overflow_key = [1]u32{100};
    var overflow_val = [1]u32{1000};
    const overflow_ptrs = [2][*]const u8{ @ptrCast(&overflow_key), @ptrCast(&overflow_val) };
    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.NEEDS_GROWTH)), vm_execute_batch(state_ptr, @ptrCast(&prog), prog.len, @ptrCast(&overflow_ptrs), 2, 1));

    const grown_size = vm_calculate_grown_state_size(state_ptr, @ptrCast(&prog), prog.len, 0);
    var grown_buf = try std.testing.allocator.alignedAlloc(u8, std.mem.Alignment.of(u64), @intCast(grown_size));
    defer std.testing.allocator.free(grown_buf);
    @memset(grown_buf, 0);
    const grown_ptr: [*]u8 = @ptrCast(grown_buf.ptr);
    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.OK)), vm_grow_state(state_ptr, grown_ptr, @ptrCast(&prog), prog.len, 0));
    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.OK)), vm_execute_batch(grown_ptr, @ptrCast(&prog), prog.len, @ptrCast(&overflow_ptrs), 2, 1));

    const meta_base = STATE_HEADER_SIZE;
    const cap = std.mem.readInt(u32, grown_buf[meta_base + 4 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 32), cap);
    for (0..11) |i| {
        try std.testing.expectEqual(vals[i], vm_map_get(grown_ptr, std.mem.readInt(u32, grown_buf[meta_base..][0..4], .little), cap, @intCast(i)));
    }
    try std.testing.expectEqual(@as(u32, 1000), vm_map_get(grown_ptr, std.mem.readInt(u32, grown_buf[meta_base..][0..4], .little), cap, 100));
}

test "hashmap invalid no-timestamp+ttl flag combination is rejected" {
    var prog = buildNoTimestampMapProgram(4, 0, 0x22, false);
    const content = prog[PROGRAM_HASH_PREFIX..];
    // Turn on has_ttl while keeping no_hashmap_timestamps.
    content[16] |= 0x10;
    // Extend init_len by 10 bytes and provide TTL payload.
    const init_len: u16 = 15;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);
    writeF32LE(content[19..23], 10.0);
    writeF32LE(content[23..27], 0.0);
    content[27] = 1;
    content[28] = @intFromEnum(DurationUnit.NONE);

    try std.testing.expectEqual(@as(u32, 0), vm_calculate_state_size(@ptrCast(&prog), prog.len));

    var state_buf = [_]u8{0} ** 256;
    const init_result = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);
    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.INVALID_PROGRAM)), init_result);
}

test "ttl hashmap - insert and evict through live reducer path" {
    const prog = buildTTLMapProgram(4, 0, true);
    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf = try std.testing.allocator.alignedAlloc(u8, std.mem.Alignment.of(u64), @intCast(state_size));
    defer std.testing.allocator.free(state_buf);
    @memset(state_buf, 0);
    const state_ptr: [*]u8 = @ptrCast(state_buf.ptr);

    const init_result = vm_init_state(state_ptr, @ptrCast(&prog), prog.len);
    try std.testing.expectEqual(@as(u32, 0), init_result);

    var key_col = [1]u32{42};
    var val_col = [1]u32{7};
    var ts_col = [1]f64{100.0};
    const col_ptrs = [3][*]const u8{ @ptrCast(&key_col), @ptrCast(&val_col), @ptrCast(&ts_col) };

    const exec_result = vm_execute_batch(state_ptr, @ptrCast(&prog), prog.len, @ptrCast(&col_ptrs), 3, 1);
    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.OK)), exec_result);

    const meta_base = STATE_HEADER_SIZE;
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const slot_cap = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    const before = vm_map_get(state_ptr, slot_offset, slot_cap, 42);
    try std.testing.expectEqual(@as(u32, 7), before);

    const evict_none = vm_evict_all_expired(state_ptr, 105.0);
    try std.testing.expectEqual(@as(u32, 0), evict_none);
    try std.testing.expectEqual(@as(u32, 7), vm_map_get(state_ptr, slot_offset, slot_cap, 42));

    const evict_one = vm_evict_all_expired(state_ptr, 111.0);
    try std.testing.expectEqual(@as(u32, 1), evict_one);
    try std.testing.expectEqual(EMPTY_KEY, vm_map_get(state_ptr, slot_offset, slot_cap, 42));
}

test "ttl hashmap - stale eviction entries do not evict newer values" {
    const prog = buildTTLMapProgram(4, 0, false);
    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf = try std.testing.allocator.alignedAlloc(u8, std.mem.Alignment.of(u64), @intCast(state_size));
    defer std.testing.allocator.free(state_buf);
    @memset(state_buf, 0);
    const state_ptr: [*]u8 = @ptrCast(state_buf.ptr);

    try std.testing.expectEqual(@as(u32, 0), vm_init_state(state_ptr, @ptrCast(&prog), prog.len));

    var key_col_a = [1]u32{9};
    var val_col_a = [1]u32{100};
    var ts_col_a = [1]f64{100.0};
    const ptrs_a = [3][*]const u8{ @ptrCast(&key_col_a), @ptrCast(&val_col_a), @ptrCast(&ts_col_a) };
    try std.testing.expectEqual(@as(u32, 0), vm_execute_batch(state_ptr, @ptrCast(&prog), prog.len, @ptrCast(&ptrs_a), 3, 1));

    var key_col_b = [1]u32{9};
    var val_col_b = [1]u32{200};
    var ts_col_b = [1]f64{200.0};
    const ptrs_b = [3][*]const u8{ @ptrCast(&key_col_b), @ptrCast(&val_col_b), @ptrCast(&ts_col_b) };
    try std.testing.expectEqual(@as(u32, 0), vm_execute_batch(state_ptr, @ptrCast(&prog), prog.len, @ptrCast(&ptrs_b), 3, 1));

    const meta_base = STATE_HEADER_SIZE;
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const slot_cap = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);

    // cutoff=140, so the stale ts=100 index entry is expired. It must not evict
    // the current key value with ts=200.
    const evicted = vm_evict_all_expired(state_ptr, 150.0);
    try std.testing.expectEqual(@as(u32, 0), evicted);
    try std.testing.expectEqual(@as(u32, 200), vm_map_get(state_ptr, slot_offset, slot_cap, 9));
}

test "ttl hashset - reinsertion refreshes ttl and evicts deterministically" {
    const prog = buildTTLSetProgram(4, 0);
    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf = try std.testing.allocator.alignedAlloc(u8, std.mem.Alignment.of(u64), @intCast(state_size));
    defer std.testing.allocator.free(state_buf);
    @memset(state_buf, 0);
    const state_ptr: [*]u8 = @ptrCast(state_buf.ptr);

    try std.testing.expectEqual(@as(u32, 0), vm_init_state(state_ptr, @ptrCast(&prog), prog.len));

    var elem_1 = [1]u32{77};
    var ts_1 = [1]f64{100.0};
    const ptrs_1 = [2][*]const u8{ @ptrCast(&elem_1), @ptrCast(&ts_1) };
    try std.testing.expectEqual(@as(u32, 0), vm_execute_batch(state_ptr, @ptrCast(&prog), prog.len, @ptrCast(&ptrs_1), 2, 1));

    var elem_2 = [1]u32{77};
    var ts_2 = [1]f64{200.0};
    const ptrs_2 = [2][*]const u8{ @ptrCast(&elem_2), @ptrCast(&ts_2) };
    try std.testing.expectEqual(@as(u32, 0), vm_execute_batch(state_ptr, @ptrCast(&prog), prog.len, @ptrCast(&ptrs_2), 2, 1));

    const meta_base = STATE_HEADER_SIZE;
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const slot_cap = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);

    try std.testing.expect(vm_set_contains(state_ptr, slot_offset, slot_cap, 77) == 1);

    // cutoff=140; refreshed timestamp=200 keeps element alive
    try std.testing.expectEqual(@as(u32, 0), vm_evict_all_expired(state_ptr, 150.0));
    try std.testing.expect(vm_set_contains(state_ptr, slot_offset, slot_cap, 77) == 1);

    // cutoff=201; refreshed timestamp now expires
    try std.testing.expectEqual(@as(u32, 1), vm_evict_all_expired(state_ptr, 211.0));
    try std.testing.expect(vm_set_contains(state_ptr, slot_offset, slot_cap, 77) == 0);
}

test "ttl eviction index overflow - returns NEEDS_GROWTH instead of dropping" {
    const prog = buildTTLMapProgram(4, 0, false);
    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf = try std.testing.allocator.alignedAlloc(u8, std.mem.Alignment.of(u64), @intCast(state_size));
    defer std.testing.allocator.free(state_buf);
    @memset(state_buf, 0);
    const state_ptr: [*]u8 = @ptrCast(state_buf.ptr);

    try std.testing.expectEqual(@as(u32, 0), vm_init_state(state_ptr, @ptrCast(&prog), prog.len));

    // Force tiny eviction index capacity to trigger deterministic overflow on
    // the second insert (policy path under test).
    const meta_base = STATE_HEADER_SIZE;
    std.mem.writeInt(u32, state_buf[meta_base + SlotMetaOffset.EVICTION_INDEX_CAPACITY ..][0..4], 1, .little);

    var key_col = [2]u32{ 1, 2 };
    var val_col = [2]u32{ 10, 20 };
    var ts_col = [2]f64{ 100.0, 101.0 };
    const ptrs = [3][*]const u8{ @ptrCast(&key_col), @ptrCast(&val_col), @ptrCast(&ts_col) };

    const result = vm_execute_batch(state_ptr, @ptrCast(&prog), prog.len, @ptrCast(&ptrs), 3, 2);
    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.NEEDS_GROWTH)), result);
    try std.testing.expectEqual(@as(u32, 0), vm_get_needs_growth_slot());
}

test "slot growth - hashmap capacity exceeded triggers NEEDS_GROWTH" {
    // Build program with tiny capacity (cap_lo=4, cap_hi=0 → effective capacity = nextPowerOf2(4*2) = 16)
    const prog = buildTestProgram(4, 0);

    // Calculate state size and allocate
    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [4096]u8 align(8) = [_]u8{0} ** 4096;
    std.debug.assert(state_size <= 4096);

    // Init state
    const init_result = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);
    try std.testing.expectEqual(@as(u32, 0), init_result);

    // Verify capacity is 16 (nextPowerOf2(4*2) = 16)
    const meta_base = STATE_HEADER_SIZE;
    const cap = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 16), cap);

    // Insert 11 unique keys (max_size = (16*7)/10 = 11, so first 11 fit exactly)
    var i: u32 = 0;
    while (i < 11) : (i += 1) {
        var key_col = [1]u32{i};
        var val_col = [1]u32{i * 10};
        const key_ptr: [*]const u8 = @ptrCast(&key_col);
        const val_ptr: [*]const u8 = @ptrCast(&val_col);
        const col_ptrs = [2][*]const u8{ key_ptr, val_ptr };
        const result = vm_execute_batch(
            @ptrCast(&state_buf),
            @ptrCast(&prog),
            prog.len,
            @ptrCast(&col_ptrs),
            2,
            1,
        );
        try std.testing.expectEqual(@as(u32, 0), result);
    }

    // Verify size = 11
    const size_after_11 = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 11), size_after_11);

    // 12th unique key should trigger NEEDS_GROWTH (size=11 >= max_size=11)
    var key_col_12 = [1]u32{100};
    var val_col_12 = [1]u32{1000};
    const key_ptr_12: [*]const u8 = @ptrCast(&key_col_12);
    const val_ptr_12: [*]const u8 = @ptrCast(&val_col_12);
    const col_ptrs_12 = [2][*]const u8{ key_ptr_12, val_ptr_12 };
    const result_12 = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs_12),
        2,
        1,
    );
    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.NEEDS_GROWTH)), result_12);

    // Verify g_needs_growth_slot is slot 0
    try std.testing.expectEqual(@as(u32, 0), vm_get_needs_growth_slot());
}

test "slot growth - grow preserves hashmap entries and aggregate" {
    const prog = buildTestProgram(4, 0);

    // Init state
    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [4096]u8 align(8) = [_]u8{0} ** 4096;
    std.debug.assert(state_size <= 4096);

    const init_result = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);
    try std.testing.expectEqual(@as(u32, 0), init_result);

    // Insert 11 unique keys
    var i: u32 = 0;
    while (i < 11) : (i += 1) {
        var key_col = [1]u32{i};
        var val_col = [1]u32{i * 10};
        const key_ptr: [*]const u8 = @ptrCast(&key_col);
        const val_ptr: [*]const u8 = @ptrCast(&val_col);
        const col_ptrs = [2][*]const u8{ key_ptr, val_ptr };
        _ = vm_execute_batch(
            @ptrCast(&state_buf),
            @ptrCast(&prog),
            prog.len,
            @ptrCast(&col_ptrs),
            2,
            1,
        );
    }

    // Verify aggregate count = 11 (11 batches of 1 row each)
    const agg_meta_base = STATE_HEADER_SIZE + SLOT_META_SIZE; // slot 1 meta
    const agg_data_offset = std.mem.readInt(u32, state_buf[agg_meta_base..][0..4], .little);
    const count_val = std.mem.readInt(u64, state_buf[agg_data_offset..][0..8], .little);
    try std.testing.expectEqual(@as(u64, 11), count_val);

    // Grow slot 0
    const grown_size = vm_calculate_grown_state_size(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        0,
    );
    var new_state_buf: [16384]u8 align(8) = [_]u8{0} ** 16384;
    std.debug.assert(grown_size <= 16384);

    const grow_result = vm_grow_state(
        @ptrCast(&state_buf),
        @ptrCast(&new_state_buf),
        @ptrCast(&prog),
        prog.len,
        0,
    );
    try std.testing.expectEqual(@as(u32, 0), grow_result);

    // Verify new capacity doubled: 16 → 32
    const new_meta_base = STATE_HEADER_SIZE;
    const new_cap = std.mem.readInt(u32, new_state_buf[new_meta_base + 4 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 32), new_cap);

    // Verify size preserved (11 entries rehashed)
    const new_size = std.mem.readInt(u32, new_state_buf[new_meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 11), new_size);

    // Verify all entries preserved via vm_map_get
    const new_data_offset = std.mem.readInt(u32, new_state_buf[new_meta_base..][0..4], .little);
    i = 0;
    while (i < 11) : (i += 1) {
        const result = vm_map_get(@ptrCast(&new_state_buf), new_data_offset, 32, i);
        try std.testing.expectEqual(i * 10, result);
    }

    // Verify aggregate count preserved (copied, not re-executed)
    const new_agg_meta_base = STATE_HEADER_SIZE + SLOT_META_SIZE;
    const new_agg_data_offset = std.mem.readInt(u32, new_state_buf[new_agg_meta_base..][0..4], .little);
    const new_count_val = std.mem.readInt(u64, new_state_buf[new_agg_data_offset..][0..8], .little);
    try std.testing.expectEqual(@as(u64, 11), new_count_val);

    // Verify we can now insert more entries without NEEDS_GROWTH
    // (new max_size = (32*7)/10 = 22, currently at 11)
    var key_col_new = [1]u32{100};
    var val_col_new = [1]u32{1000};
    const key_ptr_new: [*]const u8 = @ptrCast(&key_col_new);
    const val_ptr_new: [*]const u8 = @ptrCast(&val_col_new);
    const col_ptrs_new = [2][*]const u8{ key_ptr_new, val_ptr_new };
    const insert_result = vm_execute_batch(
        @ptrCast(&new_state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs_new),
        2,
        1,
    );
    try std.testing.expectEqual(@as(u32, 0), insert_result);

    // Verify the new entry is findable
    const lookup = vm_map_get(@ptrCast(&new_state_buf), new_data_offset, 32, 100);
    try std.testing.expectEqual(@as(u32, 1000), lookup);
}

test "slot growth - hashset growth preserves elements" {
    // Build program with one HashSet slot
    var prog = [_]u8{0} ** 56;
    const content = prog[PROGRAM_HASH_PREFIX..];
    content[0] = 0x41; // 'A'
    content[1] = 0x58; // 'X'
    content[2] = 0x45; // 'E'
    content[3] = 0x31; // '1'
    content[4] = 1;
    content[5] = 0;
    content[6] = 1; // num_slots = 1
    content[7] = 1; // num_inputs = 1
    content[8] = 0;
    content[9] = 0;
    const init_len: u16 = 5;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);
    const reduce_len: u16 = 3;
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    // SLOT_DEF slot=0, type_flags=0x01 (HASHSET), cap_lo=4, cap_hi=0 → cap=16
    content[14] = 0x10;
    content[15] = 0;
    content[16] = 0x01; // HASHSET
    content[17] = 4;
    content[18] = 0;
    // BATCH_SET_INSERT slot=0, elem_col=0
    const reduce_start = 14 + init_len;
    content[reduce_start] = 0x30;
    content[reduce_start + 1] = 0;
    content[reduce_start + 2] = 0;

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [4096]u8 align(8) = [_]u8{0} ** 4096;
    std.debug.assert(state_size <= 4096);

    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // Insert 11 elements
    var i: u32 = 0;
    while (i < 11) : (i += 1) {
        var elem_col = [1]u32{i + 1000}; // avoid EMPTY_KEY/TOMBSTONE
        const elem_ptr: [*]const u8 = @ptrCast(&elem_col);
        const col_ptrs = [1][*]const u8{elem_ptr};
        _ = vm_execute_batch(
            @ptrCast(&state_buf),
            @ptrCast(&prog),
            prog.len,
            @ptrCast(&col_ptrs),
            1,
            1,
        );
    }

    // Verify 12th triggers growth
    var elem_col_12 = [1]u32{2000};
    const elem_ptr_12: [*]const u8 = @ptrCast(&elem_col_12);
    const col_ptrs_12 = [1][*]const u8{elem_ptr_12};
    const result_12 = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs_12),
        1,
        1,
    );
    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.NEEDS_GROWTH)), result_12);

    // Grow
    const grown_size = vm_calculate_grown_state_size(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        0,
    );
    var new_state_buf: [16384]u8 align(8) = [_]u8{0} ** 16384;
    std.debug.assert(grown_size <= 16384);

    _ = vm_grow_state(
        @ptrCast(&state_buf),
        @ptrCast(&new_state_buf),
        @ptrCast(&prog),
        prog.len,
        0,
    );

    // Verify new capacity = 32
    const meta_base = STATE_HEADER_SIZE;
    const new_cap = std.mem.readInt(u32, new_state_buf[meta_base + 4 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 32), new_cap);

    // Verify all 11 elements preserved via vm_set_contains
    const new_data_offset = std.mem.readInt(u32, new_state_buf[meta_base..][0..4], .little);
    i = 0;
    while (i < 11) : (i += 1) {
        const contained = vm_set_contains(@ptrCast(&new_state_buf), new_data_offset, 32, i + 1000);
        try std.testing.expectEqual(@as(u32, 1), contained);
    }

    // Verify can insert 12th element now
    const insert_result = vm_execute_batch(
        @ptrCast(&new_state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs_12),
        1,
        1,
    );
    try std.testing.expectEqual(@as(u32, 0), insert_result);
}

// =============================================================================
// Tests — Struct Map
// =============================================================================

/// Build a program with one STRUCT_MAP slot (2 fields: UINT32 + STRING)
/// and BATCH_STRUCT_MAP_UPSERT_LAST that maps both fields.
fn buildStructMapTestProgram(comptime cap_lo: u8, comptime cap_hi: u8) [80]u8 {
    var prog = [_]u8{0} ** 80;
    const content = prog[PROGRAM_HASH_PREFIX..];
    // Magic "CLM1"
    content[0] = 0x41; // 'A'
    content[1] = 0x58; // 'X'
    content[2] = 0x45; // 'E'
    content[3] = 0x31; // '1'
    // Version: 1.0
    content[4] = 1;
    content[5] = 0;
    // num_slots = 1, num_inputs = 3
    content[6] = 1;
    content[7] = 3;
    // reserved
    content[8] = 0;
    content[9] = 0;
    // Init section: SLOT_STRUCT_MAP opcode(1) + slot(1) + type_flags(1) + cap_lo(1) + cap_hi(1)
    //               + num_fields(1) + field_types(2) = 8 bytes
    const init_len: u16 = 8;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);
    // Reduce section: BATCH_STRUCT_MAP_UPSERT_LAST opcode(1) + slot(1) + key_col(1) + num_vals(1)
    //                 + 2x (val_col(1) + field_idx(1)) + num_array_vals(1) = 9 bytes
    const reduce_len: u16 = 9;
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    // Init section at content[14]
    var off: usize = 14;
    // SLOT_STRUCT_MAP
    content[off] = 0x18; // SLOT_STRUCT_MAP opcode
    content[off + 1] = 0; // slot index
    content[off + 2] = @intFromEnum(SlotType.STRUCT_MAP); // type_flags: STRUCT_MAP, no TTL
    content[off + 3] = cap_lo;
    content[off + 4] = cap_hi;
    content[off + 5] = 2; // num_fields = 2
    content[off + 6] = 0; // field 0: UINT32
    content[off + 7] = 4; // field 1: STRING (interned u32)
    off += 8;

    // Reduce section at content[14 + init_len]
    const reduce_start = 14 + init_len;
    // BATCH_STRUCT_MAP_UPSERT_LAST slot=0, key_col=0, num_vals=2
    content[reduce_start] = 0x80;
    content[reduce_start + 1] = 0; // slot
    content[reduce_start + 2] = 0; // key_col
    content[reduce_start + 3] = 2; // num_vals
    // val_col=1, field_idx=0 (UINT32 field)
    content[reduce_start + 4] = 1;
    content[reduce_start + 5] = 0;
    // val_col=2, field_idx=1 (STRING field)
    content[reduce_start + 6] = 2;
    content[reduce_start + 7] = 1;
    // num_array_vals = 0
    content[reduce_start + 8] = 0;

    return prog;
}

test "struct map - init, upsert, and read back" {
    var prog = buildStructMapTestProgram(4, 0);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    try std.testing.expect(state_size > 0);

    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    std.debug.assert(state_size <= 8192);

    const init_result = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);
    try std.testing.expectEqual(@as(u32, 0), init_result);

    // Verify slot metadata
    const meta_base = STATE_HEADER_SIZE;
    const capacity = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    try std.testing.expect(capacity >= 16); // nextPowerOf2(4*2) = 16

    const num_fields = state_buf[meta_base + 13];
    try std.testing.expectEqual(@as(u8, 2), num_fields);

    const row_size: u32 = std.mem.readInt(u16, state_buf[meta_base + 16 ..][0..2], .little);
    // 2 fields: bitset=1 byte, UINT32=4 bytes, STRING=4 bytes => 9 bytes, aligned to 4 => 12
    try std.testing.expectEqual(@as(u32, 12), row_size);

    // Insert 3 entries: key=100 (val=42, str=1001), key=200 (val=99, str=1002), key=300 (val=7, str=1003)
    var key_col = [3]u32{ 100, 200, 300 };
    var val_col = [3]u32{ 42, 99, 7 };
    var str_col = [3]u32{ 1001, 1002, 1003 };
    const key_ptr: [*]const u8 = @ptrCast(&key_col);
    const val_ptr: [*]const u8 = @ptrCast(&val_col);
    const str_ptr: [*]const u8 = @ptrCast(&str_col);
    const col_ptrs = [3][*]const u8{ key_ptr, val_ptr, str_ptr };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        3,
        3,
    );
    try std.testing.expectEqual(@as(u32, 0), exec_result);

    // Verify size = 3
    const size_after = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 3), size_after);

    // Read back via vm_struct_map_get_row_ptr
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const row_off_100 = vm_struct_map_get_row_ptr(
        @ptrCast(&state_buf),
        slot_offset,
        capacity,
        num_fields,
        row_size,
        100,
    );
    try std.testing.expect(row_off_100 != 0xFFFFFFFF);

    // Check bitset: both fields set => byte 0 should have bits 0 and 1 set = 0x03
    try std.testing.expectEqual(@as(u8, 0x03), state_buf[row_off_100]);
    // Field 0 (UINT32) at offset 1 (after 1-byte bitset) => value 42
    const f0_val = std.mem.readInt(u32, state_buf[row_off_100 + 1 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 42), f0_val);
    // Field 1 (STRING) at offset 5 => value 1001
    const f1_val = std.mem.readInt(u32, state_buf[row_off_100 + 5 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 1001), f1_val);

    // Check key=200
    const row_off_200 = vm_struct_map_get_row_ptr(
        @ptrCast(&state_buf),
        slot_offset,
        capacity,
        num_fields,
        row_size,
        200,
    );
    try std.testing.expect(row_off_200 != 0xFFFFFFFF);
    const f0_val_200 = std.mem.readInt(u32, state_buf[row_off_200 + 1 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 99), f0_val_200);

    // Check non-existent key returns NOT_FOUND
    const row_off_missing = vm_struct_map_get_row_ptr(
        @ptrCast(&state_buf),
        slot_offset,
        capacity,
        num_fields,
        row_size,
        999,
    );
    try std.testing.expectEqual(@as(u32, 0xFFFFFFFF), row_off_missing);
}

test "struct map - upsert overwrites existing key" {
    var prog = buildStructMapTestProgram(4, 0);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    const meta_base = STATE_HEADER_SIZE;
    const capacity = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    const num_fields = state_buf[meta_base + 13];
    const row_size: u32 = std.mem.readInt(u16, state_buf[meta_base + 16 ..][0..2], .little);
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    _ = state_size;

    // Insert key=100, val=42, str=1001
    {
        var key_col = [1]u32{100};
        var val_col = [1]u32{42};
        var str_col = [1]u32{1001};
        const col_ptrs = [3][*]const u8{
            @ptrCast(&key_col),
            @ptrCast(&val_col),
            @ptrCast(&str_col),
        };
        _ = vm_execute_batch(
            @ptrCast(&state_buf),
            @ptrCast(&prog),
            prog.len,
            @ptrCast(&col_ptrs),
            3,
            1,
        );
    }

    // Overwrite key=100 with val=99, str=2002
    {
        var key_col = [1]u32{100};
        var val_col = [1]u32{99};
        var str_col = [1]u32{2002};
        const col_ptrs = [3][*]const u8{
            @ptrCast(&key_col),
            @ptrCast(&val_col),
            @ptrCast(&str_col),
        };
        _ = vm_execute_batch(
            @ptrCast(&state_buf),
            @ptrCast(&prog),
            prog.len,
            @ptrCast(&col_ptrs),
            3,
            1,
        );
    }

    // Size should still be 1 (upsert, not double-insert)
    const size_after = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 1), size_after);

    // Values should be overwritten
    const row_off = vm_struct_map_get_row_ptr(
        @ptrCast(&state_buf),
        slot_offset,
        capacity,
        num_fields,
        row_size,
        100,
    );
    try std.testing.expect(row_off != 0xFFFFFFFF);
    const f0_val = std.mem.readInt(u32, state_buf[row_off + 1 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 99), f0_val);
    const f1_val = std.mem.readInt(u32, state_buf[row_off + 5 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 2002), f1_val);
}

test "struct map - iteration" {
    var prog = buildStructMapTestProgram(4, 0);
    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    const meta_base = STATE_HEADER_SIZE;
    const capacity = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    const num_fields = state_buf[meta_base + 13];
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);

    // Insert 3 entries
    var key_col = [3]u32{ 100, 200, 300 };
    var val_col = [3]u32{ 10, 20, 30 };
    var str_col = [3]u32{ 1, 2, 3 };
    const col_ptrs = [3][*]const u8{
        @as([*]const u8, @ptrCast(&key_col)),
        @as([*]const u8, @ptrCast(&val_col)),
        @as([*]const u8, @ptrCast(&str_col)),
    };
    _ = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        3,
        3,
    );

    // Iterate and collect keys
    var found_keys: [3]u32 = undefined;
    var count: u32 = 0;
    var pos = vm_struct_map_iter_start(@ptrCast(&state_buf), slot_offset, capacity, num_fields);
    while (pos < capacity) {
        found_keys[count] = vm_struct_map_iter_key(@ptrCast(&state_buf), slot_offset, num_fields, pos);
        count += 1;
        pos = vm_struct_map_iter_next(@ptrCast(&state_buf), slot_offset, capacity, num_fields, pos);
    }
    try std.testing.expectEqual(@as(u32, 3), count);

    // All 3 keys should be present (order may vary due to hashing)
    var has_100 = false;
    var has_200 = false;
    var has_300 = false;
    for (0..count) |i| {
        if (found_keys[i] == 100) has_100 = true;
        if (found_keys[i] == 200) has_200 = true;
        if (found_keys[i] == 300) has_300 = true;
    }
    try std.testing.expect(has_100);
    try std.testing.expect(has_200);
    try std.testing.expect(has_300);
}

test "struct map - growth preserves entries" {
    // Use small capacity (cap=4 -> actual=16) to force growth
    var prog = buildStructMapTestProgram(4, 0);
    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    const meta_base = STATE_HEADER_SIZE;
    const num_fields = state_buf[meta_base + 13];
    const row_size: u32 = std.mem.readInt(u16, state_buf[meta_base + 16 ..][0..2], .little);

    // Insert 11 entries (70% of 16 = 11.2, so 12th should trigger growth)
    var i: u32 = 0;
    while (i < 11) : (i += 1) {
        var key_col = [1]u32{i + 1000};
        var val_col = [1]u32{i * 10};
        var str_col = [1]u32{i + 5000};
        const col_ptrs = [3][*]const u8{
            @ptrCast(&key_col),
            @ptrCast(&val_col),
            @ptrCast(&str_col),
        };
        const res = vm_execute_batch(
            @ptrCast(&state_buf),
            @ptrCast(&prog),
            prog.len,
            @ptrCast(&col_ptrs),
            3,
            1,
        );
        try std.testing.expectEqual(@as(u32, 0), res);
    }

    // 12th entry should trigger NEEDS_GROWTH
    var key_col_12 = [1]u32{2000};
    var val_col_12 = [1]u32{999};
    var str_col_12 = [1]u32{9999};
    const col_ptrs_12 = [3][*]const u8{
        @ptrCast(&key_col_12),
        @ptrCast(&val_col_12),
        @ptrCast(&str_col_12),
    };
    const result_12 = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs_12),
        3,
        1,
    );
    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.NEEDS_GROWTH)), result_12);

    // Grow
    const grown_size = vm_calculate_grown_state_size(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        0,
    );
    var new_state_buf: [32768]u8 align(8) = [_]u8{0} ** 32768;
    std.debug.assert(grown_size <= 32768);

    const grow_result = vm_grow_state(
        @ptrCast(&state_buf),
        @ptrCast(&new_state_buf),
        @ptrCast(&prog),
        prog.len,
        0,
    );
    try std.testing.expectEqual(@as(u32, 0), grow_result);

    // Verify new capacity = 32
    const new_cap = std.mem.readInt(u32, new_state_buf[meta_base + 4 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 32), new_cap);

    // Verify all 11 entries preserved
    const new_slot_offset = std.mem.readInt(u32, new_state_buf[meta_base..][0..4], .little);
    i = 0;
    while (i < 11) : (i += 1) {
        const row_off = vm_struct_map_get_row_ptr(
            @ptrCast(&new_state_buf),
            new_slot_offset,
            new_cap,
            num_fields,
            row_size,
            i + 1000,
        );
        try std.testing.expect(row_off != 0xFFFFFFFF);
        // Check UINT32 field value
        const f0 = std.mem.readInt(u32, new_state_buf[row_off + 1 ..][0..4], .little);
        try std.testing.expectEqual(i * 10, f0);
        // Check STRING field value
        const f1 = std.mem.readInt(u32, new_state_buf[row_off + 5 ..][0..4], .little);
        try std.testing.expectEqual(i + 5000, f1);
    }

    // Verify can insert 12th element now
    const insert_result = vm_execute_batch(
        @ptrCast(&new_state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs_12),
        3,
        1,
    );
    try std.testing.expectEqual(@as(u32, 0), insert_result);
}

// =============================================================================
// Tests — FOR_EACH block-based execution
// =============================================================================

/// Build a program with one HashMap slot (HASHMAP, LAST strategy) and a
/// FOR_EACH block wrapping MAP_UPSERT_LAST + AGG_COUNT.
/// Columns: 0=type_col(u32), 1=key_col(u32), 2=val_col(u32)
/// Slot 0: HashMap (cap_lo=4 → cap=16), Slot 1: Aggregate COUNT
fn buildForEachTestProgram(type_id: u32) [96]u8 {
    return buildForEachTestProgramMulti(&[_]u32{type_id});
}

/// Multi-match variant: match any of the given type IDs.
fn buildForEachTestProgramMulti(type_ids: []const u32) [96]u8 {
    var prog = [_]u8{0} ** 96;
    const content = prog[PROGRAM_HASH_PREFIX..];
    // Magic "CLM1"
    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    // Version 1.0
    content[4] = 1;
    content[5] = 0;
    // num_slots=2, num_inputs=3
    content[6] = 2;
    content[7] = 3;
    content[8] = 0;
    content[9] = 0;

    // Init section: 2 slot defs
    // SLOT_DEF(HashMap) = 5 bytes + SLOT_DEF(Agg COUNT) = 5 bytes = 10
    const init_len: u16 = 10;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);

    // Reduce section: FOR_EACH header + body
    // Header: opcode(1) + col(1) + match_count(1) + match_ids(4*N) + body_len(2) = 5 + 4*N
    const match_count: u8 = @intCast(type_ids.len);
    const header_len: u16 = 5 + @as(u16, match_count) * 4;
    const body_len: u16 = 6; // MAP_UPSERT_LAST(4) + AGG_COUNT(2)
    const reduce_len: u16 = header_len + body_len;
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    // Init section at content[14]
    var off: usize = 14;
    // Slot 0: HASHMAP, cap_lo=4, cap_hi=0
    content[off] = 0x10; // SLOT_DEF
    content[off + 1] = 0; // slot 0
    content[off + 2] = 0x00; // type_flags: HASHMAP=0
    content[off + 3] = 4; // cap_lo
    content[off + 4] = 0; // cap_hi
    off += 5;
    // Slot 1: AGGREGATE COUNT
    content[off] = 0x10; // SLOT_DEF
    content[off + 1] = 1; // slot 1
    content[off + 2] = 0x02; // type_flags: AGGREGATE=2
    content[off + 3] = @intFromEnum(AggType.COUNT); // aggType in cap_lo
    content[off + 4] = 0; // cap_hi
    off += 5;

    // Reduce section
    const rs = 14 + init_len;
    // FOR_EACH opcode
    content[rs] = 0xE0;
    // col=0 (type_col)
    content[rs + 1] = 0;
    // match_count
    content[rs + 2] = match_count;
    // match_ids as u32 LE
    var ids_off = rs + 3;
    for (type_ids) |tid| {
        content[ids_off] = @truncate(tid);
        content[ids_off + 1] = @truncate(tid >> 8);
        content[ids_off + 2] = @truncate(tid >> 16);
        content[ids_off + 3] = @truncate(tid >> 24);
        ids_off += 4;
    }
    // body_len as u16 LE
    content[ids_off] = @truncate(body_len);
    content[ids_off + 1] = @truncate(body_len >> 8);

    // Body: MAP_UPSERT_LAST(0x22) slot=0 key_col=1 val_col=2
    const body_start = ids_off + 2;
    content[body_start] = 0x22;
    content[body_start + 1] = 0; // slot
    content[body_start + 2] = 1; // key_col
    content[body_start + 3] = 2; // val_col

    // Body: AGG_COUNT(0x41) slot=1
    content[body_start + 4] = 0x41;
    content[body_start + 5] = 1; // slot

    return prog;
}

test "FOR_EACH - basic type filtering" {
    const TYPE_A: u32 = 1001;
    const TYPE_B: u32 = 1002;

    var prog = buildForEachTestProgram(TYPE_A);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    std.debug.assert(state_size <= 8192);

    const init_result = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);
    try std.testing.expectEqual(@as(u32, 0), init_result);

    // Batch with mixed types: 2 events of TYPE_A, 1 event of TYPE_B
    // Only TYPE_A events should be processed (type_id in program = TYPE_A)
    var type_col = [3]u32{ TYPE_A, TYPE_B, TYPE_A };
    var key_col = [3]u32{ 100, 200, 300 };
    var val_col = [3]u32{ 10, 20, 30 };
    const col_ptrs = [3][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&key_col)),
        @as([*]const u8, @ptrCast(&val_col)),
    };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        3,
        3,
    );
    try std.testing.expectEqual(@as(u32, 0), exec_result);

    // HashMap (slot 0): should have 2 entries (keys 100, 300) — key 200 was TYPE_B
    const meta_base = STATE_HEADER_SIZE;
    const map_size = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 2), map_size);

    // Verify key 100 → val 10
    const data_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const cap = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 10), vm_map_get(@ptrCast(&state_buf), data_offset, cap, 100));
    // Verify key 300 → val 30
    try std.testing.expectEqual(@as(u32, 30), vm_map_get(@ptrCast(&state_buf), data_offset, cap, 300));
    // Verify key 200 NOT present (EMPTY_KEY = 0 returned for missing)
    try std.testing.expectEqual(EMPTY_KEY, vm_map_get(@ptrCast(&state_buf), data_offset, cap, 200));

    // Aggregate COUNT (slot 1): should be 2 (only TYPE_A events counted)
    // COUNT is compact 8-byte slot: u64 count at offset 0 (no f64 value prefix)
    const agg_meta_base = STATE_HEADER_SIZE + SLOT_META_SIZE;
    const agg_offset = std.mem.readInt(u32, state_buf[agg_meta_base..][0..4], .little);
    const agg_count: u64 = std.mem.readInt(u64, state_buf[agg_offset..][0..8], .little);
    try std.testing.expectEqual(@as(u64, 2), agg_count);
}

test "FOR_EACH - all events match" {
    const TYPE_A: u32 = 1001;
    var prog = buildForEachTestProgram(TYPE_A);

    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // All 3 events match TYPE_A
    var type_col = [3]u32{ TYPE_A, TYPE_A, TYPE_A };
    var key_col = [3]u32{ 10, 20, 30 };
    var val_col = [3]u32{ 100, 200, 300 };
    const col_ptrs = [3][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&key_col)),
        @as([*]const u8, @ptrCast(&val_col)),
    };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        3,
        3,
    );
    try std.testing.expectEqual(@as(u32, 0), exec_result);

    // All 3 should be in the map
    const meta_base = STATE_HEADER_SIZE;
    const map_size = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 3), map_size);

    // Count should be 3
    const agg_meta_base = STATE_HEADER_SIZE + SLOT_META_SIZE;
    const agg_offset = std.mem.readInt(u32, state_buf[agg_meta_base..][0..4], .little);
    const agg_count: u64 = std.mem.readInt(u64, state_buf[agg_offset..][0..8], .little);
    try std.testing.expectEqual(@as(u64, 3), agg_count);
}

test "FOR_EACH - no events match" {
    const TYPE_A: u32 = 1001;
    const TYPE_B: u32 = 1002;
    var prog = buildForEachTestProgram(TYPE_A);

    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // No events match — all are TYPE_B
    var type_col = [2]u32{ TYPE_B, TYPE_B };
    var key_col = [2]u32{ 10, 20 };
    var val_col = [2]u32{ 100, 200 };
    const col_ptrs = [3][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&key_col)),
        @as([*]const u8, @ptrCast(&val_col)),
    };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        3,
        2,
    );
    try std.testing.expectEqual(@as(u32, 0), exec_result);

    // Map should be empty
    const meta_base = STATE_HEADER_SIZE;
    const map_size = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 0), map_size);

    // Count should be 0
    const agg_meta_base = STATE_HEADER_SIZE + SLOT_META_SIZE;
    const agg_offset = std.mem.readInt(u32, state_buf[agg_meta_base..][0..4], .little);
    const agg_count: u64 = std.mem.readInt(u64, state_buf[agg_offset..][0..8], .little);
    try std.testing.expectEqual(@as(u64, 0), agg_count);
}

test "FOR_EACH - multi-match (match_count=2)" {
    const TYPE_A: u32 = 1001;
    const TYPE_B: u32 = 1002;
    const TYPE_C: u32 = 1003;

    // Match both TYPE_A and TYPE_B, exclude TYPE_C
    var prog = buildForEachTestProgramMulti(&[_]u32{ TYPE_A, TYPE_B });

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    std.debug.assert(state_size <= 8192);

    const init_result = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);
    try std.testing.expectEqual(@as(u32, 0), init_result);

    // 4 events: TYPE_A, TYPE_B, TYPE_C, TYPE_A
    // Only TYPE_A and TYPE_B should be processed (3 out of 4)
    var type_col = [4]u32{ TYPE_A, TYPE_B, TYPE_C, TYPE_A };
    var key_col = [4]u32{ 100, 200, 300, 400 };
    var val_col = [4]u32{ 10, 20, 30, 40 };
    const col_ptrs = [3][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&key_col)),
        @as([*]const u8, @ptrCast(&val_col)),
    };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        3,
        4,
    );
    try std.testing.expectEqual(@as(u32, 0), exec_result);

    // HashMap (slot 0): should have 3 entries (keys 100, 200, 400) — key 300 was TYPE_C
    const meta_base = STATE_HEADER_SIZE;
    const map_size = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 3), map_size);

    const data_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const cap = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 10), vm_map_get(@ptrCast(&state_buf), data_offset, cap, 100));
    try std.testing.expectEqual(@as(u32, 20), vm_map_get(@ptrCast(&state_buf), data_offset, cap, 200));
    try std.testing.expectEqual(@as(u32, 40), vm_map_get(@ptrCast(&state_buf), data_offset, cap, 400));
    try std.testing.expectEqual(EMPTY_KEY, vm_map_get(@ptrCast(&state_buf), data_offset, cap, 300));

    // Aggregate COUNT (slot 1): should be 3 (TYPE_A×2 + TYPE_B×1)
    const agg_meta_base = STATE_HEADER_SIZE + SLOT_META_SIZE;
    const agg_offset = std.mem.readInt(u32, state_buf[agg_meta_base..][0..4], .little);
    const agg_count: u64 = std.mem.readInt(u64, state_buf[agg_offset..][0..8], .little);
    try std.testing.expectEqual(@as(u64, 3), agg_count);
}

test "FOR_EACH - multi-match (match_count=3) all types match" {
    const TYPE_A: u32 = 10;
    const TYPE_B: u32 = 20;
    const TYPE_C: u32 = 30;

    var prog = buildForEachTestProgramMulti(&[_]u32{ TYPE_A, TYPE_B, TYPE_C });

    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    var type_col = [3]u32{ TYPE_A, TYPE_B, TYPE_C };
    var key_col = [3]u32{ 1, 2, 3 };
    var val_col = [3]u32{ 111, 222, 333 };
    const col_ptrs = [3][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&key_col)),
        @as([*]const u8, @ptrCast(&val_col)),
    };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        3,
        3,
    );
    try std.testing.expectEqual(@as(u32, 0), exec_result);

    // All 3 should be in the map
    const meta_base = STATE_HEADER_SIZE;
    const map_size = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 3), map_size);

    // Count should be 3
    const agg_meta_base = STATE_HEADER_SIZE + SLOT_META_SIZE;
    const agg_offset = std.mem.readInt(u32, state_buf[agg_meta_base..][0..4], .little);
    const agg_count: u64 = std.mem.readInt(u64, state_buf[agg_offset..][0..8], .little);
    try std.testing.expectEqual(@as(u64, 3), agg_count);
}

// =============================================================================
// Tests — FOR_EACH with STRUCT_MAP
// =============================================================================

/// Build a program with:
///   Slot 0: STRUCT_MAP (2 fields: UINT32 + STRING, cap=16)
/// Reduce: FOR_EACH(type_col=0, type_id) { STRUCT_MAP_UPSERT_LAST }
/// Columns: 0=type, 1=key, 2=uint32_val, 3=string_val
fn buildBlockStructMapTestProgram(type_id: u32) [96]u8 {
    var prog = [_]u8{0} ** 96;
    const content = prog[PROGRAM_HASH_PREFIX..];
    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    content[4] = 1;
    content[5] = 0;
    content[6] = 1; // num_slots
    content[7] = 4; // num_inputs
    content[8] = 0;
    content[9] = 0;

    // Init: SLOT_STRUCT_MAP (8 bytes)
    const init_len: u16 = 8;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);

    // Reduce: FOR_EACH(9) + body: STRUCT_MAP_UPSERT_LAST(9) = 18
    const body_len: u16 = 9; // op(1)+slot(1)+key_col(1)+num_vals(1)+2*(val_col+field_idx)+num_array_vals(1)
    const reduce_len: u16 = 9 + body_len;
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    // Init section
    var off: usize = 14;
    content[off] = 0x18; // SLOT_STRUCT_MAP
    content[off + 1] = 0;
    content[off + 2] = @intFromEnum(SlotType.STRUCT_MAP); // STRUCT_MAP
    content[off + 3] = 4; // cap_lo
    content[off + 4] = 0;
    content[off + 5] = 2; // num_fields
    content[off + 6] = 0; // UINT32
    content[off + 7] = 4; // STRING
    off += 8;

    // Reduce section
    const rs = 14 + init_len;
    content[rs] = 0xE0; // FOR_EACH
    content[rs + 1] = 0; // col (type_col)
    content[rs + 2] = 1; // match_count
    content[rs + 3] = @truncate(type_id);
    content[rs + 4] = @truncate(type_id >> 8);
    content[rs + 5] = @truncate(type_id >> 16);
    content[rs + 6] = @truncate(type_id >> 24);
    content[rs + 7] = @truncate(body_len);
    content[rs + 8] = @truncate(body_len >> 8);

    // Body: STRUCT_MAP_UPSERT_LAST
    const bs = rs + 9;
    content[bs] = 0x80;
    content[bs + 1] = 0; // slot
    content[bs + 2] = 1; // key_col
    content[bs + 3] = 2; // num_vals
    content[bs + 4] = 2; // val_col=2, field_idx=0
    content[bs + 5] = 0;
    content[bs + 6] = 3; // val_col=3, field_idx=1
    content[bs + 7] = 1;
    content[bs + 8] = 0; // num_array_vals = 0

    return prog;
}

test "FOR_EACH - struct map upsert with type filtering" {
    const TYPE_A: u32 = 1001;
    const TYPE_B: u32 = 1002;
    var prog = buildBlockStructMapTestProgram(TYPE_A);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    std.debug.assert(state_size <= 8192);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // 3 events: TYPE_A, TYPE_B, TYPE_A — only 2 should be inserted
    var type_col = [3]u32{ TYPE_A, TYPE_B, TYPE_A };
    var key_col = [3]u32{ 100, 200, 300 };
    var val_col = [3]u32{ 42, 99, 7 };
    var str_col = [3]u32{ 1001, 1002, 1003 };
    const col_ptrs = [4][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&key_col)),
        @as([*]const u8, @ptrCast(&val_col)),
        @as([*]const u8, @ptrCast(&str_col)),
    };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        4,
        3,
    );
    try std.testing.expectEqual(@as(u32, 0), exec_result);

    // Struct map should have 2 entries (keys 100, 300)
    const meta_base = STATE_HEADER_SIZE;
    const sm_size = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 2), sm_size);

    // Verify key 100 present
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const capacity = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    const num_fields = state_buf[meta_base + 13];
    const row_size: u32 = std.mem.readInt(u16, state_buf[meta_base + 16 ..][0..2], .little);

    const row_off = vm_struct_map_get_row_ptr(
        @ptrCast(&state_buf),
        slot_offset,
        capacity,
        num_fields,
        row_size,
        100,
    );
    try std.testing.expect(row_off != 0xFFFFFFFF);
    const f0_val = std.mem.readInt(u32, state_buf[row_off + 1 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 42), f0_val);

    // Verify key 200 NOT present (was TYPE_B)
    const row_off_200 = vm_struct_map_get_row_ptr(
        @ptrCast(&state_buf),
        slot_offset,
        capacity,
        num_fields,
        row_size,
        200,
    );
    try std.testing.expectEqual(@as(u32, 0xFFFFFFFF), row_off_200);
}

// =============================================================================
// Tests — FLAT_MAP
// =============================================================================

/// Build a program with:
///   Slot 0: STRUCT_MAP (2 fields: UINT32 + STRING, cap=16)
/// Reduce: FOR_EACH(type_col=0, type_id) {
///            FLAT_MAP(offsets_col=1, parent_ts_col=0xFF) {
///              STRUCT_MAP_UPSERT_LAST(key_col=2, ...)
///            }
///          }
/// Columns: 0=type, 1=offsets(u32), 2=child_key(u32), 3=child_val0(u32), 4=child_val1(u32)
fn buildFlatMapTestProgram(type_id: u32) [112]u8 {
    var prog = [_]u8{0} ** 112;
    const content = prog[PROGRAM_HASH_PREFIX..];
    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    content[4] = 1;
    content[5] = 0;
    content[6] = 1; // num_slots
    content[7] = 5; // num_inputs
    content[8] = 0;
    content[9] = 0;

    const init_len: u16 = 8; // SLOT_STRUCT_MAP
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);

    // Body of FLAT_MAP: STRUCT_MAP_UPSERT_LAST = 9 bytes (8 + num_array_vals)
    const inner_body_len: u16 = 9;
    // FLAT_MAP header: op(1)+offsets_col(1)+parent_ts_col(1)+inner_body_len(2) = 5
    const flat_map_total: u16 = 5 + inner_body_len;
    // FOR_EACH header: 9 bytes (match_count=1), body = flat_map_total
    const reduce_len: u16 = 9 + flat_map_total;
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    // Init section
    var off: usize = 14;
    content[off] = 0x18;
    content[off + 1] = 0;
    content[off + 2] = 0x05;
    content[off + 3] = 4; // cap_lo
    content[off + 4] = 0;
    content[off + 5] = 2; // num_fields
    content[off + 6] = 0; // UINT32
    content[off + 7] = 4; // STRING
    off += 8;

    // Reduce section
    const rs = 14 + init_len;
    // FOR_EACH
    content[rs] = 0xE0;
    content[rs + 1] = 0; // col (type_col)
    content[rs + 2] = 1; // match_count
    content[rs + 3] = @truncate(type_id);
    content[rs + 4] = @truncate(type_id >> 8);
    content[rs + 5] = @truncate(type_id >> 16);
    content[rs + 6] = @truncate(type_id >> 24);
    content[rs + 7] = @truncate(flat_map_total);
    content[rs + 8] = @truncate(flat_map_total >> 8);

    // FLAT_MAP
    const fm = rs + 9;
    content[fm] = 0xE1; // FLAT_MAP
    content[fm + 1] = 1; // offsets_col
    content[fm + 2] = 0xFF; // parent_ts_col (unused)
    content[fm + 3] = @truncate(inner_body_len);
    content[fm + 4] = @truncate(inner_body_len >> 8);

    // Inner body: STRUCT_MAP_UPSERT_LAST
    const ib = fm + 5;
    content[ib] = 0x80;
    content[ib + 1] = 0; // slot
    content[ib + 2] = 2; // key_col (child column)
    content[ib + 3] = 2; // num_vals
    content[ib + 4] = 3; // val_col=3, field_idx=0
    content[ib + 5] = 0;
    content[ib + 6] = 4; // val_col=4, field_idx=1
    content[ib + 7] = 1;
    content[ib + 8] = 0; // num_array_vals = 0

    return prog;
}

test "FLAT_MAP - basic: 2 parents with children" {
    const TYPE_A: u32 = 1001;
    var prog = buildFlatMapTestProgram(TYPE_A);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    std.debug.assert(state_size <= 8192);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // 2 parent events (both TYPE_A):
    //   Parent 0: 2 children (offsets[0]=0, offsets[1]=2)
    //   Parent 1: 1 child   (offsets[1]=2, offsets[2]=3)
    var type_col = [2]u32{ TYPE_A, TYPE_A };
    // Offsets: [0, 2, 3] — need batch_len+1 entries
    var offsets_col = [3]u32{ 0, 2, 3 };
    // Child columns (3 total children)
    var child_key_col = [3]u32{ 100, 200, 300 };
    var child_val0_col = [3]u32{ 10, 20, 30 };
    var child_val1_col = [3]u32{ 1001, 1002, 1003 };

    const col_ptrs = [5][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&offsets_col)),
        @as([*]const u8, @ptrCast(&child_key_col)),
        @as([*]const u8, @ptrCast(&child_val0_col)),
        @as([*]const u8, @ptrCast(&child_val1_col)),
    };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        5,
        2, // batch_len = 2 parent events
    );
    try std.testing.expectEqual(@as(u32, 0), exec_result);

    // Should have 3 entries in struct map
    const meta_base = STATE_HEADER_SIZE;
    const sm_size = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 3), sm_size);

    // Verify all 3 child keys present
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const capacity = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    const num_fields = state_buf[meta_base + 13];
    const row_size: u32 = std.mem.readInt(u16, state_buf[meta_base + 16 ..][0..2], .little);

    // Key 100 → val0=10, val1=1001
    const row_100 = vm_struct_map_get_row_ptr(@ptrCast(&state_buf), slot_offset, capacity, num_fields, row_size, 100);
    try std.testing.expect(row_100 != 0xFFFFFFFF);
    try std.testing.expectEqual(@as(u32, 10), std.mem.readInt(u32, state_buf[row_100 + 1 ..][0..4], .little));
    try std.testing.expectEqual(@as(u32, 1001), std.mem.readInt(u32, state_buf[row_100 + 5 ..][0..4], .little));

    // Key 200 → val0=20, val1=1002
    const row_200 = vm_struct_map_get_row_ptr(@ptrCast(&state_buf), slot_offset, capacity, num_fields, row_size, 200);
    try std.testing.expect(row_200 != 0xFFFFFFFF);
    try std.testing.expectEqual(@as(u32, 20), std.mem.readInt(u32, state_buf[row_200 + 1 ..][0..4], .little));

    // Key 300 → val0=30, val1=1003
    const row_300 = vm_struct_map_get_row_ptr(@ptrCast(&state_buf), slot_offset, capacity, num_fields, row_size, 300);
    try std.testing.expect(row_300 != 0xFFFFFFFF);
    try std.testing.expectEqual(@as(u32, 30), std.mem.readInt(u32, state_buf[row_300 + 1 ..][0..4], .little));
}

test "FLAT_MAP - empty parent (zero children)" {
    const TYPE_A: u32 = 1001;
    var prog = buildFlatMapTestProgram(TYPE_A);

    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // 2 parents: first has 0 children, second has 1 child
    var type_col = [2]u32{ TYPE_A, TYPE_A };
    var offsets_col = [3]u32{ 0, 0, 1 }; // parent 0: [0,0), parent 1: [0,1)
    var child_key_col = [1]u32{500};
    var child_val0_col = [1]u32{55};
    var child_val1_col = [1]u32{9999};

    const col_ptrs = [5][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&offsets_col)),
        @as([*]const u8, @ptrCast(&child_key_col)),
        @as([*]const u8, @ptrCast(&child_val0_col)),
        @as([*]const u8, @ptrCast(&child_val1_col)),
    };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        5,
        2,
    );
    try std.testing.expectEqual(@as(u32, 0), exec_result);

    // Should have 1 entry (only from parent 1's child)
    const meta_base = STATE_HEADER_SIZE;
    const sm_size = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 1), sm_size);
}

test "FLAT_MAP - key collision: last child wins" {
    const TYPE_A: u32 = 1001;
    var prog = buildFlatMapTestProgram(TYPE_A);

    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // 1 parent with 2 children that have the SAME key
    var type_col = [1]u32{TYPE_A};
    var offsets_col = [2]u32{ 0, 2 };
    var child_key_col = [2]u32{ 100, 100 }; // same key
    var child_val0_col = [2]u32{ 10, 99 }; // different values — last wins
    var child_val1_col = [2]u32{ 1001, 2002 };

    const col_ptrs = [5][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&offsets_col)),
        @as([*]const u8, @ptrCast(&child_key_col)),
        @as([*]const u8, @ptrCast(&child_val0_col)),
        @as([*]const u8, @ptrCast(&child_val1_col)),
    };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        5,
        1,
    );
    try std.testing.expectEqual(@as(u32, 0), exec_result);

    // Should have 1 entry (both children had same key)
    const meta_base = STATE_HEADER_SIZE;
    const sm_size = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 1), sm_size);

    // Last child wins: val0=99, val1=2002
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const capacity = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    const num_fields = state_buf[meta_base + 13];
    const row_size: u32 = std.mem.readInt(u16, state_buf[meta_base + 16 ..][0..2], .little);
    const row_off = vm_struct_map_get_row_ptr(@ptrCast(&state_buf), slot_offset, capacity, num_fields, row_size, 100);
    try std.testing.expect(row_off != 0xFFFFFFFF);
    try std.testing.expectEqual(@as(u32, 99), std.mem.readInt(u32, state_buf[row_off + 1 ..][0..4], .little));
    try std.testing.expectEqual(@as(u32, 2002), std.mem.readInt(u32, state_buf[row_off + 5 ..][0..4], .little));
}

test "FLAT_MAP - type filtering skips non-matching parents" {
    const TYPE_A: u32 = 1001;
    const TYPE_B: u32 = 1002;
    var prog = buildFlatMapTestProgram(TYPE_A);

    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // 3 parents: TYPE_A (2 children), TYPE_B (1 child), TYPE_A (1 child)
    // TYPE_B parent's children should be skipped
    var type_col = [3]u32{ TYPE_A, TYPE_B, TYPE_A };
    var offsets_col = [4]u32{ 0, 2, 3, 4 };
    // 4 total children, but child at index 2 belongs to TYPE_B parent
    var child_key_col = [4]u32{ 100, 200, 999, 300 };
    var child_val0_col = [4]u32{ 10, 20, 99, 30 };
    var child_val1_col = [4]u32{ 1001, 1002, 9999, 1003 };

    const col_ptrs = [5][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&offsets_col)),
        @as([*]const u8, @ptrCast(&child_key_col)),
        @as([*]const u8, @ptrCast(&child_val0_col)),
        @as([*]const u8, @ptrCast(&child_val1_col)),
    };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        5,
        3,
    );
    try std.testing.expectEqual(@as(u32, 0), exec_result);

    // Should have 3 entries (keys 100, 200, 300) — NOT key 999
    const meta_base = STATE_HEADER_SIZE;
    const sm_size = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 3), sm_size);

    // Verify key 999 NOT present
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const capacity = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    const num_fields = state_buf[meta_base + 13];
    const row_size: u32 = std.mem.readInt(u16, state_buf[meta_base + 16 ..][0..2], .little);
    const row_999 = vm_struct_map_get_row_ptr(@ptrCast(&state_buf), slot_offset, capacity, num_fields, row_size, 999);
    try std.testing.expectEqual(@as(u32, 0xFFFFFFFF), row_999);

    // Verify key 300 IS present
    const row_300 = vm_struct_map_get_row_ptr(@ptrCast(&state_buf), slot_offset, capacity, num_fields, row_size, 300);
    try std.testing.expect(row_300 != 0xFFFFFFFF);
}

test "FLAT_MAP - growth: small capacity triggers NEEDS_GROWTH" {
    const TYPE_A: u32 = 1001;
    // Minimum struct map capacity = 16 (nextPowerOf2 floors at 16), max_size = 16*7/10 = 11
    // Need 12 unique keys to trigger NEEDS_GROWTH
    var prog = buildFlatMapTestProgram(TYPE_A); // cap=16 (from cap_lo=4 → nextPowerOf2(8)=16)

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    std.debug.assert(state_size <= 8192);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // 1 parent with 12 children (12 unique keys → exceeds max_size=11)
    var type_col = [1]u32{TYPE_A};
    var offsets_col = [2]u32{ 0, 12 };
    var child_key_col = [12]u32{ 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200 };
    var child_val0_col = [12]u32{ 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120 };
    var child_val1_col = [12]u32{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };

    const col_ptrs = [5][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&offsets_col)),
        @as([*]const u8, @ptrCast(&child_key_col)),
        @as([*]const u8, @ptrCast(&child_val0_col)),
        @as([*]const u8, @ptrCast(&child_val1_col)),
    };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        5,
        1,
    );
    // Should trigger NEEDS_GROWTH (error code 5) because 12 unique keys > max_size=11
    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.NEEDS_GROWTH)), exec_result);
}

// =============================================================================
// Tests — FLAT_MAP + event-level ops in same FOR_EACH block
// =============================================================================

/// Build a program with:
///   Slot 0: STRUCT_MAP (2 fields: UINT32 + STRING, cap=16) — updated via FLAT_MAP
///   Slot 1: AGGREGATE SUM — updated at event level (non-flat)
/// FOR_EACH block body: AGG_SUM(slot=1, val_col=2) + FLAT_MAP { STRUCT_MAP_UPSERT_LAST }
/// Columns: 0=type, 1=offsets(u32), 2=parent_amount(f64), 3=child_key(u32), 4=child_val0(u32), 5=child_val1(u32)
fn buildMixedBlockTestProgram(type_id: u32) [160]u8 {
    var prog = [_]u8{0} ** 160;
    const content = prog[PROGRAM_HASH_PREFIX..];
    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    content[4] = 1;
    content[5] = 0;
    content[6] = 2; // num_slots (struct map + aggregate)
    content[7] = 6; // num_inputs
    content[8] = 0;
    content[9] = 0;

    // Init section: SLOT_STRUCT_MAP(8) + SLOT_DEF for aggregate(5)
    const init_len: u16 = 8 + 5;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);

    // Body = AGG_SUM(3) + FLAT_MAP(5 + 9)
    const inner_body_len: u16 = 9; // STRUCT_MAP_UPSERT_LAST (8 + num_array_vals)
    const flat_map_total: u16 = 5 + inner_body_len;
    const agg_op_len: u16 = 3; // AGG_SUM: op + slot + val_col
    const reduce_len: u16 = 9 + agg_op_len + flat_map_total; // FOR_EACH header(9) + body
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    // Init section
    var off: usize = 14;
    // Slot 0: SLOT_STRUCT_MAP
    content[off] = 0x18;
    content[off + 1] = 0; // slot
    content[off + 2] = @intFromEnum(SlotType.STRUCT_MAP); // STRUCT_MAP
    content[off + 3] = 4; // cap_lo
    content[off + 4] = 0;
    content[off + 5] = 2; // num_fields
    content[off + 6] = 0; // UINT32
    content[off + 7] = 4; // STRING
    off += 8;

    // Slot 1: SLOT_DEF for AGGREGATE
    content[off] = @intFromEnum(Opcode.SLOT_DEF);
    content[off + 1] = 1; // slot
    content[off + 2] = @intFromEnum(SlotType.AGGREGATE); // type_flags: AGGREGATE=3
    content[off + 3] = 0; // cap_lo (unused for aggregate)
    content[off + 4] = 0; // cap_hi
    off += 5;

    // Reduce section
    const rs = 14 + init_len;
    // FOR_EACH header
    content[rs] = 0xE0;
    content[rs + 1] = 0; // col (type_col)
    content[rs + 2] = 1; // match_count
    content[rs + 3] = @truncate(type_id);
    content[rs + 4] = @truncate(type_id >> 8);
    content[rs + 5] = @truncate(type_id >> 16);
    content[rs + 6] = @truncate(type_id >> 24);
    const body_total = agg_op_len + flat_map_total;
    content[rs + 7] = @truncate(body_total);
    content[rs + 8] = @truncate(body_total >> 8);

    // Body starts after FOR_EACH header
    const body_start = rs + 9;

    // AGG_SUM: op(0x40) + slot(1) + val_col(2, parent_amount as f64)
    content[body_start] = 0x40; // AGG_SUM
    content[body_start + 1] = 1; // slot 1
    content[body_start + 2] = 2; // val_col = 2 (parent_amount)

    // FLAT_MAP
    const fm = body_start + 3;
    content[fm] = 0xE1; // FLAT_MAP
    content[fm + 1] = 1; // offsets_col
    content[fm + 2] = 0xFF; // parent_ts_col (unused)
    content[fm + 3] = @truncate(inner_body_len);
    content[fm + 4] = @truncate(inner_body_len >> 8);

    // Inner: STRUCT_MAP_UPSERT_LAST
    const ib = fm + 5;
    content[ib] = 0x80;
    content[ib + 1] = 0; // slot 0
    content[ib + 2] = 3; // key_col (child column)
    content[ib + 3] = 2; // num_vals
    content[ib + 4] = 4; // val_col=4, field_idx=0
    content[ib + 5] = 0;
    content[ib + 6] = 5; // val_col=5, field_idx=1
    content[ib + 7] = 1;
    content[ib + 8] = 0; // num_array_vals = 0

    return prog;
}

test "FLAT_MAP + event-level AGG_SUM in same FOR_EACH block" {
    const TYPE_A: u32 = 1001;
    var prog = buildMixedBlockTestProgram(TYPE_A);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    std.debug.assert(state_size <= 8192);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // 2 parents with amounts, each with children
    var type_col = [2]u32{ TYPE_A, TYPE_A };
    var offsets_col = [3]u32{ 0, 2, 3 }; // parent 0: 2 children, parent 1: 1 child
    // Parent amounts (f64) — slot 1 AGG_SUM sums these
    var parent_amounts: [2]f64 = .{ 100.0, 250.0 };
    // Child columns
    var child_key_col = [3]u32{ 10, 20, 30 };
    var child_val0_col = [3]u32{ 1, 2, 3 };
    var child_val1_col = [3]u32{ 11, 22, 33 };

    const col_ptrs = [6][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&offsets_col)),
        @as([*]const u8, @ptrCast(&parent_amounts)),
        @as([*]const u8, @ptrCast(&child_key_col)),
        @as([*]const u8, @ptrCast(&child_val0_col)),
        @as([*]const u8, @ptrCast(&child_val1_col)),
    };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        6,
        2,
    );
    try std.testing.expectEqual(@as(u32, 0), exec_result);

    // Check struct map: 3 entries from FLAT_MAP
    const meta0_base = STATE_HEADER_SIZE;
    const sm_size = std.mem.readInt(u32, state_buf[meta0_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 3), sm_size);

    // Check aggregate: sum of parent amounts = 100.0 + 250.0 = 350.0
    const meta1_base = STATE_HEADER_SIZE + SLOT_META_SIZE;
    const agg_offset = std.mem.readInt(u32, state_buf[meta1_base..][0..4], .little);
    const agg_val: f64 = @bitCast(std.mem.readInt(u64, state_buf[agg_offset..][0..8], .little));
    try std.testing.expectApproxEqAbs(@as(f64, 350.0), agg_val, 0.001);
}

// =============================================================================
// Tests — Nested FLAT_MAP (depth-2)
// =============================================================================

/// Build a program with nested FLAT_MAP (depth-2):
///   FOR_EACH { FLAT_MAP(outer) { FLAT_MAP(inner) { STRUCT_MAP_UPSERT_LAST } } }
///
///   Slot 0: STRUCT_MAP (2 fields: UINT32 + STRING, cap=16)
///   Columns: 0=type, 1=outer_offsets, 2=inner_offsets, 3=leaf_key, 4=leaf_val0, 5=leaf_val1
///
///   Event → outer children (groups) → inner children (items) → struct map
fn buildNestedFlatMapTestProgram(type_id: u32) [128]u8 {
    var prog = [_]u8{0} ** 128;
    const content = prog[PROGRAM_HASH_PREFIX..];
    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    content[4] = 1;
    content[5] = 0;
    content[6] = 1; // num_slots
    content[7] = 6; // num_inputs
    content[8] = 0;
    content[9] = 0;

    const init_len: u16 = 8; // SLOT_STRUCT_MAP

    // Inner body: STRUCT_MAP_UPSERT_LAST = 9 bytes (8 + num_array_vals)
    const inner_body_len: u16 = 9;
    // Inner FLAT_MAP: op(1)+offsets_col(1)+parent_ts_col(1)+inner_body_len(2) = 5 + inner_body_len
    const inner_flat_map_total: u16 = 5 + inner_body_len;
    // Outer FLAT_MAP: op(1)+offsets_col(1)+parent_ts_col(1)+inner_body_len(2) = 5 + inner_flat_map_total
    const outer_flat_map_total: u16 = 5 + inner_flat_map_total;
    // FOR_EACH: header(9) + outer_flat_map_total
    const reduce_len: u16 = 9 + outer_flat_map_total;

    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    // Init section
    var off: usize = 14;
    content[off] = 0x18; // SLOT_STRUCT_MAP
    content[off + 1] = 0;
    content[off + 2] = 0x05;
    content[off + 3] = 4; // cap_lo → capacity = nextPowerOf2(8) = 8
    content[off + 4] = 0;
    content[off + 5] = 2; // num_fields
    content[off + 6] = 0; // UINT32
    content[off + 7] = 4; // STRING
    off += 8;

    // Reduce section
    const rs: usize = 14 + init_len;
    // FOR_EACH
    content[rs] = 0xE0;
    content[rs + 1] = 0; // col (type_col)
    content[rs + 2] = 1; // match_count
    content[rs + 3] = @truncate(type_id);
    content[rs + 4] = @truncate(type_id >> 8);
    content[rs + 5] = @truncate(type_id >> 16);
    content[rs + 6] = @truncate(type_id >> 24);
    content[rs + 7] = @truncate(outer_flat_map_total);
    content[rs + 8] = @truncate(outer_flat_map_total >> 8);

    // Outer FLAT_MAP
    const ofm = rs + 9;
    content[ofm] = 0xE1;
    content[ofm + 1] = 1; // outer offsets_col
    content[ofm + 2] = 0xFF; // parent_ts_col (unused)
    content[ofm + 3] = @truncate(inner_flat_map_total);
    content[ofm + 4] = @truncate(inner_flat_map_total >> 8);

    // Inner FLAT_MAP (body of outer)
    const ifm = ofm + 5;
    content[ifm] = 0xE1;
    content[ifm + 1] = 2; // inner offsets_col
    content[ifm + 2] = 0xFF; // parent_ts_col (unused)
    content[ifm + 3] = @truncate(inner_body_len);
    content[ifm + 4] = @truncate(inner_body_len >> 8);

    // Inner body: STRUCT_MAP_UPSERT_LAST
    const ib = ifm + 5;
    content[ib] = 0x80;
    content[ib + 1] = 0; // slot
    content[ib + 2] = 3; // key_col (leaf key)
    content[ib + 3] = 2; // num_vals
    content[ib + 4] = 4; // val_col=4
    content[ib + 5] = 0; // field_idx=0
    content[ib + 6] = 5; // val_col=5
    content[ib + 7] = 1; // field_idx=1
    content[ib + 8] = 0; // num_array_vals = 0

    return prog;
}

test "Nested FLAT_MAP - depth-2 (groups → items → struct map)" {
    const TYPE_A: u32 = 1001;
    var prog = buildNestedFlatMapTestProgram(TYPE_A);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    std.debug.assert(state_size <= 8192);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // 1 parent event with 2 groups (outer children):
    //   Group 0: 2 items (inner children)
    //   Group 1: 1 item
    // So: 1 event → 2 groups → 3 items total
    var type_col = [1]u32{TYPE_A};

    // Outer offsets: event 0 → groups [0, 2)
    var outer_offsets = [2]u32{ 0, 2 };

    // Inner offsets: group 0 → items [0, 2), group 1 → items [2, 3)
    var inner_offsets = [3]u32{ 0, 2, 3 };

    // Leaf columns (3 items)
    var leaf_key_col = [3]u32{ 100, 200, 300 };
    var leaf_val0_col = [3]u32{ 10, 20, 30 };
    var leaf_val1_col = [3]u32{ 1001, 1002, 1003 };

    const col_ptrs = [6][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&outer_offsets)),
        @as([*]const u8, @ptrCast(&inner_offsets)),
        @as([*]const u8, @ptrCast(&leaf_key_col)),
        @as([*]const u8, @ptrCast(&leaf_val0_col)),
        @as([*]const u8, @ptrCast(&leaf_val1_col)),
    };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        6,
        1,
    );
    try std.testing.expectEqual(@as(u32, 0), exec_result);

    // Should have 3 entries in struct map
    const meta_base = STATE_HEADER_SIZE;
    const sm_size = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 3), sm_size);

    // Verify all 3 leaf keys present with correct values
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const capacity = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    const num_fields = state_buf[meta_base + 13];
    const row_size: u32 = std.mem.readInt(u16, state_buf[meta_base + 16 ..][0..2], .little);

    // Key 100 from group 0, item 0
    const row_100 = vm_struct_map_get_row_ptr(@ptrCast(&state_buf), slot_offset, capacity, num_fields, row_size, 100);
    try std.testing.expect(row_100 != 0xFFFFFFFF);
    try std.testing.expectEqual(@as(u32, 10), std.mem.readInt(u32, state_buf[row_100 + 1 ..][0..4], .little));

    // Key 200 from group 0, item 1
    const row_200 = vm_struct_map_get_row_ptr(@ptrCast(&state_buf), slot_offset, capacity, num_fields, row_size, 200);
    try std.testing.expect(row_200 != 0xFFFFFFFF);
    try std.testing.expectEqual(@as(u32, 20), std.mem.readInt(u32, state_buf[row_200 + 1 ..][0..4], .little));

    // Key 300 from group 1, item 0
    const row_300 = vm_struct_map_get_row_ptr(@ptrCast(&state_buf), slot_offset, capacity, num_fields, row_size, 300);
    try std.testing.expect(row_300 != 0xFFFFFFFF);
    try std.testing.expectEqual(@as(u32, 30), std.mem.readInt(u32, state_buf[row_300 + 1 ..][0..4], .little));
}

// =============================================================================
// Tests — FLAT_MAP with LATEST strategy (parent timestamp inheritance)
// =============================================================================

/// Build a program with MAP_UPSERT_LATEST inside FLAT_MAP, using parent_ts_col:
///   Slot 0: HASHMAP (cap=16, with timestamps)
///   FOR_EACH { FLAT_MAP(offsets_col=1, parent_ts_col=2) { MAP_UPSERT_LATEST(slot=0, key_col=3, val_col=4, ts_col=5) } }
///
/// The ts_col=5 is the "fallback" ts column, but parent_ts_col=2 overrides it inside FLAT_MAP.
/// Columns: 0=type, 1=offsets, 2=parent_ts(f64), 3=child_key, 4=child_val, 5=child_ts(f64, ignored)
fn buildFlatMapLatestTestProgram(type_id: u32) [112]u8 {
    var prog = [_]u8{0} ** 112;
    const content = prog[PROGRAM_HASH_PREFIX..];
    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    content[4] = 1;
    content[5] = 0;
    content[6] = 1; // num_slots
    content[7] = 6; // num_inputs
    content[8] = 0;
    content[9] = 0;

    // Init: SLOT_DEF for HASHMAP with timestamps = 5 bytes
    const init_len: u16 = 5;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);

    // Body: MAP_UPSERT_LATEST = 5 bytes (op + slot + key_col + val_col + ts_col)
    const inner_body_len: u16 = 5;
    const flat_map_total: u16 = 5 + inner_body_len; // FLAT_MAP header + body
    const reduce_len: u16 = 9 + flat_map_total; // FOR_EACH(9) + body
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    // Init section
    var off: usize = 14;
    content[off] = @intFromEnum(Opcode.SLOT_DEF);
    content[off + 1] = 0; // slot
    content[off + 2] = @intFromEnum(SlotType.HASHMAP); // type_flags: HASHMAP=0
    content[off + 3] = 8; // cap_lo=8 → capacity = nextPowerOf2(16) = 16
    content[off + 4] = 0; // cap_hi
    off += 5;

    // Reduce section
    const rs = 14 + init_len;
    content[rs] = 0xE0; // FOR_EACH
    content[rs + 1] = 0; // col (type_col)
    content[rs + 2] = 1; // match_count
    content[rs + 3] = @truncate(type_id);
    content[rs + 4] = @truncate(type_id >> 8);
    content[rs + 5] = @truncate(type_id >> 16);
    content[rs + 6] = @truncate(type_id >> 24);
    content[rs + 7] = @truncate(flat_map_total);
    content[rs + 8] = @truncate(flat_map_total >> 8);

    // FLAT_MAP with parent_ts_col = 2 (uses parent event's timestamp)
    const fm = rs + 9;
    content[fm] = 0xE1; // FLAT_MAP
    content[fm + 1] = 1; // offsets_col
    content[fm + 2] = 2; // parent_ts_col = col 2 (parent timestamp!)
    content[fm + 3] = @truncate(inner_body_len);
    content[fm + 4] = @truncate(inner_body_len >> 8);

    // Inner body: MAP_UPSERT_LATEST
    const ib = fm + 5;
    content[ib] = 0x20; // MAP_UPSERT_LATEST
    content[ib + 1] = 0; // slot
    content[ib + 2] = 3; // key_col (child)
    content[ib + 3] = 4; // val_col (child)
    content[ib + 4] = 5; // ts_col (child — will be OVERRIDDEN by parent_ts_col)

    return prog;
}

test "FLAT_MAP with LATEST strategy - parent timestamp overrides child ts_col" {
    const TYPE_A: u32 = 1001;
    var prog = buildFlatMapLatestTestProgram(TYPE_A);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    std.debug.assert(state_size <= 8192);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // 2 parent events:
    //   Parent 0 (ts=100.0): 1 child → key=42, val=10, child_ts=999.0 (should be IGNORED)
    //   Parent 1 (ts=200.0): 1 child → key=42, val=20, child_ts=1.0 (should be IGNORED)
    // Because parent_ts_col is set, the MAP_UPSERT_LATEST uses parent ts.
    // Parent 1 has ts=200.0 > parent 0 ts=100.0, so val=20 should win.
    var type_col = [2]u32{ TYPE_A, TYPE_A };
    var offsets_col = [3]u32{ 0, 1, 2 };
    var parent_ts: [2]f64 = .{ 100.0, 200.0 };
    var child_key = [2]u32{ 42, 42 }; // same key
    var child_val = [2]u32{ 10, 20 };
    var child_ts: [2]f64 = .{ 999.0, 1.0 }; // should be IGNORED because parent_ts_col overrides

    const col_ptrs = [6][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&offsets_col)),
        @as([*]const u8, @ptrCast(&parent_ts)),
        @as([*]const u8, @ptrCast(&child_key)),
        @as([*]const u8, @ptrCast(&child_val)),
        @as([*]const u8, @ptrCast(&child_ts)),
    };

    const exec_result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        6,
        2,
    );
    try std.testing.expectEqual(@as(u32, 0), exec_result);

    // Key 42 should have val=20 (parent 1 wins because parent ts 200.0 > 100.0)
    // If child_ts were used, val=10 would win (child_ts 999.0 > 1.0)
    const meta_base = STATE_HEADER_SIZE;
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const capacity = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    const data_ptr = state_buf[slot_offset..];
    const keys: [*]const u32 = @ptrCast(@alignCast(data_ptr.ptr));
    const values: [*]const u32 = @ptrCast(@alignCast(data_ptr[capacity * 4 ..].ptr));

    // Find key 42
    var found = false;
    var val: u32 = 0;
    var pos: u32 = 0;
    while (pos < capacity) : (pos += 1) {
        if (keys[pos] == 42) {
            found = true;
            val = values[pos];
            break;
        }
    }
    try std.testing.expect(found);
    try std.testing.expectEqual(@as(u32, 20), val); // parent ts wins, so val=20
}

// =============================================================================
// Tests — Ordered List (scalar)
// =============================================================================

/// Build a program with:
///   Slot 0: ORDERED_LIST (scalar UINT32, capacity hint=8)
///   FOR_EACH { LIST_APPEND(slot=0, val_col=1) }
/// Columns: 0=type, 1=value(u32)
fn buildScalarListTestProgram(type_id: u32) [80]u8 {
    var prog = [_]u8{0} ** 80;
    const content = prog[PROGRAM_HASH_PREFIX..];
    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    content[4] = 1;
    content[5] = 0;
    content[6] = 1; // num_slots
    content[7] = 2; // num_inputs
    content[8] = 0;
    content[9] = 0;

    // Init: SLOT_ORDERED_LIST(scalar) = 6 bytes (op + slot + type_flags + cap_lo + cap_hi + elem_type)
    const init_len: u16 = 6;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);

    // Reduce: FOR_EACH(9) + LIST_APPEND(3) = 12
    const body_len: u16 = 3;
    const reduce_len: u16 = 9 + body_len;
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    // Init section
    var off: usize = 14;
    content[off] = 0x19; // SLOT_ORDERED_LIST
    content[off + 1] = 0; // slot
    content[off + 2] = 6; // type_flags: ORDERED_LIST(6)
    content[off + 3] = 8; // cap_lo → capacity = nextPowerOf2(8) = 8
    content[off + 4] = 0; // cap_hi
    content[off + 5] = 0; // elem_type: UINT32(0)
    off += 6;

    // Reduce section
    const rs: usize = 14 + init_len;
    content[rs] = 0xE0; // FOR_EACH
    content[rs + 1] = 0; // col (type_col)
    content[rs + 2] = 1; // match_count
    content[rs + 3] = @truncate(type_id);
    content[rs + 4] = @truncate(type_id >> 8);
    content[rs + 5] = @truncate(type_id >> 16);
    content[rs + 6] = @truncate(type_id >> 24);
    content[rs + 7] = @truncate(body_len);
    content[rs + 8] = @truncate(body_len >> 8);

    // Body: LIST_APPEND
    const bo = rs + 9;
    content[bo] = 0x84; // LIST_APPEND
    content[bo + 1] = 0; // slot
    content[bo + 2] = 1; // val_col

    return prog;
}

test "ORDERED_LIST - scalar append" {
    const TYPE_A: u32 = 1001;
    var prog = buildScalarListTestProgram(TYPE_A);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [4096]u8 align(8) = [_]u8{0} ** 4096;
    std.debug.assert(state_size <= 4096);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // 3 events with values
    var type_col = [3]u32{ TYPE_A, TYPE_A, TYPE_A };
    var val_col = [3]u32{ 100, 200, 300 };

    const col_ptrs = [2][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&val_col)),
    };

    const result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        2,
        3,
    );
    try std.testing.expectEqual(@as(u32, 0), result);

    // Verify count = 3
    const meta_base = STATE_HEADER_SIZE;
    const count = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 3), count);

    // Verify values [100, 200, 300]
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const values: [*]const u32 = @ptrCast(@alignCast(&state_buf[slot_offset]));
    try std.testing.expectEqual(@as(u32, 100), values[0]);
    try std.testing.expectEqual(@as(u32, 200), values[1]);
    try std.testing.expectEqual(@as(u32, 300), values[2]);
}

test "ORDERED_LIST - scalar growth triggers NEEDS_GROWTH" {
    const TYPE_A: u32 = 1001;
    var prog = buildScalarListTestProgram(TYPE_A);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [4096]u8 align(8) = [_]u8{0} ** 4096;
    std.debug.assert(state_size <= 4096);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // Capacity = nextPowerOf2(8) = 8 (minimum might be higher, let's check)
    const meta_base = STATE_HEADER_SIZE;
    const capacity = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);

    // Fill to capacity
    var type_col: [64]u32 = undefined;
    var val_col: [64]u32 = undefined;
    for (0..capacity) |i| {
        type_col[i] = TYPE_A;
        val_col[i] = @truncate(i + 1);
    }

    const col_ptrs = [2][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&val_col)),
    };

    // Fill to capacity — should succeed
    const result_fill = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        2,
        capacity,
    );
    try std.testing.expectEqual(@as(u32, 0), result_fill);

    // One more should trigger NEEDS_GROWTH
    var type_col2 = [1]u32{TYPE_A};
    var val_col2 = [1]u32{999};
    const col_ptrs2 = [2][*]const u8{
        @as([*]const u8, @ptrCast(&type_col2)),
        @as([*]const u8, @ptrCast(&val_col2)),
    };

    const result_overflow = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs2),
        2,
        1,
    );
    try std.testing.expectEqual(@as(u32, @intFromEnum(ErrorCode.NEEDS_GROWTH)), result_overflow);
}

// =============================================================================
// Tests — Ordered List (struct)
// =============================================================================

/// Build a program with:
///   Slot 0: ORDERED_LIST (struct, 2 fields: UINT32 + STRING, capacity hint=8)
///   FOR_EACH { LIST_APPEND_STRUCT(slot=0, vals from cols 1,2) }
/// Columns: 0=type, 1=val0(u32), 2=val1(u32)
fn buildStructListTestProgram(type_id: u32) [96]u8 {
    var prog = [_]u8{0} ** 96;
    const content = prog[PROGRAM_HASH_PREFIX..];
    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    content[4] = 1;
    content[5] = 0;
    content[6] = 1; // num_slots
    content[7] = 3; // num_inputs
    content[8] = 0;
    content[9] = 0;

    // Init: SLOT_ORDERED_LIST(struct) = op(1) + slot(1) + type_flags(1) + cap_lo(1) + cap_hi(1)
    //       + elem_type(1=0xFF) + num_fields(1) + field_types(2) = 9 bytes
    const init_len: u16 = 9;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);

    // Reduce: FOR_EACH(9) + LIST_APPEND_STRUCT(3 + 2*2 = 7) = 16
    const body_len: u16 = 7;
    const reduce_len: u16 = 9 + body_len;
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    // Init section
    var off: usize = 14;
    content[off] = 0x19; // SLOT_ORDERED_LIST
    content[off + 1] = 0; // slot
    content[off + 2] = @intFromEnum(SlotType.ORDERED_LIST); // type_flags: ORDERED_LIST
    content[off + 3] = 8; // cap_lo
    content[off + 4] = 0; // cap_hi
    content[off + 5] = 0xFF; // elem_type: STRUCT
    content[off + 6] = 2; // num_fields
    content[off + 7] = 0; // UINT32
    content[off + 8] = 4; // STRING
    off += 9;

    // Reduce section
    const rs: usize = 14 + init_len;
    content[rs] = 0xE0; // FOR_EACH
    content[rs + 1] = 0; // col (type_col)
    content[rs + 2] = 1; // match_count
    content[rs + 3] = @truncate(type_id);
    content[rs + 4] = @truncate(type_id >> 8);
    content[rs + 5] = @truncate(type_id >> 16);
    content[rs + 6] = @truncate(type_id >> 24);
    content[rs + 7] = @truncate(body_len);
    content[rs + 8] = @truncate(body_len >> 8);

    // Body: LIST_APPEND_STRUCT
    const bo = rs + 9;
    content[bo] = 0x85; // LIST_APPEND_STRUCT
    content[bo + 1] = 0; // slot
    content[bo + 2] = 2; // num_vals
    content[bo + 3] = 1; // val_col=1
    content[bo + 4] = 0; // field_idx=0
    content[bo + 5] = 2; // val_col=2
    content[bo + 6] = 1; // field_idx=1

    return prog;
}

test "ORDERED_LIST - struct append" {
    const TYPE_A: u32 = 1001;
    var prog = buildStructListTestProgram(TYPE_A);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [4096]u8 align(8) = [_]u8{0} ** 4096;
    std.debug.assert(state_size <= 4096);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // 2 events
    var type_col = [2]u32{ TYPE_A, TYPE_A };
    var val0_col = [2]u32{ 42, 99 };
    var val1_col = [2]u32{ 1001, 2002 };

    const col_ptrs = [3][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&val0_col)),
        @as([*]const u8, @ptrCast(&val1_col)),
    };

    const result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        3,
        2,
    );
    try std.testing.expectEqual(@as(u32, 0), result);

    // Verify count = 2
    const meta_base = STATE_HEADER_SIZE;
    const count = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 2), count);

    // Read slot metadata for row access
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const num_fields = state_buf[meta_base + 13];
    const row_size: u32 = std.mem.readInt(u16, state_buf[meta_base + 16 ..][0..2], .little);
    const descriptor_size = align8(@as(u32, num_fields));
    const rows_base = slot_offset + descriptor_size;

    // Row 0: val0=42, val1=1001
    const row0 = state_buf[rows_base..];
    // bitset is 1 byte (2 fields < 8), field 0 at offset 1 (bitset=1B), field 1 at offset 5
    const bitset0 = row0[0];
    try std.testing.expectEqual(@as(u8, 0x03), bitset0); // both bits set
    const field0_0 = std.mem.readInt(u32, row0[1..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 42), field0_0);
    const field0_1 = std.mem.readInt(u32, row0[5..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 1001), field0_1);

    // Row 1: val0=99, val1=2002
    const row1 = state_buf[rows_base + row_size ..];
    const field1_0 = std.mem.readInt(u32, row1[1..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 99), field1_0);
    const field1_1 = std.mem.readInt(u32, row1[5..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 2002), field1_1);
}

test "ORDERED_LIST - undo restores count" {
    const TYPE_A: u32 = 1001;
    var prog = buildScalarListTestProgram(TYPE_A);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [4096]u8 align(8) = [_]u8{0} ** 4096;
    std.debug.assert(state_size <= 4096);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // Insert 2 items
    var type_col = [2]u32{ TYPE_A, TYPE_A };
    var val_col = [2]u32{ 100, 200 };
    const col_ptrs = [2][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&val_col)),
    };

    const result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        2,
        2,
    );
    try std.testing.expectEqual(@as(u32, 0), result);

    // Verify count = 2
    const meta_base = STATE_HEADER_SIZE;
    try std.testing.expectEqual(@as(u32, 2), std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little));

    // Enable undo and insert 1 more
    const state_size2 = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    vm_undo_enable(@ptrCast(&state_buf), state_size2);
    const checkpoint = vm_undo_checkpoint(@ptrCast(&state_buf));
    var type_col2 = [1]u32{TYPE_A};
    var val_col2 = [1]u32{300};
    const col_ptrs2 = [2][*]const u8{
        @as([*]const u8, @ptrCast(&type_col2)),
        @as([*]const u8, @ptrCast(&val_col2)),
    };
    const result2 = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs2),
        2,
        1,
    );
    try std.testing.expectEqual(@as(u32, 0), result2);

    // count should be 3
    try std.testing.expectEqual(@as(u32, 3), std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little));

    // Rollback
    vm_undo_rollback(@ptrCast(&state_buf), checkpoint);

    // count should be back to 2
    try std.testing.expectEqual(@as(u32, 2), std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little));
}

test "ORDERED_LIST - inside FLAT_MAP" {
    const TYPE_A: u32 = 1001;
    // Build a program: FOR_EACH { FLAT_MAP { LIST_APPEND(slot=0, val_col=2) } }
    // Slot 0: ORDERED_LIST (scalar UINT32, cap=8)
    // Columns: 0=type, 1=offsets, 2=child_val(u32)
    var prog = [_]u8{0} ** 96;
    const content = prog[PROGRAM_HASH_PREFIX..];
    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    content[4] = 1;
    content[5] = 0;
    content[6] = 1; // num_slots
    content[7] = 3; // num_inputs
    content[8] = 0;
    content[9] = 0;

    const init_len: u16 = 6; // SLOT_ORDERED_LIST scalar
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);

    const inner_body_len: u16 = 3; // LIST_APPEND
    const flat_map_total: u16 = 5 + inner_body_len;
    const reduce_len: u16 = 9 + flat_map_total;
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    // Init
    var off: usize = 14;
    content[off] = 0x19; // SLOT_ORDERED_LIST
    content[off + 1] = 0; // slot
    content[off + 2] = @intFromEnum(SlotType.ORDERED_LIST); // type_flags: ORDERED_LIST
    content[off + 3] = 8; // cap_lo
    content[off + 4] = 0;
    content[off + 5] = 0; // UINT32
    off += 6;

    // Reduce
    const rs = 14 + init_len;
    content[rs] = 0xE0;
    content[rs + 1] = 0; // col (type_col)
    content[rs + 2] = 1; // match_count
    content[rs + 3] = @truncate(TYPE_A);
    content[rs + 4] = @truncate(TYPE_A >> 8);
    content[rs + 5] = @truncate(TYPE_A >> 16);
    content[rs + 6] = @truncate(TYPE_A >> 24);
    content[rs + 7] = @truncate(flat_map_total);
    content[rs + 8] = @truncate(flat_map_total >> 8);

    const fm = rs + 9;
    content[fm] = 0xE1; // FLAT_MAP
    content[fm + 1] = 1; // offsets_col
    content[fm + 2] = 0xFF; // parent_ts_col
    content[fm + 3] = @truncate(inner_body_len);
    content[fm + 4] = @truncate(inner_body_len >> 8);

    const ib = fm + 5;
    content[ib] = 0x84; // LIST_APPEND
    content[ib + 1] = 0; // slot
    content[ib + 2] = 2; // val_col

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [4096]u8 align(8) = [_]u8{0} ** 4096;
    std.debug.assert(state_size <= 4096);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // 2 parents: parent 0 has 2 children, parent 1 has 1 child
    var type_col = [2]u32{ TYPE_A, TYPE_A };
    var offsets_col = [3]u32{ 0, 2, 3 };
    var child_val = [3]u32{ 10, 20, 30 };

    const col_ptrs = [3][*]const u8{
        @as([*]const u8, @ptrCast(&type_col)),
        @as([*]const u8, @ptrCast(&offsets_col)),
        @as([*]const u8, @ptrCast(&child_val)),
    };

    const result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        3,
        2,
    );
    try std.testing.expectEqual(@as(u32, 0), result);

    // Verify count = 3 and values = [10, 20, 30]
    const meta_base = STATE_HEADER_SIZE;
    const cnt = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 3), cnt);

    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const values: [*]const u32 = @ptrCast(@alignCast(&state_buf[slot_offset]));
    try std.testing.expectEqual(@as(u32, 10), values[0]);
    try std.testing.expectEqual(@as(u32, 20), values[1]);
    try std.testing.expectEqual(@as(u32, 30), values[2]);
}

// =============================================================================
// Tests — Array Fields in Struct Map Rows
// =============================================================================

/// Build a program with a STRUCT_MAP slot that has 1 scalar + 1 array field:
///   Field 0: UINT32 (scalar)
///   Field 1: ARRAY_U32 (array — stored as offset+length in row, data in arena)
///
/// Columns: 0=type, 1=key(u32), 2=scalar_val(u32), 3=array_offsets(u32), 4=array_values(u32)
fn buildArrayFieldTestProgram(type_id: u32) [128]u8 {
    var prog = [_]u8{0} ** 128;
    const content = prog[PROGRAM_HASH_PREFIX..];
    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    content[4] = 1;
    content[5] = 0;
    content[6] = 1; // num_slots
    content[7] = 5; // num_inputs
    content[8] = 0;
    content[9] = 0;

    // Init: SLOT_STRUCT_MAP = 8 bytes (op + slot + type_flags + cap_lo + cap_hi + num_fields + 2 field types)
    const init_len: u16 = 8;
    content[10] = @truncate(init_len);
    content[11] = @truncate(init_len >> 8);

    // Reduce: FOR_EACH(9) + body: STRUCT_MAP_UPSERT_LAST
    //   Scalar: op(1)+slot(1)+key_col(1)+num_vals(1)+1*(val_col+field_idx) = 6
    //   Array: num_array_vals(1)+1*(offsets_col+values_col+field_idx) = 4
    //   Total body = 6 + 4 = 10
    const body_len: u16 = 10;
    const reduce_len: u16 = 9 + body_len;
    content[12] = @truncate(reduce_len);
    content[13] = @truncate(reduce_len >> 8);

    // Init section
    var off: usize = 14;
    content[off] = 0x18; // SLOT_STRUCT_MAP
    content[off + 1] = 0; // slot index
    content[off + 2] = @intFromEnum(SlotType.STRUCT_MAP); // type_flags: STRUCT_MAP
    content[off + 3] = 4; // cap_lo
    content[off + 4] = 0; // cap_hi
    content[off + 5] = 2; // num_fields
    content[off + 6] = 0; // field 0: UINT32
    content[off + 7] = 5; // field 1: ARRAY_U32
    off += 8;

    // Reduce section
    const rs = 14 + init_len;
    content[rs] = 0xE0; // FOR_EACH
    content[rs + 1] = 0; // col (type_col)
    content[rs + 2] = 1; // match_count
    content[rs + 3] = @truncate(type_id);
    content[rs + 4] = @truncate(type_id >> 8);
    content[rs + 5] = @truncate(type_id >> 16);
    content[rs + 6] = @truncate(type_id >> 24);
    content[rs + 7] = @truncate(body_len);
    content[rs + 8] = @truncate(body_len >> 8);

    // Body: STRUCT_MAP_UPSERT_LAST with 1 scalar + 1 array field
    const bs = rs + 9;
    content[bs] = 0x80; // STRUCT_MAP_UPSERT_LAST
    content[bs + 1] = 0; // slot
    content[bs + 2] = 1; // key_col
    content[bs + 3] = 1; // num_scalar_vals
    content[bs + 4] = 2; // val_col=2, field_idx=0 (UINT32)
    content[bs + 5] = 0;
    // Array section
    content[bs + 6] = 1; // num_array_vals
    content[bs + 7] = 3; // offsets_col
    content[bs + 8] = 4; // values_col
    content[bs + 9] = 1; // field_idx=1 (ARRAY_U32)

    return prog;
}

test "struct map array field - basic write and read back" {
    const TYPE_A: u32 = 1001;
    var prog = buildArrayFieldTestProgram(TYPE_A);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [16384]u8 align(8) = [_]u8{0} ** 16384;
    std.debug.assert(state_size <= 16384);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // Insert 2 rows:
    //   key=100, scalar=42, array=[10, 20, 30]
    //   key=200, scalar=99, array=[40, 50]
    var type_col = [2]u32{ TYPE_A, TYPE_A };
    var key_col = [2]u32{ 100, 200 };
    var scalar_col = [2]u32{ 42, 99 };
    // CSR offsets: row0 has 3 elements [0..3), row1 has 2 elements [3..5)
    var offsets_col = [3]u32{ 0, 3, 5 };
    var values_col = [5]u32{ 10, 20, 30, 40, 50 };

    var col_ptrs = [5][*]const u8{
        @ptrCast(&type_col),
        @ptrCast(&key_col),
        @ptrCast(&scalar_col),
        @ptrCast(&offsets_col),
        @ptrCast(&values_col),
    };

    const result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        5,
        2,
    );
    try std.testing.expectEqual(@as(u32, 0), result);

    // Verify: read struct map metadata
    const meta_base = STATE_HEADER_SIZE;
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const capacity = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    const current_size = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 2), current_size);

    const num_fields: u32 = state_buf[meta_base + 13];
    try std.testing.expectEqual(@as(u32, 2), num_fields);
    const row_size: u32 = std.mem.readInt(u16, state_buf[meta_base + 16 ..][0..2], .little);

    // Arena should be initialized
    const arena_hdr_off = std.mem.readInt(u32, state_buf[meta_base + 20 ..][0..4], .little);
    try std.testing.expect(arena_hdr_off != 0);
    const arena_used = std.mem.readInt(u32, state_buf[arena_hdr_off + 4 ..][0..4], .little);
    // 3 + 2 = 5 elements × 4 bytes = 20 bytes
    try std.testing.expectEqual(@as(u32, 20), arena_used);

    // Find key=100 row and verify
    const field_types_ptr: [*]const u8 = @ptrCast(&state_buf[slot_offset]);
    const desc_size = align8(num_fields);
    const keys_offset = slot_offset + desc_size;
    const keys: [*]const u32 = @ptrCast(@alignCast(&state_buf[keys_offset]));
    const rows_base = keys_offset + capacity * 4;
    const arena_data_base = arena_hdr_off + ARENA_HEADER_SIZE;

    // Find key=100
    var pos100: ?u32 = null;
    var pos200: ?u32 = null;
    for (0..capacity) |ki| {
        if (keys[ki] == 100) pos100 = @truncate(ki);
        if (keys[ki] == 200) pos200 = @truncate(ki);
    }
    try std.testing.expect(pos100 != null);
    try std.testing.expect(pos200 != null);

    // Verify key=100: scalar=42, array=[10, 20, 30]
    {
        const row_ptr: [*]const u8 = @ptrCast(&state_buf[rows_base + pos100.? * row_size]);
        // Bitset: both fields set
        try std.testing.expect((row_ptr[0] & 0x03) == 0x03);
        // Scalar field at offset = bitset(1)
        const scalar_off = structFieldOffset(@truncate(num_fields), field_types_ptr, 0);
        const scalar_val = std.mem.readInt(u32, row_ptr[scalar_off..][0..4], .little);
        try std.testing.expectEqual(@as(u32, 42), scalar_val);
        // Array field at offset after scalar (bitset(1) + u32(4) = 5)
        const arr_off = structFieldOffset(@truncate(num_fields), field_types_ptr, 1);
        const arr_arena_offset = std.mem.readInt(u32, row_ptr[arr_off..][0..4], .little);
        const arr_len = std.mem.readInt(u32, row_ptr[arr_off + 4 ..][0..4], .little);
        try std.testing.expectEqual(@as(u32, 3), arr_len);
        // Read array elements from arena
        const arr_data: [*]const u32 = @ptrCast(@alignCast(&state_buf[arena_data_base + arr_arena_offset]));
        try std.testing.expectEqual(@as(u32, 10), arr_data[0]);
        try std.testing.expectEqual(@as(u32, 20), arr_data[1]);
        try std.testing.expectEqual(@as(u32, 30), arr_data[2]);
    }

    // Verify key=200: scalar=99, array=[40, 50]
    {
        const row_ptr: [*]const u8 = @ptrCast(&state_buf[rows_base + pos200.? * row_size]);
        const scalar_off = structFieldOffset(@truncate(num_fields), field_types_ptr, 0);
        const scalar_val = std.mem.readInt(u32, row_ptr[scalar_off..][0..4], .little);
        try std.testing.expectEqual(@as(u32, 99), scalar_val);
        const arr_off = structFieldOffset(@truncate(num_fields), field_types_ptr, 1);
        const arr_arena_offset = std.mem.readInt(u32, row_ptr[arr_off..][0..4], .little);
        const arr_len = std.mem.readInt(u32, row_ptr[arr_off + 4 ..][0..4], .little);
        try std.testing.expectEqual(@as(u32, 2), arr_len);
        const arr_data: [*]const u32 = @ptrCast(@alignCast(&state_buf[arena_data_base + arr_arena_offset]));
        try std.testing.expectEqual(@as(u32, 40), arr_data[0]);
        try std.testing.expectEqual(@as(u32, 50), arr_data[1]);
    }
}

test "struct map array field - empty array" {
    const TYPE_A: u32 = 1001;
    var prog = buildArrayFieldTestProgram(TYPE_A);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [16384]u8 align(8) = [_]u8{0} ** 16384;
    std.debug.assert(state_size <= 16384);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // key=100, scalar=42, empty array (offsets[0]=0, offsets[1]=0 → length=0)
    var type_col = [1]u32{TYPE_A};
    var key_col = [1]u32{100};
    var scalar_col = [1]u32{42};
    var offsets_col = [2]u32{ 0, 0 }; // empty array
    var values_col = [1]u32{0}; // unused

    var col_ptrs = [5][*]const u8{
        @ptrCast(&type_col),
        @ptrCast(&key_col),
        @ptrCast(&scalar_col),
        @ptrCast(&offsets_col),
        @ptrCast(&values_col),
    };

    const result = vm_execute_batch(
        @ptrCast(&state_buf),
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        5,
        1,
    );
    try std.testing.expectEqual(@as(u32, 0), result);

    // Verify: array has length 0
    const meta_base = STATE_HEADER_SIZE;
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const capacity = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    const num_fields: u32 = state_buf[meta_base + 13];
    const row_size: u32 = std.mem.readInt(u16, state_buf[meta_base + 16 ..][0..2], .little);

    const field_types_ptr: [*]const u8 = @ptrCast(&state_buf[slot_offset]);
    const desc_size = align8(num_fields);
    const keys: [*]const u32 = @ptrCast(@alignCast(&state_buf[slot_offset + desc_size]));
    const rows_base = slot_offset + desc_size + capacity * 4;

    // Find key=100
    var pos: ?u32 = null;
    for (0..capacity) |ki| {
        if (keys[ki] == 100) pos = @truncate(ki);
    }
    try std.testing.expect(pos != null);

    const row_ptr: [*]const u8 = @ptrCast(&state_buf[rows_base + pos.? * row_size]);
    const arr_off = structFieldOffset(@truncate(num_fields), field_types_ptr, 1);
    const arr_len = std.mem.readInt(u32, row_ptr[arr_off + 4 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 0), arr_len);

    // Arena should have 0 bytes used
    const arena_hdr_off = std.mem.readInt(u32, state_buf[meta_base + 20 ..][0..4], .little);
    const arena_used = std.mem.readInt(u32, state_buf[arena_hdr_off + 4 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 0), arena_used);
}

test "struct map array field - overwrite (last wins, old arena data abandoned)" {
    const TYPE_A: u32 = 1001;
    var prog = buildArrayFieldTestProgram(TYPE_A);

    const state_size = vm_calculate_state_size(@ptrCast(&prog), prog.len);
    var state_buf: [16384]u8 align(8) = [_]u8{0} ** 16384;
    std.debug.assert(state_size <= 16384);
    _ = vm_init_state(@ptrCast(&state_buf), @ptrCast(&prog), prog.len);

    // First write: key=100, scalar=42, array=[10, 20, 30]
    {
        var type_col = [1]u32{TYPE_A};
        var key_col = [1]u32{100};
        var scalar_col = [1]u32{42};
        var offsets_col = [2]u32{ 0, 3 };
        var values_col = [3]u32{ 10, 20, 30 };

        var col_ptrs = [5][*]const u8{
            @ptrCast(&type_col),
            @ptrCast(&key_col),
            @ptrCast(&scalar_col),
            @ptrCast(&offsets_col),
            @ptrCast(&values_col),
        };

        const result = vm_execute_batch(
            @ptrCast(&state_buf),
            @ptrCast(&prog),
            prog.len,
            @ptrCast(&col_ptrs),
            5,
            1,
        );
        try std.testing.expectEqual(@as(u32, 0), result);
    }

    // Overwrite: key=100, scalar=99, array=[40, 50]
    {
        var type_col = [1]u32{TYPE_A};
        var key_col = [1]u32{100};
        var scalar_col = [1]u32{99};
        var offsets_col = [2]u32{ 0, 2 };
        var values_col = [2]u32{ 40, 50 };

        var col_ptrs = [5][*]const u8{
            @ptrCast(&type_col),
            @ptrCast(&key_col),
            @ptrCast(&scalar_col),
            @ptrCast(&offsets_col),
            @ptrCast(&values_col),
        };

        const result = vm_execute_batch(
            @ptrCast(&state_buf),
            @ptrCast(&prog),
            prog.len,
            @ptrCast(&col_ptrs),
            5,
            1,
        );
        try std.testing.expectEqual(@as(u32, 0), result);
    }

    // Verify: should see new values, size still 1
    const meta_base = STATE_HEADER_SIZE;
    const slot_offset = std.mem.readInt(u32, state_buf[meta_base..][0..4], .little);
    const capacity = std.mem.readInt(u32, state_buf[meta_base + 4 ..][0..4], .little);
    const current_size = std.mem.readInt(u32, state_buf[meta_base + 8 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 1), current_size);

    const num_fields: u32 = state_buf[meta_base + 13];
    const row_size: u32 = std.mem.readInt(u16, state_buf[meta_base + 16 ..][0..2], .little);
    const field_types_ptr: [*]const u8 = @ptrCast(&state_buf[slot_offset]);
    const desc_size = align8(num_fields);
    const keys: [*]const u32 = @ptrCast(@alignCast(&state_buf[slot_offset + desc_size]));
    const rows_base = slot_offset + desc_size + capacity * 4;
    const arena_hdr_off = std.mem.readInt(u32, state_buf[meta_base + 20 ..][0..4], .little);
    const arena_data_base = arena_hdr_off + ARENA_HEADER_SIZE;

    // Arena used should be 12+8=20 (old data abandoned but still in arena)
    const arena_used = std.mem.readInt(u32, state_buf[arena_hdr_off + 4 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 20), arena_used); // 3*4 + 2*4 = 20

    // Find key=100 and verify new values
    var pos: ?u32 = null;
    for (0..capacity) |ki| {
        if (keys[ki] == 100) pos = @truncate(ki);
    }
    try std.testing.expect(pos != null);

    const row_ptr: [*]const u8 = @ptrCast(&state_buf[rows_base + pos.? * row_size]);
    // Scalar = 99
    const scalar_off = structFieldOffset(@truncate(num_fields), field_types_ptr, 0);
    try std.testing.expectEqual(@as(u32, 99), std.mem.readInt(u32, row_ptr[scalar_off..][0..4], .little));
    // Array = [40, 50] (at offset 12, length 2)
    const arr_off = structFieldOffset(@truncate(num_fields), field_types_ptr, 1);
    const arr_arena_offset = std.mem.readInt(u32, row_ptr[arr_off..][0..4], .little);
    const arr_len = std.mem.readInt(u32, row_ptr[arr_off + 4 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 12), arr_arena_offset); // after first write's 12 bytes
    try std.testing.expectEqual(@as(u32, 2), arr_len);
    const arr_data: [*]const u32 = @ptrCast(@alignCast(&state_buf[arena_data_base + arr_arena_offset]));
    try std.testing.expectEqual(@as(u32, 40), arr_data[0]);
    try std.testing.expectEqual(@as(u32, 50), arr_data[1]);
}
