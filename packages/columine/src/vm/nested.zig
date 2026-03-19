// =============================================================================
// Nested Container Slots — Arena-allocated inner maps, sets, and aggregates
// =============================================================================
//
// Supports arbitrary nesting via comptime-generic FlatHashTable:
//   Map<K, Set<V>>          → OuterMap(PtrMap) + inner HashSet per key
//   Map<K, Map<K2, V>>      → OuterMap(PtrMap) + inner HashMap per key
//   Map<K, Agg>             → OuterMap(PtrMap) + inner Aggregate per key
//
// The outer hash table maps u32 keys → u32 arena offsets.
// Inner containers are bump-allocated in a per-slot arena.
// Growth: inner containers grow 2× within the arena, old space abandoned.
// Compaction: vm_grow_state walks the tree and reclaims dead arena space.

const std = @import("std");
const types = @import("types.zig");
const ht = @import("hash_table.zig");

const EMPTY_KEY = types.EMPTY_KEY;
const align8 = types.align8;
const nextPowerOf2 = types.nextPowerOf2;
const SlotType = types.SlotType;
const AggType = types.AggType;
const ErrorCode = types.ErrorCode;
const SlotMeta = types.SlotMeta;
const ChangeFlag = types.ChangeFlag;
const setChangeFlag = types.setChangeFlag;

const HashSet = ht.HashSet;
const HashMap = ht.HashMap;
const PtrMap = ht.PtrMap;

// =============================================================================
// Arena — bump allocator within slot data region
// =============================================================================

pub const Arena = struct {
    state_base: [*]u8,
    hdr_offset: u32, // absolute offset of arena header
    data_offset: u32, // absolute offset of arena data start

    const HDR_CAP: u32 = 0;
    const HDR_USED: u32 = 4;
    pub const HDR_SIZE: u32 = 8;

    pub fn bind(state_base: [*]u8, hdr_offset: u32) Arena {
        return .{
            .state_base = state_base,
            .hdr_offset = hdr_offset,
            .data_offset = hdr_offset + HDR_SIZE,
        };
    }

    pub fn initAt(state_base: [*]u8, hdr_offset: u32, cap: u32) Arena {
        std.mem.writeInt(u32, (state_base + hdr_offset)[0..4], cap, .little);
        std.mem.writeInt(u32, (state_base + hdr_offset + HDR_USED)[0..4], 0, .little);
        return bind(state_base, hdr_offset);
    }

    fn capacity(self: Arena) u32 {
        return std.mem.readInt(u32, (self.state_base + self.hdr_offset)[0..4], .little);
    }

    pub fn used(self: Arena) u32 {
        return std.mem.readInt(u32, (self.state_base + self.hdr_offset + HDR_USED)[0..4], .little);
    }

    fn setUsed(self: Arena, val: u32) void {
        std.mem.writeInt(u32, (self.state_base + self.hdr_offset + HDR_USED)[0..4], val, .little);
    }

    /// Allocate `size` bytes. Returns absolute offset, or null if full.
    pub fn alloc(self: Arena, size: u32) ?u32 {
        const u = self.used();
        const aligned = align8(size);
        if (u + aligned > self.capacity()) return null;
        const offset = self.data_offset + u;
        self.setUsed(u + aligned);
        return offset;
    }
};

// =============================================================================
// Nested slot prefix — stored at start of slot data
// =============================================================================

pub const NESTED_PREFIX_SIZE: u32 = 8;

pub const NestedPrefix = struct {
    inner_type: SlotType,
    inner_initial_cap: u16,
    inner_agg_type: AggType,
    depth: u8,
};

pub fn readNestedPrefix(state_base: [*]u8, slot_offset: u32) NestedPrefix {
    const base = state_base + slot_offset;
    const agg_byte = base[3];
    return .{
        .inner_type = @enumFromInt(@as(u4, @truncate(base[0]))),
        .inner_initial_cap = @as(u16, base[1]) | (@as(u16, base[2]) << 8),
        .inner_agg_type = if (agg_byte >= 1 and agg_byte <= 13) @enumFromInt(agg_byte) else .SUM,
        .depth = base[4],
    };
}

pub fn writeNestedPrefix(state_base: [*]u8, slot_offset: u32, prefix: NestedPrefix) void {
    const base = state_base + slot_offset;
    base[0] = @intFromEnum(prefix.inner_type);
    base[1] = @truncate(prefix.inner_initial_cap);
    base[2] = @truncate(prefix.inner_initial_cap >> 8);
    base[3] = @intFromEnum(prefix.inner_agg_type);
    base[4] = prefix.depth;
    @memset(base[5..8], 0);
}

// =============================================================================
// Outer slot layout helpers
// =============================================================================

pub fn outerKeysOffset(slot_offset: u32) u32 {
    return slot_offset + NESTED_PREFIX_SIZE;
}

pub fn outerPtrsOffset(slot_offset: u32, capacity: u32) u32 {
    return outerKeysOffset(slot_offset) + capacity * 4;
}

pub fn arenaHeaderOffset(slot_offset: u32, capacity: u32) u32 {
    // PtrMap stores [keys: u32 × cap][entries(u32) × cap], but we use separate
    // keys + ptrs arrays for clarity. The arena header follows.
    return outerPtrsOffset(slot_offset, capacity) + capacity * 4;
}

pub fn arenaDataOffset(slot_offset: u32, capacity: u32) u32 {
    return arenaHeaderOffset(slot_offset, capacity) + Arena.HDR_SIZE;
}

// =============================================================================
// Inner container size calculation
// =============================================================================

pub fn innerContainerSize(inner_type: SlotType, capacity: u32, inner_agg_type: AggType) u32 {
    return switch (inner_type) {
        .HASHMAP => HashMap.byteSize(capacity),
        .HASHSET => HashSet.byteSize(capacity),
        .AGGREGATE => switch (inner_agg_type) {
            .COUNT => 8,
            else => 16,
        },
        else => 0,
    };
}

pub fn nestedSlotDataSize(outer_cap: u32, inner_initial_cap: u32, inner_type: SlotType, inner_agg_type: AggType) u32 {
    const per_inner = align8(innerContainerSize(inner_type, inner_initial_cap, inner_agg_type));
    return NESTED_PREFIX_SIZE + outer_cap * 4 + outer_cap * 4 + Arena.HDR_SIZE + outer_cap * per_inner;
}

// =============================================================================
// Outer hash table — typed access to keys + arena pointers
// =============================================================================

const OuterTable = struct {
    state_base: [*]u8,
    cap: u32,
    size_ptr: *align(1) u32,
    keys: [*]u32,
    ptrs: [*]u32, // arena offsets per outer key
    arena: Arena,

    fn bind(state_base: [*]u8, meta: SlotMeta) OuterTable {
        const slot_off = meta.offset;
        const cap = meta.capacity;
        const keys_off = outerKeysOffset(slot_off);
        const ptrs_off = outerPtrsOffset(slot_off, cap);
        const arena_hdr_off = arenaHeaderOffset(slot_off, cap);

        return .{
            .state_base = state_base,
            .cap = cap,
            .size_ptr = meta.size_ptr,
            .keys = @ptrCast(@alignCast(state_base + keys_off)),
            .ptrs = @ptrCast(@alignCast(state_base + ptrs_off)),
            .arena = Arena.bind(state_base, arena_hdr_off),
        };
    }

    /// Resolve outer key → inner container offset. Allocates if needed.
    /// Returns .offset (absolute) and .is_new, or null if capacity exceeded.
    fn resolve(
        self: OuterTable,
        outer_key: u32,
        prefix: NestedPrefix,
    ) ?struct { offset: u32, is_new: bool } {
        if (outer_key == EMPTY_KEY or outer_key == @as(u32, 0xFFFFFFFE)) return null;

        var pos = types.hashKey(outer_key, self.cap);
        var probes: u32 = 0;
        while (probes < self.cap) : (probes += 1) {
            const k = self.keys[pos];
            if (k == outer_key) {
                return .{ .offset = self.ptrs[pos], .is_new = false };
            }
            if (k == EMPTY_KEY or k == @as(u32, 0xFFFFFFFE)) {
                // Insert new outer key + allocate inner container
                if (self.size_ptr.* >= (self.cap * 7) / 10) return null;

                const inner_cap = nextPowerOf2(prefix.inner_initial_cap);
                const inner_size = innerContainerSize(prefix.inner_type, inner_cap, prefix.inner_agg_type);
                const inner_off = self.arena.alloc(inner_size) orelse return null;

                // Initialize inner container
                switch (prefix.inner_type) {
                    .HASHSET => _ = HashSet.init(self.state_base, inner_off, inner_cap),
                    .HASHMAP => _ = HashMap.init(self.state_base, inner_off, inner_cap),
                    .AGGREGATE => @memset((self.state_base + inner_off)[0..inner_size], 0),
                    else => {},
                }

                self.keys[pos] = outer_key;
                self.ptrs[pos] = inner_off;
                self.size_ptr.* += 1;
                return .{ .offset = inner_off, .is_new = true };
            }
            pos = (pos + 1) & (self.cap - 1);
        }
        return null;
    }

    /// Look up inner container offset for key. Returns 0 if not found.
    fn lookup(self: OuterTable, outer_key: u32) u32 {
        var pos = types.hashKey(outer_key, self.cap);
        var probes: u32 = 0;
        while (probes < self.cap) : (probes += 1) {
            const k = self.keys[pos];
            if (k == outer_key) return self.ptrs[pos];
            if (k == EMPTY_KEY) return 0;
            pos = (pos + 1) & (self.cap - 1);
        }
        return 0;
    }

    /// Update the arena pointer for an existing outer key.
    fn updatePtr(self: OuterTable, outer_key: u32, new_offset: u32) void {
        var pos = types.hashKey(outer_key, self.cap);
        while (self.keys[pos] != outer_key) {
            pos = (pos + 1) & (self.cap - 1);
        }
        self.ptrs[pos] = new_offset;
    }
};

// =============================================================================
// Public operations — typed, no raw byte manipulation
// =============================================================================

/// Insert element into nested set: Map<outer_key, Set>.add(elem)
pub fn nestedSetInsert(state_base: [*]u8, meta: SlotMeta, outer_key: u32, elem: u32) ErrorCode {
    const prefix = readNestedPrefix(state_base, meta.offset);
    const outer = OuterTable.bind(state_base, meta);
    const resolved = outer.resolve(outer_key, prefix) orelse return .CAPACITY_EXCEEDED;

    if (resolved.is_new) setChangeFlag(meta, ChangeFlag.INSERTED);

    var inner = HashSet.bind(state_base, resolved.offset);
    if (inner.insertKey(elem)) |was_new| {
        if (was_new) setChangeFlag(meta, ChangeFlag.INSERTED);
        return .OK;
    }

    // Inner needs growth — allocate 2×, rehash, update outer pointer
    const new_size = HashSet.byteSize(inner.cap * 2);
    const new_off = outer.arena.alloc(new_size) orelse return .CAPACITY_EXCEEDED;
    inner = inner.rehashInto(state_base, new_off, inner.cap * 2);
    outer.updatePtr(outer_key, new_off);

    _ = inner.insertKey(elem);
    setChangeFlag(meta, ChangeFlag.INSERTED);
    return .OK;
}

/// Upsert into nested map: Map<outer_key, Map<inner_key, value>>
pub fn nestedMapUpsertLast(state_base: [*]u8, meta: SlotMeta, outer_key: u32, inner_key: u32, value: u32) ErrorCode {
    const prefix = readNestedPrefix(state_base, meta.offset);
    const outer = OuterTable.bind(state_base, meta);
    const resolved = outer.resolve(outer_key, prefix) orelse return .CAPACITY_EXCEEDED;

    if (resolved.is_new) setChangeFlag(meta, ChangeFlag.INSERTED);

    var inner = HashMap.bind(state_base, resolved.offset);
    if (inner.upsert(inner_key, value)) |was_new| {
        if (was_new) setChangeFlag(meta, ChangeFlag.INSERTED) else setChangeFlag(meta, ChangeFlag.UPDATED);
        return .OK;
    }

    // Inner needs growth
    const new_size = HashMap.byteSize(inner.cap * 2);
    const new_off = outer.arena.alloc(new_size) orelse return .CAPACITY_EXCEEDED;
    inner = inner.rehashInto(state_base, new_off, inner.cap * 2);
    outer.updatePtr(outer_key, new_off);

    _ = inner.upsert(inner_key, value);
    setChangeFlag(meta, ChangeFlag.INSERTED);
    return .OK;
}

/// Update nested aggregate: Map<outer_key, Agg>.update(value)
pub fn nestedAggUpdate(state_base: [*]u8, meta: SlotMeta, outer_key: u32, value_bits: u64) ErrorCode {
    const prefix = readNestedPrefix(state_base, meta.offset);
    const outer = OuterTable.bind(state_base, meta);
    const resolved = outer.resolve(outer_key, prefix) orelse return .CAPACITY_EXCEEDED;

    if (resolved.is_new) setChangeFlag(meta, ChangeFlag.INSERTED);

    // Aggregate is fixed-size, no hash table — just raw bytes
    const base = state_base + resolved.offset;
    switch (prefix.inner_agg_type) {
        .COUNT => {
            const count_ptr: *align(1) u64 = @ptrCast(base);
            count_ptr.* += 1;
        },
        .SUM => {
            const val_ptr: *align(1) f64 = @ptrCast(base);
            const count_ptr: *align(1) u64 = @ptrCast(base + 8);
            val_ptr.* += @bitCast(value_bits);
            count_ptr.* += 1;
        },
        .MIN => {
            const val_ptr: *align(1) f64 = @ptrCast(base);
            const count_ptr: *align(1) u64 = @ptrCast(base + 8);
            const new_val: f64 = @bitCast(value_bits);
            if (count_ptr.* == 0 or new_val < val_ptr.*) val_ptr.* = new_val;
            count_ptr.* += 1;
        },
        .MAX => {
            const val_ptr: *align(1) f64 = @ptrCast(base);
            const count_ptr: *align(1) u64 = @ptrCast(base + 8);
            const new_val: f64 = @bitCast(value_bits);
            if (count_ptr.* == 0 or new_val > val_ptr.*) val_ptr.* = new_val;
            count_ptr.* += 1;
        },
        else => {},
    }
    setChangeFlag(meta, ChangeFlag.SIZE_CHANGED);
    return .OK;
}

// =============================================================================
// Read operations
// =============================================================================

pub fn getInnerOffset(state_base: [*]u8, meta: SlotMeta, outer_key: u32) u32 {
    return OuterTable.bind(state_base, meta).lookup(outer_key);
}

pub fn getInnerSetSize(state_base: [*]u8, inner_offset: u32) u32 {
    if (inner_offset == 0) return 0;
    return HashSet.bind(state_base, inner_offset).size();
}

pub fn innerSetContains(state_base: [*]u8, inner_offset: u32, elem: u32) bool {
    if (inner_offset == 0) return false;
    return HashSet.bind(state_base, inner_offset).contains(elem);
}

pub fn innerMapGet(state_base: [*]u8, inner_offset: u32, key: u32) u32 {
    if (inner_offset == 0) return EMPTY_KEY;
    const entry = HashMap.bind(state_base, inner_offset).get(key) orelse return EMPTY_KEY;
    return entry.*;
}

pub fn innerAggGetF64(state_base: [*]u8, inner_offset: u32) f64 {
    if (inner_offset == 0) return 0;
    const val_ptr: *align(1) const f64 = @ptrCast(state_base + inner_offset);
    return val_ptr.*;
}

pub fn innerAggGetCount(state_base: [*]u8, inner_offset: u32, agg_type: AggType) u64 {
    if (inner_offset == 0) return 0;
    const count_off: u32 = if (agg_type == .COUNT) 0 else 8;
    const count_ptr: *align(1) const u64 = @ptrCast(state_base + inner_offset + count_off);
    return count_ptr.*;
}

// =============================================================================
// Tests — colocated with implementation
// =============================================================================

const testing = std.testing;

fn setupNestedTestSlot(
    state: []u8,
    outer_cap: u32,
    inner_type: SlotType,
    inner_initial_cap: u16,
    inner_agg_type: AggType,
) SlotMeta {
    const slot_offset: u32 = 64;

    writeNestedPrefix(state.ptr, slot_offset, .{
        .inner_type = inner_type,
        .inner_initial_cap = inner_initial_cap,
        .inner_agg_type = inner_agg_type,
        .depth = 1,
    });

    // Init outer keys to EMPTY_KEY
    const keys: [*]u32 = @ptrCast(@alignCast(state.ptr + outerKeysOffset(slot_offset)));
    for (0..outer_cap) |i| keys[i] = EMPTY_KEY;

    // Init arena
    const arena_hdr = arenaHeaderOffset(slot_offset, outer_cap);
    const arena_start = arenaDataOffset(slot_offset, outer_cap);
    const arena_cap: u32 = @as(u32, @intCast(state.len)) - arena_start;
    _ = Arena.initAt(state.ptr, arena_hdr, arena_cap);

    // Store outer size at fixed location
    const size_loc: u32 = 32;
    std.mem.writeInt(u32, state[size_loc..][0..4], 0, .little);

    return SlotMeta{
        .offset = slot_offset,
        .capacity = outer_cap,
        .size_ptr = @ptrCast(@alignCast(&state[size_loc])),
        .type_flags = .{ .slot_type = .NESTED, .has_ttl = false, .has_evict_trigger = false },
        .agg_type = .SUM,
        .change_flags_ptr = @ptrCast(&state[40]),
        .timestamp_field_idx = 0,
        .ttl_seconds = 0,
        .grace_seconds = 0,
        .start_of = .NONE,
        .eviction_index_offset = 0,
        .eviction_index_capacity = 0,
        .eviction_index_size_ptr = @ptrCast(@alignCast(&state[44])),
        .evicted_buffer_offset = 0,
        .evicted_count_ptr = @ptrCast(@alignCast(&state[48])),
    };
}

test "nested set — basic insert and contains" {
    var state: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const meta = setupNestedTestSlot(&state, 16, .HASHSET, 16, .SUM);

    try testing.expectEqual(ErrorCode.OK, nestedSetInsert(&state, meta, 100, 42));
    try testing.expectEqual(ErrorCode.OK, nestedSetInsert(&state, meta, 100, 43));
    try testing.expectEqual(ErrorCode.OK, nestedSetInsert(&state, meta, 200, 42));

    try testing.expectEqual(@as(u32, 2), meta.size_ptr.*);

    const inner_100 = getInnerOffset(&state, meta, 100);
    try testing.expect(inner_100 != 0);
    try testing.expectEqual(@as(u32, 2), getInnerSetSize(&state, inner_100));
    try testing.expect(innerSetContains(&state, inner_100, 42));
    try testing.expect(innerSetContains(&state, inner_100, 43));
    try testing.expect(!innerSetContains(&state, inner_100, 44));
}

test "nested set — duplicate insert is idempotent" {
    var state: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const meta = setupNestedTestSlot(&state, 16, .HASHSET, 16, .SUM);

    _ = nestedSetInsert(&state, meta, 100, 42);
    _ = nestedSetInsert(&state, meta, 100, 42);
    _ = nestedSetInsert(&state, meta, 100, 42);

    try testing.expectEqual(@as(u32, 1), getInnerSetSize(&state, getInnerOffset(&state, meta, 100)));
}

test "nested map — upsert last-write-wins" {
    var state: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const meta = setupNestedTestSlot(&state, 16, .HASHMAP, 16, .SUM);

    _ = nestedMapUpsertLast(&state, meta, 1, 10, 100);
    _ = nestedMapUpsertLast(&state, meta, 1, 11, 200);
    _ = nestedMapUpsertLast(&state, meta, 1, 10, 300); // overwrite

    const inner = getInnerOffset(&state, meta, 1);
    try testing.expectEqual(@as(u32, 300), innerMapGet(&state, inner, 10));
    try testing.expectEqual(@as(u32, 200), innerMapGet(&state, inner, 11));
}

test "nested aggregate — count per outer key" {
    var state: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const meta = setupNestedTestSlot(&state, 16, .AGGREGATE, 0, .COUNT);

    _ = nestedAggUpdate(&state, meta, 1, 0);
    _ = nestedAggUpdate(&state, meta, 1, 0);
    _ = nestedAggUpdate(&state, meta, 1, 0);
    _ = nestedAggUpdate(&state, meta, 2, 0);

    try testing.expectEqual(@as(u64, 3), innerAggGetCount(&state, getInnerOffset(&state, meta, 1), .COUNT));
    try testing.expectEqual(@as(u64, 1), innerAggGetCount(&state, getInnerOffset(&state, meta, 2), .COUNT));
}

test "nested aggregate — sum f64 per outer key" {
    var state: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const meta = setupNestedTestSlot(&state, 16, .AGGREGATE, 0, .SUM);

    _ = nestedAggUpdate(&state, meta, 1, @bitCast(@as(f64, 100.0)));
    _ = nestedAggUpdate(&state, meta, 1, @bitCast(@as(f64, 250.5)));
    _ = nestedAggUpdate(&state, meta, 2, @bitCast(@as(f64, 50.0)));

    try testing.expectApproxEqAbs(@as(f64, 350.5), innerAggGetF64(&state, getInnerOffset(&state, meta, 1)), 0.001);
    try testing.expectApproxEqAbs(@as(f64, 50.0), innerAggGetF64(&state, getInnerOffset(&state, meta, 2)), 0.001);
}

test "nested set — inner growth on load factor exceeded" {
    var state: [16384]u8 align(8) = [_]u8{0} ** 16384;
    const meta = setupNestedTestSlot(&state, 16, .HASHSET, 4, .SUM);

    var i: u32 = 1;
    while (i <= 20) : (i += 1) {
        try testing.expectEqual(ErrorCode.OK, nestedSetInsert(&state, meta, 1, i));
    }

    const inner = getInnerOffset(&state, meta, 1);
    try testing.expectEqual(@as(u32, 20), getInnerSetSize(&state, inner));
    i = 1;
    while (i <= 20) : (i += 1) {
        try testing.expect(innerSetContains(&state, inner, i));
    }
}

test "stress — arena accounting: each outer key allocates one inner set" {
    var state: [32768]u8 align(8) = [_]u8{0} ** 32768;
    const meta = setupNestedTestSlot(&state, 64, .HASHSET, 8, .SUM);

    var k: u32 = 1;
    while (k <= 10) : (k += 1) {
        try testing.expectEqual(ErrorCode.OK, nestedSetInsert(&state, meta, k, 42));
    }

    try testing.expectEqual(@as(u32, 10), meta.size_ptr.*);
    // Verify arena used > 0 (inner containers allocated)
    const arena = Arena.bind(&state, arenaHeaderOffset(meta.offset, meta.capacity));
    try testing.expect(arena.used() > 0);
}

test "stress — randomized Map<K, Set<V>> with ground truth" {
    var state: [262144]u8 align(8) = [_]u8{0} ** 262144;
    const meta = setupNestedTestSlot(&state, 64, .HASHSET, 8, .SUM);

    var rng = std.Random.DefaultPrng.init(0xDEADBEEF);
    const random = rng.random();

    var truth: [32]std.ArrayList(u32) = undefined;
    for (&truth) |*t| t.* = .empty;
    defer for (&truth) |*t| t.deinit(testing.allocator);

    var ops: u32 = 0;
    while (ops < 500) : (ops += 1) {
        const outer_key = random.intRangeAtMost(u32, 1, 30);
        const elem = random.intRangeAtMost(u32, 1, 200);
        try testing.expectEqual(ErrorCode.OK, nestedSetInsert(&state, meta, outer_key, elem));

        const idx = outer_key - 1;
        var found = false;
        for (truth[idx].items) |v| {
            if (v == elem) { found = true; break; }
        }
        if (!found) try truth[idx].append(testing.allocator, elem);
    }

    for (0..30) |idx| {
        const outer_key: u32 = @intCast(idx + 1);
        const inner = getInnerOffset(&state, meta, outer_key);
        if (truth[idx].items.len == 0) {
            try testing.expectEqual(@as(u32, 0), inner);
            continue;
        }
        try testing.expect(inner != 0);
        try testing.expectEqual(@as(u32, @intCast(truth[idx].items.len)), getInnerSetSize(&state, inner));
        for (truth[idx].items) |elem| {
            try testing.expect(innerSetContains(&state, inner, elem));
        }
    }
}

test "stress — CAPACITY_EXCEEDED when arena exhausted" {
    var state: [1024]u8 align(8) = [_]u8{0} ** 1024;
    const meta = setupNestedTestSlot(&state, 16, .HASHSET, 4, .SUM);

    var got_exceeded = false;
    var i: u32 = 1;
    while (i <= 100) : (i += 1) {
        if (nestedSetInsert(&state, meta, i, 42) == .CAPACITY_EXCEEDED) {
            got_exceeded = true;
            break;
        }
    }
    try testing.expect(got_exceeded);
    try testing.expect(meta.size_ptr.* > 0);
}

// =============================================================================
// E2E tests — full VM pipeline
// =============================================================================

const vm = @import("vm.zig");

fn buildNestedSetE2EProgram(type_id: u32) [128]u8 {
    const PHASH = types.PROGRAM_HASH_PREFIX;
    var prog = [_]u8{0} ** 128;
    const c = prog[PHASH..];

    // Header
    c[0] = 0x41; c[1] = 0x58; c[2] = 0x45; c[3] = 0x31; // "CLM1"
    c[4] = 1; c[5] = 0; // version
    c[6] = 1; c[7] = 3; // 1 slot, 3 columns
    c[8] = 0; c[9] = 0; // reserved

    const init_len: u16 = 9; // SLOT_NESTED: 1 opcode + 8 params
    c[10] = @truncate(init_len); c[11] = @truncate(init_len >> 8);
    const body_len: u16 = 4; // NESTED_SET_INSERT: 4 bytes
    const reduce_len: u16 = 9 + body_len; // FOR_EACH header(9: op+col+match_count+1*u32+u16) + body
    c[12] = @truncate(reduce_len); c[13] = @truncate(reduce_len >> 8);

    // Init: SLOT_NESTED
    const off: usize = 14;
    c[off] = 0x1A; // SLOT_NESTED opcode
    c[off+1] = 0; // slot 0
    c[off+2] = 0x09; // type_flags: NESTED=9
    c[off+3] = 32; c[off+4] = 0; // outer_cap = 32
    c[off+5] = 1; // inner_type = HASHSET
    c[off+6] = 8; c[off+7] = 0; // inner_cap = 8
    c[off+8] = 1; // inner_agg = SUM (unused for set, but must be valid)

    // Reduce: FOR_EACH { NESTED_SET_INSERT }
    const rs = 14 + init_len;
    c[rs] = 0xE0; // FOR_EACH
    c[rs+1] = 0; // col (type_col)
    c[rs+2] = 1; // match_count
    c[rs+3] = @truncate(type_id); c[rs+4] = @truncate(type_id >> 8);
    c[rs+5] = @truncate(type_id >> 16); c[rs+6] = @truncate(type_id >> 24);
    c[rs+7] = @truncate(body_len); c[rs+8] = @truncate(body_len >> 8);

    const bs = rs + 9;
    c[bs] = 0x90; // NESTED_SET_INSERT
    c[bs+1] = 0; // slot
    c[bs+2] = 1; // outer_key_col
    c[bs+3] = 2; // elem_col

    return prog;
}

test "e2e — SLOT_NESTED state init roundtrip" {
    const prog = buildNestedSetE2EProgram(1);

    const state_size = vm.vm_calculate_state_size(@ptrCast(&prog), prog.len);
    try testing.expect(state_size > 0);
    try testing.expect(state_size <= 32768);

    var state_buf: [32768]u8 align(8) = [_]u8{0} ** 32768;
    const state_ptr: [*]u8 = @ptrCast(&state_buf);
    try testing.expectEqual(@as(u32, 0), vm.vm_init_state(state_ptr, @ptrCast(&prog), prog.len));

    const m = types.getSlotMeta(state_ptr, 0);
    try testing.expect(m.offset > 0);
    try testing.expect(m.capacity >= 32);

    const prefix = readNestedPrefix(state_ptr, m.offset);
    try testing.expectEqual(SlotType.HASHSET, prefix.inner_type);
    try testing.expectEqual(@as(u8, 1), prefix.depth);
}

test "e2e — NESTED_SET_INSERT through full VM pipeline" {
    const type_id: u32 = 1;
    const prog = buildNestedSetE2EProgram(type_id);

    const state_size = vm.vm_calculate_state_size(@ptrCast(&prog), prog.len);
    try testing.expect(state_size > 0 and state_size <= 32768);

    var state_buf: [32768]u8 align(8) = [_]u8{0} ** 32768;
    const state_ptr: [*]u8 = @ptrCast(&state_buf);
    try testing.expectEqual(@as(u32, 0), vm.vm_init_state(state_ptr, @ptrCast(&prog), prog.len));

    // 4 events: (type=1, outer_key, elem)
    var type_data = [_]u32{ type_id, type_id, type_id, type_id };
    var outer_keys = [_]u32{ 10, 10, 20, 10 };
    var elems = [_]u32{ 100, 101, 100, 100 }; // last is dup for key=10

    const col_ptrs = [_][*]const u8{
        @as([*]const u8, @ptrCast(&type_data)),
        @as([*]const u8, @ptrCast(&outer_keys)),
        @as([*]const u8, @ptrCast(&elems)),
    };

    const exec_result = vm.vm_execute_batch(
        state_ptr,
        @ptrCast(&prog),
        prog.len,
        @ptrCast(&col_ptrs),
        3,
        4,
    );
    try testing.expectEqual(@as(u32, 0), exec_result);

    // Verify: 2 outer keys, key 10 has {100, 101}, key 20 has {100}
    const meta = types.getSlotMeta(state_ptr, 0);
    try testing.expectEqual(@as(u32, 2), meta.size_ptr.*);

    const inner_10 = getInnerOffset(state_ptr, meta, 10);
    try testing.expect(inner_10 != 0);
    try testing.expectEqual(@as(u32, 2), getInnerSetSize(state_ptr, inner_10));
    try testing.expect(innerSetContains(state_ptr, inner_10, 100));
    try testing.expect(innerSetContains(state_ptr, inner_10, 101));

    const inner_20 = getInnerOffset(state_ptr, meta, 20);
    try testing.expect(inner_20 != 0);
    try testing.expectEqual(@as(u32, 1), getInnerSetSize(state_ptr, inner_20));
    try testing.expect(innerSetContains(state_ptr, inner_20, 100));
}

// =============================================================================
// Stress tests — arena fragmentation, multi-seed randomization, capacity limits
// =============================================================================

test "stress — inner growth cascade tracks arena fragmentation" {
    var state: [131072]u8 align(8) = [_]u8{0} ** 131072;
    const meta = setupNestedTestSlot(&state, 16, .HASHSET, 4, .SUM);

    // Insert 100 elements into ONE outer key, forcing inner set growth: 4->8->16->32->64->128
    var i: u32 = 1;
    while (i <= 100) : (i += 1) {
        try testing.expectEqual(ErrorCode.OK, nestedSetInsert(&state, meta, 1, i));
    }

    // All 100 elements must be present
    const inner = getInnerOffset(&state, meta, 1);
    try testing.expect(inner != 0);
    i = 1;
    while (i <= 100) : (i += 1) {
        try testing.expect(innerSetContains(&state, inner, i));
    }

    // Arena must have allocated something
    const arena = Arena.bind(&state, arenaHeaderOffset(meta.offset, meta.capacity));
    try testing.expect(arena.used() > 0);

    // Dead space: old capacities 4, 8, 16, 32, 64 are abandoned in the arena.
    // The live inner set has cap=128 (byteSize(128)). Everything else is fragmentation.
    const dead_space = HashSet.byteSize(4) + HashSet.byteSize(8) + HashSet.byteSize(16) + HashSet.byteSize(32) + HashSet.byteSize(64);
    const live_size = HashSet.byteSize(128);

    // Arena used must exceed the live data size, proving fragmentation exists
    try testing.expect(arena.used() >= live_size + dead_space);
}

test "stress — multi-seed randomized Map<K, Set<V>>" {
    const seeds = [_]u64{ 0xDEADBEEF, 0xCAFEBABE, 0x12345678, 0xFEEDFACE };

    inline for (seeds) |seed| {
        var state: [262144]u8 align(8) = [_]u8{0} ** 262144;
        const meta = setupNestedTestSlot(&state, 64, .HASHSET, 8, .SUM);

        var rng = std.Random.DefaultPrng.init(seed);
        const random = rng.random();

        var truth: [32]std.ArrayList(u32) = undefined;
        for (&truth) |*t| t.* = .empty;
        defer for (&truth) |*t| t.deinit(testing.allocator);

        var ops: u32 = 0;
        while (ops < 500) : (ops += 1) {
            const outer_key = random.intRangeAtMost(u32, 1, 30);
            const elem = random.intRangeAtMost(u32, 1, 200);
            try testing.expectEqual(ErrorCode.OK, nestedSetInsert(&state, meta, outer_key, elem));

            const idx = outer_key - 1;
            var found = false;
            for (truth[idx].items) |v| {
                if (v == elem) {
                    found = true;
                    break;
                }
            }
            if (!found) try truth[idx].append(testing.allocator, elem);
        }

        for (0..30) |idx| {
            const outer_key: u32 = @intCast(idx + 1);
            const inner = getInnerOffset(&state, meta, outer_key);
            if (truth[idx].items.len == 0) {
                try testing.expectEqual(@as(u32, 0), inner);
                continue;
            }
            try testing.expect(inner != 0);
            try testing.expectEqual(@as(u32, @intCast(truth[idx].items.len)), getInnerSetSize(&state, inner));
            for (truth[idx].items) |elem| {
                try testing.expect(innerSetContains(&state, inner, elem));
            }
        }
    }
}

test "stress — many outer keys fill arena to capacity" {
    var state: [65536]u8 align(8) = [_]u8{0} ** 65536;
    const meta = setupNestedTestSlot(&state, 128, .HASHSET, 4, .SUM);

    var successful_keys: u32 = 0;
    var k: u32 = 1;
    while (k <= 128) : (k += 1) {
        if (nestedSetInsert(&state, meta, k, k * 10) == .OK) {
            successful_keys += 1;
        } else {
            break;
        }
    }

    // Some keys succeeded, but not all (arena exhausted before outer table full)
    try testing.expect(successful_keys > 0);
    try testing.expect(successful_keys < 128);

    // Every successful key's inner set must contain its element
    var v: u32 = 1;
    while (v <= successful_keys) : (v += 1) {
        try testing.expect(innerSetContains(&state, getInnerOffset(&state, meta, v), v * 10));
    }
}

test "stress — inner map growth preserves all entries" {
    var state: [131072]u8 align(8) = [_]u8{0} ** 131072;
    const meta = setupNestedTestSlot(&state, 16, .HASHMAP, 4, .SUM);

    // Insert 50 key-value pairs into inner map under outer key=1. Values = key * 10.
    // Forces growth: 4->8->16->32->64
    var i: u32 = 1;
    while (i <= 50) : (i += 1) {
        try testing.expectEqual(ErrorCode.OK, nestedMapUpsertLast(&state, meta, 1, i, i * 10));
    }

    // Verify all 50 entries present with correct values
    const inner = getInnerOffset(&state, meta, 1);
    try testing.expect(inner != 0);
    i = 1;
    while (i <= 50) : (i += 1) {
        try testing.expectEqual(i * 10, innerMapGet(&state, inner, i));
    }
}
