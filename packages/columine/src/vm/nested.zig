// =============================================================================
// Nested Container Slots — Arena-allocated inner maps, sets, and aggregates
// =============================================================================
//
// Supports arbitrary nesting depth:
//   Map<K, Set<V>>                   → depth=1, leaf=HASHSET
//   Map<K, Map<K2, V>>              → depth=1, leaf=HASHMAP
//   Map<K, Map<K2, Set<V>>>         → depth=2, leaf=HASHSET
//   Map<K, Agg>                     → depth=1, leaf=AGGREGATE
//
// Memory layout (all byte offsets from state_base):
//
//   Outer hash table:
//     [keys:         u32 × outer_cap]   — hash probing (EMPTY_KEY = absent)
//     [inner_ptrs:   u32 × outer_cap]   — byte offset from state_base to inner container
//     [arena_header: 8 bytes]            — arena_capacity(u32) + arena_used(u32)
//     [arena_data:   arena_capacity]     — bump-allocated inner containers
//
//   Inner container (allocated in arena on first access):
//     MAP:  [cap:u32][size:u32][keys:u32×cap][values:u32×cap][timestamps:f64×cap]
//     SET:  [cap:u32][size:u32][keys:u32×cap]
//     AGG:  [value:f64/i64 (8 bytes)][count:u64 (8 bytes)] = 16 bytes (or 8 for COUNT)
//     NESTED: [cap:u32][size:u32][keys:u32×cap][inner_ptrs:u32×cap][arena_hdr:8][arena:...]
//
// Inner growth:
//   When inner hash table exceeds 70% load → allocate 2× in arena, rehash, update parent ptr.
//   Old space is abandoned (arena fragmentation). Compacted during vm_grow_state.
//
// Undo log:
//   NESTED_MAP_INSERT: (slot, outer_key, inner_key) → rollback: tombstone inner, dec size
//   NESTED_MAP_UPDATE: (slot, outer_key, inner_key, prev_val, prev_ts) → restore
//   NESTED_SET_INSERT: (slot, outer_key, elem) → rollback: tombstone, dec size
//   NESTED_OUTER_ALLOC: (slot, outer_key, prev_arena_used) → reset arena ptr, tombstone outer
//   NESTED_INNER_GROW: (slot, outer_key, old_inner_ptr) → restore old ptr
//
// The arena checkpoint at vm_undo_checkpoint saves arena_used. On rollback,
// arena_used resets to checkpoint value, reclaiming all speculative allocations.

const std = @import("std");
const types = @import("types.zig");

const EMPTY_KEY = types.EMPTY_KEY;
const TOMBSTONE = types.TOMBSTONE;
const hashKey = types.hashKey;
const align8 = types.align8;
const nextPowerOf2 = types.nextPowerOf2;
const SlotType = types.SlotType;
const AggType = types.AggType;
const ErrorCode = types.ErrorCode;
const SlotMeta = types.SlotMeta;
const getSlotMeta = types.getSlotMeta;
const setChangeFlag = types.setChangeFlag;
const ChangeFlag = types.ChangeFlag;

// =============================================================================
// Arena header offsets (relative to arena start within slot data)
// =============================================================================

const ARENA_HDR_CAPACITY: u32 = 0;
const ARENA_HDR_USED: u32 = 4;
const ARENA_HDR_SIZE: u32 = 8;

// Inner container header: capacity(u32) + size(u32) = 8 bytes
const INNER_HDR_CAP: u32 = 0;
const INNER_HDR_SIZE: u32 = 4;
const INNER_HDR_BYTES: u32 = 8;

// =============================================================================
// Nested slot metadata — stored in slot metadata extended region
// =============================================================================
// For nested slots, the standard SlotMeta fields apply to the OUTER hash table:
//   offset   → byte offset to outer keys
//   capacity → outer hash table capacity
//   size     → outer hash table current entry count
//
// Additional nested-specific data is stored in the slot data prefix:
//   [inner_type: u8]           — SlotType of the inner container
//   [inner_initial_cap: u16]   — initial capacity for new inner containers
//   [inner_agg_type: u8]       — AggType for AGGREGATE leaf (or 0)
//   [depth: u8]                — nesting depth (1 = Map<K, leaf>, 2 = Map<K, Map<K2, leaf>>)
//   [padding: 3 bytes]         — align to 8

pub const NESTED_PREFIX_SIZE: u32 = 8;

pub const NestedPrefix = struct {
    inner_type: SlotType,
    inner_initial_cap: u16,
    inner_agg_type: AggType,
    depth: u8,
};

pub fn readNestedPrefix(state_base: [*]u8, slot_offset: u32) NestedPrefix {
    const base = state_base + slot_offset;
    return .{
        .inner_type = @enumFromInt(@as(u4, @truncate(base[0]))),
        .inner_initial_cap = @as(u16, base[1]) | (@as(u16, base[2]) << 8),
        .inner_agg_type = @enumFromInt(base[3]),
        .depth = base[4],
    };
}

fn writeNestedPrefix(state_base: [*]u8, slot_offset: u32, prefix: NestedPrefix) void {
    const base = state_base + slot_offset;
    base[0] = @intFromEnum(prefix.inner_type);
    base[1] = @truncate(prefix.inner_initial_cap);
    base[2] = @truncate(prefix.inner_initial_cap >> 8);
    base[3] = @intFromEnum(prefix.inner_agg_type);
    base[4] = prefix.depth;
    base[5] = 0;
    base[6] = 0;
    base[7] = 0;
}

// =============================================================================
// Slot data layout helpers
// =============================================================================

/// Outer hash table layout (after NESTED_PREFIX_SIZE prefix):
///   [keys: u32 × cap] [inner_ptrs: u32 × cap] [arena_hdr: 8] [arena_data: ...]
fn outerKeysOffset(slot_offset: u32) u32 {
    return slot_offset + NESTED_PREFIX_SIZE;
}

fn outerPtrsOffset(slot_offset: u32, capacity: u32) u32 {
    return outerKeysOffset(slot_offset) + capacity * 4;
}

fn arenaHeaderOffset(slot_offset: u32, capacity: u32) u32 {
    return outerPtrsOffset(slot_offset, capacity) + capacity * 4;
}

fn arenaDataOffset(slot_offset: u32, capacity: u32) u32 {
    return arenaHeaderOffset(slot_offset, capacity) + ARENA_HDR_SIZE;
}

/// Read arena capacity from arena header
fn getArenaCapacity(state_base: [*]u8, arena_hdr: u32) u32 {
    return std.mem.readInt(u32, (state_base + arena_hdr)[0..4], .little);
}

/// Read arena used from arena header
fn getArenaUsed(state_base: [*]u8, arena_hdr: u32) u32 {
    return std.mem.readInt(u32, (state_base + arena_hdr + ARENA_HDR_USED)[0..4], .little);
}

/// Write arena used
fn setArenaUsed(state_base: [*]u8, arena_hdr: u32, used: u32) void {
    std.mem.writeInt(u32, (state_base + arena_hdr + ARENA_HDR_USED)[0..4], used, .little);
}

// =============================================================================
// Inner container size calculation
// =============================================================================

/// Size in bytes for an inner container at a given capacity.
pub fn innerContainerSize(inner_type: SlotType, capacity: u32, inner_agg_type: AggType) u32 {
    return switch (inner_type) {
        .HASHMAP => INNER_HDR_BYTES + capacity * 4 + capacity * 4 + capacity * 8, // hdr + keys + values + timestamps
        .HASHSET => INNER_HDR_BYTES + capacity * 4, // hdr + keys
        .AGGREGATE => switch (inner_agg_type) {
            .COUNT => 8, // u64 count only
            else => 16, // value(f64/i64) + count(u64)
        },
        else => 0,
    };
}

/// Calculate total slot data size for a nested slot.
pub fn nestedSlotDataSize(outer_cap: u32, inner_initial_cap: u32, inner_type: SlotType, inner_agg_type: AggType) u32 {
    const prefix = NESTED_PREFIX_SIZE;
    const outer_keys = outer_cap * 4;
    const outer_ptrs = outer_cap * 4;
    const arena_hdr = ARENA_HDR_SIZE;
    // Initial arena: enough for outer_cap inner containers at initial capacity
    const per_inner = align8(innerContainerSize(inner_type, inner_initial_cap, inner_agg_type));
    const arena_data = outer_cap * per_inner;
    return prefix + outer_keys + outer_ptrs + arena_hdr + arena_data;
}

// =============================================================================
// Arena bump allocator
// =============================================================================

/// Allocate `size` bytes from the arena. Returns absolute offset from state_base,
/// or 0 if arena is full (caller should return NEEDS_GROWTH).
fn arenaAlloc(state_base: [*]u8, arena_hdr: u32, arena_start: u32, size: u32) ?u32 {
    const used = getArenaUsed(state_base, arena_hdr);
    const cap = getArenaCapacity(state_base, arena_hdr);
    const aligned_size = align8(size);
    if (used + aligned_size > cap) return null;
    const offset = arena_start + used;
    setArenaUsed(state_base, arena_hdr, used + aligned_size);
    return offset;
}

// =============================================================================
// Inner container initialization
// =============================================================================

/// Initialize an inner hash map at the given absolute offset.
fn initInnerHashMap(state_base: [*]u8, offset: u32, capacity: u32) void {
    const base = state_base + offset;
    std.mem.writeInt(u32, base[INNER_HDR_CAP..][0..4], capacity, .little);
    std.mem.writeInt(u32, base[INNER_HDR_SIZE..][0..4], 0, .little);
    // Fill keys with EMPTY_KEY
    const keys: [*]u32 = @ptrCast(@alignCast(base + INNER_HDR_BYTES));
    for (0..capacity) |i| {
        keys[i] = EMPTY_KEY;
    }
}

/// Initialize an inner hash set at the given absolute offset.
fn initInnerHashSet(state_base: [*]u8, offset: u32, capacity: u32) void {
    const base = state_base + offset;
    std.mem.writeInt(u32, base[INNER_HDR_CAP..][0..4], capacity, .little);
    std.mem.writeInt(u32, base[INNER_HDR_SIZE..][0..4], 0, .little);
    const keys: [*]u32 = @ptrCast(@alignCast(base + INNER_HDR_BYTES));
    for (0..capacity) |i| {
        keys[i] = EMPTY_KEY;
    }
}

/// Initialize an inner aggregate at the given absolute offset (zero-filled).
fn initInnerAggregate(state_base: [*]u8, offset: u32, agg_type: AggType) void {
    const size: u32 = if (agg_type == .COUNT) 8 else 16;
    @memset((state_base + offset)[0..size], 0);
}

/// Allocate and initialize a new inner container from the arena.
/// Returns the absolute offset, or null if arena is full.
fn allocInnerContainer(
    state_base: [*]u8,
    arena_hdr: u32,
    arena_start: u32,
    inner_type: SlotType,
    inner_initial_cap: u32,
    inner_agg_type: AggType,
) ?u32 {
    const cap = nextPowerOf2(inner_initial_cap);
    const size = innerContainerSize(inner_type, cap, inner_agg_type);
    const offset = arenaAlloc(state_base, arena_hdr, arena_start, size) orelse return null;

    switch (inner_type) {
        .HASHMAP => initInnerHashMap(state_base, offset, cap),
        .HASHSET => initInnerHashSet(state_base, offset, cap),
        .AGGREGATE => initInnerAggregate(state_base, offset, inner_agg_type),
        else => {},
    }
    return offset;
}

// =============================================================================
// Inner hash table operations
// =============================================================================

/// Find key in inner hash table. Returns slot index or capacity (not found).
fn innerFindKey(state_base: [*]u8, inner_offset: u32, key: u32) struct { pos: u32, cap: u32 } {
    const base = state_base + inner_offset;
    const cap = std.mem.readInt(u32, base[INNER_HDR_CAP..][0..4], .little);
    const keys: [*]u32 = @ptrCast(@alignCast(base + INNER_HDR_BYTES));

    var pos = hashKey(key, cap);
    var probes: u32 = 0;
    while (probes < cap) : (probes += 1) {
        if (keys[pos] == key) return .{ .pos = pos, .cap = cap };
        if (keys[pos] == EMPTY_KEY) return .{ .pos = cap, .cap = cap }; // not found
        pos = (pos + 1) & (cap - 1);
    }
    return .{ .pos = cap, .cap = cap };
}

/// Find insert position in inner hash table. Returns slot index.
fn innerFindInsertPos(state_base: [*]u8, inner_offset: u32, key: u32) struct { pos: u32, found: bool, cap: u32 } {
    const base = state_base + inner_offset;
    const cap = std.mem.readInt(u32, base[INNER_HDR_CAP..][0..4], .little);
    const keys: [*]u32 = @ptrCast(@alignCast(base + INNER_HDR_BYTES));

    var pos = hashKey(key, cap);
    var probes: u32 = 0;
    while (probes < cap) : (probes += 1) {
        const k = keys[pos];
        if (k == key) return .{ .pos = pos, .found = true, .cap = cap };
        if (k == EMPTY_KEY or k == TOMBSTONE) return .{ .pos = pos, .found = false, .cap = cap };
        pos = (pos + 1) & (cap - 1);
    }
    return .{ .pos = cap, .found = false, .cap = cap };
}

fn innerGetSize(state_base: [*]u8, inner_offset: u32) u32 {
    return std.mem.readInt(u32, (state_base + inner_offset + INNER_HDR_SIZE)[0..4], .little);
}

fn innerSetSize(state_base: [*]u8, inner_offset: u32, size: u32) void {
    std.mem.writeInt(u32, (state_base + inner_offset + INNER_HDR_SIZE)[0..4], size, .little);
}

fn innerGetCap(state_base: [*]u8, inner_offset: u32) u32 {
    return std.mem.readInt(u32, (state_base + inner_offset + INNER_HDR_CAP)[0..4], .little);
}

// =============================================================================
// Inner set operations
// =============================================================================

/// Insert element into inner hash set. Returns true if new insert, false if already present.
/// Returns null if capacity exceeded (need inner growth).
fn innerSetInsert(state_base: [*]u8, inner_offset: u32, elem: u32) ?bool {
    if (elem == EMPTY_KEY or elem == TOMBSTONE) return false;

    const probe = innerFindInsertPos(state_base, inner_offset, elem);
    if (probe.found) return false;
    if (probe.pos >= probe.cap) return null;

    // Check load factor (70%)
    const size = innerGetSize(state_base, inner_offset);
    const max_size = (probe.cap * 7) / 10;
    if (size >= max_size) return null;

    // Insert
    const base = state_base + inner_offset;
    const keys: [*]u32 = @ptrCast(@alignCast(base + INNER_HDR_BYTES));
    keys[probe.pos] = elem;
    innerSetSize(state_base, inner_offset, size + 1);
    return true;
}

// =============================================================================
// Inner map operations
// =============================================================================

/// Values array starts after keys in inner hash map.
fn innerMapValuesOffset(inner_offset: u32, cap: u32) u32 {
    return inner_offset + INNER_HDR_BYTES + cap * 4;
}

/// Timestamps array starts after values.
fn innerMapTimestampsOffset(inner_offset: u32, cap: u32) u32 {
    return innerMapValuesOffset(inner_offset, cap) + cap * 4;
}

/// Upsert (last-write-wins) into inner hash map.
/// Returns null if capacity exceeded (need inner growth).
fn innerMapUpsertLast(state_base: [*]u8, inner_offset: u32, key: u32, value: u32) ?bool {
    if (key == EMPTY_KEY or key == TOMBSTONE) return false;

    const probe = innerFindInsertPos(state_base, inner_offset, key);
    if (probe.pos >= probe.cap) return null;

    const base = state_base + inner_offset;
    const keys: [*]u32 = @ptrCast(@alignCast(base + INNER_HDR_BYTES));
    const vals: [*]u32 = @ptrCast(@alignCast(state_base + innerMapValuesOffset(inner_offset, probe.cap)));

    if (probe.found) {
        // Update existing
        vals[probe.pos] = value;
        return false;
    }

    // Check load factor
    const size = innerGetSize(state_base, inner_offset);
    const max_size = (probe.cap * 7) / 10;
    if (size >= max_size) return null;

    // Insert new
    keys[probe.pos] = key;
    vals[probe.pos] = value;
    innerSetSize(state_base, inner_offset, size + 1);
    return true;
}

// =============================================================================
// Inner aggregate operations
// =============================================================================

/// Update inner aggregate (add value to accumulator).
fn innerAggUpdate(state_base: [*]u8, inner_offset: u32, agg_type: AggType, value_bits: u64) void {
    const base = state_base + inner_offset;
    switch (agg_type) {
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
            if (count_ptr.* == 0 or new_val < val_ptr.*) {
                val_ptr.* = new_val;
            }
            count_ptr.* += 1;
        },
        .MAX => {
            const val_ptr: *align(1) f64 = @ptrCast(base);
            const count_ptr: *align(1) u64 = @ptrCast(base + 8);
            const new_val: f64 = @bitCast(value_bits);
            if (count_ptr.* == 0 or new_val > val_ptr.*) {
                val_ptr.* = new_val;
            }
            count_ptr.* += 1;
        },
        else => {},
    }
}

// =============================================================================
// Inner container growth
// =============================================================================

/// Grow an inner hash set: allocate 2× from arena, rehash, return new offset.
/// Returns null if arena is full.
fn growInnerHashSet(
    state_base: [*]u8,
    old_offset: u32,
    arena_hdr: u32,
    arena_start: u32,
) ?u32 {
    const old_cap = innerGetCap(state_base, old_offset);
    const new_cap = old_cap * 2;
    const new_size = innerContainerSize(.HASHSET, new_cap, .SUM); // agg_type irrelevant for set
    const new_offset = arenaAlloc(state_base, arena_hdr, arena_start, new_size) orelse return null;

    initInnerHashSet(state_base, new_offset, new_cap);

    // Rehash all entries from old → new
    const old_keys: [*]const u32 = @ptrCast(@alignCast(state_base + old_offset + INNER_HDR_BYTES));
    const new_keys: [*]u32 = @ptrCast(@alignCast(state_base + new_offset + INNER_HDR_BYTES));
    var count: u32 = 0;
    for (0..old_cap) |i| {
        const k = old_keys[i];
        if (k != EMPTY_KEY and k != TOMBSTONE) {
            var pos = hashKey(k, new_cap);
            while (new_keys[pos] != EMPTY_KEY) {
                pos = (pos + 1) & (new_cap - 1);
            }
            new_keys[pos] = k;
            count += 1;
        }
    }
    innerSetSize(state_base, new_offset, count);
    return new_offset;
}

/// Grow an inner hash map: allocate 2× from arena, rehash, return new offset.
fn growInnerHashMap(
    state_base: [*]u8,
    old_offset: u32,
    arena_hdr: u32,
    arena_start: u32,
) ?u32 {
    const old_cap = innerGetCap(state_base, old_offset);
    const new_cap = old_cap * 2;
    const new_size = innerContainerSize(.HASHMAP, new_cap, .SUM);
    const new_offset = arenaAlloc(state_base, arena_hdr, arena_start, new_size) orelse return null;

    initInnerHashMap(state_base, new_offset, new_cap);

    // Rehash all entries
    const old_keys: [*]const u32 = @ptrCast(@alignCast(state_base + old_offset + INNER_HDR_BYTES));
    const old_vals: [*]const u32 = @ptrCast(@alignCast(state_base + innerMapValuesOffset(old_offset, old_cap)));
    const old_ts: [*]const f64 = @ptrCast(@alignCast(state_base + innerMapTimestampsOffset(old_offset, old_cap)));

    const new_keys: [*]u32 = @ptrCast(@alignCast(state_base + new_offset + INNER_HDR_BYTES));
    const new_vals: [*]u32 = @ptrCast(@alignCast(state_base + innerMapValuesOffset(new_offset, new_cap)));
    const new_ts: [*]f64 = @ptrCast(@alignCast(state_base + innerMapTimestampsOffset(new_offset, new_cap)));

    var count: u32 = 0;
    for (0..old_cap) |i| {
        const k = old_keys[i];
        if (k != EMPTY_KEY and k != TOMBSTONE) {
            var pos = hashKey(k, new_cap);
            while (new_keys[pos] != EMPTY_KEY) {
                pos = (pos + 1) & (new_cap - 1);
            }
            new_keys[pos] = k;
            new_vals[pos] = old_vals[i];
            new_ts[pos] = old_ts[i];
            count += 1;
        }
    }
    innerSetSize(state_base, new_offset, count);
    return new_offset;
}

// =============================================================================
// Top-level nested operations (called from dispatch loop)
// =============================================================================

/// Resolve the outer key to its inner container offset, allocating if needed.
/// Returns inner container absolute offset, or null if arena is full.
fn resolveOuter(
    state_base: [*]u8,
    meta: SlotMeta,
    outer_key: u32,
    prefix: NestedPrefix,
) ?struct { inner_offset: u32, is_new: bool } {
    if (outer_key == EMPTY_KEY or outer_key == TOMBSTONE) return null;

    const cap = meta.capacity;
    const slot_offset = meta.offset;
    const keys_off = outerKeysOffset(slot_offset);
    const ptrs_off = outerPtrsOffset(slot_offset, cap);
    const arena_hdr = arenaHeaderOffset(slot_offset, cap);
    const arena_start = arenaDataOffset(slot_offset, cap);

    const keys: [*]u32 = @ptrCast(@alignCast(state_base + keys_off));
    const ptrs: [*]u32 = @ptrCast(@alignCast(state_base + ptrs_off));

    // Probe outer hash table
    var pos = hashKey(outer_key, cap);
    var probes: u32 = 0;
    while (probes < cap) : (probes += 1) {
        const k = keys[pos];
        if (k == outer_key) {
            // Found existing — return inner ptr
            return .{ .inner_offset = ptrs[pos], .is_new = false };
        }
        if (k == EMPTY_KEY or k == TOMBSTONE) {
            // Need to insert new outer key + allocate inner container
            const outer_size = meta.size_ptr.*;
            const max_size = (cap * 7) / 10;
            if (outer_size >= max_size) return null; // outer needs growth

            const inner_offset = allocInnerContainer(
                state_base,
                arena_hdr,
                arena_start,
                prefix.inner_type,
                prefix.inner_initial_cap,
                prefix.inner_agg_type,
            ) orelse return null;

            keys[pos] = outer_key;
            ptrs[pos] = inner_offset;
            meta.size_ptr.* = outer_size + 1;
            return .{ .inner_offset = inner_offset, .is_new = true };
        }
        pos = (pos + 1) & (cap - 1);
    }
    return null; // table full
}

/// Insert element into a nested set: Map<outer_key, Set>.add(elem)
pub fn nestedSetInsert(
    state_base: [*]u8,
    meta: SlotMeta,
    outer_key: u32,
    elem: u32,
) ErrorCode {
    const prefix = readNestedPrefix(state_base, meta.offset);
    const resolved = resolveOuter(state_base, meta, outer_key, prefix) orelse
        return .CAPACITY_EXCEEDED;

    if (resolved.is_new) {
        setChangeFlag(meta, ChangeFlag.INSERTED);
    }

    // Try insert into inner set
    if (innerSetInsert(state_base, resolved.inner_offset, elem)) |was_new| {
        if (was_new) setChangeFlag(meta, ChangeFlag.INSERTED);
        return .OK;
    }

    // Inner set needs growth
    const arena_hdr = arenaHeaderOffset(meta.offset, meta.capacity);
    const arena_start = arenaDataOffset(meta.offset, meta.capacity);

    const new_inner = growInnerHashSet(state_base, resolved.inner_offset, arena_hdr, arena_start) orelse
        return .CAPACITY_EXCEEDED;

    // Update outer pointer
    const ptrs_off = outerPtrsOffset(meta.offset, meta.capacity);
    const ptrs: [*]u32 = @ptrCast(@alignCast(state_base + ptrs_off));
    // Find the outer slot again to update ptr
    const keys_off = outerKeysOffset(meta.offset);
    const keys: [*]const u32 = @ptrCast(@alignCast(state_base + keys_off));
    var pos = hashKey(outer_key, meta.capacity);
    while (keys[pos] != outer_key) {
        pos = (pos + 1) & (meta.capacity - 1);
    }
    ptrs[pos] = new_inner;

    // Retry insert into grown set
    _ = innerSetInsert(state_base, new_inner, elem);
    setChangeFlag(meta, ChangeFlag.INSERTED);
    return .OK;
}

/// Upsert into a nested map: Map<outer_key, Map<inner_key, value>>
pub fn nestedMapUpsertLast(
    state_base: [*]u8,
    meta: SlotMeta,
    outer_key: u32,
    inner_key: u32,
    value: u32,
) ErrorCode {
    const prefix = readNestedPrefix(state_base, meta.offset);
    const resolved = resolveOuter(state_base, meta, outer_key, prefix) orelse
        return .CAPACITY_EXCEEDED;

    if (resolved.is_new) {
        setChangeFlag(meta, ChangeFlag.INSERTED);
    }

    // Try upsert into inner map
    if (innerMapUpsertLast(state_base, resolved.inner_offset, inner_key, value)) |was_new| {
        if (was_new) setChangeFlag(meta, ChangeFlag.INSERTED) else setChangeFlag(meta, ChangeFlag.UPDATED);
        return .OK;
    }

    // Inner map needs growth
    const arena_hdr = arenaHeaderOffset(meta.offset, meta.capacity);
    const arena_start = arenaDataOffset(meta.offset, meta.capacity);

    const new_inner = growInnerHashMap(state_base, resolved.inner_offset, arena_hdr, arena_start) orelse
        return .CAPACITY_EXCEEDED;

    // Update outer pointer
    const ptrs_off = outerPtrsOffset(meta.offset, meta.capacity);
    const ptrs: [*]u32 = @ptrCast(@alignCast(state_base + ptrs_off));
    const keys_off = outerKeysOffset(meta.offset);
    const keys: [*]const u32 = @ptrCast(@alignCast(state_base + keys_off));
    var pos = hashKey(outer_key, meta.capacity);
    while (keys[pos] != outer_key) {
        pos = (pos + 1) & (meta.capacity - 1);
    }
    ptrs[pos] = new_inner;

    // Retry upsert
    _ = innerMapUpsertLast(state_base, new_inner, inner_key, value);
    setChangeFlag(meta, ChangeFlag.INSERTED);
    return .OK;
}

/// Update a nested aggregate: Map<outer_key, Agg>.update(value)
pub fn nestedAggUpdate(
    state_base: [*]u8,
    meta: SlotMeta,
    outer_key: u32,
    value_bits: u64,
) ErrorCode {
    const prefix = readNestedPrefix(state_base, meta.offset);
    const resolved = resolveOuter(state_base, meta, outer_key, prefix) orelse
        return .CAPACITY_EXCEEDED;

    if (resolved.is_new) {
        setChangeFlag(meta, ChangeFlag.INSERTED);
    }

    innerAggUpdate(state_base, resolved.inner_offset, prefix.inner_agg_type, value_bits);
    setChangeFlag(meta, ChangeFlag.SIZE_CHANGED);
    return .OK;
}

// =============================================================================
// Read/iteration exports
// =============================================================================

/// Get the inner container offset for a given outer key. Returns 0 if not found.
pub fn getInnerOffset(state_base: [*]u8, meta: SlotMeta, outer_key: u32) u32 {
    const cap = meta.capacity;
    const keys: [*]const u32 = @ptrCast(@alignCast(state_base + outerKeysOffset(meta.offset)));
    const ptrs: [*]const u32 = @ptrCast(@alignCast(state_base + outerPtrsOffset(meta.offset, cap)));

    var pos = hashKey(outer_key, cap);
    var probes: u32 = 0;
    while (probes < cap) : (probes += 1) {
        const k = keys[pos];
        if (k == outer_key) return ptrs[pos];
        if (k == EMPTY_KEY) return 0;
        pos = (pos + 1) & (cap - 1);
    }
    return 0;
}

/// Get the size of an inner hash set.
pub fn getInnerSetSize(state_base: [*]u8, inner_offset: u32) u32 {
    if (inner_offset == 0) return 0;
    return innerGetSize(state_base, inner_offset);
}

/// Check if an inner set contains an element.
pub fn innerSetContains(state_base: [*]u8, inner_offset: u32, elem: u32) bool {
    if (inner_offset == 0) return false;
    const result = innerFindKey(state_base, inner_offset, elem);
    return result.pos < result.cap;
}

/// Get inner map value for a key. Returns EMPTY_KEY if not found.
pub fn innerMapGet(state_base: [*]u8, inner_offset: u32, key: u32) u32 {
    if (inner_offset == 0) return EMPTY_KEY;
    const result = innerFindKey(state_base, inner_offset, key);
    if (result.pos >= result.cap) return EMPTY_KEY;
    const vals: [*]const u32 = @ptrCast(@alignCast(state_base + innerMapValuesOffset(inner_offset, result.cap)));
    return vals[result.pos];
}

/// Get inner aggregate value (f64).
pub fn innerAggGetF64(state_base: [*]u8, inner_offset: u32) f64 {
    if (inner_offset == 0) return 0;
    const val_ptr: *align(1) const f64 = @ptrCast(state_base + inner_offset);
    return val_ptr.*;
}

/// Get inner aggregate count.
pub fn innerAggGetCount(state_base: [*]u8, inner_offset: u32, agg_type: AggType) u64 {
    if (inner_offset == 0) return 0;
    const count_offset: u32 = if (agg_type == .COUNT) 0 else 8;
    const count_ptr: *align(1) const u64 = @ptrCast(state_base + inner_offset + count_offset);
    return count_ptr.*;
}

// =============================================================================
// Tests
// =============================================================================

const testing = std.testing;

fn makeTestState(comptime size: u32) [size]u8 {
    return [_]u8{0} ** size;
}

test "nested set — basic insert and contains" {
    // Set up a minimal nested slot: Map<K, Set<V>>
    // We manually lay out the state buffer for testing.
    const outer_cap: u32 = 16;
    const inner_initial_cap: u16 = 16;
    const slot_offset: u32 = 64; // after header + metadata

    // Calculate sizes
    const inner_cap = nextPowerOf2(inner_initial_cap);
    const data_size = nestedSlotDataSize(outer_cap, inner_cap, .HASHSET, .SUM);
    const total_size = slot_offset + data_size;
    var state: [4096]u8 = [_]u8{0} ** 4096;
    _ = total_size;

    // Write nested prefix
    writeNestedPrefix(&state, slot_offset, .{
        .inner_type = .HASHSET,
        .inner_initial_cap = inner_initial_cap,
        .inner_agg_type = .SUM, // unused for sets
        .depth = 1,
    });

    // Initialize outer keys to EMPTY_KEY
    const keys: [*]u32 = @ptrCast(@alignCast(&state[outerKeysOffset(slot_offset)]));
    for (0..outer_cap) |i| keys[i] = EMPTY_KEY;

    // Initialize arena header
    const arena_hdr = arenaHeaderOffset(slot_offset, outer_cap);
    const arena_data_start = arenaDataOffset(slot_offset, outer_cap);
    const arena_cap: u32 = 4096 - arena_data_start;
    std.mem.writeInt(u32, state[arena_hdr..][0..4], arena_cap, .little);
    std.mem.writeInt(u32, state[arena_hdr + 4 ..][0..4], 0, .little);

    // Build a fake SlotMeta
    // We need offset, capacity, and size_ptr
    // Store size at a known location (in the metadata area)
    const size_loc: u32 = 32; // arbitrary metadata location for outer size
    std.mem.writeInt(u32, state[size_loc..][0..4], 0, .little);

    const meta = SlotMeta{
        .offset = slot_offset,
        .capacity = outer_cap,
        .size_ptr = @ptrCast(@alignCast(&state[size_loc])),
        .type_flags = .{ .slot_type = .BITMAP, .has_ttl = false, .has_evict_trigger = false }, // slot_type doesn't matter here
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

    // Insert into nested set: Map<100, Set>.add(42)
    const r1 = nestedSetInsert(&state, meta, 100, 42);
    try testing.expectEqual(ErrorCode.OK, r1);

    // Insert another element into same outer key
    const r2 = nestedSetInsert(&state, meta, 100, 43);
    try testing.expectEqual(ErrorCode.OK, r2);

    // Insert into different outer key
    const r3 = nestedSetInsert(&state, meta, 200, 42);
    try testing.expectEqual(ErrorCode.OK, r3);

    // Verify: outer has 2 keys
    try testing.expectEqual(@as(u32, 2), meta.size_ptr.*);

    // Verify: inner set for key 100 has 2 elements
    const inner_100 = getInnerOffset(&state, meta, 100);
    try testing.expect(inner_100 != 0);
    try testing.expectEqual(@as(u32, 2), getInnerSetSize(&state, inner_100));
    try testing.expect(innerSetContains(&state, inner_100, 42));
    try testing.expect(innerSetContains(&state, inner_100, 43));
    try testing.expect(!innerSetContains(&state, inner_100, 44));

    // Verify: inner set for key 200 has 1 element
    const inner_200 = getInnerOffset(&state, meta, 200);
    try testing.expect(inner_200 != 0);
    try testing.expectEqual(@as(u32, 1), getInnerSetSize(&state, inner_200));
    try testing.expect(innerSetContains(&state, inner_200, 42));
}

test "nested set — duplicate insert is idempotent" {
    const outer_cap: u32 = 16;
    const slot_offset: u32 = 64;
    var state: [4096]u8 = [_]u8{0} ** 4096;

    writeNestedPrefix(&state, slot_offset, .{
        .inner_type = .HASHSET,
        .inner_initial_cap = 16,
        .inner_agg_type = .SUM,
        .depth = 1,
    });

    const keys: [*]u32 = @ptrCast(@alignCast(&state[outerKeysOffset(slot_offset)]));
    for (0..outer_cap) |i| keys[i] = EMPTY_KEY;

    const arena_hdr = arenaHeaderOffset(slot_offset, outer_cap);
    const arena_start = arenaDataOffset(slot_offset, outer_cap);
    std.mem.writeInt(u32, state[arena_hdr..][0..4], 4096 - arena_start, .little);

    const size_loc: u32 = 32;
    std.mem.writeInt(u32, state[size_loc..][0..4], 0, .little);

    const meta = SlotMeta{
        .offset = slot_offset,
        .capacity = outer_cap,
        .size_ptr = @ptrCast(@alignCast(&state[size_loc])),
        .type_flags = .{ .slot_type = .BITMAP, .has_ttl = false, .has_evict_trigger = false },
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

    _ = nestedSetInsert(&state, meta, 100, 42);
    _ = nestedSetInsert(&state, meta, 100, 42); // duplicate
    _ = nestedSetInsert(&state, meta, 100, 42); // duplicate

    const inner = getInnerOffset(&state, meta, 100);
    try testing.expectEqual(@as(u32, 1), getInnerSetSize(&state, inner));
}

test "nested map — upsert last-write-wins" {
    const outer_cap: u32 = 16;
    const slot_offset: u32 = 64;
    var state: [4096]u8 = [_]u8{0} ** 4096;

    writeNestedPrefix(&state, slot_offset, .{
        .inner_type = .HASHMAP,
        .inner_initial_cap = 16,
        .inner_agg_type = .SUM,
        .depth = 1,
    });

    const keys: [*]u32 = @ptrCast(@alignCast(&state[outerKeysOffset(slot_offset)]));
    for (0..outer_cap) |i| keys[i] = EMPTY_KEY;

    const arena_hdr = arenaHeaderOffset(slot_offset, outer_cap);
    const arena_start = arenaDataOffset(slot_offset, outer_cap);
    std.mem.writeInt(u32, state[arena_hdr..][0..4], 4096 - arena_start, .little);

    const size_loc: u32 = 32;
    std.mem.writeInt(u32, state[size_loc..][0..4], 0, .little);

    const meta = SlotMeta{
        .offset = slot_offset,
        .capacity = outer_cap,
        .size_ptr = @ptrCast(@alignCast(&state[size_loc])),
        .type_flags = .{ .slot_type = .BITMAP, .has_ttl = false, .has_evict_trigger = false },
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

    // Map<1, Map<10, V>>: key=1, inner_key=10, value=100
    _ = nestedMapUpsertLast(&state, meta, 1, 10, 100);
    _ = nestedMapUpsertLast(&state, meta, 1, 11, 200);
    _ = nestedMapUpsertLast(&state, meta, 1, 10, 300); // overwrite

    const inner_1 = getInnerOffset(&state, meta, 1);
    try testing.expect(inner_1 != 0);
    try testing.expectEqual(@as(u32, 2), innerGetSize(&state, inner_1)); // 2 distinct inner keys
    try testing.expectEqual(@as(u32, 300), innerMapGet(&state, inner_1, 10)); // overwritten
    try testing.expectEqual(@as(u32, 200), innerMapGet(&state, inner_1, 11)); // unchanged
}

test "nested aggregate — count per outer key" {
    const outer_cap: u32 = 16;
    const slot_offset: u32 = 64;
    var state: [4096]u8 = [_]u8{0} ** 4096;

    writeNestedPrefix(&state, slot_offset, .{
        .inner_type = .AGGREGATE,
        .inner_initial_cap = 0, // unused for aggregates
        .inner_agg_type = .COUNT,
        .depth = 1,
    });

    const keys: [*]u32 = @ptrCast(@alignCast(&state[outerKeysOffset(slot_offset)]));
    for (0..outer_cap) |i| keys[i] = EMPTY_KEY;

    const arena_hdr = arenaHeaderOffset(slot_offset, outer_cap);
    const arena_start = arenaDataOffset(slot_offset, outer_cap);
    std.mem.writeInt(u32, state[arena_hdr..][0..4], 4096 - arena_start, .little);

    const size_loc: u32 = 32;
    std.mem.writeInt(u32, state[size_loc..][0..4], 0, .little);

    const meta = SlotMeta{
        .offset = slot_offset,
        .capacity = outer_cap,
        .size_ptr = @ptrCast(@alignCast(&state[size_loc])),
        .type_flags = .{ .slot_type = .BITMAP, .has_ttl = false, .has_evict_trigger = false },
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

    // Count events per outer key
    _ = nestedAggUpdate(&state, meta, 1, 0);
    _ = nestedAggUpdate(&state, meta, 1, 0);
    _ = nestedAggUpdate(&state, meta, 1, 0);
    _ = nestedAggUpdate(&state, meta, 2, 0);

    const inner_1 = getInnerOffset(&state, meta, 1);
    const inner_2 = getInnerOffset(&state, meta, 2);
    try testing.expectEqual(@as(u64, 3), innerAggGetCount(&state, inner_1, .COUNT));
    try testing.expectEqual(@as(u64, 1), innerAggGetCount(&state, inner_2, .COUNT));
}

test "nested set — inner growth on load factor exceeded" {
    const outer_cap: u32 = 16;
    const slot_offset: u32 = 64;
    // Use a larger buffer for growth
    var state: [16384]u8 = [_]u8{0} ** 16384;

    writeNestedPrefix(&state, slot_offset, .{
        .inner_type = .HASHSET,
        .inner_initial_cap = 4, // very small → will need growth quickly
        .inner_agg_type = .SUM,
        .depth = 1,
    });

    const keys: [*]u32 = @ptrCast(@alignCast(&state[outerKeysOffset(slot_offset)]));
    for (0..outer_cap) |i| keys[i] = EMPTY_KEY;

    const arena_hdr = arenaHeaderOffset(slot_offset, outer_cap);
    const arena_start = arenaDataOffset(slot_offset, outer_cap);
    std.mem.writeInt(u32, state[arena_hdr..][0..4], 16384 - arena_start, .little);

    const size_loc: u32 = 32;
    std.mem.writeInt(u32, state[size_loc..][0..4], 0, .little);

    const meta = SlotMeta{
        .offset = slot_offset,
        .capacity = outer_cap,
        .size_ptr = @ptrCast(@alignCast(&state[size_loc])),
        .type_flags = .{ .slot_type = .BITMAP, .has_ttl = false, .has_evict_trigger = false },
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

    // Insert enough elements to trigger inner growth (initial cap=4, grows at 70% → 2 entries)
    // After growth, inner cap becomes 16 (nextPowerOf2(4) = 16, but initial is nextPowerOf2(4)=16...
    // Actually nextPowerOf2(4) = 16. So inner starts at cap=16 which holds 11 entries.
    // Let's insert many distinct elements to force growth.
    var i: u32 = 1;
    while (i <= 20) : (i += 1) {
        const result = nestedSetInsert(&state, meta, 1, i);
        try testing.expectEqual(ErrorCode.OK, result);
    }

    // All 20 elements should be present
    const inner = getInnerOffset(&state, meta, 1);
    try testing.expectEqual(@as(u32, 20), getInnerSetSize(&state, inner));

    i = 1;
    while (i <= 20) : (i += 1) {
        try testing.expect(innerSetContains(&state, inner, i));
    }
}

test "nested aggregate — sum f64 per outer key" {
    const outer_cap: u32 = 16;
    const slot_offset: u32 = 64;
    var state: [4096]u8 = [_]u8{0} ** 4096;

    writeNestedPrefix(&state, slot_offset, .{
        .inner_type = .AGGREGATE,
        .inner_initial_cap = 0,
        .inner_agg_type = .SUM,
        .depth = 1,
    });

    const keys: [*]u32 = @ptrCast(@alignCast(&state[outerKeysOffset(slot_offset)]));
    for (0..outer_cap) |i| keys[i] = EMPTY_KEY;

    const arena_hdr = arenaHeaderOffset(slot_offset, outer_cap);
    const arena_start = arenaDataOffset(slot_offset, outer_cap);
    std.mem.writeInt(u32, state[arena_hdr..][0..4], 4096 - arena_start, .little);

    const size_loc: u32 = 32;
    std.mem.writeInt(u32, state[size_loc..][0..4], 0, .little);

    const meta = SlotMeta{
        .offset = slot_offset,
        .capacity = outer_cap,
        .size_ptr = @ptrCast(@alignCast(&state[size_loc])),
        .type_flags = .{ .slot_type = .BITMAP, .has_ttl = false, .has_evict_trigger = false },
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

    // Sum values per outer key
    const v100: f64 = 100.0;
    const v250: f64 = 250.5;
    const v50: f64 = 50.0;
    _ = nestedAggUpdate(&state, meta, 1, @bitCast(v100));
    _ = nestedAggUpdate(&state, meta, 1, @bitCast(v250));
    _ = nestedAggUpdate(&state, meta, 2, @bitCast(v50));

    const inner_1 = getInnerOffset(&state, meta, 1);
    const inner_2 = getInnerOffset(&state, meta, 2);
    try testing.expectApproxEqAbs(@as(f64, 350.5), innerAggGetF64(&state, inner_1), 0.001);
    try testing.expectEqual(@as(u64, 2), innerAggGetCount(&state, inner_1, .SUM));
    try testing.expectApproxEqAbs(@as(f64, 50.0), innerAggGetF64(&state, inner_2), 0.001);
    try testing.expectEqual(@as(u64, 1), innerAggGetCount(&state, inner_2, .SUM));
}
