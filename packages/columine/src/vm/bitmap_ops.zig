// =============================================================================
// Roaring Bitmap Operations — serialization, set algebra, WASM exports
// =============================================================================
//
// All roaring bitmap storage, load/store, batch add/remove, set algebra,
// and WASM query/utility exports live here. Extracted from vm.zig.

const std = @import("std");
const builtin = @import("builtin");
const rawr = @import("rawr");
const types = @import("types.zig");
const vm = @import("vm.zig");

// Type aliases from types.zig
const SlotMeta = types.SlotMeta;
const ErrorCode = types.ErrorCode;
const ChangeFlag = types.ChangeFlag;
const EMPTY_KEY = types.EMPTY_KEY;
const TOMBSTONE = types.TOMBSTONE;
const BITMAP_SERIALIZED_LEN_BYTES = types.BITMAP_SERIALIZED_LEN_BYTES;
const BITMAP_BYTES_PER_CAPACITY = types.BITMAP_BYTES_PER_CAPACITY;
const BITMAP_BASE_BYTES = types.BITMAP_BASE_BYTES;
const setChangeFlag = types.setChangeFlag;
const getSlotMeta = types.getSlotMeta;
const StateHeaderOffset = types.StateHeaderOffset;
const STATE_HEADER_SIZE = types.STATE_HEADER_SIZE;
const SLOT_META_SIZE = types.SLOT_META_SIZE;
const SlotMetaOffset = types.SlotMetaOffset;

// =============================================================================
// Roaring bitmap globals (moved from vm.zig)
// =============================================================================

pub const RoaringBitmap = rawr.RoaringBitmap;
pub const FrozenBitmap = rawr.FrozenBitmap;
pub const bitmap_allocator = if (builtin.cpu.arch == .wasm32 or builtin.cpu.arch == .wasm64)
    std.heap.wasm_allocator
else
    std.heap.smp_allocator;

var g_bitmap_scratch_ptr: usize = 0;
var g_bitmap_scratch_len: u32 = 0;
var g_bitmap_scratch_fba = std.heap.FixedBufferAllocator.init(&[_]u8{});
pub var g_bitmap_last_error: u32 = 0;
var g_bitmap_store_temp: ?[]u8 = null;
var g_algebra_result_buf: ?[]u8 = null;

fn ensureReusableBuffer(
    allocator: std.mem.Allocator,
    buffer: *?[]u8,
    min_len: usize,
    oom_error_code: u32,
    other_error_code: u32,
) ?[]u8 {
    if (min_len == 0) return &[_]u8{};

    if (buffer.*) |existing| {
        if (existing.len >= min_len) return existing[0..min_len];

        const grown = allocator.realloc(existing, min_len) catch |err| {
            if (err == error.OutOfMemory)
                g_bitmap_last_error = oom_error_code
            else
                g_bitmap_last_error = other_error_code;
            return null;
        };
        buffer.* = grown;
        return grown[0..min_len];
    }

    const allocated = allocator.alloc(u8, min_len) catch |err| {
        if (err == error.OutOfMemory)
            g_bitmap_last_error = oom_error_code
        else
            g_bitmap_last_error = other_error_code;
        return null;
    };
    buffer.* = allocated;
    return allocated[0..min_len];
}

pub inline fn bitmapBackingAllocator() std.mem.Allocator {
    if (g_bitmap_scratch_ptr != 0 and g_bitmap_scratch_len != 0) {
        // FBA is already initialised by vm_set_rbmp_scratch — just return its allocator
        return g_bitmap_scratch_fba.allocator();
    }
    return bitmap_allocator;
}

// =============================================================================
// WASM scratch exports (moved from vm.zig)
// =============================================================================

/// Configure optional bitmap scratch workspace for the next VM call.
pub export fn vm_set_rbmp_scratch(ptr: u32, len: u32) void {
    g_bitmap_scratch_ptr = @as(usize, ptr);
    g_bitmap_scratch_len = len;
    if (ptr != 0 and len != 0) {
        const scratch_ptr: [*]u8 = @ptrFromInt(@as(usize, ptr));
        g_bitmap_scratch_fba = std.heap.FixedBufferAllocator.init(scratch_ptr[0..len]);
    }
}

pub inline fn clearBitmapScratch() void {
    g_bitmap_scratch_ptr = 0;
    g_bitmap_scratch_len = 0;
}

pub export fn vm_get_rbmp_last_error() u32 {
    return g_bitmap_last_error;
}

pub export fn vm_get_rbmp_scratch_len() u32 {
    return g_bitmap_scratch_len;
}

pub export fn vm_get_rbmp_scratch_ptr() u32 {
    return @intCast(g_bitmap_scratch_ptr);
}

// =============================================================================
// Core bitmap types and ops (moved from vm.zig)
// =============================================================================

pub const BitmapStorage = struct {
    serialized_len_ptr: *u32,
    payload_ptr: [*]u8,
    payload_capacity: u32,
};

pub const BitmapSlot = struct {
    meta: SlotMeta,
    storage: BitmapStorage,
};

pub const LoadedBitmap = struct {
    value: RoaringBitmap,

    pub fn deinit(self: *LoadedBitmap) void {
        self.value.deinit();
    }
};

pub inline fn bitmapPayloadCapacity(slot_capacity: u32) u32 {
    return slot_capacity * BITMAP_BYTES_PER_CAPACITY + BITMAP_BASE_BYTES;
}

pub inline fn getBitmapStorage(state_base: [*]u8, meta: SlotMeta) BitmapStorage {
    const data_ptr = state_base + meta.offset;
    const serialized_len_ptr: *u32 = @ptrCast(@alignCast(data_ptr));
    return .{
        .serialized_len_ptr = serialized_len_ptr,
        .payload_ptr = data_ptr + BITMAP_SERIALIZED_LEN_BYTES,
        .payload_capacity = bitmapPayloadCapacity(meta.capacity),
    };
}

pub inline fn getBitmapSlotByOffset(state_base: [*]u8, slot_offset: u32) ?BitmapSlot {
    const meta = vm.findSlotMetaByOffset(state_base, slot_offset) orelse return null;
    if (meta.slotType() != .BITMAP) return null;
    return .{ .meta = meta, .storage = getBitmapStorage(state_base, meta) };
}

pub inline fn bitmapFrozen(storage: BitmapStorage) ?FrozenBitmap {
    const serialized_len = storage.serialized_len_ptr.*;
    if (serialized_len == 0 or serialized_len > storage.payload_capacity) {
        return null;
    }
    return FrozenBitmap.init(storage.payload_ptr[0..serialized_len]) catch return null;
}

pub fn bitmapLoad(storage: BitmapStorage) ?LoadedBitmap {
    const alloc = bitmapBackingAllocator();
    const scratch_active = g_bitmap_scratch_ptr != 0 and g_bitmap_scratch_len != 0;

    const serialized_len = storage.serialized_len_ptr.*;
    if (serialized_len == 0) {
        const empty = RoaringBitmap.init(alloc) catch |err| blk: {
            if (err == error.OutOfMemory and scratch_active) {
                break :blk RoaringBitmap.init(bitmap_allocator) catch |fallback_err| {
                    if (fallback_err == error.OutOfMemory) g_bitmap_last_error = 100 else g_bitmap_last_error = 109;
                    return null;
                };
            }
            if (err == error.OutOfMemory) g_bitmap_last_error = 100 else g_bitmap_last_error = 109;
            return null;
        };
        return .{ .value = empty };
    }
    if (serialized_len > storage.payload_capacity) {
        return null;
    }

    const data = storage.payload_ptr[0..serialized_len];
    const restored = RoaringBitmap.deserialize(alloc, data) catch |err| blk: {
        if (err == error.OutOfMemory and scratch_active) {
            break :blk RoaringBitmap.deserialize(bitmap_allocator, data) catch |fallback_err| {
                if (fallback_err == error.OutOfMemory)
                    g_bitmap_last_error = 101
                else if (fallback_err == error.InvalidFormat)
                    g_bitmap_last_error = 102
                else
                    g_bitmap_last_error = 103;
                return null;
            };
        }

        if (err == error.OutOfMemory)
            g_bitmap_last_error = 101
        else if (err == error.InvalidFormat)
            g_bitmap_last_error = 102
        else
            g_bitmap_last_error = 103;
        return null;
    };
    return .{ .value = restored };
}

pub fn bitmapStore(storage: BitmapStorage, bitmap: *RoaringBitmap) ErrorCode {
    // Optimize container encoding (array -> run where beneficial) before serialization
    _ = bitmap.runOptimize() catch {};

    const serialized_size_needed = bitmap.serializedSizeInBytes();
    if (serialized_size_needed > storage.payload_capacity) {
        g_bitmap_last_error = 60;
        return .CAPACITY_EXCEEDED;
    }

    // Two-phase commit: serialize to temp buffer first, then copy into slot storage.
    // serializeIntoBuffer may partially write on failure; this keeps slot bytes unmodified
    // when we need to return CAPACITY_EXCEEDED/INVALID_STATE and retry after growth.
    const scratch_active = g_bitmap_scratch_ptr != 0 and g_bitmap_scratch_len != 0;
    const temp = if (scratch_active)
        bitmapBackingAllocator().alloc(u8, serialized_size_needed) catch |err| {
            if (err == error.OutOfMemory) {
                g_bitmap_last_error = 60;
                return .CAPACITY_EXCEEDED;
            }
            g_bitmap_last_error = 61;
            return .INVALID_STATE;
        }
    else
        ensureReusableBuffer(bitmap_allocator, &g_bitmap_store_temp, serialized_size_needed, 60, 61) orelse {
            return if (g_bitmap_last_error == 61) .INVALID_STATE else .CAPACITY_EXCEEDED;
        };

    const serialized_size_usize = bitmap.serializeIntoBuffer(temp) catch |err| {
        if (err == error.NoSpaceLeft or err == error.OutOfMemory) {
            // OOM in scratch FBA during serialization temp-buffer alloc -> treat as
            // capacity exceeded so the JS growth loop doubles the slot (and scratch).
            g_bitmap_last_error = 60;
            return .CAPACITY_EXCEEDED;
        }
        g_bitmap_last_error = 61;
        return .INVALID_STATE;
    };

    const serialized_size: u32 = @intCast(serialized_size_usize);
    if (serialized_size > storage.payload_capacity) {
        return .CAPACITY_EXCEEDED;
    }

    storage.serialized_len_ptr.* = serialized_size;
    @memcpy(storage.payload_ptr[0..serialized_size], temp[0..serialized_size]);
    if (serialized_size < storage.payload_capacity) {
        @memset(storage.payload_ptr[serialized_size..storage.payload_capacity], 0);
    }
    return .OK;
}

pub fn bitmapSelect(storage: BitmapStorage, rank: u32) ?u32 {
    const frozen = bitmapFrozen(storage) orelse return null;
    var iterator = frozen.iterator();
    var idx: u32 = 0;
    while (iterator.next()) |element| {
        if (idx == rank) return element;
        idx += 1;
    }
    return null;
}

pub fn batchBitmapAdd(
    comptime delta_mode: bool,
    state_base: [*]u8,
    meta: SlotMeta,
    slot_idx: u8,
    elem_col: [*]const u32,
    ts_col: ?[*]const f64,
    batch_len: u32,
) ErrorCode {
    g_bitmap_last_error = 0;
    const storage = getBitmapStorage(state_base, meta);
    var bitmap = bitmapLoad(storage) orelse {
        // OOM during deserialize (error 100/101) -> scratch FBA exhausted, trigger growth
        if (g_bitmap_last_error == 100 or g_bitmap_last_error == 101) return .CAPACITY_EXCEEDED;
        if (g_bitmap_last_error == 0) g_bitmap_last_error = 1;
        return .INVALID_STATE;
    };
    defer bitmap.deinit();

    const original_size = meta.size_ptr.*;
    var cardinality: u32 = @intCast(bitmap.value.cardinality());
    if (cardinality != original_size) {
        meta.size_ptr.* = cardinality;
    }
    var had_insert = false;

    var i: u32 = 0;
    while (i < batch_len) : (i += 1) {
        const elem = elem_col[i];
        if (elem == EMPTY_KEY or elem == TOMBSTONE) continue;
        const ts = if (meta.hasTTL()) ts_col.?[i] else 0;

        const already_present = bitmap.value.contains(elem);
        if (!already_present and cardinality >= meta.capacity) {
            if (had_insert) {
                const flush_result = bitmapStore(storage, &bitmap.value);
                if (flush_result != .OK) {
                    g_bitmap_last_error = 2;
                    meta.size_ptr.* = original_size;
                    return flush_result;
                }
                meta.size_ptr.* = cardinality;
                setChangeFlag(meta, ChangeFlag.INSERTED);
            } else {
                meta.size_ptr.* = original_size;
            }
            return .CAPACITY_EXCEEDED;
        }

        const inserted = if (already_present)
            false
        else
            bitmap.value.add(elem) catch {
                // OOM in scratch FBA during container growth -> flush and trigger growth
                g_bitmap_last_error = 3;
                if (had_insert) {
                    const flush_result = bitmapStore(storage, &bitmap.value);
                    if (flush_result != .OK) {
                        g_bitmap_last_error = 4;
                        meta.size_ptr.* = original_size;
                        return flush_result;
                    }
                    setChangeFlag(meta, ChangeFlag.INSERTED);
                }
                meta.size_ptr.* = cardinality;
                return .CAPACITY_EXCEEDED;
            };

        if (!inserted) {
            if (meta.hasTTL()) {
                const ttl_result = vm.insertWithTTL(state_base, meta, elem, ts);
                if (ttl_result != .OK) {
                    if (had_insert) {
                        const flush_result = bitmapStore(storage, &bitmap.value);
                        if (flush_result != .OK) {
                            meta.size_ptr.* = original_size;
                            return flush_result;
                        }
                        setChangeFlag(meta, ChangeFlag.INSERTED);
                    }
                    meta.size_ptr.* = cardinality;
                    return ttl_result;
                }
            }
            continue;
        }

        if (vm.g_undo_enabled) {
            meta.size_ptr.* = cardinality;
            vm.appendMutation(
                delta_mode,
                .{
                    .op = .SET_INSERT,
                    .slot = slot_idx,
                    ._pad1 = 0,
                    ._pad2 = 0,
                    .key = elem,
                    .prev_value = 0,
                    .aux = 0,
                },
                .{
                    .op = .SET_DELETE,
                    .slot = slot_idx,
                    ._pad1 = 0,
                    ._pad2 = 0,
                    .key = elem,
                    .prev_value = 0,
                    .aux = 0,
                },
            );
        }

        cardinality += 1;
        had_insert = true;

        if (meta.hasTTL()) {
            const ttl_result = vm.insertWithTTL(state_base, meta, elem, ts);
            if (ttl_result != .OK) {
                const flush_result = bitmapStore(storage, &bitmap.value);
                if (flush_result != .OK) {
                    g_bitmap_last_error = 5;
                    meta.size_ptr.* = original_size;
                    return flush_result;
                }
                meta.size_ptr.* = cardinality;
                if (had_insert) setChangeFlag(meta, ChangeFlag.INSERTED);
                return ttl_result;
            }
        }
    }

    const store_result = bitmapStore(storage, &bitmap.value);
    if (store_result != .OK) {
        if (g_bitmap_last_error == 0) g_bitmap_last_error = 6;
        meta.size_ptr.* = original_size;
        return store_result;
    }

    meta.size_ptr.* = cardinality;
    if (had_insert) setChangeFlag(meta, ChangeFlag.INSERTED);
    return .OK;
}

pub fn batchBitmapRemove(
    comptime delta_mode: bool,
    state_base: [*]u8,
    meta: SlotMeta,
    slot_idx: u8,
    elem_col: [*]const u32,
    batch_len: u32,
) void {
    const storage = getBitmapStorage(state_base, meta);
    var bitmap = bitmapLoad(storage) orelse return;
    defer bitmap.deinit();

    var cardinality: u32 = @intCast(bitmap.value.cardinality());
    var had_remove = false;

    var i: u32 = 0;
    while (i < batch_len) : (i += 1) {
        const elem = elem_col[i];
        if (cardinality == 0) break;

        const removed = bitmap.value.remove(elem) catch false;
        if (!removed) {
            continue;
        }

        if (vm.g_undo_enabled) {
            var prev_ts_bits: u64 = 0;
            if (meta.hasTTL()) {
                const eviction_index = vm.getEvictionIndex(state_base, meta);
                const eviction_size = meta.eviction_index_size_ptr.*;
                if (vm.findLatestEvictionTimestampForKey(eviction_index, eviction_size, elem)) |prev_ts| {
                    prev_ts_bits = @bitCast(prev_ts);
                }
            }
            meta.size_ptr.* = cardinality;
            vm.appendMutation(
                delta_mode,
                .{
                    .op = .SET_DELETE,
                    .slot = slot_idx,
                    ._pad1 = 0,
                    ._pad2 = 0,
                    .key = elem,
                    .prev_value = 0,
                    .aux = prev_ts_bits,
                },
                .{
                    .op = .SET_INSERT,
                    .slot = slot_idx,
                    ._pad1 = 0,
                    ._pad2 = 0,
                    .key = elem,
                    .prev_value = 0,
                    .aux = 0,
                },
            );
        }

        cardinality -= 1;
        had_remove = true;
        if (meta.hasTTL()) {
            vm.removeTTLEntriesForKey(state_base, meta, elem);
        }
    }

    if (!had_remove) {
        meta.size_ptr.* = cardinality;
        return;
    }

    if (bitmapStore(storage, &bitmap.value) != .OK) {
        return;
    }

    meta.size_ptr.* = cardinality;
    if (had_remove) setChangeFlag(meta, ChangeFlag.REMOVED);
}

// =============================================================================
// In-place bitmap set algebra
// =============================================================================

pub const BitmapAlgebraOp = enum { AND, OR, AND_NOT, XOR };

/// Force an undo snapshot so bulk bitmap mutations are covered by shadow rollback.
/// Same mechanism as natural overflow — captures full state at this point.
fn forceUndoSnapshot() void {
    if (comptime builtin.cpu.arch == .wasm32) {
        if (vm.g_undo_state_size <= vm.UNDO_SHADOW_CAPACITY) {
            @memcpy(vm.g_undo_shadow_static[0..vm.g_undo_state_size], vm.g_undo_state_base[0..vm.g_undo_state_size]);
            vm.g_undo_shadow_active = true;
        }
    } else {
        const shadow = vm.native_shadow_allocator.alloc(u8, vm.g_undo_state_size) catch null;
        if (shadow) |s| {
            @memcpy(s, vm.g_undo_state_base[0..vm.g_undo_state_size]);
            vm.g_undo_shadow_dynamic = shadow;
            vm.g_undo_shadow_active = true;
        }
    }
    vm.g_undo_overflow = true;
}

/// In-place set algebra: mutate target slot's bitmap with source bitmap data.
/// Uses rawr's in-place operations to avoid allocating a new result bitmap.
/// Undo uses snapshot-based rollback (not per-element tracking).
pub fn batchBitmapAlgebra(
    comptime op: BitmapAlgebraOp,
    state_base: [*]u8,
    target_meta: SlotMeta,
    source_data: []const u8,
) ErrorCode {
    const target_storage = getBitmapStorage(state_base, target_meta);
    const original_size = target_meta.size_ptr.*;

    // Empty-source identities
    if (source_data.len == 0) {
        switch (op) {
            .AND => {
                // AND with empty = clear target
                if (vm.g_undo_enabled and !vm.g_undo_overflow) forceUndoSnapshot();
                target_storage.serialized_len_ptr.* = 0;
                @memset(target_storage.payload_ptr[0..target_storage.payload_capacity], 0);
                target_meta.size_ptr.* = 0;
                if (original_size != 0) setChangeFlag(target_meta, ChangeFlag.SIZE_CHANGED);
                return .OK;
            },
            // OR/ANDNOT/XOR with empty = no change
            .OR, .AND_NOT, .XOR => return .OK,
        }
    }

    // Force undo snapshot before bulk mutation (individual element tracking impractical)
    if (vm.g_undo_enabled and !vm.g_undo_overflow) forceUndoSnapshot();

    var target = bitmapLoad(target_storage) orelse return .INVALID_STATE;
    defer target.deinit();

    // Deserialize source into arena (freed on return)
    var arena = std.heap.ArenaAllocator.init(bitmap_allocator);
    defer arena.deinit();
    var source = RoaringBitmap.deserialize(arena.allocator(), source_data) catch {
        g_bitmap_last_error = 80;
        return .INVALID_STATE;
    };

    // In-place operation
    switch (op) {
        .AND => target.value.bitwiseAndInPlace(&source) catch {
            g_bitmap_last_error = 81;
            target_meta.size_ptr.* = original_size;
            return .INVALID_STATE;
        },
        .OR => target.value.bitwiseOrInPlace(&source) catch {
            g_bitmap_last_error = 82;
            target_meta.size_ptr.* = original_size;
            return .INVALID_STATE;
        },
        .AND_NOT => target.value.bitwiseDifferenceInPlace(&source) catch {
            g_bitmap_last_error = 83;
            target_meta.size_ptr.* = original_size;
            return .INVALID_STATE;
        },
        .XOR => target.value.bitwiseXorInPlace(&source) catch {
            g_bitmap_last_error = 84;
            target_meta.size_ptr.* = original_size;
            return .INVALID_STATE;
        },
    }

    // Store back
    const store_result = bitmapStore(target_storage, &target.value);
    if (store_result != .OK) {
        target_meta.size_ptr.* = original_size;
        return store_result;
    }

    const new_card: u32 = @intCast(target.value.cardinality());
    target_meta.size_ptr.* = new_card;
    if (new_card != original_size) setChangeFlag(target_meta, ChangeFlag.SIZE_CHANGED);

    return .OK;
}

// =============================================================================
// WASM query/utility exports (moved from vm.zig)
// =============================================================================

pub export fn vm_rbmp_export_len(state_base: [*]u8, slot_offset: u32) u32 {
    const slot = getBitmapSlotByOffset(state_base, slot_offset) orelse return 0;
    const serialized_len = slot.storage.serialized_len_ptr.*;
    if (serialized_len > slot.storage.payload_capacity) return 0;
    return serialized_len;
}

pub export fn vm_rbmp_export_copy(
    state_base: [*]u8,
    slot_offset: u32,
    out_ptr: [*]u8,
    out_capacity: u32,
) u32 {
    const slot = getBitmapSlotByOffset(state_base, slot_offset) orelse return @intFromEnum(ErrorCode.INVALID_SLOT);
    const serialized_len = slot.storage.serialized_len_ptr.*;
    if (serialized_len > slot.storage.payload_capacity) return @intFromEnum(ErrorCode.INVALID_STATE);
    if (serialized_len > out_capacity) return @intFromEnum(ErrorCode.CAPACITY_EXCEEDED);
    if (serialized_len > 0) {
        @memcpy(out_ptr[0..serialized_len], slot.storage.payload_ptr[0..serialized_len]);
    }
    return @intFromEnum(ErrorCode.OK);
}

pub export fn vm_rbmp_import_copy(
    state_base: [*]u8,
    slot_offset: u32,
    in_ptr: [*]const u8,
    in_len: u32,
) u32 {
    const slot = getBitmapSlotByOffset(state_base, slot_offset) orelse return @intFromEnum(ErrorCode.INVALID_SLOT);
    if (in_len > slot.storage.payload_capacity) {
        return @intFromEnum(ErrorCode.CAPACITY_EXCEEDED);
    }

    if (in_len == 0) {
        slot.storage.serialized_len_ptr.* = 0;
        @memset(slot.storage.payload_ptr[0..slot.storage.payload_capacity], 0);
        slot.meta.size_ptr.* = 0;
        return @intFromEnum(ErrorCode.OK);
    }

    const frozen = FrozenBitmap.init(in_ptr[0..in_len]) catch return @intFromEnum(ErrorCode.INVALID_STATE);
    const card = frozen.cardinality();
    if (card > slot.meta.capacity) {
        return @intFromEnum(ErrorCode.CAPACITY_EXCEEDED);
    }

    @memcpy(slot.storage.payload_ptr[0..in_len], in_ptr[0..in_len]);
    slot.storage.serialized_len_ptr.* = in_len;
    if (in_len < slot.storage.payload_capacity) {
        @memset(slot.storage.payload_ptr[in_len..slot.storage.payload_capacity], 0);
    }
    slot.meta.size_ptr.* = @intCast(card);
    return @intFromEnum(ErrorCode.OK);
}

/// Zero-allocation intersection cardinality using FrozenBitmap's container-level ops.
fn deserializedIntersectCount(left_data: []const u8, right_data: []const u8) u32 {
    const left_frozen = FrozenBitmap.init(left_data) catch return 0;
    const right_frozen = FrozenBitmap.init(right_data) catch return 0;
    const card = left_frozen.andCardinality(&right_frozen);
    return if (card > std.math.maxInt(u32)) std.math.maxInt(u32) else @intCast(card);
}

/// Zero-allocation intersection check using FrozenBitmap's container-level ops.
fn deserializedIntersects(left_data: []const u8, right_data: []const u8) bool {
    const left_frozen = FrozenBitmap.init(left_data) catch return false;
    const right_frozen = FrozenBitmap.init(right_data) catch return false;
    return left_frozen.intersects(&right_frozen);
}

/// Get serialized byte slice from a bitmap slot (returns null if empty/invalid).
pub inline fn slotSerializedData(storage: BitmapStorage) ?[]const u8 {
    const serialized_len = storage.serialized_len_ptr.*;
    if (serialized_len == 0 or serialized_len > storage.payload_capacity) return null;
    return storage.payload_ptr[0..serialized_len];
}

pub export fn vm_rbmp_intersect_any_slots(state_base: [*]u8, left_slot_offset: u32, right_slot_offset: u32) u32 {
    const left = getBitmapSlotByOffset(state_base, left_slot_offset) orelse return 0;
    const right = getBitmapSlotByOffset(state_base, right_slot_offset) orelse return 0;
    const left_data = slotSerializedData(left.storage) orelse return 0;
    const right_data = slotSerializedData(right.storage) orelse return 0;
    return if (deserializedIntersects(left_data, right_data)) 1 else 0;
}

pub export fn vm_rbmp_intersect_count_slots(state_base: [*]u8, left_slot_offset: u32, right_slot_offset: u32) u32 {
    const left = getBitmapSlotByOffset(state_base, left_slot_offset) orelse return 0;
    const right = getBitmapSlotByOffset(state_base, right_slot_offset) orelse return 0;
    const left_data = slotSerializedData(left.storage) orelse return 0;
    const right_data = slotSerializedData(right.storage) orelse return 0;
    return deserializedIntersectCount(left_data, right_data);
}

pub export fn vm_rbmp_intersect_any_serialized(
    left_ptr: [*]const u8,
    left_len: u32,
    right_ptr: [*]const u8,
    right_len: u32,
) u32 {
    if (left_len == 0 or right_len == 0) return 0;
    return if (deserializedIntersects(left_ptr[0..left_len], right_ptr[0..right_len])) 1 else 0;
}

pub export fn vm_rbmp_intersect_count_serialized(
    left_ptr: [*]const u8,
    left_len: u32,
    right_ptr: [*]const u8,
    right_len: u32,
) u32 {
    if (left_len == 0 or right_len == 0) return 0;
    return deserializedIntersectCount(left_ptr[0..left_len], right_ptr[0..right_len]);
}

// =============================================================================
// Set Algebra Exports (decision-function-side)
// =============================================================================

/// Global result location for set algebra operations.
/// Backed by dedicated VM-owned storage so later scratch allocations cannot clobber it.
pub var g_algebra_result_ptr: u32 = 0;
pub var g_algebra_result_len: u32 = 0;

pub export fn vm_rbmp_algebra_result_ptr() u32 {
    return g_algebra_result_ptr;
}

pub export fn vm_rbmp_algebra_result_len() u32 {
    return g_algebra_result_len;
}

/// Get a bitmap slot's serialized data pointer (no copy).
pub export fn vm_rbmp_slot_data_ptr(state_base: [*]u8, slot_offset: u32) u32 {
    const slot = getBitmapSlotByOffset(state_base, slot_offset) orelse return 0;
    return @intCast(@intFromPtr(slot.storage.payload_ptr));
}

/// Get a bitmap slot's serialized data length (no copy).
pub export fn vm_rbmp_slot_data_len(state_base: [*]u8, slot_offset: u32) u32 {
    const slot = getBitmapSlotByOffset(state_base, slot_offset) orelse return 0;
    const serialized_len = slot.storage.serialized_len_ptr.*;
    if (serialized_len > slot.storage.payload_capacity) return 0;
    return serialized_len;
}

fn setAlgebraResult(src: []const u8, oom_error_code: u32) u32 {
    if (src.len == 0) {
        g_algebra_result_ptr = 0;
        g_algebra_result_len = 0;
        return @intFromEnum(ErrorCode.OK);
    }

    const out = ensureReusableBuffer(bitmap_allocator, &g_algebra_result_buf, src.len, oom_error_code, oom_error_code) orelse {
        return @intFromEnum(ErrorCode.CAPACITY_EXCEEDED);
    };
    @memcpy(out, src);
    g_algebra_result_ptr = @intCast(@intFromPtr(out.ptr));
    g_algebra_result_len = @intCast(src.len);
    return @intFromEnum(ErrorCode.OK);
}

const AlgebraOp = enum { AND, OR, AND_NOT, XOR };

/// Core set algebra: deserialize into arena, compute op, serialize result into VM-owned buffer.
fn rbmpSetAlgebra(
    left_ptr: [*]const u8,
    left_len: u32,
    right_ptr: [*]const u8,
    right_len: u32,
    comptime op: AlgebraOp,
) u32 {
    g_algebra_result_ptr = 0;
    g_algebra_result_len = 0;

    // Empty-set identities — copy survivor directly into VM-owned algebra storage
    if (left_len == 0 and right_len == 0) return @intFromEnum(ErrorCode.OK);
    if (left_len == 0) return switch (op) {
        .AND, .AND_NOT => @intFromEnum(ErrorCode.OK), // empty result
        .OR, .XOR => setAlgebraResult(right_ptr[0..right_len], 70),
    };
    if (right_len == 0) return switch (op) {
        .AND => @intFromEnum(ErrorCode.OK), // empty result
        .OR, .AND_NOT, .XOR => setAlgebraResult(left_ptr[0..left_len], 70),
    };

    // Arena for deserialization temporaries — bulk freed on return
    var arena = std.heap.ArenaAllocator.init(bitmap_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var left = RoaringBitmap.deserialize(alloc, left_ptr[0..left_len]) catch {
        g_bitmap_last_error = 71;
        return @intFromEnum(ErrorCode.INVALID_STATE);
    };
    var right = RoaringBitmap.deserialize(alloc, right_ptr[0..right_len]) catch {
        g_bitmap_last_error = 72;
        return @intFromEnum(ErrorCode.INVALID_STATE);
    };
    var result = switch (op) {
        .AND => left.bitwiseAnd(alloc, &right),
        .OR => left.bitwiseOr(alloc, &right),
        .AND_NOT => left.bitwiseDifference(alloc, &right),
        .XOR => left.bitwiseXor(alloc, &right),
    } catch {
        g_bitmap_last_error = 73;
        return @intFromEnum(ErrorCode.INVALID_STATE);
    };

    _ = result.runOptimize() catch {};

    // Serialize result into VM-owned buffer.
    const size = result.serializedSizeInBytes();
    const out = ensureReusableBuffer(bitmap_allocator, &g_algebra_result_buf, size, 74, 74) orelse {
        return @intFromEnum(ErrorCode.CAPACITY_EXCEEDED);
    };
    _ = result.serializeIntoBuffer(out) catch {
        g_bitmap_last_error = 75;
        return @intFromEnum(ErrorCode.INVALID_STATE);
    };

    g_algebra_result_ptr = @intCast(@intFromPtr(out.ptr));
    g_algebra_result_len = @intCast(size);
    return @intFromEnum(ErrorCode.OK);
}

pub export fn vm_rbmp_and(lp: [*]const u8, ll: u32, rp: [*]const u8, rl: u32) u32 {
    return rbmpSetAlgebra(lp, ll, rp, rl, .AND);
}

pub export fn vm_rbmp_or(lp: [*]const u8, ll: u32, rp: [*]const u8, rl: u32) u32 {
    return rbmpSetAlgebra(lp, ll, rp, rl, .OR);
}

pub export fn vm_rbmp_andnot(lp: [*]const u8, ll: u32, rp: [*]const u8, rl: u32) u32 {
    return rbmpSetAlgebra(lp, ll, rp, rl, .AND_NOT);
}

pub export fn vm_rbmp_xor(lp: [*]const u8, ll: u32, rp: [*]const u8, rl: u32) u32 {
    return rbmpSetAlgebra(lp, ll, rp, rl, .XOR);
}

// =============================================================================
// Query Exports for Scratch-Resident or Slot-Resident Serialized Data
// =============================================================================

/// Check if a serialized bitmap (in scratch or slot storage) contains a value.
pub export fn vm_rbmp_contains_serialized(ptr: [*]const u8, len: u32, value: u32) u32 {
    if (len == 0) return 0;
    const frozen = FrozenBitmap.init(ptr[0..len]) catch return 0;
    return if (frozen.contains(value)) 1 else 0;
}

/// Get cardinality of a serialized bitmap (in scratch or slot storage).
pub export fn vm_rbmp_cardinality_serialized(ptr: [*]const u8, len: u32) u32 {
    if (len == 0) return 0;
    const frozen = FrozenBitmap.init(ptr[0..len]) catch return 0;
    const card = frozen.cardinality();
    return if (card > std.math.maxInt(u32)) std.math.maxInt(u32) else @intCast(card);
}

/// Extract all values from a serialized bitmap into an output buffer.
/// Returns the number of values written (capped at out_capacity).
pub export fn vm_rbmp_extract_serialized(
    data_ptr: [*]const u8,
    data_len: u32,
    out_ptr: [*]u32,
    out_capacity: u32,
) u32 {
    if (data_len == 0) return 0;
    const frozen = FrozenBitmap.init(data_ptr[0..data_len]) catch return 0;
    var iter = frozen.iterator();
    var count: u32 = 0;
    while (iter.next()) |v| {
        if (count >= out_capacity) break;
        out_ptr[count] = v;
        count += 1;
    }
    return count;
}
