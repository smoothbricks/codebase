// =============================================================================
// SIMD Aggregate Operations — pure reduction functions
// =============================================================================
//
// All functions in this module are pure: they take typed column pointers and
// return a single reduced value. No state mutation, no undo log.
//
// SIMD paths:
//   f64: V4f64 (4-wide) for sum/min/max — unmasked and type-masked
//   i64: V2i64 (2-wide) for sum — unmasked and type-masked
//   u32: V4u32 for masked count
// Scalar fallback for predicated variants and i64 min/max.

const std = @import("std");

const V4f64 = @Vector(4, f64);
const V4u32 = @Vector(4, u32);
const V2i64 = @Vector(2, i64);

pub const AggKind = enum { sum, min, max };

pub const TypeMask = struct { data: [*]const u32, id: u32 };

// =============================================================================
// Unmasked f64 SIMD reductions
// =============================================================================

pub fn batchAggSum(val_col: [*]const f64, batch_len: u32) f64 {
    var sum_vec: V4f64 = @splat(0.0);
    var i: u32 = 0;
    while (i + 4 <= batch_len) : (i += 4) {
        const v: V4f64 = .{ val_col[i], val_col[i + 1], val_col[i + 2], val_col[i + 3] };
        sum_vec += v;
    }
    var result = @reduce(.Add, sum_vec);
    while (i < batch_len) : (i += 1) result += val_col[i];
    return result;
}

pub fn batchAggMin(val_col: [*]const f64, batch_len: u32, current_min: f64) f64 {
    if (batch_len == 0) return current_min;
    var min_vec: V4f64 = @splat(current_min);
    var i: u32 = 0;
    while (i + 4 <= batch_len) : (i += 4) {
        const v: V4f64 = .{ val_col[i], val_col[i + 1], val_col[i + 2], val_col[i + 3] };
        min_vec = @min(min_vec, v);
    }
    var result = @reduce(.Min, min_vec);
    while (i < batch_len) : (i += 1) result = @min(result, val_col[i]);
    return result;
}

pub fn batchAggMax(val_col: [*]const f64, batch_len: u32, current_max: f64) f64 {
    if (batch_len == 0) return current_max;
    var max_vec: V4f64 = @splat(current_max);
    var i: u32 = 0;
    while (i + 4 <= batch_len) : (i += 4) {
        const v: V4f64 = .{ val_col[i], val_col[i + 1], val_col[i + 2], val_col[i + 3] };
        max_vec = @max(max_vec, v);
    }
    var result = @reduce(.Max, max_vec);
    while (i < batch_len) : (i += 1) result = @max(result, val_col[i]);
    return result;
}

// =============================================================================
// Type-masked SIMD reductions (FOR_EACH: only rows matching type_id)
// =============================================================================

pub fn maskedAggSum(val_col: [*]const f64, type_data: [*]const u32, type_id: u32, batch_len: u32) f64 {
    if (batch_len == 0) return 0;
    var sum_vec: V4f64 = @splat(0.0);
    const type_id_vec: V4u32 = @splat(type_id);
    const zero: V4f64 = @splat(@as(f64, 0.0));
    var i: u32 = 0;
    while (i + 4 <= batch_len) : (i += 4) {
        const type_vec: V4u32 = .{ type_data[i], type_data[i + 1], type_data[i + 2], type_data[i + 3] };
        const mask = type_vec == type_id_vec;
        const val_vec: V4f64 = .{ val_col[i], val_col[i + 1], val_col[i + 2], val_col[i + 3] };
        sum_vec += @select(f64, mask, val_vec, zero);
    }
    var result = @reduce(.Add, sum_vec);
    while (i < batch_len) : (i += 1) {
        if (type_data[i] == type_id) result += val_col[i];
    }
    return result;
}

pub fn maskedAggCount(type_data: [*]const u32, type_id: u32, batch_len: u32) u32 {
    var count: u32 = 0;
    const type_id_vec: V4u32 = @splat(type_id);
    var i: u32 = 0;
    while (i + 4 <= batch_len) : (i += 4) {
        const type_vec: V4u32 = .{ type_data[i], type_data[i + 1], type_data[i + 2], type_data[i + 3] };
        const mask: @Vector(4, bool) = type_vec == type_id_vec;
        const ones: V4u32 = @select(u32, mask, @as(V4u32, @splat(@as(u32, 1))), @as(V4u32, @splat(@as(u32, 0))));
        count += @reduce(.Add, ones);
    }
    while (i < batch_len) : (i += 1) {
        if (type_data[i] == type_id) count += 1;
    }
    return count;
}

pub fn maskedAggMin(val_col: [*]const f64, type_data: [*]const u32, type_id: u32, batch_len: u32, current_min: f64) f64 {
    var min_vec: V4f64 = @splat(current_min);
    const type_id_vec: V4u32 = @splat(type_id);
    const identity: V4f64 = @splat(std.math.inf(f64));
    var i: u32 = 0;
    while (i + 4 <= batch_len) : (i += 4) {
        const type_vec: V4u32 = .{ type_data[i], type_data[i + 1], type_data[i + 2], type_data[i + 3] };
        const mask = type_vec == type_id_vec;
        const val_vec: V4f64 = .{ val_col[i], val_col[i + 1], val_col[i + 2], val_col[i + 3] };
        min_vec = @min(min_vec, @select(f64, mask, val_vec, identity));
    }
    var result = @reduce(.Min, min_vec);
    while (i < batch_len) : (i += 1) {
        if (type_data[i] == type_id) result = @min(result, val_col[i]);
    }
    return result;
}

pub fn maskedAggMax(val_col: [*]const f64, type_data: [*]const u32, type_id: u32, batch_len: u32, current_max: f64) f64 {
    var max_vec: V4f64 = @splat(current_max);
    const type_id_vec: V4u32 = @splat(type_id);
    const identity: V4f64 = @splat(-std.math.inf(f64));
    var i: u32 = 0;
    while (i + 4 <= batch_len) : (i += 4) {
        const type_vec: V4u32 = .{ type_data[i], type_data[i + 1], type_data[i + 2], type_data[i + 3] };
        const mask = type_vec == type_id_vec;
        const val_vec: V4f64 = .{ val_col[i], val_col[i + 1], val_col[i + 2], val_col[i + 3] };
        max_vec = @max(max_vec, @select(f64, mask, val_vec, identity));
    }
    var result = @reduce(.Max, max_vec);
    while (i < batch_len) : (i += 1) {
        if (type_data[i] == type_id) result = @max(result, val_col[i]);
    }
    return result;
}

// =============================================================================
// Comptime generic column reducer — dispatches to SIMD or scalar
// =============================================================================

/// Reduce a column to a single value. Dispatches to SIMD for f64 and i64 sum.
/// type_mask: only include rows matching type_id (FOR_EACH).
/// pred_col: only include rows where pred[i] != 0 (_IF variants).
pub fn reduceCol(
    comptime T: type,
    comptime kind: AggKind,
    vals: [*]const T,
    batch_len: u32,
    current: T,
    type_mask: ?TypeMask,
    pred_col: ?[*]const u32,
) T {
    // f64 SIMD path — no predicate
    if (T == f64 and pred_col == null) {
        if (type_mask == null) {
            return switch (kind) {
                .sum => current + batchAggSum(vals, batch_len),
                .min => batchAggMin(vals, batch_len, current),
                .max => batchAggMax(vals, batch_len, current),
            };
        }
        if (type_mask) |m| {
            return switch (kind) {
                .sum => current + maskedAggSum(vals, m.data, m.id, batch_len),
                .min => maskedAggMin(vals, m.data, m.id, batch_len, current),
                .max => maskedAggMax(vals, m.data, m.id, batch_len, current),
            };
        }
    }

    // i64 SIMD path for sum
    if (T == i64 and kind == .sum and pred_col == null) {
        var sum_vec: V2i64 = @splat(@as(i64, 0));
        var i: u32 = 0;
        if (type_mask) |m| {
            const V2u32 = @Vector(2, u32);
            const type_id_vec: V2u32 = @splat(m.id);
            const zero_i64: V2i64 = @splat(@as(i64, 0));
            while (i + 2 <= batch_len) : (i += 2) {
                const type_vec: V2u32 = .{ m.data[i], m.data[i + 1] };
                const mask = type_vec == type_id_vec;
                const val_vec: V2i64 = .{ vals[i], vals[i + 1] };
                sum_vec +%= @select(i64, mask, val_vec, zero_i64);
            }
        } else {
            while (i + 2 <= batch_len) : (i += 2) {
                const val_vec: V2i64 = .{ vals[i], vals[i + 1] };
                sum_vec +%= val_vec;
            }
        }
        var acc = current +% @reduce(.Add, sum_vec);
        while (i < batch_len) : (i += 1) {
            if (type_mask) |m| {
                if (m.data[i] != m.id) continue;
            }
            acc +%= vals[i];
        }
        return acc;
    }

    // Scalar path — i64 min/max, or any type with predicate column
    var acc = current;
    var i: u32 = 0;
    while (i < batch_len) : (i += 1) {
        if (type_mask) |m| {
            if (m.data[i] != m.id) continue;
        }
        if (pred_col) |p| {
            if (p[i] == 0) continue;
        }
        switch (kind) {
            .sum => acc = if (T == i64) acc +% vals[i] else acc + vals[i],
            .min => {
                if (vals[i] < acc) acc = vals[i];
            },
            .max => {
                if (vals[i] > acc) acc = vals[i];
            },
        }
    }
    return acc;
}

// =============================================================================
// Typed Aggregate Slot — comptime layout for COUNT (8 bytes) vs standard (16)
// =============================================================================

const types = @import("types.zig");

/// Initialize an aggregate slot at the given offset in state_base.
/// Handles all AggType variants: COUNT(8b, zero), SUM/AVG(16b, 0+0),
/// MIN(16b, +inf+0), MAX(16b, -inf+0), i64 variants similarly.
/// Returns the byte size consumed.
pub fn initAggSlot(state_base: [*]u8, offset: u32, agg_type: types.AggType) u32 {
    switch (agg_type) {
        .COUNT => {
            const s = AggSlot(.COUNT).bind(state_base, offset);
            s.setCount(0);
            return AggSlot(.COUNT).byte_size;
        },
        .SUM => {
            const s = AggSlot(.SUM).bind(state_base, offset);
            s.setValue(0.0);
            s.setCount(0);
            return AggSlot(.SUM).byte_size;
        },
        .MIN => {
            const s = AggSlot(.MIN).bind(state_base, offset);
            s.setValue(std.math.inf(f64));
            s.setCount(0);
            return AggSlot(.MIN).byte_size;
        },
        .MAX => {
            const s = AggSlot(.MAX).bind(state_base, offset);
            s.setValue(-std.math.inf(f64));
            s.setCount(0);
            return AggSlot(.MAX).byte_size;
        },
        .AVG => {
            const s = AggSlot(.AVG).bind(state_base, offset);
            s.setValue(0.0);
            s.setCount(0);
            return AggSlot(.AVG).byte_size;
        },
        .SUM_I64 => {
            const s = AggSlot(.SUM_I64).bind(state_base, offset);
            s.setValue(0);
            s.setCount(0);
            return AggSlot(.SUM_I64).byte_size;
        },
        .MIN_I64 => {
            const s = AggSlot(.MIN_I64).bind(state_base, offset);
            s.setValue(std.math.maxInt(i64));
            s.setCount(0);
            return AggSlot(.MIN_I64).byte_size;
        },
        .MAX_I64 => {
            const s = AggSlot(.MAX_I64).bind(state_base, offset);
            s.setValue(std.math.minInt(i64));
            s.setCount(0);
            return AggSlot(.MAX_I64).byte_size;
        },
        else => {
            // SCALAR subtypes or unknown — zero-fill 16 bytes
            @memset((state_base + offset)[0..16], 0);
            return 16;
        },
    }
}

/// Typed accessor for aggregate slot data. Encodes the layout difference
/// between compact COUNT (8 bytes: u64 count at offset 0) and standard
/// SUM/MIN/MAX (16 bytes: value at 0, count at 8).
pub fn AggSlot(comptime agg: types.AggType) type {
    const is_count = agg == .COUNT;
    const is_i64 = agg == .SUM_I64 or agg == .MIN_I64 or agg == .MAX_I64;
    const ValType = if (is_count) void else if (is_i64) i64 else f64;

    return struct {
        const Self = @This();
        base: [*]u8,

        /// Byte size of this aggregate slot.
        pub const byte_size: u32 = if (is_count) 8 else 16;

        pub fn bind(state_base: [*]u8, offset: u32) Self {
            return .{ .base = state_base + offset };
        }

        pub fn bindMeta(state_base: [*]u8, meta: types.SlotMeta) Self {
            return bind(state_base, meta.offset);
        }

        /// Read the count value (u64).
        pub fn count(self: Self) u64 {
            if (is_count) {
                // COUNT: u64 at offset 0
                const ptr: *align(1) const u64 = @ptrCast(self.base);
                return ptr.*;
            } else {
                // SUM/MIN/MAX: u64 at offset 8
                const ptr: *align(1) const u64 = @ptrCast(self.base + 8);
                return ptr.*;
            }
        }

        /// Read the accumulated value (f64 or i64). Not available for COUNT.
        pub fn value(self: Self) ValType {
            if (is_count) @compileError("COUNT slots have no value field");
            const ptr: *align(1) const ValType = @ptrCast(self.base);
            return ptr.*;
        }

        /// Write the count.
        pub fn setCount(self: Self, c: u64) void {
            if (is_count) {
                const ptr: *align(1) u64 = @ptrCast(self.base);
                ptr.* = c;
            } else {
                const ptr: *align(1) u64 = @ptrCast(self.base + 8);
                ptr.* = c;
            }
        }

        /// Write the accumulated value.
        pub fn setValue(self: Self, v: ValType) void {
            if (is_count) @compileError("COUNT slots have no value field");
            const ptr: *align(1) ValType = @ptrCast(self.base);
            ptr.* = v;
        }
    };
}

// =============================================================================
// Tests
// =============================================================================

const testing = std.testing;

test "batchAggSum — f64 SIMD reduction" {
    var data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0 };
    try testing.expectApproxEqAbs(@as(f64, 28.0), batchAggSum(&data, 7), 0.001);
    try testing.expectApproxEqAbs(@as(f64, 10.0), batchAggSum(&data, 4), 0.001); // SIMD-only (no tail)
}

test "batchAggMin/Max — f64 SIMD" {
    var data = [_]f64{ 5.0, 2.0, 8.0, 1.0, 7.0 };
    try testing.expectApproxEqAbs(@as(f64, 1.0), batchAggMin(&data, 5, std.math.inf(f64)), 0.001);
    try testing.expectApproxEqAbs(@as(f64, 8.0), batchAggMax(&data, 5, -std.math.inf(f64)), 0.001);
}

test "maskedAggSum — type-filtered f64 SIMD" {
    var vals = [_]f64{ 10.0, 20.0, 30.0, 40.0, 50.0, 60.0 };
    var types_col = [_]u32{ 1, 2, 1, 2, 1, 2 }; // only type=1 rows: 10+30+50=90
    try testing.expectApproxEqAbs(@as(f64, 90.0), maskedAggSum(&vals, &types_col, 1, 6), 0.001);
    try testing.expectApproxEqAbs(@as(f64, 120.0), maskedAggSum(&vals, &types_col, 2, 6), 0.001);
}

test "maskedAggCount — SIMD u32 count" {
    var types_col = [_]u32{ 1, 2, 1, 1, 2, 1, 2, 1 };
    try testing.expectEqual(@as(u32, 5), maskedAggCount(&types_col, 1, 8));
    try testing.expectEqual(@as(u32, 3), maskedAggCount(&types_col, 2, 8));
}

test "reduceCol — i64 sum SIMD (V2i64)" {
    var data = [_]i64{ 100, 200, 300, 400, 500 };
    const result = reduceCol(i64, .sum, &data, 5, 0, null, null);
    try testing.expectEqual(@as(i64, 1500), result);
}

test "reduceCol — f64 with predicate column" {
    var vals = [_]f64{ 10.0, 20.0, 30.0, 40.0 };
    var pred = [_]u32{ 1, 0, 1, 0 }; // only indices 0 and 2
    const result = reduceCol(f64, .sum, &vals, 4, 0, null, &pred);
    try testing.expectApproxEqAbs(@as(f64, 40.0), result, 0.001);
}

test "AggSlot — COUNT layout (8 bytes, count at offset 0)" {
    var buf: [16]u8 align(8) = [_]u8{0} ** 16;
    const slot = AggSlot(.COUNT).bind(&buf, 0);
    try testing.expectEqual(@as(u64, 0), slot.count());
    slot.setCount(42);
    try testing.expectEqual(@as(u64, 42), slot.count());
    try testing.expectEqual(@as(u32, 8), AggSlot(.COUNT).byte_size);
}

test "AggSlot — SUM layout (16 bytes, value at 0, count at 8)" {
    var buf: [16]u8 align(8) = [_]u8{0} ** 16;
    const slot = AggSlot(.SUM).bind(&buf, 0);
    try testing.expectApproxEqAbs(@as(f64, 0.0), slot.value(), 0.001);
    try testing.expectEqual(@as(u64, 0), slot.count());
    slot.setValue(123.456);
    slot.setCount(5);
    try testing.expectApproxEqAbs(@as(f64, 123.456), slot.value(), 0.001);
    try testing.expectEqual(@as(u64, 5), slot.count());
    try testing.expectEqual(@as(u32, 16), AggSlot(.SUM).byte_size);
}

test "AggSlot — SUM_I64 layout (16 bytes, i64 value)" {
    var buf: [16]u8 align(8) = [_]u8{0} ** 16;
    const slot = AggSlot(.SUM_I64).bind(&buf, 0);
    slot.setValue(999_999_999_999);
    slot.setCount(1);
    try testing.expectEqual(@as(i64, 999_999_999_999), slot.value());
    try testing.expectEqual(@as(u64, 1), slot.count());
}

// =============================================================================
// Parametric SIMD batch-size tests
// =============================================================================
// Exercise SIMD paths across tail-only, exact-multiple, mixed, and large sizes
// to ensure the 4-wide V4f64 / 2-wide V2i64 loops + scalar tails are correct.

const parametric_f64_sizes = [_]u32{ 1, 2, 3, 4, 5, 7, 8, 15, 16, 31, 32, 100, 256 };
const parametric_masked_sizes = [_]u32{ 1, 4, 8, 32, 100 };
const parametric_i64_sizes = [_]u32{ 1, 3, 7, 8, 16, 100 };

/// Precomputed f64 buffer: vals[i] = @as(f64, @floatFromInt(i + 1))
const max_param_len = 256;
const param_f64_vals: [max_param_len]f64 = blk: {
    var arr: [max_param_len]f64 = undefined;
    for (0..max_param_len) |i| {
        arr[i] = @as(f64, @floatFromInt(i + 1));
    }
    break :blk arr;
};

/// Precomputed i64 buffer: vals[i] = @as(i64, @intCast(i + 1))
const param_i64_vals: [max_param_len]i64 = blk: {
    var arr: [max_param_len]i64 = undefined;
    for (0..max_param_len) |i| {
        arr[i] = @as(i64, @intCast(i + 1));
    }
    break :blk arr;
};

/// Alternating type_ids: even indices = 1, odd indices = 2
const param_type_ids: [max_param_len]u32 = blk: {
    var arr: [max_param_len]u32 = undefined;
    for (0..max_param_len) |i| {
        arr[i] = if (i % 2 == 0) 1 else 2;
    }
    break :blk arr;
};

/// Predicate column: every 3rd element (i % 3 == 0) is 1, rest 0
const param_pred_col: [max_param_len]u32 = blk: {
    var arr: [max_param_len]u32 = undefined;
    for (0..max_param_len) |i| {
        arr[i] = if (i % 3 == 0) 1 else 0;
    }
    break :blk arr;
};

test "batchAggSum — parametric batch sizes" {
    for (parametric_f64_sizes) |n| {
        const expected = @as(f64, @floatFromInt(n)) * @as(f64, @floatFromInt(n + 1)) / 2.0;
        const result = batchAggSum(&param_f64_vals, n);
        try testing.expectApproxEqAbs(expected, result, 0.001);
    }
}

test "batchAggMin — parametric batch sizes" {
    for (parametric_f64_sizes) |n| {
        const result = batchAggMin(&param_f64_vals, n, std.math.inf(f64));
        try testing.expectApproxEqAbs(@as(f64, 1.0), result, 0.001);
    }
}

test "batchAggMax — parametric batch sizes" {
    for (parametric_f64_sizes) |n| {
        const expected = @as(f64, @floatFromInt(n));
        const result = batchAggMax(&param_f64_vals, n, -std.math.inf(f64));
        try testing.expectApproxEqAbs(expected, result, 0.001);
    }
}

test "maskedAggSum — parametric batch sizes" {
    for (parametric_masked_sizes) |n| {
        // Even-indexed elements (type_id=1): vals[0], vals[2], vals[4], ...
        // These are 1, 3, 5, ... — sum of first ceil(n/2) odd naturals
        var expected: f64 = 0.0;
        var i: u32 = 0;
        while (i < n) : (i += 1) {
            if (i % 2 == 0) expected += param_f64_vals[i];
        }
        const result = maskedAggSum(&param_f64_vals, &param_type_ids, 1, n);
        try testing.expectApproxEqAbs(expected, result, 0.001);
    }
}

test "maskedAggCount — parametric batch sizes" {
    for (parametric_masked_sizes) |n| {
        // Count of even-indexed elements (type_id=1) = ceil(n/2)
        const expected = (n + 1) / 2;
        const result = maskedAggCount(&param_type_ids, 1, n);
        try testing.expectEqual(expected, result);
    }
}

test "reduceCol i64 sum — parametric batch sizes" {
    for (parametric_i64_sizes) |n| {
        const n_i64 = @as(i64, @intCast(n));
        const expected = @divExact(n_i64 * (n_i64 + 1), 2);
        const result = reduceCol(i64, .sum, &param_i64_vals, n, 0, null, null);
        try testing.expectEqual(expected, result);
    }
}

test "reduceCol f64 sum with predicate — batch_len=32" {
    // pred[i] = 1 when i % 3 == 0, so indices 0,3,6,9,...,30
    var expected: f64 = 0.0;
    var i: u32 = 0;
    while (i < 32) : (i += 1) {
        if (i % 3 == 0) expected += param_f64_vals[i];
    }
    const result = reduceCol(f64, .sum, &param_f64_vals, 32, 0, null, &param_pred_col);
    try testing.expectApproxEqAbs(expected, result, 0.001);
}
