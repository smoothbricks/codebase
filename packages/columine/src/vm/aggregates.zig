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
// Type-masked SIMD reductions (FOR_EACH_EVENT: only rows matching type_id)
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
/// type_mask: only include rows matching type_id (FOR_EACH_EVENT).
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
