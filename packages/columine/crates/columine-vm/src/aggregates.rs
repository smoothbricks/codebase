//! Port of `packages/columine/src/vm/aggregates.zig` — slot init/accessors
//! plus the pure batch-reduction kernels.
//!
//! Layout (aggregates.zig:296-309): compact COUNT is 8 bytes (u64 count at
//! offset 0); every other aggregate is 16 bytes (value at 0 — f64, or i64 for
//! the `_I64` family — and u64 count at 8).
//!
//! # FP determinism (specs/vo/01-canonical-encoding.md profile)
//!
//! The Zig kernels accumulate through 4-wide `@Vector(4, f64)` lanes and
//! finish with `@reduce`, which Zig lowers as a SEQUENTIAL left-to-right
//! lane fold (probed on zig 0.16.0, Debug == ReleaseSmall:
//! `@reduce(.Add, .{1e16, 1.0, -1e16, 1.0}) == 1.0`, bit 0x3ff0000000000000 —
//! pairwise would give 0.0). `@min`/`@max` are minNum-flavoured — a NaN
//! operand yields the OTHER operand — and a TIE yields the SECOND operand:
//! probed `@min(-0.0, 0.0) == +0.0` and `@min(0.0, -0.0) == -0.0` (bits 0x0 /
//! 0x8000000000000000). Rust's `f64::min`/`f64::max` intrinsics do NOT pin
//! that tie order (aarch64 `fminnm` returns -0.0 for either order), so this
//! module hand-rolls `min_zig`/`max_zig` and replicates the exact lane
//! structure with scalar arithmetic: fixed evaluation order, no FMA, no
//! reassociation, no autovectorization dependence. The kernels deliberately
//! do NOT canonicalize NaN / -0 — the Zig kernels don't either; the VO
//! digest profile applies where digests are formed, and bug-for-bug parity
//! with the current runtime is this stage's oracle.

use crate::bytes;

/// AggType discriminants this module switches on. `initAggSlot` operates on
/// the RAW metadata byte, not the enum: state_init.zig routes `cap_lo`
/// straight through, and bytes outside the enum (or the SCALAR subtypes)
/// take the zero-fill default branch. Porting via `AggType::from_u8` would
/// silently rewrite invalid bytes; the raw byte IS the contract.
const AGG_SUM: u8 = 1;
const AGG_COUNT: u8 = 2;
const AGG_MIN: u8 = 3;
const AGG_MAX: u8 = 4;
const AGG_AVG: u8 = 5;
const AGG_SUM_I64: u8 = 11;
const AGG_MIN_I64: u8 = 12;
const AGG_MAX_I64: u8 = 13;

/// `AggSlot(agg).byte_size` — 8 for COUNT, 16 for everything else.
pub const fn agg_slot_byte_size(agg_type_byte: u8) -> u32 {
    if agg_type_byte == AGG_COUNT { 8 } else { 16 }
}

/// aggregates.zig:238-293 `initAggSlot`. Returns the slot's byte size.
pub fn init_agg_slot(state: &mut [u8], offset: u32, agg_type_byte: u8) -> u32 {
    match agg_type_byte {
        AGG_COUNT => {
            bytes::write_u64(state, offset, 0);
            8
        }
        AGG_SUM | AGG_AVG => {
            bytes::write_f64(state, offset, 0.0);
            bytes::write_u64(state, offset + 8, 0);
            16
        }
        AGG_MIN => {
            bytes::write_f64(state, offset, f64::INFINITY);
            bytes::write_u64(state, offset + 8, 0);
            16
        }
        AGG_MAX => {
            bytes::write_f64(state, offset, f64::NEG_INFINITY);
            bytes::write_u64(state, offset + 8, 0);
            16
        }
        AGG_SUM_I64 => {
            bytes::write_i64(state, offset, 0);
            bytes::write_u64(state, offset + 8, 0);
            16
        }
        AGG_MIN_I64 => {
            bytes::write_i64(state, offset, i64::MAX);
            bytes::write_u64(state, offset + 8, 0);
            16
        }
        AGG_MAX_I64 => {
            bytes::write_i64(state, offset, i64::MIN);
            bytes::write_u64(state, offset + 8, 0);
            16
        }
        // SCALAR subtypes (8-10) or unknown — zero-fill 16 bytes.
        _ => {
            bytes::zero(state, offset, 16);
            16
        }
    }
}

/// `AggSlot(agg).value()` for the f64 family (value at offset 0).
pub fn agg_value_f64(state: &[u8], offset: u32) -> f64 {
    bytes::read_f64(state, offset)
}

/// `AggSlot(agg).value()` for the i64 family (value at offset 0).
pub fn agg_value_i64(state: &[u8], offset: u32) -> i64 {
    bytes::read_i64(state, offset)
}

/// `AggSlot(agg).count()` — at offset 0 for COUNT, offset 8 otherwise.
pub fn agg_count(state: &[u8], offset: u32, agg_type_byte: u8) -> u64 {
    if agg_type_byte == AGG_COUNT {
        bytes::read_u64(state, offset)
    } else {
        bytes::read_u64(state, offset + 8)
    }
}

/// `AggSlot(agg).setValue()` for the f64 family (value at offset 0).
pub fn agg_set_value_f64(state: &mut [u8], offset: u32, v: f64) {
    bytes::write_f64(state, offset, v);
}

/// `AggSlot(agg).setValue()` for the i64 family (value at offset 0).
pub fn agg_set_value_i64(state: &mut [u8], offset: u32, v: i64) {
    bytes::write_i64(state, offset, v);
}

/// `AggSlot(agg).setCount()` — at offset 0 for COUNT, offset 8 otherwise.
pub fn agg_set_count(state: &mut [u8], offset: u32, agg_type_byte: u8, c: u64) {
    if agg_type_byte == AGG_COUNT {
        bytes::write_u64(state, offset, c);
    } else {
        bytes::write_u64(state, offset + 8, c);
    }
}

// =============================================================================
// Pure batch-reduction kernels (aggregates.zig:20-226)
// =============================================================================

/// aggregates.zig:20 `AggKind`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggKind {
    Sum,
    Min,
    Max,
}

/// aggregates.zig:22 `TypeMask` — FOR_EACH filtering: include row `i` only
/// when `data[i] == id`.
#[derive(Clone, Copy, Debug)]
pub struct TypeMask<'a> {
    pub data: &'a [u32],
    pub id: u32,
}

// ZIG-PARITY: min/max ties return the SECOND operand (Zig @min/@max as probed; affects ±0 selection); decide at sweep: adopt as the FP-determinism profile or normalize — digest-bearing.
/// Zig `@min` as probed (module docs): NaN yields the other operand, a tie
/// yields the SECOND operand. `f64::min` does not pin the tie order.
#[inline]
fn min_zig(a: f64, b: f64) -> f64 {
    if a.is_nan() {
        return b;
    }
    if b.is_nan() {
        return a;
    }
    if a < b { a } else { b }
}

/// Zig `@max` as probed: NaN yields the other operand, tie yields the second.
#[inline]
fn max_zig(a: f64, b: f64) -> f64 {
    if a.is_nan() {
        return b;
    }
    if b.is_nan() {
        return a;
    }
    if a > b { a } else { b }
}

/// Zig `@reduce(.Add, V4f64)` — sequential left-to-right lane fold (probed).
#[inline]
fn reduce_add_4(l: [f64; 4]) -> f64 {
    ((l[0] + l[1]) + l[2]) + l[3]
}

/// Zig `@reduce(.Min, V4f64)` with `@min` semantics, sequential (probed).
#[inline]
fn reduce_min_4(l: [f64; 4]) -> f64 {
    min_zig(min_zig(min_zig(l[0], l[1]), l[2]), l[3])
}

/// Zig `@reduce(.Max, V4f64)`, sequential.
#[inline]
fn reduce_max_4(l: [f64; 4]) -> f64 {
    max_zig(max_zig(max_zig(l[0], l[1]), l[2]), l[3])
}

/// aggregates.zig:28 `batchAggSum`. Lane `k` accumulates elements
/// `k, k+4, k+8, …` in chunk order; the lane fold and the scalar tail follow.
pub fn batch_agg_sum(vals: &[f64]) -> f64 {
    let batch_len = vals.len();
    let mut lanes = [0.0f64; 4];
    let mut i = 0;
    while i + 4 <= batch_len {
        lanes[0] += vals[i];
        lanes[1] += vals[i + 1];
        lanes[2] += vals[i + 2];
        lanes[3] += vals[i + 3];
        i += 4;
    }
    let mut result = reduce_add_4(lanes);
    while i < batch_len {
        result += vals[i];
        i += 1;
    }
    result
}

/// aggregates.zig:40 `batchAggMin`.
pub fn batch_agg_min(vals: &[f64], current_min: f64) -> f64 {
    let batch_len = vals.len();
    if batch_len == 0 {
        return current_min;
    }
    let mut lanes = [current_min; 4];
    let mut i = 0;
    while i + 4 <= batch_len {
        for (k, lane) in lanes.iter_mut().enumerate() {
            *lane = min_zig(*lane, vals[i + k]);
        }
        i += 4;
    }
    let mut result = reduce_min_4(lanes);
    while i < batch_len {
        result = min_zig(result, vals[i]);
        i += 1;
    }
    result
}

/// aggregates.zig:53 `batchAggMax`.
pub fn batch_agg_max(vals: &[f64], current_max: f64) -> f64 {
    let batch_len = vals.len();
    if batch_len == 0 {
        return current_max;
    }
    let mut lanes = [current_max; 4];
    let mut i = 0;
    while i + 4 <= batch_len {
        for (k, lane) in lanes.iter_mut().enumerate() {
            *lane = max_zig(*lane, vals[i + k]);
        }
        i += 4;
    }
    let mut result = reduce_max_4(lanes);
    while i < batch_len {
        result = max_zig(result, vals[i]);
        i += 1;
    }
    result
}

/// aggregates.zig:70 `maskedAggSum` — masked-out lanes contribute `+0.0`
/// (the Zig `@select` zero identity).
pub fn masked_agg_sum(vals: &[f64], type_data: &[u32], type_id: u32) -> f64 {
    let batch_len = vals.len();
    if batch_len == 0 {
        return 0.0;
    }
    let mut lanes = [0.0f64; 4];
    let mut i = 0;
    while i + 4 <= batch_len {
        for (k, lane) in lanes.iter_mut().enumerate() {
            *lane += if type_data[i + k] == type_id {
                vals[i + k]
            } else {
                0.0
            };
        }
        i += 4;
    }
    let mut result = reduce_add_4(lanes);
    while i < batch_len {
        if type_data[i] == type_id {
            result += vals[i];
        }
        i += 1;
    }
    result
}

/// aggregates.zig:89 `maskedAggCount`.
pub fn masked_agg_count(type_data: &[u32], type_id: u32, batch_len: usize) -> u32 {
    let mut count: u32 = 0;
    let mut i = 0;
    while i + 4 <= batch_len {
        for k in 0..4 {
            count += u32::from(type_data[i + k] == type_id);
        }
        i += 4;
    }
    while i < batch_len {
        count += u32::from(type_data[i] == type_id);
        i += 1;
    }
    count
}

/// aggregates.zig:105 `maskedAggMin` — masked-out lanes see `+inf`.
pub fn masked_agg_min(vals: &[f64], type_data: &[u32], type_id: u32, current_min: f64) -> f64 {
    let batch_len = vals.len();
    let mut lanes = [current_min; 4];
    let mut i = 0;
    while i + 4 <= batch_len {
        for (k, lane) in lanes.iter_mut().enumerate() {
            let v = if type_data[i + k] == type_id {
                vals[i + k]
            } else {
                f64::INFINITY
            };
            *lane = min_zig(*lane, v);
        }
        i += 4;
    }
    let mut result = reduce_min_4(lanes);
    while i < batch_len {
        if type_data[i] == type_id {
            result = min_zig(result, vals[i]);
        }
        i += 1;
    }
    result
}

/// aggregates.zig:123 `maskedAggMax` — masked-out lanes see `-inf`.
pub fn masked_agg_max(vals: &[f64], type_data: &[u32], type_id: u32, current_max: f64) -> f64 {
    let batch_len = vals.len();
    let mut lanes = [current_max; 4];
    let mut i = 0;
    while i + 4 <= batch_len {
        for (k, lane) in lanes.iter_mut().enumerate() {
            let v = if type_data[i + k] == type_id {
                vals[i + k]
            } else {
                f64::NEG_INFINITY
            };
            *lane = max_zig(*lane, v);
        }
        i += 4;
    }
    let mut result = reduce_max_4(lanes);
    while i < batch_len {
        if type_data[i] == type_id {
            result = max_zig(result, vals[i]);
        }
        i += 1;
    }
    result
}

/// aggregates.zig:148 `reduceCol` for `T == f64`. Dispatch mirrors the Zig
/// comptime branches: no predicate → lane kernels (sum ADDS the kernel result
/// to `current`); any predicate → the scalar path.
pub fn reduce_col_f64(
    kind: AggKind,
    vals: &[f64],
    current: f64,
    type_mask: Option<TypeMask<'_>>,
    pred_col: Option<&[u32]>,
) -> f64 {
    if pred_col.is_none() {
        return match type_mask {
            None => match kind {
                AggKind::Sum => current + batch_agg_sum(vals),
                AggKind::Min => batch_agg_min(vals, current),
                AggKind::Max => batch_agg_max(vals, current),
            },
            Some(m) => match kind {
                AggKind::Sum => current + masked_agg_sum(vals, m.data, m.id),
                AggKind::Min => masked_agg_min(vals, m.data, m.id, current),
                AggKind::Max => masked_agg_max(vals, m.data, m.id, current),
            },
        };
    }

    // ZIG-PARITY: the scalar predicated min/max accumulator is NaN-sticky (plain `<`/`>`, unlike the vector path's NaN-skipping @min/@max); intended fix: one NaN policy for both paths.
    // Scalar path (aggregates.zig:206-225). NOTE the Zig scalar min/max use
    // plain `<`/`>` comparisons, NOT `@min`/`@max`: a NaN accumulator sticks
    // (`vals[i] < NaN` is false) — preserved bug-for-bug.
    let mut acc = current;
    for (i, &v) in vals.iter().enumerate() {
        if let Some(m) = type_mask
            && m.data[i] != m.id
        {
            continue;
        }
        if let Some(p) = pred_col
            && p[i] == 0
        {
            continue;
        }
        match kind {
            AggKind::Sum => acc += v,
            AggKind::Min => {
                if v < acc {
                    acc = v;
                }
            }
            AggKind::Max => {
                if v > acc {
                    acc = v;
                }
            }
        }
    }
    acc
}

/// aggregates.zig:148 `reduceCol` for `T == i64`. Sum uses wrapping adds
/// (`+%`, V2i64 two-lane structure — associative, so the lane split is not
/// observable); min/max take the scalar path exactly like the Zig comptime
/// dispatch (no i64 SIMD min/max in the Zig).
pub fn reduce_col_i64(
    kind: AggKind,
    vals: &[i64],
    current: i64,
    type_mask: Option<TypeMask<'_>>,
    pred_col: Option<&[u32]>,
) -> i64 {
    if kind == AggKind::Sum && pred_col.is_none() {
        let mut lanes = [0i64; 2];
        let mut i = 0;
        while i + 2 <= vals.len() {
            for (k, lane) in lanes.iter_mut().enumerate() {
                let include = match type_mask {
                    Some(m) => m.data[i + k] == m.id,
                    None => true,
                };
                if include {
                    *lane = lane.wrapping_add(vals[i + k]);
                }
            }
            i += 2;
        }
        let mut acc = current.wrapping_add(lanes[0].wrapping_add(lanes[1]));
        while i < vals.len() {
            let include = match type_mask {
                Some(m) => m.data[i] == m.id,
                None => true,
            };
            if include {
                acc = acc.wrapping_add(vals[i]);
            }
            i += 1;
        }
        return acc;
    }

    let mut acc = current;
    for (i, &v) in vals.iter().enumerate() {
        if let Some(m) = type_mask
            && m.data[i] != m.id
        {
            continue;
        }
        if let Some(p) = pred_col
            && p[i] == 0
        {
            continue;
        }
        match kind {
            AggKind::Sum => acc = acc.wrapping_add(v),
            AggKind::Min => {
                if v < acc {
                    acc = v;
                }
            }
            AggKind::Max => {
                if v > acc {
                    acc = v;
                }
            }
        }
    }
    acc
}
