//! Slot initialization/accessors plus pure batch-reduction kernels.
//!
//! Layout: compact COUNT is 8 bytes (u64 count at offset 0); every other
//! aggregate is 16 bytes (value at 0 — f64, or i64 for the `_I64` family —
//! and u64 count at 8).
//!
//! # FP determinism (specs/vo/01-canonical-encoding.md profile)
//!
//! The kernels accumulate through 4-wide f64 lanes and finish with a
//! sequential left-to-right lane fold. The fixed evaluation order matters:
//! pairwise reduction would produce a different result for cancellation-prone
//! inputs. Min/max use minNum semantics — NaN yields the other operand and a
//! tie yields the second operand — so this module hand-rolls those operations
//! and preserves the exact lane structure with scalar arithmetic. The kernels
//! deliberately do not canonicalize NaN or negative zero; the VO digest
//! profile applies where digests are formed.

use crate::bytes;

/// AggType discriminants switched on by this module. Initialization operates
/// on the raw metadata byte, not the enum: the capacity byte passes through,
/// and bytes outside the enum (or scalar subtypes) take the zero-fill default.
/// Converting through `AggType::from_u8` would silently rewrite invalid bytes;
/// the raw byte is the contract.
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

/// Initialize an aggregate slot and return its byte size.
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
// Pure batch-reduction kernels

/// Aggregate kind used by the reduction kernels.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggKind {
    Sum,
    Min,
    Max,
}

/// Type mask for `FOR_EACH`: include row `i` only when `data[i] == id`.
#[derive(Clone, Copy, Debug)]
pub struct TypeMask<'a> {
    pub data: &'a [u32],
    pub id: u32,
}

// WHY tie-returns-second-operand is the profile (deliberate, digest-bearing):
// ±0 selection order is observable in digests, and changing it would silently
// re-digest archived state for no functional gain. NaN yields the other
// operand (min/max skip NaN).
//
// Maintenance-algebra note: min/max are the SUPPORT-SCAN class —
// NOT group-invertible; a retraction that removes the
// current extreme cannot be undone arithmetically and forces a support
// re-scan (or a multiset/heap support structure). SUM and COUNT are the
// group class (exactly maintainable under ±weights). Any future TREAT/delta
// maintenance layer builds on exactly this split; these kernels are the
// batch-fold form of the same per-aggregate algebra.
/// Min profile: NaN yields the other operand and ties yield the second.
#[inline]
pub(crate) fn min_profile(a: f64, b: f64) -> f64 {
    if a.is_nan() {
        return b;
    }
    if b.is_nan() {
        return a;
    }
    if a < b { a } else { b }
}

/// Max profile: NaN yields the other operand and ties yield the second.
#[inline]
pub(crate) fn max_profile(a: f64, b: f64) -> f64 {
    if a.is_nan() {
        return b;
    }
    if b.is_nan() {
        return a;
    }
    if a > b { a } else { b }
}

/// Sequential left-to-right fold of four f64 lanes.
#[inline]
fn reduce_add_4(l: [f64; 4]) -> f64 {
    ((l[0] + l[1]) + l[2]) + l[3]
}

/// Sequential four-lane minimum using the module's min profile.
#[inline]
fn reduce_min_4(l: [f64; 4]) -> f64 {
    min_profile(min_profile(min_profile(l[0], l[1]), l[2]), l[3])
}

/// Sequential four-lane maximum using the module's max profile.
#[inline]
fn reduce_max_4(l: [f64; 4]) -> f64 {
    max_profile(max_profile(max_profile(l[0], l[1]), l[2]), l[3])
}

/// Sum lanes in chunk order (`k`, `k+4`, `k+8`, …), then process the scalar
/// tail.
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

/// Batch minimum, preserving the supplied current minimum.
pub fn batch_agg_min(vals: &[f64], current_min: f64) -> f64 {
    let batch_len = vals.len();
    if batch_len == 0 {
        return current_min;
    }
    let mut lanes = [current_min; 4];
    let mut i = 0;
    while i + 4 <= batch_len {
        for (k, lane) in lanes.iter_mut().enumerate() {
            *lane = min_profile(*lane, vals[i + k]);
        }
        i += 4;
    }
    let mut result = reduce_min_4(lanes);
    while i < batch_len {
        result = min_profile(result, vals[i]);
        i += 1;
    }
    result
}

/// Batch maximum, preserving the supplied current maximum.
pub fn batch_agg_max(vals: &[f64], current_max: f64) -> f64 {
    let batch_len = vals.len();
    if batch_len == 0 {
        return current_max;
    }
    let mut lanes = [current_max; 4];
    let mut i = 0;
    while i + 4 <= batch_len {
        for (k, lane) in lanes.iter_mut().enumerate() {
            *lane = max_profile(*lane, vals[i + k]);
        }
        i += 4;
    }
    let mut result = reduce_max_4(lanes);
    while i < batch_len {
        result = max_profile(result, vals[i]);
        i += 1;
    }
    result
}

/// Masked sum; masked-out lanes contribute `+0.0`, the additive identity.
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

/// Masked count. The slice length is the batch extent, so no separate length
/// parameter can disagree with it.
pub fn masked_agg_count(type_data: &[u32], type_id: u32) -> u32 {
    let batch_len = type_data.len();
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

/// Masked minimum; masked-out lanes see `+inf`.
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
            *lane = min_profile(*lane, v);
        }
        i += 4;
    }
    let mut result = reduce_min_4(lanes);
    while i < batch_len {
        if type_data[i] == type_id {
            result = min_profile(result, vals[i]);
        }
        i += 1;
    }
    result
}

/// Masked maximum; masked-out lanes see `-inf`.
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
            *lane = max_profile(*lane, v);
        }
        i += 4;
    }
    let mut result = reduce_max_4(lanes);
    while i < batch_len {
        if type_data[i] == type_id {
            result = max_profile(result, vals[i]);
        }
        i += 1;
    }
    result
}

/// Reduce an f64 column. Without a predicate, use the lane kernels and add
/// the sum to `current`; with a predicate, use the scalar path.
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

    // Scalar path uses the same NaN policy as the lane path: min/max return
    // the other operand for NaN. Keeping one convention makes a fold
    // reproducible by a support re-scan.
    // where a fold must be reproducible by a support re-scan
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
                acc = min_profile(acc, v);
            }
            AggKind::Max => {
                acc = max_profile(acc, v);
            }
        }
    }
    acc
}

/// Reduce an i64 column. Sum uses wrapping adds; min/max use the scalar path
/// because their operation is not lane-vectorized here.
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
            AggKind::Min => acc = acc.min(v),
            AggKind::Max => acc = acc.max(v),
        }
    }
    acc
}
