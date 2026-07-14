//! Translated `test "…"` blocks from `packages/columine/src/vm/aggregates.zig`
//! (16/16), the zig-0.16.0 FP-probe bit pins, and the slice-4 proptests.
//!
//! The Zig tests assert with `expectApproxEqAbs(_, _, 0.001)`; the ports keep
//! that tolerance. The probe-pin tests assert EXACT bit patterns — they are
//! the FP-determinism contract (see the module docs of `aggregates`).

use columine_vm::aggregates::{
    AggKind, TypeMask, agg_count, agg_set_count, agg_set_value_f64, agg_set_value_i64,
    agg_slot_byte_size, agg_value_f64, agg_value_i64, batch_agg_max, batch_agg_min, batch_agg_sum,
    init_agg_slot, masked_agg_count, masked_agg_max, masked_agg_min, masked_agg_sum,
    reduce_col_f64, reduce_col_i64,
};
use proptest::prelude::*;

fn approx(a: f64, b: f64) {
    assert!((a - b).abs() < 0.001, "expected ~{b}, got {a}");
}

// ---------------------------------------------------------------------------
// aggregates.zig test blocks 1-6: kernel basics
// ---------------------------------------------------------------------------

#[test]
fn batch_agg_sum_f64_simd_reduction() {
    // aggregates.zig:364
    let data = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0];
    approx(batch_agg_sum(&data[..7]), 28.0);
    approx(batch_agg_sum(&data[..4]), 10.0); // SIMD-only (no tail)
}

#[test]
fn batch_agg_min_max_f64_simd() {
    // aggregates.zig:370
    let data = [5.0, 2.0, 8.0, 1.0, 7.0];
    approx(batch_agg_min(&data, f64::INFINITY), 1.0);
    approx(batch_agg_max(&data, f64::NEG_INFINITY), 8.0);
}

#[test]
fn masked_agg_sum_type_filtered() {
    // aggregates.zig:376
    let vals = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0];
    let types_col = [1u32, 2, 1, 2, 1, 2]; // type=1 rows: 10+30+50=90
    approx(masked_agg_sum(&vals, &types_col, 1), 90.0);
    approx(masked_agg_sum(&vals, &types_col, 2), 120.0);
}

#[test]
fn masked_agg_count_simd() {
    // aggregates.zig:383
    let types_col = [1u32, 2, 1, 1, 2, 1, 2, 1];
    assert_eq!(masked_agg_count(&types_col, 1, 8), 5);
    assert_eq!(masked_agg_count(&types_col, 2, 8), 3);
}

#[test]
fn reduce_col_i64_sum_simd() {
    // aggregates.zig:389
    let data = [100i64, 200, 300, 400, 500];
    assert_eq!(reduce_col_i64(AggKind::Sum, &data, 0, None, None), 1500);
}

#[test]
fn reduce_col_f64_with_predicate() {
    // aggregates.zig:395
    let vals = [10.0, 20.0, 30.0, 40.0];
    let pred = [1u32, 0, 1, 0]; // only indices 0 and 2
    approx(
        reduce_col_f64(AggKind::Sum, &vals, 0.0, None, Some(&pred)),
        40.0,
    );
}

// ---------------------------------------------------------------------------
// aggregates.zig test blocks 7-9: AggSlot layout
// ---------------------------------------------------------------------------

const AGG_COUNT: u8 = 2;
const AGG_SUM: u8 = 1;
const AGG_SUM_I64: u8 = 11;

#[test]
fn agg_slot_count_layout() {
    // aggregates.zig:402 — COUNT: 8 bytes, count at offset 0.
    let mut buf = [0u8; 16];
    assert_eq!(agg_count(&buf, 0, AGG_COUNT), 0);
    agg_set_count(&mut buf, 0, AGG_COUNT, 42);
    assert_eq!(agg_count(&buf, 0, AGG_COUNT), 42);
    assert_eq!(agg_slot_byte_size(AGG_COUNT), 8);
    // The count landed at offset 0, not 8.
    assert_eq!(u64::from_le_bytes(buf[0..8].try_into().unwrap()), 42);
}

#[test]
fn agg_slot_sum_layout() {
    // aggregates.zig:411 — SUM: 16 bytes, value at 0, count at 8.
    let mut buf = [0u8; 16];
    approx(agg_value_f64(&buf, 0), 0.0);
    assert_eq!(agg_count(&buf, 0, AGG_SUM), 0);
    agg_set_value_f64(&mut buf, 0, 123.456);
    agg_set_count(&mut buf, 0, AGG_SUM, 5);
    approx(agg_value_f64(&buf, 0), 123.456);
    assert_eq!(agg_count(&buf, 0, AGG_SUM), 5);
    assert_eq!(agg_slot_byte_size(AGG_SUM), 16);
    assert_eq!(u64::from_le_bytes(buf[8..16].try_into().unwrap()), 5);
}

#[test]
fn agg_slot_sum_i64_layout() {
    // aggregates.zig:423
    let mut buf = [0u8; 16];
    agg_set_value_i64(&mut buf, 0, 999_999_999_999);
    agg_set_count(&mut buf, 0, AGG_SUM_I64, 1);
    assert_eq!(agg_value_i64(&buf, 0), 999_999_999_999);
    assert_eq!(agg_count(&buf, 0, AGG_SUM_I64), 1);
}

// ---------------------------------------------------------------------------
// aggregates.zig test blocks 10-16: parametric batch sizes
// ---------------------------------------------------------------------------

const MAX_PARAM_LEN: usize = 256;
const PARAMETRIC_F64_SIZES: [usize; 13] = [1, 2, 3, 4, 5, 7, 8, 15, 16, 31, 32, 100, 256];
const PARAMETRIC_MASKED_SIZES: [usize; 5] = [1, 4, 8, 32, 100];
const PARAMETRIC_I64_SIZES: [usize; 6] = [1, 3, 7, 8, 16, 100];

fn param_f64_vals() -> [f64; MAX_PARAM_LEN] {
    core::array::from_fn(|i| (i + 1) as f64)
}

fn param_i64_vals() -> [i64; MAX_PARAM_LEN] {
    core::array::from_fn(|i| (i + 1) as i64)
}

/// Alternating type_ids: even indices = 1, odd = 2 (aggregates.zig:462).
fn param_type_ids() -> [u32; MAX_PARAM_LEN] {
    core::array::from_fn(|i| if i % 2 == 0 { 1 } else { 2 })
}

/// Every 3rd element passes the predicate (aggregates.zig:471).
fn param_pred_col() -> [u32; MAX_PARAM_LEN] {
    core::array::from_fn(|i| u32::from(i % 3 == 0))
}

#[test]
fn batch_agg_sum_parametric() {
    // aggregates.zig:479
    let vals = param_f64_vals();
    for &n in &PARAMETRIC_F64_SIZES {
        let expected = (n as f64) * ((n + 1) as f64) / 2.0;
        approx(batch_agg_sum(&vals[..n]), expected);
    }
}

#[test]
fn batch_agg_min_parametric() {
    // aggregates.zig:487
    let vals = param_f64_vals();
    for &n in &PARAMETRIC_F64_SIZES {
        approx(batch_agg_min(&vals[..n], f64::INFINITY), 1.0);
    }
}

#[test]
fn batch_agg_max_parametric() {
    // aggregates.zig:494
    let vals = param_f64_vals();
    for &n in &PARAMETRIC_F64_SIZES {
        approx(batch_agg_max(&vals[..n], f64::NEG_INFINITY), n as f64);
    }
}

#[test]
fn masked_agg_sum_parametric() {
    // aggregates.zig:502
    let vals = param_f64_vals();
    let type_ids = param_type_ids();
    for &n in &PARAMETRIC_MASKED_SIZES {
        let expected: f64 = vals[..n].iter().step_by(2).sum();
        approx(masked_agg_sum(&vals[..n], &type_ids[..n], 1), expected);
    }
}

#[test]
fn masked_agg_count_parametric() {
    // aggregates.zig:516
    let type_ids = param_type_ids();
    for &n in &PARAMETRIC_MASKED_SIZES {
        assert_eq!(masked_agg_count(&type_ids[..n], 1, n), n.div_ceil(2) as u32);
    }
}

#[test]
fn masked_agg_min_max_type_filtered() {
    // Not a Zig block — masked min/max lacked direct Zig coverage; pinned
    // here against hand-computed filtered extrema.
    let vals = [5.0, -3.0, 8.0, 1.0, 9.0, -7.0];
    let ids = [1u32, 2, 1, 2, 1, 2];
    approx(masked_agg_min(&vals, &ids, 1, f64::INFINITY), 5.0);
    approx(masked_agg_max(&vals, &ids, 1, f64::NEG_INFINITY), 9.0);
    approx(masked_agg_min(&vals, &ids, 2, f64::INFINITY), -7.0);
    approx(masked_agg_max(&vals, &ids, 2, f64::NEG_INFINITY), 1.0);
}

#[test]
fn reduce_col_i64_sum_parametric() {
    // aggregates.zig:525
    let vals = param_i64_vals();
    for &n in &PARAMETRIC_I64_SIZES {
        let n_i64 = n as i64;
        let expected = n_i64 * (n_i64 + 1) / 2;
        assert_eq!(
            reduce_col_i64(AggKind::Sum, &vals[..n], 0, None, None),
            expected
        );
    }
}

#[test]
fn reduce_col_f64_sum_with_predicate_batch_32() {
    // aggregates.zig:534
    let vals = param_f64_vals();
    let pred = param_pred_col();
    let expected: f64 = vals[..32].iter().step_by(3).sum();
    approx(
        reduce_col_f64(AggKind::Sum, &vals[..32], 0.0, None, Some(&pred[..32])),
        expected,
    );
}

// ---------------------------------------------------------------------------
// FP-probe bit pins (scratchpad fp-probe/probe.zig, zig 0.16.0, Debug ==
// ReleaseSmall on aarch64). These are EXACT: they pin lane order and
// min/max NaN/tie semantics, the part `approx` can't see.
// ---------------------------------------------------------------------------

#[test]
fn probe_pin_batch_sum_lane_order() {
    // batch_sum_8 == +0.0: lanes {1e16+1, 1+1e16, -1e16+1, 1-1e16} fold
    // sequentially to 0.0 — pairwise folding would give a different result.
    let d8 = [1e16, 1.0, -1e16, 1.0, 1.0, 1e16, 1.0, -1e16];
    assert_eq!(batch_agg_sum(&d8).to_bits(), 0x0);
    // batch_sum_7 == 2.75 (0x4006000000000000): one vector chunk + 3 tail adds.
    let d7 = [1e16, 1.0, -1e16, 1.0, 1.0, 0.25, 0.5];
    assert_eq!(batch_agg_sum(&d7).to_bits(), 0x4006000000000000);
    // batch_sum_9 == 4.5 (0x4012000000000000): two chunks + 1 tail.
    let d9 = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9];
    assert_eq!(batch_agg_sum(&d9).to_bits(), 0x4012000000000000);
}

#[test]
fn probe_pin_min_nan_and_signed_zero() {
    // batch_min_nan_data == 2.0: NaN lanes yield the other operand.
    let dn = [5.0, f64::NAN, 2.0, 8.0, f64::NAN];
    assert_eq!(
        batch_agg_min(&dn, f64::INFINITY).to_bits(),
        2.0f64.to_bits()
    );
    // batch_min_nan_current == 1.0: a NaN current_min is displaced.
    let dc = [5.0, 2.0, 8.0, 1.0];
    assert_eq!(batch_agg_min(&dc, f64::NAN).to_bits(), 1.0f64.to_bits());
    // all-NaN reduces to NaN (canonical quiet bits preserved from input).
    let all_nan = [f64::NAN; 4];
    assert!(batch_agg_min(&all_nan, f64::NAN).is_nan());
    // Signed-zero tie order: min(-0,+0)=+0, min(+0,-0)=-0 (second operand).
    let tie1 = [-0.0, 0.0, 0.0, 0.0];
    assert_eq!(batch_agg_min(&tie1, 0.0).to_bits(), 0x0);
    let tie2 = [0.0f64; 4];
    assert_eq!(batch_agg_min(&tie2, -0.0).to_bits(), 0x0); // lanes start -0, see +0 → +0
    assert_eq!(batch_agg_max(&tie2, -0.0).to_bits(), 0x0);
}

// ---------------------------------------------------------------------------
// Proptests: reference-model differential + scalar-path equivalences
// ---------------------------------------------------------------------------

/// Independent reimplementation of the probed Zig lane semantics, written
/// differently (index math instead of lane iterators) as the differential
/// reference.
fn ref_lane_sum(vals: &[f64]) -> f64 {
    let chunks = vals.len() / 4;
    let mut l = [0.0f64; 4];
    for c in 0..chunks {
        for k in 0..4 {
            l[k] += vals[c * 4 + k];
        }
    }
    let mut r = ((l[0] + l[1]) + l[2]) + l[3];
    for &v in &vals[chunks * 4..] {
        r += v;
    }
    r
}

fn adversarial_f64() -> impl Strategy<Value = f64> {
    prop_oneof![
        prop::num::f64::NORMAL,
        prop::num::f64::SUBNORMAL,
        Just(0.0),
        Just(-0.0),
        Just(f64::INFINITY),
        Just(f64::NEG_INFINITY),
        Just(f64::NAN),
        Just(1e16),
        Just(-1e16),
        Just(1.0),
    ]
}

proptest! {
    /// batch_agg_sum equals the reference lane model bit-for-bit, including
    /// NaN/inf inputs (NaN payload canonicalization is the caller's concern;
    /// equal bits here means equal-or-both-NaN).
    #[test]
    fn sum_matches_reference_lane_model(vals in prop::collection::vec(adversarial_f64(), 0..64)) {
        let a = batch_agg_sum(&vals);
        let b = ref_lane_sum(&vals);
        prop_assert!(a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan()));
    }

    /// min/max: against a sequential scalar fold with the probed zig min
    /// semantics — the lane split must not change the result for min/max
    /// (unlike sum), because min/max with the "NaN yields the other operand,
    /// ties yield the second operand" rule is associative-in-value up to
    /// signed-zero ties, which the lane order happens to preserve for the
    /// all-lanes-start-at-current shape. Bits compared exactly.
    #[test]
    fn min_matches_scalar_fold_on_finite(vals in prop::collection::vec(prop::num::f64::NORMAL, 0..64), current in prop::num::f64::NORMAL) {
        let kernel = batch_agg_min(&vals, current);
        let mut acc = current;
        for &v in &vals { if v < acc { acc = v; } }
        prop_assert_eq!(kernel.to_bits(), acc.to_bits());
    }

    /// i64 sum: batch (2-lane wrapping) vs naive sequential wrapping fold —
    /// exact equivalence for all inputs, wrapping arithmetic is associative.
    #[test]
    fn i64_sum_batch_vs_sequential(vals in prop::collection::vec(any::<i64>(), 0..64), current in any::<i64>()) {
        let batch = reduce_col_i64(AggKind::Sum, &vals, current, None, None);
        let mut acc = current;
        for &v in &vals { acc = acc.wrapping_add(v); }
        prop_assert_eq!(batch, acc);
    }

    /// Masked sum == unmasked sum over the filtered-and-REPACKED rows is NOT
    /// an identity (lane assignment differs); the honest property is against
    /// the masked reference model.
    #[test]
    fn masked_sum_matches_reference(
        vals in prop::collection::vec(adversarial_f64(), 0..64),
        ids in prop::collection::vec(0u32..3, 0..64),
    ) {
        let n = vals.len().min(ids.len());
        let (vals, ids) = (&vals[..n], &ids[..n]);
        // reference: same lane structure, mask applied at contribution time
        let chunks = n / 4;
        let mut l = [0.0f64; 4];
        for c in 0..chunks {
            for k in 0..4 {
                l[k] += if ids[c * 4 + k] == 1 { vals[c * 4 + k] } else { 0.0 };
            }
        }
        let mut expected = ((l[0] + l[1]) + l[2]) + l[3];
        for i in chunks * 4..n {
            if ids[i] == 1 { expected += vals[i]; }
        }
        let got = masked_agg_sum(vals, ids, 1);
        prop_assert!(got.to_bits() == expected.to_bits() || (got.is_nan() && expected.is_nan()));
    }

    /// Predicated f64 sum (scalar path) vs filter-then-plain-fold.
    #[test]
    fn predicated_sum_matches_filtered_fold(
        vals in prop::collection::vec(prop::num::f64::NORMAL, 0..64),
        pred in prop::collection::vec(0u32..2, 0..64),
    ) {
        let n = vals.len().min(pred.len());
        let got = reduce_col_f64(AggKind::Sum, &vals[..n], 0.0, None, Some(&pred[..n]));
        let mut acc = 0.0;
        for i in 0..n { if pred[i] != 0 { acc += vals[i]; } }
        prop_assert_eq!(got.to_bits(), acc.to_bits());
    }
}

// init_agg_slot's sentinel writes are pinned by state_init tests already;
// re-pin the MIN/MAX/I64 sentinels here against the kernel identities.
#[test]
fn init_sentinels_are_kernel_identities() {
    let mut buf = [0u8; 16];
    init_agg_slot(&mut buf, 0, 3); // MIN → +inf
    assert_eq!(agg_value_f64(&buf, 0), f64::INFINITY);
    init_agg_slot(&mut buf, 0, 4); // MAX → -inf
    assert_eq!(agg_value_f64(&buf, 0), f64::NEG_INFINITY);
    init_agg_slot(&mut buf, 0, 12); // MIN_I64
    assert_eq!(agg_value_i64(&buf, 0), i64::MAX);
    init_agg_slot(&mut buf, 0, 13); // MAX_I64
    assert_eq!(agg_value_i64(&buf, 0), i64::MIN);
    // TypeMask is exercised in kernels; silence unused-import pedantry by
    // exercising the masked scalar path with a mask + predicate together.
    let vals = [1.0, 2.0, 3.0, 4.0, 5.0];
    let ids = [1u32, 1, 2, 1, 1];
    let pred = [1u32, 0, 1, 1, 1];
    let got = reduce_col_f64(
        AggKind::Sum,
        &vals,
        0.0,
        Some(TypeMask { data: &ids, id: 1 }),
        Some(&pred),
    );
    approx(got, 1.0 + 4.0 + 5.0);
}
