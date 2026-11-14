//! Coverage for aggregate kernels, slot layout, deterministic FP probes, and
//! property tests.
//!
//! Approximate assertions use a 0.001 tolerance. Probe tests assert exact bit
//! patterns because lane order and NaN/tie behavior are part of the FP profile.

use columine_types::PROGRAM_MAGIC;
use columine_types::types::{AggType, ErrorCode, Opcode};
use columine_vm::aggregates::{
    AggKind, TypeMask, agg_count, agg_set_count, agg_set_value_f64, agg_set_value_i64,
    agg_slot_byte_size, agg_value_f64, agg_value_i64, batch_agg_max, batch_agg_min, batch_agg_sum,
    init_agg_slot, masked_agg_count, masked_agg_max, masked_agg_min, masked_agg_sum,
    reduce_col_f64, reduce_col_i64,
};
use columine_vm::meta::SlotMetaView;
use columine_vm::state_init::{DEFAULT_ACCEPTED_PROGRAM_MAGICS, calculate_state_size, init_state};
use columine_vm::temporal::TEMPORAL_MAGIC;
use columine_vm::vm::{Vm, i64s_as_bytes, u32s_as_bytes};
use proptest::prelude::*;

fn approx(a: f64, b: f64) {
    assert!((a - b).abs() < 0.001, "expected ~{b}, got {a}");
}

// ---------------------------------------------------------------------------
//  test blocks 1-6: kernel basics
// ---------------------------------------------------------------------------

#[test]
fn batch_agg_sum_f64_fixed_lane_order() {
    //
    let data = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0];
    approx(batch_agg_sum(&data[..7]), 28.0);
    approx(batch_agg_sum(&data[..4]), 10.0);
}

#[test]
fn batch_agg_min_max_f64_profile() {
    //
    let data = [5.0, 2.0, 8.0, 1.0, 7.0];
    approx(batch_agg_min(&data, f64::INFINITY), 1.0);
    approx(batch_agg_max(&data, f64::NEG_INFINITY), 8.0);
}

#[test]
fn masked_agg_sum_type_filtered() {
    //
    let vals = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0];
    let types_col = [1u32, 2, 1, 2, 1, 2]; // type=1 rows: 10+30+50=90
    approx(masked_agg_sum(&vals, &types_col, 1), 90.0);
    approx(masked_agg_sum(&vals, &types_col, 2), 120.0);
}

#[test]
fn masked_agg_count_simd() {
    //
    let types_col = [1u32, 2, 1, 1, 2, 1, 2, 1];
    assert_eq!(masked_agg_count(&types_col, 1), 5);
    assert_eq!(masked_agg_count(&types_col, 2), 3);
}

#[test]
fn reduce_col_i64_sum_simd() {
    //
    let data = [100i64, 200, 300, 400, 500];
    assert_eq!(reduce_col_i64(AggKind::Sum, &data, 0, None), Some(1500));
}

#[test]
fn reduce_col_f64_with_predicate() {
    //
    let vals = [10.0, 20.0, 30.0, 40.0];
    let pred = [1u32, 0, 1, 0]; // only indices 0 and 2
    approx(
        reduce_col_f64(AggKind::Sum, &vals, 0.0, None, Some(&pred)),
        40.0,
    );
}

// ---------------------------------------------------------------------------
//  test blocks 7-9: AggSlot layout
// ---------------------------------------------------------------------------

const AGG_COUNT: u8 = AggType::Count as u8;
const AGG_SUM: u8 = AggType::Sum as u8;
const AGG_SUM_I64: u8 = AggType::SumI64 as u8;

#[test]
fn agg_slot_count_layout() {
    //  — COUNT: 8 bytes, count at offset 0.
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
    //  — SUM: 16 bytes, value at 0, count at 8.
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
    //
    let mut buf = [0u8; 16];
    agg_set_value_i64(&mut buf, 0, 999_999_999_999);
    agg_set_count(&mut buf, 0, AGG_SUM_I64, 1);
    assert_eq!(agg_value_i64(&buf, 0), 999_999_999_999);
    assert_eq!(agg_count(&buf, 0, AGG_SUM_I64), 1);
}

// ---------------------------------------------------------------------------
//  test blocks 10-16: parametric batch sizes
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

/// Alternating type_ids: even indices = 1, odd = 2 ().
fn param_type_ids() -> [u32; MAX_PARAM_LEN] {
    core::array::from_fn(|i| if i % 2 == 0 { 1 } else { 2 })
}

/// Every 3rd element passes the predicate ().
fn param_pred_col() -> [u32; MAX_PARAM_LEN] {
    core::array::from_fn(|i| u32::from(i % 3 == 0))
}

#[test]
fn batch_agg_sum_parametric() {
    //
    let vals = param_f64_vals();
    for &n in &PARAMETRIC_F64_SIZES {
        let expected = (n as f64) * ((n + 1) as f64) / 2.0;
        approx(batch_agg_sum(&vals[..n]), expected);
    }
}

#[test]
fn batch_agg_min_parametric() {
    //
    let vals = param_f64_vals();
    for &n in &PARAMETRIC_F64_SIZES {
        approx(batch_agg_min(&vals[..n], f64::INFINITY), 1.0);
    }
}

#[test]
fn batch_agg_max_parametric() {
    //
    let vals = param_f64_vals();
    for &n in &PARAMETRIC_F64_SIZES {
        approx(batch_agg_max(&vals[..n], f64::NEG_INFINITY), n as f64);
    }
}

#[test]
fn masked_agg_sum_parametric() {
    //
    let vals = param_f64_vals();
    let type_ids = param_type_ids();
    for &n in &PARAMETRIC_MASKED_SIZES {
        let expected: f64 = vals[..n].iter().step_by(2).sum();
        approx(masked_agg_sum(&vals[..n], &type_ids[..n], 1), expected);
    }
}

#[test]
fn masked_agg_count_parametric() {
    //
    let type_ids = param_type_ids();
    for &n in &PARAMETRIC_MASKED_SIZES {
        assert_eq!(masked_agg_count(&type_ids[..n], 1), n.div_ceil(2) as u32);
    }
}

#[test]
fn masked_agg_min_max_type_filtered() {
    // Masked min/max are checked against hand-computed filtered extrema.
    let vals = [5.0, -3.0, 8.0, 1.0, 9.0, -7.0];
    let ids = [1u32, 2, 1, 2, 1, 2];
    approx(masked_agg_min(&vals, &ids, 1, f64::INFINITY), 5.0);
    approx(masked_agg_max(&vals, &ids, 1, f64::NEG_INFINITY), 9.0);
    approx(masked_agg_min(&vals, &ids, 2, f64::INFINITY), -7.0);
    approx(masked_agg_max(&vals, &ids, 2, f64::NEG_INFINITY), 1.0);
}

#[test]
fn reduce_col_i64_sum_parametric() {
    //
    let vals = param_i64_vals();
    for &n in &PARAMETRIC_I64_SIZES {
        let n_i64 = n as i64;
        let expected = n_i64 * (n_i64 + 1) / 2;
        assert_eq!(
            reduce_col_i64(AggKind::Sum, &vals[..n], 0, None),
            Some(expected)
        );
    }
}

#[test]
fn reduce_col_f64_sum_with_predicate_batch_32() {
    //
    let vals = param_f64_vals();
    let pred = param_pred_col();
    let expected: f64 = vals[..32].iter().step_by(3).sum();
    approx(
        reduce_col_f64(AggKind::Sum, &vals[..32], 0.0, None, Some(&pred[..32])),
        expected,
    );
}

// ---------------------------------------------------------------------------
// FP-probe bit pins. These exact values pin lane order and min/max NaN/tie
// semantics that approximate comparisons cannot observe.
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

/// Independent scalar oracle. Inputs are bounded integral f64 values so every
/// intermediate sum is exact and association cannot hide a wrong kernel.
fn ref_sequential_sum(vals: &[f64]) -> f64 {
    vals.iter().copied().fold(0.0, |sum, value| sum + value)
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
    /// The numeric result agrees with an independent left-to-right fold on a
    /// domain where addition is exact. Cancellation-sensitive lane order is
    /// pinned separately by `probe_pin_batch_sum_lane_order`.
    #[test]
    fn sum_matches_sequential_oracle(
        vals in prop::collection::vec(-1_000_000i32..1_000_001, 0..64)
    ) {
        let vals: Vec<f64> = vals.into_iter().map(f64::from).collect();
        prop_assert_eq!(batch_agg_sum(&vals).to_bits(), ref_sequential_sum(&vals).to_bits());
    }

    /// min/max: against a sequential scalar fold with the probed reference min
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

//#region smoo/columine!n/wide-i64-sum-refuses
// An i64 sum through the VM's execute entry: the three places a
// `BATCH_AGG_SUM_I64` runs (the top level, a `FOR_EACH` type mask, a
// row at a time under a temporal table), each cut into batches the way a host
// cuts them.

/// Columns: 0 type, 1 value, 2 the temporal table's clock.
const TYPE_COL: u8 = 0;
const VAL_COL: u8 = 1;
const CLOCK_COL: u8 = 2;
const ORDER_TYPE: u32 = 7;
const OK: u32 = ErrorCode::Ok as u32;
const SUM_OVERFLOW: u32 = ErrorCode::SumOverflow as u32;

#[derive(Clone, Copy, Debug)]
enum SumSite {
    TopLevel,
    ForEach,
    RowAtATime,
}

const SITES: [SumSite; 3] = [SumSite::TopLevel, SumSite::ForEach, SumSite::RowAtATime];

fn for_each(body: &[u8]) -> Vec<u8> {
    let mut reduce = vec![Opcode::ForEach as u8, TYPE_COL, 1];
    reduce.extend(ORDER_TYPE.to_le_bytes());
    reduce.extend(u16::try_from(body.len()).expect("small body").to_le_bytes());
    reduce.extend_from_slice(body);
    reduce.push(0);
    reduce
}

/// Slot 0 is an i64 sum of the value column, run at `site`.
fn sum_program(site: SumSite) -> Vec<u8> {
    let init = [0x10, 0, 2, AggType::SumI64 as u8, 0, 0];
    let sum = [Opcode::BatchAggSumI64 as u8, 0, VAL_COL];
    let reduce = match site {
        SumSite::TopLevel | SumSite::RowAtATime => [&sum[..], &[0]].concat(),
        SumSite::ForEach => for_each(&sum),
    };
    let mut prog = vec![0u8; 32];
    prog.extend(PROGRAM_MAGIC.to_le_bytes());
    prog.extend([2, 0, 0, 0]); // version 2, no callbacks, no flags
    prog.extend(1u32.to_le_bytes());
    prog.extend(3u32.to_le_bytes());
    prog.extend(u32::try_from(init.len()).expect("small init").to_le_bytes());
    prog.extend(
        u32::try_from(reduce.len())
            .expect("small reduce")
            .to_le_bytes(),
    );
    prog.extend(init);
    prog.extend(reduce);
    if let SumSite::RowAtATime = site {
        // A temporal table of no expiries: the reduce section runs one row at a time.
        prog.extend(TEMPORAL_MAGIC.to_le_bytes());
        prog.extend(8u32.to_le_bytes());
        prog.extend(u32::from(CLOCK_COL).to_le_bytes());
        prog.extend(0u32.to_le_bytes());
    }
    prog
}

/// The host's view of a sum: each batch of `cut` sizes runs under an undo
/// checkpoint, and a refused batch is rolled back, as the wasm host does.
/// Answers the first refusal (its status and the slot the VM named), and the
/// slot's total after it.
struct Folded {
    refusal: Option<(u32, Option<u32>)>,
    total: i64,
}

fn host_fold(site: SumSite, vals: &[i64], cut: usize) -> Folded {
    let prog = sum_program(site);
    let size = calculate_state_size(&prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS)
        .expect("valid program admission");
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, &prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("init_state");
    let mut vm = Vm::new(DEFAULT_ACCEPTED_PROGRAM_MAGICS);
    vm.undo_enable(&state).expect("undo window");
    let offset = SlotMetaView::read(&state, 0).offset;
    let mut refusal = None;
    for batch in vals.chunks(cut.max(1)) {
        // A `FOR_EACH` batch interleaves a row of another type holding
        // `i64::MAX`: the mask must keep it out of the sum.
        let (types, values): (Vec<u32>, Vec<i64>) = match site {
            SumSite::ForEach => batch
                .iter()
                .flat_map(|&v| [(ORDER_TYPE, v), (ORDER_TYPE + 1, i64::MAX)])
                .unzip(),
            SumSite::TopLevel | SumSite::RowAtATime => {
                (vec![ORDER_TYPE; batch.len()], batch.to_vec())
            }
        };
        let rows = u32::try_from(types.len()).expect("small batch");
        let clocks: Vec<i64> = (0..i64::from(rows)).collect();
        let cols: [&[u8]; 3] = [
            u32s_as_bytes(&types),
            i64s_as_bytes(&values),
            i64s_as_bytes(&clocks),
        ];
        let checkpoint = vm.undo_checkpoint();
        let status = vm.execute_batch(&mut state, &prog, &cols, rows);
        if status != OK {
            vm.undo_rollback(&mut state, checkpoint).expect("rollback");
            refusal = Some((status, vm.sum_overflow_slot()));
            break;
        }
    }
    Folded {
        refusal,
        total: agg_value_i64(&state, offset),
    }
}

#[test]
fn an_overflowing_i64_sum_is_refused() {
    for site in SITES {
        for (rows, kept) in [([i64::MAX, 1], i64::MAX), ([i64::MIN, -1], i64::MIN)] {
            let one_batch = host_fold(site, &rows, 2);
            assert_eq!(
                one_batch.refusal,
                Some((SUM_OVERFLOW, Some(0))),
                "{site:?} {rows:?} as one batch"
            );
            assert_eq!(one_batch.total, 0, "{site:?} {rows:?} as one batch");
            let per_row = host_fold(site, &rows, 1);
            assert_eq!(
                per_row.refusal,
                Some((SUM_OVERFLOW, Some(0))),
                "{site:?} {rows:?} per row"
            );
            assert_eq!(per_row.total, kept, "{site:?} {rows:?} per row");
        }
    }
}

#[test]
fn an_in_range_i64_sum_is_exact() {
    for site in SITES {
        for cut in [1, 3] {
            let folded = host_fold(site, &[i64::MAX - 1, 1, -5, 5], cut);
            assert_eq!(folded.refusal, None, "{site:?} cut {cut}");
            assert_eq!(folded.total, i64::MAX, "{site:?} cut {cut}");
        }
    }
}

/// Any rows and any cut: the sum is the exact total when no running total
/// leaves `i64`, and otherwise the batch holding the first row that leaves
/// it is refused, the slot keeping the total of the batches before it.
fn i64_row() -> impl Strategy<Value = i64> {
    prop_oneof![
        any::<i64>(),
        -1_000i64..1_000,
        Just(i64::MAX),
        Just(i64::MIN),
        (i64::MAX - 8)..=i64::MAX,
        i64::MIN..=(i64::MIN + 8),
    ]
}

proptest! {
    #[test]
    fn an_i64_sum_is_exact_or_refused_under_any_cut(
        vals in prop::collection::vec(i64_row(), 0..12),
        cut in 1usize..6,
    ) {
        let mut total = 0i128;
        let mut first_out = None;
        for (row, &v) in vals.iter().enumerate() {
            total += i128::from(v);
            if i64::try_from(total).is_err() {
                first_out = Some(row);
                break;
            }
        }
        let kept: i128 = match first_out {
            Some(row) => vals[..row - row % cut].iter().copied().map(i128::from).sum(),
            None => total,
        };
        for site in SITES {
            let folded = host_fold(site, &vals, cut);
            let refusal = first_out.map(|_| (SUM_OVERFLOW, Some(0)));
            prop_assert_eq!(folded.refusal, refusal, "{:?}", site);
            prop_assert_eq!(i128::from(folded.total), kept, "{:?}", site);
        }
    }
}
//#endregion smoo/columine!n/wide-i64-sum-refuses
