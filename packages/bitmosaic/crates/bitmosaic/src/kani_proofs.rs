//! Private Kani bounded-model-checking proof harnesses (`#[cfg(kani)]`).
//!
//! Covers:
//! - `codec::{select64, stride_quotient, stride_quotient_ceil}`
//! - `maplet`: bounded saturation zero-false-negative invariant and branchless
//!   `or_val` bit-width equivalence with `max_val`
//! - `gallop`, `gallop_u32_by`, `gallop_u64_slice`: memory safety and exact
//!   `partition_point` + exact-match equivalence
//! - `V2View`: zero-panic admission on arbitrary small byte slices

use core::ops::ControlFlow;

use crate::codec::{select64, stride_magic, stride_quotient, stride_quotient_ceil};
use crate::{Maplet, MapletMatch, Probes, V2View, gallop, gallop_u32_by, gallop_u64_slice};

#[kani::proof]
fn verify_select64_exact() {
    let word: u64 = kani::any();
    let rank: u32 = kani::any();
    kani::assume(rank < word.count_ones());

    let pos = select64(word, rank);
    assert!(pos < 64);
    assert_eq!((word >> pos) & 1, 1);
    let below_mask = (1u64 << pos) - 1;
    assert_eq!((word & below_mask).count_ones(), rank);
}

#[kani::proof]
fn verify_stride_quotient_lut_exact() {
    let stride: u16 = kani::any();
    kani::assume(stride >= 1 && stride <= 255);
    let delta: u32 = kani::any();
    kani::assume(delta <= 65_535);

    let s = u32::from(stride);
    let magic = stride_magic(stride);
    let q = stride_quotient(delta, magic);
    // Exact division identity without introducing symbolic division into SAT:
    // q == floor(delta / s)  <=>  q * s <= delta < (q + 1) * s.
    assert!(u64::from(q) * u64::from(s) <= u64::from(delta));
    assert!(u64::from(delta) < u64::from(q + 1) * u64::from(s));
}

#[kani::proof]
fn verify_stride_quotient_ceil_lut_exact() {
    let stride: u16 = kani::any();
    kani::assume(stride >= 1 && stride <= 255);
    let delta: u32 = kani::any();
    kani::assume(delta <= 65_536);

    let s = u32::from(stride);
    let magic = stride_magic(stride);
    let q = stride_quotient_ceil(delta, magic);
    // Exact ceil-division identity:
    // delta == 0 => q == 0; delta > 0 => (q - 1) * s < delta <= q * s.
    if delta == 0 {
        assert_eq!(q, 0);
    } else {
        assert!(q >= 1);
        assert!(u64::from(q - 1) * u64::from(s) < u64::from(delta));
        assert!(u64::from(delta) <= u64::from(q) * u64::from(s));
    }
}

#[kani::proof]
fn verify_stride_quotient_large_edges_exact() {
    const LARGE_STRIDES: [u16; 8] = [256, 257, 511, 512, 4096, 32_768, 65_534, 65_535];
    let idx: usize = kani::any();
    kani::assume(idx < LARGE_STRIDES.len());
    let stride = LARGE_STRIDES[idx];
    let s = u32::from(stride);
    let magic = stride_magic(stride);

    let delta: u32 = kani::any();
    kani::assume(delta <= 65_535);
    let q = stride_quotient(delta, magic);
    assert!(u64::from(q) * u64::from(s) <= u64::from(delta));
    assert!(u64::from(delta) < u64::from(q + 1) * u64::from(s));

    let delta_ceil: u32 = kani::any();
    kani::assume(delta_ceil <= 65_536);
    let qc = stride_quotient_ceil(delta_ceil, magic);
    if delta_ceil == 0 {
        assert_eq!(qc, 0);
    } else {
        assert!(qc >= 1);
        assert!(u64::from(qc - 1) * u64::from(s) < u64::from(delta_ceil));
        assert!(u64::from(delta_ceil) <= u64::from(qc) * u64::from(s));
    }
}

#[kani::proof]
fn verify_maplet_or_val_equiv_max_val() {
    let vals: [u16; 8] = kani::any();
    let mut max_val = 0u16;
    let mut or_val = 0u16;
    for v in vals {
        if v > max_val {
            max_val = v;
        }
        or_val |= v;
    }
    assert_eq!((or_val >> 8) != 0, max_val > u8::MAX as u16);
}

#[kani::proof]
#[kani::unwind(20)]
fn verify_maplet_bounded_no_false_negatives() {
    let mut m = Maplet::with_capacity(1);
    let k0: u8 = kani::any();
    let v0: u16 = kani::any();
    let k1: u8 = kani::any();
    let v1: u16 = kani::any();

    let (k0, k1) = (u32::from(k0), u32::from(k1));
    let _ = m.insert_bounded(k0, v0);
    let _ = m.insert_bounded(k1, v1);

    // Every inserted (key, val) must either be yielded by `for_each_match_bounded`
    // or report `MapletMatch::Saturated` (zero false negatives).
    for (k, expected_v) in [(k0, v0), (k1, v1)] {
        let mut found = false;
        let outcome = m.for_each_match_bounded(k, |v| {
            if v == expected_v {
                found = true;
            }
            ControlFlow::Continue(())
        });
        match outcome {
            MapletMatch::Saturated => {
                assert!(m.is_key_saturated(k));
            }
            MapletMatch::Complete => {
                assert!(found);
            }
            MapletMatch::Break(()) => unreachable!(),
        }
    }
}

#[kani::proof]
#[kani::unwind(10)]
fn verify_gallop_u16_matches_partition_point() {
    let arr: [u16; 8] = kani::any();
    let len: usize = kani::any();
    kani::assume(len <= arr.len());
    for i in 1..len {
        kani::assume(arr[i - 1] < arr[i]);
    }
    let from: usize = kani::any();
    kani::assume(from <= len);
    let target: u16 = kani::any();

    let mut tally = Probes::zero();
    let res = gallop(len, from, target, |i| arr[i], &mut tally);
    let expected = from + arr[from..len].partition_point(|&v| v < target);

    match res {
        Ok(idx) => {
            assert_eq!(idx, expected);
            assert!(idx < len);
            assert_eq!(arr[idx], target);
        }
        Err(idx) => {
            assert_eq!(idx, expected);
            assert!(idx == len || arr[idx] > target);
        }
    }
}

#[kani::proof]
#[kani::unwind(8)]
fn verify_gallop_u32_and_u64_match_partition_point() {
    let arr32: [u32; 6] = kani::any();
    let len: usize = kani::any();
    kani::assume(len <= arr32.len());
    for i in 1..len {
        kani::assume(arr32[i - 1] < arr32[i]);
    }
    let from: usize = kani::any();
    kani::assume(from <= len);
    let target32: u32 = kani::any();

    let res32 = gallop_u32_by(len, from, target32, |i| arr32[i]);
    let exp32 = from + arr32[from..len].partition_point(|&v| v < target32);
    match res32 {
        Ok(idx) => {
            assert_eq!(idx, exp32);
            assert!(idx < len && arr32[idx] == target32);
        }
        Err(idx) => {
            assert_eq!(idx, exp32);
            assert!(idx == len || arr32[idx] > target32);
        }
    }

    let arr64: [u64; 6] = arr32.map(u64::from);
    let target64 = u64::from(target32);
    let res64 = gallop_u64_slice(&arr64[..len], from, target64);
    assert_eq!(res64, res32);
}

#[kani::proof]
#[kani::unwind(8)]
fn verify_v2_view_zero_panic_admission() {
    let buf: [u8; 4] = kani::any();
    let len: usize = kani::any();
    kani::assume(len <= buf.len());
    let slice = &buf[..len];

    let _ = V2View::open(slice);
}

#[kani::proof]
#[kani::unwind(8)]
fn verify_image_header_v2_zero_panic() {
    let buf: [u8; 4] = kani::any();
    let len: usize = kani::any();
    kani::assume(len <= buf.len());
    let slice = &buf[..len];

    let _ = crate::image_header_v2(slice);
}
