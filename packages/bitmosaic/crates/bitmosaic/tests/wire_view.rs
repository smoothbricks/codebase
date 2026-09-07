//! Differential oracle for the wire format and the borrowed view.
//!
//! Two properties are asserted here that the in-crate ladder tests cannot:
//!
//! 1. **Agreement.** For every shape, `BitmosaicView` over the canonical bytes
//!    answers exactly what the owned `Bitmosaic` and a `BTreeSet` answer, on
//!    `contains` / `rank` / `select` / `and_len` / ascending walk. The view
//!    is checked against BOTH, so an encoder bug that also broke the owned
//!    forest could not hide behind a single reference.
//! 2. **No allocation on the read path.** A counting global allocator brackets
//!    the reads. This is a claim about zero, so the counter is proved capable
//!    of reporting non-zero in the same test — an instrument that cannot see
//!    a violation cannot certify its absence.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::BTreeSet;

use bitmosaic::{
    AndNotRange, AndRange, Bitmosaic, Bitmosaic64, Bitmosaic64View, BitmosaicView, OrRange, Range,
    XorRange,
};

#[test]
fn attached_runs_queries_cover_all_count_varint_widths() {
    fn push_uleb(out: &mut Vec<u8>, mut value: usize) {
        loop {
            let low = (value & 127) as u8;
            value >>= 7;
            out.push(low | if value == 0 { 0 } else { 128 });
            if value == 0 {
                break;
            }
        }
    }
    for count in [1usize, 2, 3, 128, 129, 16_384, 16_385, 32_768] {
        let mut body = Vec::new();
        push_uleb(&mut body, count - 1);
        body.extend_from_slice(&[0, 0, 0, 4]); // one chunk, key zero, Runs
        push_uleb(&mut body, count - 1);
        for i in 0..count {
            let value = (2 * i) as u16;
            body.extend_from_slice(&value.to_le_bytes());
            body.extend_from_slice(&value.to_le_bytes());
        }
        for i in 1..count.saturating_sub(1) {
            body.extend_from_slice(&(i as u16).to_le_bytes());
        }
        let mut bytes = b"BMS\x08".to_vec();
        push_uleb(&mut bytes, body.len());
        bytes.extend_from_slice(&body);
        let view = BitmosaicView::open_verified(&bytes).expect("bounded Runs count fixture");
        assert_eq!(view.len(), count as u64);
        for rank in [0, count / 2, count - 1] {
            let value = (2 * rank) as u32;
            assert!(view.contains(value));
            assert!(!view.contains(value + 1));
            assert_eq!(view.rank(value), rank as u64);
            assert_eq!(view.select(rank as u64), Some(value));
            let mut cursor = view.range();
            cursor.seek(value);
            assert_eq!(cursor.next(), Some(value));
        }
    }
}

#[test]
fn ordinary_attach_rejects_zero_stride_and_nonzero_ef_tail_bits() {
    let mut stride = Bitmosaic::from_sorted(0..100).to_forest_bytes();
    let end = stride.len();
    stride[end - 2..].fill(0);
    assert!(BitmosaicView::open(&stride).is_none());

    let mut ef =
        Bitmosaic::from_sorted((0..7u32).map(|i| 1 + i * (u32::MAX / 8) + i % 3)).to_bytes();
    assert!(BitmosaicView::open(&ef).unwrap().is_elias_fano());
    // Seven 28-bit low fields leave four unused bits before the sole zero sample.
    let last_low_byte = ef.len() - 5;
    ef[last_low_byte] |= 0x80;
    assert!(BitmosaicView::open(&ef).is_none());
}

#[test]
fn runs_validate_stored_and_implicit_cardinality_prefixes() {
    let bytes = Bitmosaic::from_sorted((0..4).chain(10..14).chain(20..24)).to_forest_bytes();
    assert_eq!(bytes.len(), 25);
    assert!(BitmosaicView::open_verified(&bytes).is_some());
    // One-byte framing/counts, then one key/kind: payload at 10.
    // Three bounds consume 12 bytes after the count; the one prefix is at 23.
    let mut bad_prefix = bytes.clone();
    bad_prefix[23..25].copy_from_slice(&5u16.to_le_bytes());
    assert!(BitmosaicView::open(&bad_prefix).is_none());
    let mut bad_total = bytes;
    bad_total[5] += 1;
    assert!(BitmosaicView::open(&bad_total).is_none());
}

#[test]
fn u64_directory_rejects_inconsistent_prefixes_offsets_and_child_bodies() {
    let bytes = Bitmosaic64::from_sorted([1, (1u64 << 32) | 3, (2u64 << 32) | 5]).to_bytes();
    assert_eq!(bytes.len(), 56);
    assert!(Bitmosaic64View::open_verified(&bytes).is_some());
    // Three children: keys at 6, interior prefix at 18, offsets at 26,
    // root bits at 34, tightly packed seven-byte singleton bodies at 35.
    for (at, value) in [(6, 2u8), (18, 2), (26, 8), (34, 0x80), (35, 1)] {
        let mut damaged = bytes.clone();
        damaged[at] = value;
        assert!(
            Bitmosaic64View::open(&damaged).is_none(),
            "corrupt plane at {at}"
        );
    }
}

#[test]
fn full_u32_cardinality_uses_widened_final_prefix() {
    fn varint(out: &mut Vec<u8>, mut value: u32) {
        loop {
            let byte = (value & 127) as u8;
            value >>= 7;
            out.push(byte | if value == 0 { 0 } else { 128 });
            if value == 0 {
                break;
            }
        }
    }
    let chunks = 65_536usize;
    let mut body = Vec::new();
    varint(&mut body, u32::MAX);
    varint(&mut body, u32::from(u16::MAX));
    for key in 0..chunks {
        body.extend_from_slice(&(key as u16).to_le_bytes());
    }
    for i in 1..chunks {
        body.extend_from_slice(&((i as u32) << 16).to_le_bytes());
    }
    for i in 1..chunks {
        body.extend_from_slice(&((i as u32) * 4).to_le_bytes());
    }
    let kinds_at = body.len();
    body.resize(kinds_at + (3 * chunks).div_ceil(8), 0);
    for i in 0..chunks {
        body[kinds_at + 3 * i / 8] |= 1 << (3 * i % 8);
    }
    for _ in 0..chunks {
        body.extend_from_slice(&[0, 0, 1, 0]);
    }
    let mut bytes = b"BMS\x08".to_vec();
    varint(&mut bytes, body.len() as u32);
    bytes.extend_from_slice(&body);
    let view = BitmosaicView::open_verified(&bytes).expect("full-domain arithmetic fixture");
    assert_eq!(view.len(), 1u64 << 32);
    assert_eq!(view.rank(u32::MAX), u64::from(u32::MAX));
    assert_eq!(view.select(u64::from(u32::MAX)), Some(u32::MAX));
    assert_eq!(view.select(1u64 << 32), None);
    assert!(view.contains(u32::MAX));
}

#[test]
fn runs_reject_overlapping_adjacent_reversed_and_inexact_payloads() {
    let bytes = Bitmosaic::from_sorted((0..4096).chain(32768..36864)).to_forest_bytes();
    assert_eq!(bytes.len(), 20);
    for (start, end) in [(4095u16, 8190u16), (4096, 8191), (40000, 36863)] {
        let mut damaged = bytes.clone();
        damaged[16..18].copy_from_slice(&start.to_le_bytes());
        damaged[18..20].copy_from_slice(&end.to_le_bytes());
        assert!(BitmosaicView::open(&damaged).is_none());
    }
    for kind in [5u8, 6, 7, 0x84] {
        let mut damaged = bytes.clone();
        damaged[10] = kind;
        assert!(BitmosaicView::open(&damaged).is_none());
    }
    let mut gap = bytes.clone();
    gap[4] += 1;
    gap.push(0);
    assert!(BitmosaicView::open(&gap).is_none());
    let mut noncanonical_count = bytes.clone();
    noncanonical_count[4] += 1;
    noncanonical_count[11] |= 128;
    noncanonical_count.insert(12, 0);
    assert!(BitmosaicView::open(&noncanonical_count).is_none());
}

#[test]
fn native_v1_compact_images_round_trip_without_padding() {
    for (values, expected_bytes) in [
        (Vec::new(), 4),
        (vec![7], 12),
        ((0..100).collect(), 14),
        ((0..4096).chain(32768..36864).collect(), 20),
    ] {
        let owned = Bitmosaic::from_sorted(values.iter().copied());
        let bytes = owned.to_bytes();
        assert_eq!(bytes.len(), expected_bytes);
        assert_eq!(&bytes[..3], b"BMS");
        for offset in 0..64 {
            let mut slot = vec![0xa5; offset];
            slot.extend_from_slice(&bytes);
            slot.extend_from_slice(&[0xa5; 16]);
            let view = BitmosaicView::open_verified(&slot[offset..]).unwrap();
            assert_eq!(view.range().collect::<Vec<_>>(), values);
            for (rank, &value) in values.iter().enumerate() {
                assert_eq!(view.rank(value), rank as u64);
                assert_eq!(view.select(rank as u64), Some(value));
            }
        }
    }
    for (values, expected_bytes) in [(vec![], 4), (vec![u64::MAX], 18)] {
        let bytes = Bitmosaic64::from_sorted(values.iter().copied()).to_bytes();
        assert_eq!(bytes.len(), expected_bytes);
        assert_eq!(
            Bitmosaic64View::open_verified(&bytes)
                .unwrap()
                .range()
                .collect::<Vec<_>>(),
            values
        );
    }
}

#[test]
fn all_five_container_pairs_agree_on_counts_and_materialized_algebra() {
    let shapes: Vec<Vec<u32>> = vec![
        (0..10_000).map(|i| i * 3).collect(),
        (0..512).map(|i| i * 73 + i % 3).collect(),
        (0..200).map(|i| i * i).collect(),
        (0..65_536u32)
            .filter(|v| v.wrapping_mul(0x9e37_79b9).rotate_left(7) & 3 != 0)
            .collect(),
        (0..4096).chain(32768..36864).collect(),
    ];
    let owned: Vec<_> = shapes
        .iter()
        .map(|values| Bitmosaic::from_sorted(values.iter().copied()))
        .collect();
    for (i, set) in owned.iter().enumerate() {
        let census = set.container_census();
        let counts = [census.0, census.1, census.2, census.3, census.4];
        assert_eq!(counts[i], 1, "fixture {i} must exercise its own container");
    }
    for (i, left) in owned.iter().enumerate() {
        for (j, right) in owned.iter().enumerate() {
            let a: BTreeSet<_> = shapes[i].iter().copied().collect();
            let b: BTreeSet<_> = shapes[j].iter().copied().collect();
            let ab = left.to_forest_bytes();
            let bb = right.to_forest_bytes();
            let av = BitmosaicView::open_verified(&ab).unwrap();
            let bv = BitmosaicView::open_verified(&bb).unwrap();
            let intersection: Vec<_> = a.intersection(&b).copied().collect();
            assert_eq!(
                left.and_len(right),
                intersection.len() as u64,
                "owned {i}/{j}"
            );
            assert_eq!(
                av.and_len(&bv),
                intersection.len() as u64,
                "borrowed {i}/{j}"
            );
            for (result, expected) in [
                (left.and(right), intersection),
                (left.or(right), a.union(&b).copied().collect()),
                (
                    left.xor(right),
                    a.symmetric_difference(&b).copied().collect(),
                ),
                (left.andnot(right), a.difference(&b).copied().collect()),
            ] {
                assert_eq!(result.iter().collect::<Vec<_>>(), expected, "pair {i}/{j}");
                assert_eq!(
                    result.to_bytes(),
                    Bitmosaic::from_sorted(expected).to_bytes(),
                    "canonical pair {i}/{j}"
                );
            }
            let wide_a =
                Bitmosaic64::from_sorted(shapes[i].iter().map(|v| (7u64 << 32) | u64::from(*v)));
            let wide_b =
                Bitmosaic64::from_sorted(shapes[j].iter().map(|v| (7u64 << 32) | u64::from(*v)));
            let wab = wide_a.to_bytes();
            let wbb = wide_b.to_bytes();
            assert_eq!(
                Bitmosaic64View::open_verified(&wab)
                    .unwrap()
                    .and_len(&Bitmosaic64View::open_verified(&wbb).unwrap()),
                a.intersection(&b).count() as u64,
            );
        }
    }
}

#[test]
fn native_header_refuses_noncanonical_framing_and_bounds_extent() {
    use bitmosaic::{EMPTY_U32_IMAGE, EMPTY_U64_IMAGE, KeyWidth, image_header};
    for (empty, width) in [
        (EMPTY_U32_IMAGE, KeyWidth::U32),
        (EMPTY_U64_IMAGE, KeyWidth::U64),
    ] {
        let mut slot = empty.to_vec();
        slot.extend_from_slice(&[0xa5; 32]);
        let header = image_header(&slot).unwrap();
        assert_eq!(header.width, width);
        assert_eq!(header.encoded_len, 4);
    }
    for control in 0..=u8::MAX {
        let mut image = vec![b'B', b'M', b'S', control, 1, 0];
        let valid = matches!(control, 8 | 9 | 10 | 12 | 13);
        assert_eq!(image_header(&image).is_some(), valid, "control {control}");
        image.truncate(3);
        assert!(image_header(&image).is_none());
    }
    for length in [
        &[0x80][..],
        &[0x80, 0][..],
        &[0x81, 0][..],
        &[0xff, 0xff, 0xff, 0xff, 0x10][..],
        &[0x80, 0x80, 0x80, 0x80, 0x80, 0][..],
        &[127][..],
    ] {
        let mut bytes = b"BMS\x08".to_vec();
        bytes.extend_from_slice(length);
        assert!(image_header(&bytes).is_none(), "{length:?}");
    }
    for old in [b"AXR1", b"AXR8"] {
        assert!(image_header(old).is_none());
    }
    for body_len in [0usize, 1, 127, 128, 16_383, 16_384] {
        let mut bytes = b"BMS\x08".to_vec();
        let mut remaining = body_len;
        loop {
            let byte = (remaining & 127) as u8;
            remaining >>= 7;
            bytes.push(byte | if remaining == 0 { 0 } else { 128 });
            if remaining == 0 {
                break;
            }
        }
        bytes.resize(bytes.len() + body_len, 0);
        let encoded_len = bytes.len();
        assert_eq!(image_header(&bytes).unwrap().encoded_len, encoded_len);
        if body_len != 0 {
            assert!(image_header(&bytes[..encoded_len - 1]).is_none());
        }
        bytes.extend_from_slice(&[0xa5; 8]);
        assert_eq!(image_header(&bytes).unwrap().encoded_len, encoded_len);
    }
}

// ── counting allocator ───────────────────────────────────────────────────

// Per-thread, because `cargo test` runs the cases in this file concurrently
// and a process-wide counter charges one test's allocations to another — a
// zero-assertion cannot be built on a counter three other threads are also
// incrementing. `const`-initialised TLS registers no destructor and
// allocates nothing, so the counter cannot recurse through the allocator it
// instruments.
thread_local! {
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
}

struct Counting;

#[inline(always)]
fn bump() {
    let _ = ALLOCS.try_with(|n| n.set(n.get() + 1));
}

// SAFETY: every method forwards to `System` unchanged; the counter is a
// thread-local side effect that cannot affect the returned pointers.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        bump();
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        bump();
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        bump();
        unsafe { System.alloc_zeroed(layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn allocations(f: impl FnOnce()) -> usize {
    let before = ALLOCS.with(|n| n.get());
    f();
    ALLOCS.with(|n| n.get()) - before
}

// ── corpus ───────────────────────────────────────────────────────────────

fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    state.wrapping_mul(0x2545_f491_4f6c_dd1d)
}

/// Distinct ascending sample of `n` values below `universe` — the g200
/// crossover generator, so the sparse points here are the points that
/// produced the published byte ratios.
fn uniform(universe: u32, n: usize, seed: u64) -> Vec<u32> {
    let mut state = seed;
    let mut seen = BTreeSet::new();
    while seen.len() < n {
        seen.insert((xorshift(&mut state) % u64::from(universe)) as u32);
    }
    seen.into_iter().collect()
}

/// Every shape the ladder and the root selector can produce, including the
/// ones each was blind to before: stride 1 (the densest progression, which
/// no oracle shape reached until the reciprocal overflowed on it) and
/// one-value-per-chunk sparsity (the shape that makes the forest all
/// directory and hands the root to Elias-Fano).
fn shapes() -> Vec<(&'static str, Vec<u32>)> {
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    vec![
        ("empty", Vec::new()),
        ("single", vec![42]),
        ("pair", vec![7, 65_540]),
        ("contiguous", (0..1_000u32).collect()),
        ("contiguous-chunk-straddle", (65_000..70_000u32).collect()),
        ("stride7", (0..20_000u32).map(|i| i * 7).collect()),
        ("stride13-wide", (0..200_000u32).map(|i| i * 13).collect()),
        (
            "jittered-dense",
            (0..20_000u32)
                .map(|i| i * 11 + (xorshift(&mut state) % 3) as u32)
                .collect(),
        ),
        (
            "jittered-sparse",
            (0..5_000u32)
                .map(|i| i * 97 + (xorshift(&mut state) % 5) as u32)
                .collect(),
        ),
        ("dense-chunk", uniform(65_536, 60_000, 0x51)),
        ("u20k/n1000", uniform(20_000, 1_000, 0xA1 + 1_000)),
        ("u20k/n4000", uniform(20_000, 4_000, 0xA1 + 4_000)),
        ("u20k/n8000", uniform(20_000, 8_000, 0xA1 + 8_000)),
        ("u13m/n100", uniform(13_000_000, 100, 0xB2 + 100)),
        ("u13m/n1000", uniform(13_000_000, 1_000, 0xB2 + 1_000)),
        ("u13m/n10000", uniform(13_000_000, 10_000, 0xB2 + 10_000)),
        (
            "wide-sparse-64chunks",
            (0..200u32).map(|i| i * 900_000).collect(),
        ),
        (
            "many-chunks",
            (0..300u32)
                .flat_map(|c| (0..3u32).map(move |i| c * 65_536 + i * 11))
                .collect(),
        ),
        // The only shape that activates the tagged slot plane: over 64
        // chunks, keys BROKEN (every other key), and dense enough per chunk
        // that the forest still beats an Elias-Fano root on bytes. Without
        // it the sweep never reaches `locate`'s hashed fallback at all.
        (
            "broken-keys-slotted",
            (0..200u32)
                .flat_map(|c| (0..5_000u32).map(move |i| c * 2 * 65_536 + i * 13))
                .collect(),
        ),
        // A Stride chunk beside a genuinely irregular dense Words chunk.
        (
            "mixed-arms",
            (1..65_536u32)
                .chain((65_536..103_000u32).filter(|i| i % 3 != 0))
                .collect(),
        ),
    ]
}

// ── agreement ────────────────────────────────────────────────────────────

#[test]
fn view_matches_owned_and_btreeset() {
    for (name, values) in shapes() {
        let oracle: BTreeSet<u32> = values.iter().copied().collect();
        let oracle_values: Vec<u32> = oracle.iter().copied().collect();
        let owned = Bitmosaic::from_sorted(values.iter().copied());
        let bytes = owned.to_bytes();
        assert_eq!(
            bytes.len(),
            owned.serialized_len(),
            "{name}: closed-form length"
        );
        let view = BitmosaicView::open(&bytes).unwrap_or_else(|| panic!("{name}: open"));

        assert_eq!(view.len(), oracle.len() as u64, "{name}: len");
        assert_eq!(view.is_empty(), oracle.is_empty(), "{name}: is_empty");

        let mut walked = Vec::new();
        view.for_each(|v| walked.push(v));
        assert_eq!(walked, oracle_values, "{name}: walk");

        for (k, &v) in oracle.iter().enumerate() {
            assert!(view.contains(v), "{name}: contains member {v}");
            if k % 37 == 0 || k + 1 == oracle.len() {
                assert_eq!(view.select(k as u64), Some(v), "{name}: select {k}");
                assert_eq!(view.rank(v), k as u64, "{name}: rank {v}");
                assert_eq!(view.rank(v), owned.rank(v), "{name}: rank vs owned {v}");
            }
        }
        assert_eq!(view.select(view.len()), None, "{name}: select past end");

        // Probes that are mostly misses, including the neighbours of every
        // member - the values an off-by-one in a bucket walk lets through.
        let mut state = 0xDEAD_BEEFu64;
        for _ in 0..2_000 {
            let probe = (xorshift(&mut state) % 13_500_000) as u32;
            assert_eq!(
                view.contains(probe),
                oracle.contains(&probe),
                "{name}: probe {probe}"
            );
            assert_eq!(
                view.rank(probe),
                oracle_values.partition_point(|value| *value < probe) as u64,
                "{name}: rank probe {probe}"
            );
        }
        for &v in oracle.iter().take(200) {
            for probe in [v.wrapping_sub(1), v.wrapping_add(1)] {
                assert_eq!(
                    view.contains(probe),
                    oracle.contains(&probe),
                    "{name}: neighbour {probe} of {v}"
                );
                assert_eq!(
                    view.rank(probe),
                    oracle_values.partition_point(|value| *value < probe) as u64,
                    "{name}: neighbour rank {probe}"
                );
            }
        }

        assert_eq!(
            view.as_arithmetic(),
            owned.as_arithmetic(),
            "{name}: as_arithmetic"
        );
    }
}

#[test]
fn and_len_matches_across_every_root_pairing() {
    let all = shapes();
    let planes: Vec<(&str, Vec<u8>, BTreeSet<u32>)> = all
        .iter()
        .map(|(name, values)| {
            (
                *name,
                Bitmosaic::from_sorted(values.iter().copied()).to_bytes(),
                values.iter().copied().collect(),
            )
        })
        .collect();
    let (mut saw_ef, mut saw_forest, mut saw_slots) = (false, false, false);
    for (na, ba, sa) in &planes {
        for (nb, bb, sb) in &planes {
            let (va, vb) = (
                BitmosaicView::open(ba).unwrap(),
                BitmosaicView::open(bb).unwrap(),
            );
            saw_ef |= va.is_elias_fano();
            saw_forest |= !va.is_elias_fano();
            saw_slots |= va.has_slot_plane();
            assert_eq!(
                va.and_len(&vb),
                sa.intersection(sb).count() as u64,
                "{na} AND {nb}"
            );
        }
    }
    // Coverage, asserted rather than assumed. Each of these selects a
    // different read path, and a corpus missing one would pass this sweep
    // while leaving that path unexercised — the shape of a green suite that
    // certifies nothing.
    assert!(saw_ef, "corpus must contain an Elias-Fano root");
    assert!(saw_forest, "corpus must contain a forest root");
    assert!(
        saw_slots,
        "corpus must contain a forest whose lookups take the slot plane"
    );
    // And that some forest carries Stride AND Words at once, so the
    // per-chunk kernel dispatch is crossed within a single plane and not
    // only between planes. Specific on purpose: `u13m/n1000` is already
    // Stride+Array, so a "more than one kind" test would pass without the
    // pairing this is here to cover.
    let mixed: Vec<&str> = all
        .iter()
        .filter(|(_, values)| {
            let c = Bitmosaic::from_sorted(values.iter().copied()).container_census();
            c.0 > 0 && c.3 > 0
        })
        .map(|(name, _)| *name)
        .collect();
    assert!(
        !mixed.is_empty(),
        "corpus must contain a forest carrying Stride and Words at once"
    );
}

/// The closed-form output shape: `and_len` sizes the buffer before any
/// result exists, `and_into` fills it. No arena, no growth, no append.
#[test]
fn and_into_fills_a_buffer_sized_by_and_len() {
    let all = shapes();
    let planes: Vec<(&str, Vec<u8>, BTreeSet<u32>)> = all
        .iter()
        .map(|(name, values)| {
            (
                *name,
                Bitmosaic::from_sorted(values.iter().copied()).to_bytes(),
                values.iter().copied().collect(),
            )
        })
        .collect();
    // One buffer for the whole sweep, sized once at the largest answer:
    // the shape a startup-sized arena has, proving no pair needs to grow.
    let widest = planes.iter().map(|(_, _, s)| s.len()).max().unwrap();
    let mut dest = vec![0u32; widest];
    for (na, ba, sa) in &planes {
        for (nb, bb, sb) in &planes {
            let (va, vb) = (
                BitmosaicView::open(ba).unwrap(),
                BitmosaicView::open(bb).unwrap(),
            );
            let n = va.and_len(&vb) as usize;
            let written = allocations(|| {
                let got = va.and_into(&vb, &mut dest[..n]);
                assert_eq!(got, n, "{na} AND {nb}: and_len must size and_into exactly");
            });
            assert_eq!(written, 0, "{na} AND {nb}: the scatter must not allocate");
            assert_eq!(
                &dest[..n],
                sa.intersection(sb).copied().collect::<Vec<_>>().as_slice(),
                "{na} AND {nb}: contents"
            );
        }
    }
}

/// Mapped planes carry no alignment guarantee. Every accessor must give the
/// same answer at every byte offset, including the SIMD arms, which take an
/// aligned fast path only when the mapping happens to provide one.
#[test]
fn view_is_correct_at_every_byte_alignment() {
    for (name, values) in shapes() {
        let bytes = Bitmosaic::from_sorted(values.iter().copied()).to_bytes();
        let oracle: BTreeSet<u32> = values.iter().copied().collect();
        let reference = BitmosaicView::open(&bytes).unwrap();
        for offset in 1..8usize {
            let mut shifted = vec![0u8; offset];
            shifted.extend_from_slice(&bytes);
            let view = BitmosaicView::open(&shifted[offset..]).unwrap();
            assert_eq!(view.len(), reference.len(), "{name}+{offset}: len");
            assert_eq!(
                view.and_len(&reference),
                reference.len(),
                "{name}+{offset}: self AND"
            );
            for (k, &v) in oracle.iter().enumerate().step_by(53) {
                assert!(view.contains(v), "{name}+{offset}: contains {v}");
                assert_eq!(view.rank(v), k as u64, "{name}+{offset}: rank {v}");
                assert_eq!(
                    view.select(k as u64),
                    Some(v),
                    "{name}+{offset}: select {k}"
                );
            }
        }
    }
}

#[test]
fn bitmosaic64_view_matches_oracle() {
    let mut state = 9u64;
    let shapes: Vec<(&str, Vec<u64>)> = vec![
        // The engine's entity domain: dense ordinals, high32 = 0.
        ("ordinals", (1..=100_000u64).collect()),
        ("strided", (0..40_000u64).map(|i| i * 7).collect()),
        (
            "multi-high",
            (0..10_000u64)
                .map(|_| xorshift(&mut state) % (3u64 << 32))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        ),
        (
            "boundary",
            (0..2_000u64).map(|i| (i << 32) | (i * 977)).collect(),
        ),
    ];
    for (name, values) in &shapes {
        let owned = Bitmosaic64::from_sorted(values.iter().copied());
        let bytes = owned.to_bytes();
        let view = Bitmosaic64View::open(&bytes).unwrap_or_else(|| panic!("{name}: open"));
        let oracle: BTreeSet<u64> = values.iter().copied().collect();
        assert_eq!(view.len(), oracle.len() as u64, "{name}: len");
        for (k, &v) in oracle.iter().enumerate() {
            if k % 97 == 0 {
                assert!(view.contains(v), "{name}: contains {v}");
                assert_eq!(view.rank(v), k as u64, "{name}: rank {v}");
                assert_eq!(view.select(k as u64), Some(v), "{name}: select {k}");
            }
        }
        assert_eq!(view.and_len(&view), oracle.len() as u64, "{name}: self AND");
        // Independent oracle: recompute the progression from the values,
        // rather than comparing the view to another bitmosaic answer. Scoped
        // to the documented contract — a single low-32 forest. A stride
        // wider than 2^32 puts one value in each of many forests, and the
        // tier declines to detect that rather than walking every forest to
        // find out.
        let want = (values.len() >= 2
            && values.iter().all(|v| v >> 32 == 0)
            && values
                .windows(2)
                .all(|w| w[1] - w[0] == values[1] - values[0]))
        .then(|| (values[0], values[1] - values[0], values.len() as u64));
        assert_eq!(view.as_arithmetic(), want, "{name}: as_arithmetic");
    }

    // The consumer shape that motivated the tier: a stride-1 ordinal domain
    // must stay a recognised progression through the wire format, so the
    // reader answers rank and select without touching a payload page.
    let ordinals = Bitmosaic64::from_sorted(1..=100_000u64);
    let bytes = ordinals.to_bytes();
    let view = Bitmosaic64View::open(&bytes).unwrap();
    assert_eq!(view.as_arithmetic(), Some((1, 1, 100_000)));
}

// ── no allocation on the read path ───────────────────────────────────────

#[test]
fn reads_allocate_nothing() {
    // Instrument check first: a counter that cannot report a violation
    // cannot certify its absence. This must be non-zero.
    let witness = allocations(|| {
        let v: Vec<u32> = Vec::with_capacity(64);
        std::hint::black_box(&v);
    });
    assert!(
        witness > 0,
        "the counting allocator must observe a real allocation"
    );

    let a = Bitmosaic::from_sorted(uniform(13_000_000, 10_000, 5)).to_bytes();
    let b = Bitmosaic::from_sorted(uniform(13_000_000, 10_000, 6)).to_bytes();
    let dense = Bitmosaic::from_sorted(uniform(20_000, 8_000, 1)).to_bytes();
    let ordinals = Bitmosaic64::from_sorted(1..=100_000u64).to_bytes();

    let (va, vb) = (
        BitmosaicView::open(&a).unwrap(),
        BitmosaicView::open(&b).unwrap(),
    );
    let vd = BitmosaicView::open(&dense).unwrap();
    let vo = Bitmosaic64View::open(&ordinals).unwrap();
    assert!(
        va.is_elias_fano(),
        "u13m/n10000 must publish as an Elias-Fano root"
    );
    assert!(
        !vd.is_elias_fano(),
        "u20k/n8000 must publish as a forest root"
    );

    let reads = allocations(|| {
        let mut acc = 0u64;
        for i in 0..20_000u32 {
            acc += u64::from(va.contains(i * 641));
            acc += va.rank(i * 641);
            acc += u64::from(vd.contains(i % 20_000));
            acc += vd.rank(i % 20_000);
            acc += vo.rank(u64::from(i));
            acc += u64::from(vo.contains(u64::from(i)));
        }
        for k in 0..5_000u64 {
            acc += u64::from(va.select(k).unwrap_or(0));
            acc += u64::from(vd.select(k).unwrap_or(0));
            acc += vo.select(k).unwrap_or(0);
        }
        acc += va.and_len(&vb);
        acc += vd.and_len(&vd);
        acc += vo.and_len(&vo);
        let mut walked = 0u64;
        va.for_each(|_| walked += 1);
        vd.for_each(|_| walked += 1);
        std::hint::black_box((acc, walked));
    });
    assert_eq!(reads, 0, "the read path must not call the allocator");

    // Attaching a view is itself a read path: a segment maps once and opens
    // a plane per key, so `open` may not allocate either.
    let opens = allocations(|| {
        for _ in 0..1_000 {
            std::hint::black_box(BitmosaicView::open(&a));
            std::hint::black_box(Bitmosaic64View::open(&ordinals));
        }
    });
    assert_eq!(opens, 0, "opening a view must not call the allocator");
}

// ── malformed input ──────────────────────────────────────────────────────

#[test]
fn truncated_and_corrupt_planes_are_refused_not_trusted() {
    for (name, values) in shapes() {
        let bytes = Bitmosaic::from_sorted(values.iter().copied()).to_bytes();
        assert!(BitmosaicView::open(&bytes).is_some(), "{name}: intact");
        for cut in [0usize, 1, 8, 16, 31] {
            if cut < bytes.len() {
                assert!(
                    BitmosaicView::open(&bytes[..cut]).is_none(),
                    "{name}: truncated to {cut}"
                );
            }
        }
        if bytes.len() > 40 {
            assert!(
                BitmosaicView::open(&bytes[..bytes.len() - 1]).is_none(),
                "{name}: one byte short"
            );
        }
        let mut wrong_magic = bytes.clone();
        wrong_magic[0] ^= 0xFF;
        assert!(BitmosaicView::open(&wrong_magic).is_none(), "{name}: magic");
        let mut wrong_version = bytes.clone();
        wrong_version[3] = (wrong_version[3] & 7) | (2 << 3);
        assert!(
            BitmosaicView::open(&wrong_version).is_none(),
            "{name}: version"
        );
    }
}

/// Every answer a view gives must be about the bytes it holds — never a
/// panic, never a read past the string. Two halves:
///
/// 1. **Invalid control bytes.** Unsupported versions/modes are refused
///    before body geometry is interpreted.
/// 2. **Single-byte damage anywhere in the string.** Whatever byte flips:
///    `open` returns `None` or a view every read path of which terminates
///    inside the string (exercised, not asserted — a panic or an
///    out-of-range read is the failure); `open_verified` returns `None` or
///    a view whose walk is strictly ascending, whose `len` is the walk's
///    length, and whose `contains` / `rank` / `select` / cursor agree with
///    that walk. A flip inside a container payload may describe a different
///    valid set — that is allowed — but a verified view never contradicts
///    itself.
#[test]
fn forged_headers_are_refused_and_damaged_planes_never_lie() {
    fn exercise(view: BitmosaicView<'_>) {
        let mut walk = Vec::new();
        view.for_each(|v| walk.push(v));
        let step = (walk.len() / 64).max(1);
        for (k, &v) in walk.iter().enumerate().step_by(step) {
            let _ = (view.contains(v), view.rank(v), view.select(k as u64));
        }
        for probe in [0u32, 1, 65_535, 65_536, u32::MAX / 2, u32::MAX] {
            let _ = (view.contains(probe), view.rank(probe));
        }
        let _ = view.select(view.len());
        let _ = view.range().count();
    }
    fn self_consistent(name: &str, what: &str, view: BitmosaicView<'_>) {
        let mut walk = Vec::new();
        view.for_each(|v| walk.push(v));
        assert_eq!(walk.len() as u64, view.len(), "{name}/{what}: len vs walk");
        assert!(
            walk.windows(2).all(|w| w[0] < w[1]),
            "{name}/{what}: walk not strictly ascending"
        );
        let step = (walk.len() / 64).max(1);
        for (k, &v) in walk.iter().enumerate().step_by(step) {
            assert!(view.contains(v), "{name}/{what}: contains({v})");
            assert_eq!(view.rank(v), k as u64, "{name}/{what}: rank({v})");
            assert_eq!(view.select(k as u64), Some(v), "{name}/{what}: select({k})");
            let miss = v.wrapping_add(1);
            if walk.get(k + 1) != Some(&miss) {
                assert!(!view.contains(miss), "{name}/{what}: contains({miss})");
            }
        }
        assert_eq!(
            view.select(view.len()),
            None,
            "{name}/{what}: select past end"
        );
        let pulled: Vec<u32> = view.range().collect();
        assert_eq!(pulled, walk, "{name}/{what}: cursor vs for_each");
    }

    for (name, values) in shapes() {
        let bytes = Bitmosaic::from_sorted(values.iter().copied()).to_bytes();
        for control in [0, 3, 11, 14, 15, 16, 255] {
            let mut forged = bytes.clone();
            forged[3] = control;
            assert!(
                BitmosaicView::open(&forged).is_none(),
                "{name}: control {control}"
            );
        }

        // Half 2: single-byte damage. Every position for small strings; for
        // large ones about 256 positions on an odd stride, so every plane
        // and every field alignment is still visited.
        let stride = (bytes.len() / 256).max(1) | 1;
        for at in (0..bytes.len()).step_by(stride) {
            for flip in [0xFFu8, 0x01, 0x80] {
                let mut damaged = bytes.clone();
                damaged[at] ^= flip;
                if let Some(view) = BitmosaicView::open(&damaged) {
                    exercise(view);
                }
                if let Some(view) = BitmosaicView::open_verified(&damaged) {
                    self_consistent(name, &format!("byte {at} ^ {flip:#04x}"), view);
                }
            }
        }
    }
}

// ── the borrowed cursor ──────────────────────────────────────────────────
//
// The wire tier had `for_each` and nothing else: a zero-copy reader could
// push every member but never pull one, so no merge — leapfrog seek, k-way
// intersection, or any of the four lazy adaptors — could run over mapped
// bytes. These cases are the contract of the cursor that closes that gap.

/// The whole point of a cursor: the pulled sequence must be exactly the
/// pushed one, on every ladder arm the encoder can produce.
///
/// Checked against `for_each` AND `BTreeSet`, because a cursor bug that
/// mirrored a `for_each` bug would hide behind either alone.
#[test]
fn wire_cursor_pulls_exactly_what_for_each_pushes() {
    let mut saw_ef_root = false;
    let mut saw_forest_root = false;
    for (name, values) in shapes() {
        let oracle: BTreeSet<u32> = values.iter().copied().collect();
        let bytes = Bitmosaic::from_sorted(values.iter().copied()).to_bytes();
        let view = BitmosaicView::open(&bytes).unwrap_or_else(|| panic!("{name}: open"));
        saw_ef_root |= view.is_elias_fano();
        saw_forest_root |= !view.is_elias_fano();

        let mut pushed = Vec::new();
        view.for_each(|v| pushed.push(v));
        let pulled: Vec<u32> = view.range().collect();
        assert_eq!(pulled, pushed, "{name}: pulled walk vs for_each");
        assert_eq!(
            pulled,
            oracle.iter().copied().collect::<Vec<_>>(),
            "{name}: vs BTreeSet"
        );

        // `size_hint` is the exact remaining count at every position, and
        // `len - remaining` is the head's ordinal — the property that lets a
        // consumer read a rank off a cursor it is already standing on.
        let mut cursor = view.range();
        for (ordinal, &want) in oracle.iter().enumerate() {
            assert_eq!(
                cursor.size_hint(),
                (oracle.len() - ordinal, Some(oracle.len() - ordinal)),
                "{name}: size_hint at {ordinal}"
            );
            assert_eq!(
                view.len() - cursor.remaining(),
                ordinal as u64,
                "{name}: ordinal"
            );
            assert_eq!(Range::front(&cursor), want, "{name}: front at {ordinal}");
            // `front` is idempotent: reading it twice observes nothing.
            assert_eq!(Range::front(&cursor), want, "{name}: front not idempotent");
            Range::pop_front(&mut cursor);
        }
        assert!(Range::empty(&cursor), "{name}: cursor outlived the set");
    }
    // Both roots must have been exercised, or the sweep proves less than it
    // appears to: the Elias-Fano root is a completely separate walk.
    assert!(saw_ef_root, "corpus never produced an Elias-Fano root");
    assert!(saw_forest_root, "corpus never produced a forest root");
}

/// `seek` is a lower bound: it lands on the first member `>= target`, for
/// targets that are members, non-members, and outside the set entirely.
#[test]
fn wire_cursor_seek_is_a_lower_bound() {
    for (name, values) in shapes() {
        let oracle: BTreeSet<u32> = values.iter().copied().collect();
        let bytes = Bitmosaic::from_sorted(values.iter().copied()).to_bytes();
        let view = BitmosaicView::open(&bytes).unwrap_or_else(|| panic!("{name}: open"));

        // A fixed probe BUDGET, not a fixed stride: the tail check below is
        // O(n) per probe, so a stride-based sweep would be quadratic in the
        // 60,000-member shapes and turn a correctness test into a timeout.
        let stride = (oracle.len() / 16).max(1);
        let mut probes: Vec<u32> = vec![0, 1, u32::MAX];
        probes.extend(
            oracle
                .iter()
                .step_by(stride)
                .flat_map(|&v| [v.saturating_sub(1), v, v.saturating_add(1)]),
        );
        for (index, probe) in probes.into_iter().enumerate() {
            let want = oracle.range(probe..).next().copied();
            let mut cursor = view.range();
            cursor.seek(probe);
            assert_eq!(cursor.next(), want, "{name}: seek {probe}");
            // Landing correctly is half of it — the REST of the walk must
            // still be the tail of the set, not a cursor left inconsistent.
            // Four per shape: the failure this catches is a corrupted cursor
            // state, which is a property of the landing, not of which member
            // was landed on.
            if index < 4
                && let Some(found) = want
            {
                let mut cursor = view.range();
                cursor.seek(probe);
                let tail: Vec<u32> = cursor.collect();
                let expected: Vec<u32> = oracle.range(found..).copied().collect();
                assert_eq!(tail, expected, "{name}: tail after seek {probe}");
            }
        }

        // Monotone seeks on ONE cursor: the leapfrog shape. A seek below the
        // head must not rewind it.
        let members: Vec<u32> = oracle.iter().copied().collect();
        if members.len() >= 4 {
            let mut cursor = view.range();
            cursor.seek(members[members.len() / 2]);
            let head = Range::front(&cursor);
            cursor.seek(members[0]);
            assert_eq!(
                Range::front(&cursor),
                head,
                "{name}: backwards seek rewound"
            );
        }
    }
}

/// The u64 tier's cursor, over the shapes that actually occupy several
/// high-32 forests — the arm the FTS posting adoption drives.
#[test]
fn u64_wire_cursor_walks_and_seeks_across_forests() {
    let mut state = 9u64;
    let shapes: Vec<(&str, Vec<u64>)> = vec![
        ("empty", Vec::new()),
        ("single", vec![1 << 40]),
        ("ordinals", (1..=100_000u64).collect()),
        ("strided", (0..40_000u64).map(|i| i * 7).collect()),
        (
            "multi-high",
            (0..10_000u64)
                .map(|_| xorshift(&mut state) % (3u64 << 32))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        ),
        // One member per high-32 forest: the directory-dominated shape, and
        // the one where a forest-crossing cursor is all crossing.
        (
            "one-per-forest",
            (0..500u64).map(|i| (i << 32) | 7).collect(),
        ),
        (
            "boundary",
            (0..2_000u64).map(|i| (i << 32) | (i * 977)).collect(),
        ),
    ];
    for (name, values) in &shapes {
        let oracle: BTreeSet<u64> = values.iter().copied().collect();
        let bytes = Bitmosaic64::from_sorted(values.iter().copied()).to_bytes();
        let view = Bitmosaic64View::open(&bytes).unwrap_or_else(|| panic!("{name}: open"));

        let mut pushed = Vec::new();
        view.for_each(|v| pushed.push(v));
        assert_eq!(
            view.range().collect::<Vec<u64>>(),
            pushed,
            "{name}: walk vs for_each"
        );
        assert_eq!(
            pushed,
            oracle.iter().copied().collect::<Vec<_>>(),
            "{name}: vs BTreeSet"
        );

        let mut probes: Vec<u64> = vec![0, u64::MAX];
        for (k, &v) in oracle.iter().enumerate() {
            if k % 41 == 0 {
                probes.extend([v.saturating_sub(1), v, v.saturating_add(1)]);
            }
        }
        for probe in probes {
            let want = oracle.range(probe..).next().copied();
            let mut cursor = view.range();
            cursor.seek(probe);
            assert_eq!(cursor.next(), want, "{name}: seek {probe}");
        }

        // Ordinal invariant across the forest boundary, where a naive
        // cursor loses the prefix count.
        let mut cursor = view.range();
        for (ordinal, &want) in oracle.iter().enumerate() {
            assert_eq!(
                view.len() - cursor.remaining(),
                ordinal as u64,
                "{name}: ordinal"
            );
            assert_eq!(cursor.next(), Some(want), "{name}: element {ordinal}");
        }
        assert_eq!(cursor.next(), None, "{name}: cursor outlived the set");
    }
}

/// The capability the cursor unlocks: the four lazy set adaptors, which
/// need `Range + Iterator` on both sides, now compose over MAPPED BYTES.
///
/// Before this cursor they could only be built over an owned `Bitmosaic64`,
/// which a durable reader does not have and cannot cheaply make.
#[test]
fn the_lazy_adaptors_compose_over_borrowed_bytes() {
    let mut state = 0x2545_f491u64;
    let mut make = |n: usize, span: u64| -> Vec<u64> {
        (0..n)
            .map(|_| xorshift(&mut state) % span)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    };
    for (name, span) in [("one-forest", 1u64 << 20), ("many-forests", 5u64 << 32)] {
        let left = make(4_000, span);
        let right = make(4_000, span);
        let (la, ra): (BTreeSet<u64>, BTreeSet<u64>) = (
            left.iter().copied().collect(),
            right.iter().copied().collect(),
        );
        let lb = Bitmosaic64::from_sorted(left.iter().copied()).to_bytes();
        let rb = Bitmosaic64::from_sorted(right.iter().copied()).to_bytes();
        let (lv, rv) = (
            Bitmosaic64View::open(&lb).expect("open left"),
            Bitmosaic64View::open(&rb).expect("open right"),
        );

        let and: Vec<u64> = AndRange::leapfrog(lv.range(), rv.range()).collect();
        assert_eq!(
            and,
            la.intersection(&ra).copied().collect::<Vec<_>>(),
            "{name}: AND"
        );
        // The fused counting kernel is the independent witness: it never
        // looks at a member, so agreeing with it checks the walk, not itself.
        assert_eq!(and.len() as u64, lv.and_len(&rv), "{name}: AND vs and_len");

        let or: Vec<u64> = OrRange::new(lv.range(), rv.range()).collect();
        assert_eq!(or, la.union(&ra).copied().collect::<Vec<_>>(), "{name}: OR");

        let xor: Vec<u64> = XorRange::new(lv.range(), rv.range()).collect();
        assert_eq!(
            xor,
            la.symmetric_difference(&ra).copied().collect::<Vec<_>>(),
            "{name}: XOR"
        );

        let andnot: Vec<u64> = AndNotRange::new(lv.range(), rv.range()).collect();
        assert_eq!(
            andnot,
            la.difference(&ra).copied().collect::<Vec<_>>(),
            "{name}: ANDNOT"
        );

        // Three-way, the FTS query shape: an adaptor over an adaptor.
        let third = make(4_000, span);
        let ta: BTreeSet<u64> = third.iter().copied().collect();
        let tb = Bitmosaic64::from_sorted(third.iter().copied()).to_bytes();
        let tv = Bitmosaic64View::open(&tb).expect("open third");
        let three: Vec<u64> =
            AndRange::leapfrog(AndRange::leapfrog(lv.range(), rv.range()), tv.range()).collect();
        let want: Vec<u64> = la
            .intersection(&ra)
            .copied()
            .filter(|v| ta.contains(v))
            .collect();
        assert_eq!(three, want, "{name}: three-way AND");
    }
}

/// A cursor that allocated would defeat the entire point of the wire tier.
#[test]
fn cursor_walks_and_seeks_allocate_nothing() {
    // Instrument check first: a counter that cannot report a violation
    // cannot certify its absence.
    let witness = allocations(|| {
        let v: Vec<u32> = Vec::with_capacity(64);
        std::hint::black_box(&v);
    });
    assert!(witness > 0, "counting allocator cannot see an allocation");

    let sparse = Bitmosaic::from_sorted(uniform(13_000_000, 10_000, 5)).to_bytes();
    let dense = Bitmosaic::from_sorted(uniform(20_000, 8_000, 1)).to_bytes();
    let wide = Bitmosaic64::from_sorted((0..500u64).map(|i| (i << 32) | 7)).to_bytes();
    let sv = BitmosaicView::open(&sparse).unwrap();
    let dv = BitmosaicView::open(&dense).unwrap();
    let wv = Bitmosaic64View::open(&wide).unwrap();

    for (name, count) in [
        (
            "sparse walk",
            allocations(|| {
                std::hint::black_box(sv.range().count());
            }),
        ),
        (
            "dense walk",
            allocations(|| {
                std::hint::black_box(dv.range().count());
            }),
        ),
        (
            "u64 walk",
            allocations(|| {
                std::hint::black_box(wv.range().count());
            }),
        ),
        (
            "seek sweep",
            allocations(|| {
                for probe in (0..13_000_000u32).step_by(65_536) {
                    let mut cursor = sv.range();
                    cursor.seek(probe);
                    std::hint::black_box(cursor.next());
                }
            }),
        ),
        (
            "lazy AND",
            allocations(|| {
                std::hint::black_box(AndRange::leapfrog(sv.range(), sv.range()).count());
            }),
        ),
    ] {
        assert_eq!(count, 0, "{name} allocated");
    }
}

// ── the slot writer ──────────────────────────────────────────────────────

/// `write_into_slice` is `to_bytes` without the `Vec`: the same bytes land
/// at the head of a caller-owned slot, exactly `serialized_len` of them,
/// with the rest of the slot untouched, and a slot too short is refused
/// with the length it would need — before a byte is written.
#[test]
fn write_into_slice_lands_the_canonical_bytes_and_refuses_a_short_slot() {
    for (name, values) in shapes() {
        let owned = Bitmosaic::from_sorted(values.iter().copied());
        let canonical = owned.to_bytes();
        let needed = owned.serialized_len();
        assert_eq!(canonical.len(), needed, "{name}: closed-form length");

        let mut slot = vec![0xA5u8; needed + 16];
        let written = allocations(|| {
            assert_eq!(
                owned.write_into_slice(&mut slot),
                Ok(needed),
                "{name}: written"
            );
        });
        assert_eq!(written, 0, "{name}: the slot writer allocates nothing");
        assert_eq!(&slot[..needed], &canonical[..], "{name}: canonical bytes");
        assert!(
            slot[needed..].iter().all(|b| *b == 0xA5),
            "{name}: bytes past the string are untouched"
        );
        assert!(
            BitmosaicView::open_verified(&slot).is_some(),
            "{name}: the slot opens (the view bounds itself by the header's total)"
        );

        if needed > 0 {
            let mut short = vec![0xA5u8; needed - 1];
            assert_eq!(
                owned.write_into_slice(&mut short),
                Err(bitmosaic::PatchError::Capacity { needed }),
                "{name}: a short slot is refused with the length it needs"
            );
            assert!(
                short.iter().all(|b| *b == 0xA5),
                "{name}: a refused write touches nothing"
            );
        }
    }
}
