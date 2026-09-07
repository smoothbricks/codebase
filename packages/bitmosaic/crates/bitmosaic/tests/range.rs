//! Oracle and allocation gate for the D-style range protocol
//! (`bitmosaic::Range` and the four lazy set adaptors).
//!
//! Three properties are asserted here, and each is the reason a lazy adaptor
//! is allowed to exist at all:
//!
//! 1. **Agreement.** Every lazy range yields EXACTLY the sequence the
//!    materialising operation produces — element for element, on randomised
//!    inputs, with a `BTreeSet` as a third witness so a shared bug in the
//!    bitmosaic pair could not hide. The crate already races `roaring` in
//!    `benches/ops.rs` and `BTreeSet` in `src/lib.rs`; this follows that
//!    precedent rather than inventing a new one.
//! 2. **Zero allocation on a steady-state traversal.** A counting global
//!    allocator brackets the walks. This is a claim about zero, so the
//!    counter is proved capable of reporting non-zero in the same test — an
//!    instrument that cannot see a violation cannot certify its absence.
//!    A count is worth more than a timing here (handbook §4.4b): it does not
//!    expire when either arm is rewritten and does not care what else the
//!    machine was doing.
//! 3. **The protocol's own contract.** `front` is idempotent, `save` is a
//!    checkpoint that costs a copy, and the three primitives compose into
//!    the same sequence the `Iterator` bridge produces. Those are the
//!    properties the adaptors are written against; if they lapse the merges
//!    are silently wrong rather than loudly broken.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::BTreeSet;

use bitmosaic::{
    AndNotRange, AndRange, Bitmosaic, Bitmosaic64, Bitmosaic64View, BitmosaicBuilder, OrRange,
    Range, RangeIter, XorRange,
};
use roaring::RoaringBitmap;

// ── a FOREIGN range ──────────────────────────────────────────────────────

/// A `Range` written by a consumer, over a plain sorted slice — nothing to
/// do with `Bitmosaic`.
///
/// It implements ONLY the four protocol methods and deliberately no
/// `Iterator`, which is the situation [`RangeIter`] exists for. If the
/// adaptors could not consume `RangeIter<SliceRange>`, the module's claim
/// that they "compose over anything implementing the protocol" would be
/// false, and a bridge that only produced `Iterator` would not fix it —
/// the adaptors need both surfaces.
#[derive(Clone, Copy)]
struct SliceRange<'a>(&'a [u32]);

impl Range for SliceRange<'_> {
    type Item = u32;

    fn empty(&self) -> bool {
        self.0.is_empty()
    }

    fn front(&self) -> u32 {
        self.0[0]
    }

    fn pop_front(&mut self) {
        self.0 = &self.0[1..];
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

/// Shapes chosen to reach every container arm AND every overlap regime the
/// merges have a distinct path for: disjoint, nested, identical, and the
/// boundary cases where one side runs out first.
fn shapes() -> Vec<(&'static str, Vec<u32>)> {
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut random = |universe: u32, n: usize| -> Vec<u32> {
        let mut seen = BTreeSet::new();
        while seen.len() < n {
            seen.insert((xorshift(&mut state) % u64::from(universe)) as u32);
        }
        seen.into_iter().collect()
    };
    vec![
        ("empty", Vec::new()),
        ("single", vec![42]),
        ("two-runs", (0..4096).chain(32768..36864).collect()),
        // Disjoint from `single`, and one value per chunk: all Array.
        ("wide-sparse", (0..200u32).map(|i| i * 7919 + 1).collect()),
        // Pure progression: the Stride cursor.
        ("stride7", (0..20_000u32).map(|i| i * 7).collect()),
        // Shares every other member with stride7 — the equal-heads path.
        ("stride14", (0..10_000u32).map(|i| i * 14).collect()),
        // Near-arithmetic under CONE_MAX_LEN: the Cone cursor.
        ("cone", (0..1_000u32).map(|i| i * 61 + i % 5).collect()),
        // One dense chunk far above ARRAY_MAX: the Words cursor, spanning
        // every one of its directory groups.
        (
            "words-full-chunk",
            (0..65_536u32).filter(|i| i % 37 != 0).collect(),
        ),
        // Multi-chunk Words, so the outer key walk is exercised too.
        (
            "words-multi-chunk",
            (0..200_000u32).filter(|i| i % 5 != 0).collect(),
        ),
        ("u20k/n8000", random(20_000, 8_000)),
        ("u13m/n1000", random(13_000_000, 1_000)),
        // Both u32 ends, where a merge that indexes past the top chunk fails.
        ("u32-boundary", vec![0, u32::MAX - 1, u32::MAX]),
    ]
}

/// One merge pass that retains the positions the aligned cursors already
/// proved. Deliberately contains no `rank` call: `rank` is the independent
/// oracle below, not part of the positioned semijoin construction.
fn positioned_intersection(a: &Bitmosaic, b: &Bitmosaic) -> Vec<(u32, u64, u64)> {
    let (left_total, right_total) = (a.len(), b.len());
    let mut range = a.and_range(b);
    let mut positioned = Vec::new();
    while !range.empty() {
        let value = range.front();
        let (left, right) = range.sources();
        let left_remaining = u64::try_from(left.len()).expect("left range length fits u64");
        let right_remaining = u64::try_from(right.len()).expect("right range length fits u64");
        positioned.push((
            value,
            left_total - left_remaining,
            right_total - right_remaining,
        ));
        range.pop_front();
    }
    positioned
}

// ── 1. agreement ─────────────────────────────────────────────────────────

/// Every lazy range yields exactly what the materialising operation yields.
///
/// FOUR witnesses per cell, because two would not settle a shared bug and
/// three would not settle a shared *design* bug: the lazy range, the
/// materialised `Bitmosaic` walked, `BTreeSet` from std, and `roaring-rs` —
/// the real implementation of this data structure, which `benches/ops.rs`
/// already races and which is the precedent this follows. The materialising
/// ops finalize matching chunks while ranges advance their original cursors,
/// so these are independent implementations of the same set operation.
#[test]
fn every_lazy_range_matches_its_materialising_operation() {
    let all = shapes();
    let mut census = (0usize, 0usize, 0usize, 0usize, 0usize);
    let mut pairs = 0usize;
    for (na, va) in &all {
        let a = Bitmosaic::from_sorted(va.iter().copied());
        let sa: BTreeSet<u32> = va.iter().copied().collect();
        let c = a.container_census();
        census = (
            census.0 + c.0,
            census.1 + c.1,
            census.2 + c.2,
            census.3 + c.3,
            census.4 + c.4,
        );
        let ra: RoaringBitmap = va.iter().copied().collect();
        for (nb, vb) in &all {
            let b = Bitmosaic::from_sorted(vb.iter().copied());
            let sb: BTreeSet<u32> = vb.iter().copied().collect();
            let rb: RoaringBitmap = vb.iter().copied().collect();
            pairs += 1;
            let cell = format!("{na} vs {nb}");

            for (op, lazy, materialised, std_ref, roaring_ref) in [
                (
                    "and",
                    a.and_range(&b).collect::<Vec<u32>>(),
                    a.and(&b).iter().collect::<Vec<u32>>(),
                    sa.intersection(&sb).copied().collect::<Vec<u32>>(),
                    (&ra & &rb).into_iter().collect::<Vec<u32>>(),
                ),
                (
                    "or",
                    a.or_range(&b).collect(),
                    a.or(&b).iter().collect(),
                    sa.union(&sb).copied().collect(),
                    (&ra | &rb).into_iter().collect(),
                ),
                (
                    "xor",
                    a.xor_range(&b).collect(),
                    a.xor(&b).iter().collect(),
                    sa.symmetric_difference(&sb).copied().collect(),
                    (&ra ^ &rb).into_iter().collect(),
                ),
                (
                    "andnot",
                    a.andnot_range(&b).collect(),
                    a.andnot(&b).iter().collect(),
                    sa.difference(&sb).copied().collect(),
                    (&ra - &rb).into_iter().collect(),
                ),
            ] {
                assert_eq!(lazy, materialised, "{cell}: {op}_range vs Bitmosaic::{op}");
                assert_eq!(lazy, std_ref, "{cell}: {op}_range vs BTreeSet");
                assert_eq!(lazy, roaring_ref, "{cell}: {op}_range vs roaring-rs");
            }

            // The merge has already proved where every shared value sits in
            // both inputs. Retain both positions from the source cursors and
            // verify them through the independent rank directory.
            let positioned = positioned_intersection(&a, &b);
            assert_eq!(
                positioned
                    .iter()
                    .map(|&(value, _, _)| value)
                    .collect::<Vec<_>>(),
                sa.intersection(&sb).copied().collect::<Vec<_>>(),
                "{cell}: positioned intersection values"
            );
            for &(value, left_ordinal, right_ordinal) in &positioned {
                assert_eq!(
                    left_ordinal,
                    a.rank(value),
                    "{cell}: left ordinal at {value}"
                );
                assert_eq!(
                    right_ordinal,
                    b.rank(value),
                    "{cell}: right ordinal at {value}"
                );
            }

            // The counting form the crate already shipped must agree with
            // the range's own count, which is the claim that lets a caller
            // pick either.
            assert_eq!(
                a.and_range(&b).count() as u64,
                a.and_len(&b),
                "{cell}: and_range().count() vs and_len()"
            );
        }
    }
    // Worthless if the sweep never reached the arm it is about.
    assert!(pairs >= 100, "sweep must be a real cross product: {pairs}");
    assert!(
        census.0 > 0,
        "no shape froze a Stride container: {census:?}"
    );
    assert!(census.1 > 0, "no shape froze a Cone container: {census:?}");
    assert!(
        census.2 > 0,
        "no shape froze an Array container: {census:?}"
    );
    assert!(census.3 > 0, "no shape froze a Words container: {census:?}");
    assert!(census.4 > 0, "no shape froze a Runs container: {census:?}");
}

/// Exhaust every pair of subsets of an eight-value domain. The positioned
/// semijoin walks once and performs zero rank operations; two independent
/// `rank` calls per joined value are used only as the differential oracle.
///
/// The same sweep checks the deliberately different source-position
/// contracts of union, difference, and symmetric difference so all four
/// public accessors remain honest rather than implying two matched ordinals
/// where only an insertion position exists.
#[test]
fn exhaustive_positioned_intersection_matches_the_rank_oracle() {
    const DOMAIN: u32 = 8;
    let sets: Vec<(u16, Bitmosaic)> = (0u16..(1u16 << DOMAIN))
        .map(|mask| {
            let values = (0..DOMAIN).filter(move |&value| mask & (1u16 << value) != 0);
            (mask, Bitmosaic::from_sorted(values))
        })
        .collect();

    let mut pairs = 0usize;
    let mut joined = 0usize;
    let mut oracle_rank_calls = 0usize;
    for (left_mask, a) in &sets {
        for (right_mask, b) in &sets {
            pairs += 1;
            let positioned = positioned_intersection(a, b);
            let expected: Vec<u32> = (0..DOMAIN)
                .filter(|&value| {
                    left_mask & (1u16 << value) != 0 && right_mask & (1u16 << value) != 0
                })
                .collect();
            assert_eq!(
                positioned
                    .iter()
                    .map(|&(value, _, _)| value)
                    .collect::<Vec<_>>(),
                expected,
                "intersection output for masks {left_mask:#x} and {right_mask:#x}"
            );
            for &(value, left_ordinal, right_ordinal) in &positioned {
                assert_eq!(
                    left_ordinal,
                    a.rank(value),
                    "left mask {left_mask:#x}, {value}"
                );
                assert_eq!(
                    right_ordinal,
                    b.rank(value),
                    "right mask {right_mask:#x}, {value}"
                );
                oracle_rank_calls += 2;
                joined += 1;
            }

            let mut union = a.or_range(b);
            while !union.empty() {
                let value = union.front();
                let (left, right) = union.sources();
                let at_left = !left.empty() && left.front() == value;
                let at_right = !right.empty() && right.front() == value;
                assert!(at_left || at_right, "union source missing {value}");
                union.pop_front();
            }

            let mut difference = a.andnot_range(b);
            while !difference.empty() {
                let value = difference.front();
                let (left, right) = difference.sources();
                assert_eq!(left.front(), value, "difference left source");
                assert!(
                    right.empty() || right.front() > value,
                    "difference right insertion point"
                );
                difference.pop_front();
            }

            let mut symmetric = a.xor_range(b);
            while !symmetric.empty() {
                let value = symmetric.front();
                let (left, right) = symmetric.sources();
                let at_left = !left.empty() && left.front() == value;
                let at_right = !right.empty() && right.front() == value;
                assert_ne!(
                    at_left, at_right,
                    "xor must have exactly one producing source"
                );
                symmetric.pop_front();
            }
        }
    }

    assert_eq!(pairs, 65_536, "must exhaust the full subset cross product");
    assert!(joined > 0, "oracle must observe real joined values");
    assert_eq!(
        oracle_rank_calls,
        joined * 2,
        "oracle uses two ranks per joined value"
    );
}

/// `buf.extend(range)` is the `and_into` shape, and must fill the same
/// buffer with the same values — including the sizing contract, which is
/// that `and_len` is exact and known before the first result exists.
#[test]
fn extending_a_buffer_from_a_range_reproduces_and_into() {
    let all = shapes();
    for (na, va) in &all {
        let a = Bitmosaic::from_sorted(va.iter().copied());
        let bytes_a = a.to_bytes();
        let view_a = bitmosaic::BitmosaicView::open(&bytes_a).expect("open a");
        for (nb, vb) in &all {
            let b = Bitmosaic::from_sorted(vb.iter().copied());
            let bytes_b = b.to_bytes();
            let view_b = bitmosaic::BitmosaicView::open(&bytes_b).expect("open b");

            let n = a.and_len(&b) as usize;
            let mut wired = vec![0u32; n];
            let written = view_a.and_into(&view_b, &mut wired);
            assert_eq!(written, n, "{na} vs {nb}: and_into wrote {written} of {n}");

            let mut extended: Vec<u32> = Vec::with_capacity(n);
            extended.extend(a.and_range(&b));
            assert_eq!(extended, wired, "{na} vs {nb}: extend(range) vs and_into");
        }
    }
}

/// The u64 tier's walk, which did not exist before: owned range, owned
/// `select` in a loop, and the wire view's `for_each`, all three agreeing
/// with a `BTreeSet`.
#[test]
fn the_u64_walk_agrees_with_select_and_the_wire_view() {
    let mut state = 0xC0_FFEEu64;
    let cases: Vec<(&str, Vec<u64>)> = vec![
        ("empty", Vec::new()),
        // The consumer shape: dense ordinals, all in high32 = 0.
        ("ordinals", (0..50_000u64).collect()),
        ("stride7-low", (0..20_000u64).map(|i| i * 7).collect()),
        // Multiple forests.
        (
            "multi-high",
            (0..8u64)
                .flat_map(|h| (0..500u64).map(move |i| (h << 32) | (i * 131)))
                .collect(),
        ),
        // One value per forest: the high plane is the whole structure.
        (
            "one-per-forest",
            (0..64u64).map(|h| (h << 32) | 7).collect(),
        ),
        ("u64-boundary", vec![0, 1, u64::MAX - 1, u64::MAX]),
        (
            "random",
            (0..4_000)
                .map(|_| xorshift(&mut state) % (1u64 << 40))
                .collect(),
        ),
    ];

    for (name, values) in cases {
        let reference: Vec<u64> = values
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let set = Bitmosaic64::from_sorted(reference.iter().copied());

        let walked: Vec<u64> = set.iter().collect();
        assert_eq!(walked, reference, "{name}: Bitmosaic64::iter vs input");

        let selected: Vec<u64> = (0..set.len())
            .map(|k| set.select(k).expect("k < len"))
            .collect();
        assert_eq!(walked, selected, "{name}: walk vs select");

        // Exact length before the walk, and exhaustion is terminal.
        let mut iter = set.iter();
        assert_eq!(
            iter.len(),
            reference.len(),
            "{name}: ExactSizeIterator::len"
        );
        for _ in 0..reference.len() {
            assert!(iter.next().is_some(), "{name}: short walk");
        }
        assert_eq!(iter.next(), None, "{name}: overrun");
        assert_eq!(iter.next(), None, "{name}: exhausted iterator restarted");

        // The wire tier's u64 walk is the third witness.
        let bytes = set.to_bytes();
        let view = Bitmosaic64View::open(&bytes).expect("open");
        let mut viewed = Vec::with_capacity(reference.len());
        view.for_each(|v| viewed.push(v));
        assert_eq!(viewed, reference, "{name}: Bitmosaic64View::for_each");
    }
}

/// Exhaust every subset of a boundary-heavy eight-value u64 domain. Each
/// metadata answer is witnessed independently by `BTreeSet`, by the owned
/// `select` sequence element-for-element, and by `Bitmosaic64View`.
#[test]
fn the_u64_metadata_primitives_match_three_exhaustive_oracles() {
    const DOMAIN: [u64; 8] = [
        0,
        1,
        u32::MAX as u64,
        1u64 << 32,
        (1u64 << 32) + 9,
        (3u64 << 32) + 7,
        u64::MAX - 1,
        u64::MAX,
    ];
    const PROBES: [u64; 15] = [
        0,
        1,
        2,
        u32::MAX as u64 - 1,
        u32::MAX as u64,
        1u64 << 32,
        (1u64 << 32) + 1,
        (1u64 << 32) + 9,
        (1u64 << 32) + 10,
        2u64 << 32,
        (3u64 << 32) + 7,
        4u64 << 32,
        u64::MAX - 2,
        u64::MAX - 1,
        u64::MAX,
    ];
    const QUANTILES: [f64; 12] = [
        f64::NEG_INFINITY,
        -0.25,
        0.0,
        0.125,
        0.25,
        0.5,
        0.625,
        0.875,
        1.0,
        1.25,
        f64::INFINITY,
        f64::NAN,
    ];

    struct Case {
        reference: BTreeSet<u64>,
        selected: Vec<u64>,
        owned: Bitmosaic64,
        bytes: Vec<u8>,
    }

    fn sample_ordinal(seed: u64, len: u64) -> Option<u64> {
        if len == 0 {
            return None;
        }
        let mut state = seed | 1;
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        Some(state % len)
    }

    let cases: Vec<Case> = (0usize..1usize << DOMAIN.len())
        .map(|mask| {
            let reference: BTreeSet<u64> = DOMAIN
                .iter()
                .enumerate()
                .filter_map(|(bit, value)| (mask & (1 << bit) != 0).then_some(*value))
                .collect();
            let selected: Vec<u64> = reference.iter().copied().collect();
            let owned = Bitmosaic64::from_sorted(selected.iter().copied());
            let bytes = owned.to_bytes();
            Case {
                reference,
                selected,
                owned,
                bytes,
            }
        })
        .collect();
    assert_eq!(cases.len(), 256, "the subset sweep must be exhaustive");

    for (mask, case) in cases.iter().enumerate() {
        let view = Bitmosaic64View::open(&case.bytes).expect("open exhaustive u64 case");

        assert_eq!(
            case.owned.len(),
            case.selected.len() as u64,
            "mask {mask}: len"
        );
        assert_eq!(
            view.len(),
            case.selected.len() as u64,
            "mask {mask}: view len"
        );
        for (ordinal, expected) in case.selected.iter().copied().enumerate() {
            assert_eq!(
                case.owned.select(ordinal as u64),
                Some(expected),
                "mask {mask}: owned select {ordinal}"
            );
            assert_eq!(
                view.select(ordinal as u64),
                Some(expected),
                "mask {mask}: view select {ordinal}"
            );
        }
        assert_eq!(
            case.owned.select(case.owned.len()),
            None,
            "mask {mask}: owned select end"
        );
        assert_eq!(
            view.select(view.len()),
            None,
            "mask {mask}: view select end"
        );

        for &lo in &PROBES {
            for &hi in &PROBES {
                let expected = if hi <= lo {
                    0
                } else {
                    case.reference.range(lo..hi).count() as u64
                };
                let selected = case
                    .selected
                    .iter()
                    .filter(|&&value| lo <= value && value < hi)
                    .count() as u64;
                let viewed = if hi <= lo {
                    0
                } else {
                    view.rank(hi) - view.rank(lo)
                };
                assert_eq!(
                    case.owned.count_range(lo, hi),
                    expected,
                    "mask {mask}: {lo}..{hi}"
                );
                assert_eq!(
                    selected, expected,
                    "mask {mask}: select witness for {lo}..{hi}"
                );
                assert_eq!(viewed, expected, "mask {mask}: view witness for {lo}..{hi}");
            }
        }

        for &probe in &PROBES {
            let expected = case.reference.range(..=probe).next_back().copied();
            let ordinal = case
                .selected
                .partition_point(|candidate| *candidate <= probe);
            let selected = ordinal.checked_sub(1).map(|at| case.selected[at]);
            let viewed = if view.contains(probe) {
                Some(probe)
            } else {
                let rank = view.rank(probe);
                rank.checked_sub(1).and_then(|at| view.select(at))
            };
            assert_eq!(
                case.owned.predecessor(probe),
                expected,
                "mask {mask}: pred {probe}"
            );
            assert_eq!(selected, expected, "mask {mask}: select pred {probe}");
            assert_eq!(viewed, expected, "mask {mask}: view pred {probe}");
        }

        for &q in &QUANTILES {
            let ordinal = if case.selected.is_empty() || !(0.0..=1.0).contains(&q) {
                None
            } else {
                Some((q * (case.selected.len() - 1) as f64).round() as usize)
            };
            let expected = ordinal.map(|at| case.selected[at]);
            let viewed = ordinal.and_then(|at| view.select(at as u64));
            assert_eq!(
                case.owned.quantile(q),
                expected,
                "mask {mask}: quantile {q:?}"
            );
            assert_eq!(viewed, expected, "mask {mask}: view quantile {q:?}");
        }

        for seed in (0u64..32).chain([u64::MAX]) {
            let ordinal = sample_ordinal(seed, case.selected.len() as u64);
            let expected = ordinal.map(|at| case.selected[at as usize]);
            let viewed = ordinal.and_then(|at| view.select(at));
            assert_eq!(
                case.owned.sample(seed),
                expected,
                "mask {mask}: sample {seed}"
            );
            assert_eq!(viewed, expected, "mask {mask}: view sample {seed}");
        }
    }

    for (left_mask, left) in cases.iter().enumerate() {
        let left_view = Bitmosaic64View::open(&left.bytes).expect("open left exhaustive case");
        for (right_mask, right) in cases.iter().enumerate() {
            let right_view =
                Bitmosaic64View::open(&right.bytes).expect("open right exhaustive case");
            let intersection = left.reference.intersection(&right.reference).count() as u64;
            let union = left.reference.union(&right.reference).count() as u64;
            let expected = if union == 0 {
                0.0
            } else {
                intersection as f64 / union as f64
            };
            let selected_intersection = left
                .selected
                .iter()
                .filter(|value| right.selected.binary_search(value).is_ok())
                .count() as u64;
            let viewed_intersection = left_view.and_len(&right_view);
            assert_eq!(
                left.owned.jaccard(&right.owned).to_bits(),
                expected.to_bits(),
                "jaccard masks {left_mask:#04x}/{right_mask:#04x}"
            );
            assert_eq!(
                selected_intersection, intersection,
                "select jaccard witness masks {left_mask:#04x}/{right_mask:#04x}"
            );
            assert_eq!(
                viewed_intersection, intersection,
                "view jaccard witness masks {left_mask:#04x}/{right_mask:#04x}"
            );
        }
    }
}

/// Exhaust every subset of `0..8` at every valid bucket width, then exercise
/// the high-32 directory and the u128-only final u64 bucket. `BTreeSet`, the
/// owned `select` sequence, and the wire view's independent rank path all
/// witness every emitted `(base, count)`.
#[test]
fn the_u64_bucket_histogram_matches_three_exhaustive_oracles() {
    fn expected_buckets(
        values: impl IntoIterator<Item = u64>,
        bucket_bits: u32,
    ) -> Vec<(u64, u64)> {
        let width = 1u128 << bucket_bits;
        let mut buckets: Vec<(u64, u64)> = Vec::new();
        for value in values {
            let base = ((u128::from(value) / width) * width) as u64;
            match buckets.last_mut() {
                Some((last_base, count)) if *last_base == base => *count += 1,
                _ => buckets.push((base, 1)),
            }
        }
        buckets
    }

    fn check_case(
        case: usize,
        values: impl IntoIterator<Item = u64>,
        bucket_bits: impl IntoIterator<Item = u32>,
    ) {
        let reference: BTreeSet<u64> = values.into_iter().collect();
        let ordered: Vec<u64> = reference.iter().copied().collect();
        let owned = Bitmosaic64::from_sorted(ordered.iter().copied());
        let bytes = owned.to_bytes();
        let view = Bitmosaic64View::open(&bytes).expect("open bucket oracle");

        let selected: Vec<u64> = (0..owned.len())
            .map(|ordinal| owned.select(ordinal).unwrap())
            .collect();
        assert_eq!(selected, ordered, "case {case}: select witness");

        for bits in bucket_bits {
            let expected = expected_buckets(reference.iter().copied(), bits);
            let selected = expected_buckets(selected.iter().copied(), bits);
            let mut actual = Vec::new();
            owned.bucket_counts(bits, |base, count| actual.push((base, count)));

            let width = 1u128 << bits;
            let domain_end = 1u128 << 64;
            let viewed: Vec<(u64, u64)> = expected
                .iter()
                .map(|&(base, _)| {
                    let hi = u128::from(base) + width;
                    let hi_rank = if hi >= domain_end {
                        view.len()
                    } else {
                        view.rank(hi as u64)
                    };
                    (base, hi_rank - view.rank(base))
                })
                .collect();

            assert_eq!(actual, expected, "case {case}: owned buckets/{bits}");
            assert_eq!(selected, expected, "case {case}: select buckets/{bits}");
            assert_eq!(viewed, expected, "case {case}: view buckets/{bits}");
        }
    }

    for mask in 0usize..1 << 8 {
        let values = (0u64..8).filter(|value| mask & (1 << *value as usize) != 0);
        check_case(mask, values, 1..=64);
    }
    assert_eq!(
        1usize << 8,
        256,
        "the low-domain subset sweep must be exhaustive"
    );

    check_case(
        256,
        [0, 1, (1u64 << 63) - 1, 1u64 << 63, u64::MAX - 1, u64::MAX],
        60..=64,
    );
    check_case(
        257,
        [0, (1u64 << 32) | 7, (3u64 << 32) | 9, (7u64 << 32) | 11],
        [32, 33, 34, 64],
    );
}

/// The four adaptors are generic over the protocol, not over `Bitmosaic`, so
/// the u64 tier gets them for free. That claim is the entire argument for
/// the trait, and it is checked here against `BTreeSet` on the tier that
/// previously had no set operations of any kind beyond `and_len`.
#[test]
fn the_u64_tier_gets_all_four_operations_from_the_same_adaptors() {
    let mut state = 0x5EEDu64;
    let mut draw = |n: usize, span: u64| -> Vec<u64> {
        let mut seen = BTreeSet::new();
        while seen.len() < n {
            // Straddle the 2^32 boundary so several forests are live.
            seen.insert(xorshift(&mut state) % span);
        }
        seen.into_iter().collect()
    };
    for (n, span) in [(1_000usize, 1u64 << 34), (5_000, 1 << 20), (200, 1 << 40)] {
        let (va, vb) = (draw(n, span), draw(n, span));
        let (a, b) = (
            Bitmosaic64::from_sorted(va.iter().copied()),
            Bitmosaic64::from_sorted(vb.iter().copied()),
        );
        let (sa, sb): (BTreeSet<u64>, BTreeSet<u64>) =
            (va.iter().copied().collect(), vb.iter().copied().collect());

        assert_eq!(
            a.and_range(&b).collect::<Vec<u64>>(),
            sa.intersection(&sb).copied().collect::<Vec<u64>>(),
            "u64 and_range"
        );
        assert_eq!(
            a.or_range(&b).collect::<Vec<u64>>(),
            sa.union(&sb).copied().collect::<Vec<u64>>(),
            "u64 or_range"
        );
        assert_eq!(
            a.xor_range(&b).collect::<Vec<u64>>(),
            sa.symmetric_difference(&sb).copied().collect::<Vec<u64>>(),
            "u64 xor_range"
        );
        assert_eq!(
            a.andnot_range(&b).collect::<Vec<u64>>(),
            sa.difference(&sb).copied().collect::<Vec<u64>>(),
            "u64 andnot_range"
        );
        // The fused counting kernel and the lazy walk must agree, on a tier
        // where only the former existed.
        assert_eq!(
            a.and_range(&b).count() as u64,
            a.and_len(&b),
            "u64 and count"
        );
    }
}

/// Adaptors nest, and a nested chain is still one merge with no buffer
/// between the stages. `(a ∩ b) ∪ (c \ b)` is checked against the same
/// expression over `BTreeSet`.
#[test]
fn adaptors_compose_without_a_buffer_between_stages() {
    let mut state = 0xABCDu64;
    let mut draw = |n: usize| -> Vec<u32> {
        let mut seen = BTreeSet::new();
        while seen.len() < n {
            seen.insert((xorshift(&mut state) % 40_000) as u32);
        }
        seen.into_iter().collect()
    };
    let (va, vb, vc) = (draw(6_000), draw(6_000), draw(6_000));
    let (a, b, c) = (
        Bitmosaic::from_sorted(va.iter().copied()),
        Bitmosaic::from_sorted(vb.iter().copied()),
        Bitmosaic::from_sorted(vc.iter().copied()),
    );
    let (sa, sb, sc): (BTreeSet<u32>, BTreeSet<u32>, BTreeSet<u32>) = (
        va.iter().copied().collect(),
        vb.iter().copied().collect(),
        vc.iter().copied().collect(),
    );

    let nested = OrRange::new(
        AndRange::leapfrog(a.range(), b.range()),
        AndNotRange::new(c.range(), b.range()),
    );
    let expected: Vec<u32> = {
        let left: BTreeSet<u32> = sa.intersection(&sb).copied().collect();
        let right: BTreeSet<u32> = sc.difference(&sb).copied().collect();
        left.union(&right).copied().collect()
    };
    assert_eq!(nested.collect::<Vec<u32>>(), expected, "(a∩b)∪(c\\b)");

    // XOR of a range with itself is empty, which is the degenerate case the
    // alignment loop must terminate on rather than spin.
    assert_eq!(XorRange::new(a.range(), a.range()).count(), 0, "a xor a");

    // A FOREIGN range — implements the protocol and nothing else — must
    // compose through `RangeIter` exactly like a native one. This is the
    // module's "composes over anything implementing the protocol" claim, and
    // it is only true because the bridge forwards BOTH surfaces.
    let flat: Vec<u32> = sb.iter().copied().collect();
    let foreign = RangeIter::new(SliceRange(&flat));
    assert_eq!(
        AndRange::leapfrog(a.range(), foreign).collect::<Vec<u32>>(),
        sa.intersection(&sb).copied().collect::<Vec<u32>>(),
        "native and foreign"
    );
    assert_eq!(
        AndNotRange::new(foreign, a.range()).collect::<Vec<u32>>(),
        sb.difference(&sa).copied().collect::<Vec<u32>>(),
        "foreign andnot native"
    );
    // Two foreign ranges, so the adaptor never sees an bitmosaic cursor at all.
    let flat_c: Vec<u32> = sc.iter().copied().collect();
    assert_eq!(
        OrRange::new(foreign, RangeIter::new(SliceRange(&flat_c))).collect::<Vec<u32>>(),
        sb.union(&sc).copied().collect::<Vec<u32>>(),
        "foreign or foreign"
    );
    // `save` on the bridge is still a copy: draining it leaves the original.
    let mut probe = foreign;
    let checkpoint = probe.save();
    probe.pop_front();
    assert_eq!(checkpoint.count(), flat.len(), "bridge save is a copy");
    assert_eq!(
        AndRange::leapfrog(a.range(), a.range()).count(),
        sa.len(),
        "a and a"
    );
}

// ── 2. the allocation gate ───────────────────────────────────────────────

/// The lazy adaptors allocate ZERO on a steady-state traversal.
///
/// "Steady state" means everything the walk needs already exists: the
/// forests are frozen, the ranges wrap them by value, and the consumer folds
/// into a scalar. Nothing here may reach the allocator — not construction,
/// not alignment, not a nested chain, and not the `Iterator` bridge.
///
/// A count, not a timing (handbook §4.4b): it is exact, reproducible, and
/// survives any rewrite of the code under it.
#[test]
fn lazy_ranges_allocate_nothing_on_a_steady_state_traversal() {
    let mut state = 0x1234_5678u64;
    let mut draw = |n: usize, universe: u32| -> Vec<u32> {
        let mut seen = BTreeSet::new();
        while seen.len() < n {
            seen.insert((xorshift(&mut state) % u64::from(universe)) as u32);
        }
        seen.into_iter().collect()
    };
    // Every container arm, so the zero is not a property of one cursor:
    // Words (dense), Cone (near-arithmetic), Array (sparse), Stride (pure).
    let dense = Bitmosaic::from_sorted(draw(8_000, 20_000));
    let sparse = Bitmosaic::from_sorted(draw(1_000, 13_000_000));
    let cone = Bitmosaic::from_sorted((0..1_000u32).map(|i| i * 61 + i % 5));
    let stride = Bitmosaic::from_sorted((0..20_000u32).map(|i| i * 7));
    let runs = Bitmosaic::from_sorted((0..4096).chain(32768..36864));
    let census = [&dense, &sparse, &cone, &stride, &runs].map(Bitmosaic::container_census);
    assert!(
        census.iter().any(|c| c.0 > 0),
        "no Stride in the fixture: {census:?}"
    );
    assert!(
        census.iter().any(|c| c.1 > 0),
        "no Cone in the fixture: {census:?}"
    );
    assert!(
        census.iter().any(|c| c.2 > 0),
        "no Array in the fixture: {census:?}"
    );
    assert!(
        census.iter().any(|c| c.3 > 0),
        "no Words in the fixture: {census:?}"
    );
    assert!(
        census.iter().any(|c| c.4 > 0),
        "no Runs in the fixture: {census:?}"
    );

    let big = Bitmosaic64::from_sorted(
        (0..4u64).flat_map(|h| (0..5_000u64).map(move |i| (h << 32) | (i * 13))),
    );
    let other = Bitmosaic64::from_sorted(
        (0..4u64).flat_map(|h| (0..5_000u64).map(move |i| (h << 32) | (i * 17))),
    );

    // The instrument must be able to fail. A zero from a counter that never
    // reports non-zero certifies nothing.
    let control = allocations(|| {
        let v: Vec<u32> = Vec::with_capacity(64);
        std::hint::black_box(&v);
    });
    assert!(
        control > 0,
        "counting allocator saw nothing on a real allocation"
    );

    // Built BEFORE the bracket, like every other fixture here: the gate is
    // about steady-state traversal, and a `Vec` built inside it would be a
    // real allocation that the assertion is not about.
    let flat: Vec<u32> = (0..20_000u32).step_by(3).collect();
    let mut sink = 0u64;
    let counted = allocations(|| {
        for a in [&dense, &sparse, &cone, &stride, &runs] {
            for b in [&dense, &sparse, &cone, &stride, &runs] {
                // Bare walk.
                sink = sink.wrapping_add(a.range().fold(0u64, |s, v| s.wrapping_add(u64::from(v))));
                let intersection = a.and_range(b);
                let union = a.or_range(b);
                let symmetric = a.xor_range(b);
                let difference = a.andnot_range(b);
                for (left, right) in [
                    intersection.sources(),
                    union.sources(),
                    symmetric.sources(),
                    difference.sources(),
                ] {
                    sink = sink.wrapping_add((left.len() ^ right.len()) as u64);
                }
                sink = sink.wrapping_add(intersection.count() as u64);
                sink = sink.wrapping_add(union.count() as u64);
                sink = sink.wrapping_add(symmetric.count() as u64);
                sink = sink.wrapping_add(difference.count() as u64);
                // A nested chain: no buffer materialises between stages.
                sink = sink.wrapping_add(
                    OrRange::new(
                        AndRange::leapfrog(a.range(), b.range()),
                        AndNotRange::new(b.range(), a.range()),
                    )
                    .count() as u64,
                );
                // Pull one element at a time, the shape `fold` cannot serve.
                let mut r = a.and_range(b);
                let mut pulled = 0u64;
                while !r.empty() {
                    let (left, right) = r.sources();
                    pulled = pulled.wrapping_add((left.len() ^ right.len()) as u64);
                    pulled = pulled.wrapping_add(u64::from(r.front()));
                    r.pop_front();
                }
                sink = sink.wrapping_add(pulled);
            }
        }
        // The u64 tier, whose walk is new.
        sink = sink.wrapping_add(big.iter().map(|v| v & 0xFFFF).sum::<u64>());
        sink = sink.wrapping_add(big.and_range(&other).count() as u64);
        sink = sink.wrapping_add(big.xor_range(&other).count() as u64);
        sink = sink.wrapping_add(big.count_range(17, (3u64 << 32) | 40_000));
        sink = sink.wrapping_add(big.predecessor((2u64 << 32) | 40_000).unwrap_or(0));
        sink = sink.wrapping_add(big.quantile(0.75).unwrap_or(0));
        sink = sink.wrapping_add(big.sample(0xA11C_E5EED).unwrap_or(0));
        sink = sink.wrapping_add(big.jaccard(&other).to_bits());
        big.bucket_counts(32, |base, count| {
            sink = sink.wrapping_add(base).wrapping_add(count);
        });
        big.bucket_counts(64, |base, count| {
            sink = sink.wrapping_add(base).wrapping_add(count);
        });
        // The FOREIGN bridge, inside the same bracket: `RangeIter` over a
        // consumer's own `Range`, fed to an adaptor. It is the one path that
        // still goes through `RangeIter` now that the native ranges are
        // Iterators, so without this the gate would no longer cover it.
        let bridged = RangeIter::new(SliceRange(&flat));
        sink = sink.wrapping_add(bridged.count() as u64);
        sink = sink.wrapping_add(AndRange::leapfrog(dense.range(), bridged).count() as u64);
    });
    std::hint::black_box(sink);
    assert_eq!(counted, 0, "lazy traversal allocated {counted} times");
}

// ── 3. the protocol's own contract ───────────────────────────────────────

/// `front` is idempotent and `pop_front` advances by exactly one — the
/// contract every adaptor's alignment loop is written against. If `front`
/// ever consumed, an aligned merge would skip elements and the oracle above
/// would fail in a way that pointed at the wrong code.
///
/// `save` is checked as what it claims to be: a copy of a positioned cursor,
/// so the original walks on unaffected. That is the operation `Iterator`
/// cannot express and the reason this is a range protocol.
#[test]
fn front_is_idempotent_and_save_checkpoints_a_cursor() {
    for (name, values) in shapes() {
        if values.is_empty() {
            continue;
        }
        let set = Bitmosaic::from_sorted(values.iter().copied());
        let expected: Vec<u32> = values
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();

        let mut range = set.range();
        let mut pulled = Vec::with_capacity(expected.len());
        while !range.empty() {
            let first = range.front();
            // Idempotent: three reads, no advance, same answer.
            assert_eq!(range.front(), first, "{name}: front is not idempotent");
            assert_eq!(range.front(), first, "{name}: front is not idempotent");
            pulled.push(first);
            range.pop_front();
        }
        assert_eq!(pulled, expected, "{name}: primitives vs input");

        // Checkpoint mid-walk, drain the copy, and confirm the original is
        // where it was left.
        let mut walker = set.range();
        let half = expected.len() / 2;
        for _ in 0..half {
            walker.pop_front();
        }
        let checkpoint = walker.save();
        let drained: Vec<u32> = checkpoint.collect();
        assert_eq!(drained, expected[half..], "{name}: saved copy");
        assert_eq!(
            walker.collect::<Vec<u32>>(),
            expected[half..],
            "{name}: original after save"
        );

        // Both surfaces of the same cursor produce the same sequence.
        assert_eq!(
            set.iter().collect::<Vec<u32>>(),
            expected,
            "{name}: Iterator"
        );
        // And the exact size hint is exact before anything is pulled.
        assert_eq!(
            set.iter().len(),
            expected.len(),
            "{name}: ExactSizeIterator::len"
        );
    }
}

/// An adaptor's `size_hint` must BOUND the truth, never contradict it — a
/// wrong upper bound is a `reserve` that under-allocates and a wrong lower
/// bound is a silent over-reservation. Checked against the realised count.
#[test]
fn adaptor_size_hints_bound_the_realised_count() {
    let all = shapes();
    for (na, va) in &all {
        let a = Bitmosaic::from_sorted(va.iter().copied());
        for (nb, vb) in &all {
            let b = Bitmosaic::from_sorted(vb.iter().copied());
            for (op, hint, realised) in [
                ("and", a.and_range(&b).size_hint(), a.and_range(&b).count()),
                ("or", a.or_range(&b).size_hint(), a.or_range(&b).count()),
                ("xor", a.xor_range(&b).size_hint(), a.xor_range(&b).count()),
                (
                    "andnot",
                    a.andnot_range(&b).size_hint(),
                    a.andnot_range(&b).count(),
                ),
            ] {
                let (lo, hi) = hint;
                assert!(lo <= realised, "{na} vs {nb}: {op} lower {lo} > {realised}");
                assert!(
                    hi.is_some_and(|h| h >= realised),
                    "{na} vs {nb}: {op} upper {hi:?} < {realised}"
                );
            }
        }
    }
}

// ── 3b. the seek primitive, and the two AndRange aligners ────────────────

/// A foreign range whose `seek` OVERSHOOTS by one member.
///
/// The negative half's instrument. `seek` is the one protocol method whose
/// override can be wrong while every other method stays right, and the
/// failure it produces is a SILENTLY SHORTER intersection — never a panic,
/// never a type error. So the oracle below is proved capable of reporting a
/// violation by being shown one: this range's answers must differ from the
/// stepping arm's, on the same corpus where a correct source's do not.
#[derive(Clone, Copy)]
struct OvershootingRange<'a>(&'a [u32]);

impl Range for OvershootingRange<'_> {
    type Item = u32;

    fn empty(&self) -> bool {
        self.0.is_empty()
    }

    fn front(&self) -> u32 {
        self.0[0]
    }

    fn pop_front(&mut self) {
        self.0 = &self.0[1..];
    }

    fn seek(&mut self, target: u32) {
        let at = self.0.partition_point(|value| *value < target);
        // The plant: one member past the first at-or-after `target`.
        self.0 = &self.0[(at + 1).min(self.0.len())..];
    }
}

/// Whether both `AndRange` arms yield the same sequence over one pair.
///
/// A predicate rather than an assertion, because the negative half needs to
/// observe it returning FALSE. An assertion that can only abort cannot be
/// shown to discriminate.
fn arms_agree<A, B>(left: (A, B), right: (A, B)) -> bool
where
    A: Range<Item = u32> + Iterator<Item = u32>,
    B: Range<Item = u32> + Iterator<Item = u32>,
{
    AndRange::leapfrog(left.0, left.1).collect::<Vec<u32>>()
        == AndRange::lockstep(right.0, right.1).collect::<Vec<u32>>()
}

/// `Range::seek` lands on the first member at or after its target, on every
/// source kind, and the two [`AndRange`] aligners are the SAME set operation.
///
/// Both halves matter and neither implies the other. The postcondition is
/// what the leapfrog aligner is written against; the arm equality is what
/// makes the aligner a measurable A/B rather than two different answers. The
/// wire cursors are included because theirs is the only ACCELERATED seek —
/// a directory walk plus at most one chunk-local `rank_below` — so an
/// override that mis-positions would show up here and nowhere else.
#[test]
fn seek_lands_at_or_after_its_target_and_both_and_arms_agree() {
    let all = shapes();
    // Every shape's owned set, mapped bytes and BTreeSet witness, built
    // ONCE. Rebuilding them per pair would make the oracle quadratic in
    // bytes and buy no coverage at all.
    let built: Vec<(&str, Bitmosaic, Vec<u8>, BTreeSet<u32>)> = all
        .iter()
        .map(|(name, values)| {
            (
                *name,
                Bitmosaic::from_sorted(values.iter().copied()),
                Bitmosaic64::from_sorted(values.iter().map(|&v| u64::from(v))).to_bytes(),
                values.iter().copied().collect(),
            )
        })
        .collect();
    let views: Vec<Bitmosaic64View<'_>> = built
        .iter()
        .map(|(_, _, bytes, _)| Bitmosaic64View::open(bytes).expect("open the u64 view"))
        .collect();

    for (index, (name, values)) in all.iter().enumerate() {
        let (_, set, _, oracle) = &built[index];
        let view = views[index];
        let Some(&last) = values.last() else { continue };

        // Targets: a SAMPLE of members with both neighbours, plus both ends.
        // Every member would be O(n) targets each costing an O(n) stepping
        // seek on the owned tier, which is a quadratic oracle and not a
        // stronger one — the boundaries a directory walk can get wrong are
        // "just below a member", "a member" and "just above a member", and a
        // stride visits all three inside every container kind.
        let stride = (values.len() / 16).max(1);
        let mut targets: Vec<u32> = vec![0, last, last.saturating_add(1_000)];
        for &value in values.iter().step_by(stride) {
            targets.push(value.saturating_sub(1));
            targets.push(value);
            targets.push(value.saturating_add(1));
        }

        for &target in &targets {
            let expected = oracle.range(target..).next().copied();

            let mut owned = set.range();
            owned.seek(target);
            assert_eq!(
                (!Range::empty(&owned)).then(|| Range::front(&owned)),
                expected,
                "{name}: owned forest seek({target})"
            );

            let mut held = view.range();
            held.seek(u64::from(target));
            assert_eq!(
                (!Range::empty(&held)).then(|| Range::front(&held)),
                expected.map(u64::from),
                "{name}: mapped u64 view seek({target})"
            );

            // Monotone: a seek behind the head must not rewind it.
            let before = (!Range::empty(&held)).then(|| Range::front(&held));
            held.seek(0);
            assert_eq!(
                (!Range::empty(&held)).then(|| Range::front(&held)),
                before,
                "{name}: seek(0) rewound a cursor at {target}"
            );
        }

        for (other, (other_name, _, _, other_oracle)) in built.iter().enumerate() {
            let truth: Vec<u32> = oracle.intersection(other_oracle).copied().collect();
            let other_set = &built[other].1;
            assert!(
                arms_agree(
                    (set.range(), other_set.range()),
                    (set.range(), other_set.range())
                ),
                "{name} vs {other_name}: the two aligners disagree"
            );
            assert_eq!(
                AndRange::leapfrog(set.range(), other_set.range()).collect::<Vec<u32>>(),
                truth,
                "{name} vs {other_name}: leapfrog arm vs BTreeSet"
            );

            // The mapped pair, where the ACCELERATED seek actually fires.
            let truth64: Vec<u64> = truth.iter().map(|&v| u64::from(v)).collect();
            assert_eq!(
                AndRange::leapfrog(view.range(), views[other].range()).collect::<Vec<u64>>(),
                truth64,
                "{name} vs {other_name}: mapped leapfrog arm"
            );
            assert_eq!(
                AndRange::lockstep(view.range(), views[other].range()).collect::<Vec<u64>>(),
                truth64,
                "{name} vs {other_name}: mapped stepping arm"
            );
            // Three-way, so the OUTER node's `seek` drives the inner node's
            // — the composition an unoverridden `AndRange::seek` would leave
            // stepping.
            let third = views[(index + other + 1) % views.len()];
            assert_eq!(
                AndRange::leapfrog(
                    AndRange::leapfrog(view.range(), views[other].range()),
                    third.range()
                )
                .collect::<Vec<u64>>(),
                AndRange::lockstep(
                    AndRange::lockstep(view.range(), views[other].range()),
                    third.range()
                )
                .collect::<Vec<u64>>(),
                "{name} vs {other_name}: three-way arms disagree"
            );
        }
    }

    // ── the negative half ────────────────────────────────────────────────
    //
    // Same predicate, one planted defect: a foreign `seek` that lands one
    // member too far. The leapfrog arm consults it and the stepping arm does
    // not, so the two answers MUST part company. A green result here would
    // mean the oracle above cannot see a mis-positioned seek at all — which
    // is not hypothetical: this assertion is what caught `RangeIter`
    // answering `seek` with the stepping default instead of forwarding it.
    //
    // The two operands INTERLEAVE rather than nest. A subset pair leaves one
    // side never behind, so the plant's `seek` is never called and the plant
    // reads clean: multiples of 4 against multiples of 6 puts each side
    // ahead in turn, and their intersection is the multiples of 12.
    let owned: Vec<u32> = (0..50_000u32).map(|i| i * 4).collect();
    let foreign: Vec<u32> = (0..33_333u32).map(|i| i * 6).collect();
    let set = Bitmosaic::from_sorted(owned.iter().copied());
    let expected: Vec<u32> = (0..16_667u32)
        .map(|i| i * 12)
        .filter(|v| *v <= owned[owned.len() - 1] && *v <= foreign[foreign.len() - 1])
        .collect();
    assert_eq!(
        AndRange::lockstep(set.range(), RangeIter::new(SliceRange(&foreign))).collect::<Vec<u32>>(),
        expected,
        "the negative half's control pair must intersect on the multiples of 12"
    );
    assert!(
        arms_agree(
            (set.range(), RangeIter::new(SliceRange(&foreign))),
            (set.range(), RangeIter::new(SliceRange(&foreign))),
        ),
        "control: a correct foreign seek must agree with the stepping arm"
    );
    assert!(
        !arms_agree(
            (set.range(), RangeIter::new(OvershootingRange(&foreign))),
            (set.range(), RangeIter::new(OvershootingRange(&foreign))),
        ),
        "NEGATIVE HALF: a seek that overshoots by one went undetected"
    );
}

// ── 4. the builder's missing accessors ───────────────────────────────────

/// `BitmosaicBuilder::contains`, and `insert` reporting novelty: the two
/// accessors `README.md` named as the gap. A caller can now use the builder
/// as a mutable membership set without freezing, and freezing afterwards
/// still costs nothing extra.
#[test]
fn the_builder_answers_membership_without_freezing() {
    let mut state = 0xFEED_FACEu64;
    let mut builder = BitmosaicBuilder::new();
    let mut reference = BTreeSet::new();

    // Values drawn with deliberate collisions, spread over several chunks,
    // so both the fresh and the repeat arm of `insert` run many times.
    let mut fresh = 0usize;
    for _ in 0..20_000 {
        let v = (xorshift(&mut state) % 5_000) as u32 * 37;
        let before = builder.contains(v);
        assert_eq!(
            before,
            reference.contains(&v),
            "contains before insert of {v}"
        );
        let was_new = builder.insert(v);
        assert_eq!(was_new, reference.insert(v), "insert novelty for {v}");
        assert_ne!(
            was_new, before,
            "insert novelty contradicts contains for {v}"
        );
        assert!(builder.contains(v), "contains after insert of {v}");
        fresh += usize::from(was_new);
    }
    assert!(fresh > 100, "corpus never exercised the fresh arm: {fresh}");
    assert!(
        fresh < 20_000,
        "corpus never exercised the repeat arm: {fresh}"
    );

    // Absent values, including in occupied chunks and in chunks that were
    // never touched at all.
    for v in [1u32, 2, 3, 1_000_001, u32::MAX] {
        assert_eq!(builder.contains(v), reference.contains(&v), "absent {v}");
    }

    // The staged answer is the frozen answer.
    let frozen = builder.freeze();
    assert_eq!(
        frozen.len() as usize,
        reference.len(),
        "cardinality survives freeze"
    );
    assert_eq!(
        frozen.iter().collect::<Vec<u32>>(),
        reference.iter().copied().collect::<Vec<u32>>()
    );

    // `clear_retaining` resets membership without releasing the arena, so a
    // second epoch must start empty.
    let mut epoch = BitmosaicBuilder::new();
    assert!(epoch.insert(7));
    epoch.clear_retaining();
    assert!(!epoch.contains(7), "clear_retaining left a member staged");
    assert!(epoch.insert(7), "value was not new after clear_retaining");
}

// ── 5. the staging ladder ────────────────────────────────────────────────

/// The builder stages a chunk inline until it earns a dense plane. Before
/// that arm existed every touched chunk took 8 KB on first contact, which
/// cost a scattered sparse set two orders of magnitude more memory than it
/// held and lost a measured conversion outright (+246.51% against
/// `HashSet`, `examples/g216_visited_lifecycle.rs`). These tests pin the
/// ladder's correctness independently of any caller: the arm a chunk lands
/// in must never be observable in an answer.
///
/// Every chunk cardinality across the inline/plane boundary agrees with
/// roaring. A single cardinality tests one arm; the walk tests the
/// transition, which is the only place the ladder can be wrong.
#[test]
fn staging_ladder_agrees_with_roaring_across_the_promotion_boundary() {
    for members in 1..=40u32 {
        let mut builder = BitmosaicBuilder::new();
        let mut oracle = RoaringBitmap::new();
        // Three chunks so the per-chunk decision is exercised in parallel,
        // with distinct spacing so one chunk's layout cannot mask another's.
        for chunk in 0..3u32 {
            for index in 0..members {
                let value = chunk * 65_536 + index * (chunk + 1) * 7 + chunk;
                assert_eq!(
                    builder.insert(value),
                    oracle.insert(value),
                    "novelty diverged at {value} (members {members})"
                );
            }
        }
        for probe in 0..3 * 65_536u32 {
            assert_eq!(
                builder.contains(probe),
                oracle.contains(probe),
                "staged membership diverged at {probe} (members {members})"
            );
        }
        let frozen = builder.freeze();
        assert_eq!(
            frozen.iter().collect::<Vec<u32>>(),
            oracle.iter().collect::<Vec<u32>>(),
            "frozen contents diverged (members {members})"
        );
    }
}

/// The inline arm keeps its members ordered by shifting on insert, so the
/// same set inserted in different orders must freeze identically. An
/// off-by-one in that shift corrupts only some orders, which a single
/// ascending corpus would never reveal.
#[test]
fn staging_is_insertion_order_independent() {
    let mut state = 0x0057_ACE1u64;
    for members in [3usize, 31, 32, 33, 100] {
        let values: Vec<u32> = (0..members).map(|index| (index as u32) * 11 + 5).collect();

        let ascending = {
            let mut builder = BitmosaicBuilder::new();
            builder.extend(values.iter().copied());
            builder.freeze()
        };
        let descending = {
            let mut builder = BitmosaicBuilder::new();
            builder.extend(values.iter().rev().copied());
            builder.freeze()
        };
        let shuffled = {
            let mut shuffled = values.clone();
            for index in (1..shuffled.len()).rev() {
                let swap = (xorshift(&mut state) as usize) % (index + 1);
                shuffled.swap(index, swap);
            }
            let mut builder = BitmosaicBuilder::new();
            // Re-insert every value twice: the repeat arm must not disturb
            // the order the fresh arm established.
            builder.extend(shuffled.iter().copied());
            builder.extend(shuffled.iter().copied());
            builder.freeze()
        };

        let expected: Vec<u32> = values.clone();
        for (label, frozen) in [
            ("ascending", &ascending),
            ("descending", &descending),
            ("shuffled", &shuffled),
        ] {
            assert_eq!(
                frozen.iter().collect::<Vec<u32>>(),
                expected,
                "{label} insertion order changed the frozen set (members {members})"
            );
        }
    }
}

/// `blit_into` must OR exactly the staged members into a caller's flat
/// plane, from both arms, at the right word offsets, and must stop at the
/// destination's end rather than writing past it.
#[test]
fn blit_into_matches_the_staged_set_from_both_arms() {
    // 3 members lands the chunk inline; 200 forces the plane. Chunk 2 is
    // beyond a 2-chunk destination, so it must be dropped, not wrapped.
    for members in [3u32, 200] {
        let mut builder = BitmosaicBuilder::new();
        let mut oracle = RoaringBitmap::new();
        for chunk in 0..3u32 {
            for index in 0..members {
                let value = chunk * 65_536 + index * 13;
                builder.insert(value);
                oracle.insert(value);
            }
        }

        // Destination covers chunks 0 and 1 only.
        let words = 2 * 65_536 / 64;
        let mut plane = vec![0u64; words];
        // Pre-set a bit the builder does not hold: the blit ORs, it does
        // not overwrite.
        let sentinel = 65_536 + 5;
        plane[sentinel / 64] |= 1u64 << (sentinel % 64);
        builder.blit_into(&mut plane);

        for value in 0..(2 * 65_536u32) {
            let position = value as usize;
            let present = plane[position / 64] >> (position % 64) & 1 == 1;
            let expected = oracle.contains(value) || position == sentinel;
            assert_eq!(
                present, expected,
                "blit diverged at {value} (members {members})"
            );
        }

        // A destination shorter than the first chunk truncates cleanly.
        let mut tiny = vec![0u64; 4];
        builder.blit_into(&mut tiny);
        for value in 0..256u32 {
            let position = value as usize;
            assert_eq!(
                tiny[position / 64] >> (position % 64) & 1 == 1,
                oracle.contains(value),
                "truncated blit diverged at {value} (members {members})"
            );
        }
    }
}

/// `clear_retaining` must empty a chunk in either arm. A promoted chunk
/// keeps its plane (that is the arena contract) and an inline chunk keeps
/// its slot; neither may keep a member.
#[test]
fn clear_retaining_empties_both_arms() {
    let mut builder = BitmosaicBuilder::new();
    // Chunk 0 inline, chunk 1 promoted.
    for value in 0..3u32 {
        builder.insert(value);
    }
    for index in 0..200u32 {
        builder.insert(65_536 + index);
    }
    builder.clear_retaining();

    for value in [0u32, 1, 2, 65_536, 65_600, 65_735] {
        assert!(
            !builder.contains(value),
            "clear_retaining left {value} staged"
        );
        assert!(builder.insert(value), "{value} was not new after clear");
    }
    assert_eq!(builder.freeze().len(), 6);
}
