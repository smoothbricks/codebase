//! Pure-core equivalence and witness tests for bitmosaic fast lanes:
//! - O(1) `open_admitted` (`BitmosaicView`, `Bitmosaic64View`)
//! - L0 single-descent `rank_if_present` (owned & borrowed, u32 & u64, EF & Forest)
//! - `Bitmosaic::combine` fast paths (`Words×Words`, `Array×Array`, `Stride×Stride`)
//! - Fused k-way `and_len_three` / `and_len_k`
//! - Chunk-level `BitmosaicView::and_into` & cursor-advancing EF `and_len`
//! - `Range::seek` on `AndNotRange` / `OrRange` / `XorRange` / `ContainerRange` / `ForestRange` / `Forest64Range`
//! - `Bitmosaic64View` binary-search directory ops & skew-adaptive `and_len`
//! - `Bitmosaic64::intersect_sorted` SIMD + galloping

use std::collections::BTreeSet;

use bitmosaic::{
    AndNotRange, Bitmosaic, Bitmosaic64, Bitmosaic64View, BitmosaicView, Dir64V2View,
    EMPTY_U32_IMAGE, EMPTY_U64_IMAGE, OrRange, Range, XorRange,
};

fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn sorted_unique_u32(mut v: Vec<u32>) -> Vec<u32> {
    v.sort_unstable();
    v.dedup();
    v
}

fn sorted_unique_u64(mut v: Vec<u64>) -> Vec<u64> {
    v.sort_unstable();
    v.dedup();
    v
}

/// Build a corpus covering every container kind (`Words`, `Array`, `Stride`,
/// `Cone`, `Runs`), multi-chunk slot-plane forests (>64 non-contiguous chunks),
/// Elias-Fano roots, singleton, and empty.
fn u32_corpus() -> Vec<Vec<u32>> {
    let mut rng = 0xFA57_1A4E_5EED_0001u64;
    let empty = vec![];
    let single = vec![42];
    // Stride (step 1 and step 7).
    let stride1: Vec<u32> = (100..2_500).collect();
    let stride7: Vec<u32> = (0..500).map(|i| 11 + i * 7).collect();
    // Cone (nearly linear with bounded jitter <= 5).
    let cone: Vec<u32> = sorted_unique_u32(
        (0..400u32)
            .map(|i| 500 + i * 19 + (xorshift(&mut rng) % 5) as u32)
            .collect(),
    );
    // Array (sparse irregular inside one chunk).
    let array: Vec<u32> = sorted_unique_u32(
        (0..300)
            .map(|_| (xorshift(&mut rng) % 65_536) as u32)
            .collect(),
    );
    // Words (dense irregular > 4096 inside one chunk).
    let words: Vec<u32> = sorted_unique_u32(
        (0..14_000)
            .map(|_| (xorshift(&mut rng) % 65_536) as u32)
            .collect(),
    );
    // Runs (many disjoint intervals summing > 4096).
    let mut runs = Vec::new();
    for block in 0..40u32 {
        let base = block * 1_500 + 10;
        runs.extend(base..base + 200);
    }
    // EF root (sparse across wide universe: 120 values across 80+ chunks).
    let ef: Vec<u32> = sorted_unique_u32(
        (0..120)
            .map(|i| i * 110_003 + (xorshift(&mut rng) % 1000) as u32)
            .collect(),
    );
    // Multi-chunk forest with slot plane (> 64 non-contiguous chunks, dense enough to stay Forest).
    let mut slot_forest = Vec::new();
    for c in 0..80u32 {
        let high = (c * 3) << 16;
        for j in 0..60u32 {
            slot_forest.push(high | (j * 500 + (c % 7)));
        }
    }
    // Mixed multi-chunk: Words + Array + Stride + Cone + Runs in separate chunks.
    let mut mixed = Vec::new();
    mixed.extend(words.iter().copied());
    mixed.extend(array.iter().map(|&v| (1 << 16) | v));
    mixed.extend(stride7.iter().map(|&v| (2 << 16) | v));
    mixed.extend(cone.iter().map(|&v| (3 << 16) | v));
    mixed.extend(runs.iter().map(|&v| (4 << 16) | v));

    vec![
        empty,
        single,
        stride1,
        stride7,
        cone,
        array,
        words,
        runs,
        ef,
        slot_forest,
        mixed,
    ]
}

#[test]
fn open_admitted_u32_and_u64_match_open_and_refuse_malformed() {
    for values in u32_corpus() {
        let set = Bitmosaic::from_sorted(values.iter().copied());
        let bytes = set.to_bytes();
        let full = BitmosaicView::open(&bytes).expect("open valid u32");
        let fast = BitmosaicView::open_admitted(&bytes).expect("open_admitted valid u32");
        assert_eq!(fast.len(), full.len());
        assert_eq!(fast.serialized_len(), full.serialized_len());
        assert_eq!(fast.is_elias_fano(), full.is_elias_fano());
        assert_eq!(fast.has_slot_plane(), full.has_slot_plane());
        let mut got = Vec::new();
        fast.for_each(|v| got.push(v));
        assert_eq!(got, values);
        for &v in values.iter().step_by((values.len() / 25).max(1)) {
            assert!(fast.contains(v));
            assert_eq!(fast.rank(v), full.rank(v));
            assert_eq!(fast.rank_if_present(v), Some(full.rank(v)));
        }
        // Also test forest-forced encoding when EF won.
        if full.is_elias_fano() {
            let fbytes = set.to_forest_bytes();
            let ffull = BitmosaicView::open(&fbytes).unwrap();
            let ffast = BitmosaicView::open_admitted(&fbytes).unwrap();
            assert_eq!(ffast.len(), ffull.len());
            assert_eq!(ffast.has_slot_plane(), ffull.has_slot_plane());
        }
    }

    // u64: 0, 1, 2, 3, and 40 high-32 forests (mixing EF and Forest children).
    let u64_cases: Vec<Vec<u64>> = vec![
        vec![],
        (0..1_000u64).map(|i| i * 3).collect(),
        vec![10, 20, (1u64 << 32) | 5, (1u64 << 32) | 99],
        vec![
            1,
            2,
            (2u64 << 32) | 100,
            (2u64 << 32) | 200,
            (5u64 << 32) | 7,
        ],
        {
            let mut vals = Vec::new();
            for h in 0..40u64 {
                let hi = (h * 3 + 1) << 32;
                if h % 2 == 0 {
                    // Sparse EF child.
                    for k in 0..25u64 {
                        vals.push(hi | (k * 90_001 + 7));
                    }
                } else {
                    // Dense Forest child.
                    for k in 0..100u64 {
                        vals.push(hi | (k * 2));
                    }
                }
            }
            vals
        },
    ];
    for values in u64_cases {
        let set = Bitmosaic64::from_sorted(values.iter().copied());
        let bytes = set.to_bytes();
        let full = Bitmosaic64View::open(&bytes).expect("open valid u64");
        let fast = Bitmosaic64View::open_admitted(&bytes).expect("open_admitted valid u64");
        assert_eq!(fast.len(), full.len());
        assert_eq!(fast.serialized_len(), full.serialized_len());
        let mut got = Vec::new();
        fast.for_each(|v| got.push(v));
        assert_eq!(got, values);
        for &v in values.iter().step_by((values.len() / 20).max(1)) {
            assert_eq!(fast.contains(v), full.contains(v));
            assert_eq!(fast.rank(v), full.rank(v));
            assert_eq!(fast.rank_if_present(v), Some(full.rank(v)));
        }
        for k in [0, values.len() / 2, values.len().saturating_sub(1)] {
            assert_eq!(fast.select(k as u64), full.select(k as u64));
        }
    }

    // Refusal safety: truncated, wrong width, bad magic, empty.
    assert!(BitmosaicView::open_admitted(&[]).is_none());
    assert!(BitmosaicView::open_admitted(b"BMS").is_none());
    assert!(BitmosaicView::open_admitted(b"BAD\x0c").is_none());
    assert!(BitmosaicView::open_admitted(&EMPTY_U64_IMAGE).is_none());
    assert!(Bitmosaic64View::open_admitted(&[]).is_none());
    assert!(Bitmosaic64View::open_admitted(b"BMS").is_none());
    assert!(Bitmosaic64View::open_admitted(&EMPTY_U32_IMAGE).is_none());
    let sample32 = Bitmosaic::from_sorted(0..500u32).to_bytes();
    for cut in 0..sample32.len() {
        assert!(BitmosaicView::open_admitted(&sample32[..cut]).is_none());
    }
    let sample64 = Bitmosaic64::from_sorted([1u64, 1 << 32, 2 << 32, 3 << 32]).to_bytes();
    for cut in 0..sample64.len() {
        assert!(Bitmosaic64View::open_admitted(&sample64[..cut]).is_none());
    }
}

#[test]
fn rank_if_present_matches_contains_and_rank_everywhere() {
    for values in u32_corpus() {
        let set = Bitmosaic::from_sorted(values.iter().copied());
        let bytes = set.to_bytes();
        let view = BitmosaicView::open_admitted(&bytes).unwrap();
        let mut probes: Vec<u32> = vec![0, 1, 65_535, 65_536, u32::MAX - 1, u32::MAX];
        for &v in values.iter().step_by((values.len() / 60).max(1)) {
            probes.push(v.saturating_sub(1));
            probes.push(v);
            probes.push(v.saturating_add(1));
        }
        for v in probes {
            let want = set.contains(v).then(|| set.rank(v));
            assert_eq!(set.rank_if_present(v), want, "owned u32 v={v}");
            assert_eq!(view.rank_if_present(v), want, "view u32 v={v}");
        }
    }

    let mut vals64 = Vec::new();
    for h in [0u64, 1, 4, 25, 100] {
        for k in 0..80u64 {
            vals64.push((h << 32) | (k * 13 + 5));
        }
    }
    let set64 = Bitmosaic64::from_sorted(vals64.iter().copied());
    let bytes64 = set64.to_bytes();
    let view64 = Bitmosaic64View::open_admitted(&bytes64).unwrap();
    let mut probes64 = vec![0u64, 1, u64::from(u32::MAX), 1 << 32, u64::MAX];
    for &v in vals64.iter().step_by(7) {
        probes64.push(v - 1);
        probes64.push(v);
        probes64.push(v + 1);
    }
    for v in probes64 {
        let want = set64.contains(v).then(|| set64.rank(v));
        assert_eq!(set64.rank_if_present(v), want, "owned u64 v={v}");
        assert_eq!(view64.rank_if_present(v), want, "view u64 v={v}");
    }
}

#[test]
fn combine_fast_paths_match_btreeset_and_canonical_bytes() {
    let mut rng = 0xC03B_14E5_9876_5432u64;
    let words_a = Bitmosaic::from_sorted(sorted_unique_u32(
        (0..15_000)
            .map(|_| (xorshift(&mut rng) % 65_536) as u32)
            .collect(),
    ));
    let words_b = Bitmosaic::from_sorted(sorted_unique_u32(
        (0..15_000)
            .map(|_| (xorshift(&mut rng) % 65_536) as u32)
            .collect(),
    ));
    let words_low = Bitmosaic::from_sorted((0..5_000u32).map(|i| i * 2 + (i % 3 == 0) as u32));
    let arr_small = Bitmosaic::from_sorted(vec![5u32, 100, 1_000, 10_000, 50_000]);
    let arr_med = Bitmosaic::from_sorted(sorted_unique_u32(
        (0..2_500)
            .map(|_| (xorshift(&mut rng) % 65_536) as u32)
            .collect(),
    ));
    let arr_big_a = Bitmosaic::from_sorted(sorted_unique_u32(
        (0..3_200)
            .map(|_| (xorshift(&mut rng) % 65_536) as u32)
            .collect(),
    ));
    let arr_big_b = Bitmosaic::from_sorted(sorted_unique_u32(
        (0..3_200)
            .map(|_| (xorshift(&mut rng) % 65_536) as u32)
            .collect(),
    ));
    // Stride × Stride cases: multi-hit, single-hit with lcm > u16::MAX, disjoint.
    let stride_a = Bitmosaic::from_sorted((0..400u32).map(|i| 10 + i * 6));
    let stride_b = Bitmosaic::from_sorted((0..300u32).map(|i| 4 + i * 10));
    let stride_coprime_a = Bitmosaic::from_sorted((0..200u32).map(|i| 100 + i * 257));
    let stride_coprime_b = Bitmosaic::from_sorted((0..200u32).map(|i| 100 + i * 259));
    let stride_disjoint = Bitmosaic::from_sorted((0..300u32).map(|i| 1 + i * 6));
    // Multi-chunk skewed forest.
    let skewed_small = Bitmosaic::from_sorted(vec![50u32, (25 << 16) | 10, (70 << 16) | 99]);
    let mut wide_vals = Vec::new();
    for c in 0..80u32 {
        wide_vals.push((c << 16) | 10);
        wide_vals.push((c << 16) | 50);
    }
    let skewed_large = Bitmosaic::from_sorted(wide_vals);

    let pairs: Vec<(&Bitmosaic, &Bitmosaic)> = vec![
        (&words_a, &words_b),
        (&words_a, &words_low),
        (&arr_small, &arr_med),
        (&arr_big_a, &arr_big_b), // OR/XOR crosses ARRAY_MAX!
        (&arr_med, &words_a),
        (&stride_a, &stride_b),
        (&stride_coprime_a, &stride_coprime_b), // lcm = 66563 > u16::MAX, count = 1
        (&stride_a, &stride_disjoint),
        (&skewed_small, &skewed_large),
    ];

    for (a, b) in pairs {
        let sa: BTreeSet<u32> = a.iter().collect();
        let sb: BTreeSet<u32> = b.iter().collect();

        let check = |got: Bitmosaic, want: Vec<u32>, op: &str| {
            let got_vec: Vec<u32> = got.iter().collect();
            assert_eq!(got_vec, want, "{op} members mismatch");
            let canonical = Bitmosaic::from_sorted(want.iter().copied());
            assert_eq!(
                got.to_bytes(),
                canonical.to_bytes(),
                "{op} canonical bytes mismatch"
            );
        };

        check(a.and(b), sa.intersection(&sb).copied().collect(), "and");
        check(a.or(b), sa.union(&sb).copied().collect(), "or");
        check(
            a.xor(b),
            sa.symmetric_difference(&sb).copied().collect(),
            "xor",
        );
        check(a.andnot(b), sa.difference(&sb).copied().collect(), "andnot");
    }
}

#[test]
fn and_into_and_kway_match_btreeset_across_roots_and_containers() {
    let corpus = u32_corpus();
    let sets: Vec<Bitmosaic> = corpus
        .iter()
        .map(|v| Bitmosaic::from_sorted(v.iter().copied()))
        .collect();
    let blobs: Vec<Vec<u8>> = sets.iter().map(Bitmosaic::to_bytes).collect();
    let views: Vec<BitmosaicView<'_>> = blobs
        .iter()
        .map(|b| BitmosaicView::open_admitted(b).unwrap())
        .collect();
    let oracles: Vec<BTreeSet<u32>> = corpus.iter().map(|v| v.iter().copied().collect()).collect();

    // Pairwise and_len + and_into.
    for i in 0..views.len() {
        for j in 0..views.len() {
            let want: Vec<u32> = oracles[i].intersection(&oracles[j]).copied().collect();
            assert_eq!(
                views[i].and_len(&views[j]),
                want.len() as u64,
                "and_len ({i},{j})"
            );
            let mut buf = vec![0u32; want.len()];
            let written = views[i].and_into(&views[j], &mut buf);
            assert_eq!(written, want.len());
            assert_eq!(buf, want, "and_into ({i},{j})");
        }
    }

    // 3-way and k-way (0, 1, 2, 3, 4, 5, 9) over Words, Stride, EF, mixed.
    let mut rng = 0x3333_4444_5555_6666u64;
    let dense_blobs: Vec<Vec<u8>> = (0..9)
        .map(|k| {
            let vals = sorted_unique_u32(
                (0..18_000)
                    .map(|_| ((xorshift(&mut rng) + k * 7) % 65_536) as u32)
                    .collect(),
            );
            Bitmosaic::from_sorted(vals).to_bytes()
        })
        .collect();
    let dense_views: Vec<BitmosaicView<'_>> = dense_blobs
        .iter()
        .map(|b| BitmosaicView::open_admitted(b).unwrap())
        .collect();
    let dense_oracles: Vec<BTreeSet<u32>> = dense_views
        .iter()
        .map(|v| {
            let mut s = BTreeSet::new();
            v.for_each(|x| {
                s.insert(x);
            });
            s
        })
        .collect();

    assert_eq!(BitmosaicView::and_len_k(&[]), 0);
    assert_eq!(
        BitmosaicView::and_len_k(&dense_views[..1]),
        dense_views[0].len()
    );
    for k in [2usize, 3, 4, 5, 9] {
        let mut inter = dense_oracles[0].clone();
        for o in &dense_oracles[1..k] {
            inter = inter.intersection(o).copied().collect();
        }
        assert_eq!(
            BitmosaicView::and_len_k(&dense_views[..k]),
            inter.len() as u64,
            "dense and_len_k k={k}"
        );
    }
    assert_eq!(
        dense_views[0].and_len_three(&dense_views[1], &dense_views[2]),
        BitmosaicView::and_len_k(&dense_views[..3])
    );

    // Mixed roots (EF + Stride + Words).
    for a in [2usize, 6, 8, 10] {
        for b in [3usize, 6, 8, 10] {
            for c in [4usize, 7, 8, 10] {
                let want = oracles[a]
                    .iter()
                    .filter(|v| oracles[b].contains(v) && oracles[c].contains(v))
                    .count() as u64;
                assert_eq!(
                    views[a].and_len_three(&views[b], &views[c]),
                    want,
                    "mixed and_len_three ({a},{b},{c})"
                );
            }
        }
    }

    // u64 k-way + skewed and_len + binary-search directory ops (200 forests).
    let mut many_vals = Vec::new();
    for h in 0..200u64 {
        let hi = (h * 5 + 3) << 32;
        for k in 0..10u64 {
            many_vals.push(hi | (k * 100));
        }
    }
    let few_vals: Vec<u64> = vec![
        (3u64 << 32) | 200,
        (503u64 << 32) | 500,
        (998u64 << 32) | 900,
    ];
    let mid_vals: Vec<u64> = (0..50u64)
        .flat_map(|h| {
            let hi = (h * 20 + 3) << 32;
            (0..5u64).map(move |k| hi | (k * 200))
        })
        .collect();
    let s_many = Bitmosaic64::from_sorted(many_vals.iter().copied());
    let s_few = Bitmosaic64::from_sorted(few_vals.iter().copied());
    let s_mid = Bitmosaic64::from_sorted(mid_vals.iter().copied());
    let b_many = s_many.to_bytes();
    let b_few = s_few.to_bytes();
    let b_mid = s_mid.to_bytes();
    let v_many = Bitmosaic64View::open_admitted(&b_many).unwrap();
    let v_few = Bitmosaic64View::open_admitted(&b_few).unwrap();
    let v_mid = Bitmosaic64View::open_admitted(&b_mid).unwrap();
    let o_many: BTreeSet<u64> = many_vals.iter().copied().collect();
    let o_few: BTreeSet<u64> = few_vals.iter().copied().collect();
    let o_mid: BTreeSet<u64> = mid_vals.iter().copied().collect();

    assert_eq!(
        s_many.and_len(&s_few),
        o_many.intersection(&o_few).count() as u64
    );
    assert_eq!(
        v_many.and_len(&v_few),
        o_many.intersection(&o_few).count() as u64
    );
    assert_eq!(
        v_few.and_len(&v_many),
        o_many.intersection(&o_few).count() as u64
    );
    let want3 = o_many
        .iter()
        .filter(|x| o_few.contains(x) && o_mid.contains(x))
        .count() as u64;
    assert_eq!(v_many.and_len_three(&v_few, &v_mid), want3);
    assert_eq!(
        Bitmosaic64View::and_len_k(&[v_many, v_few, v_mid, v_many]),
        want3
    );
    // Verify binary-search locate/rank/select over 200 non-contiguous forests.
    for (idx, &v) in many_vals.iter().enumerate().step_by(37) {
        assert!(v_many.contains(v));
        assert_eq!(v_many.rank(v), idx as u64);
        assert_eq!(v_many.rank_if_present(v), Some(idx as u64));
        assert_eq!(v_many.select(idx as u64), Some(v));
        assert!(!v_many.contains(v + 1));
        assert_eq!(v_many.rank(v + 1), idx as u64 + 1);
    }
}

/// Counting `Range` witness: proves `AndNotRange::align()` invokes `seek()`
/// rather than stepping `b.pop_front()` over every skipped element.
#[derive(Clone, Copy)]
struct CountingSliceRange<'a> {
    slice: &'a [u32],
    pops: &'a core::cell::Cell<usize>,
    seeks: &'a core::cell::Cell<usize>,
}

impl Range for CountingSliceRange<'_> {
    type Item = u32;
    fn empty(&self) -> bool {
        self.slice.is_empty()
    }
    fn front(&self) -> u32 {
        self.slice[0]
    }
    fn pop_front(&mut self) {
        self.pops.set(self.pops.get() + 1);
        self.slice = &self.slice[1..];
    }
    fn seek(&mut self, target: u32) {
        self.seeks.set(self.seeks.get() + 1);
        let at = self.slice.partition_point(|&v| v < target);
        self.slice = &self.slice[at..];
    }
}

impl Iterator for CountingSliceRange<'_> {
    type Item = u32;
    fn next(&mut self) -> Option<u32> {
        if Range::empty(self) {
            return None;
        }
        let v = Range::front(self);
        Range::pop_front(self);
        Some(v)
    }
}

#[test]
fn range_seeks_and_andnot_align_jump_rather_than_step() {
    // Witness: |A| = 5, |B| = 100_000. `AndNotRange::align()` MUST jump via
    // `b.seek()` rather than stepping `b.pop_front()` 100,000 times.
    let a_vals = vec![500u32, 25_000, 50_000, 75_000, 99_999];
    let b_vals: Vec<u32> = (0..100_000u32)
        .filter(|&v| v != 25_000 && v != 75_000)
        .collect();
    let pops_a = core::cell::Cell::new(0usize);
    let seeks_a = core::cell::Cell::new(0usize);
    let pops_b = core::cell::Cell::new(0usize);
    let seeks_b = core::cell::Cell::new(0usize);
    let ra = CountingSliceRange {
        slice: &a_vals,
        pops: &pops_a,
        seeks: &seeks_a,
    };
    let rb = CountingSliceRange {
        slice: &b_vals,
        pops: &pops_b,
        seeks: &seeks_b,
    };
    let out: Vec<u32> = AndNotRange::new(ra, rb).collect();
    assert_eq!(out, vec![25_000, 75_000]);
    assert!(seeks_b.get() >= 5, "AndNotRange::align must call b.seek()");
    assert!(
        pops_b.get() <= 5,
        "AndNotRange::align must not step b linearly (got {} pops)",
        pops_b.get()
    );

    // Seek equivalence on ForestRange (all 5 containers + multi-chunk),
    // Forest64Range (many forests), and AndNotRange / OrRange / XorRange.
    let corpus = u32_corpus();
    let mut sets = Vec::new();
    for vals in &corpus {
        sets.push(Bitmosaic::from_sorted(vals.iter().copied()));
    }
    let targets = [
        0u32,
        1,
        500,
        2_000,
        30_000,
        65_535,
        65_536,
        150_000,
        300_000,
        u32::MAX,
    ];
    for (idx, set) in sets.iter().enumerate() {
        let all = &corpus[idx];
        let mut r = set.range();
        for &t in &targets {
            r.seek(t);
            let pos = all.partition_point(|&v| v < t);
            assert_eq!(
                r.len(),
                all.len() - pos,
                "ForestRange::len after seek({t}) on shape {idx}"
            );
            if pos < all.len() {
                assert!(!r.empty());
                assert_eq!(r.front(), all[pos]);
            } else {
                assert!(r.empty());
            }
        }
    }

    // OrRange / XorRange / AndNotRange seek equivalence vs filtered oracle.
    let a = &sets[10]; // mixed multi-chunk
    let b = &sets[6]; // dense Words
    let sa: BTreeSet<u32> = a.iter().collect();
    let sb: BTreeSet<u32> = b.iter().collect();
    let want_or: Vec<u32> = sa.union(&sb).copied().collect();
    let want_xor: Vec<u32> = sa.symmetric_difference(&sb).copied().collect();
    let want_andnot: Vec<u32> = sa.difference(&sb).copied().collect();

    let mut r_or = OrRange::new(a.range(), b.range());
    let mut r_xor = XorRange::new(a.range(), b.range());
    let mut r_andnot = AndNotRange::new(a.range(), b.range());
    for &t in &targets {
        r_or.seek(t);
        r_xor.seek(t);
        r_andnot.seek(t);
        assert_eq!(
            r_or.save().collect::<Vec<_>>(),
            want_or
                .iter()
                .copied()
                .filter(|&v| v >= t)
                .collect::<Vec<_>>(),
            "OrRange::seek({t})"
        );
        assert_eq!(
            r_xor.save().collect::<Vec<_>>(),
            want_xor
                .iter()
                .copied()
                .filter(|&v| v >= t)
                .collect::<Vec<_>>(),
            "XorRange::seek({t})"
        );
        assert_eq!(
            r_andnot.save().collect::<Vec<_>>(),
            want_andnot
                .iter()
                .copied()
                .filter(|&v| v >= t)
                .collect::<Vec<_>>(),
            "AndNotRange::seek({t})"
        );
    }

    // Forest64Range::seek across 80 high-32 forests.
    let vals64: Vec<u64> = (0..80u64)
        .flat_map(|h| {
            let hi = (h * 4 + 2) << 32;
            (0..20u64).map(move |k| hi | (k * 1_000 + 7))
        })
        .collect();
    let set64 = Bitmosaic64::from_sorted(vals64.iter().copied());
    let mut r64 = set64.range();
    for &t in &[
        0u64,
        (2u64 << 32) | 5_500,
        (40u64 << 32) | 1,
        (162u64 << 32) | 10_000,
        (318u64 << 32) | 19_007,
        u64::MAX,
    ] {
        r64.seek(t);
        let pos = vals64.partition_point(|&v| v < t);
        assert_eq!(r64.len(), vals64.len() - pos);
        if pos < vals64.len() {
            assert_eq!(r64.front(), vals64[pos]);
        } else {
            assert!(r64.empty());
        }
    }
}

#[test]
fn intersect_sorted_simd_and_gallop_match_oracle() {
    let mut rng = 0x5E41_701A_1234_5678u64;
    let cases: Vec<(Vec<u64>, Vec<u64>)> = vec![
        (vec![], vec![1, 2, 3]),
        // Dense inside one 2^16 block (exercises NEON 8-lane path).
        (
            sorted_unique_u64((0..1_000).map(|_| xorshift(&mut rng) % 4_000).collect()),
            sorted_unique_u64((0..1_000).map(|_| xorshift(&mut rng) % 4_000).collect()),
        ),
        // Multi-block across high48 boundaries.
        (
            sorted_unique_u64(
                (0..600)
                    .map(|i| ((i / 30) << 16) | (xorshift(&mut rng) % 200))
                    .collect(),
            ),
            sorted_unique_u64(
                (0..600)
                    .map(|i| ((i / 30) << 16) | (xorshift(&mut rng) % 200))
                    .collect(),
            ),
        ),
        // Heavily skewed (exercises gallop path in both orientations).
        (
            vec![5, 10_000, 1 << 32, (1 << 32) | 999, (5 << 40) | 7],
            (0..2_000u64)
                .map(|i| i * 5)
                .chain([1 << 32, (1 << 32) | 999, (5 << 40) | 7])
                .collect(),
        ),
    ];
    let mut out = Vec::new();
    for (a, b) in cases {
        let sa: BTreeSet<u64> = a.iter().copied().collect();
        let sb: BTreeSet<u64> = b.iter().copied().collect();
        let want: Vec<u64> = sa.intersection(&sb).copied().collect();

        Bitmosaic64::intersect_sorted(&a, &b, &mut out);
        assert_eq!(out, want);
        Bitmosaic64::intersect_sorted(&b, &a, &mut out);
        assert_eq!(out, want);
    }
}

#[test]
fn words_dir_jump_and_wire_container_range_seeks_match_oracle() {
    let corpus = u32_corpus();
    // Targets inside a single 65,536-value chunk spanning same-word, <=4-word,
    // and long >4-word jumps (50..900 words), plus exact boundaries and tail.
    let chunk_targets = [
        0u32,
        1,
        17,
        63,
        64,
        128,
        191,
        255,
        256,
        319,
        320,
        500,
        1_024,
        4_096,
        12_345,
        32_768,
        55_555,
        65_000,
        65_535,
        65_536,
        150_000,
        300_000,
        u32::MAX,
    ];
    for (idx, values) in corpus.iter().enumerate() {
        let set = Bitmosaic::from_sorted(values.iter().copied());
        let bytes = set.to_bytes();
        let view = BitmosaicView::open_admitted(&bytes).unwrap();

        // Sequential monotone seeks on owned ForestRange and wire BitmosaicViewRange.
        let mut r_owned = set.range();
        let mut r_wire = view.range();
        for &t in &chunk_targets {
            r_owned.seek(t);
            r_wire.seek(t);
            let pos = values.partition_point(|&v| v < t);
            assert_eq!(
                r_owned.len(),
                values.len() - pos,
                "owned len after seek({t}) shape={idx}"
            );
            assert_eq!(
                r_wire.remaining() as usize,
                values.len() - pos,
                "wire remaining after seek({t}) shape={idx}"
            );
            if pos < values.len() {
                assert_eq!(r_owned.front(), values[pos], "owned front shape={idx}");
                assert_eq!(r_wire.front(), values[pos], "wire front shape={idx}");
            } else {
                assert!(r_owned.empty());
                assert!(r_wire.empty());
            }
        }

        // Fresh single long jump straight into the tail (exercises Words dir jump,
        // Cone O(1) window seek, Stride reciprocal seek, Array gallop seek).
        for &t in &[5_000u32, 32_768, 60_000] {
            let mut r_owned = set.range();
            let mut r_wire = view.range();
            r_owned.seek(t);
            r_wire.seek(t);
            let pos = values.partition_point(|&v| v < t);
            assert_eq!(r_owned.len(), values.len() - pos);
            assert_eq!(r_wire.remaining() as usize, values.len() - pos);
            assert_eq!(r_owned.collect::<Vec<_>>(), &values[pos..]);
            assert_eq!(r_wire.collect::<Vec<_>>(), &values[pos..]);
        }
    }

    // Runs × Stride exact intersection (owned + wire and_len and and_into).
    let mut runs_vals = Vec::new();
    for b in 0..30u32 {
        let start = b * 2_000 + 50;
        runs_vals.extend(start..start + 400);
    }
    let stride_vals: Vec<u32> = (0..2_000u32).map(|i| 7 + i * 31).collect();
    let s_runs = Bitmosaic::from_sorted(runs_vals.iter().copied());
    let s_stride = Bitmosaic::from_sorted(stride_vals.iter().copied());
    let b_runs = s_runs.to_bytes();
    let b_stride = s_stride.to_bytes();
    let v_runs = BitmosaicView::open_admitted(&b_runs).unwrap();
    let v_stride = BitmosaicView::open_admitted(&b_stride).unwrap();
    let o_runs: BTreeSet<u32> = runs_vals.iter().copied().collect();
    let o_stride: BTreeSet<u32> = stride_vals.iter().copied().collect();
    let want: Vec<u32> = o_runs.intersection(&o_stride).copied().collect();
    assert_eq!(s_runs.and_len(&s_stride), want.len() as u64);
    assert_eq!(v_runs.and_len(&v_stride), want.len() as u64);
    let mut buf = vec![0u32; want.len()];
    let n = v_runs.and_into(&v_stride, &mut buf);
    assert_eq!(n, want.len());
    assert_eq!(buf, want);
}

#[test]
fn u64_view_and_dir64_v2_gallop_and_seeks_match_oracle() {
    let mut many_vals = Vec::new();
    for h in 0..200u64 {
        let hi = (h * 5 + 3) << 32;
        for k in 0..12u64 {
            many_vals.push(hi | (k * 100 + 7));
        }
    }
    let few_vals: Vec<u64> = vec![
        (3u64 << 32) | 207,
        (253u64 << 32) | 507,
        (503u64 << 32) | 707,
        (998u64 << 32) | 1_107,
    ];
    let s_many = Bitmosaic64::from_sorted(many_vals.iter().copied());
    let s_few = Bitmosaic64::from_sorted(few_vals.iter().copied());

    // Format 1 Bitmosaic64ViewRange sequential forward galloping seeks.
    let b_many = s_many.to_bytes();
    let v_many = Bitmosaic64View::open_admitted(&b_many).unwrap();
    let mut r64 = v_many.range();
    for &t in &[
        0u64,
        (3u64 << 32) | 300,
        (8u64 << 32) | 1,
        (103u64 << 32) | 507,
        (500u64 << 32),
        (998u64 << 32) | 1_107,
        u64::MAX,
    ] {
        r64.seek(t);
        let pos = many_vals.partition_point(|&v| v < t);
        assert_eq!(r64.remaining() as usize, many_vals.len() - pos);
        if pos < many_vals.len() {
            assert_eq!(r64.front(), many_vals[pos]);
        } else {
            assert!(r64.empty());
        }
    }

    // Format 2 Dir64V2View skewed and_len, and_rank (both orientations), and Dir64V2Range::seek.
    let v2_many_bytes = s_many.to_v2_bytes();
    let v2_few_bytes = s_few.to_v2_bytes();
    let d_many = Dir64V2View::open(&v2_many_bytes).expect("Dir64V2View many");
    let d_few = Dir64V2View::open(&v2_few_bytes).expect("Dir64V2View few");
    let o_many: BTreeSet<u64> = many_vals.iter().copied().collect();
    let o_few: BTreeSet<u64> = few_vals.iter().copied().collect();
    let inter: Vec<u64> = o_many.intersection(&o_few).copied().collect();

    assert_eq!(d_many.and_len(&d_few), inter.len() as u64);
    assert_eq!(d_few.and_len(&d_many), inter.len() as u64);
    for &limit in &[
        0u64,
        (3u64 << 32) | 207,
        (3u64 << 32) | 208,
        (253u64 << 32) | 507,
        (503u64 << 32) | 800,
        (998u64 << 32) | 1_107,
        u64::MAX,
    ] {
        let want = inter.iter().filter(|&&v| v < limit).count() as u64;
        assert_eq!(
            d_many.and_rank(&d_few, limit),
            want,
            "many.and_rank({limit})"
        );
        assert_eq!(
            d_few.and_rank(&d_many, limit),
            want,
            "few.and_rank({limit})"
        );
    }

    let mut r_v2 = d_many.range();
    for &t in &[
        0u64,
        (3u64 << 32) | 300,
        (8u64 << 32) | 1,
        (103u64 << 32) | 507,
        (500u64 << 32),
        (998u64 << 32) | 1_107,
        u64::MAX,
    ] {
        r_v2.seek(t);
        let pos = many_vals.partition_point(|&v| v < t);
        if pos < many_vals.len() {
            assert!(!r_v2.empty());
            assert_eq!(r_v2.front(), many_vals[pos]);
        } else {
            assert!(r_v2.empty());
        }
    }
}

/// Build an irregular dense `Words` chunk occupying `[start_word, end_word)`
/// inside `chunk_high << 16`, with >= 14 pseudo-random bits per word (including
/// bit 0 and bit 63 on boundary words) so the container freezes as `Words` with
/// an exact span-tight window `[start_word, end_word)` (both multiples of 4).
fn make_words_chunk(chunk_high: u32, start_word: u32, end_word: u32, seed: u64) -> Vec<u32> {
    let mut rng = seed;
    let mut vals = Vec::new();
    let high = chunk_high << 16;
    for w in start_word..end_word {
        let mut word = 0u64;
        for _ in 0..16 {
            word |= 1u64 << (xorshift(&mut rng) & 63);
        }
        // Pin low and high bits on first/last word so the span window is exact.
        if w == start_word {
            word |= 1u64 << 1;
        }
        if w + 1 == end_word {
            word |= (1u64 << 3) | (1u64 << 61);
        }
        while word != 0 {
            let bit = word.trailing_zeros();
            vals.push(high | (w * 64 + bit));
            word &= word - 1;
        }
    }
    vals
}

#[test]
fn words_dir_jump_exact_stride_group_boundaries_owned_and_wire() {
    // DIR_STRIDE = 4 words. Exercise both a full-chunk Words container
    // (words 0..1024, base_word=0, last group 255 = words 1020..1024) and a
    // span-tight Words container (words 100..600, base_word=100, last group
    // words 596..600) on BOTH owned `ForestRange` and wire `BitmosaicViewRange`.
    const DIR_STRIDE: u32 = 4;

    let shapes: [(u32, u32, u64); 2] = [
        (0, 1024, 0xD1A0_0001_5EED_0001),
        (100, 600, 0xD1A0_0002_5EED_0002),
    ];

    for (start_w, end_w, seed) in shapes {
        let vals = make_words_chunk(0, start_w, end_w, seed);
        assert!(
            vals.len() > 4_096,
            "fixture must exceed ARRAY_MAX to freeze as Words"
        );
        let set = Bitmosaic::from_sorted(vals.iter().copied());
        let bytes = set.to_bytes();
        let view = BitmosaicView::open_admitted(&bytes).unwrap();
        assert!(!view.is_elias_fano());

        let check_jump = |start_target: Option<u32>, goal_target: u32, label: &str| {
            let mut r_owned = set.range();
            let mut r_wire = view.range();
            if let Some(s) = start_target {
                r_owned.seek(s);
                r_wire.seek(s);
            }
            r_owned.seek(goal_target);
            r_wire.seek(goal_target);
            let pos = vals.partition_point(|&v| v < goal_target);
            let want = &vals[pos..];
            assert_eq!(
                r_owned.len(),
                want.len(),
                "owned len mismatch ({label}, start_w={start_w}, goal={goal_target})"
            );
            assert_eq!(
                r_wire.remaining() as usize,
                want.len(),
                "wire remaining mismatch ({label}, start_w={start_w}, goal={goal_target})"
            );
            if let Some(&first) = want.first() {
                assert_eq!(r_owned.front(), first, "owned front ({label})");
                assert_eq!(r_wire.front(), first, "wire front ({label})");
            } else {
                assert!(r_owned.empty(), "owned empty ({label})");
                assert!(r_wire.empty(), "wire empty ({label})");
            }
            assert_eq!(
                r_owned.collect::<Vec<_>>(),
                want,
                "owned tail mismatch ({label})"
            );
            assert_eq!(
                r_wire.collect::<Vec<_>>(),
                want,
                "wire tail mismatch ({label})"
            );
        };

        // Cursor starts at `here = start_w`, `*word = start_w + 1`:
        // - `goal = start_w + DIR_STRIDE`     -> `goal - here == DIR_STRIDE`, `goal - *word == DIR_STRIDE - 1` (also group first word!)
        // - `goal = start_w + DIR_STRIDE + 1` -> `goal - *word == DIR_STRIDE` (exact threshold: short path)
        // - `goal = start_w + DIR_STRIDE + 2` -> `goal - *word == DIR_STRIDE + 1` (exact threshold + 1: dir-jump path!)
        for bit in [0u32, 17, 63] {
            check_jump(
                None,
                (start_w + DIR_STRIDE) * 64 + bit,
                "from start: +DIR_STRIDE from here (group first word)",
            );
            check_jump(
                None,
                (start_w + DIR_STRIDE + 1) * 64 + bit,
                "from start: exact DIR_STRIDE words past *word",
            );
            check_jump(
                None,
                (start_w + DIR_STRIDE + 2) * 64 + bit,
                "from start: DIR_STRIDE + 1 words past *word (dir jump)",
            );
        }

        // Same boundary check starting from an unaligned word (`start_w + 1`, so `*word = start_w + 2`).
        let unaligned_start = Some((start_w + 1) * 64);
        for bit in [0u32, 31, 63] {
            check_jump(
                unaligned_start,
                (start_w + 1 + DIR_STRIDE) * 64 + bit,
                "from unaligned: +DIR_STRIDE from here",
            );
            check_jump(
                unaligned_start,
                (start_w + 2 + DIR_STRIDE) * 64 + bit,
                "from unaligned: exact DIR_STRIDE past *word",
            );
            check_jump(
                unaligned_start,
                (start_w + 2 + DIR_STRIDE + 1) * 64 + bit,
                "from unaligned: DIR_STRIDE + 1 past *word (dir jump)",
            );
        }

        // Goals at a group's first word via directory jump (zero intra-group loop iterations).
        for group_offset in [2u32, 17, 64] {
            let group_first_word = start_w + group_offset * DIR_STRIDE;
            for bit in [0u32, 23, 63] {
                check_jump(
                    None,
                    group_first_word * 64 + bit,
                    "dir jump to group first word",
                );
            }
        }

        // Goals in the last group: first word, middle words, last word, and past-the-end exhaustion.
        let last_group_first = end_w - DIR_STRIDE;
        for w in last_group_first..end_w {
            for bit in [0u32, 30, 63] {
                check_jump(None, w * 64 + bit, "dir jump into last group");
            }
        }
        let last_val = *vals.last().unwrap();
        check_jump(None, last_val, "dir jump to exact last member");
        check_jump(None, last_val + 1, "dir jump past last member (exhaust)");
        if end_w < 1024 {
            check_jump(
                None,
                end_w * 64,
                "dir jump past span-tight word_count (exhaust)",
            );
        }
    }
}

#[test]
fn wire_words_opened_mid_chunk_then_far_dir_jumps() {
    // Multi-chunk forest:
    // - chunk 0: tiny Array chunk so the cursor begins in chunk 0
    // - chunk 1: full-span Words chunk (words 0..1024, base_word=0)
    // - chunk 2: span-tight Words chunk (words 100..600, base_word=100)
    // - chunk 3: trailing Array chunk so BitmosaicViewRange.remaining tracks
    //            trailing chunks across mid-chunk open + far jump + rollover.
    let mut all_vals = vec![10u32, 20, 30];
    all_vals.extend(make_words_chunk(1, 0, 1024, 0xABCD_0001_1111_2222));
    all_vals.extend(make_words_chunk(2, 100, 600, 0xABCD_0002_3333_4444));
    all_vals.extend([(3 << 16) | 42, (3 << 16) | 999]);

    let set = Bitmosaic::from_sorted(all_vals.iter().copied());
    let bytes = set.to_bytes();
    let view = BitmosaicView::open_admitted(&bytes).unwrap();
    assert!(!view.is_elias_fano());

    // Helper: seek from chunk 0 into the MIDDLE of a Words chunk (triggers
    // `seek_forest` -> `CRange::at_rank(container, k)` with `k > 0`), then
    // issue a sequence of monotone in-chunk jumps crossing DIR_STRIDE,
    // exact DIR_STRIDE / DIR_STRIDE+1, group first word, last group, and
    // rollover into the next chunk.
    let sequences: [&[u32]; 2] = [
        // Enter chunk 1 mid-chunk at word 20 (k > 0), then jump across strides.
        &[
            (1 << 16) | (20 * 64 + 13),   // open CRange::Words mid-chunk (k > 0)
            (1 << 16) | (25 * 64 + 5),    // +4 words past *word (exact DIR_STRIDE)
            (1 << 16) | (31 * 64 + 29),   // +5 words past *word (DIR_STRIDE + 1 -> dir jump!)
            (1 << 16) | (256 * 64),       // far dir jump to group 64 first word
            (1 << 16) | (700 * 64 + 40),  // far dir jump across 111 groups
            (1 << 16) | (1020 * 64),      // last group first word
            (1 << 16) | (1023 * 64 + 62), // past last member of chunk 1 -> rolls to chunk 2
        ],
        // Enter chunk 2 (span-tight, base_word=100) mid-chunk at word 140 (k > 0),
        // then jump across strides and roll into chunk 3.
        &[
            (2 << 16) | (140 * 64 + 19), // open span-tight CRange::Words mid-chunk (k > 0)
            (2 << 16) | (145 * 64 + 7),  // +4 words past *word (exact DIR_STRIDE)
            (2 << 16) | (151 * 64 + 33), // +5 words past *word (DIR_STRIDE + 1 -> dir jump!)
            (2 << 16) | (300 * 64),      // far dir jump to group first word (rel 200)
            (2 << 16) | (596 * 64),      // last group first word (rel 496)
            (2 << 16) | (599 * 64 + 62), // past last member of chunk 2 -> rolls to chunk 3
        ],
    ];

    for (seq_idx, targets) in sequences.iter().enumerate() {
        let mut r_owned = set.range();
        let mut r_wire = view.range();
        for (step, &t) in targets.iter().enumerate() {
            r_owned.seek(t);
            r_wire.seek(t);
            let pos = all_vals.partition_point(|&v| v < t);
            let want = &all_vals[pos..];
            assert_eq!(
                r_owned.len(),
                want.len(),
                "owned len seq={seq_idx} step={step} target={t:#x}"
            );
            assert_eq!(
                r_wire.remaining() as usize,
                want.len(),
                "wire remaining seq={seq_idx} step={step} target={t:#x}"
            );
            assert_eq!(
                r_owned.front(),
                want[0],
                "owned front seq={seq_idx} step={step}"
            );
            assert_eq!(
                r_wire.front(),
                want[0],
                "wire front seq={seq_idx} step={step}"
            );
            // Drain a saved snapshot after the mid-chunk dir jumps to verify
            // every subsequent word load and chunk rollover is exact.
            assert_eq!(
                r_wire.save().collect::<Vec<_>>(),
                want,
                "wire drained tail seq={seq_idx} step={step} target={t:#x}"
            );
        }
    }
}
