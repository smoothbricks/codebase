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
    AndNotRange, Bitmosaic, Bitmosaic64, Bitmosaic64View, BitmosaicView, EMPTY_U32_IMAGE,
    EMPTY_U64_IMAGE, OrRange, Range, XorRange,
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
