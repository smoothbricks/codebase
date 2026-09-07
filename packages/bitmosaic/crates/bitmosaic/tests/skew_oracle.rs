//! Skew-adaptive intersection (G261): what must hold for the chooser to be
//! allowed anywhere near `and_len`.
//!
//! 1. **The answer never depends on the arm.** A differential oracle against
//!    `BTreeSet` and `roaring` runs across every skew, both operand
//!    orientations, and every container tag the ladder can produce. The
//!    shipped arm is exercised by the default run; the forced balanced arm
//!    joins it under `bench-internals`, where the switch exists.
//! 2. **The kernels allocate nothing.** A counting global allocator brackets
//!    the intersections. The claim is about zero, so the counter is proved
//!    able to report non-zero in the same test — an instrument that cannot
//!    see a violation cannot certify its absence.
//! 3. **The switch is real.** Under the instrument, forcing the balanced arm
//!    must MOVE the position census. Two arms that agree on the answer and
//!    on the census are one arm, and an A/B over them measures nothing.
//! 4. **The gallop's edges hold.** Targets below the first member, above the
//!    last, and in every gap are the cases an exponential search gets wrong;
//!    they are driven through the public surface at ratios that force the
//!    drive arm.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::BTreeSet;
#[cfg(feature = "bench-internals")]
use std::sync::{Mutex, MutexGuard};

use bitmosaic::{Bitmosaic, Bitmosaic64, Bitmosaic64View, BitmosaicBuilder, BitmosaicView};
use roaring::RoaringBitmap;

#[test]
fn run_record_skew_is_independent_of_member_skew() {
    #[cfg(feature = "bench-internals")]
    let _guard = instrument();
    let left = Bitmosaic::from_sorted((0..25_000).chain(32_768..57_768));
    let right = Bitmosaic::from_sorted((0..1000u32).flat_map(|i| (i * 60 + 5)..(i * 60 + 15)));
    assert_eq!(left.container_census().4, 1);
    assert_eq!(right.container_census().4, 1);
    let oracle_a: BTreeSet<_> = left.iter().collect();
    let oracle_b: BTreeSet<_> = right.iter().collect();
    let expected = oracle_a.intersection(&oracle_b).count() as u64;
    let a_bytes = left.to_forest_bytes();
    let b_bytes = right.to_forest_bytes();
    let a = BitmosaicView::open_verified(&a_bytes).unwrap();
    let b = BitmosaicView::open_verified(&b_bytes).unwrap();
    assert_eq!(left.and_len(&right), expected);
    assert_eq!(right.and_len(&left), expected);
    assert_eq!(a.and_len(&b), expected);
    assert_eq!(b.and_len(&a), expected);
    assert_eq!(
        allocations_during(|| {
            std::hint::black_box(left.and_len(&right));
            std::hint::black_box(right.and_len(&left));
            std::hint::black_box(a.and_len(&b));
            std::hint::black_box(b.and_len(&a));
        }),
        0
    );
}

/// The arm switch is PROCESS-global, matching the crate's other
/// kernel-forcing seams, while `cargo test` runs a file's tests as parallel
/// threads in one process. Anything that moves the switch holds this first,
/// or a neighbouring test picks your arm out from under you. The position
/// counters need no such guard — they are per-thread.
#[cfg(feature = "bench-internals")]
static INSTRUMENT: Mutex<()> = Mutex::new(());

#[cfg(feature = "bench-internals")]
fn instrument() -> MutexGuard<'static, ()> {
    INSTRUMENT
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

// ── allocation instrument ────────────────────────────────────────────────

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
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn allocations_during(body: impl FnOnce()) -> usize {
    let before = ALLOCS.with(|n| n.get());
    body();
    ALLOCS.with(|n| n.get()) - before
}

// ── shapes ───────────────────────────────────────────────────────────────

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// Zipf(s=1) rank in `0..n`, inverted in closed form from
/// `CDF(k) = ln(k+1)/ln(n+1)`.
fn zipf_rank(rng: &mut Rng, n: u64) -> u64 {
    let unit = (rng.next() >> 11) as f64 / (1u64 << 53) as f64;
    ((((n + 1) as f64).ln() * unit).exp() as u64)
        .saturating_sub(1)
        .min(n - 1)
}

fn zipf_set(seed: u64, count: usize, universe: u64, scatter: bool) -> BTreeSet<u32> {
    let mut rng = Rng(seed);
    let mut out = BTreeSet::new();
    let mut budget = count as u64 * 64 + 1024;
    while out.len() < count && budget > 0 {
        budget -= 1;
        let rank = zipf_rank(&mut rng, universe);
        let id = if scatter {
            rank.wrapping_mul(0x9E37_79B9_7F4A_7C15) % universe
        } else {
            rank
        };
        out.insert(id as u32);
    }
    out
}

/// A rare posting list against a common one, with a quarter of the rare side
/// lifted from the common one so the answer stays substantial at 1000:1.
fn skew_pair(
    seed: u64,
    large_len: usize,
    ratio: usize,
    universe: u64,
    scatter: bool,
) -> (Vec<u32>, Vec<u32>) {
    let large = zipf_set(seed, large_len, universe, scatter);
    let large: Vec<u32> = large.into_iter().collect();
    let small_len = (large_len / ratio).max(1);
    let mut small = zipf_set(seed ^ 0xFFFF, small_len - small_len / 4, universe, scatter);
    let mut rng = Rng(seed.wrapping_mul(31) | 1);
    for _ in 0..small_len / 4 {
        small.insert(large[(rng.next() % large.len() as u64) as usize]);
    }
    (small.into_iter().collect(), large)
}

fn build(values: &[u32]) -> Bitmosaic {
    Bitmosaic::from_sorted(values.iter().copied())
}

/// Independent truth: neither operand's representation is involved.
fn truth(small: &[u32], large: &[u32]) -> u64 {
    let a: BTreeSet<u32> = small.iter().copied().collect();
    let b: BTreeSet<u32> = large.iter().copied().collect();
    let by_tree = a.intersection(&b).count() as u64;
    let ro_a: RoaringBitmap = small.iter().copied().collect();
    let ro_b: RoaringBitmap = large.iter().copied().collect();
    assert_eq!(
        by_tree,
        ro_a.intersection_len(&ro_b),
        "the two oracles disagree with each other"
    );
    by_tree
}

/// Every arm the build makes reachable must produce `expected`, in both
/// operand orders. Without the instrument that is the shipped arm alone;
/// with it, both forced arms.
///
/// Forcing DRIVE is the half that would otherwise never be tested: the
/// chooser sends balanced pairs to the merge, so without the force the
/// gallop kernels are exercised only on the shapes they were written for.
/// Here they run on every shape in the file.
fn assert_arms_agree(name: &str, a: &Bitmosaic, b: &Bitmosaic, expected: u64) {
    assert_eq!(a.and_len(b), expected, "{name}: shipped arm");
    assert_eq!(b.and_len(a), expected, "{name}: shipped arm, reversed");
    // The MAPPED path is a second implementation of the same kernels over
    // wire bytes, and it is the one the engine's FTS intersection calls. It
    // carries its own copy of the chooser, so it needs its own oracle.
    let (ba, bb) = (a.to_bytes(), b.to_bytes());
    let (va, vb) = (
        BitmosaicView::open(&ba).unwrap_or_else(|| panic!("{name}: open a")),
        BitmosaicView::open(&bb).unwrap_or_else(|| panic!("{name}: open b")),
    );
    assert_eq!(va.and_len(&vb), expected, "{name}: mapped view");
    assert_eq!(vb.and_len(&va), expected, "{name}: mapped view, reversed");

    // Force the mapped implementation too. Auto-only coverage can leave a
    // broken fallback arm invisible when the chooser routes around it.
    #[cfg(feature = "bench-internals")]
    {
        use bitmosaic::skew::{Arm, force};
        let _held = instrument();
        for (arm, label) in [(Arm::Balanced, "balanced"), (Arm::Drive, "drive")] {
            force(arm);
            assert_eq!(a.and_len(b), expected, "{name}: forced {label} arm");
            assert_eq!(
                b.and_len(a),
                expected,
                "{name}: forced {label} arm, reversed"
            );
            assert_eq!(
                va.and_len(&vb),
                expected,
                "{name}: mapped forced {label} arm"
            );
            assert_eq!(
                vb.and_len(&va),
                expected,
                "{name}: mapped forced {label} arm, reversed"
            );
        }
        force(Arm::Auto);
    }
}

/// The production FTS count path wraps the same mapped forest kernels in the
/// u64 directory. Exercise that public surface independently: a green
/// `BitmosaicView` test does not prove [`Bitmosaic64View::and_len`] opened and drove
/// the nested wire image correctly.
fn assert_fts_wire_arms_agree(name: &str, a: &[u32], b: &[u32], expected: u64) {
    let a64 = Bitmosaic64::from_sorted(a.iter().copied().map(u64::from));
    let b64 = Bitmosaic64::from_sorted(b.iter().copied().map(u64::from));
    assert_eq!(a64.and_len(&b64), expected, "{name}: owned u64");
    assert_eq!(b64.and_len(&a64), expected, "{name}: owned u64, reversed");

    let (ba, bb) = (a64.to_bytes(), b64.to_bytes());
    let (va, vb) = (
        Bitmosaic64View::open(&ba).unwrap_or_else(|| panic!("{name}: open u64 a")),
        Bitmosaic64View::open(&bb).unwrap_or_else(|| panic!("{name}: open u64 b")),
    );
    assert_eq!(va.and_len(&vb), expected, "{name}: FTS mapped wire");
    assert_eq!(
        vb.and_len(&va),
        expected,
        "{name}: FTS mapped wire, reversed"
    );

    #[cfg(feature = "bench-internals")]
    {
        use bitmosaic::skew::{Arm, force};
        let _held = instrument();
        for (arm, label) in [(Arm::Balanced, "balanced"), (Arm::Drive, "drive")] {
            force(arm);
            assert_eq!(
                a64.and_len(&b64),
                expected,
                "{name}: owned u64 forced {label}"
            );
            assert_eq!(
                b64.and_len(&a64),
                expected,
                "{name}: owned u64 forced {label}, reversed"
            );
            assert_eq!(va.and_len(&vb), expected, "{name}: FTS wire forced {label}");
            assert_eq!(
                vb.and_len(&va),
                expected,
                "{name}: FTS wire forced {label}, reversed"
            );
        }
        force(Arm::Auto);
    }
}

// ── 1. the answer never depends on the arm ───────────────────────────────

#[test]
fn every_skew_and_layout_matches_the_oracles() {
    // 2^20 keeps the scattered layout inside the Array arm; 2^17 pushes the
    // ranked layout's hot chunks into Words, so one loop covers both the
    // sequence kernels and the pairs that mix a bitmap with a sequence.
    for (universe, scatter, layout) in [(1u64 << 20, true, "scattered"), (1 << 17, false, "ranked")]
    {
        for large_len in [2_000usize, 40_000] {
            for ratio in [1usize, 2, 10, 33, 100, 1000] {
                let (small, large) = skew_pair(
                    0x5EED_0000 + ratio as u64,
                    large_len,
                    ratio,
                    universe,
                    scatter,
                );
                let name = format!("{layout}/{large_len}/{ratio}:1");
                let expected = truth(&small, &large);
                assert!(
                    expected > 0,
                    "{name}: an empty answer cannot separate a kernel from zero"
                );
                assert_arms_agree(&name, &build(&small), &build(&large), expected);
            }
        }
    }
}

/// The chooser keys on the container TAG as well as the ratio, so every tag
/// pairing has to be visited with a skewed cardinality. These builders name
/// the tag by construction rather than hoping the ladder picks it.
#[test]
fn every_container_tag_pairing_survives_skew() {
    let stride =
        |first: u32, step: u32, n: u32| -> Vec<u32> { (0..n).map(|i| first + i * step).collect() };
    // A cone is a near-arithmetic run: the ladder takes it below
    // CONE_MAX_LEN with small residuals.
    let cone = |first: u32, step: u32, n: u32, seed: u64| -> Vec<u32> {
        let mut rng = Rng(seed);
        let mut out: Vec<u32> = (0..n)
            .map(|i| first + i * step + (rng.next() % 3) as u32)
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    };
    let array = |n: u32, seed: u64| -> Vec<u32> {
        let mut rng = Rng(seed);
        let mut out = BTreeSet::new();
        while out.len() < n as usize {
            out.insert((rng.next() % 60_000) as u32);
        }
        out.into_iter().collect()
    };
    // Above ARRAY_MAX the ladder must take Words.
    let words = |n: u32, seed: u64| -> Vec<u32> { array(n, seed) };

    let tiny: Vec<(&str, Vec<u32>)> = vec![
        ("stride-8", stride(101, 7, 8)),
        ("cone-8", cone(53, 11, 8, 0x11)),
        ("array-8", array(8, 0x22)),
    ];
    let wide: Vec<(&str, Vec<u32>)> = vec![
        ("stride-4000", stride(3, 13, 4_000)),
        ("cone-1000", cone(7, 41, 1_000, 0x33)),
        ("array-4000", array(4_000, 0x44)),
        ("words-40000", words(40_000, 0x55)),
    ];

    for (small_name, small) in &tiny {
        for (large_name, large) in &wide {
            let name = format!("{small_name} x {large_name}");
            let (a, b) = (build(small), build(large));
            // The pairing is only meaningful if the ladder produced one
            // container per side; every fixture here lives in chunk 0.
            assert_eq!(
                a.container_census().0
                    + a.container_census().1
                    + a.container_census().2
                    + a.container_census().3,
                1,
                "{name}: small side is not one chunk"
            );
            let expected = truth(small, large);
            assert_arms_agree(&name, &a, &b, expected);
        }
    }
}

/// Forest-level skew: a set living in a handful of chunks against one
/// spanning thousands. This is the walk the container kernels never reach.
#[test]
fn forest_level_skew_matches_the_oracles() {
    let mut wide = Vec::new();
    for chunk in 0..3_000u32 {
        for i in 0..40u32 {
            wide.push((chunk << 16) | (i * 1_500 + 7));
        }
    }
    for narrow_chunks in [1usize, 2, 40] {
        let mut narrow = Vec::new();
        for c in 0..narrow_chunks as u32 {
            let chunk = c * 71;
            for i in 0..25u32 {
                // Half of these land on a `wide` member, half between two.
                narrow.push((chunk << 16) | (i * 3_000 + if i % 2 == 0 { 7 } else { 9 }));
            }
        }
        narrow.sort_unstable();
        let name = format!("forest/{narrow_chunks}-of-3000");
        let expected = truth(&narrow, &wide);
        assert!(expected > 0, "{name}: empty answer");
        assert_arms_agree(&name, &build(&narrow), &build(&wide), expected);
    }
}

/// Adversarial equality spine for every driven pair, over both in-memory
/// owners and the nested mapped form used by FTS. These shapes deliberately
/// include the degenerate answers that the acceptance timing cells exclude:
/// correctness must still hold when there is no positive-work witness.
#[test]
fn adversarial_shapes_cover_every_driven_pair_and_wire_form() {
    let stride =
        |first: u32, step: u32, n: u32| -> Vec<u32> { (0..n).map(|i| first + i * step).collect() };
    let cone = |first: u32, step: u32, n: u32, seed: u64| -> Vec<u32> {
        let mut rng = Rng(seed);
        (0..n)
            .map(|i| first + i * step + (rng.next() % 3) as u32)
            .collect()
    };
    let array = |n: usize, seed: u64| -> Vec<u32> {
        let mut rng = Rng(seed);
        let mut values = BTreeSet::new();
        while values.len() < n {
            values.insert((rng.next() % 60_000) as u32);
        }
        values.into_iter().collect()
    };
    let check = |name: &str, left: &[u32], right: &[u32]| {
        let expected = truth(left, right);
        assert_arms_agree(name, &build(left), &build(right), expected);
        assert_fts_wire_arms_agree(name, left, right, expected);
    };
    let check_pair = |name: &str,
                      left: &[u32],
                      right: &[u32],
                      left_tag: (usize, usize, usize, usize, usize),
                      right_tag: (usize, usize, usize, usize, usize)| {
        let (a, b) = (build(left), build(right));
        assert_eq!(a.container_census(), left_tag, "{name}: left tag");
        assert_eq!(b.container_census(), right_tag, "{name}: right tag");
        let expected = truth(left, right);
        assert_arms_agree(name, &a, &b, expected);
        assert_fts_wire_arms_agree(name, left, right, expected);
    };

    // The five container pair kernels named by the unit. Ratios are at or
    // beyond the member crossover, while hits range from prefix-heavy to
    // sparse, so forcing either arm cannot hide behind the chooser.
    let array_large = array(1_024, 0xA11A);
    let array_small: Vec<u32> = array_large.iter().step_by(33).copied().collect();
    check_pair(
        "Array x Array",
        &array_small,
        &array_large,
        (0, 0, 1, 0, 0),
        (0, 0, 1, 0, 0),
    );

    let cone_large = cone(7, 100, 512, 0xC0E0);
    let cone_small = cone(7, 100, 16, 0xC0E0);
    check_pair(
        "Cone x Cone",
        &cone_small,
        &cone_large,
        (0, 1, 0, 0, 0),
        (0, 1, 0, 0, 0),
    );

    let mut mixed_array: BTreeSet<u32> = array(528, 0xCA11).into_iter().collect();
    mixed_array.extend(cone_small.iter().copied());
    let mixed_array: Vec<u32> = mixed_array.into_iter().collect();
    check_pair(
        "Cone x Array",
        &cone_small,
        &mixed_array,
        (0, 1, 0, 0, 0),
        (0, 0, 1, 0, 0),
    );

    let stride_small = stride(11, 101, 16);
    let mut stride_array: BTreeSet<u32> = array(528, 0x57A1).into_iter().collect();
    stride_array.extend(stride_small.iter().copied());
    let stride_array: Vec<u32> = stride_array.into_iter().collect();
    check_pair(
        "Stride x Array",
        &stride_small,
        &stride_array,
        (1, 0, 0, 0, 0),
        (0, 0, 1, 0, 0),
    );
    check_pair(
        "Stride x Cone",
        &stride_small,
        &cone_large,
        (1, 0, 0, 0, 0),
        (0, 1, 0, 0, 0),
    );

    let forest_large: Vec<u32> = (0..96u32)
        .flat_map(|chunk| (0..4u32).map(move |i| (chunk << 16) | (i * 9_001 + 17)))
        .collect();
    let forest_small: Vec<u32> = [0u32, 47, 95]
        .into_iter()
        .flat_map(|chunk| (0..4u32).map(move |i| (chunk << 16) | (i * 9_001 + 17)))
        .collect();
    check("forest walk", &forest_small, &forest_large);

    // Empty, singleton, total overlap and total disjointness are distinct
    // gallop outcomes: no probe, one terminal probe, all hits, and all misses.
    check("empty left", &[], &array_large);
    check("empty right", &array_large, &[]);
    check("singleton hit", &[array_large[511]], &array_large);
    check("singleton miss", &[65_535], &array_large);
    check("fully overlapping", &array_small, &array_small);
    let disjoint_left: Vec<u32> = (0..64u32).map(|i| i * i * 3 + i).collect();
    let disjoint_right: Vec<u32> = disjoint_left.iter().map(|value| value + 30_000).collect();
    check("fully disjoint", &disjoint_left, &disjoint_right);

    // Ladder boundaries: Cone stops at 1,024 members; Array stops at 4,096.
    // Each pair straddles one threshold with a prefix overlap, then one run
    // crosses the u16 container-key boundary itself.
    let cone_1025 = cone(3, 61, 1_025, 0xB0A1);
    let cone_1024 = cone_1025[..1_024].to_vec();
    check_pair(
        "Cone 1024 x Array 1025",
        &cone_1024,
        &cone_1025,
        (0, 1, 0, 0, 0),
        (0, 0, 1, 0, 0),
    );
    let words_4097 = array(4_097, 0xB0A2);
    let array_4096 = words_4097[..4_096].to_vec();
    check_pair(
        "Array 4096 x Words 4097",
        &array_4096,
        &words_4097,
        (0, 0, 1, 0, 0),
        (0, 0, 0, 1, 0),
    );
    check(
        "u16 boundary straddle",
        &[65_534, 65_535, 65_536, 65_537],
        &[65_535, 65_536],
    );
}
/// The key-level crossover is sensitive to WHERE the extra keys land, not
/// only how many there are. Prefix, suffix, alternating hits and alternating
/// misses all exercise different cursor behavior at the exact 2:1 boundary.
#[test]
fn key_crossover_run_structures_agree_in_owned_and_wire_forms() {
    let values = |keys: &[u16]| -> Vec<u32> {
        keys.iter()
            .flat_map(|&key| (0..8u32).map(move |i| (u32::from(key) << 16) | (i * 8_000 + 7)))
            .collect()
    };
    let small_prefix: Vec<u16> = (0..32).collect();
    let small_suffix: Vec<u16> = (32..64).collect();
    let large_contiguous: Vec<u16> = (0..64).collect();
    let small_interleaved: Vec<u16> = (0..32).map(|i| i * 2).collect();
    let large_even: Vec<u16> = (0..64).map(|i| i * 2).collect();
    let small_misses: Vec<u16> = (0..32).map(|i| i * 4 + 1).collect();

    for (name, small_keys, large_keys) in [
        ("key extra suffix", &small_prefix, &large_contiguous),
        ("key extra prefix", &small_suffix, &large_contiguous),
        (
            "key interleaved hits",
            &small_interleaved,
            &large_contiguous,
        ),
        ("key spread all miss", &small_misses, &large_even),
    ] {
        let (small, large) = (values(small_keys), values(large_keys));
        let expected = truth(&small, &large);
        assert_arms_agree(name, &build(&small), &build(&large), expected);
        assert_fts_wire_arms_agree(name, &small, &large, expected);
    }
}

/// A truncated mapped container must be rejected at attach, even when an
/// attacker rewrites both enclosing length fields to make the shorter byte
/// string look self-consistent. FTS never receives a view on which a driven
/// kernel could read beyond the mapping.
#[test]
fn truncated_fts_wire_image_fails_closed() {
    let values: Vec<u64> = (0..4_097u64).map(|i| (i * 15_997) & 0xFFFF).collect();
    let mut values = values;
    values.sort_unstable();
    values.dedup();
    assert_eq!(
        values.len(),
        4_097,
        "fixture must freeze as one Words container"
    );

    let bytes = Bitmosaic64::from_sorted(values).to_bytes();
    let full = Bitmosaic64View::open(&bytes).expect("complete FTS wire image must open");
    assert_eq!(full.len(), 4_097);
    for cut in 0..bytes.len() {
        assert!(
            Bitmosaic64View::open(&bytes[..cut]).is_none(),
            "unmodified truncated prefix opened at {cut}/{}",
            bytes.len()
        );
    }

    // Rewrite the sole outer frame length so the unframed child's exact
    // payload extent, rather than the enclosing frame, rejects truncation.
    let mut body_at = bitmosaic::IMAGE_ID_LEN;
    while bytes[body_at] & 128 != 0 {
        body_at += 1;
    }
    body_at += 1;
    for removed in [1usize, 8, 64] {
        let body = &bytes[body_at..bytes.len() - removed];
        let mut truncated = bytes[..bitmosaic::IMAGE_ID_LEN].to_vec();
        let mut remaining = body.len();
        loop {
            let low = (remaining & 127) as u8;
            remaining >>= 7;
            truncated.push(low | if remaining == 0 { 0 } else { 128 });
            if remaining == 0 {
                break;
            }
        }
        truncated.extend_from_slice(body);
        assert!(
            Bitmosaic64View::open(&truncated).is_none(),
            "container payload truncated by {removed} bytes opened"
        );
    }
}

// ── 4. the gallop's edges ────────────────────────────────────────────────

/// A singleton driver at every interesting position of a wide partner: below
/// the first member, on it, in each gap, on the last, and above it. The
/// ratio is 4000:1, far past the drive threshold, so each of these is one
/// exponential search with nothing else running.
#[test]
fn a_singleton_driver_lands_on_every_boundary() {
    let large: Vec<u32> = (0..4_000u32).map(|i| 100 + i * 10).collect();
    let wide = build(&large);
    let mut probed = 0usize;
    for &member in &large {
        for target in [member - 1, member, member + 1] {
            let one = build(&[target]);
            let expected = large.binary_search(&target).is_ok() as u64;
            assert_eq!(one.and_len(&wide), expected, "target {target}");
            assert_eq!(wide.and_len(&one), expected, "target {target}, reversed");
            probed += 1;
        }
    }
    // Below the first and above the last: the two positions an exponential
    // search can walk off.
    for target in [0u32, 99, 40_090, 60_000] {
        let one = build(&[target]);
        let expected = large.binary_search(&target).is_ok() as u64;
        assert_eq!(one.and_len(&wide), expected, "edge target {target}");
        probed += 1;
    }
    assert_eq!(probed, 4_000 * 3 + 4);
}

// ── 2. the kernels allocate nothing ──────────────────────────────────────

#[test]
fn intersection_allocates_nothing_at_any_skew() {
    let mut cases = Vec::new();
    for ratio in [1usize, 10, 100, 1000] {
        let (small, large) = skew_pair(0xA110_C000 + ratio as u64, 40_000, ratio, 1 << 20, true);
        cases.push((ratio, small, large));
    }

    // The instrument must be able to report a violation, or its zero is
    // worth nothing. A `Vec` push over a fresh allocation is the smallest
    // thing the kernels are forbidden to do.
    let witness = allocations_during(|| {
        let v: Vec<u32> = core::iter::once(1_u32).collect();
        std::hint::black_box(&v);
    });
    assert!(
        witness > 0,
        "the allocation counter cannot see an allocation"
    );

    for (ratio, small, large) in &cases {
        let (a, b) = (build(small), build(large));
        let (ba, bb) = (a.to_bytes(), b.to_bytes());
        let (va, vb) = (
            BitmosaicView::open(&ba).unwrap(),
            BitmosaicView::open(&bb).unwrap(),
        );

        let a64 = Bitmosaic64::from_sorted(small.iter().copied().map(u64::from));
        let b64 = Bitmosaic64::from_sorted(large.iter().copied().map(u64::from));
        let (ba64, bb64) = (a64.to_bytes(), b64.to_bytes());
        let (va64, vb64) = (
            Bitmosaic64View::open(&ba64).unwrap(),
            Bitmosaic64View::open(&bb64).unwrap(),
        );

        let mut sink = 0u64;
        let allocs = allocations_during(|| {
            sink = a.and_len(&b)
                + b.and_len(&a)
                + va.and_len(&vb)
                + vb.and_len(&va)
                + a64.and_len(&b64)
                + b64.and_len(&a64)
                + va64.and_len(&vb64)
                + vb64.and_len(&va64);
        });
        assert!(sink > 0, "{ratio}:1 computed nothing");
        assert_eq!(
            allocs, 0,
            "{ratio}:1 allocated on an owned or mapped shipped arm"
        );

        #[cfg(feature = "bench-internals")]
        {
            use bitmosaic::skew::{Arm, force};
            let _held = instrument();
            for (arm, label) in [(Arm::Balanced, "balanced"), (Arm::Drive, "drive")] {
                force(arm);
                let allocs = allocations_during(|| {
                    sink = a.and_len(&b)
                        + b.and_len(&a)
                        + va.and_len(&vb)
                        + vb.and_len(&va)
                        + a64.and_len(&b64)
                        + b64.and_len(&a64)
                        + va64.and_len(&vb64)
                        + vb64.and_len(&va64);
                });
                assert_eq!(
                    allocs, 0,
                    "{ratio}:1 allocated on an owned or mapped forced {label} arm"
                );
            }
            force(Arm::Auto);
        }
    }
}

/// The staged builder allocates; the frozen forest's ops must not. Keeping
/// this beside the census stops the zero above from being read as a claim
/// about the whole crate.
#[test]
fn the_builder_is_where_allocation_lives() {
    let allocs = allocations_during(|| {
        let mut builder = BitmosaicBuilder::new();
        for i in 0..10_000u32 {
            builder.insert(i * 3);
        }
        std::hint::black_box(builder.freeze());
    });
    assert!(
        allocs > 0,
        "the builder is expected to allocate; the frozen ops are not"
    );
}

// ── 3. the switch is real ────────────────────────────────────────────────

/// The chooser's own census on a cell it must engage, against the forced
/// merge on the same cell. Both crossovers get a fixture past their own
/// ratio, since they no longer share one.
#[cfg(feature = "bench-internals")]
#[test]
fn the_shipped_chooser_engages_and_moves_the_census() {
    use bitmosaic::skew;

    let _held = instrument();
    let census_of = |a: &Bitmosaic, b: &Bitmosaic, arm: skew::Arm| {
        skew::force(arm);
        skew::reset();
        let answer = a.and_len(b);
        (answer, skew::census())
    };

    // Members: one chunk, 32 against 4096 — ratio 128, past
    // DRIVE_RATIO_MEMBERS.
    let mut rng = Rng(0xDEFA_0001);
    let mut large = BTreeSet::new();
    while large.len() < 4_096 {
        large.insert((rng.next() % 65_536) as u32);
    }
    let large: Vec<u32> = large.into_iter().collect();
    let small: Vec<u32> = large.iter().step_by(128).copied().collect();
    let (a, b) = (build(&small), build(&large));

    let (driven, drive_census) = census_of(&a, &b, skew::Arm::Auto);
    let (balanced, balanced_census) = census_of(&a, &b, skew::Arm::Balanced);
    skew::force(skew::Arm::Auto);

    assert_eq!(driven, balanced, "the arms disagree on the answer");
    assert!(
        drive_census.gallop_probes > 0,
        "the drive arm issued no probes"
    );
    assert_eq!(
        drive_census.linear_positions, 0,
        "the drive arm ran a merge"
    );
    assert!(
        balanced_census.linear_positions > 0,
        "the balanced arm consumed no positions"
    );
    assert_eq!(
        balanced_census.gallop_probes, 0,
        "the balanced arm galloped"
    );
    assert!(
        drive_census.total() < balanced_census.total(),
        "the drive arm did not do less work: {drive_census:?} vs {balanced_census:?}"
    );

    // Keys: 2 chunks against 3000 — ratio 1500, past DRIVE_RATIO_KEYS.
    let wide: Vec<u32> = (0..3_000u32)
        .flat_map(|c| (0..8u32).map(move |i| (c << 16) | (i * 8_000 + 3)))
        .collect();
    let narrow: Vec<u32> = (0..2u32)
        .flat_map(|c| (0..8u32).map(move |i| ((c * 999) << 16) | (i * 8_000 + 3)))
        .collect();
    let (a, b) = (build(&narrow), build(&wide));

    let (driven, drive_census) = census_of(&a, &b, skew::Arm::Auto);
    let (balanced, balanced_census) = census_of(&a, &b, skew::Arm::Balanced);
    skew::force(skew::Arm::Auto);

    assert_eq!(driven, balanced, "the forest arms disagree on the answer");
    assert!(
        drive_census.key_probes > 0,
        "the forest drive arm issued no key probes"
    );
    assert_eq!(
        drive_census.key_positions, 0,
        "the forest drive arm walked keys linearly"
    );
    assert!(
        balanced_census.key_positions > 0,
        "the balanced forest walk consumed no keys"
    );
    assert!(
        drive_census.key_probes * 8 < balanced_census.key_positions,
        "the forest drive arm is not decisively cheaper: {drive_census:?} vs {balanced_census:?}"
    );

    // Mapped view: the second copy of both choosers, over wire bytes. The
    // same two fixtures must move its census too, or the engine's FTS
    // intersection is still running the cardinality-blind kernels while
    // every owned-path assertion above reads green. The FOREST form is
    // pinned: the chooser under test is the forest's, and the seal's own
    // root choice is a byte price that an Elias-Fano root — which has no
    // chooser, only an enumerate-and-probe — wins on exactly these sparse
    // fixtures.
    let view_census = |small: &Bitmosaic, large: &Bitmosaic, arm: skew::Arm| {
        let (bs, bl) = (small.to_forest_bytes(), large.to_forest_bytes());
        let (vs, vl) = (
            BitmosaicView::open(&bs).unwrap(),
            BitmosaicView::open(&bl).unwrap(),
        );
        skew::force(arm);
        skew::reset();
        let answer = vs.and_len(&vl);
        (answer, skew::census())
    };

    let (view_driven, view_drive_census) = view_census(&a, &b, skew::Arm::Auto);
    let (view_balanced, view_balanced_census) = view_census(&a, &b, skew::Arm::Balanced);
    skew::force(skew::Arm::Auto);
    assert_eq!(
        view_driven, driven,
        "the mapped view disagrees with the owned forest"
    );
    assert_eq!(
        view_balanced, view_driven,
        "the mapped view's arms disagree"
    );
    assert!(
        view_drive_census.key_probes > 0,
        "the mapped forest drive arm issued no key probes"
    );
    assert!(
        view_drive_census.total() < view_balanced_census.total(),
        "the mapped drive arm did not do less work: {view_drive_census:?} vs {view_balanced_census:?}"
    );
}

/// A balanced pair must NOT take the drive arm. Without this the chooser
/// could be "always gallop" and every test above would still pass.
#[cfg(feature = "bench-internals")]
#[test]
fn a_balanced_pair_stays_on_the_merge() {
    use bitmosaic::skew;

    let _held = instrument();

    let mut rng = Rng(0xBA1A_0001);
    let mut a_vals = BTreeSet::new();
    let mut b_vals = BTreeSet::new();
    while a_vals.len() < 2_000 {
        a_vals.insert((rng.next() % 65_536) as u32);
    }
    while b_vals.len() < 2_000 {
        b_vals.insert((rng.next() % 65_536) as u32);
    }
    let (a, b) = (
        build(&a_vals.into_iter().collect::<Vec<_>>()),
        build(&b_vals.into_iter().collect::<Vec<_>>()),
    );

    skew::force(skew::Arm::Auto);
    skew::reset();
    let answer = a.and_len(&b);
    let census = skew::census();
    assert!(answer > 0, "the balanced cell computed nothing");
    assert_eq!(
        census.gallop_probes, 0,
        "a 1:1 pair took the drive arm: {census:?}"
    );
    assert!(
        census.linear_positions > 0,
        "a 1:1 pair consumed no merge positions"
    );
}

/// The key crossover is inclusive: exactly 2:1 drives, while the nearest
/// representable ratio below it merges. Guard both lips on the owned forest
/// and on the mapped implementation used by FTS.
#[cfg(feature = "bench-internals")]
#[test]
fn key_crossover_routes_both_lips_in_owned_and_mapped_forms() {
    use bitmosaic::skew;

    let values = |keys: &[u16]| -> Vec<u32> {
        keys.iter()
            .flat_map(|&key| (0..8u32).map(move |i| (u32::from(key) << 16) | (i * 8_000 + 7)))
            .collect()
    };
    let small_keys: Vec<u16> = (0..32).map(|i| i * 4 + 1).collect();
    let exact_keys: Vec<u16> = (0..64).map(|i| i * 2).collect();
    let below_keys = &exact_keys[..63];
    let small = build(&values(&small_keys));
    let exact = build(&values(&exact_keys));
    let below = build(&values(below_keys));

    // The forest form, pinned for the same reason as above: the crossover
    // under test is the forest key plane's.
    let (small_bytes, exact_bytes, below_bytes) = (
        small.to_forest_bytes(),
        exact.to_forest_bytes(),
        below.to_forest_bytes(),
    );
    let (small_view, exact_view, below_view) = (
        BitmosaicView::open(&small_bytes).unwrap(),
        BitmosaicView::open(&exact_bytes).unwrap(),
        BitmosaicView::open(&below_bytes).unwrap(),
    );

    let _held = instrument();
    let owned_census = |large: &Bitmosaic| {
        skew::force(skew::Arm::Auto);
        skew::reset();
        let answer = small.and_len(large);
        (answer, skew::census())
    };
    let mapped_census = |large: &BitmosaicView<'_>| {
        skew::force(skew::Arm::Auto);
        skew::reset();
        let answer = small_view.and_len(large);
        (answer, skew::census())
    };

    let (exact_answer, exact_census) = owned_census(&exact);
    let (below_answer, below_census) = owned_census(&below);
    assert_eq!(exact_answer, 0);
    assert_eq!(below_answer, 0);
    assert_eq!(exact_census.key_positions, 0, "exact 2:1 owned pair merged");
    assert!(
        exact_census.key_probes > 0,
        "exact 2:1 owned pair did not drive"
    );
    assert_eq!(below_census.key_probes, 0, "below-2:1 owned pair drove");
    assert!(
        below_census.key_positions > 0,
        "below-2:1 owned pair did not merge"
    );

    let (exact_answer, exact_census) = mapped_census(&exact_view);
    let (below_answer, below_census) = mapped_census(&below_view);
    assert_eq!(exact_answer, 0);
    assert_eq!(below_answer, 0);
    assert_eq!(
        exact_census.key_positions, 0,
        "exact 2:1 mapped pair merged: {exact_census:?}"
    );
    assert!(
        exact_census.key_probes > 0,
        "exact 2:1 mapped pair did not drive: {exact_census:?}"
    );
    assert_eq!(
        below_census.key_probes, 0,
        "below-2:1 mapped pair drove: {below_census:?}"
    );
    assert!(
        below_census.key_positions > 0,
        "below-2:1 mapped pair did not merge: {below_census:?}"
    );
}
