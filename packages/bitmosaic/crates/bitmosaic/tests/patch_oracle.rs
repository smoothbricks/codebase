//! The in-place patch path against the `BTreeSet` oracle.
//!
//! What must hold for `patch` to be allowed near a slot:
//!
//! 1. **The bytes are a fresh encoding's bytes, root for root.** After
//!    every batch a forest slot is byte-equal to `to_forest_bytes` of the
//!    surviving set, and an Elias-Fano slot to `to_bytes` of it for as long
//!    as that root is the sealed choice. Histories are seeded and cross
//!    every arm boundary the ladder has — the 4,096-member Array/Words
//!    edge, the Cone fit, a full stride-1 chunk losing and regaining one
//!    member, a `Words` window growing past either end and shrinking back,
//!    chunks created and emptied, the slot-plane threshold — and,
//!    on the Elias-Fano root, the low-width change and the plane's end
//!    moving in both directions.
//! 2. **Every read agrees with the oracle**, through the verified view.
//! 3. **The report is exact**: adds that were fresh, removes that held.
//! 4. **Refusals touch nothing**: a slot one byte short, a slot holding no
//!    string, a batch out of order.
//! 5. **A warm scratch allocates nothing.** The counting allocator proves
//!    it can see an allocation in the same test.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::BTreeSet;

use bitmosaic::{
    BatchOutcome, Bitmosaic, BitmosaicView, PatchError, PatchScratch, patch, patch_witnessed,
};

#[test]
fn run_patch_split_bridge_spill_and_empty_preserve_witnesses() {
    let initial = (0..4096u32).chain(32768..36864);
    let mut oracle: BTreeSet<_> = initial.collect();
    let mut slot = Bitmosaic::from_sorted(oracle.iter().copied()).to_forest_bytes();
    slot.resize(32 * 1024, 0xa5);
    let mut scratch = PatchScratch::new();
    let batches: Vec<(Vec<u32>, Vec<u32>)> = vec![
        (vec![4096, 32767, 65535], vec![1, 2048, 33000]),
        (vec![1, 2048, 33000], vec![4096, 32767]),
        ((4096..32768).collect(), vec![]),
        (vec![], (1..36864).step_by(2).collect()),
        ((1..36864).step_by(2).collect(), vec![2, 65535]),
        (vec![2, 65535], vec![2, 65535]),
        (vec![], (0..36864).collect()),
    ];
    for (step, (adds, removes)) in batches.into_iter().enumerate() {
        let add_new: Vec<_> = adds.iter().map(|value| !oracle.contains(value)).collect();
        oracle.extend(adds.iter().copied());
        let remove_held: Vec<_> = removes.iter().map(|value| oracle.contains(value)).collect();
        for value in &removes {
            oracle.remove(value);
        }
        let mut add_bits = vec![0u64; adds.len().div_ceil(64)];
        let mut remove_bits = vec![0u64; removes.len().div_ceil(64)];
        let mut witness = BatchOutcome::new(&mut add_bits, &mut remove_bits);
        let report =
            patch_witnessed(&mut slot, &adds, &removes, &mut scratch, &mut witness).unwrap();
        let canonical = Bitmosaic::from_sorted(oracle.iter().copied()).to_forest_bytes();
        assert_eq!(&slot[..report.serialized_len], canonical, "step {step}");
        assert_eq!(
            report.added as usize,
            add_new.iter().filter(|held| **held).count()
        );
        assert_eq!(
            report.removed as usize,
            remove_held.iter().filter(|held| **held).count()
        );
        for (i, expected) in add_new.into_iter().enumerate() {
            assert_eq!((add_bits[i / 64] >> (i % 64)) & 1 != 0, expected);
        }
        for (i, expected) in remove_held.into_iter().enumerate() {
            assert_eq!((remove_bits[i / 64] >> (i % 64)) & 1 != 0, expected);
        }
    }
    assert_eq!(&slot[..4], &bitmosaic::EMPTY_U32_IMAGE);
}

#[test]
fn short_ef_low_planes_patch_without_guard_padding() {
    let mut scratch = PatchScratch::new();
    for n in [65u32, 64, 63, 31, 15, 7] {
        let stride = u32::MAX / (n + 1);
        let values: Vec<_> = (0..n).map(|i| 1 + i * stride + i % 3).collect();
        let owned = Bitmosaic::from_sorted(values.iter().copied());
        let image = owned.to_bytes();
        assert!(
            BitmosaicView::open(&image).unwrap().is_elias_fano(),
            "EF fixture n={n}"
        );
        let mut slot = image;
        slot.resize(slot.len() + 1024, 0xa5);
        let mut oracle: BTreeSet<_> = values.iter().copied().collect();
        let adds = [values[n as usize / 2] + 1];
        let removes = [values[1]];
        oracle.extend(adds);
        oracle.remove(&removes[0]);
        let report = patch(&mut slot, &adds, &removes, &mut scratch).unwrap();
        assert_eq!(
            &slot[..report.serialized_len],
            Bitmosaic::from_sorted(oracle.iter().copied()).to_bytes()
        );
        assert_eq!(
            BitmosaicView::open_verified(&slot)
                .unwrap()
                .range()
                .collect::<Vec<_>>(),
            oracle.into_iter().collect::<Vec<_>>()
        );
    }
}

#[test]
fn ef_zero_sample_boundaries_survive_a_shrinking_last_bucket() {
    let mut scratch = PatchScratch::new();
    let mut rng = Rng(0xEF_0064);
    for zeros in [65u32, 64, 63] {
        // Forty members with 24 low bits, then a gap before the last two
        // buckets. Zero 0 follows one member; zero 64, when present, follows
        // all forty. These are canonical sample positions, not encoder output.
        let values: Vec<_> = (0..38)
            .chain([zeros - 2, zeros - 1])
            .map(|bucket| bucket << 24)
            .collect();
        let mut oracle: BTreeSet<_> = values.iter().copied().collect();
        let mut slot = Bitmosaic::from_sorted(values.iter().copied()).to_bytes();
        assert!(BitmosaicView::open_verified(&slot).unwrap().is_elias_fano());
        let samples: &[u8] = if zeros == 65 {
            &[1, 0, 0, 0, 104, 0, 0, 0]
        } else {
            &[1, 0, 0, 0]
        };
        assert_eq!(&slot[slot.len() - samples.len()..], samples);
        slot.resize(slot.len() + 1024, 0xa5);
        let last = values[39];
        oracle.remove(&last);
        let report = patch(&mut slot, &[], &[last], &mut scratch).unwrap();
        assert_eq!((report.added, report.removed, report.len), (0, 1, 39));
        let canonical = Bitmosaic::from_sorted(oracle.iter().copied()).to_bytes();
        assert_eq!(&slot[..report.serialized_len], canonical);
        check_reads(
            "zero sample boundary",
            zeros as usize,
            &slot[..report.serialized_len],
            &oracle,
            &mut rng,
        );
    }
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

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

fn sorted_unique(mut v: Vec<u32>) -> Vec<u32> {
    v.sort_unstable();
    v.dedup();
    v
}

/// Scattered at density 1/12: the wave-2.5 fixture, which the encoder
/// publishes as an Elias-Fano root.
fn scattered(n: u32, seed: u64) -> Vec<u32> {
    let mut rng = Rng(seed);
    let mut seen = BTreeSet::new();
    while seen.len() < n as usize {
        seen.insert(rng.below(u64::from(n) * 12 + 97) as u32);
    }
    seen.into_iter().collect()
}

/// A history: `name`, the initial set, the universe the batches draw
/// from, the largest batch, and whether the slot starts as the sealed
/// form (an Elias-Fano root where that is smaller) or the mutable forest.
struct Shape {
    name: &'static str,
    initial: Vec<u32>,
    universe: u32,
    batch_max: usize,
    sealed: bool,
}

fn shapes() -> Vec<Shape> {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    vec![
        Shape {
            name: "empty",
            initial: Vec::new(),
            universe: 200_000,
            batch_max: 40,
            sealed: false,
        },
        Shape {
            name: "wave-scattered-10k (EF root)",
            initial: scattered(10_000, 0x9E37_79B9_7F4A_7C15 ^ 10_000),
            universe: 10_000 * 12 + 97,
            batch_max: 512,
            sealed: true,
        },
        Shape {
            name: "wave-scattered-10k (forest slot)",
            initial: scattered(10_000, 0x9E37_79B9_7F4A_7C15 ^ 10_000),
            universe: 10_000 * 12 + 97,
            batch_max: 512,
            sealed: false,
        },
        Shape {
            // Sparse over five chunks with the second and fourth empty, so
            // batches create chunks inside an Elias-Fano root's range and
            // empty them again: the directory geometry of the forest arm
            // moves under the root decision.
            name: "ef-with-gaps",
            initial: sorted_unique(
                (0..2_000u32)
                    .map(|_| {
                        let chunk = [0u32, 2, 4][rng.below(3) as usize];
                        chunk * 65_536 + rng.below(65_536) as u32
                    })
                    .collect(),
            ),
            universe: 5 * 65_536,
            batch_max: 128,
            sealed: true,
        },
        Shape {
            // A full stride-1 chunk beside a second one starting late:
            // every remove of one member turns Stride into Words and every
            // add of it back turns Words into Stride.
            name: "two-strides",
            initial: (0..65_536u32).chain(70_000..100_000).collect(),
            universe: 140_000,
            batch_max: 64,
            sealed: false,
        },
        Shape {
            // Cone-fit chunks (near-arithmetic with small jitter) and an
            // Array chunk sitting on the 4,096 edge.
            name: "cone-and-array-edge",
            initial: {
                let mut v: Vec<u32> = (0..1_000u32)
                    .map(|i| i * 60 + (rng.below(3) as u32))
                    .collect();
                v.extend((0..4_090u32).map(|_| 65_536 + rng.below(65_536) as u32));
                sorted_unique(v)
            },
            universe: 131_072,
            batch_max: 64,
            sealed: false,
        },
        Shape {
            // Dense-ish: five Words chunks at density 1/2, the shape a VM
            // ordinal domain with holes takes.
            name: "words-density-half",
            initial: sorted_unique((0..160_000u32).map(|_| rng.below(320_000) as u32).collect()),
            universe: 320_000,
            batch_max: 4_096,
            sealed: false,
        },
        Shape {
            // One Words chunk occupying the middle of its window's range,
            // with adds drawn from the whole chunk: the window grows at
            // both ends where a batch lands past it, and the sparse blocks
            // it grew into empty again under removes, so it shrinks.
            name: "words-window-moves",
            initial: sorted_unique(
                (0..12_000u32)
                    .map(|_| 12_800 + rng.below(25_600) as u32)
                    .collect(),
            ),
            universe: 65_536,
            batch_max: 512,
            sealed: false,
        },
        Shape {
            // 100 chunks on a gapped key run: the slot plane is live.
            name: "slot-plane",
            initial: sorted_unique(
                (0..100u32)
                    .flat_map(|c| {
                        let base = c * 3 * 65_536;
                        (0..50u32).map(move |i| base + i * 997 % 65_536)
                    })
                    .collect(),
            ),
            universe: 300 * 65_536,
            batch_max: 256,
            sealed: false,
        },
    ]
}

// ── the oracle ───────────────────────────────────────────────────────────

/// The bytes a slot holding `oracle` must show: the forest form for a
/// forest slot; for an Elias-Fano slot, the sealed form while that is
/// still an Elias-Fano root (a root the patch keeps once the sealed choice
/// would differ has no fresh twin to compare against, and is checked by
/// reads alone), or the forest once fewer than two members remain.
fn canonical(oracle: &BTreeSet<u32>, slot_is_ef: bool) -> Option<Vec<u8>> {
    let owned = Bitmosaic::from_sorted(oracle.iter().copied());
    if !slot_is_ef {
        return Some(owned.to_forest_bytes());
    }
    let sealed = owned.to_bytes();
    BitmosaicView::open(&sealed)
        .expect("open")
        .is_elias_fano()
        .then_some(sealed)
}

fn check_reads(name: &str, step: usize, bytes: &[u8], oracle: &BTreeSet<u32>, rng: &mut Rng) {
    let view = BitmosaicView::open_verified(bytes)
        .unwrap_or_else(|| panic!("{name}/{step}: the patched string does not verify"));
    assert_eq!(view.len(), oracle.len() as u64, "{name}/{step}: len");
    let members: Vec<u32> = oracle.iter().copied().collect();
    for _ in 0..64 {
        if !members.is_empty() {
            let k = rng.below(members.len() as u64) as usize;
            let v = members[k];
            assert!(view.contains(v), "{name}/{step}: contains({v})");
            assert_eq!(view.rank(v), k as u64, "{name}/{step}: rank({v})");
            assert_eq!(view.select(k as u64), Some(v), "{name}/{step}: select({k})");
        }
        let probe = rng.next() as u32;
        assert_eq!(
            view.contains(probe),
            oracle.contains(&probe),
            "{name}/{step}: contains({probe})"
        );
        assert_eq!(
            view.rank(probe) as usize,
            members.partition_point(|value| *value < probe),
            "{name}/{step}: rank({probe})"
        );
    }
    let mut walked = Vec::with_capacity(members.len());
    view.for_each(|v| walked.push(v));
    assert_eq!(walked, members, "{name}/{step}: walk");
}

/// Drive one shape through `steps` seeded batches.
fn history(shape: &Shape, steps: usize, seed: u64, scratch: &mut PatchScratch) {
    let name = shape.name;
    let mut rng = Rng(seed);
    let mut oracle: BTreeSet<u32> = shape.initial.iter().copied().collect();
    let mut slot = vec![0u8; 2 * 1024 * 1024];
    let initial = Bitmosaic::from_sorted(oracle.iter().copied());
    let mut len = if shape.sealed {
        initial.write_into_slice(&mut slot).expect("initial fits")
    } else {
        initial
            .write_forest_into_slice(&mut slot)
            .expect("initial fits")
    };
    let mut is_ef = BitmosaicView::open(&slot).expect("open").is_elias_fano();
    assert!(
        !shape.sealed || is_ef,
        "{name}: a sealed shape must start as an Elias-Fano root"
    );
    assert_eq!(&slot[..len], &canonical(&oracle, is_ef).expect("fresh")[..]);

    for step in 0..steps {
        let members: Vec<u32> = oracle.iter().copied().collect();
        let roll = rng.next();
        let batch = 1 + rng.below(shape.batch_max as u64) as usize;
        let mut adds = Vec::new();
        let mut removes = Vec::new();
        if roll % 3 != 1 {
            for _ in 0..batch {
                // Mostly inside the universe, occasionally far outside it
                // so chunks appear where none was.
                let v = if rng.below(16) == 0 {
                    rng.next() as u32
                } else {
                    rng.below(u64::from(shape.universe)) as u32
                };
                adds.push(v);
            }
        }
        if !roll.is_multiple_of(3) && !members.is_empty() {
            for _ in 0..batch {
                let v = if rng.below(4) == 0 {
                    rng.below(u64::from(shape.universe)) as u32
                } else {
                    members[rng.below(members.len() as u64) as usize]
                };
                removes.push(v);
            }
        }
        // Every so often, empty a whole chunk or the whole set.
        if step % 17 == 16 && !members.is_empty() {
            let key = members[rng.below(members.len() as u64) as usize] >> 16;
            removes.extend(members.iter().copied().filter(|v| v >> 16 == key));
        }
        if step % 53 == 52 {
            removes.extend(members.iter().copied());
        }
        let adds = sorted_unique(adds);
        let removes = sorted_unique(removes);

        // Every other step the patch is witnessed, so the per-value answer
        // is checked on every path the plain patch takes.
        let mut adds_new = vec![0u64; adds.len().div_ceil(64)];
        let mut removes_held = vec![0u64; removes.len().div_ceil(64)];
        let report = if step % 2 == 0 {
            let mut outcome = BatchOutcome::new(&mut adds_new, &mut removes_held);
            let report = patch_witnessed(&mut slot, &adds, &removes, scratch, &mut outcome)
                .unwrap_or_else(|e| panic!("{name}/{step}: {e}"));
            for (i, v) in adds.iter().enumerate() {
                assert_eq!(
                    outcome.add_was_new(i),
                    !oracle.contains(v),
                    "{name}/{step}: add {v} witnessed"
                );
            }
            for (i, v) in removes.iter().enumerate() {
                assert_eq!(
                    outcome.remove_was_held(i),
                    oracle.contains(v) || adds.binary_search(v).is_ok(),
                    "{name}/{step}: remove {v} witnessed"
                );
            }
            report
        } else {
            patch(&mut slot, &adds, &removes, scratch)
                .unwrap_or_else(|e| panic!("{name}/{step}: {e}"))
        };
        let expected_added = adds.iter().filter(|v| oracle.insert(**v)).count() as u32;
        let expected_removed = removes.iter().filter(|v| oracle.remove(v)).count() as u32;
        assert_eq!(report.added, expected_added, "{name}/{step}: added");
        assert_eq!(report.removed, expected_removed, "{name}/{step}: removed");
        assert_eq!(report.len, oracle.len() as u64, "{name}/{step}: len");
        len = report.serialized_len;

        // The root is kept, except that an Elias-Fano root left with fewer
        // than two members becomes a forest.
        let now_ef = BitmosaicView::open(&slot).expect("open").is_elias_fano();
        assert_eq!(
            now_ef,
            is_ef && oracle.len() >= 2,
            "{name}/{step}: the patch must keep the root"
        );
        is_ef = now_ef;
        if let Some(want) = canonical(&oracle, is_ef) {
            assert_eq!(len, want.len(), "{name}/{step}: serialized_len");
            if slot[..len] != want[..] {
                let at = slot[..len]
                    .iter()
                    .zip(&want)
                    .position(|(a, b)| a != b)
                    .unwrap_or(len);
                panic!(
                    "{name}/{step}: patched bytes differ from a fresh encoding at byte {at} of {len} (adds {}, removes {}, root ef={is_ef})",
                    adds.len(),
                    removes.len(),
                );
            }
        }
        check_reads(name, step, &slot[..len], &oracle, &mut rng);
    }
}

#[test]
fn patched_bytes_are_a_fresh_freeze_and_reads_match_the_oracle() {
    let mut scratch = PatchScratch::new();
    for (i, shape) in shapes().iter().enumerate() {
        history(shape, 120, 0x5EED_0000 + i as u64, &mut scratch);
    }
}

/// The two boundaries a single member decides: a chunk at exactly
/// `ARRAY_MAX` members gains one and loses it again; a full stride-1 chunk
/// loses one and gets it back. Each step is in place on a two-chunk
/// forest, so the tail move is exercised with a footprint that changes by
/// kilobytes in both directions.
#[test]
fn a_single_member_moves_a_chunk_across_every_arm_boundary() {
    let mut scratch = PatchScratch::new();
    let mut slot = vec![0u8; 64 * 1024];
    let base: Vec<u32> = (0..4_096u32)
        .map(|i| i * 16 + 3)
        .chain(65_536..131_072)
        .chain(200_000..200_100)
        .collect();
    let mut oracle: BTreeSet<u32> = base.iter().copied().collect();
    Bitmosaic::from_sorted(base.iter().copied())
        .write_forest_into_slice(&mut slot)
        .expect("fits");
    let mut roll = Rng(7);
    let edits: [(&str, u32); 8] = [
        ("array to words", 5),
        ("words back to array", 5),
        ("stride to words", 65_536 + 40_000),
        ("words back to stride", 65_536 + 40_000),
        ("stride keeps stride at its end", 131_072),
        ("and loses it", 131_072),
        ("a new chunk far past the end", 9_000_000),
        ("and it empties again", 9_000_000),
    ];
    for (what, v) in edits {
        let (adds, removes): (Vec<u32>, Vec<u32>) = if oracle.contains(&v) {
            oracle.remove(&v);
            (vec![], vec![v])
        } else {
            oracle.insert(v);
            (vec![v], vec![])
        };
        let report = patch(&mut slot, &adds, &removes, &mut scratch).expect(what);
        let want = canonical(&oracle, false).expect("forest");
        assert_eq!(report.serialized_len, want.len(), "{what}: length");
        assert_eq!(&slot[..want.len()], &want[..], "{what}: bytes");
        check_reads(what, 0, &slot[..want.len()], &oracle, &mut roll);
    }
}

#[test]
fn refusals_touch_nothing() {
    let mut scratch = PatchScratch::new();
    let set: Vec<u32> = (0..3_000u32).map(|i| i * 7).collect();
    let owned = Bitmosaic::from_sorted(set.iter().copied());
    let need = owned.forest_len();

    // Capacity: a slot exactly the string, and a batch that grows it.
    let mut slot = vec![0u8; need];
    owned
        .write_forest_into_slice(&mut slot)
        .expect("fits exactly");
    let before = slot.clone();
    let grown: BTreeSet<u32> = set
        .iter()
        .copied()
        .chain([21_001, 21_005, 21_009])
        .collect();
    let needed = canonical(&grown, false).expect("forest").len();
    assert!(needed > need, "the fixture must grow the string");
    assert_eq!(
        patch(&mut slot, &[21_001, 21_005, 21_009], &[], &mut scratch),
        Err(PatchError::Capacity { needed }),
        "a short slot is refused with the length it needs"
    );
    assert_eq!(slot, before, "a refused patch changes nothing");
    slot.resize(needed, 0);
    let report = patch(&mut slot, &[21_001, 21_005, 21_009], &[], &mut scratch)
        .expect("the grown slot takes the same batch");
    assert_eq!(report.serialized_len, needed);
    assert_eq!(
        &slot[..needed],
        &canonical(&grown, false).expect("forest")[..]
    );

    // Order: an unsorted batch, before anything is read.
    let before = slot.clone();
    assert_eq!(
        patch(&mut slot, &[5, 3], &[], &mut scratch),
        Err(PatchError::Unsorted)
    );
    assert_eq!(
        patch(&mut slot, &[], &[9, 9], &mut scratch),
        Err(PatchError::Unsorted),
        "a duplicate is not strictly ascending"
    );
    assert_eq!(slot, before);

    // A slot holding no string.
    let mut empty = vec![0u8; 256];
    assert_eq!(
        patch(&mut empty, &[1], &[], &mut scratch),
        Err(PatchError::Malformed)
    );
    assert!(empty.iter().all(|b| *b == 0));

    // The empty set is a string, and patching it works.
    let mut fresh = vec![0u8; 256];
    let len = Bitmosaic::from_sorted(std::iter::empty())
        .write_forest_into_slice(&mut fresh)
        .expect("the empty string fits");
    let report =
        patch(&mut fresh, &[10, 20], &[20, 30], &mut scratch).expect("patch the empty set");
    assert_eq!((report.added, report.removed, report.len), (2, 1, 1));
    assert_eq!(
        &fresh[..report.serialized_len],
        &canonical(&[10].into(), false).expect("forest")[..]
    );
    assert!(len < report.serialized_len);
}

/// Zero allocations on a warm scratch, for both roots, on batches of one
/// and of thousands — and the instrument can see an allocation.
#[test]
fn a_warm_scratch_allocates_nothing() {
    assert!(
        allocations_during(|| {
            std::hint::black_box(Vec::<u8>::with_capacity(64));
        }) > 0,
        "the instrument must be able to report an allocation"
    );
    for shape in shapes().iter().filter(|s| !s.initial.is_empty()) {
        let name = shape.name;
        let mut slot = vec![0u8; 2 * 1024 * 1024];
        let owned = Bitmosaic::from_sorted(shape.initial.iter().copied());
        if shape.sealed {
            owned.write_into_slice(&mut slot).expect("fits");
        } else {
            owned.write_forest_into_slice(&mut slot).expect("fits");
        }
        let mut scratch = PatchScratch::with_capacity(slot.len(), 4_096);
        let mut rng = Rng(0xA110C);
        let batches: Vec<(Vec<u32>, Vec<u32>)> = (0..6)
            .map(|i| {
                let n = [1usize, 64, 4_096][i % 3];
                let adds = sorted_unique(
                    (0..n)
                        .map(|_| rng.below(u64::from(shape.universe)) as u32)
                        .collect(),
                );
                let removes = sorted_unique(
                    (0..n)
                        .map(|_| shape.initial[rng.below(shape.initial.len() as u64) as usize])
                        .collect(),
                );
                (adds, removes)
            })
            .collect();
        // Warm: the first batch may reach the buffers' high-water mark.
        patch(&mut slot, &batches[0].0, &batches[0].1, &mut scratch).expect("warm");
        for (i, (adds, removes)) in batches.iter().enumerate().skip(1) {
            let allocations = allocations_during(|| {
                patch(&mut slot, adds, removes, &mut scratch).expect("patch");
            });
            assert_eq!(
                allocations,
                0,
                "{name}: batch {i} ({} adds, {} removes) allocated",
                adds.len(),
                removes.len()
            );
        }
    }
}

// ── tight slots ──────────────────────────────────────────────────────────

/// A forest in a slot whose capacity is exactly the patched string's
/// length, with a lower chunk shrinking while a higher one grows. The
/// final string fits; the tail move of a descending in-place walk would
/// need more (the higher chunk grows before the lower one gives its
/// bytes back), so the patch must find another way to the same bytes
/// rather than overrun the slot.
///
/// `layouts` are (shrinking chunk, growing chunk, chunk count); an
/// untouched last chunk puts the growing chunk's tail move in the middle
/// of the string.
fn tight_slot_layouts() -> Vec<(usize, usize, usize)> {
    vec![(0, 1, 2), (0, 1, 3), (0, 2, 3), (1, 2, 3)]
}

/// The initial set for a layout: `chunks` scattered Array chunks of 100
/// members each, and the batch: the first 4 members of `shrink` removed,
/// 64 scattered adds into `grow`.
fn tight_slot_case(
    shrink: usize,
    grow: usize,
    chunks: usize,
) -> (BTreeSet<u32>, Vec<u32>, Vec<u32>) {
    let mut rng = Rng(0x7167_4A5E ^ (chunks as u64) << 8 ^ grow as u64);
    let mut initial = BTreeSet::new();
    for c in 0..chunks as u32 {
        let mut chunk = BTreeSet::new();
        while chunk.len() < 100 {
            chunk.insert((c << 16) + rng.below(8_000) as u32);
        }
        initial.extend(chunk);
    }
    let removes: Vec<u32> = initial
        .range((shrink as u32) << 16..)
        .take(4)
        .copied()
        .collect();
    let adds = sorted_unique(
        (0..64)
            .map(|_| ((grow as u32) << 16) + 8_000 + rng.below(50_000) as u32)
            .collect(),
    );
    (initial, adds, removes)
}

#[test]
fn a_tight_slot_takes_a_batch_whose_transient_would_not_fit() {
    let mut scratch = PatchScratch::new();
    for (shrink, grow, chunks) in tight_slot_layouts() {
        let what = format!("shrink {shrink}, grow {grow}, {chunks} chunks");
        let (initial, adds, removes) = tight_slot_case(shrink, grow, chunks);
        let old = canonical(&initial, false).expect("forest");
        let mut only_removes = initial.clone();
        for r in &removes {
            only_removes.remove(r);
        }
        let mut only_adds = initial.clone();
        only_adds.extend(adds.iter().copied());
        let mut result = only_adds.clone();
        for r in &removes {
            result.remove(r);
        }
        let want = canonical(&result, false).expect("forest");
        let shrunk = canonical(&only_removes, false).expect("forest").len();
        let grown = canonical(&only_adds, false).expect("forest").len();
        assert!(
            shrunk < old.len(),
            "{what}: the removes must shrink a chunk"
        );
        assert!(
            grown > want.len(),
            "{what}: the adds alone must need more than the patched string"
        );

        // Exactly the patched length: the transient peak of a descending
        // in-place walk would be `grown`, which does not fit.
        let mut slot = vec![0u8; want.len()];
        slot[..old.len()].copy_from_slice(&old);
        let report = patch(&mut slot, &adds, &removes, &mut scratch)
            .unwrap_or_else(|e| panic!("{what}: the patched string fits, yet {e}"));
        assert_eq!(report.serialized_len, want.len(), "{what}: length");
        assert_eq!(&slot[..want.len()], &want[..], "{what}: bytes");
        assert_eq!(
            (report.added, report.removed, report.len),
            (adds.len() as u32, 4, result.len() as u64),
            "{what}: report"
        );
        check_reads(&what, 0, &slot[..want.len()], &result, &mut Rng(1));

        // One byte short: refused with the patched length, bytes untouched.
        let mut short = vec![0u8; want.len() - 1];
        short[..old.len()].copy_from_slice(&old);
        let before = short.clone();
        assert_eq!(
            patch(&mut short, &adds, &removes, &mut scratch),
            Err(PatchError::Capacity { needed: want.len() }),
            "{what}: a slot one byte short is refused with the length the string needs"
        );
        assert_eq!(short, before, "{what}: a refused patch changes nothing");
    }
}

/// A seeded history on a slot that is never larger than it must be: after
/// every batch the slot is cut to the string, and before every batch it
/// is grown to exactly the larger of the old and the patched length. Every
/// growth batch then meets the transient case whenever a lower chunk
/// shrinks while a higher one grows.
#[test]
fn a_history_on_a_slot_that_is_never_larger_than_it_must_be() {
    let mut scratch = PatchScratch::new();
    let mut rng = Rng(0xB1A5_ED5E_ED00_0945);
    let universe = 6u32 << 16;
    let mut oracle: BTreeSet<u32> = (0..3_000)
        .map(|_| rng.below(u64::from(universe)) as u32)
        .collect();
    let mut slot = canonical(&oracle, false).expect("forest");
    for step in 0..400 {
        let members: Vec<u32> = oracle.iter().copied().collect();
        let batch = 1 + rng.below(96) as usize;
        let roll = rng.next() % 3;
        let adds = if roll == 1 {
            Vec::new()
        } else {
            sorted_unique(
                (0..batch)
                    .map(|_| rng.below(u64::from(universe)) as u32)
                    .collect(),
            )
        };
        let removes = if roll == 2 {
            Vec::new()
        } else {
            sorted_unique(
                (0..batch)
                    .map(|_| members[rng.below(members.len() as u64) as usize])
                    .collect(),
            )
        };
        let mut result = oracle.clone();
        result.extend(adds.iter().copied());
        for r in &removes {
            result.remove(r);
        }
        let want = canonical(&result, false).expect("forest");
        let old_len = slot.len();
        slot.resize(old_len.max(want.len()), 0);
        let report = patch(&mut slot, &adds, &removes, &mut scratch)
            .unwrap_or_else(|e| panic!("tight/{step}: {e}"));
        assert_eq!(report.serialized_len, want.len(), "tight/{step}: length");
        assert_eq!(&slot[..want.len()], &want[..], "tight/{step}: bytes");
        assert_eq!(report.len, result.len() as u64, "tight/{step}: len");
        check_reads("tight", step, &slot[..want.len()], &result, &mut rng);
        slot.truncate(want.len());
        oracle = result;
    }
}

// ── a Words window that shrinks where it lies ────────────────────────────

/// Removes that empty an end block of a `Words` chunk shrink its window
/// in place; a batch that grows one end while the other shrinks takes the
/// general path. Either way the bytes are a fresh freeze's. The chunk
/// sits below an Array chunk so the tail moves after every edit.
#[test]
fn a_words_window_shrinks_where_it_lies() {
    let mut scratch = PatchScratch::new();
    let mut rng = Rng(0x5EED_5A1E);
    let base = 3u32 << 16;
    let mut set: BTreeSet<u32> = (0..12_000u32)
        .filter(|_| rng.next() & 1 == 1)
        .map(|v| base + v)
        .collect();
    set.extend((0..50u32).map(|i| (5 << 16) + i * 7 + (i * i) % 5));
    let mut slot = vec![0u8; 64 * 1024];
    Bitmosaic::from_sorted(set.iter().copied())
        .write_forest_into_slice(&mut slot)
        .expect("fits");
    // Removes name a 512-value block of the CURRENT set, so each step's
    // batch is built when it runs.
    let steps: Vec<(&str, Vec<u32>, (u32, u32))> = vec![
        ("top block", Vec::new(), (23 * 512, 24 * 512)),
        ("bottom block", Vec::new(), (0, 512)),
        ("three top blocks", Vec::new(), (20 * 512, 23 * 512)),
        (
            "bottom block out, adds above the top",
            (0..40u32).map(|i| base + 20 * 512 + i * 11).collect(),
            (512, 1024),
        ),
        (
            "top block out, adds inside",
            (0..40u32).map(|i| base + 5 * 512 + i * 13).collect(),
            (20 * 512, 21 * 512),
        ),
    ];
    for (step, (what, adds, (lo, hi))) in steps.into_iter().enumerate() {
        let removes: Vec<u32> = set.range(base + lo..base + hi).copied().collect();
        assert!(
            !removes.is_empty(),
            "{what}: the fixture must remove something"
        );
        let mut result = set.clone();
        result.extend(adds.iter().copied());
        for r in &removes {
            result.remove(r);
        }
        assert!(
            result.range(base..base + 65_536).count() > 4_096,
            "{what}: the chunk must stay Words"
        );
        let want = canonical(&result, false).expect("forest");
        let report = patch(&mut slot, &adds, &removes, &mut scratch)
            .unwrap_or_else(|e| panic!("{what}: {e}"));
        assert_eq!(report.serialized_len, want.len(), "{what}: length");
        assert_eq!(&slot[..want.len()], &want[..], "{what}: bytes");
        assert_eq!(report.removed as usize, removes.len(), "{what}: removed");
        check_reads(what, step, &slot[..want.len()], &result, &mut rng);
        set = result;
    }
}
