//! `patched_len_bound` is an upper bound on the image of every set a patch
//! sequence can reach. A forest image is byte-equal to a fresh encoding of
//! its set, so the oracle is a fresh encoding of `(S ∪ A) \ R` for shapes
//! that stress each term: strides and runs that a single member breaks,
//! dense chunks past the Array limit, isolated members that each open a
//! chunk, and key spans that switch the slot plane on.

use std::collections::BTreeSet;

use bitmosaic::{
    Bitmosaic, BitmosaicView, EMPTY_U32_IMAGE, PatchScratch, patch, patched_len_bound,
};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    /// The low 32 bits of the next draw.
    fn word(&mut self) -> u32 {
        let [a, b, c, d, ..] = self.next().to_le_bytes();
        u32::from_le_bytes([a, b, c, d])
    }

    fn below_u32(&mut self, n: u32) -> u32 {
        self.word() % n
    }
}

/// Members of chunk `chunk` at the low bits of each offset.
fn in_chunk(chunk: u32, low: u64) -> u32 {
    (chunk << 16) | u32::try_from(low & 0xffff).expect("16 bits")
}

fn len(n: usize) -> u64 {
    u64::try_from(n).expect("a test set length fits u64")
}

/// One shaped part: a stride, a run, a dense chunk, isolated members, or a
/// stepped spread of single-member chunks.
fn part(rng: &mut Rng, out: &mut Vec<u32>) {
    let chunk = rng.below_u32(70);
    match rng.below(6) {
        0 => {
            let (start, step, n) = (rng.below(60_000), 1 + rng.below(63), 1 + rng.below(400));
            out.extend((0..n).map(|i| in_chunk(chunk, start + i * step)));
        }
        1 => {
            let (start, n) = (rng.below(60_000), 1 + rng.below(6000));
            out.extend((0..n).map(|i| in_chunk(chunk, start + i)));
        }
        2 => {
            let n = 4000 + rng.below(4000);
            out.extend((0..n).map(|_| in_chunk(chunk, rng.below(65_536))));
        }
        3 => {
            let n = 1 + rng.below(300);
            out.extend((0..n).map(|_| rng.word()));
        }
        4 => {
            let step = 1 + rng.below(1024);
            let n = 1 + rng.below(200);
            out.extend((0..n).map(|i| in_chunk(0, i * step) << 16));
        }
        _ => out.push(in_chunk(chunk, rng.below(65_536))),
    }
}

fn shaped(rng: &mut Rng, max_parts: u64) -> BTreeSet<u32> {
    let mut out = Vec::new();
    for _ in 0..rng.below(max_parts + 1) {
        part(rng, &mut out);
    }
    out.into_iter().collect()
}

#[test]
fn every_reachable_forest_stays_within_the_bound() {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    for case in 0..300 {
        let s = shaped(&mut rng, 6);
        let a = shaped(&mut rng, 4);
        let r = shaped(&mut rng, 3);
        let image = if s.is_empty() {
            EMPTY_U32_IMAGE.to_vec()
        } else {
            Bitmosaic::from_sorted(s.iter().copied()).to_forest_bytes()
        };
        let adds = len(a.difference(&s).count());
        let touches = adds + len(r.intersection(&s).count());
        let bound = patched_len_bound(&image, adds, touches).expect("u32 image");
        // Every set reachable by applying some of A and some of R.
        for reach in [&(&s | &a) - &r, &s | &a, &s - &r, &(&s - &r) | &a] {
            let len = if reach.is_empty() {
                4
            } else {
                len(Bitmosaic::from_sorted(reach.iter().copied()).forest_len())
            };
            assert!(
                len <= bound,
                "case {case}: forest {len} > bound {bound} (|S| {}, adds {adds}, touches {touches})",
                s.len()
            );
        }
    }
}

#[test]
fn elias_fano_roots_stay_within_the_bound() {
    let mut rng = Rng(0xD1B5_4A32_D192_ED03);
    let mut checked = 0;
    for _ in 0..3000 {
        let s: BTreeSet<u32> = (0..2 + rng.below(400)).map(|_| rng.word()).collect();
        let a: BTreeSet<u32> = (0..rng.below(200)).map(|_| rng.word()).collect();
        let image = Bitmosaic::from_sorted(s.iter().copied()).to_bytes();
        let view = BitmosaicView::open(&image).expect("fresh image opens");
        if !view.is_elias_fano() {
            continue;
        }
        let t = &s | &a;
        let fresh = Bitmosaic::from_sorted(t.iter().copied()).to_bytes();
        if !BitmosaicView::open(&fresh).expect("opens").is_elias_fano() {
            continue;
        }
        let adds = len(a.difference(&s).count());
        let bound = patched_len_bound(&image, adds, adds).expect("u32 image");
        assert!(
            len(fresh.len()) <= bound,
            "EF {} > bound {bound}",
            fresh.len()
        );
        checked += 1;
    }
    assert!(checked > 100, "too few Elias-Fano cases: {checked}");
}

/// A scratch reserved by `try_reserve_for` at the bound takes every batch that
/// reaches a set within that bound without growing one of its buffers.
#[test]
fn reserved_scratch_absorbs_every_reachable_patch() {
    let mut rng = Rng(0x5C2A_7E5E_12E5_0A11);
    for case in 0..200 {
        let s = shaped(&mut rng, 6);
        let a = shaped(&mut rng, 4);
        let r = shaped(&mut rng, 3);
        let image = if s.is_empty() {
            EMPTY_U32_IMAGE.to_vec()
        } else {
            Bitmosaic::from_sorted(s.iter().copied()).to_forest_bytes()
        };
        let adds = len(a.difference(&s).count());
        let touches = adds + len(r.intersection(&s).count());
        let bound = patched_len_bound(&image, adds, touches).expect("u32 image");
        let bound = usize::try_from(bound).expect("a bound fits in usize");
        let touched_chunks = usize::try_from(touches).expect("a touch count fits in usize");
        // The batch that reaches (S ∪ A) \ R: each add is new and not removed,
        // each remove is held.
        let batch_adds: Vec<u32> = a
            .iter()
            .copied()
            .filter(|v| !s.contains(v) && !r.contains(v))
            .collect();
        let batch_removes: Vec<u32> = r.intersection(&s).copied().collect();
        let mut scratch = PatchScratch::new();
        scratch
            .try_reserve_for(bound, touched_chunks)
            .expect("reserving one patch's scratch");
        let reserved = scratch.capacity_bytes();
        let mut slot = image.clone();
        slot.resize(bound, 0xa5);
        patch(&mut slot, &batch_adds, &batch_removes, &mut scratch)
            .expect("a reachable patch fits its bound");
        assert_eq!(
            scratch.capacity_bytes(),
            reserved,
            "case {case}: a reserved buffer grew"
        );
    }
}
