//! Differential oracle steps over operation lists (D5; §23.1 of the
//! ordered-index study's correctness harness).
//!
//! A step decodes an operation list from bytes, applies each operation to a
//! `BTreeSet` and to bitmosaic, and checks every answer after every
//! operation. [`patch_step`] edits one image in place through [`patch`] and
//! [`patch_witnessed`] and compares it byte for byte with a fresh encoding;
//! [`and_step`] intersects two sets on every skew arm, owned, borrowed and
//! through the u64 tier. Both check reads (`contains`, `rank`, `select`,
//! `successor`, cursor seeks, the walk) on the owned and borrowed forms.
//!
//! The fuzz targets in `fuzz/` drive these steps with coverage-guided bytes;
//! the unit tests below drive them from fixed seeds in the default test
//! lane, where the skew arms are forced too. Present only with the skew
//! instrument (`cfg(test)` or `bench-internals`): the forced arms need its
//! switch, and a shipped build carries neither.
//!
//! Values come from a recipe aimed at the ladder, not from uniform bytes:
//! chunk keys from a pool of four, three of them among the first eight
//! chunks, so operations meet chunks already present; low halves as runs,
//! strides, jittered strides, scatters and single values, the shapes that
//! select each container arm, at sizes that cross `ARRAY_MAX` and
//! `CONE_MAX_LEN`.

use std::collections::BTreeSet;

use crate::skew::{Arm, force};
use crate::{
    BatchOutcome, Bitmosaic, Bitmosaic64, Bitmosaic64View, BitmosaicView, PatchScratch, Range,
    patch, patch_witnessed,
};

/// Operations one input may run; bounds a step's time, not its reach.
const MAX_STEPS: usize = 16;
/// Room for any image the recipe can build: at most four chunks, whose
/// Elias-Fano form stays under 2 bytes a member.
const SLOT_BYTES: usize = 2 * 1024 * 1024;

/// The fuzzer's bytes, read front to back; reads past the end are zero.
struct Input<'a>(&'a [u8]);

impl Input<'_> {
    fn u8(&mut self) -> u8 {
        let Some((&byte, rest)) = self.0.split_first() else {
            return 0;
        };
        self.0 = rest;
        byte
    }

    fn u16(&mut self) -> u16 {
        u16::from_le_bytes([self.u8(), self.u8()])
    }

    fn u32(&mut self) -> u32 {
        u32::from(self.u16()) | u32::from(self.u16()) << 16
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// The chunk-key pool: three among the first eight chunks, one anywhere.
fn keys(input: &mut Input<'_>) -> [u16; 4] {
    [
        u16::from(input.u8() & 7),
        u16::from(input.u8() & 7),
        u16::from(input.u8() & 7),
        input.u16(),
    ]
}

/// One batch from the recipe, strictly ascending, into `out`.
fn values(input: &mut Input<'_>, keys: &[u16; 4], out: &mut Vec<u32>) {
    out.clear();
    let key = u32::from(keys[usize::from(input.u8() & 3)]) << 16;
    let shape = input.u8() % 5;
    let first = u32::from(input.u16());
    let mut seed = u64::from(input.u32()) | 1;
    let mut push = |low: u32| {
        if low <= 0xFFFF {
            out.push(key | low);
        }
    };
    match shape {
        // A run: a stride-1 Stride; several runs in one chunk make Runs.
        0 => (first..first + u32::from(input.u16()) % 4_500).for_each(&mut push),
        // An arithmetic progression: Stride.
        1 => {
            let step = 1 + u32::from(input.u8());
            (0..u32::from(input.u16()) % 1_100).for_each(|i| push(first + i * step));
        }
        // A jittered progression: residuals within the Cone arm's bound.
        2 => {
            let step = 2 + u32::from(input.u8());
            (0..u32::from(input.u16()) % 1_100)
                .for_each(|i| push(first + i * step + (xorshift(&mut seed) % 3) as u32));
        }
        // A scatter: Array, or Words past ARRAY_MAX members.
        3 => (0..u32::from(input.u16()) % 6_000)
            .for_each(|_| push(xorshift(&mut seed) as u32 & 0xFFFF)),
        // Values the input names: arm boundaries crossed one member at a time.
        _ => (0..1 + input.u8() % 8).for_each(|_| push(u32::from(input.u16()))),
    }
    out.sort_unstable();
    out.dedup();
}

/// Patch one image through an operation list, checking after every
/// operation that the patched bytes are a fresh encoding of the oracle, that
/// the report and the witness agree with it, and that every read does.
pub fn patch_step(data: &[u8]) {
    let mut input = Input(data);
    let keys = keys(&mut input);
    let sealed = input.u8() & 1 == 1;
    let mut adds = Vec::new();
    let mut removes = Vec::new();
    values(&mut input, &keys, &mut adds);
    let mut oracle: BTreeSet<u32> = adds.iter().copied().collect();
    let mut slot = vec![0u8; SLOT_BYTES];
    let initial = Bitmosaic::from_sorted(adds.iter().copied());
    let mut len = if sealed {
        initial.write_into_slice(&mut slot)
    } else {
        initial.write_forest_into_slice(&mut slot)
    }
    .expect("the initial image fits the slot");
    let mut is_ef = BitmosaicView::open(&slot[..len])
        .expect("a fresh image opens")
        .is_elias_fano();
    let mut scratch = PatchScratch::try_new().expect("patch scratch");
    check_reads(0, &slot[..len], &oracle, &mut input);

    for step in 1..=MAX_STEPS {
        if input.is_empty() {
            break;
        }
        let op = input.u8();
        adds.clear();
        removes.clear();
        if op & 1 != 0 {
            values(&mut input, &keys, &mut adds);
        }
        if op & 2 != 0 {
            values(&mut input, &keys, &mut removes);
        }
        // Empty one chunk of the pool, whatever it holds.
        if op & 4 != 0 {
            let key = u32::from(keys[usize::from(op >> 3 & 3)]) << 16;
            removes.extend(oracle.range(key..=key | 0xFFFF).copied());
            removes.sort_unstable();
            removes.dedup();
        }
        let report = if op & 32 != 0 {
            let mut adds_new = vec![0u64; adds.len().div_ceil(64)];
            let mut removes_held = vec![0u64; removes.len().div_ceil(64)];
            let mut outcome = BatchOutcome::new(&mut adds_new, &mut removes_held);
            let report = patch_witnessed(&mut slot, &adds, &removes, &mut scratch, &mut outcome)
                .unwrap_or_else(|e| panic!("step {step}: patch_witnessed refused: {e}"));
            for (i, v) in adds.iter().enumerate() {
                assert_eq!(
                    outcome.add_was_new(i),
                    !oracle.contains(v),
                    "step {step}: add {v} witnessed"
                );
            }
            for (i, v) in removes.iter().enumerate() {
                assert_eq!(
                    outcome.remove_was_held(i),
                    oracle.contains(v) || adds.binary_search(v).is_ok(),
                    "step {step}: remove {v} witnessed"
                );
            }
            report
        } else {
            patch(&mut slot, &adds, &removes, &mut scratch)
                .unwrap_or_else(|e| panic!("step {step}: patch refused: {e}"))
        };
        let added = adds.iter().filter(|v| oracle.insert(**v)).count() as u32;
        let removed = removes.iter().filter(|v| oracle.remove(v)).count() as u32;
        assert_eq!(report.added, added, "step {step}: added");
        assert_eq!(report.removed, removed, "step {step}: removed");
        assert_eq!(report.len, oracle.len() as u64, "step {step}: len");
        len = report.serialized_len;

        // The root is kept, except that an Elias-Fano root left with fewer
        // than two members becomes a forest.
        let now_ef = BitmosaicView::open(&slot[..len])
            .unwrap_or_else(|| panic!("step {step}: the patched image does not open"))
            .is_elias_fano();
        assert_eq!(now_ef, is_ef && oracle.len() >= 2, "step {step}: root kept");
        is_ef = now_ef;
        if let Some(want) = canonical(&oracle, is_ef) {
            assert!(
                slot[..len] == want[..],
                "step {step}: patched bytes are not a fresh encoding ({len} vs {} bytes)",
                want.len()
            );
        }
        check_reads(step, &slot[..len], &oracle, &mut input);
    }
}

/// The bytes a slot holding `oracle` must show: the forest form for a forest
/// slot; for an Elias-Fano slot, the sealed form while that is still an
/// Elias-Fano root, else `None` (such a root has no fresh twin and is
/// checked by reads alone).
fn canonical(oracle: &BTreeSet<u32>, slot_is_ef: bool) -> Option<Vec<u8>> {
    let owned = Bitmosaic::from_sorted(oracle.iter().copied());
    if !slot_is_ef {
        return Some(owned.to_forest_bytes());
    }
    let sealed = owned.to_bytes();
    BitmosaicView::open(&sealed)
        .expect("a fresh image opens")
        .is_elias_fano()
        .then_some(sealed)
}

/// Every read of the borrowed image `bytes` and of a fresh owned build
/// against `oracle`. Probes are an even sample of members with both their
/// neighbours, since a chunk's search takes one path between two members,
/// plus values the input names; seeks visit the same probes in order.
fn check_reads(step: usize, bytes: &[u8], oracle: &BTreeSet<u32>, input: &mut Input<'_>) {
    let view = BitmosaicView::open_verified(bytes)
        .unwrap_or_else(|| panic!("step {step}: the image does not verify"));
    let members: Vec<u32> = oracle.iter().copied().collect();
    let owned = Bitmosaic::from_sorted(members.iter().copied());
    let n = members.len();
    assert_eq!(view.len(), n as u64, "step {step}: view len");
    assert_eq!(owned.len(), n as u64, "step {step}: owned len");
    let mut walked = Vec::with_capacity(n);
    view.for_each(|v| walked.push(v));
    assert!(walked == members, "step {step}: walk");

    let every = (n / 64).max(1);
    let mut probes: Vec<u32> = members
        .iter()
        .step_by(every)
        .flat_map(|&m| [m.wrapping_sub(1), m, m.wrapping_add(1)])
        .chain((0..16).map(|_| input.u32()))
        .chain([0, u32::MAX])
        .collect();
    for &v in &probes {
        let rank = members.partition_point(|m| *m < v);
        let member = oracle.contains(&v);
        assert_eq!(view.contains(v), member, "step {step}: view contains({v})");
        assert_eq!(
            owned.contains(v),
            member,
            "step {step}: owned contains({v})"
        );
        assert_eq!(view.rank(v), rank as u64, "step {step}: view rank({v})");
        assert_eq!(owned.rank(v), rank as u64, "step {step}: owned rank({v})");
        assert_eq!(
            owned.successor(v),
            members.get(rank).copied(),
            "step {step}: successor({v})"
        );
    }
    for k in (0..n).step_by(every).chain([n]) {
        let want = members.get(k).copied();
        assert_eq!(view.select(k as u64), want, "step {step}: view select({k})");
        assert_eq!(
            owned.select(k as u64),
            want,
            "step {step}: owned select({k})"
        );
    }
    probes.sort_unstable();
    let (mut owned_cursor, mut view_cursor) = (owned.range(), view.range());
    for &target in &probes {
        let want = members
            .get(members.partition_point(|m| *m < target))
            .copied();
        owned_cursor.seek(target);
        view_cursor.seek(target);
        let owned_at = (!owned_cursor.empty()).then(|| owned_cursor.front());
        let view_at = (!view_cursor.empty()).then(|| view_cursor.front());
        assert_eq!(owned_at, want, "step {step}: owned seek({target})");
        assert_eq!(view_at, want, "step {step}: view seek({target})");
    }
}

/// Build two sets through an operation list and, after every operation,
/// intersect them on every skew arm in every form.
pub fn and_step(data: &[u8]) {
    let mut input = Input(data);
    let keys = keys(&mut input);
    let (mut a, mut b) = (BTreeSet::new(), BTreeSet::new());
    let mut batch = Vec::new();
    for step in 0..MAX_STEPS {
        if input.is_empty() {
            break;
        }
        let op = input.u8();
        values(&mut input, &keys, &mut batch);
        for (side, set) in [(1, &mut a), (2, &mut b)] {
            if op & 3 & side == 0 {
                continue;
            }
            if op & 4 == 0 {
                set.extend(batch.iter().copied());
            } else {
                batch.iter().for_each(|v| {
                    set.remove(v);
                });
            }
        }
        check_and(&format!("step {step}"), &a, &b);
    }
}

/// `a ∩ b` on every skew arm, both operand orders, owned, borrowed (sealed
/// and forest roots) and through the u64 tier, against the oracle's count.
pub(crate) fn check_and(name: &str, a: &BTreeSet<u32>, b: &BTreeSet<u32>) {
    let expected = a.intersection(b).count() as u64;
    let owned = |set: &BTreeSet<u32>| Bitmosaic::from_sorted(set.iter().copied());
    let (oa, ob) = (owned(a), owned(b));
    let (sa, sb) = (oa.to_bytes(), ob.to_bytes());
    let (fa, fb) = (oa.to_forest_bytes(), ob.to_forest_bytes());
    fn open(bytes: &[u8]) -> BitmosaicView<'_> {
        BitmosaicView::open_verified(bytes).expect("a fresh image verifies")
    }
    let (va, vb, wa, wb) = (open(&sa), open(&sb), open(&fa), open(&fb));
    // Two highs in the u64 tier, so its directory walk is exercised too;
    // the map keeps order, so the sets stay sorted.
    let wide = |set: &BTreeSet<u32>| {
        Bitmosaic64::from_sorted(set.iter().map(|&v| u64::from(v >> 18) << 32 | u64::from(v)))
    };
    let (a64, b64) = (wide(a), wide(b));
    let (ba64, bb64) = (a64.to_bytes(), b64.to_bytes());
    fn open64(bytes: &[u8]) -> Bitmosaic64View<'_> {
        Bitmosaic64View::open_verified(bytes).expect("a fresh u64 image verifies")
    }
    let (va64, vb64) = (open64(&ba64), open64(&bb64));

    for arm in [Arm::Balanced, Arm::Drive, Arm::Auto] {
        force(arm);
        let answers = [
            ("owned", oa.and_len(&ob), ob.and_len(&oa)),
            ("sealed view", va.and_len(&vb), vb.and_len(&va)),
            ("forest view", wa.and_len(&wb), wb.and_len(&wa)),
            ("sealed x forest", va.and_len(&wb), wb.and_len(&va)),
            ("owned u64", a64.and_len(&b64), b64.and_len(&a64)),
            ("u64 view", va64.and_len(&vb64), vb64.and_len(&va64)),
        ];
        for (form, forward, reversed) in answers {
            assert_eq!(forward, expected, "{name}: {form} on {arm:?}");
            assert_eq!(reversed, expected, "{name}: {form} on {arm:?}, reversed");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{and_step, check_and, patch_step, xorshift};

    /// Inputs the fuzz targets once failed on, replayed in the default lane;
    /// each file is named for the defect it found.
    #[test]
    fn fuzz_regressions_replay_clean() {
        // A patch on a short Elias-Fano root spliced a plane with an 8-byte
        // read at byte 52 of a 57-byte source with no pad behind it.
        patch_step(include_bytes!(
            "../fuzz/regressions/patch_step/ef-splice-unpadded-copy"
        ));
    }

    /// Fixed-seed operation lists: the fuzz targets' steps in the default
    /// lane. The skew switch is per-thread, so these tests force arms
    /// without racing any other test of the binary.
    #[test]
    fn seeded_operation_lists_match_the_oracles() {
        let mut state = 0x0A11_5EED_u64;
        for _ in 0..16 {
            let data: Vec<u8> = (0..1024).map(|_| xorshift(&mut state) as u8).collect();
            patch_step(&data);
            and_step(&data);
        }
    }

    /// Every tag the ladder builds, small against wide, on every arm: the
    /// pairings `tests/skew_oracle.rs` forces only under `bench-internals`.
    #[test]
    fn every_tag_pairing_matches_the_oracle_on_every_arm() {
        let stride = |first: u32, step: u32, n: u32| (0..n).map(|i| first + i * step).collect();
        let cone = |first: u32, step: u32, n: u32, mut seed: u64| {
            (0..n)
                .map(|i| first + i * step + (xorshift(&mut seed) % 3) as u32)
                .collect()
        };
        let scatter = |n: usize, mut seed: u64| {
            let mut out = BTreeSet::new();
            while out.len() < n {
                out.insert((xorshift(&mut seed) % 60_000) as u32);
            }
            out
        };
        let runs = |n: u32| (0..n).flat_map(|i| i * 500..i * 500 + 200).collect();
        let small: Vec<(&str, BTreeSet<u32>)> = vec![
            ("stride-8", stride(101, 7, 8)),
            ("jittered-8", cone(53, 11, 8, 0x11)),
            ("array-8", scatter(8, 0x22)),
            ("array-64", scatter(64, 0x23)),
        ];
        let wide: Vec<(&str, BTreeSet<u32>)> = vec![
            ("stride-4000", stride(3, 13, 4_000)),
            ("cone-1000", cone(7, 41, 1_000, 0x33)),
            ("array-4000", scatter(4_000, 0x44)),
            ("words-40000", scatter(40_000, 0x55)),
            ("runs-120", runs(120)),
        ];
        for (small_name, a) in &small {
            for (wide_name, b) in &wide {
                check_and(&format!("{small_name} x {wide_name}"), a, b);
            }
        }
    }
}
