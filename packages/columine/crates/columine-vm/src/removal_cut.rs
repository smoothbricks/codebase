//! Log-order removal (`specs/columine/05-log-order-removal.md`): a removal
//! erases its key at its row's log position inside the target's fold, so
//! one log folds to the same slot under every batching.
//!
//! A batch's rows are its log order, and a `FLAT_MAP` element takes its
//! parent row's position. A removal opcode erases its key from the image as
//! it stands and records a cut: the latest batch row that removed the key.
//! A producer of the slot then skips every row at or before the cut for its
//! key. The removal already erased what earlier batches left, so the rows
//! before it contribute nothing, the rows after it fold into a fresh row,
//! and a row that writes and removes one key composes write-then-erase.
//! Nothing is stored per row, and the image never holds a cut.
//!
//! The skip is exact only when a slot's removals run before its producers;
//! [`validate_order`] refuses any other program before its first batch.

use crate::capacity::{SlotWrite, slot_writes};
use columine_types::types::{EMPTY_KEY, ErrorCode, TOMBSTONE};

/// The key one removal erases.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum CutKey {
    /// A HASHMAP or one-key STRUCT_MAP key (`0x23`, `0x2b`, `0x8c`).
    Unary(u32),
    /// An exact STRUCT_MAP2 pair (`0x86`).
    Pair(u32, u32),
    /// Every STRUCT_MAP2 pair under one first key (`0x8e`).
    Prefix(u32),
}

/// The latest batch row whose removal erased `key` from `slot`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Cut {
    slot: u32,
    key: CutKey,
    row: u32,
}

impl Cut {
    fn order(&self) -> (u32, CutKey) {
        (self.slot, self.key)
    }
}

/// One batch's removal cuts, retained across batches. The batch admission
/// reserves them before the first write ([`RemovalCuts::reserve_for`]) and a
/// dry run grows them; a sealed batch never allocates.
#[derive(Debug, Default)]
pub(crate) struct RemovalCuts {
    /// `cuts[..sorted]` is ordered by `(slot, key)`, one cut per key at its
    /// latest row; the cuts past it are in record order.
    cuts: Vec<Cut>,
    sorted: usize,
    /// Debug only: the capacity the batch sealed. No record may grow it.
    #[cfg(debug_assertions)]
    sealed: Option<usize>,
}

impl RemovalCuts {
    /// Forget the last batch's cuts, keeping their storage.
    pub(crate) fn clear(&mut self) {
        self.cuts.clear();
        self.sorted = 0;
    }

    /// Back `cuts` records before a batch's first write.
    pub(crate) fn reserve_for(&mut self, cuts: usize) -> Result<(), ErrorCode> {
        self.clear();
        self.cuts
            .try_reserve(cuts)
            .map_err(|_| ErrorCode::OutOfMemory)
    }

    /// From here until [`Self::unseal`] no record may grow the storage.
    pub(crate) fn seal(&mut self) {
        #[cfg(debug_assertions)]
        {
            self.sealed = Some(self.cuts.capacity());
        }
    }

    pub(crate) fn unseal(&mut self) {
        #[cfg(debug_assertions)]
        {
            self.sealed = None;
        }
    }

    /// Record that a removal of `slot` erased `key` at log position `row`.
    /// A sentinel key names no row and records nothing.
    pub(crate) fn record(&mut self, slot: u32, key: CutKey, row: u32) -> Result<(), ErrorCode> {
        let (CutKey::Unary(lead) | CutKey::Pair(lead, _) | CutKey::Prefix(lead)) = key;
        if lead == EMPTY_KEY || lead == TOMBSTONE {
            return Ok(());
        }
        self.cuts
            .try_reserve(1)
            .map_err(|_| ErrorCode::OutOfMemory)?;
        #[cfg(debug_assertions)]
        {
            if let Some(sealed) = self.sealed {
                debug_assert_eq!(
                    self.cuts.capacity(),
                    sealed,
                    "a removal cut grew past its batch's reservation"
                );
            }
        }
        self.cuts.push(Cut { slot, key, row });
        Ok(())
    }

    /// `slot`'s cuts, ordered by key. The cuts recorded since the last
    /// lookup are merged first: a short tail by binary insertion into the
    /// ordered prefix, a long one by sorting the whole set. A removal ahead
    /// of its producer in one `FOR_EACH` body records a cut per row before
    /// each lookup; re-sorting the whole set there measured 121 ms at 4096
    /// rows and 2.3 s at 16384 (rustc -O, one new key per row), against
    /// 0.97 ms and 18 ms for binary insertion.
    pub(crate) fn of(&mut self, slot: u32) -> SlotCuts<'_> {
        if self.sorted < self.cuts.len() {
            self.settle();
        }
        let start = self.cuts.partition_point(|cut| cut.slot < slot);
        let len = self.cuts[start..].partition_point(|cut| cut.slot == slot);
        SlotCuts(&self.cuts[start..start + len])
    }

    /// Order every cut and keep one per key, at its latest row.
    fn settle(&mut self) {
        let tail = self.cuts.len() - self.sorted;
        if tail > self.sorted.max(1).ilog2() as usize + 1 {
            // The latest row leads its key, so the dedup keeps it.
            self.cuts
                .sort_unstable_by(|a, b| a.order().cmp(&b.order()).then(b.row.cmp(&a.row)));
            self.cuts
                .dedup_by(|later, kept| later.order() == kept.order());
            self.sorted = self.cuts.len();
            return;
        }
        while self.sorted < self.cuts.len() {
            let cut = self.cuts[self.sorted];
            match self.cuts[..self.sorted].binary_search_by(|kept| kept.order().cmp(&cut.order())) {
                Ok(at) => {
                    self.cuts[at].row = self.cuts[at].row.max(cut.row);
                    self.cuts.swap_remove(self.sorted);
                }
                Err(at) => {
                    self.cuts[at..=self.sorted].rotate_right(1);
                    self.sorted += 1;
                }
            }
        }
    }
}

/// One slot's cuts in the running batch, ordered by key: what a producer of
/// the slot reads before it writes a row.
#[derive(Clone, Copy, Debug)]
pub struct SlotCuts<'a>(&'a [Cut]);

impl SlotCuts<'_> {
    /// No cut: a write outside a batch.
    pub const NONE: SlotCuts<'static> = SlotCuts(&[]);

    /// Whether a removal of `key` at log position `row` or later erases a
    /// write of it at `row`.
    pub(crate) fn covers(self, key: CutKey, row: u32) -> bool {
        !self.0.is_empty()
            && self
                .0
                .binary_search_by(|cut| cut.key.cmp(&key))
                .is_ok_and(|at| row <= self.0[at].row)
    }

    /// Whether a removal of the exact pair, or of every pair under `key1`,
    /// at log position `row` or later erases a write of `(key1, key2)` at
    /// `row`.
    pub(crate) fn covers_pair(self, key1: u32, key2: u32, row: u32) -> bool {
        self.covers(CutKey::Pair(key1, key2), row) || self.covers(CutKey::Prefix(key1), row)
    }
}

/// Refuse a program that removes keys of a slot after an instruction that
/// produces into it, anywhere in the reduce section, `FOR_EACH` and
/// `FLAT_MAP` bodies included: that removal would erase rows the producer
/// folded after the removal's own row. Producers are classified by their
/// capacity effects ([`slot_writes`]). Checked once per program, before any
/// batch of it writes.
pub(crate) fn validate_order(code: &[u8]) -> Result<(), ErrorCode> {
    // Once per program. An instruction has more bytes than it names
    // operands or slots, so neither buffer grows past this.
    let mut operands = Vec::new();
    let mut produced = Vec::new();
    operands
        .try_reserve(code.len())
        .map_err(|_| ErrorCode::OutOfMemory)?;
    produced
        .try_reserve(code.len())
        .map_err(|_| ErrorCode::OutOfMemory)?;
    slot_writes(code, &mut operands, &mut |write| match write {
        SlotWrite::Produce(slot) => {
            if !produced.contains(&slot) {
                produced.push(slot);
            }
            Some(())
        }
        SlotWrite::Cut(slot) => (!produced.contains(&slot)).then_some(()),
    })
    .ok_or(ErrorCode::InvalidProgram)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every lookup answers what a scan of every record answers: the latest
    /// row per key, whichever merge (insertion or sort) settled it.
    #[test]
    fn lookups_answer_the_latest_row_per_key_under_both_merges() {
        let mut cuts = RemovalCuts::default();
        let mut log: Vec<(u32, CutKey, u32)> = Vec::new();
        let mut seed = 0x9e37_79b9u32;
        let mut next = |bound: u32| {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            seed % bound
        };
        // Bursts of one record (insertion) and of many (sort) alternate.
        for burst in [1, 40, 1, 1, 3, 200, 1, 1, 1, 64] {
            for _ in 0..burst {
                let slot = next(3);
                let key = match next(3) {
                    0 => CutKey::Unary(next(16)),
                    1 => CutKey::Pair(next(4), next(4)),
                    _ => CutKey::Prefix(next(4)),
                };
                let row = next(50);
                cuts.record(slot, key, row).unwrap();
                log.push((slot, key, row));
            }
            for slot in 0..3 {
                for key in (0..16)
                    .map(CutKey::Unary)
                    .chain((0..16).map(|k| CutKey::Pair(k / 4, k % 4)))
                    .chain((0..4).map(CutKey::Prefix))
                {
                    let latest = log
                        .iter()
                        .filter(|&&(s, k, _)| s == slot && k == key)
                        .map(|&(_, _, row)| row)
                        .max();
                    for row in [0, 25, 49] {
                        assert_eq!(
                            cuts.of(slot).covers(key, row),
                            latest.is_some_and(|latest| row <= latest),
                            "slot {slot} key {key:?} row {row}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn a_sentinel_key_records_no_cut() {
        let mut cuts = RemovalCuts::default();
        for key in [EMPTY_KEY, TOMBSTONE] {
            cuts.record(0, CutKey::Unary(key), 5).unwrap();
            cuts.record(0, CutKey::Prefix(key), 5).unwrap();
        }
        assert!(cuts.cuts.is_empty());
        assert!(!cuts.of(0).covers(CutKey::Unary(EMPTY_KEY), 0));
    }
}
