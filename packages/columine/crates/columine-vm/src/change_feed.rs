//! The changed-key feed (`specs/columine/07-change-feed.md`): which keys of which tracked sources
//! one RETE pass may have changed. The VM's key-scoped write kernels record
//! their slots' keys; the RETE layer records its derived-fact writes through
//! [`crate::vm::Vm::feed_record_fact`].
//!
//! An entry is a may-have-changed claim: for every tracked source whose
//! overflow bit is clear, the entries hold a superset of the keys whose
//! RETE-bindable row changed since the source's change flags were last
//! cleared. A write that changes nothing RETE can bind (a hash-map rewrite
//! of an equal value, a TTL refresh of a held set member) records nothing.
//!
//! Sources are named in the change-plane layout the RETE layer wakes rules
//! by: bit 0 is the derived-facts table, bit `s + 1` is slot `s`. `tracked`
//! and the overflow bitset share it, so a consumer tests overflow with the
//! same AND it tests wakes with. A source is overflowed ("the whole source
//! changed") when the buffer is full, when a write names no key (bitmap
//! algebra, a bitmap import, nested containers), when a fact index has no
//! chain, when the source's change flag was already set as the pass began
//! (a write the feed did not see), or when the slot is a kind no kernel
//! records keys for.
//! Nothing is ever overwritten.
//!
//! Precondition: every live writer of a slot sets the slot's change flag,
//! so a write made while the feed is off is seen as overflow at the next
//! begin. A bitmap import is the known exception: it sets no flag, so an
//! import made between passes is invisible to the feed (one made while the
//! feed is armed overflows its slot).
//!
//! Storage is reserved once by [`ChangeFeed::bind`]; recording never
//! allocates. The feed is armed only between [`ChangeFeed::begin_pass`] and
//! [`ChangeFeed::end_pass`], and is off during a capacity dry run.

use crate::key_index::Key;
use crate::meta::slot_meta_base;
use columine_types::types::{
    ChangeFlag, ErrorCode, SLOT_META_SIZE, STATE_HEADER_SIZE, SlotMetaOffset, SlotType,
    SlotTypeFlags, StateHeaderOffset, state_num_slots,
};

/// The source one entry belongs to: a slot ordinal or a derived-fact index.
/// The two are told apart by a flag, never by a high bit, so each spans the
/// whole `u16`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Source {
    Slot(u16),
    Fact(u16),
}

/// What one write did to its key, with the key's prior bindable value lane
/// where the source has one: a hash map's `u32` value, a two-key map's
/// `field0 | field1 << 32`, a derived fact's value image. A one-key struct
/// map and a set have none. An insertion has no prior value by construction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Change {
    Inserted,
    Updated(Option<u64>),
    Removed(Option<u64>),
    Evicted(Option<u64>),
}

/// `FeedEntry::flags`: `old` holds the prior value lane.
const HAS_OLD: u8 = 0x01;
/// `FeedEntry::flags`: `source` is a derived-fact index, not a slot ordinal.
const SOURCE_IS_FACT: u8 = 0x02;
/// The end of a per-source chain.
const NO_ENTRY: u32 = u32::MAX;

/// One recorded change, 24 bytes: `old u64 · key1 u32 · key2 u32 · source u16
/// · kind u8 · flags u8 · prev u32`. `kind` is one change-flag bit; `prev`
/// is the previous entry of the same source. Built only by
/// [`ChangeFeed::record`], so its fields always decode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct FeedEntry {
    old: u64,
    key1: u32,
    key2: u32,
    source: u16,
    kind: u8,
    flags: u8,
    prev: u32,
}

const _: () = assert!(size_of::<FeedEntry>() == 24 && align_of::<FeedEntry>() == 8);

impl FeedEntry {
    pub const fn source(&self) -> Source {
        if self.flags & SOURCE_IS_FACT == 0 {
            Source::Slot(self.source)
        } else {
            Source::Fact(self.source)
        }
    }

    /// The changed key; lane 2 is `0` for a one-key source.
    pub const fn key(&self) -> Key {
        Key::two(self.key1, self.key2)
    }

    pub fn change(&self) -> Change {
        let old = (self.flags & HAS_OLD != 0).then_some(self.old);
        match self.kind {
            ChangeFlag::INSERTED => Change::Inserted,
            ChangeFlag::UPDATED => Change::Updated(old),
            ChangeFlag::REMOVED => Change::Removed(old),
            ChangeFlag::EVICTED => Change::Evicted(old),
            kind => unreachable!("ChangeFeed::record wrote change kind {kind:#x}"),
        }
    }

    /// The index of the previous entry of the same source.
    pub const fn prev(&self) -> Option<u32> {
        if self.prev == NO_ENTRY {
            None
        } else {
            Some(self.prev)
        }
    }
}

/// What the feed did since it was bound: entries recorded, entries dropped
/// because the buffer was full, and sources that became overflowed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FeedWork {
    pub recorded: u64,
    pub dropped: u64,
    pub overflowed: u64,
}

/// One source's entries, newest first.
#[derive(Clone, Debug)]
pub struct Chain<'a> {
    entries: &'a [FeedEntry],
    next: u32,
}

impl<'a> Iterator for Chain<'a> {
    type Item = &'a FeedEntry;

    fn next(&mut self) -> Option<&'a FeedEntry> {
        let entry = self.entries.get(usize::try_from(self.next).ok()?)?;
        self.next = entry.prev;
        Some(entry)
    }
}

/// The changed-key feed of one VM. See the module documentation.
#[derive(Debug, Default)]
pub struct ChangeFeed {
    /// Reserved to `cap` at bind and never grown.
    entries: Vec<FeedEntry>,
    cap: usize,
    /// Change-plane bitsets: the sources recorded, and those overflowed.
    tracked: Vec<u64>,
    overflow: Vec<u64>,
    /// The newest entry of each source: slot `s` at `s`, fact `f` at
    /// `slots + f`.
    last: Vec<u32>,
    slots: usize,
    armed: bool,
    work: FeedWork,
}

impl ChangeFeed {
    /// Track the sources `tracked` names (change-plane layout), with chains
    /// for every tracked slot and, when the derived-facts bit is tracked, for
    /// fact indices below `facts`, and room for `cap` entries per pass. The
    /// only allocation the feed makes: memory that cannot back it is
    /// `OutOfMemory`, and a slot past the `u16` an entry names is
    /// `InvalidProgram`. A refused bind leaves a feed that tracks nothing.
    pub(crate) fn bind(&mut self, tracked: &[u64], facts: u16, cap: u32) -> Result<(), ErrorCode> {
        self.entries.clear();
        self.tracked.clear();
        self.overflow.clear();
        self.last.clear();
        self.cap = 0;
        self.slots = 0;
        self.armed = false;
        self.work = FeedWork::default();
        // The highest tracked bit is the last tracked slot plus one, so it
        // is also the number of slot chains.
        let slots = tracked
            .iter()
            .enumerate()
            .rev()
            .find(|&(_, &word)| word != 0)
            .map_or(0, |(index, &word)| {
                index * 64 + 63 - word.leading_zeros() as usize
            });
        if slots > usize::from(u16::MAX) + 1 {
            return Err(ErrorCode::InvalidProgram);
        }
        let words = (slots + 1).div_ceil(64);
        let facts = if tracked.first().is_some_and(|&word| word & 1 != 0) {
            usize::from(facts)
        } else {
            0
        };
        let cap_entries = usize::try_from(cap).map_err(|_| ErrorCode::OutOfMemory)?;
        if self.entries.try_reserve_exact(cap_entries).is_err()
            || self.tracked.try_reserve_exact(words).is_err()
            || self.overflow.try_reserve_exact(words).is_err()
            || self.last.try_reserve_exact(slots + facts).is_err()
        {
            return Err(ErrorCode::OutOfMemory);
        }
        self.tracked
            .extend((0..words).map(|index| tracked.get(index).copied().unwrap_or(0)));
        self.overflow.resize(words, 0);
        self.last.resize(slots + facts, NO_ENTRY);
        self.cap = cap_entries;
        self.slots = slots;
        Ok(())
    }

    /// Start a pass over `state`: forget the last pass's entries, overflow
    /// every tracked source whose change flag is already set or whose kind
    /// records no keys, and arm. Reads only the slot metadata; a tracked
    /// slot the image does not hold is overflowed.
    pub(crate) fn begin_pass(&mut self, state: &[u8]) {
        self.entries.clear();
        self.last.fill(NO_ENTRY);
        self.overflow.fill(0);
        let header = state.len() >= STATE_HEADER_SIZE as usize;
        let slots = if header { state_num_slots(state) } else { 0 };
        for word in 0..self.tracked.len() {
            let mut bits = self.tracked[word];
            while bits != 0 {
                let bit = word * 64 + bits.trailing_zeros() as usize;
                bits &= bits - 1;
                let unseen = if bit == 0 {
                    !header || state[StateHeaderOffset::DERIVED_FACTS_CHANGE_FLAG as usize] != 0
                } else {
                    // Below 2^16 + 1 by bind.
                    slot_changed_unseen(state, slots, (bit - 1) as u32)
                };
                if unseen {
                    self.mark(bit);
                }
            }
        }
        self.armed = true;
    }

    /// End the pass: nothing records until the next begin. The entries stay
    /// readable until then.
    pub(crate) fn end_pass(&mut self) {
        self.armed = false;
    }

    /// Stop recording for a capacity dry run; answers whether the feed was
    /// armed, for [`Self::resume`].
    pub(crate) fn suspend(&mut self) -> bool {
        core::mem::replace(&mut self.armed, false)
    }

    pub(crate) fn resume(&mut self, armed: bool) {
        self.armed = armed;
    }

    /// Whether a write to `slot` records now: the feed is armed, and the
    /// slot is tracked and not overflowed. A kernel reads it once, before
    /// its row loop, and reads a prior value only when it is set.
    #[inline(always)]
    pub(crate) fn records(&self, slot: u32) -> bool {
        // Computed in u64: a u32 slot plus one does not fit a wasm32 usize.
        let bit = u64::from(slot) + 1;
        self.armed && self.live((bit / 64) as usize, 1 << (bit % 64))
    }

    #[inline(always)]
    fn live(&self, word: usize, mask: u64) -> bool {
        self.tracked
            .get(word)
            .is_some_and(|&tracked| tracked & !self.overflow[word] & mask != 0)
    }

    /// Set `bit`'s overflow, counting the source the first time.
    fn mark(&mut self, bit: usize) {
        let (word, mask) = (bit / 64, 1u64 << (bit % 64));
        if self.overflow[word] & mask == 0 {
            self.overflow[word] |= mask;
            self.work.overflowed += 1;
        }
    }

    /// Record one change of `source`'s `key`, after its write landed. Nothing
    /// records unless the feed is armed and the source is tracked and not
    /// overflowed. A full buffer, or a fact index without a chain, overflows
    /// the source instead; a full buffer also counts the dropped entry.
    pub(crate) fn record(&mut self, source: Source, key: Key, change: Change) {
        let (bit, chain, flags) = match source {
            Source::Slot(slot) => (usize::from(slot) + 1, usize::from(slot), 0),
            Source::Fact(fact) => (0, self.slots + usize::from(fact), SOURCE_IS_FACT),
        };
        if !self.armed || !self.live(bit / 64, 1 << (bit % 64)) {
            return;
        }
        // A tracked slot always has its chain; a fact past `facts` has none.
        if chain >= self.last.len() {
            self.mark(bit);
            return;
        }
        if self.entries.len() == self.cap {
            self.work.dropped += 1;
            self.mark(bit);
            return;
        }
        let (kind, old) = match change {
            Change::Inserted => (ChangeFlag::INSERTED, None),
            Change::Updated(old) => (ChangeFlag::UPDATED, old),
            Change::Removed(old) => (ChangeFlag::REMOVED, old),
            Change::Evicted(old) => (ChangeFlag::EVICTED, old),
        };
        let index = self.entries.len() as u32; // below `cap`, a u32
        debug_assert!(
            self.entries.len() < self.entries.capacity(),
            "bind reserves every entry a pass records"
        );
        self.entries.push(FeedEntry {
            old: old.unwrap_or(0),
            key1: key.k1,
            key2: key.k2,
            source: match source {
                Source::Slot(id) | Source::Fact(id) => id,
            },
            kind,
            flags: flags | if old.is_some() { HAS_OLD } else { 0 },
            prev: self.last[chain],
        });
        self.last[chain] = index;
        self.work.recorded += 1;
    }

    /// [`Self::record`] for a kernel's `u32` slot ordinal. A slot past the
    /// `u16` is never tracked, so it records nothing.
    #[inline]
    pub(crate) fn record_slot(&mut self, slot: u32, key: Key, change: Change) {
        if let Ok(slot) = u16::try_from(slot) {
            self.record(Source::Slot(slot), key, change);
        }
    }

    /// A write changed `slot` in a way no key names: overflow it, when it
    /// records.
    pub(crate) fn changed_whole(&mut self, slot: u32) {
        if self.records(slot) {
            self.mark(slot as usize + 1);
        }
    }

    /// This pass's entries, in record order.
    pub fn entries(&self) -> &[FeedEntry] {
        &self.entries
    }

    /// The index of `source`'s newest entry this pass.
    pub fn last(&self, source: Source) -> Option<u32> {
        let chain = match source {
            Source::Slot(slot) if usize::from(slot) < self.slots => usize::from(slot),
            // Slot chains end where the fact chains begin.
            Source::Slot(_) => return None,
            Source::Fact(fact) => self.slots + usize::from(fact),
        };
        self.last
            .get(chain)
            .copied()
            .filter(|&index| index != NO_ENTRY)
    }

    /// `source`'s entries this pass, newest first.
    pub fn chain(&self, source: Source) -> Chain<'_> {
        Chain {
            entries: &self.entries,
            next: self.last(source).unwrap_or(NO_ENTRY),
        }
    }

    /// The overflowed sources, in the change-plane layout.
    pub fn overflow(&self) -> &[u64] {
        &self.overflow
    }

    /// Whether `source` is overflowed this pass; a fact reads the
    /// derived-facts bit.
    pub fn overflowed(&self, source: Source) -> bool {
        let bit = match source {
            Source::Slot(slot) => usize::from(slot) + 1,
            Source::Fact(_) => 0,
        };
        self.overflow
            .get(bit / 64)
            .is_some_and(|&word| word & (1 << (bit % 64)) != 0)
    }

    pub fn work(&self) -> FeedWork {
        self.work
    }
}

/// Whether a tracked slot may have changed in a way the feed did not see:
/// its change flag is set as the pass begins, or its kind records no keys,
/// or the image does not hold it.
fn slot_changed_unseen(state: &[u8], slots: u32, slot: u32) -> bool {
    let base = slot_meta_base(slot) as usize;
    if slot >= slots || base + SLOT_META_SIZE as usize > state.len() {
        return true;
    }
    if state[base + SlotMetaOffset::CHANGE_FLAGS as usize] != 0 {
        return true;
    }
    match SlotTypeFlags::from_byte(state[base + SlotMetaOffset::TYPE_FLAGS as usize]).slot_type() {
        // Keyed kernels record these; a nested write overflows its slot.
        Some(
            SlotType::HashMap
            | SlotType::HashSet
            | SlotType::Bitmap
            | SlotType::StructMap
            | SlotType::StructMap2
            | SlotType::Nested,
        ) => false,
        Some(
            SlotType::Aggregate
            | SlotType::Array
            | SlotType::ConditionTree
            | SlotType::Scalar
            | SlotType::OrderedList,
        )
        | None => true,
    }
}
