//! The watch log (`specs/columine/07-change-feed.md` §7). For each slot a
//! host watches it keeps a ring of the changes the slot's writes made, each
//! with its key's prior value, across passes. From that ring it answers the
//! slot's net change between a past write sequence (§6) and a state.
//!
//! [`WatchLog::watch`] reserves a slot's ring and refuses as `OutOfMemory`;
//! recording never allocates. A slot nobody watches costs a kernel call one
//! bit test.

use crate::bytes;
use crate::change_feed::{Change, ChangeFeed};
use crate::key_index::{HashIndex, InternBytes, Key, KeyCx, KeyIndex};
use crate::meta::{SlotMetaView, slot_meta_base};
use crate::state_init;
use crate::struct_map::{StructMap2Slot, StructMapSlot, StructRows};
use crate::vm::{CheckedMap, CheckedSet};
use columine_types::types::{
    AggType, ChangeFlag, EMPTY_KEY, ErrorCode, NET_CHANGE_ALIGN, NET_CHANGE_HEADER_SIZE,
    NetChangeKind, NetChangeOffset, SlotMetaOffset, SlotType, StructFieldType, is_arena_field_type,
    state_num_slots, struct_field_size,
};

/// The kind of a whole-slot entry: no change-flag bit, no key, no prior.
const WHOLE: u8 = 0;

/// One logged change of a watched slot.
#[derive(Clone, Copy, Debug, Default)]
struct Entry {
    key1: u32,
    key2: u32,
    /// The slot's write sequence after the write.
    stamp: u64,
    /// The change-flag bit of what the write did to the key; [`WHOLE`] for
    /// a write that names no key.
    kind: u8,
    /// Whether the key was present before the write.
    held: bool,
    /// Length of the prior value in the slot's image ring. The priors of
    /// consecutive entries lie end to end in it.
    prior_len: u32,
}

/// One watched slot's ring: entries oldest first from `head`, their prior
/// bytes end to end from `image_head`, both wrapping. The log is complete
/// above `coverage`.
#[derive(Clone, Debug)]
struct SlotLog {
    coverage: u64,
    /// Whether the slot's priors lead with a `u32` lane a host may intern:
    /// a hash map's value, a u32 scalar's value cell.
    u32_lane: bool,
    entries: Vec<Entry>,
    head: usize,
    len: usize,
    images: Vec<u8>,
    image_head: usize,
    image_len: usize,
}

impl SlotLog {
    fn new(
        coverage: u64,
        u32_lane: bool,
        entries: usize,
        image_bytes: usize,
    ) -> Result<Self, ErrorCode> {
        let mut ring = Vec::new();
        let mut images = Vec::new();
        if ring.try_reserve_exact(entries).is_err()
            || images.try_reserve_exact(image_bytes).is_err()
        {
            return Err(ErrorCode::OutOfMemory);
        }
        ring.resize(entries, Entry::default());
        images.resize(image_bytes, 0);
        Ok(Self {
            coverage,
            u32_lane,
            entries: ring,
            head: 0,
            len: 0,
            images,
            image_head: 0,
            image_len: 0,
        })
    }

    /// The entry `index` places after the oldest.
    fn at(&self, index: usize) -> Entry {
        self.entries[(self.head + index) % self.entries.len()]
    }

    /// The stamp a write must pass to record: the newest entry's, or the
    /// coverage of an empty ring.
    fn newest(&self) -> u64 {
        if self.len == 0 {
            self.coverage
        } else {
            self.at(self.len - 1).stamp
        }
    }

    fn image_wrap(&self, at: usize) -> usize {
        if self.images.is_empty() {
            0
        } else {
            at % self.images.len()
        }
    }

    /// Drop the oldest entry: the log is complete only above its stamp now.
    fn evict_oldest(&mut self) {
        let oldest = self.at(0);
        self.head = (self.head + 1) % self.entries.len();
        self.len -= 1;
        self.image_head = self.image_wrap(self.image_head + oldest.prior_len as usize);
        self.image_len -= oldest.prior_len as usize;
        self.coverage = oldest.stamp;
    }

    /// An empty log with room to copy `other` into it.
    fn room_for(other: &Self) -> Result<Self, ErrorCode> {
        let mut log = Self {
            coverage: 0,
            u32_lane: false,
            entries: Vec::new(),
            head: 0,
            len: 0,
            images: Vec::new(),
            image_head: 0,
            image_len: 0,
        };
        log.reserve_for(other)?;
        Ok(log)
    }

    /// Hold room to copy `other` into this log; its content is unchanged.
    fn reserve_for(&mut self, other: &Self) -> Result<(), ErrorCode> {
        reserve_total(&mut self.entries, other.entries.len())?;
        reserve_total(&mut self.images, other.images.len())
    }

    /// Become a copy of `other` within room [`Self::reserve_for`] held.
    fn copy_from(&mut self, other: &Self) {
        debug_assert!(
            self.entries.capacity() >= other.entries.len()
                && self.images.capacity() >= other.images.len()
        );
        self.coverage = other.coverage;
        self.u32_lane = other.u32_lane;
        self.entries.clear();
        self.entries.extend_from_slice(&other.entries);
        self.head = other.head;
        self.len = other.len;
        self.images.clear();
        self.images.extend_from_slice(&other.images);
        self.image_head = other.image_head;
        self.image_len = other.image_len;
    }

    /// Drop the newest entry, for a settle.
    fn pop_newest(&mut self) {
        let newest = self.at(self.len - 1);
        self.len -= 1;
        self.image_len -= newest.prior_len as usize;
    }

    /// Append `entry` with `prior`, evicting the oldest entries until both
    /// fit. The caller guarantees `prior` fits the empty ring.
    fn push(&mut self, entry: Entry, prior: &[u8]) {
        debug_assert!(prior.len() <= self.images.len());
        debug_assert_eq!(entry.prior_len as usize, prior.len());
        while self.len == self.entries.len() || self.images.len() - self.image_len < prior.len() {
            self.evict_oldest();
        }
        if !prior.is_empty() {
            let start = self.image_wrap(self.image_head + self.image_len);
            let first = prior.len().min(self.images.len() - start);
            self.images[start..start + first].copy_from_slice(&prior[..first]);
            self.images[..prior.len() - first].copy_from_slice(&prior[first..]);
            self.image_len += prior.len();
        }
        let at = (self.head + self.len) % self.entries.len();
        self.entries[at] = entry;
        self.len += 1;
    }

    /// The prior of `len` bytes at ring offset `at`, as its one or two runs.
    fn prior(&self, at: usize, len: u32) -> (&[u8], &[u8]) {
        let len = len as usize;
        if len == 0 {
            return (&[], &[]);
        }
        let first = len.min(self.images.len() - at);
        (&self.images[at..at + first], &self.images[..len - first])
    }
}

/// The prior value a watched write records, captured before the write.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Prior {
    /// The key was absent: the write inserted it.
    Absent,
    /// The key was present and carries no value: a set, bitmap or list
    /// member.
    Held,
    /// A hash map's `u32` value (4 bytes) or an aggregate's or scalar's
    /// 8-byte value cell.
    Value { bytes: [u8; 8], len: u8 },
    /// The row image [`WatchLog::row_prior`] built into the log's scratch.
    Image,
    /// The row could not be imaged: its interned text did not resolve, or
    /// it outgrew every ring. The write records a whole-slot entry.
    Unimageable,
}

impl Prior {
    /// A hash map's `u32` value.
    pub(crate) fn value_u32(value: u32) -> Self {
        let mut bytes = [0; 8];
        bytes[..4].copy_from_slice(&value.to_le_bytes());
        Self::Value { bytes, len: 4 }
    }

    /// An aggregate's or scalar's 8-byte value cell.
    pub(crate) fn value_cell(state: &[u8], offset: u32) -> Self {
        Self::Value {
            bytes: bytes::read_u64(state, offset).to_le_bytes(),
            len: 8,
        }
    }
}

/// What [`WatchLog::net_changes`] answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetAnswer {
    /// The window reaches below the slot's coverage, or holds a write that
    /// named no key: the slot changed as a whole. A slot this VM does not
    /// watch answers this too.
    Overflowed,
    /// [`NetChanges`] holds every key the window changed.
    Changes,
}

/// A slot's net changes, reused across calls. [`Self::bytes`] holds one
/// record per change in record order, each at a multiple of
/// [`NET_CHANGE_ALIGN`]: a [`NET_CHANGE_HEADER_SIZE`]-byte header
/// ([`NetChangeOffset`]: `key1 u32 · key2 u32 · kind u8 · has_prior u8 ·
/// pad u16 · prior_len u32`, little-endian, `kind` a [`NetChangeKind`]),
/// then `prior_len` prior bytes, zero-padded to the alignment.
///
/// It also holds the query's working memory, grown on first use and kept:
/// the current value's image, the keys already answered and the key-index
/// family `I`'s scratch for struct-map lookups.
#[derive(Debug, Default)]
pub struct NetChanges<I: KeyIndex = HashIndex> {
    records: Records,
    current: Vec<u8>,
    /// Open-addressed cells of `(generation, key1, key2)`, at least twice
    /// the ring's entries.
    seen: Vec<(u32, u32, u32)>,
    generation: u32,
    keys: I::Scratch,
}

impl<I: KeyIndex> NetChanges<I> {
    /// The records.
    pub fn bytes(&self) -> &[u8] {
        &self.records.bytes
    }

    /// How many changes the records hold.
    pub fn len(&self) -> u32 {
        self.records.len
    }

    pub fn is_empty(&self) -> bool {
        self.records.len == 0
    }

    /// Size the working memory for `log` and start a new generation of
    /// answered keys. Storage that cannot be had is `OutOfMemory`.
    fn prepare(&mut self, log: &SlotLog) -> Result<(), ErrorCode> {
        reserve_total(&mut self.current, log.images.len())?;
        let cells = log
            .entries
            .len()
            .checked_mul(2)
            .map(usize::next_power_of_two)
            .ok_or(ErrorCode::OutOfMemory)?;
        if self.seen.len() < cells {
            reserve_total(&mut self.seen, cells)?;
            self.seen.clear();
            self.seen.resize(cells, (0, 0, 0));
            self.generation = 0;
        }
        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            self.seen.fill((0, 0, 0));
            self.generation = 1;
        }
        Ok(())
    }
}

/// [`NetChanges`]' records.
#[derive(Debug, Default)]
struct Records {
    bytes: Vec<u8>,
    len: u32,
}

impl Records {
    fn push(&mut self, key1: u32, key2: u32, kind: u8, prior: Option<(&[u8], &[u8])>) {
        let (first, second) = prior.unwrap_or((&[], &[]));
        let prior_len = first.len() + second.len();
        let mut header = [0u8; NET_CHANGE_HEADER_SIZE as usize];
        header[NetChangeOffset::KEY1 as usize..][..4].copy_from_slice(&key1.to_le_bytes());
        header[NetChangeOffset::KEY2 as usize..][..4].copy_from_slice(&key2.to_le_bytes());
        header[NetChangeOffset::KIND as usize] = kind;
        header[NetChangeOffset::HAS_PRIOR as usize] = u8::from(prior.is_some());
        header[NetChangeOffset::PRIOR_LEN as usize..][..4]
            .copy_from_slice(&(prior_len as u32).to_le_bytes());
        self.bytes.extend_from_slice(&header);
        self.bytes.extend_from_slice(first);
        self.bytes.extend_from_slice(second);
        let padded = self.bytes.len().next_multiple_of(NET_CHANGE_ALIGN as usize);
        self.bytes.resize(padded, 0);
        self.len += 1;
    }
}

/// A key's value in the state `net_changes` reads.
enum Current {
    Absent,
    /// Present, with no value: a set, bitmap or list member.
    Held,
    /// Present, its value the first `len` bytes of the log's current
    /// scratch.
    Value(usize),
    /// Present, with an image larger than the scratch or text that does
    /// not resolve: it differs from every prior the log holds.
    Unimageable,
}

/// The watch log. Owned by the VM beside its change feed.
#[derive(Debug, Default)]
pub struct WatchLog {
    slots: Vec<Option<SlotLog>>,
    /// Bit `s` set while slot `s` is watched: the one test a kernel call
    /// pays.
    watched: Vec<u64>,
    /// The watched bits a capacity dry run set aside: a dry run records
    /// nothing.
    suspended: Vec<u64>,
    /// The row image a watched row write captures before it writes.
    prior: Vec<u8>,
}

/// Reserve `vec` to hold `total` elements.
fn reserve_total<T>(vec: &mut Vec<T>, total: usize) -> Result<(), ErrorCode> {
    vec.try_reserve(total.saturating_sub(vec.len()))
        .map_err(|_| ErrorCode::OutOfMemory)
}

/// The write sequence of `slot` in `state`; a slot past the state's slot
/// count is `InvalidSlot`, a short state `InvalidState`.
fn checked_seq(state: &[u8], slot: u32) -> Result<u64, ErrorCode> {
    if state.len() < columine_types::types::STATE_HEADER_SIZE as usize {
        return Err(ErrorCode::InvalidState);
    }
    if slot >= state_num_slots(state) {
        return Err(ErrorCode::InvalidSlot);
    }
    let at = u64::from(slot_meta_base(slot)) + u64::from(SlotMetaOffset::WRITE_SEQ);
    if at + 8 > state.len() as u64 {
        return Err(ErrorCode::InvalidState);
    }
    Ok(bytes::read_u64(state, at as u32))
}

/// The write sequence of a slot a kernel just wrote.
fn seq(state: &[u8], slot: u32) -> u64 {
    bytes::read_u64(state, slot_meta_base(slot) + SlotMetaOffset::WRITE_SEQ)
}

impl WatchLog {
    /// Start logging `slot` with a ring of `entries` entries and
    /// `image_bytes` bytes of priors, or keep its existing log. Answers the
    /// slot's coverage: its current write sequence when logging starts.
    /// Storage that cannot be had refuses as `OutOfMemory` and changes
    /// nothing; a ring of no entries is `CapacityExceeded`.
    pub fn watch(
        &mut self,
        state: &[u8],
        slot: u32,
        entries: u32,
        image_bytes: u32,
    ) -> Result<u64, ErrorCode> {
        let sequence = checked_seq(state, slot)?;
        let index = slot as usize;
        if let Some(Some(log)) = self.slots.get(index) {
            return Ok(log.coverage);
        }
        if entries == 0 {
            return Err(ErrorCode::CapacityExceeded);
        }
        let (entries, image_bytes) = (entries as usize, image_bytes as usize);
        reserve_total(&mut self.slots, index + 1)?;
        reserve_total(&mut self.watched, index / 64 + 1)?;
        reserve_total(&mut self.suspended, index / 64 + 1)?;
        reserve_total(&mut self.prior, image_bytes)?;
        let meta = SlotMetaView::read(state, slot);
        let u32_lane = match meta.slot_type() {
            SlotType::HashMap => true,
            SlotType::Scalar => meta.agg_type_byte(state) == AggType::ScalarU32 as u8,
            _ => false,
        };
        let log = SlotLog::new(sequence, u32_lane, entries, image_bytes)?;
        if self.slots.len() <= index {
            self.slots.resize(index + 1, None);
        }
        if self.watched.len() <= index / 64 {
            self.watched.resize(index / 64 + 1, 0);
        }
        self.slots[index] = Some(log);
        self.watched[index / 64] |= 1 << (index % 64);
        Ok(sequence)
    }

    /// Release `slot`'s log. A slot not watched is left as it is.
    pub fn unwatch(&mut self, slot: u32) {
        let index = slot as usize;
        if let Some(log) = self.slots.get_mut(index) {
            *log = None;
            self.watched[index / 64] &= !(1 << (index % 64));
        }
    }

    /// Every `u32` lane prior `slot`'s ring holds, oldest first; none
    /// for a slot whose priors are not `u32` lanes or that is not watched.
    pub fn priors(&self, slot: u32) -> impl Iterator<Item = u32> + '_ {
        let log = self
            .slots
            .get(slot as usize)
            .and_then(Option::as_ref)
            .filter(|log| log.u32_lane);
        log.into_iter().flat_map(|log| {
            let mut at = log.image_head;
            (0..log.len).filter_map(move |index| {
                let entry = log.at(index);
                let start = at;
                at = log.image_wrap(at + entry.prior_len as usize);
                (entry.prior_len >= 4).then(|| {
                    let byte = |offset: usize| log.images[(start + offset) % log.images.len()];
                    u32::from_le_bytes([byte(0), byte(1), byte(2), byte(3)])
                })
            })
        })
    }

    /// Whether `slot` is watched.
    pub fn watched(&self, slot: u32) -> bool {
        matches!(self.slots.get(slot as usize), Some(Some(_)))
    }

    /// The sequence above which `slot`'s log is complete, or `None` when
    /// the slot is not watched.
    pub fn coverage(&self, slot: u32) -> Option<u64> {
        self.slots
            .get(slot as usize)
            .and_then(Option::as_ref)
            .map(|log| log.coverage)
    }

    /// Whether a write to `slot` records now: the slot is watched and no
    /// capacity dry run is running.
    #[inline(always)]
    pub(crate) fn watches(&self, slot: u32) -> bool {
        let index = slot as usize;
        self.watched
            .get(index / 64)
            .is_some_and(|&word| word & (1 << (index % 64)) != 0)
    }

    /// Stop recording for a capacity dry run.
    pub(crate) fn suspend(&mut self) {
        core::mem::swap(&mut self.watched, &mut self.suspended);
    }

    /// Record again after [`Self::suspend`].
    pub(crate) fn resume(&mut self) {
        core::mem::swap(&mut self.watched, &mut self.suspended);
    }

    /// The prior a write of `slot`'s row at `pos` records: the row's image
    /// as it stands, or [`Prior::Absent`] when the write inserts its key
    /// (`pos` is `None`). Builds nothing unless the slot is watched.
    pub(crate) fn row_prior<S: InternBytes + ?Sized>(
        &mut self,
        state: &[u8],
        slot: u32,
        rows: &StructRows,
        pos: Option<u32>,
        strings: &S,
    ) -> Prior {
        match pos {
            Some(pos) if self.watches(slot) => {
                match row_image(state, rows, pos, strings, &mut self.prior) {
                    Some(()) => Prior::Image,
                    None => Prior::Unimageable,
                }
            }
            _ => Prior::Absent,
        }
    }

    /// Record one landed write of `slot`'s `key`, stamped with the slot's
    /// sequence in `state`. A write stamped at or below the slot's newest
    /// entry is a refold of a write the log holds and records nothing.
    pub(crate) fn record(&mut self, state: &[u8], slot: u32, key: Key, kind: u8, prior: Prior) {
        if self.admits(state, slot) {
            self.append(state, slot, key, kind, prior);
        }
    }

    /// Whether a write of `slot` that `state` holds records: the slot is
    /// watched and its sequence lies above the slot's newest entry, so the
    /// write is no refold of one the log holds. A write that logs several
    /// keys at its one stamp asks once, before its first key.
    pub(crate) fn admits(&self, state: &[u8], slot: u32) -> bool {
        self.watches(slot)
            && matches!(
                self.slots.get(slot as usize),
                Some(Some(log)) if seq(state, slot) > log.newest()
            )
    }

    /// Append one key of a write [`Self::admits`] admitted, at the slot's
    /// sequence in `state`.
    pub(crate) fn append(&mut self, state: &[u8], slot: u32, key: Key, kind: u8, prior: Prior) {
        let Self {
            slots,
            prior: image,
            ..
        } = self;
        let Some(Some(log)) = slots.get_mut(slot as usize) else {
            return;
        };
        let stamp = seq(state, slot);
        let (held, bytes): (bool, &[u8]) = match &prior {
            Prior::Absent => (false, &[]),
            Prior::Held => (true, &[]),
            Prior::Value { bytes, len } => (true, &bytes[..usize::from(*len)]),
            Prior::Image => (true, image.as_slice()),
            Prior::Unimageable => (true, &[]),
        };
        if matches!(prior, Prior::Unimageable) || bytes.len() > log.images.len() {
            log.push(whole(stamp), &[]);
            return;
        }
        log.push(
            Entry {
                key1: key.k1,
                key2: key.k2,
                stamp,
                kind,
                held,
                prior_len: bytes.len() as u32,
            },
            bytes,
        );
    }

    /// Record a container write's landed change of `slot`'s `key` (a hash
    /// map, set or bitmap member, or a TTL eviction): a hash map's value lane
    /// is its prior, and the change kind says whether the key was held.
    pub(crate) fn record_change(&mut self, state: &[u8], slot: u32, key: Key, change: Change) {
        if self.admits(state, slot) {
            self.append_change(state, slot, key, change);
        }
    }

    /// [`Self::append`] for a container change: its kind and prior.
    pub(crate) fn append_change(&mut self, state: &[u8], slot: u32, key: Key, change: Change) {
        let prior = |old: Option<u64>| match old {
            Some(value) => Prior::value_u32(value as u32),
            None => Prior::Held,
        };
        let (kind, prior) = match change {
            Change::Inserted => (ChangeFlag::INSERTED, Prior::Absent),
            Change::Updated(old) => (ChangeFlag::UPDATED, prior(old)),
            Change::Removed(old) => (ChangeFlag::REMOVED, prior(old)),
            Change::Evicted(old) => (ChangeFlag::EVICTED, prior(old)),
        };
        self.append(state, slot, key, kind, prior);
    }
    /// Record one landed write of `slot` that changed each of `members`
    /// (a bitmap patch): one write, so one replay decision for all.
    pub(crate) fn record_members(
        &mut self,
        state: &[u8],
        slot: u32,
        members: impl Iterator<Item = u32>,
        kind: u8,
        held: bool,
    ) {
        if !self.watches(slot) {
            return;
        }
        let Some(Some(log)) = self.slots.get_mut(slot as usize) else {
            return;
        };
        let stamp = seq(state, slot);
        if stamp <= log.newest() {
            return;
        }
        for member in members {
            log.push(
                Entry {
                    key1: member,
                    key2: 0,
                    stamp,
                    kind,
                    held,
                    prior_len: 0,
                },
                &[],
            );
        }
    }

    /// Record a landed write of `slot` that named no key: one whole-slot
    /// entry.
    pub(crate) fn record_whole(&mut self, state: &[u8], slot: u32) {
        if !self.watches(slot) {
            return;
        }
        let Some(Some(log)) = self.slots.get_mut(slot as usize) else {
            return;
        };
        let stamp = seq(state, slot);
        if stamp > log.newest() {
            log.push(whole(stamp), &[]);
        }
    }

    /// Writes made outside the recording sites (a delta rollforward) moved
    /// watched slots' sequences: each moved past its newest entry gets a
    /// whole-slot entry at its sequence.
    pub(crate) fn record_unseen(&mut self, state: &[u8]) {
        let slots = state_num_slots(state);
        for (index, log) in self.slots.iter_mut().enumerate() {
            let Some(log) = log else { continue };
            if index as u32 >= slots {
                continue;
            }
            let stamp = seq(state, index as u32);
            if stamp > log.newest() {
                log.push(whole(stamp), &[]);
            }
        }
    }

    /// Drop every entry stamped above its slot's sequence in `state` and
    /// clamp coverage to it: `state` replaced the one the log followed with
    /// a different future. A slot the state does not hold loses its log.
    pub fn settle(&mut self, state: &[u8]) {
        for index in 0..self.slots.len() {
            let Some(log) = &mut self.slots[index] else {
                continue;
            };
            let Ok(sequence) = checked_seq(state, index as u32) else {
                self.unwatch(index as u32);
                continue;
            };
            while log.len > 0 && log.at(log.len - 1).stamp > sequence {
                log.pop_newest();
            }
            log.coverage = log.coverage.min(sequence);
        }
    }

    /// Replace this log with `other`'s rings, images and coverage, reusing
    /// this log's storage where it fits. A cold path: it reserves every
    /// buffer before it copies anything, so storage that cannot be had is
    /// `OutOfMemory` and leaves this log as it was.
    pub fn clone_from_log(&mut self, other: &Self) -> Result<(), ErrorCode> {
        let words = other.watched.len();
        reserve_total(&mut self.slots, other.slots.len())?;
        reserve_total(&mut self.watched, words)?;
        reserve_total(&mut self.suspended, words)?;
        reserve_total(&mut self.prior, other.prior.capacity())?;
        let held =
            |index: usize, slots: &[Option<SlotLog>]| matches!(slots.get(index), Some(Some(_)));
        let mut fresh = Vec::new();
        let missing = (0..other.slots.len())
            .filter(|&index| other.slots[index].is_some() && !held(index, &self.slots))
            .count();
        fresh
            .try_reserve_exact(missing)
            .map_err(|_| ErrorCode::OutOfMemory)?;
        for (index, theirs) in other.slots.iter().enumerate() {
            let Some(theirs) = theirs else { continue };
            match self.slots.get_mut(index) {
                Some(Some(mine)) => mine.reserve_for(theirs)?,
                _ => fresh.push(SlotLog::room_for(theirs)?),
            }
        }
        // Every buffer is held: nothing below allocates or fails.
        let mut fresh = fresh.into_iter();
        self.slots.resize(other.slots.len(), None);
        for (mine, theirs) in self.slots.iter_mut().zip(&other.slots) {
            match (mine.as_mut(), theirs) {
                (_, None) => *mine = None,
                (Some(log), Some(theirs)) => log.copy_from(theirs),
                (None, Some(theirs)) => {
                    let log = mine.insert(fresh.next().expect("room reserved for every new log"));
                    log.copy_from(theirs);
                }
            }
        }
        self.watched.clear();
        self.watched.extend_from_slice(&other.watched);
        self.suspended.clear();
        self.suspended.resize(words, 0);
        Ok(())
    }

    /// `slot`'s net change from `since` to its sequence in `state`; entries
    /// stamped above that sequence belong to a later state and are ignored.
    /// See [`NetAnswer`] and [`NetChanges`].
    pub(crate) fn net_changes<I: KeyIndex, S: InternBytes + ?Sized>(
        &self,
        state: &[u8],
        slot: u32,
        since: u64,
        strings: &S,
        out: &mut NetChanges<I>,
    ) -> Result<NetAnswer, ErrorCode> {
        out.records.bytes.clear();
        out.records.len = 0;
        let meta = state_init::bind_slot_in::<I>(state, slot)?;
        let Some(Some(log)) = self.slots.get(slot as usize) else {
            return Ok(NetAnswer::Overflowed);
        };
        let now = meta.write_seq(state);
        if since < log.coverage {
            return Ok(NetAnswer::Overflowed);
        }
        let in_window = |entry: &Entry| entry.stamp > since && entry.stamp <= now;
        if (0..log.len)
            .map(|index| log.at(index))
            .any(|entry| in_window(&entry) && entry.kind == WHOLE)
        {
            return Ok(NetAnswer::Overflowed);
        }
        let reader = Reader::<I>::bind(state, slot, &meta)?;
        out.prepare(log)?;
        let NetChanges {
            records,
            current,
            seen,
            generation,
            keys,
        } = out;
        let cx = &mut KeyCx::new(strings, keys);
        let mut image_at = log.image_head;
        for index in 0..log.len {
            let entry = log.at(index);
            let prior_at = image_at;
            image_at = log.image_wrap(image_at + entry.prior_len as usize);
            if !in_window(&entry) || !first_seen(seen, *generation, entry.key1, entry.key2) {
                continue;
            }
            let prior = log.prior(prior_at, entry.prior_len);
            let now = reader.current(state, entry.key1, entry.key2, cx, current)?;
            let has_prior = entry.prior_len != 0;
            match (entry.held, now) {
                (false, Current::Absent) => {}
                (false, _) => records.push(entry.key1, entry.key2, NetChangeKind::INSERT, None),
                (true, Current::Absent) => records.push(
                    entry.key1,
                    entry.key2,
                    NetChangeKind::DELETE,
                    has_prior.then_some(prior),
                ),
                (true, Current::Held) => {}
                (true, Current::Value(len)) => {
                    let value = &current[..len];
                    let same = prior.0.len() + prior.1.len() == len
                        && value[..prior.0.len()] == *prior.0
                        && value[prior.0.len()..] == *prior.1;
                    if !same {
                        records.push(entry.key1, entry.key2, NetChangeKind::UPDATE, Some(prior));
                    }
                }
                (true, Current::Unimageable) => {
                    records.push(entry.key1, entry.key2, NetChangeKind::UPDATE, Some(prior));
                }
            }
        }
        Ok(NetAnswer::Changes)
    }
}

fn whole(stamp: u64) -> Entry {
    Entry {
        stamp,
        kind: WHOLE,
        ..Entry::default()
    }
}

/// Mark `(key1, key2)` answered in this generation; false when it already
/// was. The table is twice the largest ring, so it never fills.
fn first_seen(seen: &mut [(u32, u32, u32)], generation: u32, key1: u32, key2: u32) -> bool {
    let mask = seen.len() - 1;
    let hash = (u64::from(key1) | (u64::from(key2) << 32)).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    let mut at = (hash >> 32) as usize & mask;
    loop {
        let cell = &mut seen[at];
        if cell.0 != generation {
            *cell = (generation, key1, key2);
            return true;
        }
        if cell.1 == key1 && cell.2 == key2 {
            return false;
        }
        at = (at + 1) & mask;
    }
}

/// How `net_changes` reads one slot's keys in a state.
enum Reader<'a, I: KeyIndex> {
    Map(CheckedMap<'a>),
    Set(CheckedSet<'a>),
    Struct(StructMapSlot<I>),
    Struct2(StructMap2Slot<I>),
    /// An aggregate's value cell, always present.
    Aggregate(u32),
    /// A scalar's value cell, present once its timestamp is set.
    Scalar(u32),
    /// An ordered list holding this many elements.
    List(u32),
    /// A kind no keyed write records.
    Keyless,
}

impl<'a, I: KeyIndex> Reader<'a, I> {
    fn bind(state: &'a [u8], slot: u32, meta: &SlotMetaView) -> Result<Self, ErrorCode> {
        Ok(match meta.slot_type() {
            SlotType::HashMap => Self::Map(CheckedMap::bind_in::<I>(state, slot)?),
            SlotType::HashSet | SlotType::Bitmap => {
                Self::Set(CheckedSet::bind_in::<I>(state, slot)?)
            }
            SlotType::StructMap => Self::Struct(StructMapSlot::bind_in(state, slot)),
            SlotType::StructMap2 => Self::Struct2(StructMap2Slot::bind_in(state, slot)),
            SlotType::Aggregate => Self::Aggregate(meta.offset),
            SlotType::Scalar => Self::Scalar(meta.offset),
            SlotType::OrderedList => Self::List(meta.size(state)),
            _ => Self::Keyless,
        })
    }

    fn current<S: InternBytes + ?Sized>(
        &self,
        state: &[u8],
        key1: u32,
        key2: u32,
        cx: &mut KeyCx<'_, I, S>,
        scratch: &mut Vec<u8>,
    ) -> Result<Current, ErrorCode> {
        let cell = |scratch: &mut Vec<u8>, bytes: &[u8]| {
            scratch.clear();
            scratch.extend_from_slice(bytes);
            Current::Value(bytes.len())
        };
        Ok(match self {
            Self::Map(map) => match map.get(key1) {
                Some(value) => cell(scratch, &value.to_le_bytes()),
                None => Current::Absent,
            },
            Self::Set(set) => {
                if set.contains(key1) {
                    Current::Held
                } else {
                    Current::Absent
                }
            }
            Self::Struct(map) => match map.lookup(state, key1, cx)? {
                Some(pos) => imaged(state, &map.rows(), pos, cx.strings, scratch),
                None => Current::Absent,
            },
            Self::Struct2(map) => match map.lookup(state, key1, key2, cx)? {
                Some(pos) => imaged(state, &map.rows(), pos, cx.strings, scratch),
                None => Current::Absent,
            },
            Self::Aggregate(offset) => cell(scratch, &state[*offset as usize..][..8]),
            Self::Scalar(offset) => {
                if bytes::read_f64(state, offset + 8) == f64::NEG_INFINITY {
                    Current::Absent
                } else {
                    cell(scratch, &state[*offset as usize..][..8])
                }
            }
            Self::List(size) => {
                if key1 < *size {
                    Current::Held
                } else {
                    Current::Absent
                }
            }
            Self::Keyless => Current::Held,
        })
    }
}

fn imaged<S: InternBytes + ?Sized>(
    state: &[u8],
    rows: &StructRows,
    pos: u32,
    strings: &S,
    scratch: &mut Vec<u8>,
) -> Current {
    match row_image(state, rows, pos, strings, scratch) {
        Some(()) => Current::Value(scratch.len()),
        None => Current::Unimageable,
    }
}

/// Append `bytes` within `out`'s reserved capacity; `None` when they do not
/// fit, so imaging never allocates.
fn put(out: &mut Vec<u8>, bytes: &[u8]) -> Option<()> {
    (out.capacity() - out.len() >= bytes.len()).then(|| out.extend_from_slice(bytes))
}

/// Build row `pos`'s self-contained image into `out` (07 §7 Row images):
/// the row's bytes as stored, then, in descriptor order, each set field of
/// a text or arena type as a `u32` length and its bytes. An interned string
/// is its text (an absent one, `EMPTY_KEY`, is empty), an interned-string
/// array its elements each as a `u32` length and text, any other arena
/// payload its bytes. `None` when text does not resolve or the image
/// outgrows `out`'s capacity.
fn row_image<S: InternBytes + ?Sized>(
    state: &[u8],
    rows: &StructRows,
    pos: u32,
    strings: &S,
    out: &mut Vec<u8>,
) -> Option<()> {
    out.clear();
    let row = rows.row_off(pos);
    put(
        out,
        state.get(row as usize..(row + rows.row_size) as usize)?,
    )?;
    let mut offset = rows.bitset_bytes;
    for field in 0..rows.num_fields {
        let kind = StructFieldType::from_u8(rows.field_type_byte(state, field))?;
        let cell = row + offset;
        offset += struct_field_size(kind);
        if !StructMapSlot::is_field_set(state, row, field) {
            continue;
        }
        match kind {
            StructFieldType::String => {
                let id = bytes::read_u32(state, cell);
                let text = if id == EMPTY_KEY {
                    &[][..]
                } else {
                    strings.bytes(id)?
                };
                put(out, &(text.len() as u32).to_le_bytes())?;
                put(out, text)?;
            }
            StructFieldType::ArrayString => {
                let ids = rows.arena_field(state, pos, field).ok()??;
                let at = out.len();
                put(out, &[0; 4])?;
                for id in ids.as_chunks::<4>().0 {
                    let text = strings.bytes(u32::from_le_bytes(*id))?;
                    put(out, &(text.len() as u32).to_le_bytes())?;
                    put(out, text)?;
                }
                let len = (out.len() - at - 4) as u32;
                out[at..at + 4].copy_from_slice(&len.to_le_bytes());
            }
            kind if is_arena_field_type(kind) => {
                let payload = rows.arena_field(state, pos, field).ok()??;
                put(out, &(payload.len() as u32).to_le_bytes())?;
                put(out, payload)?;
            }
            _ => {}
        }
    }
    Some(())
}

/// The recorders every key-scoped kernel write feeds: the running pass's
/// changed-key feed (§1–§5) and the watch log (§7).
#[derive(Debug, Default)]
pub(crate) struct Recorders {
    pub(crate) feed: ChangeFeed,
    pub(crate) watch: WatchLog,
}

impl Recorders {
    /// Whether a write to `slot` records anywhere now. A container kernel
    /// reads it once per call and reads priors only when it is set.
    #[inline(always)]
    pub(crate) fn records(&self, slot: u32) -> bool {
        self.feed.records(slot) || self.watch.watches(slot)
    }

    /// Record a container write's landed change of `slot`'s `key` (a hash
    /// map, set or bitmap member, or a TTL eviction): to the feed when it
    /// records the slot, and to the watch log, whose prior a hash map's
    /// value lane names and whose held-ness the change kind names.
    pub(crate) fn record_container(&mut self, state: &[u8], slot: u32, key: Key, change: Change) {
        if self.feed.records(slot) {
            self.feed.record_slot(slot, key, change);
        }
        self.watch.record_change(state, slot, key, change);
    }

    /// Record a landed bitmap patch of `slot`: every requested member to the
    /// feed, which the patch reports as counts, not members; to the watch
    /// log the members whose bit `changed` sets (bit `i` for `members[i]`),
    /// as one write.
    pub(crate) fn record_members(
        &mut self,
        state: &[u8],
        slot: u32,
        members: &[u32],
        changed: &[u64],
        change: Change,
    ) {
        if self.feed.records(slot) {
            for &member in members {
                self.feed.record_slot(slot, Key::one(member), change);
            }
        }
        if self.watch.watches(slot) {
            let (kind, held) = match change {
                Change::Inserted => (ChangeFlag::INSERTED, false),
                Change::Updated(_) => (ChangeFlag::UPDATED, true),
                Change::Removed(_) => (ChangeFlag::REMOVED, true),
                Change::Evicted(_) => (ChangeFlag::EVICTED, true),
            };
            let landed = members
                .iter()
                .enumerate()
                .filter(|&(index, _)| changed[index / 64] >> (index % 64) & 1 != 0)
                .map(|(_, &member)| member);
            self.watch.record_members(state, slot, landed, kind, held);
        }
    }

    /// A landed write changed `slot` in a way no key names.
    pub(crate) fn changed_whole(&mut self, state: &[u8], slot: u32) {
        self.feed.changed_whole(slot);
        self.watch.record_whole(state, slot);
    }

    /// Suspend both for a capacity dry run; answers whether the feed was
    /// armed, for [`Self::resume`].
    pub(crate) fn suspend(&mut self) -> bool {
        self.watch.suspend();
        self.feed.suspend()
    }

    pub(crate) fn resume(&mut self, armed: bool) {
        self.watch.resume();
        self.feed.resume(armed);
    }
}

/// A scalar's first write inserts it: before it, its timestamp is unset and
/// a reader decodes no value.
pub(crate) fn scalar_prior(state: &[u8], offset: u32) -> Prior {
    if bytes::read_f64(state, offset + 8) == f64::NEG_INFINITY {
        Prior::Absent
    } else {
        Prior::value_cell(state, offset)
    }
}
