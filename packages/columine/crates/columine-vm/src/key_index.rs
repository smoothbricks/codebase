//! The key index of a struct-map slot.
//!
//! A struct-map slot is laid out `[descriptor align8(nf)][index region][rows]`,
//! then a one-key map's optional timestamp lane, then its CSR arena. A row's
//! ordinal is its identity: its bytes live at `rows_base + ordinal * row_size`,
//! and the index hands out ordinals in `0..capacity`. Which ordinal a key owns,
//! the order keys iterate in, and every byte of the index region belong to one
//! [`KeyIndex`] family: a static type whose associated functions the VM calls,
//! monomorphized, for every key it reads or writes. Nothing else reads or
//! writes an index region.
//!
//! A state image belongs to one family. The family names its state-header
//! magic and format version; initialization writes them and every header check
//! compares them, so an image of another family is refused before it is read.
//!
//! Mutation is planned, then applied. A plan names every index byte its apply
//! may change as [`IndexWrites`]: absolute ranges a journaled writer captures in
//! the same frame as the slot's size, change flag and row bytes, so a rollback
//! restores the index byte for byte. Sentinel keys (`EMPTY_KEY` or `TOMBSTONE`
//! in lane 1) are the VM's to skip or refuse per opcode before any index call.
//!
//! Key lanes are declared by the program ([`KeyLanes`]): a lane holds raw `u32`
//! values or interned-string ids. A family that orders keys reads an interned
//! id's UTF-8 bytes through the caller's [`InternBytes`]; an id it cannot
//! resolve is a refusal value, never a panic.
//!
//! [`HashIndex`] is the open-addressed linear-probe table every image of this
//! VM's own header uses.

use crate::bytes;
use crate::hash_table::{
    Probe, ProbeCell, find_key, find_key_pair, max_load_for_cap, next_live_key, probe_linear,
};
use columine_types::types::{
    EMPTY_KEY, ErrorCode, STATE_FORMAT_VERSION, STATE_MAGIC, SlotTypeFlags, TOMBSTONE, hash_key,
    hash_key_pair, next_power_of_2,
};
use core::fmt::Debug;

/// What one key lane holds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LaneKind {
    /// A `u32`, ordered numerically.
    Raw,
    /// An interned-string id, ordered by the UTF-8 bytes of its text.
    Interned,
}

impl LaneKind {
    const fn of(interned: bool) -> Self {
        if interned { Self::Interned } else { Self::Raw }
    }
}

/// The key lanes one struct-map slot declares.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyLanes {
    /// A one-key map (`STRUCT_MAP`).
    One(LaneKind),
    /// A two-key map (`STRUCT_MAP2`): lane 1, then lane 2.
    Two(LaneKind, LaneKind),
}

impl KeyLanes {
    /// The lanes a struct-map declaration's type byte declares; `None` for a
    /// lane-2 bit on a one-key map. Bits that are no key-lane bit are ignored.
    pub const fn declared(pair: bool, flags: SlotTypeFlags) -> Option<Self> {
        if pair {
            Some(Self::Two(
                LaneKind::of(flags.struct_key1_interned()),
                LaneKind::of(flags.struct_key2_interned()),
            ))
        } else if flags.struct_key2_interned() {
            None
        } else {
            Some(Self::One(LaneKind::of(flags.struct_key1_interned())))
        }
    }

    /// The lanes slot-metadata byte `STRUCT_KEY_LANES` holds; `None` for a
    /// byte no declaration writes.
    pub const fn stored(pair: bool, byte: u8) -> Option<Self> {
        if SlotTypeFlags::from_byte(byte).struct_key_lanes() != byte {
            return None;
        }
        Self::declared(pair, SlotTypeFlags::from_byte(byte))
    }

    /// The lanes an unchecked binder reads from a byte its slot's geometry
    /// check already admitted.
    pub(crate) const fn bound(pair: bool, byte: u8) -> Self {
        if pair {
            Self::Two(
                LaneKind::of(byte & SlotTypeFlags::STRUCT_KEY1_INTERNED != 0),
                LaneKind::of(byte & SlotTypeFlags::STRUCT_KEY2_INTERNED != 0),
            )
        } else {
            Self::One(LaneKind::of(
                byte & SlotTypeFlags::STRUCT_KEY1_INTERNED != 0,
            ))
        }
    }

    /// The `STRUCT_KEY_LANES` metadata byte of these lanes.
    pub const fn byte(self) -> u8 {
        const fn bit(kind: LaneKind, bit: u8) -> u8 {
            match kind {
                LaneKind::Raw => 0,
                LaneKind::Interned => bit,
            }
        }
        match self {
            Self::One(k1) => bit(k1, SlotTypeFlags::STRUCT_KEY1_INTERNED),
            Self::Two(k1, k2) => {
                bit(k1, SlotTypeFlags::STRUCT_KEY1_INTERNED)
                    | bit(k2, SlotTypeFlags::STRUCT_KEY2_INTERNED)
            }
        }
    }

    /// Whether these are a two-key map's lanes.
    pub const fn is_pair(self) -> bool {
        matches!(self, Self::Two(..))
    }

    /// The number of `u32` lanes a key carries: 1 or 2.
    pub const fn count(self) -> u32 {
        if self.is_pair() { 2 } else { 1 }
    }
}

/// One key: lane 1 and, for a two-key map, lane 2 (`0` for a one-key map).
/// The derived order is lane 1, then lane 2.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Key {
    pub k1: u32,
    pub k2: u32,
}

impl Key {
    /// A one-key map's key.
    pub const fn one(k1: u32) -> Self {
        Self { k1, k2: 0 }
    }

    /// A two-key map's key.
    pub const fn two(k1: u32, k2: u32) -> Self {
        Self { k1, k2 }
    }
}

/// One slot's index region, bound from its metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexRegion {
    /// Absolute offset of the region's first byte.
    pub at: u32,
    /// The ordinals the index hands out are `0..capacity`.
    pub capacity: u32,
    pub lanes: KeyLanes,
}

/// Most ranges one index plan names.
pub const INDEX_WRITES_MAX: usize = 8;

/// Every index byte one planned apply may change, as disjoint absolute
/// `(offset, len)` ranges held inline: planning never allocates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexWrites {
    ranges: [(u32, u32); INDEX_WRITES_MAX],
    len: u8,
}

impl IndexWrites {
    /// A plan that writes nothing.
    pub const NONE: Self = Self {
        ranges: [(0, 0); INDEX_WRITES_MAX],
        len: 0,
    };

    /// Append one range. A plan names at most [`INDEX_WRITES_MAX`]; more is a
    /// family's bug, not an operational state.
    pub fn push(&mut self, at: u32, len: u32) {
        let index = usize::from(self.len);
        assert!(
            index < INDEX_WRITES_MAX,
            "an index plan names at most INDEX_WRITES_MAX ranges"
        );
        self.ranges[index] = (at, len);
        self.len += 1;
    }

    pub fn as_slice(&self) -> &[(u32, u32)] {
        &self.ranges[..usize::from(self.len)]
    }
}

/// One planned insert: the ordinal the key owns, or will own once applied.
#[derive(Clone, Copy, Debug)]
pub struct InsertPlan<P> {
    pub ordinal: u32,
    /// `false` when the key is already present at `ordinal`: nothing to apply.
    pub is_new: bool,
    /// Every index byte the apply may change; none when `!is_new`.
    pub writes: IndexWrites,
    /// The family's own continuation of the plan.
    pub plan: P,
}

/// One planned remove of a present key.
#[derive(Clone, Copy, Debug)]
pub struct RemovePlan<P> {
    /// The ordinal the key owned; its row bytes are the caller's.
    pub ordinal: u32,
    pub writes: IndexWrites,
    pub plan: P,
}

/// The resolver of interned key lanes: an id's UTF-8 bytes, or `None` for
/// an id the caller's interner does not hold. Bytes, not `&str`: a resolver
/// slices text it already validated, and an index compares bytes.
pub trait InternBytes {
    fn bytes(&self, id: u32) -> Option<&[u8]>;
}

/// What a key-index call needs besides the image: the resolver of interned
/// lanes and the family's scratch.
pub struct KeyCx<'k, I: KeyIndex, S: InternBytes + ?Sized> {
    pub strings: &'k S,
    pub scratch: &'k mut I::Scratch,
}

impl<'k, I: KeyIndex, S: InternBytes + ?Sized> KeyCx<'k, I, S> {
    pub fn new(strings: &'k S, scratch: &'k mut I::Scratch) -> Self {
        Self { strings, scratch }
    }
}

/// The resolver the [`HashIndex`] entry points pass, which take none: it
/// resolves no id, so a family that must read an interned lane refuses.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoStrings;

impl InternBytes for NoStrings {
    fn bytes(&self, _id: u32) -> Option<&[u8]> {
        None
    }
}

/// One VM's key working memory, reserved as it is used and kept: the
/// family's scratch, and the space a seal permutes rows and arena payloads
/// through.
#[derive(Debug, Default)]
pub struct KeyScratch<I: KeyIndex> {
    pub index: I::Scratch,
    pub(crate) rows: Vec<u8>,
    pub(crate) arena: Vec<u8>,
    pub(crate) ordinals: Vec<u32>,
}

/// A key-index family: the layout of an index region and every operation
/// on it. A family is a type with no value; the VM is generic over it and
/// calls it monomorphized.
pub trait KeyIndex: Copy + Debug + Default + 'static {
    /// State-header magic of every image of this family.
    const STATE_MAGIC: u32;
    /// State-header format version of every image of this family.
    const FORMAT_VERSION: u8;

    /// The family's continuation of an insert plan.
    type InsertPlan: Copy + Debug;
    /// The family's continuation of a remove plan.
    type RemovePlan: Copy + Debug;
    /// Working memory a call may use: composite key bytes, seal space.
    type Scratch: Debug + Default;

    /// The capacity of a slot whose program declares `declared` rows.
    fn capacity_for(declared: u32) -> u32;
    /// The capacity a slot of `capacity` grows to; `InvalidState` on overflow.
    fn grown_capacity(capacity: u32) -> Result<u32, ErrorCode>;
    /// The most rows a slot of `capacity` admits; a new key past it is
    /// `CapacityExceeded`, which the VM answers with `NEEDS_GROWTH`.
    fn max_rows(capacity: u32) -> u32;
    /// The byte length of an index region; a multiple of 8 for every
    /// capacity the family hands out.
    fn region_bytes(capacity: u32, lanes: KeyLanes) -> u64;
    /// Lay out an empty region.
    fn init(state: &mut [u8], region: IndexRegion);
    /// Check a region holding `size` keys before any call trusts it.
    fn validate(state: &[u8], region: IndexRegion, size: u32) -> Result<(), ErrorCode>;

    /// The ordinal `key` owns, or `None`.
    fn find<S: InternBytes + ?Sized>(
        state: &[u8],
        region: IndexRegion,
        key: Key,
        cx: &mut KeyCx<'_, Self, S>,
    ) -> Result<Option<u32>, ErrorCode>;
    /// Plan inserting `key` into a region holding `size` keys, changing
    /// nothing.
    fn plan_insert<S: InternBytes + ?Sized>(
        state: &[u8],
        region: IndexRegion,
        size: u32,
        key: Key,
        cx: &mut KeyCx<'_, Self, S>,
    ) -> Result<InsertPlan<Self::InsertPlan>, ErrorCode>;
    /// Apply a new-key plan made against this unchanged region. The caller
    /// owns the slot's size.
    fn apply_insert<S: InternBytes + ?Sized>(
        state: &mut [u8],
        region: IndexRegion,
        key: Key,
        plan: &InsertPlan<Self::InsertPlan>,
        cx: &mut KeyCx<'_, Self, S>,
    ) -> Result<(), ErrorCode>;
    /// Plan removing `key`; `None` when it is absent.
    fn plan_remove<S: InternBytes + ?Sized>(
        state: &[u8],
        region: IndexRegion,
        key: Key,
        cx: &mut KeyCx<'_, Self, S>,
    ) -> Result<Option<RemovePlan<Self::RemovePlan>>, ErrorCode>;
    /// Apply a remove plan made against this unchanged region. The caller
    /// owns the slot's size and the freed row's bytes.
    fn apply_remove<S: InternBytes + ?Sized>(
        state: &mut [u8],
        region: IndexRegion,
        plan: &RemovePlan<Self::RemovePlan>,
        cx: &mut KeyCx<'_, Self, S>,
    ) -> Result<(), ErrorCode>;
    /// The key `ordinal` holds; `None` for a dead or out-of-range ordinal.
    fn key_at(state: &[u8], region: IndexRegion, ordinal: u32) -> Option<Key>;
    /// The first live ordinal in the family's iteration order.
    fn first<S: InternBytes + ?Sized>(
        state: &[u8],
        region: IndexRegion,
        cx: &mut KeyCx<'_, Self, S>,
    ) -> Result<Option<u32>, ErrorCode>;
    /// The live ordinal after `ordinal` in the family's iteration order.
    fn next<S: InternBytes + ?Sized>(
        state: &[u8],
        region: IndexRegion,
        ordinal: u32,
        cx: &mut KeyCx<'_, Self, S>,
    ) -> Result<Option<u32>, ErrorCode>;
    /// The first live ordinal, in iteration order, whose lane 1 is `k1`.
    fn first_with_key1<S: InternBytes + ?Sized>(
        state: &[u8],
        region: IndexRegion,
        k1: u32,
        cx: &mut KeyCx<'_, Self, S>,
    ) -> Result<Option<u32>, ErrorCode>;
    /// The live ordinal after `ordinal`, in iteration order, whose lane 1 is
    /// `k1`. Removing `ordinal` afterwards leaves the answer live: ordinals
    /// never move except by grow and seal.
    fn next_with_key1<S: InternBytes + ?Sized>(
        state: &[u8],
        region: IndexRegion,
        ordinal: u32,
        k1: u32,
        cx: &mut KeyCx<'_, Self, S>,
    ) -> Result<Option<u32>, ErrorCode>;
    /// Copy every key of `from` in `old` into the empty larger `to` in `new`,
    /// calling `moved(new, old_ordinal, new_ordinal)` once per key so the
    /// caller moves its row; answers how many keys moved.
    fn grow(
        old: &[u8],
        from: IndexRegion,
        new: &mut [u8],
        to: IndexRegion,
        moved: impl FnMut(&mut [u8], u32, u32),
    ) -> Result<u32, ErrorCode>;
    /// Rebuild the region holding `size` keys canonically: its bytes become a
    /// function of its keys and capacity alone. `moved(old_ordinal,
    /// new_ordinal)` names each key's new ordinal so the caller moves its row.
    fn seal<S: InternBytes + ?Sized>(
        state: &mut [u8],
        region: IndexRegion,
        size: u32,
        cx: &mut KeyCx<'_, Self, S>,
        moved: impl FnMut(u32, u32),
    ) -> Result<(), ErrorCode>;
}

/// The open-addressed linear-probe table: lane 1's `u32` cells, then a
/// two-key map's lane 2 cells, one per ordinal. `EMPTY_KEY` or `TOMBSTONE`
/// in lane 1 marks a dead cell; lane 2 holds any `u32`. A key's hash picks
/// its home cell, a probe walks forward from it, a remove leaves a tombstone,
/// and an insert reuses the first tombstone only when the key is absent.
/// Admission stops at 70 % load. Iteration is ascending cell order, the
/// order the TypeScript readers walk. Lane kinds are ignored: a key is its
/// `u32` lanes, and no string is ever resolved.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HashIndex;

/// [`HashIndex`]'s seal space: each live key with the cell it held.
#[derive(Debug, Default)]
pub struct HashScratch {
    live: Vec<(Key, u32)>,
}

impl HashIndex {
    const fn lane2(region: IndexRegion) -> u32 {
        region.at + region.capacity * 4
    }

    fn key1_at(state: &[u8], region: IndexRegion, pos: u32) -> u32 {
        bytes::read_u32(state, region.at + pos * 4)
    }

    const fn home(region: IndexRegion, key: Key) -> u32 {
        match region.lanes {
            KeyLanes::One(_) => hash_key(key.k1, region.capacity),
            KeyLanes::Two(..) => hash_key_pair(key.k1, key.k2, region.capacity),
        }
    }

    fn write_key(state: &mut [u8], region: IndexRegion, pos: u32, key: Key) {
        bytes::write_u32(state, region.at + pos * 4, key.k1);
        if region.lanes.is_pair() {
            bytes::write_u32(state, Self::lane2(region) + pos * 4, key.k2);
        }
    }

    /// Insert-or-update probe: a present key before the first reusable
    /// tombstone. `None` when the probe finds neither.
    pub(crate) fn probe_insert(state: &[u8], region: IndexRegion, key: Key) -> Option<Probe> {
        let pair = region.lanes.is_pair();
        probe_linear(region.capacity, Self::home(region, key), true, |pos| {
            let first = Self::key1_at(state, region, pos);
            if first == key.k1
                && (!pair || bytes::read_u32(state, Self::lane2(region) + pos * 4) == key.k2)
            {
                ProbeCell::Match
            } else {
                match first {
                    EMPTY_KEY => ProbeCell::Empty,
                    TOMBSTONE => ProbeCell::Tombstone,
                    _ => ProbeCell::Occupied,
                }
            }
        })
    }

    /// Place `key` in a region holding no tombstone and no copy of it (growth,
    /// seal): the first empty cell from its home.
    fn place(state: &mut [u8], region: IndexRegion, key: Key) -> u32 {
        let mask = region.capacity - 1;
        let mut pos = Self::home(region, key);
        while Self::key1_at(state, region, pos) != EMPTY_KEY {
            pos = (pos + 1) & mask;
        }
        Self::write_key(state, region, pos, key);
        pos
    }

    /// The first cell at or after `from` whose lane 1 holds `k1`.
    fn key1_from(state: &[u8], region: IndexRegion, from: u32, k1: u32) -> Option<u32> {
        if k1 == EMPTY_KEY || k1 == TOMBSTONE {
            return None;
        }
        (from..region.capacity).find(|&pos| Self::key1_at(state, region, pos) == k1)
    }
}

impl KeyIndex for HashIndex {
    const STATE_MAGIC: u32 = STATE_MAGIC;
    const FORMAT_VERSION: u8 = STATE_FORMAT_VERSION;

    type InsertPlan = ();
    type RemovePlan = ();
    type Scratch = HashScratch;

    fn capacity_for(declared: u32) -> u32 {
        next_power_of_2(declared * 2)
    }

    fn grown_capacity(capacity: u32) -> Result<u32, ErrorCode> {
        crate::state_init::grown_capacity(capacity)
    }

    fn max_rows(capacity: u32) -> u32 {
        max_load_for_cap(capacity)
    }

    fn region_bytes(capacity: u32, lanes: KeyLanes) -> u64 {
        u64::from(capacity) * 4 * u64::from(lanes.count())
    }

    fn init(state: &mut [u8], region: IndexRegion) {
        bytes::fill_u32(state, region.at, region.capacity, EMPTY_KEY);
        if region.lanes.is_pair() {
            bytes::zero(state, Self::lane2(region), region.capacity * 4);
        }
    }

    /// Every cell value is a probe-table state: a sentinel or a live key.
    fn validate(_state: &[u8], _region: IndexRegion, _size: u32) -> Result<(), ErrorCode> {
        Ok(())
    }

    fn find<S: InternBytes + ?Sized>(
        state: &[u8],
        region: IndexRegion,
        key: Key,
        _cx: &mut KeyCx<'_, Self, S>,
    ) -> Result<Option<u32>, ErrorCode> {
        Ok(match region.lanes {
            KeyLanes::One(_) => find_key(state, region.at, region.capacity, key.k1),
            KeyLanes::Two(..) => find_key_pair(
                state,
                region.at,
                Self::lane2(region),
                region.capacity,
                key.k1,
                key.k2,
            ),
        })
    }

    fn plan_insert<S: InternBytes + ?Sized>(
        state: &[u8],
        region: IndexRegion,
        size: u32,
        key: Key,
        _cx: &mut KeyCx<'_, Self, S>,
    ) -> Result<InsertPlan<()>, ErrorCode> {
        if key.k1 == EMPTY_KEY || key.k1 == TOMBSTONE {
            return Err(ErrorCode::InvalidKey);
        }
        let probe = Self::probe_insert(state, region, key).ok_or(ErrorCode::CapacityExceeded)?;
        if probe.found {
            return Ok(InsertPlan {
                ordinal: probe.pos,
                is_new: false,
                writes: IndexWrites::NONE,
                plan: (),
            });
        }
        if size >= max_load_for_cap(region.capacity) {
            return Err(ErrorCode::CapacityExceeded);
        }
        let mut writes = IndexWrites::NONE;
        writes.push(region.at + probe.pos * 4, 4);
        if region.lanes.is_pair() {
            writes.push(Self::lane2(region) + probe.pos * 4, 4);
        }
        Ok(InsertPlan {
            ordinal: probe.pos,
            is_new: true,
            writes,
            plan: (),
        })
    }

    fn apply_insert<S: InternBytes + ?Sized>(
        state: &mut [u8],
        region: IndexRegion,
        key: Key,
        plan: &InsertPlan<()>,
        _cx: &mut KeyCx<'_, Self, S>,
    ) -> Result<(), ErrorCode> {
        Self::write_key(state, region, plan.ordinal, key);
        Ok(())
    }

    fn plan_remove<S: InternBytes + ?Sized>(
        state: &[u8],
        region: IndexRegion,
        key: Key,
        cx: &mut KeyCx<'_, Self, S>,
    ) -> Result<Option<RemovePlan<()>>, ErrorCode> {
        Ok(Self::find(state, region, key, cx)?.map(|ordinal| {
            let mut writes = IndexWrites::NONE;
            writes.push(region.at + ordinal * 4, 4);
            RemovePlan {
                ordinal,
                writes,
                plan: (),
            }
        }))
    }

    fn apply_remove<S: InternBytes + ?Sized>(
        state: &mut [u8],
        region: IndexRegion,
        plan: &RemovePlan<()>,
        _cx: &mut KeyCx<'_, Self, S>,
    ) -> Result<(), ErrorCode> {
        bytes::write_u32(state, region.at + plan.ordinal * 4, TOMBSTONE);
        Ok(())
    }

    fn key_at(state: &[u8], region: IndexRegion, ordinal: u32) -> Option<Key> {
        if ordinal >= region.capacity {
            return None;
        }
        match Self::key1_at(state, region, ordinal) {
            EMPTY_KEY | TOMBSTONE => None,
            k1 => Some(Key {
                k1,
                k2: if region.lanes.is_pair() {
                    bytes::read_u32(state, Self::lane2(region) + ordinal * 4)
                } else {
                    0
                },
            }),
        }
    }

    fn first<S: InternBytes + ?Sized>(
        state: &[u8],
        region: IndexRegion,
        _cx: &mut KeyCx<'_, Self, S>,
    ) -> Result<Option<u32>, ErrorCode> {
        let pos = next_live_key(state, region.at, region.capacity, 0);
        Ok((pos < region.capacity).then_some(pos))
    }

    /// The scan continues past a dead `ordinal` too: a cell position is a
    /// cursor whether or not its key is live.
    fn next<S: InternBytes + ?Sized>(
        state: &[u8],
        region: IndexRegion,
        ordinal: u32,
        _cx: &mut KeyCx<'_, Self, S>,
    ) -> Result<Option<u32>, ErrorCode> {
        let pos = next_live_key(state, region.at, region.capacity, ordinal.saturating_add(1));
        Ok((pos < region.capacity).then_some(pos))
    }

    fn first_with_key1<S: InternBytes + ?Sized>(
        state: &[u8],
        region: IndexRegion,
        k1: u32,
        _cx: &mut KeyCx<'_, Self, S>,
    ) -> Result<Option<u32>, ErrorCode> {
        Ok(Self::key1_from(state, region, 0, k1))
    }

    fn next_with_key1<S: InternBytes + ?Sized>(
        state: &[u8],
        region: IndexRegion,
        ordinal: u32,
        k1: u32,
        _cx: &mut KeyCx<'_, Self, S>,
    ) -> Result<Option<u32>, ErrorCode> {
        Ok(Self::key1_from(
            state,
            region,
            ordinal.saturating_add(1),
            k1,
        ))
    }

    /// Rehash in ascending old-cell order, so equal tables grow to equal bytes.
    fn grow(
        old: &[u8],
        from: IndexRegion,
        new: &mut [u8],
        to: IndexRegion,
        mut moved: impl FnMut(&mut [u8], u32, u32),
    ) -> Result<u32, ErrorCode> {
        Self::init(new, to);
        let mut rehashed = 0u32;
        for ordinal in 0..from.capacity {
            let Some(key) = Self::key_at(old, from, ordinal) else {
                continue;
            };
            let pos = Self::place(new, to, key);
            moved(new, ordinal, pos);
            rehashed += 1;
        }
        Ok(rehashed)
    }

    /// Canonical rehash: the live keys reinserted into an empty region in
    /// ascending `(k1, k2)` order, so every tombstone is gone and the cells are
    /// a function of the key set and the capacity.
    fn seal<S: InternBytes + ?Sized>(
        state: &mut [u8],
        region: IndexRegion,
        _size: u32,
        cx: &mut KeyCx<'_, Self, S>,
        mut moved: impl FnMut(u32, u32),
    ) -> Result<(), ErrorCode> {
        let live = &mut cx.scratch.live;
        live.clear();
        let count = (0..region.capacity)
            .filter(|&pos| Self::key_at(state, region, pos).is_some())
            .count();
        live.try_reserve(count)
            .map_err(|_| ErrorCode::OutOfMemory)?;
        live.extend(
            (0..region.capacity)
                .filter_map(|pos| Self::key_at(state, region, pos).map(|key| (key, pos))),
        );
        live.sort_unstable();
        Self::init(state, region);
        for &(key, old) in live.iter() {
            moved(old, Self::place(state, region, key));
        }
        Ok(())
    }
}
