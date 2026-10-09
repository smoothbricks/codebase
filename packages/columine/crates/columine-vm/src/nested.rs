//! Nested container slots: arena-allocated inner maps, sets, and aggregates.
//!
//! `Map<K, Set<V>>`, `Map<K, Map<K2, V>>`, and `Map<K, Agg>` use an outer hash
//! table whose values are offsets into a bump arena. Inner containers are
//! initialized on first sight of an outer key and grow by rehashing into a new
//! arena allocation.
//!
//! Preparation retains the original mutable image, one outer/inner probe, and
//! any rehash destinations. A caller captures the plan's actual overwritten
//! bytes before consuming it, including unused cells and arena metadata.
//! Applying returns the same image and retained ranges for physical undo/redo;
//! neither a full arena snapshot nor a second probe is needed. Refused plans
//! leave the outer table, arena usage, values and flags unchanged.
//!
//! Raw public writers are unjournaled caller-owned storage operations.
//! The VM's speculative entrypoints capture these paid plans before applying.

use crate::meta::SlotMetaView;
use crate::{aggregates, bytes, hash_table};
use columine_types::types::{
    AggType, ChangeFlag, EMPTY_KEY, ErrorCode, SlotType, TOMBSTONE, hash_key, next_power_of_2,
};

/// Prefix stored at the start of a nested slot's data:
/// `[inner_type:u8][inner_initial_cap:u16 le][inner_agg_type:u8][depth:u8][reserved:3]`.
pub const NESTED_PREFIX_SIZE: u32 = 8;

/// Arena header: `[capacity:u32][used:u32]`.
pub const ARENA_HDR_SIZE: u32 = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NestedPrefix {
    pub inner_type: SlotType,
    pub inner_initial_cap: u16,
    /// Raw AggType byte (see `aggregates` on why raw bytes are the contract).
    pub inner_agg_type_byte: u8,
    pub depth: u8,
}

/// Write a nested-slot prefix without journaling.
pub fn write_nested_prefix(state: &mut [u8], slot_offset: u32, prefix: NestedPrefix) {
    let base = slot_offset as usize;
    state[base] = prefix.inner_type as u8;
    state[base + 1] = (prefix.inner_initial_cap & 0xff) as u8;
    state[base + 2] = (prefix.inner_initial_cap >> 8) as u8;
    state[base + 3] = prefix.inner_agg_type_byte;
    state[base + 4] = prefix.depth;
    state[base + 5..base + 8].fill(0);
}

/// Read a nested-slot prefix. The stored inner type is an invariant this
/// crate wrote; an invalid low nibble is a programmer error.
pub fn read_nested_prefix(state: &[u8], slot_offset: u32) -> NestedPrefix {
    let base = slot_offset as usize;
    let inner_type = SlotType::from_u8(state[base] & 0x0f).unwrap_or_else(|| {
        columine_types::die!("invariant: nested prefix contains an invalid inner slot type")
    });
    let agg_byte = state[base + 3];
    NestedPrefix {
        inner_type,
        inner_initial_cap: u16::from(state[base + 1]) | (u16::from(state[base + 2]) << 8),
        // Raw byte: `nested_agg_update` fails closed via AggType::from_u8.
        inner_agg_type_byte: agg_byte,
        depth: state[base + 4],
    }
}

/// Offset of the outer key table.
pub const fn outer_keys_offset(slot_offset: u32) -> u32 {
    slot_offset + NESTED_PREFIX_SIZE
}

/// Offset of the outer pointer table.
pub const fn outer_ptrs_offset(slot_offset: u32, capacity: u32) -> u32 {
    outer_keys_offset(slot_offset) + capacity * 4
}

/// Offset of the arena header.
pub const fn arena_header_offset(slot_offset: u32, capacity: u32) -> u32 {
    outer_ptrs_offset(slot_offset, capacity) + capacity * 4
}

/// Offset of arena data.
pub const fn arena_data_offset(slot_offset: u32, capacity: u32) -> u32 {
    arena_header_offset(slot_offset, capacity) + ARENA_HDR_SIZE
}

/// Compute inline-header size for an inner container.
pub fn inner_container_size(inner_type: SlotType, capacity: u32, inner_agg_type_byte: u8) -> u32 {
    match inner_type {
        SlotType::HashMap => hash_table::hashmap_byte_size(capacity),
        SlotType::HashSet => hash_table::hashset_byte_size(capacity),
        SlotType::Aggregate => aggregates::agg_slot_byte_size(inner_agg_type_byte),
        _ => 0,
    }
}

/// Compute nested slot data size: prefix + outer keys + outer pointers + arena
/// header + pre-sized inner containers, each rounded to eight-byte alignment.
pub fn nested_slot_data_size(
    outer_cap: u32,
    inner_initial_cap: u32,
    inner_type: SlotType,
    inner_agg_type_byte: u8,
) -> u32 {
    let per_inner = columine_types::types::align8(inner_container_size(
        inner_type,
        inner_initial_cap,
        inner_agg_type_byte,
    ));
    NESTED_PREFIX_SIZE + outer_cap * 4 + outer_cap * 4 + ARENA_HDR_SIZE + outer_cap * per_inner
}

/// Initialize a nested slot's arena header without journaling.
pub fn write_arena_header(state: &mut [u8], header_offset: u32, arena_capacity: u32) {
    bytes::write_u32(state, header_offset, arena_capacity);
    bytes::write_u32(state, header_offset + 4, 0);
}

// =============================================================================
// Arena — bump allocator within the slot data region

/// Arena header field offsets.
const ARENA_HDR_USED: u32 = 4;

/// A bound arena view. Carries offsets, never pointers (see `bytes`).
#[derive(Clone, Copy, Debug)]
pub struct Arena {
    /// Absolute offset of the arena header (`[capacity:u32][used:u32]`).
    pub hdr_offset: u32,
    /// Absolute offset of the arena data start.
    pub data_offset: u32,
}

impl Arena {
    /// Bind to an arena header.
    pub const fn bind(hdr_offset: u32) -> Self {
        Self {
            hdr_offset,
            data_offset: hdr_offset + ARENA_HDR_SIZE,
        }
    }

    /// Initialize `[capacity][used=0]` and bind without journaling.
    pub fn init_at(state: &mut [u8], hdr_offset: u32, cap: u32) -> Self {
        write_arena_header(state, hdr_offset, cap);
        Self::bind(hdr_offset)
    }

    /// Read arena capacity.
    pub fn capacity(&self, state: &[u8]) -> u32 {
        bytes::read_u32(state, self.hdr_offset)
    }

    /// Read arena usage.
    pub fn used(&self, state: &[u8]) -> u32 {
        bytes::read_u32(state, self.hdr_offset + ARENA_HDR_USED)
    }

    fn set_used(&self, state: &mut [u8], val: u32) {
        bytes::write_u32(state, self.hdr_offset + ARENA_HDR_USED, val);
    }

    /// Bump-allocate `size` bytes with eight-byte alignment. Return the
    /// absolute offset, or `None` when the arena is full.
    /// Unjournaled; the caller owns or has captured the arena usage word.
    pub fn alloc(&self, state: &mut [u8], size: u32) -> Option<u32> {
        let u = self.used(state);
        let aligned = columine_types::types::align8(size);
        if u + aligned > self.capacity(state) {
            return None;
        }
        let offset = self.data_offset + u;
        self.set_used(state, u + aligned);
        Some(offset)
    }
}

// =============================================================================
// Outer hash table — typed access to keys and arena pointers

/// Bound outer-table view carrying offsets.
#[derive(Clone, Copy, Debug)]
pub struct OuterTable {
    pub cap: u32,
    /// Offset of the outer u32 size field (slot metadata `SIZE`).
    pub size_off: u32,
    pub keys_off: u32,
    /// Arena-offset array, one u32 per outer key slot.
    pub ptrs_off: u32,
    pub arena: Arena,
}

impl OuterTable {
    /// Bind to a nested slot's outer table.
    pub fn bind(meta: &SlotMetaView) -> Self {
        let slot_off = meta.offset;
        let cap = meta.capacity;
        Self {
            cap,
            size_off: meta.meta_base + columine_types::types::SlotMetaOffset::SIZE,
            keys_off: outer_keys_offset(slot_off),
            ptrs_off: outer_ptrs_offset(slot_off, cap),
            arena: Arena::bind(arena_header_offset(slot_off, cap)),
        }
    }

    fn key_at(&self, state: &[u8], pos: u32) -> u32 {
        bytes::read_u32(state, self.keys_off + pos * 4)
    }

    fn ptr_at(&self, state: &[u8], pos: u32) -> u32 {
        bytes::read_u32(state, self.ptrs_off + pos * 4)
    }

    fn size(&self, state: &[u8]) -> u32 {
        bytes::read_u32(state, self.size_off)
    }

    /// Look up an outer key's inner-container offset, or return zero when
    /// absent. Arena data never starts at offset zero, so zero is outside the
    /// domain of live inner offsets and is a sound absence marker.
    pub fn lookup(&self, state: &[u8], outer_key: u32) -> u32 {
        hash_table::find_key(state, self.keys_off, self.cap, outer_key)
            .map_or(0, |pos| self.ptr_at(state, pos))
    }
}

// =============================================================================
/// Retained work space for nested mutations, shared by every call on one VM.
/// Each buffer keeps the high-water mark of the plans it has admitted: a plan
/// reserves the capacity its own path can push before it writes, and a warm
/// plan that fits that mark allocates nothing.
#[derive(Debug, Default)]
pub struct NestedScratch {
    /// Disjoint byte ranges the current plan may overwrite.
    ranges: Vec<(u32, u32)>,
    /// Rehash destinations: one per live inner key plus the inserted key.
    placements: Vec<Placement>,
    /// Occupancy of the grown inner table's cells while destinations are placed.
    occupied: Vec<bool>,
}

/// Most ranges a plan that does not rehash can push: an outer insert pushes its
/// key, size, inner header, entry, arena usage, outer pointer and change flag.
const PLAIN_PLAN_RANGES: usize = 7;

impl NestedScratch {
    /// Bytes held by the retained buffers, which is the admitted high-water mark.
    pub fn retained_bytes(&self) -> usize {
        self.ranges.capacity() * std::mem::size_of::<(u32, u32)>()
            + self.placements.capacity() * std::mem::size_of::<Placement>()
            + self.occupied.capacity() * std::mem::size_of::<bool>()
    }

    /// Start a plan: drop the previous plan's ranges and admit the ranges a
    /// plan without rehash can push. A refusal here precedes every write.
    fn begin_plan(&mut self) -> Result<(), ErrorCode> {
        self.ranges.clear();
        self.admit_ranges(PLAIN_PLAN_RANGES)
    }

    /// Admit a rehash from `old_cap` cells into `grown_cap` cells. The whole
    /// plan pushes at most `old_cap + 8` ranges: four before this point, three
    /// finishing ranges, and one run per destination, of which there are at
    /// most `old_cap + 1`.
    fn admit_rehash(&mut self, old_cap: u32, grown_cap: u32) -> Result<(), ErrorCode> {
        self.admit_ranges(old_cap as usize + 8)?;
        reserve_cleared(&mut self.placements, old_cap as usize + 1)?;
        reserve_cleared(&mut self.occupied, grown_cap as usize)
    }

    /// Reserve the ranges buffer to hold `total` entries, counting pushes this
    /// plan already made.
    fn admit_ranges(&mut self, total: usize) -> Result<(), ErrorCode> {
        self.ranges
            .try_reserve_exact(total.saturating_sub(self.ranges.len()))
            .map_err(|_| ErrorCode::InvalidState)
    }
}

/// Clear `buf` and reserve exactly `cap` elements. Capacity an earlier plan
/// paid for is kept, so a reservation the buffer already covers is free.
fn reserve_cleared<T>(buf: &mut Vec<T>, cap: usize) -> Result<(), ErrorCode> {
    buf.clear();
    buf.try_reserve_exact(cap)
        .map_err(|_| ErrorCode::InvalidState)
}

/// Append into capacity the plan admitted before it pushed. A full buffer
/// means an admission bound is wrong; growing silently would hide that.
fn push_admitted<T>(buf: &mut Vec<T>, item: T) {
    // invariant: admission reserved every bound a plan can reach.
    if buf.len() == buf.capacity() {
        columine_types::die!("nested plan outgrew its admitted scratch bound");
    }
    buf.push(item);
}

#[derive(Debug)]
struct Placement {
    pos: u32,
    key: u32,
    value: Option<u32>,
}

#[derive(Clone, Copy)]
enum Initialization {
    Table {
        offset: u32,
        capacity: u32,
        entry_size: u32,
    },
    Aggregate {
        offset: u32,
        len: u32,
    },
}

enum InnerMutation {
    Cell {
        table: hash_table::FlatTable,
        pos: u32,
        key: u32,
        value: Option<u32>,
        is_new: bool,
    },
    Rehash {
        table: hash_table::FlatTable,
    },
    Aggregate {
        offset: u32,
        kind: AggType,
        value_bits: u64,
    },
}

/// A mutation retains the original image and its paid probe/rehash plan.
/// The image cannot change between capture preflight and consuming apply.
pub struct NestedMutation<'state, 'work> {
    state: &'state mut [u8],
    scratch: &'work mut NestedScratch,
    meta: SlotMetaView,
    outer: OuterTable,
    outer_pos: u32,
    outer_key: u32,
    outer_new: bool,
    inner_offset: u32,
    next_used: Option<u32>,
    initializations: [Option<Initialization>; 2],
    mutation: InnerMutation,
    flags: u8,
}

impl<'state, 'work> NestedMutation<'state, 'work> {
    pub fn state(&self) -> &[u8] {
        self.state
    }

    pub fn ranges(&self) -> &[(u32, u32)] {
        &self.scratch.ranges
    }

    fn push_range(&mut self, offset: u32, len: u32) {
        if len != 0 {
            push_admitted(&mut self.scratch.ranges, (offset, len));
        }
    }

    fn allocate(&mut self, len: u32) -> Result<u32, ErrorCode> {
        let used = self
            .next_used
            .unwrap_or_else(|| self.outer.arena.used(self.state));
        let aligned = len.checked_add(7).ok_or(ErrorCode::CapacityExceeded)? & !7;
        let next_used = used
            .checked_add(aligned)
            .ok_or(ErrorCode::CapacityExceeded)?;
        if next_used > self.outer.arena.capacity(self.state) {
            return Err(ErrorCode::CapacityExceeded);
        }
        let offset = self
            .outer
            .arena
            .data_offset
            .checked_add(used)
            .ok_or(ErrorCode::InvalidState)?;
        if offset
            .checked_add(len)
            .is_none_or(|end| end as usize > self.state.len())
        {
            return Err(ErrorCode::InvalidState);
        }
        self.next_used = Some(next_used);
        Ok(offset)
    }

    fn initialize(&mut self, initialization: Initialization) {
        let entry = self
            .initializations
            .iter_mut()
            .find(|entry| entry.is_none())
            .unwrap_or_else(|| columine_types::die!("at most two nested table initializations"));
        *entry = Some(initialization);
        match initialization {
            Initialization::Table {
                offset, capacity, ..
            } => {
                self.push_range(offset, hash_table::HDR_BYTES + capacity * 4);
            }
            Initialization::Aggregate { offset, len } => self.push_range(offset, len),
        }
    }

    /// Apply only the cells retained by the planner; no probe or rehash repeats.
    pub fn apply(self) -> (&'state mut [u8], &'work [(u32, u32)]) {
        let state = self.state;
        for initialization in self.initializations.into_iter().flatten() {
            match initialization {
                Initialization::Table {
                    offset,
                    capacity,
                    entry_size,
                } => {
                    hash_table::FlatTable::init(state, offset, capacity, entry_size);
                }
                Initialization::Aggregate { offset, len } => bytes::zero(state, offset, len),
            }
        }
        if let Some(used) = self.next_used {
            self.outer.arena.set_used(state, used);
            bytes::write_u32(
                state,
                self.outer.ptrs_off + self.outer_pos * 4,
                self.inner_offset,
            );
        }
        if self.outer_new {
            bytes::write_u32(
                state,
                self.outer.keys_off + self.outer_pos * 4,
                self.outer_key,
            );
            let size = self.outer.size(state);
            bytes::write_u32(state, self.outer.size_off, size + 1);
        }
        match self.mutation {
            InnerMutation::Cell {
                table,
                pos,
                key,
                value,
                is_new,
            } => {
                if is_new {
                    table.set_key_at(state, pos, key);
                    let size = table.size(state);
                    table.set_size(state, size + 1);
                }
                if let Some(value) = value {
                    table.set_entry_u32_at(state, pos, value);
                }
            }
            InnerMutation::Rehash { table } => {
                for placement in &self.scratch.placements {
                    table.set_key_at(state, placement.pos, placement.key);
                    if let Some(value) = placement.value {
                        table.set_entry_u32_at(state, placement.pos, value);
                    }
                }
                table.set_size(state, self.scratch.placements.len() as u32);
            }
            InnerMutation::Aggregate {
                offset,
                kind,
                value_bits,
            } => match kind {
                AggType::Count => {
                    let count = bytes::read_u64(state, offset);
                    bytes::write_u64(state, offset, count + 1);
                }
                AggType::Sum => {
                    let value = bytes::read_f64(state, offset) + f64::from_bits(value_bits);
                    let count = bytes::read_u64(state, offset + 8);
                    bytes::write_f64(state, offset, value);
                    bytes::write_u64(state, offset + 8, count + 1);
                }
                AggType::Min | AggType::Max => {
                    let count = bytes::read_u64(state, offset + 8);
                    let current = if count == 0 {
                        if kind == AggType::Min {
                            f64::INFINITY
                        } else {
                            f64::NEG_INFINITY
                        }
                    } else {
                        bytes::read_f64(state, offset)
                    };
                    let value = f64::from_bits(value_bits);
                    let next = if kind == AggType::Min {
                        aggregates::min_profile(current, value)
                    } else {
                        aggregates::max_profile(current, value)
                    };
                    bytes::write_f64(state, offset, next);
                    bytes::write_u64(state, offset + 8, count + 1);
                }
                _ => columine_types::die!("unsupported nested aggregate passed preparation"),
            },
        }
        if self.flags != 0 {
            self.meta.set_change_flag(state, self.flags);
        }
        (state, &self.scratch.ranges)
    }
}

fn prepare_outer<'state, 'work>(
    state: &'state mut [u8],
    meta: &SlotMetaView,
    outer_key: u32,
    scratch: &'work mut NestedScratch,
    inner_type: SlotType,
) -> Result<NestedMutation<'state, 'work>, ErrorCode> {
    scratch.begin_plan()?;
    let prefix = read_nested_prefix(state, meta.offset);
    if prefix.inner_type != inner_type {
        return Err(ErrorCode::InvalidProgram);
    }
    let outer = OuterTable::bind(meta);
    if outer_key >= TOMBSTONE {
        return Err(ErrorCode::CapacityExceeded);
    }
    let probe =
        hash_table::probe_linear(
            outer.cap,
            hash_key(outer_key, outer.cap),
            true,
            |pos| match outer.key_at(state, pos) {
                key if key == outer_key => hash_table::ProbeCell::Match,
                EMPTY_KEY => hash_table::ProbeCell::Empty,
                TOMBSTONE => hash_table::ProbeCell::Tombstone,
                _ => hash_table::ProbeCell::Occupied,
            },
        )
        .ok_or(ErrorCode::CapacityExceeded)?;
    if !probe.found && outer.size(state) >= hash_table::max_load_for_cap(outer.cap) {
        return Err(ErrorCode::CapacityExceeded);
    }
    let mut plan = NestedMutation {
        inner_offset: if probe.found {
            outer.ptr_at(state, probe.pos)
        } else {
            0
        },
        state,
        scratch,
        meta: *meta,
        outer,
        outer_pos: probe.pos,
        outer_key,
        outer_new: !probe.found,
        next_used: None,
        initializations: [None; 2],
        mutation: InnerMutation::Aggregate {
            offset: 0,
            kind: AggType::Count,
            value_bits: 0,
        },
        flags: if probe.found { 0 } else { ChangeFlag::INSERTED },
    };
    if !probe.found {
        let capacity = next_power_of_2(u32::from(prefix.inner_initial_cap));
        let len = inner_container_size(inner_type, capacity, prefix.inner_agg_type_byte);
        let offset = plan.allocate(len)?;
        plan.inner_offset = offset;
        plan.push_range(outer.keys_off + probe.pos * 4, 4);
        plan.push_range(outer.size_off, 4);
        if inner_type == SlotType::Aggregate {
            plan.initialize(Initialization::Aggregate { offset, len });
        } else {
            plan.initialize(Initialization::Table {
                offset,
                capacity,
                entry_size: if inner_type == SlotType::HashMap {
                    hash_table::ENTRY_U32
                } else {
                    hash_table::ENTRY_NONE
                },
            });
        }
    }
    Ok(plan)
}

fn finish_plan(plan: &mut NestedMutation<'_, '_>) {
    if plan.next_used.is_some() {
        plan.push_range(plan.outer.arena.hdr_offset + ARENA_HDR_USED, 4);
        plan.push_range(plan.outer.ptrs_off + plan.outer_pos * 4, 4);
    }
    if plan.flags != 0 {
        plan.push_range(
            plan.meta.meta_base + columine_types::types::SlotMetaOffset::CHANGE_FLAGS,
            1,
        );
    }
}

fn prepare_table<'state, 'work>(
    state: &'state mut [u8],
    meta: &SlotMetaView,
    outer_key: u32,
    key: u32,
    value: Option<u32>,
    scratch: &'work mut NestedScratch,
) -> Result<NestedMutation<'state, 'work>, ErrorCode> {
    if key >= TOMBSTONE {
        return Err(ErrorCode::CapacityExceeded);
    }
    let inner_type = if value.is_some() {
        SlotType::HashMap
    } else {
        SlotType::HashSet
    };
    let mut plan = prepare_outer(state, meta, outer_key, scratch, inner_type)?;
    let entry_size = if value.is_some() {
        hash_table::ENTRY_U32
    } else {
        hash_table::ENTRY_NONE
    };
    let table = if plan.outer_new {
        let capacity = next_power_of_2(u32::from(
            read_nested_prefix(plan.state, meta.offset).inner_initial_cap,
        ));
        hash_table::FlatTable::bind_external(
            plan.inner_offset + hash_table::HDR_BYTES,
            capacity,
            plan.inner_offset + 4,
            entry_size,
        )
    } else {
        hash_table::FlatTable::bind(plan.state, plan.inner_offset, entry_size)
    };
    let probe = if plan.outer_new {
        hash_table::Probe {
            pos: hash_key(key, table.cap),
            found: false,
        }
    } else {
        table
            .find_insert(plan.state, key)
            .ok_or(ErrorCode::CapacityExceeded)?
    };
    let size = if plan.outer_new {
        0
    } else {
        table.size(plan.state)
    };
    if probe.found || size < table.max_load() {
        plan.mutation = InnerMutation::Cell {
            table,
            pos: probe.pos,
            key,
            value,
            is_new: !probe.found,
        };
        if !probe.found {
            if !plan.outer_new {
                plan.push_range(table.keys_off + probe.pos * 4, 4);
                plan.push_range(table.size_off, 4);
            }
            plan.flags |= ChangeFlag::INSERTED;
        } else if value.is_some() {
            plan.flags |= ChangeFlag::UPDATED;
        }
        if value.is_some() {
            plan.push_range(table.entries_off + probe.pos * 4, 4);
        }
        finish_plan(&mut plan);
        return Ok(plan);
    }

    let capacity = table
        .cap
        .checked_mul(2)
        .ok_or(ErrorCode::CapacityExceeded)?;
    let len = capacity
        .checked_mul(4 + entry_size)
        .and_then(|len| len.checked_add(hash_table::HDR_BYTES))
        .ok_or(ErrorCode::CapacityExceeded)?;
    let offset = plan.allocate(len)?;
    plan.inner_offset = offset;
    plan.initialize(Initialization::Table {
        offset,
        capacity,
        entry_size,
    });
    let grown = hash_table::FlatTable::bind_external(
        offset + hash_table::HDR_BYTES,
        capacity,
        offset + 4,
        entry_size,
    );
    plan.scratch.admit_rehash(table.cap, capacity)?;
    plan.scratch.occupied.resize(capacity as usize, false);
    if !plan.outer_new {
        for pos in 0..table.cap {
            let old_key = table.key_at(plan.state, pos);
            if old_key < TOMBSTONE {
                let mut destination = hash_key(old_key, capacity);
                while plan.scratch.occupied[destination as usize] {
                    destination = (destination + 1) & (capacity - 1);
                }
                plan.scratch.occupied[destination as usize] = true;
                push_admitted(
                    &mut plan.scratch.placements,
                    Placement {
                        pos: destination,
                        key: old_key,
                        value: value.map(|_| table.entry_u32_at(plan.state, pos)),
                    },
                );
            }
        }
    }
    let mut destination = hash_key(key, capacity);
    while plan.scratch.occupied[destination as usize] {
        destination = (destination + 1) & (capacity - 1);
    }
    push_admitted(
        &mut plan.scratch.placements,
        Placement {
            pos: destination,
            key,
            value,
        },
    );
    plan.mutation = InnerMutation::Rehash { table: grown };
    plan.flags |= ChangeFlag::INSERTED;
    finish_plan(&mut plan);
    if value.is_some() {
        plan.scratch
            .placements
            .sort_unstable_by_key(|placement| placement.pos);
        let mut start = None;
        let mut end = 0;
        for placement in &plan.scratch.placements {
            match start {
                None => start = Some(placement.pos),
                Some(_) if placement.pos == end => {}
                Some(first) => {
                    push_admitted(
                        &mut plan.scratch.ranges,
                        (grown.entries_off + first * 4, (end - first) * 4),
                    );
                    start = Some(placement.pos);
                }
            }
            end = placement.pos + 1;
        }
        if let Some(first) = start {
            push_admitted(
                &mut plan.scratch.ranges,
                (grown.entries_off + first * 4, (end - first) * 4),
            );
        }
    }
    Ok(plan)
}

pub fn prepare_nested_set_insert<'state, 'work>(
    state: &'state mut [u8],
    meta: &SlotMetaView,
    outer_key: u32,
    elem: u32,
    scratch: &'work mut NestedScratch,
) -> Result<NestedMutation<'state, 'work>, ErrorCode> {
    prepare_table(state, meta, outer_key, elem, None, scratch)
}

pub fn prepare_nested_map_upsert_last<'state, 'work>(
    state: &'state mut [u8],
    meta: &SlotMetaView,
    outer_key: u32,
    inner_key: u32,
    value: u32,
    scratch: &'work mut NestedScratch,
) -> Result<NestedMutation<'state, 'work>, ErrorCode> {
    prepare_table(state, meta, outer_key, inner_key, Some(value), scratch)
}

pub fn prepare_nested_agg_update<'state, 'work>(
    state: &'state mut [u8],
    meta: &SlotMetaView,
    outer_key: u32,
    value_bits: u64,
    scratch: &'work mut NestedScratch,
) -> Result<NestedMutation<'state, 'work>, ErrorCode> {
    let prefix = read_nested_prefix(state, meta.offset);
    let kind = match AggType::from_u8(prefix.inner_agg_type_byte) {
        Some(kind @ (AggType::Count | AggType::Sum | AggType::Min | AggType::Max)) => kind,
        _ => return Err(ErrorCode::InvalidProgram),
    };
    let mut plan = prepare_outer(state, meta, outer_key, scratch, SlotType::Aggregate)?;
    let len = aggregates::agg_slot_byte_size(prefix.inner_agg_type_byte);
    plan.mutation = InnerMutation::Aggregate {
        offset: plan.inner_offset,
        kind,
        value_bits,
    };
    if !plan.outer_new {
        plan.push_range(plan.inner_offset, len);
    }
    plan.flags |= ChangeFlag::SIZE_CHANGED;
    finish_plan(&mut plan);
    Ok(plan)
}

/// Insert into an outer-keyed inner set without journaling, planning in the
/// caller's retained `scratch`.
pub fn nested_set_insert(
    state: &mut [u8],
    meta: &SlotMetaView,
    outer_key: u32,
    elem: u32,
    scratch: &mut NestedScratch,
) -> ErrorCode {
    match prepare_nested_set_insert(state, meta, outer_key, elem, scratch) {
        Ok(plan) => {
            plan.apply();
            ErrorCode::Ok
        }
        Err(error) => error,
    }
}

/// Upsert an outer-keyed inner map value without journaling, planning in the
/// caller's retained `scratch`.
pub fn nested_map_upsert_last(
    state: &mut [u8],
    meta: &SlotMetaView,
    outer_key: u32,
    inner_key: u32,
    value: u32,
    scratch: &mut NestedScratch,
) -> ErrorCode {
    match prepare_nested_map_upsert_last(state, meta, outer_key, inner_key, value, scratch) {
        Ok(plan) => {
            plan.apply();
            ErrorCode::Ok
        }
        Err(error) => error,
    }
}

/// Update an outer-keyed inner aggregate without journaling, planning in the
/// caller's retained `scratch`.
pub fn nested_agg_update(
    state: &mut [u8],
    meta: &SlotMetaView,
    outer_key: u32,
    value_bits: u64,
    scratch: &mut NestedScratch,
) -> ErrorCode {
    match prepare_nested_agg_update(state, meta, outer_key, value_bits, scratch) {
        Ok(plan) => {
            plan.apply();
            ErrorCode::Ok
        }
        Err(error) => error,
    }
}

// =============================================================================
// Nested-container read operations

/// Return zero when the outer key is absent.
pub fn get_inner_offset(state: &[u8], meta: &SlotMetaView, outer_key: u32) -> u32 {
    OuterTable::bind(meta).lookup(state, outer_key)
}

/// Return the size of an inner set, or zero when absent.
pub fn get_inner_set_size(state: &[u8], inner_offset: u32) -> u32 {
    if inner_offset == 0 {
        return 0;
    }
    hash_table::FlatTable::bind(state, inner_offset, hash_table::ENTRY_NONE).size(state)
}

/// Test membership in an inner set.
pub fn inner_set_contains(state: &[u8], inner_offset: u32, elem: u32) -> bool {
    if inner_offset == 0 {
        return false;
    }
    hash_table::FlatTable::bind(state, inner_offset, hash_table::ENTRY_NONE).contains(state, elem)
}

/// Read an inner-map value, returning `EMPTY_KEY` when absent.
pub fn inner_map_get(state: &[u8], inner_offset: u32, key: u32) -> u32 {
    if inner_offset == 0 {
        return EMPTY_KEY;
    }
    hash_table::FlatTable::bind(state, inner_offset, hash_table::ENTRY_U32)
        .get_u32(state, key)
        .unwrap_or(EMPTY_KEY)
}

/// Read an inner aggregate f64 value, returning zero when absent.
pub fn inner_agg_get_f64(state: &[u8], inner_offset: u32) -> f64 {
    if inner_offset == 0 {
        return 0.0;
    }
    bytes::read_f64(state, inner_offset)
}

/// Read an inner aggregate count: offset zero for COUNT, offset eight otherwise.
pub fn inner_agg_get_count(state: &[u8], inner_offset: u32, agg_type_byte: u8) -> u64 {
    if inner_offset == 0 {
        return 0;
    }
    let count_off = if agg_type_byte == AggType::Count as u8 {
        0
    } else {
        8
    };
    bytes::read_u64(state, inner_offset + count_off)
}
