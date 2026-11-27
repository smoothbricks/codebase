//! Nested container slots: arena-allocated inner maps, sets, aggregates, and
//! ordered struct lists.
//!
//! `Map<K, Set<V>>`, `Map<K, Map<K2, V>>`, `Map<K, Agg>`, and
//! `Map<K, List<StructRow>>` use an outer open-addressed hash table whose
//! values are absolute byte offsets into a slot-owned bump arena. Inner
//! containers are initialized on first sight of an outer key and grow by
//! allocating a doubled container in the arena, copying live contents, and
//! re-pointing the outer entry.
//!
//! # Image Byte Layout (Little-Endian)
//!
//! Embedders reading the state image directly must match this layout:
//!
//! - **Slot metadata** (`STATE_HEADER_SIZE + slot * SLOT_META_SIZE`):
//!   `OFFSET` (`+0`, `u32`) is `slot_offset`; `CAPACITY` (`+4`, `u32`) is
//!   `outer_cap` (power of two); `SIZE` (`+8`, `u32`) is the outer live key
//!   count; `TYPE_FLAGS` (`+12`, `u8`) has low nibble `SlotType::Nested` (`9`).
//! - **Prefix** (`slot_offset .. slot_offset + 8`, [`NESTED_PREFIX_SIZE`]):
//!   - `+0` `inner_type: u8` — low nibble is `SlotType` (`0` HashMap, `1` HashSet,
//!     `2` Aggregate, `7` OrderedList).
//!   - `+1..3` `inner_initial_cap: u16` — declared initial inner capacity
//!     (default `16` when `0` in bytecode).
//!   - `+3` `inner_agg_type: u8` — raw `AggType` for `Aggregate`, `0` for `OrderedList`.
//!   - `+4` `depth: u8` — nesting depth (`1`).
//!   - `+5` `num_fields: u8` — field count for `OrderedList` (`1..=255`), `0` otherwise.
//!   - `+6..8` `row_size: u16` — 4-byte-padded struct row width for `OrderedList`, `0` otherwise.
//! - **Field descriptor** (`slot_offset + 8 .. slot_offset + 8 + align8(num_fields)`):
//!   present only when `num_fields > 0` (`OrderedList`); `num_fields` bytes of
//!   [`columine_types::types::StructFieldType`] followed by zero padding to an
//!   8-byte boundary. Zero bytes for `HashMap`, `HashSet`, and `Aggregate`.
//! - **Outer key table** (`outer_keys_off = slot_offset + 8 + align8(num_fields)`):
//!   `outer_cap` little-endian `u32` keys (`0xffff_ffff` empty, `0xffff_fffe`
//!   tombstone), probed linearly starting at `hash_key(outer_key, outer_cap)`.
//! - **Outer pointer table** (`outer_ptrs_off = outer_keys_off + outer_cap * 4`):
//!   `outer_cap` little-endian `u32` absolute byte offsets into the state image
//!   (`0` when absent).
//! - **Arena header** (`arena_hdr_off = outer_ptrs_off + outer_cap * 4`):
//!   `[arena_capacity: u32][arena_used: u32]` ([`ARENA_HDR_SIZE`] = 8).
//! - **Arena data** (`arena_data_off = arena_hdr_off + 8`):
//!   8-byte-aligned bump allocations.
//! - **Inner `OrderedList` container** (at `ptr` in arena data):
//!   `[count: u32 @ +0][capacity: u32 @ +4][rows: row_size × capacity @ +8]`
//!   ([`LIST_HDR_BYTES`] = 8). Row `i` (`0 <= i < count`) starts at
//!   `ptr + 8 + i * row_size` and is byte-identical to a top-level
//!   `ORDERED_LIST` struct row:
//!   `[bitset: ceil(num_fields / 8) bytes][field_0][field_1]...` padded to a
//!   4-byte multiple. Bit `f` (`bitset[f / 8] & (1 << (f % 8))`) is 1 iff
//!   field `f` was provided; `UInt32` and interned `String` are 4-byte `u32`,
//!   `Int64` is 8-byte `i64`, `Float64` is 8-byte `f64`, and `Bool` is 1 byte.
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

use crate::meta::{SlotMetaView, slot_meta_base};
use crate::{aggregates, bytes, hash_table};
use columine_types::types::{
    AggType, ChangeFlag, EMPTY_KEY, ErrorCode, SLOT_META_SIZE, STATE_HEADER_SIZE, SlotMetaOffset,
    SlotType, SlotTypeFlags, StructFieldType, StructRowLayout, TOMBSTONE, align8, hash_key,
    is_arena_field_type, next_power_of_2, state_num_slots,
};

/// Prefix stored at the start of a nested slot's data:
/// `[inner_type:u8][inner_initial_cap:u16 le][inner_agg_type:u8][depth:u8][num_fields:u8][row_size:u16 le]`.
pub const NESTED_PREFIX_SIZE: u32 = 8;

/// Arena header: `[capacity:u32][used:u32]`.
pub const ARENA_HDR_SIZE: u32 = 8;

/// Inner ordered-list container header: `[count:u32][capacity:u32]`.
pub const LIST_HDR_BYTES: u32 = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NestedPrefix {
    pub inner_type: SlotType,
    pub inner_initial_cap: u16,
    /// Raw AggType byte (see `aggregates` on why raw bytes are the contract).
    /// Must be `0` when `inner_type == SlotType::OrderedList`.
    pub inner_agg_type_byte: u8,
    pub depth: u8,
    /// Number of struct fields for `SlotType::OrderedList`; `0` for other inner kinds.
    pub num_fields: u8,
    /// Four-byte-padded struct row size for `SlotType::OrderedList`; `0` for other inner kinds.
    pub row_size: u16,
}

impl NestedPrefix {
    /// Byte length of the 8-byte-aligned field-type descriptor following the prefix.
    /// Zero for inner kinds without a field descriptor.
    pub const fn descriptor_size(self) -> u32 {
        align8(self.num_fields as u32)
    }

    /// Byte offset of the field-type descriptor within the state image.
    pub const fn descriptor_offset(slot_offset: u32) -> u32 {
        slot_offset + NESTED_PREFIX_SIZE
    }

    /// Byte width of one presence bitset in an inner struct row.
    pub const fn bitset_bytes(self) -> u32 {
        (self.num_fields as u32).div_ceil(8)
    }
}

/// Write a nested-slot prefix without journaling.
pub fn write_nested_prefix(state: &mut [u8], slot_offset: u32, prefix: NestedPrefix) {
    let base = slot_offset as usize;
    state[base] = prefix.inner_type as u8;
    state[base + 1] = (prefix.inner_initial_cap & 0xff) as u8;
    state[base + 2] = (prefix.inner_initial_cap >> 8) as u8;
    state[base + 3] = prefix.inner_agg_type_byte;
    state[base + 4] = prefix.depth;
    state[base + 5] = prefix.num_fields;
    let [rs_lo, rs_hi] = prefix.row_size.to_le_bytes();
    state[base + 6] = rs_lo;
    state[base + 7] = rs_hi;
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
        num_fields: state[base + 5],
        row_size: u16::from_le_bytes([state[base + 6], state[base + 7]]),
    }
}

/// Initial capacity of one inner container for `inner_type`.
/// Hash tables normalize to a power of two (minimum 16); ordered lists use the
/// exact declared `inner_initial_cap` (defaulting `0` to `16`).
pub const fn effective_inner_cap(inner_type: SlotType, inner_initial_cap: u16) -> u32 {
    let cap = inner_initial_cap as u32;
    match inner_type {
        SlotType::OrderedList => {
            if cap == 0 {
                16
            } else {
                cap
            }
        }
        _ => next_power_of_2(cap),
    }
}

/// Checked byte size of one inner `OrderedList` container (`[count][capacity][rows]`).
pub const fn inner_list_byte_size(capacity: u32, row_size: u16) -> Option<u32> {
    match capacity.checked_mul(row_size as u32) {
        Some(rows) => rows.checked_add(LIST_HDR_BYTES),
        None => None,
    }
}

/// Offset of the outer key table, after the prefix and optional aligned descriptor.
pub const fn outer_keys_offset(slot_offset: u32, descriptor_size: u32) -> u32 {
    slot_offset + NESTED_PREFIX_SIZE + descriptor_size
}

/// Offset of the outer pointer table.
pub const fn outer_ptrs_offset(slot_offset: u32, descriptor_size: u32, capacity: u32) -> u32 {
    outer_keys_offset(slot_offset, descriptor_size) + capacity * 4
}

/// Offset of the arena header.
pub const fn arena_header_offset(slot_offset: u32, descriptor_size: u32, capacity: u32) -> u32 {
    outer_ptrs_offset(slot_offset, descriptor_size, capacity) + capacity * 4
}

/// Offset of arena data.
pub const fn arena_data_offset(slot_offset: u32, descriptor_size: u32, capacity: u32) -> u32 {
    arena_header_offset(slot_offset, descriptor_size, capacity) + ARENA_HDR_SIZE
}

/// Compute inline-header size for an inner container.
pub fn inner_container_size(
    inner_type: SlotType,
    capacity: u32,
    inner_agg_type_byte: u8,
    row_size: u16,
) -> u32 {
    match inner_type {
        SlotType::HashMap => hash_table::hashmap_byte_size(capacity),
        SlotType::HashSet => hash_table::hashset_byte_size(capacity),
        SlotType::Aggregate => aggregates::agg_slot_byte_size(inner_agg_type_byte),
        SlotType::OrderedList => LIST_HDR_BYTES + capacity * u32::from(row_size),
        _ => 0,
    }
}

/// Check the inner container at `pointer` lies whole in an arena whose data
/// starts at `data` and whose used bytes are `used`: a table's capacity is a
/// power of two, its size within its load; an ordered list's capacity is
/// nonzero with `count <= capacity`; and its whole extent (header, keys and
/// entries, rows, or an aggregate's cells) lies inside the used bytes. A
/// container that reached past them would let an insert probe or write into
/// another slot's storage.
pub(crate) fn check_inner(
    state: &[u8],
    data: u32,
    used: u32,
    pointer: u32,
    prefix: &NestedPrefix,
) -> Result<(), ErrorCode> {
    let invalid = ErrorCode::InvalidState;
    let relative = pointer.checked_sub(data).ok_or(invalid)?;
    let room = used.checked_sub(relative).ok_or(invalid)?;
    let extent = match prefix.inner_type {
        SlotType::HashMap | SlotType::HashSet => {
            if room < hash_table::HDR_BYTES {
                return Err(invalid);
            }
            let cap = bytes::read_u32(state, pointer);
            let size = bytes::read_u32(state, pointer.checked_add(4).ok_or(invalid)?);
            if cap == 0 || !cap.is_power_of_two() || size > hash_table::max_load_for_cap(cap) {
                return Err(invalid);
            }
            let entry = if prefix.inner_type == SlotType::HashMap {
                hash_table::ENTRY_U32
            } else {
                hash_table::ENTRY_NONE
            };
            u64::from(hash_table::HDR_BYTES) + u64::from(cap) * u64::from(4 + entry)
        }
        SlotType::Aggregate => {
            u64::from(aggregates::agg_slot_byte_size(prefix.inner_agg_type_byte))
        }
        SlotType::OrderedList => {
            if room < LIST_HDR_BYTES || prefix.num_fields == 0 || prefix.row_size == 0 {
                return Err(invalid);
            }
            let count = bytes::read_u32(state, pointer);
            let cap = bytes::read_u32(state, pointer.checked_add(4).ok_or(invalid)?);
            if cap == 0 || count > cap {
                return Err(invalid);
            }
            u64::from(LIST_HDR_BYTES) + u64::from(cap) * u64::from(prefix.row_size)
        }
        _ => return Err(invalid),
    };
    if extent > u64::from(room) {
        return Err(invalid);
    }
    Ok(())
}

/// Validate the prefix and field-type descriptor of a nested `OrderedList`
/// slot and return its computed struct row layout.
///
/// Shared between state validation (`state_init::validate_state`) and embedder
/// lookups (`nested_list_lookup`): requires `inner_initial_cap >= 1`,
/// `inner_agg_type == 0`, `num_fields >= 1`, every descriptor byte to be a
/// valid non-arena [`StructFieldType`], and `row_size` to equal
/// `compute_struct_row_layout_padded(num_fields, descriptor).row_size`.
pub(crate) fn check_ordered_list_prefix(
    state: &[u8],
    slot_offset: u32,
    prefix: &NestedPrefix,
) -> Result<StructRowLayout, ErrorCode> {
    if prefix.inner_type != SlotType::OrderedList
        || prefix.inner_initial_cap == 0
        || prefix.inner_agg_type_byte != 0
        || prefix.num_fields == 0
    {
        return Err(ErrorCode::InvalidState);
    }
    let desc_start = NestedPrefix::descriptor_offset(slot_offset);
    let desc_end = desc_start
        .checked_add(u32::from(prefix.num_fields))
        .ok_or(ErrorCode::InvalidState)?;
    let descriptor = state
        .get(desc_start as usize..desc_end as usize)
        .ok_or(ErrorCode::InvalidState)?;
    if descriptor
        .iter()
        .any(|&ft| StructFieldType::from_u8(ft).is_none_or(is_arena_field_type))
    {
        return Err(ErrorCode::InvalidState);
    }
    let layout = crate::state_init::compute_struct_row_layout_padded(prefix.num_fields, descriptor);
    if u32::from(prefix.row_size) != layout.row_size {
        return Err(ErrorCode::InvalidState);
    }
    Ok(layout)
}

/// Compute nested slot data size: prefix + aligned field descriptor + outer
/// keys + outer pointers + arena header + pre-sized inner containers, each
/// rounded to eight-byte alignment.
pub fn nested_slot_data_size(
    outer_cap: u32,
    inner_initial_cap: u32,
    inner_type: SlotType,
    inner_agg_type_byte: u8,
    num_fields: u8,
    row_size: u16,
) -> u32 {
    let descriptor_size = align8(u32::from(num_fields));
    let per_inner = align8(inner_container_size(
        inner_type,
        inner_initial_cap,
        inner_agg_type_byte,
        row_size,
    ));
    NESTED_PREFIX_SIZE
        + descriptor_size
        + outer_cap * 4
        + outer_cap * 4
        + ARENA_HDR_SIZE
        + outer_cap * per_inner
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
    /// Bind to a nested slot's outer table, reading the prefix to skip any
    /// field-type descriptor.
    pub fn bind(state: &[u8], meta: &SlotMetaView) -> Self {
        let prefix = read_nested_prefix(state, meta.offset);
        Self::bind_with_prefix(meta, &prefix)
    }

    /// Bind to a nested slot's outer table from an already-read prefix.
    pub const fn bind_with_prefix(meta: &SlotMetaView, prefix: &NestedPrefix) -> Self {
        let slot_off = meta.offset;
        let cap = meta.capacity;
        let desc = prefix.descriptor_size();
        Self {
            cap,
            size_off: meta.meta_base + columine_types::types::SlotMetaOffset::SIZE,
            keys_off: outer_keys_offset(slot_off, desc),
            ptrs_off: outer_ptrs_offset(slot_off, desc, cap),
            arena: Arena::bind(arena_header_offset(slot_off, desc, cap)),
        }
    }

    fn key_at(&self, state: &[u8], pos: u32) -> u32 {
        bytes::read_u32(state, self.keys_off + pos * 4)
    }

    fn ptr_at(&self, state: &[u8], pos: u32) -> u32 {
        bytes::read_u32(state, self.ptrs_off + pos * 4)
    }

    /// The inner container a live outer cell points to, checked whole: a
    /// batch checks a host image's layout, not every outer pointer, so a
    /// kernel checks each pointer it follows (see [`check_inner`]).
    fn inner_at(&self, state: &[u8], pos: u32, prefix: &NestedPrefix) -> Result<u32, ErrorCode> {
        let pointer = self.ptr_at(state, pos);
        check_inner(
            state,
            self.arena.data_offset,
            self.arena.used(state),
            pointer,
            prefix,
        )?;
        Ok(pointer)
    }

    /// [`Self::lookup`] with the inner container checked as
    /// [`Self::inner_at`] does.
    fn lookup_checked(
        &self,
        state: &[u8],
        outer_key: u32,
        prefix: &NestedPrefix,
    ) -> Result<u32, ErrorCode> {
        match hash_table::find_key(state, self.keys_off, self.cap, outer_key) {
            Some(pos) => self.inner_at(state, pos, prefix),
            None => Ok(0),
        }
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
/// reserves the capacity its own path can push before it writes, unless the
/// batch already backed its largest plan with `reserve_for`. A warm plan that
/// fits that mark allocates nothing.
#[derive(Debug, Default)]
pub struct NestedScratch {
    /// Disjoint byte ranges the current plan may overwrite.
    ranges: Vec<(u32, u32)>,
    /// Rehash destinations: one per live inner key plus the inserted key.
    placements: Vec<Placement>,
    /// Occupancy of the grown inner table's cells while destinations are placed.
    occupied: Vec<bool>,
    /// Debug only: buffer capacities when a batch's reservation was sealed.
    /// No plan of the batch may grow past them until `unseal`.
    #[cfg(debug_assertions)]
    sealed: Option<[usize; 3]>,
}

/// Most ranges a plan that does not rehash can push: an outer insert pushes its
/// key, size, inner header, entry, arena usage, outer pointer, change flag and
/// write sequence.
const PLAIN_PLAN_RANGES: usize = 8;

/// The largest inner-table rehash one batch or call can plan: from
/// `old_cap` cells into `grown_cap` cells. Zero when no rehash is possible.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct NestedPlanDemand {
    /// Cells in the inner table before the rehash.
    pub(crate) old_cap: u32,
    /// Cells in the inner table after the rehash.
    pub(crate) grown_cap: u32,
}

impl NestedPlanDemand {
    /// Raise each field to the larger of the two demands, so one value bounds
    /// every rehash folded into it.
    pub(crate) fn widen(&mut self, other: Self) {
        self.old_cap = self.old_cap.max(other.old_cap);
        self.grown_cap = self.grown_cap.max(other.grown_cap);
    }
}

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
        self.admit_ranges(PLAIN_PLAN_RANGES)?;
        self.check_sealed();
        Ok(())
    }

    /// Admit a rehash from `old_cap` cells into `grown_cap` cells. The whole
    /// plan pushes at most `old_cap + 9` ranges: four before this point, four
    /// finishing ranges, and one run per destination, of which there are at
    /// most `old_cap + 1`.
    fn admit_rehash(&mut self, old_cap: u32, grown_cap: u32) -> Result<(), ErrorCode> {
        self.admit_ranges(old_cap as usize + 9)?;
        reserve_cleared(&mut self.placements, old_cap as usize + 1)?;
        reserve_cleared(&mut self.occupied, grown_cap as usize)?;
        self.check_sealed();
        Ok(())
    }

    /// Reserve the ranges buffer to hold `total` entries, counting pushes this
    /// plan already made.
    fn admit_ranges(&mut self, total: usize) -> Result<(), ErrorCode> {
        self.ranges
            .try_reserve_exact(total.saturating_sub(self.ranges.len()))
            .map_err(|_| ErrorCode::OutOfMemory)
    }

    /// Back the largest plan a batch can make before the batch's first write:
    /// a plan without a rehash, or the largest rehash `demand` names. Buffers
    /// are cleared first. A failure keeps the capacity already obtained, so a
    /// later reservation starts from it.
    pub(crate) fn reserve_for(&mut self, demand: NestedPlanDemand) -> Result<(), ErrorCode> {
        let old = usize::try_from(demand.old_cap).map_err(|_| ErrorCode::OutOfMemory)?;
        let grown = usize::try_from(demand.grown_cap).map_err(|_| ErrorCode::OutOfMemory)?;
        let rehash_ranges = old.checked_add(9).ok_or(ErrorCode::OutOfMemory)?;
        reserve_cleared(&mut self.ranges, PLAIN_PLAN_RANGES.max(rehash_ranges))?;
        if old > 0 {
            let destinations = old.checked_add(1).ok_or(ErrorCode::OutOfMemory)?;
            reserve_cleared(&mut self.placements, destinations)?;
        }
        reserve_cleared(&mut self.occupied, grown)
    }

    /// From here until [`Self::unseal`], no plan of the batch may grow these
    /// buffers. Debug builds record the capacities now and check them again
    /// when each plan starts and each rehash is admitted.
    pub(crate) fn seal(&mut self) {
        #[cfg(debug_assertions)]
        {
            self.sealed = Some(self.capacities());
        }
    }

    /// End the batch that [`Self::seal`] sealed; plans may grow buffers again.
    pub(crate) fn unseal(&mut self) {
        #[cfg(debug_assertions)]
        {
            self.sealed = None;
        }
    }

    /// Debug only: the capacities of `ranges`, `placements` and `occupied`.
    #[cfg(debug_assertions)]
    fn capacities(&self) -> [usize; 3] {
        [
            self.ranges.capacity(),
            self.placements.capacity(),
            self.occupied.capacity(),
        ]
    }

    /// Debug only: a sealed batch's buffers must still hold the capacities
    /// `seal` recorded. Growth here is an allocation the batch's reservation
    /// did not cover.
    fn check_sealed(&self) {
        #[cfg(debug_assertions)]
        {
            if let Some(sealed) = self.sealed {
                debug_assert_eq!(
                    self.capacities(),
                    sealed,
                    "a nested plan grew past its batch's reservation"
                );
            }
        }
    }
}

/// Clear `buf` and reserve exactly `cap` elements. Capacity an earlier plan
/// paid for is kept, so a reservation the buffer already covers is free.
fn reserve_cleared<T>(buf: &mut Vec<T>, cap: usize) -> Result<(), ErrorCode> {
    buf.clear();
    buf.try_reserve_exact(cap)
        .map_err(|_| ErrorCode::OutOfMemory)
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
    List {
        offset: u32,
        capacity: u32,
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
    /// An inner `SUM_I64`'s total after the row, checked at preparation.
    SumI64 {
        offset: u32,
        total: i64,
    },
    /// Append one struct row to an inner ordered list, copying existing rows
    /// when the list doubled into a new arena allocation.
    ListAppend {
        list_offset: u32,
        copy_from: Option<(u32, u32)>,
        row_off: u32,
        row_size: u32,
        next_count: u32,
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

    /// Whether the planned write changes what a reader sees, so marks its
    /// slot.
    pub fn marks(&self) -> bool {
        self.flags != 0
    }

    pub fn ranges(&self) -> &[(u32, u32)] {
        &self.scratch.ranges
    }

    /// Absolute byte offset of the appended struct row for a list-append plan.
    pub fn list_row_offset(&self) -> Option<u32> {
        match self.mutation {
            InnerMutation::ListAppend { row_off, .. } => Some(row_off),
            _ => None,
        }
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
            Initialization::List { offset, .. } => self.push_range(offset, LIST_HDR_BYTES),
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
                Initialization::List { offset, capacity } => {
                    bytes::write_u32(state, offset, 0);
                    bytes::write_u32(state, offset + 4, capacity);
                }
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
            InnerMutation::SumI64 { offset, total } => {
                let count = bytes::read_u64(state, offset + 8);
                bytes::write_i64(state, offset, total);
                bytes::write_u64(state, offset + 8, count + 1);
            }
            InnerMutation::ListAppend {
                list_offset,
                copy_from,
                row_off,
                row_size,
                next_count,
            } => {
                if let Some((old_rows_off, copy_bytes)) = copy_from {
                    let src = old_rows_off as usize;
                    let dst = (list_offset + LIST_HDR_BYTES) as usize;
                    state.copy_within(src..src + copy_bytes as usize, dst);
                }
                bytes::zero(state, row_off, row_size);
                bytes::write_u32(state, list_offset, next_count);
            }
        }
        if self.flags != 0 {
            self.meta.mark(state, self.flags);
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
    let outer = OuterTable::bind_with_prefix(meta, &prefix);
    // A sentinel names no key: no growth could place it.
    if outer_key >= TOMBSTONE {
        return Err(ErrorCode::InvalidKey);
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
            outer.inner_at(state, probe.pos, &prefix)?
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
        let capacity = effective_inner_cap(inner_type, prefix.inner_initial_cap);
        let len = if inner_type == SlotType::OrderedList {
            if prefix.num_fields == 0 || prefix.row_size == 0 {
                return Err(ErrorCode::InvalidState);
            }
            inner_list_byte_size(capacity, prefix.row_size).ok_or(ErrorCode::CapacityExceeded)?
        } else {
            inner_container_size(
                inner_type,
                capacity,
                prefix.inner_agg_type_byte,
                prefix.row_size,
            )
        };
        let offset = plan.allocate(len)?;
        plan.inner_offset = offset;
        plan.push_range(outer.keys_off + probe.pos * 4, 4);
        plan.push_range(outer.size_off, 4);
        match inner_type {
            SlotType::Aggregate => {
                plan.initialize(Initialization::Aggregate { offset, len });
            }
            SlotType::OrderedList => {
                plan.initialize(Initialization::List { offset, capacity });
            }
            SlotType::HashMap | SlotType::HashSet => {
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
            _ => return Err(ErrorCode::InvalidProgram),
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
        for (offset, len) in crate::meta::mark_ranges(plan.meta.meta_base) {
            plan.push_range(offset, len);
        }
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
        return Err(ErrorCode::InvalidKey);
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
        } else if value.is_some_and(|value| value != table.entry_u32_at(plan.state, probe.pos)) {
            // An equal value rewrites nothing a reader sees.
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
        Some(
            kind @ (AggType::Count | AggType::Sum | AggType::Min | AggType::Max | AggType::SumI64),
        ) => kind,
        _ => return Err(ErrorCode::InvalidProgram),
    };
    let mut plan = prepare_outer(state, meta, outer_key, scratch, SlotType::Aggregate)?;
    let len = aggregates::agg_slot_byte_size(prefix.inner_agg_type_byte);
    plan.mutation = if kind == AggType::SumI64 {
        // A new key's aggregate is zeroed when the plan applies: its total
        // starts at zero. A total that would leave i64 refuses the row
        // before anything is written (specs/columine/07-wide-row-cells.md,
        // "A keyed i64 sum").
        let total = if plan.outer_new {
            0
        } else {
            bytes::read_i64(plan.state, plan.inner_offset)
        };
        InnerMutation::SumI64 {
            offset: plan.inner_offset,
            total: total
                .checked_add(value_bits.cast_signed())
                .ok_or(ErrorCode::SumOverflow)?,
        }
    } else {
        InnerMutation::Aggregate {
            offset: plan.inner_offset,
            kind,
            value_bits,
        }
    };
    if !plan.outer_new {
        plan.push_range(plan.inner_offset, len);
    }
    plan.flags |= ChangeFlag::SIZE_CHANGED;
    finish_plan(&mut plan);
    Ok(plan)
}

/// Plan appending one struct row to `outer_key`'s inner ordered list.
///
/// On apply, the caller captures [`NestedMutation::ranges`], calls
/// [`NestedMutation::apply`], and then writes the row's scalar fields at
/// [`NestedMutation::list_row_offset`] before finishing the capture.
pub fn prepare_nested_list_append_struct<'state, 'work>(
    state: &'state mut [u8],
    meta: &SlotMetaView,
    outer_key: u32,
    scratch: &'work mut NestedScratch,
) -> Result<NestedMutation<'state, 'work>, ErrorCode> {
    let mut plan = prepare_outer(state, meta, outer_key, scratch, SlotType::OrderedList)?;
    let prefix = read_nested_prefix(plan.state, meta.offset);
    if prefix.num_fields == 0 || prefix.row_size == 0 {
        return Err(ErrorCode::InvalidState);
    }
    let row_size = u32::from(prefix.row_size);

    if plan.outer_new {
        let row_off = plan
            .inner_offset
            .checked_add(LIST_HDR_BYTES)
            .ok_or(ErrorCode::InvalidState)?;
        plan.push_range(row_off, row_size);
        plan.mutation = InnerMutation::ListAppend {
            list_offset: plan.inner_offset,
            copy_from: None,
            row_off,
            row_size,
            next_count: 1,
        };
        plan.flags |= ChangeFlag::INSERTED;
        finish_plan(&mut plan);
        return Ok(plan);
    }

    let count = bytes::read_u32(plan.state, plan.inner_offset);
    let capacity = bytes::read_u32(plan.state, plan.inner_offset + 4);
    if count < capacity {
        let row_off = count
            .checked_mul(row_size)
            .and_then(|off| {
                plan.inner_offset
                    .checked_add(LIST_HDR_BYTES)?
                    .checked_add(off)
            })
            .ok_or(ErrorCode::InvalidState)?;
        plan.push_range(plan.inner_offset, 4);
        plan.push_range(row_off, row_size);
        plan.mutation = InnerMutation::ListAppend {
            list_offset: plan.inner_offset,
            copy_from: None,
            row_off,
            row_size,
            next_count: count + 1,
        };
        plan.flags |= ChangeFlag::INSERTED;
        finish_plan(&mut plan);
        return Ok(plan);
    }

    let new_cap = capacity.checked_mul(2).ok_or(ErrorCode::CapacityExceeded)?;
    let new_len =
        inner_list_byte_size(new_cap, prefix.row_size).ok_or(ErrorCode::CapacityExceeded)?;
    let old_offset = plan.inner_offset;
    let new_offset = plan.allocate(new_len)?;
    let copy_bytes = count.checked_mul(row_size).ok_or(ErrorCode::InvalidState)?;
    let new_rows_off = new_offset
        .checked_add(LIST_HDR_BYTES)
        .ok_or(ErrorCode::InvalidState)?;
    let row_off = new_rows_off
        .checked_add(copy_bytes)
        .ok_or(ErrorCode::InvalidState)?;
    let written_bytes = copy_bytes
        .checked_add(row_size)
        .ok_or(ErrorCode::InvalidState)?;
    plan.inner_offset = new_offset;
    plan.initialize(Initialization::List {
        offset: new_offset,
        capacity: new_cap,
    });
    plan.push_range(new_rows_off, written_bytes);
    plan.mutation = InnerMutation::ListAppend {
        list_offset: new_offset,
        copy_from: (copy_bytes > 0).then_some((old_offset + LIST_HDR_BYTES, copy_bytes)),
        row_off,
        row_size,
        next_count: count + 1,
    };
    plan.flags |= ChangeFlag::INSERTED;
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

/// What a batch of nested mutations on one slot needs: new outer keys, arena
/// bytes, and the largest inner rehash, which sizes the plan scratch. The
/// arena is a bump allocator that never reclaims, and
/// [`prepare_outer`]/[`prepare_table`] are its only writers in a batch, so
/// the demand is exact up to the order of inner keys, which does not change
/// it: a container rehashes to twice its capacity before an insert finds
/// its size at the load limit, and that depends only on how many new keys
/// reach it.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct NestedDemand {
    pub(crate) outer_keys: u64,
    pub(crate) arena_bytes: u64,
    pub(crate) fits: bool,
    /// The largest inner rehash the batch makes; zero when none does.
    pub(crate) rehash: NestedPlanDemand,
}

/// Demand of the `(outer, inner)` key pairs one batch sends to `meta`'s slot,
/// as the slot is now; `inner_type` is the container the instructions name.
/// `pairs` is sorted and deduplicated in place; an aggregate's inner key is
/// ignored. `None` means execution refuses for another reason (a sentinel
/// key, a container of another type), which a dry run reports.
pub(crate) fn batch_demand(
    state: &[u8],
    meta: &SlotMetaView,
    inner_type: SlotType,
    pairs: &mut Vec<(u32, u32)>,
) -> Option<NestedDemand> {
    let prefix = read_nested_prefix(state, meta.offset);
    if prefix.inner_type != inner_type {
        return None;
    }
    let entry_size = match inner_type {
        SlotType::HashMap => hash_table::ENTRY_U32,
        SlotType::HashSet => hash_table::ENTRY_NONE,
        SlotType::Aggregate | SlotType::OrderedList => 0,
        _ => return None,
    };
    let aggregate = inner_type == SlotType::Aggregate;
    let ordered_list = inner_type == SlotType::OrderedList;
    if aggregate || ordered_list {
        for pair in pairs.iter_mut() {
            pair.1 = 0;
        }
    }
    pairs.sort_unstable();
    // Ordered lists append once per row even for repeated outer keys; sets,
    // maps, and aggregates collapse duplicate `(outer, inner)` pairs.
    if !ordered_list {
        pairs.dedup();
    }
    let outer = OuterTable::bind_with_prefix(meta, &prefix);
    let initial_cap = effective_inner_cap(inner_type, prefix.inner_initial_cap);
    let mut demand = NestedDemand::default();
    let mut bytes = 0u64;
    let align = |len: u64| (len + 7) & !7;
    let mut at = 0;
    while at < pairs.len() {
        let key = pairs[at].0;
        let end = at + pairs[at..].iter().take_while(|pair| pair.0 == key).count();
        if key >= TOMBSTONE
            || (!aggregate && !ordered_list && pairs[at..end].iter().any(|p| p.1 >= TOMBSTONE))
        {
            return None;
        }
        let offset = outer.lookup_checked(state, key, &prefix).ok()?;
        if ordered_list {
            if prefix.num_fields == 0 || prefix.row_size == 0 {
                return None;
            }
            let (mut cap, mut count) = if offset != 0 {
                (
                    bytes::read_u32(state, offset + 4),
                    bytes::read_u32(state, offset),
                )
            } else {
                demand.outer_keys += 1;
                let init_bytes = u64::from(LIST_HDR_BYTES)
                    .checked_add(u64::from(initial_cap).checked_mul(u64::from(prefix.row_size))?)?;
                bytes = bytes.checked_add(align(init_bytes))?;
                (initial_cap, 0)
            };
            for _ in at..end {
                if count >= cap {
                    cap = cap.checked_mul(2)?;
                    let grown = u64::from(LIST_HDR_BYTES)
                        .checked_add(u64::from(cap).checked_mul(u64::from(prefix.row_size))?)?;
                    bytes = bytes.checked_add(align(grown))?;
                }
                count = count.checked_add(1)?;
            }
            at = end;
            continue;
        }
        let existing = (offset != 0 && !aggregate)
            .then(|| hash_table::FlatTable::bind(state, offset, entry_size));
        let (mut cap, mut size) = match existing {
            Some(table) => (table.cap, table.size(state)),
            None => (initial_cap, 0),
        };
        if offset == 0 {
            demand.outer_keys += 1;
            bytes += align(u64::from(inner_container_size(
                inner_type,
                initial_cap,
                prefix.inner_agg_type_byte,
                prefix.row_size,
            )));
        }
        if !aggregate {
            for &(_, inner) in &pairs[at..end] {
                if existing.is_some_and(|table| table.contains(state, inner)) {
                    continue;
                }
                if size >= hash_table::max_load_for_cap(cap) {
                    let old = cap;
                    cap = cap.checked_mul(2)?;
                    demand.rehash.widen(NestedPlanDemand {
                        old_cap: old,
                        grown_cap: cap,
                    });
                    bytes += align(
                        u64::from(cap) * u64::from(4 + entry_size)
                            + u64::from(hash_table::HDR_BYTES),
                    );
                }
                size += 1;
            }
        }
        at = end;
    }
    demand.arena_bytes = bytes;
    demand.fits = u64::from(outer.size(state)) + demand.outer_keys
        <= u64::from(hash_table::max_load_for_cap(outer.cap))
        && u64::from(outer.arena.used(state)) + bytes <= u64::from(outer.arena.capacity(state));
    Some(demand)
}

/// Geometry of one outer key's inner ordered list inside a state image.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NestedListView {
    /// Absolute byte offset of row `0` in the state image (after `[count:u32][capacity:u32]`).
    pub rows_offset: u32,
    /// Number of appended rows currently stored (`0..=capacity`).
    pub count: u32,
    /// Allocated row capacity of this inner list container.
    pub capacity: u32,
    /// Four-byte-padded byte width of one struct row.
    pub row_size: u32,
    /// Absolute byte offset of the slot's field-type descriptor (`num_fields` bytes).
    pub descriptor_offset: u32,
    /// Number of fields per struct row (`1..=255`).
    pub num_fields: u8,
    /// Byte width of the per-row presence bitset (`ceil(num_fields / 8)`).
    pub bitset_bytes: u32,
}

/// Look up `outer_key`'s inner ordered list in nested `slot`.
///
/// Returns `Ok(Some(view))` when `outer_key` is present in a well-formed nested
/// `OrderedList` slot, or `Ok(None)` when `outer_key` is absent.
///
/// # Errors
///
/// - [`ErrorCode::InvalidSlot`] when `slot` is at or past the image's slot count.
/// - [`ErrorCode::InvalidProgram`] when `slot` is another valid slot kind or a
///   nested slot whose valid inner kind is not [`SlotType::OrderedList`].
/// - [`ErrorCode::InvalidState`] when the image, slot metadata, prefix, arena
///   bounds, or matched inner list container (checked via [`check_inner`]) is
///   malformed.
pub fn nested_list_lookup(
    state: &[u8],
    slot: u32,
    outer_key: u32,
) -> Result<Option<NestedListView>, ErrorCode> {
    if state.len() < STATE_HEADER_SIZE as usize {
        return Err(ErrorCode::InvalidState);
    }
    if slot >= state_num_slots(state) {
        return Err(ErrorCode::InvalidSlot);
    }
    let meta_base = slot_meta_base(slot);
    if meta_base
        .checked_add(SLOT_META_SIZE)
        .is_none_or(|end| end as usize > state.len())
    {
        return Err(ErrorCode::InvalidState);
    }
    let flags = SlotTypeFlags::from_byte(state[(meta_base + SlotMetaOffset::TYPE_FLAGS) as usize]);
    match flags.slot_type() {
        Some(SlotType::Nested) => {}
        Some(_) => return Err(ErrorCode::InvalidProgram),
        None => return Err(ErrorCode::InvalidState),
    }
    let meta = SlotMetaView::read(state, slot);
    if meta.capacity == 0 || !meta.capacity.is_power_of_two() {
        return Err(ErrorCode::InvalidState);
    }
    if meta
        .offset
        .checked_add(NESTED_PREFIX_SIZE)
        .is_none_or(|end| end as usize > state.len())
    {
        return Err(ErrorCode::InvalidState);
    }
    match SlotType::from_u8(state[meta.offset as usize] & 0x0f) {
        Some(SlotType::OrderedList) => {}
        Some(_) => return Err(ErrorCode::InvalidProgram),
        None => return Err(ErrorCode::InvalidState),
    }
    let prefix = read_nested_prefix(state, meta.offset);
    let layout = check_ordered_list_prefix(state, meta.offset, &prefix)?;
    let arena_hdr = meta
        .offset
        .checked_add(NESTED_PREFIX_SIZE)
        .and_then(|off| off.checked_add(layout.descriptor_size))
        .and_then(|off| off.checked_add(meta.capacity.checked_mul(8)?))
        .ok_or(ErrorCode::InvalidState)?;
    let arena_data = arena_hdr
        .checked_add(ARENA_HDR_SIZE)
        .ok_or(ErrorCode::InvalidState)?;
    if arena_data as usize > state.len() {
        return Err(ErrorCode::InvalidState);
    }
    let arena_cap = bytes::read_u32(state, arena_hdr);
    let arena_used = bytes::read_u32(state, arena_hdr + ARENA_HDR_USED);
    if arena_used > arena_cap
        || arena_data
            .checked_add(arena_cap)
            .is_none_or(|end| end as usize > state.len())
    {
        return Err(ErrorCode::InvalidState);
    }
    let outer = OuterTable::bind_with_prefix(&meta, &prefix);
    let ptr = outer.lookup_checked(state, outer_key, &prefix)?;
    if ptr == 0 {
        return Ok(None);
    }
    let count = bytes::read_u32(state, ptr);
    let capacity = bytes::read_u32(state, ptr + 4);
    Ok(Some(NestedListView {
        rows_offset: ptr + LIST_HDR_BYTES,
        count,
        capacity,
        row_size: layout.row_size,
        descriptor_offset: NestedPrefix::descriptor_offset(meta.offset),
        num_fields: prefix.num_fields,
        bitset_bytes: layout.bitset_bytes,
    }))
}

/// Return zero when the outer key is absent.
pub fn get_inner_offset(state: &[u8], meta: &SlotMetaView, outer_key: u32) -> u32 {
    OuterTable::bind(state, meta).lookup(state, outer_key)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_init;
    use crate::vm::{Vm, u32s_as_bytes};
    use columine_types::PROGRAM_MAGIC;
    use columine_types::types::Opcode;

    fn program(inner: SlotType, inner_cap: u8, body: &[u8]) -> Vec<u8> {
        let mut init = vec![
            Opcode::SlotNested as u8,
            0,
            SlotType::Nested as u8,
            0,
            1, // outer capacity 256: the arena, not the outer table, binds
            inner as u8,
            inner_cap,
            0,
            0,
        ];
        if inner == SlotType::OrderedList {
            init.push(1);
            init.push(columine_types::types::StructFieldType::UInt32 as u8);
        }
        init.push(Opcode::Halt as u8);
        let mut reduce = vec![Opcode::ForEach as u8, 2, 1];
        reduce.extend(1u32.to_le_bytes());
        reduce.extend((body.len() as u16).to_le_bytes());
        reduce.extend_from_slice(body);
        let mut prog = vec![0u8; 32];
        prog.extend(PROGRAM_MAGIC.to_le_bytes());
        prog.extend([2, 0, 0, 0]);
        prog.extend(1u32.to_le_bytes());
        prog.extend(3u32.to_le_bytes());
        prog.extend((init.len() as u32).to_le_bytes());
        prog.extend((reduce.len() as u32).to_le_bytes());
        prog.extend(init);
        prog.extend(reduce);
        prog
    }

    /// The demand model is exact: a batch that runs adds exactly the outer
    /// keys and arena bytes `batch_demand` computed for it beforehand.
    #[test]
    fn batch_demand_is_the_arena_and_outer_growth_execution_makes() {
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let set_body = [Opcode::NestedSetInsert as u8, 0, 0, 1];
        let map_body = [Opcode::NestedMapUpsertLast as u8, 0, 0, 1, 1];
        let list_body = [Opcode::NestedListAppendStruct as u8, 0, 0, 1, 1, 0];
        for (inner, cap, body) in [
            (SlotType::HashSet, 1u8, &set_body[..]),
            (SlotType::HashSet, 4, &set_body[..]),
            (SlotType::HashMap, 2, &map_body[..]),
            (SlotType::OrderedList, 2, &list_body[..]),
            (SlotType::OrderedList, 5, &list_body[..]),
        ] {
            let prog = program(inner, cap, body);
            let size = state_init::calculate_state_size(
                &prog,
                state_init::DEFAULT_ACCEPTED_PROGRAM_MAGICS,
            )
            .unwrap();
            let mut state = vec![0u8; size as usize];
            state_init::init_state(
                &mut state,
                &prog,
                state_init::DEFAULT_ACCEPTED_PROGRAM_MAGICS,
            )
            .unwrap();
            let mut vm = Vm::default();
            let mut pairs = Vec::new();
            for _ in 0..40 {
                let rows = 1 + (next() % 12) as usize;
                let outer: Vec<u32> = (0..rows).map(|_| (next() % 9) as u32).collect();
                let inner_keys: Vec<u32> = (0..rows).map(|_| (next() % 30) as u32).collect();
                let types = vec![1u32; rows];
                let meta = SlotMetaView::read(&state, 0);
                pairs.clear();
                pairs.extend(outer.iter().copied().zip(inner_keys.iter().copied()));
                let demand = batch_demand(&state, &meta, inner, &mut pairs).unwrap();
                let table = OuterTable::bind(&state, &meta);
                let (used, keys) = (table.arena.used(&state), table.size(&state));
                let status = vm.execute_batch(
                    &mut state,
                    &prog,
                    &[
                        u32s_as_bytes(&outer),
                        u32s_as_bytes(&inner_keys),
                        u32s_as_bytes(&types),
                    ],
                    rows as u32,
                );
                if !demand.fits {
                    assert_eq!(ErrorCode::NeedsGrowth as u32, status);
                    break;
                }
                assert_eq!(ErrorCode::Ok as u32, status);
                assert_eq!(
                    demand.arena_bytes,
                    u64::from(table.arena.used(&state) - used),
                    "{inner:?} cap {cap}: arena bytes"
                );
                assert_eq!(
                    demand.outer_keys,
                    u64::from(table.size(&state) - keys),
                    "{inner:?} cap {cap}: outer keys"
                );
            }
        }
    }
}
