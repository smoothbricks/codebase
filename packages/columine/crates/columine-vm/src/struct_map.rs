//! Typed accessors for struct-map rows.
//!
//! Slot data layout:
//! `[field_type_descriptor: u8 × num_fields (padded to 8)]`
//! `[key index region]` `[rows: row_size × capacity]`
//! `[timestamps: f64 × capacity]?` (one-key maps only), then the CSR arena.
//!
//! A row's ordinal is its identity: the key index (`crate::key_index`) names
//! the ordinal a key owns, and the row lives at `rows_base + ordinal *
//! row_size`. The views here reach keys only through the slot's [`KeyIndex`]
//! family. A writer plans a key once and keeps that plan's ordinal for its
//! prior-byte capture and its mutation, so every overwritten index, row field
//! and arena byte is restored physically.
//!
//! Raw public writers are unjournaled operations on caller-owned bytes.
//! Speculative callers use journaled VM operations or a bounded capture.

use crate::bytes;
use crate::hash_table::Probe;
use crate::key_index::{
    HashIndex, HashScratch, IndexRegion, InsertPlan, InternBytes, Key, KeyCx, KeyIndex, KeyLanes,
    NoStrings, RemovePlan,
};
use crate::meta::slot_meta_base;
use columine_types::types::{
    EMPTY_KEY, ErrorCode, SlotMetaOffset, StructFieldType, TOMBSTONE, align8, arena_elem_size,
    is_arena_field_type, struct_field_size,
};
use core::marker::PhantomData;

/// Checked physical bounds of a per-slot CSR arena. No schema or frame decoding.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ArenaRegion {
    pub(crate) header: u32,
    pub(crate) data: u32,
    pub(crate) capacity: u32,
    pub(crate) used: u32,
}

impl ArenaRegion {
    pub(crate) fn bind(state: &[u8], header: u32) -> Result<Self, ErrorCode> {
        if header == 0 {
            return Err(ErrorCode::InvalidState);
        }
        let data = header
            .checked_add(crate::state_init::ARENA_HEADER_SIZE)
            .ok_or(ErrorCode::InvalidState)?;
        let header_bytes = state
            .get(header as usize..data as usize)
            .ok_or(ErrorCode::InvalidState)?;
        let capacity = bytes::read_u32(header_bytes, 0);
        let used = bytes::read_u32(header_bytes, 4);
        let end = data.checked_add(capacity).ok_or(ErrorCode::InvalidState)?;
        if used > capacity || end as usize > state.len() {
            return Err(ErrorCode::InvalidState);
        }
        Ok(Self {
            header,
            data,
            capacity,
            used,
        })
    }

    pub(crate) fn bytes<'a>(
        &self,
        state: &'a [u8],
        offset: u32,
        len: u32,
        elem_size: u32,
    ) -> Result<&'a [u8], ErrorCode> {
        let byte_len = len.checked_mul(elem_size).ok_or(ErrorCode::InvalidState)?;
        let end = offset
            .checked_add(byte_len)
            .ok_or(ErrorCode::InvalidState)?;
        if elem_size == 0 || end > self.used {
            return Err(ErrorCode::InvalidState);
        }
        let start = self
            .data
            .checked_add(offset)
            .ok_or(ErrorCode::InvalidState)?;
        let end = self.data.checked_add(end).ok_or(ErrorCode::InvalidState)?;
        state
            .get(start as usize..end as usize)
            .ok_or(ErrorCode::InvalidState)
    }
}

/// Result of an `upsert` operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Upsert {
    pub pos: u32,
    pub is_new: bool,
}

fn write_scalar_cell(
    state: &mut [u8],
    f_off: u32,
    ft: StructFieldType,
    col: &[u8],
    element_idx: u32,
) {
    match ft {
        StructFieldType::UInt32 | StructFieldType::String => {
            bytes::write_u32(state, f_off, bytes::read_u32(col, element_idx * 4));
        }
        StructFieldType::Int64 | StructFieldType::Float64 => {
            bytes::write_u64(state, f_off, bytes::read_u64(col, element_idx * 8));
        }
        StructFieldType::Bool => {
            state[f_off as usize] = u8::from(bytes::read_u32(col, element_idx * 4) != 0);
        }
        StructFieldType::ArrayU32
        | StructFieldType::ArrayI64
        | StructFieldType::ArrayF64
        | StructFieldType::ArrayString
        | StructFieldType::ArrayBool
        | StructFieldType::Vo => unreachable!("arena fields require CSR operands"),
    }
}
/// Encode one column cell exactly as [`write_scalar_cell`] writes it, so the
/// probe-free scatter (0x3e) can equality-check a stored field against the
/// incoming cell without writing. Lives beside [`write_scalar_cell`]: the two
/// encodings are one contract, and a drift between them would silently break
/// the scatter's retract-iff-current semantics.
///
/// `None` for array field types — a 0x3e route destination is a scalar field
/// by definition (the value source is a column cell, not a same-layout field),
/// and an array route is refused by name instead of comparing garbage.
pub(crate) fn scalar_cell_encoded(
    ft: StructFieldType,
    col: &[u8],
    element_idx: u32,
) -> Option<([u8; 8], u32)> {
    match ft {
        StructFieldType::UInt32 | StructFieldType::String => {
            let mut buf = [0u8; 8];
            buf[..4].copy_from_slice(&bytes::read_u32(col, element_idx * 4).to_le_bytes());
            Some((buf, 4))
        }
        StructFieldType::Int64 | StructFieldType::Float64 => {
            Some((bytes::read_u64(col, element_idx * 8).to_le_bytes(), 8))
        }
        StructFieldType::Bool => {
            let mut buf = [0u8; 8];
            buf[0] = u8::from(bytes::read_u32(col, element_idx * 4) != 0);
            Some((buf, 1))
        }
        StructFieldType::ArrayU32
        | StructFieldType::ArrayI64
        | StructFieldType::ArrayF64
        | StructFieldType::ArrayString
        | StructFieldType::ArrayBool
        | StructFieldType::Vo => None,
    }
}

/// The order-preserving `u64` image of one scalar cell, in the exact stored
/// form [`write_scalar_cell`] writes and [`scalar_cell_encoded`] returns.
///
/// WHY an image and not a per-type comparison: the guarded scatter (`0x3f`)
/// compares a tuple of guard components lexicographically, once per element,
/// on the hot path. Mapping each component into one unsigned space collapses
/// the whole comparison to `[u64; N] < [u64; N]` — one code path, no per-type
/// branch inside the compare, and the same image for the element's cell and
/// the stored field because both arrive here as the same bytes.
///
/// The mapping is monotone in each type's own order: unsigned lanes pass
/// through, `Int64` flips the sign bit so negatives sort below positives, and
/// `Float64` takes the standard total order (negatives inverted, positives
/// sign-set) so every bit pattern including NaN lands somewhere definite
/// rather than comparing false against itself.
///
/// `None` for array field types, for the same reason
/// [`scalar_cell_encoded`] refuses them: an array field has no column-cell
/// form, so a guard naming one is refused by name instead of ordering
/// garbage.
pub(crate) fn scalar_order_key(ft: StructFieldType, cell: &[u8]) -> Option<u64> {
    match ft {
        StructFieldType::UInt32 | StructFieldType::String => {
            Some(u64::from(bytes::read_u32(cell, 0)))
        }
        StructFieldType::Bool => Some(u64::from(cell[0] != 0)),
        StructFieldType::Int64 => Some(bytes::read_u64(cell, 0) ^ (1u64 << 63)),
        StructFieldType::Float64 => {
            let bits = bytes::read_u64(cell, 0);
            Some(if bits >> 63 == 0 {
                bits | (1u64 << 63)
            } else {
                !bits
            })
        }
        StructFieldType::ArrayU32
        | StructFieldType::ArrayI64
        | StructFieldType::ArrayF64
        | StructFieldType::ArrayString
        | StructFieldType::ArrayBool
        | StructFieldType::Vo => None,
    }
}

/// One column cell as an order key: encoded exactly as it would be stored,
/// then imaged. The pairing lives here rather than at the call site so the
/// element side of a guard comparison cannot encode through one path and
/// order through another.
pub(crate) fn scalar_cell_order_key(
    ft: StructFieldType,
    col: &[u8],
    element_idx: u32,
) -> Option<u64> {
    let (cell, size) = scalar_cell_encoded(ft, col, element_idx)?;
    scalar_order_key(ft, &cell[..size as usize])
}

/// Whether a lane-1 key is a sentinel: `EMPTY_KEY` and `TOMBSTONE` mark dead
/// rows, so no row holds one. The views answer a sentinel themselves (absent,
/// or `InvalidKey` for an insert) and no key-index family ever sees one;
/// which opcodes skip it and which refuse it is the VM's.
const fn is_sentinel(key1: u32) -> bool {
    key1 == EMPTY_KEY || key1 == TOMBSTONE
}

/// Run one [`HashIndex`] call for the default views' convenience methods: it
/// resolves no text and its scratch stays empty.
pub(crate) fn hash_call<R>(call: impl FnOnce(&mut KeyCx<'_, HashIndex, NoStrings>) -> R) -> R {
    let mut scratch = HashScratch::default();
    call(&mut KeyCx::new(&NoStrings, &mut scratch))
}

/// The end of an index region starting at `at`, saturating: an unchecked
/// binder reads metadata its slot's geometry check already admitted.
fn region_end<I: KeyIndex>(at: u32, capacity: u32, lanes: KeyLanes) -> u32 {
    u32::try_from(u64::from(at) + I::region_bytes(capacity, lanes)).unwrap_or(u32::MAX)
}

/// The metadata both struct-map kinds bind.
struct Bound {
    slot_offset: u32,
    capacity: u32,
    size_off: u32,
    num_fields: u8,
    bitset_bytes: u32,
    row_size: u32,
    descriptor_size: u32,
    keys_off: u32,
    lanes: KeyLanes,
    rows_base: u32,
    arena_header_off: u32,
}

/// Read a struct-map slot's metadata record. Metadata byte reuse: byte 13 =
/// num_fields, byte 15 = bitset_bytes, bytes 16–17 = row_size, byte 19 = key
/// lane kinds, bytes 20–23 = arena header offset.
fn bind_meta<I: KeyIndex>(state: &[u8], slot_idx: u32, pair: bool) -> Bound {
    let meta_base = slot_meta_base(slot_idx);
    let slot_offset = bytes::read_u32(state, meta_base + SlotMetaOffset::OFFSET);
    let capacity = bytes::read_u32(state, meta_base + SlotMetaOffset::CAPACITY);
    let num_fields = state[(meta_base + SlotMetaOffset::AGG_TYPE) as usize];
    let descriptor_size = align8(u32::from(num_fields));
    let keys_off = slot_offset + descriptor_size;
    let lanes = KeyLanes::bound(
        pair,
        state[(meta_base + SlotMetaOffset::STRUCT_KEY_LANES) as usize],
    );
    Bound {
        slot_offset,
        capacity,
        size_off: meta_base + SlotMetaOffset::SIZE,
        num_fields,
        bitset_bytes: u32::from(state[(meta_base + SlotMetaOffset::STRUCT_BITSET_BYTES) as usize]),
        row_size: u32::from(bytes::read_u16(
            state,
            meta_base + SlotMetaOffset::TTL_SECONDS,
        )),
        descriptor_size,
        keys_off,
        lanes,
        rows_base: region_end::<I>(keys_off, capacity, lanes),
        arena_header_off: bytes::read_u32(state, meta_base + SlotMetaOffset::GRACE_SECONDS),
    }
}

/// The row geometry of one struct-map slot, whatever its key index: what a
/// reader needs once the index has named a row's ordinal.
#[derive(Clone, Copy, Debug)]
pub struct StructRows {
    pub capacity: u32,
    pub num_fields: u8,
    pub bitset_bytes: u32,
    pub row_size: u32,
    /// Offset of the field-type descriptor bytes.
    pub field_types_off: u32,
    /// Absolute offset of row zero.
    pub rows_base: u32,
    /// Absolute header offset of the slot's CSR arena; zero when there is none.
    pub arena_header_off: u32,
}

impl StructRows {
    /// Absolute byte offset of row `pos`.
    pub const fn row_off(&self, pos: u32) -> u32 {
        self.rows_base + pos * self.row_size
    }

    /// The raw field-type byte for `field_idx` (descriptor region).
    #[inline(always)]
    pub fn field_type_byte(&self, state: &[u8], field_idx: u8) -> u8 {
        state[(self.field_types_off + u32::from(field_idx)) as usize]
    }

    #[inline(always)]
    pub(crate) fn field_type(&self, state: &[u8], field_idx: u8) -> StructFieldType {
        StructFieldType::from_u8(self.field_type_byte(state, field_idx)).unwrap_or_else(|| {
            columine_types::die!("invariant: struct-map descriptor contains an invalid field type")
        })
    }

    /// Byte offset of a field within a row (bitset first, then cumulative
    /// field sizes without padding).
    pub fn field_offset(&self, state: &[u8], field_idx: u8) -> u32 {
        let mut off = self.bitset_bytes;
        for i in 0..field_idx {
            off += struct_field_size(self.field_type(state, i));
        }
        off
    }

    /// Clear the row's presence bitset without journaling.
    pub fn clear_bitset(&self, state: &mut [u8], row_off: u32) {
        bytes::zero(state, row_off, self.bitset_bytes);
    }

    /// Borrow a present arena field. The borrow cannot outlive a mutation, growth,
    /// restore or fork replacement of the state image. VO bytes are opaque here.
    pub fn arena_field<'a>(
        &self,
        state: &'a [u8],
        pos: u32,
        field_idx: u8,
    ) -> Result<Option<&'a [u8]>, ErrorCode> {
        if pos >= self.capacity || field_idx >= self.num_fields {
            return Err(ErrorCode::InvalidProgram);
        }
        let descriptor_end = self
            .field_types_off
            .checked_add(u32::from(self.num_fields))
            .ok_or(ErrorCode::InvalidState)?;
        let descriptor = state
            .get(self.field_types_off as usize..descriptor_end as usize)
            .ok_or(ErrorCode::InvalidState)?;
        let field_type = StructFieldType::from_u8(descriptor[usize::from(field_idx)])
            .ok_or(ErrorCode::InvalidState)?;
        if !is_arena_field_type(field_type) {
            return Err(ErrorCode::InvalidProgram);
        }
        let row = self
            .rows_base
            .checked_add(
                pos.checked_mul(self.row_size)
                    .ok_or(ErrorCode::InvalidState)?,
            )
            .ok_or(ErrorCode::InvalidState)?;
        let bit_off = row
            .checked_add(u32::from(field_idx) / 8)
            .ok_or(ErrorCode::InvalidState)?;
        let bit = state.get(bit_off as usize).ok_or(ErrorCode::InvalidState)?;
        if bit & (1 << (field_idx % 8)) == 0 {
            return Ok(None);
        }
        let mut field_offset = self.bitset_bytes;
        for &kind in &descriptor[..usize::from(field_idx)] {
            let kind = StructFieldType::from_u8(kind).ok_or(ErrorCode::InvalidState)?;
            field_offset = field_offset
                .checked_add(struct_field_size(kind))
                .ok_or(ErrorCode::InvalidState)?;
        }
        if field_offset
            .checked_add(8)
            .is_none_or(|end| end > self.row_size)
        {
            return Err(ErrorCode::InvalidState);
        }
        let field = row
            .checked_add(field_offset)
            .ok_or(ErrorCode::InvalidState)?;
        let cell_end = field.checked_add(8).ok_or(ErrorCode::InvalidState)?;
        let cell = state
            .get(field as usize..cell_end as usize)
            .ok_or(ErrorCode::InvalidState)?;
        let rows_end = self
            .rows_base
            .checked_add(
                self.capacity
                    .checked_mul(self.row_size)
                    .ok_or(ErrorCode::InvalidState)?,
            )
            .ok_or(ErrorCode::InvalidState)?;
        if self.arena_header_off < rows_end {
            return Err(ErrorCode::InvalidState);
        }
        let arena = ArenaRegion::bind(state, self.arena_header_off)?;
        arena
            .bytes(
                state,
                bytes::read_u32(cell, 0),
                bytes::read_u32(cell, 4),
                arena_elem_size(field_type),
            )
            .map(Some)
    }

    /// Write one scalar field from a raw little-endian column into the row,
    /// setting its bitset bit. Array fields are handled by the arena path.
    /// Unjournaled; the caller owns or has captured the row bytes.
    pub fn write_scalar_field(
        &self,
        state: &mut [u8],
        pos: u32,
        field_idx: u8,
        cols: &[&[u8]],
        val_col: u32,
        element_idx: u32,
    ) {
        let row = self.row_off(pos);
        let ft = self.field_type(state, field_idx);
        let f_off = row + self.field_offset(state, field_idx);
        StructMapSlot::set_field_bit(state, row, field_idx);
        write_scalar_cell(state, f_off, ft, cols[val_col as usize], element_idx);
    }
}

/// Bound one-key struct-map view carrying offsets into the state buffer.
/// No pointers into state are formed. `I` is the slot's key-index family.
#[derive(Clone, Copy, Debug)]
pub struct StructMapSlot<I: KeyIndex = HashIndex> {
    pub slot_offset: u32,
    pub capacity: u32,
    /// Offset of the u32 size field in slot metadata.
    pub size_off: u32,
    pub num_fields: u8,
    pub bitset_bytes: u32,
    pub row_size: u32,
    /// `align8(num_fields)`, the descriptor size.
    pub descriptor_size: u32,
    /// Offset of the field-type descriptor bytes (== `slot_offset`).
    pub field_types_off: u32,
    /// Absolute offset of the key index region (a [`HashIndex`]'s key lane).
    pub keys_off: u32,
    /// The key lane the program declared.
    pub lanes: KeyLanes,
    /// Absolute offset of row zero.
    pub rows_base: u32,
    /// Absolute header offset of the slot's CSR arena; zero when there is none.
    pub arena_header_off: u32,
    family: PhantomData<I>,
}

impl<I: KeyIndex> StructMapSlot<I> {
    /// Bind a one-key slot of family `I` from its metadata record, unchecked:
    /// the caller has validated the slot's geometry.
    pub fn bind_in(state: &[u8], slot_idx: u32) -> Self {
        let b = bind_meta::<I>(state, slot_idx, false);
        Self {
            slot_offset: b.slot_offset,
            capacity: b.capacity,
            size_off: b.size_off,
            num_fields: b.num_fields,
            bitset_bytes: b.bitset_bytes,
            row_size: b.row_size,
            descriptor_size: b.descriptor_size,
            field_types_off: b.slot_offset,
            keys_off: b.keys_off,
            lanes: b.lanes,
            rows_base: b.rows_base,
            arena_header_off: b.arena_header_off,
            family: PhantomData,
        }
    }

    /// The slot's index region.
    pub const fn index(&self) -> IndexRegion {
        IndexRegion {
            at: self.keys_off,
            capacity: self.capacity,
            lanes: self.lanes,
        }
    }

    /// The slot's row geometry.
    pub const fn rows(&self) -> StructRows {
        StructRows {
            capacity: self.capacity,
            num_fields: self.num_fields,
            bitset_bytes: self.bitset_bytes,
            row_size: self.row_size,
            field_types_off: self.field_types_off,
            rows_base: self.rows_base,
            arena_header_off: self.arena_header_off,
        }
    }

    /// Current element count.
    #[inline(always)]
    pub fn size(&self, state: &[u8]) -> u32 {
        bytes::read_u32(state, self.size_off)
    }

    #[inline(always)]
    pub(crate) fn set_size(&self, state: &mut [u8], value: u32) {
        bytes::write_u32(state, self.size_off, value);
    }

    /// The key row `pos` holds, or `None` when the row is dead.
    pub fn key_at(&self, state: &[u8], pos: u32) -> Option<u32> {
        I::key_at(state, self.index(), pos).map(|key| key.k1)
    }

    /// The ordinal `key` owns, or `None`.
    pub fn lookup<S: InternBytes + ?Sized>(
        &self,
        state: &[u8],
        key: u32,
        cx: &mut KeyCx<'_, I, S>,
    ) -> Result<Option<u32>, ErrorCode> {
        if is_sentinel(key) {
            return Ok(None);
        }
        I::find(state, self.index(), Key::one(key), cx)
    }

    /// Plan one insert-or-update without changing the slot. The plan's
    /// ordinal is reused by the writer and journal; no second probe runs.
    pub fn plan_upsert<S: InternBytes + ?Sized>(
        &self,
        state: &[u8],
        key: u32,
        cx: &mut KeyCx<'_, I, S>,
    ) -> Result<InsertPlan<I::InsertPlan>, ErrorCode> {
        if is_sentinel(key) {
            return Err(ErrorCode::InvalidKey);
        }
        I::plan_insert(state, self.index(), self.size(state), Key::one(key), cx)
    }

    /// Apply a plan made against this unchanged slot: a new key enters the
    /// index and the size; a present key changes nothing.
    pub fn apply_upsert<S: InternBytes + ?Sized>(
        &self,
        state: &mut [u8],
        key: u32,
        plan: &InsertPlan<I::InsertPlan>,
        cx: &mut KeyCx<'_, I, S>,
    ) -> Result<(), ErrorCode> {
        if plan.is_new {
            I::apply_insert(state, self.index(), Key::one(key), plan, cx)?;
            self.set_size(state, self.size(state) + 1);
        }
        Ok(())
    }

    /// Plan removing `key`; `None` when it is absent.
    pub fn plan_remove<S: InternBytes + ?Sized>(
        &self,
        state: &[u8],
        key: u32,
        cx: &mut KeyCx<'_, I, S>,
    ) -> Result<Option<RemovePlan<I::RemovePlan>>, ErrorCode> {
        if is_sentinel(key) {
            return Ok(None);
        }
        I::plan_remove(state, self.index(), Key::one(key), cx)
    }

    /// Apply a remove plan made against this unchanged slot and shrink the
    /// size. The vacated row's bytes are left to the caller.
    pub fn apply_remove<S: InternBytes + ?Sized>(
        &self,
        state: &mut [u8],
        plan: &RemovePlan<I::RemovePlan>,
        cx: &mut KeyCx<'_, I, S>,
    ) -> Result<(), ErrorCode> {
        I::apply_remove(state, self.index(), plan, cx)?;
        self.set_size(state, self.size(state) - 1);
        Ok(())
    }

    /// The raw field-type byte for `field_idx` (descriptor region).
    #[inline(always)]
    pub fn field_type_byte(&self, state: &[u8], field_idx: u8) -> u8 {
        self.rows().field_type_byte(state, field_idx)
    }

    #[inline(always)]
    pub(crate) fn field_type(&self, state: &[u8], field_idx: u8) -> StructFieldType {
        self.rows().field_type(state, field_idx)
    }

    /// Absolute byte offset of row `pos`.
    pub const fn row_off(&self, pos: u32) -> u32 {
        self.rows_base + pos * self.row_size
    }

    /// Byte offset of a field within a row (bitset first, then cumulative
    /// field sizes without padding).
    pub fn field_offset(&self, state: &[u8], field_idx: u8) -> u32 {
        self.rows().field_offset(state, field_idx)
    }

    /// Clear the row's presence bitset without journaling.
    pub fn clear_bitset(&self, state: &mut [u8], row_off: u32) {
        self.rows().clear_bitset(state, row_off);
    }

    /// Borrow a present arena field; see [`StructRows::arena_field`].
    pub fn arena_field<'a>(
        &self,
        state: &'a [u8],
        pos: u32,
        field_idx: u8,
    ) -> Result<Option<&'a [u8]>, ErrorCode> {
        self.rows().arena_field(state, pos, field_idx)
    }

    /// Write one scalar field and its presence bit without journaling; see
    /// [`StructRows::write_scalar_field`].
    pub fn write_scalar_field(
        &self,
        state: &mut [u8],
        pos: u32,
        field_idx: u8,
        cols: &[&[u8]],
        val_col: u32,
        element_idx: u32,
    ) {
        self.rows()
            .write_scalar_field(state, pos, field_idx, cols, val_col, element_idx);
    }
}

/// The [`HashIndex`] instantiation's constructors and raw writers. The row
/// bitset helpers are family-independent; they live on the default view so
/// `StructMapSlot::is_field_set` names them.
impl StructMapSlot {
    /// Bind to an existing [`HashIndex`] struct-map slot via its metadata
    /// record; see [`Self::bind_in`].
    pub fn bind(state: &[u8], slot_idx: u32) -> Self {
        Self::bind_in(state, slot_idx)
    }

    /// Find a key, or return `None`. Sentinel keys are never present.
    pub fn find(&self, state: &[u8], key: u32) -> Option<u32> {
        hash_call(|cx| self.lookup(state, key, cx)).ok().flatten()
    }

    /// Insert or find `key`, tracking size. `None` means sentinel key, probe
    /// exhaustion, or load factor exceeded.
    /// Unjournaled; this raw storage operation does not participate in VM undo.
    pub fn upsert(&self, state: &mut [u8], key: u32) -> Option<Upsert> {
        hash_call(|cx| {
            let plan = self.plan_upsert(state, key, cx).ok()?;
            self.apply_upsert(state, key, &plan, cx).ok()?;
            Some(Upsert {
                pos: plan.ordinal,
                is_new: plan.is_new,
            })
        })
    }

    /// Stamp `key`'s cell as a TOMBSTONE and decrement size, returning the
    /// vacated position. The row payload is left to the caller. Unjournaled;
    /// speculative callers capture the actual bytes before invoking it.
    pub fn remove(&self, state: &mut [u8], key: u32) -> Option<u32> {
        hash_call(|cx| {
            let plan = self.plan_remove(state, key, cx).ok()??;
            self.apply_remove(state, &plan, cx).ok()?;
            Some(plan.ordinal)
        })
    }

    /// Return the absolute byte offset of a key's row, or `0xFFFF_FFFF` when
    /// absent.
    /// The [`HashIndex`] insert-or-update probe: a present key's cell, else
    /// the first tombstone or empty cell its probe meets. `None` for a
    /// sentinel key or an exhausted probe.
    pub fn find_insert(&self, state: &[u8], key: u32) -> Option<Probe> {
        if is_sentinel(key) {
            return None;
        }
        HashIndex::probe_insert(state, self.index(), Key::one(key))
    }

    pub fn get_row_ptr_by_key(&self, state: &[u8], key: u32) -> u32 {
        match self.find(state, key) {
            Some(pos) => self.row_off(pos),
            None => 0xFFFF_FFFF,
        }
    }

    /// Test whether a row field is present in its leading bitset.
    #[inline(always)]
    pub fn is_field_set(state: &[u8], row_off: u32, field_idx: u8) -> bool {
        let byte = state[(row_off + u32::from(field_idx) / 8) as usize];
        byte & (1u8 << (field_idx % 8)) != 0
    }

    /// Set a row field's presence bit without journaling.
    #[inline(always)]
    pub fn set_field_bit(state: &mut [u8], row_off: u32, field_idx: u8) {
        state[(row_off + u32::from(field_idx) / 8) as usize] |= 1u8 << (field_idx % 8);
    }

    //#region reduce-typed-state.scatter-clear
    /// Clear a single field's bit in the bitset (inverse of `set_field_bit`).
    /// Used by BATCH_STRUCT_MAP_PROBE_SCATTER card-one retract to drop one
    /// routed attribute field without disturbing the row's other field bits.
    /// Unjournaled; the caller owns these bytes or already reserved their capture.
    pub fn clear_scalar_field(state: &mut [u8], row_off: u32, field_idx: u8) {
        state[(row_off + u32::from(field_idx) / 8) as usize] &= !(1u8 << (field_idx % 8));
    }
    //#endregion reduce-typed-state.scatter-clear

    /// Read the first four bytes of a row as a u32.
    pub fn read_u32(state: &[u8], row_off: u32) -> u32 {
        bytes::read_u32(state, row_off)
    }
}

/// Bound view over an exact two-lane-key struct map.
///
/// This is a separate slot kind so the one-key [`StructMapSlot`] keeps its
/// one-lane index. Physical layout:
///
/// `[descriptor padded to 8][key index region][rows]`
///
/// Identity always compares both lanes; for a [`HashIndex`] the region is
/// `[key1:u32 × capacity][key2:u32 × capacity]`, lane 1 owning the
/// EMPTY/TOMBSTONE sentinel state and lane 2 preserving every u32.
#[derive(Clone, Copy, Debug)]
pub struct StructMap2Slot<I: KeyIndex = HashIndex> {
    pub slot_offset: u32,
    pub capacity: u32,
    pub size_off: u32,
    pub num_fields: u8,
    pub bitset_bytes: u32,
    pub row_size: u32,
    pub descriptor_size: u32,
    pub field_types_off: u32,
    /// Absolute offset of the key index region (a [`HashIndex`]'s lane 1).
    pub keys1_off: u32,
    /// The key lanes the program declared.
    pub lanes: KeyLanes,
    pub rows_base: u32,
    /// Absolute header offset of the slot's CSR arena; zero when there is none.
    pub arena_header_off: u32,
    family: PhantomData<I>,
}

impl<I: KeyIndex> StructMap2Slot<I> {
    /// Bind a two-key slot of family `I` from its metadata record, unchecked:
    /// the caller has validated the slot's geometry.
    pub fn bind_in(state: &[u8], slot_idx: u32) -> Self {
        let b = bind_meta::<I>(state, slot_idx, true);
        Self {
            slot_offset: b.slot_offset,
            capacity: b.capacity,
            size_off: b.size_off,
            num_fields: b.num_fields,
            bitset_bytes: b.bitset_bytes,
            row_size: b.row_size,
            descriptor_size: b.descriptor_size,
            field_types_off: b.slot_offset,
            keys1_off: b.keys_off,
            lanes: b.lanes,
            rows_base: b.rows_base,
            arena_header_off: b.arena_header_off,
            family: PhantomData,
        }
    }

    /// The slot's index region.
    pub const fn index(&self) -> IndexRegion {
        IndexRegion {
            at: self.keys1_off,
            capacity: self.capacity,
            lanes: self.lanes,
        }
    }

    /// The slot's row geometry.
    pub const fn rows(&self) -> StructRows {
        StructRows {
            capacity: self.capacity,
            num_fields: self.num_fields,
            bitset_bytes: self.bitset_bytes,
            row_size: self.row_size,
            field_types_off: self.field_types_off,
            rows_base: self.rows_base,
            arena_header_off: self.arena_header_off,
        }
    }

    #[inline(always)]
    pub fn size(&self, state: &[u8]) -> u32 {
        bytes::read_u32(state, self.size_off)
    }

    #[inline(always)]
    pub(crate) fn set_size(&self, state: &mut [u8], value: u32) {
        bytes::write_u32(state, self.size_off, value);
    }

    /// The key pair row `pos` holds, or `None` when the row is dead.
    pub fn key_at(&self, state: &[u8], pos: u32) -> Option<(u32, u32)> {
        I::key_at(state, self.index(), pos).map(|key| (key.k1, key.k2))
    }

    /// The ordinal the exact pair owns, or `None`.
    pub fn lookup<S: InternBytes + ?Sized>(
        &self,
        state: &[u8],
        key1: u32,
        key2: u32,
        cx: &mut KeyCx<'_, I, S>,
    ) -> Result<Option<u32>, ErrorCode> {
        if is_sentinel(key1) {
            return Ok(None);
        }
        I::find(state, self.index(), Key::two(key1, key2), cx)
    }

    /// Plan one exact-pair insert-or-update without changing the slot.
    pub fn plan_upsert<S: InternBytes + ?Sized>(
        &self,
        state: &[u8],
        key1: u32,
        key2: u32,
        cx: &mut KeyCx<'_, I, S>,
    ) -> Result<InsertPlan<I::InsertPlan>, ErrorCode> {
        if is_sentinel(key1) {
            return Err(ErrorCode::InvalidKey);
        }
        I::plan_insert(
            state,
            self.index(),
            self.size(state),
            Key::two(key1, key2),
            cx,
        )
    }

    /// Apply the plan used to capture the actual prior bytes; see
    /// [`StructMapSlot::apply_upsert`].
    pub fn apply_upsert<S: InternBytes + ?Sized>(
        &self,
        state: &mut [u8],
        key1: u32,
        key2: u32,
        plan: &InsertPlan<I::InsertPlan>,
        cx: &mut KeyCx<'_, I, S>,
    ) -> Result<(), ErrorCode> {
        if plan.is_new {
            I::apply_insert(state, self.index(), Key::two(key1, key2), plan, cx)?;
            self.set_size(state, self.size(state) + 1);
        }
        Ok(())
    }

    /// Plan removing the exact pair; `None` when it is absent.
    pub fn plan_remove<S: InternBytes + ?Sized>(
        &self,
        state: &[u8],
        key1: u32,
        key2: u32,
        cx: &mut KeyCx<'_, I, S>,
    ) -> Result<Option<RemovePlan<I::RemovePlan>>, ErrorCode> {
        if is_sentinel(key1) {
            return Ok(None);
        }
        I::plan_remove(state, self.index(), Key::two(key1, key2), cx)
    }

    /// Apply a remove plan and shrink the size; see
    /// [`StructMapSlot::apply_remove`].
    pub fn apply_remove<S: InternBytes + ?Sized>(
        &self,
        state: &mut [u8],
        plan: &RemovePlan<I::RemovePlan>,
        cx: &mut KeyCx<'_, I, S>,
    ) -> Result<(), ErrorCode> {
        I::apply_remove(state, self.index(), plan, cx)?;
        self.set_size(state, self.size(state) - 1);
        Ok(())
    }

    /// The first live ordinal, in index order, whose lane 1 is `key1`.
    pub fn first_with_key1<S: InternBytes + ?Sized>(
        &self,
        state: &[u8],
        key1: u32,
        cx: &mut KeyCx<'_, I, S>,
    ) -> Result<Option<u32>, ErrorCode> {
        if is_sentinel(key1) {
            return Ok(None);
        }
        I::first_with_key1(state, self.index(), key1, cx)
    }

    /// The live ordinal after `ordinal`, in index order, whose lane 1 is
    /// `key1`; it stays live when `ordinal` is removed.
    pub fn next_with_key1<S: InternBytes + ?Sized>(
        &self,
        state: &[u8],
        ordinal: u32,
        key1: u32,
        cx: &mut KeyCx<'_, I, S>,
    ) -> Result<Option<u32>, ErrorCode> {
        if is_sentinel(key1) {
            return Ok(None);
        }
        I::next_with_key1(state, self.index(), ordinal, key1, cx)
    }

    pub const fn row_off(&self, pos: u32) -> u32 {
        self.rows_base + pos * self.row_size
    }

    pub fn field_type_byte(&self, state: &[u8], field_idx: u8) -> u8 {
        self.rows().field_type_byte(state, field_idx)
    }

    pub(crate) fn field_type(&self, state: &[u8], field_idx: u8) -> StructFieldType {
        self.rows().field_type(state, field_idx)
    }

    pub fn field_offset(&self, state: &[u8], field_idx: u8) -> u32 {
        self.rows().field_offset(state, field_idx)
    }

    /// Clear the row presence bitset without journaling.
    pub fn clear_bitset(&self, state: &mut [u8], row_off: u32) {
        self.rows().clear_bitset(state, row_off);
    }

    /// Write a scalar field and its presence bit without journaling.
    pub fn write_scalar_field(
        &self,
        state: &mut [u8],
        pos: u32,
        field_idx: u8,
        cols: &[&[u8]],
        val_col: u32,
        element_idx: u32,
    ) {
        self.rows()
            .write_scalar_field(state, pos, field_idx, cols, val_col, element_idx);
    }
}

/// The [`HashIndex`] instantiation's constructor and raw writers.
impl StructMap2Slot {
    /// Bind to an existing [`HashIndex`] two-key slot; see [`Self::bind_in`].
    pub fn bind(state: &[u8], slot_idx: u32) -> Self {
        Self::bind_in(state, slot_idx)
    }

    pub fn find(&self, state: &[u8], key1: u32, key2: u32) -> Option<u32> {
        hash_call(|cx| self.lookup(state, key1, key2, cx))
            .ok()
            .flatten()
    }

    /// Upsert an exact key pair without journaling these caller-owned bytes.
    pub fn upsert(&self, state: &mut [u8], key1: u32, key2: u32) -> Option<Upsert> {
        hash_call(|cx| {
            let plan = self.plan_upsert(state, key1, key2, cx).ok()?;
            self.apply_upsert(state, key1, key2, &plan, cx).ok()?;
            Some(Upsert {
                pos: plan.ordinal,
                is_new: plan.is_new,
            })
        })
    }

    /// Remove an exact key pair without journaling these caller-owned bytes.
    pub fn remove(&self, state: &mut [u8], key1: u32, key2: u32) -> Option<u32> {
        hash_call(|cx| {
            let plan = self.plan_remove(state, key1, key2, cx).ok()??;
            self.apply_remove(state, &plan, cx).ok()?;
            Some(plan.ordinal)
        })
    }

    pub fn get_row_ptr_by_key(&self, state: &[u8], key1: u32, key2: u32) -> u32 {
        self.find(state, key1, key2)
            .map_or(u32::MAX, |pos| self.row_off(pos))
    }

    /// Live exact key pairs and row offsets in ascending cell order.
    pub fn iter<'a>(&self, state: &'a [u8]) -> StructMap2Iter<'a> {
        StructMap2Iter {
            state,
            slot: *self,
            position: 0,
        }
    }
}

/// Allocation-free iterator over live exact key pairs and row offsets.
pub struct StructMap2Iter<'a> {
    state: &'a [u8],
    slot: StructMap2Slot,
    position: u32,
}

impl Iterator for StructMap2Iter<'_> {
    type Item = (u32, u32, u32);

    fn next(&mut self) -> Option<Self::Item> {
        while self.position < self.slot.capacity {
            let pos = self.position;
            self.position += 1;
            if let Some((key1, key2)) = self.slot.key_at(self.state, pos) {
                return Some((key1, key2, self.slot.row_off(pos)));
            }
        }
        None
    }
}
