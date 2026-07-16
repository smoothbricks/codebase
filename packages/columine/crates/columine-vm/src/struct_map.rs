//! Rust port of `packages/columine/src/vm/struct_map.zig` — typed accessor
//! for multi-field hash map rows.
//!
//! Slot data layout (struct_map.zig:5-11):
//!   `[field_type_descriptor: u8 × num_fields (padded to 8)]`
//!   `[keys: u32 × capacity]`
//!   `[rows: row_size × capacity]`
//!
//! Each row: `[bitset: ceil(num_fields/8) bytes][field0][field1]…` with NO
//! padding between fields (`fieldOffset` is a plain cumulative sum). The
//! 4-byte ROW padding lives only in `row_size` itself, which state_init
//! computes via `compute_struct_row_layout_padded` and stores in slot
//! metadata — this module only reads it back.
//!
//! Field sizes: UINT32/STRING 4, INT64/FLOAT64 8, BOOL 1, ARRAY_* 8 in-row
//! (`columine_types::types::struct_field_size`).
//!
//! Probe-semantics note (deliberate, matches Zig): unlike `hash_table.zig`'s
//! `findInsert`, struct_map's probe (struct_map.zig:92-103) does NOT scan
//! past tombstones for the key — a TOMBSTONE cell terminates the probe as an
//! insertion point exactly like EMPTY_KEY. Struct maps have no remove
//! operation, so tombstones cannot occur in well-formed state; the simpler
//! probe is still ported verbatim because the probe sequence is observable
//! ABI the moment a row is placed.

use crate::bytes;
use crate::meta::slot_meta_base;
use columine_types::types::{
    EMPTY_KEY, SlotMetaOffset, StructFieldType, TOMBSTONE, align8, hash_key, hash_key_pair,
    struct_field_size,
};

/// struct_map.zig:92 `findInsert` result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Probe {
    pub pos: u32,
    pub found: bool,
}

/// struct_map.zig:145 `upsert` result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Upsert {
    pub pos: u32,
    pub is_new: bool,
}

/// struct_map.zig:29 `StructMapSlot` — a bound view carrying offsets into the
/// state buffer (this crate never forms pointers into state; see `bytes`).
#[derive(Clone, Copy, Debug)]
pub struct StructMapSlot {
    pub slot_offset: u32,
    pub capacity: u32,
    /// Offset of the u32 size field in slot metadata (Zig `size_ptr`).
    pub size_off: u32,
    pub num_fields: u8,
    pub bitset_bytes: u32,
    pub row_size: u32,
    /// `align8(num_fields)` (struct_map.zig:51).
    pub descriptor_size: u32,
    /// Offset of the field-type descriptor bytes (== `slot_offset`).
    pub field_types_off: u32,
    pub keys_off: u32,
    /// Absolute offset of row 0 (struct_map.zig `rows_base`).
    pub rows_base: u32,
}

impl StructMapSlot {
    /// struct_map.zig:43-67 `bind` — bind to an existing struct map slot via
    /// its metadata record. Metadata byte reuse (pinned by state_init tests):
    /// byte 13 = num_fields, byte 15 = bitset_bytes, bytes 16-17 = row_size.
    pub fn bind(state: &[u8], slot_idx: u8) -> Self {
        let meta_base = slot_meta_base(slot_idx);
        let slot_offset = bytes::read_u32(state, meta_base + SlotMetaOffset::OFFSET);
        let capacity = bytes::read_u32(state, meta_base + SlotMetaOffset::CAPACITY);
        let num_fields = state[(meta_base + SlotMetaOffset::AGG_TYPE) as usize];
        let bitset_bytes =
            u32::from(state[(meta_base + SlotMetaOffset::TIMESTAMP_FIELD_IDX) as usize]);
        let row_size = u32::from(bytes::read_u16(
            state,
            meta_base + SlotMetaOffset::TTL_SECONDS,
        ));

        let descriptor_size = align8(u32::from(num_fields));
        let keys_off = slot_offset + descriptor_size;

        Self {
            slot_offset,
            capacity,
            size_off: meta_base + SlotMetaOffset::SIZE,
            num_fields,
            bitset_bytes,
            row_size,
            descriptor_size,
            field_types_off: slot_offset,
            keys_off,
            rows_base: keys_off + capacity * 4,
        }
    }

    /// struct_map.zig:69 `size`.
    #[inline(always)]
    pub fn size(&self, state: &[u8]) -> u32 {
        bytes::read_u32(state, self.size_off)
    }

    #[inline(always)]
    pub(crate) fn set_size(&self, state: &mut [u8], value: u32) {
        bytes::write_u32(state, self.size_off, value);
    }

    /// struct_map.zig:73 `maxLoad` — 70% integer load factor.
    const fn max_load(&self) -> u32 {
        self.capacity * 7 / 10
    }

    #[inline(always)]
    pub(crate) fn key_at(&self, state: &[u8], pos: u32) -> u32 {
        bytes::read_u32(state, self.keys_off + pos * 4)
    }

    /// Raw key-cell write (vm.zig rollback arms stamp TOMBSTONE directly).
    #[inline(always)]
    pub(crate) fn set_key_at(&self, state: &mut [u8], pos: u32, key: u32) {
        bytes::write_u32(state, self.keys_off + pos * 4, key);
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

    /// struct_map.zig:78 `find` — key lookup, or None. Sentinel keys are
    /// never present.
    pub fn find(&self, state: &[u8], key: u32) -> Option<u32> {
        if key == EMPTY_KEY || key == TOMBSTONE {
            return None;
        }
        debug_assert!(
            self.capacity.is_power_of_two(),
            "probe mask requires pow2 cap"
        );
        let mut pos = hash_key(key, self.capacity);
        for _ in 0..self.capacity {
            let k = self.key_at(state, pos);
            if k == key {
                return Some(pos);
            }
            if k == EMPTY_KEY {
                return None;
            }
            pos = (pos + 1) & (self.capacity - 1);
        }
        None
    }

    /// struct_map.zig:92 `findInsert` — insert-or-update probe, unified on
    /// the hash_table policy post-parity: probe for an existing key first,
    /// reusing the first tombstone seen. (The deleted Zig claimed a
    /// tombstone immediately, so an insert could shadow a live copy of the
    /// same key sitting past it.)
    pub fn find_insert(&self, state: &[u8], key: u32) -> Option<Probe> {
        if key == EMPTY_KEY || key == TOMBSTONE {
            return None;
        }
        debug_assert!(
            self.capacity.is_power_of_two(),
            "probe mask requires pow2 cap"
        );
        let mut pos = hash_key(key, self.capacity);
        let mut first_tombstone: Option<u32> = None;
        for _ in 0..self.capacity {
            let k = self.key_at(state, pos);
            if k == key {
                return Some(Probe { pos, found: true });
            }
            if k == TOMBSTONE {
                if first_tombstone.is_none() {
                    first_tombstone = Some(pos);
                }
            } else if k == EMPTY_KEY {
                return Some(Probe {
                    pos: first_tombstone.unwrap_or(pos),
                    found: false,
                });
            }
            pos = (pos + 1) & (self.capacity - 1);
        }
        first_tombstone.map(|pos| Probe { pos, found: false })
    }

    /// struct_map.zig:106 `rowPtr` — absolute byte offset of the row at hash
    /// position `pos` (the Zig version returns a pointer).
    pub const fn row_off(&self, pos: u32) -> u32 {
        self.rows_base + pos * self.row_size
    }

    /// struct_map.zig:111 `fieldOffset` — byte offset of a field within a
    /// row (bitset first, then a plain cumulative sum — no padding).
    pub fn field_offset(&self, state: &[u8], field_idx: u8) -> u32 {
        let mut off = self.bitset_bytes;
        for i in 0..field_idx {
            off += struct_field_size(self.field_type(state, i));
        }
        off
    }

    /// struct_map.zig:121 `isFieldSet` — `row_off` is the absolute offset of
    /// the row (bitset lives at its start).
    #[inline(always)]
    pub fn is_field_set(state: &[u8], row_off: u32, field_idx: u8) -> bool {
        let byte = state[(row_off + u32::from(field_idx) / 8) as usize];
        byte & (1u8 << (field_idx % 8)) != 0
    }

    /// struct_map.zig:126 `setFieldBit`.
    #[inline(always)]
    pub fn set_field_bit(state: &mut [u8], row_off: u32, field_idx: u8) {
        state[(row_off + u32::from(field_idx) / 8) as usize] |= 1u8 << (field_idx % 8);
    }

    //#region axe!n/reduce-typed-state.scatter-clear
    /// Clear a single field's bit in the bitset (inverse of `set_field_bit`).
    /// Used by BATCH_STRUCT_MAP_PROBE_SCATTER card-one retract to drop one
    /// routed attribute field without disturbing the row's other field bits.
    pub fn clear_scalar_field(state: &mut [u8], row_off: u32, field_idx: u8) {
        state[(row_off + u32::from(field_idx) / 8) as usize] &= !(1u8 << (field_idx % 8));
    }
    //#endregion axe!n/reduce-typed-state.scatter-clear

    /// struct_map.zig:140 `clearBitset`.
    pub fn clear_bitset(&self, state: &mut [u8], row_off: u32) {
        bytes::zero(state, row_off, self.bitset_bytes);
    }

    /// struct_map.zig:145 `upsert` — insert or find `key`, tracking size.
    /// None = sentinel key, probe exhaustion, or load factor exceeded.
    pub fn upsert(&self, state: &mut [u8], key: u32) -> Option<Upsert> {
        let probe = self.find_insert(state, key)?;
        if !probe.found {
            if self.size(state) >= self.max_load() {
                return None;
            }
            bytes::write_u32(state, self.keys_off + probe.pos * 4, key);
            let size = self.size(state);
            self.set_size(state, size + 1);
        }
        Some(Upsert {
            pos: probe.pos,
            is_new: !probe.found,
        })
    }

    /// struct_map.zig:157 `writeScalarField` — write one scalar field from a
    /// column into the row at `pos`, setting its bitset bit. Columns are raw
    /// LE cell arrays exactly like the Zig `col_ptrs` (u32 cells for
    /// UINT32/STRING/BOOL, u64/f64 cells for INT64/FLOAT64). ARRAY_* fields
    /// are handled separately by the arena path, matching the Zig no-op.
    pub fn write_scalar_field(
        &self,
        state: &mut [u8],
        pos: u32,
        field_idx: u8,
        cols: &[&[u8]],
        val_col: u8,
        element_idx: u32,
    ) {
        let row = self.row_off(pos);
        let ft = self.field_type(state, field_idx);
        let f_off = row + self.field_offset(state, field_idx);
        let col = cols[val_col as usize];

        Self::set_field_bit(state, row, field_idx);

        match ft {
            StructFieldType::UInt32 | StructFieldType::String => {
                let v = bytes::read_u32(col, element_idx * 4);
                bytes::write_u32(state, f_off, v);
            }
            StructFieldType::Int64 => {
                let v = bytes::read_u64(col, element_idx * 8);
                bytes::write_u64(state, f_off, v);
            }
            StructFieldType::Float64 => {
                // Zig `@bitCast`s the f64 to u64 before the LE write; reading
                // and writing the raw 8 bytes is the same operation.
                let v = bytes::read_u64(col, element_idx * 8);
                bytes::write_u64(state, f_off, v);
            }
            StructFieldType::Bool => {
                let v = bytes::read_u32(col, element_idx * 4);
                state[f_off as usize] = u8::from(v != 0);
            }
            // Array fields handled separately (struct_map.zig:190).
            StructFieldType::ArrayU32
            | StructFieldType::ArrayI64
            | StructFieldType::ArrayF64
            | StructFieldType::ArrayString
            | StructFieldType::ArrayBool => {}
        }
    }

    /// struct_map.zig:195 `getRowPtrByKey` — absolute byte offset of the
    /// key's row, or `0xFFFF_FFFF` when absent (the TS-visible sentinel).
    pub fn get_row_ptr_by_key(&self, state: &[u8], key: u32) -> u32 {
        match self.find(state, key) {
            Some(pos) => self.rows_base + pos * self.row_size,
            None => 0xFFFF_FFFF,
        }
    }

    /// struct_map.zig:201 `readU32` — reads the first 4 row bytes. Ported
    /// verbatim: the Zig version ignores `field_idx` (callers pass a row
    /// pointer already advanced to the field; see its own comment).
    pub fn read_u32(state: &[u8], row_off: u32) -> u32 {
        bytes::read_u32(state, row_off)
    }
}

/// Bound view over an exact two-lane-key struct map.
///
/// This is a separate slot kind so the unary [`StructMapSlot`] keeps its
/// compact descriptor/keys/rows layout and unchanged probe path. Physical
/// layout:
///
/// `[descriptor padded to 8][key1:u32 × capacity][key2:u32 × capacity][rows]`
///
/// Lane 1 owns the EMPTY/TOMBSTONE sentinel state; lane 2 preserves all u32
/// values. A hash chooses only the initial probe position. Identity always
/// compares both lanes.
#[derive(Clone, Copy, Debug)]
pub struct StructMap2Slot {
    pub slot_offset: u32,
    pub capacity: u32,
    pub size_off: u32,
    pub num_fields: u8,
    pub bitset_bytes: u32,
    pub row_size: u32,
    pub descriptor_size: u32,
    pub field_types_off: u32,
    pub keys1_off: u32,
    pub keys2_off: u32,
    pub rows_base: u32,
}

impl StructMap2Slot {
    pub fn bind(state: &[u8], slot_idx: u8) -> Self {
        let meta_base = slot_meta_base(slot_idx);
        let slot_offset = bytes::read_u32(state, meta_base + SlotMetaOffset::OFFSET);
        let capacity = bytes::read_u32(state, meta_base + SlotMetaOffset::CAPACITY);
        let num_fields = state[(meta_base + SlotMetaOffset::AGG_TYPE) as usize];
        let bitset_bytes =
            u32::from(state[(meta_base + SlotMetaOffset::TIMESTAMP_FIELD_IDX) as usize]);
        let row_size = u32::from(bytes::read_u16(
            state,
            meta_base + SlotMetaOffset::TTL_SECONDS,
        ));
        let descriptor_size = align8(u32::from(num_fields));
        let keys1_off = slot_offset + descriptor_size;
        let keys2_off = keys1_off + capacity * 4;
        Self {
            slot_offset,
            capacity,
            size_off: meta_base + SlotMetaOffset::SIZE,
            num_fields,
            bitset_bytes,
            row_size,
            descriptor_size,
            field_types_off: slot_offset,
            keys1_off,
            keys2_off,
            rows_base: keys2_off + capacity * 4,
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

    const fn max_load(&self) -> u32 {
        self.capacity * 7 / 10
    }

    #[inline(always)]
    pub fn key1_at(&self, state: &[u8], pos: u32) -> u32 {
        bytes::read_u32(state, self.keys1_off + pos * 4)
    }

    #[inline(always)]
    pub fn key2_at(&self, state: &[u8], pos: u32) -> u32 {
        bytes::read_u32(state, self.keys2_off + pos * 4)
    }

    #[inline(always)]
    pub(crate) fn set_keys_at(&self, state: &mut [u8], pos: u32, key1: u32, key2: u32) {
        bytes::write_u32(state, self.keys1_off + pos * 4, key1);
        bytes::write_u32(state, self.keys2_off + pos * 4, key2);
    }

    #[inline(always)]
    pub(crate) fn tombstone_at(&self, state: &mut [u8], pos: u32) {
        bytes::write_u32(state, self.keys1_off + pos * 4, TOMBSTONE);
    }

    pub fn find(&self, state: &[u8], key1: u32, key2: u32) -> Option<u32> {
        if key1 == EMPTY_KEY || key1 == TOMBSTONE {
            return None;
        }
        debug_assert!(self.capacity.is_power_of_two());
        let mut pos = hash_key_pair(key1, key2, self.capacity);
        for _ in 0..self.capacity {
            let first = self.key1_at(state, pos);
            if first == key1 && self.key2_at(state, pos) == key2 {
                return Some(pos);
            }
            if first == EMPTY_KEY {
                return None;
            }
            pos = (pos + 1) & (self.capacity - 1);
        }
        None
    }

    pub fn find_insert(&self, state: &[u8], key1: u32, key2: u32) -> Option<Probe> {
        if key1 == EMPTY_KEY || key1 == TOMBSTONE {
            return None;
        }
        debug_assert!(self.capacity.is_power_of_two());
        let mut pos = hash_key_pair(key1, key2, self.capacity);
        let mut first_tombstone = None;
        for _ in 0..self.capacity {
            let first = self.key1_at(state, pos);
            if first == key1 && self.key2_at(state, pos) == key2 {
                return Some(Probe { pos, found: true });
            }
            if first == TOMBSTONE {
                if first_tombstone.is_none() {
                    first_tombstone = Some(pos);
                }
            } else if first == EMPTY_KEY {
                return Some(Probe {
                    pos: first_tombstone.unwrap_or(pos),
                    found: false,
                });
            }
            pos = (pos + 1) & (self.capacity - 1);
        }
        first_tombstone.map(|pos| Probe { pos, found: false })
    }

    pub fn upsert(&self, state: &mut [u8], key1: u32, key2: u32) -> Option<Upsert> {
        let probe = self.find_insert(state, key1, key2)?;
        if !probe.found {
            if self.size(state) >= self.max_load() {
                return None;
            }
            self.set_keys_at(state, probe.pos, key1, key2);
            self.set_size(state, self.size(state) + 1);
        }
        Some(Upsert {
            pos: probe.pos,
            is_new: !probe.found,
        })
    }

    pub fn remove(&self, state: &mut [u8], key1: u32, key2: u32) -> Option<u32> {
        let pos = self.find(state, key1, key2)?;
        self.tombstone_at(state, pos);
        self.set_size(state, self.size(state) - 1);
        Some(pos)
    }

    pub const fn row_off(&self, pos: u32) -> u32 {
        self.rows_base + pos * self.row_size
    }

    pub fn field_type_byte(&self, state: &[u8], field_idx: u8) -> u8 {
        state[(self.field_types_off + u32::from(field_idx)) as usize]
    }

    pub(crate) fn field_type(&self, state: &[u8], field_idx: u8) -> StructFieldType {
        StructFieldType::from_u8(self.field_type_byte(state, field_idx)).unwrap_or_else(|| {
            columine_types::die!("invariant: struct-map2 descriptor contains invalid field type")
        })
    }

    pub fn field_offset(&self, state: &[u8], field_idx: u8) -> u32 {
        let mut off = self.bitset_bytes;
        for i in 0..field_idx {
            off += struct_field_size(self.field_type(state, i));
        }
        off
    }

    pub fn clear_bitset(&self, state: &mut [u8], row_off: u32) {
        bytes::zero(state, row_off, self.bitset_bytes);
    }

    pub fn write_scalar_field(
        &self,
        state: &mut [u8],
        pos: u32,
        field_idx: u8,
        cols: &[&[u8]],
        val_col: u8,
        element_idx: u32,
    ) {
        let row = self.row_off(pos);
        let ft = self.field_type(state, field_idx);
        let f_off = row + self.field_offset(state, field_idx);
        let col = cols[val_col as usize];
        StructMapSlot::set_field_bit(state, row, field_idx);
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
            | StructFieldType::ArrayBool => {}
        }
    }

    pub fn get_row_ptr_by_key(&self, state: &[u8], key1: u32, key2: u32) -> u32 {
        self.find(state, key1, key2)
            .map_or(u32::MAX, |pos| self.row_off(pos))
    }

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
            let key1 = self.slot.key1_at(self.state, pos);
            if key1 != EMPTY_KEY && key1 != TOMBSTONE {
                return Some((
                    key1,
                    self.slot.key2_at(self.state, pos),
                    self.slot.row_off(pos),
                ));
            }
        }
        None
    }
}
