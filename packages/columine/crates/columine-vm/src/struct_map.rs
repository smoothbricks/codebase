//! Typed accessor for multi-field hash-map rows.
//!
//! Slot data layout:
//! `[field_type_descriptor: u8 × num_fields (padded to 8)]`
//! `[keys: u32 × capacity]` `[rows: row_size × capacity]`
//! `[timestamps: f64 × capacity]?`.
//!
//! Struct-map probing scans past tombstones and reuses the first tombstone
//! only when the key is absent. Struct maps have no remove operation, so
//! tombstones are not expected in a well-formed state.

use crate::bytes;
use crate::hash_table::{
    Probe, ProbeCell, find_key, find_key_pair, max_load_for_cap, probe_linear,
};
use crate::meta::slot_meta_base;
use columine_types::types::{
    EMPTY_KEY, SlotMetaOffset, StructFieldType, TOMBSTONE, align8, hash_key, hash_key_pair,
    struct_field_size,
};

/// Result of an `upsert` operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Upsert {
    pub pos: u32,
    pub is_new: bool,
}

/// Bound struct-map view carrying offsets into the state buffer.
/// No pointers into state are formed.
#[derive(Clone, Copy, Debug)]
pub struct StructMapSlot {
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
    pub keys_off: u32,
    /// Absolute offset of row zero.
    pub rows_base: u32,
}

impl StructMapSlot {
    /// Bind to an existing struct-map slot via its metadata record. Metadata
    /// byte reuse: byte 13 = num_fields, byte 15 = bitset_bytes, bytes 16–17
    /// = row_size.
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

    /// Current element count.
    #[inline(always)]
    pub fn size(&self, state: &[u8]) -> u32 {
        bytes::read_u32(state, self.size_off)
    }

    #[inline(always)]
    pub(crate) fn set_size(&self, state: &mut [u8], value: u32) {
        bytes::write_u32(state, self.size_off, value);
    }

    /// Maximum size before growth, using a 70% integer load factor.
    const fn max_load(&self) -> u32 {
        max_load_for_cap(self.capacity)
    }

    #[inline(always)]
    pub(crate) fn key_at(&self, state: &[u8], pos: u32) -> u32 {
        bytes::read_u32(state, self.keys_off + pos * 4)
    }

    /// Raw key-cell write used by rollback to stamp a TOMBSTONE.
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

    /// Find a key, or return `None`. Sentinel keys are never present.
    pub fn find(&self, state: &[u8], key: u32) -> Option<u32> {
        find_key(state, self.keys_off, self.capacity, key)
    }

    /// Insert-or-update probe: find an existing key before reusing the first
    /// tombstone.
    pub fn find_insert(&self, state: &[u8], key: u32) -> Option<Probe> {
        if key == EMPTY_KEY || key == TOMBSTONE {
            return None;
        }
        probe_linear(
            self.capacity,
            hash_key(key, self.capacity),
            true,
            |pos| match self.key_at(state, pos) {
                current if current == key => ProbeCell::Match,
                EMPTY_KEY => ProbeCell::Empty,
                TOMBSTONE => ProbeCell::Tombstone,
                _ => ProbeCell::Occupied,
            },
        )
    }

    /// Absolute byte offset of row `pos`.
    pub const fn row_off(&self, pos: u32) -> u32 {
        self.rows_base + pos * self.row_size
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

    /// Test whether a row field is present in its leading bitset.
    #[inline(always)]
    pub fn is_field_set(state: &[u8], row_off: u32, field_idx: u8) -> bool {
        let byte = state[(row_off + u32::from(field_idx) / 8) as usize];
        byte & (1u8 << (field_idx % 8)) != 0
    }

    /// Set a row field's presence bit.
    #[inline(always)]
    pub fn set_field_bit(state: &mut [u8], row_off: u32, field_idx: u8) {
        state[(row_off + u32::from(field_idx) / 8) as usize] |= 1u8 << (field_idx % 8);
    }

    //#region reduce-typed-state.scatter-clear
    /// Clear a single field's bit in the bitset (inverse of `set_field_bit`).
    /// Used by BATCH_STRUCT_MAP_PROBE_SCATTER card-one retract to drop one
    /// routed attribute field without disturbing the row's other field bits.
    pub fn clear_scalar_field(state: &mut [u8], row_off: u32, field_idx: u8) {
        state[(row_off + u32::from(field_idx) / 8) as usize] &= !(1u8 << (field_idx % 8));
    }
    //#endregion reduce-typed-state.scatter-clear

    /// Clear the row's presence bitset.
    pub fn clear_bitset(&self, state: &mut [u8], row_off: u32) {
        bytes::zero(state, row_off, self.bitset_bytes);
    }

    /// Insert or find `key`, tracking size. `None` means sentinel key, probe
    /// exhaustion, or load factor exceeded.
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

    /// Stamp `key`'s cell as a TOMBSTONE and decrement size, returning the
    /// vacated position. The row payload is left to the caller (rollback
    /// clears the bitset); the same shape as [`StructMap2Slot::remove`].
    pub fn remove(&self, state: &mut [u8], key: u32) -> Option<u32> {
        let pos = self.find(state, key)?;
        self.set_key_at(state, pos, TOMBSTONE);
        self.set_size(state, self.size(state) - 1);
        Some(pos)
    }

    /// Write one scalar field from a raw little-endian column into the row,
    /// setting its bitset bit. Array fields are handled by the arena path.
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
                // Preserve the f64 bit pattern as its raw little-endian bytes.
                let v = bytes::read_u64(col, element_idx * 8);
                bytes::write_u64(state, f_off, v);
            }
            StructFieldType::Bool => {
                let v = bytes::read_u32(col, element_idx * 4);
                state[f_off as usize] = u8::from(v != 0);
            }
            // Array fields are handled separately.
            StructFieldType::ArrayU32
            | StructFieldType::ArrayI64
            | StructFieldType::ArrayF64
            | StructFieldType::ArrayString
            | StructFieldType::ArrayBool => {}
        }
    }

    /// Return the absolute byte offset of a key's row, or `0xFFFF_FFFF` when
    /// absent.
    pub fn get_row_ptr_by_key(&self, state: &[u8], key: u32) -> u32 {
        match self.find(state, key) {
            Some(pos) => self.rows_base + pos * self.row_size,
            None => 0xFFFF_FFFF,
        }
    }

    /// Read the first four bytes of a row as a u32.
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
        max_load_for_cap(self.capacity)
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
        find_key_pair(
            state,
            self.keys1_off,
            self.keys2_off,
            self.capacity,
            key1,
            key2,
        )
    }

    pub fn find_insert(&self, state: &[u8], key1: u32, key2: u32) -> Option<Probe> {
        if key1 == EMPTY_KEY || key1 == TOMBSTONE {
            return None;
        }
        probe_linear(
            self.capacity,
            hash_key_pair(key1, key2, self.capacity),
            true,
            |pos| {
                let first = self.key1_at(state, pos);
                if first == key1 && self.key2_at(state, pos) == key2 {
                    ProbeCell::Match
                } else {
                    match first {
                        EMPTY_KEY => ProbeCell::Empty,
                        TOMBSTONE => ProbeCell::Tombstone,
                        _ => ProbeCell::Occupied,
                    }
                }
            },
        )
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
