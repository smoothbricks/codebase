//! Typed rollback helpers over `FlatTable`.
//!
//! Bitmap and TTL rollback remain in the VM module; this module owns the
//! container rollback operations and the serialized undo-entry contract.
//! `FlatUndoOp`/`FlatUndoEntry` are explicit byte layouts used by exported
//! delta and fork segments. In-memory rings, overflow snapshots, change-flag
//! save/restore, and delta export assembly remain VM-owned.

use crate::bytes;
use crate::hash_table::{ENTRY_NONE, ENTRY_U32, FlatTable};
use crate::meta::SlotMetaView;
use columine_types::types::SlotMetaOffset;

/// Operation byte of a serialized undo entry. Discriminants are wire values
/// inside exported delta/fork segments.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlatUndoOp {
    /// Rollback: tombstone key, decrement size.
    MapInsert = 1,
    /// Rollback: restore prev value + timestamp.
    MapUpdate = 2,
    /// Rollback: restore key + value + timestamp, increment size.
    MapDelete = 3,
    /// Rollback: tombstone elem, decrement size.
    SetInsert = 4,
    /// Rollback: restore elem, increment size.
    SetDelete = 5,
    /// Rollback: restore prev value (f64/i64) + count (u64) — 16-byte slots.
    AggUpdate = 6,
    /// Rollback: tombstone the derived-fact identity at `prev_value`.
    FactInsertNew = 7,
    /// Restore a derived-fact identity and value at `prev_value`.
    FactInsertUpdate = 8,
    /// Restore a retracted derived-fact identity and value at `prev_value`.
    FactRetract = 9,
    /// Rollback: restore list count to prev_value.
    ListAppendUndo = 10,
    /// Rollback: restore prev count (u64) — 8-byte COUNT slot.
    CountUpdate = 11,
    /// Restore a scalar slot's value and comparison timestamp.
    /// Scalar-update is an extension; the frozen ABI did not journal scalar writes.
    ScalarUpdate = 14,
    /// Restore `pad1` (1..=8) raw bytes at absolute state offset `key` from
    /// `aux`'s little-endian bytes. Used as paired before/after entries for
    /// nested arenas and variable payloads without widening this 24-byte ABI.
    StateBytes = 15,
    /// Roll back one struct-map scalar field or remove a newly created row.
    /// `_pad1` is the field index, `_pad2` carries flags, and `aux` stores the
    /// field bytes in little-endian order.
    StructMapField = 12,
    /// Whole-row rollback for struct-map upserts that clear the row bitset.
    /// `_pad2 = 0x02` marks an absent row; `aux` stores up to eight bitset bytes.
    StructMapRow = 13,
}

/// Field-present flag in a `STRUCT_MAP_FIELD` entry.
pub const SMF_BIT_SET: u8 = 0x01;
/// Row-absent flag shared by struct-map rollback entries.
pub const SMF_ROW_ABSENT: u8 = 0x02;
/// `STRUCT_MAP_FIELD`/`STRUCT_MAP_ROW` flags. Both row-absence flags use
/// `0x02` deliberately, so rollback can share the same removal test.
pub const SMR_ROW_ABSENT: u8 = 0x02;

/// One serialized undo/redo entry. Layout: op@0, slot@1, `_pad1`@2,
/// `_pad2`@3, key@4, prev_value@8, four padding bytes, aux@16; size 24.
/// `_pad1`/`_pad2` carry field index and flags for struct-map entries.
/// Following crate convention the struct is plain Rust; the byte contract
/// lives in [`FlatUndoEntry::write_to`] / [`FlatUndoEntry::read_from`], pinned
/// by layout tests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlatUndoEntry {
    pub op: FlatUndoOp,
    /// Slot index (0xFF = derived fact); ignored by STATE_BYTES.
    pub slot: u8,
    /// STRUCT_MAP_FIELD: field_idx. FACT_*: fact_idx low. STATE_BYTES: length.
    pub pad1: u8,
    /// STRUCT_MAP_*: SMF/SMR flags. FACT_*: fact_idx high byte.
    pub pad2: u8,
    /// Container/fact key, or absolute state offset for STATE_BYTES.
    pub key: u32,
    /// Previous value, physical fact slot, or zero for STATE_BYTES.
    pub prev_value: u32,
    /// Previous timestamp bits, target/restored fact value, field/bitset bytes,
    /// or raw STATE_BYTES payload — per-op documented above.
    pub aux: u64,
}

/// Serialized size of one undo entry.
pub const FLAT_UNDO_ENTRY_SIZE: u32 = 24;

/// One undo/redo pair occupying 48 serialized bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlatDeltaEntry {
    pub undo: FlatUndoEntry,
    pub redo: FlatUndoEntry,
}

/// Serialized size of a delta pair.
pub const FLAT_DELTA_ENTRY_SIZE: u32 = 48;

impl FlatUndoEntry {
    /// Serialize with the extern-struct layout (see type doc). The four
    /// padding bytes at 12..16 are written as zero.
    pub fn write_to(&self, out: &mut [u8; FLAT_UNDO_ENTRY_SIZE as usize]) {
        out.fill(0);
        out[0] = self.op as u8;
        out[1] = self.slot;
        out[2] = self.pad1;
        out[3] = self.pad2;
        out[4..8].copy_from_slice(&self.key.to_le_bytes());
        out[8..12].copy_from_slice(&self.prev_value.to_le_bytes());
        out[16..24].copy_from_slice(&self.aux.to_le_bytes());
    }

    /// Deserialize; `None` if the op byte is not a `FlatUndoOp` (a corrupt
    /// or foreign segment — an operational condition for segment consumers,
    /// not a programmer bug).
    pub fn read_from(buf: &[u8; FLAT_UNDO_ENTRY_SIZE as usize]) -> Option<Self> {
        let op = match buf[0] {
            1 => FlatUndoOp::MapInsert,
            2 => FlatUndoOp::MapUpdate,
            3 => FlatUndoOp::MapDelete,
            4 => FlatUndoOp::SetInsert,
            5 => FlatUndoOp::SetDelete,
            6 => FlatUndoOp::AggUpdate,
            7 => FlatUndoOp::FactInsertNew,
            8 => FlatUndoOp::FactInsertUpdate,
            9 => FlatUndoOp::FactRetract,
            10 => FlatUndoOp::ListAppendUndo,
            11 => FlatUndoOp::CountUpdate,
            14 => FlatUndoOp::ScalarUpdate,
            12 => FlatUndoOp::StructMapField,
            13 => FlatUndoOp::StructMapRow,
            15 => FlatUndoOp::StateBytes,
            _ => return None,
        };
        Some(Self {
            op,
            slot: buf[1],
            pad1: buf[2],
            pad2: buf[3],
            key: u32::from_le_bytes(buf[4..8].try_into().expect("4-byte slice")),
            prev_value: u32::from_le_bytes(buf[8..12].try_into().expect("4-byte slice")),
            aux: u64::from_le_bytes(buf[16..24].try_into().expect("8-byte slice")),
        })
    }
}

/// Bind the HASHMAP table used by rollback.
fn bind_map(meta: &SlotMetaView) -> FlatTable {
    FlatTable::bind_external(
        meta.offset,
        meta.capacity,
        meta.meta_base + SlotMetaOffset::SIZE,
        ENTRY_U32,
    )
}

/// Bind the HASHSET table used by rollback.
fn bind_set(meta: &SlotMetaView) -> FlatTable {
    FlatTable::bind_external(
        meta.offset,
        meta.capacity,
        meta.meta_base + SlotMetaOffset::SIZE,
        ENTRY_NONE,
    )
}

/// Offset of a HASHMAP timestamp lane: one u64 (f64 bits) per position.
fn ts_off(meta: &SlotMetaView, pos: u32) -> u32 {
    meta.offset + meta.capacity * 8 + pos * 8
}

// Rollback is logical rather than byte-exact: inserts leave TOMBSTONE keys,
// and dead cells retain stale value/timestamp bytes outside the logical table.
/// Roll back a map insertion by writing a tombstone and decrementing size.
pub fn rollback_map_insert(state: &mut [u8], meta: &SlotMetaView, key: u32) -> bool {
    let tbl = bind_map(meta);
    let Some(pos) = tbl.find(state, key) else {
        return false;
    };
    tbl.set_key_at(state, pos, columine_types::types::TOMBSTONE);
    let size = tbl.size(state);
    tbl.set_size(state, size - 1);
    true
}

/// Restore a map value and optional timestamp from raw u64 bits.
pub fn rollback_map_update(
    state: &mut [u8],
    meta: &SlotMetaView,
    key: u32,
    prev_value: u32,
    prev_ts_bits: u64,
) -> bool {
    let tbl = bind_map(meta);
    let Some(pos) = tbl.find(state, key) else {
        return false;
    };
    tbl.set_entry_u32_at(state, pos, prev_value);
    if meta.has_hashmap_timestamp_storage() {
        bytes::write_u64(state, ts_off(meta, pos), prev_ts_bits);
    }
    true
}

/// Restore a deleted map key, value, and optional timestamp at its probe
/// position, incrementing size. Return false when the key is already present
/// or the table has no insertion position.
pub fn rollback_map_delete(
    state: &mut [u8],
    meta: &SlotMetaView,
    key: u32,
    prev_value: u32,
    prev_ts_bits: u64,
) -> bool {
    let tbl = bind_map(meta);
    let Some(probe) = tbl.find_insert(state, key) else {
        return false;
    };
    if probe.found {
        return false;
    }
    tbl.set_key_at(state, probe.pos, key);
    tbl.set_entry_u32_at(state, probe.pos, prev_value);
    if meta.has_hashmap_timestamp_storage() {
        bytes::write_u64(state, ts_off(meta, probe.pos), prev_ts_bits);
    }
    let size = tbl.size(state);
    tbl.set_size(state, size + 1);
    true
}

/// Restore the previous value bits and full u64 count of a 16-byte aggregate
/// slot (`[value: u64][count: u64]`).
// The count uses prev_value (low) and key (high) lanes so values above u32::MAX
// remain lossless.
pub fn rollback_agg_update(
    state: &mut [u8],
    meta: &SlotMetaView,
    prev_count: u64,
    prev_val_bits: u64,
) {
    bytes::write_u64(state, meta.offset, prev_val_bits);
    bytes::write_u64(state, meta.offset + 8, prev_count);
}

/// Restore the count of an 8-byte COUNT-only slot.
// Full u64 count (prev_value low + key high lanes) — see rollback_agg_update.
pub fn rollback_count_update(state: &mut [u8], meta: &SlotMetaView, prev_count: u64) {
    bytes::write_u64(state, meta.offset, prev_count);
}

/// Restore a scalar slot's value bytes and comparison timestamp.
pub fn rollback_scalar_update(state: &mut [u8], meta: &SlotMetaView, value: u64, ts: f64) {
    bytes::write_u64(state, meta.offset, value);
    bytes::write_f64(state, meta.offset + 8, ts);
}

/// Roll back a set insertion by writing a tombstone and decrementing size.
pub fn rollback_set_insert(state: &mut [u8], meta: &SlotMetaView, elem: u32) -> bool {
    let tbl = bind_set(meta);
    let Some(pos) = tbl.find(state, elem) else {
        return false;
    };
    tbl.set_key_at(state, pos, columine_types::types::TOMBSTONE);
    let size = tbl.size(state);
    tbl.set_size(state, size - 1);
    true
}

/// Restore a deleted set element and increment size.
pub fn rollback_set_delete(state: &mut [u8], meta: &SlotMetaView, elem: u32) -> bool {
    let tbl = bind_set(meta);
    let Some(probe) = tbl.find_insert(state, elem) else {
        return false;
    };
    if probe.found {
        return false;
    }
    tbl.set_key_at(state, probe.pos, elem);
    let size = tbl.size(state);
    tbl.set_size(state, size + 1);
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The serialized layout is the contract: op@0, slot@1, `_pad1`@2,
    /// `_pad2`@3, key@4 LE, prev_value@8 LE, zero padding 12..16, aux@16 LE.
    #[test]
    fn flat_undo_entry_serialized_layout_is_pinned() {
        let entry = FlatUndoEntry {
            op: FlatUndoOp::StructMapField,
            slot: 7,
            pad1: 3,
            pad2: SMF_BIT_SET,
            key: 0x11223344,
            prev_value: 0x55667788,
            aux: 0x99aabbccddeeff00,
        };
        let mut buf = [0xa5u8; 24];
        entry.write_to(&mut buf);
        assert_eq!(buf[0], 12); // StructMapField discriminant
        assert_eq!(buf[1], 7);
        assert_eq!(buf[2], 3);
        assert_eq!(buf[3], 0x01);
        assert_eq!(&buf[4..8], &0x11223344u32.to_le_bytes());
        assert_eq!(&buf[8..12], &0x55667788u32.to_le_bytes());
        assert_eq!(&buf[12..16], &[0, 0, 0, 0]); // alignment padding zeroed
        assert_eq!(&buf[16..24], &0x99aabbccddeeff00u64.to_le_bytes());
        assert_eq!(FlatUndoEntry::read_from(&buf), Some(entry));
        assert_eq!(FLAT_UNDO_ENTRY_SIZE, 24);
        assert_eq!(FLAT_DELTA_ENTRY_SIZE, 48);
    }

    /// Every FlatUndoOp discriminant matches the serialized ABI, including the
    /// deliberate gap between LIST_APPEND_UNDO=10 and COUNT_UPDATE=11.
    #[test]
    fn flat_undo_op_discriminants_match_contract() {
        assert_eq!(FlatUndoOp::MapInsert as u8, 1);
        assert_eq!(FlatUndoOp::MapUpdate as u8, 2);
        assert_eq!(FlatUndoOp::MapDelete as u8, 3);
        assert_eq!(FlatUndoOp::SetInsert as u8, 4);
        assert_eq!(FlatUndoOp::SetDelete as u8, 5);
        assert_eq!(FlatUndoOp::AggUpdate as u8, 6);
        assert_eq!(FlatUndoOp::FactInsertNew as u8, 7);
        assert_eq!(FlatUndoOp::FactInsertUpdate as u8, 8);
        assert_eq!(FlatUndoOp::FactRetract as u8, 9);
        assert_eq!(FlatUndoOp::ListAppendUndo as u8, 10);
        assert_eq!(FlatUndoOp::CountUpdate as u8, 11);
        assert_eq!(FlatUndoOp::StructMapField as u8, 12);
        assert_eq!(FlatUndoOp::StructMapRow as u8, 13);
        assert_eq!(FlatUndoOp::ScalarUpdate as u8, 14); // post-parity extension
        assert_eq!(FlatUndoOp::StateBytes as u8, 15); // post-parity extension
        assert_eq!(FlatUndoEntry::read_from(&[0u8; 24]), None); // 0 invalid
        assert_eq!(
            FlatUndoEntry::read_from(&{
                let mut b = [0u8; 24];
                b[0] = 16;
                b
            }),
            None
        ); // beyond the enum
    }
}
