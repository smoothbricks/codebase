//! Rust port of `packages/columine/src/vm/undo_log.zig` — typed rollback
//! helpers over `FlatTable`, called by vm.zig's `rollbackEntry` (dispatch
//! slice). Bitmap and TTL rollback remain vm.zig's (roaring + eviction
//! index); this module owns exactly what undo_log.zig owns.
//!
//! This module also pins the **serialized undo-entry contract**: vm.zig
//! defines `FlatUndoOp`/`FlatUndoEntry` (vm.zig:142/193) as `extern struct`
//! bytes that travel inside exported delta/fork segments consumed by the
//! Scenario fork navigator (superset_root.zig
//! `#region axe!n/reducer-speculation-scenario-fork`, specs/axe/
//! 10d-reducer-speculation.md). The entry is "self-contained in the 24-byte
//! entry (no out-of-band region)" — vm.zig:171. The in-memory ring buffers,
//! overflow shadow snapshot (wasm-static vs native-dynamic split), change-flag
//! save/restore, and the delta export surface are vm.zig globals and belong
//! to the dispatch slice; `hooks::VmHooks` remains their boundary until then.

use crate::bytes;
use crate::hash_table::{ENTRY_NONE, ENTRY_U32, FlatTable};
use crate::meta::SlotMetaView;
use columine_types::types::SlotMetaOffset;

/// vm.zig:142 `FlatUndoOp` — the op byte of a serialized undo entry.
/// Discriminants are the wire values inside exported delta/fork segments.
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
    /// Rollback: restore prev scalar value (aux) + cmp timestamp
    /// (prev_value low + key high) — 16-byte SCALAR slot. Post-parity
    /// extension: the frozen Zig ABI never journaled scalar writes.
    ScalarUpdate = 14,
    /// Restore `pad1` (1..=8) raw bytes at absolute state offset `key` from
    /// `aux`'s little-endian bytes. Used as paired before/after entries for
    /// nested arenas and variable payloads without widening this 24-byte ABI.
    StateBytes = 15,
    /// Rollback a single struct-map scalar field to a captured (bit, bytes)
    /// state, or remove a newly-created row (vm.zig:157 doc: slot = dest
    /// slot; key = row key; `_pad1` = field_idx; `_pad2` = SMF flags; aux =
    /// field cell bytes, LE, sized by the field type).
    StructMapField = 12,
    /// Whole-row rollback for struct-map upserts that clear the entire row
    /// bitset before re-writing (vm.zig:166 doc: `_pad2` 0x02 = row ABSENT;
    /// aux = prior bitset bytes, LE, sized by bitset_bytes ≤ 8).
    StructMapRow = 13,
}

/// vm.zig:186 `SMF_BIT_SET` — field bit set in the target (restored) state.
pub const SMF_BIT_SET: u8 = 0x01;
/// vm.zig:187 `SMF_ROW_ABSENT` — row should not exist in the target state.
pub const SMF_ROW_ABSENT: u8 = 0x02;
/// vm.zig:191 `SMR_ROW_ABSENT` — STRUCT_MAP_ROW's `_pad2` flag; deliberately
/// shares 0x02 with `SMF_ROW_ABSENT` (both mean "remove this row").
pub const SMR_ROW_ABSENT: u8 = 0x02;

/// vm.zig:193 `FlatUndoEntry` (`extern struct`) — one serialized undo/redo
/// entry. C layout: op@0, slot@1, _pad1@2, _pad2@3, key@4, prev_value@8,
/// four implicit padding bytes (aux is u64-aligned), aux@16; size 24.
/// `_pad1`/`_pad2` are real data for STRUCT_MAP_* (field_idx/flags),
/// FACT_* (`fact_idx` little-endian), and STATE_BYTES (length) ops.
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

/// Serialized size of one entry (vm.zig `@sizeOf(FlatUndoEntry)`; the op
/// doc calls it "the 24-byte entry", vm.zig:171).
pub const FLAT_UNDO_ENTRY_SIZE: u32 = 24;

/// vm.zig:203 `FlatDeltaEntry` — an undo/redo pair; 48 serialized bytes.
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

/// undo_log.zig:16 — HASHMAP binds as a u32-entry table, size in slot meta.
fn bind_map(meta: &SlotMetaView) -> FlatTable {
    FlatTable::bind_external(
        meta.offset,
        meta.capacity,
        meta.meta_base + SlotMetaOffset::SIZE,
        ENTRY_U32,
    )
}

/// undo_log.zig:17 — HASHSET binds as a keys-only table.
fn bind_set(meta: &SlotMetaView) -> FlatTable {
    FlatTable::bind_external(
        meta.offset,
        meta.capacity,
        meta.meta_base + SlotMetaOffset::SIZE,
        ENTRY_NONE,
    )
}

/// undo_log.zig:20 `getTimestamps` — timestamp side-array after keys+values
/// (`offset + capacity * 8`), one u64 (f64 bits) per position.
fn ts_off(meta: &SlotMetaView, pos: u32) -> u32 {
    meta.offset + meta.capacity * 8 + pos * 8
}

// WHY (kept contract, documented): rollback is logical, not byte-exact — insert rolls back to TOMBSTONE (not EMPTY) and dead cells keep stale value/timestamp bytes; intended fix: decide the byte-exactness contract with Scenario fork navigation, then either restore bytes or spec logical equality.
/// undo_log.zig:29 `rollbackMapInsert` — tombstone key, decrement size.
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

/// undo_log.zig:38 `rollbackMapUpdate` — restore previous value + optional
/// timestamp (raw u64 bits; the Zig side `@bitCast`s to f64 — same bytes).
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

/// undo_log.zig:49 `rollbackMapDelete` — restore key + value + optional
/// timestamp at the correct probe position, increment size. Returns false
/// if the key is already present (the delete being rolled back never
/// happened) or the table is full.
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

/// undo_log.zig:70 `rollbackAggUpdate` — restore previous value bits and
/// count of a 16-byte SUM/MIN/MAX/AVG slot (`[value: u64][count: u64]`).
// The full u64 count rides the entry's prev_value (low) + key (high) lanes —
// the deleted Zig truncated to u32, so counts past u32::MAX rolled back wrong.
pub fn rollback_agg_update(
    state: &mut [u8],
    meta: &SlotMetaView,
    prev_count: u64,
    prev_val_bits: u64,
) {
    bytes::write_u64(state, meta.offset, prev_val_bits);
    bytes::write_u64(state, meta.offset + 8, prev_count);
}

/// undo_log.zig:79 `rollbackCountUpdate` — restore the count of an 8-byte
/// COUNT-only slot.
// Full u64 count (prev_value low + key high lanes) — see rollback_agg_update.
pub fn rollback_count_update(state: &mut [u8], meta: &SlotMetaView, prev_count: u64) {
    bytes::write_u64(state, meta.offset, prev_count);
}

/// Post-parity ScalarUpdate: restore a 16-byte SCALAR slot's value bytes and
/// comparison timestamp (the deleted Zig never journaled scalar writes).
pub fn rollback_scalar_update(state: &mut [u8], meta: &SlotMetaView, value: u64, ts: f64) {
    bytes::write_u64(state, meta.offset, value);
    bytes::write_f64(state, meta.offset + 8, ts);
}

/// undo_log.zig:89 `rollbackSetInsert` — tombstone element, decrement size.
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

/// undo_log.zig:98 `rollbackSetDelete` — restore element, increment size.
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

    /// The serialized layout IS the contract (vm.zig:193 extern struct;
    /// "self-contained in the 24-byte entry", vm.zig:171): op@0, slot@1,
    /// _pad1@2, _pad2@3, key@4 LE, prev_value@8 LE, zero padding 12..16,
    /// aux@16 LE.
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

    /// Every FlatUndoOp discriminant matches vm.zig:142 (note the gap:
    /// COUNT_UPDATE=11 sits after LIST_APPEND_UNDO=10, not next to
    /// AGG_UPDATE=6).
    #[test]
    fn flat_undo_op_discriminants_match_vm_zig() {
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
        assert_eq!(FlatUndoEntry::read_from(&[0u8; 24]), None); // 0 invalid
        assert_eq!(
            FlatUndoEntry::read_from(&{
                let mut b = [0u8; 24];
                b[0] = 15;
                b
            }),
            None
        ); // beyond the enum
    }
}
