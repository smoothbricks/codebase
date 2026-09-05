//! Service boundary between container operations and VM-owned state.
//!
//! HashMap, HashSet and BITMAP operations use this trait for undo logging and
//! TTL eviction-index maintenance. The VM supplies the implementation;
//! [`NoVm`] is the deliberately minimal environment used when those services
//! are not wired. BITMAP work itself is not a hook: the VM routes a set
//! opcode on a BITMAP-typed slot to `bitmap_ops` with its own `BitmapEnv`.

use crate::meta::SlotMetaView;
use crate::undo_log::FlatUndoOp;
use columine_types::types::ErrorCode;

/// One side of an undo/redo pair.
///
/// `_pad1`/`_pad2` are layout padding in serialized entries and are not part
/// of this in-memory record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MutationRecord {
    pub op: FlatUndoOp,
    pub slot: u8,
    pub key: u32,
    pub prev_value: u32,
    /// 8-byte auxiliary lane: comparison or timestamp bits.
    pub aux: u64,
}

impl MutationRecord {
    pub const fn set_insert(slot: u8, key: u32) -> Self {
        Self {
            op: FlatUndoOp::SetInsert,
            slot,
            key,
            prev_value: 0,
            aux: 0,
        }
    }

    pub const fn set_delete(slot: u8, key: u32, prev_ts_bits: u64) -> Self {
        Self {
            op: FlatUndoOp::SetDelete,
            slot,
            key,
            prev_value: 0,
            aux: prev_ts_bits,
        }
    }
}

/// Services container operations may request from the VM.
pub trait VmHooks {
    /// Whether undo logging is enabled.
    fn undo_enabled(&self) -> bool;

    /// Append an undo/redo pair. The state buffer has already had its slot
    /// size flushed, so the log observes consistent bytes. On first overflow,
    /// the VM snapshots the live state; callers must pass the full buffer.
    fn append_mutation(
        &mut self,
        delta_mode: bool,
        state: &[u8],
        undo: MutationRecord,
        redo: MutationRecord,
    );

    /// Record `key` at `ts` in the slot's eviction index and return its result.
    fn insert_with_ttl(
        &mut self,
        state: &mut [u8],
        meta: &SlotMetaView,
        key: u32,
        ts: f64,
    ) -> ErrorCode;

    /// Return the key's latest TTL timestamp for remove-undo.
    fn latest_eviction_ts(&self, state: &[u8], meta: &SlotMetaView, key: u32) -> Option<f64>;

    /// Remove all eviction-index entries for a key.
    fn remove_ttl_entries_for_key(&mut self, state: &mut [u8], meta: &SlotMetaView, key: u32);

    /// Whether a snapshot already covers the state.
    fn undo_overflow(&self) -> bool;

    /// Snapshot state for bulk BITMAP algebra. Called only when undo is enabled
    /// and no prior overflow snapshot exists; the undo service owns the shadow.
    fn force_undo_snapshot(&mut self, state: &[u8]);
}

/// Minimal environment with undo and TTL services disabled. Calling an
/// unimplemented service is a programmer error, so it panics rather than
/// silently doing nothing.
#[derive(Debug, Default)]
pub struct NoVm;

impl VmHooks for NoVm {
    fn undo_enabled(&self) -> bool {
        false
    }

    fn append_mutation(
        &mut self,
        _delta_mode: bool,
        _state: &[u8],
        _undo: MutationRecord,
        _redo: MutationRecord,
    ) {
        unreachable!("append_mutation is only called when undo_enabled() — NoVm never enables it")
    }

    fn insert_with_ttl(
        &mut self,
        _state: &mut [u8],
        _meta: &SlotMetaView,
        _key: u32,
        _ts: f64,
    ) -> ErrorCode {
        columine_types::die!(
            "TTL slot reached NoVm — the vm slice's eviction machinery is required"
        )
    }

    fn latest_eviction_ts(&self, _state: &[u8], _meta: &SlotMetaView, _key: u32) -> Option<f64> {
        columine_types::die!(
            "TTL slot reached NoVm — the vm slice's eviction machinery is required"
        )
    }

    fn remove_ttl_entries_for_key(&mut self, _state: &mut [u8], _meta: &SlotMetaView, _key: u32) {
        columine_types::die!(
            "TTL slot reached NoVm — the vm slice's eviction machinery is required"
        )
    }

    fn undo_overflow(&self) -> bool {
        false
    }

    fn force_undo_snapshot(&mut self, _state: &[u8]) {
        unreachable!(
            "force_undo_snapshot is only called when undo_enabled() — NoVm never enables it"
        )
    }
}
