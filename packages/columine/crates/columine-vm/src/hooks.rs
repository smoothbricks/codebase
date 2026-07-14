//! STAGE-2C BOUNDARY: the vm.zig services the container ops call back into.
//!
//! On the Zig side, `hashmap_ops.zig` and `hashset_ops.zig` reach into
//! `vm.zig` module globals: the undo-log switch (`g_undo_enabled` +
//! `appendMutation`), TTL insertion (`insertWithTTL`, eviction-index lookup
//! for remove-undo), and the BITMAP delegation pair (`batchBitmapAdd` /
//! `batchBitmapRemove`). Those live in later slices (vm dispatch, undo_log,
//! bitmap_ops). This trait pins the exact call surface the container family
//! uses; the vm slice provides the real implementation and this comment then
//! shrinks to a doc pointer.
//!
//! `NoVm` is the "undo disabled, no TTL/BITMAP slots in play" environment —
//! byte-for-byte the behavior of a fresh Zig VM before `vm_enable_undo`, and
//! what every hash-container test in the Zig tree runs under.

use crate::meta::SlotMetaView;
use columine_types::types::ErrorCode;

/// One side of an undo/redo pair (vm.zig `appendMutation` argument shape).
/// Field names follow the Zig anonymous-struct literals; `_pad1`/`_pad2` are
/// omitted — they are layout padding in the Zig log entry, not data. The
/// undo_log slice owns the serialized entry layout.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MutationRecord {
    pub op: MutationOp,
    pub slot: u8,
    pub key: u32,
    pub prev_value: u32,
    /// 8-byte auxiliary lane: comparison/timestamp bits (`@bitCast` of the
    /// stored u64 cmp value or f64 timestamp on the Zig side).
    pub aux: u64,
}

/// The mutation opcodes the container family emits (subset of the undo-log
/// op enum; the undo_log slice completes it).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationOp {
    SetInsert,
    SetDelete,
    MapInsert,
    MapDelete,
    MapUpdate,
}

/// Services vm.zig provides to the container ops.
pub trait VmHooks {
    /// vm.zig `g_undo_enabled`.
    fn undo_enabled(&self) -> bool;

    /// vm.zig `appendMutation(delta_mode, undo, redo)`. Called only when
    /// `undo_enabled()`; the state buffer's slot `size` field has already
    /// been flushed when this is called (the Zig code writes `size_ptr.*`
    /// immediately before appending so the log sees consistent state).
    /// `state` is the full state buffer at the append moment: Zig's
    /// `undoAppend`/`undoAppendPair` snapshot it into the shadow buffer on
    /// FIRST overflow (vm.zig:246/275, via the aliasing `g_undo_state_base`
    /// global), so the append boundary must see the live bytes.
    fn append_mutation(
        &mut self,
        delta_mode: bool,
        state: &[u8],
        undo: MutationRecord,
        redo: MutationRecord,
    );

    /// vm.zig `insertWithTTL` — record `key`@`ts` in the slot's eviction
    /// index. Returns an `ErrorCode` exactly like the Zig call.
    fn insert_with_ttl(
        &mut self,
        state: &mut [u8],
        meta: &SlotMetaView,
        key: u32,
        ts: f64,
    ) -> ErrorCode;

    /// vm.zig `findLatestEvictionTimestampForKey` over the slot's eviction
    /// index — the remove-undo path captures the key's last TTL timestamp.
    fn latest_eviction_ts(&self, state: &[u8], meta: &SlotMetaView, key: u32) -> Option<f64>;

    /// vm.zig `removeTTLEntriesForKey` — bitmap remove clears the removed
    /// key's eviction-index entries.
    fn remove_ttl_entries_for_key(&mut self, state: &mut [u8], meta: &SlotMetaView, key: u32);

    /// vm.zig `g_undo_overflow` — true once a snapshot already covers the
    /// state (further per-element tracking is pointless).
    fn undo_overflow(&self) -> bool;

    /// bitmap_ops.zig:511 `forceUndoSnapshot` — snapshot-based rollback for
    /// bulk bitmap algebra (per-element tracking impractical). Called only
    /// when `undo_enabled() && !undo_overflow()`; the undo slice owns the
    /// shadow buffers.
    fn force_undo_snapshot(&mut self, state: &[u8]);

    /// vm.zig `batchBitmapAdd` — HASHSET ops delegate BITMAP-typed slots.
    fn batch_bitmap_add(
        &mut self,
        delta_mode: bool,
        state: &mut [u8],
        meta: &SlotMetaView,
        slot_idx: u8,
        elems: &[u32],
        ts_col: Option<&[f64]>,
    ) -> ErrorCode;

    /// vm.zig `batchBitmapRemove`.
    fn batch_bitmap_remove(
        &mut self,
        delta_mode: bool,
        state: &mut [u8],
        meta: &SlotMetaView,
        slot_idx: u8,
        elems: &[u32],
    );
}

/// Undo disabled, no TTL, no BITMAP slots — the environment the Zig container
/// tests run under. Reaching an unimplemented service is a programmer bug
/// (the caller passed a TTL/BITMAP slot without wiring the vm slice), so it
/// panics rather than silently no-oping.
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
        panic!("TTL slot reached NoVm — the vm slice's eviction machinery is required")
    }

    fn latest_eviction_ts(&self, _state: &[u8], _meta: &SlotMetaView, _key: u32) -> Option<f64> {
        panic!("TTL slot reached NoVm — the vm slice's eviction machinery is required")
    }

    fn remove_ttl_entries_for_key(&mut self, _state: &mut [u8], _meta: &SlotMetaView, _key: u32) {
        panic!("TTL slot reached NoVm — the vm slice's eviction machinery is required")
    }

    fn undo_overflow(&self) -> bool {
        false
    }

    fn force_undo_snapshot(&mut self, _state: &[u8]) {
        unreachable!(
            "force_undo_snapshot is only called when undo_enabled() — NoVm never enables it"
        )
    }

    fn batch_bitmap_add(
        &mut self,
        _delta_mode: bool,
        _state: &mut [u8],
        _meta: &SlotMetaView,
        _slot_idx: u8,
        _elems: &[u32],
        _ts_col: Option<&[f64]>,
    ) -> ErrorCode {
        panic!("BITMAP slot reached NoVm — the bitmap_ops slice is required")
    }

    fn batch_bitmap_remove(
        &mut self,
        _delta_mode: bool,
        _state: &mut [u8],
        _meta: &SlotMetaView,
        _slot_idx: u8,
        _elems: &[u32],
    ) {
        panic!("BITMAP slot reached NoVm — the bitmap_ops slice is required")
    }
}
