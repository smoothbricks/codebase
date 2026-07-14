//! Rust port of `packages/columine/src/vm/hashset_ops.zig` — HashSet batch
//! operations over `FlatHashTable(void)` (keys only).
//!
//! BITMAP fallback: HASHSET-verb opcodes on a BITMAP-typed slot delegate to
//! vm.zig's bitmap ops — that crossing goes through `hooks::VmHooks` until
//! the bitmap_ops slice lands.

use crate::hash_table::{ENTRY_NONE, FlatTable};
use crate::hooks::{MutationOp, MutationRecord, VmHooks};
use crate::meta::SlotMetaView;
use columine_types::types::{ChangeFlag, ErrorCode, SlotMetaOffset, SlotType, TOMBSTONE};

/// hashset_ops.zig:25 `bindSlotSet`.
pub fn bind_slot_set(meta: &SlotMetaView) -> FlatTable {
    FlatTable::bind_external(
        meta.offset,
        meta.capacity,
        meta.meta_base + SlotMetaOffset::SIZE,
        ENTRY_NONE,
    )
}

/// hashset_ops.zig:30 `batchSetInsert`. `ts_col` is required per-element
/// when the slot has TTL (the Zig `.?` unwrap is a panic here too —
/// a missing timestamp column on a TTL slot is a programmer bug).
pub fn batch_set_insert(
    delta_mode: bool,
    state: &mut [u8],
    meta: &SlotMetaView,
    slot_idx: u8,
    elems: &[u32],
    ts_col: Option<&[f64]>,
    hooks: &mut impl VmHooks,
) -> ErrorCode {
    // BITMAP fallback (hashset_ops.zig:40).
    if meta.slot_type() == SlotType::Bitmap {
        return hooks.batch_bitmap_add(delta_mode, state, meta, slot_idx, elems, ts_col);
    }

    let tbl = bind_slot_set(meta);
    let mut local_size = tbl.size(state);
    let max_load = tbl.max_load();
    let mut had_insert = false;

    for (i, &elem) in elems.iter().enumerate() {
        let ts = if meta.has_ttl() {
            ts_col.unwrap_or_else(|| columine_types::die!("TTL slot requires a timestamp column"))
                [i]
        } else {
            0.0
        };

        // Skip EMPTY_KEY/TOMBSTONE (hashset_ops.zig:54).
        let Some(probe) = tbl.find_insert(state, elem) else {
            continue;
        };

        if !probe.found {
            if local_size >= max_load {
                tbl.set_size(state, local_size);
                if had_insert {
                    meta.set_change_flag(state, ChangeFlag::INSERTED);
                }
                return ErrorCode::CapacityExceeded;
            }

            if hooks.undo_enabled() {
                tbl.set_size(state, local_size);
                hooks.append_mutation(
                    delta_mode,
                    state,
                    MutationRecord {
                        op: MutationOp::SetInsert,
                        slot: slot_idx,
                        key: elem,
                        prev_value: 0,
                        aux: 0,
                    },
                    MutationRecord {
                        op: MutationOp::SetDelete,
                        slot: slot_idx,
                        key: elem,
                        prev_value: 0,
                        aux: 0,
                    },
                );
            }

            tbl.set_key_at(state, probe.pos, elem);
            local_size += 1;
            had_insert = true;

            if meta.has_ttl() {
                let ttl_result = hooks.insert_with_ttl(state, meta, elem, ts);
                if ttl_result != ErrorCode::Ok {
                    tbl.set_size(state, local_size);
                    if had_insert {
                        meta.set_change_flag(state, ChangeFlag::INSERTED);
                    }
                    return ttl_result;
                }
            }
            continue;
        }

        // Already present — just refresh TTL (hashset_ops.zig:87).
        if meta.has_ttl() {
            let ttl_result = hooks.insert_with_ttl(state, meta, elem, ts);
            if ttl_result != ErrorCode::Ok {
                tbl.set_size(state, local_size);
                if had_insert {
                    meta.set_change_flag(state, ChangeFlag::INSERTED);
                }
                return ttl_result;
            }
        }
    }

    tbl.set_size(state, local_size);
    if had_insert {
        meta.set_change_flag(state, ChangeFlag::INSERTED);
    }
    ErrorCode::Ok
}

/// hashset_ops.zig:104 `batchSetRemove`.
pub fn batch_set_remove(
    delta_mode: bool,
    state: &mut [u8],
    meta: &SlotMetaView,
    slot_idx: u8,
    elems: &[u32],
    hooks: &mut impl VmHooks,
) {
    if meta.slot_type() == SlotType::Bitmap {
        hooks.batch_bitmap_remove(delta_mode, state, meta, slot_idx, elems);
        return;
    }

    let tbl = bind_slot_set(meta);
    let mut had_remove = false;

    for &elem in elems {
        let Some(pos) = tbl.find(state, elem) else {
            continue;
        };

        if hooks.undo_enabled() {
            // Capture the key's latest TTL timestamp for undo
            // (hashset_ops.zig:126-134).
            let prev_ts_bits: u64 = if meta.has_ttl() {
                hooks
                    .latest_eviction_ts(state, meta, elem)
                    .map(f64::to_bits)
                    .unwrap_or(0)
            } else {
                0
            };
            hooks.append_mutation(
                delta_mode,
                state,
                MutationRecord {
                    op: MutationOp::SetDelete,
                    slot: slot_idx,
                    key: elem,
                    prev_value: 0,
                    aux: prev_ts_bits,
                },
                MutationRecord {
                    op: MutationOp::SetInsert,
                    slot: slot_idx,
                    key: elem,
                    prev_value: 0,
                    aux: 0,
                },
            );
        }

        tbl.set_key_at(state, pos, TOMBSTONE);
        let size = tbl.size(state);
        tbl.set_size(state, size - 1);
        had_remove = true;
    }

    if had_remove {
        meta.set_change_flag(state, ChangeFlag::REMOVED);
    }
}

/// hashset_ops.zig:154 `singleSetInsert` — FOR_EACH body dispatch.
pub fn single_set_insert(
    delta_mode: bool,
    state: &mut [u8],
    meta: &SlotMetaView,
    slot_idx: u8,
    elem: u32,
    ts: f64,
    hooks: &mut impl VmHooks,
) -> ErrorCode {
    if meta.slot_type() == SlotType::Bitmap {
        let elems = [elem];
        let ts_arr = [ts];
        let ts_col: Option<&[f64]> = if meta.has_ttl() { Some(&ts_arr) } else { None };
        return hooks.batch_bitmap_add(delta_mode, state, meta, slot_idx, &elems, ts_col);
    }

    let tbl = bind_slot_set(meta);
    let Some(probe) = tbl.find_insert(state, elem) else {
        return ErrorCode::Ok;
    };

    if !probe.found {
        if tbl.size(state) >= tbl.max_load() {
            return ErrorCode::CapacityExceeded;
        }

        if hooks.undo_enabled() {
            hooks.append_mutation(
                delta_mode,
                state,
                MutationRecord {
                    op: MutationOp::SetInsert,
                    slot: slot_idx,
                    key: elem,
                    prev_value: 0,
                    aux: 0,
                },
                MutationRecord {
                    op: MutationOp::SetDelete,
                    slot: slot_idx,
                    key: elem,
                    prev_value: 0,
                    aux: 0,
                },
            );
        }

        tbl.set_key_at(state, probe.pos, elem);
        let size = tbl.size(state);
        tbl.set_size(state, size + 1);
        meta.set_change_flag(state, ChangeFlag::INSERTED);

        if meta.has_ttl() {
            let ttl_result = hooks.insert_with_ttl(state, meta, elem, ts);
            if ttl_result != ErrorCode::Ok {
                return ttl_result;
            }
        }
        return ErrorCode::Ok;
    }

    // Already present — refresh TTL (hashset_ops.zig:194).
    if meta.has_ttl() {
        let ttl_result = hooks.insert_with_ttl(state, meta, elem, ts);
        if ttl_result != ErrorCode::Ok {
            return ttl_result;
        }
    }
    ErrorCode::Ok
}

/// hashset_ops.zig:202 `singleSetRemove`.
pub fn single_set_remove(
    delta_mode: bool,
    state: &mut [u8],
    meta: &SlotMetaView,
    slot_idx: u8,
    elem: u32,
    hooks: &mut impl VmHooks,
) {
    if meta.slot_type() == SlotType::Bitmap {
        let elems = [elem];
        hooks.batch_bitmap_remove(delta_mode, state, meta, slot_idx, &elems);
        return;
    }

    let tbl = bind_slot_set(meta);
    let Some(pos) = tbl.find(state, elem) else {
        return;
    };

    if hooks.undo_enabled() {
        hooks.append_mutation(
            delta_mode,
            state,
            MutationRecord {
                op: MutationOp::SetDelete,
                slot: slot_idx,
                key: elem,
                prev_value: 0,
                aux: 0,
            },
            MutationRecord {
                op: MutationOp::SetInsert,
                slot: slot_idx,
                key: elem,
                prev_value: 0,
                aux: 0,
            },
        );
    }

    tbl.set_key_at(state, pos, TOMBSTONE);
    let size = tbl.size(state);
    tbl.set_size(state, size - 1);
    meta.set_change_flag(state, ChangeFlag::REMOVED);
}
