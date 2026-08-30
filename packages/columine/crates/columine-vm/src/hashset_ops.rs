//! HashSet batch operations over a key-only `FlatTable`.
//!
//! HASHSET operations on BITMAP-typed slots delegate through
//! `hooks::VmHooks`.

use crate::hash_table::{ENTRY_NONE, FlatTable};
use crate::hooks::{MutationRecord, VmHooks};
use crate::meta::SlotMetaView;
use columine_types::types::{ChangeFlag, ErrorCode, SlotMetaOffset, SlotType, TOMBSTONE};

/// Bind the key-only table for a HASHSET slot.
pub fn bind_slot_set(meta: &SlotMetaView) -> FlatTable {
    FlatTable::bind_external(
        meta.offset,
        meta.capacity,
        meta.meta_base + SlotMetaOffset::SIZE,
        ENTRY_NONE,
    )
}

/// Insert a batch of elements. A TTL slot requires one timestamp per element;
/// a missing timestamp column is a programmer error.
pub fn batch_set_insert(
    delta_mode: bool,
    state: &mut [u8],
    meta: &SlotMetaView,
    slot_idx: u8,
    elems: &[u32],
    ts_col: Option<&[f64]>,
    hooks: &mut impl VmHooks,
) -> ErrorCode {
    // BITMAP fallback.
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

        // Skip EMPTY_KEY and TOMBSTONE sentinels.
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
                    MutationRecord::set_insert(slot_idx, elem),
                    MutationRecord::set_delete(slot_idx, elem, 0),
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

        // Already present — refresh TTL.
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

/// Remove a batch of elements.
pub fn batch_set_remove(
    delta_mode: bool,
    state: &mut [u8],
    meta: &SlotMetaView,
    slot_idx: u8,
    elems: &[u32],
    hooks: &mut impl VmHooks,
) -> ErrorCode {
    if meta.slot_type() == SlotType::Bitmap {
        return hooks.batch_bitmap_remove(delta_mode, state, meta, slot_idx, elems);
    }

    let tbl = bind_slot_set(meta);
    let mut had_remove = false;

    for &elem in elems {
        let Some(pos) = tbl.find(state, elem) else {
            continue;
        };

        if hooks.undo_enabled() {
            let prev_ts_bits = if meta.has_ttl() {
                hooks
                    .latest_eviction_ts(state, meta, elem)
                    .unwrap_or_else(|| {
                        columine_types::die!("live TTL set key is missing its eviction entry")
                    })
                    .to_bits()
            } else {
                0
            };
            hooks.append_mutation(
                delta_mode,
                state,
                MutationRecord::set_delete(slot_idx, elem, prev_ts_bits),
                MutationRecord::set_insert(slot_idx, elem),
            );
        }
        if meta.has_ttl() {
            hooks.remove_ttl_entries_for_key(state, meta, elem);
        }

        tbl.set_key_at(state, pos, TOMBSTONE);
        let size = tbl.size(state);
        tbl.set_size(state, size - 1);
        had_remove = true;
    }

    if had_remove {
        meta.set_change_flag(state, ChangeFlag::REMOVED);
    }
    ErrorCode::Ok
}

/// Insert one element for per-element dispatch.
pub fn single_set_insert(
    delta_mode: bool,
    state: &mut [u8],
    meta: &SlotMetaView,
    slot_idx: u8,
    elem: u32,
    ts: f64,
    hooks: &mut impl VmHooks,
) -> ErrorCode {
    let elems = [elem];
    let timestamps = [ts];
    let ts_col = meta.has_ttl().then_some(timestamps.as_slice());
    batch_set_insert(delta_mode, state, meta, slot_idx, &elems, ts_col, hooks)
}

/// Remove one element for per-element dispatch.
pub fn single_set_remove(
    delta_mode: bool,
    state: &mut [u8],
    meta: &SlotMetaView,
    slot_idx: u8,
    elem: u32,
    hooks: &mut impl VmHooks,
) -> ErrorCode {
    batch_set_remove(delta_mode, state, meta, slot_idx, &[elem], hooks)
}
