//! HashSet batch operations over a key-only `FlatTable`.
//!
//! HASHSET operations on BITMAP-typed slots delegate through
//! `hooks::VmHooks`.
//! Each accepted primitive owns one bounded prior-byte capture, including its
//! TTL index. An enclosing VM boundary owns flag bytes instead of this capture.

use crate::hash_table::{ENTRY_NONE, EraseRanges, FlatTable};
use crate::hooks::{ByteCapture, TtlMutation, VmHooks, batch_rows};
use crate::meta::{BatchFlags, SlotMetaView};
use columine_types::types::{ChangeFlag, ErrorCode, SlotMetaOffset, SlotType};

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
    _slot_idx: u32,
    elems: &[u32],
    ts_col: Option<&[f64]>,
    hooks: &mut impl VmHooks,
) -> ErrorCode {
    if meta.slot_type() == SlotType::Bitmap {
        columine_types::die!("BITMAP slot reached hashset_ops — the VM routes it to bitmap_ops");
    }
    batch_rows!(
        hooks.undo_enabled(),
        meta.has_ttl(),
        insert_rows(delta_mode, state, meta, elems, ts_col, hooks)
    )
}

/// [`batch_set_insert`]'s row loop; `RECORDING` is the journal's state
/// and `TTL` the slot's.
#[inline(always)]
fn insert_rows<const RECORDING: bool, const TTL: bool>(
    delta_mode: bool,
    state: &mut [u8],
    meta: &SlotMetaView,
    elems: &[u32],
    ts_col: Option<&[f64]>,
    hooks: &mut impl VmHooks,
) -> ErrorCode {
    let tbl = bind_slot_set(meta);
    let mut local_size = tbl.size(state);
    let max_load = tbl.max_load();
    let mut flags = BatchFlags::default();

    for (i, &elem) in elems.iter().enumerate() {
        let ts = if TTL {
            ts_col.unwrap_or_else(|| columine_types::die!("TTL slot requires a timestamp column"))
                [i]
        } else {
            0.0
        };

        // Skip EMPTY_KEY and TOMBSTONE sentinels.
        let Some(probe) = tbl.find_insert(state, elem) else {
            continue;
        };

        if !probe.found && local_size >= max_load {
            return ErrorCode::CapacityExceeded;
        }
        if probe.found && !TTL {
            continue;
        }

        let ttl_plan = if TTL {
            match hooks.plan_ttl_batch(
                state,
                meta,
                core::iter::once(TtlMutation::Upsert {
                    key: elem,
                    timestamp: ts,
                    mapped_value: elem,
                }),
            ) {
                Ok(plan) => Some(plan),
                Err(error) => return error,
            }
        } else {
            None
        };
        let capture = if !RECORDING {
            ByteCapture::UNRECORDED
        } else {
            match hooks.begin_capture(state, || {
                let ttl = ttl_plan.as_ref().map_or(&[(0, 0); 2], |plan| plan.ranges());
                [
                    (
                        tbl.keys_off + probe.pos * 4,
                        if probe.found { 0 } else { 4 },
                    ),
                    (tbl.size_off, if probe.found { 0 } else { 4 }),
                    (
                        meta.meta_base + SlotMetaOffset::CHANGE_FLAGS,
                        if probe.found { 0 } else { 1 },
                    ),
                    ttl[0],
                    ttl[1],
                ]
            }) {
                Ok(capture) => capture,
                Err(error) => return error,
            }
        };
        if !probe.found {
            tbl.set_key_at(state, probe.pos, elem);
            local_size += 1;
            tbl.set_size(state, local_size);
            flags.set(state, meta, ChangeFlag::INSERTED);
        }
        if let Some(plan) = ttl_plan {
            hooks.consume_ttl_batch_plan(state, meta, plan);
        }
        hooks.finish_capture(delta_mode, state, capture);
    }
    ErrorCode::Ok
}

/// Remove a batch of elements.
pub fn batch_set_remove(
    delta_mode: bool,
    state: &mut [u8],
    meta: &SlotMetaView,
    _slot_idx: u32,
    elems: &[u32],
    hooks: &mut impl VmHooks,
) -> ErrorCode {
    if meta.slot_type() == SlotType::Bitmap {
        columine_types::die!("BITMAP slot reached hashset_ops — the VM routes it to bitmap_ops");
    }

    let tbl = match FlatTable::checked_bind_external(
        state,
        meta.offset,
        meta.capacity,
        meta.meta_base + SlotMetaOffset::SIZE,
        ENTRY_NONE,
    ) {
        Ok(table) => table,
        Err(error) => return error,
    };
    let flag_end = meta.meta_base.checked_add(SlotMetaOffset::CHANGE_FLAGS + 1);
    if flag_end.is_none_or(|end| end as usize > state.len()) {
        return ErrorCode::InvalidState;
    }
    let mut scratch = hooks.take_erase_scratch();
    let result = batch_rows!(
        hooks.undo_enabled(),
        meta.has_ttl(),
        remove_rows(
            delta_mode,
            state,
            meta,
            &tbl,
            elems,
            &mut scratch.ranges,
            hooks,
        )
    );
    hooks.restore_erase_scratch(scratch);
    result
}

/// [`batch_set_remove`]'s row loop over a checked table; `RECORDING` is the
/// journal's state and `TTL` the slot's.
#[inline(always)]
fn remove_rows<const RECORDING: bool, const TTL: bool>(
    delta_mode: bool,
    state: &mut [u8],
    meta: &SlotMetaView,
    tbl: &FlatTable,
    elems: &[u32],
    ranges: &mut EraseRanges,
    hooks: &mut impl VmHooks,
) -> ErrorCode {
    let mut flags = BatchFlags::default();
    for &elem in elems {
        let Some(pos) = tbl.find(state, elem) else {
            continue;
        };

        let erase = tbl.plan_erase_at(state, pos);
        let ttl_plan = if TTL {
            match hooks.plan_ttl_batch(
                state,
                meta,
                core::iter::once(TtlMutation::Remove { key: elem }),
            ) {
                Ok(plan) => Some(plan),
                Err(error) => return error,
            }
        } else {
            None
        };
        let capture = if !RECORDING {
            ByteCapture::UNRECORDED
        } else {
            match hooks.begin_capture(state, || {
                ranges.clear();
                tbl.erase_ranges(&erase, ranges);
                ranges.push((tbl.size_off, 4));
                ranges.push((meta.meta_base + SlotMetaOffset::CHANGE_FLAGS, 1));
                if let Some(plan) = &ttl_plan {
                    for &range in plan.ranges() {
                        ranges.push(range);
                    }
                }
                ranges.as_slice()
            }) {
                Ok(capture) => capture,
                Err(error) => return error,
            }
        };
        tbl.erase_planned(state, &erase, |_, _, _| {});
        let size = tbl.size(state);
        tbl.set_size(state, size - 1);
        flags.set(state, meta, ChangeFlag::REMOVED);
        if let Some(plan) = ttl_plan {
            hooks.consume_ttl_batch_plan(state, meta, plan);
        }
        hooks.finish_capture(delta_mode, state, capture);
    }
    ErrorCode::Ok
}
