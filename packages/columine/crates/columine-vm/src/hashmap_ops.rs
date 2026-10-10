//! HashMap batch operations, generic over upsert strategy.
//!
//! Top-level HASHMAP slots use a structure-of-arrays layout:
//! `[keys: u32 × cap][values: u32 × cap][cmp/timestamps: u64 × cap]`.
//! The keys+values portion binds as a `FlatTable` with u32 entries; the
//! comparison lane is 8 bytes per entry, physically u64 and interpreted by
//! `CmpType`.
//!
//! Strategy and delta mode are runtime parameters. Calls that need VM-owned
//! undo or TTL state go through the `hooks::VmHooks` boundary.
//! Each accepted primitive publishes its size inside one prior-byte capture.
//! Flags are included for direct callers; an enclosing VM boundary owns those
//! bytes instead. TTL helpers never append a second logical journal.

use crate::bytes;
use crate::hash_table::{ENTRY_U32, ErasePlan, FlatTable};
use crate::hooks::{TtlMutation, VmHooks};
use crate::meta::SlotMetaView;
use columine_types::types::{ChangeFlag, ErrorCode, SlotMetaOffset};

pub use columine_types::CmpType;

/// HashMap upsert strategy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Strategy {
    /// Update if ts > existing ts.
    Latest,
    /// Never update existing.
    First,
    /// Always update existing.
    Last,
    /// Update if cmp > existing cmp.
    Max,
    /// Update if cmp < existing cmp.
    Min,
}

impl Strategy {
    /// Latest, max, and min strategies require the comparison lane.
    pub const fn needs_timestamps(self) -> bool {
        matches!(self, Self::Latest | Self::Max | Self::Min)
    }
}

/// Read the i-th comparison value from a raw input column. u32 columns have a
/// four-byte stride; f64/i64 columns have an eight-byte stride.
#[inline(always)]
pub fn read_cmp_value(cmp_col: &[u8], i: u32, cmp_type: CmpType) -> u64 {
    match cmp_type {
        CmpType::U32 => bytes::read_u32(cmp_col, i * 4) as u64,
        CmpType::F64 => bytes::read_u64(cmp_col, i * 8),
        CmpType::I64 => bytes::read_u64(cmp_col, i * 8),
    }
}

/// Compare `a > b` under the `cmp_type` semantics.
pub(crate) fn cmp_gt(a: u64, b: u64, cmp_type: CmpType) -> bool {
    match cmp_type {
        CmpType::U32 => (a as u32) > (b as u32),
        CmpType::F64 => f64::from_bits(a) > f64::from_bits(b),
        CmpType::I64 => (a as i64) > (b as i64),
    }
}

/// Compare `a < b` under the `cmp_type` semantics.
fn cmp_lt(a: u64, b: u64, cmp_type: CmpType) -> bool {
    match cmp_type {
        CmpType::U32 => (a as u32) < (b as u32),
        CmpType::F64 => f64::from_bits(a) < f64::from_bits(b),
        CmpType::I64 => (a as i64) < (b as i64),
    }
}

/// Convert a comparison value to f64 for TTL operations.
fn cmp_to_f64(val: u64, cmp_type: CmpType) -> f64 {
    match cmp_type {
        CmpType::U32 => (val as u32) as f64,
        CmpType::F64 => f64::from_bits(val),
        CmpType::I64 => (val as i64) as f64,
    }
}

/// Bind the keys+values portion of a HASHMAP slot.
pub fn bind_slot_map(meta: &SlotMetaView) -> FlatTable {
    FlatTable::bind_external(
        meta.offset,
        meta.capacity,
        meta.meta_base + columine_types::types::SlotMetaOffset::SIZE,
        ENTRY_U32,
    )
}

/// Offset of the 8-byte comparison/timestamp lane after keys+values.
pub const fn cmp_lane_off(meta: &SlotMetaView) -> u32 {
    meta.offset + meta.capacity * 8
}

#[inline(always)]
fn read_cmp_slot(state: &[u8], meta: &SlotMetaView, pos: u32) -> u64 {
    bytes::read_u64(state, cmp_lane_off(meta) + pos * 8)
}

#[inline(always)]
fn write_cmp_slot(state: &mut [u8], meta: &SlotMetaView, pos: u32, value: u64) {
    bytes::write_u64(state, cmp_lane_off(meta) + pos * 8, value);
}

/// Decide whether a strategy updates an existing key.
fn should_update(strategy: Strategy, new_cmp: u64, existing: u64, cmp_type: CmpType) -> bool {
    match strategy {
        Strategy::First => false,
        Strategy::Last => true,
        Strategy::Latest | Strategy::Max => cmp_gt(new_cmp, existing, cmp_type),
        Strategy::Min => cmp_lt(new_cmp, existing, cmp_type),
    }
}

/// Upsert a batch of keys and values. `cmp_col` is the raw comparison or TTL
/// timestamp column (stride per `cmp_type`). It is required by comparison
/// strategies and by TTL slots.
/// Column lengths bound the batch: `keys.len()` is `batch_len` and `vals` must
/// match.
#[allow(clippy::too_many_arguments)]
pub fn batch_map_upsert(
    strategy: Strategy,
    delta_mode: bool,
    state: &mut [u8],
    meta: &SlotMetaView,
    _slot_idx: u32,
    keys: &[u32],
    vals: &[u32],
    cmp_col: Option<&[u8]>,
    cmp_type: CmpType,
    hooks: &mut impl VmHooks,
) -> ErrorCode {
    columine_types::check!(
        keys.len() == vals.len(),
        "key/value columns must be parallel"
    );
    let compares_values = strategy.needs_timestamps();
    if compares_values && !meta.has_hashmap_timestamp_storage() {
        return ErrorCode::InvalidProgram;
    }
    let stores_aux = compares_values || meta.has_ttl();

    let tbl = bind_slot_map(meta);
    let mut local_size = tbl.size(state);
    let max_load = tbl.max_load();

    for (i, (&key, &val)) in keys.iter().zip(vals).enumerate() {
        let i = i as u32;
        // Skip EMPTY_KEY and TOMBSTONE sentinels.
        let Some(probe) = tbl.find_insert(state, key) else {
            continue;
        };

        let new_cmp = if stores_aux {
            read_cmp_value(
                cmp_col.unwrap_or_else(|| columine_types::die!("cmp or TTL column required")),
                i,
                cmp_type,
            )
        } else {
            0
        };

        if !probe.found && local_size >= max_load {
            return ErrorCode::CapacityExceeded;
        }
        if probe.found
            && !should_update(
                strategy,
                new_cmp,
                if stores_aux {
                    read_cmp_slot(state, meta, probe.pos)
                } else {
                    0
                },
                cmp_type,
            )
        {
            continue;
        }

        let ttl_plan = if meta.has_ttl() {
            match hooks.plan_ttl_batch(
                state,
                meta,
                core::iter::once(TtlMutation::Upsert {
                    key,
                    timestamp: cmp_to_f64(new_cmp, cmp_type),
                    mapped_value: val,
                }),
            ) {
                Ok(plan) => Some(plan),
                Err(error) => return error,
            }
        } else {
            None
        };
        let capture = match hooks.begin_capture(state, || {
            let ttl = ttl_plan.as_ref().map_or(&[(0, 0); 2], |plan| plan.ranges());
            [
                (
                    tbl.keys_off + probe.pos * 4,
                    if probe.found { 0 } else { 4 },
                ),
                (tbl.entries_off + probe.pos * 4, 4),
                if stores_aux {
                    (cmp_lane_off(meta) + probe.pos * 8, 8)
                } else {
                    (0, 0)
                },
                (tbl.size_off, if probe.found { 0 } else { 4 }),
                (meta.meta_base + SlotMetaOffset::CHANGE_FLAGS, 1),
                ttl[0],
                ttl[1],
            ]
        }) {
            Ok(capture) => capture,
            Err(error) => return error,
        };
        if !probe.found {
            tbl.set_key_at(state, probe.pos, key);
            local_size += 1;
            tbl.set_size(state, local_size);
        }
        tbl.set_entry_u32_at(state, probe.pos, val);
        if stores_aux {
            write_cmp_slot(state, meta, probe.pos, new_cmp);
        }
        meta.set_change_flag(
            state,
            if probe.found {
                ChangeFlag::UPDATED
            } else {
                ChangeFlag::INSERTED
            },
        );
        if let Some(plan) = ttl_plan {
            hooks.consume_ttl_batch_plan(state, meta, plan);
        }
        hooks.finish_capture(delta_mode, state, capture);
    }
    ErrorCode::Ok
}

/// Apply the retained key/value/aux erase inside a caller-owned capture.
/// TTL, size and flags remain with that caller (including the eviction path).
pub(crate) fn erase_slot_map_planned(
    state: &mut [u8],
    meta: &SlotMetaView,
    table: &FlatTable,
    plan: &ErasePlan,
) {
    let has_timestamps = meta.has_hashmap_timestamp_storage();
    let tail = table.erase_planned(state, plan, |state, from, to| {
        if has_timestamps {
            let value = read_cmp_slot(state, meta, from);
            write_cmp_slot(state, meta, to, value);
        }
    });
    if has_timestamps {
        write_cmp_slot(state, meta, tail, 0);
    }
}

/// Remove a batch of keys by closing their probe clusters.
pub fn batch_map_remove(
    delta_mode: bool,
    state: &mut [u8],
    meta: &SlotMetaView,
    _slot_idx: u32,
    keys: &[u32],
    hooks: &mut impl VmHooks,
) -> ErrorCode {
    let tbl = match FlatTable::checked_bind_external(
        state,
        meta.offset,
        meta.capacity,
        meta.meta_base + SlotMetaOffset::SIZE,
        ENTRY_U32,
    ) {
        Ok(table) => table,
        Err(error) => return error,
    };
    let has_ts = meta.has_hashmap_timestamp_storage();
    let side_end = tbl
        .cap
        .checked_mul(if has_ts { 12 } else { 4 })
        .and_then(|len| tbl.entries_off.checked_add(len));
    let flag_end = meta.meta_base.checked_add(SlotMetaOffset::CHANGE_FLAGS + 1);
    if side_end.is_none_or(|end| end as usize > state.len())
        || flag_end.is_none_or(|end| end as usize > state.len())
    {
        return ErrorCode::InvalidState;
    }
    let mut scratch = hooks.take_erase_scratch();
    let result = (|| {
        let ranges = &mut scratch.ranges;
        for &key in keys {
            let Some(pos) = tbl.find(state, key) else {
                continue;
            };

            let erase = tbl.plan_erase_at(state, pos);
            let ttl_plan = if meta.has_ttl() {
                match hooks.plan_ttl_batch(
                    state,
                    meta,
                    core::iter::once(TtlMutation::Remove { key }),
                ) {
                    Ok(plan) => Some(plan),
                    Err(error) => return error,
                }
            } else {
                None
            };
            let capture = match hooks.begin_capture(state, || {
                // The plan buffer is reborrowed into the closure; the erase
                // and TTL plans stay borrowed for the write that follows.
                let plan_ranges = &mut *ranges;
                plan_ranges.clear();
                tbl.erase_ranges(&erase, plan_ranges);
                if has_ts {
                    erase.for_each_destination_run(|start, len| {
                        plan_ranges.push((cmp_lane_off(meta) + start * 8, len * 8));
                    });
                }
                plan_ranges.push((tbl.size_off, 4));
                plan_ranges.push((meta.meta_base + SlotMetaOffset::CHANGE_FLAGS, 1));
                if let Some(plan) = &ttl_plan {
                    for &range in plan.ranges() {
                        plan_ranges.push(range);
                    }
                }
                plan_ranges.as_slice()
            }) {
                Ok(capture) => capture,
                Err(error) => return error,
            };
            erase_slot_map_planned(state, meta, &tbl, &erase);
            let size = tbl.size(state);
            tbl.set_size(state, size - 1);
            meta.set_change_flag(state, ChangeFlag::REMOVED);
            if let Some(plan) = ttl_plan {
                hooks.consume_ttl_batch_plan(state, meta, plan);
            }
            hooks.finish_capture(delta_mode, state, capture);
        }
        ErrorCode::Ok
    })();
    hooks.restore_erase_scratch(scratch);
    result
}

/// Upsert one key/value pair for the per-element dispatch.
#[allow(clippy::too_many_arguments)]
pub fn single_map_upsert(
    strategy: Strategy,
    delta_mode: bool,
    state: &mut [u8],
    meta: &SlotMetaView,
    slot_idx: u32,
    key: u32,
    val: u32,
    cmp: u64,
    cmp_type: CmpType,
    hooks: &mut impl VmHooks,
) -> ErrorCode {
    let keys = [key];
    let vals = [val];
    let cmp_bytes = cmp.to_le_bytes();
    let cmp_col =
        (strategy.needs_timestamps() || meta.has_ttl()).then_some(&cmp_bytes[..cmp_type.stride()]);
    batch_map_upsert(
        strategy, delta_mode, state, meta, slot_idx, &keys, &vals, cmp_col, cmp_type, hooks,
    )
}

/// Remove one key for the per-element dispatch.
pub fn single_map_remove(
    delta_mode: bool,
    state: &mut [u8],
    meta: &SlotMetaView,
    slot_idx: u32,
    key: u32,
    hooks: &mut impl VmHooks,
) -> ErrorCode {
    batch_map_remove(delta_mode, state, meta, slot_idx, &[key], hooks)
}
