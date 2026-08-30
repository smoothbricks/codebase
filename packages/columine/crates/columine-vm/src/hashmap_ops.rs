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

use crate::bytes;
use crate::hash_table::{ENTRY_U32, FlatTable};
use crate::hooks::{MutationRecord, VmHooks};
use crate::meta::SlotMetaView;
use crate::undo_log::FlatUndoOp;
use columine_types::types::{ChangeFlag, ErrorCode};

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

/// Flush size and change flags before an early return.
#[inline(always)]
fn flush(
    state: &mut [u8],
    meta: &SlotMetaView,
    tbl: &FlatTable,
    local_size: u32,
    had_insert: bool,
    had_update: bool,
) {
    tbl.set_size(state, local_size);
    if had_insert {
        meta.set_change_flag(state, ChangeFlag::INSERTED);
    }
    if had_update {
        meta.set_change_flag(state, ChangeFlag::UPDATED);
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
    slot_idx: u8,
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
    let mut had_insert = false;
    let mut had_update = false;

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

        if !probe.found {
            // New key — check capacity before insertion.
            if local_size >= max_load {
                flush(state, meta, &tbl, local_size, had_insert, had_update);
                return ErrorCode::CapacityExceeded;
            }

            if hooks.undo_enabled() {
                // Flush size before appending so the undo log sees consistent
                // state.
                tbl.set_size(state, local_size);
                hooks.append_mutation(
                    delta_mode,
                    state,
                    MutationRecord {
                        op: FlatUndoOp::MapInsert,
                        slot: slot_idx,
                        key,
                        prev_value: 0,
                        aux: 0,
                    },
                    MutationRecord {
                        op: FlatUndoOp::MapDelete,
                        slot: slot_idx,
                        key,
                        prev_value: val,
                        aux: if stores_aux { new_cmp } else { 0 },
                    },
                );
            }

            tbl.set_key_at(state, probe.pos, key);
            tbl.set_entry_u32_at(state, probe.pos, val);
            if stores_aux {
                write_cmp_slot(state, meta, probe.pos, new_cmp);
            }
            local_size += 1;
            had_insert = true;

            if meta.has_ttl() {
                let ttl_result =
                    hooks.insert_with_ttl(state, meta, key, cmp_to_f64(new_cmp, cmp_type));
                if ttl_result != ErrorCode::Ok {
                    flush(state, meta, &tbl, local_size, had_insert, had_update);
                    return ttl_result;
                }
            }
            continue;
        }

        // Existing key — apply the selected strategy.
        let pos = probe.pos;
        if should_update(
            strategy,
            new_cmp,
            if stores_aux {
                read_cmp_slot(state, meta, pos)
            } else {
                0
            },
            cmp_type,
        ) {
            if hooks.undo_enabled() {
                tbl.set_size(state, local_size);
                let prev_cmp = if stores_aux {
                    read_cmp_slot(state, meta, pos)
                } else {
                    0
                };
                hooks.append_mutation(
                    delta_mode,
                    state,
                    MutationRecord {
                        op: FlatUndoOp::MapUpdate,
                        slot: slot_idx,
                        key,
                        prev_value: tbl.entry_u32_at(state, pos),
                        aux: prev_cmp,
                    },
                    MutationRecord {
                        op: FlatUndoOp::MapUpdate,
                        slot: slot_idx,
                        key,
                        prev_value: val,
                        aux: if stores_aux { new_cmp } else { 0 },
                    },
                );
            }
            tbl.set_entry_u32_at(state, pos, val);
            if stores_aux {
                write_cmp_slot(state, meta, pos, new_cmp);
            }
            had_update = true;

            if meta.has_ttl() {
                let ttl_result =
                    hooks.insert_with_ttl(state, meta, key, cmp_to_f64(new_cmp, cmp_type));
                if ttl_result != ErrorCode::Ok {
                    flush(state, meta, &tbl, local_size, had_insert, had_update);
                    return ttl_result;
                }
            }
        }
    }

    flush(state, meta, &tbl, local_size, had_insert, had_update);
    ErrorCode::Ok
}

/// Remove a batch of keys by writing tombstones.
pub fn batch_map_remove(
    delta_mode: bool,
    state: &mut [u8],
    meta: &SlotMetaView,
    slot_idx: u8,
    keys: &[u32],
    hooks: &mut impl VmHooks,
) {
    let tbl = bind_slot_map(meta);
    let has_ts = meta.has_hashmap_timestamp_storage();
    let mut had_remove = false;

    for &key in keys {
        let Some(pos) = tbl.find(state, key) else {
            continue;
        };

        if hooks.undo_enabled() {
            let prev_aux = if meta.has_ttl() {
                hooks
                    .latest_eviction_ts(state, meta, key)
                    .unwrap_or_else(|| {
                        columine_types::die!("live TTL map key is missing its eviction entry")
                    })
                    .to_bits()
            } else if has_ts {
                read_cmp_slot(state, meta, pos)
            } else {
                0
            };
            hooks.append_mutation(
                delta_mode,
                state,
                MutationRecord {
                    op: FlatUndoOp::MapDelete,
                    slot: slot_idx,
                    key,
                    prev_value: tbl.entry_u32_at(state, pos),
                    aux: prev_aux,
                },
                MutationRecord {
                    op: FlatUndoOp::MapInsert,
                    slot: slot_idx,
                    key,
                    prev_value: 0,
                    aux: 0,
                },
            );
        }
        if meta.has_ttl() {
            hooks.remove_ttl_entries_for_key(state, meta, key);
        }

        tbl.set_key_at(state, pos, columine_types::types::TOMBSTONE);
        let size = tbl.size(state);
        tbl.set_size(state, size - 1);
        had_remove = true;
    }

    if had_remove {
        meta.set_change_flag(state, ChangeFlag::REMOVED);
    }
}

/// Upsert one key/value pair for the per-element dispatch.
#[allow(clippy::too_many_arguments)]
pub fn single_map_upsert(
    strategy: Strategy,
    delta_mode: bool,
    state: &mut [u8],
    meta: &SlotMetaView,
    slot_idx: u8,
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
    slot_idx: u8,
    key: u32,
    hooks: &mut impl VmHooks,
) {
    batch_map_remove(delta_mode, state, meta, slot_idx, &[key], hooks);
}
