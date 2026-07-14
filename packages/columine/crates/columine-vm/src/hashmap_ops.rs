//! Rust port of `packages/columine/src/vm/hashmap_ops.zig` — HashMap batch
//! operations, generic over upsert strategy.
//!
//! Top-level HASHMAP slot layout (SoA, hashmap_ops.zig:61):
//!   `[keys: u32 × cap][values: u32 × cap][cmp/timestamps: u64 × cap]`
//! The keys+values portion binds as a `FlatTable` with u32 entries; the
//! comparison lane is 8 bytes per entry, physically u64, interpreted per
//! `CmpType`.
//!
//! Zig comptime-specializes on `(strategy, delta_mode)`; here both are
//! runtime parameters — observable behavior is identical, only codegen
//! differs (the vm dispatch slice may re-specialize if the perf gate needs
//! it). Calls into vm.zig globals (`g_undo_enabled`, `appendMutation`,
//! `insertWithTTL`) go through the `hooks::VmHooks` boundary.

use crate::bytes;
use crate::hash_table::{ENTRY_U32, FlatTable};
use crate::hooks::{MutationOp, MutationRecord, VmHooks};
use crate::meta::SlotMetaView;
use columine_types::types::{ChangeFlag, ErrorCode};

/// hashmap_ops.zig:37 `Strategy`.
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
    /// hashmap_ops.zig:138 — latest/max/min require the comparison lane.
    pub const fn needs_timestamps(self) -> bool {
        matches!(self, Self::Latest | Self::Max | Self::Min)
    }
}

/// hashmap_ops.zig:48 `CmpType` — how the 8-byte comparison lane and the
/// input comparison column are interpreted. Discriminants are the opcode
/// `cmp_type` operand byte.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CmpType {
    /// Unsigned 32-bit (string intern IDs, ordinals); 4-byte input stride,
    /// stored zero-extended.
    U32 = 0,
    /// IEEE 754 f64 (numeric fields, timestamps-as-f64); 8-byte stride.
    F64 = 1,
    /// Signed 64-bit (bigint timestamps); 8-byte stride.
    I64 = 2,
}

impl CmpType {
    /// Decode the opcode's `cmp_type` operand byte. `None` = a byte the Zig
    /// `@enumFromInt` would make UB; the dispatch maps it to INVALID_PROGRAM.
    pub const fn from_u8(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::U32),
            1 => Some(Self::F64),
            2 => Some(Self::I64),
            _ => None,
        }
    }
}

/// hashmap_ops.zig:83 `readCmpValue` — read the i-th comparison value from a
/// raw input column. u32 columns have 4-byte stride; f64/i64 8-byte.
#[inline(always)]
pub fn read_cmp_value(cmp_col: &[u8], i: u32, cmp_type: CmpType) -> u64 {
    match cmp_type {
        CmpType::U32 => bytes::read_u32(cmp_col, i * 4) as u64,
        CmpType::F64 => bytes::read_u64(cmp_col, i * 8),
        CmpType::I64 => bytes::read_u64(cmp_col, i * 8),
    }
}

/// hashmap_ops.zig:93 `cmpGt` — `a > b` under the cmp_type's semantics.
fn cmp_gt(a: u64, b: u64, cmp_type: CmpType) -> bool {
    match cmp_type {
        CmpType::U32 => (a as u32) > (b as u32),
        CmpType::F64 => f64::from_bits(a) > f64::from_bits(b),
        CmpType::I64 => (a as i64) > (b as i64),
    }
}

/// hashmap_ops.zig:102 `cmpLt`.
fn cmp_lt(a: u64, b: u64, cmp_type: CmpType) -> bool {
    match cmp_type {
        CmpType::U32 => (a as u32) < (b as u32),
        CmpType::F64 => f64::from_bits(a) < f64::from_bits(b),
        CmpType::I64 => (a as i64) < (b as i64),
    }
}

/// hashmap_ops.zig:112 `cmpToF64` — TTL always operates on f64 timestamps.
fn cmp_to_f64(val: u64, cmp_type: CmpType) -> f64 {
    match cmp_type {
        CmpType::U32 => (val as u32) as f64,
        CmpType::F64 => f64::from_bits(val),
        CmpType::I64 => (val as i64) as f64,
    }
}

/// hashmap_ops.zig:63 `bindSlotMap` — keys+values portion of the slot.
pub fn bind_slot_map(meta: &SlotMetaView) -> FlatTable {
    FlatTable::bind_external(
        meta.offset,
        meta.capacity,
        meta.meta_base + columine_types::types::SlotMetaOffset::SIZE,
        ENTRY_U32,
    )
}

/// hashmap_ops.zig:69 `getCmpSlots` / `getTimestamps` — the 8-byte lane
/// after keys+values (`offset + cap * 8`).
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

/// hashmap_ops.zig:199 — strategy decision for an existing key.
fn should_update(strategy: Strategy, new_cmp: u64, existing: u64, cmp_type: CmpType) -> bool {
    match strategy {
        Strategy::First => false,
        Strategy::Last => true,
        Strategy::Latest | Strategy::Max => cmp_gt(new_cmp, existing, cmp_type),
        Strategy::Min => cmp_lt(new_cmp, existing, cmp_type),
    }
}

/// Flush size + change flags before an early return
/// (hashmap_ops.zig:161-165 pattern, repeated on every exit path).
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

/// hashmap_ops.zig:125 `batchMapUpsert`. `cmp_col` is the raw comparison
/// column (stride per `cmp_type`); pass `None` for strategies that don't
/// need comparison. Column lengths bound the batch: `keys.len()` is
/// `batch_len` and `vals` must match.
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
    let needs_timestamps = strategy.needs_timestamps();
    if needs_timestamps && !meta.has_hashmap_timestamp_storage() {
        return ErrorCode::InvalidProgram;
    }

    let tbl = bind_slot_map(meta);
    let mut local_size = tbl.size(state);
    let max_load = tbl.max_load();
    let mut had_insert = false;
    let mut had_update = false;

    for (i, (&key, &val)) in keys.iter().zip(vals).enumerate() {
        let i = i as u32;
        // Skip EMPTY_KEY/TOMBSTONE keys (hashmap_ops.zig:154).
        let Some(probe) = tbl.find_insert(state, key) else {
            continue;
        };

        let new_cmp: u64 = if needs_timestamps {
            read_cmp_value(
                cmp_col.unwrap_or_else(|| columine_types::die!("cmp column required")),
                i,
                cmp_type,
            )
        } else {
            0
        };

        if !probe.found {
            // New key — capacity check (hashmap_ops.zig:161).
            if local_size >= max_load {
                flush(state, meta, &tbl, local_size, had_insert, had_update);
                return ErrorCode::CapacityExceeded;
            }

            if hooks.undo_enabled() {
                // Zig flushes size before appending so the log sees
                // consistent state (hashmap_ops.zig:170).
                tbl.set_size(state, local_size);
                hooks.append_mutation(
                    delta_mode,
                    state,
                    MutationRecord {
                        op: MutationOp::MapInsert,
                        slot: slot_idx,
                        key,
                        prev_value: 0,
                        aux: 0,
                    },
                    MutationRecord {
                        op: MutationOp::MapDelete,
                        slot: slot_idx,
                        key,
                        prev_value: val,
                        aux: if needs_timestamps { new_cmp } else { 0 },
                    },
                );
            }

            tbl.set_key_at(state, probe.pos, key);
            tbl.set_entry_u32_at(state, probe.pos, val);
            if needs_timestamps {
                write_cmp_slot(state, meta, probe.pos, new_cmp);
            }
            local_size += 1;
            had_insert = true;

            if meta.has_ttl() {
                let ts = if cmp_col.is_some() {
                    cmp_to_f64(new_cmp, cmp_type)
                } else {
                    0.0
                };
                let ttl_result = hooks.insert_with_ttl(state, meta, key, ts);
                if ttl_result != ErrorCode::Ok {
                    flush(state, meta, &tbl, local_size, had_insert, had_update);
                    return ttl_result;
                }
            }
            continue;
        }

        // Existing key — apply strategy (hashmap_ops.zig:197).
        let pos = probe.pos;
        if should_update(
            strategy,
            new_cmp,
            if needs_timestamps {
                read_cmp_slot(state, meta, pos)
            } else {
                0
            },
            cmp_type,
        ) {
            if hooks.undo_enabled() {
                tbl.set_size(state, local_size);
                let prev_cmp = if needs_timestamps {
                    read_cmp_slot(state, meta, pos)
                } else {
                    0
                };
                hooks.append_mutation(
                    delta_mode,
                    state,
                    MutationRecord {
                        op: MutationOp::MapUpdate,
                        slot: slot_idx,
                        key,
                        prev_value: tbl.entry_u32_at(state, pos),
                        aux: prev_cmp,
                    },
                    MutationRecord {
                        op: MutationOp::MapUpdate,
                        slot: slot_idx,
                        key,
                        prev_value: val,
                        aux: if needs_timestamps { new_cmp } else { 0 },
                    },
                );
            }
            tbl.set_entry_u32_at(state, pos, val);
            if needs_timestamps {
                write_cmp_slot(state, meta, pos, new_cmp);
            }
            had_update = true;

            if meta.has_ttl() {
                let ts = if cmp_col.is_some() {
                    cmp_to_f64(new_cmp, cmp_type)
                } else {
                    0.0
                };
                let ttl_result = hooks.insert_with_ttl(state, meta, key, ts);
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

/// hashmap_ops.zig:240 `batchMapRemove` — tombstone keys.
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
            hooks.append_mutation(
                delta_mode,
                state,
                MutationRecord {
                    op: MutationOp::MapDelete,
                    slot: slot_idx,
                    key,
                    prev_value: tbl.entry_u32_at(state, pos),
                    aux: if has_ts {
                        read_cmp_slot(state, meta, pos)
                    } else {
                        0
                    },
                },
                MutationRecord {
                    op: MutationOp::MapInsert,
                    slot: slot_idx,
                    key,
                    prev_value: 0,
                    aux: 0,
                },
            );
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

/// hashmap_ops.zig:280 `singleMapUpsert` — FOR_EACH body dispatch. `cmp` is
/// the raw 8-byte comparison value (typed by `cmp_type`).
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
    let needs_timestamps = strategy.needs_timestamps();
    if needs_timestamps && !meta.has_hashmap_timestamp_storage() {
        return ErrorCode::InvalidProgram;
    }

    let tbl = bind_slot_map(meta);
    // Skip invalid keys (hashmap_ops.zig:297).
    let Some(probe) = tbl.find_insert(state, key) else {
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
                    op: MutationOp::MapInsert,
                    slot: slot_idx,
                    key,
                    prev_value: 0,
                    aux: 0,
                },
                MutationRecord {
                    op: MutationOp::MapDelete,
                    slot: slot_idx,
                    key,
                    prev_value: val,
                    aux: if needs_timestamps { cmp } else { 0 },
                },
            );
        }

        tbl.set_key_at(state, probe.pos, key);
        tbl.set_entry_u32_at(state, probe.pos, val);
        if needs_timestamps {
            write_cmp_slot(state, meta, probe.pos, cmp);
        }
        let size = tbl.size(state);
        tbl.set_size(state, size + 1);
        meta.set_change_flag(state, ChangeFlag::INSERTED);
        return ErrorCode::Ok;
    }

    let existing = if needs_timestamps {
        read_cmp_slot(state, meta, probe.pos)
    } else {
        0
    };
    if should_update(strategy, cmp, existing, cmp_type) {
        if hooks.undo_enabled() {
            hooks.append_mutation(
                delta_mode,
                state,
                MutationRecord {
                    op: MutationOp::MapUpdate,
                    slot: slot_idx,
                    key,
                    prev_value: tbl.entry_u32_at(state, probe.pos),
                    aux: if needs_timestamps { existing } else { 0 },
                },
                MutationRecord {
                    op: MutationOp::MapUpdate,
                    slot: slot_idx,
                    key,
                    prev_value: val,
                    aux: if needs_timestamps { cmp } else { 0 },
                },
            );
        }
        tbl.set_entry_u32_at(state, probe.pos, val);
        if needs_timestamps {
            write_cmp_slot(state, meta, probe.pos, cmp);
        }
        meta.set_change_flag(state, ChangeFlag::UPDATED);
    }

    ErrorCode::Ok
}

/// hashmap_ops.zig:343 `singleMapRemove`.
pub fn single_map_remove(
    delta_mode: bool,
    state: &mut [u8],
    meta: &SlotMetaView,
    slot_idx: u8,
    key: u32,
    hooks: &mut impl VmHooks,
) {
    let tbl = bind_slot_map(meta);
    let Some(pos) = tbl.find(state, key) else {
        return;
    };
    let has_ts = meta.has_hashmap_timestamp_storage();

    if hooks.undo_enabled() {
        hooks.append_mutation(
            delta_mode,
            state,
            MutationRecord {
                op: MutationOp::MapDelete,
                slot: slot_idx,
                key,
                prev_value: tbl.entry_u32_at(state, pos),
                aux: if has_ts {
                    read_cmp_slot(state, meta, pos)
                } else {
                    0
                },
            },
            MutationRecord {
                op: MutationOp::MapInsert,
                slot: slot_idx,
                key,
                prev_value: 0,
                aux: 0,
            },
        );
    }

    tbl.set_key_at(state, pos, columine_types::types::TOMBSTONE);
    let size = tbl.size(state);
    tbl.set_size(state, size - 1);
    meta.set_change_flag(state, ChangeFlag::REMOVED);
}
