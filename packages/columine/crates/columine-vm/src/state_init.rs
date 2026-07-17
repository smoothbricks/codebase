//! Replaces `packages/columine/src/vm/state_init.zig` — program bytecode →
//! state buffer lifecycle:
//!
//! - [`calculate_state_size`] — `vm_calculate_state_size`
//! - [`init_state`] — `vm_init_state`
//! - [`reset_state`] — `vm_reset_state`
//! - [`calculate_grown_state_size`] — `vm_calculate_grown_state_size`
//! - [`grow_state`] — `vm_grow_state`
//! - [`needs_growth_slot`] — `vm_get_needs_growth_slot`
//!
//! Struct-layout helpers used by both init and execution also live here, as
//! in the Zig file. IMPORTANT DRIFT: state_init.zig carries its OWN copies of
//! `computeStructRowLayout` / `arenaInitialCapacity` that DIFFER from the
//! types.zig copies ported into `columine-types`:
//!
//! - state_init pads each struct row to 4 bytes (`(row + 3) & !3`);
//!   types.zig does not pad. The padded value is what init writes into slot
//!   metadata, so it is the value the whole running VM observes.
//! - state_init sizes arenas at `capacity * 64`; types.zig uses
//!   `capacity * 4`.
//!
//! Both drifted pairs are live in Zig today. This module ports state_init's
//! local versions verbatim (suffixed `_padded` / `_64` so a caller can never
//! pick one by accident); the container-family slice must resolve the drift
//! on the Zig side or prove the types.zig copies are dead code.
//!
//! Error contract: `calculate_state_size` returns 0 for an invalid program
//! (the Zig ABI contract). The stateful entry points return
//! `Result<(), ErrorCode>` — the wasm/NAPI wrappers (stage 5) map `Err` back
//! to the numeric codes. An undersized state buffer is a caller bug (the
//! contract requires `calculate_state_size` bytes) and panics via slice
//! bounds instead of Zig's silent out-of-bounds write. Where Zig hits UB on
//! malformed-but-length-valid bytecode (invalid slot-type nibbles), this port
//! returns `InvalidProgram`/0 — strictly-defined behavior, noted per site.

use crate::{aggregates, bitmap_ops, bytes, hash_table, nested, slot_growth};
use columine_types::types::{
    AggType, CONDITION_TREE_STATE_BYTES, DERIVED_FACT_EMPTY_IDENTITY, EMPTY_KEY, ErrorCode, Opcode,
    PROGRAM_HASH_PREFIX, PROGRAM_HEADER_SIZE, PROGRAM_MAGIC, SLOT_META_SIZE, STATE_FORMAT_VERSION,
    STATE_HEADER_SIZE, STATE_MAGIC, SlotMetaOffset, SlotType, SlotTypeFlags, StateFlags,
    StateHeaderOffset, StructFieldType, TOMBSTONE, align8, next_power_of_2,
};
use core::sync::atomic::{AtomicU8, Ordering};

/// vm.zig:129 `g_needs_growth_slot`. The dispatch loop (later slice) stores
/// the overflowing slot here; JS reads it via `vm_get_needs_growth_slot`.
/// Zig uses a plain global (single-threaded wasm); relaxed atomics keep the
/// same semantics without Rust `static mut`.
pub static NEEDS_GROWTH_SLOT: AtomicU8 = AtomicU8::new(0xff);

/// `vm_get_needs_growth_slot` — the slot that triggered NEEDS_GROWTH, or 0xFF.
pub fn needs_growth_slot() -> u32 {
    u32::from(NEEDS_GROWTH_SLOT.load(Ordering::Relaxed))
}

pub const EVICTION_ENTRY_SIZE: u32 = 16;

// =============================================================================
// Struct Layout Helpers — state_init.zig's LOCAL (drifted) versions
// =============================================================================

/// state_init.zig:91 — arena header is `[arena_capacity:u32][arena_used:u32]`.
pub const ARENA_HEADER_SIZE: u32 = 8;

/// state_init.zig:94-96 `arenaInitialCapacity` — 64 bytes per hash entry.
/// The one authoritative helper set lives HERE (the deleted Zig carried a
/// drifted types.zig twin — cap*4, unpadded rows — deleted post-parity).
pub const fn arena_initial_capacity_64(hash_capacity: u32) -> u32 {
    hash_capacity * 64
}

/// state_init.zig:100-111 `computeStructRowLayout` — unlike the types.zig
/// copy, the row is padded to 4 bytes "for clean addressing", and this padded
/// value is what init stores in slot metadata byte 16-17.
pub fn compute_struct_row_layout_padded(
    num_fields: u8,
    field_types: &[u8],
) -> columine_types::types::StructRowLayout {
    let bitset_bytes = u32::from(num_fields).div_ceil(8);
    let mut row_data = bitset_bytes;
    for &ft in field_types.iter().take(usize::from(num_fields)) {
        row_data += columine_types::types::struct_field_size(
            columine_types::types::StructFieldType::from_u8(ft).unwrap_or_else(|| {
                columine_types::die!(
                    "invariant: struct-map descriptor contains an invalid field type"
                )
            }),
        );
    }
    columine_types::types::StructRowLayout {
        row_size: (row_data + 3) & !3u32,
        bitset_bytes,
        descriptor_size: align8(u32::from(num_fields)),
    }
}

/// state_init.zig:114-121 `structFieldOffset` (byte offset within a row,
/// after the presence bitset). Identical math to the types.zig copy; kept
/// here because grow's arena compaction calls state_init's version.
pub fn struct_field_offset(num_fields: u8, field_types: &[u8], target_field: u8) -> u32 {
    columine_types::types::struct_field_offset(num_fields, field_types, target_field)
}

/// state_init.zig:84-89 `hasArrayFields` — deliberately a RAW-BYTE check
/// (`byte >= 5`), not an enum check: bytes above 9 (invalid field types) also
/// count as arrays here in Zig. Byte semantics preserved.
pub fn has_array_fields_raw(num_fields: u8, field_types: &[u8]) -> bool {
    field_types
        .iter()
        .take(usize::from(num_fields))
        .any(|&b| b >= 5)
}

/// state_init.zig:70-77 `arenaElemSize` — `unreachable` on non-array types
/// (types.zig's copy returns 0 instead). Panics: reaching this with a
/// non-array field is a programmer bug.
pub fn arena_elem_size_strict(field_type_byte: u8) -> u32 {
    match field_type_byte {
        // ARRAY_U32, ARRAY_STRING
        5 | 8 => 4,
        // ARRAY_I64, ARRAY_F64
        6 | 7 => 8,
        // ARRAY_BOOL
        9 => 1,
        _ => columine_types::die!("invariant: arenaElemSize called on a non-array field type"),
    }
}

/// state_init.zig:125-127 `getStructMapSlotDataSize`:
/// `[descriptor][keys u32 × cap][rows × cap][timestamps f64 × cap]?`.
pub const fn struct_map_slot_data_size(
    descriptor_size: u32,
    capacity: u32,
    row_size: u32,
    has_timestamps: bool,
) -> u32 {
    descriptor_size
        + capacity * 4
        + capacity * row_size
        + if has_timestamps { capacity * 8 } else { 0 }
}

/// STRUCT_MAP2 primary layout: descriptor + two exact u32 key lanes + rows.
pub const fn struct_map2_slot_data_size(descriptor_size: u32, capacity: u32, row_size: u32) -> u32 {
    descriptor_size + capacity * 8 + capacity * row_size
}

/// state_init.zig:129-137 `getTTLSideBufferSize`.
pub const fn ttl_side_buffer_size(has_ttl: bool, has_evict_trigger: bool, capacity: u32) -> u32 {
    if !has_ttl {
        return 0;
    }
    let mut size = align8(capacity * EVICTION_ENTRY_SIZE);
    if has_evict_trigger {
        size += align8(1024 * EVICTION_ENTRY_SIZE);
    }
    size
}

// =============================================================================
// Program header parsing (shared prologue of the three bytecode walkers)
// =============================================================================

struct ProgramView<'a> {
    num_slots: u8,
    init_code: &'a [u8],
}

/// The magic/length prologue every Zig entry point repeats. Returns None on
/// the conditions where Zig returns 0 / INVALID_PROGRAM.
fn parse_program(program: &[u8]) -> Option<ProgramView<'_>> {
    if (program.len() as u32) < PROGRAM_HEADER_SIZE {
        return None;
    }
    let content = &program[PROGRAM_HASH_PREFIX as usize..];
    let magic = u32::from(content[0])
        | (u32::from(content[1]) << 8)
        | (u32::from(content[2]) << 16)
        | (u32::from(content[3]) << 24);
    if magic != PROGRAM_MAGIC {
        return None;
    }
    let num_slots = content[6];
    let init_len = u16::from(content[10]) | (u16::from(content[11]) << 8);
    if PROGRAM_HEADER_SIZE + u32::from(init_len) > program.len() as u32 {
        return None;
    }
    Some(ProgramView {
        num_slots,
        init_code: &content[14..14 + usize::from(init_len)],
    })
}

/// SLOT_DEF capacity normalization shared by the size and init walkers.
fn slot_def_capacity(type_flags: SlotTypeFlags, cap_lo: u8, cap_hi: u8) -> Option<(SlotType, u32)> {
    let slot_type = type_flags.slot_type()?;
    let mut capacity = (u32::from(cap_hi) << 8) | u32::from(cap_lo);
    let is_fixed_size = matches!(
        slot_type,
        SlotType::Aggregate | SlotType::Scalar | SlotType::ConditionTree
    );
    if !is_fixed_size && capacity == 0 {
        capacity = 1024;
    }
    if !is_fixed_size {
        // 2x for load factor
        capacity = next_power_of_2(capacity * 2);
    }
    Some((slot_type, capacity))
}

fn valid_slot_index(seen: &mut [bool; 256], num_slots: u8, slot: u8) -> bool {
    let index = usize::from(slot);
    if slot >= num_slots || seen[index] {
        return false;
    }
    seen[index] = true;
    true
}

fn valid_aggregate_subtype(byte: u8) -> bool {
    let byte = if byte == 0 { AggType::Sum as u8 } else { byte };
    matches!(
        AggType::from_u8(byte),
        Some(
            AggType::Sum
                | AggType::Count
                | AggType::Min
                | AggType::Max
                | AggType::Avg
                | AggType::SumI64
                | AggType::MinI64
                | AggType::MaxI64
        )
    )
}

fn valid_scalar_subtype(byte: u8) -> bool {
    matches!(
        AggType::from_u8(byte),
        Some(AggType::ScalarU32 | AggType::ScalarF64 | AggType::ScalarI64)
    )
}

fn validate_init_code(view: &ProgramView<'_>) -> bool {
    let code = view.init_code;
    let mut seen = [false; 256];
    let mut pc = 0usize;

    while pc < code.len() {
        let Some(op) = Opcode::from_u8(code[pc]) else {
            return false;
        };
        pc += 1;

        match op {
            Opcode::Halt => {
                return pc == code.len()
                    && seen[..usize::from(view.num_slots)]
                        .iter()
                        .all(|defined| *defined);
            }
            Opcode::SlotDef => {
                let Some(operands) = code.get(pc..pc.saturating_add(4)) else {
                    return false;
                };
                let slot = operands[0];
                let type_flags = SlotTypeFlags::from_byte(operands[1]);
                let cap_lo = operands[2];
                let Some(slot_type) = type_flags.slot_type() else {
                    return false;
                };
                if !valid_slot_index(&mut seen, view.num_slots, slot) {
                    return false;
                }
                if matches!(
                    slot_type,
                    SlotType::StructMap
                        | SlotType::StructMap2
                        | SlotType::OrderedList
                        | SlotType::Nested
                ) {
                    return false;
                }
                if slot_type == SlotType::Aggregate && !valid_aggregate_subtype(cap_lo) {
                    return false;
                }
                if slot_type == SlotType::Scalar && !valid_scalar_subtype(cap_lo) {
                    return false;
                }
                if slot_type == SlotType::HashMap
                    && type_flags.has_ttl()
                    && type_flags.no_hashmap_timestamps()
                {
                    return false;
                }
                pc += 4;
                if type_flags.has_ttl() {
                    let Some(ttl) = code.get(pc..pc.saturating_add(10)) else {
                        return false;
                    };
                    if ttl[9] > 8 {
                        return false;
                    }
                    pc += 10;
                }
            }
            Opcode::SlotArray => {
                let Some(operands) = code.get(pc..pc.saturating_add(4)) else {
                    return false;
                };
                if !valid_slot_index(&mut seen, view.num_slots, operands[0]) {
                    return false;
                }
                pc += 4;
            }
            Opcode::SlotStructMap | Opcode::SlotStructMap2 => {
                let Some(operands) = code.get(pc..pc.saturating_add(5)) else {
                    return false;
                };
                if !valid_slot_index(&mut seen, view.num_slots, operands[0]) {
                    return false;
                }
                let type_flags = SlotTypeFlags::from_byte(operands[1]);
                if type_flags.has_ttl() {
                    return false;
                }
                let num_fields = usize::from(operands[4]);
                pc += 5;
                let Some(field_types) = code.get(pc..pc.saturating_add(num_fields)) else {
                    return false;
                };
                if field_types
                    .iter()
                    .any(|field_type| StructFieldType::from_u8(*field_type).is_none())
                {
                    return false;
                }
                pc += num_fields;
            }
            Opcode::SlotOrderedList => {
                let Some(operands) = code.get(pc..pc.saturating_add(5)) else {
                    return false;
                };
                if !valid_slot_index(&mut seen, view.num_slots, operands[0]) {
                    return false;
                }
                let elem_type = operands[4];
                pc += 5;
                if elem_type == 0xff {
                    let Some(&num_fields) = code.get(pc) else {
                        return false;
                    };
                    pc += 1;
                    let Some(field_types) =
                        code.get(pc..pc.saturating_add(usize::from(num_fields)))
                    else {
                        return false;
                    };
                    if field_types
                        .iter()
                        .any(|field_type| StructFieldType::from_u8(*field_type).is_none())
                    {
                        return false;
                    }
                    pc += usize::from(num_fields);
                } else if StructFieldType::from_u8(elem_type).is_none() {
                    return false;
                }
            }
            Opcode::SlotNested => {
                let Some(operands) = code.get(pc..pc.saturating_add(8)) else {
                    return false;
                };
                if !valid_slot_index(&mut seen, view.num_slots, operands[0]) {
                    return false;
                }
                let Some(inner_type) = SlotType::from_u8(operands[4] & 0x0f) else {
                    return false;
                };
                if inner_type == SlotType::Aggregate && !valid_aggregate_subtype(operands[7]) {
                    return false;
                }
                pc += 8;
            }
            _ => return false,
        }
    }

    false
}

fn validated_program(program: &[u8]) -> Option<ProgramView<'_>> {
    let view = parse_program(program)?;
    validate_init_code(&view).then_some(view)
}

// =============================================================================
// State Size Calculation — vm_calculate_state_size
// =============================================================================

/// state_init.zig:145-381 `vm_calculate_state_size`.
/// Returns the required buffer size in bytes, or 0 if the program is invalid.
pub fn calculate_state_size(program: &[u8]) -> u32 {
    let Some(view) = validated_program(program) else {
        return 0;
    };
    let init_code = view.init_code;

    // Header + slot metadata (48 bytes per slot).
    let mut size = align8(STATE_HEADER_SIZE + u32::from(view.num_slots) * SLOT_META_SIZE);

    let mut pc = 0usize;
    while pc < init_code.len() {
        let op = init_code[pc];
        pc += 1;

        if op == Opcode::SlotDef as u8 {
            // slot:u8, type_flags:u8, cap_lo:u8, cap_hi:u8 [, ttl params]
            let type_flags = SlotTypeFlags::from_byte(init_code[pc + 1]);
            let cap_lo = init_code[pc + 2];
            let cap_hi = init_code[pc + 3];
            // Zig hits UB on an invalid slot-type nibble; this port treats it
            // as an invalid program.
            let Some((slot_type, capacity)) = slot_def_capacity(type_flags, cap_lo, cap_hi) else {
                return 0;
            };
            pc += 4;

            if slot_type == SlotType::HashMap
                && type_flags.has_ttl()
                && type_flags.no_hashmap_timestamps()
            {
                return 0;
            }

            // Skip TTL params if present (f32 ttl + f32 grace + u8 ts_field + u8 start_of).
            if type_flags.has_ttl() {
                pc += 10;
            }

            match slot_type {
                SlotType::HashMap => {
                    size += capacity * 4 + capacity * 4;
                    if !type_flags.no_hashmap_timestamps() {
                        size += capacity * 8;
                    }
                }
                SlotType::HashSet => size += capacity * 4,
                SlotType::Bitmap => {
                    size += columine_types::types::BITMAP_SERIALIZED_LEN_BYTES
                        + bitmap_ops::bitmap_payload_capacity(capacity);
                }
                SlotType::Aggregate => {
                    // COUNT: u64 only. cap_lo holds AggType for AGGREGATE/SCALAR.
                    size += if cap_lo == 2 { 8 } else { 16 };
                }
                SlotType::Scalar => size += 16,
                SlotType::Array => size += capacity * 4 + capacity * 8,
                SlotType::ConditionTree => {
                    size += CONDITION_TREE_STATE_BYTES;
                    // Derived facts: interleaved u64 identities, then u32 low/high values.
                    if capacity > 0 {
                        size = align8(size);
                        size += capacity * 16;
                    }
                }
                // These use their own opcodes, not SLOT_DEF.
                SlotType::StructMap
                | SlotType::StructMap2
                | SlotType::OrderedList
                | SlotType::Nested => {}
            }
            size = align8(size);

            if type_flags.has_ttl() {
                size += capacity * EVICTION_ENTRY_SIZE;
                size = align8(size);
                if type_flags.has_evict_trigger() {
                    size += 1024 * EVICTION_ENTRY_SIZE;
                    size = align8(size);
                }
            }
        } else if op == Opcode::SlotArray as u8 {
            let cap_lo = init_code[pc + 1];
            let cap_hi = init_code[pc + 2];
            let mut capacity = (u32::from(cap_hi) << 8) | u32::from(cap_lo);
            if capacity == 0 {
                capacity = 1024;
            }
            capacity = next_power_of_2(capacity * 2);
            pc += 4;

            size += capacity * 4 + capacity * 8;
            size = align8(size);
        } else if op == Opcode::SlotStructMap as u8 || op == Opcode::SlotStructMap2 as u8 {
            let type_flags = SlotTypeFlags::from_byte(init_code[pc + 1]);
            let cap_lo = init_code[pc + 2];
            let cap_hi = init_code[pc + 3];
            let num_fields = init_code[pc + 4];
            pc += 5;

            let mut capacity = (u32::from(cap_hi) << 8) | u32::from(cap_lo);
            if capacity == 0 {
                capacity = 1024;
            }
            capacity = next_power_of_2(capacity * 2);

            let field_types = &init_code[pc..pc + usize::from(num_fields)];
            pc += usize::from(num_fields);

            let layout = compute_struct_row_layout_padded(num_fields, field_types);
            size += if op == Opcode::SlotStructMap2 as u8 {
                struct_map2_slot_data_size(layout.descriptor_size, capacity, layout.row_size)
            } else {
                struct_map_slot_data_size(layout.descriptor_size, capacity, layout.row_size, false)
            };

            if has_array_fields_raw(num_fields, field_types) {
                size += ARENA_HEADER_SIZE + arena_initial_capacity_64(capacity);
            }
            size = align8(size);

            if type_flags.has_ttl() {
                size += capacity * EVICTION_ENTRY_SIZE;
                size = align8(size);
                if type_flags.has_evict_trigger() {
                    size += 1024 * EVICTION_ENTRY_SIZE;
                    size = align8(size);
                }
            }
        } else if op == Opcode::SlotOrderedList as u8 {
            let cap_lo = init_code[pc + 2];
            let cap_hi = init_code[pc + 3];
            let elem_type = init_code[pc + 4];
            pc += 5;

            let mut capacity = (u32::from(cap_hi) << 8) | u32::from(cap_lo);
            if capacity == 0 {
                capacity = 1024;
            }
            capacity = next_power_of_2(capacity);

            if elem_type == 0xff {
                let num_fields = init_code[pc];
                let field_types = &init_code[pc + 1..pc + 1 + usize::from(num_fields)];
                pc += 1 + usize::from(num_fields);

                let layout = compute_struct_row_layout_padded(num_fields, field_types);
                size += layout.descriptor_size + capacity * layout.row_size;
            } else {
                let elem_size = columine_types::types::struct_field_size(
                    columine_types::types::StructFieldType::from_u8(elem_type).unwrap_or_else(
                        || columine_types::die!("invariant: ordered-list element type is invalid"),
                    ),
                );
                size += capacity * elem_size;
            }
            size = align8(size);
        } else if op == Opcode::SlotNested as u8 {
            let outer_cap_lo = init_code[pc + 2];
            let outer_cap_hi = init_code[pc + 3];
            let inner_type_byte = init_code[pc + 4];
            let inner_cap_lo = init_code[pc + 5];
            let inner_cap_hi = init_code[pc + 6];
            let inner_agg_type_byte = init_code[pc + 7];
            pc += 8;

            let mut outer_cap = (u32::from(outer_cap_hi) << 8) | u32::from(outer_cap_lo);
            if outer_cap == 0 {
                outer_cap = 1024;
            }
            outer_cap = next_power_of_2(outer_cap * 2);

            let mut inner_initial_cap = (u32::from(inner_cap_hi) << 8) | u32::from(inner_cap_lo);
            if inner_initial_cap == 0 {
                inner_initial_cap = 16;
            }
            // Zig truncates to u4 then @enumFromInt (UB for 10-15) — port as invalid.
            let Some(inner_type) = SlotType::from_u8(inner_type_byte & 0x0f) else {
                return 0;
            };
            // Only interpret as AggType for AGGREGATE inner containers.
            let inner_agg = if inner_type == SlotType::Aggregate && inner_agg_type_byte >= 1 {
                inner_agg_type_byte
            } else {
                1 // SUM
            };

            size += nested::nested_slot_data_size(
                outer_cap,
                next_power_of_2(inner_initial_cap),
                inner_type,
                inner_agg,
            );
            size = align8(size);
        } else if op == Opcode::Halt as u8 {
            break;
        } else {
            return 0;
        }
    }

    size
}

// =============================================================================
// State Initialization — vm_init_state / vm_reset_state
// =============================================================================

/// state_init.zig:388-437 `writeSlotMeta` (the 48-byte slot metadata record).
#[allow(clippy::too_many_arguments)]
fn write_slot_meta(
    state: &mut [u8],
    slot: u8,
    data_offset: u32,
    capacity: u32,
    type_flags: SlotTypeFlags,
    agg_type_byte: u8,
    ttl_seconds: f32,
    grace_seconds: f32,
    timestamp_field_idx: u8,
    start_of: u8,
    eviction_index_offset: u32,
    eviction_index_capacity: u32,
    evicted_buffer_offset: u32,
) {
    let meta = STATE_HEADER_SIZE + u32::from(slot) * SLOT_META_SIZE;

    bytes::write_u32(state, meta, data_offset);
    bytes::write_u32(state, meta + 4, capacity);
    bytes::write_u32(state, meta + 8, 0); // size

    state[(meta + SlotMetaOffset::TYPE_FLAGS) as usize] = type_flags.to_byte();
    state[(meta + SlotMetaOffset::AGG_TYPE) as usize] = agg_type_byte;
    state[(meta + SlotMetaOffset::CHANGE_FLAGS) as usize] = 0;
    state[(meta + SlotMetaOffset::TIMESTAMP_FIELD_IDX) as usize] = timestamp_field_idx;

    bytes::write_f32(state, meta + SlotMetaOffset::TTL_SECONDS, ttl_seconds);
    bytes::write_f32(state, meta + SlotMetaOffset::GRACE_SECONDS, grace_seconds);

    bytes::write_u32(state, meta + 24, eviction_index_offset);
    bytes::write_u32(state, meta + 28, eviction_index_capacity);
    bytes::write_u32(state, meta + 32, 0); // eviction_index_size
    bytes::write_u32(state, meta + 36, evicted_buffer_offset);
    bytes::write_u32(state, meta + 40, 0); // evicted_count

    state[(meta + SlotMetaOffset::START_OF) as usize] = start_of;
    state[(meta + 45) as usize] = 0;
    state[(meta + 46) as usize] = 0;
    state[(meta + 47) as usize] = 0;
}

/// state_init.zig:442-914 `vm_init_state`.
/// `state` must be at least `calculate_state_size(program)` bytes and zeroed
/// (Zig relies on zeroed values regions; both TS backends allocate zeroed).
pub fn init_state(state: &mut [u8], program: &[u8]) -> Result<(), ErrorCode> {
    let Some(view) = validated_program(program) else {
        return Err(ErrorCode::InvalidProgram);
    };
    let content = &program[PROGRAM_HASH_PREFIX as usize..];
    let num_slots = view.num_slots;
    let init_code = view.init_code;

    // State header (format v2): magic(4) + format_version(1) + program_version(2)
    // + ruleset_version(2) + num_slots(1) + num_vars(1) + num_bitvecs(1) + flags(1)
    // + reserved(19).
    bytes::write_u32(state, 0, STATE_MAGIC);
    state[StateHeaderOffset::FORMAT_VERSION as usize] = STATE_FORMAT_VERSION;
    state[StateHeaderOffset::PROGRAM_VERSION as usize] = content[4];
    state[StateHeaderOffset::PROGRAM_VERSION as usize + 1] = content[5];
    state[StateHeaderOffset::RULESET_VERSION as usize] = 0;
    state[StateHeaderOffset::RULESET_VERSION as usize + 1] = 0;
    state[StateHeaderOffset::NUM_SLOTS as usize] = num_slots;
    state[StateHeaderOffset::NUM_VARS as usize] = 0;
    state[StateHeaderOffset::NUM_BITVECS as usize] = 0;
    state[StateHeaderOffset::FLAGS as usize] = 0;
    state[13..32].fill(0);

    let mut data_offset = align8(STATE_HEADER_SIZE + u32::from(num_slots) * SLOT_META_SIZE);

    let mut pc = 0usize;
    while pc < init_code.len() {
        let op = init_code[pc];
        pc += 1;

        if op == Opcode::SlotDef as u8 {
            let slot = init_code[pc];
            let type_flags = SlotTypeFlags::from_byte(init_code[pc + 1]);
            let cap_lo = init_code[pc + 2];
            let cap_hi = init_code[pc + 3];
            let Some((slot_type, capacity)) = slot_def_capacity(type_flags, cap_lo, cap_hi) else {
                return Err(ErrorCode::InvalidProgram);
            };
            // For AGGREGATE/SCALAR: cap_lo encodes the AggType subtype (raw byte).
            let is_subtyped = matches!(slot_type, SlotType::Aggregate | SlotType::Scalar);
            let agg_type_byte = if is_subtyped && cap_lo > 0 { cap_lo } else { 1 };
            pc += 4;

            if slot_type == SlotType::HashMap
                && type_flags.has_ttl()
                && type_flags.no_hashmap_timestamps()
            {
                return Err(ErrorCode::InvalidProgram);
            }

            let mut ttl_seconds = 0.0f32;
            let mut grace_seconds = 0.0f32;
            let mut timestamp_field_idx = 0u8;
            let mut start_of = 0u8; // DurationUnit::None
            if type_flags.has_ttl() {
                ttl_seconds = f32::from_le_bytes(
                    init_code[pc..pc + 4]
                        .try_into()
                        .unwrap_or_else(|_| columine_types::die!("f32")),
                );
                grace_seconds = f32::from_le_bytes(
                    init_code[pc + 4..pc + 8]
                        .try_into()
                        .unwrap_or_else(|_| columine_types::die!("f32")),
                );
                timestamp_field_idx = init_code[pc + 8];
                start_of = init_code[pc + 9];
                pc += 10;
            }

            let primary_data_offset = data_offset;

            match slot_type {
                SlotType::HashMap => {
                    // Keys to EMPTY_KEY; values stay zero-init.
                    hash_table::init_external_keys(state, data_offset, capacity);
                    data_offset += capacity * 4 + capacity * 4;
                    if !type_flags.no_hashmap_timestamps() {
                        bytes::fill_f64(state, data_offset, capacity, f64::NEG_INFINITY);
                        data_offset += capacity * 8;
                    }
                }
                SlotType::ConditionTree => {
                    // ConditionTreeState { lifecycle_generation: 1, last_removed_key: EMPTY_KEY }
                    bytes::write_u32(state, data_offset, 1);
                    bytes::write_u32(state, data_offset + 4, EMPTY_KEY);
                    data_offset += CONDITION_TREE_STATE_BYTES;

                    if capacity > 0 {
                        data_offset = align8(data_offset);
                        let derived_facts_offset = data_offset;

                        for pos in 0..capacity {
                            bytes::write_u64(
                                state,
                                data_offset + pos * 8,
                                DERIVED_FACT_EMPTY_IDENTITY,
                            );
                        }
                        data_offset += capacity * 8;
                        bytes::zero(state, data_offset, capacity * 4);
                        data_offset += capacity * 4;
                        bytes::zero(state, data_offset, capacity * 4);
                        data_offset += capacity * 4;

                        bytes::write_u32(
                            state,
                            StateHeaderOffset::DERIVED_FACTS_OFFSET,
                            derived_facts_offset,
                        );
                        bytes::write_u16(
                            state,
                            StateHeaderOffset::DERIVED_FACTS_CAPACITY,
                            u16::try_from(capacity).unwrap_or_else(|_| {
                                columine_types::die!("derived facts capacity fits u16")
                            }),
                        );
                        state[StateHeaderOffset::FLAGS as usize] |= StateFlags::HAS_RETE;
                    }
                }
                SlotType::HashSet => {
                    hash_table::init_external_keys(state, data_offset, capacity);
                    data_offset += capacity * 4;
                }
                SlotType::Bitmap => {
                    let storage_size = columine_types::types::BITMAP_SERIALIZED_LEN_BYTES
                        + bitmap_ops::bitmap_payload_capacity(capacity);
                    bytes::zero(state, data_offset, storage_size);
                    data_offset += storage_size;
                }
                SlotType::Aggregate => {
                    data_offset += aggregates::init_agg_slot(state, data_offset, agg_type_byte);
                }
                SlotType::Scalar => {
                    // value ([8]u8) + cmp_ts (f64) = 16 bytes
                    bytes::zero(state, data_offset, 8);
                    bytes::write_f64(state, data_offset + 8, f64::NEG_INFINITY);
                    data_offset += 16;
                }
                SlotType::Array => {
                    bytes::fill_u32(state, data_offset, capacity, EMPTY_KEY);
                    bytes::fill_f64(
                        state,
                        data_offset + capacity * 4,
                        capacity,
                        f64::NEG_INFINITY,
                    );
                    data_offset += capacity * 4 + capacity * 8;
                }
                SlotType::StructMap
                | SlotType::StructMap2
                | SlotType::OrderedList
                | SlotType::Nested => {}
            }
            data_offset = align8(data_offset);

            let mut eviction_index_offset = 0u32;
            let mut eviction_index_capacity = 0u32;
            let mut evicted_buffer_offset = 0u32;

            if type_flags.has_ttl() {
                eviction_index_offset = data_offset;
                eviction_index_capacity = capacity;

                let eviction_size = capacity * EVICTION_ENTRY_SIZE;
                bytes::zero(state, data_offset, eviction_size);
                data_offset = align8(data_offset + eviction_size);

                if type_flags.has_evict_trigger() {
                    evicted_buffer_offset = data_offset;
                    let evicted_size = 1024 * EVICTION_ENTRY_SIZE;
                    bytes::zero(state, data_offset, evicted_size);
                    data_offset = align8(data_offset + evicted_size);
                }
            }

            write_slot_meta(
                state,
                slot,
                primary_data_offset,
                capacity,
                type_flags,
                agg_type_byte,
                ttl_seconds,
                grace_seconds,
                timestamp_field_idx,
                start_of,
                eviction_index_offset,
                eviction_index_capacity,
                evicted_buffer_offset,
            );
        } else if op == Opcode::SlotArray as u8 {
            let slot = init_code[pc];
            let cap_lo = init_code[pc + 1];
            let cap_hi = init_code[pc + 2];
            let mut capacity = (u32::from(cap_hi) << 8) | u32::from(cap_lo);
            if capacity == 0 {
                capacity = 1024;
            }
            capacity = next_power_of_2(capacity * 2);
            pc += 4;

            let type_flags = SlotTypeFlags::new(SlotType::Array, false, false, false, false);

            bytes::fill_u32(state, data_offset, capacity, EMPTY_KEY);
            bytes::fill_f64(
                state,
                data_offset + capacity * 4,
                capacity,
                f64::NEG_INFINITY,
            );

            let primary_offset = data_offset;
            data_offset = align8(data_offset + capacity * 4 + capacity * 8);

            write_slot_meta(
                state,
                slot,
                primary_offset,
                capacity,
                type_flags,
                1, // SUM
                0.0,
                0.0,
                0,
                0, // DurationUnit::None
                0,
                0,
                0,
            );
        } else if op == Opcode::SlotStructMap as u8 || op == Opcode::SlotStructMap2 as u8 {
            let slot = init_code[pc];
            let type_flags = SlotTypeFlags::from_byte(init_code[pc + 1]);
            let cap_lo = init_code[pc + 2];
            let cap_hi = init_code[pc + 3];
            let num_fields = init_code[pc + 4];
            pc += 5;

            let mut capacity = (u32::from(cap_hi) << 8) | u32::from(cap_lo);
            if capacity == 0 {
                capacity = 1024;
            }
            capacity = next_power_of_2(capacity * 2);

            let field_types = init_code[pc..pc + usize::from(num_fields)].to_vec();
            pc += usize::from(num_fields);

            let layout = compute_struct_row_layout_padded(num_fields, &field_types);
            let meta_base = STATE_HEADER_SIZE + u32::from(slot) * SLOT_META_SIZE;

            bytes::write_u32(state, meta_base, data_offset);
            bytes::write_u32(state, meta_base + 4, capacity);
            bytes::write_u32(state, meta_base + 8, 0); // size
            state[(meta_base + 12) as usize] = type_flags.to_byte();
            // Byte 13 (AGG_TYPE) is reused for num_fields; byte 15
            // (TIMESTAMP_FIELD_IDX) for bitset_bytes; bytes 16-17
            // (TTL_SECONDS low half) for row_size; byte 18 for has_timestamps.
            state[(meta_base + 13) as usize] = num_fields;
            state[(meta_base + 14) as usize] = 0; // change_flags
            state[(meta_base + 15) as usize] = layout.bitset_bytes as u8;
            bytes::write_u16(state, meta_base + 16, layout.row_size as u16);
            state[(meta_base + 18) as usize] = 0; // has_timestamps (0 for UPSERT_LAST)
            for off in 19..SLOT_META_SIZE {
                state[(meta_base + off) as usize] = 0;
            }

            // Field descriptor, exact key lane(s), then zeroed rows.
            bytes::copy(state, data_offset, &field_types, 0, u32::from(num_fields));
            let keys1_offset = data_offset + layout.descriptor_size;
            bytes::fill_u32(state, keys1_offset, capacity, EMPTY_KEY);
            let rows_offset = if op == Opcode::SlotStructMap2 as u8 {
                let keys2_offset = keys1_offset + capacity * 4;
                bytes::zero(state, keys2_offset, capacity * 4);
                keys2_offset + capacity * 4
            } else {
                keys1_offset + capacity * 4
            };
            bytes::zero(state, rows_offset, capacity * layout.row_size);

            data_offset += if op == Opcode::SlotStructMap2 as u8 {
                struct_map2_slot_data_size(layout.descriptor_size, capacity, layout.row_size)
            } else {
                struct_map_slot_data_size(layout.descriptor_size, capacity, layout.row_size, false)
            };

            if has_array_fields_raw(num_fields, &field_types) {
                let arena_cap = arena_initial_capacity_64(capacity);
                // Arena header offset lives in metadata bytes 20-23.
                bytes::write_u32(state, meta_base + 20, data_offset);
                bytes::write_u32(state, data_offset, arena_cap);
                bytes::write_u32(state, data_offset + 4, 0); // used
                bytes::zero(state, data_offset + ARENA_HEADER_SIZE, arena_cap);
                data_offset += ARENA_HEADER_SIZE + arena_cap;
            }
            data_offset = align8(data_offset);
        } else if op == Opcode::SlotOrderedList as u8 {
            let slot = init_code[pc];
            let type_flags_byte = init_code[pc + 1];
            let cap_lo = init_code[pc + 2];
            let cap_hi = init_code[pc + 3];
            let elem_type = init_code[pc + 4];
            pc += 5;

            let mut capacity = (u32::from(cap_hi) << 8) | u32::from(cap_lo);
            if capacity == 0 {
                capacity = 1024;
            }
            capacity = next_power_of_2(capacity);

            let meta_base = STATE_HEADER_SIZE + u32::from(slot) * SLOT_META_SIZE;

            if elem_type == 0xff {
                // Struct list.
                let num_fields = init_code[pc];
                let field_types = init_code[pc + 1..pc + 1 + usize::from(num_fields)].to_vec();
                pc += 1 + usize::from(num_fields);

                let layout = compute_struct_row_layout_padded(num_fields, &field_types);

                bytes::write_u32(state, meta_base, data_offset);
                bytes::write_u32(state, meta_base + 4, capacity);
                bytes::write_u32(state, meta_base + 8, 0); // count
                state[(meta_base + 12) as usize] = type_flags_byte;
                state[(meta_base + 13) as usize] = num_fields;
                state[(meta_base + 14) as usize] = 0;
                state[(meta_base + 15) as usize] = layout.bitset_bytes as u8;
                bytes::write_u16(state, meta_base + 16, layout.row_size as u16);
                state[(meta_base + 18) as usize] = elem_type; // 0xFF = struct
                for off in 19..SLOT_META_SIZE {
                    state[(meta_base + off) as usize] = 0;
                }

                bytes::copy(state, data_offset, &field_types, 0, u32::from(num_fields));
                let rows_offset = data_offset + layout.descriptor_size;
                bytes::zero(state, rows_offset, capacity * layout.row_size);

                data_offset += layout.descriptor_size + capacity * layout.row_size;
            } else {
                // Scalar list.
                let elem_size = columine_types::types::struct_field_size(
                    columine_types::types::StructFieldType::from_u8(elem_type).unwrap_or_else(
                        || columine_types::die!("invariant: ordered-list element type is invalid"),
                    ),
                );

                bytes::write_u32(state, meta_base, data_offset);
                bytes::write_u32(state, meta_base + 4, capacity);
                bytes::write_u32(state, meta_base + 8, 0); // count
                state[(meta_base + 12) as usize] = type_flags_byte;
                state[(meta_base + 13) as usize] = 0;
                state[(meta_base + 14) as usize] = 0;
                state[(meta_base + 15) as usize] = 0;
                bytes::write_u16(state, meta_base + 16, elem_size as u16);
                state[(meta_base + 18) as usize] = elem_type;
                for off in 19..SLOT_META_SIZE {
                    state[(meta_base + off) as usize] = 0;
                }

                bytes::zero(state, data_offset, capacity * elem_size);
                data_offset += capacity * elem_size;
            }
            data_offset = align8(data_offset);
        } else if op == Opcode::SlotNested as u8 {
            let slot_idx = init_code[pc];
            let outer_type_flags_byte = init_code[pc + 1];
            let outer_cap_lo = init_code[pc + 2];
            let outer_cap_hi = init_code[pc + 3];
            let inner_type_byte = init_code[pc + 4];
            let inner_cap_lo = init_code[pc + 5];
            let inner_cap_hi = init_code[pc + 6];
            let inner_agg_type_byte = init_code[pc + 7];
            pc += 8;

            let mut outer_cap = (u32::from(outer_cap_hi) << 8) | u32::from(outer_cap_lo);
            if outer_cap == 0 {
                outer_cap = 1024;
            }
            outer_cap = next_power_of_2(outer_cap * 2);

            let mut inner_initial_cap = (u32::from(inner_cap_hi) << 8) | u32::from(inner_cap_lo);
            if inner_initial_cap == 0 {
                inner_initial_cap = 16;
            }
            let inner_cap = next_power_of_2(inner_initial_cap);
            let Some(inner_type) = SlotType::from_u8(inner_type_byte & 0x0f) else {
                return Err(ErrorCode::InvalidProgram);
            };
            let inner_agg = if inner_type == SlotType::Aggregate && inner_agg_type_byte >= 1 {
                inner_agg_type_byte
            } else {
                1 // SUM
            };

            let meta_base = STATE_HEADER_SIZE + u32::from(slot_idx) * SLOT_META_SIZE;
            bytes::write_u32(state, meta_base, data_offset);
            bytes::write_u32(state, meta_base + 4, outer_cap);
            bytes::write_u32(state, meta_base + 8, 0); // size
            state[(meta_base + SlotMetaOffset::TYPE_FLAGS) as usize] = outer_type_flags_byte;
            state[(meta_base + SlotMetaOffset::AGG_TYPE) as usize] = inner_agg;
            state[(meta_base + SlotMetaOffset::CHANGE_FLAGS) as usize] = 0;
            // Note: unlike SLOT_STRUCT_MAP, Zig does NOT clear metadata bytes
            // 15-47 here — it relies on the zeroed buffer. Same here.

            nested::write_nested_prefix(
                state,
                data_offset,
                nested::NestedPrefix {
                    inner_type,
                    inner_initial_cap: u16::try_from(inner_initial_cap)
                        .unwrap_or_else(|_| columine_types::die!("nested inner capacity fits u16")),
                    inner_agg_type_byte: inner_agg,
                    depth: 1,
                },
            );

            let keys_off = nested::outer_keys_offset(data_offset);
            bytes::fill_u32(state, keys_off, outer_cap, EMPTY_KEY);

            let arena_hdr = nested::arena_header_offset(data_offset, outer_cap);
            let arena_start = nested::arena_data_offset(data_offset, outer_cap);
            let slot_data_size =
                nested::nested_slot_data_size(outer_cap, inner_cap, inner_type, inner_agg);
            let arena_cap = slot_data_size - (arena_start - data_offset);
            nested::write_arena_header(state, arena_hdr, arena_cap);

            data_offset = align8(data_offset + slot_data_size);
        } else if op == Opcode::Halt as u8 {
            break;
        } else {
            return Err(ErrorCode::InvalidProgram);
        }
    }

    Ok(())
}

/// state_init.zig:917-924 `vm_reset_state` — re-initialize in place.
///
/// The buffer is zeroed first so reset restores the exact fresh-allocation
/// contract: init never writes a HASHMAP's values side-array (it relies on
/// zeroed memory), and the deleted Zig's reset left stale value bytes behind
/// on a dirty buffer — unobservable through lookups, but a byte-level lie.
pub fn reset_state(state: &mut [u8], program: &[u8]) -> Result<(), ErrorCode> {
    if validated_program(program).is_none() {
        return Err(ErrorCode::InvalidProgram);
    }
    state.fill(0);
    init_state(state, program)
}

// =============================================================================
// Slot Growth — vm_calculate_grown_state_size / vm_grow_state
// =============================================================================
//
// When a HashMap/HashSet exceeds 70% load during executeBatch, the VM returns
// NEEDS_GROWTH. JS then queries the slot, computes the grown size, allocates,
// grows, and retries the batch (state_init.zig:929-936).

/// Per-slot facts the growth walkers re-derive from old-state metadata.
struct OldSlotMeta {
    offset: u32,
    capacity: u32,
    type_flags_byte: u8,
    slot_type: SlotType,
    has_ttl: bool,
    has_evict_trigger: bool,
    has_hashmap_timestamps: bool,
    /// Dual-purpose byte 13: AggType for aggregate/scalar, num_fields for
    /// struct_map / ordered-list-struct.
    agg_type_byte: u8,
}

fn read_old_slot_meta(old_state: &[u8], slot_i: u32) -> OldSlotMeta {
    let meta_base = STATE_HEADER_SIZE + slot_i * SLOT_META_SIZE;
    let type_flags_byte = old_state[(meta_base + 12) as usize];
    let slot_type = SlotType::from_u8(type_flags_byte & 0x0f).unwrap_or_else(|| {
        columine_types::die!("invariant: state metadata contains an invalid slot type")
    });
    OldSlotMeta {
        offset: bytes::read_u32(old_state, meta_base),
        capacity: bytes::read_u32(old_state, meta_base + 4),
        type_flags_byte,
        slot_type,
        has_ttl: type_flags_byte & 0x10 != 0,
        has_evict_trigger: type_flags_byte & 0x20 != 0,
        has_hashmap_timestamps: slot_type != SlotType::HashMap || (type_flags_byte & 0x40 == 0),
        agg_type_byte: old_state[(meta_base + 13) as usize],
    }
}

/// Struct-map metadata-driven primary size. The slot kind determines whether
/// one or two exact u32 key lanes precede the rows.
fn struct_map_primary_size_from_meta(
    old_state: &[u8],
    meta_base: u32,
    cap: u32,
    slot_type: SlotType,
) -> u32 {
    let nf = u32::from(old_state[(meta_base + 13) as usize]);
    let rs = u32::from(bytes::read_u16(old_state, meta_base + 16));
    if slot_type == SlotType::StructMap2 {
        struct_map2_slot_data_size(align8(nf), cap, rs)
    } else {
        let has_ts = old_state[(meta_base + 18) as usize] != 0;
        struct_map_slot_data_size(align8(nf), cap, rs, has_ts)
    }
}

/// ORDERED_LIST metadata-driven primary size.
fn ordered_list_primary_size_from_meta(old_state: &[u8], meta_base: u32, cap: u32) -> u32 {
    let elem_type_byte = old_state[(meta_base + 18) as usize];
    let rs = u32::from(bytes::read_u16(old_state, meta_base + 16));
    if elem_type_byte == 0xff {
        let nf = u32::from(old_state[(meta_base + 13) as usize]);
        align8(nf) + cap * rs
    } else {
        cap * rs
    }
}

/// state_init.zig:947-1008 `vm_calculate_grown_state_size` — state size with
/// 2x capacity for `grown_slot_idx`, read from OLD STATE metadata (not the
/// program), so already-grown states grow again correctly.
pub fn calculate_grown_state_size(old_state: &[u8], grown_slot_idx: u32) -> u32 {
    let num_slots = u32::from(old_state[9]);
    let mut total_size = align8(STATE_HEADER_SIZE + num_slots * SLOT_META_SIZE);

    for slot_i in 0..num_slots {
        let meta_base = STATE_HEADER_SIZE + slot_i * SLOT_META_SIZE;
        let m = read_old_slot_meta(old_state, slot_i);

        let cap = if slot_i == grown_slot_idx {
            next_power_of_2(m.capacity * 2)
        } else {
            m.capacity
        };

        let mut slot_size = match m.slot_type {
            SlotType::StructMap | SlotType::StructMap2 => {
                let mut sz =
                    struct_map_primary_size_from_meta(old_state, meta_base, cap, m.slot_type);
                // Arena: doubled on growth, kept on non-growth.
                let arena_hdr_off = bytes::read_u32(old_state, meta_base + 20);
                if arena_hdr_off != 0 {
                    let old_arena_cap = bytes::read_u32(old_state, arena_hdr_off);
                    let new_arena_cap = if slot_i == grown_slot_idx {
                        old_arena_cap * 2
                    } else {
                        old_arena_cap
                    };
                    sz += ARENA_HEADER_SIZE + new_arena_cap;
                }
                sz
            }
            SlotType::OrderedList => ordered_list_primary_size_from_meta(old_state, meta_base, cap),
            _ => slot_growth::slot_data_size(
                m.slot_type,
                cap,
                m.has_hashmap_timestamps,
                m.agg_type_byte,
            ),
        };

        slot_size += ttl_side_buffer_size(m.has_ttl, m.has_evict_trigger, cap);
        total_size = align8(total_size + slot_size);
    }

    total_size
}

/// state_init.zig:1013-1300 `vm_grow_state` — copy old state into `new_state`
/// (which must be zeroed and `calculate_grown_state_size` bytes), rehashing
/// the grown slot and recomputing every slot's offsets.
pub fn grow_state(
    old_state: &[u8],
    new_state: &mut [u8],
    grown_slot_idx: u32,
) -> Result<(), ErrorCode> {
    let num_slots = u32::from(old_state[9]);

    // Copy header verbatim.
    bytes::copy(new_state, 0, old_state, 0, STATE_HEADER_SIZE);

    let mut data_cursor = align8(STATE_HEADER_SIZE + num_slots * SLOT_META_SIZE);

    for slot_i in 0..num_slots {
        let meta_base = STATE_HEADER_SIZE + slot_i * SLOT_META_SIZE;
        let m = read_old_slot_meta(old_state, slot_i);
        let old_offset = m.offset;
        let old_cap = m.capacity;

        let new_cap = if slot_i == grown_slot_idx {
            next_power_of_2(old_cap * 2)
        } else {
            old_cap
        };
        let new_offset = data_cursor;

        // Primary data size (STRUCT_MAP and ORDERED_LIST are metadata-based).
        let new_primary_size = match m.slot_type {
            SlotType::StructMap | SlotType::StructMap2 => {
                struct_map_primary_size_from_meta(old_state, meta_base, new_cap, m.slot_type)
            }
            SlotType::OrderedList => {
                ordered_list_primary_size_from_meta(old_state, meta_base, new_cap)
            }
            _ => slot_growth::slot_data_size(
                m.slot_type,
                new_cap,
                m.has_hashmap_timestamps,
                m.agg_type_byte,
            ),
        };

        let eviction_index_offset = if m.has_ttl {
            align8(new_offset + new_primary_size)
        } else {
            0
        };
        let eviction_index_capacity = if m.has_ttl { new_cap } else { 0 };
        let evicted_buffer_offset = if m.has_ttl && m.has_evict_trigger {
            align8(eviction_index_offset + eviction_index_capacity * EVICTION_ENTRY_SIZE)
        } else {
            0
        };

        // Metadata: new offset + capacity, copy the rest, then fix TTL offsets.
        bytes::write_u32(new_state, meta_base, new_offset);
        bytes::write_u32(new_state, meta_base + 4, new_cap);
        bytes::copy(
            new_state,
            meta_base + 8,
            old_state,
            meta_base + 8,
            SLOT_META_SIZE - 8,
        );
        bytes::write_u32(
            new_state,
            meta_base + SlotMetaOffset::EVICTION_INDEX_OFFSET,
            eviction_index_offset,
        );
        bytes::write_u32(
            new_state,
            meta_base + SlotMetaOffset::EVICTION_INDEX_CAPACITY,
            eviction_index_capacity,
        );
        bytes::write_u32(
            new_state,
            meta_base + SlotMetaOffset::EVICTED_BUFFER_OFFSET,
            evicted_buffer_offset,
        );

        if slot_i == grown_slot_idx {
            match m.slot_type {
                SlotType::HashMap => {
                    let has_ts = m.type_flags_byte & 0x40 == 0;
                    let rehashed = slot_growth::grow_hash_map(
                        old_state, new_state, old_offset, new_offset, old_cap, new_cap, has_ts,
                    );
                    bytes::write_u32(new_state, meta_base + 8, rehashed);
                }
                SlotType::HashSet => {
                    let rehashed = slot_growth::grow_hash_set(
                        old_state, new_state, old_offset, new_offset, old_cap, new_cap,
                    );
                    bytes::write_u32(new_state, meta_base + 8, rehashed);
                }
                SlotType::Bitmap => {
                    // Alloc, memset, and copy all size by the ONE canonical
                    // formula (fixed semantics of telos idea i-87c94893 —
                    // Zig's grow allocation used a drifted smaller formula
                    // and overran; see slot_growth tests for the invariant).
                    let old_storage_size = columine_types::types::BITMAP_SERIALIZED_LEN_BYTES
                        + bitmap_ops::bitmap_payload_capacity(old_cap);
                    let new_storage_size = columine_types::types::BITMAP_SERIALIZED_LEN_BYTES
                        + bitmap_ops::bitmap_payload_capacity(new_cap);
                    debug_assert_eq!(new_storage_size, new_primary_size);
                    bytes::zero(new_state, new_offset, new_storage_size);
                    let copy_len = old_storage_size.min(new_storage_size);
                    bytes::copy(new_state, new_offset, old_state, old_offset, copy_len);
                }
                SlotType::StructMap | SlotType::StructMap2 => {
                    let nf = u32::from(old_state[(meta_base + 13) as usize]);
                    let rs = u32::from(bytes::read_u16(old_state, meta_base + 16));
                    let desc_size = align8(nf);
                    let new_keys_off = new_offset + desc_size;
                    let key_lane_bytes = if m.slot_type == SlotType::StructMap2 {
                        new_cap * 8
                    } else {
                        new_cap * 4
                    };
                    let new_rows_base = new_keys_off + key_lane_bytes;

                    let rehashed = if m.slot_type == SlotType::StructMap2 {
                        slot_growth::grow_struct_map2(
                            old_state, new_state, old_offset, new_offset, old_cap, new_cap, nf, rs,
                        )
                    } else {
                        slot_growth::grow_struct_map(
                            old_state, new_state, old_offset, new_offset, old_cap, new_cap, nf, rs,
                        )
                    };
                    bytes::write_u32(new_state, meta_base + 8, rehashed);

                    // Arena compaction: copy live array data into the new arena.
                    let old_arena_hdr_off = bytes::read_u32(old_state, meta_base + 20);
                    if old_arena_hdr_off != 0 {
                        let old_arena_cap = bytes::read_u32(old_state, old_arena_hdr_off);
                        let new_arena_cap = old_arena_cap * 2;
                        let struct_data_size = struct_map_primary_size_from_meta(
                            old_state,
                            meta_base,
                            new_cap,
                            m.slot_type,
                        );
                        let new_arena_hdr_off = new_offset + struct_data_size;

                        bytes::write_u32(new_state, new_arena_hdr_off, new_arena_cap);
                        bytes::write_u32(new_state, meta_base + 20, new_arena_hdr_off);

                        let old_arena_data_base = old_arena_hdr_off + ARENA_HEADER_SIZE;
                        let new_arena_data_base = new_arena_hdr_off + ARENA_HEADER_SIZE;
                        // Descriptor was copied to the new slot's prefix already.
                        let field_types: Vec<u8> =
                            new_state[new_offset as usize..(new_offset + nf) as usize].to_vec();
                        let mut new_arena_used = 0u32;

                        for ki in 0..new_cap {
                            let key = bytes::read_u32(new_state, new_keys_off + ki * 4);
                            if key == EMPTY_KEY || key == TOMBSTONE {
                                continue;
                            }
                            let row_base = new_rows_base + ki * rs;
                            for fi in 0..nf {
                                // Raw-byte array check, exactly as Zig's
                                // isArrayFieldType(@enumFromInt(byte)).
                                let ft_byte = field_types[fi as usize];
                                if ft_byte < 5 {
                                    continue;
                                }

                                let bit_set = new_state[(row_base + fi / 8) as usize]
                                    & (1u8 << (fi % 8))
                                    != 0;
                                if !bit_set {
                                    continue;
                                }

                                let f_off = struct_field_offset(nf as u8, &field_types, fi as u8);
                                let old_offset_val = bytes::read_u32(new_state, row_base + f_off);
                                let arr_len = bytes::read_u32(new_state, row_base + f_off + 4);
                                if arr_len == 0 {
                                    continue;
                                }

                                let elem_sz = arena_elem_size_strict(ft_byte);
                                let byte_len = arr_len * elem_sz;

                                bytes::copy(
                                    new_state,
                                    new_arena_data_base + new_arena_used,
                                    old_state,
                                    old_arena_data_base + old_offset_val,
                                    byte_len,
                                );
                                bytes::write_u32(new_state, row_base + f_off, new_arena_used);
                                new_arena_used += byte_len;
                            }
                        }

                        bytes::write_u32(new_state, new_arena_hdr_off + 4, new_arena_used);
                    }
                }
                SlotType::OrderedList => {
                    // memcpy existing entries — no rehash.
                    let elem_type_byte = old_state[(meta_base + 18) as usize];
                    let rs = u32::from(bytes::read_u16(old_state, meta_base + 16));
                    let count = bytes::read_u32(old_state, meta_base + 8);

                    if elem_type_byte == 0xff {
                        let nf = u32::from(old_state[(meta_base + 13) as usize]);
                        let desc_size = align8(nf);
                        bytes::copy(new_state, new_offset, old_state, old_offset, nf);
                        let copy_bytes = count * rs;
                        if copy_bytes > 0 {
                            bytes::copy(
                                new_state,
                                new_offset + desc_size,
                                old_state,
                                old_offset + desc_size,
                                copy_bytes,
                            );
                        }
                    } else {
                        let copy_bytes = count * rs;
                        if copy_bytes > 0 {
                            bytes::copy(new_state, new_offset, old_state, old_offset, copy_bytes);
                        }
                    }
                }
                _ => {
                    // Non-hash slot: copy data. The "aggregates/condition
                    // trees are never the grown slot" assumption is VERIFIED
                    // against the dispatch loop: no vm.rs arm ever signals
                    // CAPACITY_EXCEEDED/NEEDS_GROWTH for AGGREGATE, SCALAR,
                    // or CONDITION_TREE slots (fixed-size data; only hash
                    // containers, struct maps, ordered lists, nested slots,
                    // arenas, and TTL eviction indexes overflow). RETE
                    // (stage 3) owns condition-tree writes and must keep it
                    // that way or teach this arm to grow them.
                    let old_data_size = slot_growth::slot_data_size(
                        m.slot_type,
                        old_cap,
                        m.has_hashmap_timestamps,
                        m.agg_type_byte,
                    );
                    let copy_len = old_data_size.min(new_primary_size);
                    if copy_len > 0 {
                        bytes::copy(new_state, new_offset, old_state, old_offset, copy_len);
                    }
                }
            }
        } else {
            // Non-grown slot: memcpy data as-is (incl. struct-map arena).
            let primary_size = match m.slot_type {
                SlotType::StructMap | SlotType::StructMap2 => {
                    let mut sz = struct_map_primary_size_from_meta(
                        old_state,
                        meta_base,
                        old_cap,
                        m.slot_type,
                    );
                    let arena_hdr = bytes::read_u32(old_state, meta_base + 20);
                    if arena_hdr != 0 {
                        let arena_cap = bytes::read_u32(old_state, arena_hdr);
                        sz += ARENA_HEADER_SIZE + arena_cap;
                    }
                    sz
                }
                SlotType::OrderedList => {
                    ordered_list_primary_size_from_meta(old_state, meta_base, old_cap)
                }
                _ => slot_growth::slot_data_size(
                    m.slot_type,
                    old_cap,
                    m.has_hashmap_timestamps,
                    m.agg_type_byte,
                ),
            };
            if primary_size > 0 {
                bytes::copy(new_state, new_offset, old_state, old_offset, primary_size);
            }
            // Arena header offset shifts with the slot data.
            if matches!(m.slot_type, SlotType::StructMap | SlotType::StructMap2) {
                let old_arena_hdr = bytes::read_u32(old_state, meta_base + 20);
                if old_arena_hdr != 0 {
                    let struct_sz = struct_map_primary_size_from_meta(
                        old_state,
                        meta_base,
                        old_cap,
                        m.slot_type,
                    );
                    bytes::write_u32(new_state, meta_base + 20, new_offset + struct_sz);
                }
            }
        }

        if m.has_ttl {
            let old_eviction_index_offset =
                bytes::read_u32(old_state, meta_base + SlotMetaOffset::EVICTION_INDEX_OFFSET);
            let old_eviction_index_size =
                bytes::read_u32(old_state, meta_base + SlotMetaOffset::EVICTION_INDEX_SIZE);
            let mut copied_eviction_size = 0u32;

            if old_eviction_index_offset != 0
                && eviction_index_offset != 0
                && old_eviction_index_size > 0
            {
                copied_eviction_size = old_eviction_index_size.min(eviction_index_capacity);
                let copy_bytes = copied_eviction_size * EVICTION_ENTRY_SIZE;
                bytes::copy(
                    new_state,
                    eviction_index_offset,
                    old_state,
                    old_eviction_index_offset,
                    copy_bytes,
                );
            }
            bytes::write_u32(
                new_state,
                meta_base + SlotMetaOffset::EVICTION_INDEX_SIZE,
                copied_eviction_size,
            );

            if m.has_evict_trigger {
                let old_evicted_offset =
                    bytes::read_u32(old_state, meta_base + SlotMetaOffset::EVICTED_BUFFER_OFFSET);
                let old_evicted_count =
                    bytes::read_u32(old_state, meta_base + SlotMetaOffset::EVICTED_COUNT);
                let mut copied_evicted_count = 0u32;
                if old_evicted_offset != 0 && evicted_buffer_offset != 0 && old_evicted_count > 0 {
                    copied_evicted_count = old_evicted_count.min(1024);
                    let copy_bytes = copied_evicted_count * EVICTION_ENTRY_SIZE;
                    bytes::copy(
                        new_state,
                        evicted_buffer_offset,
                        old_state,
                        old_evicted_offset,
                        copy_bytes,
                    );
                }
                bytes::write_u32(
                    new_state,
                    meta_base + SlotMetaOffset::EVICTED_COUNT,
                    copied_evicted_count,
                );
            } else {
                bytes::write_u32(new_state, meta_base + SlotMetaOffset::EVICTED_COUNT, 0);
            }
        }

        let mut slot_total_size = new_primary_size;
        if matches!(m.slot_type, SlotType::StructMap | SlotType::StructMap2) {
            let arena_hdr_off = bytes::read_u32(old_state, meta_base + 20);
            if arena_hdr_off != 0 {
                let old_arena_cap = bytes::read_u32(old_state, arena_hdr_off);
                let new_arena_cap = if slot_i == grown_slot_idx {
                    old_arena_cap * 2
                } else {
                    old_arena_cap
                };
                slot_total_size += ARENA_HEADER_SIZE + new_arena_cap;
            }
        }
        slot_total_size += ttl_side_buffer_size(m.has_ttl, m.has_evict_trigger, new_cap);
        data_cursor = align8(new_offset + slot_total_size);

        // Derived-facts offset follows a moving CONDITION_TREE slot.
        if m.slot_type == SlotType::ConditionTree && new_cap > 0 {
            let new_derived_offset = align8(new_offset + CONDITION_TREE_STATE_BYTES);
            bytes::write_u32(
                new_state,
                StateHeaderOffset::DERIVED_FACTS_OFFSET,
                new_derived_offset,
            );
        }
    }

    Ok(())
}
