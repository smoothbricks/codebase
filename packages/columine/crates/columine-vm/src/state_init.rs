//! Program bytecode and state-buffer lifecycle:
//!
//! - [`calculate_state_size`] computes the required state size.
//! - [`init_state`] initializes the state buffer.
//! - [`reset_state`] reinitializes it in place.
//! - [`calculate_grown_state_size`] and [`grow_state`] resize one slot.
//! - [`needs_growth_slot`] reports the slot that requested growth.
//!
//! Struct-layout helpers used by initialization and execution live here.
//! Their formulas intentionally differ from the general type helpers:
//! - rows are padded to 4 bytes (`(row + 3) & !3`), and that padded value is
//!   stored in slot metadata;
//! - arenas reserve `capacity * 64` bytes rather than `capacity * 4`.
//!
//! Suffixes `_padded` and `_64` keep these domains distinct at call sites.
//!
//! `calculate_state_size` returns 0 for an invalid program. Stateful entry
//! points return `Result<(), ErrorCode>`, which wrappers map to numeric codes.
//! An undersized state buffer is a caller bug and panics through slice bounds.
//! Malformed-but-length-valid bytecode with invalid slot-type nibbles returns
//! `InvalidProgram`/0 rather than invoking undefined behavior.

use crate::meta::SlotMetaView;
use crate::{aggregates, bitmap_ops, bytes, hash_table, nested, slot_growth};
pub use columine_types::DEFAULT_ACCEPTED_PROGRAM_MAGICS;
use columine_types::types::{
    AggType, CONDITION_TREE_STATE_BYTES, DERIVED_FACT_EMPTY_IDENTITY, EMPTY_KEY, ErrorCode,
    EvictionEntry, Opcode, PROGRAM_HASH_PREFIX, PROGRAM_HEADER_SIZE, ProgramHeader, SLOT_META_SIZE,
    STATE_FORMAT_VERSION, STATE_HEADER_SIZE, STATE_MAGIC, SlotMetaOffset, SlotType, SlotTypeFlags,
    StateFlags, StateHeaderOffset, StructFieldType, TOMBSTONE, align8, arena_elem_size,
    has_array_fields, is_array_field_type, next_power_of_2,
};
use core::mem::size_of;
use core::sync::atomic::{AtomicU8, Ordering};

/// Slot that triggered `NEEDS_GROWTH`, or `0xFF` when none is pending.
/// Relaxed atomics preserve the single-threaded wasm global without `static mut`.
pub static NEEDS_GROWTH_SLOT: AtomicU8 = AtomicU8::new(0xff);

/// `vm_get_needs_growth_slot` — the slot that triggered NEEDS_GROWTH, or 0xFF.
pub fn needs_growth_slot() -> u32 {
    u32::from(NEEDS_GROWTH_SLOT.load(Ordering::Relaxed))
}

pub const EVICTION_ENTRY_SIZE: u32 = size_of::<EvictionEntry>() as u32;
/// Closed-form capacity of the optional evicted-row side buffer.
pub const EVICTED_BUFFER_CAP: u32 = 1024;
/// Arena header: `[arena_capacity:u32][arena_used:u32]`.
pub const ARENA_HEADER_SIZE: u32 = nested::ARENA_HDR_SIZE;

/// Struct-map / ordered-list overlays of the TTL/grace metadata fields.
const STRUCT_NUM_FIELDS: u32 = SlotMetaOffset::AGG_TYPE;
const STRUCT_BITSET_BYTES: u32 = SlotMetaOffset::TIMESTAMP_FIELD_IDX;
const STRUCT_ROW_SIZE: u32 = SlotMetaOffset::TTL_SECONDS;
const STRUCT_KIND_BYTE: u32 = SlotMetaOffset::TTL_SECONDS + 2;
const STRUCT_ARENA_HDR: u32 = SlotMetaOffset::GRACE_SECONDS;

/// Overlay record written into a STRUCT_MAP or ORDERED_LIST slot's metadata.
///
/// `data_offset` and `capacity` address the power-of-two table; the remaining
/// fields overlay the TTL/grace metadata slots.
struct OverlayMeta {
    data_offset: u32,
    capacity: u32,
    type_flags_byte: u8,
    num_fields: u8,
    bitset_bytes: u8,
    row_size: u16,
    kind_byte: u8,
}

fn write_overlay_meta(state: &mut [u8], meta_base: u32, overlay: &OverlayMeta) {
    bytes::write_u32(
        state,
        meta_base + SlotMetaOffset::OFFSET,
        overlay.data_offset,
    );
    bytes::write_u32(
        state,
        meta_base + SlotMetaOffset::CAPACITY,
        overlay.capacity,
    );
    bytes::write_u32(state, meta_base + SlotMetaOffset::SIZE, 0);
    state[(meta_base + SlotMetaOffset::TYPE_FLAGS) as usize] = overlay.type_flags_byte;
    state[(meta_base + STRUCT_NUM_FIELDS) as usize] = overlay.num_fields;
    state[(meta_base + SlotMetaOffset::CHANGE_FLAGS) as usize] = 0;
    state[(meta_base + STRUCT_BITSET_BYTES) as usize] = overlay.bitset_bytes;
    bytes::write_u16(state, meta_base + STRUCT_ROW_SIZE, overlay.row_size);
    state[(meta_base + STRUCT_KIND_BYTE) as usize] = overlay.kind_byte;
    let pad_from = (meta_base + STRUCT_KIND_BYTE + 1) as usize;
    let pad_to = (meta_base + SLOT_META_SIZE) as usize;
    state[pad_from..pad_to].fill(0);
}

fn overlay_num_fields(state: &[u8], meta_base: u32) -> u32 {
    u32::from(state[(meta_base + STRUCT_NUM_FIELDS) as usize])
}

fn overlay_row_size(state: &[u8], meta_base: u32) -> u32 {
    u32::from(bytes::read_u16(state, meta_base + STRUCT_ROW_SIZE))
}

fn overlay_kind_byte(state: &[u8], meta_base: u32) -> u8 {
    state[(meta_base + STRUCT_KIND_BYTE) as usize]
}

fn overlay_arena_hdr(state: &[u8], meta_base: u32) -> u32 {
    bytes::read_u32(state, meta_base + STRUCT_ARENA_HDR)
}

/// Initial arena capacity: 64 bytes per hash entry.
/// This helper is deliberately distinct from the general type sizing helper.
pub const fn arena_initial_capacity_64(hash_capacity: u32) -> u32 {
    hash_capacity * 64
}

/// Compute a struct row layout padded to four-byte addressing boundaries.
/// The padded size is what initialization stores in slot metadata.
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

/// Byte offset of a struct field within a row, after the presence bitset.
/// This uses the shared field-offset math because growth compaction requires
/// the same offsets.
pub fn struct_field_offset(num_fields: u8, field_types: &[u8], target_field: u8) -> u32 {
    columine_types::types::struct_field_offset(num_fields, field_types, target_field)
}

/// STRUCT_MAP slot data: descriptor + keys + rows + optional timestamps.
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

/// Size of the optional TTL eviction side buffer.
pub const fn ttl_side_buffer_size(has_ttl: bool, has_evict_trigger: bool, capacity: u32) -> u32 {
    if !has_ttl {
        return 0;
    }
    let mut size = align8(capacity * EVICTION_ENTRY_SIZE);
    if has_evict_trigger {
        size += align8(EVICTED_BUFFER_CAP * EVICTION_ENTRY_SIZE);
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

/// Return whether a program header magic is admitted by the embedder.
///
/// Keeping membership in this helper makes every bytecode walker use the same
/// acceptance rule while retaining a borrowed, allocation-free cold-path set.
#[inline]
pub(crate) fn accepts_program_magic(magic: u32, accepted_program_magics: &[u32]) -> bool {
    accepted_program_magics.contains(&magic)
}

/// Parse the shared program header and return `None` for an invalid program.
fn parse_program<'a>(
    program: &'a [u8],
    accepted_program_magics: &[u32],
) -> Option<ProgramView<'a>> {
    if (program.len() as u32) < PROGRAM_HEADER_SIZE {
        return None;
    }
    let content = &program[PROGRAM_HASH_PREFIX as usize..];
    let header_bytes: [u8; ProgramHeader::WIRE_SIZE] =
        content.get(..ProgramHeader::WIRE_SIZE)?.try_into().ok()?;
    let header = ProgramHeader::from_wire_bytes(header_bytes);
    if !accepts_program_magic(header.magic, accepted_program_magics) {
        return None;
    }
    let init_len = usize::from(header.init_code_len);
    if PROGRAM_HASH_PREFIX + ProgramHeader::WIRE_SIZE as u32 + u32::from(header.init_code_len)
        > program.len() as u32
    {
        return None;
    }
    Some(ProgramView {
        num_slots: header.num_slots,
        init_code: content.get(ProgramHeader::WIRE_SIZE..ProgramHeader::WIRE_SIZE + init_len)?,
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

fn validated_program<'a>(
    program: &'a [u8],
    accepted_program_magics: &[u32],
) -> Option<ProgramView<'a>> {
    let view = parse_program(program, accepted_program_magics)?;
    validate_init_code(&view).then_some(view)
}
// =============================================================================
// State Size Calculation — vm_calculate_state_size
// =============================================================================

/// Calculate the required state-buffer size, or return 0 for an invalid
/// program.
pub fn calculate_state_size(program: &[u8], accepted_program_magics: &[u32]) -> u32 {
    let Some(view) = validated_program(program, accepted_program_magics) else {
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
            // Invalid slot-type nibbles make the program invalid.
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

            let agg_type_byte =
                if matches!(slot_type, SlotType::Aggregate | SlotType::Scalar) && cap_lo > 0 {
                    cap_lo
                } else {
                    AggType::Sum as u8
                };
            size += slot_growth::slot_data_size(
                slot_type,
                capacity,
                !type_flags.no_hashmap_timestamps(),
                agg_type_byte,
            );
            size = align8(size);
            size += ttl_side_buffer_size(
                type_flags.has_ttl(),
                type_flags.has_evict_trigger(),
                capacity,
            );
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

            if has_array_fields(num_fields, field_types) {
                size += ARENA_HEADER_SIZE + arena_initial_capacity_64(capacity);
            }
            size = align8(size);
            size += ttl_side_buffer_size(
                type_flags.has_ttl(),
                type_flags.has_evict_trigger(),
                capacity,
            );
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
            // Truncate the inner type to its low nibble; invalid values reject
            // the program instead of reaching an unchecked enum conversion.
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

/// Write the 48-byte slot metadata record.
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

    bytes::write_u32(state, meta + SlotMetaOffset::OFFSET, data_offset);
    bytes::write_u32(state, meta + SlotMetaOffset::CAPACITY, capacity);
    bytes::write_u32(state, meta + SlotMetaOffset::SIZE, 0);

    state[(meta + SlotMetaOffset::TYPE_FLAGS) as usize] = type_flags.to_byte();
    state[(meta + SlotMetaOffset::AGG_TYPE) as usize] = agg_type_byte;
    state[(meta + SlotMetaOffset::CHANGE_FLAGS) as usize] = 0;
    state[(meta + SlotMetaOffset::TIMESTAMP_FIELD_IDX) as usize] = timestamp_field_idx;

    bytes::write_f32(state, meta + SlotMetaOffset::TTL_SECONDS, ttl_seconds);
    bytes::write_f32(state, meta + SlotMetaOffset::GRACE_SECONDS, grace_seconds);

    bytes::write_u32(
        state,
        meta + SlotMetaOffset::EVICTION_INDEX_OFFSET,
        eviction_index_offset,
    );
    bytes::write_u32(
        state,
        meta + SlotMetaOffset::EVICTION_INDEX_CAPACITY,
        eviction_index_capacity,
    );
    bytes::write_u32(state, meta + SlotMetaOffset::EVICTION_INDEX_SIZE, 0);
    bytes::write_u32(
        state,
        meta + SlotMetaOffset::EVICTED_BUFFER_OFFSET,
        evicted_buffer_offset,
    );
    bytes::write_u32(state, meta + SlotMetaOffset::EVICTED_COUNT, 0);

    state[(meta + SlotMetaOffset::START_OF) as usize] = start_of;
    state[(meta + SlotMetaOffset::START_OF + 1) as usize..meta as usize + SLOT_META_SIZE as usize]
        .fill(0);
}

/// Initialize a state buffer. `state` must be at least
/// `calculate_state_size(program)` bytes and zeroed so value regions start
/// deterministic.
pub fn init_state(
    state: &mut [u8],
    program: &[u8],
    accepted_program_magics: &[u32],
) -> Result<(), ErrorCode> {
    let Some(view) = validated_program(program, accepted_program_magics) else {
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
                    hash_table::init_external_keys(state, primary_data_offset, capacity);
                    if !type_flags.no_hashmap_timestamps() {
                        bytes::fill_f64(
                            state,
                            primary_data_offset + capacity * 8,
                            capacity,
                            f64::NEG_INFINITY,
                        );
                    }
                }
                SlotType::ConditionTree => {
                    bytes::write_u32(state, primary_data_offset, 1);
                    bytes::write_u32(state, primary_data_offset + 4, EMPTY_KEY);
                    if capacity > 0 {
                        let derived_facts_offset = primary_data_offset + CONDITION_TREE_STATE_BYTES;
                        for pos in 0..capacity {
                            bytes::write_u64(
                                state,
                                derived_facts_offset + pos * 8,
                                DERIVED_FACT_EMPTY_IDENTITY,
                            );
                        }
                        bytes::zero(state, derived_facts_offset + capacity * 8, capacity * 8);
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
                    hash_table::init_external_keys(state, primary_data_offset, capacity);
                }
                SlotType::Bitmap => {
                    bytes::zero(
                        state,
                        primary_data_offset,
                        slot_growth::slot_data_size(slot_type, capacity, false, agg_type_byte),
                    );
                }
                SlotType::Aggregate => {
                    aggregates::init_agg_slot(state, primary_data_offset, agg_type_byte);
                }
                SlotType::Scalar => {
                    bytes::zero(state, primary_data_offset, 8);
                    bytes::write_f64(state, primary_data_offset + 8, f64::NEG_INFINITY);
                }
                SlotType::Array => {
                    bytes::fill_u32(state, primary_data_offset, capacity, EMPTY_KEY);
                    bytes::fill_f64(
                        state,
                        primary_data_offset + capacity * 4,
                        capacity,
                        f64::NEG_INFINITY,
                    );
                }
                SlotType::StructMap
                | SlotType::StructMap2
                | SlotType::OrderedList
                | SlotType::Nested => {}
            }
            data_offset = align8(
                primary_data_offset
                    + slot_growth::slot_data_size(
                        slot_type,
                        capacity,
                        !type_flags.no_hashmap_timestamps(),
                        agg_type_byte,
                    ),
            );

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
                    let evicted_size = EVICTED_BUFFER_CAP * EVICTION_ENTRY_SIZE;
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

            write_overlay_meta(
                state,
                meta_base,
                &OverlayMeta {
                    data_offset,
                    capacity,
                    type_flags_byte: type_flags.to_byte(),
                    num_fields,
                    bitset_bytes: layout.bitset_bytes as u8,
                    row_size: layout.row_size as u16,
                    kind_byte: 0,
                },
            );

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

            if has_array_fields(num_fields, &field_types) {
                let arena_cap = arena_initial_capacity_64(capacity);
                // Arena header offset overlays GRACE_SECONDS.
                bytes::write_u32(state, meta_base + STRUCT_ARENA_HDR, data_offset);
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

                write_overlay_meta(
                    state,
                    meta_base,
                    &OverlayMeta {
                        data_offset,
                        capacity,
                        type_flags_byte,
                        num_fields,
                        bitset_bytes: layout.bitset_bytes as u8,
                        row_size: layout.row_size as u16,
                        kind_byte: elem_type,
                    },
                );

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

                write_overlay_meta(
                    state,
                    meta_base,
                    &OverlayMeta {
                        data_offset,
                        capacity,
                        type_flags_byte,
                        num_fields: 0,
                        bitset_bytes: 0,
                        row_size: elem_size as u16,
                        kind_byte: elem_type,
                    },
                );

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
            bytes::write_u32(state, meta_base + SlotMetaOffset::OFFSET, data_offset);
            bytes::write_u32(state, meta_base + SlotMetaOffset::CAPACITY, outer_cap);
            bytes::write_u32(state, meta_base + SlotMetaOffset::SIZE, 0);
            state[(meta_base + SlotMetaOffset::TYPE_FLAGS) as usize] = outer_type_flags_byte;
            state[(meta_base + SlotMetaOffset::AGG_TYPE) as usize] = inner_agg;
            state[(meta_base + SlotMetaOffset::CHANGE_FLAGS) as usize] = 0;
            // Metadata bytes 15–47 remain zero from the initialized buffer,
            // matching the layout's untouched reserved region.

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

/// Reinitialize a state buffer in place.
///
/// Zeroing first restores the fresh-allocation contract, including HASHMAP
/// values side-arrays that initialization leaves untouched.
pub fn reset_state(
    state: &mut [u8],
    program: &[u8],
    accepted_program_magics: &[u32],
) -> Result<(), ErrorCode> {
    if validated_program(program, accepted_program_magics).is_none() {
        return Err(ErrorCode::InvalidProgram);
    }
    state.fill(0);
    init_state(state, program, accepted_program_magics)
}
// =============================================================================
// Slot growth — calculate, allocate, copy, and rehash
// =============================================================================
//
// When a HashMap or HashSet exceeds 70% load, the VM reports NEEDS_GROWTH so
// the caller can allocate the larger state and retry the batch.

/// Per-slot facts the growth walkers re-derive from old-state metadata.
struct OldSlotMeta {
    offset: u32,
    capacity: u32,
    slot_type: SlotType,
    has_ttl: bool,
    has_evict_trigger: bool,
    has_hashmap_timestamps: bool,
    /// Dual-purpose byte 13: AggType for aggregate/scalar, num_fields for
    /// struct_map / ordered-list-struct.
    agg_type_byte: u8,
}

fn read_old_slot_meta(old_state: &[u8], slot_i: u32) -> OldSlotMeta {
    let slot = u8::try_from(slot_i)
        .unwrap_or_else(|_| columine_types::die!("invariant: grown-state slot index exceeds u8"));
    let view = SlotMetaView::read(old_state, slot);
    OldSlotMeta {
        offset: view.offset,
        capacity: view.capacity,
        slot_type: view.slot_type(),
        has_ttl: view.has_ttl(),
        has_evict_trigger: view.type_flags.has_evict_trigger(),
        has_hashmap_timestamps: view.has_hashmap_timestamp_storage(),
        agg_type_byte: view.agg_type_byte(old_state),
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
    let nf = overlay_num_fields(old_state, meta_base);
    let rs = overlay_row_size(old_state, meta_base);
    if slot_type == SlotType::StructMap2 {
        struct_map2_slot_data_size(align8(nf), cap, rs)
    } else {
        let has_ts = overlay_kind_byte(old_state, meta_base) != 0;
        struct_map_slot_data_size(align8(nf), cap, rs, has_ts)
    }
}

/// ORDERED_LIST metadata-driven primary size.
fn ordered_list_primary_size_from_meta(old_state: &[u8], meta_base: u32, cap: u32) -> u32 {
    let elem_type_byte = overlay_kind_byte(old_state, meta_base);
    let rs = overlay_row_size(old_state, meta_base);
    if elem_type_byte == 0xff {
        let nf = overlay_num_fields(old_state, meta_base);
        align8(nf) + cap * rs
    } else {
        cap * rs
    }
}

/// Nested-slot primary size from the prefix written at init. Inner capacity
/// is power-of-two'd the same way `init_state` / `calculate_state_size` do
/// before calling `nested_slot_data_size`.
fn nested_primary_size_from_prefix(old_state: &[u8], slot_offset: u32, outer_cap: u32) -> u32 {
    let prefix = nested::read_nested_prefix(old_state, slot_offset);
    nested::nested_slot_data_size(
        outer_cap,
        next_power_of_2(u32::from(prefix.inner_initial_cap)),
        prefix.inner_type,
        prefix.inner_agg_type_byte,
    )
}

/// Calculate grown-state size with 2× capacity for `grown_slot_idx`, reading
/// the slot metadata from the old state so already-grown states grow again
/// correctly.
pub fn calculate_grown_state_size(old_state: &[u8], grown_slot_idx: u32) -> u32 {
    let num_slots = u32::from(old_state[StateHeaderOffset::NUM_SLOTS as usize]);
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
                let arena_hdr_off = overlay_arena_hdr(old_state, meta_base);
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
            SlotType::Nested => nested_primary_size_from_prefix(old_state, m.offset, cap),
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

/// Copy old state into `new_state` (which must be zeroed and large enough for
/// [`calculate_grown_state_size`]), rehashing the grown slot and recomputing
/// every slot's offsets.
pub fn grow_state(
    old_state: &[u8],
    new_state: &mut [u8],
    grown_slot_idx: u32,
) -> Result<(), ErrorCode> {
    let num_slots = u32::from(old_state[StateHeaderOffset::NUM_SLOTS as usize]);

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

        // Primary data size (STRUCT_MAP, ORDERED_LIST, NESTED are layout-driven).
        let new_primary_size = match m.slot_type {
            SlotType::StructMap | SlotType::StructMap2 => {
                struct_map_primary_size_from_meta(old_state, meta_base, new_cap, m.slot_type)
            }
            SlotType::OrderedList => {
                ordered_list_primary_size_from_meta(old_state, meta_base, new_cap)
            }
            SlotType::Nested => nested_primary_size_from_prefix(old_state, old_offset, new_cap),
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
        bytes::write_u32(new_state, meta_base + SlotMetaOffset::OFFSET, new_offset);
        bytes::write_u32(new_state, meta_base + SlotMetaOffset::CAPACITY, new_cap);
        bytes::copy(
            new_state,
            meta_base + SlotMetaOffset::SIZE,
            old_state,
            meta_base + SlotMetaOffset::SIZE,
            SLOT_META_SIZE - SlotMetaOffset::SIZE,
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
                    let rehashed = slot_growth::grow_hash_map(
                        old_state,
                        new_state,
                        old_offset,
                        new_offset,
                        old_cap,
                        new_cap,
                        m.has_hashmap_timestamps,
                    );
                    bytes::write_u32(new_state, meta_base + SlotMetaOffset::SIZE, rehashed);
                }
                SlotType::HashSet => {
                    let rehashed = slot_growth::grow_hash_set(
                        old_state, new_state, old_offset, new_offset, old_cap, new_cap,
                    );
                    bytes::write_u32(new_state, meta_base + SlotMetaOffset::SIZE, rehashed);
                }
                SlotType::Bitmap => {
                    // Recompute storage bounds from the canonical bitmap
                    // capacity formula before copying the payload.
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
                    let nf = overlay_num_fields(old_state, meta_base);
                    let rs = overlay_row_size(old_state, meta_base);
                    let desc_size = align8(nf);
                    let new_keys_off = new_offset + desc_size;
                    let key_lane_bytes = if m.slot_type == SlotType::StructMap2 {
                        new_cap * 8
                    } else {
                        new_cap * 4
                    };
                    let new_rows_base = new_keys_off + key_lane_bytes;

                    let table = slot_growth::StructMapTable {
                        old_offset,
                        new_offset,
                        old_cap,
                        new_cap,
                        num_fields: nf,
                        row_size: rs,
                    };
                    let rehashed = if m.slot_type == SlotType::StructMap2 {
                        slot_growth::grow_struct_map2(old_state, new_state, &table)
                    } else {
                        slot_growth::grow_struct_map(old_state, new_state, &table)
                    };
                    bytes::write_u32(new_state, meta_base + SlotMetaOffset::SIZE, rehashed);

                    // Arena compaction: copy live array data into the new arena.
                    let old_arena_hdr_off = overlay_arena_hdr(old_state, meta_base);
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
                        bytes::write_u32(
                            new_state,
                            meta_base + STRUCT_ARENA_HDR,
                            new_arena_hdr_off,
                        );

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
                                let ft_byte = field_types[fi as usize];
                                let Some(field_type) = StructFieldType::from_u8(ft_byte) else {
                                    columine_types::die!(
                                        "invariant: struct-map descriptor contains an invalid field type"
                                    );
                                };
                                if !is_array_field_type(field_type) {
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

                                let elem_sz = arena_elem_size(field_type);
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
                    let elem_type_byte = overlay_kind_byte(old_state, meta_base);
                    let rs = overlay_row_size(old_state, meta_base);
                    let count = bytes::read_u32(old_state, meta_base + SlotMetaOffset::SIZE);

                    if elem_type_byte == 0xff {
                        let nf = overlay_num_fields(old_state, meta_base);
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
                    let arena_hdr = overlay_arena_hdr(old_state, meta_base);
                    if arena_hdr != 0 {
                        let arena_cap = bytes::read_u32(old_state, arena_hdr);
                        sz += ARENA_HEADER_SIZE + arena_cap;
                    }
                    sz
                }
                SlotType::OrderedList => {
                    ordered_list_primary_size_from_meta(old_state, meta_base, old_cap)
                }
                SlotType::Nested => nested_primary_size_from_prefix(old_state, old_offset, old_cap),
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
                let old_arena_hdr = overlay_arena_hdr(old_state, meta_base);
                if old_arena_hdr != 0 {
                    let struct_sz = struct_map_primary_size_from_meta(
                        old_state,
                        meta_base,
                        old_cap,
                        m.slot_type,
                    );
                    bytes::write_u32(
                        new_state,
                        meta_base + STRUCT_ARENA_HDR,
                        new_offset + struct_sz,
                    );
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
                    copied_evicted_count = old_evicted_count.min(EVICTED_BUFFER_CAP);
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
            let arena_hdr_off = overlay_arena_hdr(old_state, meta_base);
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
