//! Program bytecode and state-buffer lifecycle:
//!
//! - [`calculate_state_size`] computes the required state size.
//! - [`init_state`] initializes the state buffer.
//! - [`reset_state`] reinitializes it in place.
//! - [`calculate_grown_state_size`] and [`grow_state`] resize one slot.
//!
//! Struct-layout helpers used by initialization and execution live here.
//! Their formulas intentionally differ from the general type helpers:
//! - rows are padded to 4 bytes (`(row + 3) & !3`), and that padded value is
//!   stored in slot metadata;
//! - arenas reserve `capacity * 64` bytes rather than `capacity * 4`.
//!
//! Suffixes `_padded` and `_64` keep these domains distinct at call sites.
//!
//! `calculate_state_size` returns a typed admission Result. Stateful entry
//! points return `Result<(), ErrorCode>`, which wrappers map to numeric codes.
//! Malformed-but-length-valid bytecode with invalid slot-type nibbles returns
//! a typed `ProgramInvalidCause` or `InvalidProgram`, never a fabricated size.
//! Undersized or corrupt state images return `InvalidState` before writes.

use crate::element_body::{InvalidSource, SourceOp, visit_source_ops};
use crate::key_index::{HashIndex, IndexRegion, KeyIndex, KeyLanes};
use crate::meta::SlotMetaView;
use crate::struct_map::ArenaRegion;
use crate::{aggregates, bitmap_ops, bytes, hash_table, nested, slot_growth};
use bitmosaic::{EMPTY_U32_IMAGE, KeyWidth, image_header};
pub use columine_types::DEFAULT_ACCEPTED_PROGRAM_MAGICS;
use columine_types::operand::{Operands, read_index};
use columine_types::types::{
    AggType, CONDITION_TREE_STATE_BYTES, DERIVED_FACT_EMPTY_IDENTITY, EMPTY_KEY, ErrorCode,
    EvictionEntry, Opcode, PROGRAM_HASH_PREFIX, ProgramHeader, SLOT_META_SIZE, STATE_HEADER_SIZE,
    SlotMetaOffset, SlotType, SlotTypeFlags, StateFlags, StateHeaderOffset, StructFieldType,
    TOMBSTONE, align8, arena_elem_size, has_arena_fields, is_arena_field_type, next_power_of_2,
    state_num_slots,
};
use core::mem::size_of;

/// The `vm_get_needs_growth_slot` ABI value for a VM whose last batch did not
/// ask for growth ([`crate::vm::Vm::needs_growth_slot`] is `None`): no slot
/// has this index.
pub const NO_GROWTH_SLOT: u32 = u32::MAX;

pub const EVICTION_ENTRY_SIZE: u32 = size_of::<EvictionEntry>() as u32;
/// Closed-form capacity of the optional evicted-row side buffer.
pub const EVICTED_BUFFER_CAP: u32 = 1024;
/// Arena header: `[arena_capacity:u32][arena_used:u32]`.
pub const ARENA_HEADER_SIZE: u32 = nested::ARENA_HDR_SIZE;

/// Struct-map / ordered-list overlays of the TTL/grace metadata fields.
const STRUCT_NUM_FIELDS: u32 = SlotMetaOffset::AGG_TYPE;
const STRUCT_BITSET_BYTES: u32 = SlotMetaOffset::STRUCT_BITSET_BYTES;
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
    /// Struct maps: the key-lane kind bits ([`SlotMetaOffset::STRUCT_KEY_LANES`]).
    key_lanes: u8,
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
    state[(meta_base + SlotMetaOffset::STRUCT_KEY_LANES) as usize] = overlay.key_lanes;
}

fn overlay_num_fields(state: &[u8], meta_base: u32) -> u32 {
    u32::from(state[(meta_base + STRUCT_NUM_FIELDS) as usize])
}

fn overlay_row_size(state: &[u8], meta_base: u32) -> u32 {
    u32::from(bytes::read_u16(state, meta_base + STRUCT_ROW_SIZE))
}

pub(crate) fn overlay_kind_byte(state: &[u8], meta_base: u32) -> u8 {
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

/// A struct-map slot's primary bytes under key-index family `I`: descriptor,
/// index region, rows, and a one-key map's optional timestamp lane.
pub fn struct_map_primary_size<I: KeyIndex>(
    descriptor_size: u32,
    capacity: u32,
    row_size: u32,
    lanes: KeyLanes,
    has_timestamps: bool,
) -> u64 {
    u64::from(descriptor_size)
        + I::region_bytes(capacity, lanes)
        + u64::from(capacity) * (u64::from(row_size) + if has_timestamps { 8 } else { 0 })
}

/// [`HashIndex`] STRUCT_MAP slot data: descriptor + keys + rows + optional
/// timestamps.
pub fn struct_map_slot_data_size(
    descriptor_size: u32,
    capacity: u32,
    row_size: u32,
    has_timestamps: bool,
) -> u32 {
    u32::try_from(struct_map_primary_size::<HashIndex>(
        descriptor_size,
        capacity,
        row_size,
        KeyLanes::One(crate::key_index::LaneKind::Raw),
        has_timestamps,
    ))
    .unwrap_or(u32::MAX)
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
    num_slots: u32,
    init_code: &'a [u8],
    /// The reduce section, borrowed from the header: admission reads its
    /// bitmap algebra against the declarations the init section proved.
    reduce_code: &'a [u8],
}

/// Parse the shared program header and return `None` for an invalid program.
fn parse_program<'a>(
    program: &'a [u8],
    accepted_program_magics: &[u32],
) -> Option<ProgramView<'a>> {
    let sections = ProgramHeader::sections(program)?;
    if !accepted_program_magics.contains(&sections.header.magic) {
        return None;
    }
    Some(ProgramView {
        num_slots: sections.header.num_slots,
        init_code: sections.init_code,
        reduce_code: sections.reduce_code,
    })
}

/// The TTL parameters a `SLOT_DEF` with `has_ttl` carries after its
/// capacity: `ttl:f32, grace:f32, ts_col:index, start_of:u8`.
#[derive(Clone, Copy)]
pub(crate) struct TtlParams {
    ttl_seconds: f32,
    grace_seconds: f32,
    pub(crate) timestamp_col: u32,
    start_of: u8,
}

impl TtlParams {
    const NONE: Self = Self {
        ttl_seconds: 0.0,
        grace_seconds: 0.0,
        timestamp_col: 0,
        start_of: 0, // DurationUnit::None
    };

    pub(crate) fn read(code: &[u8], pc: usize) -> Option<(Self, usize)> {
        let mut operands = Operands::at(code, pc);
        let params = Self {
            ttl_seconds: operands.f32()?,
            grace_seconds: operands.f32()?,
            timestamp_col: operands.index()?,
            start_of: operands.byte()?,
        };
        Some((params, operands.pos()))
    }
}

/// The TTL v1 matrix is shared by program admission and persisted geometry:
/// only hash maps, hash sets and bitmaps carry expiry.
fn supports_ttl(slot_type: SlotType) -> bool {
    matches!(
        slot_type,
        SlotType::HashMap | SlotType::HashSet | SlotType::Bitmap
    )
}

/// SLOT_DEF capacity normalization shared by the size and init walkers.
fn slot_def_capacity(type_flags: SlotTypeFlags, cap_lo: u8, cap_hi: u8) -> Option<(SlotType, u32)> {
    let slot_type = type_flags.slot_type()?;
    if type_flags.has_ttl() && !supports_ttl(slot_type) {
        return None;
    }
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

/// The slot type an init record declares. `SLOT_ARRAY` has no type byte after
/// its slot index: its slot is `SlotType::Array`, as `init_state` writes it.
/// Every other record opens with its flags byte at `at`.
fn declared_type(op: Opcode, code: &[u8], at: usize) -> Option<SlotTypeFlags> {
    if op == Opcode::SlotArray {
        return Some(SlotTypeFlags::new(
            SlotType::Array,
            false,
            false,
            false,
            false,
        ));
    }
    Some(SlotTypeFlags::from_byte(*code.get(at)?))
}

/// Read the slot index that opens every init instruction and claim it with the
/// type that record declares: `None` when the index is malformed, out of range,
/// already defined, or its record carries no type byte.
fn claim_slot(
    code: &[u8],
    pc: usize,
    op: Opcode,
    declared: &mut [Option<SlotTypeFlags>],
) -> Option<(u32, usize)> {
    let (slot, next) = read_index(code, pc)?;
    let claimed = declared.get_mut(usize::try_from(slot).ok()?)?;
    if claimed.is_some() {
        return None;
    }
    *claimed = Some(declared_type(op, code, next)?);
    Some((slot, next))
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

/// The init grammar: every slot claimed once with the type its record declares,
/// then the reduce section's bitmap algebra checked against those declarations
/// at the successful `HALT`, where the declarations are complete.
fn validate_init_code(view: &ProgramView<'_>) -> Result<(), SizingRefusal> {
    // Every slot is defined exactly once, by an init instruction of at least
    // five bytes, so a header counting more slots than the init section has
    // bytes is refused before the bookkeeping for it is allocated.
    let malformed = SizingRefusal::Invalid(ProgramInvalidCause::MalformedProgram);
    let Ok(num_slots) = usize::try_from(view.num_slots) else {
        return Err(malformed);
    };
    if num_slots > view.init_code.len() {
        return Err(malformed);
    }
    let mut declared: Vec<Option<SlotTypeFlags>> = Vec::new();
    declared
        .try_reserve_exact(num_slots)
        .map_err(|_| SizingRefusal::OutOfMemory)?;
    declared.resize(num_slots, None);
    check_init_code(view, &mut declared).map_err(SizingRefusal::Invalid)
}

/// [`validate_init_code`] over its backed per-slot declaration table.
fn check_init_code(
    view: &ProgramView<'_>,
    declared: &mut [Option<SlotTypeFlags>],
) -> Result<(), ProgramInvalidCause> {
    let code = view.init_code;
    let mut pc = 0usize;

    while pc < code.len() {
        let Some(op) = Opcode::from_u8(code[pc]) else {
            return Err(ProgramInvalidCause::MalformedProgram);
        };
        pc += 1;
        if op == Opcode::Halt {
            if pc != code.len() || !declared.iter().all(Option::is_some) {
                return Err(ProgramInvalidCause::MalformedProgram);
            }
            return bitmap_algebra_admitted(view.reduce_code, declared);
        }
        let Some((_, after_slot)) = claim_slot(code, pc, op, declared) else {
            return Err(ProgramInvalidCause::MalformedProgram);
        };
        pc = after_slot;

        match op {
            Opcode::SlotDef => {
                let Some(operands) = code.get(pc..pc.saturating_add(3)) else {
                    return Err(ProgramInvalidCause::MalformedProgram);
                };
                let type_flags = SlotTypeFlags::from_byte(operands[0]);
                let cap_lo = operands[1];
                let Some(slot_type) = type_flags.slot_type() else {
                    return Err(ProgramInvalidCause::MalformedProgram);
                };
                if type_flags.has_ttl() && !supports_ttl(slot_type) {
                    return Err(ProgramInvalidCause::MalformedProgram);
                }
                if matches!(
                    slot_type,
                    SlotType::StructMap
                        | SlotType::StructMap2
                        | SlotType::OrderedList
                        | SlotType::Nested
                ) {
                    return Err(ProgramInvalidCause::MalformedProgram);
                }
                if slot_type == SlotType::Aggregate && !valid_aggregate_subtype(cap_lo) {
                    return Err(ProgramInvalidCause::MalformedProgram);
                }
                if slot_type == SlotType::Scalar && !valid_scalar_subtype(cap_lo) {
                    return Err(ProgramInvalidCause::MalformedProgram);
                }
                // The derived-facts region indexes with `capacity - 1` masks; a
                // capacity that is not a power of two would alias slots silently.
                let capacity = u16::from_le_bytes([cap_lo, operands[2]]);
                if slot_type == SlotType::ConditionTree
                    && capacity > 0
                    && !capacity.is_power_of_two()
                {
                    return Err(ProgramInvalidCause::MalformedProgram);
                }
                if slot_type == SlotType::HashMap
                    && type_flags.has_ttl()
                    && type_flags.no_hashmap_timestamps()
                {
                    return Err(ProgramInvalidCause::MalformedProgram);
                }
                pc += 3;
                if type_flags.has_ttl() {
                    let Some((ttl, after)) = TtlParams::read(code, pc) else {
                        return Err(ProgramInvalidCause::MalformedProgram);
                    };
                    if ttl.start_of > 8 {
                        return Err(ProgramInvalidCause::MalformedProgram);
                    }
                    pc = after;
                }
            }
            Opcode::SlotArray => {
                if code.get(pc..pc.saturating_add(3)).is_none() {
                    return Err(ProgramInvalidCause::MalformedProgram);
                }
                pc += 3;
            }
            Opcode::SlotStructMap | Opcode::SlotStructMap2 => {
                let Some(operands) = code.get(pc..pc.saturating_add(4)) else {
                    return Err(ProgramInvalidCause::MalformedProgram);
                };
                let type_flags = SlotTypeFlags::from_byte(operands[0]);
                let expected = if op == Opcode::SlotStructMap {
                    SlotType::StructMap
                } else {
                    SlotType::StructMap2
                };
                if type_flags.has_ttl()
                    || type_flags.slot_type() != Some(expected)
                    || KeyLanes::declared(op == Opcode::SlotStructMap2, type_flags).is_none()
                {
                    return Err(ProgramInvalidCause::MalformedProgram);
                }
                let num_fields = usize::from(operands[3]);
                pc += 4;
                let Some(field_types) = code.get(pc..pc.saturating_add(num_fields)) else {
                    return Err(ProgramInvalidCause::MalformedProgram);
                };
                if field_types.iter().any(|field_type| {
                    StructFieldType::from_u8(*field_type).is_none()
                        || (op == Opcode::SlotStructMap2
                            && *field_type == StructFieldType::Vo as u8)
                }) {
                    return Err(ProgramInvalidCause::MalformedProgram);
                }
                pc += num_fields;
            }
            Opcode::SlotOrderedList => {
                let Some(operands) = code.get(pc..pc.saturating_add(4)) else {
                    return Err(ProgramInvalidCause::MalformedProgram);
                };
                let type_flags = SlotTypeFlags::from_byte(operands[0]);
                // Key-lane bits are a struct map's; an ordered list has no key.
                if type_flags.slot_type() != Some(SlotType::OrderedList)
                    || type_flags.has_ttl()
                    || type_flags.struct_key_lanes() != 0
                {
                    return Err(ProgramInvalidCause::MalformedProgram);
                }
                let elem_type = operands[3];
                pc += 4;
                if elem_type == 0xff {
                    let Some(&num_fields) = code.get(pc) else {
                        return Err(ProgramInvalidCause::MalformedProgram);
                    };
                    pc += 1;
                    let Some(field_types) =
                        code.get(pc..pc.saturating_add(usize::from(num_fields)))
                    else {
                        return Err(ProgramInvalidCause::MalformedProgram);
                    };
                    if field_types.iter().any(|field_type| {
                        StructFieldType::from_u8(*field_type).is_none()
                            || *field_type == StructFieldType::Vo as u8
                    }) {
                        return Err(ProgramInvalidCause::MalformedProgram);
                    }
                    pc += usize::from(num_fields);
                } else if StructFieldType::from_u8(elem_type).is_none()
                    || elem_type == StructFieldType::Vo as u8
                {
                    return Err(ProgramInvalidCause::MalformedProgram);
                }
            }
            Opcode::SlotNested => {
                let Some(operands) = code.get(pc..pc.saturating_add(7)) else {
                    return Err(ProgramInvalidCause::MalformedProgram);
                };
                let flags = SlotTypeFlags::from_byte(operands[0]);
                if flags.has_ttl() || flags.slot_type() != Some(SlotType::Nested) {
                    return Err(ProgramInvalidCause::MalformedProgram);
                }
                let Some(inner_type) = SlotType::from_u8(operands[3] & 0x0f) else {
                    return Err(ProgramInvalidCause::MalformedProgram);
                };
                if inner_type == SlotType::Aggregate && !valid_aggregate_subtype(operands[6]) {
                    return Err(ProgramInvalidCause::MalformedProgram);
                }
                pc += 7;
            }
            _ => return Err(ProgramInvalidCause::MalformedProgram),
        }
    }

    Err(ProgramInvalidCause::MalformedProgram)
}

/// Whether the reduce section's bitmap algebra agrees with the declarations.
/// A slot×slot algebra op folds its source into its target, so a source that
/// carries timestamps cannot fold into a target that tracks none. That layout
/// change is refused here, never applied silently.
fn bitmap_algebra_admitted(
    reduce: &[u8],
    declared: &[Option<SlotTypeFlags>],
) -> Result<(), ProgramInvalidCause> {
    visit_source_ops(reduce, 0, &mut |source| {
        check_bitmap_algebra(&source, declared)
    })
}

fn check_bitmap_algebra(
    source: &SourceOp<'_>,
    declared: &[Option<SlotTypeFlags>],
) -> Result<(), ProgramInvalidCause> {
    if !matches!(
        source.op,
        Opcode::BatchBitmapAnd
            | Opcode::BatchBitmapOr
            | Opcode::BatchBitmapAndNot
            | Opcode::BatchBitmapXor
    ) {
        return Ok(());
    }
    let mut r = Operands::at(source.code, source.operands);
    let target = r.index().ok_or(ProgramInvalidCause::MalformedProgram)?;
    let source_slot = r.index().ok_or(ProgramInvalidCause::MalformedProgram)?;
    let target_flags =
        declared_bitmap(declared, target).ok_or(ProgramInvalidCause::MalformedProgram)?;
    let source_flags =
        declared_bitmap(declared, source_slot).ok_or(ProgramInvalidCause::MalformedProgram)?;
    if source_flags.has_ttl() && !target_flags.has_ttl() {
        return Err(ProgramInvalidCause::BitmapTtlSourceToUntrackedTarget {
            source: source_slot,
            target,
        });
    }
    Ok(())
}

/// The declaration of `slot`, when it exists and is a bitmap.
fn declared_bitmap(declared: &[Option<SlotTypeFlags>], slot: u32) -> Option<SlotTypeFlags> {
    let flags = (*declared.get(usize::try_from(slot).ok()?)?)?;
    (flags.slot_type() == Some(SlotType::Bitmap)).then_some(flags)
}

fn validated_program<'a>(
    program: &'a [u8],
    accepted_program_magics: &[u32],
) -> Result<ProgramView<'a>, SizingRefusal> {
    let view = parse_program(program, accepted_program_magics).ok_or(SizingRefusal::Invalid(
        ProgramInvalidCause::MalformedProgram,
    ))?;
    validate_init_code(&view)?;
    Ok(view)
}
// =============================================================================
// State Size Calculation — vm_calculate_state_size
// =============================================================================

/// Calculate the required state-buffer size, or retain the actual admission
/// cause, or `OutOfMemory` when checking the program's declarations could not
/// be backed.
pub fn calculate_state_size(
    program: &[u8],
    accepted_program_magics: &[u32],
) -> Result<u32, SizingRefusal> {
    calculate_state_size_in::<HashIndex>(program, accepted_program_magics)
}

/// [`calculate_state_size`] for an image of key-index family `I`.
pub fn calculate_state_size_in<I: KeyIndex>(
    program: &[u8],
    accepted_program_magics: &[u32],
) -> Result<u32, SizingRefusal> {
    let view = validated_program(program, accepted_program_magics)?;
    calculate_state_size_from_view::<I>(&view).map_err(SizingRefusal::Invalid)
}

fn calculate_state_size_from_view<I: KeyIndex>(
    view: &ProgramView<'_>,
) -> Result<u32, ProgramInvalidCause> {
    let init_code = view.init_code;

    // Header + slot metadata (48 bytes per slot).
    let mut size =
        (u64::from(STATE_HEADER_SIZE) + u64::from(view.num_slots) * u64::from(SLOT_META_SIZE) + 7)
            & !7;
    if size > u64::from(u32::MAX) {
        return Err(ProgramInvalidCause::MalformedProgram);
    }

    let mut pc = 0usize;
    while pc < init_code.len() {
        let op = init_code[pc];
        pc += 1;
        if op == Opcode::Halt as u8 {
            break;
        }
        // Every init instruction opens with its slot index; validation has
        // already claimed it, so only its length matters here.
        let Some((_, after_slot)) = read_index(init_code, pc) else {
            return Err(ProgramInvalidCause::MalformedProgram);
        };
        pc = after_slot;

        if op == Opcode::SlotDef as u8 {
            // type_flags:u8, cap_lo:u8, cap_hi:u8 [, ttl params]
            let type_flags = SlotTypeFlags::from_byte(init_code[pc]);
            let cap_lo = init_code[pc + 1];
            let cap_hi = init_code[pc + 2];
            // Invalid slot-type nibbles make the program invalid.
            let Some((slot_type, capacity)) = slot_def_capacity(type_flags, cap_lo, cap_hi) else {
                return Err(ProgramInvalidCause::MalformedProgram);
            };
            pc += 3;

            if slot_type == SlotType::HashMap
                && type_flags.has_ttl()
                && type_flags.no_hashmap_timestamps()
            {
                return Err(ProgramInvalidCause::MalformedProgram);
            }

            if type_flags.has_ttl() {
                let Some((_, after_ttl)) = TtlParams::read(init_code, pc) else {
                    return Err(ProgramInvalidCause::MalformedProgram);
                };
                pc = after_ttl;
            }

            let agg_type_byte =
                if matches!(slot_type, SlotType::Aggregate | SlotType::Scalar) && cap_lo > 0 {
                    cap_lo
                } else {
                    AggType::Sum as u8
                };
            size += u64::from(slot_growth::slot_data_size(
                slot_type,
                capacity,
                !type_flags.no_hashmap_timestamps(),
                agg_type_byte,
            ));
            size = (size + 7) & !7;
            size += u64::from(ttl_side_buffer_size(
                type_flags.has_ttl(),
                type_flags.has_evict_trigger(),
                capacity,
            ));
        } else if op == Opcode::SlotArray as u8 {
            let cap_lo = init_code[pc];
            let cap_hi = init_code[pc + 1];
            let mut capacity = (u32::from(cap_hi) << 8) | u32::from(cap_lo);
            if capacity == 0 {
                capacity = 1024;
            }
            capacity = next_power_of_2(capacity * 2);
            pc += 3;

            size += u64::from(capacity) * 12;
            size = (size + 7) & !7;
        } else if op == Opcode::SlotStructMap as u8 || op == Opcode::SlotStructMap2 as u8 {
            let pair = op == Opcode::SlotStructMap2 as u8;
            let type_flags = SlotTypeFlags::from_byte(init_code[pc]);
            let Some(lanes) = KeyLanes::declared(pair, type_flags) else {
                return Err(ProgramInvalidCause::MalformedProgram);
            };
            let cap_lo = init_code[pc + 1];
            let cap_hi = init_code[pc + 2];
            let num_fields = init_code[pc + 3];
            pc += 4;

            let mut declared = (u32::from(cap_hi) << 8) | u32::from(cap_lo);
            if declared == 0 {
                declared = 1024;
            }
            let capacity = I::capacity_for(declared);

            let field_types = &init_code[pc..pc + usize::from(num_fields)];
            pc += usize::from(num_fields);

            let layout = compute_struct_row_layout_padded(num_fields, field_types);
            size += struct_map_primary_size::<I>(
                layout.descriptor_size,
                capacity,
                layout.row_size,
                lanes,
                false,
            );

            if has_arena_fields(num_fields, field_types) {
                size +=
                    u64::from(ARENA_HEADER_SIZE) + u64::from(arena_initial_capacity_64(capacity));
            }
            size = (size + 7) & !7;
            size += u64::from(ttl_side_buffer_size(
                type_flags.has_ttl(),
                type_flags.has_evict_trigger(),
                capacity,
            ));
        } else if op == Opcode::SlotOrderedList as u8 {
            let cap_lo = init_code[pc + 1];
            let cap_hi = init_code[pc + 2];
            let elem_type = init_code[pc + 3];
            pc += 4;

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
                size += u64::from(layout.descriptor_size)
                    + u64::from(capacity) * u64::from(layout.row_size);
            } else {
                let elem_size = columine_types::types::struct_field_size(
                    columine_types::types::StructFieldType::from_u8(elem_type).unwrap_or_else(
                        || columine_types::die!("invariant: ordered-list element type is invalid"),
                    ),
                );
                size += u64::from(capacity) * u64::from(elem_size);
            }
            size = (size + 7) & !7;
        } else if op == Opcode::SlotNested as u8 {
            let outer_cap_lo = init_code[pc + 1];
            let outer_cap_hi = init_code[pc + 2];
            let inner_type_byte = init_code[pc + 3];
            let inner_cap_lo = init_code[pc + 4];
            let inner_cap_hi = init_code[pc + 5];
            let inner_agg_type_byte = init_code[pc + 6];
            pc += 7;

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
                return Err(ProgramInvalidCause::MalformedProgram);
            };
            // Only interpret as AggType for AGGREGATE inner containers.
            let inner_agg = if inner_type == SlotType::Aggregate && inner_agg_type_byte >= 1 {
                inner_agg_type_byte
            } else {
                1 // SUM
            };

            let inner_size = u64::from(nested::inner_container_size(
                inner_type,
                next_power_of_2(inner_initial_cap),
                inner_agg,
            ));
            size += u64::from(nested::NESTED_PREFIX_SIZE + ARENA_HEADER_SIZE)
                + u64::from(outer_cap) * (8 + ((inner_size + 7) & !7));
            size = (size + 7) & !7;
        } else {
            return Err(ProgramInvalidCause::MalformedProgram);
        }
        if size > u64::from(u32::MAX) {
            return Err(ProgramInvalidCause::MalformedProgram);
        }
    }

    u32::try_from(size).map_err(|_| ProgramInvalidCause::MalformedProgram)
}

// =============================================================================
// State Initialization — vm_init_state / vm_reset_state
// =============================================================================

/// Write the 48-byte slot metadata record.
#[allow(clippy::too_many_arguments)]
fn write_slot_meta(
    state: &mut [u8],
    slot: u32,
    data_offset: u32,
    capacity: u32,
    type_flags: SlotTypeFlags,
    agg_type_byte: u8,
    ttl: TtlParams,
    eviction_index_offset: u32,
    eviction_index_capacity: u32,
    evicted_buffer_offset: u32,
) {
    let meta = STATE_HEADER_SIZE + slot * SLOT_META_SIZE;

    bytes::write_u32(state, meta + SlotMetaOffset::OFFSET, data_offset);
    bytes::write_u32(state, meta + SlotMetaOffset::CAPACITY, capacity);
    bytes::write_u32(state, meta + SlotMetaOffset::SIZE, 0);

    state[(meta + SlotMetaOffset::TYPE_FLAGS) as usize] = type_flags.to_byte();
    state[(meta + SlotMetaOffset::AGG_TYPE) as usize] = agg_type_byte;
    state[(meta + SlotMetaOffset::CHANGE_FLAGS) as usize] = 0;
    state[(meta + SlotMetaOffset::START_OF) as usize] = ttl.start_of;

    bytes::write_f32(state, meta + SlotMetaOffset::TTL_SECONDS, ttl.ttl_seconds);
    bytes::write_f32(
        state,
        meta + SlotMetaOffset::GRACE_SECONDS,
        ttl.grace_seconds,
    );

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
    bytes::write_u32(
        state,
        meta + SlotMetaOffset::TIMESTAMP_COL,
        ttl.timestamp_col,
    );
}

/// Initialize a state buffer. `state` must be at least
/// `calculate_state_size(program)` bytes and zeroed so value regions start
/// deterministic.
pub fn init_state(
    state: &mut [u8],
    program: &[u8],
    accepted_program_magics: &[u32],
) -> Result<(), ErrorCode> {
    init_state_in::<HashIndex>(state, program, accepted_program_magics)
}

/// [`init_state`] for an image of key-index family `I`.
pub fn init_state_in<I: KeyIndex>(
    state: &mut [u8],
    program: &[u8],
    accepted_program_magics: &[u32],
) -> Result<(), ErrorCode> {
    let view = validated_program(program, accepted_program_magics).map_err(SizingRefusal::code)?;
    let required =
        calculate_state_size_from_view::<I>(&view).map_err(|_| ErrorCode::InvalidProgram)?;
    if state.len() < required as usize {
        return Err(ErrorCode::InvalidState);
    }
    init_state_from_view::<I>(state, program, view)
}

fn init_state_from_view<I: KeyIndex>(
    state: &mut [u8],
    program: &[u8],
    view: ProgramView<'_>,
) -> Result<(), ErrorCode> {
    let content = &program[PROGRAM_HASH_PREFIX as usize..];
    let num_slots = view.num_slots;
    let init_code = view.init_code;

    // State header: magic(4) + format_version(1) + program_version(2)
    // + ruleset_version(2) + reserved(1) + num_vars(1) + num_bitvecs(1)
    // + flags(1) + derived facts offset(4) + reserved(2) + derived fact
    // schemas(1) + derived facts change flag(1) + reserved(3) + num_slots(4)
    // + derived facts slot(4).
    state[..STATE_HEADER_SIZE as usize].fill(0);
    bytes::write_u32(state, 0, I::STATE_MAGIC);
    state[StateHeaderOffset::FORMAT_VERSION as usize] = I::FORMAT_VERSION;
    state[StateHeaderOffset::PROGRAM_VERSION as usize] = content[4];
    state[StateHeaderOffset::PROGRAM_VERSION as usize + 1] = content[5];
    bytes::write_u32(state, StateHeaderOffset::NUM_SLOTS, num_slots);

    let mut data_offset = align8(STATE_HEADER_SIZE + num_slots * SLOT_META_SIZE);

    let mut pc = 0usize;
    while pc < init_code.len() {
        let op = init_code[pc];
        pc += 1;
        if op == Opcode::Halt as u8 {
            break;
        }
        let Some((slot, after_slot)) = read_index(init_code, pc) else {
            return Err(ErrorCode::InvalidProgram);
        };
        pc = after_slot;

        if op == Opcode::SlotDef as u8 {
            let type_flags = SlotTypeFlags::from_byte(init_code[pc]);
            let cap_lo = init_code[pc + 1];
            let cap_hi = init_code[pc + 2];
            let Some((slot_type, capacity)) = slot_def_capacity(type_flags, cap_lo, cap_hi) else {
                return Err(ErrorCode::InvalidProgram);
            };
            // For AGGREGATE/SCALAR: cap_lo encodes the AggType subtype (raw byte).
            let is_subtyped = matches!(slot_type, SlotType::Aggregate | SlotType::Scalar);
            let agg_type_byte = if is_subtyped && cap_lo > 0 { cap_lo } else { 1 };
            pc += 3;

            if slot_type == SlotType::HashMap
                && type_flags.has_ttl()
                && type_flags.no_hashmap_timestamps()
            {
                return Err(ErrorCode::InvalidProgram);
            }

            let ttl = if type_flags.has_ttl() {
                let Some((ttl, after_ttl)) = TtlParams::read(init_code, pc) else {
                    return Err(ErrorCode::InvalidProgram);
                };
                pc = after_ttl;
                ttl
            } else {
                TtlParams::NONE
            };

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
                        // The slot's metadata, written below, holds the table's
                        // u32 capacity and its occupied-cell count (SIZE, 0).
                        bytes::write_u32(state, StateHeaderOffset::DERIVED_FACTS_SLOT, slot);
                        state[StateHeaderOffset::FLAGS as usize] |= StateFlags::HAS_RETE;
                    }
                }
                SlotType::HashSet => {
                    hash_table::init_external_keys(state, primary_data_offset, capacity);
                }
                SlotType::Bitmap => {
                    // The slot holds the native image directly: zero the
                    // region, then admit the canonical empty image. An
                    // all-zero slot is no native encoding.
                    let size =
                        slot_growth::slot_data_size(slot_type, capacity, false, agg_type_byte);
                    bytes::zero(state, primary_data_offset, size);
                    state[primary_data_offset as usize
                        ..primary_data_offset as usize + EMPTY_U32_IMAGE.len()]
                        .copy_from_slice(&EMPTY_U32_IMAGE);
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
                ttl,
                eviction_index_offset,
                eviction_index_capacity,
                evicted_buffer_offset,
            );
        } else if op == Opcode::SlotArray as u8 {
            let cap_lo = init_code[pc];
            let cap_hi = init_code[pc + 1];
            let mut capacity = (u32::from(cap_hi) << 8) | u32::from(cap_lo);
            if capacity == 0 {
                capacity = 1024;
            }
            capacity = next_power_of_2(capacity * 2);
            pc += 3;

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
                TtlParams::NONE,
                0,
                0,
                0,
            );
        } else if op == Opcode::SlotStructMap as u8 || op == Opcode::SlotStructMap2 as u8 {
            let pair = op == Opcode::SlotStructMap2 as u8;
            let type_flags = SlotTypeFlags::from_byte(init_code[pc]);
            let lanes = KeyLanes::declared(pair, type_flags).ok_or(ErrorCode::InvalidProgram)?;
            let cap_lo = init_code[pc + 1];
            let cap_hi = init_code[pc + 2];
            let num_fields = init_code[pc + 3];
            pc += 4;

            let mut declared = (u32::from(cap_hi) << 8) | u32::from(cap_lo);
            if declared == 0 {
                declared = 1024;
            }
            let capacity = I::capacity_for(declared);

            let field_types = &init_code[pc..pc + usize::from(num_fields)];
            pc += usize::from(num_fields);

            let layout = compute_struct_row_layout_padded(num_fields, field_types);
            let meta_base = STATE_HEADER_SIZE + slot * SLOT_META_SIZE;

            write_overlay_meta(
                state,
                meta_base,
                &OverlayMeta {
                    data_offset,
                    capacity,
                    type_flags_byte: type_flags.without_struct_key_lanes().to_byte(),
                    num_fields,
                    bitset_bytes: layout.bitset_bytes as u8,
                    row_size: layout.row_size as u16,
                    kind_byte: 0,
                    key_lanes: lanes.byte(),
                },
            );

            // Field descriptor, the family's empty index region, then zeroed rows.
            bytes::copy(state, data_offset, field_types, 0, u32::from(num_fields));
            let region = IndexRegion {
                at: data_offset + layout.descriptor_size,
                capacity,
                lanes,
            };
            I::init(state, region);
            let rows_offset =
                u32::try_from(u64::from(region.at) + I::region_bytes(capacity, lanes))
                    .map_err(|_| ErrorCode::InvalidProgram)?;
            bytes::zero(state, rows_offset, capacity * layout.row_size);

            data_offset = rows_offset + capacity * layout.row_size;

            if has_arena_fields(num_fields, field_types) {
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
            let type_flags_byte = init_code[pc];
            let cap_lo = init_code[pc + 1];
            let cap_hi = init_code[pc + 2];
            let elem_type = init_code[pc + 3];
            pc += 4;

            let mut capacity = (u32::from(cap_hi) << 8) | u32::from(cap_lo);
            if capacity == 0 {
                capacity = 1024;
            }
            capacity = next_power_of_2(capacity);

            let meta_base = STATE_HEADER_SIZE + slot * SLOT_META_SIZE;

            if elem_type == 0xff {
                // Struct list.
                let num_fields = init_code[pc];
                let field_types = &init_code[pc + 1..pc + 1 + usize::from(num_fields)];
                pc += 1 + usize::from(num_fields);

                let layout = compute_struct_row_layout_padded(num_fields, field_types);

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
                        key_lanes: 0,
                    },
                );

                bytes::copy(state, data_offset, field_types, 0, u32::from(num_fields));
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
                        key_lanes: 0,
                    },
                );

                bytes::zero(state, data_offset, capacity * elem_size);
                data_offset += capacity * elem_size;
            }
            data_offset = align8(data_offset);
        } else if op == Opcode::SlotNested as u8 {
            let outer_type_flags_byte = init_code[pc];
            let outer_cap_lo = init_code[pc + 1];
            let outer_cap_hi = init_code[pc + 2];
            let inner_type_byte = init_code[pc + 3];
            let inner_cap_lo = init_code[pc + 4];
            let inner_cap_hi = init_code[pc + 5];
            let inner_agg_type_byte = init_code[pc + 6];
            pc += 7;

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

            let meta_base = STATE_HEADER_SIZE + slot * SLOT_META_SIZE;
            bytes::write_u32(state, meta_base + SlotMetaOffset::OFFSET, data_offset);
            bytes::write_u32(state, meta_base + SlotMetaOffset::CAPACITY, outer_cap);
            bytes::write_u32(state, meta_base + SlotMetaOffset::SIZE, 0);
            state[(meta_base + SlotMetaOffset::TYPE_FLAGS) as usize] = outer_type_flags_byte;
            state[(meta_base + SlotMetaOffset::AGG_TYPE) as usize] = inner_agg;
            state[(meta_base + SlotMetaOffset::CHANGE_FLAGS) as usize] = 0;
            // Metadata bytes 15–47 remain zero from the initialized buffer.

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
    reset_state_in::<HashIndex>(state, program, accepted_program_magics)
}

/// [`reset_state`] for an image of key-index family `I`.
pub fn reset_state_in<I: KeyIndex>(
    state: &mut [u8],
    program: &[u8],
    accepted_program_magics: &[u32],
) -> Result<(), ErrorCode> {
    let view = validated_program(program, accepted_program_magics).map_err(SizingRefusal::code)?;
    let required =
        calculate_state_size_from_view::<I>(&view).map_err(|_| ErrorCode::InvalidProgram)?;
    if state.len() < required as usize {
        return Err(ErrorCode::InvalidState);
    }
    state.fill(0);
    init_state_from_view::<I>(state, program, view)
}
// =============================================================================
// Slot growth — calculate, allocate, copy, and rehash
// =============================================================================
//
// When a HashMap or HashSet exceeds 70% load, the VM reports NEEDS_GROWTH so
// the caller can allocate the larger state and retry the batch.

/// Validate the actual image header before any metadata access: its magic
/// and format version are key-index family `I`'s, or the image is refused.
pub(crate) fn state_geometry<I: KeyIndex>(state: &[u8]) -> Result<(u32, u32), ErrorCode> {
    if state.len() < STATE_HEADER_SIZE as usize
        || bytes::read_u32(state, StateHeaderOffset::MAGIC) != I::STATE_MAGIC
        || state[StateHeaderOffset::FORMAT_VERSION as usize] != I::FORMAT_VERSION
    {
        return Err(ErrorCode::InvalidState);
    }
    let slots = state_num_slots(state);
    let end =
        checked_size(u64::from(STATE_HEADER_SIZE) + u64::from(slots) * u64::from(SLOT_META_SIZE))?;
    let end = checked_align8(end)?;
    if end as usize > state.len() {
        return Err(ErrorCode::InvalidState);
    }
    Ok((slots, end))
}

fn checked_size(size: u64) -> Result<u32, ErrorCode> {
    u32::try_from(size).map_err(|_| ErrorCode::InvalidState)
}

fn checked_align8(size: u32) -> Result<u32, ErrorCode> {
    size.checked_add(7)
        .map(|size| size & !7)
        .ok_or(ErrorCode::InvalidState)
}

/// Preserve the established minimum-16 capacity normalization without overflow.
pub(crate) fn grown_capacity(capacity: u32) -> Result<u32, ErrorCode> {
    capacity
        .checked_mul(2)
        .and_then(|capacity| capacity.max(16).checked_next_power_of_two())
        .ok_or(ErrorCode::InvalidState)
}

/// Physical ownership follows offsets, not metadata slot-index order.
fn slot_limit(state: &[u8], slot: u32, slots: u32, metadata_end: u32) -> Result<u32, ErrorCode> {
    let offset = slot_offset(state, slot);
    let mut limit = u32::try_from(state.len()).map_err(|_| ErrorCode::InvalidState)?;
    if offset < metadata_end || offset > limit || !offset.is_multiple_of(8) {
        return Err(ErrorCode::InvalidState);
    }
    for other in 0..slots {
        if other == slot {
            continue;
        }
        let other_offset = slot_offset(state, other);
        if other_offset < metadata_end
            || other_offset > state.len() as u32
            || !other_offset.is_multiple_of(8)
            || other_offset == offset
        {
            return Err(ErrorCode::InvalidState);
        }
        if other_offset > offset {
            limit = limit.min(other_offset);
        }
    }
    Ok(limit)
}

fn checked_struct_layout(
    state: &[u8],
    meta: u32,
    kind: SlotType,
) -> Result<columine_types::types::StructRowLayout, ErrorCode> {
    let offset = bytes::read_u32(state, meta + SlotMetaOffset::OFFSET);
    let nf = state[(meta + STRUCT_NUM_FIELDS) as usize];
    let end = offset
        .checked_add(u32::from(nf))
        .ok_or(ErrorCode::InvalidState)?;
    let descriptor = state
        .get(offset as usize..end as usize)
        .ok_or(ErrorCode::InvalidState)?;
    for &field in descriptor {
        StructFieldType::from_u8(field).ok_or(ErrorCode::InvalidState)?;
        if kind != SlotType::StructMap && field == StructFieldType::Vo as u8 {
            return Err(ErrorCode::InvalidState);
        }
    }
    let layout = compute_struct_row_layout_padded(nf, descriptor);
    if overlay_row_size(state, meta) != layout.row_size
        || u32::from(state[(meta + STRUCT_BITSET_BYTES) as usize]) != layout.bitset_bytes
    {
        return Err(ErrorCode::InvalidState);
    }
    Ok(layout)
}

/// Bind one slot only after validating its actual image and physical ownership.
/// Readers derive geometry from this metadata, never caller-supplied capacities.
pub fn bind_slot(state: &[u8], slot: u32) -> Result<SlotMetaView, ErrorCode> {
    bind_slot_in::<HashIndex>(state, slot)
}

/// [`bind_slot`] for an image of key-index family `I`.
pub fn bind_slot_in<I: KeyIndex>(state: &[u8], slot: u32) -> Result<SlotMetaView, ErrorCode> {
    let (slots, metadata_end) = state_geometry::<I>(state)?;
    if slot >= slots {
        return Err(ErrorCode::InvalidSlot);
    }
    let limit = slot_limit(state, slot, slots, metadata_end)?;
    validate_slot_geometry::<I>(state, slot, limit, OuterPointers::Walk)?;
    Ok(SlotMetaView::read(state, slot))
}

/// Check a whole state image the way [`bind_slot`] checks one slot: header,
/// slot table, and every slot's physical geometry within the image (primary
/// storage, arenas, bitmap image header, TTL and evicted buffers) and every
/// nested slot's outer pointers. A state that enters the VM from outside, a
/// restored checkpoint or any host-supplied image, is checked here once.
/// Refuses with `InvalidState`.
pub fn validate_state(state: &[u8]) -> Result<(), ErrorCode> {
    validate_state_in::<HashIndex>(state)
}

/// [`validate_state`] for an image of key-index family `I`: an image whose
/// header names another family is refused.
pub fn validate_state_in<I: KeyIndex>(state: &[u8]) -> Result<(), ErrorCode> {
    validate_slots::<I>(state, OuterPointers::Walk)
}

/// [`validate_state`] without the walk over nested outer pointers, which
/// costs a nested slot's capacity: what a batch checks of the image it is
/// handed, in time proportional to the slot count. Kernels check each outer
/// pointer they follow instead.
pub(crate) fn validate_layout<I: KeyIndex>(state: &[u8]) -> Result<(), ErrorCode> {
    validate_slots::<I>(state, OuterPointers::Skip)
}

/// Whether slot validation reads every nested outer pointer.
#[derive(Clone, Copy, Eq, PartialEq)]
enum OuterPointers {
    Walk,
    Skip,
}

fn validate_slots<I: KeyIndex>(state: &[u8], outer: OuterPointers) -> Result<(), ErrorCode> {
    let (slots, metadata_end) = state_geometry::<I>(state)?;
    let len = u32::try_from(state.len()).map_err(|_| ErrorCode::InvalidState)?;
    if offsets_ascend(state, slots, metadata_end, len) {
        // Every offset is in range, aligned and distinct, and the next
        // higher one is the next slot's: the bounds `slot_limit` would find.
        for slot in 0..slots {
            let limit = if slot + 1 < slots {
                slot_offset(state, slot + 1)
            } else {
                len
            };
            validate_slot_geometry::<I>(state, slot, limit, outer)?;
        }
        return Ok(());
    }
    for slot in 0..slots {
        let limit = slot_limit(state, slot, slots, metadata_end)?;
        validate_slot_geometry::<I>(state, slot, limit, outer)?;
    }
    Ok(())
}

fn slot_offset(state: &[u8], slot: u32) -> u32 {
    bytes::read_u32(state, STATE_HEADER_SIZE + slot * SLOT_META_SIZE)
}

/// Whether slot offsets rise strictly with the slot index from
/// `metadata_end`, each 8-aligned and within `len`: the layout `init_state`
/// and `grow_state` write. One pass decides it; any other order is checked
/// by [`slot_limit`]'s search over every pair.
fn offsets_ascend(state: &[u8], slots: u32, metadata_end: u32, len: u32) -> bool {
    let mut floor = metadata_end;
    for slot in 0..slots {
        let offset = slot_offset(state, slot);
        if offset < floor || offset > len || !offset.is_multiple_of(8) {
            return false;
        }
        floor = match offset.checked_add(1) {
            Some(next) => next,
            None => return false,
        };
    }
    true
}

/// Checked single-key struct geometry, cached by the caller before probing.
/// An arena must begin at the exact primary end and remain in this slot.
pub fn bind_struct_map(
    state: &[u8],
    slot: u32,
) -> Result<crate::struct_map::StructMapSlot, ErrorCode> {
    bind_struct_map_in::<HashIndex>(state, slot)
}

/// [`bind_struct_map`] for an image of key-index family `I`. A slot of
/// another kind is `InvalidProgram`: the instruction naming it is wrong.
pub fn bind_struct_map_in<I: KeyIndex>(
    state: &[u8],
    slot: u32,
) -> Result<crate::struct_map::StructMapSlot<I>, ErrorCode> {
    let meta = bind_slot_in::<I>(state, slot)?;
    if meta.slot_type() != SlotType::StructMap {
        return Err(ErrorCode::InvalidProgram);
    }
    Ok(crate::struct_map::StructMapSlot::bind_in(state, slot))
}

/// Checked two-key struct geometry, with [`bind_struct_map_in`]'s refusals.
pub fn bind_struct_map2_in<I: KeyIndex>(
    state: &[u8],
    slot: u32,
) -> Result<crate::struct_map::StructMap2Slot<I>, ErrorCode> {
    let meta = bind_slot_in::<I>(state, slot)?;
    if meta.slot_type() != SlotType::StructMap2 {
        return Err(ErrorCode::InvalidProgram);
    }
    Ok(crate::struct_map::StructMap2Slot::bind_in(state, slot))
}

/// A struct-map slot's stored key lanes; a byte no declaration writes is
/// `InvalidState`.
fn struct_key_lanes(state: &[u8], meta: u32, kind: SlotType) -> Result<KeyLanes, ErrorCode> {
    KeyLanes::stored(
        kind == SlotType::StructMap2,
        state[(meta + SlotMetaOffset::STRUCT_KEY_LANES) as usize],
    )
    .ok_or(ErrorCode::InvalidState)
}

/// The end of an index region whose slot geometry was checked.
fn region_end<I: KeyIndex>(region: IndexRegion) -> u32 {
    u32::try_from(u64::from(region.at) + I::region_bytes(region.capacity, region.lanes))
        .unwrap_or_else(|_| columine_types::die!("invariant: a checked index region ends in u32"))
}

/// The capacity one slot grows to: a struct map's family decides its own.
fn grown_slot_capacity<I: KeyIndex>(kind: SlotType, capacity: u32) -> Result<u32, ErrorCode> {
    match kind {
        SlotType::StructMap | SlotType::StructMap2 => I::grown_capacity(capacity),
        _ => grown_capacity(capacity),
    }
}

fn checked_primary_size<I: KeyIndex>(
    state: &[u8],
    meta: u32,
    m: &OldSlotMeta,
    cap: u32,
) -> Result<u32, ErrorCode> {
    let capacity = u64::from(cap);
    let size = match m.slot_type {
        SlotType::StructMap | SlotType::StructMap2 => {
            let layout = checked_struct_layout(state, meta, m.slot_type)?;
            struct_map_primary_size::<I>(
                layout.descriptor_size,
                cap,
                layout.row_size,
                struct_key_lanes(state, meta, m.slot_type)?,
                m.slot_type == SlotType::StructMap && overlay_kind_byte(state, meta) != 0,
            )
        }
        SlotType::OrderedList => {
            let elem = overlay_kind_byte(state, meta);
            if elem == 0xff {
                let layout = checked_struct_layout(state, meta, m.slot_type)?;
                u64::from(layout.descriptor_size) + capacity * u64::from(layout.row_size)
            } else {
                let field = StructFieldType::from_u8(elem).ok_or(ErrorCode::InvalidState)?;
                if field == StructFieldType::Vo
                    || overlay_row_size(state, meta)
                        != columine_types::types::struct_field_size(field)
                {
                    return Err(ErrorCode::InvalidState);
                }
                capacity * u64::from(overlay_row_size(state, meta))
            }
        }
        SlotType::Nested => {
            let end = m
                .offset
                .checked_add(nested::NESTED_PREFIX_SIZE)
                .ok_or(ErrorCode::InvalidState)?;
            let prefix = state
                .get(m.offset as usize..end as usize)
                .ok_or(ErrorCode::InvalidState)?;
            let kind = SlotType::from_u8(prefix[0] & 0x0f).ok_or(ErrorCode::InvalidState)?;
            let inner_cap = u32::from(u16::from_le_bytes([prefix[1], prefix[2]]))
                .max(16)
                .checked_next_power_of_two()
                .ok_or(ErrorCode::InvalidState)?;
            let inner_size = match kind {
                SlotType::HashMap => 8 + u64::from(inner_cap) * 8,
                SlotType::HashSet => 8 + u64::from(inner_cap) * 4,
                SlotType::Aggregate => u64::from(aggregates::agg_slot_byte_size(prefix[3])),
                _ => 0,
            };
            u64::from(nested::NESTED_PREFIX_SIZE + ARENA_HEADER_SIZE)
                + capacity * (8 + ((inner_size + 7) & !7))
        }
        SlotType::HashMap => capacity * if m.has_hashmap_timestamps { 16 } else { 8 },
        SlotType::HashSet => capacity * 4,
        SlotType::Aggregate => u64::from(aggregates::agg_slot_byte_size(m.agg_type_byte)),
        SlotType::ConditionTree => u64::from(CONDITION_TREE_STATE_BYTES) + capacity * 16,
        SlotType::Scalar => 16,
        SlotType::Bitmap => {
            capacity * u64::from(columine_types::BITMAP_BYTES_PER_CAPACITY)
                + bitmosaic::IMAGE_ID_LEN as u64
        }
        SlotType::Array => capacity * 12,
    };
    checked_size(size)
}

fn validate_slot_geometry<I: KeyIndex>(
    state: &[u8],
    slot: u32,
    limit: u32,
    outer: OuterPointers,
) -> Result<(), ErrorCode> {
    let meta = STATE_HEADER_SIZE + slot * SLOT_META_SIZE;
    let flags = SlotTypeFlags::from_byte(state[(meta + SlotMetaOffset::TYPE_FLAGS) as usize]);
    let kind = flags.slot_type().ok_or(ErrorCode::InvalidState)?;
    let m = read_old_slot_meta(state, slot);
    // A CONDITION_TREE's capacity is its derived-facts cell count (0 for no
    // table) and its SIZE counts the occupied cells, so it is checked as a
    // table; only the fixed-size AGGREGATE and SCALAR slots are exempt.
    if !matches!(kind, SlotType::Aggregate | SlotType::Scalar)
        && ((m.capacity == 0 && !matches!(kind, SlotType::Bitmap | SlotType::ConditionTree))
            || (m.capacity != 0 && !m.capacity.is_power_of_two())
            || bytes::read_u32(state, meta + SlotMetaOffset::SIZE) > m.capacity)
    {
        return Err(ErrorCode::InvalidState);
    }
    if flags.has_ttl() && !supports_ttl(kind) {
        return Err(ErrorCode::InvalidState);
    }
    let primary = checked_primary_size::<I>(state, meta, &m, m.capacity)?;
    let mut end = m
        .offset
        .checked_add(primary)
        .ok_or(ErrorCode::InvalidState)?;
    if end > limit {
        return Err(ErrorCode::InvalidState);
    }
    if matches!(kind, SlotType::StructMap | SlotType::StructMap2) {
        let nf = state[(meta + STRUCT_NUM_FIELDS) as usize];
        let descriptor_end = m
            .offset
            .checked_add(u32::from(nf))
            .ok_or(ErrorCode::InvalidState)?;
        let descriptor = state
            .get(m.offset as usize..descriptor_end as usize)
            .ok_or(ErrorCode::InvalidState)?;
        let header = overlay_arena_hdr(state, meta);
        if has_arena_fields(nf, descriptor) {
            if header != end {
                return Err(ErrorCode::InvalidState);
            }
            let arena = ArenaRegion::bind(state, header)?;
            end = checked_size(
                u64::from(header) + u64::from(ARENA_HEADER_SIZE) + u64::from(arena.capacity),
            )?;
            if end > limit {
                return Err(ErrorCode::InvalidState);
            }
        } else if header != 0 {
            return Err(ErrorCode::InvalidState);
        }
        let region = IndexRegion {
            at: m.offset + align8(u32::from(nf)),
            capacity: m.capacity,
            lanes: struct_key_lanes(state, meta, kind)?,
        };
        I::validate(
            state,
            region,
            bytes::read_u32(state, meta + SlotMetaOffset::SIZE),
        )?;
    }
    if kind == SlotType::Nested {
        let header = m
            .offset
            .checked_add(nested::NESTED_PREFIX_SIZE)
            .and_then(|off| off.checked_add(m.capacity.checked_mul(8)?))
            .ok_or(ErrorCode::InvalidState)?;
        let arena = ArenaRegion::bind(state, header)?;
        if checked_size(
            u64::from(header) + u64::from(ARENA_HEADER_SIZE) + u64::from(arena.capacity),
        )? != end
        {
            return Err(ErrorCode::InvalidState);
        }
        // The prefix names the inner kind every kernel reads; one no
        // kernel builds is no valid geometry.
        let inner = state
            .get(m.offset as usize)
            .and_then(|byte| SlotType::from_u8(byte & 0x0f))
            .filter(|kind| {
                matches!(
                    kind,
                    SlotType::HashMap | SlotType::HashSet | SlotType::Aggregate
                )
            })
            .ok_or(ErrorCode::InvalidState)?;
        let prefix = nested::read_nested_prefix(state, m.offset);
        debug_assert_eq!(prefix.inner_type, inner);
        let keys = nested::outer_keys_offset(m.offset);
        let pointers = nested::outer_ptrs_offset(m.offset, m.capacity);
        let walked = if outer == OuterPointers::Walk {
            m.capacity
        } else {
            0
        };
        for pos in 0..walked {
            let key = bytes::read_u32(state, keys + pos * 4);
            if key == EMPTY_KEY || key == TOMBSTONE {
                continue;
            }
            let pointer = bytes::read_u32(state, pointers + pos * 4);
            nested::check_inner(
                state,
                header + ARENA_HEADER_SIZE,
                arena.used,
                pointer,
                &prefix,
            )?;
        }
    }
    if kind == SlotType::Bitmap {
        state
            .get(m.offset as usize..end as usize)
            .and_then(image_header)
            .filter(|header| header.width == KeyWidth::U32)
            .ok_or(ErrorCode::InvalidState)?;
    }
    if m.has_ttl {
        let eviction = bytes::read_u32(state, meta + SlotMetaOffset::EVICTION_INDEX_OFFSET);
        let capacity = bytes::read_u32(state, meta + SlotMetaOffset::EVICTION_INDEX_CAPACITY);
        if eviction != checked_align8(end)?
            || capacity != m.capacity
            || bytes::read_u32(state, meta + SlotMetaOffset::EVICTION_INDEX_SIZE) > capacity
        {
            return Err(ErrorCode::InvalidState);
        }
        end = checked_size(
            u64::from(eviction) + u64::from(capacity) * u64::from(EVICTION_ENTRY_SIZE),
        )?;
        if m.has_evict_trigger {
            let evicted = bytes::read_u32(state, meta + SlotMetaOffset::EVICTED_BUFFER_OFFSET);
            if evicted != checked_align8(end)?
                || bytes::read_u32(state, meta + SlotMetaOffset::EVICTED_COUNT) > EVICTED_BUFFER_CAP
            {
                return Err(ErrorCode::InvalidState);
            }
            end = checked_size(
                u64::from(evicted) + u64::from(EVICTED_BUFFER_CAP) * u64::from(EVICTION_ENTRY_SIZE),
            )?;
        }
    }
    if end > limit {
        return Err(ErrorCode::InvalidState);
    }
    Ok(())
}

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

fn read_old_slot_meta(old_state: &[u8], slot: u32) -> OldSlotMeta {
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

/// Struct-map metadata-driven primary size of a slot whose geometry was
/// checked: the family's index region for the slot's key lanes precedes the
/// rows.
fn struct_map_primary_size_from_meta<I: KeyIndex>(
    old_state: &[u8],
    meta_base: u32,
    cap: u32,
    slot_type: SlotType,
) -> u32 {
    let nf = overlay_num_fields(old_state, meta_base);
    let rs = overlay_row_size(old_state, meta_base);
    let lanes = KeyLanes::bound(
        slot_type == SlotType::StructMap2,
        old_state[(meta_base + SlotMetaOffset::STRUCT_KEY_LANES) as usize],
    );
    let has_ts = slot_type == SlotType::StructMap && overlay_kind_byte(old_state, meta_base) != 0;
    u32::try_from(struct_map_primary_size::<I>(
        align8(nf),
        cap,
        rs,
        lanes,
        has_ts,
    ))
    .unwrap_or_else(|_| columine_types::die!("invariant: a checked struct map's size fits u32"))
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

/// Calculate the checked extent after doubling one slot and its arena.
pub fn calculate_grown_state_size(old_state: &[u8], grown_slot_idx: u32) -> Result<u32, ErrorCode> {
    calculate_grown_state_size_in::<HashIndex>(old_state, grown_slot_idx)
}

/// [`calculate_grown_state_size`] for an image of key-index family `I`.
pub fn calculate_grown_state_size_in<I: KeyIndex>(
    old_state: &[u8],
    grown_slot_idx: u32,
) -> Result<u32, ErrorCode> {
    let (num_slots, metadata_end) = state_geometry::<I>(old_state)?;
    if grown_slot_idx >= num_slots {
        return Err(ErrorCode::InvalidSlot);
    }
    let mut total_size = metadata_end;
    for slot in 0..num_slots {
        let limit = slot_limit(old_state, slot, num_slots, metadata_end)?;
        validate_slot_geometry::<I>(old_state, slot, limit, OuterPointers::Walk)?;
        let meta = STATE_HEADER_SIZE + slot * SLOT_META_SIZE;
        let m = read_old_slot_meta(old_state, slot);
        let cap = if slot == grown_slot_idx {
            grown_slot_capacity::<I>(m.slot_type, m.capacity)?
        } else {
            m.capacity
        };
        let mut size = u64::from(checked_primary_size::<I>(old_state, meta, &m, cap)?);
        if matches!(m.slot_type, SlotType::StructMap | SlotType::StructMap2) {
            let header = overlay_arena_hdr(old_state, meta);
            if header != 0 {
                let capacity = bytes::read_u32(old_state, header);
                let capacity = if slot == grown_slot_idx {
                    capacity.checked_mul(2).ok_or(ErrorCode::InvalidState)?
                } else {
                    capacity
                };
                size += u64::from(ARENA_HEADER_SIZE) + u64::from(capacity);
            }
        }
        total_size = checked_align8(checked_size(u64::from(total_size) + size)?)?;
        if m.has_ttl {
            total_size = checked_align8(checked_size(
                u64::from(total_size) + u64::from(cap) * u64::from(EVICTION_ENTRY_SIZE),
            )?)?;
            if m.has_evict_trigger {
                total_size = checked_align8(checked_size(
                    u64::from(total_size)
                        + u64::from(EVICTED_BUFFER_CAP) * u64::from(EVICTION_ENTRY_SIZE),
                )?)?;
            }
        }
    }
    Ok(total_size)
}

/// Copy old state into `new_state` (which must be zeroed and large enough for
/// [`calculate_grown_state_size`]), rehashing the grown slot and recomputing
/// every slot's offsets.
pub fn grow_state(
    old_state: &[u8],
    new_state: &mut [u8],
    grown_slot_idx: u32,
) -> Result<(), ErrorCode> {
    grow_state_in::<HashIndex>(old_state, new_state, grown_slot_idx)
}

/// [`grow_state`] for an image of key-index family `I`: a grown struct map's
/// family moves its keys, and every row moves with its key's ordinal.
pub fn grow_state_in<I: KeyIndex>(
    old_state: &[u8],
    new_state: &mut [u8],
    grown_slot_idx: u32,
) -> Result<(), ErrorCode> {
    let required = calculate_grown_state_size_in::<I>(old_state, grown_slot_idx)?;
    if new_state.len() < required as usize {
        return Err(ErrorCode::InvalidState);
    }
    validate_struct_arenas::<I>(old_state, grown_slot_idx)?;
    let num_slots = state_num_slots(old_state);
    new_state[..required as usize].fill(0);

    // Copy header verbatim.
    bytes::copy(new_state, 0, old_state, 0, STATE_HEADER_SIZE);

    let mut data_cursor = align8(STATE_HEADER_SIZE + num_slots * SLOT_META_SIZE);

    for slot_i in 0..num_slots {
        let meta_base = STATE_HEADER_SIZE + slot_i * SLOT_META_SIZE;
        let m = read_old_slot_meta(old_state, slot_i);
        let old_offset = m.offset;
        let old_cap = m.capacity;

        let new_cap = if slot_i == grown_slot_idx {
            grown_slot_capacity::<I>(m.slot_type, old_cap)?
        } else {
            old_cap
        };
        let new_offset = data_cursor;

        // Primary data size (STRUCT_MAP, ORDERED_LIST, NESTED are layout-driven).
        let new_primary_size = match m.slot_type {
            SlotType::StructMap | SlotType::StructMap2 => {
                struct_map_primary_size_from_meta::<I>(old_state, meta_base, new_cap, m.slot_type)
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
                    // capacity formula before copying the image.
                    let old_storage_size = bitmap_ops::bitmap_payload_capacity(old_cap);
                    let new_storage_size = bitmap_ops::bitmap_payload_capacity(new_cap);
                    debug_assert_eq!(new_storage_size, new_primary_size);
                    bytes::zero(new_state, new_offset, new_storage_size);
                    // Copy exactly the u32 image the old header delimits. An
                    // old slot that holds no such image is a corrupt state,
                    // not an empty set: refuse, so the caller keeps the state
                    // it has and reports the failure, instead of publishing a
                    // grown slot whose region is no native encoding. Refusing
                    // here is safe because `old_state` is never written — only
                    // the destination buffer, which the caller discards.
                    let old_start = old_offset as usize;
                    let header = old_state
                        .get(old_start..old_start + old_storage_size as usize)
                        .and_then(image_header)
                        .filter(|header| header.width == KeyWidth::U32)
                        .ok_or(ErrorCode::InvalidState)?;
                    let copy_len =
                        u32::try_from(header.encoded_len).map_err(|_| ErrorCode::InvalidState)?;
                    if copy_len > new_storage_size {
                        return Err(ErrorCode::InvalidState);
                    }
                    bytes::copy(new_state, new_offset, old_state, old_offset, copy_len);
                }
                SlotType::StructMap | SlotType::StructMap2 => {
                    let nf = overlay_num_fields(old_state, meta_base);
                    let rs = overlay_row_size(old_state, meta_base);
                    let desc_size = align8(nf);
                    let lanes = KeyLanes::bound(
                        m.slot_type == SlotType::StructMap2,
                        old_state[(meta_base + SlotMetaOffset::STRUCT_KEY_LANES) as usize],
                    );
                    let from = IndexRegion {
                        at: old_offset + desc_size,
                        capacity: old_cap,
                        lanes,
                    };
                    let to = IndexRegion {
                        at: new_offset + desc_size,
                        capacity: new_cap,
                        lanes,
                    };
                    let old_rows_base = region_end::<I>(from);
                    let new_rows_base = region_end::<I>(to);
                    // A one-key map's timestamp lane follows its rows, one
                    // cell per ordinal, and moves with them.
                    let timestamps = m.slot_type == SlotType::StructMap
                        && overlay_kind_byte(old_state, meta_base) != 0;
                    let old_timestamps = old_rows_base + old_cap * rs;
                    let new_timestamps = new_rows_base + new_cap * rs;

                    bytes::copy(new_state, new_offset, old_state, old_offset, nf);
                    let rehashed =
                        I::grow(old_state, from, new_state, to, |new_state, old, new| {
                            bytes::copy(
                                new_state,
                                new_rows_base + new * rs,
                                old_state,
                                old_rows_base + old * rs,
                                rs,
                            );
                            if timestamps {
                                bytes::copy(
                                    new_state,
                                    new_timestamps + new * 8,
                                    old_state,
                                    old_timestamps + old * 8,
                                    8,
                                );
                            }
                        })?;
                    bytes::write_u32(new_state, meta_base + SlotMetaOffset::SIZE, rehashed);
                    // Arena compaction: copy live opaque payloads into the new arena.
                    let old_arena_hdr_off = overlay_arena_hdr(old_state, meta_base);
                    if old_arena_hdr_off != 0 {
                        let old_arena = ArenaRegion::bind(old_state, old_arena_hdr_off)?;
                        let new_arena_cap = old_arena
                            .capacity
                            .checked_mul(2)
                            .ok_or(ErrorCode::InvalidState)?;
                        let struct_data_size = struct_map_primary_size_from_meta::<I>(
                            old_state,
                            meta_base,
                            new_cap,
                            m.slot_type,
                        );
                        let new_arena_hdr_off = new_offset
                            .checked_add(struct_data_size)
                            .ok_or(ErrorCode::InvalidState)?;
                        bytes::write_u32(new_state, new_arena_hdr_off, new_arena_cap);
                        bytes::write_u32(
                            new_state,
                            meta_base + STRUCT_ARENA_HDR,
                            new_arena_hdr_off,
                        );
                        let new_arena_data_base = new_arena_hdr_off + ARENA_HEADER_SIZE;
                        let field_types =
                            &old_state[old_offset as usize..(old_offset + nf) as usize];
                        let mut new_arena_used = 0u32;

                        for ki in 0..new_cap {
                            if I::key_at(new_state, to, ki).is_none() {
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
                                if !is_arena_field_type(field_type) {
                                    continue;
                                }

                                let bit_set = new_state[(row_base + fi / 8) as usize]
                                    & (1u8 << (fi % 8))
                                    != 0;
                                if !bit_set {
                                    continue;
                                }

                                let f_off = struct_field_offset(nf as u8, field_types, fi as u8);
                                let old_offset_val = bytes::read_u32(new_state, row_base + f_off);
                                let arr_len = bytes::read_u32(new_state, row_base + f_off + 4);
                                let payload = old_arena.bytes(
                                    old_state,
                                    old_offset_val,
                                    arr_len,
                                    arena_elem_size(field_type),
                                )?;
                                let byte_len = u32::try_from(payload.len())
                                    .map_err(|_| ErrorCode::InvalidState)?;
                                let next_used = new_arena_used
                                    .checked_add(byte_len)
                                    .ok_or(ErrorCode::InvalidState)?;
                                if next_used > new_arena_cap {
                                    return Err(ErrorCode::InvalidState);
                                }
                                bytes::copy(
                                    new_state,
                                    new_arena_data_base + new_arena_used,
                                    payload,
                                    0,
                                    byte_len,
                                );
                                bytes::write_u32(new_state, row_base + f_off, new_arena_used);
                                new_arena_used = next_used;
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
                SlotType::Nested => {
                    copy_nested_slot(
                        old_state,
                        new_state,
                        &m,
                        new_offset,
                        new_cap,
                        new_primary_size,
                        meta_base + SlotMetaOffset::SIZE,
                    )?;
                }
                SlotType::Array => {
                    bytes::fill_u32(new_state, new_offset, new_cap, EMPTY_KEY);
                    bytes::fill_f64(
                        new_state,
                        new_offset + new_cap * 4,
                        new_cap,
                        f64::NEG_INFINITY,
                    );
                    bytes::copy(new_state, new_offset, old_state, old_offset, old_cap * 4);
                    bytes::copy(
                        new_state,
                        new_offset + new_cap * 4,
                        old_state,
                        old_offset + old_cap * 4,
                        old_cap * 8,
                    );
                }
                _ => {
                    // Non-hash slot: copy data. No vm.rs arm signals
                    // CAPACITY_EXCEEDED/NEEDS_GROWTH for AGGREGATE or SCALAR
                    // slots. A CONDITION_TREE grows only through the RETE
                    // layer that owns its derived-facts table: this copy
                    // carries its condition state, and RETE rehashes the
                    // table's lanes and recounts its SIZE afterwards.
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
                    let mut sz = struct_map_primary_size_from_meta::<I>(
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
            if m.slot_type == SlotType::Nested {
                copy_nested_slot(
                    old_state,
                    new_state,
                    &m,
                    new_offset,
                    new_cap,
                    new_primary_size,
                    meta_base + SlotMetaOffset::SIZE,
                )?;
            } else if primary_size > 0 {
                bytes::copy(new_state, new_offset, old_state, old_offset, primary_size);
            }
            // Arena header offset shifts with the slot data.
            if matches!(m.slot_type, SlotType::StructMap | SlotType::StructMap2) {
                let old_arena_hdr = overlay_arena_hdr(old_state, meta_base);
                if old_arena_hdr != 0 {
                    let struct_sz = struct_map_primary_size_from_meta::<I>(
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

/// One slot's declaration in a program's init section, as admission compares
/// it with an image: everything the init section alone decides is decided
/// when it is decoded.
#[derive(Clone, Copy, Debug)]
struct Declared {
    /// Byte offset of the slot's metadata record.
    meta: u32,
    /// The declared slot-type nibble.
    kind: u8,
    check: DeclaredShape,
}

/// What a declaration compares beyond the slot-type nibble.
#[derive(Clone, Copy, Debug)]
enum DeclaredShape {
    /// Kind only (arrays, lists, nested slots, keyed containers).
    Kind,
    /// An aggregate or scalar's subtype byte.
    Subtype(u8),
    /// A struct map's declared key lanes, its field descriptor, the `fields`
    /// span of the init section, and the row layout it implies.
    Struct {
        lanes: KeyLanes,
        fields: (usize, usize),
        layout: columine_types::types::StructRowLayout,
    },
}

/// A program's init declarations, decoded once per init section and compared
/// with every image a batch is handed. Capacity is deliberately not
/// compared: a grown image has the same shape.
#[derive(Debug, Default)]
pub(crate) struct Declarations {
    /// The init section `slots` was decoded from, and its slot count;
    /// `None` until a decode succeeds, so a failed one is never matched.
    init: Vec<u8>,
    num_slots: Option<u32>,
    slots: Vec<Declared>,
    /// Decode scratch: which slots the init section has declared.
    seen: Vec<bool>,
}

impl Declarations {
    /// Whether `state` has the slots `init` declares. A different init
    /// section is decoded first; `OutOfMemory` when its storage cannot be
    /// backed, so a refusal is never a failed allocation.
    pub(crate) fn matches<I: KeyIndex>(
        &mut self,
        init: &[u8],
        num_slots: u32,
        state: &[u8],
    ) -> Result<bool, ErrorCode> {
        if self.num_slots != Some(num_slots) || self.init != init {
            self.num_slots = None;
            self.init.clear();
            self.init
                .try_reserve(init.len())
                .map_err(|_| ErrorCode::OutOfMemory)?;
            if !self.decode(init, num_slots)? {
                return Ok(false);
            }
            self.init.extend_from_slice(init);
            self.num_slots = Some(num_slots);
        }
        Ok(self.compare::<I>(state, num_slots))
    }

    /// Decode `init` into `slots`: every slot declared exactly once, then
    /// `HALT` at the section's end, each declaration well formed.
    fn decode(&mut self, init: &[u8], num_slots: u32) -> Result<bool, ErrorCode> {
        self.slots.clear();
        self.seen.clear();
        let Ok(slot_count) = usize::try_from(num_slots) else {
            return Ok(false);
        };
        if slot_count > init.len() {
            return Ok(false);
        }
        if self.slots.try_reserve(slot_count).is_err() || self.seen.try_reserve(slot_count).is_err()
        {
            return Err(ErrorCode::OutOfMemory);
        }
        self.seen.resize(slot_count, false);
        let mut r = Operands::at(init, 0);
        while r.pos() < init.len() {
            let Some(op) = r.byte().and_then(Opcode::from_u8) else {
                return Ok(false);
            };
            if op == Opcode::Halt {
                return Ok(r.pos() == init.len() && self.seen.iter().all(|defined| *defined));
            }
            let Some(slot) = r.index().filter(|slot| *slot < num_slots) else {
                return Ok(false);
            };
            let Ok(slot_index) = usize::try_from(slot) else {
                return Ok(false);
            };
            if self.seen[slot_index] {
                return Ok(false);
            }
            self.seen[slot_index] = true;
            let Some(flags) = r.byte().map(SlotTypeFlags::from_byte) else {
                return Ok(false);
            };
            let Some(cap_lo) = r.byte() else {
                return Ok(false);
            };
            let Some(slot_type) = flags.slot_type().filter(|_| r.byte().is_some()) else {
                return Ok(false);
            };
            let kind = flags.to_byte() & 0x0f;
            let check = match op {
                Opcode::SlotStructMap | Opcode::SlotStructMap2 => {
                    let pair = op == Opcode::SlotStructMap2;
                    let expected = if pair {
                        SlotType::StructMap2
                    } else {
                        SlotType::StructMap
                    };
                    if slot_type != expected || flags.has_ttl() || kind != expected as u8 {
                        return Ok(false);
                    }
                    let Some(lanes) = KeyLanes::declared(pair, flags) else {
                        return Ok(false);
                    };
                    let Some(nf) = r.byte() else {
                        return Ok(false);
                    };
                    let start = r.pos();
                    let Some(fields) = r.bytes(usize::from(nf)) else {
                        return Ok(false);
                    };
                    if fields.iter().any(|&field| {
                        StructFieldType::from_u8(field).is_none()
                            || (pair && field == StructFieldType::Vo as u8)
                    }) {
                        return Ok(false);
                    }
                    DeclaredShape::Struct {
                        lanes,
                        fields: (start, r.pos()),
                        layout: compute_struct_row_layout_padded(nf, fields),
                    }
                }
                Opcode::SlotDef => {
                    if flags.has_ttl() {
                        let Some((_, next)) = TtlParams::read(init, r.pos()) else {
                            return Ok(false);
                        };
                        r = Operands::at(init, next);
                    }
                    if matches!(slot_type, SlotType::Aggregate | SlotType::Scalar) {
                        DeclaredShape::Subtype(cap_lo)
                    } else {
                        DeclaredShape::Kind
                    }
                }
                Opcode::SlotArray => DeclaredShape::Kind,
                Opcode::SlotOrderedList => {
                    if flags.struct_key_lanes() != 0 {
                        return Ok(false);
                    }
                    let Some(elem) = r.byte() else {
                        return Ok(false);
                    };
                    if elem == StructFieldType::Vo as u8 {
                        return Ok(false);
                    }
                    if elem == 0xff {
                        let Some(nf) = r.byte() else {
                            return Ok(false);
                        };
                        for _ in 0..nf {
                            let Some(field) = r.byte() else {
                                return Ok(false);
                            };
                            if field == StructFieldType::Vo as u8
                                || StructFieldType::from_u8(field).is_none()
                            {
                                return Ok(false);
                            }
                        }
                    }
                    DeclaredShape::Kind
                }
                Opcode::SlotNested => {
                    if r.bytes(4).is_none() {
                        return Ok(false);
                    }
                    DeclaredShape::Kind
                }
                _ => return Ok(false),
            };
            self.slots.push(Declared {
                meta: STATE_HEADER_SIZE + slot * SLOT_META_SIZE,
                kind,
                check,
            });
        }
        Ok(false)
    }

    /// Compare the decoded declarations with `state`'s metadata and struct
    /// descriptors.
    fn compare<I: KeyIndex>(&self, state: &[u8], num_slots: u32) -> bool {
        if state.len() < STATE_HEADER_SIZE as usize
            || state_num_slots(state) != num_slots
            || u64::from(STATE_HEADER_SIZE) + u64::from(num_slots) * u64::from(SLOT_META_SIZE)
                > state.len() as u64
        {
            return false;
        }
        self.slots.iter().all(|declared| {
            let meta = declared.meta;
            if state[(meta + SlotMetaOffset::TYPE_FLAGS) as usize] & 0x0f != declared.kind {
                return false;
            }
            match declared.check {
                DeclaredShape::Kind => true,
                DeclaredShape::Subtype(subtype) => {
                    state[(meta + SlotMetaOffset::AGG_TYPE) as usize] == subtype
                }
                DeclaredShape::Struct {
                    lanes,
                    fields: (start, end),
                    layout,
                } => {
                    let fields = &self.init[start..end];
                    let off = bytes::read_u32(state, meta + SlotMetaOffset::OFFSET);
                    let descriptor = u32::try_from(fields.len())
                        .ok()
                        .and_then(|len| off.checked_add(len))
                        .and_then(|end| state.get(off as usize..end as usize));
                    let capacity = bytes::read_u32(state, meta + SlotMetaOffset::CAPACITY);
                    usize::from(state[(meta + STRUCT_NUM_FIELDS) as usize]) == fields.len()
                        && descriptor == Some(fields)
                        && overlay_row_size(state, meta) == layout.row_size
                        && u32::from(state[(meta + STRUCT_BITSET_BYTES) as usize])
                            == layout.bitset_bytes
                        && state[(meta + SlotMetaOffset::STRUCT_KEY_LANES) as usize] == lanes.byte()
                        && u64::from(off)
                            + struct_map_primary_size::<I>(
                                layout.descriptor_size,
                                capacity,
                                layout.row_size,
                                lanes,
                                false,
                            )
                            <= state.len() as u64
                }
            }
        })
    }
}

/// Geometry was checked by sizing; validate live references and compaction span
/// before any destination write. The descriptor and row offsets are reused.
fn validate_struct_arenas<I: KeyIndex>(state: &[u8], grown_slot: u32) -> Result<(), ErrorCode> {
    for slot in 0..state_num_slots(state) {
        let meta = STATE_HEADER_SIZE + slot * SLOT_META_SIZE;
        let kind = SlotType::from_u8(state[(meta + SlotMetaOffset::TYPE_FLAGS) as usize] & 0x0f)
            .ok_or(ErrorCode::InvalidState)?;
        if !matches!(kind, SlotType::StructMap | SlotType::StructMap2) {
            continue;
        }
        let header = overlay_arena_hdr(state, meta);
        if header == 0 {
            continue;
        }
        let nf = state[(meta + STRUCT_NUM_FIELDS) as usize];
        let cap = bytes::read_u32(state, meta + SlotMetaOffset::CAPACITY);
        let rs = overlay_row_size(state, meta);
        let off = bytes::read_u32(state, meta + SlotMetaOffset::OFFSET);
        let descriptor = &state[off as usize..off as usize + usize::from(nf)];
        let region = IndexRegion {
            at: off + align8(u32::from(nf)),
            capacity: cap,
            lanes: struct_key_lanes(state, meta, kind)?,
        };
        let rows = region_end::<I>(region);
        let arena = ArenaRegion::bind(state, header)?;
        let mut compact_bytes = 0u64;
        for pos in 0..cap {
            if I::key_at(state, region, pos).is_none() {
                continue;
            }
            let row = rows + pos * rs;
            let mut field_offset = u32::from(nf).div_ceil(8);
            for (field_idx, &field_type) in descriptor.iter().enumerate() {
                let field_type =
                    StructFieldType::from_u8(field_type).ok_or(ErrorCode::InvalidState)?;
                if is_arena_field_type(field_type)
                    && state[row as usize + field_idx / 8] & (1 << (field_idx % 8)) != 0
                {
                    let field = row + field_offset;
                    let payload = arena.bytes(
                        state,
                        bytes::read_u32(state, field),
                        bytes::read_u32(state, field + 4),
                        arena_elem_size(field_type),
                    )?;
                    compact_bytes += payload.len() as u64;
                }
                field_offset += columine_types::types::struct_field_size(field_type);
            }
        }
        if slot == grown_slot && compact_bytes > u64::from(arena.capacity) * 2 {
            return Err(ErrorCode::InvalidState);
        }
    }
    Ok(())
}

/// Relocate absolute inner pointers while copying or rehashing an outer table.
fn copy_nested_slot(
    old: &[u8],
    new: &mut [u8],
    m: &OldSlotMeta,
    new_offset: u32,
    new_cap: u32,
    new_size: u32,
    size_off: u32,
) -> Result<(), ErrorCode> {
    let old_offset = m.offset;
    let old_cap = m.capacity;
    let old_header = nested::arena_header_offset(old_offset, old_cap);
    let old_arena = ArenaRegion::bind(old, old_header)?;
    let new_header = nested::arena_header_offset(new_offset, new_cap);
    let new_data = new_header + ARENA_HEADER_SIZE;
    let new_arena_cap = new_size - (new_data - new_offset);
    if old_arena.used > new_arena_cap {
        return Err(ErrorCode::InvalidState);
    }
    bytes::copy(new, new_offset, old, old_offset, nested::NESTED_PREFIX_SIZE);
    bytes::write_u32(new, new_header, new_arena_cap);
    bytes::write_u32(new, new_header + 4, old_arena.used);
    bytes::copy(
        new,
        new_data,
        old,
        old_header + ARENA_HEADER_SIZE,
        old_arena.used,
    );
    let old_keys = nested::outer_keys_offset(old_offset);
    let old_ptrs = nested::outer_ptrs_offset(old_offset, old_cap);
    let new_keys = nested::outer_keys_offset(new_offset);
    let new_ptrs = nested::outer_ptrs_offset(new_offset, new_cap);
    let table =
        hash_table::FlatTable::bind_external(new_keys, new_cap, size_off, hash_table::ENTRY_U32);
    if new_cap == old_cap {
        bytes::copy(new, new_keys, old, old_keys, old_cap * 8);
    } else {
        bytes::fill_u32(new, new_keys, new_cap, EMPTY_KEY);
        bytes::write_u32(new, size_off, 0);
    }
    for pos in 0..old_cap {
        let key = bytes::read_u32(old, old_keys + pos * 4);
        if key == EMPTY_KEY || key == TOMBSTONE {
            continue;
        }
        let old_ptr = bytes::read_u32(old, old_ptrs + pos * 4);
        let relative = old_ptr
            .checked_sub(old_header + ARENA_HEADER_SIZE)
            .ok_or(ErrorCode::InvalidState)?;
        if relative >= old_arena.used {
            return Err(ErrorCode::InvalidState);
        }
        let pointer = new_data + relative;
        if new_cap == old_cap {
            bytes::write_u32(new, new_ptrs + pos * 4, pointer);
        } else {
            table
                .upsert_u32(new, key, pointer)
                .ok_or(ErrorCode::InvalidState)?;
        }
    }
    Ok(())
}

/// The actual reason a program cannot be admitted, retained before state writes
/// or capability preparation. Malformed declarations/grammar and size overflow
/// are distinct from the statically forbidden bitmap expiry layout.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgramInvalidCause {
    MalformedProgram,
    BitmapTtlSourceToUntrackedTarget { source: u32, target: u32 },
}

impl From<InvalidSource> for ProgramInvalidCause {
    fn from(_: InvalidSource) -> Self {
        Self::MalformedProgram
    }
}

/// Why a program could not be sized: it is invalid, or the memory to check
/// its declarations could not be had. Only the first is a property of the
/// program; the same program may size with more room.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SizingRefusal {
    Invalid(ProgramInvalidCause),
    OutOfMemory,
}

impl SizingRefusal {
    /// The status an `ErrorCode` boundary reports for this refusal.
    pub fn code(self) -> ErrorCode {
        match self {
            Self::Invalid(_) => ErrorCode::InvalidProgram,
            Self::OutOfMemory => ErrorCode::OutOfMemory,
        }
    }
}

/// Byte length of the sizing record a status-plus-record export writes: four
/// little-endian u32 words.
pub const SIZING_RECORD_LEN: u32 = 16;
/// Word 1 of a sizing record whose status is `InvalidProgram`: malformed program.
pub const SIZING_CAUSE_MALFORMED: u32 = 1;
/// Word 1 of a sizing record whose status is `InvalidProgram`: a TTL bitmap
/// source folding into an untracked target, with words 2 and 3 naming them.
pub const SIZING_CAUSE_BITMAP_TTL_SOURCE: u32 = 2;

/// Status and record words for one sizing result. `Ok` names `[size, 0, 0, 0]`;
/// an `InvalidProgram` refusal names `[0, kind, source, target]`, where source
/// and target are meaningful only for the TTL-layout cause; `OutOfMemory`
/// names no words. The status discriminates the record; no word is read as
/// absence.
pub fn sizing_record(result: Result<u32, SizingRefusal>) -> (ErrorCode, [u32; 4]) {
    match result {
        Ok(size) => (ErrorCode::Ok, [size, 0, 0, 0]),
        Err(SizingRefusal::OutOfMemory) => (ErrorCode::OutOfMemory, [0; 4]),
        Err(SizingRefusal::Invalid(ProgramInvalidCause::MalformedProgram)) => {
            (ErrorCode::InvalidProgram, [0, SIZING_CAUSE_MALFORMED, 0, 0])
        }
        Err(SizingRefusal::Invalid(ProgramInvalidCause::BitmapTtlSourceToUntrackedTarget {
            source,
            target,
        })) => (
            ErrorCode::InvalidProgram,
            [0, SIZING_CAUSE_BITMAP_TTL_SOURCE, source, target],
        ),
    }
}
