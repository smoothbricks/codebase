//! Canonical struct-map bytes ("seal").
//!
//! A struct map's bytes depend on its history: which ordinal each key took,
//! which rows died, the order arena payloads were appended in. Sealing
//! rebuilds every struct-map slot so its bytes are a function of its keys,
//! the present fields of their rows, and its capacity alone: the family's
//! [`KeyIndex::seal`] renumbers the keys canonically and names each key's new
//! ordinal, each live row moves to that ordinal keeping only its bitset and
//! present field cells, every dead row is zero, and the CSR arena is
//! repacked in new ordinal order with its unused bytes zero. Equal key sets
//! and rows reached through any insertion order seal to identical images.
//!
//! Sealing has no journal: like growth it runs on a committed image, never
//! inside a speculative window. It allocates only through the caller's
//! [`KeyScratch`], growing it to the largest slot it seals; memory that cannot
//! back it is `OutOfMemory`, before that slot's first write.

use crate::bytes;
use crate::key_index::{IndexRegion, InternBytes, KeyCx, KeyIndex, KeyScratch};
use crate::meta::{SlotMetaView, slot_meta_base};
use crate::state_init;
use crate::struct_map::{ArenaRegion, StructMap2Slot, StructMapSlot, StructRows};
use columine_types::types::{
    ErrorCode, SlotType, StructFieldType, arena_elem_size, is_arena_field_type, state_num_slots,
    struct_field_size,
};

/// Seal every struct-map slot of an image of key-index family `I`, resolving
/// interned key lanes through `strings`. The image is validated first; a
/// refusal leaves the slots sealed so far sealed and the rest untouched.
pub fn seal_state<I: KeyIndex, S: InternBytes + ?Sized>(
    state: &mut [u8],
    strings: &S,
    scratch: &mut KeyScratch<I>,
) -> Result<(), ErrorCode> {
    state_init::validate_state_in::<I>(state)?;
    for slot in 0..state_num_slots(state) {
        match SlotMetaView::read(state, slot).slot_type() {
            SlotType::StructMap => {
                let map = StructMapSlot::<I>::bind_in(state, slot);
                let timestamps = state_init::overlay_kind_byte(state, slot_meta_base(slot)) != 0;
                seal_slot(
                    state,
                    map.index(),
                    map.rows(),
                    map.size(state),
                    timestamps,
                    strings,
                    scratch,
                )?;
            }
            SlotType::StructMap2 => {
                let map = StructMap2Slot::<I>::bind_in(state, slot);
                seal_slot(
                    state,
                    map.index(),
                    map.rows(),
                    map.size(state),
                    false,
                    strings,
                    scratch,
                )?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Back `buffer` with `len` bytes or elements before any write.
fn reserve<T>(buffer: &mut Vec<T>, len: usize) -> Result<(), ErrorCode> {
    buffer.clear();
    buffer.try_reserve(len).map_err(|_| ErrorCode::OutOfMemory)
}

/// Seal one slot: its index, then its rows (and a one-key map's timestamp
/// lane) in the index's new ordinal order, then its arena.
fn seal_slot<I: KeyIndex, S: InternBytes + ?Sized>(
    state: &mut [u8],
    region: IndexRegion,
    rows: StructRows,
    size: u32,
    timestamps: bool,
    strings: &S,
    scratch: &mut KeyScratch<I>,
) -> Result<(), ErrorCode> {
    let KeyScratch {
        index,
        rows: saved,
        arena: saved_arena,
        ordinals,
    } = scratch;
    let capacity = region.capacity as usize;
    let row_size = rows.row_size as usize;
    let rows_len = capacity * row_size;
    let lane_len = if timestamps { capacity * 8 } else { 0 };
    let rows_at = rows.rows_base as usize;
    let span = rows_at..rows_at + rows_len + lane_len;
    reserve(saved, span.len())?;
    reserve(ordinals, capacity)?;
    if rows.arena_header_off != 0 {
        let arena = ArenaRegion::bind(state, rows.arena_header_off)?;
        reserve(saved_arena, arena.used as usize)?;
    }
    saved.extend_from_slice(state.get(span.clone()).ok_or(ErrorCode::InvalidState)?);
    ordinals.resize(capacity, u32::MAX);

    I::seal(
        state,
        region,
        size,
        &mut KeyCx::new(strings, index),
        |old, new| ordinals[old as usize] = new,
    )?;

    state[span].fill(0);
    let descriptor = rows.field_types_off as usize;
    for (old, &new) in ordinals.iter().enumerate() {
        if new == u32::MAX {
            continue;
        }
        let from = &saved[old * row_size..(old + 1) * row_size];
        let to = rows.row_off(new) as usize;
        let bitset = rows.bitset_bytes as usize;
        state[to..to + bitset].copy_from_slice(&from[..bitset]);
        let mut offset = bitset;
        for field in 0..usize::from(rows.num_fields) {
            let kind = StructFieldType::from_u8(state[descriptor + field])
                .ok_or(ErrorCode::InvalidState)?;
            let width = struct_field_size(kind) as usize;
            if from[field / 8] & (1 << (field % 8)) != 0 {
                state[to + offset..to + offset + width]
                    .copy_from_slice(&from[offset..offset + width]);
            }
            offset += width;
        }
        if timestamps {
            let lane = rows_at + rows_len;
            let cell = &saved[rows_len + old * 8..rows_len + old * 8 + 8];
            state[lane + new as usize * 8..lane + new as usize * 8 + 8].copy_from_slice(cell);
        }
    }
    if rows.arena_header_off != 0 {
        repack_arena::<I>(state, region, rows, saved_arena)?;
    }
    Ok(())
}

/// Repack a sealed slot's arena: each live row's present arena payloads in
/// ascending ordinal and field order, from offset zero, every unused byte zero.
fn repack_arena<I: KeyIndex>(
    state: &mut [u8],
    region: IndexRegion,
    rows: StructRows,
    saved: &mut Vec<u8>,
) -> Result<(), ErrorCode> {
    let arena = ArenaRegion::bind(state, rows.arena_header_off)?;
    let data = arena.data as usize;
    saved.extend_from_slice(&state[data..data + arena.used as usize]);
    state[data..data + arena.capacity as usize].fill(0);
    let descriptor = rows.field_types_off as usize;
    let mut used = 0u32;
    for ordinal in 0..region.capacity {
        if I::key_at(state, region, ordinal).is_none() {
            continue;
        }
        let row = rows.row_off(ordinal);
        let mut offset = rows.bitset_bytes;
        for field in 0..u32::from(rows.num_fields) {
            let kind = StructFieldType::from_u8(state[descriptor + field as usize])
                .ok_or(ErrorCode::InvalidState)?;
            let present = state[(row + field / 8) as usize] & (1 << (field % 8)) != 0;
            if is_arena_field_type(kind) && present {
                let cell = row + offset;
                let start = bytes::read_u32(state, cell);
                let byte_len = bytes::read_u32(state, cell + 4)
                    .checked_mul(arena_elem_size(kind))
                    .ok_or(ErrorCode::InvalidState)?;
                let end = start.checked_add(byte_len).ok_or(ErrorCode::InvalidState)?;
                let payload = saved
                    .get(start as usize..end as usize)
                    .ok_or(ErrorCode::InvalidState)?;
                let next = used.checked_add(byte_len).ok_or(ErrorCode::InvalidState)?;
                if next > arena.capacity {
                    return Err(ErrorCode::InvalidState);
                }
                state[data + used as usize..data + next as usize].copy_from_slice(payload);
                bytes::write_u32(state, cell, used);
                used = next;
            }
            offset += struct_field_size(kind);
        }
    }
    bytes::write_u32(state, arena.header + 4, used);
    Ok(())
}
