//! Acceptance oracle for the state_init.zig port: every relevant Zig
//! `test` block from state_init.zig (and the init/size-only tests from
//! vm_test.zig) has a counterpart here, plus proptest invariants Zig's
//! example-based tests only sample. Comments name the Zig test each block
//! mirrors.

use columine_types::types::{
    DERIVED_FACT_EMPTY_IDENTITY, DERIVED_FACT_TOMBSTONE_IDENTITY, EMPTY_KEY, ErrorCode,
    PROGRAM_HASH_PREFIX, SLOT_META_SIZE, STATE_HEADER_SIZE, STATE_MAGIC, StateHeaderOffset,
    StructFieldType, align8, hash_key,
};
use columine_vm::state_init::{
    EVICTION_ENTRY_SIZE, arena_initial_capacity_64, calculate_grown_state_size,
    calculate_state_size, compute_struct_row_layout_padded, grow_state, init_state, reset_state,
    struct_map_slot_data_size, ttl_side_buffer_size,
};
use columine_vm::{aggregates, bytes};
use proptest::prelude::*;

const HASH_PREFIX: usize = PROGRAM_HASH_PREFIX as usize;

/// state_init.zig:1312-1340 `buildSingleSlotProgram` — one SLOT_DEF + HALT.
fn build_single_slot_program(type_flags_byte: u8, cap_lo: u8, cap_hi: u8) -> [u8; 64] {
    let mut prog = [0u8; 64];
    let content = &mut prog[HASH_PREFIX..];
    // Magic "AXE1" little-endian.
    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    content[4] = 1; // version lo
    content[5] = 0; // version hi
    content[6] = 1; // num_slots
    let init_len: u16 = 6;
    content[10] = init_len as u8;
    content[11] = (init_len >> 8) as u8;
    // Init section at content[14].
    content[14] = 0x10; // SLOT_DEF
    content[15] = 0; // slot index
    content[16] = type_flags_byte;
    content[17] = cap_lo;
    content[18] = cap_hi;
    content[19] = 0x00; // HALT
    prog
}

/// A single-slot program with TTL params (flags must have 0x10 set).
fn build_single_slot_ttl_program(
    type_flags_byte: u8,
    cap_lo: u8,
    cap_hi: u8,
    ttl: f32,
    grace: f32,
    has_evict_trigger: bool,
) -> [u8; 64] {
    let mut prog = [0u8; 64];
    let flags = type_flags_byte | 0x10 | if has_evict_trigger { 0x20 } else { 0 };
    let content = &mut prog[HASH_PREFIX..];
    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    content[4] = 1;
    content[6] = 1;
    let init_len: u16 = 16; // SLOT_DEF(1) + base(4) + ttl params(10) + HALT(1)
    content[10] = init_len as u8;
    content[11] = (init_len >> 8) as u8;
    content[14] = 0x10; // SLOT_DEF
    content[15] = 0;
    content[16] = flags;
    content[17] = cap_lo;
    content[18] = cap_hi;
    content[19..23].copy_from_slice(&ttl.to_le_bytes());
    content[23..27].copy_from_slice(&grace.to_le_bytes());
    content[27] = 1; // timestamp_field_idx
    content[28] = 0; // start_of NONE
    content[29] = 0x00; // HALT
    prog
}

/// A single-slot SLOT_STRUCT_MAP program.
fn build_struct_map_program(cap_lo: u8, cap_hi: u8, field_types: &[u8]) -> Vec<u8> {
    let mut prog = vec![0u8; 96];
    let nf = field_types.len() as u8;
    let init_len = (1 + 5 + field_types.len() + 1) as u16;
    let content = &mut prog[HASH_PREFIX..];
    content[0] = 0x41;
    content[1] = 0x58;
    content[2] = 0x45;
    content[3] = 0x31;
    content[4] = 1;
    content[6] = 1;
    content[10] = init_len as u8;
    content[11] = (init_len >> 8) as u8;
    content[14] = 0x18; // SLOT_STRUCT_MAP
    content[15] = 0; // slot
    content[16] = 0x06; // type_flags: STRUCT_MAP
    content[17] = cap_lo;
    content[18] = cap_hi;
    content[19] = nf;
    content[20..20 + field_types.len()].copy_from_slice(field_types);
    content[20 + field_types.len()] = 0x00; // HALT
    prog
}

/// A hash-map slot before a condition-tree/derived-facts slot, so growing the
/// first slot proves the 16-byte derived table relocates without lane drift.
fn build_hashmap_and_condition_tree_program(derived_capacity: u16) -> [u8; 64] {
    let mut prog = [0u8; 64];
    let content = &mut prog[HASH_PREFIX..];
    content[0..4].copy_from_slice(&0x3145_5841u32.to_le_bytes());
    content[4] = 1;
    content[6] = 2;
    content[10..12].copy_from_slice(&11u16.to_le_bytes());
    content[14..19].copy_from_slice(&[0x10, 0, 0x00, 8, 0]);
    content[19..24].copy_from_slice(&[
        0x10,
        1,
        0x04,
        derived_capacity as u8,
        (derived_capacity >> 8) as u8,
    ]);
    content[24] = 0;
    prog
}

/// Probe-read a u32 hashmap value the way vm_map_get does.
fn map_get(state: &[u8], offset: u32, cap: u32, key: u32) -> Option<u32> {
    let mut pos = hash_key(key, cap);
    loop {
        let k = bytes::read_u32(state, offset + pos * 4);
        if k == EMPTY_KEY {
            return None;
        }
        if k == key {
            return Some(bytes::read_u32(state, offset + cap * 4 + pos * 4));
        }
        pos = (pos + 1) & (cap - 1);
    }
}

// ---------------------------------------------------------------------------
// state_init.zig tests 1-3: computeStructRowLayout (LOCAL padded version)
// ---------------------------------------------------------------------------

#[test]
fn compute_struct_row_layout_2_fields_uint32_string() {
    let fields = [StructFieldType::UInt32 as u8, StructFieldType::String as u8];
    let layout = compute_struct_row_layout_padded(2, &fields);
    // bitset = 1, row_data = 1+4+4 = 9, padded to 4 = 12
    assert_eq!(layout.bitset_bytes, 1);
    assert_eq!(layout.row_size, 12);
    assert_eq!(layout.descriptor_size, 8);
}

#[test]
fn compute_struct_row_layout_3_fields_float64_bool_int64() {
    let fields = [
        StructFieldType::Float64 as u8,
        StructFieldType::Bool as u8,
        StructFieldType::Int64 as u8,
    ];
    let layout = compute_struct_row_layout_padded(3, &fields);
    // bitset = 1, row_data = 1+8+1+8 = 18, padded to 4 = 20
    assert_eq!(layout.bitset_bytes, 1);
    assert_eq!(layout.row_size, 20);
    assert_eq!(layout.descriptor_size, 8);
}

#[test]
fn compute_struct_row_layout_8_fields_all_uint32() {
    let fields = [StructFieldType::UInt32 as u8; 8];
    let layout = compute_struct_row_layout_padded(8, &fields);
    // bitset = 1, row_data = 1+8*4 = 33, padded to 4 = 36
    assert_eq!(layout.bitset_bytes, 1);
    assert_eq!(layout.row_size, 36);
    assert_eq!(layout.descriptor_size, 8);
}

/// The one authoritative layout helper set (the deleted Zig carried a
/// drifted types.zig twin — unpadded rows, cap*4 arenas — deleted
/// post-parity along with its Rust port).
#[test]
fn authoritative_layout_helpers_are_padded_and_cap64() {
    let fields = [
        StructFieldType::UInt32 as u8,
        StructFieldType::Float64 as u8,
    ];
    let padded = compute_struct_row_layout_padded(2, &fields);
    assert_eq!(padded.row_size, 16); // 1+4+8 = 13 → padded to 16
    assert_eq!(arena_initial_capacity_64(16), 1024);
}

// ---------------------------------------------------------------------------
// state_init.zig test 4: structFieldOffset
// ---------------------------------------------------------------------------

#[test]
fn struct_field_offset_3_fields() {
    let fields = [
        StructFieldType::UInt32 as u8,
        StructFieldType::Float64 as u8,
        StructFieldType::String as u8,
    ];
    let bitset_bytes = 1;
    assert_eq!(
        columine_vm::state_init::struct_field_offset(3, &fields, 0),
        bitset_bytes
    );
    assert_eq!(
        columine_vm::state_init::struct_field_offset(3, &fields, 1),
        bitset_bytes + 4
    );
    assert_eq!(
        columine_vm::state_init::struct_field_offset(3, &fields, 2),
        bitset_bytes + 4 + 8
    );
}

// ---------------------------------------------------------------------------
// state_init.zig tests 4-6: arenaInitialCapacity, getStructMapSlotDataSize,
// getTTLSideBufferSize
// ---------------------------------------------------------------------------

#[test]
fn arena_initial_capacity_is_64_bytes_per_entry() {
    assert_eq!(arena_initial_capacity_64(64), 64 * 64);
    assert_eq!(arena_initial_capacity_64(16), 16 * 64);
    assert_eq!(arena_initial_capacity_64(256), 256 * 64);
}

#[test]
fn struct_map_slot_data_size_without_timestamps() {
    assert_eq!(struct_map_slot_data_size(8, 32, 12, false), 520);
}

#[test]
fn struct_map_slot_data_size_with_timestamps() {
    assert_eq!(struct_map_slot_data_size(8, 32, 12, true), 776);
}

#[test]
fn ttl_side_buffer_size_no_ttl_is_zero() {
    assert_eq!(ttl_side_buffer_size(false, false, 32), 0);
}

#[test]
fn ttl_side_buffer_size_ttl_only() {
    assert_eq!(
        ttl_side_buffer_size(true, false, 32),
        align8(32 * EVICTION_ENTRY_SIZE)
    );
}

#[test]
fn ttl_side_buffer_size_ttl_plus_evict_trigger() {
    let cap = 32;
    let expected = align8(cap * EVICTION_ENTRY_SIZE) + align8(1024 * EVICTION_ENTRY_SIZE);
    assert_eq!(ttl_side_buffer_size(true, true, cap), expected);
}

// ---------------------------------------------------------------------------
// state_init.zig tests 7-8: vm_calculate_state_size
// ---------------------------------------------------------------------------

#[test]
fn calculate_state_size_hashmap() {
    // HASHMAP flags 0x00, requested cap 8 → effective nextPowerOf2(8*2) = 16.
    let prog = build_single_slot_program(0x00, 8, 0);
    let size = calculate_state_size(&prog);
    assert!(size > 0);

    let cap = 16u32;
    let mut expected = align8(STATE_HEADER_SIZE + SLOT_META_SIZE);
    expected += cap * 4 + cap * 4 + cap * 8; // keys + values + timestamps
    expected = align8(expected);
    assert_eq!(size, expected);
}

#[test]
fn calculate_state_size_aggregate_count() {
    // AGGREGATE flags 0x02, cap_lo = AggType COUNT (2).
    let prog = build_single_slot_program(0x02, 2, 0);
    let size = calculate_state_size(&prog);
    assert!(size > 0);

    let mut expected = align8(STATE_HEADER_SIZE + SLOT_META_SIZE);
    expected += 8; // COUNT aggregate is 8 bytes, not 16
    expected = align8(expected);
    assert_eq!(size, expected);
}

#[test]
fn condition_tree_derived_facts_use_sixteen_bytes_per_cell() {
    let capacity = 4u32;
    let prog = build_single_slot_program(0x04, capacity as u8, 0);
    let expected = align8(STATE_HEADER_SIZE + SLOT_META_SIZE + 8) + capacity * 16;
    assert_eq!(calculate_state_size(&prog), expected);

    let mut state = vec![0u8; expected as usize];
    init_state(&mut state, &prog).expect("init must succeed");

    let derived_offset = bytes::read_u32(&state, StateHeaderOffset::DERIVED_FACTS_OFFSET);
    assert_eq!(derived_offset % 8, 0);
    assert_eq!(
        u32::from(bytes::read_u16(
            &state,
            StateHeaderOffset::DERIVED_FACTS_CAPACITY,
        )),
        capacity,
    );
    for pos in 0..capacity {
        assert_eq!(
            bytes::read_u64(&state, derived_offset + pos * 8),
            DERIVED_FACT_EMPTY_IDENTITY,
        );
        assert_eq!(
            bytes::read_u32(&state, derived_offset + capacity * 8 + pos * 4),
            0,
        );
        assert_eq!(
            bytes::read_u32(&state, derived_offset + capacity * 12 + pos * 4),
            0,
        );
    }
    assert_ne!(DERIVED_FACT_EMPTY_IDENTITY, DERIVED_FACT_TOMBSTONE_IDENTITY,);
}

// ---------------------------------------------------------------------------
// state_init.zig tests 9-11: vm_init_state
// ---------------------------------------------------------------------------

#[test]
fn init_state_hashmap_keys_empty_size_zero_timestamps_neg_inf() {
    let prog = build_single_slot_program(0x00, 8, 0);
    let size = calculate_state_size(&prog);
    assert!(size > 0);

    let mut state = vec![0u8; 16384];
    init_state(&mut state, &prog).expect("init must succeed");

    assert_eq!(bytes::read_u32(&state, 0), STATE_MAGIC);

    let meta = STATE_HEADER_SIZE;
    let offset = bytes::read_u32(&state, meta);
    let cap = bytes::read_u32(&state, meta + 4);
    let slot_size = bytes::read_u32(&state, meta + 8);
    assert_eq!(slot_size, 0);
    assert_eq!(cap, 16);

    for i in 0..cap {
        assert_eq!(bytes::read_u32(&state, offset + i * 4), EMPTY_KEY);
    }
    let ts_offset = offset + cap * 4 + cap * 4;
    for i in 0..cap {
        assert_eq!(
            bytes::read_f64(&state, ts_offset + i * 8),
            f64::NEG_INFINITY
        );
    }
}

#[test]
fn init_state_aggregate_sum_value_zero_count_zero() {
    let prog = build_single_slot_program(0x02, 1, 0); // SUM
    let size = calculate_state_size(&prog);
    assert!(size > 0);

    let mut state = vec![0u8; 16384];
    init_state(&mut state, &prog).expect("init must succeed");

    let offset = bytes::read_u32(&state, STATE_HEADER_SIZE);
    assert!((aggregates::agg_value_f64(&state, offset) - 0.0).abs() < 0.001);
    assert_eq!(aggregates::agg_count(&state, offset, 1), 0);
}

#[test]
fn init_state_aggregate_min_value_pos_inf() {
    let prog = build_single_slot_program(0x02, 3, 0); // MIN
    let mut state = vec![0u8; 16384];
    init_state(&mut state, &prog).expect("init must succeed");

    let offset = bytes::read_u32(&state, STATE_HEADER_SIZE);
    assert_eq!(aggregates::agg_value_f64(&state, offset), f64::INFINITY);
    assert_eq!(aggregates::agg_count(&state, offset, 3), 0);
}

#[test]
fn init_state_hashset_keys_empty() {
    let prog = build_single_slot_program(0x01, 8, 0); // HASHSET
    let size = calculate_state_size(&prog);
    assert!(size > 0);

    let mut state = vec![0u8; 16384];
    init_state(&mut state, &prog).expect("init must succeed");

    let meta = STATE_HEADER_SIZE;
    let offset = bytes::read_u32(&state, meta);
    let cap = bytes::read_u32(&state, meta + 4);
    assert_eq!(cap, 16);
    for i in 0..cap {
        assert_eq!(bytes::read_u32(&state, offset + i * 4), EMPTY_KEY);
    }
}

// ---------------------------------------------------------------------------
// state_init.zig test 12: vm_calculate_grown_state_size
// ---------------------------------------------------------------------------

#[test]
fn calculate_grown_state_size_hashmap_cap_doubles() {
    let prog = build_single_slot_program(0x00, 8, 0);
    let size = calculate_state_size(&prog);
    let mut state = vec![0u8; 16384];
    init_state(&mut state, &prog).expect("init must succeed");

    let grown_size = calculate_grown_state_size(&state, 0);
    assert!(grown_size > size);

    let new_cap = 32u32; // nextPowerOf2(16*2)
    let mut expected = align8(STATE_HEADER_SIZE + SLOT_META_SIZE);
    expected += new_cap * 4 + new_cap * 4 + new_cap * 8;
    expected = align8(expected);
    assert_eq!(grown_size, expected);
}

// ---------------------------------------------------------------------------
// vm_test.zig extracts (init/size-only)
// ---------------------------------------------------------------------------

// vm_test.zig "hashmap no-timestamp slot reduces state size"
#[test]
fn hashmap_no_timestamp_slot_reduces_state_size() {
    let with_ts = build_single_slot_program(0x00, 4, 0);
    let no_ts = build_single_slot_program(0x40, 4, 0); // no_hashmap_timestamps

    let with_ts_size = calculate_state_size(&with_ts);
    let no_ts_size = calculate_state_size(&no_ts);
    // Effective capacity nextPowerOf2(4*2) = 16, saving one f64 per entry.
    assert_eq!(with_ts_size - no_ts_size, 16 * 8);
}

// vm_test.zig "hashmap invalid no-timestamp+ttl flag combination is rejected"
#[test]
fn hashmap_no_timestamp_plus_ttl_combination_rejected() {
    let prog = build_single_slot_ttl_program(0x40, 4, 0, 10.0, 0.0, false);
    assert_eq!(calculate_state_size(&prog), 0);

    let mut state = vec![0u8; 256];
    assert_eq!(
        init_state(&mut state, &prog),
        Err(ErrorCode::InvalidProgram)
    );
}

#[test]
fn calculate_state_size_rejects_bad_magic_and_short_programs() {
    let mut prog = build_single_slot_program(0x00, 8, 0);
    prog[HASH_PREFIX] = 0xde; // corrupt magic
    assert_eq!(calculate_state_size(&prog), 0);
    assert_eq!(calculate_state_size(&[0u8; 8]), 0);
}

// TTL sizing composition: primary + align8'd eviction index (+ evicted buffer).
#[test]
fn calculate_state_size_ttl_hashset_adds_eviction_buffers() {
    let base = build_single_slot_program(0x01, 8, 0);
    let ttl = build_single_slot_ttl_program(0x01, 8, 0, 10.0, 0.0, false);
    let ttl_evict = build_single_slot_ttl_program(0x01, 8, 0, 10.0, 0.0, true);

    let cap = 16u32;
    let base_size = calculate_state_size(&base);
    assert_eq!(
        calculate_state_size(&ttl),
        base_size + align8(cap * EVICTION_ENTRY_SIZE)
    );
    assert_eq!(
        calculate_state_size(&ttl_evict),
        base_size + align8(cap * EVICTION_ENTRY_SIZE) + align8(1024 * EVICTION_ENTRY_SIZE)
    );
}

// TTL metadata fields land in the slot meta record.
#[test]
fn init_state_ttl_hashset_writes_eviction_metadata() {
    let prog = build_single_slot_ttl_program(0x01, 8, 0, 10.0, 2.5, true);
    let size = calculate_state_size(&prog);
    assert!(size > 0);

    let mut state = vec![0u8; size as usize];
    init_state(&mut state, &prog).expect("init must succeed");

    let meta = STATE_HEADER_SIZE;
    let cap = bytes::read_u32(&state, meta + 4);
    assert_eq!(cap, 16);
    assert!((bytes::read_f32(&state, meta + 16) - 10.0).abs() < 1e-6); // ttl_seconds
    assert!((bytes::read_f32(&state, meta + 20) - 2.5).abs() < 1e-6); // grace_seconds
    let eviction_index_offset = bytes::read_u32(&state, meta + 24);
    let eviction_index_capacity = bytes::read_u32(&state, meta + 28);
    let evicted_buffer_offset = bytes::read_u32(&state, meta + 36);
    assert!(eviction_index_offset > 0);
    assert_eq!(eviction_index_capacity, cap);
    assert!(evicted_buffer_offset > eviction_index_offset);
    assert_eq!(state[(meta + 15) as usize], 1); // timestamp_field_idx
}

// ---------------------------------------------------------------------------
// Growth end-to-end (manual insertion in place of vm_execute_batch, which is
// the dispatch slice; mirrors vm_test.zig "growth preserves entries")
// ---------------------------------------------------------------------------

fn insert_into_hashmap(state: &mut [u8], offset: u32, cap: u32, key: u32, value: u32) {
    let mut pos = hash_key(key, cap);
    while bytes::read_u32(state, offset + pos * 4) != EMPTY_KEY {
        pos = (pos + 1) & (cap - 1);
    }
    bytes::write_u32(state, offset + pos * 4, key);
    bytes::write_u32(state, offset + cap * 4 + pos * 4, value);
    let meta_size = bytes::read_u32(state, STATE_HEADER_SIZE + 8) + 1;
    bytes::write_u32(state, STATE_HEADER_SIZE + 8, meta_size);
}

#[test]
fn grow_state_hashmap_preserves_entries_and_doubles_cap() {
    let prog = build_single_slot_program(0x00, 8, 0);
    let size = calculate_state_size(&prog);
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, &prog).expect("init must succeed");

    let meta = STATE_HEADER_SIZE;
    let offset = bytes::read_u32(&state, meta);
    let cap = bytes::read_u32(&state, meta + 4);
    for i in 0..11u32 {
        insert_into_hashmap(&mut state, offset, cap, i, i * 10);
    }

    let grown_size = calculate_grown_state_size(&state, 0);
    let mut grown = vec![0u8; grown_size as usize];
    grow_state(&state, &mut grown, 0).expect("grow must succeed");

    let new_offset = bytes::read_u32(&grown, meta);
    let new_cap = bytes::read_u32(&grown, meta + 4);
    assert_eq!(new_cap, 32);
    assert_eq!(bytes::read_u32(&grown, meta + 8), 11); // rehashed size
    for i in 0..11u32 {
        assert_eq!(map_get(&grown, new_offset, new_cap, i), Some(i * 10));
    }
    assert_eq!(map_get(&grown, new_offset, new_cap, 100), None);
}

#[test]
fn grow_state_relocates_full_derived_fact_cells_without_lane_drift() {
    let capacity = 4u32;
    let prog = build_hashmap_and_condition_tree_program(capacity as u16);
    let size = calculate_state_size(&prog);
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, &prog).expect("init must succeed");

    let old_derived = bytes::read_u32(&state, StateHeaderOffset::DERIVED_FACTS_OFFSET);
    let identity_a = (3u64 << 32) | 7;
    let identity_b = (9u64 << 32) | 11;
    bytes::write_u64(&mut state, old_derived + 8, identity_a);
    bytes::write_u64(&mut state, old_derived + 24, identity_b);
    bytes::write_u32(&mut state, old_derived + capacity * 8 + 4, 0x1122_3344);
    bytes::write_u32(&mut state, old_derived + capacity * 8 + 12, 0x99aa_bbcc);
    bytes::write_u32(&mut state, old_derived + capacity * 12 + 4, 0x5566_7788);
    bytes::write_u32(&mut state, old_derived + capacity * 12 + 12, 0xddee_ff00);

    let grown_size = calculate_grown_state_size(&state, 0);
    let mut grown = vec![0u8; grown_size as usize];
    grow_state(&state, &mut grown, 0).expect("grow must succeed");

    let new_derived = bytes::read_u32(&grown, StateHeaderOffset::DERIVED_FACTS_OFFSET);
    assert_ne!(new_derived, old_derived);
    assert_eq!(new_derived % 8, 0);
    assert_eq!(bytes::read_u64(&grown, new_derived + 8), identity_a);
    assert_eq!(bytes::read_u64(&grown, new_derived + 24), identity_b);
    assert_eq!(
        bytes::read_u32(&grown, new_derived + capacity * 8 + 4),
        0x1122_3344,
    );
    assert_eq!(
        bytes::read_u32(&grown, new_derived + capacity * 8 + 12),
        0x99aa_bbcc,
    );
    assert_eq!(
        bytes::read_u32(&grown, new_derived + capacity * 12 + 4),
        0x5566_7788,
    );
    assert_eq!(
        bytes::read_u32(&grown, new_derived + capacity * 12 + 12),
        0xddee_ff00,
    );
}

#[test]
fn grow_state_struct_map_rehashes_rows() {
    let fields = [StructFieldType::UInt32 as u8, StructFieldType::Int64 as u8];
    let prog = build_struct_map_program(8, 0, &fields);
    let size = calculate_state_size(&prog);
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, &prog).expect("init must succeed");

    let meta = STATE_HEADER_SIZE;
    let offset = bytes::read_u32(&state, meta);
    let cap = bytes::read_u32(&state, meta + 4);
    assert_eq!(cap, 16);
    let layout = compute_struct_row_layout_padded(2, &fields);
    assert_eq!(
        u32::from(bytes::read_u16(&state, meta + 16)),
        layout.row_size
    );

    // Manually place one row at its probe slot.
    let keys_off = offset + layout.descriptor_size;
    let rows_off = keys_off + cap * 4;
    let key = 77u32;
    let pos = hash_key(key, cap);
    bytes::write_u32(&mut state, keys_off + pos * 4, key);
    let row = rows_off + pos * layout.row_size;
    state[row as usize] = 0b11; // both fields present
    bytes::write_u32(&mut state, row + 1, 123); // UINT32 field after bitset
    bytes::write_i64(&mut state, row + 1 + 4, -9); // INT64 field
    bytes::write_u32(&mut state, meta + 8, 1);

    let grown_size = calculate_grown_state_size(&state, 0);
    let mut grown = vec![0u8; grown_size as usize];
    grow_state(&state, &mut grown, 0).expect("grow must succeed");

    let new_offset = bytes::read_u32(&grown, meta);
    let new_cap = bytes::read_u32(&grown, meta + 4);
    assert_eq!(new_cap, 32);
    assert_eq!(bytes::read_u32(&grown, meta + 8), 1);
    // Descriptor prefix copied.
    assert_eq!(
        &grown[new_offset as usize..(new_offset + 2) as usize],
        &fields
    );
    // Row lands at its new probe position with intact bytes.
    let new_keys_off = new_offset + layout.descriptor_size;
    let new_pos = hash_key(key, new_cap);
    assert_eq!(bytes::read_u32(&grown, new_keys_off + new_pos * 4), key);
    let new_row = new_keys_off + new_cap * 4 + new_pos * layout.row_size;
    assert_eq!(grown[new_row as usize], 0b11);
    assert_eq!(bytes::read_u32(&grown, new_row + 1), 123);
    assert_eq!(bytes::read_i64(&grown, new_row + 1 + 4), -9);
}

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

proptest! {
    // Size is 8-aligned and monotonic in requested capacity for every
    // capacity-bearing SLOT_DEF type. Capacity starts at 1: a requested
    // capacity of 0 means "use the 1024 default" (state_init.zig:184), so
    // monotonicity deliberately does not hold across 0 — that default is
    // pinned by `capacity_zero_means_default_1024` below.
    #[test]
    fn state_size_aligned_and_monotonic(
        flags in prop_oneof![Just(0x00u8), Just(0x01), Just(0x03), Just(0x08), Just(0x40)],
        cap_a in 1u16..2048,
        cap_b in 1u16..2048,
    ) {
        let (lo, hi) = (cap_a.min(cap_b), cap_a.max(cap_b));
        let prog_lo = build_single_slot_program(flags, lo as u8, (lo >> 8) as u8);
        let prog_hi = build_single_slot_program(flags, hi as u8, (hi >> 8) as u8);
        let size_lo = calculate_state_size(&prog_lo);
        let size_hi = calculate_state_size(&prog_hi);
        prop_assert!(size_lo > 0 && size_hi > 0);
        prop_assert_eq!(size_lo % 8, 0);
        prop_assert!(size_lo <= size_hi);
    }

    // state_init.zig:184 — requested capacity 0 defaults to 1024 for every
    // non-fixed-size slot type, so size(0) == size(1024).
    #[test]
    fn capacity_zero_means_default_1024(
        flags in prop_oneof![Just(0x00u8), Just(0x01), Just(0x03), Just(0x08), Just(0x40)],
    ) {
        let prog_zero = build_single_slot_program(flags, 0, 0);
        let prog_default = build_single_slot_program(flags, 0, 4); // 1024 = 0x0400
        prop_assert_eq!(
            calculate_state_size(&prog_zero),
            calculate_state_size(&prog_default)
        );
    }

    // Reset restores every region init actually writes. Faithfully-ported
    // Zig semantics: a HASHMAP's VALUES side-array is never written by init
    // (it relies on the buffer being zeroed exactly once, at allocation), so
    // reset does NOT restore scribbled values — only keys, timestamps,
    // header, and metadata. Keys all being EMPTY_KEY makes the stale values
    // unobservable through map lookups.
    #[test]
    fn reset_state_restores_init_written_regions(
        flags in prop_oneof![Just(0x00u8), Just(0x01), Just(0x02), Just(0x05)],
        cap in 0u8..64,
        scribble in proptest::collection::vec(any::<u8>(), 1..64),
    ) {
        // For AGGREGATE (0x02) / SCALAR (0x05), cap_lo is the AggType byte —
        // keep it in the valid range.
        let cap_lo = if flags == 0x02 || flags == 0x05 { 1 + (cap % 5) } else { cap };
        let prog = build_single_slot_program(flags, cap_lo, 0);
        let size = calculate_state_size(&prog) as usize;
        prop_assert!(size > 0);

        let mut fresh = vec![0u8; size];
        init_state(&mut fresh, &prog).expect("init");

        let meta = STATE_HEADER_SIZE;
        let offset = bytes::read_u32(&fresh, meta) as usize;
        let slot_cap = bytes::read_u32(&fresh, meta + 4) as usize;
        // The one region init leaves to zero-allocation: HASHMAP values.
        let unrestored = if flags == 0x00 {
            offset + slot_cap * 4..offset + slot_cap * 8
        } else {
            0..0
        };

        let mut scribbled = fresh.clone();
        let data_start = align8(STATE_HEADER_SIZE + SLOT_META_SIZE) as usize;
        for (i, b) in scribble.iter().enumerate() {
            let idx = data_start + (i * 7) % (size - data_start);
            scribbled[idx] = *b;
        }
        reset_state(&mut scribbled, &prog).expect("reset");
        for i in 0..size {
            if !unrestored.contains(&i) {
                prop_assert_eq!(scribbled[i], fresh[i], "byte {} not restored", i);
            }
        }
    }

    // Growing a hashmap preserves every inserted entry, whatever the keys.
    #[test]
    fn grow_preserves_arbitrary_hashmap_entries(
        keys in proptest::collection::btree_set(0u32..1_000_000, 1..11),
    ) {
        let prog = build_single_slot_program(0x00, 8, 0);
        let size = calculate_state_size(&prog) as usize;
        let mut state = vec![0u8; size];
        init_state(&mut state, &prog).expect("init");

        let meta = STATE_HEADER_SIZE;
        let offset = bytes::read_u32(&state, meta);
        let cap = bytes::read_u32(&state, meta + 4);
        for &k in &keys {
            insert_into_hashmap(&mut state, offset, cap, k, k.wrapping_mul(7));
        }

        let grown_size = calculate_grown_state_size(&state, 0) as usize;
        let mut grown = vec![0u8; grown_size];
        grow_state(&state, &mut grown, 0).expect("grow");

        let new_offset = bytes::read_u32(&grown, meta);
        let new_cap = bytes::read_u32(&grown, meta + 4);
        prop_assert_eq!(new_cap, next_pow2_check(cap * 2));
        prop_assert_eq!(bytes::read_u32(&grown, meta + 8), keys.len() as u32);
        for &k in &keys {
            prop_assert_eq!(map_get(&grown, new_offset, new_cap, k), Some(k.wrapping_mul(7)));
        }
    }

    // Grown-size calculation is metadata-driven: growing twice keeps working.
    #[test]
    fn repeated_growth_is_metadata_driven(cap in 0u8..32) {
        let prog = build_single_slot_program(0x01, cap, 0); // HASHSET
        let size = calculate_state_size(&prog) as usize;
        let mut state = vec![0u8; size];
        init_state(&mut state, &prog).expect("init");

        let meta = STATE_HEADER_SIZE;
        let cap1 = bytes::read_u32(&state, meta + 4);

        let g1_size = calculate_grown_state_size(&state, 0) as usize;
        let mut g1 = vec![0u8; g1_size];
        grow_state(&state, &mut g1, 0).expect("grow 1");
        prop_assert_eq!(bytes::read_u32(&g1, meta + 4), next_pow2_check(cap1 * 2));

        let g2_size = calculate_grown_state_size(&g1, 0) as usize;
        let mut g2 = vec![0u8; g2_size];
        grow_state(&g1, &mut g2, 0).expect("grow 2");
        prop_assert_eq!(bytes::read_u32(&g2, meta + 4), next_pow2_check(cap1 * 4));
    }
}

fn next_pow2_check(n: u32) -> u32 {
    columine_types::types::next_power_of_2(n)
}
