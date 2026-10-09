//! Coverage for struct-map and nested-container behavior, including row
//! byte-image pinning, arena growth, and randomized properties.
//!
//! Scaffolding builds real 48-byte metadata records and binds `SlotMetaView`;
//! assertions exercise metadata offsets and change flags directly. Randomized
//! tests use a deterministic LCG because the random stream is not contract.

use columine_types::types::{
    ChangeFlag, EMPTY_KEY, ErrorCode, Opcode, PROGRAM_MAGIC, STATE_FORMAT_VERSION,
    STATE_HEADER_SIZE, STATE_MAGIC, SlotMetaOffset, SlotType, StateHeaderOffset, StructFieldType,
    TOMBSTONE, hash_key, struct_field_size,
};
use columine_vm::bytes;
use columine_vm::hash_table::{ENTRY_NONE, FlatTable, hashset_byte_size};
use columine_vm::hashmap_ops::{batch_map_remove, bind_slot_map};
use columine_vm::hashset_ops::{batch_set_remove, bind_slot_set};
use columine_vm::hooks::NoVm;
use columine_vm::meta::SlotMetaView;
use columine_vm::nested::{
    Arena, NestedPrefix, NestedScratch, arena_data_offset, arena_header_offset, get_inner_offset,
    get_inner_set_size, inner_agg_get_count, inner_agg_get_f64, inner_map_get, inner_set_contains,
    nested_agg_update, nested_map_upsert_last, nested_set_insert, outer_keys_offset,
    prepare_nested_agg_update, prepare_nested_map_upsert_last, prepare_nested_set_insert,
    write_nested_prefix,
};
use columine_vm::state_init::{
    DEFAULT_ACCEPTED_PROGRAM_MAGICS, calculate_state_size, compute_struct_row_layout_padded,
    init_state,
};
use columine_vm::struct_map::{StructMap2Slot, StructMapSlot};
use columine_vm::vm::{CheckedMap, CheckedSet, CheckedStructMap, CheckedStructMap2};
use proptest::prelude::*;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

const SLOT_OFFSET: u32 = STATE_HEADER_SIZE + 48; // one 48-byte slot-meta record

/// Second key lane used by every exact-pair table in the differentials. Lane 2
/// is held constant so the pair table answers the same membership question as
/// the unary tables.
const PAIR_KEY2: u32 = 7;

// ---------------------------------------------------------------------------
// Fixtures write the real state header and the padded row layout that
// `bind_slot` validates, so every table they build reaches the checked views.
// ---------------------------------------------------------------------------

/// Header a checked binding requires: magic, format version, and one slot.
fn write_header(state: &mut [u8]) {
    bytes::write_u32(state, StateHeaderOffset::MAGIC, STATE_MAGIC);
    state[StateHeaderOffset::FORMAT_VERSION as usize] = STATE_FORMAT_VERSION;
    bytes::write_u32(state, StateHeaderOffset::NUM_SLOTS, 1);
}

/// One STRUCT_MAP or STRUCT_MAP2 slot at `SLOT_OFFSET`: metadata record, field
/// descriptor, and EMPTY key cells. The row size and bitset width come from the
/// same padded layout `state_init` stores and validates.
fn mk_struct_slot_state(state_len: usize, cap: u32, kind: SlotType, field_types: &[u8]) -> Vec<u8> {
    let mut state = vec![0u8; state_len];
    write_header(&mut state);
    let layout = compute_struct_row_layout_padded(field_types.len() as u8, field_types);
    let meta = STATE_HEADER_SIZE as usize;
    state[meta..meta + 4].copy_from_slice(&SLOT_OFFSET.to_le_bytes());
    state[meta + 4..meta + 8].copy_from_slice(&cap.to_le_bytes());
    state[meta + SlotMetaOffset::TYPE_FLAGS as usize] = kind as u8;
    state[meta + SlotMetaOffset::AGG_TYPE as usize] = field_types.len() as u8; // num_fields
    state[meta + SlotMetaOffset::STRUCT_BITSET_BYTES as usize] =
        u8::try_from(layout.bitset_bytes).expect("bitset width fits a byte");
    let row_size = u16::try_from(layout.row_size).expect("row size fits the metadata field");
    let row_at = meta + SlotMetaOffset::TTL_SECONDS as usize;
    state[row_at..row_at + 2].copy_from_slice(&row_size.to_le_bytes());
    for (i, &ft) in field_types.iter().enumerate() {
        state[SLOT_OFFSET as usize + i] = ft;
    }
    let keys = SLOT_OFFSET + layout.descriptor_size;
    for j in 0..cap {
        let off = (keys + j * 4) as usize;
        state[off..off + 4].copy_from_slice(&EMPTY_KEY.to_le_bytes());
    }
    state
}

/// STRUCT_MAP fixture over `field_types`, laid out exactly as `state_init` pads it.
fn mk_struct_map_state(state_len: usize, cap: u32, field_types: &[u8]) -> Vec<u8> {
    mk_struct_slot_state(state_len, cap, SlotType::StructMap, field_types)
}

/// STRUCT_MAP2 fixture with one UINT32 field, keyed by a pair at `SLOT_OFFSET`.
fn mk_struct_map2_state(state_len: usize, cap: u32) -> Vec<u8> {
    mk_struct_slot_state(
        state_len,
        cap,
        SlotType::StructMap2,
        &[StructFieldType::UInt32 as u8],
    )
}

/// Metadata-managed flat-table state: one slot whose data region is the
/// headerless `[keys][entries?]` image that `bind_slot_map`/`bind_slot_set` and
/// the checked views address. The header lets `bind_slot` resolve the record.
fn mk_flat_slot_state(state_len: usize, cap: u32, slot_type: SlotType) -> Vec<u8> {
    let mut state = vec![0u8; state_len];
    write_header(&mut state);
    let meta = STATE_HEADER_SIZE as usize;
    state[meta..meta + 4].copy_from_slice(&SLOT_OFFSET.to_le_bytes());
    state[meta + 4..meta + 8].copy_from_slice(&cap.to_le_bytes());
    state[meta + SlotMetaOffset::TYPE_FLAGS as usize] = slot_type as u8;
    for pos in 0..cap {
        let off = (SLOT_OFFSET + pos * 4) as usize;
        state[off..off + 4].copy_from_slice(&EMPTY_KEY.to_le_bytes());
    }
    state
}

///  `test "StructMapSlot — bind and upsert"`
#[test]
fn struct_map_bind_and_upsert() {
    // 2 fields (UINT32, STRING): bitset 1, row = 1 + 4 + 4 = 9 → padded 12.
    let mut state = mk_struct_map_state(8192, 16, &[0, 4]);
    let smap = StructMapSlot::bind(&state, 0);
    assert_eq!(smap.size(&state), 0);
    assert_eq!(smap.descriptor_size, 8); // align8(2)

    let result = smap.upsert(&mut state, 42).expect("upsert 42");
    assert!(result.is_new);
    assert_eq!(smap.size(&state), 1);

    // Write fields to the row as raw little-endian values.
    let row = smap.row_off(result.pos);
    smap.clear_bitset(&mut state, row);
    StructMapSlot::set_field_bit(&mut state, row, 0);
    let f0_off = smap.field_offset(&state, 0);
    let o = (row + f0_off) as usize;
    state[o..o + 4].copy_from_slice(&100u32.to_le_bytes());
    StructMapSlot::set_field_bit(&mut state, row, 1);
    let f1_off = smap.field_offset(&state, 1);
    let o = (row + f1_off) as usize;
    state[o..o + 4].copy_from_slice(&5000u32.to_le_bytes());

    // Read back
    let read_row_off = smap.get_row_ptr_by_key(&state, 42);
    assert_ne!(read_row_off, 0xFFFF_FFFF);
    assert!(StructMapSlot::is_field_set(&state, read_row_off, 0));
    let o = (read_row_off + f0_off) as usize;
    assert_eq!(u32::from_le_bytes(state[o..o + 4].try_into().unwrap()), 100);
    let o = (read_row_off + f1_off) as usize;
    assert_eq!(
        u32::from_le_bytes(state[o..o + 4].try_into().unwrap()),
        5000
    );

    // Upsert same key — not new
    let result2 = smap.upsert(&mut state, 42).expect("upsert 42 again");
    assert!(!result2.is_new);
    assert_eq!(smap.size(&state), 1); // unchanged
}

///  `test "StructMapSlot — CAPACITY_EXCEEDED"`
#[test]
fn struct_map_capacity_exceeded() {
    // One UINT32 field: bitset 1, row 5 padded to 8 as state_init lays it out.
    let mut state = mk_struct_map_state(8192, 16, &[0]);
    let smap = StructMapSlot::bind(&state, 0);

    // Fill to 70% load (11 entries)
    for i in 1..=11u32 {
        assert!(smap.upsert(&mut state, i).is_some(), "key {i}");
    }
    assert_eq!(smap.size(&state), 11);

    // 12th should fail
    assert!(smap.upsert(&mut state, 12).is_none());
}

/// Row byte-image + placement pinning (additional coverage): the row region
/// bytes after known writes, with the key's hash placement asserted from
/// `hash_key` — the byte contract the checked struct-map row reader exposes to TS.
#[test]
fn struct_map_row_byte_image() {
    // Fields (UINT32, BOOL, FLOAT64): bitset 1, row = 1+4+1+8 = 14 → pad 16.
    let mut state = mk_struct_map_state(8192, 16, &[0, 3, 2]);
    let smap = StructMapSlot::bind(&state, 0);

    let up = smap.upsert(&mut state, 42).unwrap();
    // hash_key(42, 16) == 4 — independently verified in columine-types tests.
    assert_eq!(up.pos, 4);
    assert_eq!(up.pos, hash_key(42, 16));

    let u32_col: Vec<u8> = 7u32.to_le_bytes().to_vec();
    let bool_col: Vec<u8> = 1u32.to_le_bytes().to_vec();
    let f64_col: Vec<u8> = 2.5f64.to_le_bytes().to_vec();
    let cols: Vec<&[u8]> = vec![&u32_col, &bool_col, &f64_col];

    smap.clear_bitset(&mut state, smap.row_off(up.pos));
    smap.write_scalar_field(&mut state, up.pos, 0, &cols, 0, 0);
    smap.write_scalar_field(&mut state, up.pos, 1, &cols, 1, 0);
    smap.write_scalar_field(&mut state, up.pos, 2, &cols, 2, 0);

    let row = smap.row_off(up.pos) as usize;
    // [bitset=0b111][u32 7 LE][bool 1][f64 2.5 LE][2 pad bytes]
    let mut expected = vec![0b0000_0111u8];
    expected.extend_from_slice(&7u32.to_le_bytes());
    expected.push(1);
    expected.extend_from_slice(&2.5f64.to_le_bytes());
    expected.extend_from_slice(&[0, 0]);
    assert_eq!(&state[row..row + 16], expected.as_slice());

    // clear_scalar_field drops exactly one bit.
    StructMapSlot::clear_scalar_field(&mut state, smap.row_off(up.pos), 1);
    assert_eq!(state[row], 0b0000_0101);
}

// ---------------------------------------------------------------------------
// Nested scaffolding uses a real metadata record and `SlotMetaView`.
// ---------------------------------------------------------------------------

fn mk_nested_state(
    state_len: usize,
    outer_cap: u32,
    inner_type: SlotType,
    inner_initial_cap: u16,
    agg_byte: u8,
) -> Vec<u8> {
    let mut state = vec![0u8; state_len];
    let meta = STATE_HEADER_SIZE as usize;
    state[meta..meta + 4].copy_from_slice(&SLOT_OFFSET.to_le_bytes());
    state[meta + 4..meta + 8].copy_from_slice(&outer_cap.to_le_bytes());
    state[meta + SlotMetaOffset::TYPE_FLAGS as usize] = SlotType::Nested as u8;

    write_nested_prefix(
        &mut state,
        SLOT_OFFSET,
        NestedPrefix {
            inner_type,
            inner_initial_cap,
            inner_agg_type_byte: agg_byte,
            depth: 1,
        },
    );

    // Outer keys → EMPTY_KEY
    for j in 0..outer_cap {
        let off = (outer_keys_offset(SLOT_OFFSET) + j * 4) as usize;
        state[off..off + 4].copy_from_slice(&EMPTY_KEY.to_le_bytes());
    }

    // Arena over the remainder of the buffer ()
    let arena_start = arena_data_offset(SLOT_OFFSET, outer_cap);
    let arena_cap = state_len as u32 - arena_start;
    Arena::init_at(
        &mut state,
        arena_header_offset(SLOT_OFFSET, outer_cap),
        arena_cap,
    );

    state
}

fn meta_of(state: &[u8]) -> SlotMetaView {
    SlotMetaView::read(state, 0)
}

///  `test "nested set — basic insert and contains"`
#[test]
fn nested_set_basic_insert_and_contains() {
    let mut state = mk_nested_state(4096, 16, SlotType::HashSet, 16, 1);
    let meta = meta_of(&state);

    let mut scratch = NestedScratch::default();
    assert_eq!(
        nested_set_insert(&mut state, &meta, 100, 42, &mut scratch),
        ErrorCode::Ok
    );
    assert_eq!(
        nested_set_insert(&mut state, &meta, 100, 43, &mut scratch),
        ErrorCode::Ok
    );
    assert_eq!(
        nested_set_insert(&mut state, &meta, 200, 42, &mut scratch),
        ErrorCode::Ok
    );

    assert_eq!(meta.size(&state), 2);
    // The INSERTED change flag is observable through metadata.
    assert_ne!(meta.change_flags(&state) & ChangeFlag::INSERTED, 0);

    let inner_100 = get_inner_offset(&state, &meta, 100);
    assert_ne!(inner_100, 0);
    assert_eq!(get_inner_set_size(&state, inner_100), 2);
    assert!(inner_set_contains(&state, inner_100, 42));
    assert!(inner_set_contains(&state, inner_100, 43));
    assert!(!inner_set_contains(&state, inner_100, 44));
}

///  `test "nested set — duplicate insert is idempotent"`
#[test]
fn nested_set_duplicate_idempotent() {
    let mut state = mk_nested_state(4096, 16, SlotType::HashSet, 16, 1);
    let meta = meta_of(&state);

    let mut scratch = NestedScratch::default();
    let _ = nested_set_insert(&mut state, &meta, 100, 42, &mut scratch);
    let _ = nested_set_insert(&mut state, &meta, 100, 42, &mut scratch);
    let _ = nested_set_insert(&mut state, &meta, 100, 42, &mut scratch);

    assert_eq!(
        get_inner_set_size(&state, get_inner_offset(&state, &meta, 100)),
        1
    );
}

///  `test "nested map — upsert last-write-wins"`
#[test]
fn nested_map_upsert_last_write_wins() {
    let mut state = mk_nested_state(4096, 16, SlotType::HashMap, 16, 1);
    let meta = meta_of(&state);

    let mut scratch = NestedScratch::default();
    let _ = nested_map_upsert_last(&mut state, &meta, 1, 10, 100, &mut scratch);
    let _ = nested_map_upsert_last(&mut state, &meta, 1, 11, 200, &mut scratch);
    let _ = nested_map_upsert_last(&mut state, &meta, 1, 10, 300, &mut scratch); // overwrite

    let inner = get_inner_offset(&state, &meta, 1);
    assert_eq!(inner_map_get(&state, inner, 10), 300);
    assert_eq!(inner_map_get(&state, inner, 11), 200);
}

///  `test "nested aggregate — count per outer key"`
#[test]
fn nested_aggregate_count_per_outer_key() {
    let mut state = mk_nested_state(4096, 16, SlotType::Aggregate, 0, 2); // COUNT
    let meta = meta_of(&state);

    let mut scratch = NestedScratch::default();
    let _ = nested_agg_update(&mut state, &meta, 1, 0, &mut scratch);
    let _ = nested_agg_update(&mut state, &meta, 1, 0, &mut scratch);
    let _ = nested_agg_update(&mut state, &meta, 1, 0, &mut scratch);
    let _ = nested_agg_update(&mut state, &meta, 2, 0, &mut scratch);

    assert_eq!(
        inner_agg_get_count(&state, get_inner_offset(&state, &meta, 1), 2),
        3
    );
    assert_eq!(
        inner_agg_get_count(&state, get_inner_offset(&state, &meta, 2), 2),
        1
    );
}

///  `test "nested aggregate — sum f64 per outer key"`
#[test]
fn nested_aggregate_sum_f64_per_outer_key() {
    let mut state = mk_nested_state(4096, 16, SlotType::Aggregate, 0, 1); // SUM
    let meta = meta_of(&state);

    let mut scratch = NestedScratch::default();
    let _ = nested_agg_update(&mut state, &meta, 1, 100.0f64.to_bits(), &mut scratch);
    let _ = nested_agg_update(&mut state, &meta, 1, 250.5f64.to_bits(), &mut scratch);
    let _ = nested_agg_update(&mut state, &meta, 2, 50.0f64.to_bits(), &mut scratch);

    let sum1 = inner_agg_get_f64(&state, get_inner_offset(&state, &meta, 1));
    let sum2 = inner_agg_get_f64(&state, get_inner_offset(&state, &meta, 2));
    assert!((sum1 - 350.5).abs() < 0.001);
    assert!((sum2 - 50.0).abs() < 0.001);
}

#[test]
fn nested_min_max_skip_leading_nan() {
    let mut scratch = NestedScratch::default();
    for (agg_type, finite) in [(3, -7.5f64), (4, 9.25)] {
        let mut state = mk_nested_state(4096, 16, SlotType::Aggregate, 0, agg_type);
        let meta = meta_of(&state);
        assert_eq!(
            nested_agg_update(&mut state, &meta, 1, f64::NAN.to_bits(), &mut scratch),
            ErrorCode::Ok
        );
        assert_eq!(
            nested_agg_update(&mut state, &meta, 1, finite.to_bits(), &mut scratch),
            ErrorCode::Ok
        );
        let inner = get_inner_offset(&state, &meta, 1);
        assert_eq!(inner_agg_get_f64(&state, inner).to_bits(), finite.to_bits());
        assert_eq!(inner_agg_get_count(&state, inner, agg_type), 2);
    }
}

///  `test "nested set — inner growth on load factor exceeded"`
#[test]
fn nested_set_inner_growth_on_load_factor() {
    let mut state = mk_nested_state(16384, 16, SlotType::HashSet, 4, 1);
    let meta = meta_of(&state);

    let mut scratch = NestedScratch::default();
    for i in 1..=20u32 {
        assert_eq!(
            nested_set_insert(&mut state, &meta, 1, i, &mut scratch),
            ErrorCode::Ok
        );
    }

    let inner = get_inner_offset(&state, &meta, 1);
    assert_eq!(get_inner_set_size(&state, inner), 20);
    for i in 1..=20u32 {
        assert!(inner_set_contains(&state, inner, i), "elem {i}");
    }
}

///  `test "stress — arena accounting: each outer key allocates one
/// inner set"`
#[test]
fn stress_arena_accounting() {
    let mut state = mk_nested_state(32768, 64, SlotType::HashSet, 8, 1);
    let meta = meta_of(&state);

    let mut scratch = NestedScratch::default();
    for k in 1..=10u32 {
        assert_eq!(
            nested_set_insert(&mut state, &meta, k, 42, &mut scratch),
            ErrorCode::Ok
        );
    }

    assert_eq!(meta.size(&state), 10);
    let arena = Arena::bind(arena_header_offset(SLOT_OFFSET, 64));
    assert!(arena.used(&state) > 0);
}

const LCG_MUL: u64 = 6364136223846793005;
const LCG_ADD: u64 = 1442695040888963407;

fn nested_randomized_ground_truth(seed: u64) {
    let mut state = mk_nested_state(262144, 64, SlotType::HashSet, 8, 1);
    let meta = meta_of(&state);

    let mut lcg = seed;
    let mut step = move || {
        lcg = lcg.wrapping_mul(LCG_MUL).wrapping_add(LCG_ADD);
        (lcg >> 33) as u32
    };

    let mut truth: Vec<Vec<u32>> = vec![Vec::new(); 32];
    let mut scratch = NestedScratch::default();
    for _ in 0..500 {
        let outer_key = 1 + step() % 30;
        let elem = 1 + step() % 200;
        assert_eq!(
            nested_set_insert(&mut state, &meta, outer_key, elem, &mut scratch),
            ErrorCode::Ok
        );
        let bucket = &mut truth[(outer_key - 1) as usize];
        if !bucket.contains(&elem) {
            bucket.push(elem);
        }
    }

    for (idx, bucket) in truth.iter().enumerate().take(30) {
        let outer_key = (idx + 1) as u32;
        let inner = get_inner_offset(&state, &meta, outer_key);
        if bucket.is_empty() {
            assert_eq!(inner, 0);
            continue;
        }
        assert_ne!(inner, 0);
        assert_eq!(get_inner_set_size(&state, inner), bucket.len() as u32);
        for &elem in bucket {
            assert!(inner_set_contains(&state, inner, elem));
        }
    }
}

///  `test "stress — randomized Map<K, Set<V>> with ground truth"`
#[test]
fn stress_randomized_map_of_sets() {
    nested_randomized_ground_truth(0xDEAD_BEEF);
}

///  `test "stress — multi-seed randomized Map<K, Set<V>>"`
#[test]
fn stress_multi_seed_randomized() {
    for seed in [0xDEAD_BEEFu64, 0xCAFE_BABE, 0x1234_5678, 0xFEED_FACE] {
        nested_randomized_ground_truth(seed);
    }
}

///  `test "stress — CAPACITY_EXCEEDED when arena exhausted"`
#[test]
fn stress_capacity_exceeded_when_arena_exhausted() {
    let mut state = mk_nested_state(1024, 16, SlotType::HashSet, 4, 1);
    let meta = meta_of(&state);

    let mut scratch = NestedScratch::default();
    let mut got_exceeded = false;
    for i in 1..=100u32 {
        if nested_set_insert(&mut state, &meta, i, 42, &mut scratch) == ErrorCode::CapacityExceeded
        {
            got_exceeded = true;
            break;
        }
    }
    assert!(got_exceeded);
    assert!(meta.size(&state) > 0);
}

///  `test "stress — inner growth cascade tracks arena fragmentation"`
#[test]
fn stress_inner_growth_cascade_fragmentation() {
    let mut state = mk_nested_state(131072, 16, SlotType::HashSet, 4, 1);
    let meta = meta_of(&state);

    let mut scratch = NestedScratch::default();
    for i in 1..=100u32 {
        assert_eq!(
            nested_set_insert(&mut state, &meta, 1, i, &mut scratch),
            ErrorCode::Ok
        );
    }

    let inner = get_inner_offset(&state, &meta, 1);
    assert_ne!(inner, 0);
    for i in 1..=100u32 {
        assert!(inner_set_contains(&state, inner, i));
    }

    let arena = Arena::bind(arena_header_offset(SLOT_OFFSET, 16));
    assert!(arena.used(&state) > 0);

    // Abandoned capacities plus the live table bound arena usage from below.
    // Growth starts at an inner capacity of 16, so the bound holds across
    // every abandoned allocation.
    let dead_space = hashset_byte_size(4)
        + hashset_byte_size(8)
        + hashset_byte_size(16)
        + hashset_byte_size(32)
        + hashset_byte_size(64);
    let live_size = hashset_byte_size(128);
    assert!(arena.used(&state) >= live_size + dead_space);
}

///  `test "stress — many outer keys fill arena to capacity"`
#[test]
fn stress_many_outer_keys_fill_arena() {
    let mut state = mk_nested_state(65536, 128, SlotType::HashSet, 4, 1);
    let meta = meta_of(&state);

    let mut scratch = NestedScratch::default();
    let mut successful_keys = 0u32;
    for k in 1..=128u32 {
        if nested_set_insert(&mut state, &meta, k, k * 10, &mut scratch) == ErrorCode::Ok {
            successful_keys += 1;
        } else {
            break;
        }
    }

    assert!(successful_keys > 0);
    assert!(successful_keys < 128);

    for v in 1..=successful_keys {
        assert!(inner_set_contains(
            &state,
            get_inner_offset(&state, &meta, v),
            v * 10
        ));
    }
}

///  `test "stress — inner map growth preserves all entries"`
#[test]
fn stress_inner_map_growth_preserves_entries() {
    let mut state = mk_nested_state(131072, 16, SlotType::HashMap, 4, 1);
    let meta = meta_of(&state);

    let mut scratch = NestedScratch::default();
    for i in 1..=50u32 {
        assert_eq!(
            nested_map_upsert_last(&mut state, &meta, 1, i, i * 10, &mut scratch),
            ErrorCode::Ok
        );
    }

    let inner = get_inner_offset(&state, &meta, 1);
    assert_ne!(inner, 0);
    for i in 1..=50u32 {
        assert_eq!(inner_map_get(&state, inner, i), i * 10);
    }
}

// ---------------------------------------------------------------------------
// Property tests cover arbitrary scalar schemas and nested-container updates.

/// Arbitrary scalar-field schemas: write via `write_scalar_field`, read every
/// field back through `field_offset`, and check the bitset — the row is the
/// TS-visible byte contract.
fn scalar_field_type() -> impl Strategy<Value = u8> {
    // UINT32, INT64, FLOAT64, BOOL, STRING (arena-resident ARRAY_* excluded)
    prop::sample::select(vec![0u8, 1, 2, 3, 4])
}

proptest! {
    #[test]
    fn prop_struct_map_row_round_trip(
        field_types in prop::collection::vec(scalar_field_type(), 1..=6),
        keys in prop::collection::hash_set(1u32..EMPTY_KEY - 2, 1..=8),
        raw in prop::collection::vec(any::<u64>(), 6),
    ) {
        let num_fields = field_types.len() as u8;
        let row_size_unpadded: u32 = 1 + field_types
            .iter()
            .map(|&ft| struct_field_size(StructFieldType::from_u8(ft).unwrap()))
            .sum::<u32>();
        let row_size = (row_size_unpadded + 3) & !3; // state_init pads rows to 4

        let mut state = mk_struct_map_state(16384, 16, &field_types);
        let smap = StructMapSlot::bind(&state, 0);
        prop_assert_eq!(smap.num_fields, num_fields);
        prop_assert_eq!(smap.row_size, row_size);

        for &key in &keys {
            let up = smap.upsert(&mut state, key).expect("load factor not reached");
            smap.clear_bitset(&mut state, smap.row_off(up.pos));

            // One single-cell column per field, value derived from key+field.
            let cells: Vec<Vec<u8>> = field_types.iter().enumerate().map(|(f, &ft)| {
                let v = raw[f].wrapping_add(u64::from(key));
                match ft {
                    0 | 4 => (v as u32).to_le_bytes().to_vec(),
                    3 => u32::from(v % 2 == 0).to_le_bytes().to_vec(),
                    _ => v.to_le_bytes().to_vec(),
                }
            }).collect();
            let cols: Vec<&[u8]> = cells.iter().map(Vec::as_slice).collect();
            for f in 0..num_fields {
                smap.write_scalar_field(&mut state, up.pos, f, &cols, u32::from(f), 0);
            }
        }

        for &key in &keys {
            let row = smap.get_row_ptr_by_key(&state, key);
            prop_assert_ne!(row, 0xFFFF_FFFF);
            for (f, &ft) in field_types.iter().enumerate() {
                prop_assert!(StructMapSlot::is_field_set(&state, row, f as u8));
                let off = (row + smap.field_offset(&state, f as u8)) as usize;
                let v = raw[f].wrapping_add(u64::from(key));
                match ft {
                    0 | 4 => prop_assert_eq!(
                        u32::from_le_bytes(state[off..off + 4].try_into().unwrap()),
                        v as u32
                    ),
                    3 => prop_assert_eq!(state[off], u8::from(v % 2 == 0)),
                    _ => prop_assert_eq!(
                        u64::from_le_bytes(state[off..off + 8].try_into().unwrap()),
                        v
                    ),
                }
            }
        }
    }

    /// Nested Map<K, Set<V>> under arbitrary interleavings vs a std model —
    /// every recovered-Ok insert is observable, sizes match, absent keys read 0.
    #[test]
    fn prop_nested_set_matches_model(
        ops in prop::collection::vec((1u32..=40, 1u32..=300), 1..300),
    ) {
        use std::collections::{BTreeMap, BTreeSet};
        let mut state = mk_nested_state(262144, 64, SlotType::HashSet, 8, 1);
        let meta = meta_of(&state);
        let mut model: BTreeMap<u32, BTreeSet<u32>> = BTreeMap::new();

        let mut scratch = NestedScratch::default();
        for &(outer, elem) in &ops {
            if nested_set_insert(&mut state, &meta, outer, elem, &mut scratch) == ErrorCode::Ok {
                model.entry(outer).or_default().insert(elem);
            }
        }

        prop_assert_eq!(meta.size(&state), model.len() as u32);
        for outer in 1..=40u32 {
            let inner = get_inner_offset(&state, &meta, outer);
            match model.get(&outer) {
                None => prop_assert_eq!(inner, 0),
                Some(set) => {
                    prop_assert_ne!(inner, 0);
                    prop_assert_eq!(get_inner_set_size(&state, inner), set.len() as u32);
                    for &elem in set {
                        prop_assert!(inner_set_contains(&state, inner, elem));
                    }
                }
            }
        }
    }

    /// The four layouts share one probe contract: identical insert/duplicate/
    /// refusal decisions and logical membership under the same workload.
    #[test]
    fn prop_table_probe_implementations_agree(
        keys in prop::collection::vec(1u32..100, 0..100),
    ) {
        let capacity = 32;
        let mut flat_state = vec![0u8; 4096];
        let flat = FlatTable::init(&mut flat_state, 0, capacity, ENTRY_NONE);
        let mut struct_state =
            mk_struct_map_state(8192, capacity, &[StructFieldType::UInt32 as u8]);
        let struct_map = StructMapSlot::bind(&struct_state, 0);
        let mut struct2_state = mk_struct_map2_state(8192, capacity);
        let struct_map2 = StructMap2Slot::bind(&struct2_state, 0);
        let mut nested_state =
            mk_nested_state(1 << 20, capacity, SlotType::HashSet, 4, 1);
        let nested_meta = meta_of(&nested_state);

        let mut scratch = NestedScratch::default();
        for key in keys {
            let flat_result = flat.insert_key(&mut flat_state, key);
            let struct_result = struct_map.upsert(&mut struct_state, key);
            let struct2_result = struct_map2.upsert(&mut struct2_state, key, 7);
            let nested_was_present =
                get_inner_offset(&nested_state, &nested_meta, key) != 0;
            let nested_result =
                nested_set_insert(&mut nested_state, &nested_meta, key, key, &mut scratch);

            let expected = flat_result.map(|is_new| (true, is_new));
            prop_assert_eq!(
                struct_result.map(|result| (true, result.is_new)),
                expected
            );
            prop_assert_eq!(
                struct2_result.map(|result| (true, result.is_new)),
                expected
            );
            prop_assert_eq!(
                (nested_result == ErrorCode::Ok)
                    .then_some((true, !nested_was_present)),
                expected
            );
        }

        for key in 1..100 {
            let present = flat.contains(&flat_state, key);
            prop_assert_eq!(struct_map.find(&struct_state, key).is_some(), present);
            prop_assert_eq!(struct_map2.find(&struct2_state, key, 7).is_some(), present);
            prop_assert_eq!(
                get_inner_offset(&nested_state, &nested_meta, key) != 0,
                present
            );
        }
    }

    /// Arena bump-allocator invariants: 8-aligned offsets, strictly
    /// increasing, non-overlapping, never past capacity.
    #[test]
    fn prop_arena_alloc_invariants(
        cap in 8u32..4096,
        sizes in prop::collection::vec(1u32..256, 1..64),
    ) {
        let hdr = 16u32;
        let mut buf = vec![0u8; (hdr + 8 + cap) as usize];
        let arena = Arena::init_at(&mut buf, hdr, cap);

        let mut prev_end = arena.data_offset;
        for &sz in &sizes {
            match arena.alloc(&mut buf, sz) {
                Some(off) => {
                    prop_assert_eq!(off % 8, 0, "8-aligned (data starts align8)");
                    prop_assert!(off >= prev_end, "no overlap");
                    prev_end = off + columine_types::types::align8(sz);
                    prop_assert!(prev_end - arena.data_offset <= cap, "within capacity");
                    prop_assert_eq!(arena.used(&buf), prev_end - arena.data_offset);
                }
                None => {
                    // Refusal must be exactly the out-of-capacity case.
                    let u = arena.used(&buf);
                    prop_assert!(u + columine_types::types::align8(sz) > cap);
                }
            }
        }
    }

    /// Flat tables, unary struct maps, and nested outer tables share one home
    /// position (`hash_key`), one bounded linear probe, and one 70% load
    /// ceiling, so the same insert workload must place every key in the same
    /// cell of all three — the cell images are compared, not just membership.
    /// `StructMap2Slot` homes on `hash_key_pair`, so only its accept/duplicate
    /// decisions are comparable, never its placement.
    ///
    /// Nested outer tables have no removal operation, so this property is
    /// insert-only; removal parity lives in
    /// `prop_table_read_exports_match_model`.
    #[test]
    fn prop_probe_cell_images_agree(
        keys in prop::collection::vec(1u32..64, 0..64),
    ) {
        let capacity = 32;
        let mut flat_state = mk_flat_slot_state(8192, capacity, SlotType::HashMap);
        let flat_meta = meta_of(&flat_state);
        let flat = bind_slot_map(&flat_meta);
        let mut struct_state =
            mk_struct_map_state(8192, capacity, &[StructFieldType::UInt32 as u8]);
        let struct_map = StructMapSlot::bind(&struct_state, 0);
        let mut struct2_state = mk_struct_map2_state(8192, capacity);
        let struct_map2 = StructMap2Slot::bind(&struct2_state, 0);
        let mut nested_state = mk_nested_state(1 << 20, capacity, SlotType::HashSet, 4, 1);
        let nested_meta = meta_of(&nested_state);

        let mut scratch = NestedScratch::default();
        for key in keys {
            // Some(true) = inserted, Some(false) = duplicate, None = refused.
            let flat_result = flat.upsert_u32(&mut flat_state, key, key);
            let struct_result = struct_map.upsert(&mut struct_state, key);
            let struct2_result = struct_map2.upsert(&mut struct2_state, key, PAIR_KEY2);
            let nested_present = get_inner_offset(&nested_state, &nested_meta, key) != 0;
            let nested_ok =
                nested_set_insert(&mut nested_state, &nested_meta, key, key, &mut scratch)
                    == ErrorCode::Ok;

            prop_assert_eq!(struct_result.map(|up| up.is_new), flat_result);
            prop_assert_eq!(struct2_result.map(|up| up.is_new), flat_result);
            prop_assert_eq!(nested_ok.then_some(!nested_present), flat_result);
        }

        let live = flat.size(&flat_state);
        prop_assert_eq!(struct_map.size(&struct_state), live);
        prop_assert_eq!(struct_map2.size(&struct2_state), live);
        prop_assert_eq!(nested_meta.size(&nested_state), live);

        let checked_struct =
            CheckedStructMap::bind(&struct_state, 0).expect("valid struct-map slot");
        for pos in 0..capacity {
            let expected = flat.key_at(&flat_state, pos);
            // A dead cell is not an iteration position, so the checked read refuses it.
            let expected_cell = if expected == EMPTY_KEY || expected == TOMBSTONE {
                Err(ErrorCode::InvalidState)
            } else {
                Ok(expected)
            };
            prop_assert_eq!(
                checked_struct.iter_key(pos),
                expected_cell,
                "struct-map cell {} diverged from the flat table", pos
            );
            prop_assert_eq!(
                bytes::read_u32(&nested_state, outer_keys_offset(SLOT_OFFSET) + pos * 4),
                expected,
                "nested outer cell {} diverged from the flat table", pos
            );
        }
    }

    /// Exported read helpers, bound table views, and an independent `BTreeMap`
    /// model must agree on membership, lookup, and iteration after an
    /// insert/remove workload. Query keys include `EMPTY_KEY` and `TOMBSTONE`:
    /// both are ordinary u32 values at the exported ABI boundary, and removal
    /// leaves TOMBSTONE cells behind that a sentinel-blind probe would match.
    #[test]
    fn prop_table_read_exports_match_model(
        ops in prop::collection::vec((any::<bool>(), 1u32..48, 0u32..1000), 0..96),
    ) {
        use std::collections::BTreeMap;
        let capacity = 32;
        let max_load = capacity * 7 / 10;

        let mut map_state = mk_flat_slot_state(8192, capacity, SlotType::HashMap);
        let map_meta = meta_of(&map_state);
        let map = bind_slot_map(&map_meta);

        let mut set_state = mk_flat_slot_state(8192, capacity, SlotType::HashSet);
        let set_meta = meta_of(&set_state);
        let set = bind_slot_set(&set_meta);

        let mut struct_state =
            mk_struct_map_state(8192, capacity, &[StructFieldType::UInt32 as u8]);
        let struct_map = StructMapSlot::bind(&struct_state, 0);
        let field0 = struct_map.field_offset(&struct_state, 0);

        let mut struct2_state = mk_struct_map2_state(8192, capacity);
        let struct_map2 = StructMap2Slot::bind(&struct2_state, 0);
        let pair_field0 = struct_map2.field_offset(&struct2_state, 0);

        let mut model: BTreeMap<u32, u32> = BTreeMap::new();
        let mut hooks = NoVm::default();

        for (is_remove, key, value) in ops {
            let present = model.contains_key(&key);
            if is_remove {
                prop_assert_eq!(map.contains(&map_state, key), present);
                prop_assert_eq!(set.contains(&set_state, key), present);
                prop_assert_eq!(
                    batch_map_remove(false, &mut map_state, &map_meta, 0, &[key], &mut hooks),
                    ErrorCode::Ok
                );
                prop_assert_eq!(
                    batch_set_remove(false, &mut set_state, &set_meta, 0, &[key], &mut hooks),
                    ErrorCode::Ok
                );
                prop_assert_eq!(struct_map.remove(&mut struct_state, key).is_some(), present);
                prop_assert_eq!(
                    struct_map2.remove(&mut struct2_state, key, PAIR_KEY2).is_some(),
                    present
                );
                model.remove(&key);
                continue;
            }

            // Refusal is exactly "absent and already at the load ceiling":
            // tombstones never exhaust a bounded probe that reuses them.
            let refused = !present && model.len() as u32 >= max_load;
            let expect = (!refused).then_some(!present);

            prop_assert_eq!(map.upsert_u32(&mut map_state, key, value), expect);
            prop_assert_eq!(set.insert_key(&mut set_state, key), expect);
            let struct_result = struct_map.upsert(&mut struct_state, key);
            prop_assert_eq!(struct_result.map(|up| up.is_new), expect);
            let struct2_result = struct_map2.upsert(&mut struct2_state, key, PAIR_KEY2);
            prop_assert_eq!(struct2_result.map(|up| up.is_new), expect);

            let cell = value.to_le_bytes();
            if let Some(up) = struct_result {
                struct_map.clear_bitset(&mut struct_state, struct_map.row_off(up.pos));
                struct_map.write_scalar_field(&mut struct_state, up.pos, 0, &[&cell], 0, 0);
            }
            if let Some(up) = struct2_result {
                struct_map2.clear_bitset(&mut struct2_state, struct_map2.row_off(up.pos));
                struct_map2.write_scalar_field(&mut struct2_state, up.pos, 0, &[&cell], 0, 0);
            }
            if !refused {
                model.insert(key, value);
            }
        }

        let checked_map = CheckedMap::bind(&map_state, 0).expect("valid map slot");
        let checked_set = CheckedSet::bind(&set_state, 0).expect("valid set slot");
        let checked_struct =
            CheckedStructMap::bind(&struct_state, 0).expect("valid struct-map slot");
        let checked_pair = CheckedStructMap2::bind(&struct2_state, 0).expect("valid pair slot");
        for key in (1u32..48).chain([EMPTY_KEY, TOMBSTONE]) {
            let expected = model.get(&key).copied();

            prop_assert_eq!(map.get_u32(&map_state, key), expected);
            prop_assert_eq!(
                checked_map.get(key),
                expected,
                "CheckedMap disagrees for key {}", key
            );

            prop_assert_eq!(set.contains(&set_state, key), expected.is_some());
            prop_assert_eq!(
                checked_set.contains(key),
                expected.is_some(),
                "CheckedSet disagrees for key {}", key
            );

            let row = checked_struct.row(key);
            prop_assert_eq!(
                row,
                struct_map.find(&struct_state, key).map(|pos| struct_map.row_off(pos)),
                "CheckedStructMap disagrees with the bound view for key {}", key
            );
            match expected {
                None => prop_assert_eq!(row, None, "absent key {} resolved a row", key),
                Some(v) => {
                    let row = row.expect("present key resolves a row");
                    prop_assert!(StructMapSlot::is_field_set(&struct_state, row, 0));
                    prop_assert_eq!(bytes::read_u32(&struct_state, row + field0), v);
                }
            }

            let pair_row = checked_pair.row(key, PAIR_KEY2);
            let pair_expected = struct_map2
                .find(&struct2_state, key, PAIR_KEY2)
                .map(|pos| struct_map2.row_off(pos));
            prop_assert_eq!(
                pair_row,
                pair_expected,
                "CheckedStructMap2 disagrees with the bound view for key {}", key
            );
            match expected {
                None => prop_assert_eq!(pair_row, None),
                Some(v) => {
                    let pair_row = pair_row.expect("present pair resolves a row");
                    prop_assert_eq!(bytes::read_u32(&struct2_state, pair_row + pair_field0), v);
                }
            }
        }

        // Exported and bound walks agree within each layout. Flat tables
        // compact on deletion; struct tables retain stable tombstone positions,
        // so only logical membership is comparable between those layouts.
        let bound: Vec<(u32, u32, u32)> = map
            .iter_live(&map_state)
            .map(|(pos, key)| (pos, key, map.entry_u32_at(&map_state, pos)))
            .collect();
        let live_cells: Vec<(u32, u32)> = bound.iter().map(|&(pos, k, _)| (pos, k)).collect();

        let mut exported = Vec::new();
        let mut pos = checked_map.iter_start();
        while pos < capacity {
            let packed = checked_map.iter_get(pos).expect("live map cell");
            exported.push((pos, packed as u32, (packed >> 32) as u32));
            pos = checked_map.iter_next(pos).expect("ascending live walk");
        }
        prop_assert_eq!(&exported, &bound);
        prop_assert_eq!(
            exported
                .iter()
                .map(|&(_, key, value)| (key, value))
                .collect::<BTreeMap<u32, u32>>(),
            model.clone()
        );

        let mut set_cells = Vec::new();
        let mut pos = checked_set.iter_start();
        while pos < capacity {
            set_cells.push((pos, checked_set.iter_get(pos).expect("live set cell")));
            pos = checked_set.iter_next(pos).expect("ascending live walk");
        }
        prop_assert_eq!(&set_cells, &live_cells);

        let mut struct_cells = Vec::new();
        let mut pos = checked_struct.iter_start();
        while pos < capacity {
            let cell = checked_struct.iter_key(pos).expect("live struct-map cell");
            struct_cells.push((pos, cell));
            pos = checked_struct.iter_next(pos).expect("ascending live walk");
        }
        prop_assert_eq!(
            struct_cells.iter().map(|&(_, key)| key).collect::<std::collections::BTreeSet<_>>(),
            live_cells.iter().map(|&(_, key)| key).collect::<std::collections::BTreeSet<_>>()
        );

        // Exact-pair tables home on `hash_key_pair`, so only the key set is
        // comparable across layouts, never the cell order.
        let mut pair_rows = BTreeMap::new();
        let mut pos = checked_pair.iter_start();
        while pos < capacity {
            pair_rows.insert(
                checked_pair.iter_key1(pos).expect("live pair cell"),
                checked_pair.iter_key2(pos).expect("live pair cell"),
            );
            pos = checked_pair.iter_next(pos).expect("ascending live walk");
        }
        prop_assert_eq!(
            pair_rows.keys().copied().collect::<Vec<u32>>(),
            model.keys().copied().collect::<Vec<u32>>()
        );
        prop_assert!(pair_rows.values().all(|&key2| key2 == PAIR_KEY2));
    }
}

#[test]
fn nested_retained_plans_cover_every_written_byte_without_mutating_during_prepare() {
    for inner_type in [SlotType::HashSet, SlotType::HashMap] {
        let mut state = mk_nested_state(8192, 16, inner_type, 4, 1);
        let meta = meta_of(&state);
        let arena_start = arena_data_offset(meta.offset, meta.capacity) as usize;
        let arena_capacity =
            Arena::bind(arena_header_offset(meta.offset, meta.capacity)).capacity(&state);
        state[arena_start..].fill(0xa5);
        let mut scratch = NestedScratch::default();
        for key in (1u32..24).chain([5, 8, 5]) {
            let before = state.clone();
            let plan = if inner_type == SlotType::HashMap {
                prepare_nested_map_upsert_last(&mut state, &meta, 123, key, key * 100, &mut scratch)
            } else {
                prepare_nested_set_insert(&mut state, &meta, 123, key, &mut scratch)
            }
            .expect("real nested plan fits the arena");
            assert_eq!(plan.state(), before, "preparation is read-only");
            let (image, ranges) = plan.apply();
            for (offset, (&old, &new)) in before.iter().zip(image.iter()).enumerate() {
                if old != new {
                    assert!(
                        ranges.iter().any(|&(start, len)| {
                            offset >= start as usize && offset < (start + len) as usize
                        }),
                        "unplanned write at byte {offset}"
                    );
                }
            }
            assert!(
                ranges.iter().map(|&(_, len)| len).sum::<u32>() < arena_capacity,
                "a nested operation must not capture the whole arena"
            );
        }
        let inner = get_inner_offset(&state, &meta, 123);
        if inner_type == SlotType::HashMap {
            assert_eq!(inner_map_get(&state, inner, 8), 800);
        } else {
            assert!(inner_set_contains(&state, inner, 8));
        }
        assert!(
            state[arena_start..].contains(&0xa5),
            "untouched arena bytes stay untouched"
        );
    }
}

#[test]
fn nested_refused_plan_leaves_outer_keys_flags_and_arena_unchanged() {
    let outer_cap = 4;
    let len = arena_data_offset(SLOT_OFFSET, outer_cap) as usize;
    let mut state = mk_nested_state(len, outer_cap, SlotType::HashMap, 4, 1);
    let meta = meta_of(&state);
    let before = state.clone();
    let mut scratch = NestedScratch::default();
    assert_eq!(
        prepare_nested_map_upsert_last(&mut state, &meta, 123, 7, 70, &mut scratch).err(),
        Some(ErrorCode::CapacityExceeded),
    );
    assert_eq!(state, before);
    assert_eq!(
        prepare_nested_map_upsert_last(&mut state, &meta, 123, TOMBSTONE, 70, &mut scratch).err(),
        Some(ErrorCode::CapacityExceeded),
    );
    assert_eq!(state, before);
}

#[test]
fn nested_aggregate_plan_captures_only_fixed_value_count_and_outer_allocation() {
    let mut state = mk_nested_state(4096, 16, SlotType::Aggregate, 1, 1);
    let meta = meta_of(&state);
    let mut scratch = NestedScratch::default();
    for value in [3.5f64, -2.0, 8.25] {
        let before = state.clone();
        let plan = prepare_nested_agg_update(&mut state, &meta, 19, value.to_bits(), &mut scratch)
            .expect("real aggregate plan fits");
        assert_eq!(plan.state(), before);
        let (image, ranges) = plan.apply();
        for (offset, (&old, &new)) in before.iter().zip(image.iter()).enumerate() {
            if old != new {
                assert!(
                    ranges.iter().any(|&(start, len)| {
                        offset >= start as usize && offset < (start + len) as usize
                    }),
                    "unplanned aggregate write at byte {offset}"
                );
            }
        }
    }
    let inner = get_inner_offset(&state, &meta, 19);
    assert_eq!(
        inner_agg_get_f64(&state, inner).to_bits(),
        9.75f64.to_bits()
    );
    assert_eq!(inner_agg_get_count(&state, inner, 1), 3);
}

// ---------------------------------------------------------------------------
// Allocation oracle. Accounting is thread-local, so concurrent tests cannot
// leak allocations into a measured scope.

struct CountingAllocator;

thread_local! {
    static ALLOCATION_COUNT: Cell<Option<usize>> = const { Cell::new(None) };
}

fn count_allocation() {
    let _ = ALLOCATION_COUNT.try_with(|count| {
        if let Some(current) = count.get() {
            count.set(Some(current + 1));
        }
    });
}

// SAFETY: every allocation and deallocation forwards the original pointer and
// layout to System; the counter does not change the allocation contract.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count_allocation();
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count_allocation();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count_allocation();
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

struct AllocationScope;

impl Drop for AllocationScope {
    fn drop(&mut self) {
        ALLOCATION_COUNT.with(|count| count.set(None));
    }
}

fn allocations_during<T>(run: impl FnOnce() -> T) -> (T, usize) {
    ALLOCATION_COUNT.with(|count| {
        assert!(count.get().is_none(), "allocation scopes cannot nest");
        count.set(Some(0));
    });
    let scope = AllocationScope;
    let result = run();
    let count = ALLOCATION_COUNT.with(|count| count.get().expect("active allocation scope"));
    drop(scope);
    (result, count)
}

/// One pass of 400 set inserts over outer keys `base + 1..=base + 9`. Every
/// pass repeats the same inner growth sequence, so a warm pass reuses the
/// capacity an earlier pass admitted.
fn set_pass(state: &mut [u8], meta: &SlotMetaView, scratch: &mut NestedScratch, base: u32) -> u32 {
    let mut planned = 0;
    for key in 1..=400u32 {
        if let Ok(plan) = prepare_nested_set_insert(state, meta, base + key % 9 + 1, key, scratch) {
            let _ = plan.apply();
            planned += 1;
        }
    }
    planned
}

/// Map counterpart of `set_pass`: each key upserts its own value.
fn map_pass(state: &mut [u8], meta: &SlotMetaView, scratch: &mut NestedScratch, base: u32) -> u32 {
    let mut planned = 0;
    for key in 1..=400u32 {
        let outer = base + key % 9 + 1;
        if let Ok(plan) = prepare_nested_map_upsert_last(state, meta, outer, key, key, scratch) {
            let _ = plan.apply();
            planned += 1;
        }
    }
    planned
}

/// Aggregate counterpart of `set_pass`: every plan updates a SUM aggregate.
fn agg_pass(state: &mut [u8], meta: &SlotMetaView, scratch: &mut NestedScratch, base: u32) -> u32 {
    let mut planned = 0;
    for key in 1..=400u32 {
        let outer = base + key % 9 + 1;
        let bits = f64::from(key).to_bits();
        if let Ok(plan) = prepare_nested_agg_update(state, meta, outer, bits, scratch) {
            let _ = plan.apply();
            planned += 1;
        }
    }
    planned
}

/// A warm pass allocates nothing. The first pass admits the high-water mark of
/// every inner table capacity its growth reaches; a repeat pass over fresh outer
/// keys walks the same capacities and must reuse that retained scratch.
#[test]
fn nested_warm_plans_allocate_nothing_for_admitted_operations() {
    const STATE_LEN: usize = 65536;
    let mut set_state = mk_nested_state(STATE_LEN, 64, SlotType::HashSet, 4, 1);
    let set_meta = meta_of(&set_state);
    let mut set_scratch = NestedScratch::default();
    assert!(set_pass(&mut set_state, &set_meta, &mut set_scratch, 0) > 0);
    let (set_planned, set_allocations) =
        allocations_during(|| set_pass(&mut set_state, &set_meta, &mut set_scratch, 9));

    let mut map_state = mk_nested_state(STATE_LEN, 64, SlotType::HashMap, 4, 1);
    let map_meta = meta_of(&map_state);
    let mut map_scratch = NestedScratch::default();
    assert!(map_pass(&mut map_state, &map_meta, &mut map_scratch, 0) > 0);
    let (map_planned, map_allocations) =
        allocations_during(|| map_pass(&mut map_state, &map_meta, &mut map_scratch, 9));

    let mut agg_state = mk_nested_state(STATE_LEN, 64, SlotType::Aggregate, 0, 1);
    let agg_meta = meta_of(&agg_state);
    let mut agg_scratch = NestedScratch::default();
    assert_eq!(
        agg_pass(&mut agg_state, &agg_meta, &mut agg_scratch, 0),
        400
    );
    let (agg_planned, agg_allocations) =
        allocations_during(|| agg_pass(&mut agg_state, &agg_meta, &mut agg_scratch, 9));

    assert!(
        set_planned > 0 && map_planned > 0,
        "measured passes planned work"
    );
    assert_eq!(
        agg_planned, 400,
        "aggregate plans never refuse on this image"
    );
    // Outer key 10 is the first fresh key of the measured set pass. It receives
    // every key divisible by nine, so its inner set grows past initial capacity.
    assert!(get_inner_set_size(&set_state, get_inner_offset(&set_state, &set_meta, 10)) > 4);
    assert_eq!(set_allocations, 0, "a warm set pass allocates nothing");
    assert_eq!(map_allocations, 0, "a warm map pass allocates nothing");
    assert_eq!(
        agg_allocations, 0,
        "a warm aggregate pass allocates nothing"
    );
}

/// A program in the wire layout `init_state` validates: a 32-byte hash prefix,
/// the header, the init section, `Halt`, then the reduce section.
fn program_bytes(num_slots: u8, num_inputs: u8, init: &[u8], reduce: &[u8]) -> Vec<u8> {
    let mut prog = vec![0u8; 32];
    prog.extend(PROGRAM_MAGIC.to_le_bytes());
    prog.extend([2, 0, 0, 0]); // version 2, no callbacks, no flags
    prog.extend(u32::from(num_slots).to_le_bytes());
    prog.extend(u32::from(num_inputs).to_le_bytes());
    prog.extend(((init.len() + 1) as u32).to_le_bytes());
    prog.extend((reduce.len() as u32).to_le_bytes());
    prog.extend_from_slice(init);
    prog.push(Opcode::Halt as u8);
    prog.extend_from_slice(reduce);
    prog
}

/// One FOR_EACH reduce section over rows of `type_id` running `body`.
fn for_each_reduce(type_id: u32, body: &[u8]) -> Vec<u8> {
    let mut reduce = vec![0xE0, 0, 1];
    reduce.extend(type_id.to_le_bytes());
    reduce.extend((body.len() as u16).to_le_bytes());
    reduce.extend_from_slice(body);
    reduce
}

/// A nested `Map<u32, Map<u32, u32>>` slot initialized through the real program
/// path. The outer hint 128 doubles to 256 cells and the inner HashMap hint 512
/// sizes each inner table, so `init_state` reserves the arena and `state.len()`
/// is the calculated image size.
fn real_nested_map_state() -> Vec<u8> {
    let init = [
        Opcode::SlotNested as u8,
        0, // slot index
        SlotType::Nested as u8,
        128,
        0, // outer capacity hint, little-endian u16
        SlotType::HashMap as u8,
        0,
        2, // inner initial capacity 512, little-endian u16
        1, // inner aggregate byte, unused by maps
    ];
    let body = [Opcode::NestedMapUpsertLast as u8, 0, 1, 2, 3];
    let prog = program_bytes(1, 4, &init, &for_each_reduce(1, &body));
    let size = calculate_state_size(&prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS)
        .expect("valid program admission");
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, &prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS)
        .expect("the nested program initializes");
    state
}

/// Peak retention follows the inner tables actually grown, not the image. The
/// image is the real `init_state` arena; 16 outer keys with 400 inserts each pay
/// one real rehash per inner table, and the retained scratch stays under one
/// sixty-fourth of the image length.
#[test]
fn nested_scratch_peak_retention_stays_far_below_state_len() {
    let mut state = real_nested_map_state();
    let state_len = state.len();
    let meta = meta_of(&state);
    assert_eq!(meta.capacity, 256, "outer hint 128 doubles to 256 cells");
    let mut scratch = NestedScratch::default();
    for outer in 1..=16u32 {
        for inner in 1..=400u32 {
            assert_eq!(
                nested_map_upsert_last(&mut state, &meta, outer, inner, inner, &mut scratch),
                ErrorCode::Ok
            );
        }
    }
    let retained = scratch.retained_bytes();
    assert!(retained > 0, "growth paid for retained scratch");
    assert!(
        retained * 64 <= state_len,
        "peak retention {retained} bytes exceeds one sixty-fourth of the {state_len}-byte image"
    );
    for outer in 1..=16u32 {
        let inner_offset = get_inner_offset(&state, &meta, outer);
        for inner in 1..=400u32 {
            assert_eq!(inner_map_get(&state, inner_offset, inner), inner);
        }
    }
}
