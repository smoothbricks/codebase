//! Translated test blocks from `packages/columine/src/vm/struct_map.zig`
//! (2/2) and `packages/columine/src/vm/nested.zig` (13/15 — the two `e2e —`
//! blocks drive vm.zig's init/execute entrypoints and are deferred to the
//! dispatch slice; see the crate README), plus row byte-image pinning and
//! proptest properties the Zig tree lacks.
//!
//! Scaffolding note: the Zig nested tests hand-build a `SlotMeta` STRUCT with
//! size/change-flag pointers at arbitrary scratch offsets (nested.zig:392).
//! That is test scaffolding, not contract — here the scaffold builds a real
//! 48-byte metadata record (like containers.rs) and binds `SlotMetaView`, so
//! the ops read size/flags from their metadata offsets. Assertions are the
//! Zig ones unchanged. Randomized Zig tests use a deterministic LCG instead
//! of `std.Random.DefaultPrng` (the PRNG stream is not contract).

use columine_types::types::{
    ChangeFlag, EMPTY_KEY, ErrorCode, STATE_HEADER_SIZE, SlotMetaOffset, SlotType, StructFieldType,
    hash_key, struct_field_size,
};
use columine_vm::hash_table::hashset_byte_size;
use columine_vm::meta::SlotMetaView;
use columine_vm::nested::{
    Arena, NestedPrefix, arena_data_offset, arena_header_offset, get_inner_offset,
    get_inner_set_size, inner_agg_get_count, inner_agg_get_f64, inner_map_get, inner_set_contains,
    nested_agg_update, nested_map_upsert_last, nested_set_insert, outer_keys_offset,
    write_nested_prefix,
};
use columine_vm::struct_map::StructMapSlot;
use proptest::prelude::*;

const SLOT_OFFSET: u32 = STATE_HEADER_SIZE + 48; // one 48-byte slot-meta record

// ---------------------------------------------------------------------------
// struct_map scaffolding — mirrors the manual metadata writes in
// struct_map.zig:215-243 (metadata byte reuse: 13 num_fields, 15 bitset,
// 16-17 row_size).
// ---------------------------------------------------------------------------

fn mk_struct_map_state(
    state_len: usize,
    cap: u32,
    field_types: &[u8],
    bitset_bytes: u8,
    row_size: u16,
) -> Vec<u8> {
    let mut state = vec![0u8; state_len];
    let meta = STATE_HEADER_SIZE as usize;
    state[meta..meta + 4].copy_from_slice(&SLOT_OFFSET.to_le_bytes());
    state[meta + 4..meta + 8].copy_from_slice(&cap.to_le_bytes());
    state[meta + SlotMetaOffset::TYPE_FLAGS as usize] = SlotType::StructMap as u8;
    state[meta + SlotMetaOffset::AGG_TYPE as usize] = field_types.len() as u8; // num_fields
    state[meta + SlotMetaOffset::TIMESTAMP_FIELD_IDX as usize] = bitset_bytes;
    state[meta + SlotMetaOffset::TTL_SECONDS as usize
        ..meta + SlotMetaOffset::TTL_SECONDS as usize + 2]
        .copy_from_slice(&row_size.to_le_bytes());

    // Field-type descriptor
    for (i, &ft) in field_types.iter().enumerate() {
        state[SLOT_OFFSET as usize + i] = ft;
    }
    // Keys → EMPTY_KEY (descriptor is align8(num_fields) bytes)
    let desc = columine_types::types::align8(field_types.len() as u32);
    for j in 0..cap {
        let off = (SLOT_OFFSET + desc + j * 4) as usize;
        state[off..off + 4].copy_from_slice(&EMPTY_KEY.to_le_bytes());
    }
    state
}

/// struct_map.zig `test "StructMapSlot — bind and upsert"`
#[test]
fn struct_map_bind_and_upsert() {
    // 2 fields (UINT32, STRING): bitset 1, row = 1 + 4 + 4 = 9 → padded 12.
    let mut state = mk_struct_map_state(8192, 16, &[0, 4], 1, 12);
    let smap = StructMapSlot::bind(&state, 0);
    assert_eq!(smap.size(&state), 0);
    assert_eq!(smap.descriptor_size, 8); // align8(2)

    let result = smap.upsert(&mut state, 42).expect("upsert 42");
    assert!(result.is_new);
    assert_eq!(smap.size(&state), 1);

    // Write fields to the row (the Zig block writes raw LE ints directly).
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

/// struct_map.zig `test "StructMapSlot — CAPACITY_EXCEEDED"`
#[test]
fn struct_map_capacity_exceeded() {
    // 1 field UINT32: bitset 1, row_size 5 (unpadded, as the Zig block writes).
    let mut state = mk_struct_map_state(8192, 16, &[0], 1, 5);
    let smap = StructMapSlot::bind(&state, 0);

    // Fill to 70% load (11 entries)
    for i in 1..=11u32 {
        assert!(smap.upsert(&mut state, i).is_some(), "key {i}");
    }
    assert_eq!(smap.size(&state), 11);

    // 12th should fail
    assert!(smap.upsert(&mut state, 12).is_none());
}

/// Row byte-image + placement pinning (no Zig counterpart): the row region
/// bytes after known writes, with the key's hash placement asserted from
/// `hash_key` — the byte contract `vm_struct_map_get_row_ptr` exposes to TS.
#[test]
fn struct_map_row_byte_image() {
    // Fields (UINT32, BOOL, FLOAT64): bitset 1, row = 1+4+1+8 = 14 → pad 16.
    let mut state = mk_struct_map_state(8192, 16, &[0, 3, 2], 1, 16);
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
// nested scaffolding — real metadata record replacing nested.zig:392's
// hand-built SlotMeta struct (see file header).
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

    // Arena over the remainder of the buffer (nested.zig:413-416)
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

/// nested.zig `test "nested set — basic insert and contains"`
#[test]
fn nested_set_basic_insert_and_contains() {
    let mut state = mk_nested_state(4096, 16, SlotType::HashSet, 16, 1);
    let meta = meta_of(&state);

    assert_eq!(nested_set_insert(&mut state, &meta, 100, 42), ErrorCode::Ok);
    assert_eq!(nested_set_insert(&mut state, &meta, 100, 43), ErrorCode::Ok);
    assert_eq!(nested_set_insert(&mut state, &meta, 200, 42), ErrorCode::Ok);

    assert_eq!(meta.size(&state), 2);
    // The INSERTED change flag is observable (Zig sets it via setChangeFlag).
    assert_ne!(meta.change_flags(&state) & ChangeFlag::INSERTED, 0);

    let inner_100 = get_inner_offset(&state, &meta, 100);
    assert_ne!(inner_100, 0);
    assert_eq!(get_inner_set_size(&state, inner_100), 2);
    assert!(inner_set_contains(&state, inner_100, 42));
    assert!(inner_set_contains(&state, inner_100, 43));
    assert!(!inner_set_contains(&state, inner_100, 44));
}

/// nested.zig `test "nested set — duplicate insert is idempotent"`
#[test]
fn nested_set_duplicate_idempotent() {
    let mut state = mk_nested_state(4096, 16, SlotType::HashSet, 16, 1);
    let meta = meta_of(&state);

    let _ = nested_set_insert(&mut state, &meta, 100, 42);
    let _ = nested_set_insert(&mut state, &meta, 100, 42);
    let _ = nested_set_insert(&mut state, &meta, 100, 42);

    assert_eq!(
        get_inner_set_size(&state, get_inner_offset(&state, &meta, 100)),
        1
    );
}

/// nested.zig `test "nested map — upsert last-write-wins"`
#[test]
fn nested_map_upsert_last_write_wins() {
    let mut state = mk_nested_state(4096, 16, SlotType::HashMap, 16, 1);
    let meta = meta_of(&state);

    let _ = nested_map_upsert_last(&mut state, &meta, 1, 10, 100);
    let _ = nested_map_upsert_last(&mut state, &meta, 1, 11, 200);
    let _ = nested_map_upsert_last(&mut state, &meta, 1, 10, 300); // overwrite

    let inner = get_inner_offset(&state, &meta, 1);
    assert_eq!(inner_map_get(&state, inner, 10), 300);
    assert_eq!(inner_map_get(&state, inner, 11), 200);
}

/// nested.zig `test "nested aggregate — count per outer key"`
#[test]
fn nested_aggregate_count_per_outer_key() {
    let mut state = mk_nested_state(4096, 16, SlotType::Aggregate, 0, 2); // COUNT
    let meta = meta_of(&state);

    let _ = nested_agg_update(&mut state, &meta, 1, 0);
    let _ = nested_agg_update(&mut state, &meta, 1, 0);
    let _ = nested_agg_update(&mut state, &meta, 1, 0);
    let _ = nested_agg_update(&mut state, &meta, 2, 0);

    assert_eq!(
        inner_agg_get_count(&state, get_inner_offset(&state, &meta, 1), 2),
        3
    );
    assert_eq!(
        inner_agg_get_count(&state, get_inner_offset(&state, &meta, 2), 2),
        1
    );
}

/// nested.zig `test "nested aggregate — sum f64 per outer key"`
#[test]
fn nested_aggregate_sum_f64_per_outer_key() {
    let mut state = mk_nested_state(4096, 16, SlotType::Aggregate, 0, 1); // SUM
    let meta = meta_of(&state);

    let _ = nested_agg_update(&mut state, &meta, 1, 100.0f64.to_bits());
    let _ = nested_agg_update(&mut state, &meta, 1, 250.5f64.to_bits());
    let _ = nested_agg_update(&mut state, &meta, 2, 50.0f64.to_bits());

    let sum1 = inner_agg_get_f64(&state, get_inner_offset(&state, &meta, 1));
    let sum2 = inner_agg_get_f64(&state, get_inner_offset(&state, &meta, 2));
    assert!((sum1 - 350.5).abs() < 0.001);
    assert!((sum2 - 50.0).abs() < 0.001);
}

/// nested.zig `test "nested set — inner growth on load factor exceeded"`
#[test]
fn nested_set_inner_growth_on_load_factor() {
    let mut state = mk_nested_state(16384, 16, SlotType::HashSet, 4, 1);
    let meta = meta_of(&state);

    for i in 1..=20u32 {
        assert_eq!(nested_set_insert(&mut state, &meta, 1, i), ErrorCode::Ok);
    }

    let inner = get_inner_offset(&state, &meta, 1);
    assert_eq!(get_inner_set_size(&state, inner), 20);
    for i in 1..=20u32 {
        assert!(inner_set_contains(&state, inner, i), "elem {i}");
    }
}

/// nested.zig `test "stress — arena accounting: each outer key allocates one
/// inner set"`
#[test]
fn stress_arena_accounting() {
    let mut state = mk_nested_state(32768, 64, SlotType::HashSet, 8, 1);
    let meta = meta_of(&state);

    for k in 1..=10u32 {
        assert_eq!(nested_set_insert(&mut state, &meta, k, 42), ErrorCode::Ok);
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
    for _ in 0..500 {
        let outer_key = 1 + step() % 30;
        let elem = 1 + step() % 200;
        assert_eq!(
            nested_set_insert(&mut state, &meta, outer_key, elem),
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

/// nested.zig `test "stress — randomized Map<K, Set<V>> with ground truth"`
#[test]
fn stress_randomized_map_of_sets() {
    nested_randomized_ground_truth(0xDEAD_BEEF);
}

/// nested.zig `test "stress — multi-seed randomized Map<K, Set<V>>"`
#[test]
fn stress_multi_seed_randomized() {
    for seed in [0xDEAD_BEEFu64, 0xCAFE_BABE, 0x1234_5678, 0xFEED_FACE] {
        nested_randomized_ground_truth(seed);
    }
}

/// nested.zig `test "stress — CAPACITY_EXCEEDED when arena exhausted"`
#[test]
fn stress_capacity_exceeded_when_arena_exhausted() {
    let mut state = mk_nested_state(1024, 16, SlotType::HashSet, 4, 1);
    let meta = meta_of(&state);

    let mut got_exceeded = false;
    for i in 1..=100u32 {
        if nested_set_insert(&mut state, &meta, i, 42) == ErrorCode::CapacityExceeded {
            got_exceeded = true;
            break;
        }
    }
    assert!(got_exceeded);
    assert!(meta.size(&state) > 0);
}

/// nested.zig `test "stress — inner growth cascade tracks arena fragmentation"`
#[test]
fn stress_inner_growth_cascade_fragmentation() {
    let mut state = mk_nested_state(131072, 16, SlotType::HashSet, 4, 1);
    let meta = meta_of(&state);

    for i in 1..=100u32 {
        assert_eq!(nested_set_insert(&mut state, &meta, 1, i), ErrorCode::Ok);
    }

    let inner = get_inner_offset(&state, &meta, 1);
    assert_ne!(inner, 0);
    for i in 1..=100u32 {
        assert!(inner_set_contains(&state, inner, i));
    }

    let arena = Arena::bind(arena_header_offset(SLOT_OFFSET, 16));
    assert!(arena.used(&state) > 0);

    // The Zig assertion verbatim (nested.zig:743-747): abandoned capacities
    // plus the live table bound the arena usage from below. (Zig's comment
    // says growth starts at 4, but nextPowerOf2 clamps the initial inner cap
    // to 16 — the `>=` bound holds either way, which is why the Zig test
    // passes today; ported unchanged.)
    let dead_space = hashset_byte_size(4)
        + hashset_byte_size(8)
        + hashset_byte_size(16)
        + hashset_byte_size(32)
        + hashset_byte_size(64);
    let live_size = hashset_byte_size(128);
    assert!(arena.used(&state) >= live_size + dead_space);
}

/// nested.zig `test "stress — many outer keys fill arena to capacity"`
#[test]
fn stress_many_outer_keys_fill_arena() {
    let mut state = mk_nested_state(65536, 128, SlotType::HashSet, 4, 1);
    let meta = meta_of(&state);

    let mut successful_keys = 0u32;
    for k in 1..=128u32 {
        if nested_set_insert(&mut state, &meta, k, k * 10) == ErrorCode::Ok {
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

/// nested.zig `test "stress — inner map growth preserves all entries"`
#[test]
fn stress_inner_map_growth_preserves_entries() {
    let mut state = mk_nested_state(131072, 16, SlotType::HashMap, 4, 1);
    let meta = meta_of(&state);

    for i in 1..=50u32 {
        assert_eq!(
            nested_map_upsert_last(&mut state, &meta, 1, i, i * 10),
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
// Proptests (no Zig counterparts)
// ---------------------------------------------------------------------------

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
        let bitset_bytes = 1u8; // ceil(n/8) for n ≤ 6
        let row_size_unpadded: u32 = 1 + field_types
            .iter()
            .map(|&ft| struct_field_size(StructFieldType::from_u8(ft).unwrap()))
            .sum::<u32>();
        let row_size = (row_size_unpadded + 3) & !3; // state_init pads rows to 4

        let mut state = mk_struct_map_state(16384, 16, &field_types, bitset_bytes, row_size as u16);
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
                smap.write_scalar_field(&mut state, up.pos, f, &cols, f, 0);
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
    /// every reported-Ok insert is observable, sizes match, absent keys read 0.
    #[test]
    fn prop_nested_set_matches_model(
        ops in prop::collection::vec((1u32..=40, 1u32..=300), 1..300),
    ) {
        use std::collections::{BTreeMap, BTreeSet};
        let mut state = mk_nested_state(262144, 64, SlotType::HashSet, 8, 1);
        let meta = meta_of(&state);
        let mut model: BTreeMap<u32, BTreeSet<u32>> = BTreeMap::new();

        for &(outer, elem) in &ops {
            if nested_set_insert(&mut state, &meta, outer, elem) == ErrorCode::Ok {
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
}
