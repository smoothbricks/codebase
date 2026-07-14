//! Translated test blocks from `packages/columine/src/vm/hash_table.zig`,
//! `hashmap_ops.zig`, and `hashset_ops.zig` (every `test "…"` block has a
//! counterpart here, same scenarios and expected values), plus iteration-
//! order pinning and proptest properties the Zig tree lacks.
//!
//! The two Zig stress tests that use `std.Random.DefaultPrng` are translated
//! with a local deterministic LCG instead (Zig's PRNG stream is not part of
//! the contract; the assertions — everything inserted is found, size matches
//! — are what the test pins).

use columine_types::types::{
    ChangeFlag, EMPTY_KEY, ErrorCode, STATE_HEADER_SIZE, SlotMetaOffset, TOMBSTONE, hash_key,
};
use columine_vm::hash_table::{
    ENTRY_NONE, ENTRY_U32, FlatTable, hashmap_byte_size, hashset_byte_size,
    timestamped_map_byte_size,
};
use columine_vm::hashmap_ops::{
    CmpType, Strategy, batch_map_remove, batch_map_upsert, bind_slot_map, single_map_remove,
    single_map_upsert,
};
use columine_vm::hashset_ops::{
    batch_set_insert, batch_set_remove, bind_slot_set, single_set_insert, single_set_remove,
};
use columine_vm::hooks::NoVm;
use columine_vm::meta::SlotMetaView;
use proptest::prelude::*;
// The `hashmap_ops::Strategy` ENUM above shadows the prelude's `Strategy`
// TRAIT (explicit import beats glob); re-import the trait under an alias so
// `.prop_map` resolves.
use proptest::strategy::Strategy as PropStrategy;

// ---------------------------------------------------------------------------
// Test scaffolding — mirrors the manual state construction in the Zig test
// blocks (hashmap_ops.zig:376-391): header, one 48-byte slot-meta record,
// slot data right after.
// ---------------------------------------------------------------------------

const SLOT_OFFSET: u32 = STATE_HEADER_SIZE + 48; // types.zig SLOT_META_SIZE

fn mk_slot_state(cap: u32, type_flags_byte: u8) -> Vec<u8> {
    let mut state = vec![0u8; 8192];
    let meta = STATE_HEADER_SIZE;
    state[meta as usize..meta as usize + 4].copy_from_slice(&SLOT_OFFSET.to_le_bytes());
    state[meta as usize + 4..meta as usize + 8].copy_from_slice(&cap.to_le_bytes());
    // size = 0 already; type_flags:
    state[(meta + SlotMetaOffset::TYPE_FLAGS) as usize] = type_flags_byte;
    // keys → EMPTY_KEY
    for j in 0..cap {
        let off = (SLOT_OFFSET + j * 4) as usize;
        state[off..off + 4].copy_from_slice(&EMPTY_KEY.to_le_bytes());
    }
    state
}

fn cmp_lane_fill(state: &mut [u8], cap: u32, bits: u64) {
    for j in 0..cap {
        let off = (SLOT_OFFSET + cap * 8 + j * 8) as usize;
        state[off..off + 8].copy_from_slice(&bits.to_le_bytes());
    }
}

fn meta_of(state: &[u8]) -> SlotMetaView {
    SlotMetaView::read(state, 0)
}

// ---------------------------------------------------------------------------
// hash_table.zig test blocks
// ---------------------------------------------------------------------------

/// hash_table.zig `test "HashSet — insert, contains, dedup"`
#[test]
fn hashset_insert_contains_dedup() {
    let mut buf = vec![0u8; 512];
    let tbl = FlatTable::init(&mut buf, 0, 16, ENTRY_NONE);

    assert_eq!(tbl.size(&buf), 0);
    assert_eq!(tbl.insert_key(&mut buf, 42), Some(true));
    assert_eq!(tbl.size(&buf), 1);
    assert!(tbl.contains(&buf, 42));
    assert!(!tbl.contains(&buf, 43));
    assert_eq!(tbl.insert_key(&mut buf, 42), Some(false));
    assert_eq!(tbl.size(&buf), 1);
    assert_eq!(tbl.insert_key(&mut buf, 100), Some(true));
    assert_eq!(tbl.insert_key(&mut buf, 200), Some(true));
    assert_eq!(tbl.size(&buf), 3);
}

/// hash_table.zig `test "HashMap — upsert, get, overwrite"`
#[test]
fn hashmap_upsert_get_overwrite() {
    let mut buf = vec![0u8; 1024];
    let tbl = FlatTable::init(&mut buf, 0, 16, ENTRY_U32);

    assert_eq!(tbl.upsert_u32(&mut buf, 10, 100), Some(true));
    assert_eq!(tbl.size(&buf), 1);
    assert_eq!(tbl.get_u32(&buf, 10), Some(100));
    assert_eq!(tbl.upsert_u32(&mut buf, 10, 200), Some(false));
    assert_eq!(tbl.get_u32(&buf, 10), Some(200));
    assert_eq!(tbl.get_u32(&buf, 99), None);
}

/// hash_table.zig `test "HashSet — load factor triggers null"`
#[test]
fn hashset_load_factor_triggers_none() {
    let mut buf = vec![0u8; 512];
    let tbl = FlatTable::init(&mut buf, 0, 16, ENTRY_NONE);

    // 70% of 16 = 11.2 → 11 fit, 12th refused.
    for i in 1..=11u32 {
        assert!(tbl.insert_key(&mut buf, i).is_some());
    }
    assert_eq!(tbl.size(&buf), 11);
    assert_eq!(tbl.insert_key(&mut buf, 12), None);
}

/// hash_table.zig `test "HashSet — rehash into larger table"`
#[test]
fn hashset_rehash_into_larger() {
    let mut buf = vec![0u8; 4096];
    let src = FlatTable::init(&mut buf, 0, 16, ENTRY_NONE);
    for i in 1..=10u32 {
        src.insert_key(&mut buf, i);
    }
    assert_eq!(src.size(&buf), 10);

    let dst = src.rehash_into(&mut buf, 512, 32);
    assert_eq!(dst.size(&buf), 10);
    for i in 1..=10u32 {
        assert!(dst.contains(&buf, i));
    }
    assert!(!dst.contains(&buf, 99));
}

/// hash_table.zig `test "HashMap — rehash preserves values"`
#[test]
fn hashmap_rehash_preserves_values() {
    let mut buf = vec![0u8; 4096];
    let src = FlatTable::init(&mut buf, 0, 16, ENTRY_U32);
    src.upsert_u32(&mut buf, 1, 100);
    src.upsert_u32(&mut buf, 2, 200);
    src.upsert_u32(&mut buf, 3, 300);

    let dst = src.rehash_into(&mut buf, 1024, 32);
    assert_eq!(dst.size(&buf), 3);
    assert_eq!(dst.get_u32(&buf, 1), Some(100));
    assert_eq!(dst.get_u32(&buf, 2), Some(200));
    assert_eq!(dst.get_u32(&buf, 3), Some(300));
}

/// hash_table.zig `test "FlatHashTable — bind to existing table"`
#[test]
fn bind_to_existing_table() {
    let mut buf = vec![0u8; 512];
    let created = FlatTable::init(&mut buf, 0, 16, ENTRY_NONE);
    created.insert_key(&mut buf, 42);
    created.insert_key(&mut buf, 99);

    let bound = FlatTable::bind(&buf, 0, ENTRY_NONE);
    assert_eq!(bound.size(&buf), 2);
    assert!(bound.contains(&buf, 42));
    assert!(bound.contains(&buf, 99));
}

/// hash_table.zig `test "FlatHashTable — byteSize calculation"`
#[test]
fn byte_size_calculation() {
    assert_eq!(hashset_byte_size(16), 72); // 8 + 16*4
    assert_eq!(hashmap_byte_size(16), 136); // 8 + 16*4 + 16*4
    assert_eq!(timestamped_map_byte_size(16), 328); // 8 + 16*4 + 16*16
}

/// hash_table.zig `test "EMPTY_KEY and TOMBSTONE are rejected"`
#[test]
fn sentinel_keys_rejected() {
    let mut buf = vec![0u8; 512];
    let tbl = FlatTable::init(&mut buf, 0, 16, ENTRY_NONE);
    assert_eq!(tbl.insert_key(&mut buf, EMPTY_KEY), None);
    assert_eq!(tbl.insert_key(&mut buf, TOMBSTONE), None);
    assert_eq!(tbl.size(&buf), 0);
}

/// hash_table.zig `test "stress — randomized insert 1000 keys, verify all
/// found"` (deterministic LCG replaces Zig's DefaultPrng; same assertions).
#[test]
fn stress_randomized_insert_1000_keys() {
    let mut buf = vec![0u8; 32768];
    let tbl = FlatTable::init(&mut buf, 0, 2048, ENTRY_NONE);

    let mut lcg: u64 = 0xDEAD_BEEF;
    let mut inserted = Vec::new();
    for _ in 0..1000 {
        lcg = lcg
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let key = 1 + ((lcg >> 33) as u32) % 100_000;
        if let Some(true) = tbl.insert_key(&mut buf, key) {
            inserted.push(key);
        }
    }
    for &key in &inserted {
        assert!(tbl.contains(&buf, key));
    }
    assert_eq!(tbl.size(&buf), inserted.len() as u32);
}

/// hash_table.zig `test "stress — fill to load factor then rehash preserves all"`
#[test]
fn stress_fill_then_rehash() {
    let mut buf = vec![0u8; 4096];
    let src = FlatTable::init(&mut buf, 0, 32, ENTRY_NONE);
    for i in 1..=22u32 {
        assert_eq!(src.insert_key(&mut buf, i), Some(true));
    }
    assert_eq!(src.size(&buf), 22);

    let dst = src.rehash_into(&mut buf, 1024, 64);
    assert_eq!(dst.size(&buf), 22);
    for i in 1..=22u32 {
        assert!(dst.contains(&buf, i));
    }
}

/// hash_table.zig `test "property — size always equals count of live keys"`
#[test]
fn size_equals_live_key_count() {
    let mut buf = vec![0u8; 8192];
    let tbl = FlatTable::init(&mut buf, 0, 128, ENTRY_U32);

    let mut count = 0u32;
    for i in 1..=80u32 {
        match tbl.upsert_u32(&mut buf, i, i * 10) {
            Some(true) => count += 1,
            Some(false) => {}
            None => break,
        }
    }
    assert_eq!(tbl.size(&buf), count);

    let live = tbl.iter_live(&buf).count() as u32;
    assert_eq!(tbl.size(&buf), live);
}

/// hash_table.zig `test "stress — HashMap upsert overwrites preserve size"`
#[test]
fn stress_upsert_overwrites_preserve_size() {
    let mut buf = vec![0u8; 4096];
    let tbl = FlatTable::init(&mut buf, 0, 128, ENTRY_U32);

    for i in 1..=50u32 {
        assert_eq!(tbl.upsert_u32(&mut buf, i, i * 100), Some(true));
    }
    assert_eq!(tbl.size(&buf), 50);
    for i in 1..=50u32 {
        assert_eq!(tbl.upsert_u32(&mut buf, i, i * 200), Some(false));
    }
    assert_eq!(tbl.size(&buf), 50);
    for i in 1..=50u32 {
        assert_eq!(tbl.get_u32(&buf, i), Some(i * 200));
    }
}

/// hash_table.zig `test "edge — max valid key (0xFFFFFFFD) works correctly"`
#[test]
fn edge_max_valid_key() {
    let mut buf = vec![0u8; 512];
    let tbl = FlatTable::init(&mut buf, 0, 16, ENTRY_NONE);
    assert_eq!(tbl.insert_key(&mut buf, 0xFFFF_FFFD), Some(true));
    assert!(tbl.contains(&buf, 0xFFFF_FFFD));
    assert_eq!(tbl.size(&buf), 1);
    assert!(!tbl.contains(&buf, EMPTY_KEY));
    assert!(!tbl.contains(&buf, TOMBSTONE));
}

/// hash_table.zig `test "edge — HashMap upsert with key near sentinels"`
#[test]
fn edge_upsert_near_sentinels() {
    let mut buf = vec![0u8; 1024];
    let tbl = FlatTable::init(&mut buf, 0, 16, ENTRY_U32);
    assert_eq!(tbl.upsert_u32(&mut buf, 0xFFFF_FFFD, 42), Some(true));
    assert_eq!(tbl.get_u32(&buf, 0xFFFF_FFFD), Some(42));
    assert_eq!(tbl.get_u32(&buf, EMPTY_KEY), None);
    assert_eq!(tbl.get_u32(&buf, TOMBSTONE), None);
}

// ---------------------------------------------------------------------------
// Iteration order — observable ABI (`vm_map_iter_*`). No Zig test pins this;
// the sequences below are hand-derived from `hash_key` (cited values) and the
// ascending-slot scan.
// ---------------------------------------------------------------------------

/// hash_key(·, 16): 1→7, 2→14, 3→15, 4→5, 5→13 (computed from the verified
/// `hash_key` port). No collisions → iteration is slot order 5,7,13,14,15 →
/// keys 4,1,5,2,3.
#[test]
fn iteration_order_pinned_no_collision() {
    let mut buf = vec![0u8; 512];
    let tbl = FlatTable::init(&mut buf, 0, 16, ENTRY_NONE);
    for k in [1u32, 2, 3, 4, 5] {
        assert_eq!(tbl.insert_key(&mut buf, k), Some(true));
    }
    let seq: Vec<(u32, u32)> = tbl.iter_live(&buf).collect();
    assert_eq!(seq, vec![(5, 4), (7, 1), (13, 5), (14, 2), (15, 3)]);
}

/// hash_key(·, 16): 20→1, 4→5, 30→5 (COLLIDES with 4; linear-probes to 6),
/// 1→7, 10→8. Insertion order 1,4,10,20,30 → iteration slots 1,5,6,7,8.
#[test]
fn iteration_order_pinned_with_collision() {
    let mut buf = vec![0u8; 512];
    let tbl = FlatTable::init(&mut buf, 0, 16, ENTRY_NONE);
    for k in [1u32, 4, 10, 20, 30] {
        assert_eq!(tbl.insert_key(&mut buf, k), Some(true));
    }
    let seq: Vec<(u32, u32)> = tbl.iter_live(&buf).collect();
    assert_eq!(seq, vec![(1, 20), (5, 4), (6, 30), (7, 1), (8, 10)]);
}

/// Tombstone reuse changes placement, and therefore iteration order, exactly
/// as in Zig: remove 4 (slot 5 becomes a tombstone), re-insert 30 — findInsert
/// finds 30 already at slot 6 (probes PAST the tombstone); insert 36 (hash 5)
/// reuses the tombstone slot.
#[test]
fn iteration_order_after_tombstone_reuse() {
    let mut buf = vec![0u8; 4096];
    let state_meta = {
        let mut s = mk_slot_state(16, 0x01); // HASHSET
        std::mem::swap(&mut buf, &mut s);
        meta_of(&buf)
    };
    let mut hooks = NoVm;
    assert_eq!(
        batch_set_insert(
            false,
            &mut buf,
            &state_meta,
            0,
            &[1, 4, 10, 20, 30],
            None,
            &mut hooks
        ),
        ErrorCode::Ok
    );
    batch_set_remove(false, &mut buf, &state_meta, 0, &[4], &mut hooks);

    let tbl = bind_slot_set(&state_meta);
    // 30 still found via probe past the tombstone at its home slot 5.
    assert!(tbl.contains(&buf, 30));
    assert_eq!(tbl.find(&buf, 30), Some(6));

    // hash_key(50,16) == 5 (computed): the tombstone slot is reused.
    assert_eq!(hash_key(50, 16), 5);
    assert_eq!(
        batch_set_insert(false, &mut buf, &state_meta, 0, &[50], None, &mut hooks),
        ErrorCode::Ok
    );
    let seq: Vec<(u32, u32)> = tbl.iter_live(&buf).collect();
    assert_eq!(seq, vec![(1, 20), (5, 50), (6, 30), (7, 1), (8, 10)]);
}

// ---------------------------------------------------------------------------
// hashmap_ops.zig test blocks
// ---------------------------------------------------------------------------

/// hashmap_ops.zig `test "batchMapUpsert — last strategy inserts and overwrites"`
#[test]
fn batch_upsert_last_inserts_and_overwrites() {
    let mut state = mk_slot_state(16, 0x00); // HASHMAP with timestamps
    let meta = meta_of(&state);
    let mut hooks = NoVm;

    let r = batch_map_upsert(
        Strategy::Last,
        false,
        &mut state,
        &meta,
        0,
        &[10, 20, 30],
        &[100, 200, 300],
        None,
        CmpType::F64,
        &mut hooks,
    );
    assert_eq!(r, ErrorCode::Ok);
    assert_eq!(meta.size(&state), 3);

    let tbl = bind_slot_map(&meta);
    assert_eq!(tbl.get_u32(&state, 10), Some(100));
    assert_eq!(tbl.get_u32(&state, 20), Some(200));
    assert_eq!(tbl.get_u32(&state, 30), Some(300));

    let r = batch_map_upsert(
        Strategy::Last,
        false,
        &mut state,
        &meta,
        0,
        &[20],
        &[999],
        None,
        CmpType::F64,
        &mut hooks,
    );
    assert_eq!(r, ErrorCode::Ok);
    assert_eq!(tbl.get_u32(&state, 20), Some(999));
    assert_eq!(meta.size(&state), 3); // overwrite — size unchanged
}

/// hashmap_ops.zig `test "batchMapUpsert — first strategy skips existing"`
#[test]
fn batch_upsert_first_skips_existing() {
    let mut state = mk_slot_state(16, 0x00);
    let meta = meta_of(&state);
    let mut hooks = NoVm;
    let tbl = bind_slot_map(&meta);

    batch_map_upsert(
        Strategy::First,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[100],
        None,
        CmpType::F64,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(100));

    batch_map_upsert(
        Strategy::First,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[999],
        None,
        CmpType::F64,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(100)); // still 100
}

fn f64_col(vals: &[f64]) -> Vec<u8> {
    vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

fn i64_col(vals: &[i64]) -> Vec<u8> {
    vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

fn u32_col(vals: &[u32]) -> Vec<u8> {
    vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

/// hashmap_ops.zig `test "batchMapUpsert — latest strategy uses timestamp
/// comparison (f64)"`
#[test]
fn batch_upsert_latest_f64() {
    let mut state = mk_slot_state(16, 0x00);
    cmp_lane_fill(&mut state, 16, f64::NEG_INFINITY.to_bits());
    let meta = meta_of(&state);
    let mut hooks = NoVm;
    let tbl = bind_slot_map(&meta);

    let col = f64_col(&[100.0]);
    batch_map_upsert(
        Strategy::Latest,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[100],
        Some(&col),
        CmpType::F64,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(100));

    // Older timestamp — keep original.
    let col = f64_col(&[50.0]);
    batch_map_upsert(
        Strategy::Latest,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[999],
        Some(&col),
        CmpType::F64,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(100));

    // Newer — update.
    let col = f64_col(&[200.0]);
    batch_map_upsert(
        Strategy::Latest,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[200],
        Some(&col),
        CmpType::F64,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(200));
}

/// hashmap_ops.zig `test "batchMapRemove — removes and tombstones"`
#[test]
fn batch_remove_tombstones() {
    let mut state = mk_slot_state(16, 0x00);
    let meta = meta_of(&state);
    let mut hooks = NoVm;
    let tbl = bind_slot_map(&meta);

    batch_map_upsert(
        Strategy::Last,
        false,
        &mut state,
        &meta,
        0,
        &[10, 20, 30],
        &[100, 200, 300],
        None,
        CmpType::F64,
        &mut hooks,
    );
    assert_eq!(tbl.size(&state), 3);

    batch_map_remove(false, &mut state, &meta, 0, &[20], &mut hooks);
    assert_eq!(tbl.size(&state), 2);
    assert_eq!(tbl.get_u32(&state, 20), None);
    assert!(tbl.get_u32(&state, 10).is_some());
    assert!(tbl.get_u32(&state, 30).is_some());
    assert_ne!(meta.change_flags(&state) & ChangeFlag::REMOVED, 0);
}

/// hashmap_ops.zig `test "batchMapUpsert — CAPACITY_EXCEEDED at 70% load"`
#[test]
fn batch_upsert_capacity_exceeded() {
    let mut state = mk_slot_state(16, 0x40); // HASHMAP, no_timestamps
    let meta = meta_of(&state);
    let mut hooks = NoVm;

    for i in 1..=11u32 {
        let r = batch_map_upsert(
            Strategy::Last,
            false,
            &mut state,
            &meta,
            0,
            &[i],
            &[i * 10],
            None,
            CmpType::F64,
            &mut hooks,
        );
        assert_eq!(r, ErrorCode::Ok);
    }
    assert_eq!(meta.size(&state), 11);

    let r = batch_map_upsert(
        Strategy::Last,
        false,
        &mut state,
        &meta,
        0,
        &[12],
        &[120],
        None,
        CmpType::F64,
        &mut hooks,
    );
    assert_eq!(r, ErrorCode::CapacityExceeded);
}

/// hashmap_ops.zig `test "batchMapUpsert — max strategy with u32 comparison"`
#[test]
fn batch_upsert_max_u32() {
    let mut state = mk_slot_state(16, 0x00);
    cmp_lane_fill(&mut state, 16, 0);
    let meta = meta_of(&state);
    let mut hooks = NoVm;
    let tbl = bind_slot_map(&meta);

    let col = u32_col(&[100]);
    batch_map_upsert(
        Strategy::Max,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[42],
        Some(&col),
        CmpType::U32,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(42));

    let col = u32_col(&[50]);
    batch_map_upsert(
        Strategy::Max,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[99],
        Some(&col),
        CmpType::U32,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(42));

    let col = u32_col(&[200]);
    batch_map_upsert(
        Strategy::Max,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[77],
        Some(&col),
        CmpType::U32,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(77));

    // Stored cmp value is the u32 zero-extended to u64.
    let pos = tbl.find(&state, 10).unwrap();
    let off = (SLOT_OFFSET + 16 * 8 + pos * 8) as usize;
    let stored = u64::from_le_bytes(state[off..off + 8].try_into().unwrap());
    assert_eq!(stored, 200);
}

/// hashmap_ops.zig `test "batchMapUpsert — latest strategy with i64 comparison"`
#[test]
fn batch_upsert_latest_i64() {
    let mut state = mk_slot_state(16, 0x00);
    cmp_lane_fill(&mut state, 16, i64::MIN as u64);
    let meta = meta_of(&state);
    let mut hooks = NoVm;
    let tbl = bind_slot_map(&meta);

    let col = i64_col(&[1000]);
    batch_map_upsert(
        Strategy::Latest,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[42],
        Some(&col),
        CmpType::I64,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(42));

    let col = i64_col(&[500]);
    batch_map_upsert(
        Strategy::Latest,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[99],
        Some(&col),
        CmpType::I64,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(42));

    let col = i64_col(&[2000]);
    batch_map_upsert(
        Strategy::Latest,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[77],
        Some(&col),
        CmpType::I64,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(77));

    let pos = tbl.find(&state, 10).unwrap();
    let off = (SLOT_OFFSET + 16 * 8 + pos * 8) as usize;
    let stored = i64::from_le_bytes(state[off..off + 8].try_into().unwrap());
    assert_eq!(stored, 2000);
}

/// hashmap_ops.zig `test "batchMapUpsert — min strategy with i64 comparison
/// (negative values)"`
#[test]
fn batch_upsert_min_i64_negative() {
    let mut state = mk_slot_state(16, 0x00);
    cmp_lane_fill(&mut state, 16, i64::MAX as u64);
    let meta = meta_of(&state);
    let mut hooks = NoVm;
    let tbl = bind_slot_map(&meta);

    let col = i64_col(&[-100]);
    batch_map_upsert(
        Strategy::Min,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[42],
        Some(&col),
        CmpType::I64,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(42));

    let col = i64_col(&[50]);
    batch_map_upsert(
        Strategy::Min,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[99],
        Some(&col),
        CmpType::I64,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(42));

    let col = i64_col(&[-200]);
    batch_map_upsert(
        Strategy::Min,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[77],
        Some(&col),
        CmpType::I64,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(77));

    let pos = tbl.find(&state, 10).unwrap();
    let off = (SLOT_OFFSET + 16 * 8 + pos * 8) as usize;
    let stored = i64::from_le_bytes(state[off..off + 8].try_into().unwrap());
    assert_eq!(stored, -200);
}

/// hashmap_ops.zig `test "singleMapUpsert — u32 comparison mode"`
#[test]
fn single_upsert_u32_mode() {
    let mut state = mk_slot_state(16, 0x00);
    cmp_lane_fill(&mut state, 16, 0);
    let meta = meta_of(&state);
    let mut hooks = NoVm;
    let tbl = bind_slot_map(&meta);

    assert_eq!(
        single_map_upsert(
            Strategy::Max,
            false,
            &mut state,
            &meta,
            0,
            5,
            42,
            10,
            CmpType::U32,
            &mut hooks
        ),
        ErrorCode::Ok
    );
    assert_eq!(tbl.get_u32(&state, 5), Some(42));

    assert_eq!(
        single_map_upsert(
            Strategy::Max,
            false,
            &mut state,
            &meta,
            0,
            5,
            99,
            5,
            CmpType::U32,
            &mut hooks
        ),
        ErrorCode::Ok
    );
    assert_eq!(tbl.get_u32(&state, 5), Some(42));

    assert_eq!(
        single_map_upsert(
            Strategy::Max,
            false,
            &mut state,
            &meta,
            0,
            5,
            77,
            20,
            CmpType::U32,
            &mut hooks
        ),
        ErrorCode::Ok
    );
    assert_eq!(tbl.get_u32(&state, 5), Some(77));
}

/// hashmap_ops.zig `test "singleMapUpsert — i64 comparison mode"`
#[test]
fn single_upsert_i64_mode() {
    let mut state = mk_slot_state(16, 0x00);
    cmp_lane_fill(&mut state, 16, i64::MIN as u64);
    let meta = meta_of(&state);
    let mut hooks = NoVm;
    let tbl = bind_slot_map(&meta);

    assert_eq!(
        single_map_upsert(
            Strategy::Latest,
            false,
            &mut state,
            &meta,
            0,
            5,
            42,
            (-50i64) as u64,
            CmpType::I64,
            &mut hooks
        ),
        ErrorCode::Ok
    );
    assert_eq!(tbl.get_u32(&state, 5), Some(42));

    assert_eq!(
        single_map_upsert(
            Strategy::Latest,
            false,
            &mut state,
            &meta,
            0,
            5,
            99,
            (-100i64) as u64,
            CmpType::I64,
            &mut hooks
        ),
        ErrorCode::Ok
    );
    assert_eq!(tbl.get_u32(&state, 5), Some(42));

    assert_eq!(
        single_map_upsert(
            Strategy::Latest,
            false,
            &mut state,
            &meta,
            0,
            5,
            77,
            100u64,
            CmpType::I64,
            &mut hooks
        ),
        ErrorCode::Ok
    );
    assert_eq!(tbl.get_u32(&state, 5), Some(77));
}

/// Zig-parity extra: singleMapRemove mirrors batch remove (no dedicated Zig
/// block; covered by vm_test.zig FOR_EACH paths — pinned here directly).
#[test]
fn single_remove_tombstones() {
    let mut state = mk_slot_state(16, 0x40);
    let meta = meta_of(&state);
    let mut hooks = NoVm;
    let tbl = bind_slot_map(&meta);

    single_map_upsert(
        Strategy::Last,
        false,
        &mut state,
        &meta,
        0,
        7,
        70,
        0,
        CmpType::F64,
        &mut hooks,
    );
    assert_eq!(tbl.size(&state), 1);
    single_map_remove(false, &mut state, &meta, 0, 7, &mut hooks);
    assert_eq!(tbl.size(&state), 0);
    assert_eq!(tbl.get_u32(&state, 7), None);
    assert_ne!(meta.change_flags(&state) & ChangeFlag::REMOVED, 0);
}

/// Latest/max/min on a no-timestamp slot is INVALID_PROGRAM
/// (hashmap_ops.zig:139) — pinned because it is an easy regression.
#[test]
fn latest_without_timestamp_storage_is_invalid_program() {
    let mut state = mk_slot_state(16, 0x40); // no_hashmap_timestamps
    let meta = meta_of(&state);
    let mut hooks = NoVm;
    let col = f64_col(&[1.0]);
    let r = batch_map_upsert(
        Strategy::Latest,
        false,
        &mut state,
        &meta,
        0,
        &[1],
        &[1],
        Some(&col),
        CmpType::F64,
        &mut hooks,
    );
    assert_eq!(r, ErrorCode::InvalidProgram);
}

// ---------------------------------------------------------------------------
// hashset_ops.zig test blocks
// ---------------------------------------------------------------------------

/// hashset_ops.zig `test "batchSetInsert — insert and dedup"`
#[test]
fn batch_set_insert_and_dedup() {
    let mut state = mk_slot_state(16, 0x01); // HASHSET
    let meta = meta_of(&state);
    let mut hooks = NoVm;
    let tbl = bind_slot_set(&meta);

    let r = batch_set_insert(
        false,
        &mut state,
        &meta,
        0,
        &[10, 20, 10, 30, 20],
        None,
        &mut hooks,
    );
    assert_eq!(r, ErrorCode::Ok);
    assert_eq!(tbl.size(&state), 3);
    assert!(tbl.contains(&state, 10));
    assert!(tbl.contains(&state, 20));
    assert!(tbl.contains(&state, 30));
    assert!(!tbl.contains(&state, 40));
    assert_ne!(meta.change_flags(&state) & ChangeFlag::INSERTED, 0);
}

/// hashset_ops.zig `test "batchSetRemove — remove existing"`
#[test]
fn batch_set_remove_existing() {
    let mut state = mk_slot_state(16, 0x01);
    let meta = meta_of(&state);
    let mut hooks = NoVm;
    let tbl = bind_slot_set(&meta);

    batch_set_insert(false, &mut state, &meta, 0, &[10, 20, 30], None, &mut hooks);
    assert_eq!(tbl.size(&state), 3);

    batch_set_remove(false, &mut state, &meta, 0, &[20], &mut hooks);
    assert_eq!(tbl.size(&state), 2);
    assert!(!tbl.contains(&state, 20));
    assert!(tbl.contains(&state, 10));
    assert!(tbl.contains(&state, 30));
}

/// hashset_ops.zig `test "singleSetInsert — CAPACITY_EXCEEDED at load factor"`
#[test]
fn single_set_insert_capacity_exceeded() {
    let mut state = mk_slot_state(16, 0x01);
    let meta = meta_of(&state);
    let mut hooks = NoVm;

    for i in 1..=11u32 {
        assert_eq!(
            single_set_insert(false, &mut state, &meta, 0, i, 0.0, &mut hooks),
            ErrorCode::Ok
        );
    }
    assert_eq!(meta.size(&state), 11);
    assert_eq!(
        single_set_insert(false, &mut state, &meta, 0, 12, 0.0, &mut hooks),
        ErrorCode::CapacityExceeded
    );
}

/// Zig-parity extra: singleSetRemove (FOR_EACH body path).
#[test]
fn single_set_remove_works() {
    let mut state = mk_slot_state(16, 0x01);
    let meta = meta_of(&state);
    let mut hooks = NoVm;
    let tbl = bind_slot_set(&meta);

    single_set_insert(false, &mut state, &meta, 0, 42, 0.0, &mut hooks);
    assert_eq!(tbl.size(&state), 1);
    single_set_remove(false, &mut state, &meta, 0, 42, &mut hooks);
    assert_eq!(tbl.size(&state), 0);
    assert!(!tbl.contains(&state, 42));
}

// ---------------------------------------------------------------------------
// Proptests — reference-model differential. The model re-derives placement
// from the Zig algorithm's SPEC (home slot = hash_key, +1 linear probe,
// first-tombstone reuse, 70% load) independently of `FlatTable`'s code, and
// the comparison is on the FULL keys-region bytes, so any divergence in probe
// order, wraparound, or sentinel handling fails loudly.
// ---------------------------------------------------------------------------

struct RefModel {
    cells: Vec<u32>, // EMPTY_KEY / TOMBSTONE / live key
    size: u32,
}

impl RefModel {
    fn new(cap: u32) -> Self {
        Self {
            cells: vec![EMPTY_KEY; cap as usize],
            size: 0,
        }
    }

    fn cap(&self) -> u32 {
        self.cells.len() as u32
    }

    fn max_load(&self) -> u32 {
        self.cap() * 7 / 10
    }

    fn find_insert(&self, key: u32) -> Option<(u32, bool)> {
        if key == EMPTY_KEY || key == TOMBSTONE {
            return None;
        }
        let cap = self.cap();
        let mut pos = hash_key(key, cap);
        let mut first_tomb = None;
        for _ in 0..cap {
            let k = self.cells[pos as usize];
            if k == key {
                return Some((pos, true));
            }
            if k == EMPTY_KEY {
                return Some((first_tomb.unwrap_or(pos), false));
            }
            if k == TOMBSTONE && first_tomb.is_none() {
                first_tomb = Some(pos);
            }
            pos = (pos + 1) & (cap - 1);
        }
        first_tomb.map(|t| (t, false))
    }

    fn insert(&mut self, key: u32) {
        if let Some((pos, found)) = self.find_insert(key)
            && !found
            && self.size < self.max_load()
        {
            self.cells[pos as usize] = key;
            self.size += 1;
        }
    }

    fn remove(&mut self, key: u32) {
        if key == EMPTY_KEY || key == TOMBSTONE {
            return;
        }
        let cap = self.cap();
        let mut pos = hash_key(key, cap);
        for _ in 0..cap {
            let k = self.cells[pos as usize];
            if k == key {
                self.cells[pos as usize] = TOMBSTONE;
                self.size -= 1;
                return;
            }
            if k == EMPTY_KEY {
                return;
            }
            pos = (pos + 1) & (cap - 1);
        }
    }

    fn live_seq(&self) -> Vec<(u32, u32)> {
        self.cells
            .iter()
            .enumerate()
            .filter(|&(_, &k)| k != EMPTY_KEY && k != TOMBSTONE)
            .map(|(i, &k)| (i as u32, k))
            .collect()
    }
}

#[derive(Clone, Debug)]
enum Op {
    Insert(u32),
    Remove(u32),
}

fn op_strategy() -> impl proptest::strategy::Strategy<Value = Op> {
    // Small key range forces collisions, tombstone reuse, and probe chains.
    let keys: std::ops::Range<u32> = 1..40;
    prop_oneof![keys.clone().prop_map(Op::Insert), keys.prop_map(Op::Remove),]
}

proptest! {
    /// Arbitrary insert/remove interleavings: the keys-region BYTES equal the
    /// reference model cell-for-cell, size matches, and the live-key
    /// iteration sequence (the `vm_map_iter_*` order) matches exactly.
    #[test]
    fn set_ops_match_reference_model(ops in proptest::collection::vec(op_strategy(), 0..200)) {
        let cap = 16u32;
        let mut state = mk_slot_state(cap, 0x01); // HASHSET
        let meta = meta_of(&state);
        let mut hooks = NoVm;
        let mut model = RefModel::new(cap);

        for op in &ops {
            match *op {
                Op::Insert(k) => {
                    let _ = batch_set_insert(false, &mut state, &meta, 0, &[k], None, &mut hooks);
                    model.insert(k);
                }
                Op::Remove(k) => {
                    batch_set_remove(false, &mut state, &meta, 0, &[k], &mut hooks);
                    model.remove(k);
                }
            }
        }

        let tbl = bind_slot_set(&meta);
        prop_assert_eq!(tbl.size(&state), model.size);
        for pos in 0..cap {
            prop_assert_eq!(tbl.key_at(&state, pos), model.cells[pos as usize],
                "cell {} diverged", pos);
        }
        let seq: Vec<(u32, u32)> = tbl.iter_live(&state).collect();
        prop_assert_eq!(seq, model.live_seq());
    }

    /// Map contents differential vs std::HashMap under Last strategy, plus
    /// model byte-parity of the keys region.
    #[test]
    fn map_last_matches_std_hashmap(ops in proptest::collection::vec(op_strategy(), 0..200)) {
        let cap = 32u32;
        let mut state = mk_slot_state(cap, 0x40); // HASHMAP, no timestamps
        let meta = meta_of(&state);
        let mut hooks = NoVm;
        let mut model = RefModel::new(cap);
        let mut oracle: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();

        for (n, op) in ops.iter().enumerate() {
            match *op {
                Op::Insert(k) => {
                    let v = n as u32 * 7 + 1;
                    let before = meta.size(&state);
                    let r = batch_map_upsert(
                        Strategy::Last, false, &mut state, &meta, 0,
                        &[k], &[v], None, CmpType::F64, &mut hooks,
                    );
                    model.insert(k);
                    // Oracle updates only when the table accepted the write:
                    // overwrite always lands; a NEW key lands unless refused
                    // by the load factor (CapacityExceeded).
                    if r == ErrorCode::Ok
                        && (oracle.contains_key(&k) || meta.size(&state) > before)
                    {
                        oracle.insert(k, v);
                    }
                }
                Op::Remove(k) => {
                    batch_map_remove(false, &mut state, &meta, 0, &[k], &mut hooks);
                    model.remove(k);
                    oracle.remove(&k);
                }
            }
        }

        let tbl = bind_slot_map(&meta);
        prop_assert_eq!(tbl.size(&state), oracle.len() as u32);
        for (&k, &v) in &oracle {
            prop_assert_eq!(tbl.get_u32(&state, k), Some(v));
        }
        for pos in 0..cap {
            prop_assert_eq!(tbl.key_at(&state, pos), model.cells[pos as usize]);
        }
    }

    /// Rehash preserves the full content set and places every key by the
    /// plain (tombstone-free) probe in ascending-source-slot order.
    #[test]
    fn rehash_matches_reference_placement(keys in proptest::collection::btree_set(1u32..1000, 0..20)) {
        let mut buf = vec![0u8; 8192];
        let src = FlatTable::init(&mut buf, 0, 32, ENTRY_U32);
        let mut live: Vec<(u32, u32)> = Vec::new();
        for &k in &keys {
            let _ = src.upsert_u32(&mut buf, k, k * 3);
        }
        for (_, k) in src.iter_live(&buf) {
            live.push((k, src.get_u32(&buf, k).unwrap()));
        }

        let new_cap = 64u32;
        let dst = src.rehash_into(&mut buf, 4096, new_cap);

        // Reference placement: iterate source slots ascending, plain probe.
        let mut ref_cells = vec![EMPTY_KEY; new_cap as usize];
        for &(k, _) in &live {
            let mut pos = hash_key(k, new_cap);
            while ref_cells[pos as usize] != EMPTY_KEY {
                pos = (pos + 1) & (new_cap - 1);
            }
            ref_cells[pos as usize] = k;
        }

        prop_assert_eq!(dst.size(&buf), live.len() as u32);
        for pos in 0..new_cap {
            prop_assert_eq!(dst.key_at(&buf, pos), ref_cells[pos as usize]);
        }
        for (k, v) in live {
            prop_assert_eq!(dst.get_u32(&buf, k), Some(v));
        }
    }
}
