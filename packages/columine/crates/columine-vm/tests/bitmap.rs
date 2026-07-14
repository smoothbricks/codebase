//! Translated `test "…"` blocks from `packages/columine/src/vm/bitmap_ops.zig`
//! (15/15), RoaringFormatSpec read-compat fixtures, and the slice-4
//! boundary-capacity proptests.
//!
//! The Zig algebra tests (blocks 12-15) call rawr's `RoaringBitmap` directly
//! because the wasm exports take linear-memory pointers; the ports go through
//! `set_algebra`, which exercises the same operations through the public
//! serialized surface — strictly more of the ported code.

use columine_vm::bitmap_ops::{
    BitmapAlgebraOp, BitmapEnv, BitmapStorage, batch_bitmap_add, batch_bitmap_algebra,
    batch_bitmap_remove, bitmap_load, bitmap_payload_capacity, bitmap_select, bitmap_store,
    cardinality_serialized, contains_serialized, extract_serialized, get_bitmap_storage,
    intersect_count_serialized, intersects_serialized, set_algebra,
};
use columine_vm::hooks::NoVm;
use columine_vm::meta::SlotMetaView;
use columine_vm::minroar::MiniRoaring as RoaringBitmap;
use proptest::prelude::*;

use columine_types::types::{
    BITMAP_BASE_BYTES, BITMAP_BYTES_PER_CAPACITY, BITMAP_SERIALIZED_LEN_BYTES, ErrorCode,
    SLOT_META_SIZE, STATE_FORMAT_VERSION, STATE_HEADER_SIZE, STATE_MAGIC, SlotMetaOffset, SlotType,
    StateHeaderOffset,
};

/// bitmap_ops.zig:905 `makeStorage` — a storage view over a local buffer:
/// `[serialized_len u32][payload…]` at offset 0.
fn make_storage(buf_len: u32) -> BitmapStorage {
    BitmapStorage {
        data_offset: 0,
        payload_capacity: buf_len - BITMAP_SERIALIZED_LEN_BYTES,
    }
}

/// bitmap_ops.zig:915 `initBitmapSlotState` — minimal state with one BITMAP
/// slot: `[STATE_HEADER (32)][SLOT_META (48)][bitmap data …]`.
fn init_bitmap_slot_state(state: &mut [u8], capacity: u32) -> SlotMetaView {
    state.fill(0);
    let hdr = STATE_MAGIC.to_le_bytes();
    state[StateHeaderOffset::MAGIC as usize..StateHeaderOffset::MAGIC as usize + 4]
        .copy_from_slice(&hdr);
    state[StateHeaderOffset::FORMAT_VERSION as usize] = STATE_FORMAT_VERSION;
    state[StateHeaderOffset::NUM_SLOTS as usize] = 1;

    let meta_base = STATE_HEADER_SIZE;
    let slot_data_offset = STATE_HEADER_SIZE + SLOT_META_SIZE;
    let w32 = |s: &mut [u8], off: u32, v: u32| {
        s[off as usize..off as usize + 4].copy_from_slice(&v.to_le_bytes());
    };
    w32(state, meta_base + SlotMetaOffset::OFFSET, slot_data_offset);
    w32(state, meta_base + SlotMetaOffset::CAPACITY, capacity);
    w32(state, meta_base + SlotMetaOffset::SIZE, 0);
    state[(meta_base + SlotMetaOffset::TYPE_FLAGS) as usize] = SlotType::Bitmap as u8;

    SlotMetaView::read(state, 0)
}

// ---------------------------------------------------------------------------
// 1. Empty bitmap load (bitmap_ops.zig:939)
// ---------------------------------------------------------------------------
#[test]
fn bitmap_load_empty_returns_cardinality_0() {
    let mut env = BitmapEnv::default();
    let buf = vec![0u8; 4096];
    let storage = make_storage(4096);
    let loaded = bitmap_load(&mut env, &buf, storage).expect("empty load");
    assert_eq!(loaded.len(), 0);
}

// ---------------------------------------------------------------------------
// 2. Serialize round-trip, 10 elements (bitmap_ops.zig:953)
// ---------------------------------------------------------------------------
#[test]
fn store_load_round_trip_preserves_10_elements() {
    let mut env = BitmapEnv::default();
    let mut buf = vec![0u8; 8192];
    let storage = make_storage(8192);

    let mut bmp = bitmap_load(&mut env, &buf, storage).unwrap();
    let elems = [5u32, 10, 15, 20, 25, 30, 35, 40, 45, 50];
    for &e in &elems {
        bmp.insert(e);
    }
    assert_eq!(
        bitmap_store(&mut env, &mut buf, storage, &mut bmp),
        ErrorCode::Ok
    );

    let reloaded = bitmap_load(&mut env, &buf, storage).unwrap();
    assert_eq!(reloaded.len(), 10);
    for &e in &elems {
        assert!(reloaded.contains(e));
    }
    assert!(!reloaded.contains(99));
}

// ---------------------------------------------------------------------------
// 3. Large bitmap, 500 elements (bitmap_ops.zig:981)
// ---------------------------------------------------------------------------
#[test]
fn store_load_500_elements_round_trip() {
    let mut env = BitmapEnv::default();
    let mut buf = vec![0u8; 65536];
    let storage = make_storage(65536);

    let mut bmp = bitmap_load(&mut env, &buf, storage).unwrap();
    for i in 1..=500u32 {
        bmp.insert(i);
    }
    assert_eq!(
        bitmap_store(&mut env, &mut buf, storage, &mut bmp),
        ErrorCode::Ok
    );

    let reloaded = bitmap_load(&mut env, &buf, storage).unwrap();
    assert_eq!(reloaded.len(), 500);
    assert!(reloaded.contains(1));
    assert!(reloaded.contains(250));
    assert!(reloaded.contains(500));
    assert!(!reloaded.contains(0));
    assert!(!reloaded.contains(501));
}

// ---------------------------------------------------------------------------
// 4-5. serialized_data on empty / iterates stored elements
// (bitmap_ops.zig:1007 bitmapFrozen tests — the roaring crate has no frozen
// view; `serialized_data` + deserialize is the ported query path)
// ---------------------------------------------------------------------------
#[test]
fn serialized_data_none_when_empty() {
    let buf = vec![0u8; 4096];
    let storage = make_storage(4096);
    assert!(storage.serialized_data(&buf).is_none());
}

#[test]
fn serialized_data_iterates_stored_elements() {
    let mut env = BitmapEnv::default();
    let mut buf = vec![0u8; 8192];
    let storage = make_storage(8192);

    let mut bmp = RoaringBitmap::new();
    bmp.insert(7);
    bmp.insert(42);
    bmp.insert(100);
    assert_eq!(
        bitmap_store(&mut env, &mut buf, storage, &mut bmp),
        ErrorCode::Ok
    );

    let data = storage.serialized_data(&buf).expect("non-empty");
    let reloaded = RoaringBitmap::deserialize_from(data).unwrap();
    let collected: Vec<u32> = reloaded.iter().collect();
    assert_eq!(collected, vec![7, 42, 100]);
}

// ---------------------------------------------------------------------------
// 6. bitmap_select (bitmap_ops.zig:1045)
// ---------------------------------------------------------------------------
#[test]
fn bitmap_select_returns_element_at_rank() {
    let mut env = BitmapEnv::default();
    let mut buf = vec![0u8; 8192];
    let storage = make_storage(8192);

    let mut bmp = RoaringBitmap::new();
    for e in [10u32, 20, 30, 40, 50] {
        bmp.insert(e);
    }
    assert_eq!(
        bitmap_store(&mut env, &mut buf, storage, &mut bmp),
        ErrorCode::Ok
    );

    assert_eq!(bitmap_select(&buf, storage, 0), Some(10));
    assert_eq!(bitmap_select(&buf, storage, 2), Some(30));
    assert_eq!(bitmap_select(&buf, storage, 4), Some(50));
    assert_eq!(bitmap_select(&buf, storage, 5), None);
}

// ---------------------------------------------------------------------------
// 7. payload-capacity formula (bitmap_ops.zig:1067)
// ---------------------------------------------------------------------------
#[test]
fn bitmap_payload_capacity_formula() {
    assert_eq!(bitmap_payload_capacity(0), BITMAP_BASE_BYTES);
    assert_eq!(
        bitmap_payload_capacity(1),
        BITMAP_BYTES_PER_CAPACITY + BITMAP_BASE_BYTES
    );
    assert_eq!(
        bitmap_payload_capacity(16),
        16 * BITMAP_BYTES_PER_CAPACITY + BITMAP_BASE_BYTES
    );
    assert_eq!(
        bitmap_payload_capacity(1000),
        1000 * BITMAP_BYTES_PER_CAPACITY + BITMAP_BASE_BYTES
    );
}

// ---------------------------------------------------------------------------
// 8. get_bitmap_storage layout (bitmap_ops.zig:1081)
// ---------------------------------------------------------------------------
#[test]
fn get_bitmap_storage_returns_correct_offsets_and_capacity() {
    let mut state = vec![0u8; 8192];
    let capacity = 64u32;
    let meta = init_bitmap_slot_state(&mut state, capacity);

    let storage = get_bitmap_storage(&meta);
    let slot_data_offset = STATE_HEADER_SIZE + SLOT_META_SIZE;
    assert_eq!(storage.data_offset, slot_data_offset);
    assert_eq!(
        storage.payload_offset(),
        slot_data_offset + BITMAP_SERIALIZED_LEN_BYTES
    );
    assert_eq!(storage.payload_capacity, bitmap_payload_capacity(capacity));
}

// ---------------------------------------------------------------------------
// 9. batch add — 5 elements (bitmap_ops.zig:1104)
// ---------------------------------------------------------------------------
#[test]
fn batch_bitmap_add_inserts_5_elements() {
    let mut env = BitmapEnv::default();
    let mut state = vec![0u8; 65536];
    let meta = init_bitmap_slot_state(&mut state, 128);
    let storage = get_bitmap_storage(&meta);

    let elems = [100u32, 200, 300, 400, 500];
    let result = batch_bitmap_add(
        &mut env, &mut NoVm, false, &mut state, &meta, 0, &elems, None,
    );
    assert_eq!(result, ErrorCode::Ok);
    assert_eq!(meta.size(&state), 5);

    let data = storage.serialized_data(&state).expect("stored");
    let bm = RoaringBitmap::deserialize_from(data).unwrap();
    assert_eq!(bm.len(), 5);
    assert!(bm.contains(100));
    assert!(bm.contains(300));
    assert!(bm.contains(500));
}

// ---------------------------------------------------------------------------
// 10. batch add — dedup (bitmap_ops.zig:1126)
// ---------------------------------------------------------------------------
#[test]
fn batch_bitmap_add_dedups_same_element() {
    let mut env = BitmapEnv::default();
    let mut state = vec![0u8; 65536];
    let meta = init_bitmap_slot_state(&mut state, 128);

    let elems = [42u32, 42];
    let result = batch_bitmap_add(
        &mut env, &mut NoVm, false, &mut state, &meta, 0, &elems, None,
    );
    assert_eq!(result, ErrorCode::Ok);
    assert_eq!(meta.size(&state), 1);
}

// ---------------------------------------------------------------------------
// 11. batch remove (bitmap_ops.zig:1140)
// ---------------------------------------------------------------------------
#[test]
fn batch_bitmap_remove_removes_elements_correctly() {
    let mut env = BitmapEnv::default();
    let mut state = vec![0u8; 65536];
    let meta = init_bitmap_slot_state(&mut state, 128);
    let storage = get_bitmap_storage(&meta);

    let add = [10u32, 20, 30, 40, 50];
    batch_bitmap_add(&mut env, &mut NoVm, false, &mut state, &meta, 0, &add, None);
    assert_eq!(meta.size(&state), 5);

    let rm = [20u32, 40];
    batch_bitmap_remove(&mut env, &mut NoVm, false, &mut state, &meta, 0, &rm);
    assert_eq!(meta.size(&state), 3);

    let data = storage.serialized_data(&state).expect("stored");
    let bm = RoaringBitmap::deserialize_from(data).unwrap();
    assert!(bm.contains(10));
    assert!(!bm.contains(20));
    assert!(bm.contains(30));
    assert!(!bm.contains(40));
    assert!(bm.contains(50));
}

// ---------------------------------------------------------------------------
// 12-15. Set algebra AND/OR/ANDNOT/XOR (bitmap_ops.zig:1197-1285) — ported
// through `set_algebra` over serialized inputs.
// ---------------------------------------------------------------------------

fn serialize(elems: &[u32]) -> Vec<u8> {
    let mut bm = RoaringBitmap::new();
    for &e in elems {
        bm.insert(e);
    }
    bm.optimize();
    let mut out = Vec::new();
    bm.serialize_into(&mut out).unwrap();
    out
}

fn algebra_elems(op: BitmapAlgebraOp, a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut env = BitmapEnv::default();
    assert_eq!(
        set_algebra(&mut env, op, &serialize(a), &serialize(b)),
        ErrorCode::Ok
    );
    if env.algebra_result().is_empty() {
        return vec![];
    }
    RoaringBitmap::deserialize_from(env.algebra_result())
        .unwrap()
        .iter()
        .collect()
}

#[test]
fn set_algebra_and_intersection() {
    let r = algebra_elems(BitmapAlgebraOp::And, &[1, 2, 3, 4, 5], &[3, 4, 5, 6, 7]);
    assert_eq!(r, vec![3, 4, 5]);
}

#[test]
fn set_algebra_or_union() {
    let r = algebra_elems(BitmapAlgebraOp::Or, &[1, 2, 3, 4, 5], &[3, 4, 5, 6, 7]);
    assert_eq!(r, vec![1, 2, 3, 4, 5, 6, 7]);
}

#[test]
fn set_algebra_andnot_difference() {
    let r = algebra_elems(BitmapAlgebraOp::AndNot, &[1, 2, 3, 4, 5], &[3, 4, 5, 6, 7]);
    assert_eq!(r, vec![1, 2]);
}

#[test]
fn set_algebra_xor_symmetric_difference() {
    let r = algebra_elems(BitmapAlgebraOp::Xor, &[1, 2, 3, 4, 5], &[3, 4, 5, 6, 7]);
    assert_eq!(r, vec![1, 2, 6, 7]);
}

// ---------------------------------------------------------------------------
// Beyond the Zig blocks: batch-add capacity refusal, slot-level algebra,
// serialized queries, spec fixtures, proptests.
// ---------------------------------------------------------------------------

#[test]
fn batch_bitmap_add_capacity_exceeded_flushes_partial_batch() {
    // bitmap_ops.zig:315-329 — the capacity branch: earlier inserts in the
    // same batch are flushed and size reflects them before CAPACITY_EXCEEDED.
    let mut env = BitmapEnv::default();
    let mut state = vec![0u8; 65536];
    let meta = init_bitmap_slot_state(&mut state, 2); // capacity 2
    let elems = [1u32, 2, 3];
    let result = batch_bitmap_add(
        &mut env, &mut NoVm, false, &mut state, &meta, 0, &elems, None,
    );
    assert_eq!(result, ErrorCode::CapacityExceeded);
    assert_eq!(meta.size(&state), 2);
    let storage = get_bitmap_storage(&meta);
    let bm = RoaringBitmap::deserialize_from(storage.serialized_data(&state).unwrap()).unwrap();
    assert!(bm.contains(1) && bm.contains(2) && !bm.contains(3));
}

#[test]
fn batch_bitmap_add_skips_sentinel_keys() {
    // EMPTY_KEY / TOMBSTONE elements are skipped (bitmap_ops.zig:311).
    let mut env = BitmapEnv::default();
    let mut state = vec![0u8; 65536];
    let meta = init_bitmap_slot_state(&mut state, 128);
    let elems = [u32::MAX, u32::MAX - 1, 7];
    let result = batch_bitmap_add(
        &mut env, &mut NoVm, false, &mut state, &meta, 0, &elems, None,
    );
    assert_eq!(result, ErrorCode::Ok);
    assert_eq!(meta.size(&state), 1);
}

#[test]
fn slot_algebra_and_with_empty_clears_target() {
    // bitmap_ops.zig:541-554 empty-source identities.
    let mut env = BitmapEnv::default();
    let mut state = vec![0u8; 65536];
    let meta = init_bitmap_slot_state(&mut state, 128);
    let add = [1u32, 2, 3];
    batch_bitmap_add(&mut env, &mut NoVm, false, &mut state, &meta, 0, &add, None);
    assert_eq!(meta.size(&state), 3);

    let r = batch_bitmap_algebra(
        &mut env,
        &mut NoVm,
        BitmapAlgebraOp::And,
        &mut state,
        &meta,
        &[],
    );
    assert_eq!(r, ErrorCode::Ok);
    assert_eq!(meta.size(&state), 0);
    let storage = get_bitmap_storage(&meta);
    assert!(storage.serialized_data(&state).is_none());

    // OR with empty = no change.
    batch_bitmap_add(&mut env, &mut NoVm, false, &mut state, &meta, 0, &add, None);
    let r = batch_bitmap_algebra(
        &mut env,
        &mut NoVm,
        BitmapAlgebraOp::Or,
        &mut state,
        &meta,
        &[],
    );
    assert_eq!(r, ErrorCode::Ok);
    assert_eq!(meta.size(&state), 3);
}

#[test]
fn slot_algebra_in_place_or() {
    let mut env = BitmapEnv::default();
    let mut state = vec![0u8; 65536];
    let meta = init_bitmap_slot_state(&mut state, 128);
    batch_bitmap_add(
        &mut env,
        &mut NoVm,
        false,
        &mut state,
        &meta,
        0,
        &[1, 2, 3],
        None,
    );

    let source = serialize(&[3, 4, 5]);
    let r = batch_bitmap_algebra(
        &mut env,
        &mut NoVm,
        BitmapAlgebraOp::Or,
        &mut state,
        &meta,
        &source,
    );
    assert_eq!(r, ErrorCode::Ok);
    assert_eq!(meta.size(&state), 5);
}

#[test]
fn serialized_queries() {
    let a = serialize(&[1, 2, 3, 100_000]);
    let b = serialize(&[3, 4]);
    let c = serialize(&[9]);
    assert!(contains_serialized(&a, 100_000));
    assert!(!contains_serialized(&a, 99));
    assert!(!contains_serialized(&[], 1));
    assert_eq!(cardinality_serialized(&a), 4);
    assert_eq!(cardinality_serialized(&[]), 0);
    assert!(intersects_serialized(&a, &b));
    assert!(!intersects_serialized(&a, &c));
    assert_eq!(intersect_count_serialized(&a, &b), 1);
    assert_eq!(intersect_count_serialized(&a, &[]), 0);
    let mut out = [0u32; 3];
    assert_eq!(extract_serialized(&a, &mut out), 3); // capped at buffer
    assert_eq!(out, [1, 2, 3]);
}

#[test]
fn set_algebra_empty_identities_copy_survivor() {
    let mut env = BitmapEnv::default();
    let a = serialize(&[1, 2]);
    // left empty: OR/XOR yield right, AND/ANDNOT yield empty.
    assert_eq!(
        set_algebra(&mut env, BitmapAlgebraOp::Or, &[], &a),
        ErrorCode::Ok
    );
    assert_eq!(env.algebra_result(), &a[..]);
    assert_eq!(
        set_algebra(&mut env, BitmapAlgebraOp::And, &[], &a),
        ErrorCode::Ok
    );
    assert!(env.algebra_result().is_empty());
    // right empty: OR/ANDNOT/XOR yield left, AND yields empty.
    assert_eq!(
        set_algebra(&mut env, BitmapAlgebraOp::AndNot, &a, &[]),
        ErrorCode::Ok
    );
    assert_eq!(env.algebra_result(), &a[..]);
    assert_eq!(
        set_algebra(&mut env, BitmapAlgebraOp::And, &a, &[]),
        ErrorCode::Ok
    );
    assert!(env.algebra_result().is_empty());
}

/// RoaringFormatSpec read-compat fixture, hand-derived from the public spec
/// (the same spec rawr's format.zig implements): cookie 12346
/// (SERIAL_COOKIE_NO_RUNCONTAINER), one array container, key 0,
/// cardinality-1 = 2, offset header, then values {7, 42, 100} as u16 LE.
/// Pins that the roaring crate parses spec-formatted bytes byte-for-byte —
/// the read-compat direction of the rawr interop story.
#[test]
fn roaring_format_spec_fixture_parses() {
    let mut fixture = Vec::new();
    fixture.extend_from_slice(&12346u32.to_le_bytes()); // cookie
    fixture.extend_from_slice(&1u32.to_le_bytes()); // container count
    fixture.extend_from_slice(&0u16.to_le_bytes()); // key (high 16 bits)
    fixture.extend_from_slice(&2u16.to_le_bytes()); // cardinality - 1
    fixture.extend_from_slice(&16u32.to_le_bytes()); // offset of container data
    for v in [7u16, 42, 100] {
        fixture.extend_from_slice(&v.to_le_bytes());
    }
    let bm = RoaringBitmap::deserialize_from(&fixture[..]).expect("spec fixture parses");
    let elems: Vec<u32> = bm.iter().collect();
    assert_eq!(elems, vec![7, 42, 100]);
    // And the write direction round-trips through our own serializer.
    assert_eq!(cardinality_serialized(&serialize(&[7, 42, 100])), 3);
}

proptest! {
    /// Round-trip through slot storage at boundary payload capacities: the
    /// 255/256/257-byte region around BITMAP_BASE_BYTES and the exact
    /// payload_capacity edge (store must refuse, not truncate, when the
    /// serialized form exceeds capacity).
    #[test]
    fn store_load_round_trip_boundary_capacities(
        elems in prop::collection::btree_set(0u32..100_000, 0..600),
        cap in 0u32..80,
    ) {
        let mut env = BitmapEnv::default();
        let payload_cap = bitmap_payload_capacity(cap);
        let buf_len = (BITMAP_SERIALIZED_LEN_BYTES + payload_cap) as usize;
        let mut buf = vec![0u8; buf_len];
        let storage = make_storage(buf_len as u32);

        let mut bm = RoaringBitmap::new();
        for &e in &elems { bm.insert(e); }
        let elems_vec: Vec<u32> = bm.iter().collect();

        match bitmap_store(&mut env, &mut buf, storage, &mut bm) {
            ErrorCode::Ok => {
                let reloaded = bitmap_load(&mut env, &buf, storage).expect("reload");
                let got: Vec<u32> = reloaded.iter().collect();
                prop_assert_eq!(got, elems_vec);
                // Tail beyond serialized_len is zeroed (two-phase commit).
                let len = storage.serialized_len(&buf);
                prop_assert!(len <= payload_cap);
                let tail_start = (storage.payload_offset() + len) as usize;
                prop_assert!(buf[tail_start..].iter().all(|&b| b == 0));
            }
            ErrorCode::CapacityExceeded => {
                // Refusal must leave the slot bytes untouched (still empty).
                prop_assert_eq!(storage.serialized_len(&buf), 0);
                prop_assert!(bm.serialized_size() > payload_cap as usize);
            }
            other => prop_assert!(false, "unexpected error {other:?}"),
        }
    }

    /// Slot-level algebra agrees with BTreeSet model algebra.
    #[test]
    fn slot_algebra_matches_set_model(
        a in prop::collection::btree_set(0u32..10_000, 0..64),
        b in prop::collection::btree_set(0u32..10_000, 0..64),
        op_idx in 0usize..4,
    ) {
        use std::collections::BTreeSet;
        let ops = [BitmapAlgebraOp::And, BitmapAlgebraOp::Or, BitmapAlgebraOp::AndNot, BitmapAlgebraOp::Xor];
        let op = ops[op_idx];

        let mut env = BitmapEnv::default();
        let mut state = vec![0u8; 1 << 20];
        let meta = init_bitmap_slot_state(&mut state, 16_384);
        let a_vec: Vec<u32> = a.iter().copied().collect();
        if !a_vec.is_empty() {
            prop_assert_eq!(
                batch_bitmap_add(&mut env, &mut NoVm, false, &mut state, &meta, 0, &a_vec, None),
                ErrorCode::Ok
            );
        }
        let source = serialize(&b.iter().copied().collect::<Vec<_>>());
        let source = if b.is_empty() { vec![] } else { source };
        prop_assert_eq!(
            batch_bitmap_algebra(&mut env, &mut NoVm, op, &mut state, &meta, &source),
            ErrorCode::Ok
        );

        let expected: BTreeSet<u32> = match op {
            BitmapAlgebraOp::And => a.intersection(&b).copied().collect(),
            BitmapAlgebraOp::Or => a.union(&b).copied().collect(),
            BitmapAlgebraOp::AndNot => a.difference(&b).copied().collect(),
            BitmapAlgebraOp::Xor => a.symmetric_difference(&b).copied().collect(),
        };
        let storage = get_bitmap_storage(&meta);
        let got: BTreeSet<u32> = match storage.serialized_data(&state) {
            Some(d) => RoaringBitmap::deserialize_from(d).unwrap().iter().collect(),
            None => BTreeSet::new(),
        };
        prop_assert_eq!(&got, &expected);
        prop_assert_eq!(meta.size(&state) as usize, expected.len());
    }

    /// Batch add/remove interleavings agree with a BTreeSet model, including
    /// meta.size — the observable slot contract.
    #[test]
    fn add_remove_matches_set_model(
        batches in prop::collection::vec(
            (prop::collection::vec(0u32..5_000, 1..32), any::<bool>()),
            1..12
        ),
    ) {
        use std::collections::BTreeSet;
        let mut env = BitmapEnv::default();
        let mut state = vec![0u8; 1 << 20];
        let meta = init_bitmap_slot_state(&mut state, 16_384);
        let mut model = BTreeSet::new();

        for (elems, is_remove) in &batches {
            if *is_remove {
                batch_bitmap_remove(&mut env, &mut NoVm, false, &mut state, &meta, 0, elems);
                for e in elems { model.remove(e); }
            } else {
                prop_assert_eq!(
                    batch_bitmap_add(&mut env, &mut NoVm, false, &mut state, &meta, 0, elems, None),
                    ErrorCode::Ok
                );
                for &e in elems { model.insert(e); }
            }
        }

        let storage = get_bitmap_storage(&meta);
        let got: BTreeSet<u32> = match storage.serialized_data(&state) {
            Some(d) => RoaringBitmap::deserialize_from(d).unwrap().iter().collect(),
            None => BTreeSet::new(),
        };
        prop_assert_eq!(&got, &model);
        prop_assert_eq!(meta.size(&state) as usize, model.len());
    }
}

// ---------------------------------------------------------------------------
// minroar ↔ roaring crate differential oracle (roaring is a DEV-dep only —
// the shipped artifact carries minroar; these tests pin cross-compat).
// ---------------------------------------------------------------------------

proptest! {
    /// minroar-serialized bytes parse in the roaring crate with identical
    /// contents, and vice versa — the two directions of portable-spec
    /// interop, including the run-container path via optimize().
    #[test]
    fn minroar_roaring_cross_compat(
        elems in proptest::collection::btree_set(any::<u32>(), 0..600),
        dense_base in 0u32..1000,
        dense_len in 0u32..5000,
        opt in any::<bool>(),
    ) {
        let mut mine = columine_vm::minroar::MiniRoaring::new();
        let mut theirs = roaring::RoaringBitmap::new();
        for &e in &elems { mine.insert(e); theirs.insert(e); }
        // Dense run to exercise run/bitset containers.
        for e in dense_base..dense_base + dense_len { mine.insert(e); theirs.insert(e); }
        if opt { mine.optimize(); }

        // mine → theirs
        let mut buf = Vec::new();
        mine.serialize_into(&mut buf).unwrap();
        prop_assert_eq!(buf.len(), mine.serialized_size());
        let reparsed = roaring::RoaringBitmap::deserialize_from(&buf[..]).expect("roaring parses minroar bytes");
        prop_assert!(reparsed.iter().eq(mine.iter()));

        // theirs → mine
        let mut rbuf = Vec::new();
        theirs.serialize_into(&mut rbuf).unwrap();
        let mine2 = columine_vm::minroar::MiniRoaring::deserialize_from(&rbuf).expect("minroar parses roaring bytes");
        prop_assert!(mine2.iter().eq(theirs.iter()));
    }

    /// Algebra + membership + cardinality equivalence against the oracle.
    #[test]
    fn minroar_ops_match_roaring(
        a in proptest::collection::btree_set(any::<u32>(), 0..300),
        b in proptest::collection::btree_set(any::<u32>(), 0..300),
        probe in any::<u32>(),
    ) {
        let (mut ma, mut mb) = (columine_vm::minroar::MiniRoaring::new(), columine_vm::minroar::MiniRoaring::new());
        let (mut ra, mut rb) = (roaring::RoaringBitmap::new(), roaring::RoaringBitmap::new());
        for &e in &a { ma.insert(e); ra.insert(e); }
        for &e in &b { mb.insert(e); rb.insert(e); }

        prop_assert_eq!(ma.len(), ra.len());
        prop_assert_eq!(ma.contains(probe), ra.contains(probe));
        prop_assert_eq!(ma.intersection_len(&mb), ra.intersection_len(&rb));
        prop_assert_eq!(ma.is_disjoint(&mb), ra.is_disjoint(&rb));

        let and = ma.clone() & mb.clone();
        let or = ma.clone() | mb.clone();
        let sub = ma.clone() - mb.clone();
        let xor = ma.clone() ^ mb.clone();
        prop_assert!(and.iter().eq((ra.clone() & rb.clone()).iter()));
        prop_assert!(or.iter().eq((ra.clone() | rb.clone()).iter()));
        prop_assert!(sub.iter().eq((ra.clone() - rb.clone()).iter()));
        prop_assert!(xor.iter().eq((ra.clone() ^ rb.clone()).iter()));

        // Removal equivalence on a shared element, if any.
        if let Some(&e) = a.iter().next() {
            prop_assert_eq!(ma.remove(e), ra.remove(e));
            prop_assert!(ma.iter().eq(ra.iter()));
        }
    }
}
