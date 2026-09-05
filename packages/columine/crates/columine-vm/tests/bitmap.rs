//! Coverage for the BITMAP slot: AXR1 storage, in-place patch mutation,
//! algebra, capacity refusals, and the differential oracle — a `BTreeSet`
//! model against the slot bytes, with the slot image required to be
//! byte-equal to a fresh forest freeze of the surviving set.

use axroar::{Axroar, AxroarView};
use columine_vm::bitmap_ops::{
    BitmapAlgebraOp, BitmapEnv, BitmapSource, BitmapStorage, batch_bitmap_add,
    batch_bitmap_algebra, batch_bitmap_remove, bitmap_import, bitmap_patch,
    bitmap_payload_capacity, bitmap_select, cardinality_serialized, cardinality_validated,
    contains_serialized, extract_serialized, get_bitmap_storage, intersect_count_serialized,
    intersects_serialized, set_algebra,
};
use columine_vm::hooks::{MutationRecord, NoVm, VmHooks};
use columine_vm::meta::SlotMetaView;
use columine_vm::state_init::{
    DEFAULT_ACCEPTED_PROGRAM_MAGICS, EVICTION_ENTRY_SIZE, calculate_state_size, init_state,
};
use columine_vm::undo_log::FlatUndoOp;
use columine_vm::vm::{Vm, find_latest_eviction_timestamp_for_key};
use proptest::prelude::*;

use columine_types::types::{
    BITMAP_BASE_BYTES, BITMAP_BYTES_PER_CAPACITY, BITMAP_SERIALIZED_LEN_BYTES, ErrorCode, Opcode,
    PROGRAM_MAGIC, SLOT_META_SIZE, STATE_FORMAT_VERSION, STATE_HEADER_SIZE, STATE_MAGIC,
    SlotMetaOffset, SlotType, StateHeaderOffset,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Default, PartialEq)]
struct RecordingHooks {
    ttl_by_key: BTreeMap<u32, u64>,
    mutations: Vec<(MutationRecord, MutationRecord)>,
}

impl VmHooks for RecordingHooks {
    fn undo_enabled(&self) -> bool {
        true
    }

    fn append_mutation(
        &mut self,
        _delta_mode: bool,
        _state: &[u8],
        undo: MutationRecord,
        redo: MutationRecord,
    ) {
        self.mutations.push((undo, redo));
    }

    fn insert_with_ttl(
        &mut self,
        _state: &mut [u8],
        _meta: &SlotMetaView,
        key: u32,
        ts: f64,
    ) -> ErrorCode {
        self.ttl_by_key.insert(key, ts.to_bits());
        ErrorCode::Ok
    }

    fn latest_eviction_ts(&self, _state: &[u8], _meta: &SlotMetaView, key: u32) -> Option<f64> {
        self.ttl_by_key.get(&key).copied().map(f64::from_bits)
    }

    fn remove_ttl_entries_for_key(&mut self, _state: &mut [u8], _meta: &SlotMetaView, key: u32) {
        self.ttl_by_key.remove(&key);
    }

    fn undo_overflow(&self) -> bool {
        false
    }

    fn force_undo_snapshot(&mut self, _state: &[u8]) {
        unreachable!("the bitmap transaction property never snapshots")
    }
}

/// A storage view over a local buffer: `[serialized_len u32][pad][payload…]`
/// at offset 0.
fn make_storage(buf_len: u32) -> BitmapStorage {
    BitmapStorage {
        data_offset: 0,
        payload_capacity: buf_len - BITMAP_SERIALIZED_LEN_BYTES,
    }
}

/// Minimal state with one BITMAP slot:
/// `[STATE_HEADER (32)][SLOT_META (48)][bitmap data …]`.
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

fn init_ttl_bitmap_slot_state(state: &mut [u8], capacity: u32) -> SlotMetaView {
    let meta = init_bitmap_slot_state(state, capacity);
    state[(meta.meta_base + SlotMetaOffset::TYPE_FLAGS) as usize] = SlotType::Bitmap as u8 | 0x10;
    SlotMetaView::read(state, 0)
}

// ---------------------------------------------------------------------------
// Real-VM scaffolding: a program-initialized TTL BITMAP slot, so the eviction
// index and the payload region are laid out by `init_state` rather than by the
// test hand-writing metadata offsets.
// ---------------------------------------------------------------------------

/// `SlotTypeFlags::HAS_TTL_MASK`, which is private to `columine-types`.
const TTL_FLAG: u8 = 0x10;

/// Assemble a program: 32-byte hash prefix + 14-byte content header + init and
/// reduce sections, each NUL-terminated.
fn program(num_slots: u8, num_inputs: u8, init: &[u8], reduce: &[u8]) -> Vec<u8> {
    let mut init = init.to_vec();
    init.push(Opcode::Halt as u8);
    let mut reduce = reduce.to_vec();
    reduce.push(Opcode::Halt as u8);
    let mut prog = vec![0u8; 32];
    prog.extend(PROGRAM_MAGIC.to_le_bytes());
    prog.extend([1, 0, num_slots, num_inputs, 0, 0]);
    prog.extend((init.len() as u16).to_le_bytes());
    prog.extend((reduce.len() as u16).to_le_bytes());
    prog.extend(init);
    prog.extend(reduce);
    prog
}

/// One TTL BITMAP slot fed by BATCH_SET_INSERT. `requested` is the program's
/// capacity request; initialization normalizes it to `next_power_of_2(2n)`.
fn ttl_bitmap_program(requested: u16) -> Vec<u8> {
    let mut def = vec![
        Opcode::SlotDef as u8,
        0,
        SlotType::Bitmap as u8 | TTL_FLAG,
        requested as u8,
        (requested >> 8) as u8,
    ];
    def.extend(10.0f32.to_le_bytes()); // ttl_seconds
    def.extend(0.0f32.to_le_bytes()); // grace_seconds
    def.push(1); // timestamp column index
    def.push(0); // DurationUnit::NONE
    program(1, 2, &def, &[Opcode::BatchSetInsert as u8, 0, 0])
}

fn init_program_state(prog: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS);
    assert!(size > 0, "program must size a state");
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("init_state");
    state
}

/// One element per 16-bit chunk: every chunk pays its directory record and
/// an 8-byte payload footprint, which is what makes a batch of them the
/// cheapest way to exhaust a payload while staying under the element cap.
fn sparse_key(high: u16) -> u32 {
    u32::from(high) << 16
}

/// A timestamp that is a pure function of the key — see the reversibility
/// property for why a key-varying timestamp is not recoverable.
fn sparse_ts(key: u32) -> f64 {
    f64::from(key >> 16)
}

/// The forest string of a set — the form the slot keeps — or zero bytes for
/// the empty set, which is the VM's only empty form.
fn serialize(elems: &[u32]) -> Vec<u8> {
    let sorted: BTreeSet<u32> = elems.iter().copied().collect();
    if sorted.is_empty() {
        return Vec::new();
    }
    Axroar::from_sorted(sorted).to_forest_bytes()
}

/// The slot's members, read through the zero-copy view.
fn members(state: &[u8], storage: BitmapStorage) -> Vec<u32> {
    match storage.serialized_data(state) {
        None => Vec::new(),
        Some(data) => AxroarView::open(data)
            .expect("slot holds a well-formed AXR1 string")
            .range()
            .collect(),
    }
}

/// The slot invariant: the payload holds the forest string of exactly `expected`
/// at its head, the length field agrees, and every byte past the string is
/// zero — so the image is a pure function of the set.
fn assert_slot_is_canonical(state: &[u8], storage: BitmapStorage, expected: &BTreeSet<u32>) {
    let payload = storage.payload_offset() as usize;
    let capacity = storage.payload_capacity as usize;
    let len = storage.serialized_len(state) as usize;
    let fresh = if expected.is_empty() {
        Vec::new()
    } else {
        Axroar::from_sorted(expected.iter().copied()).to_forest_bytes()
    };
    assert_eq!(
        len,
        fresh.len(),
        "serialized_len is the fresh forest length"
    );
    assert_eq!(
        &state[payload..payload + len],
        &fresh[..],
        "slot image is the fresh forest"
    );
    assert!(
        state[payload + len..payload + capacity]
            .iter()
            .all(|&b| b == 0),
        "payload past the string is zero"
    );
}

// ---------------------------------------------------------------------------
// Storage basics
// ---------------------------------------------------------------------------

#[test]
fn empty_slot_reads_as_the_empty_set() {
    let buf = vec![0u8; 4096];
    let storage = make_storage(4096);
    assert!(storage.serialized_data(&buf).is_none());
    assert!(storage.view(&buf).expect("empty is well-formed").is_none());
    assert_eq!(bitmap_select(&buf, storage, 0), None);
}

#[test]
fn patch_round_trip_preserves_elements() {
    let mut env = BitmapEnv::default();
    let mut buf = vec![0u8; 8192];
    let storage = make_storage(8192);

    let elems = [5u32, 10, 15, 20, 25, 30, 35, 40, 45, 50];
    let report = bitmap_patch(&mut env, &mut buf, storage, &elems, &[]).expect("patch");
    assert_eq!(report.added, 10);
    assert_eq!(report.len, 10);
    let view = storage.view(&buf).expect("well-formed").expect("non-empty");
    assert_eq!(view.len(), 10);
    for &e in &elems {
        assert!(view.contains(e));
    }
    assert!(!view.contains(99));
    assert_slot_is_canonical(&buf, storage, &elems.iter().copied().collect());
}

#[test]
fn patch_500_elements_round_trip() {
    let mut env = BitmapEnv::default();
    let mut buf = vec![0u8; 65536];
    let storage = make_storage(65536);
    let elems: Vec<u32> = (1..=500).collect();
    bitmap_patch(&mut env, &mut buf, storage, &elems, &[]).expect("patch");
    assert_eq!(members(&buf, storage), elems);
}

#[test]
fn removing_the_last_member_restores_the_empty_form() {
    let mut env = BitmapEnv::default();
    let mut buf = vec![0u8; 8192];
    let storage = make_storage(8192);
    bitmap_patch(&mut env, &mut buf, storage, &[7, 42], &[]).expect("patch");
    let report = bitmap_patch(&mut env, &mut buf, storage, &[], &[7, 42]).expect("patch");
    assert_eq!(report.removed, 2);
    assert_eq!(report.len, 0);
    assert_eq!(storage.serialized_len(&buf), 0);
    assert!(
        buf.iter().all(|&b| b == 0),
        "the empty set is all zero bytes"
    );
}

#[test]
fn bitmap_select_returns_element_at_rank() {
    let mut env = BitmapEnv::default();
    let mut buf = vec![0u8; 8192];
    let storage = make_storage(8192);
    bitmap_patch(&mut env, &mut buf, storage, &[10, 20, 30, 40, 50], &[]).expect("patch");

    assert_eq!(bitmap_select(&buf, storage, 0), Some(10));
    assert_eq!(bitmap_select(&buf, storage, 2), Some(30));
    assert_eq!(bitmap_select(&buf, storage, 4), Some(50));
    assert_eq!(bitmap_select(&buf, storage, 5), None);
}

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
    assert_eq!(
        storage.payload_offset() % 8,
        0,
        "the AXR1 payload starts on an 8-byte boundary"
    );
    assert_eq!(storage.payload_capacity, bitmap_payload_capacity(capacity));
}

// ---------------------------------------------------------------------------
// Batch mutation
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
    assert_eq!(members(&state, storage), elems);
}

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

#[test]
fn batch_bitmap_add_accepts_unsorted_input() {
    let mut env = BitmapEnv::default();
    let mut state = vec![0u8; 65536];
    let meta = init_bitmap_slot_state(&mut state, 128);
    let storage = get_bitmap_storage(&meta);
    let elems = [500u32, 3, 70_000, 3, 12];
    assert_eq!(
        batch_bitmap_add(
            &mut env, &mut NoVm, false, &mut state, &meta, 0, &elems, None
        ),
        ErrorCode::Ok
    );
    assert_eq!(members(&state, storage), [3, 12, 500, 70_000]);
    assert_eq!(meta.size(&state), 4);
}

#[test]
fn batch_bitmap_remove_removes_elements_correctly() {
    let mut env = BitmapEnv::default();
    let mut state = vec![0u8; 65536];
    let meta = init_bitmap_slot_state(&mut state, 128);
    let storage = get_bitmap_storage(&meta);

    let add = [10u32, 20, 30, 40, 50];
    batch_bitmap_add(&mut env, &mut NoVm, false, &mut state, &meta, 0, &add, None);
    assert_eq!(meta.size(&state), 5);

    let rm = [40u32, 20, 20, 99];
    batch_bitmap_remove(&mut env, &mut NoVm, false, &mut state, &meta, 0, &rm);
    assert_eq!(meta.size(&state), 3);
    assert_eq!(members(&state, storage), [10, 30, 50]);
}

// ---------------------------------------------------------------------------
// Set algebra AND/OR/ANDNOT/XOR, exercised through `set_algebra` over
// serialized inputs.
// ---------------------------------------------------------------------------

fn algebra_elems(op: BitmapAlgebraOp, a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut env = BitmapEnv::default();
    assert_eq!(
        set_algebra(&mut env, op, &serialize(a), &serialize(b)),
        ErrorCode::Ok
    );
    if env.algebra_result().is_empty() {
        return vec![];
    }
    AxroarView::open(env.algebra_result())
        .expect("algebra result is a well-formed string")
        .range()
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

#[test]
fn set_algebra_empty_result_is_zero_bytes() {
    assert!(algebra_elems(BitmapAlgebraOp::And, &[1, 2], &[3, 4]).is_empty());
    assert!(algebra_elems(BitmapAlgebraOp::Xor, &[1, 2], &[1, 2]).is_empty());
}

#[test]
fn set_algebra_result_is_the_forest_form() {
    let mut env = BitmapEnv::default();
    let a: Vec<u32> = (0..2000).map(|i| i * 7).collect();
    let b: Vec<u32> = (0..2000).map(|i| i * 11).collect();
    assert_eq!(
        set_algebra(
            &mut env,
            BitmapAlgebraOp::Or,
            &serialize(&a),
            &serialize(&b)
        ),
        ErrorCode::Ok
    );
    let expected: BTreeSet<u32> = a.iter().chain(b.iter()).copied().collect();
    assert_eq!(
        env.algebra_result(),
        &Axroar::from_sorted(expected).to_forest_bytes()[..]
    );
}

// ---------------------------------------------------------------------------
// Capacity refusals, slot-level algebra, serialized queries, import.
// ---------------------------------------------------------------------------

#[test]
fn batch_bitmap_add_capacity_exceeded_flushes_partial_batch() {
    // Earlier inserts are flushed before the capacity refusal.
    let mut env = BitmapEnv::default();
    let mut state = vec![0u8; 65536];
    let meta = init_bitmap_slot_state(&mut state, 2); // capacity 2
    let elems = [3u32, 1, 2];
    let result = batch_bitmap_add(
        &mut env, &mut NoVm, false, &mut state, &meta, 0, &elems, None,
    );
    assert_eq!(result, ErrorCode::CapacityExceeded);
    assert_eq!(meta.size(&state), 2);
    let storage = get_bitmap_storage(&meta);
    assert_eq!(members(&state, storage), [1, 3], "the batch's earliest two");
}

#[test]
fn payload_capacity_refusal_leaves_the_slot_untouched() {
    let mut env = BitmapEnv::default();
    let mut state = vec![0u8; 65536];
    // Element cap 64, payload 64*4+256 = 512 bytes: 60 one-per-chunk members
    // need 60 directory records and 60 payload footprints.
    let meta = init_bitmap_slot_state(&mut state, 64);
    let storage = get_bitmap_storage(&meta);
    let seed = [sparse_key(1), sparse_key(2)];
    assert_eq!(
        batch_bitmap_add(
            &mut env, &mut NoVm, false, &mut state, &meta, 0, &seed, None
        ),
        ErrorCode::Ok
    );
    let before = state.clone();
    let flood: Vec<u32> = (3u16..63).map(sparse_key).collect();
    assert_eq!(
        batch_bitmap_add(
            &mut env, &mut NoVm, false, &mut state, &meta, 0, &flood, None
        ),
        ErrorCode::CapacityExceeded
    );
    assert_eq!(env.last_error, 60);
    assert_eq!(state, before);
    assert_eq!(members(&state, storage), seed);

    // The empty slot refuses the same way and stays all-zero.
    let mut fresh = vec![0u8; 65536];
    let meta = init_bitmap_slot_state(&mut fresh, 64);
    let before = fresh.clone();
    assert_eq!(
        batch_bitmap_add(
            &mut env, &mut NoVm, false, &mut fresh, &meta, 0, &flood, None
        ),
        ErrorCode::CapacityExceeded
    );
    assert_eq!(fresh, before);
}

#[test]
fn batch_bitmap_add_skips_sentinel_keys() {
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
        BitmapSource::Bytes(&[]),
    );
    assert_eq!(r, ErrorCode::Ok);
    assert_eq!(meta.size(&state), 0);
    let storage = get_bitmap_storage(&meta);
    assert!(storage.serialized_data(&state).is_none());
    assert_slot_is_canonical(&state, storage, &BTreeSet::new());

    // OR with empty = no change.
    batch_bitmap_add(&mut env, &mut NoVm, false, &mut state, &meta, 0, &add, None);
    let r = batch_bitmap_algebra(
        &mut env,
        &mut NoVm,
        BitmapAlgebraOp::Or,
        &mut state,
        &meta,
        BitmapSource::Bytes(&[]),
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
        BitmapSource::Bytes(&source),
    );
    assert_eq!(r, ErrorCode::Ok);
    assert_eq!(meta.size(&state), 5);
    let storage = get_bitmap_storage(&meta);
    assert_eq!(members(&state, storage), [1, 2, 3, 4, 5]);
    assert_slot_is_canonical(&state, storage, &(1..=5).collect());
}

#[test]
fn slot_algebra_scratch_operand_reads_the_previous_result() {
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
        &[1, 2, 3, 4],
        None,
    );
    assert_eq!(
        set_algebra(
            &mut env,
            BitmapAlgebraOp::Or,
            &serialize(&[2]),
            &serialize(&[4, 9])
        ),
        ErrorCode::Ok
    );
    let r = batch_bitmap_algebra(
        &mut env,
        &mut NoVm,
        BitmapAlgebraOp::AndNot,
        &mut state,
        &meta,
        BitmapSource::Scratch,
    );
    assert_eq!(r, ErrorCode::Ok);
    assert_eq!(members(&state, get_bitmap_storage(&meta)), [1, 3]);
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
    assert_eq!(cardinality_validated(&a), Some(4));
    assert_eq!(cardinality_validated(&[]), Some(0));
    assert_eq!(cardinality_validated(&a[..a.len() - 1]), None);
    assert_eq!(cardinality_validated(b"not an AXR1 string at all"), None);
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

#[test]
fn import_keeps_the_forest_form_and_refuses_foreign_bytes() {
    let mut env = BitmapEnv::default();
    let mut state = vec![0u8; 1 << 20];
    let meta = init_bitmap_slot_state(&mut state, 16_384);
    let storage = get_bitmap_storage(&meta);

    // A sparse set whose canonical string is the Elias-Fano root.
    let sparse: BTreeSet<u32> = (0..100u32)
        .map(|i| i.wrapping_mul(2_654_435_761) >> 4)
        .collect();
    let canonical = Axroar::from_sorted(sparse.iter().copied()).to_bytes();
    assert!(
        AxroarView::open(&canonical)
            .expect("well-formed")
            .is_elias_fano()
    );
    assert_eq!(
        bitmap_import(&mut env, &mut state, &meta, &canonical),
        ErrorCode::Ok
    );
    assert_eq!(meta.size(&state), sparse.len() as u32);
    assert_slot_is_canonical(&state, storage, &sparse);

    // A forest string lands as is; the empty string clears the slot.
    let dense: BTreeSet<u32> = (0..5000).collect();
    let forest = Axroar::from_sorted(dense.iter().copied()).to_forest_bytes();
    assert_eq!(
        bitmap_import(&mut env, &mut state, &meta, &forest),
        ErrorCode::Ok
    );
    assert_slot_is_canonical(&state, storage, &dense);
    assert_eq!(
        bitmap_import(&mut env, &mut state, &meta, &[]),
        ErrorCode::Ok
    );
    assert_slot_is_canonical(&state, storage, &BTreeSet::new());
    assert_eq!(meta.size(&state), 0);

    // Foreign or truncated bytes are refused and change nothing.
    let before = state.clone();
    assert_eq!(
        bitmap_import(
            &mut env,
            &mut state,
            &meta,
            b"\x3a\x30\x00\x00\x01\x00\x00\x00"
        ),
        ErrorCode::InvalidState
    );
    assert_eq!(
        bitmap_import(&mut env, &mut state, &meta, &forest[..forest.len() - 3]),
        ErrorCode::InvalidState
    );
    assert_eq!(state, before);

    // More members than the element cap is a capacity refusal.
    let small = init_bitmap_slot_state(&mut state, 8);
    assert_eq!(
        bitmap_import(&mut env, &mut state, &small, &forest),
        ErrorCode::CapacityExceeded
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    /// Every accepted mutation has a real inverse, while a payload-capacity
    /// refusal commits neither primary bytes, TTL state, nor undo records.
    #[test]
    fn bitmap_sequence_is_reversible_across_payload_refusal(
        high_words in prop::collection::btree_set(1u16..60_000, 180..193),
        timestamp_bits in any::<u64>(),
    ) {
        let capacity = 192;
        let mut env = BitmapEnv::default();
        let mut state = vec![0u8; 8192];
        let meta = init_ttl_bitmap_slot_state(&mut state, capacity);
        let storage = get_bitmap_storage(&meta);
        let initial_state = state.clone();
        let mut hooks = RecordingHooks::default();
        let timestamp = f64::from_bits(timestamp_bits);
        let mut saw_payload_refusal = false;

        for high in high_words {
            let key = u32::from(high) << 16;
            let state_before = state.clone();
            let hooks_before = hooks.clone();
            match batch_bitmap_add(
                &mut env,
                &mut hooks,
                false,
                &mut state,
                &meta,
                0,
                &[key],
                Some(&[timestamp]),
            ) {
                ErrorCode::Ok => {}
                ErrorCode::CapacityExceeded => {
                    saw_payload_refusal = true;
                    prop_assert_eq!(&state, &state_before);
                    prop_assert_eq!(&hooks, &hooks_before);
                }
                other => prop_assert!(false, "unexpected bitmap result {other:?}"),
            }
        }
        prop_assert!(saw_payload_refusal);

        for (undo, _) in hooks.mutations.clone().iter().rev() {
            match undo.op {
                FlatUndoOp::SetInsert => {
                    let report = bitmap_patch(&mut env, &mut state, storage, &[], &[undo.key])
                        .expect("undo of an accepted insert fits");
                    prop_assert_eq!(report.removed, 1);
                    hooks.ttl_by_key.remove(&undo.key);
                }
                FlatUndoOp::SetDelete => {
                    let report = bitmap_patch(&mut env, &mut state, storage, &[undo.key], &[])
                        .expect("undo of an accepted delete fits");
                    prop_assert_eq!(report.added, 1);
                    hooks.ttl_by_key.insert(undo.key, undo.aux);
                }
                other => prop_assert!(false, "unexpected bitmap undo record {other:?}"),
            }
        }
        meta.set_size(&mut state, 0);
        state[(meta.meta_base + SlotMetaOffset::CHANGE_FLAGS) as usize] = 0;
        hooks.mutations.clear();

        prop_assert_eq!(state, initial_state);
        prop_assert_eq!(hooks, RecordingHooks::default());
    }
}

proptest! {
    /// Apply-then-undo is the identity on a TTL BITMAP slot, driven through the
    /// real `Vm` undo log and the real in-state eviction index rather than a
    /// hand replay — and including operations refused for payload capacity,
    /// which must commit nothing on any of the three planes.
    ///
    /// The refusal is spliced at a randomized point in a randomized add/remove
    /// sequence: the flood batch asks for more chunks than the payload can
    /// hold, while staying under `meta.capacity` elements so the element-cap
    /// branch — which deliberately commits what fits — stays unreachable and
    /// every `CapacityExceeded` here is the payload one.
    ///
    /// Timestamps are a pure function of the key. A TTL refresh of an element
    /// that is already present emits no undo record, so a key-varying timestamp
    /// would not be recoverable; that gap is pinned by
    /// `ttl_refresh_of_present_member_records_no_undo_entry`.
    #[test]
    fn bitmap_apply_then_undo_is_identity_across_payload_refusal(
        batches in prop::collection::vec(
            (prop::collection::btree_set(1u16..240, 1..24), any::<bool>()),
            1..14,
        ),
        flood_at in any::<prop::sample::Index>(),
    ) {
        let prog = ttl_bitmap_program(128);
        let mut state = init_program_state(&prog);
        let meta = SlotMetaView::read(&state, 0);
        prop_assert_eq!(meta.capacity, 256); // next_power_of_2(128 * 2)
        prop_assert!(meta.has_ttl());
        prop_assert_eq!(meta.eviction_index_capacity(&state), 256);

        let mut vm = Vm::default();

        // Seed with undo disabled so the eviction index starts non-empty and
        // rollback's restore path has real entries to put back.
        let seed: Vec<u32> = (1u16..61).map(sparse_key).collect();
        let seed_ts: Vec<f64> = seed.iter().copied().map(sparse_ts).collect();
        prop_assert_eq!(
            vm.set_insert(false, &mut state, &meta, 0, &seed, Some(&seed_ts)),
            ErrorCode::Ok
        );
        prop_assert_eq!(meta.eviction_index_size(&state), 60);

        vm.undo_enable(&state);
        let checkpoint = vm.undo_checkpoint();
        let initial = state.clone();

        // Every distinct high word: more chunks than the payload can hold, so
        // this batch is refused whatever the randomized prefix did.
        let flood: Vec<u32> = (1u16..240).map(sparse_key).collect();
        let flood_ts: Vec<f64> = flood.iter().copied().map(sparse_ts).collect();

        // The generated batches with the flood (`None`) spliced in at a
        // randomized point.
        let mut ops: Vec<Option<&(BTreeSet<u16>, bool)>> = batches.iter().map(Some).collect();
        ops.insert(flood_at.index(batches.len() + 1), None);

        // A generated add batch can reach the payload ceiling too, so refusal is
        // checked identically wherever it happens and the flood only guarantees
        // at least one. `state` carries the slot bytes and the eviction index;
        // the undo log lives in `Vm`, so the checkpoint is compared beside it.
        let mut saw_payload_refusal = false;
        for op in ops {
            let before = state.clone();
            let undo_before = vm.undo_checkpoint();
            let result = match op {
                None => vm.set_insert(
                    false,
                    &mut state,
                    &meta,
                    0,
                    &flood,
                    Some(&flood_ts),
                ),
                Some((highs, is_remove)) => {
                    let elems: Vec<u32> = highs.iter().copied().map(sparse_key).collect();
                    if *is_remove {
                        vm.set_remove(false, &mut state, &meta, 0, &elems)
                    } else {
                        let ts: Vec<f64> = elems.iter().copied().map(sparse_ts).collect();
                        vm.set_insert(false, &mut state, &meta, 0, &elems, Some(&ts))
                    }
                }
            };
            match result {
                ErrorCode::Ok => prop_assert!(
                    op.is_some(),
                    "the flood asks for more chunks than the payload holds"
                ),
                ErrorCode::CapacityExceeded => {
                    saw_payload_refusal = true;
                    // Distinct keys stay under `meta.capacity`, so the element-cap
                    // branch that deliberately commits what fits is unreachable.
                    // If it were not, it would leave `last_error` at zero and fail
                    // the byte comparison below rather than pass unnoticed.
                    prop_assert_ne!(
                        vm.bitmap_env.last_error,
                        0,
                        "a payload refusal must leave a diagnostic behind"
                    );
                    prop_assert_eq!(
                        &state,
                        &before,
                        "refused op wrote slot bytes or the eviction index"
                    );
                    prop_assert_eq!(
                        vm.undo_checkpoint(),
                        undo_before,
                        "refused op appended undo records"
                    );
                }
                other => prop_assert!(false, "unexpected bitmap result {other:?}"),
            }
        }
        prop_assert!(saw_payload_refusal);

        vm.undo_rollback(&mut state, checkpoint);
        prop_assert_eq!(vm.undo_checkpoint(), checkpoint);

        // Slot bytes and the metadata record are byte-exact: the image is a
        // pure function of the set and the payload tail stays zero.
        let storage = get_bitmap_storage(&meta);
        let slot = meta.offset as usize
            ..(storage.payload_offset() + storage.payload_capacity) as usize;
        prop_assert_eq!(&state[slot.clone()], &initial[slot]);
        let record = meta.meta_base as usize..(meta.meta_base + SLOT_META_SIZE) as usize;
        prop_assert_eq!(&state[record.clone()], &initial[record]);

        // The eviction index is a length-prefixed array, and
        // `remove_eviction_entries_for_key` compacts without clearing the tail,
        // so entries past `size` are outside the structure — the same rule that
        // lets a rolled-back hash table keep stale bytes in dead cells.
        let size = meta.eviction_index_size(&state);
        prop_assert_eq!(size, meta.eviction_index_size(&initial));
        let base = meta.eviction_index_offset(&state) as usize;
        let live = base..base + (size * EVICTION_ENTRY_SIZE) as usize;
        prop_assert_eq!(&state[live.clone()], &initial[live]);
    }
}

/// A TTL refresh of an element that is already present rewrites its eviction
/// timestamp without appending an undo record, so rollback cannot restore the
/// previous one. `hashset_ops::batch_set_insert` refreshes the same way, so this
/// is one policy rather than two — but it is a policy, not an accident, and the
/// reversibility property above keeps timestamps key-derived to stay inside it.
///
/// Recording it needs an undo op for "member stays, timestamp reverts", which is
/// a new `FlatUndoOp` wire value and therefore a cross-slice change.
#[test]
fn ttl_refresh_of_present_member_records_no_undo_entry() {
    let prog = ttl_bitmap_program(128);
    let mut state = init_program_state(&prog);
    let meta = SlotMetaView::read(&state, 0);
    let mut vm = Vm::default();

    let elem = [sparse_key(9)];
    assert_eq!(
        vm.set_insert(false, &mut state, &meta, 0, &elem, Some(&[100.0])),
        ErrorCode::Ok
    );

    vm.undo_enable(&state);
    let checkpoint = vm.undo_checkpoint();
    assert_eq!(
        find_latest_eviction_timestamp_for_key(&state, &meta, elem[0]),
        Some(100.0)
    );

    assert_eq!(
        vm.set_insert(false, &mut state, &meta, 0, &elem, Some(&[500.0])),
        ErrorCode::Ok
    );
    assert_eq!(
        find_latest_eviction_timestamp_for_key(&state, &meta, elem[0]),
        Some(500.0)
    );
    assert_eq!(
        vm.undo_checkpoint(),
        checkpoint,
        "the refresh appended no undo record"
    );

    vm.undo_rollback(&mut state, checkpoint);
    assert_eq!(
        find_latest_eviction_timestamp_for_key(&state, &meta, elem[0]),
        Some(500.0),
        "rollback leaves the refreshed timestamp in place"
    );
}

proptest! {
    /// A patch into a slot at boundary payload capacities — the region around
    /// BITMAP_BASE_BYTES and the exact payload_capacity edge — either lands
    /// the canonical image or refuses without touching a byte.
    #[test]
    fn patch_boundary_capacities(
        elems in prop::collection::btree_set(0u32..100_000, 0..600),
        cap in 0u32..80,
    ) {
        let mut env = BitmapEnv::default();
        let payload_cap = bitmap_payload_capacity(cap);
        let buf_len = (BITMAP_SERIALIZED_LEN_BYTES + payload_cap) as usize;
        let mut buf = vec![0u8; buf_len];
        let storage = make_storage(buf_len as u32);
        let adds: Vec<u32> = elems.iter().copied().collect();

        match bitmap_patch(&mut env, &mut buf, storage, &adds, &[]) {
            Ok(report) => {
                prop_assert_eq!(report.added as usize, elems.len());
                prop_assert_eq!(members(&buf, storage), adds);
                assert_slot_is_canonical(&buf, storage, &elems);
            }
            Err(ErrorCode::CapacityExceeded) => {
                // Refusal must leave the slot bytes untouched (still empty).
                prop_assert!(buf.iter().all(|&b| b == 0));
                let needed = if elems.is_empty() { 0 } else {
                    Axroar::from_sorted(adds.iter().copied()).forest_len()
                };
                prop_assert!(needed > payload_cap as usize);
            }
            Err(other) => prop_assert!(false, "unexpected error {other:?}"),
        }
    }

    /// Slot-level algebra agrees with BTreeSet model algebra, and the slot
    /// image is the canonical forest of the result.
    #[test]
    fn slot_algebra_matches_set_model(
        a in prop::collection::btree_set(0u32..10_000, 0..64),
        b in prop::collection::btree_set(0u32..10_000, 0..64),
        op_idx in 0usize..4,
    ) {
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
        prop_assert_eq!(
            batch_bitmap_algebra(&mut env, &mut NoVm, op, &mut state, &meta, BitmapSource::Bytes(&source)),
            ErrorCode::Ok
        );

        let expected: BTreeSet<u32> = match op {
            BitmapAlgebraOp::And => a.intersection(&b).copied().collect(),
            BitmapAlgebraOp::Or => a.union(&b).copied().collect(),
            BitmapAlgebraOp::AndNot => a.difference(&b).copied().collect(),
            BitmapAlgebraOp::Xor => a.symmetric_difference(&b).copied().collect(),
        };
        let storage = get_bitmap_storage(&meta);
        let got: BTreeSet<u32> = members(&state, storage).into_iter().collect();
        prop_assert_eq!(&got, &expected);
        prop_assert_eq!(meta.size(&state) as usize, expected.len());
        assert_slot_is_canonical(&state, storage, &expected);
    }

    /// Batch add/remove interleavings agree with a BTreeSet model, including
    /// meta.size — the observable slot contract — and the slot image is the
    /// canonical forest of the surviving set after every batch.
    #[test]
    fn add_remove_matches_set_model(
        batches in prop::collection::vec(
            (prop::collection::vec(0u32..5_000, 1..32), any::<bool>()),
            1..12
        ),
    ) {
        let mut env = BitmapEnv::default();
        let mut state = vec![0u8; 1 << 20];
        let meta = init_bitmap_slot_state(&mut state, 16_384);
        let mut model = BTreeSet::new();
        let storage = get_bitmap_storage(&meta);

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
            prop_assert_eq!(meta.size(&state) as usize, model.len());
            assert_slot_is_canonical(&state, storage, &model);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// The slot image is a pure function of the set across histories that
    /// cross the 4096 array/words boundary within a chunk in both directions,
    /// and every read answers the model: membership, rank/select through the
    /// iterator, and intersections against a second set.
    #[test]
    fn image_is_a_pure_function_of_the_set_across_the_words_boundary(
        inserts in prop::collection::btree_set(0u32..7000, 0..6000),
        dense_len in 0u32..5000,
        removes in prop::collection::vec(0u32..7000, 0..3000),
        reinserts in prop::collection::vec(0u32..7000, 0..200),
        sparse in prop::collection::btree_set(any::<u32>(), 0..64),
        other in prop::collection::btree_set(0u32..7000, 0..300),
        probes in prop::collection::vec(any::<u32>(), 0..16),
    ) {
        let mut env = BitmapEnv::default();
        let mut state = vec![0u8; 1 << 20];
        let meta = init_bitmap_slot_state(&mut state, 65_536);
        let storage = get_bitmap_storage(&meta);
        let mut model: BTreeSet<u32> = BTreeSet::new();

        let apply = |env: &mut BitmapEnv, state: &mut Vec<u8>, model: &mut BTreeSet<u32>, batch: &[u32], insert: bool| {
            let sentinel_free: Vec<u32> = batch.iter().copied().filter(|&v| v < u32::MAX - 1).collect();
            let code = if insert {
                batch_bitmap_add(env, &mut NoVm, false, state, &meta, 0, &sentinel_free, None)
            } else {
                batch_bitmap_remove(env, &mut NoVm, false, state, &meta, 0, &sentinel_free)
            };
            prop_assert_eq!(code, ErrorCode::Ok);
            for &v in &sentinel_free {
                if insert { model.insert(v); } else { model.remove(&v); }
            }
            Ok(())
        };
        let first: Vec<u32> = inserts.iter().chain(sparse.iter()).copied().collect();
        apply(&mut env, &mut state, &mut model, &first, true)?;
        let dense: Vec<u32> = (0..dense_len).collect();
        apply(&mut env, &mut state, &mut model, &dense, true)?;
        apply(&mut env, &mut state, &mut model, &removes, false)?;
        apply(&mut env, &mut state, &mut model, &reinserts, true)?;
        let drop_sparse: Vec<u32> = sparse.iter().take(32).copied().collect();
        apply(&mut env, &mut state, &mut model, &drop_sparse, false)?;

        prop_assert_eq!(meta.size(&state) as usize, model.len());
        assert_slot_is_canonical(&state, storage, &model);

        let ordered: Vec<u32> = model.iter().copied().collect();
        prop_assert_eq!(members(&state, storage), ordered.clone());
        for (rank, &expected) in ordered.iter().enumerate() {
            prop_assert_eq!(bitmap_select(&state, storage, rank as u32), Some(expected));
        }
        prop_assert_eq!(bitmap_select(&state, storage, ordered.len() as u32), None);
        let image = storage.serialized_data(&state).map(<[u8]>::to_vec).unwrap_or_default();
        for &probe in probes.iter().chain(ordered.iter()) {
            prop_assert_eq!(contains_serialized(&image, probe), model.contains(&probe));
        }
        prop_assert_eq!(cardinality_serialized(&image) as usize, model.len());
        let other_image = serialize(&other.iter().copied().collect::<Vec<_>>());
        let common = model.intersection(&other).count();
        prop_assert_eq!(intersect_count_serialized(&image, &other_image) as usize, common);
        prop_assert_eq!(intersects_serialized(&image, &other_image), common > 0);
        let mut out = vec![0u32; ordered.len() + 1];
        prop_assert_eq!(extract_serialized(&image, &mut out) as usize, ordered.len());
        prop_assert_eq!(&out[..ordered.len()], &ordered[..]);

        // A truncated image is refused, never indexed past its end.
        for cut in [0, 4, 8, image.len() / 2, image.len().saturating_sub(1)] {
            prop_assert!(!contains_serialized(&image[..cut], 0) || image.is_empty());
            prop_assert_eq!(cardinality_validated(&image[..cut]).filter(|_| cut > 0), None);
        }
    }
}
