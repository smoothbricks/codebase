//! Coverage for the BITMAP slot: native-image storage, in-place patch
//! mutation, algebra, capacity refusals, and the differential oracle — a
//! `BTreeSet` model against the slot bytes, with the slot image required to
//! be byte-equal to a fresh forest freeze of the surviving set.

use bitmosaic::{Bitmosaic, BitmosaicView, EMPTY_U32_IMAGE, EMPTY_U64_IMAGE, IMAGE_ID_LEN};
use columine_vm::bitmap_ops::{
    BitmapAlgebraOp, BitmapEnv, BitmapSource, BitmapStorage, batch_bitmap_add,
    batch_bitmap_algebra, batch_bitmap_remove, bitmap_import, bitmap_patch,
    bitmap_payload_capacity, bitmap_select, cardinality_serialized, cardinality_validated,
    contains_serialized, extract_serialized, get_bitmap_storage, intersect_count_serialized,
    intersects_serialized, set_algebra,
};
use columine_vm::hooks::NoVm;
use columine_vm::meta::SlotMetaView;
use columine_vm::state_init::{
    DEFAULT_ACCEPTED_PROGRAM_MAGICS, EVICTED_BUFFER_CAP, EVICTION_ENTRY_SIZE,
    calculate_grown_state_size, calculate_state_size, grow_state, init_state,
};
use columine_vm::undo_log::{FlatUndoEntry, FlatUndoOp};
use columine_vm::vm::{
    UNDO_CAPACITY, UNDO_POSITION_OVERFLOWED, Vm, find_latest_eviction_timestamp_for_key,
};
use proptest::prelude::*;

use columine_types::types::{
    ChangeFlag, ErrorCode, Opcode, PROGRAM_MAGIC, SLOT_META_SIZE, STATE_FORMAT_VERSION,
    STATE_HEADER_SIZE, STATE_MAGIC, SlotMetaOffset, SlotType, StateHeaderOffset,
};
use std::collections::BTreeSet;

/// A storage view over a local buffer: the native `[image][spare …]` region
/// at offset 0; the whole buffer is image capacity.
fn make_storage(buf_len: u32) -> BitmapStorage {
    BitmapStorage {
        data_offset: 0,
        payload_capacity: buf_len,
    }
}

/// A zeroed buffer admitted as the canonical empty image: every raw-buffer
/// test starts here, since all-zero bytes are no native encoding.
fn empty_buffer(buf_len: u32) -> (Vec<u8>, BitmapStorage) {
    let mut buf = vec![0u8; buf_len as usize];
    buf[..EMPTY_U32_IMAGE.len()].copy_from_slice(&EMPTY_U32_IMAGE);
    (buf, make_storage(buf_len))
}

/// Minimal state with one BITMAP slot:
/// `[STATE_HEADER (32)][SLOT_META (48)][native image …]`, the image region
/// admitted as the canonical empty image like `init_state` does.
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
    let start = slot_data_offset as usize;
    state[start..start + EMPTY_U32_IMAGE.len()].copy_from_slice(&EMPTY_U32_IMAGE);

    SlotMetaView::read(state, 0)
}

// ---------------------------------------------------------------------------
// Real-VM scaffolding: a program-initialized TTL BITMAP slot, so the eviction
// index and the payload region are laid out by `init_state` rather than by the
// test hand-writing metadata offsets.
// ---------------------------------------------------------------------------

/// `SlotTypeFlags::HAS_TTL_MASK`, which is private to `columine-types`.
const TTL_FLAG: u8 = 0x10;

/// Assemble a program: 32-byte hash prefix + 24-byte content header + init and
/// reduce sections, each NUL-terminated.
fn program(num_slots: u8, num_inputs: u8, init: &[u8], reduce: &[u8]) -> Vec<u8> {
    let mut init = init.to_vec();
    init.push(Opcode::Halt as u8);
    let mut reduce = reduce.to_vec();
    reduce.push(Opcode::Halt as u8);
    let mut prog = vec![0u8; 32];
    prog.extend(PROGRAM_MAGIC.to_le_bytes());
    prog.extend([2, 0, 0, 0]); // version 2, no callbacks, no flags
    prog.extend(u32::from(num_slots).to_le_bytes());
    prog.extend(u32::from(num_inputs).to_le_bytes());
    prog.extend((init.len() as u32).to_le_bytes());
    prog.extend((reduce.len() as u32).to_le_bytes());
    prog.extend(init);
    prog.extend(reduce);
    prog
}

/// One TTL BITMAP slot fed by BATCH_SET_INSERT. `requested` is the program's
/// capacity request; initialization normalizes it to `next_power_of_2(2n)`.
fn ttl_bitmap_program(requested: u16) -> Vec<u8> {
    ttl_bitmap_program_flags(requested, TTL_FLAG)
}

fn ttl_bitmap_program_flags(requested: u16, flags: u8) -> Vec<u8> {
    let mut def = vec![
        Opcode::SlotDef as u8,
        0,
        SlotType::Bitmap as u8 | flags,
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
    let size = calculate_state_size(prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS)
        .expect("valid program admission");
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

/// Deterministic stamps for the randomized membership/refusal sequence.
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
    Bitmosaic::from_sorted(sorted).to_forest_bytes()
}

/// The slot's members, read through the zero-copy view.
fn members(state: &[u8], storage: BitmapStorage) -> Vec<u32> {
    match storage.serialized_data(state) {
        Ok(data) => BitmosaicView::open(data)
            .expect("slot holds a well-formed native image")
            .range()
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// The slot invariant: the payload holds the forest image of exactly `expected`
/// at its head with the self-delimited length agreeing, and every byte past
/// the image is zero — so the image is a pure function of the set.
fn assert_slot_is_canonical(state: &[u8], storage: BitmapStorage, expected: &BTreeSet<u32>) {
    let payload = storage.payload_offset() as usize;
    let capacity = storage.payload_capacity as usize;
    let len = storage
        .serialized_len(state)
        .expect("canonical image parses") as usize;
    let fresh = if expected.is_empty() {
        EMPTY_U32_IMAGE.to_vec()
    } else {
        Bitmosaic::from_sorted(expected.iter().copied()).to_forest_bytes()
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
        "payload past the image is zero"
    );
}

// ---------------------------------------------------------------------------
// Storage basics
// ---------------------------------------------------------------------------

#[test]
fn empty_slot_reads_as_the_empty_set() {
    let (buf, storage) = empty_buffer(4096);
    assert_eq!(
        storage.serialized_len(&buf),
        Ok(IMAGE_ID_LEN as u32),
        "the empty image is four bytes"
    );
    assert_eq!(&buf[..IMAGE_ID_LEN], &EMPTY_U32_IMAGE);
    assert!(storage.view(&buf).expect("empty is well-formed").is_none());
    assert_eq!(bitmap_select(&buf, storage, 0), None);
}

#[test]
fn all_zero_bytes_are_no_native_encoding() {
    let buf = vec![0u8; 4096];
    let storage = make_storage(4096);
    assert_eq!(storage.serialized_len(&buf), Err(ErrorCode::InvalidState));
    assert!(storage.serialized_data(&buf).is_err());
    assert!(storage.view(&buf).is_err());
    assert_eq!(bitmap_select(&buf, storage, 0), None);
}

#[test]
fn patch_round_trip_preserves_elements() {
    let mut env = BitmapEnv::default();
    let (mut buf, storage) = empty_buffer(8192);

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
    let (mut buf, storage) = empty_buffer(65536);
    let elems: Vec<u32> = (1..=500).collect();
    bitmap_patch(&mut env, &mut buf, storage, &elems, &[]).expect("patch");
    assert_eq!(members(&buf, storage), elems);
}

#[test]
fn removing_the_last_member_restores_the_empty_form() {
    let mut env = BitmapEnv::default();
    let (mut buf, storage) = empty_buffer(8192);
    bitmap_patch(&mut env, &mut buf, storage, &[7, 42], &[]).expect("patch");
    let report = bitmap_patch(&mut env, &mut buf, storage, &[], &[7, 42]).expect("patch");
    assert_eq!(report.removed, 2);
    assert_eq!(report.len, 0);
    assert_eq!(storage.serialized_len(&buf), Ok(IMAGE_ID_LEN as u32));
    assert_eq!(&buf[..IMAGE_ID_LEN], &EMPTY_U32_IMAGE);
    assert!(
        buf[IMAGE_ID_LEN..].iter().all(|&b| b == 0),
        "only the empty image remains"
    );
}

#[test]
fn bitmap_select_returns_element_at_rank() {
    let mut env = BitmapEnv::default();
    let (mut buf, storage) = empty_buffer(8192);
    bitmap_patch(&mut env, &mut buf, storage, &[10, 20, 30, 40, 50], &[]).expect("patch");

    assert_eq!(bitmap_select(&buf, storage, 0), Some(10));
    assert_eq!(bitmap_select(&buf, storage, 2), Some(30));
    assert_eq!(bitmap_select(&buf, storage, 4), Some(50));
    assert_eq!(bitmap_select(&buf, storage, 5), None);
}

#[test]
fn capacity_zero_slot_holds_exactly_the_empty_image() {
    // No per-element reservation: the four-byte empty image is the whole slot.
    assert_eq!(bitmap_payload_capacity(0), IMAGE_ID_LEN as u32);
    let (mut buf, storage) = empty_buffer(bitmap_payload_capacity(0));
    assert_slot_is_canonical(&buf, storage, &BTreeSet::new());
    // Any member needs more than the empty image: the refusal is atomic and
    // names growth.
    let mut env = BitmapEnv::default();
    let before = buf.clone();
    assert_eq!(
        bitmap_patch(&mut env, &mut buf, storage, &[7], &[]),
        Err(ErrorCode::CapacityExceeded)
    );
    assert_eq!(env.last_error, 60);
    assert_eq!(buf, before);
}

/// The smallest bitmap a program can declare. SLOT_DEF normalization is
/// `next_power_of_2(requested * 2)` and `next_power_of_2` floors at 16
/// (`columine_types::types::next_power_of_2`, shared by every slot type and
/// unchanged here), so any requested capacity up to 8 lands on 16 elements —
/// and the slot's whole data region is then the image region alone,
/// `16 * 4 + IMAGE_ID_LEN = 68` bytes, where the old layout spent
/// `8 + (16 * 4 + 256) = 328`. A 12-byte region is the capacity-2 boundary of
/// a directly driven slot, which no declaration reaches; the two pairs are
/// different baselines.
#[test]
fn the_smallest_declarable_bitmap_slot_is_the_image_region_alone() {
    for requested in [1u8, 8] {
        let def = vec![
            Opcode::SlotDef as u8,
            0,
            SlotType::Bitmap as u8,
            requested,
            0,
        ];
        let prog = program(1, 0, &def, &[]);
        let state = init_program_state(&prog);
        let meta = SlotMetaView::read(&state, 0);
        assert_eq!(meta.capacity, 16, "requested {requested} normalizes to 16");
        let storage = get_bitmap_storage(&meta);
        assert_eq!(storage.data_offset, meta.offset);
        assert_eq!(storage.payload_offset(), meta.offset);
        assert_eq!(storage.payload_capacity, 68);
        assert_eq!(storage.serialized_len(&state), Ok(IMAGE_ID_LEN as u32));
        assert!(storage.view(&state).expect("fresh slot parses").is_none());
        assert_slot_is_canonical(&state, storage, &BTreeSet::new());
    }
}

#[test]
fn get_bitmap_storage_returns_correct_offsets_and_capacity() {
    let mut state = vec![0u8; 8192];
    let capacity = 64u32;
    let meta = init_bitmap_slot_state(&mut state, capacity);

    let storage = get_bitmap_storage(&meta);
    let slot_data_offset = STATE_HEADER_SIZE + SLOT_META_SIZE;
    assert_eq!(storage.data_offset, slot_data_offset);
    assert_eq!(storage.payload_offset(), slot_data_offset);
    assert_eq!(storage.payload_capacity, bitmap_payload_capacity(capacity));
}

// ---------------------------------------------------------------------------
// Batch mutation
// ---------------------------------------------------------------------------

#[test]
fn batch_bitmap_add_inserts_5_elements() {
    let mut env = BitmapEnv::default();
    let mut hooks = NoVm::default();
    let mut state = vec![0u8; 65536];
    let meta = init_bitmap_slot_state(&mut state, 128);
    let storage = get_bitmap_storage(&meta);

    let elems = [100u32, 200, 300, 400, 500];
    let result = batch_bitmap_add(&mut env, &mut hooks, false, &mut state, &meta, &elems, None);
    assert_eq!(result, ErrorCode::Ok);
    assert_eq!(meta.size(&state), 5);
    assert_eq!(members(&state, storage), elems);
}

#[test]
fn batch_bitmap_add_dedups_same_element() {
    let mut env = BitmapEnv::default();
    let mut hooks = NoVm::default();
    let mut state = vec![0u8; 65536];
    let meta = init_bitmap_slot_state(&mut state, 128);

    let elems = [42u32, 42];
    let result = batch_bitmap_add(&mut env, &mut hooks, false, &mut state, &meta, &elems, None);
    assert_eq!(result, ErrorCode::Ok);
    assert_eq!(meta.size(&state), 1);
}

#[test]
fn batch_bitmap_add_accepts_unsorted_input() {
    let mut env = BitmapEnv::default();
    let mut hooks = NoVm::default();
    let mut state = vec![0u8; 65536];
    let meta = init_bitmap_slot_state(&mut state, 128);
    let storage = get_bitmap_storage(&meta);
    let elems = [500u32, 3, 70_000, 3, 12];
    assert_eq!(
        batch_bitmap_add(&mut env, &mut hooks, false, &mut state, &meta, &elems, None),
        ErrorCode::Ok
    );
    assert_eq!(members(&state, storage), [3, 12, 500, 70_000]);
    assert_eq!(meta.size(&state), 4);
}

#[test]
fn batch_bitmap_remove_removes_elements_correctly() {
    let mut env = BitmapEnv::default();
    let mut hooks = NoVm::default();
    let mut state = vec![0u8; 65536];
    let meta = init_bitmap_slot_state(&mut state, 128);
    let storage = get_bitmap_storage(&meta);

    let add = [10u32, 20, 30, 40, 50];
    batch_bitmap_add(&mut env, &mut hooks, false, &mut state, &meta, &add, None);
    assert_eq!(meta.size(&state), 5);

    let rm = [40u32, 20, 20, 99];
    batch_bitmap_remove(&mut env, &mut hooks, false, &mut state, &meta, &rm);
    assert_eq!(meta.size(&state), 3);
    assert_eq!(members(&state, storage), [10, 30, 50]);
}

// ---------------------------------------------------------------------------
// Run containers and growth, driven through the real slot entry points.
// ---------------------------------------------------------------------------

/// The arm a fresh freeze of `model` selects — `(stride, cone, array, words,
/// runs)`. The slot image is byte-equal to that freeze, so this names the
/// container the slot itself holds.
fn census(model: &BTreeSet<u32>) -> (usize, usize, usize, usize, usize) {
    Bitmosaic::from_sorted(model.iter().copied()).container_census()
}

/// One batch through the real entry points, checked against the model: the
/// observable size, the canonical image, ascending members, and select at
/// both ends and one past them.
#[allow(clippy::too_many_arguments)]
fn slot_step(
    env: &mut BitmapEnv,
    hooks: &mut NoVm,
    state: &mut [u8],
    meta: &SlotMetaView,
    storage: BitmapStorage,
    model: &mut BTreeSet<u32>,
    values: &[u32],
    insert: bool,
) {
    let code = if insert {
        batch_bitmap_add(env, hooks, false, state, meta, values, None)
    } else {
        batch_bitmap_remove(env, hooks, false, state, meta, values)
    };
    assert_eq!(code, ErrorCode::Ok);
    for &v in values {
        if insert {
            model.insert(v);
        } else {
            model.remove(&v);
        }
    }
    assert_eq!(meta.size(state) as usize, model.len());
    assert_slot_is_canonical(state, storage, model);
    let ordered: Vec<u32> = model.iter().copied().collect();
    assert_eq!(members(state, storage), ordered);
    assert_eq!(bitmap_select(state, storage, 0), ordered.first().copied());
    if let Some(&last) = ordered.last() {
        assert_eq!(
            bitmap_select(state, storage, ordered.len() as u32 - 1),
            Some(last)
        );
    }
    assert_eq!(bitmap_select(state, storage, ordered.len() as u32), None);
}

/// Split, bridge, extend and erase histories over an interval-rich chunk. The
/// container the slot keeps is a function of the set, not of the history, so
/// bridging a split restores the earlier image byte for byte — including
/// after a fill that made the run count lose to the Words arm.
#[test]
fn run_container_histories_split_bridge_extend_and_erase() {
    let mut env = BitmapEnv::default();
    let mut hooks = NoVm::default();
    let mut state = vec![0u8; 1 << 20];
    let meta = init_bitmap_slot_state(&mut state, 65_536);
    let storage = get_bitmap_storage(&meta);
    let mut model: BTreeSet<u32> = BTreeSet::new();

    // The format's worked example: 8192 members in two maximal intervals.
    // Two runs cost 12 owned bytes against the Words arm's 8720, so the
    // chunk is priced as Runs, and the whole image is 20 bytes — 4
    // identifier, 1 body length, and a 15-byte body (2 cardinality, 1 chunk
    // count, 2 key, 1 kind plane, 9 runs payload).
    let two_runs: Vec<u32> = (0..4096).chain(32768..36864).collect();
    slot_step(
        &mut env, &mut hooks, &mut state, &meta, storage, &mut model, &two_runs, true,
    );
    assert_eq!(
        census(&model),
        (0, 0, 0, 0, 1),
        "two intervals freeze to one Runs container"
    );
    assert_eq!(storage.serialized_len(&state), Ok(20));
    let two_run_image = storage
        .serialized_data(&state)
        .expect("image parses")
        .to_vec();

    // Split: one interior removal turns two runs into three.
    slot_step(
        &mut env,
        &mut hooks,
        &mut state,
        &meta,
        storage,
        &mut model,
        &[2000],
        false,
    );
    assert_eq!(census(&model), (0, 0, 0, 0, 1));
    let split = storage.serialized_data(&state).expect("image parses");
    assert!(!contains_serialized(split, 2000));
    assert!(contains_serialized(split, 1999));
    assert!(contains_serialized(split, 2001));

    // Bridge: adding it back is the two-run image again.
    slot_step(
        &mut env,
        &mut hooks,
        &mut state,
        &meta,
        storage,
        &mut model,
        &[2000],
        true,
    );
    assert_eq!(
        storage.serialized_data(&state).expect("image parses"),
        &two_run_image[..]
    );

    // A scattered fill of the gap costs 6 bytes per run and loses to Words;
    // removing it returns the same two-run image.
    let mut fill = BTreeSet::new();
    let mut i = 0u32;
    while fill.len() < 3000 {
        let v = i.wrapping_mul(40_503) % 65_536;
        if (4096..32768).contains(&v) {
            fill.insert(v);
        }
        i += 1;
    }
    let fill: Vec<u32> = fill.into_iter().collect();
    slot_step(
        &mut env, &mut hooks, &mut state, &meta, storage, &mut model, &fill, true,
    );
    assert_eq!(
        census(&model),
        (0, 0, 0, 1, 0),
        "thousands of runs lose to the dense plane"
    );
    slot_step(
        &mut env, &mut hooks, &mut state, &meta, storage, &mut model, &fill, false,
    );
    assert_eq!(
        storage.serialized_data(&state).expect("image parses"),
        &two_run_image[..]
    );

    // Extend: the upper run grows at its top end.
    slot_step(
        &mut env,
        &mut hooks,
        &mut state,
        &meta,
        storage,
        &mut model,
        &[36_864, 36_865],
        true,
    );
    assert_eq!(census(&model), (0, 0, 0, 0, 1));

    // Bridge the whole gap: one maximal interval is a stride, not a run list.
    let gap: Vec<u32> = (4096..32768).collect();
    slot_step(
        &mut env, &mut hooks, &mut state, &meta, storage, &mut model, &gap, true,
    );
    assert_eq!(model.len(), 36_866);
    assert_eq!(
        census(&model),
        (1, 0, 0, 0, 0),
        "a single interval stays the closed-form stride"
    );

    // Erase inside the interval: the stride splits back into runs.
    slot_step(
        &mut env,
        &mut hooks,
        &mut state,
        &meta,
        storage,
        &mut model,
        &[20_000],
        false,
    );
    assert_eq!(census(&model), (0, 0, 0, 0, 1));

    // Erase everything: the canonical four-byte empty image and nothing else.
    let all: Vec<u32> = model.iter().copied().collect();
    slot_step(
        &mut env, &mut hooks, &mut state, &meta, storage, &mut model, &all, false,
    );
    assert!(model.is_empty());
    assert_eq!(storage.serialized_len(&state), Ok(IMAGE_ID_LEN as u32));
    let payload = storage.payload_offset() as usize;
    assert_eq!(&state[payload..payload + IMAGE_ID_LEN], &EMPTY_U32_IMAGE);
}

/// Growth copies the image the old header delimits and nothing else: the new
/// capacity is zeroed first, so the grown slot is byte-identical to a fresh
/// freeze of the same set with a zero tail, and it keeps mutating from there.
#[test]
fn growth_copies_only_the_image_and_zeroes_the_new_capacity() {
    let def = vec![Opcode::SlotDef as u8, 0, SlotType::Bitmap as u8, 8, 0];
    let prog = program(1, 0, &def, &[]);
    let mut state = init_program_state(&prog);
    let meta = SlotMetaView::read(&state, 0);
    assert_eq!(meta.capacity, 16, "requested 8 normalizes to 16");
    let storage = get_bitmap_storage(&meta);

    let mut env = BitmapEnv::default();
    let mut hooks = NoVm::default();
    let seed: Vec<u32> = (0..16u32).map(|i| i * 3 + 1).collect();
    let mut model: BTreeSet<u32> = BTreeSet::new();
    slot_step(
        &mut env, &mut hooks, &mut state, &meta, storage, &mut model, &seed, true,
    );
    let image = storage
        .serialized_data(&state)
        .expect("image parses")
        .to_vec();

    let grown_size = calculate_grown_state_size(&state, 0).expect("valid growth geometry") as usize;
    let mut grown = vec![0u8; grown_size];
    // Dirty exactly the region the grown bitmap slot will own: growth must
    // zero it, not inherit it. The slot offset is unchanged because the slot
    // count is.
    let dirty = meta.offset as usize;
    grown[dirty..dirty + bitmap_payload_capacity(32) as usize].fill(0xa5);
    grow_state(&state, &mut grown, 0).expect("grow");

    let grown_meta = SlotMetaView::read(&grown, 0);
    assert_eq!(grown_meta.capacity, 32);
    assert_eq!(grown_meta.offset, meta.offset);
    assert_eq!(grown_meta.size(&grown) as usize, model.len());
    let grown_storage = get_bitmap_storage(&grown_meta);
    assert_eq!(grown_storage.payload_capacity, bitmap_payload_capacity(32));
    assert_eq!(
        grown_storage.serialized_len(&grown),
        Ok(image.len() as u32),
        "the copied image is the one the old header delimited"
    );
    assert_eq!(
        grown_storage.serialized_data(&grown).expect("image parses"),
        &image[..]
    );
    // Canonical: image at the head, every byte of the doubled capacity past
    // it zero.
    assert_slot_is_canonical(&grown, grown_storage, &model);

    // The grown slot mutates from the copied image, into the new capacity.
    let more: Vec<u32> = (0..16u32).map(|i| i * 3 + 2).collect();
    slot_step(
        &mut env,
        &mut hooks,
        &mut grown,
        &grown_meta,
        grown_storage,
        &mut model,
        &more,
        true,
    );
    assert_eq!(model.len(), 32);
}

/// Growth never publishes an image it cannot read: an old slot whose header is
/// missing or of the wrong width is a corrupt state, so growth refuses with
/// `InvalidState` and the caller keeps the state it has — rather than handing
/// back a grown slot whose region is all-zero, which is no native encoding.
#[test]
fn growth_refuses_an_unreadable_old_image() {
    for corrupt in [vec![0u8; IMAGE_ID_LEN], EMPTY_U64_IMAGE.to_vec()] {
        let def = vec![Opcode::SlotDef as u8, 0, SlotType::Bitmap as u8, 8, 0];
        let prog = program(1, 0, &def, &[]);
        let mut state = init_program_state(&prog);
        let meta = SlotMetaView::read(&state, 0);
        let payload = meta.offset as usize;
        state[payload..payload + corrupt.len()].copy_from_slice(&corrupt);
        let before = state.clone();

        assert_eq!(
            calculate_grown_state_size(&state, 0),
            Err(ErrorCode::InvalidState)
        );
        let mut grown = vec![0xa5u8; state.len() * 2];
        let destination_before = grown.clone();
        assert_eq!(
            grow_state(&state, &mut grown, 0),
            Err(ErrorCode::InvalidState),
            "a corrupt bitmap slot refuses growth"
        );
        assert_eq!(
            grown, destination_before,
            "refusal precedes the first destination write"
        );
        // The refusal costs only the destination buffer: the live state is
        // exactly as it was, so the caller can report and keep serving it.
        assert_eq!(state, before);
    }
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
    BitmosaicView::open(env.algebra_result())
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
        &Bitmosaic::from_sorted(expected).to_forest_bytes()[..]
    );
}

// ---------------------------------------------------------------------------
// Capacity refusals, slot-level algebra, serialized queries, import.
// ---------------------------------------------------------------------------

/// The element cap is only the binding constraint once the payload can hold
/// that many members: the payload is `capacity * 4 + 4` bytes, and a one-chunk
/// image spends 10 of them on framing (4 identifier, 1 body length, 1
/// cardinality, 1 chunk count, 2 key, 1 kind plane) before a single container
/// byte. This slot is driven directly at capacity 4 (declarations floor at 16)
/// to sit one member above that framing cost: the payload is 20 bytes and the
/// four admitted members encode in 18 (an 8-byte Array), so the refusal here
/// is the element cap and the earliest four are flushed. See
/// `element_cap_below_the_framing_cost_refuses_the_whole_batch` for the slot
/// too small to hold its own element cap.
#[test]
fn batch_bitmap_add_capacity_exceeded_flushes_partial_batch() {
    let mut env = BitmapEnv::default();
    let mut hooks = NoVm::default();
    let mut state = vec![0u8; 65536];
    let meta = init_bitmap_slot_state(&mut state, 4);
    let elems = [5u32, 1, 2, 3, 4];
    let result = batch_bitmap_add(&mut env, &mut hooks, false, &mut state, &meta, &elems, None);
    assert_eq!(result, ErrorCode::CapacityExceeded);
    assert_eq!(meta.size(&state), 4);
    let storage = get_bitmap_storage(&meta);
    assert_eq!(
        members(&state, storage),
        [1, 2, 3, 5],
        "the batch's earliest four by column index"
    );
    assert_slot_is_canonical(&state, storage, &[1, 2, 3, 5].into_iter().collect());
}

/// The storage layer's tightest boundary: an element cap smaller than the
/// framing cost cannot store a full set of admitted members. Capacity 2 prices
/// 12 payload bytes while the two-member image needs 14. No program can
/// declare a capacity this small (normalization floors at 16), so this drives
/// the slot directly — the point is the arithmetic every capacity obeys, and
/// the refusal it produces: the element cap admits the batch, the payload
/// refuses it atomically, nothing is committed, the bytes are untouched, and
/// the slot-growth retry is the only way forward. That is the documented
/// contract for shapes the closed form does not cover, not a partial flush.
#[test]
fn element_cap_below_the_framing_cost_refuses_the_whole_batch() {
    let mut env = BitmapEnv::default();
    let mut hooks = NoVm::default();
    let mut state = vec![0u8; 65536];
    let meta = init_bitmap_slot_state(&mut state, 2);
    let storage = get_bitmap_storage(&meta);
    assert_eq!(storage.payload_capacity, 12);
    assert_eq!(
        Bitmosaic::from_sorted([1u32, 3]).forest_len(),
        14,
        "two members frame to more than the slot prices"
    );
    let before = state.clone();

    let result = batch_bitmap_add(
        &mut env,
        &mut hooks,
        false,
        &mut state,
        &meta,
        &[3u32, 1],
        None,
    );
    assert_eq!(result, ErrorCode::CapacityExceeded);
    assert_eq!(env.last_error, 60, "the payload, not the element cap");
    assert_eq!(meta.size(&state), 0);
    assert_eq!(state, before);
    assert_slot_is_canonical(&state, storage, &BTreeSet::new());

    // One member does fit: 4 + 1 + 1 + 1 + 2 + 1 + 2 = 12 bytes exactly.
    assert_eq!(
        batch_bitmap_add(
            &mut env,
            &mut hooks,
            false,
            &mut state,
            &meta,
            &[3u32],
            None
        ),
        ErrorCode::Ok
    );
    assert_eq!(meta.size(&state), 1);
    assert_slot_is_canonical(&state, storage, &[3].into_iter().collect());
}

#[test]
fn payload_capacity_refusal_leaves_the_slot_untouched() {
    let mut env = BitmapEnv::default();
    let mut hooks = NoVm::default();
    let mut state = vec![0u8; 65536];
    // Element cap 64, payload 64*4+4 = 260 bytes: 60 one-per-chunk members
    // need 60 directory records and 60 container payloads.
    let meta = init_bitmap_slot_state(&mut state, 64);
    let storage = get_bitmap_storage(&meta);
    let seed = [sparse_key(1), sparse_key(2)];
    assert_eq!(
        batch_bitmap_add(&mut env, &mut hooks, false, &mut state, &meta, &seed, None),
        ErrorCode::Ok
    );
    let before = state.clone();
    let flood: Vec<u32> = (3u16..63).map(sparse_key).collect();
    assert_eq!(
        batch_bitmap_add(&mut env, &mut hooks, false, &mut state, &meta, &flood, None),
        ErrorCode::CapacityExceeded
    );
    assert_eq!(env.last_error, 60);
    assert_eq!(state, before);
    assert_eq!(members(&state, storage), seed);

    // The empty slot refuses the same way and keeps the canonical empty image.
    let mut fresh = vec![0u8; 65536];
    let meta = init_bitmap_slot_state(&mut fresh, 64);
    let before = fresh.clone();
    assert_eq!(
        batch_bitmap_add(&mut env, &mut hooks, false, &mut fresh, &meta, &flood, None),
        ErrorCode::CapacityExceeded
    );
    assert_eq!(fresh, before);
}

#[test]
fn batch_bitmap_add_skips_sentinel_keys() {
    let mut env = BitmapEnv::default();
    let mut hooks = NoVm::default();
    let mut state = vec![0u8; 65536];
    let meta = init_bitmap_slot_state(&mut state, 128);
    let elems = [u32::MAX, u32::MAX - 1, 7];
    let result = batch_bitmap_add(&mut env, &mut hooks, false, &mut state, &meta, &elems, None);
    assert_eq!(result, ErrorCode::Ok);
    assert_eq!(meta.size(&state), 1);
}

#[test]
fn slot_algebra_and_with_empty_clears_target() {
    let mut env = BitmapEnv::default();
    let mut hooks = NoVm::default();
    let mut state = vec![0u8; 65536];
    let meta = init_bitmap_slot_state(&mut state, 128);
    let add = [1u32, 2, 3];
    batch_bitmap_add(&mut env, &mut hooks, false, &mut state, &meta, &add, None);
    assert_eq!(meta.size(&state), 3);

    let r = batch_bitmap_algebra(
        &mut env,
        &mut hooks,
        false,
        BitmapAlgebraOp::And,
        &mut state,
        &meta,
        BitmapSource::Bytes(&[]),
    );
    assert_eq!(r, ErrorCode::Ok);
    assert_eq!(meta.size(&state), 0);
    let storage = get_bitmap_storage(&meta);
    assert_eq!(storage.serialized_len(&state), Ok(IMAGE_ID_LEN as u32));
    assert_slot_is_canonical(&state, storage, &BTreeSet::new());

    // OR with empty = no change.
    batch_bitmap_add(&mut env, &mut hooks, false, &mut state, &meta, &add, None);
    let r = batch_bitmap_algebra(
        &mut env,
        &mut hooks,
        false,
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
    let mut hooks = NoVm::default();
    let mut state = vec![0u8; 65536];
    let meta = init_bitmap_slot_state(&mut state, 128);
    batch_bitmap_add(
        &mut env,
        &mut hooks,
        false,
        &mut state,
        &meta,
        &[1, 2, 3],
        None,
    );

    let source = serialize(&[3, 4, 5]);
    let r = batch_bitmap_algebra(
        &mut env,
        &mut hooks,
        false,
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
    let mut hooks = NoVm::default();
    let mut state = vec![0u8; 65536];
    let meta = init_bitmap_slot_state(&mut state, 128);
    batch_bitmap_add(
        &mut env,
        &mut hooks,
        false,
        &mut state,
        &meta,
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
        &mut hooks,
        false,
        BitmapAlgebraOp::AndNot,
        &mut state,
        &meta,
        BitmapSource::Scratch,
    );
    assert_eq!(r, ErrorCode::Ok);
    assert_eq!(members(&state, get_bitmap_storage(&meta)), [1, 3]);
}

/// A source slot the reader cannot parse is a corrupt state, not an empty
/// set: AND must refuse rather than clear the target.
#[test]
fn slot_algebra_refuses_a_corrupt_source_slot() {
    let init = [
        Opcode::SlotDef as u8,
        0,
        SlotType::Bitmap as u8,
        64,
        0,
        Opcode::SlotDef as u8,
        1,
        SlotType::Bitmap as u8,
        64,
        0,
    ];
    let prog = program(2, 0, &init, &[]);
    let mut state = init_program_state(&prog);
    let meta = SlotMetaView::read(&state, 0);
    let source_meta = SlotMetaView::read(&state, 1);
    let storage = get_bitmap_storage(&meta);
    let mut vm = Vm::default();
    assert_eq!(
        vm.set_insert(false, &mut state, 0, &[1, 2, 3], None),
        ErrorCode::Ok
    );
    // An actual source slot with invalid native framing, not a forged storage
    // address: authoritative binding must refuse before changing the target.
    state[source_meta.offset as usize] = 0;
    let before = state.clone();
    vm.undo_enable(&state).expect("valid speculation admission");
    let checkpoint = vm.undo_checkpoint();
    assert_eq!(
        vm.bitmap_algebra(
            false,
            BitmapAlgebraOp::And,
            &mut state,
            0,
            BitmapSource::Slot(1),
        ),
        ErrorCode::InvalidState
    );
    assert_eq!(state, before);
    assert_eq!(vm.undo_checkpoint(), checkpoint);
    assert_eq!(members(&state, storage), [1, 2, 3]);
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
    assert_eq!(cardinality_validated(&[]), None);
    assert_eq!(cardinality_validated(&a[..a.len() - 1]), None);
    assert_eq!(cardinality_validated(b"not a native image at all"), None);
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
    let mut hooks = NoVm::default();
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

    // A decision-side operand is usually a whole slot region: the image
    // followed by unused capacity. The survivor copy is the image, not the
    // region, so the scratch result is byte-equal to a fresh encoding.
    let mut state = vec![0u8; 8192];
    let meta = init_bitmap_slot_state(&mut state, 512);
    let storage = get_bitmap_storage(&meta);
    assert_eq!(
        batch_bitmap_add(
            &mut env,
            &mut hooks,
            false,
            &mut state,
            &meta,
            &[1, 2],
            None
        ),
        ErrorCode::Ok
    );
    let payload = storage.payload_offset() as usize;
    let region = &state[payload..payload + storage.payload_capacity as usize];
    assert!(region.len() > a.len(), "the region carries spare capacity");
    assert_eq!(
        set_algebra(&mut env, BitmapAlgebraOp::Or, &[], region),
        ErrorCode::Ok
    );
    assert_eq!(env.algebra_result(), &a[..]);
    assert_eq!(
        set_algebra(&mut env, BitmapAlgebraOp::Xor, region, &[]),
        ErrorCode::Ok
    );
    assert_eq!(env.algebra_result(), &a[..]);
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
    let canonical = Bitmosaic::from_sorted(sparse.iter().copied()).to_bytes();
    assert!(
        BitmosaicView::open(&canonical)
            .expect("well-formed")
            .is_elias_fano()
    );
    assert_eq!(
        bitmap_import(&mut env, &mut state, &meta, &canonical),
        ErrorCode::Ok
    );
    assert_eq!(meta.size(&state), sparse.len() as u32);
    assert_slot_is_canonical(&state, storage, &sparse);

    // A forest image lands as is; the empty image clears the slot.
    let dense: BTreeSet<u32> = (0..5000).collect();
    let forest = Bitmosaic::from_sorted(dense.iter().copied()).to_forest_bytes();
    assert_eq!(
        bitmap_import(&mut env, &mut state, &meta, &forest),
        ErrorCode::Ok
    );
    assert_slot_is_canonical(&state, storage, &dense);
    assert_eq!(
        bitmap_import(&mut env, &mut state, &meta, &EMPTY_U32_IMAGE),
        ErrorCode::Ok
    );
    assert_slot_is_canonical(&state, storage, &BTreeSet::new());
    assert_eq!(meta.size(&state), 0);
    // Zero-length bytes are no native encoding: refused, slot untouched.
    assert_eq!(
        bitmap_import(&mut env, &mut state, &meta, &[]),
        ErrorCode::InvalidState
    );
    assert_eq!(env.last_error, 102);
    assert_slot_is_canonical(&state, storage, &BTreeSet::new());

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
        let prog = ttl_bitmap_program(128);
        let mut state = init_program_state(&prog);
        let initial_state = state.clone();
        let mut vm = Vm::default();
        vm.undo_enable(&state).expect("valid speculation admission");
        let checkpoint = vm.undo_checkpoint();
        let timestamp = f64::from_bits(timestamp_bits);
        let mut saw_payload_refusal = false;

        for high in high_words {
            let key = u32::from(high) << 16;
            let state_before = state.clone();
            let journal_before = vm.undo_checkpoint();
            match vm.set_insert(
                false,
                &mut state,
                0,
                &[key],
                Some(&[timestamp]),
            ) {
                ErrorCode::Ok => {}
                ErrorCode::CapacityExceeded => {
                    saw_payload_refusal = true;
                    prop_assert_eq!(&state, &state_before);
                    prop_assert_eq!(vm.undo_checkpoint(), journal_before);
                }
                other => prop_assert!(false, "unexpected bitmap result {other:?}"),
            }
        }
        prop_assert!(saw_payload_refusal);
        vm.undo_rollback(&mut state, checkpoint).expect("valid checkpoint");
        prop_assert_eq!(state, initial_state);
        prop_assert_eq!(vm.undo_checkpoint(), checkpoint);
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
    /// Refreshes and index compaction are physical writes too: rollback restores
    /// their actual prior bytes, including every previously dead tail entry.
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
            vm.set_insert(false, &mut state, 0, &seed, Some(&seed_ts)),
            ErrorCode::Ok
        );
        prop_assert_eq!(meta.eviction_index_size(&state), 60);

        vm.undo_enable(&state).expect("valid speculation admission");
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
                    0,
                    &flood,
                    Some(&flood_ts),
                ),
                Some((highs, is_remove)) => {
                    let elems: Vec<u32> = highs.iter().copied().map(sparse_key).collect();
                    if *is_remove {
                        vm.set_remove(false, &mut state, 0, &elems)
                    } else {
                        let ts: Vec<f64> = elems.iter().copied().map(sparse_ts).collect();
                        vm.set_insert(false, &mut state, 0, &elems, Some(&ts))
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

        vm.undo_rollback(&mut state, checkpoint).expect("valid checkpoint");
        prop_assert_eq!(vm.undo_checkpoint(), checkpoint);

        prop_assert_eq!(&state, &initial, "rollback restores the complete serialized image");
    }
}

/// Refreshing an existing member journals its actual eviction-index bytes,
/// even though membership and cardinality stay unchanged.
#[test]
fn ttl_refresh_of_present_member_restores_the_exact_image() {
    let prog = ttl_bitmap_program(128);
    let mut state = init_program_state(&prog);
    let meta = SlotMetaView::read(&state, 0);
    let mut vm = Vm::default();

    let elem = [sparse_key(9)];
    assert_eq!(
        vm.set_insert(false, &mut state, 0, &elem, Some(&[100.0])),
        ErrorCode::Ok
    );

    vm.undo_enable(&state).expect("valid speculation admission");
    let checkpoint = vm.undo_checkpoint();
    let initial = state.clone();
    assert_eq!(
        find_latest_eviction_timestamp_for_key(&state, &meta, elem[0]),
        Some(100.0)
    );

    assert_eq!(
        vm.set_insert(false, &mut state, 0, &elem, Some(&[500.0])),
        ErrorCode::Ok
    );
    assert_eq!(
        find_latest_eviction_timestamp_for_key(&state, &meta, elem[0]),
        Some(500.0)
    );
    assert!(
        vm.undo_checkpoint() > checkpoint,
        "the refresh journals physical bytes"
    );

    vm.undo_rollback(&mut state, checkpoint)
        .expect("valid checkpoint");
    assert_eq!(state, initial);
    assert_eq!(
        find_latest_eviction_timestamp_for_key(&state, &meta, elem[0]),
        Some(100.0),
        "rollback restores the prior timestamp"
    );
}

// ---------------------------------------------------------------------------
// Direct set operations through the real `Vm`: each is a change-flag boundary
// of its own, journaled against the same state, container operations and undo
// log a batch uses, with no reducer program running.
// ---------------------------------------------------------------------------

/// One plain BITMAP slot and no reducer, so the whole image is the rollback
/// oracle, not only its live prefix.
fn bitmap_program(requested: u16) -> Vec<u8> {
    let def = [
        Opcode::SlotDef as u8,
        0,
        SlotType::Bitmap as u8,
        requested as u8,
        (requested >> 8) as u8,
    ];
    program(1, 0, &def, &[])
}

/// Undo to `checkpoint`; the restore is a success the test asserts.
fn rollback(vm: &mut Vm, state: &mut [u8], checkpoint: u32) {
    let size = state.len() as u32;
    assert_eq!(
        vm.undo_rollback(state, checkpoint),
        Ok(size),
        "rollback to {checkpoint}"
    );
}

/// Spend actual physical records by changing and restoring one allocated byte.
/// Two- and three-transition cycles end byte-identical, without fake entries.
fn fill_journal_until(vm: &mut Vm, state: &mut [u8], room: u32, delta: bool) {
    let offset = u32::try_from(state.len() - 1).expect("test image fits u32");
    let ranges = [(offset, 1)];
    let original = state[offset as usize];
    let target = UNDO_CAPACITY - room;
    while vm.undo_checkpoint() < target {
        let remaining = target - vm.undo_checkpoint();
        assert!(
            remaining >= 2,
            "a real identity cycle needs at least two records"
        );
        let transitions = if remaining == 3 { 3 } else { 2 };
        for step in 0..transitions {
            let value = if step + 1 == transitions {
                original
            } else {
                original ^ u8::try_from(step + 1).expect("at most three transitions")
            };
            vm.capture_mutation(delta, state, &ranges, |image| {
                image[offset as usize] = value;
            })
            .expect("real prior-byte budget");
        }
    }
    assert_eq!(state[offset as usize], original);
}

/// The complete emitted state, including unused payload and TTL-index tails,
/// is the rollback oracle.
fn assert_ttl_slot_restored(state: &[u8], initial: &[u8], _meta: &SlotMetaView) {
    assert_eq!(state, initial, "complete serialized image");
}

/// Export `[checkpoint, end)` and prove both lanes replay byte-exact on a
/// fresh VM, without touching `vm`'s own journal.
fn assert_delta_segment_replays(vm: &mut Vm, before: &[u8], after: &[u8], checkpoint: u32) {
    let end = vm.undo_checkpoint();
    assert!(end > checkpoint, "the segment journaled");
    assert!(!vm.undo_has_overflow(), "the journal stays exact");
    assert_eq!(
        Ok(end - checkpoint),
        vm.delta_export_segment(checkpoint, end)
    );
    let undo = vm.delta_export_undo().to_vec();
    let redo = vm.delta_export_redo().to_vec();
    let entry = Vm::delta_export_entry_size();
    let size = after.len() as u32;

    let mut replay = Vm::default();
    let mut rolled_back = after.to_vec();
    assert_eq!(
        replay.delta_apply_rollback_segment(&mut rolled_back, &undo, entry),
        Ok(size)
    );
    assert_eq!(rolled_back, before, "exported undo restores the image");

    let mut rolled_forward = before.to_vec();
    assert_eq!(
        replay.delta_apply_rollforward_segment(&mut rolled_forward, &redo, entry),
        Ok(size)
    );
    assert_eq!(rolled_forward, after, "exported redo reproduces the image");
}

/// The shrunk counterexample of the apply-then-undo property, pinned as a
/// fixed case: one refused flood, then one direct remove, through the real
/// `Vm` journal. The remove adds REMOVED to flags the window opened with
/// INSERTED, so the rollback has to give back the flags as well as the
/// members.
#[test]
fn direct_remove_after_a_refused_flood_rolls_back_to_the_exact_record() {
    // Sixty members seeded with undo off, so INSERTED is already set when the
    // window opens.
    let (mut state, meta, mut vm) = seeded_ttl_slot();
    assert_eq!(meta.change_flags(&state), ChangeFlag::INSERTED);

    vm.undo_enable(&state).expect("valid speculation admission");
    let checkpoint = vm.undo_checkpoint();
    let initial = state.clone();

    // A payload refusal commits nothing on any plane, flags included.
    let flood: Vec<u32> = (1u16..240).map(sparse_key).collect();
    let flood_ts: Vec<f64> = flood.iter().copied().map(sparse_ts).collect();
    assert_eq!(
        vm.set_insert(false, &mut state, 0, &flood, Some(&flood_ts)),
        ErrorCode::CapacityExceeded
    );
    assert_eq!(state, initial, "the refusal wrote bytes");
    assert_eq!(vm.undo_checkpoint(), checkpoint, "the refusal journaled");

    assert_eq!(
        vm.set_remove(false, &mut state, 0, &[sparse_key(1)]),
        ErrorCode::Ok
    );
    assert_eq!(
        meta.change_flags(&state),
        ChangeFlag::INSERTED | ChangeFlag::REMOVED
    );
    assert!(vm.undo_checkpoint() > checkpoint);

    rollback(&mut vm, &mut state, checkpoint);
    assert_eq!(vm.undo_checkpoint(), checkpoint);
    assert_eq!(meta.change_flags(&state), ChangeFlag::INSERTED);
    assert_ttl_slot_restored(&state, &initial, &meta);
}

/// Savepoints between direct set operations: each operation is a boundary, so
/// rolling back the later segment restores the flags it moved and leaves the
/// earlier segment applied. The delta lane exports the same flag transitions
/// as an exact undo and redo pair.
#[test]
fn nested_direct_savepoints_restore_the_flags_each_segment_moved() {
    let prog = bitmap_program(128);
    for delta in [false, true] {
        let mut state = init_program_state(&prog);
        let meta = SlotMetaView::read(&state, 0);
        assert_eq!(meta.change_flags(&state), 0, "a fresh image holds no flags");
        let mut vm = Vm::default();
        vm.undo_enable(&state).expect("valid speculation admission");
        let base = state.clone();
        let c1 = vm.undo_checkpoint();

        assert_eq!(
            vm.set_insert(delta, &mut state, 0, &[3, 5, 8], None),
            ErrorCode::Ok
        );
        assert_eq!(meta.change_flags(&state), ChangeFlag::INSERTED);
        let after_a = state.clone();
        let c2 = vm.undo_checkpoint();

        // Two direct calls; only the first moves a flag.
        assert_eq!(vm.set_remove(delta, &mut state, 0, &[5]), ErrorCode::Ok);
        assert_eq!(
            vm.set_insert(delta, &mut state, 0, &[13], None),
            ErrorCode::Ok
        );
        assert_eq!(
            meta.change_flags(&state),
            ChangeFlag::INSERTED | ChangeFlag::REMOVED
        );
        let after_b = state.clone();
        if delta {
            assert_delta_segment_replays(&mut vm, &after_a, &after_b, c2);
        }

        // Only the later segment is undone; the earlier one stays applied.
        rollback(&mut vm, &mut state, c2);
        assert_eq!(after_a, state, "delta={delta}: rollback to c2");
        assert_eq!(meta.change_flags(&state), ChangeFlag::INSERTED);
        rollback(&mut vm, &mut state, c1);
        assert_eq!(base, state, "delta={delta}: rollback to c1");
        assert_eq!(meta.change_flags(&state), 0);
    }
}

/// Each public mutation owns its change-flag boundary and journals every flag it
/// changes exactly once. Two sequential owners journal two flag records, one per
/// owner close, and rollback restores the complete image.
#[test]
fn sequential_public_owners_each_journal_their_changed_flag_once() {
    let prog = bitmap_program(128);
    for delta in [false, true] {
        let mut state = init_program_state(&prog);
        let meta = SlotMetaView::read(&state, 0);
        let mut vm = Vm::default();
        vm.undo_enable(&state).expect("valid speculation admission");
        let base = state.clone();

        assert_eq!(
            vm.set_insert(delta, &mut state, 0, &[1, 2, 3], None),
            ErrorCode::Ok
        );
        assert_eq!(meta.change_flags(&state), ChangeFlag::INSERTED);
        assert_eq!(vm.set_remove(delta, &mut state, 0, &[2]), ErrorCode::Ok);
        assert_eq!(
            meta.change_flags(&state),
            ChangeFlag::INSERTED | ChangeFlag::REMOVED
        );
        assert!(
            vm.undo_checkpoint() > 0,
            "delta={delta}: physical bytes journaled"
        );
        if delta {
            let end = vm.undo_checkpoint();
            assert_eq!(Ok(end), vm.delta_export_segment(0, end));
            let undo = vm.delta_export_undo().to_vec();
            let flag_offset = meta.meta_base + SlotMetaOffset::CHANGE_FLAGS;
            let flag_entries = undo
                .as_chunks::<{ Vm::delta_export_entry_size() as usize }>()
                .0
                .iter()
                .map(FlatUndoEntry::read_from)
                .map(|entry| entry.expect("valid physical entry"))
                .filter(|entry| entry.op == FlatUndoOp::StateBytes && entry.key == flag_offset)
                .count();
            assert_eq!(
                flag_entries, 2,
                "each public owner journals its own changed flag once"
            );
        }

        rollback(&mut vm, &mut state, 0);
        assert_eq!(state, base, "delta={delta}");
    }
}

/// The flat-set branch of the direct entrypoints is a boundary too.
#[test]
fn direct_flat_set_operations_roll_back_their_flags() {
    let def = [Opcode::SlotDef as u8, 0, SlotType::HashSet as u8, 16, 0];
    let prog = program(1, 0, &def, &[]);
    for delta in [false, true] {
        let mut state = init_program_state(&prog);
        let meta = SlotMetaView::read(&state, 0);
        let mut vm = Vm::default();
        vm.undo_enable(&state).expect("valid speculation admission");

        assert_eq!(
            vm.set_insert(delta, &mut state, 0, &[5, 6, 7], None),
            ErrorCode::Ok
        );
        assert_eq!(vm.set_remove(delta, &mut state, 0, &[6]), ErrorCode::Ok);
        assert_eq!(
            meta.change_flags(&state),
            ChangeFlag::INSERTED | ChangeFlag::REMOVED
        );
        assert_eq!(meta.size(&state), 2);

        rollback(&mut vm, &mut state, 0);
        assert_eq!(meta.change_flags(&state), 0, "delta={delta}");
        assert_eq!(meta.size(&state), 0, "delta={delta}");
    }
}

/// Sixty members stamped 1..=60 in a TTL BITMAP slot with a 10 second ttl,
/// seeded with undo off so INSERTED is already set when a window opens on it.
fn seeded_ttl_slot() -> (Vec<u8>, SlotMetaView, Vm) {
    let prog = ttl_bitmap_program(128);
    let mut state = init_program_state(&prog);
    let meta = SlotMetaView::read(&state, 0);
    let mut vm = Vm::default();
    let keys: Vec<u32> = (1u16..61).map(sparse_key).collect();
    let stamps: Vec<f64> = keys.iter().copied().map(sparse_ts).collect();
    assert_eq!(
        vm.set_insert(false, &mut state, 0, &keys, Some(&stamps)),
        ErrorCode::Ok
    );
    (state, meta, vm)
}

/// Seed bytes that are allocated and emitted, but were dead at the savepoint.
/// A growing image or TTL insertion must retain their actual prior contents.
fn seed_bitmap_dead_tails(state: &mut [u8], meta: &SlotMetaView) {
    let storage = get_bitmap_storage(meta);
    let old_len = storage.serialized_len(state).expect("valid image");
    let start = (storage.payload_offset() + old_len) as usize;
    let end = (storage.payload_offset() + storage.payload_capacity) as usize;
    state[start..end].fill(0xa5);
    if meta.has_ttl() {
        seed_ttl_index_dead_tail(state, meta);
    }
}

fn seed_ttl_index_dead_tail(state: &mut [u8], meta: &SlotMetaView) {
    let base = meta.eviction_index_offset(state);
    let size = meta.eviction_index_size(state);
    let capacity = meta.eviction_index_capacity(state);
    let start = (base + size * EVICTION_ENTRY_SIZE) as usize;
    let end = (base + capacity * EVICTION_ENTRY_SIZE) as usize;
    state[start..end].fill(0x9b);
}

#[test]
fn nested_ttl_savepoints_restore_payload_growth_shrink_and_dead_index_tails() {
    for delta in [false, true] {
        let (mut state, meta, mut vm) = seeded_ttl_slot();
        seed_bitmap_dead_tails(&mut state, &meta);
        vm.undo_enable(&state).expect("valid speculation admission");
        let base = state.clone();
        let c1 = vm.undo_checkpoint();
        let keys = [sparse_key(61), sparse_key(62), sparse_key(63)];
        assert_eq!(
            vm.set_insert(delta, &mut state, 0, &keys, Some(&[-3.0, -2.0, -1.0])),
            ErrorCode::Ok
        );
        assert_eq!(meta.size(&state), 63);
        assert_eq!(meta.eviction_index_size(&state), 63);
        let after_a = state.clone();
        let c2 = vm.undo_checkpoint();
        assert_eq!(
            vm.set_insert(delta, &mut state, 0, &[sparse_key(20)], Some(&[500.0])),
            ErrorCode::Ok
        );
        assert_eq!(
            vm.set_remove(
                delta,
                &mut state,
                0,
                &[sparse_key(1), sparse_key(30), keys[1]]
            ),
            ErrorCode::Ok
        );
        assert_eq!(meta.size(&state), 60);
        assert_eq!(meta.eviction_index_size(&state), 60);
        let after_b = state.clone();
        if delta {
            assert_delta_segment_replays(&mut vm, &after_a, &after_b, c2);
        }
        rollback(&mut vm, &mut state, c2);
        assert_eq!(state, after_a, "delta={delta}: nested savepoint");
        if delta {
            assert_delta_segment_replays(&mut vm, &base, &after_a, c1);
        }
        rollback(&mut vm, &mut state, c1);
        assert_eq!(state, base, "delta={delta}: dead tails at outer savepoint");
    }
}

#[test]
fn bitmap_payload_growth_restores_previously_dead_nonzero_bytes() {
    let prog = bitmap_program(128);
    for delta in [false, true] {
        let mut state = init_program_state(&prog);
        let meta = SlotMetaView::read(&state, 0);
        let mut vm = Vm::default();
        seed_bitmap_dead_tails(&mut state, &meta);
        vm.undo_enable(&state).expect("valid speculation admission");
        let initial = state.clone();
        let checkpoint = vm.undo_checkpoint();
        assert_eq!(
            vm.set_insert(delta, &mut state, 0, &[1, 70_000, 190_000], None),
            ErrorCode::Ok
        );
        let inserted = state.clone();
        if delta {
            assert_delta_segment_replays(&mut vm, &initial, &inserted, checkpoint);
        }
        assert_eq!(
            vm.set_remove(delta, &mut state, 0, &[1, 70_000, 190_000]),
            ErrorCode::Ok
        );
        assert_eq!(meta.size(&state), 0);
        let after_remove = state.clone();
        if delta {
            assert_delta_segment_replays(&mut vm, &initial, &after_remove, checkpoint);
        }
        rollback(&mut vm, &mut state, checkpoint);
        assert_eq!(state, initial, "delta={delta}: prior dead payload bytes");
    }
}

/// Eviction mutates outside any batch, so it is a boundary of its own: its
/// EVICTED flag, the size it leaves and the members it removes all roll back
/// together.
#[test]
fn eviction_rolls_back_its_flag_size_and_members() {
    let (mut state, meta, mut vm) = seeded_ttl_slot();
    seed_ttl_index_dead_tail(&mut state, &meta);
    let storage = get_bitmap_storage(&meta);
    vm.undo_enable(&state).expect("valid speculation admission");
    let checkpoint = vm.undo_checkpoint();
    let initial = state.clone();

    // At now = 35 the cutoff is 25: the 24 members stamped 1..=24 expire and
    // the 36 stamped 25..=60 stay.
    assert_eq!(vm.evict_all_expired(false, &mut state, 35.0), Ok(24));
    assert_eq!(meta.size(&state), 36);
    assert_eq!(meta.eviction_index_size(&state), 36);
    assert_eq!(
        meta.change_flags(&state),
        ChangeFlag::INSERTED | ChangeFlag::EVICTED
    );
    let survivors: BTreeSet<u32> = (25u16..61).map(sparse_key).collect();
    assert_slot_is_canonical(&state, storage, &survivors);
    assert!(vm.undo_checkpoint() > checkpoint);

    rollback(&mut vm, &mut state, checkpoint);
    assert_eq!(meta.size(&state), 60);
    assert_eq!(meta.change_flags(&state), ChangeFlag::INSERTED);
    assert_ttl_slot_restored(&state, &initial, &meta);
}

/// Every member expiring leaves an empty slot of size zero: the size loses
/// what eviction removed once, never more, so it cannot go below zero. The
/// rollback brings all sixty members back with the flags the window opened on.
#[test]
fn eviction_of_every_member_empties_the_slot_and_rolls_back() {
    let (mut state, meta, mut vm) = seeded_ttl_slot();
    seed_ttl_index_dead_tail(&mut state, &meta);
    let storage = get_bitmap_storage(&meta);
    vm.undo_enable(&state).expect("valid speculation admission");
    let checkpoint = vm.undo_checkpoint();
    let initial = state.clone();

    assert_eq!(vm.evict_all_expired(false, &mut state, 1000.0), Ok(60));
    assert_eq!(meta.size(&state), 0);
    assert_eq!(meta.eviction_index_size(&state), 0);
    assert_eq!(
        meta.change_flags(&state),
        ChangeFlag::INSERTED | ChangeFlag::EVICTED
    );
    assert_slot_is_canonical(&state, storage, &BTreeSet::new());
    assert!(
        vm.undo_checkpoint() > checkpoint,
        "physical expiry bytes journaled"
    );

    rollback(&mut vm, &mut state, checkpoint);
    assert_eq!(meta.size(&state), 60);
    assert_eq!(meta.change_flags(&state), ChangeFlag::INSERTED);
    assert_ttl_slot_restored(&state, &initial, &meta);
}

/// Overflow shadows the image before the first unlogged write; the batch lands,
/// later writes in the window succeed unrecorded, and rollback restores the
/// accepted prefix byte-exact.
#[test]
fn bitmap_insert_journal_overflow_shadows_and_rolls_back_exactly() {
    let prog = bitmap_program(128);
    for delta in [false, true] {
        let mut state = init_program_state(&prog);
        let mut vm = Vm::default();
        vm.undo_enable(&state).expect("valid speculation admission");
        let base = state.clone();
        assert_eq!(
            vm.set_insert(delta, &mut state, 0, &[1000, 2000], None),
            ErrorCode::Ok
        );
        let applied = state.clone();
        let applied_at = vm.undo_checkpoint();
        if delta {
            assert_delta_segment_replays(&mut vm, &base, &applied, 0);
        }
        fill_journal_until(&mut vm, &mut state, 0, delta);
        let batch: Vec<u32> = (1..=40).collect();
        assert_eq!(
            vm.set_insert(delta, &mut state, 0, &batch, None),
            ErrorCode::Ok
        );
        assert!(
            vm.undo_has_overflow(),
            "delta={delta}: overflow is explicit"
        );
        assert_eq!(vm.undo_checkpoint(), UNDO_POSITION_OVERFLOWED);
        let mut expected = applied.clone();
        assert_eq!(
            Vm::default().set_insert(delta, &mut expected, 0, &batch, None),
            ErrorCode::Ok
        );
        assert_eq!(state, expected, "delta={delta}: overflowed insert lands");
        assert_eq!(
            vm.set_remove(delta, &mut state, 0, &[1000]),
            ErrorCode::Ok,
            "later writes in the overflowed window succeed unrecorded"
        );
        assert!(
            vm.undo_has_overflow(),
            "overflow stays sticky until rollback or commit"
        );
        rollback(&mut vm, &mut state, applied_at);
        assert_eq!(applied, state, "delta={delta}: rollback keeps valid prefix");
        assert!(!vm.undo_has_overflow());
        assert_eq!(vm.undo_checkpoint(), applied_at);
        rollback(&mut vm, &mut state, 0);
        assert_eq!(base, state, "delta={delta}: complete prefix rollback");
    }
}

/// Overflow shadows the image before the removal writes its payload and flags;
/// the removal lands, and rollback restores the accepted insert exactly.
#[test]
fn bitmap_remove_journal_overflow_shadows_and_lands_payload_and_flags() {
    let prog = bitmap_program(128);
    for delta in [false, true] {
        let mut state = init_program_state(&prog);
        let mut vm = Vm::default();
        vm.undo_enable(&state).expect("valid speculation admission");
        let base = state.clone();
        let members: Vec<u32> = (1..=8).collect();
        assert_eq!(
            vm.set_insert(delta, &mut state, 0, &members, None),
            ErrorCode::Ok
        );
        let applied = state.clone();
        let applied_at = vm.undo_checkpoint();
        fill_journal_until(&mut vm, &mut state, 0, delta);
        assert_eq!(vm.set_remove(delta, &mut state, 0, &members), ErrorCode::Ok);
        assert!(
            vm.undo_has_overflow(),
            "delta={delta}: overflow is explicit"
        );
        assert_eq!(vm.undo_checkpoint(), UNDO_POSITION_OVERFLOWED);
        let mut expected = applied.clone();
        assert_eq!(
            Vm::default().set_remove(delta, &mut expected, 0, &members),
            ErrorCode::Ok
        );
        assert_eq!(state, expected, "delta={delta}: overflowed removal lands");
        rollback(&mut vm, &mut state, applied_at);
        assert_eq!(applied, state, "delta={delta}: rollback to accepted insert");
        rollback(&mut vm, &mut state, 0);
        assert_eq!(base, state, "delta={delta}: rollback to the start");
    }
}

/// Operations after overflow succeed unrecorded; rolling back to the savepoint
/// taken before the overflow still restores the exact image.
#[test]
fn operations_after_overflow_run_unrecorded_and_roll_back_exactly() {
    let prog = bitmap_program(128);
    for delta in [false, true] {
        let mut state = init_program_state(&prog);
        let meta = SlotMetaView::read(&state, 0);
        let mut vm = Vm::default();
        let members: Vec<u32> = (1..=20).collect();
        assert_eq!(
            vm.set_insert(false, &mut state, 0, &members, None),
            ErrorCode::Ok
        );
        vm.undo_enable(&state).expect("valid speculation admission");
        let initial = state.clone();
        fill_journal_until(&mut vm, &mut state, 1, delta);
        let meta_flag = meta.meta_base + SlotMetaOffset::CHANGE_FLAGS;
        let derived_flag = StateHeaderOffset::DERIVED_FACTS_CHANGE_FLAG;
        let flags = [(meta_flag, 1), (derived_flag, 1)];
        let flagged: Result<(), ErrorCode> =
            vm.capture_mutation(delta, &mut state, &flags, |image| {
                image[usize::try_from(meta_flag).expect("offset fits usize")] = 1;
                image[usize::try_from(derived_flag).expect("offset fits usize")] = 1;
            });
        assert_eq!(flagged, Ok(()));
        assert_eq!(vm.set_remove(delta, &mut state, 0, &members), ErrorCode::Ok);
        assert_eq!(
            vm.set_insert(delta, &mut state, 0, &[7], None),
            ErrorCode::Ok
        );
        assert!(
            vm.undo_has_overflow(),
            "delta={delta}: overflow is explicit"
        );
        assert_eq!(vm.undo_checkpoint(), UNDO_POSITION_OVERFLOWED);
        assert_ne!(
            state, initial,
            "delta={delta}: later operations landed unrecorded"
        );
        rollback(&mut vm, &mut state, 0);
        assert_eq!(
            state, initial,
            "delta={delta}: rollback restores the exact image"
        );
        assert!(!vm.undo_has_overflow());
        assert_eq!(vm.undo_checkpoint(), 0);
    }
}

/// A payload refusal is discovered before journal admission: no bytes or records
/// change, and the speculation stays usable for a subsequent fitting operation.
#[test]
fn payload_refusal_near_the_journal_limit_changes_nothing() {
    let prog = bitmap_program(128);
    for delta in [false, true] {
        let mut state = init_program_state(&prog);
        let mut vm = Vm::default();
        vm.undo_enable(&state).expect("valid speculation admission");
        let initial = state.clone();
        fill_journal_until(&mut vm, &mut state, 100, delta);
        let before = vm.undo_checkpoint();

        // The kernel rejects this extent before any capture is admitted.
        let flood: Vec<u32> = (1u16..240).map(sparse_key).collect();
        assert_eq!(
            vm.set_insert(delta, &mut state, 0, &flood, None),
            ErrorCode::CapacityExceeded
        );
        assert_ne!(vm.bitmap_env.last_error, 0, "a refusal names its cause");
        assert_eq!(state, initial, "the refusal wrote bytes");
        assert_eq!(vm.undo_checkpoint(), before, "the refusal journaled");
        assert!(!vm.undo_has_overflow(), "the refusal overflowed");

        // The window is still exact for what follows.
        assert_eq!(
            vm.set_insert(delta, &mut state, 0, &[1, 2, 3], None),
            ErrorCode::Ok
        );
        assert!(!vm.undo_has_overflow());
        rollback(&mut vm, &mut state, 0);
        assert_eq!(state, initial, "delta={delta}");
    }
}

proptest! {
    /// A patch into a slot at boundary payload capacities — the region around
    /// the empty image size and the exact payload_capacity edge — either lands
    /// the canonical image or refuses without touching a byte.
    #[test]
    fn patch_boundary_capacities(
        elems in prop::collection::btree_set(0u32..100_000, 0..600),
        cap in 0u32..80,
    ) {
        let mut env = BitmapEnv::default();
        let payload_cap = bitmap_payload_capacity(cap);
        let (mut buf, storage) = empty_buffer(payload_cap);
        let initial = buf.clone();
        let adds: Vec<u32> = elems.iter().copied().collect();

        match bitmap_patch(&mut env, &mut buf, storage, &adds, &[]) {
            Ok(report) => {
                prop_assert_eq!(report.added as usize, elems.len());
                prop_assert_eq!(members(&buf, storage), adds);
                assert_slot_is_canonical(&buf, storage, &elems);
            }
            Err(ErrorCode::CapacityExceeded) => {
                // Refusal must leave the slot bytes untouched (still the
                // canonical empty image).
                prop_assert_eq!(buf, initial);
                let needed = if elems.is_empty() {
                    IMAGE_ID_LEN
                } else {
                    Bitmosaic::from_sorted(adds.iter().copied()).forest_len()
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
        let mut hooks = NoVm::default();
        let mut state = vec![0u8; 1 << 20];
        let meta = init_bitmap_slot_state(&mut state, 16_384);
        let a_vec: Vec<u32> = a.iter().copied().collect();
        if !a_vec.is_empty() {
            prop_assert_eq!(
                batch_bitmap_add(&mut env, &mut hooks, false, &mut state, &meta, &a_vec, None),
                ErrorCode::Ok
            );
        }
        let source = serialize(&b.iter().copied().collect::<Vec<_>>());
        prop_assert_eq!(
            batch_bitmap_algebra(&mut env, &mut hooks, false, op, &mut state, &meta, BitmapSource::Bytes(&source)),
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
        let mut hooks = NoVm::default();
        let mut state = vec![0u8; 1 << 20];
        let meta = init_bitmap_slot_state(&mut state, 16_384);
        let mut model = BTreeSet::new();
        let storage = get_bitmap_storage(&meta);

        for (elems, is_remove) in &batches {
            if *is_remove {
                batch_bitmap_remove(&mut env, &mut hooks, false, &mut state, &meta, elems);
                for e in elems { model.remove(e); }
            } else {
                prop_assert_eq!(
                    batch_bitmap_add(&mut env, &mut hooks, false, &mut state, &meta, elems, None),
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
        let mut hooks = NoVm::default();
        let mut state = vec![0u8; 1 << 20];
        let meta = init_bitmap_slot_state(&mut state, 65_536);
        let storage = get_bitmap_storage(&meta);
        let mut model: BTreeSet<u32> = BTreeSet::new();

        let mut apply = |env: &mut BitmapEnv, state: &mut Vec<u8>, model: &mut BTreeSet<u32>, batch: &[u32], insert: bool| {
            let sentinel_free: Vec<u32> = batch.iter().copied().filter(|&v| v < u32::MAX - 1).collect();
            let code = if insert {
                batch_bitmap_add(env, &mut hooks, false, state, &meta, &sentinel_free, None)
            } else {
                batch_bitmap_remove(env, &mut hooks, false, state, &meta, &sentinel_free)
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

#[test]
fn real_bitmap_algebra_restores_framing_cleared_tail_and_both_delta_lanes() {
    let init = [
        Opcode::SlotDef as u8,
        0,
        SlotType::Bitmap as u8,
        0,
        4,
        Opcode::SlotDef as u8,
        1,
        SlotType::Bitmap as u8,
        0,
        4,
    ];
    let left: BTreeSet<u32> = (0..127).chain([70_000, 200_000]).collect();
    let right: BTreeSet<u32> = (64..260).chain([80_000, 200_000]).collect();
    let left_keys: Vec<u32> = left.iter().copied().collect();
    let right_keys: Vec<u32> = right.iter().copied().collect();
    for (opcode, expected) in [
        (
            Opcode::BatchBitmapAnd,
            left.intersection(&right).copied().collect::<BTreeSet<_>>(),
        ),
        (Opcode::BatchBitmapOr, left.union(&right).copied().collect()),
        (
            Opcode::BatchBitmapAndNot,
            left.difference(&right).copied().collect(),
        ),
        (
            Opcode::BatchBitmapXor,
            left.symmetric_difference(&right).copied().collect(),
        ),
    ] {
        let prog = program(2, 0, &init, &[opcode as u8, 0, 1]);
        for delta in [false, true] {
            let mut state = init_program_state(&prog);
            let target = SlotMetaView::read(&state, 0);
            let mut vm = Vm::default();
            assert_eq!(
                vm.set_insert(false, &mut state, 0, &left_keys, None),
                ErrorCode::Ok
            );
            assert_eq!(
                vm.set_insert(false, &mut state, 1, &right_keys, None),
                ErrorCode::Ok
            );
            seed_bitmap_dead_tails(&mut state, &target);
            vm.undo_enable(&state).expect("valid speculation admission");
            let initial = state.clone();
            let checkpoint = vm.undo_checkpoint();
            let result = if delta {
                vm.execute_batch_delta(&mut state, &prog, &[], 1)
            } else {
                vm.execute_batch(&mut state, &prog, &[], 1)
            };
            assert_eq!(result, ErrorCode::Ok as u32, "op={opcode:?}, delta={delta}");
            assert_eq!(
                members(&state, get_bitmap_storage(&target))
                    .into_iter()
                    .collect::<BTreeSet<_>>(),
                expected
            );
            assert_eq!(target.size(&state) as usize, expected.len());
            let applied = state.clone();
            if delta {
                assert_delta_segment_replays(&mut vm, &initial, &applied, checkpoint);
            }
            rollback(&mut vm, &mut state, checkpoint);
            assert_eq!(state, initial, "op={opcode:?}, delta={delta}: full image");
        }
    }
}

#[test]
fn bitmap_overflow_mid_reducer_lands_the_batch_and_rolls_back_exactly() {
    let init = [
        Opcode::SlotDef as u8,
        0,
        SlotType::Bitmap as u8,
        0,
        4,
        Opcode::SlotDef as u8,
        1,
        SlotType::Bitmap as u8,
        0,
        4,
    ];
    let reduce = [
        Opcode::BatchBitmapAdd as u8,
        0,
        0,
        Opcode::BatchBitmapOr as u8,
        0,
        1,
    ];
    let prog = program(2, 1, &init, &reduce);
    let source_keys: Vec<u32> = (1u16..61).map(sparse_key).collect();
    let column: Vec<u8> = (0..60).flat_map(|_| 7u32.to_le_bytes()).collect();
    for delta in [false, true] {
        let mut state = init_program_state(&prog);
        let target = SlotMetaView::read(&state, 0);
        let mut vm = Vm::default();
        assert_eq!(
            vm.set_insert(false, &mut state, 1, &source_keys, None),
            ErrorCode::Ok
        );
        seed_bitmap_dead_tails(&mut state, &target);
        let initial = state.clone();
        vm.undo_enable(&state).expect("valid speculation admission");
        fill_journal_until(&mut vm, &mut state, 32, delta);
        let checkpoint = vm.undo_checkpoint();
        let result = if delta {
            vm.execute_batch_delta(&mut state, &prog, &[&column], 60)
        } else {
            vm.execute_batch(&mut state, &prog, &[&column], 60)
        };
        assert_eq!(result, ErrorCode::Ok as u32, "delta={delta}");
        assert!(
            vm.undo_has_overflow(),
            "the overflowing algebra shadows the image"
        );
        assert_eq!(vm.undo_checkpoint(), UNDO_POSITION_OVERFLOWED);
        let mut expected = initial.clone();
        let oracle = if delta {
            Vm::default().execute_batch_delta(&mut expected, &prog, &[&column], 60)
        } else {
            Vm::default().execute_batch(&mut expected, &prog, &[&column], 60)
        };
        assert_eq!(oracle, ErrorCode::Ok as u32);
        assert_eq!(
            state, expected,
            "delta={delta}: overflowed batch writes land"
        );
        if delta {
            // The overflowed window names no restorable end: only its
            // recorded prefix exports.
            assert_eq!(
                vm.delta_export_segment(checkpoint, UNDO_POSITION_OVERFLOWED),
                Err(ErrorCode::InvalidState)
            );
            assert_eq!(vm.delta_export_segment(0, checkpoint), Ok(checkpoint));
        }
        rollback(&mut vm, &mut state, checkpoint);
        assert_eq!(state, initial, "delta={delta}: exact prefix rollback");
        assert!(!vm.undo_has_overflow());
        assert_eq!(vm.undo_checkpoint(), checkpoint);
    }
}

#[test]
fn small_bitmap_patch_reserves_encoded_extent_not_slot_capacity() {
    let prog = bitmap_program(u16::MAX);
    for delta in [false, true] {
        let mut state = init_program_state(&prog);
        let meta = SlotMetaView::read(&state, 0);
        assert!(get_bitmap_storage(&meta).payload_capacity / 8 > UNDO_CAPACITY);
        let mut vm = Vm::default();
        seed_bitmap_dead_tails(&mut state, &meta);
        vm.undo_enable(&state).expect("valid speculation admission");
        let initial = state.clone();
        let checkpoint = vm.undo_checkpoint();
        assert_eq!(
            vm.set_insert(delta, &mut state, 0, &[7], None),
            ErrorCode::Ok,
            "a tiny image fits the journal regardless of allocated slot capacity"
        );
        assert!(vm.undo_checkpoint() - checkpoint < 16);
        let applied = state.clone();
        if delta {
            assert_delta_segment_replays(&mut vm, &initial, &applied, checkpoint);
        }
        rollback(&mut vm, &mut state, checkpoint);
        assert_eq!(state, initial, "delta={delta}: full allocated image");
    }
}

#[test]
fn ttl_capture_overflow_shadows_then_writes_bitmap_payload() {
    for delta in [false, true] {
        let (mut state, meta, mut vm) = seeded_ttl_slot();
        seed_bitmap_dead_tails(&mut state, &meta);
        vm.undo_enable(&state).expect("valid speculation admission");
        fill_journal_until(&mut vm, &mut state, 8, delta);
        let before = state.clone();
        assert_eq!(
            vm.set_insert(delta, &mut state, 0, &[sparse_key(61)], Some(&[-1.0])),
            ErrorCode::Ok
        );
        assert_ne!(state, before, "delta={delta}: the TTL insert wrote");
        assert!(
            vm.undo_has_overflow(),
            "delta={delta}: overflow is explicit"
        );
        assert_eq!(vm.undo_checkpoint(), UNDO_POSITION_OVERFLOWED);
        rollback(&mut vm, &mut state, 0);
        assert_eq!(state, before, "delta={delta}: exact prefix rollback");
        assert!(!vm.undo_has_overflow());
        assert_eq!(vm.undo_checkpoint(), 0);
    }
}

#[test]
fn partial_and_all_expiry_restore_nested_savepoints_and_dead_eviction_buffer_bytes() {
    let prog = ttl_bitmap_program_flags(128, TTL_FLAG | 0x20);
    for delta in [false, true] {
        let mut state = init_program_state(&prog);
        let meta = SlotMetaView::read(&state, 0);
        let mut vm = Vm::default();
        let keys: Vec<u32> = (1u16..61).map(sparse_key).collect();
        let stamps: Vec<f64> = keys.iter().copied().map(sparse_ts).collect();
        assert_eq!(
            vm.set_insert(false, &mut state, 0, &keys, Some(&stamps)),
            ErrorCode::Ok
        );
        seed_ttl_index_dead_tail(&mut state, &meta);
        let buffer = meta.evicted_buffer_offset(&state) as usize;
        assert_ne!(buffer, 0, "trigger allocates a physical eviction buffer");
        let buffer_end = buffer + (EVICTED_BUFFER_CAP * EVICTION_ENTRY_SIZE) as usize;
        state[buffer..buffer_end].fill(0xcd);
        vm.undo_enable(&state).expect("valid speculation admission");
        let initial = state.clone();
        let c1 = vm.undo_checkpoint();
        let evicted = vm.evict_all_expired(delta, &mut state, 35.0);
        assert_eq!(evicted, Ok(24));
        assert_eq!(
            meta.size(&state),
            36,
            "cardinality subtracts each expiry once"
        );
        assert_eq!(meta.eviction_index_size(&state), 36);
        assert_eq!(meta.evicted_count(&state), 24);
        assert_eq!(
            meta.change_flags(&state),
            ChangeFlag::INSERTED | ChangeFlag::EVICTED
        );
        let after_partial = state.clone();
        let c2 = vm.undo_checkpoint();
        let evicted = vm.evict_all_expired(delta, &mut state, 1000.0);
        assert_eq!(evicted, Ok(36));
        assert_eq!(meta.size(&state), 0);
        assert_eq!(meta.eviction_index_size(&state), 0);
        assert_eq!(meta.evicted_count(&state), 36);
        let after_all = state.clone();
        if delta {
            assert_delta_segment_replays(&mut vm, &after_partial, &after_all, c2);
        }
        rollback(&mut vm, &mut state, c2);
        assert_eq!(
            state, after_partial,
            "delta={delta}: restore partial-expiry mark"
        );
        if delta {
            assert_delta_segment_replays(&mut vm, &initial, &after_partial, c1);
        }
        rollback(&mut vm, &mut state, c1);
        assert_eq!(
            state, initial,
            "delta={delta}: all dead buffer/index bytes restored"
        );
    }
}

fn full_ttl_bitmap_slot() -> (Vec<u8>, SlotMetaView, Vm) {
    let prog = ttl_bitmap_program(8);
    let mut state = init_program_state(&prog);
    let meta = SlotMetaView::read(&state, 0);
    let mut vm = Vm::default();
    let keys: Vec<u32> = (1..=meta.capacity).collect();
    let stamps: Vec<f64> = keys
        .iter()
        .copied()
        .map(|key| f64::from(key) * 10.0)
        .collect();
    assert_eq!(
        vm.set_insert(false, &mut state, 0, &keys, Some(&stamps)),
        ErrorCode::Ok
    );
    assert_eq!(meta.capacity, 16);
    assert_eq!(meta.eviction_index_size(&state), meta.capacity);

    // Seed a valid checkpoint with one no-longer-live primary key and its
    // retained index entry. Raw owned-byte setup does not admit TTL algebra,
    // forge capacity, or manufacture a timestamp for the missing member.
    let report = bitmap_patch(
        &mut vm.bitmap_env,
        &mut state,
        get_bitmap_storage(&meta),
        &[],
        &[16],
    )
    .expect("checkpoint primary removal fits");
    meta.set_size(
        &mut state,
        u32::try_from(report.len).expect("bitmap cardinality"),
    );
    assert_eq!(meta.size(&state), 15);
    assert_eq!(meta.eviction_index_size(&state), meta.capacity);
    assert_eq!(meta.eviction_index_capacity(&state), meta.capacity);
    seed_bitmap_dead_tails(&mut state, &meta);
    columine_vm::state_init::bind_slot(&state, 0).expect("valid full retained TTL index");
    (state, meta, vm)
}

#[test]
fn full_ttl_batch_refuses_before_payload_or_earlier_refresh_mutates() {
    for delta in [false, true] {
        let (mut state, _meta, mut vm) = full_ttl_bitmap_slot();
        vm.undo_enable(&state).expect("valid speculation admission");
        let initial = state.clone();
        let checkpoint = vm.undo_checkpoint();
        assert_eq!(
            vm.set_insert(delta, &mut state, 0, &[2, 70_000], Some(&[-10.0, 40.0]),),
            ErrorCode::CapacityExceeded
        );
        assert_eq!(
            state, initial,
            "bulk TTL refusal cannot strand a phantom primary key"
        );
        assert_eq!(
            vm.undo_checkpoint(),
            checkpoint,
            "bulk refusal journaled nothing"
        );
        assert!(
            !vm.undo_has_overflow(),
            "TTL capacity is not journal overflow"
        );
        assert_eq!(
            vm.set_insert(delta, &mut state, 0, &[2], Some(&[500.0])),
            ErrorCode::Ok,
            "a full-index refresh still fits after the refused batch"
        );
        let refreshed = state.clone();
        if delta {
            assert_delta_segment_replays(&mut vm, &initial, &refreshed, checkpoint);
        }
        rollback(&mut vm, &mut state, checkpoint);
        assert_eq!(
            state, initial,
            "delta={delta}: refresh after refusal restores exact bytes"
        );
    }
}

#[test]
fn full_ttl_bitmap_refresh_preserves_binary_nan_and_tie_placement() {
    for (timestamp, expected_keys) in [
        (
            f64::NAN,
            [2, 1].into_iter().chain(3..=16).collect::<Vec<u32>>(),
        ),
        (30.0, (1..=16).collect()),
        (500.0, [1].into_iter().chain(3..=16).chain([2]).collect()),
    ] {
        for delta in [false, true] {
            let (mut state, meta, mut vm) = full_ttl_bitmap_slot();
            vm.undo_enable(&state).expect("valid speculation admission");
            let initial = state.clone();
            let checkpoint = vm.undo_checkpoint();
            assert_eq!(
                vm.set_insert(delta, &mut state, 0, &[2], Some(&[timestamp])),
                ErrorCode::Ok
            );
            assert_eq!(meta.size(&state), 15);
            assert_eq!(meta.eviction_index_size(&state), 16);
            assert_eq!(
                find_latest_eviction_timestamp_for_key(&state, &meta, 2)
                    .expect("retained TTL key")
                    .to_bits(),
                timestamp.to_bits()
            );
            let base = meta.eviction_index_offset(&state) as usize;
            let keys: Vec<u32> = (0..meta.eviction_index_size(&state))
                .map(|index| {
                    let offset = base + index as usize * EVICTION_ENTRY_SIZE as usize + 8;
                    u32::from_le_bytes(state[offset..offset + 4].try_into().expect("key lane"))
                })
                .collect();
            assert_eq!(keys, expected_keys, "actual binary insertion order");
            let refreshed = state.clone();
            if delta {
                assert_delta_segment_replays(&mut vm, &initial, &refreshed, checkpoint);
            }
            rollback(&mut vm, &mut state, checkpoint);
            assert_eq!(
                state, initial,
                "delta={delta}, timestamp={timestamp:?}: full image"
            );
        }
    }
}

#[test]
fn ttl_words_removal_uses_prepared_held_witness_and_restores_exact_bytes() {
    let prog = ttl_bitmap_program(8192);
    let keys: Vec<u32> = (0..16_000).filter(|key| key % 3 != 0).collect();
    let stamps: Vec<f64> = keys.iter().copied().map(f64::from).collect();
    let removed = [15_001, 15_900, 15_901, 15_902, 15_999];
    let expected: Vec<u32> = keys
        .iter()
        .copied()
        .filter(|key| !removed.contains(key))
        .collect();
    for delta in [false, true] {
        let mut state = init_program_state(&prog);
        let meta = SlotMetaView::read(&state, 0);
        let mut vm = Vm::default();
        assert_eq!(
            vm.set_insert(false, &mut state, 0, &keys, Some(&stamps)),
            ErrorCode::Ok
        );
        seed_bitmap_dead_tails(&mut state, &meta);
        vm.undo_enable(&state).expect("valid speculation admission");
        let initial = state.clone();
        let checkpoint = vm.undo_checkpoint();
        assert_eq!(vm.set_remove(delta, &mut state, 0, &removed), ErrorCode::Ok);
        assert_eq!(meta.size(&state) as usize, keys.len() - 3);
        assert_eq!(meta.eviction_index_size(&state) as usize, keys.len() - 3);
        assert_eq!(members(&state, get_bitmap_storage(&meta)), expected);
        let applied = state.clone();
        if delta {
            assert_delta_segment_replays(&mut vm, &initial, &applied, checkpoint);
        }
        rollback(&mut vm, &mut state, checkpoint);
        assert_eq!(
            state, initial,
            "delta={delta}: prepared Words witness and TTL bytes"
        );
    }
}

fn ttl_algebra_program(source_ttl: bool) -> Vec<u8> {
    ttl_algebra_program_flags(source_ttl, TTL_FLAG)
}

fn ttl_algebra_program_flags(source_ttl: bool, target_flags: u8) -> Vec<u8> {
    ttl_algebra_program_reducing(source_ttl, target_flags, &[])
}

/// The two-slot TTL algebra program with `reduce` as its reduce section.
fn ttl_algebra_program_reducing(source_ttl: bool, target_flags: u8, reduce: &[u8]) -> Vec<u8> {
    let mut init = Vec::new();
    for slot in 0..2u8 {
        let has_ttl = slot == 0 || source_ttl;
        init.extend([
            Opcode::SlotDef as u8,
            slot,
            SlotType::Bitmap as u8
                | if slot == 0 {
                    target_flags
                } else if source_ttl {
                    TTL_FLAG
                } else {
                    0
                },
            64,
            0,
        ]);
        if has_ttl {
            init.extend(10.0f32.to_le_bytes());
            init.extend(0.0f32.to_le_bytes());
            init.extend([1, 0]);
        }
    }
    program(2, 2, &init, reduce)
}

/// Checkpoint setup uses actual bitmosaic owned-byte primitives and the given
/// retained timestamp bits. Optional expiry is represented by absent rows,
/// never a synthetic clock. Capacities/offsets remain those of init_state.
fn seed_bitmap_checkpoint(
    vm: &mut Vm,
    state: &mut [u8],
    slot: u32,
    membership: &[u32],
    retained_rows: &[(u32, u64)],
) {
    let meta = columine_vm::state_init::bind_slot(state, slot).expect("checkpoint slot geometry");
    let report = bitmap_patch(
        &mut vm.bitmap_env,
        state,
        get_bitmap_storage(&meta),
        membership,
        &[],
    )
    .expect("checkpoint bitmap fits");
    meta.set_size(
        state,
        u32::try_from(report.len).expect("bitmap cardinality"),
    );
    if meta.has_ttl() {
        assert!(retained_rows.len() <= meta.eviction_index_capacity(state) as usize);
        let base = meta.eviction_index_offset(state) as usize;
        for (index, &(key, timestamp_bits)) in retained_rows.iter().enumerate() {
            let entry = base + index * EVICTION_ENTRY_SIZE as usize;
            let timestamp =
                entry + std::mem::offset_of!(columine_types::types::EvictionEntry, timestamp);
            let key_offset =
                entry + std::mem::offset_of!(columine_types::types::EvictionEntry, key_or_idx);
            let value = entry + std::mem::offset_of!(columine_types::types::EvictionEntry, value);
            state[timestamp..timestamp + 8].copy_from_slice(&timestamp_bits.to_le_bytes());
            state[key_offset..key_offset + 4].copy_from_slice(&key.to_le_bytes());
            state[value..value + 4].copy_from_slice(&key.to_le_bytes());
        }
        meta.set_eviction_index_size(
            state,
            u32::try_from(retained_rows.len()).expect("checkpoint row count"),
        );
    } else {
        assert!(
            retained_rows.is_empty(),
            "payload-only input has no timestamp rows"
        );
    }
    seed_bitmap_dead_tails(state, &meta);
    columine_vm::state_init::bind_slot(state, slot).expect("valid seeded checkpoint");
}

// ---------------------------------------------------------------------------
// Post-warm allocation census of the public algebra and undo path.
// ---------------------------------------------------------------------------

#[test]
fn timestamp_only_algebra_overflow_preserves_the_real_accepted_prefix_and_redo() {
    let prog = ttl_algebra_program(true);
    for delta in [false, true] {
        let mut state = init_program_state(&prog);
        let mut vm = Vm::default();
        seed_bitmap_checkpoint(
            &mut vm,
            &mut state,
            0,
            &[1, 2],
            &[(1, 10.0f64.to_bits()), (2, 20.0f64.to_bits())],
        );
        seed_bitmap_checkpoint(
            &mut vm,
            &mut state,
            1,
            &[1, 2],
            &[(1, 100.0f64.to_bits()), (2, 200.0f64.to_bits())],
        );
        vm.undo_enable(&state).expect("valid speculation admission");
        let initial = state.clone();
        let start = vm.undo_checkpoint();
        assert_eq!(
            vm.set_insert(delta, &mut state, 0, &[1], Some(&[30.0])),
            ErrorCode::Ok
        );
        let accepted = state.clone();
        let accepted_at = vm.undo_checkpoint();
        assert!(
            accepted_at > start,
            "the prefix contains a real timestamp write"
        );
        fill_journal_until(&mut vm, &mut state, 0, delta);
        assert_eq!(
            state, accepted,
            "real filler cycles preserve the accepted image"
        );
        let mut expected = accepted.clone();
        assert_eq!(
            vm.bitmap_algebra(
                delta,
                BitmapAlgebraOp::Or,
                &mut state,
                0,
                BitmapSource::Slot(1)
            ),
            ErrorCode::Ok
        );
        assert_eq!(
            Vm::default().bitmap_algebra(
                delta,
                BitmapAlgebraOp::Or,
                &mut expected,
                0,
                BitmapSource::Slot(1)
            ),
            ErrorCode::Ok
        );
        assert_eq!(
            state, expected,
            "delta={delta}: overflowed clock, flag and payload writes land"
        );
        assert_eq!(vm.undo_checkpoint(), UNDO_POSITION_OVERFLOWED);
        assert!(vm.undo_has_overflow());
        if delta {
            assert_eq!(
                vm.delta_export_segment(start, accepted_at),
                Ok(accepted_at - start)
            );
            let undo = vm.delta_export_undo().to_vec();
            let redo = vm.delta_export_redo().to_vec();
            let entry_size = Vm::delta_export_entry_size();
            let mut replay = Vm::default();
            let mut backward = accepted.clone();
            assert_eq!(
                replay.delta_apply_rollback_segment(&mut backward, &undo, entry_size),
                Ok(initial.len() as u32)
            );
            assert_eq!(
                backward, initial,
                "accepted prefix undo survives later overflow"
            );
            let mut forward = initial.clone();
            assert_eq!(
                replay.delta_apply_rollforward_segment(&mut forward, &redo, entry_size),
                Ok(accepted.len() as u32)
            );
            assert_eq!(
                forward, accepted,
                "accepted prefix redo survives later overflow"
            );
        }
        rollback(&mut vm, &mut state, accepted_at);
        assert_eq!(state, accepted);
        rollback(&mut vm, &mut state, start);
        assert_eq!(
            state, initial,
            "delta={delta}: complete valid-prefix rollback"
        );
    }

    // A batch AND of a large TTL target with a small source drops most
    // target members, and its expiry plan takes one mutation per member it
    // drops. The batch reserves that plan before its first write, so the
    // plan fits the reservation; debug builds assert it.
    let and = ttl_algebra_program_reducing(true, TTL_FLAG, &[Opcode::BatchBitmapAnd as u8, 0, 1]);
    let mut state = init_program_state(&and);
    let mut vm = Vm::default();
    let target: Vec<u32> = (1..=40).collect();
    let rows: Vec<(u32, u64)> = target
        .iter()
        .map(|&key| (key, f64::from(key).to_bits()))
        .collect();
    seed_bitmap_checkpoint(&mut vm, &mut state, 0, &target, &rows);
    seed_bitmap_checkpoint(
        &mut vm,
        &mut state,
        1,
        &[1, 2],
        &[(1, 50.0f64.to_bits()), (2, 60.0f64.to_bits())],
    );
    assert_eq!(
        vm.execute_batch(&mut state, &and, &[], 1),
        ErrorCode::Ok as u32
    );
    let target_meta = SlotMetaView::read(&state, 0);
    assert_eq!(
        members(&state, get_bitmap_storage(&target_meta)),
        vec![1, 2]
    );

    let column =
        |values: &[u32]| -> Vec<u8> { values.iter().flat_map(|v| v.to_le_bytes()).collect() };

    // A batch's working memory follows what its own earlier operations do.
    // Inserting new keys into TTL bitmap 0 grows its expiry index before the
    // same batch ORs bitmap 1 into it: the merge reads the grown index, and
    // the batch reserved for it, so the merge fits (debug builds assert it).
    let insert_then_or = ttl_algebra_program_reducing(
        true,
        TTL_FLAG,
        &[
            Opcode::BatchSetInsertTtl as u8,
            0,
            0,
            1,
            Opcode::BatchBitmapOr as u8,
            0,
            1,
        ],
    );
    let mut state = init_program_state(&insert_then_or);
    let mut vm = Vm::default();
    seed_bitmap_checkpoint(&mut vm, &mut state, 0, &[1], &[(1, 1.0f64.to_bits())]);
    seed_bitmap_checkpoint(&mut vm, &mut state, 1, &[2], &[(2, 2.0f64.to_bits())]);
    // Few enough that the patched image provably fits the 64-member slot's
    // payload, so tiers 1 and 2 admit the batch with no dry run.
    let fresh: Vec<u32> = (100..106).collect();
    let stamps: Vec<u8> = fresh.iter().flat_map(|_| 5.0f64.to_le_bytes()).collect();
    let mut vm = Vm::default();
    assert_eq!(
        vm.execute_batch(&mut state, &insert_then_or, &[&column(&fresh), &stamps], 6),
        ErrorCode::Ok as u32
    );
    let target_meta = SlotMetaView::read(&state, 0);
    assert_eq!(members(&state, get_bitmap_storage(&target_meta)).len(), 8);
    assert_eq!(
        vm.capacity_counts().dry_run_fit,
        0,
        "admitted without a dry run"
    );

    // Two adds to one bitmap in one batch: the second patches an image the
    // first already grew past the slot-plane threshold. The batch's bound
    // covers every set the two reach from the starting image, so neither
    // patch outgrows the reservation (debug builds assert it).
    let two_adds = program(
        1,
        2,
        &[Opcode::SlotDef as u8, 0, SlotType::Bitmap as u8, 0, 16],
        &[
            Opcode::BatchBitmapAdd as u8,
            0,
            0,
            Opcode::BatchBitmapAdd as u8,
            0,
            1,
        ],
    );
    let mut state = init_program_state(&two_adds);
    let mut vm = Vm::default();
    let first: Vec<u32> = (0..100).map(|i| (i << 16) | i).collect();
    let second: Vec<u32> = (100..200).map(|i| (i << 16) | i).collect();
    assert_eq!(
        vm.execute_batch(
            &mut state,
            &two_adds,
            &[&column(&first), &column(&second)],
            100
        ),
        ErrorCode::Ok as u32
    );
    let meta = SlotMetaView::read(&state, 0);
    assert_eq!(members(&state, get_bitmap_storage(&meta)).len(), 200);
}

#[test]
fn active_algebra_window_refuses_reenable_without_losing_image_or_journal() {
    let prog = ttl_algebra_program(true);
    for delta in [false, true] {
        let mut state = init_program_state(&prog);
        let mut vm = Vm::default();
        seed_bitmap_checkpoint(&mut vm, &mut state, 0, &[1], &[(1, 10.0f64.to_bits())]);
        seed_bitmap_checkpoint(
            &mut vm,
            &mut state,
            1,
            &[1, 2],
            &[(1, 20.0f64.to_bits()), (2, 30.0f64.to_bits())],
        );
        vm.undo_enable(&state)
            .expect("initial speculation admission");
        let initial = state.clone();
        let checkpoint = vm.undo_checkpoint();
        assert_eq!(
            vm.bitmap_algebra(
                delta,
                BitmapAlgebraOp::Or,
                &mut state,
                0,
                BitmapSource::Slot(1)
            ),
            ErrorCode::Ok
        );
        let applied = state.clone();
        let status = vm.undo_status();
        assert!(status.enabled);
        assert!(status.checkpoint > checkpoint);
        assert_eq!(vm.undo_enable(&state), Err(ErrorCode::SpeculationActive));
        assert_eq!(
            vm.undo_status(),
            status,
            "active admission refusal retains all read-only facts"
        );
        assert_eq!(
            state, applied,
            "active admission refusal retains all emitted bytes"
        );
        if delta {
            assert_delta_segment_replays(&mut vm, &initial, &applied, checkpoint);
        }
        rollback(&mut vm, &mut state, checkpoint);
        assert_eq!(state, initial);
        vm.undo_commit();
        assert!(!vm.undo_status().enabled);
        vm.undo_enable(&state)
            .expect("explicitly ended window may be reenabled");
        assert_eq!(
            vm.bitmap_algebra(
                delta,
                BitmapAlgebraOp::Or,
                &mut state,
                0,
                BitmapSource::Slot(0)
            ),
            ErrorCode::Ok
        );
        assert_eq!(
            state, initial,
            "a fresh exact identity does not rewrite the image"
        );
        assert_eq!(vm.undo_checkpoint(), 0);
        vm.undo_commit();
    }
}

mod ttl_algebra_allocation {
    use super::{
        init_program_state, members, rollback, seed_bitmap_checkpoint, ttl_algebra_program,
    };
    use columine_types::types::ErrorCode;
    use columine_vm::bitmap_ops::{
        BitmapAlgebraOp, BitmapSource, BitmapStorage, get_bitmap_storage,
    };
    use columine_vm::state_init::bind_slot;
    use columine_vm::vm::Vm;
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::collections::BTreeSet;

    thread_local! {
        /// Set only while `allocations_during` runs on this thread. Const
        /// initialization of plain `Cell`s means the first touch inside
        /// `alloc` performs no lazy TLS setup and cannot allocate.
        static COUNTING: Cell<bool> = const { Cell::new(false) };
        /// Allocation calls this thread made while `COUNTING` was set.
        static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    }

    struct CountingAllocator;

    #[global_allocator]
    static CENSUS_ALLOCATOR: CountingAllocator = CountingAllocator;

    /// Counts one allocation call, and only on the measuring thread. `try_with`
    /// keeps the allocator from panicking if the slot is unavailable.
    fn record_allocation() {
        let _ = COUNTING.try_with(|counting| {
            if counting.get() {
                let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
            }
        });
    }

    // SAFETY: every method forwards its pointer and layout unchanged to `System`,
    // so callers observe exactly the `GlobalAlloc` contract `System` implements.
    // The bookkeeping touches only const-initialized `Cell`s, so it neither
    // allocates nor re-enters the allocator.
    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            record_allocation();
            // SAFETY: the caller's layout satisfies `GlobalAlloc::alloc`'s
            // contract, and it is forwarded unchanged to `System`.
            unsafe { System.alloc(layout) }
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            record_allocation();
            // SAFETY: as in `alloc`; `System` returns zeroed memory for this layout.
            unsafe { System.alloc_zeroed(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            // SAFETY: `ptr` and `layout` are the pair the caller obtained from this
            // allocator, which obtained them from `System`; they are returned unchanged.
            unsafe { System.dealloc(ptr, layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            record_allocation();
            // SAFETY: `ptr` and `layout` are the caller's pair and `new_size` is the
            // caller's request; all three are forwarded unchanged to `System`.
            unsafe { System.realloc(ptr, layout, new_size) }
        }
    }

    /// Clears `COUNTING` on every exit path, including a panic in the measured closure.
    struct CountingScope;

    impl Drop for CountingScope {
        fn drop(&mut self) {
            COUNTING.with(|counting| counting.set(false));
        }
    }

    /// Runs `run` and returns its result with the allocation calls this thread made
    /// during it. Scopes cannot nest, so each count belongs to exactly one closure.
    fn allocations_during<T>(run: impl FnOnce() -> T) -> (T, usize) {
        assert!(
            !COUNTING.with(|counting| counting.get()),
            "allocation scopes cannot nest"
        );
        ALLOCATIONS.with(|count| count.set(0));
        COUNTING.with(|counting| counting.set(true));
        let scope = CountingScope;
        let result = run();
        let allocations = ALLOCATIONS.with(|count| count.get());
        drop(scope);
        (result, allocations)
    }

    /// Slot 0 is the target and slot 1 the source of `ttl_algebra_program(true)`.
    const TARGET: u32 = 0;
    const SOURCE: u32 = 1;
    /// A retained row whose key is in no target member.
    const STALE_KEY: u32 = 40;
    /// Warm-up rounds; each runs every operation before any counter opens.
    const WARMUP_ROUNDS: usize = 2;
    /// Counted algebra+undo rounds per operation.
    const MEASURED_ROUNDS: u32 = 16;
    const ALGEBRA_OPS: [BitmapAlgebraOp; 4] = [
        BitmapAlgebraOp::And,
        BitmapAlgebraOp::Or,
        BitmapAlgebraOp::AndNot,
        BitmapAlgebraOp::Xor,
    ];

    /// How a side's retained index orders its physical rows. Timestamps ascend in
    /// both regimes, so each is a legal checkpoint; only key order differs, and an
    /// unsorted side takes the radix join.
    #[derive(Clone, Copy, Debug)]
    enum IndexOrder {
        KeySorted,
        KeyUnsorted,
    }

    /// Physical rows for `keys` in the requested key order. Row `i` carries
    /// timestamp `(i + 1) / 2`, so finite stamps ascend in physical order.
    fn index_rows(mut keys: Vec<u32>, order: IndexOrder) -> Vec<(u32, u64)> {
        match order {
            IndexOrder::KeySorted => keys.sort_unstable(),
            // An odd multiplier is a bijection on u32: a fixed scramble of the
            // distinct keys that is not key-ascending.
            IndexOrder::KeyUnsorted => {
                keys.sort_unstable_by_key(|key| key.wrapping_mul(0x9E37_79B9));
            }
        }
        let rows: Vec<(u32, u64)> = keys
            .into_iter()
            .enumerate()
            .map(|(index, key)| (key, ((index + 1) as f64 * 0.5).to_bits()))
            .collect();
        let key_ascending = rows.windows(2).all(|pair| pair[0].0 <= pair[1].0);
        assert_eq!(
            key_ascending,
            matches!(order, IndexOrder::KeySorted),
            "{order:?} index has the requested key order"
        );
        rows
    }

    /// Target members 1..=24, with retained rows for them and for `STALE_KEY`.
    fn target_side(order: IndexOrder) -> (Vec<u32>, Vec<(u32, u64)>) {
        let member_keys: Vec<u32> = (1..=24).collect();
        let mut row_keys = member_keys.clone();
        row_keys.push(STALE_KEY);
        (member_keys, index_rows(row_keys, order))
    }

    /// Source members 12..=36. Rows exist only for 12..=30, so members 31..=36
    /// carry no expiry.
    fn source_side(order: IndexOrder) -> (Vec<u32>, Vec<(u32, u64)>) {
        let member_keys: Vec<u32> = (12..=36).collect();
        let row_keys: Vec<u32> = (12..=30).collect();
        (member_keys, index_rows(row_keys, order))
    }

    /// Independent set model of the four operations over the fixture members.
    fn expected_members(op: BitmapAlgebraOp) -> Vec<u32> {
        let target: BTreeSet<u32> = (1..=24).collect();
        let source: BTreeSet<u32> = (12..=36).collect();
        match op {
            BitmapAlgebraOp::And => target.intersection(&source).copied().collect(),
            BitmapAlgebraOp::Or => target.union(&source).copied().collect(),
            BitmapAlgebraOp::AndNot => target.difference(&source).copied().collect(),
            BitmapAlgebraOp::Xor => target.symmetric_difference(&source).copied().collect(),
        }
    }

    /// Seeds both TTL slots through the shared checkpoint helper, then opens the
    /// undo lane on the seeded image.
    fn seeded_checkpoint(target: IndexOrder, source: IndexOrder) -> (Vec<u8>, Vm) {
        let mut state = init_program_state(&ttl_algebra_program(true));
        let mut vm = Vm::default();
        let (target_members, target_rows) = target_side(target);
        seed_bitmap_checkpoint(&mut vm, &mut state, TARGET, &target_members, &target_rows);
        let (source_members, source_rows) = source_side(source);
        seed_bitmap_checkpoint(&mut vm, &mut state, SOURCE, &source_members, &source_rows);
        vm.undo_enable(&state)
            .expect("undo lane opens on the seeded checkpoint");
        (state, vm)
    }

    /// Storage of the target slot, read from the state's own slot metadata.
    fn target_storage(state: &[u8]) -> BitmapStorage {
        get_bitmap_storage(&bind_slot(state, TARGET).expect("target slot geometry"))
    }

    /// One warm-up round, outside any counted region. Each operation must return
    /// `Ok`, match the set model, change the image, and undo to the exact checkpoint
    /// bytes. Formatting happens only inside these assertions.
    fn warm_round(vm: &mut Vm, state: &mut [u8], delta: bool, checkpoint: u32, initial: &[u8]) {
        for op in ALGEBRA_OPS {
            assert_eq!(
                vm.bitmap_algebra(delta, op, state, TARGET, BitmapSource::Slot(SOURCE)),
                ErrorCode::Ok,
                "{op:?} delta={delta}: the public algebra accepts the fixture"
            );
            assert_eq!(
                members(state, target_storage(state)),
                expected_members(op),
                "{op:?} delta={delta}: target membership matches the set model"
            );
            assert_ne!(
                &*state, initial,
                "{op:?} delta={delta}: the operation changes the image"
            );
            rollback(vm, state, checkpoint);
            assert_eq!(
                &*state, initial,
                "{op:?} delta={delta}: undo restores the checkpoint image"
            );
        }
    }

    /// One algebra call and its undo to `checkpoint`. Outcomes are counted, not
    /// asserted, so nothing inside the counted region can format or allocate.
    fn algebra_then_undo(
        vm: &mut Vm,
        state: &mut [u8],
        delta: bool,
        op: BitmapAlgebraOp,
        checkpoint: u32,
        state_len: u32,
    ) -> u32 {
        let algebra = vm.bitmap_algebra(delta, op, state, TARGET, BitmapSource::Slot(SOURCE));
        let restored = vm.undo_rollback(state, checkpoint);
        u32::from(algebra != ErrorCode::Ok) + u32::from(restored != Ok(state_len))
    }

    /// Warms every operation on a fresh checkpoint, then counts each operation's
    /// allocation calls over `MEASURED_ROUNDS` algebra+undo rounds.
    fn census(delta: bool, target: IndexOrder, source: IndexOrder) {
        let (mut state, mut vm) = seeded_checkpoint(target, source);
        let initial = state.clone();
        let checkpoint = vm.undo_checkpoint();
        let state_len = u32::try_from(state.len()).expect("test image fits u32");

        for _ in 0..WARMUP_ROUNDS {
            warm_round(&mut vm, &mut state, delta, checkpoint, &initial);
        }

        for op in ALGEBRA_OPS {
            let (failures, allocations) = allocations_during(|| {
                let mut failures = 0;
                for _ in 0..MEASURED_ROUNDS {
                    failures +=
                        algebra_then_undo(&mut vm, &mut state, delta, op, checkpoint, state_len);
                }
                failures
            });
            assert_eq!(
                failures, 0,
                "{op:?} delta={delta} target={target:?} source={source:?}: every counted outcome is Ok and restored"
            );
            assert_eq!(
                allocations, 0,
                "{op:?} delta={delta} target={target:?} source={source:?}: post-warm allocation calls over {MEASURED_ROUNDS} rounds"
            );
        }

        assert_eq!(state, initial, "the census ends on the checkpoint image");
        assert!(!vm.undo_has_overflow(), "the census journal stays exact");
    }

    /// Every pairing of key-sorted and unsorted sides, so the radix join runs for the
    /// target, for the source, and for both.
    const REGIMES: [(IndexOrder, IndexOrder); 4] = [
        (IndexOrder::KeySorted, IndexOrder::KeySorted),
        (IndexOrder::KeyUnsorted, IndexOrder::KeyUnsorted),
        (IndexOrder::KeySorted, IndexOrder::KeyUnsorted),
        (IndexOrder::KeyUnsorted, IndexOrder::KeySorted),
    ];

    /// Ordinary undo lane: the public algebra, then a complete rollback to the checkpoint.
    #[test]
    fn ordinary_undo_algebra_census_allocates_nothing_after_warmup() {
        for (target, source) in REGIMES {
            census(false, target, source);
        }
    }

    /// Paired lane: the same algebra and undo journaled through delta mode. Export and
    /// replay are never called here, so this measures only the mutation and undo path.
    #[test]
    fn paired_delta_algebra_census_allocates_nothing_after_warmup() {
        for (target, source) in REGIMES {
            census(true, target, source);
        }
    }
}

#[test]
fn ttl_algebra_full_retained_index_refuses_atomically_then_removes_before_upsert() {
    let init = [
        Opcode::SlotDef as u8,
        0,
        SlotType::Bitmap as u8 | TTL_FLAG,
        8,
        0,
    ];
    let mut declarations = init.to_vec();
    declarations.extend(10.0f32.to_le_bytes());
    declarations.extend(0.0f32.to_le_bytes());
    declarations.extend([1, 0]);
    declarations.extend([
        Opcode::SlotDef as u8,
        1,
        SlotType::Bitmap as u8 | TTL_FLAG,
        8,
        0,
    ]);
    declarations.extend(10.0f32.to_le_bytes());
    declarations.extend(0.0f32.to_le_bytes());
    declarations.extend([1, 0]);
    let prog = program(2, 2, &declarations, &[]);
    let target_members: Vec<u32> = (1..=15).collect();
    let target_rows: Vec<(u32, u64)> = (1..=16)
        .map(|key| (key, (f64::from(key) * 10.0).to_bits()))
        .collect();
    for delta in [false, true] {
        let mut state = init_program_state(&prog);
        let mut vm = Vm::default();
        seed_bitmap_checkpoint(&mut vm, &mut state, 0, &target_members, &target_rows);
        seed_bitmap_checkpoint(
            &mut vm,
            &mut state,
            1,
            &[2, 70_000],
            &[(2, 500.0f64.to_bits()), (70_000, 600.0f64.to_bits())],
        );
        let meta = SlotMetaView::read(&state, 0);
        assert_eq!(meta.capacity, 16);
        assert_eq!(meta.eviction_index_size(&state), 16);
        vm.undo_enable(&state).expect("valid speculation admission");
        let initial = state.clone();
        let checkpoint = vm.undo_checkpoint();
        assert_eq!(
            vm.bitmap_algebra(
                delta,
                BitmapAlgebraOp::Or,
                &mut state,
                0,
                BitmapSource::Slot(1)
            ),
            ErrorCode::CapacityExceeded,
            "a fitting primary result still refuses a full retained TTL index"
        );
        assert_eq!(
            state, initial,
            "refusal writes neither merged timestamps nor payload"
        );
        assert_eq!(vm.undo_checkpoint(), checkpoint);
        assert!(!vm.undo_has_overflow());
        assert_eq!(
            vm.bitmap_algebra(
                delta,
                BitmapAlgebraOp::Xor,
                &mut state,
                0,
                BitmapSource::Slot(1)
            ),
            ErrorCode::Ok,
            "removal of key 2 must create TTL room before adding key 70000"
        );
        assert_eq!(meta.size(&state), 15);
        assert_eq!(meta.eviction_index_size(&state), 16);
        assert!(!members(&state, get_bitmap_storage(&meta)).contains(&2));
        assert_eq!(
            find_latest_eviction_timestamp_for_key(&state, &meta, 70_000),
            Some(600.0)
        );
        let applied = state.clone();
        if delta {
            assert_delta_segment_replays(&mut vm, &initial, &applied, checkpoint);
        }
        rollback(&mut vm, &mut state, checkpoint);
        assert_eq!(state, initial, "delta={delta}: full retained index image");
    }
}

#[test]
fn ttl_algebra_then_real_expiry_restores_trigger_buffer_and_nested_delta_marks() {
    let prog = ttl_algebra_program_flags(true, TTL_FLAG | 0x20);
    for (op, expected_members, expected_expired) in [
        (BitmapAlgebraOp::Or, vec![2, 3, 4], 1),
        (BitmapAlgebraOp::And, vec![], 2),
        (BitmapAlgebraOp::AndNot, vec![], 1),
        (BitmapAlgebraOp::Xor, vec![4], 1),
    ] {
        for delta in [false, true] {
            let mut state = init_program_state(&prog);
            let mut vm = Vm::default();
            seed_bitmap_checkpoint(
                &mut vm,
                &mut state,
                0,
                &[1, 2, 3],
                &[
                    (1, 0.0f64.to_bits()),
                    (2, 10.0f64.to_bits()),
                    (3, 20.0f64.to_bits()),
                ],
            );
            seed_bitmap_checkpoint(
                &mut vm,
                &mut state,
                1,
                &[2, 3, 4],
                &[
                    (3, 5.0f64.to_bits()),
                    (4, 15.0f64.to_bits()),
                    (2, 30.0f64.to_bits()),
                ],
            );
            let meta = SlotMetaView::read(&state, 0);
            let buffer = meta.evicted_buffer_offset(&state) as usize;
            let end = buffer + (EVICTED_BUFFER_CAP * EVICTION_ENTRY_SIZE) as usize;
            state[buffer..end].fill(0xcd);
            vm.undo_enable(&state).expect("valid speculation admission");
            let initial = state.clone();
            let c1 = vm.undo_checkpoint();
            assert_eq!(
                vm.bitmap_algebra(delta, op, &mut state, 0, BitmapSource::Slot(1)),
                ErrorCode::Ok
            );
            let merged = state.clone();
            let c2 = vm.undo_checkpoint();
            if delta {
                assert_delta_segment_replays(&mut vm, &initial, &merged, c1);
            }
            let expired = vm.evict_all_expired(delta, &mut state, 25.0);
            // Source slot 1 has no trigger, but is also a real TTL slot. Its
            // key 3 at timestamp 5 expires in addition to the target count.
            assert_eq!(expired, Ok(expected_expired + 1));
            assert_eq!(meta.evicted_count(&state), expected_expired);
            assert_eq!(meta.size(&state) as usize, expected_members.len());
            assert_eq!(members(&state, get_bitmap_storage(&meta)), expected_members);
            let after_expiry = state.clone();
            if delta {
                assert_delta_segment_replays(&mut vm, &merged, &after_expiry, c2);
            }
            rollback(&mut vm, &mut state, c2);
            assert_eq!(
                state, merged,
                "op={op:?}, delta={delta}: expiry-only rollback"
            );
            rollback(&mut vm, &mut state, c1);
            assert_eq!(
                state, initial,
                "op={op:?}, delta={delta}: all physical bytes"
            );
        }
    }
}

#[test]
fn ttl_source_into_plain_bitmap_target_is_a_named_prewrite_refusal() {
    let mut init = vec![Opcode::SlotDef as u8, 0, SlotType::Bitmap as u8, 64, 0];
    init.extend([
        Opcode::SlotDef as u8,
        1,
        SlotType::Bitmap as u8 | TTL_FLAG,
        64,
        0,
    ]);
    init.extend(10.0f32.to_le_bytes());
    init.extend(0.0f32.to_le_bytes());
    init.extend([1, 0]);
    let prog = program(2, 2, &init, &[]);
    for delta in [false, true] {
        let mut state = init_program_state(&prog);
        let mut vm = Vm::default();
        seed_bitmap_checkpoint(&mut vm, &mut state, 0, &[1], &[]);
        seed_bitmap_checkpoint(&mut vm, &mut state, 1, &[2], &[(2, 30.0f64.to_bits())]);
        vm.undo_enable(&state).expect("valid speculation admission");
        let initial = state.clone();
        let checkpoint = vm.undo_checkpoint();
        assert_eq!(
            vm.bitmap_algebra(
                delta,
                BitmapAlgebraOp::Or,
                &mut state,
                0,
                BitmapSource::Slot(1)
            ),
            ErrorCode::InvalidProgram
        );
        assert_eq!(state, initial);
        assert_eq!(vm.undo_checkpoint(), checkpoint);
        assert!(!vm.undo_has_overflow());
    }
}

// ---------------------------------------------------------------------------
// Generated and named oracles for the TTL bitmap algebra, run on the real `Vm`.
// The expected result is computed here from the contract's optional-expiry
// rules over `BTreeSet`/`BTreeMap` truth tables. The production timestamp join
// and planner are never consulted as an oracle.
// ---------------------------------------------------------------------------

mod ttl_algebra_properties {
    use super::*;
    use std::collections::BTreeMap;

    /// Keys that generated memberships and retained rows draw from: single-chunk
    /// keys and two keys in the second 16-bit chunk.
    const GEN_KEYS: [u32; 8] = [1, 2, 3, 5, 8, 13, 65_539, 65_540];
    /// Quiet NaN with payload 1, and a negative quiet NaN with payload 2. Neither
    /// compares equal, less or greater to any timestamp.
    const NAN_PAYLOAD_POS: u64 = 0x7FF8_0000_0000_0001;
    const NAN_PAYLOAD_NEG: u64 = 0xFFF8_0000_0000_0002;
    /// Seeds per generated run. Each seed covers every op, source and journal mode.
    const GENERATED_SEEDS: u64 = 16;
    const ALL_OPS: [BitmapAlgebraOp; 4] = [
        BitmapAlgebraOp::And,
        BitmapAlgebraOp::Or,
        BitmapAlgebraOp::AndNot,
        BitmapAlgebraOp::Xor,
    ];
    const ALL_SOURCES: [GenSource; 5] = [
        GenSource::SelfSlot,
        GenSource::TtlSlot,
        GenSource::PlainSlot,
        GenSource::Bytes,
        GenSource::Scratch,
    ];

    /// Where the right-hand operand of the algebra comes from.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum GenSource {
        /// The target slot itself: the right operand is the left operand.
        SelfSlot,
        /// Slot 1 with a TTL index. Its retained rows are seeded.
        TtlSlot,
        /// Slot 1 without a TTL index: membership only.
        PlainSlot,
        /// Caller-supplied serialized membership: no expiry.
        Bytes,
        /// `env.algebra_result` membership: no expiry.
        Scratch,
    }

    /// One side's seeded state: membership and physical retained rows, in index order.
    #[derive(Clone, Debug, Default)]
    struct GenSide {
        members: BTreeSet<u32>,
        rows: Vec<(u32, u64)>,
    }

    #[derive(Clone, Debug)]
    struct GenCase {
        op: BitmapAlgebraOp,
        delta: bool,
        source: GenSource,
        target: GenSide,
        source_side: GenSide,
    }

    impl GenCase {
        /// Rows the source slot is seeded with. Only a TTL source slot has any.
        fn source_rows(&self) -> &[(u32, u64)] {
            match self.source {
                GenSource::TtlSlot => self.source_side.rows.as_slice(),
                _ => &[],
            }
        }
    }

    /// What one operation did to the target, judged by the model.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Outcome {
        /// No member and no optional expiry changed: no byte, flag or journal entry.
        Identity,
        /// Membership is unchanged and at least one retained expiry changed.
        TimestampOnly,
        /// At least one member was added or removed.
        MembershipChanged,
    }

    /// Timestamp bits the generator draws from: both zeros, equal values, the
    /// infinities, the canonical NaN, and quiet NaNs with payloads of both signs.
    fn stamp_palette() -> [u64; 10] {
        [
            1.0f64.to_bits(),
            2.0f64.to_bits(),
            5.0f64.to_bits(),
            0.0f64.to_bits(),
            (-0.0f64).to_bits(),
            f64::INFINITY.to_bits(),
            f64::NEG_INFINITY.to_bits(),
            f64::NAN.to_bits(),
            NAN_PAYLOAD_POS,
            NAN_PAYLOAD_NEG,
        ]
    }

    /// SplitMix64 with a caller-chosen seed, so every generated case replays
    /// exactly from its seed.
    struct Generator(u64);

    impl Generator {
        fn next_u64(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        /// A small index below `len`, for fixture generation.
        fn index(&mut self, len: usize) -> usize {
            let len = u64::try_from(len).expect("small fixture index domain");
            usize::try_from(self.next_u64() % len).expect("bounded fixture index")
        }
    }

    /// Fixture placement rule for retained rows: a lower-bound binary search over
    /// the physical rows so far, comparing timestamps with `<`. It is the test's
    /// own reference, not the production planner.
    fn protocol_lower_bound(rows: &[(u32, u64)], stamp: f64) -> usize {
        let (mut lo, mut hi) = (0usize, rows.len());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if f64::from_bits(rows[mid].1) < stamp {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// `rows` with `event` inserted at its protocol position.
    fn placed_row(rows: &[(u32, u64)], event: (u32, u64)) -> Vec<(u32, u64)> {
        let at = protocol_lower_bound(rows, f64::from_bits(event.1));
        let mut next = rows.to_vec();
        next.insert(at, event);
        next
    }

    /// Physical rows for `events` given in arrival order. Each event is placed at
    /// its protocol position and none is dropped, so the physical order is exactly
    /// what the lower-bound protocol produces, including NaN-induced finite disorder.
    fn place_rows(events: &[(u32, u64)]) -> Vec<(u32, u64)> {
        events
            .iter()
            .fold(Vec::new(), |rows, &event| placed_row(&rows, event))
    }

    /// A generated side. Every generated event is kept at its protocol position.
    fn generate_side(rng: &mut Generator, with_rows: bool) -> GenSide {
        let mut members = BTreeSet::new();
        for _ in 0..rng.index(6) {
            members.insert(GEN_KEYS[rng.index(GEN_KEYS.len())]);
        }
        let stamps = stamp_palette();
        let mut rows: Vec<(u32, u64)> = Vec::new();
        if with_rows {
            for _ in 0..rng.index(9) {
                let key = GEN_KEYS[rng.index(GEN_KEYS.len())];
                let bits = if rng.next_u64() & 1 == 0 {
                    stamps[rng.index(stamps.len())]
                } else {
                    rng.next_u64()
                };
                let event = (key, bits);
                rows = placed_row(&rows, event);
            }
        }
        GenSide { members, rows }
    }

    fn generate_case(
        rng: &mut Generator,
        op: BitmapAlgebraOp,
        source: GenSource,
        delta: bool,
    ) -> GenCase {
        let target = generate_side(rng, true);
        let source_side = generate_side(rng, source == GenSource::TtlSlot);
        GenCase {
            op,
            delta,
            source,
            target,
            source_side,
        }
    }

    /// A named-control side: membership plus rows given in arrival order.
    fn side(members: &[u32], events: &[(u32, u64)]) -> GenSide {
        GenSide {
            members: members.iter().copied().collect(),
            rows: place_rows(events),
        }
    }

    fn make_case(
        op: BitmapAlgebraOp,
        delta: bool,
        source: GenSource,
        target: GenSide,
        source_side: GenSide,
    ) -> GenCase {
        GenCase {
            op,
            delta,
            source,
            target,
            source_side,
        }
    }

    fn sorted_members(set: &BTreeSet<u32>) -> Vec<u32> {
        set.iter().copied().collect()
    }

    /// The independent model of one slot's optional expiries.
    #[derive(Clone, Debug, Default)]
    struct ModelSide {
        members: BTreeSet<u32>,
        /// Timestamp bits of the last physical row for each key, member or not.
        last_row: BTreeMap<u32, u64>,
    }

    impl ModelSide {
        fn of(members: &BTreeSet<u32>, rows: &[(u32, u64)]) -> Self {
            let mut last_row = BTreeMap::new();
            for &(key, bits) in rows {
                last_row.insert(key, bits);
            }
            Self {
                members: members.clone(),
                last_row,
            }
        }

        /// A non-member has no expiry, whatever stale row carries its key.
        fn expiry(&self, key: u32) -> Option<u64> {
            if self.members.contains(&key) {
                self.last_row.get(&key).copied()
            } else {
                None
            }
        }
    }

    #[derive(Debug, Default)]
    struct ModelResult {
        members: BTreeSet<u32>,
        expiry: BTreeMap<u32, u64>,
    }

    /// The membership truth table, written from the set-algebra definitions.
    fn model_keep(op: BitmapAlgebraOp, in_a: bool, in_b: bool) -> bool {
        match op {
            BitmapAlgebraOp::And => in_a && in_b,
            BitmapAlgebraOp::Or => in_a || in_b,
            BitmapAlgebraOp::AndNot => in_a && !in_b,
            BitmapAlgebraOp::Xor => in_a != in_b,
        }
    }

    /// Union: the larger of two present expiries. A tie or unordered pair keeps
    /// A's exact bits, so only a strictly larger B replaces A.
    fn model_larger(a: Option<u64>, b: Option<u64>) -> Option<u64> {
        match (a, b) {
            (Some(x), Some(y)) => Some(if f64::from_bits(y) > f64::from_bits(x) {
                y
            } else {
                x
            }),
            (Some(x), None) => Some(x),
            (None, present) => present,
        }
    }

    /// Intersection: the smaller of two present expiries, with the same tie rule.
    fn model_smaller(a: Option<u64>, b: Option<u64>) -> Option<u64> {
        match (a, b) {
            (Some(x), Some(y)) => Some(if f64::from_bits(y) < f64::from_bits(x) {
                y
            } else {
                x
            }),
            (Some(x), None) => Some(x),
            (None, present) => present,
        }
    }

    /// Expiry of an output member. A's member keeps A's expiry under A\B, and the
    /// sole contributing side supplies it under XOR.
    fn model_expiry(
        op: BitmapAlgebraOp,
        in_a: bool,
        a: Option<u64>,
        b: Option<u64>,
    ) -> Option<u64> {
        match op {
            BitmapAlgebraOp::Or => model_larger(a, b),
            BitmapAlgebraOp::And => model_smaller(a, b),
            BitmapAlgebraOp::AndNot => a,
            BitmapAlgebraOp::Xor => {
                if in_a {
                    a
                } else {
                    b
                }
            }
        }
    }

    fn model_algebra(op: BitmapAlgebraOp, a: &ModelSide, b: &ModelSide) -> ModelResult {
        let mut result = ModelResult::default();
        let universe: BTreeSet<u32> = a.members.union(&b.members).copied().collect();
        for key in universe {
            let in_a = a.members.contains(&key);
            let in_b = b.members.contains(&key);
            if !model_keep(op, in_a, in_b) {
                continue;
            }
            result.members.insert(key);
            if let Some(bits) = model_expiry(op, in_a, a.expiry(key), b.expiry(key)) {
                result.expiry.insert(key, bits);
            }
        }
        result
    }

    /// Physical retained rows of a slot in index order, as `(key, timestamp bits)`.
    fn index_rows(state: &[u8], meta: &SlotMetaView) -> Vec<(u32, u64)> {
        let base = meta.eviction_index_offset(state) as usize;
        let count = meta.eviction_index_size(state) as usize;
        let entry_size = EVICTION_ENTRY_SIZE as usize;
        let timestamp_at = std::mem::offset_of!(columine_types::types::EvictionEntry, timestamp);
        let key_at = std::mem::offset_of!(columine_types::types::EvictionEntry, key_or_idx);
        (0..count)
            .map(|index| {
                let entry = base + index * entry_size;
                let key = u32::from_le_bytes(
                    state[entry + key_at..entry + key_at + 4]
                        .try_into()
                        .expect("key lane"),
                );
                let bits = u64::from_le_bytes(
                    state[entry + timestamp_at..entry + timestamp_at + 8]
                        .try_into()
                        .expect("timestamp lane"),
                );
                (key, bits)
            })
            .collect()
    }

    /// The current entry of `key`: its last physical row, as the contract defines it.
    fn last_row_bits(rows: &[(u32, u64)], key: u32) -> Option<u64> {
        rows.iter().rev().find(|row| row.0 == key).map(|row| row.1)
    }

    /// Rows not selected by the independent expiry model retain their relative
    /// physical order and exact bits, including duplicates and related identities.
    fn untouched_rows(rows: &[(u32, u64)], touched: &BTreeSet<u32>) -> Vec<(u32, u64)> {
        rows.iter()
            .copied()
            .filter(|row| !touched.contains(&row.0))
            .collect()
    }

    /// Every byte a slot owns: its metadata record, its payload capacity, and its
    /// eviction index capacity when it has one.
    fn slot_region_bytes(state: &[u8], slot: u32) -> Vec<u8> {
        let meta = columine_vm::state_init::bind_slot(state, slot).expect("slot geometry");
        let storage = get_bitmap_storage(&meta);
        let record = meta.meta_base as usize;
        let payload = storage.payload_offset() as usize;
        let mut bytes = state[record..record + SLOT_META_SIZE as usize].to_vec();
        bytes.extend_from_slice(&state[payload..payload + storage.payload_capacity as usize]);
        if meta.has_ttl() {
            let index = meta.eviction_index_offset(state) as usize;
            let len = meta.eviction_index_capacity(state) as usize * EVICTION_ENTRY_SIZE as usize;
            bytes.extend_from_slice(&state[index..index + len]);
        }
        bytes
    }

    fn show_bits(bits: Option<u64>) -> String {
        match bits {
            Some(bits) => format!("{:?} ({bits:#018x})", f64::from_bits(bits)),
            None => "None".to_owned(),
        }
    }

    /// Run one case on a real `Vm` and check every observable against the model.
    /// `label` names the case in each failure, so a seeded failure replays exactly.
    fn check_case(label: &str, case: &GenCase) -> Outcome {
        let program = ttl_algebra_program(matches!(case.source, GenSource::TtlSlot));
        let mut state = init_program_state(&program);
        let mut vm = Vm::default();

        seed_bitmap_checkpoint(
            &mut vm,
            &mut state,
            0,
            &sorted_members(&case.target.members),
            &case.target.rows,
        );
        match case.source {
            GenSource::TtlSlot | GenSource::PlainSlot => seed_bitmap_checkpoint(
                &mut vm,
                &mut state,
                1,
                &sorted_members(&case.source_side.members),
                case.source_rows(),
            ),
            GenSource::Scratch => assert_eq!(
                set_algebra(
                    &mut vm.bitmap_env,
                    BitmapAlgebraOp::Or,
                    &serialize(&sorted_members(&case.source_side.members)),
                    &[],
                ),
                ErrorCode::Ok,
                "{label}: scratch operand"
            ),
            GenSource::SelfSlot | GenSource::Bytes => {}
        }

        let target_meta = columine_vm::state_init::bind_slot(&state, 0).expect("target geometry");
        vm.undo_enable(&state)
            .expect("undo enable on a seeded state");
        let checkpoint = vm.undo_checkpoint();
        let initial = state.clone();
        let flags_before = target_meta.change_flags(&state);
        let source_region_before = match case.source {
            GenSource::TtlSlot | GenSource::PlainSlot => Some(slot_region_bytes(&state, 1)),
            GenSource::SelfSlot | GenSource::Bytes | GenSource::Scratch => None,
        };

        let source_bytes = serialize(&sorted_members(&case.source_side.members));
        let code = match case.source {
            GenSource::SelfSlot => {
                vm.bitmap_algebra(case.delta, case.op, &mut state, 0, BitmapSource::Slot(0))
            }
            GenSource::TtlSlot | GenSource::PlainSlot => {
                vm.bitmap_algebra(case.delta, case.op, &mut state, 0, BitmapSource::Slot(1))
            }
            GenSource::Bytes => vm.bitmap_algebra(
                case.delta,
                case.op,
                &mut state,
                0,
                BitmapSource::Bytes(&source_bytes[..]),
            ),
            GenSource::Scratch => {
                vm.bitmap_algebra(case.delta, case.op, &mut state, 0, BitmapSource::Scratch)
            }
        };
        assert_eq!(
            code,
            ErrorCode::Ok,
            "{label}: bitmap_algebra on valid operands"
        );

        let target_model = ModelSide::of(&case.target.members, &case.target.rows);
        let source_model = match case.source {
            GenSource::SelfSlot => target_model.clone(),
            _ => ModelSide::of(&case.source_side.members, case.source_rows()),
        };
        let expected = model_algebra(case.op, &target_model, &source_model);

        let after_meta =
            columine_vm::state_init::bind_slot(&state, 0).expect("target geometry after algebra");
        assert_eq!(
            members(&state, get_bitmap_storage(&after_meta)),
            sorted_members(&expected.members),
            "{label}: memberships"
        );
        assert_eq!(
            after_meta.size(&state) as usize,
            expected.members.len(),
            "{label}: cardinality"
        );
        let rows_after = index_rows(&state, &after_meta);
        for &key in &expected.members {
            let want = expected.expiry.get(&key).copied();
            let got = last_row_bits(&rows_after, key);
            assert_eq!(
                got,
                want,
                "{label}: key {key} expiry: expected {}, observed {}",
                show_bits(want),
                show_bits(got)
            );
        }

        let membership_changed = expected.members != target_model.members;
        let mut touched: BTreeSet<u32> = target_model
            .members
            .difference(&expected.members)
            .copied()
            .filter(|key| target_model.last_row.contains_key(key))
            .collect();
        for &key in &expected.members {
            let expiry = expected.expiry.get(&key).copied();
            let changes_expiry = if target_model.members.contains(&key) {
                target_model.expiry(key) != expiry
            } else {
                expiry.is_some() || target_model.last_row.contains_key(&key)
            };
            if changes_expiry {
                touched.insert(key);
            }
        }
        let expiry_changed = !touched.is_empty();
        let outcome = if membership_changed {
            Outcome::MembershipChanged
        } else if expiry_changed {
            Outcome::TimestampOnly
        } else {
            Outcome::Identity
        };

        let flags_after = after_meta.change_flags(&state);
        let mut flags_expected = flags_before;
        if expected.members.len() != target_model.members.len() {
            flags_expected |= ChangeFlag::SIZE_CHANGED;
        }
        if expiry_changed {
            flags_expected |= ChangeFlag::UPDATED;
        }
        assert_eq!(
            flags_after, flags_expected,
            "{label}: exact SIZE_CHANGED and UPDATED byproducts"
        );

        assert_eq!(
            untouched_rows(&index_rows(&initial, &target_meta), &touched),
            untouched_rows(&rows_after, &touched),
            "{label}: every unchanged retained expiry preserves order and exact bits"
        );

        if let Some(before) = source_region_before {
            assert_eq!(
                slot_region_bytes(&state, 1),
                before,
                "{label}: the source slot is read-only"
            );
        }

        if outcome == Outcome::Identity {
            assert_eq!(
                vm.undo_checkpoint(),
                checkpoint,
                "{label}: identity journals nothing"
            );
            assert_eq!(state, initial, "{label}: identity changes no byte");
            return outcome;
        }

        assert!(
            vm.undo_checkpoint() > checkpoint,
            "{label}: a changed result is journaled"
        );
        if case.delta {
            assert_delta_segment_replays(&mut vm, &initial, &state, checkpoint);
        }
        rollback(&mut vm, &mut state, checkpoint);
        assert_eq!(
            state, initial,
            "{label}: ordinary rollback restores the complete image"
        );
        outcome
    }

    // -----------------------------------------------------------------------
    // Named controls.
    // -----------------------------------------------------------------------

    #[test]
    fn identity_controls_change_no_byte_and_journal_nothing() {
        let target_rows: [(u32, u64); 3] = [
            (1, 10.0f64.to_bits()),
            (2, f64::NAN.to_bits()),
            (3, 0.0f64.to_bits()),
        ];
        for op in ALL_OPS {
            for delta in [false, true] {
                let operands = match op {
                    BitmapAlgebraOp::And | BitmapAlgebraOp::Or => [
                        (GenSource::SelfSlot, side(&[], &[])),
                        (GenSource::TtlSlot, side(&[1, 2, 3], &target_rows)),
                        (GenSource::Bytes, side(&[1, 2, 3], &[])),
                    ],
                    BitmapAlgebraOp::AndNot | BitmapAlgebraOp::Xor => [
                        (GenSource::PlainSlot, side(&[], &[])),
                        (GenSource::TtlSlot, side(&[], &target_rows)),
                        (GenSource::Bytes, side(&[], &[])),
                    ],
                };
                for (source, source_side) in operands {
                    let case = make_case(
                        op,
                        delta,
                        source,
                        side(&[1, 2, 3], &target_rows),
                        source_side,
                    );
                    let label = format!("identity {case:?}");
                    assert_eq!(check_case(&label, &case), Outcome::Identity, "{label}");
                }
            }
        }
    }

    #[test]
    fn timestamp_only_change_marks_updated_and_nothing_else() {
        let target = side(
            &[1, 2, 3],
            &[
                (1, 10.0f64.to_bits()),
                (2, 20.0f64.to_bits()),
                (3, f64::NAN.to_bits()),
            ],
        );
        let source = side(
            &[1, 2, 3],
            &[
                (1, 30.0f64.to_bits()),
                (2, 20.0f64.to_bits()),
                (3, 5.0f64.to_bits()),
            ],
        );
        for delta in [false, true] {
            for (op, expected) in [
                (BitmapAlgebraOp::Or, Outcome::TimestampOnly),
                (BitmapAlgebraOp::And, Outcome::Identity),
                (BitmapAlgebraOp::AndNot, Outcome::MembershipChanged),
                (BitmapAlgebraOp::Xor, Outcome::MembershipChanged),
            ] {
                let case = make_case(
                    op,
                    delta,
                    GenSource::TtlSlot,
                    target.clone(),
                    source.clone(),
                );
                let label = format!("{case:?}");
                assert_eq!(check_case(&label, &case), expected, "{label}");
            }
        }
    }

    #[test]
    fn mixed_membership_and_clock_change_sets_updated_and_inserts_the_new_member() {
        for delta in [false, true] {
            // Key 2 is shared and its expiry moves from 20.0 to 30.0 on the source's
            // clock, while key 3 is a new member carrying 5.0. Key 1 is A-only and stays.
            let case = make_case(
                BitmapAlgebraOp::Or,
                delta,
                GenSource::TtlSlot,
                side(&[1, 2], &[(1, 10.0f64.to_bits()), (2, 20.0f64.to_bits())]),
                side(&[2, 3], &[(2, 30.0f64.to_bits()), (3, 5.0f64.to_bits())]),
            );
            let label = format!("{case:?}");
            assert_eq!(
                check_case(&label, &case),
                Outcome::MembershipChanged,
                "{label}"
            );
        }
    }

    #[test]
    fn new_untracked_member_does_not_inherit_a_stale_target_row() {
        for op in [BitmapAlgebraOp::Or, BitmapAlgebraOp::Xor] {
            for source in [GenSource::Bytes, GenSource::Scratch] {
                for delta in [false, true] {
                    // Key 7 is stale in the target: its row exists, but 7 is not a member.
                    let case = make_case(
                        op,
                        delta,
                        source,
                        side(&[1], &[(7, 10.0f64.to_bits())]),
                        side(&[7], &[]),
                    );
                    let label = format!("{case:?}");
                    assert_eq!(
                        check_case(&label, &case),
                        Outcome::MembershipChanged,
                        "{label}"
                    );
                }
            }
        }
    }

    #[test]
    fn new_untracked_member_takes_only_the_contributing_source_expiry() {
        for op in [BitmapAlgebraOp::Or, BitmapAlgebraOp::Xor] {
            for delta in [false, true] {
                // The stale target row for 7 (10.0) must lose to the source's 20.0.
                let case = make_case(
                    op,
                    delta,
                    GenSource::TtlSlot,
                    side(&[1], &[(7, 10.0f64.to_bits())]),
                    side(&[7], &[(7, 20.0f64.to_bits())]),
                );
                let label = format!("{case:?}");
                assert_eq!(
                    check_case(&label, &case),
                    Outcome::MembershipChanged,
                    "{label}"
                );
            }
        }
    }

    #[test]
    fn stale_source_row_is_not_an_expiry_for_an_absent_member() {
        for op in ALL_OPS {
            for delta in [false, true] {
                // Key 1 is absent from the source, so the source row 99.0 is not its expiry.
                let case = make_case(
                    op,
                    delta,
                    GenSource::TtlSlot,
                    side(&[1], &[(1, 4.0f64.to_bits())]),
                    side(&[], &[(1, 99.0f64.to_bits()), (2, 50.0f64.to_bits())]),
                );
                let label = format!("{case:?}");
                let expected = match op {
                    BitmapAlgebraOp::And => Outcome::MembershipChanged,
                    _ => Outcome::Identity,
                };
                assert_eq!(check_case(&label, &case), expected, "{label}");
            }
        }
    }

    #[test]
    fn unrelated_stale_target_rows_are_not_normalized() {
        for delta in [false, true] {
            // Key 9 is stale and outside both operands; its row must survive.
            let case = make_case(
                BitmapAlgebraOp::Or,
                delta,
                GenSource::Bytes,
                side(
                    &[1, 2],
                    &[
                        (1, 3.0f64.to_bits()),
                        (9, 8.0f64.to_bits()),
                        (2, 6.0f64.to_bits()),
                    ],
                ),
                side(&[2], &[]),
            );
            let label = format!("{case:?}");
            assert_eq!(check_case(&label, &case), Outcome::Identity, "{label}");
        }
    }

    #[test]
    fn duplicate_nan_rows_current_entry_is_the_last_physical_row() {
        // Arrival order puts NaN_NEG last, but protocol placement puts it first, so
        // the current entry for key 1 is NaN_POS, the last physical row.
        let target = side(&[1], &[(1, NAN_PAYLOAD_POS), (1, NAN_PAYLOAD_NEG)]);
        for delta in [false, true] {
            for (op, source, source_side) in [
                (BitmapAlgebraOp::Or, GenSource::Bytes, side(&[1], &[])),
                (BitmapAlgebraOp::And, GenSource::Bytes, side(&[1], &[])),
                (
                    BitmapAlgebraOp::Or,
                    GenSource::TtlSlot,
                    side(&[1], &[(1, 2.0f64.to_bits())]),
                ),
                (
                    BitmapAlgebraOp::And,
                    GenSource::TtlSlot,
                    side(&[1], &[(1, 2.0f64.to_bits())]),
                ),
            ] {
                let case = make_case(op, delta, source, target.clone(), source_side);
                let label = format!("{case:?}");
                assert_eq!(check_case(&label, &case), Outcome::Identity, "{label}");
            }
        }
    }

    #[test]
    fn nan_payloads_and_signed_zero_ties_keep_a_exact_bits() {
        let target = side(
            &[1, 2, 3, 4, 5],
            &[
                (1, NAN_PAYLOAD_POS),
                (2, (-0.0f64).to_bits()),
                (3, 0.0f64.to_bits()),
                (4, NAN_PAYLOAD_NEG),
                (5, 1.0f64.to_bits()),
            ],
        );
        let source = side(
            &[1, 2, 3, 4, 5],
            &[
                (1, 2.0f64.to_bits()),
                (2, 0.0f64.to_bits()),
                (3, (-0.0f64).to_bits()),
                (4, 5.0f64.to_bits()),
                (5, 2.0f64.to_bits()),
            ],
        );
        for delta in [false, true] {
            for (op, expected) in [
                // Only key 5 is strictly larger on B, so union changes its bits alone.
                (BitmapAlgebraOp::Or, Outcome::TimestampOnly),
                // Intersection keeps A's bits everywhere except where B is strictly smaller.
                (BitmapAlgebraOp::And, Outcome::Identity),
                (BitmapAlgebraOp::AndNot, Outcome::MembershipChanged),
                (BitmapAlgebraOp::Xor, Outcome::MembershipChanged),
            ] {
                let case = make_case(
                    op,
                    delta,
                    GenSource::TtlSlot,
                    target.clone(),
                    source.clone(),
                );
                let label = format!("{case:?}");
                assert_eq!(check_case(&label, &case), expected, "{label}");
            }
        }
    }

    #[test]
    fn missing_side_matrix_follows_the_present_expiry() {
        /// A shape's label, then A's and B's `(key, expiry bits)` events.
        type Shape = (&'static str, Vec<(u32, u64)>, Vec<(u32, u64)>);
        let key_five: (u32, u64) = (5, 3.0f64.to_bits());
        let key_six: (u32, u64) = (6, 8.0f64.to_bits());
        // Key 1 is shared by both sides. Its expiry is present on A only, on B only,
        // or on neither. Keys 5 and 6 are one-sided members with their own expiries.
        let shapes: [Shape; 3] = [
            (
                "A has key 1's expiry, B does not",
                vec![(1, 10.0f64.to_bits()), key_five],
                vec![key_six],
            ),
            (
                "B has key 1's expiry, A does not",
                vec![key_five],
                vec![(1, 20.0f64.to_bits()), key_six],
            ),
            (
                "neither side has key 1's expiry",
                vec![key_five],
                vec![key_six],
            ),
        ];
        for (shape, target_events, source_events) in shapes {
            for op in ALL_OPS {
                for delta in [false, true] {
                    let case = make_case(
                        op,
                        delta,
                        GenSource::TtlSlot,
                        side(&[1, 5], &target_events),
                        side(&[1, 6], &source_events),
                    );
                    let label = format!("{shape}: {case:?}");
                    check_case(&label, &case);
                }
            }
        }
    }

    #[test]
    fn self_and_empty_operands_follow_the_operand_algebra() {
        let target = side(
            &[1, 2, 3],
            &[
                (1, 4.0f64.to_bits()),
                (2, 4.0f64.to_bits()),
                (3, f64::NAN.to_bits()),
            ],
        );
        for op in ALL_OPS {
            for delta in [false, true] {
                // The self operand is the target itself: And and Or are identities,
                // and AndNot and Xor empty the target.
                let self_case = make_case(
                    op,
                    delta,
                    GenSource::SelfSlot,
                    target.clone(),
                    side(&[], &[]),
                );
                let label = format!("{self_case:?}");
                let expected = match op {
                    BitmapAlgebraOp::And | BitmapAlgebraOp::Or => Outcome::Identity,
                    BitmapAlgebraOp::AndNot | BitmapAlgebraOp::Xor => Outcome::MembershipChanged,
                };
                assert_eq!(check_case(&label, &self_case), expected, "{label}");

                // Empty operands of every kind carry stale rows that must not become expiries.
                for (source, source_side) in [
                    (
                        GenSource::TtlSlot,
                        side(&[], &[(1, 9.0f64.to_bits()), (2, 9.0f64.to_bits())]),
                    ),
                    (GenSource::Bytes, side(&[], &[])),
                    (GenSource::Scratch, side(&[], &[])),
                    (GenSource::PlainSlot, side(&[], &[])),
                ] {
                    let case = make_case(op, delta, source, target.clone(), source_side);
                    check_case(&format!("{case:?}"), &case);
                }
            }
        }
    }

    #[test]
    fn membership_only_operands_carry_no_expiry() {
        for source in [GenSource::Bytes, GenSource::Scratch, GenSource::PlainSlot] {
            for delta in [false, true] {
                // Key 3 is stale in the target and becomes a new member through the
                // source. It must not inherit the 1.0 row.
                let case = make_case(
                    BitmapAlgebraOp::Or,
                    delta,
                    source,
                    side(
                        &[1, 2],
                        &[
                            (1, 3.0f64.to_bits()),
                            (2, 4.0f64.to_bits()),
                            (3, 1.0f64.to_bits()),
                        ],
                    ),
                    side(&[2, 3], &[]),
                );
                let label = format!("{case:?}");
                assert_eq!(
                    check_case(&label, &case),
                    Outcome::MembershipChanged,
                    "{label}"
                );
            }
        }
    }

    #[test]
    fn nan_binary_placement_retains_generated_finite_disorder_without_filtering() {
        let target = side(
            &[1, 2, 3],
            &[
                (1, 1.0f64.to_bits()),
                (2, 2.0f64.to_bits()),
                (40, NAN_PAYLOAD_POS),
                (41, NAN_PAYLOAD_NEG),
                (42, NAN_PAYLOAD_POS),
                (3, 3.0f64.to_bits()),
            ],
        );
        assert_eq!(target.rows.first(), Some(&(3, 3.0f64.to_bits())));
        for delta in [false, true] {
            check_case(
                "unfiltered native NaN lower-bound order",
                &make_case(
                    BitmapAlgebraOp::Or,
                    delta,
                    GenSource::TtlSlot,
                    target.clone(),
                    side(&[1], &[(1, 10.0f64.to_bits())]),
                ),
            );
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn arbitrary_seed_optional_bit_timestamp_algebra_is_reversible(
            seed in any::<u64>(),
            op_index in 0usize..4,
            source_index in 0usize..5,
            delta in any::<bool>(),
        ) {
            let mut rng = Generator(seed);
            let case = generate_case(&mut rng, ALL_OPS[op_index], ALL_SOURCES[source_index], delta);
            check_case(&format!("arbitrary seed {seed}: {case:?}"), &case);
        }
    }

    #[test]
    fn generated_memberships_and_optional_expiry_match_the_real_vm() {
        for seed in 0..GENERATED_SEEDS {
            let mut rng = Generator(seed);
            for op in ALL_OPS {
                for source in ALL_SOURCES {
                    for delta in [false, true] {
                        let case = generate_case(&mut rng, op, source, delta);
                        check_case(&format!("seed {seed}: {case:?}"), &case);
                    }
                }
            }
        }
    }
}
