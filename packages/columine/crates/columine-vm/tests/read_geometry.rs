//! Native proofs for the borrowed checked readers in `columine_vm::vm`.
//!
//! Every fixture is a real `init_state` image populated by real batch opcodes.
//! Every corruption is a byte edit of that image, and each checked view borrows
//! the image immutably, so every refusal is asserted against unchanged bytes.

use columine_types::PROGRAM_MAGIC;
use columine_types::types::{
    EMPTY_KEY, ErrorCode, Opcode, SLOT_META_SIZE, STATE_HEADER_SIZE, SlotMetaOffset, SlotType,
    StructFieldType,
};
use columine_vm::state_init::{DEFAULT_ACCEPTED_PROGRAM_MAGICS, calculate_state_size, init_state};
use columine_vm::struct_map::{StructMap2Slot, StructMapSlot};
use columine_vm::vm::{
    CheckedMap, CheckedSet, CheckedStructMap, CheckedStructMap2, Vm, u32s_as_bytes,
};

const SLOT_MAP: u32 = 0;
const SLOT_SET: u32 = 1;
const SLOT_BITMAP: u32 = 2;
const SLOT_STRUCT: u32 = 3;
const SLOT_PAIR: u32 = 4;
const SLOTS: u32 = 5;
const SLOT_CAPACITY: u32 = 16;
/// The capacity a SLOT_DEF of `SLOT_CAPACITY` lays out: doubled for load
/// factor and rounded to a power of two (`state_init::slot_def_capacity`).
const PHYSICAL_CAPACITY: u32 = 32;
const OK: u32 = ErrorCode::Ok as u32;

const KEYS: [u32; 3] = [1, 2, 3];
const VALUES: [u32; 3] = [10, u32::MAX, 30];
const SET_ELEMS: [u32; 3] = [5, 6, 7];
const BITMAP_ELEMS: [u32; 3] = [100, 200, 300];
const KEY2: [u32; 3] = [10, 11, 12];

fn program(init: &[u8], num_inputs: u8, reduce: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; 32];
    out.extend(PROGRAM_MAGIC.to_le_bytes());
    out.extend([2, 0, 0, 0]); // version 2, no callbacks, no flags
    out.extend(SLOTS.to_le_bytes());
    out.extend(u32::from(num_inputs).to_le_bytes());
    out.extend(u32::try_from(init.len()).unwrap().to_le_bytes());
    out.extend(u32::try_from(reduce.len()).unwrap().to_le_bytes());
    out.extend_from_slice(init);
    out.extend_from_slice(reduce);
    out
}

fn slot_def(slot: u32, slot_type: SlotType) -> Vec<u8> {
    let [lo, hi] = (SLOT_CAPACITY as u16).to_le_bytes();
    vec![Opcode::SlotDef as u8, slot as u8, slot_type as u8, lo, hi]
}

/// One UINT32 field, so each struct-map row carries exactly the upserted value.
fn struct_def(op: Opcode, slot: u32, slot_type: SlotType) -> Vec<u8> {
    let [lo, hi] = (SLOT_CAPACITY as u16).to_le_bytes();
    vec![
        op as u8,
        slot as u8,
        slot_type as u8,
        lo,
        hi,
        1,
        StructFieldType::UInt32 as u8,
    ]
}

/// Columns: 0 = key, 1 = value, 2 = set element, 3 = bitmap element, 4 = second key.
fn reduce() -> Vec<u8> {
    vec![
        Opcode::BatchMapUpsertLast as u8,
        SLOT_MAP as u8,
        0,
        1,
        Opcode::BatchSetInsert as u8,
        SLOT_SET as u8,
        2,
        Opcode::BatchBitmapAdd as u8,
        SLOT_BITMAP as u8,
        3,
        Opcode::BatchStructMapUpsertLast as u8,
        SLOT_STRUCT as u8,
        0,
        1,
        1,
        0,
        0,
        Opcode::BatchStructMap2UpsertLast as u8,
        SLOT_PAIR as u8,
        0,
        4,
        1,
        1,
        0,
    ]
}

fn fixture_program() -> Vec<u8> {
    let mut init = Vec::new();
    init.extend(slot_def(SLOT_MAP, SlotType::HashMap));
    init.extend(slot_def(SLOT_SET, SlotType::HashSet));
    init.extend(slot_def(SLOT_BITMAP, SlotType::Bitmap));
    init.extend(struct_def(
        Opcode::SlotStructMap,
        SLOT_STRUCT,
        SlotType::StructMap,
    ));
    init.extend(struct_def(
        Opcode::SlotStructMap2,
        SLOT_PAIR,
        SlotType::StructMap2,
    ));
    init.push(Opcode::Halt as u8);
    program(&init, 5, &reduce())
}

fn init(program: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(program, DEFAULT_ACCEPTED_PROGRAM_MAGICS)
        .expect("valid program admission");
    assert!(size > 0);
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, program, DEFAULT_ACCEPTED_PROGRAM_MAGICS).unwrap();
    state
}

/// The fixture with three live rows in every slot kind, written by real opcodes.
fn fixture() -> Vec<u8> {
    let prog = fixture_program();
    let mut state = init(&prog);
    let mut vm = Vm::default();
    let columns: [&[u8]; 5] = [
        u32s_as_bytes(&KEYS),
        u32s_as_bytes(&VALUES),
        u32s_as_bytes(&SET_ELEMS),
        u32s_as_bytes(&BITMAP_ELEMS),
        u32s_as_bytes(&KEY2),
    ];
    assert_eq!(
        OK,
        vm.execute_batch(&mut state, &prog, &columns, KEYS.len() as u32)
    );
    state
}

/// The same program initialised with no rows: every slot is valid and empty.
fn empty_state() -> Vec<u8> {
    init(&fixture_program())
}

fn field(slot: u32, offset: u32) -> usize {
    (STATE_HEADER_SIZE + slot * SLOT_META_SIZE + offset) as usize
}

fn read_u16(state: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(state[at..at + 2].try_into().unwrap())
}

fn write_u16(state: &mut [u8], at: usize, value: u16) {
    state[at..at + 2].copy_from_slice(&value.to_le_bytes());
}

fn read_u32(state: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(state[at..at + 4].try_into().unwrap())
}

fn write_u32(state: &mut [u8], at: usize, value: u32) {
    state[at..at + 4].copy_from_slice(&value.to_le_bytes());
}

/// The key lane at a slot's metadata offset (the hash lane for map and set).
fn lane_of(state: &[u8], slot: u32) -> usize {
    read_u32(state, field(slot, SlotMetaOffset::OFFSET)) as usize
}

/// The first cell of `lane` holding no key, read straight from the image.
fn dead_cell(state: &[u8], lane: usize, capacity: u32) -> u32 {
    (0..capacity)
        .find(|&pos| read_u32(state, lane + 4 * pos as usize) == EMPTY_KEY)
        .expect("fixture keeps dead cells")
}

/// Bind `slot` through the checked view of its own kind and discard the view.
fn bind_own_kind(state: &[u8], slot: u32) -> Result<(), ErrorCode> {
    match slot {
        SLOT_MAP => CheckedMap::bind(state, slot).map(drop),
        SLOT_SET | SLOT_BITMAP => CheckedSet::bind(state, slot).map(drop),
        SLOT_STRUCT => CheckedStructMap::bind(state, slot).map(drop),
        SLOT_PAIR => CheckedStructMap2::bind(state, slot).map(drop),
        other => panic!("fixture has no slot {other}"),
    }
}

#[test]
fn valid_views_answer_present_absent_and_max_values() {
    let state = fixture();
    let before = state.clone();

    let map = CheckedMap::bind(&state, SLOT_MAP).unwrap();
    assert_eq!(map.capacity(), PHYSICAL_CAPACITY);
    assert_eq!(map.get(1), Some(10));
    assert_eq!(
        map.get(2),
        Some(u32::MAX),
        "a mapped u32::MAX is present, not absent"
    );
    assert_eq!(map.get(99), None, "an absent key is a valid None");
    assert_eq!(map.get(EMPTY_KEY), None, "a sentinel key is never present");

    let set = CheckedSet::bind(&state, SLOT_SET).unwrap();
    assert!(set.contains(5));
    assert!(!set.contains(99), "an absent element is a valid false");

    let bitmap = CheckedSet::bind(&state, SLOT_BITMAP).unwrap();
    assert!(bitmap.contains(200));
    assert!(
        !bitmap.contains(201),
        "an absent bitmap member is a valid false"
    );

    let rows = CheckedStructMap::bind(&state, SLOT_STRUCT).unwrap();
    assert!(rows.row(3).is_some());
    assert_eq!(
        rows.row(4),
        None,
        "an absent struct-map key is a valid None"
    );

    let pairs = CheckedStructMap2::bind(&state, SLOT_PAIR).unwrap();
    assert!(pairs.row(2, 11).is_some());
    assert_eq!(
        pairs.row(2, 12),
        None,
        "an absent exact pair is a valid None"
    );

    assert_eq!(state, before, "reads never touch the image");
}

#[test]
fn empty_slots_report_valid_absence_and_terminal_ends() {
    let state = empty_state();

    let map = CheckedMap::bind(&state, SLOT_MAP).unwrap();
    assert_eq!(map.get(1), None);
    assert_eq!(map.iter_start(), map.capacity());

    let set = CheckedSet::bind(&state, SLOT_SET).unwrap();
    assert!(!set.contains(5));
    assert_eq!(set.iter_start(), set.capacity());

    let bitmap = CheckedSet::bind(&state, SLOT_BITMAP).unwrap();
    assert!(!bitmap.contains(100));
    assert_eq!(bitmap.iter_start(), bitmap.capacity());

    let rows = CheckedStructMap::bind(&state, SLOT_STRUCT).unwrap();
    assert_eq!(rows.row(1), None);
    assert_eq!(rows.iter_start(), rows.capacity());

    let pairs = CheckedStructMap2::bind(&state, SLOT_PAIR).unwrap();
    assert_eq!(pairs.row(1, 10), None);
    assert_eq!(pairs.iter_start(), pairs.capacity());
}

#[test]
fn map_iteration_is_ascending_and_packs_key_then_value() {
    let state = fixture();
    let map = CheckedMap::bind(&state, SLOT_MAP).unwrap();
    let mut cells = Vec::new();
    let mut pos = map.iter_start();
    while pos < map.capacity() {
        cells.push((pos, map.iter_get(pos).unwrap()));
        let next = map.iter_next(pos).unwrap();
        assert!(next > pos, "the cursor must advance");
        pos = next;
    }
    assert_eq!(pos, map.capacity(), "the successful end is the capacity");

    let mut pairs: Vec<(u32, u32)> = cells
        .iter()
        .map(|&(_, packed)| (packed as u32, (packed >> 32) as u32))
        .collect();
    pairs.sort_unstable();
    assert_eq!(pairs, vec![(1, 10), (2, u32::MAX), (3, 30)]);
}

#[test]
fn bitmap_ranks_walk_cardinality_then_capacity() {
    let state = fixture();
    let bitmap = CheckedSet::bind(&state, SLOT_BITMAP).unwrap();
    let mut ranks = Vec::new();
    let mut pos = bitmap.iter_start();
    assert_eq!(pos, 0, "a non-empty set starts at rank zero");
    while pos < bitmap.capacity() {
        ranks.push(bitmap.iter_get(pos).unwrap());
        pos = bitmap.iter_next(pos).unwrap();
    }
    assert_eq!(ranks, BITMAP_ELEMS.to_vec());
    assert_eq!(pos, bitmap.capacity());
}

#[test]
fn composite_rows_iterate_every_exact_pair_once() {
    let state = fixture();
    let pairs = CheckedStructMap2::bind(&state, SLOT_PAIR).unwrap();
    let view = StructMap2Slot::bind(&state, SLOT_PAIR);
    let value_at = view.field_offset(&state, 0);
    let mut seen = Vec::new();
    let mut pos = pairs.iter_start();
    while pos < pairs.capacity() {
        let key1 = pairs.iter_key1(pos).unwrap();
        let key2 = pairs.iter_key2(pos).unwrap();
        let row = pairs
            .row(key1, key2)
            .expect("an iterated pair resolves to its own row");
        assert!(StructMapSlot::is_field_set(&state, row, 0));
        seen.push((key1, key2, read_u32(&state, (row + value_at) as usize)));
        pos = pairs.iter_next(pos).unwrap();
    }
    seen.sort_unstable();
    assert_eq!(seen, vec![(1, 10, 10), (2, 11, u32::MAX), (3, 12, 30)]);
}

#[test]
fn struct_map_iteration_yields_each_live_key_once() {
    let state = fixture();
    let rows = CheckedStructMap::bind(&state, SLOT_STRUCT).unwrap();
    let mut keys = Vec::new();
    let mut pos = rows.iter_start();
    while pos < rows.capacity() {
        keys.push(rows.iter_key(pos).unwrap());
        pos = rows.iter_next(pos).unwrap();
    }
    keys.sort_unstable();
    assert_eq!(keys, KEYS.to_vec());
}

#[test]
fn capacity_three_is_malformed_geometry_for_every_kind() {
    let good = fixture();
    for slot in [SLOT_MAP, SLOT_SET, SLOT_BITMAP, SLOT_STRUCT, SLOT_PAIR] {
        let mut bad = good.clone();
        write_u32(&mut bad, field(slot, SlotMetaOffset::CAPACITY), 3);
        let before = bad.clone();
        assert!(
            matches!(bind_own_kind(&bad, slot), Err(ErrorCode::InvalidState)),
            "slot {slot} with capacity 3 must be refused as InvalidState"
        );
        assert_eq!(bad, before, "a refused bind never edits the image");
    }
}

#[test]
fn short_image_and_truncated_extent_are_malformed_geometry() {
    let good = fixture();
    assert!(matches!(
        CheckedMap::bind(&good[..8], SLOT_MAP),
        Err(ErrorCode::InvalidState)
    ));

    let metadata_end = (STATE_HEADER_SIZE + SLOTS * SLOT_META_SIZE) as usize;
    assert!(matches!(
        bind_own_kind(&good[..metadata_end], SLOT_MAP),
        Err(ErrorCode::InvalidState)
    ));

    let mut cut = good.clone();
    let tail = ((good.len() - 8) & !7) as u32;
    write_u32(&mut cut, field(SLOT_MAP, SlotMetaOffset::OFFSET), tail);
    let before = cut.clone();
    assert!(matches!(
        CheckedMap::bind(&cut, SLOT_MAP),
        Err(ErrorCode::InvalidState)
    ));
    assert_eq!(cut, before);
}

#[test]
fn wrong_kind_and_out_of_range_slot_are_invalid_slot() {
    let state = fixture();
    assert!(matches!(
        CheckedMap::bind(&state, SLOT_SET),
        Err(ErrorCode::InvalidSlot)
    ));
    assert!(matches!(
        CheckedSet::bind(&state, SLOT_MAP),
        Err(ErrorCode::InvalidSlot)
    ));
    assert!(matches!(
        CheckedSet::bind(&state, SLOT_STRUCT),
        Err(ErrorCode::InvalidSlot)
    ));
    assert!(matches!(
        CheckedStructMap::bind(&state, SLOT_PAIR),
        Err(ErrorCode::InvalidSlot)
    ));
    assert!(matches!(
        CheckedStructMap2::bind(&state, SLOT_STRUCT),
        Err(ErrorCode::InvalidSlot)
    ));
    assert!(matches!(
        CheckedMap::bind(&state, SLOTS),
        Err(ErrorCode::InvalidSlot)
    ));
}

#[test]
fn overlapping_or_misaligned_owner_offset_is_invalid_state() {
    let state = fixture();

    let mut overlap = state.clone();
    let map_offset = read_u32(&overlap, field(SLOT_MAP, SlotMetaOffset::OFFSET));
    write_u32(
        &mut overlap,
        field(SLOT_SET, SlotMetaOffset::OFFSET),
        map_offset,
    );
    assert!(matches!(
        CheckedSet::bind(&overlap, SLOT_SET),
        Err(ErrorCode::InvalidState)
    ));

    let mut misaligned = state.clone();
    let set_offset = read_u32(&misaligned, field(SLOT_SET, SlotMetaOffset::OFFSET));
    write_u32(
        &mut misaligned,
        field(SLOT_SET, SlotMetaOffset::OFFSET),
        set_offset + 4,
    );
    assert!(matches!(
        CheckedSet::bind(&misaligned, SLOT_SET),
        Err(ErrorCode::InvalidState)
    ));
}

#[test]
fn row_layout_disagreeing_with_metadata_is_invalid_state() {
    let state = fixture();

    let mut rows = state.clone();
    let row_size = read_u16(&rows, field(SLOT_STRUCT, SlotMetaOffset::TTL_SECONDS));
    write_u16(
        &mut rows,
        field(SLOT_STRUCT, SlotMetaOffset::TTL_SECONDS),
        row_size + 1,
    );
    assert!(matches!(
        CheckedStructMap::bind(&rows, SLOT_STRUCT),
        Err(ErrorCode::InvalidState)
    ));

    let mut pairs = state.clone();
    let row_size = read_u16(&pairs, field(SLOT_PAIR, SlotMetaOffset::TTL_SECONDS));
    write_u16(
        &mut pairs,
        field(SLOT_PAIR, SlotMetaOffset::TTL_SECONDS),
        row_size + 1,
    );
    assert!(matches!(
        CheckedStructMap2::bind(&pairs, SLOT_PAIR),
        Err(ErrorCode::InvalidState)
    ));
}

#[test]
fn malformed_bitmap_is_refused_at_bind_never_read_as_empty() {
    let state = fixture();

    let mut payload = state.clone();
    let image = read_u32(&payload, field(SLOT_BITMAP, SlotMetaOffset::OFFSET)) as usize;
    payload[image] = b'X';
    assert!(matches!(
        CheckedSet::bind(&payload, SLOT_BITMAP),
        Err(ErrorCode::InvalidState)
    ));

    let mut cardinality = state.clone();
    write_u32(
        &mut cardinality,
        field(SLOT_BITMAP, SlotMetaOffset::SIZE),
        2,
    );
    assert!(matches!(
        CheckedSet::bind(&cardinality, SLOT_BITMAP),
        Err(ErrorCode::InvalidState)
    ));
}

#[test]
fn payload_corruption_accepted_by_bounded_open_is_refused_by_checked_membership() {
    let mut state = fixture();
    let meta = columine_vm::meta::SlotMetaView::read(&state, SLOT_BITMAP);
    assert_eq!(
        Vm::default().set_insert(
            false,
            &mut state,
            SLOT_BITMAP,
            &[1, 7, 33, 85, 121, 209, 400, 900],
            None,
        ),
        ErrorCode::Ok,
    );
    let storage = columine_vm::bitmap_ops::get_bitmap_storage(&meta);
    let image = storage.serialized_data(&state).expect("real kernel image");
    assert!(bitmosaic::BitmosaicView::open_verified(image).is_some());
    let mut damaged = None;
    for at in 4..image.len() {
        for bit in 0..8 {
            let mut candidate = image.to_vec();
            candidate[at] ^= 1 << bit;
            if let Some(view) = bitmosaic::BitmosaicView::open(&candidate)
                && view.len() == u64::from(meta.size(&state))
                && bitmosaic::BitmosaicView::open_verified(&candidate).is_none()
            {
                damaged = Some(candidate);
                break;
            }
        }
        if damaged.is_some() {
            break;
        }
    }
    let damaged = damaged.expect("real irregular payload has a bounded but invalid corruption");
    let start = meta.offset as usize;
    state[start..start + damaged.len()].copy_from_slice(&damaged);
    let before = state.clone();
    assert!(matches!(
        CheckedSet::bind(&state, SLOT_BITMAP),
        Err(ErrorCode::InvalidState)
    ));
    assert_eq!(
        state, before,
        "refused verification never repairs or normalizes bytes"
    );
}

#[test]
fn invalid_iterator_positions_are_refused_without_sentinels() {
    let state = fixture();

    let map = CheckedMap::bind(&state, SLOT_MAP).unwrap();
    let cap = map.capacity();
    let dead = dead_cell(&state, lane_of(&state, SLOT_MAP), cap);
    assert!(matches!(map.iter_get(dead), Err(ErrorCode::InvalidState)));
    assert!(matches!(map.iter_get(cap), Err(ErrorCode::InvalidState)));
    assert!(matches!(
        map.iter_get(cap + 1),
        Err(ErrorCode::InvalidState)
    ));
    assert!(matches!(
        map.iter_next(cap + 1),
        Err(ErrorCode::InvalidState)
    ));
    assert_eq!(map.iter_next(cap).ok(), Some(cap), "the end is terminal");

    let set = CheckedSet::bind(&state, SLOT_SET).unwrap();
    let dead = dead_cell(&state, lane_of(&state, SLOT_SET), set.capacity());
    assert!(matches!(set.iter_get(dead), Err(ErrorCode::InvalidState)));

    let bitmap = CheckedSet::bind(&state, SLOT_BITMAP).unwrap();
    let cap = bitmap.capacity();
    assert_eq!(cap, PHYSICAL_CAPACITY);
    assert!(matches!(bitmap.iter_get(3), Err(ErrorCode::InvalidState)));
    assert!(matches!(bitmap.iter_next(3), Err(ErrorCode::InvalidState)));
    assert!(matches!(
        bitmap.iter_next(cap + 1),
        Err(ErrorCode::InvalidState)
    ));
    assert_eq!(bitmap.iter_next(cap).ok(), Some(cap));

    let rows = CheckedStructMap::bind(&state, SLOT_STRUCT).unwrap();
    let rows_lane = StructMapSlot::bind(&state, SLOT_STRUCT).keys_off as usize;
    let dead = dead_cell(&state, rows_lane, rows.capacity());
    assert!(matches!(rows.iter_key(dead), Err(ErrorCode::InvalidState)));
    assert!(matches!(
        rows.iter_next(rows.capacity() + 1),
        Err(ErrorCode::InvalidState)
    ));

    let pairs = CheckedStructMap2::bind(&state, SLOT_PAIR).unwrap();
    let pairs_lane = StructMap2Slot::bind(&state, SLOT_PAIR).keys1_off as usize;
    let dead = dead_cell(&state, pairs_lane, pairs.capacity());
    assert!(matches!(
        pairs.iter_key1(dead),
        Err(ErrorCode::InvalidState)
    ));
    assert!(matches!(
        pairs.iter_key2(dead),
        Err(ErrorCode::InvalidState)
    ));
    assert!(matches!(
        pairs.iter_key1(pairs.capacity()),
        Err(ErrorCode::InvalidState)
    ));
}
