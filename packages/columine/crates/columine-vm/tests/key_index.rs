//! The struct-map key-index seam, proved on the HashIndex family through real
//! programs, real batches and real images: the two-key prefix removal and its
//! journal, the program's key-lane declarations, the canonical seal, and the
//! family check of the state header.

use columine_types::PROGRAM_MAGIC;
use columine_types::types::{
    ChangeFlag, EMPTY_KEY, ErrorCode, Opcode, SLOT_META_SIZE, STATE_HEADER_SIZE, SlotMetaOffset,
    SlotType, SlotTypeFlags, StateHeaderOffset, StructFieldType,
};
use columine_vm::bytes;
use columine_vm::key_index::{HashIndex, KeyIndex, KeyLanes, LaneKind, NoStrings};
use columine_vm::logical_export::{SlotEncoding, export_slot};
use columine_vm::state_init::{
    DEFAULT_ACCEPTED_PROGRAM_MAGICS, ProgramInvalidCause, SizingRefusal,
    calculate_grown_state_size, calculate_state_size, init_state, validate_state,
};
use columine_vm::struct_map::{StructMap2Slot, StructMapSlot};
use columine_vm::undo_log::FLAT_UNDO_ENTRY_SIZE;
use columine_vm::vm::{CheckedStructMap, CheckedStructMap2, Vm, u32s_as_bytes};

const OK: u32 = ErrorCode::Ok as u32;
const PAIRS: u32 = 0;
const ROWS: u32 = 1;
const PAIR_FLAGS: u8 = SlotType::StructMap2 as u8;
const ROW_FLAGS: u8 = SlotType::StructMap as u8;
/// Columns: 0 = key1, 1 = key2, 2 = value, 3 = CSR offsets, 4 = CSR values,
/// 5 = one-key map key.
const COLUMNS: u8 = 6;

fn program(init: &[u8], reduce: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; 32];
    out.extend(PROGRAM_MAGIC.to_le_bytes());
    out.extend([2, 0, 0, 0]); // version 2, no callbacks, no flags
    out.extend(2u32.to_le_bytes());
    out.extend(u32::from(COLUMNS).to_le_bytes());
    out.extend(u32::try_from(init.len()).unwrap().to_le_bytes());
    out.extend(u32::try_from(reduce.len()).unwrap().to_le_bytes());
    out.extend_from_slice(init);
    out.extend_from_slice(reduce);
    out
}

/// Slot 0: a two-key map with one UINT32 field. Slot 1: a one-key map with a
/// UINT32 field and an ARRAY_U32 field, so it owns a CSR arena.
fn init_section(pair_flags: u8, row_flags: u8) -> Vec<u8> {
    let mut init = vec![
        Opcode::SlotStructMap2 as u8,
        PAIRS as u8,
        pair_flags,
        16,
        0,
        1,
        StructFieldType::UInt32 as u8,
    ];
    init.extend([
        Opcode::SlotStructMap as u8,
        ROWS as u8,
        row_flags,
        16,
        0,
        2,
        StructFieldType::UInt32 as u8,
        StructFieldType::ArrayU32 as u8,
    ]);
    init.push(Opcode::Halt as u8);
    init
}

/// Upsert both maps: the pair keyed (0, 1) with value 2, the one-key row
/// keyed 5 with value 2 and the CSR payload (3, 4).
fn upsert_reduce() -> Vec<u8> {
    vec![
        Opcode::BatchStructMap2UpsertLast as u8,
        PAIRS as u8,
        0,
        1,
        1,
        2,
        0,
        Opcode::BatchStructMapUpsertLast as u8,
        ROWS as u8,
        5,
        1,
        2,
        0,
        1,
        3,
        4,
        1,
    ]
}

fn remove_reduce() -> Vec<u8> {
    vec![
        Opcode::BatchStructMap2Remove as u8,
        PAIRS as u8,
        0,
        1,
        Opcode::BatchStructMapRemove as u8,
        ROWS as u8,
        5,
    ]
}

fn prefix_reduce(slot: u32) -> Vec<u8> {
    vec![Opcode::BatchStructMap2RemovePrefix as u8, slot as u8, 0]
}

fn init_image(program: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(program, DEFAULT_ACCEPTED_PROGRAM_MAGICS)
        .expect("valid program admission");
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, program, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("init");
    state
}

#[derive(Clone, Copy)]
struct Row {
    id: u32,
    key1: u32,
    key2: u32,
    value: u32,
    key: u32,
}

/// Twelve rows: three lane-1 groups of four, each row with a distinct
/// one-key map key and an arena payload of one to three cells.
fn sample() -> Vec<Row> {
    (0..12)
        .map(|id| Row {
            id,
            key1: 100 + id / 4,
            key2: 7 * id + 1,
            value: 10 * id + 3,
            key: 1000 + 37 * id,
        })
        .collect()
}

fn payload(id: u32) -> Vec<u32> {
    (0..id % 3 + 1).map(|cell| id * 100 + cell).collect()
}

/// One batch of `rows` in the given order, as the six columns.
struct Batch {
    key1: Vec<u32>,
    key2: Vec<u32>,
    value: Vec<u32>,
    offsets: Vec<u32>,
    values: Vec<u32>,
    key: Vec<u32>,
}

impl Batch {
    fn of(rows: &[Row]) -> Self {
        let mut batch = Self {
            key1: Vec::new(),
            key2: Vec::new(),
            value: Vec::new(),
            offsets: vec![0],
            values: Vec::new(),
            key: Vec::new(),
        };
        for row in rows {
            batch.key1.push(row.key1);
            batch.key2.push(row.key2);
            batch.value.push(row.value);
            batch.values.extend(payload(row.id));
            batch
                .offsets
                .push(u32::try_from(batch.values.len()).unwrap());
            batch.key.push(row.key);
        }
        batch
    }

    fn key1s(key1: &[u32]) -> Self {
        Self {
            key1: key1.to_vec(),
            key2: Vec::new(),
            value: Vec::new(),
            offsets: Vec::new(),
            values: Vec::new(),
            key: Vec::new(),
        }
    }

    fn columns(&self) -> [&[u8]; 6] {
        [
            u32s_as_bytes(&self.key1),
            u32s_as_bytes(&self.key2),
            u32s_as_bytes(&self.value),
            u32s_as_bytes(&self.offsets),
            u32s_as_bytes(&self.values),
            u32s_as_bytes(&self.key),
        ]
    }

    fn rows(&self) -> u32 {
        u32::try_from(self.key1.len()).unwrap()
    }

    fn run(&self, vm: &mut Vm, state: &mut [u8], program: &[u8]) -> u32 {
        vm.execute_batch(state, program, &self.columns(), self.rows())
    }

    /// [`Self::run`] on the paired undo/redo lane.
    fn run_delta(&self, vm: &mut Vm, state: &mut [u8], program: &[u8]) -> u32 {
        vm.execute_batch_delta(state, program, &self.columns(), self.rows())
    }
}

/// Every live pair as (key1, key2, value), sorted.
fn pair_rows(state: &[u8]) -> Vec<(u32, u32, u32)> {
    let pairs = CheckedStructMap2::bind(state, PAIRS).expect("pair slot");
    let value_at = StructMap2Slot::bind(state, PAIRS).field_offset(state, 0);
    let mut rows = Vec::new();
    let mut pos = pairs.iter_start();
    while pos < pairs.capacity() {
        let row = pairs.iter_row(pos).expect("live row");
        rows.push((
            pairs.iter_key1(pos).expect("live key1"),
            pairs.iter_key2(pos).expect("live key2"),
            bytes::read_u32(state, row + value_at),
        ));
        pos = pairs.iter_next(pos).expect("cursor");
    }
    rows.sort_unstable();
    rows
}

fn meta_base(slot: u32) -> usize {
    (STATE_HEADER_SIZE + slot * SLOT_META_SIZE) as usize
}

fn slot_bytes(state: &[u8], slot: u32) -> &[u8] {
    let offset = bytes::read_u32(state, STATE_HEADER_SIZE + slot * SLOT_META_SIZE) as usize;
    let end = if slot + 1 < 2 {
        bytes::read_u32(state, STATE_HEADER_SIZE + (slot + 1) * SLOT_META_SIZE) as usize
    } else {
        state.len()
    };
    &state[offset..end]
}

fn arena_used(state: &[u8]) -> u32 {
    bytes::read_u32(state, StructMapSlot::bind(state, ROWS).arena_header_off + 4)
}

fn populated(row_order: &[Row]) -> (Vm, Vec<u8>) {
    let upsert = program(&init_section(PAIR_FLAGS, ROW_FLAGS), &upsert_reduce());
    let mut state = init_image(&upsert);
    let mut vm = Vm::default();
    assert_eq!(OK, Batch::of(row_order).run(&mut vm, &mut state, &upsert));
    (vm, state)
}

#[test]
fn prefix_removal_removes_exactly_the_key1_rows_and_rollback_restores_the_image() {
    let (mut vm, mut state) = populated(&sample());
    let before = state.clone();
    let kept: Vec<_> = pair_rows(&state)
        .into_iter()
        .filter(|&(key1, _, _)| key1 != 101)
        .collect();
    assert_eq!(kept.len(), 8);

    vm.undo_enable(&state).expect("journal admits the image");
    let checkpoint = vm.undo_checkpoint();
    let remove = program(&init_section(PAIR_FLAGS, ROW_FLAGS), &prefix_reduce(PAIRS));
    assert_eq!(
        OK,
        Batch::key1s(&[101]).run_delta(&mut vm, &mut state, &remove)
    );
    let after = state.clone();

    assert_eq!(pair_rows(&state), kept);
    assert_eq!(StructMap2Slot::bind(&state, PAIRS).size(&state), 8);
    assert_ne!(
        state[meta_base(PAIRS) + SlotMetaOffset::CHANGE_FLAGS as usize] & ChangeFlag::REMOVED,
        0
    );
    assert_eq!(slot_bytes(&state, ROWS), slot_bytes(&before, ROWS));

    // The paired redo replays the removal onto the old image exactly.
    let end = vm.undo_checkpoint();
    vm.delta_export_segment(checkpoint, end)
        .expect("paired physical delta");
    let redo = vm.delta_export_redo().to_vec();
    let mut receiver = before.clone();
    Vm::default()
        .delta_apply_rollforward_segment(&mut receiver, &redo, FLAT_UNDO_ENTRY_SIZE)
        .expect("apply physical redo");
    assert_eq!(receiver, after);

    vm.undo_rollback(&mut state, checkpoint)
        .expect("rollback the removal");
    assert_eq!(state, before);
}

#[test]
fn prefix_removal_runs_per_element_and_refuses_what_names_no_row() {
    let (mut vm, state) = populated(&sample());
    let init = init_section(PAIR_FLAGS, ROW_FLAGS);

    // FOR_EACH over a type column (column 1 here): only the matching
    // element's key1 is removed.
    let mut body_program = vec![Opcode::ForEach as u8, 1, 1];
    body_program.extend(9u32.to_le_bytes());
    let body = prefix_reduce(PAIRS);
    body_program.extend(u16::try_from(body.len()).unwrap().to_le_bytes());
    body_program.extend(body);
    let element = program(&init, &body_program);
    let mut by_element = state.clone();
    let mut batch = Batch::key1s(&[100, 102]);
    batch.key2 = vec![9, 8];
    assert_eq!(OK, batch.run(&mut vm, &mut by_element, &element));
    let expected: Vec<_> = pair_rows(&state)
        .into_iter()
        .filter(|&(key1, _, _)| key1 != 100)
        .collect();
    assert_eq!(pair_rows(&by_element), expected);

    let top = program(&init, &prefix_reduce(PAIRS));
    // A key1 no row holds removes nothing.
    let mut absent = state.clone();
    assert_eq!(OK, Batch::key1s(&[999]).run(&mut vm, &mut absent, &top));
    assert_eq!(absent, state);
    // A sentinel key1 names no row: refused, as for the exact-pair remove.
    let mut sentinel = state.clone();
    assert_eq!(
        ErrorCode::InvalidKey as u32,
        Batch::key1s(&[EMPTY_KEY]).run(&mut vm, &mut sentinel, &top)
    );
    assert_eq!(sentinel, state);
    // The opcode names a two-key map; a one-key slot is a program error.
    let mut wrong_kind = state.clone();
    let on_rows = program(&init, &prefix_reduce(ROWS));
    assert_eq!(
        ErrorCode::InvalidProgram as u32,
        Batch::key1s(&[100]).run(&mut vm, &mut wrong_kind, &on_rows)
    );
    assert_eq!(wrong_kind, state);
}

#[test]
fn key_lane_flags_are_declared_by_struct_map_records_and_stored_in_byte_19() {
    let unary = SlotTypeFlags::from_byte(ROW_FLAGS | SlotTypeFlags::STRUCT_KEY1_INTERNED);
    let pair = SlotTypeFlags::from_byte(PAIR_FLAGS | SlotTypeFlags::STRUCT_KEY_LANES);
    assert_eq!(
        KeyLanes::declared(false, unary),
        Some(KeyLanes::One(LaneKind::Interned))
    );
    assert_eq!(
        KeyLanes::declared(true, pair),
        Some(KeyLanes::Two(LaneKind::Interned, LaneKind::Interned))
    );
    assert_eq!(
        KeyLanes::declared(
            false,
            SlotTypeFlags::from_byte(ROW_FLAGS | SlotTypeFlags::STRUCT_KEY2_INTERNED)
        ),
        None
    );
    assert_eq!(KeyLanes::stored(false, 0x80), None);
    assert_eq!(KeyLanes::stored(true, 0x60).map(KeyLanes::byte), Some(0x60));

    // The record's lane bits land in byte 19; TYPE_FLAGS keeps the rest.
    let interned = program(
        &init_section(pair.to_byte(), unary.to_byte()),
        &upsert_reduce(),
    );
    let mut state = init_image(&interned);
    for (slot, kind, lanes) in [(PAIRS, PAIR_FLAGS, 0x60), (ROWS, ROW_FLAGS, 0x20)] {
        let meta = meta_base(slot);
        assert_eq!(state[meta + SlotMetaOffset::TYPE_FLAGS as usize], kind);
        assert_eq!(
            state[meta + SlotMetaOffset::STRUCT_KEY_LANES as usize],
            lanes
        );
    }

    // HashIndex keys are u32 lanes whatever their kind: the same batch folds
    // to the same rows, and the resolver is never consulted.
    let (_, raw) = populated(&sample());
    let mut vm = Vm::default();
    assert_eq!(OK, Batch::of(&sample()).run(&mut vm, &mut state, &interned));
    assert_eq!(pair_rows(&state), pair_rows(&raw));
    assert!(
        CheckedStructMap::bind(&state, ROWS)
            .expect("row slot")
            .row(1000)
            .is_some()
    );

    // An image whose declared lanes differ from the program's is refused.
    let mut mismatched = raw.clone();
    assert_eq!(
        ErrorCode::InvalidProgram as u32,
        Batch::of(&sample()).run(&mut vm, &mut mismatched, &interned)
    );
    assert_eq!(mismatched, raw);

    // A lane-2 bit on a one-key record is a malformed program.
    let lane2_on_unary = program(
        &init_section(PAIR_FLAGS, ROW_FLAGS | SlotTypeFlags::STRUCT_KEY2_INTERNED),
        &upsert_reduce(),
    );
    assert_eq!(
        calculate_state_size(&lane2_on_unary, DEFAULT_ACCEPTED_PROGRAM_MAGICS),
        Err(SizingRefusal::Invalid(
            ProgramInvalidCause::MalformedProgram
        ))
    );

    // A stored lane byte no declaration writes is a malformed image.
    for (slot, byte) in [(ROWS, 0x40), (ROWS, 0x80), (PAIRS, 0x80)] {
        let mut corrupt = raw.clone();
        corrupt[meta_base(slot) + SlotMetaOffset::STRUCT_KEY_LANES as usize] = byte;
        assert_eq!(validate_state(&corrupt), Err(ErrorCode::InvalidState));
    }
}

/// Fold `first` in, remove `removed`, then upsert `removed` back in reverse:
/// the same rows end up live whatever the orders, through tombstones,
/// reused cells and abandoned arena payloads.
fn churned(first: &[Row], removed: &[Row]) -> Vec<u8> {
    let (mut vm, mut state) = populated(first);
    let init = init_section(PAIR_FLAGS, ROW_FLAGS);
    let remove = program(&init, &remove_reduce());
    assert_eq!(OK, Batch::of(removed).run(&mut vm, &mut state, &remove));
    let upsert = program(&init, &upsert_reduce());
    let back: Vec<Row> = removed.iter().rev().copied().collect();
    assert_eq!(OK, Batch::of(&back).run(&mut vm, &mut state, &upsert));
    state
}

#[test]
fn seal_rebuilds_permuted_histories_into_identical_images() {
    let rows = sample();
    let pick = |ids: &[usize]| -> Vec<Row> { ids.iter().map(|&id| rows[id]).collect() };
    let natural = rows.clone();
    let reversed: Vec<Row> = rows.iter().rev().copied().collect();
    let shuffled = pick(&[7, 2, 11, 0, 5, 9, 3, 10, 1, 8, 4, 6]);

    let images = [
        churned(&natural, &pick(&[1, 5, 9])),
        churned(&reversed, &pick(&[9, 1, 5])),
        churned(&shuffled, &pick(&[5, 9, 1])),
    ];
    // The histories left different bytes behind: arena payloads appended in
    // different orders, and the re-upserted rows' payloads appended twice.
    assert_ne!(images[0], images[1]);
    let live_payload: u32 = rows.iter().map(|row| (row.id % 3 + 1) * 4).sum();
    assert!(arena_used(&images[0]) > live_payload);

    let mut sealed = Vec::new();
    for mut image in images {
        let mut vm = Vm::default();
        vm.seal_state(&mut image, &NoStrings).expect("seal");
        sealed.push(image);
    }
    assert_eq!(sealed[0], sealed[1]);
    assert_eq!(sealed[0], sealed[2]);

    // The sealed image is a valid image holding every row, its arena
    // repacked to exactly the live payloads.
    let image = &sealed[0];
    validate_state(image).expect("sealed image is valid");
    let expected: Vec<_> = rows
        .iter()
        .map(|row| (row.key1, row.key2, row.value))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    assert_eq!(pair_rows(image), expected);
    assert_eq!(arena_used(image), live_payload);
    let map = StructMapSlot::bind(image, ROWS);
    for row in &rows {
        let pos = map.find(image, row.key).expect("sealed row");
        let stored = map
            .arena_field(image, pos, 1)
            .expect("arena field")
            .expect("present payload");
        let cells: Vec<u32> = stored
            .as_chunks::<4>()
            .0
            .iter()
            .map(|cell| u32::from_le_bytes(*cell))
            .collect();
        assert_eq!(cells, payload(row.id));
    }

    // Sealing is idempotent, and it never runs inside a speculative window.
    let mut again = image.clone();
    let mut vm = Vm::default();
    vm.seal_state(&mut again, &NoStrings).expect("reseal");
    assert_eq!(&again, image);
    vm.undo_enable(&again).expect("open a window");
    assert_eq!(
        vm.seal_state(&mut again, &NoStrings),
        Err(ErrorCode::SpeculationActive)
    );
    assert_eq!(&again, image);
}

#[test]
fn a_state_header_of_another_family_is_refused() {
    let (_, state) = populated(&sample());
    assert_eq!(
        bytes::read_u32(&state, StateHeaderOffset::MAGIC),
        HashIndex::STATE_MAGIC
    );
    assert_eq!(
        state[StateHeaderOffset::FORMAT_VERSION as usize],
        HashIndex::FORMAT_VERSION
    );

    let mut foreign = state.clone();
    bytes::write_u32(
        &mut foreign,
        StateHeaderOffset::MAGIC,
        !HashIndex::STATE_MAGIC,
    );
    foreign[StateHeaderOffset::FORMAT_VERSION as usize] = 1;
    let before = foreign.clone();

    assert_eq!(validate_state(&foreign), Err(ErrorCode::InvalidState));
    assert_eq!(
        calculate_grown_state_size(&foreign, PAIRS),
        Err(ErrorCode::InvalidState)
    );
    assert!(matches!(
        CheckedStructMap2::bind(&foreign, PAIRS),
        Err(ErrorCode::InvalidState)
    ));
    assert!(export_slot(&foreign, PAIRS, &SlotEncoding::default(), &NoText).is_err());
    let mut vm = Vm::default();
    assert_eq!(vm.undo_enable(&foreign), Err(ErrorCode::InvalidState));
    let upsert = program(&init_section(PAIR_FLAGS, ROW_FLAGS), &upsert_reduce());
    assert_eq!(
        ErrorCode::InvalidState as u32,
        Batch::of(&sample()).run(&mut vm, &mut foreign, &upsert)
    );
    assert_eq!(
        vm.seal_state(&mut foreign, &NoStrings),
        Err(ErrorCode::InvalidState)
    );
    assert_eq!(foreign, before);
}

/// An interner with no text: export of raw lanes never asks it.
struct NoText;

impl columine_vm::logical_export::InternLookup for NoText {
    fn text(&self, _id: u32) -> Option<&str> {
        None
    }
}

#[test]
fn two_key_opcodes_bind_their_slot_checked_and_refuse_a_one_key_slot() {
    let (mut vm, state) = populated(&sample());
    let init = init_section(PAIR_FLAGS, ROW_FLAGS);
    let mut tuple = vec![
        Opcode::BatchStructMap2UpsertMaxI64x2 as u8,
        ROWS as u8,
        0,
        1,
        0,
    ];
    tuple.extend([2, 0, 2, 1]);
    let upsert = vec![
        Opcode::BatchStructMap2UpsertLast as u8,
        ROWS as u8,
        0,
        1,
        1,
        2,
        0,
    ];
    let mut element = vec![Opcode::ForEach as u8, 1, 1];
    element.extend(7u32.to_le_bytes());
    element.extend(u16::try_from(upsert.len()).unwrap().to_le_bytes());
    element.extend(&upsert);
    let remove = vec![Opcode::BatchStructMap2Remove as u8, ROWS as u8, 0, 1];
    for reduce in [upsert, tuple, remove, element] {
        let mut image = state.clone();
        let mut batch = Batch::of(&sample()[..1]);
        batch.key2 = vec![7];
        assert_eq!(
            ErrorCode::InvalidProgram as u32,
            batch.run(&mut vm, &mut image, &program(&init, &reduce)),
            "reduce {reduce:?}"
        );
        assert_eq!(image, state);
    }
}

#[test]
fn key_lane_bits_belong_to_struct_map_records_only() {
    for lane in [
        SlotTypeFlags::STRUCT_KEY1_INTERNED,
        SlotTypeFlags::STRUCT_KEY2_INTERNED,
    ] {
        let mut init = vec![
            Opcode::SlotOrderedList as u8,
            PAIRS as u8,
            SlotType::OrderedList as u8 | lane,
            8,
            0,
            StructFieldType::UInt32 as u8,
        ];
        init.extend(&init_section(PAIR_FLAGS, ROW_FLAGS)[7..]);
        assert_eq!(
            calculate_state_size(&program(&init, &[]), DEFAULT_ACCEPTED_PROGRAM_MAGICS),
            Err(SizingRefusal::Invalid(
                ProgramInvalidCause::MalformedProgram
            )),
            "lane bit {lane:#x} on an ordered list"
        );
    }
}
