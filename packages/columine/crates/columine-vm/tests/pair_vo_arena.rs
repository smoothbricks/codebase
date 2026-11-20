//! Native oracles for value-object fields of a two-key struct map
//! (`SLOT_STRUCT_MAP2`), written by the array operands of
//! `BATCH_STRUCT_MAP2_UPSERT_LAST_ARRAYS` (`0xa0`): columine 04 §9 (a
//! text lane's frame is a leading value-object field of the row, written by
//! the upsert's array operands) and 04 §2 and §6 (the arena is
//! bump-allocated; growth and seal compact it in ordinal order).
//!
//! Every scenario drives the real VM: `init_state`, `execute_batch_delta`,
//! `Vm::grow_state` for the room the refused batch names, `Vm::seal_state`,
//! the undo journal and its exported delta segments, and logical export and
//! import. Stored bytes are read back only through the checked
//! `StructRows::arena_field` of the pair a key index names.
//!
//! The slot is `[VO frame, UINT32 count, VO payload]`: field 0 is the
//! leading frame a text lane's key names, field 2 a second value object, so
//! one row carries two array operands.

use std::collections::BTreeMap;

use columine_types::PROGRAM_MAGIC;
use columine_types::types::{
    EMPTY_KEY, ErrorCode, SlotMetaOffset, SlotType, StructFieldType, TOMBSTONE,
};
use columine_vm::bytes;
use columine_vm::change_feed::{Change, Source};
use columine_vm::key_index::Key;
use columine_vm::key_index::NoStrings;
use columine_vm::logical_export::{
    InternLookup, Interner, LogicalValue, SlotEncoding, export_slot, import_slot,
};
use columine_vm::meta::slot_meta_base;
use columine_vm::state_init::{
    ARENA_HEADER_SIZE, DEFAULT_ACCEPTED_PROGRAM_MAGICS, GrowthRoom, calculate_grown_state_size,
    calculate_state_size, init_state, validate_state,
};
use columine_vm::struct_map::StructMap2Slot;
use columine_vm::vm::{Vm, u32s_as_bytes};
use columine_vm::watch_log::{NetAnswer, NetChanges};

const HALT: u8 = 0x00;
const FOR_EACH: u8 = 0xe0;
const SLOT_STRUCT_MAP2: u8 = 0x1b;
const STRUCT_MAP2_FLAGS: u8 = 10;
const UPSERT2_LAST: u8 = 0x83;
const UPSERT2_ARRAYS: u8 = 0xa0;
const REMOVE2: u8 = 0x86;
const REMOVE2_PREFIX: u8 = 0x8e;

const UINT32: u8 = 0;
const VO: u8 = 10;

const SLOT: u32 = 0;
const FRAME: u8 = 0;
const COUNT: u8 = 1;
const PAYLOAD: u8 = 2;
const TYPE_A: u32 = 1001;

fn len_u32(len: usize) -> u32 {
    u32::try_from(len).expect("length fits u32")
}

fn at(offset: u32) -> usize {
    usize::try_from(offset).expect("offset fits usize")
}

/// One slot, `num_inputs` columns, `init` then `HALT`, then `reduce`.
fn program(num_inputs: u32, init: &[u8], reduce: &[u8]) -> Vec<u8> {
    let mut prog = vec![0u8; 32];
    prog.extend(PROGRAM_MAGIC.to_le_bytes());
    prog.extend([2, 0, 0, 0]);
    prog.extend(1u32.to_le_bytes());
    prog.extend(num_inputs.to_le_bytes());
    prog.extend(len_u32(init.len() + 1).to_le_bytes());
    prog.extend(len_u32(reduce.len()).to_le_bytes());
    prog.extend_from_slice(init);
    prog.push(HALT);
    prog.extend_from_slice(reduce);
    prog
}

/// `SLOT_STRUCT_MAP2` slot 0, capacity hint 4 (64 arena bytes per row of its capacity).
fn pair_init(kinds: &[u8]) -> Vec<u8> {
    let mut init = vec![
        SLOT_STRUCT_MAP2,
        0,
        STRUCT_MAP2_FLAGS,
        4,
        0,
        u8::try_from(kinds.len()).expect("field count fits u8"),
    ];
    init.extend_from_slice(kinds);
    init
}

fn slot_init() -> Vec<u8> {
    pair_init(&[VO, UINT32, VO])
}

/// `0xa0` over columns starting at `base`: key1, key2, count, then the
/// frame's and the payload's CSR pairs.
fn upsert(base: u8) -> Vec<u8> {
    vec![
        UPSERT2_ARRAYS,
        0,
        base,
        base + 1,
        1,
        base + 2,
        COUNT,
        2,
        base + 3,
        base + 4,
        FRAME,
        base + 5,
        base + 6,
        PAYLOAD,
    ]
}

fn upsert_program() -> Vec<u8> {
    program(7, &slot_init(), &upsert(0))
}

/// `0x8e` keyed by column 0, over the same slot declaration.
fn prefix_erase_program() -> Vec<u8> {
    program(1, &slot_init(), &[REMOVE2_PREFIX, 0, 0])
}

/// `0x86` keyed by columns 0 and 1, over the same slot declaration.
fn exact_erase_program() -> Vec<u8> {
    program(2, &slot_init(), &[REMOVE2, 0, 0, 1])
}

fn init(prog: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS)
        .expect("a two-key map with value-object fields is admitted");
    let mut state = vec![0u8; at(size)];
    init_state(&mut state, prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("init_state");
    state
}

fn named(raw: u32) -> ErrorCode {
    ErrorCode::from_u32(raw).unwrap_or_else(|| panic!("VM returned unnamed status {raw}"))
}

type Pair = (u32, u32);

/// One row: its pair, its count, its frame and its payload.
#[derive(Clone)]
struct Row {
    pair: Pair,
    count: u32,
    frame: Vec<u8>,
    payload: Vec<u8>,
}

fn row(key1: u32, key2: u32, frame: &[u8], payload: &[u8]) -> Row {
    Row {
        pair: (key1, key2),
        count: key1.wrapping_mul(1000).wrapping_add(key2),
        frame: frame.to_vec(),
        payload: payload.to_vec(),
    }
}

/// The text-like frame of a pair: distinct bytes per pair, and a marker a
/// test can search an image for.
fn frame_of(key1: u32, key2: u32) -> Vec<u8> {
    format!("frame:{key1}/{key2}").into_bytes()
}

fn payload_of(key1: u32, key2: u32) -> Vec<u8> {
    let len = usize::try_from((key1 * 7 + key2 * 3) % 23).expect("small");
    (0..len)
        .map(|index| {
            u8::try_from((index + usize::try_from(key2).expect("small")) % 256).expect("byte")
        })
        .collect()
}

/// A batch's seven columns. Each CSR values column opens with a two-byte
/// prefix, so offsets are absolute and never assumed to start at zero.
struct Batch {
    key1: Vec<u32>,
    key2: Vec<u32>,
    count: Vec<u32>,
    frame_offsets: Vec<u32>,
    frames: Vec<u8>,
    payload_offsets: Vec<u32>,
    payloads: Vec<u8>,
}

impl Batch {
    fn of(rows: &[Row]) -> Self {
        let mut batch = Self {
            key1: Vec::new(),
            key2: Vec::new(),
            count: Vec::new(),
            frame_offsets: vec![2],
            frames: vec![0xee, 0xee],
            payload_offsets: vec![2],
            payloads: vec![0xdd, 0xdd],
        };
        for row in rows {
            batch.key1.push(row.pair.0);
            batch.key2.push(row.pair.1);
            batch.count.push(row.count);
            batch.frames.extend_from_slice(&row.frame);
            batch.frame_offsets.push(len_u32(batch.frames.len()));
            batch.payloads.extend_from_slice(&row.payload);
            batch.payload_offsets.push(len_u32(batch.payloads.len()));
        }
        batch
    }

    fn len(&self) -> u32 {
        len_u32(self.key1.len())
    }

    fn cols(&self) -> Vec<&[u8]> {
        vec![
            u32s_as_bytes(&self.key1),
            u32s_as_bytes(&self.key2),
            u32s_as_bytes(&self.count),
            u32s_as_bytes(&self.frame_offsets),
            &self.frames,
            u32s_as_bytes(&self.payload_offsets),
            &self.payloads,
        ]
    }
}

fn exec(vm: &mut Vm, state: &mut [u8], prog: &[u8], cols: &[&[u8]], len: u32) -> ErrorCode {
    named(vm.execute_batch_delta(state, prog, cols, len))
}

fn run(vm: &mut Vm, state: &mut [u8], prog: &[u8], batch: &Batch) -> ErrorCode {
    exec(vm, state, prog, &batch.cols(), batch.len())
}

/// Run `rows` as one batch, growing the slot by the room each refusal names
/// until the batch lands. A refusal writes nothing, so the batch reruns on
/// the grown image whole. Answers the image and the rooms growth gave.
fn run_growing(mut state: Vec<u8>, prog: &[u8], rows: &[Row]) -> (Vec<u8>, Vec<GrowthRoom>) {
    let batch = Batch::of(rows);
    let mut rooms = Vec::new();
    for _ in 0..16 {
        let mut vm = Vm::default();
        let before = state.clone();
        match run(&mut vm, &mut state, prog, &batch) {
            ErrorCode::Ok => return (state, rooms),
            ErrorCode::NeedsGrowth => {
                assert_eq!(before, state, "a batch refused for growth writes nothing");
                assert_eq!(Some(SLOT), vm.needs_growth_slot());
                let room = vm.needs_growth_room().expect("NEEDS_GROWTH names its room");
                rooms.push(room);
                state = grown(&state, room);
            }
            other => panic!("batch refused: {other:?}"),
        }
    }
    panic!("the batch never landed after 16 growths: {rooms:?}");
}

fn run_ok(state: &mut [u8], prog: &[u8], rows: &[Row]) {
    let mut vm = Vm::default();
    assert_eq!(ErrorCode::Ok, run(&mut vm, state, prog, &Batch::of(rows)));
}

fn grown(state: &[u8], room: GrowthRoom) -> Vec<u8> {
    let size = calculate_grown_state_size(state, SLOT, room).expect("grown state size");
    let mut next = vec![0u8; at(size)];
    Vm::default()
        .grow_state(state, &mut next, SLOT, room)
        .expect("Vm::grow_state");
    validate_state(&next).expect("a grown image validates");
    next
}

fn seal(state: &mut [u8]) {
    Vm::default()
        .seal_state(state, &NoStrings)
        .expect("Vm::seal_state");
    validate_state(state).expect("a sealed image validates");
}

fn erase_prefix(state: &mut [u8], key1s: &[u32]) {
    let mut vm = Vm::default();
    let cols = [u32s_as_bytes(key1s)];
    assert_eq!(
        ErrorCode::Ok,
        exec(
            &mut vm,
            state,
            &prefix_erase_program(),
            &cols,
            len_u32(key1s.len())
        )
    );
}

fn erase_pair(state: &mut [u8], key1: u32, key2: u32) {
    let mut vm = Vm::default();
    let (k1, k2) = ([key1], [key2]);
    let cols = [u32s_as_bytes(&k1), u32s_as_bytes(&k2)];
    assert_eq!(
        ErrorCode::Ok,
        exec(&mut vm, state, &exact_erase_program(), &cols, 1)
    );
}

fn map(state: &[u8]) -> StructMap2Slot {
    StructMap2Slot::bind(state, SLOT)
}

/// The field a pair's row holds, `None` when the pair names no row.
fn arena_field(state: &[u8], pair: Pair, field: u8) -> Option<Option<Vec<u8>>> {
    let map = map(state);
    let pos = map.find(state, pair.0, pair.1)?;
    Some(
        map.rows()
            .arena_field(state, pos, field)
            .unwrap_or_else(|error| panic!("{pair:?} field {field}: {error:?}"))
            .map(<[u8]>::to_vec),
    )
}

fn count(state: &[u8], pair: Pair) -> u32 {
    let map = map(state);
    let pos = map.find(state, pair.0, pair.1).expect("pair present");
    bytes::read_u32(state, map.row_off(pos) + map.field_offset(state, COUNT))
}

type Model = BTreeMap<Pair, Row>;

fn model(rows: &[Row]) -> Model {
    let mut model = Model::new();
    for row in rows {
        model.insert(row.pair, row.clone());
    }
    model
}

/// The slot holds exactly the model's pairs, each with its own frame, count
/// and payload.
fn assert_holds(state: &[u8], model: &Model) {
    let map = map(state);
    assert_eq!(len_u32(model.len()), map.size(state), "row count");
    let live: Vec<Pair> = map
        .iter(state)
        .map(|(key1, key2, _)| (key1, key2))
        .collect();
    for pair in &live {
        assert!(
            model.contains_key(pair),
            "{pair:?} holds a row the model does not"
        );
    }
    for (pair, row) in model {
        assert_eq!(
            Some(Some(row.frame.clone())),
            arena_field(state, *pair, FRAME),
            "{pair:?} frame"
        );
        assert_eq!(
            Some(Some(row.payload.clone())),
            arena_field(state, *pair, PAYLOAD),
            "{pair:?} payload"
        );
        assert_eq!(row.count, count(state, *pair), "{pair:?} count");
    }
}

fn arena(state: &[u8]) -> (u32, u32, &[u8]) {
    let header = map(state).arena_header_off;
    assert_ne!(0, header, "a value-object field gives the slot an arena");
    let capacity = bytes::read_u32(state, header);
    let used = bytes::read_u32(state, header + 4);
    let data = header + ARENA_HEADER_SIZE;
    (capacity, used, &state[at(data)..at(data + capacity)])
}

/// The slot's own bytes, descriptor through arena end: what a seal makes a
/// function of its pairs. The image header and slot metadata also count the
/// batches a history ran.
fn slot_bytes(state: &[u8]) -> &[u8] {
    let (capacity, _, _) = arena(state);
    let end = map(state).arena_header_off + ARENA_HEADER_SIZE + capacity;
    &state[at(map(state).slot_offset)..at(end)]
}
fn live_bytes(model: &Model) -> u32 {
    model
        .values()
        .map(|row| len_u32(row.frame.len() + row.payload.len()))
        .sum()
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

// =============================================================================
// Declaration and keys
// =============================================================================

#[test]
fn a_two_key_map_declares_value_object_fields_and_starts_with_an_arena() {
    let state = init(&upsert_program());
    let (capacity, used, data) = arena(&state);
    assert_eq!(
        64 * map(&state).capacity,
        capacity,
        "64 arena bytes per row of the slot"
    );
    assert_eq!(0, used);
    assert!(data.iter().all(|byte| *byte == 0));
    assert_eq!(0, map(&state).size(&state));
}

#[test]
fn one_key1_holds_a_frame_per_key2_and_the_last_write_of_a_pair_wins() {
    let prog = upsert_program();
    let mut state = init(&prog);
    let rows = [
        row(7, 0, b"op7/elem0", &[1, 2, 3]),
        row(7, 1, b"op7/elem1", &[]),
        row(8, 0, b"op8/elem0", &[0, 255]),
        row(0, 7, b"op0/elem7", &[9]),
        row(7, 0, b"op7/elem0/rewritten", &[4, 5]),
    ];
    run_ok(&mut state, &prog, &rows);
    assert_holds(&state, &model(&rows));
    assert_eq!(
        None,
        arena_field(&state, (1, 7), FRAME),
        "an unwritten pair names no row"
    );
}

#[test]
fn many_pairs_under_shared_key1s_and_mirrored_key2s_keep_every_frame_through_growth() {
    let prog = upsert_program();
    let mut rows = Vec::new();
    for key1 in 1..=6u32 {
        for key2 in 0..8u32 {
            rows.push(row(
                key1,
                key2,
                &frame_of(key1, key2),
                &payload_of(key1, key2),
            ));
        }
    }
    // (k, j) and (j, k) are distinct pairs: neither lane may stand for the other.
    for key2 in 1..=6u32 {
        rows.push(row(9, key2, &frame_of(9, key2), &payload_of(9, key2)));
        rows.push(row(key2, 9, &frame_of(key2, 9), &payload_of(key2, 9)));
    }
    let (state, rooms) = run_growing(init(&prog), &prog, &rows);
    assert!(
        rooms.contains(&GrowthRoom::Rows),
        "the rows ran out: {rooms:?}"
    );
    assert_holds(&state, &model(&rows));

    // A second batch rewrites one frame per key1; every other pair keeps its own.
    let mut expected = model(&rows);
    let rewrites: Vec<Row> = (1..=6u32)
        .map(|key1| row(key1, key1, &[0xab; 70], &frame_of(key1, 100)))
        .collect();
    for rewrite in &rewrites {
        expected.insert(rewrite.pair, rewrite.clone());
    }
    let (state, _) = run_growing(state, &prog, &rewrites);
    assert_holds(&state, &expected);
}

#[test]
fn frames_past_the_arena_grow_its_room_and_compaction_keeps_every_frame() {
    let prog = upsert_program();
    let mut state = init(&prog);
    let first = [
        row(1, 0, &[0x11; 500], &[0x12; 300]),
        row(1, 1, &[0x13; 100], &[]),
    ];
    run_ok(&mut state, &prog, &first);
    let mut expected = model(&first);

    // 900 + 300 bytes: past the 1024-byte arena, refused whole before any write.
    let over = [row(1, 0, &[0x21; 150], &[0x22; 150])];
    let before = state.clone();
    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("undo window opens");
    let checkpoint = vm.undo_checkpoint();
    assert_eq!(
        ErrorCode::NeedsGrowth,
        run(&mut vm, &mut state, &prog, &Batch::of(&over))
    );
    assert_eq!(
        Some(GrowthRoom::Arena),
        vm.needs_growth_room(),
        "the arena ran out, not the rows"
    );
    assert_eq!(before, state, "the refused batch writes nothing");
    assert_eq!(checkpoint, vm.undo_checkpoint(), "and journals nothing");

    let (state, rooms) = run_growing(state, &prog, &over);
    assert_eq!(vec![GrowthRoom::Arena], rooms);
    for row in over {
        expected.insert(row.pair, row);
    }
    assert_holds(&state, &expected);

    let (_, used, _) = arena(&state);
    assert_eq!(
        900 + 300,
        used,
        "growth compacted the 900 live bytes, then the rewrite appended its 300"
    );
    let mut sealed = state.clone();
    seal(&mut sealed);
    assert_holds(&sealed, &expected);
    let (_, used, data) = arena(&sealed);
    assert_eq!(
        live_bytes(&expected),
        used,
        "seal drops the superseded frame"
    );
    assert!(
        !contains(data, &[0x11; 200]),
        "the superseded frame's bytes are gone"
    );
}

#[test]
fn an_element_body_writes_one_pair_per_matching_event() {
    let mut body = vec![FOR_EACH, 0, 1];
    body.extend(TYPE_A.to_le_bytes());
    let inner = upsert(1);
    body.extend(u16::try_from(inner.len()).expect("fits").to_le_bytes());
    body.extend_from_slice(&inner);
    let prog = program(8, &slot_init(), &body);
    let mut state = init(&prog);
    let rows = [
        row(3, 0, b"kept", &[1]),
        row(3, 1, b"skipped", &[2]),
        row(3, 2, b"kept too", &[]),
    ];
    let batch = Batch::of(&rows);
    let types = [TYPE_A, 7, TYPE_A];
    let mut cols = vec![u32s_as_bytes(&types)];
    cols.extend(batch.cols());
    let mut vm = Vm::default();
    assert_eq!(ErrorCode::Ok, exec(&mut vm, &mut state, &prog, &cols, 3));
    let mut expected = model(&rows);
    expected.remove(&(3, 1));
    assert_holds(&state, &expected);
}

// =============================================================================
// Erase
// =============================================================================

#[test]
fn a_prefix_erase_removes_every_frame_of_its_key1_and_seal_drops_their_bytes() {
    let prog = upsert_program();
    let mut state = init(&prog);
    let rows: Vec<Row> = [(7, 0), (7, 1), (7, 2), (8, 0), (0, 7), (8, 7)]
        .into_iter()
        .map(|(key1, key2)| row(key1, key2, &frame_of(key1, key2), &payload_of(key1, key2)))
        .collect();
    run_ok(&mut state, &prog, &rows);
    erase_prefix(&mut state, &[7]);
    let mut expected = model(&rows);
    expected.retain(|pair, _| pair.0 != 7);
    assert_holds(&state, &expected);
    for key2 in 0..3 {
        assert_eq!(
            None,
            arena_field(&state, (7, key2), FRAME),
            "(7, {key2}) is erased"
        );
    }

    seal(&mut state);
    assert_holds(&state, &expected);
    let (_, used, data) = arena(&state);
    assert_eq!(
        live_bytes(&expected),
        used,
        "the sealed arena holds the live frames alone"
    );
    for key2 in 0..3 {
        assert!(
            !contains(&state, &frame_of(7, key2)),
            "(7, {key2})'s frame left the image"
        );
    }
    assert!(
        data[at(used)..].iter().all(|byte| *byte == 0),
        "every unused arena byte is zero"
    );

    // A pair under the erased key1 written again is a fresh row.
    let again = [row(7, 1, b"op7 again", &[3])];
    run_ok(&mut state, &prog, &again);
    expected.insert((7, 1), again[0].clone());
    assert_holds(&state, &expected);
}

#[test]
fn an_exact_erase_removes_one_pair_and_growth_drops_its_bytes() {
    let prog = upsert_program();
    let mut state = init(&prog);
    let rows: Vec<Row> = [(5, 0), (5, 1), (0, 5)]
        .into_iter()
        .map(|(key1, key2)| row(key1, key2, &frame_of(key1, key2), &payload_of(key1, key2)))
        .collect();
    run_ok(&mut state, &prog, &rows);
    erase_pair(&mut state, 5, 0);
    let mut expected = model(&rows);
    expected.remove(&(5, 0));
    assert_holds(&state, &expected);

    let state = grown(&state, GrowthRoom::Rows);
    assert_holds(&state, &expected);
    assert!(
        !contains(&state, &frame_of(5, 0)),
        "the erased frame left the grown image"
    );
}

// =============================================================================
// Seal, restore and the logical form
// =============================================================================

#[test]
fn histories_holding_the_same_pairs_seal_to_the_same_image() {
    let prog = upsert_program();
    let final_rows: Vec<Row> = [(2, 0), (1, 1), (1, 0), (3, 4)]
        .into_iter()
        .map(|(key1, key2)| row(key1, key2, &frame_of(key1, key2), &payload_of(key1, key2)))
        .collect();

    let mut direct = init(&prog);
    run_ok(&mut direct, &prog, &final_rows);

    // Another order, a superseded frame, and a pair inserted then erased.
    let mut winding = init(&prog);
    run_ok(
        &mut winding,
        &prog,
        &[
            row(3, 4, b"superseded", &[0xff; 40]),
            row(9, 9, b"erased", &[1]),
        ],
    );
    run_ok(
        &mut winding,
        &prog,
        &[
            final_rows[1].clone(),
            final_rows[2].clone(),
            final_rows[0].clone(),
        ],
    );
    erase_pair(&mut winding, 9, 9);
    run_ok(&mut winding, &prog, &[final_rows[3].clone()]);
    assert_ne!(
        slot_bytes(&direct),
        slot_bytes(&winding),
        "the live slots differ by history"
    );

    seal(&mut direct);
    seal(&mut winding);
    assert_eq!(
        slot_bytes(&direct),
        slot_bytes(&winding),
        "a sealed slot is a function of its pairs"
    );
    assert_eq!(map(&direct).size(&direct), map(&winding).size(&winding));
    assert_holds(&direct, &model(&final_rows));
    assert!(!contains(&winding, b"superseded"));
    assert!(!contains(&winding, b"erased"));
}

/// Export `[checkpoint, end)` and prove both lanes replay byte-exact on a
/// fresh VM, then the VM's own rollback restores the image.
fn assert_delta_roundtrip(vm: &mut Vm, before: &[u8], after: &[u8], checkpoint: u32) {
    let end = vm.undo_checkpoint();
    assert!(end > checkpoint, "pair VO writes journal");
    assert!(!vm.undo_status().overflow, "the journal stays exact");
    assert_eq!(
        Ok(end - checkpoint),
        vm.delta_export_segment(checkpoint, end)
    );
    let undo = vm.delta_export_undo().to_vec();
    let redo = vm.delta_export_redo().to_vec();
    let entry_size = Vm::delta_export_entry_size();

    let mut replay = Vm::default();
    let mut rolled_back = after.to_vec();
    assert_eq!(
        Ok(len_u32(after.len())),
        replay.delta_apply_rollback_segment(&mut rolled_back, &undo, entry_size)
    );
    assert_eq!(rolled_back, before, "exported undo restores the image");
    let mut rolled_forward = before.to_vec();
    assert_eq!(
        Ok(len_u32(before.len())),
        replay.delta_apply_rollforward_segment(&mut rolled_forward, &redo, entry_size)
    );
    assert_eq!(rolled_forward, after, "exported redo reproduces the image");

    let mut production = after.to_vec();
    assert_eq!(
        Ok(len_u32(after.len())),
        vm.undo_rollback(&mut production, checkpoint)
    );
    assert_eq!(production, before, "undo rollback restores the image");
}

#[test]
fn undo_rollback_and_exported_delta_replay_restore_frames_byte_exact() {
    let prog = upsert_program();
    let mut state = init(&prog);
    let seed = [
        row(4, 0, b"seed 4/0", &[1, 0, 255]),
        row(4, 1, b"seed 4/1", &[7; 9]),
    ];
    run_ok(&mut state, &prog, &seed);

    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("undo window opens");
    let before = state.clone();
    let checkpoint = vm.undo_checkpoint();
    let rows = [
        row(4, 0, b"rewritten 4/0", &[]),
        row(4, 2, b"new 4/2", &[2, 2]),
        row(5, 0, b"new 5/0", &[3]),
    ];
    assert_eq!(
        ErrorCode::Ok,
        run(&mut vm, &mut state, &prog, &Batch::of(&rows))
    );
    let mut expected = model(&seed);
    for row in &rows {
        expected.insert(row.pair, row.clone());
    }
    assert_holds(&state, &expected);
    let after = state.clone();
    assert_delta_roundtrip(&mut vm, &before, &after, checkpoint);
}

#[test]
fn a_refused_later_row_rolls_back_every_frame_the_batch_wrote() {
    let prog = upsert_program();
    let mut state = init(&prog);
    run_ok(&mut state, &prog, &[row(6, 0, b"kept", &[1])]);
    let before = state.clone();
    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("undo window opens");
    let checkpoint = vm.undo_checkpoint();
    // The second row's key1 is the sentinel: a two-key upsert refuses it.
    let rows = [
        row(6, 0, b"rewritten", &[2]),
        row(EMPTY_KEY, 0, b"refused", &[]),
    ];
    assert_eq!(
        ErrorCode::InvalidKey,
        run(&mut vm, &mut state, &prog, &Batch::of(&rows))
    );
    assert_eq!(
        Ok(len_u32(state.len())),
        vm.undo_rollback(&mut state, checkpoint)
    );
    assert_eq!(before, state, "rollback restores the image");
}

struct NoText;

impl InternLookup for NoText {
    fn text(&self, _id: u32) -> Option<&str> {
        None
    }
}

impl Interner for NoText {
    fn intern(&mut self, _text: columine_types::InternedStr<'_>) -> Result<u32, ErrorCode> {
        Err(ErrorCode::InvalidState)
    }
}

#[test]
fn the_logical_form_carries_each_pairs_frames_and_imports_to_the_same_reads() {
    let prog = upsert_program();
    let rows: Vec<Row> = [(1, 0), (1, 1), (2, 0), (0, 1)]
        .into_iter()
        .map(|(key1, key2)| row(key1, key2, &frame_of(key1, key2), &payload_of(key1, key2)))
        .collect();
    let mut state = init(&prog);
    run_ok(&mut state, &prog, &rows);
    let logical = export_slot(&state, SLOT, &SlotEncoding::RAW, &NoText).expect("export");
    assert_eq!(SlotType::StructMap2, logical.kind);
    assert_eq!(
        vec![
            StructFieldType::Vo,
            StructFieldType::UInt32,
            StructFieldType::Vo
        ],
        logical.field_types
    );
    let exported: Vec<(Pair, Vec<Option<LogicalValue>>)> = logical
        .rows
        .iter()
        .map(|exported| {
            let LogicalValue::U32(key1) = exported.key else {
                panic!("raw key1");
            };
            let Some(LogicalValue::U32(key2)) = exported.key2 else {
                panic!("raw key2");
            };
            ((key1, key2), exported.fields.clone())
        })
        .collect();
    let expected: Vec<(Pair, Vec<Option<LogicalValue>>)> = model(&rows)
        .into_values()
        .map(|row| {
            (
                row.pair,
                vec![
                    Some(LogicalValue::Bytes(row.frame)),
                    Some(LogicalValue::U32(row.count)),
                    Some(LogicalValue::Bytes(row.payload)),
                ],
            )
        })
        .collect();
    assert_eq!(
        expected, exported,
        "rows in ascending pair order, frames as bytes"
    );

    let mut image = init(&prog);
    import_slot(&mut image, &logical, &SlotEncoding::RAW, &mut NoText).expect("import");
    validate_state(&image).expect("an imported image validates");
    assert_holds(&image, &model(&rows));
    let back = export_slot(&image, SLOT, &SlotEncoding::RAW, &NoText).expect("export again");
    assert_eq!(logical, back, "export of import is the identity");
}

// =============================================================================
// Change feed and watch log
// =============================================================================

/// The feed entries of slot 0 since the pass began, oldest first.
fn feed_entries(vm: &Vm) -> Vec<(Key, Change)> {
    vm.feed()
        .entries()
        .iter()
        .filter(|entry| entry.source() == Source::Slot(0))
        .map(|entry| (entry.key(), entry.change()))
        .collect()
}

/// Track slot 0 in the change feed, watch it, and begin a pass.
fn armed(state: &mut [u8]) -> Vm {
    let mut vm = Vm::default();
    // Change-plane bit `slot + 1` names slot 0.
    vm.feed_bind(&[0b10, 0], 0, 64).expect("feed_bind");
    state[at(slot_meta_base(SLOT) + SlotMetaOffset::CHANGE_FLAGS)] = 0;
    vm.feed_begin_pass(state);
    vm.watch(state, SLOT, 64, 4096).expect("watch");
    vm
}

/// Columine 07 §2: the feed's prior is "the lane a rule binds: … a two-key
/// map's `field0 | field1 << 32`". A value-object or array field's cell is an
/// arena reference, not a lane, so a row whose field 0 or 1 is one binds no
/// prior, as a one-key struct map binds none; §7's watch log keeps the typed
/// row image, its frames included.
#[test]
fn a_value_object_lane_binds_no_feed_prior_and_the_watch_log_keeps_its_frame() {
    let prog = upsert_program();
    let mut state = init(&prog);
    let mut vm = armed(&mut state);
    let first = [row(7, 0, b"first frame of 7/0", &[1, 2])];
    assert_eq!(
        ErrorCode::Ok,
        run(&mut vm, &mut state, &prog, &Batch::of(&first))
    );
    // The insert was the slot's first write: write sequence 1.
    let inserted = 1;
    let second = [row(7, 0, b"second frame of 7/0", &[3])];
    assert_eq!(
        ErrorCode::Ok,
        run(&mut vm, &mut state, &prog, &Batch::of(&second))
    );
    let (k1, k2) = ([7u32], [0u32]);
    let erase = [u32s_as_bytes(&k1), u32s_as_bytes(&k2)];
    assert_eq!(
        ErrorCode::Ok,
        exec(&mut vm, &mut state, &exact_erase_program(), &erase, 1)
    );
    assert_eq!(
        feed_entries(&vm),
        [
            (Key::two(7, 0), Change::Inserted),
            (Key::two(7, 0), Change::Updated(None)),
            (Key::two(7, 0), Change::Removed(None)),
        ],
        "an arena reference is never a prior value"
    );

    // Since the insert, the pair's net change is its removal, and the prior
    // the log names is the inserted row's image, its frame bytes inline.
    let mut out = NetChanges::default();
    assert_eq!(
        Ok(NetAnswer::Changes),
        vm.net_changes(&state, SLOT, inserted, &NoStrings, &mut out)
    );
    assert!(
        contains(out.bytes(), b"first frame of 7/0"),
        "the watch log's prior is the typed row image"
    );
}

/// The gate is per lane: a row whose fields 0 and 1 are u32 lanes binds its
/// prior though another field is a value object.
#[test]
fn scalar_lanes_beside_a_value_object_field_keep_their_feed_prior() {
    let init_code = pair_init(&[UINT32, UINT32, VO]);
    let prog = program(
        7,
        &init_code,
        &[UPSERT2_ARRAYS, 0, 0, 1, 2, 2, 0, 2, 1, 1, 5, 6, 2],
    );
    let mut state = init(&prog);
    let mut vm = armed(&mut state);
    let first = [row(4, 1, b"", &[9])];
    assert_eq!(
        ErrorCode::Ok,
        run(&mut vm, &mut state, &prog, &Batch::of(&first))
    );
    let second = [row(4, 1, b"", &[8])];
    assert_eq!(
        ErrorCode::Ok,
        run(&mut vm, &mut state, &prog, &Batch::of(&second))
    );
    let count = first[0].count;
    assert_eq!(
        feed_entries(&vm),
        [
            (Key::two(4, 1), Change::Inserted),
            (
                Key::two(4, 1),
                Change::Updated(Some(u64::from(count) | (u64::from(count) << 32)))
            ),
        ]
    );
}
// =============================================================================
// The old scalar form and refusals
// =============================================================================

#[test]
fn the_scalar_pair_upsert_keeps_its_encoding_and_writes_no_arena_field() {
    let init_code = pair_init(&[UINT32, VO]);
    let scalar = program(3, &init_code, &[UPSERT2_LAST, 0, 0, 1, 1, 2, 0]);
    let arrays = program(3, &init_code, &[UPSERT2_ARRAYS, 0, 0, 1, 1, 2, 0, 0]);
    let (k1, k2, values) = ([1u32, 1], [0u32, 1], [10u32, 11]);
    let cols = [
        u32s_as_bytes(&k1),
        u32s_as_bytes(&k2),
        u32s_as_bytes(&values),
    ];
    let mut by_scalar = init(&scalar);
    let mut by_arrays = init(&arrays);
    assert_eq!(
        ErrorCode::Ok,
        exec(&mut Vm::default(), &mut by_scalar, &scalar, &cols, 2)
    );
    assert_eq!(
        ErrorCode::Ok,
        exec(&mut Vm::default(), &mut by_arrays, &arrays, &cols, 2)
    );
    assert_eq!(
        by_scalar, by_arrays,
        "0xa0 without arrays writes what 0x83 writes"
    );
    let map = map(&by_scalar);
    let pos = map.find(&by_scalar, 1, 1).expect("pair present");
    assert_eq!(
        Ok(None),
        map.rows().arena_field(&by_scalar, pos, 1),
        "the VO field stays unset"
    );
}

#[test]
fn wrong_kind_lanes_and_repeated_fields_are_invalid_programs() {
    let init_code = slot_init();
    let cases: [(&str, Vec<u8>); 5] = [
        (
            "scalar lane into a VO field",
            vec![UPSERT2_ARRAYS, 0, 0, 1, 1, 2, FRAME, 0],
        ),
        (
            "CSR pair into UINT32",
            vec![UPSERT2_ARRAYS, 0, 0, 1, 0, 1, 3, 4, COUNT],
        ),
        (
            "CSR pair past the descriptor",
            vec![UPSERT2_ARRAYS, 0, 0, 1, 0, 1, 3, 4, 3],
        ),
        (
            "one field mapped twice",
            vec![UPSERT2_ARRAYS, 0, 0, 1, 0, 2, 3, 4, FRAME, 5, 6, FRAME],
        ),
        ("seventeen arrays", {
            let mut code = vec![UPSERT2_ARRAYS, 0, 0, 1, 0, 17];
            for _ in 0..17 {
                code.extend([3, 4, FRAME]);
            }
            code
        }),
    ];
    let rows = [row(1, 0, b"f", &[1])];
    for (name, reduce) in cases {
        let prog = program(7, &init_code, &reduce);
        let mut state = init(&upsert_program());
        let before = state.clone();
        let mut vm = Vm::default();
        vm.undo_enable(&state).expect("undo window opens");
        assert_eq!(
            ErrorCode::InvalidProgram,
            run(&mut vm, &mut state, &prog, &Batch::of(&rows)),
            "{name}"
        );
        assert_eq!(before, state, "{name}: refusal writes nothing");
        assert_eq!(0, vm.undo_checkpoint(), "{name}: refusal journals nothing");
    }
}

#[test]
fn malformed_csr_and_sentinel_keys_refuse_before_any_write() {
    let prog = upsert_program();
    let mut state = init(&prog);
    run_ok(&mut state, &prog, &[row(2, 2, b"kept", &[1])]);
    let mut reversed = Batch::of(&[row(2, 3, b"abc", &[1])]);
    reversed.frame_offsets = vec![5, 2];
    let mut short = Batch::of(&[row(2, 3, b"abc", &[1])]);
    short.frame_offsets = vec![2, 99];
    let tombstone = Batch::of(&[row(TOMBSTONE, 3, b"t", &[])]);
    let cases = [
        (
            "reversed frame offsets",
            reversed,
            ErrorCode::InvalidCellValue,
        ),
        (
            "frame past its values column",
            short,
            ErrorCode::ColumnUnderrun,
        ),
        ("tombstone key1", tombstone, ErrorCode::InvalidKey),
    ];
    for (name, batch, expected) in cases {
        let before = state.clone();
        let mut vm = Vm::default();
        vm.undo_enable(&state).expect("undo window opens");
        assert_eq!(expected, run(&mut vm, &mut state, &prog, &batch), "{name}");
        assert_eq!(before, state, "{name}: refusal writes nothing");
        assert_eq!(0, vm.undo_checkpoint(), "{name}: refusal journals nothing");
    }
}
