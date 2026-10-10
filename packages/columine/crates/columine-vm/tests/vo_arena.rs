//! Native oracles for opaque value-object (VO) struct-map fields: descriptor
//! kind byte 10, an 8-byte row cell `[arena offset: u32 LE][length: u32 LE]`,
//! and element size 1 in the slot's CSR byte arena.
//!
//! Every scenario drives the real VM: programs go through `init_state`,
//! batches through `execute_batch` (undo-only lane) and `execute_batch_delta`
//! (delta lane), probes and scatters through their FLAT_MAP bodies, rollback
//! through the undo journal and exported delta segments, and growth through
//! `Vm::grow_state` once the undo window has ended. Stored bytes are read back
//! only through `StructMapSlot::arena_field`. The core owns opaque bytes, not a
//! frame codec, so payloads here are arbitrary byte strings, including 0, 1 and
//! 255.
//!
//! Negative control: `stored_ref_past_arena_used_is_forbidden` plants a row
//! reference whose end lies past `arena_used` but inside `arena_capacity`,
//! over zeroed, in-buffer bytes. Relax the end bound in
//! `StructMapSlot::arena_field` from `arena_used` to `arena_capacity` in an
//! isolated proof copy and the oracle reads `Ok(Some([9, 9, 0]))` where it
//! expects `Err(InvalidState)`, so its expectation fails.

use std::collections::BTreeMap;

use columine_types::NO_PARENT_TS_COL;
use columine_types::PROGRAM_MAGIC;
use columine_types::operand::encode_index;
use columine_types::types::{
    ChangeFlag, EMPTY_KEY, ErrorCode, STATE_HEADER_SIZE, SlotMetaOffset, StructFieldType, TOMBSTONE,
};
use columine_vm::bytes;
use columine_vm::meta::slot_meta_base;
use columine_vm::state_init::{
    ARENA_HEADER_SIZE, DEFAULT_ACCEPTED_PROGRAM_MAGICS, ProgramInvalidCause, SizingRefusal,
    calculate_grown_state_size, calculate_state_size, init_state, needs_growth_slot,
};
use columine_vm::struct_map::StructMapSlot;
use columine_vm::vm::{UNDO_CAPACITY, Vm, u32s_as_bytes};
use proptest::prelude::*;

// Wire bytes of the program and descriptor contract.
const HALT: u8 = 0x00;
const FOR_EACH: u8 = 0xe0;
const FLAT_MAP: u8 = 0xe1;
const SLOT_STRUCT_MAP: u8 = 0x18;
const SLOT_ORDERED_LIST: u8 = 0x19;
const SLOT_STRUCT_MAP2: u8 = 0x1b;
const UPSERT_LAST: u8 = 0x80;
const UPSERT_FIRST: u8 = 0x81;
const UPSERT_MAX: u8 = 0x82;
const PROBE: u8 = 0x2e;
const PROBE_SCATTER: u8 = 0x2f;
const GUARDED_SCATTER: u8 = 0x3f;
const LIST_APPEND: u8 = 0x84;
const LIST_APPEND_STRUCT: u8 = 0x85;
const STRUCT_MAP_FLAGS: u8 = 6;
const ORDERED_LIST_FLAGS: u8 = 7;
const STRUCT_MAP2_FLAGS: u8 = 10;
const STRUCT_LIST_ELEM: u8 = 0xff;

const UINT32: u8 = 0;
const BOOL: u8 = 3;
const ARRAY_U32: u8 = 5;
const ARRAY_BOOL: u8 = 9;
const VO: u8 = 10;

const TYPE_A: u32 = 1001;
const SLOT: u32 = 0;
const SCALAR_FIELD: u8 = 0;
const VO_FIELD: u8 = 1;

fn len_u32(len: usize) -> u32 {
    u32::try_from(len).expect("length fits u32")
}

fn at(offset: u32) -> usize {
    usize::try_from(offset).expect("offset fits usize")
}

/// Program with `num_slots` slot definitions in `init` and one batch section.
fn program_with_slots(num_slots: u32, num_inputs: u32, init: &[u8], reduce: &[u8]) -> Vec<u8> {
    let mut prog = vec![0u8; 32];
    prog.extend(PROGRAM_MAGIC.to_le_bytes());
    prog.extend([2, 0, 0, 0]); // version 2, no callbacks, no flags
    prog.extend(num_slots.to_le_bytes());
    prog.extend(num_inputs.to_le_bytes());
    prog.extend(len_u32(init.len() + 1).to_le_bytes());
    prog.extend(len_u32(reduce.len()).to_le_bytes());
    prog.extend_from_slice(init);
    prog.push(HALT);
    prog.extend_from_slice(reduce);
    prog
}

fn program(num_inputs: u32, init: &[u8], reduce: &[u8]) -> Vec<u8> {
    program_with_slots(1, num_inputs, init, reduce)
}

/// Struct map in `slot` with capacity hint `hint`: the slot holds
/// `next_power_of_two(2 * hint)` rows and `64 * rows` arena bytes.
fn struct_map_slot(slot: u8, hint: u16, kinds: &[u8]) -> Vec<u8> {
    let num_fields = u8::try_from(kinds.len()).expect("field count fits u8");
    let [lo, hi] = hint.to_le_bytes();
    let mut init = vec![SLOT_STRUCT_MAP, slot, STRUCT_MAP_FLAGS, lo, hi, num_fields];
    init.extend_from_slice(kinds);
    init
}

/// Struct map in slot 0 with capacity hint 4: 8 rows and 512 arena bytes.
fn struct_map_init(kinds: &[u8]) -> Vec<u8> {
    struct_map_slot(0, 4, kinds)
}

/// `op slot0 key:col0, 1 scalar (col1 -> field0), 1 CSR (offsets col2,
/// values col3 -> field1)`.
fn csr_upsert(op: u8) -> [u8; 10] {
    [op, 0, 0, 1, 1, SCALAR_FIELD, 1, 2, 3, VO_FIELD]
}

fn vo_program() -> Vec<u8> {
    program(4, &struct_map_init(&[UINT32, VO]), &csr_upsert(UPSERT_LAST))
}

fn max_program() -> Vec<u8> {
    let mut reduce = csr_upsert(UPSERT_MAX).to_vec();
    reduce.push(SCALAR_FIELD);
    program(4, &struct_map_init(&[UINT32, VO]), &reduce)
}

/// The program a `[UINT32, kind]` schema compiles to over the 4-column batch
/// shape: a CSR triple for byte-arena kinds, a scalar lane for BOOL.
fn kind_program(kind: u8) -> Vec<u8> {
    let init = struct_map_init(&[UINT32, kind]);
    if kind == BOOL {
        let reduce = [UPSERT_LAST, 0, 0, 2, 1, SCALAR_FIELD, 2, VO_FIELD, 0];
        program(4, &init, &reduce)
    } else {
        program(4, &init, &csr_upsert(UPSERT_LAST))
    }
}

/// Row replacement through the scalar lane only; the VO field stays unset.
fn scalar_only_program() -> Vec<u8> {
    let reduce = [UPSERT_LAST, 0, 0, 1, 1, SCALAR_FIELD, 0];
    program(2, &struct_map_init(&[UINT32, VO]), &reduce)
}

/// `FOR_EACH(TYPE_A) { body }`.
fn for_each(body: &[u8]) -> Vec<u8> {
    let mut reduce = vec![FOR_EACH, 0, 1];
    reduce.extend(TYPE_A.to_le_bytes());
    reduce.extend(
        u16::try_from(body.len())
            .expect("body fits u16")
            .to_le_bytes(),
    );
    reduce.extend_from_slice(body);
    reduce
}

/// `FOR_EACH(TYPE_A) { FLAT_MAP(offsets col 1, no parent ts) { inner } }`.
/// Column 0 holds event types, column 1 the child offsets, and child columns
/// start at 2; one parent with one child reads `offsets = [0, 1]`.
fn child_block(inner: &[u8]) -> Vec<u8> {
    let mut flat = vec![FLAT_MAP, 1];
    flat.extend(encode_index(NO_PARENT_TS_COL).as_bytes());
    flat.extend(
        u16::try_from(inner.len())
            .expect("body fits u16")
            .to_le_bytes(),
    );
    flat.extend_from_slice(inner);
    for_each(&flat)
}

fn init(prog: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS)
        .expect("valid program admission");
    assert!(size > 0, "program must be accepted");
    let mut state = vec![0u8; at(size)];
    init_state(&mut state, prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("init_state");
    state
}

/// Grow slot `SLOT` through the VM's own entry point. The VM refuses growth
/// while an undo window is active, so callers end the window first.
fn grown(vm: &mut Vm, state: &[u8]) -> Vec<u8> {
    let size = calculate_grown_state_size(state, SLOT).expect("grown state size");
    let mut next = vec![0u8; at(size)];
    vm.grow_state(state, &mut next, SLOT)
        .expect("Vm::grow_state");
    next
}

fn named(raw: u32) -> ErrorCode {
    ErrorCode::from_u32(raw).unwrap_or_else(|| panic!("VM returned unnamed status {raw}"))
}

/// One batch on the lane `delta` selects: `false` is the undo-only lane
/// (`execute_batch`), `true` the delta lane that exports undo/redo pairs.
fn run_lane_cols(
    vm: &mut Vm,
    state: &mut [u8],
    prog: &[u8],
    cols: &[&[u8]],
    len: u32,
    delta: bool,
) -> ErrorCode {
    let raw = if delta {
        vm.execute_batch_delta(state, prog, cols, len)
    } else {
        vm.execute_batch(state, prog, cols, len)
    };
    named(raw)
}

fn exec(vm: &mut Vm, state: &mut [u8], prog: &[u8], cols: &[&[u8]], len: u32) -> ErrorCode {
    named(vm.execute_batch_delta(state, prog, cols, len))
}

/// One CSR batch over the 4-column shape: key, scalar, offsets, values. The
/// values column starts with `prefix`, so offsets are absolute positions in
/// the source and never assumed to start at zero.
struct Rows {
    keys: Vec<u32>,
    scalars: Vec<u32>,
    offsets: Vec<u32>,
    values: Vec<u8>,
}

impl Rows {
    fn new<'a>(prefix: &[u8], rows: impl IntoIterator<Item = (u32, u32, &'a [u8])>) -> Self {
        let mut values = prefix.to_vec();
        let mut offsets = vec![len_u32(values.len())];
        let mut keys = Vec::new();
        let mut scalars = Vec::new();
        for (key, value, payload) in rows {
            keys.push(key);
            scalars.push(value);
            values.extend_from_slice(payload);
            offsets.push(len_u32(values.len()));
        }
        Self {
            keys,
            scalars,
            offsets,
            values,
        }
    }

    fn of<'a>(rows: impl IntoIterator<Item = (u32, &'a [u8])>) -> Self {
        let rows = rows
            .into_iter()
            .map(|(key, payload)| (key, key * 10, payload));
        Self::new(&[], rows)
    }

    fn raw(keys: &[u32], offsets: &[u32], values: &[u8]) -> Self {
        Self {
            keys: keys.to_vec(),
            scalars: keys.iter().map(|key| key * 10).collect(),
            offsets: offsets.to_vec(),
            values: values.to_vec(),
        }
    }

    fn len(&self) -> u32 {
        len_u32(self.keys.len())
    }

    fn cols(&self) -> Vec<&[u8]> {
        vec![
            u32s_as_bytes(&self.keys),
            u32s_as_bytes(&self.scalars),
            u32s_as_bytes(&self.offsets),
            &self.values,
        ]
    }

    /// Frame bytes the batch carries, excluding the prefix.
    fn frame_bytes(&self) -> u32 {
        let first = self.offsets.first().copied().unwrap_or(0);
        self.offsets.last().copied().unwrap_or(0) - first
    }
}

fn run(vm: &mut Vm, state: &mut [u8], prog: &[u8], rows: &Rows) -> ErrorCode {
    exec(vm, state, prog, &rows.cols(), rows.len())
}

fn run_lane(vm: &mut Vm, state: &mut [u8], prog: &[u8], rows: &Rows, delta: bool) -> ErrorCode {
    run_lane_cols(vm, state, prog, &rows.cols(), rows.len(), delta)
}

fn run_ok(state: &mut [u8], prog: &[u8], rows: &Rows) {
    let mut vm = Vm::default();
    assert_eq!(ErrorCode::Ok, run(&mut vm, state, prog, rows));
}

/// Undo to `checkpoint`. The restore is a success the test asserts, never a
/// result it ignores.
fn rollback(vm: &mut Vm, state: &mut [u8], checkpoint: u32) {
    let size = len_u32(state.len());
    assert_eq!(
        Ok(size),
        vm.undo_rollback(state, checkpoint),
        "rollback to {checkpoint}"
    );
}

fn replay_rollback(replay: &mut Vm, state: &mut [u8], undo: &[u8]) {
    let size = len_u32(state.len());
    let entry_size = Vm::delta_export_entry_size();
    assert_eq!(
        Ok(size),
        replay.delta_apply_rollback_segment(state, undo, entry_size)
    );
}

fn replay_rollforward(replay: &mut Vm, state: &mut [u8], redo: &[u8]) {
    let size = len_u32(state.len());
    let entry_size = Vm::delta_export_entry_size();
    assert_eq!(
        Ok(size),
        replay.delta_apply_rollforward_segment(state, redo, entry_size)
    );
}

fn arena_header(state: &[u8]) -> u32 {
    bytes::read_u32(state, STATE_HEADER_SIZE + SlotMetaOffset::GRACE_SECONDS)
}

fn arena_capacity(state: &[u8]) -> u32 {
    bytes::read_u32(state, arena_header(state))
}

fn arena_used(state: &[u8]) -> u32 {
    bytes::read_u32(state, arena_header(state) + 4)
}

fn arena_data(state: &[u8]) -> &[u8] {
    let base = arena_header(state) + ARENA_HEADER_SIZE;
    &state[at(base)..at(base + arena_capacity(state))]
}

fn position(state: &[u8], key: u32) -> u32 {
    StructMapSlot::bind(state, SLOT)
        .find(state, key)
        .unwrap_or_else(|| panic!("key {key} missing"))
}

fn vo(state: &[u8], key: u32) -> Result<Option<&[u8]>, ErrorCode> {
    StructMapSlot::bind(state, SLOT).arena_field(state, position(state, key), VO_FIELD)
}

fn scalar(state: &[u8], key: u32) -> u32 {
    let smap = StructMapSlot::bind(state, SLOT);
    let row = smap.row_off(position(state, key));
    bytes::read_u32(state, row + smap.field_offset(state, SCALAR_FIELD))
}

/// A `UINT32` field of the row keyed `key` in `slot`.
fn field_u32(state: &[u8], slot: u32, key: u32, field: u8) -> u32 {
    let smap = StructMapSlot::bind(state, slot);
    let pos = smap
        .find(state, key)
        .unwrap_or_else(|| panic!("key {key} missing from slot {slot}"));
    bytes::read_u32(state, smap.row_off(pos) + smap.field_offset(state, field))
}

/// Absolute offset of a row's field cell.
fn field_cell(state: &[u8], key: u32, field: u8) -> u32 {
    let smap = StructMapSlot::bind(state, SLOT);
    smap.row_off(position(state, key)) + smap.field_offset(state, field)
}

fn stored_ref(state: &[u8], key: u32) -> (u32, u32) {
    let cell = field_cell(state, key, VO_FIELD);
    (
        bytes::read_u32(state, cell),
        bytes::read_u32(state, cell + 4),
    )
}

fn plant_ref(state: &mut [u8], key: u32, offset: u32, len: u32) {
    let cell = field_cell(state, key, VO_FIELD);
    bytes::write_u32(state, cell, offset);
    bytes::write_u32(state, cell + 4, len);
}

type Model = BTreeMap<u32, (u32, Vec<u8>)>;

fn live_bytes(model: &Model) -> u32 {
    model
        .values()
        .map(|(_, payload)| len_u32(payload.len()))
        .sum()
}

fn assert_matches_model(state: &[u8], model: &Model) {
    let smap = StructMapSlot::bind(state, SLOT);
    assert_eq!(len_u32(model.len()), smap.size(state));
    for (&key, (expected_scalar, payload)) in model {
        assert_eq!(Ok(Some(payload.as_slice())), vo(state, key), "key {key}");
        assert_eq!(*expected_scalar, scalar(state, key), "key {key}");
    }
}

/// Live references tile `[0, arena_used)` exactly: compaction left no gap and
/// no dead byte behind.
fn assert_tightly_compacted(state: &[u8], keys: &[u32]) {
    let mut refs: Vec<(u32, u32)> = keys.iter().map(|&key| stored_ref(state, key)).collect();
    refs.sort_unstable();
    let mut next = 0u32;
    for (offset, len) in refs {
        assert_eq!(next, offset, "live payloads must be packed");
        next += len;
    }
    assert_eq!(next, arena_used(state));
}

/// Export `[checkpoint, end)` and prove both lanes replay byte-exact on
/// a fresh VM, without touching `vm`'s own journal.
fn assert_exported_delta_replays(vm: &mut Vm, before: &[u8], after: &[u8], checkpoint: u32) {
    let end = vm.undo_checkpoint();
    assert!(end > checkpoint, "VO writes must journal");
    assert!(!vm.undo_status().overflow, "journal must stay exact");
    assert_eq!(
        Ok(end - checkpoint),
        vm.delta_export_segment(checkpoint, end)
    );
    let undo = vm.delta_export_undo_bytes();
    let redo = vm.delta_export_redo_bytes();

    let mut replay = Vm::default();
    let mut rolled_back = after.to_vec();
    replay_rollback(&mut replay, &mut rolled_back, &undo);
    assert_eq!(rolled_back, before, "exported undo must restore the image");

    let mut rolled_forward = before.to_vec();
    replay_rollforward(&mut replay, &mut rolled_forward, &redo);
    assert_eq!(
        rolled_forward, after,
        "exported redo must reproduce the image"
    );
}

/// Exported replay plus the VM's own undo rollback of a copy of `after`.
fn assert_delta_roundtrip(vm: &mut Vm, before: &[u8], after: &[u8], checkpoint: u32) {
    assert_exported_delta_replays(vm, before, after, checkpoint);
    let mut production = after.to_vec();
    rollback(vm, &mut production, checkpoint);
    assert_eq!(production, before, "undo rollback must restore the image");
}

/// Ends the window on `state` itself. On the delta lane the exported undo and
/// redo must first replay byte-exact on a fresh VM; then the VM's own undo
/// rollback must restore `state`.
fn restore_lane(vm: &mut Vm, state: &mut [u8], before: &[u8], checkpoint: u32, delta: bool) {
    if delta {
        let after = state.to_vec();
        assert_exported_delta_replays(vm, before, &after, checkpoint);
    }
    rollback(vm, state, checkpoint);
    assert_eq!(before, &*state, "undo rollback must restore the image");
}

/// Refused before any row write or journal entry.
fn assert_refused_untouched(
    what: &str,
    prog: &[u8],
    state: &mut [u8],
    cols: &[&[u8]],
    len: u32,
    expected: ErrorCode,
) {
    let before = state.to_vec();
    let mut vm = Vm::default();
    vm.undo_enable(state).expect("undo window opens");
    let checkpoint = vm.undo_checkpoint();
    assert_eq!(expected, exec(&mut vm, state, prog, cols, len), "{what}");
    assert_eq!(
        before.as_slice(),
        &*state,
        "{what}: refusal must not touch the image"
    );
    assert_eq!(
        checkpoint,
        vm.undo_checkpoint(),
        "{what}: refusal must not journal"
    );
}

/// Refused; the host's undo rollback must restore the unchanged image.
fn assert_refused_rolled_back(prog: &[u8], state: &mut [u8], rows: &Rows, expected: ErrorCode) {
    let before = state.to_vec();
    let mut vm = Vm::default();
    vm.undo_enable(state).expect("undo window opens");
    let checkpoint = vm.undo_checkpoint();
    assert_eq!(expected, run(&mut vm, state, prog, rows));
    rollback(&mut vm, state, checkpoint);
    assert_eq!(
        before.as_slice(),
        &*state,
        "rollback must restore the image"
    );
}

// =============================================================================
// Opaque byte storage
// =============================================================================

#[test]
fn opaque_bytes_round_trip_verbatim_including_0_1_255() {
    let prog = vo_program();
    let mut state = init(&prog);
    let every_byte: Vec<u8> = (0..=u8::MAX).collect();
    let rows = Rows::new(
        &[0xaa, 0xbb, 0xcc],
        [
            (1, 7, &[0u8, 1, 255][..]),
            (2, 8, &every_byte[..]),
            (3, 9, &[255u8; 7][..]),
            (4, 0, &[][..]),
            (5, 255, &[1u8, 0, 0, 1][..]),
        ],
    );
    run_ok(&mut state, &prog, &rows);

    assert_eq!(Ok(Some(&[0u8, 1, 255][..])), vo(&state, 1));
    assert_eq!(Ok(Some(every_byte.as_slice())), vo(&state, 2));
    assert_eq!(Ok(Some(&[255u8; 7][..])), vo(&state, 3));
    assert_eq!(Ok(Some(&[][..])), vo(&state, 4));
    assert_eq!(Ok(Some(&[1u8, 0, 0, 1][..])), vo(&state, 5));
    let scalars = [1, 2, 3, 4, 5].map(|key| scalar(&state, key));
    assert_eq!([7, 8, 9, 0, 255], scalars);
    assert_eq!(3 + 256 + 7 + 4, arena_used(&state));
    // Rows append in batch order; the prefix bytes never reach the arena.
    assert_eq!((0, 3), stored_ref(&state, 1));
    assert_eq!((3, 256), stored_ref(&state, 2));
    assert_eq!((259, 7), stored_ref(&state, 3));
    assert_eq!((266, 0), stored_ref(&state, 4));
    assert_eq!((266, 4), stored_ref(&state, 5));
}

#[test]
fn for_each_body_writes_vo_per_matching_event() {
    let body = [UPSERT_LAST, 0, 1, 1, 2, SCALAR_FIELD, 1, 3, 4, VO_FIELD];
    let prog = program(5, &struct_map_init(&[UINT32, VO]), &for_each(&body));
    let mut state = init(&prog);

    let types = [TYPE_A, 7, TYPE_A];
    let keys = [1u32, 2, 3];
    let scalars = [10u32, 20, 30];
    let offsets = [0u32, 2, 5, 6];
    let values = [255u8, 0, 9, 9, 9, 1];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&keys),
        u32s_as_bytes(&scalars),
        u32s_as_bytes(&offsets),
        &values,
    ];
    let mut vm = Vm::default();
    assert_eq!(ErrorCode::Ok, exec(&mut vm, &mut state, &prog, &cols, 3));

    let smap = StructMapSlot::bind(&state, SLOT);
    assert_eq!(None, smap.find(&state, 2), "unmatched event must not write");
    assert_eq!(Ok(Some(&[255u8, 0][..])), vo(&state, 1));
    assert_eq!(Ok(Some(&[1u8][..])), vo(&state, 3));
    assert_eq!(
        3,
        arena_used(&state),
        "only matching events reach the arena"
    );
}

#[test]
fn replacement_appends_and_last_write_wins_with_dead_bytes() {
    let prog = vo_program();
    let mut state = init(&prog);
    run_ok(&mut state, &prog, &Rows::of([(1, &[1u8, 2, 3, 4, 5][..])]));
    run_ok(&mut state, &prog, &Rows::of([(1, &[6u8, 7, 8][..])]));

    assert_eq!(Ok(Some(&[6u8, 7, 8][..])), vo(&state, 1));
    assert_eq!(
        (5, 3),
        stored_ref(&state, 1),
        "replacement appends past the dead frame"
    );
    assert_eq!(8, arena_used(&state));
    assert_eq!(&[1u8, 2, 3, 4, 5, 6, 7, 8], &arena_data(&state)[..8]);

    // The same key twice in one batch: both frames are copied, the later wins.
    let twice = Rows::of([(1, &[0xa0u8; 2][..]), (1, &[0xb0u8; 3][..])]);
    run_ok(&mut state, &prog, &twice);
    assert_eq!(Ok(Some(&[0xb0u8; 3][..])), vo(&state, 1));
    assert_eq!((10, 3), stored_ref(&state, 1));
    assert_eq!(13, arena_used(&state));
    assert_eq!(1, StructMapSlot::bind(&state, SLOT).size(&state));
}

#[test]
fn unset_vo_field_reads_none_and_scalar_field_is_not_an_arena_kind() {
    let vo_prog = vo_program();
    let scalar_prog = scalar_only_program();
    let mut state = init(&vo_prog);

    let keys = [9u32, 1];
    let scalars = [90u32, 10];
    let first: Vec<&[u8]> = vec![u32s_as_bytes(&keys[..1]), u32s_as_bytes(&scalars[..1])];
    let mut vm = Vm::default();
    assert_eq!(
        ErrorCode::Ok,
        exec(&mut vm, &mut state, &scalar_prog, &first, 1)
    );
    assert_eq!(Ok(None), vo(&state, 9));
    assert_eq!(90, scalar(&state, 9));

    let smap = StructMapSlot::bind(&state, SLOT);
    let pos = position(&state, 9);
    let scalar_read = smap.arena_field(&state, pos, SCALAR_FIELD);
    assert_eq!(Err(ErrorCode::InvalidProgram), scalar_read);

    // Row replacement through the scalar lane clears the VO presence bit.
    run_ok(&mut state, &vo_prog, &Rows::of([(1, &[42u8][..])]));
    assert_eq!(Ok(Some(&[42u8][..])), vo(&state, 1));
    let second: Vec<&[u8]> = vec![u32s_as_bytes(&keys[1..]), u32s_as_bytes(&scalars[1..])];
    assert_eq!(
        ErrorCode::Ok,
        exec(&mut vm, &mut state, &scalar_prog, &second, 1)
    );
    assert_eq!(Ok(None), vo(&state, 1));
}

#[test]
fn upsert_first_keeps_the_first_frame_through_rollback_and_replay() {
    let prog = program(
        4,
        &struct_map_init(&[UINT32, VO]),
        &csr_upsert(UPSERT_FIRST),
    );
    let mut state = init(&prog);
    run_ok(&mut state, &prog, &Rows::of([(1, &[1u8, 1][..])]));
    let kept = state.clone();
    run_ok(&mut state, &prog, &Rows::of([(1, &[2u8, 2, 2][..])]));
    assert_eq!(
        kept, state,
        "a later frame must not touch arena, reference or bitset"
    );
    assert_eq!(Ok(Some(&[1u8, 1][..])), vo(&state, 1));

    // One delta batch: key 1 already holds its first frame; key 2 arrives
    // twice and keeps the earlier of the two.
    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("undo window opens");
    let before = state.clone();
    let rows = Rows::of([
        (1, &[255u8; 4][..]),
        (2, &[0u8, 255][..]),
        (2, &[7u8; 5][..]),
    ]);
    assert_eq!(ErrorCode::Ok, run(&mut vm, &mut state, &prog, &rows));
    assert_eq!(Ok(Some(&[1u8, 1][..])), vo(&state, 1));
    assert_eq!((0, 2), stored_ref(&state, 1));
    assert_eq!(Ok(Some(&[0u8, 255][..])), vo(&state, 2));
    assert_eq!(
        4,
        arena_used(&state),
        "skipped frames never reach the arena"
    );
    let after = state.clone();
    assert_delta_roundtrip(&mut vm, &before, &after, 0);
}

#[test]
fn upsert_max_replaces_vo_only_on_a_higher_comparison() {
    let prog = max_program();
    let mut state = init(&prog);
    run_ok(&mut state, &prog, &Rows::new(&[], [(1, 5, &[5u8][..])]));
    let held = state.clone();
    run_ok(&mut state, &prog, &Rows::new(&[], [(1, 3, &[3u8, 3][..])]));
    assert_eq!(held, state, "a lower comparison must be a no-op");
    run_ok(
        &mut state,
        &prog,
        &Rows::new(&[], [(1, 9, &[0u8, 255, 9][..])]),
    );
    assert_eq!(Ok(Some(&[0u8, 255, 9][..])), vo(&state, 1));
    assert_eq!(9, scalar(&state, 1));
    assert_eq!(4, arena_used(&state));
}

// =============================================================================
// Kind commitment: byte 10 versus the boolean kinds
// =============================================================================

#[test]
fn vo_kind_byte_is_the_only_difference_from_array_bool_image() {
    assert_eq!(Some(StructFieldType::Vo), StructFieldType::from_u8(VO));
    let vo_prog = kind_program(VO);
    let bool_prog = kind_program(ARRAY_BOOL);
    let mut vo_state = init(&vo_prog);
    let mut bool_state = init(&bool_prog);
    let rows = Rows::of([(1, &[0u8, 1, 255, 2][..]), (2, &[255u8][..])]);
    run_ok(&mut vo_state, &vo_prog, &rows);
    run_ok(&mut bool_state, &bool_prog, &rows);

    let smap = StructMapSlot::bind(&vo_state, SLOT);
    let descriptor_byte = at(smap.field_types_off + u32::from(VO_FIELD));
    assert_eq!(vo_state.len(), bool_state.len());
    let differing: Vec<usize> = vo_state
        .iter()
        .zip(&bool_state)
        .enumerate()
        .filter_map(|(index, (left, right))| (left != right).then_some(index))
        .collect();
    assert_eq!(vec![descriptor_byte], differing);
    assert_eq!(VO, vo_state[descriptor_byte]);
    assert_eq!(ARRAY_BOOL, bool_state[descriptor_byte]);
    assert_eq!(Ok(Some(&[0u8, 1, 255, 2][..])), vo(&vo_state, 1));
}

#[test]
fn kind_commitment_refuses_mismatched_program_and_state() {
    let rows = Rows::of([(1, &[0u8, 1, 255][..])]);
    let pairs = [(BOOL, VO), (ARRAY_BOOL, VO), (VO, ARRAY_BOOL), (VO, BOOL)];
    for (state_kind, program_kind) in pairs {
        let mut state = init(&kind_program(state_kind));
        let prog = kind_program(program_kind);
        let expected = ErrorCode::InvalidProgram;
        assert_refused_untouched(
            "kind commitment",
            &prog,
            &mut state,
            &rows.cols(),
            rows.len(),
            expected,
        );
    }
}

#[test]
fn bool_scalar_normalization_is_unchanged_beside_vo() {
    let prog = kind_program(BOOL);
    let mut state = init(&prog);
    // The BOOL program reads its cells from column 2.
    run_ok(
        &mut state,
        &prog,
        &Rows::raw(&[1, 2, 3, 4], &[255, 0, 1, 2], &[]),
    );
    let stored = [1, 2, 3, 4].map(|key| state[at(field_cell(&state, key, VO_FIELD))]);
    assert_eq!(
        [1, 0, 1, 1],
        stored,
        "BOOL cells normalize; VO bytes never do"
    );
}

// =============================================================================
// Tombstones, scalar-only replacement, undo and delta lanes
// =============================================================================

/// Removing a key leaves a TOMBSTONE in its probe cell. A fresh key whose
/// probe reaches that cell reuses it; rollback must restore the TOMBSTONE, not
/// EMPTY, and the same bytes must come back on both lanes.
#[test]
fn tombstone_reuse_restores_the_tombstone_byte_exact_on_both_lanes() {
    let prog = vo_program();
    for delta in [false, true] {
        let mut state = init(&prog);
        run_ok(
            &mut state,
            &prog,
            &Rows::of([(1, &[1u8][..]), (2, &[2u8, 2][..]), (3, &[3u8, 3, 3][..])]),
        );
        // A host-side removal, the same primitive rollback uses for absent
        // rows: the key cell becomes a TOMBSTONE, the row bits clear.
        let gone = position(&state, 2);
        let smap = StructMapSlot::bind(&state, SLOT);
        assert_eq!(Some(gone), smap.remove(&mut state, 2));
        smap.clear_bitset(&mut state, smap.row_off(gone));
        let cell = smap.keys_off + gone * 4;
        assert_eq!(TOMBSTONE, bytes::read_u32(&state, cell));

        // A fresh key whose probe reaches the tombstone before any empty cell.
        let reuse = (100u32..)
            .find(|&key| {
                matches!(smap.find_insert(&state, key), Some(probe) if !probe.found && probe.pos == gone)
            })
            .expect("some key probes the tombstone first");

        let mut vm = Vm::default();
        vm.undo_enable(&state).expect("undo window opens");
        let before = state.clone();
        let checkpoint = vm.undo_checkpoint();
        let payload = [0xabu8, 0xcd];
        let rows = Rows::of([(reuse, &payload[..])]);
        assert_eq!(
            ErrorCode::Ok,
            run_lane(&mut vm, &mut state, &prog, &rows, delta)
        );
        assert_eq!(
            gone,
            position(&state, reuse),
            "the insert reuses the tombstone cell"
        );
        assert_eq!(Ok(Some(&payload[..])), vo(&state, reuse));
        assert_eq!(3, StructMapSlot::bind(&state, SLOT).size(&state));

        restore_lane(&mut vm, &mut state, &before, checkpoint, delta);
        assert_eq!(
            TOMBSTONE,
            bytes::read_u32(&state, cell),
            "delta={delta}: tombstone restored"
        );
        assert_eq!(2, StructMapSlot::bind(&state, SLOT).size(&state));
    }
}

/// Replacing a row through the scalar lane clears the VO presence bit and
/// leaves the old frame as dead arena bytes. Undo-only and delta lanes both
/// restore the frame byte-exact.
#[test]
fn scalar_only_replacement_clears_the_vo_bit_and_rolls_back_byte_exact_on_both_lanes() {
    let vo_prog = vo_program();
    let scalar_prog = scalar_only_program();
    for delta in [false, true] {
        let mut state = init(&vo_prog);
        run_ok(&mut state, &vo_prog, &Rows::of([(1, &[1u8, 2, 3, 4][..])]));

        let keys = [1u32];
        let scalars = [42u32];
        let cols: Vec<&[u8]> = vec![u32s_as_bytes(&keys), u32s_as_bytes(&scalars)];
        let mut vm = Vm::default();
        vm.undo_enable(&state).expect("undo window opens");
        let before = state.clone();
        let checkpoint = vm.undo_checkpoint();
        assert_eq!(
            ErrorCode::Ok,
            run_lane_cols(&mut vm, &mut state, &scalar_prog, &cols, 1, delta)
        );
        assert_eq!(Ok(None), vo(&state, 1), "delta={delta}");
        assert_eq!(42, scalar(&state, 1));
        assert_eq!(
            4,
            arena_used(&state),
            "the replaced frame stays as dead bytes"
        );

        restore_lane(&mut vm, &mut state, &before, checkpoint, delta);
        assert_eq!(
            Ok(Some(&[1u8, 2, 3, 4][..])),
            vo(&state, 1),
            "delta={delta}"
        );
    }
}

// =============================================================================
// Undo, delta export, checkpoints and forks
// =============================================================================

#[test]
fn undo_rollback_and_exported_delta_replay_are_byte_exact() {
    let prog = vo_program();
    let mut state = init(&prog);
    let seed = Rows::of([(1, &[1u8, 0, 255][..]), (2, &[7u8; 9][..])]);
    run_ok(&mut state, &prog, &seed);

    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("undo window opens");
    let before = state.clone();
    let checkpoint = vm.undo_checkpoint();
    let rows = Rows::of([(1, &[255u8, 255][..]), (3, &[][..]), (4, &[0u8, 1][..])]);
    assert_eq!(ErrorCode::Ok, run(&mut vm, &mut state, &prog, &rows));
    assert_eq!(Ok(Some(&[255u8, 255][..])), vo(&state, 1));
    assert_eq!(Ok(Some(&[][..])), vo(&state, 3));
    let after = state.clone();
    assert_delta_roundtrip(&mut vm, &before, &after, checkpoint);
}

#[test]
fn nested_checkpoints_and_forks_stay_independent() {
    let prog = vo_program();
    let mut state = init(&prog);
    // Insert, then update, so later updates leave the change flags exactly as
    // they were when undo was enabled.
    run_ok(
        &mut state,
        &prog,
        &Rows::of([(1, &[1u8][..]), (2, &[2u8][..])]),
    );
    run_ok(
        &mut state,
        &prog,
        &Rows::of([(1, &[11u8][..]), (2, &[22u8][..])]),
    );

    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("undo window opens");
    let base = state.clone();
    let c1 = vm.undo_checkpoint();
    let batch_a = Rows::of([(1, &b"alpha"[..])]);
    assert_eq!(ErrorCode::Ok, run(&mut vm, &mut state, &prog, &batch_a));
    let after_a = state.clone();
    let c2 = vm.undo_checkpoint();

    let mut fork = after_a.clone();
    let mut fork_vm = Vm::default();
    fork_vm.undo_enable(&fork).expect("undo window opens");
    let fork_batch = Rows::of([(2, &b"fork"[..])]);
    assert_eq!(
        ErrorCode::Ok,
        run(&mut fork_vm, &mut fork, &prog, &fork_batch)
    );
    let batch_b = Rows::of([(1, &b"bravo"[..]), (2, &[0u8, 255][..])]);
    assert_eq!(ErrorCode::Ok, run(&mut vm, &mut state, &prog, &batch_b));
    let after_b = state.clone();

    assert_eq!(Ok(Some(&b"bravo"[..])), vo(&state, 1));
    assert_eq!(Ok(Some(&[0u8, 255][..])), vo(&state, 2));
    assert_eq!(Ok(Some(&b"alpha"[..])), vo(&fork, 1));
    assert_eq!(Ok(Some(&b"fork"[..])), vo(&fork, 2));

    // The exported B segment alone moves between the two checkpoint images.
    assert_exported_delta_replays(&mut vm, &after_a, &after_b, c2);
    rollback(&mut vm, &mut state, c2);
    assert_eq!(after_a, state);
    assert_eq!(
        Ok(Some(&b"fork"[..])),
        vo(&fork, 2),
        "rollback must not reach the fork"
    );
    rollback(&mut vm, &mut state, c1);
    assert_eq!(base, state);

    rollback(&mut fork_vm, &mut fork, 0);
    assert_eq!(after_a, fork);
}

/// Insert, then update the same key in a later segment, with the change flags
/// transitioning in each segment: rollback to the later checkpoint restores
/// the full image, flags included, and leaves the earlier segment applied.
#[test]
fn nested_checkpoint_rollback_restores_change_flags_that_transition_in_the_later_segment() {
    let prog = vo_program();
    let flags = at(slot_meta_base(SLOT) + SlotMetaOffset::CHANGE_FLAGS);
    for delta in [false, true] {
        let mut state = init(&prog);
        assert_eq!(0, state[flags], "a fresh image holds no change flags");
        let mut vm = Vm::default();
        vm.undo_enable(&state).expect("undo window opens");
        let base = state.clone();
        let c1 = vm.undo_checkpoint();

        let batch_a = Rows::of([(1, &b"alpha"[..])]);
        assert_eq!(
            ErrorCode::Ok,
            run_lane(&mut vm, &mut state, &prog, &batch_a, delta)
        );
        assert_eq!(
            ChangeFlag::INSERTED,
            state[flags],
            "segment one: 0 -> INSERTED"
        );
        let after_a = state.clone();
        let c2 = vm.undo_checkpoint();

        let batch_b = Rows::of([(1, &b"bravo"[..]), (2, &[0u8, 255][..])]);
        assert_eq!(
            ErrorCode::Ok,
            run_lane(&mut vm, &mut state, &prog, &batch_b, delta)
        );
        assert_eq!(
            ChangeFlag::INSERTED | ChangeFlag::UPDATED,
            state[flags],
            "segment two: INSERTED -> INSERTED|UPDATED"
        );
        let after_b = state.clone();
        if delta {
            assert_exported_delta_replays(&mut vm, &after_a, &after_b, c2);
        }

        // Only the later segment is undone; the earlier one stays applied.
        rollback(&mut vm, &mut state, c2);
        assert_eq!(
            after_a, state,
            "delta={delta}: rollback to c2 restores the full image"
        );
        assert_eq!(ChangeFlag::INSERTED, state[flags]);
        assert_eq!(Ok(Some(&b"alpha"[..])), vo(&state, 1));
        rollback(&mut vm, &mut state, c1);
        assert_eq!(base, state);
        assert_eq!(0, state[flags]);
    }
}

// =============================================================================
// Arena capacity, journal overflow and growth compaction
// =============================================================================

#[test]
fn exact_capacity_fill_and_empty_payload_at_arena_end_are_accepted() {
    let prog = vo_program();
    let mut state = init(&prog);
    let capacity = arena_capacity(&state);
    let full: Vec<u8> = (0..capacity).map(|index| index.to_le_bytes()[0]).collect();
    run_ok(&mut state, &prog, &Rows::of([(1, full.as_slice())]));
    assert_eq!(capacity, arena_used(&state));
    run_ok(&mut state, &prog, &Rows::of([(2, &[][..])]));
    assert_eq!((capacity, 0), stored_ref(&state, 2));
    assert_eq!(Ok(Some(&[][..])), vo(&state, 2));
    assert_eq!(Ok(Some(full.as_slice())), vo(&state, 1));

    let rows = Rows::of([(3, &[0u8][..])]);
    let expected = ErrorCode::NeedsGrowth;
    assert_refused_untouched(
        "arena full",
        &prog,
        &mut state,
        &rows.cols(),
        rows.len(),
        expected,
    );
}

#[test]
fn arena_overflow_refuses_without_row_write_then_growth_compacts_live_bytes() {
    let prog = vo_program();
    let mut state = init(&prog);
    let capacity = arena_capacity(&state);
    let marker = b"DEADVO!!";
    let dead: Vec<u8> = marker.iter().copied().cycle().take(64).collect();
    let live1: Vec<u8> = (0..100u8).map(|byte| byte ^ 0x5a).collect();
    let live2: Vec<u8> = (0..300u32)
        .map(|index| (index * 7).to_le_bytes()[0])
        .collect();
    run_ok(
        &mut state,
        &prog,
        &Rows::of([(1, dead.as_slice()), (3, &[][..])]),
    );
    let live = Rows::of([(1, live1.as_slice()), (2, live2.as_slice())]);
    run_ok(&mut state, &prog, &live);
    assert_eq!(464, arena_used(&state));
    let free = capacity - 464;

    // One row, one byte past the free space.
    let over = vec![255u8; at(free + 1)];
    let overflowing = Rows::of([(4, over.as_slice())]);
    let growth = ErrorCode::NeedsGrowth;
    assert_refused_untouched(
        "one byte past capacity",
        &prog,
        &mut state,
        &overflowing.cols(),
        overflowing.len(),
        growth,
    );
    assert_eq!(SLOT, needs_growth_slot());

    // Each row fits alone; the pair does not. Capacity admission sums the
    // rows' array bytes before the first write, so the batch refuses with
    // nothing applied and the open undo window holds no entry.
    let each = free / 2 + 1;
    assert!(
        each <= free && 2 * each > free,
        "fixture: rows fit alone, not summed"
    );
    let half = vec![1u8; at(each)];
    let summed = Rows::of([(5, half.as_slice()), (6, half.as_slice())]);
    let before_summed = state.clone();
    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("undo window opens");
    let checkpoint = vm.undo_checkpoint();
    assert_eq!(growth, run(&mut vm, &mut state, &prog, &summed));
    assert_eq!(SLOT, needs_growth_slot());
    assert_eq!(before_summed, state, "a refused batch writes nothing");
    assert_eq!(checkpoint, vm.undo_checkpoint(), "and journals nothing");
    rollback(&mut vm, &mut state, checkpoint);
    vm.undo_commit();
    assert_eq!(before_summed, state, "rollback must restore the image");

    let mut next = grown(&mut vm, &state);
    assert_eq!(capacity * 2, arena_capacity(&next));
    assert_eq!(400, arena_used(&next), "the dead frame must be dropped");
    assert_eq!(Ok(Some(live1.as_slice())), vo(&next, 1));
    assert_eq!(Ok(Some(live2.as_slice())), vo(&next, 2));
    assert_eq!(Ok(Some(&[][..])), vo(&next, 3));
    assert_tightly_compacted(&next, &[1, 2, 3]);
    let dead_survives = arena_data(&next)
        .windows(marker.len())
        .any(|window| window == marker);
    assert!(
        !dead_survives,
        "dead frame bytes must not survive compaction"
    );

    vm.undo_enable(&next).expect("undo window opens");
    let before = next.clone();
    assert_eq!(ErrorCode::Ok, run(&mut vm, &mut next, &prog, &overflowing));
    assert_eq!(Ok(Some(over.as_slice())), vo(&next, 4));
    assert_eq!(Ok(Some(live1.as_slice())), vo(&next, 1));
    assert_eq!(400 + len_u32(over.len()), arena_used(&next));
    let after = next.clone();
    assert_delta_roundtrip(&mut vm, &before, &after, 0);
}

#[test]
fn empty_payload_after_abandoned_frame_survives_growth_compaction() {
    let prog = vo_program();
    let mut state = init(&prog);
    let abandoned = vec![0xeeu8; 600];
    let seed = Rows::of([(1, abandoned.as_slice()), (2, &[4u8; 10][..])]);
    run_ok(&mut state, &prog, &seed);
    run_ok(&mut state, &prog, &Rows::of([(1, &[][..])]));
    assert_eq!((610, 0), stored_ref(&state, 1));

    let mut vm = Vm::default();
    let next = grown(&mut vm, &state);
    assert_eq!(10, arena_used(&next));
    assert_eq!(Ok(Some(&[][..])), vo(&next, 1));
    assert_eq!(Ok(Some(&[4u8; 10][..])), vo(&next, 2));
    assert_tightly_compacted(&next, &[1, 2]);
}

/// Budget exhaustion refuses the first unjournaled row. The accepted prefix
/// remains an exact physical segment, and the window stays unusable until end.
#[test]
fn journal_overflow_refuses_before_write_and_restores_the_exact_prefix_on_both_lanes() {
    const ROWS: u32 = 10_000;
    const ONE: &[u8] = &[1];
    let prog = program(
        4,
        &struct_map_slot(0, 8192, &[UINT32, VO]),
        &csr_upsert(UPSERT_LAST),
    );
    for delta in [false, true] {
        let mut state = init(&prog);
        let mut vm = Vm::default();
        vm.undo_enable(&state).expect("undo window opens");
        let committed = state.clone();
        let window = vm.undo_checkpoint();
        let earlier = Rows::of([(ROWS + 1, &[0u8, 1, 255][..])]);
        assert_eq!(
            ErrorCode::Ok,
            run_lane(&mut vm, &mut state, &prog, &earlier, delta)
        );
        let before_attempt = state.clone();
        let checkpoint = vm.undo_checkpoint();
        let rows = Rows::new(&[], (1..=ROWS).map(|key| (key, key, ONE)));
        assert_eq!(
            ErrorCode::JournalOverflow,
            run_lane(&mut vm, &mut state, &prog, &rows, delta),
        );
        assert!(
            vm.undo_status().overflow,
            "delta={delta}: real journal budget exhausted"
        );
        let end = vm.undo_checkpoint();
        assert!(end <= UNDO_CAPACITY);
        let accepted = StructMapSlot::bind(&state, SLOT).size(&state) - 1;
        assert!(
            accepted > 0 && accepted < ROWS,
            "a genuine accepted prefix precedes refusal"
        );
        assert_eq!(3 + accepted, arena_used(&state));
        assert_eq!(
            None,
            StructMapSlot::bind(&state, SLOT).find(&state, accepted + 1)
        );
        let after_prefix = state.clone();
        if delta {
            assert_eq!(
                Ok(end - checkpoint),
                vm.delta_export_segment(checkpoint, end)
            );
            assert!(vm.delta_export_overflow());
            let undo = vm.delta_export_undo_bytes();
            let redo = vm.delta_export_redo_bytes();
            let mut replay = Vm::default();
            let mut undone = after_prefix.clone();
            replay_rollback(&mut replay, &mut undone, &undo);
            assert_eq!(undone, before_attempt);
            let mut redone = before_attempt.clone();
            replay_rollforward(&mut replay, &mut redone, &redo);
            assert_eq!(redone, after_prefix);
        }
        let next = Rows::of([(1, &[0u8, 1, 255][..])]);
        assert_eq!(
            ErrorCode::JournalOverflow,
            run_lane(&mut vm, &mut state, &prog, &next, delta)
        );
        assert_eq!(after_prefix, state, "sticky refusal writes no bytes");
        assert_eq!(end, vm.undo_checkpoint());
        rollback(&mut vm, &mut state, checkpoint);
        assert_eq!(
            before_attempt, state,
            "rollback preserves the earlier window"
        );
        assert_eq!(
            ErrorCode::JournalOverflow,
            run_lane(&mut vm, &mut state, &prog, &next, delta)
        );
        assert_eq!(before_attempt, state);
        rollback(&mut vm, &mut state, window);
        assert_eq!(
            committed, state,
            "valid prefix rollback restores the complete image"
        );
        vm.undo_commit();
        vm.undo_enable(&state).expect("undo window opens");
        assert_eq!(
            ErrorCode::Ok,
            run_lane(&mut vm, &mut state, &prog, &next, delta)
        );
        restore_lane(&mut vm, &mut state, &committed, 0, delta);
    }
}

// =============================================================================
// Growth through Vm::grow_state
// =============================================================================

#[test]
fn growth_refuses_while_an_undo_window_is_active() {
    let prog = vo_program();
    let mut state = init(&prog);
    run_ok(&mut state, &prog, &Rows::of([(1, &[1u8][..])]));
    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("undo window opens");
    let mut next = vec![0u8; at(calculate_grown_state_size(&state, SLOT).expect("grown size"))];
    assert_eq!(
        Err(ErrorCode::NeedsGrowth),
        vm.grow_state(&state, &mut next, SLOT)
    );
    assert!(
        next.iter().all(|&byte| byte == 0),
        "a refused growth writes nothing"
    );

    vm.undo_commit();
    assert_eq!(Ok(()), vm.grow_state(&state, &mut next, SLOT));
    assert_eq!(1, StructMapSlot::bind(&next, SLOT).size(&next));
    assert_eq!(Ok(Some(&[1u8][..])), vo(&next, 1));
}

#[test]
fn savepoints_taken_before_growth_do_not_survive_the_resize() {
    let prog = vo_program();
    let mut state = init(&prog);
    run_ok(&mut state, &prog, &Rows::of([(1, &[1u8, 2][..])]));
    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("undo window opens");
    assert_eq!(
        ErrorCode::Ok,
        run(&mut vm, &mut state, &prog, &Rows::of([(2, &[3u8][..])]))
    );
    let stale = vm.undo_checkpoint();
    assert!(stale > 0, "the savepoint must cover a real batch");
    vm.undo_commit();

    let mut next = grown(&mut vm, &state);
    let grown_image = next.clone();
    vm.undo_enable(&next).expect("undo window opens");
    assert_eq!(
        Err(ErrorCode::InvalidState),
        vm.undo_rollback(&mut next, stale)
    );
    assert_eq!(grown_image, next, "a refused savepoint writes nothing");
    assert_eq!(Ok(Some(&[1u8, 2][..])), vo(&next, 1));
}

// =============================================================================
// Malformed CSR input and invalid programs
// =============================================================================

/// A fault in the batch's first row: nothing is written and nothing is
/// journalled.
#[test]
fn malformed_csr_in_the_first_row_refuses_before_mutation() {
    let prog = vo_program();
    let max = u32::MAX;
    let underrun = ErrorCode::ColumnUnderrun;
    let invalid_cell = ErrorCode::InvalidCellValue;
    let cases = [
        (
            "offsets end missing",
            Rows::raw(&[1], &[0], &[1, 2]),
            underrun,
        ),
        (
            "truncated source",
            Rows::raw(&[1], &[0, 5], &[1, 2, 3, 4]),
            underrun,
        ),
        (
            "end at u32::MAX",
            Rows::raw(&[1], &[0, max], &[0; 8]),
            underrun,
        ),
        (
            "empty row at u32::MAX",
            Rows::raw(&[1], &[max, max], &[0; 8]),
            underrun,
        ),
        ("key column short", Rows::raw(&[], &[0, 1], &[1]), underrun),
        ("reversed", Rows::raw(&[1], &[5, 2], &[0; 8]), invalid_cell),
        (
            "start at u32::MAX",
            Rows::raw(&[1], &[max, 0], &[0; 8]),
            invalid_cell,
        ),
    ];
    let mut state = init(&prog);
    run_ok(&mut state, &prog, &Rows::of([(7, &[7u8, 7][..])]));
    for (name, rows, expected) in cases {
        let before = state.clone();
        let mut vm = Vm::default();
        vm.undo_enable(&state).expect("undo window opens");
        // "key column short" declares one row over an empty key column.
        let status = exec(&mut vm, &mut state, &prog, &rows.cols(), 1);
        assert_eq!(expected, status, "{name}");
        assert_eq!(before, state, "{name}: refusal must not touch the image");
        assert_eq!(0, vm.undo_checkpoint(), "{name}: refusal must not journal");
    }
}

/// A fault in a later row: the rows before it may have applied, the faulting
/// row writes nothing, and the caller's checkpoint rollback restores the
/// image byte-exact, including the key lane of the new key it inserted.
#[test]
fn malformed_csr_in_a_later_row_rolls_back_to_the_checkpoint() {
    let prog = vo_program();
    let underrun = ErrorCode::ColumnUnderrun;
    let invalid_cell = ErrorCode::InvalidCellValue;
    let cases = [
        (
            "second row end missing",
            Rows::raw(&[1, 2], &[0, 2], &[1, 2, 3]),
            underrun,
        ),
        (
            "second row truncated",
            Rows::raw(&[1, 2], &[0, 2, 6], &[1, 2, 3, 4, 5]),
            underrun,
        ),
        (
            "second row reversed",
            Rows::raw(&[1, 2], &[0, 2, 1], &[0; 8]),
            invalid_cell,
        ),
    ];
    let mut state = init(&prog);
    run_ok(&mut state, &prog, &Rows::of([(7, &[7u8, 7][..])]));
    for (name, rows, expected) in cases {
        let before = state.clone();
        let mut vm = Vm::default();
        vm.undo_enable(&state).expect("undo window opens");
        let checkpoint = vm.undo_checkpoint();
        assert_eq!(expected, run(&mut vm, &mut state, &prog, &rows), "{name}");
        let smap = StructMapSlot::bind(&state, SLOT);
        assert_eq!(
            None,
            smap.find(&state, 2),
            "{name}: the faulting row writes nothing"
        );
        assert_eq!(Ok(Some(&[7u8, 7][..])), vo(&state, 7), "{name}");
        rollback(&mut vm, &mut state, checkpoint);
        assert_eq!(before, state, "{name}: rollback must restore the image");
        assert_eq!(
            None,
            StructMapSlot::bind(&state, SLOT).find(&state, 1),
            "{name}"
        );
    }
}

/// Inserting a new key into an empty probe cell and rolling back must restore
/// EMPTY, never TOMBSTONE. The arena mutation has a single capture owner, so
/// the key cell is journalled once, on both undo paths.
#[test]
fn new_key_rollback_restores_an_empty_cell_not_a_tombstone() {
    let prog = vo_program();
    let mut state = init(&prog);
    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("undo window opens");
    let before = state.clone();
    let rows = Rows::of([(3, &[0u8, 1, 255][..])]);
    assert_eq!(ErrorCode::Ok, run(&mut vm, &mut state, &prog, &rows));
    let smap = StructMapSlot::bind(&state, SLOT);
    let pos = position(&state, 3);
    let cell = smap.keys_off + pos * 4;
    assert_eq!(EMPTY_KEY, bytes::read_u32(&before, cell));
    let after = state.clone();

    let end = vm.undo_checkpoint();
    assert_eq!(Ok(end), vm.delta_export_segment(0, end));
    let undo = vm.delta_export_undo_bytes();
    let mut replayed = after.clone();
    replay_rollback(&mut Vm::default(), &mut replayed, &undo);
    assert_eq!(EMPTY_KEY, bytes::read_u32(&replayed, cell), "exported undo");
    assert_ne!(TOMBSTONE, bytes::read_u32(&replayed, cell));
    assert_eq!(before, replayed);

    rollback(&mut vm, &mut state, 0);
    assert_eq!(EMPTY_KEY, bytes::read_u32(&state, cell), "undo rollback");
    assert_eq!(0, StructMapSlot::bind(&state, SLOT).size(&state));
    assert_eq!(before, state);
}

#[test]
fn second_vo_field_fault_leaves_first_field_unwritten() {
    let reduce = [UPSERT_LAST, 0, 0, 1, 1, SCALAR_FIELD, 2, 2, 3, 1, 4, 5, 2];
    let prog = program(6, &struct_map_init(&[UINT32, VO, VO]), &reduce);
    let mut state = init(&prog);
    let keys = [1u32];
    let scalars = [10u32];
    let offsets = [0u32, 3];
    let full = [0u8, 1, 255];
    let short = [0u8, 1];
    let underrun = ErrorCode::ColumnUnderrun;

    let truncated: Vec<&[u8]> = vec![
        u32s_as_bytes(&keys),
        u32s_as_bytes(&scalars),
        u32s_as_bytes(&offsets),
        &full,
        u32s_as_bytes(&offsets),
        &short,
    ];
    assert_refused_untouched(
        "second field truncated",
        &prog,
        &mut state,
        &truncated,
        1,
        underrun,
    );

    let missing_end: Vec<&[u8]> = vec![
        u32s_as_bytes(&keys),
        u32s_as_bytes(&scalars),
        u32s_as_bytes(&offsets),
        &full,
        u32s_as_bytes(&offsets[..1]),
        &short,
    ];
    assert_refused_untouched(
        "second field end missing",
        &prog,
        &mut state,
        &missing_end,
        1,
        underrun,
    );

    let tail = [9u8, 8];
    let tail_offsets = [0u32, 2];
    let valid: Vec<&[u8]> = vec![
        u32s_as_bytes(&keys),
        u32s_as_bytes(&scalars),
        u32s_as_bytes(&offsets),
        &full,
        u32s_as_bytes(&tail_offsets),
        &tail,
    ];
    let mut vm = Vm::default();
    assert_eq!(ErrorCode::Ok, exec(&mut vm, &mut state, &prog, &valid, 1));
    let smap = StructMapSlot::bind(&state, SLOT);
    let pos = position(&state, 1);
    assert_eq!(Ok(Some(&full[..])), smap.arena_field(&state, pos, 1));
    assert_eq!(Ok(Some(&tail[..])), smap.arena_field(&state, pos, 2));
    assert_eq!(5, arena_used(&state));
}

#[test]
fn element_byte_length_overflow_is_an_invalid_cell() {
    let init_code = struct_map_init(&[UINT32, ARRAY_U32]);
    let prog = program(4, &init_code, &csr_upsert(UPSERT_LAST));
    let mut state = init(&prog);
    let keys = [1u32];
    let scalars = [10u32];
    // 0x4000_0000 four-byte elements are 2^32 bytes: the length overflows u32.
    let offsets = [0u32, 0x4000_0000];
    let values = [1u32, 2, 3, 4];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&keys),
        u32s_as_bytes(&scalars),
        u32s_as_bytes(&offsets),
        u32s_as_bytes(&values),
    ];
    let expected = ErrorCode::InvalidCellValue;
    assert_refused_untouched(
        "element length overflow",
        &prog,
        &mut state,
        &cols,
        1,
        expected,
    );
}

#[test]
fn wrong_kind_lanes_are_invalid_programs() {
    let init_code = struct_map_init(&[UINT32, VO]);
    let mut comparison_on_vo = csr_upsert(UPSERT_MAX).to_vec();
    comparison_on_vo.push(VO_FIELD);
    let scalar_into_vo = [UPSERT_LAST, 0, 0, 1, 1, VO_FIELD, 0];
    let csr_into_scalar = [UPSERT_LAST, 0, 0, 0, 1, 2, 3, SCALAR_FIELD];
    let csr_past_schema = [UPSERT_LAST, 0, 0, 0, 1, 2, 3, 5];
    let programs = [
        (
            "scalar lane into VO",
            program(4, &init_code, &scalar_into_vo),
        ),
        (
            "CSR triple into UINT32",
            program(4, &init_code, &csr_into_scalar),
        ),
        (
            "CSR triple past the schema",
            program(4, &init_code, &csr_past_schema),
        ),
        (
            "max comparison on VO",
            program(4, &init_code, &comparison_on_vo),
        ),
    ];
    let rows = Rows::of([(1, &[1u8, 2][..])]);
    let mut state = init(&vo_program());
    for (name, prog) in programs {
        let before = state.clone();
        let mut vm = Vm::default();
        vm.undo_enable(&state).expect("undo window opens");
        let status = run(&mut vm, &mut state, &prog, &rows);
        assert_eq!(ErrorCode::InvalidProgram, status, "{name}");
        assert_eq!(before, state, "{name}");
        assert_eq!(0, vm.undo_checkpoint(), "{name}");
    }
}

/// A truncated comparison column on an existing row is a typed underrun. The
/// comparison is read before the key lookup, so it is never silently skipped.
#[test]
fn truncated_max_comparison_on_an_existing_row_is_a_column_underrun() {
    let prog = max_program();
    let mut state = init(&prog);
    run_ok(&mut state, &prog, &Rows::new(&[], [(1, 5, &[5u8][..])]));
    let keys = [1u32];
    let offsets = [0u32, 1];
    let values = [9u8];
    let no_comparisons: &[u8] = &[];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&keys),
        no_comparisons,
        u32s_as_bytes(&offsets),
        &values[..],
    ];
    assert_refused_untouched(
        "truncated MAX comparison",
        &prog,
        &mut state,
        &cols,
        1,
        ErrorCode::ColumnUnderrun,
    );
}

/// FIRST and MAX check operand kinds before they may skip a row. Key 1 already
/// holds a row, so each program would skip it; every wrong-kind program must
/// still refuse with `InvalidProgram` and leave the image untouched.
#[test]
fn wrong_kind_lanes_refuse_even_when_first_or_max_would_skip_the_row() {
    let init_code = struct_map_init(&[UINT32, UINT32, VO]);
    let seed = program(4, &init_code, &[UPSERT_LAST, 0, 0, 1, 1, 0, 1, 2, 3, 2]);
    let mut state = init(&seed);
    run_ok(&mut state, &seed, &Rows::new(&[], [(1, 5, &[7u8][..])]));
    // A lower comparison (3 < 5) would skip the existing row.
    let lower = Rows::new(&[], [(1, 3, &[9u8][..])]);
    let cases: [(&str, Vec<u8>); 4] = [
        (
            "first: scalar lane into VO",
            vec![UPSERT_FIRST, 0, 0, 1, 1, 2, 0],
        ),
        (
            "first: CSR into UINT32",
            vec![UPSERT_FIRST, 0, 0, 0, 1, 2, 3, 1],
        ),
        (
            "max: CSR into UINT32",
            vec![UPSERT_MAX, 0, 0, 1, 1, 0, 1, 2, 3, 1, 0],
        ),
        (
            "max: comparison on VO",
            vec![UPSERT_MAX, 0, 0, 1, 1, 0, 0, 2],
        ),
    ];
    for (name, reduce) in cases {
        let prog = program(4, &init_code, &reduce);
        assert_refused_untouched(
            name,
            &prog,
            &mut state,
            &lower.cols(),
            lower.len(),
            ErrorCode::InvalidProgram,
        );
    }
}

// =============================================================================
// Probe, probe-scatter, guarded scatter and ordered-list refusals
// =============================================================================

/// Source slot 0 `[route: UINT32, op: UINT32, out key: UINT32, payload: VO]`
/// holds key 1 -> route 0, op 0, out key 5, payload `[7, 9]`. Output slot 1
/// `[UINT32, UINT32, VO]` starts empty. Both slots carry arenas.
fn seeded_probe_source() -> (Vec<u8>, Vec<u8>) {
    seeded_probe_source_with_output(&struct_map_slot(1, 4, &[UINT32, UINT32, VO]))
}

fn seeded_probe_source_with_output(output: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let init_code = [
        struct_map_slot(0, 4, &[UINT32, UINT32, UINT32, VO]),
        output.to_vec(),
    ]
    .concat();
    let seed = program_with_slots(
        2,
        6,
        &init_code,
        &[UPSERT_LAST, 0, 0, 3, 1, 0, 2, 1, 3, 2, 1, 4, 5, 3],
    );
    let mut state = init(&seed);
    let keys = [1u32];
    let route = [0u32];
    let op = [0u32];
    let out_key = [5u32];
    let offsets = [0u32, 2];
    let payload = [7u8, 9];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&keys),
        u32s_as_bytes(&route),
        u32s_as_bytes(&op),
        u32s_as_bytes(&out_key),
        u32s_as_bytes(&offsets),
        &payload[..],
    ];
    assert_eq!(
        ErrorCode::Ok,
        exec(&mut Vm::default(), &mut state, &seed, &cols, 1)
    );
    (init_code, state)
}

/// `0x2e` in a child block: probe slot 0 by column 2, miss skips, one field
/// pair into output slot 1, output key column 3.
fn probe_body(probe_fi: u8, out_fi: u8) -> Vec<u8> {
    vec![PROBE, 0, 2, 0, 1, 1, probe_fi, out_fi, 3]
}

/// `0x2f` in a child block: probe slot 0 by column 2, miss skips, route field
/// `route_fi`, op field 1, one kind-0 route into slot 1 field `dest_field`,
/// out key field 2, value field `v_src`.
fn scatter_body(route_fi: u8, dest_field: u8, v_src: u8) -> Vec<u8> {
    vec![
        PROBE_SCATTER,
        0,
        2,
        0,
        route_fi,
        1,
        1,
        0,
        1,
        dest_field,
        2,
        v_src,
    ]
}

/// Child columns of one FLAT_MAP child under one FOR_EACH parent: event
/// types, child offsets, then the probe and output keys.
fn probe_child_cols<'a>(
    types: &'a [u32],
    offsets: &'a [u32],
    keys: &'a [u32],
    out_keys: &'a [u32],
) -> Vec<&'a [u8]> {
    vec![
        u32s_as_bytes(types),
        u32s_as_bytes(offsets),
        u32s_as_bytes(keys),
        u32s_as_bytes(out_keys),
    ]
}

#[test]
fn probe_refuses_vo_sources_destinations_and_bad_ordinals_before_writes() {
    let types = [TYPE_A];
    let offsets = [0u32, 1];
    let keys = [1u32];
    let out_keys = [5u32];
    let cols = probe_child_cols(&types, &offsets, &keys, &out_keys);

    // Positive control: UINT32 source into UINT32 destination lands the value.
    let (init_code, mut state) = seeded_probe_source();
    let prog = program_with_slots(2, 4, &init_code, &child_block(&probe_body(2, 0)));
    let mut vm = Vm::default();
    assert_eq!(ErrorCode::Ok, exec(&mut vm, &mut state, &prog, &cols, 1));
    assert_eq!(5, field_u32(&state, 1, 5, 0));

    // (source field, destination field): every refusal is checked before the
    // output row is created.
    let refusals: [(&str, u8, u8); 5] = [
        ("VO source into VO destination", 3, 2),
        ("UINT32 source into VO destination", 0, 2),
        ("VO source into UINT32 destination", 3, 0),
        ("source ordinal past the probed row", 9, 0),
        ("destination ordinal past the output row", 2, 9),
    ];
    for (what, probe_fi, out_fi) in refusals {
        let (init_code, mut state) = seeded_probe_source();
        let prog = program_with_slots(
            2,
            4,
            &init_code,
            &child_block(&probe_body(probe_fi, out_fi)),
        );
        assert_refused_untouched(what, &prog, &mut state, &cols, 1, ErrorCode::InvalidProgram);
    }
}

#[test]
fn probe_scatter_refuses_vo_routes_and_bad_ordinals_before_writes() {
    let types = [TYPE_A];
    let offsets = [0u32, 1];
    let keys = [1u32];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&offsets),
        u32s_as_bytes(&keys),
    ];

    // Positive control: UINT32 route and op, UINT32 destination and value.
    let (init_code, mut state) = seeded_probe_source();
    let prog = program_with_slots(2, 3, &init_code, &child_block(&scatter_body(0, 0, 2)));
    let mut vm = Vm::default();
    assert_eq!(ErrorCode::Ok, exec(&mut vm, &mut state, &prog, &cols, 1));
    assert_eq!(5, field_u32(&state, 1, 5, 0));

    // (route field, destination field, value field).
    let refusals: [(&str, u8, u8, u8); 6] = [
        ("VO route field", 3, 2, 2),
        ("VO destination", 0, 2, 2),
        ("VO source into UINT32 destination", 0, 0, 3),
        ("VO source into VO destination", 0, 2, 3),
        ("destination ordinal", 0, 9, 2),
        ("value ordinal", 0, 0, 9),
    ];
    for (what, route_fi, dest_field, v_src) in refusals {
        let (init_code, mut state) = seeded_probe_source();
        let body = scatter_body(route_fi, dest_field, v_src);
        let prog = program_with_slots(2, 3, &init_code, &child_block(&body));
        assert_refused_untouched(what, &prog, &mut state, &cols, 1, ErrorCode::InvalidProgram);
    }

    let (init_code, mut state) = seeded_probe_source();
    let mut set_route = scatter_body(0, 0, 2);
    set_route[7] = 1; // A set route cannot reinterpret the VO struct-map target.
    let prog = program_with_slots(2, 3, &init_code, &child_block(&set_route));
    assert_refused_untouched(
        "set route into struct-map arena",
        &prog,
        &mut state,
        &cols,
        1,
        ErrorCode::InvalidProgram,
    );

    // An actual HashSet destination makes the source-kind refusal observable:
    // a scalar source lands key 5, but a VO cell is never a set key.
    let set_definition = [0x10, 1, 1, 4, 0];
    let (init_code, mut state) = seeded_probe_source_with_output(&set_definition);
    let prog = program_with_slots(2, 3, &init_code, &child_block(&set_route));
    assert_eq!(
        ErrorCode::Ok,
        exec(&mut Vm::default(), &mut state, &prog, &cols, 1)
    );
    assert!(
        columine_vm::vm::CheckedSet::bind(&state, 1)
            .expect("valid set slot")
            .contains(5)
    );

    let (init_code, mut state) = seeded_probe_source_with_output(&set_definition);
    let mut vo_set_route = scatter_body(0, 0, 3);
    vo_set_route[7] = 1;
    let prog = program_with_slots(2, 3, &init_code, &child_block(&vo_set_route));
    assert_refused_untouched(
        "VO cell cannot become a set key",
        &prog,
        &mut state,
        &cols,
        1,
        ErrorCode::InvalidProgram,
    );
}

/// A guarded kind-0 route whose destination is a VO field is refused when the
/// route is selected, before the guard row is created or stamped.
#[test]
fn guarded_scatter_refuses_a_vo_destination_before_the_guard_stamp() {
    let init_code = struct_map_init(&[UINT32, VO]);
    // 0x3f: route col 3, op col 4, key col 2, guard slot 0 keyed by col 5 into
    // field 0, one kind-0 route into slot 0 field 1 (VO), value col 6.
    let body = [GUARDED_SCATTER, 3, 4, 2, 0, 1, 5, 0, 1, 0, 0, 1, 6];
    let prog = program_with_slots(1, 7, &init_code, &child_block(&body));
    let types = [TYPE_A];
    let offsets = [0u32, 1];
    let keys = [1u32];
    let route = [0u32];
    let op = [0u32];
    let guard = [1u32];
    let value = [0u32];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&offsets),
        u32s_as_bytes(&keys),
        u32s_as_bytes(&route),
        u32s_as_bytes(&op),
        u32s_as_bytes(&guard),
        u32s_as_bytes(&value),
    ];
    let mut state = init(&prog);
    assert_refused_untouched(
        "guarded VO destination",
        &prog,
        &mut state,
        &cols,
        1,
        ErrorCode::InvalidProgram,
    );
    assert_eq!(
        None,
        StructMapSlot::bind(&state, SLOT).find(&state, 1),
        "no guard row is created"
    );

    let mut set_body = body;
    set_body[9] = 1; // Invalid set target must be refused before the guard stamp.
    let set_prog = program_with_slots(1, 7, &init_code, &child_block(&set_body));
    let mut state = init(&set_prog);
    assert_refused_untouched(
        "guarded set route into struct-map",
        &set_prog,
        &mut state,
        &cols,
        1,
        ErrorCode::InvalidProgram,
    );
}

/// Ordered-list opcodes name a slot by index only. Aimed at a VO struct map,
/// both list appends must refuse before any journal or write.
#[test]
fn ordered_list_opcodes_refuse_a_vo_struct_map_before_writes() {
    let init_code = struct_map_init(&[UINT32, VO]);
    let types = [TYPE_A];
    let keys = [1u32];
    let vals = [7u32];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&keys),
        u32s_as_bytes(&vals),
    ];
    let cases: [(&str, Vec<u8>); 2] = [
        ("LIST_APPEND", vec![LIST_APPEND, 0, 2]),
        ("LIST_APPEND_STRUCT", vec![LIST_APPEND_STRUCT, 0, 1, 2, 0]),
    ];
    for (what, body) in cases {
        let prog = program(3, &init_code, &for_each(&body));
        let mut state = init(&prog);
        assert_refused_untouched(what, &prog, &mut state, &cols, 1, ErrorCode::InvalidProgram);
    }
}

// =============================================================================
// Differential model
// =============================================================================

fn payload_byte() -> impl Strategy<Value = u8> {
    prop_oneof![Just(0u8), Just(1u8), Just(255u8), any::<u8>()]
}

fn row() -> impl Strategy<Value = (u32, u32, Vec<u8>)> {
    (
        1u32..=6,
        any::<u32>(),
        prop::collection::vec(payload_byte(), 0..48),
    )
}

type ModelBatch = (Vec<u8>, Vec<(u32, u32, Vec<u8>)>);

fn batch() -> impl Strategy<Value = ModelBatch> {
    (
        prop::collection::vec(any::<u8>(), 0..4),
        prop::collection::vec(row(), 1..=6),
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Arbitrary opaque frames against a same-byte model: every batch reads
    /// back exactly, appends exactly its frames to the arena, and round-trips
    /// through undo and exported undo/redo; a growth request leaves the image
    /// untouched and growth compacts to the live frames only.
    #[test]
    fn vo_batches_match_same_byte_model(batches in prop::collection::vec(batch(), 1..=12)) {
        let prog = vo_program();
        let mut state = init(&prog);
        let mut model = Model::new();
        let mut vm = Vm::default();

        for (prefix, frames) in &batches {
            let frames_in = frames
                .iter()
                .map(|(key, value, payload)| (*key, *value, payload.as_slice()));
            let rows = Rows::new(prefix, frames_in);
            vm.undo_commit();
            vm.undo_enable(&state).expect("undo window opens");
            let checkpoint = vm.undo_checkpoint();
            let mut before = state.clone();
            let mut status = run(&mut vm, &mut state, &prog, &rows);
            if status == ErrorCode::NeedsGrowth {
                // Earlier rows of the batch may have applied: the caller rolls
                // back to its checkpoint, ends the window, grows through the VM,
                // and replays the whole batch.
                prop_assert_eq!(SLOT, needs_growth_slot());
                rollback(&mut vm, &mut state, checkpoint);
                vm.undo_commit();
                prop_assert_eq!(&before, &state);
                state = grown(&mut vm, &state);
                prop_assert_eq!(live_bytes(&model), arena_used(&state));
                assert_matches_model(&state, &model);
                let keys: Vec<u32> = model.keys().copied().collect();
                assert_tightly_compacted(&state, &keys);
                vm.undo_enable(&state).expect("undo window opens");
                before = state.clone();
                status = run(&mut vm, &mut state, &prog, &rows);
            }
            prop_assert_eq!(ErrorCode::Ok, status);
            prop_assert_eq!(arena_used(&before) + rows.frame_bytes(), arena_used(&state));
            for (key, value, payload) in frames {
                model.insert(*key, (*value, payload.clone()));
            }
            assert_matches_model(&state, &model);
            let after = state.clone();
            assert_delta_roundtrip(&mut vm, &before, &after, 0);
        }
    }
}

// =============================================================================
// Forbidden stored references
// =============================================================================

/// key 1 -> [1, 2, 3] at 0, key 2 -> [9, 9] at 3; arena_used = 5.
fn two_frame_state() -> (Vec<u8>, Vec<u8>) {
    let prog = vo_program();
    let mut state = init(&prog);
    run_ok(
        &mut state,
        &prog,
        &Rows::of([(1, &[1u8, 2, 3][..]), (2, &[9u8, 9][..])]),
    );
    assert_eq!((3, 2), stored_ref(&state, 2));
    assert_eq!(5, arena_used(&state));
    (prog, state)
}

#[test]
fn stored_ref_past_arena_used_is_forbidden() {
    let (prog, mut state) = two_frame_state();
    // End 6 is past arena_used (5) but well inside capacity, over zeroed
    // in-buffer bytes: only the used bound tells this reference is forged.
    plant_ref(&mut state, 2, 3, 3);
    assert_eq!(Err(ErrorCode::InvalidState), vo(&state, 2));
    assert_eq!(
        Ok(Some(&[1u8, 2, 3][..])),
        vo(&state, 1),
        "corruption is per reference"
    );

    let rows = Rows::of([(2, &[5u8][..])]);
    assert_refused_rolled_back(&prog, &mut state, &rows, ErrorCode::InvalidState);
}

#[test]
fn corrupt_stored_refs_read_as_invalid_state() {
    let forged = [
        ("empty reference past used", 6, 0),
        ("offset plus length wraps", u32::MAX - 1, 4),
        ("length u32::MAX", 3, u32::MAX),
        ("offset u32::MAX", u32::MAX, 0),
    ];
    for (name, offset, len) in forged {
        let (_, mut state) = two_frame_state();
        plant_ref(&mut state, 2, offset, len);
        assert_eq!(Err(ErrorCode::InvalidState), vo(&state, 2), "{name}");
        assert_eq!(Ok(Some(&[1u8, 2, 3][..])), vo(&state, 1), "{name}");
    }
}

#[test]
fn corrupt_arena_header_is_invalid_state_for_read_and_write() {
    let rows = Rows::of([(3, &[0u8, 1][..])]);
    let invalid = ErrorCode::InvalidState;

    // used below a live end: the reference past it is refused, the one
    // before it is not.
    let (prog, mut state) = two_frame_state();
    let header = arena_header(&state);
    bytes::write_u32(&mut state, header + 4, 4);
    assert_eq!(Err(invalid), vo(&state, 2));
    assert_eq!(Ok(Some(&[1u8, 2, 3][..])), vo(&state, 1));

    // used past capacity.
    let (_, mut state) = two_frame_state();
    let capacity = arena_capacity(&state);
    bytes::write_u32(&mut state, header + 4, capacity + 1);
    assert_eq!(Err(invalid), vo(&state, 1));
    assert_refused_rolled_back(&prog, &mut state, &rows, invalid);

    // capacity claims bytes past the state buffer; a reference into them is
    // forged and the arena itself is no longer trustworthy.
    let (_, mut state) = two_frame_state();
    let beyond = len_u32(state.len());
    bytes::write_u32(&mut state, header, u32::MAX);
    bytes::write_u32(&mut state, header + 4, u32::MAX - 8);
    plant_ref(&mut state, 2, beyond, 4);
    assert_eq!(Err(invalid), vo(&state, 2));
    assert_eq!(Err(invalid), vo(&state, 1));
    assert_refused_rolled_back(&prog, &mut state, &rows, invalid);
}

#[test]
fn unsupported_vo_descriptors_refuse_initialization() {
    let list = |elem: &[u8]| {
        let mut init = vec![SLOT_ORDERED_LIST, 0, ORDERED_LIST_FLAGS, 8, 0];
        init.extend_from_slice(elem);
        init
    };
    let map2 = |kinds: &[u8]| {
        let mut init = vec![SLOT_STRUCT_MAP2, 0, STRUCT_MAP2_FLAGS, 4, 0, 2];
        init.extend_from_slice(kinds);
        init
    };
    let refused = [
        ("ordered list of VO", list(&[VO])),
        (
            "struct list with a VO field",
            list(&[STRUCT_LIST_ELEM, 2, UINT32, VO]),
        ),
        ("two-key struct map with a VO field", map2(&[UINT32, VO])),
        ("unknown kind 11", struct_map_init(&[UINT32, 11])),
    ];
    for (name, init_code) in refused {
        let prog = program(0, &init_code, &[]);
        let size = calculate_state_size(&prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS);
        assert_eq!(
            Err(SizingRefusal::Invalid(
                ProgramInvalidCause::MalformedProgram
            )),
            size,
            "{name}"
        );
        let mut state = vec![0u8; 1 << 16];
        let status = init_state(&mut state, &prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS);
        assert_eq!(Err(ErrorCode::InvalidProgram), status, "{name}");
    }
    // The refusals are about kind 10, not the containers.
    let accepted = [
        list(&[BOOL]),
        list(&[STRUCT_LIST_ELEM, 2, UINT32, BOOL]),
        map2(&[UINT32, BOOL]),
        struct_map_init(&[UINT32, VO]),
    ];
    for init_code in accepted {
        let prog = program(0, &init_code, &[]);
        let size = calculate_state_size(&prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS)
            .expect("valid program admission");
        assert!(size > 0);
    }
}
