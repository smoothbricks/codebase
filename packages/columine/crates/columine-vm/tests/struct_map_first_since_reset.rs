//! `BATCH_STRUCT_MAP_UPSERT_FIRST_SINCE_RESET` (0x8b): each key keeps the first
//! row since its last reset. A row replaces when its key is absent, the stored
//! row's reset field is true, or its own reset field is true. Rows fold in
//! input order, so the result depends only on the row sequence, never on how
//! it is split into batches.

use columine_types::NO_PARENT_TS_COL;
use columine_types::PROGRAM_MAGIC;
use columine_types::operand::encode_index;
use columine_types::types::{ErrorCode, STATE_HEADER_SIZE, SlotMetaOffset};
use columine_vm::bytes;
use columine_vm::state_init::{
    DEFAULT_ACCEPTED_PROGRAM_MAGICS, ProgramInvalidCause, SizingRefusal, bind_struct_map,
    calculate_state_size, init_state,
};
use columine_vm::struct_map::StructMapSlot;
use columine_vm::vm::{CheckedStructMap, Vm, u32s_as_bytes};

const OK: u32 = ErrorCode::Ok as u32;
const INVALID_PROGRAM: u32 = ErrorCode::InvalidProgram as u32;
const COLUMN_UNDERRUN: u32 = ErrorCode::ColumnUnderrun as u32;
const FIRST_SINCE_RESET: u8 = 0x8b;
const UPSERT_LAST: u8 = 0x80;
const U32: u8 = 0;
const BOOL: u8 = 3;
const ARRAY_U32: u8 = 5;
const DOWN: u32 = 0xD0_0001;
const UP: u32 = 0xD0_0002;
const UNWATCH: u32 = 0xD0_0003;
const SINCE: u8 = 0;
const REASON: u8 = 1;
const ENDED: u8 = 2;
const TAGS: u8 = 3;

fn program(num_inputs: u8, field_types: &[u8], reduce: &[u8]) -> Vec<u8> {
    let mut init = vec![
        0x18,
        0,
        6,
        8,
        0,
        u8::try_from(field_types.len()).expect("field count fits u8"),
    ];
    init.extend_from_slice(field_types);
    init.push(0);

    let mut program = vec![0u8; 32];
    program.extend(PROGRAM_MAGIC.to_le_bytes());
    program.extend([2, 0, 0, 0]); // version 2, no callbacks, no flags
    program.extend(1u32.to_le_bytes());
    program.extend(u32::from(num_inputs).to_le_bytes());
    program.extend(
        u32::try_from(init.len())
            .expect("init length fits u32")
            .to_le_bytes(),
    );
    program.extend(
        u32::try_from(reduce.len())
            .expect("reduce length fits u32")
            .to_le_bytes(),
    );
    program.extend(init);
    program.extend_from_slice(reduce);
    program
}

/// `opcode slot:0 key_col num_vals (col, field)* num_arrays (offsets, values, field)* [pick_field]`.
fn upsert(
    opcode: u8,
    key_col: u8,
    scalar_pairs: &[(u8, u8)],
    array_triples: &[(u8, u8, u8)],
    pick_field: Option<u8>,
) -> Vec<u8> {
    let mut code = vec![
        opcode,
        0,
        key_col,
        u8::try_from(scalar_pairs.len()).expect("scalar operand count fits u8"),
    ];
    for &(col, field) in scalar_pairs {
        code.extend([col, field]);
    }
    code.push(u8::try_from(array_triples.len()).expect("array operand count fits u8"));
    for &(offsets_col, values_col, field) in array_triples {
        code.extend([offsets_col, values_col, field]);
    }
    if let Some(field) = pick_field {
        code.push(field);
    }
    code
}

/// Top-level outage upsert over columns key 0, since 1, reason 2, ended 3.
fn top_level_outage() -> Vec<u8> {
    let reduce = upsert(
        FIRST_SINCE_RESET,
        0,
        &[(1, SINCE), (2, REASON), (3, ENDED)],
        &[],
        Some(ENDED),
    );
    program(4, &[U32, U32, BOOL], &reduce)
}

/// `FOR_EACH(type in {DOWN, UP})` over columns type 0, key 1, since 2,
/// reason 3, ended 4: an UNWATCH row matches no source and is skipped.
fn for_each_outage() -> Vec<u8> {
    let body = upsert(
        FIRST_SINCE_RESET,
        1,
        &[(2, SINCE), (3, REASON), (4, ENDED)],
        &[],
        Some(ENDED),
    );
    let mut reduce = vec![0xE0, 0, 2];
    reduce.extend(DOWN.to_le_bytes());
    reduce.extend(UP.to_le_bytes());
    reduce.extend(
        u16::try_from(body.len())
            .expect("body length fits u16")
            .to_le_bytes(),
    );
    reduce.extend(body);
    program(5, &[U32, U32, BOOL], &reduce)
}

fn init(program: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(program, DEFAULT_ACCEPTED_PROGRAM_MAGICS)
        .expect("valid program admission");
    let mut state = vec![0u8; usize::try_from(size).expect("state size fits usize")];
    init_state(&mut state, program, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("init state");
    state
}

fn change_flags(state: &[u8]) -> u8 {
    let offset = STATE_HEADER_SIZE + SlotMetaOffset::CHANGE_FLAGS;
    state[usize::try_from(offset).expect("metadata offset fits usize")]
}

fn clear_change_flags(state: &mut [u8]) {
    let offset = STATE_HEADER_SIZE + SlotMetaOffset::CHANGE_FLAGS;
    state[usize::try_from(offset).expect("metadata offset fits usize")] = 0;
}

/// The stored `(since, reason, ended)` of `key`, or `None` when absent. An
/// absent `ended` field reads as `None` in the third lane.
fn stored(state: &[u8], key: u32) -> Option<(u32, u32, Option<bool>)> {
    let row = CheckedStructMap::bind(state, 0)
        .expect("actual struct geometry")
        .row(key)?;
    let map = bind_struct_map(state, 0).expect("struct map slot");
    let ended = StructMapSlot::is_field_set(state, row, ENDED)
        .then(|| state[(row + map.field_offset(state, ENDED)) as usize] != 0);
    Some((
        bytes::read_u32(state, row + map.field_offset(state, SINCE)),
        bytes::read_u32(state, row + map.field_offset(state, REASON)),
        ended,
    ))
}

/// One `$unavailable` signal for service key 7: its event type, its time and
/// its reason. Only DOWN and UP feed the outage row; UP is the reset.
#[derive(Clone, Copy, Debug)]
struct Signal {
    kind: u32,
    at: u32,
    reason: u32,
}

const KEY: u32 = 7;

fn down(at: u32, reason: u32) -> Signal {
    Signal {
        kind: DOWN,
        at,
        reason,
    }
}

fn up(at: u32) -> Signal {
    Signal {
        kind: UP,
        at,
        reason: 0,
    }
}

fn unwatch(at: u32) -> Signal {
    Signal {
        kind: UNWATCH,
        at,
        reason: 0,
    }
}

/// Run `signals` as one FOR_EACH batch.
fn run_for_each(vm: &mut Vm, state: &mut [u8], program: &[u8], signals: &[Signal]) -> u32 {
    let types: Vec<u32> = signals.iter().map(|s| s.kind).collect();
    let keys = vec![KEY; signals.len()];
    let since: Vec<u32> = signals.iter().map(|s| s.at).collect();
    let reasons: Vec<u32> = signals.iter().map(|s| s.reason).collect();
    let ended: Vec<u32> = signals.iter().map(|s| u32::from(s.kind == UP)).collect();
    let cols = [
        u32s_as_bytes(&types),
        u32s_as_bytes(&keys),
        u32s_as_bytes(&since),
        u32s_as_bytes(&reasons),
        u32s_as_bytes(&ended),
    ];
    let len = u32::try_from(signals.len()).expect("batch fits u32");
    vm.execute_batch(state, program, &cols, len)
}

/// Run the DOWN/UP rows of `signals` as one top-level batch: a top-level
/// instruction sees only the rows its compiler routed to it.
fn run_top_level(vm: &mut Vm, state: &mut [u8], program: &[u8], signals: &[Signal]) -> u32 {
    let rows: Vec<Signal> = signals
        .iter()
        .copied()
        .filter(|s| s.kind != UNWATCH)
        .collect();
    let keys = vec![KEY; rows.len()];
    let since: Vec<u32> = rows.iter().map(|s| s.at).collect();
    let reasons: Vec<u32> = rows.iter().map(|s| s.reason).collect();
    let ended: Vec<u32> = rows.iter().map(|s| u32::from(s.kind == UP)).collect();
    let cols = [
        u32s_as_bytes(&keys),
        u32s_as_bytes(&since),
        u32s_as_bytes(&reasons),
        u32s_as_bytes(&ended),
    ];
    let len = u32::try_from(rows.len()).expect("batch fits u32");
    vm.execute_batch(state, program, &cols, len)
}

/// The specification as a fold: replace iff absent, stored reset, or incoming reset.
fn model(signals: &[Signal]) -> Option<(u32, u32, Option<bool>)> {
    signals
        .iter()
        .filter(|s| s.kind != UNWATCH)
        .fold(None, |current, s| {
            let ended = s.kind == UP;
            match current {
                Some((_, _, Some(false))) if !ended => current,
                _ => Some((s.at, s.reason, Some(ended))),
            }
        })
}

#[test]
fn outage_keeps_first_down_after_the_last_up_through_downs_and_unwatches() {
    let program = for_each_outage();
    let mut state = init(&program);
    let mut vm = Vm::default();

    // down, down, unwatch: the opening down holds.
    for signal in [down(1, 10), down(2, 20), unwatch(3)] {
        assert_eq!(OK, run_for_each(&mut vm, &mut state, &program, &[signal]));
        assert_eq!(Some((1, 10, Some(false))), stored(&state, KEY));
    }
    // up resets; the up row itself is stored with ended = true.
    assert_eq!(OK, run_for_each(&mut vm, &mut state, &program, &[up(4)]));
    assert_eq!(Some((4, 0, Some(true))), stored(&state, KEY));
    // The first down after the up opens the next outage; later downs keep it.
    for signal in [down(5, 50), down(6, 60), unwatch(7)] {
        assert_eq!(OK, run_for_each(&mut vm, &mut state, &program, &[signal]));
        assert_eq!(Some((5, 50, Some(false))), stored(&state, KEY));
    }
}

#[test]
fn same_batch_down_up_down_folds_in_row_order() {
    for program in [for_each_outage(), top_level_outage()] {
        let top_level = program == top_level_outage();
        let mut state = init(&program);
        let mut vm = Vm::default();
        let batch = [
            down(1, 10),
            down(2, 20),
            unwatch(3),
            up(4),
            down(5, 50),
            down(6, 60),
        ];
        let result = if top_level {
            run_top_level(&mut vm, &mut state, &program, &batch)
        } else {
            run_for_each(&mut vm, &mut state, &program, &batch)
        };
        assert_eq!(OK, result);
        assert_eq!(Some((5, 50, Some(false))), stored(&state, KEY));
    }
}

/// Every sequence of up to five DOWN/UP/UNWATCH signals gives the model's row
/// whether it arrives as one batch or one signal per batch, at top level and
/// under FOR_EACH. The fold is not associative over final rows, so this
/// parity holds because rows apply in order, not because rows merge.
#[test]
fn batch_partition_parity_matches_the_ordered_model() {
    let alphabet = [0u8, 1, 2];
    let for_each = for_each_outage();
    let top_level = top_level_outage();
    for len in 1..=5u32 {
        for code in 0..3u32.pow(len) {
            let mut digits = code;
            let signals: Vec<Signal> = (0..len)
                .map(|i| {
                    let kind = alphabet[(digits % 3) as usize];
                    digits /= 3;
                    let at = i + 1;
                    match kind {
                        0 => down(at, 100 + at),
                        1 => up(at),
                        _ => unwatch(at),
                    }
                })
                .collect();
            let expected = model(&signals);
            for (program, top) in [(&for_each, false), (&top_level, true)] {
                let run = |vm: &mut Vm, state: &mut [u8], batch: &[Signal]| {
                    if top {
                        run_top_level(vm, state, program, batch)
                    } else {
                        run_for_each(vm, state, program, batch)
                    }
                };
                let mut vm = Vm::default();
                let mut whole = init(program);
                assert_eq!(OK, run(&mut vm, &mut whole, &signals));
                let mut split = init(program);
                for signal in &signals {
                    assert_eq!(OK, run(&mut vm, &mut split, std::slice::from_ref(signal)));
                }
                assert_eq!(expected, stored(&whole, KEY), "{signals:?} top={top}");
                assert_eq!(whole, split, "{signals:?} top={top}");
            }
        }
    }
}

#[test]
fn absent_stored_reset_reads_false_and_incoming_reset_still_replaces() {
    let program = top_level_outage();
    let mut state = init(&program);
    let mut vm = Vm::default();
    // A 0x80 writer that never maps the reset field leaves it absent.
    let seed = program_with(&upsert(
        UPSERT_LAST,
        0,
        &[(1, SINCE), (2, REASON)],
        &[],
        None,
    ));
    let keys = [KEY];
    let since = [1u32];
    let reasons = [10u32];
    let seed_cols = [
        u32s_as_bytes(&keys),
        u32s_as_bytes(&since),
        u32s_as_bytes(&reasons),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &seed, &seed_cols, 1));
    assert_eq!(Some((1, 10, None)), stored(&state, KEY));

    assert_eq!(
        OK,
        run_top_level(&mut vm, &mut state, &program, &[down(2, 20)])
    );
    assert_eq!(Some((1, 10, None)), stored(&state, KEY));
    assert_eq!(OK, run_top_level(&mut vm, &mut state, &program, &[up(3)]));
    assert_eq!(Some((3, 0, Some(true))), stored(&state, KEY));
}

/// The outage layout with `reduce` as its reduce section.
fn program_with(reduce: &[u8]) -> Vec<u8> {
    program(4, &[U32, U32, BOOL], reduce)
}

/// A skipped row decides before any bitset, array, journal or change-flag
/// write; an accepted row journals and rolls back exactly.
#[test]
fn skipped_rows_mutate_nothing_and_accepted_rows_roll_back_exactly() {
    let reduce = upsert(
        FIRST_SINCE_RESET,
        0,
        &[(1, SINCE), (2, REASON), (3, ENDED)],
        &[(4, 5, TAGS)],
        Some(ENDED),
    );
    let program = program(6, &[U32, U32, BOOL, ARRAY_U32], &reduce);
    let mut state = init(&program);
    let mut vm = Vm::default();
    let run = |vm: &mut Vm, state: &mut [u8], at: u32, ended: u32, tags: [u32; 2]| {
        let keys = [KEY];
        let since = [at];
        let reasons = [at * 10];
        let ends = [ended];
        let offsets = [0u32, 2];
        let cols = [
            u32s_as_bytes(&keys),
            u32s_as_bytes(&since),
            u32s_as_bytes(&reasons),
            u32s_as_bytes(&ends),
            u32s_as_bytes(&offsets),
            u32s_as_bytes(&tags),
        ];
        vm.execute_batch_delta(state, &program, &cols, 1)
    };
    assert_eq!(OK, run(&mut vm, &mut state, 1, 0, [7, 8]));
    clear_change_flags(&mut state);

    vm.undo_enable(&state).expect("valid inactive window");
    let checkpoint = vm.undo_checkpoint();
    let opened = state.clone();
    assert_eq!(OK, run(&mut vm, &mut state, 2, 0, [90, 91]));
    assert_eq!(opened, state);
    assert_eq!(checkpoint, vm.undo_checkpoint());
    assert_eq!(0, change_flags(&state));

    assert_eq!(OK, run(&mut vm, &mut state, 3, 1, [100, 101]));
    assert_eq!(Some((3, 30, Some(true))), stored(&state, KEY));
    assert!(vm.undo_checkpoint() > checkpoint);
    assert_ne!(0, change_flags(&state));
    assert_exported_delta_replays(&mut vm, &opened, &state, checkpoint);
    vm.undo_rollback(&mut state, checkpoint)
        .expect("rollback to the opened outage");
    assert_eq!(opened, state);
}

/// Export `[checkpoint, end)` and replay both lanes byte-exact on a fresh VM.
fn assert_exported_delta_replays(vm: &mut Vm, before: &[u8], after: &[u8], checkpoint: u32) {
    let end = vm.undo_checkpoint();
    assert!(!vm.undo_status().overflow, "journal must stay exact");
    assert_eq!(
        Ok(end - checkpoint),
        vm.delta_export_segment(checkpoint, end)
    );
    let undo = vm.delta_export_undo().to_vec();
    let redo = vm.delta_export_redo().to_vec();
    let size = u32::try_from(before.len()).expect("state size fits u32");
    let entry_size = Vm::delta_export_entry_size();
    let mut replay = Vm::default();

    let mut rolled_back = after.to_vec();
    assert_eq!(
        Ok(size),
        replay.delta_apply_rollback_segment(&mut rolled_back, &undo, entry_size)
    );
    assert_eq!(rolled_back, before, "exported undo must restore the image");

    let mut rolled_forward = before.to_vec();
    assert_eq!(
        Ok(size),
        replay.delta_apply_rollforward_segment(&mut rolled_forward, &redo, entry_size)
    );
    assert_eq!(
        rolled_forward, after,
        "exported redo must reproduce the image"
    );
}

#[test]
fn flat_map_body_length_includes_the_reset_ordinal() {
    let body = upsert(
        FIRST_SINCE_RESET,
        2,
        &[(3, SINCE), (4, REASON), (5, ENDED)],
        &[],
        Some(ENDED),
    );
    let mut flat = vec![0xE1, 1];
    flat.extend(encode_index(NO_PARENT_TS_COL).as_bytes());
    flat.extend(
        u16::try_from(body.len())
            .expect("inner body length fits u16")
            .to_le_bytes(),
    );
    flat.extend(body);
    let mut reduce = vec![0xE0, 0, 1];
    reduce.extend(DOWN.to_le_bytes());
    reduce.extend(
        u16::try_from(flat.len())
            .expect("body length fits u16")
            .to_le_bytes(),
    );
    reduce.extend(flat);
    let program = program(6, &[U32, U32, BOOL], &reduce);
    let mut state = init(&program);
    let mut vm = Vm::default();

    let types = [DOWN];
    let offsets = [0u32, 4];
    let keys = [KEY; 4];
    let since = [1u32, 2, 3, 4];
    let reasons = [10u32, 20, 30, 40];
    let ended = [0u32, 1, 0, 0];
    let cols = [
        u32s_as_bytes(&types),
        u32s_as_bytes(&offsets),
        u32s_as_bytes(&keys),
        u32s_as_bytes(&since),
        u32s_as_bytes(&reasons),
        u32s_as_bytes(&ended),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &program, &cols, 1));
    assert_eq!(Some((3, 30, Some(false))), stored(&state, KEY));
}

/// Execute against a state initialised from `layout`: the refusal leaves the
/// state and the journal untouched.
fn assert_invalid_without_mutation(layout: &[u8], program: &[u8]) {
    let mut state = init(layout);
    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("valid inactive window");
    let checkpoint = vm.undo_checkpoint();
    let before = state.clone();
    assert_eq!(
        INVALID_PROGRAM,
        run_top_level(&mut vm, &mut state, program, &[down(1, 10)])
    );
    assert_eq!(before, state);
    assert_eq!(checkpoint, vm.undo_checkpoint());
}

#[test]
fn reset_lane_must_be_a_mapped_bool_field() {
    // The row writer stores a field's last mapping; a reset lane bound to an
    // earlier duplicate would decide on a cell the row does not keep.
    let duplicate = program(
        5,
        &[U32, U32, BOOL],
        &upsert(
            FIRST_SINCE_RESET,
            0,
            &[(1, SINCE), (2, REASON), (3, ENDED), (4, ENDED)],
            &[],
            Some(ENDED),
        ),
    );
    assert_invalid_without_mutation(&duplicate, &duplicate);

    let unmapped = program_with(&upsert(
        FIRST_SINCE_RESET,
        0,
        &[(1, SINCE), (2, REASON)],
        &[],
        Some(ENDED),
    ));
    assert_invalid_without_mutation(&unmapped, &unmapped);

    let out_of_range = program_with(&upsert(
        FIRST_SINCE_RESET,
        0,
        &[(1, SINCE), (2, REASON), (3, ENDED)],
        &[],
        Some(3),
    ));
    assert_invalid_without_mutation(&out_of_range, &out_of_range);

    let not_bool = program(
        4,
        &[U32, U32, U32],
        &upsert(
            FIRST_SINCE_RESET,
            0,
            &[(1, SINCE), (2, REASON), (3, ENDED)],
            &[],
            Some(ENDED),
        ),
    );
    assert_invalid_without_mutation(&not_bool, &not_bool);

    let mut truncated_reduce = upsert(
        FIRST_SINCE_RESET,
        0,
        &[(1, SINCE), (2, REASON), (3, ENDED)],
        &[],
        Some(ENDED),
    );
    truncated_reduce.pop();
    let truncated = program_with(&truncated_reduce);
    assert_eq!(
        calculate_state_size(&truncated, DEFAULT_ACCEPTED_PROGRAM_MAGICS),
        Err(SizingRefusal::Invalid(
            ProgramInvalidCause::MalformedProgram
        ))
    );
    assert_invalid_without_mutation(&program_with(&[]), &truncated);
}

#[test]
fn for_each_refuses_unmapped_duplicate_non_bool_and_out_of_range_reset_lanes_without_mutation() {
    let block = |field_types: &[u8], pairs: &[(u8, u8)], reset: u8| {
        let body = upsert(FIRST_SINCE_RESET, 1, pairs, &[], Some(reset));
        let mut reduce = vec![0xE0, 0, 1];
        reduce.extend(DOWN.to_le_bytes());
        reduce.extend(
            u16::try_from(body.len())
                .expect("body length fits u16")
                .to_le_bytes(),
        );
        reduce.extend(body);
        program(5, field_types, &reduce)
    };
    for program in [
        block(&[U32, U32, BOOL], &[(2, SINCE), (3, REASON)], ENDED),
        block(
            &[U32, U32, BOOL],
            &[(2, SINCE), (3, ENDED), (4, ENDED)],
            ENDED,
        ),
        block(
            &[U32, U32, U32],
            &[(2, SINCE), (3, REASON), (4, ENDED)],
            ENDED,
        ),
        block(&[U32, U32, BOOL], &[(2, SINCE), (3, REASON), (4, ENDED)], 3),
    ] {
        let mut state = init(&program);
        let mut vm = Vm::default();
        vm.undo_enable(&state).expect("valid inactive window");
        let checkpoint = vm.undo_checkpoint();
        let before = state.clone();
        assert_eq!(
            INVALID_PROGRAM,
            run_for_each(&mut vm, &mut state, &program, &[down(1, 10)])
        );
        assert_eq!(before, state);
        assert_eq!(checkpoint, vm.undo_checkpoint());
    }
}

#[test]
fn short_reset_column_is_an_underrun_before_any_write() {
    let program = top_level_outage();
    let mut state = init(&program);
    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("valid inactive window");
    let checkpoint = vm.undo_checkpoint();
    let before = state.clone();
    let keys = [KEY];
    let since = [1u32];
    let reasons = [10u32];
    let cols = [
        u32s_as_bytes(&keys),
        u32s_as_bytes(&since),
        u32s_as_bytes(&reasons),
        &[][..],
    ];
    assert_eq!(
        COLUMN_UNDERRUN,
        vm.execute_batch(&mut state, &program, &cols, 1)
    );
    assert_eq!(before, state);
    assert_eq!(checkpoint, vm.undo_checkpoint());
}
