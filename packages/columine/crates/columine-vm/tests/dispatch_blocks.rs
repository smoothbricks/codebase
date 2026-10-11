//! Dispatch-block tests cover mixed FOR_EACH bodies, nested FLAT_MAP, parent
//! timestamp inheritance, ORDERED_LIST, struct-map iteration/growth, and array
//! fields. Scenarios pin the corresponding ABI behavior.

use columine_types::NO_PARENT_TS_COL;
use columine_types::PROGRAM_MAGIC;
use columine_types::operand::encode_index;
use columine_types::types::{
    AggType, ErrorCode, Opcode, SLOT_META_SIZE, STATE_HEADER_SIZE, SlotMetaOffset, SlotType, align8,
};
use columine_vm::bytes;
use columine_vm::state_init::{
    ARENA_HEADER_SIZE, DEFAULT_ACCEPTED_PROGRAM_MAGICS, ProgramInvalidCause, SizingRefusal,
    calculate_grown_state_size, calculate_state_size, grow_state, init_state, struct_field_offset,
};
use columine_vm::vm::{
    CheckedStructMap, UNDO_POSITION_OVERFLOWED, Vm, f64s_as_bytes, u32s_as_bytes,
};

const OK: u32 = ErrorCode::Ok as u32;
const INVALID_PROGRAM: u32 = ErrorCode::InvalidProgram as u32;
const NEEDS_GROWTH: u32 = ErrorCode::NeedsGrowth as u32;
const TYPE_A: u32 = 1001;

fn program(num_slots: u8, num_inputs: u8, init: &[u8], reduce: &[u8]) -> Vec<u8> {
    let init_len = init.len() + 1;
    let mut prog = vec![0u8; 32];
    prog.extend(PROGRAM_MAGIC.to_le_bytes());
    prog.extend([2, 0, 0, 0]); // version 2, no callbacks, no flags
    prog.extend(u32::from(num_slots).to_le_bytes());
    prog.extend(u32::from(num_inputs).to_le_bytes());
    prog.extend((init_len as u32).to_le_bytes());
    prog.extend((reduce.len() as u32).to_le_bytes());
    prog.extend_from_slice(init);
    prog.push(Opcode::Halt as u8);
    prog.extend_from_slice(reduce);
    prog
}

fn for_each(type_id: u32, body: &[u8]) -> Vec<u8> {
    let mut reduce = vec![0xE0, 0, 1];
    reduce.extend(type_id.to_le_bytes());
    reduce.extend((body.len() as u16).to_le_bytes());
    reduce.extend_from_slice(body);
    reduce
}

fn flat_map(offsets_col: u8, parent_ts_col: u32, inner: &[u8]) -> Vec<u8> {
    let mut fm = vec![0xE1, offsets_col];
    fm.extend(encode_index(parent_ts_col).as_bytes());
    fm.extend((inner.len() as u16).to_le_bytes());
    fm.extend_from_slice(inner);
    fm
}

fn init(prog: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(
        prog,
        columine_vm::state_init::DEFAULT_ACCEPTED_PROGRAM_MAGICS,
    )
    .expect("valid program admission");
    assert!(size > 0);
    let mut state = vec![0u8; size as usize];
    init_state(
        &mut state,
        prog,
        columine_vm::state_init::DEFAULT_ACCEPTED_PROGRAM_MAGICS,
    )
    .expect("init_state");
    state
}

fn assert_delta_roundtrip(vm: &mut Vm, before: &[u8], after: &[u8], checkpoint: u32) {
    let end = vm.undo_checkpoint();
    assert!(
        end > checkpoint,
        "mutation must emit paired journal entries"
    );
    assert_eq!(
        Ok(end - checkpoint),
        vm.delta_export_segment(checkpoint, end)
    );
    let undo = vm.delta_export_undo().to_vec();
    let redo = vm.delta_export_redo().to_vec();

    let mut production = after.to_vec();
    vm.undo_rollback(&mut production, checkpoint)
        .expect("valid checkpoint");
    assert_eq!(production, before);

    let mut replay = Vm::default();
    let mut rolled_back = after.to_vec();
    replay
        .delta_apply_rollback_segment(&mut rolled_back, &undo, Vm::delta_export_entry_size())
        .expect("valid undo segment");
    assert_eq!(rolled_back, before);

    let mut rolled_forward = before.to_vec();
    replay
        .delta_apply_rollforward_segment(&mut rolled_forward, &redo, Vm::delta_export_entry_size())
        .expect("valid redo segment");
    assert_eq!(rolled_forward, after);
}

fn meta_u32(state: &[u8], slot: u8, off: u32) -> u32 {
    bytes::read_u32(
        state,
        STATE_HEADER_SIZE + u32::from(slot) * SLOT_META_SIZE + off,
    )
}

fn slot_offset(state: &[u8], slot: u8) -> u32 {
    meta_u32(state, slot, SlotMetaOffset::OFFSET)
}

fn slot_size(state: &[u8], slot: u8) -> u32 {
    meta_u32(state, slot, SlotMetaOffset::SIZE)
}

fn slot_cap(state: &[u8], slot: u8) -> u32 {
    meta_u32(state, slot, SlotMetaOffset::CAPACITY)
}

fn struct_map_row(state: &[u8], slot: u8, key: u32) -> Option<u32> {
    CheckedStructMap::bind(state, u32::from(slot))
        .expect("actual struct geometry")
        .row(key)
}

#[test]
fn unknown_and_truncated_block_opcodes_reject_before_mutation_or_undo() {
    let init_code = [
        Opcode::SlotOrderedList as u8,
        0,
        SlotType::OrderedList as u8,
        8,
        0,
        0,
    ];
    let types = [TYPE_A];
    let values = [42u32];
    let cols: Vec<&[u8]> = vec![u32s_as_bytes(&types), u32s_as_bytes(&values)];

    // The state comes from the same declarations with no reduce code: admission
    // refuses the malformed program before any state exists, and execution of
    // it against a valid state still refuses before mutation or journaling.
    let layout = init(&program(1, 2, &init_code, &[]));
    for body in [
        vec![Opcode::ListAppend as u8, 0, 1, 0xfe],
        vec![Opcode::ListAppend as u8, 0],
    ] {
        let prog = program(1, 2, &init_code, &for_each(TYPE_A, &body));
        assert_eq!(
            calculate_state_size(&prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS),
            Err(SizingRefusal::Invalid(
                ProgramInvalidCause::MalformedProgram
            ))
        );
        let mut state = layout.clone();
        let before = state.clone();
        let mut vm = Vm::default();
        vm.undo_enable(&state).expect("undo window opens");
        let checkpoint = vm.undo_checkpoint();

        assert_eq!(
            INVALID_PROGRAM,
            vm.execute_batch_delta(&mut state, &prog, &cols, 1)
        );
        assert_eq!(state, before);
        assert_eq!(vm.undo_checkpoint(), checkpoint);
    }
}

#[test]
fn truncated_top_level_opcode_rejects_before_mutation_or_undo() {
    let init_code = [
        Opcode::SlotOrderedList as u8,
        0,
        SlotType::OrderedList as u8,
        8,
        0,
        0,
    ];
    let prog = program(1, 0, &init_code, &[Opcode::BatchMapUpsertLatest as u8, 0]);
    assert_eq!(
        calculate_state_size(&prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS),
        Err(SizingRefusal::Invalid(
            ProgramInvalidCause::MalformedProgram
        ))
    );
    let mut state = init(&program(1, 0, &init_code, &[]));
    let before = state.clone();
    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("undo window opens");
    let checkpoint = vm.undo_checkpoint();

    assert_eq!(
        INVALID_PROGRAM,
        vm.execute_batch_delta(&mut state, &prog, &[], 0)
    );
    assert_eq!(state, before);
    assert_eq!(vm.undo_checkpoint(), checkpoint);
}

#[test]
fn unknown_block_scalar_subtype_rejects_before_mutation_or_undo() {
    let init_code = [
        Opcode::SlotDef as u8,
        0,
        SlotType::Scalar as u8,
        AggType::ScalarF64 as u8,
        0,
    ];
    let body = [Opcode::BatchScalarLatest as u8, 0, 1, 2];
    let prog = program(1, 3, &init_code, &for_each(TYPE_A, &body));
    let mut state = init(&prog);
    state[(STATE_HEADER_SIZE + SlotMetaOffset::AGG_TYPE) as usize] = 7;
    let before = state.clone();
    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("undo window opens");
    let checkpoint = vm.undo_checkpoint();

    let types = [TYPE_A];
    let values = [42.5f64];
    let timestamps = [100.0f64];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        f64s_as_bytes(&values),
        f64s_as_bytes(&timestamps),
    ];
    assert_eq!(
        INVALID_PROGRAM,
        vm.execute_batch_delta(&mut state, &prog, &cols, 1)
    );
    assert_eq!(state, before);
    assert_eq!(vm.undo_checkpoint(), checkpoint);
}
// =============================================================================
// FLAT_MAP plus event-level AGG_SUM

#[test]
fn flat_map_plus_event_level_agg_sum_in_same_for_each_block() {
    // Slot 0 is STRUCT_MAP and slot 1 explicitly declares AGGREGATE SUM.
    let mut init_sec = vec![0x18, 0, 6, 4, 0, 2, 0, 4];
    init_sec.extend([0x10, 1, 2, 1, 0]);

    let inner = [0x80u8, 0, 3, 2, 4, 0, 5, 1, 0];
    let mut body = vec![0x40, 1, 2]; // AGG_SUM slot=1 val_col=2
    body.extend(flat_map(1, NO_PARENT_TS_COL, &inner));
    let prog = program(2, 6, &init_sec, &for_each(TYPE_A, &body));

    let mut state = init(&prog);
    let mut vm = Vm::default();

    let types = [TYPE_A, TYPE_A];
    let offsets = [0u32, 2, 3];
    let amounts = [100.0f64, 250.0];
    let child_keys = [10u32, 20, 30];
    let child_v0 = [1u32, 2, 3];
    let child_v1 = [11u32, 22, 33];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&offsets),
        f64s_as_bytes(&amounts),
        u32s_as_bytes(&child_keys),
        u32s_as_bytes(&child_v0),
        u32s_as_bytes(&child_v1),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 2));

    assert_eq!(3, slot_size(&state, 0));
    let agg_off = slot_offset(&state, 1);
    let agg_val = f64::from_bits(bytes::read_u64(&state, agg_off));
    assert!((agg_val - 350.0).abs() < 0.001);
}

// =============================================================================
// Nested FLAT_MAP depth-2 ()
// =============================================================================

#[test]
fn nested_flat_map_depth_2_groups_items_struct_map() {
    let init_sec = [0x18u8, 0, 0x06, 4, 0, 2, 0, 4];
    let leaf = [0x80u8, 0, 3, 2, 4, 0, 5, 1, 0];
    let inner_fm = flat_map(2, NO_PARENT_TS_COL, &leaf);
    let outer_fm = flat_map(1, NO_PARENT_TS_COL, &inner_fm);
    let prog = program(1, 6, &init_sec, &for_each(TYPE_A, &outer_fm));

    let mut state = init(&prog);
    let mut vm = Vm::default();

    let types = [TYPE_A];
    let outer_offsets = [0u32, 2];
    let inner_offsets = [0u32, 2, 3];
    let leaf_keys = [100u32, 200, 300];
    let leaf_v0 = [10u32, 20, 30];
    let leaf_v1 = [1001u32, 1002, 1003];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&outer_offsets),
        u32s_as_bytes(&inner_offsets),
        u32s_as_bytes(&leaf_keys),
        u32s_as_bytes(&leaf_v0),
        u32s_as_bytes(&leaf_v1),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 1));

    assert_eq!(3, slot_size(&state, 0));
    for (key, v0) in [(100u32, 10u32), (200, 20), (300, 30)] {
        let row = struct_map_row(&state, 0, key).expect("inserted row");
        assert_eq!(v0, bytes::read_u32(&state, row + 1));
    }
}

// =============================================================================
// FLAT_MAP LATEST — parent timestamp overrides child ts_col ()
// =============================================================================

#[test]
fn flat_map_latest_parent_timestamp_overrides_child_ts_col() {
    let init_sec = [0x10u8, 0, 0, 8, 0]; // HASHMAP cap 16, with timestamps
    let inner = [0x20u8, 0, 3, 4, 5, 1]; // MAP_UPSERT_LATEST, cmp_type=f64
    let fm = flat_map(1, 2, &inner); // parent_ts_col = 2!
    let prog = program(1, 6, &init_sec, &for_each(TYPE_A, &fm));

    let mut state = init(&prog);
    let mut vm = Vm::default();

    let types = [TYPE_A, TYPE_A];
    let offsets = [0u32, 1, 2];
    let parent_ts = [100.0f64, 200.0];
    let child_keys = [42u32, 42];
    let child_vals = [10u32, 20];
    let child_ts = [999.0f64, 1.0]; // ignored: parent_ts_col overrides
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&offsets),
        f64s_as_bytes(&parent_ts),
        u32s_as_bytes(&child_keys),
        u32s_as_bytes(&child_vals),
        f64s_as_bytes(&child_ts),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 2));

    // Parent 1 (ts 200) wins → val 20. Child-ts would have picked val 10.
    let off = slot_offset(&state, 0);
    let cap = slot_cap(&state, 0);
    let mut found_val = None;
    for pos in 0..cap {
        if bytes::read_u32(&state, off + pos * 4) == 42 {
            found_val = Some(bytes::read_u32(&state, off + cap * 4 + pos * 4));
            break;
        }
    }
    assert_eq!(Some(20), found_val);
}

// =============================================================================
// ORDERED_LIST ()
// =============================================================================

///  `buildScalarListTestProgram`.
fn build_scalar_list_program(type_id: u32) -> Vec<u8> {
    let init_sec = [0x19u8, 0, 7, 8, 0, 0]; // SLOT_ORDERED_LIST, elem UINT32
    let body = [0x84u8, 0, 1];
    program(1, 2, &init_sec, &for_each(type_id, &body))
}

#[test]
fn ordered_list_scalar_append() {
    let prog = build_scalar_list_program(TYPE_A);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let types = [TYPE_A; 3];
    let vals = [100u32, 200, 300];
    let cols: Vec<&[u8]> = vec![u32s_as_bytes(&types), u32s_as_bytes(&vals)];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 3));

    assert_eq!(3, slot_size(&state, 0));
    let off = slot_offset(&state, 0);
    assert_eq!(100, bytes::read_u32(&state, off));
    assert_eq!(200, bytes::read_u32(&state, off + 4));
    assert_eq!(300, bytes::read_u32(&state, off + 8));
}

#[test]
fn ordered_list_scalar_growth_triggers_needs_growth() {
    let prog = build_scalar_list_program(TYPE_A);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let capacity = slot_cap(&state, 0);
    let types: Vec<u32> = vec![TYPE_A; capacity as usize];
    let vals: Vec<u32> = (1..=capacity).collect();
    let cols: Vec<&[u8]> = vec![u32s_as_bytes(&types), u32s_as_bytes(&vals)];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, capacity));

    let t2 = [TYPE_A];
    let v2 = [999u32];
    let cols2: Vec<&[u8]> = vec![u32s_as_bytes(&t2), u32s_as_bytes(&v2)];
    assert_eq!(NEEDS_GROWTH, vm.execute_batch(&mut state, &prog, &cols2, 1));
}

#[test]
fn ordered_list_struct_append() {
    //  `buildStructListTestProgram` — ORDERED_LIST with
    // elem_type=0xFF (struct) + 2 fields.
    let init_sec = [0x19u8, 0, 7, 8, 0, 0xFF, 2, 0, 4];
    let body = [0x85u8, 0, 2, 1, 0, 2, 1];
    let prog = program(1, 3, &init_sec, &for_each(TYPE_A, &body));

    let mut state = init(&prog);
    let mut vm = Vm::default();

    let types = [TYPE_A, TYPE_A];
    let v0 = [42u32, 99];
    let v1 = [1001u32, 2002];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&v0),
        u32s_as_bytes(&v1),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 2));

    assert_eq!(2, slot_size(&state, 0));
    let meta_base = STATE_HEADER_SIZE;
    let off = slot_offset(&state, 0);
    let num_fields = u32::from(state[(meta_base + SlotMetaOffset::AGG_TYPE) as usize]);
    let row_size = u32::from(bytes::read_u16(
        &state,
        meta_base + SlotMetaOffset::TTL_SECONDS,
    ));
    let rows_base = off + align8(num_fields);

    assert_eq!(0x03, state[rows_base as usize]);
    assert_eq!(42, bytes::read_u32(&state, rows_base + 1));
    assert_eq!(1001, bytes::read_u32(&state, rows_base + 5));
    let row1 = rows_base + row_size;
    assert_eq!(99, bytes::read_u32(&state, row1 + 1));
    assert_eq!(2002, bytes::read_u32(&state, row1 + 5));
}

#[test]
fn ordered_list_scalar_payload_delta_roundtrip() {
    let prog = build_scalar_list_program(TYPE_A);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let types = [TYPE_A, TYPE_A];
    let vals = [100u32, 200];
    let cols: Vec<&[u8]> = vec![u32s_as_bytes(&types), u32s_as_bytes(&vals)];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 2));
    let before = state.clone();

    vm.undo_enable(&state).expect("undo window opens");
    let checkpoint = vm.undo_checkpoint();
    let next_types = [TYPE_A];
    let next_values = [0xaabb_ccddu32];
    let next_cols: Vec<&[u8]> = vec![u32s_as_bytes(&next_types), u32s_as_bytes(&next_values)];
    assert_eq!(OK, vm.execute_batch_delta(&mut state, &prog, &next_cols, 1),);
    assert_eq!(3, slot_size(&state, 0));
    assert_eq!(
        0xaabb_ccdd,
        bytes::read_u32(&state, slot_offset(&state, 0) + 8),
    );

    let after = state.clone();
    assert_delta_roundtrip(&mut vm, &before, &after, checkpoint);
}

#[test]
fn ordered_list_struct_payload_delta_roundtrip() {
    let init_sec = [0x19u8, 0, 7, 8, 0, 0xFF, 2, 0, 4];
    let body = [0x85u8, 0, 2, 1, 0, 2, 1];
    let prog = program(1, 3, &init_sec, &for_each(TYPE_A, &body));
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let first_types = [TYPE_A];
    let first_a = [0x1122_3344u32];
    let first_b = [0x5566_7788u32];
    let first_cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&first_types),
        u32s_as_bytes(&first_a),
        u32s_as_bytes(&first_b),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &first_cols, 1));
    let before = state.clone();

    vm.undo_enable(&state).expect("undo window opens");
    let checkpoint = vm.undo_checkpoint();
    let second_types = [TYPE_A];
    let second_a = [0x99aa_bbccu32];
    let second_b = [0xddee_ff00u32];
    let second_cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&second_types),
        u32s_as_bytes(&second_a),
        u32s_as_bytes(&second_b),
    ];
    assert_eq!(
        OK,
        vm.execute_batch_delta(&mut state, &prog, &second_cols, 1),
    );

    let meta_base = STATE_HEADER_SIZE;
    let num_fields = u32::from(state[(meta_base + SlotMetaOffset::AGG_TYPE) as usize]);
    let row_size = u32::from(bytes::read_u16(
        &state,
        meta_base + SlotMetaOffset::TTL_SECONDS,
    ));
    let second_row = slot_offset(&state, 0) + align8(num_fields) + row_size;
    assert_eq!(0x03, state[second_row as usize]);
    assert_eq!(0x99aa_bbcc, bytes::read_u32(&state, second_row + 1));
    assert_eq!(0xddee_ff00, bytes::read_u32(&state, second_row + 5));

    let after = state.clone();
    assert_delta_roundtrip(&mut vm, &before, &after, checkpoint);
}

#[test]
fn ordered_list_inside_flat_map() {
    let init_sec = [0x19u8, 0, 7, 8, 0, 0]; // scalar UINT32 ordered list
    let inner = [0x84u8, 0, 2];
    let fm = flat_map(1, NO_PARENT_TS_COL, &inner);
    let prog = program(1, 3, &init_sec, &for_each(TYPE_A, &fm));

    let mut state = init(&prog);
    let mut vm = Vm::default();
    let before = state.clone();
    vm.undo_enable(&state).expect("undo window opens");
    let checkpoint = vm.undo_checkpoint();

    let types = [TYPE_A, TYPE_A];
    let offsets = [0u32, 2, 3];
    let child_vals = [10u32, 20, 30];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&offsets),
        u32s_as_bytes(&child_vals),
    ];
    assert_eq!(OK, vm.execute_batch_delta(&mut state, &prog, &cols, 2),);

    assert_eq!(3, slot_size(&state, 0));
    let off = slot_offset(&state, 0);
    assert_eq!(10, bytes::read_u32(&state, off));
    assert_eq!(20, bytes::read_u32(&state, off + 4));
    assert_eq!(30, bytes::read_u32(&state, off + 8));
    let after = state.clone();
    assert_delta_roundtrip(&mut vm, &before, &after, checkpoint);
}

#[test]
fn ordered_list_scalar_stale_dead_tail_rolls_back_to_full_image() {
    let prog = build_scalar_list_program(TYPE_A);
    let mut state = init(&prog);
    let mut vm = Vm::default();
    let types = [TYPE_A, TYPE_A];
    let vals = [100u32, 200];
    let cols: Vec<&[u8]> = vec![u32s_as_bytes(&types), u32s_as_bytes(&vals)];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 2));

    // Dead tail: allocated capacity past the live size holds stale nonzero
    // values, including the cell the next append overwrites.
    let off = slot_offset(&state, 0);
    for index in [2u32, 5] {
        bytes::write_u32(&mut state, off + index * 4, 0xCAFE_0000 | index);
    }
    let before = state.clone();

    vm.undo_enable(&state).expect("undo window opens");
    let checkpoint = vm.undo_checkpoint();
    let next_types = [TYPE_A];
    let next_values = [0xaabb_ccddu32];
    let next_cols: Vec<&[u8]> = vec![u32s_as_bytes(&next_types), u32s_as_bytes(&next_values)];
    assert_eq!(OK, vm.execute_batch_delta(&mut state, &prog, &next_cols, 1));
    assert_eq!(3, slot_size(&state, 0));
    assert_eq!(0xaabb_ccdd, bytes::read_u32(&state, off + 8));
    assert_eq!(
        0xCAFE_0005,
        bytes::read_u32(&state, off + 20),
        "an untouched dead tail survives the append"
    );

    let after = state.clone();
    assert_delta_roundtrip(&mut vm, &before, &after, checkpoint);
}

// =============================================================================
// Struct map iteration + growth through dispatch (, 1091)
// =============================================================================

fn build_struct_map_program() -> Vec<u8> {
    let init_sec = [0x18u8, 0, 6, 4, 0, 2, 0, 4];
    let reduce = [0x80u8, 0, 0, 2, 1, 0, 2, 1, 0];
    program(1, 3, &init_sec, &reduce)
}

#[test]
fn struct_map_iteration() {
    let prog = build_struct_map_program();
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let keys = [100u32, 200, 300];
    let vals = [10u32, 20, 30];
    let strs = [1u32, 2, 3];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&keys),
        u32s_as_bytes(&vals),
        u32s_as_bytes(&strs),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 3));

    let view = CheckedStructMap::bind(&state, 0).expect("actual struct geometry");

    let mut found = Vec::new();
    let mut pos = view.iter_start();
    while pos < view.capacity() {
        found.push(view.iter_key(pos).expect("live iteration cell"));
        pos = view.iter_next(pos).expect("valid iterator");
    }
    assert_eq!(3, found.len());
    for key in [100, 200, 300] {
        assert!(found.contains(&key), "missing key {key}");
    }
}

#[test]
fn struct_map_growth_preserves_entries() {
    let prog = build_struct_map_program();
    let mut state = init(&prog);
    let mut vm = Vm::default();

    for i in 0..11u32 {
        let keys = [i + 1000];
        let vals = [i * 10];
        let strs = [i + 5000];
        let cols: Vec<&[u8]> = vec![
            u32s_as_bytes(&keys),
            u32s_as_bytes(&vals),
            u32s_as_bytes(&strs),
        ];
        assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 1));
    }

    let keys12 = [2000u32];
    let vals12 = [999u32];
    let strs12 = [9999u32];
    let cols12: Vec<&[u8]> = vec![
        u32s_as_bytes(&keys12),
        u32s_as_bytes(&vals12),
        u32s_as_bytes(&strs12),
    ];
    assert_eq!(
        NEEDS_GROWTH,
        vm.execute_batch(&mut state, &prog, &cols12, 1)
    );

    let grown_size = calculate_grown_state_size(&state, 0).expect("valid growth geometry");
    let mut new_state = vec![0u8; grown_size as usize];
    grow_state(&state, &mut new_state, 0).expect("grow_state");

    assert_eq!(32, slot_cap(&new_state, 0));
    for i in 0..11u32 {
        let row = struct_map_row(&new_state, 0, i + 1000).expect("grown committed row");
        assert_eq!(i * 10, bytes::read_u32(&new_state, row + 1));
        assert_eq!(i + 5000, bytes::read_u32(&new_state, row + 5));
    }

    assert_eq!(OK, vm.execute_batch(&mut new_state, &prog, &cols12, 1));
    let row12 = struct_map_row(&new_state, 0, 2000).expect("new row after growth");
    assert_eq!(999, bytes::read_u32(&new_state, row12 + 1));
}

// =============================================================================
// Array fields in struct-map rows ()
// =============================================================================

///  `buildArrayFieldTestProgram`.
fn build_array_field_program(type_id: u32) -> Vec<u8> {
    let init_sec = [0x18u8, 0, 6, 4, 0, 2, 0, 5]; // UINT32 + ARRAY_U32
    let body = [0x80u8, 0, 1, 1, 2, 0, 1, 3, 4, 1];
    program(1, 5, &init_sec, &for_each(type_id, &body))
}

fn build_top_level_array_field_program() -> Vec<u8> {
    let init_sec = [0x18u8, 0, 6, 4, 0, 2, 0, 5]; // UINT32 + ARRAY_U32
    let reduce = [0x80u8, 0, 0, 1, 1, 0, 1, 2, 3, 1];
    program(1, 4, &init_sec, &reduce)
}

struct ArrayRow {
    scalar: u32,
    arena_offset: u32,
    len: u32,
}

fn read_array_row(state: &[u8], key: u32) -> ArrayRow {
    let meta_base = STATE_HEADER_SIZE;
    let off = slot_offset(state, 0);
    let num_fields = state[(meta_base + SlotMetaOffset::AGG_TYPE) as usize];
    let descriptor = &state[off as usize..(off + u32::from(num_fields)) as usize];
    let row = struct_map_row(state, 0, key).expect("array row must exist");
    assert_ne!(0, state[row as usize] & (1 << 1), "array field is unset");
    let scalar_off = struct_field_offset(num_fields, descriptor, 0);
    let arr_off = struct_field_offset(num_fields, descriptor, 1);
    ArrayRow {
        scalar: bytes::read_u32(state, row + scalar_off),
        arena_offset: bytes::read_u32(state, row + arr_off),
        len: bytes::read_u32(state, row + arr_off + 4),
    }
}

fn arena_hdr(state: &[u8]) -> u32 {
    bytes::read_u32(state, STATE_HEADER_SIZE + SlotMetaOffset::GRACE_SECONDS)
}

#[test]
fn top_level_struct_map_materializes_array_fields() {
    let prog = build_top_level_array_field_program();
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let keys = [100u32, 200];
    let scalars = [42u32, 99];
    let offsets = [0u32, 3, 5];
    let values = [10u32, 20, 30, 40, 50];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&keys),
        u32s_as_bytes(&scalars),
        u32s_as_bytes(&offsets),
        u32s_as_bytes(&values),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 2));

    let hdr = arena_hdr(&state);
    assert_eq!(20, bytes::read_u32(&state, hdr + 4));
    let arena_data = hdr + ARENA_HEADER_SIZE;
    for (key, scalar, expected) in [(100, 42, &[10u32, 20, 30][..]), (200, 99, &[40u32, 50][..])] {
        let row = read_array_row(&state, key);
        assert_eq!(scalar, row.scalar);
        assert_eq!(expected.len() as u32, row.len);
        for (index, value) in expected.iter().enumerate() {
            assert_eq!(
                *value,
                bytes::read_u32(&state, arena_data + row.arena_offset + index as u32 * 4,),
            );
        }
    }
}

#[test]
fn top_level_struct_map_array_overflow_requests_slot_growth() {
    let prog = build_top_level_array_field_program();
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let keys = [100u32];
    let scalars = [42u32];
    let arena_capacity = bytes::read_u32(&state, arena_hdr(&state));
    let value_count =
        usize::try_from(arena_capacity).expect("arena capacity") / size_of::<u32>() + 1;
    let offsets = [0u32, u32::try_from(value_count).expect("array length")];
    let values = vec![7u32; value_count];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&keys),
        u32s_as_bytes(&scalars),
        u32s_as_bytes(&offsets),
        u32s_as_bytes(&values),
    ];
    assert_eq!(NEEDS_GROWTH, vm.execute_batch(&mut state, &prog, &cols, 1),);
    assert_eq!(Some(0), vm.needs_growth_slot());
}

#[test]
fn struct_map_array_field_basic_write_and_read_back() {
    let prog = build_array_field_program(TYPE_A);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let types = [TYPE_A, TYPE_A];
    let keys = [100u32, 200];
    let scalars = [42u32, 99];
    let offsets = [0u32, 3, 5];
    let values = [10u32, 20, 30, 40, 50];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&keys),
        u32s_as_bytes(&scalars),
        u32s_as_bytes(&offsets),
        u32s_as_bytes(&values),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 2));

    assert_eq!(2, slot_size(&state, 0));
    let hdr = arena_hdr(&state);
    assert_ne!(0, hdr);
    assert_eq!(20, bytes::read_u32(&state, hdr + 4)); // 5 elems × 4 bytes

    let arena_data = hdr + ARENA_HEADER_SIZE;
    let r100 = read_array_row(&state, 100);
    assert_eq!(42, r100.scalar);
    assert_eq!(3, r100.len);
    for (i, expect) in [10u32, 20, 30].into_iter().enumerate() {
        assert_eq!(
            expect,
            bytes::read_u32(&state, arena_data + r100.arena_offset + i as u32 * 4)
        );
    }

    let r200 = read_array_row(&state, 200);
    assert_eq!(99, r200.scalar);
    assert_eq!(2, r200.len);
    for (i, expect) in [40u32, 50].into_iter().enumerate() {
        assert_eq!(
            expect,
            bytes::read_u32(&state, arena_data + r200.arena_offset + i as u32 * 4)
        );
    }
}

#[test]
fn struct_map_array_field_empty_array() {
    let prog = build_array_field_program(TYPE_A);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let types = [TYPE_A];
    let keys = [100u32];
    let scalars = [42u32];
    let offsets = [0u32, 0];
    let values = [0u32];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&keys),
        u32s_as_bytes(&scalars),
        u32s_as_bytes(&offsets),
        u32s_as_bytes(&values),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 1));

    let row = read_array_row(&state, 100);
    assert_eq!(0, row.len);
    assert_eq!(0, bytes::read_u32(&state, arena_hdr(&state) + 4));
}

#[test]
fn struct_map_array_field_overwrite_last_wins_old_arena_data_abandoned() {
    let prog = build_array_field_program(TYPE_A);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    {
        let types = [TYPE_A];
        let keys = [100u32];
        let scalars = [42u32];
        let offsets = [0u32, 3];
        let values = [10u32, 20, 30];
        let cols: Vec<&[u8]> = vec![
            u32s_as_bytes(&types),
            u32s_as_bytes(&keys),
            u32s_as_bytes(&scalars),
            u32s_as_bytes(&offsets),
            u32s_as_bytes(&values),
        ];
        assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 1));
    }
    let before = state.clone();
    vm.undo_enable(&state).expect("undo window opens");
    let checkpoint = vm.undo_checkpoint();
    {
        let types = [TYPE_A];
        let keys = [100u32];
        let scalars = [99u32];
        let offsets = [0u32, 2];
        let values = [40u32, 50];
        let cols: Vec<&[u8]> = vec![
            u32s_as_bytes(&types),
            u32s_as_bytes(&keys),
            u32s_as_bytes(&scalars),
            u32s_as_bytes(&offsets),
            u32s_as_bytes(&values),
        ];
        assert_eq!(OK, vm.execute_batch_delta(&mut state, &prog, &cols, 1),);
    }

    assert_eq!(1, slot_size(&state, 0));
    let hdr = arena_hdr(&state);
    // Old 12 bytes abandoned; new 8 appended → 20 used.
    assert_eq!(20, bytes::read_u32(&state, hdr + 4));

    let row = read_array_row(&state, 100);
    assert_eq!(99, row.scalar);
    assert_eq!(12, row.arena_offset); // after the abandoned first write
    assert_eq!(2, row.len);
    let arena_data = hdr + ARENA_HEADER_SIZE;
    assert_eq!(40, bytes::read_u32(&state, arena_data + row.arena_offset));
    assert_eq!(
        50,
        bytes::read_u32(&state, arena_data + row.arena_offset + 4)
    );
    let after = state.clone();
    assert_delta_roundtrip(&mut vm, &before, &after, checkpoint);
}

// =============================================================================
// Nested slots through the full VM pipeline
// =============================================================================

use columine_vm::meta::SlotMetaView;
use columine_vm::nested::{
    arena_data_offset, arena_header_offset, get_inner_offset, get_inner_set_size,
    inner_agg_get_count, inner_agg_get_f64, inner_map_get, inner_set_contains, read_nested_prefix,
};

///  `buildNestedSetE2EProgram`.
fn build_nested_set_e2e_program(type_id: u32) -> Vec<u8> {
    // SLOT_NESTED: outer HASHSET-keyed table cap 32, inner HASHSET cap 8.
    let init_sec = [0x1Au8, 0, 0x09, 32, 0, 1, 8, 0, 1];
    let body = [0x90u8, 0, 1, 2];
    program(1, 3, &init_sec, &for_each(type_id, &body))
}

fn build_nested_map_e2e_program(type_id: u32) -> Vec<u8> {
    let init_sec = [0x1Au8, 0, 0x09, 8, 0, 0, 4, 0, 1];
    let body = [0x92u8, 0, 1, 2, 3];
    program(1, 4, &init_sec, &for_each(type_id, &body))
}

fn build_nested_aggregate_e2e_program(type_id: u32) -> Vec<u8> {
    let init_sec = [0x1Au8, 0, 0x09, 8, 0, 2, 1, 0, 1];
    let body = [0x95u8, 0, 1, 2];
    program(1, 3, &init_sec, &for_each(type_id, &body))
}

#[test]
fn e2e_slot_nested_state_init_roundtrip() {
    let prog = build_nested_set_e2e_program(1);
    let state = init(&prog);

    let meta = SlotMetaView::read(&state, 0);
    assert!(meta.offset > 0);
    assert!(meta.capacity >= 32);

    let prefix = read_nested_prefix(&state, meta.offset);
    assert_eq!(SlotType::HashSet, prefix.inner_type);
    assert_eq!(1, prefix.depth);
}

#[test]
fn e2e_nested_set_insert_through_full_vm_pipeline() {
    let type_id = 1u32;
    let prog = build_nested_set_e2e_program(type_id);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let types = [type_id; 4];
    let outer_keys = [10u32, 10, 20, 10];
    let elems = [100u32, 101, 100, 100]; // last is a dup for key=10
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&outer_keys),
        u32s_as_bytes(&elems),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 4));

    let meta = SlotMetaView::read(&state, 0);
    assert_eq!(2, meta.size(&state));

    let inner_10 = get_inner_offset(&state, &meta, 10);
    assert_ne!(0, inner_10);
    assert_eq!(2, get_inner_set_size(&state, inner_10));
    assert!(inner_set_contains(&state, inner_10, 100));
    assert!(inner_set_contains(&state, inner_10, 101));

    let inner_20 = get_inner_offset(&state, &meta, 20);
    assert_ne!(0, inner_20);
    assert_eq!(1, get_inner_set_size(&state, inner_20));
    assert!(inner_set_contains(&state, inner_20, 100));
}

#[test]
fn nested_set_delta_restores_outer_inner_arena_and_growth() {
    let type_id = 1u32;
    let prog = build_nested_set_e2e_program(type_id);
    let mut state = init(&prog);
    let before = state.clone();
    let meta = SlotMetaView::read(&state, 0);
    let arena_header = arena_header_offset(meta.offset, meta.capacity);
    assert_eq!(bytes::read_u32(&state, arena_header + 4), 0);

    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("undo window opens");
    let checkpoint = vm.undo_checkpoint();
    let distinct = 18u32;
    let types = vec![type_id; distinct as usize];
    let outer_keys = vec![77u32; distinct as usize];
    let elems: Vec<u32> = (0..distinct).collect();
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&outer_keys),
        u32s_as_bytes(&elems),
    ];
    assert_eq!(
        OK,
        vm.execute_batch_delta(&mut state, &prog, &cols, distinct),
    );

    let meta = SlotMetaView::read(&state, 0);
    let inner = get_inner_offset(&state, &meta, 77);
    assert_ne!(inner, 0);
    assert_eq!(bytes::read_u32(&state, inner), 32);
    assert_eq!(get_inner_set_size(&state, inner), distinct);
    assert!(bytes::read_u32(&state, arena_header + 4) > 0);

    let checkpoint_before_duplicate = vm.undo_checkpoint();
    let duplicate_types = [type_id];
    let duplicate_outer = [77u32];
    let duplicate_elems = [distinct - 1];
    let duplicate_cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&duplicate_types),
        u32s_as_bytes(&duplicate_outer),
        u32s_as_bytes(&duplicate_elems),
    ];
    let before_duplicate = state.clone();
    assert_eq!(
        OK,
        vm.execute_batch_delta(&mut state, &prog, &duplicate_cols, 1),
    );
    assert_eq!(state, before_duplicate);
    assert_eq!(vm.undo_checkpoint(), checkpoint_before_duplicate);

    let after = state.clone();
    assert_delta_roundtrip(&mut vm, &before, &after, checkpoint);
}

#[test]
fn nested_map_update_delta_restores_prior_value_and_container_state() {
    let type_id = 2u32;
    let prog = build_nested_map_e2e_program(type_id);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let types = [type_id];
    let outer = [9u32];
    let inner_keys = [41u32];
    let first_values = [0x1122_3344u32];
    let first_cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&outer),
        u32s_as_bytes(&inner_keys),
        u32s_as_bytes(&first_values),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &first_cols, 1));

    let before = state.clone();
    let before_meta = SlotMetaView::read(&before, 0);
    let before_inner = get_inner_offset(&before, &before_meta, 9);
    let before_arena_used = bytes::read_u32(
        &before,
        arena_header_offset(before_meta.offset, before_meta.capacity) + 4,
    );
    vm.undo_enable(&state).expect("undo window opens");
    let checkpoint = vm.undo_checkpoint();
    let second_values = [0xaabb_ccddu32];
    let second_cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&outer),
        u32s_as_bytes(&inner_keys),
        u32s_as_bytes(&second_values),
    ];
    assert_eq!(
        OK,
        vm.execute_batch_delta(&mut state, &prog, &second_cols, 1),
    );

    let after_meta = SlotMetaView::read(&state, 0);
    let after_inner = get_inner_offset(&state, &after_meta, 9);
    assert_eq!(after_inner, before_inner);
    assert_eq!(inner_map_get(&state, after_inner, 41), 0xaabb_ccdd);
    assert_eq!(
        bytes::read_u32(
            &state,
            arena_header_offset(after_meta.offset, after_meta.capacity) + 4,
        ),
        before_arena_used,
    );
    let after = state.clone();
    assert_delta_roundtrip(&mut vm, &before, &after, checkpoint);
}

#[test]
fn nested_aggregate_delta_restores_prior_value_count_and_arena_pointer() {
    let type_id = 3u32;
    let prog = build_nested_aggregate_e2e_program(type_id);
    let mut state = init(&prog);
    let mut vm = Vm::default();
    let types = [type_id];
    let outer = [5u32];

    for value in [4.5f64, 7.25] {
        let values = [value];
        let cols: Vec<&[u8]> = vec![
            u32s_as_bytes(&types),
            u32s_as_bytes(&outer),
            f64s_as_bytes(&values),
        ];
        assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 1));
    }

    let before = state.clone();
    let before_meta = SlotMetaView::read(&before, 0);
    let before_inner = get_inner_offset(&before, &before_meta, 5);
    let before_used = bytes::read_u32(
        &before,
        arena_header_offset(before_meta.offset, before_meta.capacity) + 4,
    );
    assert_eq!(inner_agg_get_f64(&before, before_inner), 11.75);
    assert_eq!(inner_agg_get_count(&before, before_inner, 1), 2);

    vm.undo_enable(&state).expect("undo window opens");
    let checkpoint = vm.undo_checkpoint();
    let values = [3.0f64];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&outer),
        f64s_as_bytes(&values),
    ];
    assert_eq!(OK, vm.execute_batch_delta(&mut state, &prog, &cols, 1),);

    let after_meta = SlotMetaView::read(&state, 0);
    let after_inner = get_inner_offset(&state, &after_meta, 5);
    assert_eq!(after_inner, before_inner);
    assert_eq!(inner_agg_get_f64(&state, after_inner), 14.75);
    assert_eq!(inner_agg_get_count(&state, after_inner, 1), 3);
    assert_eq!(
        bytes::read_u32(
            &state,
            arena_header_offset(after_meta.offset, after_meta.capacity) + 4,
        ),
        before_used,
    );
    let after = state.clone();
    assert_delta_roundtrip(&mut vm, &before, &after, checkpoint);
}

#[test]
fn nested_set_insert_over_stale_arena_bytes_rolls_back_to_full_image() {
    let type_id = 1u32;
    let prog = build_nested_set_e2e_program(type_id);
    let mut state = init(&prog);
    let meta = SlotMetaView::read(&state, 0);

    // Stale allocated arena bytes: nonzero words where inner sets are placed.
    let arena_data = arena_data_offset(meta.offset, meta.capacity);
    for word in 0..8u32 {
        bytes::write_u32(&mut state, arena_data + word * 4, 0xD00D_0000 | word);
    }
    let before = state.clone();

    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("undo window opens");
    let checkpoint = vm.undo_checkpoint();
    let distinct = 18u32;
    let types = vec![type_id; distinct as usize];
    let outer_keys = vec![77u32; distinct as usize];
    let elems: Vec<u32> = (0..distinct).collect();
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&outer_keys),
        u32s_as_bytes(&elems),
    ];
    assert_eq!(
        OK,
        vm.execute_batch_delta(&mut state, &prog, &cols, distinct)
    );

    let after_meta = SlotMetaView::read(&state, 0);
    let inner = get_inner_offset(&state, &after_meta, 77);
    assert_eq!(distinct, get_inner_set_size(&state, inner));
    assert_ne!(before, state);

    let after = state.clone();
    assert_delta_roundtrip(&mut vm, &before, &after, checkpoint);
}

// =============================================================================
// No-timestamp hashmap rejections through dispatch ()
// =============================================================================

#[test]
fn hashmap_no_timestamp_rejects_latest_max_min_opcodes() {
    // type_flags 0x40 = HASHMAP + NO_HASHMAP_TIMESTAMPS; LATEST/MAX/MIN need
    // the timestamp lane and must refuse with INVALID_PROGRAM.
    for reduce in [
        vec![0x20u8, 0, 0, 1, 2, 1], // BATCH_MAP_UPSERT_LATEST
        vec![0x26u8, 0, 0, 1, 2, 1], // BATCH_MAP_UPSERT_MAX
        vec![0x27u8, 0, 0, 1, 2, 1], // BATCH_MAP_UPSERT_MIN
    ] {
        let init_sec = [0x10u8, 0, 0x40, 4, 0];
        let prog = program(1, 3, &init_sec, &reduce);
        let mut state = init(&prog);
        let mut vm = Vm::default();

        let keys = [1u32];
        let vals = [2u32];
        let ts = [3.0f64];
        let cols: Vec<&[u8]> = vec![
            u32s_as_bytes(&keys),
            u32s_as_bytes(&vals),
            f64s_as_bytes(&ts),
        ];
        assert_eq!(
            ErrorCode::InvalidProgram as u32,
            vm.execute_batch(&mut state, &prog, &cols, 1),
            "opcode {:#x} must refuse on a no-timestamp slot",
            reduce[0],
        );
    }
}

// =============================================================================
// BATCH_STRUCT_MAP_UPSERT_FIRST (0x81) — parity regression
// =============================================================================

/// The 0x81 regression covers the body-length table, body arm, top-level arm,
/// and both opcode registries. A prior missing entry caused operand bytes to be
/// misparsed and the upsert to disappear. The test uses the exact
/// TypeScript-compiled flatMap→mapRecord→keyBy('key')→keepValue(first('quantity'))
/// capture, including its hash prefix and FLAT_MAP field mapping.
#[test]
fn ts_struct_map_upsert_first_keeps_first_row_and_rolls_back() {
    let hex = "0000000000000000000000000000000000000000000000000000000000000000\
               434c4d310200000001000000000000000a0000001e0000001800060004030404\
               0000e00101010000001400e103ffffffff0f0b00810004030400050106020000";
    let hex: String = hex.chars().filter(|c| !c.is_whitespace()).collect();
    let prog: Vec<u8> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect();
    let mut state = init(&prog);
    let mut vm = Vm::default();

    // One event (type id 1) flat-mapping to two children with the SAME key —
    // first-wins must keep child 0's fields even within one batch.
    let types = [1u32];
    let offsets = [0u32, 2];
    let child_keys = [42u32, 42];
    let child_labels = [7u32, 8];
    let child_qty = [1u32, 2];
    let pad = [0u32];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&pad),          // col 0 (unused)
        u32s_as_bytes(&types),        // col 1: FOR_EACH type ids
        u32s_as_bytes(&pad),          // col 2 (unused)
        u32s_as_bytes(&offsets),      // col 3: FLAT_MAP offsets
        u32s_as_bytes(&child_keys),   // col 4: key (field 0)
        u32s_as_bytes(&child_labels), // col 5: label (field 1)
        u32s_as_bytes(&child_qty),    // col 6: quantity (field 2)
    ];
    // Scenario runs the delta path with undo enabled — mirror it exactly.
    vm.undo_enable(&state).expect("undo window opens");
    let cp = vm.undo_checkpoint();
    assert_eq!(OK, vm.execute_batch_delta(&mut state, &prog, &cols, 1));

    let read_row = |state: &[u8]| -> Option<(u32, u32, u32)> {
        let meta_base = STATE_HEADER_SIZE;
        let off = slot_offset(state, 0);
        let num_fields = state[(meta_base + SlotMetaOffset::AGG_TYPE) as usize];
        let descriptor: Vec<u8> =
            state[off as usize..(off + u32::from(num_fields)) as usize].to_vec();
        let row = struct_map_row(state, 0, 42)?;
        let f0 = struct_field_offset(num_fields, &descriptor, 0);
        let f1 = struct_field_offset(num_fields, &descriptor, 1);
        let f2 = struct_field_offset(num_fields, &descriptor, 2);
        Some((
            bytes::read_u32(state, row + f0),
            bytes::read_u32(state, row + f1),
            bytes::read_u32(state, row + f2),
        ))
    };

    assert_eq!(
        Some((42, 7, 1)),
        read_row(&state),
        "first child's row must win within the batch"
    );

    // Second batch, same key: still first-write-wins across batches.
    let child_labels2 = [9u32, 9];
    let child_qty2 = [3u32, 3];
    let offsets2 = [0u32, 1];
    let cols2: Vec<&[u8]> = vec![
        u32s_as_bytes(&pad),
        u32s_as_bytes(&types),
        u32s_as_bytes(&pad),
        u32s_as_bytes(&offsets2),
        u32s_as_bytes(&child_keys),
        u32s_as_bytes(&child_labels2),
        u32s_as_bytes(&child_qty2),
    ];
    assert_eq!(OK, vm.execute_batch_delta(&mut state, &prog, &cols2, 1));
    assert_eq!(
        Some((42, 7, 1)),
        read_row(&state),
        "existing row must survive a later-batch write attempt"
    );

    // Rollback to the checkpoint erases the row (Scenario fork navigation).
    vm.undo_rollback(&mut state, cp).expect("valid checkpoint");
    vm.undo_commit();
    assert_eq!(
        None,
        read_row(&state),
        "rollback must remove the first-write row"
    );
}

// Element-family overflow proofs use a separate real COUNT reducer to fill the
// journal to a known budget without overflowing its window before the element
// body is attempted.
fn element_refusal_programs(
    kind: SlotType,
    ttl: bool,
    flattened: bool,
    body: &[u8],
) -> (Vec<u8>, Vec<u8>) {
    let mut init_sec = vec![
        Opcode::SlotDef as u8,
        0,
        kind as u8 | if ttl { 0x10 } else { 0 },
    ];
    init_sec.extend(512u16.to_le_bytes());
    if ttl {
        init_sec.extend(10.0f32.to_le_bytes());
        init_sec.extend(0.0f32.to_le_bytes());
        init_sec.extend([3, 0]); // timestamp column, no calendar alignment
    }
    init_sec.extend([
        Opcode::SlotDef as u8,
        1,
        SlotType::Aggregate as u8,
        AggType::Count as u8,
        0,
    ]);
    let reduce = if flattened {
        for_each(TYPE_A, &flat_map(5, NO_PARENT_TS_COL, body))
    } else {
        for_each(TYPE_A, body)
    };
    (
        program(2, 6, &init_sec, &reduce),
        program(2, 6, &init_sec, &[Opcode::BatchAggCount as u8, 1]),
    )
}

fn execute_element_lane(
    vm: &mut Vm,
    state: &mut [u8],
    prog: &[u8],
    cols: &[&[u8]],
    batch_len: u32,
    delta: bool,
) -> u32 {
    if delta {
        vm.execute_batch_delta(state, prog, cols, batch_len)
    } else {
        vm.execute_batch(state, prog, cols, batch_len)
    }
}

fn fill_element_journal(
    vm: &mut Vm,
    state: &mut [u8],
    count_prog: &[u8],
    delta: bool,
    remaining: u32,
) {
    let target = columine_vm::vm::UNDO_CAPACITY - remaining;
    while vm.undo_checkpoint() < target {
        assert_eq!(
            OK,
            execute_element_lane(vm, state, count_prog, &[], 1, delta)
        );
    }
    assert_eq!(target, vm.undo_checkpoint());
    assert!(
        !vm.undo_status().overflow,
        "filling must not poison the element attempt"
    );
}

struct ElementRows<'a> {
    keys: &'a [u32],
    values: &'a [u32],
    comparisons: &'a [f64],
    predicates: &'a [u32],
}

fn execute_element_rows(
    vm: &mut Vm,
    state: &mut [u8],
    prog: &[u8],
    delta: bool,
    flattened: bool,
    input: ElementRows<'_>,
) -> u32 {
    let rows = u32::try_from(input.keys.len()).expect("test row count fits the VM");
    let batch_len = if flattened { 1 } else { rows };
    let types = vec![TYPE_A; usize::try_from(batch_len).expect("test batch fits memory")];
    let offsets = [0u32, rows];
    let cols = [
        u32s_as_bytes(&types),
        u32s_as_bytes(input.keys),
        u32s_as_bytes(input.values),
        f64s_as_bytes(input.comparisons),
        u32s_as_bytes(input.predicates),
        u32s_as_bytes(&offsets),
    ];
    execute_element_lane(vm, state, prog, &cols, batch_len, delta)
}

fn assert_element_exported_window(vm: &mut Vm, before: &[u8], after: &[u8], start: u32, end: u32) {
    assert!(
        end > start,
        "the accepted window must contain actual mutations"
    );
    assert_eq!(Ok(end - start), vm.delta_export_segment(start, end));
    let undo = vm.delta_export_undo().to_vec();
    let redo = vm.delta_export_redo().to_vec();
    let mut replay = Vm::default();
    let mut backward = after.to_vec();
    replay
        .delta_apply_rollback_segment(&mut backward, &undo, Vm::delta_export_entry_size())
        .expect("valid accepted-prefix undo");
    assert_eq!(backward, before, "every allocated byte must undo");
    let mut forward = before.to_vec();
    replay
        .delta_apply_rollforward_segment(&mut forward, &redo, Vm::delta_export_entry_size())
        .expect("valid accepted-prefix redo");
    assert_eq!(forward, after, "every allocated byte must redo");
}

/// Each element family past the record budget overflows the window: its write
/// lands, the window reports overflow and no savepoint, and rollback to the
/// pre-overflow savepoint restores the image byte-exact on both lanes.
#[test]
fn element_upsert_arms_overflow_lands_and_rolls_back_byte_exact() {
    const F64_CMP: u8 = columine_types::types::CmpType::F64 as u8;
    const FRESH_KEY: u32 = 7;
    const RETRY_KEY: u32 = 9;
    // Body columns are the ones `element_refusal_programs` declares: 0 target
    // slot, 1 key, 2 value, 3 f64 comparison/TTL, 4 u32 predicate. Each arm
    // runs through FOR_EACH (flattened=false) and FLAT_MAP (flattened=true).
    // Paired sibling opcodes share an arm and are listed separately.
    const CASES: &[(&str, SlotType, bool, &[u8])] = &[
        (
            "MAP_UPSERT_LATEST",
            SlotType::HashMap,
            false,
            &[Opcode::BatchMapUpsertLatest as u8, 0, 1, 2, 3, F64_CMP],
        ),
        (
            "MAP_UPSERT_LATEST_TTL",
            SlotType::HashMap,
            true,
            &[Opcode::BatchMapUpsertLatestTtl as u8, 0, 1, 2, 3, F64_CMP],
        ),
        (
            "MAP_UPSERT_FIRST",
            SlotType::HashMap,
            false,
            &[Opcode::BatchMapUpsertFirst as u8, 0, 1, 2],
        ),
        (
            "MAP_UPSERT_LAST",
            SlotType::HashMap,
            false,
            &[Opcode::BatchMapUpsertLast as u8, 0, 1, 2],
        ),
        (
            "MAP_UPSERT_LAST_TTL",
            SlotType::HashMap,
            true,
            &[Opcode::BatchMapUpsertLastTtl as u8, 0, 1, 2, 3],
        ),
        (
            "MAP_UPSERT_MAX",
            SlotType::HashMap,
            false,
            &[Opcode::BatchMapUpsertMax as u8, 0, 1, 2, 3, F64_CMP],
        ),
        (
            "MAP_UPSERT_MIN",
            SlotType::HashMap,
            false,
            &[Opcode::BatchMapUpsertMin as u8, 0, 1, 2, 3, F64_CMP],
        ),
        (
            "MAP_UPSERT_LATEST_IF",
            SlotType::HashMap,
            false,
            &[Opcode::BatchMapUpsertLatestIf as u8, 0, 1, 2, 3, F64_CMP, 4],
        ),
        (
            "MAP_UPSERT_FIRST_IF",
            SlotType::HashMap,
            false,
            &[Opcode::BatchMapUpsertFirstIf as u8, 0, 1, 2, 4],
        ),
        (
            "MAP_UPSERT_LAST_IF",
            SlotType::HashMap,
            false,
            &[Opcode::BatchMapUpsertLastIf as u8, 0, 1, 2, 4],
        ),
        (
            "MAP_UPSERT_MAX_IF",
            SlotType::HashMap,
            false,
            &[Opcode::BatchMapUpsertMaxIf as u8, 0, 1, 2, 3, F64_CMP, 4],
        ),
        (
            "MAP_UPSERT_MIN_IF",
            SlotType::HashMap,
            false,
            &[Opcode::BatchMapUpsertMinIf as u8, 0, 1, 2, 3, F64_CMP, 4],
        ),
        (
            "SET_INSERT",
            SlotType::HashSet,
            false,
            &[Opcode::BatchSetInsert as u8, 0, 1],
        ),
        (
            "SET_INSERT_TTL",
            SlotType::HashSet,
            true,
            &[Opcode::BatchSetInsertTtl as u8, 0, 1, 3],
        ),
        (
            "SET_INSERT_IF",
            SlotType::HashSet,
            false,
            &[Opcode::BatchSetInsertIf as u8, 0, 1, 4],
        ),
        (
            "BITMAP_SET_INSERT",
            SlotType::Bitmap,
            false,
            &[Opcode::BatchSetInsert as u8, 0, 1],
        ),
        (
            "BITMAP_SET_INSERT_TTL",
            SlotType::Bitmap,
            true,
            &[Opcode::BatchSetInsertTtl as u8, 0, 1, 3],
        ),
        (
            "BITMAP_SET_INSERT_IF",
            SlotType::Bitmap,
            false,
            &[Opcode::BatchSetInsertIf as u8, 0, 1, 4],
        ),
        (
            "BITMAP_ADD",
            SlotType::Bitmap,
            false,
            &[Opcode::BatchBitmapAdd as u8, 0, 1],
        ),
    ];

    for &(label, kind, ttl, body) in CASES {
        for flattened in [false, true] {
            for delta in [false, true] {
                let (target, count) = element_refusal_programs(kind, ttl, flattened, body);
                let mut state = init(&target);
                let mut vm = Vm::default();
                vm.undo_enable(&state).expect("undo window opens");
                let initial = state.clone();

                // Four records of room remain. The root boundary reserves
                // slots + derived flag (3) and is admitted; the family's first
                // capture then exhausts the budget. The shadow takes the image
                // before that write, and the write lands unrecorded.
                fill_element_journal(&mut vm, &mut state, &count, delta, 4);
                let checkpoint = vm.undo_checkpoint();
                let before = state.clone();

                let landed = execute_element_rows(
                    &mut vm,
                    &mut state,
                    &target,
                    delta,
                    flattened,
                    ElementRows {
                        keys: &[FRESH_KEY],
                        values: &[17],
                        comparisons: &[42.0],
                        predicates: &[1],
                    },
                );
                assert_eq!(
                    OK, landed,
                    "{label} flattened={flattened} delta={delta}: overflowing family must land its write"
                );
                assert_ne!(
                    before, state,
                    "{label} flattened={flattened} delta={delta}: overflowing write must change the image"
                );
                assert!(
                    vm.undo_has_overflow(),
                    "{label} flattened={flattened} delta={delta}: budget exhaustion must overflow the window"
                );
                assert_eq!(
                    None,
                    vm.undo_refusal(),
                    "{label} flattened={flattened} delta={delta}: overflow is not a refusal"
                );
                assert_eq!(
                    UNDO_POSITION_OVERFLOWED,
                    vm.undo_checkpoint(),
                    "{label} flattened={flattened} delta={delta}: overflowed window has no savepoint"
                );
                let overflowed = state.clone();

                // A later write in the overflowed window lands unrecorded too.
                let later = execute_element_rows(
                    &mut vm,
                    &mut state,
                    &target,
                    delta,
                    flattened,
                    ElementRows {
                        keys: &[RETRY_KEY],
                        values: &[18],
                        comparisons: &[43.0],
                        predicates: &[1],
                    },
                );
                assert_eq!(
                    OK, later,
                    "{label} flattened={flattened} delta={delta}: write past overflow must land"
                );
                assert_ne!(
                    overflowed, state,
                    "{label} flattened={flattened} delta={delta}: write past overflow must change the image"
                );
                assert!(
                    vm.undo_has_overflow(),
                    "{label} flattened={flattened} delta={delta}: overflow must stay set"
                );
                assert_eq!(
                    UNDO_POSITION_OVERFLOWED,
                    vm.undo_checkpoint(),
                    "{label} flattened={flattened} delta={delta}: window must stay without savepoint"
                );

                if delta {
                    assert_element_exported_window(&mut vm, &initial, &before, 0, checkpoint);
                }
                vm.undo_rollback(&mut state, checkpoint)
                    .expect("pre-overflow savepoint stays valid");
                assert_eq!(
                    before, state,
                    "{label} flattened={flattened} delta={delta}: rollback to the pre-overflow savepoint must restore its image byte-exact"
                );
                assert!(!vm.undo_has_overflow());
                assert_eq!(checkpoint, vm.undo_checkpoint());

                vm.undo_rollback(&mut state, 0).expect("valid checkpoint");
                assert_eq!(
                    initial, state,
                    "{label} flattened={flattened} delta={delta}: rollback to 0 must restore the initial image"
                );
                assert_eq!(0, vm.undo_checkpoint());
                vm.undo_commit();
                vm.undo_enable(&state).expect("undo window opens");
                let fitting = execute_element_rows(
                    &mut vm,
                    &mut state,
                    &target,
                    delta,
                    flattened,
                    ElementRows {
                        keys: &[FRESH_KEY],
                        values: &[17],
                        comparisons: &[42.0],
                        predicates: &[1],
                    },
                );
                assert_eq!(
                    OK, fitting,
                    "{label} flattened={flattened} delta={delta}: fitting same-family op must succeed after re-enable"
                );
                assert_ne!(
                    initial, state,
                    "{label} flattened={flattened} delta={delta}: fitting op must write"
                );
                assert!(!vm.undo_status().overflow);
            }
        }
    }
}

/// A 128-row batch past the record budget lands whole: the window overflows, later
/// writes land unrecorded, and rollback to the pre-batch savepoint restores its image.
#[test]
fn nested_element_journal_overflow_lands_whole_batch_and_rolls_back_exactly() {
    // Real MAP_UPSERT_LAST (slot 0, key column 1, value column 2) over 128 fresh keys.
    let body = [Opcode::BatchMapUpsertLast as u8, 0, 1, 2];
    let keys: Vec<u32> = (1..=128).collect();
    let values: Vec<u32> = (1000..1128).collect();
    let comparisons = vec![0.0f64; keys.len()];
    let predicates = vec![1u32; keys.len()];
    for flattened in [false, true] {
        for delta in [false, true] {
            let (prog, count_prog) =
                element_refusal_programs(SlotType::HashMap, false, flattened, &body);
            let mut state = init(&prog);
            let mut vm = Vm::default();
            vm.undo_enable(&state).expect("undo window opens");
            let initial = state.clone();
            // The real COUNT reducer leaves the window 96 records short of its end.
            fill_element_journal(&mut vm, &mut state, &count_prog, delta, 96);
            let before = state.clone();
            let checkpoint = vm.undo_checkpoint();

            // 128 valid fresh keys in one body batch: the window runs out mid-batch,
            // and the rest of the batch lands unrecorded behind the shadow.
            let landed = execute_element_rows(
                &mut vm,
                &mut state,
                &prog,
                delta,
                flattened,
                ElementRows {
                    keys: &keys,
                    values: &values,
                    comparisons: &comparisons,
                    predicates: &predicates,
                },
            );
            assert_eq!(OK, landed, "the overflowing batch must land");
            assert!(
                vm.undo_has_overflow(),
                "the batch must exhaust the record budget"
            );
            assert_eq!(UNDO_POSITION_OVERFLOWED, vm.undo_checkpoint());
            let after = state.clone();

            // The overflowed batch lands exactly what an undo-off run of it lands.
            let mut reference = before.clone();
            let mut reference_vm = Vm::default();
            let reference_landed = execute_element_rows(
                &mut reference_vm,
                &mut reference,
                &prog,
                false,
                flattened,
                ElementRows {
                    keys: &keys,
                    values: &values,
                    comparisons: &comparisons,
                    predicates: &predicates,
                },
            );
            assert_eq!(OK, reference_landed, "the undo-off reference must land");
            assert_eq!(
                reference, after,
                "the overflowed batch must land every row exactly"
            );

            // A later write in the overflowed window lands unrecorded as well.
            let later = execute_element_rows(
                &mut vm,
                &mut state,
                &prog,
                delta,
                flattened,
                ElementRows {
                    keys: &[200],
                    values: &[7],
                    comparisons: &[0.0],
                    predicates: &[1],
                },
            );
            assert_eq!(OK, later, "a write past overflow must land");
            assert_ne!(after, state, "a write past overflow must change the image");
            assert!(vm.undo_has_overflow());
            assert_eq!(UNDO_POSITION_OVERFLOWED, vm.undo_checkpoint());

            if delta {
                // The pre-overflow prefix exports while overflowed and replays exactly.
                assert_element_exported_window(&mut vm, &initial, &before, 0, checkpoint);
            }

            vm.undo_rollback(&mut state, checkpoint)
                .expect("the pre-overflow checkpoint stays valid after overflow");
            assert_eq!(
                before, state,
                "rollback to the checkpoint restores its image byte-exact"
            );
            assert_eq!(checkpoint, vm.undo_checkpoint());
            assert!(!vm.undo_has_overflow());
            vm.undo_rollback(&mut state, 0)
                .expect("the origin checkpoint stays valid");
            assert_eq!(
                state, initial,
                "rollback to the origin restores every allocated byte"
            );
            vm.undo_commit();
        }
    }
}

#[test]
fn malformed_bitmap_element_preserves_invalid_state_cause() {
    // BATCH_BITMAP_ADD (slot 0, element column 1) over one canonical BITMAP slot.
    let body = [Opcode::BatchBitmapAdd as u8, 0, 1];
    let keys = [7u32];
    let values = [0u32];
    let comparisons = [0.0f64];
    let predicates = [1u32];
    for flattened in [false, true] {
        for delta in [false, true] {
            let (prog, _) = element_refusal_programs(SlotType::Bitmap, false, flattened, &body);

            // Control: the same program adds the element to the canonical image.
            let mut clean = init(&prog);
            let mut clean_vm = Vm::default();
            clean_vm.undo_enable(&clean).expect("undo window opens");
            let admitted = execute_element_rows(
                &mut clean_vm,
                &mut clean,
                &prog,
                delta,
                flattened,
                ElementRows {
                    keys: &keys,
                    values: &values,
                    comparisons: &comparisons,
                    predicates: &predicates,
                },
            );
            assert_eq!(
                OK, admitted,
                "the canonical bitmap image accepts the element"
            );

            // Corrupt only the four native image-id bytes at the payload offset. Slot metadata
            // and layout stay as initialised; the U64 id differs from the U32 id in one bit.
            // Authoritative slot binding now refuses this payload before mutation;
            // this proves element-loop cause propagation, not kernel preparation.
            let mut state = init(&prog);
            let meta = SlotMetaView::read(&state, 0);
            let storage = columine_vm::bitmap_ops::get_bitmap_storage(&meta);
            assert!(
                storage.serialized_len(&state).is_ok(),
                "fresh bitmap image is canonical"
            );
            let payload = storage.payload_offset() as usize;
            state[payload..payload + bitmosaic::IMAGE_ID_LEN]
                .copy_from_slice(&bitmosaic::EMPTY_U64_IMAGE);
            assert_eq!(storage.serialized_len(&state), Err(ErrorCode::InvalidState));

            let mut vm = Vm::default();
            vm.undo_enable(&state).expect("undo window opens");
            let baseline = state.clone();
            let checkpoint = vm.undo_checkpoint();
            let result = execute_element_rows(
                &mut vm,
                &mut state,
                &prog,
                delta,
                flattened,
                ElementRows {
                    keys: &keys,
                    values: &values,
                    comparisons: &comparisons,
                    predicates: &predicates,
                },
            );
            assert_eq!(
                ErrorCode::InvalidState as u32,
                result,
                "the bitmap prepare's InvalidState must reach the element caller"
            );
            assert_eq!(state, baseline, "a refused prepare writes no bytes");
            assert_eq!(
                vm.undo_checkpoint(),
                checkpoint,
                "a refused prepare journals nothing"
            );
            assert!(
                !vm.undo_status().overflow,
                "a malformed image is not a journal refusal"
            );
        }
    }
}
