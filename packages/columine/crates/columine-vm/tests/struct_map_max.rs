use columine_types::types::{ErrorCode, STATE_HEADER_SIZE, SlotMetaOffset};
use columine_vm::bytes;
use columine_vm::state_init::{calculate_state_size, init_state};
use columine_vm::struct_map::StructMapSlot;
use columine_vm::vm::{Vm, u32s_as_bytes, vm_struct_map_get_row_ptr};

const OK: u32 = ErrorCode::Ok as u32;
const INVALID_PROGRAM: u32 = ErrorCode::InvalidProgram as u32;
const TYPE_ID: u32 = 0xA11C_E001;

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

    let mut program = vec![0u8; 32];
    program.extend([0x41, 0x58, 0x45, 0x31, 1, 0, 1, num_inputs, 0, 0]);
    program.extend(
        u16::try_from(init.len())
            .expect("init length fits u16")
            .to_le_bytes(),
    );
    program.extend(
        u16::try_from(reduce.len())
            .expect("reduce length fits u16")
            .to_le_bytes(),
    );
    program.extend(init);
    program.extend_from_slice(reduce);
    program
}

fn upsert(
    opcode: u8,
    key_col: u8,
    scalar_pairs: &[(u8, u8)],
    array_triples: &[(u8, u8, u8)],
    comparison_field_idx: Option<u8>,
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
    if let Some(field) = comparison_field_idx {
        code.push(field);
    }
    code
}

fn max_upsert(key_col: u8, scalar_pairs: &[(u8, u8)], comparison: u8) -> Vec<u8> {
    upsert(0x82, key_col, scalar_pairs, &[], Some(comparison))
}

fn for_each(body: &[u8]) -> Vec<u8> {
    let mut reduce = vec![0xE0, 0, 1];
    reduce.extend(TYPE_ID.to_le_bytes());
    reduce.extend(
        u16::try_from(body.len())
            .expect("body length fits u16")
            .to_le_bytes(),
    );
    reduce.extend_from_slice(body);
    reduce
}

fn flat_map(body: &[u8]) -> Vec<u8> {
    let mut flat = vec![0xE1, 1, 0xFF];
    flat.extend(
        u16::try_from(body.len())
            .expect("inner body length fits u16")
            .to_le_bytes(),
    );
    flat.extend_from_slice(body);
    for_each(&flat)
}

fn init(program: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(program);
    assert!(size > 0);
    let mut state = vec![0u8; usize::try_from(size).expect("state size fits usize")];
    init_state(&mut state, program).expect("init state");
    state
}

fn meta_u32(state: &[u8], field_off: u32) -> u32 {
    bytes::read_u32(state, STATE_HEADER_SIZE + field_off)
}

fn change_flags(state: &[u8]) -> u8 {
    let offset = STATE_HEADER_SIZE + SlotMetaOffset::CHANGE_FLAGS;
    state[usize::try_from(offset).expect("metadata offset fits usize")]
}

fn clear_change_flags(state: &mut [u8]) {
    let offset = STATE_HEADER_SIZE + SlotMetaOffset::CHANGE_FLAGS;
    state[usize::try_from(offset).expect("metadata offset fits usize")] = 0;
}

fn row(state: &[u8], key: u32) -> u32 {
    let meta_base = STATE_HEADER_SIZE;
    let num_fields = u32::from(state[(meta_base + SlotMetaOffset::AGG_TYPE) as usize]);
    let row_size = u32::from(bytes::read_u16(
        state,
        meta_base + SlotMetaOffset::TTL_SECONDS,
    ));
    vm_struct_map_get_row_ptr(
        state,
        meta_u32(state, SlotMetaOffset::OFFSET),
        meta_u32(state, SlotMetaOffset::CAPACITY),
        num_fields,
        row_size,
        key,
    )
}

fn payload(state: &[u8], key: u32) -> u32 {
    bytes::read_u32(state, row(state, key) + 1)
}

fn execute_top_level(
    vm: &mut Vm,
    state: &mut [u8],
    program: &[u8],
    key: u32,
    value: u32,
    comparison: &[u8],
) -> u32 {
    let keys = [key];
    let values = [value];
    let cols = [u32s_as_bytes(&keys), u32s_as_bytes(&values), comparison];
    vm.execute_batch_delta(state, program, &cols, 1)
}

fn assert_scalar_comparison(
    field_type: u8,
    initial: &[u8],
    lower: &[u8],
    equal: &[u8],
    higher: Option<&[u8]>,
) {
    let reduce = max_upsert(0, &[(1, 0), (2, 1)], 1);
    let program = program(3, &[0, field_type], &reduce);
    let mut state = init(&program);
    let mut vm = Vm::default();

    assert_eq!(
        OK,
        execute_top_level(&mut vm, &mut state, &program, 7, 10, initial)
    );
    assert_eq!(10, payload(&state, 7));
    clear_change_flags(&mut state);

    vm.undo_enable(&state);
    let checkpoint = vm.undo_checkpoint();
    let accepted = state.clone();

    assert_eq!(
        OK,
        execute_top_level(&mut vm, &mut state, &program, 7, 20, lower)
    );
    assert_eq!(
        accepted, state,
        "lower comparison mutated the row or VM flags"
    );
    assert_eq!(
        checkpoint,
        vm.undo_checkpoint(),
        "lower comparison journaled undo"
    );

    assert_eq!(
        OK,
        execute_top_level(&mut vm, &mut state, &program, 7, 30, equal)
    );
    assert_eq!(
        accepted, state,
        "equal comparison mutated the row or VM flags"
    );
    assert_eq!(
        checkpoint,
        vm.undo_checkpoint(),
        "equal comparison journaled undo"
    );

    if let Some(higher) = higher {
        assert_eq!(
            OK,
            execute_top_level(&mut vm, &mut state, &program, 7, 40, higher)
        );
        assert_eq!(40, payload(&state, 7));
        assert!(
            vm.undo_checkpoint() > checkpoint,
            "higher comparison did not journal"
        );
        assert_ne!(
            0,
            change_flags(&state),
            "higher comparison did not set change flags"
        );
    }
}

#[test]
fn top_level_max_uses_exact_scalar_comparison_semantics() {
    assert_scalar_comparison(
        0,
        &10u32.to_le_bytes(),
        &9u32.to_le_bytes(),
        &10u32.to_le_bytes(),
        Some(&11u32.to_le_bytes()),
    );
    assert_scalar_comparison(
        4,
        &10u32.to_le_bytes(),
        &9u32.to_le_bytes(),
        &10u32.to_le_bytes(),
        Some(&11u32.to_le_bytes()),
    );
    assert_scalar_comparison(
        3,
        &1u32.to_le_bytes(),
        &0u32.to_le_bytes(),
        &1u32.to_le_bytes(),
        None,
    );
    assert_scalar_comparison(
        1,
        &(-5i64).to_le_bytes(),
        &(-6i64).to_le_bytes(),
        &(-5i64).to_le_bytes(),
        Some(&(-4i64).to_le_bytes()),
    );
    assert_scalar_comparison(
        2,
        &5.0f64.to_le_bytes(),
        &4.0f64.to_le_bytes(),
        &5.0f64.to_le_bytes(),
        Some(&6.0f64.to_le_bytes()),
    );
}

#[test]
fn top_level_max_accepts_missing_key_and_missing_stored_comparison() {
    let max_reduce = max_upsert(0, &[(1, 0), (2, 1)], 1);
    let max_program = program(3, &[0, 0], &max_reduce);
    let mut state = init(&max_program);
    let mut vm = Vm::default();

    let last_reduce = upsert(0x80, 0, &[(1, 0)], &[], None);
    let last_program = program(3, &[0, 0], &last_reduce);
    let keys = [77u32];
    let values = [10u32];
    let unused = [0u32];
    let seed_cols = [
        u32s_as_bytes(&keys),
        u32s_as_bytes(&values),
        u32s_as_bytes(&unused),
    ];
    assert_eq!(
        OK,
        vm.execute_batch(&mut state, &last_program, &seed_cols, 1)
    );
    let stored_row = row(&state, 77);
    assert!(!StructMapSlot::is_field_set(&state, stored_row, 1));

    assert_eq!(
        OK,
        execute_top_level(
            &mut vm,
            &mut state,
            &max_program,
            77,
            20,
            &1u32.to_le_bytes(),
        ),
    );
    assert_eq!(20, payload(&state, 77));
    assert!(StructMapSlot::is_field_set(&state, stored_row, 1));

    assert_eq!(
        OK,
        execute_top_level(
            &mut vm,
            &mut state,
            &max_program,
            88,
            30,
            &0u32.to_le_bytes(),
        ),
    );
    assert_ne!(0xFFFF_FFFF, row(&state, 88));
}

fn assert_invalid_without_mutation(program: &[u8], comparison: &[u8]) {
    let mut state = init(program);
    let mut vm = Vm::default();
    vm.undo_enable(&state);
    let checkpoint = vm.undo_checkpoint();
    let before = state.clone();
    assert_eq!(
        INVALID_PROGRAM,
        execute_top_level(&mut vm, &mut state, program, 7, 10, comparison),
    );
    assert_eq!(before, state);
    assert_eq!(checkpoint, vm.undo_checkpoint());
}

#[test]
fn top_level_max_rejects_unmapped_invalid_and_non_scalar_comparisons() {
    let unmapped = program(3, &[0, 0], &max_upsert(0, &[(1, 0)], 1));
    assert_invalid_without_mutation(&unmapped, &1u32.to_le_bytes());

    let invalid_ordinal = program(3, &[0, 0], &max_upsert(0, &[(1, 0), (2, 1)], 2));
    assert_invalid_without_mutation(&invalid_ordinal, &1u32.to_le_bytes());

    let array_comparison = program(3, &[0, 5], &max_upsert(0, &[(1, 0), (2, 1)], 1));
    assert_invalid_without_mutation(&array_comparison, &1u32.to_le_bytes());

    let mut truncated_reduce = max_upsert(0, &[(1, 0), (2, 1)], 1);
    truncated_reduce.pop();
    let truncated = program(3, &[0, 0], &truncated_reduce);
    assert_invalid_without_mutation(&truncated, &1u32.to_le_bytes());
}

#[test]
fn for_each_max_rejects_ties_and_lower_rows_before_array_or_undo_mutation() {
    let body = upsert(0x82, 1, &[(2, 0), (3, 1)], &[(4, 5, 2)], Some(1));
    let program = program(6, &[0, 0, 5], &for_each(&body));
    let mut state = init(&program);
    let mut vm = Vm::default();

    let types = [TYPE_ID];
    let keys = [9u32];
    let values = [10u32];
    let comparisons = [5u32];
    let offsets = [0u32, 2];
    let arrays = [7u32, 8];
    let cols = [
        u32s_as_bytes(&types),
        u32s_as_bytes(&keys),
        u32s_as_bytes(&values),
        u32s_as_bytes(&comparisons),
        u32s_as_bytes(&offsets),
        u32s_as_bytes(&arrays),
    ];
    assert_eq!(OK, vm.execute_batch_delta(&mut state, &program, &cols, 1));
    assert_eq!(10, payload(&state, 9));
    clear_change_flags(&mut state);

    vm.undo_enable(&state);
    let checkpoint = vm.undo_checkpoint();
    let accepted = state.clone();

    for (candidate, comparison, array_value) in [(20u32, 5u32, 90u32), (30, 4, 91)] {
        let candidate_values = [candidate];
        let candidate_comparisons = [comparison];
        let candidate_arrays = [array_value, array_value + 1];
        let candidate_cols = [
            u32s_as_bytes(&types),
            u32s_as_bytes(&keys),
            u32s_as_bytes(&candidate_values),
            u32s_as_bytes(&candidate_comparisons),
            u32s_as_bytes(&offsets),
            u32s_as_bytes(&candidate_arrays),
        ];
        assert_eq!(
            OK,
            vm.execute_batch_delta(&mut state, &program, &candidate_cols, 1),
        );
        assert_eq!(accepted, state);
        assert_eq!(checkpoint, vm.undo_checkpoint());
    }

    let higher_values = [40u32];
    let higher_comparisons = [6u32];
    let higher_arrays = [100u32, 101];
    let higher_cols = [
        u32s_as_bytes(&types),
        u32s_as_bytes(&keys),
        u32s_as_bytes(&higher_values),
        u32s_as_bytes(&higher_comparisons),
        u32s_as_bytes(&offsets),
        u32s_as_bytes(&higher_arrays),
    ];
    assert_eq!(
        OK,
        vm.execute_batch_delta(&mut state, &program, &higher_cols, 1)
    );
    assert_eq!(40, payload(&state, 9));
    assert!(vm.undo_checkpoint() > checkpoint);
    assert_ne!(0, change_flags(&state));
}

#[test]
fn flat_map_body_length_and_dispatch_include_max_comparison_ordinal() {
    let body = max_upsert(2, &[(3, 0), (4, 1)], 1);
    let program = program(5, &[0, 0], &flat_map(&body));
    let mut state = init(&program);
    let mut vm = Vm::default();

    let types = [TYPE_ID];
    let offsets = [0u32, 3];
    let keys = [11u32, 11, 11];
    let values = [10u32, 20, 30];
    let comparisons = [5u32, 4, 6];
    let cols = [
        u32s_as_bytes(&types),
        u32s_as_bytes(&offsets),
        u32s_as_bytes(&keys),
        u32s_as_bytes(&values),
        u32s_as_bytes(&comparisons),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &program, &cols, 1));
    assert_eq!(30, payload(&state, 11));
    clear_change_flags(&mut state);

    vm.undo_enable(&state);
    let checkpoint = vm.undo_checkpoint();
    let accepted = state.clone();
    let tie_offsets = [0u32, 1];
    let tie_keys = [11u32];
    let tie_values = [99u32];
    let tie_comparisons = [6u32];
    let tie_cols = [
        u32s_as_bytes(&types),
        u32s_as_bytes(&tie_offsets),
        u32s_as_bytes(&tie_keys),
        u32s_as_bytes(&tie_values),
        u32s_as_bytes(&tie_comparisons),
    ];
    assert_eq!(
        OK,
        vm.execute_batch_delta(&mut state, &program, &tie_cols, 1)
    );
    assert_eq!(accepted, state);
    assert_eq!(checkpoint, vm.undo_checkpoint());
}

#[test]
fn block_max_rejects_unmapped_and_non_scalar_comparisons_without_mutation() {
    for (field_types, body) in [
        (vec![0u8, 0], max_upsert(1, &[(2, 0)], 1)),
        (vec![0u8, 5], max_upsert(1, &[(2, 0), (3, 1)], 1)),
    ] {
        let program = program(4, &field_types, &for_each(&body));
        let mut state = init(&program);
        let mut vm = Vm::default();
        vm.undo_enable(&state);
        let checkpoint = vm.undo_checkpoint();
        let before = state.clone();
        let types = [TYPE_ID];
        let keys = [5u32];
        let values = [10u32];
        let comparisons = [1u32];
        let cols = [
            u32s_as_bytes(&types),
            u32s_as_bytes(&keys),
            u32s_as_bytes(&values),
            u32s_as_bytes(&comparisons),
        ];
        assert_eq!(
            INVALID_PROGRAM,
            vm.execute_batch_delta(&mut state, &program, &cols, 1),
        );
        assert_eq!(before, state);
        assert_eq!(checkpoint, vm.undo_checkpoint());
    }
}
