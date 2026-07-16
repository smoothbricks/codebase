use columine_types::types::{
    EMPTY_KEY, ErrorCode, Opcode, SLOT_META_SIZE, STATE_HEADER_SIZE, SlotMetaOffset, SlotType,
    TOMBSTONE, hash_key_pair,
};
use columine_vm::bytes;
use columine_vm::state_init::{
    calculate_grown_state_size, calculate_state_size, grow_state, init_state,
};
use columine_vm::struct_map::{StructMap2Slot, StructMapSlot};
use columine_vm::vm::{Vm, u32s_as_bytes, vm_struct_map2_get_row_ptr};

const ADD: u32 = 0xadd0_0001;
const REVOKE: u32 = 0xadd0_0002;
const OK: u32 = ErrorCode::Ok as u32;

fn program(init: &[u8], num_inputs: u8, reduce: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; 32];
    out.extend([0x41, 0x58, 0x45, 0x31, 1, 0, 1, num_inputs, 0, 0]);
    out.extend(u16::try_from(init.len()).unwrap().to_le_bytes());
    out.extend(u16::try_from(reduce.len()).unwrap().to_le_bytes());
    out.extend_from_slice(init);
    out.extend_from_slice(reduce);
    out
}

fn slot_init(op: u8, slot_type: SlotType, requested_capacity: u16, fields: &[u8]) -> Vec<u8> {
    let [lo, hi] = requested_capacity.to_le_bytes();
    let mut init = vec![op, 0, slot_type as u8, lo, hi, fields.len() as u8];
    init.extend_from_slice(fields);
    init
}

fn map2_program(requested_capacity: u16, fields: &[u8], num_inputs: u8, reduce: &[u8]) -> Vec<u8> {
    program(
        &slot_init(
            Opcode::SlotStructMap2 as u8,
            SlotType::StructMap2,
            requested_capacity,
            fields,
        ),
        num_inputs,
        reduce,
    )
}

fn init(program: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(program);
    assert!(size > 0);
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, program).unwrap();
    state
}

fn upsert_code(key1_col: u8, key2_col: u8, pairs: &[(u8, u8)]) -> Vec<u8> {
    let mut code = vec![
        Opcode::BatchStructMap2UpsertLast as u8,
        0,
        key1_col,
        key2_col,
        pairs.len() as u8,
    ];
    for &(column, field) in pairs {
        code.extend([column, field]);
    }
    code
}

fn remove_code(key1_col: u8, key2_col: u8) -> Vec<u8> {
    vec![Opcode::BatchStructMap2Remove as u8, 0, key1_col, key2_col]
}

fn for_each(type_id: u32, body: &[u8]) -> Vec<u8> {
    let mut code = vec![Opcode::ForEach as u8, 0, 1];
    code.extend(type_id.to_le_bytes());
    code.extend((body.len() as u16).to_le_bytes());
    code.extend_from_slice(body);
    code
}

fn pair_row(state: &[u8], key1: u32, key2: u32) -> Option<u32> {
    let slot = StructMap2Slot::bind(state, 0);
    let row = vm_struct_map2_get_row_ptr(
        state,
        slot.slot_offset,
        slot.capacity,
        u32::from(slot.num_fields),
        slot.row_size,
        key1,
        key2,
    );
    (row != u32::MAX).then_some(row)
}

fn permission(state: &[u8], principal: u32, resource: u32) -> Option<(u32, u32)> {
    let slot = StructMap2Slot::bind(state, 0);
    let row = pair_row(state, principal, resource)?;
    assert!(StructMapSlot::is_field_set(state, row, 0));
    assert!(StructMapSlot::is_field_set(state, row, 1));
    Some((
        bytes::read_u32(state, row + slot.field_offset(state, 0)),
        bytes::read_u32(state, row + slot.field_offset(state, 1)),
    ))
}

fn execute_u32(
    vm: &mut Vm,
    state: &mut [u8],
    program: &[u8],
    columns: &[&[u32]],
    delta: bool,
) -> u32 {
    let raw: Vec<&[u8]> = columns.iter().map(|column| u32s_as_bytes(column)).collect();
    if delta {
        vm.execute_batch_delta(state, program, &raw, columns[0].len() as u32)
    } else {
        vm.execute_batch(state, program, &raw, columns[0].len() as u32)
    }
}

#[test]
fn permission_reducer_adds_updates_and_revokes_exact_pairs() {
    let mut reduce = for_each(ADD, &upsert_code(1, 2, &[(3, 0), (4, 1)]));
    reduce.extend(for_each(REVOKE, &remove_code(1, 2)));
    let program = map2_program(8, &[4, 0], 5, &reduce);
    let mut state = init(&program);
    let mut vm = Vm::default();

    let types = [ADD, ADD, ADD];
    let principals = [7, 7, 8];
    let resources = [40, 41, 40];
    let roles = [100, 101, 102];
    let granted = [1_000u32, 2_000, 3_000];
    assert_eq!(
        OK,
        execute_u32(
            &mut vm,
            &mut state,
            &program,
            &[&types, &principals, &resources, &roles, &granted],
            true,
        )
    );
    assert_eq!(permission(&state, 7, 40), Some((100, 1_000)));
    assert_eq!(permission(&state, 7, 41), Some((101, 2_000)));
    assert_eq!(permission(&state, 8, 40), Some((102, 3_000)));

    let types = [ADD, REVOKE];
    let principals = [7, 7];
    let resources = [40, 41];
    let roles = [199, 0];
    let granted = [9_000u32, 0];
    assert_eq!(
        OK,
        execute_u32(
            &mut vm,
            &mut state,
            &program,
            &[&types, &principals, &resources, &roles, &granted],
            true,
        )
    );
    assert_eq!(permission(&state, 7, 40), Some((199, 9_000)));
    assert_eq!(permission(&state, 7, 41), None);
    assert_eq!(permission(&state, 8, 40), Some((102, 3_000)));

    let rows: Vec<_> = StructMap2Slot::bind(&state, 0).iter(&state).collect();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|&(a, b, _)| (a, b) == (7, 40)));
    assert!(rows.iter().any(|&(a, b, _)| (a, b) == (8, 40)));
}

#[test]
fn collisions_and_same_first_lane_keep_full_pair_identity() {
    let program = map2_program(4, &[0], 3, &upsert_code(0, 1, &[(2, 0)]));
    let mut state = init(&program);
    let cap = StructMap2Slot::bind(&state, 0).capacity;
    let key1 = 77;
    let mut collision = None;
    for a in 0..100u32 {
        for b in (a + 1)..100u32 {
            if hash_key_pair(key1, a, cap) == hash_key_pair(key1, b, cap) {
                collision = Some((a, b));
                break;
            }
        }
        if collision.is_some() {
            break;
        }
    }
    let (key2a, key2b) = collision.expect("pair-hash collision for small table");
    let mut vm = Vm::default();
    let first = [key1, key1];
    let second = [key2a, key2b];
    let values = [10, 20];
    assert_eq!(
        OK,
        execute_u32(
            &mut vm,
            &mut state,
            &program,
            &[&first, &second, &values],
            false
        )
    );
    let slot = StructMap2Slot::bind(&state, 0);
    let row_a = pair_row(&state, key1, key2a).unwrap();
    let row_b = pair_row(&state, key1, key2b).unwrap();
    assert_ne!(row_a, row_b);
    assert_eq!(
        bytes::read_u32(&state, row_a + slot.field_offset(&state, 0)),
        10
    );
    assert_eq!(
        bytes::read_u32(&state, row_b + slot.field_offset(&state, 0)),
        20
    );
    assert!(pair_row(&state, key1, 999).is_none());
}

#[test]
fn growth_rehash_preserves_same_first_lane_pairs_and_checkpoint_forks() {
    let program = map2_program(2, &[0], 3, &upsert_code(0, 1, &[(2, 0)]));
    let mut state = init(&program);
    let mut vm = Vm::default();
    let first = [5, 5];
    let second = [10, 11];
    let values = [100, 101];
    assert_eq!(
        OK,
        execute_u32(
            &mut vm,
            &mut state,
            &program,
            &[&first, &second, &values],
            true
        )
    );

    let old_capacity = StructMap2Slot::bind(&state, 0).capacity;
    let grown_size = calculate_grown_state_size(&state, 0);
    let mut grown = vec![0u8; grown_size as usize];
    grow_state(&state, &mut grown, 0).unwrap();
    assert_eq!(StructMap2Slot::bind(&grown, 0).capacity, old_capacity * 2);
    assert_eq!(pair_row(&grown, 5, 10).is_some(), true);
    assert_eq!(pair_row(&grown, 5, 11).is_some(), true);

    let checkpoint = grown.clone();
    let mut fork = checkpoint.clone();
    let remove_program = map2_program(2, &[0], 2, &remove_code(0, 1));
    assert_eq!(
        OK,
        execute_u32(&mut vm, &mut fork, &remove_program, &[&[5], &[10]], true)
    );
    assert!(pair_row(&fork, 5, 10).is_none());
    assert!(pair_row(&checkpoint, 5, 10).is_some());
    let restored = checkpoint.clone();
    assert!(pair_row(&restored, 5, 10).is_some());
    assert!(pair_row(&restored, 5, 11).is_some());
}

#[test]
fn rollback_restores_update_remove_and_rejected_batch() {
    let program = map2_program(2, &[0], 3, &upsert_code(0, 1, &[(2, 0)]));
    let mut state = init(&program);
    let mut vm = Vm::default();
    assert_eq!(
        OK,
        execute_u32(&mut vm, &mut state, &program, &[&[9], &[1], &[10]], true)
    );

    vm.undo_enable(&state);
    let checkpoint = vm.undo_checkpoint();
    assert_eq!(
        OK,
        execute_u32(&mut vm, &mut state, &program, &[&[9], &[1], &[99]], true)
    );
    let remove_program = map2_program(2, &[0], 2, &remove_code(0, 1));
    assert_eq!(
        OK,
        execute_u32(&mut vm, &mut state, &remove_program, &[&[9], &[1]], true)
    );
    vm.undo_rollback(&mut state, checkpoint);
    let slot = StructMap2Slot::bind(&state, 0);
    let row = pair_row(&state, 9, 1).unwrap();
    assert_eq!(
        bytes::read_u32(&state, row + slot.field_offset(&state, 0)),
        10
    );
    assert_eq!(slot.size(&state), 1);
    assert_eq!(slot.capacity, 16);

    vm.undo_enable(&state);
    let checkpoint = vm.undo_checkpoint();
    let first = vec![9u32; 12];
    let second: Vec<u32> = (1..=12).collect();
    let values: Vec<u32> = (0..12).map(|value| value + 77).collect();
    assert_eq!(
        ErrorCode::NeedsGrowth as u32,
        execute_u32(
            &mut vm,
            &mut state,
            &program,
            &[&first, &second, &values],
            true
        )
    );
    vm.undo_rollback(&mut state, checkpoint);
    let slot = StructMap2Slot::bind(&state, 0);
    let row = pair_row(&state, 9, 1).unwrap();
    assert_eq!(
        bytes::read_u32(&state, row + slot.field_offset(&state, 0)),
        10
    );
    for key2 in 2..=12 {
        assert!(pair_row(&state, 9, key2).is_none());
    }
}

#[test]
fn reserved_first_lane_is_rejected_but_second_lane_is_exact_u32() {
    let program = map2_program(4, &[], 2, &upsert_code(0, 1, &[]));
    for reserved in [EMPTY_KEY, TOMBSTONE] {
        let mut state = init(&program);
        let before = state.clone();
        let mut vm = Vm::default();
        assert_eq!(
            ErrorCode::InvalidKey as u32,
            execute_u32(&mut vm, &mut state, &program, &[&[reserved], &[7]], false)
        );
        assert_eq!(state, before);
    }

    let mut state = init(&program);
    let mut vm = Vm::default();
    let first = [1, 1];
    let second = [EMPTY_KEY, TOMBSTONE];
    assert_eq!(
        OK,
        execute_u32(&mut vm, &mut state, &program, &[&first, &second], false)
    );
    assert!(pair_row(&state, 1, EMPTY_KEY).is_some());
    assert!(pair_row(&state, 1, TOMBSTONE).is_some());
}

#[test]
fn payload_free_graph_edges_and_unary_layout_remain_supported() {
    let edge_program = map2_program(4, &[], 2, &upsert_code(0, 1, &[]));
    let mut edges = init(&edge_program);
    let mut vm = Vm::default();
    let from = [1, 1, 2];
    let to = [2, 3, 3];
    assert_eq!(
        OK,
        execute_u32(&mut vm, &mut edges, &edge_program, &[&from, &to], false)
    );
    let edge_slot = StructMap2Slot::bind(&edges, 0);
    assert_eq!(edge_slot.size(&edges), 3);
    assert_eq!(edge_slot.row_size, 0);
    assert_eq!(edge_slot.iter(&edges).count(), 3);

    let unary_init = slot_init(Opcode::SlotStructMap as u8, SlotType::StructMap, 4, &[0]);
    let unary_reduce = vec![Opcode::BatchStructMapUpsertLast as u8, 0, 0, 1, 1, 0, 0];
    let unary_program = program(&unary_init, 2, &unary_reduce);
    let mut unary = init(&unary_program);
    let keys = [7];
    let values = [77];
    assert_eq!(
        OK,
        execute_u32(
            &mut vm,
            &mut unary,
            &unary_program,
            &[&keys, &values],
            false
        )
    );
    let unary_slot = StructMapSlot::bind(&unary, 0);
    assert_eq!(
        unary_slot.rows_base,
        unary_slot.slot_offset + unary_slot.descriptor_size + unary_slot.capacity * 4
    );
    assert!(unary_slot.find(&unary, 7).is_some());
    let meta_type = unary[(STATE_HEADER_SIZE + SlotMetaOffset::TYPE_FLAGS) as usize] & 0x0f;
    assert_eq!(meta_type, SlotType::StructMap as u8);
    assert_eq!(SLOT_META_SIZE, 48);
}
