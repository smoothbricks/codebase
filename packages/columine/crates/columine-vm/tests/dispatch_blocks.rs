//! The remaining vm_test.zig dispatch blocks: mixed FOR_EACH bodies (agg +
//! FLAT_MAP), depth-2 FLAT_MAP, LATEST with parent-timestamp inheritance,
//! ORDERED_LIST (LIST_APPEND / LIST_APPEND_STRUCT / undo / inside FLAT_MAP),
//! struct-map iteration + growth through dispatch, and array fields in
//! struct-map rows. Scenarios and expected values mirror the Zig blocks.

use columine_types::types::{ErrorCode, SLOT_META_SIZE, STATE_HEADER_SIZE, SlotMetaOffset, align8};
use columine_vm::bytes;
use columine_vm::state_init::{
    ARENA_HEADER_SIZE, calculate_grown_state_size, calculate_state_size, grow_state, init_state,
    struct_field_offset,
};
use columine_vm::vm::{
    Vm, f64s_as_bytes, u32s_as_bytes, vm_struct_map_get_row_ptr, vm_struct_map_iter_key,
    vm_struct_map_iter_next, vm_struct_map_iter_start,
};

const OK: u32 = ErrorCode::Ok as u32;
const NEEDS_GROWTH: u32 = ErrorCode::NeedsGrowth as u32;
const TYPE_A: u32 = 1001;

fn program(num_slots: u8, num_inputs: u8, init: &[u8], reduce: &[u8]) -> Vec<u8> {
    let mut prog = vec![0u8; 32];
    prog.extend([0x41, 0x58, 0x45, 0x31, 1, 0, num_slots, num_inputs, 0, 0]);
    prog.extend((init.len() as u16).to_le_bytes());
    prog.extend((reduce.len() as u16).to_le_bytes());
    prog.extend_from_slice(init);
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

fn flat_map(offsets_col: u8, parent_ts_col: u8, inner: &[u8]) -> Vec<u8> {
    let mut fm = vec![0xE1, offsets_col, parent_ts_col];
    fm.extend((inner.len() as u16).to_le_bytes());
    fm.extend_from_slice(inner);
    fm
}

fn init(prog: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(prog);
    assert!(size > 0);
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, prog).expect("init_state");
    state
}

fn assert_delta_roundtrip(vm: &mut Vm, before: &[u8], after: &[u8], checkpoint: u32) {
    let end = vm.undo_checkpoint();
    assert!(
        end > checkpoint,
        "mutation must emit paired journal entries"
    );
    assert_eq!(vm.delta_export_segment(checkpoint, end), end - checkpoint,);
    let undo = vm.delta_export_undo_bytes();
    let redo = vm.delta_export_redo_bytes();

    let mut production = after.to_vec();
    vm.undo_rollback(&mut production, checkpoint);
    assert_eq!(production, before);

    let mut replay = Vm::default();
    let mut rolled_back = after.to_vec();
    replay.delta_apply_rollback_segment(&mut rolled_back, &undo, Vm::delta_export_entry_size());
    assert_eq!(rolled_back, before);

    let mut rolled_forward = before.to_vec();
    replay.delta_apply_rollforward_segment(
        &mut rolled_forward,
        &redo,
        Vm::delta_export_entry_size(),
    );
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

fn struct_map_row(state: &[u8], slot: u8, key: u32) -> u32 {
    let meta_base = STATE_HEADER_SIZE + u32::from(slot) * SLOT_META_SIZE;
    let num_fields = u32::from(state[(meta_base + SlotMetaOffset::AGG_TYPE) as usize]);
    let row_size = u32::from(bytes::read_u16(
        state,
        meta_base + SlotMetaOffset::TTL_SECONDS,
    ));
    vm_struct_map_get_row_ptr(
        state,
        slot_offset(state, slot),
        slot_cap(state, slot),
        num_fields,
        row_size,
        key,
    )
}

// =============================================================================
// FLAT_MAP + event-level AGG_SUM (vm_test.zig:2748)
// =============================================================================

#[test]
fn flat_map_plus_event_level_agg_sum_in_same_for_each_block() {
    // Slot 0 STRUCT_MAP, slot 1 AGGREGATE SUM (SLOT_DEF cap_lo=0 quirk kept:
    // the Zig builder writes AggType byte 0 here and SUM semantics come from
    // the aggregate defaulting — faithful byte-for-byte).
    let mut init_sec = vec![0x18, 0, 6, 4, 0, 2, 0, 4];
    init_sec.extend([0x10, 1, 2, 0, 0]);

    let inner = [0x80u8, 0, 3, 2, 4, 0, 5, 1, 0];
    let mut body = vec![0x40, 1, 2]; // AGG_SUM slot=1 val_col=2
    body.extend(flat_map(1, 0xFF, &inner));
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
// Nested FLAT_MAP depth-2 (vm_test.zig:2895)
// =============================================================================

#[test]
fn nested_flat_map_depth_2_groups_items_struct_map() {
    let init_sec = [0x18u8, 0, 0x05, 4, 0, 2, 0, 4];
    let leaf = [0x80u8, 0, 3, 2, 4, 0, 5, 1, 0];
    let inner_fm = flat_map(2, 0xFF, &leaf);
    let outer_fm = flat_map(1, 0xFF, &inner_fm);
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
        let row = struct_map_row(&state, 0, key);
        assert_ne!(0xFFFF_FFFF, row);
        assert_eq!(v0, bytes::read_u32(&state, row + 1));
    }
}

// =============================================================================
// FLAT_MAP LATEST — parent timestamp overrides child ts_col (vm_test.zig:3044)
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
// ORDERED_LIST (vm_test.zig:3172-3462)
// =============================================================================

/// vm_test.zig:3097 `buildScalarListTestProgram`.
fn build_scalar_list_program(type_id: u32) -> Vec<u8> {
    let init_sec = [0x19u8, 0, 6, 8, 0, 0]; // SLOT_ORDERED_LIST, elem UINT32
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
    // vm_test.zig:3272 `buildStructListTestProgram` — ORDERED_LIST with
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
fn ordered_list_undo_restores_count() {
    let prog = build_scalar_list_program(TYPE_A);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let types = [TYPE_A, TYPE_A];
    let vals = [100u32, 200];
    let cols: Vec<&[u8]> = vec![u32s_as_bytes(&types), u32s_as_bytes(&vals)];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 2));
    assert_eq!(2, slot_size(&state, 0));

    vm.undo_enable(&state);
    let cp = vm.undo_checkpoint();
    let t2 = [TYPE_A];
    let v2 = [300u32];
    let cols2: Vec<&[u8]> = vec![u32s_as_bytes(&t2), u32s_as_bytes(&v2)];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols2, 1));
    assert_eq!(3, slot_size(&state, 0));

    vm.undo_rollback(&mut state, cp);
    assert_eq!(2, slot_size(&state, 0));
}

#[test]
fn ordered_list_inside_flat_map() {
    let init_sec = [0x19u8, 0, 7, 8, 0, 0]; // scalar UINT32 ordered list
    let inner = [0x84u8, 0, 2];
    let fm = flat_map(1, 0xFF, &inner);
    let prog = program(1, 3, &init_sec, &for_each(TYPE_A, &fm));

    let mut state = init(&prog);
    let mut vm = Vm::default();

    let types = [TYPE_A, TYPE_A];
    let offsets = [0u32, 2, 3];
    let child_vals = [10u32, 20, 30];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&offsets),
        u32s_as_bytes(&child_vals),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 2));

    assert_eq!(3, slot_size(&state, 0));
    let off = slot_offset(&state, 0);
    assert_eq!(10, bytes::read_u32(&state, off));
    assert_eq!(20, bytes::read_u32(&state, off + 4));
    assert_eq!(30, bytes::read_u32(&state, off + 8));
}

// =============================================================================
// Struct map iteration + growth through dispatch (vm_test.zig:1038, 1091)
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

    let off = slot_offset(&state, 0);
    let cap = slot_cap(&state, 0);
    let num_fields = u32::from(state[(STATE_HEADER_SIZE + SlotMetaOffset::AGG_TYPE) as usize]);

    let mut found = Vec::new();
    let mut pos = vm_struct_map_iter_start(&state, off, cap, num_fields);
    while pos < cap {
        found.push(vm_struct_map_iter_key(&state, off, num_fields, pos));
        pos = vm_struct_map_iter_next(&state, off, cap, num_fields, pos);
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

    let grown_size = calculate_grown_state_size(&state, 0);
    let mut new_state = vec![0u8; grown_size as usize];
    grow_state(&state, &mut new_state, 0).expect("grow_state");

    assert_eq!(32, slot_cap(&new_state, 0));
    for i in 0..11u32 {
        let row = struct_map_row(&new_state, 0, i + 1000);
        assert_ne!(0xFFFF_FFFF, row);
        assert_eq!(i * 10, bytes::read_u32(&new_state, row + 1));
        assert_eq!(i + 5000, bytes::read_u32(&new_state, row + 5));
    }

    assert_eq!(OK, vm.execute_batch(&mut new_state, &prog, &cols12, 1));
    let row12 = struct_map_row(&new_state, 0, 2000);
    assert_ne!(0xFFFF_FFFF, row12);
    assert_eq!(999, bytes::read_u32(&new_state, row12 + 1));
}

// =============================================================================
// Array fields in struct-map rows (vm_test.zig:3640-3917)
// =============================================================================

/// vm_test.zig:3556 `buildArrayFieldTestProgram`.
fn build_array_field_program(type_id: u32) -> Vec<u8> {
    let init_sec = [0x18u8, 0, 6, 4, 0, 2, 0, 5]; // UINT32 + ARRAY_U32
    let body = [0x80u8, 0, 1, 1, 2, 0, 1, 3, 4, 1];
    program(1, 5, &init_sec, &for_each(type_id, &body))
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
    let descriptor: Vec<u8> = state[off as usize..(off + u32::from(num_fields)) as usize].to_vec();
    let row = struct_map_row(state, 0, key);
    assert_ne!(0xFFFF_FFFF, row, "row for key {key} missing");
    let scalar_off = struct_field_offset(num_fields, &descriptor, 0);
    let arr_off = struct_field_offset(num_fields, &descriptor, 1);
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
        assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 1));
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
}

// =============================================================================
// Nested slots through the full VM pipeline (nested.zig:647-733 e2e blocks,
// deferred by the struct_map/nested slice until the dispatch loop existed)
// =============================================================================

use columine_types::types::SlotType;
use columine_vm::meta::SlotMetaView;
use columine_vm::nested::{
    arena_header_offset, get_inner_offset, get_inner_set_size, inner_agg_get_count,
    inner_agg_get_f64, inner_map_get, inner_set_contains, read_nested_prefix,
};

/// nested.zig `buildNestedSetE2EProgram`.
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
    vm.undo_enable(&state);
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
    vm.undo_enable(&state);
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

    vm.undo_enable(&state);
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

// =============================================================================
// No-timestamp hashmap rejections through dispatch (vm_test.zig:325)
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

/// Parity regression (scenario-delta-fork.test.ts "struct keepValue(first)"):
/// 0x81 was missing from the Rust body length table, body arm, top-level arm,
/// and BOTH opcode registries (types.rs and opcodes.rs) — opcodes.zig:236 has
/// it, and vm.zig dispatches it at 1774 (top level) and 2747/2775 (body).
/// The body pass's unknown-byte skip-1 misparsed the operand bytes as opcodes
/// and the whole upsert vanished: the TS registry read back empty. Program
/// bytes are the exact TS-compiled flatMap→mapRecord→keyBy('key')
/// →keepValue(first('quantity')) capture (hash prefix included): FOR_EACH
/// (col=1, id=1) wrapping FLAT_MAP(offsets_col=3) wrapping
/// `81 00 04 03 (04,00)(05,01)(06,02) 00`.
#[test]
fn ts_struct_map_upsert_first_keeps_first_row_and_rolls_back() {
    let hex = "0000000000000000000000000000000000000000000000000000000000000000\
               4158453101000100000009001a00180006000403040400e001010100000010\
               00e103ff0b00810004030400050106020000";
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
    vm.undo_enable(&state);
    let cp = vm.undo_checkpoint();
    assert_eq!(OK, vm.execute_batch_delta(&mut state, &prog, &cols, 1));

    let read_row = |state: &[u8]| -> Option<(u32, u32, u32)> {
        let meta_base = STATE_HEADER_SIZE;
        let off = slot_offset(state, 0);
        let num_fields = state[(meta_base + SlotMetaOffset::AGG_TYPE) as usize];
        let descriptor: Vec<u8> =
            state[off as usize..(off + u32::from(num_fields)) as usize].to_vec();
        let row = struct_map_row(state, 0, 42);
        if row == 0xFFFF_FFFF {
            return None;
        }
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
    vm.undo_rollback(&mut state, cp);
    vm.undo_commit();
    assert_eq!(
        None,
        read_row(&state),
        "rollback must remove the first-write row"
    );
}
