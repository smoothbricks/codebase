//! The write sequence (`specs/columine/07-change-feed.md` §6): every write
//! that sets a slot's change flag advances the slot's `WRITE_SEQ` by one in
//! the same write, a write that changes nothing observable advances nothing,
//! growth copies it, a seal leaves it, and the journal restores it: through
//! the change-flag boundary's one 8-byte record, or through a direct
//! capture that names it beside its flag byte.

use columine_types::PROGRAM_MAGIC;
use columine_types::types::{ChangeFlag, ErrorCode, Opcode, SlotMetaOffset, SlotType};
use columine_vm::hashmap_ops::{CmpType, Strategy};
use columine_vm::key_index::NoStrings;
use columine_vm::meta::{SlotMetaView, mark_ranges, slot_meta_base};
use columine_vm::state_init::{
    DEFAULT_ACCEPTED_PROGRAM_MAGICS, GrowthRoom, calculate_grown_state_size, calculate_state_size,
    grow_state, init_state,
};
use columine_vm::vm::Vm;

const MAP: u32 = 0;
const SET: u32 = 1;
const BITMAP: u32 = 2;

fn program() -> Vec<u8> {
    let mut init = Vec::new();
    for (slot, kind) in [
        (MAP, SlotType::HashMap),
        (SET, SlotType::HashSet),
        (BITMAP, SlotType::Bitmap),
    ] {
        init.extend([Opcode::SlotDef as u8, slot as u8, kind as u8, 16, 0]);
    }
    init.push(Opcode::Halt as u8);
    let mut out = vec![0u8; 32];
    out.extend(PROGRAM_MAGIC.to_le_bytes());
    out.extend([2, 0, 0, 0]); // version 2, no callbacks, no flags
    out.extend(3u32.to_le_bytes());
    out.extend(1u32.to_le_bytes());
    out.extend(u32::try_from(init.len()).unwrap().to_le_bytes());
    out.extend(0u32.to_le_bytes());
    out.extend_from_slice(&init);
    out
}

fn image() -> Vec<u8> {
    let code = program();
    let size = calculate_state_size(&code, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("admitted");
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, &code, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("init");
    state
}

fn seq_offset(slot: u32) -> u32 {
    slot_meta_base(slot) + SlotMetaOffset::WRITE_SEQ
}

fn seq(state: &[u8], slot: u32) -> u64 {
    let at = seq_offset(slot) as usize;
    u64::from_le_bytes(state[at..at + 8].try_into().unwrap())
}

fn flags(state: &[u8], slot: u32) -> u8 {
    state[(slot_meta_base(slot) + SlotMetaOffset::CHANGE_FLAGS) as usize]
}

fn upsert(vm: &mut Vm, state: &mut [u8], keys: &[u32], vals: &[u32]) -> ErrorCode {
    vm.map_upsert(
        Strategy::Last,
        false,
        state,
        MAP,
        keys,
        vals,
        None,
        CmpType::U32,
    )
}

/// The journal's records of `len` bytes at `offset`.
fn records_at(vm: &Vm, offset: u32, len: u8) -> usize {
    vm.undo_records()
        .expect("recording window")
        .iter()
        .filter(|entry| entry.key == offset && entry.pad1 == len)
        .count()
}

#[test]
fn a_write_advances_its_slot_sequence_by_one() {
    let mut state = image();
    let mut vm = Vm::default();
    assert_eq!(
        [seq(&state, MAP), seq(&state, SET), seq(&state, BITMAP)],
        [0; 3]
    );

    assert_eq!(upsert(&mut vm, &mut state, &[5], &[10]), ErrorCode::Ok);
    assert_eq!(seq(&state, MAP), 1);
    assert_eq!(upsert(&mut vm, &mut state, &[5], &[11]), ErrorCode::Ok);
    assert_eq!(seq(&state, MAP), 2);
    assert_eq!(vm.map_remove(false, &mut state, MAP, &[5]), ErrorCode::Ok);
    assert_eq!(seq(&state, MAP), 3);
    // An absent key removes nothing and writes nothing.
    assert_eq!(vm.map_remove(false, &mut state, MAP, &[5]), ErrorCode::Ok);
    assert_eq!(seq(&state, MAP), 3);

    assert_eq!(
        vm.set_insert(false, &mut state, SET, &[3], None),
        ErrorCode::Ok
    );
    assert_eq!(seq(&state, SET), 1);
    // A held member inserts nothing.
    assert_eq!(
        vm.set_insert(false, &mut state, SET, &[3], None),
        ErrorCode::Ok
    );
    assert_eq!(seq(&state, SET), 1);
    assert_eq!(vm.set_remove(false, &mut state, SET, &[3]), ErrorCode::Ok);
    assert_eq!(seq(&state, SET), 2);

    assert_eq!(
        vm.set_insert(false, &mut state, BITMAP, &[7], None),
        ErrorCode::Ok
    );
    assert_eq!(seq(&state, BITMAP), 1);
    assert_eq!(
        vm.set_remove(false, &mut state, BITMAP, &[7]),
        ErrorCode::Ok
    );
    assert_eq!(seq(&state, BITMAP), 2);

    // Each slot counts its own writes.
    assert_eq!(seq(&state, MAP), 3);
    assert_eq!(seq(&state, SET), 2);
}

#[test]
fn an_equal_hash_map_value_sets_no_flag_and_advances_nothing() {
    let mut state = image();
    let mut vm = Vm::default();
    assert_eq!(upsert(&mut vm, &mut state, &[5], &[10]), ErrorCode::Ok);
    state[(slot_meta_base(MAP) + SlotMetaOffset::CHANGE_FLAGS) as usize] = 0;
    assert_eq!(upsert(&mut vm, &mut state, &[5], &[10]), ErrorCode::Ok);
    assert_eq!(
        flags(&state, MAP),
        0,
        "an equal value changes nothing a reader sees"
    );
    assert_eq!(seq(&state, MAP), 1);
}

#[test]
fn a_rolled_back_attempt_restores_the_sequence_through_one_boundary_record() {
    let mut state = image();
    let mut vm = Vm::default();
    assert_eq!(upsert(&mut vm, &mut state, &[5], &[10]), ErrorCode::Ok);
    let before = state.clone();
    vm.undo_enable(&state).expect("window");
    let checkpoint = vm.undo_checkpoint();
    // Two rows in one boundary: the boundary owns the sequence, so no row
    // capture journals it and its close journals it once.
    assert_eq!(
        upsert(&mut vm, &mut state, &[6, 7], &[20, 30]),
        ErrorCode::Ok
    );
    assert!(seq(&state, MAP) > 1);
    assert_eq!(records_at(&vm, seq_offset(MAP), 8), 1);
    assert_eq!(upsert(&mut vm, &mut state, &[5], &[11]), ErrorCode::Ok);
    assert_eq!(records_at(&vm, seq_offset(MAP), 8), 2);
    vm.undo_rollback(&mut state, checkpoint).expect("rollback");
    assert_eq!(seq(&state, MAP), 1);
    assert_eq!(state, before);
    vm.undo_commit();
}

#[test]
fn a_direct_capture_outside_a_boundary_journals_the_sequence() {
    let mut state = image();
    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("window");
    let checkpoint = vm.undo_checkpoint();
    let ranges = mark_ranges(slot_meta_base(MAP));
    vm.capture_mutation(false, &mut state, &ranges, |state| {
        SlotMetaView::read(state, MAP).mark(state, ChangeFlag::UPDATED);
    })
    .expect("captured");
    assert_eq!(seq(&state, MAP), 1);
    assert_eq!(flags(&state, MAP), ChangeFlag::UPDATED);
    assert_eq!(records_at(&vm, seq_offset(MAP), 8), 1);
    vm.undo_rollback(&mut state, checkpoint).expect("rollback");
    assert_eq!(seq(&state, MAP), 0);
    assert_eq!(flags(&state, MAP), 0);
    vm.undo_commit();
}

#[test]
fn growth_copies_the_sequence_and_a_seal_leaves_it() {
    let mut state = image();
    let mut vm = Vm::default();
    assert_eq!(upsert(&mut vm, &mut state, &[5], &[10]), ErrorCode::Ok);
    assert_eq!(upsert(&mut vm, &mut state, &[6], &[20]), ErrorCode::Ok);
    assert_eq!(
        vm.set_insert(false, &mut state, SET, &[3], None),
        ErrorCode::Ok
    );
    let size = calculate_grown_state_size(&state, MAP, GrowthRoom::Rows).expect("grown size");
    let mut grown = vec![0u8; size as usize];
    grow_state(&state, &mut grown, MAP, GrowthRoom::Rows).expect("grow");
    assert_eq!(seq(&grown, MAP), 2);
    assert_eq!(seq(&grown, SET), 1);
    vm.seal_state(&mut grown, &NoStrings).expect("seal");
    assert_eq!(seq(&grown, MAP), 2);
    assert_eq!(seq(&grown, SET), 1);
}
