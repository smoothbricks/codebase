//! Physical undo/redo coverage through actual VM container mutations.
//!
//! Complete images, including dead cells and unused auxiliary storage, are the
//! oracle. Logical membership models additionally check every generated step.

use columine_types::types::{
    DERIVED_FACT_EMPTY_IDENTITY, DERIVED_FACT_TOMBSTONE_IDENTITY, EMPTY_KEY, ErrorCode,
    PROGRAM_MAGIC, STATE_FORMAT_VERSION, STATE_HEADER_SIZE, STATE_MAGIC, SlotMetaOffset, SlotType,
    StateHeaderOffset,
};
use columine_vm::bytes;
use columine_vm::hashmap_ops::{CmpType, Strategy, bind_slot_map};
use columine_vm::hashset_ops::bind_slot_set;
use columine_vm::meta::SlotMetaView;
use columine_vm::state_init::{DEFAULT_ACCEPTED_PROGRAM_MAGICS, calculate_state_size, init_state};
use columine_vm::undo_log::{FLAT_UNDO_ENTRY_SIZE, FlatUndoEntry, FlatUndoOp};
use columine_vm::vm::{
    UNDO_CAPACITY, Vm, f64s_as_bytes, u32s_as_bytes, write_derived_facts_header,
};
use proptest::prelude::*;
use proptest::strategy::Strategy as PropStrategy;
use std::collections::{BTreeMap, BTreeSet};

const SLOT_OFFSET: u32 = STATE_HEADER_SIZE + 48;

fn mk_slot_state(cap: u32, kind: SlotType) -> Vec<u8> {
    let mut state = vec![0xa5; 4096];
    state[..SLOT_OFFSET as usize].fill(0);
    bytes::write_u32(&mut state, StateHeaderOffset::NUM_SLOTS, 1);
    bytes::write_u32(&mut state, StateHeaderOffset::MAGIC, STATE_MAGIC);
    state[StateHeaderOffset::FORMAT_VERSION as usize] = STATE_FORMAT_VERSION;
    bytes::write_u32(
        &mut state,
        STATE_HEADER_SIZE + SlotMetaOffset::OFFSET,
        SLOT_OFFSET,
    );
    bytes::write_u32(
        &mut state,
        STATE_HEADER_SIZE + SlotMetaOffset::CAPACITY,
        cap,
    );
    state[(STATE_HEADER_SIZE + SlotMetaOffset::TYPE_FLAGS) as usize] = kind as u8;
    for pos in 0..cap {
        bytes::write_u32(&mut state, SLOT_OFFSET + pos * 4, EMPTY_KEY);
        if kind == SlotType::HashMap {
            bytes::write_u64(
                &mut state,
                SLOT_OFFSET + cap * 8 + pos * 8,
                f64::NEG_INFINITY.to_bits(),
            );
        }
    }
    state
}

fn map_upsert(vm: &mut Vm, state: &mut [u8], key: u32, value: u32, delta: bool) -> ErrorCode {
    vm.map_upsert(
        Strategy::Last,
        delta,
        state,
        0,
        &[key],
        &[value],
        None,
        CmpType::F64,
    )
}

fn map_remove(vm: &mut Vm, state: &mut [u8], key: u32, delta: bool) -> ErrorCode {
    vm.map_remove(delta, state, 0, &[key])
}

fn set_mutate(vm: &mut Vm, state: &mut [u8], key: u32, insert: bool, delta: bool) -> ErrorCode {
    if insert {
        vm.set_insert(delta, state, 0, &[key], None)
    } else {
        vm.set_remove(delta, state, 0, &[key])
    }
}

fn assert_roundtrip(vm: &mut Vm, state: &mut [u8], before: &[u8], checkpoint: u32, delta: bool) {
    let after = state.to_vec();
    if delta {
        let end = vm.undo_checkpoint();
        assert_eq!(
            Ok(end - checkpoint),
            vm.delta_export_segment(checkpoint, end)
        );
        let undo = vm.delta_export_undo().to_vec();
        let redo = vm.delta_export_redo().to_vec();
        let mut replay = Vm::default();
        let mut undone = after.clone();
        replay
            .delta_apply_rollback_segment(&mut undone, &undo, FLAT_UNDO_ENTRY_SIZE)
            .expect("actual undo segment");
        assert_eq!(undone, before, "undo restores every byte");
        let mut redone = before.to_vec();
        replay
            .delta_apply_rollforward_segment(&mut redone, &redo, FLAT_UNDO_ENTRY_SIZE)
            .expect("actual redo segment");
        assert_eq!(redone, after, "redo restores every byte");
    }
    vm.undo_rollback(state, checkpoint)
        .expect("actual checkpoint");
    assert_eq!(state, before, "production rollback restores every byte");
}

#[test]
fn map_insert_update_and_delete_restore_the_complete_image_on_both_lanes() {
    for delta in [false, true] {
        for operation in 0..3 {
            let mut state = mk_slot_state(16, SlotType::HashMap);
            let mut vm = Vm::default();
            if operation != 0 {
                assert_eq!(
                    map_upsert(&mut vm, &mut state, 42, 100, delta),
                    ErrorCode::Ok
                );
            }
            let before = state.clone();
            vm.undo_enable(&state).expect("undo window opens");
            let checkpoint = vm.undo_checkpoint();
            let status = if operation == 2 {
                map_remove(&mut vm, &mut state, 42, delta)
            } else {
                map_upsert(&mut vm, &mut state, 42, 200, delta)
            };
            assert_eq!(status, ErrorCode::Ok);
            let meta = SlotMetaView::read(&state, 0);
            assert_eq!(
                bind_slot_map(&meta).get_u32(&state, 42),
                if operation == 2 { None } else { Some(200) }
            );
            assert_roundtrip(&mut vm, &mut state, &before, checkpoint, delta);
        }
    }
}

#[test]
fn set_insert_and_delete_restore_stale_dead_cells_on_both_lanes() {
    for delta in [false, true] {
        for insert in [false, true] {
            let mut state = mk_slot_state(16, SlotType::HashSet);
            let mut vm = Vm::default();
            if !insert {
                assert_eq!(
                    set_mutate(&mut vm, &mut state, 42, true, delta),
                    ErrorCode::Ok
                );
            }
            let before = state.clone();
            vm.undo_enable(&state).expect("undo window opens");
            let checkpoint = vm.undo_checkpoint();
            assert_eq!(
                set_mutate(&mut vm, &mut state, 42, insert, delta),
                ErrorCode::Ok
            );
            assert_eq!(
                bind_slot_set(&SlotMetaView::read(&state, 0)).contains(&state, 42),
                insert
            );
            assert_roundtrip(&mut vm, &mut state, &before, checkpoint, delta);
        }
    }
}

#[test]
fn absent_removal_does_not_change_primary_storage_or_create_physical_entries() {
    for kind in [SlotType::HashMap, SlotType::HashSet] {
        let mut state = mk_slot_state(16, kind);
        let before = state.clone();
        let mut vm = Vm::default();
        vm.undo_enable(&state).expect("undo window opens");
        let status = if kind == SlotType::HashMap {
            map_remove(&mut vm, &mut state, 7, true)
        } else {
            set_mutate(&mut vm, &mut state, 7, false, true)
        };
        assert_eq!(status, ErrorCode::Ok);
        assert_eq!(state, before);
        assert_eq!(vm.undo_checkpoint(), 0);
    }
}

#[test]
fn aggregate_value_and_counts_above_u32_max_roundtrip_without_narrowing() {
    for delta in [false, true] {
        for count_only in [false, true] {
            let mut state = mk_slot_state(0, SlotType::Aggregate);
            let count_offset = SLOT_OFFSET + if count_only { 0 } else { 8 };
            let big = (1u64 << 33) + 7;
            bytes::write_u64(&mut state, count_offset, big);
            if !count_only {
                bytes::write_f64(&mut state, SLOT_OFFSET, 10.0);
            }
            let before = state.clone();
            let mut vm = Vm::default();
            vm.undo_enable(&state).expect("undo window opens");
            let ranges = [(SLOT_OFFSET, if count_only { 8 } else { 16 })];
            vm.capture_mutation(delta, &mut state, &ranges, |image| {
                bytes::write_u64(image, count_offset, big + 1);
                if !count_only {
                    bytes::write_f64(image, SLOT_OFFSET, 25.0);
                }
            })
            .expect("physical aggregate authority");
            assert_eq!(bytes::read_u64(&state, count_offset), big + 1);
            assert_roundtrip(&mut vm, &mut state, &before, 0, delta);
            assert_eq!(bytes::read_u64(&state, count_offset), big);
        }
    }
}

#[derive(Clone, Debug)]
enum MapOp {
    Upsert { key: u32, value: u32, ts: f64 },
    Delete { key: u32 },
}

proptest! {
    #[test]
    fn map_rollback_and_replay_restore_full_images_and_model(
        ops in prop::collection::vec(
            prop_oneof![
                (1u32..=8, 0u32..1000, prop::num::f64::NORMAL).prop_map(|(key, value, ts)| MapOp::Upsert { key, value, ts }),
                (1u32..=8).prop_map(|key| MapOp::Delete { key }),
            ], 0..24,
        ),
    ) {
        for delta in [false, true] {
            let mut state = mk_slot_state(16, SlotType::HashMap);
            let mut vm = Vm::default();
            let mut model = BTreeMap::new();
            for key in 1u32..=4 {
                prop_assert_eq!(map_upsert(&mut vm, &mut state, key, key * 10, delta), ErrorCode::Ok);
                let meta = SlotMetaView::read(&state, 0);
                let pos = bind_slot_map(&meta).find(&state, key).expect("seeded key");
                bytes::write_f64(&mut state, SLOT_OFFSET + meta.capacity * 8 + pos * 8, f64::from(key));
                model.insert(key, (key * 10, f64::from(key).to_bits()));
            }
            let before = state.clone();
            vm.undo_enable(&state).expect("undo window opens");
            for op in &ops {
                match *op {
                    MapOp::Upsert { key, value, ts } => {
                        let cmp = ts.to_le_bytes();
                        let status = vm.map_upsert(Strategy::Latest, delta, &mut state, 0, &[key], &[value], Some(&cmp), CmpType::F64);
                        prop_assert_eq!(status, ErrorCode::Ok);
                        if model.get(&key).is_none_or(|&(_, bits)| ts > f64::from_bits(bits)) {
                            model.insert(key, (value, ts.to_bits()));
                        }
                    }
                    MapOp::Delete { key } => {
                        prop_assert_eq!(map_remove(&mut vm, &mut state, key, delta), ErrorCode::Ok);
                        model.remove(&key);
                    }
                }
                let meta = SlotMetaView::read(&state, 0);
                let table = bind_slot_map(&meta);
                let actual: BTreeMap<_, _> = table.iter_live(&state).map(|(pos, key)| {
                    (key, (table.entry_u32_at(&state, pos), bytes::read_u64(&state, SLOT_OFFSET + meta.capacity * 8 + pos * 8)))
                }).collect();
                prop_assert_eq!(&actual, &model);
                prop_assert_eq!(table.size(&state) as usize, model.len());
            }
            assert_roundtrip(&mut vm, &mut state, &before, 0, delta);
        }
    }

    #[test]
    fn set_rollback_and_replay_restore_full_images_and_model(
        ops in prop::collection::vec((1u32..=8, prop::bool::ANY), 0..24),
    ) {
        for delta in [false, true] {
            let mut state = mk_slot_state(16, SlotType::HashSet);
            let mut vm = Vm::default();
            let mut model = BTreeSet::new();
            for key in 1u32..=4 {
                prop_assert_eq!(set_mutate(&mut vm, &mut state, key, true, delta), ErrorCode::Ok);
                model.insert(key);
            }
            let before = state.clone();
            vm.undo_enable(&state).expect("undo window opens");
            for &(key, insert) in &ops {
                prop_assert_eq!(set_mutate(&mut vm, &mut state, key, insert, delta), ErrorCode::Ok);
                if insert { model.insert(key); } else { model.remove(&key); }
                let table = bind_slot_set(&SlotMetaView::read(&state, 0));
                let actual: BTreeSet<_> = table.iter_live(&state).map(|(_, key)| key).collect();
                prop_assert_eq!(&actual, &model);
                prop_assert_eq!(table.size(&state) as usize, model.len());
            }
            assert_roundtrip(&mut vm, &mut state, &before, 0, delta);
        }
    }

    #[test]
    fn physical_entry_wire_round_trip(
        len in 1u8..=8,
        offset in prop::num::u32::ANY,
        value in prop::num::u64::ANY,
    ) {
        let entry = FlatUndoEntry { op: FlatUndoOp::StateBytes, slot: 0, pad1: len, pad2: 0, key: offset, prev_value: 0, aux: value };
        let mut bytes = [0u8; FLAT_UNDO_ENTRY_SIZE as usize];
        entry.write_to(&mut bytes);
        prop_assert_eq!(FlatUndoEntry::read_from(&bytes), Some(entry));
    }
}

#[test]
fn logical_wire_operations_are_rejected_instead_of_decoded_as_compatibility() {
    for op in 1..15 {
        let mut bytes = [0u8; FLAT_UNDO_ENTRY_SIZE as usize];
        bytes[0] = op;
        assert!(
            FlatUndoEntry::read_from(&bytes).is_none(),
            "obsolete logical operation {op}"
        );
    }
}

/// Raw wire parsing treats an all-zero row as a no-op. That rule applies to
/// untrusted bytes only: paired exports never emit such a row, so this proves
/// nothing about stream completeness (see the mixed-lane oracle below).
#[test]
fn raw_wire_zero_rows_parse_as_noops_outside_paired_export() {
    let mut state = mk_slot_state(0, SlotType::Aggregate);
    bytes::write_u64(&mut state, SLOT_OFFSET, 1);
    let entry = FlatUndoEntry {
        op: FlatUndoOp::StateBytes,
        slot: 0,
        pad1: 8,
        pad2: 0,
        key: SLOT_OFFSET,
        prev_value: 0,
        aux: 7,
    };
    let mut encoded = [0u8; FLAT_UNDO_ENTRY_SIZE as usize];
    entry.write_to(&mut encoded);
    let mut segment = vec![0u8; 3 * FLAT_UNDO_ENTRY_SIZE as usize];
    segment[..24].copy_from_slice(&encoded);
    segment[48..].copy_from_slice(&encoded);
    Vm::default()
        .delta_apply_rollforward_segment(&mut state, &segment, FLAT_UNDO_ENTRY_SIZE)
        .expect("raw wire segment with zero rows parses");
    assert_eq!(bytes::read_u64(&state, SLOT_OFFSET), 7);
}

/// Program: one TTL HASHMAP (capacity 16, ttl 10, timestamp column 2) written by
/// `BATCH_MAP_UPSERT_LATEST` over key, value and timestamp columns.
fn ttl_map_program() -> Vec<u8> {
    let mut init: Vec<u8> = vec![0x10, 0, 0x10, 16, 0];
    init.extend(10.0f32.to_le_bytes());
    init.extend(0.0f32.to_le_bytes());
    init.extend([2, 0]);
    program(1, 3, &init, &[0x20, 0, 0, 1, 2, 1])
}

/// Program sections are length-prefixed and terminated exactly as the dispatch
/// fixtures build them.
fn program(num_slots: u32, num_inputs: u32, init: &[u8], reduce: &[u8]) -> Vec<u8> {
    let mut init = init.to_vec();
    init.push(0);
    let mut reduce = reduce.to_vec();
    reduce.push(0);
    let mut prog = vec![0u8; 32];
    prog.extend(PROGRAM_MAGIC.to_le_bytes());
    prog.extend([2, 0, 0, 0]);
    prog.extend(num_slots.to_le_bytes());
    prog.extend(num_inputs.to_le_bytes());
    prog.extend(
        u32::try_from(init.len())
            .expect("init section fits u32")
            .to_le_bytes(),
    );
    prog.extend(
        u32::try_from(reduce.len())
            .expect("reduce section fits u32")
            .to_le_bytes(),
    );
    prog.extend(init);
    prog.extend(reduce);
    prog
}

fn init_ttl_state(prog: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS)
        .expect("valid program admission");
    let mut state = vec![0u8; usize::try_from(size).expect("state size fits memory")];
    init_state(&mut state, prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("init_state");
    state
}

fn ttl_get(state: &[u8], key: u32) -> Option<u32> {
    let meta = SlotMetaView::read(state, 0);
    let table = bind_slot_map(&meta);
    table.get_u32(state, key)
}

/// One paired TTL insert, then a real expiry on the ordinary undo lane in the
/// same window. The complete export is refused before any cursor, count, byte or
/// overflow state changes; the earlier paired export keeps every byte. The
/// paired-expiry control exports the full window and replays it exactly.
#[test]
fn mixed_lane_ttl_window_refuses_full_export_and_keeps_the_previous_one() {
    let prog = ttl_map_program();
    let keys = [42u32];
    let values = [7u32];
    let timestamps = [100.0f64];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&keys),
        u32s_as_bytes(&values),
        f64s_as_bytes(&timestamps),
    ];

    let mut state = init_ttl_state(&prog);
    let before = state.clone();
    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("undo window opens");
    assert_eq!(
        ErrorCode::Ok as u32,
        vm.execute_batch_delta(&mut state, &prog, &cols, 1)
    );
    assert_eq!(Some(7), ttl_get(&state, 42));
    let paired_end = vm.undo_checkpoint();
    assert!(paired_end > 0, "the TTL insert journals paired entries");
    assert_eq!(Ok(paired_end), vm.delta_export_segment(0, paired_end));
    let previous_undo = vm.delta_export_undo().to_vec();
    let previous_redo = vm.delta_export_redo().to_vec();
    let previous_len = vm.delta_export_len_bytes();
    let previous_overflow = vm.delta_export_overflow();

    // Real expiry on the undo-only lane: 100 + ttl 10 <= 111.
    assert_eq!(Ok(1), vm.evict_all_expired(false, &mut state, 111.0));
    assert_eq!(None, ttl_get(&state, 42));
    let mixed_end = vm.undo_checkpoint();
    assert!(mixed_end > paired_end, "expiry journals undo-only entries");

    assert_eq!(
        Err(ErrorCode::DeltaLaneMismatch),
        vm.delta_export_segment(0, mixed_end)
    );
    assert_eq!(previous_undo, vm.delta_export_undo().to_vec());
    assert_eq!(previous_redo, vm.delta_export_redo().to_vec());
    assert_eq!(previous_len, vm.delta_export_len_bytes());
    assert_eq!(previous_overflow, vm.delta_export_overflow());

    // The paired prefix remains the exact previous export.
    let paired = vm.undo_status().delta_count;
    assert_eq!(
        paired_end, paired,
        "undo-only entries never advance the paired prefix"
    );
    assert_eq!(Ok(paired), vm.delta_export_segment(0, paired));
    assert_eq!(previous_undo, vm.delta_export_undo().to_vec());
    assert_eq!(previous_redo, vm.delta_export_redo().to_vec());

    vm.undo_rollback(&mut state, 0)
        .expect("the live journal still rolls back through the refusal");
    assert_eq!(before, state);

    // Paired control: the same window expires on the delta lane, so its complete
    // export is exact and replays in both directions.
    let mut control = init_ttl_state(&prog);
    let control_before = control.clone();
    let mut control_vm = Vm::default();
    control_vm.undo_enable(&control).expect("undo window opens");
    let checkpoint = control_vm.undo_checkpoint();
    assert_eq!(
        ErrorCode::Ok as u32,
        control_vm.execute_batch_delta(&mut control, &prog, &cols, 1)
    );
    assert_eq!(
        Ok(1),
        control_vm.evict_all_expired(true, &mut control, 111.0)
    );
    assert_eq!(None, ttl_get(&control, 42));
    assert_roundtrip(
        &mut control_vm,
        &mut control,
        &control_before,
        checkpoint,
        true,
    );
}

/// A closure's own error is a value, not a capture refusal: the bytes it already
/// wrote stay journaled, so the whole prefix rolls back exactly.
#[test]
fn capture_closure_error_keeps_the_written_prefix_journaled() {
    for delta in [false, true] {
        let mut state = mk_slot_state(0, SlotType::Aggregate);
        bytes::write_u64(&mut state, SLOT_OFFSET, 1);
        let before = state.clone();
        let mut vm = Vm::default();
        vm.undo_enable(&state).expect("undo window opens");
        let checkpoint = vm.undo_checkpoint();
        let ranges = [(SLOT_OFFSET, 8)];
        let outcome = vm
            .capture_mutation(delta, &mut state, &ranges, |image| {
                bytes::write_u64(image, SLOT_OFFSET, 9);
                Err::<(), ErrorCode>(ErrorCode::InvalidState)
            })
            .expect("physical aggregate authority");
        assert_eq!(outcome, Err(ErrorCode::InvalidState));
        assert_eq!(
            bytes::read_u64(&state, SLOT_OFFSET),
            9,
            "the written prefix stays written"
        );
        assert!(
            vm.undo_checkpoint() > checkpoint,
            "the written prefix is journaled"
        );
        assert_roundtrip(&mut vm, &mut state, &before, checkpoint, delta);
    }
}

fn mk_derived_state(capacity: u32) -> Vec<u8> {
    let mut state = vec![0xa5; (STATE_HEADER_SIZE + capacity * 16) as usize];
    state[..STATE_HEADER_SIZE as usize].fill(0);
    bytes::write_u32(&mut state, StateHeaderOffset::MAGIC, STATE_MAGIC);
    state[StateHeaderOffset::FORMAT_VERSION as usize] = STATE_FORMAT_VERSION;
    let header_capacity = u16::try_from(capacity).expect("derived capacity fits its u16 header");
    write_derived_facts_header(&mut state, STATE_HEADER_SIZE, header_capacity, 1);
    for pos in 0..capacity {
        bytes::write_u64(
            &mut state,
            STATE_HEADER_SIZE + pos * 8,
            DERIVED_FACT_EMPTY_IDENTITY,
        );
    }
    state
}

fn set_fact_cell(state: &mut [u8], capacity: u32, pos: u32, identity: u64, value: u64) {
    bytes::write_u64(state, STATE_HEADER_SIZE + pos * 8, identity);
    bytes::write_u32(
        state,
        STATE_HEADER_SIZE + capacity * 8 + pos * 4,
        u32::try_from(value & u64::from(u32::MAX)).expect("low word"),
    );
    bytes::write_u32(
        state,
        STATE_HEADER_SIZE + capacity * 12 + pos * 4,
        u32::try_from(value >> 32).expect("high word"),
    );
}

#[test]
fn derived_fact_insert_update_and_retract_restore_full_identity_and_dead_values() {
    let capacity = 4;
    let pos = 2;
    let identity = (u64::from(0x1234u16) << 32) | u64::from(0xfedc_ba98u32);
    for delta in [false, true] {
        for operation in 0..3 {
            let mut state = mk_derived_state(capacity);
            if operation != 0 {
                set_fact_cell(&mut state, capacity, pos, identity, 0x1122_3344_5566_7788);
            }
            let before = state.clone();
            let mut vm = Vm::default();
            vm.undo_enable(&state).expect("undo window opens");
            let ranges = [
                (STATE_HEADER_SIZE + pos * 8, 8),
                (STATE_HEADER_SIZE + capacity * 8 + pos * 4, 4),
                (STATE_HEADER_SIZE + capacity * 12 + pos * 4, 4),
            ];
            vm.capture_mutation(delta, &mut state, &ranges, |image| {
                if operation == 2 {
                    bytes::write_u64(
                        image,
                        STATE_HEADER_SIZE + pos * 8,
                        DERIVED_FACT_TOMBSTONE_IDENTITY,
                    );
                } else {
                    set_fact_cell(image, capacity, pos, identity, 0x99aa_bbcc_ddee_ff00);
                }
            })
            .expect("physical derived cell authority");
            assert_roundtrip(&mut vm, &mut state, &before, 0, delta);
        }
    }
}

/// The capture reserves its records before its closure runs. Two real flag bytes
/// need two records and one remains, so the refusal writes no flag, no image
/// byte, and no journal entry, and leaves the last paired segment intact.
#[test]
fn capture_budget_refusal_never_runs_the_closure_or_changes_flags_image_or_journal() {
    for delta in [false, true] {
        let mut state = mk_slot_state(16, SlotType::HashMap);
        let slot_flag = STATE_HEADER_SIZE + SlotMetaOffset::CHANGE_FLAGS;
        let derived_flag: u32 = StateHeaderOffset::DERIVED_FACTS_CHANGE_FLAG;
        state[slot_flag as usize] = 5;
        state[StateHeaderOffset::DERIVED_FACTS_CHANGE_FLAG as usize] = 6;
        let initial = state.clone();
        let mut vm = Vm::default();
        vm.undo_enable(&state).expect("undo window opens");
        let byte = u32::try_from(state.len() - 1).expect("test image fits u32");
        let ranges = [(byte, 1)];
        for _ in 0..UNDO_CAPACITY - 1 {
            vm.capture_mutation(delta, &mut state, &ranges, |image| {
                image[byte as usize] ^= 1;
            })
            .expect("actual prior-byte budget");
        }
        let checkpoint = vm.undo_checkpoint();
        assert_eq!(checkpoint, UNDO_CAPACITY - 1);
        let before = state.clone();
        let (undo, redo) = if delta {
            assert_eq!(Ok(checkpoint), vm.delta_export_segment(0, checkpoint));
            (
                vm.delta_export_undo().to_vec(),
                vm.delta_export_redo().to_vec(),
            )
        } else {
            (Vec::new(), Vec::new())
        };
        let flags = [(slot_flag, 1), (derived_flag, 1)];
        let mut called = false;
        let result = vm.capture_mutation(delta, &mut state, &flags, |image| {
            called = true;
            image[slot_flag as usize] = 0;
            image[derived_flag as usize] = 0;
        });
        assert_eq!(result, Err(ErrorCode::JournalOverflow));
        assert!(
            !called,
            "a refused reservation must precede arbitrary callback effects"
        );
        assert_eq!(
            state, before,
            "flags, derived flags and every emitted byte are unchanged"
        );
        assert_eq!(vm.undo_checkpoint(), checkpoint);
        if delta {
            assert_eq!(Ok(checkpoint), vm.delta_export_segment(0, checkpoint));
            assert_eq!(vm.delta_export_undo().to_vec(), undo);
            assert_eq!(vm.delta_export_redo().to_vec(), redo);
        }
        vm.undo_rollback(&mut state, 0)
            .expect("real prefix still rolls back");
        assert_eq!(state, initial);
    }
}
