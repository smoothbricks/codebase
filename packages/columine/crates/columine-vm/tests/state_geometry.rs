//! Geometry admission uses actual image bounds and physical slot ownership.
use columine_types::types::{
    ErrorCode, Opcode, PROGRAM_FORMAT_VERSION, PROGRAM_MAGIC, ProgramHeader, SLOT_META_SIZE,
    STATE_HEADER_SIZE, SlotMetaOffset, SlotType, StateHeaderOffset, StructFieldType,
};
use columine_vm::{bytes, state_init};

fn program() -> Vec<u8> {
    // Deliberately define slot 1 first: index order is not physical order.
    let mut init = Vec::new();
    for slot in [1u8, 0] {
        init.extend([
            Opcode::SlotStructMap as u8,
            slot,
            SlotType::StructMap as u8,
            8,
            0,
            1,
            StructFieldType::ArrayU32 as u8,
        ]);
    }
    init.push(Opcode::Halt as u8);
    let header = ProgramHeader {
        magic: PROGRAM_MAGIC,
        version: PROGRAM_FORMAT_VERSION,
        num_callbacks: 0,
        flags: 0,
        num_slots: 2,
        num_inputs: 0,
        init_code_len: init.len() as u32,
        reduce_code_len: 1,
    };
    let mut program = vec![0; 32];
    program.extend(header.to_wire_bytes());
    program.extend(init);
    program.push(Opcode::Halt as u8);
    program
}

fn initialized() -> Vec<u8> {
    let program = program();
    let size = state_init::calculate_state_size(&program, &[PROGRAM_MAGIC])
        .expect("valid program admission");
    let mut state = vec![0; size as usize];
    state_init::init_state(&mut state, &program, &[PROGRAM_MAGIC])
        .expect("valid initialized image");
    state
}

#[test]
fn checked_binding_accepts_unordered_offsets_and_rejects_every_corrupt_geometry() {
    let state = initialized();
    let first = state_init::bind_struct_map(&state, 1).expect("physically first slot");
    let second = state_init::bind_struct_map(&state, 0).expect("physically second slot");
    assert!(first.slot_offset < second.slot_offset);
    assert!(state_init::calculate_grown_state_size(&state, 1).is_ok());
    assert!(matches!(
        state_init::bind_struct_map(&state, 2),
        Err(ErrorCode::InvalidSlot)
    ));
    let meta = STATE_HEADER_SIZE + SLOT_META_SIZE;
    let mutations = [
        (StateHeaderOffset::MAGIC, 0),
        (StateHeaderOffset::NUM_SLOTS, u32::MAX),
        (meta + SlotMetaOffset::CAPACITY, 0),
        (meta + SlotMetaOffset::CAPACITY, 3),
        (meta + SlotMetaOffset::CAPACITY, 1 << 31),
        (meta + SlotMetaOffset::SIZE, first.capacity + 1),
        (meta + SlotMetaOffset::OFFSET, STATE_HEADER_SIZE),
        (meta + SlotMetaOffset::OFFSET, second.slot_offset),
        (
            meta + SlotMetaOffset::GRACE_SECONDS,
            first.arena_header_off + 8,
        ),
        (
            meta + SlotMetaOffset::GRACE_SECONDS,
            second.arena_header_off,
        ),
        (
            first.arena_header_off + 4,
            bytes::read_u32(&state, first.arena_header_off) + 1,
        ),
        (first.arena_header_off, u32::MAX),
    ];
    for (offset, value) in mutations {
        let mut corrupt = state.clone();
        bytes::write_u32(&mut corrupt, offset, value);
        assert!(
            matches!(
                state_init::bind_struct_map(&corrupt, 1),
                Err(ErrorCode::InvalidState)
            ),
            "offset {offset}, value {value}"
        );
        let mut destination = vec![0xa5; state.len() * 2];
        let before = destination.clone();
        assert_eq!(
            state_init::grow_state(&corrupt, &mut destination, 1),
            Err(ErrorCode::InvalidState)
        );
        assert_eq!(
            destination, before,
            "corrupt geometry must precede every destination write"
        );
    }
    for (offset, value) in [
        (StateHeaderOffset::FORMAT_VERSION, 0xff),
        (meta + SlotMetaOffset::STRUCT_BITSET_BYTES, 0),
        (meta + SlotMetaOffset::TTL_SECONDS, 1),
        (first.field_types_off, 0xff),
    ] {
        let mut corrupt = state.clone();
        corrupt[offset as usize] = value;
        assert!(matches!(
            state_init::bind_struct_map(&corrupt, 1),
            Err(ErrorCode::InvalidState)
        ));
    }
    let mut wrong_kind = state.clone();
    wrong_kind[(meta + SlotMetaOffset::TYPE_FLAGS) as usize] = SlotType::HashSet as u8;
    assert!(matches!(
        state_init::bind_struct_map(&wrong_kind, 1),
        Err(ErrorCode::InvalidProgram)
    ));
    for length in [
        0,
        STATE_HEADER_SIZE as usize - 1,
        first.keys_off as usize,
        second.slot_offset as usize - 1,
    ] {
        assert!(matches!(
            state_init::bind_struct_map(&state[..length], 1),
            Err(ErrorCode::InvalidState)
        ));
    }
}

#[test]
fn init_and_growth_refuse_short_destinations_without_touching_them() {
    let program = program();
    let size = state_init::calculate_state_size(&program, &[PROGRAM_MAGIC])
        .expect("valid program admission");
    let mut short = vec![0xa5; size as usize - 1];
    let before = short.clone();
    assert_eq!(
        state_init::init_state(&mut short, &program, &[PROGRAM_MAGIC]),
        Err(ErrorCode::InvalidState)
    );
    assert_eq!(short, before);
    assert_eq!(
        state_init::reset_state(&mut short, &program, &[PROGRAM_MAGIC]),
        Err(ErrorCode::InvalidState)
    );
    assert_eq!(short, before);
    let state = initialized();
    let required =
        state_init::calculate_grown_state_size(&state, 1).expect("checked growth extent");
    let mut short = vec![0xa5; required as usize - 1];
    let before = short.clone();
    assert_eq!(
        state_init::grow_state(&state, &mut short, 1),
        Err(ErrorCode::InvalidState)
    );
    assert_eq!(short, before);
    assert_eq!(
        state_init::calculate_grown_state_size(&state, 2),
        Err(ErrorCode::InvalidSlot)
    );
}

#[test]
fn timestamp_overlay_is_part_of_the_exact_primary_end() {
    let state = initialized();
    // Slot 0 is physically last, so adding its existing timestamp overlay
    // cannot move another slot or change the descriptor/row bytes.
    let slot = state_init::bind_struct_map(&state, 0).expect("valid physical last slot");
    let header = slot.arena_header_off as usize;
    let timestamp_bytes = slot.capacity as usize * 8;
    let mut with_timestamps = state[..header].to_vec();
    with_timestamps.resize(header + timestamp_bytes, 0);
    with_timestamps.extend_from_slice(&state[header..]);
    with_timestamps[(STATE_HEADER_SIZE + SlotMetaOffset::TTL_SECONDS + 2) as usize] = 1;
    bytes::write_u32(
        &mut with_timestamps,
        STATE_HEADER_SIZE + SlotMetaOffset::GRACE_SECONDS,
        slot.arena_header_off + timestamp_bytes as u32,
    );
    assert!(state_init::bind_struct_map(&with_timestamps, 0).is_ok());
    bytes::write_u32(
        &mut with_timestamps,
        STATE_HEADER_SIZE + SlotMetaOffset::GRACE_SECONDS,
        slot.arena_header_off,
    );
    assert!(matches!(
        state_init::bind_struct_map(&with_timestamps, 0),
        Err(ErrorCode::InvalidState)
    ));
}

proptest::proptest! {
    #[test]
    fn randomized_committed_growth_and_stable_delta_replay_preserve_images_and_forks(
        pairs in proptest::collection::vec((1u32..1_000_000, proptest::num::u32::ANY), 20..120)
    ) {
        use columine_vm::vm::Vm;
        let init = [Opcode::SlotDef as u8, 0, SlotType::HashMap as u8, 8, 0, Opcode::Halt as u8];
        let reduce = [Opcode::BatchMapUpsertLast as u8, 0, 0, 1, Opcode::Halt as u8];
        let header = ProgramHeader {
            magic: PROGRAM_MAGIC, version: PROGRAM_FORMAT_VERSION, num_callbacks: 0, flags: 0,
            num_slots: 1, num_inputs: 2, init_code_len: init.len() as u32, reduce_code_len: reduce.len() as u32,
        };
        let mut program = vec![0; 32];
        program.extend(header.to_wire_bytes());
        program.extend(init);
        program.extend(reduce);
        let size = state_init::calculate_state_size(&program, &[PROGRAM_MAGIC])
            .expect("valid program admission");
        let mut state = vec![0; size as usize];
        state_init::init_state(&mut state, &program, &[PROGRAM_MAGIC]).expect("valid program");
        let mut vm = Vm::default();
        let seed_key = 0u32.to_le_bytes();
        let seed_value = 42u32.to_le_bytes();
        proptest::prop_assert_eq!(vm.execute_batch(&mut state, &program, &[&seed_key, &seed_value], 1), ErrorCode::Ok as u32);
        let original = state.clone();
        let mut committed = state.clone();
        vm.undo_enable(&state).expect("undo window opens");
        let mut checkpoint = vm.undo_checkpoint();
        let keys: Vec<u8> = pairs.iter().flat_map(|(key, _)| key.to_le_bytes()).collect();
        let values: Vec<u8> = pairs.iter().flat_map(|(_, value)| value.to_le_bytes()).collect();
        loop {
            let result = vm.execute_batch_delta(&mut state, &program, &[&keys, &values], pairs.len() as u32);
            if result == ErrorCode::Ok as u32 { break; }
            proptest::prop_assert_eq!(result, ErrorCode::NeedsGrowth as u32);
            let slot = state_init::needs_growth_slot();
            let required = state_init::calculate_grown_state_size(&committed, slot).expect("checked growth");
            let mut grown = vec![0; required as usize];
            let active_checkpoint = vm.undo_checkpoint();
            proptest::prop_assert_eq!(vm.grow_state(&state, &mut grown, slot), Err(ErrorCode::NeedsGrowth));
            proptest::prop_assert_eq!(vm.undo_checkpoint(), active_checkpoint);
            proptest::prop_assert!(grown.iter().all(|&byte| byte == 0));
            let logical = vm.undo_rollback(&mut state, checkpoint).expect("full-window rollback");
            proptest::prop_assert_eq!(logical, state.len() as u32);
            proptest::prop_assert_eq!(&state, &committed);
            vm.undo_commit();
            vm.grow_state(&state, &mut grown, slot).expect("committed growth");
            proptest::prop_assert_eq!(&state, &committed, "growth preserves its old authority");
            proptest::prop_assert_eq!(columine_vm::vm::CheckedMap::bind(&grown, 0).map(|map| map.get(0)), Ok(Some(42)));
            proptest::prop_assert_eq!(bytes::read_u32(&grown, STATE_HEADER_SIZE + SlotMetaOffset::SIZE), 1);
            state = grown;
            committed = state.clone();
            vm.undo_enable(&state).expect("undo window opens");
            checkpoint = vm.undo_checkpoint();
        }
        let mut expected: std::collections::BTreeMap<u32, u32> = pairs.iter().copied().collect();
        expected.insert(0, 42);
        proptest::prop_assert_eq!(bytes::read_u32(&state, STATE_HEADER_SIZE + SlotMetaOffset::SIZE) as usize, expected.len());
        let map = columine_vm::vm::CheckedMap::bind(&state, 0).expect("valid map after growth");
        for (&key, &value) in &expected {
            proptest::prop_assert_eq!(map.get(key), Some(value));
        }
        let after = state.clone();
        let fork = after.clone();
        let end = vm.undo_checkpoint();
        proptest::prop_assert_eq!(vm.delta_export_segment(checkpoint, end), Ok(end - checkpoint));
        let undo = vm.delta_export_undo_bytes();
        let redo = vm.delta_export_redo_bytes();
        let entry = Vm::delta_export_entry_size();
        let logical = vm.undo_rollback(&mut state, checkpoint).expect("current-layout checkpoint");
        proptest::prop_assert_eq!(logical, state.len() as u32);
        proptest::prop_assert_eq!(&state, &committed);
        proptest::prop_assert_eq!(&fork, &after);
        vm.undo_commit();
        let mut replay = Vm::default();
        let required = Vm::delta_required_state_size(&redo, entry, state.len() as u32).expect("native span preflight");
        proptest::prop_assert_eq!(required, state.len() as u32);
        let logical = replay.delta_apply_rollforward_segment(&mut state, &redo, entry).expect("same-layout redo");
        proptest::prop_assert_eq!(logical, state.len() as u32);
        proptest::prop_assert_eq!(&state, &after);
        let required = Vm::delta_required_state_size(&undo, entry, state.len() as u32).expect("native reverse span preflight");
        proptest::prop_assert_eq!(required, state.len() as u32);
        let logical = replay.delta_apply_rollback_segment(&mut state, &undo, entry).expect("same-layout undo");
        proptest::prop_assert_eq!(logical, state.len() as u32);
        proptest::prop_assert_eq!(&state, &committed);
        proptest::prop_assert_eq!(&fork, &after);
        proptest::prop_assert_eq!(bytes::read_u32(&original, STATE_HEADER_SIZE + SlotMetaOffset::SIZE), 1);
    }
}
