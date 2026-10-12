//! Dispatch tests cover batch execution, undo/rollback, TTL eviction, and
//! growth signaling. Scenarios and expected values are pinned as ABI behavior.

use columine_types::NO_PARENT_TS_COL;
use columine_types::PROGRAM_MAGIC;
use columine_types::operand::encode_index;
use columine_types::types::{
    AggType, ChangeFlag, EMPTY_KEY, ErrorCode, Opcode, SLOT_META_SIZE, STATE_HEADER_SIZE,
    SlotMetaOffset, SlotType,
};
use columine_vm::bytes;
use columine_vm::meta::SlotMetaView;
use columine_vm::state_init::{
    EVICTION_ENTRY_SIZE, calculate_grown_state_size, calculate_state_size, grow_state, init_state,
};
use columine_vm::struct_map::{StructMap2Slot, StructMapSlot};
use columine_vm::vm::{
    CheckedMap, CheckedSet, CheckedStructMap, UNDO_CAPACITY, UNDO_POSITION_OVERFLOWED, Vm,
    f64s_as_bytes, i64s_as_bytes, u32s_as_bytes,
};

const OK: u32 = ErrorCode::Ok as u32;
const NEEDS_GROWTH: u32 = ErrorCode::NeedsGrowth as u32;

// =============================================================================
// Program builders (, 1200-1293, 1530-1600, 1957-2036,
// 2450-2530 equivalents)
// =============================================================================

/// Assemble a full program: 32-byte hash prefix + 24-byte content header +
/// init + reduce.
fn program(num_slots: u32, num_inputs: u32, init: &[u8], reduce: &[u8]) -> Vec<u8> {
    let mut init = init.to_vec();
    init.push(0);
    let mut reduce = reduce.to_vec();
    reduce.push(0);
    let mut prog = vec![0u8; 32];
    prog.extend(PROGRAM_MAGIC.to_le_bytes());
    prog.extend([2, 0, 0, 0]); // version 2, no callbacks, no flags
    prog.extend(num_slots.to_le_bytes());
    prog.extend(num_inputs.to_le_bytes());
    prog.extend((init.len() as u32).to_le_bytes());
    prog.extend((reduce.len() as u32).to_le_bytes());
    prog.extend(init);
    prog.extend(reduce);
    prog
}

/// SLOT_DEF(0x10): slot, type_flags, cap_lo, cap_hi.
fn slot_def(slot: u8, type_flags: u8, cap_lo: u8, cap_hi: u8) -> [u8; 5] {
    [0x10, slot, type_flags, cap_lo, cap_hi]
}

/// SLOT_DEF with the 10-byte TTL payload.
fn slot_def_ttl(
    slot: u8,
    type_flags: u8,
    cap_lo: u8,
    cap_hi: u8,
    ttl: f32,
    grace: f32,
    ts_field_idx: u8,
) -> Vec<u8> {
    let mut def = vec![0x10, slot, type_flags, cap_lo, cap_hi];
    def.extend(ttl.to_le_bytes());
    def.extend(grace.to_le_bytes());
    def.push(ts_field_idx);
    def.push(0); // DurationUnit.NONE
    def
}

/// SLOT_STRUCT_MAP(0x18): slot, type_flags, cap_lo, cap_hi, num_fields, field types.
fn slot_struct_map(slot: u8, type_flags: u8, cap_lo: u8, field_types: &[u8]) -> Vec<u8> {
    let mut def = vec![0x18, slot, type_flags, cap_lo, 0, field_types.len() as u8];
    def.extend_from_slice(field_types);
    def
}

///  `buildTestProgram` — HASHMAP(LAST) + AGG COUNT.
fn build_test_program(cap_lo: u8, cap_hi: u8) -> Vec<u8> {
    let mut init = Vec::new();
    init.extend(slot_def(0, 0x00, cap_lo, cap_hi));
    init.extend(slot_def(1, 0x02, 2, 0)); // AGGREGATE, AggType.COUNT
    let reduce = [0x22, 0, 0, 1, 0x41, 1];
    program(2, 2, &init, &reduce)
}

///  `buildTTLMapProgram` — TTL HASHMAP + BATCH_MAP_UPSERT_LATEST.
fn build_ttl_map_program(cap_lo: u8, cap_hi: u8, has_evict_trigger: bool) -> Vec<u8> {
    let evict_flag = if has_evict_trigger { 0x20 } else { 0 };
    let init = slot_def_ttl(0, 0x10 | evict_flag, cap_lo, cap_hi, 10.0, 0.0, 2);
    let reduce = [0x20, 0, 0, 1, 2, 1]; // slot, key, val, ts, cmp_type=f64
    program(1, 3, &init, &reduce)
}

///  `buildTTLSetProgram` — TTL HASHSET + BATCH_SET_INSERT.
fn build_ttl_set_program(cap_lo: u8, cap_hi: u8) -> Vec<u8> {
    let init = slot_def_ttl(0, 0x01 | 0x10, cap_lo, cap_hi, 10.0, 0.0, 1);
    let reduce = [0x30, 0, 0];
    program(1, 2, &init, &reduce)
}

///  `buildForEachTestProgramMulti` — FOR_EACH { MAP_UPSERT_LAST + AGG_COUNT }.
fn build_for_each_program(type_ids: &[u32]) -> Vec<u8> {
    let mut init = Vec::new();
    init.extend(slot_def(0, 0x00, 4, 0));
    init.extend(slot_def(1, 0x02, 2, 0));

    let body = [0x22u8, 0, 1, 2, 0x41, 1];
    let mut reduce = vec![0xE0, 0, type_ids.len() as u8];
    for tid in type_ids {
        reduce.extend(tid.to_le_bytes());
    }
    reduce.extend((body.len() as u16).to_le_bytes());
    reduce.extend(body);
    program(2, 3, &init, &reduce)
}

///  `buildBlockStructMapTestProgram` — FOR_EACH { 0x80 }.
fn build_block_struct_map_program(type_id: u32) -> Vec<u8> {
    let init = slot_struct_map(0, 6, 4, &[0, 4]); // STRUCT_MAP=6; UINT32 + STRING
    let body = [0x80u8, 0, 1, 2, 2, 0, 3, 1, 0];
    let mut reduce = vec![0xE0, 0, 1];
    reduce.extend(type_id.to_le_bytes());
    reduce.extend((body.len() as u16).to_le_bytes());
    reduce.extend(body);
    program(1, 4, &init, &reduce)
}

/// Build a FOR_EACH/FLAT_MAP program whose declared slot kind matches its
/// struct-map opcode and actual state shape.
fn build_flat_map_program(type_id: u32) -> Vec<u8> {
    let init = slot_struct_map(0, 0x06, 4, &[0, 4]);
    let inner_body = [0x80u8, 0, 2, 2, 3, 0, 4, 1, 0];
    let mut flat_map = vec![0xE1, 1];
    flat_map.extend(encode_index(NO_PARENT_TS_COL).as_bytes());
    flat_map.extend((inner_body.len() as u16).to_le_bytes());
    flat_map.extend(inner_body);
    let mut reduce = vec![0xE0, 0, 1];
    reduce.extend(type_id.to_le_bytes());
    reduce.extend((flat_map.len() as u16).to_le_bytes());
    reduce.extend(&flat_map);
    program(1, 5, &init, &reduce)
}

///  `buildStructMapTestProgram` — top-level 0x80.
fn build_struct_map_program(cap_lo: u8, cap_hi: u8) -> Vec<u8> {
    let init = slot_struct_map(0, 6, cap_lo, &[0, 4]);
    let mut init2 = init.clone();
    init2[4] = cap_hi;
    let reduce = [0x80u8, 0, 0, 2, 1, 0, 2, 1, 0];
    program(1, 3, &init2, &reduce)
}

///  `buildProbeScatterTestProgram` — FOR_EACH { FLAT_MAP { 0x2f } }.
fn build_probe_scatter_program(type_id: u32) -> Vec<u8> {
    let mut init = Vec::new();
    // Slot 0: stagedSuggestions [route:U32, op:U32, e:U32, v_str:STR, v_num:F64, v_set:STR]
    init.extend(slot_struct_map(0, 6, 4, &[0, 0, 0, 4, 2, 4]));
    // Slot 1: nodes [title:STR, status:STR, boost:F64]
    init.extend(slot_struct_map(1, 6, 4, &[4, 4, 2]));
    // Slots 2, 3: HASHSET nodeDeps / nodeTouch
    init.extend(slot_def(2, 1, 4, 0));
    init.extend(slot_def(3, 1, 4, 0));

    // 0x2f body: probe_slot=0, key_col=2, miss=skip, route_fi=0, op_fi=1, 5 routes.
    #[rustfmt::skip]
    let scatter: [u8; 32] = [
        0x2f, 0, 2, 0, 0, 1, 5,
        0, 1, 0, 2, 3, // route 0: kind0 → nodes.title, v_src=v_str
        0, 1, 1, 2, 3, // route 1: kind0 → nodes.status, v_src=v_str
        1, 2, 0, 2, 5, // route 2: kind1 → nodeDeps, v_src=v_set
        1, 3, 0, 2, 5, // route 3: kind1 → nodeTouch, v_src=v_set
        0, 1, 2, 2, 4, // route 4: kind0 → nodes.boost, v_src=v_num
    ];
    let mut flat_map = vec![0xE1, 1];
    flat_map.extend(encode_index(NO_PARENT_TS_COL).as_bytes());
    flat_map.extend((scatter.len() as u16).to_le_bytes());
    flat_map.extend(scatter);
    let mut reduce = vec![0xE0, 0, 1];
    reduce.extend(type_id.to_le_bytes());
    reduce.extend((flat_map.len() as u16).to_le_bytes());
    reduce.extend(&flat_map);
    program(4, 3, &init, &reduce)
}

/// Three shared ordinals: consumed scalar, no consumer, consumed scalar.
/// The omitted destination has no slot definition, and its ignored value
/// operand deliberately names no source.
fn sparse_scatter_program(opcode: Opcode, skipped_kind: u8, ignored_value_col: u32) -> Vec<u8> {
    let mut init = slot_struct_map(0, 6, 8, &[0, 0, 0, 4, 2, 4]);
    init.extend(slot_struct_map(1, 6, 8, &[4, 4, 0]));
    let probe = opcode == Opcode::BatchStructMapProbeScatter;
    let mut body = if probe {
        vec![opcode as u8, 0, 2, 0, 0, 1, 3]
    } else if opcode == Opcode::BatchStructMapScatterGuarded {
        vec![opcode as u8, 3, 4, 2, 1, 1, 6, 2, 3]
    } else {
        vec![opcode as u8, 3, 4, 2, 3]
    };
    for (kind, dest, field) in [(0, 1, 0), (skipped_kind, u32::MAX, 255), (0, 1, 1)] {
        body.push(kind);
        body.extend(encode_index(dest).as_bytes());
        body.push(field);
        if probe {
            body.extend(if dest == u32::MAX { [255, 255] } else { [2, 3] });
        } else {
            body.extend(
                encode_index(if dest == u32::MAX {
                    ignored_value_col
                } else {
                    5
                })
                .as_bytes(),
            );
        }
    }
    let mut flat = vec![Opcode::FlatMap as u8, 1];
    flat.extend(encode_index(NO_PARENT_TS_COL).as_bytes());
    flat.extend(u16::try_from(body.len()).expect("small body").to_le_bytes());
    flat.extend(body);
    let mut reduce = vec![Opcode::ForEach as u8, 0, 1];
    reduce.extend(2009u32.to_le_bytes());
    reduce.extend(u16::try_from(flat.len()).expect("small body").to_le_bytes());
    reduce.extend(flat);
    program(2, 7, &init, &reduce)
}

fn run_sparse_scatter(
    vm: &mut Vm,
    state: &mut [u8],
    prog: &[u8],
    opcode: Opcode,
    ordinal: u32,
    retract: u32,
    delta: bool,
) -> u32 {
    let types = [2009u32];
    let offsets = [0u32, 1];
    let keys = [if opcode == Opcode::BatchStructMapProbeScatter {
        10 + ordinal
    } else {
        600
    }];
    let routes = [ordinal];
    let ops = [retract];
    let values = [700 + ordinal];
    let guards = [1u32];
    let cols = [
        u32s_as_bytes(&types),
        u32s_as_bytes(&offsets),
        u32s_as_bytes(&keys),
        u32s_as_bytes(&routes),
        u32s_as_bytes(&ops),
        u32s_as_bytes(&values),
        u32s_as_bytes(&guards),
    ];
    if delta {
        vm.execute_batch_delta(state, prog, &cols, 1)
    } else {
        vm.execute_batch(state, prog, &cols, 1)
    }
}

#[test]
fn scatter_no_consumer_preserves_middle_ordinals_and_undo() {
    for opcode in [
        Opcode::BatchStructMapProbeScatter,
        Opcode::BatchStructMapScatter,
        Opcode::BatchStructMapScatterGuarded,
    ] {
        for skipped_kind in [0, 1] {
            for delta in [false, true] {
                let prog = sparse_scatter_program(opcode, skipped_kind, u32::MAX);
                let mut state = init(&prog);
                if opcode == Opcode::BatchStructMapProbeScatter {
                    for ordinal in 0..3 {
                        seed_probe_row(
                            &mut state,
                            10 + ordinal,
                            ordinal,
                            0,
                            600,
                            700 + ordinal,
                            0.0,
                            0,
                        );
                    }
                }
                assert_eq!(2, columine_types::types::state_num_slots(&state));
                let mut columns = Vec::new();
                columine_vm::column_uses::reduce_column_uses(&prog, &state, &mut |col, _| {
                    columns.push(col)
                })
                .expect("sparse routes admit their real columns");
                assert!(
                    !columns.contains(&u32::MAX),
                    "ignored value is not a column read"
                );
                if opcode != Opcode::BatchStructMapProbeScatter {
                    assert!(columns.contains(&5), "consumed value remains a column read");
                }
                let mut vm = Vm::default();
                let before = state.clone();
                for retract in [0, 1] {
                    assert_eq!(
                        OK,
                        run_sparse_scatter(&mut vm, &mut state, &prog, opcode, 1, retract, delta),
                        "fresh {opcode:?}, kind {skipped_kind}"
                    );
                    assert_eq!(before, state);
                    assert_eq!(0, vm.undo_checkpoint());
                }
                vm.undo_enable(&state).expect("undo window opens");
                let checkpoint = vm.undo_checkpoint();
                for retract in [0, 1] {
                    assert_eq!(
                        OK,
                        run_sparse_scatter(&mut vm, &mut state, &prog, opcode, 1, retract, delta),
                        "{opcode:?}, kind {skipped_kind}"
                    );
                    assert_eq!(
                        before, state,
                        "ignored route neither writes nor stamps its guard"
                    );
                    assert_eq!(checkpoint, vm.undo_checkpoint());
                }
                for ordinal in [2, 0] {
                    assert_eq!(
                        OK,
                        run_sparse_scatter(&mut vm, &mut state, &prog, opcode, ordinal, 0, delta)
                    );
                }
                let row = present_row(&state, 1, 600);
                assert_eq!(700, bytes::read_u32(&state, row + 1));
                assert_eq!(
                    702,
                    bytes::read_u32(&state, row + 5),
                    "later route retains ordinal 2"
                );
                if opcode == Opcode::BatchStructMapScatterGuarded {
                    assert_eq!(
                        1,
                        bytes::read_u32(&state, row + 9),
                        "consumed routes still stamp the guard"
                    );
                }
                vm.undo_rollback(&mut state, checkpoint)
                    .expect("sparse scatter rolls back");
                assert_eq!(before, state);
            }
        }
    }
}

#[test]
fn scatter_no_consumer_keeps_consumed_shared_value_column() {
    for opcode in [
        Opcode::BatchStructMapScatter,
        Opcode::BatchStructMapScatterGuarded,
    ] {
        let prog = sparse_scatter_program(opcode, 1, 5);
        let state = init(&prog);
        let mut columns = Vec::new();
        columine_vm::column_uses::reduce_column_uses(&prog, &state, &mut |col, _| {
            columns.push(col)
        })
        .expect("shared value column remains admitted");
        assert_eq!(
            2,
            columns.iter().filter(|&&col| col == 5).count(),
            "both consumed entries contribute their shared value read, the skipped entry does not"
        );
    }
}

#[test]
fn scatter_no_consumer_is_not_a_third_route_kind() {
    for opcode in [
        Opcode::BatchStructMapProbeScatter,
        Opcode::BatchStructMapScatter,
        Opcode::BatchStructMapScatterGuarded,
    ] {
        let prog = sparse_scatter_program(opcode, 2, u32::MAX);
        let mut state = init(&prog);
        if opcode == Opcode::BatchStructMapProbeScatter {
            seed_probe_row(&mut state, 11, 1, 0, 600, 701, 0.0, 0);
        }
        let before = state.clone();
        let mut vm = Vm::default();
        assert_eq!(
            ErrorCode::InvalidProgram as u32,
            run_sparse_scatter(&mut vm, &mut state, &prog, opcode, 1, 0, false)
        );
        assert_eq!(before, state);
    }
}

// =============================================================================
// State / metadata helpers
// =============================================================================

fn init(prog: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(
        prog,
        columine_vm::state_init::DEFAULT_ACCEPTED_PROGRAM_MAGICS,
    )
    .expect("valid program admission");
    assert!(size > 0, "state size must be > 0");
    let mut state = vec![0u8; size as usize];
    init_state(
        &mut state,
        prog,
        columine_vm::state_init::DEFAULT_ACCEPTED_PROGRAM_MAGICS,
    )
    .expect("init_state");
    state
}

fn meta_u32(state: &[u8], slot: u8, field_off: u32) -> u32 {
    bytes::read_u32(
        state,
        STATE_HEADER_SIZE + u32::from(slot) * SLOT_META_SIZE + field_off,
    )
}

fn slot_offset(state: &[u8], slot: u8) -> u32 {
    meta_u32(state, slot, SlotMetaOffset::OFFSET)
}

fn slot_cap(state: &[u8], slot: u8) -> u32 {
    meta_u32(state, slot, SlotMetaOffset::CAPACITY)
}

fn slot_size(state: &[u8], slot: u8) -> u32 {
    meta_u32(state, slot, SlotMetaOffset::SIZE)
}

fn slot_change_flags(state: &[u8], slot: u8) -> u8 {
    let meta_base = STATE_HEADER_SIZE + u32::from(slot) * SLOT_META_SIZE;
    state[(meta_base + SlotMetaOffset::CHANGE_FLAGS) as usize]
}

fn clear_slot_change_flags(state: &mut [u8], slot: u8) {
    let meta_base = STATE_HEADER_SIZE + u32::from(slot) * SLOT_META_SIZE;
    state[(meta_base + SlotMetaOffset::CHANGE_FLAGS) as usize] = 0;
}

/// Typed HASHMAP point read. A refused slot is a test failure; absence is `None`.
fn map_get(state: &[u8], slot: u8, key: u32) -> Option<u32> {
    CheckedMap::bind(state, u32::from(slot))
        .expect("valid hashmap slot")
        .get(key)
}

/// Typed HASHSET membership. A refused slot is a test failure, never `false`.
fn set_contains(state: &[u8], slot: u8, elem: u32) -> bool {
    CheckedSet::bind(state, u32::from(slot))
        .expect("valid set slot")
        .contains(elem)
}

/// Typed STRUCT_MAP row lookup; `None` is a valid absent key.
fn struct_map_row(state: &[u8], slot: u8, key: u32) -> Option<u32> {
    CheckedStructMap::bind(state, u32::from(slot))
        .expect("valid struct-map slot")
        .row(key)
}

/// The row of a key the test has already established as present.
fn present_row(state: &[u8], slot: u8, key: u32) -> u32 {
    struct_map_row(state, slot, key).expect("key present")
}

fn bit_set(state: &[u8], row: u32, fi: u8) -> bool {
    StructMapSlot::is_field_set(state, row, fi)
}

///  `seedProbeRow`.
#[allow(clippy::too_many_arguments)]
fn seed_probe_row(
    state: &mut [u8],
    key: u32,
    route: u32,
    op_retract: u32,
    e_val: u32,
    v_str: u32,
    v_num: f64,
    v_set: u32,
) {
    let smap = StructMapSlot::bind(state, 0);
    let up = smap.upsert(state, key).expect("probe upsert");
    let row = smap.row_off(up.pos);
    smap.clear_bitset(state, row);
    for (fi, cell4) in [(0u8, route), (1, op_retract), (2, e_val), (3, v_str)] {
        StructMapSlot::set_field_bit(state, row, fi);
        let off = row + smap.field_offset(state, fi);
        bytes::write_u32(state, off, cell4);
    }
    StructMapSlot::set_field_bit(state, row, 4);
    let off4 = row + smap.field_offset(state, 4);
    bytes::write_u64(state, off4, v_num.to_bits());
    StructMapSlot::set_field_bit(state, row, 5);
    let off5 = row + smap.field_offset(state, 5);
    bytes::write_u32(state, off5, v_set);
}

// =============================================================================
// TTL through the live reducer path ()
// =============================================================================

#[test]
fn ttl_hashmap_insert_and_evict_through_live_reducer_path() {
    let prog = build_ttl_map_program(4, 0, true);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let key = [42u32];
    let val = [7u32];
    let ts = [100.0f64];
    let cols: Vec<&[u8]> = vec![u32s_as_bytes(&key), u32s_as_bytes(&val), f64s_as_bytes(&ts)];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 1));

    assert_eq!(Some(7), map_get(&state, 0, 42));

    assert_eq!(0, vm.evict_all_expired(false, &mut state, 105.0).unwrap());
    assert_eq!(Some(7), map_get(&state, 0, 42));

    assert_eq!(1, vm.evict_all_expired(false, &mut state, 111.0).unwrap());
    assert_eq!(None, map_get(&state, 0, 42));
}

#[test]
fn ttl_eviction_result_distinguishes_four_rows_from_invalid_state() {
    let prog = build_ttl_map_program(8, 0, true);
    let mut state = init(&prog);
    let mut vm = Vm::default();
    let keys = [1u32, 2, 3, 4];
    let vals = [10u32, 20, 30, 40];
    let timestamps = [100.0f64, 101.0, 102.0, 103.0];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&keys),
        u32s_as_bytes(&vals),
        f64s_as_bytes(&timestamps),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 4));
    assert_eq!(Ok(4), vm.evict_all_expired(false, &mut state, 114.0));

    let meta = SlotMetaView::read(&state, 0);
    assert_eq!(4, meta.evicted_count(&state));
    let mut invalid = vec![0u8; STATE_HEADER_SIZE as usize];
    assert_eq!(
        Err(ErrorCode::InvalidState),
        vm.evict_all_expired(false, &mut invalid, 114.0)
    );
}

#[test]
fn ttl_hashmap_stale_eviction_entries_do_not_evict_newer_values() {
    let prog = build_ttl_map_program(4, 0, false);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    for (val, ts) in [(100u32, 100.0f64), (200, 200.0)] {
        let key = [9u32];
        let vals = [val];
        let tss = [ts];
        let cols: Vec<&[u8]> = vec![
            u32s_as_bytes(&key),
            u32s_as_bytes(&vals),
            f64s_as_bytes(&tss),
        ];
        assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 1));
    }

    // cutoff=140: the stale ts=100 index entry must not evict the ts=200 value.
    assert_eq!(0, vm.evict_all_expired(false, &mut state, 150.0).unwrap());
    assert_eq!(Some(200), map_get(&state, 0, 9));
}

#[test]
fn ttl_hashset_reinsertion_refreshes_ttl_and_evicts_deterministically() {
    let prog = build_ttl_set_program(4, 0);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    for ts in [100.0f64, 200.0] {
        let elem = [77u32];
        let tss = [ts];
        let cols: Vec<&[u8]> = vec![u32s_as_bytes(&elem), f64s_as_bytes(&tss)];
        assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 1));
    }

    assert!(set_contains(&state, 0, 77));

    assert_eq!(0, vm.evict_all_expired(false, &mut state, 150.0).unwrap());
    assert!(set_contains(&state, 0, 77));

    assert_eq!(1, vm.evict_all_expired(false, &mut state, 211.0).unwrap());
    assert!(!set_contains(&state, 0, 77));
}

#[test]
fn ttl_eviction_index_overflow_returns_needs_growth_instead_of_dropping() {
    let prog = build_ttl_map_program(4, 0, false);
    let mut state = init(&prog);
    let mut vm = Vm::default();
    let keys = [1u32];
    let vals = [10u32];
    let tss = [100.0f64];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&keys),
        u32s_as_bytes(&vals),
        f64s_as_bytes(&tss),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 1));

    // A retained index may hold older rows for a live key: fill it to its
    // capacity with rows for key 1, a valid geometry the map's load factor
    // does not see.
    let meta = SlotMetaView::read(&state, 0);
    let base = meta.eviction_index_offset(&state);
    let capacity = meta.eviction_index_capacity(&state);
    for row in 0..capacity {
        let at = base + row * EVICTION_ENTRY_SIZE;
        bytes::write_f64(&mut state, at, 50.0 + f64::from(row));
        bytes::write_u32(&mut state, at + 8, 1);
        bytes::write_u32(&mut state, at + 12, 10);
    }
    meta.set_eviction_index_size(&mut state, capacity);

    let keys = [2u32];
    let vals = [20u32];
    let tss = [101.0f64];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&keys),
        u32s_as_bytes(&vals),
        f64s_as_bytes(&tss),
    ];
    let before = state.clone();
    assert_eq!(NEEDS_GROWTH, vm.execute_batch(&mut state, &prog, &cols, 1));
    assert_eq!(Some(0), vm.needs_growth_slot());
    assert_eq!(before, state, "the refused batch writes nothing");

    // An index capacity that disagrees with its slot is no valid geometry:
    // the batch is refused as InvalidState where the state enters, not as
    // a request to grow.
    bytes::write_u32(
        &mut state,
        STATE_HEADER_SIZE + SlotMetaOffset::EVICTION_INDEX_CAPACITY,
        1,
    );
    assert_eq!(
        ErrorCode::InvalidState as u32,
        vm.execute_batch(&mut state, &prog, &cols, 1)
    );

    // Eviction is an entry too: an image cut inside its expiry index is
    // refused before any eviction reads it, with or without a journal.
    let mut whole = init(&prog);
    let keys = [1u32, 2, 3];
    let vals = [10u32, 20, 30];
    let tss = [100.0f64, 101.0, 102.0];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&keys),
        u32s_as_bytes(&vals),
        f64s_as_bytes(&tss),
    ];
    assert_eq!(OK, Vm::default().execute_batch(&mut whole, &prog, &cols, 3));
    let cut = whole[..whole.len() - 16].to_vec();
    for recording in [false, true] {
        let mut state = cut.clone();
        let mut vm = Vm::default();
        if recording {
            vm.undo_enable(&state).expect("window opens");
        }
        assert_eq!(
            Err(ErrorCode::InvalidState),
            vm.evict_all_expired(false, &mut state, 1_000.0),
            "recording {recording}"
        );
        assert_eq!(cut, state, "a refused image is not written");
    }
}

// =============================================================================
// Slot growth through dispatch ()
// =============================================================================

#[test]
fn slot_growth_hashmap_capacity_exceeded_triggers_needs_growth() {
    let prog = build_test_program(4, 0);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    assert_eq!(16, slot_cap(&state, 0)); // nextPowerOf2(4*2) = 16

    for i in 0..11u32 {
        let key = [i];
        let val = [i * 10];
        let cols: Vec<&[u8]> = vec![u32s_as_bytes(&key), u32s_as_bytes(&val)];
        assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 1));
    }
    assert_eq!(11, slot_size(&state, 0));

    // 12th unique key: size=11 >= max_size=(16*7)/10=11 → NEEDS_GROWTH.
    let key = [100u32];
    let val = [1000u32];
    let cols: Vec<&[u8]> = vec![u32s_as_bytes(&key), u32s_as_bytes(&val)];
    assert_eq!(NEEDS_GROWTH, vm.execute_batch(&mut state, &prog, &cols, 1));
    assert_eq!(Some(0), vm.needs_growth_slot());
}

#[test]
fn slot_growth_grow_preserves_hashmap_entries_and_aggregate() {
    let prog = build_test_program(4, 0);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    for i in 0..11u32 {
        let key = [i];
        let val = [i * 10];
        let cols: Vec<&[u8]> = vec![u32s_as_bytes(&key), u32s_as_bytes(&val)];
        assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 1));
    }
    // Aggregate COUNT slot 1 = 11 (11 single-row batches).
    let agg_off = slot_offset(&state, 1);
    assert_eq!(11, bytes::read_u64(&state, agg_off));

    let grown_size = calculate_grown_state_size(&state, 0).expect("valid growth geometry");
    let mut new_state = vec![0u8; grown_size as usize];
    grow_state(&state, &mut new_state, 0).expect("grow_state");

    assert_eq!(32, slot_cap(&new_state, 0));
    assert_eq!(11, slot_size(&new_state, 0));

    for i in 0..11u32 {
        assert_eq!(Some(i * 10), map_get(&new_state, 0, i));
    }
    // Aggregate preserved (copied, not re-executed).
    let new_agg_off = slot_offset(&new_state, 1);
    assert_eq!(11, bytes::read_u64(&new_state, new_agg_off));

    // Room again: 12th key inserts without NEEDS_GROWTH.
    let key = [100u32];
    let val = [1000u32];
    let cols: Vec<&[u8]> = vec![u32s_as_bytes(&key), u32s_as_bytes(&val)];
    assert_eq!(OK, vm.execute_batch(&mut new_state, &prog, &cols, 1));
    assert_eq!(Some(1000), map_get(&new_state, 0, 100));
}

#[test]
fn slot_growth_hashset_growth_preserves_elements() {
    //  — one HASHSET slot, BATCH_SET_INSERT, grow after fill.
    let init_sec = slot_def(0, 0x01, 4, 0);
    let reduce = [0x30u8, 0, 0];
    let prog = program(1, 1, &init_sec, &reduce);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    for i in 0..11u32 {
        let elem = [i + 1000];
        let cols: Vec<&[u8]> = vec![u32s_as_bytes(&elem)];
        assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 1));
    }
    assert_eq!(11, slot_size(&state, 0));

    let elem = [5000u32];
    let cols: Vec<&[u8]> = vec![u32s_as_bytes(&elem)];
    assert_eq!(NEEDS_GROWTH, vm.execute_batch(&mut state, &prog, &cols, 1));

    let grown_size = calculate_grown_state_size(&state, 0).expect("valid growth geometry");
    let mut new_state = vec![0u8; grown_size as usize];
    grow_state(&state, &mut new_state, 0).expect("grow_state");

    assert_eq!(32, slot_cap(&new_state, 0));
    assert_eq!(11, slot_size(&new_state, 0));
    for i in 0..11u32 {
        assert!(set_contains(&new_state, 0, i + 1000));
    }
    // And the overflowing element now fits.
    assert_eq!(OK, vm.execute_batch(&mut new_state, &prog, &cols, 1));
    assert!(set_contains(&new_state, 0, 5000));
}

// =============================================================================
// FOR_EACH ()
// =============================================================================

const TYPE_A: u32 = 1001;
const TYPE_B: u32 = 1002;
const TYPE_C: u32 = 1003;

fn agg_count(state: &[u8], slot: u8) -> u64 {
    bytes::read_u64(state, slot_offset(state, slot))
}

#[test]
fn for_each_basic_type_filtering() {
    let prog = build_for_each_program(&[TYPE_A]);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let types = [TYPE_A, TYPE_B, TYPE_A];
    let keys = [100u32, 200, 300];
    let vals = [10u32, 20, 30];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&keys),
        u32s_as_bytes(&vals),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 3));

    assert_eq!(2, slot_size(&state, 0));
    assert_eq!(Some(10), map_get(&state, 0, 100));
    assert_eq!(Some(30), map_get(&state, 0, 300));
    assert_eq!(None, map_get(&state, 0, 200));
    assert_eq!(2, agg_count(&state, 1));
}

#[test]
fn for_each_all_events_match() {
    let prog = build_for_each_program(&[TYPE_A]);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let types = [TYPE_A; 3];
    let keys = [10u32, 20, 30];
    let vals = [100u32, 200, 300];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&keys),
        u32s_as_bytes(&vals),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 3));

    assert_eq!(3, slot_size(&state, 0));
    assert_eq!(3, agg_count(&state, 1));
}

#[test]
fn for_each_no_events_match() {
    let prog = build_for_each_program(&[TYPE_A]);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let types = [TYPE_B; 2];
    let keys = [10u32, 20];
    let vals = [100u32, 200];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&keys),
        u32s_as_bytes(&vals),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 2));

    assert_eq!(0, slot_size(&state, 0));
    assert_eq!(0, agg_count(&state, 1));
}

#[test]
fn for_each_multi_match_2() {
    let prog = build_for_each_program(&[TYPE_A, TYPE_B]);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let types = [TYPE_A, TYPE_B, TYPE_C, TYPE_A];
    let keys = [100u32, 200, 300, 400];
    let vals = [10u32, 20, 30, 40];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&keys),
        u32s_as_bytes(&vals),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 4));

    assert_eq!(3, slot_size(&state, 0));
    assert_eq!(Some(10), map_get(&state, 0, 100));
    assert_eq!(Some(20), map_get(&state, 0, 200));
    assert_eq!(Some(40), map_get(&state, 0, 400));
    assert_eq!(None, map_get(&state, 0, 300));
    assert_eq!(3, agg_count(&state, 1));
}

#[test]
fn for_each_multi_match_3_all_types_match() {
    let prog = build_for_each_program(&[10, 20, 30]);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let types = [10u32, 20, 30];
    let keys = [1u32, 2, 3];
    let vals = [111u32, 222, 333];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&keys),
        u32s_as_bytes(&vals),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 3));

    assert_eq!(3, slot_size(&state, 0));
    assert_eq!(3, agg_count(&state, 1));
}

#[test]
fn for_each_struct_map_upsert_with_type_filtering() {
    let prog = build_block_struct_map_program(TYPE_A);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let types = [TYPE_A, TYPE_B, TYPE_A];
    let keys = [100u32, 200, 300];
    let vals = [42u32, 99, 7];
    let strs = [1001u32, 1002, 1003];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&keys),
        u32s_as_bytes(&vals),
        u32s_as_bytes(&strs),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 3));

    assert_eq!(2, slot_size(&state, 0));
    let row_100 = present_row(&state, 0, 100);
    assert_eq!(42, bytes::read_u32(&state, row_100 + 1)); // bitset(1) + field 0
    assert_eq!(None, struct_map_row(&state, 0, 200));
}

// =============================================================================
// FLAT_MAP ()
// =============================================================================

#[test]
fn flat_map_basic_two_parents_with_children() {
    let prog = build_flat_map_program(TYPE_A);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let types = [TYPE_A, TYPE_A];
    let offsets = [0u32, 2, 3];
    let child_keys = [100u32, 200, 300];
    let child_v0 = [10u32, 20, 30];
    let child_v1 = [1001u32, 1002, 1003];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&offsets),
        u32s_as_bytes(&child_keys),
        u32s_as_bytes(&child_v0),
        u32s_as_bytes(&child_v1),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 2));

    assert_eq!(3, slot_size(&state, 0));
    let r100 = present_row(&state, 0, 100);
    assert_eq!(10, bytes::read_u32(&state, r100 + 1));
    assert_eq!(1001, bytes::read_u32(&state, r100 + 5));
    let r200 = present_row(&state, 0, 200);
    assert_eq!(20, bytes::read_u32(&state, r200 + 1));
    let r300 = present_row(&state, 0, 300);
    assert_eq!(30, bytes::read_u32(&state, r300 + 1));
}

#[test]
fn flat_map_empty_parent_zero_children() {
    let prog = build_flat_map_program(TYPE_A);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let types = [TYPE_A, TYPE_A];
    let offsets = [0u32, 0, 1];
    let child_keys = [500u32];
    let child_v0 = [55u32];
    let child_v1 = [9999u32];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&offsets),
        u32s_as_bytes(&child_keys),
        u32s_as_bytes(&child_v0),
        u32s_as_bytes(&child_v1),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 2));
    assert_eq!(1, slot_size(&state, 0));
}

#[test]
fn flat_map_key_collision_last_child_wins() {
    let prog = build_flat_map_program(TYPE_A);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let types = [TYPE_A];
    let offsets = [0u32, 2];
    let child_keys = [100u32, 100];
    let child_v0 = [10u32, 99];
    let child_v1 = [1001u32, 2002];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&offsets),
        u32s_as_bytes(&child_keys),
        u32s_as_bytes(&child_v0),
        u32s_as_bytes(&child_v1),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 1));

    assert_eq!(1, slot_size(&state, 0));
    let row = present_row(&state, 0, 100);
    assert_eq!(99, bytes::read_u32(&state, row + 1));
    assert_eq!(2002, bytes::read_u32(&state, row + 5));
}

#[test]
fn flat_map_type_filtering_skips_non_matching_parents() {
    let prog = build_flat_map_program(TYPE_A);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let types = [TYPE_A, TYPE_B, TYPE_A];
    let offsets = [0u32, 2, 3, 4];
    let child_keys = [100u32, 200, 999, 300];
    let child_v0 = [10u32, 20, 99, 30];
    let child_v1 = [1001u32, 1002, 9999, 1003];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&offsets),
        u32s_as_bytes(&child_keys),
        u32s_as_bytes(&child_v0),
        u32s_as_bytes(&child_v1),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 3));

    assert_eq!(3, slot_size(&state, 0));
    assert_eq!(None, struct_map_row(&state, 0, 999));
    assert!(struct_map_row(&state, 0, 300).is_some());
}

#[test]
fn flat_map_growth_small_capacity_triggers_needs_growth() {
    let prog = build_flat_map_program(TYPE_A);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let types = [TYPE_A];
    let offsets = [0u32, 12];
    let child_keys: Vec<u32> = (1..=12u32).map(|i| i * 100).collect();
    let child_v0: Vec<u32> = (1..=12u32).map(|i| i * 10).collect();
    let child_v1: Vec<u32> = (1..=12u32).collect();
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&offsets),
        u32s_as_bytes(&child_keys),
        u32s_as_bytes(&child_v0),
        u32s_as_bytes(&child_v1),
    ];
    // 12 unique keys > max_size = 16*7/10 = 11.
    assert_eq!(NEEDS_GROWTH, vm.execute_batch(&mut state, &prog, &cols, 1));
}

// =============================================================================
// STRUCT_MAP_PROBE_SCATTER 0x2f ()
// =============================================================================

fn run_scatter_one(vm: &mut Vm, state: &mut [u8], prog: &[u8], type_id: u32, key: u32) -> u32 {
    let types = [type_id];
    let offsets = [0u32, 1];
    let keys = [key];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&offsets),
        u32s_as_bytes(&keys),
    ];
    vm.execute_batch(state, prog, &cols, 1)
}

#[test]
fn probe_scatter_apply_parity_across_kinds_and_skip() {
    const T: u32 = 2002;
    const SKIP: u32 = 0xFFFF_FFFF;
    let prog = build_probe_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    seed_probe_row(&mut state, 10, 0, 0, 500, 7001, 0.0, 0);
    seed_probe_row(&mut state, 11, 1, 0, 500, 7002, 0.0, 0);
    seed_probe_row(&mut state, 12, 2, 0, 0, 7003, 0.0, 8003);
    seed_probe_row(&mut state, 13, 3, 0, 0, 7004, 0.0, 8004);
    seed_probe_row(&mut state, 14, 0, 1, 500, 7001, 0.0, 0);
    seed_probe_row(&mut state, 15, 1, 1, 500, 9999, 0.0, 0);
    seed_probe_row(&mut state, 16, 4, 0, 500, 0, 2.5, 0);
    seed_probe_row(&mut state, 17, SKIP, 0, 0, 0, 0.0, 0);
    seed_probe_row(&mut state, 18, 3, 1, 0, 7004, 0.0, 8004);

    let types = [T];
    let offsets = [0u32, 10];
    let keys = [10u32, 11, 12, 12, 13, 18, 17, 14, 15, 16];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&offsets),
        u32s_as_bytes(&keys),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 1));

    // nodes (slot 1): row 500 — bitset(1) + title(4) + status(4) + boost(8).
    let row_500 = present_row(&state, 1, 500);
    assert!(!bit_set(&state, row_500, 0)); // title retracted (MATCH)
    assert_eq!(0, bytes::read_u32(&state, row_500 + 1));
    assert!(bit_set(&state, row_500, 1)); // status survives NON-MATCH retract
    assert_eq!(7002, bytes::read_u32(&state, row_500 + 5));
    assert!(bit_set(&state, row_500, 2)); // boost via typed v_num
    assert_eq!(2.5, f64::from_bits(bytes::read_u64(&state, row_500 + 9)));

    // nodeDeps (slot 2): {(e,7003)=8003} deduped → size 1.
    assert_eq!(1, slot_size(&state, 2));
    assert!(set_contains(&state, 2, 8003));

    // nodeTouch (slot 3): asserted then retracted → empty.
    assert_eq!(0, slot_size(&state, 3));
    assert!(!set_contains(&state, 3, 8004));

    // A probed row whose output key is a sentinel names no destination row.
    // Growing the destination cannot place it, so it is refused by name,
    // with nothing written, rather than as a request for growth.
    for sentinel in [EMPTY_KEY, EMPTY_KEY - 1] {
        seed_probe_row(&mut state, 19, 0, 0, sentinel, 7001, 0.0, 0);
        let before = state.clone();
        assert_eq!(
            ErrorCode::InvalidKey as u32,
            run_scatter_one(&mut vm, &mut state, &prog, T, 19)
        );
        assert_eq!(before, state, "the refused row writes nothing");
    }
}

#[test]
fn probe_scatter_kind0_insert_sets_effective_flag_without_delta_mode() {
    const T: u32 = 2006;
    let prog = build_probe_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    seed_probe_row(&mut state, 10, 0, 0, 600, 7001, 0.0, 0);
    vm.undo_enable(&state).expect("undo window opens");
    let cp = vm.undo_checkpoint();

    assert_eq!(OK, run_scatter_one(&mut vm, &mut state, &prog, T, 10));
    assert_eq!(ChangeFlag::INSERTED, slot_change_flags(&state, 1));
    assert!(vm.undo_checkpoint() > cp);
    let row = present_row(&state, 1, 600);
    assert!(bit_set(&state, row, 0));
    assert_eq!(7001, bytes::read_u32(&state, row + 1));
}

#[test]
fn probe_scatter_kind0_differing_assert_sets_updated_without_delta_mode() {
    const T: u32 = 2007;
    let prog = build_probe_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    seed_probe_row(&mut state, 10, 0, 0, 600, 7001, 0.0, 0);
    seed_probe_row(&mut state, 11, 0, 0, 600, 7002, 0.0, 0);
    assert_eq!(OK, run_scatter_one(&mut vm, &mut state, &prog, T, 10));
    clear_slot_change_flags(&mut state, 1);
    vm.undo_enable(&state).expect("undo window opens");
    let cp = vm.undo_checkpoint();

    assert_eq!(OK, run_scatter_one(&mut vm, &mut state, &prog, T, 11));
    assert_eq!(ChangeFlag::UPDATED, slot_change_flags(&state, 1));
    assert!(vm.undo_checkpoint() > cp);
    let row = present_row(&state, 1, 600);
    assert!(bit_set(&state, row, 0));
    assert_eq!(7002, bytes::read_u32(&state, row + 1));
}

#[test]
fn probe_scatter_kind0_identical_assert_is_noop_without_undo_or_flag() {
    const T: u32 = 2008;
    let prog = build_probe_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    seed_probe_row(&mut state, 10, 0, 0, 600, 7001, 0.0, 0);
    assert_eq!(OK, run_scatter_one(&mut vm, &mut state, &prog, T, 10));
    clear_slot_change_flags(&mut state, 1);
    vm.undo_enable(&state).expect("undo window opens");
    let cp = vm.undo_checkpoint();
    let before = state.clone();

    assert_eq!(OK, run_scatter_one(&mut vm, &mut state, &prog, T, 10));
    assert_eq!(0, slot_change_flags(&state, 1));
    assert_eq!(cp, vm.undo_checkpoint());
    assert_eq!(before, state);
}

#[test]
fn probe_scatter_kind0_retract_only_matching_value_removes() {
    const T: u32 = 2009;
    let prog = build_probe_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    seed_probe_row(&mut state, 10, 0, 0, 600, 7001, 0.0, 0);
    seed_probe_row(&mut state, 11, 0, 1, 600, 9999, 0.0, 0);
    seed_probe_row(&mut state, 12, 0, 1, 600, 7001, 0.0, 0);
    assert_eq!(OK, run_scatter_one(&mut vm, &mut state, &prog, T, 10));
    clear_slot_change_flags(&mut state, 1);
    vm.undo_enable(&state).expect("undo window opens");
    let cp = vm.undo_checkpoint();
    let before = state.clone();

    assert_eq!(OK, run_scatter_one(&mut vm, &mut state, &prog, T, 11));
    assert_eq!(0, slot_change_flags(&state, 1));
    assert_eq!(cp, vm.undo_checkpoint());
    assert_eq!(before, state);

    assert_eq!(OK, run_scatter_one(&mut vm, &mut state, &prog, T, 12));
    assert_eq!(ChangeFlag::REMOVED, slot_change_flags(&state, 1));
    assert!(vm.undo_checkpoint() > cp);
    let row = present_row(&state, 1, 600);
    assert!(!bit_set(&state, row, 0));
    assert_eq!(0, bytes::read_u32(&state, row + 1));
}

#[test]
fn probe_scatter_card_many_keeps_two_entities_sharing_a_value_distinct() {
    const T: u32 = 2005;
    let prog = build_probe_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    seed_probe_row(&mut state, 20, 2, 0, 100, 7003, 0.0, 8101);
    seed_probe_row(&mut state, 21, 2, 0, 200, 7003, 0.0, 8102);

    let types = [T];
    let offsets = [0u32, 2];
    let keys = [20u32, 21];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&offsets),
        u32s_as_bytes(&keys),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 1));

    assert_eq!(2, slot_size(&state, 2));
    assert!(set_contains(&state, 2, 8101));
    assert!(set_contains(&state, 2, 8102));
    assert!(!set_contains(&state, 2, 7003));
}

#[test]
fn probe_scatter_kind0_assert_then_rollback_restores_absence() {
    const T: u32 = 2003;
    let prog = build_probe_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    seed_probe_row(&mut state, 10, 0, 0, 600, 7001, 0.0, 0);

    assert_eq!(None, struct_map_row(&state, 1, 600));

    let before = state.clone();
    vm.undo_enable(&state).expect("undo window opens");
    let cp = vm.undo_checkpoint();
    assert_eq!(OK, run_scatter_one(&mut vm, &mut state, &prog, T, 10));

    let row_after = present_row(&state, 1, 600);
    assert!(bit_set(&state, row_after, 0));
    assert_eq!(7001, bytes::read_u32(&state, row_after + 1));
    assert_eq!(1, slot_size(&state, 1));

    vm.undo_rollback(&mut state, cp).expect("valid checkpoint");
    assert_eq!(before, state);
    assert_eq!(None, struct_map_row(&state, 1, 600));
    assert_eq!(0, slot_size(&state, 1));
    vm.undo_commit();
}

#[test]
fn probe_scatter_kind0_retract_clear_then_rollback_restores_value_and_bit() {
    const T: u32 = 2004;
    let prog = build_probe_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    seed_probe_row(&mut state, 10, 0, 0, 700, 7001, 0.0, 0);
    seed_probe_row(&mut state, 11, 0, 1, 700, 7001, 0.0, 0);

    // Commit the assert WITHOUT undo.
    assert_eq!(OK, run_scatter_one(&mut vm, &mut state, &prog, T, 10));
    let row = present_row(&state, 1, 700);
    assert!(bit_set(&state, row, 0));
    assert_eq!(7001, bytes::read_u32(&state, row + 1));

    let before = state.clone();
    vm.undo_enable(&state).expect("undo window opens");
    let cp = vm.undo_checkpoint();
    assert_eq!(OK, run_scatter_one(&mut vm, &mut state, &prog, T, 11));
    assert!(!bit_set(&state, row, 0));
    assert_eq!(0, bytes::read_u32(&state, row + 1));

    vm.undo_rollback(&mut state, cp).expect("valid checkpoint");
    assert_eq!(before, state);
    assert!(bit_set(&state, row, 0));
    assert_eq!(7001, bytes::read_u32(&state, row + 1));
    vm.undo_commit();
}

// =============================================================================
// STRUCT_MAP_SCATTER 0x3e — probe-free attribute-routed dispatch ()
// =============================================================================

/// One resolved element of a `BATCH_STRUCT_MAP_SCATTER` batch.
/// Columns: 0=type(FOR_EACH filter), 1=offsets(FLAT_MAP child), 2=route(u32),
/// 3=op(u32), 4=key(u32), 5=v_str(interned u32), 6=v_num(f64),
/// 7=v_set(interned composite u32).
struct ScatterElement {
    route: u32,
    op_retract: u32,
    key: u32,
    v_str: u32,
    v_num: f64,
    v_set: u32,
}

const SKIP_ROUTE: u32 = 0xFFFF_FFFF;

/// FOR_EACH { FLAT_MAP { 0x3e } } over slots 0=nodes, 1=nodeDeps, 2=nodeTouch.
/// Route table mirrors the 0x2f suite's five routes minus the probe.
fn build_struct_map_scatter_program(type_id: u32) -> Vec<u8> {
    let mut init = Vec::new();
    // Slot 0: nodes [title:STR, status:STR, boost:F64]
    init.extend(slot_struct_map(0, 6, 4, &[4, 4, 2]));
    // Slots 1, 2: HASHSET nodeDeps / nodeTouch
    init.extend(slot_def(1, 1, 4, 0));
    init.extend(slot_def(2, 1, 4, 0));

    // 0x3e body: route_col=2, op_col=3, key_col=4, 5 routes.
    #[rustfmt::skip]
    let scatter: [u8; 25] = [
        0x3e, 2, 3, 4, 5,
        0, 0, 0, 5, // route 0: kind0 -> nodes.title, v=v_str
        0, 0, 1, 5, // route 1: kind0 -> nodes.status, v=v_str
        1, 1, 0, 7, // route 2: kind1 -> nodeDeps, v=v_set
        1, 2, 0, 7, // route 3: kind1 -> nodeTouch, v=v_set
        0, 0, 2, 6, // route 4: kind0 -> nodes.boost, v=v_num
    ];
    let mut flat_map = vec![0xE1, 1];
    flat_map.extend(encode_index(NO_PARENT_TS_COL).as_bytes());
    flat_map.extend((scatter.len() as u16).to_le_bytes());
    flat_map.extend(scatter);
    let mut reduce = vec![0xE0, 0, 1];
    reduce.extend(type_id.to_le_bytes());
    reduce.extend((flat_map.len() as u16).to_le_bytes());
    reduce.extend(&flat_map);
    program(3, 8, &init, &reduce)
}

fn run_struct_scatter(
    vm: &mut Vm,
    state: &mut [u8],
    prog: &[u8],
    type_id: u32,
    elements: &[ScatterElement],
) -> u32 {
    run_struct_scatter_lane(vm, state, prog, type_id, elements, false)
}

/// [`run_struct_scatter`] on either lane; `delta` journals every element as a pair.
fn run_struct_scatter_lane(
    vm: &mut Vm,
    state: &mut [u8],
    prog: &[u8],
    type_id: u32,
    elements: &[ScatterElement],
    delta: bool,
) -> u32 {
    let types: Vec<u32> = vec![type_id; elements.len()];
    let routes: Vec<u32> = elements.iter().map(|e| e.route).collect();
    let ops: Vec<u32> = elements.iter().map(|e| e.op_retract).collect();
    let keys: Vec<u32> = elements.iter().map(|e| e.key).collect();
    let v_strs: Vec<u32> = elements.iter().map(|e| e.v_str).collect();
    let v_nums: Vec<f64> = elements.iter().map(|e| e.v_num).collect();
    let v_sets: Vec<u32> = elements.iter().map(|e| e.v_set).collect();
    let n = elements.len() as u32;
    let offsets = [0u32, n];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&offsets),
        u32s_as_bytes(&routes),
        u32s_as_bytes(&ops),
        u32s_as_bytes(&keys),
        u32s_as_bytes(&v_strs),
        f64s_as_bytes(&v_nums),
        u32s_as_bytes(&v_sets),
    ];
    if delta {
        vm.execute_batch_delta(state, prog, &cols, 1)
    } else {
        vm.execute_batch(state, prog, &cols, 1)
    }
}

/// The 0x2f parity fixture replayed through the probe-free opcode: same
/// elements, same final slots, no staged row materialized anywhere.
#[test]
fn struct_scatter_apply_parity_across_kinds_and_skip() {
    const T: u32 = 2102;
    let prog = build_struct_map_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let elements = [
        ScatterElement {
            route: 0,
            op_retract: 0,
            key: 500,
            v_str: 7001,
            v_num: 0.0,
            v_set: 0,
        },
        ScatterElement {
            route: 1,
            op_retract: 0,
            key: 500,
            v_str: 7002,
            v_num: 0.0,
            v_set: 0,
        },
        ScatterElement {
            route: 2,
            op_retract: 0,
            key: 0,
            v_str: 7003,
            v_num: 0.0,
            v_set: 8003,
        },
        ScatterElement {
            route: 3,
            op_retract: 0,
            key: 0,
            v_str: 7004,
            v_num: 0.0,
            v_set: 8004,
        },
        ScatterElement {
            route: 0,
            op_retract: 1,
            key: 500,
            v_str: 7001,
            v_num: 0.0,
            v_set: 0,
        },
        ScatterElement {
            route: 1,
            op_retract: 1,
            key: 500,
            v_str: 9999,
            v_num: 0.0,
            v_set: 0,
        },
        ScatterElement {
            route: 4,
            op_retract: 0,
            key: 500,
            v_str: 0,
            v_num: 2.5,
            v_set: 0,
        },
        ScatterElement {
            route: SKIP_ROUTE,
            op_retract: 0,
            key: 0,
            v_str: 0,
            v_num: 0.0,
            v_set: 0,
        },
        ScatterElement {
            route: 3,
            op_retract: 1,
            key: 0,
            v_str: 7004,
            v_num: 0.0,
            v_set: 8004,
        },
    ];
    assert_eq!(
        OK,
        run_struct_scatter(&mut vm, &mut state, &prog, T, &elements)
    );

    // nodes (slot 0): row 500 — bitset(1) + title(4) + status(4) + boost(8).
    let row_500 = present_row(&state, 0, 500);
    assert!(!bit_set(&state, row_500, 0)); // title retracted (MATCH)
    assert_eq!(0, bytes::read_u32(&state, row_500 + 1));
    assert!(bit_set(&state, row_500, 1)); // status survives NON-MATCH retract
    assert_eq!(7002, bytes::read_u32(&state, row_500 + 5));
    assert!(bit_set(&state, row_500, 2)); // boost via typed v_num
    assert_eq!(2.5, f64::from_bits(bytes::read_u64(&state, row_500 + 9)));

    // nodeDeps (slot 1): {(e,7003)=8003} deduped -> size 1.
    assert_eq!(1, slot_size(&state, 1));
    assert!(set_contains(&state, 1, 8003));

    // nodeTouch (slot 2): asserted then retracted -> empty.
    assert_eq!(0, slot_size(&state, 2));
    assert!(!set_contains(&state, 2, 8004));
}

#[test]
fn struct_scatter_kind0_insert_sets_effective_flag_without_delta_mode() {
    const T: u32 = 2106;
    let prog = build_struct_map_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let assert_title = [ScatterElement {
        route: 0,
        op_retract: 0,
        key: 600,
        v_str: 7001,
        v_num: 0.0,
        v_set: 0,
    }];
    vm.undo_enable(&state).expect("undo window opens");
    let cp = vm.undo_checkpoint();

    assert_eq!(
        OK,
        run_struct_scatter(&mut vm, &mut state, &prog, T, &assert_title)
    );
    assert_eq!(ChangeFlag::INSERTED, slot_change_flags(&state, 0));
    assert!(vm.undo_checkpoint() > cp);
    let row = present_row(&state, 0, 600);
    assert!(bit_set(&state, row, 0));
    assert_eq!(7001, bytes::read_u32(&state, row + 1));
}

#[test]
fn struct_scatter_kind0_differing_assert_sets_updated_without_delta_mode() {
    const T: u32 = 2107;
    let prog = build_struct_map_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let first = [ScatterElement {
        route: 0,
        op_retract: 0,
        key: 600,
        v_str: 7001,
        v_num: 0.0,
        v_set: 0,
    }];
    let second = [ScatterElement {
        route: 0,
        op_retract: 0,
        key: 600,
        v_str: 7002,
        v_num: 0.0,
        v_set: 0,
    }];
    assert_eq!(
        OK,
        run_struct_scatter(&mut vm, &mut state, &prog, T, &first)
    );
    clear_slot_change_flags(&mut state, 0);
    vm.undo_enable(&state).expect("undo window opens");
    let cp = vm.undo_checkpoint();

    assert_eq!(
        OK,
        run_struct_scatter(&mut vm, &mut state, &prog, T, &second)
    );
    assert_eq!(ChangeFlag::UPDATED, slot_change_flags(&state, 0));
    assert!(vm.undo_checkpoint() > cp);
    let row = present_row(&state, 0, 600);
    assert!(bit_set(&state, row, 0));
    assert_eq!(7002, bytes::read_u32(&state, row + 1));
}

#[test]
fn struct_scatter_kind0_identical_assert_is_noop_without_undo_or_flag() {
    const T: u32 = 2108;
    let prog = build_struct_map_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let element = [ScatterElement {
        route: 0,
        op_retract: 0,
        key: 600,
        v_str: 7001,
        v_num: 0.0,
        v_set: 0,
    }];
    assert_eq!(
        OK,
        run_struct_scatter(&mut vm, &mut state, &prog, T, &element)
    );
    clear_slot_change_flags(&mut state, 0);
    vm.undo_enable(&state).expect("undo window opens");
    let cp = vm.undo_checkpoint();
    let before = state.clone();

    assert_eq!(
        OK,
        run_struct_scatter(&mut vm, &mut state, &prog, T, &element)
    );
    assert_eq!(0, slot_change_flags(&state, 0));
    assert_eq!(cp, vm.undo_checkpoint());
    assert_eq!(before, state);
}

#[test]
fn struct_scatter_kind0_retract_only_matching_value_removes() {
    const T: u32 = 2109;
    let prog = build_struct_map_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let assert_title = [ScatterElement {
        route: 0,
        op_retract: 0,
        key: 600,
        v_str: 7001,
        v_num: 0.0,
        v_set: 0,
    }];
    let nonmatch = [ScatterElement {
        route: 0,
        op_retract: 1,
        key: 600,
        v_str: 9999,
        v_num: 0.0,
        v_set: 0,
    }];
    let matching = [ScatterElement {
        route: 0,
        op_retract: 1,
        key: 600,
        v_str: 7001,
        v_num: 0.0,
        v_set: 0,
    }];
    assert_eq!(
        OK,
        run_struct_scatter(&mut vm, &mut state, &prog, T, &assert_title)
    );
    clear_slot_change_flags(&mut state, 0);
    vm.undo_enable(&state).expect("undo window opens");
    let cp = vm.undo_checkpoint();
    let before = state.clone();

    assert_eq!(
        OK,
        run_struct_scatter(&mut vm, &mut state, &prog, T, &nonmatch)
    );
    assert_eq!(0, slot_change_flags(&state, 0));
    assert_eq!(cp, vm.undo_checkpoint());
    assert_eq!(before, state);

    assert_eq!(
        OK,
        run_struct_scatter(&mut vm, &mut state, &prog, T, &matching)
    );
    assert_eq!(ChangeFlag::REMOVED, slot_change_flags(&state, 0));
    assert!(vm.undo_checkpoint() > cp);
    let row = present_row(&state, 0, 600);
    assert!(!bit_set(&state, row, 0));
    assert_eq!(0, bytes::read_u32(&state, row + 1));
}

#[test]
fn struct_scatter_card_many_keeps_two_entities_sharing_a_value_distinct() {
    const T: u32 = 2105;
    let prog = build_struct_map_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let elements = [
        ScatterElement {
            route: 2,
            op_retract: 0,
            key: 100,
            v_str: 7003,
            v_num: 0.0,
            v_set: 8101,
        },
        ScatterElement {
            route: 2,
            op_retract: 0,
            key: 200,
            v_str: 7003,
            v_num: 0.0,
            v_set: 8102,
        },
    ];
    assert_eq!(
        OK,
        run_struct_scatter(&mut vm, &mut state, &prog, T, &elements)
    );

    assert_eq!(2, slot_size(&state, 1));
    assert!(set_contains(&state, 1, 8101));
    assert!(set_contains(&state, 1, 8102));
    assert!(!set_contains(&state, 1, 7003));
}

#[test]
fn struct_scatter_kind0_assert_then_rollback_restores_absence() {
    const T: u32 = 2103;
    let prog = build_struct_map_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let assert_title = [ScatterElement {
        route: 0,
        op_retract: 0,
        key: 600,
        v_str: 7001,
        v_num: 0.0,
        v_set: 0,
    }];

    assert_eq!(None, struct_map_row(&state, 0, 600));

    let before = state.clone();
    vm.undo_enable(&state).expect("undo window opens");
    let cp = vm.undo_checkpoint();
    assert_eq!(
        OK,
        run_struct_scatter(&mut vm, &mut state, &prog, T, &assert_title)
    );

    let row_after = present_row(&state, 0, 600);
    assert!(bit_set(&state, row_after, 0));
    assert_eq!(7001, bytes::read_u32(&state, row_after + 1));
    assert_eq!(1, slot_size(&state, 0));

    vm.undo_rollback(&mut state, cp).expect("valid checkpoint");
    assert_eq!(before, state);
    assert_eq!(None, struct_map_row(&state, 0, 600));
    assert_eq!(0, slot_size(&state, 0));
    vm.undo_commit();
}

#[test]
fn struct_scatter_kind0_retract_clear_then_rollback_restores_value_and_bit() {
    const T: u32 = 2104;
    let prog = build_struct_map_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let assert_title = [ScatterElement {
        route: 0,
        op_retract: 0,
        key: 700,
        v_str: 7001,
        v_num: 0.0,
        v_set: 0,
    }];
    let retract_title = [ScatterElement {
        route: 0,
        op_retract: 1,
        key: 700,
        v_str: 7001,
        v_num: 0.0,
        v_set: 0,
    }];

    // Commit the assert WITHOUT undo.
    assert_eq!(
        OK,
        run_struct_scatter(&mut vm, &mut state, &prog, T, &assert_title)
    );
    let row = present_row(&state, 0, 700);
    assert!(bit_set(&state, row, 0));
    assert_eq!(7001, bytes::read_u32(&state, row + 1));

    let before = state.clone();
    vm.undo_enable(&state).expect("undo window opens");
    let cp = vm.undo_checkpoint();
    assert_eq!(
        OK,
        run_struct_scatter(&mut vm, &mut state, &prog, T, &retract_title)
    );
    assert!(!bit_set(&state, row, 0));
    assert_eq!(0, bytes::read_u32(&state, row + 1));

    vm.undo_rollback(&mut state, cp).expect("valid checkpoint");
    assert!(bit_set(&state, row, 0));
    assert_eq!(7001, bytes::read_u32(&state, row + 1));
    assert_eq!(before, state);
    vm.undo_commit();
}

/// A route kind outside the implemented set refuses by name — the same
/// INVALID_PROGRAM contract 0x2f carries for its Phase-2 placeholder.
#[test]
fn struct_scatter_unknown_route_kind_is_invalid_program() {
    const T: u32 = 2110;
    let prog = build_struct_map_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    // Kind 2 routes to a real slot but is never emitted; the boundary refuses.
    let mut patched = prog.clone();
    let kind_off = prog
        .windows(25)
        .position(|w| {
            w == [
                0x3e, 2, 3, 4, 5, 0, 0, 0, 5, 0, 0, 1, 5, 1, 1, 0, 7, 1, 2, 0, 7, 0, 0, 2, 6,
            ]
        })
        .expect("0x3e body in program");
    patched[kind_off + 5 + 4 * 4] = 2; // route 4's kind byte
    let element = [ScatterElement {
        route: 4,
        op_retract: 0,
        key: 600,
        v_str: 0,
        v_num: 1.0,
        v_set: 0,
    }];
    assert_eq!(
        ErrorCode::InvalidProgram as u32,
        run_struct_scatter(&mut vm, &mut state, &patched, T, &element)
    );
}

/// An array-typed destination field has no column-cell value form; the
/// boundary refuses by name instead of comparing field bytes against garbage.
#[test]
fn struct_scatter_array_destination_is_invalid_program() {
    const T: u32 = 2111;
    let mut init_code = Vec::new();
    // Slot 0: nodes [title:STR, tags:ARRAY_U32]
    init_code.extend(slot_struct_map(0, 6, 4, &[4, 5]));
    let scatter: [u8; 9] = [0x3e, 2, 3, 4, 1, 0, 0, 1, 5];
    let mut flat_map = vec![0xE1, 1];
    flat_map.extend(encode_index(NO_PARENT_TS_COL).as_bytes());
    flat_map.extend((scatter.len() as u16).to_le_bytes());
    flat_map.extend(scatter);
    let mut reduce = vec![0xE0, 0, 1];
    reduce.extend(T.to_le_bytes());
    reduce.extend((flat_map.len() as u16).to_le_bytes());
    reduce.extend(&flat_map);
    let prog = program(1, 8, &init_code, &reduce);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let element = [ScatterElement {
        route: 0,
        op_retract: 0,
        key: 600,
        v_str: 7001,
        v_num: 0.0,
        v_set: 0,
    }];
    assert_eq!(
        ErrorCode::InvalidProgram as u32,
        run_struct_scatter(&mut vm, &mut state, &prog, T, &element)
    );
}

/// A route table larger than the decode arrays refuses at the boundary —
/// the same admission contract for both scatter forms (92 §4: reject at
/// sizing, never overflow later).
#[test]
fn struct_scatter_over_capacity_route_table_is_invalid_program() {
    const T: u32 = 2113;
    let prog = build_struct_map_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    // The declared count must be validated against MAX_SCATTER_ROUTES, not
    // left to index past the decode arrays; the table itself is junk rows of
    // route 0 so only the count check can refuse.
    let mut scatter = vec![0x3eu8, 2, 3, 4, 33];
    let route = [0u8, 0, 0, 5];
    for _ in 0..33 {
        scatter.extend_from_slice(&route);
    }
    let mut flat_map = vec![0xE1, 1];
    flat_map.extend(encode_index(NO_PARENT_TS_COL).as_bytes());
    flat_map.extend((scatter.len() as u16).to_le_bytes());
    flat_map.extend(scatter);
    let mut reduce = vec![0xE0, 0, 1];
    reduce.extend(T.to_le_bytes());
    reduce.extend((flat_map.len() as u16).to_le_bytes());
    reduce.extend(&flat_map);
    let mut full_init = Vec::new();
    full_init.extend(slot_struct_map(0, 6, 4, &[4, 4, 2]));
    full_init.extend(slot_def(1, 1, 4, 0));
    full_init.extend(slot_def(2, 1, 4, 0));
    let full = program(3, 8, &full_init, &reduce);
    let element = [ScatterElement {
        route: 0,
        op_retract: 0,
        key: 600,
        v_str: 7001,
        v_num: 0.0,
        v_set: 0,
    }];
    assert_eq!(
        ErrorCode::InvalidProgram as u32,
        run_struct_scatter(&mut vm, &mut state, &full, T, &element)
    );
}

// =============================================================================
// STRUCT_MAP_SCATTER_GUARDED 0x3f — 0x3e's dispatch behind a per-row guard
// =============================================================================

/// One resolved element of a `BATCH_STRUCT_MAP_SCATTER_GUARDED` batch.
/// Columns: 0=type(FOR_EACH filter), 1=offsets(FLAT_MAP child), 2=route(u32),
/// 3=op(u32), 4=key(u32), 5=v_str(interned u32), 6=v_set(interned composite
/// u32), 7=guard at_ns(i64), 8=guard ordinal(i64).
struct GuardedElement {
    route: u32,
    op_retract: u32,
    key: u32,
    v_str: u32,
    v_set: u32,
    at_ns: i64,
    ordinal: i64,
}

/// Row layout of the guarded `checks` slot: bitset(1) then verdict(4),
/// detail(4), at_ns(8), ordinal(8).
const VERDICT_AT: u32 = 1;
const DETAIL_AT: u32 = 5;
const AT_NS_AT: u32 = 9;
const ORDINAL_AT: u32 = 17;

/// FOR_EACH { FLAT_MAP { 0x3f } } over slot 0 = `checks`, a guarded struct
/// map \[verdict:STR, detail:STR, at_ns:I64, ordinal:I64\], and slot 1 =
/// `tags`, a HASHSET. The guard is (at_ns, ordinal) carried in columns 7
/// and 8 and remembered in fields 2 and 3 of the guarded row.
fn build_guarded_scatter_program(type_id: u32) -> Vec<u8> {
    let mut init = Vec::new();
    init.extend(slot_struct_map(0, 6, 8, &[4, 4, 1, 1]));
    init.extend(slot_def(1, 1, 8, 0));
    let mut reduce = vec![0xE0, 0, 1];
    reduce.extend(type_id.to_le_bytes());
    let flat_map = guarded_flat_map(&guarded_scatter_body());
    reduce.extend((flat_map.len() as u16).to_le_bytes());
    reduce.extend(&flat_map);
    program(2, 9, &init, &reduce)
}

/// The 0x3f body the suite routes through: guard_slot=0, two guard pairs,
/// three routes — two kind-0 columns of the guarded row and one kind-1 set.
fn guarded_scatter_body() -> Vec<u8> {
    #[rustfmt::skip]
    let body: [u8; 23] = [
        0x3f, 2, 3, 4, 0, 2,
        7, 2,       // guard 0: column 7 -> field 2 (at_ns), most significant
        8, 3,       // guard 1: column 8 -> field 3 (ordinal)
        3,          // num_routes
        0, 0, 0, 5, // route 0: kind0 -> checks.verdict, v=v_str
        0, 0, 1, 5, // route 1: kind0 -> checks.detail,  v=v_str
        1, 1, 0, 6, // route 2: kind1 -> tags,           v=v_set
    ];
    body.to_vec()
}

fn guarded_flat_map(body: &[u8]) -> Vec<u8> {
    let mut flat_map = vec![0xE1, 1];
    flat_map.extend(encode_index(NO_PARENT_TS_COL).as_bytes());
    flat_map.extend((body.len() as u16).to_le_bytes());
    flat_map.extend_from_slice(body);
    flat_map
}

fn run_guarded_scatter(
    vm: &mut Vm,
    state: &mut [u8],
    prog: &[u8],
    type_id: u32,
    elements: &[GuardedElement],
) -> u32 {
    run_guarded_scatter_lane(vm, state, prog, type_id, elements, false)
}

/// [`run_guarded_scatter`] on either lane; `delta` journals every element as a pair.
fn run_guarded_scatter_lane(
    vm: &mut Vm,
    state: &mut [u8],
    prog: &[u8],
    type_id: u32,
    elements: &[GuardedElement],
    delta: bool,
) -> u32 {
    let types: Vec<u32> = vec![type_id; elements.len()];
    let routes: Vec<u32> = elements.iter().map(|e| e.route).collect();
    let ops: Vec<u32> = elements.iter().map(|e| e.op_retract).collect();
    let keys: Vec<u32> = elements.iter().map(|e| e.key).collect();
    let v_strs: Vec<u32> = elements.iter().map(|e| e.v_str).collect();
    let v_sets: Vec<u32> = elements.iter().map(|e| e.v_set).collect();
    let at_ns: Vec<i64> = elements.iter().map(|e| e.at_ns).collect();
    let ordinals: Vec<i64> = elements.iter().map(|e| e.ordinal).collect();
    let n = elements.len() as u32;
    let offsets = [0u32, n];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&offsets),
        u32s_as_bytes(&routes),
        u32s_as_bytes(&ops),
        u32s_as_bytes(&keys),
        u32s_as_bytes(&v_strs),
        u32s_as_bytes(&v_sets),
        i64s_as_bytes(&at_ns),
        i64s_as_bytes(&ordinals),
    ];
    if delta {
        vm.execute_batch_delta(state, prog, &cols, 1)
    } else {
        vm.execute_batch(state, prog, &cols, 1)
    }
}

/// The three elements of one transaction against key `key`: verdict,
/// detail, and one set member, all carrying the same guard.
fn guarded_transaction(
    key: u32,
    verdict: u32,
    detail: u32,
    tag: u32,
    at_ns: i64,
    ordinal: i64,
) -> [GuardedElement; 3] {
    [
        GuardedElement {
            route: 0,
            op_retract: 0,
            key,
            v_str: verdict,
            v_set: 0,
            at_ns,
            ordinal,
        },
        GuardedElement {
            route: 1,
            op_retract: 0,
            key,
            v_str: detail,
            v_set: 0,
            at_ns,
            ordinal,
        },
        GuardedElement {
            route: 2,
            op_retract: 0,
            key,
            v_str: 0,
            v_set: tag,
            at_ns,
            ordinal,
        },
    ]
}

fn read_i64(state: &[u8], off: u32) -> i64 {
    bytes::read_u64(state, off) as i64
}

fn tag_present(state: &[u8], tag: u32) -> bool {
    set_contains(state, 1, tag)
}

/// The row-granularity rule: an element whose guard is strictly below the
/// row's writes through NO route of its group. The kind-1 arm is the one
/// that a per-route guard would leak — a set has no row to carry a guard,
/// so only the group gate can stop it.
#[test]
fn guarded_scatter_stale_transaction_writes_through_no_route_including_the_set() {
    const T: u32 = 3101;
    let prog = build_guarded_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let fresh = guarded_transaction(500, 7002, 7012, 8002, 2000, 20);
    assert_eq!(
        OK,
        run_guarded_scatter(&mut vm, &mut state, &prog, T, &fresh)
    );

    // Same row, an earlier transaction arriving late.
    let stale = guarded_transaction(500, 7001, 7011, 8001, 1000, 10);
    assert_eq!(
        OK,
        run_guarded_scatter(&mut vm, &mut state, &prog, T, &stale)
    );

    let row = present_row(&state, 0, 500);
    assert_eq!(7002, bytes::read_u32(&state, row + VERDICT_AT));
    assert_eq!(7012, bytes::read_u32(&state, row + DETAIL_AT));
    assert_eq!(2000, read_i64(&state, row + AT_NS_AT));
    assert_eq!(20, read_i64(&state, row + ORDINAL_AT));
    assert!(tag_present(&state, 8002));
    assert!(
        !tag_present(&state, 8001),
        "the stale transaction's set member is gated by the same row guard"
    );
    assert_eq!(1, slot_size(&state, 1));
}

/// Equal guards PROCEED, which is what lets one transaction's elements all
/// land: the first stamps the row and the rest compare equal. A
/// strictly-greater rule would leave the row holding only its first column.
/// The same test pins the order-independence a total guard order buys for a
/// card-one field, and the honest limit of it for a card-many set.
#[test]
fn guarded_scatter_equal_guard_completes_the_row_and_order_does_not_change_it() {
    const T: u32 = 3102;
    let prog = build_guarded_scatter_program(T);
    let mut vm = Vm::default();

    let older = guarded_transaction(500, 7001, 7011, 8001, 1000, 10);
    let newer = guarded_transaction(500, 7002, 7012, 8002, 2000, 20);

    let mut forward = init(&prog);
    assert_eq!(
        OK,
        run_guarded_scatter(&mut vm, &mut forward, &prog, T, &older)
    );
    // Every column of the older transaction landed: three elements, one
    // guard, none of them skipped.
    let row = present_row(&forward, 0, 500);
    assert_eq!(7001, bytes::read_u32(&forward, row + VERDICT_AT));
    assert_eq!(7011, bytes::read_u32(&forward, row + DETAIL_AT));
    assert!(tag_present(&forward, 8001));
    assert_eq!(
        OK,
        run_guarded_scatter(&mut vm, &mut forward, &prog, T, &newer)
    );

    let mut reverse = init(&prog);
    assert_eq!(
        OK,
        run_guarded_scatter(&mut vm, &mut reverse, &prog, T, &newer)
    );
    assert_eq!(
        OK,
        run_guarded_scatter(&mut vm, &mut reverse, &prog, T, &older)
    );

    let f_row = present_row(&forward, 0, 500);
    let r_row = present_row(&reverse, 0, 500);
    for (name, at) in [("verdict", VERDICT_AT), ("detail", DETAIL_AT)] {
        assert_eq!(
            bytes::read_u32(&forward, f_row + at),
            bytes::read_u32(&reverse, r_row + at),
            "card-one {name} does not depend on which order the two transactions arrived in"
        );
    }
    assert_eq!(7002, bytes::read_u32(&forward, f_row + VERDICT_AT));
    assert_eq!(2000, read_i64(&forward, f_row + AT_NS_AT));
    assert_eq!(2000, read_i64(&reverse, r_row + AT_NS_AT));

    // A set accumulates, so it converges to what was ACCEPTED, not to the
    // highest transaction: the older one contributed when it arrived first
    // and contributed nothing at all when it arrived last. All-or-nothing
    // per transaction is the guarantee; convergence is not.
    assert!(tag_present(&forward, 8001) && tag_present(&forward, 8002));
    assert!(tag_present(&reverse, 8002) && !tag_present(&reverse, 8001));
}

/// A retract is guarded too. The stale retract here names the value the row
/// actually holds, so retract-iff-current would fire and clear it without
/// the guard in front.
#[test]
fn guarded_scatter_stale_retract_does_not_delete_a_fresher_row() {
    const T: u32 = 3103;
    let prog = build_guarded_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let fresh = guarded_transaction(500, 7002, 7012, 8002, 2000, 20);
    assert_eq!(
        OK,
        run_guarded_scatter(&mut vm, &mut state, &prog, T, &fresh)
    );

    let stale_retract = [
        GuardedElement {
            route: 0,
            op_retract: 1,
            key: 500,
            v_str: 7002, // matches what is stored: an unguarded retract clears
            v_set: 0,
            at_ns: 1000,
            ordinal: 10,
        },
        GuardedElement {
            route: 2,
            op_retract: 1,
            key: 500,
            v_str: 0,
            v_set: 8002, // present in the set: an unguarded remove drops it
            at_ns: 1000,
            ordinal: 10,
        },
    ];
    assert_eq!(
        OK,
        run_guarded_scatter(&mut vm, &mut state, &prog, T, &stale_retract)
    );

    let row = present_row(&state, 0, 500);
    assert!(bit_set(&state, row, 0));
    assert_eq!(7002, bytes::read_u32(&state, row + VERDICT_AT));
    assert!(tag_present(&state, 8002));

    // The same retract at the row's own guard is not stale, and clears.
    let current_retract = [GuardedElement {
        route: 0,
        op_retract: 1,
        key: 500,
        v_str: 7002,
        v_set: 0,
        at_ns: 2000,
        ordinal: 20,
    }];
    assert_eq!(
        OK,
        run_guarded_scatter(&mut vm, &mut state, &prog, T, &current_retract)
    );
    assert!(!bit_set(&state, row, 0));
}

/// The one asymmetry between a passing assert and a passing retract: an
/// assert upserts the guarded row, a retract stamps only a row that
/// already exists. A retract of an absent row has nothing to clear, and a
/// map that grew a row per retracted unknown key would be unbounded in the
/// keys nothing ever asserted.
#[test]
fn guarded_scatter_a_retract_of_an_absent_row_creates_nothing() {
    const T: u32 = 3108;
    let prog = build_guarded_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let retracts: Vec<GuardedElement> = (0..4)
        .map(|i| GuardedElement {
            route: 0,
            op_retract: 1,
            key: 900 + i,
            v_str: 7002,
            v_set: 0,
            at_ns: 1000,
            ordinal: 10,
        })
        .collect();
    assert_eq!(
        OK,
        run_guarded_scatter(&mut vm, &mut state, &prog, T, &retracts)
    );
    assert_eq!(
        0,
        slot_size(&state, 0),
        "four retracts of keys nothing asserted leave the map empty"
    );
}

/// The stamp writes guard fields, so it journals them, and exactly one of
/// its entries claims the row it created — otherwise a rolled-back
/// transaction would leave a row of guard columns behind, and the next
/// element to arrive would lose to a transaction that never happened.
#[test]
fn guarded_scatter_rolling_back_a_transaction_removes_the_row_it_stamped() {
    const T: u32 = 3109;
    let prog = build_guarded_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let first = guarded_transaction(500, 7001, 7011, 8001, 1000, 10);
    assert_eq!(
        OK,
        run_guarded_scatter(&mut vm, &mut state, &prog, T, &first)
    );

    let before = state.clone();
    vm.undo_enable(&state).expect("undo window opens");
    let cp = vm.undo_checkpoint();
    let second = guarded_transaction(501, 7002, 7012, 8002, 2000, 20);
    assert_eq!(
        OK,
        run_guarded_scatter(&mut vm, &mut state, &prog, T, &second)
    );
    assert_eq!(2, slot_size(&state, 0));

    vm.undo_rollback(&mut state, cp).expect("valid checkpoint");
    assert_eq!(
        before, state,
        "rollback restores every guard bit and payload byte"
    );
    vm.undo_commit();
    assert_eq!(
        None,
        struct_map_row(&state, 0, 501),
        "the rolled-back transaction leaves no guard-only row behind"
    );
    assert_eq!(1, slot_size(&state, 0));

    // And the row it did not touch still remembers its own guard, so a
    // transaction older than the first still loses.
    let row = present_row(&state, 0, 500);
    assert_eq!(1000, read_i64(&state, row + AT_NS_AT));
    let older = guarded_transaction(500, 7009, 7019, 8009, 500, 5);
    assert_eq!(
        OK,
        run_guarded_scatter(&mut vm, &mut state, &prog, T, &older)
    );
    assert_eq!(7001, bytes::read_u32(&state, row + VERDICT_AT));

    // The stamped guard is also restored, not just the row: re-running the
    // rolled-back transaction against key 501 must succeed from scratch.
    assert_eq!(
        OK,
        run_guarded_scatter(&mut vm, &mut state, &prog, T, &second)
    );
    let restored = present_row(&state, 0, 501);
    assert_eq!(7002, bytes::read_u32(&state, restored + VERDICT_AT));
    assert_eq!(2000, read_i64(&state, restored + AT_NS_AT));
}

/// Replay is a no-op by construction: the guard is equal, so the elements
/// PROCEED into the write path, where the identical-assert check and
/// retract-iff-current make each write nothing. Byte equality of the whole
/// state is the claim — a stamp that rewrote the guard, or a set arm that
/// re-inserted, would show up here.
#[test]
fn guarded_scatter_replaying_an_identical_transaction_changes_no_state() {
    const T: u32 = 3104;
    let prog = build_guarded_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let tx = guarded_transaction(500, 7002, 7012, 8002, 2000, 20);
    assert_eq!(OK, run_guarded_scatter(&mut vm, &mut state, &prog, T, &tx));
    clear_slot_change_flags(&mut state, 0);
    clear_slot_change_flags(&mut state, 1);
    let applied = state.clone();

    assert_eq!(OK, run_guarded_scatter(&mut vm, &mut state, &prog, T, &tx));
    assert_eq!(
        applied, state,
        "an identical transaction replayed leaves every byte of state alone"
    );
}

/// The stamp precedes the route action, and the ordering is observable. A
/// transaction that re-asserts the value already stored takes the kind-0
/// identical-assert `continue`; if the guard were stamped after the write
/// it would never be stamped at all, and a transaction BETWEEN the two
/// would then still be able to overwrite the row.
#[test]
fn guarded_scatter_an_identical_assert_still_advances_the_row_guard() {
    const T: u32 = 3107;
    let prog = build_guarded_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let first = guarded_transaction(500, 7002, 7012, 8002, 1000, 10);
    assert_eq!(
        OK,
        run_guarded_scatter(&mut vm, &mut state, &prog, T, &first)
    );
    // The stamp comes first, so the row exists by the time the kind-0 arm
    // looks for it: on a row the stamp had not yet created, every kind-0
    // route would find nothing and the transaction would leave a row of
    // guard columns and no values.
    let row = present_row(&state, 0, 500);
    assert_eq!(7002, bytes::read_u32(&state, row + VERDICT_AT));
    assert_eq!(7012, bytes::read_u32(&state, row + DETAIL_AT));
    assert_eq!(1000, read_i64(&state, row + AT_NS_AT));

    // A later transaction re-asserting exactly what is stored: every route
    // writes nothing, and the row's guard must still move to 3000.
    let identical = guarded_transaction(500, 7002, 7012, 8002, 3000, 30);
    assert_eq!(
        OK,
        run_guarded_scatter(&mut vm, &mut state, &prog, T, &identical)
    );
    assert_eq!(3000, read_i64(&state, row + AT_NS_AT));

    // Between the two, and therefore stale against the row as it now
    // stands: it must not write.
    let between = guarded_transaction(500, 7009, 7019, 8009, 2000, 20);
    assert_eq!(
        OK,
        run_guarded_scatter(&mut vm, &mut state, &prog, T, &between)
    );
    assert_eq!(7002, bytes::read_u32(&state, row + VERDICT_AT));
    assert!(!tag_present(&state, 8009));
}

/// A kind-0 route pointing at a slot other than the guarded one would be a
/// card-one write gated by a guard no row of its own slot keeps. Refused by
/// name rather than left silently unguarded.
#[test]
fn guarded_scatter_kind0_route_outside_the_guarded_slot_is_invalid_program() {
    const T: u32 = 3105;
    let mut body = guarded_scatter_body();
    body[12] = 1; // route 0's dest_slot: the set slot, not the guarded map
    let refused = guarded_program_with_body(&body, T);
    let mut state = init(&refused);
    let mut vm = Vm::default();
    let tx = guarded_transaction(500, 7002, 7012, 8002, 2000, 20);
    assert_eq!(
        ErrorCode::InvalidProgram as u32,
        run_guarded_scatter(&mut vm, &mut state, &refused, T, &tx)
    );
}

/// A guard tuple the compare array cannot hold, and a guard tuple of zero
/// components, both refuse: an unguarded scatter is 0x3e, and a wider tuple
/// would index past the decode array (92 §4 — reject at sizing).
#[test]
fn guarded_scatter_guard_width_outside_the_decode_array_is_invalid_program() {
    const T: u32 = 3106;
    let tx = guarded_transaction(500, 7002, 7012, 8002, 2000, 20);

    for pairs in [0usize, 5] {
        let mut body = vec![0x3fu8, 2, 3, 4, 0, pairs as u8];
        for _ in 0..pairs {
            body.extend_from_slice(&[7, 2]);
        }
        body.push(1);
        body.extend_from_slice(&[0, 0, 0, 5]);
        let refused = guarded_program_with_body(&body, T);
        let mut state = init(&refused);
        let mut vm = Vm::default();
        assert_eq!(
            ErrorCode::InvalidProgram as u32,
            run_guarded_scatter(&mut vm, &mut state, &refused, T, &tx),
            "a guard tuple of {pairs} components must refuse"
        );
    }
}

fn guarded_program_with_body(body: &[u8], type_id: u32) -> Vec<u8> {
    let mut init_code = Vec::new();
    init_code.extend(slot_struct_map(0, 6, 8, &[4, 4, 1, 1]));
    init_code.extend(slot_def(1, 1, 8, 0));
    let mut reduce = vec![0xE0, 0, 1];
    reduce.extend(type_id.to_le_bytes());
    let flat_map = guarded_flat_map(body);
    reduce.extend((flat_map.len() as u16).to_le_bytes());
    reduce.extend(&flat_map);
    program(2, 9, &init_code, &reduce)
}

/// The probe scatter carries the same route-table admission bound (the
/// pre-existing class this const closes for both forms).
#[test]
fn probe_scatter_over_capacity_route_table_is_invalid_program() {
    // Rebuild the 0x2f parity program with 33 declared routes.
    let mut probe_init = Vec::new();
    probe_init.extend(slot_struct_map(0, 6, 4, &[0, 0, 0, 4, 2, 4]));
    probe_init.extend(slot_struct_map(1, 6, 4, &[4, 4, 2]));
    probe_init.extend(slot_def(2, 1, 4, 0));
    probe_init.extend(slot_def(3, 1, 4, 0));
    let mut scatter = vec![0x2fu8, 0, 2, 0, 0, 1, 33];
    let route = [0u8, 1, 0, 2, 3];
    for _ in 0..33 {
        scatter.extend_from_slice(&route);
    }
    let mut flat_map = vec![0xE1, 1];
    flat_map.extend(encode_index(NO_PARENT_TS_COL).as_bytes());
    flat_map.extend((scatter.len() as u16).to_le_bytes());
    flat_map.extend(scatter);
    let mut reduce = vec![0xE0, 0, 1];
    reduce.extend(2100u32.to_le_bytes());
    reduce.extend((flat_map.len() as u16).to_le_bytes());
    reduce.extend(&flat_map);
    let prog = program(4, 3, &probe_init, &reduce);
    let mut state = init(&prog);
    let mut vm = Vm::default();
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&[2100u32]),
        u32s_as_bytes(&[0u32, 1]),
        u32s_as_bytes(&[10u32]),
    ];
    assert_eq!(
        ErrorCode::InvalidProgram as u32,
        vm.execute_batch(&mut state, &prog, &cols, 1)
    );
}

/// Sentinel destination keys are never written (the same guard the keyed
/// struct-map upserts carry).
#[test]
fn struct_scatter_sentinel_key_is_skipped() {
    const T: u32 = 2112;
    let prog = build_struct_map_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let element = [ScatterElement {
        route: 0,
        op_retract: 0,
        key: EMPTY_KEY,
        v_str: 7001,
        v_num: 0.0,
        v_set: 0,
    }];
    assert_eq!(
        OK,
        run_struct_scatter(&mut vm, &mut state, &prog, T, &element)
    );
    assert_eq!(None, struct_map_row(&state, 0, EMPTY_KEY));
    assert_eq!(0, slot_size(&state, 0));
}

// =============================================================================
// STRUCT_MAP_UPSERT_LAST 0x80 rollback ()
// =============================================================================

fn exec_struct_upsert(
    vm: &mut Vm,
    state: &mut [u8],
    prog: &[u8],
    keys: &[u32],
    vals: &[u32],
    strs: &[u32],
) -> u32 {
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(keys),
        u32s_as_bytes(vals),
        u32s_as_bytes(strs),
    ];
    vm.execute_batch(state, prog, &cols, keys.len() as u32)
}

#[test]
fn struct_map_upsert_overwrite_then_rollback_restores_prior_field_value_and_bit() {
    let prog = build_struct_map_program(4, 0);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    assert_eq!(
        OK,
        exec_struct_upsert(&mut vm, &mut state, &prog, &[100], &[42], &[1001])
    );
    let row = present_row(&state, 0, 100);
    assert_eq!(0x03, state[row as usize]);
    assert_eq!(42, bytes::read_u32(&state, row + 1));
    assert_eq!(1001, bytes::read_u32(&state, row + 5));

    let before = state.clone();
    vm.undo_enable(&state).expect("undo window opens");
    assert!(vm.undo_status().enabled);
    let cp = vm.undo_checkpoint();
    assert_eq!(0, cp); // fresh enable resets the log
    assert_eq!(
        OK,
        exec_struct_upsert(&mut vm, &mut state, &prog, &[100], &[777], &[2002])
    );

    assert!(vm.undo_checkpoint() > cp); // whole-row journaling fired
    assert_eq!(777, bytes::read_u32(&state, row + 1));
    assert_eq!(2002, bytes::read_u32(&state, row + 5));
    assert!(!vm.undo_status().overflow); // the window accepted every entry

    vm.undo_rollback(&mut state, cp).expect("valid checkpoint");
    assert_eq!(0x03, state[row as usize]);
    assert_eq!(42, bytes::read_u32(&state, row + 1));
    assert_eq!(1001, bytes::read_u32(&state, row + 5));
    assert_eq!(1, slot_size(&state, 0));
    assert_eq!(before, state);
    vm.undo_commit();
}

#[test]
fn struct_map_upsert_create_then_rollback_removes_key_and_restores_size() {
    let prog = build_struct_map_program(4, 0);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    assert_eq!(
        OK,
        exec_struct_upsert(&mut vm, &mut state, &prog, &[100], &[42], &[1001])
    );
    assert_eq!(1, slot_size(&state, 0));
    assert_eq!(None, struct_map_row(&state, 0, 555));

    let before = state.clone();
    vm.undo_enable(&state).expect("undo window opens");
    let cp = vm.undo_checkpoint();
    assert_eq!(
        OK,
        exec_struct_upsert(&mut vm, &mut state, &prog, &[555], &[88], &[3003])
    );
    assert!(struct_map_row(&state, 0, 555).is_some());
    assert_eq!(2, slot_size(&state, 0));
    assert!(!vm.undo_status().overflow);

    vm.undo_rollback(&mut state, cp).expect("valid checkpoint");
    assert_eq!(None, struct_map_row(&state, 0, 555));
    assert_eq!(1, slot_size(&state, 0));
    let row100 = present_row(&state, 0, 100);
    assert_eq!(42, bytes::read_u32(&state, row100 + 1));
    assert_eq!(before, state);
    vm.undo_commit();
}

#[test]
fn struct_map_upsert_multi_row_speculative_batch_fully_rolled_back() {
    let prog = build_struct_map_program(4, 0);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    assert_eq!(
        OK,
        exec_struct_upsert(
            &mut vm,
            &mut state,
            &prog,
            &[100, 200],
            &[42, 43],
            &[1001, 1002]
        )
    );
    let r100 = present_row(&state, 0, 100);
    let r200 = present_row(&state, 0, 200);

    let before = state.clone();
    vm.undo_enable(&state).expect("undo window opens");
    let cp = vm.undo_checkpoint();
    assert_eq!(
        OK,
        exec_struct_upsert(
            &mut vm,
            &mut state,
            &prog,
            &[100, 200, 300],
            &[7000, 8000, 9000],
            &[7, 8, 9]
        )
    );
    assert_eq!(3, slot_size(&state, 0));
    assert_eq!(7000, bytes::read_u32(&state, r100 + 1));
    assert!(!vm.undo_status().overflow);

    vm.undo_rollback(&mut state, cp).expect("valid checkpoint");
    assert_eq!(2, slot_size(&state, 0));
    assert_eq!(0x03, state[r100 as usize]);
    assert_eq!(42, bytes::read_u32(&state, r100 + 1));
    assert_eq!(1001, bytes::read_u32(&state, r100 + 5));
    assert_eq!(43, bytes::read_u32(&state, r200 + 1));
    assert_eq!(1002, bytes::read_u32(&state, r200 + 5));
    assert_eq!(None, struct_map_row(&state, 0, 300));
    assert_eq!(before, state);
    vm.undo_commit();
}

// =============================================================================
// Struct map through top-level dispatch ()
// =============================================================================

#[test]
fn struct_map_init_upsert_and_read_back() {
    let prog = build_struct_map_program(4, 0);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    assert_eq!(
        OK,
        exec_struct_upsert(&mut vm, &mut state, &prog, &[100], &[42], &[1001])
    );
    assert_eq!(1, slot_size(&state, 0));
    let row = present_row(&state, 0, 100);
    assert_eq!(0x03, state[row as usize]); // both field bits
    assert_eq!(42, bytes::read_u32(&state, row + 1));
    assert_eq!(1001, bytes::read_u32(&state, row + 5));
}

#[test]
fn struct_map_upsert_overwrites_existing_key() {
    let prog = build_struct_map_program(4, 0);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    assert_eq!(
        OK,
        exec_struct_upsert(&mut vm, &mut state, &prog, &[100], &[42], &[1001])
    );
    assert_eq!(
        OK,
        exec_struct_upsert(&mut vm, &mut state, &prog, &[100], &[777], &[2002])
    );
    assert_eq!(1, slot_size(&state, 0));
    let row = present_row(&state, 0, 100);
    assert_eq!(777, bytes::read_u32(&state, row + 1));
    assert_eq!(2002, bytes::read_u32(&state, row + 5));
}

// =============================================================================
// Delta export/apply round-trip (proptest)
// =============================================================================

use proptest::prelude::*;

proptest! {
    /// Rollback through the REAL dispatch path restores the full image, byte for
    /// byte, for arbitrary speculative batches over a committed baseline.
    #[test]
    fn dispatch_rollback_restores_baseline(
        baseline in proptest::collection::vec((0u32..500, 1u32..1000), 1..8),
        speculative in proptest::collection::vec((0u32..500, 1u32..1000), 1..8),
    ) {
        let prog = build_test_program(16, 0); // cap 32, max_load 22
        let mut state = init(&prog);
        let mut vm = Vm::default();

        let keys: Vec<u32> = baseline.iter().map(|(k, _)| *k + 1).collect();
        let vals: Vec<u32> = baseline.iter().map(|(_, v)| *v).collect();
        let cols: Vec<&[u8]> = vec![u32s_as_bytes(&keys), u32s_as_bytes(&vals)];
        prop_assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, keys.len() as u32));
        let base_snapshot: Vec<(u32, Option<u32>)> = {
            let map = CheckedMap::bind(&state, 0).expect("valid map slot");
            keys.iter().map(|&k| (k, map.get(k))).collect()
        };
        let base_size = slot_size(&state, 0);
        let base_count = agg_count(&state, 1);

        let base_image = state.clone();
        vm.undo_enable(&state).expect("undo window opens");
        let cp = vm.undo_checkpoint();
        let skeys: Vec<u32> = speculative.iter().map(|(k, _)| *k + 1).collect();
        let svals: Vec<u32> = speculative.iter().map(|(_, v)| *v + 5000).collect();
        let scols: Vec<&[u8]> = vec![u32s_as_bytes(&skeys), u32s_as_bytes(&svals)];
        prop_assert_eq!(OK, vm.execute_batch(&mut state, &prog, &scols, skeys.len() as u32));

        vm.undo_rollback(&mut state, cp).expect("valid checkpoint");
        vm.undo_commit();
        prop_assert_eq!(&base_image, &state, "rollback restores the baseline image byte for byte");

        prop_assert_eq!(base_size, slot_size(&state, 0));
        prop_assert_eq!(base_count, agg_count(&state, 1));
        let map = CheckedMap::bind(&state, 0).expect("valid map slot");
        for (k, v) in base_snapshot {
            prop_assert_eq!(v, map.get(k));
        }
    }

    /// Delta export/apply: exporting a delta-mode batch's segments and
    /// applying rollback+rollforward to a COPY of the pre-batch state lands
    /// on the post-batch image, byte for byte: the Scenario fork-navigation
    /// contract over the physical journal.
    #[test]
    fn delta_segments_roundtrip_fork_navigation(
        batch in proptest::collection::vec((0u32..200, 1u32..1000), 1..10),
        remove_picks in proptest::collection::vec(proptest::bool::ANY, 1..10),
    ) {
        let prog = build_test_program(16, 0);
        let mut state = init(&prog);
        let mut vm = Vm::default();

        vm.undo_enable(&state).expect("undo window opens");
        let keys: Vec<u32> = batch.iter().map(|(k, _)| *k + 1).collect();
        let vals: Vec<u32> = batch.iter().map(|(_, v)| *v).collect();
        let pre_state = state.clone();
        let cols: Vec<&[u8]> = vec![u32s_as_bytes(&keys), u32s_as_bytes(&vals)];
        prop_assert_eq!(OK, vm.execute_batch_delta(&mut state, &prog, &cols, keys.len() as u32));

        // A second delta batch REMOVES a subset of the keys, so the excovered
        // segment carries MapDelete pairs too (rollforward must re-delete,
        // rollback must re-insert with the pre-remove value).
        let rkeys: Vec<u32> = keys
            .iter()
            .zip(remove_picks.iter().cycle())
            .filter(|(_, pick)| **pick)
            .map(|(k, _)| *k)
            .collect();
        if !rkeys.is_empty() {
            // Same init section (identical slot layout), remove-only reduce.
            let mut init_code = Vec::new();
            init_code.extend(slot_def(0, 0x00, 16, 0));
            init_code.extend(slot_def(1, 0x02, 2, 0));
            let remove_prog = program(2, 1, &init_code, &[0x23, 0, 0]);
            let rcols: Vec<&[u8]> = vec![u32s_as_bytes(&rkeys)];
            prop_assert_eq!(OK, vm.execute_batch_delta(&mut state, &remove_prog, &rcols, rkeys.len() as u32));
        }

        let end = vm.undo_checkpoint();
        prop_assert_eq!(vm.delta_export_segment(0, end), Ok(end));
        prop_assert!(end > 0);
        let undo_seg = vm.delta_export_undo().to_vec();
        let redo_seg = vm.delta_export_redo().to_vec();
        let entry_size = Vm::delta_export_entry_size();

        // Roll the LIVE state back via the exported undo segment → the pre-batch image.
        let mut rolled = state.clone();
        vm.delta_apply_rollback_segment(&mut rolled, &undo_seg, entry_size).expect("valid undo segment");
        prop_assert_eq!(&pre_state, &rolled, "undo segment restores the pre-batch image byte for byte");
        let pre_map = CheckedMap::bind(&pre_state, 0).expect("valid map slot");
        let rolled_map = CheckedMap::bind(&rolled, 0).expect("valid map slot");
        for &k in &keys {
            prop_assert_eq!(pre_map.get(k), rolled_map.get(k));
        }
        prop_assert_eq!(slot_size(&pre_state, 0), slot_size(&rolled, 0));

        // Roll it forward again via the redo segment → post-batch content.
        vm.delta_apply_rollforward_segment(&mut rolled, &redo_seg, entry_size).expect("valid redo segment");
        prop_assert_eq!(&state, &rolled, "redo segment reproduces the post-batch image byte for byte");
        let state_map = CheckedMap::bind(&state, 0).expect("valid map slot");
        let rolled_map = CheckedMap::bind(&rolled, 0).expect("valid map slot");
        for &k in &keys {
            prop_assert_eq!(state_map.get(k), rolled_map.get(k));
        }
        prop_assert_eq!(slot_size(&state, 0), slot_size(&rolled, 0));
        vm.undo_commit();
    }

    /// Batch-vs-sequential equivalence: one N-row batch produces the same
    /// logical map content as N single-row batches (COUNT differs only by
    /// batch granularity for MAP_UPSERT_LAST + AGG_COUNT).
    #[test]
    fn batch_vs_sequential_equivalence(
        rows in proptest::collection::vec((0u32..300, 1u32..1000), 1..12),
    ) {
        let prog = build_test_program(16, 0);
        let keys: Vec<u32> = rows.iter().map(|(k, _)| *k + 1).collect();
        let vals: Vec<u32> = rows.iter().map(|(_, v)| *v).collect();

        let mut state_a = init(&prog);
        let mut vm_a = Vm::default();
        let cols: Vec<&[u8]> = vec![u32s_as_bytes(&keys), u32s_as_bytes(&vals)];
        prop_assert_eq!(OK, vm_a.execute_batch(&mut state_a, &prog, &cols, keys.len() as u32));

        let mut state_b = init(&prog);
        let mut vm_b = Vm::default();
        for i in 0..keys.len() {
            let k = [keys[i]];
            let v = [vals[i]];
            let c: Vec<&[u8]> = vec![u32s_as_bytes(&k), u32s_as_bytes(&v)];
            prop_assert_eq!(OK, vm_b.execute_batch(&mut state_b, &prog, &c, 1));
        }

        prop_assert_eq!(slot_size(&state_a, 0), slot_size(&state_b, 0));
        prop_assert_eq!(agg_count(&state_a, 1), agg_count(&state_b, 1));
        let map_a = CheckedMap::bind(&state_a, 0).expect("valid map slot");
        let map_b = CheckedMap::bind(&state_b, 0).expect("valid map slot");
        for &k in &keys {
            prop_assert_eq!(map_a.get(k), map_b.get(k));
        }
    }
}

// =============================================================================
// Journal overflow and full-image windows through the real dispatch path. A
// write past the record budget runs into the shadow; rollback stays exact.
// =============================================================================

/// One delta window: production rollback and the exported undo and redo
/// segments must each reproduce the full image byte for byte.
fn assert_full_image_roundtrip(vm: &mut Vm, before: &[u8], after: &[u8], checkpoint: u32) {
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
    let entry = Vm::delta_export_entry_size();

    let mut production = after.to_vec();
    vm.undo_rollback(&mut production, checkpoint)
        .expect("valid checkpoint");
    assert_eq!(
        before,
        production.as_slice(),
        "production rollback restores the full image"
    );

    let mut replay = Vm::default();
    let mut rolled_back = after.to_vec();
    replay
        .delta_apply_rollback_segment(&mut rolled_back, &undo, entry)
        .expect("valid undo segment");
    assert_eq!(
        before,
        rolled_back.as_slice(),
        "undo segment restores the full image"
    );

    let mut rolled_forward = before.to_vec();
    replay
        .delta_apply_rollforward_segment(&mut rolled_forward, &redo, entry)
        .expect("valid redo segment");
    assert_eq!(
        after,
        rolled_forward.as_slice(),
        "redo segment reproduces the full image"
    );
}

/// Real alternating writes fill the journal through `execute_batch_delta`. The
/// first write the budget cannot cover overflows the window into a shadow: it
/// still returns Ok and lands, and every later write runs unrecorded. The
/// pre-overflow prefix still rolls back and redoes to exact images from its
/// start and from its midpoint, and the overflowed window rolls back to a
/// pre-overflow savepoint byte for byte.
#[test]
fn journal_overflow_shadows_and_prefix_stays_exact() {
    let mut init_code = Vec::new();
    init_code.extend(slot_def(0, 0x00, 16, 0));
    let reduce = [0x22, 0, 0, 1]; // BATCH_MAP_UPSERT_LAST slot=0 key=c0 val=c1
    let prog = program(1, 2, &init_code, &reduce);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    // One committed row; every window write below changes only its value cell.
    let seed_keys = [7u32];
    let seed_vals = [1u32];
    let seed_cols: Vec<&[u8]> = vec![u32s_as_bytes(&seed_keys), u32s_as_bytes(&seed_vals)];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &seed_cols, 1));
    let pristine = state.clone();

    vm.undo_enable(&state).expect("undo window opens");
    let window_start = vm.undo_checkpoint();
    assert_eq!(0, window_start);

    let mut midpoint: Option<(u32, Vec<u8>)> = None;
    let mut prefix: Option<(u32, Vec<u8>)> = None;
    let mut writes = 0u32;
    while !vm.undo_has_overflow() {
        assert!(
            writes < 2 * UNDO_CAPACITY,
            "journal never overflowed on a real write"
        );
        let checkpoint = vm.undo_checkpoint();
        let image = state.clone();
        if midpoint.is_none() && checkpoint >= UNDO_CAPACITY / 2 {
            midpoint = Some((checkpoint, image.clone()));
        }
        let keys = [7u32];
        let vals = [if writes.is_multiple_of(2) {
            0xAAAA_0001u32
        } else {
            0xBBBB_0002
        }];
        let cols: Vec<&[u8]> = vec![u32s_as_bytes(&keys), u32s_as_bytes(&vals)];
        assert_eq!(OK, vm.execute_batch_delta(&mut state, &prog, &cols, 1));
        if vm.undo_has_overflow() {
            prefix = Some((checkpoint, image));
        }
        writes += 1;
    }
    let (prefix_checkpoint, image_at_prefix) = prefix.expect("the overflowing write was recorded");
    let (midpoint_checkpoint, midpoint_image) = midpoint.expect("the window passed its midpoint");
    assert!(
        prefix_checkpoint + 4 >= UNDO_CAPACITY,
        "overflow must come at capacity, not earlier (checkpoint {prefix_checkpoint})"
    );
    assert!(midpoint_checkpoint < prefix_checkpoint);
    assert!(vm.undo_status().overflow);
    assert_eq!(UNDO_POSITION_OVERFLOWED, vm.undo_checkpoint());
    // The overflowing write itself landed in the image.
    let overflowing_write = state.clone();
    assert_ne!(
        image_at_prefix, overflowing_write,
        "the overflowing write landed"
    );

    // Sticky overflow: a further write still lands and stays unrecorded.
    let keys = [7u32];
    let vals = [0xCCCC_0003u32];
    let cols: Vec<&[u8]> = vec![u32s_as_bytes(&keys), u32s_as_bytes(&vals)];
    assert_eq!(OK, vm.execute_batch_delta(&mut state, &prog, &cols, 1));
    assert_eq!(UNDO_POSITION_OVERFLOWED, vm.undo_checkpoint());
    let overflowed = state.clone();

    // Export the pre-overflow prefix before any rollback truncates the journal.
    let entry = Vm::delta_export_entry_size();
    assert_eq!(
        Ok(prefix_checkpoint),
        vm.delta_export_segment(window_start, prefix_checkpoint)
    );
    let window_undo = vm.delta_export_undo().to_vec();
    let window_redo = vm.delta_export_redo().to_vec();
    assert_eq!(
        Ok(prefix_checkpoint - midpoint_checkpoint),
        vm.delta_export_segment(midpoint_checkpoint, prefix_checkpoint)
    );
    let tail_undo = vm.delta_export_undo().to_vec();
    let tail_redo = vm.delta_export_redo().to_vec();

    let mut replay = Vm::default();
    let mut at_midpoint = image_at_prefix.clone();
    replay
        .delta_apply_rollback_segment(&mut at_midpoint, &tail_undo, entry)
        .expect("valid tail undo");
    assert_eq!(
        midpoint_image, at_midpoint,
        "the tail undo reaches the midpoint image"
    );
    replay
        .delta_apply_rollforward_segment(&mut at_midpoint, &tail_redo, entry)
        .expect("valid tail redo");
    assert_eq!(
        image_at_prefix, at_midpoint,
        "the tail redo reaches the prefix image"
    );

    let mut at_start = image_at_prefix.clone();
    replay
        .delta_apply_rollback_segment(&mut at_start, &window_undo, entry)
        .expect("valid window undo");
    assert_eq!(
        pristine, at_start,
        "the accepted window undoes to the pristine image"
    );
    replay
        .delta_apply_rollforward_segment(&mut at_start, &window_redo, entry)
        .expect("valid window redo");
    assert_eq!(
        image_at_prefix, at_start,
        "the accepted window redoes exactly"
    );

    // The overflowed window rejects the overflow savepoint and changes nothing.
    assert_eq!(
        Err(ErrorCode::InvalidState),
        vm.undo_rollback(&mut state, UNDO_POSITION_OVERFLOWED)
    );
    assert_eq!(
        overflowed, state,
        "an overflowed savepoint rollback changes nothing"
    );

    // The live journal rolls back to the midpoint, then to the start, exactly.
    vm.undo_rollback(&mut state, midpoint_checkpoint)
        .expect("valid checkpoint");
    assert_eq!(midpoint_image, state);
    assert!(!vm.undo_has_overflow());
    assert_eq!(midpoint_checkpoint, vm.undo_checkpoint());
    vm.undo_rollback(&mut state, window_start)
        .expect("valid checkpoint");
    assert_eq!(pristine, state);
    assert!(!vm.undo_has_overflow());
    assert_eq!(window_start, vm.undo_checkpoint());
    vm.undo_commit();
}

/// Journal exhaustion inside one multi-row batch. Real alternating single-row
/// writes first bring the journal to within a few entries of capacity. Then one
/// batch of distinct rows runs past it: the batch returns Ok and every row
/// lands, with the window overflowed into a shadow mid-batch. The savepoint
/// before the batch and the window start each roll back to exact full images.
#[test]
fn journal_exhaustion_mid_batch_lands_whole_batch_and_rolls_back_exact() {
    const FILL_MARGIN: u32 = 16;
    const ROWS: u32 = 24;
    let mut init_code = Vec::new();
    init_code.extend(slot_def(0, 0x00, 16, 0));
    let reduce = [0x22, 0, 0, 1]; // BATCH_MAP_UPSERT_LAST slot=0 key=c0 val=c1
    let prog = program(1, 2, &init_code, &reduce);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let seed_keys = [7u32];
    let seed_vals = [1u32];
    let seed_cols: Vec<&[u8]> = vec![u32s_as_bytes(&seed_keys), u32s_as_bytes(&seed_vals)];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &seed_cols, 1));
    let pristine = state.clone();

    vm.undo_enable(&state).expect("undo window opens");
    let window_start = vm.undo_checkpoint();

    // Real alternating writes until the journal has only a little room left.
    let mut writes = 0u32;
    while vm.undo_checkpoint() + FILL_MARGIN < UNDO_CAPACITY {
        assert!(
            writes < 2 * UNDO_CAPACITY,
            "the fill never reached the margin"
        );
        let keys = [7u32];
        let vals = [if writes.is_multiple_of(2) {
            0xAAAA_0001u32
        } else {
            0xBBBB_0002
        }];
        let cols: Vec<&[u8]> = vec![u32s_as_bytes(&keys), u32s_as_bytes(&vals)];
        assert_eq!(OK, vm.execute_batch_delta(&mut state, &prog, &cols, 1));
        writes += 1;
    }
    let before_cp = vm.undo_checkpoint();
    let image_before = state.clone();

    // One batch of distinct values: each row changes the value cell, so every
    // row prefix has its own image.
    let keys = vec![7u32; ROWS as usize];
    let values: Vec<u32> = (0..ROWS).map(|row| 0x1000_0000 + row).collect();
    let cols: Vec<&[u8]> = vec![u32s_as_bytes(&keys), u32s_as_bytes(&values)];
    assert_eq!(OK, vm.execute_batch_delta(&mut state, &prog, &cols, ROWS));
    assert!(
        vm.undo_status().overflow,
        "the batch overflowed the journal mid-batch"
    );
    assert_eq!(UNDO_POSITION_OVERFLOWED, vm.undo_checkpoint());

    // Every row landed: the image equals the same batch applied to a fresh VM
    // from the pre-batch image.
    let mut expected = image_before.clone();
    let mut reference_vm = Vm::default();
    assert_eq!(
        OK,
        reference_vm.execute_batch(&mut expected, &prog, &cols, ROWS)
    );
    assert_eq!(expected, state, "every row of the overflowing batch lands");

    // Overflow is sticky: the next write also lands and stays unrecorded.
    let next_keys = [7u32];
    let next_vals = [0x2000_0000u32];
    let next_cols: Vec<&[u8]> = vec![u32s_as_bytes(&next_keys), u32s_as_bytes(&next_vals)];
    assert_eq!(OK, vm.execute_batch_delta(&mut state, &prog, &next_cols, 1));
    assert_eq!(UNDO_POSITION_OVERFLOWED, vm.undo_checkpoint());
    let overflowed = state.clone();

    // An export naming the overflowed position is refused; the recorded
    // prefix succeeds.
    assert_eq!(
        Err(ErrorCode::InvalidState),
        vm.delta_export_segment(before_cp, UNDO_POSITION_OVERFLOWED)
    );
    assert_eq!(
        Ok(before_cp - window_start),
        vm.delta_export_segment(window_start, before_cp)
    );
    assert_eq!(
        Err(ErrorCode::InvalidState),
        vm.undo_rollback(&mut state, UNDO_POSITION_OVERFLOWED)
    );
    assert_eq!(
        overflowed, state,
        "an overflowed savepoint rollback changes nothing"
    );

    // Live rollbacks: the savepoint before the batch, then the window start.
    vm.undo_rollback(&mut state, before_cp)
        .expect("valid checkpoint");
    assert_eq!(
        image_before, state,
        "the savepoint before the batch restores its image"
    );
    assert!(!vm.undo_has_overflow());
    assert_eq!(before_cp, vm.undo_checkpoint());
    vm.undo_rollback(&mut state, window_start)
        .expect("valid checkpoint");
    assert_eq!(
        pristine, state,
        "the window start restores the pristine image"
    );
    assert!(!vm.undo_has_overflow());
    vm.undo_commit();
}

/// A COUNT that cannot add its matched rows is refused before its cell, its
/// change flag, or any journal entry is written. It is a semantic refusal, so
/// the journal diagnostic stays clear.
#[test]
fn count_overflow_refuses_prewrite_without_journal_or_flag() {
    let prog = program(1, 1, &slot_def(0, 0x02, 2, 0), &[0x41, 0]);
    let mut state = init(&prog);
    let mut vm = Vm::default();
    let count_off = slot_offset(&state, 0);
    bytes::write_u64(&mut state, count_off, u64::MAX);
    let before = state.clone();
    let rows = [0u32];
    let cols: Vec<&[u8]> = vec![u32s_as_bytes(&rows)];

    vm.undo_enable(&state).expect("undo window opens");
    let cp = vm.undo_checkpoint();
    assert_eq!(
        ErrorCode::CountOverflow as u32,
        vm.execute_batch_delta(&mut state, &prog, &cols, 1)
    );
    assert_eq!(before, state);
    assert_eq!(cp, vm.undo_checkpoint());
    assert_eq!(0, slot_change_flags(&state, 0));
    assert!(
        !vm.undo_status().overflow,
        "a refused count is not a journal refusal"
    );
    vm.undo_commit();
}

/// A COUNT crossing the 32-bit word boundary journals its whole 8-byte cell and
/// its SIZE_CHANGED flag; rollback restores both exactly.
#[test]
fn count_aggregate_high_word_rollback_restores_cell_and_size_flag() {
    let prog = program(1, 1, &slot_def(0, 0x02, 2, 0), &[0x41, 0]);
    let mut state = init(&prog);
    let mut vm = Vm::default();
    let count_off = slot_offset(&state, 0);
    bytes::write_u64(&mut state, count_off, u64::from(u32::MAX));
    let before = state.clone();
    let rows = [0u32];
    let cols: Vec<&[u8]> = vec![u32s_as_bytes(&rows)];

    vm.undo_enable(&state).expect("undo window opens");
    let cp = vm.undo_checkpoint();
    assert_eq!(OK, vm.execute_batch_delta(&mut state, &prog, &cols, 1));
    assert_eq!(0x1_0000_0000, agg_count(&state, 0));
    assert_eq!(ChangeFlag::SIZE_CHANGED, slot_change_flags(&state, 0));
    let after = state.clone();
    assert_full_image_roundtrip(&mut vm, &before, &after, cp);
}

/// A U32 scalar latest writes four value bytes and its timestamp cell. The upper
/// word of the 8-byte value lane is stale and survives the write, the noop, and
/// the rollback exactly.
#[test]
fn scalar_u32_latest_rollback_preserves_upper_word_and_timestamp() {
    const T: u32 = 3201;
    let init_code = slot_def(0, SlotType::Scalar as u8, AggType::ScalarU32 as u8, 0);
    let body = [Opcode::BatchScalarLatest as u8, 0, 1, 2];
    let mut reduce = vec![0xE0, 0, 1];
    reduce.extend(T.to_le_bytes());
    reduce.extend((body.len() as u16).to_le_bytes());
    reduce.extend_from_slice(&body);
    let prog = program(1, 3, &init_code, &reduce);
    let mut state = init(&prog);
    let mut vm = Vm::default();
    let data = slot_offset(&state, 0);
    bytes::write_u32(&mut state, data + 4, 0xA5A5_5A5A);
    let before = state.clone();

    vm.undo_enable(&state).expect("undo window opens");
    let cp = vm.undo_checkpoint();
    let types = [T];
    let values = [42u32];
    let stamps = [100.0f64];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&values),
        f64s_as_bytes(&stamps),
    ];
    assert_eq!(OK, vm.execute_batch_delta(&mut state, &prog, &cols, 1));
    assert_eq!(42, bytes::read_u32(&state, data));
    assert_eq!(
        0xA5A5_5A5A,
        bytes::read_u32(&state, data + 4),
        "a U32 write leaves the upper word alone"
    );
    assert_eq!(100.0, bytes::read_f64(&state, data + 8));
    let after = state.clone();

    // An older timestamp selects nothing: same image, no journal entry.
    let late_values = [7u32];
    let late_stamps = [50.0f64];
    let late_cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&late_values),
        f64s_as_bytes(&late_stamps),
    ];
    let end = vm.undo_checkpoint();
    assert_eq!(OK, vm.execute_batch_delta(&mut state, &prog, &late_cols, 1));
    assert_eq!(end, vm.undo_checkpoint());
    assert_eq!(after, state);

    assert_full_image_roundtrip(&mut vm, &before, &after, cp);
}

/// A U32 scalar latest stores 0xffffffff from a matching row like any other
/// value; a non-matching row with a newer timestamp selects nothing.
#[test]
fn scalar_u32_latest_stores_all_ones_on_a_matching_row_only() {
    const T: u32 = 3202;
    let init_code = slot_def(0, SlotType::Scalar as u8, AggType::ScalarU32 as u8, 0);
    let body = [Opcode::BatchScalarLatest as u8, 0, 1, 2];
    let mut reduce = vec![0xE0, 0, 1];
    reduce.extend(T.to_le_bytes());
    reduce.extend((body.len() as u16).to_le_bytes());
    reduce.extend_from_slice(&body);
    let prog = program(1, 3, &init_code, &reduce);
    let mut state = init(&prog);
    let mut vm = Vm::default();
    let data = slot_offset(&state, 0);

    let types = [T, T, T + 1];
    let values = [7u32, u32::MAX, 9];
    let stamps = [10.0f64, 20.0, 30.0];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&values),
        f64s_as_bytes(&stamps),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 3));
    assert_eq!(u32::MAX, bytes::read_u32(&state, data));
    assert_eq!(20.0, bytes::read_f64(&state, data + 8));
}

/// A row whose field bit is clear but whose payload stays allocated and nonzero
/// is physical state. The upsert that re-sets the field and its rollback leave
/// every byte of the image exact, the stale payload included.
#[test]
fn struct_map_upsert_over_stale_unset_payload_rolls_back_to_full_image() {
    let prog = build_struct_map_program(4, 0);
    let mut state = init(&prog);
    let mut vm = Vm::default();
    assert_eq!(
        OK,
        exec_struct_upsert(
            &mut vm,
            &mut state,
            &prog,
            &[100, 200],
            &[42, 43],
            &[1001, 1002]
        )
    );
    let smap = StructMapSlot::bind(&state, 0);
    let row = present_row(&state, 0, 100);
    let stale_payload = row + smap.field_offset(&state, 1);
    state[row as usize] &= !0x02;
    bytes::write_u32(&mut state, stale_payload, 0xDEAD_BEEF);
    let before = state.clone();

    vm.undo_enable(&state).expect("undo window opens");
    let cp = vm.undo_checkpoint();
    let next_keys = [100u32];
    let next_vals = [777u32];
    let next_strs = [2002u32];
    let next_cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&next_keys),
        u32s_as_bytes(&next_vals),
        u32s_as_bytes(&next_strs),
    ];
    assert_eq!(OK, vm.execute_batch_delta(&mut state, &prog, &next_cols, 1));
    assert_eq!(0x03, state[row as usize]);
    assert_eq!(777, bytes::read_u32(&state, row + 1));
    assert_eq!(2002, bytes::read_u32(&state, stale_payload));
    let after = state.clone();
    assert_full_image_roundtrip(&mut vm, &before, &after, cp);
}

/// Composite-key (StructMap2) rows share the bitset-and-payload layout: a stale
/// unset payload under a composite key survives its upsert and rollback exactly.
#[test]
fn struct_map2_upsert_over_stale_unset_payload_rolls_back_to_full_image() {
    let init_code = [
        Opcode::SlotStructMap2 as u8,
        0,
        SlotType::StructMap2 as u8,
        8,
        0,
        2,
        0,
        4,
    ];
    // BATCH_STRUCT_MAP2_UPSERT_LAST slot=0 key1=c0 key2=c1, (c2 -> field 0), (c3 -> field 1).
    let reduce = [
        Opcode::BatchStructMap2UpsertLast as u8,
        0,
        0,
        1,
        2,
        2,
        0,
        3,
        1,
    ];
    let prog = program(1, 4, &init_code, &reduce);
    let mut state = init(&prog);
    let mut vm = Vm::default();
    let key1 = [1u32];
    let key2 = [9u32];
    let first_a = [42u32];
    let first_b = [1001u32];
    let first_cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&key1),
        u32s_as_bytes(&key2),
        u32s_as_bytes(&first_a),
        u32s_as_bytes(&first_b),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &first_cols, 1));

    let smap = StructMap2Slot::bind(&state, 0);
    let row = smap.get_row_ptr_by_key(&state, 1, 9);
    assert_ne!(0xFFFF_FFFF, row);
    let stale_payload = row + smap.field_offset(&state, 1);
    state[row as usize] &= !0x02;
    bytes::write_u32(&mut state, stale_payload, 0xDEAD_BEEF);
    let before = state.clone();

    vm.undo_enable(&state).expect("undo window opens");
    let cp = vm.undo_checkpoint();
    let next_a = [7u32];
    let next_b = [2002u32];
    let next_cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&key1),
        u32s_as_bytes(&key2),
        u32s_as_bytes(&next_a),
        u32s_as_bytes(&next_b),
    ];
    assert_eq!(OK, vm.execute_batch_delta(&mut state, &prog, &next_cols, 1));
    assert_ne!(before, state);
    let after = state.clone();
    assert_full_image_roundtrip(&mut vm, &before, &after, cp);
}

/// The probe scatter's assert over a node row whose title bit is clear, while the
/// title payload stays allocated and nonzero: the write and its rollback leave
/// every image byte exact.
#[test]
fn probe_scatter_assert_over_stale_unset_title_rolls_back_to_full_image() {
    const T: u32 = 2011;
    let prog = build_probe_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    seed_probe_row(&mut state, 10, 0, 0, 600, 7001, 0.0, 0);
    let nodes = StructMapSlot::bind(&state, 1);
    let node = nodes.upsert(&mut state, 600).expect("node row");
    let row = nodes.row_off(node.pos);
    nodes.clear_bitset(&mut state, row);
    let stale_title = row + nodes.field_offset(&state, 0);
    bytes::write_u32(&mut state, stale_title, 0xDEAD_BEEF);
    let before = state.clone();

    vm.undo_enable(&state).expect("undo window opens");
    let cp = vm.undo_checkpoint();
    let types = [T];
    let offsets = [0u32, 1];
    let keys = [10u32];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&offsets),
        u32s_as_bytes(&keys),
    ];
    assert_eq!(OK, vm.execute_batch_delta(&mut state, &prog, &cols, 1));
    assert!(bit_set(&state, row, 0));
    assert_eq!(7001, bytes::read_u32(&state, stale_title));
    let after = state.clone();
    assert_full_image_roundtrip(&mut vm, &before, &after, cp);
}

/// Nested savepoints over one window: each checkpoint restores the exact image
/// it saw, innermost first, and the middle window replays from its own export.
#[test]
fn nested_savepoints_roll_back_innermost_first_to_exact_images() {
    let prog = build_test_program(16, 0);
    let mut state = init(&prog);
    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("undo window opens");

    let mut checkpoints = vec![vm.undo_checkpoint()];
    let mut images = vec![state.clone()];
    let rounds: [([u32; 2], [u32; 2]); 3] =
        [([1, 2], [11, 12]), ([3, 2], [23, 22]), ([4, 5], [34, 35])];
    for (keys, vals) in rounds {
        let cols: Vec<&[u8]> = vec![u32s_as_bytes(&keys), u32s_as_bytes(&vals)];
        assert_eq!(OK, vm.execute_batch_delta(&mut state, &prog, &cols, 2));
        checkpoints.push(vm.undo_checkpoint());
        images.push(state.clone());
    }

    // The middle window [checkpoints[1], checkpoints[2]) replays exactly from its export.
    let (from, to) = (checkpoints[1], checkpoints[2]);
    assert_eq!(Ok(to - from), vm.delta_export_segment(from, to));
    let undo = vm.delta_export_undo().to_vec();
    let redo = vm.delta_export_redo().to_vec();
    let entry = Vm::delta_export_entry_size();
    let mut replay = Vm::default();
    let mut middle = images[2].clone();
    replay
        .delta_apply_rollback_segment(&mut middle, &undo, entry)
        .expect("valid middle undo");
    assert_eq!(
        images[1], middle,
        "middle undo reaches its opening savepoint"
    );
    replay
        .delta_apply_rollforward_segment(&mut middle, &redo, entry)
        .expect("valid middle redo");
    assert_eq!(
        images[2], middle,
        "middle redo reaches its closing savepoint"
    );

    // Live rollbacks, innermost first, each landing on the image its savepoint saw.
    assert_eq!(images[3], state);
    for level in (0..3).rev() {
        vm.undo_rollback(&mut state, checkpoints[level])
            .expect("valid checkpoint");
        assert_eq!(
            images[level], state,
            "savepoint {level} restores its exact image"
        );
    }
    vm.undo_commit();
}

/// Empty batches may provide no column pointers even when the program references
/// columns: no column is dereferenced when `batch_len == 0`, and `col_at`
/// resolves out-of-range to the empty column. Non-empty missing columns remain
/// a malformed FFI input. The bytes below are the exact TypeScript-compiled
/// `count()` reducer capture.
#[test]
fn empty_batch_with_no_column_pointers_is_ok() {
    let hex = "0000000000000000000000000000000000000000000000000000000000000000434c4d31020000000100000000000000060000000c000000100002020000e00101010000000200410000";
    let prog: Vec<u8> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect();
    let mut vm = Vm::default();
    let size = calculate_state_size(
        &prog,
        columine_vm::state_init::DEFAULT_ACCEPTED_PROGRAM_MAGICS,
    )
    .expect("valid program admission");
    assert!(size > 0, "state size must be > 0");
    let mut state = vec![0u8; size as usize];
    init_state(
        &mut state,
        &prog,
        columine_vm::state_init::DEFAULT_ACCEPTED_PROGRAM_MAGICS,
    )
    .expect("init_state");

    // Plain and delta paths, zero columns: both must no-op cleanly.
    let cols: Vec<&[u8]> = Vec::new();
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 0));
    vm.undo_enable(&state).expect("undo window opens");
    let cp = vm.undo_checkpoint();
    assert_eq!(OK, vm.execute_batch_delta(&mut state, &prog, &cols, 0));
    vm.undo_rollback(&mut state, cp).expect("valid checkpoint");
    vm.undo_commit();
}

/// A non-empty batch whose program references columns the batch does not
/// carry (or carries short) is a malformed batch: every batch-driven op
/// refuses with COLUMN_UNDERRUN. Found live by an embedding host's runtime gate —
/// a scenario batch with `batch_len == 1` and zero columns drove FOR_EACH's
/// AGG_COUNT off an empty type column, and the panic aborted the host process
/// through its extern "C" entry. Same program as
/// `empty_batch_with_no_column_pointers_is_ok`; the batch length is the only
/// difference between "clean no-op" and "named refusal".
#[test]
fn nonempty_batch_with_missing_columns_is_column_underrun() {
    let hex = "0000000000000000000000000000000000000000000000000000000000000000434c4d31020000000100000000000000060000000c000000100002020000e00101010000000200410000";
    let prog: Vec<u8> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect();
    let mut vm = Vm::default();
    let size = calculate_state_size(
        &prog,
        columine_vm::state_init::DEFAULT_ACCEPTED_PROGRAM_MAGICS,
    )
    .expect("valid program admission");
    let mut state = vec![0u8; size as usize];
    init_state(
        &mut state,
        &prog,
        columine_vm::state_init::DEFAULT_ACCEPTED_PROGRAM_MAGICS,
    )
    .expect("init_state");

    let underrun = ErrorCode::ColumnUnderrun as u32;

    // No columns at all.
    let cols: Vec<&[u8]> = Vec::new();
    assert_eq!(underrun, vm.execute_batch(&mut state, &prog, &cols, 1));

    // Type column (index 1) present but one cell short of the batch.
    let keys = [0u32, 0];
    let short_types = [1u32];
    let cols: Vec<&[u8]> = vec![u32s_as_bytes(&keys), u32s_as_bytes(&short_types)];
    assert_eq!(underrun, vm.execute_batch(&mut state, &prog, &cols, 2));

    // Fully covered batch still counts.
    let keys = [0u32];
    let types_col = [1u32];
    let cols: Vec<&[u8]> = vec![u32s_as_bytes(&keys), u32s_as_bytes(&types_col)];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 1));
}

/// Scalar latest() uses SCALAR_U32=8, SCALAR_F64=9, and SCALAR_I64=10;
/// 6–7 are reserved. The bytes below are the exact TypeScript-compiled
/// `latest('score')` capture.
/// init `05 09 00` = scalar slot AggType SCALAR_F64, reduce = FOR_EACH(type_col=1,
/// id=1) around `48 00 03 02` (slot 0, val_col 3, cmp_col 2).
#[test]
fn ts_scalar_latest_program_writes_value_and_cmp() {
    let hex = "0000000000000000000000000000000000000000000000000000000000000000434c4d31020000000100000000000000060000000e000000100005090000e001010100000004004800030200";
    let prog: Vec<u8> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect();
    let mut vm = Vm::default();
    let size = calculate_state_size(
        &prog,
        columine_vm::state_init::DEFAULT_ACCEPTED_PROGRAM_MAGICS,
    )
    .expect("valid program admission");
    assert!(size > 0, "state size must be > 0");
    let mut state = vec![0u8; size as usize];
    init_state(
        &mut state,
        &prog,
        columine_vm::state_init::DEFAULT_ACCEPTED_PROGRAM_MAGICS,
    )
    .expect("init_state");

    let keys = [0u32];
    let type_ids = [1u32];
    let cmp = [5000.0f64];
    let vals = [42.5f64];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&keys),
        u32s_as_bytes(&type_ids),
        f64s_as_bytes(&cmp),
        f64s_as_bytes(&vals),
    ];
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 1));

    let (off, _) = (slot_offset(&state, 0), slot_cap(&state, 0));
    let base = off as usize;
    let value = f64::from_le_bytes(state[base..base + 8].try_into().unwrap());
    let cmp_ts = f64::from_le_bytes(state[base + 8..base + 16].try_into().unwrap());
    assert_eq!(42.5, value, "scalar value written");
    assert_eq!(5000.0, cmp_ts, "cmp timestamp written");
}

#[test]
fn incompatible_state_format_is_refused_without_mutating_the_image() {
    let program = build_test_program(16, 0);
    let mut state = init(&program);
    state[columine_types::types::StateHeaderOffset::FORMAT_VERSION as usize] =
        columine_types::types::STATE_FORMAT_VERSION - 1;
    let before = state.clone();
    let mut vm = Vm::default();
    let keys = [3u32];
    let values = [30u32];
    let cols = [u32s_as_bytes(&keys), u32s_as_bytes(&values)];
    assert_eq!(
        vm.execute_batch(&mut state, &program, &cols, 1),
        ErrorCode::InvalidState as u32
    );
    assert_eq!(
        vm.evict_all_expired(false, &mut state, 100.0),
        Err(ErrorCode::InvalidState)
    );
    assert_eq!(state, before);
}

// =============================================================================
// Index operands past one byte
// =============================================================================

/// `count` HASHSET slots of requested capacity 16, slot `s` defined by an
/// index operand, and one `BATCH_SET_INSERT` per `(slot, column)` pair.
fn wide_set_program(count: u32, inserts: &[(u32, u32)]) -> Vec<u8> {
    let mut init = Vec::new();
    for slot in 0..count {
        init.push(Opcode::SlotDef as u8);
        init.extend(encode_index(slot).as_bytes());
        init.extend([SlotType::HashSet as u8, 16, 0]);
    }
    let mut reduce = Vec::new();
    for &(slot, col) in inserts {
        reduce.push(Opcode::BatchSetInsert as u8);
        reduce.extend(encode_index(slot).as_bytes());
        reduce.extend(encode_index(col).as_bytes());
    }
    program(count, count, &init, &reduce)
}

/// Slot and column operands are indexes, so a program reaches slot 299 and
/// column 299 exactly as it reaches slot 0: on either side of the one- and
/// two-byte boundaries (127/128, 255/256) every insert lands in its own slot
/// and reads its own column.
#[test]
fn slots_and_columns_past_one_byte_are_addressed_exactly() {
    const SLOTS: u32 = 300;
    let addressed = [0, 127, 128, 255, 256, 299];
    let prog = wide_set_program(SLOTS, &addressed.map(|slot| (slot, SLOTS - 1 - slot)));
    let mut state = init(&prog);
    let cells: Vec<[u32; 1]> = (0..SLOTS).map(|col| [10_000 + col]).collect();
    let cols: Vec<&[u8]> = cells.iter().map(|cell| u32s_as_bytes(cell)).collect();
    assert_eq!(OK, Vm::default().execute_batch(&mut state, &prog, &cols, 1));

    for slot in 0..SLOTS {
        let set = CheckedSet::bind(&state, slot).expect("valid set slot");
        let read_col = SLOTS - 1 - slot;
        assert_eq!(
            set.contains(10_000 + read_col),
            addressed.contains(&slot),
            "slot {slot}"
        );
    }
}

/// The growth signal names a slot past one byte, and growing that slot is
/// what lets the batch through.
#[test]
fn growth_reports_and_grows_a_slot_past_one_byte() {
    const SLOTS: u32 = 300;
    let prog = wide_set_program(SLOTS, &[(299, 0)]);
    let mut state = init(&prog);
    let elems: Vec<u32> = (1..=40).collect();
    let cols: Vec<&[u8]> = vec![u32s_as_bytes(&elems)];
    let mut vm = Vm::default();
    assert_eq!(NEEDS_GROWTH, vm.execute_batch(&mut state, &prog, &cols, 40));
    assert_eq!(vm.needs_growth_slot(), Some(299));

    while vm.execute_batch(&mut state, &prog, &cols, 40) == NEEDS_GROWTH {
        let mut grown = vec![
            0u8;
            calculate_grown_state_size(&state, 299).expect("valid growth geometry")
                as usize
        ];
        grow_state(&state, &mut grown, 299).expect("grow_state");
        state = grown;
    }
    let set = CheckedSet::bind(&state, 299).expect("valid set slot");
    assert!(elems.iter().all(|&e| set.contains(e)));
}

/// Each VM owns its growth register: a second VM's refusal does not rename
/// the first VM's slot, and a batch that does not ask for growth clears it.
#[test]
fn each_vm_names_only_its_own_growth_slot() {
    const SLOTS: u32 = 300;
    let wide = wide_set_program(SLOTS, &[(299, 0)]);
    let narrow = wide_set_program(SLOTS, &[(7, 0)]);
    let elems: Vec<u32> = (1..=40).collect();
    let cols: Vec<&[u8]> = vec![u32s_as_bytes(&elems)];
    let (mut wide_state, mut narrow_state) = (init(&wide), init(&narrow));
    let (mut wide_vm, mut narrow_vm) = (Vm::default(), Vm::default());
    assert_eq!(
        NEEDS_GROWTH,
        wide_vm.execute_batch(&mut wide_state, &wide, &cols, 40)
    );
    assert_eq!(
        NEEDS_GROWTH,
        narrow_vm.execute_batch(&mut narrow_state, &narrow, &cols, 40)
    );
    assert_eq!(Some(299), wide_vm.needs_growth_slot());
    assert_eq!(Some(7), narrow_vm.needs_growth_slot());

    let one = [1u32];
    let cols: Vec<&[u8]> = vec![u32s_as_bytes(&one)];
    assert_eq!(OK, wide_vm.execute_batch(&mut wide_state, &wide, &cols, 1));
    assert_eq!(None, wide_vm.needs_growth_slot());
    assert_eq!(Some(7), narrow_vm.needs_growth_slot());
}

/// One exported delta window `[from, to)`: its undo and redo segments.
fn export_window(vm: &mut Vm, from: u32, to: u32) -> (Vec<u8>, Vec<u8>) {
    assert_eq!(Ok(to - from), vm.delta_export_segment(from, to));
    (
        vm.delta_export_undo().to_vec(),
        vm.delta_export_redo().to_vec(),
    )
}

/// Reverse, then forward, one exported window from its end image. Each step
/// must reproduce the full image it was exported from, byte for byte.
fn assert_window_replays(start: &[u8], end: &[u8], undo: &[u8], redo: &[u8]) {
    let entry = Vm::delta_export_entry_size();
    let mut replay = Vm::default();
    let mut image = end.to_vec();
    replay
        .delta_apply_rollback_segment(&mut image, undo, entry)
        .expect("valid undo window");
    assert_eq!(
        start,
        image.as_slice(),
        "undo window restores the start image"
    );
    replay
        .delta_apply_rollforward_segment(&mut image, redo, entry)
        .expect("valid redo window");
    assert_eq!(
        end,
        image.as_slice(),
        "redo window reproduces the end image"
    );
}

/// Struct scatter (0x3e) over a seeded stale row: every field bit is clear while
/// the title, status and boost payloads hold nonzero bytes. Two real asserts land
/// in their own savepoints. Each rollback restores the exact image, stale payloads
/// included, and both windows replay exactly through their exported segments.
#[test]
fn struct_scatter_stale_unset_payload_nested_savepoints_roll_back_to_exact_images() {
    const T: u32 = 2111;
    let prog = build_struct_map_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let nodes = StructMapSlot::bind(&state, 0);
    let node = nodes.upsert(&mut state, 600).expect("seed node row");
    let row = nodes.row_off(node.pos);
    nodes.clear_bitset(&mut state, row);
    let title = row + nodes.field_offset(&state, 0);
    let status = row + nodes.field_offset(&state, 1);
    let boost = row + nodes.field_offset(&state, 2);
    bytes::write_u32(&mut state, title, 0xDEAD_0001);
    bytes::write_u32(&mut state, status, 0xDEAD_0002);
    bytes::write_u64(&mut state, boost, 0xDEAD_BEEF_F00D_CAFE);
    let before = state.clone();

    vm.undo_enable(&state).expect("undo window opens");
    let cp0 = vm.undo_checkpoint();

    // Savepoint 1: the title assert finds its bit clear and sets it.
    let assert_title = [ScatterElement {
        route: 0,
        op_retract: 0,
        key: 600,
        v_str: 7001,
        v_num: 0.0,
        v_set: 0,
    }];
    assert_eq!(
        OK,
        run_struct_scatter_lane(&mut vm, &mut state, &prog, T, &assert_title, true)
    );
    assert!(bit_set(&state, row, 0));
    assert_eq!(7001, bytes::read_u32(&state, title));
    assert!(!bit_set(&state, row, 2));
    let cp1 = vm.undo_checkpoint();
    let image1 = state.clone();

    // Savepoint 2: the boost assert sets its own bit over the stale payload.
    let assert_boost = [ScatterElement {
        route: 4,
        op_retract: 0,
        key: 600,
        v_str: 0,
        v_num: 2.5,
        v_set: 0,
    }];
    assert_eq!(
        OK,
        run_struct_scatter_lane(&mut vm, &mut state, &prog, T, &assert_boost, true)
    );
    assert!(bit_set(&state, row, 2));
    assert_eq!(2.5, f64::from_bits(bytes::read_u64(&state, boost)));
    let cp2 = vm.undo_checkpoint();
    let image2 = state.clone();
    assert!(cp0 < cp1 && cp1 < cp2);
    assert_ne!(image1, image2);

    // Export both windows before any rollback truncates the journal.
    let (undo_inner, redo_inner) = export_window(&mut vm, cp1, cp2);
    let (undo_all, redo_all) = export_window(&mut vm, cp0, cp2);
    assert_window_replays(&image1, &image2, &undo_inner, &redo_inner);
    assert_window_replays(&before, &image2, &undo_all, &redo_all);

    // Live nested rollbacks, innermost first.
    vm.undo_rollback(&mut state, cp2).expect("valid checkpoint");
    assert_eq!(image2, state);
    vm.undo_rollback(&mut state, cp1).expect("valid checkpoint");
    assert_eq!(
        image1, state,
        "savepoint 1 restores its exact image over the stale boost"
    );
    vm.undo_rollback(&mut state, cp0).expect("valid checkpoint");
    assert_eq!(
        before, state,
        "the outer window restores the stale row byte for byte"
    );
    vm.undo_commit();
}

/// Guarded scatter (0x3f) over a seeded stale stamp: the row's guard bits are
/// clear while its guard payloads and verdict/detail payloads hold nonzero bytes.
/// The first transaction stamps the unset guard. A newer transaction stacks on it
/// in its own savepoint. An older one loses and writes nothing. Each savepoint
/// restores its exact image, and both windows replay exactly.
#[test]
fn guarded_scatter_stale_stamp_bitset_nested_savepoints_roll_back_to_exact_images() {
    const T: u32 = 3111;
    let prog = build_guarded_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    let checks = StructMapSlot::bind(&state, 0);
    let node = checks.upsert(&mut state, 500).expect("seed guarded row");
    let row = checks.row_off(node.pos);
    checks.clear_bitset(&mut state, row);
    let verdict = row + checks.field_offset(&state, 0);
    let detail = row + checks.field_offset(&state, 1);
    let at_ns = row + checks.field_offset(&state, 2);
    let ordinal = row + checks.field_offset(&state, 3);
    bytes::write_u32(&mut state, verdict, 0xDEAD_0001);
    bytes::write_u32(&mut state, detail, 0xDEAD_0002);
    bytes::write_u64(&mut state, at_ns, 0xBAAD_F00D_0000_0001);
    bytes::write_u64(&mut state, ordinal, 0xBAAD_F00D_0000_0002);
    let before = state.clone();

    vm.undo_enable(&state).expect("undo window opens");
    let cp0 = vm.undo_checkpoint();

    // Savepoint 1: the stamp bits are clear, so no guard is stored and the first
    // transaction lands, stamping the row.
    let first = guarded_transaction(500, 7001, 7011, 8001, 1000, 10);
    assert_eq!(
        OK,
        run_guarded_scatter_lane(&mut vm, &mut state, &prog, T, &first, true)
    );
    assert!(bit_set(&state, row, 2));
    assert!(bit_set(&state, row, 3));
    assert_eq!(7001, bytes::read_u32(&state, verdict));
    assert_eq!(1000, read_i64(&state, at_ns));
    assert_eq!(10, read_i64(&state, ordinal));
    assert!(tag_present(&state, 8001));
    let cp1 = vm.undo_checkpoint();
    let image1 = state.clone();

    // Savepoint 2: a newer transaction wins over the stamp the first one wrote.
    let second = guarded_transaction(500, 7002, 7012, 8002, 2000, 20);
    assert_eq!(
        OK,
        run_guarded_scatter_lane(&mut vm, &mut state, &prog, T, &second, true)
    );
    assert_eq!(7002, bytes::read_u32(&state, verdict));
    assert_eq!(2000, read_i64(&state, at_ns));
    assert_eq!(20, read_i64(&state, ordinal));
    assert!(tag_present(&state, 8002));
    let cp2 = vm.undo_checkpoint();
    let image2 = state.clone();
    assert_ne!(image1, image2);

    // An older transaction loses to the remembered guard: no route writes and no
    // journal entry is made.
    let older = guarded_transaction(500, 7003, 7013, 8003, 1500, 15);
    assert_eq!(
        OK,
        run_guarded_scatter_lane(&mut vm, &mut state, &prog, T, &older, true)
    );
    assert_eq!(cp2, vm.undo_checkpoint());
    assert_eq!(image2, state);
    assert!(!tag_present(&state, 8003));

    // Export both windows before any rollback truncates the journal.
    let (undo_inner, redo_inner) = export_window(&mut vm, cp1, cp2);
    let (undo_all, redo_all) = export_window(&mut vm, cp0, cp2);
    assert_window_replays(&image1, &image2, &undo_inner, &redo_inner);
    assert_window_replays(&before, &image2, &undo_all, &redo_all);

    // Live nested rollbacks, innermost first.
    vm.undo_rollback(&mut state, cp2).expect("valid checkpoint");
    assert_eq!(image2, state);
    vm.undo_rollback(&mut state, cp1).expect("valid checkpoint");
    assert_eq!(
        image1, state,
        "savepoint 1 restores its exact image over the stale stamp"
    );
    vm.undo_rollback(&mut state, cp0).expect("valid checkpoint");
    assert_eq!(
        before, state,
        "the outer window restores the stale stamp byte for byte"
    );
    vm.undo_commit();
}

/// Fill the window with `fill` one-record captures of the derived-facts flag,
/// run `op`, and when it overflowed, roll back to the window start: the image
/// must be the pristine one byte for byte. Returns whether `op` overflowed.
fn overflowed_op_rolls_back_exactly(
    pristine: &[u8],
    fill: u32,
    delta: bool,
    op: impl FnOnce(&mut Vm, &mut Vec<u8>),
) -> Result<bool, (u32, bool, Vec<usize>)> {
    use columine_types::types::StateHeaderOffset;
    let flag = StateHeaderOffset::DERIVED_FACTS_CHANGE_FLAG;
    let mut state = pristine.to_vec();
    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("window opens");
    let savepoint = vm.undo_checkpoint();
    for _ in 0..fill {
        vm.capture_mutation(delta, &mut state, &[(flag, 1)], |s| s[flag as usize] ^= 1)
            .expect("filler");
    }
    op(&mut vm, &mut state);
    if !vm.undo_has_overflow() {
        return Ok(false);
    }
    vm.undo_rollback(&mut state, savepoint)
        .expect("a pre-overflow savepoint rolls back");
    if state.as_slice() == pristine {
        Ok(true)
    } else {
        let diff = (0..state.len())
            .filter(|&i| state[i] != pristine[i])
            .collect();
        Err((fill, delta, diff))
    }
}

/// An eviction whose outer capture opens before the overflow while a nested
/// removal overflows the window: the outer frame takes EVICTED_COUNT from 4
/// through 0 back to 4, so it closes unchanged while the shadow holds an
/// intermediate count. Every overflow position inside the loop, on both
/// lanes, rolls back to the pristine image byte for byte.
#[test]
fn an_eviction_overflowing_inside_its_loop_rolls_back_byte_exact() {
    let prog = build_ttl_map_program(16, 0, true);
    let mut pristine = init(&prog);
    let mut vm = Vm::default();
    let run = |vm: &mut Vm, state: &mut Vec<u8>, keys: &[u32], ts: &[f64]| {
        let vals: Vec<u32> = keys.iter().map(|k| k * 10).collect();
        let cols: Vec<&[u8]> = vec![u32s_as_bytes(keys), u32s_as_bytes(&vals), f64s_as_bytes(ts)];
        assert_eq!(OK, vm.execute_batch(state, &prog, &cols, keys.len() as u32));
    };
    run(
        &mut vm,
        &mut pristine,
        &[1, 2, 3, 4],
        &[100.0, 101.0, 102.0, 103.0],
    );
    assert_eq!(Ok(4), vm.evict_all_expired(false, &mut pristine, 114.0));
    run(
        &mut vm,
        &mut pristine,
        &[5, 6, 7, 8],
        &[200.0, 201.0, 202.0, 203.0],
    );
    assert_eq!(4, SlotMetaView::read(&pristine, 0).evicted_count(&pristine));

    let mut overflowed = 0;
    let mut failures = Vec::new();
    for fill in (UNDO_CAPACITY - 160)..=UNDO_CAPACITY {
        for delta in [false, true] {
            match overflowed_op_rolls_back_exactly(&pristine, fill, delta, |vm, state| {
                assert_eq!(Ok(4), vm.evict_all_expired(delta, state, 214.0));
            }) {
                Ok(true) => overflowed += 1,
                Ok(false) => {}
                Err(failure) => failures.push(failure),
            }
        }
    }
    assert!(overflowed > 0, "no fill overflowed the window");
    assert!(
        failures.is_empty(),
        "{} overflowing fills diverge: {:?}",
        failures.len(),
        &failures[..failures.len().min(6)]
    );
}

/// A TTL upsert batch that inserts and updates keys, with every fill that
/// puts the overflow at a different capture of the batch: each overflowing
/// run lands the whole batch and rolls back to the pristine image exactly.
#[test]
fn a_ttl_upsert_batch_overflowing_at_every_position_rolls_back_byte_exact() {
    let prog = build_ttl_map_program(64, 0, true);
    let mut pristine = init(&prog);
    let keys: Vec<u32> = (1..=12).collect();
    let vals: Vec<u32> = keys.iter().map(|k| k * 10).collect();
    let ts: Vec<f64> = keys.iter().map(|&k| 100.0 + f64::from(k)).collect();
    let seed: Vec<&[u8]> = vec![
        u32s_as_bytes(&keys[..6]),
        u32s_as_bytes(&vals[..6]),
        f64s_as_bytes(&ts[..6]),
    ];
    assert_eq!(
        OK,
        Vm::default().execute_batch(&mut pristine, &prog, &seed, 6)
    );
    let later: Vec<f64> = ts.iter().map(|t| t + 50.0).collect();
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&keys),
        u32s_as_bytes(&vals),
        f64s_as_bytes(&later),
    ];

    let mut expected = pristine.clone();
    assert_eq!(
        OK,
        Vm::default().execute_batch(&mut expected, &prog, &cols, 12)
    );

    let mut overflowed = 0;
    let mut failures = Vec::new();
    for fill in (UNDO_CAPACITY - 200)..=UNDO_CAPACITY {
        for delta in [false, true] {
            match overflowed_op_rolls_back_exactly(&pristine, fill, delta, |vm, state| {
                let code = if delta {
                    vm.execute_batch_delta(state, &prog, &cols, 12)
                } else {
                    vm.execute_batch(state, &prog, &cols, 12)
                };
                assert_eq!(OK, code, "fill {fill}, delta {delta}");
            }) {
                Ok(true) => overflowed += 1,
                Ok(false) => {}
                Err(failure) => failures.push(failure),
            }
        }
    }
    assert!(overflowed > 0, "no fill overflowed the window");
    assert!(
        failures.is_empty(),
        "{} overflowing fills diverge: {:?}",
        failures.len(),
        &failures[..failures.len().min(6)]
    );
}
