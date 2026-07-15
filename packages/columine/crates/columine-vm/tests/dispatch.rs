//! vm_test.zig blocks that exercise the DISPATCH loop (`vm_execute_batch`),
//! the undo/rollback surface, TTL eviction through the live reducer path, and
//! growth signaling. Each test cites the vm_test.zig block it translates;
//! scenarios and expected values are identical.

use columine_types::types::{
    ChangeFlag, EMPTY_KEY, ErrorCode, SLOT_META_SIZE, STATE_HEADER_SIZE, SlotMetaOffset,
};
use columine_vm::bytes;
use columine_vm::state_init::{
    calculate_grown_state_size, calculate_state_size, grow_state, init_state, needs_growth_slot,
};
use columine_vm::struct_map::StructMapSlot;
use columine_vm::vm::{
    Vm, f64s_as_bytes, u32s_as_bytes, vm_map_get, vm_set_contains, vm_struct_map_get_row_ptr,
};

const OK: u32 = ErrorCode::Ok as u32;
const NEEDS_GROWTH: u32 = ErrorCode::NeedsGrowth as u32;

// =============================================================================
// Program builders (vm_test.zig:78-260, 1200-1293, 1530-1600, 1957-2036,
// 2450-2530 equivalents)
// =============================================================================

/// Assemble a full program: 32-byte hash prefix + 14-byte content header +
/// init + reduce.
fn program(num_slots: u8, num_inputs: u8, init: &[u8], reduce: &[u8]) -> Vec<u8> {
    let mut prog = vec![0u8; 32];
    prog.extend([0x41, 0x58, 0x45, 0x31, 1, 0, num_slots, num_inputs, 0, 0]);
    prog.extend((init.len() as u16).to_le_bytes());
    prog.extend((reduce.len() as u16).to_le_bytes());
    prog.extend_from_slice(init);
    prog.extend_from_slice(reduce);
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

/// vm_test.zig:82 `buildTestProgram` — HASHMAP(LAST) + AGG COUNT.
fn build_test_program(cap_lo: u8, cap_hi: u8) -> Vec<u8> {
    let mut init = Vec::new();
    init.extend(slot_def(0, 0x00, cap_lo, cap_hi));
    init.extend(slot_def(1, 0x02, 2, 0)); // AGGREGATE, AggType.COUNT
    let reduce = [0x22, 0, 0, 1, 0x41, 1];
    program(2, 2, &init, &reduce)
}

/// vm_test.zig:185 `buildTTLMapProgram` — TTL HASHMAP + BATCH_MAP_UPSERT_LATEST.
fn build_ttl_map_program(cap_lo: u8, cap_hi: u8, has_evict_trigger: bool) -> Vec<u8> {
    let evict_flag = if has_evict_trigger { 0x20 } else { 0 };
    let init = slot_def_ttl(0, 0x10 | evict_flag, cap_lo, cap_hi, 10.0, 0.0, 2);
    let reduce = [0x20, 0, 0, 1, 2, 1]; // slot, key, val, ts, cmp_type=f64
    program(1, 3, &init, &reduce)
}

/// vm_test.zig:236 `buildTTLSetProgram` — TTL HASHSET + BATCH_SET_INSERT.
fn build_ttl_set_program(cap_lo: u8, cap_hi: u8) -> Vec<u8> {
    let init = slot_def_ttl(0, 0x01 | 0x10, cap_lo, cap_hi, 10.0, 0.0, 1);
    let reduce = [0x30, 0, 0];
    program(1, 2, &init, &reduce)
}

/// vm_test.zig:1211 `buildForEachTestProgramMulti` — FOR_EACH { MAP_UPSERT_LAST + AGG_COUNT }.
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

/// vm_test.zig:1530 `buildBlockStructMapTestProgram` — FOR_EACH { 0x80 }.
fn build_block_struct_map_program(type_id: u32) -> Vec<u8> {
    let init = slot_struct_map(0, 6, 4, &[0, 4]); // STRUCT_MAP=6; UINT32 + STRING
    let body = [0x80u8, 0, 1, 2, 2, 0, 3, 1, 0];
    let mut reduce = vec![0xE0, 0, 1];
    reduce.extend(type_id.to_le_bytes());
    reduce.extend((body.len() as u16).to_le_bytes());
    reduce.extend(body);
    program(1, 4, &init, &reduce)
}

/// vm_test.zig:1957 `buildFlatMapTestProgram` — FOR_EACH { FLAT_MAP { 0x80 } }.
/// Faithful byte quirk: the Zig builder writes type_flags 0x05 here (vs the
/// enum value 6 in its sibling builders) — SLOT_STRUCT_MAP's init path keys
/// on the opcode, not the flag byte's type bits.
fn build_flat_map_program(type_id: u32) -> Vec<u8> {
    let init = slot_struct_map(0, 0x05, 4, &[0, 4]);
    let inner_body = [0x80u8, 0, 2, 2, 3, 0, 4, 1, 0];
    let mut flat_map = vec![0xE1, 1, 0xFF];
    flat_map.extend((inner_body.len() as u16).to_le_bytes());
    flat_map.extend(inner_body);
    let mut reduce = vec![0xE0, 0, 1];
    reduce.extend(type_id.to_le_bytes());
    reduce.extend((flat_map.len() as u16).to_le_bytes());
    reduce.extend(&flat_map);
    program(1, 5, &init, &reduce)
}

/// vm_test.zig:2480 `buildStructMapTestProgram` — top-level 0x80.
fn build_struct_map_program(cap_lo: u8, cap_hi: u8) -> Vec<u8> {
    let init = slot_struct_map(0, 6, cap_lo, &[0, 4]);
    let mut init2 = init.clone();
    init2[4] = cap_hi;
    let reduce = [0x80u8, 0, 0, 2, 1, 0, 2, 1, 0];
    program(1, 3, &init2, &reduce)
}

/// vm_test.zig:2036 `buildProbeScatterTestProgram` — FOR_EACH { FLAT_MAP { 0x2f } }.
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
    let mut flat_map = vec![0xE1, 1, 0xFF];
    flat_map.extend((scatter.len() as u16).to_le_bytes());
    flat_map.extend(scatter);
    let mut reduce = vec![0xE0, 0, 1];
    reduce.extend(type_id.to_le_bytes());
    reduce.extend((flat_map.len() as u16).to_le_bytes());
    reduce.extend(&flat_map);
    program(4, 3, &init, &reduce)
}

// =============================================================================
// State / metadata helpers
// =============================================================================

fn init(prog: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(prog);
    assert!(size > 0, "state size must be > 0");
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, prog).expect("init_state");
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

fn bit_set(state: &[u8], row: u32, fi: u8) -> bool {
    StructMapSlot::is_field_set(state, row, fi)
}

/// vm_test.zig:2150 `seedProbeRow`.
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
// TTL through the live reducer path (vm_test.zig:406-531)
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

    let (off, cap) = (slot_offset(&state, 0), slot_cap(&state, 0));
    assert_eq!(7, vm_map_get(&state, off, cap, 42));

    assert_eq!(0, vm.evict_all_expired(&mut state, 105.0));
    assert_eq!(7, vm_map_get(&state, off, cap, 42));

    assert_eq!(1, vm.evict_all_expired(&mut state, 111.0));
    assert_eq!(EMPTY_KEY, vm_map_get(&state, off, cap, 42));
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

    let (off, cap) = (slot_offset(&state, 0), slot_cap(&state, 0));
    // cutoff=140: the stale ts=100 index entry must not evict the ts=200 value.
    assert_eq!(0, vm.evict_all_expired(&mut state, 150.0));
    assert_eq!(200, vm_map_get(&state, off, cap, 9));
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

    let (off, cap) = (slot_offset(&state, 0), slot_cap(&state, 0));
    assert!(vm_set_contains(&mut vm.bitmap_env, &state, off, cap, 77));

    assert_eq!(0, vm.evict_all_expired(&mut state, 150.0));
    assert!(vm_set_contains(&mut vm.bitmap_env, &state, off, cap, 77));

    assert_eq!(1, vm.evict_all_expired(&mut state, 211.0));
    assert!(!vm_set_contains(&mut vm.bitmap_env, &state, off, cap, 77));
}

#[test]
fn ttl_eviction_index_overflow_returns_needs_growth_instead_of_dropping() {
    let prog = build_ttl_map_program(4, 0, false);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    // Force a tiny eviction-index capacity for deterministic overflow.
    let meta_base = STATE_HEADER_SIZE;
    bytes::write_u32(
        &mut state,
        meta_base + SlotMetaOffset::EVICTION_INDEX_CAPACITY,
        1,
    );

    let keys = [1u32, 2];
    let vals = [10u32, 20];
    let tss = [100.0f64, 101.0];
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&keys),
        u32s_as_bytes(&vals),
        f64s_as_bytes(&tss),
    ];
    assert_eq!(NEEDS_GROWTH, vm.execute_batch(&mut state, &prog, &cols, 2));
    assert_eq!(0, needs_growth_slot());
}

// =============================================================================
// Slot growth through dispatch (vm_test.zig:533-871)
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
    assert_eq!(0, needs_growth_slot());
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

    let grown_size = calculate_grown_state_size(&state, 0);
    let mut new_state = vec![0u8; grown_size as usize];
    grow_state(&state, &mut new_state, 0).expect("grow_state");

    assert_eq!(32, slot_cap(&new_state, 0));
    assert_eq!(11, slot_size(&new_state, 0));

    let new_off = slot_offset(&new_state, 0);
    for i in 0..11u32 {
        assert_eq!(i * 10, vm_map_get(&new_state, new_off, 32, i));
    }
    // Aggregate preserved (copied, not re-executed).
    let new_agg_off = slot_offset(&new_state, 1);
    assert_eq!(11, bytes::read_u64(&new_state, new_agg_off));

    // Room again: 12th key inserts without NEEDS_GROWTH.
    let key = [100u32];
    let val = [1000u32];
    let cols: Vec<&[u8]> = vec![u32s_as_bytes(&key), u32s_as_bytes(&val)];
    assert_eq!(OK, vm.execute_batch(&mut new_state, &prog, &cols, 1));
    assert_eq!(1000, vm_map_get(&new_state, new_off, 32, 100));
}

#[test]
fn slot_growth_hashset_growth_preserves_elements() {
    // vm_test.zig:693 — one HASHSET slot, BATCH_SET_INSERT, grow after fill.
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

    let grown_size = calculate_grown_state_size(&state, 0);
    let mut new_state = vec![0u8; grown_size as usize];
    grow_state(&state, &mut new_state, 0).expect("grow_state");

    assert_eq!(32, slot_cap(&new_state, 0));
    assert_eq!(11, slot_size(&new_state, 0));
    let new_off = slot_offset(&new_state, 0);
    for i in 0..11u32 {
        assert!(vm_set_contains(
            &mut vm.bitmap_env,
            &new_state,
            new_off,
            32,
            i + 1000
        ));
    }
    // And the overflowing element now fits.
    assert_eq!(OK, vm.execute_batch(&mut new_state, &prog, &cols, 1));
    assert!(vm_set_contains(
        &mut vm.bitmap_env,
        &new_state,
        new_off,
        32,
        5000
    ));
}

// =============================================================================
// FOR_EACH (vm_test.zig:1295-1596)
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
    let (off, cap) = (slot_offset(&state, 0), slot_cap(&state, 0));
    assert_eq!(10, vm_map_get(&state, off, cap, 100));
    assert_eq!(30, vm_map_get(&state, off, cap, 300));
    assert_eq!(EMPTY_KEY, vm_map_get(&state, off, cap, 200));
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
    let (off, cap) = (slot_offset(&state, 0), slot_cap(&state, 0));
    assert_eq!(10, vm_map_get(&state, off, cap, 100));
    assert_eq!(20, vm_map_get(&state, off, cap, 200));
    assert_eq!(40, vm_map_get(&state, off, cap, 400));
    assert_eq!(EMPTY_KEY, vm_map_get(&state, off, cap, 300));
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
    let row_100 = struct_map_row(&state, 0, 100);
    assert_ne!(0xFFFF_FFFF, row_100);
    assert_eq!(42, bytes::read_u32(&state, row_100 + 1)); // bitset(1) + field 0
    assert_eq!(0xFFFF_FFFF, struct_map_row(&state, 0, 200));
}

// =============================================================================
// FLAT_MAP (vm_test.zig:1752-1993)
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
    let r100 = struct_map_row(&state, 0, 100);
    assert_ne!(0xFFFF_FFFF, r100);
    assert_eq!(10, bytes::read_u32(&state, r100 + 1));
    assert_eq!(1001, bytes::read_u32(&state, r100 + 5));
    let r200 = struct_map_row(&state, 0, 200);
    assert_eq!(20, bytes::read_u32(&state, r200 + 1));
    let r300 = struct_map_row(&state, 0, 300);
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
    let row = struct_map_row(&state, 0, 100);
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
    assert_eq!(0xFFFF_FFFF, struct_map_row(&state, 0, 999));
    assert_ne!(0xFFFF_FFFF, struct_map_row(&state, 0, 300));
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
// STRUCT_MAP_PROBE_SCATTER 0x2f (vm_test.zig:2194-2451)
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
fn probe_scatter_apply_datom_parity_across_kinds_and_skip() {
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
    let row_500 = struct_map_row(&state, 1, 500);
    assert_ne!(0xFFFF_FFFF, row_500);
    assert!(!bit_set(&state, row_500, 0)); // title retracted (MATCH)
    assert_eq!(0, bytes::read_u32(&state, row_500 + 1));
    assert!(bit_set(&state, row_500, 1)); // status survives NON-MATCH retract
    assert_eq!(7002, bytes::read_u32(&state, row_500 + 5));
    assert!(bit_set(&state, row_500, 2)); // boost via typed v_num
    assert_eq!(2.5, f64::from_bits(bytes::read_u64(&state, row_500 + 9)));

    // nodeDeps (slot 2): {(e,7003)=8003} deduped → size 1.
    assert_eq!(1, slot_size(&state, 2));
    let (d_off, d_cap) = (slot_offset(&state, 2), slot_cap(&state, 2));
    assert!(vm_set_contains(
        &mut vm.bitmap_env,
        &state,
        d_off,
        d_cap,
        8003
    ));

    // nodeTouch (slot 3): asserted then retracted → empty.
    assert_eq!(0, slot_size(&state, 3));
    let (t_off, t_cap) = (slot_offset(&state, 3), slot_cap(&state, 3));
    assert!(!vm_set_contains(
        &mut vm.bitmap_env,
        &state,
        t_off,
        t_cap,
        8004
    ));
}

#[test]
fn probe_scatter_kind0_insert_sets_effective_flag_without_delta_mode() {
    const T: u32 = 2006;
    let prog = build_probe_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    seed_probe_row(&mut state, 10, 0, 0, 600, 7001, 0.0, 0);
    vm.undo_enable(&state);
    let cp = vm.undo_checkpoint();

    assert_eq!(OK, run_scatter_one(&mut vm, &mut state, &prog, T, 10));
    assert_eq!(ChangeFlag::INSERTED, slot_change_flags(&state, 1));
    assert!(vm.undo_checkpoint() > cp);
    let row = struct_map_row(&state, 1, 600);
    assert_ne!(0xFFFF_FFFF, row);
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
    vm.undo_enable(&state);
    let cp = vm.undo_checkpoint();

    assert_eq!(OK, run_scatter_one(&mut vm, &mut state, &prog, T, 11));
    assert_eq!(ChangeFlag::UPDATED, slot_change_flags(&state, 1));
    assert!(vm.undo_checkpoint() > cp);
    let row = struct_map_row(&state, 1, 600);
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
    vm.undo_enable(&state);
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
    vm.undo_enable(&state);
    let cp = vm.undo_checkpoint();
    let before = state.clone();

    assert_eq!(OK, run_scatter_one(&mut vm, &mut state, &prog, T, 11));
    assert_eq!(0, slot_change_flags(&state, 1));
    assert_eq!(cp, vm.undo_checkpoint());
    assert_eq!(before, state);

    assert_eq!(OK, run_scatter_one(&mut vm, &mut state, &prog, T, 12));
    assert_eq!(ChangeFlag::REMOVED, slot_change_flags(&state, 1));
    assert!(vm.undo_checkpoint() > cp);
    let row = struct_map_row(&state, 1, 600);
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
    let (d_off, d_cap) = (slot_offset(&state, 2), slot_cap(&state, 2));
    assert!(vm_set_contains(
        &mut vm.bitmap_env,
        &state,
        d_off,
        d_cap,
        8101
    ));
    assert!(vm_set_contains(
        &mut vm.bitmap_env,
        &state,
        d_off,
        d_cap,
        8102
    ));
    assert!(!vm_set_contains(
        &mut vm.bitmap_env,
        &state,
        d_off,
        d_cap,
        7003
    ));
}

#[test]
fn probe_scatter_kind0_assert_then_rollback_restores_absence() {
    const T: u32 = 2003;
    let prog = build_probe_scatter_program(T);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    seed_probe_row(&mut state, 10, 0, 0, 600, 7001, 0.0, 0);

    assert_eq!(0xFFFF_FFFF, struct_map_row(&state, 1, 600));

    vm.undo_enable(&state);
    let cp = vm.undo_checkpoint();
    assert_eq!(OK, run_scatter_one(&mut vm, &mut state, &prog, T, 10));

    let row_after = struct_map_row(&state, 1, 600);
    assert_ne!(0xFFFF_FFFF, row_after);
    assert!(bit_set(&state, row_after, 0));
    assert_eq!(7001, bytes::read_u32(&state, row_after + 1));
    assert_eq!(1, slot_size(&state, 1));

    vm.undo_rollback(&mut state, cp);
    assert_eq!(0xFFFF_FFFF, struct_map_row(&state, 1, 600));
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
    let row = struct_map_row(&state, 1, 700);
    assert_ne!(0xFFFF_FFFF, row);
    assert!(bit_set(&state, row, 0));
    assert_eq!(7001, bytes::read_u32(&state, row + 1));

    vm.undo_enable(&state);
    let cp = vm.undo_checkpoint();
    assert_eq!(OK, run_scatter_one(&mut vm, &mut state, &prog, T, 11));
    assert!(!bit_set(&state, row, 0));
    assert_eq!(0, bytes::read_u32(&state, row + 1));

    vm.undo_rollback(&mut state, cp);
    assert!(bit_set(&state, row, 0));
    assert_eq!(7001, bytes::read_u32(&state, row + 1));
    vm.undo_commit();
}

// =============================================================================
// STRUCT_MAP_UPSERT_LAST 0x80 rollback (vm_test.zig:2453-2662)
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
    let row = struct_map_row(&state, 0, 100);
    assert_ne!(0xFFFF_FFFF, row);
    assert_eq!(0x03, state[row as usize]);
    assert_eq!(42, bytes::read_u32(&state, row + 1));
    assert_eq!(1001, bytes::read_u32(&state, row + 5));

    vm.undo_enable(&state);
    assert!(vm.undo.enabled);
    let cp = vm.undo_checkpoint();
    assert_eq!(0, cp); // fresh enable resets the log
    assert_eq!(
        OK,
        exec_struct_upsert(&mut vm, &mut state, &prog, &[100], &[777], &[2002])
    );

    assert!(vm.undo_checkpoint() > cp); // whole-row journaling fired
    assert_eq!(777, bytes::read_u32(&state, row + 1));
    assert_eq!(2002, bytes::read_u32(&state, row + 5));
    assert!(!vm.undo_has_overflow()); // no shadow snapshot in play

    vm.undo_rollback(&mut state, cp);
    assert_eq!(0x03, state[row as usize]);
    assert_eq!(42, bytes::read_u32(&state, row + 1));
    assert_eq!(1001, bytes::read_u32(&state, row + 5));
    assert_eq!(1, slot_size(&state, 0));
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
    assert_eq!(0xFFFF_FFFF, struct_map_row(&state, 0, 555));

    vm.undo_enable(&state);
    let cp = vm.undo_checkpoint();
    assert_eq!(
        OK,
        exec_struct_upsert(&mut vm, &mut state, &prog, &[555], &[88], &[3003])
    );
    assert_ne!(0xFFFF_FFFF, struct_map_row(&state, 0, 555));
    assert_eq!(2, slot_size(&state, 0));
    assert!(!vm.undo_has_overflow());

    vm.undo_rollback(&mut state, cp);
    assert_eq!(0xFFFF_FFFF, struct_map_row(&state, 0, 555));
    assert_eq!(1, slot_size(&state, 0));
    let row100 = struct_map_row(&state, 0, 100);
    assert_ne!(0xFFFF_FFFF, row100);
    assert_eq!(42, bytes::read_u32(&state, row100 + 1));
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
    let r100 = struct_map_row(&state, 0, 100);
    let r200 = struct_map_row(&state, 0, 200);

    vm.undo_enable(&state);
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
    assert!(!vm.undo_has_overflow());

    vm.undo_rollback(&mut state, cp);
    assert_eq!(2, slot_size(&state, 0));
    assert_eq!(0x03, state[r100 as usize]);
    assert_eq!(42, bytes::read_u32(&state, r100 + 1));
    assert_eq!(1001, bytes::read_u32(&state, r100 + 5));
    assert_eq!(43, bytes::read_u32(&state, r200 + 1));
    assert_eq!(1002, bytes::read_u32(&state, r200 + 5));
    assert_eq!(0xFFFF_FFFF, struct_map_row(&state, 0, 300));
    vm.undo_commit();
}

// =============================================================================
// Struct map through top-level dispatch (vm_test.zig:871-1091)
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
    let row = struct_map_row(&state, 0, 100);
    assert_ne!(0xFFFF_FFFF, row);
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
    let row = struct_map_row(&state, 0, 100);
    assert_eq!(777, bytes::read_u32(&state, row + 1));
    assert_eq!(2002, bytes::read_u32(&state, row + 5));
}

// =============================================================================
// Delta export/apply round-trip (proptest)
// =============================================================================

use proptest::prelude::*;

proptest! {
    /// Rollback through the REAL dispatch path restores logical map content
    /// for arbitrary speculative batches over a committed baseline.
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
        let base_snapshot: Vec<(u32, u32)> = {
            let (off, cap) = (slot_offset(&state, 0), slot_cap(&state, 0));
            keys.iter().map(|&k| (k, vm_map_get(&state, off, cap, k))).collect()
        };
        let base_size = slot_size(&state, 0);
        let base_count = agg_count(&state, 1);

        vm.undo_enable(&state);
        let cp = vm.undo_checkpoint();
        let skeys: Vec<u32> = speculative.iter().map(|(k, _)| *k + 1).collect();
        let svals: Vec<u32> = speculative.iter().map(|(_, v)| *v + 5000).collect();
        let scols: Vec<&[u8]> = vec![u32s_as_bytes(&skeys), u32s_as_bytes(&svals)];
        prop_assert_eq!(OK, vm.execute_batch(&mut state, &prog, &scols, skeys.len() as u32));

        vm.undo_rollback(&mut state, cp);
        vm.undo_commit();

        prop_assert_eq!(base_size, slot_size(&state, 0));
        prop_assert_eq!(base_count, agg_count(&state, 1));
        let (off, cap) = (slot_offset(&state, 0), slot_cap(&state, 0));
        for (k, v) in base_snapshot {
            prop_assert_eq!(v, vm_map_get(&state, off, cap, k));
        }
    }

    /// Delta export/apply: exporting a delta-mode batch's segments and
    /// applying rollback+rollforward to a COPY of the pre-batch state lands
    /// on the post-batch logical content — the Scenario fork-navigation
    /// contract (logical, not byte-exact: rollback leaves tombstones).
    #[test]
    fn delta_segments_roundtrip_fork_navigation(
        batch in proptest::collection::vec((0u32..200, 1u32..1000), 1..10),
        remove_picks in proptest::collection::vec(proptest::bool::ANY, 1..10),
    ) {
        let prog = build_test_program(16, 0);
        let mut state = init(&prog);
        let mut vm = Vm::default();

        vm.undo_enable(&state);
        let keys: Vec<u32> = batch.iter().map(|(k, _)| *k + 1).collect();
        let vals: Vec<u32> = batch.iter().map(|(_, v)| *v).collect();
        let pre_state = state.clone();
        let cols: Vec<&[u8]> = vec![u32s_as_bytes(&keys), u32s_as_bytes(&vals)];
        prop_assert_eq!(OK, vm.execute_batch_delta(&mut state, &prog, &cols, keys.len() as u32));

        // A second delta batch REMOVES a subset of the keys, so the exported
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

        let count = vm.delta_export_segment(0, vm.undo.delta_count());
        prop_assert!(count > 0);
        let undo_seg = vm.delta_export_undo_bytes();
        let redo_seg = vm.delta_export_redo_bytes();
        let entry_size = Vm::delta_export_entry_size();

        // Roll the LIVE state back via the exported undo segment → logical
        // pre-batch content.
        let mut rolled = state.clone();
        vm.delta_apply_rollback_segment(&mut rolled, &undo_seg, entry_size);
        let (off, cap) = (slot_offset(&pre_state, 0), slot_cap(&pre_state, 0));
        for &k in &keys {
            prop_assert_eq!(vm_map_get(&pre_state, off, cap, k), vm_map_get(&rolled, off, cap, k));
        }
        prop_assert_eq!(slot_size(&pre_state, 0), slot_size(&rolled, 0));

        // Roll it forward again via the redo segment → post-batch content.
        vm.delta_apply_rollforward_segment(&mut rolled, &redo_seg, entry_size);
        for &k in &keys {
            prop_assert_eq!(vm_map_get(&state, off, cap, k), vm_map_get(&rolled, off, cap, k));
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
        let (off, cap) = (slot_offset(&state_a, 0), slot_cap(&state_a, 0));
        for &k in &keys {
            prop_assert_eq!(vm_map_get(&state_a, off, cap, k), vm_map_get(&state_b, off, cap, k));
        }
    }
}

// =============================================================================
// Undo overflow through the container-op hooks path (vm.zig:240-299: EVERY
// append snapshots on first overflow via the aliasing g_undo_state_base —
// including appends from inside hashmap/hashset/bitmap ops)
// =============================================================================

#[test]
fn undo_overflow_inside_container_op_snapshots_and_rolls_back() {
    // HASHMAP(LAST) cap 32768: 70% load = 22937 max entries, so 18000
    // distinct-key inserts overflow the 16384-entry undo log through the
    // hashmap_ops hooks path without ever signaling growth.
    let mut init_code = Vec::new();
    init_code.extend(slot_def(0, 0x00, 0, 0x80));
    let reduce = [0x22, 0, 0, 1]; // BATCH_MAP_UPSERT_LAST slot=0 key=c0 val=c1
    let prog = program(1, 2, &init_code, &reduce);
    let mut state = init(&prog);
    let mut vm = Vm::default();

    vm.undo_enable(&state);
    let cp = vm.undo_checkpoint();
    assert_eq!(0, cp);

    const TOTAL: u32 = 18_000;
    const BATCH: u32 = 1_000;
    let mut batch = 0u32;
    while batch < TOTAL / BATCH {
        let keys: Vec<u32> = (0..BATCH).map(|i| batch * BATCH + i + 1).collect();
        let vals: Vec<u32> = keys.iter().map(|k| k + 7).collect();
        let cols: Vec<&[u8]> = vec![u32s_as_bytes(&keys), u32s_as_bytes(&vals)];
        assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, BATCH));
        batch += 1;
    }
    assert_eq!(TOTAL, slot_size(&state, 0));
    // The log overflowed strictly inside batch_map_upsert (the only journaled
    // op in the program), so the shadow snapshot MUST have been taken there.
    assert!(vm.undo_has_overflow());

    vm.undo_rollback(&mut state, cp);

    // Every key — logged (1..=16384) AND unlogged past the overflow point
    // (16385..=18000, restorable only via the shadow) — must be gone.
    let (off, cap) = (slot_offset(&state, 0), slot_cap(&state, 0));
    assert_eq!(0, slot_size(&state, 0));
    for k in [1u32, 2, 16_384, 16_385, 16_386, 17_500, 18_000] {
        assert_eq!(
            EMPTY_KEY,
            vm_map_get(&state, off, cap, k),
            "key {k} survived rollback"
        );
    }
    vm.undo_commit();
}

/// Parity regression (found by reducer-vm-integration.test.ts "handles empty
/// batch"): the TS side ships an EMPTY column-pointer array for an empty
/// batch while the program still references columns — Zig fetches the garbage
/// pointer and never dereferences it (batch_len == 0); checked `cols[idx]`
/// indexing panicked instead. `col_at` resolves out-of-range to the empty
/// column. Program bytes are the exact TS-compiled `count()` reducer capture
/// (exact capture incl. 32-byte hash prefix; header "AXE1" + SLOT_DEF AGGREGATE/COUNT +
/// top-level 0xE0 FOR_EACH wrapping AGG_COUNT).
#[test]
fn empty_batch_with_no_column_pointers_is_ok() {
    let hex = "00000000000000000000000000000000000000000000000000000000000000004158453101000100000005000c001000020200e00101010000000200410000";
    let prog: Vec<u8> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect();
    let mut vm = Vm::default();
    let size = calculate_state_size(&prog);
    assert!(size > 0, "state size must be > 0");
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, &prog).expect("init_state");

    // Plain and delta paths, zero columns: both must no-op cleanly.
    let cols: Vec<&[u8]> = Vec::new();
    assert_eq!(OK, vm.execute_batch(&mut state, &prog, &cols, 0));
    vm.undo_enable(&state);
    let cp = vm.undo_checkpoint();
    assert_eq!(OK, vm.execute_batch_delta(&mut state, &prog, &cols, 0));
    vm.undo_rollback(&mut state, cp);
    vm.undo_commit();
}

/// Parity regression (reducer-vm-integration.test.ts "Scalar Slot — latest()"):
/// exec_scalar_latest matched AggType 5/6/7, but the registry says SCALAR_U32=8,
/// SCALAR_F64=9, SCALAR_I64=10 (types.zig:214-216; 6-7 reserved) — every scalar
/// op was a silent no-op and NO Rust test covered 0x48 at all. Program bytes are
/// the exact TS-compiled `latest('score')` capture (hash prefix included):
/// init `05 09 00` = scalar slot AggType SCALAR_F64, reduce = FOR_EACH(type_col=1,
/// id=1) around `48 00 03 02` (slot 0, val_col 3, cmp_col 2).
#[test]
fn ts_scalar_latest_program_writes_value_and_cmp() {
    let hex = "00000000000000000000000000000000000000000000000000000000000000004158453101000100000005000e001000050900e001010100000004004800030200";
    let prog: Vec<u8> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect();
    let mut vm = Vm::default();
    let size = calculate_state_size(&prog);
    assert!(size > 0, "state size must be > 0");
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, &prog).expect("init_state");

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
