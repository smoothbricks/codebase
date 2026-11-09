//! The changed-key feed: every key-scoped kernel write records exactly its
//! key and prior bindable value while the feed is armed and its source is
//! tracked and not overflowed; a write no key names, a full buffer, a fact
//! without a chain, an unseen pre-pass write or an unkeyable slot kind
//! overflows the whole source instead; a rolled-back attempt leaves nothing
//! behind; and a bound feed never allocates.

mod common;

use bitmosaic::Bitmosaic;
use columine_types::PROGRAM_MAGIC;
use columine_types::operand::encode_index;
use columine_types::types::{
    AggType, ErrorCode, Opcode, SlotMetaOffset, SlotType, StateHeaderOffset, StructFieldType,
    state_num_slots,
};
use columine_vm::bitmap_ops::{BitmapAlgebraOp, BitmapSource};
use columine_vm::change_feed::{Change, Source};
use columine_vm::hashmap_ops::{CmpType, Strategy};
use columine_vm::key_index::Key;
use columine_vm::meta::slot_meta_base;
use columine_vm::row_exprs::StoredRow;
use columine_vm::state_init::{
    DEFAULT_ACCEPTED_PROGRAM_MAGICS, GrowthRoom, calculate_grown_state_size, calculate_state_size,
    init_state,
};
use columine_vm::vm::{Vm, f64s_as_bytes, i64s_as_bytes, u32s_as_bytes};
use common::memory::with_allocations;

const OK: u32 = ErrorCode::Ok as u32;

const KEY: u32 = 5;
/// A key no image holds.
const ABSENT: u32 = 900;
const GUARD_KEY: u32 = 77;
const ADD: u32 = 0xc0_0001;

/// Batch columns.
const TYPE: u8 = 0;
const OFFSETS: u8 = 1;
const REMOVE_K1: u8 = 2;
const REMOVE_K2: u8 = 3;
const ADD_K1: u8 = 4;
const ADD_K2: u8 = 5;
const VALUE: u8 = 6;
const VALUE_I64: u8 = 7;
const ZERO_I64: u8 = 8;
const ZERO: u8 = 9;
const INIT_OFFSETS: u8 = 10;
const INIT_VALUES: u8 = 11;
const PREDICATE: u8 = 12;
const TICK: u8 = 13;
const TICK_F64: u8 = 14;
const ONES: u8 = 15;
const PROBE_ID: u8 = 16;
const GUARD: u8 = 17;
const VALUE2: u8 = 18;
const COLUMNS: usize = 19;

/// A fresh counts frame: prefix `P`, a zero count, the ref word naming the
/// histogram region and that region: one bin, zero.
const COUNTS_INIT: [u8; 15] = [b'P', 0, 0, 0, 0, 8, 0, 0, 0, 1, 0, 0, 0, 0, 0];

/// One log row: the keys its removals and its producers name, and its two
/// value cells.
#[derive(Clone, Copy)]
struct Row {
    remove: (u32, u32),
    add: (u32, u32),
    value: u32,
    value2: u32,
}

/// A row that writes `(key, key2)` and removes nothing.
const fn add(key: u32, key2: u32, value: u32, value2: u32) -> Row {
    Row {
        remove: (ABSENT, 0),
        add: (key, key2),
        value,
        value2,
    }
}

struct Batch {
    cols: Vec<Vec<u8>>,
    rows: u32,
}

impl Batch {
    fn slices(&self) -> Vec<&[u8]> {
        self.cols.iter().map(Vec::as_slice).collect()
    }
}

fn batch(rows: &[Row]) -> Batch {
    let lane = |cell: &dyn Fn(usize, &Row) -> u32| -> Vec<u8> {
        let cells: Vec<u32> = rows
            .iter()
            .enumerate()
            .map(|(index, row)| cell(index, row))
            .collect();
        u32s_as_bytes(&cells).to_vec()
    };
    let units = rows.len();
    let wide: Vec<i64> = rows.iter().map(|row| i64::from(row.value)).collect();
    let ticks: Vec<f64> = (1..=units).map(|tick| tick as f64).collect();
    let init_offsets: Vec<u32> = (0..=units)
        .map(|unit| u32::try_from(unit * COUNTS_INIT.len()).unwrap())
        .collect();
    let offsets: Vec<u32> = (0..=u32::try_from(units).unwrap()).collect();
    let mut cols = vec![Vec::new(); COLUMNS];
    cols[usize::from(TYPE)] = lane(&|_, _| ADD);
    cols[usize::from(OFFSETS)] = u32s_as_bytes(&offsets).to_vec();
    cols[usize::from(REMOVE_K1)] = lane(&|_, row| row.remove.0);
    cols[usize::from(REMOVE_K2)] = lane(&|_, row| row.remove.1);
    cols[usize::from(ADD_K1)] = lane(&|_, row| row.add.0);
    cols[usize::from(ADD_K2)] = lane(&|_, row| row.add.1);
    cols[usize::from(VALUE)] = lane(&|_, row| row.value);
    cols[usize::from(VALUE_I64)] = i64s_as_bytes(&wide).to_vec();
    cols[usize::from(ZERO_I64)] = i64s_as_bytes(&vec![0; units]).to_vec();
    cols[usize::from(ZERO)] = lane(&|_, _| 0);
    cols[usize::from(INIT_OFFSETS)] = u32s_as_bytes(&init_offsets).to_vec();
    cols[usize::from(INIT_VALUES)] = COUNTS_INIT.repeat(units);
    cols[usize::from(PREDICATE)] = lane(&|_, _| 1);
    cols[usize::from(TICK)] = lane(&|index, _| u32::try_from(index + 1).unwrap());
    cols[usize::from(TICK_F64)] = f64s_as_bytes(&ticks).to_vec();
    cols[usize::from(ONES)] = lane(&|_, _| 1);
    cols[usize::from(PROBE_ID)] = lane(&|_, row| row.add.0 * 1_000 + row.value);
    cols[usize::from(GUARD)] = lane(&|_, _| GUARD_KEY);
    cols[usize::from(VALUE2)] = lane(&|_, row| row.value2);
    Batch {
        cols,
        rows: u32::try_from(units).unwrap(),
    }
}

fn program(slots: u32, init: &[u8], reduce: &[u8]) -> Vec<u8> {
    let mut init = init.to_vec();
    init.push(Opcode::Halt as u8);
    let mut out = vec![0u8; 32];
    out.extend(PROGRAM_MAGIC.to_le_bytes());
    out.extend([2, 0, 0, 0]); // version 2, no callbacks, no flags
    out.extend(slots.to_le_bytes());
    out.extend(u32::try_from(COLUMNS).unwrap().to_le_bytes());
    out.extend(u32::try_from(init.len()).unwrap().to_le_bytes());
    out.extend(u32::try_from(reduce.len()).unwrap().to_le_bytes());
    out.extend_from_slice(&init);
    out.extend_from_slice(reduce);
    out
}

fn image(program: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(program, DEFAULT_ACCEPTED_PROGRAM_MAGICS)
        .expect("valid program admission");
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, program, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("init");
    state
}

fn run(vm: &mut Vm, state: &mut [u8], program: &[u8], batch: &Batch) -> u32 {
    vm.execute_batch(state, program, &batch.slices(), batch.rows)
}

fn slot_def(slot: u8, kind: SlotType) -> Vec<u8> {
    vec![Opcode::SlotDef as u8, slot, kind as u8, 16, 0]
}

fn struct_map(slot: u8, hint: u8, fields: &[StructFieldType]) -> Vec<u8> {
    let mut def = vec![
        Opcode::SlotStructMap as u8,
        slot,
        SlotType::StructMap as u8,
        hint,
        0,
        u8::try_from(fields.len()).unwrap(),
    ];
    def.extend(fields.iter().map(|&field| field as u8));
    def
}

fn struct_map2(slot: u8, fields: &[StructFieldType]) -> Vec<u8> {
    let mut def = struct_map(slot, 16, fields);
    def[0] = Opcode::SlotStructMap2 as u8;
    def[2] = SlotType::StructMap2 as u8;
    def
}

fn for_each(body: &[u8]) -> Vec<u8> {
    let mut code = vec![Opcode::ForEach as u8, TYPE, 1];
    code.extend(ADD.to_le_bytes());
    code.extend(u16::try_from(body.len()).unwrap().to_le_bytes());
    code.extend_from_slice(body);
    code
}

/// `[BATCH_STRUCT_MAP_UPSERT_LAST slot key_col 1 VALUE→0, no arrays]`.
fn struct_map_upsert(slot: u8, key_col: u8, value_col: u8) -> [u8; 7] {
    [
        Opcode::BatchStructMapUpsertLast as u8,
        slot,
        key_col,
        1,
        value_col,
        0,
        0,
    ]
}

/// A change-plane bitset naming `slots`, and the derived-facts table when
/// `facts`.
fn plane(slots: &[u16], facts: bool) -> Vec<u64> {
    let mut words = vec![0u64; 2];
    words[0] = u64::from(facts);
    for &slot in slots {
        let bit = usize::from(slot) + 1;
        words[bit / 64] |= 1 << (bit % 64);
    }
    words
}

/// Clear every change flag, as a RETE pass does at its end.
fn clear_flags(state: &mut [u8]) {
    for slot in 0..state_num_slots(state) {
        state[(slot_meta_base(slot) + SlotMetaOffset::CHANGE_FLAGS) as usize] = 0;
    }
    state[StateHeaderOffset::DERIVED_FACTS_CHANGE_FLAG as usize] = 0;
}

/// Bind `tracked`, clear the flags and begin a pass.
fn arm(vm: &mut Vm, state: &mut [u8], tracked: &[u64], facts: u16, cap: u32) {
    vm.feed_bind(tracked, facts, cap).expect("bind");
    clear_flags(state);
    vm.feed_begin_pass(state);
}

/// The pass's entries, oldest first.
fn entries(vm: &Vm) -> Vec<(Source, Key, Change)> {
    vm.feed()
        .entries()
        .iter()
        .map(|entry| (entry.source(), entry.key(), entry.change()))
        .collect()
}

const fn slot(slot: u16) -> Source {
    Source::Slot(slot)
}

const fn one(key: u32) -> Key {
    Key::one(key)
}

#[test]
fn hash_map_writes_record_their_key_and_prior_value() {
    let code = program(1, &slot_def(0, SlotType::HashMap), &[]);
    let mut state = image(&code);
    let mut vm = Vm::default();
    arm(&mut vm, &mut state, &plane(&[0], false), 0, 64);
    let mut upsert = |state: &mut Vec<u8>, keys: &[u32], vals: &[u32]| {
        vm.map_upsert(
            Strategy::Last,
            false,
            state,
            0,
            keys,
            vals,
            None,
            CmpType::U32,
        )
    };
    assert_eq!(upsert(&mut state, &[5, 6], &[10, 20]), ErrorCode::Ok);
    assert_eq!(upsert(&mut state, &[5], &[11]), ErrorCode::Ok);
    // An equal rewrite changes nothing a rule binds.
    assert_eq!(upsert(&mut state, &[6], &[20]), ErrorCode::Ok);
    assert_eq!(vm.map_remove(false, &mut state, 0, &[6, 99]), ErrorCode::Ok);
    assert_eq!(
        entries(&vm),
        [
            (slot(0), one(5), Change::Inserted),
            (slot(0), one(6), Change::Inserted),
            (slot(0), one(5), Change::Updated(Some(10))),
            (slot(0), one(6), Change::Removed(Some(20))),
        ]
    );
    let feed = vm.feed();
    assert_eq!(feed.last(slot(0)), Some(3));
    let newest_first: Vec<u32> = feed.chain(slot(0)).map(|entry| entry.key().k1).collect();
    assert_eq!(newest_first, [6, 5, 6, 5]);
    assert_eq!(feed.work().recorded, 4);
}

#[test]
fn set_and_bitmap_writes_record_their_members_and_algebra_overflows_the_slot() {
    let init = [
        slot_def(0, SlotType::HashSet),
        slot_def(1, SlotType::Bitmap),
        slot_def(2, SlotType::Bitmap),
    ]
    .concat();
    let code = program(3, &init, &[]);
    let mut state = image(&code);
    let mut vm = Vm::default();
    assert_eq!(
        vm.set_insert(false, &mut state, 2, &[40, 41], None),
        ErrorCode::Ok
    );
    arm(&mut vm, &mut state, &plane(&[0, 1], false), 0, 64);

    assert_eq!(
        vm.set_insert(false, &mut state, 0, &[3, 4], None),
        ErrorCode::Ok
    );
    // A held member inserts nothing.
    assert_eq!(
        vm.set_insert(false, &mut state, 0, &[3], None),
        ErrorCode::Ok
    );
    assert_eq!(vm.set_remove(false, &mut state, 0, &[4, 8]), ErrorCode::Ok);
    assert_eq!(
        vm.set_insert(false, &mut state, 1, &[7, 9], None),
        ErrorCode::Ok
    );
    // A patch reports how many members it added, not which: every member
    // it was asked for is recorded.
    assert_eq!(
        vm.set_insert(false, &mut state, 1, &[9, 2], None),
        ErrorCode::Ok
    );
    assert_eq!(vm.set_remove(false, &mut state, 1, &[7]), ErrorCode::Ok);
    let keyed = [
        (slot(0), one(3), Change::Inserted),
        (slot(0), one(4), Change::Inserted),
        (slot(0), one(4), Change::Removed(None)),
        (slot(1), one(7), Change::Inserted),
        (slot(1), one(9), Change::Inserted),
        (slot(1), one(2), Change::Inserted),
        (slot(1), one(9), Change::Inserted),
        (slot(1), one(7), Change::Removed(None)),
    ];
    assert_eq!(entries(&vm), keyed);
    assert!(!vm.feed().overflowed(slot(1)));

    assert_eq!(
        vm.bitmap_algebra(
            false,
            BitmapAlgebraOp::Or,
            &mut state,
            1,
            BitmapSource::Slot(2)
        ),
        ErrorCode::Ok
    );
    assert!(vm.feed().overflowed(slot(1)), "algebra names no key");
    assert!(!vm.feed().overflowed(slot(0)));
    // An overflowed slot records nothing more.
    assert_eq!(
        vm.set_insert(false, &mut state, 1, &[50], None),
        ErrorCode::Ok
    );
    assert_eq!(entries(&vm), keyed);
    assert_eq!(vm.feed().overflow()[0], 0b100);
    assert_eq!(vm.feed_work().overflowed, 1);
}

#[test]
fn struct_map_and_two_key_writes_record_their_keys_and_the_two_key_value_lane() {
    use StructFieldType::UInt32;
    let init = [
        struct_map(0, 16, &[UInt32]),
        struct_map2(1, &[UInt32, UInt32]),
    ]
    .concat();
    let upsert = program(
        2,
        &init,
        &[
            struct_map_upsert(0, ADD_K1, VALUE).as_slice(),
            &[
                Opcode::BatchStructMap2UpsertLast as u8,
                1,
                ADD_K1,
                ADD_K2,
                2,
                VALUE,
                0,
                VALUE2,
                1,
            ][..],
        ]
        .concat(),
    );
    // The exact-pair removal reads REMOVE_K1/REMOVE_K2, the prefix removal
    // ADD_K1.
    let remove = program(
        2,
        &init,
        &[
            Opcode::BatchStructMapRemove as u8,
            0,
            REMOVE_K1,
            Opcode::BatchStructMap2Remove as u8,
            1,
            REMOVE_K1,
            REMOVE_K2,
            Opcode::BatchStructMap2RemovePrefix as u8,
            1,
            ADD_K1,
        ],
    );
    let mut state = image(&upsert);
    let mut vm = Vm::default();
    arm(&mut vm, &mut state, &plane(&[0, 1], false), 0, 64);
    let first = batch(&[add(5, 1, 10, 0), add(6, 2, 20, 1)]);
    assert_eq!(OK, run(&mut vm, &mut state, &upsert, &first));
    let second = batch(&[add(5, 1, 11, 3)]);
    assert_eq!(OK, run(&mut vm, &mut state, &upsert, &second));
    let removal = batch(&[Row {
        remove: (5, 1),
        add: (6, 0),
        value: 0,
        value2: 0,
    }]);
    assert_eq!(OK, run(&mut vm, &mut state, &remove, &removal));
    assert_eq!(
        entries(&vm),
        [
            (slot(0), one(5), Change::Inserted),
            (slot(0), one(6), Change::Inserted),
            (slot(1), Key::two(5, 1), Change::Inserted),
            (slot(1), Key::two(6, 2), Change::Inserted),
            (slot(0), one(5), Change::Updated(None)),
            (slot(1), Key::two(5, 1), Change::Updated(Some(10))),
            (slot(0), one(5), Change::Removed(None)),
            (
                slot(1),
                Key::two(5, 1),
                Change::Removed(Some(11 | (3 << 32)))
            ),
            (
                slot(1),
                Key::two(6, 2),
                Change::Removed(Some(20 | (1 << 32)))
            ),
        ]
    );
}

#[test]
fn a_row_the_removal_cut_skips_records_nothing() {
    let init = [
        struct_map(0, 16, &[StructFieldType::UInt32]),
        slot_def(1, SlotType::HashMap),
    ]
    .concat();
    let producers = [
        struct_map_upsert(0, ADD_K1, VALUE).as_slice(),
        &[Opcode::BatchMapUpsertLast as u8, 1, ADD_K1, VALUE][..],
    ]
    .concat();
    let seed = program(2, &init, &producers);
    let code = program(
        2,
        &init,
        &[
            &[
                Opcode::BatchStructMapRemove as u8,
                0,
                REMOVE_K1,
                Opcode::BatchMapRemove as u8,
                1,
                REMOVE_K1,
            ][..],
            producers.as_slice(),
        ]
        .concat(),
    );
    let mut state = image(&code);
    let mut vm = Vm::default();
    assert_eq!(
        OK,
        run(&mut vm, &mut state, &seed, &batch(&[add(KEY, 0, 9, 0)]))
    );
    arm(&mut vm, &mut state, &plane(&[0, 1], false), 0, 64);
    // Row 0 writes KEY, row 1 removes it: the removal erases the seed row at
    // row 1, so row 0's write is cut and lands nowhere.
    let log = [
        add(KEY, 0, 5, 0),
        Row {
            remove: (KEY, 0),
            add: (801, 0),
            value: 1,
            value2: 0,
        },
    ];
    assert_eq!(OK, run(&mut vm, &mut state, &code, &batch(&log)));
    assert_eq!(
        entries(&vm),
        [
            (slot(0), one(KEY), Change::Removed(None)),
            (slot(1), one(KEY), Change::Removed(Some(9))),
            (slot(0), one(801), Change::Inserted),
            (slot(1), one(801), Change::Inserted),
        ]
    );
}

/// A counts fold whose new rows' init frames fill the struct map's arena
/// while its rows are far below the admission bound: the refusal names the
/// arena, and one growth for that room lets the batch through with the rows
/// unchanged. Named as the rows, the growth would double rows the slot never
/// lacked, every replay asking again, without bound.
#[test]
fn a_counts_fold_that_fills_its_arena_asks_for_arena_room() {
    const PREFIX: usize = 200;
    let init = struct_map(0, 16, &[StructFieldType::Vo]);
    let mut reduce = vec![
        Opcode::BatchStructMapCountsFold as u8,
        0,
        ADD_K1,
        0,
        INIT_OFFSETS,
        INIT_VALUES,
        0,
        1,
        4,
        4,
        1,
        ZERO,
        0,
    ];
    reduce.extend_from_slice(encode_index(PREFIX as u32).as_bytes());
    reduce.extend_from_slice(&[b'P'; PREFIX]);
    let code = program(1, &init, &reduce);
    let mut state = image(&code);
    let frame = [&[b'P'; PREFIX][..], &COUNTS_INIT[1..]].concat();
    let one = |key: u32| {
        let mut rows = batch(&[add(key, 0, 1, 0)]);
        rows.cols[usize::from(INIT_OFFSETS)] =
            u32s_as_bytes(&[0, u32::try_from(frame.len()).unwrap()]).to_vec();
        rows.cols[usize::from(INIT_VALUES)] = frame.clone();
        rows
    };
    let capacity = |state: &[u8]| {
        columine_vm::bytes::read_u32(state, slot_meta_base(0) + SlotMetaOffset::CAPACITY)
    };
    let rows = capacity(&state);
    let mut vm = Vm::default();
    let mut key = 1;
    while run(&mut vm, &mut state, &code, &one(key)) == OK {
        key += 1;
    }
    assert!(
        key < rows / 2,
        "fixture: the arena fills at key {key}, far below {rows} rows"
    );
    assert_eq!(
        Some(GrowthRoom::Arena),
        vm.needs_growth_room(),
        "the arena ran out"
    );

    let room = vm.needs_growth_room().unwrap();
    let mut next = vec![0u8; calculate_grown_state_size(&state, 0, room).unwrap() as usize];
    vm.grow_state(&state, &mut next, 0, room).unwrap();
    assert_eq!(rows, capacity(&next), "the rows never ran out");
    assert_eq!(OK, run(&mut vm, &mut next, &code, &one(key)));
}
#[test]
fn a_counts_fold_records_once_though_admission_dry_runs_it() {
    let init = struct_map(0, 16, &[StructFieldType::Vo]);
    let code = program(
        1,
        &init,
        &[
            Opcode::BatchStructMapCountsFold as u8,
            0,
            ADD_K1,
            0,
            INIT_OFFSETS,
            INIT_VALUES,
            0,
            1,
            4,
            4,
            1,
            ZERO,
            0,
            1,
            b'P',
        ],
    );
    let mut state = image(&code);
    let mut vm = Vm::default();
    arm(&mut vm, &mut state, &plane(&[0], false), 0, 64);
    let dry_runs = vm.capacity_counts().dry_run_fit;
    let log = [add(KEY, 0, 1, 0), add(6, 0, 1, 0), add(KEY, 0, 1, 0)];
    assert_eq!(OK, run(&mut vm, &mut state, &code, &batch(&log)));
    assert_eq!(
        vm.capacity_counts().dry_run_fit,
        dry_runs + 1,
        "admission dry-ran the fold"
    );
    assert_eq!(
        entries(&vm),
        [
            (slot(0), one(KEY), Change::Inserted),
            (slot(0), one(6), Change::Inserted),
            (slot(0), one(KEY), Change::Updated(None)),
        ]
    );
}

#[test]
fn a_guarded_upsert_records_its_row_and_the_claim_it_consumes() {
    use StructFieldType::UInt32;
    let init = [struct_map(0, 16, &[UInt32]), struct_map(1, 128, &[UInt32])].concat();
    let claims = program(
        2,
        &init,
        &[
            Opcode::BatchStructMapUpsertLast as u8,
            1,
            ADD_K1,
            1,
            GUARD,
            0,
            0,
        ],
    );
    // Each removal of the destination clears its claim: the pairing every
    // guarded program keeps.
    let code = program(
        2,
        &init,
        &[
            Opcode::BatchStructMapRemove as u8,
            0,
            REMOVE_K1,
            Opcode::BatchStructMapRemove as u8,
            1,
            REMOVE_K1,
            Opcode::BatchStructMapGuardedUpsertLast as u8,
            0,
            1,
            ADD_K1,
            GUARD,
            0,
            1,
            VALUE,
            0,
            0,
        ],
    );
    let mut state = image(&code);
    let mut vm = Vm::default();
    assert_eq!(
        OK,
        run(&mut vm, &mut state, &claims, &batch(&[add(KEY, 0, 0, 0)]))
    );
    arm(&mut vm, &mut state, &plane(&[0, 1], false), 0, 64);
    assert_eq!(
        OK,
        run(&mut vm, &mut state, &code, &batch(&[add(KEY, 0, 1, 0)]))
    );
    assert_eq!(
        entries(&vm),
        [
            (slot(0), one(KEY), Change::Inserted),
            (slot(1), one(KEY), Change::Removed(None)),
        ]
    );
}

#[test]
fn a_conditional_upsert_records_only_the_rows_its_predicate_admits() {
    // 0x8f is a body instruction: FOR_EACH { 0x8f slot:0 ADD_K1 1 (VALUE, 0) no arrays, predicate VALUE2 }.
    let mut upsert = struct_map_upsert(0, ADD_K1, VALUE).to_vec();
    upsert[0] = Opcode::BatchStructMapUpsertLastIf as u8;
    upsert.push(VALUE2);
    let code = program(
        1,
        &struct_map(0, 16, &[StructFieldType::UInt32]),
        &for_each(&upsert),
    );
    let mut state = image(&code);
    let mut vm = Vm::default();
    arm(&mut vm, &mut state, &plane(&[0], false), 0, 64);
    // value2 is each row's predicate: KEY is written and then rewritten, 6 is
    // refused while absent and stays absent, and KEY's last row is refused.
    let log = [
        add(KEY, 0, 1, 1),
        add(6, 0, 2, 0),
        add(KEY, 0, 3, 1),
        add(KEY, 0, 4, 0),
    ];
    assert_eq!(OK, run(&mut vm, &mut state, &code, &batch(&log)));
    assert_eq!(
        entries(&vm),
        [
            (slot(0), one(KEY), Change::Inserted),
            (slot(0), one(KEY), Change::Updated(None)),
        ]
    );
    // What the entries claim is what the slot holds: 6 is absent, and KEY
    // keeps its last admitted value.
    assert!(
        StoredRow::lookup(&state, 0, 6, None)
            .expect("slot 0")
            .is_none(),
        "a refused absent key stays absent"
    );
    let row = StoredRow::lookup(&state, 0, KEY, None)
        .expect("slot 0")
        .expect("KEY was written");
    assert_eq!(
        row.field(0).expect("field 0"),
        Some(3u32.to_le_bytes().as_slice()),
        "a refused rewrite leaves the admitted value"
    );
}

#[test]
fn a_conditional_upsert_records_once_though_admission_dry_runs_it() {
    let mut upsert = struct_map_upsert(0, ADD_K1, VALUE).to_vec();
    upsert[0] = Opcode::BatchStructMapUpsertLastIf as u8;
    upsert.push(VALUE2);
    // A slot declared for four keys: fifty rows pass no bound, so admission
    // dry-runs the batch, whose predicates refuse all but two.
    let code = program(
        1,
        &struct_map(0, 4, &[StructFieldType::UInt32]),
        &for_each(&upsert),
    );
    let mut state = image(&code);
    let mut vm = Vm::default();
    arm(&mut vm, &mut state, &plane(&[0], false), 0, 64);
    let dry_runs = vm.capacity_counts().dry_run_fit;
    let mut log: Vec<Row> = (100..148).map(|key| add(key, 0, 1, 0)).collect();
    log.extend([add(KEY, 0, 2, 1), add(6, 0, 3, 1)]);
    assert_eq!(OK, run(&mut vm, &mut state, &code, &batch(&log)));
    assert_eq!(
        vm.capacity_counts().dry_run_fit,
        dry_runs + 1,
        "admission dry-ran the batch"
    );
    assert_eq!(
        entries(&vm),
        [
            (slot(0), one(KEY), Change::Inserted),
            (slot(0), one(6), Change::Inserted),
        ]
    );
}

#[test]
fn a_probe_upsert_records_its_output_key() {
    use StructFieldType::UInt32;
    let init = [
        struct_map(0, 16, &[UInt32]),
        struct_map(1, 128, &[UInt32; 4]),
    ]
    .concat();
    // Source rows hold (route 0, assert, key, value) under `1000 * key + value`.
    let sources = program(
        2,
        &init,
        &[
            Opcode::BatchStructMapUpsertLast as u8,
            1,
            PROBE_ID,
            4,
            ZERO,
            0,
            ZERO,
            1,
            ADD_K1,
            2,
            VALUE,
            3,
            0,
        ],
    );
    let code = program(
        2,
        &init,
        &for_each(&[
            Opcode::BatchStructMapProbe as u8,
            1,
            PROBE_ID,
            0,
            0,
            1,
            3,
            0,
            ADD_K1,
        ]),
    );
    let mut state = image(&code);
    let mut vm = Vm::default();
    let row = batch(&[add(KEY, 0, 3, 0)]);
    assert_eq!(OK, run(&mut vm, &mut state, &sources, &row));
    arm(&mut vm, &mut state, &plane(&[0], false), 0, 64);
    assert_eq!(OK, run(&mut vm, &mut state, &code, &row));
    assert_eq!(entries(&vm), [(slot(0), one(KEY), Change::Inserted)]);
}

#[test]
fn scatter_writes_record_only_a_changed_card() {
    let init = struct_map(0, 16, &[StructFieldType::UInt32]);
    // Route 0: kind 0, field 0 of slot 0 from VALUE; asserts only.
    let code = program(
        1,
        &init,
        &for_each(&[
            Opcode::BatchStructMapScatter as u8,
            ZERO,
            ZERO,
            ADD_K1,
            1,
            0,
            0,
            0,
            VALUE,
        ]),
    );
    let mut state = image(&code);
    let mut vm = Vm::default();
    arm(&mut vm, &mut state, &plane(&[0], false), 0, 64);
    for value in [1, 1, 2] {
        assert_eq!(
            OK,
            run(&mut vm, &mut state, &code, &batch(&[add(KEY, 0, value, 0)]))
        );
    }
    assert_eq!(
        entries(&vm),
        [
            (slot(0), one(KEY), Change::Inserted),
            (slot(0), one(KEY), Change::Updated(None)),
        ]
    );
}

#[test]
fn a_guarded_scatter_records_its_stamped_row() {
    use StructFieldType::UInt32;
    let init = struct_map(0, 16, &[UInt32, UInt32]);
    // Slot 0 guards each row with the transaction in field 1 (TICK).
    let code = program(
        1,
        &init,
        &for_each(&[
            Opcode::BatchStructMapScatterGuarded as u8,
            ZERO,
            ZERO,
            ADD_K1,
            0,
            1,
            TICK,
            1,
            1,
            0,
            0,
            0,
            VALUE,
        ]),
    );
    let mut state = image(&code);
    let mut vm = Vm::default();
    arm(&mut vm, &mut state, &plane(&[0], false), 0, 64);
    assert_eq!(
        OK,
        run(&mut vm, &mut state, &code, &batch(&[add(KEY, 0, 1, 0)]))
    );
    assert_eq!(entries(&vm), [(slot(0), one(KEY), Change::Inserted)]);
}

#[test]
fn static_expiry_records_only_changed_keys_it_removes() {
    let init = struct_map(0, 16, &[StructFieldType::UInt32]);
    let seed = program(1, &init, &struct_map_upsert(0, ADD_K1, VALUE));
    let mut code = struct_map_upsert(0, ADD_K1, VALUE).to_vec();
    code.extend([Opcode::StructMapSweep as u8, 0, PREDICATE]);
    let mut sweep = program(1, &init, &code);
    sweep.extend(columine_vm::temporal::TEMPORAL_MAGIC.to_le_bytes());
    sweep.extend(32u32.to_le_bytes());
    for value in [u32::from(ZERO_I64), 1, 0, u32::from(PREDICATE), u32::MAX] {
        sweep.extend(value.to_le_bytes());
    }
    sweep.extend([0; 12]);
    let mut state = image(&seed);
    let mut vm = Vm::default();
    assert_eq!(
        OK,
        run(
            &mut vm,
            &mut state,
            &seed,
            &batch(&[add(KEY, 0, 1, 0), add(6, 0, 1, 0)])
        )
    );
    arm(&mut vm, &mut state, &plane(&[0], false), 0, 64);
    assert_eq!(
        OK,
        run(&mut vm, &mut state, &sweep, &batch(&[add(KEY, 0, 2, 0)]))
    );
    assert_eq!(
        entries(&vm),
        [
            (slot(0), one(KEY), Change::Updated(None)),
            (slot(0), one(KEY), Change::Removed(None)),
        ]
    );
    assert!(
        StoredRow::lookup(&state, 0, 6, None)
            .expect("stored key")
            .is_some()
    );
}

#[test]
fn an_eviction_records_the_evicted_key_and_value() {
    let mut init = vec![
        Opcode::SlotDef as u8,
        0,
        SlotType::HashMap as u8 | 0x10,
        16,
        0,
    ];
    init.extend(1.0f32.to_le_bytes());
    init.extend(0.0f32.to_le_bytes());
    init.extend([TICK_F64, 0]);
    let code = program(1, &init, &[]);
    let mut state = image(&code);
    let mut vm = Vm::default();
    let stamps = f64s_as_bytes(&[1.0]).to_vec();
    assert_eq!(
        vm.map_upsert(
            Strategy::Last,
            false,
            &mut state,
            0,
            &[KEY],
            &[10],
            Some(stamps.as_slice()),
            CmpType::F64
        ),
        ErrorCode::Ok
    );
    arm(&mut vm, &mut state, &plane(&[0], false), 0, 64);
    assert_eq!(vm.evict_all_expired(false, &mut state, 1_000.0), Ok(1));
    assert_eq!(
        entries(&vm),
        [(slot(0), one(KEY), Change::Evicted(Some(10)))]
    );
}

#[test]
fn a_bitmap_import_overflows_its_slot_while_armed() {
    let init = [slot_def(0, SlotType::Bitmap), slot_def(1, SlotType::Bitmap)].concat();
    let code = program(2, &init, &[]);
    let mut state = image(&code);
    let mut vm = Vm::default();
    let members = Bitmosaic::from_sorted([2u32, 3, 5]).to_bytes();
    arm(&mut vm, &mut state, &plane(&[0], false), 0, 64);
    assert_eq!(vm.bitmap_import(&mut state, 1, &members), ErrorCode::Ok);
    assert_eq!(
        vm.feed().overflow(),
        [0],
        "an untracked import overflows nothing"
    );
    // A refused import changes nothing and overflows nothing.
    assert_eq!(
        vm.bitmap_import(&mut state, 0, &[]),
        ErrorCode::InvalidState
    );
    assert!(!vm.feed().overflowed(slot(0)));
    assert_eq!(vm.bitmap_import(&mut state, 0, &members), ErrorCode::Ok);
    assert!(vm.feed().entries().is_empty(), "an import names no key");
    assert!(vm.feed().overflowed(slot(0)));
    assert_eq!(vm.feed_work().overflowed, 1);
}

#[test]
fn a_nested_write_overflows_its_slot() {
    let init = [
        Opcode::SlotNested as u8,
        0,
        SlotType::Nested as u8,
        16,
        0,
        SlotType::HashMap as u8,
        16,
        0,
        1,
    ];
    let code = program(
        1,
        &init,
        &for_each(&[Opcode::NestedMapUpsertLast as u8, 0, ADD_K1, ADD_K2, VALUE]),
    );
    let mut state = image(&code);
    let mut vm = Vm::default();
    arm(&mut vm, &mut state, &plane(&[0], false), 0, 64);
    assert!(!vm.feed().overflowed(slot(0)), "a nested slot starts keyed");
    assert_eq!(
        OK,
        run(&mut vm, &mut state, &code, &batch(&[add(KEY, 1, 1, 0)]))
    );
    assert!(entries(&vm).is_empty());
    assert!(vm.feed().overflowed(slot(0)));
}

#[test]
fn untracked_sources_and_an_unarmed_feed_record_nothing() {
    let init = [
        slot_def(0, SlotType::HashMap),
        slot_def(1, SlotType::HashMap),
    ]
    .concat();
    let code = program(2, &init, &[]);
    let mut state = image(&code);
    let mut vm = Vm::default();
    // Unbound: nothing is tracked.
    let upsert = |vm: &mut Vm, state: &mut Vec<u8>, slot: u32, key: u32| {
        vm.map_upsert(
            Strategy::Last,
            false,
            state,
            slot,
            &[key],
            &[1],
            None,
            CmpType::U32,
        )
    };
    assert_eq!(upsert(&mut vm, &mut state, 0, 1), ErrorCode::Ok);
    arm(&mut vm, &mut state, &plane(&[1], false), 0, 64);
    assert_eq!(upsert(&mut vm, &mut state, 0, 2), ErrorCode::Ok);
    assert_eq!(upsert(&mut vm, &mut state, 1, 3), ErrorCode::Ok);
    vm.feed_record_fact(0, 4, Change::Inserted);
    vm.feed_end_pass();
    assert_eq!(upsert(&mut vm, &mut state, 1, 5), ErrorCode::Ok);
    assert_eq!(entries(&vm), [(slot(1), one(3), Change::Inserted)]);
    assert_eq!(vm.feed().overflow(), [0], "nothing untracked overflows");
}

#[test]
fn a_full_buffer_overflows_the_source_and_counts_the_drop() {
    let init = [
        slot_def(0, SlotType::HashMap),
        slot_def(1, SlotType::HashMap),
    ]
    .concat();
    let code = program(2, &init, &[]);
    let mut state = image(&code);
    let mut vm = Vm::default();
    arm(&mut vm, &mut state, &plane(&[0, 1], false), 0, 2);
    let upsert = |vm: &mut Vm, state: &mut Vec<u8>, slot: u32, keys: &[u32]| {
        let vals = vec![1; keys.len()];
        vm.map_upsert(
            Strategy::Last,
            false,
            state,
            slot,
            keys,
            &vals,
            None,
            CmpType::U32,
        )
    };
    assert_eq!(upsert(&mut vm, &mut state, 0, &[1, 2, 3, 4]), ErrorCode::Ok);
    assert_eq!(
        entries(&vm),
        [
            (slot(0), one(1), Change::Inserted),
            (slot(0), one(2), Change::Inserted),
        ],
        "nothing is overwritten"
    );
    assert!(vm.feed().overflowed(slot(0)));
    assert_eq!(upsert(&mut vm, &mut state, 1, &[1]), ErrorCode::Ok);
    assert!(vm.feed().overflowed(slot(1)));
    assert_eq!(vm.feed().overflow()[0], 0b110);
    let work = vm.feed_work();
    assert_eq!((work.recorded, work.dropped, work.overflowed), (2, 2, 2));
    assert_eq!(vm.feed().chain(slot(0)).count(), 2);
    assert_eq!(vm.feed().chain(slot(1)).count(), 0);
}

#[test]
fn begin_pass_overflows_what_changed_unseen_and_unkeyable_slot_kinds() {
    let init = [
        slot_def(0, SlotType::HashMap),
        slot_def(1, SlotType::HashMap),
        vec![
            Opcode::SlotDef as u8,
            2,
            SlotType::Aggregate as u8,
            AggType::Count as u8,
            0,
        ],
    ]
    .concat();
    let code = program(3, &init, &[]);
    let mut state = image(&code);
    let mut vm = Vm::default();
    arm(&mut vm, &mut state, &plane(&[0, 1, 2], true), 0, 64);
    // An aggregate names no keys: tracked, it is overflowed from the start.
    assert_eq!(vm.feed().overflow()[0], 0b1000);
    vm.feed_end_pass();
    clear_flags(&mut state);

    // Writes between passes reach no feed but still set their flags.
    assert_eq!(
        vm.map_upsert(
            Strategy::Last,
            false,
            &mut state,
            0,
            &[1],
            &[1],
            None,
            CmpType::U32
        ),
        ErrorCode::Ok
    );
    state[StateHeaderOffset::DERIVED_FACTS_CHANGE_FLAG as usize] = 1;
    vm.feed_begin_pass(&state);
    assert!(vm.feed().entries().is_empty());
    assert!(vm.feed().overflowed(slot(0)));
    assert!(vm.feed().overflowed(Source::Fact(0)));
    assert!(!vm.feed().overflowed(slot(1)));
    assert_eq!(vm.feed().overflow()[0], 0b1011);
    assert_eq!(vm.feed_work().overflowed, 4);
}

#[test]
fn fact_writes_record_on_their_chains_and_an_unchained_fact_overflows_the_table() {
    let code = program(1, &slot_def(0, SlotType::HashMap), &[]);
    let mut state = image(&code);
    let mut vm = Vm::default();
    arm(&mut vm, &mut state, &plane(&[0], true), 2, 64);
    vm.feed_record_fact(1, 40, Change::Inserted);
    vm.feed_record_fact(0, 41, Change::Updated(Some(7)));
    assert_eq!(
        entries(&vm),
        [
            (Source::Fact(1), one(40), Change::Inserted),
            (Source::Fact(0), one(41), Change::Updated(Some(7))),
        ]
    );
    assert_eq!(vm.feed().chain(Source::Fact(1)).count(), 1);
    assert_eq!(vm.feed().last(Source::Fact(0)), Some(1));
    assert!(!vm.feed().overflowed(Source::Fact(0)));
    // A fact index past the bound chains has no chain: the derived-facts
    // table overflows, counted, and records nothing more.
    vm.feed_record_fact(2, 42, Change::Removed(None));
    assert!(vm.feed().overflowed(Source::Fact(0)));
    assert_eq!(vm.feed_work().overflowed, 1);
    vm.feed_record_fact(1, 43, Change::Inserted);
    assert_eq!(vm.feed().entries().len(), 2);
    assert!(!vm.feed().overflowed(slot(0)));
}

#[test]
fn a_rolled_back_attempt_leaves_no_entries_and_its_retry_records_the_same() {
    let init = [
        slot_def(0, SlotType::HashMap),
        struct_map(1, 16, &[StructFieldType::UInt32]),
    ]
    .concat();
    let code = program(
        2,
        &init,
        &[
            &[Opcode::BatchMapUpsertLast as u8, 0, ADD_K1, VALUE][..],
            struct_map_upsert(1, ADD_K1, VALUE).as_slice(),
        ]
        .concat(),
    );
    let mut state = image(&code);
    let mut vm = Vm::default();
    let log = batch(&[add(KEY, 0, 1, 0), add(6, 0, 2, 0)]);
    vm.feed_bind(&plane(&[0, 1], false), 0, 64).expect("bind");
    clear_flags(&mut state);
    let before = state.clone();

    vm.undo_enable(&state).expect("valid inactive window");
    let checkpoint = vm.undo_checkpoint();
    vm.feed_begin_pass(&state);
    assert_eq!(OK, run(&mut vm, &mut state, &code, &log));
    let attempt = entries(&vm);
    assert_eq!(attempt.len(), 4);
    vm.feed_end_pass();
    vm.undo_rollback(&mut state, checkpoint)
        .expect("valid checkpoint");
    vm.undo_commit();
    assert!(state == before, "the rollback restored the flags");

    vm.feed_begin_pass(&state);
    assert!(vm.feed().entries().is_empty());
    assert_eq!(vm.feed().overflow()[0], 0);
    assert_eq!(OK, run(&mut vm, &mut state, &code, &log));
    assert_eq!(entries(&vm), attempt);
}

#[test]
fn a_bound_feed_records_warm_passes_without_allocating() {
    use StructFieldType::UInt32;
    let init = [
        slot_def(0, SlotType::HashMap),
        struct_map(1, 16, &[UInt32]),
        struct_map2(2, &[UInt32, UInt32]),
        slot_def(3, SlotType::HashSet),
    ]
    .concat();
    let code = program(
        4,
        &init,
        &[
            &[Opcode::BatchMapUpsertLast as u8, 0, ADD_K1, VALUE][..],
            struct_map_upsert(1, ADD_K1, VALUE).as_slice(),
            &[
                Opcode::BatchStructMap2UpsertLast as u8,
                2,
                ADD_K1,
                ADD_K2,
                2,
                VALUE,
                0,
                VALUE2,
                1,
            ][..],
            &[Opcode::BatchSetInsert as u8, 3, ADD_K1][..],
        ]
        .concat(),
    );
    let mut state = image(&code);
    let mut vm = Vm::default();
    vm.feed_bind(&plane(&[0, 1, 2, 3], false), 0, 64)
        .expect("bind");
    // Two batches whose values differ, so every map row changes each pass.
    let passes: [Batch; 2] = [1, 2].map(|value| {
        let rows: Vec<Row> = (0..8).map(|key| add(10 + key, key, value, 0)).collect();
        batch(&rows)
    });
    let cols: Vec<Vec<&[u8]>> = passes.iter().map(Batch::slices).collect();
    let pass = |vm: &mut Vm, state: &mut Vec<u8>, index: usize| -> (u32, usize) {
        clear_flags(state);
        vm.feed_begin_pass(state);
        let result = vm.execute_batch(state, &code, &cols[index % 2], 8);
        vm.feed_end_pass();
        (result, vm.feed().entries().len())
    };
    // Warm-up: the first pass inserts every key in all four slots.
    assert_eq!(pass(&mut vm, &mut state, 0), (OK, 32));
    assert_eq!(pass(&mut vm, &mut state, 1), (OK, 24));
    const WARM: usize = 32;
    let mut seen = [(0u32, 0usize); WARM];
    with_allocations(0, || {
        for (index, outcome) in seen.iter_mut().enumerate() {
            *outcome = pass(&mut vm, &mut state, index);
        }
    });
    // A held set member inserts nothing; every map, struct-map and two-key
    // row is rewritten with a new value.
    assert!(seen.iter().all(|&outcome| outcome == (OK, 24)), "{seen:?}");
    let work = vm.feed_work();
    assert_eq!(work.recorded, 32 + 24 + 24 * WARM as u64);
    assert_eq!((work.dropped, work.overflowed), (0, 0));
}
