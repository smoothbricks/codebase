//! Log-order removal (`specs/columine/05-log-order-removal.md`): a removal
//! erases its key at its own row inside the target's fold, so one log leaves
//! the same slot whether it arrives as one batch or as one batch per row.
//!
//! Every removal kind (one-key, exact pair, key1 prefix, hash map, conditional
//! hash map) runs with every producer pick it serves, in four program shapes:
//! top-level instructions; one `FOR_EACH` body that removes, then produces,
//! row by row; two `FOR_EACH`es gated by event type; and the same under
//! `FLAT_MAP`, whose elements take their parent row's position. Each log
//! folds once merged and once split, and the two images must be identical
//! byte for byte once sealed, with the per-batch change flags left out.

mod common;

use columine_types::PROGRAM_MAGIC;
use columine_types::operand::encode_index;
use columine_types::types::{
    EMPTY_KEY, ErrorCode, NO_PARENT_TS_COL, Opcode, SlotMetaOffset, SlotType, StructFieldType,
    TOMBSTONE,
};
use columine_vm::hashmap_ops::CmpType;
use columine_vm::key_index::NoStrings;
use columine_vm::meta::slot_meta_base;
use columine_vm::state_init::{DEFAULT_ACCEPTED_PROGRAM_MAGICS, calculate_state_size, init_state};
use columine_vm::struct_map::{StructMap2Slot, StructMapSlot};
use columine_vm::vm::{CheckedMap, Vm, i64s_as_bytes, u32s_as_bytes};
use common::memory::with_allocations;

const OK: u32 = ErrorCode::Ok as u32;
const SLOT: u8 = 0;
/// The key every scenario folds and removes; pair targets pair it with
/// `KEY2`.
const KEY: u32 = 5;
const KEY2: u32 = 7;
/// What an earlier batch left under `KEY` before each scenario.
const SEED: u32 = 9;

const ADD: u32 = 0xc0_0001;
const REMOVE: u32 = 0xc0_0002;
const BOTH: u32 = 0xc0_0003;

// Columns. TYPE and OFFSETS hold one cell per batch row; the rest one per
// unit: a row, or a FLAT_MAP element.
const TYPE: u8 = 0;
const OFFSETS: u8 = 1;
const REMOVE_K1: u8 = 2;
const REMOVE_K2: u8 = 3;
const ADD_K1: u8 = 4;
const ADD_K2: u8 = 5;
const VALUE: u8 = 6;
const VALUE_I64: u8 = 7;
const ZERO_I64: u8 = 8;
const BUCKET: u8 = 9;
const INIT_OFFSETS: u8 = 10;
const INIT_VALUES: u8 = 11;
const PREDICATE: u8 = 12;
const COLUMNS: usize = 13;

/// A fresh counts frame: prefix `P`, a zero count, the ref word naming the
/// histogram region (origin-relative 8), and that region: one bin, zero.
const COUNTS_INIT: [u8; 15] = [b'P', 0, 0, 0, 0, 8, 0, 0, 0, 1, 0, 0, 0, 0, 0];

/// The removal opcode under test.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Target {
    /// `BATCH_STRUCT_MAP_REMOVE` (0x8c).
    Unary,
    /// `BATCH_STRUCT_MAP2_REMOVE` (0x86).
    Pair,
    /// `BATCH_STRUCT_MAP2_REMOVE_PREFIX` (0x8e).
    Prefix,
    /// `BATCH_MAP_REMOVE` (0x23).
    Map,
    /// `BATCH_MAP_REMOVE_IF` (0x2b), element bodies only.
    MapIf,
}

/// The producer folding the target slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Pick {
    Last,
    First,
    Max,
    Counts,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    /// The removal, then the producer, as top-level instructions.
    Top,
    /// One `FOR_EACH` body holding the removal, then the producer.
    Body,
    /// A `FOR_EACH` of the removing types, then one of the adding types.
    Gated,
    /// `Gated` with each arm under `FLAT_MAP`, two elements per row.
    FlatMap,
}

const COMBOS: [(Target, Pick); 13] = [
    (Target::Unary, Pick::Last),
    (Target::Unary, Pick::First),
    (Target::Unary, Pick::Max),
    (Target::Unary, Pick::Counts),
    (Target::Pair, Pick::Last),
    (Target::Pair, Pick::Max),
    (Target::Prefix, Pick::Last),
    (Target::Prefix, Pick::Max),
    (Target::Map, Pick::Last),
    (Target::Map, Pick::First),
    (Target::Map, Pick::Max),
    (Target::MapIf, Pick::Last),
    (Target::MapIf, Pick::Max),
];

const SHAPES: [Shape; 4] = [Shape::Top, Shape::Body, Shape::Gated, Shape::FlatMap];

/// What one log row does to its key.
#[derive(Clone, Copy, Debug)]
enum Op {
    Add(u32),
    Remove,
    /// A row both arms read: it writes its value and removes its key.
    Both(u32),
    /// A conditional removal row whose predicate fails.
    Skip,
}

/// What a scenario leaves under `KEY`.
#[derive(Clone, Copy, Debug)]
enum Expect {
    Absent,
    /// A row first folded after the last removal: `value`, or a count of 1.
    Fresh(u32),
    /// The seed row, untouched.
    Seeded,
    /// Exactly this field value.
    Value(u32),
}

struct Scenario {
    name: &'static str,
    log: &'static [(Op, u32)],
    expect: Expect,
    applies: fn(Target, Pick) -> bool,
}

fn every(_: Target, _: Pick) -> bool {
    true
}

/// The one-key removals: a sentinel key there removes nothing, while the
/// two-key removals refuse it.
fn one_key(target: Target, _: Pick) -> bool {
    matches!(target, Target::Unary | Target::Map | Target::MapIf)
}

fn conditional_last(target: Target, pick: Pick) -> bool {
    target == Target::MapIf && pick == Pick::Last
}

const SCENARIOS: [Scenario; 7] = [
    Scenario {
        name: "a removal before an add leaves the add",
        log: &[(Op::Remove, KEY), (Op::Add(1), KEY)],
        expect: Expect::Fresh(1),
        applies: every,
    },
    Scenario {
        name: "an add before a removal is erased",
        log: &[(Op::Add(1), KEY), (Op::Remove, KEY)],
        expect: Expect::Absent,
        applies: every,
    },
    Scenario {
        name: "rows after the removal fold into a fresh row",
        log: &[(Op::Add(5), KEY), (Op::Remove, KEY), (Op::Add(3), KEY)],
        expect: Expect::Fresh(3),
        applies: every,
    },
    Scenario {
        name: "a row that writes and removes one key composes write-then-erase",
        log: &[(Op::Both(1), KEY)],
        expect: Expect::Absent,
        applies: every,
    },
    Scenario {
        name: "a later add folds after a row that wrote and removed its key",
        log: &[(Op::Both(4), KEY), (Op::Add(2), KEY)],
        expect: Expect::Fresh(2),
        applies: every,
    },
    Scenario {
        name: "a sentinel key removes nothing",
        log: &[(Op::Remove, EMPTY_KEY), (Op::Remove, TOMBSTONE)],
        expect: Expect::Seeded,
        applies: one_key,
    },
    Scenario {
        name: "a conditional removal whose predicate fails cuts nothing",
        log: &[(Op::Add(1), KEY), (Op::Skip, KEY)],
        expect: Expect::Value(1),
        applies: conditional_last,
    },
];

fn shapes(target: Target) -> impl Iterator<Item = Shape> {
    // The conditional removal is an element-body opcode.
    SHAPES
        .into_iter()
        .filter(move |&shape| target != Target::MapIf || shape != Shape::Top)
}

fn removal(target: Target) -> Vec<u8> {
    match target {
        Target::Unary => vec![Opcode::BatchStructMapRemove as u8, SLOT, REMOVE_K1],
        Target::Pair => vec![
            Opcode::BatchStructMap2Remove as u8,
            SLOT,
            REMOVE_K1,
            REMOVE_K2,
        ],
        Target::Prefix => vec![Opcode::BatchStructMap2RemovePrefix as u8, SLOT, REMOVE_K1],
        Target::Map => vec![Opcode::BatchMapRemove as u8, SLOT, REMOVE_K1],
        Target::MapIf => vec![Opcode::BatchMapRemoveIf as u8, SLOT, REMOVE_K1, PREDICATE],
    }
}

fn producer(target: Target, pick: Pick) -> Vec<u8> {
    match (target, pick) {
        // slot, key, one (column, field) pair, no arrays [, pick field].
        (Target::Unary, Pick::Last) => vec![
            Opcode::BatchStructMapUpsertLast as u8,
            SLOT,
            ADD_K1,
            1,
            VALUE,
            0,
            0,
        ],
        (Target::Unary, Pick::First) => vec![
            Opcode::BatchStructMapUpsertFirst as u8,
            SLOT,
            ADD_K1,
            1,
            VALUE,
            0,
            0,
        ],
        (Target::Unary, Pick::Max) => vec![
            Opcode::BatchStructMapUpsertMax as u8,
            SLOT,
            ADD_K1,
            1,
            VALUE,
            0,
            0,
            0,
        ],
        // slot, key, VO field 0, init frames, count at 0, one histogram
        // (ref at 4, elements at 4, one bin, bucket column), no masks,
        // prefix `P`.
        (Target::Unary, Pick::Counts) => vec![
            Opcode::BatchStructMapCountsFold as u8,
            SLOT,
            ADD_K1,
            0,
            INIT_OFFSETS,
            INIT_VALUES,
            0,
            1,
            4,
            4,
            1,
            BUCKET,
            0,
            1,
            b'P',
        ],
        (Target::Pair | Target::Prefix, Pick::Last) => vec![
            Opcode::BatchStructMap2UpsertLast as u8,
            SLOT,
            ADD_K1,
            ADD_K2,
            1,
            VALUE,
            0,
        ],
        // The greatest (value, 0), stored in fields 1 and 2.
        (Target::Pair | Target::Prefix, Pick::Max) => vec![
            Opcode::BatchStructMap2UpsertMaxI64x2 as u8,
            SLOT,
            ADD_K1,
            ADD_K2,
            1,
            VALUE,
            0,
            VALUE_I64,
            1,
            ZERO_I64,
            2,
        ],
        (Target::Map | Target::MapIf, Pick::Last) => {
            vec![Opcode::BatchMapUpsertLast as u8, SLOT, ADD_K1, VALUE]
        }
        (Target::Map | Target::MapIf, Pick::First) => {
            vec![Opcode::BatchMapUpsertFirst as u8, SLOT, ADD_K1, VALUE]
        }
        (Target::Map | Target::MapIf, Pick::Max) => vec![
            Opcode::BatchMapUpsertMax as u8,
            SLOT,
            ADD_K1,
            VALUE,
            VALUE,
            CmpType::U32 as u8,
        ],
        _ => unreachable!("no {pick:?} producer folds a {target:?} target"),
    }
}

fn init(target: Target, pick: Pick) -> Vec<u8> {
    let field = if pick == Pick::Counts {
        StructFieldType::Vo
    } else {
        StructFieldType::UInt32
    };
    let mut init = match (target, pick) {
        (Target::Unary, _) => vec![
            Opcode::SlotStructMap as u8,
            SLOT,
            SlotType::StructMap as u8,
            16,
            0,
            1,
            field as u8,
        ],
        (Target::Pair | Target::Prefix, Pick::Max) => vec![
            Opcode::SlotStructMap2 as u8,
            SLOT,
            SlotType::StructMap2 as u8,
            16,
            0,
            3,
            StructFieldType::UInt32 as u8,
            StructFieldType::Int64 as u8,
            StructFieldType::Int64 as u8,
        ],
        (Target::Pair | Target::Prefix, _) => vec![
            Opcode::SlotStructMap2 as u8,
            SLOT,
            SlotType::StructMap2 as u8,
            16,
            0,
            1,
            StructFieldType::UInt32 as u8,
        ],
        (Target::Map | Target::MapIf, _) => {
            vec![Opcode::SlotDef as u8, SLOT, SlotType::HashMap as u8, 16, 0]
        }
    };
    init.push(Opcode::Halt as u8);
    init
}

fn for_each(types: &[u32], body: &[u8]) -> Vec<u8> {
    let mut code = vec![
        Opcode::ForEach as u8,
        TYPE,
        u8::try_from(types.len()).unwrap(),
    ];
    for id in types {
        code.extend(id.to_le_bytes());
    }
    code.extend(u16::try_from(body.len()).unwrap().to_le_bytes());
    code.extend_from_slice(body);
    code
}

fn flat_map(body: &[u8]) -> Vec<u8> {
    let mut code = vec![Opcode::FlatMap as u8, OFFSETS];
    code.extend_from_slice(encode_index(NO_PARENT_TS_COL).as_bytes());
    code.extend(u16::try_from(body.len()).unwrap().to_le_bytes());
    code.extend_from_slice(body);
    code
}

fn reduce(target: Target, pick: Pick, shape: Shape) -> Vec<u8> {
    let (remove, produce) = (removal(target), producer(target, pick));
    match shape {
        Shape::Top => [remove, produce].concat(),
        Shape::Body => for_each(&[ADD, REMOVE, BOTH], &[remove, produce].concat()),
        Shape::Gated => [
            for_each(&[REMOVE, BOTH], &remove),
            for_each(&[ADD, BOTH], &produce),
        ]
        .concat(),
        Shape::FlatMap => [
            for_each(&[REMOVE, BOTH], &flat_map(&remove)),
            for_each(&[ADD, BOTH], &flat_map(&produce)),
        ]
        .concat(),
    }
}

fn program(init: &[u8], slots: u32, reduce: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; 32];
    out.extend(PROGRAM_MAGIC.to_le_bytes());
    out.extend([2, 0, 0, 0]); // version 2, no callbacks, no flags
    out.extend(slots.to_le_bytes());
    out.extend(u32::try_from(COLUMNS).unwrap().to_le_bytes());
    out.extend(u32::try_from(init.len()).unwrap().to_le_bytes());
    out.extend(u32::try_from(reduce.len()).unwrap().to_le_bytes());
    out.extend_from_slice(init);
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

/// One batch's columns.
struct Batch {
    cols: Vec<Vec<u8>>,
    rows: u32,
}

/// The batch carrying log rows `log`, the first of which is log row `first`.
/// Top-level and shared-body programs run both arms over every row, so a
/// row that does not remove names a removal key no row adds (`900 + row`),
/// and one that does not add a key no row removes (`800 + row`). Under
/// `FLAT_MAP` each row leads with a pad element of its own key
/// (`700 + row`), so element indexes never equal row indexes.
fn batch(shape: Shape, log: &[(Op, u32)], first: u32) -> Batch {
    let mut types = Vec::new();
    let mut offsets = vec![0u32];
    let (mut remove_k1, mut remove_k2, mut add_k1, mut add_k2) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let (mut values, mut predicates) = (Vec::<u32>::new(), Vec::new());
    for (row, &(op, key)) in (first..).zip(log) {
        // Whether the row names its key to the removal and to the producer,
        // and whether a conditional removal's predicate passes.
        let (event, value, removes, adds, passes) = match op {
            Op::Add(value) => (ADD, value, false, true, false),
            Op::Remove => (REMOVE, 0, true, false, true),
            Op::Both(value) => (BOTH, value, true, true, true),
            Op::Skip => (REMOVE, 0, true, false, false),
        };
        types.push(event);
        let shared = matches!(shape, Shape::Top | Shape::Body);
        let mut unit = |key: u32, key2: u32, value: u32| {
            remove_k1.push(if removes || !shared { key } else { 900 + row });
            add_k1.push(if adds || !shared { key } else { 800 + row });
            remove_k2.push(key2);
            add_k2.push(key2);
            values.push(value);
            predicates.push(u32::from(passes));
        };
        if shape == Shape::FlatMap {
            unit(700 + row, 1, 1);
        }
        unit(key, KEY2, value);
        offsets.push(u32::try_from(values.len()).unwrap());
    }
    let units = values.len();
    let wide: Vec<i64> = values.iter().map(|&value| i64::from(value)).collect();
    let init_offsets: Vec<u32> = (0..=units)
        .map(|unit| u32::try_from(unit * COUNTS_INIT.len()).unwrap())
        .collect();
    let mut cols = vec![Vec::new(); COLUMNS];
    cols[usize::from(TYPE)] = u32s_as_bytes(&types).to_vec();
    cols[usize::from(OFFSETS)] = u32s_as_bytes(&offsets).to_vec();
    cols[usize::from(REMOVE_K1)] = u32s_as_bytes(&remove_k1).to_vec();
    cols[usize::from(REMOVE_K2)] = u32s_as_bytes(&remove_k2).to_vec();
    cols[usize::from(ADD_K1)] = u32s_as_bytes(&add_k1).to_vec();
    cols[usize::from(ADD_K2)] = u32s_as_bytes(&add_k2).to_vec();
    cols[usize::from(VALUE)] = u32s_as_bytes(&values).to_vec();
    cols[usize::from(VALUE_I64)] = i64s_as_bytes(&wide).to_vec();
    cols[usize::from(ZERO_I64)] = i64s_as_bytes(&vec![0; units]).to_vec();
    cols[usize::from(BUCKET)] = u32s_as_bytes(&vec![0; units]).to_vec();
    cols[usize::from(INIT_OFFSETS)] = u32s_as_bytes(&init_offsets).to_vec();
    cols[usize::from(INIT_VALUES)] = COUNTS_INIT.repeat(units);
    cols[usize::from(PREDICATE)] = u32s_as_bytes(&predicates).to_vec();
    Batch {
        cols,
        rows: u32::try_from(log.len()).unwrap(),
    }
}

impl Batch {
    fn slices(&self) -> Vec<&[u8]> {
        self.cols.iter().map(Vec::as_slice).collect()
    }
}

fn run(vm: &mut Vm, state: &mut [u8], program: &[u8], batch: &Batch) -> u32 {
    vm.execute_batch(state, program, &batch.slices(), batch.rows)
}

/// [`run`] on the paired undo/redo lane.
fn run_delta(vm: &mut Vm, state: &mut [u8], program: &[u8], batch: &Batch) -> u32 {
    vm.execute_batch_delta(state, program, &batch.slices(), batch.rows)
}

/// The image as a later batch reads it: every struct map sealed into its
/// canonical layout, and the slot's per-batch change flags cleared.
fn settled(vm: &mut Vm, mut state: Vec<u8>) -> Vec<u8> {
    vm.seal_state(&mut state, &NoStrings).expect("seal");
    state[(slot_meta_base(u32::from(SLOT)) + SlotMetaOffset::CHANGE_FLAGS) as usize] = 0;
    state
}

/// The value field (or, for counts, the count) `KEY` holds.
fn observe(state: &[u8], target: Target, pick: Pick) -> Option<u32> {
    let slot = u32::from(SLOT);
    match target {
        Target::Unary => {
            let map = StructMapSlot::bind(state, slot);
            let pos = map.find(state, KEY)?;
            Some(if pick == Pick::Counts {
                let frame = map.arena_field(state, pos, 0).unwrap().unwrap();
                u32::from_le_bytes(frame[1..5].try_into().unwrap())
            } else {
                let at = (map.row_off(pos) + map.field_offset(state, 0)) as usize;
                u32::from_le_bytes(state[at..at + 4].try_into().unwrap())
            })
        }
        Target::Pair | Target::Prefix => {
            let map = StructMap2Slot::bind(state, slot);
            let pos = map.find(state, KEY, KEY2)?;
            let at = (map.row_off(pos) + map.field_offset(state, 0)) as usize;
            Some(u32::from_le_bytes(state[at..at + 4].try_into().unwrap()))
        }
        Target::Map | Target::MapIf => CheckedMap::bind(state, slot).unwrap().get(KEY),
    }
}

fn expected(expect: Expect, pick: Pick) -> Option<u32> {
    match (expect, pick) {
        (Expect::Absent, _) => None,
        (Expect::Fresh(_) | Expect::Seeded, Pick::Counts) => Some(1),
        (Expect::Fresh(value) | Expect::Value(value), _) => Some(value),
        (Expect::Seeded, _) => Some(SEED),
    }
}

/// A program for `(target, pick, shape)` and an image an earlier batch
/// seeded with `KEY`.
fn seeded(target: Target, pick: Pick, shape: Shape) -> (Vec<u8>, Vec<u8>, Vm) {
    let program = program(&init(target, pick), 1, &reduce(target, pick, shape));
    let mut state = image(&program);
    let mut vm = Vm::default();
    let seed = batch(shape, &[(Op::Add(SEED), KEY)], 100);
    assert_eq!(OK, run(&mut vm, &mut state, &program, &seed));
    (program, state, vm)
}

#[test]
fn one_batch_and_one_batch_per_row_leave_identical_slots() {
    for (target, pick) in COMBOS {
        for shape in shapes(target) {
            for scenario in SCENARIOS.iter().filter(|s| (s.applies)(target, pick)) {
                let case = format!("{target:?} {pick:?} {shape:?}: {}", scenario.name);
                let (program, seeded, mut vm) = seeded(target, pick, shape);

                let mut merged = seeded.clone();
                let log = batch(shape, scenario.log, 0);
                assert_eq!(OK, run(&mut vm, &mut merged, &program, &log), "{case}");

                let mut split = seeded;
                for (row, event) in (0..).zip(scenario.log) {
                    let one = batch(shape, core::slice::from_ref(event), row);
                    assert_eq!(OK, run(&mut vm, &mut split, &program, &one), "{case}");
                }

                // (one batch, one batch per row): the row each left under KEY.
                let want = expected(scenario.expect, pick);
                assert_eq!(
                    (
                        observe(&merged, target, pick),
                        observe(&split, target, pick)
                    ),
                    (want, want),
                    "{case}"
                );
                assert!(
                    settled(&mut vm, merged) == settled(&mut vm, split),
                    "{case}: one batch and one batch per row diverge"
                );
            }
        }
    }
}

#[test]
fn a_rolled_back_or_refused_batch_leaves_the_image_byte_for_byte() {
    let log = [(Op::Add(5), KEY), (Op::Remove, KEY), (Op::Add(3), KEY)];
    for (target, pick) in COMBOS {
        for shape in shapes(target) {
            let case = format!("{target:?} {pick:?} {shape:?}");
            let (program, mut state, mut vm) = seeded(target, pick, shape);
            let before = state.clone();

            vm.undo_enable(&state).expect("valid inactive window");
            let checkpoint = vm.undo_checkpoint();
            let fold = batch(shape, &log, 0);
            assert_eq!(
                OK,
                run_delta(&mut vm, &mut state, &program, &fold),
                "{case}"
            );
            assert!(state != before, "{case}: the batch folded nothing");
            vm.undo_rollback(&mut state, checkpoint)
                .expect("valid checkpoint");
            vm.undo_commit();
            assert!(state == before, "{case}: rollback left other bytes");

            // Past the slot's capacity the batch refuses before its removal
            // erases the seed row.
            let crowd: Vec<(Op, u32)> = core::iter::once((Op::Remove, KEY))
                .chain((0..64).map(|key| (Op::Add(1), 1_000 + key)))
                .collect();
            assert_eq!(
                ErrorCode::NeedsGrowth as u32,
                run(&mut vm, &mut state, &program, &batch(shape, &crowd, 0)),
                "{case}"
            );
            assert!(state == before, "{case}: a refused batch wrote");
        }
    }
}

#[test]
fn a_warmed_batch_records_and_reads_its_cuts_without_allocating() {
    let log = [
        (Op::Add(5), KEY),
        (Op::Remove, KEY),
        (Op::Add(3), KEY),
        (Op::Both(4), 6),
        (Op::Add(2), 6),
    ];
    for (target, pick) in COMBOS {
        for shape in shapes(target) {
            let case = format!("{target:?} {pick:?} {shape:?}");
            let (program, seeded, mut vm) = seeded(target, pick, shape);
            let fold = batch(shape, &log, 0);
            let mut warm = seeded.clone();
            assert_eq!(OK, run(&mut vm, &mut warm, &program, &fold), "{case}");
            let mut state = seeded;
            // The columns are built outside the window: only the VM runs in it.
            let cols = fold.slices();
            let result = with_allocations(0, || {
                vm.execute_batch(&mut state, &program, &cols, fold.rows)
            });
            assert_eq!(OK, result, "{case}");
            assert!(state == warm, "{case}");
        }
    }
}

#[test]
fn a_removal_after_a_producer_of_its_slot_refuses_the_program_before_any_write() {
    for (target, pick) in COMBOS {
        let (remove, produce) = (removal(target), producer(target, pick));
        let late = [
            (
                "in one body",
                for_each(
                    &[ADD, REMOVE, BOTH],
                    &[produce.clone(), remove.clone()].concat(),
                ),
            ),
            (
                "in a later body",
                [
                    for_each(&[ADD, BOTH], &produce),
                    for_each(&[REMOVE, BOTH], &remove),
                ]
                .concat(),
            ),
            (
                "under a later FLAT_MAP",
                [
                    for_each(&[ADD, BOTH], &flat_map(&produce)),
                    for_each(&[REMOVE, BOTH], &flat_map(&remove)),
                ]
                .concat(),
            ),
            ("at top level", [produce.clone(), remove.clone()].concat()),
        ];
        for (place, reduce) in late {
            if target == Target::MapIf && place == "at top level" {
                continue;
            }
            let program = program(&init(target, pick), 1, &reduce);
            let mut state = image(&program);
            let before = state.clone();
            let mut vm = Vm::default();
            let log = batch(Shape::FlatMap, &[(Op::Add(1), KEY), (Op::Remove, KEY)], 0);
            assert_eq!(
                ErrorCode::InvalidProgram as u32,
                run(&mut vm, &mut state, &program, &log),
                "{target:?} {pick:?} {place}"
            );
            assert!(state == before, "{target:?} {pick:?} {place}");
        }
    }

    // The order is per slot: a removal may follow another slot's producer.
    let mut init = init(Target::Unary, Pick::Last);
    init.pop();
    init.extend([
        Opcode::SlotStructMap as u8,
        1,
        SlotType::StructMap as u8,
        16,
        0,
        1,
        StructFieldType::UInt32 as u8,
        Opcode::Halt as u8,
    ]);
    let reduce = [
        producer(Target::Unary, Pick::Last),
        vec![Opcode::BatchStructMapRemove as u8, 1, REMOVE_K1],
    ]
    .concat();
    let program = program(&init, 2, &reduce);
    let mut state = image(&program);
    let log = batch(Shape::Top, &[(Op::Add(1), KEY)], 0);
    assert_eq!(OK, run(&mut Vm::default(), &mut state, &program, &log));
}
