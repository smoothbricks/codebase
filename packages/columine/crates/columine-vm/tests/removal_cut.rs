//! Log-order removal (`specs/columine/05-log-order-removal.md`): a removal
//! erases its key at its own row inside the target's fold, so one log leaves
//! the same slot whether it arrives as one batch or as one batch per row.
//!
//! Every removal kind (one-key, exact pair, key1 prefix, hash map, conditional
//! hash map) runs against every producer that can fold its slot: each
//! struct-map pick, the counts fold, the guarded upsert, the probe, the three
//! scatters, the two-key last and tuple picks, and every hash-map upsert
//! family with its TTL and conditional forms. Each pair runs in every program
//! shape its opcodes allow: top-level instructions; one `FOR_EACH` body that
//! removes, then produces, row by row; two `FOR_EACH`es gated by event type;
//! and the same under `FLAT_MAP`, whose elements take their parent row's
//! position. Each log folds once merged and once split, and the two images
//! must be identical byte for byte once sealed, with the per-batch change
//! flags left out.

mod common;

use columine_types::PROGRAM_MAGIC;
use columine_types::operand::encode_index;
use columine_types::types::{
    EMPTY_KEY, ErrorCode, NO_PARENT_TS_COL, Opcode, SlotMetaOffset, SlotType, StructFieldType,
    TOMBSTONE, state_num_slots,
};
use columine_vm::hashmap_ops::CmpType;
use columine_vm::key_index::NoStrings;
use columine_vm::meta::{SlotMetaView, slot_meta_base};
use columine_vm::state_init::{
    DEFAULT_ACCEPTED_PROGRAM_MAGICS, EVICTION_ENTRY_SIZE, calculate_state_size, init_state,
};
use columine_vm::struct_map::{StructMap2Slot, StructMapSlot};
use columine_vm::vm::{CheckedMap, Vm, f64s_as_bytes, i64s_as_bytes, u32s_as_bytes};
use common::memory::with_allocations;

const OK: u32 = ErrorCode::Ok as u32;
/// The slot every removal and producer under test names.
const SLOT: u8 = 0;
/// The slot a probe reads, or a guarded upsert's companion claims.
const AUX: u8 = 1;
/// The key every scenario folds and removes; pair targets pair it with
/// `KEY2`.
const KEY: u32 = 5;
const KEY2: u32 = 7;
/// What an earlier batch left under `KEY` before each scenario.
const SEED: u32 = 9;
/// The guard key every companion claim and guarded write carries.
const GUARD_KEY: u32 = 77;

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
/// Zeros: a counts bucket, a reset flag, a scatter route and an assert.
const ZERO: u8 = 9;
const INIT_OFFSETS: u8 = 10;
const INIT_VALUES: u8 = 11;
/// A conditional removal's predicate.
const PREDICATE: u8 = 12;
/// The log row plus one: a timestamp or transaction guard that rises with
/// the log, as `u32` and as `f64`.
const TICK: u8 = 13;
const TICK_F64: u8 = 14;
/// Ones: every conditional upsert's predicate.
const ONES: u8 = 15;
/// `1000 * key + value`: the probe source row an add reads.
const PROBE_ID: u8 = 16;
const GUARD: u8 = 17;
const COLUMNS: usize = 18;

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
    Min,
    /// 0x8b with a reset flag that never fires.
    FirstSinceReset,
    Counts,
    /// 0x8d into `SLOT`, admitted by companion claims in `AUX`.
    Guarded,
    /// 0x2e: `SLOT`'s row copied from the source row in `AUX`.
    Probe,
    /// 0x3e, 0x2f (through `AUX`) and 0x3f: a card-one route into `SLOT`.
    Scatter,
    ProbeScatter,
    ScatterGuarded,
    /// 0x20, 0x24 and 0x25 (both on a TTL slot), and the conditional forms.
    Latest,
    LatestTtl,
    LastTtl,
    LatestIf,
    FirstIf,
    LastIf,
    MaxIf,
    MinIf,
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

const COMBOS: [(Target, Pick); 30] = [
    (Target::Unary, Pick::Last),
    (Target::Unary, Pick::First),
    (Target::Unary, Pick::Max),
    (Target::Unary, Pick::FirstSinceReset),
    (Target::Unary, Pick::Counts),
    (Target::Unary, Pick::Guarded),
    (Target::Unary, Pick::Probe),
    (Target::Unary, Pick::Scatter),
    (Target::Unary, Pick::ProbeScatter),
    (Target::Unary, Pick::ScatterGuarded),
    (Target::Pair, Pick::Last),
    (Target::Pair, Pick::Max),
    (Target::Pair, Pick::Min),
    (Target::Prefix, Pick::Last),
    (Target::Prefix, Pick::Max),
    (Target::Prefix, Pick::Min),
    (Target::Map, Pick::Last),
    (Target::Map, Pick::First),
    (Target::Map, Pick::Max),
    (Target::Map, Pick::Min),
    (Target::Map, Pick::Latest),
    (Target::Map, Pick::LatestTtl),
    (Target::Map, Pick::LastTtl),
    (Target::Map, Pick::LatestIf),
    (Target::Map, Pick::FirstIf),
    (Target::Map, Pick::LastIf),
    (Target::Map, Pick::MaxIf),
    (Target::Map, Pick::MinIf),
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

/// A guarded upsert writes only while its companion claims the key and the
/// destination row is absent, and consumes the claim; each removal of the
/// destination clears the claim too. No destination row is seeded.
const GUARDED_SCENARIOS: [Scenario; 6] = [
    Scenario {
        name: "a guarded write lands while its claim stands",
        log: &[(Op::Add(1), KEY)],
        expect: Expect::Value(1),
        applies: every,
    },
    Scenario {
        name: "a removal before a guarded write clears its claim",
        log: &[(Op::Remove, KEY), (Op::Add(1), KEY)],
        expect: Expect::Absent,
        applies: every,
    },
    Scenario {
        name: "a guarded write before a removal is erased",
        log: &[(Op::Add(1), KEY), (Op::Remove, KEY)],
        expect: Expect::Absent,
        applies: every,
    },
    Scenario {
        name: "the first guarded write consumes the claim",
        log: &[(Op::Add(5), KEY), (Op::Add(3), KEY)],
        expect: Expect::Value(5),
        applies: every,
    },
    Scenario {
        name: "a row that writes and removes one key composes write-then-erase",
        log: &[(Op::Both(1), KEY)],
        expect: Expect::Absent,
        applies: every,
    },
    Scenario {
        name: "a sentinel removal leaves the claim",
        log: &[(Op::Remove, EMPTY_KEY), (Op::Add(2), KEY)],
        expect: Expect::Value(2),
        applies: every,
    },
];

fn scenarios(target: Target, pick: Pick) -> impl Iterator<Item = &'static Scenario> {
    let table: &'static [Scenario] = if pick == Pick::Guarded {
        &GUARDED_SCENARIOS
    } else {
        &SCENARIOS
    };
    table
        .iter()
        .filter(move |scenario| (scenario.applies)(target, pick))
}

/// Whether a removal or producer of the pair is an element-body opcode.
fn element_only(target: Target, pick: Pick) -> bool {
    target == Target::MapIf
        || matches!(
            pick,
            Pick::Probe
                | Pick::Scatter
                | Pick::ProbeScatter
                | Pick::ScatterGuarded
                | Pick::LatestIf
                | Pick::FirstIf
                | Pick::LastIf
                | Pick::MaxIf
                | Pick::MinIf
        )
}

fn shapes(target: Target, pick: Pick) -> impl Iterator<Item = Shape> {
    SHAPES
        .into_iter()
        .filter(move |&shape| shape != Shape::Top || !element_only(target, pick))
}

fn removal(target: Target, pick: Pick) -> Vec<u8> {
    let mut code = match target {
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
    };
    // A guarded destination's removal clears its companion claim under the
    // same key column: the pairing every guarded program keeps.
    if pick == Pick::Guarded {
        code.extend([Opcode::BatchStructMapRemove as u8, AUX, REMOVE_K1]);
    }
    code
}

fn producer(target: Target, pick: Pick) -> Vec<u8> {
    let u32_cmp = CmpType::U32 as u8;
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
        // The value in field 0, the reset flag (always clear) in field 1.
        (Target::Unary, Pick::FirstSinceReset) => vec![
            Opcode::BatchStructMapUpsertFirstSinceReset as u8,
            SLOT,
            ADD_K1,
            2,
            VALUE,
            0,
            ZERO,
            1,
            0,
            1,
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
            ZERO,
            0,
            1,
            b'P',
        ],
        // slot, companion, key, guard key column, companion field 0, then
        // the upsert-last row.
        (Target::Unary, Pick::Guarded) => vec![
            Opcode::BatchStructMapGuardedUpsertLast as u8,
            SLOT,
            AUX,
            ADD_K1,
            GUARD,
            0,
            1,
            VALUE,
            0,
            0,
        ],
        // Probe AUX by PROBE_ID, miss skips, copy its field 3 into field 0
        // of SLOT under the row's key.
        (Target::Unary, Pick::Probe) => vec![
            Opcode::BatchStructMapProbe as u8,
            AUX,
            PROBE_ID,
            0,
            SLOT,
            1,
            3,
            0,
            ADD_K1,
        ],
        // Route 0 (kind 0: field 0 of SLOT from VALUE), asserts only.
        (Target::Unary, Pick::Scatter) => vec![
            Opcode::BatchStructMapScatter as u8,
            ZERO,
            ZERO,
            ADD_K1,
            1,
            0,
            SLOT,
            0,
            VALUE,
        ],
        // The probed AUX row names route (field 0), op (field 1), the
        // destination key (field 2) and the value (field 3).
        (Target::Unary, Pick::ProbeScatter) => vec![
            Opcode::BatchStructMapProbeScatter as u8,
            AUX,
            PROBE_ID,
            0,
            0,
            1,
            1,
            0,
            SLOT,
            0,
            2,
            3,
        ],
        // SLOT guards each row with the transaction in field 1 (TICK).
        (Target::Unary, Pick::ScatterGuarded) => vec![
            Opcode::BatchStructMapScatterGuarded as u8,
            ZERO,
            ZERO,
            ADD_K1,
            SLOT,
            1,
            TICK,
            1,
            1,
            0,
            SLOT,
            0,
            VALUE,
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
        // The greatest or least (value, 0), stored in fields 1 and 2.
        (Target::Pair | Target::Prefix, Pick::Max | Pick::Min) => vec![
            match pick {
                Pick::Max => Opcode::BatchStructMap2UpsertMaxI64x2 as u8,
                _ => Opcode::BatchStructMap2UpsertMinI64x2 as u8,
            },
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
            u32_cmp,
        ],
        (Target::Map, Pick::Min) => vec![
            Opcode::BatchMapUpsertMin as u8,
            SLOT,
            ADD_K1,
            VALUE,
            VALUE,
            u32_cmp,
        ],
        (Target::Map, Pick::Latest) => vec![
            Opcode::BatchMapUpsertLatest as u8,
            SLOT,
            ADD_K1,
            VALUE,
            TICK,
            u32_cmp,
        ],
        (Target::Map, Pick::LatestTtl) => vec![
            Opcode::BatchMapUpsertLatestTtl as u8,
            SLOT,
            ADD_K1,
            VALUE,
            TICK_F64,
            CmpType::F64 as u8,
        ],
        (Target::Map, Pick::LastTtl) => vec![
            Opcode::BatchMapUpsertLastTtl as u8,
            SLOT,
            ADD_K1,
            VALUE,
            TICK_F64,
        ],
        (Target::Map, Pick::LatestIf) => vec![
            Opcode::BatchMapUpsertLatestIf as u8,
            SLOT,
            ADD_K1,
            VALUE,
            TICK,
            u32_cmp,
            ONES,
        ],
        (Target::Map, Pick::FirstIf) => {
            vec![
                Opcode::BatchMapUpsertFirstIf as u8,
                SLOT,
                ADD_K1,
                VALUE,
                ONES,
            ]
        }
        (Target::Map, Pick::LastIf) => {
            vec![
                Opcode::BatchMapUpsertLastIf as u8,
                SLOT,
                ADD_K1,
                VALUE,
                ONES,
            ]
        }
        (Target::Map, Pick::MaxIf | Pick::MinIf) => vec![
            match pick {
                Pick::MaxIf => Opcode::BatchMapUpsertMaxIf as u8,
                _ => Opcode::BatchMapUpsertMinIf as u8,
            },
            SLOT,
            ADD_K1,
            VALUE,
            VALUE,
            u32_cmp,
            ONES,
        ],
        _ => unreachable!("no {pick:?} producer folds a {target:?} target"),
    }
}

/// The slots a pair's program declares: the target, and for a probe or a
/// guarded upsert the `AUX` slot it reads.
fn slots(pick: Pick) -> u32 {
    if matches!(pick, Pick::Guarded | Pick::Probe | Pick::ProbeScatter) {
        2
    } else {
        1
    }
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

fn init(target: Target, pick: Pick) -> Vec<u8> {
    use StructFieldType::{Bool, Int64, UInt32, Vo};
    let mut init = match (target, pick) {
        (Target::Unary, Pick::Counts) => struct_map(SLOT, 16, &[Vo]),
        (Target::Unary, Pick::FirstSinceReset) => struct_map(SLOT, 16, &[UInt32, Bool]),
        (Target::Unary, Pick::ScatterGuarded) => struct_map(SLOT, 16, &[UInt32, UInt32]),
        // The companion admits every claim the seed and the crowd make.
        (Target::Unary, Pick::Guarded) => [
            struct_map(SLOT, 16, &[UInt32]),
            struct_map(AUX, 128, &[UInt32]),
        ]
        .concat(),
        (Target::Unary, Pick::Probe | Pick::ProbeScatter) => [
            struct_map(SLOT, 16, &[UInt32]),
            struct_map(AUX, 128, &[UInt32; 4]),
        ]
        .concat(),
        (Target::Unary, _) => struct_map(SLOT, 16, &[UInt32]),
        (Target::Pair | Target::Prefix, _) => {
            let fields: &[StructFieldType] = if matches!(pick, Pick::Max | Pick::Min) {
                &[UInt32, Int64, Int64]
            } else {
                &[UInt32]
            };
            let mut def = vec![
                Opcode::SlotStructMap2 as u8,
                SLOT,
                SlotType::StructMap2 as u8,
                16,
                0,
                u8::try_from(fields.len()).unwrap(),
            ];
            def.extend(fields.iter().map(|&field| field as u8));
            def
        }
        // A TTL map keeps its timestamps in TICK_F64 and never expires a
        // row within these logs.
        (Target::Map | Target::MapIf, Pick::LatestTtl | Pick::LastTtl) => {
            let mut def = vec![
                Opcode::SlotDef as u8,
                SLOT,
                SlotType::HashMap as u8 | 0x10,
                16,
                0,
            ];
            def.extend(1.0e9f32.to_le_bytes());
            def.extend(0.0f32.to_le_bytes());
            def.extend([TICK_F64, 0]);
            def
        }
        (Target::Map | Target::MapIf, _) => {
            vec![Opcode::SlotDef as u8, SLOT, SlotType::HashMap as u8, 16, 0]
        }
    };
    init.push(Opcode::Halt as u8);
    init
}

/// The program that fills `AUX` before a probe or guarded pair runs: its own
/// program never writes there. Probe source rows hold (route 0, assert,
/// key, value) under `1000 * key + value`; companion claims hold
/// `GUARD_KEY` under the key.
fn aux_seed(pick: Pick) -> Option<Vec<u8>> {
    match pick {
        Pick::Probe | Pick::ProbeScatter => Some(vec![
            Opcode::BatchStructMapUpsertLast as u8,
            AUX,
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
        ]),
        Pick::Guarded => Some(vec![
            Opcode::BatchStructMapUpsertLast as u8,
            AUX,
            ADD_K1,
            1,
            GUARD,
            0,
            0,
        ]),
        _ => None,
    }
}

/// Every key the scenarios, the warm log and the capacity crowd add, with
/// every value they add it with.
fn aux_log() -> Vec<(Op, u32)> {
    [KEY, 6]
        .into_iter()
        .flat_map(|key| (0..16).map(move |value| (Op::Add(value), key)))
        .chain((0..64).map(|key| (Op::Add(1), 1_000 + key)))
        .collect()
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

fn flat_map_over(offsets: u8, body: &[u8]) -> Vec<u8> {
    let mut code = vec![Opcode::FlatMap as u8, offsets];
    code.extend_from_slice(encode_index(NO_PARENT_TS_COL).as_bytes());
    code.extend(u16::try_from(body.len()).unwrap().to_le_bytes());
    code.extend_from_slice(body);
    code
}

fn flat_map(body: &[u8]) -> Vec<u8> {
    flat_map_over(OFFSETS, body)
}

fn reduce(target: Target, pick: Pick, shape: Shape) -> Vec<u8> {
    let (remove, produce) = (removal(target, pick), producer(target, pick));
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

impl Batch {
    fn slices(&self) -> Vec<&[u8]> {
        self.cols.iter().map(Vec::as_slice).collect()
    }
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
    let (mut values, mut predicates, mut ticks) = (Vec::<u32>::new(), Vec::new(), Vec::new());
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
            ticks.push(row + 1);
        };
        if shape == Shape::FlatMap {
            unit(700 + row, 1, 1);
        }
        unit(key, KEY2, value);
        offsets.push(u32::try_from(values.len()).unwrap());
    }
    let units = values.len();
    let wide: Vec<i64> = values.iter().map(|&value| i64::from(value)).collect();
    let ticks_f64: Vec<f64> = ticks.iter().map(|&tick| f64::from(tick)).collect();
    let probe_ids: Vec<u32> = add_k1
        .iter()
        .zip(&values)
        .map(|(&key, &value)| key.wrapping_mul(1_000).wrapping_add(value))
        .collect();
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
    cols[usize::from(ZERO)] = u32s_as_bytes(&vec![0; units]).to_vec();
    cols[usize::from(INIT_OFFSETS)] = u32s_as_bytes(&init_offsets).to_vec();
    cols[usize::from(INIT_VALUES)] = COUNTS_INIT.repeat(units);
    cols[usize::from(PREDICATE)] = u32s_as_bytes(&predicates).to_vec();
    cols[usize::from(TICK)] = u32s_as_bytes(&ticks).to_vec();
    cols[usize::from(TICK_F64)] = f64s_as_bytes(&ticks_f64).to_vec();
    cols[usize::from(ONES)] = u32s_as_bytes(&vec![1; units]).to_vec();
    cols[usize::from(PROBE_ID)] = u32s_as_bytes(&probe_ids).to_vec();
    cols[usize::from(GUARD)] = u32s_as_bytes(&vec![GUARD_KEY; units]).to_vec();
    Batch {
        cols,
        rows: u32::try_from(log.len()).unwrap(),
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
/// canonical layout, the per-batch change flags cleared, and each TTL slot's
/// eviction-index cells past its live entries zeroed. A removal shifts the
/// live entries left and leaves the vacated cell as it was, so those dead
/// bytes keep a history the live index has forgotten, as a struct map's dead
/// rows do until sealed.
fn settled(vm: &mut Vm, mut state: Vec<u8>) -> Vec<u8> {
    vm.seal_state(&mut state, &NoStrings).expect("seal");
    for slot in 0..state_num_slots(&state) {
        state[(slot_meta_base(slot) + SlotMetaOffset::CHANGE_FLAGS) as usize] = 0;
        let meta = SlotMetaView::read(&state, slot);
        if meta.has_ttl() {
            let base = meta.eviction_index_offset(&state);
            let live = base + meta.eviction_index_size(&state) * EVICTION_ENTRY_SIZE;
            let end = base + meta.eviction_index_capacity(&state) * EVICTION_ENTRY_SIZE;
            state[live as usize..end as usize].fill(0);
        }
    }
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

/// A program for `(target, pick, shape)` and an image whose `AUX` rows are
/// written and, unless the producer is a guarded upsert, whose `KEY` an
/// earlier batch seeded.
fn seeded(target: Target, pick: Pick, shape: Shape) -> (Vec<u8>, Vec<u8>, Vm) {
    let declared = init(target, pick);
    let code = program(&declared, slots(pick), &reduce(target, pick, shape));
    let mut state = image(&code);
    let mut vm = Vm::default();
    if let Some(aux) = aux_seed(pick) {
        let seeder = program(&declared, slots(pick), &aux);
        let rows = batch(Shape::Top, &aux_log(), 200);
        assert_eq!(OK, run(&mut vm, &mut state, &seeder, &rows));
    }
    if pick != Pick::Guarded {
        let seed = batch(shape, &[(Op::Add(SEED), KEY)], 100);
        assert_eq!(OK, run(&mut vm, &mut state, &code, &seed));
    }
    (code, state, vm)
}

/// Fold `scenario` as one batch and as one batch per row; `Err` says how the
/// two, or either and the scenario's outcome, differ.
fn fold_both_ways(
    target: Target,
    pick: Pick,
    shape: Shape,
    scenario: &Scenario,
) -> Result<(), String> {
    let (code, seeded, mut vm) = seeded(target, pick, shape);
    let mut merged = seeded.clone();
    let result = run(&mut vm, &mut merged, &code, &batch(shape, scenario.log, 0));
    if result != OK {
        return Err(format!("one batch answered {result}"));
    }
    let mut split = seeded;
    for (row, event) in (0..).zip(scenario.log) {
        let one = batch(shape, core::slice::from_ref(event), row);
        let result = run(&mut vm, &mut split, &code, &one);
        if result != OK {
            return Err(format!("row {row} alone answered {result}"));
        }
    }
    let want = expected(scenario.expect, pick);
    let left = (
        observe(&merged, target, pick),
        observe(&split, target, pick),
    );
    if left != (want, want) {
        return Err(format!(
            "(one batch, one batch per row) left {left:?} under the key, want {want:?}"
        ));
    }
    if settled(&mut vm, merged) != settled(&mut vm, split) {
        return Err("the two images differ".to_owned());
    }
    Ok(())
}

#[test]
fn one_batch_and_one_batch_per_row_leave_identical_slots() {
    let mut failures = Vec::new();
    for (target, pick) in COMBOS {
        for shape in shapes(target, pick) {
            for scenario in scenarios(target, pick) {
                if let Err(failure) = fold_both_ways(target, pick, shape, scenario) {
                    failures.push(format!(
                        "{target:?} {pick:?} {shape:?}: {}: {failure}",
                        scenario.name
                    ));
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} cases fold differently:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn a_rolled_back_or_refused_batch_leaves_the_image_byte_for_byte() {
    let log = [(Op::Add(5), KEY), (Op::Remove, KEY), (Op::Add(3), KEY)];
    for (target, pick) in COMBOS {
        for shape in shapes(target, pick) {
            let case = format!("{target:?} {pick:?} {shape:?}");
            let (code, mut state, mut vm) = seeded(target, pick, shape);
            let before = state.clone();

            vm.undo_enable(&state).expect("valid inactive window");
            let checkpoint = vm.undo_checkpoint();
            let fold = batch(shape, &log, 0);
            assert_eq!(OK, run_delta(&mut vm, &mut state, &code, &fold), "{case}");
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
                run(&mut vm, &mut state, &code, &batch(shape, &crowd, 0)),
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
        for shape in shapes(target, pick) {
            let case = format!("{target:?} {pick:?} {shape:?}");
            let (code, seeded, mut vm) = seeded(target, pick, shape);
            let fold = batch(shape, &log, 0);
            let mut warm = seeded.clone();
            assert_eq!(OK, run(&mut vm, &mut warm, &code, &fold), "{case}");
            let mut state = seeded;
            // The columns are built outside the window: only the VM runs in it.
            let cols = fold.slices();
            let result =
                with_allocations(0, || vm.execute_batch(&mut state, &code, &cols, fold.rows));
            assert_eq!(OK, result, "{case}");
            assert!(state == warm, "{case}");
        }
    }
}

#[test]
fn a_removal_after_a_producer_of_its_slot_refuses_the_program_before_any_write() {
    let mut failures = Vec::new();
    for (target, pick) in COMBOS {
        let (remove, produce) = (removal(target, pick), producer(target, pick));
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
            if place == "at top level" && element_only(target, pick) {
                continue;
            }
            let code = program(&init(target, pick), slots(pick), &reduce);
            let mut state = image(&code);
            let before = state.clone();
            let log = batch(Shape::FlatMap, &[(Op::Add(1), KEY), (Op::Remove, KEY)], 0);
            let result = run(&mut Vm::default(), &mut state, &code, &log);
            if result != ErrorCode::InvalidProgram as u32 || state != before {
                failures.push(format!(
                    "{target:?} {pick:?} {place}: answered {result}, image changed: {}",
                    state != before
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} late removals admitted:\n{}",
        failures.len(),
        failures.join("\n")
    );

    // The order is per slot: a removal may follow another slot's producer.
    let mut declared = init(Target::Unary, Pick::Last);
    declared.pop();
    declared.extend(struct_map(AUX, 16, &[StructFieldType::UInt32]));
    declared.push(Opcode::Halt as u8);
    let reduce = [
        producer(Target::Unary, Pick::Last),
        vec![Opcode::BatchStructMapRemove as u8, AUX, REMOVE_K1],
    ]
    .concat();
    let code = program(&declared, 2, &reduce);
    let mut state = image(&code);
    let log = batch(Shape::Top, &[(Op::Add(1), KEY)], 0);
    assert_eq!(OK, run(&mut Vm::default(), &mut state, &code, &log));
}

/// A dry run lets each container call reserve its own scratch mid-batch (a
/// set insert here); that reservation keeps the cuts the batch's earlier
/// removals recorded. The counts fold forces the dry run, and the batch
/// removes and folds more keys than its one-key map admits: were the cuts
/// forgotten, every fold would land and the batch would ask for growth it
/// does not need.
#[test]
fn a_container_call_inside_a_dry_run_keeps_the_batch_cuts() {
    let mut declared = struct_map(SLOT, 1, &[StructFieldType::Vo]);
    declared.extend([
        Opcode::SlotDef as u8,
        AUX,
        SlotType::HashSet as u8,
        128,
        0,
        Opcode::Halt as u8,
    ]);
    let reduce = [
        removal(Target::Unary, Pick::Counts),
        vec![Opcode::BatchSetInsert as u8, AUX, ADD_K1],
        producer(Target::Unary, Pick::Counts),
    ]
    .concat();
    let code = program(&declared, 2, &reduce);
    let empty = image(&code);
    let log: Vec<(Op, u32)> = (100..132).map(|key| (Op::Both(1), key)).collect();
    let fold = batch(Shape::Top, &log, 0);
    let mut vm = Vm::default();

    let mut merged = empty.clone();
    assert_eq!(OK, run(&mut vm, &mut merged, &code, &fold));
    let mut split = empty.clone();
    for (row, event) in (0..).zip(&log) {
        let one = batch(Shape::Top, core::slice::from_ref(event), row);
        assert_eq!(OK, run(&mut vm, &mut split, &code, &one), "row {row}");
    }
    let absent = |state: &[u8]| {
        let map = StructMapSlot::bind(state, u32::from(SLOT));
        (100..132).all(|key| map.find(state, key).is_none())
    };
    assert!(absent(&merged) && absent(&split));
    assert!(settled(&mut vm, merged.clone()) == settled(&mut vm, split));

    // Warmed, the same batch reserves, records and reads without allocating.
    let mut state = empty;
    let cols = fold.slices();
    let result = with_allocations(0, || vm.execute_batch(&mut state, &code, &cols, fold.rows));
    assert_eq!(OK, result);
    assert!(state == merged);
}

/// One row of a two-array FLAT_MAP batch: its event type, the keys its
/// removal elements name, and the (key, value) pairs its producer elements
/// write.
struct ArrayRow {
    event: u32,
    removed: &'static [u32],
    added: &'static [(u32, u32)],
}

/// The batch carrying `rows`: the removal arm flattens column 2 by the
/// offsets in column 1, the producer arm columns 4 and 5 by the offsets in
/// column 3, so the two arms number their elements independently.
fn array_batch(rows: &[ArrayRow]) -> Batch {
    let mut types = Vec::new();
    let (mut removal_offsets, mut removed) = (vec![0u32], Vec::new());
    let (mut producer_offsets, mut keys, mut values) = (vec![0u32], Vec::new(), Vec::new());
    for row in rows {
        types.push(row.event);
        removed.extend_from_slice(row.removed);
        removal_offsets.push(u32::try_from(removed.len()).unwrap());
        for &(key, value) in row.added {
            keys.push(key);
            values.push(value);
        }
        producer_offsets.push(u32::try_from(keys.len()).unwrap());
    }
    let mut cols = vec![Vec::new(); COLUMNS];
    cols[0] = u32s_as_bytes(&types).to_vec();
    cols[1] = u32s_as_bytes(&removal_offsets).to_vec();
    cols[2] = u32s_as_bytes(&removed).to_vec();
    cols[3] = u32s_as_bytes(&producer_offsets).to_vec();
    cols[4] = u32s_as_bytes(&keys).to_vec();
    cols[5] = u32s_as_bytes(&values).to_vec();
    Batch {
        cols,
        rows: u32::try_from(rows.len()).unwrap(),
    }
}

/// A FLAT_MAP element takes its parent row's log position, never its own
/// element index: here the removal and producer arms flatten different
/// arrays, so an earlier row's add sits at a higher element index than a
/// later row's removal, and the reverse.
#[test]
fn flat_map_elements_take_their_parent_rows_position() {
    let removal_arm = [Opcode::BatchStructMapRemove as u8, SLOT, 2];
    let producer_arm = [Opcode::BatchStructMapUpsertLast as u8, SLOT, 4, 1, 5, 0, 0];
    let reduce = [
        for_each(&[REMOVE], &flat_map_over(1, &removal_arm)),
        for_each(&[ADD], &flat_map_over(3, &producer_arm)),
    ]
    .concat();
    let code = program(&init(Target::Unary, Pick::Last), 1, &reduce);
    let cases: [(&str, &[ArrayRow], Expect); 2] = [
        (
            "an add at element 3 of row 0, then a removal at element 0 of row 1",
            &[
                ArrayRow {
                    event: ADD,
                    removed: &[],
                    added: &[(700, 1), (701, 1), (702, 1), (KEY, 7)],
                },
                ArrayRow {
                    event: REMOVE,
                    removed: &[KEY],
                    added: &[],
                },
            ],
            Expect::Absent,
        ),
        (
            "a removal at element 3 of row 0, then an add at element 0 of row 1",
            &[
                ArrayRow {
                    event: REMOVE,
                    removed: &[710, 711, 712, KEY],
                    added: &[],
                },
                ArrayRow {
                    event: ADD,
                    removed: &[],
                    added: &[(KEY, 7)],
                },
            ],
            Expect::Value(7),
        ),
    ];
    for (name, rows, expect) in cases {
        let mut vm = Vm::default();
        let empty = image(&code);
        let mut merged = empty.clone();
        assert_eq!(
            OK,
            run(&mut vm, &mut merged, &code, &array_batch(rows)),
            "{name}"
        );
        let mut split = empty;
        for row in rows {
            let one = array_batch(core::slice::from_ref(row));
            assert_eq!(OK, run(&mut vm, &mut split, &code, &one), "{name}");
        }
        let want = expected(expect, Pick::Last);
        assert_eq!(
            (
                observe(&merged, Target::Unary, Pick::Last),
                observe(&split, Target::Unary, Pick::Last)
            ),
            (want, want),
            "{name}"
        );
        assert!(
            settled(&mut vm, merged) == settled(&mut vm, split),
            "{name}: the two images differ"
        );
    }
}
