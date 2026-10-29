//! Fold equivalence of the struct-map scatters (`0x2f`, `0x3e`, `0x3f`):
//! one reduce over a batch leaves exactly the state that reducing the same
//! rows one call at a time, in arrival order, leaves. The contract is the
//! opcodes' (`Opcode::BatchStructMapScatter`, `specs/columine/01-scatter-routes.md`
//! "Stream order"): a card-one assert overwrites whatever an earlier element
//! wrote, a retract compares against the value as it stands at its own
//! position, and a guard compares against the guard an earlier row of the same
//! batch stamped.

use columine_types::NO_PARENT_TS_COL;
use columine_types::PROGRAM_MAGIC;
use columine_types::operand::encode_index;
use columine_types::types::{ErrorCode, SLOT_META_SIZE, STATE_HEADER_SIZE, SlotMetaOffset};
use columine_vm::bytes;
use columine_vm::state_init::{DEFAULT_ACCEPTED_PROGRAM_MAGICS, calculate_state_size, init_state};
use columine_vm::struct_map::StructMapSlot;
use columine_vm::vm::{
    CheckedSet, CheckedStructMap, Vm, f64s_as_bytes, i64s_as_bytes, u32s_as_bytes,
};
use proptest::prelude::*;

const OK: u32 = ErrorCode::Ok as u32;
const T: u32 = 4201;
const NUM_SLOTS: u32 = 3;
/// The `0x2f` form's staged rows; the probe-free forms never read it.
const PROBE_SLOT: u32 = 2;

// Routes, shared by the three opcodes' route tables.
const STATUS: u32 = 0;
const LANDED: u32 = 1;
const DEP: u32 = 2;
const BOOST: u32 = 3;
/// A second route onto the status field: a producer may route two attributes,
/// such as a set's own commit and a later revert of it, onto one field.
const REVERTED: u32 = 4;
const SKIP: u32 = u32::MAX;

// Slot 0 row: bitset(1), status:STR, landedAt:STR, boost:F64, at_ns:I64, ordinal:I64.
const STATUS_AT: u32 = 1;
const LANDED_AT: u32 = 5;
const BOOST_AT: u32 = 9;
const GUARD_AT_NS_AT: u32 = 17;
const GUARD_ORDINAL_AT: u32 = 25;

// Interned handles and keys.
const A: u32 = 500;
const U: u32 = 501;
const Y: u32 = 502;
const X: u32 = 503;
const CLOSED: u32 = 7001;
const OPEN: u32 = 7002;
const AT_07: u32 = 7007;
const AT_09: u32 = 7009;

/// One resolved datom: a FLAT_MAP child element. Columns 0=type and
/// 1=offsets are per parent row; per element 2=route, 3=op, 4=key, 5=v_str,
/// 6=v_num(f64), 7=v_set, 8=guard at_ns(i64), 9=guard ordinal(i64), and
/// 10=the key its staged copy has in the probe slot (`0x2f`).
#[derive(Clone, Copy, Debug)]
struct Datom {
    route: u32,
    retract: bool,
    key: u32,
    v_str: u32,
    v_num: f64,
    v_set: u32,
    at_ns: i64,
    ordinal: i64,
}

const fn datom(route: u32, retract: bool, key: u32, v_str: u32) -> Datom {
    Datom {
        route,
        retract,
        key,
        v_str,
        v_num: 0.0,
        v_set: 0,
        at_ns: 0,
        ordinal: 0,
    }
}

const fn assert(route: u32, key: u32, v_str: u32) -> Datom {
    datom(route, false, key, v_str)
}

const fn retract(route: u32, key: u32, v_str: u32) -> Datom {
    datom(route, true, key, v_str)
}

const fn boost(key: u32, v_num: f64) -> Datom {
    Datom {
        v_num,
        ..datom(BOOST, false, key, 0)
    }
}

const fn dep(retract: bool, key: u32, v_set: u32) -> Datom {
    Datom {
        v_set,
        ..datom(DEP, retract, key, 0)
    }
}

/// `datoms` as one transaction carrying guard `(at_ns, ordinal)`.
fn guarded(at_ns: i64, ordinal: i64, datoms: &[Datom]) -> Vec<Datom> {
    datoms
        .iter()
        .map(|&d| Datom {
            at_ns,
            ordinal,
            ..d
        })
        .collect()
}

#[derive(Clone, Copy, Debug)]
enum Scatter {
    Probe,
    Plain,
    Guarded,
}

/// Every unguarded case runs through all three forms: the guarded form with
/// every guard equal proceeds for every element, so it must agree too.
const UNGUARDED: &[Scatter] = &[Scatter::Probe, Scatter::Plain, Scatter::Guarded];

/// The scatter body. `0x3e`: route_col=2, op_col=3, key_col=4. `0x3f`: the
/// same, then guard_slot=0 with (col 8 -> field 3, col 9 -> field 4). Both
/// then take four routes reading their value from a column. `0x2f`: probe
/// the probe slot by column 10, reading route, op and output key from its
/// fields 0, 1 and 2, then four routes reading their value from a field.
fn scatter_body(form: Scatter) -> Vec<u8> {
    #[rustfmt::skip]
    let column_routes: [u8; 21] = [
        5,
        0, 0, 0, 5, // STATUS: kind0 -> slot 0 field 0, v=v_str
        0, 0, 1, 5, // LANDED: kind0 -> slot 0 field 1, v=v_str
        1, 1, 0, 7, // DEP:    kind1 -> slot 1,         v=v_set
        0, 0, 2, 6, // BOOST:  kind0 -> slot 0 field 2, v=v_num
        0, 0, 0, 5, // REVERTED: kind0 -> slot 0 field 0, v=v_str
    ];
    #[rustfmt::skip]
    let probe: [u8; 32] = [
        0x2f, PROBE_SLOT as u8, 10, 0, 0, 1, 5,
        0, 0, 0, 2, 3, // STATUS: kind0 -> slot 0 field 0, key=e, v=v_str
        0, 0, 1, 2, 3, // LANDED: kind0 -> slot 0 field 1, key=e, v=v_str
        1, 1, 0, 2, 5, // DEP:    kind1 -> slot 1,         key=e, v=v_set
        0, 0, 2, 2, 4, // BOOST:  kind0 -> slot 0 field 2, key=e, v=v_num
        0, 0, 0, 2, 3, // REVERTED: kind0 -> slot 0 field 0, key=e, v=v_str
    ];
    let mut body = match form {
        Scatter::Probe => return probe.to_vec(),
        Scatter::Plain => vec![0x3e, 2, 3, 4],
        Scatter::Guarded => vec![0x3f, 2, 3, 4, 0, 2, 8, 3, 9, 4],
    };
    body.extend(column_routes);
    body
}

/// FOR_EACH(type == T) { FLAT_MAP(offsets = col 1) { scatter } } over slot 0,
/// a struct map [status:STR, landedAt:STR, boost:F64, at_ns:I64,
/// ordinal:I64], slot 1, a HASHSET, and the probe slot, a struct map
/// [route:U32, op:U32, e:U32, v_str:STR, v_num:F64, v_set:U32].
fn scatter_program(form: Scatter) -> Vec<u8> {
    let mut init = vec![0x18, 0, 6, 32, 0, 5, 4, 4, 2, 1, 1];
    init.extend([0x10, 1, 1, 32, 0]);
    init.extend([0x18, PROBE_SLOT as u8, 6, 64, 0, 6, 0, 0, 0, 4, 2, 0]);
    init.push(0);
    let body = scatter_body(form);
    let mut flat_map = vec![0xE1, 1];
    flat_map.extend(encode_index(NO_PARENT_TS_COL).as_bytes());
    flat_map.extend((body.len() as u16).to_le_bytes());
    flat_map.extend(body);
    let mut reduce = vec![0xE0, 0, 1];
    reduce.extend(T.to_le_bytes());
    reduce.extend((flat_map.len() as u16).to_le_bytes());
    reduce.extend(flat_map);
    reduce.push(0);
    let mut prog = vec![0u8; 32];
    prog.extend(PROGRAM_MAGIC.to_le_bytes());
    prog.extend([2, 0, 0, 0]);
    prog.extend(NUM_SLOTS.to_le_bytes());
    prog.extend(11u32.to_le_bytes());
    prog.extend((init.len() as u32).to_le_bytes());
    prog.extend((reduce.len() as u32).to_le_bytes());
    prog.extend(init);
    prog.extend(reduce);
    prog
}

fn fresh_state(prog: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("program admits");
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("state initializes");
    state
}

/// Stage every datom of `rows` in the probe slot under `staged_key` of its
/// arrival position, the key the `0x2f` element column carries.
fn stage(state: &mut [u8], rows: &[Vec<Datom>]) {
    let probe = StructMapSlot::bind(state, PROBE_SLOT);
    for (at, d) in rows.iter().flatten().enumerate() {
        let up = probe
            .upsert(state, staged_key(at))
            .expect("probe slot has room");
        let row = probe.row_off(up.pos);
        probe.clear_bitset(state, row);
        let cells = [
            (0, d.route),
            (1, u32::from(d.retract)),
            (2, d.key),
            (3, d.v_str),
            (5, d.v_set),
        ];
        for (field, cell) in cells {
            StructMapSlot::set_field_bit(state, row, field);
            bytes::write_u32(state, row + probe.field_offset(state, field), cell);
        }
        StructMapSlot::set_field_bit(state, row, 4);
        bytes::write_u64(state, row + probe.field_offset(state, 4), d.v_num.to_bits());
    }
}

/// The probe-slot key of the datom at arrival position `at`.
fn staged_key(at: usize) -> u32 {
    u32::try_from(at + 1).expect("a test batch fits u32")
}

/// One reduce call over `rows`, each row one parent of the FLAT_MAP; `first`
/// is the arrival position of the call's first datom.
fn reduce(vm: &mut Vm, state: &mut [u8], prog: &[u8], rows: &[&[Datom]], first: usize) -> u32 {
    let types = vec![T; rows.len()];
    let mut offsets = vec![0u32];
    let mut end = 0u32;
    for row in rows {
        end += row.len() as u32;
        offsets.push(end);
    }
    let datoms: Vec<Datom> = rows.iter().flat_map(|row| row.iter().copied()).collect();
    let staged: Vec<u32> = (first..first + datoms.len()).map(staged_key).collect();
    let routes: Vec<u32> = datoms.iter().map(|d| d.route).collect();
    let ops: Vec<u32> = datoms.iter().map(|d| u32::from(d.retract)).collect();
    let keys: Vec<u32> = datoms.iter().map(|d| d.key).collect();
    let v_strs: Vec<u32> = datoms.iter().map(|d| d.v_str).collect();
    let v_nums: Vec<f64> = datoms.iter().map(|d| d.v_num).collect();
    let v_sets: Vec<u32> = datoms.iter().map(|d| d.v_set).collect();
    let at_ns: Vec<i64> = datoms.iter().map(|d| d.at_ns).collect();
    let ordinals: Vec<i64> = datoms.iter().map(|d| d.ordinal).collect();
    let cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&offsets),
        u32s_as_bytes(&routes),
        u32s_as_bytes(&ops),
        u32s_as_bytes(&keys),
        u32s_as_bytes(&v_strs),
        f64s_as_bytes(&v_nums),
        u32s_as_bytes(&v_sets),
        i64s_as_bytes(&at_ns),
        i64s_as_bytes(&ordinals),
        u32s_as_bytes(&staged),
    ];
    vm.execute_batch(state, prog, &cols, rows.len() as u32)
}

/// A state image with each slot's change flags cleared: they record what the
/// last call changed, which the batch's one call and the fold's last call
/// legitimately disagree on. Every other byte is the state.
fn settled(mut state: Vec<u8>) -> Vec<u8> {
    for slot in 0..NUM_SLOTS {
        state
            [(STATE_HEADER_SIZE + slot * SLOT_META_SIZE + SlotMetaOffset::CHANGE_FLAGS) as usize] =
            0;
    }
    state
}

/// The state after one reduce over every row, and after one reduce per row.
/// Both start from the same staged probe slot.
fn batch_and_fold(form: Scatter, rows: &[Vec<Datom>]) -> (Vec<u8>, Vec<u8>) {
    let prog = scatter_program(form);
    let start = || {
        let mut state = fresh_state(&prog);
        stage(&mut state, rows);
        state
    };
    let refs: Vec<&[Datom]> = rows.iter().map(Vec::as_slice).collect();
    let mut batch = start();
    assert_eq!(OK, reduce(&mut Vm::default(), &mut batch, &prog, &refs, 0));
    let mut fold = start();
    let mut vm = Vm::default();
    let mut first = 0;
    for row in &refs {
        assert_eq!(OK, reduce(&mut vm, &mut fold, &prog, &[row], first));
        first += row.len();
    }
    (settled(batch), settled(fold))
}

/// The field at `field_at` of `key`'s row: `None` while its bit is clear.
fn field(state: &[u8], key: u32, field: u8, field_at: u32) -> Option<u64> {
    let row = CheckedStructMap::bind(state, 0)
        .expect("struct-map slot")
        .row(key)?;
    StructMapSlot::is_field_set(state, row, field).then(|| match field_at {
        STATUS_AT | LANDED_AT => u64::from(bytes::read_u32(state, row + field_at)),
        _ => bytes::read_u64(state, row + field_at),
    })
}

fn status(state: &[u8], key: u32) -> Option<u64> {
    field(state, key, 0, STATUS_AT)
}

fn landed(state: &[u8], key: u32) -> Option<u64> {
    field(state, key, 1, LANDED_AT)
}

fn boosted(state: &[u8], key: u32) -> Option<f64> {
    field(state, key, 2, BOOST_AT).map(f64::from_bits)
}

fn guard_of(state: &[u8], key: u32) -> (Option<u64>, Option<u64>) {
    (
        field(state, key, 3, GUARD_AT_NS_AT),
        field(state, key, 4, GUARD_ORDINAL_AT),
    )
}

fn has_dep(state: &[u8], elem: u32) -> bool {
    CheckedSet::bind(state, 1).expect("set slot").contains(elem)
}

struct Case {
    name: &'static str,
    forms: &'static [Scatter],
    rows: Vec<Vec<Datom>>,
    expect: fn(&[u8]),
}

fn cases() -> Vec<Case> {
    vec![
        // [commit A@07 reverting X, U reverts A, commit A@09 reverting Y]:
        // a commit asserts its own status OPEN through STATUS and its target's
        // CLOSED through REVERTED, two routes onto one field. The last datom to
        // arrive decides the field, whichever route carried it.
        Case {
            name: "revert chain, two routes onto one field",
            forms: UNGUARDED,
            rows: vec![
                vec![
                    assert(STATUS, A, OPEN),
                    assert(LANDED, A, AT_07),
                    assert(REVERTED, X, CLOSED),
                ],
                vec![assert(STATUS, U, OPEN), assert(REVERTED, A, CLOSED)],
                vec![
                    assert(STATUS, A, OPEN),
                    assert(LANDED, A, AT_09),
                    assert(REVERTED, Y, CLOSED),
                ],
            ],
            expect: |s| {
                assert_eq!(Some(u64::from(OPEN)), status(s, A));
                assert_eq!(Some(u64::from(AT_09)), landed(s, A));
                assert_eq!(Some(u64::from(OPEN)), status(s, U));
                assert_eq!(Some(u64::from(CLOSED)), status(s, X));
                assert_eq!(Some(u64::from(CLOSED)), status(s, Y));
            },
        },
        Case {
            name: "two asserts to one key and field",
            forms: UNGUARDED,
            rows: vec![
                vec![assert(STATUS, A, CLOSED)],
                vec![assert(STATUS, A, OPEN)],
            ],
            expect: |s| assert_eq!(Some(u64::from(OPEN)), status(s, A)),
        },
        Case {
            name: "assert, retract, assert",
            forms: UNGUARDED,
            rows: vec![
                vec![assert(LANDED, A, AT_07)],
                vec![retract(LANDED, A, AT_07)],
                vec![assert(LANDED, A, AT_09)],
            ],
            expect: |s| assert_eq!(Some(u64::from(AT_09)), landed(s, A)),
        },
        // The retract names a value an earlier row wrote and a later row already
        // replaced, so at its own position it matches nothing and clears nothing.
        Case {
            name: "retract of a superseded value",
            forms: UNGUARDED,
            rows: vec![
                vec![assert(LANDED, A, AT_07)],
                vec![assert(LANDED, A, AT_09)],
                vec![retract(LANDED, A, AT_07)],
            ],
            expect: |s| assert_eq!(Some(u64::from(AT_09)), landed(s, A)),
        },
        Case {
            name: "routes interleaved on one key",
            forms: UNGUARDED,
            rows: vec![
                vec![assert(STATUS, A, CLOSED), assert(LANDED, A, AT_07)],
                vec![
                    assert(LANDED, A, AT_09),
                    assert(STATUS, A, OPEN),
                    boost(A, 1.5),
                ],
                vec![
                    dep(false, A, 8001),
                    assert(STATUS, A, CLOSED),
                    boost(A, 2.5),
                ],
                vec![dep(true, A, 8001), retract(STATUS, A, OPEN), boost(A, 0.5)],
            ],
            expect: |s| {
                assert_eq!(Some(u64::from(CLOSED)), status(s, A));
                assert_eq!(Some(u64::from(AT_09)), landed(s, A));
                assert_eq!(Some(0.5), boosted(s, A));
                assert!(!has_dep(s, 8001));
            },
        },
        Case {
            name: "keys never repeat",
            forms: UNGUARDED,
            rows: vec![
                vec![assert(STATUS, A, CLOSED), assert(LANDED, U, AT_07)],
                vec![assert(STATUS, Y, OPEN), dep(false, A, 8001)],
                vec![boost(U, 1.5), retract(STATUS, X, OPEN)],
            ],
            expect: |s| {
                assert_eq!(Some(u64::from(CLOSED)), status(s, A));
                assert_eq!(Some(u64::from(AT_07)), landed(s, U));
                assert_eq!(Some(u64::from(OPEN)), status(s, Y));
                assert_eq!(Some(1.5), boosted(s, U));
                assert!(has_dep(s, 8001));
            },
        },
        // Row 2 is older than the guard row 1 stamped earlier in the same
        // batch: it writes nothing by any route. Row 3 is newer and its
        // retract and assert both land. Row 4 is older than row 3, so its
        // retract clears nothing.
        Case {
            name: "guard stamped by an earlier row of the batch",
            forms: &[Scatter::Guarded],
            rows: vec![
                guarded(
                    2000,
                    20,
                    &[assert(STATUS, A, CLOSED), assert(LANDED, A, AT_09)],
                ),
                guarded(
                    1000,
                    21,
                    &[
                        assert(STATUS, A, OPEN),
                        assert(LANDED, A, AT_07),
                        dep(false, A, 8001),
                    ],
                ),
                guarded(
                    3000,
                    22,
                    &[retract(LANDED, A, AT_09), assert(STATUS, A, OPEN)],
                ),
                guarded(2500, 23, &[retract(STATUS, A, OPEN)]),
            ],
            expect: |s| {
                assert_eq!(Some(u64::from(OPEN)), status(s, A));
                assert_eq!(None, landed(s, A));
                assert!(!has_dep(s, 8001));
                assert_eq!((Some(3000), Some(22)), guard_of(s, A));
            },
        },
        Case {
            name: "guarded: two transactions to one key and field",
            forms: &[Scatter::Guarded],
            rows: vec![
                guarded(1000, 1, &[assert(STATUS, A, CLOSED)]),
                guarded(1000, 2, &[assert(STATUS, A, OPEN)]),
            ],
            expect: |s| assert_eq!(Some(u64::from(OPEN)), status(s, A)),
        },
    ]
}

#[test]
fn one_reduce_over_a_batch_equals_the_per_row_fold() {
    for case in cases() {
        for &form in case.forms {
            let (batch, fold) = batch_and_fold(form, &case.rows);
            (case.expect)(&fold);
            assert!(
                batch == fold,
                "{} ({form:?}): one reduce over the batch diverges from the per-row fold",
                case.name
            );
        }
    }
}

/// A random datom over a small domain, so keys, fields and values collide.
fn any_datom() -> impl Strategy<Value = Datom> {
    (
        prop_oneof![
            Just(STATUS),
            Just(LANDED),
            Just(DEP),
            Just(BOOST),
            Just(REVERTED),
            Just(SKIP)
        ],
        any::<bool>(),
        prop_oneof![Just(A), Just(U), Just(Y)],
        prop_oneof![Just(CLOSED), Just(OPEN), Just(AT_07)],
        prop_oneof![Just(0.5), Just(1.5)],
        prop_oneof![Just(8001u32), Just(8002u32)],
    )
        .prop_map(|(route, retract, key, v_str, v_num, v_set)| Datom {
            route,
            retract,
            key,
            v_str,
            v_num,
            v_set,
            at_ns: 0,
            ordinal: 0,
        })
}

/// Rows of datoms; each row is one transaction with its own guard: a random
/// instant and a unique ordinal (the emitter's no-tie precondition).
fn any_rows() -> impl Strategy<Value = Vec<Vec<Datom>>> {
    prop::collection::vec((0i64..4, prop::collection::vec(any_datom(), 1..5)), 1..7).prop_map(
        |rows| {
            rows.into_iter()
                .enumerate()
                .map(|(ordinal, (at, datoms))| guarded(at * 1000, ordinal as i64, &datoms))
                .collect()
        },
    )
}

proptest! {
    #[test]
    fn probe_scatter_batch_equals_fold(rows in any_rows()) {
        let (batch, fold) = batch_and_fold(Scatter::Probe, &rows);
        prop_assert!(batch == fold);
    }

    #[test]
    fn plain_scatter_batch_equals_fold(rows in any_rows()) {
        let (batch, fold) = batch_and_fold(Scatter::Plain, &rows);
        prop_assert!(batch == fold);
    }

    #[test]
    fn guarded_scatter_batch_equals_fold(rows in any_rows()) {
        let (batch, fold) = batch_and_fold(Scatter::Guarded, &rows);
        prop_assert!(batch == fold);
    }
}
