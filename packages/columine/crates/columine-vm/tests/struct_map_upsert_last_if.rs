//! `BATCH_STRUCT_MAP_UPSERT_LAST_IF` (0x8f), the struct-map record form of
//! `BATCH_MAP_UPSERT_LAST_IF` (`specs/columine/06-struct-map-upsert-last-if.md`):
//! a `FOR_EACH`/`FLAT_MAP` body instruction whose predicate cell is read at
//! the element's own position, after the rows before it landed. A live
//! predicate sees the destination's stored row for the element's key, or
//! `StoredBinding::Absent` when the key holds none. Nonzero writes the row as
//! 0x80 does; zero writes nothing, so an absent key stays absent.
//!
//! The fixture is the lease-winner fold. The embedder's predicate below is the
//! authored expression
//!
//! ```text
//! (incoming.action == 'claim' && (_ == null || _.action == 'release'
//!     || _.holder == incoming.holder || incoming.atMicros > _.untilMicros))
//! || (incoming.action != 'claim' && _ != null && _.action != 'release'
//!     && _.holder == incoming.holder)
//! ```
//!
//! over typed field reads: the interned action and holder compare by id, the
//! INT64 microsecond times exactly, at values past 2^53 where an f64 widening
//! would tie one microsecond to the next.

mod common;

use std::collections::BTreeMap;

use columine_types::PROGRAM_MAGIC;
use columine_types::operand::encode_index;
use columine_types::types::{
    ErrorCode, NO_PARENT_TS_COL, Opcode, SlotMetaOffset, SlotType, StructFieldType,
};
use columine_vm::key_index::NoStrings;
use columine_vm::meta::slot_meta_base;
use columine_vm::row_exprs::{
    BatchView, Binding, ROW_EXPRESSIONS_MAGIC, RowColumns, RowExpression, StoredBinding, StoredRow,
    bind_row_columns,
};
use columine_vm::state_init::{
    DEFAULT_ACCEPTED_PROGRAM_MAGICS, calculate_grown_state_size, calculate_state_size, init_state,
};
use columine_vm::vm::{Vm, col_i64_exact, col_u32_exact, i64s_as_bytes, u32s_as_bytes};
use common::memory::with_allocations;

const OK: u32 = ErrorCode::Ok as u32;
const NEEDS_GROWTH: u32 = ErrorCode::NeedsGrowth as u32;
const INVALID_PROGRAM: u32 = ErrorCode::InvalidProgram as u32;

/// Input columns: event type, lease key, action, holder, atMicros,
/// untilMicros, and the predicate the row table derives.
const TYPE_COL: u8 = 0;
const KEY_COL: u8 = 1;
const ACTION_COL: u8 = 2;
const HOLDER_COL: u8 = 3;
const AT_COL: u8 = 4;
const UNTIL_COL: u8 = 5;
const PRED_COL: u8 = 6;
const INPUTS: u8 = 7;

/// Fields of the lease record.
const ACTION: u8 = 0;
const HOLDER: u8 = 1;
const AT: u8 = 2;
const UNTIL: u8 = 3;

const LEASE_EVENT: u32 = 0x0001_EA5E;
const DROP_EVENT: u32 = 0x0000_D20B;

/// Interned ids: the action enum and two holders.
const CLAIM: u32 = 101;
const RENEW: u32 = 102;
const RELEASE: u32 = 103;
const ALICE: u32 = 201;
const BOB: u32 = 202;

/// 2^53: from here on, consecutive microsecond times share one f64
/// (`2^53 + 1` rounds to `2^53`).
const T: i64 = 1 << 53;

/// The row table's one expression: the lease-winner predicate.
const LEASE_WINNER: u8 = 0x4C;

fn len_u16(len: usize) -> [u8; 2] {
    u16::try_from(len).expect("length fits u16").to_le_bytes()
}

fn len_u32(len: usize) -> [u8; 4] {
    u32::try_from(len).expect("length fits u32").to_le_bytes()
}

fn program(num_inputs: u8, capacity: u8, reduce: &[u8], trailer: &[u8]) -> Vec<u8> {
    let init = [
        Opcode::SlotStructMap as u8,
        0,
        SlotType::StructMap as u8,
        capacity,
        0,
        4,
        StructFieldType::String as u8,
        StructFieldType::String as u8,
        StructFieldType::Int64 as u8,
        StructFieldType::Int64 as u8,
        Opcode::Halt as u8,
    ];
    let mut program = vec![0u8; 32];
    program.extend(PROGRAM_MAGIC.to_le_bytes());
    program.extend([2, 0, 0, 0]); // version 2, no callbacks, no flags
    program.extend(1u32.to_le_bytes());
    program.extend(u32::from(num_inputs).to_le_bytes());
    program.extend(len_u32(init.len()));
    program.extend(len_u32(reduce.len()));
    program.extend(init);
    program.extend_from_slice(reduce);
    program.extend_from_slice(trailer);
    program
}

/// `0x8f slot:0 key num_vals:4 (col, field) × 4 num_arrays:0 pred`, over
/// `[key, action, holder, at, until, pred]`.
fn upsert_last_if([key, action, holder, at, until, pred]: [u8; 6]) -> Vec<u8> {
    vec![
        Opcode::BatchStructMapUpsertLastIf as u8,
        0,
        key,
        4,
        action,
        ACTION,
        holder,
        HOLDER,
        at,
        AT,
        until,
        UNTIL,
        0,
        pred,
    ]
}

const LEASE_COLS: [u8; 6] = [KEY_COL, ACTION_COL, HOLDER_COL, AT_COL, UNTIL_COL, PRED_COL];

fn for_each(event: u32, body: &[u8]) -> Vec<u8> {
    let mut out = vec![Opcode::ForEach as u8, TYPE_COL, 1];
    out.extend(event.to_le_bytes());
    out.extend(len_u16(body.len()));
    out.extend_from_slice(body);
    out
}

/// The row table deriving `PRED_COL` from the lease-winner predicate for
/// every lease row.
fn lease_table() -> Vec<u8> {
    let mut out = ROW_EXPRESSIONS_MAGIC.to_le_bytes().to_vec();
    out.extend(1u16.to_le_bytes());
    out.push(TYPE_COL);
    out.extend([PRED_COL, 1]);
    out.extend(LEASE_EVENT.to_le_bytes());
    out.extend(1u16.to_le_bytes());
    out.push(LEASE_WINNER);
    out
}

/// `FOR_EACH(lease) { 0x8f }` with the live lease-winner predicate.
fn lease_program(capacity: u8) -> Vec<u8> {
    program(
        INPUTS,
        capacity,
        &for_each(LEASE_EVENT, &upsert_last_if(LEASE_COLS)),
        &lease_table(),
    )
}

/// The lease fold behind a removal of the same slot: `FOR_EACH(drop) {
/// remove }` precedes the producer, as log-order removal requires.
fn cut_program() -> Vec<u8> {
    let mut reduce = for_each(
        DROP_EVENT,
        &[Opcode::BatchStructMapRemove as u8, 0, KEY_COL],
    );
    reduce.extend(for_each(LEASE_EVENT, &upsert_last_if(LEASE_COLS)));
    program(INPUTS, 16, &reduce, &lease_table())
}

fn init(program: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(program, DEFAULT_ACCEPTED_PROGRAM_MAGICS)
        .expect("valid program admission");
    let mut state = vec![0u8; usize::try_from(size).expect("state size fits usize")];
    init_state(&mut state, program, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("init state");
    state
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Lease {
    action: u32,
    holder: u32,
    at: i64,
    until: i64,
}

#[derive(Clone, Copy, Debug)]
struct Row {
    event: u32,
    key: u32,
    lease: Lease,
}

fn row(key: u32, action: u32, holder: u32, at: i64, until: i64) -> Row {
    Row {
        event: LEASE_EVENT,
        key,
        lease: Lease {
            action,
            holder,
            at,
            until,
        },
    }
}

fn claim(key: u32, holder: u32, at: i64, until: i64) -> Row {
    row(key, CLAIM, holder, at, until)
}

fn renew(key: u32, holder: u32, at: i64, until: i64) -> Row {
    row(key, RENEW, holder, at, until)
}

fn release(key: u32, holder: u32, at: i64) -> Row {
    row(key, RELEASE, holder, at, at)
}

fn drop_key(key: u32) -> Row {
    Row {
        event: DROP_EVENT,
        key,
        lease: Lease {
            action: 0,
            holder: 0,
            at: 0,
            until: 0,
        },
    }
}

/// `_` as the predicate reads it: absent, or a record each of whose fields
/// may be unset.
#[derive(Clone, Copy, Debug, Default)]
struct Current {
    action: Option<u32>,
    holder: Option<u32>,
    until: Option<i64>,
}

impl From<Lease> for Current {
    fn from(lease: Lease) -> Self {
        Self {
            action: Some(lease.action),
            holder: Some(lease.holder),
            until: Some(lease.until),
        }
    }
}

/// The authored predicate. A field read on a null `_` is null, and every
/// comparison with null is false except `!=`.
fn lease_winner(current: Option<Current>, incoming: Lease) -> bool {
    let action = current.and_then(|c| c.action);
    let holder = current.and_then(|c| c.holder);
    let until = current.and_then(|c| c.until);
    if incoming.action == CLAIM {
        current.is_none()
            || action == Some(RELEASE)
            || holder == Some(incoming.holder)
            || until.is_some_and(|until| incoming.at > until)
    } else {
        current.is_some() && action != Some(RELEASE) && holder == Some(incoming.holder)
    }
}

/// An interned STRING field: its id, or null when unset. A field of any
/// other physical type is a mistyped read.
fn interned(row: &StoredRow<'_>, field: u8) -> Result<Option<u32>, ErrorCode> {
    if row.field_type(field)? != StructFieldType::String {
        return Err(ErrorCode::InvalidCellValue);
    }
    row.field(field)?
        .map(|cell| {
            cell.try_into()
                .map(u32::from_le_bytes)
                .map_err(|_| ErrorCode::InvalidState)
        })
        .transpose()
}

/// An INT64 microsecond field, read exactly: no f64 on the way.
fn micros(row: &StoredRow<'_>, field: u8) -> Result<Option<i64>, ErrorCode> {
    if row.field_type(field)? != StructFieldType::Int64 {
        return Err(ErrorCode::InvalidCellValue);
    }
    row.field(field)?
        .map(|cell| {
            cell.try_into()
                .map(i64::from_le_bytes)
                .map_err(|_| ErrorCode::InvalidState)
        })
        .transpose()
}

/// The embedder's evaluator for `LEASE_WINNER`: live, because it reads `_`.
#[derive(Default)]
struct LeaseWinner {
    evaluated: usize,
    absent: usize,
}

impl RowExpression for LeaseWinner {
    fn admit(&mut self, expr: &[u8], batch: &BatchView<'_>) -> Result<Binding, ErrorCode> {
        if expr != [LEASE_WINNER] || !matches!(batch.stored(), StoredBinding::Unbound) {
            return Err(ErrorCode::InvalidProgram);
        }
        for col in [ACTION_COL, HOLDER_COL] {
            col_u32_exact(batch.column(u32::from(col)), batch.batch_len)
                .ok_or(ErrorCode::ColumnUnderrun)?;
        }
        col_i64_exact(batch.column(u32::from(AT_COL)), batch.batch_len)
            .ok_or(ErrorCode::ColumnUnderrun)?;
        Ok(Binding::Live)
    }

    fn eval(&mut self, _: &[u8], batch: &BatchView<'_>, row: u32) -> Result<u32, ErrorCode> {
        self.evaluated += 1;
        let current = match batch.stored() {
            // A conditional upsert always binds `_`; an unbound one is a
            // program the predicate cannot answer.
            StoredBinding::Unbound => return Err(ErrorCode::InvalidProgram),
            StoredBinding::Absent => {
                self.absent += 1;
                None
            }
            StoredBinding::Row(stored) => Some(Current {
                action: interned(stored, ACTION)?,
                holder: interned(stored, HOLDER)?,
                until: micros(stored, UNTIL)?,
            }),
        };
        let at = usize::try_from(row).map_err(|_| ErrorCode::InvalidProgram)?;
        let u32_cell = |col: u8| {
            col_u32_exact(batch.column(u32::from(col)), batch.batch_len)
                .and_then(|cells| cells.get(at).copied())
                .ok_or(ErrorCode::ColumnUnderrun)
        };
        let incoming = Lease {
            action: u32_cell(ACTION_COL)?,
            holder: u32_cell(HOLDER_COL)?,
            at: col_i64_exact(batch.column(u32::from(AT_COL)), batch.batch_len)
                .and_then(|cells| cells.get(at).copied())
                .ok_or(ErrorCode::ColumnUnderrun)?,
            // The predicate reads no incoming until.
            until: 0,
        };
        Ok(u32::from(lease_winner(current, incoming)))
    }
}

/// One batch's input columns, in the program's order.
struct Batch {
    types: Vec<u32>,
    keys: Vec<u32>,
    actions: Vec<u32>,
    holders: Vec<u32>,
    at: Vec<i64>,
    until: Vec<i64>,
}

impl Batch {
    fn of(rows: &[Row]) -> Self {
        Self {
            types: rows.iter().map(|r| r.event).collect(),
            keys: rows.iter().map(|r| r.key).collect(),
            actions: rows.iter().map(|r| r.lease.action).collect(),
            holders: rows.iter().map(|r| r.lease.holder).collect(),
            at: rows.iter().map(|r| r.lease.at).collect(),
            until: rows.iter().map(|r| r.lease.until).collect(),
        }
    }

    /// The host's columns; the predicate column is the bind's to fill.
    fn cols(&self) -> [&[u8]; INPUTS as usize] {
        [
            u32s_as_bytes(&self.types),
            u32s_as_bytes(&self.keys),
            u32s_as_bytes(&self.actions),
            u32s_as_bytes(&self.holders),
            i64s_as_bytes(&self.at),
            i64s_as_bytes(&self.until),
            &[],
        ]
    }

    fn len(&self) -> u32 {
        u32::try_from(self.types.len()).expect("batch fits u32")
    }
}

/// Bind `rows` and run them as one batch on the delta lane.
fn execute(
    vm: &mut Vm,
    state: &mut [u8],
    program: &[u8],
    rows: &[Row],
    eval: &mut LeaseWinner,
    store: &mut RowColumns,
) -> u32 {
    let batch = Batch::of(rows);
    let mut cols = batch.cols();
    let len = batch.len();
    let mut live = bind_row_columns(program, state, &mut cols, len, eval, store)
        .expect("bind the lease predicate");
    vm.execute_batch_delta_live(state, program, &cols, len, &mut live)
}

/// The lease `key` stores, exactly as its fields hold it.
fn stored(state: &[u8], key: u32) -> Option<Lease> {
    let row = StoredRow::lookup(state, 0, key, None).expect("lease slot")?;
    let field = |field: u8| {
        row.field(field)
            .expect("lease field")
            .expect("every write maps all four fields")
    };
    Some(Lease {
        action: u32::from_le_bytes(field(ACTION).try_into().expect("u32 cell")),
        holder: u32::from_le_bytes(field(HOLDER).try_into().expect("u32 cell")),
        at: i64::from_le_bytes(field(AT).try_into().expect("i64 cell")),
        until: i64::from_le_bytes(field(UNTIL).try_into().expect("i64 cell")),
    })
}

/// The specification as a sequential fold: a lease row writes iff the
/// predicate admits it against what its key holds; a drop removes the key.
fn model(rows: &[Row]) -> BTreeMap<u32, Lease> {
    let mut leases = BTreeMap::new();
    for row in rows {
        if row.event == DROP_EVENT {
            leases.remove(&row.key);
        } else if lease_winner(leases.get(&row.key).copied().map(Current::from), row.lease) {
            leases.insert(row.key, row.lease);
        }
    }
    leases
}

/// The image as a later batch reads it: sealed, change flags cleared.
fn settled(vm: &mut Vm, mut state: Vec<u8>) -> Vec<u8> {
    vm.seal_state(&mut state, &NoStrings).expect("seal");
    state[(slot_meta_base(0) + SlotMetaOffset::CHANGE_FLAGS) as usize] = 0;
    state
}

fn change_flags(state: &[u8]) -> u8 {
    state[(slot_meta_base(0) + SlotMetaOffset::CHANGE_FLAGS) as usize]
}

fn clear_change_flags(state: &mut [u8]) {
    state[(slot_meta_base(0) + SlotMetaOffset::CHANGE_FLAGS) as usize] = 0;
}

/// Fold `rows` from an empty image as one batch, or one batch per row.
fn fold(program: &[u8], rows: &[Row], split: bool) -> (Vm, Vec<u8>) {
    let mut vm = Vm::default();
    let mut state = init(program);
    let mut eval = LeaseWinner::default();
    let mut store = RowColumns::new();
    if split {
        for row in rows {
            let status = execute(
                &mut vm,
                &mut state,
                program,
                core::slice::from_ref(row),
                &mut eval,
                &mut store,
            );
            assert_eq!(status, OK, "row {row:?}");
        }
    } else {
        let status = execute(&mut vm, &mut state, program, rows, &mut eval, &mut store);
        assert_eq!(status, OK, "batch {rows:?}");
    }
    (vm, state)
}

/// One batch of the rows and one batch per row store the model's leases in
/// byte-identical sealed images.
fn assert_batchings_agree(program: &[u8], rows: &[Row]) -> BTreeMap<u32, Lease> {
    let expected = model(rows);
    let (mut merged_vm, merged) = fold(program, rows, false);
    let (mut split_vm, split) = fold(program, rows, true);
    let keys: Vec<u32> = rows.iter().map(|r| r.key).collect();
    for key in keys {
        let want = expected.get(&key).copied();
        assert_eq!(stored(&merged, key), want, "one batch, key {key}: {rows:?}");
        assert_eq!(
            stored(&split, key),
            want,
            "one batch per row, key {key}: {rows:?}"
        );
    }
    assert_eq!(
        settled(&mut merged_vm, merged),
        settled(&mut split_vm, split),
        "one batch of n rows leaves the image n batches of one row leave: {rows:?}"
    );
    expected
}

/// The lease-winner transition matrix: each scenario on its own key, with the
/// row it must leave stored (`None`: the key stays absent).
fn matrix() -> Vec<(&'static str, Vec<Row>, Option<usize>)> {
    let u = T;
    vec![
        (
            "claim on empty writes",
            vec![claim(1, ALICE, u - 10, u)],
            Some(0),
        ),
        (
            "renew on empty stays absent",
            vec![renew(2, ALICE, u - 10, u)],
            None,
        ),
        (
            "release on empty stays absent",
            vec![release(3, ALICE, u - 10)],
            None,
        ),
        (
            "foreign claim at until is inert",
            vec![claim(4, ALICE, u - 10, u), claim(4, BOB, u, u + 5)],
            Some(0),
        ),
        (
            "foreign claim before until is inert",
            vec![claim(5, ALICE, u - 10, u), claim(5, BOB, u - 1, u + 5)],
            Some(0),
        ),
        (
            "foreign claim one microsecond past until takes over",
            vec![claim(6, ALICE, u - 10, u), claim(6, BOB, u + 1, u + 9)],
            Some(1),
        ),
        (
            "holder re-claim writes",
            vec![claim(7, ALICE, u - 10, u), claim(7, ALICE, u - 5, u + 7)],
            Some(1),
        ),
        (
            "holder renew writes",
            vec![claim(8, ALICE, u - 10, u), renew(8, ALICE, u - 5, u + 7)],
            Some(1),
        ),
        (
            "holder release writes",
            vec![claim(9, ALICE, u - 10, u), release(9, ALICE, u - 4)],
            Some(1),
        ),
        (
            "foreign renew is inert",
            vec![claim(10, ALICE, u - 10, u), renew(10, BOB, u - 5, u + 7)],
            Some(0),
        ),
        (
            "foreign renew after expiry is inert",
            vec![claim(11, ALICE, u - 10, u), renew(11, BOB, u + 1, u + 9)],
            Some(0),
        ),
        (
            "foreign release is inert",
            vec![claim(12, ALICE, u - 10, u), release(12, BOB, u - 5)],
            Some(0),
        ),
        (
            "foreign release after expiry is inert",
            vec![claim(13, ALICE, u - 10, u), release(13, BOB, u + 1)],
            Some(0),
        ),
        (
            "a released lease is acquired by a foreign claim before until",
            vec![
                claim(14, ALICE, u - 10, u),
                release(14, ALICE, u - 4),
                claim(14, BOB, u - 3, u + 3),
            ],
            Some(2),
        ),
        (
            "the holder renews after expiry",
            vec![claim(15, ALICE, u - 10, u), renew(15, ALICE, u + 1, u + 9)],
            Some(1),
        ),
        (
            "a renew after the holder's own release is inert",
            vec![
                claim(16, ALICE, u - 10, u),
                release(16, ALICE, u - 4),
                renew(16, ALICE, u - 3, u + 3),
            ],
            Some(1),
        ),
        (
            "an orphan renew leaves the key open for a claim",
            vec![renew(17, ALICE, u - 10, u), claim(17, BOB, u - 9, u + 1)],
            Some(1),
        ),
    ]
}

#[test]
fn the_lease_transition_matrix_holds_exactly_past_two_to_the_53() {
    let program = lease_program(64);
    let mut interleaved = Vec::new();
    for (name, rows, keeps) in matrix() {
        let want = keeps.map(|index| rows[index].lease);
        let key = rows[0].key;
        assert_eq!(model(&rows).get(&key).copied(), want, "model: {name}");
        let leases = assert_batchings_agree(&program, &rows);
        assert_eq!(leases.get(&key).copied(), want, "{name}");
        interleaved.push(rows);
    }
    // Every scenario at once, their rows interleaved in one batch: each key
    // folds only its own rows.
    let longest = interleaved.iter().map(Vec::len).max().unwrap_or(0);
    let mut rows = Vec::new();
    for step in 0..longest {
        rows.extend(
            interleaved
                .iter()
                .filter_map(|rows| rows.get(step).copied()),
        );
    }
    let (_, state) = fold(&program, &rows, false);
    for (name, scenario, keeps) in matrix() {
        let want = keeps.map(|index| scenario[index].lease);
        assert_eq!(stored(&state, scenario[0].key), want, "interleaved: {name}");
    }
}

#[test]
fn the_takeover_boundary_is_one_microsecond_that_f64_cannot_see() {
    // 2^53 + 1 is not an f64: it rounds to 2^53, the stored until. A
    // widened comparison would call the claim simultaneous and refuse it.
    #[allow(clippy::cast_precision_loss)]
    let widened = (((T + 1) as f64).to_bits(), (T as f64).to_bits());
    assert_eq!(widened.0, widened.1);
    let rows = [claim(1, ALICE, T - 10, T), claim(1, BOB, T + 1, T + 3)];
    let (_, state) = fold(&lease_program(8), &rows, false);
    assert_eq!(
        stored(&state, 1),
        Some(Lease {
            action: CLAIM,
            holder: BOB,
            at: T + 1,
            until: T + 3,
        })
    );
}

/// Every sequence of up to three lease rows over two holders, the three
/// actions and times straddling 2^53 folds to the model's lease, as one batch
/// and as one batch per row, into byte-identical images.
#[test]
fn every_short_sequence_folds_alike_in_one_batch_and_one_batch_per_row() {
    let program = lease_program(8);
    let mut alphabet = Vec::new();
    for action in [CLAIM, RENEW, RELEASE] {
        for holder in [ALICE, BOB] {
            for at in [T - 1, T, T + 1] {
                alphabet.push(row(1, action, holder, at, at + 1));
            }
        }
    }
    let mut sequences: Vec<Vec<Row>> = alphabet.iter().map(|&r| vec![r]).collect();
    let mut frontier = sequences.clone();
    for _ in 1..3 {
        frontier = frontier
            .iter()
            .flat_map(|prefix| {
                alphabet.iter().map(move |&next| {
                    let mut rows = prefix.clone();
                    rows.push(next);
                    rows
                })
            })
            .collect();
        sequences.extend(frontier.iter().cloned());
    }
    assert_eq!(sequences.len(), 18 + 18 * 18 + 18 * 18 * 18);
    for rows in &sequences {
        assert_batchings_agree(&program, rows);
    }
}

/// `_` is the destination's row for the element's key as the rows before it
/// left it: absent before the first write, the written row after it.
#[test]
fn each_element_reads_the_row_the_elements_before_it_left() {
    let program = lease_program(8);
    let mut vm = Vm::default();
    let mut state = init(&program);
    let mut eval = LeaseWinner::default();
    let mut store = RowColumns::new();
    let rows = [
        renew(1, ALICE, T, T + 1),
        claim(1, ALICE, T, T + 1),
        renew(1, ALICE, T + 1, T + 2),
        claim(2, BOB, T, T + 1),
    ];
    assert_eq!(
        execute(&mut vm, &mut state, &program, &rows, &mut eval, &mut store),
        OK
    );
    assert_eq!(
        eval.evaluated, 4,
        "one evaluation per element, at the element"
    );
    assert_eq!(
        eval.absent, 3,
        "key 1 twice before its claim landed, key 2 once"
    );
    assert_eq!(stored(&state, 1), Some(rows[2].lease));
    assert_eq!(stored(&state, 2), Some(rows[3].lease));
}

/// A row a removal cut covers is skipped before its predicate is evaluated;
/// the rows after the removal fold into a fresh key.
#[test]
fn a_removal_cut_skips_the_row_before_its_predicate() {
    let program = cut_program();
    for (rows, want) in [
        (
            vec![
                claim(1, ALICE, T, T + 9),
                drop_key(1),
                renew(1, ALICE, T + 1, T + 9),
            ],
            None,
        ),
        (
            vec![
                claim(1, ALICE, T, T + 9),
                drop_key(1),
                claim(1, BOB, T + 1, T + 2),
            ],
            Some(Lease {
                action: CLAIM,
                holder: BOB,
                at: T + 1,
                until: T + 2,
            }),
        ),
    ] {
        let leases = assert_batchings_agree(&program, &rows);
        assert_eq!(leases.get(&1).copied(), want);

        let mut vm = Vm::default();
        let mut state = init(&program);
        let mut eval = LeaseWinner::default();
        let mut store = RowColumns::new();
        assert_eq!(
            execute(&mut vm, &mut state, &program, &rows, &mut eval, &mut store),
            OK
        );
        assert_eq!(
            (eval.evaluated, eval.absent),
            (1, 1),
            "the cut row is never evaluated; the row after the removal sees an absent key"
        );
    }
}

/// Inert rows journal nothing and leave the image and its change flags as
/// they were; written rows roll back exactly and export a delta that replays
/// byte for byte.
#[test]
fn inert_rows_journal_nothing_and_written_rows_roll_back_exactly() {
    let program = lease_program(8);
    let mut vm = Vm::default();
    let mut state = init(&program);
    let mut eval = LeaseWinner::default();
    let mut store = RowColumns::new();
    let held = [claim(1, ALICE, T - 10, T)];
    assert_eq!(
        execute(&mut vm, &mut state, &program, &held, &mut eval, &mut store),
        OK
    );
    clear_change_flags(&mut state);

    vm.undo_enable(&state).expect("valid inactive window");
    let checkpoint = vm.undo_checkpoint();
    let before = state.clone();
    let inert = [
        renew(1, BOB, T + 1, T + 9),
        release(1, BOB, T + 1),
        claim(1, BOB, T, T + 9),
        renew(2, ALICE, T, T + 9),
        release(3, BOB, T),
    ];
    assert_eq!(
        execute(&mut vm, &mut state, &program, &inert, &mut eval, &mut store),
        OK
    );
    assert_eq!(state, before);
    assert_eq!(vm.undo_checkpoint(), checkpoint);
    assert_eq!(change_flags(&state), 0);

    let written = [renew(1, ALICE, T + 1, T + 9), claim(2, BOB, T + 2, T + 3)];
    assert_eq!(
        execute(
            &mut vm, &mut state, &program, &written, &mut eval, &mut store
        ),
        OK
    );
    assert_eq!(stored(&state, 1), Some(written[0].lease));
    assert_eq!(stored(&state, 2), Some(written[1].lease));
    assert!(vm.undo_checkpoint() > checkpoint);
    assert_ne!(change_flags(&state), 0);
    assert_exported_delta_replays(&mut vm, &before, &state, checkpoint);
    vm.undo_rollback(&mut state, checkpoint)
        .expect("roll back the written rows");
    assert_eq!(state, before);
}

/// Export `[checkpoint, end)` and replay both lanes byte-exact on a fresh VM.
fn assert_exported_delta_replays(vm: &mut Vm, before: &[u8], after: &[u8], checkpoint: u32) {
    let end = vm.undo_checkpoint();
    assert!(!vm.undo_status().overflow, "journal must stay exact");
    assert_eq!(
        Ok(end - checkpoint),
        vm.delta_export_segment(checkpoint, end)
    );
    let undo = vm.delta_export_undo().to_vec();
    let redo = vm.delta_export_redo().to_vec();
    let size = u32::try_from(before.len()).expect("state size fits u32");
    let entry_size = Vm::delta_export_entry_size();
    let mut replay = Vm::default();
    let mut rolled_back = after.to_vec();
    assert_eq!(
        Ok(size),
        replay.delta_apply_rollback_segment(&mut rolled_back, &undo, entry_size)
    );
    assert_eq!(rolled_back, before, "exported undo must restore the image");
    let mut rolled_forward = before.to_vec();
    assert_eq!(
        Ok(size),
        replay.delta_apply_rollforward_segment(&mut rolled_forward, &redo, entry_size)
    );
    assert_eq!(
        rolled_forward, after,
        "exported redo must reproduce the image"
    );
}

/// Run `rows` the way a host without a journal does: on `NEEDS_GROWTH` the
/// image is exactly as it was; grow the named slot, bind again and retry.
/// Answers how many growths the batch needed.
fn run_growing(
    vm: &mut Vm,
    state: &mut Vec<u8>,
    program: &[u8],
    rows: &[Row],
    eval: &mut LeaseWinner,
    store: &mut RowColumns,
) -> usize {
    for growths in 0..32 {
        let before = state.clone();
        match execute(vm, state, program, rows, eval, store) {
            OK => return growths,
            NEEDS_GROWTH => {
                assert_eq!(&before, state, "a refused batch wrote state");
                let slot = vm.needs_growth_slot().expect("NEEDS_GROWTH names its slot");
                let size = calculate_grown_state_size(state, slot).expect("grown size");
                let mut next = vec![0u8; usize::try_from(size).expect("size fits usize")];
                vm.grow_state(state, &mut next, slot).expect("grow");
                *state = next;
            }
            other => panic!("batch refused with {other}"),
        }
    }
    panic!("growth did not converge");
}

/// Claims on more keys than the slot holds refuse the whole batch before any
/// write, and the batch run again over the grown image stores the model's
/// leases; rows the predicate refuses never ask for capacity.
#[test]
fn growth_refuses_before_any_write_and_the_retry_folds_the_model() {
    let program = lease_program(4);
    let mut rows = Vec::new();
    for key in 1..=48u32 {
        rows.push(renew(key, ALICE, T, T + 1));
        rows.push(claim(key, ALICE, T, T + i64::from(key)));
        rows.push(claim(key, BOB, T + 1, T + 99));
        rows.push(renew(key, ALICE, T + 1, T + 2 * i64::from(key)));
    }
    let mut vm = Vm::default();
    let mut state = init(&program);
    let mut eval = LeaseWinner::default();
    let mut store = RowColumns::new();
    let growths = run_growing(&mut vm, &mut state, &program, &rows, &mut eval, &mut store);
    assert!(growths > 0, "48 keys outgrow a slot declared for 4");
    let expected = model(&rows);
    assert_eq!(expected.len(), 48);
    for (&key, &lease) in &expected {
        assert_eq!(stored(&state, key), Some(lease), "key {key}");
    }

    // Renews on absent keys write nothing, however many there are.
    let orphans: Vec<Row> = (1..=48u32).map(|key| renew(key, BOB, T, T + 1)).collect();
    let mut vm = Vm::default();
    let mut state = init(&program);
    let before = state.clone();
    assert_eq!(
        execute(
            &mut vm, &mut state, &program, &orphans, &mut eval, &mut store
        ),
        OK
    );
    assert_eq!(state, before);
    assert_eq!(
        vm.capacity_counts().dry_run_fit,
        1,
        "48 absent keys pass no bound; only the dry run's predicates clear them"
    );
}

/// After one warm-up batch, binding and running the same batch shape makes no
/// allocation: the predicate, the stored-row view and the row write borrow
/// what the VM and the row store already hold.
#[test]
fn a_warmed_batch_binds_evaluates_and_writes_without_allocating() {
    let program = lease_program(16);
    let mut vm = Vm::default();
    let mut state = init(&program);
    let mut eval = LeaseWinner::default();
    let mut store = RowColumns::new();
    let rows = [
        claim(1, ALICE, T - 10, T),
        renew(1, ALICE, T - 5, T + 5),
        claim(1, BOB, T + 1, T + 9),
        claim(2, BOB, T, T + 1),
        release(2, ALICE, T),
        renew(3, ALICE, T, T + 1),
    ];
    vm.undo_enable(&state).expect("valid inactive window");
    let checkpoint = vm.undo_checkpoint();
    assert_eq!(
        execute(&mut vm, &mut state, &program, &rows, &mut eval, &mut store),
        OK
    );
    vm.undo_rollback(&mut state, checkpoint)
        .expect("restore the warmed image");

    let batch = Batch::of(&rows);
    let mut cols = batch.cols();
    let len = batch.len();
    let status = with_allocations(0, || {
        let mut live = bind_row_columns(&program, &state, &mut cols, len, &mut eval, &mut store)
            .expect("a warmed bind allocates nothing");
        vm.execute_batch_delta_live(&mut state, &program, &cols, len, &mut live)
    });
    assert_eq!(status, OK);
    assert_eq!(stored(&state, 1), Some(rows[1].lease));
    assert_eq!(stored(&state, 2), Some(rows[3].lease));
    assert_eq!(stored(&state, 3), None);
}

/// A predicate the batch carries (no row table) decides the same way: the
/// cell at the element, nonzero writes, zero leaves the key as it is.
#[test]
fn a_bound_predicate_column_gates_each_element() {
    let program = program(
        INPUTS,
        8,
        &for_each(LEASE_EVENT, &upsert_last_if(LEASE_COLS)),
        &[],
    );
    let mut vm = Vm::default();
    let mut state = init(&program);
    let rows = [
        claim(1, ALICE, T, T + 1),
        claim(1, BOB, T, T + 2),
        claim(2, BOB, T, T + 3),
    ];
    let batch = Batch::of(&rows);
    let mut cols = batch.cols();
    let pred = [1u32, 0, 0];
    cols[usize::from(PRED_COL)] = u32s_as_bytes(&pred);
    assert_eq!(vm.execute_batch(&mut state, &program, &cols, 3), OK);
    assert_eq!(stored(&state, 1), Some(rows[0].lease));
    assert_eq!(
        stored(&state, 2),
        None,
        "a zero predicate leaves an absent key absent"
    );

    // A predicate column that does not cover the element is an underrun,
    // not a zero, refused before the element writes.
    let mut state = init(&program);
    let before = state.clone();
    let one = Batch::of(&rows[..1]);
    let mut cols = one.cols();
    cols[usize::from(PRED_COL)] = &[];
    assert_eq!(
        vm.execute_batch(&mut state, &program, &cols, 1),
        ErrorCode::ColumnUnderrun as u32
    );
    assert_eq!(state, before);
}

/// The trailing predicate operand is part of the instruction under
/// `FLAT_MAP` too: every child element decides on its own cell.
#[test]
fn flat_map_body_length_includes_the_predicate_column() {
    // Columns: type 0, offsets 1, then key, action, holder, at, until, pred.
    let body = upsert_last_if([2, 3, 4, 5, 6, 7]);
    let mut flat = vec![Opcode::FlatMap as u8, 1];
    flat.extend(encode_index(NO_PARENT_TS_COL).as_bytes());
    flat.extend(len_u16(body.len()));
    flat.extend(body);
    let program = program(8, 8, &for_each(LEASE_EVENT, &flat), &[]);
    let mut vm = Vm::default();
    let mut state = init(&program);
    let children = [
        claim(9, ALICE, T, T + 1),
        claim(9, BOB, T + 1, T + 2),
        renew(9, ALICE, T + 2, T + 3),
    ];
    let batch = Batch::of(&children);
    let types = [LEASE_EVENT];
    let offsets = [0u32, 3];
    let pred = [1u32, 0, 1];
    let child = batch.cols();
    let cols = [
        u32s_as_bytes(&types),
        u32s_as_bytes(&offsets),
        child[usize::from(KEY_COL)],
        child[usize::from(ACTION_COL)],
        child[usize::from(HOLDER_COL)],
        child[usize::from(AT_COL)],
        child[usize::from(UNTIL_COL)],
        u32s_as_bytes(&pred),
    ];
    assert_eq!(vm.execute_batch(&mut state, &program, &cols, 1), OK);
    assert_eq!(stored(&state, 9), Some(children[2].lease));
}

/// Like `BATCH_MAP_UPSERT_LAST_IF`, the instruction runs only in a body: at
/// top level it is refused before any write, and a live predicate there is
/// refused at bind.
#[test]
fn top_level_upsert_last_if_is_refused() {
    let reduce = upsert_last_if(LEASE_COLS);
    let plain = program(INPUTS, 8, &reduce, &[]);
    let mut vm = Vm::default();
    let mut state = init(&plain);
    let before = state.clone();
    let rows = [claim(1, ALICE, T, T + 1)];
    let batch = Batch::of(&rows);
    let mut cols = batch.cols();
    let pred = [1u32];
    cols[usize::from(PRED_COL)] = u32s_as_bytes(&pred);
    assert_eq!(
        vm.execute_batch(&mut state, &plain, &cols, 1),
        INVALID_PROGRAM
    );
    assert_eq!(state, before);

    let live = program(INPUTS, 8, &reduce, &lease_table());
    let state = init(&live);
    let mut cols = batch.cols();
    let mut eval = LeaseWinner::default();
    let mut store = RowColumns::new();
    assert_eq!(
        bind_row_columns(&live, &state, &mut cols, 1, &mut eval, &mut store)
            .err()
            .map(|e| e as u32),
        Some(INVALID_PROGRAM)
    );
}
