//! Wide row cells (specs/columine/07-wide-row-cells.md): a row-expression
//! entry in the f64 or i64 lane binds 8-byte cells, which the f64/i64
//! aggregates and the FLOAT64/INT64 struct-map fields read exactly as they
//! read a host column of that lane, whether the host sends one batch or one
//! batch per row. An RXP1 table binds every entry as u32, as before.

mod common;

use columine_types::PROGRAM_MAGIC;
use columine_types::types::{AggType, CmpType, ErrorCode, StructFieldType};
use columine_vm::aggregates::{agg_value_f64, agg_value_i64};
use columine_vm::column_uses::{ColumnUse, ReduceRead, ReduceReads};
use columine_vm::key_index::NoStrings;
use columine_vm::meta::SlotMetaView;
use columine_vm::row_exprs::{
    BatchView, Binding, LANED_ROW_EXPRESSIONS_MAGIC, ROW_EXPRESSIONS_MAGIC, RowCell, RowColumns,
    RowExpression, StoredRow, bind_row_columns,
};
use columine_vm::state_init::{DEFAULT_ACCEPTED_PROGRAM_MAGICS, calculate_state_size, init_state};
use columine_vm::vm::{CheckedMap, Vm, col_i64_exact, col_u32_exact, i64s_as_bytes, u32s_as_bytes};

use common::memory::with_allocations;
use proptest::prelude::*;

const OK: u32 = ErrorCode::Ok as u32;

/// Columns: 0 type, 1 key, 2 a u32 input `n`, 3 and 4 derived.
const TYPE_COL: u8 = 0;
const KEY_COL: u8 = 1;
const N_COL: u8 = 2;
const DERIVED: u8 = 3;
const DERIVED_2: u8 = 4;
const NUM_COLS: u32 = 5;
const ORDER_TYPE: u32 = 7;

/// 2^53: past it an f64 no longer holds every integer, so an i64 cell that
/// travelled as an f64 would round.
const TWO_53: i64 = 1 << 53;

/// Expression `[op, col]` over the row's cell of `col`: `1.5 * n` as an f64.
const HALF_MORE: u8 = 0;
/// `2^53 + n` as an i64.
const PAST_F64: u8 = 1;
/// An earlier i64 derived column plus one, as an i64.
const NEXT: u8 = 2;
/// `n` as a u32.
const COPY: u8 = 3;
/// Copy the u32 input through a live predicate rather than at bind.
const LIVE_COPY: u8 = 4;

/// The embedder of these tests. Each expression answers its own lane,
/// whatever lane the entry names, so a table naming another lane shows the
/// bind's refusal.
struct Wide;

impl RowExpression for Wide {
    fn admit(
        &mut self,
        expr: &[u8],
        _: CmpType,
        batch: &BatchView<'_>,
    ) -> Result<Binding, ErrorCode> {
        let &[op, col] = expr else {
            return Err(ErrorCode::InvalidProgram);
        };
        let col = u32::from(col);
        if col as usize >= batch.num_cols() {
            return Err(ErrorCode::InvalidProgram);
        }
        let covered = match op {
            HALF_MORE | PAST_F64 | COPY | LIVE_COPY => {
                col_u32_exact(batch.column(col), batch.batch_len).is_some()
            }
            NEXT if batch.derived_lane(col) == Some(CmpType::I64) => {
                col_i64_exact(batch.column(col), batch.batch_len).is_some()
            }
            _ => return Err(ErrorCode::InvalidProgram),
        };
        if covered {
            Ok(if op == LIVE_COPY {
                Binding::Live
            } else {
                Binding::Bound
            })
        } else {
            Err(ErrorCode::ColumnUnderrun)
        }
    }

    fn eval(
        &mut self,
        expr: &[u8],
        _: CmpType,
        batch: &BatchView<'_>,
        row: u32,
        _: &mut dyn columine_vm::logical_export::Interner,
    ) -> Result<RowCell, ErrorCode> {
        let column = batch.column(u32::from(expr[1]));
        let row = row as usize;
        let n = || col_u32_exact(column, batch.batch_len).expect("admit proved coverage")[row];
        Ok(match expr[0] {
            HALF_MORE => RowCell::F64(1.5 * f64::from(n())),
            PAST_F64 => RowCell::I64(TWO_53 + i64::from(n())),
            COPY | LIVE_COPY => RowCell::U32(n()),
            _ => RowCell::I64(
                col_i64_exact(column, batch.batch_len).expect("admit proved coverage")[row] + 1,
            ),
        })
    }
}

/// One RXP2 entry over the order rows: target column, lane, expression.
fn laned(target: u8, lane: CmpType, expr: [u8; 2]) -> Vec<u8> {
    let mut out = vec![target, lane as u8, 1];
    out.extend(ORDER_TYPE.to_le_bytes());
    out.extend(2u16.to_le_bytes());
    out.extend(expr);
    out
}

/// One RXP1 entry over the order rows: target column, expression.
fn unlaned(target: u8, expr: [u8; 2]) -> Vec<u8> {
    let mut out = laned(target, CmpType::U32, expr);
    out.remove(1);
    out
}

fn table(magic: u32, entries: &[Vec<u8>]) -> Vec<u8> {
    let mut out = magic.to_le_bytes().to_vec();
    out.extend(
        u16::try_from(entries.len())
            .expect("count fits u16")
            .to_le_bytes(),
    );
    out.push(TYPE_COL);
    for entry in entries {
        out.extend_from_slice(entry);
    }
    out
}

fn program(num_slots: u32, init: &[u8], reduce: &[u8], trailer: &[u8]) -> Vec<u8> {
    let mut prog = vec![0u8; 32];
    prog.extend(PROGRAM_MAGIC.to_le_bytes());
    prog.extend([2, 0, 0, 0]); // version 2, no callbacks, no flags
    prog.extend(num_slots.to_le_bytes());
    prog.extend(NUM_COLS.to_le_bytes());
    prog.extend(
        u32::try_from(init.len())
            .expect("init fits u32")
            .to_le_bytes(),
    );
    prog.extend(
        u32::try_from(reduce.len())
            .expect("reduce fits u32")
            .to_le_bytes(),
    );
    prog.extend_from_slice(init);
    prog.extend_from_slice(reduce);
    prog.extend_from_slice(trailer);
    prog
}

/// `FOR_EACH(type == ORDER_TYPE) { body }`, then HALT.
fn for_each(body: &[u8]) -> Vec<u8> {
    let mut reduce = vec![0xE0, TYPE_COL, 1];
    reduce.extend(ORDER_TYPE.to_le_bytes());
    reduce.extend(
        u16::try_from(body.len())
            .expect("body fits u16")
            .to_le_bytes(),
    );
    reduce.extend_from_slice(body);
    reduce.push(0);
    reduce
}

/// Slot 0 sums column `f64_col` (`BATCH_AGG_SUM`), slot 1 sums column
/// `i64_col` (`BATCH_AGG_SUM_I64`).
fn sums(f64_col: u8, i64_col: u8, trailer: &[u8]) -> Vec<u8> {
    let init = [
        0x10,
        0,
        2,
        AggType::Sum as u8,
        0,
        0x10,
        1,
        2,
        AggType::SumI64 as u8,
        0,
        0,
    ];
    program(2, &init, &[0x40, 0, f64_col, 0x49, 1, i64_col, 0], trailer)
}

/// Slot 0 is a struct map of two fields of `kinds` keyed by the key column;
/// each order row upserts column `first` into field 0 and `second` into
/// field 1 (`BATCH_STRUCT_MAP_UPSERT_LAST`).
fn record(first: u8, second: u8, kinds: [StructFieldType; 2], trailer: &[u8]) -> Vec<u8> {
    let init = [0x18, 0, 6, 8, 0, 2, kinds[0] as u8, kinds[1] as u8, 0];
    let body = [0x80, 0, KEY_COL, 2, first, 0, second, 1, 0];
    program(1, &init, &for_each(&body), trailer)
}

/// Slot 0 is a hash map; each order row upserts its value column into the
/// key while column `pred` holds a nonzero cell (`BATCH_MAP_UPSERT_LAST_IF`).
fn gated(pred: u8, trailer: &[u8]) -> Vec<u8> {
    let init = [0x10, 0, 0x00, 8, 0, 0];
    program(
        1,
        &init,
        &for_each(&[0x2a, 0, KEY_COL, N_COL, pred]),
        trailer,
    )
}

fn init(prog: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS)
        .expect("valid program admission");
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("init_state");
    state
}

/// Bind `prog`'s table over order rows of `keys` and `ns`, then run the batch.
fn fold(prog: &[u8], state: &mut [u8], keys: &[u32], ns: &[u32]) {
    let types = vec![ORDER_TYPE; ns.len()];
    let mut cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(keys),
        u32s_as_bytes(ns),
        &[],
        &[],
    ];
    let len = u32::try_from(ns.len()).expect("batch fits u32");
    let mut rows = RowColumns::new();
    let mut eval = Wide;
    let mut live = bind_row_columns(
        prog,
        &ReduceReads::of(prog).expect("the reduce reads derive"),
        state,
        &mut cols,
        len,
        &mut eval,
        &mut NoStrings,
        &mut rows,
    )
    .expect("bind");
    assert_eq!(
        Vm::default().execute_batch_live(state, prog, &cols, len, &mut live),
        OK
    );
}

/// `prog` folded over the rows as one batch, and as one batch per row.
fn one_batch_and_one_per_row(prog: &[u8], keys: &[u32], ns: &[u32]) -> [Vec<u8>; 2] {
    let mut one = init(prog);
    fold(prog, &mut one, keys, ns);
    let mut per_row = init(prog);
    for row in 0..ns.len() {
        fold(prog, &mut per_row, &keys[row..=row], &ns[row..=row]);
    }
    [one, per_row]
}

#[test]
fn wide_cells_feed_the_f64_and_i64_sums_exactly_however_the_batches_are_cut() {
    let prog = sums(
        DERIVED,
        DERIVED_2,
        &table(
            LANED_ROW_EXPRESSIONS_MAGIC,
            &[
                laned(DERIVED, CmpType::F64, [HALF_MORE, N_COL]),
                laned(DERIVED_2, CmpType::I64, [PAST_F64, N_COL]),
            ],
        ),
    );
    let keys = [1u32, 2, 3, 4];
    let ns = [1u32, 3, 5, 7];
    for state in one_batch_and_one_per_row(&prog, &keys, &ns) {
        let f64_sum = agg_value_f64(&state, SlotMetaView::read(&state, 0).offset);
        assert_eq!(f64_sum.to_bits(), 24.0f64.to_bits());
        // 2^53 + 1 has no f64: an i64 cell that passed through one rounds.
        let i64_sum = agg_value_i64(&state, SlotMetaView::read(&state, 1).offset);
        assert_eq!(i64_sum, 4 * TWO_53 + 16);
    }
}

#[test]
fn an_i64_record_field_computed_by_a_row_expression_stores_exactly() {
    let prog = record(
        DERIVED,
        DERIVED_2,
        [StructFieldType::Float64, StructFieldType::Int64],
        &table(
            LANED_ROW_EXPRESSIONS_MAGIC,
            &[
                laned(DERIVED, CmpType::F64, [HALF_MORE, N_COL]),
                laned(DERIVED_2, CmpType::I64, [PAST_F64, N_COL]),
            ],
        ),
    );
    let keys = [1u32, 2, 3];
    let ns = [1u32, 2, 3];
    for state in one_batch_and_one_per_row(&prog, &keys, &ns) {
        for (&key, &n) in keys.iter().zip(&ns) {
            let row = StoredRow::lookup(&state, 0, key, None)
                .expect("a struct-map slot")
                .expect("the key is stored");
            let field = |index| {
                <[u8; 8]>::try_from(row.field(index).expect("a field").expect("set"))
                    .expect("an 8-byte cell")
            };
            assert_eq!(
                f64::from_le_bytes(field(0)).to_bits(),
                (1.5 * f64::from(n)).to_bits()
            );
            assert_eq!(i64::from_le_bytes(field(1)), TWO_53 + i64::from(n));
        }
    }
}

#[test]
fn a_later_entry_reads_an_earlier_wide_column_at_its_lane() {
    let init_section = [0x10, 0, 2, AggType::SumI64 as u8, 0, 0];
    let prog = program(
        1,
        &init_section,
        &[0x49, 0, DERIVED_2, 0],
        &table(
            LANED_ROW_EXPRESSIONS_MAGIC,
            &[
                laned(DERIVED, CmpType::I64, [PAST_F64, N_COL]),
                laned(DERIVED_2, CmpType::I64, [NEXT, DERIVED]),
            ],
        ),
    );
    let mut state = init(&prog);
    let (types, keys, ns) = ([ORDER_TYPE; 2], [1u32, 2], [1u32, 2]);
    let mut cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&keys),
        u32s_as_bytes(&ns),
        &[],
        &[],
    ];
    let mut rows = RowColumns::new();
    let mut eval = Wide;
    let mut live = bind_row_columns(
        &prog,
        &ReduceReads::of(&prog).expect("the reduce reads derive"),
        &state,
        &mut cols,
        2,
        &mut eval,
        &mut NoStrings,
        &mut rows,
    )
    .expect("bind");
    assert_eq!(
        cols[usize::from(DERIVED)],
        i64s_as_bytes(&[TWO_53 + 1, TWO_53 + 2])
    );
    assert_eq!(
        cols[usize::from(DERIVED_2)],
        i64s_as_bytes(&[TWO_53 + 2, TWO_53 + 3])
    );
    assert_eq!(
        Vm::default().execute_batch_live(&mut state, &prog, &cols, 2, &mut live),
        OK
    );
    let sum = agg_value_i64(&state, SlotMetaView::read(&state, 0).offset);
    assert_eq!(sum, 2 * TWO_53 + 5);
}

#[test]
fn an_rxp1_table_binds_the_u32_cells_an_rxp2_u32_entry_binds() {
    for trailer in [
        table(ROW_EXPRESSIONS_MAGIC, &[unlaned(DERIVED, [COPY, N_COL])]),
        table(
            LANED_ROW_EXPRESSIONS_MAGIC,
            &[laned(DERIVED, CmpType::U32, [COPY, N_COL])],
        ),
    ] {
        let prog = gated(DERIVED, &trailer);
        let state = init(&prog);
        let (types, keys, ns) = ([ORDER_TYPE; 3], [1u32, 2, 3], [4u32, 0, 6]);
        let mut cols: Vec<&[u8]> = vec![
            u32s_as_bytes(&types),
            u32s_as_bytes(&keys),
            u32s_as_bytes(&ns),
            &[],
            &[],
        ];
        let mut rows = RowColumns::new();
        bind_row_columns(
            &prog,
            &ReduceReads::of(&prog).expect("the reduce reads derive"),
            &state,
            &mut cols,
            3,
            &mut Wide,
            &mut NoStrings,
            &mut rows,
        )
        .expect("bind");
        assert_eq!(cols[usize::from(DERIVED)], u32s_as_bytes(&ns));
    }
}

/// The bind's refusal of `prog` over one order row. The refusal leaves every
/// column as the host passed it.
fn refusal(prog: &[u8]) -> ErrorCode {
    let state = init(prog);
    let (types, keys, ns) = ([ORDER_TYPE], [1u32], [1u32]);
    let placeholder = [0xAAu32, 0xAA];
    let mut cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&keys),
        u32s_as_bytes(&ns),
        u32s_as_bytes(&placeholder),
        u32s_as_bytes(&placeholder),
    ];
    let mut rows = RowColumns::new();
    let code = bind_row_columns(
        prog,
        &ReduceReads::of(prog).expect("the reduce reads derive"),
        &state,
        &mut cols,
        1,
        &mut Wide,
        &mut NoStrings,
        &mut rows,
    )
    .map(|_| ())
    .expect_err("the bind refuses");
    for col in [DERIVED, DERIVED_2] {
        assert_eq!(cols[usize::from(col)], u32s_as_bytes(&placeholder));
    }
    code
}

#[test]
fn a_cell_of_another_lane_than_its_entry_is_refused() {
    let rxp1 = table(
        ROW_EXPRESSIONS_MAGIC,
        &[unlaned(DERIVED, [HALF_MORE, N_COL])],
    );
    assert_eq!(
        refusal(&gated(DERIVED, &rxp1)),
        ErrorCode::InvalidCellValue,
        "an RXP1 entry is u32"
    );
    let rxp2 = table(
        LANED_ROW_EXPRESSIONS_MAGIC,
        &[laned(DERIVED_2, CmpType::I64, [HALF_MORE, N_COL])],
    );
    assert_eq!(
        refusal(&sums(N_COL, DERIVED_2, &rxp2)),
        ErrorCode::InvalidCellValue,
        "an f64 answer in an i64 entry"
    );
}

#[test]
fn a_wide_column_read_off_its_lane_is_refused() {
    let f64_and_i64 = || {
        table(
            LANED_ROW_EXPRESSIONS_MAGIC,
            &[
                laned(DERIVED, CmpType::F64, [HALF_MORE, N_COL]),
                laned(DERIVED_2, CmpType::I64, [PAST_F64, N_COL]),
            ],
        )
    };
    let cases = [
        (
            "the f64 sum reads the i64 column and the i64 sum the f64 one",
            sums(DERIVED_2, DERIVED, &f64_and_i64()),
        ),
        (
            "an f64 column fills an INT64 field and an i64 column a FLOAT64 one",
            record(
                DERIVED,
                DERIVED_2,
                [StructFieldType::Int64, StructFieldType::Float64],
                &f64_and_i64(),
            ),
        ),
        (
            "an f64 column fills a UINT32 field",
            record(
                DERIVED,
                DERIVED_2,
                [StructFieldType::UInt32, StructFieldType::Int64],
                &f64_and_i64(),
            ),
        ),
        (
            "a u32 predicate reads an f64 column",
            gated(DERIVED, &f64_and_i64()),
        ),
    ];
    for (name, prog) in cases {
        assert_eq!(refusal(&prog), ErrorCode::InvalidProgram, "{name}");
    }
}

#[test]
fn a_wide_type_column_or_an_unknown_lane_is_refused() {
    let wide_type = table(
        LANED_ROW_EXPRESSIONS_MAGIC,
        &[laned(TYPE_COL, CmpType::F64, [HALF_MORE, N_COL])],
    );
    assert_eq!(
        refusal(&gated(DERIVED, &wide_type)),
        ErrorCode::InvalidProgram
    );
    let mut unknown = laned(DERIVED, CmpType::U32, [COPY, N_COL]);
    unknown[1] = 3;
    assert_eq!(
        refusal(&gated(
            DERIVED,
            &table(LANED_ROW_EXPRESSIONS_MAGIC, &[unknown])
        )),
        ErrorCode::InvalidProgram
    );
}

proptest! {
    /// Readability is judged from the final splice's lane, while a live column is never a plain value.
    #[test]
    fn mixed_live_and_duplicate_columns_keep_their_final_lane_and_refusals(
        ns in prop::collection::vec(0u32..100, 1..7),
    ) {
        let mut rows = RowColumns::new();
        let types = vec![ORDER_TYPE; ns.len()];
        let keys: Vec<u32> = (1..=u32::try_from(ns.len()).expect("small batch")).collect();
        let placeholder = vec![0xAAu32; ns.len()];
        for (wide_last, plain_live) in [(false, false), (true, false), (false, true), (true, true)] {
            let mut entries = vec![
                laned(DERIVED_2, CmpType::U32, [COPY, N_COL]),
                laned(DERIVED_2, CmpType::I64, [PAST_F64, N_COL]),
            ];
            if !wide_last {
                entries.push(laned(DERIVED_2, CmpType::U32, [COPY, N_COL]));
            }
            entries.push(laned(DERIVED, CmpType::U32, [LIVE_COPY, N_COL]));
            let value_col = if plain_live { DERIVED } else { DERIVED_2 };
            let prog = program(
                1,
                &[0x10, 0, 0x00, 8, 0, 0],
                &for_each(&[0x2a, 0, KEY_COL, value_col, DERIVED]),
                &table(LANED_ROW_EXPRESSIONS_MAGIC, &entries),
            );
            let mut state = init(&prog);
            let mut cols: Vec<&[u8]> = vec![
                u32s_as_bytes(&types), u32s_as_bytes(&keys), u32s_as_bytes(&ns),
                u32s_as_bytes(&placeholder), u32s_as_bytes(&placeholder),
            ];
            let mut eval = Wide;
            let bound = bind_row_columns(
                &prog, &ReduceReads::of(&prog).expect("the reduce reads derive"), &state, &mut cols, u32::try_from(ns.len()).expect("small batch"),
                &mut eval, &mut NoStrings, &mut rows,
            );
            match bound {
                Err(error) => {
                    prop_assert!(wide_last || plain_live);
                    prop_assert_eq!(error, ErrorCode::InvalidProgram);
                    for col in [DERIVED, DERIVED_2] {
                        prop_assert_eq!(cols[usize::from(col)], u32s_as_bytes(&placeholder));
                    }
                }
                Ok(mut live) => {
                    prop_assert!(!wide_last && !plain_live);
                    prop_assert_eq!(Vm::default().execute_batch_live(
                        &mut state, &prog, &cols, u32::try_from(ns.len()).expect("small batch"), &mut live,
                    ), OK);
                    let map = CheckedMap::bind(&state, 0).expect("valid map slot");
                    for (&key, &n) in keys.iter().zip(&ns) {
                        prop_assert_eq!(map.get(key), (n != 0).then_some(n));
                    }
                }
            }
        }
    }
}

/// Binds an entry up front.
const BOUND: u8 = 0;
/// Binds an entry live.
const LIVE: u8 = 1;
/// Binds an entry live when the batch holds an odd number of rows: which
/// entries bind live is the embedder's answer per batch.
const LIVE_IF_ODD: u8 = 2;

/// The embedder of the verdict property: an entry's expression `[mode, _]`
/// names how it binds, and every cell is 1 in the entry's own lane.
struct Modes;

impl RowExpression for Modes {
    fn admit(
        &mut self,
        expr: &[u8],
        _: CmpType,
        batch: &BatchView<'_>,
    ) -> Result<Binding, ErrorCode> {
        Ok(match expr[0] {
            BOUND => Binding::Bound,
            LIVE => Binding::Live,
            _ if batch.batch_len % 2 == 1 => Binding::Live,
            _ => Binding::Bound,
        })
    }

    fn eval(
        &mut self,
        _: &[u8],
        lane: CmpType,
        _: &BatchView<'_>,
        _: u32,
        _: &mut dyn columine_vm::logical_export::Interner,
    ) -> Result<RowCell, ErrorCode> {
        Ok(match lane {
            CmpType::U32 => RowCell::U32(1),
            CmpType::F64 => RowCell::F64(1.0),
            CmpType::I64 => RowCell::I64(1),
        })
    }
}

/// The judgement a bind made over every read before the verdict was derived
/// once per program: `entries` are the table's targets and lanes in order,
/// `live` the columns an entry bound live in this batch.
fn walked_refusal(reads: &[ReduceRead], entries: &[(u8, CmpType)], live: &[u8]) -> bool {
    let has_wide = entries.iter().any(|&(_, lane)| lane != CmpType::U32);
    if live.is_empty() && !has_wide {
        return false;
    }
    let wide_lane = |column: u32| {
        entries
            .iter()
            .rev()
            .find(|&&(target, _)| u32::from(target) == column)
            .map(|&(_, lane)| lane)
            .filter(|&lane| lane != CmpType::U32)
    };
    let is_live = |column: u32| live.iter().any(|&target| u32::from(target) == column);
    let type_col = u32::from(TYPE_COL);
    if is_live(type_col) || (has_wide && wide_lane(type_col).is_some()) {
        return true;
    }
    reads.iter().any(|read| {
        (read.usage == ColumnUse::Read && is_live(read.column))
            || (has_wide && wide_lane(read.column).is_some_and(|wide| read.lane != Some(wide)))
    })
}

/// A generated program: slot 0 sums an f64 column, slot 1 an i64 column,
/// slot 2 is a hash map, slot 3 counts rows; `top` are top-level
/// `(op, column)` reads (0 f64 sum, 1 i64 sum, 2 count-if predicate) and
/// `body` an optional `FOR_EACH` map upsert of `(key, value, predicate)`.
fn generated(
    top: &[(u8, u8)],
    body: Option<(u8, u8, u8)>,
    entries: &[(u8, CmpType, u8)],
) -> Vec<u8> {
    let init = [
        0x10,
        0,
        2,
        AggType::Sum as u8,
        0,
        0x10,
        1,
        2,
        AggType::SumI64 as u8,
        0,
        0x10,
        2,
        0x00,
        8,
        0,
        0x10,
        3,
        2,
        AggType::Count as u8,
        0,
        0,
    ];
    let mut reduce = Vec::new();
    for &(op, column) in top {
        reduce.extend(match op {
            0 => [0x40, 0, column],
            1 => [0x49, 1, column],
            _ => [0x45, 3, column],
        });
    }
    match body {
        Some((key, value, pred)) => reduce.extend(for_each(&[0x2a, 2, key, value, pred])),
        None => reduce.push(0),
    }
    let entries: Vec<Vec<u8>> = entries
        .iter()
        .map(|&(target, lane, mode)| laned(target, lane, [mode, 0]))
        .collect();
    program(
        4,
        &init,
        &reduce,
        &table(LANED_ROW_EXPRESSIONS_MAGIC, &entries),
    )
}

fn lane_of(index: u8) -> CmpType {
    CmpType::from_u8(index).expect("a generated lane")
}

/// The verdict derived once per program, with each bind checking only its own
/// live entries, refuses exactly the binds the per-bind judgement over every
/// read refused, with the same code, over generated programs (wide, live and
/// batch-dependent live entries, the type column among the targets) and
/// generated batches. The counts printed show both answers were exercised.
#[test]
fn the_once_derived_verdict_refuses_exactly_what_the_per_bind_walk_refused() {
    use proptest::test_runner::{TestCaseError, TestRunner};
    use std::cell::Cell;
    let cases = (
        prop::collection::vec((0u8..3, 0u8..5), 0..4),
        prop::option::of((0u8..5, 0u8..5, 0u8..5)),
        prop::collection::vec((0u8..5, 0u8..3, 0u8..3), 0..5),
        1u32..6,
    );
    let (refused, accepted, live_refused) = (Cell::new(0u32), Cell::new(0u32), Cell::new(0u32));
    let mut runner = TestRunner::new(ProptestConfig::with_cases(4096));
    let verdict = runner.run(&cases, |(top, body, entries, len)| {
        let entries: Vec<(u8, CmpType, u8)> = entries
            .into_iter()
            .map(|(target, lane, mode)| (target, lane_of(lane), mode))
            .collect();
        let prog = generated(&top, body, &entries);
        let reads = ReduceReads::of(&prog).expect("the reduce reads derive");
        let state = init(&prog);
        let types = vec![ORDER_TYPE; len as usize];
        let host: Vec<u32> = (1..=len).collect();
        let mut cols: Vec<&[u8]> = vec![u32s_as_bytes(&types)];
        cols.extend((1..NUM_COLS).map(|_| u32s_as_bytes(&host)));
        let live: Vec<u8> = entries
            .iter()
            .filter(|&&(_, _, mode)| mode == LIVE || (mode == LIVE_IF_ODD && len % 2 == 1))
            .map(|&(target, _, _)| target)
            .collect();
        let twice_live = live
            .iter()
            .enumerate()
            .any(|(i, target)| live[..i].contains(target));
        let lanes: Vec<(u8, CmpType)> = entries
            .iter()
            .map(|&(target, lane, _)| (target, lane))
            .collect();
        let walked = twice_live || walked_refusal(reads.reads(), &lanes, &live);
        let mut out = RowColumns::new();
        let mut eval = Modes;
        let bound = bind_row_columns(
            &prog,
            &reads,
            &state,
            &mut cols,
            len,
            &mut eval,
            &mut NoStrings,
            &mut out,
        )
        .err();
        if bound != walked.then_some(ErrorCode::InvalidProgram) {
            return Err(TestCaseError::fail(format!(
                "bind {bound:?}, walked {walked}: reads {:?}, entries {entries:?}, live {live:?}",
                reads.reads()
            )));
        }
        if walked {
            refused.set(refused.get() + 1);
            if !reads.refuses_wide() {
                live_refused.set(live_refused.get() + 1);
            }
        } else {
            accepted.set(accepted.get() + 1);
        }
        Ok(())
    });
    println!(
        "verdict property: {} refused ({} by live entries, the rest by the wide verdict), {} accepted",
        refused.get(),
        live_refused.get(),
        accepted.get()
    );
    if let Err(failure) = verdict {
        panic!("{failure}");
    }
    assert!(
        accepted.get() > 0 && live_refused.get() > 0 && refused.get() > live_refused.get(),
        "every judgement is exercised"
    );
}
#[test]
fn a_warmed_wide_bind_allocates_nothing() {
    let prog = sums(
        DERIVED,
        DERIVED_2,
        &table(
            LANED_ROW_EXPRESSIONS_MAGIC,
            &[
                laned(DERIVED, CmpType::F64, [HALF_MORE, N_COL]),
                laned(DERIVED_2, CmpType::I64, [PAST_F64, N_COL]),
            ],
        ),
    );
    let state = init(&prog);
    let reads = ReduceReads::of(&prog).expect("the reduce reads derive");
    let (types, keys, ns) = ([ORDER_TYPE; 3], [1u32, 2, 3], [1u32, 2, 3]);
    let host = || -> Vec<&[u8]> {
        vec![
            u32s_as_bytes(&types),
            u32s_as_bytes(&keys),
            u32s_as_bytes(&ns),
            &[],
            &[],
        ]
    };
    let mut rows = RowColumns::new();
    let mut cold = host();
    bind_row_columns(
        &prog,
        &reads,
        &state,
        &mut cold,
        3,
        &mut Wide,
        &mut NoStrings,
        &mut rows,
    )
    .expect("cold bind");
    let mut warm = host();
    let bound = with_allocations(0, || {
        bind_row_columns(
            &prog,
            &reads,
            &state,
            &mut warm,
            3,
            &mut Wide,
            &mut NoStrings,
            &mut rows,
        )
        .map(|_| ())
    });
    assert_eq!(bound, Ok(()));
}
