//! Row-expression binding: the table after the reduce section is evaluated
//! per row through the embedder's evaluator, gated on the type column, and
//! spliced into the batch so dispatch reads the derived column like input.

use columine_types::PROGRAM_MAGIC;
use columine_types::types::{EMPTY_KEY, ErrorCode};
use columine_vm::meta::SlotMetaView;
use columine_vm::row_exprs::{
    BatchView, ROW_EXPRESSIONS_MAGIC, RowColumns, RowExpression, bind_row_columns,
};
use columine_vm::state_init::{DEFAULT_ACCEPTED_PROGRAM_MAGICS, calculate_state_size, init_state};
use columine_vm::vm::{Vm, col_u32_exact, u32s_as_bytes, vm_map_get};

const OK: u32 = ErrorCode::Ok as u32;

/// Columns: 0 type, 1 key, 2 value, 3 derived predicate.
const TYPE_COL: u8 = 0;
const KEY_COL: u8 = 1;
const VAL_COL: u8 = 2;
const PRED_COL: u8 = 3;
const ORDER_TYPE: u32 = 7;

/// A program whose reduce section is `FOR_EACH(type == ORDER_TYPE) {
/// BatchMapUpsertLastIf }` gated by the derived column, followed by `trailer`.
fn program(trailer: &[u8]) -> Vec<u8> {
    let init = [0x10u8, 0, 0x00, 8, 0, 0]; // SLOT_DEF hashmap cap 8, HALT
    let body = [0x2au8, 0, KEY_COL, VAL_COL, PRED_COL];
    let mut reduce = vec![0xE0u8, TYPE_COL, 1];
    reduce.extend(ORDER_TYPE.to_le_bytes());
    reduce.extend((body.len() as u16).to_le_bytes());
    reduce.extend(body);
    reduce.push(0); // HALT
    let mut prog = vec![0u8; 32];
    prog.extend(PROGRAM_MAGIC.to_le_bytes());
    prog.extend([1, 0, 1, 4, 0, 0]);
    prog.extend((init.len() as u16).to_le_bytes());
    prog.extend((reduce.len() as u16).to_le_bytes());
    prog.extend(init);
    prog.extend(&reduce);
    prog.extend_from_slice(trailer);
    prog
}

/// One table entry: target column, matched type ids, expression bytes.
fn entry(target: u8, ids: &[u32], expr: &[u8]) -> Vec<u8> {
    let mut out = vec![target, ids.len() as u8];
    for id in ids {
        out.extend(id.to_le_bytes());
    }
    out.extend((expr.len() as u16).to_le_bytes());
    out.extend_from_slice(expr);
    out
}

fn table(type_col: u8, entries: &[Vec<u8>]) -> Vec<u8> {
    let mut out = ROW_EXPRESSIONS_MAGIC.to_le_bytes().to_vec();
    out.extend((entries.len() as u16).to_le_bytes());
    out.push(type_col);
    for e in entries {
        out.extend_from_slice(e);
    }
    out
}

fn init(prog: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS);
    assert!(size > 0, "state size must be > 0");
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("init_state");
    state
}

/// Expression `[col, threshold u32le]`: 1 when the row's `col` cell exceeds
/// `threshold`, else 0. `admit` refuses a column that cannot cover the batch.
struct Threshold {
    admitted: usize,
    evaluated: usize,
}

impl RowExpression for Threshold {
    fn admit(&mut self, expr: &[u8], batch: &BatchView<'_>) -> Result<(), ErrorCode> {
        self.admitted += 1;
        if expr.len() != 5 {
            return Err(ErrorCode::InvalidProgram);
        }
        if usize::from(expr[0]) >= batch.num_cols() {
            return Err(ErrorCode::InvalidProgram);
        }
        col_u32_exact(batch.column(expr[0]), batch.batch_len)
            .map(|_| ())
            .ok_or(ErrorCode::ColumnUnderrun)
    }

    fn eval(&mut self, expr: &[u8], batch: &BatchView<'_>, row: u32) -> Result<u32, ErrorCode> {
        self.evaluated += 1;
        let cells =
            col_u32_exact(batch.column(expr[0]), batch.batch_len).expect("admit proved coverage");
        let threshold = u32::from_le_bytes([expr[1], expr[2], expr[3], expr[4]]);
        Ok(u32::from(cells[row as usize] > threshold))
    }
}

fn threshold_expr(col: u8, threshold: u32) -> Vec<u8> {
    let mut e = vec![col];
    e.extend(threshold.to_le_bytes());
    e
}

fn map_value(state: &[u8], key: u32) -> u32 {
    let meta = SlotMetaView::read(state, 0);
    vm_map_get(state, meta.offset, meta.capacity, key)
}

#[test]
fn derived_predicate_gates_the_upsert_and_unmatched_rows_stay_zero() {
    let prog = program(&table(
        TYPE_COL,
        &[entry(
            PRED_COL,
            &[ORDER_TYPE],
            &threshold_expr(VAL_COL, 100),
        )],
    ));
    let mut state = init(&prog);
    let types = [ORDER_TYPE, ORDER_TYPE, 9, ORDER_TYPE];
    let keys = [1u32, 2, 3, 4];
    let vals = [50u32, 150, 999, 101];
    // The host's placeholder for the derived column is empty: the splice
    // must replace it, or dispatch refuses the batch with ColumnUnderrun.
    let mut cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&keys),
        u32s_as_bytes(&vals),
        &[],
    ];
    let mut rows = RowColumns::new();
    let mut eval = Threshold {
        admitted: 0,
        evaluated: 0,
    };
    bind_row_columns(&prog, &state, &mut cols, 4, &mut eval, &mut rows).expect("bind");
    assert_eq!(cols[3], u32s_as_bytes(&[0u32, 1, 0, 1]));
    assert_eq!(eval.admitted, 1);
    assert_eq!(
        eval.evaluated, 3,
        "the row of another type is never evaluated"
    );

    let mut vm = Vm::new(DEFAULT_ACCEPTED_PROGRAM_MAGICS);
    assert_eq!(vm.execute_batch(&mut state, &prog, &cols, 4), OK);
    assert_eq!(map_value(&state, 1), EMPTY_KEY);
    assert_eq!(map_value(&state, 2), 150);
    assert_eq!(map_value(&state, 3), EMPTY_KEY);
    assert_eq!(map_value(&state, 4), 101);
}

#[test]
fn a_program_without_a_table_binds_nothing() {
    let prog = program(&[]);
    let state = init(&prog);
    let placeholder = [7u32, 7];
    let mut cols: Vec<&[u8]> = vec![&[], &[], &[], u32s_as_bytes(&placeholder)];
    let mut rows = RowColumns::new();
    let mut eval = Threshold {
        admitted: 0,
        evaluated: 0,
    };
    bind_row_columns(&prog, &state, &mut cols, 2, &mut eval, &mut rows).expect("bind");
    assert_eq!(cols[3], u32s_as_bytes(&placeholder));
    assert_eq!(eval.admitted, 0);
}

#[test]
fn an_empty_batch_binds_nothing_even_without_column_pointers() {
    let prog = program(&table(
        TYPE_COL,
        &[entry(
            PRED_COL,
            &[ORDER_TYPE],
            &threshold_expr(VAL_COL, 100),
        )],
    ));
    let state = init(&prog);
    let mut cols: Vec<&[u8]> = Vec::new();
    let mut rows = RowColumns::new();
    let mut eval = Threshold {
        admitted: 0,
        evaluated: 0,
    };
    bind_row_columns(&prog, &state, &mut cols, 0, &mut eval, &mut rows).expect("bind");
    assert!(cols.is_empty());
    assert_eq!(eval.admitted, 0);
}

fn bind_err<'a>(
    prog: &[u8],
    cols: &mut [&'a [u8]],
    batch_len: u32,
    rows: &'a mut RowColumns,
) -> ErrorCode {
    let state = init(prog);
    let mut eval = Threshold {
        admitted: 0,
        evaluated: 0,
    };
    bind_row_columns(prog, &state, cols, batch_len, &mut eval, rows).expect_err("bind must refuse")
}

#[test]
fn malformed_tables_are_refused_and_leave_the_columns_untouched() {
    let types = [ORDER_TYPE];
    let keys = [1u32];
    let vals = [50u32];
    let placeholder = [0xAAu32];
    let fresh = || -> Vec<&[u8]> {
        vec![
            u32s_as_bytes(&types),
            u32s_as_bytes(&keys),
            u32s_as_bytes(&vals),
            u32s_as_bytes(&placeholder),
        ]
    };
    let cases: [(&str, Vec<u8>, ErrorCode); 6] = [
        (
            "bad magic",
            vec![1, 2, 3, 4, 0, 0, 0],
            ErrorCode::InvalidProgram,
        ),
        (
            "target column out of range",
            table(
                TYPE_COL,
                &[entry(9, &[ORDER_TYPE], &threshold_expr(VAL_COL, 1))],
            ),
            ErrorCode::InvalidProgram,
        ),
        (
            "type column out of range",
            table(
                9,
                &[entry(PRED_COL, &[ORDER_TYPE], &threshold_expr(VAL_COL, 1))],
            ),
            ErrorCode::InvalidProgram,
        ),
        (
            "zero match ids",
            table(
                TYPE_COL,
                &[entry(PRED_COL, &[], &threshold_expr(VAL_COL, 1))],
            ),
            ErrorCode::InvalidProgram,
        ),
        (
            "truncated entry",
            table(
                TYPE_COL,
                &[entry(PRED_COL, &[ORDER_TYPE], &threshold_expr(VAL_COL, 1))[..6].to_vec()],
            ),
            ErrorCode::InvalidProgram,
        ),
        (
            "expression names a column that cannot cover the batch",
            table(
                TYPE_COL,
                &[entry(PRED_COL, &[ORDER_TYPE], &threshold_expr(PRED_COL, 1))],
            ),
            ErrorCode::ColumnUnderrun,
        ),
    ];
    for (name, trailer, expected) in cases {
        let prog = program(&trailer);
        let mut cols = fresh();
        // The underrun case needs the placeholder short, not the others.
        if expected == ErrorCode::ColumnUnderrun {
            cols[3] = &[];
        }
        let mut rows = RowColumns::new();
        let err = bind_err(&prog, &mut cols, 1, &mut rows);
        assert_eq!(err, expected, "{name}");
        if expected != ErrorCode::ColumnUnderrun {
            assert_eq!(
                cols[3],
                u32s_as_bytes(&placeholder),
                "{name}: columns untouched"
            );
        }
    }
}

#[test]
fn trailing_bytes_after_the_last_entry_are_refused() {
    let mut trailer = table(
        TYPE_COL,
        &[entry(PRED_COL, &[ORDER_TYPE], &threshold_expr(VAL_COL, 1))],
    );
    trailer.push(0);
    let prog = program(&trailer);
    let types = [ORDER_TYPE];
    let mut rows = RowColumns::new();
    let mut cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&types),
        u32s_as_bytes(&types),
        &[],
    ];
    assert_eq!(
        bind_err(&prog, &mut cols, 1, &mut rows),
        ErrorCode::InvalidProgram
    );
}

#[test]
fn a_later_entry_reads_an_earlier_entry_s_derived_column() {
    // Columns: 0 type, 1 key, 2 value, 3 derived (value > 100), 4 derived
    // (column 3 > 0) — the second entry reads the first's cells, not the
    // host's empty placeholder.
    let init_code = [0x10u8, 0, 0x00, 8, 0, 0];
    let reduce = [0u8];
    let trailer = table(
        TYPE_COL,
        &[
            entry(3, &[ORDER_TYPE], &threshold_expr(VAL_COL, 100)),
            entry(4, &[ORDER_TYPE], &threshold_expr(3, 0)),
        ],
    );
    let mut prog = vec![0u8; 32];
    prog.extend(PROGRAM_MAGIC.to_le_bytes());
    prog.extend([1, 0, 1, 5, 0, 0]);
    prog.extend((init_code.len() as u16).to_le_bytes());
    prog.extend((reduce.len() as u16).to_le_bytes());
    prog.extend(init_code);
    prog.extend(reduce);
    prog.extend_from_slice(&trailer);
    let state = init(&prog);
    let types = [ORDER_TYPE, ORDER_TYPE];
    let vals = [50u32, 150];
    let mut cols: Vec<&[u8]> = vec![u32s_as_bytes(&types), &[], u32s_as_bytes(&vals), &[], &[]];
    let mut rows = RowColumns::new();
    let mut eval = Threshold {
        admitted: 0,
        evaluated: 0,
    };
    bind_row_columns(&prog, &state, &mut cols, 2, &mut eval, &mut rows).expect("bind");
    assert_eq!(cols[3], u32s_as_bytes(&[0u32, 1]));
    assert_eq!(cols[4], u32s_as_bytes(&[0u32, 1]));
}

#[test]
fn two_entries_bind_their_own_targets_over_the_host_batch() {
    // Columns: 0 type, 1 key, 2 value, 3 derived, 4 derived.
    let init_code = [0x10u8, 0, 0x00, 8, 0, 0];
    let reduce = [0u8];
    let trailer = table(
        TYPE_COL,
        &[
            entry(3, &[ORDER_TYPE], &threshold_expr(VAL_COL, 100)),
            entry(4, &[ORDER_TYPE], &threshold_expr(VAL_COL, 10)),
        ],
    );
    let mut prog = vec![0u8; 32];
    prog.extend(PROGRAM_MAGIC.to_le_bytes());
    prog.extend([1, 0, 1, 5, 0, 0]);
    prog.extend((init_code.len() as u16).to_le_bytes());
    prog.extend((reduce.len() as u16).to_le_bytes());
    prog.extend(init_code);
    prog.extend(reduce);
    prog.extend_from_slice(&trailer);
    let state = init(&prog);
    let types = [ORDER_TYPE, ORDER_TYPE];
    let vals = [50u32, 150];
    let mut cols: Vec<&[u8]> = vec![u32s_as_bytes(&types), &[], u32s_as_bytes(&vals), &[], &[]];
    let mut rows = RowColumns::new();
    let mut eval = Threshold {
        admitted: 0,
        evaluated: 0,
    };
    bind_row_columns(&prog, &state, &mut cols, 2, &mut eval, &mut rows).expect("bind");
    assert_eq!(cols[3], u32s_as_bytes(&[0u32, 1]));
    assert_eq!(cols[4], u32s_as_bytes(&[1u32, 1]));
}
