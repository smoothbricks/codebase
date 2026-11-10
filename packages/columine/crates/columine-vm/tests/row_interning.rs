//! Row interning (specs/columine/09-row-interning.md): the bind hands a
//! row expression the caller's interner, so a string the expression builds
//! is answered as an id of the same table the host's ids resolve through,
//! equal for equal text in any batch, and a refusal of the table refuses the
//! bind before any column is spliced.

use std::collections::HashMap;

use columine_types::InternedStr;
use columine_types::PROGRAM_MAGIC;
use columine_types::types::{CmpType, ErrorCode};
use columine_vm::column_uses::ReduceReads;
use columine_vm::key_index::NoStrings;
use columine_vm::logical_export::{InternLookup, Interner};
use columine_vm::meta::SlotMetaView;
use columine_vm::row_exprs::{
    BatchView, Binding, EntryReads, ROW_EXPRESSIONS_MAGIC, RowCell, RowColumns, RowExpression,
    RowPlan, bind_row_columns,
};
use columine_vm::state_init::{DEFAULT_ACCEPTED_PROGRAM_MAGICS, calculate_state_size, init_state};
use columine_vm::vm::{Vm, col_u32_exact, u32s_as_bytes};

const OK: u32 = ErrorCode::Ok as u32;

/// Columns: 0 type, 1 a head id, 2 a tail id, 3 derived.
const TYPE_COL: u8 = 0;
const HEAD_COL: u8 = 1;
const TAIL_COL: u8 = 2;
const DERIVED: u8 = 3;
const NUM_COLS: u32 = 4;
const CALL_TYPE: u32 = 7;

/// The caller's intern table: ids in first-intern order, refusing past
/// `limit` strings.
struct Table {
    texts: Vec<String>,
    ids: HashMap<String, u32>,
    limit: usize,
}

impl Table {
    fn with(seed: &[&str], limit: usize) -> Self {
        let mut table = Self {
            texts: Vec::new(),
            ids: HashMap::new(),
            limit,
        };
        for text in seed {
            table
                .intern(InternedStr::new(text).expect("a short seed"))
                .expect("the seed fits");
        }
        table
    }
}

impl InternLookup for Table {
    fn text(&self, id: u32) -> Option<&str> {
        self.texts.get(id as usize).map(String::as_str)
    }
}

impl Interner for Table {
    fn intern(&mut self, text: InternedStr<'_>) -> Result<u32, ErrorCode> {
        let text = text.as_str();
        if let Some(&id) = self.ids.get(text) {
            return Ok(id);
        }
        if self.texts.len() == self.limit {
            return Err(ErrorCode::CapacityExceeded);
        }
        let id = u32::try_from(self.texts.len()).expect("ids fit u32");
        self.texts.push(text.to_owned());
        self.ids.insert(text.to_owned(), id);
        Ok(id)
    }
}

/// The embedder of these tests: expression `[head_col, tail_col]` joins the
/// two ids' texts with `::` and interns the join, reusing one buffer.
#[derive(Default)]
struct Join {
    scratch: String,
}

impl RowExpression for Join {
    fn admit(
        &mut self,
        expr: &[u8],
        lane: CmpType,
        reads: &mut EntryReads<'_>,
    ) -> Result<Binding, ErrorCode> {
        let &[head, tail] = expr else {
            return Err(ErrorCode::InvalidProgram);
        };
        if lane != CmpType::U32 {
            return Err(ErrorCode::InvalidProgram);
        }
        for col in [head, tail] {
            reads.read(u32::from(col), CmpType::U32)?;
        }
        Ok(Binding::Bound)
    }

    fn eval(
        &mut self,
        expr: &[u8],
        _: CmpType,
        batch: &BatchView<'_>,
        row: u32,
        strings: &mut dyn Interner,
    ) -> Result<RowCell, ErrorCode> {
        let id = |col: u8| {
            col_u32_exact(batch.column(u32::from(col)), batch.batch_len)
                .expect("admit proved coverage")[row as usize]
        };
        self.scratch.clear();
        self.scratch.push_str(
            strings
                .text(id(expr[0]))
                .ok_or(ErrorCode::InvalidCellValue)?,
        );
        self.scratch.push_str("::");
        self.scratch.push_str(
            strings
                .text(id(expr[1]))
                .ok_or(ErrorCode::InvalidCellValue)?,
        );
        // A joined key longer than an interned identity holds no u32 cell (09 §2).
        let joined = InternedStr::new(&self.scratch).map_err(|_| ErrorCode::InvalidCellValue)?;
        strings.intern(joined).map(RowCell::U32)
    }
}

/// One RXP1 table whose single entry joins the head and tail ids into `DERIVED`.
fn join_table() -> Vec<u8> {
    let mut out = ROW_EXPRESSIONS_MAGIC.to_le_bytes().to_vec();
    out.extend(1u16.to_le_bytes());
    out.push(TYPE_COL);
    out.extend([DERIVED, 1]);
    out.extend(CALL_TYPE.to_le_bytes());
    out.extend(2u16.to_le_bytes());
    out.extend([HEAD_COL, TAIL_COL]);
    out
}

/// Slot 0 is a hash set; each call row inserts its derived id.
fn program() -> Vec<u8> {
    let init = [0x10, 0, 0x01, 8, 0, 0];
    let body = [0x30, 0, DERIVED];
    let mut reduce = vec![0xE0, TYPE_COL, 1];
    reduce.extend(CALL_TYPE.to_le_bytes());
    reduce.extend(
        u16::try_from(body.len())
            .expect("body fits u16")
            .to_le_bytes(),
    );
    reduce.extend(body);
    reduce.push(0);
    let mut prog = vec![0u8; 32];
    prog.extend(PROGRAM_MAGIC.to_le_bytes());
    prog.extend([2, 0, 0, 0]);
    prog.extend(1u32.to_le_bytes());
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
    prog.extend(init);
    prog.extend(reduce);
    prog.extend(join_table());
    prog
}

fn init(prog: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS)
        .expect("valid program admission");
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("init_state");
    state
}

/// The derived ids one bind answers over call rows of `heads` and `tails`.
fn bind(
    prog: &[u8],
    strings: &mut Table,
    heads: &[u32],
    tails: &[u32],
) -> Result<Vec<u32>, ErrorCode> {
    let state = init(prog);
    let types = vec![CALL_TYPE; heads.len()];
    let mut cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(heads),
        u32s_as_bytes(tails),
        &[],
    ];
    let len = u32::try_from(heads.len()).expect("batch fits u32");
    let mut rows = RowColumns::new();
    bind_row_columns(
        prog,
        &RowPlan::of(
            prog,
            &ReduceReads::of(prog).expect("the reduce reads derive"),
            &mut Join::default(),
        )
        .expect("the table plans"),
        &state,
        &mut cols,
        len,
        &mut Join::default(),
        strings,
        &mut rows,
    )?;
    Ok(col_u32_exact(cols[usize::from(DERIVED)], len)
        .expect("the bind spliced every row")
        .to_vec())
}

/// Fold call rows of `heads` and `tails` into `state`, interning into `strings`.
fn fold(prog: &[u8], state: &mut [u8], strings: &mut Table, heads: &[u32], tails: &[u32]) {
    let types = vec![CALL_TYPE; heads.len()];
    let mut cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(heads),
        u32s_as_bytes(tails),
        &[],
    ];
    let len = u32::try_from(heads.len()).expect("batch fits u32");
    let mut rows = RowColumns::new();
    let mut eval = Join::default();
    let mut live = bind_row_columns(
        prog,
        &RowPlan::of(
            prog,
            &ReduceReads::of(prog).expect("the reduce reads derive"),
            &mut eval,
        )
        .expect("the table plans"),
        state,
        &mut cols,
        len,
        &mut eval,
        strings,
        &mut rows,
    )
    .expect("bind");
    assert_eq!(
        Vm::default().execute_batch_live(state, prog, &cols, len, &mut live),
        OK
    );
}

const SEED: [&str; 4] = ["s1", "s2", "c1", "c2"];
const S1: u32 = 0;
const S2: u32 = 1;
const C1: u32 = 2;

#[test]
fn joined_ids_are_equal_for_equal_text_within_and_across_batches() {
    let prog = program();
    let mut strings = Table::with(&SEED, 64);
    let first = bind(&prog, &mut strings, &[S1, S2, S1], &[C1, C1, C1]).expect("bind");
    assert_eq!(first[0], first[2]);
    assert_ne!(first[0], first[1]);
    assert_eq!(strings.text(first[0]), Some("s1::c1"));
    assert_eq!(strings.text(first[1]), Some("s2::c1"));
    let second = bind(&prog, &mut strings, &[S1], &[C1]).expect("bind");
    assert_eq!(second, [first[0]]);
}

#[test]
fn joined_ids_key_a_set_however_the_batches_are_cut() {
    let prog = program();
    let heads = [S1, S2, S1];
    let tails = [C1, C1, C1];
    let mut one = init(&prog);
    let mut one_strings = Table::with(&SEED, 64);
    fold(&prog, &mut one, &mut one_strings, &heads, &tails);
    let mut per_row = init(&prog);
    let mut per_row_strings = Table::with(&SEED, 64);
    for row in 0..heads.len() {
        fold(
            &prog,
            &mut per_row,
            &mut per_row_strings,
            &heads[row..=row],
            &tails[row..=row],
        );
    }
    for state in [&one, &per_row] {
        assert_eq!(SlotMetaView::read(state, 0).size(state), 2);
    }
    assert_eq!(one_strings.texts, per_row_strings.texts);
}

#[test]
fn an_interner_refusal_refuses_the_bind_and_splices_nothing() {
    let prog = program();
    let state = init(&prog);
    let mut strings = Table::with(&SEED, SEED.len());
    let types = [CALL_TYPE];
    let mut cols: Vec<&[u8]> = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&[S1]),
        u32s_as_bytes(&[C1]),
        &[],
    ];
    let mut rows = RowColumns::new();
    let refused = bind_row_columns(
        &prog,
        &RowPlan::of(
            &prog,
            &ReduceReads::of(&prog).expect("the reduce reads derive"),
            &mut Join::default(),
        )
        .expect("the table plans"),
        &state,
        &mut cols,
        1,
        &mut Join::default(),
        &mut strings,
        &mut rows,
    )
    .map(|_| ())
    .err();
    assert_eq!(refused, Some(ErrorCode::CapacityExceeded));
    assert!(cols[usize::from(DERIVED)].is_empty());
}

#[test]
fn no_strings_names_no_text_and_refuses_an_intern() {
    let mut strings = NoStrings;
    assert_eq!(strings.text(0), None);
    let joined = InternedStr::new("s1::c1").expect("a short key");
    assert_eq!(strings.intern(joined), Err(ErrorCode::InvalidProgram));
}
