//! Row expressions — derived batch columns the VM computes per row.
//!
//! A program may carry, after its reduce section, a table of derived
//! columns. Each entry names the column it produces, the event types whose
//! rows it covers, and an opaque expression the embedder's [`RowExpression`]
//! evaluates against one row. [`bind_row_columns`] evaluates the table into
//! [`RowColumns`] and splices the results into the batch's column set, so the
//! reduce section reads a derived column exactly like an input column.
//!
//! WHY the VM computes these rather than the host: an expression may read
//! live state — the value a map already stores for the row's key — which
//! only exists at apply time, inside the VM. A host-side pre-evaluation
//! cannot see it, and a second evaluator on the host is a second set of
//! semantics to keep in parity. The expression language itself stays the
//! embedder's: this module owns only the table, the row gate, and the splice.
//!
//! ## Table encoding (little-endian, immediately after the reduce section)
//!
//! ```text
//! u32 magic "RXP1"
//! u16 count
//! u8  type_col            column holding each row's u32 event-type id
//! count × {
//!   u8  target_col        column the entry produces (replaced in the batch)
//!   u8  match_count       ≥ 1
//!   u32 × match_count     event-type ids whose rows are evaluated
//!   u16 expr_len
//!   expr_len bytes        opaque to this crate; the embedder's expression
//! }
//! ```
//!
//! Rows whose type id is not in the entry's match set keep the zero cell, the
//! same absent value an unmatched row has on any predicate column. Entries
//! bind in table order and a later entry reads an earlier entry's column
//! through [`BatchView::column`], so a derived value can feed a derived
//! predicate. A program whose content ends with the reduce section carries
//! no table and binds nothing.
//!
//! ## Bound and live entries
//!
//! An entry that reads only the batch is *bound*: its column is computed
//! once, before the reduce section runs. An entry that reads live state —
//! the value a map stores for the row's key — is *live*: [`admit`] answers
//! [`Binding::Live`], the bind leaves its column as zero cells, and the
//! reduce section evaluates the expression at the row where it reads the
//! cell, through [`LiveColumns`]. WHY: the reduce section applies rows in
//! order, and a live read at row `r` must see rows `0..r` of the same batch
//! already applied — otherwise the answer depends on where the host cut its
//! batches, and one batch of `n` rows and `n` batches of one row store
//! different values. A live column is read by the per-element conditional
//! opcodes (the `*_IF` family); the batch aggregates read the bound cells,
//! so an embedder lowers a state-reading expression only to a per-element
//! predicate.
//!
//! [`admit`]: RowExpression::admit

use columine_types::types::{ErrorCode, PROGRAM_HASH_PREFIX, ProgramHeader};

use crate::bytes;
use crate::vm::{col_at, col_u32, col_u32_exact, u32s_as_bytes};

/// ASCII `R X P 1` in little-endian wire order.
pub const ROW_EXPRESSIONS_MAGIC: u32 = 0x3150_5852;

const TABLE_HEADER_BYTES: usize = 4 + 2 + 1;

/// The batch as one entry sees it: the live state, the host's columns, and
/// the derived columns the entries before it already bound.
pub struct BatchView<'a> {
    pub state: &'a [u8],
    pub batch_len: u32,
    cols: &'a [&'a [u8]],
    /// Target column of each earlier entry, parallel to `derived`.
    derived_targets: &'a [u8],
    derived: &'a [Vec<u32>],
}

impl<'a> BatchView<'a> {
    /// The batch as the host passed it, with no derived columns bound yet —
    /// the view an embedder evaluates a standalone expression against.
    pub const fn host(state: &'a [u8], cols: &'a [&'a [u8]], batch_len: u32) -> Self {
        Self {
            state,
            batch_len,
            cols,
            derived_targets: &[],
            derived: &[],
        }
    }

    /// Column `col` as this entry reads it: an earlier entry's derived cells
    /// when one targets `col`, else the host's column (empty past the
    /// host's count, like [`col_at`]).
    pub fn column(&self, col: u8) -> &'a [u8] {
        match self.derived_targets.iter().position(|&t| t == col) {
            Some(i) => u32s_as_bytes(&self.derived[i]),
            None => col_at(self.cols, usize::from(col)),
        }
    }

    /// Number of columns the host passed.
    pub fn num_cols(&self) -> usize {
        self.cols.len()
    }
}

/// How an admitted entry binds: computed before the reduce section, or at
/// the row where the reduce section reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Binding {
    /// The expression reads only the batch: its column is bound up front.
    Bound,
    /// The expression reads live state: it is evaluated per row, after the
    /// rows before it applied.
    Live,
}

/// The embedder's expression evaluator.
pub trait RowExpression {
    /// Validate one entry's expression against the batch before any row is
    /// evaluated, and answer how it binds: every column it reads must cover
    /// `batch_len`, so per-row evaluation is infallible on that axis. A
    /// malformed expression is `InvalidProgram`; a short column is
    /// `ColumnUnderrun`.
    fn admit(&mut self, expr: &[u8], batch: &BatchView<'_>) -> Result<Binding, ErrorCode>;

    /// Evaluate the expression for one row and answer the u32 cell to store.
    /// A value the cell cannot hold is `InvalidCellValue`.
    fn eval(&mut self, expr: &[u8], batch: &BatchView<'_>, row: u32) -> Result<u32, ErrorCode>;
}

/// Storage for the derived columns of one batch. Owned by the embedder's
/// runtime and reused across batches so a bind allocates only when a batch
/// is wider or longer than every batch before it.
#[derive(Debug, Default)]
pub struct RowColumns {
    cells: Vec<Vec<u32>>,
    targets: Vec<u8>,
    /// The entries the last bind deferred, in table order.
    live: Vec<LiveEntry>,
    /// Where the last bind's table starts in its program, and the batch it
    /// bound — the live reads re-parse the entry from there.
    table_start: usize,
    type_col: u8,
    batch_len: u32,
}

/// One deferred entry: its column and where it sits in the table.
#[derive(Debug, Clone, Copy)]
struct LiveEntry {
    target_col: u8,
    /// Offset of the entry within the table.
    pos: usize,
}

impl RowColumns {
    pub const fn new() -> Self {
        Self {
            cells: Vec::new(),
            targets: Vec::new(),
            live: Vec::new(),
            table_start: 0,
            type_col: 0,
            batch_len: 0,
        }
    }

    /// The derived column bound at table position `entry`, for tests and
    /// diagnostics; the batch reads it through the spliced column set.
    pub fn column(&self, entry: usize) -> Option<&[u32]> {
        self.cells.get(entry).map(Vec::as_slice)
    }

    /// Replace every target column in `cols` with the cells the last bind
    /// computed — the splice [`bind_row_columns`] performs, repeated for a
    /// host that rebuilds its column set from the same batch (a resumed
    /// run must see the derived columns the run it resumes saw).
    pub fn splice<'a>(&'a self, cols: &mut [&'a [u8]]) -> Result<(), ErrorCode> {
        for (cells, &target) in self.cells.iter().zip(&self.targets) {
            let col = cols
                .get_mut(usize::from(target))
                .ok_or(ErrorCode::InvalidProgram)?;
            *col = u32s_as_bytes(cells);
        }
        Ok(())
    }
}

/// The live columns of one batch: the entries the bind deferred, resolved
/// per row when the reduce section reads them, against the state as it
/// stands at that row. [`bind_row_columns`] answers one per bind; the
/// reduce section runs with it (`Vm::execute_batch_live`).
pub struct LiveColumns<'a, 'e> {
    rows: &'a RowColumns,
    program: &'a [u8],
    /// The evaluator, borrowed for the handle's life only — dropping the
    /// handle hands it back, whatever the bound columns still cover.
    eval: &'e mut dyn RowExpression,
}

impl LiveColumns<'_, '_> {
    /// Whether the bind deferred any entry to the reduce section.
    pub fn is_empty(&self) -> bool {
        self.rows.live.is_empty()
    }

    /// The cell of live column `col` at `row`, evaluated now, or `None` when
    /// `col` is not a live column and the batch's bytes hold its cell.
    /// `cols` is the spliced column set the reduce section reads (a live
    /// expression may read the bound columns); a row whose type is not in
    /// the entry's match set, or that lies past the bound batch (a
    /// flat-mapped child row), is the zero cell, as it is for a bound entry.
    pub fn cell(
        &mut self,
        col: u8,
        row: u32,
        state: &[u8],
        cols: &[&[u8]],
    ) -> Option<Result<u32, ErrorCode>> {
        let entry = self.rows.live.iter().find(|e| e.target_col == col)?;
        let rows = self.rows;
        let type_cell = col_u32(col_at(cols, usize::from(rows.type_col)), rows.batch_len)
            .get(row as usize)
            .copied();
        Some(self.eval_entry(*entry, row, type_cell, state, cols))
    }

    fn eval_entry(
        &mut self,
        entry: LiveEntry,
        row: u32,
        type_cell: Option<u32>,
        state: &[u8],
        cols: &[&[u8]],
    ) -> Result<u32, ErrorCode> {
        let rows = self.rows;
        // The bind parsed this entry from the same bytes; a program that no
        // longer holds it is the caller's mismatch, refused like a bad table.
        let table = self
            .program
            .get(rows.table_start..)
            .ok_or(ErrorCode::InvalidProgram)?;
        let mut pos = entry.pos;
        let parsed = parse_entry(table, &mut pos)?;
        if !type_cell.is_some_and(|t| parsed.matches(t)) {
            return Ok(0);
        }
        let batch = BatchView::host(state, cols, rows.batch_len);
        self.eval.eval(parsed.expr, &batch, row)
    }
}

struct Entry<'a> {
    target_col: u8,
    /// Raw `u32le × match_count` bytes; decoded per comparison so the entry
    /// borrows the program instead of collecting the ids.
    match_ids: &'a [u8],
    expr: &'a [u8],
}

impl Entry<'_> {
    fn matches(&self, type_id: u32) -> bool {
        self.match_ids
            .as_chunks::<4>()
            .0
            .iter()
            .any(|id| u32::from_le_bytes(*id) == type_id)
    }
}

/// The row-expression table of `program`: `None` when the content ends with
/// the reduce section. A program too short for its own header, or a trailer
/// that does not open with the table magic, is `InvalidProgram`.
fn row_table(program: &[u8]) -> Result<Option<&[u8]>, ErrorCode> {
    let content = program
        .get(PROGRAM_HASH_PREFIX as usize..)
        .ok_or(ErrorCode::InvalidProgram)?;
    let header_bytes = content
        .get(..ProgramHeader::WIRE_SIZE)
        .and_then(|bytes| <[u8; ProgramHeader::WIRE_SIZE]>::try_from(bytes).ok())
        .ok_or(ErrorCode::InvalidProgram)?;
    let header = ProgramHeader::from_wire_bytes(header_bytes);
    let trailer_start = ProgramHeader::WIRE_SIZE
        + usize::from(header.init_code_len)
        + usize::from(header.reduce_code_len);
    let trailer = content
        .get(trailer_start..)
        .ok_or(ErrorCode::InvalidProgram)?;
    if trailer.is_empty() {
        return Ok(None);
    }
    if trailer.len() < TABLE_HEADER_BYTES || bytes::read_u32(trailer, 0) != ROW_EXPRESSIONS_MAGIC {
        return Err(ErrorCode::InvalidProgram);
    }
    Ok(Some(trailer))
}

/// Parse one entry at `*pos`, advancing past it.
fn parse_entry<'a>(table: &'a [u8], pos: &mut usize) -> Result<Entry<'a>, ErrorCode> {
    let head = table.get(*pos..*pos + 2).ok_or(ErrorCode::InvalidProgram)?;
    let (target_col, match_count) = (head[0], usize::from(head[1]));
    if match_count == 0 {
        return Err(ErrorCode::InvalidProgram);
    }
    let ids_start = *pos + 2;
    let ids_end = ids_start + match_count * 4;
    let match_ids = table
        .get(ids_start..ids_end)
        .ok_or(ErrorCode::InvalidProgram)?;
    let expr_len = table
        .get(ids_end..ids_end + 2)
        .map(|b| usize::from(u16::from_le_bytes([b[0], b[1]])))
        .ok_or(ErrorCode::InvalidProgram)?;
    let expr_start = ids_end + 2;
    let expr = table
        .get(expr_start..expr_start + expr_len)
        .ok_or(ErrorCode::InvalidProgram)?;
    *pos = expr_start + expr_len;
    Ok(Entry {
        target_col,
        match_ids,
        expr,
    })
}

/// Evaluate `program`'s row-expression table over the batch and replace each
/// target column in `cols` with its derived cells. Entries bind in table
/// order; a later entry reads an earlier one's column through
/// [`BatchView::column`]. `out` holds the cells for as long as `cols` names
/// them. The answer is the batch's live columns — the entries deferred to
/// the reduce section — which the reduce section runs with; it is empty
/// when every entry bound up front.
///
/// Errors leave `cols` untouched: the table is validated and every cell
/// computed before the first splice.
pub fn bind_row_columns<'a, 'e, E: RowExpression>(
    program: &'a [u8],
    state: &[u8],
    cols: &mut [&'a [u8]],
    batch_len: u32,
    eval: &'e mut E,
    out: &'a mut RowColumns,
) -> Result<LiveColumns<'a, 'e>, ErrorCode> {
    // `out` describes this bind only: a table-less program or an empty batch
    // must not leave the previous batch's targets or live entries behind.
    out.targets.clear();
    out.live.clear();
    let Some(table) = row_table(program)? else {
        return Ok(LiveColumns {
            rows: out,
            program,
            eval,
        });
    };
    // An empty batch carries no rows to derive and may carry no column
    // pointers at all; the reduce section reads nothing from it either.
    if batch_len == 0 {
        return Ok(LiveColumns {
            rows: out,
            program,
            eval,
        });
    }
    let count = usize::from(bytes::read_u16(table, 4));
    let type_col = usize::from(table[6]);
    if type_col >= cols.len() {
        return Err(ErrorCode::InvalidProgram);
    }
    out.cells.resize_with(count, Vec::new);
    out.targets.resize(count, 0);
    out.table_start = program.len() - table.len();
    out.type_col = table[6];
    out.batch_len = batch_len;

    let view: &[&[u8]] = cols;
    let type_cells =
        col_u32_exact(col_at(view, type_col), batch_len).ok_or(ErrorCode::ColumnUnderrun)?;
    let mut pos = TABLE_HEADER_BYTES;
    for i in 0..count {
        let entry_pos = pos;
        let entry = parse_entry(table, &mut pos)?;
        if usize::from(entry.target_col) >= view.len() {
            return Err(ErrorCode::InvalidProgram);
        }
        out.targets[i] = entry.target_col;
        // The entries before `i` are final; this entry reads them and writes
        // only its own cells.
        let (done, rest) = out.cells.split_at_mut(i);
        let batch = BatchView {
            state,
            batch_len,
            cols: view,
            derived_targets: &out.targets[..i],
            derived: done,
        };
        let binding = eval.admit(entry.expr, &batch)?;
        let cells = &mut rest[0];
        cells.clear();
        cells.resize(batch_len as usize, 0);
        if binding == Binding::Live {
            // The zero cells stand in for the column the reduce section
            // reads live; they also keep the column covering the batch.
            out.live.push(LiveEntry {
                target_col: entry.target_col,
                pos: entry_pos,
            });
            continue;
        }
        for (row, cell) in cells.iter_mut().enumerate() {
            if !entry.matches(type_cells[row]) {
                continue;
            }
            // `row < batch_len`, which is a u32.
            #[allow(clippy::cast_possible_truncation)]
            let row = row as u32;
            *cell = eval.eval(entry.expr, &batch, row)?;
        }
    }
    if pos != table.len() {
        return Err(ErrorCode::InvalidProgram);
    }

    let bound: &'a RowColumns = out;
    bound.splice(cols)?;
    Ok(LiveColumns {
        rows: bound,
        program,
        eval,
    })
}
