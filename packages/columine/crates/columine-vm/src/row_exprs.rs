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
//! same absent value an unmatched row has on any predicate column. A program
//! whose content ends with the reduce section carries no table and binds
//! nothing.

use columine_types::types::{ErrorCode, PROGRAM_HASH_PREFIX, ProgramHeader};

use crate::bytes;
use crate::vm::{col_at, col_u32_exact, u32s_as_bytes};

/// ASCII `R X P 1` in little-endian wire order.
pub const ROW_EXPRESSIONS_MAGIC: u32 = 0x3150_5852;

const TABLE_HEADER_BYTES: usize = 4 + 2 + 1;

/// One row of the batch as an expression sees it: the live state, the
/// batch's columns as the host passed them, and the row index.
pub struct RowContext<'a> {
    pub state: &'a [u8],
    pub cols: &'a [&'a [u8]],
    pub batch_len: u32,
    pub row: u32,
}

/// The embedder's expression evaluator.
pub trait RowExpression {
    /// Validate one entry's expression against the batch shape before any
    /// row is evaluated: every column it reads must cover `batch_len`, so
    /// per-row evaluation is infallible on that axis. A malformed expression
    /// is `InvalidProgram`; a short column is `ColumnUnderrun`.
    fn admit(&mut self, expr: &[u8], cols: &[&[u8]], batch_len: u32) -> Result<(), ErrorCode>;

    /// Evaluate the expression for one row and answer the u32 cell to store.
    /// A value the cell cannot hold is `InvalidCellValue`.
    fn eval(&mut self, expr: &[u8], ctx: &RowContext<'_>) -> Result<u32, ErrorCode>;
}

/// Storage for the derived columns of one batch. Owned by the embedder's
/// runtime and reused across batches so a bind allocates only when a batch
/// is wider or longer than every batch before it.
#[derive(Debug, Default)]
pub struct RowColumns {
    cells: Vec<Vec<u32>>,
}

impl RowColumns {
    pub const fn new() -> Self {
        Self { cells: Vec::new() }
    }

    /// The derived column bound at table position `entry`, for tests and
    /// diagnostics; the batch reads it through the spliced column set.
    pub fn column(&self, entry: usize) -> Option<&[u32]> {
        self.cells.get(entry).map(Vec::as_slice)
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
/// target column in `cols` with its derived cells. Every entry reads the
/// batch as the host passed it — a derived column is not an input of another
/// entry — so the table's order carries no meaning. `out` holds the cells
/// for as long as `cols` names them.
///
/// Errors leave `cols` untouched: the table is validated and every cell
/// computed before the first splice.
pub fn bind_row_columns<'a, E: RowExpression>(
    program: &[u8],
    state: &[u8],
    cols: &mut [&'a [u8]],
    batch_len: u32,
    eval: &mut E,
    out: &'a mut RowColumns,
) -> Result<(), ErrorCode> {
    let Some(table) = row_table(program)? else {
        return Ok(());
    };
    // An empty batch carries no rows to derive and may carry no column
    // pointers at all; the reduce section reads nothing from it either.
    if batch_len == 0 {
        return Ok(());
    }
    let count = usize::from(bytes::read_u16(table, 4));
    let type_col = usize::from(table[6]);
    if type_col >= cols.len() {
        return Err(ErrorCode::InvalidProgram);
    }
    out.cells.resize_with(count, Vec::new);

    let view: &[&[u8]] = cols;
    let type_cells =
        col_u32_exact(col_at(view, type_col), batch_len).ok_or(ErrorCode::ColumnUnderrun)?;
    let mut pos = TABLE_HEADER_BYTES;
    for cells in out.cells.iter_mut().take(count) {
        let entry = parse_entry(table, &mut pos)?;
        if usize::from(entry.target_col) >= view.len() {
            return Err(ErrorCode::InvalidProgram);
        }
        eval.admit(entry.expr, view, batch_len)?;
        cells.clear();
        cells.resize(batch_len as usize, 0);
        for (row, cell) in cells.iter_mut().enumerate() {
            if !entry.matches(type_cells[row]) {
                continue;
            }
            *cell = eval.eval(
                entry.expr,
                &RowContext {
                    state,
                    cols: view,
                    batch_len,
                    // `row < batch_len`, which is a u32.
                    #[allow(clippy::cast_possible_truncation)]
                    row: row as u32,
                },
            )?;
        }
    }
    if pos != table.len() {
        return Err(ErrorCode::InvalidProgram);
    }

    let bound: &'a RowColumns = out;
    let mut pos = TABLE_HEADER_BYTES;
    for cells in bound.cells.iter().take(count) {
        let entry = parse_entry(table, &mut pos)?;
        cols[usize::from(entry.target_col)] = u32s_as_bytes(cells);
    }
    Ok(())
}
