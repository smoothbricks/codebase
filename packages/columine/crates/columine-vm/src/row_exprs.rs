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
//! u32   magic "RXP1" or "RXP2"
//! u16   count
//! index type_col          column holding each row's u32 event-type id
//! count × {
//!   index target_col      column the entry produces (replaced in the batch)
//!   u8    lane            RXP2 only: the entry's cell lane, a `CmpType`
//!   index match_count     ≥ 1
//!   u32 × match_count     event-type ids whose rows are evaluated
//!   u16 expr_len
//!   expr_len bytes        opaque to this crate; the embedder's expression
//! }
//! ```
//!
//! An `index` is an unsigned LEB128 `u32` ([`columine_types::operand`]), the
//! encoding every column and count operand of a program shares.
//!
//! Rows whose type id is not in the entry's match set keep the zero cell, the
//! same absent value an unmatched row has on any predicate column. Entries
//! bind in table order and a later entry reads an earlier entry's column
//! through [`BatchView::column`], so a derived value can feed a derived
//! predicate. A program whose content ends with the reduce section carries
//! no table and binds nothing.
//!
//! ## Cell lanes
//!
//! An entry's lane is the cell its column holds: a u32, or an f64 or an i64
//! in eight little-endian bytes, spelled as the [`CmpType`] a comparing
//! upsert names its input with. Every RXP1 entry is u32, and a compiler
//! writes RXP2 only when an entry is wide. The evaluator answers each row in
//! its entry's lane ([`RowCell`]); the bind refuses a cell of another lane
//! with `InvalidCellValue`. The reduce section reads a wide column as it
//! reads a host column of that lane: an f64 or i64 aggregate, a FLOAT64 or
//! INT64 struct-map or struct-list field, a comparison or timestamp operand
//! of that lane. A program that reads a wide column at any other lane is
//! refused with `InvalidProgram` (see [`column_uses`]): a u32 operand would
//! take half a cell as data. The table's type column is u32.
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
//! opcodes (the `*_IF` family) or by a top-level stored-row sweep. The sweep
//! supplies [`StoredRow`] for each live stored key and the last input row
//! for the logical clock; the struct-map upsert-last-if binds the
//! destination's stored row for the element's key, or
//! [`StoredBinding::Absent`] when the key holds none. Batch aggregates still
//! read bound cells.
//! A live column read by anything else — an aggregate, an
//! aggregate's `*_IF` predicate, a key, value, timestamp or element operand,
//! the `FOR_EACH` type column, the table's own type column — would see the
//! zero placeholder; the bind refuses such a program with `InvalidProgram`
//! (see [`column_uses`]) so the zeros can never be mistaken for an answer.
//!
//! [`admit`]: RowExpression::admit
//! [`column_uses`]: crate::column_uses

use columine_types::operand::Operands;
use columine_types::types::{
    CmpType, ErrorCode, ProgramHeader, SlotType, StructFieldType, is_arena_field_type,
    struct_field_size,
};

use crate::bytes;
use crate::column_uses::{ColumnUse, reduce_column_reads};
use crate::key_index::{InternBytes, KeyCx, KeyIndex, NoStrings};
use crate::logical_export::Interner;
use crate::struct_map::{StructMap2Slot, StructMapSlot, StructRows};
use crate::vm::{col_at, col_u32, col_u32_exact, u64s_as_bytes};

pub use columine_types::types::{LANED_ROW_EXPRESSIONS_MAGIC, ROW_EXPRESSIONS_MAGIC};

/// Magic and entry count: the fixed part of the table header.
const TABLE_FIXED_BYTES: usize = 4 + 2;

/// One live stored row, borrowing the state image: the row geometry of its
/// slot, the ordinal its key index named, and its keys. The sweep constructs
/// this view for the row it visits; an embedder evaluating an expression
/// over a frozen stored row constructs it with lookup, which validates the
/// slot first.
pub struct StoredRow<'a> {
    state: &'a [u8],
    rows: StructRows,
    position: u32,
    key: u32,
    key2: Option<u32>,
}

impl<'a> StoredRow<'a> {
    pub(crate) const fn new(
        state: &'a [u8],
        rows: StructRows,
        position: u32,
        key: u32,
        key2: Option<u32>,
    ) -> Self {
        Self {
            state,
            rows,
            position,
            key,
            key2,
        }
    }

    /// The stored row of a struct map at the given key, or None when the
    /// key is absent. One-key maps take key2: None; two-key maps require
    /// Some(key2) — a mismatch with the slot's kind is InvalidProgram.
    /// The slot's geometry is validated before the probe, so a malformed
    /// image is a typed error, never a wild read.
    pub fn lookup(
        state: &'a [u8],
        slot: u32,
        key: u32,
        key2: Option<u32>,
    ) -> Result<Option<Self>, ErrorCode> {
        crate::struct_map::hash_call(|cx| Self::lookup_in(state, slot, key, key2, cx))
    }

    /// [`Self::lookup`] in an image of key-index family `I`, resolving
    /// interned key lanes through `cx`.
    pub fn lookup_in<I: KeyIndex, S: InternBytes + ?Sized>(
        state: &'a [u8],
        slot: u32,
        key: u32,
        key2: Option<u32>,
        cx: &mut KeyCx<'_, I, S>,
    ) -> Result<Option<Self>, ErrorCode> {
        let meta = crate::state_init::bind_slot_in::<I>(state, slot)?;
        match (meta.slot_type(), key2) {
            (SlotType::StructMap, None) => {
                let map = StructMapSlot::<I>::bind_in(state, slot);
                Ok(map
                    .lookup(state, key, cx)?
                    .map(|position| Self::new(state, map.rows(), position, key, None)))
            }
            (SlotType::StructMap2, Some(key2)) => {
                let map = StructMap2Slot::<I>::bind_in(state, slot);
                Ok(map
                    .lookup(state, key, key2, cx)?
                    .map(|position| Self::new(state, map.rows(), position, key, Some(key2))))
            }
            _ => Err(ErrorCode::InvalidProgram),
        }
    }

    pub fn key(&self) -> u32 {
        self.key
    }

    /// The second key lane of a two-key stored row.
    pub fn key2(&self) -> Option<u32> {
        self.key2
    }

    pub const fn num_fields(&self) -> u8 {
        self.rows.num_fields
    }

    pub fn field_type(&self, field: u8) -> Result<StructFieldType, ErrorCode> {
        if field >= self.num_fields() {
            return Err(ErrorCode::InvalidProgram);
        }
        Ok(self.rows.field_type(self.state, field))
    }

    /// Present scalar cell bytes or an arena field's borrowed payload.
    /// An absent field is `None`; an invalid field ordinal is a typed error.
    pub fn field(&self, field: u8) -> Result<Option<&'a [u8]>, ErrorCode> {
        let kind = self.field_type(field)?;
        if is_arena_field_type(kind) {
            return self.rows.arena_field(self.state, self.position, field);
        }
        let row = self.rows.row_off(self.position);
        if !StructMapSlot::is_field_set(self.state, row, field) {
            return Ok(None);
        }
        let offset = row + self.rows.field_offset(self.state, field);
        let end = offset + struct_field_size(kind);
        let offset = usize::try_from(offset).map_err(|_| ErrorCode::InvalidState)?;
        let end = usize::try_from(end).map_err(|_| ErrorCode::InvalidState)?;
        self.state
            .get(offset..end)
            .map(Some)
            .ok_or(ErrorCode::InvalidState)
    }
}

/// What `_` binds to while an expression evaluates: the stored row a
/// predicate competes with, when the instruction evaluating it has one.
pub enum StoredBinding<'a> {
    /// No stored row is in scope: ordinary row evaluation and admission.
    Unbound,
    /// The instruction's key holds no stored row: `_` is null, and every
    /// field read through it is null.
    Absent,
    /// The stored row in scope: a sweep's visited row, or the row an
    /// upsert-last-if's key already holds.
    Row(StoredRow<'a>),
}

/// The batch as one entry sees it: the live state, the host's columns, and
/// the derived columns the entries before it already bound.
pub struct BatchView<'a> {
    pub state: &'a [u8],
    pub batch_len: u32,
    cols: &'a [&'a [u8]],
    /// Target column of each earlier entry, parallel to `derived_lanes` and
    /// `derived`.
    derived_targets: &'a [u32],
    derived_lanes: &'a [CmpType],
    derived: &'a [Vec<u64>],
    stored: StoredBinding<'a>,
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
            derived_lanes: &[],
            derived: &[],
            stored: StoredBinding::Unbound,
        }
    }

    /// Column `col` as this entry reads it: an earlier entry's derived cells
    /// when one targets `col`, else the host's column (empty past the
    /// host's count, like [`col_at`]).
    pub fn column(&self, col: u32) -> &'a [u8] {
        match self.derived_targets.iter().position(|&t| t == col) {
            Some(i) => lane_bytes(&self.derived[i], self.derived_lanes[i], self.batch_len),
            None => col_at(self.cols, col as usize),
        }
    }

    /// The lane of column `col` when an earlier entry derived it; `None` for
    /// a host column, whose lane only the program's readers name.
    pub fn derived_lane(&self, col: u32) -> Option<CmpType> {
        let i = self.derived_targets.iter().position(|&t| t == col)?;
        Some(self.derived_lanes[i])
    }

    /// The stored row in scope, when there is one: absent during ordinary
    /// row evaluation and admission, and when an upsert-last-if's key holds
    /// no row. The view cannot outlive this batch borrow.
    pub fn stored_row(&self) -> Option<&StoredRow<'a>> {
        match &self.stored {
            StoredBinding::Row(row) => Some(row),
            StoredBinding::Unbound | StoredBinding::Absent => None,
        }
    }

    /// What `_` binds to in this evaluation: an expression that reads the
    /// stored row tells an absent key (`_` is null) from an evaluation with
    /// no stored row in scope (a program error).
    pub fn stored(&self) -> &StoredBinding<'a> {
        &self.stored
    }

    /// Attach a stored row for stored-row expressions: the same read the
    /// sweep supplies per visited row, for an embedder evaluating an
    /// expression over a frozen stored row.
    pub fn with_stored_row(mut self, row: StoredRow<'a>) -> Self {
        self.stored = StoredBinding::Row(row);
        self
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

/// One row's cell, in the lane of the entry it answers.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RowCell {
    U32(u32),
    F64(f64),
    I64(i64),
}

/// The embedder's expression evaluator. Its answer for an expression is a
/// function of the expression bytes, the batch's columns and the state at
/// the row: a resume over retained cells evaluates its deferred entries
/// with whatever evaluator it is handed, so every evaluator one embedder
/// passes for a program implements one expression language.
pub trait RowExpression {
    /// Validate one entry's expression against the batch before any row is
    /// evaluated, and answer how it binds: every column it reads must cover
    /// `batch_len`, so per-row evaluation is infallible on that axis. `lane`
    /// is the cell the entry's column holds. A malformed expression, or one
    /// that cannot answer `lane`, is `InvalidProgram`; a short column is
    /// `ColumnUnderrun`.
    fn admit(
        &mut self,
        expr: &[u8],
        lane: CmpType,
        batch: &BatchView<'_>,
    ) -> Result<Binding, ErrorCode>;

    /// Evaluate the expression for one row and answer the cell to store, in
    /// `lane`. A value `lane` cannot hold is `InvalidCellValue`, and the bind
    /// refuses a cell of another lane the same way. `strings` names the text
    /// of an id cell and interns a string the expression builds, answered as
    /// its u32 id (specs/columine/09-row-interning.md); a live evaluation is
    /// handed [`NoStrings`].
    fn eval(
        &mut self,
        expr: &[u8],
        lane: CmpType,
        batch: &BatchView<'_>,
        row: u32,
        strings: &mut dyn Interner,
    ) -> Result<RowCell, ErrorCode>;
}

/// Storage for the derived columns of one batch. Owned by the embedder's
/// runtime and reused across batches so a bind allocates only when a batch
/// is wider or longer than every batch before it.
#[derive(Debug)]
pub struct RowColumns {
    /// Each entry's cells in 8-byte words, so an f64 or i64 column is
    /// aligned for its typed view: two u32 cells to a word, or one wide cell.
    cells: Vec<Vec<u64>>,
    /// Each entry's lane, parallel to `cells` and `targets`.
    lanes: Vec<CmpType>,
    targets: Vec<u32>,
    /// The entries the last bind deferred, in table order, parsed once at
    /// bind: the per-row read indexes them, it never re-parses the table.
    live: Vec<LiveEntry>,
    /// Column → position in `live`, [`NOT_LIVE`] for every other column of
    /// the batch the last bind ran over. WHY a table and not a scan: the read
    /// runs once per row per conditional opcode, so it answers in one load;
    /// the table grows to the widest batch once and is reused.
    live_index: Vec<u32>,
    type_col: u32,
    batch_len: u32,
    /// Whether the last bind completed: a failed bind leaves partial cells
    /// that no splice may hand out as that bind's.
    bound: bool,
    /// The row table the last bind evaluated, and where it sat in its
    /// program. The cells, and which entries were deferred, are a function
    /// of these bytes, so a resume is admitted only over the same table at
    /// the same position.
    table: Vec<u8>,
    table_start: usize,
}

/// `live_index` value of a column no live entry targets.
const NOT_LIVE: u32 = u32::MAX;

/// One deferred entry, as parsed at bind: byte ranges into the program the
/// bind ran against, so the read borrows the ids and the expression without
/// copying either.
#[derive(Debug, Clone, Copy)]
struct LiveEntry {
    /// Program range of the raw `u32le × match_count` ids.
    match_ids: Range,
    /// Program range of the expression bytes.
    expr: Range,
    /// The entry's lane: a live cell is read as a u32 predicate.
    lane: CmpType,
}

/// A `start..end` byte range, `Copy` so an entry stays a plain record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Range {
    start: usize,
    end: usize,
}

impl Default for RowColumns {
    fn default() -> Self {
        Self::new()
    }
}

impl RowColumns {
    pub const fn new() -> Self {
        Self {
            cells: Vec::new(),
            lanes: Vec::new(),
            targets: Vec::new(),
            live: Vec::new(),
            live_index: Vec::new(),
            type_col: 0,
            batch_len: 0,
            bound: false,
            table: Vec::new(),
            table_start: 0,
        }
    }

    /// The live columns of a batch that resumes after this bind: the
    /// deferred entries evaluate again, at their rows, against the state as
    /// it stands when the resumed run reaches them. Refused unless these are
    /// the cells of a completed bind of `program`'s table over `batch_len`
    /// rows (see [`Self::bound_for`]).
    pub(crate) fn resume<'a, 'e>(
        &'a self,
        program: &'a [u8],
        batch_len: u32,
        eval: &'e mut dyn RowExpression,
    ) -> Result<LiveColumns<'a, 'e>, ErrorCode> {
        self.bound_for(program, batch_len)?;
        Ok(LiveColumns {
            rows: self,
            program,
            eval,
        })
    }

    /// Whether these are the cells of a completed bind of `program`'s table
    /// over a batch of `batch_len` rows: the same table bytes at the same
    /// program position. The targets, the type column, which entries were
    /// deferred and the program ranges a live read indexes all follow from
    /// those bytes, so a program of the same table shape with other
    /// expressions is refused. A batch with no rows binds nothing, and a
    /// program without a table needs no bind.
    fn bound_for(&self, program: &[u8], batch_len: u32) -> Result<(), ErrorCode> {
        let (table, table_start) = match row_table(program)? {
            None if self.targets.is_empty() => return Ok(()),
            Some(_) if batch_len == 0 && self.bound && self.targets.is_empty() => return Ok(()),
            Some(table) if batch_len != 0 => table,
            _ => return Err(ErrorCode::InvalidProgram),
        };
        if !self.bound
            || self.batch_len != batch_len
            || self.table_start != table_start
            || self.table != table
        {
            return Err(ErrorCode::InvalidProgram);
        }
        Ok(())
    }

    /// Forget the last bind's live entries; the next bind records its own.
    fn clear_live(&mut self) {
        self.live.clear();
        self.live_index.clear();
    }

    /// The position in `live` of the entry targeting `col`, if any.
    fn live_entry(&self, col: u32) -> Option<usize> {
        let index = *self.live_index.get(col as usize)?;
        (index != NOT_LIVE).then_some(index as usize)
    }

    /// The lane of the wide entry whose cells column `col` holds after the
    /// splice: the last entry targeting a column is the one spliced.
    fn wide_lane(&self, col: u32) -> Option<CmpType> {
        let entry = self.targets.iter().rposition(|&target| target == col)?;
        Some(self.lanes[entry]).filter(|&lane| lane != CmpType::U32)
    }

    /// Replace every target column in `cols` with the cells the last bind
    /// computed — the splice [`bind_row_columns`] performs, repeated for a
    /// host that rebuilds its column set from the same batch (a resumed
    /// run must see the derived columns the run it resumes saw).
    pub fn splice<'a>(&'a self, cols: &mut [&'a [u8]]) -> Result<(), ErrorCode> {
        let entries = self.cells.iter().zip(&self.lanes).zip(&self.targets);
        for ((cells, &lane), &target) in entries {
            let col = cols
                .get_mut(target as usize)
                .ok_or(ErrorCode::InvalidProgram)?;
            *col = lane_bytes(cells, lane, self.batch_len);
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

    /// Whether the bind deferred `col` to the reduce section: its cell is
    /// evaluated at the row, never read from the batch.
    pub(crate) fn defers(&self, col: u32) -> bool {
        self.rows.live_entry(col).is_some()
    }

    /// The cell of live column `col` at `row`, evaluated now, or `None` when
    /// `col` is not a live column and the batch's bytes hold its cell.
    /// `cols` is the spliced column set the reduce section reads (a live
    /// expression may read the bound columns); a row whose type is not in
    /// the entry's match set, or that lies past the bound batch (a
    /// flat-mapped child row), is the zero cell, as it is for a bound entry.
    pub fn cell(
        &mut self,
        col: u32,
        row: u32,
        state: &[u8],
        cols: &[&[u8]],
    ) -> Option<Result<u32, ErrorCode>> {
        self.cell_at(col, row, state, cols, StoredBinding::Unbound)
    }

    pub(crate) fn stored_cell<'a>(
        &mut self,
        col: u32,
        row: u32,
        state: &'a [u8],
        cols: &'a [&'a [u8]],
        stored: StoredBinding<'a>,
    ) -> Option<Result<u32, ErrorCode>> {
        self.cell_at(col, row, state, cols, stored)
    }

    fn cell_at<'a>(
        &mut self,
        col: u32,
        row: u32,
        state: &'a [u8],
        cols: &'a [&'a [u8]],
        stored: StoredBinding<'a>,
    ) -> Option<Result<u32, ErrorCode>> {
        let rows = self.rows;
        let entry = rows.live[rows.live_entry(col)?];
        // A live cell is a predicate's, read as a u32: the bind refuses a
        // program that reads a wide live column, so only a direct read of
        // one lands here.
        if entry.lane != CmpType::U32 {
            return Some(Err(ErrorCode::InvalidProgram));
        }
        let type_cell = col_u32(col_at(cols, rows.type_col as usize), rows.batch_len)
            .get(row as usize)
            .copied();
        // The bind cut these ranges from this program: `program` is the
        // slice the handle was bound over, so the indexing is exact.
        let match_ids = &self.program[entry.match_ids.start..entry.match_ids.end];
        if !type_cell.is_some_and(|t| matches(match_ids, t)) {
            return Some(Ok(0));
        }
        let expr = &self.program[entry.expr.start..entry.expr.end];
        let mut batch = BatchView::host(state, cols, rows.batch_len);
        batch.stored = stored;
        Some(
            match self
                .eval
                .eval(expr, CmpType::U32, &batch, row, &mut NoStrings)
            {
                Ok(RowCell::U32(cell)) => Ok(cell),
                Ok(RowCell::F64(_) | RowCell::I64(_)) => Err(ErrorCode::InvalidCellValue),
                Err(code) => Err(code),
            },
        )
    }
}

/// Whether `type_id` is among the raw `u32le × n` ids; decoded per
/// comparison so the entry borrows the program instead of collecting them.
fn matches(match_ids: &[u8], type_id: u32) -> bool {
    match_ids
        .as_chunks::<4>()
        .0
        .iter()
        .any(|id| u32::from_le_bytes(*id) == type_id)
}

struct Entry<'a> {
    target_col: u32,
    lane: CmpType,
    /// Raw `u32le × match_count` bytes; decoded per comparison so the entry
    /// borrows the program instead of collecting the ids.
    match_ids: &'a [u8],
    expr: &'a [u8],
    /// Where `match_ids` and `expr` sit within the table, for a live entry
    /// to record and read back without re-parsing.
    match_ids_at: Range,
    expr_at: Range,
}

impl Entry<'_> {
    fn matches(&self, type_id: u32) -> bool {
        matches(self.match_ids, type_id)
    }
}

/// The row-expression table of \`program\` and its offset in the program:
/// \`None\` when the tail opens with no row table (it ends with the reduce
/// section, or carries only extension tables). A program too short for its
/// own header, or a tail that is not a row table and whole extension tables
/// ([\`ProgramTail\`]), is \`InvalidProgram\`.
pub(crate) fn row_table(program: &[u8]) -> Result<Option<(&[u8], usize)>, ErrorCode> {
    let tail = ProgramHeader::sections(program)
        .and_then(|sections| sections.tables())
        .ok_or(ErrorCode::InvalidProgram)?;
    let Some(table) = tail.row_table else {
        return Ok(None);
    };
    if table.len() < TABLE_FIXED_BYTES {
        return Err(ErrorCode::InvalidProgram);
    }
    // The row table opens the tail, which runs to the program's end.
    Ok(Some((table, program.len() - tail.len())))
}

/// Parse one entry at `*pos`, advancing past it. An RXP2 entry (`laned`)
/// names its lane after its target column; an RXP1 entry is u32.
fn parse_entry<'a>(table: &'a [u8], pos: &mut usize, laned: bool) -> Result<Entry<'a>, ErrorCode> {
    let mut operands = Operands::at(table, *pos);
    let target_col = operands.index().ok_or(ErrorCode::InvalidProgram)?;
    let lane = if laned {
        operands
            .byte()
            .and_then(CmpType::from_u8)
            .ok_or(ErrorCode::InvalidProgram)?
    } else {
        CmpType::U32
    };
    let match_count = operands
        .index()
        .and_then(|count| usize::try_from(count).ok())
        .ok_or(ErrorCode::InvalidProgram)?;
    if match_count == 0 {
        return Err(ErrorCode::InvalidProgram);
    }
    let ids_start = operands.pos();
    let match_ids = operands
        .bytes(
            match_count
                .checked_mul(4)
                .ok_or(ErrorCode::InvalidProgram)?,
        )
        .ok_or(ErrorCode::InvalidProgram)?;
    let ids_end = operands.pos();
    let expr_len = usize::from(operands.u16().ok_or(ErrorCode::InvalidProgram)?);
    let expr_start = operands.pos();
    let expr = operands.bytes(expr_len).ok_or(ErrorCode::InvalidProgram)?;
    *pos = operands.pos();
    Ok(Entry {
        target_col,
        lane,
        match_ids,
        expr,
        match_ids_at: Range {
            start: ids_start,
            end: ids_end,
        },
        expr_at: Range {
            start: expr_start,
            end: expr_start + expr_len,
        },
    })
}

impl Range {
    /// The same range shifted by `offset`: a table range as a program range.
    const fn shifted(self, offset: usize) -> Self {
        Self {
            start: self.start + offset,
            end: self.end + offset,
        }
    }
}

/// Refuse, before any column is spliced, a program whose reduce section
/// reads a derived column its cells cannot answer:
///
/// - A live column holds zero placeholder cells, which a per-element `*_IF`
///   predicate or a stored-row sweep resolves through the handle. The
///   table's type column and every plain read ([`ColumnUse::Read`]) would
///   take the zeros as cells.
/// - A wide column holds 8-byte cells of its lane. An operand of another
///   lane would take half a cell, or the other number type, as data, and
///   the table's type column is u32. The handle answers u32 predicate cells,
///   so a wide live column has no reader at all.
fn refuse_unreadable_columns(
    program: &[u8],
    state: &[u8],
    out: &RowColumns,
) -> Result<(), ErrorCode> {
    let has_wide = out.lanes.iter().any(|&lane| lane != CmpType::U32);
    if out.live.is_empty() && !has_wide {
        return Ok(());
    }
    if out.live_entry(out.type_col).is_some() || (has_wide && out.wide_lane(out.type_col).is_some())
    {
        return Err(ErrorCode::InvalidProgram);
    }
    let init = ProgramHeader::sections(program)
        .ok_or(ErrorCode::InvalidProgram)?
        .init_code;
    let mut unreadable = false;
    reduce_column_reads(program, state, &mut |col, usage, lane| {
        let plain_read_of_live = usage == ColumnUse::Read && out.live_entry(col).is_some();
        let off_lane = has_wide
            && out
                .wide_lane(col)
                .is_some_and(|wide| lane.resolve(init) != Some(wide));
        unreadable |= plain_read_of_live || off_lane;
    })?;
    if unreadable {
        return Err(ErrorCode::InvalidProgram);
    }
    Ok(())
}

/// Back `additional` more elements of `buf`, refusing with `OutOfMemory`.
fn room<T>(buf: &mut Vec<T>, additional: usize) -> Result<(), ErrorCode> {
    buf.try_reserve(additional)
        .map_err(|_| ErrorCode::OutOfMemory)
}

/// The first `batch_len` cells of an entry's words, in `lane`.
fn lane_bytes(words: &[u64], lane: CmpType, batch_len: u32) -> &[u8] {
    &u64s_as_bytes(words)[..batch_len as usize * lane.stride()]
}

/// Store `cell` as row `row` of an entry's words, which start zeroed and take
/// each row once. A cell of another lane than the entry's is
/// `InvalidCellValue`.
fn store(words: &mut [u64], lane: CmpType, row: usize, cell: RowCell) -> Result<(), ErrorCode> {
    match (lane, cell) {
        // Two u32 cells to a little-endian word: the even row is its low half.
        (CmpType::U32, RowCell::U32(value)) => {
            words[row / 2] |= u64::from(value) << ((row % 2) * 32);
        }
        (CmpType::F64, RowCell::F64(value)) => words[row] = value.to_bits(),
        (CmpType::I64, RowCell::I64(value)) => words[row] = value.cast_unsigned(),
        _ => return Err(ErrorCode::InvalidCellValue),
    }
    Ok(())
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
/// computed before the first splice. `out` grows to the widest and longest
/// batch it has bound and keeps that storage; memory that cannot back it
/// refuses with `OutOfMemory`.
///
/// `strings` is the caller's interner: every bound entry's evaluation may
/// read an id's text and intern the strings it builds, and the bind is the
/// table's only writer (specs/columine/09-row-interning.md). A refusal of the
/// table refuses the bind with its code, before any splice.
pub fn bind_row_columns<'a, 'e, E: RowExpression>(
    program: &'a [u8],
    state: &[u8],
    cols: &mut [&'a [u8]],
    batch_len: u32,
    eval: &'e mut E,
    strings: &mut dyn Interner,
    out: &'a mut RowColumns,
) -> Result<LiveColumns<'a, 'e>, ErrorCode> {
    // `out` describes this bind only: a table-less program or an empty batch
    // must not leave the previous batch's targets or live entries behind.
    out.targets.clear();
    out.lanes.clear();
    out.clear_live();
    out.bound = false;
    out.table.clear();
    let Some((table, table_start)) = row_table(program)? else {
        out.bound = true;
        return Ok(LiveColumns {
            rows: out,
            program,
            eval,
        });
    };
    // An empty batch carries no rows to derive and may carry no column
    // pointers at all; the reduce section reads nothing from it either.
    if batch_len == 0 {
        out.bound = true;
        return Ok(LiveColumns {
            rows: out,
            program,
            eval,
        });
    }
    let count = usize::from(bytes::read_u16(table, 4));
    let laned = bytes::read_u32(table, 0) == LANED_ROW_EXPRESSIONS_MAGIC;
    let mut header = Operands::at(table, TABLE_FIXED_BYTES);
    let type_col = header.index().ok_or(ErrorCode::InvalidProgram)?;
    if type_col as usize >= cols.len() {
        return Err(ErrorCode::InvalidProgram);
    }
    room(&mut out.table, table.len())?;
    out.table.extend_from_slice(table);
    out.table_start = table_start;
    let have = out.cells.len();
    room(&mut out.cells, count.saturating_sub(have))?;
    out.cells.resize_with(count, Vec::new);
    let have = out.targets.len();
    room(&mut out.targets, count.saturating_sub(have))?;
    out.targets.resize(count, 0);
    let have = out.lanes.len();
    room(&mut out.lanes, count.saturating_sub(have))?;
    out.lanes.resize(count, CmpType::U32);
    out.type_col = type_col;
    out.batch_len = batch_len;

    let view: &[&[u8]] = cols;
    let type_cells = col_u32_exact(col_at(view, type_col as usize), batch_len)
        .ok_or(ErrorCode::ColumnUnderrun)?;
    let mut pos = header.pos();
    for i in 0..count {
        let entry = parse_entry(table, &mut pos, laned)?;
        if entry.target_col as usize >= view.len() {
            return Err(ErrorCode::InvalidProgram);
        }
        out.targets[i] = entry.target_col;
        out.lanes[i] = entry.lane;
        // The entries before `i` are final; this entry reads them and writes
        // only its own cells.
        let (done, rest) = out.cells.split_at_mut(i);
        let batch = BatchView {
            state,
            batch_len,
            cols: view,
            derived_targets: &out.targets[..i],
            derived_lanes: &out.lanes[..i],
            derived: done,
            stored: StoredBinding::Unbound,
        };
        let binding = eval.admit(entry.expr, entry.lane, &batch)?;
        let cells = &mut rest[0];
        cells.clear();
        let words = (batch_len as usize * entry.lane.stride()).div_ceil(8);
        room(cells, words)?;
        cells.resize(words, 0);
        if binding == Binding::Live {
            // The zero cells stand in for the column the reduce section
            // reads live; they also keep the column covering the batch.
            let index = u32::try_from(out.live.len()).map_err(|_| ErrorCode::InvalidProgram)?;
            if out.live_entry(entry.target_col).is_some() {
                // Two live entries on one column: the read could only answer
                // one of them.
                return Err(ErrorCode::InvalidProgram);
            }
            if out.live_index.len() < view.len() {
                let have = out.live_index.len();
                room(&mut out.live_index, view.len() - have)?;
                out.live_index.resize(view.len(), NOT_LIVE);
            }
            room(&mut out.live, 1)?;
            out.live_index[entry.target_col as usize] = index;
            out.live.push(LiveEntry {
                match_ids: entry.match_ids_at.shifted(table_start),
                expr: entry.expr_at.shifted(table_start),
                lane: entry.lane,
            });
            continue;
        }
        for (row, &type_id) in type_cells.iter().enumerate() {
            if !entry.matches(type_id) {
                continue;
            }
            // `row < batch_len`, which is a u32.
            #[allow(clippy::cast_possible_truncation)]
            let at = row as u32;
            store(
                cells,
                entry.lane,
                row,
                eval.eval(entry.expr, entry.lane, &batch, at, strings)?,
            )?;
        }
    }
    if pos != table.len() {
        return Err(ErrorCode::InvalidProgram);
    }
    refuse_unreadable_columns(program, state, out)?;

    out.bound = true;
    let bound: &'a RowColumns = out;
    bound.splice(cols)?;
    Ok(LiveColumns {
        rows: bound,
        program,
        eval,
    })
}
