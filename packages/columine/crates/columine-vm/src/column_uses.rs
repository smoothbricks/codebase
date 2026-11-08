//! The column operands of a reduce section, with how each is read.
//!
//! [`ReduceReads::of`] walks a program's reduce section — top-level ops,
//! `FOR_EACH` bodies and the `FLAT_MAP` bodies nested in them — and reports
//! every operand that names a batch column together with its
//! [`ColumnUse`]. The one distinction that matters to a caller is whether a
//! read resolves through the live-column handle: the per-element
//! conditional opcodes (`*_IF` in a `FOR_EACH` body) and the top-level
//! stored-row sweep ask [`LiveColumns`] for their predicate cell; every other
//! read — aggregates, their `*_IF` predicates, keys, values, timestamps,
//! elements, offsets, the `FOR_EACH`
//! type column — takes the bytes the batch carries. A live column carries
//! zero placeholder cells, so such a read would silently see zeros;
//! `row_exprs` refuses the bind instead.
//!
//! A program's reads are a function of its bytes: [`ReduceReads`] is that
//! walk's answer, derived once per program and checked against every bind.
//! [`reduce_column_roles`] is the same walk for load-time admission. It
//! reports each operand as a `ColumnOperand`: an ordinary cell, or a
//! value-object CSR pair. [`ReduceReads`] holds a CSR pair as its two reads.
//!
//! Each cell operand also names its [`OperandLane`]: the cell the dispatch
//! reads its column at, a u32 or an 8-byte f64 or i64. An instruction fixes
//! the lane or names it (`cmp_type`); a struct-map or struct-list field, a
//! scalar list's element and a scalar aggregate's value take the lane their
//! slot's definition declares. The bind reads a wide derived column only at
//! its own lane (`row_exprs`).
//!
//! WHY a walker and not a role table beside the dispatch: the operand order
//! of every opcode is already spelled once in the dispatch arms and once in
//! `body_op_len`; this walk is the third spelling and the tests hold the
//! three together. An opcode the walk does not know is `InvalidProgram`, the
//! same answer the dispatch gives an opcode it does not know — so a new
//! reduce opcode without an arm here fails loudly on its first live bind,
//! never silently. Both entry points share this one walk, so a role and a
//! read can never disagree about which operand an opcode names.
//!
//! [`LiveColumns`]: crate::row_exprs::LiveColumns

use columine_types::operand::Operands;
use columine_types::types::{
    AggType, CmpType, ErrorCode, NO_PARENT_TS_COL, Opcode, ProgramHeader, SCATTER_SKIP, SlotType,
    SlotTypeFlags, StructFieldType,
};

use crate::vm::{
    ScalarOperands, StructMapPick, body_op_len, decode_struct_map_guarded_upsert_operands,
    decode_struct_map_upsert_operands, decode_struct_map2_tuple_operands,
    decode_struct_map2_upsert_operands,
};

/// How the reduce section reads a column operand.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnUse {
    /// The cell bytes the batch carries, at the row or over the batch.
    Read,
    /// The predicate of a per-element conditional opcode: resolved through
    /// the live-column handle when the column is live.
    ElementPredicate,
    /// A top-level predicate evaluated against a borrowed stored map row.
    StoredRowPredicate,
}

/// One column operand of a reduce section, by the role it plays.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ColumnOperand {
    /// An ordinary cell column, read as `usage` says at `lane`.
    Cell {
        column: u32,
        usage: ColumnUse,
        lane: OperandLane,
    },
    /// The offsets and values columns of a struct-map CSR field: element `e`
    /// is `values[offsets[e]..offsets[e + 1]]`. `slot` and `field_idx` name
    /// the field the instruction fills, which admission links to a schema.
    Csr {
        slot: u32,
        field_idx: u8,
        offsets_col: u32,
        values_col: u32,
    },
}

/// The lane an operand reads its column's cells at: one the instruction
/// fixes or names, or the one a slot's definition declares for what the
/// operand fills.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OperandLane {
    Cell(CmpType),
    /// Field `field` of a struct map, two-key struct map or struct list.
    Field {
        slot: u32,
        field: u8,
    },
    /// The element of a scalar list.
    Element {
        slot: u32,
    },
    /// The value of a scalar aggregate.
    Scalar {
        slot: u32,
    },
}

const U32: OperandLane = OperandLane::Cell(CmpType::U32);
const F64: OperandLane = OperandLane::Cell(CmpType::F64);
const I64: OperandLane = OperandLane::Cell(CmpType::I64);

impl OperandLane {
    /// The cell lane, reading a slot's definition from `init`. `None` when
    /// the definition declares no scalar cell there: an array or
    /// value-object field, a slot of another kind, or no such slot.
    pub(crate) fn resolve(self, init: &[u8]) -> Option<CmpType> {
        match self {
            Self::Cell(lane) => Some(lane),
            Self::Field { slot, field } => field_lane(
                *slot_record(init, slot)
                    .ok()?
                    .fields
                    .get(usize::from(field))?,
            ),
            Self::Element { slot } => field_lane(slot_record(init, slot).ok()?.element?),
            Self::Scalar { slot } => {
                let record = slot_record(init, slot).ok()?;
                if record.op != Opcode::SlotDef
                    || record.flags.slot_type() != Some(SlotType::Aggregate)
                {
                    return None;
                }
                match AggType::from_u8(record.cap_lo)? {
                    AggType::ScalarU32 => Some(CmpType::U32),
                    AggType::ScalarF64 => Some(CmpType::F64),
                    AggType::ScalarI64 => Some(CmpType::I64),
                    AggType::Sum
                    | AggType::Count
                    | AggType::Min
                    | AggType::Max
                    | AggType::Avg
                    | AggType::SumI64
                    | AggType::MinI64
                    | AggType::MaxI64 => None,
                }
            }
        }
    }
}

/// The lane a scalar field kind is filled from: eight bytes for INT64 and
/// FLOAT64, a u32 cell for every other scalar (a BOOL from its nonzero
/// test). An array or value-object field takes no cell.
fn field_lane(kind: u8) -> Option<CmpType> {
    match StructFieldType::from_u8(kind)? {
        StructFieldType::Int64 => Some(CmpType::I64),
        StructFieldType::Float64 => Some(CmpType::F64),
        StructFieldType::UInt32 | StructFieldType::String | StructFieldType::Bool => {
            Some(CmpType::U32)
        }
        StructFieldType::ArrayU32
        | StructFieldType::ArrayI64
        | StructFieldType::ArrayF64
        | StructFieldType::ArrayString
        | StructFieldType::ArrayBool
        | StructFieldType::Vo => None,
    }
}

/// One cell read of a reduce section: its column, how it is read, and the
/// lane its cells are read at, `None` where the slot's definition declares
/// no scalar cell there (an array or value-object field).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReduceRead {
    pub column: u32,
    pub usage: ColumnUse,
    pub lane: Option<CmpType>,
}

/// Every cell read of a program's reduce section, in walk order, with what a
/// bind judges against them. The walk reads only the program's bytes: a TTL
/// slot's timestamp column is the one its init definition names, which is
/// the column every state laid out for the program records. So the reads are
/// derived once per program, together with the two answers a bind needs from
/// them ([`crate::row_exprs::bind_row_columns`]): the columns some operand
/// reads plainly, which a live entry's zero cells cannot answer, and whether
/// the program's own row-expression table holds a wide column its reduce
/// section reads at another lane — the table's targets and lanes are program
/// bytes, so that verdict is the program's. Which entries bind live is the
/// embedder's answer per batch, so only that check is left to each bind. A
/// CSR pair is its offsets read, then its values read, at u32: they hold
/// offsets and element bytes, never a cell of a lane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReduceReads {
    reads: Vec<ReduceRead>,
    /// The columns some operand reads plainly ([`ColumnUse::Read`]), sorted
    /// and unique.
    plain: Vec<u32>,
    /// Whether the program's row-expression table holds a wide column its
    /// reduce section cannot read as its cells
    /// ([`crate::row_exprs::refuses_wide_reads`]).
    off_lane: bool,
}

impl ReduceReads {
    /// The reads of `program`'s reduce section.
    ///
    /// # Errors
    /// `InvalidProgram` for a reduce section the VM would refuse, or a slot
    /// its init section does not define; `OutOfMemory` when memory cannot
    /// back the reads.
    pub fn of(program: &[u8]) -> Result<Self, ErrorCode> {
        let sections = ProgramHeader::sections(program).ok_or(ErrorCode::InvalidProgram)?;
        let init = sections.init_code;
        let mut reads = Vec::new();
        let mut out_of_memory = false;
        let mut keep = |read: ReduceRead| {
            if reads.try_reserve(1).is_ok() {
                reads.push(read);
            } else {
                out_of_memory = true;
            }
        };
        reduce_column_roles(init, sections.reduce_code, &mut |operand| match operand {
            ColumnOperand::Cell {
                column,
                usage,
                lane,
            } => keep(ReduceRead {
                column,
                usage,
                lane: lane.resolve(init),
            }),
            ColumnOperand::Csr {
                offsets_col,
                values_col,
                ..
            } => {
                for column in [offsets_col, values_col] {
                    keep(ReduceRead {
                        column,
                        usage: ColumnUse::Read,
                        lane: Some(CmpType::U32),
                    });
                }
            }
        })?;
        if out_of_memory {
            return Err(ErrorCode::OutOfMemory);
        }
        let mut plain = Vec::new();
        let plain_reads = reads.iter().filter(|read| read.usage == ColumnUse::Read);
        plain
            .try_reserve_exact(plain_reads.clone().count())
            .map_err(|_| ErrorCode::OutOfMemory)?;
        plain.extend(plain_reads.map(|read| read.column));
        plain.sort_unstable();
        plain.dedup();
        let off_lane = crate::row_exprs::refuses_wide_reads(program, &reads)?;
        Ok(Self {
            reads,
            plain,
            off_lane,
        })
    }

    /// Each read, in walk order.
    #[must_use]
    pub fn reads(&self) -> &[ReduceRead] {
        &self.reads
    }

    /// Whether some operand reads `column` plainly: as the bytes the batch
    /// carries, which a live entry's zero cells cannot answer.
    #[must_use]
    pub fn reads_plainly(&self, column: u32) -> bool {
        self.plain.binary_search(&column).is_ok()
    }

    /// Whether the program's row-expression table holds a wide column its
    /// reduce section cannot read as its cells.
    #[must_use]
    pub fn refuses_wide(&self) -> bool {
        self.off_lane
    }
}

/// Visit every column operand of the reduce section `code` by role. `init` is
/// the init section the program is compiled from: a `BatchMapUpsertLast` on a
/// TTL slot reads the timestamp column its definition names. A reduce section
/// the walk refuses, or a slot `init` does not define, is `InvalidProgram`.
pub(crate) fn reduce_column_roles(
    init: &[u8],
    code: &[u8],
    visit: &mut dyn FnMut(ColumnOperand),
) -> Result<(), ErrorCode> {
    walk(code, init, false, NO_PARENT_TS_COL, visit)
}

/// The timestamp column of `slot` when its definition in `init` gives it a
/// TTL, at the lane its deadline is kept in.
fn ttl_timestamp(init: &[u8], slot: u32) -> Result<Option<(u32, OperandLane)>, ErrorCode> {
    let record = slot_record(init, slot)?;
    Ok(record.timestamp_col.map(|col| {
        (
            col,
            if record.flags.integer_deadline() {
                I64
            } else {
                F64
            },
        )
    }))
}

/// What one instruction sequence's operands are read under: the init section
/// that names a TTL slot's timestamp column, how a `*_IF` predicate is read, and the
/// parent timestamp column of an enclosing `FLAT_MAP` (`NO_PARENT_TS_COL`
/// outside one), which its body's timestamp operands read instead of their
/// own.
#[derive(Clone, Copy)]
struct Context<'a> {
    init: &'a [u8],
    predicate: ColumnUse,
    parent_ts: u32,
}

/// Walk one instruction sequence: the top level (`element == false`, where
/// a `*_IF` opcode is not dispatched and its predicate is a plain read) or
/// a `FOR_EACH`/`FLAT_MAP` body.
fn walk(
    code: &[u8],
    init: &[u8],
    element: bool,
    parent_ts: u32,
    visit: &mut dyn FnMut(ColumnOperand),
) -> Result<(), ErrorCode> {
    let context = Context {
        init,
        predicate: if element {
            ColumnUse::ElementPredicate
        } else {
            ColumnUse::Read
        },
        parent_ts,
    };
    let mut pc = 0usize;
    while pc < code.len() {
        let op = Opcode::from_u8(code[pc]).ok_or(ErrorCode::InvalidProgram)?;
        let len = body_op_len(code, pc).ok_or(ErrorCode::InvalidProgram)?;
        // `body_op_len` proved every operand of `code[pc..pc + len]` decodes.
        let mut ops = Operands::at(code, pc + 1);
        walk_op(op, &mut ops, code, pc, context, visit).ok_or(ErrorCode::InvalidProgram)??;
        if op == Opcode::Halt {
            break;
        }
        pc += len;
    }
    Ok(())
}

/// Visit the column operands of the instruction at `pc` (opcode `op`, its
/// operands read through `ops`) under `context`. `None` is a malformed
/// operand.
fn walk_op(
    op: Opcode,
    ops: &mut Operands<'_>,
    code: &[u8],
    pc: usize,
    context: Context<'_>,
    visit: &mut dyn FnMut(ColumnOperand),
) -> Option<Result<(), ErrorCode>> {
    let Context {
        init,
        predicate,
        parent_ts,
    } = context;
    // Read `n` index operands as plain u32 reads.
    let reads = |ops: &mut Operands<'_>, n: usize, visit: &mut dyn FnMut(ColumnOperand)| {
        for _ in 0..n {
            cell(visit, ops.index()?, ColumnUse::Read, U32);
        }
        Some(())
    };
    // Read one index operand as a plain read at `lane`.
    let read = |ops: &mut Operands<'_>, lane: OperandLane, visit: &mut dyn FnMut(ColumnOperand)| {
        cell(visit, ops.index()?, ColumnUse::Read, lane);
        Some(())
    };
    // A timestamp operand read at `lane`. In a `FLAT_MAP` body the dispatch
    // reads the parent timestamp column there instead.
    let timestamp = |ts: u32, lane: OperandLane, visit: &mut dyn FnMut(ColumnOperand)| {
        cell(visit, ts, ColumnUse::Read, lane);
        if parent_ts != NO_PARENT_TS_COL {
            cell(visit, parent_ts, ColumnUse::Read, lane);
        }
    };
    // A TTL slot's timestamp column, named by the slot, not the instruction.
    let ttl = |slot: u32, visit: &mut dyn FnMut(ColumnOperand)| -> Result<(), ErrorCode> {
        if let Some((column, lane)) = ttl_timestamp(init, slot)? {
            cell(visit, column, ColumnUse::Read, lane);
        }
        Ok(())
    };
    match op {
        Opcode::Halt => {}
        Opcode::StructMapSweep => {
            if predicate != ColumnUse::Read {
                return None;
            }
            ops.index()?;
            cell(visit, ops.index()?, ColumnUse::StoredRowPredicate, U32);
        }
        // slot, key, val, ts, cmp_type: the timestamp reads at its cmp_type.
        Opcode::BatchMapUpsertLatest | Opcode::BatchMapUpsertLatestTtl => {
            ops.index()?;
            reads(ops, 2, visit)?;
            let ts = ops.index()?;
            timestamp(ts, cmp_lane(ops)?, visit);
        }
        Opcode::BatchMapUpsertFirst => {
            ops.index()?;
            reads(ops, 2, visit)?;
        }
        Opcode::BatchMapUpsertLast => {
            let slot = ops.index()?;
            reads(ops, 2, visit)?;
            if let Err(error) = ttl(slot, visit) {
                return Some(Err(error));
            }
        }
        Opcode::BatchMapRemove => {
            ops.index()?;
            reads(ops, 1, visit)?;
        }
        Opcode::BatchMapUpsertLastTtl => {
            ops.index()?;
            reads(ops, 2, visit)?;
            let ts = ops.index()?;
            timestamp(ts, F64, visit);
        }
        // slot, key, val, cmp, cmp_type.
        Opcode::BatchMapUpsertMax | Opcode::BatchMapUpsertMin => {
            ops.index()?;
            reads(ops, 2, visit)?;
            let cmp = ops.index()?;
            cell(visit, cmp, ColumnUse::Read, cmp_lane(ops)?);
        }
        // slot, key, val, ts, cmp_type, pred.
        Opcode::BatchMapUpsertLatestIf => {
            ops.index()?;
            reads(ops, 2, visit)?;
            let ts = ops.index()?;
            timestamp(ts, cmp_lane(ops)?, visit);
            cell(visit, ops.index()?, predicate, U32);
        }
        // slot, key, val, cmp, cmp_type, pred.
        Opcode::BatchMapUpsertMaxIf | Opcode::BatchMapUpsertMinIf => {
            ops.index()?;
            reads(ops, 2, visit)?;
            let cmp = ops.index()?;
            cell(visit, cmp, ColumnUse::Read, cmp_lane(ops)?);
            cell(visit, ops.index()?, predicate, U32);
        }
        Opcode::BatchMapUpsertFirstIf => {
            ops.index()?;
            reads(ops, 2, visit)?;
            cell(visit, ops.index()?, predicate, U32);
        }
        Opcode::BatchMapUpsertLastIf => {
            let slot = ops.index()?;
            reads(ops, 2, visit)?;
            cell(visit, ops.index()?, predicate, U32);
            if let Err(error) = ttl(slot, visit) {
                return Some(Err(error));
            }
        }
        Opcode::BatchMapRemoveIf => {
            ops.index()?;
            reads(ops, 1, visit)?;
            cell(visit, ops.index()?, predicate, U32);
        }
        Opcode::BatchStructMapProbe => {
            // probe_slot, key, miss_mode, out_slot, num_fields,
            // (probe_field, out_field) × num_fields, out_key.
            ops.index()?;
            reads(ops, 1, visit)?;
            ops.byte()?;
            ops.index()?;
            let num_fields = usize::from(ops.byte()?);
            ops.bytes(num_fields * 2)?;
            reads(ops, 1, visit)?;
        }
        Opcode::BatchStructMapProbeScatter => {
            // probe_slot, key; the rest are fields of the probed row.
            ops.index()?;
            reads(ops, 1, visit)?;
        }
        Opcode::BatchStructMapScatter => {
            // route_col, op_col, key_col, num_routes, then
            // [kind, dest_slot, dest_field, v_col] × num_routes — the route
            // table's v columns are reads too.
            reads(ops, 3, visit)?;
            route_value_reads(ops, visit)?;
        }
        Opcode::BatchStructMapScatterGuarded => {
            // route_col, op_col, key_col, guard_slot, num_guards, then
            // [guard_col, guard_field] × num_guards, num_routes, then
            // [kind, dest_slot, dest_field, v_col] × num_routes.
            //
            // A guard column is READ and never written, and it is read by the
            // dispatch straight out of the batch's bytes. Leave it out and a
            // derived guard column deferred to the reduce section binds as
            // live, where the batch carries zeros for it — the guard would
            // then order every element against zero and silently let stale
            // transactions through. Naming it here is what turns that into a
            // refusal. Each guard is ordered as its field of the guard slot
            // stores it, so it reads at that field's lane.
            reads(ops, 3, visit)?;
            let slot = ops.index()?;
            let num_guards = ops.byte()?;
            for _ in 0..num_guards {
                let column = ops.index()?;
                let field = ops.byte()?;
                cell(
                    visit,
                    column,
                    ColumnUse::Read,
                    OperandLane::Field { slot, field },
                );
            }
            route_value_reads(ops, visit)?;
        }
        Opcode::BatchSetInsert | Opcode::BatchBitmapAdd => {
            let slot = ops.index()?;
            reads(ops, 1, visit)?;
            if let Err(error) = ttl(slot, visit) {
                return Some(Err(error));
            }
        }
        Opcode::BatchSetRemove
        | Opcode::BatchBitmapRemove
        // The aggregate pass reads its predicate from the batch's bytes,
        // never through the live handle.
        | Opcode::BatchAggCountIf => {
            ops.index()?;
            reads(ops, 1, visit)?;
        }
        Opcode::BatchAggSum | Opcode::BatchAggMin | Opcode::BatchAggMax => {
            ops.index()?;
            read(ops, F64, visit)?;
        }
        Opcode::BatchAggSumI64 | Opcode::BatchAggMinI64 | Opcode::BatchAggMaxI64 => {
            ops.index()?;
            read(ops, I64, visit)?;
        }
        Opcode::ListAppend => {
            let slot = ops.index()?;
            read(ops, OperandLane::Element { slot }, visit)?;
        }
        Opcode::BatchBitmapAnd
        | Opcode::BatchBitmapOr
        | Opcode::BatchBitmapAndNot
        | Opcode::BatchBitmapXor
        | Opcode::BatchBitmapAndScratch
        | Opcode::BatchBitmapOrScratch
        | Opcode::BatchBitmapAndNotScratch
        | Opcode::BatchBitmapXorScratch
        | Opcode::BatchAggCount => {}
        Opcode::BatchSetInsertTtl => {
            ops.index()?;
            reads(ops, 1, visit)?;
            let ts = ops.index()?;
            timestamp(ts, F64, visit);
        }
        // slot, val, pred: the aggregate pass reads its predicate from the
        // batch's bytes.
        Opcode::BatchAggSumIf | Opcode::BatchAggMinIf | Opcode::BatchAggMaxIf => {
            ops.index()?;
            read(ops, F64, visit)?;
            reads(ops, 1, visit)?;
        }
        // slot, val, cmp: the value reads at the slot's scalar kind.
        Opcode::BatchScalarLatest => {
            let slot = ops.index()?;
            read(ops, OperandLane::Scalar { slot }, visit)?;
            read(ops, F64, visit)?;
        }
        Opcode::BatchStructMap2Remove | Opcode::NestedSetInsert => {
            ops.index()?;
            reads(ops, 2, visit)?;
        }
        Opcode::NestedAggUpdate => {
            ops.index()?;
            reads(ops, 1, visit)?;
            read(ops, F64, visit)?;
        }
        Opcode::BatchSetInsertIf => {
            let slot = ops.index()?;
            reads(ops, 1, visit)?;
            cell(visit, ops.index()?, predicate, U32);
            if let Err(error) = ttl(slot, visit) {
                return Some(Err(error));
            }
        }
        Opcode::NestedMapUpsertLast => {
            ops.index()?;
            reads(ops, 3, visit)?;
        }
        Opcode::BatchStructMapUpsertLast
        | Opcode::BatchStructMapUpsertFirst
        | Opcode::BatchStructMapUpsertMax
        | Opcode::BatchStructMapUpsertFirstSinceReset
        | Opcode::BatchStructMapUpsertLastIf => {
            let operands = decode_struct_map_upsert_operands(code, pc + 1, op)?;
            cell(visit, operands.key_col, ColumnUse::Read, U32);
            field_reads(visit, operands.slot, &operands.vals);
            // Each array field is a CSR pair, named by the field it fills.
            for array in operands.array_fields() {
                visit(ColumnOperand::Csr {
                    slot: operands.slot,
                    field_idx: array.field_idx,
                    offsets_col: array.offsets_col,
                    values_col: array.values_col,
                });
            }
            // 0x8f's predicate, named last: a live column resolves at the row.
            if let StructMapPick::LastIf { pred_col } = operands.pick {
                cell(visit, pred_col, predicate, U32);
            }
        }
        Opcode::BatchStructMapCountsFold => {
            let fold = crate::counts_fold::CountsFoldOp::decode(code, pc + 1).ok()?;
            // The key, then the init frame's CSR pair, then the buckets and
            // masks: the order the fold's columns are named in.
            cell(visit, fold.key_col, ColumnUse::Read, U32);
            visit(ColumnOperand::Csr {
                slot: fold.slot,
                field_idx: fold.field_idx,
                offsets_col: fold.init_offsets_col,
                values_col: fold.init_values_col,
            });
            for hist in fold.histograms() {
                cell(visit, hist.bucket_col, ColumnUse::Read, U32);
            }
            for mask in fold.masks() {
                cell(visit, mask.mask_col, ColumnUse::Read, U32);
            }
        }
        Opcode::BatchStructMap2UpsertLast => {
            struct_map2_reads(&decode_struct_map2_upsert_operands(code, pc + 1)?, visit);
        }
        Opcode::BatchStructMapRemove | Opcode::BatchStructMap2RemovePrefix => {
            ops.index()?;
            reads(ops, 1, visit)?;
        }
        Opcode::BatchStructMapGuardedUpsertLast => {
            let operands = decode_struct_map_guarded_upsert_operands(code, pc + 1)?;
            cell(visit, operands.row.key_col, ColumnUse::Read, U32);
            cell(visit, operands.guard_key_col, ColumnUse::Read, U32);
            field_reads(visit, operands.row.slot, &operands.row.vals);
            for array in operands.row.array_fields() {
                visit(ColumnOperand::Csr {
                    slot: operands.row.slot,
                    field_idx: array.field_idx,
                    offsets_col: array.offsets_col,
                    values_col: array.values_col,
                });
            }
        }
        Opcode::BatchStructMap2UpsertMaxI64x2 | Opcode::BatchStructMap2UpsertMinI64x2 => {
            let operands = decode_struct_map2_tuple_operands(code, pc + 1, op)?;
            struct_map2_reads(&operands.row, visit);
            cell(visit, operands.cmp1_col, ColumnUse::Read, I64);
            cell(visit, operands.cmp2_col, ColumnUse::Read, I64);
        }
        Opcode::ListAppendStruct => {
            // slot, num_vals, (col, field) × num_vals: each value fills its
            // field of the list's rows.
            let slot = ops.index()?;
            let num_vals = ops.byte()?;
            for _ in 0..num_vals {
                let column = ops.index()?;
                let field = ops.byte()?;
                cell(
                    visit,
                    column,
                    ColumnUse::Read,
                    OperandLane::Field { slot, field },
                );
            }
        }
        Opcode::FlatMap => {
            // offsets, parent_ts, body_len u16, body. The offsets name the
            // expansion; the parent timestamp is read by the body's timestamp
            // operands, at their lanes.
            reads(ops, 1, visit)?;
            let parent_ts = ops.index()?;
            let body_len = usize::from(ops.u16()?);
            return Some(walk(ops.bytes(body_len)?, init, true, parent_ts, visit));
        }
        Opcode::ForEach => {
            // col, match_count, ids u32 × match_count, body_len u16, body.
            reads(ops, 1, visit)?;
            let match_count = usize::try_from(ops.index()?).ok()?;
            ops.bytes(match_count * 4)?;
            let body_len = usize::from(ops.u16()?);
            return Some(walk(ops.bytes(body_len)?, init, true, NO_PARENT_TS_COL, visit));
        }
        _ => return Some(Err(ErrorCode::InvalidProgram)),
    }
    Some(Ok(()))
}

/// Visit an ordinary cell operand, read as `usage` says at `lane`.
fn cell(visit: &mut dyn FnMut(ColumnOperand), column: u32, usage: ColumnUse, lane: OperandLane) {
    visit(ColumnOperand::Cell {
        column,
        usage,
        lane,
    });
}

/// The lane a comparing upsert's `cmp_type:u8` operand names.
fn cmp_lane(ops: &mut Operands<'_>) -> Option<OperandLane> {
    ops.byte().and_then(CmpType::from_u8).map(OperandLane::Cell)
}

/// Each `(val_col, field_idx)` pair of a struct-map row: the value fills its
/// field of `slot`.
fn field_reads(visit: &mut dyn FnMut(ColumnOperand), slot: u32, vals: &ScalarOperands) {
    for (&column, &field) in vals.cols().iter().zip(vals.fields()) {
        cell(
            visit,
            column,
            ColumnUse::Read,
            OperandLane::Field { slot, field },
        );
    }
}

/// `num_routes:u8, (kind, dest_slot, dest_field, v_col) × num_routes`:
/// every consumed route contributes its value-column read. A kind-0 value
/// fills its destination field; a kind-1 value is a u32 set element.
fn route_value_reads(ops: &mut Operands<'_>, visit: &mut dyn FnMut(ColumnOperand)) -> Option<()> {
    let num_routes = ops.byte()?;
    for _ in 0..num_routes {
        let kind = ops.byte()?;
        let dest = ops.index()?;
        let field = ops.byte()?;
        let col = ops.index()?;
        if dest != SCATTER_SKIP {
            let lane = if kind == 0 {
                OperandLane::Field { slot: dest, field }
            } else {
                U32
            };
            cell(visit, col, ColumnUse::Read, lane);
        }
    }
    Some(())
}

fn struct_map2_reads(
    operands: &crate::vm::StructMap2UpsertOperands,
    visit: &mut dyn FnMut(ColumnOperand),
) {
    cell(visit, operands.key1_col, ColumnUse::Read, U32);
    cell(visit, operands.key2_col, ColumnUse::Read, U32);
    field_reads(visit, operands.slot, &operands.vals);
}

/// One slot definition of an init section, as far as the walk reads it.
struct SlotRecord<'a> {
    op: Opcode,
    flags: SlotTypeFlags,
    /// `cap_lo`, which an aggregate `SLOT_DEF` spends on its `AggType`.
    cap_lo: u8,
    /// A TTL `SLOT_DEF`'s timestamp column.
    timestamp_col: Option<u32>,
    /// The field kinds of a struct map, two-key struct map or struct list.
    fields: &'a [u8],
    /// The element kind of a scalar list.
    element: Option<u8>,
}

/// The definition of `slot` in `init`. Program admission validates the init
/// grammar before compile, so this scan mirrors only what it needs to reach
/// `slot`: each record's head (`op, slot, flags, cap_lo, cap_hi`), the TTL
/// params (`ttl:f32, grace:f32, ts_col:index, start_of:u8`) only when a
/// `SLOT_DEF`'s flags carry TTL, a struct record's field kinds, a list's
/// element kind, and the length of every other record kind. A slot `init`
/// does not define is `InvalidProgram`.
fn slot_record(init: &[u8], slot: u32) -> Result<SlotRecord<'_>, ErrorCode> {
    let mut r = Operands::at(init, 0);
    while r.pos() < init.len() {
        let op = r
            .byte()
            .and_then(Opcode::from_u8)
            .ok_or(ErrorCode::InvalidProgram)?;
        if op == Opcode::Halt {
            break;
        }
        let id = r.index().ok_or(ErrorCode::InvalidProgram)?;
        let flags = SlotTypeFlags::from_byte(r.byte().ok_or(ErrorCode::InvalidProgram)?);
        let cap_lo = r.byte().ok_or(ErrorCode::InvalidProgram)?;
        // `cap_hi`.
        r.byte().ok_or(ErrorCode::InvalidProgram)?;
        let mut record = SlotRecord {
            op,
            flags,
            cap_lo,
            timestamp_col: None,
            fields: &[],
            element: None,
        };
        match op {
            Opcode::SlotDef if flags.has_ttl() => {
                let (ttl, next) = crate::state_init::TtlParams::read(init, r.pos())
                    .ok_or(ErrorCode::InvalidProgram)?;
                r = Operands::at(init, next);
                record.timestamp_col = Some(ttl.timestamp_col);
            }
            Opcode::SlotDef | Opcode::SlotArray => {}
            Opcode::SlotStructMap | Opcode::SlotStructMap2 => {
                // count, then one field-kind byte per field.
                let count = r.byte().ok_or(ErrorCode::InvalidProgram)?;
                record.fields = r
                    .bytes(usize::from(count))
                    .ok_or(ErrorCode::InvalidProgram)?;
            }
            Opcode::SlotOrderedList => {
                // The element kind, or 0xff and the field kinds of a struct
                // list.
                let kind = r.byte().ok_or(ErrorCode::InvalidProgram)?;
                if kind == 0xff {
                    let count = r.byte().ok_or(ErrorCode::InvalidProgram)?;
                    record.fields = r
                        .bytes(usize::from(count))
                        .ok_or(ErrorCode::InvalidProgram)?;
                } else {
                    record.element = Some(kind);
                }
            }
            Opcode::SlotNested => {
                r.bytes(4).ok_or(ErrorCode::InvalidProgram)?;
            }
            _ => return Err(ErrorCode::InvalidProgram),
        }
        if id == slot {
            return Ok(record);
        }
    }
    Err(ErrorCode::InvalidProgram)
}
