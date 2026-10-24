//! The column operands of a reduce section, with how each is read.
//!
//! [`reduce_column_uses`] walks a program's reduce section — top-level ops,
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
//! [`reduce_column_roles`] is the same walk for load-time admission. It
//! reports each operand as a `ColumnOperand`: an ordinary cell, or a
//! value-object CSR pair. The public walk reports a CSR pair as its two reads.
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
    ErrorCode, NO_PARENT_TS_COL, Opcode, ProgramHeader, SCATTER_SKIP, SlotTypeFlags,
};

use crate::meta::SlotMetaView;
use crate::vm::{
    body_op_len, decode_struct_map_upsert_operands, decode_struct_map2_tuple_operands,
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
    /// An ordinary cell column, read as `usage` says.
    Cell { column: u32, usage: ColumnUse },
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

/// Visit every column operand of `program`'s reduce section. `state` is the
/// initialised state the program runs against: a `BatchMapUpsertLast` on a
/// TTL slot reads the slot's timestamp column, which only the state names.
/// A CSR pair is visited as its offsets read, then its values read.
/// A reduce section the VM would refuse is `InvalidProgram` here too.
pub fn reduce_column_uses(
    program: &[u8],
    state: &[u8],
    visit: &mut dyn FnMut(u32, ColumnUse),
) -> Result<(), ErrorCode> {
    let sections = ProgramHeader::sections(program).ok_or(ErrorCode::InvalidProgram)?;
    walk(
        sections.reduce_code,
        TimestampSource::State(state),
        false,
        &mut |operand| match operand {
            ColumnOperand::Cell { column, usage } => visit(column, usage),
            ColumnOperand::Csr {
                offsets_col,
                values_col,
                ..
            } => {
                visit(offsets_col, ColumnUse::Read);
                visit(values_col, ColumnUse::Read);
            }
        },
    )
}

/// Visit every column operand of the reduce section `code` by role. `init` is
/// the init section the program is compiled from: a `BatchMapUpsertLast` on a
/// TTL slot reads the timestamp column its definition names, and `init` is
/// the only place that column is named before a state exists. A reduce
/// section the walk refuses, or a slot `init` does not define, is
/// `InvalidProgram`.
pub(crate) fn reduce_column_roles(
    init: &[u8],
    code: &[u8],
    visit: &mut dyn FnMut(ColumnOperand),
) -> Result<(), ErrorCode> {
    walk(code, TimestampSource::Init(init), false, visit)
}

/// Where a `BatchMapUpsertLast` on a TTL slot finds the slot's timestamp
/// column: the state the program runs against, or the init section it is
/// compiled from.
#[derive(Clone, Copy)]
enum TimestampSource<'a> {
    State(&'a [u8]),
    Init(&'a [u8]),
}

impl TimestampSource<'_> {
    /// The timestamp column of `slot` when the slot has a TTL.
    fn timestamp_col(self, slot: u32) -> Result<Option<u32>, ErrorCode> {
        match self {
            Self::State(state) => {
                let meta = SlotMetaView::read(state, slot);
                Ok(meta.has_ttl().then(|| meta.timestamp_col(state)))
            }
            Self::Init(init) => init_timestamp_col(init, slot),
        }
    }
}

/// Walk one instruction sequence: the top level (`element == false`, where
/// a `*_IF` opcode is not dispatched and its predicate is a plain read) or
/// a `FOR_EACH`/`FLAT_MAP` body.
fn walk(
    code: &[u8],
    source: TimestampSource<'_>,
    element: bool,
    visit: &mut dyn FnMut(ColumnOperand),
) -> Result<(), ErrorCode> {
    let predicate = if element {
        ColumnUse::ElementPredicate
    } else {
        ColumnUse::Read
    };
    let mut pc = 0usize;
    while pc < code.len() {
        let op = Opcode::from_u8(code[pc]).ok_or(ErrorCode::InvalidProgram)?;
        let len = body_op_len(code, pc).ok_or(ErrorCode::InvalidProgram)?;
        // `body_op_len` proved every operand of `code[pc..pc + len]` decodes.
        let mut ops = Operands::at(code, pc + 1);
        walk_op(op, &mut ops, code, pc, source, predicate, visit)
            .ok_or(ErrorCode::InvalidProgram)??;
        if op == Opcode::Halt {
            break;
        }
        pc += len;
    }
    Ok(())
}

/// Visit the column operands of the instruction at `pc` (opcode `op`, its
/// operands read through `ops`). `None` is a malformed operand.
fn walk_op(
    op: Opcode,
    ops: &mut Operands<'_>,
    code: &[u8],
    pc: usize,
    source: TimestampSource<'_>,
    predicate: ColumnUse,
    visit: &mut dyn FnMut(ColumnOperand),
) -> Option<Result<(), ErrorCode>> {
    // Read `n` index operands as plain column reads.
    let reads = |ops: &mut Operands<'_>, n: usize, visit: &mut dyn FnMut(ColumnOperand)| {
        for _ in 0..n {
            cell(visit, ops.index()?, ColumnUse::Read);
        }
        Some(())
    };
    match op {
        Opcode::Halt => {}
        Opcode::StructMapSweep => {
            if predicate != ColumnUse::Read {
                return None;
            }
            ops.index()?;
            cell(visit, ops.index()?, ColumnUse::StoredRowPredicate);
        }
        Opcode::BatchMapUpsertLatest | Opcode::BatchMapUpsertLatestTtl => {
            ops.index()?;
            reads(ops, 3, visit)?;
        }
        Opcode::BatchMapUpsertFirst => {
            ops.index()?;
            reads(ops, 2, visit)?;
        }
        Opcode::BatchMapUpsertLast => {
            let slot = ops.index()?;
            reads(ops, 2, visit)?;
            // The TTL slot's timestamp column is named by the slot, not the
            // instruction.
            match source.timestamp_col(slot) {
                Ok(Some(column)) => cell(visit, column, ColumnUse::Read),
                Ok(None) => {}
                Err(error) => return Some(Err(error)),
            }
        }
        Opcode::BatchMapRemove => {
            ops.index()?;
            reads(ops, 1, visit)?;
        }
        Opcode::BatchMapUpsertLastTtl | Opcode::BatchMapUpsertMax | Opcode::BatchMapUpsertMin => {
            ops.index()?;
            reads(ops, 3, visit)?;
        }
        Opcode::BatchMapUpsertLatestIf | Opcode::BatchMapUpsertMaxIf | Opcode::BatchMapUpsertMinIf => {
            ops.index()?;
            reads(ops, 3, visit)?;
            ops.byte()?;
            cell(visit, ops.index()?, predicate);
        }
        Opcode::BatchMapUpsertFirstIf | Opcode::BatchMapUpsertLastIf => {
            ops.index()?;
            reads(ops, 2, visit)?;
            cell(visit, ops.index()?, predicate);
        }
        Opcode::BatchMapRemoveIf => {
            ops.index()?;
            reads(ops, 1, visit)?;
            cell(visit, ops.index()?, predicate);
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
            // refusal.
            reads(ops, 3, visit)?;
            ops.index()?;
            let num_guards = ops.byte()?;
            for _ in 0..num_guards {
                reads(ops, 1, visit)?;
                ops.byte()?;
            }
            route_value_reads(ops, visit)?;
        }
        Opcode::BatchSetInsert
        | Opcode::BatchSetRemove
        | Opcode::BatchBitmapAdd
        | Opcode::BatchBitmapRemove
        | Opcode::BatchAggSum
        | Opcode::BatchAggMin
        | Opcode::BatchAggMax
        | Opcode::BatchAggSumI64
        | Opcode::BatchAggMinI64
        | Opcode::BatchAggMaxI64
        | Opcode::ListAppend
        // The aggregate pass reads its predicate from the batch's bytes,
        // never through the live handle.
        | Opcode::BatchAggCountIf => {
            ops.index()?;
            reads(ops, 1, visit)?;
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
        Opcode::BatchSetInsertTtl
        | Opcode::BatchAggSumIf
        | Opcode::BatchAggMinIf
        | Opcode::BatchAggMaxIf
        | Opcode::BatchScalarLatest
        | Opcode::BatchStructMap2Remove
        | Opcode::NestedSetInsert
        | Opcode::NestedAggUpdate => {
            ops.index()?;
            reads(ops, 2, visit)?;
        }
        Opcode::BatchSetInsertIf => {
            ops.index()?;
            reads(ops, 1, visit)?;
            cell(visit, ops.index()?, predicate);
        }
        Opcode::NestedMapUpsertLast => {
            ops.index()?;
            reads(ops, 3, visit)?;
        }
        Opcode::BatchStructMapUpsertLast
        | Opcode::BatchStructMapUpsertFirst
        | Opcode::BatchStructMapUpsertMax
        | Opcode::BatchStructMapUpsertFirstSinceReset => {
            let operands = decode_struct_map_upsert_operands(code, pc + 1, op)?;
            cell(visit, operands.key_col, ColumnUse::Read);
            for &col in operands.vals.cols() {
                cell(visit, col, ColumnUse::Read);
            }
            // Each array field is a CSR pair, named by the field it fills.
            for array in operands.array_fields() {
                visit(ColumnOperand::Csr {
                    slot: operands.slot,
                    field_idx: array.field_idx,
                    offsets_col: array.offsets_col,
                    values_col: array.values_col,
                });
            }
        }
        Opcode::BatchStructMapCountsFold => {
            let fold = crate::counts_fold::CountsFoldOp::decode(code, pc + 1).ok()?;
            // The key, then the init frame's CSR pair, then the buckets and
            // masks: the order the fold's columns are named in.
            cell(visit, fold.key_col, ColumnUse::Read);
            visit(ColumnOperand::Csr {
                slot: fold.slot,
                field_idx: fold.field_idx,
                offsets_col: fold.init_offsets_col,
                values_col: fold.init_values_col,
            });
            for hist in fold.histograms() {
                cell(visit, hist.bucket_col, ColumnUse::Read);
            }
            for mask in fold.masks() {
                cell(visit, mask.mask_col, ColumnUse::Read);
            }
        }
        Opcode::BatchStructMap2UpsertLast => {
            struct_map2_reads(&decode_struct_map2_upsert_operands(code, pc + 1)?, visit);
        }
        Opcode::BatchStructMap2UpsertMaxI64x2 | Opcode::BatchStructMap2UpsertMinI64x2 => {
            let operands = decode_struct_map2_tuple_operands(code, pc + 1, op)?;
            struct_map2_reads(&operands.row, visit);
            cell(visit, operands.cmp1_col, ColumnUse::Read);
            cell(visit, operands.cmp2_col, ColumnUse::Read);
        }
        Opcode::ListAppendStruct => {
            // slot, num_vals, (col, field) × num_vals.
            ops.index()?;
            let num_vals = ops.byte()?;
            for _ in 0..num_vals {
                reads(ops, 1, visit)?;
                ops.byte()?;
            }
        }
        Opcode::FlatMap => {
            // offsets, parent_ts, body_len u16, body. Both columns are cells:
            // the offsets name the expansion, the parent timestamp is a read.
            reads(ops, 1, visit)?;
            let parent_ts = ops.index()?;
            if parent_ts != NO_PARENT_TS_COL {
                cell(visit, parent_ts, ColumnUse::Read);
            }
            let body_len = usize::from(ops.u16()?);
            return Some(walk(ops.bytes(body_len)?, source, true, visit));
        }
        Opcode::ForEach => {
            // col, match_count, ids u32 × match_count, body_len u16, body.
            reads(ops, 1, visit)?;
            let match_count = usize::try_from(ops.index()?).ok()?;
            ops.bytes(match_count * 4)?;
            let body_len = usize::from(ops.u16()?);
            return Some(walk(ops.bytes(body_len)?, source, true, visit));
        }
        _ => return Some(Err(ErrorCode::InvalidProgram)),
    }
    Some(Ok(()))
}

/// Visit an ordinary cell operand, read as `usage` says.
fn cell(visit: &mut dyn FnMut(ColumnOperand), column: u32, usage: ColumnUse) {
    visit(ColumnOperand::Cell { column, usage });
}

/// `num_routes:u8, (kind, dest_slot, dest_field, v_col) × num_routes`:
/// every consumed route contributes its value-column read.
fn route_value_reads(ops: &mut Operands<'_>, visit: &mut dyn FnMut(ColumnOperand)) -> Option<()> {
    let num_routes = ops.byte()?;
    for _ in 0..num_routes {
        ops.byte()?;
        let dest = ops.index()?;
        ops.byte()?;
        let col = ops.index()?;
        if dest != SCATTER_SKIP {
            cell(visit, col, ColumnUse::Read);
        }
    }
    Some(())
}

fn struct_map2_reads(
    operands: &crate::vm::StructMap2UpsertOperands,
    visit: &mut dyn FnMut(ColumnOperand),
) {
    cell(visit, operands.key1_col, ColumnUse::Read);
    cell(visit, operands.key2_col, ColumnUse::Read);
    for &col in operands.vals.cols() {
        cell(visit, col, ColumnUse::Read);
    }
}

/// The timestamp column `slot`'s definition names when the slot has a TTL,
/// `None` otherwise. Program admission validates the init grammar before
/// compile, so this scan mirrors only what it needs to reach `slot`: each
/// record's head (`op, slot, flags, cap_lo, cap_hi`), the TTL params
/// (`ttl:f32, grace:f32, ts_col:index, start_of:u8`) only when a `SLOT_DEF`'s
/// flags carry TTL, and the length of every other record kind.
fn init_timestamp_col(init: &[u8], slot: u32) -> Result<Option<u32>, ErrorCode> {
    let mut r = Operands::at(init, 0);
    while r.pos() < init.len() {
        let op = r
            .byte()
            .and_then(Opcode::from_u8)
            .ok_or(ErrorCode::InvalidProgram)?;
        if op == Opcode::Halt {
            break;
        }
        let record = r.index().ok_or(ErrorCode::InvalidProgram)?;
        let flags = SlotTypeFlags::from_byte(r.byte().ok_or(ErrorCode::InvalidProgram)?);
        // The two bytes after the flags: `cap_lo, cap_hi` for a `SLOT_DEF`.
        r.bytes(2).ok_or(ErrorCode::InvalidProgram)?;
        let timestamp = match op {
            Opcode::SlotDef if flags.has_ttl() => {
                let (ttl, next) = crate::state_init::TtlParams::read(init, r.pos())
                    .ok_or(ErrorCode::InvalidProgram)?;
                r = Operands::at(init, next);
                Some(ttl.timestamp_col)
            }
            Opcode::SlotDef | Opcode::SlotArray => None,
            Opcode::SlotStructMap | Opcode::SlotStructMap2 => {
                // count, then one field-kind byte per field.
                let count = r.byte().ok_or(ErrorCode::InvalidProgram)?;
                r.bytes(usize::from(count))
                    .ok_or(ErrorCode::InvalidProgram)?;
                None
            }
            Opcode::SlotOrderedList => {
                // kind, then for kind 0xff a count and that many bytes.
                if r.byte().ok_or(ErrorCode::InvalidProgram)? == 0xff {
                    let count = r.byte().ok_or(ErrorCode::InvalidProgram)?;
                    r.bytes(usize::from(count))
                        .ok_or(ErrorCode::InvalidProgram)?;
                }
                None
            }
            Opcode::SlotNested => {
                r.bytes(4).ok_or(ErrorCode::InvalidProgram)?;
                None
            }
            _ => return Err(ErrorCode::InvalidProgram),
        };
        if record == slot {
            return Ok(timestamp);
        }
    }
    Err(ErrorCode::InvalidProgram)
}
