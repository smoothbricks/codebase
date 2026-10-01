//! The column operands of a reduce section, with how each is read.
//!
//! [`reduce_column_uses`] walks a program's reduce section — top-level ops,
//! `FOR_EACH` bodies and the `FLAT_MAP` bodies nested in them — and reports
//! every operand that names a batch column together with its
//! [`ColumnUse`]. The one distinction that matters to a caller is whether a
//! read resolves through the live-column handle: the per-element
//! conditional opcodes (`*_IF` in a `FOR_EACH` body) ask [`LiveColumns`]
//! for their predicate cell; every other read — aggregates, their `*_IF`
//! predicates, keys, values, timestamps, elements, offsets, the `FOR_EACH`
//! type column — takes the bytes the batch carries. A live column carries
//! zero placeholder cells, so such a read would silently see zeros;
//! `row_exprs` refuses the bind instead.
//!
//! WHY a walker and not a role table beside the dispatch: the operand order
//! of every opcode is already spelled once in the dispatch arms and once in
//! `body_op_len`; this walk is the third spelling and the tests hold the
//! three together. An opcode the walk does not know is `InvalidProgram`, the
//! same answer the dispatch gives an opcode it does not know — so a new
//! reduce opcode without an arm here fails loudly on its first live bind,
//! never silently.
//!
//! [`LiveColumns`]: crate::row_exprs::LiveColumns

use columine_types::operand::Operands;
use columine_types::types::{ErrorCode, NO_PARENT_TS_COL, Opcode, ProgramHeader};

use crate::meta::SlotMetaView;
use crate::vm::{
    body_op_len, decode_struct_map_upsert_operands, decode_struct_map2_max_i64x2_operands,
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
}

/// Visit every column operand of `program`'s reduce section. `state` is the
/// initialised state the program runs against: a `BatchMapUpsertLast` on a
/// TTL slot reads the slot's timestamp column, which only the state names.
/// A reduce section the VM would refuse is `InvalidProgram` here too.
pub fn reduce_column_uses(
    program: &[u8],
    state: &[u8],
    visit: &mut dyn FnMut(u32, ColumnUse),
) -> Result<(), ErrorCode> {
    let sections = ProgramHeader::sections(program).ok_or(ErrorCode::InvalidProgram)?;
    walk(sections.reduce_code, state, false, visit)
}

/// Walk one instruction sequence: the top level (`element == false`, where
/// a `*_IF` opcode is not dispatched and its predicate is a plain read) or
/// a `FOR_EACH`/`FLAT_MAP` body.
fn walk(
    code: &[u8],
    state: &[u8],
    element: bool,
    visit: &mut dyn FnMut(u32, ColumnUse),
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
        walk_op(op, &mut ops, code, pc, state, predicate, visit)
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
    state: &[u8],
    predicate: ColumnUse,
    visit: &mut dyn FnMut(u32, ColumnUse),
) -> Option<Result<(), ErrorCode>> {
    // Read `n` index operands as plain column reads.
    let reads = |ops: &mut Operands<'_>, n: usize, visit: &mut dyn FnMut(u32, ColumnUse)| {
        for _ in 0..n {
            visit(ops.index()?, ColumnUse::Read);
        }
        Some(())
    };
    match op {
        Opcode::Halt => {}
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
            let meta = SlotMetaView::read(state, slot);
            if meta.has_ttl() {
                visit(meta.timestamp_col(state), ColumnUse::Read);
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
            visit(ops.index()?, predicate);
        }
        Opcode::BatchMapUpsertFirstIf | Opcode::BatchMapUpsertLastIf => {
            ops.index()?;
            reads(ops, 2, visit)?;
            visit(ops.index()?, predicate);
        }
        Opcode::BatchMapRemoveIf => {
            ops.index()?;
            reads(ops, 1, visit)?;
            visit(ops.index()?, predicate);
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
            visit(ops.index()?, predicate);
        }
        Opcode::NestedMapUpsertLast => {
            ops.index()?;
            reads(ops, 3, visit)?;
        }
        Opcode::BatchStructMapUpsertLast
        | Opcode::BatchStructMapUpsertFirst
        | Opcode::BatchStructMapUpsertMax => {
            let operands =
                decode_struct_map_upsert_operands(code, pc + 1, op == Opcode::BatchStructMapUpsertMax)?;
            visit(operands.key_col, ColumnUse::Read);
            for &col in operands.vals.cols() {
                visit(col, ColumnUse::Read);
            }
            for array in operands.array_fields() {
                visit(array.offsets_col, ColumnUse::Read);
                visit(array.values_col, ColumnUse::Read);
            }
        }
        Opcode::BatchStructMap2UpsertLast => {
            struct_map2_reads(&decode_struct_map2_upsert_operands(code, pc + 1)?, visit);
        }
        Opcode::BatchStructMap2UpsertMaxI64x2 => {
            let operands = decode_struct_map2_max_i64x2_operands(code, pc + 1)?;
            struct_map2_reads(&operands.row, visit);
            visit(operands.cmp1_col, ColumnUse::Read);
            visit(operands.cmp2_col, ColumnUse::Read);
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
            // offsets, parent_ts, body_len u16, body.
            reads(ops, 1, visit)?;
            let parent_ts = ops.index()?;
            if parent_ts != NO_PARENT_TS_COL {
                visit(parent_ts, ColumnUse::Read);
            }
            let body_len = usize::from(ops.u16()?);
            return Some(walk(ops.bytes(body_len)?, state, true, visit));
        }
        Opcode::ForEach => {
            // col, match_count, ids u32 × match_count, body_len u16, body.
            reads(ops, 1, visit)?;
            let match_count = usize::try_from(ops.index()?).ok()?;
            ops.bytes(match_count * 4)?;
            let body_len = usize::from(ops.u16()?);
            return Some(walk(ops.bytes(body_len)?, state, true, visit));
        }
        _ => return Some(Err(ErrorCode::InvalidProgram)),
    }
    Some(Ok(()))
}

/// `num_routes:u8, (kind, dest_slot, dest_field, v_col) × num_routes`: the
/// value column of every route is a read.
fn route_value_reads(ops: &mut Operands<'_>, visit: &mut dyn FnMut(u32, ColumnUse)) -> Option<()> {
    let num_routes = ops.byte()?;
    for _ in 0..num_routes {
        ops.byte()?;
        ops.index()?;
        ops.byte()?;
        visit(ops.index()?, ColumnUse::Read);
    }
    Some(())
}

fn struct_map2_reads(
    operands: &crate::vm::StructMap2UpsertOperands,
    visit: &mut dyn FnMut(u32, ColumnUse),
) {
    visit(operands.key1_col, ColumnUse::Read);
    visit(operands.key2_col, ColumnUse::Read);
    for &col in operands.vals.cols() {
        visit(col, ColumnUse::Read);
    }
}
