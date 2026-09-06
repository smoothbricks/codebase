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

use columine_types::types::{ErrorCode, Opcode, PROGRAM_HASH_PREFIX, ProgramHeader};

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

/// Unset parent timestamp column of a `FLAT_MAP` body.
const NO_PARENT_TS_COL: u8 = 0xFF;

/// Visit every column operand of `program`'s reduce section. `state` is the
/// initialised state the program runs against: a `BatchMapUpsertLast` on a
/// TTL slot reads the slot's timestamp column, which only the state names.
/// A reduce section the VM would refuse is `InvalidProgram` here too.
pub fn reduce_column_uses(
    program: &[u8],
    state: &[u8],
    visit: &mut dyn FnMut(u8, ColumnUse),
) -> Result<(), ErrorCode> {
    let content = program
        .get(PROGRAM_HASH_PREFIX as usize..)
        .ok_or(ErrorCode::InvalidProgram)?;
    let header_bytes = content
        .get(..ProgramHeader::WIRE_SIZE)
        .and_then(|bytes| <[u8; ProgramHeader::WIRE_SIZE]>::try_from(bytes).ok())
        .ok_or(ErrorCode::InvalidProgram)?;
    let header = ProgramHeader::from_wire_bytes(header_bytes);
    let code_start = ProgramHeader::WIRE_SIZE + usize::from(header.init_code_len);
    let code = content
        .get(code_start..code_start + usize::from(header.reduce_code_len))
        .ok_or(ErrorCode::InvalidProgram)?;
    walk(code, state, false, visit)
}

/// Walk one instruction sequence: the top level (`element == false`, where
/// a `*_IF` opcode is not dispatched and its predicate is a plain read) or
/// a `FOR_EACH`/`FLAT_MAP` body.
fn walk(
    code: &[u8],
    state: &[u8],
    element: bool,
    visit: &mut dyn FnMut(u8, ColumnUse),
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
        // `body_op_len` proved `code[pc..pc + len]` is in bounds.
        let ops = &code[pc + 1..pc + len];
        match op {
            Opcode::Halt => break,
            Opcode::BatchMapUpsertLatest | Opcode::BatchMapUpsertLatestTtl => {
                read(visit, &ops[1..4]);
            }
            Opcode::BatchMapUpsertFirst => read(visit, &ops[1..3]),
            Opcode::BatchMapUpsertLast => {
                read(visit, &ops[1..3]);
                // The TTL slot's timestamp column is named by the slot, not
                // the instruction.
                let meta = SlotMetaView::read(state, ops[0]);
                if meta.has_ttl() {
                    visit(meta.timestamp_field_idx(state), ColumnUse::Read);
                }
            }
            Opcode::BatchMapRemove => read(visit, &ops[1..2]),
            Opcode::BatchMapUpsertLastTtl => read(visit, &ops[1..4]),
            Opcode::BatchMapUpsertMax | Opcode::BatchMapUpsertMin => read(visit, &ops[1..4]),
            Opcode::BatchMapUpsertLatestIf => {
                read(visit, &ops[1..4]);
                visit(ops[5], predicate);
            }
            Opcode::BatchMapUpsertFirstIf | Opcode::BatchMapUpsertLastIf => {
                read(visit, &ops[1..3]);
                visit(ops[3], predicate);
            }
            Opcode::BatchMapRemoveIf => {
                read(visit, &ops[1..2]);
                visit(ops[2], predicate);
            }
            Opcode::BatchMapUpsertMaxIf | Opcode::BatchMapUpsertMinIf => {
                read(visit, &ops[1..4]);
                visit(ops[5], predicate);
            }
            Opcode::BatchStructMapProbe => {
                // probe_slot, key, miss_mode, out_slot, num_fields,
                // (probe_field, out_field) × num_fields, out_key.
                visit(ops[1], ColumnUse::Read);
                let num_fields = usize::from(ops[4]);
                visit(ops[5 + num_fields * 2], ColumnUse::Read);
            }
            Opcode::BatchStructMapProbeScatter => visit(ops[1], ColumnUse::Read),
            Opcode::BatchSetInsert
            | Opcode::BatchSetRemove
            | Opcode::BatchBitmapAdd
            | Opcode::BatchBitmapRemove => visit(ops[1], ColumnUse::Read),
            Opcode::BatchBitmapAnd
            | Opcode::BatchBitmapOr
            | Opcode::BatchBitmapAndNot
            | Opcode::BatchBitmapXor
            | Opcode::BatchBitmapAndScratch
            | Opcode::BatchBitmapOrScratch
            | Opcode::BatchBitmapAndNotScratch
            | Opcode::BatchBitmapXorScratch
            | Opcode::BatchAggCount => {}
            Opcode::BatchSetInsertTtl => read(visit, &ops[1..3]),
            Opcode::BatchSetInsertIf => {
                visit(ops[1], ColumnUse::Read);
                visit(ops[2], predicate);
            }
            Opcode::BatchAggSum
            | Opcode::BatchAggMin
            | Opcode::BatchAggMax
            | Opcode::BatchAggSumI64
            | Opcode::BatchAggMinI64
            | Opcode::BatchAggMaxI64
            | Opcode::ListAppend => visit(ops[1], ColumnUse::Read),
            // The aggregate pass reads its predicate from the batch's bytes,
            // never through the live handle.
            Opcode::BatchAggSumIf
            | Opcode::BatchAggMinIf
            | Opcode::BatchAggMaxIf
            | Opcode::BatchScalarLatest => read(visit, &ops[1..3]),
            Opcode::BatchAggCountIf => visit(ops[1], ColumnUse::Read),
            Opcode::BatchStructMapUpsertLast
            | Opcode::BatchStructMapUpsertFirst
            | Opcode::BatchStructMapUpsertMax => {
                let operands = decode_struct_map_upsert_operands(
                    code,
                    pc + 1,
                    op == Opcode::BatchStructMapUpsertMax,
                )
                .ok_or(ErrorCode::InvalidProgram)?;
                visit(operands.key_col, ColumnUse::Read);
                let pairs_end = operands.scalar_pairs_start + operands.num_vals * 2;
                for pair in code[operands.scalar_pairs_start..pairs_end]
                    .as_chunks::<2>()
                    .0
                {
                    visit(pair[0], ColumnUse::Read);
                }
                let triples_end = operands.array_triples_start + operands.num_array_vals * 3;
                for triple in code[operands.array_triples_start..triples_end]
                    .as_chunks::<3>()
                    .0
                {
                    visit(triple[0], ColumnUse::Read);
                    visit(triple[1], ColumnUse::Read);
                }
            }
            Opcode::BatchStructMap2UpsertLast => {
                let operands = decode_struct_map2_upsert_operands(code, pc + 1)
                    .ok_or(ErrorCode::InvalidProgram)?;
                struct_map2_reads(code, operands, visit);
            }
            Opcode::BatchStructMap2UpsertMaxI64x2 => {
                let operands = decode_struct_map2_max_i64x2_operands(code, pc + 1)
                    .ok_or(ErrorCode::InvalidProgram)?;
                struct_map2_reads(code, operands.row, visit);
                visit(operands.cmp1_col, ColumnUse::Read);
                visit(operands.cmp2_col, ColumnUse::Read);
            }
            Opcode::BatchStructMap2Remove => read(visit, &ops[1..3]),
            Opcode::ListAppendStruct => {
                // slot, num_vals, (col, field) × num_vals.
                for pair in ops[2..].as_chunks::<2>().0 {
                    visit(pair[0], ColumnUse::Read);
                }
            }
            Opcode::NestedSetInsert | Opcode::NestedAggUpdate => read(visit, &ops[1..3]),
            Opcode::NestedMapUpsertLast => read(visit, &ops[1..4]),
            Opcode::FlatMap => {
                // offsets, parent_ts, body_len u16, body.
                visit(ops[0], ColumnUse::Read);
                if ops[1] != NO_PARENT_TS_COL {
                    visit(ops[1], ColumnUse::Read);
                }
                walk(&ops[4..], state, true, visit)?;
            }
            Opcode::ForEach => {
                // col, match_count, ids u32 × match_count, body_len u16, body.
                visit(ops[0], ColumnUse::Read);
                let body_start = 2 + usize::from(ops[1]) * 4 + 2;
                walk(&ops[body_start..], state, true, visit)?;
            }
            _ => return Err(ErrorCode::InvalidProgram),
        }
        pc += len;
    }
    Ok(())
}

fn read(visit: &mut dyn FnMut(u8, ColumnUse), cols: &[u8]) {
    for &col in cols {
        visit(col, ColumnUse::Read);
    }
}

fn struct_map2_reads(
    code: &[u8],
    operands: crate::vm::StructMap2UpsertOperands,
    visit: &mut dyn FnMut(u8, ColumnUse),
) {
    visit(operands.key1_col, ColumnUse::Read);
    visit(operands.key2_col, ColumnUse::Read);
    let pairs_end = operands.scalar_pairs_start + operands.num_vals * 2;
    for pair in code[operands.scalar_pairs_start..pairs_end]
        .as_chunks::<2>()
        .0
    {
        visit(pair[0], ColumnUse::Read);
    }
}
