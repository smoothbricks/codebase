//! A `FOR_EACH` body's per-element form.
//!
//! A program carries its index operands as LEB128 (`columine_types::operand`)
//! so a narrow program stays small. A `FOR_EACH` body runs once per matched
//! element, though, and decoding its instructions again for every element is
//! work the batch already did. So the batch widens the body once: aggregates
//! (pass one's, never an element's) are dropped, every index operand becomes a
//! little-endian `u32`, and a `FLAT_MAP`'s inner body is widened in place
//! behind a `u32` length. An element then reads fixed-width operands with no
//! per-operand branch, and walks only the instructions it executes.
//!
//! The instruction shapes are stated once, in [`walk_operands`]: measuring an
//! instruction ([`crate::vm::body_op_len`]) and widening it are the same walk
//! with a different [`OperandSink`].

use columine_types::operand::Operands;
use columine_types::types::Opcode;

use crate::vm::{
    MAX_STRUCT_ARRAY_OPERANDS, MAX_STRUCT_SCALAR_OPERANDS, body_op_len, is_aggregate_op,
};

/// Operand reads in wire order, over either encoding a body takes: the
/// program's ([`Operands`], LEB128 indexes) or a widened body
/// ([`WideOperands`], `u32` indexes). A decoder written against it reads both.
pub(crate) trait OperandRead<'a> {
    /// A column, slot or count index.
    fn index(&mut self) -> Option<u32>;
    /// One fixed-width byte.
    fn byte(&mut self) -> Option<u8>;
    /// The next `len` bytes, unread.
    fn bytes(&mut self, len: usize) -> Option<&'a [u8]>;
    /// Position of the next unread byte.
    fn pos(&self) -> usize;

    /// A little-endian `u32`.
    fn u32(&mut self) -> Option<u32> {
        let bytes: [u8; 4] = self.bytes(4)?.try_into().ok()?;
        Some(u32::from_le_bytes(bytes))
    }
}

impl<'a> OperandRead<'a> for Operands<'a> {
    #[inline(always)]
    fn index(&mut self) -> Option<u32> {
        Operands::index(self)
    }

    #[inline(always)]
    fn byte(&mut self) -> Option<u8> {
        Operands::byte(self)
    }

    #[inline(always)]
    fn bytes(&mut self, len: usize) -> Option<&'a [u8]> {
        Operands::bytes(self, len)
    }

    #[inline(always)]
    fn pos(&self) -> usize {
        Operands::pos(self)
    }
}

/// Operands of a widened body: every index is a little-endian `u32`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct WideOperands<'a> {
    code: &'a [u8],
    at: usize,
}

impl<'a> WideOperands<'a> {
    /// Read `code` from position `at` (the first operand byte).
    pub(crate) const fn at(code: &'a [u8], at: usize) -> Self {
        Self { code, at }
    }
}

impl<'a> OperandRead<'a> for WideOperands<'a> {
    #[inline(always)]
    fn index(&mut self) -> Option<u32> {
        self.u32()
    }

    #[inline(always)]
    fn byte(&mut self) -> Option<u8> {
        let value = *self.code.get(self.at)?;
        self.at += 1;
        Some(value)
    }

    #[inline(always)]
    fn bytes(&mut self, len: usize) -> Option<&'a [u8]> {
        let end = self.at.checked_add(len)?;
        let bytes = self.code.get(self.at..end)?;
        self.at = end;
        Some(bytes)
    }

    #[inline(always)]
    fn pos(&self) -> usize {
        self.at
    }

    #[inline(always)]
    fn u32(&mut self) -> Option<u32> {
        let bytes: [u8; 4] = self.bytes(4)?.try_into().ok()?;
        Some(u32::from_le_bytes(bytes))
    }
}

/// Where a walk sends an instruction's operands, in wire order: each index
/// as its value, every fixed-width operand as its bytes.
pub(crate) trait OperandSink {
    fn index(&mut self, value: u32);
    fn raw(&mut self, bytes: &[u8]);
}

/// A walk that only measures.
pub(crate) struct Skip;

impl OperandSink for Skip {
    #[inline(always)]
    fn index(&mut self, _: u32) {}

    #[inline(always)]
    fn raw(&mut self, _: &[u8]) {}
}

/// A walk that writes the widened form: indexes as `u32`, the rest as read.
struct Widen<'o>(&'o mut Vec<u8>);

impl OperandSink for Widen<'_> {
    fn index(&mut self, value: u32) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }

    fn raw(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(bytes);
    }
}

fn index<S: OperandSink>(r: &mut Operands<'_>, sink: &mut S) -> Option<()> {
    sink.index(r.index()?);
    Some(())
}

fn raw<S: OperandSink>(r: &mut Operands<'_>, sink: &mut S, len: usize) -> Option<()> {
    sink.raw(r.bytes(len)?);
    Some(())
}

/// A count operand: forwarded as its byte, answered as its value.
fn count<S: OperandSink>(r: &mut Operands<'_>, sink: &mut S) -> Option<usize> {
    let bytes = r.bytes(1)?;
    sink.raw(bytes);
    Some(usize::from(bytes[0]))
}

/// One fixed-shape operand of the layout table.
#[derive(Clone, Copy)]
enum Shape {
    /// A column, slot or count index.
    I,
    /// A fixed-width byte.
    B,
}

/// Walk the operands of `op` from `r` (just past its opcode) into `sink`.
/// `None` for an opcode with no body layout, or operands short of their
/// layout. A `FLAT_MAP`'s inner body and a `FOR_EACH`'s body pass as raw
/// bytes: measuring needs no more, and widening handles `FLAT_MAP` itself.
pub(crate) fn walk_operands<S: OperandSink>(
    op: Opcode,
    r: &mut Operands<'_>,
    sink: &mut S,
) -> Option<()> {
    use Shape::{B, I};
    let fixed: &[Shape] = match op {
        Opcode::Halt => &[],
        Opcode::BatchMapUpsertLatest | Opcode::BatchMapUpsertLatestTtl => &[I, I, I, I, B],
        Opcode::BatchMapUpsertFirst | Opcode::BatchMapUpsertLast => &[I, I, I],
        Opcode::BatchMapRemove => &[I, I],
        Opcode::BatchMapUpsertLastTtl => &[I, I, I, I],
        Opcode::BatchMapUpsertMax | Opcode::BatchMapUpsertMin => &[I, I, I, I, B],
        Opcode::BatchMapUpsertLatestIf => &[I, I, I, I, B, I],
        Opcode::BatchMapUpsertFirstIf | Opcode::BatchMapUpsertLastIf => &[I, I, I, I],
        Opcode::BatchMapRemoveIf => &[I, I, I],
        Opcode::BatchMapUpsertMaxIf | Opcode::BatchMapUpsertMinIf => &[I, I, I, I, B, I],
        //#region reduce-typed-state.probe-len
        Opcode::BatchStructMapProbe => {
            // probe_slot, key_col, miss_mode, out_slot, num_fields,
            // (probe_field, out_field) × num_fields, out_key_col.
            index(r, sink)?;
            index(r, sink)?;
            raw(r, sink, 1)?;
            index(r, sink)?;
            let num_fields = count(r, sink)?;
            raw(r, sink, num_fields.checked_mul(2)?)?;
            index(r, sink)?;
            &[]
        }
        //#region reduce-typed-state.scatter-len
        Opcode::BatchStructMapProbeScatter => {
            // probe_slot, key_col, miss_mode, route_field, op_field,
            // num_routes, (kind, dest_slot, dest_field, out_key_field,
            // v_src_field) × num_routes.
            index(r, sink)?;
            index(r, sink)?;
            raw(r, sink, 3)?;
            let num_routes = count(r, sink)?;
            for _ in 0..num_routes {
                raw(r, sink, 1)?;
                index(r, sink)?;
                raw(r, sink, 3)?;
            }
            &[]
        }
        //#region reduce-typed-state.scatter-element-len
        Opcode::BatchStructMapScatter => {
            // route_col, op_col, key_col, then the route table.
            for _ in 0..3 {
                index(r, sink)?;
            }
            scatter_routes(r, sink)?;
            &[]
        }
        //#endregion struct-map scatter-element length
        //#region reduce-typed-state.scatter-element-guarded-len
        // The guard pairs sit between the fixed prefix and `num_routes`, so
        // the route count is found through the guard count, never at a fixed
        // offset. A width the compare array cannot hold is refused at
        // execution, not here: this answers only how long the instruction is.
        Opcode::BatchStructMapScatterGuarded => {
            for _ in 0..4 {
                index(r, sink)?;
            }
            let num_guards = count(r, sink)?;
            for _ in 0..num_guards {
                index(r, sink)?;
                raw(r, sink, 1)?;
            }
            scatter_routes(r, sink)?;
            &[]
        }
        //#endregion struct-map guarded scatter-element length
        Opcode::BatchSetInsert
        | Opcode::BatchSetRemove
        | Opcode::BatchBitmapAdd
        | Opcode::BatchBitmapRemove
        | Opcode::BatchBitmapAnd
        | Opcode::BatchBitmapOr
        | Opcode::BatchBitmapAndNot
        | Opcode::BatchBitmapXor => &[I, I],
        Opcode::BatchBitmapAndScratch
        | Opcode::BatchBitmapOrScratch
        | Opcode::BatchBitmapAndNotScratch
        | Opcode::BatchBitmapXorScratch => &[I],
        Opcode::BatchSetInsertTtl | Opcode::BatchSetInsertIf => &[I, I, I],
        Opcode::BatchAggSum | Opcode::BatchAggMin | Opcode::BatchAggMax => &[I, I],
        Opcode::BatchAggCount => &[I],
        Opcode::BatchAggSumIf => &[I, I, I],
        Opcode::BatchAggCountIf => &[I, I],
        Opcode::BatchAggMinIf | Opcode::BatchAggMaxIf | Opcode::BatchScalarLatest => &[I, I, I],
        Opcode::BatchAggSumI64 | Opcode::BatchAggMinI64 | Opcode::BatchAggMaxI64 => &[I, I],
        Opcode::BatchStructMapUpsertLast
        | Opcode::BatchStructMapUpsertFirst
        | Opcode::BatchStructMapUpsertMax => {
            // slot, key_col, num_vals, (val_col, field_idx) × num_vals,
            // num_arrays, (offsets_col, values_col, field_idx) × num_arrays,
            // and 0x82's comparison field ordinal.
            index(r, sink)?;
            index(r, sink)?;
            pairs(r, sink, MAX_STRUCT_SCALAR_OPERANDS)?;
            let num_arrays = count(r, sink)?;
            if num_arrays > MAX_STRUCT_ARRAY_OPERANDS {
                return None;
            }
            for _ in 0..num_arrays {
                index(r, sink)?;
                index(r, sink)?;
                raw(r, sink, 1)?;
            }
            if op == Opcode::BatchStructMapUpsertMax {
                raw(r, sink, 1)?;
            }
            &[]
        }
        Opcode::BatchStructMap2UpsertLast => {
            // slot, key1_col, key2_col, then the value pairs.
            for _ in 0..3 {
                index(r, sink)?;
            }
            pairs(r, sink, MAX_STRUCT_SCALAR_OPERANDS)?;
            &[]
        }
        Opcode::BatchStructMap2UpsertMaxI64x2 => {
            // 0x83's row, then (cmp_col, cmp_field) for both comparison lanes,
            // which take two of the row's pair slots.
            for _ in 0..3 {
                index(r, sink)?;
            }
            pairs(r, sink, MAX_STRUCT_SCALAR_OPERANDS - 2)?;
            &[I, B, I, B]
        }
        Opcode::ListAppend => &[I, I],
        Opcode::BatchStructMap2Remove => &[I, I, I],
        Opcode::ListAppendStruct => {
            // slot, then the value pairs.
            index(r, sink)?;
            pairs(r, sink, MAX_STRUCT_SCALAR_OPERANDS)?;
            &[]
        }
        Opcode::FlatMap => {
            // offsets_col, parent_ts_col, inner_body_len:u16, inner body.
            index(r, sink)?;
            index(r, sink)?;
            let len = r.u16()?;
            sink.raw(&len.to_le_bytes());
            raw(r, sink, usize::from(len))?;
            &[]
        }
        Opcode::NestedSetInsert => &[I, I, I],
        Opcode::NestedMapUpsertLast => &[I, I, I, I],
        Opcode::NestedAggUpdate => &[I, I, I],
        Opcode::ForEach => {
            // type_col, match_count, match_ids:u32 × match_count,
            // body_len:u16, body.
            index(r, sink)?;
            let match_count = r.index()?;
            sink.index(match_count);
            raw(r, sink, usize::try_from(match_count).ok()?.checked_mul(4)?)?;
            let len = r.u16()?;
            sink.raw(&len.to_le_bytes());
            raw(r, sink, usize::from(len))?;
            &[]
        }
        _ => return None,
    };
    for shape in fixed {
        match shape {
            I => index(r, sink)?,
            B => raw(r, sink, 1)?,
        }
    }
    Some(())
}

/// `num_vals:u8, (val_col:index, field_idx:u8) × num_vals`, at most `max`
/// pairs: the decoders' fixed arrays make the maximum part of the accepted
/// program.
fn pairs<S: OperandSink>(r: &mut Operands<'_>, sink: &mut S, max: usize) -> Option<()> {
    let num_vals = count(r, sink)?;
    if num_vals > max {
        return None;
    }
    for _ in 0..num_vals {
        index(r, sink)?;
        raw(r, sink, 1)?;
    }
    Some(())
}

/// `num_routes:u8, (kind:u8, dest_slot:index, dest_field:u8, v_col:index) ×
/// num_routes` — the route table both probe-free scatters share.
fn scatter_routes<S: OperandSink>(r: &mut Operands<'_>, sink: &mut S) -> Option<()> {
    let num_routes = count(r, sink)?;
    for _ in 0..num_routes {
        raw(r, sink, 1)?;
        index(r, sink)?;
        raw(r, sink, 1)?;
        index(r, sink)?;
    }
    Some(())
}

/// Append `body`'s per-element form to `out`. `None` for a body that does
/// not decode; the caller validated it, so that is a malformed program.
pub(crate) fn widen_element_body(body: &[u8], out: &mut Vec<u8>) -> Option<()> {
    let mut pc = 0;
    while pc < body.len() {
        let op = Opcode::from_u8(body[pc])?;
        if is_aggregate_op(op) {
            pc = pc.checked_add(body_op_len(body, pc)?)?;
            continue;
        }
        out.push(body[pc]);
        let mut r = Operands::at(body, pc.checked_add(1)?);
        if op == Opcode::FlatMap {
            let mut sink = Widen(out);
            index(&mut r, &mut sink)?;
            index(&mut r, &mut sink)?;
            let inner_len = usize::from(r.u16()?);
            let inner = r.bytes(inner_len)?;
            let len_at = out.len();
            out.extend_from_slice(&[0; 4]);
            widen_element_body(inner, out)?;
            let inner_wide = u32::try_from(out.len() - len_at - 4).ok()?;
            out[len_at..len_at + 4].copy_from_slice(&inner_wide.to_le_bytes());
        } else {
            walk_operands(op, &mut r, &mut Widen(out))?;
        }
        pc = r.pos();
    }
    Some(())
}
