//! Value-object CSR inputs of a reduce section, linked at load and admitted
//! per batch.
//!
//! [`InputPlan::compile`] links each struct-map CSR pair the program reads for
//! a value-object field (the `0x80`–`0x82` array fields and the `0x88` counts
//! init pair) to that field's schema, and records the scope the pair is read
//! under: the batch rows, filtered by a top-level `FOR_EACH`, expanded through
//! each enclosing `FLAT_MAP`. It then classifies every operand of the reduce
//! section by role and refuses any reader that would observe a projected
//! column: a projection replaces a pair's columns for every reader, so only
//! the pair's own linked CSR reads may name them.
//! [`InputPlan::admit_and_execute`] then admits exactly the selected frames,
//! each once, before the program runs. A frame the capability projected into
//! the reader's layout is written into the rebuilt CSR columns the program
//! receives as it is admitted; otherwise the batch's own columns are borrowed
//! unchanged.
//!
//! CSR offsets are byte offsets into their values column: element `e` is
//! `values[offsets[e]..offsets[e + 1]]`, and a value-object field's arena
//! element size is one byte.
//!
//! Physical CSR geometry is generic storage. Semantic frame admission and
//! compatible historical-writer projection belong to the injected capability;
//! this module contains no VO codec or positive substitute for that capability.

use columine_types::operand::Operands;
use columine_types::types::{ErrorCode, Opcode};
use columine_types::vo::{VoAdmission, VoFrameProof, VoProgramMetadata, VoRefusal, admit_frame};

use crate::column_uses::{ColumnOperand, reduce_column_roles};
use crate::counts_fold::CountsFoldOp;
use crate::program_admission::ProgramAdmissionError;
use crate::vm::{body_op_len, decode_struct_map_upsert_operands};

/// Container nesting a program may reach. Bounds compile recursion, and the
/// runtime FLAT_MAP walk keeps one offset floor per level.
const MAX_NESTING: usize = 64;

/// Why a batch's value-object inputs were refused before the program ran.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InputAdmissionError {
    /// A column or CSR cell the program reads is missing, short, or malformed.
    Physical(ErrorCode),
    /// The capability refused one selected frame.
    Frame {
        slot: u32,
        field_idx: u8,
        row: u32,
        cause: VoRefusal,
    },
    /// Binding the program's row-expression table failed; nothing was
    /// admitted or run.
    Rows(ErrorCode),
    /// The program carries a row-expression table but was run without
    /// binding it. Its scopes and VO columns may be derived, so admission
    /// reads the bound columns: run it through `AdmittedProgram::execute_bound`.
    UnboundRows,
}

/// Reusable working memory of [`InputPlan::admit_and_execute`]: per-input
/// visited marks and rebuilt columns. Empty until first use; a warmed lane
/// reuses capacity up to its high-water marks. Same-layout frames stay borrowed.
#[derive(Debug, Default)]
pub struct ProgramScratch {
    inputs: Vec<InputScratch>,
}

#[derive(Debug, Default)]
struct InputScratch {
    /// Bit `e` is set when element `e`'s CSR cell is selected.
    visited: Vec<u64>,
    /// Whether a selected frame was projected. Only then are the rebuilt
    /// columns substituted for the batch's own.
    projected: bool,
    /// The rebuilt offsets column of a projected input.
    offsets: Vec<u8>,
    /// The rebuilt values column of a projected input.
    values: Vec<u8>,
}

/// The value-object CSR inputs of one reduce section, linked at load.
#[derive(Debug)]
pub(crate) struct InputPlan {
    inputs: Vec<Input>,
}

/// One CSR pair and the schema its frames carry. A pair read by several
/// fields with the same schema is one input.
#[derive(Debug)]
struct Input {
    /// Index into `metadata.fields` of the first link that named this pair.
    link: usize,
    slot: u32,
    field_idx: u8,
    offsets_col: u32,
    values_col: u32,
    /// The union of the scopes the pair is read under, deduplicated.
    scopes: Vec<Scope>,
}

/// The elements a CSR pair is read at: the batch rows (all of them, or those
/// whose top-level FOR_EACH type cell matches), each expanded through the
/// enclosing FLAT_MAP offsets, outermost first.
#[derive(Clone, Debug, Default, PartialEq)]
struct Scope {
    filter: Option<ForEachFilter>,
    flat_maps: Vec<u32>,
}

#[derive(Clone, Debug, PartialEq)]
struct ForEachFilter {
    type_col: u32,
    /// Exactly the ids the instruction names, in order.
    match_ids: Box<[u32]>,
}

impl InputPlan {
    /// Link every value-object CSR pair the reduce section `code` reads to a
    /// schema of `metadata`, then refuse any operand that would observe a
    /// projected column. Field kinds are program admission's to validate;
    /// `init` supplies only the timestamp columns of TTL slots.
    pub(crate) fn compile(
        init: &[u8],
        code: &[u8],
        metadata: &VoProgramMetadata,
    ) -> Result<Self, ProgramAdmissionError> {
        let mut builder = Builder {
            metadata,
            inputs: Vec::new(),
        };
        builder.collect(code, &mut Scope::default(), 0)?;
        builder.check_roles(init, code)?;
        Ok(Self {
            inputs: builder.inputs,
        })
    }

    /// Admit every selected value-object frame, then run `execute` on the
    /// batch's columns. A projected frame's CSR columns are substituted by
    /// their rebuilt versions. Every refusal happens before `execute` runs, and
    /// the original columns and state are never written.
    /// Memory that cannot back admission refuses the batch as
    /// `Physical(OutOfMemory)` before execution.
    pub(crate) fn admit_and_execute<A: VoAdmission>(
        &self,
        metadata: &VoProgramMetadata,
        admission: &A,
        cols: &[&[u8]],
        batch_len: u32,
        scratch: &mut ProgramScratch,
        execute: impl FnOnce(&[&[u8]]) -> u32,
    ) -> Result<u32, InputAdmissionError> {
        let count = self.inputs.len();
        if scratch.inputs.len() < count {
            let grow = count.saturating_sub(scratch.inputs.len());
            room(&mut scratch.inputs, grow).map_err(InputAdmissionError::Physical)?;
            scratch.inputs.resize_with(count, InputScratch::default);
        }
        let states = &mut scratch.inputs[..count];
        for (input, state) in self.inputs.iter().zip(states.iter_mut()) {
            input
                .mark(cols, batch_len, state)
                .map_err(InputAdmissionError::Physical)?;
            input.admit(metadata, admission, cols, state)?;
        }
        let states: &[InputScratch] = states;
        if states.iter().all(|state| !state.projected) {
            return Ok(execute(cols));
        }
        let mut refs: Vec<&[u8]> = Vec::new();
        room(&mut refs, cols.len()).map_err(InputAdmissionError::Physical)?;
        refs.extend_from_slice(cols);
        for (input, state) in self.inputs.iter().zip(states) {
            if state.projected {
                *refs
                    .get_mut(input.offsets_col as usize)
                    .ok_or(InputAdmissionError::Physical(ErrorCode::ColumnUnderrun))? =
                    state.offsets.as_slice();
                *refs
                    .get_mut(input.values_col as usize)
                    .ok_or(InputAdmissionError::Physical(ErrorCode::ColumnUnderrun))? =
                    state.values.as_slice();
            }
        }
        Ok(execute(&refs))
    }
}

struct Builder<'m> {
    metadata: &'m VoProgramMetadata,
    inputs: Vec<Input>,
}

impl Builder<'_> {
    /// Walk one instruction sequence: the reduce section at `depth == 0`, or a
    /// container body under `scope`.
    fn collect(
        &mut self,
        code: &[u8],
        scope: &mut Scope,
        depth: usize,
    ) -> Result<(), ProgramAdmissionError> {
        let invalid = ProgramAdmissionError::InvalidProgram(
            crate::state_init::ProgramInvalidCause::MalformedProgram,
        );
        let mut pc = 0usize;
        while pc < code.len() {
            let op = Opcode::from_u8(code[pc]).ok_or(invalid.clone())?;
            let len = body_op_len(code, pc).ok_or(invalid.clone())?;
            match op {
                Opcode::Halt if depth == 0 => break,
                Opcode::ForEach if depth == 0 => {
                    let mut r = Operands::at(code, pc + 1);
                    let type_col = r.index().ok_or(invalid.clone())?;
                    let count = r.index().ok_or(invalid.clone())?;
                    let ids_len = (count as usize).checked_mul(4).ok_or(invalid.clone())?;
                    let ids = r.bytes(ids_len).ok_or(invalid.clone())?;
                    let body_len = usize::from(r.u16().ok_or(invalid.clone())?);
                    let body = r.bytes(body_len).ok_or(invalid.clone())?;
                    scope.filter = Some(ForEachFilter {
                        type_col,
                        match_ids: ids
                            .as_chunks::<4>()
                            .0
                            .iter()
                            .map(|id| u32::from_le_bytes(*id))
                            .collect(),
                    });
                    self.collect(body, scope, depth + 1)?;
                    scope.filter = None;
                }
                Opcode::FlatMap if depth != 0 && depth < MAX_NESTING => {
                    let mut r = Operands::at(code, pc + 1);
                    let offsets_col = r.index().ok_or(invalid.clone())?;
                    // The parent timestamp column is not a CSR input.
                    r.index().ok_or(invalid.clone())?;
                    let body_len = usize::from(r.u16().ok_or(invalid.clone())?);
                    let body = r.bytes(body_len).ok_or(invalid.clone())?;
                    scope.flat_maps.push(offsets_col);
                    self.collect(body, scope, depth + 1)?;
                    scope.flat_maps.pop();
                }
                // FOR_EACH only at the top level, FLAT_MAP only inside a container
                // within the nesting bound, and HALT only at the top level: the
                // element executor refuses all three.
                Opcode::Halt | Opcode::ForEach | Opcode::FlatMap => return Err(invalid),
                Opcode::BatchStructMapUpsertLast
                | Opcode::BatchStructMapUpsertFirst
                | Opcode::BatchStructMapUpsertMax => {
                    let operands = decode_struct_map_upsert_operands(
                        code,
                        pc + 1,
                        op == Opcode::BatchStructMapUpsertMax,
                    )
                    .ok_or(invalid.clone())?;
                    // Only an array field linked to a schema is a value-object
                    // input; the others are ordinary arena fields.
                    for array in operands.array_fields() {
                        if let Some(link) = self.link(operands.slot, array.field_idx) {
                            self.add(link, array.offsets_col, array.values_col, scope)?;
                        }
                    }
                }
                Opcode::BatchStructMapCountsFold => {
                    let fold = CountsFoldOp::decode(code, pc + 1).map_err(|_| invalid.clone())?;
                    // A counts fold writes a value-object field, so it must be linked.
                    let link = self
                        .link(fold.slot, fold.field_idx)
                        .ok_or(invalid.clone())?;
                    self.add(link, fold.init_offsets_col, fold.init_values_col, scope)?;
                }
                _ => {}
            }
            pc += len;
        }
        Ok(())
    }

    /// Refuse every operand of the reduce section that would observe a
    /// projected column. A projection replaces the offsets and values columns
    /// of a linked pair for every reader, so an ordinary cell reading one of
    /// them would see transformed bytes.
    fn check_roles(&self, init: &[u8], code: &[u8]) -> Result<(), ProgramAdmissionError> {
        let mut refusal: Option<ProgramAdmissionError> = None;
        reduce_column_roles(init, code, &mut |operand| {
            if refusal.is_none() {
                refusal = self.check_role(operand).err();
            }
        })
        .map_err(|_| {
            ProgramAdmissionError::InvalidProgram(
                crate::state_init::ProgramInvalidCause::MalformedProgram,
            )
        })?;
        match refusal {
            Some(refusal) => Err(refusal),
            None => Ok(()),
        }
    }

    /// Whether one operand of the reduce section may read beside the inputs.
    /// A cell may not name a pair's column. A CSR may name a pair only as that
    /// pair's own linked field, with the pair's schema; any other CSR that
    /// overlaps a pair is refused, and an ordinary CSR never names a pair.
    fn check_role(&self, operand: ColumnOperand) -> Result<(), ProgramAdmissionError> {
        let invalid = ProgramAdmissionError::InvalidProgram(
            crate::state_init::ProgramInvalidCause::MalformedProgram,
        );
        match operand {
            ColumnOperand::Cell { column, .. } => {
                if self.inputs.iter().any(|input| input.holds(column)) {
                    return Err(invalid);
                }
            }
            ColumnOperand::Csr {
                slot,
                field_idx,
                offsets_col,
                values_col,
            } => {
                for input in &self.inputs {
                    if input.offsets_col == offsets_col && input.values_col == values_col {
                        let link = self.link(slot, field_idx).ok_or(invalid.clone())?;
                        let linked = self.metadata.fields.get(link).ok_or(invalid.clone())?;
                        let known = self
                            .metadata
                            .fields
                            .get(input.link)
                            .ok_or(invalid.clone())?;
                        if linked.schema != known.schema {
                            return Err(invalid);
                        }
                    } else if input.holds(offsets_col) || input.holds(values_col) {
                        return Err(invalid);
                    }
                }
            }
        }
        Ok(())
    }

    fn link(&self, slot: u32, field_idx: u8) -> Option<usize> {
        self.metadata
            .fields
            .iter()
            .position(|link| link.slot == slot && link.field_idx == field_idx)
    }

    /// Record one CSR pair read under `scope`. A pair is one input: it may be
    /// read again under another scope, but only with the same schema, and no
    /// column may serve as offsets for one pair and as values for another.
    fn add(
        &mut self,
        link: usize,
        offsets_col: u32,
        values_col: u32,
        scope: &Scope,
    ) -> Result<(), ProgramAdmissionError> {
        let invalid = ProgramAdmissionError::InvalidProgram(
            crate::state_init::ProgramInvalidCause::MalformedProgram,
        );
        if offsets_col == values_col {
            return Err(invalid);
        }
        let metadata = self.metadata;
        let field = metadata.fields.get(link).ok_or(invalid.clone())?;
        for input in &mut self.inputs {
            let same_offsets = input.offsets_col == offsets_col;
            let same_values = input.values_col == values_col;
            if same_offsets && same_values {
                let known = metadata.fields.get(input.link).ok_or(invalid.clone())?;
                if known.schema != field.schema {
                    return Err(invalid);
                }
                if !input.scopes.contains(scope) {
                    input.scopes.push(scope.clone());
                }
                return Ok(());
            }
            if same_offsets
                || same_values
                || input.values_col == offsets_col
                || input.offsets_col == values_col
            {
                return Err(invalid);
            }
        }
        self.inputs.push(Input {
            link,
            slot: field.slot,
            field_idx: field.field_idx,
            offsets_col,
            values_col,
            scopes: vec![scope.clone()],
        });
        Ok(())
    }
}

impl Input {
    fn columns<'c>(&self, cols: &[&'c [u8]]) -> Result<(&'c [u8], &'c [u8]), ErrorCode> {
        let offsets = cols
            .get(self.offsets_col as usize)
            .copied()
            .ok_or(ErrorCode::ColumnUnderrun)?;
        let values = cols
            .get(self.values_col as usize)
            .copied()
            .ok_or(ErrorCode::ColumnUnderrun)?;
        Ok((offsets, values))
    }

    /// Whether `col` is one of the pair's two columns.
    fn holds(&self, col: u32) -> bool {
        col == self.offsets_col || col == self.values_col
    }

    /// Mark every element the scopes select. A missing offsets column has no
    /// cells, so any selected element refuses in [`mark_element`].
    fn mark(
        &self,
        cols: &[&[u8]],
        batch_len: u32,
        state: &mut InputScratch,
    ) -> Result<(), ErrorCode> {
        let offsets = cols.get(self.offsets_col as usize).copied().unwrap_or(&[]);
        let cells = offsets.len() / 4;
        let cells_u32 = u32::try_from(cells).map_err(|_| ErrorCode::CapacityExceeded)?;
        state.visited.clear();
        let words = cells.div_ceil(64);
        room(&mut state.visited, words)?;
        state.visited.resize(words, 0);
        for scope in &self.scopes {
            mark_scope(scope, cols, batch_len, cells_u32, &mut state.visited)?;
        }
        Ok(())
    }

    /// Admit each marked element's frame once, in ascending order. A frame the
    /// capability returns as its own bytes is dropped after its check. A
    /// projected frame is written into the rebuilt columns at its element's
    /// position as it is admitted, so the certified bytes are copied once and
    /// no staging buffer holds them.
    fn admit<A: VoAdmission>(
        &self,
        metadata: &VoProgramMetadata,
        admission: &A,
        cols: &[&[u8]],
        state: &mut InputScratch,
    ) -> Result<(), InputAdmissionError> {
        // The plan was compiled against this link. A metadata that disagrees
        // is refused rather than guessed at.
        let link = metadata
            .fields
            .get(self.link)
            .filter(|link| link.slot == self.slot && link.field_idx == self.field_idx)
            .ok_or(InputAdmissionError::Physical(ErrorCode::InvalidProgram))?;
        let InputScratch {
            visited,
            projected,
            offsets,
            values,
        } = state;
        *projected = false;
        offsets.clear();
        values.clear();
        let mut rebuild = Rebuild::new(offsets, values);
        for (word_index, &word) in visited.iter().enumerate() {
            let mut bits = word;
            while bits != 0 {
                let bit = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                let row = u32::try_from(word_index * 64 + bit)
                    .map_err(|_| InputAdmissionError::Physical(ErrorCode::CapacityExceeded))?;
                let frame = self
                    .frame(cols, row)
                    .map_err(InputAdmissionError::Physical)?;
                let proof = admit_frame(admission, &link.schema, frame).map_err(|cause| {
                    InputAdmissionError::Frame {
                        slot: self.slot,
                        field_idx: self.field_idx,
                        row,
                        cause,
                    }
                })?;
                let certified = proof.bytes();
                if core::ptr::eq(certified, frame) {
                    continue;
                }
                *projected = true;
                let (src_offsets, src_values) =
                    self.columns(cols).map_err(InputAdmissionError::Physical)?;
                rebuild
                    .project(src_offsets, src_values, row, certified)
                    .map_err(InputAdmissionError::Physical)?;
            }
        }
        if *projected {
            let (src_offsets, src_values) =
                self.columns(cols).map_err(InputAdmissionError::Physical)?;
            rebuild
                .finish(src_offsets, src_values)
                .map_err(InputAdmissionError::Physical)?;
        }
        Ok(())
    }

    /// The frame of selected element `row`, from its checked CSR pair.
    fn frame<'c>(&self, cols: &[&'c [u8]], row: u32) -> Result<&'c [u8], ErrorCode> {
        let start = column_cell(cols, self.offsets_col, row)?;
        let next = row.checked_add(1).ok_or(ErrorCode::ColumnUnderrun)?;
        let end = column_cell(cols, self.offsets_col, next)?;
        if end < start {
            return Err(ErrorCode::InvalidCellValue);
        }
        let values = cols
            .get(self.values_col as usize)
            .copied()
            .ok_or(ErrorCode::ColumnUnderrun)?;
        values
            .get(start as usize..end as usize)
            .ok_or(ErrorCode::ColumnUnderrun)
    }
}

/// Streams one projected input's CSR into its rebuilt output columns, one
/// element at a time in ascending order. Elements `0..next` are written, and
/// `cursor` is the original values position the output has consumed through.
/// Each element is written as the gap before it, its offset, then its bytes:
/// the original bytes, or the certified ones for a projected element.
struct Rebuild<'o> {
    out_offsets: &'o mut Vec<u8>,
    out_values: &'o mut Vec<u8>,
    next: u32,
    cursor: usize,
}

impl<'o> Rebuild<'o> {
    fn new(out_offsets: &'o mut Vec<u8>, out_values: &'o mut Vec<u8>) -> Self {
        Self {
            out_offsets,
            out_values,
            next: 0,
            cursor: 0,
        }
    }

    /// Write projected `element` with its certified bytes, after the
    /// unselected elements before it.
    fn project(
        &mut self,
        offsets: &[u8],
        values: &[u8],
        element: u32,
        certified: &[u8],
    ) -> Result<(), ErrorCode> {
        self.copy_through(offsets, values, element)?;
        self.write(offsets, values, element, Some(certified))
    }

    /// Write the elements after the last projected one, then the final offset,
    /// the values past the last cell, and the offsets' bytes past the last
    /// whole cell, which pass through unchanged.
    fn finish(&mut self, offsets: &[u8], values: &[u8]) -> Result<(), ErrorCode> {
        let cells = u32::try_from(offsets.len() / 4).map_err(|_| ErrorCode::CapacityExceeded)?;
        let count = cells.checked_sub(1).ok_or(ErrorCode::InvalidCellValue)?;
        self.copy_through(offsets, values, count)?;
        push_offset(self.out_offsets, self.out_values.len())?;
        let tail = values.get(self.cursor..).ok_or(ErrorCode::ColumnUnderrun)?;
        room(self.out_values, tail.len())?;
        self.out_values.extend_from_slice(tail);
        let whole = offsets.len() / 4 * 4;
        let trailing = offsets.get(whole..).ok_or(ErrorCode::ColumnUnderrun)?;
        room(self.out_offsets, trailing.len())?;
        self.out_offsets.extend_from_slice(trailing);
        Ok(())
    }

    /// Write the unselected elements `next..upto` with their original bytes.
    fn copy_through(&mut self, offsets: &[u8], values: &[u8], upto: u32) -> Result<(), ErrorCode> {
        while self.next < upto {
            self.write(offsets, values, self.next, None)?;
        }
        Ok(())
    }

    /// Write `element` in order. The bounds of the original cell are checked
    /// against the cells written so far, so a prefix that is not monotonic
    /// refuses before any byte of it is trusted.
    fn write(
        &mut self,
        offsets: &[u8],
        values: &[u8],
        element: u32,
        certified: Option<&[u8]>,
    ) -> Result<(), ErrorCode> {
        let next = element.checked_add(1).ok_or(ErrorCode::ColumnUnderrun)?;
        let start = cell_u32(offsets, element).ok_or(ErrorCode::ColumnUnderrun)? as usize;
        let end = cell_u32(offsets, next).ok_or(ErrorCode::ColumnUnderrun)? as usize;
        if end < start || start < self.cursor {
            return Err(ErrorCode::InvalidCellValue);
        }
        let gap = values
            .get(self.cursor..start)
            .ok_or(ErrorCode::ColumnUnderrun)?;
        room(self.out_values, gap.len())?;
        self.out_values.extend_from_slice(gap);
        push_offset(self.out_offsets, self.out_values.len())?;
        let bytes = match certified {
            Some(bytes) => bytes,
            None => values.get(start..end).ok_or(ErrorCode::ColumnUnderrun)?,
        };
        room(self.out_values, bytes.len())?;
        self.out_values.extend_from_slice(bytes);
        self.cursor = end;
        self.next = next;
        Ok(())
    }
}

/// Grow `buf` by `additional` elements, or refuse as out of memory.
fn room<T>(buf: &mut Vec<T>, additional: usize) -> Result<(), ErrorCode> {
    buf.try_reserve(additional)
        .map_err(|_| ErrorCode::OutOfMemory)
}

/// Mark the elements `scope` selects in `marks`, lazily: filter the batch rows,
/// then expand each selected row through the nested FLAT_MAP offsets.
fn mark_scope(
    scope: &Scope,
    cols: &[&[u8]],
    batch_len: u32,
    cells: u32,
    marks: &mut [u64],
) -> Result<(), ErrorCode> {
    let mut floors = [0u32; MAX_NESTING];
    for row in 0..batch_len {
        if let Some(filter) = &scope.filter {
            let value = column_cell(cols, filter.type_col, row)?;
            if !filter.match_ids.contains(&value) {
                continue;
            }
        }
        descend(&scope.flat_maps, cols, row, cells, marks, &mut floors)?;
    }
    Ok(())
}

/// Expand `element` through the FLAT_MAP offsets `flat_maps`, outermost first,
/// and mark the leaves. Each level's visited offsets must be nondecreasing, so
/// the children of one level never overlap an earlier visited range.
fn descend(
    flat_maps: &[u32],
    cols: &[&[u8]],
    element: u32,
    cells: u32,
    marks: &mut [u64],
    floors: &mut [u32],
) -> Result<(), ErrorCode> {
    let Some((&offsets_col, inner)) = flat_maps.split_first() else {
        return mark_element(element, cells, marks);
    };
    let [floor, deeper @ ..] = floors else {
        return Err(ErrorCode::InvalidProgram);
    };
    let start = column_cell(cols, offsets_col, element)?;
    let end = column_cell(
        cols,
        offsets_col,
        element.checked_add(1).ok_or(ErrorCode::ColumnUnderrun)?,
    )?;
    if end < start || start < *floor {
        return Err(ErrorCode::InvalidCellValue);
    }
    *floor = end;
    for child in start..end {
        descend(inner, cols, child, cells, marks, deeper)?;
    }
    Ok(())
}

fn mark_element(element: u32, cells: u32, marks: &mut [u64]) -> Result<(), ErrorCode> {
    // The element's own pair, offsets[element] and offsets[element + 1], must
    // lie in the offsets column.
    match element.checked_add(1) {
        Some(next) if next < cells => {}
        _ => return Err(ErrorCode::ColumnUnderrun),
    }
    let word = marks
        .get_mut(element as usize / 64)
        .ok_or(ErrorCode::ColumnUnderrun)?;
    *word |= 1u64 << (element % 64);
    Ok(())
}

/// The `u32` cell `cell` of one column's bytes, read little-endian and checked.
fn cell_u32(bytes: &[u8], cell: u32) -> Option<u32> {
    let at = (cell as usize).checked_mul(4)?;
    let end = at.checked_add(4)?;
    Some(u32::from_le_bytes(bytes.get(at..end)?.try_into().ok()?))
}

fn column_cell(cols: &[&[u8]], col: u32, cell: u32) -> Result<u32, ErrorCode> {
    cols.get(col as usize)
        .and_then(|bytes| cell_u32(bytes, cell))
        .ok_or(ErrorCode::ColumnUnderrun)
}

/// Append one rebuilt offset. A rebuilt byte length past `u32` is a value the
/// output column cannot hold, so it is the input's invalid cell value rather
/// than a capacity of the storage growth.
fn push_offset(out: &mut Vec<u8>, at: usize) -> Result<(), ErrorCode> {
    let at = u32::try_from(at).map_err(|_| ErrorCode::InvalidCellValue)?;
    let bytes = at.to_le_bytes();
    room(out, bytes.len())?;
    out.extend_from_slice(&bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use columine_types::operand::encode_index;
    use columine_types::types::NO_PARENT_TS_COL;
    use columine_types::vo::{
        UnavailableVoAdmission, VoAdmissionError, VoFieldLink, VoMetadata, VoProfile,
    };

    use super::*;

    // The Rebuild tests use arbitrary opaque physical bytes: the rebuild is
    // the byte-moving half of admission and decodes no frame. Semantic frames
    // are certified by the real kernel's own tests, never by this module.

    /// The little-endian image of an offsets column holding `cells`.
    fn column(cells: &[u32]) -> Vec<u8> {
        cells.iter().flat_map(|cell| cell.to_le_bytes()).collect()
    }

    /// Element `element` of a CSR pair by plain slicing, independent of the
    /// production cell reader.
    fn element_bytes<'v>(offsets: &[u8], values: &'v [u8], element: usize) -> &'v [u8] {
        let cell = |index: usize| -> usize {
            let at = index * 4;
            let bytes: [u8; 4] = offsets[at..at + 4].try_into().unwrap();
            usize::try_from(u32::from_le_bytes(bytes)).unwrap()
        };
        &values[cell(element)..cell(element + 1)]
    }

    /// Projection of `element` to its certified `bytes`.
    fn at(element: u32, bytes: &'static [u8]) -> (u32, &'static [u8]) {
        (element, bytes)
    }

    /// Stream `projections` (ascending elements) and the finish through the
    /// production rebuild, returning the rebuilt (offsets, values).
    fn rebuilt(
        offsets: &[u8],
        values: &[u8],
        projections: &[(u32, &[u8])],
    ) -> Result<(Vec<u8>, Vec<u8>), ErrorCode> {
        let mut out_offsets = Vec::new();
        let mut out_values = Vec::new();
        let mut rebuild = Rebuild::new(&mut out_offsets, &mut out_values);
        for &(element, certified) in projections {
            rebuild.project(offsets, values, element, certified)?;
        }
        rebuild.finish(offsets, values)?;
        Ok((out_offsets, out_values))
    }

    /// Five elements `aa bbb ccc dddd ee`, written as
    ///   values  `PP aa bbb ccc dddd ee TT` (two-byte prefix and tail)
    ///   offsets `2 4 7 10 14 16` then the stray bytes `EE FF`
    /// so the prefix, the tail and the bytes past the last whole offsets cell
    /// all have to pass through.
    fn source() -> (Vec<u8>, Vec<u8>) {
        let mut offsets = column(&[2, 4, 7, 10, 14, 16]);
        offsets.extend_from_slice(&[0xee, 0xff]);
        (offsets, b"PPaabbbcccddddeeTT".to_vec())
    }

    struct Case {
        name: &'static str,
        projections: Vec<(u32, &'static [u8])>,
        /// Expected rebuilt offsets cells, before the two stray bytes.
        offsets: [u32; 6],
        values: &'static [u8],
    }

    #[test]
    fn rebuild_projects_cells_between_untouched_neighbours_prefix_tail_and_stray_bytes() {
        let cases = [
            Case {
                name: "first only, longer by one",
                projections: vec![at(0, b"XYZ")],
                offsets: [2, 5, 8, 11, 15, 17],
                values: b"PPXYZbbbcccddddeeTT",
            },
            Case {
                name: "middle only, shorter by two",
                projections: vec![at(1, b"Q")],
                offsets: [2, 4, 5, 8, 12, 14],
                values: b"PPaaQcccddddeeTT",
            },
            Case {
                name: "final only, longer by three",
                projections: vec![at(4, b"LMNOP")],
                offsets: [2, 4, 7, 10, 14, 19],
                values: b"PPaabbbcccddddLMNOPTT",
            },
            Case {
                name: "first, middle and final with differing lengths",
                projections: vec![at(0, b"XYZ"), at(2, b"Q"), at(4, b"LMNOP")],
                offsets: [2, 5, 8, 9, 13, 18],
                values: b"PPXYZbbbQddddLMNOPTT",
            },
            Case {
                name: "growth and shrink with no net change",
                projections: vec![at(1, b"QQQQQQ"), at(3, b"R")],
                offsets: [2, 4, 10, 13, 14, 16],
                values: b"PPaaQQQQQQcccReeTT",
            },
            Case {
                name: "every element",
                projections: vec![
                    at(0, b"A"),
                    at(1, b"BB"),
                    at(2, b"C"),
                    at(3, b"DD"),
                    at(4, b"E"),
                ],
                offsets: [2, 3, 5, 6, 8, 9],
                values: b"PPABBCDDETT",
            },
        ];
        let (offsets, values) = source();
        let (offsets_before, values_before) = (offsets.clone(), values.clone());
        for case in cases {
            let (out_offsets, out_values) = rebuilt(&offsets, &values, &case.projections)
                .unwrap_or_else(|status| panic!("{}: {status:?}", case.name));
            let mut expected_offsets = column(&case.offsets);
            expected_offsets.extend_from_slice(&[0xee, 0xff]);
            assert_eq!(out_offsets, expected_offsets, "{}: offsets", case.name);
            assert_eq!(out_values, case.values, "{}: values", case.name);
            // Selected elements read back as their certified bytes, every
            // other element as its original bytes.
            for element in 0..5usize {
                let projected = case
                    .projections
                    .iter()
                    .find(|(index, _)| usize::try_from(*index).unwrap() == element);
                let expected = match projected {
                    Some((_, certified)) => *certified,
                    None => element_bytes(&offsets, &values, element),
                };
                assert_eq!(
                    element_bytes(&out_offsets, &out_values, element),
                    expected,
                    "{}: element {element}",
                    case.name
                );
            }
            assert_eq!(offsets, offsets_before, "{}: source offsets", case.name);
            assert_eq!(values, values_before, "{}: source values", case.name);
        }
    }

    #[test]
    fn rebuild_without_prefix_tail_or_stray_bytes() {
        // offsets `0 1 3`, values `a bc`: the smallest whole CSR.
        let offsets = column(&[0, 1, 3]);
        let (out_offsets, out_values) = rebuilt(&offsets, b"abc", &[at(0, b"XX")]).unwrap();
        assert_eq!(out_offsets, column(&[0, 2, 4]));
        assert_eq!(out_values, b"XXbc");
    }

    #[test]
    fn rebuild_refuses_malformed_offsets_by_name_before_and_after_a_projected_cell() {
        // Before: element 1's cell is backward (4 then 2), met while copying
        // the elements ahead of projected element 2.
        assert_eq!(
            rebuilt(&column(&[0, 4, 2, 6]), b"abcdef", &[at(2, b"XY")]).err(),
            Some(ErrorCode::InvalidCellValue)
        );
        // Before: element 0 claims bytes past the values column.
        assert_eq!(
            rebuilt(&column(&[0, 50, 52]), b"abcdef", &[at(1, b"XY")]).err(),
            Some(ErrorCode::ColumnUnderrun)
        );
        // After: element 2 is backward (4 then 3) behind projected element 0.
        assert_eq!(
            rebuilt(&column(&[0, 2, 4, 3]), b"abcd", &[at(0, b"XY")]).err(),
            Some(ErrorCode::InvalidCellValue)
        );
        // After: element 2 claims bytes past the values column.
        assert_eq!(
            rebuilt(&column(&[0, 2, 4, 50]), b"abcd", &[at(0, b"XY")]).err(),
            Some(ErrorCode::ColumnUnderrun)
        );
        // The projected element's own closing cell is absent.
        assert_eq!(
            rebuilt(&column(&[0, 2]), b"ab", &[at(1, b"XY")]).err(),
            Some(ErrorCode::ColumnUnderrun)
        );
        // Projections must ascend: element 1 lies behind the cursor element 2
        // already consumed.
        let (offsets, values) = source();
        assert_eq!(
            rebuilt(&offsets, &values, &[at(2, b"XY"), at(1, b"XY")]).err(),
            Some(ErrorCode::InvalidCellValue)
        );
        // An offsets column with no whole cell has no closing offset to write.
        let (mut out_offsets, mut out_values) = (Vec::new(), Vec::new());
        assert_eq!(
            Rebuild::new(&mut out_offsets, &mut out_values).finish(&[0xaa, 0xbb], b""),
            Err(ErrorCode::InvalidCellValue)
        );
    }

    // The plan tests compile real reduce-section bytes. The VO pair of every
    // program is slot 0 field 0, offsets column 1, values column 2, keyed by
    // column 3; column 0 is a FOR_EACH type column and 4, 5 are FLAT_MAP
    // offsets columns.

    const INIT: [u8; 9] = [0x18, 0, 6, 4, 0, 2, 10, 10, 0];

    fn link(field_idx: u8, root: &str) -> VoFieldLink {
        VoFieldLink {
            slot: 0,
            field_idx,
            schema: VoMetadata {
                root: root.into(),
                frame: vec![1],
                rows: vec![vec![1]],
                profile: VoProfile::default(),
                retained: vec![],
            },
        }
    }

    fn one() -> Vec<VoFieldLink> {
        vec![link(0, "t.A")]
    }

    fn compile(code: &[u8], fields: Vec<VoFieldLink>) -> Result<InputPlan, ProgramAdmissionError> {
        let metadata = VoProgramMetadata {
            fields,
            counts: vec![],
        };
        InputPlan::compile(&INIT, code, &metadata)
    }

    /// `BATCH_STRUCT_MAP_UPSERT_LAST`: slot, key, scalar `(col, field)` pairs,
    /// then CSR `(offsets, values, field)` arrays.
    fn upsert(slot: u8, key_col: u8, vals: &[(u8, u8)], arrays: &[(u8, u8, u8)]) -> Vec<u8> {
        let mut code = vec![0x80, slot, key_col, u8::try_from(vals.len()).unwrap()];
        for &(col, field) in vals {
            code.extend_from_slice(&[col, field]);
        }
        code.push(u8::try_from(arrays.len()).unwrap());
        for &(offsets_col, values_col, field) in arrays {
            code.extend_from_slice(&[offsets_col, values_col, field]);
        }
        code
    }

    /// The one read of the pair these tests scope.
    fn vo_read() -> Vec<u8> {
        upsert(0, 3, &[], &[(1, 2, 0)])
    }

    fn for_each(type_col: u8, ids: &[u32], body: &[u8]) -> Vec<u8> {
        let mut code = vec![0xe0, type_col, u8::try_from(ids.len()).unwrap()];
        for id in ids {
            code.extend_from_slice(&id.to_le_bytes());
        }
        code.extend_from_slice(&u16::try_from(body.len()).unwrap().to_le_bytes());
        code.extend_from_slice(body);
        code
    }

    /// A FLAT_MAP with no parent timestamp column.
    fn flat_map(offsets_col: u8, body: &[u8]) -> Vec<u8> {
        let mut code = vec![0xe1, offsets_col];
        code.extend_from_slice(encode_index(NO_PARENT_TS_COL).as_bytes());
        code.extend_from_slice(&u16::try_from(body.len()).unwrap().to_le_bytes());
        code.extend_from_slice(body);
        code
    }

    fn filter(ids: &[u32]) -> Option<ForEachFilter> {
        Some(ForEachFilter {
            type_col: 0,
            match_ids: ids.into(),
        })
    }

    #[test]
    fn compile_records_each_pair_with_the_scope_it_is_read_under() {
        let body = vo_read();
        let top = compile(&body, one()).unwrap();
        assert_eq!(top.inputs.len(), 1);
        let input = &top.inputs[0];
        assert_eq!(
            (
                input.slot,
                input.field_idx,
                input.offsets_col,
                input.values_col
            ),
            (0, 0, 1, 2)
        );
        assert_eq!(input.scopes, vec![Scope::default()]);

        let each = compile(&for_each(0, &[1, 3], &body), one()).unwrap();
        assert_eq!(
            each.inputs[0].scopes,
            vec![Scope {
                filter: filter(&[1, 3]),
                flat_maps: vec![]
            }]
        );
        let flat = compile(&for_each(0, &[1], &flat_map(4, &body)), one()).unwrap();
        assert_eq!(
            flat.inputs[0].scopes,
            vec![Scope {
                filter: filter(&[1]),
                flat_maps: vec![4]
            }]
        );
        let nested = compile(&for_each(0, &[1], &flat_map(4, &flat_map(5, &body))), one()).unwrap();
        assert_eq!(
            nested.inputs[0].scopes,
            vec![Scope {
                filter: filter(&[1]),
                flat_maps: vec![4, 5]
            }]
        );

        // One pair read at the top level and twice under the same FOR_EACH is
        // one input with two scopes.
        let union = [
            body.clone(),
            for_each(0, &[1], &body),
            for_each(0, &[1], &body),
        ]
        .concat();
        let plan = compile(&union, one()).unwrap();
        assert_eq!(plan.inputs.len(), 1);
        assert_eq!(
            plan.inputs[0].scopes,
            vec![
                Scope::default(),
                Scope {
                    filter: filter(&[1]),
                    flat_maps: vec![]
                }
            ]
        );

        // An array field with no schema link is an ordinary arena field.
        let unlinked = compile(&upsert(0, 3, &[], &[(1, 2, 5)]), one()).unwrap();
        assert!(unlinked.inputs.is_empty());
    }

    #[test]
    fn compile_refuses_cell_readers_that_share_a_vo_column() {
        let invalid = Some(ProgramAdmissionError::InvalidProgram(
            crate::state_init::ProgramInvalidCause::MalformedProgram,
        ));
        let pair = vo_read();
        let aliases = [
            // Scalar key column named as the offsets or the values column.
            upsert(0, 1, &[], &[(1, 2, 0)]),
            upsert(0, 2, &[], &[(1, 2, 0)]),
            // Scalar value column named as the offsets or the values column.
            upsert(0, 3, &[(1, 1)], &[(1, 2, 0)]),
            upsert(0, 3, &[(2, 1)], &[(1, 2, 0)]),
            // Another instruction, before or after the pair's own read.
            [pair.clone(), upsert(0, 2, &[], &[])].concat(),
            [upsert(0, 1, &[], &[]), pair.clone()].concat(),
            // A FOR_EACH type column or a FLAT_MAP offsets column.
            for_each(1, &[1], &pair),
            for_each(0, &[1], &flat_map(2, &pair)),
        ];
        for (index, code) in aliases.iter().enumerate() {
            assert_eq!(compile(code, one()).err(), invalid, "alias case {index}");
        }
        // The same shapes over columns the pair does not hold compile.
        let control = upsert(0, 3, &[(4, 1)], &[(1, 2, 0)]);
        assert_eq!(compile(&control, one()).unwrap().inputs.len(), 1);
    }

    #[test]
    fn compile_refuses_overlapping_or_conflicting_pairs() {
        let invalid = Some(ProgramAdmissionError::InvalidProgram(
            crate::state_init::ProgramInvalidCause::MalformedProgram,
        ));
        let read =
            |field: u8, offsets: u8, values: u8| upsert(0, 3, &[], &[(offsets, values, field)]);
        let same = vec![link(0, "t.A"), link(1, "t.A")];
        let different = vec![link(0, "t.A"), link(1, "t.B")];
        // Exactly one column shared, in either role, or swapped roles.
        for (second_offsets, second_values) in [(1, 5), (4, 2), (2, 5), (5, 1)] {
            let code = [read(0, 1, 2), read(1, second_offsets, second_values)].concat();
            assert_eq!(
                compile(&code, same.clone()).err(),
                invalid,
                "pair ({second_offsets}, {second_values})"
            );
        }
        // One column serving as both offsets and values.
        assert_eq!(compile(&read(0, 1, 1), same.clone()).err(), invalid);
        // The same pair under two different schemas.
        let conflicting = [read(0, 1, 2), read(1, 1, 2)].concat();
        assert_eq!(compile(&conflicting, different).err(), invalid);
        // The same pair under one schema is one input.
        let plan = compile(&conflicting, same).unwrap();
        assert_eq!(plan.inputs.len(), 1);
        assert_eq!(plan.inputs[0].scopes, vec![Scope::default()]);
    }

    /// The elements the plan's only input marks over `cols`.
    fn marked(plan: &InputPlan, cols: &[&[u8]], batch_len: u32) -> Result<Vec<u64>, ErrorCode> {
        let mut state = InputScratch::default();
        plan.inputs[0].mark(cols, batch_len, &mut state)?;
        Ok(state.visited)
    }

    #[test]
    fn top_level_reads_select_every_batch_row_across_mark_words() {
        let plan = compile(&vo_read(), one()).unwrap();
        let offsets = column(&[0, 2, 2, 4]);
        let cols: [&[u8]; 2] = [&[], &offsets];
        assert_eq!(marked(&plan, &cols, 3), Ok(vec![0b111]));
        // The pair's closing cell must exist too: four rows need five cells.
        assert_eq!(marked(&plan, &cols, 4), Err(ErrorCode::ColumnUnderrun));
        // Seventy rows span two mark words.
        let cells: Vec<u32> = (0..71).collect();
        let offsets = column(&cells);
        let cols: [&[u8]; 2] = [&[], &offsets];
        assert_eq!(marked(&plan, &cols, 70), Ok(vec![u64::MAX, 0b11_1111]));
    }

    #[test]
    fn for_each_never_selects_rows_with_empty_or_missing_pairs() {
        let plan = compile(&for_each(0, &[1, 3], &vo_read()), one()).unwrap();
        let values: &[u8] = b"abcd";
        // Row 1 does not match and carries an empty frame (offsets 2, 2).
        let ty = column(&[3, 2, 1]);
        let offsets = column(&[0, 2, 2, 4]);
        let cols: [&[u8]; 3] = [&ty, &offsets, values];
        assert_eq!(marked(&plan, &cols, 3), Ok(vec![0b101]));
        // Row 2 does not match and its pair is missing: three offsets cells
        // cover rows 0 and 1 only.
        let ty = column(&[1, 3, 2]);
        let offsets = column(&[0, 2, 4]);
        let cols: [&[u8]; 3] = [&ty, &offsets, values];
        assert_eq!(marked(&plan, &cols, 3), Ok(vec![0b011]));
        // The same columns refuse when row 2 does match.
        let ty = column(&[1, 3, 1]);
        let cols: [&[u8]; 3] = [&ty, &offsets, values];
        assert_eq!(marked(&plan, &cols, 3), Err(ErrorCode::ColumnUnderrun));
        // No row matches and the pair's columns are absent altogether.
        let ty = column(&[2, 2, 2]);
        let cols: [&[u8]; 1] = [&ty];
        assert_eq!(marked(&plan, &cols, 3), Ok(Vec::new()));
        // The type column itself must cover the batch.
        let ty = column(&[2, 2]);
        let cols: [&[u8]; 1] = [&ty];
        assert_eq!(marked(&plan, &cols, 3), Err(ErrorCode::ColumnUnderrun));
    }

    #[test]
    fn flat_map_selects_the_actual_child_elements() {
        let plan = compile(&for_each(0, &[1], &flat_map(4, &vo_read())), one()).unwrap();
        let vo_offsets = column(&[0, 1, 2, 3, 4, 5, 6]);
        let columns = |ty: &[u8], flat: &[u8], vo: &[u8]| -> Result<Vec<u64>, ErrorCode> {
            let cols: [&[u8]; 5] = [ty, vo, &[], &[], flat];
            marked(&plan, &cols, 3)
        };
        // Rows 0 and 2 match. Row 0 expands to children 1 and 2, row 2 to 4
        // and 5; row 1 would have expanded to 3. Elements 0 (the row's own
        // index) and 3 are never selected.
        assert_eq!(
            columns(&column(&[1, 2, 1]), &column(&[1, 3, 4, 6]), &vo_offsets),
            Ok(vec![0b11_0110])
        );
        let all = column(&[1, 1, 1]);
        // A child past the pair's last element.
        assert_eq!(
            columns(&all, &column(&[5, 7, 7, 7]), &vo_offsets),
            Err(ErrorCode::ColumnUnderrun)
        );
        // A backward row range (3 then 2).
        assert_eq!(
            columns(&all, &column(&[1, 3, 2, 6]), &vo_offsets),
            Err(ErrorCode::InvalidCellValue)
        );
        // A range behind one already visited, across an unselected row.
        assert_eq!(
            columns(&column(&[1, 2, 1]), &column(&[0, 3, 2, 4]), &vo_offsets),
            Err(ErrorCode::InvalidCellValue)
        );
        // The expansion column must cover the batch.
        assert_eq!(
            columns(&all, &column(&[0, 2, 4]), &vo_offsets),
            Err(ErrorCode::ColumnUnderrun)
        );
    }

    #[test]
    fn nested_flat_maps_expand_children_of_children() {
        let plan = compile(
            &for_each(0, &[1], &flat_map(4, &flat_map(5, &vo_read()))),
            one(),
        )
        .unwrap();
        let ty = column(&[1, 1]);
        // Row 0 owns children 0 and 1, row 1 owns child 2.
        let outer = column(&[0, 2, 3]);
        // Child 0 owns elements 1 and 2, child 1 none, child 2 elements 3, 4.
        let inner = column(&[1, 3, 3, 5]);
        let vo_offsets = column(&[0, 1, 2, 3, 4, 5]);
        let cols: [&[u8]; 6] = [&ty, &vo_offsets, &[], &[], &outer, &inner];
        assert_eq!(marked(&plan, &cols, 2), Ok(vec![0b1_1110]));
    }

    #[test]
    fn zero_selected_rows_never_consult_the_capability() {
        // UnavailableVoAdmission refuses every frame it is shown, so an
        // admitted batch proves no frame reached it.
        let plan = compile(&for_each(0, &[1], &vo_read()), one()).unwrap();
        let metadata = VoProgramMetadata {
            fields: one(),
            counts: vec![],
        };
        let mut scratch = ProgramScratch::default();

        // Admission's own storage that memory cannot back refuses the batch
        // by name before it runs, never aborting.
        let ty = column(&[1, 2, 2]);
        let offsets = column(&[0, 2]);
        let cols: [&[u8]; 3] = [&ty, &offsets, b"ab"];
        let refused = crate::ttl_plan::tests::without_memory(|| {
            plan.admit_and_execute(
                &metadata,
                &UnavailableVoAdmission,
                &cols,
                3,
                &mut ProgramScratch::default(),
                |_| unreachable!("refused before execution"),
            )
        });
        assert_eq!(
            refused,
            Err(InputAdmissionError::Physical(ErrorCode::OutOfMemory))
        );

        // No row matches, and the VO pair's columns are absent altogether.
        let ty = column(&[2, 2, 2]);
        let cols: [&[u8]; 1] = [&ty];
        let mut ran = false;
        let result = plan.admit_and_execute(
            &metadata,
            &UnavailableVoAdmission,
            &cols,
            3,
            &mut scratch,
            |columns| {
                ran = true;
                assert!(core::ptr::eq(columns, &cols[..]), "own columns lent");
                42
            },
        );
        assert_eq!(result, Ok(42));
        assert!(ran);

        // A matching row whose pair is missing refuses physically, unrun.
        let ty = column(&[1, 2, 2]);
        let cols: [&[u8]; 1] = [&ty];
        let mut ran = false;
        let result = plan.admit_and_execute(
            &metadata,
            &UnavailableVoAdmission,
            &cols,
            3,
            &mut scratch,
            |_| {
                ran = true;
                0
            },
        );
        assert_eq!(
            result,
            Err(InputAdmissionError::Physical(ErrorCode::ColumnUnderrun))
        );
        assert!(!ran);

        // A matching row whose pair is present reaches the capability, which
        // refuses it by name: the capability really is consulted for it.
        let offsets = column(&[0, 2]);
        let cols: [&[u8]; 3] = [&ty, &offsets, b"ab"];
        let mut ran = false;
        let result = plan.admit_and_execute(
            &metadata,
            &UnavailableVoAdmission,
            &cols,
            3,
            &mut scratch,
            |_| {
                ran = true;
                0
            },
        );
        assert_eq!(
            result,
            Err(InputAdmissionError::Frame {
                slot: 0,
                field_idx: 0,
                row: 0,
                cause: VoRefusal::Admission(VoAdmissionError::Unavailable),
            })
        );
        assert!(!ran);
    }
}
