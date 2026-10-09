//! Native value-object program admission, before any state is initialized.
//!
//! The storage kernel owns physical geometry. A caller supplies the one real
//! value-object capability; this module binds every VO field and compares each
//! counts instruction with its kernel-certified schema-role layout. The
//! admitted program owns its immutable bytecode and metadata. Its execution
//! methods certify selected input frames and reuse reader-layout projections
//! before any VM mutation; primitive byte-arena storage performs no decoding.
//!
//! [`declares_vo`] is the capability-free question an owner asks first.

use columine_types::operand::Operands;
use columine_types::types::{ErrorCode, Opcode, ProgramHeader, SlotTypeFlags, StructFieldType};
use columine_types::vo::{
    VoCountsProof, VoPreparationError, VoProgramAdmission, VoProgramMetadata, VoProgramRefusal,
    certify_counts, prepare_each,
};

use crate::counts_fold::CountsFoldOp;
use crate::element_body::{InvalidSource, SourceOp, visit_source_ops};
use crate::program_inputs::InputPlan;
use crate::row_exprs::{LiveColumns, RowColumns, RowExpression, bind_row_columns, row_table};
use crate::state_init;
use crate::vm::Vm;

pub use crate::program_inputs::{InputAdmissionError, ProgramScratch};

/// A refusal at the program boundary, not a partially initialized program.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProgramAdmissionError {
    InvalidProgram(state_init::ProgramInvalidCause),
    DuplicateField {
        slot: u32,
        field_idx: u8,
    },
    MissingSchema {
        slot: u32,
        field_idx: u8,
    },
    UnexpectedSchema {
        slot: u32,
        field_idx: u8,
    },
    DuplicateRole {
        instruction_offset: u32,
    },
    MissingRole {
        instruction_offset: u32,
    },
    UnexpectedRole {
        instruction_offset: u32,
    },
    LayoutMismatch {
        instruction_offset: u32,
    },
    Preparation(VoPreparationError),
    Counts {
        instruction_offset: u32,
        cause: VoProgramRefusal,
    },
}

/// Bytecode and schema declarations certified together by a native capability.
/// There is no public unchecked constructor and no positive boolean receipt.
/// The registry owns this value: its immutable bytecode and metadata move into
/// it, without a self-reference or a second copy of either declaration.
pub struct AdmittedProgram<A: VoProgramAdmission> {
    bytecode: Box<[u8]>,
    metadata: VoProgramMetadata,
    admission: A,
    inputs: InputPlan,
    /// Whether the bytecode carries a row-expression table, which only
    /// [`Self::execute_bound`] binds.
    binds_rows: bool,
    magic: u32,
    state_size: u32,
}

impl<A: VoProgramAdmission> AdmittedProgram<A> {
    pub fn load(
        bytecode: Box<[u8]>,
        metadata: VoProgramMetadata,
        mut admission: A,
        accepted_magics: &[u32],
    ) -> Result<Self, ProgramAdmissionError> {
        let sections =
            ProgramHeader::sections(&bytecode).ok_or(ProgramAdmissionError::InvalidProgram(
                state_init::ProgramInvalidCause::MalformedProgram,
            ))?;
        let state_size = state_init::calculate_state_size(&bytecode, accepted_magics)
            .map_err(ProgramAdmissionError::InvalidProgram)?;
        let magic = sections.header.magic;
        let binds_rows = row_table(&bytecode)
            .map_err(|_| {
                ProgramAdmissionError::InvalidProgram(
                    state_init::ProgramInvalidCause::MalformedProgram,
                )
            })?
            .is_some();
        for (index, link) in metadata.fields.iter().enumerate() {
            if metadata.fields[..index]
                .iter()
                .any(|prior| prior.slot == link.slot && prior.field_idx == link.field_idx)
            {
                return Err(ProgramAdmissionError::DuplicateField {
                    slot: link.slot,
                    field_idx: link.field_idx,
                });
            }
            if declared_field(sections.init_code, link.slot, link.field_idx)?
                != Some(StructFieldType::Vo)
            {
                return Err(ProgramAdmissionError::UnexpectedSchema {
                    slot: link.slot,
                    field_idx: link.field_idx,
                });
            }
        }
        for (index, role) in metadata.counts.iter().enumerate() {
            if metadata.counts[..index]
                .iter()
                .any(|prior| prior.instruction_offset == role.instruction_offset)
            {
                return Err(ProgramAdmissionError::DuplicateRole {
                    instruction_offset: role.instruction_offset,
                });
            }
        }
        visit_fields(sections.init_code, |slot, field_idx, kind| {
            if kind == StructFieldType::Vo && metadata.schema_for(slot, field_idx).is_none() {
                Err(ProgramAdmissionError::MissingSchema { slot, field_idx })
            } else {
                Ok(())
            }
        })?;
        let inputs = InputPlan::compile(sections.init_code, sections.reduce_code, &metadata)?;
        prepare_each(&mut admission, metadata.schemas())
            .map_err(ProgramAdmissionError::Preparation)?;

        let reduce_base = sections.reduce_code.as_ptr() as usize - bytecode.as_ptr() as usize;
        let mut found_roles = Vec::with_capacity(metadata.counts.len());
        visit_counts(sections.reduce_code, reduce_base, &mut |offset, op| {
            if declared_field(sections.init_code, op.slot, op.field_idx)?
                != Some(StructFieldType::Vo)
            {
                return Err(ProgramAdmissionError::InvalidProgram(
                    state_init::ProgramInvalidCause::MalformedProgram,
                ));
            }
            let role = metadata
                .counts
                .iter()
                .find(|role| role.instruction_offset == offset)
                .ok_or(ProgramAdmissionError::MissingRole {
                    instruction_offset: offset,
                })?;
            let schema = metadata.schema_for(op.slot, op.field_idx).ok_or(
                ProgramAdmissionError::MissingSchema {
                    slot: op.slot,
                    field_idx: op.field_idx,
                },
            )?;
            let proof = certify_counts(&admission, schema, role).map_err(|cause| {
                ProgramAdmissionError::Counts {
                    instruction_offset: offset,
                    cause,
                }
            })?;
            if !op.matches_layout(proof.layout()) {
                return Err(ProgramAdmissionError::LayoutMismatch {
                    instruction_offset: offset,
                });
            }
            found_roles.push(offset);
            Ok(())
        })?;
        for role in &metadata.counts {
            if !found_roles.contains(&role.instruction_offset) {
                return Err(ProgramAdmissionError::UnexpectedRole {
                    instruction_offset: role.instruction_offset,
                });
            }
        }
        Ok(Self {
            bytecode,
            metadata,
            admission,
            inputs,
            binds_rows,
            magic,
            state_size,
        })
    }

    pub fn bytecode(&self) -> &[u8] {
        &self.bytecode
    }

    pub fn metadata(&self) -> &VoProgramMetadata {
        &self.metadata
    }

    pub fn admission(&self) -> &A {
        &self.admission
    }

    pub fn state_size(&self) -> u32 {
        self.state_size
    }

    /// Initialize the exact admitted program, never a separately supplied one.
    pub fn initialize_state(&self, state: &mut [u8]) -> Result<(), ErrorCode> {
        state_init::init_state(state, &self.bytecode, core::slice::from_ref(&self.magic))
    }

    /// Run an embedding dispatch loop over this certificate's own bytecode
    /// and certified columns: admission completes before the callback may
    /// mutate state. A program with a row-expression table is refused with
    /// [`InputAdmissionError::UnboundRows`]; it runs through
    /// [`Self::execute_bound`], so admission reads the columns execution reads.
    pub fn execute_with(
        &self,
        cols: &[&[u8]],
        batch_len: u32,
        scratch: &mut ProgramScratch,
        execute: impl FnOnce(&[u8], &[&[u8]]) -> u32,
    ) -> Result<u32, InputAdmissionError> {
        if self.binds_rows {
            return Err(InputAdmissionError::UnboundRows);
        }
        self.inputs.admit_and_execute(
            &self.metadata,
            &self.admission,
            cols,
            batch_len,
            scratch,
            |cols| execute(&self.bytecode, cols),
        )
    }

    /// Bind the row-expression table over `cols`, admit the selected VO frames
    /// against that bound column set, then run `execute` on it with the
    /// batch's live columns. A scope filter, FLAT_MAP offsets or VO column
    /// may be a derived column, so admission must read the same cells the
    /// reduce section will: a host placeholder for a derived column is never
    /// what admission sees. Binding reads `state` and writes only `rows` and
    /// `cols`; every refusal happens before `execute` runs. A program without
    /// a table binds nothing and its live columns are empty.
    #[allow(clippy::too_many_arguments)]
    pub fn execute_bound<'a, E: RowExpression>(
        &'a self,
        state: &mut [u8],
        cols: &mut [&'a [u8]],
        batch_len: u32,
        scratch: &mut ProgramScratch,
        eval: &mut E,
        rows: &'a mut RowColumns,
        execute: impl FnOnce(&[u8], &mut [u8], &[&[u8]], &mut LiveColumns<'_, '_>) -> u32,
    ) -> Result<u32, InputAdmissionError> {
        let mut live = bind_row_columns(&self.bytecode, state, cols, batch_len, eval, rows)
            .map_err(InputAdmissionError::Rows)?;
        self.inputs.admit_and_execute(
            &self.metadata,
            &self.admission,
            cols,
            batch_len,
            scratch,
            |cols| execute(&self.bytecode, state, cols, &mut live),
        )
    }

    /// Re-admit a batch that resumes after its bind: splice the derived
    /// cells `rows` retained from this program's last bind over this batch
    /// into `cols`, admit the selected VO frames over that spliced set, then
    /// run `execute` on it with the batch's live columns. A resume sees the
    /// bound cells the paused run saw, never a fresh bind over the state as
    /// it stands now; a deferred live entry evaluates at its row, as it does
    /// in any run, against the state the resumed rows before it left. `rows`
    /// from another table or another batch length is refused as
    /// `Rows(InvalidProgram)`, before admission or execution.
    ///
    /// Embedder contract for `eval`: it implements the same expression
    /// language as the evaluator that bound `rows`. The table bytes `rows`
    /// is bound to are the expressions; which entries were deferred is the
    /// bind's answer and is never asked again, and the bound cells are
    /// never re-evaluated, so only deferred entries reach `eval`, at their
    /// rows. An evaluator whose answer depends on anything beyond the
    /// expression, the batch's columns and the state at the row is outside
    /// this contract, as it is for any run.
    #[allow(clippy::too_many_arguments)]
    pub fn execute_spliced<'a, E: RowExpression>(
        &'a self,
        state: &mut [u8],
        cols: &mut [&'a [u8]],
        batch_len: u32,
        scratch: &mut ProgramScratch,
        eval: &mut E,
        rows: &'a RowColumns,
        execute: impl FnOnce(&[u8], &mut [u8], &[&[u8]], &mut LiveColumns<'_, '_>) -> u32,
    ) -> Result<u32, InputAdmissionError> {
        let mut live = rows
            .resume(&self.bytecode, batch_len, eval)
            .and_then(|live| rows.splice(cols).map(|()| live))
            .map_err(InputAdmissionError::Rows)?;
        self.inputs.admit_and_execute(
            &self.metadata,
            &self.admission,
            cols,
            batch_len,
            scratch,
            |cols| execute(&self.bytecode, state, cols, &mut live),
        )
    }

    /// Admit selected VO cells and counts templates before the VM touches any
    /// state; their schemas and counts roles were certified at load. Same-layout
    /// cells stay borrowed, while retained-writer projections are written from
    /// their certified reader-layout bytes.
    pub fn execute_batch(
        &self,
        vm: &mut Vm,
        state: &mut [u8],
        cols: &[&[u8]],
        batch_len: u32,
        scratch: &mut ProgramScratch,
    ) -> Result<u32, InputAdmissionError> {
        self.execute_with(cols, batch_len, scratch, |bytecode, cols| {
            vm.execute_batch(state, bytecode, cols, batch_len)
        })
    }

    /// The same admission boundary on the paired undo/redo execution lane.
    pub fn execute_batch_delta(
        &self,
        vm: &mut Vm,
        state: &mut [u8],
        cols: &[&[u8]],
        batch_len: u32,
        scratch: &mut ProgramScratch,
    ) -> Result<u32, InputAdmissionError> {
        self.execute_with(cols, batch_len, scratch, |bytecode, cols| {
            vm.execute_batch_delta(state, bytecode, cols, batch_len)
        })
    }
}

/// Whether `bytecode` declares any value-object struct-map field. It reads
/// only the program header and the closed init grammar that `load` also
/// walks, and decodes no frame or schema. Every init record is walked through
/// its terminating `HALT`, never short-circuited at the first VO field. An
/// unrecognized or truncated init declaration is
/// [`ProgramAdmissionError::InvalidProgram`]; this predicate does not validate
/// reduce instructions. An embedding without a VO capability uses it to
/// refuse raw VO programs before state mutation.
pub fn declares_vo(bytecode: &[u8]) -> Result<bool, ProgramAdmissionError> {
    let sections = ProgramHeader::sections(bytecode).ok_or(
        ProgramAdmissionError::InvalidProgram(state_init::ProgramInvalidCause::MalformedProgram),
    )?;
    let mut declared = false;
    visit_fields(sections.init_code, |_, _, kind| {
        declared |= kind == StructFieldType::Vo;
        Ok(())
    })?;
    Ok(declared)
}

fn visit_fields(
    init: &[u8],
    mut visit: impl FnMut(u32, u8, StructFieldType) -> Result<(), ProgramAdmissionError>,
) -> Result<(), ProgramAdmissionError> {
    let mut r = Operands::at(init, 0);
    while r.pos() < init.len() {
        let op =
            r.byte()
                .and_then(Opcode::from_u8)
                .ok_or(ProgramAdmissionError::InvalidProgram(
                    state_init::ProgramInvalidCause::MalformedProgram,
                ))?;
        if op == Opcode::Halt {
            return if r.pos() == init.len() {
                Ok(())
            } else {
                Err(ProgramAdmissionError::InvalidProgram(
                    state_init::ProgramInvalidCause::MalformedProgram,
                ))
            };
        }
        let slot = r.index().ok_or(ProgramAdmissionError::InvalidProgram(
            state_init::ProgramInvalidCause::MalformedProgram,
        ))?;
        let flags =
            SlotTypeFlags::from_byte(r.byte().ok_or(ProgramAdmissionError::InvalidProgram(
                state_init::ProgramInvalidCause::MalformedProgram,
            ))?);
        r.byte().ok_or(ProgramAdmissionError::InvalidProgram(
            state_init::ProgramInvalidCause::MalformedProgram,
        ))?;
        r.byte().ok_or(ProgramAdmissionError::InvalidProgram(
            state_init::ProgramInvalidCause::MalformedProgram,
        ))?;
        match op {
            Opcode::SlotDef => {
                if flags.has_ttl() {
                    let (_, next) = state_init::TtlParams::read(init, r.pos()).ok_or(
                        ProgramAdmissionError::InvalidProgram(
                            state_init::ProgramInvalidCause::MalformedProgram,
                        ),
                    )?;
                    r = Operands::at(init, next);
                }
            }
            Opcode::SlotArray => {}
            Opcode::SlotStructMap | Opcode::SlotStructMap2 => {
                let count = r.byte().ok_or(ProgramAdmissionError::InvalidProgram(
                    state_init::ProgramInvalidCause::MalformedProgram,
                ))?;
                for field_idx in 0..count {
                    let kind = r.byte().and_then(StructFieldType::from_u8).ok_or(
                        ProgramAdmissionError::InvalidProgram(
                            state_init::ProgramInvalidCause::MalformedProgram,
                        ),
                    )?;
                    visit(slot, field_idx, kind)?;
                }
            }
            Opcode::SlotOrderedList => {
                let kind = r.byte().ok_or(ProgramAdmissionError::InvalidProgram(
                    state_init::ProgramInvalidCause::MalformedProgram,
                ))?;
                if kind == 0xff {
                    let count = r.byte().ok_or(ProgramAdmissionError::InvalidProgram(
                        state_init::ProgramInvalidCause::MalformedProgram,
                    ))?;
                    r.bytes(usize::from(count))
                        .ok_or(ProgramAdmissionError::InvalidProgram(
                            state_init::ProgramInvalidCause::MalformedProgram,
                        ))?;
                }
            }
            Opcode::SlotNested => {
                r.bytes(4).ok_or(ProgramAdmissionError::InvalidProgram(
                    state_init::ProgramInvalidCause::MalformedProgram,
                ))?;
            }
            _ => {
                return Err(ProgramAdmissionError::InvalidProgram(
                    state_init::ProgramInvalidCause::MalformedProgram,
                ));
            }
        }
    }
    Err(ProgramAdmissionError::InvalidProgram(
        state_init::ProgramInvalidCause::MalformedProgram,
    ))
}

/// The kind `slot`'s field `field_idx` declares, if any. An init section that
/// does not decode keeps its own named refusal.
fn declared_field(
    init: &[u8],
    slot: u32,
    field_idx: u8,
) -> Result<Option<StructFieldType>, ProgramAdmissionError> {
    let mut found = None;
    visit_fields(init, |declared_slot, declared_field, kind| {
        if declared_slot == slot && declared_field == field_idx {
            found = Some(kind);
        }
        Ok(())
    })?;
    Ok(found)
}

fn visit_counts<'p>(
    code: &'p [u8],
    base: usize,
    visit: &mut impl FnMut(u32, &CountsFoldOp<'p>) -> Result<(), ProgramAdmissionError>,
) -> Result<(), ProgramAdmissionError> {
    let mut counts = |source: SourceOp<'p>| -> Result<(), ProgramAdmissionError> {
        if source.op != Opcode::BatchStructMapCountsFold {
            return Ok(());
        }
        let op = CountsFoldOp::decode(source.code, source.operands).map_err(|_| {
            ProgramAdmissionError::InvalidProgram(state_init::ProgramInvalidCause::MalformedProgram)
        })?;
        let offset = u32::try_from(source.offset).map_err(|_| {
            ProgramAdmissionError::InvalidProgram(state_init::ProgramInvalidCause::MalformedProgram)
        })?;
        visit(offset, &op)
    };
    visit_source_ops(code, base, &mut counts)
}

impl From<InvalidSource> for ProgramAdmissionError {
    fn from(_: InvalidSource) -> Self {
        Self::InvalidProgram(state_init::ProgramInvalidCause::MalformedProgram)
    }
}

#[cfg(test)]
mod tests {
    use columine_types::PROGRAM_MAGIC;
    use columine_types::types::ErrorCode;
    use columine_types::vo::{
        UnavailableVoAdmission, VoAdmissionError, VoFieldLink, VoMetadata, VoProfile,
        VoProgramMetadata, VoRefusal,
    };

    use super::{AdmittedProgram, InputAdmissionError, ProgramScratch};
    use crate::program_inputs::InputPlan;
    use crate::row_exprs::{
        BatchView, Binding, LiveColumns, ROW_EXPRESSIONS_MAGIC, RowColumns, RowExpression,
    };

    /// The derived column the FOR_EACH reads its type from.
    const DERIVED: u8 = 4;
    /// A live derived column no instruction reads plainly.
    const LIVE: u8 = 5;
    /// The event type whose rows the table derives `FOR_EACH_TYPE` for.
    const EVENT: u32 = 1;
    /// The type the FOR_EACH selects; only the derived column carries it.
    const FOR_EACH_TYPE: u8 = 7;

    /// `[k]` and `[k, 1]`: the constant `k` for every matched row, bound up
    /// front. `[k, 0]`: `k` plus the state's first byte, a live read
    /// evaluated at the row against the state as it then stands.
    struct Constant;

    impl RowExpression for Constant {
        fn admit(&mut self, expr: &[u8], _: &BatchView<'_>) -> Result<Binding, ErrorCode> {
            match expr {
                [_] | [_, 1] => Ok(Binding::Bound),
                [_, 0] => Ok(Binding::Live),
                _ => Err(ErrorCode::InvalidProgram),
            }
        }

        fn eval(&mut self, expr: &[u8], batch: &BatchView<'_>, _: u32) -> Result<u32, ErrorCode> {
            let state = batch.state.first().copied().unwrap_or(0);
            Ok(u32::from(expr[0])
                + if expr == [expr[0], 0] {
                    u32::from(state)
                } else {
                    0
                })
        }
    }

    /// A StructMap slot with one VO field, read under a FOR_EACH whose type
    /// column is derived by the program's row table from column 0:
    /// `FOR_EACH(col 4 == 7) { upsert(key col 3, VO pair cols 1/2) }`. Each
    /// table entry is `(target, event type, expression)`.
    fn derived_scope_program(
        entries: &[(u8, u32, &[u8])],
        pad: usize,
    ) -> (Box<[u8]>, Vec<u8>, Vec<u8>) {
        let init = vec![0x18, 0, 6, 4, 0, 1, 10, 0];
        let body = [0x80, 0, 3, 0, 1, 1, 2, 0];
        let mut reduce = vec![0xe0, DERIVED, 1];
        reduce.extend(u32::from(FOR_EACH_TYPE).to_le_bytes());
        reduce.extend(u16::try_from(body.len()).unwrap().to_le_bytes());
        reduce.extend(body);
        reduce.push(0);
        // Bytes past the reduce section's HALT move the table, not its bytes.
        reduce.resize(reduce.len() + pad, 0);
        let mut table = ROW_EXPRESSIONS_MAGIC.to_le_bytes().to_vec();
        table.extend(u16::try_from(entries.len()).unwrap().to_le_bytes());
        table.push(0);
        for &(target, event, expr) in entries {
            table.extend([target, 1]);
            table.extend(event.to_le_bytes());
            table.extend(u16::try_from(expr.len()).unwrap().to_le_bytes());
            table.extend(expr);
        }
        let mut bytes = vec![0u8; 32];
        bytes.extend(PROGRAM_MAGIC.to_le_bytes());
        bytes.extend([2, 0, 0, 0]);
        bytes.extend(1u32.to_le_bytes());
        bytes.extend(6u32.to_le_bytes());
        bytes.extend(u32::try_from(init.len()).unwrap().to_le_bytes());
        bytes.extend(u32::try_from(reduce.len()).unwrap().to_le_bytes());
        bytes.extend(&init);
        bytes.extend(&reduce);
        bytes.extend(table);
        (bytes.into_boxed_slice(), init, reduce)
    }

    /// The program admitted with a capability that refuses every frame it is
    /// shown, so a refusal names exactly the frames admission selected. Built
    /// directly: `load` would refuse the capability at preparation.
    fn admitted_with(entries: &[(u8, u32, &[u8])]) -> AdmittedProgram<UnavailableVoAdmission> {
        admitted_padded(entries, 0)
    }

    fn admitted_padded(
        entries: &[(u8, u32, &[u8])],
        pad: usize,
    ) -> AdmittedProgram<UnavailableVoAdmission> {
        let (bytecode, init, reduce) = derived_scope_program(entries, pad);
        let metadata = VoProgramMetadata {
            fields: vec![VoFieldLink {
                slot: 0,
                field_idx: 0,
                schema: VoMetadata {
                    root: "example.Counter".into(),
                    frame: vec![1],
                    rows: vec![vec![1]],
                    profile: VoProfile::default(),
                    retained: vec![],
                },
            }],
            counts: vec![],
        };
        let inputs = InputPlan::compile(&init, &reduce, &metadata).expect("plan compiles");
        AdmittedProgram {
            bytecode,
            metadata,
            admission: UnavailableVoAdmission,
            inputs,
            binds_rows: true,
            magic: PROGRAM_MAGIC,
            state_size: 0,
        }
    }

    /// The program whose table derives only the FOR_EACH type, from `expr`.
    fn admitted(expr: &[u8]) -> AdmittedProgram<UnavailableVoAdmission> {
        admitted_with(&[(DERIVED, EVENT, expr)])
    }

    /// The same program with a live entry on `LIVE` for rows of the type the
    /// FOR_EACH does not select; `first` is the first entry's expression.
    fn admitted_live(first: &[u8]) -> AdmittedProgram<UnavailableVoAdmission> {
        admitted_with(&[(DERIVED, EVENT, first), (LIVE, EVENT + 1, &[3, 0])])
    }

    /// Refused before admission: runs nothing.
    fn never(_: &[u8], _: &mut [u8], _: &[&[u8]], _: &mut LiveColumns<'_, '_>) -> u32 {
        unreachable!("refused before admission")
    }

    fn cells(values: &[u32]) -> Vec<u8> {
        values
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect()
    }

    /// Admission reads the derived type column as bound, not the host's
    /// empty placeholder: a row of the derived event type has its frame
    /// shown to the capability, and a row of another type selects nothing
    /// and runs with the bound zero cell.
    #[test]
    fn admission_reads_the_bound_derived_scope_column() {
        let program = admitted(&[FOR_EACH_TYPE]);
        let offsets = cells(&[0, 2]);
        let key = cells(&[5]);
        let mut scratch = ProgramScratch::default();
        let mut rows = RowColumns::new();

        let types = cells(&[EVENT]);
        let mut cols: [&[u8]; 6] = [&types, &offsets, b"ab", &key, &[], &[]];
        let mut ran = false;
        let result = program.execute_bound(
            &mut [],
            &mut cols,
            1,
            &mut scratch,
            &mut Constant,
            &mut rows,
            |_, _, _, _| {
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
            }),
            "the derived type selects row 0 for admission"
        );
        assert!(!ran);

        let types = cells(&[EVENT + 1]);
        let mut rows = RowColumns::new();
        let mut cols: [&[u8]; 6] = [&types, &offsets, b"ab", &key, &[], &[]];
        let result = program.execute_bound(
            &mut [],
            &mut cols,
            1,
            &mut scratch,
            &mut Constant,
            &mut rows,
            |bytecode, _, cols, live| {
                assert!(core::ptr::eq(bytecode, program.bytecode()));
                assert!(live.is_empty());
                assert_eq!(cols[usize::from(DERIVED)], cells(&[0]).as_slice());
                42
            },
        );
        assert_eq!(result, Ok(42), "an unmatched row selects no frame");
    }

    /// A program with a row table is never run over unbound columns, and a
    /// table the evaluator refuses is refused before admission or execution.
    #[test]
    fn row_tables_run_only_through_the_bound_path() {
        let program = admitted(&[FOR_EACH_TYPE]);
        let types = cells(&[EVENT]);
        let offsets = cells(&[0, 2]);
        let key = cells(&[5]);
        let cols: [&[u8]; 6] = [&types, &offsets, b"ab", &key, &[], &[]];
        let mut scratch = ProgramScratch::default();
        assert_eq!(
            program.execute_with(&cols, 1, &mut scratch, |_, _| unreachable!("never run")),
            Err(InputAdmissionError::UnboundRows)
        );

        let refused = admitted(&[]);
        let mut cols: [&[u8]; 6] = [&types, &offsets, b"ab", &key, &[], &[]];
        let mut rows = RowColumns::new();
        assert_eq!(
            refused.execute_bound(
                &mut [],
                &mut cols,
                1,
                &mut scratch,
                &mut Constant,
                &mut rows,
                |_, _, _, _| unreachable!("never run"),
            ),
            Err(InputAdmissionError::Rows(ErrorCode::InvalidProgram))
        );
    }

    /// A resume re-admits over the derived cells its bind retained, not the
    /// host's placeholder: the frame the bind's derived type selected is
    /// shown to the capability again, and an unselected row runs on the
    /// retained zero cell.
    #[test]
    fn a_resume_admits_over_the_retained_derived_cells() {
        let program = admitted(&[FOR_EACH_TYPE]);
        let offsets = cells(&[0, 2]);
        let key = cells(&[5]);
        let mut scratch = ProgramScratch::default();
        let frame_refusal = Err(InputAdmissionError::Frame {
            slot: 0,
            field_idx: 0,
            row: 0,
            cause: VoRefusal::Admission(VoAdmissionError::Unavailable),
        });

        let types = cells(&[EVENT]);
        let mut rows = RowColumns::new();
        let mut cols: [&[u8]; 6] = [&types, &offsets, b"ab", &key, &[], &[]];
        let bound = program.execute_bound(
            &mut [],
            &mut cols,
            1,
            &mut scratch,
            &mut Constant,
            &mut rows,
            |_, _, _, _| unreachable!("refused at admission"),
        );
        assert_eq!(bound, frame_refusal);
        let mut resumed: [&[u8]; 6] = [&types, &offsets, b"ab", &key, &[], &[]];
        assert_eq!(
            program.execute_spliced(
                &mut [],
                &mut resumed,
                1,
                &mut scratch,
                &mut Constant,
                &rows,
                never,
            ),
            frame_refusal,
            "the resume admits the frame its bind selected"
        );

        let other = cells(&[EVENT + 1]);
        let mut rows = RowColumns::new();
        let mut cols: [&[u8]; 6] = [&other, &offsets, b"ab", &key, &[], &[]];
        assert_eq!(
            program.execute_bound(
                &mut [],
                &mut cols,
                1,
                &mut scratch,
                &mut Constant,
                &mut rows,
                |_, _, _, _| 1,
            ),
            Ok(1)
        );
        let mut resumed: [&[u8]; 6] = [&other, &offsets, b"ab", &key, &[], &[]];
        assert_eq!(
            program.execute_spliced(
                &mut [],
                &mut resumed,
                1,
                &mut scratch,
                &mut Constant,
                &rows,
                |_, _, cols, _| {
                    assert_eq!(cols[usize::from(DERIVED)], cells(&[0]).as_slice());
                    2
                },
            ),
            Ok(2)
        );

        // A deferred live entry evaluates again on resume, at its row and
        // against the state the resumed run hands it, never as its zero
        // placeholder.
        let program = admitted_live(&[FOR_EACH_TYPE]);
        let mut rows = RowColumns::new();
        let mut cols: [&[u8]; 6] = [&other, &offsets, b"ab", &key, &[], &[]];
        assert_eq!(
            program.execute_bound(
                &mut [2],
                &mut cols,
                1,
                &mut scratch,
                &mut Constant,
                &mut rows,
                |_, state, cols, live| {
                    assert_eq!(cols[usize::from(LIVE)], cells(&[0]).as_slice());
                    assert_eq!(live.cell(u32::from(LIVE), 0, state, cols), Some(Ok(5)));
                    3
                },
            ),
            Ok(3)
        );
        let mut resumed: [&[u8]; 6] = [&other, &offsets, b"ab", &key, &[], &[]];
        assert_eq!(
            program.execute_spliced(
                &mut [9],
                &mut resumed,
                1,
                &mut scratch,
                &mut Constant,
                &rows,
                |_, state, cols, live| {
                    assert!(!live.is_empty(), "the resume carries the deferred entry");
                    assert_eq!(live.cell(u32::from(LIVE), 0, state, cols), Some(Ok(12)));
                    4
                },
            ),
            Ok(4)
        );
    }

    /// Cells that are not this program's completed bind over this batch
    /// length are refused before admission: never bound, bound over another
    /// length, or left by a failed bind.
    #[test]
    fn a_resume_refuses_cells_that_are_not_its_bind() {
        let program = admitted(&[FOR_EACH_TYPE]);
        let types = cells(&[EVENT + 1]);
        let offsets = cells(&[0, 2]);
        let key = cells(&[5]);
        let mut scratch = ProgramScratch::default();
        let refused = Err(InputAdmissionError::Rows(ErrorCode::InvalidProgram));

        let fresh = RowColumns::new();
        let mut cols: [&[u8]; 6] = [&types, &offsets, b"ab", &key, &[], &[]];
        assert_eq!(
            program.execute_spliced(
                &mut [],
                &mut cols,
                1,
                &mut scratch,
                &mut Constant,
                &fresh,
                never
            ),
            refused,
            "never bound"
        );

        let mut rows = RowColumns::new();
        let mut cols: [&[u8]; 6] = [&types, &offsets, b"ab", &key, &[], &[]];
        assert_eq!(
            program.execute_bound(
                &mut [],
                &mut cols,
                1,
                &mut scratch,
                &mut Constant,
                &mut rows,
                |_, _, _, _| 0,
            ),
            Ok(0)
        );
        let two = cells(&[EVENT + 1, EVENT + 1]);
        let mut cols: [&[u8]; 6] = [&two, &offsets, b"ab", &key, &[], &[]];
        assert_eq!(
            program.execute_spliced(
                &mut [],
                &mut cols,
                2,
                &mut scratch,
                &mut Constant,
                &rows,
                never
            ),
            refused,
            "bound over another batch length"
        );

        let failed = admitted(&[]);
        let mut cols: [&[u8]; 6] = [&types, &offsets, b"ab", &key, &[], &[]];
        assert_eq!(
            failed.execute_bound(
                &mut [],
                &mut cols,
                1,
                &mut scratch,
                &mut Constant,
                &mut rows,
                |_, _, _, _| unreachable!("bind refused"),
            ),
            Err(InputAdmissionError::Rows(ErrorCode::InvalidProgram))
        );
        let mut cols: [&[u8]; 6] = [&types, &offsets, b"ab", &key, &[], &[]];
        assert_eq!(
            program.execute_spliced(
                &mut [],
                &mut cols,
                1,
                &mut scratch,
                &mut Constant,
                &rows,
                never
            ),
            refused,
            "left by a failed bind"
        );

        // A live entry recorded at another program range than this table
        // holds it: a live read would index this program's bytes wrongly.
        let live = admitted_live(&[FOR_EACH_TYPE]);
        let mut rows = RowColumns::new();
        let mut cols: [&[u8]; 6] = [&types, &offsets, b"ab", &key, &[], &[]];
        assert_eq!(
            live.execute_bound(
                &mut [],
                &mut cols,
                1,
                &mut scratch,
                &mut Constant,
                &mut rows,
                |_, _, _, _| 0,
            ),
            Ok(0)
        );
        let shifted = admitted_live(&[FOR_EACH_TYPE, 0, 0]);
        let mut cols: [&[u8]; 6] = [&types, &offsets, b"ab", &key, &[], &[]];
        assert_eq!(
            shifted.execute_spliced(
                &mut [],
                &mut cols,
                1,
                &mut scratch,
                &mut Constant,
                &rows,
                never
            ),
            refused,
            "a live entry at another range"
        );

        // A program of the same table shape whose entry on `LIVE` binds up
        // front: its bind recorded no live entry, so resuming the live
        // program over it would read the bound cell where a live read
        // belongs.
        let bound_split = admitted_with(&[
            (DERIVED, EVENT, &[FOR_EACH_TYPE]),
            (LIVE, EVENT + 1, &[3, 1]),
        ]);
        let mut rows = RowColumns::new();
        let mut cols: [&[u8]; 6] = [&types, &offsets, b"ab", &key, &[], &[]];
        assert_eq!(
            bound_split.execute_bound(
                &mut [],
                &mut cols,
                1,
                &mut scratch,
                &mut Constant,
                &mut rows,
                |_, _, _, _| 0,
            ),
            Ok(0)
        );
        let mut cols: [&[u8]; 6] = [&types, &offsets, b"ab", &key, &[], &[]];
        assert_eq!(
            live.execute_spliced(
                &mut [],
                &mut cols,
                1,
                &mut scratch,
                &mut Constant,
                &rows,
                never
            ),
            refused,
            "another program's split of the same table shape"
        );

        // The same table bytes at another program position: a live read
        // would index this program at the bind's offsets.
        let moved = admitted_padded(
            &[
                (DERIVED, EVENT, &[FOR_EACH_TYPE]),
                (LIVE, EVENT + 1, &[3, 0]),
            ],
            4,
        );
        let mut rows = RowColumns::new();
        let mut cols: [&[u8]; 6] = [&types, &offsets, b"ab", &key, &[], &[]];
        assert_eq!(
            live.execute_bound(
                &mut [],
                &mut cols,
                1,
                &mut scratch,
                &mut Constant,
                &mut rows,
                |_, _, _, _| 0,
            ),
            Ok(0)
        );
        let mut cols: [&[u8]; 6] = [&types, &offsets, b"ab", &key, &[], &[]];
        assert_eq!(
            moved.execute_spliced(
                &mut [],
                &mut cols,
                1,
                &mut scratch,
                &mut Constant,
                &rows,
                never
            ),
            refused,
            "the same table at another position"
        );
    }
}
