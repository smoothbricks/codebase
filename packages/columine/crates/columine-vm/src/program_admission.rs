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
            if declared_field(sections.init_code, link.slot, link.field_idx)
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
            if declared_field(sections.init_code, op.slot, op.field_idx)
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
    /// and certified columns. This is the boundary for live-column or superset
    /// execution: admission completes before the callback may mutate state.
    pub fn execute_with(
        &self,
        cols: &[&[u8]],
        batch_len: u32,
        scratch: &mut ProgramScratch,
        execute: impl FnOnce(&[u8], &[&[u8]]) -> u32,
    ) -> Result<u32, InputAdmissionError> {
        self.inputs.admit_and_execute(
            &self.metadata,
            &self.admission,
            cols,
            batch_len,
            scratch,
            |cols| execute(&self.bytecode, cols),
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

fn declared_field(init: &[u8], slot: u32, field_idx: u8) -> Option<StructFieldType> {
    let mut found = None;
    visit_fields(init, |declared_slot, declared_field, kind| {
        if declared_slot == slot && declared_field == field_idx {
            found = Some(kind);
        }
        Ok(())
    })
    .ok()?;
    found
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
