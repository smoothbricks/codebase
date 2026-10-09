//! The owning program boundary drives the real VM on ordinary programs and
//! refuses missing VO capability/schema declarations before an image exists.
//! Positive VO kernel certification lives with the runtime's real capability.

use columine_types::PROGRAM_MAGIC;
use columine_types::vo::{
    UnavailableVoAdmission, VoAdmissionError, VoFieldLink, VoMetadata, VoProfile, VoProgramMetadata,
};
use columine_vm::program_admission::{
    AdmittedProgram, ProgramAdmissionError, ProgramScratch, declares_vo,
};
use columine_vm::state_init::{DEFAULT_ACCEPTED_PROGRAM_MAGICS, ProgramInvalidCause};
use columine_vm::struct_map::StructMapSlot;
use columine_vm::vm::{Vm, u32s_as_bytes};

/// Wrap an init and a reduce section in a real program header: one slot,
/// three inputs, each section's length taken from its bytes.
fn assemble(init: &[u8], reduce: &[u8]) -> Box<[u8]> {
    let mut bytes = vec![0u8; 32];
    bytes.extend_from_slice(&PROGRAM_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&[2, 0, 0, 0]);
    bytes.extend_from_slice(&1u32.to_le_bytes());
    bytes.extend_from_slice(&3u32.to_le_bytes());
    bytes.extend_from_slice(&u32::try_from(init.len()).unwrap().to_le_bytes());
    bytes.extend_from_slice(&u32::try_from(reduce.len()).unwrap().to_le_bytes());
    bytes.extend_from_slice(init);
    bytes.extend_from_slice(reduce);
    bytes.into_boxed_slice()
}

/// The init section of one StructMap slot declaring `kinds` as its fields in
/// order: `SLOT_STRUCT_MAP, slot, flags, capacity, field count, kinds, HALT`.
fn struct_map_init(kinds: &[u8]) -> Vec<u8> {
    let mut init = vec![0x18, 0, 6, 4, 0, u8::try_from(kinds.len()).unwrap()];
    init.extend_from_slice(kinds);
    init.push(0);
    init
}

/// One CSR array upsert of field 0 through columns 1 (offsets) and 2 (values),
/// keyed by column 0.
const CSR_UPSERT: [u8; 8] = [0x80, 0, 0, 0, 1, 1, 2, 0];

fn program(kind: u8) -> Box<[u8]> {
    // One StructMap field and one real row upsert. A VO field uses CSR input;
    // an ordinary u32 field uses the scalar column.
    let init = struct_map_init(&[kind]);
    let reduce: &[u8] = if kind == 10 {
        &CSR_UPSERT
    } else {
        &[0x80, 0, 0, 1, 1, 0, 0]
    };
    assemble(&init, reduce)
}

fn metadata() -> VoProgramMetadata {
    VoProgramMetadata {
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
    }
}

#[test]
fn ordinary_owned_program_executes_and_rolls_back_without_a_vo_capability() {
    let code = program(0);
    let original_pointer = code.as_ptr();
    let admitted = AdmittedProgram::load(
        code,
        VoProgramMetadata::default(),
        UnavailableVoAdmission,
        DEFAULT_ACCEPTED_PROGRAM_MAGICS,
    )
    .unwrap_or_else(|error| panic!("ordinary admission: {error:?}"));
    assert_eq!(
        original_pointer,
        admitted.bytecode().as_ptr(),
        "ownership moves without copying"
    );
    let mut state = vec![0; usize::try_from(admitted.state_size()).unwrap()];
    admitted.initialize_state(&mut state).unwrap();
    let before = state.clone();
    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("valid inactive window");
    let checkpoint = vm.undo_checkpoint();
    let keys = [7u32];
    let values = [42u32];
    let cols = [u32s_as_bytes(&keys), u32s_as_bytes(&values)];
    let mut scratch = ProgramScratch::default();
    assert_eq!(
        admitted.execute_batch_delta(&mut vm, &mut state, &cols, 1, &mut scratch),
        Ok(0)
    );
    let slot = StructMapSlot::bind(&state, 0);
    let pos = slot.find(&state, 7).expect("real VM inserted key");
    let row = slot.row_off(pos);
    assert_eq!(
        columine_vm::bytes::read_u32(&state, row + slot.field_offset(&state, 0)),
        42
    );
    assert_eq!(
        vm.undo_rollback(&mut state, checkpoint),
        Ok(admitted.state_size())
    );
    // Rollback restores the physical image: the removed key's probe cells and
    // bitsets equal the pre-batch image, byte for byte.
    assert!(StructMapSlot::bind(&state, 0).find(&state, 7).is_none());
    assert_eq!(state, before);
}

#[test]
fn vo_load_requires_both_schema_link_and_real_capability() {
    assert_eq!(
        AdmittedProgram::load(
            program(10),
            VoProgramMetadata::default(),
            UnavailableVoAdmission,
            DEFAULT_ACCEPTED_PROGRAM_MAGICS,
        )
        .err(),
        Some(ProgramAdmissionError::MissingSchema {
            slot: 0,
            field_idx: 0
        })
    );
    let refusal = AdmittedProgram::load(
        program(10),
        metadata(),
        UnavailableVoAdmission,
        DEFAULT_ACCEPTED_PROGRAM_MAGICS,
    )
    .err();
    assert!(matches!(
        refusal,
        Some(ProgramAdmissionError::Preparation(error))
            if error.error == VoAdmissionError::Unavailable
    ));
}

#[test]
fn declaration_mismatch_and_duplicates_refuse_before_preparation() {
    assert_eq!(
        AdmittedProgram::load(
            program(0),
            metadata(),
            UnavailableVoAdmission,
            DEFAULT_ACCEPTED_PROGRAM_MAGICS,
        )
        .err(),
        Some(ProgramAdmissionError::UnexpectedSchema {
            slot: 0,
            field_idx: 0
        })
    );
    let mut duplicate = metadata();
    duplicate.fields.push(duplicate.fields[0].clone());
    assert_eq!(
        AdmittedProgram::load(
            program(10),
            duplicate,
            UnavailableVoAdmission,
            DEFAULT_ACCEPTED_PROGRAM_MAGICS,
        )
        .err(),
        Some(ProgramAdmissionError::DuplicateField {
            slot: 0,
            field_idx: 0
        })
    );
}

#[test]
fn omitted_shape_declarations_refuse_before_raw_vm_mutation() {
    let original = program(0);
    let size =
        columine_vm::state_init::calculate_state_size(&original, DEFAULT_ACCEPTED_PROGRAM_MAGICS)
            .expect("valid program admission");
    let mut state = vec![0; usize::try_from(size).unwrap()];
    columine_vm::state_init::init_state(&mut state, &original, DEFAULT_ACCEPTED_PROGRAM_MAGICS)
        .unwrap();
    let before = state.clone();
    // The header still claims one slot, but its bounded init section contains
    // only HALT. The reduce section is the original real row upsert.
    let mut malformed = original[..56].to_vec();
    malformed[48..52].copy_from_slice(&1u32.to_le_bytes());
    malformed.push(0);
    malformed.extend_from_slice(&original[64..]);
    let keys = [7u32];
    let values = [42u32];
    let cols = [u32s_as_bytes(&keys), u32s_as_bytes(&values)];
    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("valid inactive window");
    assert_eq!(vm.execute_batch_delta(&mut state, &malformed, &cols, 1), 2);
    assert_eq!(state, before);
    assert_eq!(vm.undo_checkpoint(), 0);
}

#[test]
fn vo_input_columns_cannot_also_supply_scalar_keys() {
    for key_col in [1u8, 2] {
        let mut code = program(10).into_vec();
        // The CSR offsets and payload columns cannot also be the key source:
        // a retained-writer projection changes them before execution.
        code[66] = key_col;
        assert_eq!(
            AdmittedProgram::load(
                code.into_boxed_slice(),
                metadata(),
                UnavailableVoAdmission,
                DEFAULT_ACCEPTED_PROGRAM_MAGICS,
            )
            .err(),
            Some(ProgramAdmissionError::InvalidProgram(
                ProgramInvalidCause::MalformedProgram
            ))
        );
    }
}

#[test]
fn declares_vo_reads_only_the_init_declarations() {
    assert_eq!(declares_vo(&program(0)), Ok(false));
    assert_eq!(declares_vo(&program(10)), Ok(true));
    // ArrayBool is the ordinary bool-array lane and takes the same CSR upsert
    // a value-object field does; only its declared kind differs.
    let array_bool = assemble(&struct_map_init(&[9]), &CSR_UPSERT);
    assert_eq!(declares_vo(&array_bool), Ok(false));
    for kind in 0..=9 {
        assert_eq!(declares_vo(&program(kind)), Ok(false), "kind {kind}");
    }
}

#[test]
fn declares_vo_finds_a_value_object_field_at_any_position() {
    for (kinds, expected) in [
        (&[10u8][..], true),
        (&[10, 0, 9][..], true),
        (&[0, 10, 9][..], true),
        (&[0, 9, 5, 4, 10][..], true),
        (&[0, 9, 5, 4, 8][..], false),
    ] {
        let code = assemble(&struct_map_init(kinds), &CSR_UPSERT);
        assert_eq!(declares_vo(&code), Ok(expected), "kinds {kinds:?}");
    }
}

#[test]
fn declares_vo_refuses_unknown_kinds_and_malformed_programs() {
    let invalid: Result<bool, ProgramAdmissionError> = Err(ProgramAdmissionError::InvalidProgram(
        ProgramInvalidCause::MalformedProgram,
    ));
    // A kind byte past the closed field-kind set refuses alone and after a VO
    // field: the whole init section is validated, never cut short at the first
    // VO declaration.
    for kinds in [&[11u8][..], &[0xff][..], &[10, 11][..], &[11, 10][..]] {
        let code = assemble(&struct_map_init(kinds), &CSR_UPSERT);
        assert_eq!(declares_vo(&code), invalid, "kinds {kinds:?}");
    }
    // An init section with no HALT, a field list cut short by its HALT, and a
    // record that is no slot declaration at all.
    let mut unterminated = struct_map_init(&[10]);
    unterminated.pop();
    let truncated_fields = vec![0x18, 0, 6, 4, 0, 2, 10, 0];
    let not_a_slot = vec![0x22, 0, 0, 0, 0, 0];
    for init in [unterminated, truncated_fields, not_a_slot] {
        let code = assemble(&init, &CSR_UPSERT);
        assert_eq!(declares_vo(&code), invalid, "init {init:?}");
    }
    let mut after_halt = struct_map_init(&[10]);
    after_halt.push(11);
    assert_eq!(
        declares_vo(&assemble(&after_halt, &CSR_UPSERT)),
        invalid,
        "trailing init data after HALT"
    );
    // Header refusals: too short for a header, a section cut short, another
    // format version, a section length past the program.
    let valid = program(10);
    assert_eq!(declares_vo(&[]), invalid);
    assert_eq!(
        declares_vo(&valid[..40]),
        invalid,
        "shorter than the header"
    );
    assert_eq!(declares_vo(&valid[..60]), invalid, "init section cut short");
    let mut version = valid.to_vec();
    version[36..38].copy_from_slice(&3u16.to_le_bytes());
    assert_eq!(declares_vo(&version), invalid, "wrong format version");
    let mut oversized = valid.to_vec();
    oversized[48..52].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(
        declares_vo(&oversized),
        invalid,
        "init length past the program"
    );
}

#[test]
fn declares_vo_agrees_with_the_owning_load_boundary() {
    for kind in [0u8, 10] {
        let code = program(kind);
        let declared = declares_vo(&code).unwrap();
        let loaded = AdmittedProgram::load(
            code,
            VoProgramMetadata::default(),
            UnavailableVoAdmission,
            DEFAULT_ACCEPTED_PROGRAM_MAGICS,
        );
        // With no schema declaration an ordinary program loads and a VO
        // program refuses by name; the capability-free answer predicts which.
        assert_eq!(declared, loaded.is_err(), "kind {kind}");
        if declared {
            assert_eq!(
                loaded.err(),
                Some(ProgramAdmissionError::MissingSchema {
                    slot: 0,
                    field_idx: 0
                })
            );
        }
    }
}

#[test]
fn struct_map_opcode_cannot_alias_another_declared_slot_kind() {
    let mut code = program(0).into_vec();
    code[58] = 5; // StructMap opcode, Array type flags: no historical alias.
    assert_eq!(
        columine_vm::state_init::calculate_state_size(&code, DEFAULT_ACCEPTED_PROGRAM_MAGICS),
        Err(ProgramInvalidCause::MalformedProgram)
    );
    assert_eq!(
        AdmittedProgram::load(
            code.into_boxed_slice(),
            VoProgramMetadata::default(),
            UnavailableVoAdmission,
            DEFAULT_ACCEPTED_PROGRAM_MAGICS,
        )
        .err(),
        Some(ProgramAdmissionError::InvalidProgram(
            ProgramInvalidCause::MalformedProgram
        ))
    );
}
