//! Static TTL algebra admission on native programs. A slot×slot bitmap algebra
//! source that carries TTL cannot fold into a target that declares none, so
//! native admission refuses such a program before any state image or capability
//! exists. The rule is exercised through the real sizing, init and admission
//! entry points, on programs built from the wire header and the opcode grammar.

use columine_types::PROGRAM_MAGIC;
use columine_types::operand::encode_index;
use columine_types::types::{
    ErrorCode, NO_PARENT_TS_COL, Opcode, PROGRAM_FORMAT_VERSION, PROGRAM_HASH_PREFIX,
    ProgramHeader, SlotType, SlotTypeFlags,
};
use columine_types::vo::{UnavailableVoAdmission, VoProgramMetadata};
use columine_vm::program_admission::{AdmittedProgram, ProgramAdmissionError};
use columine_vm::state_init::{
    DEFAULT_ACCEPTED_PROGRAM_MAGICS, ProgramInvalidCause, SizingRefusal, calculate_state_size,
    init_state,
};

const HASH_PREFIX: usize = PROGRAM_HASH_PREFIX as usize;

/// The four slot×slot algebra ops whose source is read in place.
const ALGEBRA: [Opcode; 4] = [
    Opcode::BatchBitmapAnd,
    Opcode::BatchBitmapOr,
    Opcode::BatchBitmapAndNot,
    Opcode::BatchBitmapXor,
];

/// The refusal for a TTL bitmap source (slot 1) folded into a plain target
/// (slot 0), as the fixtures below declare them.
const TTL_SOURCE_INTO_PLAIN_TARGET: ProgramInvalidCause =
    ProgramInvalidCause::BitmapTtlSourceToUntrackedTarget {
        source: 1,
        target: 0,
    };

/// A program whose init section ends in `HALT`, under the real wire header.
/// The reduce section is taken verbatim: admission walks exactly its bytes.
fn program(num_slots: u32, init: &[u8], reduce: &[u8]) -> Vec<u8> {
    let mut init = init.to_vec();
    init.push(Opcode::Halt as u8);
    let mut prog = vec![0u8; HASH_PREFIX];
    prog.extend(
        ProgramHeader {
            magic: PROGRAM_MAGIC,
            version: PROGRAM_FORMAT_VERSION,
            num_callbacks: 0,
            flags: 0,
            num_slots,
            num_inputs: 0,
            init_code_len: init.len() as u32,
            reduce_code_len: reduce.len() as u32,
        }
        .to_wire_bytes(),
    );
    prog.extend(init);
    prog.extend_from_slice(reduce);
    prog
}

/// `SLOT_DEF` for a bitmap of capacity 64. With TTL it carries the tail:
/// ttl 10s, grace 0, timestamp column 1, no start-of unit.
fn bitmap_def(slot: u8, has_ttl: bool) -> Vec<u8> {
    let flags = SlotTypeFlags::new(SlotType::Bitmap, has_ttl, false, false, false).to_byte();
    let mut def = vec![Opcode::SlotDef as u8, slot, flags, 64, 0];
    if has_ttl {
        def.extend(10.0f32.to_le_bytes());
        def.extend(0.0f32.to_le_bytes());
        def.extend(encode_index(1).as_bytes());
        def.push(0);
    }
    def
}

/// `SLOT_DEF` for a hashmap: a declared slot that is not a bitmap.
fn hashmap_def(slot: u8) -> Vec<u8> {
    vec![Opcode::SlotDef as u8, slot, SlotType::HashMap as u8, 8, 0]
}

/// `SLOT_ARRAY`: the slot, then capacity and one trailing byte. It has no type
/// byte, and its slot is an array.
fn array_def(slot: u8) -> Vec<u8> {
    vec![Opcode::SlotArray as u8, slot, 8, 0, 0]
}

/// A slot×slot algebra instruction: target, then source.
fn algebra(op: Opcode, target: u8, source: u8) -> Vec<u8> {
    vec![op as u8, target, source]
}

/// `FOR_EACH` over type column 0 matching one type id, around `body`.
fn for_each(body: &[u8]) -> Vec<u8> {
    let mut code = vec![Opcode::ForEach as u8, 0, 1];
    code.extend(7u32.to_le_bytes());
    code.extend((body.len() as u16).to_le_bytes());
    code.extend_from_slice(body);
    code
}

/// `FLAT_MAP` over offsets column 0 with no parent timestamp, around `body`.
fn flat_map(body: &[u8]) -> Vec<u8> {
    let mut code = vec![Opcode::FlatMap as u8, 0];
    code.extend(encode_index(NO_PARENT_TS_COL).as_bytes());
    code.extend((body.len() as u16).to_le_bytes());
    code.extend_from_slice(body);
    code
}

/// `body` inside `levels` nested `FLAT_MAP`s.
fn nested_flat_maps(body: &[u8], levels: usize) -> Vec<u8> {
    (0..levels).fold(body.to_vec(), |inner, _| flat_map(&inner))
}

/// Native sizing refuses the program with `cause`, native init refuses it
/// before writing a byte, and the admitted-program boundary refuses it before
/// any capability.
fn assert_refused(prog: &[u8], cause: ProgramInvalidCause) {
    assert_eq!(
        calculate_state_size(prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS),
        Err(SizingRefusal::Invalid(cause))
    );
    let mut image = vec![0xa5u8; 4096];
    assert_eq!(
        init_state(&mut image, prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS),
        Err(ErrorCode::InvalidProgram)
    );
    assert!(
        image.iter().all(|byte| *byte == 0xa5),
        "refused init wrote the image"
    );
    assert_eq!(
        AdmittedProgram::load(
            prog.to_vec().into_boxed_slice(),
            VoProgramMetadata::default(),
            UnavailableVoAdmission,
            DEFAULT_ACCEPTED_PROGRAM_MAGICS,
        )
        .err(),
        Some(ProgramAdmissionError::InvalidProgram(cause))
    );
}

/// Native sizing admits the program, and native init writes its image.
fn assert_admitted(prog: &[u8]) {
    let size = calculate_state_size(prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS)
        .expect("valid program admission");
    assert!(size > 0, "program must be admitted");
    let mut image = vec![0u8; size as usize];
    init_state(&mut image, prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS)
        .expect("admitted program initializes");
}

#[test]
fn ttl_source_into_a_plain_target_is_refused_for_every_algebra_op() {
    let init = [bitmap_def(0, false), bitmap_def(1, true)].concat();
    for op in ALGEBRA {
        assert_refused(
            &program(2, &init, &algebra(op, 0, 1)),
            TTL_SOURCE_INTO_PLAIN_TARGET,
        );
    }
}

#[test]
fn ttl_target_and_plain_source_pairs_are_admitted_for_every_algebra_op() {
    for (target_ttl, source_ttl) in [(true, true), (true, false), (false, false)] {
        let init = [bitmap_def(0, target_ttl), bitmap_def(1, source_ttl)].concat();
        for op in ALGEBRA {
            assert_admitted(&program(2, &init, &algebra(op, 0, 1)));
        }
    }
}

#[test]
fn scratch_algebra_has_no_tracked_source_and_stays_admitted() {
    for op in [
        Opcode::BatchBitmapAndScratch,
        Opcode::BatchBitmapOrScratch,
        Opcode::BatchBitmapAndNotScratch,
        Opcode::BatchBitmapXorScratch,
    ] {
        for target_ttl in [false, true] {
            assert_admitted(&program(1, &bitmap_def(0, target_ttl), &[op as u8, 0]));
        }
    }
}

#[test]
fn algebra_nested_in_a_for_each_or_flat_map_is_refused_at_every_depth() {
    let init = [bitmap_def(0, false), bitmap_def(1, true)].concat();
    for op in ALGEBRA {
        let inner = algebra(op, 0, 1);
        assert_refused(
            &program(2, &init, &for_each(&inner)),
            TTL_SOURCE_INTO_PLAIN_TARGET,
        );
        assert_refused(
            &program(2, &init, &for_each(&flat_map(&inner))),
            TTL_SOURCE_INTO_PLAIN_TARGET,
        );
        assert_refused(
            &program(2, &init, &flat_map(&for_each(&inner))),
            TTL_SOURCE_INTO_PLAIN_TARGET,
        );
    }
}

#[test]
fn legal_algebra_nested_in_envelopes_is_admitted() {
    let init = [bitmap_def(0, true), bitmap_def(1, false)].concat();
    let inner = algebra(Opcode::BatchBitmapXor, 0, 1);
    assert_admitted(&program(2, &init, &for_each(&flat_map(&inner))));
    assert_admitted(&program(2, &init, &flat_map(&for_each(&inner))));
}

#[test]
fn undeclared_or_non_bitmap_algebra_operands_are_refused() {
    // The source slot is never declared.
    assert_refused(
        &program(
            1,
            &bitmap_def(0, false),
            &algebra(Opcode::BatchBitmapAnd, 0, 1),
        ),
        ProgramInvalidCause::MalformedProgram,
    );
    // The target is a hashmap, not a bitmap.
    let init = [hashmap_def(0), bitmap_def(1, false)].concat();
    assert_refused(
        &program(2, &init, &algebra(Opcode::BatchBitmapOr, 0, 1)),
        ProgramInvalidCause::MalformedProgram,
    );
    // The source is an array: `SLOT_ARRAY` declares no bitmap.
    let init = [bitmap_def(0, false), array_def(1)].concat();
    assert_refused(
        &program(2, &init, &algebra(Opcode::BatchBitmapXor, 0, 1)),
        ProgramInvalidCause::MalformedProgram,
    );
}

#[test]
fn malformed_algebra_traversal_is_refused_without_relaxing_the_grammar() {
    let init = [bitmap_def(0, false), bitmap_def(1, true)].concat();
    // The source operand is missing from the reduce section.
    assert_refused(
        &program(2, &init, &[Opcode::BatchBitmapAnd as u8, 0]),
        ProgramInvalidCause::MalformedProgram,
    );
    // A FOR_EACH whose body length runs past the end of the section.
    let mut overlong = for_each(&algebra(Opcode::BatchBitmapAnd, 0, 1));
    overlong[7..9].copy_from_slice(&200u16.to_le_bytes());
    assert_refused(
        &program(2, &init, &overlong),
        ProgramInvalidCause::MalformedProgram,
    );
}

#[test]
fn duplicate_slot_claims_are_still_refused() {
    let init = [bitmap_def(0, false), bitmap_def(0, true)].concat();
    assert_refused(
        &program(2, &init, &[]),
        ProgramInvalidCause::MalformedProgram,
    );
}

#[test]
fn algebra_nesting_is_bounded_at_sixty_four_containers() {
    // Both sides are a legal TTL/TTL pair, so only the depth decides.
    let init = [bitmap_def(0, true), bitmap_def(1, true)].concat();
    let legal = algebra(Opcode::BatchBitmapAnd, 0, 1);
    assert_admitted(&program(2, &init, &nested_flat_maps(&legal, 64)));
    assert_refused(
        &program(2, &init, &nested_flat_maps(&legal, 65)),
        ProgramInvalidCause::MalformedProgram,
    );
}

#[test]
fn admitted_program_loads_a_legal_top_level_algebra() {
    let init = [bitmap_def(0, true), bitmap_def(1, false)].concat();
    let prog = program(2, &init, &algebra(Opcode::BatchBitmapAndNot, 0, 1));
    AdmittedProgram::load(
        prog.into_boxed_slice(),
        VoProgramMetadata::default(),
        UnavailableVoAdmission,
        DEFAULT_ACCEPTED_PROGRAM_MAGICS,
    )
    .unwrap_or_else(|error| panic!("legal algebra admission: {error:?}"));
}
