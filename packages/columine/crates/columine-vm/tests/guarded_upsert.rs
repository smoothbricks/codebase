//! BATCH_STRUCT_MAP_REMOVE (0x8c) and BATCH_STRUCT_MAP_GUARDED_UPSERT_LAST
//! (0x8d): a write gated on a companion struct map holding the row key with
//! an exact guard-key cell. Slot 0 is the destination struct map (value
//! u32, stamp i64); slot 1 is its companion
//! companion (guardKey interned u32, basis i64), keyed by the same row key.

use columine_types::PROGRAM_MAGIC;
use columine_types::types::{ErrorCode, Opcode, SlotType, StructFieldType};
use columine_vm::bytes;
use columine_vm::state_init::{calculate_state_size, init_state};
use columine_vm::struct_map::StructMapSlot;
use columine_vm::vm::{Vm, i64s_as_bytes, u32s_as_bytes};

const OK: u32 = ErrorCode::Ok as u32;
const SEED: u32 = 0x5eed_0001;
const GUARDED: u32 = 0x5eed_0002;
const REWRITE: u32 = 0x5eed_0003;
const DELETE: u32 = 0x5eed_0004;

fn program(init: &[u8], num_inputs: u8, reduce: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; 32];
    out.extend(PROGRAM_MAGIC.to_le_bytes());
    out.extend([2, 0, 0, 0]);
    out.extend(2u32.to_le_bytes()); // num_slots: destination + companion
    out.extend(u32::from(num_inputs).to_le_bytes());
    out.extend(u32::try_from(init.len()).unwrap().to_le_bytes());
    out.extend(u32::try_from(reduce.len()).unwrap().to_le_bytes());
    out.extend_from_slice(init);
    out.extend_from_slice(reduce);
    out
}

fn slot_def(slot: u8, fields: &[StructFieldType]) -> Vec<u8> {
    let mut init = vec![
        Opcode::SlotStructMap as u8,
        slot,
        SlotType::StructMap as u8,
        64,
        0,
        fields.len() as u8,
    ];
    init.extend(fields.iter().map(|field| *field as u8));
    init
}

fn for_each(type_id: u32, body: &[u8]) -> Vec<u8> {
    let mut code = vec![Opcode::ForEach as u8, 0, 1];
    code.extend(type_id.to_le_bytes());
    code.extend((body.len() as u16).to_le_bytes());
    code.extend_from_slice(body);
    code
}

/// Columns: 0 = type, 1 = row key, 2 = value u32 / companion guardKey,
/// 3 = since/basis i64, 4 = guard key.
fn guarded_program() -> Vec<u8> {
    let mut init = slot_def(0, &[StructFieldType::UInt32, StructFieldType::Int64]);
    init.extend(slot_def(
        1,
        &[StructFieldType::String, StructFieldType::Int64],
    ));
    init.push(Opcode::Halt as u8);

    // SEED: write companion rows (key <- col 1, guardKey <- col 2, basis <- col 3).
    let seed = vec![
        Opcode::BatchStructMapUpsertLast as u8,
        1,
        1,
        2,
        2,
        0,
        3,
        1,
        0,
    ];
    // GUARDED: guarded upsert into slot 0 (value <- col 2, since <- col 3),
    // companion slot 1, terminal key <- col 4, guardKey field 0.
    let guarded = vec![
        Opcode::BatchStructMapGuardedUpsertLast as u8,
        0,
        1,
        1,
        4,
        0,
        2,
        2,
        0,
        3,
        1,
        0,
    ];
    // REWRITE: a normal write to slot 0 followed by its companion clear.
    let mut rewrite = vec![
        Opcode::BatchStructMapUpsertLast as u8,
        0,
        1,
        2,
        2,
        0,
        3,
        1,
        0,
    ];
    rewrite.extend([Opcode::BatchStructMapRemove as u8, 1, 1]);

    // DELETE: remove the main row and its companion row by the same key column.
    let delete = vec![
        Opcode::BatchStructMapRemove as u8,
        0,
        1,
        Opcode::BatchStructMapRemove as u8,
        1,
        1,
    ];

    // Every removal precedes its slot's producers: DELETE removes rows of
    // both slots and REWRITE clears the companion before SEED and GUARDED
    // write either.
    let mut reduce = for_each(DELETE, &delete);
    reduce.extend(for_each(REWRITE, &rewrite));
    reduce.extend(for_each(SEED, &seed));
    reduce.extend(for_each(GUARDED, &guarded));
    program(&init, 5, &reduce)
}

fn init(program: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(
        program,
        columine_vm::state_init::DEFAULT_ACCEPTED_PROGRAM_MAGICS,
    )
    .expect("valid program admission");
    let mut state = vec![0u8; size as usize];
    init_state(
        &mut state,
        program,
        columine_vm::state_init::DEFAULT_ACCEPTED_PROGRAM_MAGICS,
    )
    .unwrap();
    state
}

#[allow(clippy::too_many_arguments)]
fn run(
    vm: &mut Vm,
    state: &mut [u8],
    program: &[u8],
    types: &[u32],
    keys: &[u32],
    values: &[u32],
    stamps: &[i64],
    terminals: &[u32],
) -> u32 {
    let columns: [&[u8]; 5] = [
        u32s_as_bytes(types),
        u32s_as_bytes(keys),
        u32s_as_bytes(values),
        i64s_as_bytes(stamps),
        u32s_as_bytes(terminals),
    ];
    vm.execute_batch(state, program, &columns, types.len() as u32)
}

fn row(state: &[u8], slot: u32, key: u32) -> Option<(u32, i64)> {
    let map = StructMapSlot::bind(state, slot);
    let pos = map.find(state, key)?;
    let off = map.row_off(pos);
    Some((
        bytes::read_u32(state, off + map.field_offset(state, 0)),
        bytes::read_i64(state, off + map.field_offset(state, 1)),
    ))
}

fn seeded() -> (Vm, Vec<u8>, Vec<u8>) {
    let program = guarded_program();
    let mut state = init(&program);
    let mut vm = Vm::default();
    // Two unresolved rows: key 7 under guard key 700, key 9 under guard key 900.
    assert_eq!(
        run(
            &mut vm,
            &mut state,
            &program,
            &[SEED, SEED],
            &[7, 9],
            &[700, 900],
            &[41, 97],
            &[0, 0]
        ),
        OK
    );
    assert_eq!(row(&state, 1, 7), Some((700, 41)));
    assert_eq!(row(&state, 1, 9), Some((900, 97)));
    (vm, state, program)
}

#[test]
fn guarded_write_lands_only_under_the_exact_guard_key_and_consumes_it() {
    let (mut vm, mut state, program) = seeded();
    // Key 7 with its guard key lands; key 9 with a stale key does not.
    assert_eq!(
        run(
            &mut vm,
            &mut state,
            &program,
            &[GUARDED, GUARDED],
            &[7, 9],
            &[70, 90],
            &[1000, 1000],
            &[700, 901]
        ),
        OK
    );
    assert_eq!(row(&state, 0, 7), Some((70, 1000)));
    assert_eq!(
        row(&state, 1, 7),
        None,
        "the admitted write consumes its companion row"
    );
    assert_eq!(row(&state, 0, 9), None, "a stale guard key writes nothing");
    assert_eq!(
        row(&state, 1, 9),
        Some((900, 97)),
        "the unresolved row stays unresolved"
    );

    // Replaying the same terminal is a no-op, not a second write.
    assert_eq!(
        run(
            &mut vm,
            &mut state,
            &program,
            &[GUARDED],
            &[7],
            &[71],
            &[1001],
            &[700]
        ),
        OK
    );
    assert_eq!(row(&state, 0, 7), Some((70, 1000)));
}

#[test]
fn a_rewrite_clears_the_companion_so_the_late_guarded_write_never_overwrites() {
    let (mut vm, mut state, program) = seeded();
    assert_eq!(
        run(
            &mut vm,
            &mut state,
            &program,
            &[REWRITE],
            &[7],
            &[77],
            &[2000],
            &[0]
        ),
        OK
    );
    assert_eq!(
        row(&state, 1, 7),
        None,
        "the normal write cleared the companion row"
    );
    assert_eq!(
        run(
            &mut vm,
            &mut state,
            &program,
            &[GUARDED],
            &[7],
            &[70],
            &[1000],
            &[700]
        ),
        OK
    );
    assert_eq!(
        row(&state, 0, 7),
        Some((77, 2000)),
        "the rewritten row is never overwritten by the stale guarded write"
    );
}

#[test]
fn a_row_deleted_while_unresolved_stays_deleted_when_its_guarded_write_lands() {
    let (mut vm, mut state, program) = seeded();
    assert_eq!(
        run(
            &mut vm,
            &mut state,
            &program,
            &[DELETE],
            &[9],
            &[0],
            &[0],
            &[0]
        ),
        OK
    );
    assert_eq!(
        row(&state, 1, 9),
        None,
        "the delete cleared the companion row"
    );
    assert_eq!(
        run(
            &mut vm,
            &mut state,
            &program,
            &[GUARDED],
            &[9],
            &[90],
            &[1000],
            &[900]
        ),
        OK
    );
    assert_eq!(
        row(&state, 0, 9),
        None,
        "the deleted row is never resurrected"
    );

    // A sentinel-key remove is a no-op, so clears compose unconditionally.
    assert_eq!(
        run(
            &mut vm,
            &mut state,
            &program,
            &[DELETE],
            &[u32::MAX],
            &[0],
            &[0],
            &[0]
        ),
        OK
    );
}

/// A program in the seeded shape whose extra body is caller-chosen, for
/// admission cases the paired program cannot express.
fn variant_program(extra_type: u32, extra_body: &[u8]) -> Vec<u8> {
    let mut init = slot_def(0, &[StructFieldType::UInt32, StructFieldType::Int64]);
    init.extend(slot_def(
        1,
        &[StructFieldType::String, StructFieldType::Int64],
    ));
    init.push(Opcode::Halt as u8);
    let seed = vec![
        Opcode::BatchStructMapUpsertLast as u8,
        1,
        1,
        2,
        2,
        0,
        3,
        1,
        0,
    ];
    let guarded = vec![
        Opcode::BatchStructMapGuardedUpsertLast as u8,
        0,
        1,
        1,
        4,
        0,
        2,
        2,
        0,
        3,
        1,
        0,
    ];
    // The extra body leads, so a removal in it precedes every producer and
    // the companion pairing alone decides the program.
    let mut reduce = for_each(extra_type, extra_body);
    reduce.extend(for_each(SEED, &seed));
    reduce.extend(for_each(GUARDED, &guarded));
    program(&init, 5, &reduce)
}

const INVALID: u32 = ErrorCode::InvalidProgram as u32;
const CHALLENGE: u32 = 0x5eed_0009;

#[test]
fn an_unpaired_destination_delete_refuses_the_whole_program() {
    // The delete removes the destination row but not its companion row: a
    // later guarded write would resurrect the row, so admission refuses
    // before the seed batch folds anything.
    let delete_dest_only = vec![Opcode::BatchStructMapRemove as u8, 0, 1];
    let program = variant_program(CHALLENGE, &delete_dest_only);
    let mut state = init(&program);
    let mut vm = Vm::default();
    assert_eq!(
        run(
            &mut vm,
            &mut state,
            &program,
            &[SEED],
            &[7],
            &[700],
            &[41],
            &[0]
        ),
        INVALID
    );
    assert_eq!(row(&state, 1, 7), None);
}

#[test]
fn an_unpaired_destination_rewrite_refuses_the_whole_program() {
    // A plain upsert into the destination with no companion clear after it.
    let rewrite_only = vec![
        Opcode::BatchStructMapUpsertLast as u8,
        0,
        1,
        2,
        2,
        0,
        3,
        1,
        0,
    ];
    let program = variant_program(CHALLENGE, &rewrite_only);
    let mut state = init(&program);
    let mut vm = Vm::default();
    assert_eq!(
        run(
            &mut vm,
            &mut state,
            &program,
            &[SEED],
            &[7],
            &[700],
            &[41],
            &[0]
        ),
        INVALID
    );
}

#[test]
fn a_self_companion_guarded_write_refuses_the_whole_program() {
    // Companion == destination: the write would delete the row it just wrote.
    let self_guarded = vec![
        Opcode::BatchStructMapGuardedUpsertLast as u8,
        0,
        0,
        1,
        4,
        0,
        2,
        2,
        0,
        3,
        1,
        0,
    ];
    let program = variant_program(CHALLENGE, &self_guarded);
    let mut state = init(&program);
    let mut vm = Vm::default();
    assert_eq!(
        run(
            &mut vm,
            &mut state,
            &program,
            &[SEED],
            &[7],
            &[700],
            &[41],
            &[0]
        ),
        INVALID
    );
}
