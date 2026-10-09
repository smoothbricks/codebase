//! Native smoke tests THROUGH the extern layer: raw pointers, the region
//! registry, and the binary-root statics — the machinery the wasm artifact
//! ships. Semantics are pinned by the library crates' suites; these tests
//! pin the wrapper plumbing (bounding, statics routing, packing).
//!
//! `unsafe` throughout is the point: this exercises the exact C ABI calls
//! the TS backends make.

use columine_types::PROGRAM_MAGIC;
use columine_types::types::{
    ErrorCode, Opcode, PROGRAM_HASH_PREFIX, SLOT_META_SIZE, STATE_HEADER_SIZE, SlotMetaOffset,
    SlotType, SlotTypeFlags, StructFieldType,
};
use columine_wasm::{
    __register_region, vm_calculate_state_size, vm_delta_export_entry_size,
    vm_delta_export_len_bytes, vm_delta_export_redo_ptr, vm_delta_export_segment,
    vm_delta_export_undo_ptr, vm_evict_all_expired, vm_execute_batch, vm_execute_batch_delta,
    vm_init_state, vm_map_get, vm_reset_state, vm_set_contains, vm_struct_map2_get_row_ptr,
    vm_struct_map2_iter_key1, vm_struct_map2_iter_key2, vm_struct_map2_iter_next,
    vm_struct_map2_iter_row_ptr, vm_struct_map2_iter_start, vm_undo_checkpoint, vm_undo_enable,
    vm_undo_has_overflow, vm_undo_rollback,
};
use std::sync::{Mutex, MutexGuard};

const OK: u32 = ErrorCode::Ok as u32;
const INVALID_STATE: u32 = ErrorCode::InvalidState as u32;

// The extern layer routes through binary-root statics (RUNTIME, the region
// registry) — single-threaded by contract on wasm, but the native test
// harness runs #[test]s on parallel threads sharing this process's statics.
// Serialize them; tolerate poisoning so one failure doesn't cascade.
static SERIAL: Mutex<()> = Mutex::new(());
fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn meta_u32(state: &[u8], slot: u8, field_off: u32) -> u32 {
    let off = (STATE_HEADER_SIZE + u32::from(slot) * SLOT_META_SIZE + field_off) as usize;
    u32::from_le_bytes(state[off..off + 4].try_into().unwrap())
}

fn register(buf: &[u8]) {
    __register_region(buf.as_ptr(), buf.len());
}

fn u32s(values: &[u32]) -> Vec<u8> {
    values
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect()
}

fn word(record: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(record[at..at + 4].try_into().unwrap())
}

/// Wire-format program: a zeroed prefix, the header, then init and reduce code.
fn program(slots: u32, inputs: u32, init: &[u8], reduce: &[u8]) -> Vec<u8> {
    let mut prog = vec![0u8; 32];
    prog.extend(PROGRAM_MAGIC.to_le_bytes());
    prog.extend([2, 0, 0, 0]); // version 2, no callbacks, no flags
    prog.extend(slots.to_le_bytes());
    prog.extend(inputs.to_le_bytes());
    prog.extend((init.len() as u32).to_le_bytes());
    prog.extend((reduce.len() as u32).to_le_bytes());
    prog.extend_from_slice(init);
    prog.extend_from_slice(reduce);
    prog
}

/// Build a HASHMAP(LAST) capacity-16 program with an AGG COUNT operation.
fn test_program() -> Vec<u8> {
    let mut init: Vec<u8> = [[0x10, 0, 0x00, 16, 0], [0x10, 1, 0x02, 2, 0]].concat();
    init.push(0);
    let reduce = [0x22u8, 0, 0, 1, 0x41, 1];
    program(2, 2, &init, &reduce)
}

/// Sizing through the extern layer into a registered 16-byte record. The status
/// comes first; the words mean something only under it.
fn size_record(program: &[u8]) -> (u32, [u8; 16]) {
    register(program);
    let mut record = [0xa5u8; 16];
    register(&record);
    let status = unsafe {
        vm_calculate_state_size(program.as_ptr(), program.len() as u32, record.as_mut_ptr())
    };
    (status, record)
}

/// The record's four little-endian words.
fn words(record: &[u8]) -> Vec<u32> {
    (0..record.len())
        .step_by(4)
        .map(|at| word(record, at))
        .collect()
}

/// Size and initialize a state image through the extern layer.
fn init_state(program: &[u8]) -> Vec<u8> {
    let (status, record) = size_record(program);
    assert_eq!(status, OK, "a valid program is admitted");
    let size = word(&record, 0);
    assert!(size > 0);
    let mut state = vec![0u8; size as usize];
    register(&state);
    assert_eq!(
        unsafe { vm_init_state(state.as_mut_ptr(), program.as_ptr(), program.len() as u32) },
        OK
    );
    state
}

/// Run one batch through the extern layer. Columns and both pointer tables are
/// registered so the bounds checks see real owned memory.
fn execute_batch(
    state: &mut [u8],
    program: &[u8],
    columns: &[Vec<u8>],
    batch_len: u32,
    delta: bool,
) -> u32 {
    let ptrs: Vec<*const u8> = columns.iter().map(|column| column.as_ptr()).collect();
    let lens: Vec<u32> = columns.iter().map(|column| column.len() as u32).collect();
    for column in columns {
        register(column);
    }
    __register_region(
        ptrs.as_ptr().cast(),
        core::mem::size_of_val(ptrs.as_slice()),
    );
    __register_region(
        lens.as_ptr().cast(),
        core::mem::size_of_val(lens.as_slice()),
    );
    let execute = if delta {
        vm_execute_batch_delta
    } else {
        vm_execute_batch
    };
    unsafe {
        execute(
            state.as_mut_ptr(),
            state.len() as u32,
            program.as_ptr(),
            program.len() as u32,
            ptrs.as_ptr(),
            lens.as_ptr(),
            columns.len() as u32,
            batch_len,
        )
    }
}

/// One typed read through the extern layer into a caller record seeded with
/// 0xa5, so a refused or unwritten byte stays visible instead of reading as zero.
fn typed_read<const N: usize>(
    state: &[u8],
    read: impl FnOnce(*const u8, u32, *mut u8) -> u32,
) -> (u32, [u8; N]) {
    let mut record = [0xa5u8; N];
    register(&record);
    let status = read(state.as_ptr(), state.len() as u32, record.as_mut_ptr());
    (status, record)
}

fn map_get(state: &[u8], slot: u32, key: u32) -> (u32, [u8; 8]) {
    typed_read(state, |s, n, out| unsafe {
        vm_map_get(s, n, slot, key, out)
    })
}

fn set_contains(state: &[u8], slot: u32, elem: u32) -> (u32, [u8; 4]) {
    typed_read(state, |s, n, out| unsafe {
        vm_set_contains(s, n, slot, elem, out)
    })
}

fn struct_map2_row(state: &[u8], slot: u32, key1: u32, key2: u32) -> (u32, [u8; 8]) {
    typed_read(state, |s, n, out| unsafe {
        vm_struct_map2_get_row_ptr(s, n, slot, key1, key2, out)
    })
}

fn struct_map2_iter_start(state: &[u8], slot: u32) -> (u32, [u8; 4]) {
    typed_read(state, |s, n, out| unsafe {
        vm_struct_map2_iter_start(s, n, slot, out)
    })
}

fn struct_map2_iter_next(state: &[u8], slot: u32, current: u32) -> (u32, [u8; 4]) {
    typed_read(state, |s, n, out| unsafe {
        vm_struct_map2_iter_next(s, n, slot, current, out)
    })
}

fn struct_map2_iter_key1(state: &[u8], slot: u32, pos: u32) -> (u32, [u8; 4]) {
    typed_read(state, |s, n, out| unsafe {
        vm_struct_map2_iter_key1(s, n, slot, pos, out)
    })
}

fn struct_map2_iter_key2(state: &[u8], slot: u32, pos: u32) -> (u32, [u8; 4]) {
    typed_read(state, |s, n, out| unsafe {
        vm_struct_map2_iter_key2(s, n, slot, pos, out)
    })
}

#[test]
fn init_execute_read_undo_round_trip() {
    let _serial = serial();
    let program = test_program();
    let mut state = init_state(&program);
    let size = state.len() as u32;

    assert_eq!(unsafe { vm_undo_enable(state.as_mut_ptr(), size) }, OK);
    let cp = unsafe { vm_undo_checkpoint(state.as_mut_ptr()) };

    // Batch: keys [7, 9], values [70, 90].
    assert_eq!(
        execute_batch(
            &mut state,
            &program,
            &[u32s(&[7, 9]), u32s(&[70, 90])],
            2,
            false
        ),
        OK
    );
    assert_eq!(map_get(&state, 0, 7), (OK, [1, 0, 0, 0, 70, 0, 0, 0]));
    assert_eq!(map_get(&state, 0, 9), (OK, [1, 0, 0, 0, 90, 0, 0, 0]));
    assert_eq!(vm_undo_has_overflow(), 0);

    // An undo-only mutation is a distinct typed export refusal, not empty redo.
    let mut record = [0xa5; 4];
    register(&record);
    let end = unsafe { vm_undo_checkpoint(state.as_mut_ptr()) };
    assert_eq!(
        unsafe { vm_delta_export_segment(state.as_ptr(), size, cp, end, record.as_mut_ptr()) },
        ErrorCode::DeltaLaneMismatch as u32,
    );
    assert_eq!(record, [0xa5; 4]);
    assert_eq!(vm_delta_export_entry_size(), 24);

    let mut logical_size = [0u8; 4];
    register(&logical_size);
    assert_eq!(
        unsafe { vm_undo_rollback(state.as_mut_ptr(), size, cp, logical_size.as_mut_ptr()) },
        OK
    );
    assert_eq!(u32::from_le_bytes(logical_size), size);
    assert_eq!(
        map_get(&state, 0, 7),
        (OK, [0; 8]),
        "rollback erases the insert: absence is presence 0 with an unused value lane"
    );
}

/// A legal u32::MAX mapped value is present; an unmapped key is presence 0.
#[test]
fn map_reads_keep_legal_max_present_and_absence_distinct() {
    let _serial = serial();
    let program = test_program();
    let mut state = init_state(&program);
    assert_eq!(
        execute_batch(
            &mut state,
            &program,
            &[u32s(&[7, 11]), u32s(&[70, u32::MAX])],
            2,
            false
        ),
        OK
    );
    assert_eq!(map_get(&state, 0, 7), (OK, [1, 0, 0, 0, 70, 0, 0, 0]));
    assert_eq!(
        map_get(&state, 0, 11),
        (OK, [1, 0, 0, 0, 0xff, 0xff, 0xff, 0xff])
    );
    assert_eq!(map_get(&state, 0, 8), (OK, [0; 8]));
}

/// Membership: a stored element reads 1 and an absent one a strict 0 word.
#[test]
fn set_membership_reports_absence_as_a_strict_zero_word() {
    let _serial = serial();
    let init = [
        Opcode::SlotDef as u8,
        0,
        SlotType::HashSet as u8,
        8,
        0,
        Opcode::Halt as u8,
    ];
    let reduce = [Opcode::BatchSetInsert as u8, 0, 0];
    let program = program(1, 1, &init, &reduce);
    let mut state = init_state(&program);
    assert_eq!(
        execute_batch(&mut state, &program, &[u32s(&[5])], 1, false),
        OK
    );
    assert_eq!(set_contains(&state, 0, 5), (OK, [1, 0, 0, 0]));
    assert_eq!(set_contains(&state, 0, 6), (OK, [0, 0, 0, 0]));
}

/// Composite pairs: exact row lookup, and a live-cell walk that names exactly the
/// stored pairs, each with a present row, and ends at the capacity sentinel.
#[test]
fn struct_map2_rows_and_iteration_name_exactly_the_live_pairs() {
    let _serial = serial();
    let init = [
        Opcode::SlotStructMap2 as u8,
        0,
        SlotType::StructMap2 as u8,
        8,
        0,
        2,
        StructFieldType::UInt32 as u8,
        StructFieldType::UInt32 as u8,
        Opcode::Halt as u8,
    ];
    // Key lanes are columns 0 and 1; columns 2 and 3 feed fields 0 and 1.
    let reduce = [
        Opcode::BatchStructMap2UpsertLast as u8,
        0,
        0,
        1,
        2,
        2,
        0,
        3,
        1,
    ];
    let program = program(1, 4, &init, &reduce);
    let mut state = init_state(&program);
    let columns = [
        u32s(&[1, 3]),
        u32s(&[2, 4]),
        u32s(&[10, 30]),
        u32s(&[20, 40]),
    ];
    assert_eq!(execute_batch(&mut state, &program, &columns, 2, false), OK);
    let size = state.len() as u32;
    let capacity = meta_u32(&state, 0, SlotMetaOffset::CAPACITY);

    // Exact pair lookup: a stored pair names an in-image row; an absent pair is presence 0.
    let (status, record) = struct_map2_row(&state, 0, 1, 2);
    assert_eq!(status, OK);
    assert_eq!(word(&record, 0), 1);
    assert!(
        word(&record, 4) < size,
        "the row offset lies inside the image"
    );
    assert_eq!(struct_map2_row(&state, 0, 9, 9), (OK, [0; 8]));

    // Walk the live cells: every position names one stored pair with a present row.
    let (status, start) = struct_map2_iter_start(&state, 0);
    assert_eq!(status, OK);
    let mut pos = word(&start, 0);
    let mut pairs = Vec::new();
    for _ in 0..capacity {
        if pos == capacity {
            break;
        }
        let (status, key1) = struct_map2_iter_key1(&state, 0, pos);
        assert_eq!(status, OK);
        let (status, key2) = struct_map2_iter_key2(&state, 0, pos);
        assert_eq!(status, OK);
        let (key1, key2) = (word(&key1, 0), word(&key2, 0));
        let (status, row) = struct_map2_row(&state, 0, key1, key2);
        assert_eq!(status, OK);
        assert_eq!(word(&row, 0), 1, "a live pair has a present row");
        let (row_status, row_offset) = typed_read::<4>(&state, |s, n, out| unsafe {
            vm_struct_map2_iter_row_ptr(s, n, 0, pos, out)
        });
        assert_eq!(row_status, OK);
        assert_eq!(word(&row_offset, 0), word(&row, 4));
        pairs.push((key1, key2));
        let (status, next) = struct_map2_iter_next(&state, 0, pos);
        assert_eq!(status, OK);
        pos = word(&next, 0);
    }
    assert_eq!(pos, capacity, "the walk ends at the capacity sentinel");
    pairs.sort_unstable();
    assert_eq!(pairs, [(1, 2), (3, 4)]);

    // A position past capacity is an invalid iterator, never an end sentinel.
    let (status, record) = struct_map2_iter_next(&state, 0, capacity + 1);
    assert_eq!(status, INVALID_STATE);
    assert_eq!(record, [0xa5; 4], "a refused position writes nothing");
}

#[test]
fn mixed_ttl_export_refusal_preserves_native_extern_selection_and_output() {
    let _serial = serial();
    let mut init = vec![
        Opcode::SlotDef as u8,
        0,
        SlotType::HashMap as u8 | 0x10,
        8,
        0,
    ];
    init.extend_from_slice(&10f32.to_le_bytes());
    init.extend_from_slice(&0f32.to_le_bytes());
    init.extend_from_slice(&[2, 0, Opcode::Halt as u8]);
    let reduce = [Opcode::BatchMapUpsertLast as u8, 0, 0, 1];
    let program = program(1, 3, &init, &reduce);
    let mut state = init_state(&program);
    let size = state.len() as u32;
    let columns = [u32s(&[7]), u32s(&[30]), 1f64.to_le_bytes().to_vec()];
    assert_eq!(execute_batch(&mut state, &program, &columns, 1, false), OK);
    assert_eq!(unsafe { vm_undo_enable(state.as_mut_ptr(), size) }, OK);
    let checkpoint = unsafe { vm_undo_checkpoint(state.as_mut_ptr()) };
    let updates = [u32s(&[7]), u32s(&[70]), 1f64.to_le_bytes().to_vec()];
    assert_eq!(execute_batch(&mut state, &program, &updates, 1, true), OK);
    let paired_end = unsafe { vm_undo_checkpoint(state.as_mut_ptr()) };
    let mut output = [0xa5; 4];
    register(&output);
    assert_eq!(
        unsafe {
            vm_delta_export_segment(
                state.as_ptr(),
                size,
                checkpoint,
                paired_end,
                output.as_mut_ptr(),
            )
        },
        OK,
    );
    assert_eq!(word(&output, 0), paired_end - checkpoint);
    let selected = (
        vm_delta_export_len_bytes(),
        vm_delta_export_undo_ptr(),
        vm_delta_export_redo_ptr(),
    );
    assert_eq!(
        unsafe { vm_evict_all_expired(state.as_mut_ptr(), size, 100.0, 0) },
        OK
    );
    let end = unsafe { vm_undo_checkpoint(state.as_mut_ptr()) };
    output.fill(0xa5);
    let before = state.clone();
    assert_eq!(
        unsafe {
            vm_delta_export_segment(state.as_ptr(), size, checkpoint, end, output.as_mut_ptr())
        },
        ErrorCode::DeltaLaneMismatch as u32,
    );
    assert_eq!(output, [0xa5; 4], "refused export must not write a count");
    assert_eq!(state, before);
    assert_eq!(
        selected,
        (
            vm_delta_export_len_bytes(),
            vm_delta_export_undo_ptr(),
            vm_delta_export_redo_ptr(),
        ),
        "refused export preserves the prior selected cursor and buffer identities"
    );
}

/// A bitmap declaration, capacity 64. A TTL bitmap carries its tail: a ten-second
/// window, no grace, timestamp column 1, and no start-of unit.
fn bitmap_def(slot: u8, has_ttl: bool) -> Vec<u8> {
    let flags = SlotTypeFlags::new(SlotType::Bitmap, has_ttl, false, false, false).to_byte();
    let mut def = vec![Opcode::SlotDef as u8, slot, flags, 64, 0];
    if has_ttl {
        def.extend(10f32.to_le_bytes());
        def.extend(0f32.to_le_bytes());
        def.extend([1, 0]);
    }
    def
}

/// A refused program writes nothing: init and reset report `InvalidProgram`, and a
/// registered 4 KiB image keeps every byte it held.
fn assert_refused_without_writes(program: &[u8]) {
    register(program);
    let mut image = vec![0xa5u8; 4096];
    register(&image);
    let len = program.len() as u32;
    let refused = ErrorCode::InvalidProgram as u32;
    assert_eq!(
        unsafe { vm_init_state(image.as_mut_ptr(), program.as_ptr(), len) },
        refused
    );
    assert_eq!(
        unsafe { vm_reset_state(image.as_mut_ptr(), program.as_ptr(), len) },
        refused
    );
    assert!(
        image.iter().all(|byte| *byte == 0xa5),
        "a refused program wrote the image"
    );
}

/// A malformed program is named `MalformedProgram`; input and state stay untouched.
#[test]
fn sizing_names_a_malformed_program_and_leaves_input_and_state_untouched() {
    let _serial = serial();
    let mut program = test_program();
    program[PROGRAM_HASH_PREFIX as usize] = 0xde; // corrupt the magic
    let before = program.clone();
    let (status, record) = size_record(&program);
    assert_eq!(status, ErrorCode::InvalidProgram as u32);
    assert_eq!(words(&record), [0, 1, 0, 0], "kind 1 is MalformedProgram");
    assert_eq!(program, before, "sizing never writes its program");
    assert_refused_without_writes(&program);
}

/// A TTL source slot folded into a plain target is named by both slots, exactly.
#[test]
fn sizing_names_a_ttl_source_into_a_plain_target_and_leaves_input_and_state_untouched() {
    let _serial = serial();
    // Slot 0 is a plain bitmap target; slot 1 carries TTL and is the algebra source.
    let init = [
        bitmap_def(0, false),
        bitmap_def(1, true),
        vec![Opcode::Halt as u8],
    ]
    .concat();
    let program = program(2, 0, &init, &[Opcode::BatchBitmapOr as u8, 0, 1]);
    let before = program.clone();
    let (status, record) = size_record(&program);
    assert_eq!(status, ErrorCode::InvalidProgram as u32);
    assert_eq!(
        words(&record),
        [0, 2, 1, 0],
        "kind 2: source slot 1, target slot 0"
    );
    assert_eq!(program, before, "sizing never writes its program");
    assert_refused_without_writes(&program);
}

/// The record must lie wholly in registered memory and may not overlap the program.
/// Both negative controls refuse with `InvalidState` and write no byte.
#[test]
fn sizing_refuses_an_out_of_range_or_overlapping_record_and_writes_nothing() {
    let _serial = serial();
    let program = test_program();
    register(&program);

    // Only 12 bytes are registered, so the 16-byte record runs past the span.
    let mut short = [0xa5u8; 16];
    register(&short[..12]);
    let status = unsafe {
        vm_calculate_state_size(program.as_ptr(), program.len() as u32, short.as_mut_ptr())
    };
    assert_eq!(status, INVALID_STATE);
    assert_eq!(short, [0xa5; 16], "an out-of-range record writes no byte");

    // The record starts four bytes before the program's end, sharing its tail.
    let len = program.len();
    let mut arena = program.clone();
    arena.resize(len + 16, 0xa5);
    register(&arena);
    let before = arena.clone();
    let base = arena.as_mut_ptr();
    let status = unsafe { vm_calculate_state_size(base, len as u32, base.add(len - 4)) };
    assert_eq!(status, INVALID_STATE);
    assert_eq!(arena, before, "an overlapping record writes no byte");

    // Adjacent spans are disjoint: the record begins exactly where the program ends.
    let status = unsafe { vm_calculate_state_size(base, len as u32, base.add(len)) };
    assert_eq!(status, OK);
    assert_eq!(
        &arena[..len],
        program.as_slice(),
        "the program is never written"
    );
    let record = words(&arena[len..]);
    assert!(record[0] > 0, "an admitted program sizes a state");
    assert_eq!(record[1..], [0, 0, 0]);
}
