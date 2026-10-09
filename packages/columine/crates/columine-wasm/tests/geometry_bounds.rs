//! Actual native extern bounds and committed-boundary growth between undo windows.
//! Tests share the export runtime's statics, so each one takes the serial lock.
use columine_types::types::{
    ErrorCode, Opcode, PROGRAM_FORMAT_VERSION, PROGRAM_MAGIC, ProgramHeader, STATE_HEADER_SIZE,
    SlotMetaOffset, SlotType, StructFieldType,
};
use columine_vm::undo_log::{FLAT_UNDO_ENTRY_SIZE, FlatUndoEntry, FlatUndoOp};
use columine_vm::{bytes, state_init};
use columine_wasm::{
    __register_region, vm_calculate_grown_state_size, vm_delta_apply_rollforward_segment,
    vm_delta_required_state_size, vm_execute_batch, vm_grow_state, vm_init_state, vm_map_get,
    vm_set_contains, vm_struct_map_arena_field, vm_struct_map2_get_row_ptr,
    vm_struct_map2_iter_next, vm_struct_map2_iter_start, vm_undo_checkpoint, vm_undo_commit,
    vm_undo_enable, vm_undo_rollback,
};
use std::sync::{Mutex, MutexGuard};

fn program() -> Vec<u8> {
    let init = [
        Opcode::SlotDef as u8,
        0,
        SlotType::HashMap as u8,
        8,
        0,
        Opcode::Halt as u8,
    ];
    let reduce = [
        Opcode::BatchMapUpsertLast as u8,
        0,
        0,
        1,
        Opcode::Halt as u8,
    ];
    wire_program(1, 2, &init, &reduce)
}

/// One struct map (u32 key, VO field) built through the real init and batch
/// wire format; the VO column carries opaque bytes, never normalized.
fn vo_program() -> Vec<u8> {
    let init = [
        Opcode::SlotStructMap as u8,
        0,
        SlotType::StructMap as u8,
        4,
        0,
        2,
        StructFieldType::UInt32 as u8,
        StructFieldType::Vo as u8,
        Opcode::Halt as u8,
    ];
    // Key col 0; scalar col 1 -> field 0; CSR offsets col 2, values col 3 -> field 1.
    let reduce = [
        Opcode::BatchStructMapUpsertLast as u8,
        0,
        0,
        1,
        1,
        0,
        1,
        2,
        3,
        1,
    ];
    let header = ProgramHeader {
        magic: PROGRAM_MAGIC,
        version: PROGRAM_FORMAT_VERSION,
        num_callbacks: 0,
        flags: 0,
        num_slots: 1,
        num_inputs: 4,
        init_code_len: init.len() as u32,
        reduce_code_len: reduce.len() as u32,
    };
    let mut program = vec![0; 32];
    program.extend(header.to_wire_bytes());
    program.extend(init);
    program.extend(reduce);
    program
}

#[test]
fn actual_bounds_and_committed_growth_preserve_both_window_authorities() {
    let _serial = serial();
    let program = program();
    __register_region(program.as_ptr(), program.len());
    let size = state_init::calculate_state_size(&program, &[PROGRAM_MAGIC])
        .expect("valid program admission");
    let mut state = vec![0; size as usize];
    __register_region(state.as_ptr(), state.len());
    assert_eq!(
        unsafe { vm_init_state(state.as_mut_ptr(), program.as_ptr(), program.len() as u32) },
        ErrorCode::Ok as u32
    );
    // A committed key makes both speculative windows real replacements. The
    // rollback assertions compare the whole emitted image, not live entries:
    // the image is the authority, so any byte a rolled-back or refused window
    // leaves behind is a leaked write.
    let seed_key = 7u32.to_le_bytes();
    let seed_value = 30u32.to_le_bytes();
    let seed_columns = [seed_key.as_ptr(), seed_value.as_ptr()];
    let seed_lengths = [4u32, 4u32];
    __register_region(seed_key.as_ptr(), seed_key.len());
    __register_region(seed_value.as_ptr(), seed_value.len());
    __register_region(
        seed_columns.as_ptr().cast(),
        core::mem::size_of_val(&seed_columns),
    );
    __register_region(
        seed_lengths.as_ptr().cast(),
        core::mem::size_of_val(&seed_lengths),
    );
    assert_eq!(
        unsafe {
            vm_execute_batch(
                state.as_mut_ptr(),
                size,
                program.as_ptr(),
                program.len() as u32,
                seed_columns.as_ptr(),
                seed_lengths.as_ptr(),
                2,
                1,
            )
        },
        ErrorCode::Ok as u32
    );
    let initial = state.clone();
    assert_eq!(
        unsafe { vm_undo_enable(state.as_mut_ptr(), size) },
        ErrorCode::Ok as u32
    );
    let checkpoint = unsafe { vm_undo_checkpoint(state.as_mut_ptr()) };
    let keys = 7u32.to_le_bytes();
    let values = 70u32.to_le_bytes();
    let columns = [keys.as_ptr(), values.as_ptr()];
    let lengths = [4u32, 4u32];
    __register_region(keys.as_ptr(), keys.len());
    __register_region(values.as_ptr(), values.len());
    __register_region(columns.as_ptr().cast(), core::mem::size_of_val(&columns));
    __register_region(lengths.as_ptr().cast(), core::mem::size_of_val(&lengths));
    assert_eq!(
        unsafe {
            vm_execute_batch(
                state.as_mut_ptr(),
                size,
                program.as_ptr(),
                program.len() as u32,
                columns.as_ptr(),
                lengths.as_ptr(),
                2,
                1,
            )
        },
        ErrorCode::Ok as u32
    );
    let before_growth = state.clone();
    let mut output = [0xa5; 4];
    __register_region(output.as_ptr(), output.len());
    assert_eq!(
        unsafe { vm_calculate_grown_state_size(state.as_ptr(), size + 1, 0, output.as_mut_ptr()) },
        ErrorCode::InvalidState as u32
    );
    assert_eq!(output, [0xa5; 4]);
    assert_eq!(
        unsafe { vm_calculate_grown_state_size(state.as_ptr(), size, 0, state.as_mut_ptr()) },
        ErrorCode::InvalidState as u32
    );
    assert_eq!(state, before_growth);
    assert_eq!(
        unsafe { vm_calculate_grown_state_size(state.as_ptr(), size, 0, output.as_mut_ptr()) },
        ErrorCode::Ok as u32
    );
    let grown_size = u32::from_le_bytes(output);
    let mut grown = vec![0xa5; grown_size as usize];
    __register_region(grown.as_ptr(), grown.len());
    let untouched = grown.clone();
    let before_checkpoint = unsafe { vm_undo_checkpoint(state.as_mut_ptr()) };
    assert_eq!(
        unsafe { vm_grow_state(state.as_ptr(), size, grown.as_mut_ptr(), grown_size, 0) },
        ErrorCode::NeedsGrowth as u32
    );
    assert_eq!(grown, untouched);
    assert_eq!(state, before_growth);
    assert_eq!(
        unsafe { vm_undo_checkpoint(state.as_mut_ptr()) },
        before_checkpoint,
        "refused growth must not clear the active journal"
    );
    assert_eq!(
        unsafe { vm_undo_rollback(state.as_mut_ptr(), size, checkpoint, output.as_mut_ptr()) },
        ErrorCode::Ok as u32
    );
    assert_eq!(u32::from_le_bytes(output), size);
    assert_eq!(state, initial);
    unsafe { vm_undo_commit(state.as_mut_ptr(), checkpoint) };

    assert_eq!(
        unsafe { vm_grow_state(state.as_ptr(), size, grown.as_mut_ptr(), grown_size - 1, 0) },
        ErrorCode::InvalidState as u32
    );
    assert_eq!(grown, untouched);
    assert_eq!(
        unsafe { vm_grow_state(state.as_ptr(), size, state.as_mut_ptr(), size, 0) },
        ErrorCode::InvalidState as u32
    );
    assert_eq!(state, initial);
    grown.fill(0);
    assert_eq!(
        unsafe { vm_grow_state(state.as_ptr(), size, grown.as_mut_ptr(), grown_size, 0) },
        ErrorCode::Ok as u32
    );
    assert_eq!(
        state, initial,
        "committed growth must not alter the old authority"
    );
    assert_eq!(bytes::read_u32(&grown, 0), bytes::read_u32(&initial, 0));
    assert_eq!(
        bytes::read_u32(&grown, STATE_HEADER_SIZE + SlotMetaOffset::CAPACITY),
        2 * bytes::read_u32(&initial, STATE_HEADER_SIZE + SlotMetaOffset::CAPACITY)
    );
    assert_eq!(
        bytes::read_u32(&grown, STATE_HEADER_SIZE + SlotMetaOffset::SIZE),
        1
    );
    let committed_grown = grown.clone();
    assert_eq!(
        unsafe { vm_undo_enable(grown.as_mut_ptr(), grown_size) },
        ErrorCode::Ok as u32
    );
    let grown_checkpoint = unsafe { vm_undo_checkpoint(grown.as_mut_ptr()) };
    assert_eq!(
        unsafe {
            vm_execute_batch(
                grown.as_mut_ptr(),
                grown_size,
                program.as_ptr(),
                program.len() as u32,
                columns.as_ptr(),
                lengths.as_ptr(),
                2,
                1,
            )
        },
        ErrorCode::Ok as u32
    );
    let grown_before = grown.clone();
    assert_eq!(
        unsafe {
            vm_undo_rollback(
                grown.as_mut_ptr(),
                grown_size,
                u32::MAX,
                output.as_mut_ptr(),
            )
        },
        ErrorCode::InvalidState as u32
    );
    assert_eq!(grown, grown_before);
    assert_eq!(
        unsafe {
            vm_undo_rollback(
                grown.as_mut_ptr(),
                grown_size + 1,
                grown_checkpoint,
                output.as_mut_ptr(),
            )
        },
        ErrorCode::InvalidState as u32
    );
    assert_eq!(grown, grown_before);
    assert_eq!(
        unsafe {
            vm_undo_rollback(
                grown.as_mut_ptr(),
                grown_size,
                grown_checkpoint,
                output.as_mut_ptr(),
            )
        },
        ErrorCode::Ok as u32
    );
    assert_eq!(
        u32::from_le_bytes(output),
        grown_size,
        "rollback stays within its committed layout"
    );
    assert_eq!(grown, committed_grown);
    assert_eq!(state, initial);
    unsafe { vm_undo_commit(grown.as_mut_ptr(), grown_checkpoint) };

    // Use the actual journal serializer, not a duplicated operation-byte table.
    let invalid = FlatUndoEntry {
        op: FlatUndoOp::StateBytes,
        slot: 0,
        pad1: 1,
        pad2: 0,
        key: grown_size,
        prev_value: 0,
        aux: 0,
    };
    let mut segment = [0; FLAT_UNDO_ENTRY_SIZE as usize];
    invalid.write_to(&mut segment);
    __register_region(segment.as_ptr(), segment.len());
    output.fill(0xa5);
    assert_eq!(
        unsafe {
            vm_delta_required_state_size(
                segment.as_ptr(),
                segment.len() as u32,
                FLAT_UNDO_ENTRY_SIZE,
                grown_size,
                output.as_mut_ptr(),
            )
        },
        ErrorCode::InvalidState as u32
    );
    assert_eq!(output, [0xa5; 4]);
    assert_eq!(
        unsafe {
            vm_delta_apply_rollforward_segment(
                grown.as_mut_ptr(),
                grown_size,
                segment.as_ptr(),
                segment.len() as u32,
                FLAT_UNDO_ENTRY_SIZE,
                output.as_mut_ptr(),
            )
        },
        ErrorCode::InvalidState as u32
    );
    assert_eq!(
        grown, committed_grown,
        "an out-of-image journal span cannot expand or mutate the authority"
    );
    assert_eq!(output, [0xa5; 4]);

    // Struct-map arena reads borrow the image itself. Each refusal leaves the
    // record and the image untouched, and the extern never aliases its output.
    let vo = vo_program();
    __register_region(vo.as_ptr(), vo.len());
    let vo_size =
        state_init::calculate_state_size(&vo, &[PROGRAM_MAGIC]).expect("valid program admission");
    let mut vo_state = vec![0; vo_size as usize];
    __register_region(vo_state.as_ptr(), vo_state.len());
    assert_eq!(
        unsafe { vm_init_state(vo_state.as_mut_ptr(), vo.as_ptr(), vo.len() as u32) },
        ErrorCode::Ok as u32
    );
    assert_eq!(
        unsafe { vm_undo_enable(vo_state.as_mut_ptr(), vo_size) },
        ErrorCode::Ok as u32
    );
    let payload = [1u8, 2, 3];
    let keys = 7u32.to_le_bytes();
    let scalars = 70u32.to_le_bytes();
    let offsets = [0u32.to_le_bytes(), 3u32.to_le_bytes()].concat();
    let vo_columns = [
        keys.as_ptr(),
        scalars.as_ptr(),
        offsets.as_ptr(),
        payload.as_ptr(),
    ];
    let vo_lengths = [4u32, 4, 8, 3];
    __register_region(keys.as_ptr(), keys.len());
    __register_region(scalars.as_ptr(), scalars.len());
    __register_region(offsets.as_ptr(), offsets.len());
    __register_region(payload.as_ptr(), payload.len());
    __register_region(
        vo_columns.as_ptr().cast(),
        core::mem::size_of_val(&vo_columns),
    );
    __register_region(
        vo_lengths.as_ptr().cast(),
        core::mem::size_of_val(&vo_lengths),
    );
    assert_eq!(
        unsafe {
            vm_execute_batch(
                vo_state.as_mut_ptr(),
                vo_size,
                vo.as_ptr(),
                vo.len() as u32,
                vo_columns.as_ptr(),
                vo_lengths.as_ptr(),
                4,
                1,
            )
        },
        ErrorCode::Ok as u32
    );
    let mut record = [0xa5u8; 8];
    __register_region(record.as_ptr(), record.len());
    assert_eq!(
        unsafe {
            vm_struct_map_arena_field(vo_state.as_ptr(), vo_size, 0, 7, 1, record.as_mut_ptr())
        },
        ErrorCode::Ok as u32
    );
    let (offset, len) = (bytes::read_u32(&record, 0), bytes::read_u32(&record, 4));
    assert_eq!(len, payload.len() as u32);
    assert_eq!(
        &vo_state[offset as usize..(offset + len) as usize],
        &payload[..],
        "the record names the image's own arena bytes"
    );
    record.fill(0xa5);
    assert_eq!(
        unsafe {
            vm_struct_map_arena_field(vo_state.as_ptr(), vo_size, 0, 8, 1, record.as_mut_ptr())
        },
        ErrorCode::Ok as u32
    );
    assert_eq!(
        (bytes::read_u32(&record, 0), bytes::read_u32(&record, 4)),
        (u32::MAX, 0),
        "an absent key is an empty record"
    );
    record.fill(0xa5);
    assert_eq!(
        unsafe {
            vm_struct_map_arena_field(vo_state.as_ptr(), vo_size, 0, 7, 0, record.as_mut_ptr())
        },
        ErrorCode::InvalidProgram as u32,
        "a scalar field is not an arena"
    );
    assert_eq!(
        unsafe {
            vm_struct_map_arena_field(vo_state.as_ptr(), vo_size, 0, 7, 2, record.as_mut_ptr())
        },
        ErrorCode::InvalidProgram as u32,
        "a field past the descriptor is refused"
    );
    assert_eq!(record, [0xa5; 8]);
    let before = vo_state.clone();
    assert_eq!(
        unsafe {
            vm_struct_map_arena_field(
                vo_state.as_ptr(),
                vo_size,
                0,
                7,
                1,
                vo_state.as_mut_ptr().add(STATE_HEADER_SIZE as usize),
            )
        },
        ErrorCode::InvalidState as u32
    );
    assert_eq!(
        vo_state, before,
        "an output inside the image is refused before any write"
    );
    assert_eq!(
        unsafe {
            vm_struct_map_arena_field(
                vo_state.as_ptr(),
                STATE_HEADER_SIZE - 1,
                0,
                7,
                1,
                record.as_mut_ptr(),
            )
        },
        ErrorCode::InvalidState as u32
    );
    assert_eq!(record, [0xa5; 8]);
    for (corrupt_at, value) in [
        (STATE_HEADER_SIZE + SlotMetaOffset::CAPACITY, 3),
        (STATE_HEADER_SIZE + SlotMetaOffset::GRACE_SECONDS, 0),
    ] {
        let mut corrupt = vo_state.clone();
        __register_region(corrupt.as_ptr(), corrupt.len());
        bytes::write_u32(&mut corrupt, corrupt_at, value);
        assert_eq!(
            unsafe {
                vm_struct_map_arena_field(corrupt.as_ptr(), vo_size, 0, 7, 1, record.as_mut_ptr())
            },
            ErrorCode::InvalidState as u32,
            "corrupt image at {corrupt_at}"
        );
        assert_eq!(record, [0xa5; 8]);
    }
}

const OK: u32 = ErrorCode::Ok as u32;
const INVALID_STATE: u32 = ErrorCode::InvalidState as u32;
const INVALID_SLOT: u32 = ErrorCode::InvalidSlot as u32;

// The export runtime's statics are process-wide; every test in this binary takes this lock.
static SERIAL: Mutex<()> = Mutex::new(());
fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
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

/// Wire-format program with an explicit slot and input count.
fn wire_program(num_slots: u32, num_inputs: u32, init: &[u8], reduce: &[u8]) -> Vec<u8> {
    let header = ProgramHeader {
        magic: PROGRAM_MAGIC,
        version: PROGRAM_FORMAT_VERSION,
        num_callbacks: 0,
        flags: 0,
        num_slots,
        num_inputs,
        init_code_len: init.len() as u32,
        reduce_code_len: reduce.len() as u32,
    };
    let mut program = vec![0; 32];
    program.extend(header.to_wire_bytes());
    program.extend(init);
    program.extend(reduce);
    program
}

/// HASHSET capacity-8 slot with one insert over column 0.
fn set_program() -> Vec<u8> {
    let init = [
        Opcode::SlotDef as u8,
        0,
        SlotType::HashSet as u8,
        8,
        0,
        Opcode::Halt as u8,
    ];
    let reduce = [Opcode::BatchSetInsert as u8, 0, 0];
    wire_program(1, 1, &init, &reduce)
}

/// Empty BITMAP capacity-8 slot: every element is a real, valid false.
fn bitmap_program() -> Vec<u8> {
    let init = [
        Opcode::SlotDef as u8,
        0,
        SlotType::Bitmap as u8,
        8,
        0,
        Opcode::Halt as u8,
    ];
    wire_program(1, 0, &init, &[])
}

/// STRUCT_MAP2 capacity-8 slot with two u32 fields fed by columns 2 and 3.
fn composite_program() -> Vec<u8> {
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
    wire_program(1, 4, &init, &reduce)
}

/// Size and initialize a state image; the program and image stay registered.
fn init_state(program: &[u8]) -> Vec<u8> {
    register(program);
    let size = state_init::calculate_state_size(program, &[PROGRAM_MAGIC])
        .expect("valid program admission");
    let mut state = vec![0; size as usize];
    register(&state);
    assert_eq!(
        unsafe { vm_init_state(state.as_mut_ptr(), program.as_ptr(), program.len() as u32) },
        OK
    );
    state
}

/// Run one batch through the extern layer with every column and pointer table registered.
fn execute_batch(state: &mut [u8], program: &[u8], columns: &[Vec<u8>], batch_len: u32) -> u32 {
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
    unsafe {
        vm_execute_batch(
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

/// Every refusal names its typed cause and writes nothing. Each caller record is
/// seeded with 0xa5 and each image is compared whole after the refused call.
#[test]
fn typed_reads_refuse_malformed_geometry_before_any_write() {
    let _serial = serial();

    // Map: committed key 7 makes the valid read a real hit before any corruption.
    let map = program();
    let mut state = init_state(&map);
    let size = state.len() as u32;
    assert_eq!(
        execute_batch(&mut state, &map, &[u32s(&[7]), u32s(&[70])], 1),
        OK
    );
    let mut out = [0xa5u8; 8];
    register(&out);
    assert_eq!(
        unsafe { vm_map_get(state.as_ptr(), size, 0, 7, out.as_mut_ptr()) },
        OK
    );
    assert_eq!(out, [1, 0, 0, 0, 70, 0, 0, 0]);
    let image = state.clone();

    // Bad slot: the image holds one slot.
    out.fill(0xa5);
    assert_eq!(
        unsafe { vm_map_get(state.as_ptr(), size, 1, 7, out.as_mut_ptr()) },
        INVALID_SLOT
    );
    assert_eq!(out, [0xa5; 8]);
    assert_eq!(state, image);

    // Truncated extent: slot 0's data ends exactly at the image end, so one byte
    // short leaves the slot running past the image it is read from.
    out.fill(0xa5);
    assert_eq!(
        unsafe { vm_map_get(state.as_ptr(), size - 1, 0, 7, out.as_mut_ptr()) },
        INVALID_STATE
    );
    assert_eq!(out, [0xa5; 8]);
    assert_eq!(state, image);

    // Output inside the image: refused before any write, so the image is untouched.
    assert_eq!(
        unsafe {
            vm_map_get(
                state.as_ptr(),
                size,
                0,
                7,
                state.as_mut_ptr().add(STATE_HEADER_SIZE as usize),
            )
        },
        INVALID_STATE
    );
    assert_eq!(state, image);

    // Capacity 3 in an otherwise valid image: refused, and the copy is unchanged.
    let mut corrupt = image.clone();
    register(&corrupt);
    bytes::write_u32(
        &mut corrupt,
        STATE_HEADER_SIZE + SlotMetaOffset::CAPACITY,
        3,
    );
    let corrupt_image = corrupt.clone();
    out.fill(0xa5);
    assert_eq!(
        unsafe { vm_map_get(corrupt.as_ptr(), size, 0, 7, out.as_mut_ptr()) },
        INVALID_STATE
    );
    assert_eq!(out, [0xa5; 8]);
    assert_eq!(corrupt, corrupt_image);

    // HashSet: a valid stored element is 1; a corrupted capacity is refused.
    let set = set_program();
    let mut set_state = init_state(&set);
    let set_size = set_state.len() as u32;
    assert_eq!(execute_batch(&mut set_state, &set, &[u32s(&[5])], 1), OK);
    let mut bit = [0xa5u8; 4];
    register(&bit);
    assert_eq!(
        unsafe { vm_set_contains(set_state.as_ptr(), set_size, 0, 5, bit.as_mut_ptr()) },
        OK
    );
    assert_eq!(bit, [1, 0, 0, 0]);
    bit.fill(0xa5);
    let mut corrupt_set = set_state.clone();
    register(&corrupt_set);
    bytes::write_u32(
        &mut corrupt_set,
        STATE_HEADER_SIZE + SlotMetaOffset::CAPACITY,
        3,
    );
    let corrupt_set_image = corrupt_set.clone();
    assert_eq!(
        unsafe { vm_set_contains(corrupt_set.as_ptr(), set_size, 0, 5, bit.as_mut_ptr()) },
        INVALID_STATE
    );
    assert_eq!(bit, [0xa5; 4]);
    assert_eq!(corrupt_set, corrupt_set_image);

    // BITMAP: an empty valid image is a real strict false; a corrupted capacity is refused.
    let bitmap = bitmap_program();
    let bitmap_state = init_state(&bitmap);
    let bitmap_size = bitmap_state.len() as u32;
    assert_eq!(
        unsafe { vm_set_contains(bitmap_state.as_ptr(), bitmap_size, 0, 5, bit.as_mut_ptr()) },
        OK
    );
    assert_eq!(bit, [0, 0, 0, 0]);
    bit.fill(0xa5);
    let mut corrupt_bitmap = bitmap_state.clone();
    register(&corrupt_bitmap);
    bytes::write_u32(
        &mut corrupt_bitmap,
        STATE_HEADER_SIZE + SlotMetaOffset::CAPACITY,
        3,
    );
    let corrupt_bitmap_image = corrupt_bitmap.clone();
    assert_eq!(
        unsafe { vm_set_contains(corrupt_bitmap.as_ptr(), bitmap_size, 0, 5, bit.as_mut_ptr()) },
        INVALID_STATE
    );
    assert_eq!(bit, [0xa5; 4]);
    assert_eq!(corrupt_bitmap, corrupt_bitmap_image);

    // STRUCT_MAP2: one committed pair; corrupt capacity, bad slot, and a position past
    // capacity are each refused with the image and the record intact.
    let composite = composite_program();
    let mut pair_state = init_state(&composite);
    let pair_size = pair_state.len() as u32;
    assert_eq!(
        execute_batch(
            &mut pair_state,
            &composite,
            &[u32s(&[1]), u32s(&[2]), u32s(&[10]), u32s(&[20])],
            1
        ),
        OK
    );
    let pair_image = pair_state.clone();
    let mut row = [0xa5u8; 8];
    register(&row);
    assert_eq!(
        unsafe {
            vm_struct_map2_get_row_ptr(pair_state.as_ptr(), pair_size, 0, 1, 2, row.as_mut_ptr())
        },
        OK
    );
    assert_eq!(bytes::read_u32(&row, 0), 1);
    row.fill(0xa5);
    let mut corrupt_pair = pair_image.clone();
    register(&corrupt_pair);
    bytes::write_u32(
        &mut corrupt_pair,
        STATE_HEADER_SIZE + SlotMetaOffset::CAPACITY,
        3,
    );
    let corrupt_pair_image = corrupt_pair.clone();
    assert_eq!(
        unsafe {
            vm_struct_map2_get_row_ptr(corrupt_pair.as_ptr(), pair_size, 0, 1, 2, row.as_mut_ptr())
        },
        INVALID_STATE
    );
    assert_eq!(row, [0xa5; 8]);
    assert_eq!(corrupt_pair, corrupt_pair_image);
    assert_eq!(
        unsafe {
            vm_struct_map2_get_row_ptr(pair_state.as_ptr(), pair_size, 1, 1, 2, row.as_mut_ptr())
        },
        INVALID_SLOT
    );
    assert_eq!(row, [0xa5; 8]);
    let mut next = [0xa5u8; 4];
    register(&next);
    let capacity = bytes::read_u32(&pair_state, STATE_HEADER_SIZE + SlotMetaOffset::CAPACITY);
    assert_eq!(
        unsafe {
            vm_struct_map2_iter_next(
                pair_state.as_ptr(),
                pair_size,
                0,
                capacity + 1,
                next.as_mut_ptr(),
            )
        },
        INVALID_STATE
    );
    assert_eq!(next, [0xa5; 4]);
    let mut start = [0xa5u8; 4];
    register(&start);
    assert_eq!(
        unsafe { vm_struct_map2_iter_start(pair_state.as_ptr(), pair_size, 0, start.as_mut_ptr()) },
        OK
    );
    assert_eq!(pair_state, pair_image);
}
