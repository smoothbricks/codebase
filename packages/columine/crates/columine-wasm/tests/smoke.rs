//! Native smoke tests THROUGH the extern layer: raw pointers, the region
//! registry, and the binary-root statics — the machinery the wasm artifact
//! ships. Semantics are pinned by the library crates' suites; these tests
//! pin the wrapper plumbing (bounding, statics routing, packing).
//!
//! `unsafe` throughout is the point: this exercises the exact C ABI calls
//! the TS backends make.

use columine_types::types::{SLOT_META_SIZE, STATE_HEADER_SIZE, SlotMetaOffset};
use columine_wasm::{
    __register_region, vm_calculate_state_size, vm_delta_export_entry_size,
    vm_delta_export_len_bytes, vm_delta_export_segment, vm_delta_export_undo_ptr, vm_execute_batch,
    vm_init_state, vm_map_get, vm_rbmp_algebra_result_len, vm_rbmp_and,
    vm_rbmp_cardinality_serialized, vm_undo_checkpoint, vm_undo_enable, vm_undo_has_overflow,
    vm_undo_rollback,
};
use std::sync::{Mutex, MutexGuard};

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

/// vm_test.zig:82 `buildTestProgram` — HASHMAP(LAST) cap 16 + AGG COUNT.
fn test_program() -> Vec<u8> {
    let mut prog = vec![0u8; 32];
    prog.extend([0x41, 0x58, 0x45, 0x31, 1, 0, 2, 2, 0, 0]);
    let mut init: Vec<u8> = [[0x10, 0, 0x00, 16, 0], [0x10, 1, 0x02, 2, 0]].concat();
    init.push(0);
    let reduce = [0x22u8, 0, 0, 1, 0x41, 1];
    prog.extend((init.len() as u16).to_le_bytes());
    prog.extend((reduce.len() as u16).to_le_bytes());
    prog.extend_from_slice(&init);
    prog.extend_from_slice(&reduce);
    prog
}

fn register(buf: &[u8]) {
    __register_region(buf.as_ptr(), buf.len());
}

#[test]
fn init_execute_read_undo_round_trip() {
    let _serial = serial();
    let program = test_program();
    register(&program);
    let size = unsafe { vm_calculate_state_size(program.as_ptr(), program.len() as u32) };
    assert!(size > 0);

    let mut state = vec![0u8; size as usize];
    let rc = unsafe { vm_init_state(state.as_mut_ptr(), program.as_ptr(), program.len() as u32) };
    assert_eq!(rc, 0);

    // Batch: keys [7, 9], values [70, 90].
    let keys: Vec<u32> = vec![7, 9];
    let vals: Vec<u32> = vec![70, 90];
    let key_bytes: Vec<u8> = keys.iter().flat_map(|k| k.to_le_bytes()).collect();
    let val_bytes: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
    register(&key_bytes);
    register(&val_bytes);
    let cols: Vec<*const u8> = vec![key_bytes.as_ptr(), val_bytes.as_ptr()];

    unsafe { vm_undo_enable(state.as_mut_ptr(), size) };
    let cp = unsafe { vm_undo_checkpoint(state.as_mut_ptr()) };

    let rc = unsafe {
        vm_execute_batch(
            state.as_mut_ptr(),
            program.as_ptr(),
            program.len() as u32,
            cols.as_ptr(),
            2,
            2,
        )
    };
    assert_eq!(rc, 0);
    let (off, cap) = (
        meta_u32(&state, 0, SlotMetaOffset::OFFSET),
        meta_u32(&state, 0, SlotMetaOffset::CAPACITY),
    );
    assert_eq!(unsafe { vm_map_get(state.as_mut_ptr(), off, cap, 7) }, 70);
    assert_eq!(unsafe { vm_map_get(state.as_mut_ptr(), off, cap, 9) }, 90);
    assert_eq!(vm_undo_has_overflow(), 0);

    // Delta lane is empty for a non-delta batch; the undo lane has entries.
    let exported = unsafe { vm_delta_export_segment(state.as_mut_ptr(), 0, u32::MAX) };
    assert_eq!(exported, 0, "non-delta batch exports no delta pairs");
    assert_eq!(vm_delta_export_entry_size(), 24);
    assert_eq!(vm_delta_export_len_bytes(), 0);
    let _ = vm_delta_export_undo_ptr();

    unsafe { vm_undo_rollback(state.as_mut_ptr(), cp) };
    assert_eq!(
        unsafe { vm_map_get(state.as_mut_ptr(), off, cap, 7) },
        u32::MAX,
        "rollback erases the insert (EMPTY_KEY sentinel)"
    );
}

#[test]
fn rbmp_serialized_ops_route_through_the_env() {
    let _serial = serial();
    // Empty ∧ empty → OK (0), empty algebra result.
    assert_eq!(
        unsafe { vm_rbmp_and(core::ptr::null(), 0, core::ptr::null(), 0) },
        0
    );
    assert_eq!(vm_rbmp_algebra_result_len(), 0);
    assert_eq!(
        unsafe { vm_rbmp_cardinality_serialized(core::ptr::null(), 0) },
        0
    );
}
