//! `columine.wasm` — the generic reducer-VM export layer (VM without RETE).
//!
//! The export surface is `columine_types::wasm_abi::COLUMINE_VM_EXPORTS` plus
//! memory; `tests/export_checklist.rs` holds both the artifact and the
//! TypeScript host's bindings to that one table, so the count lives in exactly
//! one place. Every export here is reachable from the published
//! `@smoothbricks/columine` host; container iteration and the roaring-bitmap
//! entry points were removed because nothing bound them, and an export no
//! caller can reach is surface that only has to be kept working.
//!
//! The published npm package ships this artifact as `dist/columine.wasm`
//! (`"./wasm"` export).
//!
//! Statics policy: single-threaded wasm assumed (one caller per instance).
//! Pointer policy: wasm32 pointers are bounded by linear memory; native tests
//! register regions explicitly.
// WHY one crate-level allow instead of an identical `# Safety` section on
// every export: they all share this module's pointer-bounding policy — the
// caller owns the memory for the duration of the call, wasm bounds it with
// linear memory, native tests with registered regions. There is no export
// here with a different contract.
#![allow(clippy::missing_safety_doc)]

use columine_types::DEFAULT_ACCEPTED_PROGRAM_MAGICS;
use columine_types::types::ErrorCode;
use columine_vm::state_init;
use columine_vm::vm::{self as vmops, Vm};

// =============================================================================
// Runtime statics
// =============================================================================

struct Runtime {
    vm: Vm,
    /// Serialized delta-segment lanes. Rust materializes them after export so
    /// the `_ptr` results remain stable until the next export.
    delta_undo: Vec<u8>,
    delta_redo: Vec<u8>,
    /// Count from the last successful `vm_evict_all_expired` call.
    last_evicted_count: u32,
    #[cfg(not(target_arch = "wasm32"))]
    regions: Vec<(usize, usize)>,
}
impl Runtime {
    fn new() -> Self {
        Runtime {
            vm: Vm::default(),
            delta_undo: Vec::new(),
            delta_redo: Vec::new(),
            last_evicted_count: 0,
            #[cfg(not(target_arch = "wasm32"))]
            regions: Vec::new(),
        }
    }
}

static mut RUNTIME: Option<Runtime> = None;

/// Single-threaded wasm: one mutable global, initialized on first use.
#[allow(static_mut_refs)]
fn rt() -> &'static mut Runtime {
    unsafe { RUNTIME.get_or_insert_with(Runtime::new) }
}

// =============================================================================
// Pointer bounding
// =============================================================================

#[cfg(target_arch = "wasm32")]
fn bound_of(ptr: usize) -> usize {
    let end = core::arch::wasm32::memory_size(0) * 65536;
    debug_assert!(ptr <= end, "pointer past linear memory");
    end
}

/// Native builds are tests-only for this crate (the npm package ships only
/// the wasm artifact — no columine dylib lane): strict region registry.
#[cfg(not(target_arch = "wasm32"))]
fn bound_of(ptr: usize) -> usize {
    rt().regions
        .iter()
        .find(|(base, len)| ptr >= *base && ptr < base + len)
        .map(|(base, len)| base + len)
        .unwrap_or_else(|| {
            panic!("native caller must __register_region the buffer containing {ptr:#x}")
        })
}

/// Native tests register the buffers they pass by raw pointer; wasm32 needs no
/// registration because linear memory supplies the bound.
#[doc(hidden)]
pub fn __register_region(ptr: *const u8, len: usize) {
    #[cfg(not(target_arch = "wasm32"))]
    {
        let base = ptr as usize;
        let rt = rt();
        rt.regions.retain(|(b, _)| *b != base);
        rt.regions.push((base, len));
    }
    #[cfg(target_arch = "wasm32")]
    {
        let _ = (ptr, len);
    }
}

/// # Safety
/// `base` must point into memory the caller owns for the duration of the call.
unsafe fn state_mut<'a>(base: *mut u8) -> &'a mut [u8] {
    let start = base as usize;
    unsafe { core::slice::from_raw_parts_mut(base, bound_of(start) - start) }
}

unsafe fn state_ref<'a>(base: *const u8) -> &'a [u8] {
    let start = base as usize;
    unsafe { core::slice::from_raw_parts(base, bound_of(start) - start) }
}

unsafe fn buf<'a>(ptr: *const u8, len: u32) -> &'a [u8] {
    if len == 0 {
        return &[];
    }
    unsafe { core::slice::from_raw_parts(ptr, len as usize) }
}

unsafe fn buf_mut<'a, T>(ptr: *mut T, len: u32) -> &'a mut [T] {
    if len == 0 {
        return &mut [];
    }
    unsafe { core::slice::from_raw_parts_mut(ptr, len as usize) }
}

/// Column-pointer array → bounded slices (each column is bounded like a state
/// pointer).
unsafe fn cols_vec<'a>(col_ptrs: *const *const u8, num_cols: u32) -> Vec<&'a [u8]> {
    let mut cols = Vec::with_capacity(num_cols as usize);
    for i in 0..num_cols as usize {
        let p = unsafe { *col_ptrs.add(i) };
        cols.push(unsafe { state_ref(p) });
    }
    cols
}

fn err_code(r: Result<(), ErrorCode>) -> u32 {
    match r {
        Ok(()) => ErrorCode::Ok as u32,
        Err(e) => e as u32,
    }
}

// =============================================================================
// State lifecycle and growth exports
// =============================================================================

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_calculate_state_size(program_ptr: *const u8, program_len: u32) -> u32 {
    state_init::calculate_state_size(
        unsafe { buf(program_ptr, program_len) },
        DEFAULT_ACCEPTED_PROGRAM_MAGICS,
    )
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_init_state(
    state_ptr: *mut u8,
    program_ptr: *const u8,
    program_len: u32,
) -> u32 {
    let program = unsafe { buf(program_ptr, program_len) };
    let size = state_init::calculate_state_size(program, DEFAULT_ACCEPTED_PROGRAM_MAGICS);
    __register_region(state_ptr, size as usize);
    let state = unsafe { buf_mut(state_ptr, size) };
    err_code(state_init::init_state(
        state,
        program,
        DEFAULT_ACCEPTED_PROGRAM_MAGICS,
    ))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_reset_state(
    state_ptr: *mut u8,
    program_ptr: *const u8,
    program_len: u32,
) -> u32 {
    let program = unsafe { buf(program_ptr, program_len) };
    let size = state_init::calculate_state_size(program, DEFAULT_ACCEPTED_PROGRAM_MAGICS);
    let state = unsafe { buf_mut(state_ptr, size) };
    err_code(state_init::reset_state(
        state,
        program,
        DEFAULT_ACCEPTED_PROGRAM_MAGICS,
    ))
}

#[unsafe(no_mangle)]
pub extern "C" fn vm_get_needs_growth_slot() -> u32 {
    state_init::needs_growth_slot()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_calculate_grown_state_size(
    old_state_ptr: *const u8,
    _program_ptr: *const u8,
    _program_len: u32,
    grown_slot_idx: u32,
) -> u32 {
    state_init::calculate_grown_state_size(unsafe { state_ref(old_state_ptr) }, grown_slot_idx)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_grow_state(
    old_state_ptr: *const u8,
    new_state_ptr: *mut u8,
    _program_ptr: *const u8,
    _program_len: u32,
    grown_slot_idx: u32,
) -> u32 {
    let old_state = unsafe { state_ref(old_state_ptr) };
    let new_size = state_init::calculate_grown_state_size(old_state, grown_slot_idx);
    __register_region(new_state_ptr, new_size as usize);
    let new_state = unsafe { buf_mut(new_state_ptr, new_size) };
    match state_init::grow_state(old_state, new_state, grown_slot_idx) {
        Ok(()) => ErrorCode::Ok as u32,
        Err(e) => e as u32,
    }
}

// =============================================================================
// Batch execution exports
// =============================================================================

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_execute_batch(
    state_base: *mut u8,
    program_ptr: *const u8,
    program_len: u32,
    col_ptrs_ptr: *const *const u8,
    num_cols: u32,
    batch_len: u32,
) -> u32 {
    let state = unsafe { state_mut(state_base) };
    let program = unsafe { buf(program_ptr, program_len) };
    let cols = unsafe { cols_vec(col_ptrs_ptr, num_cols) };
    rt().vm.execute_batch(state, program, &cols, batch_len)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_execute_batch_delta(
    state_base: *mut u8,
    program_ptr: *const u8,
    program_len: u32,
    col_ptrs_ptr: *const *const u8,
    num_cols: u32,
    batch_len: u32,
) -> u32 {
    let state = unsafe { state_mut(state_base) };
    let program = unsafe { buf(program_ptr, program_len) };
    let cols = unsafe { cols_vec(col_ptrs_ptr, num_cols) };
    rt().vm
        .execute_batch_delta(state, program, &cols, batch_len)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_evict_all_expired(state_base: *mut u8, now: f64) -> u32 {
    let state = unsafe { state_mut(state_base) };
    let runtime = rt();
    match runtime.vm.evict_all_expired(state, now) {
        Ok(count) => {
            runtime.last_evicted_count = count;
            ErrorCode::Ok as u32
        }
        Err(error) => {
            runtime.last_evicted_count = 0;
            error as u32
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn vm_get_evicted_count() -> u32 {
    rt().last_evicted_count
}

// =============================================================================
// Map/set/struct-map reads and iteration exports
// =============================================================================

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_map_get(
    state_base: *mut u8,
    slot_offset: u32,
    capacity: u32,
    key: u32,
) -> u32 {
    vmops::vm_map_get(unsafe { state_ref(state_base) }, slot_offset, capacity, key)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_set_contains(
    state_base: *mut u8,
    slot_offset: u32,
    capacity: u32,
    elem: u32,
) -> u32 {
    let r = rt();
    u32::from(vmops::vm_set_contains(
        &mut r.vm.bitmap_env,
        unsafe { state_ref(state_base) },
        slot_offset,
        capacity,
        elem,
    ))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_struct_map2_get_row_ptr(
    state_base_ptr: *const u8,
    slot_offset: u32,
    capacity: u32,
    num_fields: u32,
    row_size: u32,
    key1: u32,
    key2: u32,
) -> u32 {
    vmops::vm_struct_map2_get_row_ptr(
        unsafe { state_ref(state_base_ptr) },
        slot_offset,
        capacity,
        num_fields,
        row_size,
        key1,
        key2,
    )
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_struct_map2_iter_start(
    state_base_ptr: *const u8,
    slot_offset: u32,
    capacity: u32,
    num_fields: u32,
) -> u32 {
    vmops::vm_struct_map2_iter_start(
        unsafe { state_ref(state_base_ptr) },
        slot_offset,
        capacity,
        num_fields,
    )
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_struct_map2_iter_next(
    state_base_ptr: *const u8,
    slot_offset: u32,
    capacity: u32,
    num_fields: u32,
    current: u32,
) -> u32 {
    vmops::vm_struct_map2_iter_next(
        unsafe { state_ref(state_base_ptr) },
        slot_offset,
        capacity,
        num_fields,
        current,
    )
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_struct_map2_iter_key1(
    state_base_ptr: *const u8,
    slot_offset: u32,
    num_fields: u32,
    pos: u32,
) -> u32 {
    vmops::vm_struct_map2_iter_key1(
        unsafe { state_ref(state_base_ptr) },
        slot_offset,
        num_fields,
        pos,
    )
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_struct_map2_iter_key2(
    state_base_ptr: *const u8,
    slot_offset: u32,
    capacity: u32,
    num_fields: u32,
    pos: u32,
) -> u32 {
    vmops::vm_struct_map2_iter_key2(
        unsafe { state_ref(state_base_ptr) },
        slot_offset,
        capacity,
        num_fields,
        pos,
    )
}

// =============================================================================
// Undo, checkpoint, and delta-segment exports
// =============================================================================

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_undo_enable(state_base: *mut u8, state_size: u32) {
    __register_region(state_base, state_size as usize);
    let state = unsafe { buf(state_base as *const u8, state_size) };
    rt().vm.undo_enable(state);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_undo_checkpoint(_state_base: *mut u8) -> u32 {
    rt().vm.undo_checkpoint()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_undo_rollback(state_base: *mut u8, checkpoint_pos: u32) {
    let state = unsafe { state_mut(state_base) };
    rt().vm.undo_rollback(state, checkpoint_pos);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_undo_commit(_state_base: *mut u8, _checkpoint_pos: u32) {
    rt().vm.undo_commit();
}

#[unsafe(no_mangle)]
pub extern "C" fn vm_undo_has_overflow() -> u32 {
    u32::from(rt().vm.undo_has_overflow())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_delta_export_segment(
    _state_base: *mut u8,
    from_pos: u32,
    to_pos: u32,
) -> u32 {
    let r = rt();
    let count = r.vm.delta_export_segment(from_pos, to_pos);
    // Materialize both lanes so the returned pointers remain stable until the
    // next export.
    r.delta_undo = r.vm.delta_export_undo_bytes();
    r.delta_redo = r.vm.delta_export_redo_bytes();
    count
}

#[unsafe(no_mangle)]
pub extern "C" fn vm_delta_export_undo_ptr() -> u32 {
    rt().delta_undo.as_ptr() as usize as u32
}

#[unsafe(no_mangle)]
pub extern "C" fn vm_delta_export_redo_ptr() -> u32 {
    rt().delta_redo.as_ptr() as usize as u32
}

#[unsafe(no_mangle)]
pub extern "C" fn vm_delta_export_len_bytes() -> u32 {
    rt().vm.delta_export_len_bytes()
}

#[unsafe(no_mangle)]
pub extern "C" fn vm_delta_export_entry_size() -> u32 {
    columine_vm::undo_log::FLAT_UNDO_ENTRY_SIZE
}

#[unsafe(no_mangle)]
pub extern "C" fn vm_delta_export_overflow() -> u32 {
    u32::from(rt().vm.delta_export_overflow())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_delta_apply_rollback_segment(
    state_base: *mut u8,
    undo_segment_ptr: *const u8,
    segment_len_bytes: u32,
    entry_size: u32,
) {
    let state = unsafe { state_mut(state_base) };
    let segment = unsafe { buf(undo_segment_ptr, segment_len_bytes) };
    rt().vm
        .delta_apply_rollback_segment(state, segment, entry_size);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_delta_apply_rollforward_segment(
    state_base: *mut u8,
    redo_segment_ptr: *const u8,
    segment_len_bytes: u32,
    entry_size: u32,
) {
    let state = unsafe { state_mut(state_base) };
    let segment = unsafe { buf(redo_segment_ptr, segment_len_bytes) };
    rt().vm
        .delta_apply_rollforward_segment(state, segment, entry_size);
}

