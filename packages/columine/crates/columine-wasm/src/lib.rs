//! `columine.wasm` — the generic reducer-VM export layer (VM without RETE).
//!
//! The export surface is `columine_types::wasm_abi::COLUMINE_VM_EXPORTS` plus
//! memory; `tests/export_checklist.rs` holds both the artifact and the
//! TypeScript host's bindings to that one table, so the count lives in exactly
//! one place. Every export here is reachable from the published
//! `@smoothbricks/columine` host; container iteration and the bitmap
//! entry points were removed because nothing bound them, and an export no
//! caller can reach is surface that only has to be kept working.
//!
//! The published npm package ships this artifact as `dist/columine.wasm`
//! (`"./wasm"` export).
//!
//! Statics policy: single-threaded wasm assumed (one caller per instance).
//! Pointer policy: wasm32 pointers are bounded by linear memory; native tests
//! register regions explicitly.
//! State reads and mutations receive the actual image length, never a heap suffix.
//! Growth inputs and destinations, and mutable output records, must be disjoint.
//! Grown size, rollback and delta application return `ErrorCode` and write a
//! little-endian four-byte extent only on success. Sizing returns its status
//! first, then a 16-byte record of four words (see `vm_calculate_state_size`).
//! Undo and delta replay keep
//! the actual image extent unchanged. Growth is routed through `Vm` and refuses
//! enabled undo; the caller must roll back and end the full speculative window,
//! grow its committed image, then re-enable undo and retry the whole window.
// WHY one crate-level allow instead of an identical `# Safety` section on
// every export: they all share this module's pointer-bounding policy — the
// caller owns the memory for the duration of the call, wasm bounds it with
// linear memory, native tests with registered regions. There is no export
// here with a different contract.
#![allow(clippy::missing_safety_doc)]

use columine_types::DEFAULT_ACCEPTED_PROGRAM_MAGICS;
use columine_types::types::{ErrorCode, STATE_HEADER_SIZE};
use columine_vm::bytes;
use columine_vm::state_init::{self, SIZING_RECORD_LEN, sizing_record};
use columine_vm::vm::{CheckedMap, CheckedSet, CheckedStructMap2, Vm};

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
fn range_is_bounded(ptr: usize, len: usize) -> bool {
    ptr != 0
        && ptr
            .checked_add(len)
            .is_some_and(|end| end <= core::arch::wasm32::memory_size(0) * 65536)
}

#[cfg(not(target_arch = "wasm32"))]
fn range_is_bounded(ptr: usize, len: usize) -> bool {
    ptr != 0
        && ptr.checked_add(len).is_some_and(|end| {
            rt().regions.iter().any(|(base, size)| {
                base.checked_add(*size)
                    .is_some_and(|limit| ptr >= *base && end <= limit)
            })
        })
}

/// Native tests register actual owned allocations, never inferred extents.
#[doc(hidden)]
pub fn __register_region(ptr: *const u8, len: usize) {
    #[cfg(not(target_arch = "wasm32"))]
    {
        let base = ptr as usize;
        let runtime = rt();
        runtime
            .regions
            .retain(|(registered, _)| *registered != base);
        runtime.regions.push((base, len));
    }
    #[cfg(target_arch = "wasm32")]
    {
        let _ = (ptr, len);
    }
}

fn disjoint(a: *const u8, a_len: u32, b: *const u8, b_len: u32) -> bool {
    let a = a as usize;
    let b = b as usize;
    a.checked_add(a_len as usize)
        .zip(b.checked_add(b_len as usize))
        .is_some_and(|(a_end, b_end)| a_end <= b || b_end <= a)
}

unsafe fn bounded_buf<'a>(ptr: *const u8, len: u32) -> Result<&'a [u8], ErrorCode> {
    if len == 0 {
        return Ok(&[]);
    }
    if !range_is_bounded(ptr as usize, len as usize) {
        return Err(ErrorCode::ColumnUnderrun);
    }
    Ok(unsafe { core::slice::from_raw_parts(ptr, len as usize) })
}

unsafe fn bounded_state_mut<'a>(ptr: *mut u8, len: u32) -> Result<&'a mut [u8], ErrorCode> {
    if len < STATE_HEADER_SIZE || !range_is_bounded(ptr as usize, len as usize) {
        return Err(ErrorCode::InvalidState);
    }
    Ok(unsafe { core::slice::from_raw_parts_mut(ptr, len as usize) })
}

unsafe fn write_extent(out: *mut u8, result: Result<u32, ErrorCode>) -> u32 {
    match result {
        Ok(size) => {
            let output = unsafe { core::slice::from_raw_parts_mut(out, 4) };
            bytes::write_u32(output, 0, size);
            ErrorCode::Ok as u32
        }
        Err(error) => error as u32,
    }
}

unsafe fn cols_vec<'a>(
    col_ptrs: *const *const u8,
    col_lengths: *const u32,
    num_cols: u32,
) -> Result<Vec<&'a [u8]>, ErrorCode> {
    let count = num_cols as usize;
    if count != 0
        && (!range_is_bounded(
            col_ptrs as usize,
            count
                .checked_mul(core::mem::size_of::<*const u8>())
                .ok_or(ErrorCode::ColumnUnderrun)?,
        ) || !range_is_bounded(
            col_lengths as usize,
            count
                .checked_mul(core::mem::size_of::<u32>())
                .ok_or(ErrorCode::ColumnUnderrun)?,
        ) || !(col_ptrs as usize).is_multiple_of(core::mem::align_of::<*const u8>())
            || !(col_lengths as usize).is_multiple_of(core::mem::align_of::<u32>()))
    {
        return Err(ErrorCode::ColumnUnderrun);
    }
    let mut cols = Vec::with_capacity(count);
    for i in 0..count {
        let ptr = unsafe { *col_ptrs.add(i) };
        let len = unsafe { *col_lengths.add(i) };
        cols.push(unsafe { bounded_buf(ptr, len)? });
    }
    Ok(cols)
}

fn err_code(r: Result<(), ErrorCode>) -> u32 {
    match r {
        Ok(()) => ErrorCode::Ok as u32,
        Err(e) => e as u32,
    }
}

/// Read-only state image for the typed readers. Only the byte span is bounded
/// here; slot geometry is authoritative in the native checked views.
unsafe fn bounded_state<'a>(ptr: *const u8, len: u32) -> Result<&'a [u8], ErrorCode> {
    if len < STATE_HEADER_SIZE || !range_is_bounded(ptr as usize, len as usize) {
        return Err(ErrorCode::InvalidState);
    }
    Ok(unsafe { core::slice::from_raw_parts(ptr, len as usize) })
}

/// One typed read into a caller-owned record. The state image and the record are
/// validated before the native view binds, and the record is written only after
/// the read succeeds, so every refusal leaves output and state untouched.
unsafe fn typed_read<T>(
    state_ptr: *const u8,
    state_len: u32,
    out_ptr: *mut u8,
    out_len: u32,
    read: impl FnOnce(&[u8]) -> Result<T, ErrorCode>,
    write: impl FnOnce(&mut [u8], T),
) -> u32 {
    let state = match unsafe { bounded_state(state_ptr, state_len) } {
        Ok(state) => state,
        Err(error) => return error as u32,
    };
    if !range_is_bounded(out_ptr as usize, out_len as usize)
        || !disjoint(state_ptr, state_len, out_ptr, out_len)
    {
        return ErrorCode::InvalidState as u32;
    }
    match read(state) {
        Ok(value) => {
            write(
                unsafe { core::slice::from_raw_parts_mut(out_ptr, out_len as usize) },
                value,
            );
            ErrorCode::Ok as u32
        }
        Err(error) => error as u32,
    }
}

/// Presence record `[present, value]`: absence is `[0, 0]`, with the value lane
/// unused rather than a sentinel. Every u32 value, `u32::MAX` included, is present.
fn write_presence(out: &mut [u8], value: Option<u32>) {
    let (present, value) = match value {
        Some(value) => (1, value),
        None => (0, 0),
    };
    bytes::write_u32(out, 0, present);
    bytes::write_u32(out, 4, value);
}

/// Scalar record: one little-endian u32 at offset zero.
fn write_word(out: &mut [u8], value: u32) {
    bytes::write_u32(out, 0, value);
}

// =============================================================================
// State lifecycle and growth exports
// =============================================================================

/// Calculate the state-buffer size of `program` into the caller's 16-byte record.
///
/// The status comes first. `Ok` writes `[size, 0, 0, 0]`; `InvalidProgram` writes
/// `[0, 1, 0, 0]` for a malformed program, or `[0, 2, source, target]` for a TTL
/// source slot feeding an untracked target. `InvalidState` marks a program or
/// record span that is out of range or overlaps the other, and writes nothing.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_calculate_state_size(
    program_ptr: *const u8,
    program_len: u32,
    out: *mut u8,
) -> u32 {
    if !range_is_bounded(out as usize, SIZING_RECORD_LEN as usize)
        || !disjoint(program_ptr, program_len, out, SIZING_RECORD_LEN)
    {
        return ErrorCode::InvalidState as u32;
    }
    let program = match unsafe { bounded_buf(program_ptr, program_len) } {
        Ok(program) => program,
        // An unbounded program span is an invalid span: no program was read.
        Err(_) => return ErrorCode::InvalidState as u32,
    };
    let (status, words) = sizing_record(state_init::calculate_state_size(
        program,
        DEFAULT_ACCEPTED_PROGRAM_MAGICS,
    ));
    let output = unsafe { core::slice::from_raw_parts_mut(out, SIZING_RECORD_LEN as usize) };
    for (at, word) in [0, 4, 8, 12].into_iter().zip(words) {
        bytes::write_u32(output, at, word);
    }
    status as u32
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_init_state(
    state_ptr: *mut u8,
    program_ptr: *const u8,
    program_len: u32,
) -> u32 {
    let program = match unsafe { bounded_buf(program_ptr, program_len) } {
        Ok(program) => program,
        Err(_) => return ErrorCode::InvalidProgram as u32,
    };
    let size = match state_init::calculate_state_size(program, DEFAULT_ACCEPTED_PROGRAM_MAGICS) {
        Ok(size) => size,
        // This ABI reports status only; vm_calculate_state_size names the cause.
        Err(_) => return ErrorCode::InvalidProgram as u32,
    };
    if !disjoint(state_ptr, size, program_ptr, program_len) {
        return ErrorCode::InvalidState as u32;
    }
    let state = match unsafe { bounded_state_mut(state_ptr, size) } {
        Ok(state) => state,
        Err(error) => return error as u32,
    };
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
    let program = match unsafe { bounded_buf(program_ptr, program_len) } {
        Ok(program) => program,
        Err(_) => return ErrorCode::InvalidProgram as u32,
    };
    let size = match state_init::calculate_state_size(program, DEFAULT_ACCEPTED_PROGRAM_MAGICS) {
        Ok(size) => size,
        // This ABI reports status only; vm_calculate_state_size names the cause.
        Err(_) => return ErrorCode::InvalidProgram as u32,
    };
    if !disjoint(state_ptr, size, program_ptr, program_len) {
        return ErrorCode::InvalidState as u32;
    }
    let state = match unsafe { bounded_state_mut(state_ptr, size) } {
        Ok(state) => state,
        Err(error) => return error as u32,
    };
    err_code(state_init::reset_state(
        state,
        program,
        DEFAULT_ACCEPTED_PROGRAM_MAGICS,
    ))
}

/// Check a host-supplied state image's whole geometry where it enters the
/// VM (`state_init::validate_state`): `Ok`, or `InvalidState`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_validate_state(state_ptr: *const u8, state_len: u32) -> u32 {
    let state = match unsafe { bounded_buf(state_ptr, state_len) } {
        Ok(state) => state,
        Err(_) => return ErrorCode::InvalidState as u32,
    };
    match state_init::validate_state(state) {
        Ok(()) => ErrorCode::Ok as u32,
        Err(error) => error as u32,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn vm_get_needs_growth_slot() -> u32 {
    state_init::needs_growth_slot()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_calculate_grown_state_size(
    old_state_ptr: *const u8,
    old_state_len: u32,
    grown_slot_idx: u32,
    out_ptr: *mut u8,
) -> u32 {
    if !range_is_bounded(out_ptr as usize, 4) || !disjoint(old_state_ptr, old_state_len, out_ptr, 4)
    {
        return ErrorCode::InvalidState as u32;
    }
    let old = match unsafe { bounded_buf(old_state_ptr, old_state_len) } {
        Ok(state) => state,
        Err(_) => return ErrorCode::InvalidState as u32,
    };
    unsafe {
        write_extent(
            out_ptr,
            state_init::calculate_grown_state_size(old, grown_slot_idx),
        )
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_grow_state(
    old_state_ptr: *const u8,
    old_state_len: u32,
    new_state_ptr: *mut u8,
    new_state_len: u32,
    grown_slot_idx: u32,
) -> u32 {
    if !range_is_bounded(old_state_ptr as usize, old_state_len as usize)
        || !range_is_bounded(new_state_ptr as usize, new_state_len as usize)
        || !disjoint(old_state_ptr, old_state_len, new_state_ptr, new_state_len)
    {
        return ErrorCode::InvalidState as u32;
    }
    let old = unsafe { core::slice::from_raw_parts(old_state_ptr, old_state_len as usize) };
    let new = unsafe { core::slice::from_raw_parts_mut(new_state_ptr, new_state_len as usize) };
    err_code(rt().vm.grow_state(old, new, grown_slot_idx))
}

// =============================================================================
// Batch execution exports
// =============================================================================

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_execute_batch(
    state_base: *mut u8,
    state_len: u32,
    program_ptr: *const u8,
    program_len: u32,
    col_ptrs_ptr: *const *const u8,
    col_lengths_ptr: *const u32,
    num_cols: u32,
    batch_len: u32,
) -> u32 {
    if !disjoint(state_base, state_len, program_ptr, program_len) {
        return ErrorCode::InvalidState as u32;
    }
    let program = match unsafe { bounded_buf(program_ptr, program_len) } {
        Ok(program) => program,
        Err(_) => return ErrorCode::InvalidProgram as u32,
    };
    let cols = match unsafe { cols_vec(col_ptrs_ptr, col_lengths_ptr, num_cols) } {
        Ok(cols) => cols,
        Err(error) => return error as u32,
    };
    for col in &cols {
        let Ok(len) = u32::try_from(col.len()) else {
            return ErrorCode::ColumnUnderrun as u32;
        };
        if !disjoint(state_base, state_len, col.as_ptr(), len) {
            return ErrorCode::InvalidState as u32;
        }
    }
    let state = match unsafe { bounded_state_mut(state_base, state_len) } {
        Ok(state) => state,
        Err(error) => return error as u32,
    };
    rt().vm.execute_batch(state, program, &cols, batch_len)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_execute_batch_delta(
    state_base: *mut u8,
    state_len: u32,
    program_ptr: *const u8,
    program_len: u32,
    col_ptrs_ptr: *const *const u8,
    col_lengths_ptr: *const u32,
    num_cols: u32,
    batch_len: u32,
) -> u32 {
    if !disjoint(state_base, state_len, program_ptr, program_len) {
        return ErrorCode::InvalidState as u32;
    }
    let program = match unsafe { bounded_buf(program_ptr, program_len) } {
        Ok(program) => program,
        Err(_) => return ErrorCode::InvalidProgram as u32,
    };
    let cols = match unsafe { cols_vec(col_ptrs_ptr, col_lengths_ptr, num_cols) } {
        Ok(cols) => cols,
        Err(error) => return error as u32,
    };
    for col in &cols {
        let Ok(len) = u32::try_from(col.len()) else {
            return ErrorCode::ColumnUnderrun as u32;
        };
        if !disjoint(state_base, state_len, col.as_ptr(), len) {
            return ErrorCode::InvalidState as u32;
        }
    }
    let state = match unsafe { bounded_state_mut(state_base, state_len) } {
        Ok(state) => state,
        Err(error) => return error as u32,
    };
    rt().vm
        .execute_batch_delta(state, program, &cols, batch_len)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_evict_all_expired(
    state_base: *mut u8,
    state_len: u32,
    now: f64,
    delta_mode: u32,
) -> u32 {
    if delta_mode > 1 {
        return ErrorCode::InvalidProgram as u32;
    }
    let state = match unsafe { bounded_state_mut(state_base, state_len) } {
        Ok(state) => state,
        Err(error) => return error as u32,
    };
    let runtime = rt();
    match runtime.vm.evict_all_expired(delta_mode == 1, state, now) {
        Ok(count) => {
            runtime.last_evicted_count = count;
            ErrorCode::Ok as u32
        }
        Err(error) => error as u32,
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
    state_base: *const u8,
    state_len: u32,
    slot: u32,
    key: u32,
    out_ptr: *mut u8,
) -> u32 {
    unsafe {
        typed_read(
            state_base,
            state_len,
            out_ptr,
            8,
            |state| CheckedMap::bind(state, slot).map(|map| map.get(key)),
            write_presence,
        )
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_set_contains(
    state_base: *const u8,
    state_len: u32,
    slot: u32,
    elem: u32,
    out_ptr: *mut u8,
) -> u32 {
    unsafe {
        typed_read(
            state_base,
            state_len,
            out_ptr,
            4,
            |state| CheckedSet::bind(state, slot).map(|set| set.contains(elem)),
            |out, present| write_word(out, u32::from(present)),
        )
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_struct_map2_get_row_ptr(
    state_base: *const u8,
    state_len: u32,
    slot: u32,
    key1: u32,
    key2: u32,
    out_ptr: *mut u8,
) -> u32 {
    unsafe {
        typed_read(
            state_base,
            state_len,
            out_ptr,
            8,
            |state| CheckedStructMap2::bind(state, slot).map(|map| map.row(key1, key2)),
            write_presence,
        )
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_struct_map2_iter_start(
    state_base: *const u8,
    state_len: u32,
    slot: u32,
    out_ptr: *mut u8,
) -> u32 {
    unsafe {
        typed_read(
            state_base,
            state_len,
            out_ptr,
            4,
            |state| CheckedStructMap2::bind(state, slot).map(|map| map.iter_start()),
            write_word,
        )
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_struct_map2_iter_next(
    state_base: *const u8,
    state_len: u32,
    slot: u32,
    current: u32,
    out_ptr: *mut u8,
) -> u32 {
    unsafe {
        typed_read(
            state_base,
            state_len,
            out_ptr,
            4,
            |state| CheckedStructMap2::bind(state, slot).and_then(|map| map.iter_next(current)),
            write_word,
        )
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_struct_map2_iter_key1(
    state_base: *const u8,
    state_len: u32,
    slot: u32,
    pos: u32,
    out_ptr: *mut u8,
) -> u32 {
    unsafe {
        typed_read(
            state_base,
            state_len,
            out_ptr,
            4,
            |state| CheckedStructMap2::bind(state, slot).and_then(|map| map.iter_key1(pos)),
            write_word,
        )
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_struct_map2_iter_key2(
    state_base: *const u8,
    state_len: u32,
    slot: u32,
    pos: u32,
    out_ptr: *mut u8,
) -> u32 {
    unsafe {
        typed_read(
            state_base,
            state_len,
            out_ptr,
            4,
            |state| CheckedStructMap2::bind(state, slot).and_then(|map| map.iter_key2(pos)),
            write_word,
        )
    }
}

/// Resolve a live iterator row through the same checked native geometry.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_struct_map2_iter_row_ptr(
    state_base: *const u8,
    state_len: u32,
    slot: u32,
    pos: u32,
    out_ptr: *mut u8,
) -> u32 {
    unsafe {
        typed_read(
            state_base,
            state_len,
            out_ptr,
            4,
            |state| CheckedStructMap2::bind(state, slot).and_then(|map| map.iter_row(pos)),
            write_word,
        )
    }
}

/// Read one generic arena field into a caller-owned eight-byte range record.
/// The bytes stay in the state image; host code copies only when it needs ownership.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_struct_map_arena_field(
    state_base: *const u8,
    state_len: u32,
    slot: u32,
    key: u32,
    field_idx: u32,
    out_ptr: *mut u8,
) -> u32 {
    if !range_is_bounded(out_ptr as usize, 8) || !disjoint(state_base, state_len, out_ptr, 8) {
        return ErrorCode::InvalidState as u32;
    }
    let state = match unsafe { bounded_buf(state_base, state_len) } {
        Ok(state) => state,
        Err(_) => return ErrorCode::InvalidState as u32,
    };
    let smap = match state_init::bind_struct_map(state, slot) {
        Ok(slot) => slot,
        Err(error) => return error as u32,
    };
    let Ok(field_idx) = u8::try_from(field_idx) else {
        return ErrorCode::InvalidProgram as u32;
    };
    if field_idx >= smap.num_fields {
        return ErrorCode::InvalidProgram as u32;
    }
    let (offset, len) = match smap.find(state, key) {
        None => (u32::MAX, 0),
        Some(pos) => match smap.arena_field(state, pos, field_idx) {
            Ok(None) => (u32::MAX - 1, 0),
            Ok(Some(payload)) => {
                let offset = payload.as_ptr() as usize - state.as_ptr() as usize;
                let (Ok(offset), Ok(len)) = (u32::try_from(offset), u32::try_from(payload.len()))
                else {
                    return ErrorCode::InvalidState as u32;
                };
                (offset, len)
            }
            Err(error) => return error as u32,
        },
    };
    let output = unsafe { core::slice::from_raw_parts_mut(out_ptr, 8) };
    bytes::write_u32(output, 0, offset);
    bytes::write_u32(output, 4, len);
    ErrorCode::Ok as u32
}

// =============================================================================
// Undo, checkpoint, and delta-segment exports
// =============================================================================

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_undo_enable(state_base: *mut u8, state_size: u32) -> u32 {
    let state = match unsafe { bounded_buf(state_base, state_size) } {
        Ok(state) if state.len() >= STATE_HEADER_SIZE as usize => state,
        _ => return ErrorCode::InvalidState as u32,
    };
    err_code(rt().vm.undo_enable(state))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_undo_checkpoint(_state_base: *mut u8) -> u32 {
    rt().vm.undo_checkpoint()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_undo_rollback(
    state_base: *mut u8,
    state_len: u32,
    checkpoint_pos: u32,
    out_ptr: *mut u8,
) -> u32 {
    if !range_is_bounded(out_ptr as usize, 4) || !disjoint(state_base, state_len, out_ptr, 4) {
        return ErrorCode::InvalidState as u32;
    }
    let state = match unsafe { bounded_state_mut(state_base, state_len) } {
        Ok(state) => state,
        Err(error) => return error as u32,
    };
    unsafe { write_extent(out_ptr, rt().vm.undo_rollback(state, checkpoint_pos)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_undo_commit(_state_base: *mut u8, _checkpoint_pos: u32) {
    rt().vm.undo_commit();
}

#[unsafe(no_mangle)]
pub extern "C" fn vm_undo_has_overflow() -> u32 {
    u32::from(rt().vm.undo_has_overflow())
}

/// Export only a complete paired range. Refusal preserves prior export buffers
/// and the output record, so an undo-only row cannot masquerade as empty redo.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_delta_export_segment(
    state_base: *const u8,
    state_len: u32,
    from_pos: u32,
    to_pos: u32,
    out_ptr: *mut u8,
) -> u32 {
    if unsafe { bounded_state(state_base, state_len) }.is_err()
        || !range_is_bounded(out_ptr as usize, 4)
        || !disjoint(state_base, state_len, out_ptr, 4)
    {
        return ErrorCode::InvalidState as u32;
    }
    let runtime = rt();
    let count = match runtime.vm.delta_export_segment(from_pos, to_pos) {
        Ok(count) => count,
        Err(error) => return error as u32,
    };
    runtime.delta_undo = runtime.vm.delta_export_undo_bytes();
    runtime.delta_redo = runtime.vm.delta_export_redo_bytes();
    let output = unsafe { core::slice::from_raw_parts_mut(out_ptr, 4) };
    bytes::write_u32(output, 0, count);
    ErrorCode::Ok as u32
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
pub unsafe extern "C" fn vm_delta_required_state_size(
    segment_ptr: *const u8,
    segment_len: u32,
    entry_size: u32,
    initial_size: u32,
    out_ptr: *mut u8,
) -> u32 {
    if !range_is_bounded(out_ptr as usize, 4) || !disjoint(segment_ptr, segment_len, out_ptr, 4) {
        return ErrorCode::InvalidState as u32;
    }
    let segment = match unsafe { bounded_buf(segment_ptr, segment_len) } {
        Ok(segment) => segment,
        Err(_) => return ErrorCode::InvalidState as u32,
    };
    unsafe {
        write_extent(
            out_ptr,
            Vm::delta_required_state_size(segment, entry_size, initial_size),
        )
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_delta_apply_rollback_segment(
    state_base: *mut u8,
    state_len: u32,
    segment_ptr: *const u8,
    segment_len: u32,
    entry_size: u32,
    out_ptr: *mut u8,
) -> u32 {
    if !range_is_bounded(out_ptr as usize, 4)
        || !disjoint(state_base, state_len, out_ptr, 4)
        || !disjoint(segment_ptr, segment_len, out_ptr, 4)
        || !disjoint(state_base, state_len, segment_ptr, segment_len)
    {
        return ErrorCode::InvalidState as u32;
    }
    let segment = match unsafe { bounded_buf(segment_ptr, segment_len) } {
        Ok(segment) => segment,
        Err(_) => return ErrorCode::InvalidState as u32,
    };
    let state = match unsafe { bounded_state_mut(state_base, state_len) } {
        Ok(state) => state,
        Err(error) => return error as u32,
    };
    unsafe {
        write_extent(
            out_ptr,
            rt().vm
                .delta_apply_rollback_segment(state, segment, entry_size),
        )
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vm_delta_apply_rollforward_segment(
    state_base: *mut u8,
    state_len: u32,
    segment_ptr: *const u8,
    segment_len: u32,
    entry_size: u32,
    out_ptr: *mut u8,
) -> u32 {
    if !range_is_bounded(out_ptr as usize, 4)
        || !disjoint(state_base, state_len, out_ptr, 4)
        || !disjoint(segment_ptr, segment_len, out_ptr, 4)
        || !disjoint(state_base, state_len, segment_ptr, segment_len)
    {
        return ErrorCode::InvalidState as u32;
    }
    let segment = match unsafe { bounded_buf(segment_ptr, segment_len) } {
        Ok(segment) => segment,
        Err(_) => return ErrorCode::InvalidState as u32,
    };
    let state = match unsafe { bounded_state_mut(state_base, state_len) } {
        Ok(state) => state,
        Err(error) => return error as u32,
    };
    unsafe {
        write_extent(
            out_ptr,
            rt().vm
                .delta_apply_rollforward_segment(state, segment, entry_size),
        )
    }
}
