//! columine's `event_processor.wasm` — the required six-function `ep_*`
//! export layer over `columine-event-processor`.
//!
//! Parse and CPB1 Compact share the same retained, validated Arrow schema
//! handle and caller-owned output-buffer protocol.
//!
//! Buffer protocol (`src/parse-backend.ts` contract): unlike the earlier EP's
//! `ep_input_ptr` handshake, the caller owns the geometry. JS writes request
//! bytes at an offset it chose in the exported memory and passes
//! `(input_ptr, input_len, output_ptr, output_len)` per call; the result is
//! `[ResultHeader][Arrow IPC]` at `output_ptr`.
//!
//! Statics policy: single-threaded wasm assumed — parse-backend gives each
//! backend its own instance; the handle table sees one caller. Native tests
//! serialize via a per-file lock (see tests/smoke.rs).

#![allow(clippy::missing_safety_doc)]

use columine_arrow::schema::DynamicSchemaConfig;
use columine_event_processor::{
    CollisionPolicy, CompactValidationError, CreateFailure, EpWiring, EventProcessor, InputFormat,
    RESULT_HEADER_SIZE, ResultCode, ResultDiagnostic, write_compact_result_header,
};

/// Same wire version as the consumer EP artifact (one event_processor lineage).
pub const VERSION: u32 = 2;

/// Capacity honesty (post-parity): the earlier implementation silently
/// clamped every wasm instance to 256 events regardless of the requested
/// capacity; the requested capacity is honored now. A sanity ceiling guards
/// against unreasonable requests corrupting the address space. Requests above
/// it are refused with [`CreateFailure::Capacity`], never a bare 0.
///
/// It is a guard, not a guarantee. The column plane a capacity implies scales
/// with schema width, so a request well under this ceiling can still exhaust
/// linear memory — and a wasm allocation failure aborts, so it surfaces as a
/// trap in the host rather than as any [`CreateFailure`] code. Measured: 32
/// utf8 columns create at capacity 61440 and trap at 61932.
pub const MAX_EVENT_CAPACITY: u32 = 1 << 20;

struct EpInstance {
    ep: EventProcessor,
}

const NO_INSTANCE: Option<Box<EpInstance>> = None;
static mut HANDLES: [Option<Box<EpInstance>>; 256] = [NO_INSTANCE; 256];
static mut NEXT_HANDLE: u32 = 1;

/// Single-threaded wasm: the handle table is the sole mutable global.
#[allow(static_mut_refs)]
fn handles() -> &'static mut [Option<Box<EpInstance>>; 256] {
    unsafe { &mut HANDLES }
}

/// Scan the 255 usable slots starting at `NEXT_HANDLE`.
///
/// Handle 0 is the creation-failure sentinel and is permanently reserved.
/// Keeping it out of the allocation ring makes every successful creation
/// unambiguous, including after the ring wraps during long-lived use.
fn alloc_handle(ep: Box<EpInstance>) -> Result<u32, CreateFailure> {
    let table = handles();
    #[allow(static_mut_refs)]
    let next = unsafe { &mut NEXT_HANDLE };
    for offset in 0..255u32 {
        let idx = 1 + ((*next - 1 + offset) % 255);
        if table[idx as usize].is_none() {
            table[idx as usize] = Some(ep);
            *next = if idx == 255 { 1 } else { idx + 1 };
            return Ok(idx);
        }
    }
    Err(CreateFailure::HandlesExhausted)
}

fn get_processor(handle: u32) -> Option<&'static mut EpInstance> {
    if !(1..=255).contains(&handle) {
        return None;
    }
    handles()[handle as usize].as_deref_mut()
}

fn new_instance(
    capacity: u32,
    schema_config: DynamicSchemaConfig,
) -> Result<Box<EpInstance>, CreateFailure> {
    if capacity == 0 || capacity > MAX_EVENT_CAPACITY {
        return Err(CreateFailure::Capacity);
    }
    let column_capacity = capacity;
    // Columine has no deduplication, so the policy argument is unused on this
    // path; `Latest` satisfies the shared core signature.
    let ep = EventProcessor::with_column_capacity(
        EpWiring::columine(),
        capacity,
        column_capacity,
        CollisionPolicy::Latest,
        schema_config,
    )?;
    Ok(Box::new(EpInstance { ep }))
}

// =============================================================================
// Exports — exact six-function production ABI (tests/export_checklist.rs)
// =============================================================================

#[unsafe(no_mangle)]
pub extern "C" fn ep_version() -> u32 {
    VERSION
}

#[unsafe(no_mangle)]
pub extern "C" fn ep_destroy(handle: u32) {
    if handle < 256 {
        handles()[handle as usize] = None;
    }
}

/// Validate the caller's pointer triple and decode the retained schema.
/// `field_names` is `None` on the no-names path.
unsafe fn wire_schema(
    schema_ptr: *const u8,
    schema_len: u32,
    field_meta_ptr: *const u8,
    field_count: u32,
    field_names: Option<(*const u8, u32)>,
) -> Result<DynamicSchemaConfig, CreateFailure> {
    let field_meta_len = (field_count as usize)
        .checked_mul(4)
        .ok_or(CreateFailure::BadRequest)?;
    if schema_ptr.is_null() || (field_meta_len != 0 && field_meta_ptr.is_null()) {
        return Err(CreateFailure::BadRequest);
    }
    let schema_bytes = unsafe { std::slice::from_raw_parts(schema_ptr, schema_len as usize) };
    let field_meta: &[u8] = if field_meta_len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(field_meta_ptr, field_meta_len) }
    };
    let Some((names_ptr, names_len)) = field_names else {
        return DynamicSchemaConfig::from_wire(schema_bytes, field_meta)
            .map_err(CreateFailure::from);
    };
    if names_len != 0 && names_ptr.is_null() {
        return Err(CreateFailure::BadRequest);
    }
    let names: &[u8] = if names_len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(names_ptr, names_len as usize) }
    };
    DynamicSchemaConfig::from_wire_with_field_names(schema_bytes, field_meta, names)
        .map_err(CreateFailure::from)
}

/// Collapse the creation outcome onto the single u32 the ABI returns. Every
/// export funnels through here so no path can invent an unnamed failure.
fn create_result(result: Result<u32, CreateFailure>) -> u32 {
    match result {
        Ok(handle) => handle,
        Err(failure) => failure as u32,
    }
}

/// No field names: export-compatibility path (JSON keys cannot be matched to
/// columns, so `ep_create_log_entry` will refuse).
///
/// Returns a handle in `1..=255`, or a [`CreateFailure`] code.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ep_create_with_schema(
    capacity: u32,
    schema_ptr: *const u8,
    schema_len: u32,
    field_meta_ptr: *const u8,
    field_count: u32,
) -> u32 {
    create_result(
        unsafe { wire_schema(schema_ptr, schema_len, field_meta_ptr, field_count, None) }
            .and_then(|config| new_instance(capacity, config))
            .and_then(alloc_handle),
    )
}

/// Primary path: field names enable extraction for `value.*` schemas.
///
/// Returns a handle in `1..=255`, or a [`CreateFailure`] code.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ep_create_with_schema_and_names(
    capacity: u32,
    schema_ptr: *const u8,
    schema_len: u32,
    field_meta_ptr: *const u8,
    field_count: u32,
    field_names_ptr: *const u8,
    field_names_len: u32,
) -> u32 {
    create_result(
        unsafe {
            wire_schema(
                schema_ptr,
                schema_len,
                field_meta_ptr,
                field_count,
                Some((field_names_ptr, field_names_len)),
            )
        }
        .and_then(|config| new_instance(capacity, config))
        .and_then(alloc_handle),
    )
}

/// Parse `input_len` bytes at `input_ptr`; write `[ResultHeader][Arrow IPC]`
/// into the caller's buffer at `output_ptr`. Returns the result code (also
/// mirrored in the header).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ep_create_log_entry(
    handle: u32,
    input_ptr: *const u8,
    input_len: u32,
    format: u8,
    output_ptr: *mut u8,
    output_len: u32,
) -> u32 {
    let Some(instance) = get_processor(handle) else {
        return ResultCode::InvalidHandle as u32;
    };
    if (output_len as usize) < RESULT_HEADER_SIZE {
        return ResultCode::OutOfMemory as u32;
    }
    // Reject format bytes outside the ABI enum rather than interpreting them
    // as an unchecked value.
    let format = match format {
        0 => InputFormat::Json,
        1 => InputFormat::Msgpack,
        2 => InputFormat::ArrowPassthrough,
        3 => InputFormat::MsgpackStream,
        _ => return ResultCode::InvalidFormat as u32,
    };
    let input = unsafe { std::slice::from_raw_parts(input_ptr, input_len as usize) };
    let output = unsafe { std::slice::from_raw_parts_mut(output_ptr, output_len as usize) };
    instance.ep.create_log_entry(input, format, output) as u32
}

/// Encode one CPB1 typed column batch into `[ResultHeader][Arrow IPC]`.
///
/// The request and output regions must be disjoint. The returned status is
/// mirrored at output offset 0 whenever `output_len >= 32`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ep_compact(
    handle: u32,
    batch_ptr: *const u8,
    batch_len: u32,
    output_ptr: *mut u8,
    output_len: u32,
) -> u32 {
    if (output_len as usize) < RESULT_HEADER_SIZE || output_ptr.is_null() {
        return ResultCode::OutOfMemory as u32;
    }
    let output_start = output_ptr as usize;
    let Some(output_end) = output_start.checked_add(output_len as usize) else {
        return ResultCode::OutOfMemory as u32;
    };
    let output = unsafe { std::slice::from_raw_parts_mut(output_ptr, output_len as usize) };
    let Some(instance) = get_processor(handle) else {
        let diagnostic = ResultDiagnostic::default();
        write_compact_result_header(output, ResultCode::InvalidHandle, 0, 0, 0, &diagnostic);
        return ResultCode::InvalidHandle as u32;
    };

    if batch_len != 0 && batch_ptr.is_null() {
        let error = CompactValidationError::bad_request();
        write_compact_result_header(output, error.code, 0, 0, 0, &error.diagnostic);
        return error.code as u32;
    }
    let batch_start = batch_ptr as usize;
    let Some(batch_end) = batch_start.checked_add(batch_len as usize) else {
        let error = CompactValidationError::bad_request();
        write_compact_result_header(output, error.code, 0, 0, 0, &error.diagnostic);
        return error.code as u32;
    };
    if batch_len != 0 && batch_start < output_end && output_start < batch_end {
        let error = CompactValidationError::output_overlap();
        write_compact_result_header(output, error.code, 0, 0, 0, &error.diagnostic);
        return error.code as u32;
    }

    let batch = if batch_len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(batch_ptr, batch_len as usize) }
    };
    instance.ep.compact(batch, output) as u32
}
