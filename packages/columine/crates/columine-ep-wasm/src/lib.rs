//! columine's `event_processor.wasm` — the 5-function `ep_*` export layer.
//!
//! Replaces the export section of columine's Zig event_processor build
//! (5 functions + exported memory, enumerated from the Zig artifact's export
//! section and pinned by `tests/export_checklist.rs`). The processing core is
//! `columine-event-processor` with `EpWiring::columine()` — the standalone
//! npm variant: no dedup, base path enabled, msgpack workspace growth on.
//!
//! Buffer protocol (`src/parse-backend.ts` contract, unlike the axe EP's
//! `ep_input_ptr` handshake): the caller owns the geometry. JS writes request
//! bytes at an offset it chose in the exported memory and passes
//! `(input_ptr, input_len, output_ptr, output_len)` per call; the result is
//! `[ResultHeader][Arrow IPC]` at `output_ptr`.
//!
//! Statics policy: single-threaded wasm assumed — parse-backend gives each
//! backend its own instance; the handle table sees one caller. Native tests
//! serialize via a per-file lock (see tests/smoke.rs).

#![allow(clippy::missing_safety_doc)]

use columine_arrow::schema::{DynamicSchemaConfig, SignalSchemaField};
use columine_event_processor::{
    CollisionPolicy, EpWiring, EventProcessor, InputFormat, RESULT_HEADER_SIZE, ResultCode,
};

/// Same wire version as the axe EP artifact (one event_processor lineage).
pub const VERSION: u32 = 2;

/// Zig `initCommon` wasm clamp — shared lineage with the axe EP artifact.
/// ZIG-PARITY: the clamp itself is the Zig 256-event ceiling; intended fix
/// (capacity honesty) lands with the post-parity sweep for both artifacts.
const WASM_EVENT_CAPACITY: u32 = 256;

struct EpInstance {
    ep: EventProcessor,
}

const NO_INSTANCE: Option<Box<EpInstance>> = None;
static mut HANDLES: [Option<Box<EpInstance>>; 256] = [NO_INSTANCE; 256];
static mut NEXT_HANDLE: u32 = 1;

/// Single-threaded wasm: the handle-table mirror of Zig's globals.
#[allow(static_mut_refs)]
fn handles() -> &'static mut [Option<Box<EpInstance>>; 256] {
    unsafe { &mut HANDLES }
}

/// `allocHandle`: scan 256 slots starting at `g_next_handle`. Faithful to the
/// Zig including its wraparound quirk: after enough churn, slot 0 can be
/// allocated, whose handle is indistinguishable from the error sentinel 0.
fn alloc_handle(ep: Box<EpInstance>) -> Option<u32> {
    let table = handles();
    #[allow(static_mut_refs)]
    let next = unsafe { &mut NEXT_HANDLE };
    for i in 0..256u32 {
        let idx = ((*next + i) % 256) as usize;
        if table[idx].is_none() {
            table[idx] = Some(ep);
            *next = (idx as u32 + 1) % 256;
            return Some(idx as u32);
        }
    }
    None
}

fn get_processor(handle: u32) -> Option<&'static mut EpInstance> {
    if handle >= 256 {
        return None;
    }
    handles()[handle as usize].as_deref_mut()
}

fn new_instance(capacity: u32, schema_config: DynamicSchemaConfig) -> Option<Box<EpInstance>> {
    let column_capacity = if cfg!(target_arch = "wasm32") {
        capacity.min(WASM_EVENT_CAPACITY)
    } else {
        capacity
    };
    // Columine has no dedup (EpWiring::columine()); the policy parameter in
    // the core signature is vestigial on this path — Latest is what the Zig
    // hardcoded.
    let ep = EventProcessor::with_column_capacity(
        EpWiring::columine(),
        capacity,
        column_capacity,
        CollisionPolicy::Latest,
        schema_config,
    )
    .ok()?;
    Some(Box::new(EpInstance { ep }))
}

// =============================================================================
// Exports — exactly the Zig artifact's five (tests/export_checklist.rs)
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

/// No field names: export-compatibility path (JSON keys cannot be matched to
/// columns, so `ep_create_log_entry` will refuse). Returns handle (0 = error).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ep_create_with_schema(
    capacity: u32,
    schema_ptr: *const u8,
    schema_len: u32,
    field_meta_ptr: *const SignalSchemaField,
    field_count: u32,
) -> u32 {
    let schema_bytes = unsafe { std::slice::from_raw_parts(schema_ptr, schema_len as usize) };
    let field_meta = unsafe { std::slice::from_raw_parts(field_meta_ptr, field_count as usize) };
    let config = DynamicSchemaConfig::new(schema_bytes, field_meta);
    let Some(instance) = new_instance(capacity, config) else {
        return 0;
    };
    alloc_handle(instance).unwrap_or(0)
}

/// Primary path: field names enable extraction for `value.*` schemas.
/// Returns handle (0 = error).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ep_create_with_schema_and_names(
    capacity: u32,
    schema_ptr: *const u8,
    schema_len: u32,
    field_meta_ptr: *const SignalSchemaField,
    field_count: u32,
    field_names_ptr: *const u8,
    field_names_len: u32,
) -> u32 {
    let schema_bytes = unsafe { std::slice::from_raw_parts(schema_ptr, schema_len as usize) };
    let field_meta = unsafe { std::slice::from_raw_parts(field_meta_ptr, field_count as usize) };
    let field_names =
        unsafe { std::slice::from_raw_parts(field_names_ptr, field_names_len as usize) };
    let config = DynamicSchemaConfig::with_field_names(schema_bytes, field_meta, field_names);
    let Some(instance) = new_instance(capacity, config) else {
        return 0;
    };
    alloc_handle(instance).unwrap_or(0)
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
    // ZIG-PARITY: Zig `@enumFromInt(format)` on a byte outside {0,1,2,3} is
    // UB under ReleaseSmall; checked INVALID_FORMAT here. Intended fix: keep
    // the checked behavior at the sweep.
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
