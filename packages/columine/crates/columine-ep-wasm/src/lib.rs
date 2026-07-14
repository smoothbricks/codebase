//! `event_processor.wasm` replacement — the `ep_*` export layer.
//!
//! Replaces the export section of `packages/columine/src/event_processor.zig`
//! (15 functions + exported memory, enumerated from the Zig artifact's export
//! section and pinned by `tests/export_checklist.rs`). The processing core is
//! `columine-event-processor` with `EpWiring::columine()`; this crate owns what the Zig
//! file kept as globals: the 256-slot handle table and each instance's
//! host-visible input/output buffers.
//!
//! Host-buffer geometry (`src/lib.ts` contract): JS writes request bytes at
//! `ep_input_ptr`, calls `ep_create_log_entry`, and reads
//! `[ResultHeader 32B][Arrow IPC]` at `ep_output_ptr`. Buffers are reused
//! across calls; JS re-acquires views after any call that may grow memory.
//! `ep_reserve` grows both buffers commit-only-after-success, exactly like
//! the Zig `reserveHostBuffers`.
//!
//! Statics policy: single-threaded wasm assumed — `src/lib.ts` gives each
//! EventProcessor its own instance ("Each EventProcessor gets its own WASM
//! instance for isolation"), so the handle table sees one caller.
//!
//! Pointer policy: same as columine-wasm — wasm32 pointers are bounded by
//! linear memory; pointer-returning exports truncate to u32 (exact on
//! wasm32). Native tests use the doc-hidden slice helpers instead of the
//! truncated pointers.

#![allow(clippy::missing_safety_doc)]

use columine_arrow::schema::{DynamicSchemaConfig, SignalSchemaField};
use columine_event_processor::{
    CollisionPolicy, EpWiring, EventProcessor, InputFormat, RESULT_HEADER_SIZE, ResultCode,
};

/// `event_processor.zig VERSION` (the columine package's, = 1).
pub const VERSION: u32 = 1;

// Bounded batch column capacity (event_processor.zig `EventProcessor`).
const WASM_EVENT_CAPACITY: u32 = 256;

struct EpInstance {
    ep: EventProcessor,
}

const NO_INSTANCE: Option<Box<EpInstance>> = None;
static mut HANDLES: [Option<Box<EpInstance>>; 256] = [NO_INSTANCE; 256];
static mut NEXT_HANDLE: u32 = 1;

/// Single-threaded wasm: the handle-table access mirror of Zig's
/// `g_handles`/`g_next_handle` globals.
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

impl EpInstance {
    fn new(capacity: u32, schema_config: DynamicSchemaConfig) -> Option<Box<EpInstance>> {
        // Zig `initCommon`: columns cap at 256 events on wasm32 to fit
        // memory; native builds take the full capacity.
        let column_capacity = if cfg!(target_arch = "wasm32") {
            capacity.min(WASM_EVENT_CAPACITY)
        } else {
            capacity
        };
        // No dedup under EpWiring::columine(), so the collision policy is
        // inert; Latest mirrors the Zig default.
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
}

// =============================================================================
// Exports — the columine.wasm ABI: five functions, caller-provided buffers
// (build.zig columine ep_wasm; JS computes the memory layout in
// wasm-memory-contract.ts and passes raw offsets into exported memory).
// =============================================================================

#[unsafe(no_mangle)]
pub extern "C" fn ep_version() -> u32 {
    VERSION
}

/// `ep_create_with_schema` — no field names: `ep_create_log_entry` then
/// fails for extraction schemas since JSON keys cannot be matched to
/// columns. Returns handle (0 = error).
///
/// # Safety
/// `schema_ptr..schema_len` and `field_meta_ptr..field_count` must be valid
/// for reads — on wasm32 they are offsets into this module's exported
/// memory, written by the host per wasm-memory-contract.ts.
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
    let Some(instance) = EpInstance::new(capacity, config) else {
        return 0;
    };
    alloc_handle(instance).unwrap_or(0)
}

/// `ep_create_with_schema_and_names` — the primary path: field names enable
/// the extraction path for `value.*` schemas. Returns handle (0 = error).
///
/// # Safety
/// All pointer/length pairs must be valid for reads (host-written offsets
/// into exported memory on wasm32).
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
    let Some(instance) = EpInstance::new(capacity, config) else {
        return 0;
    };
    alloc_handle(instance).unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn ep_destroy(handle: u32) {
    if handle < 256 {
        handles()[handle as usize] = None;
    }
}

/// Process one input batch: `[ResultHeader 32B][Arrow IPC]` lands in
/// `output_ptr`. Returns the ResultCode (0 = OK).
///
/// # Safety
/// `input_ptr..input_len` must be valid for reads and
/// `output_ptr..output_len` for writes (host-provided buffers).
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
