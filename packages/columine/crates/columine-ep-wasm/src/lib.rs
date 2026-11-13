//! columine's `event_processor.wasm` — the required seven-function `ep_*`
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

use columine_arrow::schema::{
    DynamicSchemaConfig, FIELD_DESCRIPTOR_BYTES, MAX_SCHEMA_FIELDS, SchemaDictionaryTable,
    SchemaParts,
};
use columine_event_processor::{
    CategoryImage, CompactValidationError, CreateFailure, EventProcessor, InputFormat, ListImage,
    ParseOptions, RESULT_HEADER_SIZE, ResultCode, ResultDiagnostic, WASM_CATEGORY_LIMIT,
    WASM_CATEGORY_VALUE_BYTES, write_compact_result_header,
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
/// linear memory; creation then refuses with
/// [`CreateFailure::MemoryExhausted`] and builds nothing.
pub const MAX_EVENT_CAPACITY: u32 = 1 << 20;

struct EpInstance {
    ep: EventProcessor,
    /// The handle's stream dictionary (specs/columine/10-physical-vocabulary.md
    /// §5): every batch the handle parses shares it, and a refused batch's
    /// values are dropped from it again.
    categories: ListImage,
}

const NO_INSTANCE: Option<Slot> = None;
/// One boxed instance. The array form lets the box be built from fallibly
/// backed storage: stable Rust has no fallible `Box::new`.
type Slot = Box<[EpInstance; 1]>;

static mut HANDLES: [Option<Slot>; 256] = [NO_INSTANCE; 256];
static mut NEXT_HANDLE: u32 = 1;

/// Single-threaded wasm: the handle table is the sole mutable global.
#[allow(static_mut_refs)]
fn handles() -> &'static mut [Option<Slot>; 256] {
    unsafe { &mut HANDLES }
}

/// Scan the 255 usable slots starting at `NEXT_HANDLE`.
///
/// Handle 0 is the creation-failure sentinel and is permanently reserved.
/// Keeping it out of the allocation ring makes every successful creation
/// unambiguous, including after the ring wraps during long-lived use.
fn alloc_handle(ep: Slot) -> Result<u32, CreateFailure> {
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
    handles()[handle as usize]
        .as_deref_mut()
        .map(|[instance]| instance)
}

fn new_instance(capacity: u32, schema_config: DynamicSchemaConfig) -> Result<Slot, CreateFailure> {
    let ep = EventProcessor::new(
        ParseOptions {
            base_path: true,
            msgpack_growth: true,
            diagnostics: false,
        },
        capacity,
        schema_config,
    )?;
    let mut storage = Vec::new();
    storage
        .try_reserve_exact(1)
        .map_err(|_| CreateFailure::MemoryExhausted)?;
    storage.push(EpInstance {
        ep,
        categories: ListImage::new(WASM_CATEGORY_LIMIT, WASM_CATEGORY_VALUE_BYTES),
    });
    let slot: Result<Slot, _> = storage.into_boxed_slice().try_into();
    Ok(slot.unwrap_or_else(|_| unreachable!("one pushed instance boxes as one")))
}

// =============================================================================
// Exports — exact seven-function production ABI (tests/export_checklist.rs)
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

/// The caller's `(ptr, len)` byte span: empty for length 0, refused when a
/// nonzero length comes with a null pointer.
unsafe fn lent<'a>((ptr, len): (*const u8, u32)) -> Result<&'a [u8], CreateFailure> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err(CreateFailure::BadRequest);
    }
    Ok(unsafe { std::slice::from_raw_parts(ptr, len as usize) })
}

/// Validate the caller's pointers and decode the retained schema. A null
/// `field_names` or `dictionaries` span of length 0 is none.
unsafe fn wire_schema(
    schema_ptr: *const u8,
    schema_len: u32,
    field_meta_ptr: *const u8,
    field_count: u32,
    field_names: (*const u8, u32),
    dictionaries: (*const u8, u32),
) -> Result<DynamicSchemaConfig, CreateFailure> {
    // A count past the capacity is refused before a slice is formed over the
    // caller's pointer: no table that long was lent, and on wasm32 the byte
    // length `count * FIELD_DESCRIPTOR_BYTES` would wrap. Native and wasm
    // refuse it alike.
    let field_count = field_count as usize;
    if field_count > MAX_SCHEMA_FIELDS {
        return Err(CreateFailure::SchemaTooManyFields);
    }
    let field_meta_len = field_count * FIELD_DESCRIPTOR_BYTES;
    if schema_ptr.is_null() || (field_meta_len != 0 && field_meta_ptr.is_null()) {
        return Err(CreateFailure::BadRequest);
    }
    let schema_bytes = unsafe { std::slice::from_raw_parts(schema_ptr, schema_len as usize) };
    let field_meta: &[u8] = if field_meta_len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(field_meta_ptr, field_meta_len) }
    };
    let field_names = unsafe { lent(field_names) }?;
    let dictionaries = unsafe { lent(dictionaries) }?;
    DynamicSchemaConfig::from_wire_parts(
        field_meta,
        SchemaParts {
            schema_message: schema_bytes,
            field_names,
            dictionaries: SchemaDictionaryTable::Wire(dictionaries),
        },
    )
    .map_err(CreateFailure::from)
}

/// The whole creation pipeline, in refusal order: the scalar precondition
/// first, then the caller's pointers and schema, then the instance, then a
/// slot. Capacity is checked before the schema is decoded because decoding is
/// wasted work for a request that can never succeed, and because a caller who
/// got the capacity wrong should hear about that rather than about the schema.
unsafe fn create_handle(
    capacity: u32,
    schema_ptr: *const u8,
    schema_len: u32,
    field_meta_ptr: *const u8,
    field_count: u32,
    field_names: (*const u8, u32),
    dictionaries: (*const u8, u32),
) -> Result<u32, CreateFailure> {
    if capacity == 0 || capacity > MAX_EVENT_CAPACITY {
        return Err(CreateFailure::Capacity);
    }
    let config = unsafe {
        wire_schema(
            schema_ptr,
            schema_len,
            field_meta_ptr,
            field_count,
            field_names,
            dictionaries,
        )
    }?;
    alloc_handle(new_instance(capacity, config)?)
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
    create_result(unsafe {
        create_handle(
            capacity,
            schema_ptr,
            schema_len,
            field_meta_ptr,
            field_count,
            (std::ptr::null(), 0),
            (std::ptr::null(), 0),
        )
    })
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
    create_result(unsafe {
        create_handle(
            capacity,
            schema_ptr,
            schema_len,
            field_meta_ptr,
            field_count,
            (field_names_ptr, field_names_len),
            (std::ptr::null(), 0),
        )
    })
}

/// Field names and the schema dictionary table
/// (specs/columine/10-physical-vocabulary.md §4): `[count u32]`, then per
/// dictionary `[values u32]`, then per value `[length u32][UTF-8 bytes]`,
/// little-endian, length `0xFFFF_FFFF` for a null entry. Slot `2 + k` of a
/// field descriptor names dictionary `k`.
///
/// Returns a handle in `1..=255`, or a [`CreateFailure`] code.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn ep_create_with_schema_and_dictionaries(
    capacity: u32,
    schema_ptr: *const u8,
    schema_len: u32,
    field_meta_ptr: *const u8,
    field_count: u32,
    field_names_ptr: *const u8,
    field_names_len: u32,
    dictionaries_ptr: *const u8,
    dictionaries_len: u32,
) -> u32 {
    create_result(unsafe {
        create_handle(
            capacity,
            schema_ptr,
            schema_len,
            field_meta_ptr,
            field_count,
            (field_names_ptr, field_names_len),
            (dictionaries_ptr, dictionaries_len),
        )
    })
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
        return ResultCode::OutputTooSmall as u32;
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
    // A refused batch's stream values are dropped again: the image holds
    // only values some emitted batch defined.
    let mark = instance.categories.len();
    let code =
        instance
            .ep
            .create_log_entry_with(input, format, output, &[], &mut instance.categories);
    if code != ResultCode::Ok {
        instance.categories.truncate(mark);
    }
    code as u32
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
        return ResultCode::OutputTooSmall as u32;
    }
    let output_start = output_ptr as usize;
    let Some(output_end) = output_start.checked_add(output_len as usize) else {
        return ResultCode::OutputTooSmall as u32;
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
    // A compact caller of this artifact has no way to add stream values, so
    // its batch brings none: the mark is the image's whole length.
    let mark = instance.categories.len();
    instance
        .ep
        .compact_with(batch, output, &[], &instance.categories, mark) as u32
}
