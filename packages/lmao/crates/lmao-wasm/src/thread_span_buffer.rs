//! Numeric-handle adapter for the canonical [`lmao_core::ThreadSpanBuffer`] ABI.
//!
//! Native callers use `lmao_core::thread_ffi`, whose opaque handle is a Rust
//! pointer. WASM callers cannot safely retain that pointer as a JavaScript
//! number, so this module owns a per-module slot table and keeps the pointer
//! private. The row store, lifecycle, parentage, overflow, and value decoding
//! remain in `lmao-core`; this file only validates slots and translates the
//! frozen numeric ABI.

use std::cell::RefCell;

use lmao_core::{
    ATTRIBUTE_KIND_BOOLEAN, ATTRIBUTE_KIND_ENUM, ATTRIBUTE_KIND_NUMBER, ATTRIBUTE_KIND_TEXT,
    ATTRIBUTE_KIND_UINT64, ColumnValueRef, EntryType, TextInput, ThreadBufferError,
    ThreadSpanBuffer, ThreadStore, TraceId, VocabularyId,
};

const STATUS_OK: u8 = 0;
const STATUS_ERROR: u8 = 1;

thread_local! {
    static HANDLES: RefCell<Vec<Option<ThreadStore>>> = const { RefCell::new(Vec::new()) };
}

fn valid_capacity(capacity: u32) -> Option<usize> {
    let capacity = usize::try_from(capacity).ok()?;
    capacity
        .is_power_of_two()
        .then_some(capacity)
        .filter(|capacity| (lmao_core::MIN_CAPACITY..=lmao_core::MAX_CAPACITY).contains(capacity))
}

fn with_handle<R>(
    handle: u32,
    f: impl FnOnce(&mut ThreadSpanBuffer) -> Result<R, ThreadBufferError>,
) -> Result<R, ThreadBufferError> {
    if handle == 0 {
        return Err(ThreadBufferError::UnknownSpan(0));
    }
    HANDLES.with(|handles| {
        handles
            .borrow_mut()
            .get_mut(handle as usize - 1)
            .and_then(Option::as_mut)
            .ok_or(ThreadBufferError::UnknownSpan(0))
            .and_then(|store| f(store))
    })
}

fn allocate_handle(slot: ThreadStore) -> u32 {
    HANDLES.with(|handles| {
        let mut handles = handles.borrow_mut();
        if let Some(index) = handles.iter().position(Option::is_none) {
            handles[index] = Some(slot);
            return u32::try_from(index + 1).unwrap_or(0);
        }
        handles.push(Some(slot));
        u32::try_from(handles.len()).unwrap_or(0)
    })
}

fn pack(span_id: u32, row: usize) -> u64 {
    (u64::from(span_id) << 32) | u64::try_from(row).unwrap_or(u64::MAX)
}

unsafe fn bytes<'a>(ptr: *const u8, len: usize) -> Option<&'a [u8]> {
    if len > 0 && ptr.is_null() {
        return None;
    }
    // SAFETY: each exported dynamic entrypoint documents that the caller owns
    // this readable range for the duration of the call. A null pointer is
    // valid for an empty range and `from_raw_parts` accepts it only through the
    // explicit empty-slice branch below.
    if len == 0 {
        return Some(&[]);
    }
    Some(unsafe { std::slice::from_raw_parts(ptr, len) })
}

fn trace_id(ptr: *const u8, len: usize) -> Option<TraceId> {
    let bytes = unsafe { bytes(ptr, len) }?;
    let value = std::str::from_utf8(bytes).ok()?.to_owned();
    TraceId::new(value).ok()
}

/// Borrow caller bytes as dynamic text for the arena's intern path. Nothing is
/// copied here: `ThreadSpanBuffer::cell` interns the value, so the copy is per
/// distinct string, not per call.
fn dynamic_text<'a>(ptr: *const u8, len: usize) -> Option<TextInput<'a>> {
    let bytes = unsafe { bytes(ptr, len) }?;
    std::str::from_utf8(bytes).ok().map(TextInput::Dynamic)
}

#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub extern "C" fn thread_span_buffer_new(thread_id: u64, capacity: u32) -> u32 {
    let Some(capacity) = valid_capacity(capacity) else {
        return 0;
    };
    allocate_handle(ThreadStore::with_fields(thread_id, capacity, &[]))
}

/// Construct a schema-bearing buffer from the compact generated-schema blob.
///
/// Each field is `[kind:u8][name_len:u8][name bytes]`; enum fields append
/// `[variant_count:u16 LE]` and `[len:u8][variant bytes]` for each variant.
/// # Safety
/// `handle` must be a token previously returned by a live `thread_span_buffer_new*`
/// export, and every pointer/length pair must name a readable byte range for the
/// duration of the call; null pointers are valid only with zero lengths.
#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub unsafe extern "C" fn thread_span_buffer_new_with_schema(
    thread_id: u64,
    capacity: u32,
    fields_ptr: *const u8,
    fields_len: usize,
) -> u32 {
    let Some(capacity) = valid_capacity(capacity) else {
        return 0;
    };
    let Some(blob) = (unsafe { bytes(fields_ptr, fields_len) }) else {
        return 0;
    };
    ThreadStore::from_schema_blob(thread_id, capacity, blob).map_or(0, allocate_handle)
}

#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub extern "C" fn thread_span_buffer_free(handle: u32) {
    if handle == 0 {
        return;
    }
    HANDLES.with(|handles| {
        if let Some(slot) = handles.borrow_mut().get_mut(handle as usize - 1) {
            *slot = None;
        }
    });
}

/// Release every row and span on a handle, keeping its interned vocabulary.
/// Returns 0 on success and a non-zero status for an unknown handle.
#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub extern "C" fn thread_span_buffer_reset(handle: u32) -> i32 {
    match with_handle(handle, |buffer| {
        buffer.reset();
        Ok(())
    }) {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

/// # Safety
/// `handle` must be a token previously returned by a live `thread_span_buffer_new*`
/// export, and every pointer/length pair must name a readable byte range for the
/// duration of the call; null pointers are valid only with zero lengths.
#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub unsafe extern "C" fn thread_span_buffer_intern(handle: u32, ptr: *const u8, len: usize) -> u32 {
    let Some(bytes) = (unsafe { bytes(ptr, len) }) else {
        return 0;
    };
    with_handle(handle, |buffer| buffer.intern_utf8(bytes)).unwrap_or(0)
}

/// # Safety
/// `handle` must be a token previously returned by a live `thread_span_buffer_new*`
/// export, and every pointer/length pair must name a readable byte range for the
/// duration of the call; null pointers are valid only with zero lengths.
#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub unsafe extern "C" fn thread_span_buffer_open_span(
    handle: u32,
    trace_ptr: *const u8,
    trace_len: usize,
    parent_thread_id: u64,
    parent_span_id: u32,
    name_ordinal: u32,
    timestamp: i64,
    line: u32,
) -> u64 {
    let Some(trace_id) = trace_id(trace_ptr, trace_len) else {
        return 0;
    };
    with_handle(handle, |buffer| {
        let span_id = buffer.open_span_interned(
            trace_id,
            parent_thread_id,
            parent_span_id,
            name_ordinal,
            timestamp,
            line,
        )?;
        let row = buffer
            .start_row(span_id)
            .ok_or(ThreadBufferError::InvalidRow(0))?;
        Ok(pack(span_id, row))
    })
    .unwrap_or(0)
}

/// # Safety
/// `handle` must be a token previously returned by a live `thread_span_buffer_new*`
/// export, and every pointer/length pair must name a readable byte range for the
/// duration of the call; null pointers are valid only with zero lengths.
#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub unsafe extern "C" fn thread_span_buffer_open_span_static(
    handle: u32,
    trace_ptr: *const u8,
    trace_len: usize,
    parent_thread_id: u64,
    parent_span_id: u32,
    name_id: u32,
    timestamp: i64,
    line: u32,
) -> u64 {
    let Some(trace_id) = trace_id(trace_ptr, trace_len) else {
        return 0;
    };
    let Ok(name_id) = VocabularyId::try_from(name_id) else {
        return 0;
    };
    with_handle(handle, |buffer| {
        let span_id = buffer.open_span_static(
            trace_id,
            parent_thread_id,
            parent_span_id,
            name_id,
            timestamp,
            line,
        )?;
        let row = buffer
            .start_row(span_id)
            .ok_or(ThreadBufferError::InvalidRow(0))?;
        Ok(pack(span_id, row))
    })
    .unwrap_or(0)
}

/// # Safety
/// `handle` must be a token previously returned by a live `thread_span_buffer_new*`
/// export, and every pointer/length pair must name a readable byte range for the
/// duration of the call; null pointers are valid only with zero lengths.
#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub unsafe extern "C" fn thread_span_buffer_open_span_dynamic(
    handle: u32,
    trace_ptr: *const u8,
    trace_len: usize,
    parent_thread_id: u64,
    parent_span_id: u32,
    name_ptr: *const u8,
    name_len: usize,
    timestamp: i64,
    line: u32,
) -> u64 {
    let Some(trace_id) = trace_id(trace_ptr, trace_len) else {
        return 0;
    };
    let Some(name) = dynamic_text(name_ptr, name_len) else {
        return 0;
    };
    with_handle(handle, |buffer| {
        let span_id = buffer.open_span(
            trace_id,
            parent_thread_id,
            parent_span_id,
            name,
            timestamp,
            line,
        )?;
        let row = buffer
            .start_row(span_id)
            .ok_or(ThreadBufferError::InvalidRow(0))?;
        Ok(pack(span_id, row))
    })
    .unwrap_or(0)
}

/// Complete a span with the caller's entry type.
///
/// Replaces the end_ok/end_err pair: two entry points could only express two
/// of the completion types, so a thrown exception arrived as a handled error.
#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub extern "C" fn thread_span_buffer_end(
    handle: u32,
    span_id: u32,
    entry_type: u8,
    timestamp: i64,
) -> u8 {
    let Some(entry_type) = EntryType::from_u8(entry_type) else {
        return STATUS_ERROR;
    };
    with_handle(handle, |buffer| buffer.end(span_id, entry_type, timestamp))
        .map(|()| STATUS_OK)
        .unwrap_or(STATUS_ERROR)
}

#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub extern "C" fn thread_span_buffer_append_log(
    handle: u32,
    span_id: u32,
    entry_type: u8,
    message_ordinal: u32,
    timestamp: i64,
    line: u32,
) -> u64 {
    let Some(entry_type) = EntryType::from_u8(entry_type) else {
        return 0;
    };
    with_handle(handle, |buffer| {
        let row =
            buffer.append_log_interned(span_id, entry_type, message_ordinal, line, timestamp)?;
        Ok(pack(span_id, row as usize))
    })
    .unwrap_or(0)
}

#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub extern "C" fn thread_span_buffer_append_log_static(
    handle: u32,
    span_id: u32,
    entry_type: u8,
    message_id: u32,
    timestamp: i64,
    line: u32,
) -> u64 {
    let Some(entry_type) = EntryType::from_u8(entry_type) else {
        return 0;
    };
    let Ok(message_id) = VocabularyId::try_from(message_id) else {
        return 0;
    };
    with_handle(handle, |buffer| {
        let row = buffer.append_log_static(span_id, entry_type, message_id, line, timestamp)?;
        Ok(pack(span_id, row as usize))
    })
    .unwrap_or(0)
}

/// # Safety
/// `handle` must be a token previously returned by a live `thread_span_buffer_new*`
/// export, and every pointer/length pair must name a readable byte range for the
/// duration of the call; null pointers are valid only with zero lengths.
#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub unsafe extern "C" fn thread_span_buffer_append_log_dynamic(
    handle: u32,
    span_id: u32,
    entry_type: u8,
    message_ptr: *const u8,
    message_len: usize,
    timestamp: i64,
    line: u32,
) -> u64 {
    let Some(entry_type) = EntryType::from_u8(entry_type) else {
        return 0;
    };
    let Some(message) = dynamic_text(message_ptr, message_len) else {
        return 0;
    };
    with_handle(handle, |buffer| {
        let row = buffer.append_log(span_id, entry_type, Some(message), line, timestamp)?;
        Ok(pack(span_id, row as usize))
    })
    .unwrap_or(0)
}

/// # Safety
/// `handle` must be a token previously returned by a live `thread_span_buffer_new*`
/// export, and every pointer/length pair must name a readable byte range for the
/// duration of the call; null pointers are valid only with zero lengths.
#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub unsafe extern "C" fn thread_span_buffer_set_completion_message(
    handle: u32,
    span_id: u32,
    message_ptr: *const u8,
    message_len: usize,
) -> u8 {
    let Some(message) = dynamic_text(message_ptr, message_len) else {
        return STATUS_ERROR;
    };
    with_handle(handle, |buffer| {
        buffer.set_completion_message(span_id, message)
    })
    .map(|()| STATUS_OK)
    .unwrap_or(STATUS_ERROR)
}

#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub extern "C" fn thread_span_buffer_set_scope(
    handle: u32,
    span_id: u32,
    ordinal: u16,
    kind: u8,
    value: u64,
) -> u8 {
    with_handle(handle, |buffer| {
        buffer.set_scope_encoded(span_id, ordinal, kind, value)
    })
    .map(|()| STATUS_OK)
    .unwrap_or(STATUS_ERROR)
}

/// Linear-memory offset of `block`'s attribute cells, for a TypedArray view
/// (`lmao_core::AttributeCells` documents the layout). Zero when the block does
/// not exist yet or the schema has no attributes. The offset is stable for the
/// handle's life; a `memory.grow` only detaches views, it never moves cells.
#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub extern "C" fn thread_span_buffer_attribute_cells(handle: u32, block: u32) -> usize {
    with_handle(handle, |buffer| {
        Ok(buffer
            .attribute_cells(block as usize)
            .filter(|cells| !cells.is_empty())
            .map_or(0, |cells| cells.as_ptr() as usize))
    })
    .unwrap_or(0)
}

fn copy_out(dst: *mut u8, dst_len: usize, src: &[u8]) -> u32 {
    let needed = u32::try_from(src.len()).unwrap_or(u32::MAX);
    if dst.is_null() || dst_len < src.len() {
        return needed;
    }
    if !src.is_empty() {
        // SAFETY: the caller documents that `dst` is writable for `dst_len` bytes
        // and we just checked `dst_len >= src.len()`.
        unsafe {
            std::ptr::copy_nonoverlapping(src.as_ptr(), dst, src.len());
        }
    }
    needed
}

fn encode_attr(buffer: &mut ThreadSpanBuffer, row: usize, ordinal: u16) -> Option<(u8, u64)> {
    let value = buffer.attribute_at(row, ordinal)?;
    match value {
        ColumnValueRef::Number(value) => Some((ATTRIBUTE_KIND_NUMBER, value.to_bits())),
        ColumnValueRef::Uint64(value) => Some((ATTRIBUTE_KIND_UINT64, value)),
        ColumnValueRef::Boolean(value) => Some((ATTRIBUTE_KIND_BOOLEAN, u64::from(value))),
        ColumnValueRef::Enum(value) => Some((ATTRIBUTE_KIND_ENUM, u64::from(value))),
        ColumnValueRef::Text(value) => {
            let owned = value.to_owned();
            let id = buffer.intern(&owned).ok()?;
            Some((ATTRIBUTE_KIND_TEXT, u64::from(id)))
        }
    }
}

#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub extern "C" fn thread_span_buffer_row_count(handle: u32) -> u32 {
    with_handle(handle, |buffer| {
        u32::try_from(buffer.row_count())
            .map_err(|_| ThreadBufferError::InvalidRow(buffer.row_count()))
    })
    .unwrap_or(0)
}

#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub extern "C" fn thread_span_buffer_materialize_scope(
    handle: u32,
    start_row: u32,
    row_count: u32,
) -> u8 {
    with_handle(handle, |buffer| {
        buffer
            .materialize_scope_window(start_row as usize, row_count as usize)
            .map(|_| ())
    })
    .map(|()| STATUS_OK)
    .unwrap_or(STATUS_ERROR)
}

#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub extern "C" fn thread_span_buffer_read_timestamp(handle: u32, row: u32) -> i64 {
    with_handle(handle, |buffer| {
        buffer
            .timestamp_at(row as usize)
            .ok_or(ThreadBufferError::InvalidRow(row as usize))
    })
    .unwrap_or(0)
}

#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub extern "C" fn thread_span_buffer_read_span_id(handle: u32, row: u32) -> u32 {
    with_handle(handle, |buffer| {
        buffer
            .span_id_at(row as usize)
            .ok_or(ThreadBufferError::InvalidRow(row as usize))
    })
    .unwrap_or(0)
}

#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub extern "C" fn thread_span_buffer_read_header(handle: u32, row: u32) -> u32 {
    with_handle(handle, |buffer| {
        buffer
            .packed_header_at(row as usize)
            .ok_or(ThreadBufferError::InvalidRow(row as usize))
    })
    .unwrap_or(0)
}

#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub extern "C" fn thread_span_buffer_read_parent_span_id(handle: u32, row: u32) -> u32 {
    with_handle(handle, |buffer| {
        buffer
            .parent_span_id_at(row as usize)
            .ok_or(ThreadBufferError::InvalidRow(row as usize))
    })
    .unwrap_or(0)
}

#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub extern "C" fn thread_span_buffer_read_parent_thread_id(handle: u32, row: u32) -> u64 {
    with_handle(handle, |buffer| {
        buffer
            .parent_thread_id_at(row as usize)
            .ok_or(ThreadBufferError::InvalidRow(row as usize))
    })
    .unwrap_or(0)
}

#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub extern "C" fn thread_span_buffer_read_line(handle: u32, row: u32) -> u32 {
    with_handle(handle, |buffer| {
        buffer
            .line_at(row as usize)
            .ok_or(ThreadBufferError::InvalidRow(row as usize))
    })
    .unwrap_or(0)
}

/// Copy the row's trace id into `out_ptr`. Returns the UTF-8 length; zero is
/// failure. When `out_len` is too small the length is still returned and nothing
/// is written, so the caller can grow scratch and retry.
/// # Safety
/// `handle` must be a token previously returned by a live `thread_span_buffer_new*`
/// export, and every pointer/length pair must name a readable byte range for the
/// duration of the call; null pointers are valid only with zero lengths.
#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub unsafe extern "C" fn thread_span_buffer_read_trace_id(
    handle: u32,
    row: u32,
    out_ptr: *mut u8,
    out_len: usize,
) -> u32 {
    with_handle(handle, |buffer| {
        let value = buffer
            .trace_id_at(row as usize)
            .ok_or(ThreadBufferError::InvalidRow(row as usize))?;
        Ok(copy_out(out_ptr, out_len, value.as_bytes()))
    })
    .unwrap_or(0)
}

/// # Safety
/// `handle` must be a token previously returned by a live `thread_span_buffer_new*`
/// export, and every pointer/length pair must name a readable byte range for the
/// duration of the call; null pointers are valid only with zero lengths.
#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub unsafe extern "C" fn thread_span_buffer_read_message(
    handle: u32,
    row: u32,
    out_ptr: *mut u8,
    out_len: usize,
) -> u32 {
    with_handle(handle, |buffer| {
        let value = buffer.dynamic_message_at(row as usize).unwrap_or("");
        Ok(copy_out(out_ptr, out_len, value.as_bytes()))
    })
    .unwrap_or(0)
}

/// Write kind and scalar value for a present attribute. STATUS_ERROR means
/// the cell is null or the row/ordinal is invalid.
/// # Safety
/// `handle` must be a token previously returned by a live `thread_span_buffer_new*`
/// export, and every pointer/length pair must name a readable byte range for the
/// duration of the call; null pointers are valid only with zero lengths.
#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub unsafe extern "C" fn thread_span_buffer_read_attr(
    handle: u32,
    row: u32,
    ordinal: u16,
    out_kind: *mut u8,
    out_value: *mut u64,
) -> u8 {
    if out_kind.is_null() || out_value.is_null() {
        return STATUS_ERROR;
    }
    with_handle(handle, |buffer| {
        let (kind, value) = encode_attr(buffer, row as usize, ordinal)
            .ok_or(ThreadBufferError::InvalidColumnOrdinal(ordinal))?;
        // SAFETY: both pointers are non-null and caller-owned for this call.
        unsafe {
            *out_kind = kind;
            *out_value = value;
        }
        Ok(())
    })
    .map(|()| STATUS_OK)
    .unwrap_or(STATUS_ERROR)
}

/// # Safety
/// `handle` must be a token previously returned by a live `thread_span_buffer_new*`
/// export, and every pointer/length pair must name a readable byte range for the
/// duration of the call; null pointers are valid only with zero lengths.
#[cfg_attr(target_family = "wasm", unsafe(no_mangle))]
pub unsafe extern "C" fn thread_span_buffer_read_interned(
    handle: u32,
    ordinal: u32,
    out_ptr: *mut u8,
    out_len: usize,
) -> u32 {
    with_handle(handle, |buffer| {
        let value = buffer
            .interned(ordinal)
            .ok_or(ThreadBufferError::InvalidColumnOrdinal(
                u16::try_from(ordinal).unwrap_or(u16::MAX),
            ))?;
        Ok(copy_out(out_ptr, out_len, value.as_bytes()))
    })
    .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lmao_core::SYSTEM_COLUMN_COUNT;

    fn bytes(value: &str) -> (*const u8, usize) {
        (value.as_ptr(), value.len())
    }

    #[test]
    fn numeric_handle_adapter_uses_core_rows_and_overflow() {
        HANDLES.with(|handles| handles.borrow_mut().clear());
        let handle = thread_span_buffer_new(7, 8);
        assert_ne!(handle, 0);
        let (trace, trace_len) = bytes("trace");
        let (name, name_len) = bytes("root");
        let name_id = unsafe { thread_span_buffer_intern(handle, name, name_len) };
        assert_eq!(name_id, 1);
        let parent =
            unsafe { thread_span_buffer_open_span(handle, trace, trace_len, 0, 0, name_id, 10, 1) };
        assert_ne!(parent, 0);
        let parent_id = (parent >> 32) as u32;
        for timestamp in 11..20 {
            let row = thread_span_buffer_append_log(handle, parent_id, 5, name_id, timestamp, 2);
            assert_ne!(row, 0);
        }
        assert_eq!(
            thread_span_buffer_end(handle, parent_id, EntryType::SpanOk as u8, 20),
            STATUS_OK
        );
        assert_eq!(unsafe { thread_span_buffer_intern(0, trace, trace_len) }, 0);
        thread_span_buffer_free(handle);
        assert_eq!(
            thread_span_buffer_end(handle, parent_id, EntryType::SpanOk as u8, 21),
            STATUS_ERROR
        );
    }

    #[test]
    fn capacity_rejects_values_outside_core_domain() {
        assert_eq!(thread_span_buffer_new(7, 4), 0);
        assert_eq!(thread_span_buffer_new(7, 12), 0);
        assert_eq!(thread_span_buffer_new(7, 2048), 0);
        let handle = thread_span_buffer_new(7, 1024);
        assert_ne!(handle, 0);
        thread_span_buffer_free(handle);
    }

    #[test]
    fn row_reads_see_open_and_appended_rows() {
        HANDLES.with(|handles| handles.borrow_mut().clear());
        let handle = thread_span_buffer_new(7, 8);
        assert_ne!(handle, 0);
        let (span_id, start_row) = open_named(handle, "root");
        assert_eq!(thread_span_buffer_row_count(handle), 2);
        assert_eq!(thread_span_buffer_read_span_id(handle, start_row), span_id);
        assert_eq!(thread_span_buffer_read_timestamp(handle, start_row), 10);
        assert_eq!(
            thread_span_buffer_read_header(handle, start_row) & 0xff,
            u32::from(EntryType::SpanStart.as_u8())
        );
        let packed = thread_span_buffer_append_log(handle, span_id, 8, 1, 11, 3);
        assert_ne!(packed, 0);
        let log_row = packed as u32;
        assert_eq!(thread_span_buffer_row_count(handle), 3);
        assert_eq!(thread_span_buffer_read_timestamp(handle, log_row), 11);
        assert_eq!(thread_span_buffer_read_line(handle, log_row), 3);
        thread_span_buffer_free(handle);
    }

    fn number_field_blob() -> Vec<u8> {
        let mut blob = vec![ATTRIBUTE_KIND_NUMBER, 1];
        blob.extend_from_slice(b"n");
        blob
    }

    fn enum_field_blob() -> Vec<u8> {
        let mut blob = vec![ATTRIBUTE_KIND_ENUM, 1];
        blob.extend_from_slice(b"e");
        blob.extend_from_slice(&2u16.to_le_bytes());
        blob.push(1);
        blob.extend_from_slice(b"a");
        blob.push(1);
        blob.extend_from_slice(b"b");
        blob
    }

    fn open_named(handle: u32, name: &str) -> (u32, u32) {
        let (trace, trace_len) = bytes("trace");
        let (name, name_len) = bytes(name);
        let name_id = unsafe { thread_span_buffer_intern(handle, name, name_len) };
        let packed =
            unsafe { thread_span_buffer_open_span(handle, trace, trace_len, 0, 0, name_id, 10, 1) };
        assert_ne!(packed, 0);
        ((packed >> 32) as u32, packed as u32)
    }

    #[test]
    fn a_cell_stored_through_the_exported_address_reads_back_through_the_schema() {
        HANDLES.with(|handles| handles.borrow_mut().clear());
        let blob = [number_field_blob(), enum_field_blob()].concat();
        let handle = unsafe { thread_span_buffer_new_with_schema(7, 8, blob.as_ptr(), blob.len()) };
        assert_ne!(handle, 0);
        let (_span_id, row) = open_named(handle, "root");
        let cells = thread_span_buffer_attribute_cells(handle, 0) as *mut u64;
        assert!(!cells.is_null());
        assert_eq!(
            thread_span_buffer_attribute_cells(handle, 1),
            0,
            "no second block yet"
        );
        let stride = lmao_core::attribute_cells::stride(8);
        let local = row as usize;
        // What the TypedArray lane does: a Float64 store for field 0, a Uint32
        // store for field 1, and a validity bit for each.
        unsafe {
            *cells.cast::<f64>().add(local) = 1.5;
            *cells.cast::<u8>().add(8 * 8 + (local >> 3)) |= 1 << (local & 7);
            *cells.cast::<u32>().add((stride + local) * 2) = 7;
            *cells.cast::<u8>().add((stride + 8) * 8 + (local >> 3)) |= 1 << (local & 7);
        }
        let ordinal = u16::try_from(SYSTEM_COLUMN_COUNT).expect("system prefix fits u16");
        let (mut kind, mut value) = (0u8, 0u64);
        assert_eq!(
            unsafe { thread_span_buffer_read_attr(handle, row, ordinal, &mut kind, &mut value) },
            STATUS_OK
        );
        assert_eq!((kind, f64::from_bits(value)), (ATTRIBUTE_KIND_NUMBER, 1.5));
        // Variant 7 of a two-variant enum is a foreign writer's garbage: it
        // reads as absent rather than as a value.
        assert_eq!(
            unsafe {
                thread_span_buffer_read_attr(handle, row, ordinal + 1, &mut kind, &mut value)
            },
            STATUS_ERROR
        );
        thread_span_buffer_free(handle);

        let schemaless = thread_span_buffer_new(7, 8);
        assert_eq!(thread_span_buffer_attribute_cells(schemaless, 0), 0);
        thread_span_buffer_free(schemaless);
    }

    #[test]
    fn set_scope_after_overflow_fills_latest_value() {
        HANDLES.with(|handles| handles.borrow_mut().clear());
        let blob = {
            let mut blob = vec![ATTRIBUTE_KIND_TEXT, 4];
            blob.extend_from_slice(b"user");
            blob
        };
        let handle = unsafe { thread_span_buffer_new_with_schema(7, 8, blob.as_ptr(), blob.len()) };
        assert_ne!(handle, 0);
        let (span_id, _start) = open_named(handle, "root");
        let name_id = 1;
        for timestamp in 11..22 {
            assert_ne!(
                thread_span_buffer_append_log(handle, span_id, 8, name_id, timestamp, 2),
                0
            );
        }
        let rows = thread_span_buffer_row_count(handle);
        assert!(rows > 8);
        let ordinal = u16::try_from(SYSTEM_COLUMN_COUNT).expect("system prefix fits u16");
        let (user, user_len) = bytes("late");
        let user_id = unsafe { thread_span_buffer_intern(handle, user, user_len) };
        assert_ne!(user_id, 0);
        assert_eq!(
            thread_span_buffer_set_scope(
                handle,
                span_id,
                ordinal,
                ATTRIBUTE_KIND_TEXT,
                u64::from(user_id)
            ),
            STATUS_OK
        );
        assert_eq!(
            thread_span_buffer_materialize_scope(handle, 0, rows),
            STATUS_OK
        );
        let mut kind = 0u8;
        let mut value = 0u64;
        assert_eq!(
            unsafe {
                thread_span_buffer_read_attr(handle, rows - 1, ordinal, &mut kind, &mut value)
            },
            STATUS_OK
        );
        assert_eq!(kind, ATTRIBUTE_KIND_TEXT);
        assert_eq!(value, u64::from(user_id));
        thread_span_buffer_free(handle);
    }
}
