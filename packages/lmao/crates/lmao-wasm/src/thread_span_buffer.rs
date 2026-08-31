//! Numeric-handle adapter for the canonical [`lmao_core::ThreadSpanBuffer`] ABI.
//!
//! Native callers use `lmao_core::thread_ffi`, whose opaque handle is a Rust
//! pointer. WASM callers cannot safely retain that pointer as a JavaScript
//! number, so this module owns a per-module slot table and keeps the pointer
//! private. The row store, lifecycle, parentage, overflow, and value decoding
//! remain in `lmao-core`; this file only validates slots and translates the
//! frozen numeric ABI.

use std::cell::RefCell;
use std::mem::ManuallyDrop;
use std::{collections::HashSet, str};

use lmao_core::{
    ATTRIBUTE_KIND_BOOLEAN, ATTRIBUTE_KIND_ENUM, ATTRIBUTE_KIND_NUMBER, ATTRIBUTE_KIND_TEXT,
    ATTRIBUTE_KIND_UINT64, ColumnValue, ColumnValueRef, EntryType, FieldMeta, FieldStrategy,
    ScopeValue, SharedStr, ThreadBufferError, ThreadSpanBuffer, TraceId, VocabularyId,
};

const STATUS_OK: u8 = 0;
const STATUS_ERROR: u8 = 1;
const SYSTEM_COLUMN_COUNT: usize = lmao_core::SYSTEM_COLUMN_COUNT;

thread_local! {
    static HANDLES: RefCell<Vec<Option<ThreadBufferSlot>>> = const { RefCell::new(Vec::new()) };
}

#[derive(Debug)]
enum ParsedFieldStrategy {
    Number,
    Uint64,
    Boolean,
    Text,
    Enum(Vec<String>),
}

#[derive(Debug)]
struct ParsedField {
    name: String,
    strategy: ParsedFieldStrategy,
}

#[derive(Debug)]
struct SchemaStorage {
    fields: Box<[FieldMeta]>,
    _names: Vec<Box<str>>,
    _enum_variants: Vec<Vec<Box<str>>>,
    _enum_values: Vec<Box<[&'static str]>>,
}

impl SchemaStorage {
    fn from_fields(fields: Vec<ParsedField>) -> Self {
        let mut names = Vec::with_capacity(fields.len());
        let mut enum_variants = Vec::new();
        let mut enum_values = Vec::new();
        let mut metadata = Vec::with_capacity(fields.len());
        for field in fields {
            names.push(field.name.into_boxed_str());
            let name_ptr = names.last().expect("just-pushed schema name").as_ref() as *const str;
            // SAFETY: `names` owns this allocation for the lifetime of this
            // storage, and `SchemaStorage` is dropped only after its buffer.
            let name: &'static str = unsafe { &*name_ptr };
            let strategy = match field.strategy {
                ParsedFieldStrategy::Number => FieldStrategy::Number,
                ParsedFieldStrategy::Uint64 => FieldStrategy::Uint64,
                ParsedFieldStrategy::Boolean => FieldStrategy::Boolean,
                ParsedFieldStrategy::Text => FieldStrategy::Text,
                ParsedFieldStrategy::Enum(values) => {
                    let owned_values = values
                        .into_iter()
                        .map(String::into_boxed_str)
                        .collect::<Vec<_>>();
                    let values = owned_values
                        .iter()
                        .map(|value| {
                            let value_ptr = value.as_ref() as *const str;
                            // SAFETY: the boxed strings are retained by
                            // `_enum_variants` for the schema lifetime.
                            unsafe { &*value_ptr }
                        })
                        .collect::<Vec<_>>()
                        .into_boxed_slice();
                    let values_ptr = values.as_ref() as *const [&'static str];
                    // SAFETY: the boxed slice is retained by `_enum_values`.
                    let values_ref: &'static [&'static str] = unsafe { &*values_ptr };
                    enum_variants.push(owned_values);
                    enum_values.push(values);
                    FieldStrategy::Enum(values_ref)
                }
            };
            metadata.push(FieldMeta::new(name, strategy));
        }
        Self {
            fields: metadata.into_boxed_slice(),
            _names: names,
            _enum_variants: enum_variants,
            _enum_values: enum_values,
        }
    }

    fn fields(&self) -> &'static [FieldMeta] {
        let fields_ptr = self.fields.as_ref() as *const [FieldMeta];
        // SAFETY: `ThreadBufferSlot` drops its buffer before this storage, and
        // the buffer is the only consumer of these metadata references.
        unsafe { &*fields_ptr }
    }
}

#[derive(Debug)]
struct ThreadBufferSlot {
    buffer: ManuallyDrop<ThreadSpanBuffer>,
    _schema: SchemaStorage,
}

impl ThreadBufferSlot {
    fn new(thread_id: u64, capacity: usize, fields: Vec<ParsedField>) -> Self {
        let schema = SchemaStorage::from_fields(fields);
        let buffer = ThreadSpanBuffer::new(thread_id, capacity, schema.fields());
        Self {
            buffer: ManuallyDrop::new(buffer),
            _schema: schema,
        }
    }
}

impl Drop for ThreadBufferSlot {
    fn drop(&mut self) {
        // SAFETY: the metadata owner remains alive until this explicit buffer
        // drop completes.
        unsafe { ManuallyDrop::drop(&mut self.buffer) };
    }
}

fn parse_schema(ptr: *const u8, len: usize) -> Option<Vec<ParsedField>> {
    let bytes = unsafe { bytes(ptr, len) }?;
    let mut cursor = 0;
    let mut names = HashSet::new();
    let mut fields = Vec::new();

    while cursor < bytes.len() {
        let kind = *bytes.get(cursor)?;
        cursor += 1;
        let name_len = usize::from(*bytes.get(cursor)?);
        cursor += 1;
        let name_end = cursor.checked_add(name_len)?;
        let name_bytes = bytes.get(cursor..name_end)?;
        cursor = name_end;
        let name = str::from_utf8(name_bytes).ok()?;
        if name.is_empty() || !names.insert(name) {
            return None;
        }

        let strategy = match kind {
            ATTRIBUTE_KIND_NUMBER => ParsedFieldStrategy::Number,
            ATTRIBUTE_KIND_UINT64 => ParsedFieldStrategy::Uint64,
            ATTRIBUTE_KIND_BOOLEAN => ParsedFieldStrategy::Boolean,
            ATTRIBUTE_KIND_TEXT => ParsedFieldStrategy::Text,
            ATTRIBUTE_KIND_ENUM => {
                let count_end = cursor.checked_add(2)?;
                let count_bytes = bytes.get(cursor..count_end)?;
                cursor = count_end;
                let count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]);
                if count == 0 {
                    return None;
                }
                let mut variants = Vec::with_capacity(usize::from(count));
                let mut variant_names = HashSet::with_capacity(usize::from(count));
                for _ in 0..count {
                    let variant_len = usize::from(*bytes.get(cursor)?);
                    cursor += 1;
                    let variant_end = cursor.checked_add(variant_len)?;
                    let variant_bytes = bytes.get(cursor..variant_end)?;
                    cursor = variant_end;
                    let variant = str::from_utf8(variant_bytes).ok()?;
                    if variant.is_empty() || !variant_names.insert(variant) {
                        return None;
                    }
                    variants.push(variant.to_owned());
                }
                ParsedFieldStrategy::Enum(variants)
            }
            _ => return None,
        };
        fields.push(ParsedField {
            name: name.to_owned(),
            strategy,
        });
    }

    let highest_ordinal = fields
        .len()
        .checked_add(SYSTEM_COLUMN_COUNT)?
        .checked_sub(1)?;
    u16::try_from(highest_ordinal).ok()?;
    Some(fields)
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
            .and_then(|slot| f(&mut slot.buffer))
    })
}

fn allocate_handle(thread_id: u64, capacity: usize, fields: Vec<ParsedField>) -> u32 {
    let slot = ThreadBufferSlot::new(thread_id, capacity, fields);
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

fn shared_string(ptr: *const u8, len: usize) -> Option<SharedStr> {
    let bytes = unsafe { bytes(ptr, len) }?;
    ThreadSpanBuffer::shared_utf8(bytes).ok()
}

fn write_value(buffer: &mut ThreadSpanBuffer, row: u32, ordinal: u16, kind: u8, value: u64) -> u8 {
    let Some(value) = buffer.decode_abi_value(kind, value) else {
        return STATUS_ERROR;
    };
    buffer
        .write_attr(row, ordinal, value)
        .map(|()| STATUS_OK)
        .unwrap_or(STATUS_ERROR)
}

#[unsafe(no_mangle)]
pub extern "C" fn thread_span_buffer_new(thread_id: u64, capacity: u32) -> u32 {
    let Some(capacity) = valid_capacity(capacity) else {
        return 0;
    };
    allocate_handle(thread_id, capacity, Vec::new())
}

/// Construct a schema-bearing buffer from the compact generated-schema blob.
///
/// Each field is `[kind:u8][name_len:u8][name bytes]`; enum fields append
/// `[variant_count:u16 LE]` and `[len:u8][variant bytes]` for each variant.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn thread_span_buffer_new_with_schema(
    thread_id: u64,
    capacity: u32,
    fields_ptr: *const u8,
    fields_len: usize,
) -> u32 {
    let Some(capacity) = valid_capacity(capacity) else {
        return 0;
    };
    let Some(fields) = parse_schema(fields_ptr, fields_len) else {
        return 0;
    };
    allocate_handle(thread_id, capacity, fields)
}

#[unsafe(no_mangle)]
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
#[unsafe(no_mangle)]
pub extern "C" fn thread_span_buffer_reset(handle: u32) -> i32 {
    match with_handle(handle, |buffer| {
        buffer.reset();
        Ok(())
    }) {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn thread_span_buffer_intern(handle: u32, ptr: *const u8, len: usize) -> u32 {
    let Some(bytes) = (unsafe { bytes(ptr, len) }) else {
        return 0;
    };
    with_handle(handle, |buffer| buffer.intern_utf8(bytes)).unwrap_or(0)
}

#[unsafe(no_mangle)]
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

#[unsafe(no_mangle)]
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

#[unsafe(no_mangle)]
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
    let Some(name) = shared_string(name_ptr, name_len) else {
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

#[unsafe(no_mangle)]
pub extern "C" fn thread_span_buffer_end_ok(handle: u32, span_id: u32, timestamp: i64) -> u8 {
    with_handle(handle, |buffer| buffer.end_ok(span_id, timestamp))
        .map(|()| STATUS_OK)
        .unwrap_or(STATUS_ERROR)
}

#[unsafe(no_mangle)]
pub extern "C" fn thread_span_buffer_end_err(handle: u32, span_id: u32, timestamp: i64) -> u8 {
    with_handle(handle, |buffer| buffer.end_err(span_id, timestamp))
        .map(|()| STATUS_OK)
        .unwrap_or(STATUS_ERROR)
}

#[unsafe(no_mangle)]
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

#[unsafe(no_mangle)]
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

#[unsafe(no_mangle)]
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
    let Some(message) = shared_string(message_ptr, message_len) else {
        return 0;
    };
    with_handle(handle, |buffer| {
        let row = buffer.append_log(span_id, entry_type, Some(message), line, timestamp)?;
        Ok(pack(span_id, row as usize))
    })
    .unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn thread_span_buffer_write_attr(
    handle: u32,
    row: u32,
    ordinal: u16,
    kind: u8,
    value: u64,
) -> u8 {
    with_handle(handle, |buffer| {
        Ok(write_value(buffer, row, ordinal, kind, value))
    })
    .unwrap_or(STATUS_ERROR)
}

#[unsafe(no_mangle)]
pub extern "C" fn thread_span_buffer_write_tag(
    handle: u32,
    span_id: u32,
    ordinal: u16,
    kind: u8,
    value: u64,
) -> u8 {
    with_handle(handle, |buffer| {
        let row = buffer
            .start_row(span_id)
            .and_then(|row| u32::try_from(row).ok())
            .ok_or(ThreadBufferError::UnknownSpan(span_id))?;
        Ok(write_value(buffer, row, ordinal, kind, value))
    })
    .unwrap_or(STATUS_ERROR)
}

#[unsafe(no_mangle)]
pub extern "C" fn thread_span_buffer_set_scope(
    handle: u32,
    span_id: u32,
    ordinal: u16,
    kind: u8,
    value: u64,
) -> u8 {
    with_handle(handle, |buffer| {
        let index = usize::from(ordinal)
            .checked_sub(SYSTEM_COLUMN_COUNT)
            .ok_or(ThreadBufferError::InvalidColumnOrdinal(ordinal))?;
        let field = buffer
            .schema_fields()
            .get(index)
            .ok_or(ThreadBufferError::InvalidColumnOrdinal(ordinal))?;
        // Kind 0 is the clear sentinel: 01i `setScope({ field: null })` deletes the
        // field. Decode would refuse kind 0, so the clear path never goes through it.
        if kind == 0 {
            let update = [(field.name, None)];
            return buffer.set_scope(span_id, &update);
        }
        let value = buffer
            .decode_abi_value(kind, value)
            .ok_or(ThreadBufferError::InvalidColumnOrdinal(ordinal))?;
        let scope_value = match value {
            ColumnValue::Number(value) => ScopeValue::Number(value),
            ColumnValue::Uint64(value) => ScopeValue::Uint64(value),
            ColumnValue::Boolean(value) => ScopeValue::Boolean(value),
            ColumnValue::Text(value) => ScopeValue::Text(value),
            ColumnValue::Enum(value) => ScopeValue::EnumIndex(value),
        };
        let update = [(field.name, Some(scope_value))];
        buffer.set_scope(span_id, &update)
    })
    .map(|()| STATUS_OK)
    .unwrap_or(STATUS_ERROR)
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

#[unsafe(no_mangle)]
pub extern "C" fn thread_span_buffer_row_count(handle: u32) -> u32 {
    with_handle(handle, |buffer| {
        u32::try_from(buffer.row_count())
            .map_err(|_| ThreadBufferError::InvalidRow(buffer.row_count()))
    })
    .unwrap_or(0)
}

#[unsafe(no_mangle)]
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

#[unsafe(no_mangle)]
pub extern "C" fn thread_span_buffer_read_timestamp(handle: u32, row: u32) -> i64 {
    with_handle(handle, |buffer| {
        buffer
            .timestamp_at(row as usize)
            .ok_or(ThreadBufferError::InvalidRow(row as usize))
    })
    .unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn thread_span_buffer_read_span_id(handle: u32, row: u32) -> u32 {
    with_handle(handle, |buffer| {
        buffer
            .span_id_at(row as usize)
            .ok_or(ThreadBufferError::InvalidRow(row as usize))
    })
    .unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn thread_span_buffer_read_header(handle: u32, row: u32) -> u32 {
    with_handle(handle, |buffer| {
        buffer
            .packed_header_at(row as usize)
            .ok_or(ThreadBufferError::InvalidRow(row as usize))
    })
    .unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn thread_span_buffer_read_parent_span_id(handle: u32, row: u32) -> u32 {
    with_handle(handle, |buffer| {
        buffer
            .parent_span_id_at(row as usize)
            .ok_or(ThreadBufferError::InvalidRow(row as usize))
    })
    .unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn thread_span_buffer_read_parent_thread_id(handle: u32, row: u32) -> u64 {
    with_handle(handle, |buffer| {
        buffer
            .parent_thread_id_at(row as usize)
            .ok_or(ThreadBufferError::InvalidRow(row as usize))
    })
    .unwrap_or(0)
}

#[unsafe(no_mangle)]
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
#[unsafe(no_mangle)]
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

#[unsafe(no_mangle)]
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
#[unsafe(no_mangle)]
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

#[unsafe(no_mangle)]
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
        assert_eq!(thread_span_buffer_end_ok(handle, parent_id, 20), STATUS_OK);
        assert_eq!(unsafe { thread_span_buffer_intern(0, trace, trace_len) }, 0);
        thread_span_buffer_free(handle);
        assert_eq!(
            thread_span_buffer_end_ok(handle, parent_id, 21),
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
    fn kind_zero_is_invalid_and_schema_ordinals_are_real() {
        HANDLES.with(|handles| handles.borrow_mut().clear());
        let blob = number_field_blob();
        let handle = unsafe { thread_span_buffer_new_with_schema(7, 8, blob.as_ptr(), blob.len()) };
        assert_ne!(handle, 0);
        let (span_id, row) = open_named(handle, "root");
        let ordinal = u16::try_from(SYSTEM_COLUMN_COUNT).expect("system prefix fits u16");
        let bits = 1.5f64.to_bits();
        assert_eq!(
            thread_span_buffer_write_attr(handle, row, ordinal, 0, bits),
            STATUS_ERROR
        );
        assert_eq!(
            thread_span_buffer_write_attr(handle, row, ordinal, ATTRIBUTE_KIND_NUMBER, bits),
            STATUS_OK
        );
        assert_eq!(
            thread_span_buffer_write_attr(handle, row, ordinal - 1, ATTRIBUTE_KIND_NUMBER, bits),
            STATUS_ERROR
        );
        assert_eq!(
            thread_span_buffer_write_attr(handle, row, ordinal + 1, ATTRIBUTE_KIND_NUMBER, bits),
            STATUS_ERROR
        );
        assert_eq!(
            thread_span_buffer_write_tag(handle, span_id, ordinal, 0, bits),
            STATUS_ERROR
        );
        assert_eq!(
            thread_span_buffer_set_scope(handle, span_id, ordinal, 0, bits),
            STATUS_OK
        );
        assert_eq!(
            thread_span_buffer_set_scope(handle, span_id, ordinal, ATTRIBUTE_KIND_NUMBER, bits),
            STATUS_OK
        );
        thread_span_buffer_free(handle);
    }

    #[test]
    fn enum_schema_blob_accepts_in_range_and_rejects_kind_zero() {
        HANDLES.with(|handles| handles.borrow_mut().clear());
        let blob = enum_field_blob();
        let handle = unsafe { thread_span_buffer_new_with_schema(7, 8, blob.as_ptr(), blob.len()) };
        assert_ne!(handle, 0);
        let (span_id, row) = open_named(handle, "root");
        let ordinal = u16::try_from(SYSTEM_COLUMN_COUNT).expect("system prefix fits u16");
        assert_eq!(
            thread_span_buffer_write_attr(handle, row, ordinal, 0, 0),
            STATUS_ERROR
        );
        assert_eq!(
            thread_span_buffer_write_attr(handle, row, ordinal, ATTRIBUTE_KIND_ENUM, 1),
            STATUS_OK
        );
        assert_eq!(
            thread_span_buffer_write_tag(handle, span_id, ordinal, ATTRIBUTE_KIND_ENUM, 2),
            STATUS_ERROR
        );
        thread_span_buffer_free(handle);
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
