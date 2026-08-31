//! Phase-A opaque-handle ABI for the shared thread span buffer.
//!
//! The registry and row model are deliberately local to this shim. They make the
//! ABI executable before `lmao-core::ThreadSpanBuffer` lands, while keeping every
//! exported signature identical to the core-backed implementation. Phase B
//! replaces `StubThreadSpanBuffer` and its adapter; callers do not change.

use std::cell::RefCell;
use std::str;

const OK: u8 = 0;
const INVALID_HANDLE: u8 = 1;
const INVALID_INPUT: u8 = 2;
const UNKNOWN_SPAN: u8 = 3;
const INVALID_COLUMN: u8 = 4;
const EXHAUSTED: u8 = 5;

const SPAN_START: u8 = 1;
const SPAN_OK: u8 = 2;
const SPAN_ERR: u8 = 3;
const SPAN_EXCEPTION: u8 = 4;
const SYSTEM_COLUMN_COUNT: u16 = 12;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Attr {
    ordinal: u16,
    kind: u8,
    value: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Row {
    span_id: u32,
    entry_type: u8,
    timestamp: i64,
    line: u32,
    message: Option<String>,
    attrs: Vec<Attr>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Span {
    span_id: u32,
    start_row: u32,
    completion_row: u32,
    parent_thread_id: u64,
    parent_span_id: u32,
    trace_id: Vec<u8>,
    open: bool,
}

#[derive(Debug)]
struct StubThreadSpanBuffer {
    thread_id: u64,
    capacity: usize,
    next_span_id: u32,
    rows: Vec<Row>,
    spans: Vec<Span>,
    scope: Vec<(u32, Attr)>,
    interned: Vec<String>,
}

impl StubThreadSpanBuffer {
    fn new(thread_id: u64, capacity: usize) -> Self {
        Self {
            thread_id,
            capacity,
            next_span_id: 1,
            rows: Vec::with_capacity(capacity),
            spans: Vec::with_capacity(capacity),
            scope: Vec::new(),
            interned: Vec::new(),
        }
    }

    fn next_span_id(&mut self) -> Result<u32, u8> {
        let span_id = self.next_span_id;
        if span_id == 0 {
            return Err(EXHAUSTED);
        }
        self.next_span_id = self.next_span_id.checked_add(1).ok_or(EXHAUSTED)?;
        Ok(span_id)
    }
    fn reserve_row(&self) -> Result<(), u8> {
        (self.rows.len() < self.capacity)
            .then_some(())
            .ok_or(EXHAUSTED)
    }

    fn push_span(
        &mut self,
        trace_id: &[u8],
        parent_thread_id: u64,
        parent_span_id: u32,
        message: Option<String>,
        timestamp: i64,
        line: u32,
    ) -> Result<u64, u8> {
        let _ = self.thread_id;
        let span_id = self.next_span_id()?;
        self.reserve_row()?;
        let start_row = u32::try_from(self.rows.len()).map_err(|_| EXHAUSTED)?;
        self.rows.push(Row {
            span_id,
            entry_type: SPAN_START,
            timestamp,
            line,
            message,
            attrs: Vec::new(),
        });
        self.reserve_row()?;
        let completion_row = u32::try_from(self.rows.len()).map_err(|_| EXHAUSTED)?;
        self.rows.push(Row {
            span_id,
            entry_type: SPAN_EXCEPTION,
            timestamp,
            line: 0,
            message: None,
            attrs: Vec::new(),
        });
        self.spans.push(Span {
            span_id,
            start_row,
            completion_row,
            parent_thread_id,
            parent_span_id,
            trace_id: trace_id.to_vec(),
            open: true,
        });
        Ok(pack(span_id, start_row))
    }

    fn append_log(
        &mut self,
        span_id: u32,
        entry_type: u8,
        message: Option<String>,
        timestamp: i64,
        line: u32,
    ) -> Result<u64, u8> {
        self.find_span(span_id)?;
        self.reserve_row()?;
        let row = u32::try_from(self.rows.len()).map_err(|_| EXHAUSTED)?;
        self.rows.push(Row {
            span_id,
            entry_type,
            timestamp,
            line,
            message,
            attrs: Vec::new(),
        });
        Ok(pack(span_id, row))
    }

    fn end(&mut self, span_id: u32, entry_type: u8, timestamp: i64) -> Result<(), u8> {
        let span = self.find_span(span_id)?.clone();
        let row = self
            .rows
            .get_mut(span.completion_row as usize)
            .ok_or(EXHAUSTED)?;
        row.entry_type = entry_type;
        row.timestamp = timestamp;
        self.spans
            .iter_mut()
            .find(|candidate| candidate.span_id == span_id)
            .ok_or(UNKNOWN_SPAN)?
            .open = false;
        Ok(())
    }

    fn write_attr(&mut self, row: u32, ordinal: u16, kind: u8, value: u64) -> Result<(), u8> {
        if ordinal < SYSTEM_COLUMN_COUNT || kind == 0 {
            return Err(INVALID_COLUMN);
        }
        let attrs = &mut self.rows.get_mut(row as usize).ok_or(INVALID_INPUT)?.attrs;
        if let Some(attr) = attrs.iter_mut().find(|attr| attr.ordinal == ordinal) {
            attr.kind = kind;
            attr.value = value;
        } else {
            attrs.push(Attr {
                ordinal,
                kind,
                value,
            });
        }
        Ok(())
    }

    fn write_tag(&mut self, span_id: u32, ordinal: u16, kind: u8, value: u64) -> Result<(), u8> {
        let row = self.find_span(span_id)?.start_row;
        self.write_attr(row, ordinal, kind, value)
    }

    fn set_scope(&mut self, span_id: u32, ordinal: u16, kind: u8, value: u64) -> Result<(), u8> {
        if ordinal < SYSTEM_COLUMN_COUNT || kind == 0 {
            return Err(INVALID_COLUMN);
        }
        self.find_span(span_id)?;
        let attr = Attr {
            ordinal,
            kind,
            value,
        };
        if let Some((_, existing)) = self
            .scope
            .iter_mut()
            .find(|(candidate, existing)| *candidate == span_id && existing.ordinal == ordinal)
        {
            *existing = attr;
        } else {
            self.scope.push((span_id, attr));
        }
        Ok(())
    }

    fn intern(&mut self, value: &str) -> Result<u32, u8> {
        if let Some((index, _)) = self
            .interned
            .iter()
            .enumerate()
            .find(|(_, existing)| existing.as_str() == value)
        {
            return u32::try_from(index + 1).map_err(|_| EXHAUSTED);
        }
        self.interned.push(value.to_owned());
        u32::try_from(self.interned.len()).map_err(|_| EXHAUSTED)
    }

    fn find_span(&self, span_id: u32) -> Result<&Span, u8> {
        self.spans
            .iter()
            .find(|span| span.span_id == span_id)
            .ok_or(UNKNOWN_SPAN)
    }
}

fn pack(span_id: u32, row: u32) -> u64 {
    (u64::from(span_id) << 32) | u64::from(row)
}

thread_local! {
    static HANDLES: RefCell<Vec<Option<StubThreadSpanBuffer>>> = const { RefCell::new(Vec::new()) };
}

fn with_handle<R>(
    handle: u32,
    f: impl FnOnce(&mut StubThreadSpanBuffer) -> Result<R, u8>,
) -> Result<R, u8> {
    if handle == 0 {
        return Err(INVALID_HANDLE);
    }
    HANDLES.with(|handles| {
        let mut handles = handles.borrow_mut();
        handles
            .get_mut(handle as usize - 1)
            .and_then(Option::as_mut)
            .ok_or(INVALID_HANDLE)
            .and_then(f)
    })
}

unsafe fn read_bytes<'a>(ptr: *const u8, len: usize) -> Result<&'a [u8], u8> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err(INVALID_INPUT);
    }
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
}

fn decode_utf8(ptr: *const u8, len: usize) -> Result<String, u8> {
    let bytes = unsafe { read_bytes(ptr, len) }?;
    str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| INVALID_INPUT)
}

fn decode_trace(ptr: *const u8, len: usize) -> Result<Vec<u8>, u8> {
    Ok(unsafe { read_bytes(ptr, len) }?.to_vec())
}

#[unsafe(no_mangle)]
pub extern "C" fn thread_span_buffer_new(thread_id: u64, capacity: u32) -> u32 {
    if capacity < 2 {
        return 0;
    }
    HANDLES.with(|handles| {
        let mut handles = handles.borrow_mut();
        let Some(slot) = handles.iter().position(Option::is_none) else {
            handles.push(Some(StubThreadSpanBuffer::new(
                thread_id,
                capacity as usize,
            )));
            return u32::try_from(handles.len()).unwrap_or(0);
        };
        handles[slot] = Some(StubThreadSpanBuffer::new(thread_id, capacity as usize));
        u32::try_from(slot + 1).unwrap_or(0)
    })
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

#[unsafe(no_mangle)]
pub extern "C" fn thread_span_buffer_open_span(
    handle: u32,
    trace_ptr: *const u8,
    trace_len: usize,
    parent_thread_id: u64,
    parent_span_id: u32,
    name_vocab: u32,
    timestamp: i64,
    line: u32,
) -> u64 {
    if name_vocab == 0 {
        return 0;
    }
    let trace_id = match decode_trace(trace_ptr, trace_len) {
        Ok(trace_id) => trace_id,
        Err(_) => return 0,
    };
    with_handle(handle, |buffer| {
        buffer.push_span(
            &trace_id,
            parent_thread_id,
            parent_span_id,
            Some(format!("vocab:{name_vocab}")),
            timestamp,
            line,
        )
    })
    .unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn thread_span_buffer_open_span_dynamic(
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
    let trace_id = match decode_trace(trace_ptr, trace_len) {
        Ok(trace_id) => trace_id,
        Err(_) => return 0,
    };
    let name = match decode_utf8(name_ptr, name_len) {
        Ok(name) => name,
        Err(_) => return 0,
    };
    with_handle(handle, |buffer| {
        buffer.push_span(
            &trace_id,
            parent_thread_id,
            parent_span_id,
            Some(name),
            timestamp,
            line,
        )
    })
    .unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn thread_span_buffer_end_ok(handle: u32, span_id: u32, timestamp: i64) -> u8 {
    with_handle(handle, |buffer| buffer.end(span_id, SPAN_OK, timestamp))
        .map(|()| OK)
        .unwrap_or_else(|error| error)
}

#[unsafe(no_mangle)]
pub extern "C" fn thread_span_buffer_end_err(handle: u32, span_id: u32, timestamp: i64) -> u8 {
    with_handle(handle, |buffer| buffer.end(span_id, SPAN_ERR, timestamp))
        .map(|()| OK)
        .unwrap_or_else(|error| error)
}

#[unsafe(no_mangle)]
pub extern "C" fn thread_span_buffer_append_log(
    handle: u32,
    span_id: u32,
    entry_type: u8,
    message_vocab: u32,
    timestamp: i64,
    line: u32,
) -> u64 {
    if entry_type == 0 || message_vocab == 0 {
        return 0;
    }
    with_handle(handle, |buffer| {
        buffer.append_log(
            span_id,
            entry_type,
            Some(format!("vocab:{message_vocab}")),
            timestamp,
            line,
        )
    })
    .unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn thread_span_buffer_append_log_dynamic(
    handle: u32,
    span_id: u32,
    entry_type: u8,
    message_ptr: *const u8,
    message_len: usize,
    timestamp: i64,
    line: u32,
) -> u64 {
    if entry_type == 0 {
        return 0;
    }
    let message = match decode_utf8(message_ptr, message_len) {
        Ok(message) => message,
        Err(_) => return 0,
    };
    with_handle(handle, |buffer| {
        buffer.append_log(span_id, entry_type, Some(message), timestamp, line)
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
        buffer.write_attr(row, ordinal, kind, value)
    })
    .map(|()| OK)
    .unwrap_or_else(|error| error)
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
        buffer.write_tag(span_id, ordinal, kind, value)
    })
    .map(|()| OK)
    .unwrap_or_else(|error| error)
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
        buffer.set_scope(span_id, ordinal, kind, value)
    })
    .map(|()| OK)
    .unwrap_or_else(|error| error)
}

#[unsafe(no_mangle)]
pub extern "C" fn thread_span_buffer_intern(handle: u32, ptr: *const u8, len: usize) -> u32 {
    let value = match decode_utf8(ptr, len) {
        Ok(value) => value,
        Err(_) => return 0,
    };
    with_handle(handle, |buffer| buffer.intern(&value)).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reset_handles() {
        HANDLES.with(|handles| handles.borrow_mut().clear());
    }

    #[test]
    fn rejects_capacity_without_a_start_and_completion_pair() {
        reset_handles();
        assert_eq!(thread_span_buffer_new(7, 0), 0);
        assert_eq!(thread_span_buffer_new(7, 1), 0);
    }

    #[test]
    fn kind_zero_is_invalid_and_intern_zero_is_not_an_id() {
        reset_handles();
        let handle = thread_span_buffer_new(7, 16);
        assert_ne!(handle, 0);
        let trace = b"trace";
        let packed =
            thread_span_buffer_open_span(handle, trace.as_ptr(), trace.len(), 0, 0, 11, 10, 4);
        let span_id = (packed >> 32) as u32;
        assert_ne!(span_id, 0);
        assert_eq!(
            thread_span_buffer_write_attr(handle, 0, SYSTEM_COLUMN_COUNT, 0, 1),
            INVALID_COLUMN
        );
        assert_eq!(
            thread_span_buffer_write_tag(handle, span_id, SYSTEM_COLUMN_COUNT, 0, 1),
            INVALID_COLUMN
        );
        assert_eq!(
            thread_span_buffer_set_scope(handle, span_id, SYSTEM_COLUMN_COUNT, 0, 1),
            INVALID_COLUMN
        );
        let bits = 1.5f64.to_bits();
        assert_eq!(
            thread_span_buffer_write_attr(handle, 0, SYSTEM_COLUMN_COUNT, 3, bits),
            OK
        );
        assert_eq!(thread_span_buffer_intern(0, trace.as_ptr(), trace.len()), 0);
        thread_span_buffer_free(handle);
        assert_eq!(
            thread_span_buffer_intern(handle, trace.as_ptr(), trace.len()),
            0
        );
    }

    #[test]
    fn static_and_dynamic_writes_share_the_same_row_contract() {
        reset_handles();
        let handle = thread_span_buffer_new(7, 16);
        assert_ne!(handle, 0);
        let trace = b"trace";
        let static_span =
            thread_span_buffer_open_span(handle, trace.as_ptr(), trace.len(), 0, 0, 11, 10, 4);
        let dynamic_name = b"dynamic";
        let dynamic_span = thread_span_buffer_open_span_dynamic(
            handle,
            trace.as_ptr(),
            trace.len(),
            7,
            (static_span >> 32) as u32,
            dynamic_name.as_ptr(),
            dynamic_name.len(),
            20,
            8,
        );
        assert_eq!(static_span as u32, 0);
        assert_eq!(dynamic_span as u32, 2);
        assert_ne!(static_span >> 32, 0);
        assert_ne!(dynamic_span >> 32, 0);

        let static_id = (static_span >> 32) as u32;
        let dynamic_id = (dynamic_span >> 32) as u32;
        let static_log = thread_span_buffer_append_log(handle, static_id, 5, 12, 30, 10);
        let message = b"hello";
        let dynamic_log = thread_span_buffer_append_log_dynamic(
            handle,
            dynamic_id,
            5,
            message.as_ptr(),
            message.len(),
            40,
            12,
        );
        assert_eq!(static_log as u32, 4);
        assert_eq!(dynamic_log as u32, 5);
        assert_eq!(
            thread_span_buffer_write_attr(handle, 0, SYSTEM_COLUMN_COUNT, 1, 9),
            OK
        );
        assert_eq!(
            thread_span_buffer_write_tag(handle, static_id, SYSTEM_COLUMN_COUNT + 1, 2, 10),
            OK
        );
        assert_eq!(
            thread_span_buffer_set_scope(handle, dynamic_id, SYSTEM_COLUMN_COUNT + 2, 3, 11),
            OK
        );
        assert_eq!(thread_span_buffer_end_ok(handle, static_id, 50), OK);
        assert_eq!(thread_span_buffer_end_err(handle, dynamic_id, 60), OK);
        assert_eq!(
            thread_span_buffer_intern(handle, message.as_ptr(), message.len()),
            1
        );
        assert_eq!(
            thread_span_buffer_intern(handle, message.as_ptr(), message.len()),
            1
        );
        thread_span_buffer_free(handle);
        assert_eq!(
            thread_span_buffer_end_ok(handle, static_id, 70),
            INVALID_HANDLE
        );
    }
}
