//! Packed-u64 FFI contract: failure is bare 0, which must stay distinct from
//! a successful span whose start_row is 0 (`(span_id << 32) | 0`).

use lmao_core::thread_ffi::{
    thread_span_buffer_append_log, thread_span_buffer_free, thread_span_buffer_intern,
    thread_span_buffer_new, thread_span_buffer_open_span, thread_span_buffer_open_span_dynamic,
};

fn bytes(value: &str) -> (*const u8, usize) {
    (value.as_ptr(), value.len())
}

#[test]
fn failed_open_is_bare_zero_not_packed_row_zero() {
    let handle = thread_span_buffer_new(7, 8);
    assert!(!handle.is_null());
    let (trace, trace_len) = bytes("trace");

    let missing_handle = unsafe {
        thread_span_buffer_open_span(std::ptr::null_mut(), trace, trace_len, 0, 0, 1, 10, 1)
    };
    assert_eq!(missing_handle, 0, "null handle must return bare 0");

    let bad_intern =
        unsafe { thread_span_buffer_open_span(handle, trace, trace_len, 0, 0, 0, 10, 1) };
    assert_eq!(
        bad_intern, 0,
        "unknown intern ordinal must return bare 0, not (span_id<<32)|0"
    );

    let (name, name_len) = bytes("root");
    let name_id = unsafe { thread_span_buffer_intern(handle, name, name_len) };
    assert_ne!(name_id, 0);
    let packed =
        unsafe { thread_span_buffer_open_span(handle, trace, trace_len, 0, 0, name_id, 10, 1) };
    assert_ne!(
        packed, 0,
        "successful first span must not collapse to bare 0"
    );
    let span_id = (packed >> 32) as u32;
    let start_row = packed as u32;
    assert_ne!(span_id, 0);
    assert_eq!(
        start_row, 0,
        "first span occupies row 0; packed success is (span_id<<32)|0"
    );
    assert_eq!(packed, u64::from(span_id) << 32);
    assert_ne!(
        packed, bad_intern,
        "row-0 success must stay distinguishable from failure"
    );

    let unknown_log = unsafe {
        thread_span_buffer_append_log(handle, span_id.wrapping_add(1), 8, name_id, 11, 0)
    };
    assert_eq!(
        unknown_log, 0,
        "append_log of an unknown span must return bare 0"
    );

    unsafe { thread_span_buffer_free(handle) };
}

#[test]
fn dynamic_open_failure_is_bare_zero() {
    let handle = thread_span_buffer_new(7, 8);
    let (trace, trace_len) = bytes("trace");
    let (name, name_len) = bytes("dyn");
    let fail = unsafe {
        thread_span_buffer_open_span_dynamic(
            handle,
            trace,
            trace_len,
            0,
            0,
            std::ptr::null(),
            4,
            1,
            0,
        )
    };
    assert_eq!(
        fail, 0,
        "null name pointer with nonzero len must return bare 0"
    );
    let ok = unsafe {
        thread_span_buffer_open_span_dynamic(handle, trace, trace_len, 0, 0, name, name_len, 1, 0)
    };
    assert_ne!(ok, 0);
    unsafe { thread_span_buffer_free(handle) };
}
