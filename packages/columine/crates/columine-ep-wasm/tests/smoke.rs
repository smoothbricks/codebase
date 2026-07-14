//! Native smoke tests through the extern layer: the exact C-ABI calls the
//! columine parse-backend makes (pointer-model: caller-provided buffers,
//! no reserve protocol). Semantics are pinned by columine-event-processor's
//! own suite; these tests pin the wrapper plumbing.

use columine_arrow::schema::{ArrowType, SignalSchemaField};
use columine_ep_wasm::{
    ep_create_log_entry, ep_create_with_schema_and_names, ep_destroy, ep_version,
};
use columine_event_processor::read_result_header;
use std::sync::{Mutex, MutexGuard};

// The extern layer routes through binary-root statics (the handle table) —
// single-threaded by contract on wasm, but the native test harness runs
// #[test]s on parallel threads sharing this process's statics. Serialize
// them; tolerate poisoning so one failure doesn't cascade.
static SERIAL: Mutex<()> = Mutex::new(());
fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn base_fields() -> [SignalSchemaField; 4] {
    [
        SignalSchemaField::new(ArrowType::Utf8, false),  // id
        SignalSchemaField::new(ArrowType::Utf8, false),  // type
        SignalSchemaField::new(ArrowType::Int64, false), // timestamp
        SignalSchemaField::new(ArrowType::Utf8, false),  // value (JSON passthrough)
    ]
}

/// Schema bytes are opaque to the wrapper (stored for IPC output); a
/// sentinel blob suffices for wrapper-plumbing tests.
const SCHEMA_SENTINEL: &[u8] = b"flechette-schema-bytes";
const FIELD_NAMES: &[u8] = b"id\0type\0timestamp\0value\0";

fn create_processor(capacity: u32) -> u32 {
    let fields = base_fields();
    unsafe {
        ep_create_with_schema_and_names(
            capacity,
            SCHEMA_SENTINEL.as_ptr(),
            SCHEMA_SENTINEL.len() as u32,
            fields.as_ptr(),
            fields.len() as u32,
            FIELD_NAMES.as_ptr(),
            FIELD_NAMES.len() as u32,
        )
    }
}

#[test]
fn version_matches_zig() {
    let _serial = serial();
    // packages/columine src/event_processor.zig `VERSION = 1`.
    assert_eq!(ep_version(), 1);
}

#[test]
fn create_log_entry_writes_header_and_ipc() {
    let _serial = serial();
    let handle = create_processor(64);
    assert_ne!(handle, 0, "processor created");
    let input = br#"[{"id":"e1","type":"click","timestamp":1705315800000000,"value":{"a":1}}]"#;
    let mut output = vec![0u8; 256 * 1024];
    let code = unsafe {
        ep_create_log_entry(
            handle,
            input.as_ptr(),
            input.len() as u32,
            0, // JSON
            output.as_mut_ptr(),
            output.len() as u32,
        )
    };
    assert_eq!(code, 0, "OK");
    let (hcode, _offset, arrow_len, processed, dupes) = read_result_header(&output);
    assert_eq!((hcode, processed), (0, 1));
    assert_eq!(dupes, 0, "no dedup under EpWiring::columine()");
    assert!(arrow_len > 0, "Arrow IPC body present");
    ep_destroy(handle);
}

#[test]
fn dedup_free_wiring_processes_identical_ids() {
    let _serial = serial();
    let handle = create_processor(64);
    let input = br#"[{"id":"dup-1","type":"click","timestamp":1705315800000000,"value":{}}]"#;
    let mut output = vec![0u8; 64 * 1024];
    for _ in 0..2 {
        let code = unsafe {
            ep_create_log_entry(
                handle,
                input.as_ptr(),
                input.len() as u32,
                0,
                output.as_mut_ptr(),
                output.len() as u32,
            )
        };
        assert_eq!(code, 0);
        let (_, _, _, processed, dupes) = read_result_header(&output);
        // Zig columine: "No dedup in columine - all events are processed".
        assert_eq!((processed, dupes), (1, 0));
    }
    ep_destroy(handle);
}

#[test]
fn boundary_refusals() {
    let _serial = serial();
    let handle = create_processor(16);
    let input = b"[]";
    let mut tiny = [0u8; 8]; // < RESULT_HEADER_SIZE
    let code = unsafe { ep_create_log_entry(handle, input.as_ptr(), 2, 0, tiny.as_mut_ptr(), 8) };
    assert_ne!(code, 0, "undersized output refused");
    let mut output = vec![0u8; 4096];
    let bad_format =
        unsafe { ep_create_log_entry(handle, input.as_ptr(), 2, 9, output.as_mut_ptr(), 4096) };
    assert_ne!(
        bad_format, 0,
        "unknown format byte refused (checked, Zig UB)"
    );
    let bad_handle =
        unsafe { ep_create_log_entry(9999, input.as_ptr(), 2, 0, output.as_mut_ptr(), 4096) };
    assert_ne!(bad_handle, 0, "invalid handle refused");
    ep_destroy(handle);
}
