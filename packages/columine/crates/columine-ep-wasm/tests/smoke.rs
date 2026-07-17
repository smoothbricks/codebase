//! Native smoke tests through the extern layer — the caller-buffer protocol
//! parse-backend.ts drives (fixed offsets it chose; header + Arrow IPC read
//! back from the output span).

use columine_arrow::schema::{ArrowType, SignalSchemaField};
use columine_ep_wasm::{
    ep_create_log_entry, ep_create_with_schema, ep_create_with_schema_and_names, ep_destroy,
    ep_version,
};
use columine_event_processor::read_result_header;
use std::sync::{Mutex, MutexGuard};

// The extern layer routes through binary-root statics — single-threaded by
// contract on wasm, but the native test harness runs #[test]s on parallel
// threads sharing this process's statics. Serialize them; tolerate
// poisoning so one failure doesn't cascade.
static SERIAL: Mutex<()> = Mutex::new(());
fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

const SCHEMA_BYTES: [u8; 16] = [
    0xFF, 0xFF, 0xFF, 0xFF, 0x08, 0x00, 0x00, 0x00, //
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
];

fn base_fields() -> [SignalSchemaField; 4] {
    [
        SignalSchemaField::new(ArrowType::Utf8, false),  // id
        SignalSchemaField::new(ArrowType::Utf8, false),  // type
        SignalSchemaField::new(ArrowType::Int64, false), // timestamp
        SignalSchemaField::new(ArrowType::Binary, true), // value
    ]
}

fn create_processor(capacity: u32) -> u32 {
    let fields = base_fields();
    let names = b"id\0type\0timestamp\0value\0";
    unsafe {
        ep_create_with_schema_and_names(
            capacity,
            SCHEMA_BYTES.as_ptr(),
            SCHEMA_BYTES.len() as u32,
            fields.as_ptr(),
            fields.len() as u32,
            names.as_ptr(),
            names.len() as u32,
        )
    }
}

#[test]
fn version_is_stable() {
    let _serial = serial();
    assert_eq!(ep_version(), 2);
}

/// The parse-backend flow: caller-owned buffers, JSON in, header + IPC out.
#[test]
fn create_log_entry_via_caller_buffers() {
    let _serial = serial();
    let handle = create_processor(256);
    assert_ne!(handle, 0, "processor creation failed");

    let input = br#"[{"id":"e1","type":"click","timestamp":1,"value":{"x":1}}]"#;
    let mut output = vec![0u8; 256 * 1024];
    let code = unsafe {
        ep_create_log_entry(
            handle,
            input.as_ptr(),
            input.len() as u32,
            0, // INPUT_FORMAT_JSON
            output.as_mut_ptr(),
            output.len() as u32,
        )
    };
    assert_eq!(code, 0, "expected OK");
    let (hdr_code, _arrow_offset, arrow_len, events_processed, _reserved) =
        read_result_header(&output);
    assert_eq!(hdr_code, 0);
    assert_eq!(events_processed, 1);
    assert!(arrow_len > 0, "arrow IPC bytes expected");
    ep_destroy(handle);
}

/// Schema-without-names path creates, but JSON parsing has no key mapping.
#[test]
fn create_without_names_yields_handle() {
    let _serial = serial();
    let fields = base_fields();
    let handle = unsafe {
        ep_create_with_schema(
            256,
            SCHEMA_BYTES.as_ptr(),
            SCHEMA_BYTES.len() as u32,
            fields.as_ptr(),
            fields.len() as u32,
        )
    };
    assert_ne!(handle, 0);
    ep_destroy(handle);
}

/// Bad format byte refuses without touching the output buffer's header.
#[test]
fn invalid_format_refuses() {
    let _serial = serial();
    let handle = create_processor(16);
    assert_ne!(handle, 0);
    let mut output = vec![0u8; 1024];
    let code =
        unsafe { ep_create_log_entry(handle, [0u8; 1].as_ptr(), 0, 9, output.as_mut_ptr(), 1024) };
    assert_ne!(code, 0, "invalid format must refuse");
    ep_destroy(handle);
}

#[test]
fn handle_zero_is_reserved_and_full_table_creation_fails() {
    let _serial = serial();
    let handles = (0..255).map(|_| create_processor(1)).collect::<Vec<_>>();

    assert!(handles.iter().all(|handle| (1..=255).contains(handle)));
    let mut unique = handles.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), 255);
    assert_eq!(create_processor(1), 0, "a full handle table must refuse");

    for handle in handles {
        ep_destroy(handle);
    }
}

#[test]
fn destroyed_handle_is_reused_and_invalid_or_double_destroy_is_harmless() {
    let _serial = serial();
    let handles = (0..255).map(|_| create_processor(1)).collect::<Vec<_>>();
    assert!(handles.iter().all(|handle| *handle != 0));

    let reusable = handles[127];
    ep_destroy(0);
    ep_destroy(256);
    ep_destroy(u32::MAX);
    ep_destroy(reusable);
    ep_destroy(reusable);

    let replacement = create_processor(1);
    assert_eq!(replacement, reusable);

    ep_destroy(replacement);
    for handle in handles.into_iter().filter(|handle| *handle != reusable) {
        ep_destroy(handle);
    }
}

#[test]
fn handle_zero_is_never_returned_across_repeated_wraparound() {
    let _serial = serial();

    for cycle in 0..1_024 {
        let handle = create_processor(1);
        assert_ne!(handle, 0, "cycle {cycle} returned the failure sentinel");
        assert!((1..=255).contains(&handle));
        ep_destroy(handle);
    }
}
