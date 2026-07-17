//! Native smoke tests through the extern layer — the caller-buffer protocol
//! parse-backend.ts drives (fixed offsets it chose; header + Arrow IPC read
//! back from the output span).

use arrow_array::{Array, BinaryArray, Int64Array, StringArray};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use columine_arrow::schema::{ArrowType, SignalSchemaField};
use columine_ep_wasm::{
    ep_compact, ep_create_log_entry, ep_create_with_schema, ep_create_with_schema_and_names,
    ep_destroy, ep_version,
};
use columine_event_processor::{
    COMPACT_ABI_VERSION, COMPACT_BATCH_MAGIC, COMPACT_DESCRIPTOR_SIZE, ResultCode,
    read_result_header,
};
use std::io::Cursor;
use std::sync::{LazyLock, Mutex, MutexGuard};

// The extern layer routes through binary-root statics — single-threaded by
// contract on wasm, but the native test harness runs #[test]s on parallel
// threads sharing this process's statics. Serialize them; tolerate
// poisoning so one failure doesn't cascade.
static SERIAL: Mutex<()> = Mutex::new(());
fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

static SCHEMA_BYTES: LazyLock<Vec<u8>> = LazyLock::new(|| {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("value", DataType::Binary, true),
    ]);
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, &schema).unwrap();
        writer.finish().unwrap();
    }
    bytes.truncate(bytes.len() - 8);
    bytes
});

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
            fields.as_ptr().cast(),
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
            fields.as_ptr().cast(),
            fields.len() as u32,
        )
    };
    assert_ne!(handle, 0);
    ep_destroy(handle);
}
fn write_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn write_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn descriptor(
    batch: &mut [u8],
    index: usize,
    tag: ArrowType,
    flags: u8,
    validity: (u32, u32),
    offsets: (u32, u32),
    data: (u32, u32),
) {
    let base = 16 + index * COMPACT_DESCRIPTOR_SIZE;
    batch[base] = tag as u8;
    batch[base + 1] = flags;
    write_u32(batch, base + 4, validity.0);
    write_u32(batch, base + 8, validity.1);
    write_u32(batch, base + 12, offsets.0);
    write_u32(batch, base + 16, offsets.1);
    write_u32(batch, base + 20, data.0);
    write_u32(batch, base + 24, data.1);
}

fn one_row_compact_batch() -> Vec<u8> {
    let mut batch = vec![0; 200];
    write_u32(&mut batch, 0, COMPACT_BATCH_MAGIC);
    write_u16(&mut batch, 4, COMPACT_ABI_VERSION);
    write_u16(&mut batch, 6, COMPACT_DESCRIPTOR_SIZE as u16);
    write_u32(&mut batch, 8, 1);
    write_u32(&mut batch, 12, 4);

    descriptor(
        &mut batch,
        0,
        ArrowType::Utf8,
        0,
        (0, 0),
        (144, 8),
        (152, 2),
    );
    write_u32(&mut batch, 148, 2);
    batch[152..154].copy_from_slice(b"e1");

    descriptor(
        &mut batch,
        1,
        ArrowType::Utf8,
        0,
        (0, 0),
        (160, 8),
        (168, 5),
    );
    write_u32(&mut batch, 164, 5);
    batch[168..173].copy_from_slice(b"click");

    descriptor(&mut batch, 2, ArrowType::Int64, 0, (0, 0), (0, 0), (176, 8));
    batch[176..184].copy_from_slice(&1i64.to_le_bytes());

    descriptor(
        &mut batch,
        3,
        ArrowType::Binary,
        1,
        (184, 1),
        (192, 8),
        (0, 0),
    );
    batch
}

#[test]
fn compact_capacity_header_supports_one_exact_retry() {
    let _serial = serial();
    let handle = create_processor(1);
    assert_ne!(handle, 0);
    let batch = one_row_compact_batch();

    let mut first_output = [0u8; 32];
    let first_status = unsafe {
        ep_compact(
            handle,
            batch.as_ptr(),
            batch.len() as u32,
            first_output.as_mut_ptr(),
            first_output.len() as u32,
        )
    };
    let (first_header, arrow_offset, required_arrow_len, rows, duplicates) =
        read_result_header(&first_output);
    assert_eq!(first_status, ResultCode::EncodeError as u32);
    assert_eq!(first_header, first_status);
    assert_eq!(arrow_offset, 32);
    assert!(required_arrow_len > 0);
    assert_eq!((rows, duplicates), (0, 0));

    let mut retry_output = vec![0u8; 32 + usize::max(4096, required_arrow_len as usize)];
    let retry_status = unsafe {
        ep_compact(
            handle,
            batch.as_ptr(),
            batch.len() as u32,
            retry_output.as_mut_ptr(),
            retry_output.len() as u32,
        )
    };
    let (retry_header, retry_offset, retry_arrow_len, retry_rows, retry_duplicates) =
        read_result_header(&retry_output);
    assert_eq!(retry_status, ResultCode::Ok as u32);
    assert_eq!(retry_header, retry_status);
    assert_eq!(retry_offset, 32);
    assert_eq!(retry_arrow_len, required_arrow_len);
    assert_eq!((retry_rows, retry_duplicates), (1, 0));
    let ipc = &retry_output[retry_offset as usize..(retry_offset + retry_arrow_len) as usize];
    assert!(ipc.ends_with(&[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0]));
    let mut reader = StreamReader::try_new(Cursor::new(ipc), None).unwrap();
    let record = reader.next().unwrap().unwrap();
    assert!(reader.next().is_none());
    assert_eq!(record.num_rows(), 1);
    assert_eq!(
        record
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "e1"
    );
    assert_eq!(
        record
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "click"
    );
    assert_eq!(
        record
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    assert!(
        record
            .column(3)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .is_null(0)
    );
    ep_destroy(handle);
}

#[test]
fn compact_mirrors_invalid_handle_status_in_header() {
    let _serial = serial();
    let mut output = [0xff; 32];
    let status = unsafe {
        ep_compact(
            0,
            std::ptr::null(),
            0,
            output.as_mut_ptr(),
            output.len() as u32,
        )
    };
    let (header_status, arrow_offset, arrow_len, rows, duplicates) = read_result_header(&output);
    assert_eq!(status, ResultCode::InvalidHandle as u32);
    assert_eq!(header_status, status);
    assert_eq!((arrow_offset, arrow_len, rows, duplicates), (0, 0, 0, 0));
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
