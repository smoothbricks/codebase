//! Native smoke tests through the extern layer — the caller-buffer protocol
//! parse-backend.ts drives (fixed offsets it chose; header + Arrow IPC read
//! back from the output span).

#[path = "../../columine-vm/tests/common/memory.rs"]
mod memory;

use arrow_array::{Array, BinaryArray, Int64Array, StringArray};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use columine_arrow::schema::{ArrowType, SignalSchemaField};
use columine_ep_wasm::{
    MAX_EVENT_CAPACITY, ep_compact, ep_create_log_entry, ep_create_with_schema,
    ep_create_with_schema_and_names, ep_destroy, ep_version,
};
use columine_event_processor::{
    COMPACT_ABI_VERSION, COMPACT_BATCH_MAGIC, COMPACT_DESCRIPTOR_SIZE, CreateFailure,
    EventProcessor, InputFormat, ParseOptions, ResultCode, diagnostic_detail, read_result_header,
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

fn ipc_schema_bytes(fields: Vec<Field>) -> Vec<u8> {
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, &Schema::new(fields)).unwrap();
        writer.finish().unwrap();
    }
    // Drop the EOS marker: the create ABI takes the Schema message alone.
    bytes.truncate(bytes.len() - 8);
    bytes
}

static SCHEMA_BYTES: LazyLock<Vec<u8>> = LazyLock::new(|| {
    ipc_schema_bytes(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("value", DataType::Binary, true),
    ])
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

    // Under a memory ceiling a batch refuses with MemoryExhausted, never
    // traps, on the base scanner and on the named extractor, JSON and
    // MessagePack: values long enough to grow the columns, every ceiling
    // from nothing up to what a fresh handle's first batch needs.
    let label = "x".repeat(3000);
    let json: Vec<u8> = format!(
        "[{}]",
        (0..24)
            .map(|row| format!(
                r#"{{"id":"{row}-{label}","type":"t","timestamp":{row},"value":{{"amount":{row},"label":"{label}"}}}}"#
            ))
            .collect::<Vec<_>>()
            .join(",")
    )
    .into_bytes();
    let text = |out: &mut Vec<u8>, value: &str| {
        out.push(0xda);
        out.extend(u16::try_from(value.len()).unwrap().to_be_bytes());
        out.extend(value.bytes());
    };
    let mut msgpack = vec![0xdc, 0, 24];
    for row in 0..24i64 {
        msgpack.push(0x84);
        text(&mut msgpack, "id");
        text(&mut msgpack, &format!("{row}-{label}"));
        text(&mut msgpack, "type");
        text(&mut msgpack, "t");
        text(&mut msgpack, "timestamp");
        msgpack.push(0xd3);
        msgpack.extend(row.to_be_bytes());
        text(&mut msgpack, "value");
        msgpack.push(0x82);
        text(&mut msgpack, "amount");
        msgpack.push(0xd3);
        msgpack.extend(row.to_be_bytes());
        text(&mut msgpack, "label");
        text(&mut msgpack, &label);
    }
    let named_fields = [
        SignalSchemaField::new(ArrowType::Utf8, false),
        SignalSchemaField::new(ArrowType::Utf8, false),
        SignalSchemaField::new(ArrowType::Int64, false),
        SignalSchemaField::new(ArrowType::Int64, true),
        SignalSchemaField::new(ArrowType::Utf8, true),
    ];
    let named_schema = ipc_schema_bytes(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("value.amount", DataType::Int64, true),
        Field::new("value.label", DataType::Utf8, true),
    ]);
    let named = || unsafe {
        let names = b"id\0type\0timestamp\0value.amount\0value.label\0";
        ep_create_with_schema_and_names(
            256,
            named_schema.as_ptr(),
            named_schema.len() as u32,
            named_fields.as_ptr().cast(),
            named_fields.len() as u32,
            names.as_ptr(),
            names.len() as u32,
        )
    };
    let create_base = || create_processor(256);
    let creators: [&dyn Fn() -> u32; 2] = [&create_base, &named];
    for (create, (format, input)) in creators
        .iter()
        .flat_map(|create| [(create, (0, &json)), (create, (1, &msgpack))])
    {
        let process = |handle: u32, output: &mut [u8]| unsafe {
            ep_create_log_entry(
                handle,
                input.as_ptr(),
                input.len() as u32,
                format,
                output.as_mut_ptr(),
                output.len() as u32,
            )
        };
        let mut output = vec![0u8; 4 << 20];
        let handle = create();
        let (code, whole) = memory::peak_bytes(|| process(handle, &mut output));
        assert_eq!(
            code,
            ResultCode::Ok as u32,
            "unlimited batch, format {format}"
        );
        ep_destroy(handle);
        let mut refused = 0;
        for ceiling in (0..=whole).step_by(1024) {
            let handle = create();
            let code = memory::with_ceiling(ceiling, || process(handle, &mut output));
            ep_destroy(handle);
            if code == ResultCode::MemoryExhausted as u32 {
                refused += 1;
            } else {
                assert_eq!(
                    code,
                    ResultCode::Ok as u32,
                    "format {format}, ceiling {ceiling}"
                );
            }
        }
        assert!(refused > 0, "a ceiling under the batch's need refuses it");
    }

    // A processor that reports diagnostics names the refusal it returns:
    // a JSON carrier workspace that cannot grow is MEMORY_EXHAUSTED, not the
    // overflow that asked for the growth. Each event carries an undeclared
    // member longer than the starting workspace.
    let carried: Vec<u8> = format!(
        "[{}]",
        (0..8)
            .map(|row| format!(
                r#"{{"id":"{row}","type":"t","timestamp":{row},"value":{{"amount":{row},"note":"{label}"}}}}"#
            ))
            .collect::<Vec<_>>()
            .join(",")
    )
    .into_bytes();
    let carrier_fields = [
        SignalSchemaField::new(ArrowType::Utf8, false),
        SignalSchemaField::new(ArrowType::Utf8, false),
        SignalSchemaField::new(ArrowType::Int64, false),
        SignalSchemaField::new(ArrowType::Int64, true),
        SignalSchemaField::new(ArrowType::Binary, true),
    ];
    let processor = || {
        let config = columine_arrow::DynamicSchemaConfig::from_physical_fields_with_names(
            &carrier_fields,
            b"id\0type\0timestamp\0value.amount\0value.$undeclared\0",
        )
        .expect("carrier schema");
        EventProcessor::new(
            ParseOptions {
                base_path: true,
                msgpack_growth: true,
                diagnostics: true,
            },
            256,
            config,
        )
        .expect("named processor")
    };
    let (parsed, whole) =
        memory::peak_bytes(|| processor().parse(&carried, InputFormat::Json).map(|_| ()));
    parsed.expect("unlimited carrier batch");
    let mut refused = 0;
    for ceiling in (0..=whole).step_by(1024) {
        let mut ep = processor();
        let outcome = memory::with_ceiling(ceiling, || {
            ep.parse(&carried, InputFormat::Json).map(|_| ())
        });
        if let Err(error) = outcome {
            assert_eq!(error.code, ResultCode::MemoryExhausted, "ceiling {ceiling}");
            let detail = error.diagnostic.map(|diagnostic| diagnostic.detail);
            assert_eq!(
                detail,
                Some(diagnostic_detail::MEMORY_EXHAUSTED),
                "ceiling {ceiling}"
            );
            refused += 1;
        }
    }
    assert!(refused > 0);

    // A processor that judges batches against a semantic envelope refuses as
    // MemoryExhausted, never traps, at every allocation it makes: creating
    // it (envelope, discriminated members, open paths), judging and
    // extracting admitted JSON and MessagePack batches (nested objects,
    // enums, ranges, unions, records, a captured open member, a lookahead
    // for a `type` spelled after `value`), and spelling the diagnostic of a
    // refused one.
    semantic_batches_refuse_exhausted_memory();
}

fn semantic_batches_refuse_exhausted_memory() {
    use columine_arrow::schema::{DiscriminatedMember, PayloadDiscriminator};
    use columine_event_processor::{CreateFailure, EpInitError, ProcessError};

    let fields = [
        SignalSchemaField::new(ArrowType::Utf8, false),
        SignalSchemaField::new(ArrowType::Utf8, false),
        SignalSchemaField::new(ArrowType::Int64, false),
        SignalSchemaField::new(ArrowType::Int32, true),
        SignalSchemaField::new(ArrowType::Binary, true),
        SignalSchemaField::new(ArrowType::Binary, true),
        SignalSchemaField::new(ArrowType::Int32, true),
        SignalSchemaField::new(ArrowType::Binary, true),
    ];
    let envelope = br#"{"close":{"kind":"object","fields":{"outcome":{"kind":"optional","value":{"kind":"enum","variants":["achieved","stuck"]}},"authorRole":{"kind":"optional","value":{"kind":"enum","variants":["human"]}},"items":{"kind":"array","item":{"kind":"object","fields":{"sku":{"kind":"string"},"qty":{"kind":"range","min":0,"max":9,"integer":true}}}},"meta":{"kind":"object","open":true,"fields":{"note":{"kind":"optional","value":{"kind":"string"}}}},"tally":{"kind":"optional","value":{"kind":"record","value":{"kind":"number"}}},"pick":{"kind":"optional","value":{"kind":"union","variants":[{"kind":"string"},{"kind":"i32"}]}}}}}"#;
    let config = || {
        columine_arrow::DynamicSchemaConfig {
        semantic_schema: Some(envelope.to_vec()),
        payload_discriminator: Some(PayloadDiscriminator {
            key: "type".into(),
            members: vec![DiscriminatedMember {
                discriminant: "close".into(),
                member: "authorRole".into(),
                column: 6,
            }],
        }),
        ..columine_arrow::DynamicSchemaConfig::from_physical_fields_with_names(
            &fields,
            b"id\0type\0timestamp\0value.outcome\0value.items\0value.meta\0authorRole@close\0value.$undeclared\0",
        )
        .expect("semantic schema")
    }
    };
    let admitted_json = br#"[{"id":"a","type":"close","timestamp":1,"value":{"outcome":"stuck","authorRole":"human","items":[{"sku":"s1","qty":3},{"sku":"s2","qty":9}],"meta":{"note":"n","extra.key":"x"},"tally":{"a":1,"b":2.5},"pick":"p"}},{"id":"b","timestamp":2,"value":{"items":[],"meta":{},"pick":7},"type":"close"}]"#;
    let refused_json = br#"[{"id":"a","type":"close","timestamp":1,"value":{"items":[{"sku":"s1","qty":3},{"sku":"s2","qty":12}],"meta":{}}}]"#;

    // A minimal MessagePack writer for the same events.
    fn text(out: &mut Vec<u8>, value: &str) {
        out.push(0xd9);
        out.push(u8::try_from(value.len()).unwrap());
        out.extend(value.bytes());
    }
    fn int(out: &mut Vec<u8>, value: i64) {
        out.push(0xd3);
        out.extend(value.to_be_bytes());
    }
    let event = |out: &mut Vec<u8>, id: &str, qty: i64| {
        out.push(0x84);
        text(out, "id");
        text(out, id);
        text(out, "timestamp");
        int(out, 1);
        text(out, "type");
        text(out, "close");
        text(out, "value");
        out.push(0x84);
        text(out, "outcome");
        text(out, "achieved");
        text(out, "items");
        out.push(0x91);
        out.push(0x82);
        text(out, "sku");
        text(out, "s1");
        text(out, "qty");
        int(out, qty);
        text(out, "meta");
        out.push(0x82);
        text(out, "note");
        text(out, "n");
        text(out, "extra.key");
        text(out, "x");
        text(out, "tally");
        out.push(0x81);
        text(out, "a");
        int(out, 1);
    };
    let mut admitted_msgpack = vec![0x92];
    event(&mut admitted_msgpack, "a", 3);
    event(&mut admitted_msgpack, "b", 9);
    let mut refused_msgpack = vec![0x91];
    event(&mut refused_msgpack, "a", 12);

    #[derive(Debug)]
    enum Refusal {
        Init(EpInitError),
        Batch(ProcessError),
    }
    let batches = [
        (
            InputFormat::Json,
            &admitted_json[..],
            &refused_json[..],
            "value.items[1].qty",
        ),
        (
            InputFormat::Msgpack,
            &admitted_msgpack,
            &refused_msgpack,
            "value.items[0].qty",
        ),
    ];
    for (format, admitted, refused, refused_path) in batches {
        let ((rows, ep), budget) = memory::at_every_allocation(
            config,
            |config| {
                let mut ep = EventProcessor::new(
                    ParseOptions {
                        base_path: false,
                        msgpack_growth: true,
                        diagnostics: true,
                    },
                    16,
                    config,
                )
                .map_err(Refusal::Init)?;
                let rows = ep
                    .parse(admitted, format)
                    .map_err(Refusal::Batch)?
                    .row_count();
                match ep.parse(refused, format) {
                    Ok(_) => panic!("{format:?}: a quantity outside its range is admitted"),
                    Err(error) if error.code == ResultCode::ParseError => {}
                    Err(error) => return Err(Refusal::Batch(error)),
                }
                // Read once the budget is lifted: copying the diagnostic
                // here would spend the budget under test.
                Ok((rows, ep))
            },
            |budget, refusal| match refusal {
                Refusal::Init(error) => assert_eq!(
                    CreateFailure::from(error),
                    CreateFailure::MemoryExhausted,
                    "{format:?}, budget {budget}"
                ),
                Refusal::Batch(error) => {
                    assert_eq!(
                        error.code,
                        ResultCode::MemoryExhausted,
                        "{format:?}, budget {budget}"
                    );
                    assert_eq!(
                        error.diagnostic.map(|diagnostic| diagnostic.detail),
                        Some(diagnostic_detail::MEMORY_EXHAUSTED),
                        "{format:?}, budget {budget}"
                    );
                }
            },
        );
        assert_eq!(rows, 2, "{format:?}");
        let diagnostic = ep
            .payload_validation_diagnostic()
            .expect("a refused batch names its violation");
        assert_eq!(
            (diagnostic.path.as_str(), diagnostic.expected.as_str()),
            (refused_path, "safe integer in [0, 9]"),
            "{format:?}"
        );
        assert!(budget > 0, "{format:?}");
    }
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
    assert_eq!(
        create_processor(1),
        CreateFailure::HandlesExhausted as u32,
        "a full handle table must say so, not return a bare 0"
    );

    for handle in handles {
        ep_destroy(handle);
    }
}

/// Every refusal used to be a bare `0`, so a schema `TypeMismatch` was
/// indistinguishable from a capacity refusal — the ambiguity that let a real
/// regression sit behind a green test. Each cause must name itself.
#[test]
fn creation_failures_name_their_cause() {
    let _serial = serial();
    let fields = base_fields();
    let meta_ptr = fields.as_ptr().cast::<u8>();
    let names = b"id\0type\0timestamp\0value\0";

    let create = |schema: &[u8], meta: *const u8, count: u32| unsafe {
        ep_create_with_schema(256, schema.as_ptr(), schema.len() as u32, meta, count)
    };

    // Under a memory ceiling, creation refuses with MemoryExhausted and
    // builds nothing, never traps. The schema decode is the one bounded
    // infallible step, so every ceiling from its need up to a whole handle's
    // answers either a handle or MemoryExhausted.
    let (decoded, decode_need) = memory::peak_bytes(|| {
        columine_arrow::DynamicSchemaConfig::from_wire_with_field_names(
            &SCHEMA_BYTES,
            unsafe { std::slice::from_raw_parts(meta_ptr, 16) },
            names,
        )
    });
    decoded.expect("the base schema decodes");
    let (handle, whole) = memory::peak_bytes(|| create_processor(256));
    assert!(
        (1..=255).contains(&handle),
        "unlimited creation: {handle:#x}"
    );
    ep_destroy(handle);
    let mut refused = 0;
    for ceiling in (decode_need..=whole).step_by(64) {
        let handle = memory::with_ceiling(ceiling, || create_processor(256));
        if handle == CreateFailure::MemoryExhausted as u32 {
            refused += 1;
        } else {
            assert!(
                (1..=255).contains(&handle),
                "ceiling {ceiling}: {handle:#x}"
            );
            ep_destroy(handle);
        }
    }
    assert!(refused > 0, "some ceiling under a whole handle is refused");

    assert_eq!(create_processor(0), CreateFailure::Capacity as u32);
    assert_eq!(
        create_processor(MAX_EVENT_CAPACITY + 1),
        CreateFailure::Capacity as u32
    );

    // Refusal order: a request that can never succeed is refused on its
    // scalar precondition, not on whatever the schema decoder finds first.
    assert_eq!(
        unsafe { ep_create_with_schema(0, b"not a schema msg".as_ptr(), 16, meta_ptr, 4) },
        CreateFailure::Capacity as u32
    );

    assert_eq!(
        unsafe { ep_create_with_schema(256, std::ptr::null(), 0, meta_ptr, 4) },
        CreateFailure::BadRequest as u32
    );
    assert_eq!(
        unsafe {
            ep_create_with_schema_and_names(
                256,
                SCHEMA_BYTES.as_ptr(),
                SCHEMA_BYTES.len() as u32,
                meta_ptr,
                4,
                std::ptr::null(),
                names.len() as u32,
            )
        },
        CreateFailure::BadRequest as u32
    );

    assert_eq!(
        create(b"not a schema msg", meta_ptr, 4),
        CreateFailure::SchemaMessage as u32
    );
    assert_eq!(
        create(&SCHEMA_BYTES, meta_ptr, 3),
        CreateFailure::SchemaFieldCount as u32
    );

    // A physical tag byte outside the ArrowType enum. Derived from the table
    // rather than typed: this used to be a literal 9, which stopped being an
    // invalid tag the moment the plane set grew past it and turned this into a
    // test of UInt8 instead.
    let mut bad_tags = [0u8; 16];
    bad_tags[0] = ArrowType::ALL.len() as u8;
    assert_eq!(
        create(&SCHEMA_BYTES, bad_tags.as_ptr(), 4),
        CreateFailure::SchemaFieldMetadata as u32
    );

    // The regression this test exists for: tag 1 names the signed Int32
    // plane, so a UInt32 logical column does not match it and the refusal
    // must say TypeMismatch rather than a bare 0.
    let unsigned = ipc_schema_bytes(vec![Field::new("value", DataType::UInt32, true)]);
    let signed = ipc_schema_bytes(vec![Field::new("value", DataType::Int32, true)]);
    let int32_meta = [SignalSchemaField::new(ArrowType::Int32, true)];
    assert_eq!(
        create(&unsigned, int32_meta.as_ptr().cast(), 1),
        CreateFailure::SchemaTypeMismatch as u32
    );
    let handle = create(&signed, int32_meta.as_ptr().cast(), 1);
    assert!(
        (1..=255).contains(&handle),
        "the signed twin must create: {handle:#x}"
    );
    ep_destroy(handle);

    let non_null_meta = [SignalSchemaField::new(ArrowType::Int32, false)];
    assert_eq!(
        create(&signed, non_null_meta.as_ptr().cast(), 1),
        CreateFailure::SchemaNullability as u32
    );

    // Two names for a four-field schema.
    let short_names = b"id\0type\0";
    assert_eq!(
        unsafe {
            ep_create_with_schema_and_names(
                256,
                SCHEMA_BYTES.as_ptr(),
                SCHEMA_BYTES.len() as u32,
                meta_ptr,
                4,
                short_names.as_ptr(),
                short_names.len() as u32,
            )
        },
        CreateFailure::SchemaFieldNames as u32
    );
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
