//! Native smoke tests through the extern layer — the caller-buffer protocol
//! parse-backend.ts drives (fixed offsets it chose; header + Arrow IPC read
//! back from the output span).

#[path = "../../columine-vm/tests/common/memory.rs"]
mod memory;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BinaryArray, StringArray, TimestampNanosecondArray};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use columine_arrow::schema::{
    ArrowType, DictionarySlot, IndexWidth, MAX_SCHEMA_FIELDS, MAX_SCHEMA_MESSAGE_BYTES,
    SignalSchemaField, schema_ipc_bytes,
};
use columine_ep_wasm::{
    MAX_EVENT_CAPACITY, ep_compact, ep_create_log_entry, ep_create_with_schema,
    ep_create_with_schema_and_dictionaries, ep_create_with_schema_and_names, ep_destroy,
    ep_version,
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
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("value", DataType::Binary, true),
    ])
});

fn base_fields() -> [SignalSchemaField; 4] {
    [
        SignalSchemaField::new(ArrowType::Utf8, false), // id
        SignalSchemaField::new(ArrowType::Utf8, false), // type
        SignalSchemaField::new_timestamp(TimeUnit::Nanosecond, false), // timestamp
        SignalSchemaField::new(ArrowType::Binary, true), // value
    ]
}

/// The eight-byte wire descriptors of `fields`, as the create exports take them.
fn wire(fields: &[SignalSchemaField]) -> Vec<u8> {
    fields.iter().flat_map(|field| field.to_wire()).collect()
}

fn create_processor(capacity: u32) -> u32 {
    let fields = base_fields();
    let names = b"id\0type\0timestamp\0value\0";
    unsafe {
        ep_create_with_schema_and_names(
            capacity,
            SCHEMA_BYTES.as_ptr(),
            SCHEMA_BYTES.len() as u32,
            wire(&fields).as_ptr(),
            fields.len() as u32,
            names.as_ptr(),
            names.len() as u32,
        )
    }
}

fn assert_create_log_entry_capacity_retry(input: &[u8], format: u8, expected_value: &[u8]) {
    let _serial = serial();
    let handle = create_processor(1);
    assert_ne!(handle, 0, "processor creation failed");
    // SAFETY: the handle stays live, and input and each output borrow are valid
    // for the synchronous call and do not overlap.
    let process = |output: &mut [u8]| unsafe {
        ep_create_log_entry(
            handle,
            input.as_ptr(),
            input.len() as u32,
            format,
            output.as_mut_ptr(),
            output.len() as u32,
        )
    };

    let mut first_output = vec![0xa5; 64 * 1024];
    let first_status = process(&mut first_output);
    let (first_header, arrow_offset, required_capacity, rows, duplicates) =
        read_result_header(&first_output);
    assert_eq!(
        first_status,
        ResultCode::OutputTooSmall as u32,
        "format {format}"
    );
    assert_eq!(
        (first_header, arrow_offset, rows, duplicates),
        (4, 32, 0, 0),
        "format {format}"
    );
    assert!(
        required_capacity > 64 * 1024,
        "large IPC required, format {format}"
    );
    assert_eq!(&first_output[20..32], &[0; 12]);
    assert!(
        first_output[32..].iter().all(|byte| *byte == 0xa5),
        "capacity refusal modified the IPC destination, format {format}"
    );

    // The writer's answer is the IPC capacity, not the total output capacity.
    let mut retry_output = vec![0; 32 + required_capacity as usize];
    let retry_status = process(&mut retry_output);
    let mut baseline = vec![0; 256 * 1024];
    let baseline_status = process(&mut baseline);
    ep_destroy(handle);

    assert_eq!(retry_status, ResultCode::Ok as u32, "format {format}");
    assert_eq!(
        read_result_header(&retry_output),
        (retry_status, 32, required_capacity, 1, 0),
        "one exact retry, format {format}"
    );
    assert_eq!(&retry_output[20..32], &[0; 12]);
    assert_eq!(baseline_status, ResultCode::Ok as u32, "format {format}");
    assert_eq!(
        read_result_header(&baseline),
        (baseline_status, 32, required_capacity, 1, 0),
        "generous-buffer baseline, format {format}"
    );
    let ipc = &retry_output[32..];
    assert_eq!(
        ipc,
        &baseline[32..32 + required_capacity as usize],
        "retry must preserve the complete baseline IPC, format {format}"
    );
    assert!(ipc.ends_with(&[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0]));
    let mut reader = StreamReader::try_new(Cursor::new(ipc), None).unwrap();
    let record = reader.next().unwrap().unwrap();
    assert!(
        reader.next().is_none(),
        "one complete batch, format {format}"
    );
    assert_eq!(record.num_rows(), 1);
    assert_eq!(record.num_columns(), 4);
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
        "large"
    );
    assert_eq!(
        record
            .column(2)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(0),
        0
    );
    let values = record
        .column(3)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert!(!values.is_null(0));
    assert_eq!(
        values.value(0),
        expected_value,
        "full value, format {format}"
    );
}

#[test]
fn create_log_entry_json_capacity_header_supports_one_exact_retry() {
    let value = format!("\"{}\"", "x".repeat(65_537));
    let input = format!(r#"[{{"id":"e1","type":"large","timestamp":0,"value":{value}}}]"#);
    assert_create_log_entry_capacity_retry(input.as_bytes(), 0, value.as_bytes());
}

#[test]
fn create_log_entry_msgpack_capacity_header_supports_one_exact_retry() {
    let text = |output: &mut Vec<u8>, value: &str| {
        output.push(0xdb);
        output.extend_from_slice(&u32::try_from(value.len()).unwrap().to_be_bytes());
        output.extend_from_slice(value.as_bytes());
    };
    let mut value = Vec::new();
    text(&mut value, &"x".repeat(65_537));
    let mut input = vec![0x91, 0x84];
    text(&mut input, "id");
    text(&mut input, "e1");
    text(&mut input, "type");
    text(&mut input, "large");
    text(&mut input, "timestamp");
    input.push(0);
    text(&mut input, "value");
    input.extend_from_slice(&value);
    assert_create_log_entry_capacity_retry(&input, 1, &value);
}

#[test]
fn create_log_entry_short_output_refuses_without_writes() {
    let _serial = serial();
    let handle = create_processor(1);
    assert_ne!(handle, 0, "processor creation failed");
    let input = br#"[{"id":"e1","type":"large","timestamp":0,"value":1}]"#;
    for length in 0..32 {
        let mut output = [0xa5; 32];
        // SAFETY: both buffers are live and disjoint, and length is within output.
        let status = unsafe {
            ep_create_log_entry(
                handle,
                input.as_ptr(),
                input.len() as u32,
                0,
                output.as_mut_ptr(),
                length,
            )
        };
        assert_eq!(status, ResultCode::OutputTooSmall as u32, "length {length}");
        assert_eq!(status, 4, "existing ABI status, length {length}");
        assert_eq!(
            output, [0xa5; 32],
            "short output was modified, length {length}"
        );
    }
    ep_destroy(handle);
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
        SignalSchemaField::new_timestamp(TimeUnit::Nanosecond, false),
        SignalSchemaField::new(ArrowType::Int64, true),
        SignalSchemaField::new(ArrowType::Utf8, true),
    ];
    let named_schema = ipc_schema_bytes(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("value.amount", DataType::Int64, true),
        Field::new("value.label", DataType::Utf8, true),
    ]);
    let named = || unsafe {
        let names = b"id\0type\0timestamp\0value.amount\0value.label\0";
        ep_create_with_schema_and_names(
            256,
            named_schema.as_ptr(),
            named_schema.len() as u32,
            wire(&named_fields).as_ptr(),
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
        SignalSchemaField::new_timestamp(TimeUnit::Nanosecond, false),
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
        SignalSchemaField::new_timestamp(TimeUnit::Nanosecond, false),
        SignalSchemaField::new_dictionary(DictionarySlot::Schema(0), IndexWidth::U8, true),
        SignalSchemaField::new(ArrowType::Binary, true),
        SignalSchemaField::new(ArrowType::Binary, true),
        SignalSchemaField::new_dictionary(DictionarySlot::Schema(1), IndexWidth::U8, true),
        SignalSchemaField::new(ArrowType::Binary, true),
        SignalSchemaField::new(ArrowType::Binary, true),
        SignalSchemaField::new(ArrowType::Binary, true),
    ];
    // An enum member's column is a schema dictionary of its variants.
    let dictionaries = [
        vec![Some("achieved".to_owned()), Some("stuck".to_owned())],
        vec![Some("human".to_owned())],
    ];
    let schema_message = schema_ipc_bytes(&fields).expect("schema message");
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
        ..columine_arrow::DynamicSchemaConfig::from_parts(
            &fields,
            columine_arrow::schema::SchemaParts {
                schema_message: &schema_message,
                field_names: b"id\0type\0timestamp\0value.outcome\0value.items\0value.meta\0authorRole@close\0value.$undeclared\0value.tally\0value.pick\0",
                dictionaries: columine_arrow::schema::SchemaDictionaryTable::Typed(&dictionaries),
            },
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
            wire(&fields).as_ptr(),
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

    descriptor(
        &mut batch,
        2,
        ArrowType::Timestamp,
        0,
        (0, 0),
        (0, 0),
        (176, 8),
    );
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
            .downcast_ref::<TimestampNanosecondArray>()
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
    let meta = wire(&fields);
    let meta_ptr = meta.as_ptr();
    let names = b"id\0type\0timestamp\0value\0";

    let create = |schema: &[u8], meta: *const u8, count: u32| unsafe {
        ep_create_with_schema(256, schema.as_ptr(), schema.len() as u32, meta, count)
    };

    // Under a memory ceiling, creation refuses with MemoryExhausted and
    // builds nothing, never traps. The schema decode is the one bounded
    // infallible step, so every ceiling from its need up to a whole handle's
    // answers either a handle or MemoryExhausted.
    let (decoded, decode_need) = memory::peak_bytes(|| {
        columine_arrow::DynamicSchemaConfig::from_wire_with_field_names(&SCHEMA_BYTES, &meta, names)
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
    let mut bad_tags = [0u8; 32];
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
        create(&unsigned, wire(&int32_meta).as_ptr(), 1),
        CreateFailure::SchemaTypeMismatch as u32
    );
    let handle = create(&signed, wire(&int32_meta).as_ptr(), 1);
    assert!(
        (1..=255).contains(&handle),
        "the signed twin must create: {handle:#x}"
    );
    ep_destroy(handle);

    // The same well-formed one-field schema, named past the message cap, is
    // refused by its length before the reader decodes it.
    let over_cap = ipc_schema_bytes(vec![Field::new(
        "v".repeat(MAX_SCHEMA_MESSAGE_BYTES),
        DataType::Int32,
        true,
    )]);
    assert_eq!(
        create(&over_cap, wire(&int32_meta).as_ptr(), 1),
        CreateFailure::SchemaTooLarge as u32
    );

    let non_null_meta = [SignalSchemaField::new(ArrowType::Int32, false)];
    assert_eq!(
        create(&signed, wire(&non_null_meta).as_ptr(), 1),
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

/// The physical field capacity holds end to end through the extern ABI. A
/// named schema of exactly `MAX_SCHEMA_FIELDS` nullable utf8 fields creates a
/// handle, and one compact row through it reads back every column, the last
/// field index included. Each column carries validity, offsets and data, so
/// compact validation fills its whole range table. One field more is refused
/// as `SchemaTooManyFields` whichever input carries it, and so is a metadata
/// count no table could hold.
#[test]
fn widest_schema_round_trips_and_one_more_field_is_refused() {
    let _serial = serial();
    let schema_of = |count: usize| {
        ipc_schema_bytes(
            (0..count)
                .map(|index| Field::new(format!("f{index}"), DataType::Utf8, true))
                .collect(),
        )
    };
    let widest = schema_of(MAX_SCHEMA_FIELDS);
    let over = schema_of(MAX_SCHEMA_FIELDS + 1);
    let fields = wire(&vec![
        SignalSchemaField::new(ArrowType::Utf8, true);
        MAX_SCHEMA_FIELDS + 1
    ]);
    let over_names: Vec<u8> = (0..=MAX_SCHEMA_FIELDS)
        .flat_map(|index| format!("f{index}\0").into_bytes())
        .collect();
    let widest_names = &over_names[..over_names.len() - format!("f{MAX_SCHEMA_FIELDS}\0").len()];
    let create = |schema: &[u8], count: u32, names: &[u8]| unsafe {
        ep_create_with_schema_and_names(
            1,
            schema.as_ptr(),
            schema.len() as u32,
            fields.as_ptr(),
            count,
            names.as_ptr(),
            names.len() as u32,
        )
    };
    let widest_count = MAX_SCHEMA_FIELDS as u32;
    let too_many = CreateFailure::SchemaTooManyFields as u32;
    assert_eq!(create(&widest, widest_count + 1, &over_names), too_many);
    assert_eq!(create(&over, widest_count, widest_names), too_many);
    assert_eq!(create(&over, widest_count + 1, &over_names), too_many);
    assert_eq!(create(&widest, u32::MAX, widest_names), too_many);
    assert_eq!(
        create(&widest, widest_count, &over_names),
        CreateFailure::SchemaFieldNames as u32
    );

    let handle = create(&widest, widest_count, widest_names);
    assert!(
        (1..=255).contains(&handle),
        "the widest schema must create: {handle:#x}"
    );

    let table_end = 16 + MAX_SCHEMA_FIELDS * COMPACT_DESCRIPTOR_SIZE;
    let mut batch = vec![0; table_end + MAX_SCHEMA_FIELDS * 24];
    write_u32(&mut batch, 0, COMPACT_BATCH_MAGIC);
    write_u16(&mut batch, 4, COMPACT_ABI_VERSION);
    write_u16(&mut batch, 6, COMPACT_DESCRIPTOR_SIZE as u16);
    write_u32(&mut batch, 8, 1);
    write_u32(&mut batch, 12, widest_count);
    for index in 0..MAX_SCHEMA_FIELDS {
        let value = format!("v{index}");
        let at = table_end + index * 24;
        let base = at as u32;
        descriptor(
            &mut batch,
            index,
            ArrowType::Utf8,
            1,
            (base, 1),
            (base + 8, 8),
            (base + 16, value.len() as u32),
        );
        batch[at] = 1;
        write_u32(&mut batch, at + 12, value.len() as u32);
        batch[at + 16..at + 16 + value.len()].copy_from_slice(value.as_bytes());
    }

    let mut probe = [0u8; 32];
    let probe_status = unsafe {
        ep_compact(
            handle,
            batch.as_ptr(),
            batch.len() as u32,
            probe.as_mut_ptr(),
            probe.len() as u32,
        )
    };
    assert_eq!(probe_status, ResultCode::EncodeError as u32);
    let (_, _, required_arrow_len, _, _) = read_result_header(&probe);
    let mut output = vec![0u8; 32 + usize::max(4096, required_arrow_len as usize)];
    let status = unsafe {
        ep_compact(
            handle,
            batch.as_ptr(),
            batch.len() as u32,
            output.as_mut_ptr(),
            output.len() as u32,
        )
    };
    assert_eq!(status, ResultCode::Ok as u32);
    let (_, arrow_offset, arrow_len, rows, _) = read_result_header(&output);
    assert_eq!(rows, 1);
    let ipc = &output[arrow_offset as usize..(arrow_offset + arrow_len) as usize];
    let mut reader = StreamReader::try_new(Cursor::new(ipc), None).unwrap();
    let record = reader.next().unwrap().unwrap();
    assert_eq!(record.num_columns(), MAX_SCHEMA_FIELDS);
    for index in 0..MAX_SCHEMA_FIELDS {
        assert_eq!(record.schema().field(index).name(), &format!("f{index}"));
        let column = record
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(column.value(0), format!("v{index}"), "field {index}");
    }
    ep_destroy(handle);
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

/// The texts of a dictionary-encoded column, `None` for a null row.
fn texts(column: &ArrayRef) -> Vec<Option<String>> {
    let dictionary = column.as_any_dictionary();
    let keys = dictionary.normalized_keys();
    let values = dictionary.values().as_string::<i32>();
    (0..column.len())
        .map(|row| (!column.is_null(row)).then(|| values.value(keys[row]).to_owned()))
        .collect()
}

/// Parse JSON `input` through `handle` and return the status and the output.
fn parse_json(handle: u32, input: &[u8]) -> (u32, Vec<u8>) {
    let mut output = vec![0u8; 64 * 1024];
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
    (code, output)
}

/// The single record batch of a successful result.
fn only_batch(output: &[u8]) -> arrow_array::RecordBatch {
    let (_, offset, length, ..) = read_result_header(output);
    let ipc = &output[offset as usize..(offset + length) as usize];
    let mut reader = StreamReader::try_new(Cursor::new(ipc), None).unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert!(reader.next().is_none());
    batch
}

//#region smoo/columine!n/vocab-wasm-schema-dictionary
/// A schema dictionary column created through the dictionary-table export
/// reads back as its declared values; the same schema without the table is
/// refused naming the dictionaries.
#[test]
fn a_schema_dictionary_created_with_its_table_reads_back_its_values() {
    let _serial = serial();
    let fields = [
        SignalSchemaField::new(ArrowType::Utf8, false),
        SignalSchemaField::new_dictionary(DictionarySlot::Schema(0), IndexWidth::U8, true),
    ];
    let schema = schema_ipc_bytes(&fields).unwrap();
    let meta = wire(&fields);
    let names = b"id\0status\0";
    // One dictionary of two values: [count][values][len][bytes]...
    let mut table = Vec::new();
    for word in [1u32, 2] {
        table.extend(word.to_le_bytes());
    }
    for value in ["open", "closed"] {
        table.extend(u32::try_from(value.len()).unwrap().to_le_bytes());
        table.extend(value.as_bytes());
    }
    let handle = unsafe {
        ep_create_with_schema_and_dictionaries(
            16,
            schema.as_ptr(),
            schema.len() as u32,
            meta.as_ptr(),
            fields.len() as u32,
            names.as_ptr(),
            names.len() as u32,
            table.as_ptr(),
            table.len() as u32,
        )
    };
    assert!((1..=255).contains(&handle), "created: {handle:#x}");
    let (code, output) = parse_json(handle, br#"[{"id":"a","status":"closed"}]"#);
    assert_eq!(code, ResultCode::Ok as u32);
    assert_eq!(
        texts(only_batch(&output).column(1)),
        [Some("closed".to_owned())]
    );
    ep_destroy(handle);

    let without_table = unsafe {
        ep_create_with_schema_and_names(
            16,
            schema.as_ptr(),
            schema.len() as u32,
            meta.as_ptr(),
            fields.len() as u32,
            names.as_ptr(),
            names.len() as u32,
        )
    };
    assert_eq!(without_table, CreateFailure::SchemaDictionary as u32);
}
//#endregion smoo/columine!n/vocab-wasm-schema-dictionary

//#region smoo/columine!n/vocab-wasm-image-undo
/// A handle's stream dictionary keeps no value of a refused batch: the next
/// accepted batch's dictionary message defines the dictionary from its own
/// values, so its stream reads on its own.
#[test]
fn a_refused_batch_leaves_no_stream_value_in_the_handle() {
    let _serial = serial();
    let fields = [
        SignalSchemaField::new(ArrowType::Utf8, false),
        SignalSchemaField::new_dictionary(DictionarySlot::Stream, IndexWidth::U16, true),
    ];
    let schema = schema_ipc_bytes(&fields).unwrap();
    let meta = wire(&fields);
    let names = b"id\0tag\0";
    let handle = unsafe {
        ep_create_with_schema_and_names(
            16,
            schema.as_ptr(),
            schema.len() as u32,
            meta.as_ptr(),
            fields.len() as u32,
            names.as_ptr(),
            names.len() as u32,
        )
    };
    assert!((1..=255).contains(&handle), "created: {handle:#x}");

    let (code, _) = parse_json(handle, br#"[{"id":"a","tag":"red"},{"id":"b","tag":7}]"#);
    assert_eq!(code, ResultCode::ParseError as u32, "a number is not a tag");

    let (code, output) = parse_json(handle, br#"[{"id":"c","tag":"blue"}]"#);
    assert_eq!(code, ResultCode::Ok as u32);
    assert_eq!(
        texts(only_batch(&output).column(1)),
        [Some("blue".to_owned())]
    );
    ep_destroy(handle);
}
//#endregion smoo/columine!n/vocab-wasm-image-undo
