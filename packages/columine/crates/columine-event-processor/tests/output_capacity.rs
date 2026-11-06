//! Public capacity-retry regressions for the complete Arrow IPC result.
//!
//! ParsedBatch's existing result header reports the minimum IPC writer
//! capacity on a typed output refusal. One exact caller-buffer resize must
//! produce the same complete stream as a generous output buffer.

use arrow_array::{Array, StringArray};
use arrow_ipc::{MessageHeader, reader::StreamReader, writer::StreamWriter};
use arrow_schema::{DataType, Field, Schema};
use columine_arrow::{
    ArrowType, DynamicSchemaConfig, MIN_ARROW_OUTPUT_CAPACITY, SignalSchemaField,
};
use columine_event_processor::{
    EventProcessor, InputFormat, ParseOptions, RESULT_HEADER_SIZE, ResultCode, read_result_header,
};
use std::io::Cursor;

// The native caller starts with a 64 KiB result buffer, including its header.
const INITIAL_OUTPUT_CAPACITY: usize = 64 * 1024;
const UNWRITTEN: u8 = 0xa5;
const BATCH_METADATA: [(&str, &str); 2] =
    [("source", "capacity-test-α"), ("batch.tag", "complete-λ")];

fn new_processor() -> EventProcessor {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("message", DataType::Utf8, false),
    ]);
    let mut schema_bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut schema_bytes, &schema).unwrap();
        writer.finish().unwrap();
    }
    // Construction accepts the Schema message, without the stream's EOS marker.
    assert!(schema_bytes.ends_with(&[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0]));
    schema_bytes.truncate(schema_bytes.len() - 8);
    let fields = [
        SignalSchemaField::new(ArrowType::Utf8, false),
        SignalSchemaField::new(ArrowType::Utf8, false),
    ];
    let config =
        DynamicSchemaConfig::with_field_names(&schema_bytes, &fields, b"id\0message\0").unwrap();
    EventProcessor::new(
        ParseOptions {
            base_path: false,
            msgpack_growth: true,
            diagnostics: true,
        },
        2,
        config,
    )
    .unwrap()
}

fn refused_capacity(output: &[u8]) -> usize {
    let (code, offset, required, rows, duplicates) = read_result_header(output);
    assert_eq!((code, offset, rows, duplicates), (4, 32, 0, 0));
    assert!(
        required > 0,
        "a capacity refusal must tell the caller how much to reserve"
    );
    assert!(output[20..RESULT_HEADER_SIZE].iter().all(|byte| *byte == 0));
    assert!(
        output[RESULT_HEADER_SIZE..]
            .iter()
            .all(|byte| *byte == UNWRITTEN),
        "capacity refusal must not mutate any Arrow output byte"
    );
    usize::try_from(required).unwrap()
}

fn successful_stream(output: &[u8], expected_rows: u32) -> &[u8] {
    let (code, offset, written, rows, duplicates) = read_result_header(output);
    assert_eq!((code, offset, rows, duplicates), (0, 32, expected_rows, 0));
    assert!(written > 0);
    assert!(output[20..RESULT_HEADER_SIZE].iter().all(|byte| *byte == 0));
    let start = usize::try_from(offset).unwrap();
    &output[start..start + usize::try_from(written).unwrap()]
}

fn assert_complete_batch(ipc: &[u8], expected: &[(&str, &str)]) {
    assert_eq!(&ipc[..4], &[0xff; 4]);
    let schema_len = usize::try_from(u32::from_le_bytes(ipc[4..8].try_into().unwrap())).unwrap();
    let schema = arrow_ipc::root_as_message(&ipc[8..8 + schema_len]).unwrap();
    assert_eq!(schema.header_type(), MessageHeader::Schema);
    assert!(
        schema.custom_metadata().is_none(),
        "batch metadata is not schema metadata"
    );

    let batch_start = 8 + schema_len;
    assert_eq!(&ipc[batch_start..batch_start + 4], &[0xff; 4]);
    let metadata_len = usize::try_from(u32::from_le_bytes(
        ipc[batch_start + 4..batch_start + 8].try_into().unwrap(),
    ))
    .unwrap();
    let message =
        arrow_ipc::root_as_message(&ipc[batch_start + 8..batch_start + 8 + metadata_len]).unwrap();
    assert_eq!(message.header_type(), MessageHeader::RecordBatch);
    assert_eq!(
        message.header_as_record_batch().unwrap().length(),
        i64::try_from(expected.len()).unwrap()
    );
    let metadata = message
        .custom_metadata()
        .expect("generic batch metadata survives encoding");
    assert_eq!(metadata.len(), BATCH_METADATA.len());
    for (actual, (key, value)) in metadata.iter().zip(BATCH_METADATA) {
        assert_eq!(actual.key(), Some(key));
        assert_eq!(actual.value(), Some(value));
    }
    assert!(ipc.ends_with(&[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0]));

    let mut reader = StreamReader::try_new(Cursor::new(ipc), None).unwrap();
    let batch = reader.next().expect("one complete record batch").unwrap();
    assert_eq!(batch.num_rows(), expected.len());
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.schema().field(0).name(), "id");
    assert_eq!(batch.schema().field(1).name(), "message");
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let messages = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(ids.null_count(), 0);
    assert_eq!(messages.null_count(), 0);
    for (row, (id, message)) in expected.iter().enumerate() {
        assert_eq!(ids.value(row), *id, "id at row {row}");
        assert_eq!(messages.value(row), *message, "message at row {row}");
    }
    assert!(
        reader.next().is_none(),
        "the stream has exactly one record batch"
    );
}

#[test]
fn large_json_capacity_refusal_retries_exactly_with_metadata() {
    // Match the existing long-input regression's 65,537-byte scale, with a
    // distinct Unicode suffix and a second row to expose truncation or loss.
    let long_message = format!("{}Ω-tail", "x".repeat(65_537));
    let input = format!(
        r#"[{{"id":"row-α","message":"{long_message}"}},{{"id":"row-two","message":"last-row-λ"}}]"#
    );
    let mut processor = new_processor();
    let mut initial = vec![UNWRITTEN; INITIAL_OUTPUT_CAPACITY];
    assert_eq!(
        processor.create_log_entry(
            input.as_bytes(),
            InputFormat::Json,
            &mut initial,
            &BATCH_METADATA,
        ),
        ResultCode::OutputTooSmall
    );
    let required = refused_capacity(&initial);
    assert!(
        required > INITIAL_OUTPUT_CAPACITY,
        "the reported Arrow IPC tail alone must exceed 64 KiB: {required}"
    );

    // The refusal is the only sizing authority: no preflight and no doubling.
    let mut exact = vec![UNWRITTEN; RESULT_HEADER_SIZE + required];
    let batch = processor
        .parse(input.as_bytes(), InputFormat::Json)
        .unwrap();
    assert_eq!(batch.row_count(), 2);
    assert_eq!(batch.encode(&mut exact, &BATCH_METADATA), ResultCode::Ok);
    let ipc = successful_stream(&exact, 2);
    assert_eq!(
        ipc.len(),
        required,
        "large results report the exact IPC length"
    );
    assert_eq!(exact.len(), RESULT_HEADER_SIZE + ipc.len());
    assert_complete_batch(
        ipc,
        &[("row-α", long_message.as_str()), ("row-two", "last-row-λ")],
    );

    let mut baseline_processor = new_processor();
    let mut baseline = vec![UNWRITTEN; 4 * INITIAL_OUTPUT_CAPACITY];
    assert_eq!(
        baseline_processor.create_log_entry(
            input.as_bytes(),
            InputFormat::Json,
            &mut baseline,
            &BATCH_METADATA,
        ),
        ResultCode::Ok
    );
    let baseline_ipc = successful_stream(&baseline, 2);
    assert_eq!(
        ipc, baseline_ipc,
        "exact retry must equal a generous-buffer encode byte for byte"
    );
    assert_eq!(
        &exact[..],
        &baseline[..RESULT_HEADER_SIZE + baseline_ipc.len()]
    );
    assert!(
        baseline[RESULT_HEADER_SIZE + baseline_ipc.len()..]
            .iter()
            .all(|byte| *byte == UNWRITTEN)
    );
}

#[test]
fn parsed_batch_encode_short_headers_leave_output_untouched() {
    let input = br#"[{"id":"tiny","message":"short"}]"#;
    let mut processor = new_processor();
    for length in [0, RESULT_HEADER_SIZE - 1] {
        let mut backing = [UNWRITTEN; 64];
        let batch = processor.parse(input, InputFormat::Json).unwrap();
        assert_eq!(batch.row_count(), 1);
        assert_eq!(
            batch.encode(&mut backing[..length], &BATCH_METADATA),
            ResultCode::OutputTooSmall,
            "a {length}-byte output cannot hold the result header"
        );
        assert_eq!(
            backing, [UNWRITTEN; 64],
            "no partial header or out-of-span write"
        );
    }
}

#[test]
fn parsed_batch_encode_header_only_reports_minimum_writer_capacity() {
    let input = br#"[{"id":"tiny","message":"short"}]"#;
    let mut processor = new_processor();
    let mut header = [UNWRITTEN; RESULT_HEADER_SIZE];
    assert_eq!(
        processor
            .parse(input, InputFormat::Json)
            .unwrap()
            .encode(&mut header, &BATCH_METADATA),
        ResultCode::OutputTooSmall
    );
    let required = refused_capacity(&header);
    assert_eq!(
        required, 4096,
        "tiny streams retain the IPC writer's capacity floor"
    );
    assert_eq!(required, MIN_ARROW_OUTPUT_CAPACITY);

    let mut below_floor = vec![UNWRITTEN; RESULT_HEADER_SIZE + required - 1];
    assert_eq!(
        processor
            .parse(input, InputFormat::Json)
            .unwrap()
            .encode(&mut below_floor, &BATCH_METADATA),
        ResultCode::OutputTooSmall
    );
    assert_eq!(refused_capacity(&below_floor), required);

    let mut exact = vec![UNWRITTEN; RESULT_HEADER_SIZE + required];
    assert_eq!(
        processor
            .parse(input, InputFormat::Json)
            .unwrap()
            .encode(&mut exact, &BATCH_METADATA),
        ResultCode::Ok
    );
    let ipc = successful_stream(&exact, 1);
    assert!(
        ipc.len() < required,
        "success reports actual bytes, not the capacity floor"
    );
    assert_complete_batch(ipc, &[("tiny", "short")]);
    assert!(
        exact[RESULT_HEADER_SIZE + ipc.len()..]
            .iter()
            .all(|byte| *byte == UNWRITTEN)
    );
}

#[test]
fn malformed_json_refusal_is_not_output_capacity() {
    let input = br#"[{"id":"broken","message":}]"#;
    let mut processor = new_processor();
    let error = match processor.parse(input, InputFormat::Json) {
        Err(error) => error,
        Ok(_) => panic!("malformed JSON must be refused before encoding"),
    };
    assert_eq!(error.code, ResultCode::ParseError);

    // Even a header-only output must not turn malformed input into a retry.
    for length in [RESULT_HEADER_SIZE, INITIAL_OUTPUT_CAPACITY] {
        let mut output = vec![UNWRITTEN; length];
        assert_eq!(
            processor.create_log_entry(input, InputFormat::Json, &mut output, &BATCH_METADATA),
            ResultCode::ParseError
        );
        assert_eq!(read_result_header(&output), (2, 0, 0, 0, 0));
        assert!(
            output[RESULT_HEADER_SIZE..]
                .iter()
                .all(|byte| *byte == UNWRITTEN)
        );
    }
}
