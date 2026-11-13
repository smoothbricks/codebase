//! The physical vocabulary through `EventProcessor`
//! (specs/columine/10-physical-vocabulary.md §10): each test drives a public
//! entry point and reads the result back with `arrow-ipc`'s `StreamReader`.

use arrow_array::cast::AsArray;
use arrow_array::types::{
    DurationMillisecondType, Int64Type, TimestampMillisecondType, TimestampNanosecondType,
};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_ipc::MessageHeader;
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, Field, TimeUnit};
use columine_arrow::{
    ArrowType, DictionarySlot, DynamicSchemaConfig, FIELD_IDENTITY_KEY, IndexWidth,
    SchemaDictionaryTable, SchemaParts, SignalSchemaField, logical_schema_ipc_bytes,
    schema_ipc_bytes,
};
use columine_event_processor::{
    COMPACT_ABI_VERSION, COMPACT_BATCH_MAGIC, COMPACT_DESCRIPTOR_SIZE, COMPACT_DIAGNOSTIC_STAGE,
    COMPACT_HEADER_SIZE, CategoryImage, EpInitError, EventProcessor, InputFormat, ListImage,
    NoCategories, ParseOptions, ResultCode, compact_detail, diagnostic_detail, read_result_header,
};
use columine_parsing::identity::{FieldIdentity, IdentityError};
use std::collections::HashMap;
use std::io::Cursor;

const OUTPUT: usize = 64 * 1024;

fn options() -> ParseOptions {
    ParseOptions {
        base_path: false,
        msgpack_growth: true,
        diagnostics: true,
    }
}

/// A schema of `fields` named by `names`, with `dictionaries` as its schema
/// dictionary table and `envelope` as its semantic schema.
fn schema(
    fields: &[SignalSchemaField],
    names: &[u8],
    dictionaries: &[Vec<Option<String>>],
    envelope: Option<&str>,
) -> DynamicSchemaConfig {
    let message = schema_ipc_bytes(fields).unwrap();
    DynamicSchemaConfig {
        semantic_schema: envelope.map(|envelope| envelope.as_bytes().to_vec()),
        ..DynamicSchemaConfig::from_parts(
            fields,
            SchemaParts {
                schema_message: &message,
                field_names: names,
                dictionaries: SchemaDictionaryTable::Typed(dictionaries),
            },
        )
        .unwrap()
    }
}

fn processor(config: DynamicSchemaConfig) -> EventProcessor {
    EventProcessor::new(options(), 8, config).unwrap()
}

fn values(texts: &[&str]) -> Vec<Option<String>> {
    texts.iter().map(|text| Some((*text).to_owned())).collect()
}

/// The Arrow IPC stream of a successful result.
fn stream(output: &[u8]) -> &[u8] {
    let (code, offset, length, ..) = read_result_header(output);
    assert_eq!(code, ResultCode::Ok as u32, "the batch is accepted");
    &output[offset as usize..(offset + length) as usize]
}

fn batches(stream: &[u8]) -> Vec<RecordBatch> {
    StreamReader::try_new(Cursor::new(stream), None)
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

/// The header type, and for a DictionaryBatch its delta flag and value
/// count, of each message of `stream` from byte `skip` on, up to EOS.
fn messages(stream: &[u8], skip: usize) -> Vec<(MessageHeader, bool, i64)> {
    let mut found = Vec::new();
    let mut at = skip;
    while at + 8 <= stream.len() {
        let length = u32::from_le_bytes(stream[at + 4..at + 8].try_into().unwrap()) as usize;
        if length == 0 {
            break;
        }
        let message = arrow_ipc::root_as_message(&stream[at + 8..at + 8 + length]).unwrap();
        let (delta, count) = match message.header_as_dictionary_batch() {
            Some(dictionary) => (dictionary.isDelta(), dictionary.data().unwrap().length()),
            None => (false, 0),
        };
        found.push((message.header_type(), delta, count));
        at += 8 + length + usize::try_from(message.bodyLength()).unwrap();
    }
    found
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

/// A minimal MessagePack writer for the test events.
enum Mp<'a> {
    Str(&'a str),
    Int(i64),
    Map(Vec<(&'a str, Mp<'a>)>),
}

fn msgpack(out: &mut Vec<u8>, value: &Mp<'_>) {
    match value {
        Mp::Str(text) => {
            out.push(0xd9);
            out.push(u8::try_from(text.len()).unwrap());
            out.extend(text.as_bytes());
        }
        Mp::Int(value) => {
            out.push(0xd3);
            out.extend(value.to_be_bytes());
        }
        Mp::Map(entries) => {
            out.push(0x80 | u8::try_from(entries.len()).unwrap());
            for (key, value) in entries {
                msgpack(out, &Mp::Str(key));
                msgpack(out, value);
            }
        }
    }
}

/// A MessagePack array of `events`.
fn msgpack_batch(events: &[Mp<'_>]) -> Vec<u8> {
    let mut out = vec![0x90 | u8::try_from(events.len()).unwrap()];
    for event in events {
        msgpack(&mut out, event);
    }
    out
}

fn create(ep: &mut EventProcessor, input: &[u8], format: InputFormat) -> (ResultCode, Vec<u8>) {
    let mut output = vec![0; OUTPUT];
    let code = ep.create_log_entry(input, format, &mut output, &[]);
    (code, output)
}

//#region smoo/columine!n/vocab-temporal-instant
/// `at` is a millisecond Timestamp column, `n` an Int64 column.
fn instant_schema() -> DynamicSchemaConfig {
    schema(
        &[
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new_timestamp(TimeUnit::Millisecond, false),
            SignalSchemaField::new(ArrowType::Int64, false),
        ],
        b"id\0at\0n\0",
        &[],
        None,
    )
}

/// One event `{id, at, n}` on the wire `format`.
fn instant_event(format: InputFormat, at: &str, n: Mp<'_>) -> Vec<u8> {
    match format {
        InputFormat::Json => {
            let n = match n {
                Mp::Str(text) => format!("\"{text}\""),
                Mp::Int(value) => value.to_string(),
                Mp::Map(_) => unreachable!("an instant test sends no map"),
            };
            format!(r#"[{{"id":"a","at":"{at}","n":{n}}}]"#).into_bytes()
        }
        InputFormat::Msgpack => msgpack_batch(&[Mp::Map(vec![
            ("id", Mp::Str("a")),
            ("at", Mp::Str(at)),
            ("n", n),
        ])]),
        InputFormat::ArrowPassthrough | InputFormat::MsgpackStream => {
            unreachable!("the instant tests drive JSON and MessagePack")
        }
    }
}

#[test]
fn an_instant_is_stored_in_its_columns_unit_on_both_lanes() {
    for format in [InputFormat::Json, InputFormat::Msgpack] {
        let mut ep = processor(instant_schema());
        let (code, output) = create(
            &mut ep,
            &instant_event(format, "2024-01-15T10:30:00.123Z", Mp::Int(7)),
            format,
        );
        assert_eq!(code, ResultCode::Ok, "{format:?}");
        let batch = &batches(stream(&output))[0];
        assert_eq!(
            batch.schema().field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );
        assert_eq!(
            batch
                .column(1)
                .as_primitive::<TimestampMillisecondType>()
                .value(0),
            1_705_314_600_123,
            "{format:?}"
        );
        assert_eq!(batch.column(2).as_primitive::<Int64Type>().value(0), 7);
    }
}

#[test]
fn an_instant_finer_than_its_columns_unit_is_refused_on_both_lanes() {
    for format in [InputFormat::Json, InputFormat::Msgpack] {
        let mut ep = processor(instant_schema());
        let (code, output) = create(
            &mut ep,
            &instant_event(format, "2024-01-15T10:30:00.1234Z", Mp::Int(7)),
            format,
        );
        assert_eq!(code, ResultCode::ParseError, "{format:?}");
        assert_eq!(read_result_header(&output).0, ResultCode::ParseError as u32);
    }
}

#[test]
fn an_instant_is_refused_by_an_int64_column_on_both_lanes() {
    for format in [InputFormat::Json, InputFormat::Msgpack] {
        let mut ep = processor(instant_schema());
        let (code, _) = create(
            &mut ep,
            &instant_event(
                format,
                "2024-01-15T10:30:00.123Z",
                Mp::Str("2024-01-15T10:30:00Z"),
            ),
            format,
        );
        assert_eq!(code, ResultCode::ParseError, "{format:?}");
    }
}
//#endregion smoo/columine!n/vocab-temporal-instant

//#region smoo/columine!n/vocab-enum-dictionary
const STATUS_ENVELOPE: &str = r#"{"order":{"kind":"object","fields":{"status":{"kind":"enum","variants":["open","closed"]}}}}"#;

/// The event log with an enum member stored in a schema-dictionary column.
fn enum_schema() -> DynamicSchemaConfig {
    schema(
        &[
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new_timestamp(TimeUnit::Nanosecond, false),
            SignalSchemaField::new_dictionary(DictionarySlot::Schema(0), IndexWidth::U8, true),
        ],
        b"id\0type\0timestamp\0value.status\0",
        &[values(&["open", "closed"])],
        Some(STATUS_ENVELOPE),
    )
}

#[test]
fn an_enum_member_reads_back_as_a_dictionary_of_its_declared_values() {
    let mut ep = processor(enum_schema());
    let (code, output) = create(
        &mut ep,
        br#"[{"id":"a","type":"order","timestamp":1,"value":{"status":"closed"}},{"id":"b","type":"order","timestamp":2,"value":{"status":"open"}}]"#,
        InputFormat::Json,
    );
    assert_eq!(code, ResultCode::Ok);
    let batch = &batches(stream(&output))[0];
    assert_eq!(
        batch.schema().field(3).data_type(),
        &DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8))
    );
    let dictionary = batch.column(3).as_any_dictionary();
    assert_eq!(
        dictionary
            .values()
            .as_string::<i32>()
            .iter()
            .collect::<Vec<_>>(),
        [Some("open"), Some("closed")],
        "the dictionary holds the declared values in order"
    );
    assert_eq!(
        texts(batch.column(3)),
        [Some("closed".to_owned()), Some("open".to_owned())]
    );

    let (code, _) = create(
        &mut ep,
        br#"[{"id":"c","type":"order","timestamp":3,"value":{"status":"pending"}}]"#,
        InputFormat::Json,
    );
    assert_eq!(code, ResultCode::ParseError, "an undeclared variant");
    assert_eq!(
        ep.payload_validation_diagnostic()
            .map(|diagnostic| diagnostic.path.as_str()),
        Some("value.status")
    );
}

#[test]
fn a_text_its_schema_dictionary_does_not_hold_is_refused_naming_the_column() {
    let mut ep = processor(schema(
        &[
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new_dictionary(DictionarySlot::Schema(0), IndexWidth::U8, true),
        ],
        b"id\0status\0",
        &[values(&["open", "closed"])],
        None,
    ));
    let (code, output) = create(
        &mut ep,
        br#"[{"id":"a","status":"open"},{"id":"b","status":"pending"}]"#,
        InputFormat::Json,
    );
    assert_eq!(code, ResultCode::ParseError);
    assert_eq!(output[22], diagnostic_detail::DICTIONARY_VALUE);
    assert_eq!(
        u16::from_le_bytes([output[26], output[27]]),
        1,
        "the column"
    );
    assert_eq!(u16::from_le_bytes([output[28], output[29]]), 1, "the row");
}
//#endregion smoo/columine!n/vocab-enum-dictionary

//#region smoo/columine!n/vocab-stream-dictionary
/// `tag` is a stream-dictionary column.
fn tag_schema() -> DynamicSchemaConfig {
    schema(
        &[
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new_dictionary(DictionarySlot::Stream, IndexWidth::U16, true),
        ],
        b"id\0tag\0",
        &[],
        None,
    )
}

fn tag_events(tags: &[&str]) -> Vec<u8> {
    let events: Vec<String> = tags
        .iter()
        .enumerate()
        .map(|(row, tag)| format!(r#"{{"id":"{row}","tag":"{tag}"}}"#))
        .collect();
    format!("[{}]", events.join(",")).into_bytes()
}

/// Parse `tags` against `image` and return the batch's mark and output.
fn tag_batch(ep: &mut EventProcessor, image: &mut ListImage, tags: &[&str]) -> (u32, Vec<u8>) {
    let input = tag_events(tags);
    let mut output = vec![0; OUTPUT];
    let batch = ep.parse_with(&input, InputFormat::Json, image).unwrap();
    let mark = batch.category_mark();
    assert_eq!(batch.encode(&mut output, &[]), ResultCode::Ok);
    (mark, output)
}

#[test]
fn three_batches_through_one_image_define_extend_and_reuse_the_stream_dictionary() {
    let mut ep = processor(tag_schema());
    let preamble = ep.schema_config().schema_bytes.len();
    let mut image = ListImage::new(16, 64);

    // The first stream defines the dictionary and reads on its own.
    let (mark, first) = tag_batch(&mut ep, &mut image, &["red", "blue", "red"]);
    assert_eq!(mark, 0);
    let first = stream(&first).to_vec();
    assert_eq!(
        messages(&first, preamble),
        [
            (MessageHeader::DictionaryBatch, false, 2),
            (MessageHeader::RecordBatch, false, 0)
        ]
    );
    assert_eq!(
        texts(batches(&first)[0].column(1)),
        [
            Some("red".to_owned()),
            Some("blue".to_owned()),
            Some("red".to_owned())
        ]
    );

    // The second carries only the value it added, as a delta.
    let (mark, second) = tag_batch(&mut ep, &mut image, &["blue", "green"]);
    assert_eq!(mark, 2);
    let second = stream(&second).to_vec();
    assert_eq!(
        messages(&second, preamble),
        [
            (MessageHeader::DictionaryBatch, true, 1),
            (MessageHeader::RecordBatch, false, 0)
        ]
    );

    // The third added nothing and carries no dictionary message.
    let (mark, third) = tag_batch(&mut ep, &mut image, &["green", "red"]);
    assert_eq!(mark, 3);
    let third = stream(&third).to_vec();
    assert_eq!(
        messages(&third, preamble),
        [(MessageHeader::RecordBatch, false, 0)]
    );

    // One reader over the three streams, each after the first without its
    // preamble, reads every batch's texts.
    let mut joined = first[..first.len() - 8].to_vec();
    joined.extend(&second[preamble..second.len() - 8]);
    joined.extend(&third[preamble..]);
    let read: Vec<Vec<Option<String>>> = batches(&joined)
        .iter()
        .map(|batch| texts(batch.column(1)))
        .collect();
    let owned = |tags: &[&str]| -> Vec<Option<String>> {
        tags.iter().map(|tag| Some((*tag).to_owned())).collect()
    };
    assert_eq!(
        read,
        [
            owned(&["red", "blue", "red"]),
            owned(&["blue", "green"]),
            owned(&["green", "red"])
        ]
    );
    assert_eq!(image.len(), 3);
}

#[test]
fn a_stream_value_without_an_image_is_refused() {
    let mut ep = processor(tag_schema());
    let (code, output) = create(&mut ep, &tag_events(&["red"]), InputFormat::Json);
    assert_eq!(code, ResultCode::ParseError);
    assert_eq!(output[22], diagnostic_detail::DICTIONARY_VALUE);
}
//#endregion smoo/columine!n/vocab-stream-dictionary

//#region smoo/columine!n/vocab-image-limit
#[test]
fn a_value_past_the_images_limit_refuses_the_batch_and_truncating_to_its_mark_undoes_it() {
    let mut ep = processor(tag_schema());
    let mut image = ListImage::new(3, 64);
    tag_batch(&mut ep, &mut image, &["red", "blue"]);
    let before = image.clone();
    let mark = image.len();

    let mut output = vec![0; OUTPUT];
    let code = ep.create_log_entry_with(
        &tag_events(&["green", "violet"]),
        InputFormat::Json,
        &mut output,
        &[],
        &mut image,
    );
    assert_eq!(code, ResultCode::ParseError);
    assert_eq!(output[22], diagnostic_detail::DICTIONARY_VALUE);
    image.truncate(mark);
    assert_eq!(image, before);
}
//#endregion smoo/columine!n/vocab-image-limit

//#region smoo/columine!n/vocab-compact-index
/// A one-row CPB1 batch of one encoded column holding `index`.
fn one_index_batch(index: u8) -> Vec<u8> {
    let table_end = COMPACT_HEADER_SIZE + COMPACT_DESCRIPTOR_SIZE;
    let mut batch = vec![0; table_end + 8];
    batch[0..4].copy_from_slice(&COMPACT_BATCH_MAGIC.to_le_bytes());
    batch[4..6].copy_from_slice(&COMPACT_ABI_VERSION.to_le_bytes());
    batch[6..8].copy_from_slice(&(COMPACT_DESCRIPTOR_SIZE as u16).to_le_bytes());
    batch[8..12].copy_from_slice(&1u32.to_le_bytes());
    batch[12..16].copy_from_slice(&1u32.to_le_bytes());
    let descriptor = COMPACT_HEADER_SIZE;
    // The column carries its storage plane: an encoded column's index plane.
    batch[descriptor] = ArrowType::UInt8 as u8;
    batch[descriptor + 20..descriptor + 24].copy_from_slice(&(table_end as u32).to_le_bytes());
    batch[descriptor + 24..descriptor + 28].copy_from_slice(&1u32.to_le_bytes());
    batch[table_end] = index;
    batch
}

fn assert_bad_index(output: &[u8]) {
    assert_eq!(
        read_result_header(output).0,
        ResultCode::InvalidInput as u32
    );
    assert_eq!(output[21], COMPACT_DIAGNOSTIC_STAGE);
    assert_eq!(output[22], compact_detail::BAD_DICTIONARY_INDEX);
    assert_eq!(u16::from_le_bytes([output[26], output[27]]), 0, "the field");
    assert_eq!(u16::from_le_bytes([output[28], output[29]]), 0, "the row");
}

#[test]
fn a_compact_index_past_its_schema_dictionary_is_refused() {
    let mut ep = processor(schema(
        &[SignalSchemaField::new_dictionary(
            DictionarySlot::Schema(0),
            IndexWidth::U8,
            false,
        )],
        b"status\0",
        &[values(&["open", "closed"])],
        None,
    ));
    let mut output = vec![0; OUTPUT];
    assert_eq!(
        ep.compact(&one_index_batch(2), &mut output, &[]),
        ResultCode::InvalidInput
    );
    assert_bad_index(&output);

    assert_eq!(
        ep.compact(&one_index_batch(1), &mut output, &[]),
        ResultCode::Ok
    );
    assert_eq!(
        texts(batches(stream(&output))[0].column(0)),
        [Some("closed".to_owned())]
    );
}

#[test]
fn a_compact_index_past_the_image_is_refused_and_a_held_one_brings_its_values() {
    let mut ep = processor(schema(
        &[SignalSchemaField::new_dictionary(
            DictionarySlot::Stream,
            IndexWidth::U8,
            false,
        )],
        b"tag\0",
        &[],
        None,
    ));
    let mut image = ListImage::new(16, 64);
    image.ordinal("red").unwrap();
    let mut output = vec![0; OUTPUT];
    assert_eq!(
        ep.compact_with(&one_index_batch(1), &mut output, &[], &image, 0),
        ResultCode::InvalidInput
    );
    assert_bad_index(&output);

    assert_eq!(
        ep.compact_with(&one_index_batch(0), &mut output, &[], &image, 2),
        ResultCode::InvalidInput,
        "a mark past the image"
    );

    assert_eq!(
        ep.compact_with(&one_index_batch(0), &mut output, &[], &image, 0),
        ResultCode::Ok
    );
    assert_eq!(
        texts(batches(stream(&output))[0].column(0)),
        [Some("red".to_owned())]
    );
    assert_eq!(
        ep.compact_with(&one_index_batch(0), &mut output, &[], &NoCategories, 0),
        ResultCode::InvalidInput,
        "an empty image holds no ordinal"
    );
}
//#endregion smoo/columine!n/vocab-compact-index

//#region smoo/columine!n/vocab-field-identity
/// Two Int64 columns whose fields carry the `sb.fields` values `identities`.
fn identity_processor(identities: [Option<&str>; 2]) -> Result<EventProcessor, EpInitError> {
    let fields = [
        SignalSchemaField::new(ArrowType::Int64, true),
        SignalSchemaField::new(ArrowType::Int64, true),
    ];
    let logical = ["x", "y"]
        .iter()
        .zip(identities)
        .map(|(name, identity)| {
            let field = Field::new(*name, DataType::Int64, true);
            match identity {
                Some(value) => field.with_metadata(HashMap::from([(
                    FIELD_IDENTITY_KEY.to_owned(),
                    value.to_owned(),
                )])),
                None => field,
            }
        })
        .collect();
    let message = logical_schema_ipc_bytes(logical).unwrap();
    let config = DynamicSchemaConfig::with_field_names(&message, &fields, b"x\0y\0").unwrap();
    EventProcessor::new(options(), 8, config)
}

#[test]
fn a_column_with_two_type_digests_is_refused_at_creation() {
    let refused = identity_processor([
        Some(r#"[{"lib":"orders","n":1,"name":"a","type":"d1"},{"lib":"orders","n":2,"name":"b","type":"d2"}]"#),
        None,
    ])
    .err();
    assert_eq!(
        refused,
        Some(EpInitError::FieldIdentity(IdentityError::TwoTypes {
            field_index: 0
        }))
    );
}

#[test]
fn one_identity_on_two_columns_is_refused_at_creation() {
    let identity = r#"[{"lib":"orders","n":1,"name":"a","type":"d1"}]"#;
    let refused = identity_processor([Some(identity), Some(identity)]).err();
    assert_eq!(
        refused,
        Some(EpInitError::FieldIdentity(IdentityError::Repeated {
            field_index: 1
        }))
    );
}

#[test]
fn an_accepted_identity_is_exposed_by_field() {
    let ep = identity_processor([
        Some(r#"[{"lib":"orders","n":1,"name":"a","type":"d1"},{"lib":"orders","n":2,"name":"b","type":"d1"}]"#),
        None,
    ])
    .expect("one digest per column, one column per identity");
    let identity = |number, name: &str| FieldIdentity {
        library: "orders".to_owned(),
        number,
        name: name.to_owned(),
        type_digest: "d1".to_owned(),
    };
    assert_eq!(
        ep.field_identities(),
        [vec![identity(1, "a"), identity(2, "b")], Vec::new()]
    );
}
//#endregion smoo/columine!n/vocab-field-identity

//#region smoo/columine!n/vocab-lanes-agree
#[test]
fn json_and_messagepack_produce_one_stream_for_one_logical_batch() {
    let envelope = r#"{"order":{"kind":"object","fields":{"took":{"kind":"number"},"status":{"kind":"enum","variants":["open","closed"]},"tag":{"kind":"string"}}}}"#;
    let config = schema(
        &[
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new_timestamp(TimeUnit::Nanosecond, false),
            SignalSchemaField::new_duration(TimeUnit::Millisecond, true),
            SignalSchemaField::new_dictionary(DictionarySlot::Schema(0), IndexWidth::U8, true),
            SignalSchemaField::new_dictionary(DictionarySlot::Stream, IndexWidth::U16, true),
        ],
        b"id\0type\0timestamp\0value.took\0value.status\0value.tag\0",
        &[values(&["open", "closed"])],
        Some(envelope),
    );
    let json = br#"[{"id":"a","type":"order","timestamp":"2024-01-15T10:30:00.123456789Z","value":{"took":1500,"status":"closed","tag":"red"}},{"id":"b","type":"order","timestamp":1705314600000000000,"value":{"took":20,"status":"open","tag":"blue"}},{"id":"c","type":"order","timestamp":1705314601000000000,"value":{"took":3,"status":"closed","tag":"red"}}]"#;
    let event = |id, timestamp, took, status, tag| {
        Mp::Map(vec![
            ("id", Mp::Str(id)),
            ("type", Mp::Str("order")),
            ("timestamp", timestamp),
            (
                "value",
                Mp::Map(vec![
                    ("took", Mp::Int(took)),
                    ("status", Mp::Str(status)),
                    ("tag", Mp::Str(tag)),
                ]),
            ),
        ])
    };
    let msgpack = msgpack_batch(&[
        event(
            "a",
            Mp::Str("2024-01-15T10:30:00.123456789Z"),
            1500,
            "closed",
            "red",
        ),
        event("b", Mp::Int(1_705_314_600_000_000_000), 20, "open", "blue"),
        event("c", Mp::Int(1_705_314_601_000_000_000), 3, "closed", "red"),
    ]);

    let encode = |input: &[u8], format: InputFormat| {
        let mut ep = processor(config.clone());
        let mut image = ListImage::new(16, 64);
        let mut output = vec![0; OUTPUT];
        assert_eq!(
            ep.create_log_entry_with(input, format, &mut output, &[], &mut image),
            ResultCode::Ok,
            "{format:?}"
        );
        stream(&output).to_vec()
    };
    let from_json = encode(json, InputFormat::Json);
    assert_eq!(
        encode(&msgpack, InputFormat::Msgpack),
        from_json,
        "the lanes write the same bytes"
    );

    let batch = &batches(&from_json)[0];
    assert_eq!(
        batch
            .column(2)
            .as_primitive::<TimestampNanosecondType>()
            .values()
            .to_vec(),
        [
            1_705_314_600_123_456_789,
            1_705_314_600_000_000_000,
            1_705_314_601_000_000_000
        ]
    );
    assert_eq!(
        batch
            .column(3)
            .as_primitive::<DurationMillisecondType>()
            .values()
            .to_vec(),
        [1500, 20, 3]
    );
    let owned = |texts: &[&str]| -> Vec<Option<String>> {
        texts.iter().map(|text| Some((*text).to_owned())).collect()
    };
    assert_eq!(texts(batch.column(4)), owned(&["closed", "open", "closed"]));
    assert_eq!(texts(batch.column(5)), owned(&["red", "blue", "red"]));
}
//#endregion smoo/columine!n/vocab-lanes-agree
