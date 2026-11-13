//! The value cell seam through `EventProcessor`
//! (specs/columine/10-physical-vocabulary.md §7, §8): each test creates a
//! processor over a schema, parses a batch on both lanes and reads the cells
//! back. The embedder is a `ValueCells` that writes a recognisable text: the
//! member, record row or carrier path and the value's kinds.

use arrow_schema::{DataType, Field};
use columine_arrow::schema::PayloadDiscriminator;
use columine_arrow::{
    ArrowType, CELL_ENCODING_KEY, DynamicSchemaConfig, RecordMember, SignalSchemaField, TextColumn,
    logical_schema_ipc_bytes,
};
use columine_event_processor::{
    CellRefusal, CellTarget, EpInitError, EventProcessor, InputFormat, NoValueCells, ParseOptions,
    RecordRow, ResultCode, TEXT_HASH_BYTES, TextPlace, ValueCells, diagnostic_detail,
};
use columine_parsing::validate::{
    NoAddressGrammar, NumberValue, UnavailableVoAdmission, ValueKind, ValueView,
};
use columine_parsing::{ColumnValue, ConfigError, read_cell};
use std::collections::HashMap;

//#region smoo/columine!n/vocab-value-cells
const ENCODING: &str = "test-cells";

/// The one content hash the test embedder holds.
const KNOWN_HASH: [u8; TEXT_HASH_BYTES] = [0xab; TEXT_HASH_BYTES];

/// A value as text: strings quoted, numbers as their integer or float,
/// objects and arrays by their members, every other value by its kind.
fn render(value: &dyn ValueView) -> String {
    match value.kind() {
        ValueKind::Object => {
            let mut members = Vec::new();
            value.visit_object(&mut |name, member| {
                members.push(format!("{name}={}", render(member)));
            });
            format!("{{{}}}", members.join(","))
        }
        ValueKind::Array => {
            let mut items = Vec::new();
            value.visit_array(&mut |_, item| items.push(render(item)));
            format!("[{}]", items.join(","))
        }
        ValueKind::String => format!("{:?}", value.as_str().unwrap_or_default()),
        ValueKind::Number => match value.as_number() {
            Some(NumberValue::Signed(number)) => number.to_string(),
            Some(NumberValue::Unsigned(number)) => number.to_string(),
            Some(NumberValue::Float(number)) => number.to_string(),
            None => "number".to_owned(),
        },
        kind => kind.as_str().to_owned(),
    }
}

/// The test embedder: each cell is the text of what it was handed.
#[derive(Default)]
struct TextCells {
    record: Vec<String>,
    carrier: Vec<String>,
}

fn finish(parts: &[String], out: &mut Vec<u8>) -> Result<bool, CellRefusal> {
    if parts.is_empty() {
        return Ok(false);
    }
    out.extend(parts.join(";").bytes());
    Ok(true)
}

impl ValueCells for TextCells {
    fn encoding(&self) -> &'static str {
        ENCODING
    }

    fn prepare(&mut self, _target: CellTarget<'_>) -> Result<(), CellRefusal> {
        Ok(())
    }

    fn member(
        &mut self,
        column: u32,
        value: &dyn ValueView,
        out: &mut Vec<u8>,
    ) -> Result<(), CellRefusal> {
        out.extend(format!("member {column} {}", render(value)).bytes());
        Ok(())
    }

    fn begin_record(&mut self) {
        self.record.clear();
    }

    fn put(&mut self, row: &RecordRow<'_>, value: &dyn ValueView) -> Result<(), CellRefusal> {
        self.record.push(format!(
            "{:?}@{}={}",
            row.path,
            row.descriptor_position,
            render(value)
        ));
        Ok(())
    }

    fn finish_record(&mut self, out: &mut Vec<u8>) -> Result<bool, CellRefusal> {
        finish(&self.record, out)
    }

    fn begin_undeclared(&mut self) {
        self.carrier.clear();
    }

    fn put_undeclared(&mut self, path: &str, value: &dyn ValueView) -> Result<(), CellRefusal> {
        self.carrier.push(format!("{path}={}", render(value)));
        Ok(())
    }

    fn finish_undeclared(&mut self, out: &mut Vec<u8>) -> Result<bool, CellRefusal> {
        finish(&self.carrier, out)
    }

    /// The hash is the text's length in every byte; `keep` is kept inline.
    fn text(
        &mut self,
        _hash_column: u32,
        value: &str,
        hash: &mut [u8; TEXT_HASH_BYTES],
    ) -> Result<TextPlace, CellRefusal> {
        *hash = [u8::try_from(value.len()).unwrap(); TEXT_HASH_BYTES];
        Ok(if value == "keep" {
            TextPlace::Inline
        } else {
            TextPlace::HashOnly
        })
    }

    fn text_by_hash(
        &mut self,
        _hash_column: u32,
        hash: &[u8; TEXT_HASH_BYTES],
    ) -> Result<(), CellRefusal> {
        if *hash == KNOWN_HASH {
            Ok(())
        } else {
            Err(CellRefusal::Refused {
                code: "unknown-text",
            })
        }
    }
}

type Embedded = EventProcessor<UnavailableVoAdmission, NoAddressGrammar, TextCells>;

fn options() -> ParseOptions {
    ParseOptions {
        base_path: false,
        msgpack_growth: true,
        diagnostics: true,
    }
}

/// One column: its name, its physical field, its logical type and the
/// `sb.cell` its field metadata names.
struct Column {
    name: &'static str,
    field: SignalSchemaField,
    logical: DataType,
    cell: Option<&'static str>,
}

fn column(name: &'static str, arrow_type: ArrowType, logical: DataType) -> Column {
    Column {
        name,
        field: SignalSchemaField::new(arrow_type, true),
        logical,
        cell: None,
    }
}

fn utf8(name: &'static str) -> Column {
    column(name, ArrowType::Utf8, DataType::Utf8)
}

fn binary(name: &'static str, cell: Option<&'static str>) -> Column {
    Column {
        cell,
        ..column(name, ArrowType::Binary, DataType::Binary)
    }
}

/// A schema of `columns` whose payload `envelope` is discriminated by the
/// event's `type`.
fn schema(columns: &[Column], envelope: &str) -> DynamicSchemaConfig {
    let fields: Vec<SignalSchemaField> = columns.iter().map(|column| column.field).collect();
    let logical = columns
        .iter()
        .map(|column| {
            let field = Field::new(column.name, column.logical.clone(), true);
            match column.cell {
                Some(cell) => field.with_metadata(HashMap::from([(
                    CELL_ENCODING_KEY.to_owned(),
                    cell.to_owned(),
                )])),
                None => field,
            }
        })
        .collect();
    let message = logical_schema_ipc_bytes(logical).unwrap();
    let names: Vec<u8> = columns
        .iter()
        .flat_map(|column| column.name.bytes().chain([0]))
        .collect();
    DynamicSchemaConfig {
        semantic_schema: Some(envelope.as_bytes().to_vec()),
        payload_discriminator: Some(PayloadDiscriminator {
            key: "type".into(),
            members: Vec::new(),
        }),
        ..DynamicSchemaConfig::with_field_names(&message, &fields, &names).unwrap()
    }
}

fn embedded(config: DynamicSchemaConfig) -> Result<Embedded, EpInitError> {
    Embedded::new_with_cells(
        options(),
        8,
        config,
        UnavailableVoAdmission,
        TextCells::default(),
    )
}

fn native(config: DynamicSchemaConfig) -> Result<EventProcessor, EpInitError> {
    EventProcessor::new(options(), 8, config)
}

/// A minimal MessagePack writer for the test events.
enum Mp<'a> {
    Str(&'a str),
    Int(i64),
    Bin(&'a [u8]),
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
        Mp::Bin(bytes) => {
            out.push(0xc4);
            out.push(u8::try_from(bytes.len()).unwrap());
            out.extend(*bytes);
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

/// A MessagePack array of `{"type":"order","value":<payload>}` events.
fn orders(payloads: Vec<Mp<'_>>) -> Vec<u8> {
    let mut out = vec![0x90 | u8::try_from(payloads.len()).unwrap()];
    for payload in payloads {
        msgpack(
            &mut out,
            &Mp::Map(vec![("type", Mp::Str("order")), ("value", payload)]),
        );
    }
    out
}

/// The cells of `columns`, row by row, of a batch the processor accepts.
fn cells<C: ValueCells>(
    ep: &mut EventProcessor<UnavailableVoAdmission, NoAddressGrammar, C>,
    input: &[u8],
    format: InputFormat,
    columns: &[usize],
) -> Vec<Vec<Option<ColumnValue<'static>>>> {
    let mut batch = ep
        .parse(input, format)
        .unwrap_or_else(|error| panic!("{format:?}: the batch is accepted: {error:?}"));
    let rows = batch.row_count();
    let stored = batch.columns_mut();
    (0..rows)
        .map(|row| {
            columns
                .iter()
                .map(|column| read_cell(stored, *column, row as usize))
                .collect()
        })
        .collect()
}

fn bytes(text: &str) -> Option<ColumnValue<'static>> {
    Some(ColumnValue::Binary(text.as_bytes().to_vec().into()))
}

fn text(text: &str) -> Option<ColumnValue<'static>> {
    Some(ColumnValue::Utf8(text.to_owned().into()))
}

fn hash(hash: [u8; TEXT_HASH_BYTES]) -> Option<ColumnValue<'static>> {
    Some(ColumnValue::FixedBytes(hash.to_vec().into()))
}

const RECORD_ENVELOPE: &str = r#"{"order":{"kind":"object","fields":{"name":{"kind":"string"},"qty":{"kind":"optional","value":{"kind":"number"}}}}}"#;

/// `type`, `value.name`, the declared record and the carrier, with `qty`
/// routed to the record at field path `[3]`, descriptor 7.
fn record_schema(cell: Option<&'static str>, envelope: &str) -> DynamicSchemaConfig {
    DynamicSchemaConfig {
        record_members: vec![RecordMember {
            discriminant: "order".into(),
            member: "qty".into(),
            path: vec![3],
            descriptor_position: 7,
        }],
        ..schema(
            &[
                utf8("type"),
                utf8("value.name"),
                binary("value.$declared", cell),
                binary("value.$undeclared", cell),
            ],
            envelope,
        )
    }
}

/// Both lanes' spelling of a batch of `{name, qty?}` orders.
fn record_batches() -> [(InputFormat, Vec<u8>); 2] {
    [
        (
            InputFormat::Json,
            br#"[{"type":"order","value":{"name":"a","qty":5}},{"type":"order","value":{"name":"b"}}]"#
                .to_vec(),
        ),
        (
            InputFormat::Msgpack,
            orders(vec![
                Mp::Map(vec![("name", Mp::Str("a")), ("qty", Mp::Int(5))]),
                Mp::Map(vec![("name", Mp::Str("b"))]),
            ]),
        ),
    ]
}

#[test]
fn a_declared_member_without_a_column_is_written_to_the_record_and_nowhere_else() {
    for (format, input) in record_batches() {
        let mut ep = embedded(record_schema(Some(ENCODING), RECORD_ENVELOPE)).unwrap();
        let row = &cells(&mut ep, &input, format, &[1, 2, 3])[0];
        assert_eq!(
            row,
            &[text("a"), bytes("[3]@7=5"), None],
            "{format:?}: the record holds qty and the carrier nothing"
        );
    }
}

#[test]
fn a_row_with_no_record_member_stores_a_null_record_cell() {
    for (format, input) in record_batches() {
        let mut ep = embedded(record_schema(Some(ENCODING), RECORD_ENVELOPE)).unwrap();
        assert_eq!(
            cells(&mut ep, &input, format, &[1, 2])[1],
            [text("b"), None],
            "{format:?}"
        );
    }
}

#[test]
fn creation_without_an_embedder_refuses_a_record_row_and_a_text_column() {
    assert_eq!(
        native(record_schema(None, RECORD_ENVELOPE)).err(),
        Some(EpInitError::ValueCells(CellRefusal::Unsupported))
    );
    assert_eq!(
        native(text_schema()).err(),
        Some(EpInitError::ValueCells(CellRefusal::Unsupported))
    );
}

#[test]
fn a_declared_member_with_neither_a_column_nor_a_routing_row_refuses_creation() {
    let envelope = r#"{"order":{"kind":"object","fields":{"name":{"kind":"string"},"qty":{"kind":"number"},"lost":{"kind":"string"}}}}"#;
    let unplaced = Some(EpInitError::Config(ConfigError::UnplacedMember));
    assert_eq!(
        embedded(record_schema(Some(ENCODING), envelope)).err(),
        unplaced
    );
    // With no embedder too: a declared member never falls to the carrier.
    let native_schema = schema(
        &[
            utf8("type"),
            utf8("value.name"),
            binary("value.$undeclared", None),
        ],
        r#"{"order":{"kind":"object","fields":{"name":{"kind":"string"},"lost":{"kind":"string"}}}}"#,
    );
    assert_eq!(native(native_schema).err(), unplaced);
}

#[test]
fn an_embedder_written_member_and_carrier_hold_the_embedders_bytes() {
    let config = || {
        schema(
            &[
                utf8("type"),
                binary("value.meta", Some(ENCODING)),
                binary("value.$undeclared", Some(ENCODING)),
            ],
            r#"{"order":{"kind":"object","open":true,"fields":{"meta":{"kind":"object","open":true,"fields":{"note":{"kind":"string"}}}}}}"#,
        )
    };
    let batches = [
        (
            InputFormat::Json,
            br#"[{"type":"order","value":{"meta":{"note":"n","extra.key":"x"},"loose":1}}]"#
                .to_vec(),
        ),
        (
            InputFormat::Msgpack,
            orders(vec![Mp::Map(vec![
                (
                    "meta",
                    Mp::Map(vec![("note", Mp::Str("n")), ("extra.key", Mp::Str("x"))]),
                ),
                ("loose", Mp::Int(1)),
            ])]),
        ),
    ];
    for (format, input) in batches {
        let mut ep = embedded(config()).unwrap();
        assert_eq!(
            cells(&mut ep, &input, format, &[1, 2])[0],
            [
                bytes(r#"member 1 {note="n",extra.key="x"}"#),
                // The undeclared key below the open member keeps its payload
                // path, `.` in a key spelled `%2E`.
                bytes(r#"meta.extra%2Ekey="x";loose=1"#),
            ],
            "{format:?}"
        );
    }
}

/// `type`, a text's hash column `value.note` and its inline column.
fn text_schema() -> DynamicSchemaConfig {
    DynamicSchemaConfig {
        text_columns: vec![TextColumn {
            hash: 1,
            inline: Some(2),
        }],
        ..schema(
            &[
                utf8("type"),
                Column {
                    name: "value.note",
                    field: SignalSchemaField::new_fixed_size_binary(32, true),
                    logical: DataType::FixedSizeBinary(32),
                    cell: None,
                },
                utf8("note_inline"),
            ],
            r#"{"order":{"kind":"object","fields":{"note":{"kind":"union","variants":[{"kind":"string"},{"kind":"binary"}]}}}}"#,
        )
    }
}

#[test]
fn a_text_stores_the_embedders_hash_and_its_bytes_only_when_kept_inline() {
    let batches = [
        (
            InputFormat::Json,
            br#"[{"type":"order","value":{"note":"keep"}},{"type":"order","value":{"note":"drop"}}]"#
                .to_vec(),
        ),
        (
            InputFormat::Msgpack,
            orders(vec![
                Mp::Map(vec![("note", Mp::Str("keep"))]),
                Mp::Map(vec![("note", Mp::Str("drop"))]),
            ]),
        ),
    ];
    for (format, input) in batches {
        let mut ep = embedded(text_schema()).unwrap();
        assert_eq!(
            cells(&mut ep, &input, format, &[1, 2]),
            [
                [hash([4; TEXT_HASH_BYTES]), text("keep")],
                [hash([4; TEXT_HASH_BYTES]), None],
            ],
            "{format:?}"
        );
    }
}

#[test]
fn a_text_that_arrives_as_its_hash_is_stored_when_the_embedder_holds_it() {
    let mut ep = embedded(text_schema()).unwrap();
    let known = orders(vec![Mp::Map(vec![("note", Mp::Bin(&KNOWN_HASH))])]);
    assert_eq!(
        cells(&mut ep, &known, InputFormat::Msgpack, &[1, 2]),
        [[hash(KNOWN_HASH), None]]
    );
    let unknown = orders(vec![Mp::Map(vec![(
        "note",
        Mp::Bin(&[1; TEXT_HASH_BYTES]),
    )])]);
    let refused = ep.parse(&unknown, InputFormat::Msgpack).err().unwrap();
    assert_eq!(
        (
            refused.code,
            refused.diagnostic.map(|diagnostic| diagnostic.detail)
        ),
        (
            ResultCode::ParseError,
            Some(diagnostic_detail::CELL_REFUSED)
        )
    );
}

/// `type` and `value.blob`, a Binary field whose `sb.cell` is `cell`.
fn blob_schema(cell: Option<&'static str>) -> DynamicSchemaConfig {
    schema(
        &[utf8("type"), binary("value.blob", cell)],
        r#"{"order":{"kind":"object","fields":{"blob":{"kind":"unknown"}}}}"#,
    )
}

#[test]
fn a_binary_field_whose_cell_names_another_encoding_refuses_creation() {
    let mismatch = Some(EpInitError::Config(ConfigError::CellEncodingMismatch));
    assert_eq!(embedded(blob_schema(Some("other"))).err(), mismatch);
    assert_eq!(native(blob_schema(Some("other"))).err(), mismatch);
    // Columine's own MessagePack is the native path's encoding, not the
    // embedder's.
    assert_eq!(
        embedded(blob_schema(Some(NoValueCells.encoding()))).err(),
        mismatch
    );
}

#[test]
fn a_raw_binary_field_takes_bytes_and_refuses_a_structured_value_beside_an_embedder() {
    let mut ep = embedded(blob_schema(None)).unwrap();
    let raw = orders(vec![Mp::Map(vec![("blob", Mp::Bin(&[1, 2, 3]))])]);
    assert_eq!(
        cells(&mut ep, &raw, InputFormat::Msgpack, &[1]),
        [[Some(ColumnValue::Binary(vec![1, 2, 3].into()))]]
    );
    let structured = orders(vec![Mp::Map(vec![(
        "blob",
        Mp::Map(vec![("a", Mp::Int(1))]),
    )])]);
    for (format, input) in [
        (
            InputFormat::Json,
            br#"[{"type":"order","value":{"blob":{"a":1}}}]"#.to_vec(),
        ),
        (InputFormat::Msgpack, structured),
    ] {
        assert_eq!(
            ep.parse(&input, format).err().map(|error| error.code),
            Some(ResultCode::ParseError),
            "{format:?}"
        );
    }
}
//#endregion smoo/columine!n/vocab-value-cells
