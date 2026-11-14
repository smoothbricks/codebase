//! A declared member without a column reaches the row's declared record
//! (specs/columine/10-physical-vocabulary.md §7 "Placement is total"): the
//! routing row `(type, member)` places it, whether or not the processor holds
//! a semantic schema, in both payload spellings (a nested `value` object and
//! flat `value.<member>` keys) and whichever side of the payload `type` is
//! spelled on, on both lanes.

use columine_arrow::RecordMember;
use columine_parsing::json_extractor::{ExtractionDiagnostic, extract_json_events};
use columine_parsing::msgpack_extractor::extract_msgpack_events_with_projections;
use columine_parsing::validate::{NumberValue, ValueKind, ValueView, parse_schema_envelope};
use columine_parsing::{
    ArrowType, CellLayout, CellWriter, ColumnValue, DynamicColumns, FrameProjections,
    PayloadDiscriminator, SignalSchemaField, build_extraction_config_with_semantic_schemas,
    read_cell,
};
use columine_types::category::NoCategories;
use columine_types::cells::{
    CellRefusal, CellTarget, RecordRow, TEXT_HASH_BYTES, TextPlace, ValueCells,
};

//#region smoo/columine!n/vocab-value-cells
const ENCODING: &str = "test-cells";

/// A value as text: strings quoted, numbers as written, any other value by
/// its kind.
fn render(value: &dyn ValueView) -> String {
    match value.kind() {
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

/// The test embedder: the record and the carrier are the text of what each
/// was handed.
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

    fn text(
        &mut self,
        _hash_column: u32,
        _value: &str,
        _hash: &mut [u8; TEXT_HASH_BYTES],
    ) -> Result<TextPlace, CellRefusal> {
        Err(CellRefusal::Unsupported)
    }

    fn text_by_hash(
        &mut self,
        _hash_column: u32,
        _hash: &[u8; TEXT_HASH_BYTES],
    ) -> Result<(), CellRefusal> {
        Err(CellRefusal::Unsupported)
    }
}

const NAMES: [&str; 4] = ["type", "value.name", "value.$declared", "value.$undeclared"];

/// `type`, `value.name`, the declared record and the carrier, the last two
/// written by the embedder, with `order.qty` routed to the record at field
/// path `[3]`, descriptor 7; `semantic` adds the schema of `order`.
fn record_config(semantic: bool) -> (Vec<SignalSchemaField>, columine_parsing::ExtractionConfig) {
    let fields = vec![
        SignalSchemaField::new(ArrowType::Utf8, true),
        SignalSchemaField::new(ArrowType::Utf8, true),
        SignalSchemaField::new(ArrowType::Binary, true),
        SignalSchemaField::new(ArrowType::Binary, true),
    ];
    let schemas = parse_schema_envelope(
        br#"{"order":{"kind":"object","fields":{"name":{"kind":"string"},"qty":{"kind":"optional","value":{"kind":"number"}}}}}"#,
    )
    .unwrap();
    let cell = Some(ENCODING.to_owned());
    let config = build_extraction_config_with_semantic_schemas(
        &fields,
        &NAMES,
        &[],
        semantic.then_some(&schemas),
        Some(&PayloadDiscriminator {
            key: "type".into(),
            members: Vec::new(),
        }),
        CellLayout {
            cell_metadata: &[None, None, cell.clone(), cell],
            record_members: &[RecordMember {
                discriminant: "order".into(),
                member: "qty".into(),
                path: vec![3],
                descriptor_position: 7,
            }],
            encoding: Some(ENCODING),
            ..CellLayout::default()
        },
    )
    .unwrap();
    (fields, config)
}

/// One `order` event in each spelling: nested and flat, `type` before and
/// after the payload, then a nested and a flat event without `qty`. `loose`
/// is a member nothing places.
const JSON_ORDERS: &[u8] = br#"[
    {"type":"order","value":{"name":"a","qty":5,"loose":1}},
    {"value":{"name":"b","qty":6,"loose":2},"type":"order"},
    {"type":"order","value.name":"c","value.qty":7,"value.loose":3},
    {"value.name":"d","value.qty":8,"value.loose":4,"type":"order"},
    {"type":"order","value":{"name":"e"}},
    {"type":"order","value.name":"f"}
]"#;

/// A MessagePack value of the test events.
enum Mp<'a> {
    Str(&'a str),
    Int(u8),
    Map(Vec<(&'a str, Mp<'a>)>),
}

fn msgpack(out: &mut Vec<u8>, value: &Mp<'_>) {
    match value {
        Mp::Str(text) => {
            out.push(0xa0 | u8::try_from(text.len()).unwrap());
            out.extend(text.as_bytes());
        }
        Mp::Int(value) => out.push(*value),
        Mp::Map(entries) => {
            out.push(0x80 | u8::try_from(entries.len()).unwrap());
            for (key, value) in entries {
                msgpack(out, &Mp::Str(key));
                msgpack(out, value);
            }
        }
    }
}

/// [`JSON_ORDERS`] as a MessagePack array.
fn msgpack_orders() -> Vec<u8> {
    use Mp::{Int, Map, Str};
    let events = [
        Map(vec![
            ("type", Str("order")),
            (
                "value",
                Map(vec![("name", Str("a")), ("qty", Int(5)), ("loose", Int(1))]),
            ),
        ]),
        Map(vec![
            (
                "value",
                Map(vec![("name", Str("b")), ("qty", Int(6)), ("loose", Int(2))]),
            ),
            ("type", Str("order")),
        ]),
        Map(vec![
            ("type", Str("order")),
            ("value.name", Str("c")),
            ("value.qty", Int(7)),
            ("value.loose", Int(3)),
        ]),
        Map(vec![
            ("value.name", Str("d")),
            ("value.qty", Int(8)),
            ("value.loose", Int(4)),
            ("type", Str("order")),
        ]),
        Map(vec![
            ("type", Str("order")),
            ("value", Map(vec![("name", Str("e"))])),
        ]),
        Map(vec![("type", Str("order")), ("value.name", Str("f"))]),
    ];
    let mut out = vec![0x90 | u8::try_from(events.len()).unwrap()];
    for event in &events {
        msgpack(&mut out, event);
    }
    out
}

/// Columns 1 to 3 of every row the lane stores, as text: `-` is the empty cell.
fn stored(lane: &str, semantic: bool) -> Vec<[String; 3]> {
    let (fields, config) = record_config(semantic);
    let mut columns = DynamicColumns::new(&fields, 8).expect("column plane fits");
    let mut embedder = TextCells::default();
    let mut work = [0; 512];
    let rows = match lane {
        "json" => extract_json_events(
            JSON_ORDERS,
            &config,
            &mut columns,
            &mut work,
            &mut ExtractionDiagnostic::default(),
            &mut NoCategories,
            &mut CellWriter::Embedder(&mut embedder),
            &mut Vec::new(),
        ),
        _ => extract_msgpack_events_with_projections(
            &msgpack_orders(),
            &config,
            &mut columns,
            &mut work,
            false,
            FrameProjections::default(),
            &mut NoCategories,
            &mut CellWriter::Embedder(&mut embedder),
            &mut Vec::new(),
        ),
    }
    .unwrap_or_else(|error| panic!("{lane}: the batch is accepted: {error:?}"));
    (0..rows)
        .map(|row| {
            [1, 2, 3].map(|column| match read_cell(&columns, column, row) {
                None => "-".to_owned(),
                Some(ColumnValue::Utf8(text)) => text.into_owned(),
                Some(ColumnValue::Binary(bytes)) => String::from_utf8_lossy(&bytes).into_owned(),
                Some(other) => format!("{other:?}"),
            })
        })
        .collect()
}

#[test]
fn a_routed_member_reaches_the_declared_record_in_every_spelling_with_or_without_a_semantic_schema()
{
    let lanes = [
        ("json", true),
        ("json", false),
        ("msgpack", true),
        ("msgpack", false),
    ];
    let row = |cells: [&str; 3]| cells.map(str::to_owned);
    let expected = [
        // Nested, `type` first and last.
        row(["a", "[3]@7=5", "loose=1"]),
        row(["b", "[3]@7=6", "loose=2"]),
        // Flat, `type` first and last: the unplaced member keeps its whole
        // key as its carrier path.
        row(["c", "[3]@7=7", "value%2Eloose=3"]),
        row(["d", "[3]@7=8", "value%2Eloose=4"]),
        // An absent member stays absent in either spelling.
        row(["e", "-", "-"]),
        row(["f", "-", "-"]),
    ];
    assert_eq!(
        lanes.map(|(lane, semantic)| (lane, semantic, stored(lane, semantic))),
        lanes.map(|(lane, semantic)| (lane, semantic, expected.to_vec())),
        "(lane, semantic schema, [value.name, value.$declared, value.$undeclared] per row)"
    );
}
//#endregion smoo/columine!n/vocab-value-cells
