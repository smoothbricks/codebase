//! A declared member without a column reaches the row's declared record
//! (specs/columine/10-physical-vocabulary.md §7 "Placement is total"): the
//! routing row `(type, member)` places it, whether or not the processor holds
//! a semantic schema, in both payload spellings (a nested `value` object and
//! flat `value.<member>` keys) and whichever side of the payload `type` is
//! spelled on, on both lanes.

use columine_arrow::{RecordMember, RecordSubtype};
use columine_parsing::json_extractor::{
    ExtractionDiagnostic, ExtractionError, extract_json_events,
};
use columine_parsing::msgpack_extractor::extract_msgpack_events_with_projections;
use columine_parsing::validate::{NumberValue, ValueKind, ValueView, parse_schema_envelope};
use columine_parsing::{
    ArrowType, CellLayout, CellWriter, ColumnValue, DiscriminatedMember, DynamicColumns,
    FrameProjections, PayloadDiscriminator, SignalSchemaField,
    build_extraction_config_with_semantic_schemas, read_cell,
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
        ValueKind::Object => {
            let mut members = Vec::new();
            value.visit_object(&mut |name, member| {
                members.push(format!("{name}:{}", render(member)))
            });
            format!("{{{}}}", members.join(","))
        }
        ValueKind::Array => {
            let mut items = Vec::new();
            value.visit_array(&mut |_, item| items.push(render(item)));
            format!("[{}]", items.join(","))
        }
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

    fn text_frame(
        &mut self,
        _hash_column: u32,
        _bytes: &[u8],
        _hash: &mut [u8; TEXT_HASH_BYTES],
        _inline: &mut Vec<u8>,
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
            members: vec![DiscriminatedMember {
                discriminant: "order".into(),
                member: "name".into(),
                path: Vec::new(),
                column: 1,
            }],
        }),
        CellLayout {
            cell_metadata: &[None, None, cell.clone(), cell],
            record_members: &[RecordMember {
                discriminant: "order".into(),
                subtype: None,
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
    Arr(Vec<Mp<'a>>),
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
        Mp::Arr(items) => {
            out.push(0x90 | u8::try_from(items.len()).unwrap());
            for item in items {
                msgpack(out, item);
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

/// The rows a lane stores from `json` or its MessagePack twin `msgpack`
/// into `columns`, or its refusal of the batch.
fn run(
    lane: &str,
    config: &columine_parsing::ExtractionConfig,
    columns: &mut DynamicColumns,
    json: &[u8],
    msgpack: &[u8],
) -> Result<usize, ExtractionError> {
    let mut embedder = TextCells::default();
    let mut work = [0; 512];
    match lane {
        "json" => extract_json_events(
            json,
            config,
            columns,
            &mut work,
            &mut ExtractionDiagnostic::default(),
            &mut NoCategories,
            &mut CellWriter::Embedder(&mut embedder),
            &mut Vec::new(),
        ),
        _ => extract_msgpack_events_with_projections(
            msgpack,
            config,
            columns,
            &mut work,
            false,
            FrameProjections::default(),
            &mut NoCategories,
            &mut CellWriter::Embedder(&mut embedder),
            &mut Vec::new(),
        ),
    }
}

/// The cells at `cells` of every row a lane stores from `json` or its
/// MessagePack twin `msgpack`, as text: `-` is the empty cell.
fn extract<const N: usize>(
    lane: &str,
    fields: &[SignalSchemaField],
    config: &columine_parsing::ExtractionConfig,
    json: &[u8],
    msgpack: &[u8],
    cells: [usize; N],
) -> Vec<[String; N]> {
    let mut columns = DynamicColumns::new(fields, 8).expect("column plane fits");
    let rows = run(lane, config, &mut columns, json, msgpack)
        .unwrap_or_else(|error| panic!("{lane}: the batch is accepted: {error:?}"));
    (0..rows)
        .map(|row| {
            cells.map(|column| match read_cell(&columns, column, row) {
                None => "-".to_owned(),
                Some(ColumnValue::Utf8(text)) => text.into_owned(),
                Some(ColumnValue::Binary(bytes)) => String::from_utf8_lossy(&bytes).into_owned(),
                Some(other) => format!("{other:?}"),
            })
        })
        .collect()
}

/// Columns 1 to 3 of every row the lane stores from the `order` events.
fn stored(lane: &str, semantic: bool) -> Vec<[String; 3]> {
    let (fields, config) = record_config(semantic);
    extract(
        lane,
        &fields,
        &config,
        JSON_ORDERS,
        &msgpack_orders(),
        [1, 2, 3],
    )
}

const SHADOWED_NAMES: [&str; 5] = [
    "type",
    "value.name",
    "value.$declared",
    "value.$undeclared",
    "value.qty",
];

/// [`record_config`] with a column `value.qty` that type `tally`'s `qty` has,
/// by its discriminated row: `order`'s `qty` keeps its routing row.
fn shadowed_config(semantic: bool) -> (Vec<SignalSchemaField>, columine_parsing::ExtractionConfig) {
    let fields = vec![
        SignalSchemaField::new(ArrowType::Utf8, true),
        SignalSchemaField::new(ArrowType::Utf8, true),
        SignalSchemaField::new(ArrowType::Binary, true),
        SignalSchemaField::new(ArrowType::Binary, true),
        SignalSchemaField::new(ArrowType::Int64, true),
    ];
    let schemas = parse_schema_envelope(
        br#"{"order":{"kind":"object","fields":{"name":{"kind":"string"},"qty":{"kind":"optional","value":{"kind":"number"}}}},"tally":{"kind":"object","fields":{"qty":{"kind":"number"}}}}"#,
    )
    .unwrap();
    let cell = Some(ENCODING.to_owned());
    let config = build_extraction_config_with_semantic_schemas(
        &fields,
        &SHADOWED_NAMES,
        &[],
        semantic.then_some(&schemas),
        Some(&PayloadDiscriminator {
            key: "type".into(),
            members: vec![
                DiscriminatedMember {
                    discriminant: "order".into(),
                    member: "name".into(),
                    path: Vec::new(),
                    column: 1,
                },
                DiscriminatedMember {
                    discriminant: "tally".into(),
                    member: "qty".into(),
                    path: Vec::new(),
                    column: 4,
                },
            ],
        }),
        CellLayout {
            cell_metadata: &[None, None, cell.clone(), cell, None],
            record_members: &[RecordMember {
                discriminant: "order".into(),
                subtype: None,
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

/// An `order` and a `tally` in each spelling.
const JSON_SHADOWED: &[u8] = br#"[
    {"type":"order","value":{"name":"a","qty":5}},
    {"type":"order","value.name":"c","value.qty":7},
    {"type":"tally","value":{"qty":9}},
    {"type":"tally","value.qty":10}
]"#;

/// [`JSON_SHADOWED`] as a MessagePack array.
fn msgpack_shadowed() -> Vec<u8> {
    use Mp::{Int, Map, Str};
    let events = [
        Map(vec![
            ("type", Str("order")),
            ("value", Map(vec![("name", Str("a")), ("qty", Int(5))])),
        ]),
        Map(vec![
            ("type", Str("order")),
            ("value.name", Str("c")),
            ("value.qty", Int(7)),
        ]),
        Map(vec![
            ("type", Str("tally")),
            ("value", Map(vec![("qty", Int(9))])),
        ]),
        Map(vec![("type", Str("tally")), ("value.qty", Int(10))]),
    ];
    let mut out = vec![0x90 | u8::try_from(events.len()).unwrap()];
    for event in &events {
        msgpack(&mut out, event);
    }
    out
}

#[test]
fn a_routed_member_keeps_its_declared_record_beside_another_types_column_of_its_name() {
    let lanes = [
        ("json", true),
        ("json", false),
        ("msgpack", true),
        ("msgpack", false),
    ];
    let row = |cells: [&str; 3]| cells.map(str::to_owned);
    let expected = [
        // `order`'s `qty` is in its record, nested and flat.
        row(["a", "[3]@7=5", "-"]),
        row(["c", "[3]@7=7", "-"]),
        // `tally`'s `qty` is its column, nested and flat.
        row(["-", "-", "Int(9)"]),
        row(["-", "-", "Int(10)"]),
    ];
    let stored = |lane: &str, semantic: bool| {
        let (fields, config) = shadowed_config(semantic);
        extract(
            lane,
            &fields,
            &config,
            JSON_SHADOWED,
            &msgpack_shadowed(),
            [1, 2, 4],
        )
    };
    assert_eq!(
        lanes.map(|(lane, semantic)| (lane, semantic, stored(lane, semantic))),
        lanes.map(|(lane, semantic)| (lane, semantic, expected.to_vec())),
        "(lane, semantic schema, [value.name, value.$declared, value.qty] per row)"
    );
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

/// `type`, `value.kind` and the declared record, with no carrier: `step`'s
/// `detail` lives in the record at `[1]`, descriptor 3, on rows whose `kind`
/// is `a`, and at `[2]`, descriptor 4, on rows whose `kind` is `b`.
fn subtype_config() -> (Vec<SignalSchemaField>, columine_parsing::ExtractionConfig) {
    let fields = vec![
        SignalSchemaField::new(ArrowType::Utf8, true),
        SignalSchemaField::new(ArrowType::Utf8, true),
        SignalSchemaField::new(ArrowType::Binary, true),
    ];
    let schemas = parse_schema_envelope(
        br#"{"step":{"kind":"object","fields":{"kind":{"kind":"string"},"detail":{"kind":"number"}}}}"#,
    )
    .unwrap();
    let row = |subtype: &str, field: u16, descriptor_position: u32| RecordMember {
        discriminant: "step".into(),
        subtype: Some(RecordSubtype {
            key: "kind".into(),
            value: subtype.into(),
        }),
        member: "detail".into(),
        path: vec![field],
        descriptor_position,
    };
    let config = build_extraction_config_with_semantic_schemas(
        &fields,
        &["type", "value.kind", "value.$declared"],
        &[],
        Some(&schemas),
        Some(&PayloadDiscriminator {
            key: "type".into(),
            members: vec![DiscriminatedMember {
                discriminant: "step".into(),
                member: "kind".into(),
                path: Vec::new(),
                column: 1,
            }],
        }),
        CellLayout {
            cell_metadata: &[None, None, Some(ENCODING.to_owned())],
            record_members: &[row("a", 1, 3), row("b", 2, 4)],
            encoding: Some(ENCODING),
            ..CellLayout::default()
        },
    )
    .unwrap();
    (fields, config)
}

/// One `step` of each subtype in each spelling: nested and flat, the subtype
/// key before and after the member, `type` before and after the payload.
const JSON_STEPS: &[u8] = br#"[
    {"type":"step","value":{"kind":"a","detail":5}},
    {"type":"step","value":{"detail":6,"kind":"b"}},
    {"type":"step","value.kind":"a","value.detail":7},
    {"value.detail":8,"type":"step","value.kind":"b"}
]"#;

/// [`JSON_STEPS`] as a MessagePack array.
fn msgpack_steps() -> Vec<u8> {
    use Mp::{Int, Map, Str};
    let events = [
        Map(vec![
            ("type", Str("step")),
            ("value", Map(vec![("kind", Str("a")), ("detail", Int(5))])),
        ]),
        Map(vec![
            ("type", Str("step")),
            ("value", Map(vec![("detail", Int(6)), ("kind", Str("b"))])),
        ]),
        Map(vec![
            ("type", Str("step")),
            ("value.kind", Str("a")),
            ("value.detail", Int(7)),
        ]),
        Map(vec![
            ("value.detail", Int(8)),
            ("type", Str("step")),
            ("value.kind", Str("b")),
        ]),
    ];
    let mut out = vec![0x90 | u8::try_from(events.len()).unwrap()];
    for event in &events {
        msgpack(&mut out, event);
    }
    out
}

#[test]
fn a_subtype_routing_row_places_its_member_in_the_record_on_rows_of_its_subtype() {
    let (fields, config) = subtype_config();
    for lane in ["json", "msgpack"] {
        assert_eq!(
            extract(lane, &fields, &config, JSON_STEPS, &msgpack_steps(), [1, 2]),
            [
                ["a", "[1]@3=5"],
                ["b", "[2]@4=6"],
                ["a", "[1]@3=7"],
                ["b", "[2]@4=8"]
            ]
            .map(|row| row.map(str::to_owned)),
            "{lane}: [value.kind, value.$declared] per row, the subtype key before or after its member"
        );
    }
}

#[test]
fn a_payload_member_nothing_places_is_refused_by_name_where_the_schema_has_a_record() {
    use Mp::{Int, Map, Str};
    let (fields, config) = subtype_config();
    let one = |payload: Mp<'static>| {
        let mut out = vec![0x91];
        msgpack(
            &mut out,
            &Map(vec![("type", Str("step")), ("value", payload)]),
        );
        out
    };
    let cases: [(&[u8], Vec<u8>, &str); 2] = [
        (
            br#"[{"type":"step","value":{"kind":"c","detail":9}}]"#,
            one(Map(vec![("kind", Str("c")), ("detail", Int(9))])),
            "value.detail",
        ),
        (
            br#"[{"type":"step","value":{"kind":"a","detail":1,"stray":2}}]"#,
            one(Map(vec![
                ("kind", Str("a")),
                ("detail", Int(1)),
                ("stray", Int(2)),
            ])),
            "value.stray",
        ),
    ];
    for (json, packed, path) in &cases {
        for lane in ["json", "msgpack"] {
            let mut columns = DynamicColumns::new(&fields, 8).expect("column plane fits");
            assert_eq!(
                run(lane, &config, &mut columns, json, packed),
                Err(ExtractionError::UnplacedMember {
                    path: (*path).to_owned()
                }),
                "{lane}: {path}, which no column, routing row or carrier places"
            );
        }
    }
}

/// The rule asks for no column of a reserved name: a schema whose payload
/// members have columns of their own, with no declared record and no carrier,
/// refuses a member nothing places as a schema with a record does. With a
/// carrier the same member is the carrier's.
#[test]
fn a_payload_member_nothing_places_is_refused_by_name_whatever_the_columns_are_named() {
    use Mp::{Int, Map, Str};
    let fields = vec![
        SignalSchemaField::new(ArrowType::Utf8, true),
        SignalSchemaField::new(ArrowType::Utf8, true),
        SignalSchemaField::new(ArrowType::Binary, true),
    ];
    let config = |names: &[&str; 3]| {
        build_extraction_config_with_semantic_schemas(
            &fields,
            names,
            &[],
            None,
            None,
            CellLayout {
                cell_metadata: &[None, None, Some(ENCODING.to_owned())],
                encoding: Some(ENCODING),
                ..CellLayout::default()
            },
        )
        .unwrap()
    };
    let json: &[u8] = br#"[{"type":"order","value":{"name":"a","stray":2}}]"#;
    let mut packed = vec![0x91];
    msgpack(
        &mut packed,
        &Map(vec![
            ("type", Str("order")),
            ("value", Map(vec![("name", Str("a")), ("stray", Int(2))])),
        ]),
    );
    let plain = config(&["type", "value.name", "value.detail"]);
    let carried = config(&["type", "value.name", "value.$undeclared"]);
    for lane in ["json", "msgpack"] {
        let mut columns = DynamicColumns::new(&fields, 8).expect("column plane fits");
        assert_eq!(
            run(lane, &plain, &mut columns, json, &packed),
            Err(ExtractionError::UnplacedMember {
                path: "value.stray".to_owned()
            }),
            "{lane}: no column, routing row or carrier places value.stray"
        );
        let mut columns = DynamicColumns::new(&fields, 8).expect("column plane fits");
        assert_eq!(
            run(lane, &carried, &mut columns, json, &packed),
            Ok(1),
            "{lane}: a carrier holds what nothing else places"
        );
    }
}
//#region smoo/columine!n/vocab-bounded-string
/// `box` has a structured column the embedder writes and `meta` is routed to
/// the declared record; each holds optional bounded strings at depth.
fn card_config() -> (Vec<SignalSchemaField>, columine_parsing::ExtractionConfig) {
    let fields = vec![
        SignalSchemaField::new(ArrowType::Utf8, true),
        SignalSchemaField::new(ArrowType::Binary, true),
        SignalSchemaField::new(ArrowType::Binary, true),
        SignalSchemaField::new(ArrowType::Binary, true),
    ];
    let schemas = parse_schema_envelope(
        br#"{"card":{"kind":"object","fields":{"box":{"kind":"object","fields":{"tag":{"kind":"optional","value":{"kind":"string","maxBytes":4}},"free":{"kind":"optional","value":{"kind":"string"}},"items":{"kind":"array","item":{"kind":"object","fields":{"tag":{"kind":"optional","value":{"kind":"string","maxBytes":4}}}}}}},"meta":{"kind":"optional","value":{"kind":"object","fields":{"tag":{"kind":"optional","value":{"kind":"string","maxBytes":4}},"by":{"kind":"record","value":{"kind":"optional","value":{"kind":"string","maxBytes":4}}}}}}}}}"#,
    )
    .unwrap();
    let cell = Some(ENCODING.to_owned());
    let config = build_extraction_config_with_semantic_schemas(
        &fields,
        &["type", "value.box", "value.$declared", "value.$undeclared"],
        &[],
        Some(&schemas),
        Some(&PayloadDiscriminator {
            key: "type".into(),
            members: vec![DiscriminatedMember {
                discriminant: "card".into(),
                member: "box".into(),
                path: Vec::new(),
                column: 1,
            }],
        }),
        CellLayout {
            cell_metadata: &[None, cell.clone(), cell.clone(), cell],
            record_members: &[RecordMember {
                discriminant: "card".into(),
                subtype: None,
                member: "meta".into(),
                path: vec![4],
                descriptor_position: 9,
            }],
            encoding: Some(ENCODING),
            ..CellLayout::default()
        },
    )
    .unwrap();
    (fields, config)
}

/// An empty text under an optional bounded string inside a structured value
/// is the empty state there too: the cell and the record member the embedder
/// writes hold no such member, in both spellings and on both lanes; an
/// unbounded string keeps its empty text.
#[test]
fn a_structured_cell_holds_no_empty_text_of_an_optional_bounded_string() {
    const JSON: &[u8] = br#"[
        {"type":"card","value":{"box":{"tag":"","free":"","items":[{"tag":""},{"tag":"t"}]},"meta":{"tag":"","by":{"a":"","b":"x"}}}},
        {"value.box":{"tag":"","free":"","items":[{"tag":""},{"tag":"t"}]},"value.meta":{"tag":"","by":{"a":"","b":"x"}},"type":"card"}
    ]"#;
    use Mp::{Arr, Map, Str};
    let card_box = || {
        Map(vec![
            ("tag", Str("")),
            ("free", Str("")),
            (
                "items",
                Arr(vec![
                    Map(vec![("tag", Str(""))]),
                    Map(vec![("tag", Str("t"))]),
                ]),
            ),
        ])
    };
    let meta = || {
        Map(vec![
            ("tag", Str("")),
            ("by", Map(vec![("a", Str("")), ("b", Str("x"))])),
        ])
    };
    let events = [
        Map(vec![
            ("type", Str("card")),
            ("value", Map(vec![("box", card_box()), ("meta", meta())])),
        ]),
        Map(vec![
            ("value.box", card_box()),
            ("value.meta", meta()),
            ("type", Str("card")),
        ]),
    ];
    let mut packed = vec![0x90 | u8::try_from(events.len()).unwrap()];
    for event in &events {
        msgpack(&mut packed, event);
    }
    let (fields, config) = card_config();
    let row = [
        r#"member 1 {free:"",items:[{},{tag:"t"}]}"#.to_owned(),
        r#"[4]@9={by:{b:"x"}}"#.to_owned(),
    ];
    for lane in ["json", "msgpack"] {
        assert_eq!(
            extract(lane, &fields, &config, JSON, &packed, [1, 2]),
            vec![row.clone(), row.clone()],
            "{lane}: [value.box, value.$declared] per row"
        );
    }
}
//#endregion smoo/columine!n/vocab-bounded-string
