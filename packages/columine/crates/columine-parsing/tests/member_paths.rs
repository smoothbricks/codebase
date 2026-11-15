//! An object member no column holds whole is placed value by value
//! (specs/columine/10-physical-vocabulary.md §7 "A placement row may name a
//! path"): each placement row names a member path and its column, and both
//! lanes descend the object in both payload spellings.

use columine_parsing::json_extractor::{
    ExtractionDiagnostic, ExtractionError, extract_json_events,
};
use columine_parsing::msgpack_extractor::extract_msgpack_events;
use columine_parsing::validate::parse_schema_envelope;
use columine_parsing::{
    ArrowType, CellLayout, CellWriter, ColumnValue, ConfigError, DiscriminatedMember,
    DynamicColumns, ExtractionConfig, PayloadDiscriminator, SignalSchemaField,
    build_extraction_config_with_semantic_schemas, read_cell,
};
use columine_types::category::NoCategories;

//#region smoo/columine!n/vocab-member-paths
const NAMES: [&str; 7] = [
    "type",
    "value.order_id",
    "value.qty",
    "value.range.lo",
    "value.range.hi",
    "value.range.unit.name",
    "value.range.unit.note",
];
const LO: usize = 3;
const HI: usize = 4;
const UNIT_NAME: usize = 5;
const UNIT_NOTE: usize = 6;

fn fields() -> [SignalSchemaField; 7] {
    let utf8 = SignalSchemaField::new(ArrowType::Utf8, true);
    let int64 = SignalSchemaField::new(ArrowType::Int64, true);
    [utf8, utf8, int64, int64, int64, utf8, utf8]
}

/// An `order` whose optional `range` object no column holds: `lo`, an
/// optional `hi`, and an optional `unit` object of a `name` and an optional
/// bounded `note`.
const ENVELOPE: &[u8] = br#"{"order":{"kind":"object","fields":{"order_id":{"kind":"string"},"qty":{"kind":"optional","value":{"kind":"number"}},"range":{"kind":"optional","value":{"kind":"object","fields":{"lo":{"kind":"number"},"hi":{"kind":"optional","value":{"kind":"number"}},"unit":{"kind":"optional","value":{"kind":"object","fields":{"name":{"kind":"string"},"note":{"kind":"optional","value":{"kind":"string","maxBytes":8}}}}}}}}}}}"#;

fn row(member: &str, path: &[&str], column: usize) -> DiscriminatedMember {
    DiscriminatedMember {
        discriminant: "order".into(),
        member: member.into(),
        path: path.iter().map(|name| (*name).to_owned()).collect(),
        column,
    }
}

/// A row for every value of `range`.
fn range_rows() -> Vec<DiscriminatedMember> {
    vec![
        row("range", &["lo"], LO),
        row("range", &["hi"], HI),
        row("range", &["unit", "name"], UNIT_NAME),
        row("range", &["unit", "note"], UNIT_NOTE),
    ]
}

/// The whole-member rows of `order_id` and `qty`, each unless `members` names
/// that member, then `members`.
fn create(
    envelope: Option<&[u8]>,
    members: Vec<DiscriminatedMember>,
) -> Result<ExtractionConfig, ConfigError> {
    let schemas = envelope.map(|envelope| parse_schema_envelope(envelope).unwrap());
    build_extraction_config_with_semantic_schemas(
        &fields(),
        &NAMES,
        &[],
        schemas.as_ref(),
        Some(&PayloadDiscriminator {
            key: "type".into(),
            members: ["order_id", "qty"]
                .into_iter()
                .zip(1..)
                .filter(|(member, _)| members.iter().all(|row| row.member != *member))
                .map(|(member, column)| row(member, &[], column))
                .collect::<Vec<_>>()
                .into_iter()
                .chain(members)
                .collect(),
        }),
        CellLayout::default(),
    )
}

/// A MessagePack value of the test events.
enum Mp<'a> {
    Str(&'a str),
    Int(u8),
    True,
    Nil,
    Map(Vec<(&'a str, Mp<'a>)>),
}

fn msgpack(out: &mut Vec<u8>, value: &Mp<'_>) {
    match value {
        Mp::Str(text) => {
            out.push(
                0xa0 | u8::try_from(text.len())
                    .ok()
                    .filter(|len| *len < 32)
                    .unwrap(),
            );
            out.extend(text.as_bytes());
        }
        Mp::Int(value) => {
            assert!(*value < 0x80, "a positive fixint");
            out.push(*value);
        }
        Mp::True => out.push(0xc3),
        Mp::Nil => out.push(0xc0),
        Mp::Map(entries) => {
            out.push(0x80 | u8::try_from(entries.len()).unwrap());
            for (key, value) in entries {
                msgpack(out, &Mp::Str(key));
                msgpack(out, value);
            }
        }
    }
}

/// One `order` event whose other top-level members are `rest`.
fn order<'a>(rest: Vec<(&'a str, Mp<'a>)>) -> Mp<'a> {
    let mut members = vec![("type", Mp::Str("order"))];
    members.extend(rest);
    Mp::Map(members)
}

fn msgpack_events(events: &[Mp<'_>]) -> Vec<u8> {
    let mut out = vec![0x90 | u8::try_from(events.len()).unwrap()];
    for event in events {
        msgpack(&mut out, event);
    }
    out
}

/// The cells of `range`'s four columns in every row a lane stores, as text:
/// `-` is the null cell.
fn run(
    lane: &str,
    config: &ExtractionConfig,
    json: &[u8],
    events: &[Mp<'_>],
) -> Result<Vec<[String; 4]>, ExtractionError> {
    let fields = fields();
    let mut columns = DynamicColumns::new(&fields, 8).expect("column plane fits");
    let mut work = [0; 256];
    let rows = match lane {
        "json" => extract_json_events(
            json,
            config,
            &mut columns,
            &mut work,
            &mut ExtractionDiagnostic::default(),
            &mut NoCategories,
            &mut CellWriter::Native,
            &mut Vec::new(),
        ),
        _ => extract_msgpack_events(
            &msgpack_events(events),
            config,
            &mut columns,
            &mut work,
            false,
            &mut NoCategories,
        ),
    }?;
    Ok((0..rows)
        .map(|row| {
            [LO, HI, UNIT_NAME, UNIT_NOTE].map(|column| match read_cell(&columns, column, row) {
                None => "-".to_owned(),
                Some(ColumnValue::Int(value)) => value.to_string(),
                Some(ColumnValue::Utf8(text)) => text.into_owned(),
                Some(other) => panic!("row {row}, column {column}: {other:?}"),
            })
        })
        .collect())
}

const LANES: [&str; 2] = ["json", "msgpack"];

fn cells(row: [&str; 4]) -> [String; 4] {
    row.map(str::to_owned)
}

#[test]
fn an_object_member_is_placed_by_its_path_rows_to_any_depth_in_both_spellings() {
    let config = create(Some(ENVELOPE), range_rows())
        .expect("a member with a row for each of its values is placed");
    let json = br#"[
        {"type":"order","value":{"order_id":"a","range":{"lo":1,"hi":9,"unit":{"name":"mm","note":"wide"}}}},
        {"value":{"range":{"unit":{"name":"cm"},"lo":2},"order_id":"b"},"type":"order"},
        {"type":"order","value.order_id":"c","value.range":{"hi":7,"lo":3}}
    ]"#;
    let events = [
        order(vec![(
            "value",
            Mp::Map(vec![
                ("order_id", Mp::Str("a")),
                (
                    "range",
                    Mp::Map(vec![
                        ("lo", Mp::Int(1)),
                        ("hi", Mp::Int(9)),
                        (
                            "unit",
                            Mp::Map(vec![("name", Mp::Str("mm")), ("note", Mp::Str("wide"))]),
                        ),
                    ]),
                ),
            ]),
        )]),
        Mp::Map(vec![
            (
                "value",
                Mp::Map(vec![
                    (
                        "range",
                        Mp::Map(vec![
                            ("unit", Mp::Map(vec![("name", Mp::Str("cm"))])),
                            ("lo", Mp::Int(2)),
                        ]),
                    ),
                    ("order_id", Mp::Str("b")),
                ]),
            ),
            ("type", Mp::Str("order")),
        ]),
        order(vec![
            ("value.order_id", Mp::Str("c")),
            (
                "value.range",
                Mp::Map(vec![("hi", Mp::Int(7)), ("lo", Mp::Int(3))]),
            ),
        ]),
    ];
    for lane in LANES {
        assert_eq!(
            run(lane, &config, json, &events),
            Ok(vec![
                cells(["1", "9", "mm", "wide"]),
                cells(["2", "-", "cm", "-"]),
                cells(["3", "7", "-", "-"]),
            ]),
            "{lane}: nested with every value, nested before `type` with values absent, flat"
        );
    }
}

#[test]
fn an_absent_or_null_object_and_an_absent_null_or_empty_value_leave_their_cells_null() {
    let config = create(Some(ENVELOPE), range_rows()).unwrap();
    let json = br#"[
        {"type":"order","value":{"order_id":"a"}},
        {"type":"order","value":{"order_id":"b","range":null}},
        {"type":"order","value":{"order_id":"c","range":{"lo":4,"hi":null,"unit":null}}},
        {"type":"order","value":{"order_id":"d","range":{"lo":5,"unit":{"name":"m","note":""}}}}
    ]"#;
    let value = |id: &'static str, range: Option<Mp<'static>>| {
        let mut members = vec![("order_id", Mp::Str(id))];
        members.extend(range.map(|range| ("range", range)));
        order(vec![("value", Mp::Map(members))])
    };
    let events = [
        value("a", None),
        value("b", Some(Mp::Nil)),
        value(
            "c",
            Some(Mp::Map(vec![
                ("lo", Mp::Int(4)),
                ("hi", Mp::Nil),
                ("unit", Mp::Nil),
            ])),
        ),
        value(
            "d",
            Some(Mp::Map(vec![
                ("lo", Mp::Int(5)),
                (
                    "unit",
                    Mp::Map(vec![("name", Mp::Str("m")), ("note", Mp::Str(""))]),
                ),
            ])),
        ),
    ];
    for lane in LANES {
        assert_eq!(
            run(lane, &config, json, &events),
            Ok(vec![
                cells(["-", "-", "-", "-"]),
                cells(["-", "-", "-", "-"]),
                cells(["4", "-", "-", "-"]),
                cells(["5", "-", "m", "-"]),
            ]),
            "{lane}: no object, a null object, null values, an optional bounded string's empty text"
        );
    }
}

#[test]
fn a_value_of_the_wrong_type_is_refused_as_a_member_of_the_wrong_type_is() {
    let config = create(Some(ENVELOPE), range_rows()).unwrap();
    let member = (
        &br#"[{"type":"order","value":{"order_id":"a","qty":true}}]"#[..],
        [order(vec![(
            "value",
            Mp::Map(vec![("order_id", Mp::Str("a")), ("qty", Mp::True)]),
        )])],
    );
    let beneath = (
        &br#"[{"type":"order","value":{"order_id":"a","range":{"lo":true}}}]"#[..],
        [order(vec![(
            "value",
            Mp::Map(vec![
                ("order_id", Mp::Str("a")),
                ("range", Mp::Map(vec![("lo", Mp::True)])),
            ]),
        )])],
    );
    let not_an_object = (
        &br#"[{"type":"order","value":{"order_id":"a","range":5}}]"#[..],
        [order(vec![(
            "value",
            Mp::Map(vec![("order_id", Mp::Str("a")), ("range", Mp::Int(5))]),
        )])],
    );
    for lane in LANES {
        let refused = run(lane, &config, member.0, &member.1);
        assert_eq!(
            refused,
            Err(ExtractionError::InvalidFieldType),
            "{lane}: a member"
        );
        assert_eq!(
            run(lane, &config, beneath.0, &beneath.1),
            refused,
            "{lane}: a value a path row places"
        );
        assert_eq!(
            run(lane, &config, not_an_object.0, &not_an_object.1),
            refused,
            "{lane}: a number where the rows name members"
        );
    }
}

/// With no semantic schema the rows alone place the object, and a member
/// beneath it that no row names has no place.
#[test]
fn a_member_beneath_a_placed_object_that_no_row_names_is_refused_by_name() {
    let config = create(None, range_rows()).unwrap();
    let json = |range: &str| format!(r#"[{{"type":"order","value":{{"range":{range}}}}}]"#);
    let event = |range: Mp<'static>| [order(vec![("value", Mp::Map(vec![("range", range)]))])];
    for lane in LANES {
        assert_eq!(
            run(
                lane,
                &config,
                json(r#"{"lo":1,"unit":{"name":"mm"}}"#).as_bytes(),
                &event(Mp::Map(vec![
                    ("lo", Mp::Int(1)),
                    ("unit", Mp::Map(vec![("name", Mp::Str("mm"))])),
                ])),
            ),
            Ok(vec![cells(["1", "-", "mm", "-"])]),
            "{lane}: the rows place the object without a semantic schema"
        );
        let cases = [
            (
                json(r#"{"lo":1,"stray":2}"#),
                event(Mp::Map(vec![("lo", Mp::Int(1)), ("stray", Mp::Int(2))])),
                "value.range.stray",
            ),
            (
                json(r#"{"unit":{"name":"mm","stray":2}}"#),
                event(Mp::Map(vec![(
                    "unit",
                    Mp::Map(vec![("name", Mp::Str("mm")), ("stray", Mp::Int(2))]),
                )])),
                "value.range.unit.stray",
            ),
        ];
        for (json, events, path) in &cases {
            assert_eq!(
                run(lane, &config, json.as_bytes(), events),
                Err(ExtractionError::UnplacedMember {
                    path: (*path).to_owned()
                }),
                "{lane}: {path}"
            );
        }
    }
}

#[test]
fn creation_refuses_a_declared_member_no_column_and_no_row_places() {
    let refused = |members| create(Some(ENVELOPE), members).err();
    assert_eq!(
        refused(Vec::new()),
        Some(ConfigError::UnplacedMember),
        "`range` has neither a column nor rows"
    );
    let mut rows = range_rows();
    rows.remove(1);
    assert_eq!(
        refused(rows),
        Some(ConfigError::UnplacedMember),
        "`range.hi` is declared and no row names it"
    );
    let mut rows = range_rows();
    rows.truncate(2);
    assert_eq!(
        refused(rows),
        Some(ConfigError::UnplacedMember),
        "`range.unit` is declared and no row names a value of it"
    );
}

#[test]
fn creation_refuses_a_row_whose_path_the_schema_does_not_have() {
    let with = |extra: DiscriminatedMember| {
        let mut rows = range_rows();
        rows.push(extra);
        rows
    };
    let mut beneath_a_number = range_rows();
    beneath_a_number[0] = row("range", &["lo", "digits"], LO);
    for (rows, why) in [
        (
            with(row("range", &["mid"], HI)),
            "`range` declares no `mid`",
        ),
        (beneath_a_number, "`range.lo` is not an object"),
        (
            with(row("order_id", &["first"], HI)),
            "`order_id` is not an object",
        ),
    ] {
        assert_eq!(
            create(Some(ENVELOPE), rows).err(),
            Some(ConfigError::UnknownMemberPath),
            "{why}"
        );
    }
}

#[test]
fn creation_refuses_rows_that_place_a_member_and_a_member_beneath_it() {
    for (extra, why) in [
        (row("range", &[], LO), "the whole member after its values"),
        (
            row("range", &["unit"], UNIT_NAME),
            "an object after its values",
        ),
        (
            row("range", &["lo", "digits"], HI),
            "a value beneath a placed value",
        ),
    ] {
        let mut rows = range_rows();
        rows.push(extra);
        assert_eq!(
            create(None, rows).err(),
            Some(ConfigError::MemberRowsDisagree),
            "{why}"
        );
    }
    // The same path twice is one placement, the later row's.
    let mut rows = range_rows();
    rows.push(row("range", &["lo"], HI));
    rows.push(row("range", &["hi"], LO));
    let config = create(Some(ENVELOPE), rows).expect("a repeated path is no disagreement");
    let json = br#"[{"type":"order","value":{"order_id":"a","range":{"lo":1,"hi":9}}}]"#;
    let events = [order(vec![(
        "value",
        Mp::Map(vec![
            ("order_id", Mp::Str("a")),
            (
                "range",
                Mp::Map(vec![("lo", Mp::Int(1)), ("hi", Mp::Int(9))]),
            ),
        ]),
    )])];
    for lane in LANES {
        assert_eq!(
            run(lane, &config, json, &events),
            Ok(vec![cells(["9", "1", "-", "-"])]),
            "{lane}"
        );
    }
}
//#endregion smoo/columine!n/vocab-member-paths
