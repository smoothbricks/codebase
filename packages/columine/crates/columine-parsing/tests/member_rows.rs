//! A schema with a discriminator places its payload members by its rows
//! alone (specs/columine/10-physical-vocabulary.md §7 "Rows alone place a
//! member"): a column's name places nothing there, on either lane or in
//! either payload spelling.

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

//#region smoo/columine!n/vocab-member-rows
const NAMES: [&str; 4] = ["type", "value.order_id", "value.note", "value.$undeclared"];
const ORDER_ID: usize = 1;
const NOTE: usize = 2;
const CARRIER: usize = 3;

/// The columns of the schema, with or without its carrier.
fn fields(carrier: bool) -> Vec<SignalSchemaField> {
    let utf8 = SignalSchemaField::new(ArrowType::Utf8, true);
    let mut fields = vec![utf8, utf8, utf8];
    if carrier {
        fields.push(SignalSchemaField::new(ArrowType::Binary, true));
    }
    fields
}

/// An `order` that declares both members the schema has a column named for.
const ENVELOPE: &[u8] = br#"{"order":{"kind":"object","fields":{"order_id":{"kind":"string"},"note":{"kind":"optional","value":{"kind":"string"}}}}}"#;

fn row(event_type: &str, member: &str, column: usize) -> DiscriminatedMember {
    DiscriminatedMember {
        discriminant: event_type.into(),
        member: member.into(),
        path: Vec::new(),
        column,
    }
}

/// A schema whose rows are `rows`, or one created with no discriminator.
fn create(
    carrier: bool,
    envelope: Option<&[u8]>,
    rows: Option<Vec<DiscriminatedMember>>,
) -> Result<ExtractionConfig, ConfigError> {
    let fields = fields(carrier);
    let schemas = envelope.map(|envelope| parse_schema_envelope(envelope).unwrap());
    let discriminator = rows.map(|members| PayloadDiscriminator {
        key: "type".into(),
        members,
    });
    build_extraction_config_with_semantic_schemas(
        &fields,
        &NAMES[..fields.len()],
        &[],
        schemas.as_ref(),
        discriminator.as_ref(),
        CellLayout::default(),
    )
}

/// A MessagePack value of the test events.
enum Mp<'a> {
    Str(&'a str),
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
        Mp::Map(entries) => {
            out.push(0x80 | u8::try_from(entries.len()).unwrap());
            for (key, value) in entries {
                msgpack(out, &Mp::Str(key));
                msgpack(out, value);
            }
        }
    }
}

/// The carrier cell that holds one text under one path.
fn carried(path: &str, text: &str) -> Option<Vec<u8>> {
    let mut cell = vec![0xdf, 0, 0, 0, 1];
    msgpack(&mut cell, &Mp::Str(path));
    msgpack(&mut cell, &Mp::Str(text));
    Some(cell)
}

/// What one stored row holds: the `value.order_id` cell, the `value.note`
/// cell (`-` is the null cell) and the carrier cell.
type Row = (String, String, Option<Vec<u8>>);

fn stored(order_id: &str, note: &str, carrier: Option<Vec<u8>>) -> Row {
    (order_id.to_owned(), note.to_owned(), carrier)
}

fn run(
    lane: &str,
    carrier: bool,
    config: &ExtractionConfig,
    json: &[u8],
    events: &[Mp<'_>],
) -> Result<Vec<Row>, ExtractionError> {
    let fields = fields(carrier);
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
        _ => {
            let mut bytes = vec![0x90 | u8::try_from(events.len()).unwrap()];
            for event in events {
                msgpack(&mut bytes, event);
            }
            extract_msgpack_events(
                &bytes,
                config,
                &mut columns,
                &mut work,
                false,
                &mut NoCategories,
            )
        }
    }?;
    let text = |column, row| match read_cell(&columns, column, row) {
        None => "-".to_owned(),
        Some(ColumnValue::Utf8(text)) => text.into_owned(),
        Some(other) => panic!("row {row}, column {column}: {other:?}"),
    };
    Ok((0..rows)
        .map(|row| {
            let carrier = carrier
                .then(|| read_cell(&columns, CARRIER, row))
                .flatten()
                .map(|cell| match cell {
                    ColumnValue::Binary(bytes) => bytes.into_owned(),
                    other => panic!("row {row}, carrier: {other:?}"),
                });
            (text(ORDER_ID, row), text(NOTE, row), carrier)
        })
        .collect())
}

const LANES: [&str; 2] = ["json", "msgpack"];

/// One event of `event_type` whose payload is nested in `value`.
fn nested<'a>(event_type: &'a str, members: Vec<(&'a str, Mp<'a>)>) -> Mp<'a> {
    Mp::Map(vec![
        ("type", Mp::Str(event_type)),
        ("value", Mp::Map(members)),
    ])
}

/// One event of `event_type` whose other top-level keys are `keys`.
fn flat<'a>(event_type: &'a str, keys: Vec<(&'a str, Mp<'a>)>) -> Mp<'a> {
    let mut members = vec![("type", Mp::Str(event_type))];
    members.extend(keys);
    Mp::Map(members)
}

#[test]
fn a_member_no_row_names_is_carried_under_its_path_and_never_lands_in_a_column_of_its_name() {
    let config = create(true, None, Some(vec![row("order", "order_id", ORDER_ID)])).unwrap();
    let json = br#"[
        {"type":"order","value":{"order_id":"a","note":"n"}},
        {"value":{"note":"m","order_id":"b"},"type":"order"},
        {"type":"order","value.order_id":"c","value.note":"o"},
        {"type":"memo","value":{"order_id":"d"}}
    ]"#;
    let events = [
        nested(
            "order",
            vec![("order_id", Mp::Str("a")), ("note", Mp::Str("n"))],
        ),
        Mp::Map(vec![
            (
                "value",
                Mp::Map(vec![("note", Mp::Str("m")), ("order_id", Mp::Str("b"))]),
            ),
            ("type", Mp::Str("order")),
        ]),
        flat(
            "order",
            vec![
                ("value.order_id", Mp::Str("c")),
                ("value.note", Mp::Str("o")),
            ],
        ),
        nested("memo", vec![("order_id", Mp::Str("d"))]),
    ];
    for lane in LANES {
        assert_eq!(
            run(lane, true, &config, json, &events),
            Ok(vec![
                stored("a", "-", carried("note", "n")),
                stored("b", "-", carried("note", "m")),
                stored("c", "-", carried("value%2Enote", "o")),
                stored("-", "-", carried("order_id", "d")),
            ]),
            "{lane}: nested, nested before `type`, flat, and a type no row names"
        );
    }
}

#[test]
fn a_member_no_row_names_is_refused_by_name_where_the_schema_has_no_carrier() {
    let config = create(false, None, Some(vec![row("order", "order_id", ORDER_ID)])).unwrap();
    let unplaced = |path: &str| {
        Err(ExtractionError::UnplacedMember {
            path: path.to_owned(),
        })
    };
    let cases: [(&[u8], Mp<'_>, &str); 3] = [
        (
            br#"[{"type":"order","value":{"order_id":"a","note":"n"}}]"#,
            nested(
                "order",
                vec![("order_id", Mp::Str("a")), ("note", Mp::Str("n"))],
            ),
            "value.note",
        ),
        (
            br#"[{"type":"order","value.order_id":"a","value.note":"n"}]"#,
            flat(
                "order",
                vec![
                    ("value.order_id", Mp::Str("a")),
                    ("value.note", Mp::Str("n")),
                ],
            ),
            "value.note",
        ),
        (
            br#"[{"type":"memo","value":{"order_id":"a"}}]"#,
            nested("memo", vec![("order_id", Mp::Str("a"))]),
            "value.order_id",
        ),
    ];
    for lane in LANES {
        for (json, event, path) in &cases {
            assert_eq!(
                run(lane, false, &config, json, core::slice::from_ref(event)),
                unplaced(path),
                "{lane}: {path}"
            );
        }
    }
}

#[test]
fn a_member_lands_in_the_column_its_row_names_for_its_own_type_alone() {
    let config = create(
        true,
        None,
        Some(vec![
            row("order", "order_id", ORDER_ID),
            row("memo", "note", NOTE),
        ]),
    )
    .unwrap();
    let json = br#"[
        {"type":"memo","value":{"note":"n"}},
        {"type":"memo","value.note":"m"},
        {"type":"order","value":{"order_id":"a","note":"o"}}
    ]"#;
    let events = [
        nested("memo", vec![("note", Mp::Str("n"))]),
        flat("memo", vec![("value.note", Mp::Str("m"))]),
        nested(
            "order",
            vec![("order_id", Mp::Str("a")), ("note", Mp::Str("o"))],
        ),
    ];
    for lane in LANES {
        assert_eq!(
            run(lane, true, &config, json, &events),
            Ok(vec![
                stored("-", "n", None),
                stored("-", "m", None),
                stored("a", "-", carried("note", "o")),
            ]),
            "{lane}: a memo's note has a row, an order's note has none"
        );
    }
}

#[test]
fn a_top_level_key_never_reaches_the_value_column_of_its_name() {
    let config = create(true, None, Some(vec![row("order", "note", NOTE)])).unwrap();
    let json = br#"[{"type":"order","note":"n"}]"#;
    let events = [flat("order", vec![("note", Mp::Str("n"))])];
    for lane in LANES {
        assert_eq!(
            run(lane, true, &config, json, &events),
            Ok(vec![stored("-", "-", carried("note", "n"))]),
            "{lane}"
        );
    }
}

#[test]
fn creation_refuses_a_declared_member_no_row_names_whatever_columns_exist() {
    for carrier in [false, true] {
        assert_eq!(
            create(
                carrier,
                Some(ENVELOPE),
                Some(vec![row("order", "order_id", ORDER_ID)])
            )
            .err(),
            Some(ConfigError::UnplacedMember {
                event_type: "order".into(),
                path: "value.note".into(),
            }),
            "carrier {carrier}: `note` has a column named for it and no row"
        );
        assert!(
            create(
                carrier,
                Some(ENVELOPE),
                Some(vec![
                    row("order", "order_id", ORDER_ID),
                    row("order", "note", NOTE),
                ]),
            )
            .is_ok(),
            "carrier {carrier}: a row for every declared member"
        );
    }
}

#[test]
fn a_schema_with_no_discriminator_places_a_member_in_the_column_named_for_it() {
    let json = br#"[
        {"type":"order","value":{"order_id":"a","note":"n"}},
        {"type":"order","value.order_id":"b","value.note":"m"}
    ]"#;
    let events = [
        nested(
            "order",
            vec![("order_id", Mp::Str("a")), ("note", Mp::Str("n"))],
        ),
        flat(
            "order",
            vec![
                ("value.order_id", Mp::Str("b")),
                ("value.note", Mp::Str("m")),
            ],
        ),
    ];
    let stray_json = br#"[{"type":"order","value":{"order_id":"a","stray":"s"}}]"#;
    let stray = [nested(
        "order",
        vec![("order_id", Mp::Str("a")), ("stray", Mp::Str("s"))],
    )];
    for envelope in [None, Some(ENVELOPE)] {
        for lane in LANES {
            let carrying = create(true, envelope, None).unwrap();
            assert_eq!(
                run(lane, true, &carrying, json, &events),
                Ok(vec![stored("a", "n", None), stored("b", "m", None)]),
                "{lane}: both spellings"
            );
            if envelope.is_some() {
                // The closed schema's stray member is the validator's to
                // refuse, not placement's.
                continue;
            }
            assert_eq!(
                run(lane, true, &carrying, stray_json, &stray),
                Ok(vec![stored("a", "-", carried("stray", "s"))]),
                "{lane}: a member with no column of its name is carried"
            );
            let bare = create(false, None, None).unwrap();
            assert_eq!(
                run(lane, false, &bare, stray_json, &stray),
                Err(ExtractionError::UnplacedMember {
                    path: "value.stray".to_owned(),
                }),
                "{lane}: and refused by name with no carrier"
            );
        }
    }
}
//#endregion smoo/columine!n/vocab-member-rows
