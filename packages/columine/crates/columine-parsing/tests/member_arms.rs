//! A member whose value is one of several objects is placed by path rows
//! (specs/columine/10-physical-vocabulary.md §7 "Path rows place a set of
//! arms"): a row for every member of every variant, and both lanes store the
//! values of the object they are given, in both payload spellings.

use columine_arrow::{DictionarySlot, IndexWidth, SchemaDictionary};
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

//#region smoo/columine!n/vocab-member-arms
const NAMES: [&str; 6] = [
    "type",
    "value.id",
    "value.sender.kind",
    "value.sender.from",
    "value.sender.tx",
    "value.sender.agent",
];
const ID: usize = 1;
const KIND: usize = 2;
const FROM: usize = 3;
const TX: usize = 4;
const AGENT: usize = 5;

fn fields() -> [SignalSchemaField; 6] {
    let utf8 = SignalSchemaField::new(ArrowType::Utf8, true);
    let kind = SignalSchemaField::new_dictionary(DictionarySlot::Schema(0), IndexWidth::U8, true);
    let int64 = SignalSchemaField::new(ArrowType::Int64, true);
    [utf8, utf8, kind, utf8, int64, utf8]
}

/// The dictionary of the `kind` column.
fn kinds(values: [&str; 2]) -> [SchemaDictionary; 1] {
    [SchemaDictionary {
        slot: DictionarySlot::Schema(0),
        arrow_ids: vec![0],
        values: values.map(|value| Some(value.to_owned())).to_vec(),
    }]
}

const KINDS: [&str; 2] = ["inner", "outer"];

/// A `held` event whose `sender` is `variants`, each a schema.
fn envelope(variants: &[&str]) -> String {
    format!(
        r#"{{"held":{{"kind":"object","fields":{{"id":{{"kind":"string"}},"sender":{{"kind":"optional","value":{{"kind":"union","variants":[{}]}}}}}}}}}}"#,
        variants.join(",")
    )
}

/// The arm of an `inner` sender: its `from` and its `tx`.
const INNER: &str = r#"{"kind":"object","fields":{"kind":{"kind":"enum","variants":["inner"]},"from":{"kind":"string"},"tx":{"kind":"number"}}}"#;
/// The arm of an `outer` sender: its `from` and the `agent` it acts for.
const OUTER: &str = r#"{"kind":"object","fields":{"kind":{"kind":"enum","variants":["outer"]},"from":{"kind":"string"},"agent":{"kind":"string"}}}"#;

fn row(member: &str, path: &[&str], column: usize) -> DiscriminatedMember {
    DiscriminatedMember {
        discriminant: "held".into(),
        member: member.into(),
        path: path.iter().map(|name| (*name).to_owned()).collect(),
        column,
    }
}

/// A row for every member of both arms of `sender`.
fn sender_rows() -> Vec<DiscriminatedMember> {
    vec![
        row("sender", &["kind"], KIND),
        row("sender", &["from"], FROM),
        row("sender", &["tx"], TX),
        row("sender", &["agent"], AGENT),
    ]
}

/// A processor of `held` events whose `sender` is `variants`, placed by
/// `rows` beside the whole-member row of `id`.
fn create(
    variants: &[&str],
    dictionary: [&str; 2],
    rows: Vec<DiscriminatedMember>,
) -> Result<ExtractionConfig, ConfigError> {
    let schemas = parse_schema_envelope(envelope(variants).as_bytes()).unwrap();
    build_extraction_config_with_semantic_schemas(
        &fields(),
        &NAMES,
        &kinds(dictionary),
        Some(&schemas),
        Some(&PayloadDiscriminator {
            key: "type".into(),
            members: std::iter::once(row("id", &[], ID)).chain(rows).collect(),
        }),
        CellLayout::default(),
    )
}

/// A MessagePack value of the test events.
enum Mp<'a> {
    Str(&'a str),
    Int(u8),
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

/// The cells of `sender`'s four columns in every row a lane stores, as text:
/// `-` is the null cell and a `kind` is its ordinal in the dictionary.
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
        _ => {
            let mut batch = vec![0x90 | u8::try_from(events.len()).unwrap()];
            for event in events {
                msgpack(&mut batch, event);
            }
            extract_msgpack_events(
                &batch,
                config,
                &mut columns,
                &mut work,
                false,
                &mut NoCategories,
            )
        }
    }?;
    Ok((0..rows)
        .map(|row| {
            [KIND, FROM, TX, AGENT].map(|column| match read_cell(&columns, column, row) {
                None => "-".to_owned(),
                Some(ColumnValue::Int(value)) => value.to_string(),
                Some(ColumnValue::UInt(value)) => value.to_string(),
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
fn a_value_of_each_arm_lands_its_members_and_leaves_the_other_arms_null() {
    let config = create(&[INNER, OUTER], KINDS, sender_rows())
        .expect("arms with a row for every member of every variant are placed");
    let json = br#"[
        {"type":"held","value":{"id":"a","sender":{"kind":"inner","from":"x","tx":7}}},
        {"value":{"sender":{"agent":"z","from":"y","kind":"outer"},"id":"b"},"type":"held"},
        {"type":"held","value.id":"c","value.sender":{"kind":"outer","from":"w","agent":"v"}},
        {"type":"held","value.sender":{"kind":"inner","from":"u","tx":9},"value.id":"d"},
        {"type":"held","value":{"id":"e","sender":null}},
        {"type":"held","value":{"id":"f"}}
    ]"#;
    let held = |rest: Vec<(&'static str, Mp<'static>)>| {
        let mut members = vec![("type", Mp::Str("held"))];
        members.extend(rest);
        Mp::Map(members)
    };
    let inner = |from, tx| {
        Mp::Map(vec![
            ("kind", Mp::Str("inner")),
            ("from", Mp::Str(from)),
            ("tx", Mp::Int(tx)),
        ])
    };
    let events = [
        held(vec![(
            "value",
            Mp::Map(vec![("id", Mp::Str("a")), ("sender", inner("x", 7))]),
        )]),
        Mp::Map(vec![
            (
                "value",
                Mp::Map(vec![
                    (
                        "sender",
                        Mp::Map(vec![
                            ("agent", Mp::Str("z")),
                            ("from", Mp::Str("y")),
                            ("kind", Mp::Str("outer")),
                        ]),
                    ),
                    ("id", Mp::Str("b")),
                ]),
            ),
            ("type", Mp::Str("held")),
        ]),
        held(vec![
            ("value.id", Mp::Str("c")),
            (
                "value.sender",
                Mp::Map(vec![
                    ("kind", Mp::Str("outer")),
                    ("from", Mp::Str("w")),
                    ("agent", Mp::Str("v")),
                ]),
            ),
        ]),
        held(vec![
            ("value.sender", inner("u", 9)),
            ("value.id", Mp::Str("d")),
        ]),
        held(vec![(
            "value",
            Mp::Map(vec![("id", Mp::Str("e")), ("sender", Mp::Nil)]),
        )]),
        held(vec![("value", Mp::Map(vec![("id", Mp::Str("f"))]))]),
    ];
    for lane in LANES {
        assert_eq!(
            run(lane, &config, json, &events),
            Ok(vec![
                cells(["0", "x", "7", "-"]),
                cells(["1", "y", "-", "z"]),
                cells(["1", "w", "-", "v"]),
                cells(["0", "u", "9", "-"]),
                cells(["-", "-", "-", "-"]),
                cells(["-", "-", "-", "-"]),
            ]),
            "{lane}: each arm nested, each arm flat, a null and a missing sender"
        );
    }
}

#[test]
fn a_member_no_row_names_is_refused_by_its_path_on_both_lanes() {
    let config = create(&[INNER, OUTER], KINDS, sender_rows()).unwrap();
    let json =
        br#"[{"type":"held","value":{"id":"a","sender":{"kind":"inner","from":"x","stray":1}}}]"#;
    let events = [Mp::Map(vec![
        ("type", Mp::Str("held")),
        (
            "value",
            Mp::Map(vec![
                ("id", Mp::Str("a")),
                (
                    "sender",
                    Mp::Map(vec![
                        ("kind", Mp::Str("inner")),
                        ("from", Mp::Str("x")),
                        ("stray", Mp::Int(1)),
                    ]),
                ),
            ]),
        ),
    ])];
    for lane in LANES {
        assert_eq!(
            run(lane, &config, json, &events),
            Err(ExtractionError::UnplacedMember {
                path: "value.sender.stray".to_owned()
            }),
            "{lane}"
        );
    }
}

#[test]
fn creation_refuses_a_member_of_a_variant_that_no_row_names_by_its_path() {
    // `tx` is the first arm's alone, `agent` the second's, `from` both's.
    for (missing, path) in [
        (2, "value.sender.tx"),
        (3, "value.sender.agent"),
        (1, "value.sender.from"),
    ] {
        let mut rows = sender_rows();
        rows.remove(missing);
        assert_eq!(
            create(&[INNER, OUTER], KINDS, rows).err(),
            Some(ConfigError::UnplacedMember {
                event_type: "held".into(),
                path: path.into(),
            }),
        );
    }
}

#[test]
fn creation_refuses_a_row_whose_path_no_variant_has_by_its_path() {
    let mut rows = sender_rows();
    rows.push(row("sender", &["addressed"], AGENT));
    let mut beneath_a_text = sender_rows();
    beneath_a_text[1] = row("sender", &["from", "host"], FROM);
    for (rows, path) in [
        (rows, "value.sender.addressed"),
        (beneath_a_text, "value.sender.from.host"),
    ] {
        assert_eq!(
            create(&[INNER, OUTER], KINDS, rows).err(),
            Some(ConfigError::UnknownMemberPath {
                event_type: "held".into(),
                path: path.into(),
            }),
        );
    }
}

#[test]
fn creation_refuses_path_rows_beneath_a_union_with_a_variant_that_is_no_object() {
    for variants in [
        [INNER, r#"{"kind":"string"}"#],
        [r#"{"kind":"string"}"#, r#"{"kind":"number"}"#],
    ] {
        assert_eq!(
            create(&variants, KINDS, sender_rows()).err(),
            Some(ConfigError::UnknownMemberPath {
                event_type: "held".into(),
                // The first of the rows, which are held by name.
                path: "value.sender.agent".into(),
            }),
        );
    }
}

#[test]
fn enums_the_variants_declare_under_one_path_share_the_columns_dictionary() {
    // Each arm narrows the one dictionary, whatever its order.
    create(&[INNER, OUTER], ["outer", "inner"], sender_rows())
        .expect("the dictionary holds each arm's variant");
    // An enum beside a declaration that is no enum has no dictionary.
    let text_kind = r#"{"kind":"object","fields":{"kind":{"kind":"string"},"from":{"kind":"string"},"agent":{"kind":"string"}}}"#;
    assert_eq!(
        create(&[INNER, text_kind], KINDS, sender_rows()).err(),
        Some(ConfigError::EnumDictionaryMismatch {
            event_type: "held".into(),
            path: "value.sender.kind".into(),
            column: KIND,
            value: None,
        }),
    );
}
//#endregion smoo/columine!n/vocab-member-arms
