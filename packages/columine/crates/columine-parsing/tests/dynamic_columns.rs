//! A dynamic value's explicit null is a present cell in the column that holds
//! it (specs/columine/10-physical-vocabulary.md §7 "A dynamic member's null is
//! data"), whichever payload spelling reaches the column: a nested member, or
//! the column's own name as a flat `value.…` key, on both lanes.

use columine_arrow::DynamicMember;
use columine_parsing::json_extractor::{ExtractionDiagnostic, extract_json_events};
use columine_parsing::msgpack_extractor::extract_msgpack_events;
use columine_parsing::{
    ArrowType, CellLayout, CellWriter, ColumnValue, ConfigError, DiscriminatedMember,
    DynamicColumns, ExtractionConfig, PayloadDiscriminator, SignalSchemaField,
    build_extraction_config_with_semantic_schemas, read_cell,
};
use columine_types::category::NoCategories;

//#region smoo/columine!n/vocab-dynamic-null
fn utf8() -> SignalSchemaField {
    SignalSchemaField::new(ArrowType::Utf8, true)
}

fn binary() -> SignalSchemaField {
    SignalSchemaField::new(ArrowType::Binary, true)
}

/// A MessagePack value of the test events.
enum Mp<'a> {
    Str(&'a str),
    Nil,
    Map(Vec<(&'a str, Mp<'a>)>),
}

fn msgpack(out: &mut Vec<u8>, value: &Mp<'_>) {
    match value {
        Mp::Str(text) => {
            match u8::try_from(text.len()).unwrap() {
                len @ 0..=31 => out.push(0xa0 | len),
                len => out.extend([0xd9, len]),
            }
            out.extend(text.as_bytes());
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

fn msgpack_events(events: &[Mp<'_>]) -> Vec<u8> {
    let mut out = vec![0x90 | u8::try_from(events.len()).unwrap()];
    for event in events {
        msgpack(&mut out, event);
    }
    out
}

/// The cell of `column` in every row each lane stores, `[JSON, MessagePack]`:
/// `nil` is a present null, `-` the empty cell.
fn stored(
    config: &ExtractionConfig,
    fields: &[SignalSchemaField],
    column: usize,
    json: &[u8],
    events: &[Mp<'_>],
) -> [Vec<&'static str>; 2] {
    let cells = |columns: &DynamicColumns, rows: usize| {
        (0..rows)
            .map(|row| match read_cell(columns, column, row) {
                None => "-",
                Some(ColumnValue::Binary(bytes)) if *bytes == [0xc0] => "nil",
                Some(other) => panic!("row {row}: {other:?}"),
            })
            .collect::<Vec<_>>()
    };
    let mut work = [0; 256];
    let mut columns = DynamicColumns::new(fields, 8).expect("column plane fits");
    let rows = extract_json_events(
        json,
        config,
        &mut columns,
        &mut work,
        &mut ExtractionDiagnostic::default(),
        &mut NoCategories,
        &mut CellWriter::Native,
        &mut Vec::new(),
    )
    .expect("the JSON batch is accepted");
    let from_json = cells(&columns, rows);
    let mut columns = DynamicColumns::new(fields, 8).expect("column plane fits");
    let rows = extract_msgpack_events(
        &msgpack_events(events),
        config,
        &mut columns,
        &mut work,
        false,
        &mut NoCategories,
    )
    .expect("the MessagePack batch is accepted");
    [from_json, cells(&columns, rows)]
}

/// The variant column the discriminator places `order.data` in.
const VARIANT: &str = "value.$variant.order.data";

/// A member the owner names dynamic as `(event type, member)` marks the
/// discriminated column that places it, so the column's own name spelled
/// flat reaches the same dynamic cell as the nested member.
#[test]
fn a_named_dynamic_members_null_is_data_in_its_discriminated_column_in_both_spellings() {
    let fields = [utf8(), binary()];
    let config = build_extraction_config_with_semantic_schemas(
        &fields,
        &["type", VARIANT],
        &[],
        None,
        Some(&PayloadDiscriminator {
            key: "type".into(),
            members: vec![DiscriminatedMember {
                discriminant: "order".into(),
                member: "data".into(),
                column: 1,
            }],
        }),
        CellLayout {
            dynamic_members: &[DynamicMember {
                event_type: "order".into(),
                member: "data".into(),
            }],
            ..CellLayout::default()
        },
    )
    .unwrap();
    let json = br#"[
        {"type":"order","value":{"data":null}},
        {"type":"order","value.$variant.order.data":null},
        {"value.$variant.order.data":null,"type":"order"},
        {"type":"order","value":{}},
        {"type":"order"}
    ]"#;
    let order = || ("type", Mp::Str("order"));
    let events = [
        Mp::Map(vec![order(), ("value", Mp::Map(vec![("data", Mp::Nil)]))]),
        Mp::Map(vec![order(), (VARIANT, Mp::Nil)]),
        Mp::Map(vec![(VARIANT, Mp::Nil), order()]),
        Mp::Map(vec![order(), ("value", Mp::Map(Vec::new()))]),
        Mp::Map(vec![order()]),
    ];
    let expected = vec!["nil", "nil", "nil", "-", "-"];
    assert_eq!(
        stored(&config, &fields, 1, json, &events),
        [expected.clone(), expected],
        "[JSON, MessagePack]: nested, flat with type first and last, then absent"
    );
}

/// A subtype's column that no `(event type, member)` reaches, spelled only as
/// a flat key, is dynamic when the owner names the column; a Binary column it
/// does not name keeps null as the empty cell.
#[test]
fn a_named_dynamic_columns_null_is_data_and_another_columns_is_empty() {
    const PROBE: &str = "value.$variant.undefinedCell.%24ok.probe.value";
    let fields = [utf8(), binary(), binary()];
    let config = build_extraction_config_with_semantic_schemas(
        &fields,
        &["type", PROBE, "value.raw"],
        &[],
        None,
        None,
        CellLayout {
            dynamic_columns: &[1],
            ..CellLayout::default()
        },
    )
    .unwrap();
    let json = br#"[
        {"type":"$ok","value.$variant.undefinedCell.%24ok.probe.value":null,"value.raw":null},
        {"type":"$ok"}
    ]"#;
    let ok = || ("type", Mp::Str("$ok"));
    let events = [
        Mp::Map(vec![ok(), (PROBE, Mp::Nil), ("value.raw", Mp::Nil)]),
        Mp::Map(vec![ok()]),
    ];
    assert_eq!(
        stored(&config, &fields, 1, json, &events),
        [vec!["nil", "-"], vec!["nil", "-"]],
        "the named column: a present null, then absent"
    );
    assert_eq!(
        stored(&config, &fields, 2, json, &events),
        [vec!["-", "-"], vec!["-", "-"]],
        "a column not named dynamic"
    );
}

#[test]
fn creation_refuses_a_dynamic_column_that_is_not_binary_or_not_in_the_schema() {
    let fields = [utf8(), binary()];
    for column in [0, 2] {
        assert_eq!(
            build_extraction_config_with_semantic_schemas(
                &fields,
                &["type", "value.data"],
                &[],
                None,
                None,
                CellLayout {
                    dynamic_columns: &[column],
                    ..CellLayout::default()
                },
            )
            .err(),
            Some(ConfigError::InvalidDynamicColumn),
            "column {column}"
        );
    }
}
//#endregion smoo/columine!n/vocab-dynamic-null
