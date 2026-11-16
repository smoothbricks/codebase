//! A member's enum is a subset of its column's dictionary
//! (specs/columine/10-physical-vocabulary.md §7 "A member's enum is a subset
//! of its column's dictionary"): members and arms narrow the one dictionary
//! of a column, and an admitted value is stored as the dictionary's index.

use columine_arrow::{DictionarySlot, IndexWidth, SchemaDictionary};
use columine_parsing::json_extractor::{
    ExtractionDiagnostic, ExtractionError, extract_json_events,
};
use columine_parsing::msgpack_extractor::extract_msgpack_events;
use columine_parsing::validate::{
    BatchValidationError, NoAddressGrammar, PreparedSchemas, UnavailableVoAdmission,
    parse_schema_envelope,
};
use columine_parsing::{
    ArrowType, CellLayout, CellWriter, ColumnValue, ConfigError, DiscriminatedMember,
    DynamicColumns, ExtractionConfig, PayloadDiscriminator, SignalSchemaField,
    build_extraction_config_with_semantic_schemas, read_cell,
};
use columine_types::category::NoCategories;

//#region smoo/columine!n/vocab-enum-subset
const NAMES: [&str; 6] = [
    "type",
    "value.outcome",
    "value.snapshot.status",
    "value.snapshot.at",
    "value.snapshot.why",
    "value.label",
];
const OUTCOME: usize = 1;
const STATUS: usize = 2;
const AT: usize = 3;
const WHY: usize = 4;
const LABEL: usize = 5;

/// The one dictionary the `outcome` and `status` columns share.
const DICTIONARY: [&str; 5] = ["ok", "pending", "refused", "interrupted", "ready"];

fn fields() -> [SignalSchemaField; 6] {
    let utf8 = SignalSchemaField::new(ArrowType::Utf8, true);
    let encoded =
        SignalSchemaField::new_dictionary(DictionarySlot::Schema(0), IndexWidth::U8, true);
    let int64 = SignalSchemaField::new(ArrowType::Int64, true);
    [utf8, encoded, encoded, int64, utf8, utf8]
}

/// A `check` event: an `outcome` of the enum `outcome`, and a `snapshot`
/// that is one of two arms, each with a `status` of its own enum.
fn envelope(outcome: &str, ready: &str, refused: &str) -> String {
    let variants = |list: &str| format!(r#"{{"kind":"enum","variants":[{list}]}}"#);
    format!(
        r#"{{"check":{{"kind":"object","fields":{{"outcome":{},"snapshot":{{"kind":"union","variants":[{{"kind":"object","fields":{{"status":{},"at":{{"kind":"number"}}}}}},{{"kind":"object","fields":{{"status":{},"why":{{"kind":"string"}}}}}}]}}}}}}}}"#,
        variants(outcome),
        variants(ready),
        variants(refused),
    )
}

/// The members narrowed: `outcome` to two variants in another order than the
/// dictionary's, each arm's `status` to one.
fn narrowed() -> String {
    envelope(r#""pending","ok""#, r#""ready""#, r#""refused""#)
}

fn row(member: &str, path: &[&str], column: usize) -> DiscriminatedMember {
    DiscriminatedMember {
        discriminant: "check".into(),
        member: member.into(),
        path: path.iter().map(|name| (*name).to_owned()).collect(),
        column,
    }
}

/// A processor of `check` events with `status` in `status_column`.
fn create(envelope: &str, status_column: usize) -> Result<ExtractionConfig, ConfigError> {
    let schemas = parse_schema_envelope(envelope.as_bytes()).unwrap();
    build_extraction_config_with_semantic_schemas(
        &fields(),
        &NAMES,
        &[SchemaDictionary {
            slot: DictionarySlot::Schema(0),
            arrow_ids: vec![0],
            values: DICTIONARY.map(|value| Some(value.to_owned())).to_vec(),
        }],
        Some(&schemas),
        Some(&PayloadDiscriminator {
            key: "type".into(),
            members: vec![
                row("outcome", &[], OUTCOME),
                row("snapshot", &["status"], status_column),
                row("snapshot", &["at"], AT),
                row("snapshot", &["why"], WHY),
            ],
        }),
        CellLayout::default(),
    )
}

/// The ready arm at 7 with outcome `pending`, then the refused arm with
/// outcome `ok`, as MessagePack.
fn msgpack_events() -> Vec<u8> {
    fn text(out: &mut Vec<u8>, text: &str) {
        out.push(
            0xa0 | u8::try_from(text.len())
                .ok()
                .filter(|len| *len < 32)
                .unwrap(),
        );
        out.extend(text.as_bytes());
    }
    let mut out = vec![0x92];
    for (outcome, status, leaf) in [("pending", "ready", None), ("ok", "refused", Some("late"))] {
        out.push(0x82);
        text(&mut out, "type");
        text(&mut out, "check");
        text(&mut out, "value");
        out.push(0x82);
        text(&mut out, "outcome");
        text(&mut out, outcome);
        text(&mut out, "snapshot");
        out.push(0x82);
        text(&mut out, "status");
        text(&mut out, status);
        match leaf {
            None => {
                text(&mut out, "at");
                out.push(7);
            }
            Some(why) => {
                text(&mut out, "why");
                text(&mut out, why);
            }
        }
    }
    out
}

const JSON_EVENTS: &[u8] = br#"[
    {"type":"check","value":{"outcome":"pending","snapshot":{"status":"ready","at":7}}},
    {"type":"check","value":{"outcome":"ok","snapshot":{"status":"refused","why":"late"}}}
]"#;

/// The `outcome` and `status` cells of every row a lane stores: each the
/// index of its text in the column's dictionary.
fn indexes(
    lane: &str,
    config: &ExtractionConfig,
) -> Result<Vec<[Option<u64>; 2]>, ExtractionError> {
    let fields = fields();
    let mut columns = DynamicColumns::new(&fields, 8).expect("column plane fits");
    let mut work = [0; 256];
    let rows = match lane {
        "json" => extract_json_events(
            JSON_EVENTS,
            config,
            &mut columns,
            &mut work,
            &mut ExtractionDiagnostic::default(),
            &mut NoCategories,
            &mut CellWriter::Native,
            &mut Vec::new(),
        ),
        _ => extract_msgpack_events(
            &msgpack_events(),
            config,
            &mut columns,
            &mut work,
            false,
            &mut NoCategories,
        ),
    }?;
    Ok((0..rows)
        .map(|row| {
            [OUTCOME, STATUS].map(|column| match read_cell(&columns, column, row) {
                None => None,
                Some(ColumnValue::UInt(index)) => Some(index),
                Some(other) => panic!("row {row}, column {column}: {other:?}"),
            })
        })
        .collect())
}

#[test]
fn members_and_arms_that_narrow_their_columns_dictionary_store_its_index() {
    let config = create(&narrowed(), STATUS)
        .expect("each member's enum is a subset of its column's dictionary");
    for lane in ["json", "msgpack"] {
        assert_eq!(
            indexes(lane, &config),
            Ok(vec![[Some(1), Some(4)], [Some(0), Some(2)]]),
            "{lane}: pending and ready, then ok and refused, by the dictionary's order"
        );
    }
}

#[test]
fn a_value_of_the_dictionary_that_the_member_does_not_admit_is_refused_at_the_member() {
    let schemas = parse_schema_envelope(narrowed().as_bytes()).unwrap();
    let prepared =
        PreparedSchemas::<_, NoAddressGrammar>::prepare(schemas, UnavailableVoAdmission).unwrap();
    assert_eq!(prepared.validate_json_batch(JSON_EVENTS), Ok(()));
    for (event, why) in [
        (
            r#"{"outcome":"refused","snapshot":{"status":"ready","at":7}}"#,
            "`outcome` admits no `refused`",
        ),
        (
            r#"{"outcome":"ok","snapshot":{"status":"pending","at":7}}"#,
            "no arm's `status` admits `pending`",
        ),
        (
            r#"{"outcome":"ok","snapshot":{"status":"refused","at":7}}"#,
            "`refused` is the other arm's",
        ),
    ] {
        let batch = format!(r#"[{{"type":"check","value":{event}}}]"#);
        assert!(
            matches!(
                prepared.validate_json_batch(batch.as_bytes()),
                Err(BatchValidationError::Violation { event_index: 0, .. })
            ),
            "{why}"
        );
    }
}

#[test]
fn creation_refuses_an_enum_its_columns_dictionary_does_not_hold_by_name() {
    let mismatch = |path: &str, column, value: Option<&str>| {
        Some(ConfigError::EnumDictionaryMismatch {
            event_type: "check".into(),
            path: path.into(),
            column,
            value: value.map(str::to_owned),
        })
    };
    assert_eq!(
        create(
            &envelope(r#""ok","lost""#, r#""ready""#, r#""refused""#),
            STATUS
        )
        .err(),
        mismatch("value.outcome", OUTCOME, Some("lost")),
        "a member a row places whole"
    );
    assert_eq!(
        create(&envelope(r#""ok""#, r#""ready""#, r#""gone""#), STATUS).err(),
        mismatch("value.snapshot.status", STATUS, Some("gone")),
        "an arm's member a path row places"
    );
    assert_eq!(
        create(&narrowed(), LABEL).err(),
        mismatch("value.snapshot.status", LABEL, None),
        "a column that is no schema-dictionary column"
    );
}
//#endregion smoo/columine!n/vocab-enum-subset
