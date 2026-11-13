//! Cross-format differential oracle for typed extraction.
//!
//! One logical document must extract to identical typed cells whether it
//! arrives as JSON or as MessagePack. The coercion table (specs/columine/10-physical-vocabulary.md §8) is a
//! single contract; the two extractors are two instantiations of it over two
//! wire vocabularies, not two policies. [`coerce`] is that table written once,
//! and the properties below hold both extractors to it.
//!
//! The MessagePack document is produced by the crate's own canonical encoder
//! ([`MsgpackValueWriter`]) fed from the JSON document, so the two inputs are
//! the same logical value by construction rather than by a second hand-written
//! encoder that could drift with the thing it tests.

use crate::instant::parse_instant;
use crate::json_extractor::{
    ExtractionDiagnostic, ExtractionError, MsgpackValueWriter, extract_json_events,
};
use crate::json_parser::JsonParser;
use crate::msgpack_extractor::extract_msgpack_events;
use crate::{
    ArrowType, ColumnValue, DynamicColumns, SignalSchemaField,
    build_extraction_config_with_semantic_schemas, read_cell,
};
use columine_arrow::{DictionarySlot, IndexWidth, SchemaDictionary, TIME_UNITS, TimeUnit};
use columine_types::category::{CategoryImage, CategoryRefusal, ListImage, NoCategories};
use proptest::prelude::*;

/// A scalar as the ingest contract sees it, independent of wire encoding.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Logical {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
}

impl Logical {
    /// The JSON literal for this value. The text strategies exclude `"` and
    /// `\` so no escaping is needed and the JSON bytes stay a contiguous run.
    fn json(&self) -> String {
        match self {
            Self::Null => "null".to_owned(),
            Self::Bool(true) => "true".to_owned(),
            Self::Bool(false) => "false".to_owned(),
            Self::Int(value) => value.to_string(),
            // `{:?}` is the shortest round-tripping form, and it keeps a
            // decimal point on integral floats so the value stays a float on
            // both wires.
            Self::Float(value) => format!("{value:?}"),
            Self::Text(text) => format!("\"{text}\""),
        }
    }
}

/// The one column a differential document fills.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Column {
    Plain(ArrowType),
    Timestamp(TimeUnit),
    Duration(TimeUnit),
    /// Encoded against [`SCHEMA_VALUES`].
    SchemaDictionary,
    /// Encoded against the stream dictionary.
    StreamDictionary,
}

impl From<ArrowType> for Column {
    fn from(kind: ArrowType) -> Self {
        Self::Plain(kind)
    }
}

impl Column {
    fn field(self) -> SignalSchemaField {
        match self {
            Self::Plain(kind) => SignalSchemaField::new(kind, true),
            Self::Timestamp(unit) => SignalSchemaField::new_timestamp(unit, true),
            Self::Duration(unit) => SignalSchemaField::new_duration(unit, true),
            Self::SchemaDictionary => {
                SignalSchemaField::new_dictionary(DictionarySlot::Schema(0), IndexWidth::U8, true)
            }
            Self::StreamDictionary => {
                SignalSchemaField::new_dictionary(DictionarySlot::Stream, IndexWidth::U16, true)
            }
        }
    }
}

/// The schema dictionary of [`Column::SchemaDictionary`]; index 1 is skipped.
const SCHEMA_VALUES: [Option<&str>; 3] = [Some("alpha"), None, Some("beta")];

fn dictionaries() -> Vec<SchemaDictionary> {
    vec![SchemaDictionary {
        slot: DictionarySlot::Schema(0),
        arrow_ids: vec![0],
        values: SCHEMA_VALUES
            .iter()
            .map(|value| value.map(str::to_owned))
            .collect(),
    }]
}

/// An integral float inside `i64`, which an integer plane takes on either wire.
fn integral(value: f64) -> Result<i64, ()> {
    let bound = 9_223_372_036_854_775_808.0_f64;
    if value.fract() != 0.0 || !value.is_finite() || value >= bound || value < -bound {
        return Err(());
    }
    #[allow(
        clippy::cast_possible_truncation,
        reason = "integral and range-checked against i64 just above"
    )]
    Ok(value as i64)
}

/// The coercion table: what a logical value must become in a column. `Err`
/// means the value must be refused as [`ExtractionError::InvalidFieldType`].
///
/// Rules, one place:
/// - `Null` is absent in every column.
/// - `Utf8` takes strings only.
/// - `Int32` takes integers in `i32` range only, an integral float included.
/// - `Int64` and `Duration` take integers, and a decimal `i64` string
///   (bigint-as-string, which JSON cannot represent losslessly).
/// - `Timestamp` takes what `Int64` takes, and an RFC 3339 instant no
///   finer than its unit, converted to the unit.
/// - A schema-dictionary column takes one of its values' texts, stored as its
///   index; a stream-dictionary column takes any text, stored as the ordinal
///   the image answers (a fresh image answers 0).
/// - `Float64` takes integers and floats.
/// - `Bool` takes booleans only.
/// - `Binary` takes anything and stores its canonical MessagePack bytes.
/// - `Null` columns consume the value and store nothing.
pub(crate) fn coerce(column: Column, value: &Logical) -> Result<Option<ColumnValue<'_>>, ()> {
    if column == Column::Plain(ArrowType::Null) || matches!(value, Logical::Null) {
        return Ok(None);
    }
    let int = |value: i64| Ok(Some(ColumnValue::Int(value)));
    match (column, value) {
        (Column::Plain(ArrowType::Utf8), Logical::Text(text)) => {
            Ok(Some(ColumnValue::Utf8(text.clone().into())))
        }
        (Column::Plain(ArrowType::Int32), Logical::Int(value)) => i32::try_from(*value)
            .map(|narrow| Some(ColumnValue::Int(i64::from(narrow))))
            .map_err(|_| ()),
        (Column::Plain(ArrowType::Int32), Logical::Float(value)) => {
            i32::try_from(integral(*value)?)
                .map(|narrow| Some(ColumnValue::Int(i64::from(narrow))))
                .map_err(|_| ())
        }
        (
            Column::Plain(ArrowType::Int64) | Column::Timestamp(_) | Column::Duration(_),
            Logical::Int(value),
        ) => int(*value),
        // An integral number is an integer on either wire: standard
        // MessagePack spells a JavaScript number above u32 as float64 (the
        // 64-bit integer markers are reserved for BigInt), and JSON has one
        // number token. Only a fraction or a value outside the plane refuses.
        (
            Column::Plain(ArrowType::Int64) | Column::Timestamp(_) | Column::Duration(_),
            Logical::Float(value),
        ) => int(integral(*value)?),
        (Column::Plain(ArrowType::Int64) | Column::Duration(_), Logical::Text(text)) => {
            int(text.parse::<i64>().map_err(|_| ())?)
        }
        (Column::Timestamp(unit), Logical::Text(text)) => int(text
            .parse::<i64>()
            .or_else(|_| parse_instant(text, unit))
            .map_err(|_| ())?),
        (Column::SchemaDictionary, Logical::Text(text)) => SCHEMA_VALUES
            .iter()
            .position(|known| *known == Some(text.as_str()))
            .and_then(|index| u64::try_from(index).ok())
            .map(|index| Some(ColumnValue::UInt(index)))
            .ok_or(()),
        (Column::StreamDictionary, Logical::Text(_)) => Ok(Some(ColumnValue::UInt(0))),
        (Column::Plain(ArrowType::Float64), Logical::Int(value)) => {
            Ok(Some(ColumnValue::Float(*value as f64)))
        }
        (Column::Plain(ArrowType::Float64), Logical::Float(value)) => {
            Ok(Some(ColumnValue::Float(*value)))
        }
        (Column::Plain(ArrowType::Bool), Logical::Bool(value)) => {
            Ok(Some(ColumnValue::Bool(*value)))
        }
        (Column::Plain(ArrowType::Binary), value) => Ok(Some(ColumnValue::Binary(
            canonical_msgpack(&value.json()).into(),
        ))),
        _ => Err(()),
    }
}

/// Canonical MessagePack bytes for one JSON value, produced by the crate's
/// own encoder. This is the SSOT for "the same logical value on the other
/// wire": the undeclared carrier and `Binary` encoders already use it in production.
pub(crate) fn canonical_msgpack(json: &str) -> Vec<u8> {
    let mut buffer = vec![0_u8; 1 << 16];
    let written = {
        let mut writer =
            MsgpackValueWriter::new(&mut buffer).expect("64 KiB is above the 5-byte floor");
        let mut parser = JsonParser::new(json.as_bytes());
        let token = parser.next_token().expect("generated JSON is well formed");
        writer
            .write_value(&mut parser, token)
            .expect("64 KiB holds a generated value");
        writer.offset()
    };
    buffer.truncate(written);
    buffer
}

/// Extract a single-field document through the JSON pipeline.
fn json_with(
    column: Column,
    literal: &str,
    image: &mut dyn CategoryImage,
) -> Result<Option<ColumnValue<'static>>, ExtractionError> {
    let document = format!("[{{\"v\":{literal}}}]");
    let fields = [column.field()];
    let config = build_extraction_config_with_semantic_schemas(
        &fields,
        &["v"],
        &dictionaries(),
        None,
        None,
        crate::CellLayout::default(),
    )
    .expect("single-field schema is valid");
    let mut columns = DynamicColumns::new(&fields, 4).expect("column plane fits");
    let mut work = [0_u8; 1 << 12];
    let mut diagnostic = ExtractionDiagnostic::default();
    extract_json_events(
        document.as_bytes(),
        &config,
        &mut columns,
        &mut work,
        &mut diagnostic,
        image,
        &mut crate::CellWriter::Native,
        &mut Vec::new(),
    )?;
    Ok(read_cell(&columns, 0, 0))
}

/// Extract the canonical MessagePack encoding of the same document.
fn msgpack_with(
    column: Column,
    literal: &str,
    image: &mut dyn CategoryImage,
) -> Result<Option<ColumnValue<'static>>, ExtractionError> {
    let document = canonical_msgpack(&format!("[{{\"v\":{literal}}}]"));
    let fields = [column.field()];
    let config = build_extraction_config_with_semantic_schemas(
        &fields,
        &["v"],
        &dictionaries(),
        None,
        None,
        crate::CellLayout::default(),
    )
    .expect("single-field schema is valid");
    let mut columns = DynamicColumns::new(&fields, 4).expect("column plane fits");
    let mut work = [0_u8; 1 << 12];
    extract_msgpack_events(&document, &config, &mut columns, &mut work, false, image)?;
    Ok(read_cell(&columns, 0, 0))
}

/// [`json_with`] against a fresh stream image.
fn via_json(
    column: impl Into<Column>,
    literal: &str,
) -> Result<Option<ColumnValue<'static>>, ExtractionError> {
    json_with(column.into(), literal, &mut ListImage::new(16, 64))
}

/// [`msgpack_with`] against a fresh stream image.
fn via_msgpack(
    column: impl Into<Column>,
    literal: &str,
) -> Result<Option<ColumnValue<'static>>, ExtractionError> {
    msgpack_with(column.into(), literal, &mut ListImage::new(16, 64))
}

fn column() -> impl Strategy<Value = Column> {
    let unit = || proptest::sample::select(TIME_UNITS.to_vec());
    prop_oneof![
        7 => prop_oneof![
            Just(ArrowType::Utf8),
            Just(ArrowType::Int32),
            Just(ArrowType::Int64),
            Just(ArrowType::Float64),
            Just(ArrowType::Bool),
            Just(ArrowType::Binary),
            Just(ArrowType::Null),
        ]
        .prop_map(Column::Plain),
        2 => unit().prop_map(Column::Timestamp),
        1 => unit().prop_map(Column::Duration),
        1 => Just(Column::SchemaDictionary),
        1 => Just(Column::StreamDictionary),
    ]
}

fn logical() -> impl Strategy<Value = Logical> {
    prop_oneof![
        2 => Just(Logical::Null),
        3 => any::<bool>().prop_map(Logical::Bool),
        6 => prop_oneof![
            -1_000_000_i64..1_000_000,
            // Straddles i32 range so the Int32 narrowing rule is exercised
            // from both sides.
            i64::from(i32::MIN) - 4..i64::from(i32::MIN) + 4,
            i64::from(i32::MAX) - 4..i64::from(i32::MAX) + 4,
            any::<i64>(),
        ]
        .prop_map(Logical::Int),
        4 => prop_oneof![
            (-1e9_f64..1e9).prop_map(Logical::Float),
            Just(Logical::Float(0.5)),
            Just(Logical::Float(-0.5)),
            Just(Logical::Float(1.0)),
        ],
        10 => prop_oneof![
            "[a-zA-Z0-9 _.:+-]{0,24}".prop_map(Logical::Text),
            (-1_000_000_i64..1_000_000).prop_map(|value| Logical::Text(value.to_string())),
            // Fractions of zero to nine digits, so an instant finer than the
            // column's unit is met as often as one that fits it.
            (
                (1970_u32..2100, 1_u32..13, 1_u32..29),
                (0_u32..24, 0_u32..60, 0_u32..60),
                "([0-9]{1,9})?",
            )
                .prop_map(|((year, month, day), (hour, minute, second), fraction)| {
                    let fraction = if fraction.is_empty() {
                        String::new()
                    } else {
                        format!(".{fraction}")
                    };
                    Logical::Text(format!(
                        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}{fraction}Z"
                    ))
                }),
            Just(Logical::Text("alpha".to_owned())),
            Just(Logical::Text("beta".to_owned())),
        ],
    ]
}

//#region smoo/columine!n/vocab-lanes-agree
proptest! {
    #![proptest_config(ProptestConfig::with_cases(2048))]

    /// The oracle: one logical value, two wire encodings, one answer. A
    /// divergence here is a live spec bug in whichever extractor disagrees
    /// with [`coerce`].
    #[test]
    fn json_and_msgpack_extract_identically(column in column(), value in logical()) {
        let literal = value.json();
        let json = via_json(column, &literal);
        let msgpack = via_msgpack(column, &literal);
        let table = coerce(column, &value);

        match table {
            Ok(expected) => {
                prop_assert_eq!(
                    json.as_ref().map_err(|_| ()),
                    Ok(&expected),
                    "JSON rejected or mis-coerced {:?} in a {:?} column (literal {})",
                    value, column, literal
                );
                prop_assert_eq!(
                    msgpack.as_ref().map_err(|_| ()),
                    Ok(&expected),
                    "MessagePack rejected or mis-coerced {:?} in a {:?} column (literal {})",
                    value, column, literal
                );
            }
            Err(()) => {
                prop_assert_eq!(
                    json.as_ref().err(),
                    Some(&ExtractionError::InvalidFieldType),
                    "JSON accepted {:?} in a {:?} column (literal {})",
                    value, column, literal
                );
                prop_assert_eq!(
                    msgpack.as_ref().err(),
                    Some(&ExtractionError::InvalidFieldType),
                    "MessagePack accepted {:?} in a {:?} column (literal {})",
                    value, column, literal
                );
            }
        }
    }
}

#[cfg(test)]
mod pins {
    use super::*;

    /// One literal into one column, through both lanes: both answer `expected`.
    fn both(
        column: impl Into<Column>,
        literal: &str,
        expected: Result<Option<ColumnValue<'static>>, ExtractionError>,
    ) {
        let column = column.into();
        assert_eq!(
            via_json(column, literal),
            expected,
            "JSON {literal} into {column:?}"
        );
        assert_eq!(
            via_msgpack(column, literal),
            expected,
            "MessagePack {literal} into {column:?}"
        );
    }

    /// The four coercions that diverged between the pipelines, pinned as
    /// explicit cases so a regression names itself instead of surfacing as a
    /// proptest shrink.
    #[test]
    fn diverged_coercions_now_agree() {
        // Int32 takes no floats.
        both(
            ArrowType::Int32,
            "1.5",
            Err(ExtractionError::InvalidFieldType),
        );
        // Int32 takes no instant strings.
        both(
            ArrowType::Int32,
            "\"2024-01-15T10:30:00Z\"",
            Err(ExtractionError::InvalidFieldType),
        );
        // Int32 refuses an integer outside i32 range on both wires.
        both(
            ArrowType::Int32,
            "3000000000",
            Err(ExtractionError::InvalidFieldType),
        );
        // Int64 takes bigint-as-string on both wires.
        both(
            ArrowType::Int64,
            "\"12345\"",
            Ok(Some(ColumnValue::Int(12345))),
        );
        // Int64 takes no floats.
        both(
            ArrowType::Int64,
            "1.5",
            Err(ExtractionError::InvalidFieldType),
        );
    }

    //#region smoo/columine!n/vocab-temporal-instant
    /// 2024-01-15T10:30:00Z in seconds since the epoch.
    const BASE_SECONDS: i64 = 19_737 * 86_400 + 37_800;

    #[test]
    fn an_instant_is_stored_in_the_timestamp_unit_on_both_lanes() {
        let instant = "\"2024-01-15T10:30:00.123Z\"";
        both(
            Column::Timestamp(TimeUnit::Nanosecond),
            instant,
            Ok(Some(ColumnValue::Int(
                BASE_SECONDS * 1_000_000_000 + 123_000_000,
            ))),
        );
        both(
            Column::Timestamp(TimeUnit::Millisecond),
            instant,
            Ok(Some(ColumnValue::Int(BASE_SECONDS * 1_000 + 123))),
        );
        both(
            Column::Timestamp(TimeUnit::Second),
            "\"2024-01-15T10:30:00Z\"",
            Ok(Some(ColumnValue::Int(BASE_SECONDS))),
        );
    }

    #[test]
    fn an_instant_finer_than_the_unit_or_into_int64_or_duration_is_refused() {
        both(
            Column::Timestamp(TimeUnit::Millisecond),
            "\"2024-01-15T10:30:00.123456789Z\"",
            Err(ExtractionError::InvalidFieldType),
        );
        both(
            ArrowType::Int64,
            "\"2024-01-15T10:30:00Z\"",
            Err(ExtractionError::InvalidFieldType),
        );
        both(
            Column::Duration(TimeUnit::Nanosecond),
            "\"2024-01-15T10:30:00Z\"",
            Err(ExtractionError::InvalidFieldType),
        );
    }
    //#endregion smoo/columine!n/vocab-temporal-instant

    #[test]
    fn a_duration_takes_an_integer_and_a_decimal_string() {
        both(
            Column::Duration(TimeUnit::Microsecond),
            "-42",
            Ok(Some(ColumnValue::Int(-42))),
        );
        both(
            Column::Duration(TimeUnit::Second),
            "\"9007199254740993\"",
            Ok(Some(ColumnValue::Int(9_007_199_254_740_993))),
        );
    }

    //#region smoo/columine!n/vocab-enum-dictionary
    #[test]
    fn a_schema_dictionary_text_stores_its_index_and_another_value_is_refused() {
        both(
            Column::SchemaDictionary,
            "\"beta\"",
            Ok(Some(ColumnValue::UInt(2))),
        );
        both(Column::SchemaDictionary, "null", Ok(None));
        both(
            Column::SchemaDictionary,
            "\"gamma\"",
            Err(ExtractionError::InvalidFieldType),
        );
        both(
            Column::SchemaDictionary,
            "0",
            Err(ExtractionError::InvalidFieldType),
        );
    }
    //#endregion smoo/columine!n/vocab-enum-dictionary

    //#region smoo/columine!n/vocab-image-limit
    #[test]
    fn a_stream_dictionary_text_the_image_refuses_is_a_category_refusal() {
        let refused = Err(ExtractionError::Category(CategoryRefusal::Full {
            limit: 0,
        }));
        assert_eq!(
            json_with(Column::StreamDictionary, "\"open\"", &mut NoCategories),
            refused
        );
        assert_eq!(
            msgpack_with(Column::StreamDictionary, "\"open\"", &mut NoCategories),
            refused
        );
        both(
            Column::StreamDictionary,
            "true",
            Err(ExtractionError::InvalidFieldType),
        );
    }
    //#endregion smoo/columine!n/vocab-image-limit

    /// A negative `Int32` cell must read back negative. The physical plane is
    /// four bytes wide; reading it as unsigned turns `-1` into `4294967295`.
    #[test]
    fn negative_int32_round_trips_signed() {
        both(ArrowType::Int32, "-1", Ok(Some(ColumnValue::Int(-1))));
        assert_eq!(
            via_json(ArrowType::Int32, "-2147483648"),
            Ok(Some(ColumnValue::Int(i64::from(i32::MIN))))
        );
    }
}
//#endregion smoo/columine!n/vocab-lanes-agree
