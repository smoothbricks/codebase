//! Cross-format differential oracle for typed extraction.
//!
//! One logical document must extract to identical typed cells whether it
//! arrives as JSON or as MessagePack. The Arrow-type coercion table is a
//! single contract; the two extractors are two instantiations of it over two
//! wire vocabularies, not two policies. [`coerce`] is that table written once,
//! and the properties below hold both extractors to it.
//!
//! The MessagePack document is produced by the crate's own canonical encoder
//! ([`MsgpackValueWriter`]) fed from the JSON document, so the two inputs are
//! the same logical value by construction rather than by a second hand-written
//! encoder that could drift with the thing it tests.

use crate::json_extractor::{
    ExtractionDiagnostic, ExtractionError, MsgpackValueWriter, extract_json_events,
};
use crate::json_parser::JsonParser;
use crate::json_scanner::parse_iso8601_to_micros;
use crate::msgpack_extractor::extract_msgpack_events;
use crate::{
    ArrowType, ColumnValue, DynamicColumns, SignalSchemaField, build_extraction_config, read_cell,
};
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

/// The Arrow-type coercion table: what a logical value must become in a column
/// of the given type. `Err` means the value must be rejected as
/// [`ExtractionError::InvalidFieldType`].
///
/// Rules, one place:
/// - `Null` is absent in every column type.
/// - `Utf8` takes strings only.
/// - `Int32` takes integers in `i32` range only. No floats, no strings: a
///   32-bit column is not a timestamp column and truncating a float loses the
///   value the producer sent.
/// - `Int64` takes integers, and strings as either a decimal `i64`
///   (bigint-as-string, which JSON cannot represent losslessly) or an ISO-8601
///   instant. No floats, for the same reason as `Int32`.
/// - `Float64` takes integers and floats.
/// - `Bool` takes booleans only.
/// - `Binary` takes anything and stores its canonical MessagePack bytes.
/// - `Null` columns consume the value and store nothing.
pub(crate) fn coerce(kind: ArrowType, value: &Logical) -> Result<Option<ColumnValue>, ()> {
    if kind == ArrowType::Null {
        return Ok(None);
    }
    if matches!(value, Logical::Null) {
        return Ok(None);
    }
    match (kind, value) {
        (ArrowType::Utf8, Logical::Text(text)) => Ok(Some(ColumnValue::Utf8(text.clone()))),
        (ArrowType::Int32, Logical::Int(value)) => i32::try_from(*value)
            .map(|narrow| Some(ColumnValue::Int(i64::from(narrow))))
            .map_err(|_| ()),
        (ArrowType::Int64, Logical::Int(value)) => Ok(Some(ColumnValue::Int(*value))),
        // An integral number is an integer on either wire: standard
        // MessagePack spells a JavaScript number above u32 as float64 (the
        // 64-bit integer markers are reserved for BigInt), and JSON has one
        // number token. Only a fraction or a value outside the plane refuses.
        (ArrowType::Int32 | ArrowType::Int64, Logical::Float(value)) => {
            let integral = value.fract() == 0.0 && value.is_finite();
            let bound = 9_223_372_036_854_775_808.0_f64;
            if !integral || *value >= bound || *value < -bound {
                return Err(());
            }
            #[allow(
                clippy::cast_possible_truncation,
                reason = "integral and range-checked against i64 just above"
            )]
            let wide = *value as i64;
            if kind == ArrowType::Int32 {
                i32::try_from(wide)
                    .map(|narrow| Some(ColumnValue::Int(i64::from(narrow))))
                    .map_err(|_| ())
            } else {
                Ok(Some(ColumnValue::Int(wide)))
            }
        }
        (ArrowType::Int64, Logical::Text(text)) => text
            .parse::<i64>()
            .or_else(|_| parse_iso8601_to_micros(text).map_err(|_| ()))
            .map(|micros| Some(ColumnValue::Int(micros)))
            .map_err(|_| ()),
        (ArrowType::Float64, Logical::Int(value)) => Ok(Some(ColumnValue::Float(*value as f64))),
        (ArrowType::Float64, Logical::Float(value)) => Ok(Some(ColumnValue::Float(*value))),
        (ArrowType::Bool, Logical::Bool(value)) => Ok(Some(ColumnValue::Bool(*value))),
        (ArrowType::Binary, value) => {
            Ok(Some(ColumnValue::Binary(canonical_msgpack(&value.json()))))
        }
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

fn schema(kind: ArrowType) -> [SignalSchemaField; 1] {
    [SignalSchemaField::new(kind, true)]
}

/// Extract a single-field document through the JSON pipeline.
fn via_json(kind: ArrowType, literal: &str) -> Result<Option<ColumnValue>, ExtractionError> {
    let document = format!("[{{\"v\":{literal}}}]");
    let fields = schema(kind);
    let config = build_extraction_config(&fields, &["v"]).expect("single-field schema is valid");
    let mut columns = DynamicColumns::new(&fields, 4);
    let mut work = [0_u8; 1 << 12];
    let mut diagnostic = ExtractionDiagnostic::default();
    extract_json_events(
        document.as_bytes(),
        &config,
        &mut columns,
        &mut work,
        &mut diagnostic,
    )?;
    Ok(read_cell(&columns, 0, 0))
}

/// Extract the canonical MessagePack encoding of the same document.
fn via_msgpack(kind: ArrowType, literal: &str) -> Result<Option<ColumnValue>, ExtractionError> {
    let document = canonical_msgpack(&format!("[{{\"v\":{literal}}}]"));
    let fields = schema(kind);
    let config = build_extraction_config(&fields, &["v"]).expect("single-field schema is valid");
    let mut columns = DynamicColumns::new(&fields, 4);
    let mut work = [0_u8; 1 << 12];
    extract_msgpack_events(&document, &config, &mut columns, &mut work, false)?;
    Ok(read_cell(&columns, 0, 0))
}

fn arrow_type() -> impl Strategy<Value = ArrowType> {
    prop_oneof![
        Just(ArrowType::Utf8),
        Just(ArrowType::Int32),
        Just(ArrowType::Int64),
        Just(ArrowType::Float64),
        Just(ArrowType::Bool),
        Just(ArrowType::Binary),
        Just(ArrowType::Null),
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
        8 => prop_oneof![
            "[a-zA-Z0-9 _.:+-]{0,24}".prop_map(Logical::Text),
            (-1_000_000_i64..1_000_000).prop_map(|value| Logical::Text(value.to_string())),
            (1970_u32..2100, 1_u32..13, 1_u32..29, 0_u32..24, 0_u32..60, 0_u32..60).prop_map(
                |(year, month, day, hour, minute, second)| Logical::Text(format!(
                    "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z"
                ))
            ),
        ],
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2048))]

    /// The oracle: one logical value, two wire encodings, one answer. A
    /// divergence here is a live spec bug in whichever extractor disagrees
    /// with [`coerce`].
    #[test]
    fn json_and_msgpack_extract_identically(kind in arrow_type(), value in logical()) {
        let literal = value.json();
        let json = via_json(kind, &literal);
        let msgpack = via_msgpack(kind, &literal);
        let table = coerce(kind, &value);

        match table {
            Ok(expected) => {
                prop_assert_eq!(
                    json.as_ref().map_err(|_| ()),
                    Ok(&expected),
                    "JSON rejected or mis-coerced {:?} in a {:?} column (literal {})",
                    value, kind, literal
                );
                prop_assert_eq!(
                    msgpack.as_ref().map_err(|_| ()),
                    Ok(&expected),
                    "MessagePack rejected or mis-coerced {:?} in a {:?} column (literal {})",
                    value, kind, literal
                );
            }
            Err(()) => {
                prop_assert_eq!(
                    json.as_ref().err(),
                    Some(&ExtractionError::InvalidFieldType),
                    "JSON accepted {:?} in a {:?} column (literal {})",
                    value, kind, literal
                );
                prop_assert_eq!(
                    msgpack.as_ref().err(),
                    Some(&ExtractionError::InvalidFieldType),
                    "MessagePack accepted {:?} in a {:?} column (literal {})",
                    value, kind, literal
                );
            }
        }
    }
}

#[cfg(test)]
mod pins {
    use super::*;

    /// The four coercions that diverged between the pipelines, pinned as
    /// explicit cases so a regression names itself instead of surfacing as a
    /// proptest shrink.
    #[test]
    fn diverged_coercions_now_agree() {
        // Int32 takes no floats.
        assert_eq!(
            via_json(ArrowType::Int32, "1.5"),
            Err(ExtractionError::InvalidFieldType)
        );
        assert_eq!(
            via_msgpack(ArrowType::Int32, "1.5"),
            Err(ExtractionError::InvalidFieldType)
        );
        // Int32 takes no ISO strings.
        assert_eq!(
            via_json(ArrowType::Int32, "\"2024-01-15T10:30:00Z\""),
            Err(ExtractionError::InvalidFieldType)
        );
        assert_eq!(
            via_msgpack(ArrowType::Int32, "\"2024-01-15T10:30:00Z\""),
            Err(ExtractionError::InvalidFieldType)
        );
        // Int32 refuses an integer outside i32 range on both wires.
        assert_eq!(
            via_json(ArrowType::Int32, "3000000000"),
            Err(ExtractionError::InvalidFieldType)
        );
        assert_eq!(
            via_msgpack(ArrowType::Int32, "3000000000"),
            Err(ExtractionError::InvalidFieldType)
        );
        // Int64 takes bigint-as-string on both wires.
        assert_eq!(
            via_json(ArrowType::Int64, "\"12345\""),
            Ok(Some(ColumnValue::Int(12345)))
        );
        assert_eq!(
            via_msgpack(ArrowType::Int64, "\"12345\""),
            Ok(Some(ColumnValue::Int(12345)))
        );
        // Int64 takes ISO-8601 on both wires.
        let micros = parse_iso8601_to_micros("2024-01-15T10:30:00Z").unwrap();
        assert_eq!(
            via_json(ArrowType::Int64, "\"2024-01-15T10:30:00Z\""),
            Ok(Some(ColumnValue::Int(micros)))
        );
        assert_eq!(
            via_msgpack(ArrowType::Int64, "\"2024-01-15T10:30:00Z\""),
            Ok(Some(ColumnValue::Int(micros)))
        );
        // Int64 takes no floats.
        assert_eq!(
            via_json(ArrowType::Int64, "1.5"),
            Err(ExtractionError::InvalidFieldType)
        );
        assert_eq!(
            via_msgpack(ArrowType::Int64, "1.5"),
            Err(ExtractionError::InvalidFieldType)
        );
    }

    /// A negative `Int32` cell must read back negative. The physical plane is
    /// four bytes wide; reading it as unsigned turns `-1` into `4294967295`.
    #[test]
    fn negative_int32_round_trips_signed() {
        assert_eq!(
            via_json(ArrowType::Int32, "-1"),
            Ok(Some(ColumnValue::Int(-1)))
        );
        assert_eq!(
            via_msgpack(ArrowType::Int32, "-1"),
            Ok(Some(ColumnValue::Int(-1)))
        );
        assert_eq!(
            via_json(ArrowType::Int32, "-2147483648"),
            Ok(Some(ColumnValue::Int(i64::from(i32::MIN))))
        );
    }
}
