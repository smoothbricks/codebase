//! Rust port of Columine's JSON and MessagePack parsing pipeline.
//!
//! The five modules mirror the Zig parsing inventory. Column storage and
//! schema metadata are the real Arrow-layout implementations from
//! `columine-arrow` (the former stage-4B boundary stand-ins are gone);
//! [`ColumnValue`] remains as the extractors' typed-token carrier and the
//! tests' readable cell view.

use std::collections::HashMap;

pub mod json_extractor;
pub mod json_parser;
pub mod json_scanner;
pub mod msgpack_extractor;
pub mod msgpack_scanner;
pub mod scan;

pub use columine_arrow::{
    ArrowType, ColumnStorage, ColumnType, DynamicColumns, EventColumns, MAX_EVENTS_PER_BATCH,
    MAX_STRING_BYTES, MAX_VALUE_BYTES, ParseError, SignalSchemaField,
};

/// A base event as a row view over [`EventColumns`] (scanner tests and
/// row-oriented consumers; the columnar buffers are the storage of record).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedEvent {
    pub id: String,
    pub event_type: String,
    pub timestamp_micros: i64,
    pub value: Option<Vec<u8>>,
}

/// Row view over the real columnar storage.
pub fn parsed_event(cols: &EventColumns, row: u32) -> Option<ParsedEvent> {
    Some(ParsedEvent {
        id: String::from_utf8_lossy(cols.get_id(row)?).into_owned(),
        event_type: String::from_utf8_lossy(cols.get_type(row)?).into_owned(),
        timestamp_micros: cols.get_timestamp(row)?,
        value: cols.get_value(row).map(<[u8]>::to_vec),
    })
}

/// Typed value carrier between the token stream and the typed appends.
#[derive(Clone, Debug, PartialEq)]
pub enum ColumnValue {
    Utf8(String),
    Int64(i64),
    Float64(f64),
    Bool(bool),
    Binary(Vec<u8>),
}

/// Dispatch one extracted value to the real typed append
/// (json_extractor.zig routes each token type to appendUtf8/appendInt64/…;
/// `None` is `appendNull`). The `ColumnValue` materialization mirrors the
/// parser's owned tokens — an efficiency residual for the perf pass, not a
/// semantic one.
pub(crate) fn append_cell(
    columns: &mut DynamicColumns,
    column: usize,
    value: Option<ColumnValue>,
) -> Result<(), ParseError> {
    let column = column as u32;
    match value {
        None => columns.append_null(column),
        Some(ColumnValue::Utf8(s)) => columns.append_utf8(column, s.as_bytes()),
        Some(ColumnValue::Int64(v)) => columns.append_int64(column, v),
        Some(ColumnValue::Float64(v)) => columns.append_float64(column, v),
        Some(ColumnValue::Bool(v)) => columns.append_bool(column, v),
        Some(ColumnValue::Binary(b)) => columns.append_binary(column, &b),
    }
}

/// Read one cell back out of the real columnar storage as a typed value
/// (test/differential view; production consumers read the Arrow buffers).
pub fn read_cell(columns: &DynamicColumns, column: usize, row: usize) -> Option<ColumnValue> {
    let (col_idx, row_idx) = (column as u32, row as u32);
    if columns.is_null(col_idx, row_idx) {
        return None;
    }
    let storage = columns.get_column(col_idx)?;
    Some(match storage.col_type {
        ColumnType::Utf8 => {
            ColumnValue::Utf8(String::from_utf8_lossy(storage.read_variable(row_idx)?).into_owned())
        }
        ColumnType::Binary => ColumnValue::Binary(storage.read_variable(row_idx)?.to_vec()),
        ColumnType::Int64 => ColumnValue::Int64(storage.read_fixed_i64(row_idx)?),
        ColumnType::Float64 => ColumnValue::Float64(storage.read_fixed_f64(row_idx)?),
        ColumnType::Bool => ColumnValue::Bool(storage.read_bool(row_idx)?),
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FieldLookup {
    pub column: usize,
    pub arrow_type: ArrowType,
}

/// Schema-name lookup for extraction (json_extractor.zig
/// `buildExtractionConfig`): O(1) name → column/type, plus the
/// `value.$extra` fallback column when declared.
#[derive(Clone, Debug)]
pub struct ExtractionConfig {
    pub(crate) field_entries: Vec<(usize, ArrowType, String)>,
    pub(crate) field_map: HashMap<String, FieldLookup>,
    pub(crate) fallback_column: Option<usize>,
    pub(crate) presence_entries: Vec<(usize, usize)>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigError {
    FieldNameCountMismatch,
    DuplicateFieldName,
    InvalidPresenceField,
}

const VALUE_PRESENCE_PREFIX: &str = "event_value_present.";

fn decode_presence_source(name: &str) -> Result<Option<String>, ConfigError> {
    let Some(encoded) = name.strip_prefix(VALUE_PRESENCE_PREFIX) else {
        return Ok(None);
    };
    let bytes = encoded.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let Some(hex) = bytes.get(index + 1..index + 3) else {
                return Err(ConfigError::InvalidPresenceField);
            };
            let hex = std::str::from_utf8(hex).map_err(|_| ConfigError::InvalidPresenceField)?;
            decoded
                .push(u8::from_str_radix(hex, 16).map_err(|_| ConfigError::InvalidPresenceField)?);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded)
        .map(Some)
        .map_err(|_| ConfigError::InvalidPresenceField)
}

pub fn build_extraction_config(
    fields: &[SignalSchemaField],
    names: &[&str],
) -> Result<ExtractionConfig, ConfigError> {
    if fields.len() != names.len() {
        return Err(ConfigError::FieldNameCountMismatch);
    }
    let mut field_entries = Vec::with_capacity(fields.len());
    let mut field_map = HashMap::with_capacity(fields.len());
    let mut unresolved_presence = Vec::new();
    let mut fallback_column = None;
    for (column, (field, name)) in fields.iter().zip(names).enumerate() {
        if let Some(source_name) = decode_presence_source(name)? {
            if field.arrow_type != ArrowType::Bool {
                return Err(ConfigError::InvalidPresenceField);
            }
            unresolved_presence.push((column, source_name));
            field_entries.push((column, field.arrow_type, (*name).to_owned()));
            continue;
        }
        if field_map
            .insert(
                (*name).to_owned(),
                FieldLookup {
                    column,
                    arrow_type: field.arrow_type,
                },
            )
            .is_some()
        {
            return Err(ConfigError::DuplicateFieldName);
        }
        if *name == "value.$extra" {
            fallback_column = Some(column);
        }
        field_entries.push((column, field.arrow_type, (*name).to_owned()));
    }
    let mut presence_entries = Vec::with_capacity(unresolved_presence.len());
    for (presence_column, source_name) in unresolved_presence {
        let source = field_map
            .get(&source_name)
            .ok_or(ConfigError::InvalidPresenceField)?;
        presence_entries.push((presence_column, source.column));
    }
    Ok(ExtractionConfig {
        field_entries,
        field_map,
        fallback_column,
        presence_entries,
    })
}

/// Rust owns allocation, so dropping the configuration is the direct
/// equivalent of Zig's explicit `freeExtractionConfig`.
pub fn free_extraction_config(config: ExtractionConfig) {
    drop(config);
}

/// Method-style cell/count views for tests and differential assertions.
pub trait CellExt {
    fn cell(&self, column: usize, row: usize) -> Option<ColumnValue>;
    fn count(&self) -> usize;
}

impl CellExt for DynamicColumns {
    fn cell(&self, column: usize, row: usize) -> Option<ColumnValue> {
        read_cell(self, column, row)
    }
    fn count(&self) -> usize {
        self.count as usize
    }
}

#[cfg(test)]
mod properties {
    use super::*;
    use proptest::prelude::*;

    /// Arbitrary well-formed MessagePack value bytes (leaves + shallow
    /// containers) for skip/position properties.
    fn msgpack_value() -> impl Strategy<Value = Vec<u8>> {
        let leaf = prop_oneof![
            (0_u8..=0x7f).prop_map(|byte| vec![byte]),
            any::<i64>().prop_map(|value| {
                let mut out = vec![0xd3];
                out.extend(value.to_be_bytes());
                out
            }),
            any::<f64>().prop_map(|value| {
                let mut out = vec![0xcb];
                out.extend(value.to_bits().to_be_bytes());
                out
            }),
            "[a-z]{0,20}".prop_map(|text| {
                let mut out = vec![0xa0 | u8::try_from(text.len()).unwrap()];
                out.extend(text.as_bytes());
                out
            }),
            Just(vec![0xc0]),
            Just(vec![0xc2]),
            Just(vec![0xc3]),
        ];
        leaf.prop_recursive(3, 24, 4, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..4).prop_map(|items| {
                    let mut out = vec![0x90 | u8::try_from(items.len()).unwrap()];
                    for item in items {
                        out.extend(item);
                    }
                    out
                }),
                prop::collection::vec(("[a-z]{1,8}", inner), 0..4).prop_map(|pairs| {
                    let mut out = vec![0x80 | u8::try_from(pairs.len()).unwrap()];
                    for (key, value) in pairs {
                        out.push(0xa0 | u8::try_from(key.len()).unwrap());
                        out.extend(key.as_bytes());
                        out.extend(value);
                    }
                    out
                }),
            ]
        })
    }

    proptest! {
        /// Scanner output and the streaming parser agree on generated event
        /// documents. The generator limits strings to JSON-safe ASCII because
        /// escaping itself is covered by the lexer unit tests.
        #[test]
        fn json_scanner_and_parser_agree(
            id in "[a-z0-9]{1,12}",
            event_type in "[a-z]{1,12}",
            timestamp in 0_i64..4_000_000_000_000_i64,
            value in -10_000_i64..10_000_i64,
        ) {
            let document = format!(r#"[{{"id":"{id}","type":"{event_type}","timestamp":{timestamp},"value":{value}}}]"#);
            let mut parser = json_parser::JsonParser::new(document.as_bytes());
            parser.expect_array_begin().unwrap();
            parser.expect_object_begin().unwrap();
            assert_eq!(parser.expect_field_name().unwrap(), "id");
            assert_eq!(parser.expect_string().unwrap(), id);

            let mut columns = EventColumns::new(1);
            json_scanner::parse_json_events(document.as_bytes(), &mut columns).unwrap();
            let event = parsed_event(&columns, 0).unwrap();
            assert_eq!(event.id, id);
            assert_eq!(event.event_type, event_type);
            assert_eq!(event.timestamp_micros, timestamp * 1_000);
            assert_eq!(event.value.as_deref(), Some(value.to_string().as_bytes()));
        }

        /// Hand-written MessagePack maps retain the scanner/extractor typed
        /// values without depending on a general MessagePack crate.
        #[test]
        fn msgpack_scan_and_extract_round_trip(
            id in "[a-z0-9]{1,12}",
            event_type in "[a-z]{1,12}",
            timestamp in 0_i64..4_000_000_000_000_i64,
            quantity in 0_i64..100_i64,
        ) {
            let mut input = vec![0x84];
            write_fixstr(&mut input, "id"); write_fixstr(&mut input, &id);
            write_fixstr(&mut input, "type"); write_fixstr(&mut input, &event_type);
            write_fixstr(&mut input, "timestamp"); input.push(0xd3); input.extend(timestamp.to_be_bytes());
            write_fixstr(&mut input, "quantity"); input.push(quantity as u8);

            let mut base = EventColumns::new(1);
            msgpack_scanner::parse_msgpack_stream(&input, &mut base).unwrap();
            assert_eq!(parsed_event(&base, 0).unwrap().id, id);

            let fields = [
                SignalSchemaField::new(ArrowType::Utf8, false),
                SignalSchemaField::new(ArrowType::Utf8, false),
                SignalSchemaField::new(ArrowType::Int64, false),
                SignalSchemaField::new(ArrowType::Int64, true),
            ];
            let config = build_extraction_config(&fields, &["id", "type", "timestamp", "quantity"]).unwrap();
            // Capacity 2 for a 1-event batch: the ported Zig capacity check
            // rejects exactly-capacity batches (count >= capacity).
            let mut typed = DynamicColumns::new(&fields, 2);
            let mut work = [0_u8; 128];
            assert_eq!(msgpack_extractor::extract_msgpack_events(&input, &config, &mut typed, &mut work, true).unwrap(), 1);
            assert_eq!(read_cell(&typed, 0, 0), Some(ColumnValue::Utf8(id)));
            assert_eq!(read_cell(&typed, 1, 0), Some(ColumnValue::Utf8(event_type)));
            assert_eq!(read_cell(&typed, 2, 0), Some(ColumnValue::Int64(timestamp)));
            assert_eq!(read_cell(&typed, 3, 0), Some(ColumnValue::Int64(quantity)));
        }

        /// skip_value consumes exactly one well-formed value: the reader's
        /// final position equals the value length regardless of trailing
        /// bytes. This pins the zero-copy `value` capture in the scanners.
        #[test]
        fn msgpack_skip_value_consumes_exactly_one_value(
            value in msgpack_value(),
            trailer in prop::collection::vec(any::<u8>(), 0..8),
        ) {
            let mut input = value.clone();
            input.extend(&trailer);
            let mut reader = msgpack_scanner::Reader::new(&input);
            reader.skip_value().unwrap();
            prop_assert_eq!(reader.position(), value.len());
        }

        /// JSON extractor differential: declared fields land typed, the
        /// undeclared field lands as typed msgpack in $extra, byte-exact.
        #[test]
        fn json_extractor_routes_declared_and_undeclared_fields(
            id in "[a-z0-9]{1,12}",
            quantity in -1_000_i64..1_000,
            extra_key in "[a-z]{1,8}",
            extra_flag in proptest::bool::ANY,
        ) {
            let document = format!(
                r#"[{{"id":"{id}","qty":{quantity},"x{extra_key}":{extra_flag}}}]"#
            );
            let fields = [
                SignalSchemaField::new(ArrowType::Utf8, false),
                SignalSchemaField::new(ArrowType::Int64, false),
                SignalSchemaField::new(ArrowType::Binary, true),
            ];
            let config = build_extraction_config(&fields, &["id", "qty", "value.$extra"]).unwrap();
            let mut typed = DynamicColumns::new(&fields, 2);
            let mut work = [0_u8; 256];
            assert_eq!(
                json_extractor::extract_json_events(document.as_bytes(), &config, &mut typed, &mut work, &mut json_extractor::ExtractionDiagnostic::default()).unwrap(),
                1
            );
            prop_assert_eq!(read_cell(&typed, 0, 0), Some(ColumnValue::Utf8(id)));
            prop_assert_eq!(read_cell(&typed, 1, 0), Some(ColumnValue::Int64(quantity)));
            let mut expected = vec![0xdf, 0, 0, 0, 1];
            expected.push(0xa0 | u8::try_from(extra_key.len() + 1).unwrap());
            expected.push(b'x');
            expected.extend(extra_key.as_bytes());
            expected.push(if extra_flag { 0xc3 } else { 0xc2 });
            prop_assert_eq!(read_cell(&typed, 2, 0), Some(ColumnValue::Binary(expected)));
        }
    }

    fn write_fixstr(output: &mut Vec<u8>, value: &str) {
        output.push(0xa0 | u8::try_from(value.len()).unwrap());
        output.extend_from_slice(value.as_bytes());
    }
}
