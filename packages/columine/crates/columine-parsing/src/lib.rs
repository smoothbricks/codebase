//! JSON and MessagePack parsing into Arrow-compatible columnar storage.
//!
//! The parsers share scanners and typed extraction diagnostics; column storage
//! and schema metadata live in [`columine-arrow`]. [`ColumnValue`] remains the
//! extractors' typed-token carrier and the tests' readable cell view.

use std::collections::HashMap;

#[cfg(test)]
mod cross_format;

pub mod json_extractor;
pub mod json_parser;
pub mod json_scanner;
pub mod msgpack_extractor;
pub mod msgpack_scanner;
pub mod scan;
pub mod validate;

pub use columine_arrow::{
    ArrowType, BASE_EVENT_LOG_FIELDS, BASE_EVENT_LOG_NAMES, ColumnStorage, DynamicColumns,
    MAX_EVENTS_PER_BATCH, MAX_VALUE_BYTES, ParseError, PlaneKind, SignalSchemaField,
};

/// Column indices of the base event log, matching
/// [`BASE_EVENT_LOG_FIELDS`] by position.
pub mod base_column {
    pub const ID: u32 = 0;
    pub const TYPE: u32 = 1;
    pub const TIMESTAMP: u32 = 2;
    pub const VALUE: u32 = 3;
}

/// Commit one base event into the four base-event-log columns.
///
/// Both scanners end here, so the column order and the null-`value` rule live
/// in one place rather than being restated per format.
pub(crate) fn commit_base_event(
    columns: &mut DynamicColumns,
    id: &[u8],
    event_type: &[u8],
    timestamp_micros: i64,
    value: Option<&[u8]>,
) -> Result<(), ParseError> {
    if !columns.begin_row() {
        return Err(ParseError::TooManyEvents);
    }
    let result = (|| {
        columns.append_variable(base_column::ID, id)?;
        columns.append_variable(base_column::TYPE, event_type)?;
        columns.append_int(base_column::TIMESTAMP, timestamp_micros)?;
        match value {
            Some(bytes) => columns.append_variable(base_column::VALUE, bytes),
            None => columns.append_null(base_column::VALUE),
        }
    })();
    if result.is_err() {
        // A half-written row must not become visible.
        columns.abandon_row();
        return result;
    }
    columns.end_row();
    Ok(())
}

/// Base event-log columns for scanner tests: the one column store, configured
/// with the base schema the scanners write by index.
#[cfg(test)]
pub(crate) fn base_columns(capacity: u32) -> DynamicColumns {
    DynamicColumns::new(&BASE_EVENT_LOG_FIELDS, capacity)
}

/// A base event as a row view over the base event-log columns (scanner tests
/// and row-oriented consumers; the columnar buffers are the storage of record).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedEvent {
    pub id: String,
    pub event_type: String,
    pub timestamp_micros: i64,
    pub value: Option<Vec<u8>>,
}

/// Row view over the real columnar storage.
pub fn parsed_event(columns: &DynamicColumns, row: u32) -> Option<ParsedEvent> {
    if row >= columns.count {
        return None;
    }
    let text = |column: u32| -> Option<String> {
        Some(String::from_utf8_lossy(columns.get_column(column)?.read_variable(row)?).into_owned())
    };
    Some(ParsedEvent {
        id: text(base_column::ID)?,
        event_type: text(base_column::TYPE)?,
        timestamp_micros: columns.get_column(base_column::TIMESTAMP)?.read_int(row)?,
        value: (!columns.is_null(base_column::VALUE, row))
            .then(|| {
                columns
                    .get_column(base_column::VALUE)
                    .and_then(|column| column.read_variable(row))
                    .map(<[u8]>::to_vec)
            })
            .flatten(),
    })
}

/// Typed value carrier between the token stream and the typed appends.
///
/// One variant per plane KIND, not per plane: the column knows its own width,
/// so a producer says "this is a signed integer" and the plane decides whether
/// that integer fits. Twenty-three planes therefore need seven carriers.
#[derive(Clone, Debug, PartialEq)]
pub enum ColumnValue {
    /// Text for the Utf8 and LargeUtf8 planes.
    Utf8(String),
    /// Opaque bytes for the Binary and LargeBinary planes.
    Binary(Vec<u8>),
    /// Signed integer for the Int8/16/32/64 and IntervalYearMonth planes,
    /// which is also every temporal plane.
    Int(i64),
    /// Unsigned integer for the UInt8/16/32/64 planes.
    UInt(u64),
    /// Float for the Float16/32/64 planes.
    Float(f64),
    Bool(bool),
    /// Exactly-width bytes for the decimal, wide-interval and fixed-size
    /// binary planes, which the plane does not interpret.
    FixedBytes(Vec<u8>),
}

/// Dispatch an extracted value to the matching typed append operation.
///
/// [`ColumnValue`] materialization mirrors the parser's owned tokens; it is
/// useful for tests and keeps storage concerns out of the token stream.
pub(crate) fn append_cell(
    columns: &mut DynamicColumns,
    column: usize,
    value: Option<ColumnValue>,
) -> Result<(), ParseError> {
    let column = column as u32;
    match value {
        None => columns.append_null(column),
        Some(ColumnValue::Utf8(text)) => columns.append_variable(column, text.as_bytes()),
        Some(ColumnValue::Binary(bytes)) => columns.append_variable(column, &bytes),
        Some(ColumnValue::Int(value)) => columns.append_int(column, value),
        Some(ColumnValue::UInt(value)) => columns.append_uint(column, value),
        Some(ColumnValue::Float(value)) => columns.append_float(column, value),
        Some(ColumnValue::Bool(value)) => columns.append_bool(column, value),
        Some(ColumnValue::FixedBytes(bytes)) => columns.append_fixed_bytes(column, &bytes),
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
    Some(match storage.kind {
        // Every row of the Null plane is null, so `is_null` already returned;
        // reaching here would mean a validity bit was set on a plane that has
        // no value to be valid.
        PlaneKind::Empty => return None,
        PlaneKind::Bool => ColumnValue::Bool(storage.read_bool(row_idx)?),
        PlaneKind::SignedInt { .. } => ColumnValue::Int(storage.read_int(row_idx)?),
        PlaneKind::UnsignedInt { .. } => ColumnValue::UInt(storage.read_uint(row_idx)?),
        PlaneKind::Float { .. } => ColumnValue::Float(storage.read_float(row_idx)?),
        PlaneKind::FixedBytes { .. } => {
            ColumnValue::FixedBytes(storage.read_fixed_bytes(row_idx)?.to_vec())
        }
        PlaneKind::Text { .. } => {
            ColumnValue::Utf8(String::from_utf8_lossy(storage.read_variable(row_idx)?).into_owned())
        }
        PlaneKind::Bytes { .. } => ColumnValue::Binary(storage.read_variable(row_idx)?.to_vec()),
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FieldLookup {
    pub column: usize,
    /// The whole field descriptor, not just its tag: the extractors coerce by
    /// plane kind, and one plane's kind depends on its `type_param`.
    pub field: SignalSchemaField,
}
/// Schema-name lookup for extraction: O(1) name → column/type, plus the
/// [`UNDECLARED_COLUMN_NAME`] carrier column when declared.
#[derive(Clone, Debug)]
pub struct ExtractionConfig {
    pub(crate) field_entries: Vec<(usize, SignalSchemaField)>,
    pub(crate) field_map: HashMap<String, FieldLookup>,
    pub(crate) fallback_column: Option<usize>,
    pub(crate) presence_entries: Vec<(usize, usize)>,
    /// Relative payload paths whose object schemas opt into capture.
    pub(crate) open_paths: Vec<String>,
    pub(crate) semantic_schemas: Option<crate::validate::SemanticSchemaSet>,
    /// Whether any column names a `value.<field>` payload path — the arming
    /// condition for the nested-envelope descent in the extractors.
    pub(crate) has_value_fields: bool,
}

impl ExtractionConfig {
    pub(crate) fn is_open_payload_path(&self, path: &str) -> bool {
        self.open_paths.iter().any(|open| open == path)
    }

    pub(crate) fn semantic_field_schema(
        &self,
        name: &str,
    ) -> Option<&crate::validate::SemanticSchema> {
        let name = name.strip_prefix("value.").unwrap_or(name);
        let schemas = self.semantic_schemas.as_ref()?;
        schemas.values().find_map(|schema| match schema {
            crate::validate::SemanticSchema::Object { fields, .. } => fields
                .iter()
                .find(|(field, _)| field == name)
                .map(|(_, schema)| schema),
            _ => None,
        })
    }
    pub(crate) fn lookup_field(&self, name: &str) -> Option<&FieldLookup> {
        self.field_map.get(name).or_else(|| {
            let prefixed = format!("value.{name}");
            self.field_map.get(&prefixed)
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigError {
    FieldNameCountMismatch,
    DuplicateFieldName,
    InvalidPresenceField,
}

const VALUE_PRESENCE_PREFIX: &str = "event_value_present.";

/// The sole Arrow/config/spec spelling for the bounded undeclared-value carrier.
pub const UNDECLARED_COLUMN_NAME: &str = "value.$undeclared";

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
    build_extraction_config_with_semantic_schemas(fields, names, None)
}

pub fn build_extraction_config_with_semantic_schemas(
    fields: &[SignalSchemaField],
    names: &[&str],
    semantic_schemas: Option<&crate::validate::SemanticSchemaSet>,
) -> Result<ExtractionConfig, ConfigError> {
    if fields.len() != names.len() {
        return Err(ConfigError::FieldNameCountMismatch);
    }
    let mut field_entries = Vec::with_capacity(fields.len());
    let mut field_map = HashMap::with_capacity(fields.len());
    let mut unresolved_presence = Vec::new();
    let mut open_paths = Vec::new();
    if let Some(semantic_schemas) = semantic_schemas {
        for schema in semantic_schemas.values() {
            crate::validate::collect_open_paths(schema, "", &mut open_paths);
        }
        open_paths.sort();
        open_paths.dedup();
    }
    let mut fallback_column = None;
    for (column, (field, name)) in fields.iter().zip(names).enumerate() {
        if let Some(source_name) = decode_presence_source(name)? {
            if field.arrow_type != ArrowType::Bool {
                return Err(ConfigError::InvalidPresenceField);
            }
            unresolved_presence.push((column, source_name));
            field_entries.push((column, *field));
            continue;
        }
        if field_map
            .insert(
                (*name).to_owned(),
                FieldLookup {
                    column,
                    field: *field,
                },
            )
            .is_some()
        {
            return Err(ConfigError::DuplicateFieldName);
        }
        if *name == UNDECLARED_COLUMN_NAME {
            fallback_column = Some(column);
        }
        field_entries.push((column, *field));
    }
    let mut presence_entries = Vec::with_capacity(unresolved_presence.len());
    for (presence_column, source_name) in unresolved_presence {
        let source = field_map
            .get(&source_name)
            .ok_or(ConfigError::InvalidPresenceField)?;
        presence_entries.push((presence_column, source.column));
    }
    // The nested-envelope descent arms only for schemas that declare payload
    // columns: a pure system schema has no `value.<field>` names, and its
    // `value` member (an op result's unwrapped scalar, say) must keep its
    // undeclared handling instead of being walked as a payload object.
    let has_value_fields = field_map.keys().any(|name| name.starts_with("value."));
    Ok(ExtractionConfig {
        field_entries,
        field_map,
        fallback_column,
        presence_entries,
        open_paths,
        semantic_schemas: semantic_schemas.cloned(),
        has_value_fields,
    })
}

/// Release an extraction configuration.
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

            let mut columns = base_columns(1);
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

            let mut base = base_columns(1);
            msgpack_scanner::parse_msgpack_stream(&input, &mut base).unwrap();
            assert_eq!(parsed_event(&base, 0).unwrap().id, id);

            let fields = [
                SignalSchemaField::new(ArrowType::Utf8, false),
                SignalSchemaField::new(ArrowType::Utf8, false),
                SignalSchemaField::new(ArrowType::Int64, false),
                SignalSchemaField::new(ArrowType::Int64, true),
            ];
            let config = build_extraction_config(&fields, &["id", "type", "timestamp", "quantity"]).unwrap();
            // An exactly-capacity batch is legal; this input has spare
            // capacity and therefore must be accepted.
            let mut typed = DynamicColumns::new(&fields, 2);
            let mut work = [0_u8; 128];
            assert_eq!(msgpack_extractor::extract_msgpack_events(&input, &config, &mut typed, &mut work, true).unwrap(), 1);
            assert_eq!(read_cell(&typed, 0, 0), Some(ColumnValue::Utf8(id)));
            assert_eq!(read_cell(&typed, 1, 0), Some(ColumnValue::Utf8(event_type)));
            assert_eq!(read_cell(&typed, 2, 0), Some(ColumnValue::Int(timestamp)));
            assert_eq!(read_cell(&typed, 3, 0), Some(ColumnValue::Int(quantity)));
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
        /// undeclared field lands as typed msgpack in the undeclared carrier, byte-exact.
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
            let config = build_extraction_config(&fields, &["id", "qty", UNDECLARED_COLUMN_NAME]).unwrap();
            let mut typed = DynamicColumns::new(&fields, 2);
            let mut work = [0_u8; 256];
            assert_eq!(
                json_extractor::extract_json_events(document.as_bytes(), &config, &mut typed, &mut work, &mut json_extractor::ExtractionDiagnostic::default()).unwrap(),
                1
            );
            prop_assert_eq!(read_cell(&typed, 0, 0), Some(ColumnValue::Utf8(id)));
            prop_assert_eq!(read_cell(&typed, 1, 0), Some(ColumnValue::Int(quantity)));
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
