//! Schema-layer validation for signal payload values.
//!
//! The validator deliberately knows nothing about Arrow or any concrete value
//! encoding. [`ValueView`] is the small read-only seam used by JSON, MessagePack,
//! and native adapters; keeping it unsealed lets downstream runtimes implement
//! the same judgment without creating a dependency back into those runtimes.
use std::{collections::BTreeMap, fmt};

use crate::{
    UNDECLARED_COLUMN_NAME,
    json_parser::{JsonParser, Token},
    msgpack_scanner::Reader,
};

const MAX_SCHEMA_DEPTH: usize = 256;
const MAX_VALUE_DEPTH: usize = 256;

/// Semantic kinds understood by the signal schema tree and value adapters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ValueKind {
    Unknown,
    Null,
    String,
    Number,
    Boolean,
    Binary,
    BigInt,
    Array,
    Object,
}

impl ValueKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Null => "null",
            Self::String => "string",
            Self::Number => "number",
            Self::Boolean => "boolean",
            Self::Binary => "binary",
            Self::BigInt => "bigint",
            Self::Array => "array",
            Self::Object => "object",
        }
    }
}

impl fmt::Display for ValueKind {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(self.as_str())
    }
}

/// Exact numeric forms exposed by a value adapter.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum NumberValue {
    Signed(i128),
    Unsigned(u128),
    Float(f64),
}

/// Read-only value-model seam for schema validation.
///
/// Object and array traversal is callback-based so implementations can expose
/// borrowed values without allocating or requiring a common owned tree. A
/// runtime may implement only the accessors relevant to its [`ValueKind`]. The
/// default accessors are intentionally empty: a malformed adapter is rejected
/// as a type/shape violation rather than causing a panic.
pub trait ValueView {
    fn kind(&self) -> ValueKind;

    fn as_str(&self) -> Option<&str> {
        None
    }

    fn as_number(&self) -> Option<NumberValue> {
        None
    }

    fn as_bytes(&self) -> Option<&[u8]> {
        None
    }

    fn object_field(&self, _name: &str) -> Option<&dyn ValueView> {
        None
    }

    fn visit_object(&self, _visit: &mut dyn FnMut(&str, &dyn ValueView)) {}

    fn visit_array(&self, _visit: &mut dyn FnMut(usize, &dyn ValueView)) {}
}

/// A value tree useful to adapters and tests that already have JSON-shaped
/// data. Binary and bigint values remain representable for MessagePack/native
/// conformance tests even though canonical JSON cannot spell them directly.
#[derive(Clone, Debug, PartialEq)]
pub enum JsonValue {
    Unknown,
    Null,
    String(String),
    Number(NumberValue),
    Boolean(bool),
    Binary(Vec<u8>),
    BigInt,
    Array(Vec<JsonValue>),
    Object(Vec<(String, JsonValue)>),
}

impl JsonValue {
    /// Parse one JSON value with the repository's strict streaming parser.
    pub fn parse(input: &[u8]) -> Result<Self, ValueParseError> {
        let mut parser = JsonParser::new(input);
        let value = parse_json_value(&mut parser, 0)?;
        match parser.next_token() {
            Err(_) if parser.cursor() == input.len() => Ok(value),
            _ => Err(ValueParseError::InvalidJson),
        }
    }

    /// Parse one MessagePack value using the extraction crate's reader.
    pub(crate) fn parse_msgpack(input: &[u8]) -> Result<Self, ValueParseError> {
        let mut reader = Reader::new(input);
        let value = parse_msgpack_value(&mut reader, 0)?;
        if reader.at_end() {
            Ok(value)
        } else {
            Err(ValueParseError::InvalidMsgpack)
        }
    }
    pub fn object(fields: Vec<(String, JsonValue)>) -> Self {
        Self::Object(fields)
    }
}

impl ValueView for JsonValue {
    fn kind(&self) -> ValueKind {
        match self {
            Self::Unknown => ValueKind::Unknown,
            Self::Null => ValueKind::Null,
            Self::String(_) => ValueKind::String,
            Self::Number(_) => ValueKind::Number,
            Self::Boolean(_) => ValueKind::Boolean,
            Self::Binary(_) => ValueKind::Binary,
            Self::BigInt => ValueKind::BigInt,
            Self::Array(_) => ValueKind::Array,
            Self::Object(_) => ValueKind::Object,
        }
    }

    fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    fn as_number(&self) -> Option<NumberValue> {
        match self {
            Self::Number(value) => Some(*value),
            _ => None,
        }
    }

    fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Binary(value) => Some(value),
            _ => None,
        }
    }

    fn object_field(&self, name: &str) -> Option<&dyn ValueView> {
        match self {
            Self::Object(fields) => fields
                .iter()
                .find(|(field, _)| field == name)
                .map(|(_, value)| value as &dyn ValueView),
            _ => None,
        }
    }

    fn visit_object(&self, visit: &mut dyn FnMut(&str, &dyn ValueView)) {
        if let Self::Object(fields) = self {
            for (name, value) in fields {
                visit(name, value);
            }
        }
    }

    fn visit_array(&self, visit: &mut dyn FnMut(usize, &dyn ValueView)) {
        if let Self::Array(values) = self {
            for (index, value) in values.iter().enumerate() {
                visit(index, value);
            }
        }
    }
}

/// Why canonical schema-tree decoding failed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchemaParseError {
    InvalidJson,
    InvalidTree,
    UnknownKind,
    MissingOperand,
    InvalidOperand,
    DuplicateField,
    TooDeep,
    EmptyUnion,
}

/// Why a value failed the semantic schema judgment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PayloadViolation {
    pub path: String,
    pub expected: String,
    pub observed: String,
}

impl PayloadViolation {
    fn new(path: impl Into<String>, expected: impl Into<String>, observed: ValueKind) -> Self {
        Self {
            path: path.into(),
            expected: expected.into(),
            observed: observed.as_str().to_owned(),
        }
    }

    fn shape(path: &str, expected: &str, value: &dyn ValueView) -> Self {
        Self::new(path, expected, value.kind())
    }

    fn closed_unknown(path: &str, fields: &[(String, SemanticSchema)]) -> Self {
        let declared = fields
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        Self {
            path: path.to_owned(),
            expected: format!(
                "not declared (declared: {declared}); declare the enclosing object open — S.object({{…}}, {{ open: true }}) — to capture undeclared keys into {UNDECLARED_COLUMN_NAME}"
            ),
            observed: "undeclared".into(),
        }
    }
}
impl fmt::Display for PayloadViolation {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            output,
            "{}: expected {}, got {}",
            self.path, self.expected, self.observed
        )
    }
}

/// The recursive canonical-JSON semantic schema tree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SemanticSchema {
    Unknown,
    String,
    Number,
    Boolean,
    Binary,
    BigInt,
    I32,
    U32,
    Enum(Vec<String>),
    Array(Box<Self>),
    Object {
        fields: Vec<(String, Self)>,
        open: bool,
    },
    Union(Vec<Self>),
    Map {
        key: Box<Self>,
        value: Box<Self>,
    },
    Record(Box<Self>),
    Optional(Box<Self>),
    Nullable(Box<Self>),
}

impl SemanticSchema {
    /// Decode the canonical-JSON tree bytes used by the CAS value-schema
    /// artifact. The optional `open` member is meaningful only on objects;
    /// absent and `false` are both closed.
    pub fn from_tree_bytes(bytes: &[u8]) -> Result<Self, SchemaParseError> {
        let value = JsonValue::parse(bytes).map_err(|_| SchemaParseError::InvalidJson)?;
        Self::from_tree_value(&value)
    }

    pub fn from_tree_value(value: &JsonValue) -> Result<Self, SchemaParseError> {
        parse_schema(value, 0)
    }

    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::String => "string",
            Self::Number => "number",
            Self::Boolean => "boolean",
            Self::Binary => "binary",
            Self::BigInt => "bigint",
            Self::I32 => "i32",
            Self::U32 => "u32",
            Self::Enum(_) => "enum",
            Self::Array(_) => "array",
            Self::Object { .. } => "object",
            Self::Union(_) => "union",
            Self::Map { .. } => "map",
            Self::Record(_) => "record",
            Self::Optional(_) => "optional",
            Self::Nullable(_) => "nullable",
        }
    }

    pub fn is_open_object(&self) -> bool {
        matches!(self, Self::Object { open: true, .. })
    }
}

pub(crate) fn collect_open_paths(schema: &SemanticSchema, path: &str, output: &mut Vec<String>) {
    match schema {
        SemanticSchema::Object { fields, open } => {
            if *open {
                output.push(path.to_owned());
            }
            for (name, child) in fields {
                let child_path = if path.is_empty() {
                    name.clone()
                } else {
                    format!("{path}.{name}")
                };
                collect_open_paths(child, &child_path, output);
            }
        }
        SemanticSchema::Array(item)
        | SemanticSchema::Record(item)
        | SemanticSchema::Optional(item)
        | SemanticSchema::Nullable(item) => collect_open_paths(item, path, output),
        SemanticSchema::Map { key, value } => {
            collect_open_paths(key, path, output);
            collect_open_paths(value, path, output);
        }
        SemanticSchema::Union(variants) => {
            for variant in variants {
                collect_open_paths(variant, path, output);
            }
        }
        SemanticSchema::Unknown
        | SemanticSchema::String
        | SemanticSchema::Number
        | SemanticSchema::Boolean
        | SemanticSchema::Binary
        | SemanticSchema::BigInt
        | SemanticSchema::I32
        | SemanticSchema::U32
        | SemanticSchema::Enum(_) => {}
    }
}

/// Judge one value against one semantic schema.
///
/// Unknown members below an open object are intentionally not traversed: the
/// extractor owns bounded carrier capture, while this function owns only the
/// admission judgment. No mutation, deduplication, or output allocation is
/// performed here beyond diagnostics for the first violation.
pub fn validate_value<V: ValueView>(
    schema: &SemanticSchema,
    value: &V,
) -> Result<(), PayloadViolation> {
    validate_at(schema, value, "value", 0)
}

/// Semantic schemas keyed by event type as carried at EP creation.
pub type SemanticSchemaSet = BTreeMap<String, SemanticSchema>;

/// A validation failure tied to one event in an input batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BatchValidationError {
    InvalidInput,
    Violation {
        event_index: usize,
        event_type: String,
        violation: PayloadViolation,
    },
}

/// Decode `{"<eventType>": <tree>, ...}` once at processor creation.
pub fn parse_schema_envelope(bytes: &[u8]) -> Result<SemanticSchemaSet, SchemaParseError> {
    let value = JsonValue::parse(bytes).map_err(|_| SchemaParseError::InvalidJson)?;
    let fields = object_fields(&value)?;
    let mut schemas = BTreeMap::new();
    for (event_type, tree) in fields {
        if schemas
            .insert(event_type.clone(), SemanticSchema::from_tree_value(tree)?)
            .is_some()
        {
            return Err(SchemaParseError::DuplicateField);
        }
    }
    Ok(schemas)
}

/// Validate every JSON event whose `type` exists in the semantic envelope.
pub fn validate_json_batch(
    input: &[u8],
    schemas: &SemanticSchemaSet,
) -> Result<(), BatchValidationError> {
    let batch = JsonValue::parse(input).map_err(|_| BatchValidationError::InvalidInput)?;
    let JsonValue::Array(events) = batch else {
        return Err(BatchValidationError::InvalidInput);
    };
    validate_event_values(&events, schemas)
}

/// Validate every MessagePack event whose `type` exists in the semantic
/// envelope. This consumes no extraction columns or dedup state.
pub fn validate_msgpack_batch(
    input: &[u8],
    schemas: &SemanticSchemaSet,
    stream: bool,
) -> Result<(), BatchValidationError> {
    if input.is_empty() {
        return Ok(());
    }
    let mut reader = Reader::new(input);
    let mut events = Vec::new();
    if stream {
        while !reader.at_end() {
            events.push(
                parse_msgpack_event(&mut reader).map_err(|_| BatchValidationError::InvalidInput)?,
            );
        }
    } else {
        let count = reader
            .read_array_header()
            .ok_or(BatchValidationError::InvalidInput)?;
        for _ in 0..count {
            events.push(
                parse_msgpack_event(&mut reader).map_err(|_| BatchValidationError::InvalidInput)?,
            );
        }
        if !reader.at_end() {
            return Err(BatchValidationError::InvalidInput);
        }
    }
    validate_event_values(&events, schemas)
}

fn validate_event_values(
    events: &[JsonValue],
    schemas: &SemanticSchemaSet,
) -> Result<(), BatchValidationError> {
    for (event_index, event) in events.iter().enumerate() {
        let JsonValue::Object(fields) = event else {
            return Err(BatchValidationError::InvalidInput);
        };
        let Some(JsonValue::String(event_type)) = field(fields, "type") else {
            continue;
        };
        let Some(schema) = schemas.get(event_type) else {
            continue;
        };
        let value = field(fields, "value").unwrap_or(&JsonValue::Unknown);
        if let Err(violation) = validate_value(schema, value) {
            return Err(BatchValidationError::Violation {
                event_index,
                event_type: event_type.clone(),
                violation,
            });
        }
    }
    Ok(())
}

fn validate_at(
    schema: &SemanticSchema,
    value: &dyn ValueView,
    path: &str,
    depth: usize,
) -> Result<(), PayloadViolation> {
    if depth > MAX_VALUE_DEPTH {
        return Err(PayloadViolation::new(
            path,
            "value depth <= 256",
            value.kind(),
        ));
    }
    match schema {
        SemanticSchema::Unknown => Ok(()),
        SemanticSchema::String => require_kind(path, value, ValueKind::String),
        SemanticSchema::Boolean => require_kind(path, value, ValueKind::Boolean),
        SemanticSchema::Binary => require_kind(path, value, ValueKind::Binary),
        SemanticSchema::BigInt => require_kind(path, value, ValueKind::BigInt),
        SemanticSchema::Number => validate_number(path, value),
        SemanticSchema::I32 => validate_i32(path, value),
        SemanticSchema::U32 => validate_u32(path, value),
        SemanticSchema::Enum(variants) => {
            if value.kind() != ValueKind::String {
                return Err(PayloadViolation::shape(path, "enum string", value));
            }
            let Some(actual) = value.as_str() else {
                return Err(PayloadViolation::shape(path, "enum string", value));
            };
            if variants.iter().any(|variant| variant == actual) {
                Ok(())
            } else {
                Err(PayloadViolation::new(
                    path,
                    format!("enum {{{}}}", variants.join(", ")),
                    value.kind(),
                ))
            }
        }
        SemanticSchema::Array(item) => {
            if value.kind() != ValueKind::Array {
                return Err(PayloadViolation::shape(path, "array", value));
            }
            let mut violation = None;
            value.visit_array(&mut |index, item_value| {
                if violation.is_some() {
                    return;
                }
                let item_path = format!("{path}[{index}]");
                violation = validate_at(item, item_value, &item_path, depth + 1).err();
            });
            violation.map_or(Ok(()), Err)
        }
        SemanticSchema::Object { fields, open } => {
            if value.kind() != ValueKind::Object {
                return Err(PayloadViolation::shape(path, "object", value));
            }
            for (name, field_schema) in fields {
                let Some(field_value) = value.object_field(name) else {
                    if !matches!(field_schema, SemanticSchema::Optional(_)) {
                        return Err(PayloadViolation {
                            path: field_path(path, name),
                            expected: field_schema.kind().into(),
                            observed: "absent".into(),
                        });
                    }
                    continue;
                };
                let child_path = field_path(path, name);
                validate_at(field_schema, field_value, &child_path, depth + 1)?;
            }
            if !open {
                let mut violation = None;
                value.visit_object(&mut |name, _| {
                    if violation.is_none() && !fields.iter().any(|(field, _)| field == name) {
                        violation = Some(PayloadViolation::closed_unknown(
                            &field_path(path, name),
                            fields,
                        ));
                    }
                });
                if let Some(error) = violation {
                    return Err(error);
                }
            }
            Ok(())
        }
        SemanticSchema::Union(variants) => {
            let mut matches = 0;
            for variant in variants {
                if validate_at(variant, value, path, depth + 1).is_ok() {
                    matches += 1;
                }
            }
            if matches == 1 {
                Ok(())
            } else {
                Err(PayloadViolation::new(
                    path,
                    "exactly one union variant",
                    value.kind(),
                ))
            }
        }
        SemanticSchema::Map { key, value: item } => {
            if value.kind() != ValueKind::Object {
                return Err(PayloadViolation::shape(path, "map", value));
            }
            let mut violation = None;
            value.visit_object(&mut |name, item_value| {
                if violation.is_some() {
                    return;
                }
                let key_value = StringValue(name);
                violation = validate_at(key, &key_value, path, depth + 1)
                    .err()
                    .or_else(|| {
                        validate_at(item, item_value, &field_path(path, name), depth + 1).err()
                    });
            });
            violation.map_or(Ok(()), Err)
        }
        SemanticSchema::Record(item) => {
            if value.kind() != ValueKind::Object {
                return Err(PayloadViolation::shape(path, "record", value));
            }
            let mut violation = None;
            value.visit_object(&mut |name, item_value| {
                if violation.is_none() {
                    violation =
                        validate_at(item, item_value, &field_path(path, name), depth + 1).err();
                }
            });
            violation.map_or(Ok(()), Err)
        }
        SemanticSchema::Optional(item) => validate_at(item, value, path, depth + 1),
        SemanticSchema::Nullable(item) => {
            if value.kind() == ValueKind::Null {
                Ok(())
            } else {
                validate_at(item, value, path, depth + 1)
            }
        }
    }
}

fn require_kind(
    path: &str,
    value: &dyn ValueView,
    expected: ValueKind,
) -> Result<(), PayloadViolation> {
    if value.kind() == expected {
        Ok(())
    } else {
        Err(PayloadViolation::shape(path, expected.as_str(), value))
    }
}

fn validate_number(path: &str, value: &dyn ValueView) -> Result<(), PayloadViolation> {
    if value.kind() != ValueKind::Number {
        return Err(PayloadViolation::shape(path, "number", value));
    }
    match value.as_number() {
        Some(NumberValue::Float(number)) if number.is_finite() => Ok(()),
        Some(NumberValue::Signed(_) | NumberValue::Unsigned(_)) => Ok(()),
        _ => Err(PayloadViolation::shape(path, "finite number", value)),
    }
}

fn validate_i32(path: &str, value: &dyn ValueView) -> Result<(), PayloadViolation> {
    if value.kind() != ValueKind::Number {
        return Err(PayloadViolation::shape(path, "i32", value));
    }
    let valid = match value.as_number() {
        Some(NumberValue::Signed(number)) => i32::try_from(number).is_ok(),
        Some(NumberValue::Unsigned(number)) => i32::try_from(number).is_ok(),
        Some(NumberValue::Float(number)) => {
            number.is_finite()
                && number.fract() == 0.0
                && number >= i32::MIN as f64
                && number <= i32::MAX as f64
        }
        None => false,
    };
    if valid {
        Ok(())
    } else {
        Err(PayloadViolation::shape(path, "i32", value))
    }
}

fn validate_u32(path: &str, value: &dyn ValueView) -> Result<(), PayloadViolation> {
    if value.kind() != ValueKind::Number {
        return Err(PayloadViolation::shape(path, "u32", value));
    }
    let valid = match value.as_number() {
        Some(NumberValue::Signed(number)) => u32::try_from(number).is_ok(),
        Some(NumberValue::Unsigned(number)) => u32::try_from(number).is_ok(),
        Some(NumberValue::Float(number)) => {
            number.is_finite()
                && number.fract() == 0.0
                && number >= 0.0
                && number <= u32::MAX as f64
        }
        None => false,
    };
    if valid {
        Ok(())
    } else {
        Err(PayloadViolation::shape(path, "u32", value))
    }
}

fn field_path(parent: &str, segment: &str) -> String {
    format!("{parent}.{}", encode_path_segment(segment))
}

/// Presence columns use percent-encoded bytes for path segments. Percent is
/// escaped first so a literal `%2E` cannot be confused with a literal dot.
pub(crate) fn encode_path_segment(segment: &str) -> String {
    let mut output = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        match byte {
            b'.' => output.push_str("%2E"),
            b'%' => output.push_str("%25"),
            _ => output.push(byte as char),
        }
    }
    output
}

struct StringValue<'a>(&'a str);

impl ValueView for StringValue<'_> {
    fn kind(&self) -> ValueKind {
        ValueKind::String
    }

    fn as_str(&self) -> Option<&str> {
        Some(self.0)
    }
}

fn parse_schema(value: &JsonValue, depth: usize) -> Result<SemanticSchema, SchemaParseError> {
    if depth > MAX_SCHEMA_DEPTH {
        return Err(SchemaParseError::TooDeep);
    }
    let fields = object_fields(value)?;
    let kind = string_field(fields, "kind")?;
    match kind {
        "unknown" => Ok(SemanticSchema::Unknown),
        "string" => Ok(SemanticSchema::String),
        "number" => Ok(SemanticSchema::Number),
        "boolean" => Ok(SemanticSchema::Boolean),
        "binary" => Ok(SemanticSchema::Binary),
        "bigint" => Ok(SemanticSchema::BigInt),
        "i32" => Ok(SemanticSchema::I32),
        "u32" => Ok(SemanticSchema::U32),
        "enum" => Ok(SemanticSchema::Enum(parse_string_array(field(
            fields, "variants",
        ))?)),
        "array" => Ok(SemanticSchema::Array(Box::new(parse_schema(
            field(fields, "item").ok_or(SchemaParseError::MissingOperand)?,
            depth + 1,
        )?))),
        "object" => {
            let object_fields =
                object_fields(field(fields, "fields").ok_or(SchemaParseError::MissingOperand)?)?;
            let open = match field(fields, "open") {
                None => false,
                Some(JsonValue::Boolean(value)) => *value,
                Some(_) => return Err(SchemaParseError::InvalidOperand),
            };
            let mut parsed = Vec::with_capacity(object_fields.len());
            for (name, field_schema) in object_fields {
                if parsed.iter().any(|(known, _)| known == name) {
                    return Err(SchemaParseError::DuplicateField);
                }
                parsed.push((name.clone(), parse_schema(field_schema, depth + 1)?));
            }
            Ok(SemanticSchema::Object {
                fields: parsed,
                open,
            })
        }
        "union" => {
            let variants = parse_schema_array(field(fields, "variants"), depth)?;
            if variants.is_empty() {
                return Err(SchemaParseError::EmptyUnion);
            }
            Ok(SemanticSchema::Union(variants))
        }
        "map" => Ok(SemanticSchema::Map {
            key: Box::new(parse_schema(
                field(fields, "key").ok_or(SchemaParseError::MissingOperand)?,
                depth + 1,
            )?),
            value: Box::new(parse_schema(
                field(fields, "value").ok_or(SchemaParseError::MissingOperand)?,
                depth + 1,
            )?),
        }),
        "record" => Ok(SemanticSchema::Record(Box::new(parse_schema(
            field(fields, "value").ok_or(SchemaParseError::MissingOperand)?,
            depth + 1,
        )?))),
        "optional" => Ok(SemanticSchema::Optional(Box::new(parse_schema(
            field(fields, "value").ok_or(SchemaParseError::MissingOperand)?,
            depth + 1,
        )?))),
        "nullable" => Ok(SemanticSchema::Nullable(Box::new(parse_schema(
            field(fields, "value").ok_or(SchemaParseError::MissingOperand)?,
            depth + 1,
        )?))),
        _ => Err(SchemaParseError::UnknownKind),
    }
}

fn object_fields(value: &JsonValue) -> Result<&[(String, JsonValue)], SchemaParseError> {
    match value {
        JsonValue::Object(fields) => Ok(fields),
        _ => Err(SchemaParseError::InvalidTree),
    }
}

fn field<'a>(fields: &'a [(String, JsonValue)], name: &str) -> Option<&'a JsonValue> {
    fields
        .iter()
        .find(|(field, _)| field == name)
        .map(|(_, value)| value)
}

fn string_field<'a>(
    fields: &'a [(String, JsonValue)],
    name: &str,
) -> Result<&'a str, SchemaParseError> {
    match field(fields, name) {
        Some(JsonValue::String(value)) => Ok(value),
        Some(_) => Err(SchemaParseError::InvalidOperand),
        None => Err(SchemaParseError::MissingOperand),
    }
}

fn parse_string_array(value: Option<&JsonValue>) -> Result<Vec<String>, SchemaParseError> {
    let Some(JsonValue::Array(values)) = value else {
        return Err(value.map_or(SchemaParseError::MissingOperand, |_| {
            SchemaParseError::InvalidOperand
        }));
    };
    let mut output = Vec::with_capacity(values.len());
    for value in values {
        let JsonValue::String(value) = value else {
            return Err(SchemaParseError::InvalidOperand);
        };
        if output.iter().any(|known| known == value) {
            return Err(SchemaParseError::DuplicateField);
        }
        output.push(value.clone());
    }
    Ok(output)
}

fn parse_schema_array(
    value: Option<&JsonValue>,
    depth: usize,
) -> Result<Vec<SemanticSchema>, SchemaParseError> {
    let Some(JsonValue::Array(values)) = value else {
        return Err(value.map_or(SchemaParseError::MissingOperand, |_| {
            SchemaParseError::InvalidOperand
        }));
    };
    values
        .iter()
        .map(|value| parse_schema(value, depth + 1))
        .collect()
}

fn parse_json_value(
    parser: &mut JsonParser<'_>,
    depth: usize,
) -> Result<JsonValue, ValueParseError> {
    if depth > MAX_SCHEMA_DEPTH {
        return Err(ValueParseError::TooDeep);
    }
    match parser
        .next_token()
        .map_err(|_| ValueParseError::InvalidJson)?
    {
        Token::Null => Ok(JsonValue::Null),
        Token::True => Ok(JsonValue::Boolean(true)),
        Token::False => Ok(JsonValue::Boolean(false)),
        Token::String(value) => Ok(JsonValue::String(value)),
        Token::Number(value) => parse_json_number(&value),
        Token::ObjectBegin => {
            let mut fields = Vec::new();
            while !parser.is_object_end() {
                let name = parser
                    .expect_field_name()
                    .map_err(|_| ValueParseError::InvalidJson)?;
                let value = parse_json_value(parser, depth + 1)?;
                fields.push((name, value));
            }
            parser
                .next_token()
                .map_err(|_| ValueParseError::InvalidJson)
                .and_then(|token| match token {
                    Token::ObjectEnd => Ok(JsonValue::Object(fields)),
                    _ => Err(ValueParseError::InvalidJson),
                })
        }
        Token::ArrayBegin => {
            let mut values = Vec::new();
            while !parser.is_array_end() {
                values.push(parse_json_value(parser, depth + 1)?);
            }
            parser
                .next_token()
                .map_err(|_| ValueParseError::InvalidJson)
                .and_then(|token| match token {
                    Token::ArrayEnd => Ok(JsonValue::Array(values)),
                    _ => Err(ValueParseError::InvalidJson),
                })
        }
        Token::ObjectEnd | Token::ArrayEnd => Err(ValueParseError::InvalidJson),
    }
}

fn parse_json_number(value: &str) -> Result<JsonValue, ValueParseError> {
    if value.contains(['.', 'e', 'E']) {
        let number = value
            .parse::<f64>()
            .map_err(|_| ValueParseError::InvalidNumber)?;
        Ok(JsonValue::Number(NumberValue::Float(number)))
    } else if value.starts_with('-') {
        value
            .parse::<i128>()
            .map(|number| JsonValue::Number(NumberValue::Signed(number)))
            .map_err(|_| ValueParseError::InvalidNumber)
    } else if let Ok(number) = value.parse::<u128>() {
        Ok(JsonValue::Number(NumberValue::Unsigned(number)))
    } else {
        let number = value
            .parse::<f64>()
            .map_err(|_| ValueParseError::InvalidNumber)?;
        Ok(JsonValue::Number(NumberValue::Float(number)))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValueParseError {
    InvalidJson,
    InvalidMsgpack,
    InvalidNumber,
    TooDeep,
}

fn parse_msgpack_event(reader: &mut Reader<'_>) -> Result<JsonValue, ValueParseError> {
    let fields = reader
        .read_map_header()
        .ok_or(ValueParseError::InvalidMsgpack)?;
    let mut event = Vec::with_capacity(fields as usize);
    for _ in 0..fields {
        let key = reader
            .read_string()
            .ok_or(ValueParseError::InvalidMsgpack)?;
        let key = std::str::from_utf8(key).map_err(|_| ValueParseError::InvalidMsgpack)?;
        if key == "type" || key == "value" {
            event.push((key.to_owned(), parse_msgpack_value(reader, 0)?));
        } else {
            reader.skip_value().ok_or(ValueParseError::InvalidMsgpack)?;
        }
    }
    Ok(JsonValue::Object(event))
}

fn parse_msgpack_value(
    reader: &mut Reader<'_>,
    depth: usize,
) -> Result<JsonValue, ValueParseError> {
    if depth > MAX_SCHEMA_DEPTH {
        return Err(ValueParseError::TooDeep);
    }
    let first = *reader
        .input()
        .get(reader.position())
        .ok_or(ValueParseError::InvalidMsgpack)?;
    if Reader::is_integer(first) {
        if first == 0xcf {
            return reader
                .read_unsigned_integer()
                .map(|value| JsonValue::Number(NumberValue::Unsigned(u128::from(value))))
                .ok_or(ValueParseError::InvalidMsgpack);
        }
        return reader
            .read_integer()
            .map(|value| JsonValue::Number(NumberValue::Signed(i128::from(value))))
            .ok_or(ValueParseError::InvalidMsgpack);
    }
    if Reader::is_string(first) {
        let bytes = reader
            .read_string()
            .ok_or(ValueParseError::InvalidMsgpack)?;
        return Ok(match std::str::from_utf8(bytes) {
            Ok(value) => JsonValue::String(value.to_owned()),
            Err(_) => JsonValue::Binary(bytes.to_vec()),
        });
    }
    if Reader::is_float(first) {
        return reader
            .read_float()
            .map(|value| JsonValue::Number(NumberValue::Float(value)))
            .ok_or(ValueParseError::InvalidMsgpack);
    }
    match first {
        0xc0 => {
            reader.skip_value().ok_or(ValueParseError::InvalidMsgpack)?;
            Ok(JsonValue::Null)
        }
        0xc2 | 0xc3 => {
            reader.skip_value().ok_or(ValueParseError::InvalidMsgpack)?;
            Ok(JsonValue::Boolean(first == 0xc3))
        }
        0xc4..=0xc6 => reader
            .read_bin()
            .map(|value| JsonValue::Binary(value.to_vec()))
            .ok_or(ValueParseError::InvalidMsgpack),
        0x80..=0x8f | 0xde | 0xdf => {
            let count = reader
                .read_map_header()
                .ok_or(ValueParseError::InvalidMsgpack)?;
            let mut fields = Vec::with_capacity(count as usize);
            for _ in 0..count {
                let key = reader
                    .read_string()
                    .ok_or(ValueParseError::InvalidMsgpack)?;
                let key = std::str::from_utf8(key)
                    .map_err(|_| ValueParseError::InvalidMsgpack)?
                    .to_owned();
                fields.push((key, parse_msgpack_value(reader, depth + 1)?));
            }
            Ok(JsonValue::Object(fields))
        }
        0x90..=0x9f | 0xdc | 0xdd => {
            let count = reader
                .read_array_header()
                .ok_or(ValueParseError::InvalidMsgpack)?;
            let mut values = Vec::with_capacity(count as usize);
            for _ in 0..count {
                values.push(parse_msgpack_value(reader, depth + 1)?);
            }
            Ok(JsonValue::Array(values))
        }
        _ => Err(ValueParseError::InvalidMsgpack),
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn schema(tree: &str) -> SemanticSchema {
        SemanticSchema::from_tree_bytes(tree.as_bytes()).unwrap()
    }

    fn json(value: &str) -> JsonValue {
        JsonValue::parse(value.as_bytes()).unwrap()
    }

    #[test]
    fn declared_scalar_type_is_checked() {
        let result = validate_value(&schema(r#"{"kind":"string"}"#), &json("42"));
        assert_eq!(
            result,
            Err(PayloadViolation {
                path: "value".into(),
                expected: "string".into(),
                observed: "number".into(),
            })
        );
    }
    #[test]
    fn missing_required_and_optional_fields_are_distinct() {
        let required = schema(r#"{"kind":"object","fields":{"name":{"kind":"string"}}}"#);
        let optional = schema(
            r#"{"kind":"object","fields":{"name":{"kind":"optional","value":{"kind":"string"}}}}"#,
        );
        let empty = json("{}");
        let required_error = validate_value(&required, &empty).unwrap_err();
        assert_eq!(required_error.path, "value.name");
        assert_eq!(required_error.observed, "absent");
        assert!(validate_value(&optional, &empty).is_ok());
    }

    #[test]
    fn open_unknown_is_accepted_and_closed_unknown_is_refused() {
        let closed = schema(r#"{"kind":"object","fields":{"name":{"kind":"string"}}}"#);
        let open = schema(r#"{"kind":"object","fields":{"name":{"kind":"string"}},"open":true}"#);
        let value = json(r#"{"name":"ok","bogus_field":true}"#);
        let closed_error = validate_value(&closed, &value).unwrap_err();
        assert_eq!(closed_error.path, "value.bogus_field");
        assert_eq!(closed_error.observed, "undeclared");
        assert_eq!(
            closed_error.expected,
            format!(
                "not declared (declared: name); declare the enclosing object open — S.object({{…}}, {{ open: true }}) — to capture undeclared keys into {}",
                crate::UNDECLARED_COLUMN_NAME
            )
        );
        assert!(validate_value(&open, &value).is_ok());
    }

    #[test]
    fn nested_open_and_dot_keys_have_stable_paths() {
        let value_schema = schema(
            r#"{"kind":"object","fields":{"user":{"kind":"object","fields":{"id":{"kind":"string"}},"open":true}}}"#,
        );
        let value = json(r#"{"user":{"id":"u","nick.name":true}}"#);
        assert!(validate_value(&value_schema, &value).is_ok());
        let closed = schema(r#"{"kind":"object","fields":{}}"#);
        let dotted = json(r#"{"dot.key":1}"#);
        assert_eq!(
            validate_value(&closed, &dotted).unwrap_err().path,
            "value.dot%2Ekey"
        );
    }

    #[test]
    fn numeric_ranges_enum_and_exact_union_are_enforced() {
        let i32_schema = schema(r#"{"kind":"i32"}"#);
        assert!(validate_value(&i32_schema, &json("2147483647")).is_ok());
        assert!(validate_value(&i32_schema, &json("2147483648")).is_err());
        let u32_schema = schema(r#"{"kind":"u32"}"#);
        assert!(validate_value(&u32_schema, &json("4294967295")).is_ok());
        assert!(validate_value(&u32_schema, &json("4294967296")).is_err());
        let enum_schema = schema(r#"{"kind":"enum","variants":["a","b"]}"#);
        assert!(validate_value(&enum_schema, &json("\"a\"")).is_ok());
        assert!(validate_value(&enum_schema, &json("\"c\"")).is_err());
        let union = schema(r#"{"kind":"union","variants":[{"kind":"string"},{"kind":"number"}]}"#);
        assert!(validate_value(&union, &json("\"ok\"")).is_ok());
        assert!(validate_value(&union, &json("true")).is_err());
    }

    #[test]
    fn nullable_and_nested_collections_are_checked() {
        let schema = schema(
            r#"{"kind":"object","fields":{"items":{"kind":"array","item":{"kind":"nullable","value":{"kind":"string"}}},"labels":{"kind":"record","value":{"kind":"string"}}}}"#,
        );
        assert!(
            validate_value(
                &schema,
                &json(r#"{"items":[null,"ok"],"labels":{"a":"b"}}"#)
            )
            .is_ok()
        );
        assert_eq!(
            validate_value(&schema, &json(r#"{"items":[1],"labels":{}}"#))
                .unwrap_err()
                .path,
            "value.items[0]"
        );
    }

    #[test]
    fn malformed_schema_and_values_are_errors_not_panics() {
        assert_eq!(
            SemanticSchema::from_tree_bytes(br#"{"kind":"wat"}"#),
            Err(SchemaParseError::UnknownKind)
        );
        assert!(JsonValue::parse(br#"{"#).is_err());
    }

    #[test]
    fn shared_json_corpus_matches_validator() {
        let JsonValue::Array(vectors) =
            JsonValue::parse(include_bytes!("../../../validation-corpus.json")).unwrap()
        else {
            panic!("validation corpus must be an array");
        };
        for vector in vectors {
            let JsonValue::Object(fields) = vector else {
                panic!("validation corpus entry must be an object");
            };
            let tree = field(&fields, "schema").expect("schema");
            let value = field(&fields, "value").expect("value");
            let verdict = match field(&fields, "verdict").expect("verdict") {
                JsonValue::String(value) => value.as_str(),
                _ => panic!("verdict must be a string"),
            };
            let schema = SemanticSchema::from_tree_value(tree).unwrap();
            let result = validate_value(&schema, value);
            match verdict {
                "accept" | "capture" => {
                    assert!(result.is_ok(), "{verdict} vector refused: {result:?}")
                }
                "refuse" => {
                    let violation = result.expect_err("refuse vector accepted");
                    let expected_path = match field(&fields, "path").expect("path") {
                        JsonValue::String(value) => value,
                        _ => panic!("path must be a string"),
                    };
                    let expected = match field(&fields, "expected").expect("expected") {
                        JsonValue::String(value) => value,
                        _ => panic!("expected must be a string"),
                    };
                    let observed = match field(&fields, "observed").expect("observed") {
                        JsonValue::String(value) => value,
                        _ => panic!("observed must be a string"),
                    };
                    assert_eq!(&violation.path, expected_path);
                    assert_eq!(&violation.expected, expected);
                    assert_eq!(&violation.observed, observed);
                }
                other => panic!("unknown corpus verdict {other}"),
            }
        }
    }
}
