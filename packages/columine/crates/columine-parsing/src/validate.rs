//! Schema-layer validation for signal payload values.
//!
//! The validator deliberately knows nothing about Arrow or any concrete value
//! encoding. [`ValueView`] is the small read-only seam used by JSON, MessagePack,
//! and native adapters; keeping it unsealed lets downstream runtimes implement
//! the same judgment without creating a dependency back into those runtimes.
//!
//! Integers keep the representation their wire gave them. A JSON number token
//! and a MessagePack integer marker are numbers, the MessagePack bigint
//! extension is a bigint, and neither is ever reinterpreted as the other. An
//! exact 64-bit integer from a canonical codec that does not record whether it
//! began as a number or a bigint is [`ValueKind::CanonicalInt64`], proven by
//! [`ValueView::as_canonical_i64`]. Each numeric schema judges that exact value
//! against its own bounds: `bigint` accepts every `i64`, `number` only the
//! safe integral JavaScript numbers (`±(2^53 − 1)`), and `i32`/`u32` their
//! ranges. A union still requires exactly one matching variant, so a safe
//! integer under `number | bigint` is ambiguous while a wider one is a bigint.
//!
//! A `range` node bounds a number by finite closed bounds and judges integer
//! tokens and canonical int64 values exactly against them, never through a
//! rounded double. An `array` node may demand an exact length.
//!
//! A `vo` node carries canonical value-object metadata — a qualified root, a
//! frame table, and the manifest rows it depends on — decoded once from
//! lowercase hex. Decoding judges only that envelope's syntax; this crate has
//! no value-object kernel and does not pretend to verify one. Admission is
//! the shared [`VoAdmission`] capability the caller injects: it is prepared
//! once per declared metadata, and every frame is judged through
//! [`admit_frame`], which checks the capability's proof against the exact
//! borrowed payload and the declared root and frame.
//! [`UnavailableVoAdmission`] is the explicit refusal capability the ordinary
//! entry points use, so a `vo` schema is refused there rather than accepted
//! unchecked.
use std::{cmp::Ordering, collections::BTreeMap, fmt};

pub use columine_types::vo::{
    UnavailableVoAdmission, VoAdmission, VoAdmissionError, VoCountsRole, VoDefinition, VoFieldLink,
    VoFrameProof, VoMetadata, VoPreparationError, VoProfile, VoProgramMetadata, VoRefusal,
    VoSchemaRef, admit_frame, prepare_each,
};

use crate::{
    UNDECLARED_COLUMN_NAME,
    json_parser::{JsonParser, ParserError, Token},
    msgpack_scanner::Reader,
};

const MAX_SCHEMA_DEPTH: usize = 256;
const MAX_VALUE_DEPTH: usize = 256;
/// `Number.MAX_SAFE_INTEGER`: the widest integer a JavaScript number holds
/// exactly.
const MAX_SAFE_INTEGER: i64 = (1_i64 << 53) - 1;
const MAX_SAFE_INTEGER_F64: f64 = 9_007_199_254_740_991.0;

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
    /// An exact signed integer from a canonical codec that does not retain the
    /// originating host's number/bigint distinction. Never a JSON number token.
    CanonicalInt64,
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
            Self::CanonicalInt64 => "canonical int64",
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
/// as a type/shape violation rather than causing a panic. Reporting
/// [`ValueKind::CanonicalInt64`] without answering
/// [`ValueView::as_canonical_i64`] is such an adapter: the kind alone proves no
/// value, and no numeric schema accepts it.
pub trait ValueView {
    fn kind(&self) -> ValueKind;

    fn as_str(&self) -> Option<&str> {
        None
    }

    fn as_number(&self) -> Option<NumberValue> {
        None
    }

    /// The exact signed payload witnessing [`ValueKind::CanonicalInt64`].
    /// Adapters must not reconstruct it from a floating-point value or infer
    /// this representation from an ordinary JSON/MessagePack number token.
    fn as_canonical_i64(&self) -> Option<i64> {
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
/// data. Binary, bigint, and canonical int64 values remain representable for
/// MessagePack/native conformance tests even though canonical JSON cannot spell
/// them directly.
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
    /// Preserves a canonical codec's exact integer, independently of the
    /// schema that will judge it. JSON and MessagePack parsers never emit it.
    CanonicalInt64(i64),
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
            Self::CanonicalInt64(_) => ValueKind::CanonicalInt64,
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

    fn as_canonical_i64(&self) -> Option<i64> {
        match self {
            Self::CanonicalInt64(value) => Some(*value),
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
    /// A `vo` node's metadata is absent or malformed at the named member.
    /// Only the envelope's syntax is judged here; whether root, frame, and
    /// rows describe one coherent schema is the admission capability's call.
    InvalidVoMetadata(VoMetadataField),
    /// The decoded tree or metadata could not be backed.
    OutOfMemory,
}

/// The `vo` schema member that failed envelope decoding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VoMetadataField {
    /// Not two or more dot-separated identifier segments.
    Root,
    /// Not non-empty canonical lowercase hex.
    Frame,
    /// Not a non-empty array of non-empty canonical lowercase hex strings.
    Rows,
    /// Not an object of `records` and `attributes` lists of uniquely named
    /// `{name, definition}` entries, each definition canonical lowercase hex.
    Profile,
    /// Not an array of non-empty canonical lowercase hex strings.
    Retained,
}

/// Why a value failed the semantic schema judgment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PayloadViolation {
    pub path: String,
    pub expected: String,
    pub observed: String,
    /// The value-object admission cause, when the refusal came from the
    /// value-object boundary rather than from the value's shape.
    pub vo: Option<VoRefusal>,
}

impl PayloadViolation {
    fn new(path: impl Into<String>, expected: impl Into<String>, observed: ValueKind) -> Self {
        Self {
            path: path.into(),
            expected: expected.into(),
            observed: observed.as_str().to_owned(),
            vo: None,
        }
    }

    fn shape(path: &str, expected: &str, value: &dyn ValueView) -> Self {
        Self::new(path, expected, value.kind())
    }

    fn vo(path: &str, metadata: &VoMetadata, refusal: VoRefusal) -> Self {
        Self {
            vo: Some(refusal),
            ..Self::new(path, vo_expected(metadata), ValueKind::Binary)
        }
    }

    /// An event whose `type` names no declared signal type: the refusal
    /// every lane that admits events against a semantic schema gives it.
    pub fn undeclared_type() -> Self {
        Self::unmodeled("undeclared")
    }

    /// An event the judgment cannot place: its `type` is missing, not a
    /// string, or names no declared signal type.
    fn unmodeled(observed: &str) -> Self {
        Self {
            path: "type".into(),
            expected: "signal type declared by the semantic schema".into(),
            observed: observed.into(),
            vo: None,
        }
    }

    /// A top-level `value.<member>` key: the admission payload is the nested
    /// `value` object, and a flat member would reach its column unjudged.
    fn flat_payload_key(key: &str) -> Self {
        Self {
            path: key.to_owned(),
            expected: "payload member nested under value".into(),
            observed: "flat payload key".into(),
            vo: None,
        }
    }

    /// A key spelled twice in one object: the judgment reads one spelling and
    /// the extractor stores the other, so judged and stored bytes diverge.
    fn repeated_key(path: &str) -> Self {
        Self {
            path: path.to_owned(),
            expected: "each member spelled once".into(),
            observed: "repeated key".into(),
            vo: None,
        }
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
            vo: None,
        }
    }
}
impl fmt::Display for PayloadViolation {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            output,
            "{}: expected {}, got {}",
            self.path, self.expected, self.observed
        )?;
        match self.vo {
            Some(refusal) => write!(output, " ({refusal})"),
            None => Ok(()),
        }
    }
}

fn vo_expected(metadata: &VoMetadata) -> String {
    format!("vo {}", metadata.root)
}

/// Whether a schema set declares any `vo` node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VoPresence {
    Absent,
    Present,
}

/// Closed numeric bounds of a `range` node. Both bounds are finite and
/// ordered by construction, so equality on them is an equivalence; an
/// integral range holds safe JavaScript integers only.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct NumericRange {
    min: f64,
    max: f64,
    integer: bool,
}

impl Eq for NumericRange {}

impl NumericRange {
    pub fn new(min: f64, max: f64, integer: bool) -> Result<Self, SchemaParseError> {
        let bound = |value: f64| {
            value.is_finite()
                && (!integer || (value.fract() == 0.0 && value.abs() <= MAX_SAFE_INTEGER_F64))
        };
        if bound(min) && bound(max) && min <= max {
            Ok(Self { min, max, integer })
        } else {
            Err(SchemaParseError::InvalidOperand)
        }
    }

    pub const fn min(self) -> f64 {
        self.min
    }

    pub const fn max(self) -> f64 {
        self.max
    }

    pub const fn integer(self) -> bool {
        self.integer
    }

    fn contains_float(self, value: f64) -> bool {
        value.is_finite()
            && (!self.integer || value.fract() == 0.0)
            && self.min <= value
            && value <= self.max
    }

    /// An integer lies in the range when it lies between the bounds rounded
    /// inward to integers; both comparisons are exact.
    fn contains_integer(self, value: ExactInteger) -> bool {
        value.cmp_integral(self.min.ceil()) != Ordering::Less
            && value.cmp_integral(self.max.floor()) != Ordering::Greater
    }

    fn expected(self) -> String {
        let kind = if self.integer {
            "safe integer"
        } else {
            "finite number"
        };
        format!("{kind} in [{}, {}]", self.min, self.max)
    }
}

/// An integer token's exact value, compared with float bounds without first
/// rounding the integer through `f64`.
#[derive(Clone, Copy)]
struct ExactInteger {
    negative: bool,
    magnitude: u128,
}

impl ExactInteger {
    fn signed(value: i128) -> Self {
        Self {
            negative: value < 0,
            magnitude: value.unsigned_abs(),
        }
    }

    fn unsigned(value: u128) -> Self {
        Self {
            negative: false,
            magnitude: value,
        }
    }

    /// Order against an integral, finite `bound`.
    fn cmp_integral(self, bound: f64) -> Ordering {
        const TWO_POW_128: f64 = 340_282_366_920_938_463_463_374_607_431_768_211_456.0;
        let bound_negative = bound < 0.0;
        let bound_magnitude = bound.abs();
        if bound_magnitude >= TWO_POW_128 {
            return if bound_negative {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }
        // Integral and below 2^128, so this conversion is exact.
        let bound_magnitude = bound_magnitude as u128;
        match (self.negative, bound_negative) {
            (false, false) => self.magnitude.cmp(&bound_magnitude),
            (true, true) => bound_magnitude.cmp(&self.magnitude),
            (false, true) => Ordering::Greater,
            (true, false) => Ordering::Less,
        }
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
    /// A number inside closed finite bounds, integral when the range says so.
    Range(NumericRange),
    Enum(Vec<String>),
    /// Items of one schema; `length` demands exactly that many when present.
    Array {
        item: Box<Self>,
        length: Option<u32>,
    },
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
    /// One canonical value-object frame, admitted only through a
    /// [`VoAdmission`] capability prepared for this metadata.
    Vo(Box<VoMetadata>),
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
            Self::Range(_) => "range",
            Self::Enum(_) => "enum",
            Self::Array { .. } => "array",
            Self::Object { .. } => "object",
            Self::Union(_) => "union",
            Self::Map { .. } => "map",
            Self::Record(_) => "record",
            Self::Optional(_) => "optional",
            Self::Nullable(_) => "nullable",
            Self::Vo(_) => "vo",
        }
    }

    pub fn is_open_object(&self) -> bool {
        matches!(self, Self::Object { open: true, .. })
    }
}

/// Visit every top-level payload member declared as an enum (directly or
/// under `optional`/`nullable`) with its variants in ordinal order.
pub(crate) fn collect_enum_members(
    schema: &SemanticSchema,
    mut visit: impl FnMut(&str, &[String]),
) {
    let SemanticSchema::Object { fields, .. } = schema else {
        return;
    };
    for (name, field) in fields {
        let mut inner = field;
        while let SemanticSchema::Optional(next) | SemanticSchema::Nullable(next) = inner {
            inner = next;
        }
        if let SemanticSchema::Enum(variants) = inner {
            visit(name, variants);
        }
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
        SemanticSchema::Array { item, .. }
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
        | SemanticSchema::Range(_)
        | SemanticSchema::Enum(_)
        | SemanticSchema::Vo(_) => {}
    }
}

/// Every `vo` metadata below `schema`, repeats included.
fn collect_vo_metadata<'a>(schema: &'a SemanticSchema, output: &mut Vec<&'a VoMetadata>) {
    match schema {
        SemanticSchema::Vo(metadata) => output.push(metadata),
        SemanticSchema::Object { fields, .. } => {
            for (_, child) in fields {
                collect_vo_metadata(child, output);
            }
        }
        SemanticSchema::Array { item, .. }
        | SemanticSchema::Record(item)
        | SemanticSchema::Optional(item)
        | SemanticSchema::Nullable(item) => collect_vo_metadata(item, output),
        SemanticSchema::Map { key, value } => {
            collect_vo_metadata(key, output);
            collect_vo_metadata(value, output);
        }
        SemanticSchema::Union(variants) => {
            for variant in variants {
                collect_vo_metadata(variant, output);
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
        | SemanticSchema::Range(_)
        | SemanticSchema::Enum(_) => {}
    }
}

/// Judge one value against one semantic schema with no value-object
/// capability: any `vo` node is refused as [`VoAdmissionError::Unavailable`].
///
/// Unknown members below an open object are intentionally not traversed: the
/// extractor owns bounded carrier capture, while this function owns only the
/// admission judgment. No mutation, deduplication, or output allocation is
/// performed here beyond diagnostics for the first violation.
pub fn validate_value<V: ValueView>(
    schema: &SemanticSchema,
    value: &V,
) -> Result<(), PayloadViolation> {
    validate_value_with_admission(schema, value, &UnavailableVoAdmission)
}

/// [`validate_value`] with a value-object capability already prepared for
/// every `vo` node of `schema`.
pub fn validate_value_with_admission<V: ValueView, A: VoAdmission>(
    schema: &SemanticSchema,
    value: &V,
    admission: &A,
) -> Result<(), PayloadViolation> {
    validate_at(schema, value, "value", 0, &mut JudgeOnly(admission))
}

/// Where the judgment sends each value-object frame it reaches. A judgment
/// that tries several union variants marks the sink before each one and
/// rolls back what a refused variant admitted.
trait FrameAdmission<'s> {
    fn admit(&mut self, metadata: &'s VoMetadata, frame: &[u8]) -> Result<(), VoRefusal>;
    fn mark(&self) -> usize;
    fn rollback(&mut self, mark: usize);
}

/// Admits each frame and keeps nothing: the judgment alone.
struct JudgeOnly<'a, A>(&'a A);

impl<'s, A: VoAdmission> FrameAdmission<'s> for JudgeOnly<'_, A> {
    fn admit(&mut self, metadata: &'s VoMetadata, frame: &[u8]) -> Result<(), VoRefusal> {
        admit_frame(self.0, metadata, frame).map(drop)
    }

    fn mark(&self) -> usize {
        0
    }

    fn rollback(&mut self, _mark: usize) {}
}

/// Admits each frame as a slice of the batch and keeps every proof whose
/// certified bytes are a projection, for the extractor to write in place of
/// the lent slice. A frame in the reader's own table keeps nothing: the
/// extractor reads it from the batch.
struct RetainProjections<'x, A: VoAdmission + 'x> {
    admission: &'x A,
    input: &'x [u8],
    projected: Vec<A::Frame<'x>>,
}

impl<'s: 'x, 'x, A: VoAdmission + 'x> FrameAdmission<'s> for RetainProjections<'x, A> {
    fn admit(&mut self, metadata: &'s VoMetadata, frame: &[u8]) -> Result<(), VoRefusal> {
        let Some(lent) = batch_slice(self.input, frame) else {
            columine_types::die!("a MessagePack value-object frame is a slice of its batch")
        };
        let proof = admit_frame(self.admission, metadata, lent)?;
        if !core::ptr::eq(proof.bytes(), proof.lent()) {
            self.projected.push(proof);
        }
        Ok(())
    }

    fn mark(&self) -> usize {
        self.projected.len()
    }

    fn rollback(&mut self, mark: usize) {
        self.projected.truncate(mark);
    }
}

/// `frame` as the slice of `input` it occupies — the same address and
/// length — or `None` when it lies elsewhere.
fn batch_slice<'i>(input: &'i [u8], frame: &[u8]) -> Option<&'i [u8]> {
    let start = frame.as_ptr().addr().checked_sub(input.as_ptr().addr())?;
    let slice = input.get(start..start.checked_add(frame.len())?)?;
    core::ptr::eq(slice, frame).then_some(slice)
}

/// Semantic schemas keyed by event type as carried at EP creation.
pub type SemanticSchemaSet = BTreeMap<String, SemanticSchema>;

/// A validation failure tied to one event in an input batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BatchValidationError {
    InvalidInput,
    Violation {
        event_index: usize,
        /// The event's signal type, when it named one.
        event_type: Option<String>,
        violation: Box<PayloadViolation>,
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

/// A semantic envelope whose value-object capability is prepared for every
/// `vo` node it declares. [`PreparedSchemas::prepare`] is the only way to
/// build one, so no batch is ever judged against an unprepared capability.
///
/// Every event must name a signal type the envelope declares: an unknown or
/// missing type is a refusal, never a pass. An admission payload is the
/// nested event — the payload is one `value` object, the form the judgment
/// reads — so a flat `value.<member>` key is refused: it would reach its
/// column without being judged. The flat spelling belongs to trusted,
/// post-admission rows, which are never judged here.
/// A key spelled twice in one judged object is refused for the same reason:
/// the judgment reads one spelling and the extractor stores another.
pub struct PreparedSchemas<A> {
    schemas: SemanticSchemaSet,
    admission: A,
    presence: VoPresence,
}

impl<A: VoAdmission> PreparedSchemas<A> {
    /// Prepare `admission` once for every distinct `vo` metadata declared
    /// anywhere in `schemas`. The first refusal stops preparation and names
    /// its root; a set with no `vo` node never consults the capability.
    pub fn prepare(
        schemas: SemanticSchemaSet,
        mut admission: A,
    ) -> Result<Self, VoPreparationError> {
        let mut declared = Vec::new();
        for schema in schemas.values() {
            collect_vo_metadata(schema, &mut declared);
        }
        let prepared =
            prepare_each(&mut admission, declared.iter().copied()).map_err(|refused| {
                VoPreparationError {
                    root: declared[refused.index].root.clone(),
                    error: refused.error,
                }
            })?;
        let presence = match prepared {
            0 => VoPresence::Absent,
            _ => VoPresence::Present,
        };
        Ok(Self {
            schemas,
            admission,
            presence,
        })
    }

    pub fn schemas(&self) -> &SemanticSchemaSet {
        &self.schemas
    }

    pub fn presence(&self) -> VoPresence {
        self.presence
    }

    pub fn admission(&self) -> &A {
        &self.admission
    }

    /// Validate a JSON batch: an array of event objects. JSON cannot spell a
    /// binary frame, so no value-object frame is ever admitted here.
    pub fn validate_json_batch(&self, input: &[u8]) -> Result<(), BatchValidationError> {
        let batch = JsonValue::parse(input).map_err(|_| BatchValidationError::InvalidInput)?;
        let JsonValue::Array(events) = batch else {
            return Err(BatchValidationError::InvalidInput);
        };
        self.validate_events(&events, &mut JudgeOnly(&self.admission))
    }

    /// Validate a MessagePack batch — an array of event maps, or a stream of
    /// them. Strings and binary payloads stay borrowed from `input`, so a
    /// value-object frame is admitted as the wire bytes themselves. Returns
    /// the proofs whose certified bytes are a projection into the reader's
    /// layout, each naming the batch slice it replaces; the extractor writes
    /// those bytes, so no frame is admitted twice. This consumes no
    /// extraction columns or dedup state.
    pub fn validate_msgpack_batch<'a>(
        &'a self,
        input: &'a [u8],
        stream: bool,
    ) -> Result<Vec<A::Frame<'a>>, BatchValidationError> {
        if input.is_empty() {
            return Ok(Vec::new());
        }
        let mut reader = Reader::new(input);
        let mut events = Vec::new();
        if stream {
            while !reader.at_end() {
                events.push(
                    parse_msgpack_event(&mut reader)
                        .map_err(|_| BatchValidationError::InvalidInput)?,
                );
            }
        } else {
            let count = reader
                .read_array_header()
                .ok_or(BatchValidationError::InvalidInput)?;
            for _ in 0..count {
                events.push(
                    parse_msgpack_event(&mut reader)
                        .map_err(|_| BatchValidationError::InvalidInput)?,
                );
            }
            if !reader.at_end() {
                return Err(BatchValidationError::InvalidInput);
            }
        }
        let mut frames = RetainProjections {
            admission: &self.admission,
            input,
            projected: Vec::new(),
        };
        self.validate_events(&events, &mut frames)?;
        Ok(frames.projected)
    }

    fn validate_events<'s, V: ValueView>(
        &'s self,
        events: &[V],
        frames: &mut dyn FrameAdmission<'s>,
    ) -> Result<(), BatchValidationError> {
        for (event_index, event) in events.iter().enumerate() {
            if event.kind() != ValueKind::Object {
                return Err(BatchValidationError::InvalidInput);
            }
            let type_value = event.object_field("type");
            let event_type = type_value
                .filter(|value| value.kind() == ValueKind::String)
                .and_then(|value| value.as_str());
            let refuse = |violation| BatchValidationError::Violation {
                event_index,
                event_type: event_type.map(str::to_owned),
                violation: Box::new(violation),
            };
            // A repeated `type` or `value` is judged by one spelling and stored
            // from another, so the judged and stored bytes diverge.
            if let Some(name) = ["type", "value"]
                .into_iter()
                .find(|name| key_count(event, name) > 1)
            {
                return Err(refuse(PayloadViolation::repeated_key(name)));
            }
            let Some(schema) = event_type.and_then(|name| self.schemas.get(name)) else {
                return Err(refuse(match (event_type, type_value) {
                    (Some(_), _) => PayloadViolation::undeclared_type(),
                    (None, Some(value)) => PayloadViolation::unmodeled(value.kind().as_str()),
                    (None, None) => PayloadViolation::unmodeled("absent"),
                }));
            };
            refuse_flat_payload_keys(event).map_err(refuse)?;
            let value = event.object_field("value").unwrap_or(&AbsentValue);
            validate_at(schema, value, "value", 0, frames).map_err(refuse)?;
        }
        Ok(())
    }
}

/// The admission shape is nested: a top-level `value.<member>` key is a
/// payload member the judgment never sees.
fn refuse_flat_payload_keys(event: &dyn ValueView) -> Result<(), PayloadViolation> {
    let mut violation = None;
    event.visit_object(&mut |key, _| {
        if violation.is_none() && key.starts_with("value.") {
            violation = Some(PayloadViolation::flat_payload_key(key));
        }
    });
    violation.map_or(Ok(()), Err)
}

/// How many members of `object` are spelled `name`.
fn key_count(object: &dyn ValueView, name: &str) -> usize {
    let mut count = 0;
    object.visit_object(&mut |key, _| count += usize::from(key == name));
    count
}

/// Stands in for an event's absent `value` member.
struct AbsentValue;

impl ValueView for AbsentValue {
    fn kind(&self) -> ValueKind {
        ValueKind::Unknown
    }
}

fn validate_at<'s>(
    schema: &'s SemanticSchema,
    value: &dyn ValueView,
    path: &str,
    depth: usize,
    frames: &mut dyn FrameAdmission<'s>,
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
        SemanticSchema::BigInt => validate_bigint(path, value),
        SemanticSchema::Number => validate_number(path, value),
        SemanticSchema::I32 => validate_i32(path, value),
        SemanticSchema::U32 => validate_u32(path, value),
        SemanticSchema::Range(range) => validate_range(path, *range, value),
        SemanticSchema::Vo(metadata) => validate_vo(path, metadata, value, frames),
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
        SemanticSchema::Array { item, length } => {
            if value.kind() != ValueKind::Array {
                return Err(PayloadViolation::shape(path, "array", value));
            }
            let mut count = 0_u64;
            let mut violation = None;
            value.visit_array(&mut |index, item_value| {
                count += 1;
                if violation.is_some() {
                    return;
                }
                let item_path = format!("{path}[{index}]");
                violation = validate_at(item, item_value, &item_path, depth + 1, frames).err();
            });
            if let Some(expected) = *length
                && count != u64::from(expected)
            {
                return Err(PayloadViolation {
                    path: path.to_owned(),
                    expected: format!("array of length {expected}"),
                    observed: format!("array of length {count}"),
                    vo: None,
                });
            }
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
                            vo: None,
                        });
                    }
                    continue;
                };
                let child_path = field_path(path, name);
                if key_count(value, name) > 1 {
                    return Err(PayloadViolation::repeated_key(&child_path));
                }
                validate_at(field_schema, field_value, &child_path, depth + 1, frames)?;
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
            // A union that admits nothing keeps the first value-object cause
            // so a capability refusal stays observable through the union.
            let mut vo = None;
            for variant in variants {
                let mark = frames.mark();
                match validate_at(variant, value, path, depth + 1, frames) {
                    Ok(()) => matches += 1,
                    Err(violation) => {
                        frames.rollback(mark);
                        vo = vo.or(violation.vo);
                    }
                }
            }
            if matches == 1 {
                Ok(())
            } else {
                Err(PayloadViolation {
                    vo: if matches == 0 { vo } else { None },
                    ..PayloadViolation::new(path, "exactly one union variant", value.kind())
                })
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
                violation = validate_at(key, &key_value, path, depth + 1, frames)
                    .err()
                    .or_else(|| {
                        validate_at(item, item_value, &field_path(path, name), depth + 1, frames)
                            .err()
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
                        validate_at(item, item_value, &field_path(path, name), depth + 1, frames)
                            .err();
                }
            });
            violation.map_or(Ok(()), Err)
        }
        SemanticSchema::Optional(item) => validate_at(item, value, path, depth + 1, frames),
        SemanticSchema::Nullable(item) => {
            if value.kind() == ValueKind::Null {
                Ok(())
            } else {
                validate_at(item, value, path, depth + 1, frames)
            }
        }
    }
}

/// A value-object payload is binary and admitted through the sink, which
/// checks the capability's proof names exactly the borrowed bytes under the
/// declared root and frame table.
fn validate_vo<'s>(
    path: &str,
    metadata: &'s VoMetadata,
    value: &dyn ValueView,
    frames: &mut dyn FrameAdmission<'s>,
) -> Result<(), PayloadViolation> {
    let bytes = match (value.kind(), value.as_bytes()) {
        (ValueKind::Binary, Some(bytes)) => bytes,
        _ => {
            return Err(PayloadViolation::new(
                path,
                vo_expected(metadata),
                value.kind(),
            ));
        }
    };
    frames
        .admit(metadata, bytes)
        .map_err(|refusal| PayloadViolation::vo(path, metadata, refusal))
}

/// A range admits numbers exactly as `number` does — any finite JSON or
/// MessagePack number, and a canonical int64 only within the safe-integer
/// range — and then requires the value inside its bounds. Integer tokens and
/// canonical int64 values are compared exactly, never through a double.
fn validate_range(
    path: &str,
    range: NumericRange,
    value: &dyn ValueView,
) -> Result<(), PayloadViolation> {
    let admitted = match value.kind() {
        ValueKind::Number => match value.as_number() {
            Some(NumberValue::Float(number)) => range.contains_float(number),
            Some(NumberValue::Signed(number)) => {
                range.contains_integer(ExactInteger::signed(number))
            }
            Some(NumberValue::Unsigned(number)) => {
                range.contains_integer(ExactInteger::unsigned(number))
            }
            None => false,
        },
        ValueKind::CanonicalInt64 => value.as_canonical_i64().is_some_and(|number| {
            (-MAX_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(&number)
                && range.contains_integer(ExactInteger::signed(i128::from(number)))
        }),
        _ => false,
    };
    if admitted {
        Ok(())
    } else {
        Err(PayloadViolation::new(path, range.expected(), value.kind()))
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

/// A bigint is a native bigint where the wire has one, and otherwise its
/// decimal string: canonical JSON cannot spell a bigint, so the JSON lane
/// carries `"5000000000"`, the bigint-as-string form both extractors accept
/// on the eight-byte planes. A JSON number is refused by the shared corpus
/// (`bigint-wrong-number`): a number that fits a double is not evidence the
/// author meant a bigint, and one that does not fit has already lost bits.
///
/// A canonical int64 is also sufficient evidence: its adapter retained every
/// bit, but the canonical codec erased the originating host's numeric type.
fn validate_bigint(path: &str, value: &dyn ValueView) -> Result<(), PayloadViolation> {
    match value.kind() {
        ValueKind::BigInt => Ok(()),
        ValueKind::CanonicalInt64 if value.as_canonical_i64().is_some() => Ok(()),
        ValueKind::String => {
            let text = value.as_str().unwrap_or_default();
            let digits = text.strip_prefix('-').unwrap_or(text);
            let decimal = !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit());
            if decimal && (text.parse::<i64>().is_ok() || text.parse::<u64>().is_ok()) {
                Ok(())
            } else {
                Err(PayloadViolation::shape(path, "bigint", value))
            }
        }
        _ => Err(PayloadViolation::shape(path, "bigint", value)),
    }
}

/// A number is any finite JSON/MessagePack number. A canonical int64 is a
/// number only within the safe-integer range `±(2^53 − 1)`; wider values fit
/// `bigint` alone, even when an individual value is representable as a double.
fn validate_number(path: &str, value: &dyn ValueView) -> Result<(), PayloadViolation> {
    let kind = value.kind();
    if !matches!(kind, ValueKind::Number | ValueKind::CanonicalInt64) {
        return Err(PayloadViolation::shape(path, "number", value));
    }
    if kind == ValueKind::CanonicalInt64 {
        return match value.as_canonical_i64() {
            Some(number) if (-MAX_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(&number) => Ok(()),
            _ => Err(PayloadViolation::shape(path, "safe number", value)),
        };
    }
    match value.as_number() {
        Some(NumberValue::Float(number)) if number.is_finite() => Ok(()),
        Some(NumberValue::Signed(_) | NumberValue::Unsigned(_)) => Ok(()),
        _ => Err(PayloadViolation::shape(path, "finite number", value)),
    }
}

/// Numeric schemas share the same exact canonical integer; each applies its
/// own bounds rather than first rounding through a floating-point number.
fn numeric_value(value: &dyn ValueView) -> Option<NumberValue> {
    match value.kind() {
        ValueKind::Number => value.as_number(),
        ValueKind::CanonicalInt64 => value
            .as_canonical_i64()
            .map(|number| NumberValue::Signed(i128::from(number))),
        _ => None,
    }
}

fn validate_i32(path: &str, value: &dyn ValueView) -> Result<(), PayloadViolation> {
    let valid = match numeric_value(value) {
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
    let valid = match numeric_value(value) {
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
    let mut output = Vec::with_capacity(encoded_segment_len(segment));
    encode_segment_into(segment, |chunk| output.extend_from_slice(chunk));
    // Splitting only at the ASCII `.` and `%` keeps every chunk UTF-8.
    String::from_utf8(output).unwrap_or_else(|_| unreachable!("ASCII splits keep UTF-8"))
}

/// The length of `segment`'s carrier spelling.
pub(crate) fn encoded_segment_len(segment: &str) -> usize {
    segment.len()
        + 2 * segment
            .bytes()
            .filter(|byte| matches!(byte, b'.' | b'%'))
            .count()
}

/// Feed `segment`'s carrier spelling to `emit`: `.` as `%2E`, `%` as `%25`,
/// and every other byte, a non-ASCII one included, as itself.
pub(crate) fn encode_segment_into(segment: &str, mut emit: impl FnMut(&[u8])) {
    let bytes = segment.as_bytes();
    let mut start = 0;
    for (index, byte) in bytes.iter().enumerate() {
        let escape: &[u8] = match byte {
            b'.' => b"%2E",
            b'%' => b"%25",
            _ => continue,
        };
        emit(&bytes[start..index]);
        emit(escape);
        start = index + 1;
    }
    emit(&bytes[start..]);
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
        "range" => Ok(SemanticSchema::Range(parse_range(fields)?)),
        "array" => Ok(SemanticSchema::Array {
            item: Box::new(parse_schema(
                field(fields, "item").ok_or(SchemaParseError::MissingOperand)?,
                depth + 1,
            )?),
            length: parse_array_length(field(fields, "length"))?,
        }),
        "vo" => Ok(SemanticSchema::Vo(Box::new(parse_vo_metadata(fields)?))),
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

fn parse_range(fields: &[(String, JsonValue)]) -> Result<NumericRange, SchemaParseError> {
    let integer = match field(fields, "integer") {
        Some(JsonValue::Boolean(integer)) => *integer,
        Some(_) => return Err(SchemaParseError::InvalidOperand),
        None => return Err(SchemaParseError::MissingOperand),
    };
    NumericRange::new(
        range_bound(field(fields, "min"))?,
        range_bound(field(fields, "max"))?,
        integer,
    )
}

/// A bound is any JSON number token, read as the nearest double — the value
/// JavaScript parses the same token to. [`NumericRange::new`] then demands a
/// finite, ordered pair and, for an integral range, safe integers, inside
/// which this read is exact.
fn range_bound(value: Option<&JsonValue>) -> Result<f64, SchemaParseError> {
    match value {
        Some(JsonValue::Number(NumberValue::Float(bound))) => Ok(*bound),
        Some(JsonValue::Number(NumberValue::Signed(bound))) => Ok(*bound as f64),
        Some(JsonValue::Number(NumberValue::Unsigned(bound))) => Ok(*bound as f64),
        Some(_) => Err(SchemaParseError::InvalidOperand),
        None => Err(SchemaParseError::MissingOperand),
    }
}

/// An exact length is an unsigned integer token that fits `u32`.
fn parse_array_length(value: Option<&JsonValue>) -> Result<Option<u32>, SchemaParseError> {
    value.map(|length| unsigned(Some(length))).transpose()
}

/// An exact unsigned integer token that fits `T`. A signed, fractional, or
/// out-of-range token is never narrowed into one.
fn unsigned<T: TryFrom<u128>>(value: Option<&JsonValue>) -> Result<T, SchemaParseError> {
    match value {
        Some(JsonValue::Number(NumberValue::Unsigned(number))) => {
            T::try_from(*number).map_err(|_| SchemaParseError::InvalidOperand)
        }
        Some(_) => Err(SchemaParseError::InvalidOperand),
        None => Err(SchemaParseError::MissingOperand),
    }
}

fn array(value: Option<&JsonValue>) -> Result<&[JsonValue], SchemaParseError> {
    match value {
        Some(JsonValue::Array(values)) => Ok(values),
        Some(_) => Err(SchemaParseError::InvalidOperand),
        None => Err(SchemaParseError::MissingOperand),
    }
}

/// An object carrying every `required` member, members from `allowed` only,
/// and each at most once.
fn exact_members<'a>(
    value: &'a JsonValue,
    required: &[&str],
    allowed: &[&str],
) -> Result<&'a [(String, JsonValue)], SchemaParseError> {
    let fields = object_fields(value)?;
    for (index, (name, _)) in fields.iter().enumerate() {
        if !allowed.contains(&name.as_str()) {
            return Err(SchemaParseError::InvalidOperand);
        }
        if fields[..index].iter().any(|(known, _)| known == name) {
            return Err(SchemaParseError::DuplicateField);
        }
    }
    if required.iter().any(|name| field(fields, name).is_none()) {
        return Err(SchemaParseError::MissingOperand);
    }
    Ok(fields)
}

/// Decode the value-object declarations a compiled program carries beside
/// its bytecode, once, from canonical JSON:
///
/// ```text
/// {"fields": [{"slot": u32, "field_idx": u8, "schema": <vo metadata>}],
///  "counts": [{"instruction_offset": u32, "count_field": u16,
///              "histogram_fields": [u16], "mask_fields": [u16]}]}
/// ```
///
/// Both lists are required (either may be empty) and every object carries
/// exactly its members. A schema is the members of a `vo` node — `root`,
/// `frame`, `rows`, optional `profile` and `retained` — decoded by the same
/// rules. `instruction_offset` is the counts opcode's byte offset in the whole
/// program buffer. Whether each link and role names a real field and
/// instruction, once, is the program loader's check against the program.
///
/// Every allocation is backed fallibly: memory that cannot hold the decoded
/// tree or the metadata refuses with [`SchemaParseError::OutOfMemory`].
pub fn parse_program_metadata(bytes: &[u8]) -> Result<VoProgramMetadata, SchemaParseError> {
    let value = JsonValue::parse(bytes).map_err(|error| match error {
        ValueParseError::OutOfMemory => SchemaParseError::OutOfMemory,
        _ => SchemaParseError::InvalidJson,
    })?;
    let program = exact_members(&value, &["fields", "counts"], &["fields", "counts"])?;
    let fields = each(array(field(program, "fields"))?, parse_field_link)?;
    let counts = each(array(field(program, "counts"))?, parse_counts_role)?;
    Ok(VoProgramMetadata { fields, counts })
}

/// `parse` applied to every item, into exactly backed storage.
fn each<T, U>(
    items: &[T],
    parse: impl Fn(&T) -> Result<U, SchemaParseError>,
) -> Result<Vec<U>, SchemaParseError> {
    let mut out = Vec::new();
    out.try_reserve_exact(items.len())
        .map_err(|_| SchemaParseError::OutOfMemory)?;
    for item in items {
        out.push(parse(item)?);
    }
    Ok(out)
}

/// Why a metadata member could not be decoded: it is malformed, or its
/// storage could not be backed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Decoding {
    Invalid,
    OutOfMemory,
}

impl Decoding {
    /// The refusal for `member`.
    fn at(self, member: VoMetadataField) -> SchemaParseError {
        match self {
            Self::Invalid => SchemaParseError::InvalidVoMetadata(member),
            Self::OutOfMemory => SchemaParseError::OutOfMemory,
        }
    }
}

/// An empty vector backed for `len` elements.
fn backed<T>(len: usize) -> Result<Vec<T>, Decoding> {
    let mut out = Vec::new();
    out.try_reserve_exact(len)
        .map_err(|_| Decoding::OutOfMemory)?;
    Ok(out)
}

/// An owned copy of `text` in exactly backed storage.
fn owned_text(text: &str) -> Result<String, Decoding> {
    let mut out = String::new();
    out.try_reserve_exact(text.len())
        .map_err(|_| Decoding::OutOfMemory)?;
    out.push_str(text);
    Ok(out)
}

fn parse_field_link(value: &JsonValue) -> Result<VoFieldLink, SchemaParseError> {
    const MEMBERS: &[&str] = &["slot", "field_idx", "schema"];
    let link = exact_members(value, MEMBERS, MEMBERS)?;
    let schema = exact_members(
        field(link, "schema").ok_or(SchemaParseError::MissingOperand)?,
        &[],
        &["root", "frame", "rows", "profile", "retained"],
    )?;
    Ok(VoFieldLink {
        slot: unsigned(field(link, "slot"))?,
        field_idx: unsigned(field(link, "field_idx"))?,
        schema: parse_vo_metadata(schema)?,
    })
}

fn parse_counts_role(value: &JsonValue) -> Result<VoCountsRole, SchemaParseError> {
    const MEMBERS: &[&str] = &[
        "instruction_offset",
        "count_field",
        "histogram_fields",
        "mask_fields",
    ];
    let role = exact_members(value, MEMBERS, MEMBERS)?;
    let field_ids = |name: &str| -> Result<Vec<u16>, SchemaParseError> {
        each(array(field(role, name))?, |id| unsigned(Some(id)))
    };
    Ok(VoCountsRole {
        instruction_offset: unsigned(field(role, "instruction_offset"))?,
        count_field: unsigned(field(role, "count_field"))?,
        histogram_fields: field_ids("histogram_fields")?,
        mask_fields: field_ids("mask_fields")?,
    })
}

fn parse_vo_metadata(fields: &[(String, JsonValue)]) -> Result<VoMetadata, SchemaParseError> {
    let root = match field(fields, "root") {
        Some(JsonValue::String(root)) if is_qualified_root(root) => owned_text(root),
        _ => Err(Decoding::Invalid),
    }
    .map_err(|error| error.at(VoMetadataField::Root))?;
    let frame = match field(fields, "frame") {
        Some(JsonValue::String(hex)) => decode_lowercase_hex(hex),
        _ => Err(Decoding::Invalid),
    }
    .map_err(|error| error.at(VoMetadataField::Frame))?;
    let rows = match field(fields, "rows") {
        Some(JsonValue::Array(rows)) if !rows.is_empty() => decode_hex_rows(rows),
        _ => Err(Decoding::Invalid),
    }
    .map_err(|error| error.at(VoMetadataField::Rows))?;
    let profile = match field(fields, "profile") {
        None => Ok(VoProfile::default()),
        Some(JsonValue::Object(profile)) => parse_vo_profile(profile),
        Some(_) => Err(Decoding::Invalid),
    }
    .map_err(|error| error.at(VoMetadataField::Profile))?;
    let retained = match field(fields, "retained") {
        None => Ok(Vec::new()),
        Some(JsonValue::Array(rows)) => decode_hex_rows(rows),
        Some(_) => Err(Decoding::Invalid),
    }
    .map_err(|error| error.at(VoMetadataField::Retained))?;
    Ok(VoMetadata {
        root,
        frame,
        rows,
        profile,
        retained,
    })
}

/// Every row as non-empty canonical lowercase hex, decoded.
fn decode_hex_rows(rows: &[JsonValue]) -> Result<Vec<Vec<u8>>, Decoding> {
    let mut decoded = backed(rows.len())?;
    for row in rows {
        decoded.push(match row {
            JsonValue::String(hex) => decode_lowercase_hex(hex)?,
            _ => return Err(Decoding::Invalid),
        });
    }
    Ok(decoded)
}

/// `{"records": [...], "attributes": [...]}`; an absent list is empty. Each
/// entry is `{"name": <non-empty>, "definition": <lowercase hex>}`, names
/// unique within their list. A definition is the canonical vo1 bytes its
/// commitment is computed over, decoded and carried verbatim: verifying them
/// against the rows' commitments is the capability's call.
fn parse_vo_profile(fields: &[(String, JsonValue)]) -> Result<VoProfile, Decoding> {
    if fields
        .iter()
        .any(|(name, _)| name != "records" && name != "attributes")
    {
        return Err(Decoding::Invalid);
    }
    Ok(VoProfile {
        records: parse_vo_definitions(field(fields, "records"))?,
        attributes: parse_vo_definitions(field(fields, "attributes"))?,
    })
}

fn parse_vo_definitions(value: Option<&JsonValue>) -> Result<Vec<VoDefinition>, Decoding> {
    let entries = match value {
        None => return Ok(Vec::new()),
        Some(JsonValue::Array(entries)) => entries,
        Some(_) => return Err(Decoding::Invalid),
    };
    let mut definitions: Vec<VoDefinition> = backed(entries.len())?;
    for entry in entries {
        let JsonValue::Object(entry) = entry else {
            return Err(Decoding::Invalid);
        };
        if entry.len() != 2 {
            return Err(Decoding::Invalid);
        }
        let (Some(JsonValue::String(name)), Some(JsonValue::String(definition))) =
            (field(entry, "name"), field(entry, "definition"))
        else {
            return Err(Decoding::Invalid);
        };
        if name.is_empty() || definitions.iter().any(|known| &known.name == name) {
            return Err(Decoding::Invalid);
        }
        definitions.push(VoDefinition {
            name: owned_text(name)?,
            definition: decode_lowercase_hex(definition)?,
        });
    }
    Ok(definitions)
}

/// `namespace.Record`: two or more dot-separated segments. VO names are
/// dot-namespaced and DNS-anchored, so every namespace segment is a DNS label
/// (1-63 letters, digits or hyphens, neither first nor last a hyphen; an
/// underscore is also admitted, as in identifier-style namespaces). The final
/// segment names the record and is an ASCII identifier: a letter or
/// underscore, then letters, digits, underscores.
fn is_qualified_root(root: &str) -> bool {
    let Some((namespace, record)) = root.rsplit_once('.') else {
        return false;
    };
    namespace.split('.').all(is_namespace_label) && is_identifier(record)
}

fn is_namespace_label(label: &str) -> bool {
    let bytes = label.as_bytes();
    let (Some(&first), Some(&last)) = (bytes.first(), bytes.last()) else {
        return false;
    };
    bytes.len() <= 63
        && first != b'-'
        && last != b'-'
        && bytes
            .iter()
            .all(|&byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

fn is_identifier(segment: &str) -> bool {
    let mut bytes = segment.bytes();
    bytes
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

/// Canonical lowercase hex: non-empty, even length, `0-9a-f` only.
fn decode_lowercase_hex(text: &str) -> Result<Vec<u8>, Decoding> {
    let digits = text.as_bytes();
    if digits.is_empty() || !digits.len().is_multiple_of(2) {
        return Err(Decoding::Invalid);
    }
    let mut bytes = backed(digits.len() / 2)?;
    for pair in digits.as_chunks::<2>().0 {
        let (Some(high), Some(low)) = (hex_digit(pair[0]), hex_digit(pair[1])) else {
            return Err(Decoding::Invalid);
        };
        bytes.push((high << 4) | low);
    }
    Ok(bytes)
}

fn hex_digit(digit: u8) -> Option<u8> {
    match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        _ => None,
    }
}

fn parse_json_value(
    parser: &mut JsonParser<'_>,
    depth: usize,
) -> Result<JsonValue, ValueParseError> {
    if depth > MAX_SCHEMA_DEPTH {
        return Err(ValueParseError::TooDeep);
    }
    match parser.next_token()? {
        Token::Null => Ok(JsonValue::Null),
        Token::True => Ok(JsonValue::Boolean(true)),
        Token::False => Ok(JsonValue::Boolean(false)),
        Token::String(value) => Ok(JsonValue::String(value.to_owned_text()?)),
        Token::Number(value) => parse_json_number(value),
        Token::ObjectBegin => {
            let mut fields = Vec::new();
            while !parser.is_object_end() {
                let name = parser.expect_field_name()?.to_owned_text()?;
                let value = parse_json_value(parser, depth + 1)?;
                one_more(&mut fields)?;
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
                let value = parse_json_value(parser, depth + 1)?;
                one_more(&mut values)?;
                values.push(value);
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
    /// The value tree could not be backed.
    OutOfMemory,
}

impl From<ParserError> for ValueParseError {
    fn from(error: ParserError) -> Self {
        match error {
            ParserError::OutOfMemory => Self::OutOfMemory,
            ParserError::InvalidJson
            | ParserError::UnexpectedToken
            | ParserError::EndOfInput
            | ParserError::InvalidNumber => Self::InvalidJson,
        }
    }
}

/// Back one more element of `buf`, or refuse as out of memory.
fn one_more<T>(buf: &mut Vec<T>) -> Result<(), ValueParseError> {
    buf.try_reserve(1).map_err(|_| ValueParseError::OutOfMemory)
}

/// A MessagePack value borrowed from its input. Strings, keys, and binary
/// payloads point into the batch, so the judgment copies nothing and a
/// value-object frame is admitted as the wire bytes themselves.
#[derive(Clone, Debug, PartialEq)]
enum MsgpackValue<'a> {
    Unknown,
    Null,
    String(&'a str),
    Number(NumberValue),
    Boolean(bool),
    Binary(&'a [u8]),
    BigInt,
    Array(Vec<Self>),
    Object(Vec<(&'a str, Self)>),
}

impl ValueView for MsgpackValue<'_> {
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
                .find(|(field, _)| *field == name)
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

/// Capacity for `count` decoded elements: every element takes at least one
/// input byte, so a header cannot reserve more than the bytes left behind it.
fn element_capacity(reader: &Reader<'_>, count: u32) -> usize {
    (count as usize).min(reader.input().len().saturating_sub(reader.position()))
}

/// One event map. `type` and `value` are decoded; every other key is kept
/// with an unknown value so the judgment can see which keys the event spells.
fn parse_msgpack_event<'a>(reader: &mut Reader<'a>) -> Result<MsgpackValue<'a>, ValueParseError> {
    let fields = reader
        .read_map_header()
        .ok_or(ValueParseError::InvalidMsgpack)?;
    let mut event = Vec::with_capacity(element_capacity(reader, fields));
    for _ in 0..fields {
        let key = reader
            .read_string()
            .ok_or(ValueParseError::InvalidMsgpack)?;
        let key = std::str::from_utf8(key).map_err(|_| ValueParseError::InvalidMsgpack)?;
        let value = if key == "type" || key == "value" {
            parse_msgpack_value(reader, 0)?
        } else {
            reader.skip_value().ok_or(ValueParseError::InvalidMsgpack)?;
            MsgpackValue::Unknown
        };
        event.push((key, value));
    }
    Ok(MsgpackValue::Object(event))
}

fn parse_msgpack_value<'a>(
    reader: &mut Reader<'a>,
    depth: usize,
) -> Result<MsgpackValue<'a>, ValueParseError> {
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
                .map(|value| MsgpackValue::Number(NumberValue::Unsigned(u128::from(value))))
                .ok_or(ValueParseError::InvalidMsgpack);
        }
        return reader
            .read_integer()
            .map(|value| MsgpackValue::Number(NumberValue::Signed(i128::from(value))))
            .ok_or(ValueParseError::InvalidMsgpack);
    }
    if Reader::is_ext(first) {
        // A bigint travels as the canonical bigint extension on both wires;
        // any other extension is opaque to the judgment.
        let (kind, _payload) = reader.read_ext().ok_or(ValueParseError::InvalidMsgpack)?;
        return Ok(if kind == crate::msgpack_scanner::BIGINT_EXTENSION {
            MsgpackValue::BigInt
        } else {
            MsgpackValue::Unknown
        });
    }
    if Reader::is_string(first) {
        let bytes = reader
            .read_string()
            .ok_or(ValueParseError::InvalidMsgpack)?;
        return Ok(match std::str::from_utf8(bytes) {
            Ok(value) => MsgpackValue::String(value),
            // Text that is not UTF-8 is malformed text, never the `bin` family a
            // value-object frame is spelled in.
            Err(_) => MsgpackValue::Unknown,
        });
    }
    if Reader::is_float(first) {
        return reader
            .read_float()
            .map(|value| MsgpackValue::Number(NumberValue::Float(value)))
            .ok_or(ValueParseError::InvalidMsgpack);
    }
    match first {
        0xc0 => {
            reader.skip_value().ok_or(ValueParseError::InvalidMsgpack)?;
            Ok(MsgpackValue::Null)
        }
        0xc2 | 0xc3 => {
            reader.skip_value().ok_or(ValueParseError::InvalidMsgpack)?;
            Ok(MsgpackValue::Boolean(first == 0xc3))
        }
        0xc4..=0xc6 => reader
            .read_bin()
            .map(MsgpackValue::Binary)
            .ok_or(ValueParseError::InvalidMsgpack),
        0x80..=0x8f | 0xde | 0xdf => {
            let count = reader
                .read_map_header()
                .ok_or(ValueParseError::InvalidMsgpack)?;
            let mut fields = Vec::with_capacity(element_capacity(reader, count));
            for _ in 0..count {
                let key = reader
                    .read_string()
                    .ok_or(ValueParseError::InvalidMsgpack)?;
                let key = std::str::from_utf8(key).map_err(|_| ValueParseError::InvalidMsgpack)?;
                fields.push((key, parse_msgpack_value(reader, depth + 1)?));
            }
            Ok(MsgpackValue::Object(fields))
        }
        0x90..=0x9f | 0xdc | 0xdd => {
            let count = reader
                .read_array_header()
                .ok_or(ValueParseError::InvalidMsgpack)?;
            let mut values = Vec::with_capacity(element_capacity(reader, count));
            for _ in 0..count {
                values.push(parse_msgpack_value(reader, depth + 1)?);
            }
            Ok(MsgpackValue::Array(values))
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

    /// The decoded `value` member of one MessagePack event row.
    fn msgpack_event_value(row: &[u8]) -> MsgpackValue<'_> {
        let MsgpackValue::Object(fields) = parse_msgpack_event(&mut Reader::new(row)).unwrap()
        else {
            panic!("event is an object")
        };
        fields
            .into_iter()
            .find_map(|(name, value)| (name == "value").then_some(value))
            .expect("value field")
    }

    #[test]
    fn bigint_accepts_its_json_spellings_and_refuses_the_rest() {
        let bigint = schema(r#"{"kind":"bigint"}"#);
        assert!(validate_value(&bigint, &json(r#""5000000000""#)).is_ok());
        assert!(validate_value(&bigint, &json(r#""-42""#)).is_ok());
        assert!(validate_value(&bigint, &JsonValue::BigInt).is_ok());
        for bad in [r#""1.5""#, r#""abc""#, "5000000000", "1.5", "true", r#""""#] {
            let error = validate_value(&bigint, &json(bad)).unwrap_err();
            assert_eq!(error.expected, "bigint", "{bad} must refuse");
        }
    }

    #[test]
    fn msgpack_bigint_extension_is_a_bigint_and_integer_markers_are_numbers() {
        let mut bigint_row = vec![0x81];
        // fixext 2, type 20, sign 0, magnitude 5
        bigint_row.extend([0xa5, b'v', b'a', b'l', b'u', b'e', 0xd5, 20, 0, 5]);
        assert_eq!(msgpack_event_value(&bigint_row), MsgpackValue::BigInt);

        let mut number_row = vec![0x81];
        number_row.extend([0xa5, b'v', b'a', b'l', b'u', b'e', 0xce]);
        number_row.extend(5_u32.to_be_bytes());
        assert_eq!(
            msgpack_event_value(&number_row),
            MsgpackValue::Number(NumberValue::Signed(5))
        );

        // A binary payload is the input's own bytes, never a copy.
        let binary_row = [0x81, 0xa5, b'v', b'a', b'l', b'u', b'e', 0xc4, 2, 7, 8];
        let MsgpackValue::Binary(bytes) = msgpack_event_value(&binary_row) else {
            panic!("bin decodes as binary")
        };
        assert!(std::ptr::eq(bytes, &binary_row[9..]));
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
                vo: None,
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
        // Only `.` and `%` are escaped; every other byte, a non-ASCII one
        // included, is kept as itself, so UTF-8 keys stay UTF-8.
        let accented = json(r#"{"caf\u00e9.%":1}"#);
        assert_eq!(
            validate_value(&closed, &accented).unwrap_err().path,
            "value.caf\u{e9}%2E%25"
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

    /// A downstream adapter's own integer cell: the seam a runtime with an
    /// exact 64-bit integer codec implements without building a `JsonValue`.
    struct ExactInt64(i64);

    impl ValueView for ExactInt64 {
        fn kind(&self) -> ValueKind {
            ValueKind::CanonicalInt64
        }

        fn as_canonical_i64(&self) -> Option<i64> {
            Some(self.0)
        }
    }

    /// Claims the canonical kind without its exact payload, optionally
    /// offering an ordinary number in its place.
    struct UnprovenInt64(Option<NumberValue>);

    impl ValueView for UnprovenInt64 {
        fn kind(&self) -> ValueKind {
            ValueKind::CanonicalInt64
        }

        fn as_number(&self) -> Option<NumberValue> {
            self.0
        }
    }

    /// Reports an ordinary number while also answering the exact accessor.
    struct NumberAnsweringExact(i64);

    impl ValueView for NumberAnsweringExact {
        fn kind(&self) -> ValueKind {
            ValueKind::Number
        }

        fn as_number(&self) -> Option<NumberValue> {
            Some(NumberValue::Signed(i128::from(self.0)))
        }

        fn as_canonical_i64(&self) -> Option<i64> {
            Some(self.0)
        }
    }

    #[test]
    fn canonical_int64_is_judged_by_each_schemas_exact_bounds() {
        let accepted: [(&str, &[i64]); 4] = [
            (
                r#"{"kind":"bigint"}"#,
                &[i64::MIN, -1, 0, MAX_SAFE_INTEGER + 1, i64::MAX],
            ),
            (
                r#"{"kind":"number"}"#,
                &[-MAX_SAFE_INTEGER, 0, MAX_SAFE_INTEGER],
            ),
            (
                r#"{"kind":"i32"}"#,
                &[i64::from(i32::MIN), 0, i64::from(i32::MAX)],
            ),
            (r#"{"kind":"u32"}"#, &[0, i64::from(u32::MAX)]),
        ];
        for (tree, values) in accepted {
            let schema = schema(tree);
            for &value in values {
                assert_eq!(
                    validate_value(&schema, &JsonValue::CanonicalInt64(value)),
                    Ok(()),
                    "{tree} must accept {value}"
                );
                assert_eq!(
                    validate_value(&schema, &ExactInt64(value)),
                    Ok(()),
                    "{tree} must accept {value} from an adapter"
                );
            }
        }
        let refused: [(&str, &[i64]); 7] = [
            (
                r#"{"kind":"number"}"#,
                &[
                    MAX_SAFE_INTEGER + 1,
                    -MAX_SAFE_INTEGER - 1,
                    i64::MAX,
                    i64::MIN,
                ],
            ),
            (
                r#"{"kind":"i32"}"#,
                &[i64::from(i32::MAX) + 1, i64::from(i32::MIN) - 1, i64::MAX],
            ),
            (
                r#"{"kind":"u32"}"#,
                &[-1, i64::from(u32::MAX) + 1, i64::MIN],
            ),
            (r#"{"kind":"string"}"#, &[0]),
            (r#"{"kind":"boolean"}"#, &[1]),
            (r#"{"kind":"binary"}"#, &[0]),
            (r#"{"kind":"enum","variants":["1"]}"#, &[1]),
        ];
        for (tree, values) in refused {
            let schema = schema(tree);
            for &value in values {
                assert!(
                    validate_value(&schema, &JsonValue::CanonicalInt64(value)).is_err(),
                    "{tree} must refuse {value}"
                );
                assert!(
                    validate_value(&schema, &ExactInt64(value)).is_err(),
                    "{tree} must refuse {value} from an adapter"
                );
            }
        }
    }

    #[test]
    fn the_canonical_kind_without_its_exact_payload_proves_nothing() {
        let numeric = ["bigint", "number", "i32", "u32"];
        for unproven in [
            UnprovenInt64(None),
            UnprovenInt64(Some(NumberValue::Signed(5))),
        ] {
            for kind in numeric {
                let schema = schema(&format!(r#"{{"kind":"{kind}"}}"#));
                assert!(
                    validate_value(&schema, &unproven).is_err(),
                    "{kind} must not accept the marker alone"
                );
            }
        }
        let number_or_bigint =
            schema(r#"{"kind":"union","variants":[{"kind":"number"},{"kind":"bigint"}]}"#);
        assert!(validate_value(&number_or_bigint, &UnprovenInt64(None)).is_err());
    }

    #[test]
    fn number_tokens_and_markers_are_never_coerced_to_canonical_int64() {
        let bigint = schema(r#"{"kind":"bigint"}"#);
        let number = schema(r#"{"kind":"number"}"#);
        let wide = 9_007_199_254_740_993_i64;

        for text in ["9007199254740993", "-9007199254740993"] {
            let token = json(text);
            assert!(validate_value(&bigint, &token).is_err());
            assert_eq!(validate_value(&number, &token), Ok(()));
        }

        let mut row = vec![0x81, 0xa5, b'v', b'a', b'l', b'u', b'e', 0xd3];
        row.extend(wide.to_be_bytes());
        let marker = msgpack_event_value(&row);
        assert!(validate_value(&bigint, &marker).is_err());
        assert_eq!(validate_value(&number, &marker), Ok(()));

        // The reported kind decides: a number stays a number even when its
        // adapter also answers the exact accessor.
        assert!(validate_value(&bigint, &NumberAnsweringExact(5)).is_err());
        assert_eq!(validate_value(&number, &NumberAnsweringExact(wide)), Ok(()));
    }

    #[test]
    fn a_union_admits_canonical_int64_through_exactly_one_variant() {
        let number_or_bigint =
            schema(r#"{"kind":"union","variants":[{"kind":"number"},{"kind":"bigint"}]}"#);
        for ambiguous in [-MAX_SAFE_INTEGER, 0, MAX_SAFE_INTEGER] {
            assert!(
                validate_value(&number_or_bigint, &JsonValue::CanonicalInt64(ambiguous)).is_err(),
                "{ambiguous} fits both number and bigint"
            );
        }
        for wide in [
            MAX_SAFE_INTEGER + 1,
            -MAX_SAFE_INTEGER - 1,
            i64::MAX,
            i64::MIN,
        ] {
            assert_eq!(
                validate_value(&number_or_bigint, &JsonValue::CanonicalInt64(wide)),
                Ok(()),
                "{wide} fits only bigint"
            );
        }
        assert_eq!(validate_value(&number_or_bigint, &json("5")), Ok(()));

        let i32_or_u32 = schema(r#"{"kind":"union","variants":[{"kind":"i32"},{"kind":"u32"}]}"#);
        let exact = JsonValue::CanonicalInt64;
        assert!(validate_value(&i32_or_u32, &exact(5)).is_err());
        assert_eq!(validate_value(&i32_or_u32, &exact(-5)), Ok(()));
        assert_eq!(
            validate_value(&i32_or_u32, &exact(i64::from(u32::MAX))),
            Ok(())
        );
        assert!(validate_value(&i32_or_u32, &exact(i64::from(u32::MAX) + 1)).is_err());

        let string_or_bigint =
            schema(r#"{"kind":"union","variants":[{"kind":"string"},{"kind":"bigint"}]}"#);
        assert_eq!(validate_value(&string_or_bigint, &exact(0)), Ok(()));
    }

    #[test]
    fn canonical_int64_is_judged_at_its_path_inside_nested_shapes() {
        let schema = schema(
            r#"{"kind":"object","fields":{"id":{"kind":"bigint"},"count":{"kind":"optional","value":{"kind":"number"}},"marks":{"kind":"array","item":{"kind":"nullable","value":{"kind":"i32"}}},"limits":{"kind":"map","key":{"kind":"string"},"value":{"kind":"u32"}},"totals":{"kind":"record","value":{"kind":"bigint"}}}}"#,
        );
        let exact = JsonValue::CanonicalInt64;
        let payload = |count: Option<i64>, mark: i64, limit: i64| {
            let mut fields = vec![("id".to_owned(), exact(i64::MAX))];
            if let Some(count) = count {
                fields.push(("count".to_owned(), exact(count)));
            }
            fields.extend([
                (
                    "marks".to_owned(),
                    JsonValue::Array(vec![JsonValue::Null, exact(mark)]),
                ),
                (
                    "limits".to_owned(),
                    JsonValue::object(vec![("a".to_owned(), exact(limit))]),
                ),
                (
                    "totals".to_owned(),
                    JsonValue::object(vec![("x".to_owned(), exact(i64::MIN))]),
                ),
            ]);
            JsonValue::object(fields)
        };
        assert_eq!(
            validate_value(
                &schema,
                &payload(None, i64::from(i32::MIN), i64::from(u32::MAX))
            ),
            Ok(())
        );
        assert_eq!(
            validate_value(&schema, &payload(Some(-MAX_SAFE_INTEGER), 0, 0)),
            Ok(())
        );
        for (value, path) in [
            (payload(Some(MAX_SAFE_INTEGER + 1), 0, 0), "value.count"),
            (payload(None, i64::from(i32::MAX) + 1, 0), "value.marks[1]"),
            (payload(None, 0, -1), "value.limits.a"),
        ] {
            assert_eq!(validate_value(&schema, &value).unwrap_err().path, path);
        }
    }

    #[test]
    fn range_bounds_are_finite_ordered_and_integral_ranges_hold_safe_integers() {
        assert_eq!(
            schema(r#"{"kind":"range","min":-1.5,"max":10,"integer":false}"#),
            SemanticSchema::Range(NumericRange::new(-1.5, 10.0, false).unwrap())
        );
        assert_eq!(
            schema(
                r#"{"kind":"range","min":-9007199254740991,"max":9007199254740991,"integer":true}"#
            ),
            SemanticSchema::Range(
                NumericRange::new(-MAX_SAFE_INTEGER_F64, MAX_SAFE_INTEGER_F64, true).unwrap()
            )
        );
        for (tree, error) in [
            (
                r#"{"kind":"range","min":2,"max":1,"integer":false}"#,
                SchemaParseError::InvalidOperand,
            ),
            (
                r#"{"kind":"range","min":0,"max":1e400,"integer":false}"#,
                SchemaParseError::InvalidOperand,
            ),
            (
                r#"{"kind":"range","min":0.5,"max":1,"integer":true}"#,
                SchemaParseError::InvalidOperand,
            ),
            (
                r#"{"kind":"range","min":0,"max":9007199254740992,"integer":true}"#,
                SchemaParseError::InvalidOperand,
            ),
            (
                r#"{"kind":"range","min":0,"max":"1","integer":false}"#,
                SchemaParseError::InvalidOperand,
            ),
            (
                r#"{"kind":"range","min":0,"max":1,"integer":"yes"}"#,
                SchemaParseError::InvalidOperand,
            ),
            (
                r#"{"kind":"range","min":0,"max":1}"#,
                SchemaParseError::MissingOperand,
            ),
            (
                r#"{"kind":"range","max":1,"integer":true}"#,
                SchemaParseError::MissingOperand,
            ),
        ] {
            assert_eq!(
                SemanticSchema::from_tree_bytes(tree.as_bytes()),
                Err(error),
                "{tree}"
            );
        }
    }

    #[test]
    fn integral_range_admits_exact_integers_inside_its_bounds() {
        let range = schema(r#"{"kind":"range","min":-3,"max":5,"integer":true}"#);
        for accepted in ["-3", "0", "5", "4.0"] {
            assert_eq!(
                validate_value(&range, &json(accepted)),
                Ok(()),
                "{accepted}"
            );
        }
        for refused in ["-4", "6", "2.5", "\"3\"", "true", "18446744073709551616"] {
            assert!(validate_value(&range, &json(refused)).is_err(), "{refused}");
        }
        assert_eq!(
            validate_value(&range, &JsonValue::CanonicalInt64(5)),
            Ok(())
        );
        assert_eq!(validate_value(&range, &ExactInt64(-3)), Ok(()));
        assert!(validate_value(&range, &ExactInt64(6)).is_err());
        assert_eq!(
            validate_value(&range, &json("6")),
            Err(PayloadViolation {
                path: "value".into(),
                expected: "safe integer in [-3, 5]".into(),
                observed: "number".into(),
                vo: None,
            })
        );
    }

    #[test]
    fn fractional_range_admits_finite_numbers_and_compares_integers_exactly() {
        let range = schema(r#"{"kind":"range","min":0.5,"max":2.5,"integer":false}"#);
        for accepted in ["0.5", "1", "2", "2.5"] {
            assert_eq!(
                validate_value(&range, &json(accepted)),
                Ok(()),
                "{accepted}"
            );
        }
        for refused in ["0.25", "3", "-1", "null"] {
            assert!(validate_value(&range, &json(refused)).is_err(), "{refused}");
        }

        // 2^53 + 1 rounds to the bound as a double, yet lies above it.
        let wide = schema(r#"{"kind":"range","min":0,"max":9007199254740992,"integer":false}"#);
        assert_eq!(validate_value(&wide, &json("9007199254740992")), Ok(()));
        assert!(validate_value(&wide, &json("9007199254740993")).is_err());

        // A canonical int64 is a number only inside the safe range, whatever
        // the bounds allow; an ordinary integer token is judged exactly.
        let huge = schema(r#"{"kind":"range","min":-1e300,"max":1e300,"integer":false}"#);
        assert_eq!(
            validate_value(&huge, &JsonValue::CanonicalInt64(MAX_SAFE_INTEGER)),
            Ok(())
        );
        for unsafe_integer in [MAX_SAFE_INTEGER + 1, i64::MAX, i64::MIN] {
            assert!(validate_value(&huge, &JsonValue::CanonicalInt64(unsafe_integer)).is_err());
        }
        assert_eq!(validate_value(&huge, &json("18446744073709551616")), Ok(()));
    }

    #[test]
    fn exact_length_arrays_refuse_every_other_length() {
        let pair = schema(r#"{"kind":"array","item":{"kind":"i32"},"length":2}"#);
        assert_eq!(
            pair,
            SemanticSchema::Array {
                item: Box::new(SemanticSchema::I32),
                length: Some(2),
            }
        );
        assert_eq!(validate_value(&pair, &json("[1,2]")), Ok(()));
        for (value, observed) in [("[1]", 1), ("[1,2,3]", 3), (r#"["x"]"#, 1)] {
            assert_eq!(
                validate_value(&pair, &json(value)),
                Err(PayloadViolation {
                    path: "value".into(),
                    expected: "array of length 2".into(),
                    observed: format!("array of length {observed}"),
                    vo: None,
                }),
                "{value}"
            );
        }
        assert_eq!(
            validate_value(&pair, &json(r#"[1,"x"]"#)).unwrap_err().path,
            "value[1]"
        );

        let empty = schema(r#"{"kind":"array","item":{"kind":"i32"},"length":0}"#);
        assert_eq!(validate_value(&empty, &json("[]")), Ok(()));
        assert!(validate_value(&empty, &json("[1]")).is_err());

        let any_length = schema(r#"{"kind":"array","item":{"kind":"i32"}}"#);
        for value in ["[]", "[1,2,3]"] {
            assert_eq!(validate_value(&any_length, &json(value)), Ok(()));
        }

        for length in ["-1", "1.5", "4294967296", "\"2\"", "null"] {
            let tree = format!(r#"{{"kind":"array","item":{{"kind":"i32"}},"length":{length}}}"#);
            assert_eq!(
                SemanticSchema::from_tree_bytes(tree.as_bytes()),
                Err(SchemaParseError::InvalidOperand),
                "{tree}"
            );
        }
    }

    fn vo_tree(root: &str, frame: &str, rows: &str) -> String {
        format!(r#"{{"kind":"vo","root":"{root}","frame":"{frame}","rows":{rows}}}"#)
    }

    fn line_tree() -> String {
        vo_tree("orders.Line", "0a1b", r#"["00ff","10"]"#)
    }

    #[test]
    fn vo_metadata_decodes_canonical_hex_once() {
        assert_eq!(
            schema(&line_tree()),
            SemanticSchema::Vo(Box::new(VoMetadata {
                root: "orders.Line".into(),
                frame: vec![0x0a, 0x1b],
                rows: vec![vec![0x00, 0xff], vec![0x10]],
                profile: VoProfile::default(),
                retained: Vec::new(),
            }))
        );
        assert!(matches!(
            schema(&vo_tree("_a.b_2.C", "00", r#"["00"]"#)),
            SemanticSchema::Vo(_)
        ));
        // Namespace segments are DNS labels: hyphens inside, a leading digit.
        let longest_label = format!("{}.Line", "a".repeat(63));
        for root in [
            "org.example.shell-exec.v3.Failed",
            "org.ex-ample.9lives.Sample",
            longest_label.as_str(),
        ] {
            assert!(
                matches!(
                    schema(&vo_tree(root, "00", r#"["00"]"#)),
                    SemanticSchema::Vo(_)
                ),
                "{root}"
            );
        }
        let SemanticSchema::Vo(retained) = schema(
            r#"{"kind":"vo","root":"orders.Line","frame":"00","rows":["00"],"retained":["0a0b","ff"]}"#,
        ) else {
            panic!("vo node")
        };
        assert_eq!(retained.retained, vec![vec![0x0a, 0x0b], vec![0xff]]);
    }

    #[test]
    fn vo_profile_carries_committed_definition_bytes_verbatim() {
        let tree = r#"{"kind":"vo","root":"orders.Line","frame":"00","rows":["00"],"profile":{"records":[{"name":"orders.Line","definition":"0a0b0c"}],"attributes":[{"name":"orders.qty","definition":"ff"}]}}"#;
        let SemanticSchema::Vo(metadata) = schema(tree) else {
            panic!("vo node")
        };
        assert_eq!(
            metadata.profile,
            VoProfile {
                records: vec![VoDefinition {
                    name: "orders.Line".into(),
                    definition: vec![0x0a, 0x0b, 0x0c],
                }],
                attributes: vec![VoDefinition {
                    name: "orders.qty".into(),
                    definition: vec![0xff],
                }],
            }
        );
        let only_records = r#"{"kind":"vo","root":"orders.Line","frame":"00","rows":["00"],"profile":{"records":[]}}"#;
        let SemanticSchema::Vo(metadata) = schema(only_records) else {
            panic!("vo node")
        };
        assert_eq!(metadata.profile, VoProfile::default());
    }

    #[test]
    fn malformed_vo_metadata_is_a_typed_schema_error() {
        use VoMetadataField::{Frame, Profile, Retained, Root, Rows};
        let rows = r#"["00"]"#;
        let cases = [
            (vo_tree("Line", "00", rows), Root),
            (vo_tree("orders.", "00", rows), Root),
            (vo_tree(".Line", "00", rows), Root),
            (vo_tree("orders..Line", "00", rows), Root),
            (vo_tree("orders.1Line", "00", rows), Root),
            (vo_tree("orders.Li-ne", "00", rows), Root),
            (vo_tree("-orders.Line", "00", rows), Root),
            (vo_tree("orders-.Line", "00", rows), Root),
            (vo_tree("or ders.Line", "00", rows), Root),
            (
                vo_tree(&format!("{}.Line", "a".repeat(64)), "00", rows),
                Root,
            ),
            (
                r#"{"kind":"vo","frame":"00","rows":["00"]}"#.to_owned(),
                Root,
            ),
            (vo_tree("orders.Line", "", rows), Frame),
            (vo_tree("orders.Line", "0", rows), Frame),
            (vo_tree("orders.Line", "0A", rows), Frame),
            (vo_tree("orders.Line", "zz", rows), Frame),
            (
                r#"{"kind":"vo","root":"orders.Line","frame":10,"rows":["00"]}"#.to_owned(),
                Frame,
            ),
            (vo_tree("orders.Line", "00", "[]"), Rows),
            (vo_tree("orders.Line", "00", r#"[""]"#), Rows),
            (vo_tree("orders.Line", "00", r#"["0F"]"#), Rows),
            (vo_tree("orders.Line", "00", r#"["00",1]"#), Rows),
            (vo_tree("orders.Line", "00", r#""00""#), Rows),
            (
                r#"{"kind":"vo","root":"orders.Line","frame":"00"}"#.to_owned(),
                Rows,
            ),
            (
                format!(
                    r#"{{"kind":"vo","root":"orders.Line","frame":"00","rows":{rows},"profile":[]}}"#
                ),
                Profile,
            ),
            (
                format!(
                    r#"{{"kind":"vo","root":"orders.Line","frame":"00","rows":{rows},"profile":{{"extra":[]}}}}"#
                ),
                Profile,
            ),
            (
                format!(
                    r#"{{"kind":"vo","root":"orders.Line","frame":"00","rows":{rows},"profile":{{"records":[{{"name":"","definition":"00"}}]}}}}"#
                ),
                Profile,
            ),
            (
                format!(
                    r#"{{"kind":"vo","root":"orders.Line","frame":"00","rows":{rows},"profile":{{"records":[{{"name":"a.B","definition":1}}]}}}}"#
                ),
                Profile,
            ),
            (
                format!(
                    r#"{{"kind":"vo","root":"orders.Line","frame":"00","rows":{rows},"profile":{{"records":[{{"name":"a.B","definition":"0A"}}]}}}}"#
                ),
                Profile,
            ),
            (
                format!(
                    r#"{{"kind":"vo","root":"orders.Line","frame":"00","rows":{rows},"profile":{{"records":[{{"name":"a.B","definition":"{{}}"}}]}}}}"#
                ),
                Profile,
            ),
            (
                format!(
                    r#"{{"kind":"vo","root":"orders.Line","frame":"00","rows":{rows},"profile":{{"attributes":[{{"name":"a.b","definition":"00"}},{{"name":"a.b","definition":"01"}}]}}}}"#
                ),
                Profile,
            ),
            (
                format!(
                    r#"{{"kind":"vo","root":"orders.Line","frame":"00","rows":{rows},"profile":{{"records":[{{"name":"a.B","definition":"00","extra":1}}]}}}}"#
                ),
                Profile,
            ),
            (
                format!(
                    r#"{{"kind":"vo","root":"orders.Line","frame":"00","rows":{rows},"retained":"00"}}"#
                ),
                Retained,
            ),
            (
                format!(
                    r#"{{"kind":"vo","root":"orders.Line","frame":"00","rows":{rows},"retained":["0A"]}}"#
                ),
                Retained,
            ),
            (
                format!(
                    r#"{{"kind":"vo","root":"orders.Line","frame":"00","rows":{rows},"retained":[""]}}"#
                ),
                Retained,
            ),
        ];
        for (tree, member) in cases {
            assert_eq!(
                SemanticSchema::from_tree_bytes(tree.as_bytes()),
                Err(SchemaParseError::InvalidVoMetadata(member)),
                "{tree}"
            );
        }
    }

    #[test]
    fn vo_nodes_without_a_capability_refuse_with_a_typed_cause() {
        let tree = schema(&format!(
            r#"{{"kind":"object","fields":{{"line":{}}}}}"#,
            line_tree()
        ));
        let frame = JsonValue::object(vec![("line".to_owned(), JsonValue::Binary(vec![1, 2, 3]))]);
        let violation = validate_value(&tree, &frame).unwrap_err();
        assert_eq!(
            violation,
            PayloadViolation {
                path: "value.line".into(),
                expected: "vo orders.Line".into(),
                observed: "binary".into(),
                vo: Some(VoRefusal::Admission(VoAdmissionError::Unavailable)),
            }
        );
        assert_eq!(
            validate_value_with_admission(&tree, &frame, &UnavailableVoAdmission),
            Err(violation.clone())
        );
        assert_eq!(
            violation.to_string(),
            "value.line: expected vo orders.Line, got binary (value-object admission unavailable)"
        );

        // A frame must be binary before any capability is consulted.
        let text = JsonValue::object(vec![("line".to_owned(), JsonValue::String("0001".into()))]);
        let shape = validate_value(&tree, &text).unwrap_err();
        assert_eq!((shape.observed.as_str(), shape.vo), ("string", None));

        // Through a union that admits nothing, the cause stays observable.
        let union = schema(&format!(
            r#"{{"kind":"union","variants":[{},{{"kind":"string"}}]}}"#,
            line_tree()
        ));
        assert_eq!(
            validate_value(&union, &JsonValue::Binary(vec![0]))
                .unwrap_err()
                .vo,
            Some(VoRefusal::Admission(VoAdmissionError::Unavailable))
        );
    }

    #[test]
    fn preparing_without_a_capability_names_the_refused_root() {
        let envelope = format!(
            r#"{{"plain":{{"kind":"string"}},"lines":{{"kind":"array","item":{}}}}}"#,
            line_tree()
        );
        let schemas = parse_schema_envelope(envelope.as_bytes()).unwrap();
        assert_eq!(
            PreparedSchemas::prepare(schemas, UnavailableVoAdmission).err(),
            Some(VoPreparationError {
                root: "orders.Line".into(),
                error: VoAdmissionError::Unavailable,
            })
        );
        let plain = parse_schema_envelope(br#"{"plain":{"kind":"string"}}"#).unwrap();
        assert_eq!(
            PreparedSchemas::prepare(plain, UnavailableVoAdmission)
                .map(|prepared| prepared.presence())
                .ok(),
            Some(VoPresence::Absent)
        );
    }

    #[test]
    fn msgpack_batches_judge_declared_types_and_refuse_unknown_ones() {
        let schemas = parse_schema_envelope(
            br#"{"hi":{"kind":"object","fields":{"greeting":{"kind":"string"}}}}"#,
        )
        .unwrap();
        let prepared = PreparedSchemas::prepare(schemas, UnavailableVoAdmission).unwrap();
        // {"type":"hi","value":{"greeting":42}}
        let judged = [
            0x82, 0xa4, b't', b'y', b'p', b'e', 0xa2, b'h', b'i', 0xa5, b'v', b'a', b'l', b'u',
            b'e', 0x81, 0xa8, b'g', b'r', b'e', b'e', b't', b'i', b'n', b'g', 42,
        ];
        match prepared.validate_msgpack_batch(&judged, true) {
            Err(BatchValidationError::Violation {
                event_index: 0,
                event_type,
                violation,
            }) => {
                assert_eq!(event_type.as_deref(), Some("hi"));
                assert_eq!(violation.path, "value.greeting");
            }
            other => panic!("a declared type is judged: {other:?}"),
        }
        // {"value":1} and {"type":"other","value":1}: an event whose type the
        // envelope does not declare is refused, never passed unjudged.
        let untyped = [0x81, 0xa5, b'v', b'a', b'l', b'u', b'e', 1];
        let other = [
            0x82, 0xa4, b't', b'y', b'p', b'e', 0xa5, b'o', b't', b'h', b'e', b'r', 0xa5, b'v',
            b'a', b'l', b'u', b'e', 1,
        ];
        for (input, event_type, observed) in [
            (&untyped[..], None, "absent"),
            (&other[..], Some("other"), "undeclared"),
        ] {
            match prepared.validate_msgpack_batch(input, true) {
                Err(BatchValidationError::Violation {
                    event_index: 0,
                    event_type: refused_type,
                    violation,
                }) => {
                    assert_eq!(refused_type.as_deref(), event_type);
                    assert_eq!(
                        (violation.path.as_str(), violation.observed.as_str()),
                        ("type", observed)
                    );
                    if event_type.is_some() {
                        assert_eq!(*violation, PayloadViolation::undeclared_type());
                    }
                }
                result => panic!("an unknown type must refuse: {result:?}"),
            }
        }
        // A header claiming more elements than the bytes behind it is
        // malformed input, not a large reservation.
        assert_eq!(
            prepared.validate_msgpack_batch(&[0xdd, 0xff, 0xff, 0xff, 0xff], false),
            Err(BatchValidationError::InvalidInput)
        );
    }

    const LINK: &str = r#"{"slot":0,"field_idx":2,"schema":{"root":"orders.Line","frame":"0a1b","rows":["00ff"]}}"#;
    const ROLE: &str =
        r#"{"instruction_offset":96,"count_field":1,"histogram_fields":[2,3],"mask_fields":[4]}"#;

    fn program(fields: &str, counts: &str) -> String {
        format!(r#"{{"fields":[{fields}],"counts":[{counts}]}}"#)
    }

    #[test]
    fn program_metadata_decodes_links_and_roles_once() {
        let full = program(
            r#"{"slot":7,"field_idx":255,"schema":{"root":"orders.Line","frame":"0a1b","rows":["00ff","10"],"profile":{"records":[{"name":"orders.Line","definition":"0a"}],"attributes":[{"name":"orders.qty","definition":"0b0c"}]},"retained":["0c","0d0e"]}}"#,
            r#"{"instruction_offset":4294967295,"count_field":65535,"histogram_fields":[2,3],"mask_fields":[]}"#,
        );
        assert_eq!(
            parse_program_metadata(full.as_bytes()),
            Ok(VoProgramMetadata {
                fields: vec![VoFieldLink {
                    slot: 7,
                    field_idx: 255,
                    schema: VoMetadata {
                        root: "orders.Line".into(),
                        frame: vec![0x0a, 0x1b],
                        rows: vec![vec![0x00, 0xff], vec![0x10]],
                        profile: VoProfile {
                            records: vec![VoDefinition {
                                name: "orders.Line".into(),
                                definition: vec![0x0a],
                            }],
                            attributes: vec![VoDefinition {
                                name: "orders.qty".into(),
                                definition: vec![0x0b, 0x0c],
                            }],
                        },
                        retained: vec![vec![0x0c], vec![0x0d, 0x0e]],
                    },
                }],
                counts: vec![VoCountsRole {
                    instruction_offset: u32::MAX,
                    count_field: u16::MAX,
                    histogram_fields: vec![2, 3],
                    mask_fields: vec![],
                }],
            })
        );
        assert_eq!(
            parse_program_metadata(br#"{"counts":[],"fields":[]}"#),
            Ok(VoProgramMetadata::default())
        );
        let minimal = parse_program_metadata(program(LINK, ROLE).as_bytes()).unwrap();
        assert_eq!(minimal.fields[0].schema.profile, VoProfile::default());
        assert!(minimal.fields[0].schema.retained.is_empty());

        // Under a memory ceiling the decode refuses as OutOfMemory, never
        // traps: about 20 KB of metadata, at every ceiling from nothing up
        // to what the decode needs.
        let rows = vec![format!(r#""{}""#, "ab".repeat(150)); 64].join(",");
        let large = program(
            &format!(
                r#"{{"slot":1,"field_idx":2,"schema":{{"root":"orders.Line","frame":"0a1b","rows":[{rows}]}}}}"#
            ),
            ROLE,
        );
        assert!(large.len() > 19_000);
        let (decoded, need) =
            crate::memory::peak_bytes(|| parse_program_metadata(large.as_bytes()));
        assert_eq!(
            decoded.expect("unlimited decode").fields[0]
                .schema
                .rows
                .len(),
            64
        );
        let mut refused = 0;
        for ceiling in (0..need).step_by(256) {
            match crate::memory::with_ceiling(ceiling, || parse_program_metadata(large.as_bytes()))
            {
                Err(SchemaParseError::OutOfMemory) => refused += 1,
                Ok(_) => {}
                other => panic!("ceiling {ceiling}: {other:?}"),
            }
        }
        assert!(refused > 0, "a ceiling under the decode's need refuses it");
    }

    #[test]
    fn malformed_program_metadata_is_a_typed_schema_error() {
        use SchemaParseError::{
            DuplicateField, InvalidJson, InvalidOperand, InvalidTree, InvalidVoMetadata,
            MissingOperand,
        };
        let link = |from: &str, to: &str| program(&LINK.replace(from, to), ROLE);
        let role = |from: &str, to: &str| program(LINK, &ROLE.replace(from, to));
        // A schema with one more member after its rows.
        let with_member = |member: &str| {
            link(
                r#""rows":["00ff"]"#,
                &format!(r#""rows":["00ff"],{member}"#),
            )
        };
        let cases: Vec<(String, SchemaParseError)> = vec![
            ("{".into(), InvalidJson),
            ("[]".into(), InvalidTree),
            (r#"{"fields":[]}"#.into(), MissingOperand),
            (r#"{"counts":[]}"#.into(), MissingOperand),
            (
                r#"{"fields":[],"counts":[],"extra":[]}"#.into(),
                InvalidOperand,
            ),
            (
                r#"{"fields":[],"fields":[],"counts":[]}"#.into(),
                DuplicateField,
            ),
            (r#"{"fields":{},"counts":[]}"#.into(), InvalidOperand),
            (program("[]", ROLE), InvalidTree),
            (link(r#""slot":0,"#, ""), MissingOperand),
            (link(r#""slot":0"#, r#""slot":-1"#), InvalidOperand),
            (link(r#""slot":0"#, r#""slot":1.5"#), InvalidOperand),
            (link(r#""slot":0"#, r#""slot":"0""#), InvalidOperand),
            (link(r#""slot":0"#, r#""slot":4294967296"#), InvalidOperand),
            (
                link(r#""field_idx":2"#, r#""field_idx":256"#),
                InvalidOperand,
            ),
            (link(r#""slot":0"#, r#""slot":0,"extra":1"#), InvalidOperand),
            (link(r#""slot":0"#, r#""slot":0,"slot":1"#), DuplicateField),
            (
                link(
                    r#""schema":{"root":"orders.Line","frame":"0a1b","rows":["00ff"]}"#,
                    r#""schema":"orders.Line""#,
                ),
                InvalidTree,
            ),
            (with_member(r#""kind":"vo""#), InvalidOperand),
            (
                link(r#""root":"orders.Line","#, ""),
                InvalidVoMetadata(VoMetadataField::Root),
            ),
            (
                link(r#""frame":"0a1b""#, r#""frame":"0A1B""#),
                InvalidVoMetadata(VoMetadataField::Frame),
            ),
            (
                link(r#""rows":["00ff"]"#, r#""rows":[]"#),
                InvalidVoMetadata(VoMetadataField::Rows),
            ),
            (
                with_member(r#""profile":{"records":[{"name":"orders.Line","definition":"{}"}]}"#),
                InvalidVoMetadata(VoMetadataField::Profile),
            ),
            (
                with_member(r#""retained":["zz"]"#),
                InvalidVoMetadata(VoMetadataField::Retained),
            ),
            (
                role(
                    r#""instruction_offset":96"#,
                    r#""instruction_offset":4294967296"#,
                ),
                InvalidOperand,
            ),
            (
                role(r#""count_field":1"#, r#""count_field":65536"#),
                InvalidOperand,
            ),
            (role(r#","mask_fields":[4]"#, ""), MissingOperand),
            (
                role(r#""histogram_fields":[2,3]"#, r#""histogram_fields":"2""#),
                InvalidOperand,
            ),
            (
                role(
                    r#""histogram_fields":[2,3]"#,
                    r#""histogram_fields":[2,-3]"#,
                ),
                InvalidOperand,
            ),
            (
                role(r#""mask_fields":[4]"#, r#""mask_fields":[65536]"#),
                InvalidOperand,
            ),
            (
                role(r#""count_field":1"#, r#""count_field":1,"extra":1"#),
                InvalidOperand,
            ),
            (
                role(r#""count_field":1"#, r#""count_field":1,"count_field":2"#),
                DuplicateField,
            ),
        ];
        for (bytes, error) in cases {
            assert_eq!(
                parse_program_metadata(bytes.as_bytes()),
                Err(error.clone()),
                "{bytes} must refuse as {error:?}"
            );
        }
    }

    #[test]
    fn batch_slice_recovers_identity_and_never_matches_by_content() {
        let input = [1_u8, 2, 3, 1, 2, 3];
        let inner = &input[3..5];
        let slice = batch_slice(&input, inner).unwrap();
        assert!(core::ptr::eq(slice, inner));
        let elsewhere = [1_u8, 2];
        assert_eq!(batch_slice(&input, &elsewhere), None);
    }

    #[test]
    fn admission_payloads_are_nested_and_flat_member_keys_refuse() {
        let schemas = parse_schema_envelope(
            br#"{"hi":{"kind":"object","fields":{"greeting":{"kind":"optional","value":{"kind":"string"}}}}}"#,
        )
        .unwrap();
        let prepared = PreparedSchemas::prepare(schemas, UnavailableVoAdmission).unwrap();
        assert_eq!(
            prepared.validate_json_batch(br#"[{"type":"hi","value":{"greeting":"ok"}}]"#),
            Ok(())
        );
        match prepared.validate_json_batch(br#"[{"type":"hi","value.greeting":42}]"#) {
            Err(BatchValidationError::Violation {
                event_index: 0,
                event_type,
                violation,
            }) => {
                assert_eq!(event_type.as_deref(), Some("hi"));
                assert_eq!(
                    (violation.path.as_str(), violation.observed.as_str()),
                    ("value.greeting", "flat payload key")
                );
            }
            other => panic!("a flat payload key must refuse: {other:?}"),
        }
    }

    #[test]
    fn repeated_type_or_member_is_refused_before_either_spelling_is_judged() {
        let schemas = parse_schema_envelope(
            br#"{"hi":{"kind":"object","fields":{"greeting":{"kind":"string"}}}}"#,
        )
        .unwrap();
        let prepared = PreparedSchemas::prepare(schemas, UnavailableVoAdmission).unwrap();
        let Err(BatchValidationError::Violation {
            event_index,
            violation,
            ..
        }) = prepared
            .validate_json_batch(br#"[{"type":"hi","type":"hi","value":{"greeting":"ok"}}]"#)
        else {
            panic!("a repeated type must refuse");
        };
        assert_eq!(
            (
                event_index,
                violation.path.as_str(),
                violation.observed.as_str()
            ),
            (0, "type", "repeated key")
        );
        let Err(BatchValidationError::Violation { violation, .. }) = prepared
            .validate_json_batch(br#"[{"type":"hi","value":{"greeting":"ok","greeting":42}}]"#)
        else {
            panic!("a repeated member must refuse");
        };
        assert_eq!(
            (violation.path.as_str(), violation.observed.as_str()),
            ("value.greeting", "repeated key")
        );
    }

    #[test]
    fn text_that_is_not_utf8_is_never_a_binary_frame() {
        // {"line": <fixstr of two bytes that are not UTF-8>}: malformed text,
        // which must not reach the value-object capability as `bin`.
        let tree = schema(&format!(
            r#"{{"kind":"object","fields":{{"line":{}}}}}"#,
            line_tree()
        ));
        let input = [0x81, 0xa4, b'l', b'i', b'n', b'e', 0xa2, 0xff, 0xfe];
        let value = parse_msgpack_value(&mut Reader::new(&input), 0).unwrap();
        let violation =
            validate_value_with_admission(&tree, &value, &UnavailableVoAdmission).unwrap_err();
        assert_eq!(
            (
                violation.path.as_str(),
                violation.observed.as_str(),
                violation.vo
            ),
            ("value.line", "unknown", None)
        );
    }
}
