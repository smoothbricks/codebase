//! Validated dynamic Arrow schema storage and field metadata.
//!
//! The schema is supplied as one complete Arrow IPC Schema message. It is
//! decoded once during processor creation so the retained four-byte physical
//! metadata cannot disagree with the logical Arrow schema copied to output.

use arrow_ipc::{
    MessageHeader, convert::try_schema_from_ipc_buffer, root_as_message, writer::StreamWriter,
};
use arrow_schema::{
    DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DECIMAL256_MAX_PRECISION, DECIMAL256_MAX_SCALE,
    DataType, Field, IntervalUnit, Schema, TimeUnit,
};

use crate::columns::{MAX_EVENTS_PER_BATCH, MAX_VALUE_BYTES};

/// Maximum supported fields in one flattened schema.
pub const MAX_SCHEMA_FIELDS: usize = 256;

/// Largest declared byte width of a `FixedSizeBinary` field.
///
/// Derived, not chosen. A fixed-width column allocates `capacity * width`
/// eagerly, so this is the width at which one fixed-size-binary column costs
/// exactly what one variable-width column is allowed to retain. A wider field
/// would let a schema out-allocate a budget it was never granted.
pub const MAX_FIXED_SIZE_BINARY_WIDTH: u16 = (MAX_VALUE_BYTES / MAX_EVENTS_PER_BATCH) as u16;

/// Declares the plane table once: the [`ArrowType`] enum with its frozen tag
/// discriminants, and [`ArrowType::ALL`]. Generating the list together with
/// the enum is what stops the two from drifting — there is no second place to
/// forget a plane.
macro_rules! arrow_planes {
    ($( $(#[$attr:meta])* $variant:ident = $tag:literal => $kind:literal, )+) => {
        /// Arrow physical planes, matching the TypeScript `COMPACT_KIND_TAG`
        /// table byte for byte.
        ///
        /// A *plane* is one memory layout: a value width, a signedness, and a
        /// buffer count. Several logical Arrow types share a plane when they
        /// are byte-identical and read back through the same accessor — Date32
        /// and Time32 ride the four-byte signed plane, Date64, Time64,
        /// Timestamp and Duration ride the eight-byte signed plane.
        /// `logical_type_matches` is what keeps that sharing honest: a plane
        /// admits exactly its own logical types and rejects every other type
        /// of the same width.
        ///
        /// Tag values are the wire ABI shared with `parse-backend.ts` and are
        /// frozen: they are baked into the shipped `dist/event_processor.wasm`
        /// and into every persisted fixture. New planes append; nothing
        /// renumbers.
        ///
        /// # Nested and dictionary-encoded types are excluded
        ///
        /// List, LargeList, FixedSizeList, Struct, Union, Map, RunEndEncoded
        /// and Dictionary are deliberately absent and must not be added as
        /// further arms. The retained-metadata contract encodes at most three
        /// buffers per field — [`PlaneKind::buffer_count`] returns at most 3
        /// and `MetadataLimits::default()` sizes its buffer table as
        /// `MAX_SCHEMA_FIELDS * 3` — because every plane here is validity plus
        /// at most offsets plus data. A nested type owns child fields with
        /// their own buffers and their own field nodes, and a dictionary type
        /// owns a second message carrying the dictionary body. Both need a
        /// different layout contract: a recursive field descriptor and a
        /// per-field buffer budget. Adding one as an `ArrowType` arm would
        /// silently overflow the buffer table, not extend it.
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        #[repr(u8)]
        pub enum ArrowType {
            $( $(#[$attr])* $variant = $tag, )+
        }

        impl ArrowType {
            /// Every plane, ascending by tag. Generated from the same table as
            /// the enum, so it cannot fall behind it.
            pub const ALL: &'static [ArrowType] = &[ $( ArrowType::$variant, )+ ];

            /// This plane's name on the TypeScript side of the ABI.
            ///
            /// Declared with the tag rather than restated in TypeScript, because
            /// a hand-maintained copy is how tag 1 came to be called `'u32'`
            /// while the plane it selects is signed. `COMPACT_KIND_TAG` is
            /// generated from this, so a plane cannot be renamed on one side
            /// only, and a new plane cannot be added without naming it here —
            /// the macro will not expand without the name.
            pub const fn ts_kind(self) -> &'static str {
                match self {
                    $( ArrowType::$variant => $kind, )+
                }
            }
        }
    };
}

arrow_planes! {
    /// No values: one field node, zero buffers, every row null.
    Null = 0 => "null",
    /// The four-byte signed plane: one little-endian `i32` per row. Carries
    /// Int32, Date32 (days since epoch) and Time32 (seconds or milliseconds
    /// since midnight), which are byte-identical and told apart by the logical
    /// schema. UInt32, Float32 and Interval(YearMonth) are the same width but
    /// have their own planes, so they are rejected here.
    Int32 = 1 => "i32",
    /// One little-endian IEEE-754 `binary64` per row.
    Float64 = 2 => "f64",
    /// Opaque bytes behind 32-bit monotone offsets.
    Binary = 3 => "binary",
    /// UTF-8 validated bytes behind 32-bit monotone offsets.
    Utf8 = 4 => "utf8",
    /// One bit per row, Arrow LSB-first.
    Bool = 5 => "bool",
    /// The eight-byte signed plane: one little-endian `i64` per row. Carries
    /// Int64, Date64, Time64, Timestamp and Duration.
    Int64 = 6 => "i64",
    /// One little-endian `i8` per row.
    Int8 = 7 => "i8",
    /// One little-endian `i16` per row.
    Int16 = 8 => "i16",
    /// One `u8` per row.
    UInt8 = 9 => "u8",
    /// One little-endian `u16` per row.
    UInt16 = 10 => "u16",
    /// One little-endian `u32` per row. Distinct from [`ArrowType::Int32`]
    /// because the widening read differs: `0xFFFF_FFFF` is 4294967295 here and
    /// -1 there, and no width check can tell those apart.
    UInt32 = 11 => "u32",
    /// One little-endian `u64` per row.
    UInt64 = 12 => "u64",
    /// One little-endian IEEE-754 `binary16` per row, stored as its raw bits.
    Float16 = 13 => "f16",
    /// One little-endian IEEE-754 `binary32` per row.
    Float32 = 14 => "f32",
    /// Sixteen little-endian two's-complement bytes per row.
    Decimal128 = 15 => "decimal128",
    /// Thirty-two little-endian two's-complement bytes per row.
    Decimal256 = 16 => "decimal256",
    /// Opaque bytes behind 64-bit monotone offsets.
    LargeBinary = 17 => "largeBinary",
    /// UTF-8 validated bytes behind 64-bit monotone offsets.
    LargeUtf8 = 18 => "largeUtf8",
    /// The one parameterized plane: `SignalSchemaField::type_param` bytes per
    /// row, bounded by [`MAX_FIXED_SIZE_BINARY_WIDTH`].
    FixedSizeBinary = 19 => "fixedSizeBinary",
    /// One little-endian `i32` month count per row.
    IntervalYearMonth = 20 => "intervalYearMonth",
    /// Eight bytes per row: little-endian `i32` days then `i32` milliseconds.
    IntervalDayTime = 21 => "intervalDayTime",
    /// Sixteen bytes per row: little-endian `i32` months, `i32` days, `i64`
    /// nanoseconds.
    IntervalMonthDayNano = 22 => "intervalMonthDayNano",
    // Nested and dictionary-encoded types do NOT belong here. See the
    // `ArrowType` doc comment: the retained-metadata contract encodes at most
    // three buffers per field, and a nested or dictionary type needs a
    // different layout contract rather than another arm.
}

impl ArrowType {
    /// Decode a physical type byte without constructing an invalid enum.
    ///
    /// Derived from [`ArrowType::ALL`] rather than restating the tag table: a
    /// second copy of 23 numbers is a second copy that can be wrong. Called
    /// once per field per schema, so the scan is not on any hot path.
    pub fn from_u8(value: u8) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|plane| *plane as u8 == value)
    }
}

/// What one cell of a plane is, and therefore how the plane is stored.
///
/// This is the single per-plane classification: storage sizing, Arrow buffer
/// count, IPC buffer shape, producer range checks and reader widening all
/// derive from it, so a plane is described in one place instead of five.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlaneKind {
    /// No cells and no buffers.
    Empty,
    /// One bit per row, Arrow LSB-first.
    Bool,
    /// Signed two's-complement integer, `width` little-endian bytes per row.
    SignedInt { width: u32 },
    /// Unsigned integer, `width` little-endian bytes per row.
    UnsignedInt { width: u32 },
    /// IEEE-754 binary float, `width` little-endian bytes per row.
    Float { width: u32 },
    /// Opaque fixed-size value, `width` bytes per row: decimals, the interval
    /// units that are not a single scalar, and fixed-size binary. The plane
    /// does not interpret the bytes; the logical type does.
    FixedBytes { width: u32 },
    /// UTF-8 validated bytes behind `offset_width`-byte monotone offsets.
    Text { offset_width: u32 },
    /// Opaque bytes behind `offset_width`-byte monotone offsets.
    Bytes { offset_width: u32 },
}

impl PlaneKind {
    /// Bytes of value storage per row, or `None` for the bit-packed,
    /// variable-width and empty kinds, which do not have one.
    pub fn value_width(self) -> Option<u32> {
        match self {
            Self::SignedInt { width }
            | Self::UnsignedInt { width }
            | Self::Float { width }
            | Self::FixedBytes { width } => Some(width),
            Self::Empty | Self::Bool | Self::Text { .. } | Self::Bytes { .. } => None,
        }
    }

    /// Byte width of one offset entry, or `None` for the kinds without an
    /// offsets buffer.
    pub fn offset_width(self) -> Option<u32> {
        match self {
            Self::Text { offset_width } | Self::Bytes { offset_width } => Some(offset_width),
            Self::Empty
            | Self::Bool
            | Self::SignedInt { .. }
            | Self::UnsignedInt { .. }
            | Self::Float { .. }
            | Self::FixedBytes { .. } => None,
        }
    }

    /// Arrow IPC buffers this kind contributes: validity, then offsets when
    /// the values are variable-width, then data.
    ///
    /// The maximum is 3, and `MetadataLimits::default()` depends on that.
    pub fn buffer_count(self) -> u32 {
        match self {
            Self::Empty => 0,
            Self::Text { .. } | Self::Bytes { .. } => 3,
            Self::Bool
            | Self::SignedInt { .. }
            | Self::UnsignedInt { .. }
            | Self::Float { .. }
            | Self::FixedBytes { .. } => 2,
        }
    }

    /// Does `value` fit this kind's signed storage? `false` for every kind
    /// that does not hold signed integers.
    ///
    /// The producers ask this before building a cell so an out-of-range number
    /// is reported as a bad number where it was read, and `append_int` asks it
    /// again so no caller can bypass the check. One rule, two callers.
    pub fn holds_int(self, value: i64) -> bool {
        let Self::SignedInt { width } = self else {
            return false;
        };
        // An eight-byte plane holds every `i64`, and computing its bound would
        // overflow the shift.
        width >= 8 || {
            let limit = 1i64 << (width * 8 - 1);
            value >= -limit && value < limit
        }
    }

    /// Does `value` fit this kind's unsigned storage? `false` for every kind
    /// that does not hold unsigned integers.
    pub fn holds_uint(self, value: u64) -> bool {
        let Self::UnsignedInt { width } = self else {
            return false;
        };
        width >= 8 || value < 1u64 << (width * 8)
    }
}

/// Four-byte physical field descriptor: `[type, nullable, type_param u16 LE]`.
///
/// `type_param` is the `FixedSizeBinary` byte width and must be zero for every
/// other plane. It exists so the physical descriptor alone determines the
/// layout of every plane: a fixed-size-binary field without a width is not
/// representable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct SignalSchemaField {
    pub arrow_type: ArrowType,
    pub nullable: u8,
    pub type_param: u16,
}

/// The base event-log schema: `id`, `type`, `timestamp`, `value`, in that
/// order. The scanners in `columine-parsing` write exactly these four columns
/// by index, so this is the only schema they can serve.
pub const BASE_EVENT_LOG_FIELDS: [SignalSchemaField; 4] = [
    SignalSchemaField::new(ArrowType::Utf8, false),
    SignalSchemaField::new(ArrowType::Utf8, false),
    SignalSchemaField::new(ArrowType::Int64, false),
    SignalSchemaField::new(ArrowType::Binary, true),
];

/// Field names of the base event-log schema, matching
/// [`BASE_EVENT_LOG_FIELDS`] by index.
pub const BASE_EVENT_LOG_NAMES: [&str; 4] = ["id", "type", "timestamp", "value"];

impl SignalSchemaField {
    pub const fn new(arrow_type: ArrowType, nullable: bool) -> Self {
        Self {
            arrow_type,
            nullable: nullable as u8,
            type_param: 0,
        }
    }

    /// A `FixedSizeBinary` field of `byte_width` bytes per row. The width is
    /// validated when the schema is built, not here, so that a bad width fails
    /// as a schema error rather than a panic.
    pub const fn new_fixed_size_binary(byte_width: u16, nullable: bool) -> Self {
        Self {
            arrow_type: ArrowType::FixedSizeBinary,
            nullable: nullable as u8,
            type_param: byte_width,
        }
    }

    pub fn is_nullable(self) -> bool {
        self.nullable == 1
    }

    /// Physical layout of this field. Total: `FixedSizeBinary` resolves its
    /// per-field byte width from `type_param`, so no caller has to know that
    /// one plane is parameterized.
    pub fn plane_kind(self) -> PlaneKind {
        match self.arrow_type {
            ArrowType::Null => PlaneKind::Empty,
            ArrowType::Bool => PlaneKind::Bool,
            ArrowType::Int8 => PlaneKind::SignedInt { width: 1 },
            ArrowType::Int16 => PlaneKind::SignedInt { width: 2 },
            ArrowType::Int32 | ArrowType::IntervalYearMonth => PlaneKind::SignedInt { width: 4 },
            ArrowType::Int64 => PlaneKind::SignedInt { width: 8 },
            ArrowType::UInt8 => PlaneKind::UnsignedInt { width: 1 },
            ArrowType::UInt16 => PlaneKind::UnsignedInt { width: 2 },
            ArrowType::UInt32 => PlaneKind::UnsignedInt { width: 4 },
            ArrowType::UInt64 => PlaneKind::UnsignedInt { width: 8 },
            ArrowType::Float16 => PlaneKind::Float { width: 2 },
            ArrowType::Float32 => PlaneKind::Float { width: 4 },
            ArrowType::Float64 => PlaneKind::Float { width: 8 },
            ArrowType::IntervalDayTime => PlaneKind::FixedBytes { width: 8 },
            ArrowType::Decimal128 | ArrowType::IntervalMonthDayNano => {
                PlaneKind::FixedBytes { width: 16 }
            }
            ArrowType::Decimal256 => PlaneKind::FixedBytes { width: 32 },
            ArrowType::FixedSizeBinary => PlaneKind::FixedBytes {
                width: u32::from(self.type_param),
            },
            ArrowType::Utf8 => PlaneKind::Text { offset_width: 4 },
            ArrowType::LargeUtf8 => PlaneKind::Text { offset_width: 8 },
            ArrowType::Binary => PlaneKind::Bytes { offset_width: 4 },
            ArrowType::LargeBinary => PlaneKind::Bytes { offset_width: 8 },
        }
    }

    pub fn buffer_count(self) -> u32 {
        self.plane_kind().buffer_count()
    }

    /// Canonical Arrow `DataType` for this field: the logical type a schema
    /// synthesized from physical tags declares.
    ///
    /// Planes shared by several logical types return their canonical one
    /// (Int32 for the four-byte signed plane), and the parameterized logical
    /// types return the representative that spans the whole plane
    /// (`Decimal128(38, 0)` is the widest legal Decimal128).
    pub fn to_data_type(self) -> DataType {
        match self.arrow_type {
            ArrowType::Null => DataType::Null,
            ArrowType::Bool => DataType::Boolean,
            ArrowType::Int8 => DataType::Int8,
            ArrowType::Int16 => DataType::Int16,
            ArrowType::Int32 => DataType::Int32,
            ArrowType::Int64 => DataType::Int64,
            ArrowType::UInt8 => DataType::UInt8,
            ArrowType::UInt16 => DataType::UInt16,
            ArrowType::UInt32 => DataType::UInt32,
            ArrowType::UInt64 => DataType::UInt64,
            ArrowType::Float16 => DataType::Float16,
            ArrowType::Float32 => DataType::Float32,
            ArrowType::Float64 => DataType::Float64,
            ArrowType::Decimal128 => DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0),
            ArrowType::Decimal256 => DataType::Decimal256(DECIMAL256_MAX_PRECISION, 0),
            ArrowType::Binary => DataType::Binary,
            ArrowType::LargeBinary => DataType::LargeBinary,
            ArrowType::FixedSizeBinary => DataType::FixedSizeBinary(i32::from(self.type_param)),
            ArrowType::Utf8 => DataType::Utf8,
            ArrowType::LargeUtf8 => DataType::LargeUtf8,
            ArrowType::IntervalYearMonth => DataType::Interval(IntervalUnit::YearMonth),
            ArrowType::IntervalDayTime => DataType::Interval(IntervalUnit::DayTime),
            ArrowType::IntervalMonthDayNano => DataType::Interval(IntervalUnit::MonthDayNano),
        }
    }

    /// Physical-metadata self-consistency: `type_param` is the
    /// `FixedSizeBinary` byte width and must be zero for every other plane.
    fn param_is_valid(self) -> bool {
        match self.arrow_type {
            // The only parameterized plane. Every other plane's layout is
            // fully determined by its tag, so a nonzero param there is a
            // producer bug, not a wider field.
            ArrowType::FixedSizeBinary => {
                (1..=MAX_FIXED_SIZE_BINARY_WIDTH).contains(&self.type_param)
            }
            _ => self.type_param == 0,
        }
    }
}

/// Buffer count for a schema: each field contributes per its type.
pub fn compute_buffer_count(fields: &[SignalSchemaField]) -> u32 {
    fields.iter().map(|field| field.buffer_count()).sum()
}

/// One continuation-prefixed IPC Schema message for explicit logical fields.
///
/// The physical-tag encoder below and every caller that needs a non-canonical
/// logical type — Date32 on the four-byte plane, Timestamp on the eight-byte
/// plane — share this one StreamWriter dance.
pub fn logical_schema_ipc_bytes(fields: Vec<Field>) -> Result<Vec<u8>, SchemaError> {
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, &Schema::new(fields))
            .map_err(|_| SchemaError::InvalidMessage)?;
        writer.finish().map_err(|_| SchemaError::InvalidMessage)?;
    }
    bytes.truncate(bytes.len().saturating_sub(8));
    Ok(bytes)
}

/// One continuation-prefixed IPC Schema message for `fields`, using each
/// plane's canonical logical type.
pub fn schema_ipc_bytes(fields: &[SignalSchemaField]) -> Result<Vec<u8>, SchemaError> {
    logical_schema_ipc_bytes(
        fields
            .iter()
            .enumerate()
            .map(|(index, metadata)| {
                Field::new(
                    format!("field_{index}"),
                    metadata.to_data_type(),
                    metadata.is_nullable(),
                )
            })
            .collect(),
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchemaError {
    InvalidMessage,
    TooManyFields,
    InvalidFieldMetadata { field_index: usize },
    FieldCountMismatch { schema: usize, metadata: usize },
    TypeMismatch { field_index: usize },
    NullabilityMismatch { field_index: usize },
    InvalidFieldNames,
}

/// Payload members whose column depends on the value of one envelope member.
///
/// The extractors read the envelope member named `key`; its string value is
/// the discriminant. A nested payload member then resolves to the column
/// `members` names for `(discriminant, member)` before the plain
/// `value.<member>` column, and a member the semantic schema armed under the
/// same discriminant declares as an enum lands as the ordinal of the variant
/// string the wire spells. Which columns are discriminated, and how their
/// names encode that, is the caller's convention: this table is its result,
/// not its rule.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PayloadDiscriminator {
    pub key: String,
    pub members: Vec<DiscriminatedMember>,
}

/// One `(discriminant, payload member)` → column binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscriminatedMember {
    pub discriminant: String,
    pub member: String,
    pub column: usize,
}

/// Owned, validated schema configuration retained by an EventProcessor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DynamicSchemaConfig {
    /// One complete continuation-prefixed Arrow IPC Schema message.
    pub schema_bytes: Vec<u8>,
    pub field_metadata: Vec<SignalSchemaField>,
    /// Logical types decoded from `schema_bytes`, in field order.
    pub logical_types: Vec<DataType>,
    /// Optional canonical-JSON semantic schema envelope:
    /// `{"<eventType>": <tree>, ...}`. `None` preserves the pre-validation
    /// behavior; when present it is immutable for the lifetime of an EP.
    pub semantic_schema: Option<Vec<u8>>,
    /// True when this schema IS the base event log — the four
    /// [`BASE_EVENT_LOG_FIELDS`] with the [`BASE_EVENT_LOG_NAMES`] when names
    /// were supplied. The scanners write those four columns by index, so
    /// anything else must go through schema-driven extraction.
    pub is_base_event_log: bool,
    pub field_names: Vec<String>,
    /// Discriminated payload columns and the envelope key that selects them;
    /// `None` resolves every nested payload member by its plain name.
    pub payload_discriminator: Option<PayloadDiscriminator>,
}

impl DynamicSchemaConfig {
    pub fn new(
        schema_bytes: &[u8],
        field_metadata: &[SignalSchemaField],
    ) -> Result<Self, SchemaError> {
        validate_typed_metadata(field_metadata)?;
        Self::build(schema_bytes, field_metadata.to_vec(), Vec::new())
    }

    /// Synthesize an IPC Schema message from physical tags so tests do not
    /// restate the ArrowType→DataType table.
    pub fn from_physical_fields(fields: &[SignalSchemaField]) -> Result<Self, SchemaError> {
        Self::new(&schema_ipc_bytes(fields)?, fields)
    }

    pub fn from_physical_fields_with_names(
        fields: &[SignalSchemaField],
        field_names_raw: &[u8],
    ) -> Result<Self, SchemaError> {
        Self::with_field_names(&schema_ipc_bytes(fields)?, fields, field_names_raw)
    }

    /// Decode the untrusted four-byte-per-field FFI metadata table.
    pub fn from_wire(schema_bytes: &[u8], field_metadata: &[u8]) -> Result<Self, SchemaError> {
        let fields = decode_field_metadata(field_metadata)?;
        Self::build(schema_bytes, fields, Vec::new())
    }

    pub fn with_field_names(
        schema_bytes: &[u8],
        field_metadata: &[SignalSchemaField],
        field_names_raw: &[u8],
    ) -> Result<Self, SchemaError> {
        validate_typed_metadata(field_metadata)?;
        let names = parse_field_names(field_names_raw)?;
        Self::build(schema_bytes, field_metadata.to_vec(), names)
    }

    pub fn from_wire_with_field_names(
        schema_bytes: &[u8],
        field_metadata: &[u8],
        field_names_raw: &[u8],
    ) -> Result<Self, SchemaError> {
        let fields = decode_field_metadata(field_metadata)?;
        let names = parse_field_names(field_names_raw)?;
        Self::build(schema_bytes, fields, names)
    }

    fn build(
        schema_bytes: &[u8],
        field_metadata: Vec<SignalSchemaField>,
        field_names: Vec<String>,
    ) -> Result<Self, SchemaError> {
        let schema = decode_schema_message(schema_bytes)?;
        if schema.fields().len() > MAX_SCHEMA_FIELDS {
            return Err(SchemaError::TooManyFields);
        }
        if schema.fields().len() != field_metadata.len() {
            return Err(SchemaError::FieldCountMismatch {
                schema: schema.fields().len(),
                metadata: field_metadata.len(),
            });
        }
        if !field_names.is_empty() && field_names.len() != field_metadata.len() {
            return Err(SchemaError::InvalidFieldNames);
        }

        let mut logical_types = Vec::with_capacity(field_metadata.len());
        for (field_index, (field, metadata)) in schema
            .fields()
            .iter()
            .zip(field_metadata.iter())
            .enumerate()
        {
            if !logical_type_matches(*metadata, field.data_type()) {
                return Err(SchemaError::TypeMismatch { field_index });
            }
            if field.is_nullable() != metadata.is_nullable()
                || (metadata.arrow_type == ArrowType::Null && !metadata.is_nullable())
            {
                return Err(SchemaError::NullabilityMismatch { field_index });
            }
            logical_types.push(field.data_type().clone());
        }

        // A field COUNT of four proved nothing: any four-field extraction
        // schema was classified as the event log and then written with
        // hard-coded utf8/utf8/int64/binary buffers, producing a stream whose
        // Schema message and RecordBatch body disagreed. Identity is the four
        // physical types, plus the names when the caller supplied them.
        let is_base_event_log = field_metadata == BASE_EVENT_LOG_FIELDS
            && (field_names.is_empty()
                || field_names
                    .iter()
                    .zip(BASE_EVENT_LOG_NAMES)
                    .all(|(actual, expected)| actual == expected));

        Ok(Self {
            is_base_event_log,
            payload_discriminator: None,
            schema_bytes: schema_bytes.to_vec(),
            field_metadata,
            logical_types,
            semantic_schema: None,
            field_names,
        })
    }

    pub fn compute_buffer_count(&self) -> u32 {
        compute_buffer_count(&self.field_metadata)
    }

    pub fn schema_message_size(&self) -> usize {
        self.schema_bytes.len()
    }

    pub fn write_schema_message(&self, output: &mut [u8]) -> usize {
        if output.len() < self.schema_bytes.len() {
            return 0;
        }
        output[..self.schema_bytes.len()].copy_from_slice(&self.schema_bytes);
        self.schema_bytes.len()
    }
}

fn decode_schema_message(bytes: &[u8]) -> Result<arrow_schema::Schema, SchemaError> {
    if bytes.len() < 8 || bytes[..4] != [0xff; 4] {
        return Err(SchemaError::InvalidMessage);
    }
    let payload_len = u32::from_le_bytes(
        bytes[4..8]
            .try_into()
            .map_err(|_| SchemaError::InvalidMessage)?,
    ) as usize;
    let expected_len = 8usize
        .checked_add(payload_len)
        .ok_or(SchemaError::InvalidMessage)?;
    if payload_len == 0 || !payload_len.is_multiple_of(8) || expected_len != bytes.len() {
        return Err(SchemaError::InvalidMessage);
    }
    let message = root_as_message(&bytes[8..]).map_err(|_| SchemaError::InvalidMessage)?;
    if message.header_type() != MessageHeader::Schema || message.bodyLength() != 0 {
        return Err(SchemaError::InvalidMessage);
    }
    try_schema_from_ipc_buffer(bytes).map_err(|_| SchemaError::InvalidMessage)
}

/// Does `logical` name a type this plane can actually store?
///
/// Physical width agreeing is NOT the test. Every logical Arrow type belongs
/// to exactly one plane, so a plane rejects every other type of its own width:
/// UInt32 is not admitted on the four-byte signed plane and Float32 is not
/// admitted on either, because reading those bytes back through the wrong
/// plane produces a different number, not a different name.
fn logical_type_matches(field: SignalSchemaField, logical: &DataType) -> bool {
    match field.arrow_type {
        ArrowType::Null => matches!(logical, DataType::Null),
        ArrowType::Bool => matches!(logical, DataType::Boolean),
        ArrowType::Int8 => matches!(logical, DataType::Int8),
        ArrowType::Int16 => matches!(logical, DataType::Int16),
        // The four-byte signed plane. Date32 counts days since the epoch and
        // Time32 counts seconds or milliseconds since midnight; both are a
        // little-endian `i32` and are told apart by the logical schema, not by
        // the tag. Arrow restricts Time32 to Second and Millisecond, so a
        // Time32 in microseconds is not a legal Arrow type and is rejected
        // here rather than accepted because it happens to be four bytes.
        ArrowType::Int32 => matches!(
            logical,
            DataType::Int32
                | DataType::Date32
                | DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
        ),
        // The eight-byte signed plane, same rule. Date64 counts milliseconds
        // since the epoch, Time64 counts microseconds or nanoseconds since
        // midnight (the only two units Arrow allows for it), and Timestamp and
        // Duration carry their unit — and Timestamp its zone — in the logical
        // type where a reader can see it.
        ArrowType::Int64 => matches!(
            logical,
            DataType::Int64
                | DataType::Date64
                | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond)
                | DataType::Timestamp(_, _)
                | DataType::Duration(_)
        ),
        ArrowType::UInt8 => matches!(logical, DataType::UInt8),
        ArrowType::UInt16 => matches!(logical, DataType::UInt16),
        ArrowType::UInt32 => matches!(logical, DataType::UInt32),
        ArrowType::UInt64 => matches!(logical, DataType::UInt64),
        ArrowType::Float16 => matches!(logical, DataType::Float16),
        ArrowType::Float32 => matches!(logical, DataType::Float32),
        ArrowType::Float64 => matches!(logical, DataType::Float64),
        ArrowType::Decimal128 => matches!(
            logical,
            DataType::Decimal128(precision, scale)
                if decimal_fits(*precision, *scale, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE)
        ),
        ArrowType::Decimal256 => matches!(
            logical,
            DataType::Decimal256(precision, scale)
                if decimal_fits(*precision, *scale, DECIMAL256_MAX_PRECISION, DECIMAL256_MAX_SCALE)
        ),
        ArrowType::Binary => matches!(logical, DataType::Binary),
        ArrowType::LargeBinary => matches!(logical, DataType::LargeBinary),
        // The declared byte width has to agree with the physical descriptor:
        // the column store sizes this plane from `type_param`, so a logical
        // FixedSizeBinary(32) over a 16-byte plane would read past every row.
        ArrowType::FixedSizeBinary => {
            *logical == DataType::FixedSizeBinary(i32::from(field.type_param))
        }
        ArrowType::Utf8 => matches!(logical, DataType::Utf8),
        ArrowType::LargeUtf8 => matches!(logical, DataType::LargeUtf8),
        ArrowType::IntervalYearMonth => {
            matches!(logical, DataType::Interval(IntervalUnit::YearMonth))
        }
        ArrowType::IntervalDayTime => matches!(logical, DataType::Interval(IntervalUnit::DayTime)),
        ArrowType::IntervalMonthDayNano => {
            matches!(logical, DataType::Interval(IntervalUnit::MonthDayNano))
        }
    }
}

/// Does a decimal of this precision and scale fit the plane's storage width?
///
/// Bounds come from the `DECIMAL*_MAX_*` constants arrow-schema publishes, so
/// `Decimal128(50, 0)` — which does not fit 128 bits — is rejected rather than
/// stored as sixteen bytes that cannot hold it.
fn decimal_fits(precision: u8, scale: i8, max_precision: u8, max_scale: i8) -> bool {
    precision >= 1 && precision <= max_precision && scale >= -max_scale && scale <= max_scale
}

fn validate_typed_metadata(fields: &[SignalSchemaField]) -> Result<(), SchemaError> {
    if fields.len() > MAX_SCHEMA_FIELDS {
        return Err(SchemaError::TooManyFields);
    }
    for (field_index, field) in fields.iter().enumerate() {
        if field.nullable > 1 || !field.param_is_valid() {
            return Err(SchemaError::InvalidFieldMetadata { field_index });
        }
    }
    Ok(())
}

fn decode_field_metadata(bytes: &[u8]) -> Result<Vec<SignalSchemaField>, SchemaError> {
    if !bytes.len().is_multiple_of(4) || bytes.len() / 4 > MAX_SCHEMA_FIELDS {
        return Err(if bytes.len() / 4 > MAX_SCHEMA_FIELDS {
            SchemaError::TooManyFields
        } else {
            SchemaError::InvalidFieldMetadata { field_index: 0 }
        });
    }
    let mut fields = Vec::with_capacity(bytes.len() / 4);
    for (field_index, raw) in bytes.as_chunks::<4>().0.iter().enumerate() {
        let Some(arrow_type) = ArrowType::from_u8(raw[0]) else {
            return Err(SchemaError::InvalidFieldMetadata { field_index });
        };
        let field = SignalSchemaField {
            arrow_type,
            nullable: raw[1],
            type_param: u16::from_le_bytes([raw[2], raw[3]]),
        };
        if raw[1] > 1 || !field.param_is_valid() {
            return Err(SchemaError::InvalidFieldMetadata { field_index });
        }
        fields.push(field);
    }
    Ok(fields)
}

fn parse_field_names(raw: &[u8]) -> Result<Vec<String>, SchemaError> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    if raw.last() != Some(&0) {
        return Err(SchemaError::InvalidFieldNames);
    }
    raw[..raw.len() - 1]
        .split(|byte| *byte == 0)
        .map(|name| {
            if name.is_empty() {
                return Err(SchemaError::InvalidFieldNames);
            }
            std::str::from_utf8(name)
                .map(str::to_owned)
                .map_err(|_| SchemaError::InvalidFieldNames)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{Field, Schema};

    fn schema_message(schema: &Schema) -> Vec<u8> {
        let mut bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut bytes, schema).unwrap();
            writer.finish().unwrap();
        }
        assert!(bytes.ends_with(&[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0]));
        bytes.truncate(bytes.len() - 8);
        bytes
    }

    fn base_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("type", DataType::Utf8, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Binary, true),
        ])
    }

    fn base_fields() -> [SignalSchemaField; 4] {
        [
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new(ArrowType::Int64, false),
            SignalSchemaField::new(ArrowType::Binary, true),
        ]
    }

    #[test]
    fn field_metadata_layout_and_discriminants_are_stable() {
        // Four bytes on the wire is the ABI. Alignment is two because the
        // fourth and third bytes are one `u16` type_param, and nothing casts a
        // byte slice to this struct — `decode_field_metadata` reads the four
        // bytes explicitly.
        assert_eq!(core::mem::size_of::<SignalSchemaField>(), 4);
        assert_eq!(core::mem::align_of::<SignalSchemaField>(), 2);

        // The seven original tags are frozen: they are baked into the shipped
        // wasm and into persisted fixtures.
        for (value, expected) in [
            (0, ArrowType::Null),
            (1, ArrowType::Int32),
            (2, ArrowType::Float64),
            (3, ArrowType::Binary),
            (4, ArrowType::Utf8),
            (5, ArrowType::Bool),
            (6, ArrowType::Int64),
        ] {
            assert_eq!(expected as u8, value);
            assert_eq!(ArrowType::from_u8(value), Some(expected));
        }

        // `ALL` is the whole table, ascending and gapless, and `from_u8` is
        // exactly its inverse.
        for (index, plane) in ArrowType::ALL.iter().enumerate() {
            assert_eq!(*plane as u8 as usize, index);
            assert_eq!(ArrowType::from_u8(index as u8), Some(*plane));
        }
        let highest = ArrowType::ALL.len() as u8;
        for value in highest..=u8::MAX {
            assert_eq!(
                ArrowType::from_u8(value),
                None,
                "tag {value} is not a plane"
            );
        }
    }

    #[test]
    fn real_schema_message_is_decoded_and_retained() {
        let bytes = schema_message(&base_schema());
        let config = DynamicSchemaConfig::new(&bytes, &base_fields()).unwrap();
        assert_eq!(config.schema_bytes, bytes);
        assert_eq!(config.logical_types[2], DataType::Int64);
        assert_eq!(config.compute_buffer_count(), 11);
        assert!(config.is_base_event_log);

        let mut output = vec![0; bytes.len()];
        assert_eq!(config.write_schema_message(&mut output), bytes.len());
        assert_eq!(output, bytes);
        assert_eq!(config.write_schema_message(&mut [0; 4]), 0);
    }

    #[test]
    fn schema_metadata_type_count_and_nullability_must_agree() {
        let bytes = schema_message(&base_schema());
        assert!(matches!(
            DynamicSchemaConfig::new(&bytes, &base_fields()[..3]),
            Err(SchemaError::FieldCountMismatch { .. })
        ));

        let mut fields = base_fields();
        fields[2] = SignalSchemaField::new(ArrowType::Float64, false);
        assert_eq!(
            DynamicSchemaConfig::new(&bytes, &fields),
            Err(SchemaError::TypeMismatch { field_index: 2 })
        );

        let mut fields = base_fields();
        fields[3] = SignalSchemaField::new(ArrowType::Binary, false);
        assert_eq!(
            DynamicSchemaConfig::new(&bytes, &fields),
            Err(SchemaError::NullabilityMismatch { field_index: 3 })
        );
    }

    #[test]
    fn wire_metadata_is_decoded_without_enum_casts() {
        let bytes = schema_message(&base_schema());
        let raw = [
            4, 0, 0, 0, // Utf8
            4, 0, 0, 0, // Utf8
            6, 0, 0, 0, // Int64
            3, 1, 0, 0, // nullable Binary
        ];
        let config = DynamicSchemaConfig::from_wire(&bytes, &raw).unwrap();
        assert_eq!(config.field_metadata, base_fields());

        let mut invalid = raw;
        invalid[8] = 255;
        assert_eq!(
            DynamicSchemaConfig::from_wire(&bytes, &invalid),
            Err(SchemaError::InvalidFieldMetadata { field_index: 2 })
        );
        let mut invalid = raw;
        invalid[1] = 2;
        assert_eq!(
            DynamicSchemaConfig::from_wire(&bytes, &invalid),
            Err(SchemaError::InvalidFieldMetadata { field_index: 0 })
        );
    }

    #[test]
    fn malformed_or_non_schema_messages_are_rejected() {
        let bytes = schema_message(&base_schema());
        assert_eq!(
            DynamicSchemaConfig::new(&bytes[..bytes.len() - 1], &base_fields()),
            Err(SchemaError::InvalidMessage)
        );
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert_eq!(
            DynamicSchemaConfig::new(&trailing, &base_fields()),
            Err(SchemaError::InvalidMessage)
        );
    }

    #[test]
    fn names_are_strict_utf8_terminated_and_ordered() {
        let bytes = schema_message(&base_schema());
        let config = DynamicSchemaConfig::with_field_names(
            &bytes,
            &base_fields(),
            b"id\0type\0timestamp\0value\0",
        )
        .unwrap();
        assert_eq!(config.field_names, ["id", "type", "timestamp", "value"]);
        assert_eq!(
            DynamicSchemaConfig::with_field_names(&bytes, &base_fields(), b"id\0type"),
            Err(SchemaError::InvalidFieldNames)
        );
    }
}
