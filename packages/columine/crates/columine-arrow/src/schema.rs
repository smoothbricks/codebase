//! Validated dynamic Arrow schema storage and field metadata.
//!
//! The schema is supplied as one complete Arrow IPC Schema message. It is
//! decoded once during processor creation so the retained eight-byte physical
//! descriptors cannot disagree with the logical Arrow schema copied to output.
//! The design is `specs/columine/10-physical-vocabulary.md`.

use crate::fallible::copied;
use crate::record_batch::{DictionaryValues, dictionary_batch_len, encode_dictionary_batch};
use arrow_ipc::{
    MessageHeader, convert::try_schema_from_ipc_buffer, root_as_message, writer::StreamWriter,
};
use arrow_schema::{
    DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DECIMAL256_MAX_PRECISION, DECIMAL256_MAX_SCALE,
    DataType, Field, IntervalUnit, Schema, TimeUnit,
};

use crate::columns::{MAX_EVENTS_PER_BATCH, MAX_VALUE_BYTES};

/// Maximum physical Arrow fields in one flattened schema.
///
/// This is the processor's column capacity, not a row, event or decision
/// bound. Nothing is sized by it: retained metadata and the compact
/// validator's range table take their counts from the schema's own layout
/// ([`compute_buffer_count`]). The TypeScript side reads it from the
/// generated ABI table. A schema this wide is still refused when its Schema
/// message passes [`MAX_SCHEMA_MESSAGE_BYTES`].
pub const MAX_SCHEMA_FIELDS: usize = 512;

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
/// forget a plane. This invocation is the source of the host's plane table:
/// `columine-event-processor/tests/host_abi.rs` renders it into
/// `packages/columine/src/event-processor-abi.generated.ts`.
macro_rules! arrow_planes {
    ($( $(#[$attr:meta])* $variant:ident = $tag:literal => $kind:literal, )+) => {
        /// Arrow physical planes. The table below is the source of the host's
        /// plane table: `columine-event-processor/tests/host_abi.rs` renders it
        /// into `event-processor-abi.generated.ts`, deriving each plane's typed
        /// arrays from [`SignalSchemaField::value_kind`].
        ///
        /// A *plane* is one memory layout: a value width, a signedness, and a
        /// buffer count. Several logical Arrow types share a plane when they
        /// are byte-identical and read back through the same accessor — Date32
        /// and Time32 ride the four-byte signed plane, Date64 and Time64
        /// ride the eight-byte signed plane. Timestamp and Duration have
        /// planes of their own, because their unit decides what a producer's
        /// value means and so belongs in the descriptor.
        /// `logical_type_matches` is what keeps that sharing honest: a plane
        /// admits exactly its own logical types and rejects every other type
        /// of the same width.
        ///
        /// Tag values are the wire ABI the generated host table carries and are
        /// frozen: they are baked into the shipped `dist/event_processor.wasm`
        /// and into every persisted fixture. New planes append; nothing
        /// renumbers.
        ///
        /// # A plane is a value layout; dictionary encoding is not a plane
        ///
        /// A dictionary-encoded column keeps its value plane here (Utf8) and
        /// says in its descriptor which dictionary and which index width it
        /// uses ([`DictionaryEncoding`]), as an Arrow field does. Its buffers
        /// are then the index plane's, and the dictionary's values travel in
        /// their own message, sized from the dictionary and not from a
        /// per-field constant.
        ///
        /// # Nested types are excluded
        ///
        /// List, LargeList, FixedSizeList, Struct, Union, Map and
        /// RunEndEncoded are absent. A nested type owns child fields with
        /// their own field nodes and buffers, so it needs a recursive
        /// descriptor whose buffer count is the sum over its children; the
        /// layout's counts are already computed per field
        /// ([`SignalSchemaField::buffer_count`]), so that is an extension of
        /// the descriptor and not of a limit.
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
            /// Declared with the tag, so the generated host table takes both from
            /// one row: a plane cannot be renamed on one side only, and a new
            /// plane cannot be added without naming it here — the macro will not
            /// expand without the name.
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
    /// Int64, Date64 and Time64.
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
    /// An instant: one little-endian `i64` per row counting the field's unit
    /// since the Unix epoch, UTC. `SignalSchemaField::type_param` is the
    /// unit ([`time_unit`]).
    Timestamp = 23 => "timestamp",
    /// A span of time: one little-endian `i64` per row counting the field's
    /// unit. `SignalSchemaField::type_param` is the unit ([`time_unit`]).
    Duration = 24 => "duration",
    // Nested types and dictionary encoding do NOT belong here. See the
    // `ArrowType` doc comment.
}

impl ArrowType {
    /// Decode a physical type byte without constructing an invalid enum.
    ///
    /// Derived from [`ArrowType::ALL`] rather than restating the tag table: a
    /// second copy of the numbers is a second copy that can be wrong. Called
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

/// The unit codes of the Timestamp and Duration planes' `type_param`, in the
/// order Arrow declares its units: 0 seconds, 1 milliseconds, 2 microseconds,
/// 3 nanoseconds.
pub const TIME_UNITS: [TimeUnit; 4] = [
    TimeUnit::Second,
    TimeUnit::Millisecond,
    TimeUnit::Microsecond,
    TimeUnit::Nanosecond,
];

/// The unit a temporal plane's `type_param` names, `None` for a code past
/// [`TIME_UNITS`].
pub fn time_unit(type_param: u16) -> Option<TimeUnit> {
    TIME_UNITS.get(usize::from(type_param)).copied()
}

/// The `type_param` code of `unit`.
pub const fn time_unit_code(unit: TimeUnit) -> u16 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    }
}

/// How many of `unit` make one second.
pub fn units_per_second(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

/// The time zone every Timestamp column declares. An instant has one
/// meaning; a zone-less or otherwise-zoned timestamp is a different logical
/// type and has no plane.
pub const TIMESTAMP_ZONE: &str = "UTC";

/// Which dictionary a dictionary-encoded field indexes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DictionarySlot {
    /// The stream dictionary: one per schema, grown by the data and handed
    /// to the writer by its owner. Wire slot 1.
    Stream,
    /// A schema dictionary: fixed values supplied with the schema, by its
    /// position in the creation call's dictionary table. Wire slot
    /// `2 + position`.
    Schema(u16),
}

impl DictionarySlot {
    /// The descriptor's `dictionary` value for this slot.
    pub const fn wire(self) -> u16 {
        match self {
            Self::Stream => 1,
            Self::Schema(position) => 2 + position,
        }
    }

    /// The slot a nonzero descriptor value names. Zero is "not encoded" and
    /// has no slot; `u16::MAX` would be schema dictionary 65533, which
    /// [`MAX_SCHEMA_FIELDS`] rules out long before.
    pub const fn from_wire(value: u16) -> Option<Self> {
        match value {
            0 => None,
            1 => Some(Self::Stream),
            position => Some(Self::Schema(position - 2)),
        }
    }
}

/// The unsigned integer plane a dictionary-encoded field stores its indexes
/// in. Unsigned only: an index is a position.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexWidth {
    U8,
    U16,
    U32,
}

impl IndexWidth {
    /// The index plane, which is also the descriptor's `index_plane` byte.
    pub const fn plane(self) -> ArrowType {
        match self {
            Self::U8 => ArrowType::UInt8,
            Self::U16 => ArrowType::UInt16,
            Self::U32 => ArrowType::UInt32,
        }
    }

    pub const fn from_plane(plane: ArrowType) -> Option<Self> {
        match plane {
            ArrowType::UInt8 => Some(Self::U8),
            ArrowType::UInt16 => Some(Self::U16),
            ArrowType::UInt32 => Some(Self::U32),
            _ => None,
        }
    }

    /// How many values this width can index.
    pub const fn capacity(self) -> u64 {
        match self {
            Self::U8 => 1 << 8,
            Self::U16 => 1 << 16,
            Self::U32 => 1 << 32,
        }
    }
}

/// A field's dictionary encoding: which dictionary, and how wide its indexes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DictionaryEncoding {
    pub slot: DictionarySlot,
    pub index: IndexWidth,
}

/// Bytes of one field descriptor on the wire.
pub const FIELD_DESCRIPTOR_BYTES: usize = 8;

/// Physical field descriptor. On the wire, eight bytes:
/// `[plane u8][nullable u8][type_param u16 LE][dictionary u16 LE][index_plane u8][0]`.
///
/// `arrow_type` is the VALUE plane. `type_param` is the `FixedSizeBinary`
/// byte width, or the unit of a Timestamp or Duration plane, and zero for
/// every other plane. `dictionary` is `None` for a plain column; an encoded
/// column stores indexes of `dictionary.index` width into the dictionary of
/// `dictionary.slot`, whose values are of the value plane. The descriptor
/// alone determines the layout of every column.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SignalSchemaField {
    pub arrow_type: ArrowType,
    pub nullable: u8,
    pub type_param: u16,
    pub dictionary: Option<DictionaryEncoding>,
}

/// The base event-log schema: `id`, `type`, `timestamp`, `value`, in that
/// order. The scanners in `columine-parsing` write exactly these four columns
/// by index, so this is the only schema they can serve.
pub const BASE_EVENT_LOG_FIELDS: [SignalSchemaField; 4] = [
    SignalSchemaField::new(ArrowType::Utf8, false),
    SignalSchemaField::new(ArrowType::Utf8, false),
    SignalSchemaField::new_timestamp(TimeUnit::Nanosecond, false),
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
            dictionary: None,
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
            dictionary: None,
        }
    }

    /// A `Timestamp(unit, "UTC")` field.
    pub const fn new_timestamp(unit: TimeUnit, nullable: bool) -> Self {
        Self {
            arrow_type: ArrowType::Timestamp,
            nullable: nullable as u8,
            type_param: time_unit_code(unit),
            dictionary: None,
        }
    }

    /// A `Duration(unit)` field.
    pub const fn new_duration(unit: TimeUnit, nullable: bool) -> Self {
        Self {
            arrow_type: ArrowType::Duration,
            nullable: nullable as u8,
            type_param: time_unit_code(unit),
            dictionary: None,
        }
    }

    /// A dictionary-encoded Utf8 field: `Dictionary<index, Utf8>` over the
    /// dictionary of `slot`.
    pub const fn new_dictionary(slot: DictionarySlot, index: IndexWidth, nullable: bool) -> Self {
        Self {
            arrow_type: ArrowType::Utf8,
            nullable: nullable as u8,
            type_param: 0,
            dictionary: Some(DictionaryEncoding { slot, index }),
        }
    }

    /// The plane this field's buffers are of: the index plane of an encoded
    /// field, otherwise the value plane. This is the tag a caller-built
    /// column carries.
    pub const fn storage_type(self) -> ArrowType {
        match self.dictionary {
            Some(encoding) => encoding.index.plane(),
            None => self.arrow_type,
        }
    }

    /// The eight wire bytes of this descriptor.
    pub fn to_wire(self) -> [u8; FIELD_DESCRIPTOR_BYTES] {
        let param = self.type_param.to_le_bytes();
        let (slot, index) = match self.dictionary {
            Some(encoding) => (encoding.slot.wire(), encoding.index.plane() as u8),
            None => (0, 0),
        };
        let slot = slot.to_le_bytes();
        [
            self.arrow_type as u8,
            self.nullable,
            param[0],
            param[1],
            slot[0],
            slot[1],
            index,
            0,
        ]
    }

    /// Decode eight wire bytes; `None` when a byte names no plane, slot or
    /// index width, or a reserved byte is set. Parameter validity is
    /// [`Self::param_is_valid`]'s.
    pub fn from_wire(raw: [u8; FIELD_DESCRIPTOR_BYTES]) -> Option<Self> {
        let arrow_type = ArrowType::from_u8(raw[0])?;
        let dictionary = match (u16::from_le_bytes([raw[4], raw[5]]), raw[6]) {
            (0, 0) => None,
            (slot, index) => Some(DictionaryEncoding {
                slot: DictionarySlot::from_wire(slot)?,
                index: IndexWidth::from_plane(ArrowType::from_u8(index)?)?,
            }),
        };
        (raw[7] == 0).then_some(Self {
            arrow_type,
            nullable: raw[1],
            type_param: u16::from_le_bytes([raw[2], raw[3]]),
            dictionary,
        })
    }

    pub fn is_nullable(self) -> bool {
        self.nullable == 1
    }

    /// Physical layout of this field's own buffers: the index plane's kind
    /// for a dictionary-encoded field, the value plane's otherwise. Storage,
    /// the Arrow body and caller-built columns all follow this.
    pub fn plane_kind(self) -> PlaneKind {
        match self.dictionary {
            Some(encoding) => Self::new(encoding.index.plane(), false).value_kind(),
            None => self.value_kind(),
        }
    }

    /// Physical layout of one VALUE of this field. Total: `FixedSizeBinary`
    /// resolves its per-field byte width from `type_param`, so no caller has
    /// to know that one plane is parameterized.
    pub fn value_kind(self) -> PlaneKind {
        match self.arrow_type {
            ArrowType::Null => PlaneKind::Empty,
            ArrowType::Bool => PlaneKind::Bool,
            ArrowType::Int8 => PlaneKind::SignedInt { width: 1 },
            ArrowType::Int16 => PlaneKind::SignedInt { width: 2 },
            ArrowType::Int32 | ArrowType::IntervalYearMonth => PlaneKind::SignedInt { width: 4 },
            ArrowType::Int64 | ArrowType::Timestamp | ArrowType::Duration => {
                PlaneKind::SignedInt { width: 8 }
            }
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
        let value = self.value_data_type();
        match self.dictionary {
            Some(encoding) => DataType::Dictionary(
                Box::new(Self::new(encoding.index.plane(), false).value_data_type()),
                Box::new(value),
            ),
            None => value,
        }
    }

    fn value_data_type(self) -> DataType {
        // A unit code a valid descriptor cannot hold falls back to
        // nanoseconds; `param_is_valid` refuses it before a schema is built.
        let unit = time_unit(self.type_param).unwrap_or(TimeUnit::Nanosecond);
        match self.arrow_type {
            ArrowType::Timestamp => DataType::Timestamp(unit, Some(TIMESTAMP_ZONE.into())),
            ArrowType::Duration => DataType::Duration(unit),
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

    /// Descriptor self-consistency: `type_param` is the `FixedSizeBinary`
    /// byte width or a temporal unit and zero for every other plane, and
    /// only a Utf8 value plane is dictionary-encoded.
    fn param_is_valid(self) -> bool {
        if self.dictionary.is_some() && self.arrow_type != ArrowType::Utf8 {
            return false;
        }
        match self.arrow_type {
            ArrowType::Timestamp | ArrowType::Duration => time_unit(self.type_param).is_some(),
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
    InvalidFieldMetadata {
        field_index: usize,
    },
    FieldCountMismatch {
        schema: usize,
        metadata: usize,
    },
    TypeMismatch {
        field_index: usize,
    },
    NullabilityMismatch {
        field_index: usize,
    },
    InvalidFieldNames,
    /// The Schema message is longer than [`MAX_SCHEMA_MESSAGE_BYTES`]; it is
    /// refused before it is read, however well formed.
    MessageTooLarge,
    /// The decoded schema's storage could not be backed.
    OutOfMemory,
    /// The schema dictionary table is malformed, or holds a dictionary no
    /// field names.
    InvalidDictionaryTable,
    /// A dictionary-encoded field and the schema's dictionaries disagree: the
    /// field names a schema dictionary the table lacks, its index width
    /// cannot index every value, or its Arrow dictionary id is one another
    /// slot's field uses.
    DictionaryMismatch {
        field_index: usize,
    },
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

/// The Arrow field-metadata key under which a column names the declared
/// fields it holds. Its value is the producer's; this crate retains it per
/// field and `columine-parsing` decodes it.
pub const FIELD_IDENTITY_KEY: &str = "sb.fields";

/// The Arrow field-metadata key under which a Binary column names the
/// encoding of its cells. A column without it holds raw bytes or columine's
/// own MessagePack; a column an embedder's writer fills names that writer's
/// encoding, so a reader never guesses an encoding from bytes.
pub const CELL_ENCODING_KEY: &str = "sb.cell";

/// One dictionary of a schema: its slot, the Arrow dictionary ids its fields
/// declare, and its values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaDictionary {
    pub slot: DictionarySlot,
    /// Every Arrow dictionary id a field of this slot declares, ascending.
    /// A Schema message that shares one dictionary between fields gives one
    /// id; a writer that numbers each field itself gives one per field, and
    /// the dictionary's messages are then written once per id.
    pub arrow_ids: Vec<i64>,
    /// A schema dictionary's values by ordinal, `None` where the declaration
    /// skips an ordinal. Empty for the stream dictionary, whose values are
    /// its owner's.
    pub values: Vec<Option<String>>,
}

impl DictionaryValues for [Option<String>] {
    fn len(&self) -> u32 {
        <[Option<String>]>::len(self) as u32
    }

    fn value(&self, index: u32) -> Option<&[u8]> {
        self.get(index as usize)?.as_deref().map(str::as_bytes)
    }
}

/// The parts a schema is built from. Every constructor of
/// [`DynamicSchemaConfig`] is this value with some parts empty.
#[derive(Clone, Copy, Debug, Default)]
pub struct SchemaParts<'a> {
    /// One complete continuation-prefixed Arrow IPC Schema message.
    pub schema_message: &'a [u8],
    /// NUL-terminated field names in field order; empty for none.
    pub field_names: &'a [u8],
    /// The schema dictionary table ([`SchemaDictionaryTable`]).
    pub dictionaries: SchemaDictionaryTable<'a>,
}

/// A schema's dictionaries with fixed values, by table position.
#[derive(Clone, Copy, Debug)]
pub enum SchemaDictionaryTable<'a> {
    /// Already typed: one value list per dictionary.
    Typed(&'a [Vec<Option<String>>]),
    /// The untrusted creation-call bytes: `[count u32]`, then per dictionary
    /// `[values u32]`, then per value `[length u32][UTF-8 bytes]`,
    /// little-endian; length `0xFFFF_FFFF` is a null entry.
    Wire(&'a [u8]),
}

impl Default for SchemaDictionaryTable<'_> {
    fn default() -> Self {
        Self::Typed(&[])
    }
}

/// One declared payload member that has no column and lives in the row's
/// declared record: under which discriminant, its field-number path into the
/// record, and the position of its field descriptor in the schema its owner
/// derived the routing from. The discriminant is the value of the
/// [`PayloadDiscriminator`]'s key member.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordMember {
    pub discriminant: String,
    pub member: String,
    pub path: Vec<u16>,
    pub descriptor_position: u32,
}

/// One payload member whose explicit null is data: a dynamic value, which
/// holds a null as it holds any other value, so its cell is present where
/// every other member's null is the empty cell. `event_type` is the value of
/// the event's `type` member.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DynamicMember {
    pub event_type: String,
    pub member: String,
}
/// One text member's columns: its content hash, a `FixedSizeBinary(32)`
/// column, and the Utf8 column that holds the bytes of a text stored inline,
/// when the schema has one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TextColumn {
    pub hash: usize,
    pub inline: Option<usize>,
}
/// Owned, validated schema configuration retained by an EventProcessor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DynamicSchemaConfig {
    /// The stream preamble copied to every output: the Schema message, then
    /// one DictionaryBatch message per schema dictionary. With no schema
    /// dictionary it is the Schema message alone.
    pub schema_bytes: Vec<u8>,
    /// Length of the Schema message at the head of `schema_bytes`.
    pub schema_message_len: usize,
    pub field_metadata: Vec<SignalSchemaField>,
    /// Logical types decoded from the Schema message, in field order.
    pub logical_types: Vec<DataType>,
    /// The schema's dictionaries, ascending by slot.
    pub dictionaries: Vec<SchemaDictionary>,
    /// Each field's [`FIELD_IDENTITY_KEY`] metadata value, as written.
    pub identity_metadata: Vec<Option<String>>,
    /// Each field's [`CELL_ENCODING_KEY`] metadata value, as written.
    pub cell_metadata: Vec<Option<String>>,
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
    /// Declared members routed to the declared record; empty when every
    /// declared member has a column.
    pub record_members: Vec<RecordMember>,
    /// The schema's text members; empty when it has none.
    pub text_columns: Vec<TextColumn>,
    /// Payload members whose explicit null is data, named by the schema's
    /// owner; a semantic schema's own dynamic members need no entry.
    pub dynamic_members: Vec<DynamicMember>,
}

impl DynamicSchemaConfig {
    pub fn new(
        schema_bytes: &[u8],
        field_metadata: &[SignalSchemaField],
    ) -> Result<Self, SchemaError> {
        Self::from_parts(
            field_metadata,
            SchemaParts {
                schema_message: schema_bytes,
                ..SchemaParts::default()
            },
        )
    }

    /// Typed descriptors with any of the other parts.
    pub fn from_parts(
        field_metadata: &[SignalSchemaField],
        parts: SchemaParts<'_>,
    ) -> Result<Self, SchemaError> {
        validate_typed_metadata(field_metadata)?;
        let fields = copied(field_metadata).ok_or(SchemaError::OutOfMemory)?;
        Self::build(fields, parts)
    }

    /// The untrusted eight-byte-per-field FFI descriptor table with any of
    /// the other parts.
    pub fn from_wire_parts(
        field_metadata: &[u8],
        parts: SchemaParts<'_>,
    ) -> Result<Self, SchemaError> {
        Self::build(decode_field_metadata(field_metadata)?, parts)
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

    /// Decode the untrusted eight-byte-per-field FFI metadata table.
    pub fn from_wire(schema_bytes: &[u8], field_metadata: &[u8]) -> Result<Self, SchemaError> {
        Self::from_wire_parts(
            field_metadata,
            SchemaParts {
                schema_message: schema_bytes,
                ..SchemaParts::default()
            },
        )
    }

    pub fn with_field_names(
        schema_bytes: &[u8],
        field_metadata: &[SignalSchemaField],
        field_names_raw: &[u8],
    ) -> Result<Self, SchemaError> {
        Self::from_parts(
            field_metadata,
            SchemaParts {
                schema_message: schema_bytes,
                field_names: field_names_raw,
                ..SchemaParts::default()
            },
        )
    }

    pub fn from_wire_with_field_names(
        schema_bytes: &[u8],
        field_metadata: &[u8],
        field_names_raw: &[u8],
    ) -> Result<Self, SchemaError> {
        Self::from_wire_parts(
            field_metadata,
            SchemaParts {
                schema_message: schema_bytes,
                field_names: field_names_raw,
                ..SchemaParts::default()
            },
        )
    }

    fn build(
        field_metadata: Vec<SignalSchemaField>,
        parts: SchemaParts<'_>,
    ) -> Result<Self, SchemaError> {
        let schema_bytes = parts.schema_message;
        let field_names = parse_field_names(parts.field_names)?;
        let (schema, arrow_ids) = decode_schema_message(schema_bytes)?;
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

        let mut logical_types = Vec::new();
        logical_types
            .try_reserve_exact(field_metadata.len())
            .map_err(|_| SchemaError::OutOfMemory)?;
        let mut identity_metadata = Vec::new();
        identity_metadata
            .try_reserve_exact(field_metadata.len())
            .map_err(|_| SchemaError::OutOfMemory)?;
        let mut cell_metadata = Vec::new();
        cell_metadata
            .try_reserve_exact(field_metadata.len())
            .map_err(|_| SchemaError::OutOfMemory)?;
        let owned_value = |value: Option<&String>| -> Result<Option<String>, SchemaError> {
            let Some(value) = value else {
                return Ok(None);
            };
            let mut owned = String::new();
            owned
                .try_reserve_exact(value.len())
                .map_err(|_| SchemaError::OutOfMemory)?;
            owned.push_str(value);
            Ok(Some(owned))
        };
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
            identity_metadata.push(owned_value(field.metadata().get(FIELD_IDENTITY_KEY))?);
            cell_metadata.push(owned_value(field.metadata().get(CELL_ENCODING_KEY))?);
        }

        let table = match parts.dictionaries {
            SchemaDictionaryTable::Typed(table) => {
                let mut owned = Vec::new();
                owned
                    .try_reserve_exact(table.len())
                    .map_err(|_| SchemaError::OutOfMemory)?;
                for values in table {
                    owned.push(copied(values).ok_or(SchemaError::OutOfMemory)?);
                }
                owned
            }
            SchemaDictionaryTable::Wire(bytes) => decode_dictionary_table(bytes)?,
        };
        let dictionaries = bind_dictionaries(&field_metadata, &arrow_ids, table)?;

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
            record_members: Vec::new(),
            text_columns: Vec::new(),
            dynamic_members: Vec::new(),
            schema_bytes: preamble(schema_bytes, &dictionaries)?,
            schema_message_len: schema_bytes.len(),
            field_metadata,
            logical_types,
            dictionaries,
            identity_metadata,
            cell_metadata,
            semantic_schema: None,
            field_names,
        })
    }

    pub fn compute_buffer_count(&self) -> u32 {
        compute_buffer_count(&self.field_metadata)
    }

    /// Length of the stream preamble: the Schema message and the schema
    /// dictionaries' messages.
    pub fn schema_message_size(&self) -> usize {
        self.schema_bytes.len()
    }

    /// The Schema message alone.
    pub fn schema_message(&self) -> &[u8] {
        &self.schema_bytes[..self.schema_message_len]
    }

    /// The dictionary of `slot`, when a field of the schema names it.
    pub fn dictionary(&self, slot: DictionarySlot) -> Option<&SchemaDictionary> {
        self.dictionaries
            .binary_search_by_key(&slot.wire(), |dictionary| dictionary.slot.wire())
            .ok()
            .map(|index| &self.dictionaries[index])
    }

    pub fn write_schema_message(&self, output: &mut [u8]) -> usize {
        if output.len() < self.schema_bytes.len() {
            return 0;
        }
        output[..self.schema_bytes.len()].copy_from_slice(&self.schema_bytes);
        self.schema_bytes.len()
    }
}

/// Bind every dictionary-encoded field to its dictionary: one entry per slot
/// a field names, with the Arrow ids its fields declare and, for a schema
/// dictionary, its values from the table.
fn bind_dictionaries(
    fields: &[SignalSchemaField],
    arrow_ids: &[Option<i64>],
    mut table: Vec<Vec<Option<String>>>,
) -> Result<Vec<SchemaDictionary>, SchemaError> {
    let mut dictionaries: Vec<SchemaDictionary> = Vec::new();
    for (field_index, (field, arrow_id)) in fields.iter().zip(arrow_ids).enumerate() {
        let mismatch = SchemaError::DictionaryMismatch { field_index };
        let Some(encoding) = field.dictionary else {
            continue;
        };
        // `logical_type_matches` has already required a Dictionary type, and
        // a Dictionary-typed field always declares its encoding.
        let arrow_id = arrow_id.ok_or(mismatch.clone())?;
        if dictionaries
            .iter()
            .any(|other| other.slot != encoding.slot && other.arrow_ids.contains(&arrow_id))
        {
            return Err(mismatch);
        }
        let position = match dictionaries
            .binary_search_by_key(&encoding.slot.wire(), |dictionary| dictionary.slot.wire())
        {
            Ok(position) => position,
            Err(position) => {
                let values = match encoding.slot {
                    DictionarySlot::Stream => Vec::new(),
                    DictionarySlot::Schema(entry) => table
                        .get_mut(usize::from(entry))
                        .map(std::mem::take)
                        .ok_or(mismatch.clone())?,
                };
                dictionaries
                    .try_reserve(1)
                    .map_err(|_| SchemaError::OutOfMemory)?;
                dictionaries.insert(
                    position,
                    SchemaDictionary {
                        slot: encoding.slot,
                        arrow_ids: Vec::new(),
                        values,
                    },
                );
                position
            }
        };
        let dictionary = &mut dictionaries[position];
        if dictionary.values.len() as u64 > encoding.index.capacity() {
            return Err(mismatch);
        }
        if let Err(at) = dictionary.arrow_ids.binary_search(&arrow_id) {
            dictionary
                .arrow_ids
                .try_reserve(1)
                .map_err(|_| SchemaError::OutOfMemory)?;
            dictionary.arrow_ids.insert(at, arrow_id);
        }
    }
    // Every table entry a field named was taken above. One left with values
    // is a dictionary nothing indexes; so is a table longer than the slots.
    let named = dictionaries
        .iter()
        .filter(|dictionary| matches!(dictionary.slot, DictionarySlot::Schema(_)))
        .count();
    if named != table.len() {
        return Err(SchemaError::InvalidDictionaryTable);
    }
    Ok(dictionaries)
}

/// The stream preamble: the Schema message, then one non-delta
/// DictionaryBatch per schema dictionary and Arrow id, ascending by slot.
fn preamble(
    schema_message: &[u8],
    dictionaries: &[SchemaDictionary],
) -> Result<Vec<u8>, SchemaError> {
    let schema_dictionaries = || {
        dictionaries
            .iter()
            .filter(|dictionary| matches!(dictionary.slot, DictionarySlot::Schema(_)))
    };
    let mut length = schema_message.len();
    for dictionary in schema_dictionaries() {
        let message = dictionary_batch_len(dictionary.values.as_slice())
            .map_err(|_| SchemaError::InvalidDictionaryTable)?;
        length = message
            .checked_mul(dictionary.arrow_ids.len())
            .and_then(|bytes| bytes.checked_add(length))
            .ok_or(SchemaError::InvalidDictionaryTable)?;
    }
    let mut bytes = crate::fallible::filled(0u8, length).ok_or(SchemaError::OutOfMemory)?;
    bytes[..schema_message.len()].copy_from_slice(schema_message);
    let mut at = schema_message.len();
    for dictionary in schema_dictionaries() {
        for arrow_id in &dictionary.arrow_ids {
            at += encode_dictionary_batch(
                &mut bytes[at..],
                *arrow_id,
                false,
                dictionary.values.as_slice(),
            )
            .map_err(|_| SchemaError::InvalidDictionaryTable)?;
        }
    }
    debug_assert_eq!(at, length);
    Ok(bytes)
}

/// The largest Schema message a handle accepts, framing included.
///
/// A complete event schema whose fields each carry selector metadata outgrows
/// 64 KiB well under [`MAX_SCHEMA_FIELDS`] fields (177 such fields take
/// 103,232 bytes); this cap admits it with room to grow.
///
/// Decoding goes through the arrow-ipc reader, whose allocations are
/// infallible, so this cap bounds them before the call. For the max-field,
/// long-name fixture of `schema_decode_at_the_message_cap_stays_bounded`, the
/// decode's additional peak requested allocation stays within four times this
/// cap. That fixture bound is not a worst-case memory guarantee for every
/// schema, nor a measure of the memory a host commits.
pub const MAX_SCHEMA_MESSAGE_BYTES: usize = 256 * 1024;

/// Decode a Schema message into its Arrow schema and each field's Arrow
/// dictionary id (`None` for a field that is not dictionary-encoded).
fn decode_schema_message(
    bytes: &[u8],
) -> Result<(arrow_schema::Schema, Vec<Option<i64>>), SchemaError> {
    if bytes.len() > MAX_SCHEMA_MESSAGE_BYTES {
        return Err(SchemaError::MessageTooLarge);
    }
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
    // The ids are read from the message itself: the decoded `Schema` keeps
    // them only behind an accessor arrow-schema has deprecated.
    let mut arrow_ids = Vec::new();
    if let Some(fields) = message
        .header_as_schema()
        .and_then(|schema| schema.fields())
    {
        arrow_ids
            .try_reserve_exact(fields.len())
            .map_err(|_| SchemaError::OutOfMemory)?;
        arrow_ids.extend(
            fields
                .iter()
                .map(|field| field.dictionary().map(|encoding| encoding.id())),
        );
    }
    let schema = try_schema_from_ipc_buffer(bytes).map_err(|_| SchemaError::InvalidMessage)?;
    Ok((schema, arrow_ids))
}

/// Does `logical` name a type this plane can actually store?
///
/// Physical width agreeing is NOT the test. Every logical Arrow type belongs
/// to exactly one plane, so a plane rejects every other type of its own width:
/// UInt32 is not admitted on the four-byte signed plane and Float32 is not
/// admitted on either, because reading those bytes back through the wrong
/// plane produces a different number, not a different name.
fn logical_type_matches(field: SignalSchemaField, logical: &DataType) -> bool {
    if let Some(encoding) = field.dictionary {
        // An encoded field declares exactly its index plane and its value
        // plane; the value is judged as the plain field it would be.
        let DataType::Dictionary(index, value) = logical else {
            return false;
        };
        let plain = SignalSchemaField {
            dictionary: None,
            ..field
        };
        return **index == SignalSchemaField::new(encoding.index.plane(), false).value_data_type()
            && logical_type_matches(plain, value);
    }
    match field.arrow_type {
        // An instant is UTC and nothing else: a zone-less timestamp is wall
        // time, a different logical type no plane here stores.
        ArrowType::Timestamp => matches!(
            logical,
            DataType::Timestamp(unit, Some(zone))
                if Some(*unit) == time_unit(field.type_param) && zone.as_ref() == TIMESTAMP_ZONE
        ),
        ArrowType::Duration => matches!(
            logical,
            DataType::Duration(unit) if Some(*unit) == time_unit(field.type_param)
        ),
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
        // since the epoch and Time64 counts microseconds or nanoseconds since
        // midnight (the only two units Arrow allows for it). Timestamp and
        // Duration are not admitted: their unit is the descriptor's, on their
        // own planes.
        ArrowType::Int64 => matches!(
            logical,
            DataType::Int64
                | DataType::Date64
                | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond)
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
    let count = bytes.len() / FIELD_DESCRIPTOR_BYTES;
    if count > MAX_SCHEMA_FIELDS {
        return Err(SchemaError::TooManyFields);
    }
    if !bytes.len().is_multiple_of(FIELD_DESCRIPTOR_BYTES) {
        return Err(SchemaError::InvalidFieldMetadata { field_index: 0 });
    }
    let mut fields = Vec::new();
    fields
        .try_reserve_exact(count)
        .map_err(|_| SchemaError::OutOfMemory)?;
    for (field_index, raw) in bytes
        .as_chunks::<FIELD_DESCRIPTOR_BYTES>()
        .0
        .iter()
        .enumerate()
    {
        let field = SignalSchemaField::from_wire(*raw)
            .filter(|field| field.nullable <= 1 && field.param_is_valid())
            .ok_or(SchemaError::InvalidFieldMetadata { field_index })?;
        fields.push(field);
    }
    Ok(fields)
}

/// Decode the creation call's schema dictionary table:
/// `[count u32]` then per dictionary `[values u32]` then per value
/// `[length u32][UTF-8 bytes]`, all little-endian, where length
/// `0xFFFF_FFFF` is a null entry (an ordinal the declaration skips). An empty
/// table is no dictionaries.
fn decode_dictionary_table(bytes: &[u8]) -> Result<Vec<Vec<Option<String>>>, SchemaError> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    if bytes.len() > MAX_SCHEMA_MESSAGE_BYTES {
        return Err(SchemaError::InvalidDictionaryTable);
    }
    let mut at = 0usize;
    let word = |at: &mut usize| -> Result<u32, SchemaError> {
        let raw = bytes
            .get(
                *at..at
                    .checked_add(4)
                    .ok_or(SchemaError::InvalidDictionaryTable)?,
            )
            .ok_or(SchemaError::InvalidDictionaryTable)?;
        *at += 4;
        Ok(u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]))
    };
    let count = word(&mut at)? as usize;
    // Every dictionary and every value costs at least four table bytes, so a
    // count past the table's length is refused before anything is reserved.
    if count > bytes.len() / 4 || count > MAX_SCHEMA_FIELDS {
        return Err(SchemaError::InvalidDictionaryTable);
    }
    let mut dictionaries = Vec::new();
    dictionaries
        .try_reserve_exact(count)
        .map_err(|_| SchemaError::OutOfMemory)?;
    for _ in 0..count {
        let value_count = word(&mut at)? as usize;
        if value_count > bytes.len() / 4 {
            return Err(SchemaError::InvalidDictionaryTable);
        }
        let mut values = Vec::new();
        values
            .try_reserve_exact(value_count)
            .map_err(|_| SchemaError::OutOfMemory)?;
        for _ in 0..value_count {
            let length = word(&mut at)?;
            if length == u32::MAX {
                values.push(None);
                continue;
            }
            let text = bytes
                .get(
                    at..at
                        .checked_add(length as usize)
                        .ok_or(SchemaError::InvalidDictionaryTable)?,
                )
                .and_then(|raw| std::str::from_utf8(raw).ok())
                .ok_or(SchemaError::InvalidDictionaryTable)?;
            at += length as usize;
            let mut owned = String::new();
            owned
                .try_reserve_exact(text.len())
                .map_err(|_| SchemaError::OutOfMemory)?;
            owned.push_str(text);
            values.push(Some(owned));
        }
        dictionaries.push(values);
    }
    if at != bytes.len() {
        return Err(SchemaError::InvalidDictionaryTable);
    }
    Ok(dictionaries)
}

fn parse_field_names(raw: &[u8]) -> Result<Vec<String>, SchemaError> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    if raw.last() != Some(&0) {
        return Err(SchemaError::InvalidFieldNames);
    }
    let raw = &raw[..raw.len() - 1];
    // One name per terminator, counted before any is copied: a blob naming
    // more fields than a schema may hold is refused without allocating.
    let count = raw.iter().filter(|byte| **byte == 0).count() + 1;
    if count > MAX_SCHEMA_FIELDS {
        return Err(SchemaError::InvalidFieldNames);
    }
    let mut names = Vec::new();
    names
        .try_reserve_exact(count)
        .map_err(|_| SchemaError::OutOfMemory)?;
    for name in raw.split(|byte| *byte == 0) {
        let name = std::str::from_utf8(name)
            .ok()
            .filter(|name| !name.is_empty())
            .ok_or(SchemaError::InvalidFieldNames)?;
        let mut owned = String::new();
        owned
            .try_reserve_exact(name.len())
            .map_err(|_| SchemaError::OutOfMemory)?;
        owned.push_str(name);
        names.push(owned);
    }
    Ok(names)
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
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("value", DataType::Binary, true),
        ])
    }

    fn base_fields() -> [SignalSchemaField; 4] {
        [
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new_timestamp(TimeUnit::Nanosecond, false),
            SignalSchemaField::new(ArrowType::Binary, true),
        ]
    }

    #[test]
    fn field_metadata_layout_and_discriminants_are_stable() {
        // The descriptor is eight bytes on the wire. Nothing casts a byte
        // slice to this struct: `to_wire` and `from_wire` write and read the
        // eight bytes explicitly, so the struct's own layout is not the ABI.
        assert_eq!(FIELD_DESCRIPTOR_BYTES, 8);

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

        // Every plane survives the wire: the descriptor a field serializes to
        // is the field it is read back as.
        for plane in ArrowType::ALL {
            let field = match plane {
                ArrowType::FixedSizeBinary => SignalSchemaField::new_fixed_size_binary(4, true),
                ArrowType::Timestamp => {
                    SignalSchemaField::new_timestamp(TimeUnit::Microsecond, true)
                }
                ArrowType::Duration => SignalSchemaField::new_duration(TimeUnit::Second, false),
                other => SignalSchemaField::new(*other, true),
            };
            let wire = field.to_wire();
            assert_eq!(wire[0], *plane as u8);
            assert_eq!(SignalSchemaField::from_wire(wire), Some(field), "{plane:?}");
        }
        for slot in [
            DictionarySlot::Stream,
            DictionarySlot::Schema(0),
            DictionarySlot::Schema(7),
        ] {
            for index in [IndexWidth::U8, IndexWidth::U16, IndexWidth::U32] {
                let field = SignalSchemaField::new_dictionary(slot, index, true);
                assert_eq!(SignalSchemaField::from_wire(field.to_wire()), Some(field));
            }
        }

        // The layout is `[plane, nullable, type_param lo, hi, dictionary lo,
        // hi, index_plane, 0]`.
        assert_eq!(
            SignalSchemaField::new_timestamp(TimeUnit::Microsecond, false).to_wire(),
            [23, 0, 2, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            SignalSchemaField::new_dictionary(DictionarySlot::Schema(1), IndexWidth::U16, true)
                .to_wire(),
            [4, 1, 0, 0, 3, 0, 10, 0]
        );

        // A byte that names no plane, slot or index plane, an index plane that
        // is not unsigned, and a set reserved byte name no descriptor.
        for raw in [
            [25, 1, 0, 0, 0, 0, 0, 0],
            [4, 1, 0, 0, 1, 0, 0, 0],
            [4, 1, 0, 0, 0, 0, 9, 0],
            [4, 1, 0, 0, 1, 0, 6, 0],
            [4, 1, 0, 0, 0, 0, 0, 1],
        ] {
            assert_eq!(SignalSchemaField::from_wire(raw), None, "{raw:?}");
        }
    }

    #[test]
    fn real_schema_message_is_decoded_and_retained() {
        let bytes = schema_message(&base_schema());
        let config = DynamicSchemaConfig::new(&bytes, &base_fields()).unwrap();
        assert_eq!(config.schema_bytes, bytes);
        assert_eq!(
            config.logical_types[2],
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
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
            4, 0, 0, 0, 0, 0, 0, 0, // Utf8
            4, 0, 0, 0, 0, 0, 0, 0, // Utf8
            23, 0, 3, 0, 0, 0, 0, 0, // Timestamp, nanoseconds
            3, 1, 0, 0, 0, 0, 0, 0, // nullable Binary
        ];
        let config = DynamicSchemaConfig::from_wire(&bytes, &raw).unwrap();
        assert_eq!(config.field_metadata, base_fields());

        let mut invalid = raw;
        invalid[16] = 255;
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
        // The last byte of a row is reserved and must be zero.
        let mut invalid = raw;
        invalid[31] = 1;
        assert_eq!(
            DynamicSchemaConfig::from_wire(&bytes, &invalid),
            Err(SchemaError::InvalidFieldMetadata { field_index: 3 })
        );
        // A table that is not a whole number of eight-byte rows is refused.
        assert_eq!(
            DynamicSchemaConfig::from_wire(&bytes, &raw[..31]),
            Err(SchemaError::InvalidFieldMetadata { field_index: 0 })
        );
        // Four bytes per field is not a descriptor table.
        let four_byte_rows = [4, 0, 0, 0, 4, 0, 0, 0, 6, 0, 0, 0, 3, 1, 0, 0];
        assert_eq!(
            DynamicSchemaConfig::from_wire(&bytes, &four_byte_rows),
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
