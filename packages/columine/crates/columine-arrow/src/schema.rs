//! Dynamic Arrow schema storage and field metadata.
//!
//! Ports `arrow/dynamic_schema.zig` (columine copy is the base; the
//! axe-runtime copy only deletes `has_extraction_fields`). TypeScript
//! generates the schema FlatBuffer bytes with Flechette; this side stores
//! them verbatim and derives buffer counts from the 4-byte-per-field
//! metadata table.

/// Arrow type identifiers matching the TypeScript `ArrowType` enum in
/// `ArrowSchemaDescriptor.ts`. The discriminants are FFI contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ArrowType {
    Null = 0,
    /// 32-bit signed integer (enum ordinals, S.i32(), S.u32())
    Int32 = 1,
    /// 64-bit IEEE 754 float (S.number(), S.f64())
    Float64 = 2,
    Binary = 3,
    Utf8 = 4,
    Bool = 5,
    /// 64-bit signed integer (S.bigint(), S.i64(), S.timestamp(), timestamps)
    Int64 = 6,
}

impl ArrowType {
    /// Decode the FFI byte (`dynamic_schema.zig` trusts the TS side; a bad
    /// byte is a boundary error, not an invariant).
    pub fn from_u8(value: u8) -> Option<Self> {
        Some(match value {
            0 => Self::Null,
            1 => Self::Int32,
            2 => Self::Float64,
            3 => Self::Binary,
            4 => Self::Utf8,
            5 => Self::Bool,
            6 => Self::Int64,
            _ => return None,
        })
    }
}

/// Field metadata passed from TypeScript for buffer computation.
///
/// 4 bytes, alignment 1 — the layout matches `generateFieldMetadata()` in
/// `generate-dynamic-schema.ts` (byte 0: ArrowType, byte 1: nullable,
/// bytes 2-3: padding). Pinned by `signal_schema_field_layout`.
// Plain repr(C) already yields size 4 / align 1: every field is align-1
// (ArrowType is repr(u8)), matching Zig's extern struct exactly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct SignalSchemaField {
    /// Arrow type for this field.
    pub arrow_type: ArrowType,
    /// Whether nullable (1 = true, 0 = false).
    pub nullable: u8,
    /// Padding for 4-byte stride (dynamic_schema.zig `_pad`).
    pub _pad: [u8; 2],
}

impl SignalSchemaField {
    pub fn new(arrow_type: ArrowType, nullable: bool) -> Self {
        Self {
            arrow_type,
            nullable: u8::from(nullable),
            _pad: [0; 2],
        }
    }

    /// True if this field allows null values.
    pub fn is_nullable(self) -> bool {
        self.nullable != 0
    }

    /// Number of buffers this field contributes to a RecordBatch.
    /// Flechette always includes validity buffers, even for non-nullable
    /// columns (dynamic_schema.zig `bufferCount`).
    pub fn buffer_count(self) -> u32 {
        // Validity buffer (always present per Flechette convention)
        1 + match self.arrow_type {
            // Variable-length: offsets buffer + data buffer
            ArrowType::Utf8 | ArrowType::Binary => 2,
            // Fixed-length: just data buffer
            ArrowType::Int32 | ArrowType::Int64 | ArrowType::Float64 | ArrowType::Bool => 1,
            // Null type has no data buffer
            ArrowType::Null => 0,
        }
    }
}

/// Configuration for dynamic schema encoding: pre-computed schema bytes from
/// TypeScript plus field metadata (`dynamic_schema.zig DynamicSchemaConfig`).
/// Rust ownership replaces the Zig allocator plumbing; `deinit` is `Drop`.
#[derive(Clone, Debug)]
pub struct DynamicSchemaConfig {
    /// Complete schema message bytes incl. continuation marker and size.
    pub schema_bytes: Vec<u8>,
    /// Field metadata for buffer computation.
    pub field_metadata: Vec<SignalSchemaField>,
    /// True if the schema has value.* extraction fields. Zig derives this as
    /// `field_count != 4` (base schema is exactly id/type/timestamp/value);
    /// the columine EP selects the base vs extraction path with it.
    pub has_extraction_fields: bool,
    /// Field names for JSON key matching (parsed from the null-terminated
    /// blob TS passes; empty for the no-names init path).
    pub field_names: Vec<String>,
}

impl DynamicSchemaConfig {
    /// Init without field names (`DynamicSchemaConfig.init`). This path
    /// exists for checkpoint/restore and export compatibility; extraction
    /// needs names.
    pub fn new(schema_bytes: &[u8], field_metadata: &[SignalSchemaField]) -> Self {
        Self {
            schema_bytes: schema_bytes.to_vec(),
            field_metadata: field_metadata.to_vec(),
            has_extraction_fields: field_metadata.len() != 4,
            field_names: Vec::new(),
        }
    }

    /// Init with field names (`DynamicSchemaConfig.initWithFieldNames`).
    /// `field_names_raw` is the concatenated null-terminated blob, e.g.
    /// `"id\0type\0timestamp\0value.orderId\0"`.
    pub fn with_field_names(
        schema_bytes: &[u8],
        field_metadata: &[SignalSchemaField],
        field_names_raw: &[u8],
    ) -> Self {
        let mut config = Self::new(schema_bytes, field_metadata);
        config.field_names = parse_field_names(field_names_raw);
        config
    }

    /// Total buffer count for this schema (`computeBufferCount`).
    pub fn compute_buffer_count(&self) -> u32 {
        self.field_metadata.iter().map(|f| f.buffer_count()).sum()
    }

    /// Schema message size for IPC output sizing.
    pub fn schema_message_size(&self) -> usize {
        self.schema_bytes.len()
    }

    /// Write the schema message; returns bytes written, 0 if the buffer is
    /// too small (Zig's sentinel contract, kept for the writer's use).
    pub fn write_schema_message(&self, output: &mut [u8]) -> usize {
        if output.len() < self.schema_bytes.len() {
            return 0;
        }
        output[..self.schema_bytes.len()].copy_from_slice(&self.schema_bytes);
        self.schema_bytes.len()
    }
}

/// Parse names from the null-terminated concatenated blob
/// (`dynamic_schema.zig parseFieldNames`): empty segments (consecutive
/// terminators) are skipped, a trailing unterminated segment is dropped.
fn parse_field_names(raw: &[u8]) -> Vec<String> {
    let mut names = Vec::new();
    let mut start = 0usize;
    for (i, byte) in raw.iter().enumerate() {
        if *byte == 0 {
            if i > start {
                names.push(String::from_utf8_lossy(&raw[start..i]).into_owned());
            }
            start = i + 1;
        }
    }
    names
}

#[cfg(test)]
mod tests {
    use super::*;

    // test "SignalSchemaField size and alignment" (dynamic_schema.zig)
    #[test]
    fn signal_schema_field_layout() {
        assert_eq!(core::mem::size_of::<SignalSchemaField>(), 4);
        assert_eq!(core::mem::align_of::<SignalSchemaField>(), 1);
        assert_eq!(core::mem::offset_of!(SignalSchemaField, arrow_type), 0);
        assert_eq!(core::mem::offset_of!(SignalSchemaField, nullable), 1);
    }

    #[test]
    fn arrow_type_discriminants_match_ts_enum() {
        // ArrowSchemaDescriptor.ts values (dynamic_schema.zig:18-29).
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
        assert_eq!(ArrowType::from_u8(7), None);
    }

    // test "SignalSchemaField isNullable"
    #[test]
    fn is_nullable() {
        assert!(SignalSchemaField::new(ArrowType::Utf8, true).is_nullable());
        assert!(!SignalSchemaField::new(ArrowType::Utf8, false).is_nullable());
    }

    // test "SignalSchemaField bufferCount"
    #[test]
    fn buffer_count_per_type() {
        assert_eq!(
            SignalSchemaField::new(ArrowType::Utf8, false).buffer_count(),
            3
        );
        assert_eq!(
            SignalSchemaField::new(ArrowType::Binary, true).buffer_count(),
            3
        );
        assert_eq!(
            SignalSchemaField::new(ArrowType::Int32, false).buffer_count(),
            2
        );
        assert_eq!(
            SignalSchemaField::new(ArrowType::Float64, true).buffer_count(),
            2
        );
        assert_eq!(
            SignalSchemaField::new(ArrowType::Bool, false).buffer_count(),
            2
        );
        assert_eq!(
            SignalSchemaField::new(ArrowType::Null, true).buffer_count(),
            1
        );
    }

    fn base_fields() -> [SignalSchemaField; 4] {
        [
            SignalSchemaField::new(ArrowType::Utf8, false),  // id
            SignalSchemaField::new(ArrowType::Utf8, false),  // type
            SignalSchemaField::new(ArrowType::Int64, false), // timestamp
            SignalSchemaField::new(ArrowType::Binary, true), // value
        ]
    }

    // test "DynamicSchemaConfig init and deinit"
    #[test]
    fn config_init() {
        let schema_bytes = [0xFF, 0xFF, 0xFF, 0xFF, 0x10, 0x00, 0x00, 0x00];
        let config = DynamicSchemaConfig::new(&schema_bytes, &base_fields());
        assert_eq!(config.field_metadata.len(), 4);
        assert_eq!(config.schema_bytes.len(), 8);
        // Base 4-column schema is NOT an extraction schema (field_count == 4).
        assert!(!config.has_extraction_fields);
    }

    // test "DynamicSchemaConfig computeBufferCount for 4-field schema"
    #[test]
    fn buffer_count_4_field() {
        let schema_bytes = [0xFF, 0xFF, 0xFF, 0xFF, 0x10, 0x00, 0x00, 0x00];
        let config = DynamicSchemaConfig::new(&schema_bytes, &base_fields());
        // 3 + 3 + 2 + 3 = 11 (matches Phase 7's hardcoded count)
        assert_eq!(config.compute_buffer_count(), 11);
    }

    // test "DynamicSchemaConfig computeBufferCount for 5-field schema"
    #[test]
    fn buffer_count_5_field() {
        let schema_bytes = [0xFF, 0xFF, 0xFF, 0xFF, 0x10, 0x00, 0x00, 0x00];
        let fields = [
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new(ArrowType::Int64, false),
            SignalSchemaField::new(ArrowType::Utf8, true),
            SignalSchemaField::new(ArrowType::Float64, true),
        ];
        let config = DynamicSchemaConfig::new(&schema_bytes, &fields);
        assert_eq!(config.compute_buffer_count(), 13);
        // 5 fields => extraction schema per the field_count != 4 rule.
        assert!(config.has_extraction_fields);
    }

    // test "DynamicSchemaConfig writeSchemaMessage"
    #[test]
    fn write_schema_message() {
        let schema_bytes = [0xFF, 0xFF, 0xFF, 0xFF, 0x10, 0x00, 0x00, 0x00];
        let config = DynamicSchemaConfig::new(&schema_bytes, &[]);
        let mut output = [0u8; 16];
        assert_eq!(config.write_schema_message(&mut output), 8);
        assert_eq!(&output[..8], &schema_bytes);
        let mut small = [0u8; 4];
        assert_eq!(config.write_schema_message(&mut small), 0);
    }

    #[test]
    fn field_name_parsing() {
        // dynamic_schema.zig parseFieldNames: null-terminated segments,
        // empty segments skipped, unterminated tail dropped.
        let config = DynamicSchemaConfig::with_field_names(
            &[],
            &[],
            b"id\0type\0\0timestamp\0value.orderId\0tail-no-term",
        );
        assert_eq!(
            config.field_names,
            ["id", "type", "timestamp", "value.orderId"]
        );
    }
}
