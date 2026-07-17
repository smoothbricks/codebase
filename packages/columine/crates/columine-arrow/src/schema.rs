//! Validated dynamic Arrow schema storage and field metadata.
//!
//! The schema is supplied as one complete Arrow IPC Schema message. It is
//! decoded once during processor creation so the retained four-byte physical
//! metadata cannot disagree with the logical Arrow schema copied to output.

use arrow_ipc::{MessageHeader, convert::try_schema_from_ipc_buffer, root_as_message};
use arrow_schema::DataType;

/// Maximum supported fields in one flattened schema.
pub const MAX_SCHEMA_FIELDS: usize = 256;

/// Arrow type identifiers matching the TypeScript `ArrowType` enum.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ArrowType {
    Null = 0,
    /// One 32-bit value per row. The logical schema may be Int32 or UInt32.
    Int32 = 1,
    Float64 = 2,
    Binary = 3,
    Utf8 = 4,
    Bool = 5,
    /// One signed 64-bit value per row; Timestamp uses this physical layout.
    Int64 = 6,
}

impl ArrowType {
    /// Decode a physical type byte without constructing an invalid enum.
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

/// Four-byte physical field descriptor: `[type, nullable, 0, 0]`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct SignalSchemaField {
    pub arrow_type: ArrowType,
    pub nullable: u8,
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

    pub fn is_nullable(self) -> bool {
        self.nullable == 1
    }

    pub fn buffer_count(self) -> u32 {
        match self.arrow_type {
            ArrowType::Null => 0,
            ArrowType::Utf8 | ArrowType::Binary => 3,
            ArrowType::Int32 | ArrowType::Int64 | ArrowType::Float64 | ArrowType::Bool => 2,
        }
    }
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

/// Owned, validated schema configuration retained by an EventProcessor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DynamicSchemaConfig {
    /// One complete continuation-prefixed Arrow IPC Schema message.
    pub schema_bytes: Vec<u8>,
    pub field_metadata: Vec<SignalSchemaField>,
    /// Logical types decoded from `schema_bytes`, in field order.
    pub logical_types: Vec<DataType>,
    pub has_extraction_fields: bool,
    pub field_names: Vec<String>,
}

impl DynamicSchemaConfig {
    pub fn new(
        schema_bytes: &[u8],
        field_metadata: &[SignalSchemaField],
    ) -> Result<Self, SchemaError> {
        validate_typed_metadata(field_metadata)?;
        Self::build(schema_bytes, field_metadata.to_vec(), Vec::new())
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
            if !logical_type_matches(metadata.arrow_type, field.data_type()) {
                return Err(SchemaError::TypeMismatch { field_index });
            }
            if field.is_nullable() != metadata.is_nullable()
                || (metadata.arrow_type == ArrowType::Null && !metadata.is_nullable())
            {
                return Err(SchemaError::NullabilityMismatch { field_index });
            }
            logical_types.push(field.data_type().clone());
        }

        Ok(Self {
            has_extraction_fields: field_metadata.len() != 4,
            schema_bytes: schema_bytes.to_vec(),
            field_metadata,
            logical_types,
            field_names,
        })
    }

    pub fn compute_buffer_count(&self) -> u32 {
        self.field_metadata
            .iter()
            .map(|field| field.buffer_count())
            .sum()
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

fn logical_type_matches(physical: ArrowType, logical: &DataType) -> bool {
    match physical {
        ArrowType::Null => matches!(logical, DataType::Null),
        ArrowType::Int32 => matches!(logical, DataType::Int32 | DataType::UInt32),
        ArrowType::Float64 => matches!(logical, DataType::Float64),
        ArrowType::Binary => matches!(logical, DataType::Binary),
        ArrowType::Utf8 => matches!(logical, DataType::Utf8),
        ArrowType::Bool => matches!(logical, DataType::Boolean),
        ArrowType::Int64 => matches!(logical, DataType::Int64 | DataType::Timestamp(_, _)),
    }
}

fn validate_typed_metadata(fields: &[SignalSchemaField]) -> Result<(), SchemaError> {
    if fields.len() > MAX_SCHEMA_FIELDS {
        return Err(SchemaError::TooManyFields);
    }
    for (field_index, field) in fields.iter().enumerate() {
        if field.nullable > 1 || field._pad != [0; 2] {
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
    for (field_index, raw) in bytes.chunks_exact(4).enumerate() {
        let Some(arrow_type) = ArrowType::from_u8(raw[0]) else {
            return Err(SchemaError::InvalidFieldMetadata { field_index });
        };
        if raw[1] > 1 || raw[2] != 0 || raw[3] != 0 {
            return Err(SchemaError::InvalidFieldMetadata { field_index });
        }
        fields.push(SignalSchemaField::new(arrow_type, raw[1] == 1));
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
        assert_eq!(core::mem::size_of::<SignalSchemaField>(), 4);
        assert_eq!(core::mem::align_of::<SignalSchemaField>(), 1);
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

    #[test]
    fn real_schema_message_is_decoded_and_retained() {
        let bytes = schema_message(&base_schema());
        let config = DynamicSchemaConfig::new(&bytes, &base_fields()).unwrap();
        assert_eq!(config.schema_bytes, bytes);
        assert_eq!(config.logical_types[2], DataType::Int64);
        assert_eq!(config.compute_buffer_count(), 11);
        assert!(!config.has_extraction_fields);

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
