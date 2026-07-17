//! Arrow IPC stream writer.
//!
//! All producer paths converge on [`write_arrow_ipc_from_borrowed_columns`]:
//! schema bytes are copied once, each borrowed Arrow buffer is copied once
//! into its final aligned body position, then the RecordBatch metadata and
//! EOS marker are emitted around that body.

use crate::columns::{DynamicColumns, EventColumns};
use crate::record_batch::{
    DynamicBodyBuilder, DynamicColumn, MetadataStorage, compute_buffer_count,
    encode_record_batch_dynamic, record_batch_metadata_size,
};
use crate::schema::{ArrowType, DynamicSchemaConfig};

pub const EOS_MARKER: [u8; 8] = [0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00];
pub const MIN_ARROW_OUTPUT_CAPACITY: usize = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IpcError {
    BufferTooSmall { required: usize },
    InvalidColumn,
    SizeOverflow,
}

fn checked_align_to_8(size: usize) -> Result<usize, IpcError> {
    size.checked_add(7)
        .map(|value| value & !7usize)
        .ok_or(IpcError::SizeOverflow)
}

fn checked_add_aligned(total: &mut usize, length: usize) -> Result<(), IpcError> {
    *total = total
        .checked_add(checked_align_to_8(length)?)
        .ok_or(IpcError::SizeOverflow)?;
    Ok(())
}

fn validate_column_shape(
    index: usize,
    expected: ArrowType,
    nullable: bool,
    column: DynamicColumn<'_>,
) -> Result<usize, IpcError> {
    if column.field_idx as usize != index
        || column.arrow_type != expected
        || column.nullable != nullable
        || (!nullable && column.validity.is_some())
    {
        return Err(IpcError::InvalidColumn);
    }

    if expected == ArrowType::Null {
        if column.validity.is_some() || column.offsets.is_some() || !column.data.is_empty() {
            return Err(IpcError::InvalidColumn);
        }
        return Ok(0);
    }

    let mut body_length = 0usize;
    checked_add_aligned(&mut body_length, column.validity.map_or(0, <[u8]>::len))?;
    match expected {
        ArrowType::Utf8 | ArrowType::Binary => {
            let offsets = column.offsets.ok_or(IpcError::InvalidColumn)?;
            checked_add_aligned(&mut body_length, offsets.len())?;
            checked_add_aligned(&mut body_length, column.data.len())?;
        }
        ArrowType::Int32 | ArrowType::Int64 | ArrowType::Float64 | ArrowType::Bool => {
            if column.offsets.is_some() {
                return Err(IpcError::InvalidColumn);
            }
            checked_add_aligned(&mut body_length, column.data.len())?;
        }
        ArrowType::Null => return Err(IpcError::InvalidColumn),
    }
    Ok(body_length)
}

/// Exact Arrow IPC byte length for a validated set of borrowed columns.
pub fn required_arrow_ipc_len<'a, F>(
    schema_config: &DynamicSchemaConfig,
    mut column_at: F,
) -> Result<usize, IpcError>
where
    F: FnMut(usize) -> Result<DynamicColumn<'a>, IpcError>,
{
    let mut body_length = 0usize;
    for (index, field) in schema_config.field_metadata.iter().enumerate() {
        let column = column_at(index)?;
        body_length = body_length
            .checked_add(validate_column_shape(
                index,
                field.arrow_type,
                field.is_nullable(),
                column,
            )?)
            .ok_or(IpcError::SizeOverflow)?;
    }
    let field_count =
        u32::try_from(schema_config.field_metadata.len()).map_err(|_| IpcError::SizeOverflow)?;
    let metadata_size = record_batch_metadata_size(
        field_count,
        compute_buffer_count(&schema_config.field_metadata),
    );
    schema_config
        .schema_message_size()
        .checked_add(8)
        .and_then(|size| size.checked_add(metadata_size))
        .and_then(|size| size.checked_add(body_length))
        .and_then(|size| size.checked_add(EOS_MARKER.len()))
        .ok_or(IpcError::SizeOverflow)
}

#[derive(Clone, Copy)]
struct BuiltBody {
    start: usize,
    length: usize,
    node_count: usize,
    desc_count: usize,
}

fn finish_stream(
    output: &mut [u8],
    write_offset: usize,
    row_count: i64,
    metadata: &MetadataStorage,
    body: BuiltBody,
    required: usize,
) -> Result<usize, IpcError> {
    let rb_written = encode_record_batch_dynamic(
        &mut output[write_offset..],
        row_count,
        Some(body.start - write_offset),
        &metadata.field_nodes[..body.node_count],
        &metadata.buffer_descs[..body.desc_count],
        &[],
        body.length,
    );
    if rb_written == 0 {
        return Err(IpcError::BufferTooSmall { required });
    }
    let mut end = write_offset
        .checked_add(rb_written)
        .ok_or(IpcError::SizeOverflow)?;
    let eos_end = end
        .checked_add(EOS_MARKER.len())
        .ok_or(IpcError::SizeOverflow)?;
    if eos_end > output.len() {
        return Err(IpcError::BufferTooSmall { required });
    }
    output[end..eos_end].copy_from_slice(&EOS_MARKER);
    end = eos_end;
    Ok(end)
}

/// Production writer over borrowed, already validated Arrow column buffers.
///
/// `column_at` is called once during the no-mutation size preflight and once
/// while building the body. `null_count_at` is called only during the build.
pub fn write_arrow_ipc_from_borrowed_columns<'a, F, N>(
    row_count: u32,
    schema_config: &DynamicSchemaConfig,
    output: &mut [u8],
    metadata_storage: &mut MetadataStorage,
    mut column_at: F,
    mut null_count_at: N,
) -> Result<usize, IpcError>
where
    F: FnMut(usize) -> Result<DynamicColumn<'a>, IpcError>,
    N: FnMut(usize) -> i64,
{
    let required = required_arrow_ipc_len(schema_config, &mut column_at)?;
    let required_capacity = required.max(MIN_ARROW_OUTPUT_CAPACITY);
    if output.len() < required_capacity {
        return Err(IpcError::BufferTooSmall {
            required: required_capacity,
        });
    }

    let write_offset = schema_config.write_schema_message(output);
    if write_offset != schema_config.schema_message_size() {
        return Err(IpcError::BufferTooSmall {
            required: required_capacity,
        });
    }

    let field_count =
        u32::try_from(schema_config.field_metadata.len()).map_err(|_| IpcError::SizeOverflow)?;
    let record_batch_prefix = 8usize
        .checked_add(record_batch_metadata_size(
            field_count,
            compute_buffer_count(&schema_config.field_metadata),
        ))
        .ok_or(IpcError::SizeOverflow)?;
    let body_start = write_offset
        .checked_add(record_batch_prefix)
        .ok_or(IpcError::SizeOverflow)?;

    let body = {
        let body_region = output
            .get_mut(body_start..)
            .ok_or(IpcError::BufferTooSmall {
                required: required_capacity,
            })?;
        let mut builder = DynamicBodyBuilder::new(body_region, metadata_storage);
        for index in 0..schema_config.field_metadata.len() {
            let column = column_at(index)?;
            if !builder.add_column(column, i64::from(row_count), null_count_at(index)) {
                return Err(IpcError::BufferTooSmall {
                    required: required_capacity,
                });
            }
        }
        BuiltBody {
            start: body_start,
            length: builder.body_length(),
            node_count: builder.field_nodes().len(),
            desc_count: builder.buffer_descs().len(),
        }
    };

    finish_stream(
        output,
        write_offset,
        i64::from(row_count),
        metadata_storage,
        body,
        required_capacity,
    )
}

fn dynamic_column<'a>(
    columns: &'a DynamicColumns,
    schema: &DynamicSchemaConfig,
    index: usize,
) -> Result<DynamicColumn<'a>, IpcError> {
    let field = schema
        .field_metadata
        .get(index)
        .ok_or(IpcError::InvalidColumn)?;
    let column = columns.columns.get(index).ok_or(IpcError::InvalidColumn)?;
    let field_index = u32::try_from(index).map_err(|_| IpcError::InvalidColumn)?;
    let validity = field
        .is_nullable()
        .then(|| column.validity_bytes(columns.count));
    match field.arrow_type {
        ArrowType::Utf8 => Ok(DynamicColumn::utf8(
            field_index,
            field.is_nullable(),
            validity,
            column
                .offsets_bytes(columns.count)
                .ok_or(IpcError::InvalidColumn)?,
            column.data_bytes().ok_or(IpcError::InvalidColumn)?,
        )),
        ArrowType::Binary => Ok(DynamicColumn::binary(
            field_index,
            field.is_nullable(),
            validity,
            column
                .offsets_bytes(columns.count)
                .ok_or(IpcError::InvalidColumn)?,
            column.data_bytes().ok_or(IpcError::InvalidColumn)?,
        )),
        ArrowType::Int32 => Ok(DynamicColumn::int32(
            field_index,
            field.is_nullable(),
            validity,
            column
                .fixed_i32_bytes(columns.count)
                .ok_or(IpcError::InvalidColumn)?,
        )),
        ArrowType::Int64 => Ok(DynamicColumn::int64(
            field_index,
            field.is_nullable(),
            validity,
            column
                .fixed_i64_bytes(columns.count)
                .ok_or(IpcError::InvalidColumn)?,
        )),
        ArrowType::Float64 => Ok(DynamicColumn::float64(
            field_index,
            field.is_nullable(),
            validity,
            column
                .fixed_f64_bytes(columns.count)
                .ok_or(IpcError::InvalidColumn)?,
        )),
        ArrowType::Bool => Ok(DynamicColumn::boolean(
            field_index,
            field.is_nullable(),
            validity,
            column
                .bool_bytes(columns.count)
                .ok_or(IpcError::InvalidColumn)?,
        )),
        ArrowType::Null => Ok(DynamicColumn {
            field_idx: field_index,
            arrow_type: ArrowType::Null,
            nullable: true,
            validity: None,
            data: &[],
            offsets: None,
        }),
    }
}

fn dynamic_null_count(columns: &DynamicColumns, index: usize) -> i64 {
    let Ok(field_index) = u32::try_from(index) else {
        return 0;
    };
    (0..columns.count)
        .filter(|row| columns.is_null(field_index, *row))
        .count() as i64
}

pub fn write_arrow_ipc_from_dynamic_columns(
    columns: &DynamicColumns,
    schema_config: &DynamicSchemaConfig,
    output: &mut [u8],
    metadata_storage: &mut MetadataStorage,
) -> Result<usize, IpcError> {
    write_arrow_ipc_from_borrowed_columns(
        columns.count,
        schema_config,
        output,
        metadata_storage,
        |index| dynamic_column(columns, schema_config, index),
        |index| dynamic_null_count(columns, index),
    )
}

pub fn write_arrow_ipc_from_columns_with_schema(
    columns: &EventColumns,
    schema_config: &DynamicSchemaConfig,
    output: &mut [u8],
    metadata_storage: &mut MetadataStorage,
) -> Result<usize, IpcError> {
    if schema_config.field_metadata.len() != 4 {
        return Err(IpcError::InvalidColumn);
    }
    let value_null_count = (0..columns.count)
        .filter(|row| !columns.has_value(*row))
        .count() as i64;
    write_arrow_ipc_from_borrowed_columns(
        columns.count,
        schema_config,
        output,
        metadata_storage,
        |index| match index {
            0 => Ok(DynamicColumn::utf8(
                0,
                false,
                None,
                columns.id_offsets_bytes(),
                columns.id_data_bytes(),
            )),
            1 => Ok(DynamicColumn::utf8(
                1,
                false,
                None,
                columns.type_offsets_bytes(),
                columns.type_data_bytes(),
            )),
            2 => Ok(DynamicColumn::int64(
                2,
                false,
                None,
                columns.timestamps_bytes(),
            )),
            3 => Ok(DynamicColumn::binary(
                3,
                true,
                Some(columns.value_nulls_bytes()),
                columns.value_offsets_bytes(),
                columns.value_data_bytes(),
            )),
            _ => Err(IpcError::InvalidColumn),
        },
        |index| if index == 3 { value_null_count } else { 0 },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record_batch::{MetadataLimits, MetadataStorage};
    use crate::schema::{ArrowType, DynamicSchemaConfig, SignalSchemaField};
    use arrow_array::{
        Array, BinaryArray, BooleanArray, Float64Array, Int64Array, NullArray, StringArray,
        UInt32Array,
    };
    use arrow_ipc::reader::StreamReader;
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field, Schema};
    use std::io::Cursor;

    fn base_fields() -> [SignalSchemaField; 4] {
        [
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new(ArrowType::Int64, false),
            SignalSchemaField::new(ArrowType::Binary, true),
        ]
    }

    fn schema_bytes(fields: &[SignalSchemaField]) -> Vec<u8> {
        let fields = fields
            .iter()
            .enumerate()
            .map(|(index, metadata)| {
                let data_type = match metadata.arrow_type {
                    ArrowType::Null => DataType::Null,
                    ArrowType::Int32 => DataType::Int32,
                    ArrowType::Float64 => DataType::Float64,
                    ArrowType::Binary => DataType::Binary,
                    ArrowType::Utf8 => DataType::Utf8,
                    ArrowType::Bool => DataType::Boolean,
                    ArrowType::Int64 => DataType::Int64,
                };
                Field::new(format!("field_{index}"), data_type, metadata.is_nullable())
            })
            .collect::<Vec<_>>();
        let mut bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut bytes, &Schema::new(fields)).unwrap();
            writer.finish().unwrap();
        }
        bytes.truncate(bytes.len() - EOS_MARKER.len());
        bytes
    }

    /// End-to-end: base and dynamic writers produce byte-identical streams
    /// for the same 4-column content — the unification claim in one test.
    #[test]
    fn base_and_dynamic_streams_are_byte_identical() {
        let fields = base_fields();
        let config = DynamicSchemaConfig::new(&schema_bytes(&fields), &fields).unwrap();

        // Base path.
        let mut base = EventColumns::new(8);
        base.add_event(b"id-1", b"click", 100, Some(b"v1")).unwrap();
        base.add_event(b"id-2", b"view", 200, None).unwrap();
        let mut base_meta =
            MetadataStorage::for_fields(&fields, MetadataLimits::default()).unwrap();
        let mut base_out = vec![0u8; 8192];
        let base_len =
            write_arrow_ipc_from_columns_with_schema(&base, &config, &mut base_out, &mut base_meta)
                .unwrap();

        // Dynamic path with the same logical content.
        let mut dynamic = DynamicColumns::new(&fields, 8);
        assert!(dynamic.begin_row());
        dynamic.append_utf8(0, b"id-1").unwrap();
        dynamic.append_utf8(1, b"click").unwrap();
        dynamic.append_int64(2, 100).unwrap();
        dynamic.append_binary(3, b"v1").unwrap();
        dynamic.end_row();
        assert!(dynamic.begin_row());
        dynamic.append_utf8(0, b"id-2").unwrap();
        dynamic.append_utf8(1, b"view").unwrap();
        dynamic.append_int64(2, 200).unwrap();
        dynamic.append_null(3).unwrap();
        dynamic.end_row();
        let mut dyn_meta = MetadataStorage::for_fields(&fields, MetadataLimits::default()).unwrap();
        let mut dyn_out = vec![0u8; 8192];
        let dyn_len =
            write_arrow_ipc_from_dynamic_columns(&dynamic, &config, &mut dyn_out, &mut dyn_meta)
                .unwrap();

        // WHY (kept wire behavior): the base path emits empty validity buffers for
        // the non-nullable id/type/timestamp columns AND the dynamic path
        // does the same (validity passed only when nullable) — so the two
        // streams agree byte for byte.
        assert_eq!(base_out[..base_len], dyn_out[..dyn_len]);
        assert!(base_out[..base_len].ends_with(&EOS_MARKER));
    }

    #[test]
    fn dynamic_stream_shape() {
        let fields = [
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new(ArrowType::Int64, false),
            SignalSchemaField::new(ArrowType::Float64, true),
            SignalSchemaField::new(ArrowType::Bool, true),
        ];
        let config = DynamicSchemaConfig::new(&schema_bytes(&fields), &fields).unwrap();
        let mut cols = DynamicColumns::new(&fields, 4);
        assert!(cols.begin_row());
        cols.append_utf8(0, b"row-1").unwrap();
        cols.append_int64(1, 7).unwrap();
        cols.append_float64(2, 1.5).unwrap();
        cols.append_null(3).unwrap();
        cols.end_row();

        let mut meta = MetadataStorage::for_fields(&fields, MetadataLimits::default()).unwrap();
        let mut out = vec![0u8; 8192];
        let len =
            write_arrow_ipc_from_dynamic_columns(&cols, &config, &mut out, &mut meta).unwrap();

        // Stream = schema bytes + RecordBatch message + EOS.
        let schema_len = config.schema_bytes.len();
        assert_eq!(&out[..schema_len], &config.schema_bytes[..]);
        assert_eq!(
            u32::from_le_bytes(out[schema_len..schema_len + 4].try_into().unwrap()),
            0xFFFF_FFFF
        );
        assert!(out[..len].ends_with(&EOS_MARKER));

        // The RecordBatch metadata size matches the exact-size formula the
        // in-place strategy depends on.
        let metadata_size =
            u32::from_le_bytes(out[schema_len + 4..schema_len + 8].try_into().unwrap());
        assert_eq!(
            metadata_size as usize,
            crate::record_batch::record_batch_metadata_size(4, compute_buffer_count(&fields))
        );
    }

    #[test]
    fn mixed_schema_with_null_type_round_trips_through_arrow_reader() {
        let fields = [
            SignalSchemaField::new(ArrowType::Int32, false),
            SignalSchemaField::new(ArrowType::Float64, true),
            SignalSchemaField::new(ArrowType::Int64, false),
            SignalSchemaField::new(ArrowType::Binary, true),
            SignalSchemaField::new(ArrowType::Utf8, true),
            SignalSchemaField::new(ArrowType::Bool, true),
            SignalSchemaField::new(ArrowType::Null, true),
        ];
        let schema = Schema::new(vec![
            Field::new("u32", DataType::UInt32, false),
            Field::new("f64", DataType::Float64, true),
            Field::new("i64", DataType::Int64, false),
            Field::new("binary", DataType::Binary, true),
            Field::new("utf8", DataType::Utf8, true),
            Field::new("bool", DataType::Boolean, true),
            Field::new("null", DataType::Null, true),
        ]);
        let mut encoded_schema = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut encoded_schema, &schema).unwrap();
            writer.finish().unwrap();
        }
        encoded_schema.truncate(encoded_schema.len() - EOS_MARKER.len());
        let config = DynamicSchemaConfig::new(&encoded_schema, &fields).unwrap();
        assert_eq!(config.compute_buffer_count(), 14);

        let u32_data = [0u32, 1 << 31, u32::MAX]
            .into_iter()
            .flat_map(u32::to_le_bytes)
            .collect::<Vec<_>>();
        let f64_data = [1.5f64, 0.0, f64::NEG_INFINITY]
            .into_iter()
            .flat_map(f64::to_le_bytes)
            .collect::<Vec<_>>();
        let i64_data = [i64::MIN, 0, i64::MAX]
            .into_iter()
            .flat_map(i64::to_le_bytes)
            .collect::<Vec<_>>();
        let binary_offsets = [0u32, 2, 2, 2]
            .into_iter()
            .flat_map(u32::to_le_bytes)
            .collect::<Vec<_>>();
        let utf8_offsets = [0u32, 2, 2, 3]
            .into_iter()
            .flat_map(u32::to_le_bytes)
            .collect::<Vec<_>>();
        let validity = [0b0000_0101];
        let bool_data = [0b0000_0101];

        let mut metadata = MetadataStorage::for_fields(&fields, MetadataLimits::default()).unwrap();
        let mut output = vec![0u8; 8192];
        let written = write_arrow_ipc_from_borrowed_columns(
            3,
            &config,
            &mut output,
            &mut metadata,
            |index| match index {
                0 => Ok(DynamicColumn::int32(0, false, None, &u32_data)),
                1 => Ok(DynamicColumn::float64(1, true, Some(&validity), &f64_data)),
                2 => Ok(DynamicColumn::int64(2, false, None, &i64_data)),
                3 => Ok(DynamicColumn::binary(
                    3,
                    true,
                    Some(&validity),
                    &binary_offsets,
                    &[0, 1],
                )),
                4 => Ok(DynamicColumn::utf8(
                    4,
                    true,
                    Some(&validity),
                    &utf8_offsets,
                    "αz".as_bytes(),
                )),
                5 => Ok(DynamicColumn::boolean(5, true, Some(&validity), &bool_data)),
                6 => Ok(DynamicColumn {
                    field_idx: 6,
                    arrow_type: ArrowType::Null,
                    nullable: true,
                    validity: None,
                    data: &[],
                    offsets: None,
                }),
                _ => Err(IpcError::InvalidColumn),
            },
            |index| match index {
                1 | 3 | 4 | 5 => 1,
                6 => 3,
                _ => 0,
            },
        )
        .unwrap();

        let mut reader = StreamReader::try_new(Cursor::new(&output[..written]), None).unwrap();
        assert_eq!(reader.schema().as_ref(), &schema);
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);

        let u32s = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(u32s.values(), &[0, 1 << 31, u32::MAX]);
        let floats = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(floats.value(0), 1.5);
        assert!(floats.is_null(1));
        assert_eq!(floats.value(2), f64::NEG_INFINITY);
        let ints = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ints.values(), &[i64::MIN, 0, i64::MAX]);
        let binary = batch
            .column(3)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(binary.value(0), &[0, 1]);
        assert!(binary.is_null(1));
        assert_eq!(binary.value(2), &[]);
        let utf8 = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(utf8.value(0), "α");
        assert!(utf8.is_null(1));
        assert_eq!(utf8.value(2), "z");
        let booleans = batch
            .column(5)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(booleans.value(0));
        assert!(booleans.is_null(1));
        assert!(booleans.value(2));
        let nulls = batch
            .column(6)
            .as_any()
            .downcast_ref::<NullArray>()
            .unwrap();
        assert_eq!(nulls.len(), 3);
        assert_eq!(nulls.logical_null_count(), 3);
        assert!(reader.next().is_none());
    }

    #[test]
    fn too_small_output_refuses() {
        let fields = base_fields();
        let config = DynamicSchemaConfig::new(&schema_bytes(&fields), &fields).unwrap();
        let cols = DynamicColumns::new(&fields, 4);
        let mut meta = MetadataStorage::for_fields(&fields, MetadataLimits::default()).unwrap();
        let mut out = vec![0u8; 1024]; // < 4096 hard floor
        assert_eq!(
            write_arrow_ipc_from_dynamic_columns(&cols, &config, &mut out, &mut meta),
            Err(IpcError::BufferTooSmall {
                required: MIN_ARROW_OUTPUT_CAPACITY,
            })
        );
    }
}
