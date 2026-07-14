//! Arrow IPC stream writer (`arrow/ipc_writer.zig`, unified).
//!
//! Stream layout: `[Schema message][RecordBatch message][EOS]`.
//!
//! Drift audit: the axe-runtime copy deletes the base `EventColumns` entry
//! point (196 lines; its only addition is a comment) and builds the body
//! directly at its final IPC offset so the relocation memcpy disappears
//! (1 payload copy). Columine's dynamic path has the same optimization
//! forward-ported; only its base path still builds in a scratch half and
//! relocates. The unified port keeps BOTH entry points and uses the
//! in-place strategy for both — output bytes are identical, the scratch
//! relocation was pure waste, not semantics (t-5dacc729 audit).

use crate::columns::{DynamicColumns, EventColumns};
use crate::record_batch::{
    DynamicBodyBuilder, DynamicColumn, MetadataStorage, compute_buffer_count,
    encode_record_batch_dynamic, record_batch_metadata_size,
};
use crate::schema::{ArrowType, DynamicSchemaConfig};

/// End-of-stream marker (continuation + zero metadata size).
pub const EOS_MARKER: [u8; 8] = [0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00];

/// Errors from the stream writer (Zig `error{BufferTooSmall}`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IpcError {
    BufferTooSmall,
}

/// Incremental stream writer (`IpcWriter`): tracks the output offset across
/// schema/batch/EOS writes. The batch entry points below manage their own
/// offsets; this type serves callers composing streams manually.
#[derive(Debug)]
pub struct IpcWriter<'a> {
    buffer: &'a mut [u8],
    offset: usize,
    schema_written: bool,
}

impl<'a> IpcWriter<'a> {
    pub fn new(buffer: &'a mut [u8]) -> Self {
        Self {
            buffer,
            offset: 0,
            schema_written: false,
        }
    }

    /// Write the end-of-stream marker; false if the buffer is too small.
    pub fn write_eos(&mut self) -> bool {
        if self.buffer.len() - self.offset < EOS_MARKER.len() {
            return false;
        }
        self.buffer[self.offset..][..EOS_MARKER.len()].copy_from_slice(&EOS_MARKER);
        self.offset += EOS_MARKER.len();
        true
    }

    pub fn bytes_written(&self) -> usize {
        self.offset
    }

    pub fn output(&self) -> &[u8] {
        &self.buffer[..self.offset]
    }

    pub fn has_schema(&self) -> bool {
        self.schema_written
    }

    pub fn remaining_capacity(&self) -> usize {
        self.buffer.len() - self.offset
    }
}

/// Shared tail of both entry points: body is already built in
/// `output[body_start..]`; emit the RecordBatch message at `write_offset`
/// (in place — no body copy) and the EOS marker after it.
#[allow(clippy::too_many_arguments)] // mirrors the Zig call surface; a param struct would obscure the 1:1 mapping
fn finish_stream(
    output: &mut [u8],
    write_offset: usize,
    body_start: usize,
    row_count: i64,
    body_length: usize,
    metadata: &MetadataStorage,
    node_count: usize,
    desc_count: usize,
) -> Result<usize, IpcError> {
    let nodes = metadata.field_nodes[..node_count].to_vec();
    let descs = metadata.buffer_descs[..desc_count].to_vec();
    let rb_written = encode_record_batch_dynamic(
        &mut output[write_offset..],
        row_count,
        Some(body_start - write_offset),
        &nodes,
        &descs,
        &[],
        body_length,
    );
    if rb_written == 0 {
        return Err(IpcError::BufferTooSmall);
    }
    let mut end = write_offset + rb_written;
    if end + EOS_MARKER.len() > output.len() {
        return Err(IpcError::BufferTooSmall);
    }
    output[end..][..EOS_MARKER.len()].copy_from_slice(&EOS_MARKER);
    end += EOS_MARKER.len();
    Ok(end)
}

/// Write Arrow IPC from [`DynamicColumns`] (`writeArrowIpcFromDynamicColumns`)
/// — the sole IPC encoding path for all schemas in the AxE artifact.
/// Returns the number of valid IPC bytes at the start of `output`.
pub fn write_arrow_ipc_from_dynamic_columns(
    dyn_cols: &DynamicColumns,
    schema_config: &DynamicSchemaConfig,
    output: &mut [u8],
    metadata_storage: &mut MetadataStorage,
) -> Result<usize, IpcError> {
    let row_count = i64::from(dyn_cols.count);
    if output.len() < 4096 {
        return Err(IpcError::BufferTooSmall);
    }

    // 1. Schema message.
    let schema_len = schema_config.write_schema_message(output);
    if schema_len == 0 {
        return Err(IpcError::BufferTooSmall);
    }
    let write_offset = schema_len;

    // 2. Build the body directly at its final RecordBatch IPC position.
    let field_count = schema_config.field_metadata.len() as u32;
    let buffer_count = compute_buffer_count(&schema_config.field_metadata);
    let record_batch_prefix = 8 + record_batch_metadata_size(field_count, buffer_count);
    let body_start = write_offset
        .checked_add(record_batch_prefix)
        .ok_or(IpcError::BufferTooSmall)?;
    if body_start >= output.len() {
        return Err(IpcError::BufferTooSmall);
    }

    let (body_length, node_count, desc_count) = {
        let (_, body_region) = output.split_at_mut(body_start);
        let mut builder = DynamicBodyBuilder::new(body_region, metadata_storage);

        // 3. Add every schema column.
        for (col_idx, meta) in schema_config.field_metadata.iter().enumerate() {
            let col = &dyn_cols.columns[col_idx];
            let col_idx_u32 = col_idx as u32;

            let mut null_count = 0i64;
            for row in 0..dyn_cols.count {
                if dyn_cols.is_null(col_idx_u32, row) {
                    null_count += 1;
                }
            }

            let validity = meta
                .is_nullable()
                .then(|| col.validity_bytes(dyn_cols.count));

            let ok = match meta.arrow_type {
                ArrowType::Utf8 => builder.add_column(
                    DynamicColumn::utf8(
                        col_idx_u32,
                        meta.is_nullable(),
                        validity,
                        col.offsets_bytes(dyn_cols.count)
                            .ok_or(IpcError::BufferTooSmall)?,
                        col.data_bytes().ok_or(IpcError::BufferTooSmall)?,
                    ),
                    row_count,
                    null_count,
                ),
                ArrowType::Int32 | ArrowType::Int64 => builder.add_column(
                    DynamicColumn::int64(
                        col_idx_u32,
                        meta.is_nullable(),
                        validity,
                        col.fixed_i64_bytes(dyn_cols.count)
                            .ok_or(IpcError::BufferTooSmall)?,
                    ),
                    row_count,
                    null_count,
                ),
                ArrowType::Float64 => builder.add_column(
                    DynamicColumn::float64(
                        col_idx_u32,
                        meta.is_nullable(),
                        validity,
                        col.fixed_f64_bytes(dyn_cols.count)
                            .ok_or(IpcError::BufferTooSmall)?,
                    ),
                    row_count,
                    null_count,
                ),
                ArrowType::Bool => builder.add_column(
                    DynamicColumn::boolean(
                        col_idx_u32,
                        meta.is_nullable(),
                        validity,
                        col.bool_bytes(dyn_cols.count)
                            .ok_or(IpcError::BufferTooSmall)?,
                    ),
                    row_count,
                    null_count,
                ),
                ArrowType::Binary => builder.add_column(
                    DynamicColumn::binary(
                        col_idx_u32,
                        meta.is_nullable(),
                        validity,
                        col.offsets_bytes(dyn_cols.count)
                            .ok_or(IpcError::BufferTooSmall)?,
                        col.data_bytes().ok_or(IpcError::BufferTooSmall)?,
                    ),
                    row_count,
                    null_count,
                ),
                ArrowType::Null => builder.add_column(
                    DynamicColumn {
                        field_idx: col_idx_u32,
                        arrow_type: ArrowType::Null,
                        nullable: true,
                        validity,
                        data: &[],
                        offsets: None,
                    },
                    row_count,
                    i64::from(dyn_cols.count),
                ),
            };
            if !ok {
                return Err(IpcError::BufferTooSmall);
            }
        }
        (
            builder.body_length(),
            builder.field_nodes().len(),
            builder.buffer_descs().len(),
        )
    };

    // 4-5. RecordBatch message (in place) + EOS.
    finish_stream(
        output,
        write_offset,
        body_start,
        row_count,
        body_length,
        metadata_storage,
        node_count,
        desc_count,
    )
}

/// Write Arrow IPC from base [`EventColumns`]
/// (`writeArrowIpcFromColumnsWithSchema`, columine npm variant): maps the
/// fixed 4 fields (id, type, timestamp, value) onto the schema.
pub fn write_arrow_ipc_from_columns_with_schema(
    cols: &EventColumns,
    schema_config: &DynamicSchemaConfig,
    output: &mut [u8],
    metadata_storage: &mut MetadataStorage,
) -> Result<usize, IpcError> {
    let row_count = i64::from(cols.count);
    if output.len() < 4096 {
        return Err(IpcError::BufferTooSmall);
    }

    let schema_len = schema_config.write_schema_message(output);
    if schema_len == 0 {
        return Err(IpcError::BufferTooSmall);
    }
    let write_offset = schema_len;

    // In-place body strategy (see module doc: columine's scratch-half
    // relocation in this path is un-forward-ported drift; bytes identical).
    let record_batch_prefix = 8 + record_batch_metadata_size(4, 11);
    let body_start = write_offset
        .checked_add(record_batch_prefix)
        .ok_or(IpcError::BufferTooSmall)?;
    if body_start >= output.len() {
        return Err(IpcError::BufferTooSmall);
    }

    let (body_length, node_count, desc_count) = {
        let (_, body_region) = output.split_at_mut(body_start);
        let mut builder = DynamicBodyBuilder::new(body_region, metadata_storage);

        let mut value_null_count = 0i64;
        for i in 0..cols.count {
            if !cols.has_value(i) {
                value_null_count += 1;
            }
        }

        let ok = builder.add_column(
            DynamicColumn::utf8(
                0,
                false,
                None,
                cols.id_offsets_bytes(),
                cols.id_data_bytes(),
            ),
            row_count,
            0,
        ) && builder.add_column(
            DynamicColumn::utf8(
                1,
                false,
                None,
                cols.type_offsets_bytes(),
                cols.type_data_bytes(),
            ),
            row_count,
            0,
        ) && builder.add_column(
            DynamicColumn::int64(2, false, None, cols.timestamps_bytes()),
            row_count,
            0,
        ) && builder.add_column(
            DynamicColumn::binary(
                3,
                true,
                Some(cols.value_nulls_bytes()),
                cols.value_offsets_bytes(),
                cols.value_data_bytes(),
            ),
            row_count,
            value_null_count,
        );
        if !ok {
            return Err(IpcError::BufferTooSmall);
        }
        (
            builder.body_length(),
            builder.field_nodes().len(),
            builder.buffer_descs().len(),
        )
    };

    finish_stream(
        output,
        write_offset,
        body_start,
        row_count,
        body_length,
        metadata_storage,
        node_count,
        desc_count,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record_batch::{MetadataLimits, MetadataStorage};
    use crate::schema::{ArrowType, DynamicSchemaConfig, SignalSchemaField};

    // test "IpcWriter writes EOS"
    #[test]
    fn ipc_writer_eos() {
        let mut buffer = [0u8; 512];
        let mut writer = IpcWriter::new(&mut buffer);
        assert!(writer.write_eos());
        assert_eq!(writer.bytes_written(), 8);
        assert_eq!(writer.output(), &EOS_MARKER);
    }

    fn base_fields() -> [SignalSchemaField; 4] {
        [
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new(ArrowType::Int64, false),
            SignalSchemaField::new(ArrowType::Binary, true),
        ]
    }

    fn fake_schema_bytes() -> Vec<u8> {
        // Continuation + size prefix + 8 placeholder bytes: enough for the
        // writer, which copies schema bytes verbatim.
        vec![
            0xFF, 0xFF, 0xFF, 0xFF, 0x08, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]
    }

    /// End-to-end: base and dynamic writers produce byte-identical streams
    /// for the same 4-column content — the unification claim in one test.
    #[test]
    fn base_and_dynamic_streams_are_byte_identical() {
        let fields = base_fields();
        let config = DynamicSchemaConfig::new(&fake_schema_bytes(), &fields);

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

        // ZIG-PARITY nuance: the base path emits empty validity buffers for
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
        let config = DynamicSchemaConfig::new(&fake_schema_bytes(), &fields);
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
    fn too_small_output_refuses() {
        let fields = base_fields();
        let config = DynamicSchemaConfig::new(&fake_schema_bytes(), &fields);
        let cols = DynamicColumns::new(&fields, 4);
        let mut meta = MetadataStorage::for_fields(&fields, MetadataLimits::default()).unwrap();
        let mut out = vec![0u8; 1024]; // < 4096 hard floor
        assert_eq!(
            write_arrow_ipc_from_dynamic_columns(&cols, &config, &mut out, &mut meta),
            Err(IpcError::BufferTooSmall)
        );
    }
}
