//! Dynamic RecordBatch encoder (`arrow/dynamic_record_batch.zig`).
//!
//! Drift audit: byte-identical between columine and axe-runtime today — the
//! `recordBatchMetadataSize` + in-place-body optimization was forward-ported
//! to columine before this port. The RecordBatch FlatBuffer is hand-emitted
//! in place, field by field; no FlatBuffer library, no intermediate model.
//! The emitted bytes ARE the contract (Flechette/arrow-js parses them), so
//! every offset below mirrors the Zig source line-for-line.

use crate::schema::{ArrowType, SignalSchemaField};

/// Continuation marker for Arrow IPC messages.
pub const CONTINUATION_MARKER: u32 = 0xFFFF_FFFF;

/// Buffer descriptor (offset + length) for Arrow IPC.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BufferDesc {
    pub offset: i64,
    pub length: i64,
}

/// Field node (length + null_count) for Arrow IPC.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FieldNode {
    pub length: i64,
    pub null_count: i64,
}

/// Align a size to an 8-byte boundary (Arrow IPC requirement).
pub fn align_to_8(size: usize) -> usize {
    (size + 7) & !7usize
}

/// Buffer count for a schema: each field contributes per its type
/// (`computeBufferCount`). SignalSchemaField is the flattened physical wire
/// layout: logical nested, list, and dictionary payloads arrive as Binary
/// leaves, so summing each leaf's exact layout covers every recursive
/// physical child exactly once.
pub fn compute_buffer_count(fields: &[SignalSchemaField]) -> u32 {
    fields.iter().map(|f| f.buffer_count()).sum()
}

/// Exact size of the RecordBatch FlatBuffer metadata this encoder emits
/// (`recordBatchMetadataSize`): 76-byte base + 16 bytes per buffer/node
/// vector entry, 8-aligned.
pub fn record_batch_metadata_size(field_count: u32, buffer_count: u32) -> usize {
    let base_overhead = 76usize;
    let buffers_vec_size = 4 + buffer_count as usize * 16;
    let nodes_vec_size = 4 + field_count as usize * 16;
    align_to_8(base_overhead + buffers_vec_size + nodes_vec_size)
}

/// Retained-metadata limits (`MetadataLimits`).
#[derive(Clone, Copy, Debug)]
pub struct MetadataLimits {
    pub max_fields: usize,
    pub max_buffers: usize,
}

impl Default for MetadataLimits {
    fn default() -> Self {
        Self {
            max_fields: 256,
            max_buffers: 768,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataError {
    TooManyFields,
    TooManyBuffers,
}

/// Schema-sized retained storage for field nodes and buffer descriptors
/// (`MetadataStorage`): allocated once per processor, reused per batch.
#[derive(Clone, Debug)]
pub struct MetadataStorage {
    pub field_nodes: Vec<FieldNode>,
    pub buffer_descs: Vec<BufferDesc>,
}

impl MetadataStorage {
    pub fn for_fields(
        fields: &[SignalSchemaField],
        limits: MetadataLimits,
    ) -> Result<Self, MetadataError> {
        Self::for_counts(fields.len(), compute_buffer_count(fields) as usize, limits)
    }

    pub fn for_counts(
        field_count: usize,
        buffer_count: usize,
        limits: MetadataLimits,
    ) -> Result<Self, MetadataError> {
        if field_count > limits.max_fields {
            return Err(MetadataError::TooManyFields);
        }
        if buffer_count > limits.max_buffers {
            return Err(MetadataError::TooManyBuffers);
        }
        Ok(Self {
            field_nodes: vec![FieldNode::default(); field_count],
            buffer_descs: vec![BufferDesc::default(); buffer_count],
        })
    }
}

/// One column's borrowed buffers for RecordBatch encoding (`DynamicColumn`).
#[derive(Clone, Copy, Debug)]
pub struct DynamicColumn<'a> {
    /// Field index in schema.
    pub field_idx: u32,
    pub arrow_type: ArrowType,
    pub nullable: bool,
    /// Validity bitmap (None => empty validity buffer per Flechette).
    pub validity: Option<&'a [u8]>,
    /// Data buffer (empty for Null type).
    pub data: &'a [u8],
    /// Offsets buffer (variable-length types only).
    pub offsets: Option<&'a [u8]>,
}

impl<'a> DynamicColumn<'a> {
    pub fn utf8(
        field_idx: u32,
        nullable: bool,
        validity: Option<&'a [u8]>,
        offsets: &'a [u8],
        data: &'a [u8],
    ) -> Self {
        Self {
            field_idx,
            arrow_type: ArrowType::Utf8,
            nullable,
            validity,
            data,
            offsets: Some(offsets),
        }
    }

    pub fn binary(
        field_idx: u32,
        nullable: bool,
        validity: Option<&'a [u8]>,
        offsets: &'a [u8],
        data: &'a [u8],
    ) -> Self {
        Self {
            field_idx,
            arrow_type: ArrowType::Binary,
            nullable,
            validity,
            data,
            offsets: Some(offsets),
        }
    }

    /// ZIG-PARITY: dynamic_record_batch.zig's `int64` constructor tags the
    /// column `.Int32` (zig line 164). Harmless — both are "fixed-width,
    /// data buffer only" in `add_column`'s switch — but the tag is kept
    /// verbatim so any future per-type branching reproduces Zig; intended
    /// fix at the post-parity sweep: tag Int64.
    pub fn int64(
        field_idx: u32,
        nullable: bool,
        validity: Option<&'a [u8]>,
        data: &'a [u8],
    ) -> Self {
        Self {
            field_idx,
            arrow_type: ArrowType::Int32,
            nullable,
            validity,
            data,
            offsets: None,
        }
    }

    pub fn float64(
        field_idx: u32,
        nullable: bool,
        validity: Option<&'a [u8]>,
        data: &'a [u8],
    ) -> Self {
        Self {
            field_idx,
            arrow_type: ArrowType::Float64,
            nullable,
            validity,
            data,
            offsets: None,
        }
    }

    pub fn boolean(
        field_idx: u32,
        nullable: bool,
        validity: Option<&'a [u8]>,
        data: &'a [u8],
    ) -> Self {
        Self {
            field_idx,
            arrow_type: ArrowType::Bool,
            nullable,
            validity,
            data,
            offsets: None,
        }
    }
}

/// Body builder over a caller-provided output region (`DynamicBodyBuilder`):
/// copies each column buffer once into the 8-aligned contiguous body and
/// records descriptors into the retained [`MetadataStorage`].
pub struct DynamicBodyBuilder<'a> {
    buffer: &'a mut [u8],
    offset: usize,
    storage: &'a mut MetadataStorage,
    buffer_desc_count: usize,
    field_node_count: usize,
}

impl<'a> DynamicBodyBuilder<'a> {
    pub fn new(buffer: &'a mut [u8], storage: &'a mut MetadataStorage) -> Self {
        Self {
            buffer,
            offset: 0,
            storage,
            buffer_desc_count: 0,
            field_node_count: 0,
        }
    }

    /// Add a column: field node + validity buffer + type-specific buffers.
    /// Returns false when the output or retained metadata is too small.
    pub fn add_column(
        &mut self,
        column: DynamicColumn<'_>,
        row_count: i64,
        null_count: i64,
    ) -> bool {
        if self.field_node_count >= self.storage.field_nodes.len() {
            return false;
        }
        self.storage.field_nodes[self.field_node_count] = FieldNode {
            length: row_count,
            null_count,
        };
        self.field_node_count += 1;

        // Validity buffer (always present per Flechette convention;
        // non-nullable columns get an empty one).
        let ok = match column.validity {
            Some(validity) => self.add_buffer(validity),
            None => self.add_empty_buffer(),
        };
        if !ok {
            return false;
        }

        match column.arrow_type {
            ArrowType::Utf8 | ArrowType::Binary => {
                // Offsets buffer (required for variable-length).
                let Some(offsets) = column.offsets else {
                    return false;
                };
                self.add_buffer(offsets) && self.add_buffer(column.data)
            }
            ArrowType::Int32 | ArrowType::Int64 | ArrowType::Float64 | ArrowType::Bool => {
                self.add_buffer(column.data)
            }
            // No data buffer for Null type.
            ArrowType::Null => true,
        }
    }

    fn add_buffer(&mut self, data: &[u8]) -> bool {
        let aligned_len = align_to_8(data.len());
        if self.offset + aligned_len > self.buffer.len() {
            return false;
        }
        if self.buffer_desc_count >= self.storage.buffer_descs.len() {
            return false;
        }
        self.storage.buffer_descs[self.buffer_desc_count] = BufferDesc {
            offset: self.offset as i64,
            length: data.len() as i64,
        };
        self.buffer_desc_count += 1;

        self.buffer[self.offset..][..data.len()].copy_from_slice(data);
        // Zero-fill padding.
        self.buffer[self.offset + data.len()..self.offset + aligned_len].fill(0);
        self.offset += aligned_len;
        true
    }

    fn add_empty_buffer(&mut self) -> bool {
        if self.buffer_desc_count >= self.storage.buffer_descs.len() {
            return false;
        }
        self.storage.buffer_descs[self.buffer_desc_count] = BufferDesc {
            offset: self.offset as i64,
            length: 0,
        };
        self.buffer_desc_count += 1;
        true
    }

    pub fn body_length(&self) -> usize {
        self.offset
    }

    pub fn body(&self) -> &[u8] {
        &self.buffer[..self.offset]
    }

    pub fn buffer_descs(&self) -> &[BufferDesc] {
        &self.storage.buffer_descs[..self.buffer_desc_count]
    }

    pub fn field_nodes(&self) -> &[FieldNode] {
        &self.storage.field_nodes[..self.field_node_count]
    }
}

fn put_u16(out: &mut [u8], at: usize, value: u16) {
    out[at..at + 2].copy_from_slice(&value.to_le_bytes());
}
fn put_u32(out: &mut [u8], at: usize, value: u32) {
    out[at..at + 4].copy_from_slice(&value.to_le_bytes());
}
fn put_i32(out: &mut [u8], at: usize, value: i32) {
    out[at..at + 4].copy_from_slice(&value.to_le_bytes());
}
fn put_i64(out: &mut [u8], at: usize, value: i64) {
    out[at..at + 8].copy_from_slice(&value.to_le_bytes());
}

/// Result of [`encode_record_batch_dynamic`]: total bytes written into
/// `output` from its start, `0` when the buffer is too small (the Zig
/// sentinel contract).
///
/// Layout: `[continuation 0xFFFFFFFF][metadata_size u32][FlatBuffer][body]`.
/// When the body was already built at its final IPC position inside
/// `output` (the 1-copy strategy), the final body memcpy is skipped —
/// mirrored from the Zig pointer guard via range comparison.
pub fn encode_record_batch_dynamic(
    output: &mut [u8],
    row_count: i64,
    body_builder_output_offset: Option<usize>,
    field_nodes: &[FieldNode],
    buffer_descs: &[BufferDesc],
    body: &[u8],
    body_length: usize,
) -> usize {
    let field_count = field_nodes.len() as u32;
    let buffer_count = buffer_descs.len() as u32;

    // Metadata size mirrors dynamic_record_batch.zig:563-576.
    let buffers_vec_size = 4 + buffer_count as usize * 16;
    let metadata_size = record_batch_metadata_size(field_count, buffer_count);

    // 8 = continuation + metadata_size prefix.
    let total_size = 8 + metadata_size + body_length;
    if output.len() < total_size {
        return 0;
    }

    let mut write_offset = 0usize;
    put_u32(output, write_offset, CONTINUATION_MARKER);
    write_offset += 4;
    put_u32(output, write_offset, metadata_size as u32);
    write_offset += 4;
    let metadata_start = write_offset;

    // Clear metadata area, then build the FlatBuffer in place.
    output[metadata_start..metadata_start + metadata_size].fill(0);

    // FlatBuffer layout (offsets relative to metadata_start), verbatim from
    // dynamic_record_batch.zig:601-681:
    //   0: root offset -> Message table at 20
    //   8: Message vtable, 20: Message table
    //  42: RecordBatch vtable, 52: RecordBatch table
    //  76: buffers vector, 76+buffers_vec_size: nodes vector
    put_u32(output, metadata_start, 20);

    // Message vtable (at 8): [vtable_size][table_size][field offsets...]
    let msg_vtable = metadata_start + 8;
    put_u16(output, msg_vtable, 12); // vtable size
    put_u16(output, msg_vtable + 2, 22); // table size
    put_u16(output, msg_vtable + 4, 20); // version offset
    put_u16(output, msg_vtable + 6, 19); // header_type offset
    put_u16(output, msg_vtable + 8, 12); // header offset
    put_u16(output, msg_vtable + 10, 4); // bodyLength offset

    // Message table (at 20): soffset to vtable (20-12=8).
    put_i32(output, metadata_start + 20, 12);
    // bodyLength at 24 (i64).
    put_i64(output, metadata_start + 24, body_length as i64);
    // header offset at 32: RecordBatch table at 52, relative 52-32=20.
    put_u32(output, metadata_start + 32, 20);
    // version at 40 (u16) = 4 (IPC v4).
    put_u16(output, metadata_start + 40, 4);
    // header_type at 39 (u8) = 3 (RecordBatch).
    output[metadata_start + 39] = 3;

    // RecordBatch vtable (at 42).
    let rb_vtable = metadata_start + 42;
    put_u16(output, rb_vtable, 10); // vtable size
    put_u16(output, rb_vtable + 2, 24); // table size
    put_u16(output, rb_vtable + 4, 12); // length offset
    put_u16(output, rb_vtable + 6, 8); // nodes offset
    put_u16(output, rb_vtable + 8, 4); // buffers offset

    // RecordBatch table (at 52): soffset to vtable (52-10=42).
    put_i32(output, metadata_start + 52, 10);
    // buffers vector offset at 56 (relative).
    let buffers_vector_offset = 76usize;
    put_u32(
        output,
        metadata_start + 56,
        (buffers_vector_offset - 56) as u32,
    );
    // nodes vector offset at 60 (relative).
    let nodes_vector_offset = buffers_vector_offset + buffers_vec_size;
    put_u32(
        output,
        metadata_start + 60,
        (nodes_vector_offset - 60) as u32,
    );
    // length (row_count) at 64 (i64).
    put_i64(output, metadata_start + 64, row_count);

    // Buffers vector.
    put_u32(output, metadata_start + buffers_vector_offset, buffer_count);
    let mut at = metadata_start + buffers_vector_offset + 4;
    for buf in buffer_descs {
        put_i64(output, at, buf.offset);
        at += 8;
        put_i64(output, at, buf.length);
        at += 8;
    }

    // Nodes vector.
    put_u32(output, metadata_start + nodes_vector_offset, field_count);
    let mut at = metadata_start + nodes_vector_offset + 4;
    for node in field_nodes {
        put_i64(output, at, node.length);
        at += 8;
        put_i64(output, at, node.null_count);
        at += 8;
    }

    write_offset += metadata_size;

    // The body may already sit at its final IPC position (the caller built
    // it at `body_builder_output_offset` inside this same output buffer).
    let in_place = body_builder_output_offset == Some(write_offset);
    if !in_place {
        output[write_offset..write_offset + body_length].copy_from_slice(&body[..body_length]);
    }
    write_offset + body_length
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ArrowType, SignalSchemaField};

    fn field(arrow_type: ArrowType, nullable: bool) -> SignalSchemaField {
        SignalSchemaField::new(arrow_type, nullable)
    }

    // test "computeBufferCount for 4-field schema"
    #[test]
    fn buffer_count_4_field() {
        let fields = [
            field(ArrowType::Utf8, false),
            field(ArrowType::Utf8, false),
            field(ArrowType::Int64, false),
            field(ArrowType::Binary, true),
        ];
        assert_eq!(compute_buffer_count(&fields), 11);
    }

    // test "computeBufferCount for extended schema"
    #[test]
    fn buffer_count_extended() {
        let fields = [
            field(ArrowType::Utf8, false),
            field(ArrowType::Utf8, false),
            field(ArrowType::Int64, false),
            field(ArrowType::Utf8, true),
            field(ArrowType::Float64, true),
            field(ArrowType::Bool, true),
        ];
        assert_eq!(compute_buffer_count(&fields), 15);
    }

    // test "computeBufferCount covers null and flattened nested physical layouts"
    #[test]
    fn buffer_count_null_and_nested() {
        let fields = [
            field(ArrowType::Null, true),
            field(ArrowType::Utf8, true),
            field(ArrowType::Binary, true),
            field(ArrowType::Int64, false),
        ];
        assert_eq!(compute_buffer_count(&fields), 9);
    }

    // test "DynamicColumn constructors"
    #[test]
    fn dynamic_column_constructors() {
        let col = DynamicColumn::utf8(0, false, None, &[], &[]);
        assert_eq!(col.field_idx, 0);
        assert_eq!(col.arrow_type, ArrowType::Utf8);
        assert!(!col.nullable);
        let int_col = DynamicColumn::int64(2, false, None, &[]);
        // ZIG-PARITY tag (see constructor doc).
        assert_eq!(int_col.arrow_type, ArrowType::Int32);
        assert!(int_col.offsets.is_none());
    }

    // test "DynamicBodyBuilder addColumn Utf8"
    #[test]
    fn body_builder_utf8() {
        let mut buffer = [0u8; 256];
        let mut metadata = MetadataStorage::for_counts(1, 3, MetadataLimits::default()).unwrap();
        let mut builder = DynamicBodyBuilder::new(&mut buffer, &mut metadata);
        let offsets = [0, 0, 0, 0, 5, 0, 0, 0]; // [0, 5] as u32 LE
        let data = b"hello";
        assert!(builder.add_column(DynamicColumn::utf8(0, false, None, &offsets, data), 1, 0));
        assert_eq!(builder.buffer_descs().len(), 3);
        assert_eq!(builder.buffer_descs()[0].length, 0); // empty validity
        assert_eq!(builder.buffer_descs()[1].length, 8); // offsets
        assert_eq!(builder.buffer_descs()[2].length, 5); // data
        assert_eq!(builder.field_nodes().len(), 1);
        assert_eq!(builder.field_nodes()[0].length, 1);
        assert_eq!(builder.field_nodes()[0].null_count, 0);
    }

    // test "DynamicBodyBuilder addColumn Int64"
    #[test]
    fn body_builder_int64() {
        let mut buffer = [0u8; 256];
        let mut metadata = MetadataStorage::for_counts(1, 2, MetadataLimits::default()).unwrap();
        let mut builder = DynamicBodyBuilder::new(&mut buffer, &mut metadata);
        let data = 12345i64.to_le_bytes();
        assert!(builder.add_column(DynamicColumn::int64(0, false, None, &data), 1, 0));
        assert_eq!(builder.buffer_descs().len(), 2);
        assert_eq!(builder.buffer_descs()[0].length, 0);
        assert_eq!(builder.buffer_descs()[1].length, 8);
    }

    // test "DynamicBodyBuilder alignment"
    #[test]
    fn body_builder_alignment() {
        let mut buffer = [0u8; 256];
        let mut metadata = MetadataStorage::for_counts(2, 6, MetadataLimits::default()).unwrap();
        let mut builder = DynamicBodyBuilder::new(&mut buffer, &mut metadata);
        let offsets = [0, 0, 0, 0, 5, 0, 0, 0];
        assert!(builder.add_column(
            DynamicColumn::utf8(0, false, None, &offsets, b"hello"),
            1,
            0
        ));
        // 0 (empty validity) + 8 (offsets) + 8 (padded data) = 16.
        assert_eq!(builder.body_length(), 16);
        let offsets2 = [0, 0, 0, 0, 3, 0, 0, 0];
        assert!(builder.add_column(DynamicColumn::utf8(1, false, None, &offsets2, b"abc"), 1, 0));
        assert_eq!(builder.buffer_descs()[4].offset, 16);
    }

    // test "DynamicBodyBuilder uses exact retained metadata beyond 64 fields"
    #[test]
    fn body_builder_80_fields() {
        let mut output = [0u8; 4096];
        let mut metadata = MetadataStorage::for_counts(80, 240, MetadataLimits::default()).unwrap();
        let mut builder = DynamicBodyBuilder::new(&mut output, &mut metadata);
        let validity = [1u8];
        let offsets = [0u8; 8];
        let data = [0u8; 8];
        for field_idx in 0..80 {
            assert!(builder.add_column(
                DynamicColumn::binary(field_idx, true, Some(&validity), &offsets, &data),
                1,
                0
            ));
        }
        assert_eq!(builder.field_nodes().len(), 80);
        assert_eq!(builder.buffer_descs().len(), 240);
    }

    // test "MetadataStorage enforces configured exact boundaries"
    #[test]
    fn metadata_limits() {
        let limits = MetadataLimits {
            max_fields: 80,
            max_buffers: 240,
        };
        assert!(MetadataStorage::for_counts(80, 240, limits).is_ok());
        assert_eq!(
            MetadataStorage::for_counts(81, 240, limits).unwrap_err(),
            MetadataError::TooManyFields
        );
        assert_eq!(
            MetadataStorage::for_counts(80, 241, limits).unwrap_err(),
            MetadataError::TooManyBuffers
        );
    }

    // test "encodeRecordBatchDynamic basic"
    #[test]
    fn encode_basic() {
        let mut body_buffer = [0u8; 256];
        let mut metadata = MetadataStorage::for_counts(1, 3, MetadataLimits::default()).unwrap();
        let mut builder = DynamicBodyBuilder::new(&mut body_buffer, &mut metadata);
        let offsets = [0, 0, 0, 0, 5, 0, 0, 0];
        assert!(builder.add_column(
            DynamicColumn::utf8(0, false, None, &offsets, b"hello"),
            1,
            0
        ));
        let body_length = builder.body_length();
        let nodes = builder.field_nodes().to_vec();
        let descs = builder.buffer_descs().to_vec();
        let body = builder.body().to_vec();

        let mut output = [0u8; 1024];
        let result =
            encode_record_batch_dynamic(&mut output, 1, None, &nodes, &descs, &body, body_length);
        assert_eq!(
            u32::from_le_bytes(output[0..4].try_into().unwrap()),
            0xFFFF_FFFF
        );
        assert!(result > 8);
    }

    // test "encodeRecordBatchDynamic 4-field schema"
    #[test]
    fn encode_4_field_in_place() {
        let mut output = [0u8; 2048];
        let body_start = 8 + record_batch_metadata_size(4, 11);
        let mut metadata = MetadataStorage::for_counts(4, 11, MetadataLimits::default()).unwrap();
        let (body_length, nodes, descs) = {
            let mut builder = DynamicBodyBuilder::new(&mut output[body_start..], &mut metadata);
            let id_offsets = [0, 0, 0, 0, 7, 0, 0, 0];
            assert!(builder.add_column(
                DynamicColumn::utf8(0, false, None, &id_offsets, b"test-id"),
                1,
                0
            ));
            let type_offsets = [0, 0, 0, 0, 5, 0, 0, 0];
            assert!(builder.add_column(
                DynamicColumn::utf8(1, false, None, &type_offsets, b"click"),
                1,
                0
            ));
            let ts = 1_705_315_800_000_000_i64.to_le_bytes();
            assert!(builder.add_column(DynamicColumn::int64(2, false, None, &ts), 1, 0));
            let value_offsets = [0u8; 8];
            let validity = [0u8];
            assert!(builder.add_column(
                DynamicColumn::binary(3, true, Some(&validity), &value_offsets, b""),
                1,
                1
            ));
            (
                builder.body_length(),
                builder.field_nodes().to_vec(),
                builder.buffer_descs().to_vec(),
            )
        };

        // Body already occupies its final region: pass the in-place offset.
        let result = encode_record_batch_dynamic(
            &mut output,
            1,
            Some(body_start),
            &nodes,
            &descs,
            &[],
            body_length,
        );
        assert_eq!(
            u32::from_le_bytes(output[0..4].try_into().unwrap()),
            0xFFFF_FFFF
        );
        let metadata_size = u32::from_le_bytes(output[4..8].try_into().unwrap());
        assert!(metadata_size > 100 && metadata_size < 400);
        assert_eq!(8 + metadata_size as usize + body_length, result);
    }
}
