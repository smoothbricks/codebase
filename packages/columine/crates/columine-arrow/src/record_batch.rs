//! Dynamic RecordBatch and DictionaryBatch encoders.
//!
//! Both FlatBuffers are hand-emitted in place, field by field; no FlatBuffer
//! library or intermediate model is needed. The emitted bytes are the
//! contract consumed by Flechette, arrow-js and arrow-ipc, so every offset is
//! explicit.

use crate::ipc::IpcError;
use crate::schema::{PlaneKind, SignalSchemaField};

pub use crate::schema::compute_buffer_count;

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

/// Exact size of the RecordBatch Message metadata, including standard per-batch
/// KeyValue pairs. An empty slice keeps the original message layout unchanged.
pub fn record_batch_metadata_size(
    field_count: u32,
    buffer_count: u32,
    custom_metadata: &[(&str, &str)],
) -> usize {
    if custom_metadata.is_empty() {
        return align_to_8(76 + 4 + buffer_count as usize * 16 + 4 + field_count as usize * 16);
    }
    let mut size =
        104 + (buffer_count as usize + field_count as usize) * 16 + 4 + custom_metadata.len() * 4;
    for (key, value) in custom_metadata {
        // KeyValue vtable (8), table (12), and two length-prefixed, NUL-terminated strings.
        size += 20 + align_to_4(5 + key.len()) + align_to_4(5 + value.len());
    }
    align_to_8(size)
}

fn align_to_4(size: usize) -> usize {
    (size + 3) & !3usize
}

/// The retained storage could not be backed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataOutOfMemory;

/// Retained storage for one record batch's field nodes and buffer
/// descriptors (`MetadataStorage`): allocated once per processor at exactly
/// the counts its schema's layout gives, reused per batch.
#[derive(Clone, Debug)]
pub struct MetadataStorage {
    pub field_nodes: Vec<FieldNode>,
    pub buffer_descs: Vec<BufferDesc>,
}

impl MetadataStorage {
    /// Storage for one record batch of `fields`: one node per field and the
    /// buffers each field's own layout declares.
    pub fn for_fields(fields: &[SignalSchemaField]) -> Result<Self, MetadataOutOfMemory> {
        Self::for_counts(fields.len(), compute_buffer_count(fields) as usize)
    }

    pub fn for_counts(
        field_count: usize,
        buffer_count: usize,
    ) -> Result<Self, MetadataOutOfMemory> {
        Ok(Self {
            field_nodes: crate::fallible::filled(FieldNode::default(), field_count)
                .ok_or(MetadataOutOfMemory)?,
            buffer_descs: crate::fallible::filled(BufferDesc::default(), buffer_count)
                .ok_or(MetadataOutOfMemory)?,
        })
    }
}

/// One column's borrowed buffers for RecordBatch encoding (`DynamicColumn`).
///
/// Carries the whole field descriptor rather than a bare tag, so the column
/// knows its own plane kind — including the `FixedSizeBinary` byte width — and
/// cannot disagree with itself about nullability.
#[derive(Clone, Copy, Debug)]
pub struct DynamicColumn<'a> {
    /// Field index in schema.
    pub field_idx: u32,
    pub field: SignalSchemaField,
    /// Validity bitmap (None => empty validity buffer per Flechette).
    pub validity: Option<&'a [u8]>,
    /// Data buffer (empty for the Null plane).
    pub data: &'a [u8],
    /// Offsets buffer (variable-width planes only).
    pub offsets: Option<&'a [u8]>,
}

impl<'a> DynamicColumn<'a> {
    /// A fixed-width, bit-packed, or empty column: validity plus data.
    pub fn fixed(
        field_idx: u32,
        field: SignalSchemaField,
        validity: Option<&'a [u8]>,
        data: &'a [u8],
    ) -> Self {
        Self {
            field_idx,
            field,
            validity,
            data,
            offsets: None,
        }
    }

    /// A variable-width column: validity, offsets, then the byte payload.
    pub fn variable(
        field_idx: u32,
        field: SignalSchemaField,
        validity: Option<&'a [u8]>,
        offsets: &'a [u8],
        data: &'a [u8],
    ) -> Self {
        Self {
            field_idx,
            field,
            validity,
            data,
            offsets: Some(offsets),
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

        // Arrow Null arrays have a field node but no buffers, including no
        // validity bitmap. Nullness is carried entirely by `null_count`.
        if column.field.plane_kind() == PlaneKind::Empty {
            return column.validity.is_none()
                && column.offsets.is_none()
                && column.data.is_empty()
                && null_count == row_count;
        }

        // Every non-Null physical type has a validity buffer descriptor;
        // non-nullable columns use an empty descriptor.
        let ok = match column.validity {
            Some(validity) => self.add_buffer(validity),
            None => self.add_empty_buffer(),
        };
        if !ok {
            return false;
        }

        // Offsets belong to exactly the variable-width planes. Emitting the
        // wrong count here would produce a body whose buffer vector disagrees
        // with the Schema message, which is unreadable rather than merely
        // wrong, so a mismatched shape fails instead of being copied.
        match column.field.plane_kind().offset_width() {
            Some(_) => {
                let Some(offsets) = column.offsets else {
                    return false;
                };
                self.add_buffer(offsets) && self.add_buffer(column.data)
            }
            None => column.offsets.is_none() && self.add_buffer(column.data),
        }
    }

    fn add_buffer(&mut self, data: &[u8]) -> bool {
        let Some(aligned_len) = data.len().checked_add(7).map(|value| value & !7usize) else {
            return false;
        };
        let Some(end) = self.offset.checked_add(aligned_len) else {
            return false;
        };
        if end > self.buffer.len() {
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

        self.buffer[self.offset..self.offset + data.len()].copy_from_slice(data);
        self.buffer[self.offset + data.len()..end].fill(0);
        self.offset = end;
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
/// `output` from its start, or `0` when the buffer is too small.
///
/// Layout: `[continuation 0xFFFFFFFF][metadata_size u32][FlatBuffer][body]`.
/// Some(body) copies borrowed bytes; None means the caller has already placed
/// body_length bytes at 8 + record_batch_metadata_size(..., custom_metadata).
pub fn encode_record_batch_dynamic(
    output: &mut [u8],
    row_count: i64,
    field_nodes: &[FieldNode],
    buffer_descs: &[BufferDesc],
    body: Option<&[u8]>,
    body_length: usize,
    custom_metadata: &[(&str, &str)],
) -> usize {
    let field_count = field_nodes.len() as u32;
    let buffer_count = buffer_descs.len() as u32;

    // Compute metadata size from the field and buffer vectors.
    let buffers_vec_size = 4 + buffer_count as usize * 16;
    let metadata_size = record_batch_metadata_size(field_count, buffer_count, custom_metadata);

    // 8 = continuation + metadata_size prefix.
    let Some(total_size) = 8_usize
        .checked_add(metadata_size)
        .and_then(|size| size.checked_add(body_length))
    else {
        return 0;
    };
    if output.len() < total_size || body.is_some_and(|bytes| bytes.len() < body_length) {
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

    let (buffers_vector_offset, nodes_vector_offset) = if custom_metadata.is_empty() {
        // Original Message and RecordBatch layout: no custom_metadata vtable entry.
        put_u32(output, metadata_start, 20);
        let msg_vtable = metadata_start + 8;
        put_u16(output, msg_vtable, 12);
        put_u16(output, msg_vtable + 2, 22);
        put_u16(output, msg_vtable + 4, 20);
        put_u16(output, msg_vtable + 6, 19);
        put_u16(output, msg_vtable + 8, 12);
        put_u16(output, msg_vtable + 10, 4);
        put_i32(output, metadata_start + 20, 12);
        put_i64(output, metadata_start + 24, body_length as i64);
        put_u32(output, metadata_start + 32, 20);
        put_u16(output, metadata_start + 40, 4);
        output[metadata_start + 39] = 3;
        let rb_vtable = metadata_start + 42;
        put_u16(output, rb_vtable, 10);
        put_u16(output, rb_vtable + 2, 24);
        put_u16(output, rb_vtable + 4, 12);
        put_u16(output, rb_vtable + 6, 8);
        put_u16(output, rb_vtable + 8, 4);
        put_i32(output, metadata_start + 52, 10);
        put_u32(output, metadata_start + 56, 20);
        put_u32(output, metadata_start + 60, (16 + buffers_vec_size) as u32);
        put_i64(output, metadata_start + 64, row_count);
        (76, 76 + buffers_vec_size)
    } else {
        // Message.fbs fields: version, header_type, header, bodyLength, custom_metadata.
        // The larger vtable precedes Message at 24; RecordBatch follows at 64.
        put_u32(output, metadata_start, 24);
        let msg_vtable = metadata_start + 8;
        for (index, value) in [14, 28, 26, 25, 16, 8, 20].into_iter().enumerate() {
            put_u16(output, msg_vtable + index * 2, value);
        }
        put_i32(output, metadata_start + 24, 16);
        put_i64(output, metadata_start + 32, body_length as i64);
        put_u32(output, metadata_start + 40, 24);
        output[metadata_start + 49] = 3;
        put_u16(output, metadata_start + 50, 4);
        let rb_vtable = metadata_start + 52;
        for (index, value) in [10, 24, 16, 8, 4].into_iter().enumerate() {
            put_u16(output, rb_vtable + index * 2, value);
        }
        put_i32(output, metadata_start + 64, 12);
        put_u32(output, metadata_start + 68, 24);
        let nodes = 100 + buffer_count as usize * 16;
        put_u32(output, metadata_start + 72, (nodes - 72) as u32);
        put_i64(output, metadata_start + 80, row_count);
        (92, nodes)
    };

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

    if !custom_metadata.is_empty() {
        let vector = metadata_start + nodes_vector_offset + 4 + field_nodes.len() * 16;
        put_u32(
            output,
            metadata_start + 44,
            (vector - metadata_start - 44) as u32,
        );
        put_u32(output, vector, custom_metadata.len() as u32);
        let mut entry = vector + 4 + custom_metadata.len() * 4;
        for (index, (key, value)) in custom_metadata.iter().enumerate() {
            let table = entry + 8;
            let slot = vector + 4 + index * 4;
            put_u32(output, slot, (table - slot) as u32);
            for (index, value) in [8, 12, 4, 8].into_iter().enumerate() {
                put_u16(output, entry + index * 2, value);
            }
            put_i32(output, table, 8);
            entry = table + 12;
            for (slot, text) in [(table + 4, *key), (table + 8, *value)] {
                put_u32(output, slot, (entry - slot) as u32);
                put_u32(output, entry, text.len() as u32);
                output[entry + 4..entry + 4 + text.len()].copy_from_slice(text.as_bytes());
                entry += align_to_4(5 + text.len());
            }
        }
    }

    write_offset += metadata_size;

    if let Some(body) = body {
        output[write_offset..write_offset + body_length].copy_from_slice(&body[..body_length]);
    }
    write_offset + body_length
}

/// The values of one dictionary message, in index order.
///
/// Read twice by the writer, once to size the message and once to write it,
/// so it is a lookup and not an iterator.
pub trait DictionaryValues {
    /// How many values the message carries.
    fn len(&self) -> u32;

    /// Whether the message carries no value.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The UTF-8 bytes of value `index`, or `None` for a null entry.
    fn value(&self, index: u32) -> Option<&[u8]>;
}

/// Metadata bytes of one DictionaryBatch message of Utf8 values: the fixed
/// FlatBuffer [`encode_dictionary_batch`] emits.
const DICTIONARY_METADATA_SIZE: usize = 176;

/// The three body buffers of a Utf8 dictionary: validity, offsets, data.
struct DictionaryBody {
    validity_len: usize,
    offsets_len: usize,
    data_len: usize,
    null_count: i64,
}

impl DictionaryBody {
    fn of(values: &(impl DictionaryValues + ?Sized)) -> Result<Self, IpcError> {
        let count = values.len();
        let mut data_len = 0usize;
        let mut null_count = 0i64;
        for index in 0..count {
            match values.value(index) {
                Some(value) => {
                    data_len = data_len
                        .checked_add(value.len())
                        .ok_or(IpcError::SizeOverflow)?;
                }
                None => null_count += 1,
            }
        }
        // Utf8 offsets are `i32`: a dictionary whose text passes that is not
        // a Utf8 array.
        if i32::try_from(data_len).is_err() {
            return Err(IpcError::SizeOverflow);
        }
        Ok(Self {
            // No null entry, no bitmap: an empty validity buffer says every
            // value is valid.
            validity_len: if null_count == 0 {
                0
            } else {
                (count as usize).div_ceil(8)
            },
            offsets_len: (count as usize + 1) * 4,
            data_len,
            null_count,
        })
    }

    fn len(&self) -> usize {
        align_to_8(self.validity_len) + align_to_8(self.offsets_len) + align_to_8(self.data_len)
    }
}

/// Exact byte length of the DictionaryBatch message [`encode_dictionary_batch`]
/// writes for `values`.
pub fn dictionary_batch_len(values: &(impl DictionaryValues + ?Sized)) -> Result<usize, IpcError> {
    Ok(8 + DICTIONARY_METADATA_SIZE + DictionaryBody::of(values)?.len())
}

/// Write one continuation-prefixed DictionaryBatch message of Utf8 values
/// for the Arrow dictionary `id` at the start of `output`, and return its
/// length. `is_delta` appends the values to the dictionary a reader holds;
/// otherwise the message defines it.
///
/// Layout: `[continuation][metadata_size u32][FlatBuffer][body]`, the body
/// being the values' validity, offsets and data buffers, each padded to
/// eight bytes.
pub fn encode_dictionary_batch(
    output: &mut [u8],
    id: i64,
    is_delta: bool,
    values: &(impl DictionaryValues + ?Sized),
) -> Result<usize, IpcError> {
    let body = DictionaryBody::of(values)?;
    let total = 8 + DICTIONARY_METADATA_SIZE + body.len();
    if output.len() < total {
        return Err(IpcError::BufferTooSmall { required: total });
    }
    let count = values.len();
    output[..total].fill(0);
    put_u32(output, 0, CONTINUATION_MARKER);
    put_u32(output, 4, DICTIONARY_METADATA_SIZE as u32);
    let m = 8usize;

    // Message: version, header_type, header, bodyLength.
    put_u32(output, m, 20);
    for (index, value) in [12, 22, 20, 19, 12, 4].into_iter().enumerate() {
        put_u16(output, m + 8 + index * 2, value);
    }
    put_i32(output, m + 20, 12);
    put_i64(output, m + 24, body.len() as i64);
    put_u32(output, m + 32, 20);
    output[m + 39] = 2; // MessageHeader::DictionaryBatch
    put_u16(output, m + 40, 4); // MetadataVersion::V5

    // DictionaryBatch: id, data, isDelta.
    for (index, value) in [10, 17, 4, 12, 16].into_iter().enumerate() {
        put_u16(output, m + 42 + index * 2, value);
    }
    put_i32(output, m + 52, 10);
    put_i64(output, m + 56, id);
    put_u32(output, m + 64, 16);
    output[m + 68] = u8::from(is_delta);

    // RecordBatch: length, nodes, buffers.
    for (index, value) in [10, 20, 8, 16, 4].into_iter().enumerate() {
        put_u16(output, m + 70 + index * 2, value);
    }
    put_i32(output, m + 80, 10);
    put_u32(output, m + 84, 16);
    put_i64(output, m + 88, i64::from(count));
    put_u32(output, m + 96, 60);

    // Buffers: validity, offsets, data, at their padded body positions.
    let offsets_at = align_to_8(body.validity_len);
    let data_at = offsets_at + align_to_8(body.offsets_len);
    put_u32(output, m + 100, 3);
    for (index, (offset, length)) in [
        (0, body.validity_len),
        (offsets_at, body.offsets_len),
        (data_at, body.data_len),
    ]
    .into_iter()
    .enumerate()
    {
        put_i64(output, m + 104 + index * 16, offset as i64);
        put_i64(output, m + 112 + index * 16, length as i64);
    }

    // Nodes: the one values array.
    put_u32(output, m + 156, 1);
    put_i64(output, m + 160, i64::from(count));
    put_i64(output, m + 168, body.null_count);

    let body_at = m + DICTIONARY_METADATA_SIZE;
    let mut written = 0usize;
    for index in 0..count {
        put_u32(
            output,
            body_at + offsets_at + index as usize * 4,
            written as u32,
        );
        if let Some(value) = values.value(index) {
            let at = body_at + data_at + written;
            output[at..at + value.len()].copy_from_slice(value);
            written += value.len();
            if body.validity_len != 0 {
                output[body_at + index as usize / 8] |= 1u8 << (index % 8);
            }
        }
    }
    put_u32(
        output,
        body_at + offsets_at + count as usize * 4,
        written as u32,
    );
    // A value that changed between the sizing read and this one would leave
    // the offsets describing bytes the body does not hold.
    if written != body.data_len {
        return Err(IpcError::InvalidColumn);
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ArrowType, DictionarySlot, IndexWidth, SignalSchemaField};
    use arrow_schema::TimeUnit;

    fn field(arrow_type: ArrowType, nullable: bool) -> SignalSchemaField {
        SignalSchemaField::new(arrow_type, nullable)
    }

    // test "computeBufferCount for 4-field schema"
    #[test]
    fn buffer_count_4_field() {
        let fields = [
            field(ArrowType::Utf8, false),
            field(ArrowType::Utf8, false),
            SignalSchemaField::new_timestamp(TimeUnit::Nanosecond, false),
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
        assert_eq!(compute_buffer_count(&fields), 8);
    }

    // test "DynamicColumn constructors"
    #[test]
    fn dynamic_column_constructors() {
        let col = DynamicColumn::variable(
            0,
            SignalSchemaField::new(ArrowType::Utf8, false),
            None,
            &[],
            &[],
        );
        assert_eq!(col.field_idx, 0);
        assert_eq!(col.field.arrow_type, ArrowType::Utf8);
        assert!(!col.field.is_nullable());
        let int_col = DynamicColumn::fixed(
            2,
            SignalSchemaField::new(ArrowType::Int64, false),
            None,
            &[],
        );
        // The tag is authoritative for the column's Arrow type.
        assert_eq!(int_col.field.arrow_type, ArrowType::Int64);
        assert!(int_col.offsets.is_none());
    }

    // test "DynamicBodyBuilder addColumn Utf8"
    #[test]
    fn body_builder_utf8() {
        let mut buffer = [0u8; 256];
        let mut metadata = MetadataStorage::for_counts(1, 3).unwrap();
        let mut builder = DynamicBodyBuilder::new(&mut buffer, &mut metadata);
        let offsets = [0, 0, 0, 0, 5, 0, 0, 0]; // [0, 5] as u32 LE
        let data = b"hello";
        assert!(builder.add_column(
            DynamicColumn::variable(
                0,
                SignalSchemaField::new(ArrowType::Utf8, false),
                None,
                &offsets,
                data
            ),
            1,
            0
        ));
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
        let mut metadata = MetadataStorage::for_counts(1, 2).unwrap();
        let mut builder = DynamicBodyBuilder::new(&mut buffer, &mut metadata);
        let data = 12345i64.to_le_bytes();
        assert!(builder.add_column(
            DynamicColumn::fixed(
                0,
                SignalSchemaField::new(ArrowType::Int64, false),
                None,
                &data
            ),
            1,
            0
        ));
        assert_eq!(builder.buffer_descs().len(), 2);
        assert_eq!(builder.buffer_descs()[0].length, 0);
        assert_eq!(builder.buffer_descs()[1].length, 8);
    }

    // test "DynamicBodyBuilder alignment"
    #[test]
    fn body_builder_alignment() {
        let mut buffer = [0u8; 256];
        let mut metadata = MetadataStorage::for_counts(2, 6).unwrap();
        let mut builder = DynamicBodyBuilder::new(&mut buffer, &mut metadata);
        let offsets = [0, 0, 0, 0, 5, 0, 0, 0];
        assert!(builder.add_column(
            DynamicColumn::variable(
                0,
                SignalSchemaField::new(ArrowType::Utf8, false),
                None,
                &offsets,
                b"hello"
            ),
            1,
            0
        ));
        // 0 (empty validity) + 8 (offsets) + 8 (padded data) = 16.
        assert_eq!(builder.body_length(), 16);
        let offsets2 = [0, 0, 0, 0, 3, 0, 0, 0];
        assert!(builder.add_column(
            DynamicColumn::variable(
                1,
                SignalSchemaField::new(ArrowType::Utf8, false),
                None,
                &offsets2,
                b"abc"
            ),
            1,
            0
        ));
        assert_eq!(builder.buffer_descs()[4].offset, 16);
    }

    // test "DynamicBodyBuilder uses exact retained metadata beyond 64 fields"
    #[test]
    fn body_builder_80_fields() {
        let mut output = [0u8; 4096];
        let mut metadata = MetadataStorage::for_counts(80, 240).unwrap();
        let mut builder = DynamicBodyBuilder::new(&mut output, &mut metadata);
        let validity = [1u8];
        let offsets = [0u8; 8];
        let data = [0u8; 8];
        for field_idx in 0..80 {
            assert!(builder.add_column(
                DynamicColumn::variable(
                    field_idx,
                    SignalSchemaField::new(ArrowType::Binary, true),
                    Some(&validity),
                    &offsets,
                    &data
                ),
                1,
                0
            ));
        }
        assert_eq!(builder.field_nodes().len(), 80);
        assert_eq!(builder.buffer_descs().len(), 240);
    }

    // test "encodeRecordBatchDynamic basic"
    #[test]
    fn encode_basic() {
        let mut body_buffer = [0u8; 256];
        let mut metadata = MetadataStorage::for_counts(1, 3).unwrap();
        let mut builder = DynamicBodyBuilder::new(&mut body_buffer, &mut metadata);
        let offsets = [0, 0, 0, 0, 5, 0, 0, 0];
        assert!(builder.add_column(
            DynamicColumn::variable(
                0,
                SignalSchemaField::new(ArrowType::Utf8, false),
                None,
                &offsets,
                b"hello"
            ),
            1,
            0
        ));
        let body_length = builder.body_length();
        let nodes = builder.field_nodes().to_vec();
        let descs = builder.buffer_descs().to_vec();
        let body = builder.body().to_vec();

        let mut output = [0u8; 1024];
        let result = encode_record_batch_dynamic(
            &mut output,
            1,
            &nodes,
            &descs,
            Some(&body),
            body_length,
            &[],
        );
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
        let body_start = 8 + record_batch_metadata_size(4, 11, &[]);
        let mut metadata = MetadataStorage::for_counts(4, 11).unwrap();
        let (body_length, nodes, descs) = {
            let mut builder = DynamicBodyBuilder::new(&mut output[body_start..], &mut metadata);
            let id_offsets = [0, 0, 0, 0, 7, 0, 0, 0];
            assert!(builder.add_column(
                DynamicColumn::variable(
                    0,
                    SignalSchemaField::new(ArrowType::Utf8, false),
                    None,
                    &id_offsets,
                    b"test-id"
                ),
                1,
                0
            ));
            let type_offsets = [0, 0, 0, 0, 5, 0, 0, 0];
            assert!(builder.add_column(
                DynamicColumn::variable(
                    1,
                    SignalSchemaField::new(ArrowType::Utf8, false),
                    None,
                    &type_offsets,
                    b"click"
                ),
                1,
                0
            ));
            let ts = 1_705_315_800_000_000_000_i64.to_le_bytes();
            assert!(builder.add_column(
                DynamicColumn::fixed(
                    2,
                    SignalSchemaField::new_timestamp(TimeUnit::Nanosecond, false),
                    None,
                    &ts
                ),
                1,
                0
            ));
            let value_offsets = [0u8; 8];
            let validity = [0u8];
            assert!(builder.add_column(
                DynamicColumn::variable(
                    3,
                    SignalSchemaField::new(ArrowType::Binary, true),
                    Some(&validity),
                    &value_offsets,
                    b""
                ),
                1,
                1
            ));
            (
                builder.body_length(),
                builder.field_nodes().to_vec(),
                builder.buffer_descs().to_vec(),
            )
        };

        // None means the body already occupies its metadata-derived final region.
        let result =
            encode_record_batch_dynamic(&mut output, 1, &nodes, &descs, None, body_length, &[]);
        assert_eq!(
            u32::from_le_bytes(output[0..4].try_into().unwrap()),
            0xFFFF_FFFF
        );
        let metadata_size = u32::from_le_bytes(output[4..8].try_into().unwrap());
        assert!(metadata_size > 100 && metadata_size < 400);
        assert_eq!(8 + metadata_size as usize + body_length, result);
    }

    #[test]
    fn borrowed_and_in_place_bodies_match_with_message_metadata() {
        let nodes = [FieldNode {
            length: 4,
            null_count: 0,
        }];
        let buffers = [BufferDesc {
            offset: 0,
            length: 32,
        }];
        let body = [
            0, 0, 0, 0, 0, 0, 0, 128, 255, 255, 255, 255, 255, 255, 255, 127, 0, 255, 0, 255, 0,
            255, 0, 255, 255, 0, 255, 0, 255, 0, 255, 0,
        ];
        let pairs = [("batch.source", "α𐐀"), ("", "")];
        for metadata in [&[][..], &pairs[..]] {
            let body_start = 8 + record_batch_metadata_size(1, 1, metadata);
            let required = body_start + body.len();
            let mut borrowed = vec![0xaa; required];
            let mut in_place = vec![0xbb; required];
            in_place[body_start..].copy_from_slice(&body);
            assert_eq!(
                encode_record_batch_dynamic(
                    &mut borrowed,
                    4,
                    &nodes,
                    &buffers,
                    Some(&body),
                    body.len(),
                    metadata,
                ),
                required
            );
            assert_eq!(
                encode_record_batch_dynamic(
                    &mut in_place,
                    4,
                    &nodes,
                    &buffers,
                    None,
                    body.len(),
                    metadata,
                ),
                required
            );
            assert_eq!(in_place, borrowed);
            assert_eq!(&in_place[body_start..], &body);
            let mut refused = vec![0xcc; required];
            assert_eq!(
                encode_record_batch_dynamic(
                    &mut refused,
                    4,
                    &nodes,
                    &buffers,
                    Some(&body[..31]),
                    body.len(),
                    metadata,
                ),
                0
            );
            assert!(refused.iter().all(|byte| *byte == 0xcc));
            assert_eq!(
                encode_record_batch_dynamic(
                    &mut refused[..required - 1],
                    4,
                    &nodes,
                    &buffers,
                    None,
                    body.len(),
                    metadata,
                ),
                0
            );
            assert!(refused.iter().all(|byte| *byte == 0xcc));
            assert_eq!(
                encode_record_batch_dynamic(
                    &mut refused,
                    4,
                    &nodes,
                    &buffers,
                    None,
                    usize::MAX,
                    metadata,
                ),
                0
            );
            assert!(refused.iter().all(|byte| *byte == 0xcc));
        }
    }

    #[test]
    fn empty_message_metadata_keeps_the_original_exact_bytes() {
        let nodes = [FieldNode {
            length: 1,
            null_count: 0,
        }];
        let buffers = [BufferDesc {
            offset: 0,
            length: 8,
        }];
        let mut output = [0xaa; 136];
        let written = encode_record_batch_dynamic(
            &mut output,
            1,
            &nodes,
            &buffers,
            Some(&42_i64.to_le_bytes()),
            8,
            &[],
        );
        let expected = [
            255, 255, 255, 255, 120, 0, 0, 0, 20, 0, 0, 0, 0, 0, 0, 0, 12, 0, 22, 0, 20, 0, 19, 0,
            12, 0, 4, 0, 12, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 20, 0, 0, 0, 0, 0, 0, 3, 4, 0, 10, 0,
            24, 0, 12, 0, 8, 0, 4, 0, 10, 0, 0, 0, 20, 0, 0, 0, 36, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 0, 0, 0, 0, 0,
        ];
        assert_eq!(written, expected.len());
        assert_eq!(output, expected);
    }

    //#region smoo/columine!n/vocab-buffer-budget

    /// A field is one node and the buffers its own layout declares. An encoded
    /// column's are its index plane's, never its values'.
    #[test]
    fn retained_metadata_is_sized_by_each_fields_own_buffers() {
        let fields = [
            field(ArrowType::Null, true),
            SignalSchemaField::new_dictionary(DictionarySlot::Schema(0), IndexWidth::U8, true),
            SignalSchemaField::new_dictionary(DictionarySlot::Stream, IndexWidth::U32, false),
            field(ArrowType::Utf8, true),
            SignalSchemaField::new_timestamp(TimeUnit::Nanosecond, false),
            SignalSchemaField::new_duration(TimeUnit::Second, true),
        ];
        // Null has none; an encoded column has validity and indexes; Utf8 has
        // validity, offsets and data; each temporal plane has validity and data.
        assert_eq!(compute_buffer_count(&fields), 11);
        let storage = MetadataStorage::for_fields(&fields).unwrap();
        assert_eq!(storage.field_nodes.len(), 6);
        assert_eq!(storage.buffer_descs.len(), 11);
    }

    /// A dictionary's values travel in their own message, exactly as long as
    /// it is sized, with a null entry or none, and a short output names the
    /// length it needs.
    #[test]
    fn a_dictionary_batch_is_exactly_as_long_as_it_is_sized() {
        let with_a_null: Vec<Option<String>> =
            vec![Some("open".into()), None, Some("closed".into())];
        let empty: Vec<Option<String>> = Vec::new();
        for values in [with_a_null, empty] {
            let required = dictionary_batch_len(values.as_slice()).unwrap();
            assert_eq!(required % 8, 0, "an Arrow message is eight-byte aligned");
            let mut output = vec![0xaa; required + 16];
            assert_eq!(
                encode_dictionary_batch(&mut output, 3, true, values.as_slice()),
                Ok(required)
            );
            assert!(
                output[required..].iter().all(|byte| *byte == 0xaa),
                "nothing is written past the message"
            );
            assert_eq!(output[..4], CONTINUATION_MARKER.to_le_bytes());
            let metadata_len = u32::from_le_bytes(output[4..8].try_into().unwrap()) as usize;
            let message = arrow_ipc::root_as_message(&output[8..8 + metadata_len]).unwrap();
            let batch = message
                .header_as_dictionary_batch()
                .expect("a DictionaryBatch message");
            assert_eq!(batch.id(), 3);
            assert!(batch.isDelta());
            assert_eq!(
                batch.data().unwrap().length(),
                i64::try_from(values.len()).unwrap()
            );

            assert_eq!(
                encode_dictionary_batch(&mut output[..required - 1], 3, true, values.as_slice()),
                Err(IpcError::BufferTooSmall { required })
            );
        }
    }

    //#endregion smoo/columine!n/vocab-buffer-budget
}
