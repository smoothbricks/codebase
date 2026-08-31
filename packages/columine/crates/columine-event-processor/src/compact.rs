//! CPB1 Compact batch decoder and validation.
//!
//! Wire integers and column values are little-endian. Validity and Boolean
//! bitmaps are Arrow LSB-first (`1 = valid` / `1 = true`). Offsets in a
//! descriptor are relative to the beginning of the CPB1 request.

use columine_arrow::{
    ArrowType, DynamicColumn, DynamicSchemaConfig, IpcError, MAX_EVENTS_PER_BATCH,
    MAX_SCHEMA_FIELDS, MAX_VALUE_BYTES, PlaneKind,
};
use columine_parsing::json_extractor::diagnostic_stage;

use crate::{ResultCode, ResultDiagnostic};

pub const COMPACT_BATCH_MAGIC: u32 = 0x3142_5043; // "CPB1"
pub const COMPACT_ABI_VERSION: u16 = 1;
pub const COMPACT_HEADER_SIZE: usize = 16;
pub const COMPACT_DESCRIPTOR_SIZE: usize = 32;
pub const COMPACT_DIAGNOSTIC_STAGE: u8 = diagnostic_stage::COMPACT;

pub use columine_parsing::json_extractor::compact_detail;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactValidationError {
    pub code: ResultCode,
    pub diagnostic: ResultDiagnostic,
}

impl CompactValidationError {
    fn invalid(detail: u8, field_index: usize, row_index: usize) -> Self {
        Self::new(
            ResultCode::InvalidInput,
            detail,
            field_index,
            row_index,
            0,
            0,
        )
    }

    fn schema(detail: u8, field_index: usize, expected: u8, actual: u8) -> Self {
        Self::new(
            ResultCode::SchemaMismatch,
            detail,
            field_index,
            0,
            expected,
            actual,
        )
    }

    fn new(
        code: ResultCode,
        detail: u8,
        field_index: usize,
        row_index: usize,
        expected_type: u8,
        actual_type: u8,
    ) -> Self {
        Self {
            code,
            diagnostic: ResultDiagnostic {
                stage: COMPACT_DIAGNOSTIC_STAGE,
                detail,
                expected_type,
                actual_type,
                field_index: u16::try_from(field_index).unwrap_or(u16::MAX),
                row_index: u16::try_from(row_index).unwrap_or(u16::MAX),
            },
        }
    }

    pub fn bad_request() -> Self {
        Self::invalid(compact_detail::BAD_DESCRIPTOR, usize::MAX, 0)
    }

    pub fn output_overlap() -> Self {
        Self::invalid(compact_detail::OUTPUT_OVERLAP, usize::MAX, 0)
    }
}

#[derive(Clone, Copy, Debug)]
struct Descriptor {
    tag: ArrowType,
    flags: u8,
    validity_offset: u32,
    validity_len: u32,
    offsets_offset: u32,
    offsets_len: u32,
    data_offset: u32,
    data_len: u32,
}

/// Validated, allocation-free view over one CPB1 request.
#[derive(Clone, Copy, Debug)]
pub struct CompactBatchView<'a> {
    bytes: &'a [u8],
    row_count: u32,
    column_count: usize,
}

impl<'a> CompactBatchView<'a> {
    pub fn parse(
        bytes: &'a [u8],
        schema: &DynamicSchemaConfig,
    ) -> Result<Self, CompactValidationError> {
        if bytes.len() < COMPACT_HEADER_SIZE
            || read_u32(bytes, 0) != Some(COMPACT_BATCH_MAGIC)
            || read_u16(bytes, 4) != Some(COMPACT_ABI_VERSION)
            || read_u16(bytes, 6) != Some(COMPACT_DESCRIPTOR_SIZE as u16)
        {
            return Err(CompactValidationError::invalid(
                compact_detail::BAD_HEADER,
                usize::MAX,
                0,
            ));
        }

        let row_count = read_u32(bytes, 8).ok_or_else(|| {
            CompactValidationError::invalid(compact_detail::BAD_HEADER, usize::MAX, 0)
        })?;
        if row_count > MAX_EVENTS_PER_BATCH {
            return Err(CompactValidationError::invalid(
                compact_detail::BAD_ROW_COUNT,
                usize::MAX,
                0,
            ));
        }
        let column_count_u32 = read_u32(bytes, 12).ok_or_else(|| {
            CompactValidationError::invalid(compact_detail::BAD_HEADER, usize::MAX, 0)
        })?;
        let column_count = usize::try_from(column_count_u32).map_err(|_| {
            CompactValidationError::invalid(compact_detail::BAD_HEADER, usize::MAX, 0)
        })?;
        if column_count > MAX_SCHEMA_FIELDS {
            return Err(CompactValidationError::invalid(
                compact_detail::BAD_HEADER,
                usize::MAX,
                0,
            ));
        }
        if column_count != schema.field_metadata.len() {
            return Err(CompactValidationError::schema(
                compact_detail::FIELD_COUNT_MISMATCH,
                usize::MAX,
                u8::try_from(schema.field_metadata.len()).unwrap_or(u8::MAX),
                u8::try_from(column_count).unwrap_or(u8::MAX),
            ));
        }
        let descriptors_len = column_count
            .checked_mul(COMPACT_DESCRIPTOR_SIZE)
            .ok_or_else(|| {
                CompactValidationError::invalid(compact_detail::BAD_HEADER, usize::MAX, 0)
            })?;
        let table_end = COMPACT_HEADER_SIZE
            .checked_add(descriptors_len)
            .ok_or_else(|| {
                CompactValidationError::invalid(compact_detail::BAD_HEADER, usize::MAX, 0)
            })?;
        if table_end > bytes.len() {
            return Err(CompactValidationError::invalid(
                compact_detail::BAD_DESCRIPTOR,
                usize::MAX,
                0,
            ));
        }

        let mut ranges = [(0usize, 0usize); MAX_SCHEMA_FIELDS * 3];
        let mut range_count = 0usize;
        for (field_index, field) in schema.field_metadata.iter().enumerate() {
            let descriptor = decode_descriptor(bytes, field_index)?;
            if descriptor.tag != field.arrow_type {
                return Err(CompactValidationError::schema(
                    compact_detail::TYPE_MISMATCH,
                    field_index,
                    field.arrow_type as u8,
                    descriptor.tag as u8,
                ));
            }
            let validity_present = descriptor.flags & 1 != 0;
            if descriptor.flags & !1 != 0 {
                return Err(CompactValidationError::invalid(
                    compact_detail::BAD_DESCRIPTOR,
                    field_index,
                    0,
                ));
            }
            if validity_present && !field.is_nullable() {
                return Err(CompactValidationError::schema(
                    compact_detail::NULLABILITY_MISMATCH,
                    field_index,
                    0,
                    1,
                ));
            }
            if field.arrow_type == ArrowType::Null && !field.is_nullable() {
                return Err(CompactValidationError::schema(
                    compact_detail::NULLABILITY_MISMATCH,
                    field_index,
                    1,
                    0,
                ));
            }

            let bitmap_len = usize::try_from(row_count).unwrap_or(usize::MAX).div_ceil(8);
            if validity_present {
                if descriptor.validity_len as usize != bitmap_len {
                    return Err(CompactValidationError::invalid(
                        compact_detail::BAD_VALIDITY,
                        field_index,
                        0,
                    ));
                }
                let validity = checked_range(
                    bytes,
                    descriptor.validity_offset,
                    descriptor.validity_len,
                    table_end,
                    &mut ranges,
                    &mut range_count,
                    field_index,
                )?;
                if !unused_bits_are_zero(validity, row_count) {
                    return Err(CompactValidationError::invalid(
                        compact_detail::BAD_VALIDITY,
                        field_index,
                        row_count.saturating_sub(1) as usize,
                    ));
                }
            } else if descriptor.validity_offset != 0 || descriptor.validity_len != 0 {
                return Err(CompactValidationError::invalid(
                    compact_detail::BAD_VALIDITY,
                    field_index,
                    0,
                ));
            }

            match field.plane_kind() {
                PlaneKind::Empty => {
                    if validity_present
                        || descriptor.offsets_offset != 0
                        || descriptor.offsets_len != 0
                        || descriptor.data_offset != 0
                        || descriptor.data_len != 0
                    {
                        return Err(CompactValidationError::invalid(
                            compact_detail::BAD_DESCRIPTOR,
                            field_index,
                            0,
                        ));
                    }
                }
                // Every fixed-width plane is `row_count * width` bytes, and
                // the width comes from the plane rather than from a special
                // case per type — which is what makes a one-byte plane cost
                // one byte per row instead of eight.
                PlaneKind::SignedInt { width }
                | PlaneKind::UnsignedInt { width }
                | PlaneKind::Float { width }
                | PlaneKind::FixedBytes { width } => {
                    require_no_offsets(descriptor, field_index)?;
                    let expected = usize::try_from(row_count)
                        .ok()
                        .and_then(|rows| rows.checked_mul(width as usize))
                        .ok_or_else(|| {
                            CompactValidationError::invalid(
                                compact_detail::BAD_FIXED_DATA,
                                field_index,
                                0,
                            )
                        })?;
                    require_data_length(descriptor, expected, field_index)?;
                    checked_range(
                        bytes,
                        descriptor.data_offset,
                        descriptor.data_len,
                        table_end,
                        &mut ranges,
                        &mut range_count,
                        field_index,
                    )?;
                }
                PlaneKind::Bool => {
                    require_no_offsets(descriptor, field_index)?;
                    require_data_length(descriptor, bitmap_len, field_index)?;
                    let data = checked_range(
                        bytes,
                        descriptor.data_offset,
                        descriptor.data_len,
                        table_end,
                        &mut ranges,
                        &mut range_count,
                        field_index,
                    )?;
                    if !unused_bits_are_zero(data, row_count) {
                        return Err(CompactValidationError::invalid(
                            compact_detail::BAD_FIXED_DATA,
                            field_index,
                            row_count.saturating_sub(1) as usize,
                        ));
                    }
                }
                PlaneKind::Text { offset_width } | PlaneKind::Bytes { offset_width } => {
                    let expected_offsets = usize::try_from(row_count)
                        .ok()
                        .and_then(|rows| rows.checked_add(1))
                        .and_then(|entries| entries.checked_mul(offset_width as usize))
                        .ok_or_else(|| {
                            CompactValidationError::invalid(
                                compact_detail::BAD_OFFSETS,
                                field_index,
                                0,
                            )
                        })?;
                    if descriptor.offsets_len as usize != expected_offsets {
                        return Err(CompactValidationError::invalid(
                            compact_detail::BAD_OFFSETS,
                            field_index,
                            0,
                        ));
                    }
                    if descriptor.data_len > MAX_VALUE_BYTES {
                        return Err(CompactValidationError::invalid(
                            compact_detail::BAD_OFFSETS,
                            field_index,
                            0,
                        ));
                    }
                    let offsets = checked_range(
                        bytes,
                        descriptor.offsets_offset,
                        descriptor.offsets_len,
                        table_end,
                        &mut ranges,
                        &mut range_count,
                        field_index,
                    )?;
                    let data = checked_range(
                        bytes,
                        descriptor.data_offset,
                        descriptor.data_len,
                        table_end,
                        &mut ranges,
                        &mut range_count,
                        field_index,
                    )?;
                    validate_variable(
                        offsets,
                        data,
                        validity(bytes, descriptor),
                        row_count,
                        offset_width,
                        matches!(field.plane_kind(), PlaneKind::Text { .. }),
                        field_index,
                    )?;
                }
            }
        }

        Ok(Self {
            bytes,
            row_count,
            column_count,
        })
    }

    pub fn row_count(self) -> u32 {
        self.row_count
    }

    pub fn column_count(self) -> usize {
        self.column_count
    }

    pub fn column(
        self,
        field_index: usize,
        schema: &DynamicSchemaConfig,
    ) -> Result<DynamicColumn<'a>, IpcError> {
        let descriptor =
            decode_descriptor(self.bytes, field_index).map_err(|_| IpcError::InvalidColumn)?;
        let field = *schema
            .field_metadata
            .get(field_index)
            .ok_or(IpcError::InvalidColumn)?;
        let field_index = u32::try_from(field_index).map_err(|_| IpcError::InvalidColumn)?;

        // The Null plane carries a field node and nothing else, so it takes
        // neither the validity bitmap nor any data even though it is nullable.
        if field.plane_kind() == PlaneKind::Empty {
            return Ok(DynamicColumn::fixed(field_index, field, None, &[]));
        }

        let validity = validity(self.bytes, descriptor);
        let data = buffer(self.bytes, descriptor.data_offset, descriptor.data_len);
        Ok(match field.plane_kind().offset_width() {
            Some(_) => DynamicColumn::variable(
                field_index,
                field,
                validity,
                buffer(
                    self.bytes,
                    descriptor.offsets_offset,
                    descriptor.offsets_len,
                ),
                data,
            ),
            None => DynamicColumn::fixed(field_index, field, validity, data),
        })
    }

    pub fn null_count(self, field_index: usize) -> i64 {
        let Ok(descriptor) = decode_descriptor(self.bytes, field_index) else {
            return 0;
        };
        if descriptor.tag == ArrowType::Null {
            return i64::from(self.row_count);
        }
        let Some(validity) = validity(self.bytes, descriptor) else {
            return 0;
        };
        let valid = validity.iter().map(|byte| byte.count_ones()).sum::<u32>();
        i64::from(self.row_count.saturating_sub(valid))
    }
}

fn decode_descriptor(
    bytes: &[u8],
    field_index: usize,
) -> Result<Descriptor, CompactValidationError> {
    let start = COMPACT_HEADER_SIZE
        .checked_add(
            field_index
                .checked_mul(COMPACT_DESCRIPTOR_SIZE)
                .ok_or_else(|| {
                    CompactValidationError::invalid(compact_detail::BAD_DESCRIPTOR, field_index, 0)
                })?,
        )
        .ok_or_else(|| {
            CompactValidationError::invalid(compact_detail::BAD_DESCRIPTOR, field_index, 0)
        })?;
    let raw = bytes
        .get(start..start + COMPACT_DESCRIPTOR_SIZE)
        .ok_or_else(|| {
            CompactValidationError::invalid(compact_detail::BAD_DESCRIPTOR, field_index, 0)
        })?;
    let tag = ArrowType::from_u8(raw[0]).ok_or_else(|| {
        CompactValidationError::invalid(compact_detail::BAD_DESCRIPTOR, field_index, 0)
    })?;
    if raw[2] != 0 || raw[3] != 0 || raw[28] != 0 || raw[29] != 0 || raw[30] != 0 || raw[31] != 0 {
        return Err(CompactValidationError::invalid(
            compact_detail::BAD_DESCRIPTOR,
            field_index,
            0,
        ));
    }
    Ok(Descriptor {
        tag,
        flags: raw[1],
        validity_offset: u32::from_le_bytes(raw[4..8].try_into().unwrap_or([0; 4])),
        validity_len: u32::from_le_bytes(raw[8..12].try_into().unwrap_or([0; 4])),
        offsets_offset: u32::from_le_bytes(raw[12..16].try_into().unwrap_or([0; 4])),
        offsets_len: u32::from_le_bytes(raw[16..20].try_into().unwrap_or([0; 4])),
        data_offset: u32::from_le_bytes(raw[20..24].try_into().unwrap_or([0; 4])),
        data_len: u32::from_le_bytes(raw[24..28].try_into().unwrap_or([0; 4])),
    })
}

fn checked_range<'a>(
    bytes: &'a [u8],
    offset: u32,
    length: u32,
    table_end: usize,
    ranges: &mut [(usize, usize); MAX_SCHEMA_FIELDS * 3],
    range_count: &mut usize,
    field_index: usize,
) -> Result<&'a [u8], CompactValidationError> {
    let offset = offset as usize;
    let length = length as usize;
    if length == 0 {
        if offset != 0 {
            return Err(CompactValidationError::invalid(
                compact_detail::BAD_DESCRIPTOR,
                field_index,
                0,
            ));
        }
        return Ok(&[]);
    }
    let end = offset.checked_add(length).ok_or_else(|| {
        CompactValidationError::invalid(compact_detail::BAD_DESCRIPTOR, field_index, 0)
    })?;
    if offset < table_end || !offset.is_multiple_of(8) || end > bytes.len() {
        return Err(CompactValidationError::invalid(
            compact_detail::BAD_DESCRIPTOR,
            field_index,
            0,
        ));
    }
    if ranges[..*range_count]
        .iter()
        .any(|(other_start, other_end)| offset < *other_end && *other_start < end)
    {
        return Err(CompactValidationError::invalid(
            compact_detail::BAD_DESCRIPTOR,
            field_index,
            0,
        ));
    }
    ranges[*range_count] = (offset, end);
    *range_count += 1;
    bytes.get(offset..end).ok_or_else(|| {
        CompactValidationError::invalid(compact_detail::BAD_DESCRIPTOR, field_index, 0)
    })
}

fn require_no_offsets(
    descriptor: Descriptor,
    field_index: usize,
) -> Result<(), CompactValidationError> {
    if descriptor.offsets_offset != 0 || descriptor.offsets_len != 0 {
        return Err(CompactValidationError::invalid(
            compact_detail::BAD_FIXED_DATA,
            field_index,
            0,
        ));
    }
    Ok(())
}

fn require_data_length(
    descriptor: Descriptor,
    expected: usize,
    field_index: usize,
) -> Result<(), CompactValidationError> {
    if descriptor.data_len as usize != expected
        || (expected == 0 && descriptor.data_offset != 0)
        || (expected != 0 && descriptor.data_offset == 0)
    {
        return Err(CompactValidationError::invalid(
            compact_detail::BAD_FIXED_DATA,
            field_index,
            0,
        ));
    }
    Ok(())
}

fn validate_variable(
    offsets: &[u8],
    data: &[u8],
    validity: Option<&[u8]>,
    row_count: u32,
    offset_width: u32,
    utf8: bool,
    field_index: usize,
) -> Result<(), CompactValidationError> {
    if offset_at(offsets, 0, offset_width) != Some(0) {
        return Err(CompactValidationError::invalid(
            compact_detail::BAD_OFFSETS,
            field_index,
            0,
        ));
    }
    let mut previous = 0usize;
    for row in 0..row_count as usize {
        let next = offset_at(offsets, row + 1, offset_width).ok_or_else(|| {
            CompactValidationError::invalid(compact_detail::BAD_OFFSETS, field_index, row)
        })? as usize;
        if next < previous || next > data.len() {
            return Err(CompactValidationError::invalid(
                compact_detail::BAD_OFFSETS,
                field_index,
                row,
            ));
        }
        let is_valid = validity
            .map(|bits| bits[row / 8] & (1 << (row % 8)) != 0)
            .unwrap_or(true);
        if !is_valid && next != previous {
            return Err(CompactValidationError::invalid(
                compact_detail::BAD_OFFSETS,
                field_index,
                row,
            ));
        }
        if utf8 && is_valid && std::str::from_utf8(&data[previous..next]).is_err() {
            return Err(CompactValidationError::invalid(
                compact_detail::BAD_UTF8,
                field_index,
                row,
            ));
        }
        previous = next;
    }
    if previous != data.len() {
        return Err(CompactValidationError::invalid(
            compact_detail::BAD_OFFSETS,
            field_index,
            row_count as usize,
        ));
    }
    Ok(())
}

fn unused_bits_are_zero(bitmap: &[u8], row_count: u32) -> bool {
    let remainder = row_count % 8;
    if remainder == 0 || bitmap.is_empty() {
        return true;
    }
    let used_mask = (1u16 << remainder) as u8 - 1;
    bitmap.last().is_none_or(|last| last & !used_mask == 0)
}

fn validity(bytes: &[u8], descriptor: Descriptor) -> Option<&[u8]> {
    (descriptor.flags & 1 != 0)
        .then(|| buffer(bytes, descriptor.validity_offset, descriptor.validity_len))
}

fn buffer(bytes: &[u8], offset: u32, length: u32) -> &[u8] {
    if length == 0 {
        return &[];
    }
    let start = offset as usize;
    let Some(end) = start.checked_add(length as usize) else {
        return &[];
    };
    bytes.get(start..end).unwrap_or(&[])
}

/// Read offset entry `index` from a buffer of `width`-byte little-endian
/// entries.
///
/// The large-offset planes declare 64-bit offsets, but retained data is capped
/// at `MAX_VALUE_BYTES`, so a value that needs the high four bytes is out of
/// range by definition and is rejected here rather than truncated.
fn offset_at(offsets: &[u8], index: usize, width: u32) -> Option<u32> {
    let start = index.checked_mul(width as usize)?;
    let cell = offsets.get(start..start + width as usize)?;
    let (low, high) = cell.split_at(4);
    if high.iter().any(|byte| *byte != 0) {
        return None;
    }
    Some(u32::from_le_bytes(low.try_into().ok()?))
}

fn read_u16(bytes: &[u8], offset: usize) -> Option<u16> {
    Some(u16::from_le_bytes(
        bytes.get(offset..offset + 2)?.try_into().ok()?,
    ))
}

fn read_u32(bytes: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        bytes.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use columine_arrow::SignalSchemaField;

    fn schema(nullable: bool) -> DynamicSchemaConfig {
        DynamicSchemaConfig::from_physical_fields(&[SignalSchemaField::new(
            ArrowType::Utf8,
            nullable,
        )])
        .unwrap()
    }

    fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
        bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
    }

    fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
        bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    fn nullable_utf8_batch() -> Vec<u8> {
        let mut batch = vec![0u8; 74];
        put_u32(&mut batch, 0, COMPACT_BATCH_MAGIC);
        put_u16(&mut batch, 4, COMPACT_ABI_VERSION);
        put_u16(&mut batch, 6, COMPACT_DESCRIPTOR_SIZE as u16);
        put_u32(&mut batch, 8, 2);
        put_u32(&mut batch, 12, 1);

        batch[16] = ArrowType::Utf8 as u8;
        batch[17] = 1;
        put_u32(&mut batch, 20, 48);
        put_u32(&mut batch, 24, 1);
        put_u32(&mut batch, 28, 56);
        put_u32(&mut batch, 32, 12);
        put_u32(&mut batch, 36, 72);
        put_u32(&mut batch, 40, 2);

        batch[48] = 0b0000_0001;
        put_u32(&mut batch, 56, 0);
        put_u32(&mut batch, 60, 2);
        put_u32(&mut batch, 64, 2);
        batch[72..74].copy_from_slice(b"ok");
        batch
    }

    fn assert_error(batch: &[u8], schema: &DynamicSchemaConfig, code: ResultCode, detail: u8) {
        let error = CompactBatchView::parse(batch, schema).unwrap_err();
        assert_eq!(error.code, code);
        assert_eq!(error.diagnostic.stage, COMPACT_DIAGNOSTIC_STAGE);
        assert_eq!(error.diagnostic.detail, detail);
    }

    #[test]
    fn valid_nullable_utf8_is_borrowed_without_repacking() {
        let schema = schema(true);
        let batch = nullable_utf8_batch();
        let view = CompactBatchView::parse(&batch, &schema).unwrap();
        assert_eq!(view.row_count(), 2);
        assert_eq!(view.column_count(), 1);
        assert_eq!(view.null_count(0), 1);

        let column = view.column(0, &schema).unwrap();
        assert_eq!(column.validity, Some(&batch[48..49]));
        assert_eq!(column.offsets, Some(&batch[56..68]));
        assert_eq!(column.data, &batch[72..74]);
    }

    #[test]
    fn zero_row_nullable_column_accepts_present_empty_validity() {
        let schema = schema(true);
        let mut batch = vec![0u8; 52];
        put_u32(&mut batch, 0, COMPACT_BATCH_MAGIC);
        put_u16(&mut batch, 4, COMPACT_ABI_VERSION);
        put_u16(&mut batch, 6, COMPACT_DESCRIPTOR_SIZE as u16);
        put_u32(&mut batch, 8, 0);
        put_u32(&mut batch, 12, 1);
        batch[16] = ArrowType::Utf8 as u8;
        batch[17] = 1;
        // Exact empty validity is canonical as flag=1, offset=0, length=0.
        put_u32(&mut batch, 28, 48);
        put_u32(&mut batch, 32, 4);

        let view = CompactBatchView::parse(&batch, &schema).unwrap();
        let column = view.column(0, &schema).unwrap();
        assert_eq!(column.validity, Some(&[][..]));
        assert_eq!(column.offsets, Some(&batch[48..52]));
        assert!(column.data.is_empty());
        assert_eq!(view.null_count(0), 0);
    }

    #[test]
    fn validation_rejects_schema_and_physical_ambiguity() {
        let nullable = schema(true);

        let mut batch = nullable_utf8_batch();
        batch[16] = ArrowType::Binary as u8;
        assert_error(
            &batch,
            &nullable,
            ResultCode::SchemaMismatch,
            compact_detail::TYPE_MISMATCH,
        );

        let batch = nullable_utf8_batch();
        assert_error(
            &batch,
            &schema(false),
            ResultCode::SchemaMismatch,
            compact_detail::NULLABILITY_MISMATCH,
        );

        let mut batch = nullable_utf8_batch();
        put_u32(&mut batch, 36, 56);
        assert_error(
            &batch,
            &nullable,
            ResultCode::InvalidInput,
            compact_detail::BAD_DESCRIPTOR,
        );
    }

    #[test]
    fn validation_rejects_noncanonical_rows_and_values() {
        let schema = schema(true);

        let mut batch = nullable_utf8_batch();
        batch[48] |= 0x80;
        assert_error(
            &batch,
            &schema,
            ResultCode::InvalidInput,
            compact_detail::BAD_VALIDITY,
        );

        let mut batch = nullable_utf8_batch();
        put_u32(&mut batch, 60, 1);
        assert_error(
            &batch,
            &schema,
            ResultCode::InvalidInput,
            compact_detail::BAD_OFFSETS,
        );

        let mut batch = nullable_utf8_batch();
        batch[72] = 0xff;
        assert_error(
            &batch,
            &schema,
            ResultCode::InvalidInput,
            compact_detail::BAD_UTF8,
        );

        let mut batch = nullable_utf8_batch();
        put_u32(&mut batch, 8, MAX_EVENTS_PER_BATCH + 1);
        assert_error(
            &batch,
            &schema,
            ResultCode::InvalidInput,
            compact_detail::BAD_ROW_COUNT,
        );
    }
}
