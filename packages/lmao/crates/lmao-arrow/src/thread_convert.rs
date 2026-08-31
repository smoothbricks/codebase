//! Conversion of the shared per-thread buffer.
//!
//! The first twelve fields are exactly [`crate::trace_schema`]'s system prefix;
//! schema attributes are appended in their generated ordinal order. Scope is
//! materialized immediately before conversion, so the live buffer never carries
//! eager scope-prefill state.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::builder::BooleanBufferBuilder;
use arrow_array::{
    ArrayRef, BooleanArray, DictionaryArray, Float64Array, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field, Schema};
use lmao_core::{
    ColumnValueRef, EntryType, FieldMeta, FieldStrategy, FlushWindow, SYSTEM_COLUMN_COUNT,
    ThreadSpanBuffer, entry_type_from_header, vocabulary_id_from_header,
};

use crate::convert::{ConvertError, ENTRY_TYPE_NAMES, trace_schema};
use crate::dict::{
    StableVocabularyCatalog, StableVocabularyKind, StableVocabularyLookupError,
    vocabulary_dictionary,
};

fn required_vocabulary_kind(entry_type: EntryType) -> Option<StableVocabularyKind> {
    match entry_type {
        EntryType::SpanStart => Some(StableVocabularyKind::SpanName),
        EntryType::Trace
        | EntryType::Debug
        | EntryType::Info
        | EntryType::Warn
        | EntryType::Error => Some(StableVocabularyKind::LogTemplate),
        _ => None,
    }
}

fn string_dictionary(values: &[Option<&str>]) -> (Vec<u32>, NullBuffer, StringArray) {
    let mut dictionary = BTreeMap::<&str, u32>::new();
    for value in values.iter().flatten() {
        let next = u32::try_from(dictionary.len()).expect("attribute dictionary exceeds u32");
        dictionary.entry(value).or_insert(next);
    }
    let mut keys = Vec::with_capacity(values.len());
    let mut valid = BooleanBufferBuilder::new(values.len());
    for value in values {
        match value {
            Some(value) => {
                keys.push(
                    *dictionary
                        .get(value)
                        .expect("dictionary pass observed value"),
                );
                valid.append(true);
            }
            None => {
                keys.push(0);
                valid.append(false);
            }
        }
    }
    let strings = StringArray::from_iter_values(dictionary.keys().copied());
    (keys, NullBuffer::new(valid.finish()), strings)
}

/// Convert a prepared window from one [`ThreadSpanBuffer`] into the system ∪
/// schema-attribute Arrow batch.
pub fn convert_thread_span_buffer(
    buffer: &mut ThreadSpanBuffer,
    vocabulary: &StableVocabularyCatalog<'_>,
    window: FlushWindow,
) -> Result<RecordBatch, ConvertError> {
    let end = window
        .start_row
        .checked_add(window.row_count)
        .ok_or(ConvertError::RowCountOverflow)?;
    if end > buffer.row_count() {
        return Err(ConvertError::MissingSourceValue {
            row: end,
            column: "thread-buffer window",
        });
    }
    buffer
        .materialize_scope_window(window.start_row, window.row_count)
        .map_err(|_| ConvertError::MissingSourceValue {
            row: window.start_row,
            column: "scope",
        })?;

    let rows: Vec<usize> = (window.start_row..end).collect();
    let total_rows = rows.len();
    let mut trace_values = Vec::with_capacity(total_rows);
    let mut thread_ids = Vec::with_capacity(total_rows);
    let mut span_ids = Vec::with_capacity(total_rows);
    let mut parent_thread_ids = Vec::with_capacity(total_rows);
    let mut parent_span_ids = Vec::with_capacity(total_rows);
    let mut parent_valid = BooleanBufferBuilder::new(total_rows);
    let mut timestamps = Vec::with_capacity(total_rows);
    let mut entry_keys = Vec::with_capacity(total_rows);
    let mut lines = Vec::with_capacity(total_rows);
    let mut message_values = Vec::with_capacity(total_rows);
    let mut attrs: Vec<Vec<Option<ColumnValueRef<'_>>>> = buffer
        .schema_fields()
        .iter()
        .map(|_| Vec::with_capacity(total_rows))
        .collect();

    for row in rows {
        let header = buffer
            .packed_header_at(row)
            .ok_or(ConvertError::MissingSourceValue {
                row,
                column: "packed_header",
            })?;
        let entry_type = entry_type_from_header(header).ok_or(ConvertError::InvalidEntryType {
            row,
            entry_type: header as u8,
        })?;
        let span_id = buffer
            .span_id_at(row)
            .ok_or(ConvertError::MissingSourceValue {
                row,
                column: "span_id",
            })?;
        let timestamp =
            if buffer.completion_row(span_id) == Some(row) && buffer.is_span_open(span_id) {
                window.timestamp
            } else {
                buffer
                    .timestamp_at(row)
                    .ok_or(ConvertError::MissingSourceValue {
                        row,
                        column: "timestamp",
                    })?
            };
        let vocabulary_id = vocabulary_id_from_header(header);
        let message = if let Some(id) = vocabulary_id {
            let kind = required_vocabulary_kind(entry_type)
                .ok_or(ConvertError::InvalidVocabularyId { row, id: id.get() })?;
            let key = vocabulary
                .key_for_id(id.get(), kind)
                .map_err(|error| match error {
                    StableVocabularyLookupError::UnknownId(id) => {
                        ConvertError::InvalidVocabularyId { row, id }
                    }
                    StableVocabularyLookupError::KindMismatch {
                        id,
                        expected,
                        actual,
                    } => ConvertError::VocabularyKindMismatch {
                        row,
                        id,
                        expected,
                        actual,
                    },
                })?;
            Some(
                vocabulary
                    .entries()
                    .get(key as usize)
                    .map(|entry| entry.value)
                    .ok_or(ConvertError::InvalidVocabularyId { row, id: id.get() })?,
            )
        } else {
            buffer.dynamic_message_at(row)
        };

        let parent_span_id =
            buffer
                .parent_span_id_at(row)
                .ok_or(ConvertError::MissingSourceValue {
                    row,
                    column: "parent_span_id",
                })?;
        parent_valid.append(parent_span_id != 0);
        timestamps.push(timestamp);
        trace_values.push(
            buffer
                .trace_id_at(row)
                .ok_or(ConvertError::MissingSourceValue {
                    row,
                    column: "trace_id",
                })?,
        );
        thread_ids.push(buffer.thread_id());
        span_ids.push(span_id);
        parent_thread_ids.push(buffer.parent_thread_id_at(row).unwrap_or(0));
        parent_span_ids.push(parent_span_id);
        entry_keys.push(entry_type.as_u8());
        lines.push(buffer.line_at(row).unwrap_or(0));
        message_values.push(message);
        for (index, values) in attrs.iter_mut().enumerate() {
            values.push(buffer.attribute_at(
                row,
                u16::try_from(SYSTEM_COLUMN_COUNT + index).expect("schema ordinal exceeds u16"),
            ));
        }
    }

    let mut trace_dictionary = BTreeMap::<&str, u32>::new();
    for value in &trace_values {
        let next =
            u32::try_from(trace_dictionary.len()).map_err(|_| ConvertError::DictionaryOverflow)?;
        trace_dictionary.entry(value).or_insert(next);
    }
    let trace_keys = trace_values
        .iter()
        .map(|value| {
            *trace_dictionary
                .get(value)
                .expect("trace dictionary observed value")
        })
        .collect::<Vec<_>>();
    let trace_col = DictionaryArray::try_new(
        UInt32Array::from(trace_keys),
        Arc::new(StringArray::from_iter_values(
            trace_dictionary.keys().copied(),
        )) as ArrayRef,
    )?;

    let mut dynamic_messages = BTreeMap::<&str, u32>::new();
    for value in message_values.iter().flatten() {
        let next =
            u32::try_from(dynamic_messages.len()).map_err(|_| ConvertError::DictionaryOverflow)?;
        dynamic_messages.entry(value).or_insert(next);
    }
    // Keep the catalog's stable prefix and append thread-local dynamic strings.
    let dynamic_values = dynamic_messages.keys().copied().collect::<Vec<_>>();
    let message_values_array = vocabulary_dictionary(vocabulary, &dynamic_values)?;
    let stable_count =
        u32::try_from(vocabulary.len()).map_err(|_| ConvertError::DictionaryOverflow)?;
    let mut message_keys = Vec::with_capacity(total_rows);
    let mut message_valid = BooleanBufferBuilder::new(total_rows);
    for value in &message_values {
        match value {
            Some(value) => {
                let key = vocabulary.key_for_value(value).unwrap_or_else(|| {
                    stable_count
                        + u32::try_from(
                            dynamic_values
                                .binary_search_by(|candidate| {
                                    candidate.as_bytes().cmp(value.as_bytes())
                                })
                                .expect("dynamic message observed"),
                        )
                        .expect("dynamic dictionary exceeds u32")
                });
                message_keys.push(key);
                message_valid.append(true);
            }
            None => {
                message_keys.push(0);
                message_valid.append(false);
            }
        }
    }
    let message_col = DictionaryArray::try_new(
        UInt32Array::new(
            message_keys.into(),
            Some(NullBuffer::new(message_valid.finish())),
        ),
        message_values_array,
    )?;
    let parent_nulls = NullBuffer::new(parent_valid.finish());
    let entry_col = DictionaryArray::try_new(
        UInt8Array::from(entry_keys),
        Arc::new(StringArray::from_iter_values(ENTRY_TYPE_NAMES)) as ArrayRef,
    )?;

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(TimestampNanosecondArray::from(timestamps)),
        Arc::new(trace_col),
        Arc::new(UInt64Array::from(thread_ids)),
        Arc::new(UInt32Array::from(span_ids)),
        Arc::new(UInt64Array::new(
            parent_thread_ids.into(),
            Some(parent_nulls.clone()),
        )),
        Arc::new(UInt32Array::new(parent_span_ids.into(), Some(parent_nulls))),
        Arc::new(entry_col),
        null_dictionary(total_rows),
        null_dictionary(total_rows),
        null_dictionary(total_rows),
        Arc::new(message_col),
        Arc::new(UInt32Array::from(lines)),
    ];
    let mut fields = trace_schema().fields().to_vec();
    for (meta, values) in buffer.schema_fields().iter().zip(attrs) {
        let (field, column) = convert_attribute(meta, values)?;
        fields.push(Arc::new(field));
        columns.push(column);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(ConvertError::Arrow)
}

/// Convenience wrapper that prepares a window and converts it.
pub fn convert_thread_buffer(
    buffer: &mut ThreadSpanBuffer,
    vocabulary: &StableVocabularyCatalog<'_>,
    start_row: usize,
    row_count: usize,
    timestamp: i64,
) -> Result<RecordBatch, ConvertError> {
    let window = buffer
        .flush_window(start_row, row_count, timestamp)
        .map_err(|_| ConvertError::MissingSourceValue {
            row: start_row,
            column: "thread-buffer window",
        })?;
    convert_thread_span_buffer(buffer, vocabulary, window)
}

fn null_dictionary(rows: usize) -> ArrayRef {
    let mut valid = BooleanBufferBuilder::new(rows);
    for _ in 0..rows {
        valid.append(false);
    }
    Arc::new(
        DictionaryArray::try_new(
            UInt32Array::new(vec![0; rows].into(), Some(NullBuffer::new(valid.finish()))),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        )
        .expect("empty nullable dictionary is valid"),
    )
}

fn convert_attribute(
    meta: &FieldMeta,
    values: Vec<Option<ColumnValueRef<'_>>>,
) -> Result<(Field, ArrayRef), ConvertError> {
    match meta.strategy {
        FieldStrategy::Number => {
            let values = values
                .into_iter()
                .map(|value| match value {
                    Some(ColumnValueRef::Number(value)) => Some(value),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let (data, valid) = optional_f64(&values);
            Ok((
                Field::new(meta.name, DataType::Float64, true),
                Arc::new(Float64Array::new(data.into(), Some(valid))) as ArrayRef,
            ))
        }
        FieldStrategy::Uint64 => {
            let values = values
                .into_iter()
                .map(|value| match value {
                    Some(ColumnValueRef::Uint64(value)) => Some(value),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let (data, valid) = optional_u64(&values);
            Ok((
                Field::new(meta.name, DataType::UInt64, true),
                Arc::new(UInt64Array::new(data.into(), Some(valid))) as ArrayRef,
            ))
        }
        FieldStrategy::Boolean => {
            let values = values
                .into_iter()
                .map(|value| match value {
                    Some(ColumnValueRef::Boolean(value)) => Some(value),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let mut data = Vec::with_capacity(values.len());
            let mut valid = BooleanBufferBuilder::new(values.len());
            for value in values {
                data.push(value.unwrap_or(false));
                valid.append(value.is_some());
            }
            Ok((
                Field::new(meta.name, DataType::Boolean, true),
                Arc::new(BooleanArray::new(
                    data.into(),
                    Some(NullBuffer::new(valid.finish())),
                )) as ArrayRef,
            ))
        }
        FieldStrategy::Category | FieldStrategy::Text => {
            let values = values
                .iter()
                .map(|value| match value {
                    Some(ColumnValueRef::Text(value)) => Some(*value),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let (keys, valid, dictionary) = string_dictionary(&values);
            let column = DictionaryArray::try_new(
                UInt32Array::new(keys.into(), Some(valid)),
                Arc::new(dictionary) as ArrayRef,
            )?;
            Ok((
                Field::new(
                    meta.name,
                    DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                    true,
                ),
                Arc::new(column) as ArrayRef,
            ))
        }
        FieldStrategy::Enum(variants) => {
            let mut data = Vec::with_capacity(values.len());
            let mut valid = BooleanBufferBuilder::new(values.len());
            for value in values {
                match value {
                    Some(ColumnValueRef::Enum(value)) if usize::from(value) < variants.len() => {
                        data.push(value);
                        valid.append(true);
                    }
                    _ => {
                        data.push(0);
                        valid.append(false);
                    }
                }
            }
            let column = DictionaryArray::try_new(
                UInt16Array::new(data.into(), Some(NullBuffer::new(valid.finish()))),
                Arc::new(StringArray::from_iter_values(variants.iter().copied())) as ArrayRef,
            )?;
            Ok((
                Field::new(
                    meta.name,
                    DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
                    true,
                ),
                Arc::new(column) as ArrayRef,
            ))
        }
    }
}

fn optional_f64(values: &[Option<f64>]) -> (Vec<f64>, NullBuffer) {
    let mut data = Vec::with_capacity(values.len());
    let mut valid = BooleanBufferBuilder::new(values.len());
    for value in values {
        data.push(value.unwrap_or_default());
        valid.append(value.is_some());
    }
    (data, NullBuffer::new(valid.finish()))
}
fn optional_u64(values: &[Option<u64>]) -> (Vec<u64>, NullBuffer) {
    let mut data = Vec::with_capacity(values.len());
    let mut valid = BooleanBufferBuilder::new(values.len());
    for value in values {
        data.push(value.unwrap_or_default());
        valid.append(value.is_some());
    }
    (data, NullBuffer::new(valid.finish()))
}
