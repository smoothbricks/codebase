//! Two-pass span-tree → single `RecordBatch` conversion.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use arrow_array::builder::BooleanBufferBuilder;
use arrow_array::{
    ArrayRef, DictionaryArray, Int64Array, RecordBatch, StringArray, UInt8Array, UInt32Array,
    UInt64Array,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{ArrowError, DataType, Field, Schema};

use crate::dict::{
    ColumnDictionary, FirstSeenDictionary, StableVocabularyCatalog, StableVocabularyKind,
    StableVocabularyLookupError, vocabulary_dictionary,
};
use crate::source::{SpanSource, walk_pre_order};

pub const ENTRY_TYPE_NAMES: [&str; 24] = [
    "span-start",
    "span-ok",
    "span-err",
    "span-exception",
    "span-retry",
    "trace",
    "debug",
    "info",
    "warn",
    "error",
    "ff-access",
    "ff-usage",
    "period-start",
    "op-invocations",
    "op-errors",
    "op-exceptions",
    "op-duration-total",
    "op-duration-ok",
    "op-duration-err",
    "op-duration-min",
    "op-duration-max",
    "buffer-writes",
    "buffer-spans",
    "buffer-capacity",
];

fn dict_type(key: DataType) -> DataType {
    DataType::Dictionary(Box::new(key), Box::new(DataType::Utf8))
}

pub fn trace_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("trace_id", dict_type(DataType::UInt32), false),
        Field::new("thread_id", DataType::UInt64, false),
        Field::new("span_id", DataType::UInt32, false),
        Field::new("parent_thread_id", DataType::UInt64, true),
        Field::new("parent_span_id", DataType::UInt32, true),
        Field::new("entry_type", dict_type(DataType::UInt8), false),
        Field::new("message", dict_type(DataType::UInt32), true),
        Field::new("line_number", DataType::UInt32, false),
    ]))
}

#[derive(Debug)]
pub enum ConvertError {
    Arrow(ArrowError),
    RowCountOverflow,
    InvalidEntryType {
        row: usize,
        entry_type: u8,
    },
    InvalidVocabularyId {
        row: usize,
        id: u32,
    },
    VocabularyKindMismatch {
        row: usize,
        id: u32,
        expected: StableVocabularyKind,
        actual: StableVocabularyKind,
    },
    MissingDynamicMessage {
        row: usize,
        entry_type: u8,
    },
}

impl fmt::Display for ConvertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Arrow(error) => error.fmt(f),
            Self::RowCountOverflow => f.write_str("Arrow row count exceeds usize"),
            Self::InvalidEntryType { row, entry_type } => {
                write!(f, "invalid packed entry type {entry_type} at row {row}")
            }
            Self::InvalidVocabularyId { row, id } => {
                write!(f, "unknown static vocabulary id {id} at row {row}")
            }
            Self::VocabularyKindMismatch {
                row,
                id,
                expected,
                actual,
            } => write!(
                f,
                "static vocabulary id {id} has kind tag {}, expected {} at row {row}",
                *actual as u8, *expected as u8,
            ),
            Self::MissingDynamicMessage { row, entry_type } => write!(
                f,
                "dynamic entry type {entry_type} is missing its message at row {row}",
            ),
        }
    }
}

impl Error for ConvertError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Arrow(error) => Some(error),
            _ => None,
        }
    }
}

impl From<ArrowError> for ConvertError {
    fn from(value: ArrowError) -> Self {
        Self::Arrow(value)
    }
}

#[inline]
fn required_vocabulary_kind(entry_type: u8) -> Option<StableVocabularyKind> {
    match entry_type {
        1 => Some(StableVocabularyKind::SpanName),
        6..=10 => Some(StableVocabularyKind::LogTemplate),
        _ => None,
    }
}

#[inline]
fn split_packed_header(header: u32) -> (u8, u32) {
    (header as u8, header >> 8)
}

pub fn convert_span_trees<S: SpanSource>(
    roots: &[S],
    vocabulary: &StableVocabularyCatalog<'_>,
) -> Result<RecordBatch, ConvertError> {
    let mut total_rows = 0usize;
    let mut trace_dict = ColumnDictionary::default();
    let mut dynamic_messages = FirstSeenDictionary::default();
    let mut failure = None;
    let mut absolute_row = 0usize;
    walk_pre_order(roots, &mut |buffer: &S| {
        let rows = buffer.row_count();
        total_rows = match total_rows.checked_add(rows) {
            Some(total) => total,
            None => {
                failure.get_or_insert(ConvertError::RowCountOverflow);
                total_rows
            }
        };
        trace_dict.observe(buffer.identity().trace_id.as_str());
        for row in 0..rows {
            let (entry_type, vocabulary_id) = split_packed_header(buffer.packed_header(row));
            if entry_type == 0 || entry_type as usize > ENTRY_TYPE_NAMES.len() {
                failure.get_or_insert(ConvertError::InvalidEntryType {
                    row: absolute_row,
                    entry_type,
                });
            } else if vocabulary_id != 0 {
                match required_vocabulary_kind(entry_type) {
                    None => {
                        failure.get_or_insert(ConvertError::InvalidVocabularyId {
                            row: absolute_row,
                            id: vocabulary_id,
                        });
                    }
                    Some(kind) => match vocabulary.key_for_id(vocabulary_id, kind) {
                        Ok(_) => {}
                        Err(StableVocabularyLookupError::UnknownId(id)) => {
                            failure.get_or_insert(ConvertError::InvalidVocabularyId {
                                row: absolute_row,
                                id,
                            });
                        }
                        Err(StableVocabularyLookupError::KindMismatch {
                            id,
                            expected,
                            actual,
                        }) => {
                            failure.get_or_insert(ConvertError::VocabularyKindMismatch {
                                row: absolute_row,
                                id,
                                expected,
                                actual,
                            });
                        }
                    },
                }
            } else if let Some(message) = buffer.dynamic_message(row) {
                if vocabulary.key_for_value(message).is_none() {
                    dynamic_messages.observe(message);
                }
            } else if required_vocabulary_kind(entry_type).is_some() {
                failure.get_or_insert(ConvertError::MissingDynamicMessage {
                    row: absolute_row,
                    entry_type,
                });
            }
            absolute_row += 1;
        }
    });
    if let Some(error) = failure {
        return Err(error);
    }

    let trace_dict = trace_dict.finalize_indexed();

    let mut timestamps = Vec::with_capacity(total_rows);
    let mut trace_keys = Vec::with_capacity(total_rows);
    let mut thread_ids = Vec::with_capacity(total_rows);
    let mut span_ids = Vec::with_capacity(total_rows);
    let mut parent_thread_ids = Vec::with_capacity(total_rows);
    let mut parent_span_ids = Vec::with_capacity(total_rows);
    let mut parent_valid = BooleanBufferBuilder::new(total_rows);
    let mut entry_keys = Vec::with_capacity(total_rows);
    let mut message_keys = Vec::with_capacity(total_rows);
    let mut message_valid = BooleanBufferBuilder::new(total_rows);
    let mut line_numbers = Vec::with_capacity(total_rows);

    walk_pre_order(roots, &mut |buffer: &S| {
        let identity = buffer.identity();
        let trace_key = trace_dict
            .index_of(identity.trace_id.as_str())
            .expect("trace ID observed in pass 1");
        let (parent_thread, parent_span, has_parent) = match &identity.parent {
            Some(parent) => (parent.thread_id, parent.span_id, true),
            None => (0, 0, false),
        };
        for row in 0..buffer.row_count() {
            timestamps.push(buffer.timestamp(row));
            trace_keys.push(trace_key);
            thread_ids.push(identity.thread_id);
            span_ids.push(identity.span_id);
            parent_thread_ids.push(parent_thread);
            parent_span_ids.push(parent_span);
            parent_valid.append(has_parent);
            let (entry_type, vocabulary_id) = split_packed_header(buffer.packed_header(row));
            entry_keys.push(entry_type - 1);
            if vocabulary_id != 0 {
                message_keys.push(
                    vocabulary
                        .key_for_id(
                            vocabulary_id,
                            required_vocabulary_kind(entry_type)
                                .expect("validated static row kind in pass 1"),
                        )
                        .expect("validated static vocabulary ID in pass 1"),
                );
                message_valid.append(true);
            } else if let Some(message) = buffer.dynamic_message(row) {
                message_keys.push(match vocabulary.key_for_value(message) {
                    Some(key) => key,
                    None => {
                        vocabulary.len() as u32
                            + dynamic_messages
                                .index_of(message)
                                .expect("dynamic message observed in pass 1")
                    }
                });
                message_valid.append(true);
            } else {
                message_keys.push(0);
                message_valid.append(false);
            }
            line_numbers.push(buffer.line_number(row));
        }
    });

    let parent_nulls = NullBuffer::new(parent_valid.finish());
    let message_nulls = NullBuffer::new(message_valid.finish());
    let entry_col = DictionaryArray::try_new(
        UInt8Array::from(entry_keys),
        Arc::new(StringArray::from_iter_values(ENTRY_TYPE_NAMES)) as ArrayRef,
    )?;
    let trace_col = DictionaryArray::try_new(
        UInt32Array::from(trace_keys),
        Arc::new(StringArray::from_iter_values(trace_dict.values.iter())) as ArrayRef,
    )?;
    let message_values: ArrayRef = vocabulary_dictionary(vocabulary, &dynamic_messages.values)?;
    let message_col = DictionaryArray::try_new(
        UInt32Array::new(message_keys.into(), Some(message_nulls)),
        message_values,
    )?;

    Ok(RecordBatch::try_new(
        trace_schema(),
        vec![
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(trace_col),
            Arc::new(UInt64Array::from(thread_ids)),
            Arc::new(UInt32Array::from(span_ids)),
            Arc::new(UInt64Array::new(
                parent_thread_ids.into(),
                Some(parent_nulls.clone()),
            )),
            Arc::new(UInt32Array::new(parent_span_ids.into(), Some(parent_nulls))),
            Arc::new(entry_col),
            Arc::new(message_col),
            Arc::new(UInt32Array::from(line_numbers)),
        ],
    )?)
}
