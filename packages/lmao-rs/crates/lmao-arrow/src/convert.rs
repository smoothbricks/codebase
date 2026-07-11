//! Two-pass span-tree → single `RecordBatch` conversion (`01k`, `01f`).

use std::sync::Arc;

use arrow_array::builder::BooleanBufferBuilder;
use arrow_array::{
    ArrayRef, DictionaryArray, Int64Array, RecordBatch, StringArray, UInt8Array, UInt32Array,
    UInt64Array,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{ArrowError, DataType, Field, Schema};

use crate::dict::ColumnDictionary;
use crate::source::{SpanSource, walk_pre_order};

/// Wire names for the 23 entry types (`01h`), indexed by discriminant − 1. The
/// entry_type dictionary is static — known at schema-definition time, zero flush work
/// (`01a`'s `enum` strategy).
pub const ENTRY_TYPE_NAMES: [&str; 23] = [
    "span-start",
    "span-ok",
    "span-err",
    "span-exception",
    "info",
    "debug",
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
    "buffer-overflow-writes",
    "buffer-created",
    "buffer-overflows",
];

fn dict_type(key: DataType) -> DataType {
    DataType::Dictionary(Box::new(key), Box::new(DataType::Utf8))
}

/// The flat trace-table schema (`01f`): every event is one row; core columns always
/// present. Attribute columns (sparse, schema-defined) are appended by the
/// macro-generated layer later — this is the invariant prefix.
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

/// Convert one flush's root buffers into a SINGLE RecordBatch with dictionaries
/// shared across all roots (`01k`: this is why a flush is one batch, not N).
pub fn convert_span_trees<S: SpanSource>(roots: &[S]) -> Result<RecordBatch, ArrowError> {
    // ---- Pass 1: count rows, accumulate string dictionaries. ----
    let mut total_rows = 0usize;
    let mut trace_dict = ColumnDictionary::default();
    let mut message_dict = ColumnDictionary::default();
    walk_pre_order(roots, &mut |b: &S| {
        let rows = b.row_count();
        total_rows += rows;
        trace_dict.observe(b.identity().trace_id.as_str());
        for row in 0..rows {
            if let Some(m) = b.message(row) {
                message_dict.observe(m);
            }
        }
    });
    let trace_dict = trace_dict.finalize_indexed();
    let message_dict = message_dict.finalize_indexed();

    // ---- Pass 2: exact-size columns, sequential writes. ----
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

    walk_pre_order(roots, &mut |b: &S| {
        let id = b.identity();
        let trace_key = trace_dict
            .index_of(id.trace_id.as_str())
            .expect("trace_id observed in pass 1");
        let (p_thread, p_span, p_ok) = match &id.parent {
            Some(p) => (p.thread_id, p.span_id, true),
            None => (0, 0, false),
        };
        for row in 0..b.row_count() {
            timestamps.push(b.timestamp(row));
            trace_keys.push(trace_key);
            thread_ids.push(id.thread_id);
            span_ids.push(id.span_id);
            parent_thread_ids.push(p_thread);
            parent_span_ids.push(p_span);
            parent_valid.append(p_ok);
            // Discriminants are 1-based; 0 (unwritten row) is unreachable because we
            // only walk rows < row_count.
            entry_keys.push(b.entry_type(row).saturating_sub(1));
            match b.message(row) {
                Some(m) => {
                    message_keys.push(message_dict.index_of(m).expect("observed in pass 1"));
                    message_valid.append(true);
                }
                None => {
                    message_keys.push(0);
                    message_valid.append(false);
                }
            }
            line_numbers.push(b.line_number(row));
        }
    });
    debug_assert_eq!(
        timestamps.len(),
        total_rows,
        "pass-1/pass-2 row-count drift"
    );

    let parent_nulls = NullBuffer::new(parent_valid.finish());
    let message_nulls = NullBuffer::new(message_valid.finish());

    let entry_values = StringArray::from_iter_values(ENTRY_TYPE_NAMES);
    let entry_col = DictionaryArray::try_new(
        UInt8Array::from(entry_keys),
        Arc::new(entry_values) as ArrayRef,
    )?;
    let trace_col = DictionaryArray::try_new(
        UInt32Array::from(trace_keys),
        Arc::new(StringArray::from_iter_values(trace_dict.values.iter())) as ArrayRef,
    )?;
    // An all-null dictionary column still needs ≥1 dictionary value slot.
    let message_values: ArrayRef = if message_dict.is_empty() {
        Arc::new(StringArray::from(vec![""]))
    } else {
        Arc::new(StringArray::from_iter_values(message_dict.values.iter()))
    };
    let message_col = DictionaryArray::try_new(
        UInt32Array::new(message_keys.into(), Some(message_nulls)),
        message_values,
    )?;

    RecordBatch::try_new(
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
    )
}
