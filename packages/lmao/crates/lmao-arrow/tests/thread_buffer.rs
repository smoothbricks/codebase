use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch, StringArray};
use lmao_arrow::{StableVocabularyCatalog, convert_thread_buffer};
use lmao_core::{ColumnValue, EntryType, FieldMeta, FieldStrategy, ThreadSpanBuffer, TraceId};

static FIELDS: &[FieldMeta] = &[
    FieldMeta::new("answer", FieldStrategy::Number),
    FieldMeta::new("label", FieldStrategy::Category),
];

fn trace() -> TraceId {
    TraceId::new("thread-test").unwrap()
}

fn empty_catalog() -> StableVocabularyCatalog<'static> {
    StableVocabularyCatalog::EMPTY
}

fn batch(buffer: &mut ThreadSpanBuffer) -> RecordBatch {
    let rows = buffer.row_count();
    convert_thread_buffer(buffer, &empty_catalog(), 0, rows, 99).unwrap()
}

#[test]
fn child_attributes_are_written_to_child_rows() {
    let mut buffer = ThreadSpanBuffer::new(7, 8, FIELDS);
    let parent = buffer
        .open_span(trace(), 0, 0, "parent".into(), 10, 1)
        .unwrap();
    let child = buffer
        .open_span(trace(), 7, parent, "child".into(), 11, 2)
        .unwrap();
    let child_start = buffer.start_row(child).unwrap();
    buffer
        .write_attr(child_start as u32, 12, ColumnValue::Number(42.0))
        .unwrap();

    let output = batch(&mut buffer);
    let values = output
        .column(12)
        .as_primitive::<arrow_array::types::Float64Type>();
    assert_eq!(values.value(child_start), 42.0);
    assert!(values.is_valid(child_start));
}

#[test]
fn child_after_parent_close_keeps_parent_id() {
    let mut buffer = ThreadSpanBuffer::new(7, 8, FIELDS);
    let parent = buffer
        .open_span(trace(), 0, 0, "parent".into(), 10, 1)
        .unwrap();
    buffer.end_ok(parent, 12).unwrap();
    let child = buffer
        .open_span(trace(), 7, parent, "child".into(), 13, 2)
        .unwrap();
    let row = buffer.start_row(child).unwrap();
    assert_eq!(buffer.parent_span_id_at(row), Some(parent));
    let output = batch(&mut buffer);
    let parents = output
        .column(5)
        .as_primitive::<arrow_array::types::UInt32Type>();
    assert_eq!(parents.value(row), parent);
    assert!(parents.is_valid(row));
}

#[test]
fn open_span_gets_synthesized_exception_at_flush_without_closing_live_span() {
    let mut buffer = ThreadSpanBuffer::new(7, 8, FIELDS);
    let span = buffer
        .open_span(trace(), 0, 0, "open".into(), 10, 1)
        .unwrap();
    let completion = buffer.completion_row(span).unwrap();
    let output = batch(&mut buffer);
    let entries = output
        .column(6)
        .as_dictionary::<arrow_array::types::UInt8Type>();
    assert_eq!(entries.key(completion), Some(4));
    assert_eq!(
        output
            .column(0)
            .as_primitive::<arrow_array::types::TimestampNanosecondType>()
            .value(completion),
        99
    );
    assert!(buffer.is_span_open(span));
    buffer.end_ok(span, 120).unwrap();
    let output = batch(&mut buffer);
    let entries = output
        .column(6)
        .as_dictionary::<arrow_array::types::UInt8Type>();
    assert_eq!(entries.key(completion), Some(2));
}

#[test]
fn output_has_system_prefix_then_schema_columns() {
    let mut buffer = ThreadSpanBuffer::new(7, 8, FIELDS);
    let span = buffer
        .open_span(trace(), 0, 0, "name".into(), 1, 7)
        .unwrap();
    buffer
        .append_log(span, EntryType::Info, Some("log".into()), 8, 2)
        .unwrap();
    let output = batch(&mut buffer);
    assert_eq!(output.num_columns(), 14);
    assert_eq!(output.schema().field(12).name(), "answer");
    let names = output
        .column(10)
        .as_dictionary::<arrow_array::types::UInt32Type>();
    let dict = names
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!((0..dict.len()).any(|index| dict.value(index) == "name"));
}
