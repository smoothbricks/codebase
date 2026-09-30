use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch, StringArray};
use lmao_arrow::{StableVocabularyCatalog, convert_thread_buffer, convert_thread_span_rows};
use lmao_core::tuning::ARENA_RECLAIM_BYTES;
use lmao_core::{
    ColumnValue, EntryType, FieldMeta, FieldStrategy, TextInput, ThreadSpanBuffer, TraceId,
};

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

/// Row `row` of the dictionary-encoded string column `name`.
fn text_at(batch: &RecordBatch, name: &str, row: usize) -> Option<String> {
    let column = batch.column_by_name(name).expect("column");
    let dictionary = column.as_any_dictionary();
    let values = dictionary.values().as_string::<i32>();
    let keys = dictionary.normalized_keys();
    column
        .is_valid(row)
        .then(|| values.value(keys[row]).to_owned())
}

/// A long-lived store reclaims the text its retired rows named. Past the
/// threshold, a retain rebuilds the arena from what the open spans still name,
/// renumbers their cells and moves the epoch — so churn never walks the store
/// into its ceiling, and the span it kept still converts with its own name and
/// label.
#[test]
fn a_retain_past_the_threshold_reclaims_text_and_keeps_what_open_spans_name() {
    let mut buffer = ThreadSpanBuffer::new(7, 8, FIELDS);
    let kept = buffer
        .open_span(trace(), 0, 0, TextInput::Dynamic("kept-span"), 10, 1)
        .unwrap();
    let sku = buffer.intern("sku-kept").unwrap();
    buffer.write_tag(kept, 13, ColumnValue::Text(sku)).unwrap();
    let epoch = buffer.text_epoch();

    let filler = "x".repeat(1024);
    let mut count = 0u32;
    while buffer.arena().len() < ARENA_RECLAIM_BYTES {
        let name = format!("{count}-{filler}");
        let span = buffer
            .open_span(trace(), 0, 0, TextInput::Dynamic(&name), 11, 2)
            .unwrap();
        buffer.end_ok(span, 12).unwrap();
        count += 1;
    }
    let mut rows = Vec::new();
    buffer.flush_rows(&mut rows).unwrap();
    buffer.retain_open();

    assert_ne!(
        buffer.text_epoch(),
        epoch,
        "a reclaim renumbers, so the epoch moves"
    );
    assert_eq!(
        buffer.arena().len(),
        "kept-span".len() + "sku-kept".len(),
        "only what the open span names survives"
    );
    let fresh = buffer.intern("after-reclaim").unwrap();
    buffer
        .append_log(
            kept,
            EntryType::Info,
            Some(TextInput::Dynamic("after-reclaim")),
            3,
            13,
        )
        .unwrap();
    buffer.end_ok(kept, 20).unwrap();
    buffer.flush_rows(&mut rows).unwrap();
    let batch = convert_thread_span_rows(&buffer, &empty_catalog(), &rows).unwrap();
    assert_eq!(batch.num_rows(), 3, "start, completion, the log row");
    assert_eq!(text_at(&batch, "message", 0).as_deref(), Some("kept-span"));
    assert_eq!(text_at(&batch, "label", 0).as_deref(), Some("sku-kept"));
    assert_eq!(
        text_at(&batch, "message", 2).as_deref(),
        Some("after-reclaim")
    );
    assert_eq!(buffer.interned(fresh), Some("after-reclaim"));
}

#[test]
fn a_streaming_flush_leaves_an_open_span_open_and_emits_its_completion_later() {
    let mut buffer = ThreadSpanBuffer::new(7, 8, FIELDS);
    let span = buffer
        .open_span(trace(), 0, 0, "pending".into(), 10, 1)
        .unwrap();
    let label = buffer.intern("sku").unwrap();
    buffer
        .write_tag(span, 13, ColumnValue::Text(label))
        .unwrap();
    let mut rows = Vec::new();
    buffer.flush_rows(&mut rows).unwrap();
    let first = convert_thread_span_rows(&buffer, &empty_catalog(), &rows).unwrap();
    let entries = first
        .column(6)
        .as_dictionary::<arrow_array::types::UInt8Type>();
    let names = entries
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(first.num_rows(), 1, "only the start row of an open span");
    assert_eq!(names.value(entries.keys().value(0) as usize), "span-start");
    buffer.retain_open();

    buffer.end_ok(span, 20).unwrap();
    buffer.flush_rows(&mut rows).unwrap();
    let second = convert_thread_span_rows(&buffer, &empty_catalog(), &rows).unwrap();
    assert_eq!(
        second.num_rows(),
        2,
        "the final start row and the completion"
    );
    let labels = second
        .column(13)
        .as_dictionary::<arrow_array::types::UInt32Type>();
    let values = labels
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(labels.keys().value(0) as usize), "sku");
    let stamps = second
        .column(0)
        .as_primitive::<arrow_array::types::TimestampNanosecondType>();
    assert_eq!(stamps.value(1), 20);
}
