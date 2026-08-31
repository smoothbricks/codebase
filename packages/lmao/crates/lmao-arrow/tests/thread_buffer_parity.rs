//! Schema and system-column oracles against the legacy per-span convert path.
//! These must be able to fail.

use std::sync::Arc;

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, TimestampNanosecondType, UInt32Type, UInt64Type};
use arrow_schema::{DataType, Field};
use lmao_arrow::{
    StableVocabularyCatalog, convert_span_trees, convert_thread_buffer, trace_schema,
};
use lmao_core::{
    Clock, ColumnValue, EntryType, FieldMeta, FieldStrategy, SYSTEM_COLUMN_COUNT, SYSTEM_COLUMNS,
    SpanBuffer, SpanIdentity, ThreadSpanBuffer, TraceAnchor, TraceId,
};

static FIELDS: &[FieldMeta] = &[
    FieldMeta::new("answer", FieldStrategy::Number),
    FieldMeta::new("label", FieldStrategy::Category),
];

struct FixedClock;
impl Clock for FixedClock {
    fn wall_nanos(&self) -> i64 {
        1_700_000_000_000_000_000
    }
    fn monotonic_nanos(&self) -> u64 {
        0
    }
}

fn trace() -> TraceId {
    TraceId::new("parity-trace").unwrap()
}

fn empty_catalog() -> StableVocabularyCatalog<'static> {
    StableVocabularyCatalog::EMPTY
}

fn field_shape(field: &Field) -> (String, DataType, bool) {
    (
        field.name().to_string(),
        field.data_type().clone(),
        field.is_nullable(),
    )
}

#[test]
fn thread_batch_system_prefix_matches_trace_schema_and_legacy_trees() {
    let clock = FixedClock;
    let anchor = TraceAnchor::capture(&clock);
    let identity = Arc::new(SpanIdentity {
        thread_id: 7,
        span_id: 1,
        trace_id: trace(),
        parent: None,
    });
    let mut tree = SpanBuffer::start_dynamic(identity, 8, "root".into(), &anchor, &clock);
    tree.end_ok(&anchor, &clock);
    let tree_batch = convert_span_trees(&[tree], &empty_catalog()).unwrap();

    let mut thread = ThreadSpanBuffer::new(7, 8, FIELDS);
    let span = thread
        .open_span(trace(), 0, 0, "root".into(), 10, 1)
        .unwrap();
    thread.end_ok(span, 11).unwrap();
    let rows = thread.row_count();
    let thread_batch = convert_thread_buffer(&mut thread, &empty_catalog(), 0, rows, 99).unwrap();

    let expected = trace_schema();
    assert_eq!(
        expected.fields().len(),
        SYSTEM_COLUMN_COUNT,
        "trace_schema length drifted from generated SYSTEM_COLUMN_COUNT"
    );
    assert_eq!(
        SYSTEM_COLUMNS.len(),
        SYSTEM_COLUMN_COUNT,
        "SYSTEM_COLUMNS / SYSTEM_COLUMN_COUNT disagree"
    );

    assert_eq!(
        tree_batch.num_columns(),
        SYSTEM_COLUMN_COUNT,
        "legacy convert_span_trees must emit exactly the system prefix; got {} columns {:?}",
        tree_batch.num_columns(),
        tree_batch
            .schema()
            .fields()
            .iter()
            .map(|f| field_shape(f))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        thread_batch.num_columns(),
        SYSTEM_COLUMN_COUNT + FIELDS.len(),
        "thread converter must be system prefix plus schema attributes; got {} columns {:?}",
        thread_batch.num_columns(),
        thread_batch
            .schema()
            .fields()
            .iter()
            .map(|f| field_shape(f))
            .collect::<Vec<_>>()
    );

    for i in 0..SYSTEM_COLUMN_COUNT {
        let from_trace = field_shape(expected.field(i));
        let from_tree = field_shape(tree_batch.schema().field(i));
        let from_thread = field_shape(thread_batch.schema().field(i));
        assert_eq!(
            from_trace, from_tree,
            "legacy tree field {i} != trace_schema"
        );
        assert_eq!(
            from_trace, from_thread,
            "thread-buffer field {i} != trace_schema (live layout is not the Arrow system prefix)"
        );
    }

    // The over-wide claim is "live layout == Arrow layout". The live thread
    // converter's *full* schema is not the legacy RecordBatch schema: it appends
    // attributes. Pin that difference so a later silent drop of attributes fails.
    assert_ne!(
        thread_batch.schema().as_ref(),
        tree_batch.schema().as_ref(),
        "thread and legacy batches unexpectedly share a full schema; attribute columns missing?"
    );
    assert_eq!(thread_batch.schema().field(12).name(), "answer");
    assert_eq!(
        thread_batch.schema().field(12).data_type(),
        &DataType::Float64
    );
}

#[test]
fn start_row_message_resolves_to_span_name_not_sorted_slot() {
    let mut buffer = ThreadSpanBuffer::new(7, 8, FIELDS);
    let span = buffer
        .open_span(trace(), 0, 0, "parent".into(), 10, 1)
        .unwrap();
    buffer
        .append_log(span, EntryType::Info, Some("hello".into()), 3, 11)
        .unwrap();
    buffer.end_ok(span, 12).unwrap();
    let start = buffer.start_row(span).unwrap();
    let log = start + 2;
    let rows = buffer.row_count();
    let output = convert_thread_buffer(&mut buffer, &empty_catalog(), 0, rows, 99).unwrap();
    let col = output.column(10).as_dictionary::<UInt32Type>();
    let dict = col.values().as_string::<i32>();
    let at = |row: usize| -> Option<&str> { col.key(row).map(|key| dict.value(key)) };
    assert_eq!(
        at(start),
        Some("parent"),
        "span-start message must be the span name"
    );
    assert_eq!(
        at(log),
        Some("hello"),
        "info row message must be the log template"
    );
}

#[test]
fn child_attribute_does_not_leak_onto_parent_rows() {
    let mut buffer = ThreadSpanBuffer::new(7, 8, FIELDS);
    // Burn the 0→1 mint collision so parent and child ids are distinct.
    let burned = buffer
        .open_span(trace(), 0, 0, "burn".into(), 9, 0)
        .unwrap();
    buffer.end_ok(burned, 9).unwrap();
    let parent = buffer
        .open_span(trace(), 0, 0, "parent".into(), 10, 1)
        .unwrap();
    let child = buffer
        .open_span(trace(), 7, parent, "child".into(), 11, 2)
        .unwrap();
    assert_ne!(parent, child, "attribute oracle requires distinct span ids");
    let parent_start = buffer.start_row(parent).unwrap();
    let child_start = buffer.start_row(child).unwrap();
    buffer
        .write_attr(child_start as u32, 12, ColumnValue::Number(42.0))
        .unwrap();

    let rows = buffer.row_count();
    let output = convert_thread_buffer(&mut buffer, &empty_catalog(), 0, rows, 99).unwrap();
    let values = output.column(12).as_primitive::<Float64Type>();
    assert!(values.is_valid(child_start), "child start must carry 42");
    assert_eq!(values.value(child_start), 42.0);
    assert!(
        !values.is_valid(parent_start),
        "parent start row must stay NULL when only the child was tagged; got {:?}",
        values
            .is_valid(parent_start)
            .then(|| values.value(parent_start))
    );
}

#[test]
fn system_column_content_matches_legacy_per_span_when_ids_are_aligned() {
    // Legacy SpanBuffer takes span_id from identity; ThreadSpanBuffer mints.
    // Compare the system columns that are independent of minting: timestamps,
    // trace_id, thread_id, parentage nullability, entry_type, message, line —
    // grouped by span role (root vs child), not by absolute row, because the
    // thread buffer interleaves while convert_span_trees walks pre-order.
    let clock = FixedClock;
    let anchor = TraceAnchor::capture(&clock);
    let root_id = Arc::new(SpanIdentity {
        thread_id: 7,
        span_id: 11,
        trace_id: trace(),
        parent: None,
    });
    let child_id = Arc::new(SpanIdentity {
        thread_id: 7,
        span_id: 12,
        trace_id: trace(),
        parent: Some(root_id.clone()),
    });
    let mut root = SpanBuffer::start_dynamic(root_id, 8, "parent".into(), &anchor, &clock);
    let mut child = SpanBuffer::start_dynamic(child_id, 8, "child".into(), &anchor, &clock);
    child.append_dynamic(EntryType::Info, Some("hello".into()), 3, &anchor, &clock);
    child.end_ok(&anchor, &clock);
    root.end_ok(&anchor, &clock);
    root.add_child(child);
    let tree_batch = convert_span_trees(&[root], &empty_catalog()).unwrap();

    let mut thread = ThreadSpanBuffer::new(7, 8, FIELDS);
    let parent = thread
        .open_span(trace(), 0, 0, "parent".into(), clock.wall_nanos(), 0)
        .unwrap();
    let kid = thread
        .open_span(trace(), 7, parent, "child".into(), clock.wall_nanos(), 0)
        .unwrap();
    thread
        .append_log(
            kid,
            EntryType::Info,
            Some("hello".into()),
            3,
            clock.wall_nanos(),
        )
        .unwrap();
    thread.end_ok(kid, clock.wall_nanos()).unwrap();
    thread.end_ok(parent, clock.wall_nanos()).unwrap();
    let rows = thread.row_count();
    let thread_batch =
        convert_thread_buffer(&mut thread, &empty_catalog(), 0, rows, clock.wall_nanos()).unwrap();

    fn messages(batch: &arrow_array::RecordBatch) -> Vec<Option<String>> {
        let col = batch.column(10).as_dictionary::<UInt32Type>();
        let dict = col.values().as_string::<i32>();
        (0..col.len())
            .map(|i| {
                if col.is_valid(i) {
                    Some(dict.value(col.key(i).unwrap()).to_string())
                } else {
                    None
                }
            })
            .collect()
    }
    fn entry_keys(batch: &arrow_array::RecordBatch) -> Vec<u8> {
        batch
            .column(6)
            .as_dictionary::<arrow_array::types::UInt8Type>()
            .keys()
            .values()
            .iter()
            .copied()
            .collect()
    }

    let tree_messages = messages(&tree_batch);
    let thread_messages = messages(&thread_batch);
    let tree_entries = entry_keys(&tree_batch);
    let thread_entries = entry_keys(&thread_batch);

    // Same multiset of (entry_type, message). Absolute row order is allowed to
    // differ; a missing or extra system row is not.
    let mut tree_pairs: Vec<(u8, Option<String>)> =
        tree_entries.into_iter().zip(tree_messages).collect();
    let mut thread_pairs: Vec<(u8, Option<String>)> =
        thread_entries.into_iter().zip(thread_messages).collect();
    tree_pairs.sort();
    thread_pairs.sort();
    assert_eq!(
        tree_pairs, thread_pairs,
        "system (entry_type, message) multisets differ between convert_span_trees and convert_thread_buffer"
    );

    let tree_threads = tree_batch.column(2).as_primitive::<UInt64Type>();
    let thread_threads = thread_batch.column(2).as_primitive::<UInt64Type>();
    for i in 0..thread_threads.len() {
        assert_eq!(
            thread_threads.value(i),
            7,
            "thread_id column drifted at {i}"
        );
    }
    for i in 0..tree_threads.len() {
        assert_eq!(tree_threads.value(i), 7, "legacy thread_id drifted at {i}");
    }

    // Timestamps in the thread path must be the written values, not a silent
    // conversion default. Open-span completion that is still exception uses the
    // flush timestamp; both spans were ended, so every row is wall_nanos.
    let ts = thread_batch
        .column(0)
        .as_primitive::<TimestampNanosecondType>();
    for i in 0..ts.len() {
        assert_eq!(
            ts.value(i),
            clock.wall_nanos(),
            "thread timestamp at {i} is {}, not the written wall time",
            ts.value(i)
        );
    }
}
