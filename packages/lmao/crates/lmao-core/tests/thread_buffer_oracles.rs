//! Pair oracles for ThreadSpanBuffer. These are allowed to fail: they name
//! contracts the implementer has not yet pinned.

use lmao_core::{
    ColumnValue, EntryType, FieldMeta, FieldStrategy, SYSTEM_COLUMN_COUNT, ThreadBufferError,
    ThreadSpanBuffer, TraceId,
};

static FIELDS: &[FieldMeta] = &[
    FieldMeta::new("answer", FieldStrategy::Number),
    FieldMeta::new("label", FieldStrategy::Category),
];

fn trace() -> TraceId {
    TraceId::new("oracle-trace").unwrap()
}

/// Span ids are thread-local. Isolate the counter so this does not depend on
/// whatever other tests on this worker thread already minted.
fn on_fresh_thread<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::spawn(f)
        .join()
        .expect("oracle thread panicked")
}

#[test]
fn first_two_span_ids_on_a_fresh_thread_are_nonzero_and_distinct() {
    on_fresh_thread(|| {
        let mut buffer = ThreadSpanBuffer::new(7, 8, FIELDS);
        let first = buffer.open_span(trace(), 0, 0, "one".into(), 1, 0).unwrap();
        let second = buffer.open_span(trace(), 0, 0, "two".into(), 2, 0).unwrap();
        assert_ne!(first, 0, "span id must be nonzero");
        assert_ne!(second, 0, "span id must be nonzero");
        assert_ne!(
            first, second,
            "fresh-thread first two opens collided at span_id={first}; mapping next_span_id()==0 to 1 collides with the subsequent 1"
        );
        assert!(
            buffer.start_row(first).is_some(),
            "first span record must survive the second open"
        );
        assert!(
            buffer.start_row(second).is_some(),
            "second span record must be present"
        );
        assert_ne!(
            buffer.start_row(first),
            buffer.start_row(second),
            "colliding span ids overwrite the HashMap side table and lose the first span's rows"
        );
    });
}

#[test]
fn open_when_one_slot_remains_does_not_leave_a_dense_row_hole() {
    let mut buffer = ThreadSpanBuffer::new(7, 8, FIELDS);
    let span = buffer
        .open_span(trace(), 0, 0, "span".into(), 1, 0)
        .unwrap();
    for i in 0..5 {
        buffer
            .append_log(span, EntryType::Info, Some("log".into()), i, 2)
            .unwrap();
    }
    assert_eq!(buffer.row_count(), 7, "2 reserved + 5 logs fill 7 of 8");

    let child = buffer
        .open_span(trace(), 7, span, "child".into(), 3, 0)
        .unwrap();
    let child_start = buffer.start_row(child).unwrap();
    assert_eq!(
        child_start,
        7,
        "ensure_rows(2) with remaining==1 abandoned the last slot of the live block; child start_row is {child_start}, row_count is {}",
        buffer.row_count()
    );
    for row in 0..buffer.row_count() {
        assert!(
            buffer.trace_id_at(row).is_some(),
            "row {row} is addressable via row_count={} but has no trace_id — a hole in the dense row space",
            buffer.row_count()
        );
        assert_ne!(
            buffer.span_id_at(row),
            Some(0),
            "row {row} carries span_id 0, which is the block's unwritten default"
        );
    }
}

#[test]
fn write_attr_refuses_system_prefix_ordinals() {
    let mut buffer = ThreadSpanBuffer::new(7, 8, FIELDS);
    let span = buffer
        .open_span(trace(), 0, 0, "span".into(), 1, 0)
        .unwrap();
    let row = buffer.start_row(span).unwrap() as u32;
    for ordinal in 0..SYSTEM_COLUMN_COUNT as u16 {
        let err = buffer
            .write_attr(row, ordinal, ColumnValue::Number(1.0))
            .unwrap_err();
        assert!(
            matches!(err, ThreadBufferError::InvalidColumnOrdinal(o) if o == ordinal),
            "system ordinal {ordinal} must be refused, got {err:?}"
        );
    }
}
