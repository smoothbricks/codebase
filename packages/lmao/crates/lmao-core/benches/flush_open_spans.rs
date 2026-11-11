//! What one streaming flush of a long-lived thread row store costs while spans
//! stay open across it: `flush_rows` plus `retain_open`, timed alone, after a
//! window of finished work written the way a host runtime writes it.
//!
//! Shape: an embedding host's tracer store (256-row blocks, 32 schema
//! attributes), `open` long-lived spans (a receive loop, a prompt, an owed
//! restore) whose oldest ends and is replaced every window, so the open
//! set churns from the front, and 64 finished spans of two logs each per
//! window. The replacement opens halfway through the window, behind finished
//! rows.

use std::collections::VecDeque;
use std::hint::black_box;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use lmao_core::{ColumnValue, EntryType, FieldMeta, FieldStrategy, ThreadSpanBuffer, TraceId};

const CAPACITY: usize = 256;
const FINISHED_PER_WINDOW: usize = 64;
/// The ordinal of schema attribute 0: the system columns come first.
const FIRST_ATTRIBUTE: u16 = 12;

static FIELDS: &[FieldMeta] = &[
    FieldMeta::new("f00", FieldStrategy::Number),
    FieldMeta::new("f01", FieldStrategy::Category),
    FieldMeta::new("f02", FieldStrategy::Uint64),
    FieldMeta::new("f03", FieldStrategy::Text),
    FieldMeta::new("f04", FieldStrategy::Boolean),
    FieldMeta::new("f05", FieldStrategy::Number),
    FieldMeta::new("f06", FieldStrategy::Category),
    FieldMeta::new("f07", FieldStrategy::Number),
    FieldMeta::new("f08", FieldStrategy::Number),
    FieldMeta::new("f09", FieldStrategy::Category),
    FieldMeta::new("f10", FieldStrategy::Uint64),
    FieldMeta::new("f11", FieldStrategy::Text),
    FieldMeta::new("f12", FieldStrategy::Boolean),
    FieldMeta::new("f13", FieldStrategy::Number),
    FieldMeta::new("f14", FieldStrategy::Category),
    FieldMeta::new("f15", FieldStrategy::Number),
    FieldMeta::new("f16", FieldStrategy::Number),
    FieldMeta::new("f17", FieldStrategy::Category),
    FieldMeta::new("f18", FieldStrategy::Uint64),
    FieldMeta::new("f19", FieldStrategy::Text),
    FieldMeta::new("f20", FieldStrategy::Boolean),
    FieldMeta::new("f21", FieldStrategy::Number),
    FieldMeta::new("f22", FieldStrategy::Category),
    FieldMeta::new("f23", FieldStrategy::Number),
    FieldMeta::new("f24", FieldStrategy::Number),
    FieldMeta::new("f25", FieldStrategy::Category),
    FieldMeta::new("f26", FieldStrategy::Uint64),
    FieldMeta::new("f27", FieldStrategy::Text),
    FieldMeta::new("f28", FieldStrategy::Boolean),
    FieldMeta::new("f29", FieldStrategy::Number),
    FieldMeta::new("f30", FieldStrategy::Category),
    FieldMeta::new("f31", FieldStrategy::Number),
];

fn open_long(store: &mut ThreadSpanBuffer, trace: &TraceId, timestamp: i64) -> u32 {
    let span = store
        .open_span(trace.clone(), 0, 0, "long".into(), timestamp, 1)
        .unwrap();
    for field in 0..4 {
        store
            .write_tag(span, FIRST_ATTRIBUTE + field * 8, ColumnValue::Number(1.0))
            .unwrap();
    }
    span
}

/// One flush interval's writes: the oldest long span ends, 64 finished spans
/// run, and a new long span opens among them.
fn window(store: &mut ThreadSpanBuffer, open: &mut VecDeque<u32>, trace: &TraceId, timestamp: i64) {
    let oldest = open.pop_front().expect("the open set is never empty");
    store.end_ok(oldest, timestamp).unwrap();
    for index in 0..FINISHED_PER_WINDOW {
        if index == FINISHED_PER_WINDOW / 2 {
            open.push_back(open_long(store, trace, timestamp));
        }
        let span = store
            .open_span(trace.clone(), 0, 0, "work".into(), timestamp, 2)
            .unwrap();
        store
            .write_tag(span, FIRST_ATTRIBUTE, ColumnValue::Number(2.0))
            .unwrap();
        store
            .write_tag(span, FIRST_ATTRIBUTE + 2, ColumnValue::Uint64(3))
            .unwrap();
        for _ in 0..2 {
            store
                .append_log(span, EntryType::Info, Some("step".into()), 3, timestamp)
                .unwrap();
        }
        store.end_ok(span, timestamp).unwrap();
    }
}

fn bench_flush_with_open_spans(c: &mut Criterion) {
    let mut group = c.benchmark_group("flush_with_open_spans");
    let trace = TraceId::new("flush-bench").unwrap();
    for open_spans in [8usize, 64, 512] {
        group.bench_with_input(
            BenchmarkId::from_parameter(open_spans),
            &open_spans,
            |b, &open_spans| {
                let mut store = ThreadSpanBuffer::new(1, CAPACITY, FIELDS);
                let mut open: VecDeque<u32> = (0..open_spans)
                    .map(|_| open_long(&mut store, &trace, 0))
                    .collect();
                let mut rows = Vec::new();
                let mut timestamp = 0i64;
                b.iter_custom(|iterations| {
                    let mut spent = Duration::ZERO;
                    for _ in 0..iterations {
                        timestamp += 1;
                        window(&mut store, &mut open, &trace, timestamp);
                        let start = Instant::now();
                        store.flush_rows(&mut rows).unwrap();
                        store.retain_open();
                        spent += start.elapsed();
                        black_box(&rows);
                    }
                    spent
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_flush_with_open_spans);
criterion_main!(benches);
