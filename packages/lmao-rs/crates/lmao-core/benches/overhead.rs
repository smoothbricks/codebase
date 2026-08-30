//! Measures the cold `SpanBuffer` construction cost.
//!
//! This is deliberately not labeled as an end-to-end tracing-overhead or RSS gate:
//! those require an untraced control executing the same application workload.

use criterion::{Criterion, criterion_group, criterion_main};
use lmao_core::clock::{Clock, TraceAnchor};
use lmao_core::{SpanBuffer, SpanIdentity, TraceId};
use std::hint::black_box;
use std::sync::Arc;

struct FixedClock;
impl Clock for FixedClock {
    fn wall_nanos(&self) -> i64 {
        0
    }
    fn monotonic_nanos(&self) -> u64 {
        0
    }
}

fn bench_span_start(c: &mut Criterion) {
    let clock = FixedClock;
    let anchor = TraceAnchor::capture(&clock);
    let identity = Arc::new(SpanIdentity {
        thread_id: 1,
        span_id: 0,
        trace_id: TraceId::new("bench").unwrap(),
        parent: None,
    });
    c.bench_function("span_start_cap64", |b| {
        b.iter(|| {
            black_box(SpanBuffer::start_dynamic(
                identity.clone(),
                64,
                "span".into(),
                &anchor,
                &clock,
            ))
        })
    });
}

criterion_group!(benches, bench_span_start);
criterion_main!(benches);
