//! Placeholder bench harness for the AxE overhead gates
//! (`AxE/specs/sim/01-deterministic-scheduler.md` §5):
//! - enabling tracing must not cut median throughput by >20% at a 10^6-event run
//! - peak RSS increase ≤25%
//!
//! Once the tracer facade exists, add a traced-vs-untraced pair over the same
//! synthetic workload and report the ratio. For now this benches buffer creation
//! so the harness wiring is proven.

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
        b.iter(|| black_box(SpanBuffer::start(identity.clone(), 64, &anchor, &clock)))
    });
}

criterion_group!(benches, bench_span_start);
criterion_main!(benches);
