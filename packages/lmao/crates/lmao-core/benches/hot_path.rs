//! Native-Rust hot-path benchmarks, shape-matched to
//! `packages/lmao/benchmarks/js-vs-wasm.bench.ts` so the ns/op numbers are
//! directly comparable against the recorded JS (mitata) results:
//!
//! - "Warm: Simple trace"             -> span_lifecycle_*
//! - "Warm: Multiple log entries(50)" -> span_50_logs (per-event cost = delta/50)
//! - "Warm: Trace with tags"          -> schema_tag_write_* (generated real columns)
//!
//! Two clock variants isolate what's actually being measured:
//! - FixedClock: pure buffer machinery, no time syscalls
//! - SystemClock: production cost including `Instant::now()`

use criterion::{Criterion, criterion_group, criterion_main};
use lmao_core::clock::{Clock, SystemClock, TraceAnchor};
use lmao_core::{EntryType, SpanBuffer, SpanIdentity, TextInput, TraceId};
use std::hint::black_box;
use std::sync::Arc;

struct FixedClock;
impl Clock for FixedClock {
    fn wall_nanos(&self) -> i64 {
        1_700_000_000_000_000_000
    }
    fn monotonic_nanos(&self) -> u64 {
        42
    }
}

fn identity() -> Arc<SpanIdentity> {
    Arc::new(SpanIdentity {
        thread_id: 0xDEAD_BEEF,
        span_id: 1,
        trace_id: TraceId::new("bench-trace-0001").unwrap(),
        parent: None,
    })
}

/// Warm simple trace: span start + ok completion (includes the two column Vec
/// allocations, i.e. the UNPOOLED cost — compare against "Cold start" JS numbers
/// as much as warm ones; the pooled bench below is the true warm analogue).
fn bench_span_lifecycle(c: &mut Criterion) {
    let id = identity();
    let fixed = FixedClock;
    let sys = SystemClock::new();
    let fixed_anchor = TraceAnchor::capture(&fixed);
    let sys_anchor = TraceAnchor::capture(&sys);

    c.bench_function("span_lifecycle_cap8_fixed_clock", |b| {
        b.iter(|| {
            let mut s =
                SpanBuffer::start_dynamic(id.clone(), 8, "span".into(), &fixed_anchor, &fixed);
            s.end_ok(&fixed_anchor, &fixed);
            black_box(s)
        })
    });
    c.bench_function("span_lifecycle_cap8_system_clock", |b| {
        b.iter(|| {
            let mut s = SpanBuffer::start_dynamic(id.clone(), 8, "span".into(), &sys_anchor, &sys);
            s.end_ok(&sys_anchor, &sys);
            black_box(s)
        })
    });
}

/// Warm: 50 log entries appended to one span (cap 64 -> exercises one overflow
/// chain hop, same as the TS shape with capacity 8 exercises several).
fn bench_span_50_logs(c: &mut Criterion) {
    let id = identity();
    let fixed = FixedClock;
    let sys = SystemClock::new();
    let fixed_anchor = TraceAnchor::capture(&fixed);
    let sys_anchor = TraceAnchor::capture(&sys);

    c.bench_function("span_plus_50_logs_fixed_clock", |b| {
        b.iter(|| {
            let mut s =
                SpanBuffer::start_dynamic(id.clone(), 64, "span".into(), &fixed_anchor, &fixed);
            for _ in 0..50 {
                s.append_dynamic(
                    EntryType::Info,
                    Some("log template".into()),
                    0,
                    &fixed_anchor,
                    &fixed,
                );
            }
            s.end_ok(&fixed_anchor, &fixed);
            black_box(s)
        })
    });
    c.bench_function("span_plus_50_logs_system_clock", |b| {
        b.iter(|| {
            let mut s = SpanBuffer::start_dynamic(id.clone(), 64, "span".into(), &sys_anchor, &sys);
            for _ in 0..50 {
                s.append_dynamic(
                    EntryType::Info,
                    Some("log template".into()),
                    0,
                    &sys_anchor,
                    &sys,
                );
            }
            s.end_ok(&sys_anchor, &sys);
            black_box(s)
        })
    });
}

/// Per-event append cost in isolation on a warm buffer (the zero-alloc gate
/// path): reset write_index by rebuilding is avoided by using a large buffer and
/// measuring batches of 1000 appends without overflow.
fn bench_append_only(c: &mut Criterion) {
    let id = identity();
    let fixed = FixedClock;
    let anchor = TraceAnchor::capture(&fixed);

    c.bench_function("append_1000_no_overflow_fixed_clock", |b| {
        b.iter_batched(
            || SpanBuffer::start_dynamic(id.clone(), 1024, "span".into(), &anchor, &fixed),
            |mut s| {
                for _ in 0..1000 {
                    s.append_dynamic(
                        EntryType::Info,
                        Some("log template".into()),
                        0,
                        &anchor,
                        &fixed,
                    );
                }
                black_box(s)
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

criterion_group!(
    benches,
    bench_span_lifecycle,
    bench_span_50_logs,
    bench_append_only,
    bench_clock_variants,
    bench_lazy_first_touch,
    bench_schema_tag_write,
    bench_ratchet
);
criterion_main!(benches);

/// The benchmark-driven optimization: coarse/batch-stamped clock vs per-event
/// `Instant::now()` (`docs/optimization-investigation.md` — precise reads were
/// ~80% of the hot path). Shape: 50 appends per span, comparable to
/// span_plus_50_logs_system_clock.
fn bench_clock_variants(c: &mut Criterion) {
    use lmao_core::CoarseClock;
    let id = identity();
    let sys = SystemClock::new();
    let coarse = CoarseClock::new(SystemClock::new(), 16);
    let sys_anchor = TraceAnchor::capture(&sys);
    let coarse_anchor = TraceAnchor::capture(&coarse);

    c.bench_function("span_50_logs_precise_clock", |b| {
        b.iter(|| {
            let mut s = SpanBuffer::start_dynamic(id.clone(), 64, "span".into(), &sys_anchor, &sys);
            for _ in 0..50 {
                s.append_dynamic(EntryType::Info, None, 0, &sys_anchor, &sys);
            }
            s.end_ok(&sys_anchor, &sys);
            black_box(s)
        })
    });
    c.bench_function("span_50_logs_coarse_clock_16", |b| {
        b.iter(|| {
            let mut s =
                SpanBuffer::start_dynamic(id.clone(), 64, "span".into(), &coarse_anchor, &coarse);
            for _ in 0..50 {
                s.append_dynamic(EntryType::Info, None, 0, &coarse_anchor, &coarse);
            }
            coarse.invalidate(); // span boundary stays precise
            s.end_ok(&coarse_anchor, &coarse);
            black_box(s)
        })
    });
}

/// Lazy column economics: first touch (alloc) vs steady-state write.
fn bench_lazy_first_touch(c: &mut Criterion) {
    use lmao_core::F64Column;
    c.bench_function("lazy_column_first_touch_cap1024", |b| {
        b.iter(|| {
            let mut col = F64Column::new();
            col.set(0, 1024, black_box(1.0));
            black_box(col)
        })
    });
    let mut warm = F64Column::new();
    warm.set(0, 1024, 0.0);
    let mut row = 0usize;
    c.bench_function("lazy_column_steady_write", |b| {
        b.iter(|| {
            row = (row + 1) & 1023;
            warm.set(row, 1024, black_box(2.5));
        })
    });
}

/// Real generated tag write (macro output), replacing the hand-rolled proxy.
fn bench_schema_tag_write(c: &mut Criterion) {
    use lmao_core::Clock;
    lmao_macros::define_log_schema!(BenchSchema {
        latency: number,
        route: category,
        outcome: enum["ok", "err"],
    });

    let fixed = FixedClock;
    let anchor = TraceAnchor::capture(&fixed);
    let span = SpanBuffer::start_dynamic(
        identity(),
        64,
        TextInput::Static("benchmark"),
        &anchor,
        &fixed,
    );
    let mut buf = BenchSchema::from_span(span);
    // warm all three columns
    buf.tag_latency(0.0).tag_route(TextInput::Static("warm"));
    buf.tag_outcome(0).unwrap();
    let route: std::sync::Arc<str> = "GET /api/v1/sessions".into();

    c.bench_function("schema_tag_write_f64", |b| {
        b.iter(|| {
            buf.tag_latency(black_box(42.5));
        })
    });
    c.bench_function("schema_tag_write_enum", |b| {
        b.iter(|| {
            buf.tag_outcome(black_box(1)).unwrap();
        })
    });
    c.bench_function("schema_tag_write_category_arena", |b| {
        b.iter(|| {
            buf.tag_route(TextInput::Dynamic(route.as_ref()));
        })
    });
    let _ = fixed.wall_nanos();
}

/// Ratchet accounting cost per finished span.
fn bench_ratchet(c: &mut Criterion) {
    use lmao_core::CapacityRatchet;
    let mut r = CapacityRatchet::new(64);
    c.bench_function("ratchet_record_span", |b| {
        b.iter(|| {
            r.record_span(black_box(30));
            black_box(r.capacity())
        })
    });
}
