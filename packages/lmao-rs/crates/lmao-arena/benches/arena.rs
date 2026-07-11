//! Arena micro-benchmarks: alloc/free churn, tier churn, and the span hot path
//! over arena blocks. Shapes mirror benchmarks/js-vs-wasm.bench.ts in
//! packages/lmao so numbers are comparable across JS / Zig-WASM / Rust-WASM /
//! native Rust.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use lmao_arena::raw::{self};
use lmao_arena::{Arena, SizeClass};

fn alloc_free_churn(c: &mut Criterion) {
    let mut g = c.benchmark_group("arena");

    // Steady-state freelist reuse: the warm production path.
    g.bench_function("alloc_free_reuse_span_system_64", |b| {
        let mut arena = Arena::new(1 << 22);
        // warm: seed the freelist
        let off = arena.alloc(SizeClass::SpanSystem, 64);
        arena.free(off, SizeClass::SpanSystem, 64);
        b.iter(|| {
            let off = arena.alloc(SizeClass::SpanSystem, black_box(64));
            arena.free(off, SizeClass::SpanSystem, 64);
            black_box(off)
        });
    });

    // Tier churn: alternating capacities forces split/merge traffic.
    g.bench_function("tier_churn_split_merge", |b| {
        let mut arena = Arena::new(1 << 22);
        b.iter(|| {
            let small_a = arena.alloc(SizeClass::Col8B, 64);
            let small_b = arena.alloc(SizeClass::Col8B, 64);
            arena.free(small_a, SizeClass::Col8B, 64);
            arena.free(small_b, SizeClass::Col8B, 64); // merges to 128
            let big = arena.alloc(SizeClass::Col8B, 128); // reuses merged
            arena.free(big, SizeClass::Col8B, 128);
            black_box(big)
        });
    });

    // Identity block churn (fixed-size freelist).
    g.bench_function("identity_alloc_free", |b| {
        let mut arena = Arena::new(1 << 22);
        let seed = arena.alloc_identity();
        arena.free_identity(seed);
        b.iter(|| {
            let id = arena.alloc_identity();
            arena.free_identity(id);
            black_box(id)
        });
    });

    g.finish();
}

fn span_hot_path(c: &mut Criterion) {
    let mut g = c.benchmark_group("span_over_arena");

    // Matches js-vs-wasm.bench.ts "trace with tags" shape: span_start + 6 column
    // writes + span_end, on pre-allocated blocks (steady state).
    g.bench_function("span_start_6tags_end", |b| {
        let mut arena = Arena::new(1 << 22);
        let cap = 64u32;
        let system = arena.alloc(SizeClass::SpanSystem, cap);
        let identity = arena.alloc_identity();
        let root = arena.alloc(SizeClass::Col8B, 8);
        let cols: Vec<u32> = (0..6).map(|_| arena.alloc(SizeClass::Col8B, cap)).collect();
        let m = arena.mem_mut();
        raw::init_trace_root(m, root, 1_000.0, 0.0);
        let mut now = 0.0f64;
        b.iter(|| {
            now += 0.001;
            raw::span_start(m, system, identity, root, cap, now);
            for (i, &col) in cols.iter().enumerate() {
                raw::write_col_f64(m, col, 0, i as f64, cap);
            }
            raw::span_end(m, system, root, cap, raw::ENTRY_TYPE_SPAN_OK, now);
            black_box(raw::read_timestamp(m, system, 1))
        });
    });

    // Matches "50 log entries" marginal-cost shape.
    g.bench_function("write_log_entry_50", |b| {
        let mut arena = Arena::new(1 << 22);
        let cap = 64u32;
        let system = arena.alloc(SizeClass::SpanSystem, cap);
        let identity = arena.alloc_identity();
        let root = arena.alloc(SizeClass::Col8B, 8);
        let m = arena.mem_mut();
        raw::init_trace_root(m, root, 1_000.0, 0.0);
        b.iter(|| {
            raw::span_start(m, system, identity, root, cap, 1.0);
            for i in 0..50u32 {
                raw::write_log_entry(m, system, identity, root, 5, cap, 1.0 + i as f64);
            }
            black_box(raw::read_write_index(m, identity))
        });
    });

    g.finish();
}

criterion_group!(benches, alloc_free_churn, span_hot_path);
criterion_main!(benches);
