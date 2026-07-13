//! Flush-path benchmarks — settles the "flush in Rust" verdict left open by
//! `docs/optimization-investigation.md` (approach (c): plausible but unproven).
//!
//! Comparable TS baselines (Apple M5 Max, bun/JSC, from the investigation +
//! packages/lmao benchmarks): JS Map dictionary build for a 256-string flush ≈ 3.3 µs;
//! Rust std HashMap (SipHash) measured at 5.8 µs — the FxHashMap column here is the
//! remedy under test.

use std::sync::Arc;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use lmao_arrow::{ColumnDictionary, MockSpan, convert_span_trees};
use lmao_core::{SpanIdentity, TraceId};

/// 256 observations over 64 distinct templates — same shape as the TS
/// 256-string dictionary flush bench.
fn strings_256() -> Vec<String> {
    (0..256)
        .map(|i| format!("user {{id}} did thing number {}", i % 64))
        .collect()
}

fn bench_dictionary_build(c: &mut Criterion) {
    let strings = strings_256();
    let mut g = c.benchmark_group("dictionary_build_256");

    g.bench_function("fxhashmap_column_dictionary", |b| {
        b.iter(|| {
            let mut d = ColumnDictionary::default();
            for s in &strings {
                d.observe(black_box(s));
            }
            black_box(d.finalize_indexed())
        })
    });

    g.bench_function("std_hashmap_siphash", |b| {
        b.iter(|| {
            let mut counts: std::collections::HashMap<&str, u64> = Default::default();
            for s in &strings {
                *counts.entry(black_box(s.as_str())).or_default() += 1;
            }
            let mut values: Vec<&str> = counts.keys().copied().collect();
            values.sort_unstable();
            black_box(values)
        })
    });

    g.bench_function("btreemap", |b| {
        b.iter(|| {
            let mut counts: std::collections::BTreeMap<&str, u64> = Default::default();
            for s in &strings {
                *counts.entry(black_box(s.as_str())).or_default() += 1;
            }
            black_box(counts.len())
        })
    });

    g.bench_function("vec_sort_dedup", |b| {
        b.iter(|| {
            let mut values: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();
            values.sort_unstable();
            values.dedup();
            black_box(values)
        })
    });

    g.finish();
}

/// Realistic flush tree: `spans` root spans, each with `logs` log rows drawn from a
/// 64-template pool — mirrors the convertToArrow.ts bench shapes.
fn build_flush_input(spans: usize, logs: usize) -> Vec<MockSpan> {
    let trace_id = TraceId::new("bench-trace").unwrap();
    (0..spans)
        .map(|s| {
            let identity = Arc::new(SpanIdentity {
                thread_id: 42,
                span_id: s as u32,
                trace_id: trace_id.clone(),
                parent: None,
            });
            let mut timestamps = vec![0, 1];
            let mut packed_headers = vec![1u32, 2];
            let mut messages = vec![Some(format!("span-{}", s % 8)), None];
            for i in 0..logs {
                timestamps.push(2 + i as i64);
                packed_headers.push(8);
                messages.push(Some(format!("user {{id}} did thing number {}", i % 64)));
            }
            MockSpan {
                identity,
                timestamps,
                packed_headers,
                messages,
                overflow: None,
                children: vec![],
            }
        })
        .collect()
}

fn bench_full_flush(c: &mut Criterion) {
    let mut g = c.benchmark_group("flush_to_record_batch");
    for (spans, logs) in [(8, 30), (64, 30), (256, 62)] {
        let input = build_flush_input(spans, logs);
        let rows = spans * (logs + 2);
        g.throughput(criterion::Throughput::Elements(rows as u64));
        g.bench_function(format!("{spans}spans_x_{logs}logs"), |b| {
            b.iter(|| black_box(convert_span_trees(black_box(&input)).unwrap()))
        });
    }
    g.finish();
}

criterion_group!(benches, bench_dictionary_build, bench_full_flush);
criterion_main!(benches);
