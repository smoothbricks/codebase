//! bitmosaic vs roaring-rs: the crate must match-or-beat the incumbent on ops
//! AND bytes, per shape. Shapes mirror the measured g98 arms.

use criterion::{Criterion, criterion_group, criterion_main};
use roaring::RoaringBitmap;
use std::hint::black_box;

fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn shapes() -> Vec<(&'static str, Vec<u32>, Vec<u32>)> {
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let sorted_unique = |v: Vec<u32>| {
        let mut v = v;
        v.sort_unstable();
        v.dedup();
        v
    };
    vec![
        (
            "two-long-runs",
            (0..4096).chain(32768..36864).collect(),
            (2048..6144).chain(34816..38912).collect(),
        ),
        (
            "skewed-run-counts",
            (0..30_000).chain(40_000..60_000).collect(),
            (0..120u32)
                .flat_map(|i| (i * 500)..(i * 500 + 200))
                .collect(),
        ),
        (
            "uniform-sparse-3k",
            sorted_unique(
                (0..3000)
                    .map(|_| xorshift(&mut state) as u32 % 500_000)
                    .collect(),
            ),
            sorted_unique(
                (0..3000)
                    .map(|_| xorshift(&mut state) as u32 % 500_000)
                    .collect(),
            ),
        ),
        (
            "strided-20k",
            (0..20_000u32).map(|i| i * 13).collect(),
            (0..20_000u32).map(|i| i * 11).collect(),
        ),
        (
            "jittered-20k",
            sorted_unique(
                (0..20_000u32)
                    .map(|i| i * 13 + (xorshift(&mut state) % 3) as u32)
                    .collect(),
            ),
            sorted_unique(
                (0..20_000u32)
                    .map(|i| i * 11 + (xorshift(&mut state) % 3) as u32)
                    .collect(),
            ),
        ),
        (
            "clustered-32k",
            (0..40u32)
                .flat_map(|c| {
                    let base = c * 100_000;
                    (0..800u32).map(move |i| base + i * 2)
                })
                .collect(),
            sorted_unique(
                (0..32_000)
                    .map(|_| xorshift(&mut state) as u32 % 4_000_000)
                    .collect(),
            ),
        ),
        (
            "dense-60k",
            sorted_unique(
                (0..60_000)
                    .map(|_| xorshift(&mut state) as u32 % 65_536)
                    .collect(),
            ),
            sorted_unique(
                (0..60_000)
                    .map(|_| xorshift(&mut state) as u32 % 65_536)
                    .collect(),
            ),
        ),
    ]
}

fn bench_ops(c: &mut Criterion) {
    for (name, va, vb) in shapes() {
        let ax_a = bitmosaic::Bitmosaic::from_sorted(va.iter().copied());
        let ax_b = bitmosaic::Bitmosaic::from_sorted(vb.iter().copied());
        let ro_a: RoaringBitmap = va.iter().copied().collect();
        let ro_b: RoaringBitmap = vb.iter().copied().collect();
        let a_bytes = ax_a.to_bytes();
        let b_bytes = ax_b.to_bytes();
        let a_view = bitmosaic::BitmosaicView::open(&a_bytes).expect("attach left");
        let b_view = bitmosaic::BitmosaicView::open(&b_bytes).expect("attach right");

        // Byte scoreboard + result oracle, once per shape.
        assert_eq!(
            ax_a.and_len(&ax_b),
            ro_a.intersection_len(&ro_b),
            "{name} AND oracle"
        );
        assert_eq!(ax_a.len(), ro_a.len(), "{name} len oracle");
        eprintln!(
            "BYTES {name}: bitmosaic heap={} wire={} roaring wire={} [census {:?}]",
            ax_a.heap_bytes(),
            a_bytes.len(),
            ro_a.serialized_size(),
            ax_a.container_census(),
        );

        let mut group = c.benchmark_group(name);
        group.bench_function("attach/bitmosaic", |b| {
            b.iter(|| black_box(bitmosaic::BitmosaicView::open(black_box(&a_bytes)).unwrap()))
        });
        group.bench_function("and_len/borrowed", |b| {
            b.iter(|| black_box(a_view.and_len(black_box(&b_view))))
        });
        group.bench_function("and_len/enumerated-control", |b| {
            b.iter(|| black_box(ax_a.iter().filter(|v| ax_b.contains(*v)).count()))
        });
        group.bench_function("or/bitmosaic", |b| {
            b.iter(|| black_box(ax_a.or(black_box(&ax_b))))
        });
        group.bench_function("or/restaged-control", |b| {
            b.iter(|| {
                let mut builder = bitmosaic::BitmosaicBuilder::new();
                builder.extend(ax_a.iter().chain(ax_b.iter()));
                black_box(builder.freeze())
            })
        });
        group.bench_function("and_len/bitmosaic", |b| {
            b.iter(|| black_box(ax_a.and_len(black_box(&ax_b))))
        });
        group.bench_function("and_len/roaring", |b| {
            b.iter(|| black_box(ro_a.intersection_len(black_box(&ro_b))))
        });
        let probes: Vec<u32> = va.iter().step_by(7).copied().collect();
        group.bench_function("contains/bitmosaic", |b| {
            b.iter(|| {
                let mut hits = 0u32;
                for p in &probes {
                    hits += ax_a.contains(black_box(*p)) as u32;
                }
                black_box(hits)
            })
        });
        group.bench_function("contains/roaring", |b| {
            b.iter(|| {
                let mut hits = 0u32;
                for p in &probes {
                    hits += ro_a.contains(black_box(*p)) as u32;
                }
                black_box(hits)
            })
        });
        group.bench_function("rank/bitmosaic", |b| {
            b.iter(|| {
                let mut acc = 0u64;
                for p in &probes {
                    acc += ax_a.rank(black_box(*p));
                }
                black_box(acc)
            })
        });
        group.bench_function("rank/roaring", |b| {
            b.iter(|| {
                let mut acc = 0u64;
                for p in &probes {
                    acc += ro_a.rank(black_box(*p));
                }
                black_box(acc)
            })
        });
        let ks: Vec<u64> = (0..ax_a.len()).step_by(97).collect();
        group.bench_function("select/bitmosaic", |b| {
            b.iter(|| {
                let mut acc = 0u64;
                for k in &ks {
                    acc += ax_a.select(black_box(*k)).unwrap() as u64;
                }
                black_box(acc)
            })
        });
        group.bench_function("select/roaring", |b| {
            b.iter(|| {
                let mut acc = 0u64;
                for k in &ks {
                    acc += ro_a.select(black_box(*k as u32)).unwrap() as u64;
                }
                black_box(acc)
            })
        });
        // Emergent surface: successor probes (roaring's cheapest equivalent
        // is rank+select), and windowed bucket counts (roaring:
        // range_cardinality per window).
        let succ_probes: Vec<u32> = {
            let mut st = 0xD1CEu64;
            (0..1_000)
                .map(|_| xorshift(&mut st) as u32 % 4_200_000)
                .collect()
        };
        group.bench_function("successor/bitmosaic", |b| {
            b.iter(|| {
                let mut acc = 0u64;
                for &v in &succ_probes {
                    acc = acc.wrapping_add(ax_a.successor(v).unwrap_or(0) as u64);
                }
                black_box(acc)
            })
        });
        group.bench_function("successor/roaring", |b| {
            b.iter(|| {
                let mut acc = 0u64;
                for &v in &succ_probes {
                    let r = ro_a.rank(v.wrapping_sub(1).min(v));
                    acc = acc.wrapping_add(ro_a.select(r as u32).unwrap_or(0) as u64);
                }
                black_box(acc)
            })
        });
        group.bench_function("bucket_counts/bitmosaic", |b| {
            b.iter(|| {
                let mut acc = 0u64;
                ax_a.bucket_counts(16, |base, count| {
                    acc = acc.wrapping_add(base as u64 ^ count);
                });
                black_box(acc)
            })
        });
        group.bench_function("bucket_counts/roaring", |b| {
            b.iter(|| {
                let mut acc = 0u64;
                let mut lo = 0u64;
                while lo < 1 << 32 {
                    let hi = lo + (1 << 16);
                    let count = ro_a.range_cardinality(lo as u32..=(hi - 1) as u32);
                    if count > 0 {
                        acc = acc.wrapping_add(lo ^ count);
                    }
                    lo = hi;
                    if ro_a.rank((hi - 1) as u32) >= ro_a.len() {
                        break;
                    }
                }
                black_box(acc)
            })
        });
        group.finish();
    }
}

fn bench_u64(c: &mut Criterion) {
    use roaring::RoaringTreemap;
    // The engine's EntityId shape: dense ordinal-derived u64 ids.
    let a: Vec<u64> = (0..1_000_000u64).map(|i| i * 7).collect();
    let b: Vec<u64> = (0..1_000_000u64).map(|i| i * 11).collect();
    let ax_a = bitmosaic::Bitmosaic64::from_sorted(a.iter().copied());
    let ax_b = bitmosaic::Bitmosaic64::from_sorted(b.iter().copied());
    let ro_a: RoaringTreemap = a.iter().copied().collect();
    let ro_b: RoaringTreemap = b.iter().copied().collect();
    assert_eq!(
        ax_a.and_len(&ax_b),
        ro_a.intersection_len(&ro_b),
        "u64 AND oracle"
    );
    assert_eq!(ax_a.len(), ro_a.len(), "u64 len oracle");
    eprintln!(
        "BYTES u64-ordinal-1m: bitmosaic64 {} B vs treemap {} B ({:.2}x)",
        ax_a.heap_bytes(),
        ro_a.serialized_size(),
        ro_a.serialized_size() as f64 / ax_a.heap_bytes() as f64,
    );
    let mut group = c.benchmark_group("u64-ordinal-1m");
    group.bench_function("and_len/bitmosaic64", |bch| {
        bch.iter(|| black_box(ax_a.and_len(black_box(&ax_b))))
    });
    group.bench_function("and_len/treemap", |bch| {
        bch.iter(|| black_box(ro_a.intersection_len(black_box(&ro_b))))
    });
    let probes: Vec<u64> = a.iter().step_by(97).copied().collect();
    group.bench_function("contains/bitmosaic64", |bch| {
        bch.iter(|| {
            let mut hits = 0u32;
            for p in &probes {
                hits += ax_a.contains(black_box(*p)) as u32;
            }
            black_box(hits)
        })
    });
    group.bench_function("contains/treemap", |bch| {
        bch.iter(|| {
            let mut hits = 0u32;
            for p in &probes {
                hits += ro_a.contains(black_box(*p)) as u32;
            }
            black_box(hits)
        })
    });
    group.bench_function("rank/bitmosaic64", |bch| {
        bch.iter(|| {
            let mut acc = 0u64;
            for p in &probes {
                acc += ax_a.rank(black_box(*p));
            }
            black_box(acc)
        })
    });
    group.bench_function("rank/treemap", |bch| {
        bch.iter(|| {
            let mut acc = 0u64;
            for p in &probes {
                acc += ro_a.rank(black_box(*p));
            }
            black_box(acc)
        })
    });
    group.bench_function("successor/bitmosaic64", |bch| {
        bch.iter(|| {
            let mut acc = 0u64;
            for p in &probes {
                acc = acc.wrapping_add(ax_a.successor(black_box(*p ^ 1)).unwrap_or(0));
            }
            black_box(acc)
        })
    });
    group.bench_function("successor/treemap", |bch| {
        bch.iter(|| {
            let mut acc = 0u64;
            for p in &probes {
                let r = ro_a.rank(black_box(*p ^ 1).wrapping_sub(1));
                acc = acc.wrapping_add(ro_a.select(r).unwrap_or(0));
            }
            black_box(acc)
        })
    });
    group.finish();
}

criterion_group!(benches, bench_ops, bench_u64);
criterion_main!(benches);
