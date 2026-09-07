//! G217: what does the D-style range protocol cost, against the
//! hand-written alloc-free kernels it generalises and against the
//! `Peekable` merge it exists to avoid?
//!
//! Three questions, and the ONLY one that is genuinely open is the third.
//!
//! 1. **Does the lazy `AndRange` replace `and_len`?** Predicted no, and the
//!    prediction is structural rather than a hunch: `and_len` dispatches a
//!    per-chunk kernel that never looks at a member — closed form for
//!    stride∩stride, `ldp q`/`cnt.16b`/`vpadal` word popcount for dense —
//!    while a range advances a cursor per element. This measures the size of
//!    the gap so the docs can say which to reach for and why, instead of
//!    presenting two APIs and shrugging.
//! 2. **Did the cursor rewrite regress the walk?** `Bitmosaic::iter` was
//!    already streaming before this change; what changed is WHEN the `Words`
//!    arm refills (`pop_front` normalises eagerly so `front` is idempotent,
//!    where the old `next` refilled lazily). Same loads, one step earlier.
//!    The load-immune half of that claim is the word census in
//!    `a_words_walk_costs_its_span_where_select_costs_its_cardinality`,
//!    which is an equality and did not move. This panel is the wall-clock
//!    corroboration, and it is deliberately the same cells and the same arm
//!    set as `g216_iter_stream.rs` so the RATIOS are comparable across the
//!    two builds even though the raw ns are not (see below).
//! 3. **Is the protocol worth it — `front`/`pop_front` against
//!    `Peekable`?** This is the claim the module doc makes, and it is the
//!    one that must be measured in-binary rather than argued: the same
//!    three-way merge, over the same two forests, written once against
//!    idempotent `front` and once against `Peekable<RangeIter>`. `Peekable`
//!    buffers an `Option<T>` that is a copy of state the cursor already
//!    holds; whether LLVM removes that buffer is not knowable from the
//!    source.
//!
//! ## Measurement discipline, inherited from `g216_iter_stream.rs`
//!
//! - **`--profile perf`.** `--release` here is `opt-level="z"` + stripped,
//!   which suppresses exactly the inlining a cursor depends on. `--release`
//!   is fine for COUNTS and must never be quoted for a timing.
//! - **Every comparison that decides something is IN-BINARY.** g216 recorded
//!   two builds of byte-identical source disagreeing 2.2x on this workload,
//!   cause unresolved. So panels 1 and 3 race their arms inside one process,
//!   and the only cross-build number reported anywhere is a RATIO against
//!   its own in-binary control.
//! - **A/A floor measured in-run** at this profile, two arms running the
//!   identical closure over the identical data. An in-run A/A bounds drift
//!   between arms that saw the same conditions; it does not certify a quiet
//!   machine.
//! - **Interleave every arm inside each repeat, MIN per arm.**
//! - **Oracle before timing on every cell.** A fast wrong answer is not a
//!   result, and every arm here is a different implementation of the same
//!   set operation, so they can check each other.
//!
//! Run: cargo run -p bitmosaic --profile perf --example g217_ranges

use std::collections::BTreeSet;
use std::hint::black_box;
use std::time::Instant;

use bitmosaic::{Bitmosaic, BitmosaicView, Range};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
}

fn uniform(universe: u32, n: usize, seed: u64) -> Vec<u32> {
    let mut rng = Rng(seed);
    let mut seen = BTreeSet::new();
    while seen.len() < n {
        seen.insert((rng.next() % u64::from(universe)) as u32);
    }
    seen.into_iter().collect()
}

/// Near-arithmetic with a bounded residual: the Cone arm's home ground.
fn coned(chunks: u32, per_chunk: u32, skew: u32) -> Vec<u32> {
    (0..chunks)
        .flat_map(|c| {
            let base = c * 65_536;
            (0..per_chunk).map(move |i| base + i * 61 + (i + skew) % 7)
        })
        .collect()
}

const REPS: u32 = 15;

/// PRE-REGISTERED, before any number was seen: a delta counts as REAL only
/// at this multiple of its own same-session like-arm null. Below it the cell
/// is UNRESOLVED and no direction is reported for it.
///
/// It is a constant in the source rather than a judgement applied to the
/// output because a threshold chosen after seeing the numbers is not a
/// threshold. This box is 6 P-cores + 12 E-cores under a shared load, and
/// macOS will put a bench thread on an E-core at roughly half the
/// performance — which is a bimodal, systematic bias, not noise that
/// averaging removes.
const VERDICT_BAR: f64 = 2.0;

/// Race every arm inside one repeat loop, MIN per arm. Arms are `dyn` so
/// they share one call shape — an arm inlined into the timing loop while its
/// rival is not would be the same defect as measuring at two profiles.
fn race(per_call: usize, arms: &mut [&mut dyn FnMut() -> u64]) -> Vec<f64> {
    let mut best = vec![f64::INFINITY; arms.len()];
    for _ in 0..3 {
        for arm in arms.iter_mut() {
            black_box(arm());
        }
    }
    for _ in 0..REPS {
        for (i, arm) in arms.iter_mut().enumerate() {
            let t = Instant::now();
            black_box(arm());
            let ns = t.elapsed().as_secs_f64() * 1e9 / per_call as f64;
            if ns < best[i] {
                best[i] = ns;
            }
        }
    }
    best
}

fn fold(sum: u64, v: u32) -> u64 {
    sum.wrapping_add(u64::from(v))
}

/// The merge this protocol exists for, written against `Iterator`.
///
/// `Peekable` is not a strawman choice: it is the ONLY way to express "look
/// at the head without consuming it" over `Iterator`, which is what a
/// three-way merge needs. The buffered `Option<u32>` it adds per source is a
/// copy of state the cursor already holds.
fn peekable_and_sum(a: &Bitmosaic, b: &Bitmosaic) -> u64 {
    let mut x = a.iter().peekable();
    let mut y = b.iter().peekable();
    let mut sum = 0u64;
    loop {
        let (Some(&u), Some(&v)) = (x.peek(), y.peek()) else {
            return sum;
        };
        if u < v {
            x.next();
        } else if v < u {
            y.next();
        } else {
            sum = fold(sum, u);
            x.next();
            y.next();
        }
    }
}

/// The same merge over the range protocol, spelled out rather than routed
/// through `AndRange`, so panel 3 compares two merge BODIES and not a merge
/// body against an adaptor's extra layer.
fn range_and_sum(a: &Bitmosaic, b: &Bitmosaic) -> u64 {
    let (mut x, mut y) = (a.range(), b.range());
    let mut sum = 0u64;
    while !x.empty() && !y.empty() {
        let (u, v) = (x.front(), y.front());
        if u < v {
            x.pop_front();
        } else if v < u {
            y.pop_front();
        } else {
            sum = fold(sum, u);
            x.pop_front();
            y.pop_front();
        }
    }
    sum
}

// ── panel 1: the full ascending walk ─────────────────────────────────────

fn walk_panel(label: &str, values: &[u32]) {
    let owned = Bitmosaic::from_sorted(values.iter().copied());
    let bytes = owned.to_bytes();
    let view = BitmosaicView::open(&bytes).expect("open");
    let (stride, cone, array, words, runs) = owned.container_census();
    let census = format!("{stride}s/{cone}c/{array}a/{words}w/{runs}r");
    let n = values.len();

    let streamed: Vec<u32> = owned.iter().collect();
    assert_eq!(streamed, values, "{label}: stream vs input");
    assert_eq!(
        streamed,
        (0..owned.len())
            .map(|k| owned.select(k).unwrap())
            .collect::<Vec<_>>(),
        "{label}: stream vs select"
    );
    let mut walked = Vec::with_capacity(n);
    view.for_each(|v| walked.push(v));
    assert_eq!(walked, streamed, "{label}: view for_each vs owned iter");

    // Each timed sample must sit far above the clock's granularity, and the
    // INPUT goes through `black_box` too: a walk over an unchanging set is
    // loop-invariant, and an opaque result alone does not stop LLVM hoisting
    // the walk out of the repeat.
    let inner = (2_000_000 / n).max(1);
    let t = race(
        n * inner,
        &mut [
            &mut || {
                (0..inner)
                    .map(|_| black_box(&owned).iter().fold(0u64, fold))
                    .sum()
            },
            &mut || {
                let mut sum = 0u64;
                for _ in 0..inner {
                    let mut r = black_box(&owned).range();
                    while !r.empty() {
                        sum = fold(sum, r.front());
                        r.pop_front();
                    }
                }
                sum
            },
            &mut || {
                (0..inner)
                    .map(|_| {
                        let o = black_box(&owned);
                        (0..o.len()).map(|k| o.select(k).unwrap()).fold(0u64, fold)
                    })
                    .sum()
            },
            &mut || {
                (0..inner)
                    .map(|_| black_box(values).iter().copied().fold(0u64, fold))
                    .sum()
            },
        ],
    );

    println!(
        "| `{label}` | {census} | {n} | **{:.3}** | {:.3} | {:.3} | {:.3} | **{:.1}x** | {:.1}x |",
        t[0],
        t[1],
        t[2],
        t[3],
        t[2] / t[0],
        t[0] / t[3],
    );
}

// ── panel 2: consuming an intersection ───────────────────────────────────

fn and_panel(label: &str, va: &[u32], vb: &[u32]) {
    let a = Bitmosaic::from_sorted(va.iter().copied());
    let b = Bitmosaic::from_sorted(vb.iter().copied());
    let (sa, _, _, wa, ra) = a.container_census();
    let census = format!("{sa}s/{wa}w/{ra}r");

    // Oracle: four independent implementations of the same cardinality.
    let truth = a.and_range(&b).count() as u64;
    assert_eq!(truth, a.and_len(&b), "{label}: and_len");
    assert_eq!(truth, a.and(&b).len(), "{label}: materialised and");
    let reference: BTreeSet<u32> = va.iter().copied().collect();
    let other: BTreeSet<u32> = vb.iter().copied().collect();
    assert_eq!(
        truth as usize,
        reference.intersection(&other).count(),
        "{label}: BTreeSet"
    );

    // Normalised by the MERGE's input size, which is the work a sequence
    // merge does. `and_len` does less than that by construction — that is
    // the finding, not a normalisation error.
    let steps = va.len() + vb.len();
    let inner = (2_000_000 / steps).max(1);
    let t = race(
        steps * inner,
        &mut [
            &mut || {
                (0..inner)
                    .map(|_| black_box(&a).and_len(black_box(&b)))
                    .sum()
            },
            &mut || {
                (0..inner)
                    .map(|_| black_box(&a).and_range(black_box(&b)).count() as u64)
                    .sum()
            },
            &mut || {
                (0..inner)
                    .map(|_| black_box(&a).and(black_box(&b)).len())
                    .sum()
            },
        ],
    );

    println!(
        "| `{label}` | {census} | {steps} | {truth} | **{:.3}** | {:.3} | {:.3} | **{:.1}x** |",
        t[0],
        t[1],
        t[2],
        t[1] / t[0],
    );
}

// ── panel 3: the protocol against Peekable ───────────────────────────────

fn merge_panel(label: &str, va: &[u32], vb: &[u32]) {
    let a = Bitmosaic::from_sorted(va.iter().copied());
    let b = Bitmosaic::from_sorted(vb.iter().copied());
    let (sa, ca, aa, wa, ra) = a.container_census();
    let census = format!("{sa}s/{ca}c/{aa}a/{wa}w/{ra}r");

    let truth = range_and_sum(&a, &b);
    assert_eq!(truth, peekable_and_sum(&a, &b), "{label}: peekable merge");
    assert_eq!(
        truth,
        a.and_range(&b).fold(0u64, fold),
        "{label}: AndRange adaptor"
    );

    let steps = va.len() + vb.len();
    let inner = (2_000_000 / steps).max(1);
    // Arm 3 is a BYTE-IDENTICAL copy of arm 0. It is the like-arm null, and
    // it is what establishes the floor FOR THIS RUN: two arms that cannot
    // differ, racing under exactly the conditions the real arms saw. A delta
    // that does not clear this floor by the pre-registered factor is
    // UNRESOLVED, and no direction is reported for it.
    let t = race(
        steps * inner,
        &mut [
            &mut || {
                (0..inner)
                    .map(|_| range_and_sum(black_box(&a), black_box(&b)))
                    .sum()
            },
            &mut || {
                (0..inner)
                    .map(|_| peekable_and_sum(black_box(&a), black_box(&b)))
                    .sum()
            },
            &mut || {
                (0..inner)
                    .map(|_| black_box(&a).and_range(black_box(&b)).fold(0u64, fold))
                    .sum()
            },
            &mut || {
                (0..inner)
                    .map(|_| range_and_sum(black_box(&a), black_box(&b)))
                    .sum()
            },
        ],
    );

    let null = (t[3].max(t[0]) / t[3].min(t[0]) - 1.0) * 100.0;
    let delta = (t[1] / t[0] - 1.0) * 100.0;
    let factor = if null > 0.0 {
        delta.abs() / null
    } else {
        f64::INFINITY
    };
    let verdict = if factor >= VERDICT_BAR {
        "**REAL**"
    } else {
        "unresolved"
    };
    println!(
        "| `{label}` | {census} | {steps} | **{:.3}** | {:.3} | {:.3} | {:+.2}% | {null:.2}% | {factor:.2}x | {verdict} |",
        t[0], t[1], t[2], delta,
    );
}

// ── panel 4: filling a caller buffer ─────────────────────────────────────

fn fill_panel(label: &str, va: &[u32], vb: &[u32]) {
    let a = Bitmosaic::from_sorted(va.iter().copied());
    let b = Bitmosaic::from_sorted(vb.iter().copied());
    let (bytes_a, bytes_b) = (a.to_bytes(), b.to_bytes());
    let view_a = BitmosaicView::open(&bytes_a).expect("open a");
    let view_b = BitmosaicView::open(&bytes_b).expect("open b");

    let n = a.and_len(&b) as usize;
    let mut dest = vec![0u32; n];
    let written = view_a.and_into(&view_b, &mut dest);
    assert_eq!(written, n, "{label}: and_into sizing");
    let mut extended: Vec<u32> = Vec::with_capacity(n);
    extended.extend(a.and_range(&b));
    assert_eq!(extended, dest, "{label}: extend(range) vs and_into");

    // Both arms write into a buffer that already exists — the closed-form
    // sizing contract `and_into` was built around, applied to both so the
    // comparison is not "one of them allocates".
    let steps = va.len() + vb.len();
    let inner = (2_000_000 / steps).max(1);
    let mut buf_a = vec![0u32; n];
    let mut buf_b: Vec<u32> = Vec::with_capacity(n);
    let t = race(
        steps * inner,
        &mut [
            &mut || {
                let mut total = 0u64;
                for _ in 0..inner {
                    total += black_box(&view_a).and_into(black_box(&view_b), &mut buf_a) as u64;
                }
                total
            },
            &mut || {
                let mut total = 0u64;
                for _ in 0..inner {
                    buf_b.clear();
                    buf_b.extend(black_box(&a).and_range(black_box(&b)));
                    total += buf_b.len() as u64;
                }
                total
            },
        ],
    );

    println!(
        "| `{label}` | {steps} | {n} | {:.3} | **{:.3}** | **{:.2}x** |",
        t[0],
        t[1],
        t[0] / t[1],
    );
}

fn main() {
    let dense_a = uniform(20_000, 8_000, 0xA1 + 8_000);
    let dense_b = uniform(20_000, 8_000, 0xB7 + 8_000);
    let gapped: Vec<u32> = (1..=103_000u32).filter(|i| i % 37 != 0).collect();
    let gapped_b: Vec<u32> = (1..=103_000u32).filter(|i| i % 41 != 0).collect();
    let stride7: Vec<u32> = (0..20_000u32).map(|i| i * 7).collect();
    let stride14: Vec<u32> = (0..10_000u32).map(|i| i * 14).collect();
    let sparse_a = uniform(20_000, 1_000, 0xA1 + 1_000);
    let sparse_b = uniform(20_000, 1_000, 0xC3 + 1_000);
    let cone_a = coned(8, 1_000, 0);
    let cone_b = coned(8, 1_000, 3);

    // ── floor, measured here, at this profile, before anything else ──
    let owned = Bitmosaic::from_sorted(dense_a.iter().copied());
    let aa_inner = 2_000_000 / dense_a.len();
    let mut aa_a = || {
        (0..aa_inner)
            .map(|_| black_box(&owned).iter().fold(0u64, fold))
            .sum::<u64>()
    };
    let mut aa_b = || {
        (0..aa_inner)
            .map(|_| black_box(&owned).iter().fold(0u64, fold))
            .sum::<u64>()
    };
    let aa = race(dense_a.len() * aa_inner, &mut [&mut aa_a, &mut aa_b]);
    let floor = (aa[0].max(aa[1]) / aa[0].min(aa[1]) - 1.0) * 100.0;
    println!("## A/A control — identical closure, identical data, two labels\n");
    println!(
        "arm A {:.3} ns/element, arm B {:.3} ns/element -> **floor +-{floor:.2}%** \
         (MIN of {REPS} interleaved, --profile perf, {} elements/rep, measured in-run \
         on `u20k/n8000`; do not inherit this).\n",
        aa[0],
        aa[1],
        dense_a.len()
    );

    println!("## 1. Full ascending walk, ns/element, MIN of {REPS} interleaved repeats\n");
    println!(
        "The cursor rewrite's corroboration. `iter` is the bridge, `primitives` is \
         the same walk driven by `empty`/`front`/`pop_front` directly — if the \
         protocol cost anything at the source tier, the two would separate.\n"
    );
    println!(
        "| shape | census | n | **iter** | primitives | select | flat `Vec` | **iter vs select** | iter vs flat |"
    );
    println!("| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |");
    walk_panel("u20k/n8000", &dense_a);
    walk_panel("gapped/n100000", &gapped);
    walk_panel("stride7/n20000", &stride7);
    walk_panel("u20k/n1000", &sparse_a);
    walk_panel("cone/n8000", &cone_a);

    println!("\n## 2. Consuming an intersection, ns per INPUT element\n");
    println!(
        "`and_len` never looks at a member; the range does. Normalised by \
         `|a| + |b|`, the work a sequence merge does.\n"
    );
    println!(
        "| cell | census | steps | result | **and_len** | and_range().count() | and().len() | **range/and_len** |"
    );
    println!("| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: |");
    and_panel("u20k dense pair", &dense_a, &dense_b);
    and_panel("gapped/n100000 pair", &gapped, &gapped_b);
    and_panel("stride7 vs stride14", &stride7, &stride14);
    and_panel("u20k sparse pair", &sparse_a, &sparse_b);
    and_panel("cone pair", &cone_a, &cone_b);

    println!("\n## 3. The protocol against `Peekable`, ns per INPUT element\n");
    println!(
        "Same merge body twice: idempotent `front` against a buffered peek. \
         `AndRange` rides along to price the adaptor layer over a hand merge. \
         A FOURTH arm, byte-identical to the first, is the like-arm null and \
         sets this run's floor; `peek/front` is REAL only at \
         {VERDICT_BAR:.0}x that floor, pre-registered.\n"
    );
    println!(
        "| cell | census | steps | **front/pop_front** | `Peekable` | `AndRange` | **peek vs front** | null | factor | verdict |"
    );
    println!("| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |");
    merge_panel("u20k dense pair", &dense_a, &dense_b);
    merge_panel("gapped/n100000 pair", &gapped, &gapped_b);
    merge_panel("stride7 vs stride14", &stride7, &stride14);
    merge_panel("u20k sparse pair", &sparse_a, &sparse_b);
    merge_panel("cone pair", &cone_a, &cone_b);

    println!("\n## 4. Filling a caller-sized buffer, ns per INPUT element\n");
    println!(
        "`BitmosaicView::and_into` enumerates the smaller side against the larger \
         side's O(1) probe; `extend(range)` is a merge. Both write into a buffer \
         that already exists.\n"
    );
    println!("| cell | steps | out | and_into (view) | **extend(range)** | **and_into/extend** |");
    println!("| :--- | ---: | ---: | ---: | ---: | ---: |");
    fill_panel("u20k dense pair", &dense_a, &dense_b);
    fill_panel("gapped/n100000 pair", &gapped, &gapped_b);
    fill_panel("stride7 vs stride14", &stride7, &stride14);
    fill_panel("u20k sparse pair", &sparse_a, &sparse_b);
    fill_panel("cone pair", &cone_a, &cone_b);
}
