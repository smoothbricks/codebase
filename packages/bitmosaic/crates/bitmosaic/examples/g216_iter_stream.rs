//! G216: what does a full ascending walk of a frozen set cost, per element,
//! against the uncompressed array it is competing with?
//!
//! `bitmosaic/README.md` recorded `Bitmosaic::iter()` as select-driven — one
//! `Container::select` per element — and measured **18.0 ns/element** on
//! `u20k/n8000`, a single `Words` chunk, against **0.06** for a flat
//! `Vec<u32>`. That is the defect `ptmcart::planes::MonotoneIter` had
//! (`10b73772`): `select` on a directory-guided container re-enters the
//! group seek and rescans from a group boundary for every element, so an
//! O(n) sequence costs O(n · groupwidth) to produce.
//!
//! The four arms raced here are the four ways to read the same values:
//!
//! - **stream** — `Bitmosaic::iter()`, the cursor form.
//! - **select** — `Bitmosaic::select(k)` for `k` in `0..len`, the positional
//!   walk. On the single-chunk cells this is the prior `iter()` within one
//!   cumulative-plane compare, so it is the 18.0 control, measured in this
//!   binary rather than inherited from a docs table.
//! - **view** — `BitmosaicView::for_each` over the wire form, which already
//!   streamed and is the crate's existence proof that the shape is reachable.
//! - **flat** — `Vec<u32>`, the uncompressed target. Not a strawman: the
//!   README's fit criterion says a positional consumer should keep the array,
//!   and this is the number that claim is made against.
//!
//! ## Measurement discipline, and the reason for each part
//!
//! - **`--profile perf`.** `cargo build --release` here is `opt-level="z"`
//!   plus `strip="symbols"`, which suppresses exactly the inlining a cursor
//!   depends on. Check the binary, not the intent:
//!   `nm target/perf/examples/g216_iter_stream | wc -l` reads ~740 at this
//!   profile and 68 at `--release`.
//! - **Every comparison that decides something is IN-BINARY.** `view` and
//!   `flat` sit in the same table as `stream` for that reason. Cross-build
//!   deltas on this workload did not survive scrutiny: while this harness was
//!   being written, `Bitmosaic::andnot` on a dense pair measured 1.36-1.56
//!   ns/element in one build and 0.61-0.65 in another **from byte-identical
//!   source** — cleanly separated distributions, 2.2x apart. The cause is
//!   unresolved and deliberately not asserted here: the two batches were run
//!   back to back rather than interleaved, on a machine with other work on
//!   it, so code layout and machine load are both live explanations and this
//!   harness cannot separate them. Either way the conclusion is the same —
//!   a two-build delta on this workload is not evidence. Anything under that
//!   gap needs a same-binary arm or a source-level argument.
//! - **A count beats a timing wherever both exist.** Nothing on this page is
//!   load-immune. Byte counts, allocation counts, `nm`, `size_of` and the
//!   container census are; prefer them, and reach for this harness only for
//!   the question it is the only instrument for.
//! - **A/A control measured IN-RUN at this profile**, two arms running the
//!   identical closure over the identical data. No floor is inherited, and an
//!   in-run A/A does not certify a quiet machine — it only bounds drift
//!   between two arms that saw the same conditions.
//! - **Interleave every arm inside each repeat**, MIN per arm, so drift and
//!   thermal state hit all arms equally.
//! - **Oracle before timing** on every cell: all four arms must produce the
//!   same sequence, and that also settles the README's open question of
//!   whether `BitmosaicView::for_each` and the owned `iter()` agree element for
//!   element. That part is load-immune and is the durable half of this file.
//!
//! Run: cargo run -p bitmosaic --profile perf --example g216_iter_stream

use std::collections::BTreeSet;
use std::hint::black_box;
use std::time::Instant;

use bitmosaic::{Bitmosaic, BitmosaicView};

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

const REPS: u32 = 15;

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

/// Checksum shape shared by all four arms, so none of them can win by
/// producing fewer values than the others.
fn fold(sum: u64, v: u32) -> u64 {
    sum.wrapping_add(u64::from(v))
}

fn panel(label: &str, values: &[u32]) {
    let owned = Bitmosaic::from_sorted(values.iter().copied());
    let bytes = owned.to_bytes();
    let view = BitmosaicView::open(&bytes).expect("open");
    let (stride, cone, array, words, runs) = owned.container_census();
    let census = format!("{stride}s/{cone}c/{array}a/{words}w/{runs}r");
    let n = values.len();

    // Oracle before timing: a fast wrong answer is not a result. This also
    // answers the README's open question about view/owned agreement.
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
    // (Exactness of `size_hint` is pinned by the unit oracle, not here: this
    // file must compile against both the control and treatment libraries.)

    // Each timed sample must be far above the clock's granularity. A single
    // 1,000-element walk at 0.04 ns/element is ~40 ns, which is one tick of
    // `Instant` on this machine: the Array cell read exactly 0.000 in one run
    // before this loop existed. Repeat until a sample is ~100 us.
    //
    // Every arm's INPUT goes through `black_box`, not just its result: a walk
    // over an unchanging set is loop-invariant, and an opaque result does not
    // stop LLVM from hoisting the walk itself out of the repeat.
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
                (0..inner)
                    .map(|_| {
                        let o = black_box(&owned);
                        (0..o.len()).map(|k| o.select(k).unwrap()).fold(0u64, fold)
                    })
                    .sum()
            },
            &mut || {
                let mut sum = 0u64;
                for _ in 0..inner {
                    black_box(&view).for_each(|v| sum = fold(sum, v));
                }
                sum
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
        t[1] / t[0],
        t[0] / t[3],
    );
}

/// Near-arithmetic with a bounded residual: the Cone arm's home ground,
/// under `CONE_MAX_LEN` so the ladder is allowed to pick it.
fn coned(chunks: u32, per_chunk: u32) -> Vec<u32> {
    (0..chunks)
        .flat_map(|c| {
            let base = c * 65_536;
            (0..per_chunk).map(move |i| base + i * 61 + i % 7)
        })
        .collect()
}

fn main() {
    // ── floor, measured here, at this profile, before anything else ──
    let corpus = uniform(20_000, 8_000, 0xA1 + 8_000);
    let owned = Bitmosaic::from_sorted(corpus.iter().copied());
    let aa_inner = 2_000_000 / corpus.len();
    let mut aa_arm = || {
        (0..aa_inner)
            .map(|_| black_box(&owned).iter().fold(0u64, fold))
            .sum::<u64>()
    };
    let mut aa_arm_b = || {
        (0..aa_inner)
            .map(|_| black_box(&owned).iter().fold(0u64, fold))
            .sum::<u64>()
    };
    let aa = race(corpus.len() * aa_inner, &mut [&mut aa_arm, &mut aa_arm_b]);
    let floor = (aa[0].max(aa[1]) / aa[0].min(aa[1]) - 1.0) * 100.0;
    println!("## A/A control — identical closure, identical data, two labels\n");
    println!(
        "arm A {:.3} ns/element, arm B {:.3} ns/element -> **floor +-{floor:.2}%** \
         (MIN of {REPS} interleaved, --profile perf, {} elements/rep, measured in-run \
         on `u20k/n8000`; do not inherit this).\n",
        aa[0],
        aa[1],
        corpus.len()
    );

    println!("## ns per element, full ascending walk, MIN of {REPS} interleaved repeats\n");
    println!(
        "| shape | census | n | **stream** | select | view | flat `Vec` | **stream vs select** | stream vs flat |"
    );
    println!("| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |");
    // The subject: one Words chunk, the cell the 18.0 figure was taken on.
    panel("u20k/n8000", &corpus);
    // Words again, multi-chunk, and the engine's real gapped ordinal domain.
    panel(
        "gapped/n100000",
        &(1..=103_000u32).filter(|i| i % 37 != 0).collect::<Vec<_>>(),
    );
    // The three arms that were already cheap and must not regress.
    panel(
        "stride7/n20000",
        &(0..20_000u32).map(|i| i * 7).collect::<Vec<_>>(),
    );
    panel("u20k/n1000", &uniform(20_000, 1_000, 0xA1 + 1_000));
    panel("cone/n8000", &coned(8, 1_000));
    println!(
        "\nREADME's prior figures on this walk: 18.0 ns/element on `u20k/n8000`, \
         0.04-0.23 for the Array/Stride shapes, 0.06 for a flat `Vec<u32>`."
    );
}
