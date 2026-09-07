//! G215: does reading from the wire cost what reading from the heap costs,
//! and is the Elias-Fano root in the probe class this crate contracts for?
//!
//! `bitmosaic/README.md` measured `ptmcart::planes::MonotoneView` at **201.6 ns**
//! `contains` and **180.2 ns** `rank` against the owned forest's 0.4 / 2.7,
//! and concluded an Elias-Fano arm "needs a select structure, not a
//! `MonotoneView` wrapper". The wire format publishes an EF root wherever it
//! is strictly smaller, so that conclusion is a constraint on this code: an
//! EF probe here must be position-computed, not searched.
//!
//! ## Measurement discipline, and the reason for each part
//!
//! - **`--profile perf`.** `cargo build --release` in this workspace is
//!   `opt-level="z"`, which suppresses exactly the wins that are inlinable.
//! - **Warm up before timing.** The first version of this harness ran each
//!   arm's whole repeat set before the next arm's, and its A/A control read
//!   **+-76.3%** — it was measuring the CPU's frequency ramp, not the arms.
//! - **Interleave every arm inside each repeat**, MIN per arm across
//!   repeats, so drift and thermal state hit all arms equally.
//! - **A/A control measured IN-RUN at this profile**, from two arms running
//!   the identical closure over the identical data. No floor is inherited
//!   from another harness; the fleet's broadcast +-0.3% was an opt-z number.
//! - Timings are ratios between arms and expire when either arm is
//!   rewritten. The byte counts in `g214_wire_bytes` do not.
//!
//! Run: cargo run -p bitmosaic --profile perf --example g215_wire_ops

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
    // Warm up: pull every plane into cache and let the core reach its
    // steady clock before any sample is kept.
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

fn probe_set(values: &[u32]) -> Vec<u32> {
    // Half hits, half misses, in set order so the walk is not the subject.
    values
        .iter()
        .step_by(7)
        .copied()
        .chain(values.iter().step_by(11).map(|v| v.wrapping_add(1)))
        .collect()
}

fn panel(label: &str, values: &[u32]) {
    let owned = Bitmosaic::from_sorted(values.iter().copied());
    let bytes = owned.to_bytes();
    let view = BitmosaicView::open(&bytes).expect("open");
    let root = if view.is_elias_fano() { "ef" } else { "forest" };

    let probes = probe_set(values);
    let ks: Vec<u64> = (0..values.len() as u64).step_by(13).collect();

    // Oracle before timing: a fast wrong answer is not a result.
    let oracle: BTreeSet<u32> = values.iter().copied().collect();
    for &p in probes.iter().step_by(37) {
        assert_eq!(
            view.contains(p),
            oracle.contains(&p),
            "{label}: contains oracle"
        );
        assert_eq!(
            view.rank(p),
            oracle.range(..p).count() as u64,
            "{label}: rank oracle"
        );
    }

    let np = probes.len();
    let c = race(
        np,
        &mut [
            &mut || probes.iter().filter(|&&v| owned.contains(v)).count() as u64,
            &mut || probes.iter().filter(|&&v| view.contains(v)).count() as u64,
            &mut || {
                probes
                    .iter()
                    .filter(|&&v| values.binary_search(&v).is_ok())
                    .count() as u64
            },
        ],
    );
    let r = race(
        np,
        &mut [
            &mut || probes.iter().map(|&v| owned.rank(v)).sum(),
            &mut || probes.iter().map(|&v| view.rank(v)).sum(),
        ],
    );
    let s = race(
        ks.len(),
        &mut [
            &mut || ks.iter().filter_map(|&k| owned.select(k)).count() as u64,
            &mut || ks.iter().filter_map(|&k| view.select(k)).count() as u64,
        ],
    );

    println!(
        "| `{label}` | {root} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} |",
        c[0], c[1], c[2], r[0], r[1], s[0], s[1]
    );
}

fn main() {
    // ── floor, measured here, at this profile, before anything else ──
    let corpus = uniform(13_000_000, 10_000, 5);
    let plane = Bitmosaic::from_sorted(corpus.iter().copied()).to_bytes();
    let view = BitmosaicView::open(&plane).unwrap();
    let probes = probe_set(&corpus);
    let aa = race(
        probes.len(),
        &mut [
            &mut || probes.iter().filter(|&&v| view.contains(v)).count() as u64,
            &mut || probes.iter().filter(|&&v| view.contains(v)).count() as u64,
        ],
    );
    let floor = (aa[0].max(aa[1]) / aa[0].min(aa[1]) - 1.0) * 100.0;
    println!("## A/A control — identical closure, identical data, two labels\n");
    println!(
        "arm A {:.3} ns/probe, arm B {:.3} ns/probe -> **floor +-{floor:.1}%** \
         (MIN of {REPS} interleaved, --profile perf, {} probes/rep, measured in-run; \
         do not inherit this).\n",
        aa[0],
        aa[1],
        probes.len()
    );

    println!("## ns per probe, MIN of {REPS} interleaved repeats\n");
    println!(
        "| shape | root | contains owned | contains **view** | contains flat | rank owned | rank **view** | select owned | select **view** |"
    );
    println!("| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |");
    panel(
        "stride7/n20000",
        &(0..20_000u32).map(|i| i * 7).collect::<Vec<_>>(),
    );
    panel("u20k/n8000", &uniform(20_000, 8_000, 0xA1 + 8_000));
    panel("u20k/n1000", &uniform(20_000, 1_000, 0xA1 + 1_000));
    panel("u13m/n10000", &uniform(13_000_000, 10_000, 0xB2 + 10_000));
    panel(
        "u13m/n100000",
        &uniform(13_000_000, 100_000, 0xB2 + 100_000),
    );
    // The engine's real entity domain: dense ordinals with attribute idents
    // consuming some of them, so the run is broken and the chunks ladder to
    // Words. This is the shape `EntityDict::entity()` indexes, and select in
    // a loop is exactly what it does.
    panel(
        "gapped/n10000",
        &(1..=10_300u32).filter(|i| i % 37 != 0).collect::<Vec<_>>(),
    );
    panel(
        "gapped/n100000",
        &(1..=103_000u32).filter(|i| i % 37 != 0).collect::<Vec<_>>(),
    );
    println!(
        "\nThe class the EF root has to clear, from README's published \
         `MonotoneView` figures on the same ops: contains 201.6 ns, rank 180.2 ns."
    );
}
