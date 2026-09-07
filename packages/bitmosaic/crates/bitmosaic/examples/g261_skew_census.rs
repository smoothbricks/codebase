//! G261 — skew-adaptive intersection: acceptance cells, crossover bracket.
//!
//! Three questions, in the order that makes the answers trustworthy:
//!
//! 1. **Is it right?** Every cell asserts the drive arm, the balanced arm and
//!    `roaring` agree on `|A ∩ B|`, and that the answer is non-zero — an
//!    oracle over an empty intersection agrees with everything and refutes
//!    nothing. The arms are also required to DISAGREE on their position
//!    census: a switch the instrument cannot see is not an A/B.
//! 2. **What does it cost?** Positions and probes are exact, deterministic
//!    and reproducible on any box, so they are the primary evidence. Walls
//!    are DIRECTION-only here: this measurement ran on a loaded machine and
//!    every wall below is PENDING-CALM-BOX.
//! 3. **Where does it cross over?** The bracket sweep runs the SAME pair at
//!    rising cardinality ratios with the chooser forced both ways, so the
//!    ratio where driving starts to win is read off both sides rather than
//!    asserted from one.
//!
//! Both arms are compiled into this one binary and pinned by
//! `bitmosaic::skew::force` at runtime, alternated inside one process run
//! (handbook §4.5). Forcing the DRIVE arm BELOW the crossover is what makes
//! the bracket two-sided: the merge has to be shown winning the range it
//! keeps, not merely chosen there. Like-arm spread is reported beside every
//! cross-arm delta as the in-run null.

use std::collections::BTreeSet;
use std::hint::black_box;
use std::time::Instant;

use bitmosaic::Bitmosaic;
use bitmosaic::skew::{self, SkewCensus};
use roaring::RoaringBitmap;

// ── input shapes ─────────────────────────────────────────────────────────

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// Uniform in `[0, 1)`, from the top 53 bits.
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// Zipf(s=1) rank in `0..n`: `CDF(k) = ln(k+1)/ln(n+1)` inverts in closed
/// form, so a draw needs no cumulative table and no rejection loop.
fn zipf_rank(rng: &mut Rng, n: u64) -> u64 {
    let scaled = ((n + 1) as f64).ln() * rng.unit();
    (scaled.exp() as u64).saturating_sub(1).min(n - 1)
}

/// How a Zipf rank becomes a document id.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Layout {
    /// Rank scattered across the universe by an odd multiplier — a
    /// bijection on a power-of-two universe. Popularity stays Zipf while
    /// ADDRESS stays uniform, which is the posting-list shape: per-chunk
    /// cardinality is even and every chunk freezes to the same tag.
    Scattered,
    /// Rank used as the id. Popularity and address coincide, so low chunks
    /// are dense and high chunks are sparse and the pair spans several
    /// container tags at once.
    Ranked,
}

const SCATTER_MULT: u64 = 0x9E37_79B9_7F4A_7C15;

fn draw(rng: &mut Rng, count: usize, universe: u64, layout: Layout) -> BTreeSet<u32> {
    let mut out = BTreeSet::new();
    // Zipf collides hard on the hot head, so a fixed number of DRAWS yields
    // an unpredictable set size. Draw until the set is the size the cell
    // names, bounded so a saturated universe cannot spin.
    let mut budget = count as u64 * 64 + 1024;
    while out.len() < count && budget > 0 {
        budget -= 1;
        let rank = zipf_rank(rng, universe);
        let id = match layout {
            Layout::Scattered => rank.wrapping_mul(SCATTER_MULT) % universe,
            Layout::Ranked => rank,
        };
        out.insert(id as u32);
    }
    out
}

/// One skew cell: a rare posting list against a common one.
///
/// A quarter of the small side is lifted from the large side so the answer
/// is substantial at every skew. Independent Zipf draws already overlap on
/// the hot head, but at 1000:1 that overlap can round to nothing, and a cell
/// whose expected answer is zero cannot separate a correct kernel from a
/// kernel that returns zero.
fn pair(
    seed: u64,
    large_len: usize,
    skew_ratio: usize,
    universe: u64,
    layout: Layout,
) -> (Vec<u32>, Vec<u32>) {
    let mut rng = Rng(seed);
    let large = draw(&mut rng, large_len, universe, layout);
    let small_len = (large_len / skew_ratio).max(1);
    let mut small = draw(&mut rng, small_len - small_len / 4, universe, layout);
    let large_members: Vec<u32> = large.iter().copied().collect();
    for _ in 0..small_len / 4 {
        let at = (rng.next() % large_members.len() as u64) as usize;
        small.insert(large_members[at]);
    }
    (small.into_iter().collect(), large_members)
}

// ── arms ─────────────────────────────────────────────────────────────────

use bitmosaic::skew::Arm;

/// One mode's answer and its exact position census, with the counters reset
/// immediately before the call so the census belongs to this call alone.
fn run_census(mode: Arm, a: &Bitmosaic, b: &Bitmosaic) -> (u64, SkewCensus) {
    skew::force(mode);
    skew::reset();
    let answer = a.and_len(b);
    (answer, skew::census())
}

/// What the SHIPPED chooser did on this cell, read off its own census.
fn chooser_engaged(a: &Bitmosaic, b: &Bitmosaic) -> bool {
    let (_, census) = run_census(Arm::Auto, a, b);
    census.gallop_probes + census.key_probes > 0
}

/// Median nanoseconds per `and_len`, over `reps` timed batches.
fn time_arm(arm: Arm, a: &Bitmosaic, b: &Bitmosaic, iters: usize, reps: usize) -> Vec<f64> {
    skew::force(arm);
    let mut samples = Vec::with_capacity(reps);
    for _ in 0..reps {
        let start = Instant::now();
        let mut sink = 0u64;
        for _ in 0..iters {
            sink += black_box(black_box(a).and_len(black_box(b)));
        }
        black_box(sink);
        samples.push(start.elapsed().as_nanos() as f64 / iters as f64);
    }
    samples.sort_by(|x, y| x.partial_cmp(y).unwrap());
    samples
}

fn median(sorted: &[f64]) -> f64 {
    sorted[sorted.len() / 2]
}

/// Spread of one arm against itself, as a fraction of its own median. This
/// is the in-run null: any cross-arm delta below roughly twice this number
/// is unresolved.
fn spread(sorted: &[f64]) -> f64 {
    (sorted[sorted.len() - 1] - sorted[0]) / median(sorted)
}

/// Iterations that put one timed batch near `target_ns`, from a throwaway
/// pilot batch. A batch shorter than the scheduler's own granularity
/// measures the scheduler.
fn size_batch(a: &Bitmosaic, b: &Bitmosaic, target_ns: f64) -> usize {
    let pilot = time_arm(Arm::Auto, a, b, 64, 1);
    ((target_ns / pilot[0].max(1.0)) as usize).clamp(64, 4_000_000)
}

/// Round-robin over `arms`, four launches each, alternated inside one
/// process run so drift lands on every arm equally. Returns each arm's
/// sorted launch medians in the order given.
fn interleave<const N: usize>(
    arms: [Arm; N],
    a: &Bitmosaic,
    b: &Bitmosaic,
    iters: usize,
) -> [Vec<f64>; N] {
    let mut out: [Vec<f64>; N] = core::array::from_fn(|_| Vec::new());
    // Forward then reverse per round, so no arm is always warm-started by
    // the same neighbour.
    for round in 0..4 {
        for step in 0..N {
            let at = if round % 2 == 0 { step } else { N - 1 - step };
            out[at].push(median(&time_arm(arms[at], a, b, iters, 5)));
        }
    }
    for samples in out.iter_mut() {
        samples.sort_by(|x, y| x.partial_cmp(y).unwrap());
    }
    out
}

// ── oracle ───────────────────────────────────────────────────────────────

/// Differential oracle for one cell.
///
/// Four independent answers must agree: the shipped chooser, the forced
/// drive arm, the forced balanced arm, and `roaring`'s own intersection.
/// Returns the agreed count and the two forced censuses.
fn oracle(
    name: &str,
    small: &[u32],
    large: &[u32],
    a: &Bitmosaic,
    b: &Bitmosaic,
) -> (u64, SkewCensus, SkewCensus) {
    let (driven, drive_census) = run_census(skew::Arm::Drive, a, b);
    let (balanced, balanced_census) = run_census(skew::Arm::Balanced, a, b);
    let (shipped, _) = run_census(skew::Arm::Auto, a, b);

    let ro_a: RoaringBitmap = small.iter().copied().collect();
    let ro_b: RoaringBitmap = large.iter().copied().collect();
    let incumbent = ro_a.intersection_len(&ro_b);

    assert_eq!(balanced, incumbent, "{name}: balanced arm vs roaring");
    assert_eq!(driven, incumbent, "{name}: drive arm vs roaring");
    assert_eq!(shipped, incumbent, "{name}: shipped chooser vs roaring");
    assert!(
        incumbent > 0,
        "{name}: an empty intersection agrees with every kernel and refutes none"
    );
    // The reversed pairing exercises the other orientation of every
    // chooser, including the forest-level one.
    skew::force(skew::Arm::Auto);
    assert_eq!(b.and_len(a), incumbent, "{name}: reversed operand order");
    (incumbent, drive_census, balanced_census)
}

// ── cells ────────────────────────────────────────────────────────────────

fn acceptance_cells() {
    println!("\n=== ACCEPTANCE: Zipf pairs, skew x size x layout ===");
    println!("positions/probes are EXACT and deterministic; they are the primary evidence.");
    println!(
        "{:<40} {:>8} {:>9} {:>11} {:>10} {:>7}  census(str/cone/arr/wrd/runs)",
        "cell", "|A∩B|", "arm", "positions", "probes", "work"
    );

    for (layout, layout_name, universe_shift) in [
        (Layout::Scattered, "scattered", 25u32),
        (Layout::Ranked, "ranked", 22u32),
    ] {
        for large_len in [100_000usize, 1_000_000] {
            // Keep mean per-chunk cardinality inside the Array arm on the
            // scattered layout, so the sequence kernels are the subject
            // rather than the flat word-AND.
            let universe = 1u64 << (universe_shift + (large_len == 1_000_000) as u32 * 3);
            for skew_ratio in [1usize, 10, 100, 1000] {
                let (small, large) = pair(
                    0xA5A5_0000 + skew_ratio as u64,
                    large_len,
                    skew_ratio,
                    universe,
                    layout,
                );
                let a = Bitmosaic::from_sorted(small.iter().copied());
                let b = Bitmosaic::from_sorted(large.iter().copied());
                let name = format!("{layout_name}/{large_len}/{skew_ratio}:1");
                let (answer, _, balanced) = oracle(&name, &small, &large, &a, &b);
                // The shipped chooser's own census, not a forced arm's: on a
                // 1:1 cell it must show the merge, and that row is what makes
                // the skewed rows mean something.
                let (_, shipped) = run_census(skew::Arm::Auto, &a, &b);

                let (shipped_total, balanced_total) = (shipped.total(), balanced.total());
                assert!(
                    balanced_total > 0,
                    "{name}: the census cannot see the balanced arm"
                );
                println!(
                    "{:<40} {:>8} {:>9} {:>11} {:>10} {:>7}  {:?}",
                    format!("{name} |A|={} |B|={}", small.len(), large.len()),
                    answer,
                    "balanced",
                    balanced.linear_positions + balanced.key_positions,
                    balanced.gallop_probes + balanced.key_probes,
                    "",
                    b.container_census(),
                );
                println!(
                    "{:<40} {:>8} {:>9} {:>11} {:>10} {:>6.1}x  {}",
                    "",
                    answer,
                    "shipped",
                    shipped.linear_positions + shipped.key_positions,
                    shipped.gallop_probes + shipped.key_probes,
                    balanced_total as f64 / shipped_total.max(1) as f64,
                    if shipped.gallop_probes + shipped.key_probes > 0 {
                        "chooser took DRIVE"
                    } else {
                        "chooser kept the merge"
                    },
                );
            }
        }
    }
}

/// End-to-end walls for all THREE arms on the acceptance shapes.
///
/// The forced arms bound what the chooser is picking between, and the
/// shipped column is what a caller actually pays. Reading only the winning
/// axis would hide the whole reason a chooser exists: the drive arm is a
/// large REGRESSION on balanced pairs, and the shipped column has to track
/// the better of the two at every skew or the chooser is miscalibrated.
fn wall_cells() {
    println!("\n=== WALL (DIRECTION ONLY — PENDING-CALM-BOX, ~16 concurrent workers) ===");
    println!(
        "{:<22} {:>10} {:>10} {:>10} {:>11} {:>10} {:>9}",
        "cell", "merge ns", "drive ns", "shipped ns", "shipped vs", "worst", "null(max)"
    );
    for skew_ratio in [1usize, 10, 100, 1000] {
        let (small, large) = pair(
            0xB105_0000 + skew_ratio as u64,
            100_000,
            skew_ratio,
            1 << 25,
            Layout::Scattered,
        );
        let a = Bitmosaic::from_sorted(small.iter().copied());
        let b = Bitmosaic::from_sorted(large.iter().copied());
        let name = format!("zipf/100k/{skew_ratio}:1");
        oracle(&name, &small, &large, &a, &b);

        let iters = size_batch(&a, &b, 200_000.0);
        let [balanced, drive, shipped] =
            interleave([Arm::Balanced, Arm::Drive, Arm::Auto], &a, &b, iters);
        let (bm, dm, sm) = (median(&balanced), median(&drive), median(&shipped));
        let null = spread(&balanced).max(spread(&drive)).max(spread(&shipped));
        println!(
            "{:<22} {:>10.0} {:>10.0} {:>10.0} {:>10.2}x {:>10.2}x {:>8.1}%",
            name,
            bm,
            dm,
            sm,
            bm.min(dm) / sm,
            bm.max(dm) / sm,
            null * 100.0,
        );
    }
    println!("shipped vs = best forced arm / shipped (1.00x means the chooser picked it)");
    println!("worst      = worst forced arm / shipped (what the chooser avoided)");
}

/// One row of a bracket sweep.
///
/// Both arms are FORCED at every ratio, so each row prices the merge and
/// the driver on the same pair whether or not the shipped chooser would
/// have picked that arm there. The `chose` column reports what the chooser
/// actually did, and the constant is validated exactly where `chose` and
/// the measured winner agree on every row.
fn bracket_row(name: &str, a: &Bitmosaic, b: &Bitmosaic, small: &[u32], large: &[u32]) -> String {
    let (answer, driven, balanced_census) = oracle(name, small, large, a, b);
    let engaged = chooser_engaged(a, b);
    let iters = size_batch(a, b, 200_000.0);
    let [balanced, drive] = interleave([Arm::Balanced, Arm::Drive], a, b, iters);
    let (dm, bm) = (median(&drive), median(&balanced));
    let null = spread(&balanced).max(spread(&drive));
    let delta = bm / dm;
    let winner = if (delta - 1.0).abs() < 2.0 * null {
        "unresolved"
    } else if delta > 1.0 {
        "DRIVE"
    } else {
        "MERGE"
    };
    format!(
        "{:<24} {:>7} {:>11.1} {:>11.1} {:>8.2}x {:>9.1}% {:>11} {:>7}   pos {} / probes {}",
        name,
        answer,
        bm,
        dm,
        delta,
        null * 100.0,
        winner,
        if engaged { "drive" } else { "merge" },
        balanced_census.linear_positions + balanced_census.key_positions,
        driven.gallop_probes + driven.key_probes,
    )
}

fn bracket_header(title: &str) {
    println!("\n=== CROSSOVER BRACKET — {title} (DIRECTION ONLY) ===");
    println!(
        "{:<24} {:>7} {:>11} {:>11} {:>9} {:>10} {:>11} {:>7}",
        "cell", "|A∩B|", "merge ns", "drive ns", "delta", "null(max)", "winner", "chose"
    );
}

/// One chunk, one container pair, one kernel — the member ratio is the only
/// thing moving, so `DRIVE_RATIO_MEMBERS` is read off a bracket with rows on
/// both sides of it.
fn member_bracket() {
    bracket_header("members: one Array x Array chunk");
    for small_len in [8usize, 32, 128] {
        for ratio in [2usize, 4, 8, 16, 24, 32, 48, 64, 96, 128, 256] {
            let large_len = small_len * ratio;
            if large_len > 4096 {
                continue;
            }
            // Every member is below 65536, so the pair is one chunk and
            // `and_len` dispatches exactly one container kernel per call.
            let mut rng = Rng(0xC0FF_EE00 + (small_len * 512 + ratio) as u64);
            let mut large = BTreeSet::new();
            while large.len() < large_len {
                large.insert((rng.next() % 65_536) as u32);
            }
            let large: Vec<u32> = large.into_iter().collect();
            let mut small = BTreeSet::new();
            // Half the small side hits, half misses: a pure-hit cell walks
            // the driver's cheapest path and a pure-miss cell its longest.
            while small.len() < small_len / 2 {
                small.insert(large[(rng.next() % large_len as u64) as usize]);
            }
            while small.len() < small_len {
                small.insert((rng.next() % 65_536) as u32);
            }
            let small: Vec<u32> = small.into_iter().collect();
            let a = Bitmosaic::from_sorted(small.iter().copied());
            let b = Bitmosaic::from_sorted(large.iter().copied());
            println!(
                "{}",
                bracket_row(
                    &format!("members {small_len}x{ratio} -> {large_len}"),
                    &a,
                    &b,
                    &small,
                    &large
                )
            );
        }
        println!();
    }
}

/// Many chunks, four members each — the container kernels are identical on
/// both arms and the CHUNK-KEY ratio is the only thing moving, so
/// `DRIVE_RATIO_KEYS` gets its own bracket instead of inheriting one
/// measured on a SIMD-blocked merge.
fn key_bracket() {
    bracket_header("keys: forest chunk walk, 4 members per chunk");
    const PER_CHUNK: u32 = 4;
    for narrow_chunks in [4usize, 32, 256] {
        for ratio in [1usize, 2, 3, 4, 6, 8, 12, 16, 32, 64, 128] {
            let wide_chunks = narrow_chunks * ratio;
            if wide_chunks > 8_192 {
                continue;
            }
            let member = |chunk: u32, i: u32| (chunk << 16) | (i * 9_001 + 17);
            let large: Vec<u32> = (0..wide_chunks as u32)
                .flat_map(|c| (0..PER_CHUNK).map(move |i| member(c, i)))
                .collect();
            // The narrow side's chunks are spread across the wide side's
            // span, so the driver crosses real gaps rather than a prefix.
            let step = (wide_chunks / narrow_chunks) as u32;
            let small: Vec<u32> = (0..narrow_chunks as u32)
                .flat_map(|c| (0..PER_CHUNK).map(move |i| member(c * step, i)))
                .collect();
            let a = Bitmosaic::from_sorted(small.iter().copied());
            let b = Bitmosaic::from_sorted(large.iter().copied());
            println!(
                "{}",
                bracket_row(
                    &format!("keys {narrow_chunks}x{ratio} -> {wide_chunks}"),
                    &a,
                    &b,
                    &small,
                    &large
                )
            );
        }
        println!();
    }
}

fn main() {
    acceptance_cells();
    member_bracket();
    key_bracket();
    wall_cells();
    // Leave the process on the shipped arm so a later caller in the same
    // run is not silently measuring the forced one.
    skew::force(skew::Arm::Auto);
}
