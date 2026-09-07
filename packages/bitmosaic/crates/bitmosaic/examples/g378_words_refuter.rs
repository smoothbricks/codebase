//! G378: the `Words` min/max refuter — what does a chunk's own bounds,
//! carried in the alignment pad, delete from a leapfrog seek?
//!
//! ## The lever
//!
//! A frozen `Words` payload opens with a 24-byte head: a 16-byte block
//! summary, `base_word`, `word_count`, and — until this unit — four bytes of
//! pad whose only job was to put the word plane on an 8-byte boundary. The
//! pad now carries the chunk's EXACT first and last member, derived from the
//! chunk's own words during the scan that already locates the span-tight
//! window. `CView::open` reads that head; the refuter therefore rides a line
//! the probe has already paid for.
//!
//! `base_word`/`word_count` already bound the members, but LOOSELY: the
//! window is snapped out to whole 8-word summary blocks, so it is slack by up
//! to 511 positions at each end. Every probe landing in that slack currently
//! walks the directory and up to `DIR_STRIDE` words of the payload only to
//! conclude "rank 0" or "all of them". Those are the loads this unit deletes.
//!
//! ## What is being claimed, and what is not
//!
//! The SUBJECT is `BitmosaicViewRange::seek` — the borrowed cursor that exists
//! so a leapfrog join can run over mapped bytes. It has no production caller
//! in this tree today: `AndRange` and friends align by `pop_front`, not by
//! `seek`, and the FTS lane drives owned forests. So this example is the
//! honest cell for a plane whose consumer is filed, not shipped — the D-a
//! demand masks of `10x-docs/datalog-eval-brief.md` §1, where a magic-set
//! demand window is exactly a range predicate against a chunk.
//!
//! ## Acceptance is COUNTS FIRST
//!
//! Panel 1 is exact and box-free: the same seek workload run with the read
//! routed as it was before the rider (`Arm::Off`) and as it is now
//! (`Arm::Auto`), reporting the word- and directory-plane loads `rank_below`
//! charged in each. That difference is the deleted work, in loads, with no
//! timing assumption in it and no calm box required. Panel 2 is the wall
//! clock, same binary, arms switched at runtime, interleaved, MIN per arm,
//! with an in-run A/A floor — and it is CORROBORATION, not the claim.
//!
//! ## Residues (§7.14e), enumerated rather than carried in a formula
//!
//! Every seek against a `Words` chunk falls in exactly one of three classes,
//! and panel 1 prints the histogram rather than a rate:
//!
//! - SKIPPED — probe above the chunk's last member. The chunk is rejected
//!   from the head line; the walk resumes at the next chunk having touched
//!   neither plane.
//! - HEADED — probe at or below the first member. Answered as rank 0.
//! - WALKED — probe strictly inside. Pays `rank_below` exactly as before,
//!   PLUS the refuter's two compares against values on the head line. That
//!   is the miss cost, and it is the whole of it: no extra load, because the
//!   bytes compared arrived with `base_word`.
//!
//! The deleted LOADS are narrower than the refuted PROBES, and panel 1
//! reports both because conflating them would overstate the rider.
//! `rank_below` already returns without touching a plane when the probe
//! falls outside the block-snapped window — it has `base_word` and
//! `word_count` in register. So a refuted probe deletes real loads only when
//! it lands in the SNAP SLACK: inside the window, outside the member span, at
//! most 511 positions at each end. Outside that band the refuter deletes two
//! register compares, not a walk. `rank loads OFF/ON` is the honest column;
//! `decided from head` is the reach.
//!
//! Which class a probe falls in is a property of the DATA and of the DRIVER,
//! not of the query text: the refuter's reach is the fraction of a chunk's
//! key space lying outside its member span, times the fraction of probes that
//! enter a chunk the cursor has not already advanced into. The four cells
//! below cross both factors.
//!
//! Run: cargo run -p bitmosaic --profile bench --features bench-internals \
//!        --example g378_words_refuter
//!
//! `--profile bench` (opt3 + LTO, unstripped, debug); `perf` is its sibling.
//! `--release` is opt-level "z" in this workspace and voids every number.

use std::hint::black_box;
use std::time::Instant;

use bitmosaic::refuter::{self, Arm};
use bitmosaic::{Bitmosaic, BitmosaicView, Range};

const REPS: u32 = 15;

/// PRE-REGISTERED, before any number was seen: a timing delta counts as REAL
/// only at this multiple of its own same-session like-arm null. Below it the
/// cell is UNRESOLVED and no direction is reported from the clock — panel 1
/// still stands, because a load count has no null to clear.
const VERDICT_BAR: f64 = 2.0;

/// Race every arm inside one repeat loop, MIN per arm. Arms are `dyn` so they
/// share one call shape — an arm inlined into the timing loop while its rival
/// is not would be the same defect as measuring at two profiles.
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

/// `chunks` chunks, each holding a dense band inset into its 65,536-position
/// key space. The inset is the point: it is the slack the block-snapped
/// window cannot see and the refuter can.
///
/// Density is not decoration — the ladder publishes `Words` only where it
/// undercuts every other arm, so a sparse band would freeze to a `Cone` or an
/// Elias-Fano root and the cell would measure a container that has no refuter
/// in it. `assert_words` below refuses to time such a subject.
fn inset_chunks(chunks: u32, lo: u32, hi: u32, keep: u32) -> Vec<u32> {
    (0..chunks)
        .flat_map(move |c| {
            let base = c * 65_536;
            (lo..hi)
                .filter(move |v| keep == 1 || v % keep != 0)
                .map(move |v| base + v)
        })
        .collect()
}

/// Ascending probes at a fixed stride — the driver side of a leapfrog join,
/// which is the only shape that drives `seek` at all. The stride is coprime
/// with the chunk width so the probes do not phase-lock to one band.
fn probes(domain: u32, stride: u32) -> Vec<u32> {
    (0..domain / stride).map(|i| i * stride).collect()
}

/// The workload. ONE cursor advanced monotonically, which is what makes this
/// a leapfrog rather than a sequence of independent lookups.
fn leapfrog(view: &BitmosaicView<'_>, probes: &[u32]) -> u64 {
    let mut cursor = view.range();
    let mut sum = 0u64;
    for &p in probes {
        cursor.seek(p);
        if Range::empty(&cursor) {
            break;
        }
        sum = sum.wrapping_add(u64::from(Range::front(&cursor)));
    }
    sum
}

fn assert_words(label: &str, owned: &Bitmosaic) -> String {
    let (stride, cone, array, words, runs) = owned.container_census();
    assert!(
        words > 0 && words >= stride + cone + array + runs,
        "{label}: subject is not predominantly Words ({stride}s/{cone}c/{array}a/{words}w/{runs}r) - \
         the refuter would not be exercised"
    );
    format!("{stride}s/{cone}c/{array}a/{words}w/{runs}r")
}

/// Runs both panels for one shape and returns their rows, so the two tables
/// print whole instead of interleaved.
fn cell(label: &str, values: &[u32], probe_stride: u32) -> (String, String) {
    let owned = Bitmosaic::from_sorted(values.iter().copied());
    let census = assert_words(label, &owned);
    let bytes = owned.to_bytes();
    let view = BitmosaicView::open(&bytes).expect("open");
    let domain = values.last().copied().unwrap_or(0) + 1;
    let probes = probes(domain, probe_stride);

    // ORACLE BEFORE TIMING. The two arms are two routings of one answer; if
    // they disagree the refuter is wrong and no timing of it means anything.
    refuter::set_arm(Arm::Off);
    let unrouted = leapfrog(&view, &probes);
    refuter::set_arm(Arm::Auto);
    let routed = leapfrog(&view, &probes);
    assert_eq!(unrouted, routed, "{label}: the refuter changed the answer");

    // ── panel 1: loads, exact ────────────────────────────────────────────
    refuter::set_arm(Arm::Off);
    refuter::reset();
    black_box(leapfrog(&view, &probes));
    let off = refuter::census();

    refuter::set_arm(Arm::Auto);
    refuter::reset();
    black_box(leapfrog(&view, &probes));
    let on = refuter::census();

    let decided = on.skipped + on.headed;
    let seen = decided + on.walked;
    let row1 = format!(
        "| `{label}` | {census} | {} | {} | {} | {} | {} | {} | {:.1}% | {} | {} | **{:.2}x** |",
        values.len(),
        probes.len(),
        seen,
        on.skipped,
        on.headed,
        on.walked,
        100.0 * decided as f64 / seen.max(1) as f64,
        off.rank_words,
        on.rank_words,
        off.rank_words as f64 / on.rank_words.max(1) as f64,
    );

    // ── panel 2: the clock, corroborating ────────────────────────────────
    //
    // Four arms, not two: the third and fourth are BOTH `Arm::Auto` running
    // the identical closure, so the session measures its own like-arm null
    // under the same interleave that produced the comparison.
    // A selective driver leaves only a few hundred probes per pass, which is
    // too small a sample against the clock's granularity — so each timed
    // sample repeats the whole monotone walk from a fresh cursor, and the
    // per-probe figure divides by the total.
    let inner = (2_000_000 / probes.len()).max(1);
    let pass = |arm: Arm| {
        refuter::set_arm(arm);
        (0..inner)
            .map(|_| leapfrog(black_box(&view), black_box(&probes)))
            .fold(0u64, u64::wrapping_add)
    };
    let per_call = probes.len() * inner;
    let t = race(
        per_call,
        &mut [
            &mut || pass(Arm::Off),
            &mut || pass(Arm::Auto),
            &mut || pass(Arm::Auto),
            &mut || pass(Arm::Auto),
        ],
    );

    // THE FLOOR IS A SEPARATE RACE, all four arms identical, and the null is
    // taken from POSITIONS 0 AND 1 — the same two slots the comparison above
    // occupies. A null read off positions 2 and 3 bounds late-slot drift and
    // says nothing about the first-versus-second bias that the comparison is
    // actually exposed to, which is the harness trap in §7.14e: an A/A floor
    // measured somewhere other than where the arms ran is not that comparison's
    // floor. The larger of the two is taken, so neither can flatter a verdict.
    let aa = race(
        per_call,
        &mut [
            &mut || pass(Arm::Auto),
            &mut || pass(Arm::Auto),
            &mut || pass(Arm::Auto),
            &mut || pass(Arm::Auto),
        ],
    );
    let ratio = |a: f64, b: f64| (a.max(b) / a.min(b)) - 1.0;
    let (off_ns, on_ns) = (t[0], t[1]);
    let null = ratio(aa[0], aa[1]).max(ratio(t[2], t[3]));
    let delta = (off_ns / on_ns) - 1.0;
    let verdict = if delta.abs() < null * VERDICT_BAR {
        "UNRESOLVED"
    } else if delta > 0.0 {
        "refuter faster"
    } else {
        "refuter SLOWER"
    };
    let row2 = format!(
        "| `{label}` | {off_ns:.2} | {on_ns:.2} | {:+.1}% | {:.1}% | {verdict} |",
        delta * 100.0,
        null * 100.0
    );
    (row1, row2)
}

fn main() {
    // Two shapes bracketing the refuter's reach. `slack-16pct` is 4 of every
    // 5 positions across 5,000..60,000 - a chunk that is nearly full, where
    // the block-snapped window is already almost tight and the refuter has
    // only the inset to work with. `slack-90pct` is a narrow dense band, the
    // shape a selective attribute plane actually produces.
    let wide = inset_chunks(256, 5_000, 60_000, 5);
    // Gapped, not contiguous: a solid run ladders to `Stride`, which is a
    // closed form with no planes to refute and so not this unit's subject.
    let narrow = inset_chunks(256, 30_000, 36_500, 5);

    // Both shapes under both DRIVERS, because the driver decides the refuter's
    // reach as much as the data does.
    //
    // - `dense` (stride 997, ~66 probes per chunk) is the adversarial case.
    //   A monotone cursor that has already been advanced into a chunk answers
    //   the next probe from its own `front` without calling the chunk at all,
    //   so the low-band verdict never reaches the refuter and only ONE probe
    //   per chunk — the one that runs off the end — can be refuted. Almost
    //   every seek here is a WALKED miss paying the two extra compares.
    // - `selective` (stride 68,111, prime, ~1.04 chunks apart) is the shape a
    //   demand mask or a range predicate produces: each probe enters a chunk
    //   the cursor has not seen, so the refuter is consulted on its merits and
    //   the refute rate approaches the shape's slack fraction. The stride is
    //   prime and not a divisor of 65,536 precisely so the probe offset drifts
    //   across the band instead of phase-locking to one part of it.
    let rows: Vec<(String, String)> = [
        ("slack-16pct/dense", &wide, 997u32),
        ("slack-16pct/selective", &wide, 68_111),
        ("slack-90pct/dense", &narrow, 997),
        ("slack-90pct/selective", &narrow, 68_111),
    ]
    .into_iter()
    .map(|(label, values, stride)| cell(label, values, stride))
    .collect();

    println!("## G378 panel 1 - plane loads deleted (exact, box-free)\n");
    println!(
        "| cell | census | members | probes | Words seeks | skipped | headed | walked | \
         decided from head | rank loads OFF | rank loads ON | ratio |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|");
    for (row, _) in &rows {
        println!("{row}");
    }

    println!(
        "\n## G378 panel 2 - wall clock, same binary, arms switched at runtime\n\
         \nCORROBORATION ONLY; panel 1 is the acceptance. ns are per probe.\n"
    );
    println!("| cell | OFF ns | ON ns | delta | in-run A/A null | verdict |");
    println!("|---|---|---|---|---|---|");
    for (_, row) in &rows {
        println!("{row}");
    }
}
