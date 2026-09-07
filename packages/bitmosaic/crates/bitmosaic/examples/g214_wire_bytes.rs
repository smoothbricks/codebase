//! G214: what does the canonical byte string cost, on the corpus that
//! produced the published `bitmosaic/README.md` byte ratios?
//!
//! The README's worst sparse point is `u13m/n100` at **4.42x** the smallest
//! alternative, and the attribution there is not the container payloads: 81
//! chunks hold 100 values, so four fifths of the cost is directory. This
//! instrument prices the wire format against the same corpus and the same
//! two baselines, and reports the ratio per point.
//!
//! **Instrument check, run first and printed above the table.** The owned
//! `heap_bytes` column and the two baseline closed forms are re-derived here
//! rather than imported, and each is checked against the figure
//! `ptmcart/examples/g200_bitmosaic_crossover.rs` published from real encoder
//! output. A baseline that cannot reproduce a known point cannot price an
//! unknown one, and a ratio whose denominator is unvalidated is a number
//! with no subject.
//!
//! Every figure is a byte count — the length of a string an encoder actually
//! produced, or a closed form validated against one. Byte counts are
//! determined by the input and the format, so they neither move with the
//! build profile nor expire when the code around them is rewritten.
//!
//! Run: cargo run -p bitmosaic --profile perf --example g214_wire_bytes

use std::collections::BTreeSet;

use bitmosaic::{Bitmosaic, BitmosaicView};

// ── corpus (the g200 generator, so the points are the published points) ──

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

fn progression(step: u32, n: usize) -> Vec<u32> {
    (0..n as u32).map(|i| i * step).collect()
}

// ── baselines ────────────────────────────────────────────────────────────

/// `ptmcart`'s FTS span-tight bitmap container: a 28-byte record plus one
/// `ptmcart`'s FTS bitmap container (`encode_bitmap_container`, 24-byte
/// header): word-aligned, not bit-tight — `base_word = first >> 6` and
/// `word_count = (last >> 6) - base_word + 1`.
fn span_tight_bitmap(values: &[u32]) -> usize {
    let (first, last) = (values[0] >> 6, values[values.len() - 1] >> 6);
    24 + ((last - first) as usize + 1) * 8
}
/// `ptmcart`'s FTS Elias-Fano container: a 32-byte record, `n * low_bits`
/// packed low bits, and a `(span >> low_bits) + n` bit high plane.
fn fts_elias_fano(values: &[u32]) -> usize {
    let n = values.len() as u64;
    let span = u64::from(values[values.len() - 1] - values[0]);
    let mut lb = if span <= n {
        0
    } else {
        (63 - span.leading_zeros()) - (63 - n.leading_zeros())
    };
    if lb != 0 && (span >> lb) < n {
        lb -= 1;
    }
    32 + ((n * u64::from(lb)).div_ceil(8) + ((span >> lb) + n).div_ceil(64) * 8) as usize
}

fn smallest_baseline(values: &[u32]) -> (usize, &'static str) {
    let (b, e) = (span_tight_bitmap(values), fts_elias_fano(values));
    if b <= e {
        (b, "span-bmp")
    } else {
        (e, "fts-ef")
    }
}

// ── instrument check ─────────────────────────────────────────────────────

/// Every figure `g200` published from real encoder output, at the points it
/// published them for. If any row disagrees the table below is unusable.
/// `(label, members, expected byte size, expected words count, expected stride count)`
/// — one row per published figure.
type PreflightPoint = (&'static str, Vec<u32>, usize, Option<usize>, Option<usize>);

fn preflight() -> bool {
    let points: Vec<PreflightPoint> = vec![
        (
            "u13m/n100",
            uniform(13_000_000, 100, 0xB2 + 100),
            1_202,
            None,
            Some(272),
        ),
        (
            "u13m/n1000",
            uniform(13_000_000, 1_000, 0xB2 + 1_000),
            4_398,
            None,
            Some(1_985),
        ),
        (
            "u20k/n1000",
            uniform(20_000, 1_000, 0xA1 + 1_000),
            2_012,
            None,
            Some(820),
        ),
        (
            "u20k/n4000",
            uniform(20_000, 4_000, 0xA1 + 4_000),
            8_012,
            Some(2_528),
            None,
        ),
        (
            "u20k/n8000",
            uniform(20_000, 8_000, 0xA1 + 8_000),
            8_732,
            Some(2_528),
            None,
        ),
    ];
    let mut ok = true;
    println!("## Pre-flight — closed forms against g200's published encoder output\n");
    println!("| point | quantity | g200 published | here | |");
    println!("| :--- | :--- | ---: | ---: | :--- |");
    for (label, values, owned, bitmap, ef) in &points {
        let mut check = |what: &str, published: usize, here: usize| {
            let agree = published == here;
            ok &= agree;
            println!(
                "| `{label}` | {what} | {published} | {here} | {} |",
                if agree { "match" } else { "**DISAGREE**" }
            );
        };
        check(
            "owned heap_bytes",
            *owned,
            Bitmosaic::from_sorted(values.iter().copied()).heap_bytes(),
        );
        if let Some(b) = bitmap {
            check("span-tight bitmap", *b, span_tight_bitmap(values));
        }
        if let Some(e) = ef {
            check("FTS Elias-Fano", *e, fts_elias_fano(values));
        }
    }
    println!(
        "\n{}\n",
        if ok {
            "All rows match: the baselines reproduce real encoder output, so the ratios below have a validated denominator."
        } else {
            "**A row disagrees — do not read the table below.**"
        }
    );
    ok
}

// ── table ────────────────────────────────────────────────────────────────

fn row(label: &str, values: &[u32]) -> f64 {
    let owned = Bitmosaic::from_sorted(values.iter().copied());
    let bytes = owned.to_bytes();
    let view = BitmosaicView::open(&bytes).expect("canonical bytes must open");
    assert_eq!(
        view.len(),
        values.len() as u64,
        "{label}: the view lost values"
    );

    let (best, which) = smallest_baseline(values);
    let (was, now) = (
        owned.heap_bytes() as f64 / best as f64,
        bytes.len() as f64 / best as f64,
    );
    println!(
        "| `{label}` | {n} | {owned_b} | {wire} | {root} | {best} ({which}) | {was:.2}x | **{now:.2}x** |",
        n = values.len(),
        owned_b = owned.heap_bytes(),
        wire = bytes.len(),
        root = if view.is_elias_fano() { "ef" } else { "forest" },
    );
    now
}

fn main() {
    if !preflight() {
        std::process::exit(1);
    }
    println!("## Wire bytes vs the smallest of the two FTS baselines\n");
    println!("| shape | n | owned | wire | root | smallest baseline | owned/best | wire/best |");
    println!("| :--- | ---: | ---: | ---: | :--- | ---: | ---: | ---: |");

    let mut worst = (0.0f64, String::new());
    let mut worst_owned = (0.0f64, String::new());
    // The published 4.42x is against the smallest of the two baselines, but
    // a span-tight bitmap is the one named as the size target, so it is
    // reported on its own axis too rather than only when it happens to win.
    let mut worst_bmp = (0.0f64, String::new());
    let mut record = |label: String, values: Vec<u32>| {
        let owned = Bitmosaic::from_sorted(values.iter().copied()).heap_bytes() as f64
            / smallest_baseline(&values).0 as f64;
        let wire = Bitmosaic::from_sorted(values.iter().copied())
            .to_bytes()
            .len() as f64;
        let bmp = wire / span_tight_bitmap(&values) as f64;
        let ratio = row(&label, &values);
        if ratio > worst.0 {
            worst = (ratio, label.clone());
        }
        if owned > worst_owned.0 {
            worst_owned = (owned, label.clone());
        }
        if bmp > worst_bmp.0 {
            worst_bmp = (bmp, label);
        }
    };

    for (name, step) in [("stride1", 1u32), ("stride7", 7), ("stride1000", 1_000)] {
        for n in [1_000usize, 20_000, 200_000] {
            record(format!("{name}/n{n}"), progression(step, n));
        }
    }
    for n in [10usize, 100, 1_000, 4_000, 4_200, 8_000, 15_000] {
        record(format!("u20k/n{n}"), uniform(20_000, n, 0xA1 + n as u64));
    }
    for n in [100usize, 1_000, 10_000, 100_000] {
        record(
            format!("u13m/n{n}"),
            uniform(13_000_000, n, 0xB2 + n as u64),
        );
    }

    println!(
        "\n**Worst across the corpus, vs the smaller baseline: owned {:.2}x at `{}` -> wire {:.2}x at `{}`.**",
        worst_owned.0, worst_owned.1, worst.0, worst.1
    );
    println!(
        "**Worst vs a span-tight bitmap alone: wire {:.2}x at `{}`.**",
        worst_bmp.0, worst_bmp.1
    );
}
