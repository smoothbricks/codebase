//! Fixed-work cells for comparing two builds of this crate by retired
//! instructions, where a loaded host makes wall time useless:
//!
//! ```sh
//! cargo build --locked -p bitmosaic --profile bench --example format_gate
//! /usr/bin/time -l target/release/examples/format_gate view-dense-1e6 forest contains 2000
//! ```
//!
//! `format_gate <shape> <sealed|forest> <op> <iterations>` builds the shape,
//! runs the operation that many times and prints a checksum. Run a cell at
//! zero iterations and at N: the difference in `instructions retired`,
//! divided by N, is the operation's cost, free of the setup. The shapes are
//! those of `benches/ops.rs`' large-view group plus two small ordinal sets.

use std::hint::black_box;

use bitmosaic::{Bitmosaic, BitmosaicView, PatchScratch, patch};

fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn uniform(n: usize, below: u64, seed: u64) -> Vec<u32> {
    let mut state = seed;
    let mut values: Vec<u32> = (0..n * 5 / 4)
        .map(|_| (xorshift(&mut state) % below) as u32)
        .collect();
    values.sort_unstable();
    values.dedup();
    values.truncate(n);
    values
}

fn runs(count: u32, len: u32, gap: u32, offset: u32) -> Vec<u32> {
    (0..count)
        .flat_map(|i| {
            let start = offset + i * (len + gap);
            start..start + len
        })
        .collect()
}

fn shape(name: &str) -> (Vec<u32>, Vec<u32>) {
    match name {
        "view-dense-1e4" => (
            uniform(10_000, 20_000, 0x1e4_0001),
            uniform(10_000, 20_000, 0x1e4_0002),
        ),
        "view-sparse-1e4" => (
            uniform(10_000, 1 << 32, 0x1e4_0003),
            uniform(10_000, 1 << 32, 0x1e4_0004),
        ),
        "view-runs-1e4" => (runs(10, 1_000, 9_000, 0), runs(10, 1_000, 9_000, 500)),
        "view-dense-1e6" => (
            uniform(1_000_000, 1 << 21, 0x1e6_0001),
            uniform(1_000_000, 1 << 21, 0x1e6_0002),
        ),
        "view-sparse-1e6" => (
            uniform(1_000_000, 1 << 28, 0x1e6_0003),
            uniform(1_000_000, 1 << 28, 0x1e6_0004),
        ),
        "view-runs-1e6" => (runs(1_000, 1_000, 3_000, 0), runs(1_000, 1_000, 3_000, 500)),
        "small-4-of-83" => (vec![7, 17, 40, 82], vec![3, 17, 41, 82]),
        "small-13-of-70" => (
            vec![1, 3, 9, 14, 20, 25, 33, 40, 47, 55, 60, 66, 69],
            vec![0, 3, 8, 14, 21, 25, 30, 40, 48, 55, 61, 66, 68],
        ),
        other => panic!("unknown shape {other}"),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let [_, shape_name, form, op, iterations] = args.as_slice() else {
        panic!("usage: format_gate <shape> <sealed|forest> <op> <iterations>");
    };
    let iterations: u64 = iterations.parse().expect("an iteration count");
    let (va, vb) = shape(shape_name);
    let (owned_a, owned_b) = (
        Bitmosaic::from_sorted(va.iter().copied()),
        Bitmosaic::from_sorted(vb.iter().copied()),
    );
    let (a_bytes, b_bytes) = match form.as_str() {
        "sealed" => (owned_a.to_bytes(), owned_b.to_bytes()),
        "forest" => (owned_a.to_forest_bytes(), owned_b.to_forest_bytes()),
        other => panic!("unknown form {other}"),
    };
    let a_view = BitmosaicView::open(&a_bytes).expect("attach left");
    let b_view = BitmosaicView::open(&b_bytes).expect("attach right");
    let mut state = 0x9120_be5_u64 ^ va.len() as u64;
    let top = u64::from(va[va.len() - 1]) + 2;
    let probes: Vec<u32> = (0..1024)
        .map(|i| {
            let r = xorshift(&mut state);
            if i % 2 == 0 {
                va[(r % va.len() as u64) as usize]
            } else {
                (r % top) as u32
            }
        })
        .collect();
    let ranks: Vec<u64> = (0..1024)
        .map(|_| xorshift(&mut state) % va.len() as u64)
        .collect();
    let mut ascending = probes.clone();
    ascending.sort_unstable();
    // Sixteen values absent from the set, spread over its chunks: one batch
    // adds them and the next removes them, so every iteration starts from
    // the same image.
    let mut batch: Vec<u32> = (0..16u64)
        .map(|i| (i * top / 16) as u32)
        .filter(|v| va.binary_search(v).is_err())
        .collect();
    batch.dedup();
    let mut slot = vec![0u8; a_bytes.len() * 2 + (1 << 20)];
    slot[..a_bytes.len()].copy_from_slice(&a_bytes);
    let mut scratch = PatchScratch::try_new().expect("scratch");

    let mut sum = 0u64;
    for _ in 0..iterations {
        sum = sum.wrapping_add(match op.as_str() {
            "attach" => BitmosaicView::open(black_box(&a_bytes)).map_or(0, |view| view.len()),
            "verify" => u64::from(BitmosaicView::open_verified(black_box(&a_bytes)).is_ok()),
            "contains" => probes
                .iter()
                .map(|&p| u64::from(a_view.contains(black_box(p))))
                .sum(),
            "rank" => probes.iter().map(|&p| a_view.rank(black_box(p))).sum(),
            "select" => ranks
                .iter()
                .map(|&k| u64::from(a_view.select(black_box(k)).unwrap_or(0)))
                .sum(),
            "walk" => {
                let mut walked = 0u64;
                black_box(&a_view).for_each(|v| walked += u64::from(v));
                walked
            }
            "cursor" => black_box(&a_view).range().map(u64::from).sum(),
            "seek" => {
                let mut cursor = black_box(&a_view).range();
                ascending
                    .iter()
                    .map(|&p| {
                        cursor.seek(p);
                        cursor.remaining()
                    })
                    .sum()
            }
            "and_len" => a_view.and_len(black_box(&b_view)),
            "to_bytes" => black_box(&owned_a).to_bytes().len() as u64,
            "to_forest_bytes" => black_box(&owned_a).to_forest_bytes().len() as u64,
            "patch" => {
                let added = patch(&mut slot, &batch, &[], &mut scratch).expect("room to add");
                let removed = patch(&mut slot, &[], &batch, &mut scratch).expect("room to remove");
                added.len + removed.len
            }
            other => panic!("unknown op {other}"),
        });
    }
    println!("{shape_name} {form} {op} x{iterations}: {sum}");
}
