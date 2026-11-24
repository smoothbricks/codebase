//! Same-binary interleaved A/B direction benchmark & tiny-sets byte table for Format 2 vs Format 1.

#[cfg(target_arch = "wasm32")]
fn main() {}

#[cfg(not(target_arch = "wasm32"))]
fn main() {
    use std::hint::black_box;
    use std::time::Instant;

    use bitmosaic::{
        BitmapView, Bitmosaic, ListView, PatchScratch, V2View, patch, patch_v2, write_image_into_v2,
    };

    fn xorshift(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    fn bench_ab(
        name: &str,
        rounds: usize,
        iters: usize,
        mut control: impl FnMut() -> u64,
        mut fast: impl FnMut() -> u64,
    ) {
        black_box(control());
        black_box(fast());
        let mut t_ctrl = Vec::with_capacity(rounds);
        let mut t_fast = Vec::with_capacity(rounds);
        let mut aa_ratios = Vec::with_capacity(rounds);
        for r in 0..rounds {
            let (c, f) = if r % 2 == 0 {
                let t0 = Instant::now();
                let mut acc = 0u64;
                for _ in 0..iters {
                    acc = acc.wrapping_add(black_box(control()));
                }
                black_box(acc);
                let dc = t0.elapsed().as_nanos() as f64 / iters as f64;

                let t1 = Instant::now();
                let mut acc2 = 0u64;
                for _ in 0..iters {
                    acc2 = acc2.wrapping_add(black_box(fast()));
                }
                black_box(acc2);
                let df = t1.elapsed().as_nanos() as f64 / iters as f64;
                (dc, df)
            } else {
                let t1 = Instant::now();
                let mut acc2 = 0u64;
                for _ in 0..iters {
                    acc2 = acc2.wrapping_add(black_box(fast()));
                }
                black_box(acc2);
                let df = t1.elapsed().as_nanos() as f64 / iters as f64;

                let t0 = Instant::now();
                let mut acc = 0u64;
                for _ in 0..iters {
                    acc = acc.wrapping_add(black_box(control()));
                }
                black_box(acc);
                let dc = t0.elapsed().as_nanos() as f64 / iters as f64;
                (dc, df)
            };
            let t2 = Instant::now();
            let mut acc3 = 0u64;
            for _ in 0..iters {
                acc3 = acc3.wrapping_add(black_box(fast()));
            }
            black_box(acc3);
            let df2 = t2.elapsed().as_nanos() as f64 / iters as f64;
            aa_ratios.push((df2 / f).max(f / df2) - 1.0);
            t_ctrl.push(c);
            t_fast.push(f);
        }
        let ratios: Vec<f64> = t_ctrl.iter().zip(&t_fast).map(|(c, f)| c / f).collect();
        let n = ratios.len() as f64;
        let mean_c = t_ctrl.iter().sum::<f64>() / n;
        let mean_f = t_fast.iter().sum::<f64>() / n;
        let mean_r = ratios.iter().sum::<f64>() / n;
        let var_r = ratios.iter().map(|r| (r - mean_r).powi(2)).sum::<f64>() / (n - 1.0);
        let ci = 1.96 * (var_r / n).sqrt();
        let aa_mean = aa_ratios.iter().sum::<f64>() / n * 100.0;
        let verdict = if mean_r - ci > 1.0 + aa_mean / 100.0 {
            "DIRECTION SUPPORTED"
        } else if mean_r + ci < 1.0 - aa_mean / 100.0 {
            "LOSS"
        } else {
            "UNRESOLVED"
        };
        println!(
            "{name:<36} | F1={mean_c:7.1}ns  F2={mean_f:7.1}ns | {mean_r:5.2}x +/- {ci:4.2}x | A/A={aa_mean:4.1}% | {verdict}"
        );
    }

    // 1. Tiny-sets byte size comparison (agent-todo/bitmosaic-tiny-sets.md).
    println!("=== Format 2 vs Format 1 Byte Sizes (bitmosaic-tiny-sets.md) ===");
    let tiny_cases: [(&str, Vec<u32>); 6] = [
        ("empty", vec![]),
        ("singleton {1000000}", vec![1_000_000]),
        ("4 of 83 (low: 0,1,2,10)", vec![0, 1, 2, 10]),
        ("4 of 83 (spread: 0,12,41,82)", vec![0, 12, 41, 82]),
        ("13 of 70 (0..=11, 70)", (0..12).chain([70]).collect()),
        ("20 of 83 (0..=18, 82)", (0..19).chain([82]).collect()),
    ];
    for (name, members) in &tiny_cases {
        let bm = Bitmosaic::from_sorted(members.iter().copied());
        let v1 = bm.to_bytes().len();
        let v2 = bm.to_v2_bytes().len();
        let v2_form = V2View::open(&bm.to_v2_bytes()).unwrap().form();
        println!(
            "  {name:<30}: F1={v1:3}B  F2={v2:3}B ({v2_form:?})  ratio={:.2}x smaller",
            v1 as f64 / v2 as f64
        );
    }
    let all83: Vec<u32> = (0..83).collect();
    let bm83 = Bitmosaic::from_sorted(all83.iter().copied());
    println!(
        "  {:<30}: F1={:3}B  F2={:3}B ({:?})  ratio={:.2}x smaller",
        "all 83 (0..=82)",
        bm83.to_bytes().len(),
        bm83.to_v2_bytes().len(),
        V2View::open(&bm83.to_v2_bytes()).unwrap().form(),
        bm83.to_bytes().len() as f64 / bm83.to_v2_bytes().len() as f64
    );

    // 2. Interleaved A/B direction benchmarks (same binary, 16 paired rounds).
    println!("\n=== Format 2 vs Format 1 Direction Benchmarks (16 paired rounds) ===");
    let rounds = 16;

    // Case A: Write 13-of-70 frame (Format 1 Bitmosaic::from_sorted + write_into_slice vs Format 2 write_image_into_v2).
    let frame13: Vec<u32> = (0..12).chain([70]).collect();
    let mut out1 = [0u8; 128];
    let mut out2 = [0u8; 128];
    bench_ab(
        "write 13-of-70 frame",
        rounds,
        40_000,
        || {
            let s = Bitmosaic::from_sorted(black_box(&frame13).iter().copied());
            s.write_into_slice(&mut out1).unwrap() as u64
        },
        || write_image_into_v2(black_box(&frame13), &mut out2).unwrap() as u64,
    );

    // Case B: Write 3-member posting tail {5, 70000, 140000}.
    let tail3 = [5u32, 70_000, 140_000];
    bench_ab(
        "write 3-member posting tail",
        rounds,
        40_000,
        || {
            let s = Bitmosaic::from_sorted(black_box(&tail3).iter().copied());
            s.write_into_slice(&mut out1).unwrap() as u64
        },
        || write_image_into_v2(black_box(&tail3), &mut out2).unwrap() as u64,
    );

    // Prepare probe keys.
    let mut rng = 0x5EED_0002_CAFE_1234u64;
    let frame_probes: Vec<u32> = (0..1024)
        .map(|_| (xorshift(&mut rng) % 90) as u32)
        .collect();
    let frame_v1 = Bitmosaic::from_sorted(frame13.iter().copied()).to_bytes();
    let frame_v2 = Bitmosaic::from_sorted(frame13.iter().copied()).to_v2_bytes();
    let f1_view = bitmosaic::BitmosaicView::open_admitted(&frame_v1).unwrap();
    let bm_view = BitmapView::open_admitted(&frame_v2).unwrap();

    bench_ab(
        "contains 13-of-70 (direct Bitmap)",
        rounds,
        2_000,
        || {
            let mut hits = 0u64;
            for &p in black_box(&frame_probes) {
                hits += u64::from(f1_view.contains(p));
            }
            hits
        },
        || {
            let mut hits = 0u64;
            for &p in black_box(&frame_probes) {
                hits += u64::from(bm_view.contains(p));
            }
            hits
        },
    );

    bench_ab(
        "rank 13-of-70 (direct Bitmap)",
        rounds,
        2_000,
        || {
            let mut acc = 0u64;
            for &p in black_box(&frame_probes) {
                acc += f1_view.rank(p);
            }
            acc
        },
        || {
            let mut acc = 0u64;
            for &p in black_box(&frame_probes) {
                acc += u64::from(bm_view.rank(p));
            }
            acc
        },
    );

    // Case C: List contains & rank (8-member posting tail).
    let list8 = [300u32, 301, 310, 400, 401, 450, 500, 555];
    let list_probes: Vec<u32> = (0..1024)
        .map(|_| 290 + (xorshift(&mut rng) % 300) as u32)
        .collect();
    let list_v1 = Bitmosaic::from_sorted(list8).to_bytes();
    let list_v2 = Bitmosaic::from_sorted(list8).to_v2_bytes();
    let l1_view = bitmosaic::BitmosaicView::open_admitted(&list_v1).unwrap();
    let l2_view = ListView::open_admitted(&list_v2).unwrap();

    bench_ab(
        "contains 8-member List",
        rounds,
        2_000,
        || {
            let mut hits = 0u64;
            for &p in black_box(&list_probes) {
                hits += u64::from(l1_view.contains(p));
            }
            hits
        },
        || {
            let mut hits = 0u64;
            for &p in black_box(&list_probes) {
                hits += u64::from(l2_view.contains(p));
            }
            hits
        },
    );

    bench_ab(
        "rank 8-member List",
        rounds,
        2_000,
        || {
            let mut acc = 0u64;
            for &p in black_box(&list_probes) {
                acc += l1_view.rank(p);
            }
            acc
        },
        || {
            let mut acc = 0u64;
            for &p in black_box(&list_probes) {
                acc += u64::from(l2_view.rank(p));
            }
            acc
        },
    );

    // Case D: 1-member patch (add + remove member 42 on 13-of-70 frame).
    let mut slot1 = vec![0u8; 256];
    let f1_forest = Bitmosaic::from_sorted(frame13.iter().copied()).to_forest_bytes();
    slot1[..f1_forest.len()].copy_from_slice(&f1_forest);
    let mut slot2 = vec![0u8; 256];
    slot2[..frame_v2.len()].copy_from_slice(&frame_v2);
    let mut scratch1 = PatchScratch::try_new().unwrap();
    scratch1.try_reserve_for(256, 1).unwrap();
    let mut scratch2 = PatchScratch::try_new().unwrap();

    bench_ab(
        "1-member patch add+remove (Bitmap)",
        rounds,
        20_000,
        || {
            let r1 = patch(&mut slot1, &[42], &[], &mut scratch1).unwrap();
            let r2 = patch(&mut slot1, &[], &[42], &mut scratch1).unwrap();
            r1.len + r2.len
        },
        || {
            let r1 = patch_v2(&mut slot2, &[42], &[], &mut scratch2).unwrap();
            let r2 = patch_v2(&mut slot2, &[], &[42], &mut scratch2).unwrap();
            r1.len + r2.len
        },
    );
}
