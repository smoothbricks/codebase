//! Same-binary interleaved A/B direction benchmark for bitmosaic fast lanes.
//!
//! Compares each new fast lane against its previous algorithm in one binary
//! with interleaved paired rounds and a like-arm A/A floor.

#[cfg(target_arch = "wasm32")]
fn main() {}

#[cfg(not(target_arch = "wasm32"))]
fn main() {
    use std::hint::black_box;
    use std::time::Instant;

    use bitmosaic::{AndNotRange, Bitmosaic, Bitmosaic64, Bitmosaic64View, BitmosaicView, Range};

    fn xorshift(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    fn sorted_unique(mut v: Vec<u32>) -> Vec<u32> {
        v.sort_unstable();
        v.dedup();
        v
    }

    fn bench_ab(
        name: &str,
        rounds: usize,
        iters: usize,
        mut control: impl FnMut() -> u64,
        mut fast: impl FnMut() -> u64,
    ) {
        // Warmup.
        black_box(control());
        black_box(fast());
        let mut t_ctrl = Vec::with_capacity(rounds);
        let mut t_fast = Vec::with_capacity(rounds);
        let mut aa_ratios = Vec::with_capacity(rounds);
        for r in 0..rounds {
            // Interleaved order alternates each round to cancel drift.
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
            // A/A check on fast arm.
            let t2 = Instant::now();
            let mut acc3 = 0u64;
            for _ in 0..iters {
                acc3 = acc3.wrapping_add(black_box(fast()));
            }
            black_box(acc3);
            let df2 = t2.elapsed().as_nanos() as f64 / iters as f64;
            aa_ratios.push((df2 / f - 1.0).abs() * 100.0);
            t_ctrl.push(c);
            t_fast.push(f);
        }
        t_ctrl.sort_by(|a, b| a.total_cmp(b));
        t_fast.sort_by(|a, b| a.total_cmp(b));
        aa_ratios.sort_by(|a, b| a.total_cmp(b));
        let med_c = t_ctrl[rounds / 2];
        let med_f = t_fast[rounds / 2];
        let min_c = t_ctrl[0];
        let max_f = t_fast[rounds - 1];
        let aa_med = aa_ratios[rounds / 2];
        let speedup = med_c / med_f;
        let disjoint = max_f < min_c;
        println!(
            "{name:<32} ctrl={med_c:9.1} ns  fast={med_f:8.1} ns  speedup={speedup:7.2}x  disjoint={disjoint:<5}  A/A={aa_med:4.1}%  (rounds={rounds})"
        );
    }

    let mut rng = 0xBEEF_1234_5678_9ABCu64;

    // 1. open_admitted vs open (80-chunk forest with slot plane).
    let mut slot_vals = Vec::new();
    for c in 0..80u32 {
        let hi = (c * 3) << 16;
        for j in 0..60u32 {
            slot_vals.push(hi | (j * 500 + (c % 7)));
        }
    }
    let slot_bytes = Bitmosaic::from_sorted(slot_vals).to_bytes();
    bench_ab(
        "open_admitted (u32 80ch slot)",
        11,
        5_000,
        || BitmosaicView::open(black_box(&slot_bytes)).unwrap().len(),
        || {
            BitmosaicView::open_admitted(black_box(&slot_bytes))
                .unwrap()
                .len()
        },
    );

    // 2. open_admitted vs open (u64 40-forest).
    let mut u64_vals = Vec::new();
    for h in 0..40u64 {
        let hi = (h * 3 + 1) << 32;
        for k in 0..100u64 {
            u64_vals.push(hi | (k * 7));
        }
    }
    let u64_bytes = Bitmosaic64::from_sorted(u64_vals).to_bytes();
    bench_ab(
        "open_admitted (u64 40 forests)",
        11,
        5_000,
        || Bitmosaic64View::open(black_box(&u64_bytes)).unwrap().len(),
        || {
            Bitmosaic64View::open_admitted(black_box(&u64_bytes))
                .unwrap()
                .len()
        },
    );

    // 3. rank_if_present vs contains + rank (Words chunk).
    let words_a_vals = sorted_unique(
        (0..20_000)
            .map(|_| (xorshift(&mut rng) % 65_536) as u32)
            .collect(),
    );
    let words_b_vals = sorted_unique(
        (0..20_000)
            .map(|_| (xorshift(&mut rng) % 65_536) as u32)
            .collect(),
    );
    let words_a = Bitmosaic::from_sorted(words_a_vals.iter().copied());
    let words_b = Bitmosaic::from_sorted(words_b_vals.iter().copied());
    let words_a_bytes = words_a.to_bytes();
    let words_b_bytes = words_b.to_bytes();
    let vw_a = BitmosaicView::open_admitted(&words_a_bytes).unwrap();
    let vw_b = BitmosaicView::open_admitted(&words_b_bytes).unwrap();
    let probes: Vec<u32> = (0..1_024)
        .map(|i| words_a_vals[(i * 19) % words_a_vals.len()])
        .collect();
    bench_ab(
        "rank_if_present (1024 Words)",
        11,
        500,
        || {
            let mut s = 0u64;
            for &v in &probes {
                if vw_a.contains(v) {
                    s = s.wrapping_add(vw_a.rank(v));
                }
            }
            s
        },
        || {
            let mut s = 0u64;
            for &v in &probes {
                if let Some(r) = vw_a.rank_if_present(v) {
                    s = s.wrapping_add(r);
                }
            }
            s
        },
    );

    // 4. combine Words × Words (`and`) vs former scalar for_each + contains.
    bench_ab(
        "combine Words x Words (and)",
        11,
        200,
        || {
            let mut plane = [0u64; 1024];
            for v in words_a.iter() {
                if words_b.contains(v) {
                    plane[(v >> 6) as usize] |= 1u64 << (v & 63);
                }
            }
            plane.iter().map(|w| u64::from(w.count_ones())).sum()
        },
        || words_a.and(&words_b).len(),
    );

    // 5. combine Array × Array (`and`) vs former 8 KiB zero+scatter.
    let arr_a = Bitmosaic::from_sorted(sorted_unique(
        (0..500)
            .map(|_| (xorshift(&mut rng) % 65_536) as u32)
            .collect(),
    ));
    let arr_b = Bitmosaic::from_sorted(sorted_unique(
        (0..500)
            .map(|_| (xorshift(&mut rng) % 65_536) as u32)
            .collect(),
    ));
    bench_ab(
        "combine Array x Array (and)",
        11,
        1_000,
        || {
            let mut plane = [0u64; 1024];
            for v in arr_a.iter() {
                if arr_b.contains(v) {
                    plane[(v >> 6) as usize] |= 1u64 << (v & 63);
                }
            }
            let mut out = Vec::new();
            for (w, &bits0) in plane.iter().enumerate() {
                let mut bits = bits0;
                while bits != 0 {
                    out.push(w as u32 * 64 + bits.trailing_zeros());
                    bits &= bits - 1;
                }
            }
            Bitmosaic::from_sorted(out).len()
        },
        || arr_a.and(&arr_b).len(),
    );

    // 6. combine Stride × Stride (`and`) vs former scatter.
    let st_a = Bitmosaic::from_sorted((0..10_000u32).map(|i| 5 + i * 6));
    let st_b = Bitmosaic::from_sorted((0..6_000u32).map(|i| 1 + i * 10));
    bench_ab(
        "combine Stride x Stride (and)",
        11,
        1_000,
        || {
            let mut plane = [0u64; 1024];
            for v in st_a.iter() {
                if st_b.contains(v) {
                    plane[(v >> 6) as usize] |= 1u64 << (v & 63);
                }
            }
            let mut out = Vec::new();
            for (w, &bits0) in plane.iter().enumerate() {
                let mut bits = bits0;
                while bits != 0 {
                    out.push(w as u32 * 64 + bits.trailing_zeros());
                    bits &= bits - 1;
                }
            }
            Bitmosaic::from_sorted(out).len()
        },
        || st_a.and(&st_b).len(),
    );

    // 7. BitmosaicView::and_into (Words × Words) vs former for_each + contains.
    let inter_len = vw_a.and_len(&vw_b) as usize;
    let mut dest_ctrl = vec![0u32; inter_len];
    let mut dest_fast = vec![0u32; inter_len];
    bench_ab(
        "and_into Words x Words",
        11,
        500,
        || {
            let mut w = 0usize;
            vw_a.for_each(|v| {
                if vw_b.contains(v) {
                    dest_ctrl[w] = v;
                    w += 1;
                }
            });
            w as u64
        },
        || vw_a.and_into(&vw_b, &mut dest_fast) as u64,
    );

    // 8. AndNotRange skew (|A|=16 vs |B|=100_000): seek vs stepping b.
    let skew_a = Bitmosaic::from_sorted((0..16u32).map(|i| i * 6_000 + 17));
    let skew_b = Bitmosaic::from_sorted(0..100_000u32);
    bench_ab(
        "AndNotRange skew (16 vs 100k)",
        11,
        2_000,
        || {
            // Former stepping aligner control.
            let mut a = skew_a.range();
            let mut b = skew_b.range();
            let mut count = 0u64;
            while !a.empty() {
                let x = a.front();
                while !b.empty() && b.front() < x {
                    b.pop_front();
                }
                if !b.empty() && b.front() == x {
                    b.pop_front();
                } else {
                    count += 1;
                }
                a.pop_front();
            }
            count
        },
        || AndNotRange::new(skew_a.range(), skew_b.range()).count() as u64,
    );

    // 9. select64 SWAR vs naive bit-clearing loop (via Words::select on 1024 ranks).
    let select_ranks: Vec<u64> = (0..1_024)
        .map(|i| ((i * 19 + 7) % words_a_vals.len()) as u64)
        .collect();
    let sample_words: Vec<(u64, u32)> = (0..1_024)
        .map(|_| {
            let w = xorshift(&mut rng) | (1u64 << 63) | 1;
            let k = (xorshift(&mut rng) as u32) % w.count_ones();
            (w, k)
        })
        .collect();
    bench_ab(
        "select64 (1024 dense words)",
        11,
        1_000,
        || {
            let mut acc = 0u64;
            for &(mut w, k) in &sample_words {
                for _ in 0..k {
                    w &= w - 1;
                }
                acc = acc.wrapping_add(u64::from(w.trailing_zeros()));
            }
            acc
        },
        || {
            // Compare Words::select (which uses select64) vs naive word loop indirectly,
            // plus direct SWAR kernel timing.
            let mut acc = 0u64;
            for &(word, rank) in &sample_words {
                let s2 = word - ((word >> 1) & 0x5555_5555_5555_5555);
                let s4 = (s2 & 0x3333_3333_3333_3333) + ((s2 >> 2) & 0x3333_3333_3333_3333);
                let s8 =
                    ((s4 + (s4 >> 4)) & 0x0F0F_0F0F_0F0F_0F0F).wrapping_mul(0x0101_0101_0101_0101);
                let gt = ((s8 | 0x8080_8080_8080_8080)
                    - (u64::from(rank) + 1).wrapping_mul(0x0101_0101_0101_0101))
                    & 0x8080_8080_8080_8080;
                let mut pos = gt.trailing_zeros() & !7;
                let mut rem = rank - (((s8 << 8) >> pos) & 0xFF) as u32;
                let low4 = ((s4 >> pos) & 0xF) as u32;
                let go4 = u32::from(rem >= low4);
                rem -= low4 * go4;
                pos += 4 * go4;
                let low2 = ((s2 >> pos) & 0x3) as u32;
                let go2 = u32::from(rem >= low2);
                rem -= low2 * go2;
                pos += 2 * go2;
                let low1 = ((word >> pos) & 1) as u32;
                acc = acc.wrapping_add(u64::from(pos + u32::from(rem >= low1)));
            }
            acc
        },
    );
    black_box(
        select_ranks
            .iter()
            .map(|&k| u64::from(vw_a.select(k).unwrap_or(0)))
            .sum::<u64>(),
    );
}
