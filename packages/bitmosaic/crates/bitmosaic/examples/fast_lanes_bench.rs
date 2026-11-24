//! Same-binary interleaved A/B direction benchmark for bitmosaic fast lanes.
//!
//! Compares each fast lane against its previous algorithm in one binary
//! with 24 interleaved paired rounds and a like-arm A/A floor.

#[cfg(target_arch = "wasm32")]
fn main() {}

#[cfg(not(target_arch = "wasm32"))]
fn main() {
    use std::hint::black_box;
    use std::time::Instant;

    use bitmosaic::{
        AndNotRange, Bitmosaic, Bitmosaic64, Bitmosaic64View, BitmosaicView, Dir64V2View, Range,
    };

    const ROUNDS: usize = 24;

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
        // Equivalence check + warmup before timing.
        let c0 = black_box(control());
        let f0 = black_box(fast());
        assert_eq!(c0, f0, "benchmark {name}: control ({c0}) != fast ({f0})");
        let mut t_ctrl = Vec::with_capacity(rounds);
        let mut t_fast = Vec::with_capacity(rounds);
        let mut aa_ratios = Vec::with_capacity(rounds);
        for r in 0..rounds {
            // Interleaved order alternates each round to cancel host-load drift.
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
            // Like-arm A/A floor on the fast arm.
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
            "{name:<36} ctrl={med_c:9.1} ns  fast={med_f:8.1} ns  speedup={speedup:7.2}x  disjoint={disjoint:<5}  A/A={aa_med:4.1}%  (rounds={rounds})"
        );
    }

    let mut rng = 0xBEEF_1234_5678_9ABCu64;

    // 1. andnot_skewed_seek (|A|=64, |B|=100_000): AndNotRange::align seek vs stepping b.
    let skew_a = Bitmosaic::from_sorted((0..64u32).map(|i| i * 1_550 + 17));
    let skew_b = Bitmosaic::from_sorted(0..100_000u32);
    let skew_a_bytes = skew_a.to_bytes();
    let skew_b_bytes = skew_b.to_bytes();
    let skew_va = BitmosaicView::open_admitted(&skew_a_bytes).unwrap();
    let skew_vb = BitmosaicView::open_admitted(&skew_b_bytes).unwrap();
    bench_ab(
        "andnot_skewed_seek (64 vs 100k)",
        ROUNDS,
        500,
        || {
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
    bench_ab(
        "andnot_skewed_seek_wire (64 vs 100k)",
        ROUNDS,
        500,
        || {
            let mut a = skew_va.range();
            let mut b = skew_vb.range();
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
        || AndNotRange::new(skew_va.range(), skew_vb.range()).count() as u64,
    );

    // 2. swar_select64 vs serial `bits &= bits - 1` (1024 dense words).
    let sample_words: Vec<(u64, u32)> = (0..1_024)
        .map(|_| {
            let w = xorshift(&mut rng) | (1u64 << 63) | 1;
            let k = (xorshift(&mut rng) as u32) % w.count_ones();
            (w, k)
        })
        .collect();
    bench_ab(
        "swar_select64 (1024 dense words)",
        ROUNDS,
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

    // 3a. cview_stride_contains (owned & wire reciprocal vs udiv `(v - first) / stride`).
    let stride_first = 13u32;
    let stride_step = 7u32;
    let stride_count = 4_000u32;
    let stride_set =
        Bitmosaic::from_sorted((0..stride_count).map(|i| stride_first + i * stride_step));
    let stride_bytes = stride_set.to_bytes();
    let stride_view = BitmosaicView::open_admitted(&stride_bytes).unwrap();
    let stride_probes: Vec<u32> = (0..1_024)
        .map(|i| {
            let idx = (xorshift(&mut rng) as u32) % stride_count;
            stride_first + idx * stride_step + (i & 1)
        })
        .collect();
    // Precomputed Lemire reciprocal matching `codec::stride_magic(7)`.
    let stride_magic = (1u64 << 32).div_ceil(u64::from(stride_step));
    bench_ab(
        "stride_contains_kernel (recip/udiv)",
        ROUNDS,
        1_000,
        || {
            let mut hits = 0u64;
            let first = black_box(stride_first);
            let step = black_box(stride_step);
            let len = black_box(stride_count - 1);
            for &v in &stride_probes {
                if v >= first {
                    let d = v - first;
                    let q = d / step;
                    if q <= len && d.is_multiple_of(step) {
                        hits += 1;
                    }
                }
            }
            hits
        },
        || {
            let mut hits = 0u64;
            let first = black_box(stride_first);
            let step = black_box(stride_step);
            let len = black_box(stride_count - 1);
            let magic = black_box(stride_magic);
            for &v in &stride_probes {
                if v >= first {
                    let q = ((u64::from(v - first) * magic) >> 32) as u32;
                    if q <= len && first + q * step == v {
                        hits += 1;
                    }
                }
            }
            hits
        },
    );
    bench_ab(
        "cview_stride_contains (owned/wire)",
        ROUNDS,
        1_000,
        || {
            // Wire open-per-probe vs owned pre-decoded reciprocal.
            let mut hits = 0u64;
            for &v in &stride_probes {
                hits += u64::from(stride_view.contains(v));
            }
            hits
        },
        || {
            let mut hits = 0u64;
            for &v in &stride_probes {
                hits += u64::from(stride_set.contains(v));
            }
            hits
        },
    );

    // 3b. cone_contains (owned & wire reciprocal + counted bisection vs checked_div + linear scan).
    let mut cone_vals = Vec::with_capacity(900);
    let mut c_rng = 0xC04E_1234_5678_9ABCu64;
    for i in 0..900u32 {
        cone_vals.push(400 + i * 61 + (xorshift(&mut c_rng) % 45) as u32);
    }
    let cone_vals = sorted_unique(cone_vals);
    let cone_set = Bitmosaic::from_sorted(cone_vals.iter().copied());
    let cone_bytes = cone_set.to_bytes();
    let cone_view = BitmosaicView::open_admitted(&cone_bytes).unwrap();
    let cone_first = cone_vals[0];
    let cone_last = *cone_vals.last().unwrap();
    let cone_n = cone_vals.len();
    let cone_scale = (u64::from(cone_last - cone_first) << 32) / (cone_n as u64 - 1);
    let mut cone_res = Vec::with_capacity(cone_n);
    let mut cone_eps = 0u8;
    for (i, &v) in cone_vals.iter().enumerate() {
        let pred = i64::from(cone_first) + ((i as u64 * cone_scale) >> 32) as i64;
        let r = (i64::from(v) - pred) as i8;
        cone_eps = cone_eps.max(r.unsigned_abs());
        cone_res.push(r);
    }
    let cone_probes: Vec<u32> = (0..1_024)
        .map(|i| {
            let v = cone_vals[(xorshift(&mut rng) as usize) % cone_vals.len()];
            v + (i & 1)
        })
        .collect();
    bench_ab(
        "cone_contains_owned (1024, eps~40)",
        ROUNDS,
        400,
        || {
            let mut hits = 0u64;
            let scale = black_box(cone_scale);
            let first = black_box(cone_first);
            let margin = 2 + i64::from(cone_eps);
            for &v in &cone_probes {
                let delta = v.saturating_sub(first);
                let guess = (u64::from(delta) << 32).checked_div(scale).unwrap_or(0) as i64;
                let lo = (guess - margin).max(0).min(cone_n as i64) as usize;
                let hi = ((guess + margin + 1) as usize).min(cone_n);
                for (i, &res) in cone_res.iter().enumerate().take(hi).skip(lo) {
                    let m = (i64::from(first) + ((i as u64 * scale) >> 32) as i64 + i64::from(res))
                        as u32;
                    if m >= v {
                        hits += u64::from(m == v);
                        break;
                    }
                }
            }
            hits
        },
        || {
            let mut hits = 0u64;
            for &v in &cone_probes {
                hits += u64::from(cone_set.contains(v));
            }
            hits
        },
    );
    bench_ab(
        "cone_contains_wire (1024, eps~40)",
        ROUNDS,
        400,
        || {
            let mut hits = 0u64;
            let scale = black_box(cone_scale);
            let first = black_box(cone_first);
            let margin = 2 + i64::from(cone_eps);
            for &v in &cone_probes {
                let delta = v.saturating_sub(first);
                let guess = (u64::from(delta) << 32).checked_div(scale).unwrap_or(0) as i64;
                let lo = (guess - margin).max(0).min(cone_n as i64) as usize;
                let hi = ((guess + margin + 1) as usize).min(cone_n);
                for (i, &res) in cone_res.iter().enumerate().take(hi).skip(lo) {
                    let m = (i64::from(first) + ((i as u64 * scale) >> 32) as i64 + i64::from(res))
                        as u32;
                    if m >= v {
                        hits += u64::from(m == v);
                        break;
                    }
                }
            }
            hits
        },
        || {
            let mut hits = 0u64;
            for &v in &cone_probes {
                hits += u64::from(cone_view.contains(v));
            }
            hits
        },
    );

    // 4a. bitmosaic64_view_and_len_skewed (8 forests vs 512 forests): galloping vs linear walk.
    let mut big64_vals = Vec::new();
    for h in 0..512u64 {
        let hi = (h * 7 + 1) << 32;
        for k in 0..16u64 {
            big64_vals.push(hi | (k * 100 + 3));
        }
    }
    let mut small64_vals = Vec::new();
    for &h in &[0u64, 64, 128, 192, 256, 320, 400, 511] {
        let hi = (h * 7 + 1) << 32;
        for k in 0..8u64 {
            small64_vals.push(hi | (k * 200 + 3));
        }
    }
    let big64 = Bitmosaic64::from_sorted(big64_vals.iter().copied());
    let small64 = Bitmosaic64::from_sorted(small64_vals.iter().copied());
    let big64_bytes = big64.to_bytes();
    let small64_bytes = small64.to_bytes();
    let v_big64 = Bitmosaic64View::open_admitted(&big64_bytes).unwrap();
    let v_small64 = Bitmosaic64View::open_admitted(&small64_bytes).unwrap();
    let big_highs: Vec<u32> = (0..512u32).map(|h| h * 7 + 1).collect();
    let small_highs: Vec<u32> = [0u32, 64, 128, 192, 256, 320, 400, 511]
        .iter()
        .map(|&h| h * 7 + 1)
        .collect();
    let big_children: Vec<Vec<u8>> = (0..512u32)
        .map(|_| Bitmosaic::from_sorted((0..16u32).map(|k| k * 100 + 3)).to_bytes())
        .collect();
    let small_children: Vec<Vec<u8>> = (0..8u32)
        .map(|_| Bitmosaic::from_sorted((0..8u32).map(|k| k * 200 + 3)).to_bytes())
        .collect();
    bench_ab(
        "bitmosaic64_view_and_len_skewed",
        ROUNDS,
        2_000,
        || {
            // Former linear two-pointer high-32 directory scan.
            let (mut i, mut j, mut count) = (0usize, 0usize, 0u64);
            while i < small_highs.len() && j < big_highs.len() {
                let (ha, hb) = (small_highs[i], big_highs[j]);
                if ha == hb {
                    let a = BitmosaicView::open_admitted(&small_children[i]).unwrap();
                    let b = BitmosaicView::open_admitted(&big_children[j]).unwrap();
                    count += a.and_len(&b);
                    i += 1;
                    j += 1;
                } else if ha < hb {
                    i += 1;
                } else {
                    j += 1;
                }
            }
            count
        },
        || v_small64.and_len(&v_big64),
    );

    // 4b. dir64_v2_and_len_skewed (Format 2 Dir64V2View: 8 vs 512 children).
    let v2_big_bytes = big64.to_v2_bytes();
    let v2_small_bytes = small64.to_v2_bytes();
    let d_big64 = Dir64V2View::open(&v2_big_bytes).unwrap();
    let d_small64 = Dir64V2View::open(&v2_small_bytes).unwrap();
    bench_ab(
        "dir64_v2_and_len_skewed (8 vs 512)",
        ROUNDS,
        2_000,
        || {
            let (mut i, mut j, mut count) = (0usize, 0usize, 0u64);
            while i < small_highs.len() && j < big_highs.len() {
                let (ha, hb) = (small_highs[i], big_highs[j]);
                if ha == hb {
                    let a = BitmosaicView::open_admitted(&small_children[i]).unwrap();
                    let b = BitmosaicView::open_admitted(&big_children[j]).unwrap();
                    count += a.and_len(&b);
                    i += 1;
                    j += 1;
                } else if ha < hb {
                    i += 1;
                } else {
                    j += 1;
                }
            }
            count
        },
        || d_small64.and_len(&d_big64),
    );

    // 5. words_range_seek (800-word jump inside dense Words chunk: dir jump vs word scan).
    let dense_vals = sorted_unique(
        (0..20_000)
            .map(|_| (xorshift(&mut rng) % 65_536) as u32)
            .collect(),
    );
    let dense_set = Bitmosaic::from_sorted(dense_vals.iter().copied());
    let dense_bytes = dense_set.to_bytes();
    let dense_view = BitmosaicView::open_admitted(&dense_bytes).unwrap();
    let mut dense_words = [0u64; 1024];
    for &v in &dense_vals {
        dense_words[(v >> 6) as usize] |= 1u64 << (v & 63);
    }
    let jump_target = 52_000u32;
    bench_ab(
        "words_range_seek (owned 800-word)",
        ROUNDS,
        5_000,
        || {
            // Former word-by-word scan to track remaining.
            let mut rem = dense_vals.len() as u32;
            let goal = (jump_target >> 6) as usize;
            let mask = !((1u64 << (jump_target & 63)) - 1);
            for &word in dense_words.iter().take(goal) {
                rem -= word.count_ones();
            }
            let landing = dense_words[goal];
            rem -= (landing & !mask).count_ones();
            u64::from(rem)
        },
        || {
            let mut r = dense_set.range();
            r.seek(black_box(jump_target));
            r.len() as u64
        },
    );
    bench_ab(
        "words_range_seek (wire 800-word)",
        ROUNDS,
        5_000,
        || {
            let mut rem = dense_vals.len() as u32;
            let goal = (jump_target >> 6) as usize;
            let mask = !((1u64 << (jump_target & 63)) - 1);
            for &word in dense_words.iter().take(goal) {
                rem -= word.count_ones();
            }
            let landing = dense_words[goal];
            rem -= (landing & !mask).count_ones();
            u64::from(rem)
        },
        || {
            let mut r = dense_view.range();
            r.seek(black_box(jump_target));
            r.remaining()
        },
    );
}
