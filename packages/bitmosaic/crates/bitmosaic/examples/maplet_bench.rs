//! Byte-size census and same-binary interleaved A/B lookup benchmark for
//! `bitmosaic::Maplet` (live 64-byte SIMD bucketed) vs `bitmosaic::MapletView`
//! (sealed Format 2 V2View + packed value plane).

#[cfg(target_arch = "wasm32")]
fn main() {}

#[cfg(not(target_arch = "wasm32"))]
fn main() {
    use std::hint::black_box;
    use std::time::Instant;

    use bitmosaic::{Maplet, MapletView};

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
        ops_per_iter: usize,
        mut live_arm: impl FnMut() -> u64,
        mut sealed_arm: impl FnMut() -> u64,
    ) {
        black_box(live_arm());
        black_box(sealed_arm());
        let denom = (iters * ops_per_iter) as f64;
        let mut t_live = Vec::with_capacity(rounds);
        let mut t_sealed = Vec::with_capacity(rounds);
        let mut aa_ratios = Vec::with_capacity(rounds);
        for r in 0..rounds {
            let (dl, ds) = if r % 2 == 0 {
                let t0 = Instant::now();
                let mut acc = 0u64;
                for _ in 0..iters {
                    acc = acc.wrapping_add(black_box(live_arm()));
                }
                black_box(acc);
                let l = t0.elapsed().as_nanos() as f64 / denom;

                let t1 = Instant::now();
                let mut acc2 = 0u64;
                for _ in 0..iters {
                    acc2 = acc2.wrapping_add(black_box(sealed_arm()));
                }
                black_box(acc2);
                let s = t1.elapsed().as_nanos() as f64 / denom;
                (l, s)
            } else {
                let t1 = Instant::now();
                let mut acc2 = 0u64;
                for _ in 0..iters {
                    acc2 = acc2.wrapping_add(black_box(sealed_arm()));
                }
                black_box(acc2);
                let s = t1.elapsed().as_nanos() as f64 / denom;

                let t0 = Instant::now();
                let mut acc = 0u64;
                for _ in 0..iters {
                    acc = acc.wrapping_add(black_box(live_arm()));
                }
                black_box(acc);
                let l = t0.elapsed().as_nanos() as f64 / denom;
                (l, s)
            };
            let t2 = Instant::now();
            let mut acc3 = 0u64;
            for _ in 0..iters {
                acc3 = acc3.wrapping_add(black_box(live_arm()));
            }
            black_box(acc3);
            let dl2 = t2.elapsed().as_nanos() as f64 / denom;
            aa_ratios.push((dl2 / dl).max(dl / dl2) - 1.0);
            t_live.push(dl);
            t_sealed.push(ds);
        }
        let n = rounds as f64;
        let mean_l = t_live.iter().sum::<f64>() / n;
        let mean_s = t_sealed.iter().sum::<f64>() / n;
        let aa_pct = aa_ratios.iter().sum::<f64>() / n * 100.0;
        println!(
            "{name:<22} | live={mean_l:6.2} ns/op  sealed={mean_s:6.2} ns/op | ratio={:5.2}x | A/A={aa_pct:4.1}%",
            mean_s / mean_l
        );
    }

    println!("=== bitmosaic::maplet Byte-Size Census (32-bit random keys, u8 values) ===");
    println!(
        "{:<8} | {:>10} | {:>10} | {:>10} | {:>12}",
        "N", "Sealed (B)", "B / entry", "u16 Sealed", "Multi-2x (B)"
    );
    let mut rng = 0xCAFE_BEEF_1234_5678u64;
    for &n in &[0usize, 1, 4, 16, 64, 1024, 16_384] {
        let mut m_u8 = Maplet::with_capacity(n);
        let mut m_u16 = Maplet::with_capacity(n);
        let mut m_mv = Maplet::with_capacity(n * 2);
        for i in 0..n {
            let k = (xorshift(&mut rng) >> 16) as u32;
            m_u8.insert(k, (i & 0xFF) as u16);
            m_u16.insert(k, ((i * 7) % 50_000 + 300) as u16);
            m_mv.insert(k, (i & 0xFF) as u16);
            m_mv.insert(k, ((i + 1) & 0xFF) as u16);
        }
        let s_u8 = m_u8.seal();
        let s_u16 = m_u16.seal();
        let s_mv = m_mv.seal();
        let bpe = if n == 0 {
            0.0
        } else {
            s_u8.len() as f64 / n as f64
        };
        println!(
            "{n:<8} | {:>10} | {:>10.2} | {:>10} | {:>12}",
            s_u8.len(),
            bpe,
            s_u16.len(),
            s_mv.len()
        );
    }

    println!("\n=== bitmosaic::maplet Lookup Latency (50% hit / 50% miss, 16 paired rounds) ===");
    let probe_count = 1024usize;
    for &n in &[1usize, 4, 16, 64, 1024, 16_384] {
        let mut m = Maplet::with_capacity(n);
        let mut keys = Vec::with_capacity(n);
        for i in 0..n {
            let k = (xorshift(&mut rng) >> 16) as u32;
            keys.push(k);
            m.insert(k, (i & 0xFF) as u16);
        }
        let sealed = m.seal();
        let view = MapletView::open_admitted(&sealed).unwrap();

        let mut probes = Vec::with_capacity(probe_count);
        for i in 0..probe_count {
            if i % 2 == 0 {
                probes.push(keys[i % n]);
            } else {
                probes.push((xorshift(&mut rng) >> 16) as u32);
            }
        }

        let iters = 500usize;
        let label = format!("N={n}");
        bench_ab(
            &label,
            16,
            iters,
            probe_count,
            || {
                let mut acc = 0u64;
                for &k in black_box(&probes) {
                    acc = acc.wrapping_add(u64::from(m.get(k).unwrap_or(0)));
                }
                acc
            },
            || {
                let mut acc = 0u64;
                for &k in black_box(&probes) {
                    acc = acc.wrapping_add(u64::from(view.get(k).unwrap_or(0)));
                }
                acc
            },
        );
    }
}
