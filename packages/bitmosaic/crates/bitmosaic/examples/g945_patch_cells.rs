//! G945 direction cell: the in-place patch against the restage arm on the
//! VM slot's own batch cells, same binary, both arms interleaved per cell.
//!
//! The restage arm is what the wave-2.5 audit measured for bitmosaic before
//! this unit existed: open the slot's string, walk every member into a
//! fresh `BitmosaicBuilder`, fold the batch in, freeze, and write the forest
//! back into the slot. The patch arm is `patch`. Both start every rep from
//! the same image; the restore is outside the timed span.
//!
//! Paired A/A observations for each arm, interleaved with reversed order on
//! alternating repetitions. Report sample ranges, not a blanket speed claim.

use std::hint::black_box;
use std::time::Instant;

use bitmosaic::{Bitmosaic, BitmosaicBuilder, BitmosaicView, PatchScratch, patch};

const REPS: usize = 8;

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

fn distinct(n: usize, universe: u64, seed: u64) -> Vec<u32> {
    let mut s = seed;
    let mut v = std::collections::BTreeSet::new();
    while v.len() < n {
        v.insert((xorshift(&mut s) % universe) as u32);
    }
    v.into_iter().collect()
}

fn restage(slot: &mut [u8], adds: &[u32], removes: &[u32]) -> usize {
    let mut builder = BitmosaicBuilder::new();
    BitmosaicView::open(slot).expect("open").for_each(|v| {
        builder.insert(v);
    });
    for &v in adds {
        builder.insert(v);
    }
    for &v in removes {
        builder.remove(v);
    }
    builder
        .freeze()
        .write_forest_into_slice(slot)
        .expect("fits")
}

/// One observation, with image restoration outside the timed span.
fn sample(slot: &mut [u8], image: &[u8], mut run: impl FnMut(&mut [u8]) -> usize) -> u128 {
    slot[..image.len()].copy_from_slice(image);
    let started = Instant::now();
    black_box(run(slot));
    started.elapsed().as_nanos()
}

fn main() {
    for &n in &[10_000usize, 300_000] {
        let fixtures = [
            (
                "scattered",
                n as u64 * 12 + 97,
                0x9E37_79B9_7F4A_7C15 ^ n as u64,
            ),
            ("words", n as u64 * 2, 0x5EED_0000 ^ n as u64),
            ("runs", n.div_ceil(8192) as u64 * 65_536, 0),
        ];
        for (family, universe, seed) in fixtures {
            let members = if family == "runs" {
                (0..n as u32)
                    .map(|i| i / 8192 * 65_536 + (i % 8192) / 4096 * 32_768 + i % 4096)
                    .collect()
            } else {
                distinct(n, universe, seed)
            };
            let image = Bitmosaic::from_sorted(members.iter().copied()).to_forest_bytes();
            let capacity = (n + 8192) * 4 + bitmosaic::IMAGE_ID_LEN;
            let mut slot = vec![0u8; capacity];
            let mut scratch = PatchScratch::with_capacity(capacity, 4096);
            let mut cells: Vec<(&str, Vec<u32>, Vec<u32>)> = Vec::new();
            for &bl in &[1usize, 64, 4096] {
                let mut s = 0x00AB_CDEF_u64 + bl as u64;
                let adds = sorted_unique(
                    (0..bl)
                        .map(|_| (xorshift(&mut s) % (universe + 1 + bl as u64)) as u32)
                        .collect(),
                );
                let name = match bl {
                    1 => "add/1",
                    64 => "add/64",
                    _ => "add/4096",
                };
                cells.push((name, adds, Vec::new()));
            }
            for &bl in &[1usize, 4096] {
                let mut s = 0xFEED_0000u64 + bl as u64;
                let removes = sorted_unique(
                    (0..bl)
                        .map(|_| members[(xorshift(&mut s) as usize) % members.len()])
                        .collect(),
                );
                cells.push((
                    if bl == 1 { "rem/1" } else { "rem/4096" },
                    Vec::new(),
                    removes,
                ));
            }
            // Every member of the top 512-value block: the window of the
            // top chunk shrinks by one block, a domain retiring its
            // highest ordinals.
            let top_block = members.last().expect("members") & !0x1FF;
            cells.push((
                "rem/top-block",
                Vec::new(),
                members
                    .iter()
                    .copied()
                    .filter(|v| *v >= top_block)
                    .collect(),
            ));
            for (batch, adds, removes) in &cells {
                slot[..image.len()].copy_from_slice(&image);
                let length = restage(&mut slot, adds, removes);
                let expected = slot[..length].to_vec();
                slot[..image.len()].copy_from_slice(&image);
                let report = patch(&mut slot, adds, removes, &mut scratch).expect("patch");
                assert_eq!(
                    &slot[..report.serialized_len],
                    expected,
                    "both arms must emit identical bytes"
                );
                let mut observations = [(u128::MAX, 0u128); 4];
                for rep in 0..REPS {
                    let order = if rep % 2 == 0 {
                        [0, 1, 2, 3]
                    } else {
                        [3, 2, 1, 0]
                    };
                    for lane in order {
                        let elapsed = if lane % 2 == 0 {
                            sample(&mut slot, &image, |slot| restage(slot, adds, removes))
                        } else {
                            sample(&mut slot, &image, |slot| {
                                patch(slot, adds, removes, &mut scratch)
                                    .expect("patch")
                                    .serialized_len
                            })
                        };
                        observations[lane].0 = observations[lane].0.min(elapsed);
                        observations[lane].1 = observations[lane].1.max(elapsed);
                    }
                }
                println!(
                    "MG-G945 fixture={family}-{}k batch={batch} restage_ns={} patch_ns={} restage_repeat_ns={} patch_repeat_ns={} restage_a_range_ns={:?} patch_a_range_ns={:?} restage_b_range_ns={:?} patch_b_range_ns={:?}",
                    n / 1000,
                    observations[0].0,
                    observations[1].0,
                    observations[2].0,
                    observations[3].0,
                    observations[0],
                    observations[1],
                    observations[2],
                    observations[3],
                );
            }
        }
    }
}
