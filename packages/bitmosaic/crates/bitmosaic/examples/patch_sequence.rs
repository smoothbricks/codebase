//! Same-binary sparse patch control: former dense staging versus compact merge.
//! Run with `cargo run -p bitmosaic --profile bench --features bench-internals
//! --example patch_sequence`. Restore and fixture construction are not timed.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::BTreeSet;
use std::hint::black_box;
use std::time::Instant;

use bitmosaic::{Bitmosaic, BitmosaicView, PatchScratch, patch, patch_sequence};

thread_local! {
    static ALLOCATION: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
}
struct Counting;
fn charge(bytes: usize) {
    ALLOCATION.with(|count| {
        let (calls, total) = count.get();
        count.set((calls + 1, total + bytes));
    });
}
// SAFETY: allocation ownership and layouts are forwarded unchanged to System.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        charge(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        charge(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        charge(size);
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn main() {
    // Positive control for the allocator, outside all measurements.
    ALLOCATION.with(|count| count.set((0, 0)));
    drop(black_box(vec![0u8; 97]));
    assert!(ALLOCATION.with(|count| count.get().0) > 0);
    for family in ["stride", "cone", "array"] {
        for n in [2u32, 16, 32, 128, 512, 1024, 4094, 4095] {
            if (family == "cone" && !(32..=1024).contains(&n)) || (family == "array" && n == 2) {
                continue;
            }
            let members: Vec<_> = if family == "array" {
                let mut rng = 0x1234_5678_9abc_def0u64;
                let mut values = BTreeSet::new();
                while values.len() < n as usize {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    values.insert((rng as u32) & 0xffff);
                }
                values.into_iter().collect()
            } else {
                (0..n)
                    .map(|i| 16 + i * 8 + if family == "cone" { i % 3 } else { 0 })
                    .collect()
            };
            let owned = Bitmosaic::from_sorted(members.iter().copied());
            let image = owned.to_forest_bytes();
            let adds = [members[0], members[n as usize / 2] + 1];
            let removes = [members[n as usize / 2]];
            let mut expected: BTreeSet<_> = members.iter().copied().collect();
            let added = adds
                .iter()
                .filter(|value| !expected.contains(value))
                .count() as u32;
            expected.extend(adds);
            let removed = removes
                .iter()
                .filter(|value| expected.contains(value))
                .count() as u32;
            for value in removes {
                expected.remove(&value);
            }
            let expected: Vec<_> = expected.into_iter().collect();
            let canonical = Bitmosaic::from_sorted(expected.iter().copied()).to_forest_bytes();
            let mut slot = vec![0u8; 32 * 1024];
            let mut scratch = PatchScratch::try_new().expect("scratch");
            scratch.try_reserve_for(slot.len(), 1).expect("reserve");
            for dense in [true, false] {
                patch_sequence::force_dense(dense);
                slot[..image.len()].copy_from_slice(&image);
                let report = patch(&mut slot, &adds, &removes, &mut scratch).expect("patch");
                assert_eq!(&slot[..report.serialized_len], canonical);
                assert_eq!(
                    (report.added, report.removed, report.len),
                    (added, removed, expected.len() as u64)
                );
                assert_eq!(
                    BitmosaicView::open_verified(&slot)
                        .unwrap()
                        .range()
                        .collect::<Vec<_>>(),
                    expected
                );
            }
            let mut samples = [Vec::new(), Vec::new()];
            let mut allocations = [(0, 0); 2];
            const ITERS: usize = 256;
            for rep in 0..17 {
                for lane in if rep % 2 == 0 { [0, 1] } else { [1, 0] } {
                    patch_sequence::force_dense(lane == 0);
                    let mut elapsed = 0u128;
                    ALLOCATION.with(|count| count.set((0, 0)));
                    for _ in 0..ITERS {
                        slot[..image.len()].copy_from_slice(&image);
                        let start = Instant::now();
                        black_box(
                            patch(
                                black_box(&mut slot),
                                black_box(&adds),
                                black_box(&removes),
                                black_box(&mut scratch),
                            )
                            .expect("patch"),
                        );
                        elapsed += start.elapsed().as_nanos();
                    }
                    allocations[lane] = ALLOCATION.with(|count| count.get());
                    assert_eq!(allocations[lane], (0, 0), "warm patch allocates");
                    samples[lane].push(elapsed as f64 / ITERS as f64);
                }
            }
            for sample in &mut samples {
                sample.sort_by(f64::total_cmp);
            }
            println!(
                "family={family} members={n} adds=2 removes=1 image_bytes={} dense_ns={:.1} compact_ns={:.1} speedup={:.3} dense_range={:.1}..{:.1} compact_range={:.1}..{:.1} warm_allocations={allocations:?}",
                image.len(),
                samples[0][8],
                samples[1][8],
                samples[0][8] / samples[1][8],
                samples[0][0],
                samples[0][16],
                samples[1][0],
                samples[1][16]
            );
        }
    }
    patch_sequence::force_dense(false);
}
