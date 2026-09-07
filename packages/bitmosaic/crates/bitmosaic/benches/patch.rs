//! The in-place patch path on the VM slot's own cells: add/1, add/64,
//! add/4096, rem/1 and rem/4096 batches against sets of 10k and 300k
//! members, each cell timed from the same image and censused for
//! allocations.
//!
//! Two fixture families, each as the slot form (a forest root, which a
//! patch keeps) and — for `scattered` — also as the sealed form, an
//! Elias-Fano root, where a patch is a word-granular splice of one global
//! plane. `scattered` is the wave-2.5 fixture (density 1/12, the
//! pseudo-random generator and seeds of the audit's `codec.rs`); `words`
//! is the same cardinality at density 1/2, the shape a VM ordinal domain
//! with holes takes. On a forest a patch touches the chunks the batch
//! names and nothing else.
//!
//! Timing excludes restoring the image (`iter_custom`); every cell reports
//! the allocation census of one warm patch, and a census above zero fails
//! the bench, so `--test` is the gate.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bitmosaic::{Bitmosaic, BitmosaicView, PatchScratch, patch};
use criterion::{Criterion, criterion_group, criterion_main};

struct CountingAllocator;

static TRACK_ALLOCATIONS: AtomicBool = AtomicBool::new(false);
static ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOCATION_BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if TRACK_ALLOCATIONS.load(Ordering::Relaxed) && !pointer.is_null() {
            ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
            ALLOCATION_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let resized = unsafe { System.realloc(pointer, layout, new_size) };
        if TRACK_ALLOCATIONS.load(Ordering::Relaxed) && !resized.is_null() {
            ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
            ALLOCATION_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        }
        resized
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

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

/// `n` distinct values below `universe`, from a seed.
fn distinct(n: usize, universe: u64, seed: u64) -> Vec<u32> {
    let mut s = seed;
    let mut v = std::collections::BTreeSet::new();
    while v.len() < n {
        v.insert((xorshift(&mut s) % universe) as u32);
    }
    v.into_iter().collect()
}

struct Fixture {
    name: String,
    members: Vec<u32>,
    universe: u64,
    /// The sealed form (`to_bytes`) rather than the slot's forest form.
    sealed: bool,
}

fn fixtures() -> Vec<Fixture> {
    let mut out = vec![Fixture {
        name: "two-long-runs".into(),
        members: (0..4096).chain(32768..36864).collect(),
        universe: 65_536,
        sealed: false,
    }];
    for &n in &[10_000usize, 300_000] {
        // The wave-2.5 generator: `xorshift % (n * 12 + 97)`, seeded
        // `0x9E3779B97F4A7C15 ^ n`.
        let universe = n as u64 * 12 + 97;
        let members = distinct(n, universe, 0x9E37_79B9_7F4A_7C15 ^ n as u64);
        out.push(Fixture {
            name: format!("scattered-{}k", n / 1000),
            members: members.clone(),
            universe,
            sealed: false,
        });
        out.push(Fixture {
            name: format!("scattered-{}k-sealed", n / 1000),
            members,
            universe,
            sealed: true,
        });
        let universe = n as u64 * 2;
        out.push(Fixture {
            name: format!("words-{}k", n / 1000),
            members: distinct(n, universe, 0x5EED_0000 ^ n as u64),
            universe,
            sealed: false,
        });
    }
    out
}

/// Batches per the audit's shapes: adds drawn from `universe + 1 + bl`
/// with seed `0xABCDEF + bl` (so some are already members), removes drawn
/// from the members.
fn batches(fixture: &Fixture) -> Vec<(&'static str, Vec<u32>, Vec<u32>)> {
    let mut out = Vec::new();
    for &bl in &[1usize, 64, 4096] {
        let mut s = 0x00AB_CDEF_u64 + bl as u64;
        let adds = sorted_unique(
            (0..bl)
                .map(|_| (xorshift(&mut s) % (fixture.universe + 1 + bl as u64)) as u32)
                .collect(),
        );
        let name: &'static str = match bl {
            1 => "add/1",
            64 => "add/64",
            _ => "add/4096",
        };
        out.push((name, adds, Vec::new()));
    }
    for &bl in &[1usize, 4096] {
        let mut s = 0xFEED_0000u64 + bl as u64;
        let removes = sorted_unique(
            (0..bl)
                .map(|_| fixture.members[(xorshift(&mut s) as usize) % fixture.members.len()])
                .collect(),
        );
        let name: &'static str = if bl == 1 { "rem/1" } else { "rem/4096" };
        out.push((name, Vec::new(), removes));
    }
    // Every member of the top 512-value block: the top chunk's window
    // shrinks by one block.
    let top_block = fixture.members.last().expect("members") & !0x1FF;
    out.push((
        "rem/top-block",
        Vec::new(),
        fixture
            .members
            .iter()
            .copied()
            .filter(|v| *v >= top_block)
            .collect(),
    ));
    out
}

fn census(
    slot: &mut [u8],
    image: &[u8],
    adds: &[u32],
    removes: &[u32],
    scratch: &mut PatchScratch,
) -> (u64, u64) {
    slot[..image.len()].copy_from_slice(image);
    TRACK_ALLOCATIONS.store(false, Ordering::Relaxed);
    ALLOCATION_CALLS.store(0, Ordering::Relaxed);
    ALLOCATION_BYTES.store(0, Ordering::Relaxed);
    TRACK_ALLOCATIONS.store(true, Ordering::Relaxed);
    let report = patch(slot, adds, removes, scratch).expect("patch");
    TRACK_ALLOCATIONS.store(false, Ordering::Relaxed);
    black_box(report);
    (
        ALLOCATION_CALLS.load(Ordering::Relaxed),
        ALLOCATION_BYTES.load(Ordering::Relaxed),
    )
}

fn bench_patch(c: &mut Criterion) {
    for fixture in fixtures() {
        let owned = Bitmosaic::from_sorted(fixture.members.iter().copied());
        let image = if fixture.sealed {
            owned.to_bytes()
        } else {
            owned.to_forest_bytes()
        };
        let root = if BitmosaicView::open(&image).expect("open").is_elias_fano() {
            "ef"
        } else {
            "forest"
        };
        // Native-image slot capacity, excluding the containing VM state's alignment.
        let capacity = (fixture.members.len() + 8192) * 4 + bitmosaic::IMAGE_ID_LEN;
        let mut slot = vec![0u8; capacity];
        let mut scratch = PatchScratch::with_capacity(capacity, 4096);
        let mut group =
            c.benchmark_group(format!("patch/{}[{root},{}B]", fixture.name, image.len()));
        for (cell, adds, removes) in batches(&fixture) {
            // Warm, then census one patch: the bench is also the gate.
            slot[..image.len()].copy_from_slice(&image);
            patch(&mut slot, &adds, &removes, &mut scratch).expect("warm");
            let (calls, bytes) = census(&mut slot, &image, &adds, &removes, &mut scratch);
            eprintln!(
                "census patch/{}/{cell}: allocations={calls} bytes={bytes}",
                fixture.name
            );
            assert_eq!(
                calls, 0,
                "patch/{}/{cell} allocated on a warm scratch",
                fixture.name
            );
            group.bench_function(cell, |b| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        slot[..image.len()].copy_from_slice(&image);
                        let started = Instant::now();
                        let report =
                            patch(&mut slot, &adds, &removes, &mut scratch).expect("patch");
                        total += started.elapsed();
                        black_box(report);
                    }
                    total
                })
            });
        }
        group.finish();
    }
}

criterion_group!(benches, bench_patch);
criterion_main!(benches);
