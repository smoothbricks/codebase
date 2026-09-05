//! The seen-set cells: admit (a new id), miss (an absent id judged, not
//! admitted), and hit (a duplicate) at 10k and 300k resident ids, for the
//! trie carrier (Bytes namespace) and the AXR1 carrier (Ordinal namespace).
//! Ids share a 24-byte prefix, the shape a hashed filter never sees and a
//! trie must descend through. Wall time is the mean of timed probes after
//! the set is filled; the allocation census counts every heap allocation
//! the timed probes make.
//!
//! The control arm — the bloom filter the seen-set replaced — was measured
//! on these same ids before it was deleted; its numbers are recorded beside
//! the seen-set's in the campaign spec (93 V5), not kept alive here.
//!
//! Run: `cargo bench -p columine-event-processor --bench dedup_cells`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use columine_event_processor::{CollisionPolicy, IdNamespace, SeenSet};

struct Counting;

static ALLOCS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

// ---------------------------------------------------------------------------
// Ids: a shared 24-byte prefix, then a decimal ordinal, scattered.
// ---------------------------------------------------------------------------

/// A bijection on `u32`, so resident and absent ids never collide.
fn scatter(i: u32) -> u32 {
    i.wrapping_mul(0x9E37_79B1).rotate_left(13)
}

fn byte_ids(count: u32, base: u32) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| format!("sig:orders:us-east-1:{}", scatter(i + base)).into_bytes())
        .collect()
}

fn ordinal_ids(count: u32, base: u32) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| (i + base).to_string().into_bytes())
        .collect()
}

struct Cell {
    ns: f64,
    allocs: usize,
}

fn timed(probes: usize, mut probe: impl FnMut()) -> Cell {
    for _ in 0..probes.min(1024) {
        probe();
    }
    let before = ALLOCS.load(Ordering::Relaxed);
    let start = Instant::now();
    for _ in 0..probes {
        probe();
    }
    let elapsed = start.elapsed();
    let allocs = ALLOCS.load(Ordering::Relaxed) - before;
    Cell {
        ns: elapsed.as_nanos() as f64 / probes as f64,
        allocs,
    }
}

fn seen_cells(label: &str, resident: u32, namespace: IdNamespace) {
    let ids = match namespace {
        IdNamespace::Bytes => byte_ids(resident, 0),
        IdNamespace::Ordinal => ordinal_ids(resident, 0),
    };
    // 1024 extra absent ids feed the admit cell's warm-up probes.
    let absent = match namespace {
        IdNamespace::Bytes => byte_ids(resident + 1024, resident),
        IdNamespace::Ordinal => ordinal_ids(resident + 1024, resident),
    };
    let ceiling = resident * 2 + 2048;
    let mut set = SeenSet::new(CollisionPolicy::Discard, ceiling, 1024);
    for (i, id) in ids.iter().enumerate() {
        set.judge(id, namespace).unwrap();
        if i % 256 == 255 {
            set.commit(i as u64 / 256);
        }
    }
    set.commit(u64::from(resident) / 256 + 1);
    let probes = resident as usize;

    let mut cursor = 0usize;
    let hit = timed(probes, || {
        let id = &ids[cursor % probes];
        cursor += 1;
        assert!(matches!(
            set.judge(id, namespace),
            Ok(columine_event_processor::Judgment::Duplicate { .. })
        ));
    });
    set.abandon();

    // A miss judges an absent id and retracts it, so the set stays at
    // `resident` and the cost is the descent to the first divergent byte.
    let mut cursor = 0usize;
    let miss = timed(probes, || {
        let id = &absent[cursor % absent.len()];
        cursor += 1;
        assert_eq!(
            set.judge(id, namespace),
            Ok(columine_event_processor::Judgment::New)
        );
        set.abandon();
    });

    // Admit: the absent ids enter for real, committed in 256-row batches.
    let mut cursor = 0usize;
    let admit = timed(probes, || {
        let id = &absent[cursor];
        cursor += 1;
        set.judge(id, namespace).unwrap();
        if cursor.is_multiple_of(256) {
            set.commit(1_000_000 + cursor as u64 / 256);
        }
    });
    set.commit(2_000_000);
    println!(
        "{label:<14} {resident:>7} | admit {:>7.1} ns ({} allocs) | miss {:>7.1} ns ({} allocs) | hit {:>7.1} ns ({} allocs)",
        admit.ns, admit.allocs, miss.ns, miss.allocs, hit.ns, hit.allocs
    );
}

fn main() {
    for resident in [10_000u32, 300_000] {
        seen_cells("trie(bytes)", resident, IdNamespace::Bytes);
        seen_cells("axr1(ordinal)", resident, IdNamespace::Ordinal);
    }
}
