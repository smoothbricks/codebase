//! The batch-mutation cells of the BITMAP slot, measured through the VM's own
//! entry points (`batch_bitmap_add` / `batch_bitmap_remove`) on a slot laid
//! out by `bitmap_payload_capacity`: add/1, add/64, add/4096, rem/1,
//! rem/4096 against 10k- and 300k-member scattered sets. Wall time is the
//! mean of timed iterations after a warm-up; the allocation census counts
//! every heap allocation the timed call makes, so a steady path that
//! allocates is visible as a number rather than a suspicion.
//!
//! Run: `cargo bench -p columine-vm --bench bitmap_cells`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use columine_types::types::{
    BITMAP_SERIALIZED_LEN_BYTES, ErrorCode, SLOT_META_SIZE, STATE_FORMAT_VERSION,
    STATE_HEADER_SIZE, STATE_MAGIC, SlotMetaOffset, SlotType, StateHeaderOffset,
};
use columine_vm::bitmap_ops::{
    BitmapEnv, batch_bitmap_add, batch_bitmap_remove, bitmap_payload_capacity,
};
use columine_vm::hooks::NoVm;
use columine_vm::meta::SlotMetaView;

struct Counting;

static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(new_size, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// One BITMAP slot laid out exactly as `state_init` would: header, one
/// metadata record, then `[serialized_len][payload]`.
fn init_slot(capacity: u32) -> (Vec<u8>, SlotMetaView) {
    let data_offset = STATE_HEADER_SIZE + SLOT_META_SIZE;
    let total = data_offset + BITMAP_SERIALIZED_LEN_BYTES + bitmap_payload_capacity(capacity);
    let mut state = vec![0u8; total as usize];
    state[StateHeaderOffset::MAGIC as usize..StateHeaderOffset::MAGIC as usize + 4]
        .copy_from_slice(&STATE_MAGIC.to_le_bytes());
    state[StateHeaderOffset::FORMAT_VERSION as usize] = STATE_FORMAT_VERSION;
    state[StateHeaderOffset::NUM_SLOTS as usize] = 1;
    let meta_base = STATE_HEADER_SIZE;
    let w32 = |s: &mut [u8], off: u32, v: u32| {
        s[off as usize..off as usize + 4].copy_from_slice(&v.to_le_bytes());
    };
    w32(&mut state, meta_base + SlotMetaOffset::OFFSET, data_offset);
    w32(&mut state, meta_base + SlotMetaOffset::CAPACITY, capacity);
    w32(&mut state, meta_base + SlotMetaOffset::SIZE, 0);
    state[(meta_base + SlotMetaOffset::TYPE_FLAGS) as usize] = SlotType::Bitmap as u8;
    let meta = SlotMetaView::read(&state, 0);
    (state, meta)
}

/// Scattered pseudo-random members: `n` distinct values in `0..8n`, a
/// domain eight times the cardinality so chunks hold mixed containers.
fn scattered(n: usize, seed: u64) -> Vec<u32> {
    let domain = (n as u64 * 8).min(u64::from(u32::MAX));
    let mut x = seed | 1;
    let mut seen = std::collections::BTreeSet::new();
    while seen.len() < n {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        seen.insert((x % domain) as u32);
    }
    seen.into_iter().collect()
}

struct Cell {
    name: &'static str,
    set: usize,
    batch: usize,
    remove: bool,
}

fn main() {
    let cells = [
        Cell {
            name: "add/1",
            set: 10_000,
            batch: 1,
            remove: false,
        },
        Cell {
            name: "add/64",
            set: 10_000,
            batch: 64,
            remove: false,
        },
        Cell {
            name: "add/4096",
            set: 10_000,
            batch: 4096,
            remove: false,
        },
        Cell {
            name: "rem/1",
            set: 10_000,
            batch: 1,
            remove: true,
        },
        Cell {
            name: "rem/4096",
            set: 10_000,
            batch: 4096,
            remove: true,
        },
        Cell {
            name: "add/1",
            set: 300_000,
            batch: 1,
            remove: false,
        },
        Cell {
            name: "add/64",
            set: 300_000,
            batch: 64,
            remove: false,
        },
        Cell {
            name: "add/4096",
            set: 300_000,
            batch: 4096,
            remove: false,
        },
        Cell {
            name: "rem/1",
            set: 300_000,
            batch: 1,
            remove: true,
        },
        Cell {
            name: "rem/4096",
            set: 300_000,
            batch: 4096,
            remove: true,
        },
    ];
    println!("| set | cell | wall (mean) | allocs | bytes | image |");
    println!("| --- | --- | --- | --- | --- | --- |");
    for cell in &cells {
        let members = scattered(cell.set, 0x9E37_79B9_7F4A_7C15);
        // The batch: for adds, values absent from the set; for removes, a
        // scattered subset of members.
        let batch: Vec<u32> = if cell.remove {
            members
                .iter()
                .step_by(members.len() / cell.batch)
                .take(cell.batch)
                .copied()
                .collect()
        } else {
            let present: std::collections::BTreeSet<u32> = members.iter().copied().collect();
            let mut out = Vec::with_capacity(cell.batch);
            let mut x = 0xD1B5_4A32_D192_ED03u64;
            let domain = cell.set as u64 * 8;
            while out.len() < cell.batch {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let v = (x % domain) as u32;
                if !present.contains(&v) && !out.contains(&v) {
                    out.push(v);
                }
            }
            out.sort_unstable();
            out
        };
        let (mut state, meta) = init_slot((cell.set * 2) as u32);
        let mut env = BitmapEnv::default();
        assert_eq!(
            batch_bitmap_add(
                &mut env, &mut NoVm, false, &mut state, &meta, 0, &members, None
            ),
            ErrorCode::Ok
        );
        let image = columine_vm::bitmap_ops::get_bitmap_storage(&meta).serialized_len(&state);
        let iters = if cell.set >= 300_000 { 30 } else { 100 };
        let mut run = |timed: bool| -> (u128, usize, usize) {
            let a0 = ALLOCS.load(Ordering::Relaxed);
            let b0 = BYTES.load(Ordering::Relaxed);
            let t0 = Instant::now();
            let r = if cell.remove == timed {
                batch_bitmap_remove(&mut env, &mut NoVm, false, &mut state, &meta, 0, &batch)
            } else {
                batch_bitmap_add(
                    &mut env, &mut NoVm, false, &mut state, &meta, 0, &batch, None,
                )
            };
            let dt = t0.elapsed().as_nanos();
            assert_eq!(r, ErrorCode::Ok);
            (
                dt,
                ALLOCS.load(Ordering::Relaxed) - a0,
                BYTES.load(Ordering::Relaxed) - b0,
            )
        };
        // Warm: three timed-direction ops each followed by its inverse.
        for _ in 0..3 {
            run(true);
            run(false);
        }
        let (mut total, mut allocs, mut bytes) = (0u128, 0usize, 0usize);
        for _ in 0..iters {
            let (dt, a, b) = run(true);
            total += dt;
            allocs += a;
            bytes += b;
            run(false);
        }
        let mean = total as f64 / iters as f64;
        println!(
            "| {} | {} | {:.1} µs | {} | {} B | {} B |",
            cell.set,
            cell.name,
            mean / 1000.0,
            allocs / iters,
            bytes / iters,
            image
        );
    }
}
