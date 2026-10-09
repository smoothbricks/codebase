//! The batch-mutation cells of the BITMAP slot, measured through the VM's own
//! entry points (`batch_bitmap_add` / `batch_bitmap_remove`) on a slot laid
//! out by `bitmap_payload_capacity`: add/1, add/64, add/4096, rem/1,
//! rem/4096 against 10k- and 300k-member scattered sets, and the same cells
//! against an interval-rich set whose chunks the chooser prices as `Runs`, so
//! bridging and splitting runs is measured rather than assumed. A second
//! table merges two `Array` chunks that start one byte apart, since the
//! native image has no alignment padding and the vector merge must read
//! either offset.
//!
//! Wall time is the mean of timed iterations after a warm-up; the allocation
//! census counts every heap allocation the timed call makes, so a steady path
//! that allocates is visible as a number rather than a suspicion.
//!
//! A third table times TTL bitmap algebra (`OR` of two TTL bitmaps through
//! `Vm::bitmap_algebra`) at N = 1K..16K members: every expiry changes, one
//! expiry changes, and every expiry changes over an index carrying NaN
//! timestamps (the exact replay plan). Per-member cost must stay flat as N
//! doubles.
//!
//! Run: `cargo bench -p columine-vm --bench bitmap_cells`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use bitmosaic::EMPTY_U32_IMAGE;
use columine_types::types::{
    ErrorCode, SLOT_META_SIZE, STATE_FORMAT_VERSION, STATE_HEADER_SIZE, STATE_MAGIC,
    SlotMetaOffset, SlotType, StateHeaderOffset,
};
use columine_vm::bitmap_ops::{
    BitmapAlgebraOp, BitmapEnv, BitmapSource, batch_bitmap_add, batch_bitmap_remove,
    bitmap_payload_capacity, get_bitmap_storage, intersect_count_serialized,
};
use columine_vm::hooks::NoVm;
use columine_vm::meta::SlotMetaView;
use columine_vm::state_init::{DEFAULT_ACCEPTED_PROGRAM_MAGICS, calculate_state_size, init_state};
use columine_vm::vm::{Vm, f64s_as_bytes, u32s_as_bytes};

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
/// metadata record, then the native `[image][spare]` region admitted as the
/// canonical empty image.
fn init_slot(capacity: u32) -> (Vec<u8>, SlotMetaView) {
    let data_offset = STATE_HEADER_SIZE + SLOT_META_SIZE;
    let total = data_offset + bitmap_payload_capacity(capacity);
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
    let start = data_offset as usize;
    state[start..start + EMPTY_U32_IMAGE.len()].copy_from_slice(&EMPTY_U32_IMAGE);
    let meta = SlotMetaView::read(&state, 0);
    (state, meta)
}

fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// Scattered pseudo-random members: `n` distinct values in `0..8n`, a
/// domain eight times the cardinality so chunks hold mixed containers.
fn scattered(n: usize, seed: u64) -> Vec<u32> {
    let domain = (n as u64 * 8).min(u64::from(u32::MAX));
    let mut x = seed | 1;
    let mut seen = std::collections::BTreeSet::new();
    while seen.len() < n {
        seen.insert((xorshift(&mut x) % domain) as u32);
    }
    seen.into_iter().collect()
}

/// Interval-rich members: every value whose position in its 64-value period
/// is not the last, so each full chunk holds 1024 maximal runs — 6 * 1024 =
/// 6144 owned bytes against the Words arm's 8720 — and the chooser prices the
/// chunk as `Runs`. The omitted values are the bridges: adding one merges two
/// runs, and removing an interior value splits one, so this single fixture
/// drives both edit shapes.
fn interval_rich(n: usize) -> Vec<u32> {
    (0u32..).filter(|v| v % 64 != 63).take(n).collect()
}

/// The run edits: `remove` takes interior members (period residue 31, each
/// removal splitting one run into two), and add takes the absent bridges
/// (residue 63, each insert merging two runs into one).
fn interval_batch(members: &[u32], batch: usize, remove: bool) -> Vec<u32> {
    let last = *members.last().expect("interval fixture is nonempty");
    let residue = if remove { 31 } else { 63 };
    let out: Vec<u32> = (0..=last)
        .filter(|v| v % 64 == residue)
        .take(batch)
        .collect();
    assert_eq!(
        out.len(),
        batch,
        "the interval fixture must offer {batch} run edits"
    );
    out
}

/// Which member shape the cell seeds and edits.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Shape {
    Scattered,
    Intervals,
}

struct Cell {
    name: &'static str,
    shape: Shape,
    set: usize,
    batch: usize,
    remove: bool,
}

/// A leading chunk with an odd-length payload (a two-run `Runs` payload is
/// `1 + 4 * 2 = 9` bytes) or an even-length one (a `Stride` payload is 4
/// bytes), followed by a chunk of `array_len` irregular members, which the
/// chooser prices as `Array` (2 bytes per member against the Words arm's
/// 8720). The two variants therefore place that `Array` payload one byte
/// apart: the native image has no padding, so exactly one of the pair starts
/// at an odd byte offset and both must merge through the same vector path.
fn shifted_array_members(odd_shift: bool, array_len: usize, seed: u64) -> Vec<u32> {
    let mut members: Vec<u32> = if odd_shift {
        (0u32..100).chain(200u32..300).collect()
    } else {
        (0u32..200).step_by(2).collect()
    };
    let mut x = seed | 1;
    let mut low = std::collections::BTreeSet::new();
    while low.len() < array_len {
        low.insert((xorshift(&mut x) & 0xffff) as u32);
    }
    members.extend(low.into_iter().map(|v| (1u32 << 16) | v));
    members
}

/// Seed a slot with `members` and return the state plus its metadata.
fn seeded_slot(members: &[u32]) -> (Vec<u8>, SlotMetaView) {
    let (mut state, meta) = init_slot((members.len() * 2) as u32);
    let mut env = BitmapEnv::default();
    let mut hooks = NoVm::default();
    assert_eq!(
        batch_bitmap_add(
            &mut env, &mut hooks, false, &mut state, &meta, members, None
        ),
        ErrorCode::Ok
    );
    (state, meta)
}

fn main() {
    let cells = [
        Cell {
            name: "add/1",
            shape: Shape::Scattered,
            set: 10_000,
            batch: 1,
            remove: false,
        },
        Cell {
            name: "add/64",
            shape: Shape::Scattered,
            set: 10_000,
            batch: 64,
            remove: false,
        },
        Cell {
            name: "add/4096",
            shape: Shape::Scattered,
            set: 10_000,
            batch: 4096,
            remove: false,
        },
        Cell {
            name: "rem/1",
            shape: Shape::Scattered,
            set: 10_000,
            batch: 1,
            remove: true,
        },
        Cell {
            name: "rem/4096",
            shape: Shape::Scattered,
            set: 10_000,
            batch: 4096,
            remove: true,
        },
        Cell {
            name: "add/1",
            shape: Shape::Scattered,
            set: 300_000,
            batch: 1,
            remove: false,
        },
        Cell {
            name: "add/64",
            shape: Shape::Scattered,
            set: 300_000,
            batch: 64,
            remove: false,
        },
        Cell {
            name: "add/4096",
            shape: Shape::Scattered,
            set: 300_000,
            batch: 4096,
            remove: false,
        },
        Cell {
            name: "rem/1",
            shape: Shape::Scattered,
            set: 300_000,
            batch: 1,
            remove: true,
        },
        Cell {
            name: "rem/4096",
            shape: Shape::Scattered,
            set: 300_000,
            batch: 4096,
            remove: true,
        },
        // Interval-rich cells: the timed add bridges runs, the timed remove
        // splits them. The 10k fixture spans ~158 periods, which bounds its
        // batch; the 300k fixture spans ~4,760.
        Cell {
            name: "runs bridge/1",
            shape: Shape::Intervals,
            set: 10_000,
            batch: 1,
            remove: false,
        },
        Cell {
            name: "runs bridge/64",
            shape: Shape::Intervals,
            set: 10_000,
            batch: 64,
            remove: false,
        },
        Cell {
            name: "runs split/1",
            shape: Shape::Intervals,
            set: 10_000,
            batch: 1,
            remove: true,
        },
        Cell {
            name: "runs split/64",
            shape: Shape::Intervals,
            set: 10_000,
            batch: 64,
            remove: true,
        },
        Cell {
            name: "runs bridge/4096",
            shape: Shape::Intervals,
            set: 300_000,
            batch: 4096,
            remove: false,
        },
        Cell {
            name: "runs split/4096",
            shape: Shape::Intervals,
            set: 300_000,
            batch: 4096,
            remove: true,
        },
    ];
    println!("| set | cell | wall (mean) | allocs | bytes | image |");
    println!("| --- | --- | --- | --- | --- | --- |");
    for cell in &cells {
        let members = match cell.shape {
            Shape::Scattered => scattered(cell.set, 0x9E37_79B9_7F4A_7C15),
            Shape::Intervals => interval_rich(cell.set),
        };
        // The batch: for adds, values absent from the set; for removes, a
        // subset of members. The interval shape names both by period residue,
        // so a bridge is always a merge and a split is always a split.
        let batch: Vec<u32> = match cell.shape {
            Shape::Intervals => interval_batch(&members, cell.batch, cell.remove),
            Shape::Scattered if cell.remove => members
                .iter()
                .step_by(members.len() / cell.batch)
                .take(cell.batch)
                .copied()
                .collect(),
            Shape::Scattered => {
                let present: std::collections::BTreeSet<u32> = members.iter().copied().collect();
                let mut out = Vec::with_capacity(cell.batch);
                let mut x = 0xD1B5_4A32_D192_ED03u64;
                let domain = cell.set as u64 * 8;
                while out.len() < cell.batch {
                    let v = (xorshift(&mut x) % domain) as u32;
                    if !present.contains(&v) && !out.contains(&v) {
                        out.push(v);
                    }
                }
                out.sort_unstable();
                out
            }
        };
        let (mut state, meta) = init_slot((cell.set * 2) as u32);
        let mut env = BitmapEnv::default();
        let mut hooks = NoVm::default();
        assert_eq!(
            batch_bitmap_add(
                &mut env, &mut hooks, false, &mut state, &meta, &members, None
            ),
            ErrorCode::Ok
        );
        let image = get_bitmap_storage(&meta)
            .serialized_len(&state)
            .expect("seeded image parses");
        let iters = if cell.set >= 300_000 { 30 } else { 100 };
        let mut run = |timed: bool| -> (u128, usize, usize) {
            let a0 = ALLOCS.load(Ordering::Relaxed);
            let b0 = BYTES.load(Ordering::Relaxed);
            let t0 = Instant::now();
            let r = if cell.remove == timed {
                batch_bitmap_remove(&mut env, &mut hooks, false, &mut state, &meta, &batch)
            } else {
                batch_bitmap_add(&mut env, &mut hooks, false, &mut state, &meta, &batch, None)
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

    // Array x Array over the live slot images, with the array payload shifted
    // one byte between the two rows: the leading chunk is a 9-byte Runs
    // payload in one and a 4-byte Stride payload in the other, and the image
    // pads nothing, so the merge reads an odd start in exactly one row.
    println!();
    println!("| array pair | wall (mean) | count | left image | right image |");
    println!("| --- | --- | --- | --- | --- | ");
    for (name, odd_shift) in [("odd-shifted", true), ("even-shifted", false)] {
        let left_members = shifted_array_members(odd_shift, 2000, 0x243F_6A88_85A3_08D3);
        let right_members = shifted_array_members(odd_shift, 2000, 0x1319_8A2E_0370_7344);
        let (left_state, left_meta) = seeded_slot(&left_members);
        let (right_state, right_meta) = seeded_slot(&right_members);
        let left = get_bitmap_storage(&left_meta)
            .serialized_data(&left_state)
            .expect("left image parses");
        let right = get_bitmap_storage(&right_meta)
            .serialized_data(&right_state)
            .expect("right image parses");

        let count = intersect_count_serialized(left, right);
        for _ in 0..100 {
            std::hint::black_box(intersect_count_serialized(left, right));
        }
        let iters = 1_000u32;
        let t0 = Instant::now();
        for _ in 0..iters {
            std::hint::black_box(intersect_count_serialized(left, right));
        }
        let mean = t0.elapsed().as_nanos() as f64 / f64::from(iters);
        println!(
            "| {} | {:.2} µs | {} | {} B | {} B |",
            name,
            mean / 1000.0,
            count,
            left.len(),
            right.len()
        );
    }

    ttl_algebra_cells();
}

/// A program of two TTL bitmaps of `capacity`; its reduce section is `reduce`.
fn ttl_bitmaps_program(capacity: u16, reduce: &[u8]) -> Vec<u8> {
    use columine_types::PROGRAM_MAGIC;
    use columine_types::types::Opcode;
    let mut init = Vec::new();
    for slot in 0..2u8 {
        let [lo, hi] = capacity.to_le_bytes();
        init.extend([
            Opcode::SlotDef as u8,
            slot,
            SlotType::Bitmap as u8 | 0x10,
            lo,
            hi,
        ]);
        init.extend(3600.0f32.to_le_bytes());
        init.extend(0.0f32.to_le_bytes());
        init.extend([1, 0]);
    }
    init.push(Opcode::Halt as u8);
    let mut program = vec![0u8; 32];
    program.extend(PROGRAM_MAGIC.to_le_bytes());
    program.extend([2, 0, 0, 0]);
    program.extend(2u32.to_le_bytes());
    program.extend(2u32.to_le_bytes());
    program.extend((init.len() as u32).to_le_bytes());
    program.extend((reduce.len() as u32).to_le_bytes());
    program.extend(init);
    program.extend_from_slice(reduce);
    program
}

/// Two TTL bitmaps seeded through the real `BATCH_SET_INSERT_TTL` opcode.
fn ttl_pair(n: u32, target: &[f64], source: &[f64]) -> Vec<u8> {
    use columine_types::types::Opcode;
    let capacity = u16::try_from(n).expect("bench N fits a slot capacity");
    let layout = ttl_bitmaps_program(capacity, &[]);
    let size = calculate_state_size(&layout, DEFAULT_ACCEPTED_PROGRAM_MAGICS)
        .expect("valid program admission");
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, &layout, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("init");
    let keys: Vec<u32> = (0..n).collect();
    let mut vm = Vm::default();
    for (slot, stamps) in [(0u8, target), (1u8, source)] {
        let insert = ttl_bitmaps_program(capacity, &[Opcode::BatchSetInsertTtl as u8, slot, 0, 1]);
        let cols = [u32s_as_bytes(&keys), f64s_as_bytes(stamps)];
        assert_eq!(
            vm.execute_batch(&mut state, &insert, &cols, n),
            ErrorCode::Ok as u32
        );
    }
    state
}

fn ttl_algebra_cells() {
    println!();
    println!("| ttl algebra (OR) | N | wall (mean) | per member | allocs |");
    println!("| --- | --- | --- | --- | --- |");
    for (name, change_all, nan) in [
        ("all expiries change", true, false),
        ("one expiry changes", false, false),
        ("all change, NaN in index", true, true),
    ] {
        for n in [1024u32, 2048, 4096, 8192, 16384] {
            let target: Vec<f64> = (0..n)
                .map(|i| {
                    if nan && i % 64 == 0 {
                        f64::NAN
                    } else {
                        f64::from(i)
                    }
                })
                .collect();
            let source: Vec<f64> = (0..n)
                .map(|i| {
                    if change_all || i == n / 2 {
                        f64::from(i + n)
                    } else {
                        f64::from(i)
                    }
                })
                .collect();
            let seeded = ttl_pair(n, &target, &source);
            let mut vm = Vm::default();
            let mut work = seeded.clone();
            let mut run = |work: &mut Vec<u8>| -> (u128, usize) {
                work.copy_from_slice(&seeded);
                let a0 = ALLOCS.load(Ordering::Relaxed);
                let t0 = Instant::now();
                let r =
                    vm.bitmap_algebra(false, BitmapAlgebraOp::Or, work, 0, BitmapSource::Slot(1));
                let dt = t0.elapsed().as_nanos();
                assert_eq!(r, ErrorCode::Ok);
                (dt, ALLOCS.load(Ordering::Relaxed) - a0)
            };
            for _ in 0..3 {
                run(&mut work);
            }
            let iters = 20u32;
            let (mut total, mut allocs) = (0u128, 0usize);
            for _ in 0..iters {
                let (dt, a) = run(&mut work);
                total += dt;
                allocs += a;
            }
            let mean = total as f64 / f64::from(iters);
            println!(
                "| {} | {} | {:.1} µs | {:.1} ns | {} |",
                name,
                n,
                mean / 1000.0,
                mean / f64::from(n),
                allocs / iters as usize
            );
        }
    }
}
