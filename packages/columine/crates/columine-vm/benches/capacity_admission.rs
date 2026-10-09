//! Batch capacity admission, measured through `Vm::execute_batch`.
//!
//! The first table times one batch per cell, restoring the same image before
//! every timed call, so each cell is decided by exactly the tier it names:
//!
//! - `room`: the row count clears the batch (tier 1).
//! - `full, updates`: only reading the rows clears it (tier 2).
//! - `full, new keys repeated`: only a dry run can tell it fits (tier 3).
//! - `full, one key too many`: refused before any write.
//! - `nested, updates`: a nested slot, whose arena demand tier 2 computes.
//!
//! The second table streams batches into a map that starts small, growing on
//! every `NEEDS_GROWTH`, and counts which tier decided each batch.
//!
//! Run: `cargo bench -p columine-vm --bench capacity_admission`.

use std::time::Instant;

use columine_types::PROGRAM_MAGIC;
use columine_types::types::{ErrorCode, Opcode, SlotType};
use columine_vm::state_init::{
    DEFAULT_ACCEPTED_PROGRAM_MAGICS, calculate_grown_state_size, calculate_state_size, init_state,
    needs_growth_slot,
};
use columine_vm::vm::{Vm, u32s_as_bytes};

const OK: u32 = ErrorCode::Ok as u32;
const NEEDS_GROWTH: u32 = ErrorCode::NeedsGrowth as u32;

fn assemble(num_inputs: u32, init: &[u8], reduce: &[u8]) -> Vec<u8> {
    let mut prog = vec![0u8; 32];
    prog.extend(PROGRAM_MAGIC.to_le_bytes());
    prog.extend([2, 0, 0, 0]);
    prog.extend(1u32.to_le_bytes());
    prog.extend(num_inputs.to_le_bytes());
    prog.extend((init.len() as u32).to_le_bytes());
    prog.extend((reduce.len() as u32).to_le_bytes());
    prog.extend_from_slice(init);
    prog.extend_from_slice(reduce);
    prog
}

fn map_program(capacity: u16) -> Vec<u8> {
    let [lo, hi] = capacity.to_le_bytes();
    assemble(
        3,
        &[
            Opcode::SlotDef as u8,
            0,
            SlotType::HashMap as u8,
            lo,
            hi,
            Opcode::Halt as u8,
        ],
        &[Opcode::BatchMapUpsertLast as u8, 0, 0, 1],
    )
}

/// A nested slot (outer map of inner sets) filled by a `FOR_EACH` body.
fn nested_program() -> Vec<u8> {
    let init = [
        Opcode::SlotNested as u8,
        0,
        SlotType::Nested as u8,
        0,
        0x40,
        SlotType::HashSet as u8,
        16,
        0,
        0,
        Opcode::Halt as u8,
    ];
    let body = [Opcode::NestedSetInsert as u8, 0, 0, 1];
    let mut reduce = vec![Opcode::ForEach as u8, 2, 1];
    reduce.extend(1u32.to_le_bytes());
    reduce.extend((body.len() as u16).to_le_bytes());
    reduce.extend(body);
    assemble(3, &init, &reduce)
}

fn init(prog: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("valid program");
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("init");
    state
}

fn run(vm: &mut Vm, state: &mut [u8], prog: &[u8], keys: &[u32]) -> u32 {
    let vals: Vec<u32> = keys.iter().map(|k| k ^ 0x5a5a).collect();
    let types = vec![1u32; keys.len()];
    vm.execute_batch(
        state,
        prog,
        &[
            u32s_as_bytes(keys),
            u32s_as_bytes(&vals),
            u32s_as_bytes(&types),
        ],
        keys.len() as u32,
    )
}

/// An image holding keys `0..live`.
fn filled(prog: &[u8], live: u32) -> Vec<u8> {
    let mut state = init(prog);
    let mut vm = Vm::default();
    let keys: Vec<u32> = (0..live).collect();
    for chunk in keys.chunks(4096) {
        assert_eq!(OK, run(&mut vm, &mut state, prog, chunk));
    }
    state
}

fn time_cell(name: &str, prog: &[u8], image: &[u8], keys: &[u32], expect: u32) {
    let mut vm = Vm::default();
    let mut state = image.to_vec();
    for _ in 0..5 {
        state.copy_from_slice(image);
        assert_eq!(expect, run(&mut vm, &mut state, prog, keys));
    }
    let before = vm.capacity_counts();
    let iters = 200u32;
    let mut total = 0u128;
    for _ in 0..iters {
        state.copy_from_slice(image);
        let t0 = Instant::now();
        let r = run(&mut vm, &mut state, prog, keys);
        total += t0.elapsed().as_nanos();
        assert_eq!(expect, r);
    }
    let after = vm.capacity_counts();
    let mean = total as f64 / f64::from(iters);
    println!(
        "| {name} | {} | {:.1} µs | {:.1} ns | {}/{}/{}/{} |",
        keys.len(),
        mean / 1000.0,
        mean / keys.len() as f64,
        after.bound - before.bound,
        after.probed - before.probed,
        after.dry_run_fit - before.dry_run_fit,
        after.refused - before.refused,
    );
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// Stream `batches` batches of `rows` rows into a map declared at 1024, with
/// `new_percent` of rows naming a fresh key and the rest an existing one,
/// growing on every refusal.
fn stream(batches: u32, rows: usize, new_percent: u64) {
    let prog = map_program(1024);
    let mut state = init(&prog);
    let mut vm = Vm::default();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut next_key = 0u32;
    let mut growths = 0u32;
    let mut keys = Vec::with_capacity(rows);
    let t0 = Instant::now();
    for _ in 0..batches {
        keys.clear();
        for _ in 0..rows {
            if next_key == 0 || rng.next() % 100 < new_percent {
                keys.push(next_key);
                next_key += 1;
            } else {
                keys.push((rng.next() % u64::from(next_key)) as u32);
            }
        }
        loop {
            match run(&mut vm, &mut state, &prog, &keys) {
                OK => break,
                NEEDS_GROWTH => {
                    let slot = needs_growth_slot();
                    let size = calculate_grown_state_size(&state, slot).expect("grown size");
                    let mut next = vec![0u8; size as usize];
                    vm.grow_state(&state, &mut next, slot).expect("grow");
                    state = next;
                    growths += 1;
                }
                other => panic!("stream refused with {other}"),
            }
        }
    }
    let elapsed = t0.elapsed().as_secs_f64() * 1000.0;
    let c = vm.capacity_counts();
    println!(
        "| map from 1024 | {batches} × {rows} | {new_percent}% | {elapsed:.1} ms | {} | {} | {} | {} | {growths} |",
        c.bound, c.probed, c.dry_run_fit, c.refused
    );
}

fn main() {
    println!("| cell | rows | wall (mean) | per row | bound/probed/dry-run/refused |");
    println!("| --- | --- | --- | --- | --- |");
    // Declared 32768: physical 65536 cells, 45875 keys admitted.
    let prog = map_program(32768);
    for rows in [64usize, 1024, 4096] {
        let room = filled(&prog, 20_000);
        let keys: Vec<u32> = (0..rows as u32).map(|i| i * 3 % 20_000).collect();
        time_cell("room", &prog, &room, &keys, OK);
    }
    let full = filled(&prog, 45_870);
    for rows in [64usize, 1024, 4096] {
        let updates: Vec<u32> = (0..rows as u32).map(|i| i * 7 % 45_870).collect();
        time_cell("full, updates", &prog, &full, &updates, OK);
        let repeated: Vec<u32> = (0..rows as u32).map(|i| 100_000 + i % 5).collect();
        time_cell("full, new keys repeated", &prog, &full, &repeated, OK);
        let mut over: Vec<u32> = (0..rows as u32).map(|i| i * 7 % 45_870).collect();
        over.extend(200_000..200_006);
        time_cell("full, one key too many", &prog, &full, &over, NEEDS_GROWTH);
    }
    let nested = nested_program();
    let nested_image = filled(&nested, 512);
    for rows in [64usize, 1024] {
        let keys: Vec<u32> = (0..rows as u32).map(|i| i % 512).collect();
        time_cell("nested, updates", &nested, &nested_image, &keys, OK);
    }

    println!();
    println!(
        "| stream | batches | new keys | wall | bound | probed | dry-run | refused | growths |"
    );
    println!("| --- | --- | --- | --- | --- | --- | --- | --- | --- |");
    for (rows, new_percent) in [(512usize, 1u64), (512, 10), (512, 50), (4096, 10)] {
        stream(2000, rows, new_percent);
    }
}
