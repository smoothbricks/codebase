//! Per-batch cost of `BATCH_MAP_UPSERT_LAST` into four hash maps of declared
//! capacity 32768 holding 20000 live keys, by batch size, journal off and on.
//!
//! Every row names a live key, so the batch is all updates: the cells time
//! the batch entry checks, capacity admission and the row loop, never growth.
//! The same image is restored before every timed batch. A cell whose batch is
//! refused prints its status instead: with the journal on, 10000 rows change
//! 40000 value cells, past the journal's 16384-record budget, so that batch
//! is refused with `JournalOverflow` before it writes.
//!
//! Run: `cargo bench -p columine-vm --bench map_upsert_scale`.

use std::time::Instant;

use columine_types::PROGRAM_MAGIC;
use columine_types::types::{ErrorCode, Opcode, SlotType};
use columine_vm::state_init::{DEFAULT_ACCEPTED_PROGRAM_MAGICS, calculate_state_size, init_state};
use columine_vm::vm::{Vm, u32s_as_bytes};

const MAPS: u8 = 4;
const LIVE_KEYS: u32 = 20_000;
const WARMUP: u32 = 20;

fn program() -> Vec<u8> {
    let mut init = Vec::new();
    for slot in 0..MAPS {
        // Declared capacity 0x8000, little-endian.
        init.extend([
            Opcode::SlotDef as u8,
            slot,
            SlotType::HashMap as u8,
            0x00,
            0x80,
        ]);
    }
    init.push(Opcode::Halt as u8);
    let mut reduce = Vec::new();
    for slot in 0..MAPS {
        reduce.extend([Opcode::BatchMapUpsertLast as u8, slot, 0, 1]);
    }
    reduce.push(Opcode::Halt as u8);
    let mut prog = vec![0u8; 32];
    prog.extend(PROGRAM_MAGIC.to_le_bytes());
    prog.extend([2, 0, 0, 0]);
    prog.extend(u32::from(MAPS).to_le_bytes());
    prog.extend(2u32.to_le_bytes());
    prog.extend(u32::try_from(init.len()).unwrap().to_le_bytes());
    prog.extend(u32::try_from(reduce.len()).unwrap().to_le_bytes());
    prog.extend(init);
    prog.extend(reduce);
    prog
}

/// An image whose maps each hold keys `0..LIVE_KEYS`.
fn loaded_image(prog: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("size");
    let mut image = vec![0u8; usize::try_from(size).unwrap()];
    init_state(&mut image, prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("init");
    let mut vm = Vm::default();
    for chunk in (0..LIVE_KEYS).collect::<Vec<_>>().chunks(4096) {
        let vals: Vec<u32> = chunk.iter().map(|k| k * 3).collect();
        let rows = u32::try_from(chunk.len()).unwrap();
        let cols = [u32s_as_bytes(chunk), u32s_as_bytes(&vals)];
        assert_eq!(
            ErrorCode::Ok as u32,
            vm.execute_batch(&mut image, prog, &cols, rows)
        );
    }
    image
}

fn main() {
    let prog = program();
    let image = loaded_image(&prog);
    let mut vm = Vm::default();
    let mut rng = 0x9E37_79B9_7F4A_7C15u64;
    println!("| journal | rows | per batch |");
    println!("|---|---:|---:|");
    for journal in [false, true] {
        for rows in [1usize, 100, 1000, 10_000] {
            let keys: Vec<u32> = (0..rows)
                .map(|_| {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    u32::try_from(rng % u64::from(LIVE_KEYS)).unwrap()
                })
                .collect();
            let vals: Vec<u32> = keys.iter().map(|k| k + 1).collect();
            let cols = [u32s_as_bytes(&keys), u32s_as_bytes(&vals)];
            let batch_len = u32::try_from(rows).unwrap();
            let iters: u32 = match rows {
                1 => 20_000,
                10_000 => 200,
                _ => 2000,
            };
            let mut state = image.clone();
            let mut total = 0u128;
            let mut refused = None;
            for i in 0..iters + WARMUP {
                state.copy_from_slice(&image);
                let start = Instant::now();
                if journal {
                    vm.undo_enable(&state).expect("window opens");
                }
                let status = vm.execute_batch(&mut state, &prog, &cols, batch_len);
                if journal {
                    vm.undo_commit();
                }
                let elapsed = start.elapsed().as_nanos();
                if status != ErrorCode::Ok as u32 {
                    refused = Some(status);
                    break;
                }
                if i >= WARMUP {
                    total += elapsed;
                }
            }
            match refused {
                Some(status) => println!("| {journal} | {rows} | status {status} |"),
                None => println!(
                    "| {journal} | {rows} | {:.3} us |",
                    total as f64 / f64::from(iters) / 1000.0
                ),
            }
        }
    }
}
