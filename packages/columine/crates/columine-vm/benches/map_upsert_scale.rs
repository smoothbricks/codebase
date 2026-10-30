//! Per-batch cost of upserting into four maps of declared capacity 32768
//! holding 20000 live keys each, by batch size, journal off and on, change
//! feed unarmed and armed. Two targets: hash maps (`BATCH_MAP_UPSERT_LAST`)
//! and two-key struct maps (`BATCH_STRUCT_MAP2_UPSERT_LAST`, two `u32`
//! fields), whose feed entry carries the prior `field0 | field1 << 32` lane.
//!
//! Every row names a live key with a new value, so the batch is all updates:
//! the cells time the batch entry checks, capacity admission and the row
//! loop, never growth. The same image is restored before every timed batch.
//! A cell whose batch is refused prints its status instead. With the journal
//! on, 10000 rows change 40000 value cells, past the journal's 16384-record
//! budget: that batch is not refused. Its window overflows into a shadow,
//! one copy of the image taken inside the timed batch, and every later write
//! runs unrecorded.
//!
//! The feed tracks all four maps and is bound once. Armed, each timed batch
//! begins and ends its pass, so a cell pays the begin's scan of the tracked
//! flags and one entry per changed row; `entries` counts them. Unarmed, it
//! is the VM every other caller runs. Every feed call is in [`feed`], so a
//! build that predates the feed runs this file with that module stubbed.
//!
//! Run: `cargo bench -p columine-vm --bench map_upsert_scale`.

use std::time::Instant;

use columine_types::PROGRAM_MAGIC;
use columine_types::types::{ErrorCode, Opcode, SlotMetaOffset, SlotType, StructFieldType};
use columine_vm::meta::slot_meta_base;
use columine_vm::state_init::{DEFAULT_ACCEPTED_PROGRAM_MAGICS, calculate_state_size, init_state};
use columine_vm::vm::{Vm, u32s_as_bytes};

const MAPS: u8 = 4;
const LIVE_KEYS: u32 = 20_000;
const WARMUP: u32 = 20;
/// Columns: key, value, second key, second value.
const COLUMNS: u32 = 4;

#[derive(Clone, Copy, Debug)]
enum Target {
    HashMap,
    StructMap2,
}

fn program(target: Target) -> Vec<u8> {
    let mut init = Vec::new();
    let mut reduce = Vec::new();
    for slot in 0..MAPS {
        // Declared capacity 0x8000, little-endian.
        match target {
            Target::HashMap => {
                init.extend([
                    Opcode::SlotDef as u8,
                    slot,
                    SlotType::HashMap as u8,
                    0x00,
                    0x80,
                ]);
                reduce.extend([Opcode::BatchMapUpsertLast as u8, slot, 0, 1]);
            }
            Target::StructMap2 => {
                init.extend([
                    Opcode::SlotStructMap2 as u8,
                    slot,
                    SlotType::StructMap2 as u8,
                    0x00,
                    0x80,
                    2,
                    StructFieldType::UInt32 as u8,
                    StructFieldType::UInt32 as u8,
                ]);
                reduce.extend([
                    Opcode::BatchStructMap2UpsertLast as u8,
                    slot,
                    0,
                    2,
                    2,
                    1,
                    0,
                    3,
                    1,
                ]);
            }
        }
    }
    init.push(Opcode::Halt as u8);
    reduce.push(Opcode::Halt as u8);
    let mut prog = vec![0u8; 32];
    prog.extend(PROGRAM_MAGIC.to_le_bytes());
    prog.extend([2, 0, 0, 0]);
    prog.extend(u32::from(MAPS).to_le_bytes());
    prog.extend(COLUMNS.to_le_bytes());
    prog.extend(u32::try_from(init.len()).unwrap().to_le_bytes());
    prog.extend(u32::try_from(reduce.len()).unwrap().to_le_bytes());
    prog.extend(init);
    prog.extend(reduce);
    prog
}

/// The four columns of a batch naming `keys` with value `value(key)`: a
/// two-key map's second key is the key's low byte, its second value 1.
fn columns(keys: &[u32], value: impl Fn(u32) -> u32) -> [Vec<u32>; 4] {
    [
        keys.to_vec(),
        keys.iter().map(|&key| value(key)).collect(),
        keys.iter().map(|&key| key & 0xff).collect(),
        vec![1; keys.len()],
    ]
}

/// An image whose maps each hold keys `0..LIVE_KEYS`.
fn loaded_image(prog: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("size");
    let mut image = vec![0u8; usize::try_from(size).unwrap()];
    init_state(&mut image, prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("init");
    let mut vm = Vm::default();
    for chunk in (0..LIVE_KEYS).collect::<Vec<_>>().chunks(4096) {
        let lanes = columns(chunk, |key| key * 3);
        let rows = u32::try_from(chunk.len()).unwrap();
        let cols = lanes.each_ref().map(|lane| u32s_as_bytes(lane));
        assert_eq!(
            ErrorCode::Ok as u32,
            vm.execute_batch(&mut image, prog, &cols, rows)
        );
    }
    image
}

/// Clear every map's change flag, as a RETE pass leaves them.
fn clear_flags(state: &mut [u8]) {
    for slot in 0..u32::from(MAPS) {
        state[(slot_meta_base(slot) + SlotMetaOffset::CHANGE_FLAGS) as usize] = 0;
    }
}

fn main() {
    println!("| target | journal | feed | rows | entries | per batch | per write |");
    println!("|---|---|---|---:|---:|---:|---:|");
    for target in [Target::HashMap, Target::StructMap2] {
        let prog = program(target);
        let image = loaded_image(&prog);
        let mut vm = Vm::default();
        feed::bind(&mut vm);
        let mut rng = 0x9E37_79B9_7F4A_7C15u64;
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
                let lanes = columns(&keys, |key| key + 1);
                let cols = lanes.each_ref().map(|lane| u32s_as_bytes(lane));
                let batch_len = u32::try_from(rows).unwrap();
                let iters: u32 = match rows {
                    1 => 20_000,
                    10_000 => 200,
                    _ => 2000,
                };
                for &armed in feed::MODES {
                    let mut state = image.clone();
                    let mut total = 0u128;
                    let mut refused = None;
                    for i in 0..iters + WARMUP {
                        state.copy_from_slice(&image);
                        if armed {
                            clear_flags(&mut state);
                        }
                        let start = Instant::now();
                        if journal {
                            vm.undo_enable(&state).expect("window opens");
                        }
                        if armed {
                            feed::begin(&mut vm, &state);
                        }
                        let status = vm.execute_batch(&mut state, &prog, &cols, batch_len);
                        if armed {
                            feed::end(&mut vm);
                        }
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
                    let entries = if armed {
                        feed::entries(&vm).to_string()
                    } else {
                        "-".to_owned()
                    };
                    let cell = format!("| {target:?} | {journal} | {armed} | {rows} | {entries}");
                    match refused {
                        Some(status) => println!("{cell} | status {status} | - |"),
                        None => {
                            let per_batch = total as f64 / f64::from(iters);
                            let writes = (rows * usize::from(MAPS)) as f64;
                            println!(
                                "{cell} | {:.3} us | {:.2} ns |",
                                per_batch / 1000.0,
                                per_batch / writes
                            );
                        }
                    }
                }
            }
        }
    }
}

/// The change-feed half of the bench.
mod feed {
    use columine_vm::vm::Vm;

    /// Unarmed, then armed.
    pub const MODES: &[bool] = &[false, true];

    /// Track every map, with room for the largest batch's entries.
    pub fn bind(vm: &mut Vm) {
        // Slots 0..MAPS are change-plane bits 1..=MAPS.
        let tracked = [(1u64 << (super::MAPS + 1)) - 2];
        vm.feed_bind(&tracked, 0, 4 * 10_000).expect("feed binds");
    }

    pub fn begin(vm: &mut Vm, state: &[u8]) {
        vm.feed_begin_pass(state);
    }

    pub fn end(vm: &mut Vm) {
        vm.feed_end_pass();
    }

    pub fn entries(vm: &Vm) -> usize {
        vm.feed().entries().len()
    }
}
