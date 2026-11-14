//! The i64 sum through the VM's execute entry: `BATCH_AGG_SUM_I64` over one
//! batch, at the top level (every row) and under `FOR_EACH` (a type mask that
//! takes half the rows); and the kernel alone, beside a reference that folds
//! the same rows with wrapping adds and no overflow check (the work removed).
//! Phase: warmed fold. Wall time is the median of timed batches after a
//! warm-up, per row of the batch.
//!
//! Run: `cargo bench -p columine-vm --bench aggregates`.

use std::hint::black_box;
use std::time::Instant;

use columine_types::PROGRAM_MAGIC;
use columine_types::types::{AggType, ErrorCode};
use columine_vm::aggregates::{AggKind, reduce_col_i64};
use columine_vm::state_init::{DEFAULT_ACCEPTED_PROGRAM_MAGICS, calculate_state_size, init_state};
use columine_vm::vm::{Vm, i64s_as_bytes, u32s_as_bytes};

/// Columns: 0 type, 1 value.
const TYPE_COL: u8 = 0;
const VAL_COL: u8 = 1;
const ORDER_TYPE: u32 = 7;
const ITERS: usize = 400;

fn program(reduce: &[u8]) -> Vec<u8> {
    let init = [0x10, 0, 2, AggType::SumI64 as u8, 0, 0];
    let mut prog = vec![0u8; 32];
    prog.extend(PROGRAM_MAGIC.to_le_bytes());
    prog.extend([2, 0, 0, 0]); // version 2, no callbacks, no flags
    prog.extend(1u32.to_le_bytes());
    prog.extend(2u32.to_le_bytes());
    prog.extend((init.len() as u32).to_le_bytes());
    prog.extend((reduce.len() as u32).to_le_bytes());
    prog.extend(init);
    prog.extend(reduce);
    prog
}

fn top_level() -> Vec<u8> {
    program(&[0x49, 0, VAL_COL, 0])
}

fn for_each() -> Vec<u8> {
    let body = [0x49u8, 0, VAL_COL];
    let mut reduce = vec![0xE0u8, TYPE_COL, 1];
    reduce.extend(ORDER_TYPE.to_le_bytes());
    reduce.extend((body.len() as u16).to_le_bytes());
    reduce.extend(body);
    reduce.push(0);
    program(&reduce)
}

fn init(prog: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS)
        .expect("valid program admission");
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("init_state");
    state
}

fn main() {
    println!("| cell | rows | median per batch | per row |");
    println!("| --- | --- | --- | --- |");
    for rows in [64usize, 4096, 65536] {
        let types: Vec<u32> = (0..rows)
            .map(|i| {
                if i % 2 == 0 {
                    ORDER_TYPE
                } else {
                    ORDER_TYPE + 1
                }
            })
            .collect();
        let vals: Vec<i64> = (0..rows).map(|i| (i as i64 % 1000) - 500).collect();
        let cols: [&[u8]; 2] = [u32s_as_bytes(&types), i64s_as_bytes(&vals)];
        for (name, prog) in [("vm/top", top_level()), ("vm/for_each/half", for_each())] {
            let mut vm = Vm::new(DEFAULT_ACCEPTED_PROGRAM_MAGICS);
            let mut state = init(&prog);
            report(name, rows, || {
                let status = vm.execute_batch(&mut state, &prog, &cols, rows as u32);
                assert_eq!(status, ErrorCode::Ok as u32);
            });
        }
        report("kernel/checked", rows, || {
            black_box(reduce_col_i64(AggKind::Sum, black_box(&vals), 0, None));
        });
        report("kernel/wrapping reference", rows, || {
            black_box(wrapping_sum(black_box(&vals)));
        });
    }
}

/// The sum with its overflow check removed: wrapping adds over two lanes.
fn wrapping_sum(vals: &[i64]) -> i64 {
    let mut lanes = [0i64; 2];
    let (pairs, tail) = vals.as_chunks::<2>();
    for pair in pairs {
        lanes[0] = lanes[0].wrapping_add(pair[0]);
        lanes[1] = lanes[1].wrapping_add(pair[1]);
    }
    tail.iter()
        .fold(lanes[0].wrapping_add(lanes[1]), |acc, &v| {
            acc.wrapping_add(v)
        })
}

fn report(name: &str, rows: usize, mut batch: impl FnMut()) {
    let mut run = || {
        let t0 = Instant::now();
        batch();
        t0.elapsed().as_nanos()
    };
    for _ in 0..ITERS / 10 {
        run();
    }
    let mut times: Vec<u128> = (0..ITERS).map(|_| run()).collect();
    times.sort_unstable();
    let median = times[ITERS / 2];
    println!(
        "| {name} | {rows} | {median} ns | {:.3} ns |",
        median as f64 / rows as f64
    );
}
