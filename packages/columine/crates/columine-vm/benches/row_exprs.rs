//! The live row-expression cell: one batch bound through `bind_row_columns`
//! and applied through `execute_batch_live`, where every row's conditional
//! upsert resolves its predicate through `LiveColumns::cell`. Cells: 1, 64
//! and 4096 rows, one live entry and one live entry behind three bound
//! entries (the read must find its entry among several). Wall time is the
//! mean of timed iterations after a warm-up; the allocation census counts
//! every heap allocation the timed call makes, so a per-row read that
//! allocates is visible as a number rather than a suspicion.
//!
//! Run: `cargo bench -p columine-vm --bench row_exprs`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use columine_types::PROGRAM_MAGIC;
use columine_types::types::{EMPTY_KEY, ErrorCode};
use columine_vm::meta::SlotMetaView;
use columine_vm::row_exprs::{
    BatchView, Binding, ROW_EXPRESSIONS_MAGIC, RowColumns, RowExpression, bind_row_columns,
};
use columine_vm::state_init::{DEFAULT_ACCEPTED_PROGRAM_MAGICS, calculate_state_size, init_state};
use columine_vm::vm::{Vm, col_u32_exact, u32s_as_bytes, vm_map_get};

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

/// Columns: 0 type, 1 key, 2 value, 3..=5 bound derived, 6 live derived.
const TYPE_COL: u8 = 0;
const KEY_COL: u8 = 1;
const VAL_COL: u8 = 2;
const BOUND_COLS: [u8; 3] = [3, 4, 5];
const LIVE_COL: u8 = 6;
const NUM_COLS: u8 = 7;
const ORDER_TYPE: u32 = 7;

/// `FOR_EACH(type == ORDER_TYPE) { BatchMapUpsertLastIf(slot 0, key, value,
/// live) }` over one hash-map slot, followed by `trailer`.
fn program(capacity: u32, trailer: &[u8]) -> Vec<u8> {
    let mut init = vec![0x10u8, 0, 0x00];
    init.extend(capacity.to_le_bytes()[..2].iter());
    init.push(0);
    let body = [0x2au8, 0, KEY_COL, VAL_COL, LIVE_COL];
    let mut reduce = vec![0xE0u8, TYPE_COL, 1];
    reduce.extend(ORDER_TYPE.to_le_bytes());
    reduce.extend((body.len() as u16).to_le_bytes());
    reduce.extend(body);
    reduce.push(0);
    let mut prog = vec![0u8; 32];
    prog.extend(PROGRAM_MAGIC.to_le_bytes());
    prog.extend([1, 0, 1, NUM_COLS, 0, 0]);
    prog.extend((init.len() as u16).to_le_bytes());
    prog.extend((reduce.len() as u16).to_le_bytes());
    prog.extend(init);
    prog.extend(&reduce);
    prog.extend_from_slice(trailer);
    prog
}

fn entry(target: u8, expr: &[u8]) -> Vec<u8> {
    let mut out = vec![target, 1];
    out.extend(ORDER_TYPE.to_le_bytes());
    out.extend((expr.len() as u16).to_le_bytes());
    out.extend_from_slice(expr);
    out
}

fn table(entries: &[Vec<u8>]) -> Vec<u8> {
    let mut out = ROW_EXPRESSIONS_MAGIC.to_le_bytes().to_vec();
    out.extend((entries.len() as u16).to_le_bytes());
    out.push(TYPE_COL);
    for e in entries {
        out.extend_from_slice(e);
    }
    out
}

/// Expression `[0, col, threshold u32le]` binds up front: 1 when the row's
/// `col` exceeds `threshold`. Expression `[1, key_col, val_col]` binds live:
/// 1 when the row's value exceeds what slot 0 stores for its key.
struct Exprs;

impl RowExpression for Exprs {
    fn admit(&mut self, expr: &[u8], _batch: &BatchView<'_>) -> Result<Binding, ErrorCode> {
        match expr[0] {
            0 => Ok(Binding::Bound),
            _ => Ok(Binding::Live),
        }
    }

    fn eval(&mut self, expr: &[u8], batch: &BatchView<'_>, row: u32) -> Result<u32, ErrorCode> {
        if expr[0] == 0 {
            let cells = col_u32_exact(batch.column(expr[1]), batch.batch_len).expect("covered");
            let threshold = u32::from_le_bytes([expr[2], expr[3], expr[4], expr[5]]);
            return Ok(u32::from(cells[row as usize] > threshold));
        }
        let key =
            col_u32_exact(batch.column(expr[1]), batch.batch_len).expect("covered")[row as usize];
        let val =
            col_u32_exact(batch.column(expr[2]), batch.batch_len).expect("covered")[row as usize];
        let meta = SlotMetaView::read(batch.state, 0);
        let stored = vm_map_get(batch.state, meta.offset, meta.capacity, key);
        Ok(u32::from(stored == EMPTY_KEY || val > stored))
    }
}

fn bound_expr(col: u8, threshold: u32) -> Vec<u8> {
    let mut e = vec![0, col];
    e.extend(threshold.to_le_bytes());
    e
}

fn init(prog: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS);
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("init_state");
    state
}

struct Cell {
    name: &'static str,
    rows: usize,
    bound_entries: usize,
}

fn main() {
    let cells = [
        Cell {
            name: "live/1",
            rows: 1,
            bound_entries: 0,
        },
        Cell {
            name: "live/64",
            rows: 64,
            bound_entries: 0,
        },
        Cell {
            name: "live/4096",
            rows: 4096,
            bound_entries: 0,
        },
        Cell {
            name: "live+3bound/64",
            rows: 64,
            bound_entries: 3,
        },
        Cell {
            name: "live+3bound/4096",
            rows: 4096,
            bound_entries: 3,
        },
    ];
    println!("| cell | wall (mean) | per row | allocs | bytes |");
    println!("| --- | --- | --- | --- | --- |");
    for cell in &cells {
        let mut entries: Vec<Vec<u8>> = BOUND_COLS[..cell.bound_entries]
            .iter()
            .map(|&col| entry(col, &bound_expr(VAL_COL, 10)))
            .collect();
        entries.push(entry(LIVE_COL, &[1, KEY_COL, VAL_COL]));
        let prog = program(8192, &table(&entries));
        let n = cell.rows;
        let types = vec![ORDER_TYPE; n];
        // 64 keys, values ascending so most rows land (the upsert's cost is
        // in the census either way; the read is what the cell measures).
        let keys: Vec<u32> = (0..n).map(|i| (i % 64) as u32 + 1).collect();
        let vals: Vec<u32> = (0..n).map(|i| i as u32 + 1).collect();
        let mut rows = RowColumns::new();
        let mut eval = Exprs;
        let mut vm = Vm::new(DEFAULT_ACCEPTED_PROGRAM_MAGICS);
        let iters = if n >= 4096 { 200 } else { 2000 };
        let mut run = |timed: bool| -> (u128, usize, usize) {
            let mut state = init(&prog);
            let mut cols: Vec<&[u8]> = vec![
                u32s_as_bytes(&types),
                u32s_as_bytes(&keys),
                u32s_as_bytes(&vals),
                &[],
                &[],
                &[],
                &[],
            ];
            let a0 = ALLOCS.load(Ordering::Relaxed);
            let b0 = BYTES.load(Ordering::Relaxed);
            let t0 = Instant::now();
            let mut live =
                bind_row_columns(&prog, &state, &mut cols, n as u32, &mut eval, &mut rows)
                    .expect("bind");
            let status = vm.execute_batch_live(&mut state, &prog, &cols, n as u32, &mut live);
            let dt = t0.elapsed().as_nanos();
            let allocs = ALLOCS.load(Ordering::Relaxed) - a0;
            let bytes = BYTES.load(Ordering::Relaxed) - b0;
            assert_eq!(status, ErrorCode::Ok as u32);
            if timed {
                (dt, allocs, bytes)
            } else {
                (0, 0, 0)
            }
        };
        for _ in 0..iters / 10 {
            run(false);
        }
        let mut total = 0u128;
        let (mut allocs, mut bytes) = (0usize, 0usize);
        for _ in 0..iters {
            let (dt, a, b) = run(true);
            total += dt;
            allocs = allocs.max(a);
            bytes = bytes.max(b);
        }
        let mean = total / iters as u128;
        println!(
            "| {} | {} ns | {} ns | {} | {} |",
            cell.name,
            mean,
            mean / n as u128,
            allocs,
            bytes
        );
    }
}
