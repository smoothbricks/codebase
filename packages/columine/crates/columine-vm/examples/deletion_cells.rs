//! Same-binary control/candidate deletion cell for native and wasm hosts.
//! Both arms execute the library's lookup/upsert kernels; only removal differs.
use columine_types::types::TOMBSTONE;
use columine_vm::hash_table::{ENTRY_U32, FlatTable, hashmap_byte_size};
use std::hint::black_box;

#[unsafe(no_mangle)]
pub extern "C" fn deletion_cell(mode: u32, cap: u32, rounds: u32, seed: u32) -> u32 {
    assert!(mode <= 1 && cap.is_power_of_two() && (16..=4096).contains(&cap));
    let mut state = vec![0; hashmap_byte_size(cap) as usize];
    let table = FlatTable::init(&mut state, 0, cap, ENTRY_U32);
    let mut live: Vec<_> = (0..table.max_load() / 2)
        .map(|i| i + (seed % 1000))
        .collect();
    for &key in &live {
        assert_eq!(table.upsert_u32(&mut state, key, key), Some(true));
    }
    let first_new = live.last().copied().expect("nonempty cell") + 1;
    let mut checksum = 0u32;
    for i in 0..rounds {
        let next = first_new + i;
        let index = i as usize % live.len();
        let key = live[index];
        checksum = checksum.wrapping_add(
            table
                .get_u32(black_box(&state), black_box(key))
                .expect("live key"),
        );
        let pos = table
            .find(black_box(&state), black_box(key))
            .expect("live key");
        if mode == 0 {
            table.set_key_at(&mut state, pos, TOMBSTONE);
        } else {
            table.erase_at(&mut state, pos, |_, _, _| {});
        }
        let size = table.size(&state);
        table.set_size(&mut state, size - 1);
        assert_eq!(table.upsert_u32(&mut state, next, next), Some(true));
        live[index] = next;
        checksum ^= table
            .get_u32(black_box(&state), black_box(next + 1_000_000))
            .unwrap_or(0);
    }
    black_box(checksum)
}

#[cfg(not(target_arch = "wasm32"))]
fn main() {
    for cap in [16, 128, 1024] {
        let mut times = [Vec::new(), Vec::new()];
        let mut results = [0; 2];
        for rep in 0..10 {
            for arm in 0..2 {
                let mode = (rep + arm) % 2;
                let start = std::time::Instant::now();
                results[mode] = deletion_cell(mode as u32, cap, 20_000, 92009);
                if rep != 0 {
                    times[mode].push(start.elapsed().as_nanos() as f64 / 20_000.0);
                }
            }
        }
        assert_eq!(results[0], results[1]);
        for t in &mut times {
            t.sort_by(f64::total_cmp);
        }
        println!(
            "cap={cap} old_ns={:.2} new_ns={:.2} checksum={}",
            times[0][4], times[1][4], results[0]
        );
    }
}
