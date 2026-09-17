//! Measured deletion differential. The control uses the prior tombstone
//! kernel; the candidate calls the library's compacting deletion.
//! Run with `cargo run -p columine-vm --example deletion_swap --release`.
use columine_types::types::{EMPTY_KEY, TOMBSTONE, hash_key};
use columine_types::types::{SLOT_META_SIZE, STATE_HEADER_SIZE, SlotMetaOffset};
use columine_vm::bytes;
use columine_vm::hash_table::{ENTRY_U32, FlatTable, hashmap_byte_size};
use columine_vm::hashmap_ops::{
    CmpType, Strategy, batch_map_remove, batch_map_upsert, bind_slot_map,
};
use columine_vm::hooks::NoVm;
use columine_vm::meta::SlotMetaView;
use proptest::prelude::*;
use std::hint::black_box;
use std::time::Instant;

#[inline(never)]
fn tombstone(table: &FlatTable, state: &mut [u8], key: u32) -> bool {
    let Some(pos) = table.find(state, key) else {
        return false;
    };
    table.set_key_at(state, pos, TOMBSTONE);
    table.set_size(state, table.size(state) - 1);
    true
}

#[inline(never)]
fn backward_shift(table: &FlatTable, state: &mut [u8], key: u32) -> bool {
    let Some(pos) = table.find(state, key) else {
        return false;
    };
    table.erase_at(state, pos, |_, _, _| {});
    table.set_size(state, table.size(state) - 1);
    true
}

fn new_table(cap: u32) -> (FlatTable, Vec<u8>) {
    let mut bytes = vec![0; hashmap_byte_size(cap) as usize];
    let table = FlatTable::init(&mut bytes, 0, cap, ENTRY_U32);
    (table, bytes)
}

fn differential(ops: &[(bool, u32, u32)]) -> Result<(), TestCaseError> {
    let (table, mut control) = new_table(128);
    let mut candidate = control.clone();
    for &(remove, key, value) in ops {
        if remove {
            prop_assert_eq!(
                tombstone(&table, &mut control, key),
                backward_shift(&table, &mut candidate, key)
            );
        } else {
            prop_assert_eq!(
                table.upsert_u32(&mut control, key, value),
                table.upsert_u32(&mut candidate, key, value)
            );
        }
        prop_assert_eq!(table.size(&control), table.size(&candidate));
        for key in 0..80 {
            prop_assert_eq!(table.get_u32(&control, key), table.get_u32(&candidate, key));
        }
        let mut left: Vec<_> = table
            .iter_live(&control)
            .map(|(_, key)| (key, table.get_u32(&control, key)))
            .collect();
        let mut right: Vec<_> = table
            .iter_live(&candidate)
            .map(|(_, key)| (key, table.get_u32(&candidate, key)))
            .collect();
        left.sort_unstable();
        right.sort_unstable();
        prop_assert_eq!(left, right);
    }
    Ok(())
}

fn order_witness() {
    let cap = 16;
    let keys: Vec<_> = (0..10_000)
        .filter(|&key| hash_key(key, cap) == cap - 1)
        .take(3)
        .collect();
    let (table, mut control) = new_table(cap);
    for &key in &keys {
        table.upsert_u32(&mut control, key, key + 1);
    }
    let mut candidate = control.clone();
    tombstone(&table, &mut control, keys[0]);
    backward_shift(&table, &mut candidate, keys[0]);
    let old_order: Vec<_> = table.iter_live(&control).collect();
    let new_order: Vec<_> = table.iter_live(&candidate).collect();
    let (table, mut reverse) = new_table(cap);
    for &key in keys.iter().rev() {
        table.upsert_u32(&mut reverse, key, key + 1);
    }
    let mut old_reverse = reverse.clone();
    tombstone(&table, &mut old_reverse, keys[0]);
    backward_shift(&table, &mut reverse, keys[0]);
    println!("order witness: keys={keys:?} old={old_order:?} new={new_order:?}");
    println!(
        "old-only forward/reverse byte equality: {}",
        control == old_reverse
    );
    println!(
        "new-only forward/reverse byte equality: {}",
        candidate == reverse
    );
}

fn batch_order_witness() {
    let offset = STATE_HEADER_SIZE + SLOT_META_SIZE;
    let mut images = Vec::new();
    for keys in [[3u32, 18, 24], [24, 18, 3]] {
        let mut state = vec![0u8; (offset + 16 * 8) as usize];
        bytes::write_u32(&mut state, STATE_HEADER_SIZE, offset);
        bytes::write_u32(&mut state, STATE_HEADER_SIZE + SlotMetaOffset::CAPACITY, 16);
        state[(STATE_HEADER_SIZE + SlotMetaOffset::TYPE_FLAGS) as usize] = 0x40;
        bytes::fill_u32(&mut state, offset, 16, EMPTY_KEY);
        let meta = SlotMetaView::read(&state, 0);
        let values = keys.map(|key| key + 1);
        let result = batch_map_upsert(
            Strategy::Last,
            false,
            &mut state,
            &meta,
            0,
            &keys,
            &values,
            None,
            CmpType::F64,
            &mut NoVm,
        );
        assert_eq!(result, columine_types::types::ErrorCode::Ok);
        let mut shifted = state.clone();
        let table = bind_slot_map(&meta);
        tombstone(&table, &mut state, 3);
        meta.set_change_flag(&mut state, columine_types::types::ChangeFlag::REMOVED);
        batch_map_remove(false, &mut shifted, &meta, 0, &[3], &mut NoVm);
        images.push((state, shifted));
    }
    println!(
        "actual within-batch permutation: old_equal={} new_equal={}",
        images[0].0 == images[1].0,
        images[0].1 == images[1].1
    );
}

#[inline(never)]
fn churn(
    table: &FlatTable,
    state: &mut [u8],
    live: &mut [u32],
    next: &mut u32,
    shift: bool,
    rounds: usize,
) -> u64 {
    let mut checksum = 0u64;
    for i in 0..rounds {
        let slot = i % live.len();
        let key = live[slot];
        checksum += u64::from(
            table
                .get_u32(black_box(state), black_box(key))
                .expect("live key"),
        );
        if shift {
            backward_shift(table, state, black_box(key));
        } else {
            tombstone(table, state, black_box(key));
        }
        let incoming = *next;
        *next += 1;
        assert_eq!(table.upsert_u32(state, incoming, incoming), Some(true));
        live[slot] = incoming;
        checksum ^= u64::from(
            table
                .get_u32(black_box(state), black_box(incoming + 1_000_000))
                .unwrap_or(0),
        );
    }
    black_box(checksum)
}

fn cells() {
    for cap in [16, 128, 1024] {
        let (table, mut initial) = new_table(cap);
        let count = (table.max_load() / 2) as usize;
        let initial_keys: Vec<_> = (0..count as u32).collect();
        for &key in &initial_keys {
            table.upsert_u32(&mut initial, key, key);
        }
        let mut control = initial.clone();
        let mut candidate = initial;
        let mut control_keys = initial_keys.clone();
        let mut candidate_keys = initial_keys;
        let mut control_next = count as u32;
        let mut candidate_next = control_next;
        let rounds = 20_000;
        let mut times = [Vec::new(), Vec::new()];
        for rep in 0..11 {
            for lane in [rep % 2, 1 - rep % 2] {
                let start = Instant::now();
                let checksum = if lane == 0 {
                    churn(
                        &table,
                        &mut control,
                        &mut control_keys,
                        &mut control_next,
                        false,
                        rounds,
                    )
                } else {
                    churn(
                        &table,
                        &mut candidate,
                        &mut candidate_keys,
                        &mut candidate_next,
                        true,
                        rounds,
                    )
                };
                black_box(checksum);
                let elapsed = start.elapsed().as_nanos() as f64 / rounds as f64;
                if rep != 0 {
                    times[lane].push(elapsed);
                }
            }
        }
        for values in &mut times {
            values.sort_by(f64::total_cmp);
        }
        println!(
            "churn cap={cap} occupancy={count} old_ns={:.2} new_ns={:.2} ratio={:.3}",
            times[0][5],
            times[1][5],
            times[1][5] / times[0][5]
        );
    }
}

fn main() {
    let mut runner = proptest::test_runner::TestRunner::new(proptest::test_runner::Config {
        cases: 256,
        rng_seed: proptest::test_runner::RngSeed::Fixed(0xdecafbad),
        failure_persistence: None,
        ..Default::default()
    });
    runner
        .run(
            &prop::collection::vec((any::<bool>(), 0u32..80, any::<u32>()), 0..400),
            |ops| differential(&ops),
        )
        .expect("old/new logical deletion oracle");
    println!(
        "logical deletion oracle: 256 streams passed (up to 399 operations; all 80 keys checked after each)"
    );
    order_witness();
    batch_order_witness();
    cells();
}
