//! Coverage for HashMap/HashSet containers, iteration order, stress cases,
//! and property tests.
//!
//! Stress tests use a deterministic local LCG; the random stream is not part
//! of the contract. Assertions pin insertion, lookup, size, and placement.

use columine_types::types::{
    ChangeFlag, EMPTY_KEY, ErrorCode, PROGRAM_FORMAT_VERSION, STATE_FORMAT_VERSION,
    STATE_HEADER_SIZE, STATE_MAGIC, SlotMetaOffset, SlotType, StateHeaderOffset, TOMBSTONE,
    hash_key,
};
use columine_vm::bytes;
use columine_vm::hash_table::{
    ENTRY_NONE, ENTRY_U32, FlatTable, hashmap_byte_size, hashset_byte_size,
};
use columine_vm::hashmap_ops::{
    CmpType, Strategy, batch_map_remove, batch_map_upsert, bind_slot_map, single_map_remove,
    single_map_upsert,
};
use columine_vm::hashset_ops::{batch_set_insert, batch_set_remove, bind_slot_set};
use columine_vm::hooks::NoVm;
use columine_vm::meta::SlotMetaView;
use columine_vm::slot_growth::slot_data_size;
use columine_vm::vm::{UNDO_CAPACITY, Vm};
use proptest::prelude::*;
// The `hashmap_ops::Strategy` ENUM above shadows the prelude's `Strategy`
// TRAIT (explicit import beats glob); re-import the trait under an alias so
// `.prop_map` resolves.
use proptest::strategy::Strategy as PropStrategy;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct CountingAllocator;

thread_local! {
    // Thread-local accounting excludes allocations made by concurrent tests.
    static ALLOCATION_COUNT: Cell<Option<usize>> = const { Cell::new(None) };
    // While set, every allocation on this thread fails.
    static ALLOCATION_REFUSED: Cell<bool> = const { Cell::new(false) };
}

fn allocation_refused() -> bool {
    ALLOCATION_REFUSED.try_with(Cell::get).unwrap_or(false)
}

fn count_allocation() {
    let _ = ALLOCATION_COUNT.try_with(|count| {
        if let Some(current) = count.get() {
            count.set(Some(current + 1));
        }
    });
}

// SAFETY: every allocation/deallocation forwards the original pointer and
// layout to System; the counter does not change the allocation contract. A
// refused allocation returns null, which GlobalAlloc permits for any call.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count_allocation();
        if allocation_refused() {
            return core::ptr::null_mut();
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count_allocation();
        if allocation_refused() {
            return core::ptr::null_mut();
        }
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count_allocation();
        if allocation_refused() {
            return core::ptr::null_mut();
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

struct AllocationScope;

impl Drop for AllocationScope {
    fn drop(&mut self) {
        ALLOCATION_COUNT.with(|count| count.set(None));
    }
}

struct RefusalScope;

impl Drop for RefusalScope {
    fn drop(&mut self) {
        ALLOCATION_REFUSED.with(|refused| refused.set(false));
    }
}

/// Run `run` while every allocation on this thread fails.
fn without_memory<T>(run: impl FnOnce() -> T) -> T {
    ALLOCATION_REFUSED.with(|refused| refused.set(true));
    let scope = RefusalScope;
    let result = run();
    drop(scope);
    result
}

fn allocations_during<T>(run: impl FnOnce() -> T) -> (T, usize) {
    ALLOCATION_COUNT.with(|count| {
        assert!(count.get().is_none(), "allocation scopes cannot nest");
        count.set(Some(0));
    });
    let scope = AllocationScope;
    let result = run();
    let count = ALLOCATION_COUNT.with(|count| count.get().expect("active allocation scope"));
    drop(scope);
    (result, count)
}

// ---------------------------------------------------------------------------
// Test scaffolding: manual state construction with a header, one 48-byte
// slot-metadata record, and slot data immediately after it.
// ---------------------------------------------------------------------------

const SLOT_OFFSET: u32 = STATE_HEADER_SIZE + 48; // SLOT_META_SIZE

fn mk_slot_state(cap: u32, type_flags_byte: u8) -> Vec<u8> {
    let mut state = vec![0u8; 8192usize.max((SLOT_OFFSET + cap * 32) as usize)];
    bytes::write_u32(&mut state, StateHeaderOffset::MAGIC, STATE_MAGIC);
    state[StateHeaderOffset::FORMAT_VERSION as usize] = STATE_FORMAT_VERSION;
    bytes::write_u16(
        &mut state,
        StateHeaderOffset::PROGRAM_VERSION,
        PROGRAM_FORMAT_VERSION,
    );
    let meta = STATE_HEADER_SIZE;
    state[meta as usize..meta as usize + 4].copy_from_slice(&SLOT_OFFSET.to_le_bytes());
    state[meta as usize + 4..meta as usize + 8].copy_from_slice(&cap.to_le_bytes());
    // size = 0 already; type_flags:
    state[(meta + SlotMetaOffset::TYPE_FLAGS) as usize] = type_flags_byte;
    bytes::write_u32(&mut state, StateHeaderOffset::NUM_SLOTS, 1);
    // keys → EMPTY_KEY
    for j in 0..cap {
        let off = (SLOT_OFFSET + j * 4) as usize;
        state[off..off + 4].copy_from_slice(&EMPTY_KEY.to_le_bytes());
    }
    state
}

fn cmp_lane_fill(state: &mut [u8], cap: u32, bits: u64) {
    for j in 0..cap {
        let off = (SLOT_OFFSET + cap * 8 + j * 8) as usize;
        state[off..off + 8].copy_from_slice(&bits.to_le_bytes());
    }
}

fn meta_of(state: &[u8]) -> SlotMetaView {
    SlotMetaView::read(state, 0)
}

fn mk_ttl_slot_state(cap: u32, type_flags: u8) -> Vec<u8> {
    let mut state = mk_slot_state(cap, type_flags);
    let meta = STATE_HEADER_SIZE;
    let view = meta_of(&state);
    let primary_size = slot_data_size(
        view.slot_type(),
        cap,
        view.has_hashmap_timestamp_storage(),
        0,
    );
    bytes::write_u32(
        &mut state,
        meta + SlotMetaOffset::EVICTION_INDEX_OFFSET,
        SLOT_OFFSET + primary_size,
    );
    bytes::write_u32(
        &mut state,
        meta + SlotMetaOffset::EVICTION_INDEX_CAPACITY,
        cap,
    );
    state
}

/// A valid retained checkpoint may contain older TTL records for a live key.
/// Fill the actual index geometry, leaving primary storage seeded by public VM
/// operations. Capacity still equals the slot capacity; no admission bypass.
fn fill_retained_ttl_index(state: &mut [u8], meta: &SlotMetaView, first: u32, repeated: u32) {
    let value = |key| match meta.slot_type() {
        SlotType::HashMap => bind_slot_map(meta)
            .get_u32(state, key)
            .expect("live fixture key"),
        SlotType::HashSet => key,
        _ => panic!("retained TTL fixture is only a map/set"),
    };
    let first_value = value(first);
    let repeated_value = value(repeated);
    let base = meta.eviction_index_offset(state);
    let capacity = meta.eviction_index_capacity(state);
    for position in 0..capacity {
        let offset = base + position * columine_types::types::EVICTION_ENTRY_SIZE;
        bytes::write_f64(state, offset, 10.0 + f64::from(position));
        bytes::write_u32(
            state,
            offset + 8,
            if position == 0 { first } else { repeated },
        );
        bytes::write_u32(
            state,
            offset + 12,
            if position == 0 {
                first_value
            } else {
                repeated_value
            },
        );
    }
    meta.set_eviction_index_size(state, capacity);
}

// ---------------------------------------------------------------------------
// Flat-table behavior

///  `test "HashSet — insert, contains, dedup"`
#[test]
fn hashset_insert_contains_dedup() {
    let mut buf = vec![0u8; 512];
    let tbl = FlatTable::init(&mut buf, 0, 16, ENTRY_NONE);

    assert_eq!(tbl.size(&buf), 0);
    assert_eq!(tbl.insert_key(&mut buf, 42), Some(true));
    assert_eq!(tbl.size(&buf), 1);
    assert!(tbl.contains(&buf, 42));
    assert!(!tbl.contains(&buf, 43));
    assert_eq!(tbl.insert_key(&mut buf, 42), Some(false));
    assert_eq!(tbl.size(&buf), 1);
    assert_eq!(tbl.insert_key(&mut buf, 100), Some(true));
    assert_eq!(tbl.insert_key(&mut buf, 200), Some(true));
    assert_eq!(tbl.size(&buf), 3);
}

///  `test "HashMap — upsert, get, overwrite"`
#[test]
fn hashmap_upsert_get_overwrite() {
    let mut buf = vec![0u8; 1024];
    let tbl = FlatTable::init(&mut buf, 0, 16, ENTRY_U32);

    assert_eq!(tbl.upsert_u32(&mut buf, 10, 100), Some(true));
    assert_eq!(tbl.size(&buf), 1);
    assert_eq!(tbl.get_u32(&buf, 10), Some(100));
    assert_eq!(tbl.upsert_u32(&mut buf, 10, 200), Some(false));
    assert_eq!(tbl.get_u32(&buf, 10), Some(200));
    assert_eq!(tbl.get_u32(&buf, 99), None);
}

///  `test "HashSet — load factor triggers null"`
#[test]
fn hashset_load_factor_triggers_none() {
    let mut buf = vec![0u8; 512];
    let tbl = FlatTable::init(&mut buf, 0, 16, ENTRY_NONE);

    // 70% of 16 = 11.2 → 11 fit, 12th refused.
    for i in 1..=11u32 {
        assert!(tbl.insert_key(&mut buf, i).is_some());
    }
    assert_eq!(tbl.size(&buf), 11);
    assert_eq!(tbl.insert_key(&mut buf, 12), None);
}

///  `test "HashSet — rehash into larger table"`
#[test]
fn hashset_rehash_into_larger() {
    let mut buf = vec![0u8; 4096];
    let src = FlatTable::init(&mut buf, 0, 16, ENTRY_NONE);
    for i in 1..=10u32 {
        src.insert_key(&mut buf, i);
    }
    assert_eq!(src.size(&buf), 10);

    let dst = src.rehash_into(&mut buf, 512, 32);
    assert_eq!(dst.size(&buf), 10);
    for i in 1..=10u32 {
        assert!(dst.contains(&buf, i));
    }
    assert!(!dst.contains(&buf, 99));
}

///  `test "HashMap — rehash preserves values"`
#[test]
fn hashmap_rehash_preserves_values() {
    let mut buf = vec![0u8; 4096];
    let src = FlatTable::init(&mut buf, 0, 16, ENTRY_U32);
    src.upsert_u32(&mut buf, 1, 100);
    src.upsert_u32(&mut buf, 2, 200);
    src.upsert_u32(&mut buf, 3, 300);

    let dst = src.rehash_into(&mut buf, 1024, 32);
    assert_eq!(dst.size(&buf), 3);
    assert_eq!(dst.get_u32(&buf, 1), Some(100));
    assert_eq!(dst.get_u32(&buf, 2), Some(200));
    assert_eq!(dst.get_u32(&buf, 3), Some(300));
}

///  `test "FlatHashTable — bind to existing table"`
#[test]
fn bind_to_existing_table() {
    let mut buf = vec![0u8; 512];
    let created = FlatTable::init(&mut buf, 0, 16, ENTRY_NONE);
    created.insert_key(&mut buf, 42);
    created.insert_key(&mut buf, 99);

    let bound = FlatTable::bind(&buf, 0, ENTRY_NONE);
    assert_eq!(bound.size(&buf), 2);
    assert!(bound.contains(&buf, 42));
    assert!(bound.contains(&buf, 99));
}

///  `test "FlatHashTable — byteSize calculation"`
#[test]
fn byte_size_calculation() {
    assert_eq!(hashset_byte_size(16), 72); // 8 + 16*4
    assert_eq!(hashmap_byte_size(16), 136); // 8 + 16*4 + 16*4
}

///  `test "EMPTY_KEY and TOMBSTONE are rejected"`
#[test]
fn sentinel_keys_rejected() {
    let mut buf = vec![0u8; 512];
    let tbl = FlatTable::init(&mut buf, 0, 16, ENTRY_NONE);
    assert_eq!(tbl.insert_key(&mut buf, EMPTY_KEY), None);
    assert_eq!(tbl.insert_key(&mut buf, TOMBSTONE), None);
    assert_eq!(tbl.size(&buf), 0);
}

///  `test "stress — randomized insert 1000 keys, verify all
/// found"` (deterministic LCG replaces the reference DefaultPrng; same assertions).
#[test]
fn stress_randomized_insert_1000_keys() {
    let mut buf = vec![0u8; 32768];
    let tbl = FlatTable::init(&mut buf, 0, 2048, ENTRY_NONE);

    let mut lcg: u64 = 0xDEAD_BEEF;
    let mut inserted = Vec::new();
    for _ in 0..1000 {
        lcg = lcg
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let key = 1 + ((lcg >> 33) as u32) % 100_000;
        if let Some(true) = tbl.insert_key(&mut buf, key) {
            inserted.push(key);
        }
    }
    for &key in &inserted {
        assert!(tbl.contains(&buf, key));
    }
    assert_eq!(tbl.size(&buf), inserted.len() as u32);
}

///  `test "stress — fill to load factor then rehash preserves all"`
#[test]
fn stress_fill_then_rehash() {
    let mut buf = vec![0u8; 4096];
    let src = FlatTable::init(&mut buf, 0, 32, ENTRY_NONE);
    for i in 1..=22u32 {
        assert_eq!(src.insert_key(&mut buf, i), Some(true));
    }
    assert_eq!(src.size(&buf), 22);

    let dst = src.rehash_into(&mut buf, 1024, 64);
    assert_eq!(dst.size(&buf), 22);
    for i in 1..=22u32 {
        assert!(dst.contains(&buf, i));
    }
}

///  `test "property — size always equals count of live keys"`
#[test]
fn size_equals_live_key_count() {
    let mut buf = vec![0u8; 8192];
    let tbl = FlatTable::init(&mut buf, 0, 128, ENTRY_U32);

    let mut count = 0u32;
    for i in 1..=80u32 {
        match tbl.upsert_u32(&mut buf, i, i * 10) {
            Some(true) => count += 1,
            Some(false) => {}
            None => break,
        }
    }
    assert_eq!(tbl.size(&buf), count);

    let live = tbl.iter_live(&buf).count() as u32;
    assert_eq!(tbl.size(&buf), live);
}

///  `test "stress — HashMap upsert overwrites preserve size"`
#[test]
fn stress_upsert_overwrites_preserve_size() {
    let mut buf = vec![0u8; 4096];
    let tbl = FlatTable::init(&mut buf, 0, 128, ENTRY_U32);

    for i in 1..=50u32 {
        assert_eq!(tbl.upsert_u32(&mut buf, i, i * 100), Some(true));
    }
    assert_eq!(tbl.size(&buf), 50);
    for i in 1..=50u32 {
        assert_eq!(tbl.upsert_u32(&mut buf, i, i * 200), Some(false));
    }
    assert_eq!(tbl.size(&buf), 50);
    for i in 1..=50u32 {
        assert_eq!(tbl.get_u32(&buf, i), Some(i * 200));
    }
}

///  `test "edge — max valid key (0xFFFFFFFD) works correctly"`
#[test]
fn edge_max_valid_key() {
    let mut buf = vec![0u8; 512];
    let tbl = FlatTable::init(&mut buf, 0, 16, ENTRY_NONE);
    assert_eq!(tbl.insert_key(&mut buf, 0xFFFF_FFFD), Some(true));
    assert!(tbl.contains(&buf, 0xFFFF_FFFD));
    assert_eq!(tbl.size(&buf), 1);
    assert!(!tbl.contains(&buf, EMPTY_KEY));
    assert!(!tbl.contains(&buf, TOMBSTONE));
}

///  `test "edge — HashMap upsert with key near sentinels"`
#[test]
fn edge_upsert_near_sentinels() {
    let mut buf = vec![0u8; 1024];
    let tbl = FlatTable::init(&mut buf, 0, 16, ENTRY_U32);
    assert_eq!(tbl.upsert_u32(&mut buf, 0xFFFF_FFFD, 42), Some(true));
    assert_eq!(tbl.get_u32(&buf, 0xFFFF_FFFD), Some(42));
    assert_eq!(tbl.get_u32(&buf, EMPTY_KEY), None);
    assert_eq!(tbl.get_u32(&buf, TOMBSTONE), None);
}

// ---------------------------------------------------------------------------
// Iteration order is observable through checked readers. The sequences below are
// hand-derived from `hash_key` and the ascending-slot scan.
// ---------------------------------------------------------------------------

/// hash_key(·, 16): 1→7, 2→14, 3→15, 4→5, 5→13 (computed from the verified
/// `hash_key` port). No collisions → iteration is slot order 5,7,13,14,15 →
/// keys 4,1,5,2,3.
#[test]
fn iteration_order_pinned_no_collision() {
    let mut buf = vec![0u8; 512];
    let tbl = FlatTable::init(&mut buf, 0, 16, ENTRY_NONE);
    for k in [1u32, 2, 3, 4, 5] {
        assert_eq!(tbl.insert_key(&mut buf, k), Some(true));
    }
    let seq: Vec<(u32, u32)> = tbl.iter_live(&buf).collect();
    assert_eq!(seq, vec![(5, 4), (7, 1), (13, 5), (14, 2), (15, 3)]);
}

/// hash_key(·, 16): 20→1, 4→5, 30→5 (COLLIDES with 4; linear-probes to 6),
/// 1→7, 10→8. Insertion order 1,4,10,20,30 → iteration slots 1,5,6,7,8.
#[test]
fn iteration_order_pinned_with_collision() {
    let mut buf = vec![0u8; 512];
    let tbl = FlatTable::init(&mut buf, 0, 16, ENTRY_NONE);
    for k in [1u32, 4, 10, 20, 30] {
        assert_eq!(tbl.insert_key(&mut buf, k), Some(true));
    }
    let seq: Vec<(u32, u32)> = tbl.iter_live(&buf).collect();
    assert_eq!(seq, vec![(1, 20), (5, 4), (6, 30), (7, 1), (8, 10)]);
}

// ---------------------------------------------------------------------------
//  test blocks
// ---------------------------------------------------------------------------

///  `test "batchMapUpsert — last strategy inserts and overwrites"`
#[test]
fn batch_upsert_last_inserts_and_overwrites() {
    let mut state = mk_slot_state(16, 0x00); // HASHMAP with timestamps
    let meta = meta_of(&state);
    let mut hooks = NoVm::default();

    let r = batch_map_upsert(
        Strategy::Last,
        false,
        &mut state,
        &meta,
        0,
        &[10, 20, 30],
        &[100, 200, 300],
        None,
        CmpType::F64,
        &mut hooks,
    );
    assert_eq!(r, ErrorCode::Ok);
    assert_eq!(meta.size(&state), 3);

    let tbl = bind_slot_map(&meta);
    assert_eq!(tbl.get_u32(&state, 10), Some(100));
    assert_eq!(tbl.get_u32(&state, 20), Some(200));
    assert_eq!(tbl.get_u32(&state, 30), Some(300));

    let r = batch_map_upsert(
        Strategy::Last,
        false,
        &mut state,
        &meta,
        0,
        &[20],
        &[999],
        None,
        CmpType::F64,
        &mut hooks,
    );
    assert_eq!(r, ErrorCode::Ok);
    assert_eq!(tbl.get_u32(&state, 20), Some(999));
    assert_eq!(meta.size(&state), 3); // overwrite — size unchanged

    // One batch that updates and then inserts reports both changes, though
    // it writes each flag bit only once.
    state[(meta.meta_base + SlotMetaOffset::CHANGE_FLAGS) as usize] = 0;
    let r = batch_map_upsert(
        Strategy::Last,
        false,
        &mut state,
        &meta,
        0,
        &[30, 40, 10, 50],
        &[301, 400, 101, 500],
        None,
        CmpType::F64,
        &mut hooks,
    );
    assert_eq!(r, ErrorCode::Ok);
    assert_eq!(
        meta.change_flags(&state),
        ChangeFlag::UPDATED | ChangeFlag::INSERTED
    );
    assert_eq!(meta.size(&state), 5);
}

///  `test "batchMapUpsert — first strategy skips existing"`
#[test]
fn batch_upsert_first_skips_existing() {
    let mut state = mk_slot_state(16, 0x00);
    let meta = meta_of(&state);
    let mut hooks = NoVm::default();
    let tbl = bind_slot_map(&meta);

    batch_map_upsert(
        Strategy::First,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[100],
        None,
        CmpType::F64,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(100));

    batch_map_upsert(
        Strategy::First,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[999],
        None,
        CmpType::F64,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(100)); // still 100
}

fn f64_col(vals: &[f64]) -> Vec<u8> {
    vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

fn i64_col(vals: &[i64]) -> Vec<u8> {
    vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

fn u32_col(vals: &[u32]) -> Vec<u8> {
    vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

///  `test "batchMapUpsert — latest strategy uses timestamp
/// comparison (f64)"`
#[test]
fn batch_upsert_latest_f64() {
    let mut state = mk_slot_state(16, 0x00);
    cmp_lane_fill(&mut state, 16, f64::NEG_INFINITY.to_bits());
    let meta = meta_of(&state);
    let mut hooks = NoVm::default();
    let tbl = bind_slot_map(&meta);

    let col = f64_col(&[100.0]);
    batch_map_upsert(
        Strategy::Latest,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[100],
        Some(&col),
        CmpType::F64,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(100));

    // Older timestamp — keep original.
    let col = f64_col(&[50.0]);
    batch_map_upsert(
        Strategy::Latest,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[999],
        Some(&col),
        CmpType::F64,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(100));

    // Newer — update.
    let col = f64_col(&[200.0]);
    batch_map_upsert(
        Strategy::Latest,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[200],
        Some(&col),
        CmpType::F64,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(200));
}

/// Removing a key preserves the remaining map and publishes its change flag.
#[test]
fn batch_remove_preserves_other_keys() {
    let mut state = mk_slot_state(16, 0x00);
    let meta = meta_of(&state);
    let mut hooks = NoVm::default();
    let tbl = bind_slot_map(&meta);

    batch_map_upsert(
        Strategy::Last,
        false,
        &mut state,
        &meta,
        0,
        &[10, 20, 30],
        &[100, 200, 300],
        None,
        CmpType::F64,
        &mut hooks,
    );
    assert_eq!(tbl.size(&state), 3);

    assert_eq!(
        batch_map_remove(false, &mut state, &meta, 0, &[20], &mut hooks),
        ErrorCode::Ok
    );
    assert_eq!(tbl.size(&state), 2);
    assert_eq!(tbl.get_u32(&state, 20), None);
    assert!(tbl.get_u32(&state, 10).is_some());
    assert!(tbl.get_u32(&state, 30).is_some());
    assert_ne!(meta.change_flags(&state) & ChangeFlag::REMOVED, 0);
}

///  `test "batchMapUpsert — CAPACITY_EXCEEDED at 70% load"`
#[test]
fn batch_upsert_capacity_exceeded() {
    let mut state = mk_slot_state(16, 0x40); // HASHMAP, no_timestamps
    let meta = meta_of(&state);
    let mut hooks = NoVm::default();

    for i in 1..=11u32 {
        let r = batch_map_upsert(
            Strategy::Last,
            false,
            &mut state,
            &meta,
            0,
            &[i],
            &[i * 10],
            None,
            CmpType::F64,
            &mut hooks,
        );
        assert_eq!(r, ErrorCode::Ok);
    }
    assert_eq!(meta.size(&state), 11);

    let r = batch_map_upsert(
        Strategy::Last,
        false,
        &mut state,
        &meta,
        0,
        &[12],
        &[120],
        None,
        CmpType::F64,
        &mut hooks,
    );
    assert_eq!(r, ErrorCode::CapacityExceeded);
}

///  `test "batchMapUpsert — max strategy with u32 comparison"`
#[test]
fn batch_upsert_max_u32() {
    let mut state = mk_slot_state(16, 0x00);
    cmp_lane_fill(&mut state, 16, 0);
    let meta = meta_of(&state);
    let mut hooks = NoVm::default();
    let tbl = bind_slot_map(&meta);

    let col = u32_col(&[100]);
    batch_map_upsert(
        Strategy::Max,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[42],
        Some(&col),
        CmpType::U32,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(42));

    let col = u32_col(&[50]);
    batch_map_upsert(
        Strategy::Max,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[99],
        Some(&col),
        CmpType::U32,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(42));

    let col = u32_col(&[200]);
    batch_map_upsert(
        Strategy::Max,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[77],
        Some(&col),
        CmpType::U32,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(77));

    // Stored cmp value is the u32 zero-extended to u64.
    let pos = tbl.find(&state, 10).unwrap();
    let off = (SLOT_OFFSET + 16 * 8 + pos * 8) as usize;
    let stored = u64::from_le_bytes(state[off..off + 8].try_into().unwrap());
    assert_eq!(stored, 200);
}

///  `test "batchMapUpsert — latest strategy with i64 comparison"`
#[test]
fn batch_upsert_latest_i64() {
    let mut state = mk_slot_state(16, 0x00);
    cmp_lane_fill(&mut state, 16, i64::MIN as u64);
    let meta = meta_of(&state);
    let mut hooks = NoVm::default();
    let tbl = bind_slot_map(&meta);

    let col = i64_col(&[1000]);
    batch_map_upsert(
        Strategy::Latest,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[42],
        Some(&col),
        CmpType::I64,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(42));

    let col = i64_col(&[500]);
    batch_map_upsert(
        Strategy::Latest,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[99],
        Some(&col),
        CmpType::I64,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(42));

    let col = i64_col(&[2000]);
    batch_map_upsert(
        Strategy::Latest,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[77],
        Some(&col),
        CmpType::I64,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(77));

    let pos = tbl.find(&state, 10).unwrap();
    let off = (SLOT_OFFSET + 16 * 8 + pos * 8) as usize;
    let stored = i64::from_le_bytes(state[off..off + 8].try_into().unwrap());
    assert_eq!(stored, 2000);
}

///  `test "batchMapUpsert — min strategy with i64 comparison
/// (negative values)"`
#[test]
fn batch_upsert_min_i64_negative() {
    let mut state = mk_slot_state(16, 0x00);
    cmp_lane_fill(&mut state, 16, i64::MAX as u64);
    let meta = meta_of(&state);
    let mut hooks = NoVm::default();
    let tbl = bind_slot_map(&meta);

    let col = i64_col(&[-100]);
    batch_map_upsert(
        Strategy::Min,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[42],
        Some(&col),
        CmpType::I64,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(42));

    let col = i64_col(&[50]);
    batch_map_upsert(
        Strategy::Min,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[99],
        Some(&col),
        CmpType::I64,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(42));

    let col = i64_col(&[-200]);
    batch_map_upsert(
        Strategy::Min,
        false,
        &mut state,
        &meta,
        0,
        &[10],
        &[77],
        Some(&col),
        CmpType::I64,
        &mut hooks,
    );
    assert_eq!(tbl.get_u32(&state, 10), Some(77));

    let pos = tbl.find(&state, 10).unwrap();
    let off = (SLOT_OFFSET + 16 * 8 + pos * 8) as usize;
    let stored = i64::from_le_bytes(state[off..off + 8].try_into().unwrap());
    assert_eq!(stored, -200);
}

///  `test "singleMapUpsert — u32 comparison mode"`
#[test]
fn single_upsert_u32_mode() {
    let mut state = mk_slot_state(16, 0x00);
    cmp_lane_fill(&mut state, 16, 0);
    let meta = meta_of(&state);
    let mut hooks = NoVm::default();
    let tbl = bind_slot_map(&meta);

    assert_eq!(
        single_map_upsert(
            Strategy::Max,
            false,
            &mut state,
            &meta,
            0,
            5,
            42,
            10,
            CmpType::U32,
            &mut hooks
        ),
        ErrorCode::Ok
    );
    assert_eq!(tbl.get_u32(&state, 5), Some(42));

    assert_eq!(
        single_map_upsert(
            Strategy::Max,
            false,
            &mut state,
            &meta,
            0,
            5,
            99,
            5,
            CmpType::U32,
            &mut hooks
        ),
        ErrorCode::Ok
    );
    assert_eq!(tbl.get_u32(&state, 5), Some(42));

    assert_eq!(
        single_map_upsert(
            Strategy::Max,
            false,
            &mut state,
            &meta,
            0,
            5,
            77,
            20,
            CmpType::U32,
            &mut hooks
        ),
        ErrorCode::Ok
    );
    assert_eq!(tbl.get_u32(&state, 5), Some(77));
}

///  `test "singleMapUpsert — i64 comparison mode"`
#[test]
fn single_upsert_i64_mode() {
    let mut state = mk_slot_state(16, 0x00);
    cmp_lane_fill(&mut state, 16, i64::MIN as u64);
    let meta = meta_of(&state);
    let mut hooks = NoVm::default();
    let tbl = bind_slot_map(&meta);

    assert_eq!(
        single_map_upsert(
            Strategy::Latest,
            false,
            &mut state,
            &meta,
            0,
            5,
            42,
            (-50i64) as u64,
            CmpType::I64,
            &mut hooks
        ),
        ErrorCode::Ok
    );
    assert_eq!(tbl.get_u32(&state, 5), Some(42));

    assert_eq!(
        single_map_upsert(
            Strategy::Latest,
            false,
            &mut state,
            &meta,
            0,
            5,
            99,
            (-100i64) as u64,
            CmpType::I64,
            &mut hooks
        ),
        ErrorCode::Ok
    );
    assert_eq!(tbl.get_u32(&state, 5), Some(42));

    assert_eq!(
        single_map_upsert(
            Strategy::Latest,
            false,
            &mut state,
            &meta,
            0,
            5,
            77,
            100u64,
            CmpType::I64,
            &mut hooks
        ),
        ErrorCode::Ok
    );
    assert_eq!(tbl.get_u32(&state, 5), Some(77));
}

/// Additional coverage: single-key removal mirrors batch removal and is
/// exercised directly here.
#[test]
fn single_remove_preserves_other_entries() {
    let mut state = mk_slot_state(16, 0x40);
    let meta = meta_of(&state);
    let mut hooks = NoVm::default();
    let tbl = bind_slot_map(&meta);

    single_map_upsert(
        Strategy::Last,
        false,
        &mut state,
        &meta,
        0,
        7,
        70,
        0,
        CmpType::F64,
        &mut hooks,
    );
    assert_eq!(tbl.size(&state), 1);
    assert_eq!(
        single_map_remove(false, &mut state, &meta, 0, 7, &mut hooks),
        ErrorCode::Ok
    );
    assert_eq!(tbl.size(&state), 0);
    assert_eq!(tbl.get_u32(&state, 7), None);
    assert_ne!(meta.change_flags(&state) & ChangeFlag::REMOVED, 0);
}

/// Latest/max/min on a no-timestamp slot is INVALID_PROGRAM
/// () — pinned because it is an easy regression.
#[test]
fn latest_without_timestamp_storage_is_invalid_program() {
    let mut state = mk_slot_state(16, 0x40); // no_hashmap_timestamps
    let meta = meta_of(&state);
    let mut hooks = NoVm::default();
    let col = f64_col(&[1.0]);
    let r = batch_map_upsert(
        Strategy::Latest,
        false,
        &mut state,
        &meta,
        0,
        &[1],
        &[1],
        Some(&col),
        CmpType::F64,
        &mut hooks,
    );
    assert_eq!(r, ErrorCode::InvalidProgram);
}

// ---------------------------------------------------------------------------
//  test blocks
// ---------------------------------------------------------------------------

///  `test "batchSetInsert — insert and dedup"`
#[test]
fn batch_set_insert_and_dedup() {
    let mut state = mk_slot_state(16, 0x01); // HASHSET
    let meta = meta_of(&state);
    let mut hooks = NoVm::default();
    let tbl = bind_slot_set(&meta);

    let r = batch_set_insert(
        false,
        &mut state,
        &meta,
        0,
        &[10, 20, 10, 30, 20],
        None,
        &mut hooks,
    );
    assert_eq!(r, ErrorCode::Ok);
    assert_eq!(tbl.size(&state), 3);
    assert!(tbl.contains(&state, 10));
    assert!(tbl.contains(&state, 20));
    assert!(tbl.contains(&state, 30));
    assert!(!tbl.contains(&state, 40));
    assert_ne!(meta.change_flags(&state) & ChangeFlag::INSERTED, 0);
}

///  `test "batchSetRemove — remove existing"`
#[test]
fn batch_set_remove_existing() {
    let mut state = mk_slot_state(16, 0x01);
    let meta = meta_of(&state);
    let mut hooks = NoVm::default();
    let tbl = bind_slot_set(&meta);

    batch_set_insert(false, &mut state, &meta, 0, &[10, 20, 30], None, &mut hooks);
    assert_eq!(tbl.size(&state), 3);

    assert_eq!(
        batch_set_remove(false, &mut state, &meta, 0, &[20], &mut hooks),
        ErrorCode::Ok
    );
    assert_eq!(tbl.size(&state), 2);
    assert!(!tbl.contains(&state, 20));
    assert!(tbl.contains(&state, 10));
    assert!(tbl.contains(&state, 30));
}

///  `test "singleSetInsert — CAPACITY_EXCEEDED at load factor"`: the
/// per-element dispatch path is a batch of one.
#[test]
fn set_insert_one_at_a_time_capacity_exceeded() {
    let mut state = mk_slot_state(16, 0x01);
    let meta = meta_of(&state);
    let mut hooks = NoVm::default();

    for i in 1..=11u32 {
        assert_eq!(
            batch_set_insert(false, &mut state, &meta, 0, &[i], None, &mut hooks),
            ErrorCode::Ok
        );
    }
    assert_eq!(meta.size(&state), 11);
    assert_eq!(
        batch_set_insert(false, &mut state, &meta, 0, &[12], None, &mut hooks),
        ErrorCode::CapacityExceeded
    );
}

/// Additional coverage: batch-of-one removal (FOR_EACH body path).
#[test]
fn set_remove_one_at_a_time_works() {
    let mut state = mk_slot_state(16, 0x01);
    let meta = meta_of(&state);
    let mut hooks = NoVm::default();
    let tbl = bind_slot_set(&meta);

    batch_set_insert(false, &mut state, &meta, 0, &[42], None, &mut hooks);
    assert_eq!(tbl.size(&state), 1);
    assert_eq!(
        batch_set_remove(false, &mut state, &meta, 0, &[42], &mut hooks),
        ErrorCode::Ok
    );
    assert_eq!(tbl.size(&state), 0);
    assert!(!tbl.contains(&state, 42));
}

// ---------------------------------------------------------------------------
// The control model retains the prior tombstone kernel. The differential
// checks its membership/admission semantics against compacting deletion;
// rehash placement remains a separately tested byte-level contract.
// ---------------------------------------------------------------------------

struct RefModel {
    cells: Vec<u32>, // EMPTY_KEY / TOMBSTONE / live key
    size: u32,
}

impl RefModel {
    fn new(cap: u32) -> Self {
        Self {
            cells: vec![EMPTY_KEY; cap as usize],
            size: 0,
        }
    }

    fn cap(&self) -> u32 {
        self.cells.len() as u32
    }

    fn max_load(&self) -> u32 {
        self.cap() * 7 / 10
    }

    fn find_insert(&self, key: u32) -> Option<(u32, bool)> {
        if key == EMPTY_KEY || key == TOMBSTONE {
            return None;
        }
        let cap = self.cap();
        let mut pos = hash_key(key, cap);
        let mut first_tomb = None;
        for _ in 0..cap {
            let k = self.cells[pos as usize];
            if k == key {
                return Some((pos, true));
            }
            if k == EMPTY_KEY {
                return Some((first_tomb.unwrap_or(pos), false));
            }
            if k == TOMBSTONE && first_tomb.is_none() {
                first_tomb = Some(pos);
            }
            pos = (pos + 1) & (cap - 1);
        }
        first_tomb.map(|t| (t, false))
    }

    fn insert(&mut self, key: u32) {
        if let Some((pos, found)) = self.find_insert(key)
            && !found
            && self.size < self.max_load()
        {
            self.cells[pos as usize] = key;
            self.size += 1;
        }
    }

    fn remove(&mut self, key: u32) {
        if key == EMPTY_KEY || key == TOMBSTONE {
            return;
        }
        let cap = self.cap();
        let mut pos = hash_key(key, cap);
        for _ in 0..cap {
            let k = self.cells[pos as usize];
            if k == key {
                self.cells[pos as usize] = TOMBSTONE;
                self.size -= 1;
                return;
            }
            if k == EMPTY_KEY {
                return;
            }
            pos = (pos + 1) & (cap - 1);
        }
    }

    fn live_seq(&self) -> Vec<(u32, u32)> {
        self.cells
            .iter()
            .enumerate()
            .filter(|&(_, &k)| k != EMPTY_KEY && k != TOMBSTONE)
            .map(|(i, &k)| (i as u32, k))
            .collect()
    }
}

#[derive(Clone, Debug)]
enum Op {
    Insert(u32),
    Remove(u32),
}

fn op_strategy() -> impl proptest::strategy::Strategy<Value = Op> {
    // Small key range forces collisions, tombstone reuse, and probe chains.
    let keys: std::ops::Range<u32> = 1..40;
    prop_oneof![keys.clone().prop_map(Op::Insert), keys.prop_map(Op::Remove),]
}

#[derive(Clone, Copy, Debug)]
enum TtlOp {
    Upsert {
        key: u32,
        value: u32,
        timestamp_bits: u64,
        first: bool,
    },
    Remove(u32),
}

fn ttl_op_strategy() -> impl proptest::strategy::Strategy<Value = TtlOp> {
    prop_oneof![
        (1u32..24, any::<u32>(), any::<u64>(), any::<bool>()).prop_map(
            |(key, value, timestamp_bits, first)| TtlOp::Upsert {
                key,
                value,
                timestamp_bits,
                first,
            }
        ),
        (1u32..24).prop_map(TtlOp::Remove),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Public singleton First/Last encodings agree with an independent model.
    /// Actual FOR_EACH scalar dispatch has its own image oracle below.
    #[test]
    fn public_map_first_last_match_independent_model_and_restore_images(
        ops in proptest::collection::vec(ttl_op_strategy(), 1..100)
    ) {
        for delta in [false, true] {
            let mut state = mk_ttl_slot_state(32, 0x10);
            cmp_lane_fill(&mut state, 32, f64::NEG_INFINITY.to_bits());
            let meta = meta_of(&state);
            let table = bind_slot_map(&meta);
            let before = state.clone();
            let mut vm = Vm::default();
            vm.undo_enable(&state).expect("begin valid speculation window");
            let mut model = std::collections::BTreeMap::<u32, (u32, u64)>::new();
            for &op in &ops {
                let previous = state.clone();
                let checkpoint = vm.undo_checkpoint();
                let (result, expected) = match op {
                    TtlOp::Upsert { key, value, timestamp_bits, first } => {
                        let exists = model.contains_key(&key);
                        let expected = if !exists && model.len() >= 22 {
                            ErrorCode::CapacityExceeded // cap32 * 7 / 10
                        } else {
                            if !first || !exists {
                                model.insert(key, (value, timestamp_bits));
                            }
                            ErrorCode::Ok
                        };
                        let bytes = timestamp_bits.to_le_bytes();
                        let result = vm.map_upsert(
                            if first { Strategy::First } else { Strategy::Last },
                            delta, &mut state, 0, &[key], &[value], Some(&bytes), CmpType::F64,
                        );
                        (result, expected)
                    }
                    TtlOp::Remove(key) => {
                        model.remove(&key);
                        (vm.map_remove(delta, &mut state, 0, &[key]), ErrorCode::Ok)
                    }
                };
                prop_assert_eq!(result, expected);
                if result != ErrorCode::Ok {
                    prop_assert_eq!(&state, &previous, "refusing primitive writes no bytes");
                    prop_assert_eq!(vm.undo_checkpoint(), checkpoint);
                }
                prop_assert_eq!(usize::try_from(table.size(&state)).unwrap(), model.len());
                prop_assert_eq!(usize::try_from(meta.eviction_index_size(&state)).unwrap(), model.len());
                for (position, key) in table.iter_live(&state) {
                    let &(value, timestamp_bits) = model.get(&key).expect("independent live key");
                    prop_assert_eq!(table.entry_u32_at(&state, position), value);
                    prop_assert_eq!(
                        bytes::read_u64(&state, columine_vm::hashmap_ops::cmp_lane_off(&meta) + position * 8),
                        timestamp_bits,
                    );
                }
            }
            if delta && vm.undo_checkpoint() != 0 {
                assert_exported_images(&mut vm, 0, &before, &state);
            }
            vm.undo_rollback(&mut state, 0).expect("independent model checkpoint");
            prop_assert_eq!(&state, &before, "every emitted byte returns to the original image");
        }
    }
}

proptest! {
    /// Differential against the pre-compaction tombstone kernel. Logical
    /// membership and admission must agree even though physical cells move.
    #[test]
    fn set_ops_match_reference_model(ops in proptest::collection::vec(op_strategy(), 0..200)) {
        let cap = 16u32;
        let mut state = mk_slot_state(cap, 0x01); // HASHSET
        let meta = meta_of(&state);
        let mut hooks = NoVm::default();
        let mut model = RefModel::new(cap);

        for op in &ops {
            match *op {
                Op::Insert(k) => {
                    let _ = batch_set_insert(false, &mut state, &meta, 0, &[k], None, &mut hooks);
                    model.insert(k);
                }
                Op::Remove(k) => {
                    prop_assert_eq!(
                        batch_set_remove(false, &mut state, &meta, 0, &[k], &mut hooks),
                        ErrorCode::Ok,
                    );
                    model.remove(k);
                }
            }
        }

        let tbl = bind_slot_set(&meta);
        prop_assert_eq!(tbl.size(&state), model.size);
        let mut actual: Vec<u32> = tbl.iter_live(&state).map(|(_, key)| key).collect();
        let mut expected: Vec<u32> = model.live_seq().into_iter().map(|(_, key)| key).collect();
        actual.sort_unstable();
        expected.sort_unstable();
        prop_assert_eq!(actual, expected);
    }

    /// Map contents differential vs std::HashMap under Last strategy.
    #[test]
    fn map_last_matches_std_hashmap(ops in proptest::collection::vec(op_strategy(), 0..200)) {
        let cap = 32u32;
        let mut state = mk_slot_state(cap, 0x40); // HASHMAP, no timestamps
        let meta = meta_of(&state);
        let mut hooks = NoVm::default();
        let mut oracle: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();

        for (n, op) in ops.iter().enumerate() {
            match *op {
                Op::Insert(k) => {
                    let v = n as u32 * 7 + 1;
                    let before = meta.size(&state);
                    let r = batch_map_upsert(
                        Strategy::Last, false, &mut state, &meta, 0,
                        &[k], &[v], None, CmpType::F64, &mut hooks,
                    );
                    // Oracle updates only when the table accepted the write:
                    // overwrite always lands; a NEW key lands unless refused
                    // by the load factor (CapacityExceeded).
                    if r == ErrorCode::Ok
                        && (oracle.contains_key(&k) || meta.size(&state) > before)
                    {
                        oracle.insert(k, v);
                    }
                }
                Op::Remove(k) => {
                    prop_assert_eq!(
                        batch_map_remove(false, &mut state, &meta, 0, &[k], &mut hooks),
                        ErrorCode::Ok,
                    );
                    oracle.remove(&k);
                }
            }
        }

        let tbl = bind_slot_map(&meta);
        prop_assert_eq!(tbl.size(&state), oracle.len() as u32);
        for (&k, &v) in &oracle {
            prop_assert_eq!(tbl.get_u32(&state, k), Some(v));
        }
    }

    /// Rehash preserves the full content set and places every key by the
    /// plain (tombstone-free) probe in ascending-source-slot order.
    #[test]
    fn rehash_matches_reference_placement(keys in proptest::collection::btree_set(1u32..1000, 0..20)) {
        let mut buf = vec![0u8; 8192];
        let src = FlatTable::init(&mut buf, 0, 32, ENTRY_U32);
        let mut live: Vec<(u32, u32)> = Vec::new();
        for &k in &keys {
            let _ = src.upsert_u32(&mut buf, k, k * 3);
        }
        for (_, k) in src.iter_live(&buf) {
            live.push((k, src.get_u32(&buf, k).unwrap()));
        }

        let new_cap = 64u32;
        let dst = src.rehash_into(&mut buf, 4096, new_cap);

        // Reference placement: iterate source slots ascending, plain probe.
        let mut ref_cells = vec![EMPTY_KEY; new_cap as usize];
        for &(k, _) in &live {
            let mut pos = hash_key(k, new_cap);
            while ref_cells[pos as usize] != EMPTY_KEY {
                pos = (pos + 1) & (new_cap - 1);
            }
            ref_cells[pos as usize] = k;
        }

        prop_assert_eq!(dst.size(&buf), live.len() as u32);
        for pos in 0..new_cap {
            prop_assert_eq!(dst.key_at(&buf, pos), ref_cells[pos as usize]);
        }
        for (k, v) in live {
            prop_assert_eq!(dst.get_u32(&buf, k), Some(v));
        }
    }
}

#[test]
fn wrapped_cluster_deletion_preserves_comparison_values() {
    let mut state = mk_slot_state(16, 0x00);
    let meta = meta_of(&state);
    let table = bind_slot_map(&meta);
    let mut hooks = NoVm::default();
    let timestamps: Vec<u8> = [10u64, 20, 30]
        .into_iter()
        .flat_map(u64::to_le_bytes)
        .collect();
    assert_eq!(
        batch_map_upsert(
            Strategy::Latest,
            false,
            &mut state,
            &meta,
            0,
            &[3, 18, 24],
            &[30, 180, 240],
            Some(&timestamps),
            CmpType::I64,
            &mut hooks,
        ),
        ErrorCode::Ok,
    );
    // These keys share home cell 15, so removing 3 moves 18 across the wrap.
    assert_eq!(
        batch_map_remove(false, &mut state, &meta, 0, &[3], &mut hooks),
        ErrorCode::Ok
    );
    assert_eq!(
        single_map_upsert(
            Strategy::Latest,
            false,
            &mut state,
            &meta,
            0,
            18,
            999,
            19,
            CmpType::I64,
            &mut hooks,
        ),
        ErrorCode::Ok,
    );
    assert_eq!(table.get_u32(&state, 18), Some(180));
    assert_eq!(
        batch_map_remove(false, &mut state, &meta, 0, &[24], &mut hooks),
        ErrorCode::Ok,
    );
    assert_eq!(
        single_map_upsert(
            Strategy::Latest,
            false,
            &mut state,
            &meta,
            0,
            18,
            181,
            21,
            CmpType::I64,
            &mut hooks,
        ),
        ErrorCode::Ok,
    );
    assert_eq!(table.get_u32(&state, 18), Some(181));
    assert_eq!(table.get_u32(&state, 3), None);
    assert_eq!(table.get_u32(&state, 24), None);
    assert_eq!(table.size(&state), 1);
}

fn assert_exported_images(vm: &mut Vm, checkpoint: u32, before: &[u8], after: &[u8]) {
    let end = vm.undo_checkpoint();
    assert!(end > checkpoint, "a real mutation must journal");
    assert_eq!(
        vm.delta_export_segment(checkpoint, end),
        Ok(end - checkpoint)
    );
    let undo = vm.delta_export_undo_bytes();
    let redo = vm.delta_export_redo_bytes();
    let mut replay = Vm::default();
    let mut undone = after.to_vec();
    replay
        .delta_apply_rollback_segment(&mut undone, &undo, Vm::delta_export_entry_size())
        .expect("actual exported undo");
    assert_eq!(undone, before, "undo must restore every emitted byte");
    let mut redone = before.to_vec();
    replay
        .delta_apply_rollforward_segment(&mut redone, &redo, Vm::delta_export_entry_size())
        .expect("actual exported redo");
    assert_eq!(redone, after, "redo must reproduce every emitted byte");
}

#[allow(clippy::too_many_arguments)]
fn journal_map_upsert(
    vm: &mut Vm,
    state: &mut [u8],
    strategy: Strategy,
    keys: &[u32],
    values: &[u32],
    cmp: Option<&[u8]>,
    cmp_type: CmpType,
    delta: bool,
) -> ErrorCode {
    vm.map_upsert(strategy, delta, state, 0, keys, values, cmp, cmp_type)
}

fn journal_map_remove(vm: &mut Vm, state: &mut [u8], keys: &[u32], delta: bool) -> ErrorCode {
    vm.map_remove(delta, state, 0, keys)
}

#[test]
fn retained_erase_plan_matches_actual_wrapped_table_geometry() {
    for entry_size in [ENTRY_NONE, ENTRY_U32] {
        let mut state = vec![0xa5; 512];
        let table = FlatTable::init(&mut state, 0, 16, entry_size);
        for (key, value) in [(3, 30), (18, 180), (24, 240)] {
            if entry_size == ENTRY_NONE {
                assert_eq!(table.insert_key(&mut state, key), Some(true));
            } else {
                assert_eq!(table.upsert_u32(&mut state, key, value), Some(true));
            }
        }
        let pos = table.find(&state, 3).expect("collision head");
        assert_eq!(pos, 15);
        let plan = table.plan_erase_at(&state, pos);
        let mut runs = Vec::new();
        plan.for_each_destination_run(|start, len| runs.push((start, len)));
        assert_eq!(runs, [(15, 1), (0, 2)], "wrap splits touched cells");
        let mut expected = state.clone();
        for (position, key, value) in [(15, 18, 180), (0, 24, 240), (1, EMPTY_KEY, 0)] {
            table.set_key_at(&mut expected, position, key);
            if entry_size == ENTRY_U32 {
                table.set_entry_u32_at(&mut expected, position, value);
            }
        }
        assert_eq!(plan.tail(), 1, "fixed wrapped vacancy");
        assert_eq!(table.erase_planned(&mut state, &plan, |_, _, _| {}), 1);
        assert_eq!(
            state, expected,
            "fixed key/value/tail image preserves the table ABI"
        );
    }
}

#[test]
fn map_tombstone_insert_and_unused_allocated_aux_restore_exact_images() {
    for delta in [false, true] {
        let mut state = mk_slot_state(16, 0x00);
        let meta = meta_of(&state);
        let table = bind_slot_map(&meta);
        for pos in 0..table.cap {
            table.set_entry_u32_at(&mut state, pos, 0xa500_0000 + pos);
        }
        cmp_lane_fill(&mut state, 16, f64::NEG_INFINITY.to_bits());
        table.set_key_at(&mut state, 15, TOMBSTONE);
        meta.set_change_flag(&mut state, ChangeFlag::REMOVED);
        let before = state.clone();
        let mut vm = Vm::default();
        vm.undo_enable(&state)
            .expect("begin valid speculation window");
        assert_eq!(
            journal_map_upsert(
                &mut vm,
                &mut state,
                Strategy::Last,
                &[3, 18, 24],
                &[30, 180, 240],
                None,
                CmpType::F64,
                delta,
            ),
            ErrorCode::Ok,
        );
        assert_eq!(table.find(&state, 3), Some(15));
        for pos in 0..16 {
            assert_eq!(
                bytes::read_u64(&state, SLOT_OFFSET + 16 * 8 + pos * 8),
                f64::NEG_INFINITY.to_bits(),
                "LAST never rewrites its allocated unused lane",
            );
        }
        let inserted = state.clone();
        let inner = vm.undo_checkpoint();
        assert_eq!(
            journal_map_upsert(
                &mut vm,
                &mut state,
                Strategy::Last,
                &[3],
                &[31],
                None,
                CmpType::F64,
                delta,
            ),
            ErrorCode::Ok,
        );
        if delta {
            assert_exported_images(&mut vm, inner, &inserted, &state);
        }
        vm.undo_rollback(&mut state, inner)
            .expect("nested update checkpoint");
        assert_eq!(state, inserted);
        if delta {
            assert_exported_images(&mut vm, 0, &before, &state);
        }
        vm.undo_rollback(&mut state, 0)
            .expect("insertion checkpoint");
        assert_eq!(
            state, before,
            "tombstone, dead value, aux and flags all restore"
        );
    }
}

#[test]
fn wrapped_map_delete_restores_shifted_lanes_and_tail_at_nested_savepoints() {
    for delta in [false, true] {
        let mut state = mk_slot_state(16, 0x00);
        let meta = meta_of(&state);
        let timestamps: Vec<u8> = [10u64, 20, 30]
            .into_iter()
            .flat_map(u64::to_le_bytes)
            .collect();
        let mut vm = Vm::default();
        assert_eq!(
            journal_map_upsert(
                &mut vm,
                &mut state,
                Strategy::Latest,
                &[3, 18, 24],
                &[30, 180, 240],
                Some(&timestamps),
                CmpType::I64,
                delta,
            ),
            ErrorCode::Ok,
        );
        meta.set_change_flag(&mut state, ChangeFlag::UPDATED);
        let before = state.clone();
        vm.undo_enable(&state)
            .expect("begin valid speculation window");
        assert_eq!(
            journal_map_remove(&mut vm, &mut state, &[3], delta),
            ErrorCode::Ok
        );
        let prefix = state.clone();
        let inner = vm.undo_checkpoint();
        assert_eq!(
            journal_map_remove(&mut vm, &mut state, &[24], delta),
            ErrorCode::Ok
        );
        if delta {
            assert_exported_images(&mut vm, inner, &prefix, &state);
        }
        vm.undo_rollback(&mut state, inner)
            .expect("nested erase checkpoint");
        assert_eq!(
            state, prefix,
            "cleared tail restores at the inner savepoint"
        );
        if delta {
            assert_exported_images(&mut vm, 0, &before, &state);
        }
        vm.undo_rollback(&mut state, 0)
            .expect("outer erase checkpoint");
        assert_eq!(state, before, "wrapped keys, values, aux and flags restore");
    }
}

#[test]
fn set_collision_wrap_insert_delete_restores_tombstones_and_flags() {
    for delta in [false, true] {
        let mut state = mk_slot_state(16, 0x01);
        let meta = meta_of(&state);
        let table = bind_slot_set(&meta);
        table.set_key_at(&mut state, 15, TOMBSTONE);
        meta.set_change_flag(&mut state, ChangeFlag::UPDATED);
        let before = state.clone();
        let mut vm = Vm::default();
        vm.undo_enable(&state)
            .expect("begin valid speculation window");
        assert_eq!(
            vm.set_insert(delta, &mut state, 0, &[3, 18, 24], None),
            ErrorCode::Ok,
        );
        let inserted = state.clone();
        let inner = vm.undo_checkpoint();
        assert_eq!(vm.set_remove(delta, &mut state, 0, &[3, 24]), ErrorCode::Ok);
        assert!(table.contains(&state, 18));
        assert_eq!(table.size(&state), 1);
        if delta {
            assert_exported_images(&mut vm, inner, &inserted, &state);
        }
        vm.undo_rollback(&mut state, inner)
            .expect("nested set erase checkpoint");
        assert_eq!(state, inserted);
        if delta {
            assert_exported_images(&mut vm, 0, &before, &state);
        }
        vm.undo_rollback(&mut state, 0)
            .expect("set insertion checkpoint");
        assert_eq!(
            state, before,
            "the original tombstone and flags must survive"
        );
    }
}

#[test]
fn map_and_set_ttl_refresh_remove_restore_live_index_and_dead_destinations() {
    for delta in [false, true] {
        for is_map in [false, true] {
            let mut state = mk_ttl_slot_state(16, if is_map { 0x10 } else { 0x11 });
            let meta = meta_of(&state);
            let index = meta.eviction_index_offset(&state) as usize;
            state[index..index + 16 * 16].fill(0xa5);
            if is_map {
                cmp_lane_fill(&mut state, 16, f64::NEG_INFINITY.to_bits());
            }
            let mut vm = Vm::default();
            let times = [10.0f64, 20.0, 30.0];
            let time_bytes: Vec<u8> = times.into_iter().flat_map(f64::to_le_bytes).collect();
            let seed = if is_map {
                journal_map_upsert(
                    &mut vm,
                    &mut state,
                    Strategy::Last,
                    &[3, 18, 24],
                    &[30, 180, 240],
                    Some(&time_bytes),
                    CmpType::F64,
                    delta,
                )
            } else {
                vm.set_insert(delta, &mut state, 0, &[3, 18, 24], Some(&times))
            };
            assert_eq!(seed, ErrorCode::Ok);
            assert_eq!(meta.eviction_index_size(&state), 3);
            assert_eq!(&state[index + 3 * 16..index + 4 * 16], &[0xa5; 16]);
            let before = state.clone();
            vm.undo_enable(&state)
                .expect("begin valid speculation window");
            let refreshed = if is_map {
                journal_map_upsert(
                    &mut vm,
                    &mut state,
                    Strategy::Last,
                    &[3, 50],
                    &[31, 500],
                    Some(&[40.0f64.to_le_bytes(), 15.0f64.to_le_bytes()].concat()),
                    CmpType::F64,
                    delta,
                )
            } else {
                vm.set_insert(delta, &mut state, 0, &[3, 50], Some(&[40.0, 15.0]))
            };
            assert_eq!(refreshed, ErrorCode::Ok);
            assert_eq!(meta.eviction_index_size(&state), 4);
            let prefix = state.clone();
            let inner = vm.undo_checkpoint();
            let removed = if is_map {
                journal_map_remove(&mut vm, &mut state, &[18, 24], delta)
            } else {
                vm.set_remove(delta, &mut state, 0, &[18, 24])
            };
            assert_eq!(removed, ErrorCode::Ok);
            assert_eq!(meta.eviction_index_size(&state), 2);
            if delta {
                assert_exported_images(&mut vm, inner, &prefix, &state);
            }
            vm.undo_rollback(&mut state, inner)
                .expect("TTL remove checkpoint");
            assert_eq!(
                state, prefix,
                "TTL length, shifted live entries and dead tail restore"
            );
            if delta {
                assert_exported_images(&mut vm, 0, &before, &state);
            }
            vm.undo_rollback(&mut state, 0)
                .expect("TTL refresh/insert checkpoint");
            assert_eq!(
                state, before,
                "formerly dead insertion destination restores exactly"
            );
        }
    }
}

#[test]
fn map_overflow_mid_batch_refuses_current_operation_and_preserves_exact_prefix() {
    for delta in [false, true] {
        let mut state = mk_slot_state(16, 0x00);
        let meta = meta_of(&state);
        cmp_lane_fill(&mut state, 16, f64::NEG_INFINITY.to_bits());
        let mut vm = Vm::default();
        assert_eq!(
            journal_map_upsert(
                &mut vm,
                &mut state,
                Strategy::Last,
                &[3],
                &[0],
                None,
                CmpType::U32,
                delta,
            ),
            ErrorCode::Ok,
        );
        let before = state.clone();
        vm.undo_enable(&state)
            .expect("begin valid speculation window");
        let keys = vec![3; UNDO_CAPACITY as usize + 10];
        let values: Vec<u32> = (1..=keys.len() as u32).collect();
        assert_eq!(
            journal_map_upsert(
                &mut vm,
                &mut state,
                Strategy::Last,
                &keys,
                &values,
                None,
                CmpType::U32,
                delta,
            ),
            ErrorCode::JournalOverflow,
        );
        let accepted = bind_slot_map(&meta)
            .get_u32(&state, 3)
            .expect("seed remains") as usize;
        assert!(
            accepted > 0 && accepted < keys.len(),
            "the overflow must occur mid-batch"
        );
        assert!(vm.undo_checkpoint() <= UNDO_CAPACITY);
        let mut expected = before.clone();
        assert_eq!(
            batch_map_upsert(
                Strategy::Last,
                false,
                &mut expected,
                &meta,
                0,
                &keys[..accepted],
                &values[..accepted],
                None,
                CmpType::U32,
                &mut NoVm::default(),
            ),
            ErrorCode::Ok,
        );
        assert_eq!(
            state, expected,
            "the first refusing update performs no write"
        );
        let prefix = state.clone();
        let checkpoint = vm.undo_checkpoint();
        assert_eq!(
            journal_map_upsert(
                &mut vm,
                &mut state,
                Strategy::Last,
                &[3],
                &[u32::MAX],
                None,
                CmpType::U32,
                delta,
            ),
            ErrorCode::JournalOverflow,
        );
        assert_eq!(
            state, prefix,
            "overflow remains a refusal until the window ends"
        );
        assert_eq!(vm.undo_checkpoint(), checkpoint);
        if delta {
            assert_exported_images(&mut vm, 0, &before, &state);
        }
        vm.undo_rollback(&mut state, 0)
            .expect("overflowed window still owns its valid prefix");
        assert_eq!(
            state, before,
            "valid-prefix rollback includes allocated aux and flags"
        );
    }
}

#[test]
fn set_overflow_mid_batch_preserves_only_the_accepted_physical_prefix() {
    for delta in [false, true] {
        let mut state = mk_slot_state(32768, 0x01);
        let meta = meta_of(&state);
        let before = state.clone();
        let mut vm = Vm::default();
        vm.undo_enable(&state)
            .expect("begin valid speculation window");
        let keys: Vec<u32> = (1..=UNDO_CAPACITY + 10).collect();
        assert_eq!(
            vm.set_insert(delta, &mut state, 0, &keys, None),
            ErrorCode::JournalOverflow,
        );
        let accepted = meta.size(&state) as usize;
        assert!(accepted > 0 && accepted < keys.len());
        assert!(vm.undo_checkpoint() <= UNDO_CAPACITY);
        let mut expected = before.clone();
        assert_eq!(
            batch_set_insert(
                false,
                &mut expected,
                &meta,
                0,
                &keys[..accepted],
                None,
                &mut NoVm::default()
            ),
            ErrorCode::Ok,
        );
        assert_eq!(
            state, expected,
            "the refusing insertion never touches its EMPTY cell"
        );
        let prefix = state.clone();
        let checkpoint = vm.undo_checkpoint();
        assert_eq!(
            vm.set_remove(delta, &mut state, 0, &[1]),
            ErrorCode::JournalOverflow,
        );
        assert_eq!(state, prefix, "no erase runs after journal refusal");
        assert_eq!(vm.undo_checkpoint(), checkpoint);
        if delta {
            assert_exported_images(&mut vm, 0, &before, &state);
        }
        vm.undo_rollback(&mut state, 0)
            .expect("set valid-prefix checkpoint");
        assert_eq!(
            state, before,
            "all EMPTY cells and metadata restore byte-exact"
        );
    }
}

#[test]
fn map_first_and_comparison_noops_preserve_image_and_checkpoint() {
    for strategy in [
        Strategy::First,
        Strategy::Latest,
        Strategy::Max,
        Strategy::Min,
    ] {
        let mut state = mk_slot_state(16, 0x00);
        let mut vm = Vm::default();
        assert_eq!(
            journal_map_upsert(
                &mut vm,
                &mut state,
                Strategy::Latest,
                &[7],
                &[70],
                Some(&20i64.to_le_bytes()),
                CmpType::I64,
                true,
            ),
            ErrorCode::Ok,
        );
        let before = state.clone();
        vm.undo_enable(&state)
            .expect("begin valid speculation window");
        let ignored_cmp = if strategy == Strategy::Min {
            21i64
        } else {
            19i64
        };
        assert_eq!(
            journal_map_upsert(
                &mut vm,
                &mut state,
                strategy,
                &[7, EMPTY_KEY, TOMBSTONE],
                &[71, 99, 99],
                Some(
                    &[
                        ignored_cmp.to_le_bytes(),
                        0i64.to_le_bytes(),
                        0i64.to_le_bytes()
                    ]
                    .concat()
                ),
                CmpType::I64,
                true,
            ),
            ErrorCode::Ok,
        );
        assert_eq!(state, before);
        assert_eq!(
            vm.undo_checkpoint(),
            0,
            "no-op/sentinel paths do not journal"
        );
    }
}

#[test]
fn public_vm_mutations_own_change_flags_once_without_raw_context_access() {
    for delta in [false, true] {
        for is_map in [false, true] {
            let mut state = mk_slot_state(16, if is_map { 0x00 } else { 0x01 });
            let meta = meta_of(&state);
            if is_map {
                cmp_lane_fill(&mut state, 16, f64::NEG_INFINITY.to_bits());
            }
            meta.set_change_flag(&mut state, ChangeFlag::REMOVED);
            let before = state.clone();
            let mut vm = Vm::default();
            vm.undo_enable(&state)
                .expect("begin valid speculation window");
            let result = if is_map {
                vm.map_upsert(
                    Strategy::Last,
                    delta,
                    &mut state,
                    0,
                    &[3],
                    &[30],
                    None,
                    CmpType::U32,
                )
            } else {
                vm.set_insert(delta, &mut state, 0, &[3], None)
            };
            assert_eq!(result, ErrorCode::Ok);
            assert_eq!(
                vm.undo_checkpoint(),
                if is_map { 4 } else { 3 },
                "one physical flag owner"
            );
            assert_eq!(
                meta.change_flags(&state),
                ChangeFlag::REMOVED | ChangeFlag::INSERTED
            );
            if delta {
                assert_exported_images(&mut vm, 0, &before, &state);
            }
            vm.undo_rollback(&mut state, 0)
                .expect("public operation checkpoint");
            assert_eq!(
                state, before,
                "public operation flag ownership restores exact prior bytes"
            );
        }
    }
}

#[test]
fn ttl_capacity_error_keeps_its_cause_and_the_valid_journal_rolls_back() {
    for delta in [false, true] {
        for is_map in [false, true] {
            let mut state = mk_ttl_slot_state(16, if is_map { 0x10 } else { 0x11 });
            let meta = meta_of(&state);
            let mut vm = Vm::default();
            let seed = if is_map {
                vm.map_upsert(
                    Strategy::Last,
                    false,
                    &mut state,
                    0,
                    &[3, 18],
                    &[30, 180],
                    Some(&[10.0f64.to_le_bytes(), 20.0f64.to_le_bytes()].concat()),
                    CmpType::F64,
                )
            } else {
                vm.set_insert(false, &mut state, 0, &[3, 18], Some(&[10.0, 20.0]))
            };
            assert_eq!(seed, ErrorCode::Ok);
            fill_retained_ttl_index(&mut state, &meta, 3, 18);
            columine_vm::state_init::bind_slot(&state, 0).expect("valid full retained index");
            let before = state.clone();
            vm.undo_enable(&state)
                .expect("begin valid speculation window");
            let result = if is_map {
                journal_map_upsert(
                    &mut vm,
                    &mut state,
                    Strategy::Last,
                    &[3, 24],
                    &[31, 240],
                    Some(&[40.0f64.to_le_bytes(), 50.0f64.to_le_bytes()].concat()),
                    CmpType::F64,
                    delta,
                )
            } else {
                vm.set_insert(delta, &mut state, 0, &[3, 24], Some(&[40.0, 50.0]))
            };
            assert_eq!(
                result,
                ErrorCode::CapacityExceeded,
                "TTL capacity is not journal overflow"
            );
            assert!(!vm.undo_status().overflow);
            assert_eq!(meta.eviction_index_size(&state), 16);
            assert_eq!(
                meta.size(&state),
                2,
                "the TTL-refused second key never enters primary storage"
            );
            if delta {
                assert_exported_images(&mut vm, 0, &before, &state);
            }
            vm.undo_rollback(&mut state, 0)
                .expect("TTL capacity valid-prefix checkpoint");
            assert_eq!(
                state, before,
                "a propagated TTL error cannot lose logged table/index bytes"
            );
        }
    }
}

#[test]
fn full_ttl_index_refuses_new_key_before_any_primary_or_metadata_write() {
    for delta in [false, true] {
        for is_map in [false, true] {
            let mut state = mk_ttl_slot_state(16, if is_map { 0x10 } else { 0x11 });
            let meta = meta_of(&state);
            let mut vm = Vm::default();
            let seed = if is_map {
                journal_map_upsert(
                    &mut vm,
                    &mut state,
                    Strategy::Last,
                    &[3],
                    &[30],
                    Some(&10.0f64.to_le_bytes()),
                    CmpType::F64,
                    delta,
                )
            } else {
                vm.set_insert(delta, &mut state, 0, &[3], Some(&[10.0]))
            };
            assert_eq!(seed, ErrorCode::Ok);
            fill_retained_ttl_index(&mut state, &meta, 3, 3);
            columine_vm::state_init::bind_slot(&state, 0).expect("valid full retained index");
            let before = state.clone();
            vm.undo_enable(&state)
                .expect("begin valid speculation window");
            let result = if is_map {
                journal_map_upsert(
                    &mut vm,
                    &mut state,
                    Strategy::Last,
                    &[18],
                    &[180],
                    Some(&20.0f64.to_le_bytes()),
                    CmpType::F64,
                    delta,
                )
            } else {
                vm.set_insert(delta, &mut state, 0, &[18], Some(&[20.0]))
            };
            assert_eq!(result, ErrorCode::CapacityExceeded);
            assert_eq!(
                state, before,
                "TTL preflight must precede key/value/aux/size/flag writes"
            );
            assert_eq!(
                vm.undo_checkpoint(),
                0,
                "refusal must not append a journal entry"
            );
            assert!(
                !vm.undo_status().overflow,
                "TTL capacity keeps its original error cause"
            );
            vm.undo_rollback(&mut state, 0)
                .expect("unchanged refusal checkpoint");
            assert_eq!(state, before);
        }
    }

    // Each new key's TTL plan is one entry longer than the last. A call whose
    // later plans need more planner memory than the VM holds is refused as a
    // whole, with nothing written and no undo window to fall back on, rather
    // than after the keys whose plans still fit.
    let mut state = mk_ttl_slot_state(16, 0x11);
    let mut vm = Vm::default();
    assert_eq!(
        vm.set_insert(false, &mut state, 0, &[3], Some(&[10.0])),
        ErrorCode::Ok
    );
    let before = state.clone();
    let keys: Vec<u32> = (100..108).collect();
    let times = vec![20.0f64; keys.len()];
    assert_eq!(
        without_memory(|| vm.set_insert(false, &mut state, 0, &keys, Some(&times))),
        ErrorCode::OutOfMemory
    );
    assert_eq!(state, before, "a refused call writes nothing");
    assert_eq!(
        vm.set_insert(false, &mut state, 0, &keys, Some(&times)),
        ErrorCode::Ok
    );
}

#[test]
fn full_ttl_index_same_key_refresh_reuses_its_entry_and_replays_exact_images() {
    for delta in [false, true] {
        for is_map in [false, true] {
            let mut state = mk_ttl_slot_state(16, if is_map { 0x10 } else { 0x11 });
            let meta = meta_of(&state);
            let index = meta.eviction_index_offset(&state) as usize;
            state[index..index + 16 * 16].fill(0xa5);
            let mut vm = Vm::default();
            let seed = if is_map {
                journal_map_upsert(
                    &mut vm,
                    &mut state,
                    Strategy::Last,
                    &[3],
                    &[30],
                    Some(&10.0f64.to_le_bytes()),
                    CmpType::F64,
                    delta,
                )
            } else {
                vm.set_insert(delta, &mut state, 0, &[3], Some(&[10.0]))
            };
            assert_eq!(seed, ErrorCode::Ok);
            fill_retained_ttl_index(&mut state, &meta, 3, 3);
            columine_vm::state_init::bind_slot(&state, 0).expect("valid full retained index");
            let before = state.clone();
            vm.undo_enable(&state)
                .expect("begin valid speculation window");
            let refreshed = if is_map {
                journal_map_upsert(
                    &mut vm,
                    &mut state,
                    Strategy::Last,
                    &[3],
                    &[31],
                    Some(&20.0f64.to_le_bytes()),
                    CmpType::F64,
                    delta,
                )
            } else {
                vm.set_insert(delta, &mut state, 0, &[3], Some(&[20.0]))
            };
            assert_eq!(
                refreshed,
                ErrorCode::Ok,
                "a full index can replace its own entry"
            );
            assert_eq!(meta.size(&state), 1);
            assert_eq!(meta.eviction_index_size(&state), 1);
            assert_eq!(bytes::read_f64(&state, index as u32), 20.0);
            assert_eq!(
                bytes::read_u32(&state, index as u32 + 12),
                if is_map { 31 } else { 3 },
                "the retained TTL plan records the incoming mapped value",
            );
            assert_eq!(
                &state[index + 16..index + 16 * 16],
                &before[index + 16..index + 16 * 16],
                "refresh leaves every formerly live tail byte untouched"
            );
            if delta {
                assert_exported_images(&mut vm, 0, &before, &state);
            }
            vm.undo_rollback(&mut state, 0)
                .expect("full-index refresh checkpoint");
            assert_eq!(
                state, before,
                "full-index refresh restores exact primary/index bytes"
            );
        }
    }
}

#[test]
fn retained_ttl_plan_matches_preserved_lower_bound_bit_golden() {
    const NAN1: u64 = 0x7ff8_0000_0000_0001;
    const NAN2: u64 = 0x7ff8_0000_0000_0002;
    const NEG_NAN: u64 = 0xfff8_0000_0000_0003;
    const NEG_ONE: u64 = 0xbff0_0000_0000_0000;
    const ONE: u64 = 0x3ff0_0000_0000_0000;
    const HALF: u64 = 0x3fe0_0000_0000_0000;
    const NEG_ZERO: u64 = 0x8000_0000_0000_0000;
    // Fixed goldens derived from the preserved HEAD lower-bound primitive:
    // compact the old key, then binary-search with `old_ts < incoming_ts`.
    // Ties and unordered comparisons choose the left branch, not a total sort.
    type TtlStep<'a> = (u32, u64, &'a [(u32, u64)]);
    let steps: &[TtlStep<'_>] = &[
        (1, NAN1, &[(1, NAN1)]),
        (2, NEG_ONE, &[(2, NEG_ONE), (1, NAN1)]),
        (3, ONE, &[(2, NEG_ONE), (3, ONE), (1, NAN1)]),
        (4, NAN2, &[(4, NAN2), (2, NEG_ONE), (3, ONE), (1, NAN1)]),
        (
            5,
            0,
            &[(4, NAN2), (2, NEG_ONE), (5, 0), (3, ONE), (1, NAN1)],
        ),
        (
            6,
            NEG_ZERO,
            &[
                (4, NAN2),
                (2, NEG_ONE),
                (6, NEG_ZERO),
                (5, 0),
                (3, ONE),
                (1, NAN1),
            ],
        ),
        (
            7,
            0,
            &[
                (4, NAN2),
                (2, NEG_ONE),
                (7, 0),
                (6, NEG_ZERO),
                (5, 0),
                (3, ONE),
                (1, NAN1),
            ],
        ),
        (
            1,
            HALF,
            &[
                (4, NAN2),
                (2, NEG_ONE),
                (7, 0),
                (6, NEG_ZERO),
                (5, 0),
                (1, HALF),
                (3, ONE),
            ],
        ),
        (
            3,
            NEG_NAN,
            &[
                (3, NEG_NAN),
                (4, NAN2),
                (2, NEG_ONE),
                (7, 0),
                (6, NEG_ZERO),
                (5, 0),
                (1, HALF),
            ],
        ),
    ];
    for is_map in [false, true] {
        let mut state = mk_ttl_slot_state(16, if is_map { 0x10 } else { 0x11 });
        let meta = meta_of(&state);
        let index = meta.eviction_index_offset(&state);
        state[index as usize..index as usize + 16 * 16].fill(0xa5);
        let before = state.clone();
        let mut vm = Vm::default();
        vm.undo_enable(&state)
            .expect("begin valid speculation window");
        for &(key, bits, expected) in steps {
            let result = if is_map {
                journal_map_upsert(
                    &mut vm,
                    &mut state,
                    Strategy::Last,
                    &[key],
                    &[key * 10],
                    Some(&bits.to_le_bytes()),
                    CmpType::F64,
                    true,
                )
            } else {
                vm.set_insert(true, &mut state, 0, &[key], Some(&[f64::from_bits(bits)]))
            };
            assert_eq!(result, ErrorCode::Ok);
            assert_eq!(meta.eviction_index_size(&state) as usize, expected.len());
            for (position, &(expected_key, expected_bits)) in expected.iter().enumerate() {
                let offset = index + position as u32 * 16;
                assert_eq!(
                    bytes::read_u64(&state, offset),
                    expected_bits,
                    "exact timestamp bits"
                );
                assert_eq!(
                    bytes::read_u32(&state, offset + 8),
                    expected_key,
                    "fixed golden placement"
                );
                assert_eq!(
                    bytes::read_u32(&state, offset + 12),
                    if is_map {
                        expected_key * 10
                    } else {
                        expected_key
                    },
                    "retained mapped value",
                );
            }
            let tail = index as usize + expected.len() * 16;
            assert!(
                state[tail..index as usize + 16 * 16]
                    .iter()
                    .all(|&byte| byte == 0xa5)
            );
        }
        assert_exported_images(&mut vm, 0, &before, &state);
        vm.undo_rollback(&mut state, 0)
            .expect("fixed golden placement checkpoint");
        assert_eq!(
            state, before,
            "NaN/signed-zero bits and dead tails restore exactly"
        );
    }
}

#[test]
fn warm_public_vm_singleton_and_batch_erase_reuse_paid_scratch_without_allocating() {
    for delta in [false, true] {
        for is_map in [false, true] {
            for ttl in [false, true] {
                let flags = (if is_map { 0x00 } else { 0x01 }) | if ttl { 0x10 } else { 0 };
                let mut state = if ttl {
                    mk_ttl_slot_state(16, flags)
                } else {
                    mk_slot_state(16, flags)
                };
                let mut vm = Vm::default();
                let timestamps = [10.0f64, 20.0, 30.0];
                let timestamp_bytes: Vec<u8> =
                    timestamps.into_iter().flat_map(f64::to_le_bytes).collect();
                let seeded = if is_map {
                    vm.map_upsert(
                        Strategy::Last,
                        delta,
                        &mut state,
                        0,
                        &[3, 18, 24],
                        &[30, 180, 240],
                        ttl.then_some(timestamp_bytes.as_slice()),
                        CmpType::F64,
                    )
                } else {
                    vm.set_insert(
                        delta,
                        &mut state,
                        0,
                        &[3, 18, 24],
                        ttl.then_some(timestamps.as_slice()),
                    )
                };
                assert_eq!(seeded, ErrorCode::Ok);
                let before = state.clone();
                vm.undo_enable(&state)
                    .expect("begin valid speculation window");
                let remove = |vm: &mut Vm, state: &mut [u8], batch: bool| {
                    public_remove_fixture(delta, state, is_map, batch, vm)
                };
                // Pay the maximum wrapped cluster/range/capture/TTL high-water
                // once, then keep the same real VM across per-element calls.
                assert_eq!(remove(&mut vm, &mut state, true), ErrorCode::Ok);
                vm.undo_rollback(&mut state, 0)
                    .expect("warm erase checkpoint");
                assert_eq!(state, before);

                // A preflight refusal must return the paid scratch too, not
                // silently drop its capacity and allocate on the next call.
                let shorter = state.len() - 1;
                assert_eq!(
                    remove(&mut vm, &mut state[..shorter], true),
                    ErrorCode::InvalidState,
                );
                assert_eq!(state, before);
                assert_eq!(vm.undo_checkpoint(), 0);
                for batch in [false, true] {
                    for _ in 0..32 {
                        let (result, allocations) =
                            allocations_during(|| remove(&mut vm, &mut state, batch));
                        assert_eq!(result, ErrorCode::Ok);
                        assert_eq!(
                            allocations, 0,
                            "paid public VM erase: map={is_map}, ttl={ttl}, delta={delta}, batch={batch}",
                        );
                        vm.undo_rollback(&mut state, 0)
                            .expect("repeated erase checkpoint");
                        assert_eq!(
                            state, before,
                            "scratch reuse must preserve byte-exact rollback"
                        );
                    }
                }
            }
        }
    }
}

fn no_vm_remove_fixture(
    delta: bool,
    state: &mut [u8],
    meta: &SlotMetaView,
    is_map: bool,
    batch: bool,
    hooks: &mut NoVm,
) -> ErrorCode {
    if is_map && !batch {
        single_map_remove(delta, state, meta, 0, 3, hooks)
    } else {
        let keys: &[u32] = if batch { &[3, 18, 24] } else { &[3] };
        if is_map {
            batch_map_remove(delta, state, meta, 0, keys, hooks)
        } else {
            batch_set_remove(delta, state, meta, 0, keys, hooks)
        }
    }
}

fn public_remove_fixture(
    delta: bool,
    state: &mut [u8],
    is_map: bool,
    batch: bool,
    vm: &mut Vm,
) -> ErrorCode {
    let keys: &[u32] = if batch { &[3, 18, 24] } else { &[3] };
    if is_map {
        vm.map_remove(delta, state, 0, keys)
    } else {
        vm.set_remove(delta, state, 0, keys)
    }
}

#[test]
fn warmed_disabled_vm_and_owned_no_vm_erase_reuse_scratch_and_match_enabled_images() {
    for delta in [false, true] {
        for is_map in [false, true] {
            // NoVm is genuinely non-TTL. Disabled real Vm also covers TTL,
            // whose retained preflight/consumer must remain authoritative.
            for (use_no_vm, ttl) in [(true, false), (false, false), (false, true)] {
                let flags = (if is_map { 0x00 } else { 0x01 }) | if ttl { 0x10 } else { 0 };
                let mut state = if ttl {
                    mk_ttl_slot_state(16, flags)
                } else {
                    mk_slot_state(16, flags)
                };
                let meta = meta_of(&state);
                let mut vm = Vm::default();
                let timestamps = [10.0f64, 20.0, 30.0];
                let timestamp_bytes: Vec<u8> =
                    timestamps.into_iter().flat_map(f64::to_le_bytes).collect();
                let seeded = if is_map {
                    vm.map_upsert(
                        Strategy::Last,
                        delta,
                        &mut state,
                        0,
                        &[3, 18, 24],
                        &[30, 180, 240],
                        ttl.then_some(timestamp_bytes.as_slice()),
                        CmpType::F64,
                    )
                } else {
                    vm.set_insert(
                        delta,
                        &mut state,
                        0,
                        &[3, 18, 24],
                        ttl.then_some(timestamps.as_slice()),
                    )
                };
                assert_eq!(seeded, ErrorCode::Ok);
                let before = state.clone();
                let mut no_vm = NoVm::default();
                // Both disabled environments retain the same paid plan/ranges.
                // Warm their maximum wrapped erase outside the measurement.
                let warmed = if use_no_vm {
                    no_vm_remove_fixture(delta, &mut state, &meta, is_map, true, &mut no_vm)
                } else {
                    public_remove_fixture(delta, &mut state, is_map, true, &mut vm)
                };
                assert_eq!(warmed, ErrorCode::Ok);
                state.copy_from_slice(&before);
                for batch in [false, true] {
                    let mut expected = before.clone();
                    let mut enabled = Vm::default();
                    enabled
                        .undo_enable(&expected)
                        .expect("begin valid comparison window");
                    assert_eq!(
                        public_remove_fixture(delta, &mut expected, is_map, batch, &mut enabled,),
                        ErrorCode::Ok,
                    );
                    for _ in 0..32 {
                        let (result, allocations) = allocations_during(|| {
                            if use_no_vm {
                                no_vm_remove_fixture(
                                    delta, &mut state, &meta, is_map, batch, &mut no_vm,
                                )
                            } else {
                                public_remove_fixture(delta, &mut state, is_map, batch, &mut vm)
                            }
                        });
                        assert_eq!(result, ErrorCode::Ok);
                        assert_eq!(
                            allocations, 0,
                            "disabled erase: NoVm={use_no_vm}, map={is_map}, ttl={ttl}, batch={batch}",
                        );
                        assert_eq!(
                            state, expected,
                            "disabled and enabled retained writers share exact layout semantics"
                        );
                        assert_eq!(
                            vm.undo_checkpoint(),
                            0,
                            "disabled VM does not fabricate a journal"
                        );
                        state.copy_from_slice(&before);
                    }
                }
            }
        }
    }
}

#[test]
fn unified_erase_refuses_truncated_lanes_before_any_write() {
    for is_map in [false, true] {
        let mut state = mk_slot_state(16, if is_map { 0x00 } else { 0x01 });
        let meta = meta_of(&state);
        let mut vm = Vm::default();
        let seeded = if is_map {
            vm.map_upsert(
                Strategy::Last,
                false,
                &mut state,
                0,
                &[3, 18, 24],
                &[30, 180, 240],
                None,
                CmpType::F64,
            )
        } else {
            vm.set_insert(false, &mut state, 0, &[3, 18, 24], None)
        };
        assert_eq!(seeded, ErrorCode::Ok);
        let required_end = (SLOT_OFFSET + 16 * if is_map { 16 } else { 4 }) as usize;
        let before = state[..required_end - 1].to_vec();
        for environment in 0..3 {
            let mut truncated = before.clone();
            let mut actual = Vm::default();
            let mut no_vm = NoVm::default();
            if environment == 2 {
                actual
                    .undo_enable(&truncated)
                    .expect("begin valid bounded-image window");
            }
            let (result, allocations) = allocations_during(|| {
                if environment == 0 {
                    no_vm_remove_fixture(false, &mut truncated, &meta, is_map, true, &mut no_vm)
                } else {
                    public_remove_fixture(false, &mut truncated, is_map, true, &mut actual)
                }
            });
            assert_eq!(
                result,
                ErrorCode::InvalidState,
                "every mode shares checked geometry"
            );
            assert_eq!(
                allocations, 0,
                "typed bounds refusal must not build scratch"
            );
            assert_eq!(
                truncated, before,
                "truncated key/aux extent is refused before erase"
            );
            assert_eq!(
                actual.undo_checkpoint(),
                0,
                "refusal writes no journal in any mode"
            );
        }
    }
}

#[test]
fn checked_flat_table_binding_rejects_bad_geometry_without_allocating() {
    let mut state = [0u8; 64];
    bytes::write_u32(&mut state, 0, 2);
    for (data, cap, size, width) in [
        (4, 0, 0, ENTRY_NONE),
        (4, 3, 0, ENTRY_NONE),
        (u32::MAX - 1, 4, 0, ENTRY_NONE),
        (4, 1 << 30, 0, ENTRY_NONE),
        (4, 4, 0, u32::MAX),
        (4, 4, u32::MAX - 1, ENTRY_NONE),
        (4, 1, 0, ENTRY_NONE),
    ] {
        let (result, allocations) =
            allocations_during(|| FlatTable::checked_bind_external(&state, data, cap, size, width));
        assert!(matches!(result, Err(ErrorCode::InvalidState)));
        assert_eq!(allocations, 0);
    }
    let (result, allocations) =
        allocations_during(|| FlatTable::checked_bind_external(&state, 4, 4, 0, ENTRY_U32));
    let table = result.expect("exact in-bounds geometry");
    assert_eq!(table.keys_off, 4);
    assert_eq!(table.entries_off, 20);
    assert_eq!(table.size(&state), 2);
    assert_eq!(allocations, 0);
}

fn for_each_map_program(body: &[u8]) -> Vec<u8> {
    use columine_types::types::Opcode;
    // Existing dispatch-block wire pattern: one TTL map, columns
    // [event_type, key, value, timestamp], and one real FOR_EACH body.
    let mut init = vec![Opcode::SlotDef as u8, 0, 0x10, 16, 0];
    init.extend(10.0f32.to_le_bytes());
    init.extend(0.0f32.to_le_bytes());
    init.extend([3, 0]); // timestamp column and DurationUnit.NONE
    init.push(Opcode::Halt as u8);
    let mut reduce = vec![Opcode::ForEach as u8, 0, 1];
    reduce.extend(1001u32.to_le_bytes());
    reduce.extend(
        u16::try_from(body.len())
            .expect("small element body")
            .to_le_bytes(),
    );
    reduce.extend_from_slice(body);
    reduce.push(Opcode::Halt as u8);
    let mut program = vec![0; columine_types::types::PROGRAM_HASH_PREFIX as usize];
    program.extend(columine_types::PROGRAM_MAGIC.to_le_bytes());
    program.extend(PROGRAM_FORMAT_VERSION.to_le_bytes());
    program.extend([0, 0]); // callbacks and flags
    program.extend(1u32.to_le_bytes());
    program.extend(4u32.to_le_bytes());
    program.extend(u32::try_from(init.len()).expect("small init").to_le_bytes());
    program.extend(
        u32::try_from(reduce.len())
            .expect("small reduce")
            .to_le_bytes(),
    );
    program.extend(init);
    program.extend(reduce);
    program
}

fn execute_one_map_element(
    vm: &mut Vm,
    state: &mut [u8],
    program: &[u8],
    key: u32,
    value: u32,
    timestamp: u64,
    delta: bool,
) -> ErrorCode {
    let event_type = [1001u32];
    let keys = [key];
    let values = [value];
    let timestamps = [f64::from_bits(timestamp)];
    let columns = [
        columine_vm::vm::u32s_as_bytes(&event_type),
        columine_vm::vm::u32s_as_bytes(&keys),
        columine_vm::vm::u32s_as_bytes(&values),
        columine_vm::vm::f64s_as_bytes(&timestamps),
    ];
    let result = if delta {
        vm.execute_batch_delta(state, program, &columns, 1)
    } else {
        vm.execute_batch(state, program, &columns, 1)
    };
    ErrorCode::from_u32(result).expect("real VM returns a registered status")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn real_for_each_last_ttl_and_remove_match_public_map_images(
        ops in proptest::collection::vec(ttl_op_strategy(), 1..100)
    ) {
        use columine_types::types::Opcode;
        let upsert = for_each_map_program(&[
            Opcode::BatchMapUpsertLastTtl as u8, 0, 1, 2, 3,
        ]);
        let remove = for_each_map_program(&[Opcode::BatchMapRemove as u8, 0, 1]);
        let accepted = columine_vm::state_init::DEFAULT_ACCEPTED_PROGRAM_MAGICS;
        let size = columine_vm::state_init::calculate_state_size(&upsert, accepted)
            .expect("valid program admission");
        prop_assert!(size > 0);
        for delta in [false, true] {
            let mut scalar = vec![0; size as usize];
            columine_vm::state_init::init_state(&mut scalar, &upsert, accepted)
                .expect("real FOR_EACH program initialization");
            let mut public = scalar.clone();
            let before = scalar.clone();
            let mut scalar_vm = Vm::default();
            let mut public_vm = Vm::default();
            scalar_vm.undo_enable(&scalar).expect("begin valid scalar window");
            public_vm.undo_enable(&public).expect("begin valid public window");
            for &op in &ops {
                let (actual, expected) = match op {
                    TtlOp::Upsert { key, value, timestamp_bits, .. } => {
                        // This oracle is specifically LAST_TTL scalar dispatch;
                        // arbitrary FIRST semantics are independently modeled above.
                        let timestamp = timestamp_bits.to_le_bytes();
                        (
                            execute_one_map_element(
                                &mut scalar_vm, &mut scalar, &upsert,
                                key, value, timestamp_bits, delta,
                            ),
                            public_vm.map_upsert(
                                Strategy::Last, delta, &mut public, 0, &[key], &[value],
                                Some(&timestamp), CmpType::F64,
                            ),
                        )
                    }
                    TtlOp::Remove(key) => (
                        execute_one_map_element(&mut scalar_vm, &mut scalar, &remove, key, 0, 0, delta),
                        public_vm.map_remove(delta, &mut public, 0, &[key]),
                    ),
                };
                prop_assert_eq!(actual, expected);
                prop_assert_eq!(&scalar, &public, "real scalar/batch paths restore every byte");
                prop_assert_eq!(scalar_vm.undo_checkpoint(), public_vm.undo_checkpoint());
            }
            if delta && scalar_vm.undo_checkpoint() != 0 {
                assert_exported_images(&mut scalar_vm, 0, &before, &scalar);
                assert_exported_images(&mut public_vm, 0, &before, &public);
                prop_assert_eq!(scalar_vm.delta_export_undo_bytes(), public_vm.delta_export_undo_bytes());
                prop_assert_eq!(scalar_vm.delta_export_redo_bytes(), public_vm.delta_export_redo_bytes());
            }
            scalar_vm.undo_rollback(&mut scalar, 0).expect("scalar checkpoint");
            public_vm.undo_rollback(&mut public, 0).expect("public checkpoint");
            prop_assert_eq!(&scalar, &before);
            prop_assert_eq!(&public, &before);
        }
    }
}

#[test]
fn real_for_each_first_ttl_retains_its_established_zero_timestamp() {
    use columine_types::types::Opcode;
    let program = for_each_map_program(&[Opcode::BatchMapUpsertFirst as u8, 0, 1, 2]);
    let accepted = columine_vm::state_init::DEFAULT_ACCEPTED_PROGRAM_MAGICS;
    let size = columine_vm::state_init::calculate_state_size(&program, accepted)
        .expect("valid program admission");
    assert!(size > 0);
    for delta in [false, true] {
        let mut state = vec![0; size as usize];
        columine_vm::state_init::init_state(&mut state, &program, accepted)
            .expect("real first program");
        let meta = meta_of(&state);
        let before = state.clone();
        let mut vm = Vm::default();
        vm.undo_enable(&state)
            .expect("begin valid speculation window");
        assert_eq!(
            execute_one_map_element(
                &mut vm,
                &mut state,
                &program,
                7,
                70,
                100.0f64.to_bits(),
                delta
            ),
            ErrorCode::Ok,
        );
        let table = bind_slot_map(&meta);
        let position = table.find(&state, 7).expect("first scalar inserted");
        assert_eq!(table.entry_u32_at(&state, position), 70);
        assert_eq!(
            bytes::read_u64(
                &state,
                columine_vm::hashmap_ops::cmp_lane_off(&meta) + position * 8
            ),
            0,
            "the established FIRST scalar opcode supplies zero, not its input timestamp",
        );
        let checkpoint = vm.undo_checkpoint();
        let inserted = state.clone();
        assert_eq!(
            execute_one_map_element(
                &mut vm,
                &mut state,
                &program,
                7,
                71,
                200.0f64.to_bits(),
                delta
            ),
            ErrorCode::Ok,
        );
        assert_eq!(state, inserted);
        assert_eq!(vm.undo_checkpoint(), checkpoint);
        if delta {
            assert_exported_images(&mut vm, 0, &before, &state);
        }
        vm.undo_rollback(&mut state, 0)
            .expect("first scalar checkpoint");
        assert_eq!(state, before);
    }
}

#[test]
fn public_physical_capture_finishes_on_error_return_and_owns_flags_once() {
    for delta in [false, true] {
        let mut state = mk_slot_state(16, 0x00);
        let meta = meta_of(&state);
        let flag = meta.meta_base + SlotMetaOffset::CHANGE_FLAGS;
        let value = meta.offset + meta.capacity * 4;
        let ranges = [(flag, 1), (value, 4)];
        let before = state.clone();
        let mut vm = Vm::default();
        vm.undo_enable(&state)
            .expect("begin valid speculation window");
        let result = vm.capture_mutation(delta, &mut state, &ranges, |state| {
            meta.set_change_flag(state, ChangeFlag::UPDATED);
            bytes::write_u32(state, value, 123);
            Err::<(), _>(ErrorCode::CapacityExceeded)
        });
        assert_eq!(result, Ok(Err(ErrorCode::CapacityExceeded)));
        assert_eq!(
            vm.undo_checkpoint(),
            2,
            "one record per changed range and no duplicate flag"
        );
        if delta {
            assert_exported_images(&mut vm, 0, &before, &state);
        }
        vm.undo_rollback(&mut state, 0)
            .expect("public capture error checkpoint");
        assert_eq!(
            state, before,
            "normal error return cannot abandon before-bytes"
        );
    }
}
