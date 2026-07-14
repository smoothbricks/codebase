//! Translated test blocks from `packages/columine/src/vm/undo_log.zig` (all
//! 6 have counterparts here, same scenarios and expected values) plus
//! rollback round-trip proptests, and behavior tests for `intern.rs`
//! (intern.zig has NO test blocks of its own — the tests in
//! `src/intern.rs` and here are new coverage).
//!
//! The vm_test.zig undo blocks (STRUCT_MAP_* rollback, "ORDERED_LIST - undo
//! restores count") all execute batches through the dispatch loop — they are
//! the dispatch slice's to translate.

use columine_types::types::{
    AggType, EMPTY_KEY, STATE_HEADER_SIZE, SlotMetaOffset, SlotType, TOMBSTONE,
};
use columine_vm::hash_table::{ENTRY_NONE, ENTRY_U32, FlatTable};
use columine_vm::intern::StringIntern;
use columine_vm::meta::SlotMetaView;
use columine_vm::undo_log::{
    FLAT_UNDO_ENTRY_SIZE, FlatUndoEntry, FlatUndoOp, rollback_agg_update, rollback_count_update,
    rollback_map_delete, rollback_map_insert, rollback_map_update, rollback_set_delete,
    rollback_set_insert,
};
use proptest::prelude::*;

// ---------------------------------------------------------------------------
// Scaffolding — mirrors undo_log.zig's test-state construction (undo_log.zig
// :115-127): header, one 48-byte slot-meta record, slot data right after.
// ---------------------------------------------------------------------------

const SLOT_OFFSET: u32 = STATE_HEADER_SIZE + 48; // types.zig SLOT_META_SIZE

fn mk_slot_state(cap: u32, type_flags_byte: u8) -> Vec<u8> {
    let mut state = vec![0u8; 4096];
    let meta = STATE_HEADER_SIZE as usize;
    state[meta..meta + 4].copy_from_slice(&SLOT_OFFSET.to_le_bytes());
    state[meta + 4..meta + 8].copy_from_slice(&cap.to_le_bytes());
    state[meta + SlotMetaOffset::TYPE_FLAGS as usize] = type_flags_byte;
    for j in 0..cap {
        let off = (SLOT_OFFSET + j * 4) as usize;
        state[off..off + 4].copy_from_slice(&EMPTY_KEY.to_le_bytes());
    }
    state
}

fn meta_of(state: &[u8]) -> SlotMetaView {
    SlotMetaView::read(state, 0)
}

fn map_table(meta: &SlotMetaView) -> FlatTable {
    FlatTable::bind_external(
        meta.offset,
        meta.capacity,
        meta.meta_base + SlotMetaOffset::SIZE,
        ENTRY_U32,
    )
}

fn set_table(meta: &SlotMetaView) -> FlatTable {
    FlatTable::bind_external(
        meta.offset,
        meta.capacity,
        meta.meta_base + SlotMetaOffset::SIZE,
        ENTRY_NONE,
    )
}

/// undo_log.zig test flags byte: HASHMAP (0) + no_timestamps (0x40).
const HASHMAP_NO_TS: u8 = 0x40;

// ---------------------------------------------------------------------------
// undo_log.zig test blocks
// ---------------------------------------------------------------------------

/// undo_log.zig:114 "rollbackMapInsert — tombstones key and decrements size"
#[test]
fn rollback_map_insert_tombstones_key_and_decrements_size() {
    let mut state = mk_slot_state(16, HASHMAP_NO_TS);
    let meta = meta_of(&state);
    let tbl = map_table(&meta);

    tbl.upsert_u32(&mut state, 42, 100);
    assert_eq!(tbl.size(&state), 1);

    assert!(rollback_map_insert(&mut state, &meta, 42));
    assert_eq!(tbl.size(&state), 0);
    assert!(!tbl.contains(&state, 42));
}

/// undo_log.zig:139 "rollbackMapUpdate — restores previous value"
#[test]
fn rollback_map_update_restores_previous_value() {
    let mut state = mk_slot_state(16, HASHMAP_NO_TS);
    let meta = meta_of(&state);
    let tbl = map_table(&meta);

    tbl.upsert_u32(&mut state, 42, 100);
    tbl.upsert_u32(&mut state, 42, 200);
    assert_eq!(tbl.get_u32(&state, 42), Some(200));

    assert!(rollback_map_update(&mut state, &meta, 42, 100, 0));
    assert_eq!(tbl.get_u32(&state, 42), Some(100));
}

/// undo_log.zig:164 "rollbackMapDelete — restores key+value and increments size"
#[test]
fn rollback_map_delete_restores_key_value_and_increments_size() {
    let mut state = mk_slot_state(16, HASHMAP_NO_TS);
    let meta = meta_of(&state);
    let tbl = map_table(&meta);

    // Insert then manually tombstone (simulating what the VM does on delete)
    tbl.upsert_u32(&mut state, 42, 100);
    let pos = tbl.find(&state, 42).expect("inserted");
    tbl.set_key_at(&mut state, pos, TOMBSTONE);
    let size = tbl.size(&state);
    tbl.set_size(&mut state, size - 1);
    assert!(!tbl.contains(&state, 42));

    assert!(rollback_map_delete(&mut state, &meta, 42, 100, 0));
    assert!(tbl.contains(&state, 42));
    assert_eq!(tbl.get_u32(&state, 42), Some(100));
    assert_eq!(tbl.size(&state), 1);
}

/// undo_log.zig:195 "rollbackSetInsert — tombstones element"
#[test]
fn rollback_set_insert_tombstones_element() {
    let mut state = mk_slot_state(16, SlotType::HashSet as u8);
    let meta = meta_of(&state);
    let tbl = set_table(&meta);

    tbl.insert_key(&mut state, 42);
    assert!(tbl.contains(&state, 42));

    assert!(rollback_set_insert(&mut state, &meta, 42));
    assert!(!tbl.contains(&state, 42));
    assert_eq!(tbl.size(&state), 0);
}

/// undo_log.zig:220 "rollback — AGG_UPDATE restores previous f64 value and count"
#[test]
fn rollback_agg_update_restores_previous_f64_value_and_count() {
    let mut state = mk_slot_state(0, SlotType::Aggregate as u8);
    let meta = meta_of(&state);
    let off = SLOT_OFFSET as usize;

    // value=10.0 at offset, count=5 at offset+8
    state[off..off + 8].copy_from_slice(&10.0f64.to_bits().to_le_bytes());
    state[off + 8..off + 16].copy_from_slice(&5u64.to_le_bytes());
    let prev_val_bits = 10.0f64.to_bits();
    let prev_count: u32 = 5;

    // Simulate an aggregate update
    state[off..off + 8].copy_from_slice(&25.0f64.to_bits().to_le_bytes());
    state[off + 8..off + 16].copy_from_slice(&8u64.to_le_bytes());
    assert_eq!(
        f64::from_bits(u64::from_le_bytes(state[off..off + 8].try_into().unwrap())),
        25.0
    );

    rollback_agg_update(&mut state, &meta, prev_count, prev_val_bits);

    assert_eq!(
        f64::from_bits(u64::from_le_bytes(state[off..off + 8].try_into().unwrap())),
        10.0
    );
    assert_eq!(
        u64::from_le_bytes(state[off + 8..off + 16].try_into().unwrap()),
        5
    );
}

/// undo_log.zig:263 "rollback — COUNT_UPDATE restores previous u64 count"
#[test]
fn rollback_count_update_restores_previous_u64_count() {
    let mut state = mk_slot_state(0, SlotType::Aggregate as u8);
    let meta_base = STATE_HEADER_SIZE as usize;
    state[meta_base + 13] = AggType::Count as u8;
    let meta = meta_of(&state);
    let off = SLOT_OFFSET as usize;

    state[off..off + 8].copy_from_slice(&42u64.to_le_bytes());
    let prev_count: u32 = 42;

    state[off..off + 8].copy_from_slice(&99u64.to_le_bytes());
    assert_eq!(
        u64::from_le_bytes(state[off..off + 8].try_into().unwrap()),
        99
    );

    rollback_count_update(&mut state, &meta, prev_count);
    assert_eq!(
        u64::from_le_bytes(state[off..off + 8].try_into().unwrap()),
        42
    );
}

/// Zig-parity extra (no Zig counterpart): rollbackSetDelete restores the
/// element — undo_log.zig:98 has no test block for it; same shape as the
/// map-delete test.
#[test]
fn rollback_set_delete_restores_element() {
    let mut state = mk_slot_state(16, SlotType::HashSet as u8);
    let meta = meta_of(&state);
    let tbl = set_table(&meta);

    tbl.insert_key(&mut state, 42);
    let pos = tbl.find(&state, 42).expect("inserted");
    tbl.set_key_at(&mut state, pos, TOMBSTONE);
    let size = tbl.size(&state);
    tbl.set_size(&mut state, size - 1);

    assert!(rollback_set_delete(&mut state, &meta, 42));
    assert!(tbl.contains(&state, 42));
    assert_eq!(tbl.size(&state), 1);
}

/// Zig-parity extras: the false paths (key not found / key already present)
/// that rollbackEntry relies on to skip stale entries.
#[test]
fn rollback_helpers_return_false_on_mismatched_state() {
    let mut state = mk_slot_state(16, HASHMAP_NO_TS);
    let meta = meta_of(&state);
    assert!(!rollback_map_insert(&mut state, &meta, 7)); // not present
    assert!(!rollback_map_update(&mut state, &meta, 7, 0, 0)); // not present

    let tbl = map_table(&meta);
    tbl.upsert_u32(&mut state, 7, 1);
    assert!(!rollback_map_delete(&mut state, &meta, 7, 1, 0)); // already present

    let mut sstate = mk_slot_state(16, SlotType::HashSet as u8);
    let smeta = meta_of(&sstate);
    assert!(!rollback_set_insert(&mut sstate, &smeta, 7));
    let stbl = set_table(&smeta);
    stbl.insert_key(&mut sstate, 7);
    assert!(!rollback_set_delete(&mut sstate, &smeta, 7)); // already present
}

// ---------------------------------------------------------------------------
// Rollback round-trip proptests — the composition vm.zig's rollbackEntry
// performs: journal a mutation sequence, roll it back in reverse, and the
// LOGICAL state is restored. Rollback is not byte-exact at this layer:
// rollbackMapInsert/rollbackSetInsert write TOMBSTONE where the insert found
// EMPTY (undo_log.zig:32/92), and dead cells keep stale value/timestamp
// bytes — only live content is the contract (pinned explicitly below).
// ---------------------------------------------------------------------------

/// Live logical content of a map slot: (key → (value, ts_bits)) + size.
fn map_content(state: &[u8], meta: &SlotMetaView) -> (Vec<(u32, u32, u64)>, u32) {
    let tbl = map_table(meta);
    let mut live: Vec<(u32, u32, u64)> = (1u32..=8)
        .filter_map(|k| {
            tbl.find(state, k).map(|pos| {
                let toff = ts_off(meta, pos);
                let ts_bits = u64::from_le_bytes(state[toff..toff + 8].try_into().unwrap());
                (k, tbl.entry_u32_at(state, pos), ts_bits)
            })
        })
        .collect();
    live.sort_unstable();
    (live, tbl.size(state))
}

fn set_content(state: &[u8], meta: &SlotMetaView) -> (Vec<u32>, u32) {
    let tbl = set_table(meta);
    let live: Vec<u32> = (1u32..=8).filter(|&k| tbl.contains(state, k)).collect();
    (live, tbl.size(state))
}

/// Pins the residue that makes rollback logical-not-byte-exact: rolling back
/// a fresh insert leaves TOMBSTONE (undo_log.zig:32), not EMPTY_KEY.
#[test]
fn rollback_of_insert_leaves_tombstone_not_empty() {
    let mut state = mk_slot_state(16, HASHMAP_NO_TS);
    let meta = meta_of(&state);
    let tbl = map_table(&meta);
    tbl.upsert_u32(&mut state, 42, 100);
    let pos = tbl.find(&state, 42).unwrap();
    assert!(rollback_map_insert(&mut state, &meta, 42));
    assert_eq!(tbl.key_at(&state, pos), TOMBSTONE);
}

/// A journaled map mutation, as vm.zig's map paths would record it.
#[derive(Clone, Debug)]
enum MapOp {
    Upsert { key: u32, value: u32, ts: f64 },
    Delete { key: u32 },
}

fn ts_off(meta: &SlotMetaView, pos: u32) -> usize {
    (meta.offset + meta.capacity * 8 + pos * 8) as usize
}

proptest! {
    /// HASHMAP with timestamp storage: arbitrary upsert/delete interleavings
    /// journal FlatUndoEntry-shaped records; reverse rollback restores the
    /// pre-sequence LOGICAL state (live keys, values, timestamps, size).
    #[test]
    fn map_rollback_round_trips_state_bytes(
        ops in prop::collection::vec(
            prop_oneof![
                (1u32..=8, 0u32..1000, prop::num::f64::NORMAL).prop_map(|(key, value, ts)| MapOp::Upsert { key, value, ts }),
                (1u32..=8).prop_map(|key| MapOp::Delete { key }),
            ],
            0..24,
        ),
    ) {
        let mut state = mk_slot_state(16, SlotType::HashMap as u8); // ts storage present
        let meta = meta_of(&state);
        let tbl = map_table(&meta);

        // Warm the map with a fixed prefix so deletes/updates have targets.
        for k in 1u32..=4 {
            tbl.upsert_u32(&mut state, k, k * 10);
            let pos = tbl.find(&state, k).unwrap();
            let off = ts_off(&meta, pos);
            state[off..off + 8].copy_from_slice(&(f64::from(k)).to_bits().to_le_bytes());
        }

        let before = map_content(&state, &meta);
        let mut journal: Vec<FlatUndoEntry> = Vec::new();

        for op in &ops {
            match *op {
                MapOp::Upsert { key, value, ts } => {
                    let existing = tbl.find(&state, key);
                    match existing {
                        Some(pos) => {
                            let prev_value = tbl.entry_u32_at(&state, pos);
                            let toff = ts_off(&meta, pos);
                            let prev_ts_bits = u64::from_le_bytes(state[toff..toff + 8].try_into().unwrap());
                            tbl.set_entry_u32_at(&mut state, pos, value);
                            state[toff..toff + 8].copy_from_slice(&ts.to_bits().to_le_bytes());
                            journal.push(FlatUndoEntry {
                                op: FlatUndoOp::MapUpdate,
                                slot: 0, pad1: 0, pad2: 0,
                                key, prev_value, aux: prev_ts_bits,
                            });
                        }
                        None => {
                            let probe = tbl.find_insert(&state, key).unwrap();
                            tbl.set_key_at(&mut state, probe.pos, key);
                            tbl.set_entry_u32_at(&mut state, probe.pos, value);
                            let toff = ts_off(&meta, probe.pos);
                            state[toff..toff + 8].copy_from_slice(&ts.to_bits().to_le_bytes());
                            let size = tbl.size(&state);
                            tbl.set_size(&mut state, size + 1);
                            journal.push(FlatUndoEntry {
                                op: FlatUndoOp::MapInsert,
                                slot: 0, pad1: 0, pad2: 0,
                                key, prev_value: 0, aux: 0,
                            });
                        }
                    }
                }
                MapOp::Delete { key } => {
                    if let Some(pos) = tbl.find(&state, key) {
                        let prev_value = tbl.entry_u32_at(&state, pos);
                        let toff = ts_off(&meta, pos);
                        let prev_ts_bits = u64::from_le_bytes(state[toff..toff + 8].try_into().unwrap());
                        tbl.set_key_at(&mut state, pos, TOMBSTONE);
                        let size = tbl.size(&state);
                        tbl.set_size(&mut state, size - 1);
                        journal.push(FlatUndoEntry {
                            op: FlatUndoOp::MapDelete,
                            slot: 0, pad1: 0, pad2: 0,
                            key, prev_value, aux: prev_ts_bits,
                        });
                    }
                }
            }
        }

        // Reverse rollback, dispatching exactly like vm.zig rollbackEntry.
        for entry in journal.iter().rev() {
            let applied = match entry.op {
                FlatUndoOp::MapInsert => rollback_map_insert(&mut state, &meta, entry.key),
                FlatUndoOp::MapUpdate => rollback_map_update(&mut state, &meta, entry.key, entry.prev_value, entry.aux),
                FlatUndoOp::MapDelete => rollback_map_delete(&mut state, &meta, entry.key, entry.prev_value, entry.aux),
                _ => unreachable!(),
            };
            prop_assert!(applied, "journal entry must apply: {entry:?}");
        }

        prop_assert_eq!(map_content(&state, &meta), before);
    }

    /// HASHSET: same round-trip with insert/delete.
    #[test]
    fn set_rollback_round_trips_state_bytes(
        ops in prop::collection::vec((1u32..=8, prop::bool::ANY), 0..24),
    ) {
        let mut state = mk_slot_state(16, SlotType::HashSet as u8);
        let meta = meta_of(&state);
        let tbl = set_table(&meta);
        for k in 1u32..=4 {
            tbl.insert_key(&mut state, k);
        }

        let before = set_content(&state, &meta);
        let mut journal: Vec<FlatUndoEntry> = Vec::new();

        for &(key, insert) in &ops {
            if insert {
                if tbl.find(&state, key).is_none() {
                    let probe = tbl.find_insert(&state, key).unwrap();
                    tbl.set_key_at(&mut state, probe.pos, key);
                    let size = tbl.size(&state);
                    tbl.set_size(&mut state, size + 1);
                    journal.push(FlatUndoEntry {
                        op: FlatUndoOp::SetInsert,
                        slot: 0, pad1: 0, pad2: 0, key, prev_value: 0, aux: 0,
                    });
                }
            } else if let Some(pos) = tbl.find(&state, key) {
                tbl.set_key_at(&mut state, pos, TOMBSTONE);
                let size = tbl.size(&state);
                tbl.set_size(&mut state, size - 1);
                journal.push(FlatUndoEntry {
                    op: FlatUndoOp::SetDelete,
                    slot: 0, pad1: 0, pad2: 0, key, prev_value: 0, aux: 0,
                });
            }
        }

        for entry in journal.iter().rev() {
            let applied = match entry.op {
                FlatUndoOp::SetInsert => rollback_set_insert(&mut state, &meta, entry.key),
                FlatUndoOp::SetDelete => rollback_set_delete(&mut state, &meta, entry.key),
                _ => unreachable!(),
            };
            prop_assert!(applied, "journal entry must apply: {entry:?}");
        }
        prop_assert_eq!(set_content(&state, &meta), before);
    }

    /// The 24-byte wire format round-trips arbitrary entries.
    #[test]
    fn flat_undo_entry_wire_round_trip(
        op_byte in 1u8..=13,
        slot in prop::num::u8::ANY,
        pad1 in prop::num::u8::ANY,
        pad2 in prop::num::u8::ANY,
        key in prop::num::u32::ANY,
        prev_value in prop::num::u32::ANY,
        aux in prop::num::u64::ANY,
    ) {
        let mut probe = [0u8; FLAT_UNDO_ENTRY_SIZE as usize];
        probe[0] = op_byte;
        let op = FlatUndoEntry::read_from(&probe).expect("1..=13 are valid ops").op;
        let entry = FlatUndoEntry { op, slot, pad1, pad2, key, prev_value, aux };
        let mut buf = [0u8; FLAT_UNDO_ENTRY_SIZE as usize];
        entry.write_to(&mut buf);
        prop_assert_eq!(FlatUndoEntry::read_from(&buf), Some(entry));
    }

    /// StringIntern against a first-seen-index model: idx assignment,
    /// dedup, get() round-trip, and the offsets invariant (monotone,
    /// offsets[count] == data_len).
    #[test]
    fn intern_matches_first_seen_model(
        strings in prop::collection::vec(prop::collection::vec(prop::num::u8::ANY, 0..20), 0..32),
    ) {
        let mut si = StringIntern::new(64);
        let mut model: Vec<Vec<u8>> = Vec::new();
        for s in &strings {
            let idx = si.intern(s);
            let expected = match model.iter().position(|m| m == s) {
                Some(i) => i as u32,
                None => {
                    model.push(s.clone());
                    (model.len() - 1) as u32
                }
            };
            prop_assert_eq!(idx, expected);
            prop_assert_eq!(si.get(idx), s.as_slice());
        }
        prop_assert_eq!(si.count() as usize, model.len());
        let offsets = si.offsets();
        prop_assert_eq!(offsets.len(), model.len() + 1);
        prop_assert!(offsets.windows(2).all(|w| w[0] <= w[1]));
        prop_assert_eq!(*offsets.last().unwrap(), si.data_len());
        prop_assert_eq!(si.data_bytes().len() as u32, si.data_len());
    }
}
