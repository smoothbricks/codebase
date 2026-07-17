//! Rust port of `packages/columine/src/vm/vm.zig` (3,799 LOC) — the dispatch
//! loop, the runtime undo/delta machinery, TTL eviction, and the read/iterate
//! export surface. Zig line citations point at the file at the port baseline.
//!
//! The Zig module keeps its runtime state in module globals (single-threaded
//! wasm). Rust models them as [`Vm`] (owning [`UndoState`] +
//! [`bitmap_ops::BitmapEnv`]); the wasm/NAPI bindings stage owns making one
//! long-lived instance per module instance.
//!
//! Shadow-buffer note: Zig splits overflow snapshots between a wasm-static
//! 1 MB buffer (skipping the snapshot entirely when the state is larger —
//! leaving post-overflow mutations unrecoverable) and an exact-size native
//! page allocation. Rust unifies on the exact-size allocation (the native
//! semantics, strictly more correct); the bindings stage decides whether the
//! wasm build needs the static-buffer/size-cap variant back.
//!
//! `defer clearBitmapScratch()` in the Zig entrypoints resets the wasm
//! scratch FixedBufferAllocator; Rust's allocator model has no observable
//! counterpart (see `bitmap_ops` module docs), so the entrypoints have no
//! equivalent step.

use crate::aggregates::{self, AggKind, TypeMask};
use crate::bitmap_ops::{
    self, BitmapAlgebraOp, BitmapEnv, batch_bitmap_add, batch_bitmap_algebra, batch_bitmap_remove,
    bitmap_load, bitmap_select, bitmap_store, get_bitmap_storage,
};
use crate::bytes;
use crate::hashmap_ops::{self, CmpType, Strategy};
use crate::hashset_ops;
use crate::hooks::{MutationOp, MutationRecord, VmHooks};
use crate::meta::{SlotMetaView, slot_meta_base};
use crate::nested;
use crate::state_init::{self, ARENA_HEADER_SIZE, NEEDS_GROWTH_SLOT};
use crate::struct_map::{StructMap2Slot, StructMapSlot};
use crate::undo_log::{
    self, FLAT_UNDO_ENTRY_SIZE, FlatUndoEntry, FlatUndoOp, SMF_BIT_SET, SMF_ROW_ABSENT,
    SMR_ROW_ABSENT,
};
use columine_types::types::{
    ChangeFlag, DERIVED_FACT_TOMBSTONE_IDENTITY, EMPTY_KEY, ErrorCode, Opcode, PROGRAM_HASH_PREFIX,
    PROGRAM_HEADER_SIZE, PROGRAM_MAGIC, SLOT_META_SIZE, STATE_HEADER_SIZE, STATE_MAGIC,
    SlotMetaOffset, SlotType, StateHeaderOffset, StructFieldType, TOMBSTONE, align8,
    struct_field_size,
};
use core::sync::atomic::Ordering;

// =============================================================================
// Input columns — vm.zig `col_ptrs` reinterpret casts
// =============================================================================

/// The VM's byte contract (state, program, columns) is little-endian; the
/// column views below reinterpret in place exactly like the Zig
/// `@ptrCast(@alignCast(...))` and are only equivalent to `from_le_bytes`
/// reads on a little-endian target.
#[cfg(target_endian = "big")]
compile_error!("columine-vm input-column views require a little-endian target");

/// vm.zig `getColU32` — a batch column as u32 cells.
///
/// The Zig contract is pointer + op-determined extent: section columns
/// (FOR_EACH / scatter sources) are legitimately SHORTER than `batch_len` —
/// each op reads the count its own semantics dictate (ingest-backends-parity
/// passes a 40-cell txn column with batch 50). The view therefore clamps to
/// `min(batch extent, available cells)`: batch-driven kernels iterate the
/// slice and must never see more than `batch_len` cells (on wasm the
/// "column" spans to the end of linear memory), while a section column
/// exposes its own shorter extent and an op that genuinely over-indexes
/// panics at the access (Zig reads garbage there — UB; checked-stricter
/// divergence, same class as the other checked-vs-UB sites). Alignment is
/// still a hard invariant.
pub fn col_u32(col: &[u8], batch_len: u32) -> &[u32] {
    let usable = (col.len() & !3).min(batch_len as usize * 4);
    // SAFETY: length within `col`; alignment checked via the `prefix`
    // assert below; u32 has no invalid bit patterns; the returned slice
    // borrows `col`, so no lifetime extension happens.
    let (prefix, cells, _) = unsafe { col[..usable].align_to::<u32>() };
    columine_types::check!(prefix.is_empty(), "u32 column misaligned");
    cells
}

/// vm.zig `getColF64` — a batch column as f64 cells (see `col_u32` for the
/// pointer + op-determined-extent contract).
pub fn col_f64(col: &[u8], batch_len: u32) -> &[f64] {
    let usable = (col.len() & !7).min(batch_len as usize * 8);
    // SAFETY: as in `col_u32`; every bit pattern is a valid f64.
    let (prefix, cells, _) = unsafe { col[..usable].align_to::<f64>() };
    columine_types::check!(prefix.is_empty(), "f64 column misaligned");
    cells
}

/// Reverse views (typed slice → LE bytes) for callers that own typed
/// columns (tests, bindings). Always sound: u8 alignment is 1 and every byte
/// of the source is initialized.
pub fn u32s_as_bytes(v: &[u32]) -> &[u8] {
    // SAFETY: see above — alignment 1, length exact, lifetime tied to `v`.
    unsafe { core::slice::from_raw_parts(v.as_ptr().cast(), v.len() * 4) }
}

pub fn f64s_as_bytes(v: &[f64]) -> &[u8] {
    // SAFETY: as in `u32s_as_bytes`.
    unsafe { core::slice::from_raw_parts(v.as_ptr().cast(), v.len() * 8) }
}

pub fn i64s_as_bytes(v: &[i64]) -> &[u8] {
    // SAFETY: as in `u32s_as_bytes`.
    unsafe { core::slice::from_raw_parts(v.as_ptr().cast(), v.len() * 8) }
}

/// vm.zig `getColI64` — a batch column as i64 cells (see `col_u32` for the
/// pointer + op-determined-extent contract).
pub fn col_i64(col: &[u8], batch_len: u32) -> &[i64] {
    let usable = (col.len() & !7).min(batch_len as usize * 8);
    // SAFETY: as in `col_u32`.
    let (prefix, cells, _) = unsafe { col[..usable].align_to::<i64>() };
    columine_types::check!(prefix.is_empty(), "i64 column misaligned");
    cells
}

/// vm.zig `getCol*` never bounds-checks the column-pointer table: with
/// `batch_len == 0` the TS side legitimately passes FEWER column pointers than
/// the program references (empty batches ship an empty column array), and the
/// Zig arms fetch the garbage pointer but never dereference it. Checked
/// indexing here would panic where Zig is well-behaved, so out-of-range
/// resolves to the empty column; any actual dereference attempt then fails
/// the `col_*` length assert (batch_len > 0 with a missing column is a real
/// JS/FFI-boundary bug, same contract as the alignment asserts).
#[inline(always)]
pub fn col_at<'a>(cols: &[&'a [u8]], idx: usize) -> &'a [u8] {
    cols.get(idx).copied().unwrap_or(&[])
}

// =============================================================================
// Eviction entries — 16-byte LE records (types.zig `EvictionEntry`)
// =============================================================================

/// `EvictionEntry` layout: `timestamp:f64 @0, key_or_idx:u32 @8, value:u32 @12`
/// (16 bytes; layout pinned in columine-types). Accessors below address entry
/// `i` of the index/buffer starting at `base`.
pub const EVICTION_ENTRY_SIZE: u32 = 16;

fn evict_ts(state: &[u8], base: u32, i: u32) -> f64 {
    bytes::read_f64(state, base + i * EVICTION_ENTRY_SIZE)
}

fn evict_key(state: &[u8], base: u32, i: u32) -> u32 {
    bytes::read_u32(state, base + i * EVICTION_ENTRY_SIZE + 8)
}

fn evict_value(state: &[u8], base: u32, i: u32) -> u32 {
    bytes::read_u32(state, base + i * EVICTION_ENTRY_SIZE + 12)
}

fn evict_write(state: &mut [u8], base: u32, i: u32, ts: f64, key: u32, value: u32) {
    let off = base + i * EVICTION_ENTRY_SIZE;
    bytes::write_f64(state, off, ts);
    bytes::write_u32(state, off + 8, key);
    bytes::write_u32(state, off + 12, value);
}

fn evict_copy(state: &mut [u8], base: u32, dst: u32, src: u32) {
    let (ts, key, value) = (
        evict_ts(state, base, src),
        evict_key(state, base, src),
        evict_value(state, base, src),
    );
    evict_write(state, base, dst, ts, key, value);
}

// =============================================================================
// TTL eviction operations (vm.zig:665-1023)
// =============================================================================

/// vm.zig:680 `binarySearchEvictionPos` — insertion position by timestamp.
fn binary_search_eviction_pos(state: &[u8], base: u32, size: u32, timestamp: f64) -> u32 {
    let (mut left, mut right) = (0u32, size);
    while left < right {
        let mid = left + (right - left) / 2;
        if evict_ts(state, base, mid) < timestamp {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    left
}

/// vm.zig:696 `shiftEvictionLeft`.
fn shift_eviction_left(state: &mut [u8], base: u32, count: u32, size: u32) {
    if count >= size {
        return;
    }
    for i in 0..size - count {
        evict_copy(state, base, i, i + count);
    }
}

/// vm.zig:706 `shiftEvictionRight`.
fn shift_eviction_right(state: &mut [u8], base: u32, pos: u32, size: u32) {
    if size == 0 {
        return;
    }
    let mut i = size;
    while i > pos {
        evict_copy(state, base, i, i - 1);
        i -= 1;
    }
}

/// vm.zig:717 `removeEvictionEntriesForKey` — compacting removal; returns the
/// removed count.
fn remove_eviction_entries_for_key(state: &mut [u8], base: u32, size: u32, key: u32) -> u32 {
    let mut write_idx = 0u32;
    let mut removed = 0u32;
    for read_idx in 0..size {
        if evict_key(state, base, read_idx) == key {
            removed += 1;
            continue;
        }
        if write_idx != read_idx {
            evict_copy(state, base, write_idx, read_idx);
        }
        write_idx += 1;
    }
    removed
}

/// vm.zig:737 `findLatestEvictionTimestampForKey`.
pub fn find_latest_eviction_timestamp_for_key(
    state: &[u8],
    meta: &SlotMetaView,
    key: u32,
) -> Option<f64> {
    let base = meta.eviction_index_offset(state);
    let size = meta.eviction_index_size(state);
    (0..size)
        .rev()
        .find(|&i| evict_key(state, base, i) == key)
        .map(|i| evict_ts(state, base, i))
}

/// vm.zig:750 `removeTTLEntriesForKey`.
pub fn remove_ttl_entries_for_key(state: &mut [u8], meta: &SlotMetaView, key: u32) {
    if !meta.has_ttl() {
        return;
    }
    let base = meta.eviction_index_offset(state);
    let size = meta.eviction_index_size(state);
    let removed = remove_eviction_entries_for_key(state, base, size, key);
    if removed > 0 {
        meta.set_eviction_index_size(state, size - removed);
    }
}

/// vm.zig:761 `restoreTTLEntry` — rollback re-inserts a TTL entry; failure is
/// a programmer bug (the entry fit before the rollback).
fn restore_ttl_entry(state: &mut [u8], meta: &SlotMetaView, key: u32, timestamp: f64) {
    if !meta.has_ttl() {
        return;
    }
    let result = insert_with_ttl(state, meta, key, timestamp);
    debug_assert_eq!(result, ErrorCode::Ok);
}

/// vm.zig:769 `isEvictionEntryCurrent` — no newer entry for the key later in
/// the sorted index.
fn is_eviction_entry_current(state: &[u8], base: u32, size: u32, entry_idx: u32, key: u32) -> bool {
    ((entry_idx + 1)..size).all(|i| evict_key(state, base, i) != key)
}

/// vm.zig:961 `insertWithTTL` — sorted eviction-index insert (removes stale
/// entries for the key first). CAPACITY_EXCEEDED signals index growth through
/// the same path as primary storage.
pub fn insert_with_ttl(
    state: &mut [u8],
    meta: &SlotMetaView,
    key: u32,
    timestamp: f64,
) -> ErrorCode {
    if !meta.has_ttl() {
        return ErrorCode::Ok;
    }

    let base = meta.eviction_index_offset(state);
    let size_initial = meta.eviction_index_size(state);
    let removed = remove_eviction_entries_for_key(state, base, size_initial, key);
    let size = size_initial - removed;
    meta.set_eviction_index_size(state, size);

    if size >= meta.eviction_index_capacity(state) {
        return ErrorCode::CapacityExceeded;
    }

    let pos = binary_search_eviction_pos(state, base, size, timestamp);

    // vm.zig:980 — the entry snapshots the key's current mapped value.
    let entry_value = match meta.slot_type() {
        SlotType::HashMap => {
            let tbl = hashmap_ops::bind_slot_map(meta);
            tbl.get_u32(state, key).unwrap_or(0)
        }
        SlotType::HashSet | SlotType::Bitmap => key,
        _ => 0,
    };

    shift_eviction_right(state, base, pos, size);
    evict_write(state, base, pos, timestamp, key, entry_value);
    meta.set_eviction_index_size(state, size + 1);
    ErrorCode::Ok
}

/// vm.zig:997 `clearEvictedBuffer`.
pub fn clear_evicted_buffer(state: &mut [u8], meta: &SlotMetaView) {
    meta.set_evicted_count(state, 0);
}

/// vm.zig:883 `removeEntryByKey` — primary-storage removal per slot type.
fn remove_entry_by_key(
    env: &mut BitmapEnv,
    state: &mut [u8],
    meta: &SlotMetaView,
    key: u32,
) -> bool {
    match meta.slot_type() {
        SlotType::HashMap => {
            let tbl = hashmap_ops::bind_slot_map(meta);
            match tbl.find(state, key) {
                Some(pos) => {
                    tbl.set_key_at(state, pos, TOMBSTONE);
                    true
                }
                None => false,
            }
        }
        SlotType::HashSet => {
            let tbl = hashset_ops::bind_slot_set(meta);
            match tbl.find(state, key) {
                Some(pos) => {
                    tbl.set_key_at(state, pos, TOMBSTONE);
                    true
                }
                None => false,
            }
        }
        SlotType::Bitmap => {
            let storage = get_bitmap_storage(meta);
            let Some(mut bitmap) = bitmap_load(env, state, storage) else {
                return false;
            };
            if !bitmap.remove(key) {
                return false;
            }
            let cardinality = bitmap.len() as u32;
            if bitmap_store(env, state, storage, &mut bitmap) != ErrorCode::Ok {
                return false;
            }
            meta.set_size(state, cardinality);
            true
        }
        SlotType::Array => {
            // vm.zig:909 — key_or_idx is the array index; tombstone approach.
            let data = meta.offset;
            if key < meta.capacity {
                let k = bytes::read_u32(state, data + key * 4);
                if k == EMPTY_KEY || k == TOMBSTONE {
                    return false;
                }
                bytes::write_u32(state, data + key * 4, TOMBSTONE);
                return true;
            }
            false
        }
        SlotType::ConditionTree => {
            // vm.zig:954 `removeConditionTreeEntry` — bump lifecycle
            // generation (+%= wrap) and record the removed key.
            // ConditionTreeState: lifecycle_generation:u32 @0, last_removed_key:u32 @4.
            let off = meta.offset;
            let generation = bytes::read_u32(state, off).wrapping_add(1);
            bytes::write_u32(state, off, generation);
            bytes::write_u32(state, off + 4, key);
            true
        }
        // vm.zig:922-946 — no per-entry TTL removal for these slot types.
        SlotType::Aggregate
        | SlotType::StructMap
        | SlotType::StructMap2
        | SlotType::OrderedList
        | SlotType::Scalar
        | SlotType::Nested => false,
    }
}

/// vm.zig:785 `evictExpired` — evict everything older than the cutoff.
/// `#region axe!n/vm-architecture-ttl.evict` carries over from the Zig.
//#region axe!n/vm-architecture-ttl.evict #eviction-index #o-expired
pub fn evict_expired(
    undo: &mut UndoState,
    env: &mut BitmapEnv,
    state: &mut [u8],
    meta: &SlotMetaView,
    slot_idx: u8,
    now: f64,
) -> u32 {
    if !meta.has_ttl() {
        return 0;
    }

    let cutoff = meta.cutoff(state, now);
    let base = meta.eviction_index_offset(state);
    let eviction_size = meta.eviction_index_size(state);

    let mut processed = 0u32;
    let mut removed_count = 0u32;

    while processed < eviction_size {
        let entry_ts = evict_ts(state, base, processed);
        if entry_ts >= cutoff {
            break;
        }
        let entry_key = evict_key(state, base, processed);
        let entry_value = evict_value(state, base, processed);

        if !is_eviction_entry_current(state, base, eviction_size, processed, entry_key) {
            processed += 1;
            continue;
        }

        // vm.zig:809 — journal the eviction. Evictions are undo-only BY
        // DESIGN: an eviction has no meaningful forward replay (rollforward
        // re-derives expiry from TTL state), so its redo lane is the zeroed
        // no-op marker that delta_apply_rollforward_segment skips.
        if undo.enabled {
            match meta.slot_type() {
                SlotType::HashMap => {
                    let tbl = hashmap_ops::bind_slot_map(meta);
                    if let Some(idx) = tbl.find(state, entry_key) {
                        let value = tbl.entry_u32_at(state, idx);
                        // Timestamp lane lives after keys+values (cap*8).
                        let ts = bytes::read_f64(state, meta.offset + meta.capacity * 8 + idx * 8);
                        undo.append_undo_only_snapshot(
                            state,
                            FlatUndoEntry {
                                op: FlatUndoOp::MapDelete,
                                slot: slot_idx,
                                pad1: 0,
                                pad2: 0,
                                key: entry_key,
                                prev_value: value,
                                aux: ts.to_bits(),
                            },
                        );
                    }
                }
                SlotType::HashSet | SlotType::Bitmap => {
                    undo.append_undo_only_snapshot(
                        state,
                        FlatUndoEntry {
                            op: FlatUndoOp::SetDelete,
                            slot: slot_idx,
                            pad1: 0,
                            pad2: 0,
                            key: entry_key,
                            prev_value: 0,
                            aux: entry_ts.to_bits(),
                        },
                    );
                }
                _ => {}
            }
        }

        if remove_entry_by_key(env, state, meta, entry_key) {
            if meta.has_evict_trigger() {
                let buf = meta.evicted_buffer_offset(state);
                let evicted_count = meta.evicted_count(state);
                evict_write(state, buf, evicted_count, entry_ts, entry_key, entry_value);
                meta.set_evicted_count(state, evicted_count + 1);
            }
            removed_count += 1;
        }

        processed += 1;
    }

    if processed > 0 {
        shift_eviction_left(state, base, processed, eviction_size);
        meta.set_eviction_index_size(state, eviction_size - processed);

        if removed_count > 0 {
            let size = meta.size(state);
            meta.set_size(state, size - removed_count);
            meta.set_change_flag(state, ChangeFlag::EVICTED);
        }
    }

    removed_count
}
//#endregion axe!n/vm-architecture-ttl.evict

// =============================================================================
// Derived-facts header access (vm.zig:3294-3348)
// =============================================================================

pub fn get_derived_facts_offset(state: &[u8]) -> u32 {
    bytes::read_u32(state, StateHeaderOffset::DERIVED_FACTS_OFFSET)
}

pub fn get_derived_facts_capacity(state: &[u8]) -> u16 {
    bytes::read_u16(state, StateHeaderOffset::DERIVED_FACTS_CAPACITY)
}

pub fn get_num_derived_fact_schemas(state: &[u8]) -> u8 {
    state[StateHeaderOffset::NUM_DERIVED_FACT_SCHEMAS as usize]
}

pub fn get_derived_facts_change_flag(state: &[u8]) -> u8 {
    state[StateHeaderOffset::DERIVED_FACTS_CHANGE_FLAG as usize]
}

pub fn set_derived_facts_change_flag(state: &mut [u8], flag: u8) {
    state[StateHeaderOffset::DERIVED_FACTS_CHANGE_FLAG as usize] |= flag;
}

pub fn clear_derived_facts_change_flag(state: &mut [u8]) {
    state[StateHeaderOffset::DERIVED_FACTS_CHANGE_FLAG as usize] = 0;
}

/// vm.zig:3327 `writeDerivedFactsHeader` (RETE program loading — stage 3
/// consumes it; the header fields themselves are columine's).
pub fn write_derived_facts_header(
    state: &mut [u8],
    derived_offset: u32,
    capacity: u16,
    num_schemas: u8,
) {
    bytes::write_u32(
        state,
        StateHeaderOffset::DERIVED_FACTS_OFFSET,
        derived_offset,
    );
    bytes::write_u16(state, StateHeaderOffset::DERIVED_FACTS_CAPACITY, capacity);
    state[StateHeaderOffset::NUM_DERIVED_FACT_SCHEMAS as usize] = num_schemas;
    state[StateHeaderOffset::DERIVED_FACTS_CHANGE_FLAG as usize] = 0;
}

// =============================================================================
// Undo/delta runtime state (vm.zig:208-333, 3630-3792)
// =============================================================================

/// vm.zig:208 `UNDO_CAPACITY`.
pub const UNDO_CAPACITY: u32 = 16384;

/// The vm.zig undo/delta module globals as one owned value.
#[derive(Debug)]
pub struct UndoState {
    /// `g_undo_entries[0..g_undo_count]`.
    entries: Vec<FlatUndoEntry>,
    /// `g_redo_entries` — parallel lane. Undo-only appends store the zeroed
    /// entry as an explicit no-op marker (op byte 0), which rollforward
    /// skips — deterministic exports, garbage-free replay. (The deleted Zig
    /// exported undefined bytes for these lanes.)
    redo: Vec<FlatUndoEntry>,
    /// `g_delta_count` — pairs are valid up to here.
    delta_count: u32,
    pub overflow: bool,
    pub enabled: bool,
    /// `g_undo_shadow_*` — exact-size snapshot taken at first overflow.
    shadow: Option<Vec<u8>>,
    shadow_active: bool,
    /// `g_undo_overflow_entry` (+ redo lane) — the entry that hit the wall.
    overflow_entry: Option<(FlatUndoEntry, FlatUndoEntry)>,
    /// `g_undo_state_size` — size registered at `vm_undo_enable`.
    pub state_size: u32,
    /// `g_saved_change_flags` (+count).
    saved_change_flags: [u8; 257],
    saved_change_flags_count: u32,
    /// `g_delta_export_*`.
    export_start: u32,
    export_count: u32,
    export_overflow: bool,
    /// Reused pre-mutation bytes for post-mutation paired diff journaling.
    capture_scratch: Vec<u8>,
}

impl Default for UndoState {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            redo: Vec::new(),
            delta_count: 0,
            overflow: false,
            enabled: false,
            shadow: None,
            shadow_active: false,
            overflow_entry: None,
            state_size: 0,
            saved_change_flags: [0; 257],
            saved_change_flags_count: 0,
            export_start: 0,
            export_count: 0,
            export_overflow: false,
            capture_scratch: Vec::new(),
        }
    }
}

const ZERO_ENTRY: FlatUndoEntry = FlatUndoEntry {
    op: FlatUndoOp::MapInsert, // placeholder discriminant; never read (see redo lane note)
    slot: 0,
    pad1: 0,
    pad2: 0,
    key: 0,
    prev_value: 0,
    aux: 0,
};

impl UndoState {
    pub fn count(&self) -> u32 {
        self.entries.len() as u32
    }

    pub fn delta_count(&self) -> u32 {
        self.delta_count
    }

    fn snapshot(&mut self, state: &[u8]) {
        let size = self.state_size as usize;
        debug_assert!(
            state.len() >= size,
            "undo state shrank since vm_undo_enable"
        );
        self.shadow = Some(state[..size].to_vec());
        self.shadow_active = true;
    }

    /// rete.zig derived-fact journaling (FACT_INSERT_NEW / FACT_INSERT_UPDATE /
    /// FACT_RETRACT) appends through the same undo-only lane as evict — Zig's
    /// rete.zig:445/463/540 call `vm.undoAppend` directly. Public API extension
    /// for the axe-rete crate; caller checks `enabled` like the Zig call sites.
    pub fn append_undo_only(&mut self, state: &[u8], entry: FlatUndoEntry) {
        self.append_undo_only_snapshot(state, entry);
    }

    /// Append a paired before/after mutation through the same bounded journal
    /// and first-overflow snapshot path as VM-owned delta mutations.
    ///
    /// AxE's derived-fact VM uses this public seam because its RETE execution
    /// lives in a sibling crate while the journal remains owned here.
    pub fn append_pair(
        &mut self,
        state: &[u8],
        undo_entry: FlatUndoEntry,
        redo_entry: FlatUndoEntry,
    ) {
        self.append_pair_snapshot(state, undo_entry, redo_entry);
    }

    /// vm.zig:240 `undoAppend` — undo-only lane (non-delta batches, evict).
    fn append_undo_only_snapshot(&mut self, state: &[u8], entry: FlatUndoEntry) {
        if self.count() < UNDO_CAPACITY {
            self.entries.push(entry);
            self.redo.push(ZERO_ENTRY);
        } else if !self.overflow {
            self.snapshot(state);
            self.overflow_entry = Some((entry, ZERO_ENTRY));
            self.overflow = true;
        }
        // Already overflowed: silently drop — the shadow covers it.
    }

    /// vm.zig:269 `undoAppendPair`.
    fn append_pair_snapshot(&mut self, state: &[u8], undo_e: FlatUndoEntry, redo_e: FlatUndoEntry) {
        if self.count() < UNDO_CAPACITY {
            self.entries.push(undo_e);
            self.redo.push(redo_e);
            self.delta_count = self.count();
        } else if !self.overflow {
            self.snapshot(state);
            self.overflow_entry = Some((undo_e, redo_e));
            self.overflow = true;
        }
    }

    /// Capture discontiguous state regions before a mutation whose exact byte
    /// changes are only known afterward. Capacity is reserved up front so a
    /// first overflow snapshots the true pre-mutation state, never post-state.
    fn begin_state_capture(
        &mut self,
        state: &[u8],
        ranges: &[(u32, u32)],
        additional_entries: u32,
    ) -> bool {
        if !self.enabled || self.overflow {
            return false;
        }
        let max_entries = ranges
            .iter()
            .map(|(_, len)| len.div_ceil(8))
            .sum::<u32>()
            .saturating_add(additional_entries);
        if self.count().saturating_add(max_entries) > UNDO_CAPACITY {
            self.snapshot(state);
            self.overflow_entry = None;
            self.overflow = true;
            return false;
        }

        self.capture_scratch.clear();
        let capture_len = ranges.iter().map(|(_, len)| *len as usize).sum();
        self.capture_scratch.reserve(capture_len);
        for &(offset, len) in ranges {
            let start = offset as usize;
            let end = start + len as usize;
            self.capture_scratch.extend_from_slice(&state[start..end]);
        }
        true
    }

    /// Emit only changed 1..=8-byte chunks from a prior `begin_state_capture`.
    fn finish_state_capture(&mut self, delta_mode: bool, state: &[u8], ranges: &[(u32, u32)]) {
        let mut scratch_offset = 0usize;
        for &(state_offset, len) in ranges {
            let mut relative = 0u32;
            while relative < len {
                let chunk_len = (len - relative).min(8);
                let before_start = scratch_offset + relative as usize;
                let before_end = before_start + chunk_len as usize;
                let after_start = (state_offset + relative) as usize;
                let after_end = after_start + chunk_len as usize;
                let before = &self.capture_scratch[before_start..before_end];
                let after = &state[after_start..after_end];
                if before != after {
                    let undo_entry =
                        state_bytes_entry(state_offset + relative, chunk_len as u8, before);
                    let redo_entry =
                        state_bytes_entry(state_offset + relative, chunk_len as u8, after);
                    if delta_mode {
                        self.append_pair_snapshot(state, undo_entry, redo_entry);
                    } else {
                        self.append_undo_only_snapshot(state, undo_entry);
                    }
                }
                relative += chunk_len;
            }
            scratch_offset += len as usize;
        }
        self.capture_scratch.clear();
    }

    /// vm.zig:311 `saveChangeFlags`.
    fn save_change_flags(&mut self, state: &[u8]) {
        let num_slots = state[StateHeaderOffset::NUM_SLOTS as usize] as u32;
        for i in 0..num_slots {
            let meta_offset = STATE_HEADER_SIZE + i * SLOT_META_SIZE;
            self.saved_change_flags[i as usize] =
                state[(meta_offset + SlotMetaOffset::CHANGE_FLAGS) as usize];
        }
        self.saved_change_flags[num_slots as usize] =
            state[StateHeaderOffset::DERIVED_FACTS_CHANGE_FLAG as usize];
        self.saved_change_flags_count = num_slots + 1;
    }

    /// vm.zig:324 `restoreChangeFlags`.
    fn restore_change_flags(&self, state: &mut [u8]) {
        if self.saved_change_flags_count == 0 {
            return;
        }
        let num_slots = self.saved_change_flags_count - 1;
        for i in 0..num_slots {
            let meta_offset = STATE_HEADER_SIZE + i * SLOT_META_SIZE;
            state[(meta_offset + SlotMetaOffset::CHANGE_FLAGS) as usize] =
                self.saved_change_flags[i as usize];
        }
        state[StateHeaderOffset::DERIVED_FACTS_CHANGE_FLAG as usize] =
            self.saved_change_flags[num_slots as usize];
    }
}

fn state_bytes_entry(offset: u32, len: u8, value: &[u8]) -> FlatUndoEntry {
    debug_assert!((1..=8).contains(&len));
    debug_assert_eq!(usize::from(len), value.len());
    let mut cell = [0u8; 8];
    cell[..value.len()].copy_from_slice(value);
    FlatUndoEntry {
        op: FlatUndoOp::StateBytes,
        slot: 0,
        pad1: len,
        pad2: 0,
        key: offset,
        prev_value: 0,
        aux: u64::from_le_bytes(cell),
    }
}

/// The Zig module globals as one long-lived VM instance.
#[derive(Debug, Default)]
pub struct Vm {
    pub undo: UndoState,
    pub bitmap_env: BitmapEnv,
}

/// Split-borrow view implementing the container-ops hooks boundary: the undo
/// lane and the bitmap environment are independent Zig global groups, and the
/// bitmap ops need both (`hooks` + `env`) at once.
pub struct VmCtx<'a> {
    pub undo: &'a mut UndoState,
    pub env: &'a mut BitmapEnv,
}

impl Vm {
    pub fn ctx(&mut self) -> VmCtx<'_> {
        VmCtx {
            undo: &mut self.undo,
            env: &mut self.bitmap_env,
        }
    }
}

const fn mutation_op_to_flat(op: MutationOp) -> FlatUndoOp {
    match op {
        MutationOp::SetInsert => FlatUndoOp::SetInsert,
        MutationOp::SetDelete => FlatUndoOp::SetDelete,
        MutationOp::MapInsert => FlatUndoOp::MapInsert,
        MutationOp::MapDelete => FlatUndoOp::MapDelete,
        MutationOp::MapUpdate => FlatUndoOp::MapUpdate,
    }
}

const fn record_to_entry(r: MutationRecord) -> FlatUndoEntry {
    FlatUndoEntry {
        op: mutation_op_to_flat(r.op),
        slot: r.slot,
        pad1: 0,
        pad2: 0,
        key: r.key,
        prev_value: r.prev_value,
        aux: r.aux,
    }
}

impl VmHooks for VmCtx<'_> {
    fn undo_enabled(&self) -> bool {
        self.undo.enabled
    }

    fn append_mutation(
        &mut self,
        delta_mode: bool,
        state: &[u8],
        undo_r: MutationRecord,
        redo_r: MutationRecord,
    ) {
        // Zig's undoAppend/undoAppendPair snapshot the state on FIRST
        // overflow through the aliasing `g_undo_state_base` global — for
        // every append path, container ops included. The ops reborrow the
        // buffer shared for the call, so the snapshot happens at the true
        // overflow moment here too.
        let (u, r) = (record_to_entry(undo_r), record_to_entry(redo_r));
        if delta_mode {
            self.undo.append_pair_snapshot(state, u, r);
        } else {
            self.undo.append_undo_only_snapshot(state, u);
        }
    }

    fn insert_with_ttl(
        &mut self,
        state: &mut [u8],
        meta: &SlotMetaView,
        key: u32,
        ts: f64,
    ) -> ErrorCode {
        insert_with_ttl(state, meta, key, ts)
    }

    fn latest_eviction_ts(&self, state: &[u8], meta: &SlotMetaView, key: u32) -> Option<f64> {
        find_latest_eviction_timestamp_for_key(state, meta, key)
    }

    fn remove_ttl_entries_for_key(&mut self, state: &mut [u8], meta: &SlotMetaView, key: u32) {
        remove_ttl_entries_for_key(state, meta, key);
    }

    fn undo_overflow(&self) -> bool {
        self.undo.overflow
    }

    fn force_undo_snapshot(&mut self, state: &[u8]) {
        // bitmap_ops.zig:511 — bulk algebra rollback is snapshot-based.
        self.undo.snapshot(state);
        self.undo.overflow = true;
    }

    fn batch_bitmap_add(
        &mut self,
        delta_mode: bool,
        state: &mut [u8],
        meta: &SlotMetaView,
        slot_idx: u8,
        elems: &[u32],
        ts_col: Option<&[f64]>,
    ) -> ErrorCode {
        let mut inner = VmCtx {
            undo: self.undo,
            env: &mut BitmapEnv::default(),
        };
        // The delegated call needs env AND hooks; reborrowing self.env for
        // both is impossible, and bitmap ops only use hooks for the undo
        // lane, so the inner ctx carries a throwaway env for the recursive
        // hooks slot while the REAL env does the bitmap work.
        batch_bitmap_add(
            self.env, &mut inner, delta_mode, state, meta, slot_idx, elems, ts_col,
        )
    }

    fn batch_bitmap_remove(
        &mut self,
        delta_mode: bool,
        state: &mut [u8],
        meta: &SlotMetaView,
        slot_idx: u8,
        elems: &[u32],
    ) {
        let mut inner = VmCtx {
            undo: self.undo,
            env: &mut BitmapEnv::default(),
        };
        batch_bitmap_remove(
            self.env, &mut inner, delta_mode, state, meta, slot_idx, elems,
        );
    }
}

// =============================================================================
// Rollback (vm.zig:343-543)
// =============================================================================

/// vm.zig:343 `rollbackEntry` — reverse one mutation on the state buffer.
pub fn rollback_entry(env: &mut BitmapEnv, state: &mut [u8], entry: &FlatUndoEntry) {
    match entry.op {
        FlatUndoOp::MapInsert => {
            let meta = SlotMetaView::read(state, entry.slot);
            if undo_log::rollback_map_insert(state, &meta, entry.key) && meta.has_ttl() {
                remove_ttl_entries_for_key(state, &meta, entry.key);
            }
        }
        FlatUndoOp::MapUpdate => {
            let meta = SlotMetaView::read(state, entry.slot);
            if undo_log::rollback_map_update(state, &meta, entry.key, entry.prev_value, entry.aux)
                && meta.has_ttl()
            {
                remove_ttl_entries_for_key(state, &meta, entry.key);
                restore_ttl_entry(state, &meta, entry.key, f64::from_bits(entry.aux));
            }
        }
        FlatUndoOp::MapDelete => {
            let meta = SlotMetaView::read(state, entry.slot);
            if undo_log::rollback_map_delete(state, &meta, entry.key, entry.prev_value, entry.aux)
                && meta.has_ttl()
            {
                remove_ttl_entries_for_key(state, &meta, entry.key);
                restore_ttl_entry(state, &meta, entry.key, f64::from_bits(entry.aux));
            }
        }
        FlatUndoOp::SetInsert => {
            let meta = SlotMetaView::read(state, entry.slot);
            if meta.slot_type() == SlotType::Bitmap {
                let storage = get_bitmap_storage(&meta);
                let Some(mut bitmap) = bitmap_load(env, state, storage) else {
                    return;
                };
                if bitmap.remove(entry.key) {
                    let cardinality = bitmap.len() as u32;
                    if bitmap_store(env, state, storage, &mut bitmap) != ErrorCode::Ok {
                        return;
                    }
                    meta.set_size(state, cardinality);
                    if meta.has_ttl() {
                        remove_ttl_entries_for_key(state, &meta, entry.key);
                    }
                }
            } else if undo_log::rollback_set_insert(state, &meta, entry.key) && meta.has_ttl() {
                remove_ttl_entries_for_key(state, &meta, entry.key);
            }
        }
        FlatUndoOp::SetDelete => {
            let meta = SlotMetaView::read(state, entry.slot);
            if meta.slot_type() == SlotType::Bitmap {
                let storage = get_bitmap_storage(&meta);
                let Some(mut bitmap) = bitmap_load(env, state, storage) else {
                    return;
                };
                bitmap.insert(entry.key);
                let cardinality = bitmap.len() as u32;
                if cardinality <= meta.capacity
                    && bitmap_store(env, state, storage, &mut bitmap) == ErrorCode::Ok
                {
                    meta.set_size(state, cardinality);
                }
                if meta.has_ttl() {
                    remove_ttl_entries_for_key(state, &meta, entry.key);
                    restore_ttl_entry(state, &meta, entry.key, f64::from_bits(entry.aux));
                }
            } else if undo_log::rollback_set_delete(state, &meta, entry.key) && meta.has_ttl() {
                remove_ttl_entries_for_key(state, &meta, entry.key);
                restore_ttl_entry(state, &meta, entry.key, f64::from_bits(entry.aux));
            }
        }
        FlatUndoOp::AggUpdate => {
            let meta = SlotMetaView::read(state, entry.slot);
            let count = u64::from(entry.prev_value) | (u64::from(entry.key) << 32);
            undo_log::rollback_agg_update(state, &meta, count, entry.aux);
        }
        FlatUndoOp::CountUpdate => {
            let meta = SlotMetaView::read(state, entry.slot);
            let count = u64::from(entry.prev_value) | (u64::from(entry.key) << 32);
            undo_log::rollback_count_update(state, &meta, count);
        }
        FlatUndoOp::ScalarUpdate => {
            let meta = SlotMetaView::read(state, entry.slot);
            let ts = u64::from(entry.prev_value) | (u64::from(entry.key) << 32);
            undo_log::rollback_scalar_update(state, &meta, entry.aux, f64::from_bits(ts));
        }
        FlatUndoOp::FactInsertNew => {
            let derived_offset = get_derived_facts_offset(state);
            let capacity = u32::from(get_derived_facts_capacity(state));
            let slot_idx = entry.prev_value;
            if slot_idx < capacity {
                bytes::write_u64(
                    state,
                    derived_offset + slot_idx * 8,
                    DERIVED_FACT_TOMBSTONE_IDENTITY,
                );
            }
        }
        FlatUndoOp::FactInsertUpdate | FlatUndoOp::FactRetract => {
            let derived_offset = get_derived_facts_offset(state);
            let capacity = u32::from(get_derived_facts_capacity(state));
            let slot_idx = entry.prev_value;
            if slot_idx < capacity {
                let fact_idx = u16::from_le_bytes([entry.pad1, entry.pad2]);
                let identity = (u64::from(fact_idx) << 32) | u64::from(entry.key);
                bytes::write_u64(state, derived_offset + slot_idx * 8, identity);
                bytes::write_u32(
                    state,
                    derived_offset + capacity * 8 + slot_idx * 4,
                    entry.aux as u32,
                );
                bytes::write_u32(
                    state,
                    derived_offset + capacity * 12 + slot_idx * 4,
                    (entry.aux >> 32) as u32,
                );
            }
        }
        FlatUndoOp::StateBytes => {
            let len = u32::from(entry.pad1);
            if len == 0 || len > 8 || entry.key.saturating_add(len) > state.len() as u32 {
                return;
            }
            let cell = entry.aux.to_le_bytes();
            state[entry.key as usize..(entry.key + len) as usize]
                .copy_from_slice(&cell[..len as usize]);
        }
        FlatUndoOp::ListAppendUndo => {
            // vm.zig:464 — restore the ORDERED_LIST count (raw SIZE field;
            // getSlotMeta cannot bind ORDERED_LIST's repurposed metadata).
            let meta_off = slot_meta_base(entry.slot);
            bytes::write_u32(state, meta_off + SlotMetaOffset::SIZE, entry.prev_value);
        }
        FlatUndoOp::StructMapField => {
            if SlotMetaView::read(state, entry.slot).slot_type() == SlotType::StructMap2 {
                rollback_struct_map2_field(state, entry);
            } else {
                rollback_struct_map_field(state, entry);
            }
        }
        FlatUndoOp::StructMapRow => {
            if SlotMetaView::read(state, entry.slot).slot_type() == SlotType::StructMap2 {
                rollback_struct_map2_row(state, entry);
            } else {
                rollback_struct_map_row(state, entry);
            }
        }
    }
}

fn rollback_struct_map_field(state: &mut [u8], entry: &FlatUndoEntry) {
    let smap = StructMapSlot::bind(state, entry.slot);
    let field_idx = entry.pad1;
    let want_absent = entry.pad2 & SMF_ROW_ABSENT != 0;
    let want_bit_set = entry.pad2 & SMF_BIT_SET != 0;
    if want_absent {
        if let Some(pos) = smap.find(state, entry.key) {
            smap.set_key_at(state, pos, TOMBSTONE);
            smap.set_size(state, smap.size(state) - 1);
            smap.clear_bitset(state, smap.row_off(pos));
        }
        return;
    }
    let Some(ins) = smap.find_insert(state, entry.key) else {
        return;
    };
    if !ins.found {
        smap.set_key_at(state, ins.pos, entry.key);
        smap.set_size(state, smap.size(state) + 1);
        smap.clear_bitset(state, smap.row_off(ins.pos));
    }
    let row = smap.row_off(ins.pos);
    let fsize = struct_field_size(smap.field_type(state, field_idx));
    let f_off = row + smap.field_offset(state, field_idx);
    if want_bit_set {
        StructMapSlot::set_field_bit(state, row, field_idx);
        let cell = entry.aux.to_le_bytes();
        state[f_off as usize..(f_off + fsize) as usize].copy_from_slice(&cell[..fsize as usize]);
    } else {
        StructMapSlot::clear_scalar_field(state, row, field_idx);
        bytes::zero(state, f_off, fsize);
    }
}

fn rollback_struct_map_row(state: &mut [u8], entry: &FlatUndoEntry) {
    let smap = StructMapSlot::bind(state, entry.slot);
    if entry.pad2 & SMR_ROW_ABSENT != 0 {
        if let Some(pos) = smap.find(state, entry.key) {
            smap.set_key_at(state, pos, TOMBSTONE);
            smap.set_size(state, smap.size(state) - 1);
            smap.clear_bitset(state, smap.row_off(pos));
        }
        return;
    }
    let Some(ins) = smap.find_insert(state, entry.key) else {
        return;
    };
    if !ins.found {
        smap.set_key_at(state, ins.pos, entry.key);
        smap.set_size(state, smap.size(state) + 1);
    }
    let row = smap.row_off(ins.pos);
    let n = smap.bitset_bytes.min(8);
    let cell = entry.aux.to_le_bytes();
    state[row as usize..(row + n) as usize].copy_from_slice(&cell[..n as usize]);
}

fn rollback_struct_map2_field(state: &mut [u8], entry: &FlatUndoEntry) {
    let smap = StructMap2Slot::bind(state, entry.slot);
    let (key1, key2) = (entry.key, entry.prev_value);
    let field_idx = entry.pad1;
    let want_absent = entry.pad2 & SMF_ROW_ABSENT != 0;
    let want_bit_set = entry.pad2 & SMF_BIT_SET != 0;
    if want_absent {
        if let Some(pos) = smap.remove(state, key1, key2) {
            smap.clear_bitset(state, smap.row_off(pos));
        }
        return;
    }
    let Some(ins) = smap.find_insert(state, key1, key2) else {
        return;
    };
    if !ins.found {
        smap.set_keys_at(state, ins.pos, key1, key2);
        smap.set_size(state, smap.size(state) + 1);
        smap.clear_bitset(state, smap.row_off(ins.pos));
    }
    let row = smap.row_off(ins.pos);
    let fsize = struct_field_size(smap.field_type(state, field_idx));
    let f_off = row + smap.field_offset(state, field_idx);
    if want_bit_set {
        StructMapSlot::set_field_bit(state, row, field_idx);
        let cell = entry.aux.to_le_bytes();
        state[f_off as usize..(f_off + fsize) as usize].copy_from_slice(&cell[..fsize as usize]);
    } else {
        StructMapSlot::clear_scalar_field(state, row, field_idx);
        bytes::zero(state, f_off, fsize);
    }
}

fn rollback_struct_map2_row(state: &mut [u8], entry: &FlatUndoEntry) {
    let smap = StructMap2Slot::bind(state, entry.slot);
    let (key1, key2) = (entry.key, entry.prev_value);
    if entry.pad2 & SMR_ROW_ABSENT != 0 {
        if let Some(pos) = smap.remove(state, key1, key2) {
            smap.clear_bitset(state, smap.row_off(pos));
        }
        return;
    }
    let Some(ins) = smap.find_insert(state, key1, key2) else {
        return;
    };
    if !ins.found {
        smap.set_keys_at(state, ins.pos, key1, key2);
        smap.set_size(state, smap.size(state) + 1);
    }
    let row = smap.row_off(ins.pos);
    let n = smap.bitset_bytes.min(8);
    let cell = entry.aux.to_le_bytes();
    state[row as usize..(row + n) as usize].copy_from_slice(&cell[..n as usize]);
}

// =============================================================================
// Struct-map journaling (vm.zig:545-663)
// =============================================================================

/// vm.zig:553 `packFieldBytes` — ≤8 field-cell bytes as a LE u64.
fn pack_field_bytes(bytes_in: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    buf[..bytes_in.len()].copy_from_slice(bytes_in);
    u64::from_le_bytes(buf)
}

/// vm.zig:658 `packBitsetBytes`.
fn pack_bitset_bytes(state: &[u8], off: u32, bitset_bytes: u32) -> u64 {
    let n = bitset_bytes.min(8) as usize;
    let mut buf = [0u8; 8];
    buf[..n].copy_from_slice(&state[off as usize..off as usize + n]);
    u64::from_le_bytes(buf)
}

/// vm.zig:559-566 `g_smr_prior_*` — the pre-overwrite row snapshot. Rust
/// passes it explicitly instead of using module globals.
struct RowPrior {
    bitset: [u8; 8],
    cells: [u64; 64],
}

impl RowPrior {
    const fn new() -> Self {
        Self {
            bitset: [0; 8],
            cells: [0; 64],
        }
    }

    fn is_field_set(&self, fi: u8) -> bool {
        self.bitset[(fi / 8) as usize] & (1 << (fi % 8)) != 0
    }
}

/// vm.zig:573 `captureStructMapRowPrior`.
fn capture_struct_map_row_prior(
    state: &[u8],
    smap: &StructMapSlot,
    row: u32,
    prior: &mut RowPrior,
) {
    let nb = smap.bitset_bytes.min(8) as usize;
    prior.bitset = [0; 8];
    prior.bitset[..nb].copy_from_slice(&state[row as usize..row as usize + nb]);
    let n_fields = smap.num_fields.min(64);
    for fi in 0..n_fields {
        if StructMapSlot::is_field_set(state, row, fi) {
            let ft = smap.field_type(state, fi);
            let fsize = struct_field_size(ft);
            let f_off = row + smap.field_offset(state, fi);
            prior.cells[fi as usize] =
                pack_field_bytes(&state[f_off as usize..(f_off + fsize) as usize]);
        }
    }
}

/// vm.zig:604 `emitStructMapRowJournal` — STRUCT_MAP_ROW first (authoritative
/// prior bitset runs LAST in reverse rollback), then per-field entries.
#[allow(clippy::too_many_arguments)]
fn emit_struct_map_row_journal(
    undo: &mut UndoState,
    delta_mode: bool,
    state: &[u8],
    smap: &StructMapSlot,
    slot_idx: u8,
    key: u32,
    pos: u32,
    was_new: bool,
    prior: &RowPrior,
) {
    let row = smap.row_off(pos);

    let smr = |pad2: u8, aux: u64| FlatUndoEntry {
        op: FlatUndoOp::StructMapRow,
        slot: slot_idx,
        pad1: 0,
        pad2,
        key,
        prev_value: 0,
        aux,
    };
    if was_new {
        append_mutation_state(
            undo,
            delta_mode,
            state,
            smr(SMR_ROW_ABSENT, 0),
            smr(0, pack_bitset_bytes(state, row, smap.bitset_bytes)),
        );
    } else {
        append_mutation_state(
            undo,
            delta_mode,
            state,
            smr(
                0,
                pack_field_bytes(&prior.bitset[..smap.bitset_bytes.min(8) as usize]),
            ),
            smr(0, pack_bitset_bytes(state, row, smap.bitset_bytes)),
        );
    }

    let n_fields = smap.num_fields.min(64);
    for fi in 0..n_fields {
        let prior_set = !was_new && prior.is_field_set(fi);
        let now_set = StructMapSlot::is_field_set(state, row, fi);
        if !prior_set && !now_set {
            continue;
        }

        let ft = smap.field_type(state, fi);
        let fsize = struct_field_size(ft);
        let f_off = row + smap.field_offset(state, fi);

        let smf = |pad2: u8, aux: u64| FlatUndoEntry {
            op: FlatUndoOp::StructMapField,
            slot: slot_idx,
            pad1: fi,
            pad2,
            key,
            prev_value: 0,
            aux,
        };
        let undo_flags = if prior_set { SMF_BIT_SET } else { 0 };
        let undo_aux = if prior_set {
            prior.cells[fi as usize]
        } else {
            0
        };
        let redo_flags = if now_set { SMF_BIT_SET } else { 0 };
        let redo_aux = if now_set {
            pack_field_bytes(&state[f_off as usize..(f_off + fsize) as usize])
        } else {
            0
        };
        append_mutation_state(
            undo,
            delta_mode,
            state,
            smf(undo_flags, undo_aux),
            smf(redo_flags, redo_aux),
        );
    }
}

fn capture_struct_map2_row_prior(
    state: &[u8],
    smap: &StructMap2Slot,
    row: u32,
    prior: &mut RowPrior,
) {
    let nb = smap.bitset_bytes.min(8) as usize;
    prior.bitset = [0; 8];
    prior.bitset[..nb].copy_from_slice(&state[row as usize..row as usize + nb]);
    for fi in 0..smap.num_fields.min(64) {
        if StructMapSlot::is_field_set(state, row, fi) {
            let fsize = struct_field_size(smap.field_type(state, fi));
            let f_off = row + smap.field_offset(state, fi);
            prior.cells[fi as usize] =
                pack_field_bytes(&state[f_off as usize..(f_off + fsize) as usize]);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_struct_map2_upsert_journal(
    undo: &mut UndoState,
    delta_mode: bool,
    state: &[u8],
    smap: &StructMap2Slot,
    slot: u8,
    key1: u32,
    key2: u32,
    pos: u32,
    was_new: bool,
    prior: &RowPrior,
) {
    let row = smap.row_off(pos);
    let row_entry = |pad2: u8, aux: u64| FlatUndoEntry {
        op: FlatUndoOp::StructMapRow,
        slot,
        pad1: 0,
        pad2,
        key: key1,
        prev_value: key2,
        aux,
    };
    let prior_bits = if was_new {
        0
    } else {
        pack_field_bytes(&prior.bitset[..smap.bitset_bytes.min(8) as usize])
    };
    append_mutation_state(
        undo,
        delta_mode,
        state,
        row_entry(if was_new { SMR_ROW_ABSENT } else { 0 }, prior_bits),
        row_entry(0, pack_bitset_bytes(state, row, smap.bitset_bytes)),
    );

    for fi in 0..smap.num_fields.min(64) {
        let prior_set = !was_new && prior.is_field_set(fi);
        let now_set = StructMapSlot::is_field_set(state, row, fi);
        if !prior_set && !now_set {
            continue;
        }
        let fsize = struct_field_size(smap.field_type(state, fi));
        let f_off = row + smap.field_offset(state, fi);
        let field_entry = |pad2: u8, aux: u64| FlatUndoEntry {
            op: FlatUndoOp::StructMapField,
            slot,
            pad1: fi,
            pad2,
            key: key1,
            prev_value: key2,
            aux,
        };
        append_mutation_state(
            undo,
            delta_mode,
            state,
            field_entry(
                if prior_set { SMF_BIT_SET } else { 0 },
                if prior_set {
                    prior.cells[fi as usize]
                } else {
                    0
                },
            ),
            field_entry(
                if now_set { SMF_BIT_SET } else { 0 },
                if now_set {
                    pack_field_bytes(&state[f_off as usize..(f_off + fsize) as usize])
                } else {
                    0
                },
            ),
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_struct_map2_remove_journal(
    undo: &mut UndoState,
    delta_mode: bool,
    state: &[u8],
    smap: &StructMap2Slot,
    slot: u8,
    key1: u32,
    key2: u32,
    prior: &RowPrior,
) {
    let entry = |op: FlatUndoOp, field: u8, flags: u8, aux: u64| FlatUndoEntry {
        op,
        slot,
        pad1: field,
        pad2: flags,
        key: key1,
        prev_value: key2,
        aux,
    };
    append_mutation_state(
        undo,
        delta_mode,
        state,
        entry(
            FlatUndoOp::StructMapRow,
            0,
            0,
            pack_field_bytes(&prior.bitset[..smap.bitset_bytes.min(8) as usize]),
        ),
        entry(FlatUndoOp::StructMapRow, 0, SMR_ROW_ABSENT, 0),
    );
    for fi in 0..smap.num_fields.min(64) {
        if !prior.is_field_set(fi) {
            continue;
        }
        append_mutation_state(
            undo,
            delta_mode,
            state,
            entry(
                FlatUndoOp::StructMapField,
                fi,
                SMF_BIT_SET,
                prior.cells[fi as usize],
            ),
            entry(FlatUndoOp::StructMapField, fi, SMF_ROW_ABSENT, 0),
        );
    }
}

/// vm.zig:301 `appendMutation` where the caller owns the state buffer (so
/// first-overflow snapshots can happen, exactly like the Zig global-pointer
/// path).
fn append_mutation_state(
    undo: &mut UndoState,
    delta_mode: bool,
    state: &[u8],
    undo_e: FlatUndoEntry,
    redo_e: FlatUndoEntry,
) {
    if delta_mode {
        undo.append_pair_snapshot(state, undo_e, redo_e);
    } else {
        undo.append_undo_only_snapshot(state, undo_e);
    }
}

fn nested_journal_ranges(state: &[u8], meta: &SlotMetaView) -> [(u32, u32); 2] {
    let arena_header = nested::arena_header_offset(meta.offset, meta.capacity);
    let arena_capacity = bytes::read_u32(state, arena_header);
    let slot_data_len = arena_header + nested::ARENA_HDR_SIZE + arena_capacity - meta.offset;
    [
        (meta.meta_base, SLOT_META_SIZE),
        (meta.offset, slot_data_len),
    ]
}

// =============================================================================
// Single-element struct-map operations (vm.zig:1072-1273)
// =============================================================================

struct StructUpsertResult {
    err: ErrorCode,
    pos: u32,
}

const MAX_STRUCT_SCALAR_OPERANDS: usize = 32;
const MAX_STRUCT_ARRAY_OPERANDS: usize = 16;

#[derive(Clone, Copy)]
struct StructMapUpsertOperands {
    slot: u8,
    key_col: u8,
    num_vals: usize,
    scalar_pairs_start: usize,
    num_array_vals: usize,
    array_triples_start: usize,
    comparison_field_idx: Option<u8>,
    end: usize,
}

/// Decode the shared 0x80/0x81 row operands and 0x82's trailing comparison
/// ordinal without mutating state. Fixed operand arrays in both dispatch paths
/// make their encoded maxima part of the accepted-program contract.
fn decode_struct_map_upsert_operands(
    code: &[u8],
    start: usize,
    has_comparison: bool,
) -> Option<StructMapUpsertOperands> {
    let header_end = start.checked_add(3)?;
    let header = code.get(start..header_end)?;
    let num_vals = usize::from(header[2]);
    if num_vals > MAX_STRUCT_SCALAR_OPERANDS {
        return None;
    }

    let scalar_pairs_start = header_end;
    let array_count_at = scalar_pairs_start.checked_add(num_vals.checked_mul(2)?)?;
    let num_array_vals = usize::from(*code.get(array_count_at)?);
    if num_array_vals > MAX_STRUCT_ARRAY_OPERANDS {
        return None;
    }

    let array_triples_start = array_count_at.checked_add(1)?;
    let comparison_at = array_triples_start.checked_add(num_array_vals.checked_mul(3)?)?;
    let (comparison_field_idx, end) = if has_comparison {
        (
            Some(*code.get(comparison_at)?),
            comparison_at.checked_add(1)?,
        )
    } else {
        (None, comparison_at)
    };

    Some(StructMapUpsertOperands {
        slot: header[0],
        key_col: header[1],
        num_vals,
        scalar_pairs_start,
        num_array_vals,
        array_triples_start,
        comparison_field_idx,
        end,
    })
}

#[derive(Clone, Copy)]
struct StructMap2UpsertOperands {
    slot: u8,
    key1_col: u8,
    key2_col: u8,
    num_vals: usize,
    scalar_pairs_start: usize,
    end: usize,
}

fn decode_struct_map2_upsert_operands(
    code: &[u8],
    start: usize,
) -> Option<StructMap2UpsertOperands> {
    let header_end = start.checked_add(4)?;
    let header = code.get(start..header_end)?;
    let num_vals = usize::from(header[3]);
    if num_vals > MAX_STRUCT_SCALAR_OPERANDS {
        return None;
    }
    let end = header_end.checked_add(num_vals.checked_mul(2)?)?;
    code.get(header_end..end)?;
    Some(StructMap2UpsertOperands {
        slot: header[0],
        key1_col: header[1],
        key2_col: header[2],
        num_vals,
        scalar_pairs_start: header_end,
        end,
    })
}

#[derive(Clone, Copy)]
struct StructMap2MaxI64x2Operands {
    row: StructMap2UpsertOperands,
    cmp1_col: u8,
    cmp1_field: u8,
    cmp2_col: u8,
    cmp2_field: u8,
    end: usize,
}

fn decode_struct_map2_max_i64x2_operands(
    code: &[u8],
    start: usize,
) -> Option<StructMap2MaxI64x2Operands> {
    let row = decode_struct_map2_upsert_operands(code, start)?;
    if row.num_vals > MAX_STRUCT_SCALAR_OPERANDS - 2 {
        return None;
    }
    let end = row.end.checked_add(4)?;
    let comparison = code.get(row.end..end)?;
    Some(StructMap2MaxI64x2Operands {
        row,
        cmp1_col: comparison[0],
        cmp1_field: comparison[1],
        cmp2_col: comparison[2],
        cmp2_field: comparison[3],
        end,
    })
}

fn validate_struct_map2_max_i64x2(
    state: &[u8],
    smap: &StructMap2Slot,
    code: &[u8],
    operands: StructMap2MaxI64x2Operands,
) -> bool {
    if operands.cmp1_field == operands.cmp2_field
        || operands.cmp1_field >= smap.num_fields
        || operands.cmp2_field >= smap.num_fields
        || smap.field_type(state, operands.cmp1_field) != StructFieldType::Int64
        || smap.field_type(state, operands.cmp2_field) != StructFieldType::Int64
    {
        return false;
    }
    let pairs_end = operands.row.scalar_pairs_start + operands.row.num_vals * 2;
    code[operands.row.scalar_pairs_start..pairs_end]
        .chunks_exact(2)
        .all(|pair| {
            pair[1] < smap.num_fields
                && pair[1] != operands.cmp1_field
                && pair[1] != operands.cmp2_field
        })
}

fn should_upsert_struct_map2_max_i64x2(
    state: &[u8],
    smap: &StructMap2Slot,
    key1: u32,
    key2: u32,
    operands: StructMap2MaxI64x2Operands,
    cols: &[&[u8]],
    element_idx: u32,
) -> bool {
    let Some(pos) = smap.find(state, key1, key2) else {
        return true;
    };
    let row = smap.row_off(pos);
    if !StructMapSlot::is_field_set(state, row, operands.cmp1_field)
        || !StructMapSlot::is_field_set(state, row, operands.cmp2_field)
    {
        return true;
    }
    let existing = (
        bytes::read_i64(state, row + smap.field_offset(state, operands.cmp1_field)),
        bytes::read_i64(state, row + smap.field_offset(state, operands.cmp2_field)),
    );
    let comparison_offset = element_idx * 8;
    let candidate = (
        bytes::read_i64(
            col_at(cols, usize::from(operands.cmp1_col)),
            comparison_offset,
        ),
        bytes::read_i64(
            col_at(cols, usize::from(operands.cmp2_col)),
            comparison_offset,
        ),
    );
    candidate > existing
}

#[derive(Clone, Copy)]
struct StructMapMaxComparison {
    field_idx: u8,
    col: u8,
    field_type: StructFieldType,
    cmp_type: CmpType,
}

/// Resolve and validate 0x82's comparison lane before processing any rows.
/// The comparison ordinal must name a mapped scalar field.
fn resolve_struct_map_max_comparison(
    state: &[u8],
    smap: &StructMapSlot,
    scalar_pairs: &[u8],
    comparison_field_idx: u8,
) -> Option<StructMapMaxComparison> {
    if comparison_field_idx >= smap.num_fields {
        return None;
    }
    let field_type = StructFieldType::from_u8(smap.field_type_byte(state, comparison_field_idx))?;
    let cmp_type = match field_type {
        StructFieldType::UInt32 | StructFieldType::String | StructFieldType::Bool => CmpType::U32,
        StructFieldType::Int64 => CmpType::I64,
        StructFieldType::Float64 => CmpType::F64,
        StructFieldType::ArrayU32
        | StructFieldType::ArrayI64
        | StructFieldType::ArrayF64
        | StructFieldType::ArrayString
        | StructFieldType::ArrayBool => return None,
    };
    let col = scalar_pairs
        .chunks_exact(2)
        .find_map(|pair| (pair[1] == comparison_field_idx).then_some(pair[0]))?;
    Some(StructMapMaxComparison {
        field_idx: comparison_field_idx,
        col,
        field_type,
        cmp_type,
    })
}

/// Decide a struct-map arg-max before bitset clearing, journaling, row writes,
/// or change flags. Missing keys or missing stored comparison fields accept;
/// equal and lower values are true no-ops.
fn should_upsert_struct_map_max(
    state: &[u8],
    smap: &StructMapSlot,
    key: u32,
    comparison: StructMapMaxComparison,
    cols: &[&[u8]],
    element_idx: u32,
) -> bool {
    let Some(position) = smap.find(state, key) else {
        return true;
    };
    let row = smap.row_off(position);
    if !StructMapSlot::is_field_set(state, row, comparison.field_idx) {
        return true;
    }

    let field_offset = row + smap.field_offset(state, comparison.field_idx);
    let existing = match comparison.field_type {
        StructFieldType::UInt32 | StructFieldType::String => {
            u64::from(bytes::read_u32(state, field_offset))
        }
        StructFieldType::Bool => {
            u64::from(state[usize::try_from(field_offset).expect("state offset fits usize")])
        }
        StructFieldType::Int64 | StructFieldType::Float64 => bytes::read_u64(state, field_offset),
        StructFieldType::ArrayU32
        | StructFieldType::ArrayI64
        | StructFieldType::ArrayF64
        | StructFieldType::ArrayString
        | StructFieldType::ArrayBool => unreachable!("comparison kind validated as scalar"),
    };
    let incoming = hashmap_ops::read_cmp_value(
        col_at(cols, usize::from(comparison.col)),
        element_idx,
        comparison.cmp_type,
    );
    hashmap_ops::cmp_gt(incoming, existing, comparison.cmp_type)
}

/// vm.zig:1080 `singleStructMapUpsertLast`.
#[allow(clippy::too_many_arguments)]
fn single_struct_map_upsert_last(
    undo: &mut UndoState,
    delta_mode: bool,
    state: &mut [u8],
    slot_idx: u8,
    key: u32,
    val_cols: &[u8],
    field_idxs: &[u8],
    cols: &[&[u8]],
    element_idx: u32,
) -> StructUpsertResult {
    if key == EMPTY_KEY || key == TOMBSTONE {
        return StructUpsertResult {
            err: ErrorCode::Ok,
            pos: 0,
        };
    }

    let smap = StructMapSlot::bind(state, slot_idx);
    let Some(result) = smap.upsert(state, key) else {
        if delta_mode {
            let meta_base = slot_meta_base(slot_idx);
            state[(meta_base + SlotMetaOffset::CHANGE_FLAGS) as usize] |= ChangeFlag::INSERTED;
        }
        NEEDS_GROWTH_SLOT.store(slot_idx, Ordering::Relaxed);
        return StructUpsertResult {
            err: ErrorCode::CapacityExceeded,
            pos: 0,
        };
    };

    let row = smap.row_off(result.pos);

    // vm.zig:1106 — capture the prior row BEFORE clearBitset wipes it; small
    // (non-overflow) batches have no shadow snapshot to fall back on.
    let mut prior = RowPrior::new();
    if undo.enabled && !result.is_new {
        capture_struct_map_row_prior(state, &smap, row, &mut prior);
    }

    smap.clear_bitset(state, row);

    for (vi, &val_col) in val_cols.iter().enumerate() {
        smap.write_scalar_field(
            state,
            result.pos,
            field_idxs[vi],
            cols,
            val_col,
            element_idx,
        );
    }

    if undo.enabled {
        emit_struct_map_row_journal(
            undo,
            delta_mode,
            state,
            &smap,
            slot_idx,
            key,
            result.pos,
            result.is_new,
            &prior,
        );
    }

    if delta_mode {
        let meta_base = slot_meta_base(slot_idx);
        state[(meta_base + SlotMetaOffset::CHANGE_FLAGS) as usize] |= ChangeFlag::INSERTED;
    }
    StructUpsertResult {
        err: ErrorCode::Ok,
        pos: result.pos,
    }
}

#[allow(clippy::too_many_arguments)]
fn single_struct_map2_upsert_last(
    undo: &mut UndoState,
    delta_mode: bool,
    state: &mut [u8],
    slot: u8,
    key1: u32,
    key2: u32,
    val_cols: &[u8],
    field_idxs: &[u8],
    cols: &[&[u8]],
    element_idx: u32,
) -> StructUpsertResult {
    if key1 == EMPTY_KEY || key1 == TOMBSTONE {
        return StructUpsertResult {
            err: ErrorCode::InvalidKey,
            pos: 0,
        };
    }
    let smap = StructMap2Slot::bind(state, slot);
    if field_idxs.iter().any(|&field| field >= smap.num_fields) {
        return StructUpsertResult {
            err: ErrorCode::InvalidProgram,
            pos: 0,
        };
    }
    let Some(result) = smap.upsert(state, key1, key2) else {
        NEEDS_GROWTH_SLOT.store(slot, Ordering::Relaxed);
        return StructUpsertResult {
            err: ErrorCode::CapacityExceeded,
            pos: 0,
        };
    };
    let row = smap.row_off(result.pos);
    let mut prior = RowPrior::new();
    if undo.enabled && !result.is_new {
        capture_struct_map2_row_prior(state, &smap, row, &mut prior);
    }
    smap.clear_bitset(state, row);
    for (index, &val_col) in val_cols.iter().enumerate() {
        smap.write_scalar_field(
            state,
            result.pos,
            field_idxs[index],
            cols,
            val_col,
            element_idx,
        );
    }
    if undo.enabled {
        emit_struct_map2_upsert_journal(
            undo,
            delta_mode,
            state,
            &smap,
            slot,
            key1,
            key2,
            result.pos,
            result.is_new,
            &prior,
        );
    }
    let meta_base = slot_meta_base(slot);
    state[(meta_base + SlotMetaOffset::CHANGE_FLAGS) as usize] |= if result.is_new {
        ChangeFlag::INSERTED
    } else {
        ChangeFlag::UPDATED
    };
    StructUpsertResult {
        err: ErrorCode::Ok,
        pos: result.pos,
    }
}

#[allow(clippy::too_many_arguments)]
fn single_struct_map2_upsert_max_i64x2(
    undo: &mut UndoState,
    delta_mode: bool,
    state: &mut [u8],
    code: &[u8],
    operands: StructMap2MaxI64x2Operands,
    key1: u32,
    key2: u32,
    cols: &[&[u8]],
    element_idx: u32,
) -> StructUpsertResult {
    if key1 == EMPTY_KEY || key1 == TOMBSTONE {
        return StructUpsertResult {
            err: ErrorCode::InvalidKey,
            pos: 0,
        };
    }
    let smap = StructMap2Slot::bind(state, operands.row.slot);
    if !should_upsert_struct_map2_max_i64x2(state, &smap, key1, key2, operands, cols, element_idx) {
        return StructUpsertResult {
            err: ErrorCode::Ok,
            pos: 0,
        };
    }

    let mut val_cols = [0_u8; MAX_STRUCT_SCALAR_OPERANDS];
    let mut field_idxs = [0_u8; MAX_STRUCT_SCALAR_OPERANDS];
    for i in 0..operands.row.num_vals {
        val_cols[i] = code[operands.row.scalar_pairs_start + i * 2];
        field_idxs[i] = code[operands.row.scalar_pairs_start + i * 2 + 1];
    }
    let count = operands.row.num_vals;
    val_cols[count] = operands.cmp1_col;
    field_idxs[count] = operands.cmp1_field;
    val_cols[count + 1] = operands.cmp2_col;
    field_idxs[count + 1] = operands.cmp2_field;
    single_struct_map2_upsert_last(
        undo,
        delta_mode,
        state,
        operands.row.slot,
        key1,
        key2,
        &val_cols[..count + 2],
        &field_idxs[..count + 2],
        cols,
        element_idx,
    )
}

fn single_struct_map2_remove(
    undo: &mut UndoState,
    delta_mode: bool,
    state: &mut [u8],
    slot: u8,
    key1: u32,
    key2: u32,
) -> ErrorCode {
    if key1 == EMPTY_KEY || key1 == TOMBSTONE {
        return ErrorCode::InvalidKey;
    }
    let smap = StructMap2Slot::bind(state, slot);
    let Some(pos) = smap.find(state, key1, key2) else {
        return ErrorCode::Ok;
    };
    let row = smap.row_off(pos);
    let mut prior = RowPrior::new();
    if undo.enabled {
        capture_struct_map2_row_prior(state, &smap, row, &mut prior);
    }
    let removed = smap.remove(state, key1, key2);
    debug_assert_eq!(removed, Some(pos));
    smap.clear_bitset(state, row);
    if undo.enabled {
        emit_struct_map2_remove_journal(undo, delta_mode, state, &smap, slot, key1, key2, &prior);
    }
    state[(slot_meta_base(slot) + SlotMetaOffset::CHANGE_FLAGS) as usize] |= ChangeFlag::REMOVED;
    ErrorCode::Ok
}

//#region axe!n/reduce-typed-state.probe-upsert
/// vm.zig:1139 `singleStructMapUpsertFromProbe` — copy remapped fields from a
/// probed source row into the out slot (the `.lookup` join).
#[allow(clippy::too_many_arguments)]
fn single_struct_map_upsert_from_probe(
    undo: &mut UndoState,
    delta_mode: bool,
    state: &mut [u8],
    out_slot: u8,
    out_key: u32,
    probe: &StructMapSlot,
    probe_row: u32,
    probe_field_idxs: &[u8],
    out_field_idxs: &[u8],
) -> StructUpsertResult {
    if out_key == EMPTY_KEY || out_key == TOMBSTONE {
        return StructUpsertResult {
            err: ErrorCode::Ok,
            pos: 0,
        };
    }

    let out = StructMapSlot::bind(state, out_slot);
    let Some(result) = out.upsert(state, out_key) else {
        if delta_mode {
            let meta_base = slot_meta_base(out_slot);
            state[(meta_base + SlotMetaOffset::CHANGE_FLAGS) as usize] |= ChangeFlag::INSERTED;
        }
        NEEDS_GROWTH_SLOT.store(out_slot, Ordering::Relaxed);
        return StructUpsertResult {
            err: ErrorCode::CapacityExceeded,
            pos: 0,
        };
    };

    let out_row = out.row_off(result.pos);

    let mut prior = RowPrior::new();
    if undo.enabled && !result.is_new {
        capture_struct_map_row_prior(state, &out, out_row, &mut prior);
    }

    out.clear_bitset(state, out_row);

    for (i, &probe_fi) in probe_field_idxs.iter().enumerate() {
        let out_fi = out_field_idxs[i];
        let out_ft = out.field_type(state, out_fi);
        let fsize = struct_field_size(out_ft);
        let src_off = probe_row + probe.field_offset(state, probe_fi);
        let dst_off = out_row + out.field_offset(state, out_fi);

        StructMapSlot::set_field_bit(state, out_row, out_fi);
        bytes::copy_within(state, src_off, dst_off, fsize);
    }

    if undo.enabled {
        emit_struct_map_row_journal(
            undo,
            delta_mode,
            state,
            &out,
            out_slot,
            out_key,
            result.pos,
            result.is_new,
            &prior,
        );
    }

    if delta_mode {
        let meta_base = slot_meta_base(out_slot);
        state[(meta_base + SlotMetaOffset::CHANGE_FLAGS) as usize] |= ChangeFlag::INSERTED;
    }
    StructUpsertResult {
        err: ErrorCode::Ok,
        pos: result.pos,
    }
}
//#endregion axe!n/reduce-typed-state.probe-upsert

/// vm.zig:1200 `writeStructMapArrayFields` — CSR array fields into the arena.
#[allow(clippy::too_many_arguments)]
fn write_struct_map_array_fields(
    state: &mut [u8],
    slot_idx: u8,
    row_pos: u32,
    array_offsets_cols: &[u8],
    array_values_cols: &[u8],
    array_field_idxs: &[u8],
    cols: &[&[u8]],
    child_idx: u32,
) -> ErrorCode {
    if array_offsets_cols.is_empty() {
        return ErrorCode::Ok;
    }

    let meta_base = slot_meta_base(slot_idx);
    let slot_offset = bytes::read_u32(state, meta_base + SlotMetaOffset::OFFSET);
    let capacity = bytes::read_u32(state, meta_base + SlotMetaOffset::CAPACITY);
    let num_fields = state[(meta_base + SlotMetaOffset::AGG_TYPE) as usize];
    let row_size = u32::from(bytes::read_u16(
        state,
        meta_base + SlotMetaOffset::TTL_SECONDS,
    ));
    let arena_header_off = bytes::read_u32(state, meta_base + SlotMetaOffset::GRACE_SECONDS);

    if arena_header_off == 0 {
        return ErrorCode::Ok; // vm.zig:1220 — no arena.
    }

    let descriptor_size = align8(u32::from(num_fields));
    let keys_offset = slot_offset + descriptor_size;
    let rows_base = keys_offset + capacity * 4;
    let row_off = rows_base + row_pos * row_size;

    let arena_capacity = bytes::read_u32(state, arena_header_off);
    let mut arena_used = bytes::read_u32(state, arena_header_off + 4);
    let arena_data_base = arena_header_off + ARENA_HEADER_SIZE;

    for (vi, &field_idx) in array_field_idxs.iter().enumerate() {
        let field_type_byte = state[(slot_offset + u32::from(field_idx)) as usize];
        let f_offset = {
            let descriptor =
                &state[slot_offset as usize..(slot_offset + u32::from(num_fields)) as usize];
            state_init::struct_field_offset(num_fields, descriptor, field_idx)
        };
        let elem_size = state_init::arena_elem_size_strict(field_type_byte);

        let offsets = col_u32(cols[array_offsets_cols[vi] as usize], child_idx + 2);
        let arr_start = offsets[child_idx as usize];
        let arr_end = offsets[child_idx as usize + 1];
        let arr_len = arr_end - arr_start;
        let byte_len = arr_len * elem_size;

        if arena_used + byte_len > arena_capacity {
            NEEDS_GROWTH_SLOT.store(slot_idx, Ordering::Relaxed);
            return ErrorCode::ArenaOverflow;
        }

        state[(row_off + u32::from(field_idx) / 8) as usize] |= 1 << (field_idx % 8);

        bytes::write_u32(state, row_off + f_offset, arena_used);
        bytes::write_u32(state, row_off + f_offset + 4, arr_len);

        if byte_len > 0 {
            let src = cols[array_values_cols[vi] as usize];
            let src_off = (arr_start * elem_size) as usize;
            let dst_off = (arena_data_base + arena_used) as usize;
            state[dst_off..dst_off + byte_len as usize]
                .copy_from_slice(&src[src_off..src_off + byte_len as usize]);
        }

        arena_used += byte_len;
    }

    bytes::write_u32(state, arena_header_off + 4, arena_used);
    ErrorCode::Ok
}

// =============================================================================
// Aggregate execution (vm.zig:1297-1329)
// =============================================================================

/// vm.zig:1297 `execAgg` for f64 slots (16-byte value+count layout). The
/// full u64 count rides prev_value (low) + key (high) journal lanes.
#[allow(clippy::too_many_arguments)]
fn exec_agg_f64(
    undo: &mut UndoState,
    delta_mode: bool,
    state: &mut [u8],
    slot: u8,
    kind: AggKind,
    vals: &[f64],
    type_mask: Option<TypeMask<'_>>,
    pred_col: Option<&[u32]>,
) {
    let meta = SlotMetaView::read(state, slot);
    let old_val = bytes::read_f64(state, meta.offset);
    let count = bytes::read_u64(state, meta.offset + 8);

    let new_val = aggregates::reduce_col_f64(kind, vals, old_val, type_mask, pred_col);

    // Float compare exactly like Zig `!=` (a NaN old/new always "changes").
    #[allow(clippy::float_cmp)]
    if new_val != old_val {
        if undo.enabled {
            let agg = |aux: u64| FlatUndoEntry {
                op: FlatUndoOp::AggUpdate,
                slot,
                pad1: 0,
                pad2: 0,
                // Full u64 count: low half in prev_value, high half in the
                // otherwise-unused key lane (the deleted Zig truncated to
                // u32, so counts past 4.29e9 rolled back wrong).
                key: (count >> 32) as u32,
                prev_value: count as u32,
                aux,
            };
            append_mutation_state(
                undo,
                delta_mode,
                state,
                agg(old_val.to_bits()),
                agg(new_val.to_bits()),
            );
        }
        bytes::write_f64(state, meta.offset, new_val);
        meta.set_change_flag(state, ChangeFlag::SIZE_CHANGED);
    }
}

/// vm.zig:1297 `execAgg` for i64 slots.
#[allow(clippy::too_many_arguments)]
fn exec_agg_i64(
    undo: &mut UndoState,
    delta_mode: bool,
    state: &mut [u8],
    slot: u8,
    kind: AggKind,
    vals: &[i64],
    type_mask: Option<TypeMask<'_>>,
    pred_col: Option<&[u32]>,
) {
    let meta = SlotMetaView::read(state, slot);
    let old_val = bytes::read_i64(state, meta.offset);
    let count = bytes::read_u64(state, meta.offset + 8);

    let new_val = aggregates::reduce_col_i64(kind, vals, old_val, type_mask, pred_col);

    if new_val != old_val {
        if undo.enabled {
            let agg = |aux: u64| FlatUndoEntry {
                op: FlatUndoOp::AggUpdate,
                slot,
                pad1: 0,
                pad2: 0,
                // Full u64 count: low half in prev_value, high half in the
                // otherwise-unused key lane (the deleted Zig truncated to
                // u32, so counts past 4.29e9 rolled back wrong).
                key: (count >> 32) as u32,
                prev_value: count as u32,
                aux,
            };
            append_mutation_state(
                undo,
                delta_mode,
                state,
                agg(old_val as u64),
                agg(new_val as u64),
            );
        }
        bytes::write_i64(state, meta.offset, new_val);
        meta.set_change_flag(state, ChangeFlag::SIZE_CHANGED);
    }
}

/// Shared COUNT-slot bump (vm.zig BATCH_AGG_COUNT / 0x41 / 0x45 bodies).
fn exec_agg_count(
    undo: &mut UndoState,
    delta_mode: bool,
    state: &mut [u8],
    slot: u8,
    matched: u64,
) {
    let meta = SlotMetaView::read(state, slot);
    if matched == 0 {
        return;
    }
    let prev_count = bytes::read_u64(state, meta.offset);
    let next_count = prev_count + matched;
    if undo.enabled {
        // Full u64 count: prev_value low + key high lanes (see undo_log).
        let cu = |prev: u64| FlatUndoEntry {
            op: FlatUndoOp::CountUpdate,
            slot,
            pad1: 0,
            pad2: 0,
            key: (prev >> 32) as u32,
            prev_value: prev as u32,
            aux: 0,
        };
        append_mutation_state(undo, delta_mode, state, cu(prev_count), cu(next_count));
    }
    bytes::write_u64(state, meta.offset, next_count);
    meta.set_change_flag(state, ChangeFlag::SIZE_CHANGED);
}

/// vm.zig BATCH_SCALAR_LATEST core (top-level and 0x48 body share it via the
/// optional type mask). Unknown scalar subtypes are always INVALID_PROGRAM.
///
/// Scalar writes ARE undo-journaled (ScalarUpdate, one undo/redo pair per
/// call when the slot changed) — the deleted Zig journaled nothing here, so
/// a speculative BATCH_SCALAR_LATEST mutation survived rollback unless the
/// overflow shadow snapshot happened to cover it.
#[allow(clippy::too_many_arguments)]
fn exec_scalar_latest(
    undo: &mut UndoState,
    delta_mode: bool,
    state: &mut [u8],
    slot: u8,
    val_col: &[u8],
    cmp_vals: &[f64],
    batch_len: u32,
    type_mask: Option<(&[u32], u32)>,
) -> ErrorCode {
    let meta = SlotMetaView::read(state, slot);
    let data = meta.offset;
    let scalar_type = meta.agg_type_byte(state);
    if meta.slot_type() != SlotType::Scalar || !matches!(scalar_type, 8..=10) {
        return ErrorCode::InvalidProgram;
    }
    let prev_value = bytes::read_u64(state, data);
    let prev_ts = bytes::read_f64(state, data + 8);

    // AggType: SCALAR_U32 = 8, SCALAR_F64 = 9, SCALAR_I64 = 10 (types.zig:214-216;
    // 6-7 are the reserved gap — a prior misread as 5/6/7 made every scalar op a
    // silent no-op, self-confirmed by tests built from the same wrong constants
    // and caught only by the TS parity suite).
    let matches = |i: usize| type_mask.is_none_or(|(td, id)| td[i] == id);
    match scalar_type {
        8 => {
            let vals = col_u32(val_col, batch_len);
            for i in 0..batch_len as usize {
                let ts = cmp_vals[i];
                if matches(i) && ts > bytes::read_f64(state, data + 8) && vals[i] != EMPTY_KEY {
                    bytes::write_u32(state, data, vals[i]);
                    bytes::write_f64(state, data + 8, ts);
                    meta.set_change_flag(state, ChangeFlag::UPDATED);
                }
            }
        }
        9 => {
            let vals = col_f64(val_col, batch_len);
            for i in 0..batch_len as usize {
                let ts = cmp_vals[i];
                if matches(i) && ts > bytes::read_f64(state, data + 8) {
                    bytes::write_f64(state, data, vals[i]);
                    bytes::write_f64(state, data + 8, ts);
                    meta.set_change_flag(state, ChangeFlag::UPDATED);
                }
            }
        }
        10 => {
            let vals = col_i64(val_col, batch_len);
            for i in 0..batch_len as usize {
                let ts = cmp_vals[i];
                if matches(i) && ts > bytes::read_f64(state, data + 8) {
                    bytes::write_i64(state, data, vals[i]);
                    bytes::write_f64(state, data + 8, ts);
                    meta.set_change_flag(state, ChangeFlag::UPDATED);
                }
            }
        }
        _ => unreachable!("scalar subtype validated before execution"),
    }

    if undo.enabled {
        let next_value = bytes::read_u64(state, data);
        let next_ts = bytes::read_f64(state, data + 8);
        if next_value != prev_value || next_ts.to_bits() != prev_ts.to_bits() {
            // value bits ride aux; timestamp bits ride prev_value (low) +
            // key (high) — same split as the widened count lanes.
            let su = |value: u64, ts: f64| FlatUndoEntry {
                op: FlatUndoOp::ScalarUpdate,
                slot,
                pad1: 0,
                pad2: 0,
                key: (ts.to_bits() >> 32) as u32,
                prev_value: ts.to_bits() as u32,
                aux: value,
            };
            append_mutation_state(
                undo,
                delta_mode,
                state,
                su(prev_value, prev_ts),
                su(next_value, next_ts),
            );
        }
    }
    ErrorCode::Ok
}

// =============================================================================
// Block-based execution tables (vm.zig:1869-1984)
// =============================================================================

/// vm.zig:1870 `isAggregateOp` — 0x40-0x4F.
const fn is_aggregate_op(op_byte: u8) -> bool {
    op_byte & 0xF0 == 0x40
}

/// vm.zig:1875 `aggOpLen`.
const fn agg_op_len(op_byte: u8) -> u32 {
    match op_byte {
        0x40 => 3,
        0x41 => 2,
        0x42 | 0x43 => 3,
        0x44 => 4,
        0x45 => 3,
        0x46..=0x48 => 4,
        0x49..=0x4b => 3,
        _ => 2, // conservative fallback (vm.zig:1889)
    }
}

/// vm.zig:1895 `bodyOpLen` — length (incl. opcode) of a non-agg body op.
fn body_op_len(code: &[u8], pc: usize) -> Option<usize> {
    let op = Opcode::from_u8(*code.get(pc)?)?;
    let len = match op {
        Opcode::BatchMapUpsertLatest | Opcode::BatchMapUpsertLatestTtl => 6,
        Opcode::BatchMapUpsertFirst | Opcode::BatchMapUpsertLast => 4,
        Opcode::BatchMapRemove => 3,
        Opcode::BatchMapUpsertLastTtl => 5,
        Opcode::BatchMapUpsertMax | Opcode::BatchMapUpsertMin => 6,
        Opcode::BatchMapUpsertLatestIf => 7,
        Opcode::BatchMapUpsertFirstIf | Opcode::BatchMapUpsertLastIf => 5,
        Opcode::BatchMapRemoveIf => 4,
        Opcode::BatchMapUpsertMaxIf | Opcode::BatchMapUpsertMinIf => 7,
        //#region axe!n/reduce-typed-state.probe-len
        Opcode::BatchStructMapProbe => {
            let num_fields = usize::from(*code.get(pc.checked_add(5)?)?);
            6usize
                .checked_add(num_fields.checked_mul(2)?)?
                .checked_add(1)?
        }
        //#endregion axe!n/reduce-typed-state.probe-len
        //#region axe!n/reduce-typed-state.scatter-len
        Opcode::BatchStructMapProbeScatter => {
            let num_routes = usize::from(*code.get(pc.checked_add(6)?)?);
            7usize.checked_add(num_routes.checked_mul(5)?)?
        }
        //#endregion axe!n/reduce-typed-state.scatter-len
        Opcode::BatchSetInsert
        | Opcode::BatchSetRemove
        | Opcode::BatchBitmapAdd
        | Opcode::BatchBitmapRemove => 3,
        Opcode::BatchSetInsertTtl | Opcode::BatchSetInsertIf => 4,
        Opcode::BatchAggSum | Opcode::BatchAggMin | Opcode::BatchAggMax => 3,
        Opcode::BatchAggCount => 2,
        Opcode::BatchAggSumIf => 4,
        Opcode::BatchAggCountIf => 3,
        Opcode::BatchAggMinIf | Opcode::BatchAggMaxIf | Opcode::BatchScalarLatest => 4,
        Opcode::BatchAggSumI64 | Opcode::BatchAggMinI64 | Opcode::BatchAggMaxI64 => 3,
        Opcode::BatchStructMapUpsertLast
        | Opcode::BatchStructMapUpsertFirst
        | Opcode::BatchStructMapUpsertMax => {
            let operands = decode_struct_map_upsert_operands(
                code,
                pc.checked_add(1)?,
                op == Opcode::BatchStructMapUpsertMax,
            )?;
            operands.end.checked_sub(pc)?
        }
        Opcode::BatchStructMap2UpsertLast => {
            let operands = decode_struct_map2_upsert_operands(code, pc.checked_add(1)?)?;
            operands.end.checked_sub(pc)?
        }
        Opcode::BatchStructMap2UpsertMaxI64x2 => {
            let operands = decode_struct_map2_max_i64x2_operands(code, pc.checked_add(1)?)?;
            operands.end.checked_sub(pc)?
        }
        Opcode::ListAppend => 3,
        Opcode::BatchStructMap2Remove => 4,
        Opcode::ListAppendStruct => {
            let num_vals = usize::from(*code.get(pc.checked_add(2)?)?);
            3usize.checked_add(num_vals.checked_mul(2)?)?
        }
        Opcode::FlatMap => {
            let low = usize::from(*code.get(pc.checked_add(3)?)?);
            let high = usize::from(*code.get(pc.checked_add(4)?)?);
            5usize.checked_add(low | (high << 8))?
        }
        Opcode::NestedSetInsert => 4,
        Opcode::NestedMapUpsertLast => 5,
        Opcode::NestedAggUpdate => 4,
        _ => return None,
    };
    let end = pc.checked_add(len)?;
    (end <= code.len()).then_some(len)
}

fn validate_body(state: &[u8], body: &[u8]) -> bool {
    let mut pc = 0usize;
    while pc < body.len() {
        let Some(op) = Opcode::from_u8(body[pc]) else {
            return false;
        };
        let Some(len) = body_op_len(body, pc) else {
            return false;
        };

        if op == Opcode::BatchScalarLatest {
            let Some(&slot) = body.get(pc + 1) else {
                return false;
            };
            let num_slots = state[StateHeaderOffset::NUM_SLOTS as usize];
            if slot >= num_slots {
                return false;
            }
            let meta = SlotMetaView::read(state, slot);
            let scalar_type = meta.agg_type_byte(state);
            if meta.slot_type() != SlotType::Scalar || !matches!(scalar_type, 8..=10) {
                return false;
            }
        }

        if op == Opcode::FlatMap {
            let Some(inner_start) = pc.checked_add(5) else {
                return false;
            };
            let Some(inner_len) = len.checked_sub(5) else {
                return false;
            };
            let Some(inner_end) = inner_start.checked_add(inner_len) else {
                return false;
            };
            if !validate_body(state, &body[inner_start..inner_end]) {
                return false;
            }
        }

        pc += len;
    }
    true
}

// =============================================================================
// Dispatch (vm.zig:1346-3288)
// =============================================================================

const OK: u32 = ErrorCode::Ok as u32;
const INVALID_STATE: u32 = ErrorCode::InvalidState as u32;
const INVALID_PROGRAM: u32 = ErrorCode::InvalidProgram as u32;
const NEEDS_GROWTH: u32 = ErrorCode::NeedsGrowth as u32;

/// vm.zig:1406 `signalGrowth` — CAPACITY_EXCEEDED → NEEDS_GROWTH + slot.
fn signal_growth(slot_idx: u8, result: ErrorCode) -> u32 {
    if result == ErrorCode::CapacityExceeded {
        NEEDS_GROWTH_SLOT.store(slot_idx, Ordering::Relaxed);
        return NEEDS_GROWTH;
    }
    result as u32
}

impl Vm {
    /// vm.zig:3266 `vm_execute_batch`.
    pub fn execute_batch(
        &mut self,
        state: &mut [u8],
        program: &[u8],
        cols: &[&[u8]],
        batch_len: u32,
    ) -> u32 {
        self.execute_impl(false, state, program, cols, batch_len)
    }

    /// vm.zig:3278 `vm_execute_batch_delta`.
    pub fn execute_batch_delta(
        &mut self,
        state: &mut [u8],
        program: &[u8],
        cols: &[&[u8]],
        batch_len: u32,
    ) -> u32 {
        self.execute_impl(true, state, program, cols, batch_len)
    }

    /// vm.zig:1003 `vm_evict_all_expired`.
    /// WHY (kept contract): on a bad state magic this returns INVALID_STATE (4) through
    /// a channel whose meaning is "total evicted count" — callers cannot
    /// distinguish "4 evicted" from "invalid state"; intended fix is a
    /// distinct error surface at the post-parity sweep.
    pub fn evict_all_expired(&mut self, state: &mut [u8], now: f64) -> u32 {
        if bytes::read_u32(state, 0) != STATE_MAGIC {
            return INVALID_STATE;
        }
        let num_slots = state[StateHeaderOffset::NUM_SLOTS as usize];
        let mut total = 0u32;
        for i in 0..num_slots {
            let meta = SlotMetaView::read(state, i);
            if meta.has_ttl() {
                clear_evicted_buffer(state, &meta);
                total += evict_expired(&mut self.undo, &mut self.bitmap_env, state, &meta, i, now);
            }
        }
        total
    }

    /// vm.zig:1346 `executeBatchImpl`.
    fn execute_impl(
        &mut self,
        delta_mode: bool,
        state: &mut [u8],
        program: &[u8],
        cols: &[&[u8]],
        batch_len: u32,
    ) -> u32 {
        if bytes::read_u32(state, 0) != STATE_MAGIC {
            return INVALID_STATE;
        }
        if (program.len() as u32) < PROGRAM_HEADER_SIZE {
            return INVALID_PROGRAM;
        }
        let content = &program[PROGRAM_HASH_PREFIX as usize..];
        if bytes::read_u32(content, 0) != PROGRAM_MAGIC {
            return INVALID_PROGRAM;
        }
        // Content header: magic(4) version(2) numSlots(1) numInputs(1)
        // reserved(2) initLen(2) reduceLen(2) = 14 bytes.
        let init_len = u32::from(bytes::read_u16(content, 10));
        let reduce_len = u32::from(bytes::read_u16(content, 12));
        if PROGRAM_HEADER_SIZE + init_len + reduce_len > program.len() as u32 {
            return INVALID_PROGRAM;
        }
        let code = &content[(14 + init_len) as usize..(14 + init_len + reduce_len) as usize];

        let mut pc = 0usize;
        while pc < code.len() {
            let op_byte = code[pc];
            pc += 1;
            // Unknown byte or a registry-only opcode both take the Zig
            // dispatch's `else` arm: INVALID_PROGRAM.
            let Some(op) = Opcode::from_u8(op_byte) else {
                return INVALID_PROGRAM;
            };

            match op {
                Opcode::Halt => break,

                Opcode::BatchMapUpsertLatest | Opcode::BatchMapUpsertLatestTtl => {
                    let (slot, key_col, val_col, ts_col) =
                        (code[pc], code[pc + 1], code[pc + 2], code[pc + 3]);
                    let Some(cmp_type) = CmpType::from_u8(code[pc + 4]) else {
                        return INVALID_PROGRAM;
                    };
                    pc += 5;
                    let meta = SlotMetaView::read(state, slot);
                    let result = hashmap_ops::batch_map_upsert(
                        Strategy::Latest,
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        col_u32(col_at(cols, key_col as usize), batch_len),
                        col_u32(col_at(cols, val_col as usize), batch_len),
                        Some(col_at(cols, ts_col as usize)),
                        cmp_type,
                        &mut self.ctx(),
                    );
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                Opcode::BatchMapUpsertFirst => {
                    let (slot, key_col, val_col) = (code[pc], code[pc + 1], code[pc + 2]);
                    pc += 3;
                    let meta = SlotMetaView::read(state, slot);
                    let result = hashmap_ops::batch_map_upsert(
                        Strategy::First,
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        col_u32(col_at(cols, key_col as usize), batch_len),
                        col_u32(col_at(cols, val_col as usize), batch_len),
                        None,
                        CmpType::F64,
                        &mut self.ctx(),
                    );
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                Opcode::BatchMapUpsertLast => {
                    let (slot, key_col, val_col) = (code[pc], code[pc + 1], code[pc + 2]);
                    pc += 3;
                    let meta = SlotMetaView::read(state, slot);
                    let ts = if meta.has_ttl() {
                        Some(col_at(cols, meta.timestamp_field_idx(state) as usize))
                    } else {
                        None
                    };
                    let result = hashmap_ops::batch_map_upsert(
                        Strategy::Last,
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        col_u32(col_at(cols, key_col as usize), batch_len),
                        col_u32(col_at(cols, val_col as usize), batch_len),
                        ts,
                        CmpType::F64,
                        &mut self.ctx(),
                    );
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                Opcode::BatchMapUpsertLastTtl => {
                    let (slot, key_col, val_col, ts_col) =
                        (code[pc], code[pc + 1], code[pc + 2], code[pc + 3]);
                    pc += 4;
                    let meta = SlotMetaView::read(state, slot);
                    let result = hashmap_ops::batch_map_upsert(
                        Strategy::Last,
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        col_u32(col_at(cols, key_col as usize), batch_len),
                        col_u32(col_at(cols, val_col as usize), batch_len),
                        Some(col_at(cols, ts_col as usize)),
                        CmpType::F64,
                        &mut self.ctx(),
                    );
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                Opcode::BatchMapRemove => {
                    let (slot, key_col) = (code[pc], code[pc + 1]);
                    pc += 2;
                    let meta = SlotMetaView::read(state, slot);
                    hashmap_ops::batch_map_remove(
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        col_u32(col_at(cols, key_col as usize), batch_len),
                        &mut self.ctx(),
                    );
                }

                Opcode::BatchMapUpsertMax | Opcode::BatchMapUpsertMin => {
                    let (slot, key_col, val_col, cmp_col) =
                        (code[pc], code[pc + 1], code[pc + 2], code[pc + 3]);
                    let Some(cmp_type) = CmpType::from_u8(code[pc + 4]) else {
                        return INVALID_PROGRAM;
                    };
                    pc += 5;
                    let strategy = if op == Opcode::BatchMapUpsertMax {
                        Strategy::Max
                    } else {
                        Strategy::Min
                    };
                    let meta = SlotMetaView::read(state, slot);
                    let result = hashmap_ops::batch_map_upsert(
                        strategy,
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        col_u32(col_at(cols, key_col as usize), batch_len),
                        col_u32(col_at(cols, val_col as usize), batch_len),
                        Some(col_at(cols, cmp_col as usize)),
                        cmp_type,
                        &mut self.ctx(),
                    );
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                Opcode::BatchSetInsert => {
                    let (slot, elem_col) = (code[pc], code[pc + 1]);
                    pc += 2;
                    let meta = SlotMetaView::read(state, slot);
                    let ts = if meta.has_ttl() {
                        Some(col_f64(
                            col_at(cols, meta.timestamp_field_idx(state) as usize),
                            batch_len,
                        ))
                    } else {
                        None
                    };
                    let result = hashset_ops::batch_set_insert(
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        col_u32(col_at(cols, elem_col as usize), batch_len),
                        ts,
                        &mut self.ctx(),
                    );
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                Opcode::BatchSetInsertTtl => {
                    let (slot, elem_col, ts_col) = (code[pc], code[pc + 1], code[pc + 2]);
                    pc += 3;
                    let meta = SlotMetaView::read(state, slot);
                    let result = hashset_ops::batch_set_insert(
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        col_u32(col_at(cols, elem_col as usize), batch_len),
                        Some(col_f64(col_at(cols, ts_col as usize), batch_len)),
                        &mut self.ctx(),
                    );
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                Opcode::BatchSetRemove => {
                    let (slot, elem_col) = (code[pc], code[pc + 1]);
                    pc += 2;
                    let meta = SlotMetaView::read(state, slot);
                    hashset_ops::batch_set_remove(
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        col_u32(col_at(cols, elem_col as usize), batch_len),
                        &mut self.ctx(),
                    );
                }

                Opcode::BatchBitmapAdd => {
                    let (slot, elem_col) = (code[pc], code[pc + 1]);
                    pc += 2;
                    let meta = SlotMetaView::read(state, slot);
                    let ts = if meta.has_ttl() {
                        Some(col_f64(
                            col_at(cols, meta.timestamp_field_idx(state) as usize),
                            batch_len,
                        ))
                    } else {
                        None
                    };
                    let (env, undo) = (&mut self.bitmap_env, &mut self.undo);
                    let mut hooks = VmCtx {
                        undo,
                        env: &mut BitmapEnv::default(),
                    };
                    let result = batch_bitmap_add(
                        env,
                        &mut hooks,
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        col_u32(col_at(cols, elem_col as usize), batch_len),
                        ts,
                    );
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                Opcode::BatchBitmapRemove => {
                    let (slot, elem_col) = (code[pc], code[pc + 1]);
                    pc += 2;
                    let meta = SlotMetaView::read(state, slot);
                    let (env, undo) = (&mut self.bitmap_env, &mut self.undo);
                    let mut hooks = VmCtx {
                        undo,
                        env: &mut BitmapEnv::default(),
                    };
                    batch_bitmap_remove(
                        env,
                        &mut hooks,
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        col_u32(col_at(cols, elem_col as usize), batch_len),
                    );
                }

                Opcode::BatchBitmapAnd
                | Opcode::BatchBitmapOr
                | Opcode::BatchBitmapAndNot
                | Opcode::BatchBitmapXor => {
                    let (target_slot, source_slot) = (code[pc], code[pc + 1]);
                    pc += 2;
                    let target_meta = SlotMetaView::read(state, target_slot);
                    let source_meta = SlotMetaView::read(state, source_slot);
                    let source_storage = get_bitmap_storage(&source_meta);
                    let source_len = source_storage.serialized_len(state);
                    let source_data = if source_len > 0 {
                        let off = source_storage.payload_offset() as usize;
                        state[off..off + source_len as usize].to_vec()
                    } else {
                        Vec::new()
                    };

                    let alg_op = match op {
                        Opcode::BatchBitmapAnd => BitmapAlgebraOp::And,
                        Opcode::BatchBitmapOr => BitmapAlgebraOp::Or,
                        Opcode::BatchBitmapAndNot => BitmapAlgebraOp::AndNot,
                        _ => BitmapAlgebraOp::Xor,
                    };
                    let (env, undo) = (&mut self.bitmap_env, &mut self.undo);
                    let mut hooks = VmCtx {
                        undo,
                        env: &mut BitmapEnv::default(),
                    };
                    let result = batch_bitmap_algebra(
                        env,
                        &mut hooks,
                        alg_op,
                        state,
                        &target_meta,
                        &source_data,
                    );
                    if result == ErrorCode::CapacityExceeded {
                        return signal_growth(target_slot, result);
                    }
                    if result != ErrorCode::Ok {
                        return result as u32;
                    }
                }

                Opcode::BatchBitmapAndScratch
                | Opcode::BatchBitmapOrScratch
                | Opcode::BatchBitmapAndNotScratch
                | Opcode::BatchBitmapXorScratch => {
                    let target_slot = code[pc];
                    pc += 1;
                    let target_meta = SlotMetaView::read(state, target_slot);
                    let source_data = self.bitmap_env.algebra_result().to_vec();

                    let alg_op = match op {
                        Opcode::BatchBitmapAndScratch => BitmapAlgebraOp::And,
                        Opcode::BatchBitmapOrScratch => BitmapAlgebraOp::Or,
                        Opcode::BatchBitmapAndNotScratch => BitmapAlgebraOp::AndNot,
                        _ => BitmapAlgebraOp::Xor,
                    };
                    let (env, undo) = (&mut self.bitmap_env, &mut self.undo);
                    let mut hooks = VmCtx {
                        undo,
                        env: &mut BitmapEnv::default(),
                    };
                    let result = batch_bitmap_algebra(
                        env,
                        &mut hooks,
                        alg_op,
                        state,
                        &target_meta,
                        &source_data,
                    );
                    if result == ErrorCode::CapacityExceeded {
                        return signal_growth(target_slot, result);
                    }
                    if result != ErrorCode::Ok {
                        return result as u32;
                    }
                }

                Opcode::BatchAggSum | Opcode::BatchAggMin | Opcode::BatchAggMax => {
                    let (slot, val_col) = (code[pc], code[pc + 1]);
                    pc += 2;
                    let kind = match op {
                        Opcode::BatchAggSum => AggKind::Sum,
                        Opcode::BatchAggMin => AggKind::Min,
                        _ => AggKind::Max,
                    };
                    exec_agg_f64(
                        &mut self.undo,
                        delta_mode,
                        state,
                        slot,
                        kind,
                        col_f64(col_at(cols, val_col as usize), batch_len),
                        None,
                        None,
                    );
                }

                Opcode::BatchAggCount => {
                    let slot = code[pc];
                    pc += 1;
                    exec_agg_count(
                        &mut self.undo,
                        delta_mode,
                        state,
                        slot,
                        u64::from(batch_len),
                    );
                }

                Opcode::BatchAggSumI64 | Opcode::BatchAggMinI64 | Opcode::BatchAggMaxI64 => {
                    let (slot, val_col) = (code[pc], code[pc + 1]);
                    pc += 2;
                    let kind = match op {
                        Opcode::BatchAggSumI64 => AggKind::Sum,
                        Opcode::BatchAggMinI64 => AggKind::Min,
                        _ => AggKind::Max,
                    };
                    exec_agg_i64(
                        &mut self.undo,
                        delta_mode,
                        state,
                        slot,
                        kind,
                        col_i64(col_at(cols, val_col as usize), batch_len),
                        None,
                        None,
                    );
                }

                Opcode::BatchScalarLatest => {
                    let (slot, val_col, cmp_col) = (code[pc], code[pc + 1], code[pc + 2]);
                    pc += 3;
                    let cmp_vals = col_f64(col_at(cols, cmp_col as usize), batch_len);
                    let result = exec_scalar_latest(
                        &mut self.undo,
                        delta_mode,
                        state,
                        slot,
                        col_at(cols, val_col as usize),
                        cmp_vals,
                        batch_len,
                        None,
                    );
                    if result != ErrorCode::Ok {
                        return result as u32;
                    }
                }

                Opcode::BatchStructMapUpsertLast
                | Opcode::BatchStructMapUpsertFirst
                | Opcode::BatchStructMapUpsertMax => {
                    let has_comparison = op == Opcode::BatchStructMapUpsertMax;
                    let Some(operands) =
                        decode_struct_map_upsert_operands(code, pc, has_comparison)
                    else {
                        return INVALID_PROGRAM;
                    };
                    pc = operands.end;

                    let scalar_pairs_end = operands.scalar_pairs_start + operands.num_vals * 2;
                    let scalar_pairs = &code[operands.scalar_pairs_start..scalar_pairs_end];
                    let mut val_cols = [0u8; MAX_STRUCT_SCALAR_OPERANDS];
                    let mut field_idxs = [0u8; MAX_STRUCT_SCALAR_OPERANDS];
                    for (vi, pair) in scalar_pairs.chunks_exact(2).enumerate() {
                        val_cols[vi] = pair[0];
                        field_idxs[vi] = pair[1];
                    }

                    // The top-level batch path intentionally parses but does
                    // not materialize array operands, matching the Zig VM.
                    let smap = StructMapSlot::bind(state, operands.slot);
                    let comparison = match operands.comparison_field_idx {
                        Some(field_idx) => {
                            let Some(comparison) = resolve_struct_map_max_comparison(
                                state,
                                &smap,
                                scalar_pairs,
                                field_idx,
                            ) else {
                                return INVALID_PROGRAM;
                            };
                            Some(comparison)
                        }
                        None => None,
                    };

                    let keys = col_u32(col_at(cols, usize::from(operands.key_col)), batch_len);
                    for i in 0..batch_len {
                        let key = keys[usize::try_from(i).expect("batch index fits usize")];
                        let should_write = match op {
                            Opcode::BatchStructMapUpsertFirst => smap.find(state, key).is_none(),
                            Opcode::BatchStructMapUpsertMax => should_upsert_struct_map_max(
                                state,
                                &smap,
                                key,
                                comparison.expect("max comparison resolved"),
                                cols,
                                i,
                            ),
                            _ => true,
                        };
                        if !should_write {
                            continue;
                        }

                        let result = single_struct_map_upsert_last(
                            &mut self.undo,
                            delta_mode,
                            state,
                            operands.slot,
                            key,
                            &val_cols[..operands.num_vals],
                            &field_idxs[..operands.num_vals],
                            cols,
                            i,
                        );
                        if result.err == ErrorCode::CapacityExceeded {
                            NEEDS_GROWTH_SLOT.store(operands.slot, Ordering::Relaxed);
                            return NEEDS_GROWTH;
                        }
                    }
                }

                Opcode::BatchStructMap2UpsertLast => {
                    let Some(operands) = decode_struct_map2_upsert_operands(code, pc) else {
                        return INVALID_PROGRAM;
                    };
                    pc = operands.end;
                    let pairs_end = operands.scalar_pairs_start + operands.num_vals * 2;
                    let mut val_cols = [0u8; MAX_STRUCT_SCALAR_OPERANDS];
                    let mut field_idxs = [0u8; MAX_STRUCT_SCALAR_OPERANDS];
                    for (index, pair) in code[operands.scalar_pairs_start..pairs_end]
                        .chunks_exact(2)
                        .enumerate()
                    {
                        val_cols[index] = pair[0];
                        field_idxs[index] = pair[1];
                    }
                    let keys1 = col_u32(col_at(cols, usize::from(operands.key1_col)), batch_len);
                    let keys2 = col_u32(col_at(cols, usize::from(operands.key2_col)), batch_len);
                    for i in 0..batch_len {
                        let index = usize::try_from(i).expect("batch index fits usize");
                        let result = single_struct_map2_upsert_last(
                            &mut self.undo,
                            delta_mode,
                            state,
                            operands.slot,
                            keys1[index],
                            keys2[index],
                            &val_cols[..operands.num_vals],
                            &field_idxs[..operands.num_vals],
                            cols,
                            i,
                        );
                        if result.err == ErrorCode::CapacityExceeded {
                            return NEEDS_GROWTH;
                        }
                        if result.err != ErrorCode::Ok {
                            return result.err as u32;
                        }
                    }
                }

                Opcode::BatchStructMap2UpsertMaxI64x2 => {
                    let Some(operands) = decode_struct_map2_max_i64x2_operands(code, pc) else {
                        return INVALID_PROGRAM;
                    };
                    pc = operands.end;
                    let smap = StructMap2Slot::bind(state, operands.row.slot);
                    if !validate_struct_map2_max_i64x2(state, &smap, code, operands) {
                        return INVALID_PROGRAM;
                    }
                    let keys1 =
                        col_u32(col_at(cols, usize::from(operands.row.key1_col)), batch_len);
                    let keys2 =
                        col_u32(col_at(cols, usize::from(operands.row.key2_col)), batch_len);
                    for i in 0..batch_len {
                        let index = usize::try_from(i).expect("batch index fits usize");
                        let result = single_struct_map2_upsert_max_i64x2(
                            &mut self.undo,
                            delta_mode,
                            state,
                            code,
                            operands,
                            keys1[index],
                            keys2[index],
                            cols,
                            i,
                        );
                        if result.err == ErrorCode::CapacityExceeded {
                            return NEEDS_GROWTH;
                        }
                        if result.err != ErrorCode::Ok {
                            return result.err as u32;
                        }
                    }
                }

                Opcode::BatchStructMap2Remove => {
                    let (slot, key1_col, key2_col) = (code[pc], code[pc + 1], code[pc + 2]);
                    pc += 3;
                    let keys1 = col_u32(col_at(cols, usize::from(key1_col)), batch_len);
                    let keys2 = col_u32(col_at(cols, usize::from(key2_col)), batch_len);
                    for index in 0..batch_len as usize {
                        let result = single_struct_map2_remove(
                            &mut self.undo,
                            delta_mode,
                            state,
                            slot,
                            keys1[index],
                            keys2[index],
                        );
                        if result != ErrorCode::Ok {
                            return result as u32;
                        }
                    }
                }

                Opcode::ForEach => {
                    // Header: col:u8, match_count:u8, match_ids:u32le[N], body_len:u16le.
                    let Some(header) = code.get(pc..pc.saturating_add(2)) else {
                        return INVALID_PROGRAM;
                    };
                    let col_idx = header[0];
                    let match_count = usize::from(header[1]);
                    let match_ids_start = pc + 2;
                    let Some(match_ids_len) = match_count.checked_mul(4) else {
                        return INVALID_PROGRAM;
                    };
                    let Some(body_len_offset) = match_ids_start.checked_add(match_ids_len) else {
                        return INVALID_PROGRAM;
                    };
                    let Some(body_len_bytes) =
                        code.get(body_len_offset..body_len_offset.saturating_add(2))
                    else {
                        return INVALID_PROGRAM;
                    };
                    let body_len =
                        usize::from(body_len_bytes[0]) | (usize::from(body_len_bytes[1]) << 8);
                    let body_start = body_len_offset + 2;
                    let Some(body_end) = body_start.checked_add(body_len) else {
                        return INVALID_PROGRAM;
                    };
                    let Some(body) = code.get(body_start..body_end) else {
                        return INVALID_PROGRAM;
                    };
                    if !validate_body(state, body) {
                        return INVALID_PROGRAM;
                    }
                    pc = body_end;

                    let type_col = col_at(cols, col_idx as usize);

                    // Pass 1: batch aggregates, once per match id (vm.zig:1806).
                    for mi in 0..match_count {
                        let id_off = match_ids_start + mi * 4;
                        let match_id = bytes::read_u32(code, id_off as u32);
                        let agg_result = self.execute_batch_aggregates(
                            delta_mode, state, body, cols, batch_len, type_col, match_id,
                        );
                        if agg_result != OK {
                            return agg_result;
                        }
                    }

                    // Pass 2: per-element scalar ops (vm.zig:1827).
                    let type_data = col_u32(type_col, batch_len);
                    for ei in 0..batch_len {
                        let val = type_data[ei as usize];
                        let mut matched = false;
                        for mj in 0..match_count {
                            let id_off = match_ids_start + mj * 4;
                            if val == bytes::read_u32(code, id_off as u32) {
                                matched = true;
                                break;
                            }
                        }
                        if !matched {
                            continue;
                        }
                        let elem_result = self
                            .execute_element_opcodes(delta_mode, state, body, cols, ei, ei, 0xFF);
                        if elem_result != OK {
                            return elem_result;
                        }
                    }
                }

                // Registry-only / init-section opcodes reaching the reduce
                // dispatch take the Zig `else` arm.
                _ => return INVALID_PROGRAM,
            }
        }

        OK
    }

    /// vm.zig:1988 `executeBatchAggregates` — pass 1 of FOR_EACH.
    #[allow(clippy::too_many_arguments)]
    fn execute_batch_aggregates(
        &mut self,
        delta_mode: bool,
        state: &mut [u8],
        body: &[u8],
        cols: &[&[u8]],
        batch_len: u32,
        type_col: &[u8],
        type_id: u32,
    ) -> u32 {
        let type_data = col_u32(type_col, batch_len);
        let mut bpc = 0usize;
        while bpc < body.len() {
            let op_byte = body[bpc];
            if !is_aggregate_op(op_byte) {
                let Some(op_len) = body_op_len(body, bpc) else {
                    return INVALID_PROGRAM;
                };
                bpc += op_len;
                continue;
            }
            match op_byte {
                0x40 | 0x42 | 0x43 => {
                    let (slot, val_col) = (body[bpc + 1], body[bpc + 2]);
                    bpc += 3;
                    let kind = match op_byte {
                        0x40 => AggKind::Sum,
                        0x42 => AggKind::Min,
                        _ => AggKind::Max,
                    };
                    exec_agg_f64(
                        &mut self.undo,
                        delta_mode,
                        state,
                        slot,
                        kind,
                        col_f64(col_at(cols, val_col as usize), batch_len),
                        Some(TypeMask {
                            data: type_data,
                            id: type_id,
                        }),
                        None,
                    );
                }
                0x41 => {
                    let slot = body[bpc + 1];
                    bpc += 2;
                    let matched =
                        aggregates::masked_agg_count(type_data, type_id, batch_len as usize);
                    exec_agg_count(&mut self.undo, delta_mode, state, slot, u64::from(matched));
                }
                0x44 | 0x46 | 0x47 => {
                    let (slot, val_col, pred_col) = (body[bpc + 1], body[bpc + 2], body[bpc + 3]);
                    bpc += 4;
                    let kind = match op_byte {
                        0x44 => AggKind::Sum,
                        0x46 => AggKind::Min,
                        _ => AggKind::Max,
                    };
                    exec_agg_f64(
                        &mut self.undo,
                        delta_mode,
                        state,
                        slot,
                        kind,
                        col_f64(col_at(cols, val_col as usize), batch_len),
                        Some(TypeMask {
                            data: type_data,
                            id: type_id,
                        }),
                        Some(col_u32(col_at(cols, pred_col as usize), batch_len)),
                    );
                }
                0x45 => {
                    let (slot, pred_col) = (body[bpc + 1], body[bpc + 2]);
                    bpc += 3;
                    let preds = col_u32(col_at(cols, pred_col as usize), batch_len);
                    let mut matched = 0u64;
                    for i in 0..batch_len as usize {
                        if type_data[i] == type_id && preds[i] != 0 {
                            matched += 1;
                        }
                    }
                    exec_agg_count(&mut self.undo, delta_mode, state, slot, matched);
                }
                0x48 => {
                    let (slot, val_col, cmp_col) = (body[bpc + 1], body[bpc + 2], body[bpc + 3]);
                    bpc += 4;
                    let cmp_vals = col_f64(col_at(cols, cmp_col as usize), batch_len);
                    let result = exec_scalar_latest(
                        &mut self.undo,
                        delta_mode,
                        state,
                        slot,
                        col_at(cols, val_col as usize),
                        cmp_vals,
                        batch_len,
                        Some((type_data, type_id)),
                    );
                    if result != ErrorCode::Ok {
                        return result as u32;
                    }
                }
                0x49..=0x4b => {
                    let (slot, val_col) = (body[bpc + 1], body[bpc + 2]);
                    bpc += 3;
                    let kind = match op_byte {
                        0x49 => AggKind::Sum,
                        0x4a => AggKind::Min,
                        _ => AggKind::Max,
                    };
                    exec_agg_i64(
                        &mut self.undo,
                        delta_mode,
                        state,
                        slot,
                        kind,
                        col_i64(col_at(cols, val_col as usize), batch_len),
                        Some(TypeMask {
                            data: type_data,
                            id: type_id,
                        }),
                        None,
                    );
                }
                _ => {
                    bpc += agg_op_len(op_byte) as usize;
                }
            }
        }
        OK
    }

    /// vm.zig:2178 `executeElementOpcodes` — pass 2, one element at a time;
    /// recursive through FLAT_MAP.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn execute_element_opcodes(
        &mut self,
        delta_mode: bool,
        state: &mut [u8],
        body: &[u8],
        cols: &[&[u8]],
        child_idx: u32,
        parent_idx: u32,
        parent_ts_col: u8,
    ) -> u32 {
        // Column cell reads at an element index. Bounds are the column's own
        // length (child columns can be longer than batch_len under FLAT_MAP).
        let cell_u32 =
            |cols: &[&[u8]], idx: u8, i: u32| bytes::read_u32(col_at(cols, idx as usize), i * 4);
        let cell_f64 =
            |cols: &[&[u8]], idx: u8, i: u32| bytes::read_f64(col_at(cols, idx as usize), i * 8);

        let mut bpc = 0usize;
        while bpc < body.len() {
            let op_byte = body[bpc];
            if is_aggregate_op(op_byte) {
                bpc += agg_op_len(op_byte) as usize;
                continue;
            }

            match op_byte {
                // MAP_UPSERT_LATEST (0x20) / MAP_UPSERT_LATEST_TTL (0x24)
                0x20 | 0x24 => {
                    let (slot, key_col, val_col, ts_col) =
                        (body[bpc + 1], body[bpc + 2], body[bpc + 3], body[bpc + 4]);
                    let Some(cmp_type) = CmpType::from_u8(body[bpc + 5]) else {
                        return INVALID_PROGRAM;
                    };
                    bpc += 6;

                    let (cmp_raw, cmp_idx) = if parent_ts_col != 0xFF {
                        (col_at(cols, parent_ts_col as usize), parent_idx)
                    } else {
                        (col_at(cols, ts_col as usize), child_idx)
                    };
                    let cmp_val = hashmap_ops::read_cmp_value(cmp_raw, cmp_idx, cmp_type);

                    let meta = SlotMetaView::read(state, slot);
                    let result = hashmap_ops::single_map_upsert(
                        Strategy::Latest,
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        cell_u32(cols, key_col, child_idx),
                        cell_u32(cols, val_col, child_idx),
                        cmp_val,
                        cmp_type,
                        &mut self.ctx(),
                    );
                    if result == ErrorCode::CapacityExceeded {
                        NEEDS_GROWTH_SLOT.store(slot, Ordering::Relaxed);
                        return NEEDS_GROWTH;
                    }
                }

                // MAP_UPSERT_FIRST (0x21)
                0x21 => {
                    let (slot, key_col, val_col) = (body[bpc + 1], body[bpc + 2], body[bpc + 3]);
                    bpc += 4;
                    let meta = SlotMetaView::read(state, slot);
                    let result = hashmap_ops::single_map_upsert(
                        Strategy::First,
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        cell_u32(cols, key_col, child_idx),
                        cell_u32(cols, val_col, child_idx),
                        0,
                        CmpType::F64,
                        &mut self.ctx(),
                    );
                    if result == ErrorCode::CapacityExceeded {
                        NEEDS_GROWTH_SLOT.store(slot, Ordering::Relaxed);
                        return NEEDS_GROWTH;
                    }
                }

                // MAP_UPSERT_LAST (0x22)
                0x22 => {
                    let (slot, key_col, val_col) = (body[bpc + 1], body[bpc + 2], body[bpc + 3]);
                    bpc += 4;
                    let meta = SlotMetaView::read(state, slot);
                    let ttl_cmp = if meta.has_ttl() {
                        cell_f64(cols, meta.timestamp_field_idx(state), child_idx).to_bits()
                    } else {
                        0
                    };
                    let result = hashmap_ops::single_map_upsert(
                        Strategy::Last,
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        cell_u32(cols, key_col, child_idx),
                        cell_u32(cols, val_col, child_idx),
                        ttl_cmp,
                        CmpType::F64,
                        &mut self.ctx(),
                    );
                    if result == ErrorCode::CapacityExceeded {
                        NEEDS_GROWTH_SLOT.store(slot, Ordering::Relaxed);
                        return NEEDS_GROWTH;
                    }
                }

                // MAP_UPSERT_LAST_TTL (0x25)
                0x25 => {
                    let (slot, key_col, val_col, ts_col) =
                        (body[bpc + 1], body[bpc + 2], body[bpc + 3], body[bpc + 4]);
                    bpc += 5;
                    let (ts_raw, ts_idx) = if parent_ts_col != 0xFF {
                        (col_at(cols, parent_ts_col as usize), parent_idx)
                    } else {
                        (col_at(cols, ts_col as usize), child_idx)
                    };
                    let ts_val = hashmap_ops::read_cmp_value(ts_raw, ts_idx, CmpType::F64);

                    let meta = SlotMetaView::read(state, slot);
                    let result = hashmap_ops::single_map_upsert(
                        Strategy::Last,
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        cell_u32(cols, key_col, child_idx),
                        cell_u32(cols, val_col, child_idx),
                        ts_val,
                        CmpType::F64,
                        &mut self.ctx(),
                    );
                    if result == ErrorCode::CapacityExceeded {
                        NEEDS_GROWTH_SLOT.store(slot, Ordering::Relaxed);
                        return NEEDS_GROWTH;
                    }
                }

                // MAP_REMOVE (0x23)
                0x23 => {
                    let (slot, key_col) = (body[bpc + 1], body[bpc + 2]);
                    bpc += 3;
                    let meta = SlotMetaView::read(state, slot);
                    hashmap_ops::single_map_remove(
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        cell_u32(cols, key_col, child_idx),
                        &mut self.ctx(),
                    );
                }

                // MAP_UPSERT_MAX (0x26) / MIN (0x27)
                0x26 | 0x27 => {
                    let (slot, key_col, val_col, cmp_col) =
                        (body[bpc + 1], body[bpc + 2], body[bpc + 3], body[bpc + 4]);
                    let Some(cmp_type) = CmpType::from_u8(body[bpc + 5]) else {
                        return INVALID_PROGRAM;
                    };
                    bpc += 6;
                    let strategy = if op_byte == 0x26 {
                        Strategy::Max
                    } else {
                        Strategy::Min
                    };
                    let meta = SlotMetaView::read(state, slot);
                    let cmp_val = hashmap_ops::read_cmp_value(
                        col_at(cols, cmp_col as usize),
                        child_idx,
                        cmp_type,
                    );
                    let result = hashmap_ops::single_map_upsert(
                        strategy,
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        cell_u32(cols, key_col, child_idx),
                        cell_u32(cols, val_col, child_idx),
                        cmp_val,
                        cmp_type,
                        &mut self.ctx(),
                    );
                    if result == ErrorCode::CapacityExceeded {
                        NEEDS_GROWTH_SLOT.store(slot, Ordering::Relaxed);
                        return NEEDS_GROWTH;
                    }
                }

                // MAP_UPSERT_LATEST_IF (0x28)
                0x28 => {
                    let (slot, key_col, val_col, ts_col) =
                        (body[bpc + 1], body[bpc + 2], body[bpc + 3], body[bpc + 4]);
                    let Some(cmp_type) = CmpType::from_u8(body[bpc + 5]) else {
                        return INVALID_PROGRAM;
                    };
                    let pred_col = body[bpc + 6];
                    bpc += 7;

                    if cell_u32(cols, pred_col, child_idx) == 0 {
                        continue;
                    }

                    let (cmp_raw, cmp_idx) = if parent_ts_col != 0xFF {
                        (col_at(cols, parent_ts_col as usize), parent_idx)
                    } else {
                        (col_at(cols, ts_col as usize), child_idx)
                    };
                    let cmp_val = hashmap_ops::read_cmp_value(cmp_raw, cmp_idx, cmp_type);

                    let meta = SlotMetaView::read(state, slot);
                    let result = hashmap_ops::single_map_upsert(
                        Strategy::Latest,
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        cell_u32(cols, key_col, child_idx),
                        cell_u32(cols, val_col, child_idx),
                        cmp_val,
                        cmp_type,
                        &mut self.ctx(),
                    );
                    if result == ErrorCode::CapacityExceeded {
                        NEEDS_GROWTH_SLOT.store(slot, Ordering::Relaxed);
                        return NEEDS_GROWTH;
                    }
                }

                // MAP_UPSERT_FIRST_IF (0x29) / LAST_IF (0x2A)
                0x29 | 0x2A => {
                    let (slot, key_col, val_col, pred_col) =
                        (body[bpc + 1], body[bpc + 2], body[bpc + 3], body[bpc + 4]);
                    bpc += 5;

                    if cell_u32(cols, pred_col, child_idx) == 0 {
                        continue;
                    }

                    let meta = SlotMetaView::read(state, slot);
                    let (strategy, cmp) = if op_byte == 0x29 {
                        (Strategy::First, 0u64)
                    } else {
                        let ttl_cmp = if meta.has_ttl() {
                            cell_f64(cols, meta.timestamp_field_idx(state), child_idx).to_bits()
                        } else {
                            0
                        };
                        (Strategy::Last, ttl_cmp)
                    };
                    let result = hashmap_ops::single_map_upsert(
                        strategy,
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        cell_u32(cols, key_col, child_idx),
                        cell_u32(cols, val_col, child_idx),
                        cmp,
                        CmpType::F64,
                        &mut self.ctx(),
                    );
                    if result == ErrorCode::CapacityExceeded {
                        NEEDS_GROWTH_SLOT.store(slot, Ordering::Relaxed);
                        return NEEDS_GROWTH;
                    }
                }

                // MAP_REMOVE_IF (0x2B)
                0x2B => {
                    let (slot, key_col, pred_col) = (body[bpc + 1], body[bpc + 2], body[bpc + 3]);
                    bpc += 4;
                    if cell_u32(cols, pred_col, child_idx) == 0 {
                        continue;
                    }
                    let meta = SlotMetaView::read(state, slot);
                    hashmap_ops::single_map_remove(
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        cell_u32(cols, key_col, child_idx),
                        &mut self.ctx(),
                    );
                }

                // MAP_UPSERT_MAX_IF (0x2C) / MIN_IF (0x2D)
                0x2C | 0x2D => {
                    let (slot, key_col, val_col, cmp_col) =
                        (body[bpc + 1], body[bpc + 2], body[bpc + 3], body[bpc + 4]);
                    let Some(cmp_type) = CmpType::from_u8(body[bpc + 5]) else {
                        return INVALID_PROGRAM;
                    };
                    let pred_col = body[bpc + 6];
                    bpc += 7;

                    if cell_u32(cols, pred_col, child_idx) == 0 {
                        continue;
                    }

                    let strategy = if op_byte == 0x2C {
                        Strategy::Max
                    } else {
                        Strategy::Min
                    };
                    let meta = SlotMetaView::read(state, slot);
                    let cmp_val = hashmap_ops::read_cmp_value(
                        col_at(cols, cmp_col as usize),
                        child_idx,
                        cmp_type,
                    );
                    let result = hashmap_ops::single_map_upsert(
                        strategy,
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        cell_u32(cols, key_col, child_idx),
                        cell_u32(cols, val_col, child_idx),
                        cmp_val,
                        cmp_type,
                        &mut self.ctx(),
                    );
                    if result == ErrorCode::CapacityExceeded {
                        NEEDS_GROWTH_SLOT.store(slot, Ordering::Relaxed);
                        return NEEDS_GROWTH;
                    }
                }

                // SET_INSERT (0x30) / BITMAP_ADD (0x34) — vm.zig routes both
                // through singleSetInsert (BITMAP delegation via hooks).
                0x30 | 0x34 => {
                    let (slot, elem_col) = (body[bpc + 1], body[bpc + 2]);
                    bpc += 3;
                    let meta = SlotMetaView::read(state, slot);
                    let ts = if meta.has_ttl() {
                        cell_f64(cols, meta.timestamp_field_idx(state), child_idx)
                    } else {
                        0.0
                    };
                    let result = hashset_ops::single_set_insert(
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        cell_u32(cols, elem_col, child_idx),
                        ts,
                        &mut self.ctx(),
                    );
                    if result == ErrorCode::CapacityExceeded {
                        NEEDS_GROWTH_SLOT.store(slot, Ordering::Relaxed);
                        return NEEDS_GROWTH;
                    }
                }

                // SET_INSERT_TTL (0x32)
                0x32 => {
                    let (slot, elem_col, ts_col) = (body[bpc + 1], body[bpc + 2], body[bpc + 3]);
                    bpc += 4;
                    let ts = if parent_ts_col != 0xFF {
                        cell_f64(cols, parent_ts_col, parent_idx)
                    } else {
                        cell_f64(cols, ts_col, child_idx)
                    };
                    let meta = SlotMetaView::read(state, slot);
                    let result = hashset_ops::single_set_insert(
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        cell_u32(cols, elem_col, child_idx),
                        ts,
                        &mut self.ctx(),
                    );
                    if result == ErrorCode::CapacityExceeded {
                        NEEDS_GROWTH_SLOT.store(slot, Ordering::Relaxed);
                        return NEEDS_GROWTH;
                    }
                }

                // SET_INSERT_IF (0x33)
                0x33 => {
                    let (slot, elem_col, pred_col) = (body[bpc + 1], body[bpc + 2], body[bpc + 3]);
                    bpc += 4;
                    if cell_u32(cols, pred_col, child_idx) == 0 {
                        continue;
                    }
                    let meta = SlotMetaView::read(state, slot);
                    let ts = if meta.has_ttl() {
                        cell_f64(cols, meta.timestamp_field_idx(state), child_idx)
                    } else {
                        0.0
                    };
                    let result = hashset_ops::single_set_insert(
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        cell_u32(cols, elem_col, child_idx),
                        ts,
                        &mut self.ctx(),
                    );
                    if result == ErrorCode::CapacityExceeded {
                        NEEDS_GROWTH_SLOT.store(slot, Ordering::Relaxed);
                        return NEEDS_GROWTH;
                    }
                }

                // SET_REMOVE (0x31) / BITMAP_REMOVE (0x35)
                0x31 | 0x35 => {
                    let (slot, elem_col) = (body[bpc + 1], body[bpc + 2]);
                    bpc += 3;
                    let meta = SlotMetaView::read(state, slot);
                    hashset_ops::single_set_remove(
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        cell_u32(cols, elem_col, child_idx),
                        &mut self.ctx(),
                    );
                }

                // STRUCT_MAP_UPSERT_LAST/FIRST/MAX (0x80/0x81/0x82)
                0x80..=0x82 => {
                    let Some(operands) =
                        decode_struct_map_upsert_operands(body, bpc + 1, op_byte == 0x82)
                    else {
                        return INVALID_PROGRAM;
                    };
                    bpc = operands.end;

                    let scalar_pairs_end = operands.scalar_pairs_start + operands.num_vals * 2;
                    let scalar_pairs = &body[operands.scalar_pairs_start..scalar_pairs_end];
                    let mut vc = [0u8; MAX_STRUCT_SCALAR_OPERANDS];
                    let mut fi = [0u8; MAX_STRUCT_SCALAR_OPERANDS];
                    for (vi, pair) in scalar_pairs.chunks_exact(2).enumerate() {
                        vc[vi] = pair[0];
                        fi[vi] = pair[1];
                    }

                    let mut aoc = [0u8; MAX_STRUCT_ARRAY_OPERANDS];
                    let mut avc = [0u8; MAX_STRUCT_ARRAY_OPERANDS];
                    let mut afi = [0u8; MAX_STRUCT_ARRAY_OPERANDS];
                    let array_triples_end =
                        operands.array_triples_start + operands.num_array_vals * 3;
                    for (ai, triple) in body[operands.array_triples_start..array_triples_end]
                        .chunks_exact(3)
                        .enumerate()
                    {
                        aoc[ai] = triple[0];
                        avc[ai] = triple[1];
                        afi[ai] = triple[2];
                    }

                    let smap = StructMapSlot::bind(state, operands.slot);
                    let comparison = match operands.comparison_field_idx {
                        Some(field_idx) => {
                            let Some(comparison) = resolve_struct_map_max_comparison(
                                state,
                                &smap,
                                scalar_pairs,
                                field_idx,
                            ) else {
                                return INVALID_PROGRAM;
                            };
                            Some(comparison)
                        }
                        None => None,
                    };
                    let key = cell_u32(cols, operands.key_col, child_idx);
                    let should_write = match op_byte {
                        0x81 => smap.find(state, key).is_none(),
                        0x82 => should_upsert_struct_map_max(
                            state,
                            &smap,
                            key,
                            comparison.expect("max comparison resolved"),
                            cols,
                            child_idx,
                        ),
                        _ => true,
                    };
                    if should_write {
                        let (array_journal_ranges, array_captured) = if operands.num_array_vals > 0
                        {
                            let meta_base = slot_meta_base(operands.slot);
                            let arena_header =
                                bytes::read_u32(state, meta_base + SlotMetaOffset::GRACE_SECONDS);
                            let arena_capacity = bytes::read_u32(state, arena_header);
                            let ranges = [
                                (meta_base, SLOT_META_SIZE),
                                (
                                    smap.slot_offset,
                                    arena_header + ARENA_HEADER_SIZE + arena_capacity
                                        - smap.slot_offset,
                                ),
                            ];
                            let captured = self.undo.begin_state_capture(
                                state,
                                &ranges,
                                u32::from(smap.num_fields) + 1,
                            );
                            (ranges, captured)
                        } else {
                            ([(0, 0); 2], false)
                        };

                        let result = single_struct_map_upsert_last(
                            &mut self.undo,
                            delta_mode,
                            state,
                            operands.slot,
                            key,
                            &vc[..operands.num_vals],
                            &fi[..operands.num_vals],
                            cols,
                            child_idx,
                        );
                        if result.err == ErrorCode::CapacityExceeded {
                            if array_captured {
                                self.undo.finish_state_capture(
                                    delta_mode,
                                    state,
                                    &array_journal_ranges,
                                );
                            }
                            NEEDS_GROWTH_SLOT.store(operands.slot, Ordering::Relaxed);
                            return NEEDS_GROWTH;
                        }

                        if operands.num_array_vals > 0 {
                            let arr_result = write_struct_map_array_fields(
                                state,
                                operands.slot,
                                result.pos,
                                &aoc[..operands.num_array_vals],
                                &avc[..operands.num_array_vals],
                                &afi[..operands.num_array_vals],
                                cols,
                                child_idx,
                            );
                            if array_captured {
                                self.undo.finish_state_capture(
                                    delta_mode,
                                    state,
                                    &array_journal_ranges,
                                );
                            }
                            if arr_result == ErrorCode::ArenaOverflow {
                                NEEDS_GROWTH_SLOT.store(operands.slot, Ordering::Relaxed);
                                return NEEDS_GROWTH;
                            }
                        }
                    }
                }

                // Exact two-lane struct map upsert/remove.
                0x83 => {
                    let Some(operands) = decode_struct_map2_upsert_operands(body, bpc + 1) else {
                        return INVALID_PROGRAM;
                    };
                    bpc = operands.end;
                    let pairs_end = operands.scalar_pairs_start + operands.num_vals * 2;
                    let mut val_cols = [0u8; MAX_STRUCT_SCALAR_OPERANDS];
                    let mut field_idxs = [0u8; MAX_STRUCT_SCALAR_OPERANDS];
                    for (index, pair) in body[operands.scalar_pairs_start..pairs_end]
                        .chunks_exact(2)
                        .enumerate()
                    {
                        val_cols[index] = pair[0];
                        field_idxs[index] = pair[1];
                    }
                    let result = single_struct_map2_upsert_last(
                        &mut self.undo,
                        delta_mode,
                        state,
                        operands.slot,
                        cell_u32(cols, operands.key1_col, child_idx),
                        cell_u32(cols, operands.key2_col, child_idx),
                        &val_cols[..operands.num_vals],
                        &field_idxs[..operands.num_vals],
                        cols,
                        child_idx,
                    );
                    if result.err == ErrorCode::CapacityExceeded {
                        return NEEDS_GROWTH;
                    }
                    if result.err != ErrorCode::Ok {
                        return result.err as u32;
                    }
                }
                0x87 => {
                    let Some(operands) = decode_struct_map2_max_i64x2_operands(body, bpc + 1)
                    else {
                        return INVALID_PROGRAM;
                    };
                    bpc = operands.end;
                    let smap = StructMap2Slot::bind(state, operands.row.slot);
                    if !validate_struct_map2_max_i64x2(state, &smap, body, operands) {
                        return INVALID_PROGRAM;
                    }
                    let result = single_struct_map2_upsert_max_i64x2(
                        &mut self.undo,
                        delta_mode,
                        state,
                        body,
                        operands,
                        cell_u32(cols, operands.row.key1_col, child_idx),
                        cell_u32(cols, operands.row.key2_col, child_idx),
                        cols,
                        child_idx,
                    );
                    if result.err == ErrorCode::CapacityExceeded {
                        return NEEDS_GROWTH;
                    }
                    if result.err != ErrorCode::Ok {
                        return result.err as u32;
                    }
                }
                0x86 => {
                    let (slot, key1_col, key2_col) = (body[bpc + 1], body[bpc + 2], body[bpc + 3]);
                    bpc += 4;
                    let result = single_struct_map2_remove(
                        &mut self.undo,
                        delta_mode,
                        state,
                        slot,
                        cell_u32(cols, key1_col, child_idx),
                        cell_u32(cols, key2_col, child_idx),
                    );
                    if result != ErrorCode::Ok {
                        return result as u32;
                    }
                }

                //#region axe!n/reduce-typed-state.probe-exec
                // STRUCT_MAP_PROBE (0x2e)
                0x2e => {
                    let (probe_slot, key_col, _miss_mode, out_slot, num_fields) = (
                        body[bpc + 1],
                        body[bpc + 2],
                        body[bpc + 3],
                        body[bpc + 4],
                        body[bpc + 5] as usize,
                    );
                    bpc += 6;

                    let mut probe_fis = [0u8; 32];
                    let mut out_fis = [0u8; 32];
                    for f in 0..num_fields {
                        probe_fis[f] = body[bpc];
                        out_fis[f] = body[bpc + 1];
                        bpc += 2;
                    }
                    let out_key_col = body[bpc];
                    bpc += 1;

                    let key = cell_u32(cols, key_col, child_idx);
                    let probe = StructMapSlot::bind(state, probe_slot);
                    let Some(probe_pos) = probe.find(state, key) else {
                        // Miss: skip (inner join); 'null' (left join) is
                        // compiler-rejected — treat defensively as skip.
                        continue;
                    };

                    let probe_row = probe.row_off(probe_pos);
                    let out_key = cell_u32(cols, out_key_col, child_idx);
                    let result = single_struct_map_upsert_from_probe(
                        &mut self.undo,
                        delta_mode,
                        state,
                        out_slot,
                        out_key,
                        &probe,
                        probe_row,
                        &probe_fis[..num_fields],
                        &out_fis[..num_fields],
                    );
                    if result.err == ErrorCode::CapacityExceeded {
                        NEEDS_GROWTH_SLOT.store(out_slot, Ordering::Relaxed);
                        return NEEDS_GROWTH;
                    }
                }
                //#endregion axe!n/reduce-typed-state.probe-exec

                //#region axe!n/reduce-typed-state.scatter-exec
                // STRUCT_MAP_PROBE_SCATTER (0x2f)
                0x2f => {
                    let (
                        probe_slot,
                        key_col,
                        _miss_mode,
                        route_field_idx,
                        op_field_idx,
                        num_routes,
                    ) = (
                        body[bpc + 1],
                        body[bpc + 2],
                        body[bpc + 3],
                        body[bpc + 4],
                        body[bpc + 5],
                        body[bpc + 6] as usize,
                    );
                    bpc += 7;

                    let mut route_kinds = [0u8; 32];
                    let mut route_dest_slots = [0u8; 32];
                    let mut route_dest_fields = [0u8; 32];
                    let mut route_out_key_fields = [0u8; 32];
                    let mut route_v_src_fields = [0u8; 32];
                    for ri in 0..num_routes {
                        route_kinds[ri] = body[bpc];
                        route_dest_slots[ri] = body[bpc + 1];
                        route_dest_fields[ri] = body[bpc + 2];
                        route_out_key_fields[ri] = body[bpc + 3];
                        route_v_src_fields[ri] = body[bpc + 4];
                        bpc += 5;
                    }

                    let key = cell_u32(cols, key_col, child_idx);
                    let probe = StructMapSlot::bind(state, probe_slot);
                    let Some(probe_pos) = probe.find(state, key) else {
                        continue;
                    };
                    let probe_row = probe.row_off(probe_pos);

                    let route_off = probe_row + probe.field_offset(state, route_field_idx);
                    let route_ord = bytes::read_u32(state, route_off);
                    if route_ord == 0xFFFF_FFFF || route_ord as usize >= num_routes {
                        continue; // SKIP / out-of-range → SKIP
                    }

                    let op_off = probe_row + probe.field_offset(state, op_field_idx);
                    let is_retract = bytes::read_u32(state, op_off) != 0;
                    let ri = route_ord as usize;
                    let route_kind = route_kinds[ri];
                    let dest_slot = route_dest_slots[ri];
                    let dest_field_idx = route_dest_fields[ri];
                    let v_src_field_idx = route_v_src_fields[ri];
                    let out_key_off =
                        probe_row + probe.field_offset(state, route_out_key_fields[ri]);
                    let out_key = bytes::read_u32(state, out_key_off);

                    match route_kind {
                        // kind 0: card-one struct field on the `nodes` map.
                        0 => {
                            let out = StructMapSlot::bind(state, dest_slot);
                            let dst_ft = out.field_type(state, dest_field_idx);
                            let fsize = struct_field_size(dst_ft);
                            let src_off = probe_row + probe.field_offset(state, v_src_field_idx);

                            if !is_retract {
                                let Some(up) = out.upsert(state, out_key) else {
                                    NEEDS_GROWTH_SLOT.store(dest_slot, Ordering::Relaxed);
                                    return NEEDS_GROWTH;
                                };
                                let row = out.row_off(up.pos);
                                let dst_off = row + out.field_offset(state, dest_field_idx);

                                let prior_bit =
                                    StructMapSlot::is_field_set(state, row, dest_field_idx);
                                let value_matches = prior_bit
                                    && state[dst_off as usize..(dst_off + fsize) as usize]
                                        == state[src_off as usize..(src_off + fsize) as usize];
                                if value_matches {
                                    continue;
                                }

                                if self.undo.enabled {
                                    let undo_flags = if up.is_new {
                                        SMF_ROW_ABSENT
                                    } else if prior_bit {
                                        SMF_BIT_SET
                                    } else {
                                        0
                                    };
                                    let prior_aux = if up.is_new || !prior_bit {
                                        0
                                    } else {
                                        pack_field_bytes(
                                            &state[dst_off as usize..(dst_off + fsize) as usize],
                                        )
                                    };
                                    let v_aux = pack_field_bytes(
                                        &state[src_off as usize..(src_off + fsize) as usize],
                                    );
                                    let smf = |pad2: u8, aux: u64| FlatUndoEntry {
                                        op: FlatUndoOp::StructMapField,
                                        slot: dest_slot,
                                        pad1: dest_field_idx,
                                        pad2,
                                        key: out_key,
                                        prev_value: 0,
                                        aux,
                                    };
                                    append_mutation_state(
                                        &mut self.undo,
                                        delta_mode,
                                        state,
                                        smf(undo_flags, prior_aux),
                                        smf(SMF_BIT_SET, v_aux),
                                    );
                                }

                                StructMapSlot::set_field_bit(state, row, dest_field_idx);
                                bytes::copy_within(state, src_off, dst_off, fsize);
                                let meta_base = slot_meta_base(dest_slot);
                                state[(meta_base + SlotMetaOffset::CHANGE_FLAGS) as usize] |=
                                    if prior_bit {
                                        ChangeFlag::UPDATED
                                    } else {
                                        ChangeFlag::INSERTED
                                    };
                            } else if let Some(pos) = out.find(state, out_key) {
                                let row = out.row_off(pos);
                                let dst_off = row + out.field_offset(state, dest_field_idx);
                                // Retract is a no-op unless the stored value matches v.
                                let stored_matches =
                                    StructMapSlot::is_field_set(state, row, dest_field_idx)
                                        && state[dst_off as usize..(dst_off + fsize) as usize]
                                            == state[src_off as usize..(src_off + fsize) as usize];
                                if stored_matches {
                                    if self.undo.enabled {
                                        let prior_aux = pack_field_bytes(
                                            &state[dst_off as usize..(dst_off + fsize) as usize],
                                        );
                                        let smf = |pad2: u8, aux: u64| FlatUndoEntry {
                                            op: FlatUndoOp::StructMapField,
                                            slot: dest_slot,
                                            pad1: dest_field_idx,
                                            pad2,
                                            key: out_key,
                                            prev_value: 0,
                                            aux,
                                        };
                                        append_mutation_state(
                                            &mut self.undo,
                                            delta_mode,
                                            state,
                                            smf(SMF_BIT_SET, prior_aux),
                                            smf(0, 0),
                                        );
                                    }
                                    StructMapSlot::clear_scalar_field(state, row, dest_field_idx);
                                    bytes::zero(state, dst_off, fsize);
                                    let meta_base = slot_meta_base(dest_slot);
                                    state[(meta_base + SlotMetaOffset::CHANGE_FLAGS) as usize] |=
                                        ChangeFlag::REMOVED;
                                }
                            }
                        }
                        // kind 1: card-many set — element is the precomputed
                        // interned composite (e, v) read off the probed row.
                        1 => {
                            let meta = SlotMetaView::read(state, dest_slot);
                            let src_off = probe_row + probe.field_offset(state, v_src_field_idx);
                            let v = bytes::read_u32(state, src_off);
                            if is_retract {
                                hashset_ops::single_set_remove(
                                    delta_mode,
                                    state,
                                    &meta,
                                    dest_slot,
                                    v,
                                    &mut self.ctx(),
                                );
                            } else {
                                let set_result = hashset_ops::single_set_insert(
                                    delta_mode,
                                    state,
                                    &meta,
                                    dest_slot,
                                    v,
                                    0.0,
                                    &mut self.ctx(),
                                );
                                // Faithful: ANY failure becomes NEEDS_GROWTH
                                // (vm.zig:3025 checks != .OK, not CAPACITY_EXCEEDED).
                                if set_result != ErrorCode::Ok {
                                    NEEDS_GROWTH_SLOT.store(dest_slot, Ordering::Relaxed);
                                    return NEEDS_GROWTH;
                                }
                            }
                        }
                        // kind 2: deferred to Phase 2 — never emitted.
                        _ => return INVALID_PROGRAM,
                    }
                }
                //#endregion axe!n/reduce-typed-state.scatter-exec

                // LIST_APPEND (0x84)
                0x84 => {
                    let (slot, val_col) = (body[bpc + 1], body[bpc + 2]);
                    bpc += 3;

                    // Raw metadata reads — getSlotMeta cannot bind
                    // ORDERED_LIST's repurposed fields (vm.zig:3045).
                    let meta_base = slot_meta_base(slot);
                    let slot_offset = bytes::read_u32(state, meta_base + SlotMetaOffset::OFFSET);
                    let capacity = bytes::read_u32(state, meta_base + SlotMetaOffset::CAPACITY);
                    let mut count = bytes::read_u32(state, meta_base + SlotMetaOffset::SIZE);
                    let elem_size = u32::from(bytes::read_u16(
                        state,
                        meta_base + SlotMetaOffset::TTL_SECONDS,
                    ));

                    if count >= capacity {
                        NEEDS_GROWTH_SLOT.store(slot, Ordering::Relaxed);
                        return NEEDS_GROWTH;
                    }

                    let write_off = slot_offset + count * elem_size;
                    let payload_ranges = [
                        (write_off, elem_size),
                        (meta_base + SlotMetaOffset::CHANGE_FLAGS, 1),
                    ];
                    let captured = self.undo.begin_state_capture(state, &payload_ranges, 1);
                    if self.undo.enabled {
                        let lau = |prev: u32| FlatUndoEntry {
                            op: FlatUndoOp::ListAppendUndo,
                            slot,
                            pad1: 0,
                            pad2: 0,
                            key: 0,
                            prev_value: prev,
                            aux: 0,
                        };
                        append_mutation_state(
                            &mut self.undo,
                            delta_mode,
                            state,
                            lau(count),
                            lau(count + 1),
                        );
                    }
                    if elem_size == 4 {
                        bytes::write_u32(state, write_off, cell_u32(cols, val_col, child_idx));
                    } else if elem_size == 8 {
                        let v = bytes::read_u64(col_at(cols, val_col as usize), child_idx * 8);
                        bytes::write_u64(state, write_off, v);
                    } else {
                        // BOOL (1 byte)
                        let v = cell_u32(cols, val_col, child_idx);
                        state[write_off as usize] = u8::from(v != 0);
                    }

                    count += 1;
                    bytes::write_u32(state, meta_base + SlotMetaOffset::SIZE, count);
                    state[(meta_base + SlotMetaOffset::CHANGE_FLAGS) as usize] |=
                        ChangeFlag::INSERTED;
                    if captured {
                        self.undo
                            .finish_state_capture(delta_mode, state, &payload_ranges);
                    }
                }

                // LIST_APPEND_STRUCT (0x85)
                0x85 => {
                    let (slot, num_vals) = (body[bpc + 1], body[bpc + 2] as usize);
                    bpc += 3;

                    let mut vc = [0u8; 32];
                    let mut fi = [0u8; 32];
                    for vi in 0..num_vals {
                        vc[vi] = body[bpc];
                        fi[vi] = body[bpc + 1];
                        bpc += 2;
                    }

                    let meta_base = slot_meta_base(slot);
                    let slot_offset = bytes::read_u32(state, meta_base + SlotMetaOffset::OFFSET);
                    let capacity = bytes::read_u32(state, meta_base + SlotMetaOffset::CAPACITY);
                    let mut count = bytes::read_u32(state, meta_base + SlotMetaOffset::SIZE);
                    let num_fields = state[(meta_base + SlotMetaOffset::AGG_TYPE) as usize];
                    let bitset_bytes = u32::from(
                        state[(meta_base + SlotMetaOffset::TIMESTAMP_FIELD_IDX) as usize],
                    );
                    let row_size = u32::from(bytes::read_u16(
                        state,
                        meta_base + SlotMetaOffset::TTL_SECONDS,
                    ));

                    if count >= capacity {
                        NEEDS_GROWTH_SLOT.store(slot, Ordering::Relaxed);
                        return NEEDS_GROWTH;
                    }

                    let descriptor_size = align8(u32::from(num_fields));
                    let rows_base = slot_offset + descriptor_size;
                    let row_off = rows_base + count * row_size;
                    let payload_ranges = [
                        (row_off, row_size),
                        (meta_base + SlotMetaOffset::CHANGE_FLAGS, 1),
                    ];
                    let captured = self.undo.begin_state_capture(state, &payload_ranges, 1);
                    if self.undo.enabled {
                        let lau = |prev: u32| FlatUndoEntry {
                            op: FlatUndoOp::ListAppendUndo,
                            slot,
                            pad1: 0,
                            pad2: 0,
                            key: 0,
                            prev_value: prev,
                            aux: 0,
                        };
                        append_mutation_state(
                            &mut self.undo,
                            delta_mode,
                            state,
                            lau(count),
                            lau(count + 1),
                        );
                    }

                    bytes::zero(state, row_off, bitset_bytes);

                    for vi in 0..num_vals {
                        let field_idx = fi[vi];
                        let field_type_byte = state[(slot_offset + u32::from(field_idx)) as usize];
                        let Some(field_type) = StructFieldType::from_u8(field_type_byte) else {
                            columine_types::die!(
                                "invariant: ordered-list descriptor contains an invalid field type"
                            );
                        };
                        let f_offset = row_off + {
                            let descriptor = &state[slot_offset as usize
                                ..(slot_offset + u32::from(num_fields)) as usize];
                            state_init::struct_field_offset(num_fields, descriptor, field_idx)
                        };

                        state[(row_off + u32::from(field_idx) / 8) as usize] |=
                            1 << (field_idx % 8);

                        match field_type {
                            StructFieldType::UInt32 | StructFieldType::String => {
                                bytes::write_u32(
                                    state,
                                    f_offset,
                                    cell_u32(cols, vc[vi], child_idx),
                                );
                            }
                            StructFieldType::Int64 => {
                                let v = bytes::read_u64(cols[vc[vi] as usize], child_idx * 8);
                                bytes::write_u64(state, f_offset, v);
                            }
                            StructFieldType::Float64 => {
                                let v = bytes::read_u64(cols[vc[vi] as usize], child_idx * 8);
                                bytes::write_u64(state, f_offset, v);
                            }
                            StructFieldType::Bool => {
                                let v = cell_u32(cols, vc[vi], child_idx);
                                state[f_offset as usize] = u8::from(v != 0);
                            }
                            // Array fields in ordered-list rows — not supported (vm.zig:3156).
                            StructFieldType::ArrayU32
                            | StructFieldType::ArrayI64
                            | StructFieldType::ArrayF64
                            | StructFieldType::ArrayString
                            | StructFieldType::ArrayBool => {}
                        }
                    }

                    count += 1;
                    bytes::write_u32(state, meta_base + SlotMetaOffset::SIZE, count);
                    state[(meta_base + SlotMetaOffset::CHANGE_FLAGS) as usize] |=
                        ChangeFlag::INSERTED;
                    if captured {
                        self.undo
                            .finish_state_capture(delta_mode, state, &payload_ranges);
                    }
                }

                // FLAT_MAP (0xE1)
                0xE1 => {
                    let offsets_col = body[bpc + 1];
                    let inner_parent_ts_col = body[bpc + 2];
                    let inner_body_len =
                        usize::from(body[bpc + 3]) | (usize::from(body[bpc + 4]) << 8);
                    bpc += 5;

                    let inner_body = &body[bpc..bpc + inner_body_len];
                    bpc += inner_body_len;

                    let start = cell_u32(cols, offsets_col, child_idx);
                    let end = cell_u32(cols, offsets_col, child_idx + 1);

                    for j in start..end {
                        let result = self.execute_element_opcodes(
                            delta_mode,
                            state,
                            inner_body,
                            cols,
                            j,
                            child_idx,
                            inner_parent_ts_col,
                        );
                        if result != OK {
                            return result;
                        }
                    }
                }

                // NESTED_SET_INSERT (0x90)
                0x90 => {
                    let (slot, outer_key_col, elem_col) =
                        (body[bpc + 1], body[bpc + 2], body[bpc + 3]);
                    bpc += 4;
                    let meta = SlotMetaView::read(state, slot);
                    let journal_ranges = nested_journal_ranges(state, &meta);
                    let captured = self.undo.begin_state_capture(state, &journal_ranges, 0);
                    let result = nested::nested_set_insert(
                        state,
                        &meta,
                        cell_u32(cols, outer_key_col, child_idx),
                        cell_u32(cols, elem_col, child_idx),
                    );
                    if captured {
                        self.undo
                            .finish_state_capture(delta_mode, state, &journal_ranges);
                    }
                    if result == ErrorCode::CapacityExceeded {
                        NEEDS_GROWTH_SLOT.store(slot, Ordering::Relaxed);
                        return NEEDS_GROWTH;
                    }
                }

                // NESTED_MAP_UPSERT_LAST (0x92)
                0x92 => {
                    let (slot, outer_key_col, inner_key_col, val_col) =
                        (body[bpc + 1], body[bpc + 2], body[bpc + 3], body[bpc + 4]);
                    bpc += 5;
                    let meta = SlotMetaView::read(state, slot);
                    let journal_ranges = nested_journal_ranges(state, &meta);
                    let captured = self.undo.begin_state_capture(state, &journal_ranges, 0);
                    let result = nested::nested_map_upsert_last(
                        state,
                        &meta,
                        cell_u32(cols, outer_key_col, child_idx),
                        cell_u32(cols, inner_key_col, child_idx),
                        cell_u32(cols, val_col, child_idx),
                    );
                    if captured {
                        self.undo
                            .finish_state_capture(delta_mode, state, &journal_ranges);
                    }
                    if result == ErrorCode::CapacityExceeded {
                        NEEDS_GROWTH_SLOT.store(slot, Ordering::Relaxed);
                        return NEEDS_GROWTH;
                    }
                }

                // NESTED_AGG_UPDATE (0x95)
                0x95 => {
                    let (slot, outer_key_col, val_col) =
                        (body[bpc + 1], body[bpc + 2], body[bpc + 3]);
                    bpc += 4;
                    let meta = SlotMetaView::read(state, slot);
                    let journal_ranges = nested_journal_ranges(state, &meta);
                    let captured = self.undo.begin_state_capture(state, &journal_ranges, 0);
                    let result = nested::nested_agg_update(
                        state,
                        &meta,
                        cell_u32(cols, outer_key_col, child_idx),
                        cell_f64(cols, val_col, child_idx).to_bits(),
                    );
                    if captured {
                        self.undo
                            .finish_state_capture(delta_mode, state, &journal_ranges);
                    }
                    if result == ErrorCode::CapacityExceeded {
                        NEEDS_GROWTH_SLOT.store(slot, Ordering::Relaxed);
                        return NEEDS_GROWTH;
                    }
                }

                _ => return INVALID_PROGRAM,
            }
        }
        OK
    }

    // =========================================================================
    // Undo-log surface (vm.zig:3630-3731)
    // =========================================================================

    /// vm.zig:3630 `vm_undo_enable`.
    pub fn undo_enable(&mut self, state: &[u8]) {
        let u = &mut self.undo;
        u.enabled = true;
        u.entries.clear();
        u.redo.clear();
        u.delta_count = 0;
        u.overflow = false;
        u.shadow_active = false;
        u.overflow_entry = None;
        u.shadow = None;
        u.state_size = state.len() as u32;
        u.save_change_flags(state);
    }

    /// vm.zig:3651 `vm_undo_checkpoint`.
    pub fn undo_checkpoint(&self) -> u32 {
        self.undo.count()
    }

    /// vm.zig:3660 `vm_undo_rollback`.
    pub fn undo_rollback(&mut self, state: &mut [u8], checkpoint_pos: u32) {
        let u = &mut self.undo;
        if u.overflow && u.shadow_active {
            if let Some(shadow) = u.shadow.take() {
                let size = u.state_size as usize;
                state[..size].copy_from_slice(&shadow[..size]);
            }
            u.shadow_active = false;
            if let Some((overflow_entry, _redo)) = u.overflow_entry.take() {
                rollback_entry(&mut self.bitmap_env, state, &overflow_entry);
            }
        }
        let u = &mut self.undo;
        let mut i = u.count();
        while i > checkpoint_pos {
            i -= 1;
            let entry = u.entries[i as usize];
            rollback_entry(&mut self.bitmap_env, state, &entry);
        }
        let u = &mut self.undo;
        u.entries.truncate(checkpoint_pos as usize);
        u.redo.truncate(checkpoint_pos as usize);
        if u.delta_count > u.count() {
            u.delta_count = u.count();
        }
        u.restore_change_flags(state);
    }

    /// vm.zig:3704 `vm_undo_commit`.
    pub fn undo_commit(&mut self) {
        let u = &mut self.undo;
        u.shadow = None;
        u.shadow_active = false;
        u.overflow_entry = None;
        u.entries.clear();
        u.redo.clear();
        u.delta_count = 0;
        u.overflow = false;
        u.enabled = false;
    }

    /// vm.zig:3729 `vm_undo_has_overflow`.
    pub fn undo_has_overflow(&self) -> bool {
        self.undo.overflow
    }

    // =========================================================================
    // Delta export/apply (vm.zig:3733-3792)
    // =========================================================================

    /// vm.zig:3733 `vm_delta_export_segment` — clamp `[from, to)` to the
    /// valid pair range; returns the exported count.
    pub fn delta_export_segment(&mut self, from_pos: u32, to_pos: u32) -> u32 {
        let u = &mut self.undo;
        let end = to_pos.min(u.delta_count);
        let start = from_pos.min(end);
        u.export_start = start;
        u.export_count = end - start;
        u.export_overflow = u.overflow;
        u.export_count
    }

    /// The exported undo lane as serialized 24-byte entries (bindings expose
    /// the raw pointer; core code returns owned bytes).
    pub fn delta_export_undo_bytes(&self) -> Vec<u8> {
        serialize_entries(
            &self.undo.entries,
            self.undo.export_start,
            self.undo.export_count,
        )
    }

    /// The exported redo lane.
    pub fn delta_export_redo_bytes(&self) -> Vec<u8> {
        serialize_entries(
            &self.undo.redo,
            self.undo.export_start,
            self.undo.export_count,
        )
    }

    /// vm.zig:3751 `vm_delta_export_len_bytes`.
    pub fn delta_export_len_bytes(&self) -> u32 {
        self.undo.export_count * FLAT_UNDO_ENTRY_SIZE
    }

    /// vm.zig:3759 `vm_delta_export_overflow`.
    pub fn delta_export_overflow(&self) -> bool {
        self.undo.export_overflow
    }

    /// vm.zig:3763 `vm_delta_apply_rollback_segment` — reverse-apply an undo
    /// segment. Zig trusts the bytes (UB on a corrupt op); Rust panics on one
    /// — segments are produced by `delta_export_*` and a corrupt op byte is a
    /// programmer bug at the boundary.
    pub fn delta_apply_rollback_segment(
        &mut self,
        state: &mut [u8],
        undo_segment: &[u8],
        entry_size: u32,
    ) {
        if entry_size != FLAT_UNDO_ENTRY_SIZE
            || !(undo_segment.len() as u32).is_multiple_of(entry_size)
        {
            return;
        }
        let count = undo_segment.len() / FLAT_UNDO_ENTRY_SIZE as usize;
        for i in (0..count).rev() {
            let entry = parse_entry(undo_segment, i);
            rollback_entry(&mut self.bitmap_env, state, &entry);
        }
    }

    /// vm.zig:3779 `vm_delta_apply_rollforward_segment` — forward-apply a
    /// redo segment (the entries are "rollbacks" toward the target state).
    pub fn delta_apply_rollforward_segment(
        &mut self,
        state: &mut [u8],
        redo_segment: &[u8],
        entry_size: u32,
    ) {
        if entry_size != FLAT_UNDO_ENTRY_SIZE
            || !(redo_segment.len() as u32).is_multiple_of(entry_size)
        {
            return;
        }
        let count = redo_segment.len() / FLAT_UNDO_ENTRY_SIZE as usize;
        for i in 0..count {
            // Undo-only journal items (evictions, derived-fact appends) have
            // no forward effect: their redo lane is the explicit zeroed
            // no-op marker (op byte 0). Skip them — the deleted Zig exported
            // undefined bytes here, and treating the marker as corruption
            // would panic mid-rollforward.
            if redo_segment[i * FLAT_UNDO_ENTRY_SIZE as usize] == 0 {
                continue;
            }
            let entry = parse_entry(redo_segment, i);
            rollback_entry(&mut self.bitmap_env, state, &entry);
        }
    }

    /// vm.zig:3755 `vm_delta_export_entry_size`.
    pub const fn delta_export_entry_size() -> u32 {
        FLAT_UNDO_ENTRY_SIZE
    }
}

fn serialize_entries(entries: &[FlatUndoEntry], start: u32, count: u32) -> Vec<u8> {
    let mut out = vec![0u8; (count * FLAT_UNDO_ENTRY_SIZE) as usize];
    for i in 0..count as usize {
        let mut buf = [0u8; FLAT_UNDO_ENTRY_SIZE as usize];
        entries[start as usize + i].write_to(&mut buf);
        out[i * FLAT_UNDO_ENTRY_SIZE as usize..(i + 1) * FLAT_UNDO_ENTRY_SIZE as usize]
            .copy_from_slice(&buf);
    }
    out
}

fn parse_entry(segment: &[u8], i: usize) -> FlatUndoEntry {
    let mut buf = [0u8; FLAT_UNDO_ENTRY_SIZE as usize];
    buf.copy_from_slice(
        &segment[i * FLAT_UNDO_ENTRY_SIZE as usize..(i + 1) * FLAT_UNDO_ENTRY_SIZE as usize],
    );
    FlatUndoEntry::read_from(&buf)
        .unwrap_or_else(|| columine_types::die!("corrupt undo-entry op byte in delta segment"))
}

// =============================================================================
// Read / iteration exports (vm.zig:3358-3621)
// =============================================================================

/// vm.zig:3359 `vm_map_get`.
pub fn vm_map_get(state: &[u8], slot_offset: u32, capacity: u32, key: u32) -> u32 {
    let keys = slot_offset;
    let values = slot_offset + capacity * 4;
    let mut slot = columine_types::types::hash_key(key, capacity);
    for _ in 0..capacity {
        let k = bytes::read_u32(state, keys + slot * 4);
        if k == EMPTY_KEY {
            return EMPTY_KEY;
        }
        if k == key {
            return bytes::read_u32(state, values + slot * 4);
        }
        slot = (slot + 1) & (capacity - 1);
    }
    EMPTY_KEY
}

/// vm.zig:3382 `vm_set_contains`.
pub fn vm_set_contains(
    env: &mut BitmapEnv,
    state: &[u8],
    slot_offset: u32,
    capacity: u32,
    elem: u32,
) -> bool {
    if let Some(meta) = find_slot_meta_by_offset(state, slot_offset)
        && meta.slot_type() == SlotType::Bitmap
    {
        let storage = get_bitmap_storage(&meta);
        let _ = env;
        return storage
            .serialized_data(state)
            .is_some_and(|data| bitmap_ops::contains_serialized(data, elem));
    }

    let mut slot = columine_types::types::hash_key(elem, capacity);
    for _ in 0..capacity {
        let k = bytes::read_u32(state, slot_offset + slot * 4);
        if k == EMPTY_KEY {
            return false;
        }
        if k == elem {
            return true;
        }
        slot = (slot + 1) & (capacity - 1);
    }
    false
}

/// vm.zig:3414 `vm_map_iter_start` — ascending-slot scan, end = capacity.
pub fn vm_map_iter_start(state: &[u8], slot_offset: u32, capacity: u32) -> u32 {
    (0..capacity)
        .find(|&i| {
            let k = bytes::read_u32(state, slot_offset + i * 4);
            k != EMPTY_KEY && k != TOMBSTONE
        })
        .unwrap_or(capacity)
}

/// vm.zig:3435 `vm_map_iter_next`.
pub fn vm_map_iter_next(state: &[u8], slot_offset: u32, capacity: u32, current: u32) -> u32 {
    ((current + 1)..capacity)
        .find(|&i| {
            let k = bytes::read_u32(state, slot_offset + i * 4);
            k != EMPTY_KEY && k != TOMBSTONE
        })
        .unwrap_or(capacity)
}

/// vm.zig:3456 `vm_map_iter_get` — value in the high 32 bits, key low.
pub fn vm_map_iter_get(state: &[u8], slot_offset: u32, capacity: u32, pos: u32) -> u64 {
    let key = bytes::read_u32(state, slot_offset + pos * 4);
    let val = bytes::read_u32(state, slot_offset + capacity * 4 + pos * 4);
    (u64::from(val) << 32) | u64::from(key)
}

/// vm.zig:3472 `vm_set_iter_start` — BITMAP slots iterate by rank.
pub fn vm_set_iter_start(state: &[u8], slot_offset: u32, capacity: u32) -> u32 {
    if let Some(meta) = find_slot_meta_by_offset(state, slot_offset)
        && meta.slot_type() == SlotType::Bitmap
    {
        return if meta.size(state) == 0 { capacity } else { 0 };
    }
    vm_map_iter_start(state, slot_offset, capacity)
}

/// vm.zig:3489 `vm_set_iter_next`.
pub fn vm_set_iter_next(state: &[u8], slot_offset: u32, capacity: u32, current: u32) -> u32 {
    if let Some(meta) = find_slot_meta_by_offset(state, slot_offset)
        && meta.slot_type() == SlotType::Bitmap
    {
        let next = current + 1;
        return if next < meta.size(state) {
            next
        } else {
            capacity
        };
    }
    vm_map_iter_next(state, slot_offset, capacity, current)
}

/// vm.zig:3506 `vm_set_iter_get`.
pub fn vm_set_iter_get(state: &[u8], slot_offset: u32, pos: u32) -> u32 {
    if let Some(meta) = find_slot_meta_by_offset(state, slot_offset)
        && meta.slot_type() == SlotType::Bitmap
    {
        let storage = get_bitmap_storage(&meta);
        return bitmap_select(state, storage, pos).unwrap_or(EMPTY_KEY);
    }
    bytes::read_u32(state, slot_offset + pos * 4)
}

/// vm.zig:3526 `findSlotMetaByOffset`.
pub fn find_slot_meta_by_offset(state: &[u8], slot_offset: u32) -> Option<SlotMetaView> {
    let num_slots = state[StateHeaderOffset::NUM_SLOTS as usize];
    (0..num_slots)
        .map(|slot| SlotMetaView::read(state, slot))
        .find(|meta| meta.offset == slot_offset)
}

/// vm.zig:3566 `vm_struct_map_get_row_ptr` — row byte offset or 0xFFFFFFFF.
pub fn vm_struct_map_get_row_ptr(
    state: &[u8],
    slot_offset: u32,
    capacity: u32,
    num_fields: u32,
    row_size: u32,
    key: u32,
) -> u32 {
    let descriptor_size = align8(num_fields);
    let keys_off = slot_offset + descriptor_size;
    let rows_base = keys_off + capacity * 4;
    let mut slot = columine_types::types::hash_key(key, capacity);
    for _ in 0..capacity {
        let k = bytes::read_u32(state, keys_off + slot * 4);
        if k == EMPTY_KEY {
            return 0xFFFF_FFFF;
        }
        if k == key {
            return rows_base + slot * row_size;
        }
        slot = (slot + 1) & (capacity - 1);
    }
    0xFFFF_FFFF
}

/// vm.zig:3581 `vm_struct_map_iter_start`.
pub fn vm_struct_map_iter_start(
    state: &[u8],
    slot_offset: u32,
    capacity: u32,
    num_fields: u32,
) -> u32 {
    let keys_off = slot_offset + align8(num_fields);
    (0..capacity)
        .find(|&pos| {
            let k = bytes::read_u32(state, keys_off + pos * 4);
            k != EMPTY_KEY && k != TOMBSTONE
        })
        .unwrap_or(capacity)
}

/// vm.zig:3597 `vm_struct_map_iter_next`.
pub fn vm_struct_map_iter_next(
    state: &[u8],
    slot_offset: u32,
    capacity: u32,
    num_fields: u32,
    current: u32,
) -> u32 {
    let keys_off = slot_offset + align8(num_fields);
    ((current + 1)..capacity)
        .find(|&pos| {
            let k = bytes::read_u32(state, keys_off + pos * 4);
            k != EMPTY_KEY && k != TOMBSTONE
        })
        .unwrap_or(capacity)
}

/// vm.zig:3613 `vm_struct_map_iter_key`.
pub fn vm_struct_map_iter_key(state: &[u8], slot_offset: u32, num_fields: u32, pos: u32) -> u32 {
    bytes::read_u32(state, slot_offset + align8(num_fields) + pos * 4)
}

/// Exact pair point lookup for the two-key struct-map ABI.
pub fn vm_struct_map2_get_row_ptr(
    state: &[u8],
    slot_offset: u32,
    capacity: u32,
    num_fields: u32,
    row_size: u32,
    key1: u32,
    key2: u32,
) -> u32 {
    if key1 == EMPTY_KEY || key1 == TOMBSTONE {
        return u32::MAX;
    }
    let descriptor_size = align8(num_fields);
    let keys1 = slot_offset + descriptor_size;
    let keys2 = keys1 + capacity * 4;
    let rows = keys2 + capacity * 4;
    let mut pos = columine_types::types::hash_key_pair(key1, key2, capacity);
    for _ in 0..capacity {
        let first = bytes::read_u32(state, keys1 + pos * 4);
        if first == EMPTY_KEY {
            return u32::MAX;
        }
        if first == key1 && bytes::read_u32(state, keys2 + pos * 4) == key2 {
            return rows + pos * row_size;
        }
        pos = (pos + 1) & (capacity - 1);
    }
    u32::MAX
}

pub fn vm_struct_map2_iter_start(
    state: &[u8],
    slot_offset: u32,
    capacity: u32,
    num_fields: u32,
) -> u32 {
    vm_struct_map_iter_start(state, slot_offset, capacity, num_fields)
}

pub fn vm_struct_map2_iter_next(
    state: &[u8],
    slot_offset: u32,
    capacity: u32,
    num_fields: u32,
    current: u32,
) -> u32 {
    vm_struct_map_iter_next(state, slot_offset, capacity, num_fields, current)
}

pub fn vm_struct_map2_iter_key1(state: &[u8], slot_offset: u32, num_fields: u32, pos: u32) -> u32 {
    vm_struct_map_iter_key(state, slot_offset, num_fields, pos)
}

pub fn vm_struct_map2_iter_key2(
    state: &[u8],
    slot_offset: u32,
    capacity: u32,
    num_fields: u32,
    pos: u32,
) -> u32 {
    let keys1 = slot_offset + align8(num_fields);
    bytes::read_u32(state, keys1 + capacity * 4 + pos * 4)
}
