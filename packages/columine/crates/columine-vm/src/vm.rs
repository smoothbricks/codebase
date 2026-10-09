//! Reducer VM dispatch loop, runtime undo/delta machinery, TTL eviction, and
//! read/iteration operations.
//!
//! [`Vm`] owns [`UndoState`] and [`bitmap_ops::BitmapEnv`]. Bindings keep one
//! long-lived instance per module instance.
//!
//! Mutations reserve bounded physical before-byte records before writing.
//! Budget refusal leaves the accepted prefix exact and rollbackable.
//!
//! The allocator handles temporary bitmap storage; no separate scratch reset
//! step is observable at entrypoint boundaries.

use crate::aggregates::{self, AggKind, TypeMask};
use crate::bitmap_ops::{
    self, BitmapAlgebraOp, BitmapEnv, BitmapSource, batch_bitmap_add, batch_bitmap_algebra,
    batch_bitmap_remove, get_bitmap_storage,
};
use crate::bytes;
use crate::capacity::{Admission, CapacityCounts, CapacityScratch};
use crate::hash_table;
use crate::hashmap_ops::{self, CmpType, Strategy};
use crate::hashset_ops;
use crate::hooks::{ByteCapture, CaptureFrame, EraseScratch, VmHooks};
use crate::meta::{SlotMetaView, slot_meta_base};
use crate::nested;
use crate::row_exprs::LiveColumns;
use crate::state_init::{self, EVICTION_ENTRY_SIZE, NEEDS_GROWTH_SLOT};
use crate::struct_map::{
    ArenaRegion, StructMap2Slot, StructMapSlot, scalar_cell_encoded, scalar_cell_order_key,
    scalar_order_key,
};
use crate::ttl_plan::{TtlBatchPlan, TtlBatchScratch, consume_ttl_batch, prepare_ttl_batch};
use crate::undo_log::{FLAT_UNDO_ENTRY_SIZE, FlatUndoEntry, FlatUndoOp};
use bitmosaic::BitmosaicView;
use columine_types::DEFAULT_ACCEPTED_PROGRAM_MAGICS;
use columine_types::operand::Operands;

use crate::element_body::{
    BodyPlan, OperandRead, Skip, WideOperands, plan_element_body, walk_operands, widen_element_body,
};
use columine_types::types::{
    AggType, ChangeFlag, EMPTY_KEY, ErrorCode, MAX_GUARD_COMPONENTS, MAX_SCATTER_ROUTES,
    NO_PARENT_TS_COL, Opcode, ProgramHeader, SLOT_META_SIZE, STATE_FORMAT_VERSION, STATE_MAGIC,
    SlotMetaOffset, SlotType, StateHeaderOffset, StructFieldType, TOMBSTONE, align8,
    arena_elem_size, is_arena_field_type, state_num_slots, struct_field_size,
};
use core::sync::atomic::Ordering;

/// Read one operand of an instruction [`body_op_len`] has already measured,
/// or refuse the program: a `None` here is a malformed operand.
macro_rules! operand {
    ($read:expr) => {
        match $read {
            Some(value) => value,
            None => return INVALID_PROGRAM,
        }
    };
}

// =============================================================================
// Input columns — bounded typed views
// =============================================================================

/// The VM's state, program, and column bytes are little-endian. Typed views
/// therefore require aligned input and are only available on little-endian
/// targets.
#[cfg(target_endian = "big")]
compile_error!("columine-vm input-column views require a little-endian target");

/// View a batch column as u32 cells.
///
/// Section columns (FOR_EACH/scatter sources) may be shorter than `batch_len`;
/// each operation reads the extent its semantics require. The view clamps to
/// complete cells and exposes the shorter extent. An operation that
/// over-indexes then panics at the access, making the boundary error visible.
pub fn col_u32(col: &[u8], batch_len: u32) -> &[u32] {
    let usable = (col.len() & !3).min(batch_len as usize * 4);
    // SAFETY: length within `col`; alignment checked via the `prefix`
    // assert below; u32 has no invalid bit patterns; the returned slice
    // borrows `col`, so no lifetime extension happens.
    let (prefix, cells, _) = unsafe { col[..usable].align_to::<u32>() };
    columine_types::check!(prefix.is_empty(), "u32 column misaligned");
    cells
}

/// View a batch column as f64 cells; see [`col_u32`] for extent rules.
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

/// View a batch column as i64 cells; see [`col_u32`] for extent rules.
pub fn col_i64(col: &[u8], batch_len: u32) -> &[i64] {
    let usable = (col.len() & !7).min(batch_len as usize * 8);
    // SAFETY: as in `col_u32`.
    let (prefix, cells, _) = unsafe { col[..usable].align_to::<i64>() };
    columine_types::check!(prefix.is_empty(), "i64 column misaligned");
    cells
}

/// Batch-extent views: the op indexes rows `0..batch_len`, so the column MUST
/// cover the whole batch. `None` is a malformed batch — the caller answers
/// with `ErrorCode::ColumnUnderrun` instead of the clamped view's panic. The
/// clamped `col_*` views above stay for section columns, whose extent is the
/// op's own (`ForEach` array offsets, scatter sources); the two contracts are
/// separate functions so a call site names which one it means.
pub fn col_u32_exact(col: &[u8], batch_len: u32) -> Option<&[u32]> {
    let cells = col_u32(col, batch_len);
    (cells.len() >= batch_len as usize).then_some(cells)
}

pub fn col_f64_exact(col: &[u8], batch_len: u32) -> Option<&[f64]> {
    let cells = col_f64(col, batch_len);
    (cells.len() >= batch_len as usize).then_some(cells)
}

pub fn col_i64_exact(col: &[u8], batch_len: u32) -> Option<&[i64]> {
    let cells = col_i64(col, batch_len);
    (cells.len() >= batch_len as usize).then_some(cells)
}

/// Take a batch-extent view inside a `-> u32` VM dispatch body, refusing the
/// batch with `ColumnUnderrun` when the column cannot cover it.
macro_rules! batch_col {
    ($view:expr) => {
        match $view {
            Some(cells) => cells,
            None => return ErrorCode::ColumnUnderrun as u32,
        }
    };
}

/// A raw comparison/timestamp column checked to cover `batch_len` values at
/// `cmp_type`'s stride, so `read_cmp_value` stays in bounds by construction.
fn cmp_col_exact(col: &[u8], batch_len: u32, cmp_type: CmpType) -> Option<&[u8]> {
    (col.len() >= batch_len as usize * cmp_type.stride()).then_some(col)
}

/// Return bounded column views. Empty batches may provide fewer pointers than
/// the program references because no column is dereferenced; out-of-range
/// entries therefore resolve to an empty column. With a non-empty batch, a
/// missing pointer is an FFI-boundary bug and the typed-view length assertion
/// reports it.
#[inline(always)]
pub fn col_at<'a>(cols: &[&'a [u8]], idx: usize) -> &'a [u8] {
    cols.get(idx).copied().unwrap_or(&[])
}

// =============================================================================
// Eviction entries — 16-byte little-endian records

/// `EvictionEntry` layout: `timestamp:f64 @0, key_or_idx:u32 @8, value:u32 @12`
/// (layout pinned in columine-types; byte size is `state_init::EVICTION_ENTRY_SIZE`).
/// Accessors below address entry `i` of the index/buffer starting at `base`.
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
// TTL eviction operations
// =============================================================================

/// Shift eviction entries left after removing an item.
fn shift_eviction_left(state: &mut [u8], base: u32, count: u32, size: u32) {
    if count >= size {
        return;
    }
    for i in 0..size - count {
        evict_copy(state, base, i, i + count);
    }
}

/// Find the latest eviction timestamp for a key, if present.
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

/// Return whether an eviction entry is current (no newer entry for its key).
fn is_eviction_entry_current(state: &[u8], base: u32, size: u32, entry_idx: u32, key: u32) -> bool {
    ((entry_idx + 1)..size).all(|i| evict_key(state, base, i) != key)
}

/// Remove only primary payload bytes. Metadata and TTL have one enclosing
/// owner; each paid probe/patch plan is consumed once within that authority.
fn remove_entry_by_key(
    delta_mode: bool,
    undo: &mut UndoState,
    env: &mut BitmapEnv,
    state: &mut [u8],
    meta: &SlotMetaView,
    key: u32,
) -> Result<bool, ErrorCode> {
    let EraseScratch {
        plan: mut erase,
        mut ranges,
    } = core::mem::take(&mut undo.erase_scratch);
    ranges.clear();
    let result = (|| match meta.slot_type() {
        SlotType::HashMap | SlotType::HashSet => {
            let table = if meta.slot_type() == SlotType::HashMap {
                hashmap_ops::bind_slot_map(meta)
            } else {
                hashset_ops::bind_slot_set(meta)
            };
            let Some(position) = table.find(state, key) else {
                return Ok(false);
            };
            table.plan_erase_at(state, position, &mut erase)?;
            let reserved = table.erase_ranges(&erase, &mut ranges)?;
            if meta.has_hashmap_timestamp_storage() {
                erase.for_each_destination_run(|start, len| {
                    ranges.push((hashmap_ops::cmp_lane_off(meta) + start * 8, len * 8));
                });
            }
            debug_assert_eq!(
                ranges.capacity(),
                reserved,
                "erase capture ranges grew past their reservation"
            );
            let capture = undo.begin_capture(state, &ranges)?;
            if meta.slot_type() == SlotType::HashMap {
                hashmap_ops::erase_slot_map_planned(state, meta, &table, &erase);
            } else {
                table.erase_planned(state, &erase, |_, _, _| {});
            }
            undo.finish_capture(delta_mode, state, &ranges, capture);
            Ok(true)
        }
        SlotType::Bitmap => {
            let storage = get_bitmap_storage(meta);
            let removes = [key];
            let prepared = bitmap_ops::prepare_bitmap_patch(env, state, storage, &[], &removes)?;
            let mut tail = prepared.cleared_tail_range();
            for &(offset, len) in prepared.mutation_ranges() {
                if let Some((tail_offset, tail_len)) = tail
                    && tail_offset <= offset
                {
                    bitmap_ops::push_payload_capture_range(
                        &mut ranges,
                        storage.payload_offset(),
                        tail_offset,
                        tail_len,
                    )?;
                    tail = None;
                }
                bitmap_ops::push_payload_capture_range(
                    &mut ranges,
                    storage.payload_offset(),
                    offset,
                    len,
                )?;
            }
            if let Some((offset, len)) = tail {
                bitmap_ops::push_payload_capture_range(
                    &mut ranges,
                    storage.payload_offset(),
                    offset,
                    len,
                )?;
            }
            let capture = undo.begin_capture(prepared.state(), &ranges)?;
            let (report, state) = prepared.apply_with_cleared_tail();
            undo.finish_capture(delta_mode, state, &ranges, capture);
            Ok(report.removed == 1)
        }
        SlotType::Array => {
            if key >= meta.capacity {
                return Ok(false);
            }
            let offset = meta
                .offset
                .checked_add(key.checked_mul(4).ok_or(ErrorCode::InvalidState)?)
                .ok_or(ErrorCode::InvalidState)?;
            if offset
                .checked_add(4)
                .is_none_or(|end| end as usize > state.len())
            {
                return Err(ErrorCode::InvalidState);
            }
            let current = bytes::read_u32(state, offset);
            if current == EMPTY_KEY || current == TOMBSTONE {
                return Ok(false);
            }
            ranges.push((offset, 4));
            let capture = undo.begin_capture(state, &ranges)?;
            bytes::write_u32(state, offset, TOMBSTONE);
            undo.finish_capture(delta_mode, state, &ranges, capture);
            Ok(true)
        }
        SlotType::ConditionTree => {
            ranges.push((meta.offset, 8));
            let capture = undo.begin_capture(state, &ranges)?;
            let generation = bytes::read_u32(state, meta.offset).wrapping_add(1);
            bytes::write_u32(state, meta.offset, generation);
            bytes::write_u32(state, meta.offset + 4, key);
            undo.finish_capture(delta_mode, state, &ranges, capture);
            Ok(true)
        }
        SlotType::Aggregate
        | SlotType::StructMap
        | SlotType::StructMap2
        | SlotType::OrderedList
        | SlotType::Scalar
        | SlotType::Nested => Ok(false),
    })();
    undo.erase_scratch = EraseScratch {
        plan: erase,
        ranges,
    };
    result
}

/// TTL metadata, index shifts and trigger destinations have one outer owner.
/// A refusing payload write is never applied; this owner still closes the
/// exact accepted prefix, including its processed-index compaction.
pub(crate) fn evict_expired(
    delta_mode: bool,
    undo: &mut UndoState,
    env: &mut BitmapEnv,
    state: &mut [u8],
    meta: &SlotMetaView,
    now: f64,
) -> Result<u32, ErrorCode> {
    if !meta.has_ttl() {
        return Ok(0);
    }
    if !matches!(
        meta.slot_type(),
        SlotType::HashMap | SlotType::HashSet | SlotType::Bitmap
    ) {
        return Err(ErrorCode::InvalidState);
    }
    crate::hooks::ttl_capture_ranges(state, meta, 0)?;
    let cutoff = meta.cutoff(state, now);
    let base = meta.eviction_index_offset(state);
    let initial = meta.eviction_index_size(state);
    let mut expired = 0u32;
    while expired < initial
        && !matches!(
            evict_ts(state, base, expired).partial_cmp(&cutoff),
            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal),
        )
    {
        expired += 1;
    }
    // A budget refusal may end this operation at any processed prefix. The
    // maximum actual left-shift destination is the live prefix minus one,
    // never allocated capacity or the already-dead tail.
    let shift_len = if expired == 0 {
        0
    } else {
        (initial - 1)
            .checked_mul(EVICTION_ENTRY_SIZE)
            .ok_or(ErrorCode::InvalidState)?
    };
    let buffer_len = if meta.has_evict_trigger() {
        expired
            .min(state_init::EVICTED_BUFFER_CAP)
            .checked_mul(EVICTION_ENTRY_SIZE)
            .ok_or(ErrorCode::InvalidState)?
    } else {
        0
    };
    let ranges = [
        (
            meta.meta_base + SlotMetaOffset::SIZE,
            if expired == 0 { 0 } else { 4 },
        ),
        (
            meta.meta_base + SlotMetaOffset::CHANGE_FLAGS,
            if expired == 0 { 0 } else { 1 },
        ),
        (
            meta.meta_base + SlotMetaOffset::EVICTION_INDEX_SIZE,
            if expired == 0 { 0 } else { 4 },
        ),
        (meta.meta_base + SlotMetaOffset::EVICTED_COUNT, 4),
        (base, shift_len),
        (
            if buffer_len == 0 {
                0
            } else {
                meta.evicted_buffer_offset(state)
            },
            buffer_len,
        ),
    ];
    undo.prepare_redo(delta_mode)?;
    let capture = undo.begin_capture(state, &ranges)?;
    meta.set_evicted_count(state, 0);
    let mut processed = 0u32;
    let mut removed = 0u32;
    let mut error = None;
    while processed < expired {
        let key = evict_key(state, base, processed);
        if !is_eviction_entry_current(state, base, initial, processed, key) {
            processed += 1;
            continue;
        }
        if meta.has_evict_trigger() && removed == state_init::EVICTED_BUFFER_CAP {
            error = Some(ErrorCode::CapacityExceeded);
            break;
        }
        match remove_entry_by_key(delta_mode, undo, env, state, meta, key) {
            Ok(true) => {
                if meta.has_evict_trigger() {
                    let buffer = meta.evicted_buffer_offset(state);
                    let timestamp = evict_ts(state, base, processed);
                    let value = evict_value(state, base, processed);
                    evict_write(state, buffer, removed, timestamp, key, value);
                    meta.set_evicted_count(state, removed + 1);
                }
                removed += 1;
            }
            Ok(false) => {}
            Err(failure) => {
                error = Some(failure);
                break;
            }
        }
        processed += 1;
    }
    if processed != 0 {
        shift_eviction_left(state, base, processed, initial);
        meta.set_eviction_index_size(state, initial - processed);
    }
    if removed != 0 {
        if let Some(size) = meta.size(state).checked_sub(removed) {
            meta.set_size(state, size);
            meta.set_change_flag(state, ChangeFlag::EVICTED);
        } else {
            error = Some(ErrorCode::InvalidState);
        }
    }
    undo.finish_capture(delta_mode, state, &ranges, capture);
    match error {
        Some(error) => Err(error),
        None => Ok(removed),
    }
}
//#endregion vm-architecture-ttl.evict

// =============================================================================
// Derived-facts header access

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

/// Write the derived-facts header used when loading a RETE program.
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
// Undo/delta runtime state
// =============================================================================

/// Maximum number of undo entries one window may hold. A logical budget:
/// journal storage grows to actual demand and is admitted before any write.
pub const UNDO_CAPACITY: u32 = 16384;

/// Source of window generations, unique across every journal in the process,
/// so a capture frame names exactly one window of exactly one journal.
static NEXT_GENERATION: core::sync::atomic::AtomicU64 = core::sync::atomic::AtomicU64::new(1);

fn next_generation() -> u64 {
    NEXT_GENERATION.fetch_add(1, Ordering::Relaxed)
}

/// Read-only lifecycle witness for the scenario lane, not record authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedoLaneStatus {
    pub active: bool,
    pub entries: usize,
    pub capacity: usize,
}

/// Read-only journal facts. Mutation authority remains with the owning VM.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UndoStatus {
    pub enabled: bool,
    pub overflow: bool,
    pub checkpoint: u32,
    pub delta_count: u32,
}

/// Owned, bounded physical undo/delta state for one VM instance.
#[derive(Debug, Default)]
pub(crate) struct UndoState {
    entries: Vec<FlatUndoEntry>,
    redo: Vec<Option<FlatUndoEntry>>,
    redo_active: bool,
    delta_count: u32,
    overflow: bool,
    enabled: bool,
    state_size: u32,
    saved_change_flags: Vec<u8>,
    reserved_entries: u32,
    capture_depth: u32,
    generation: u64,
    export_start: u32,
    export_count: u32,
    export_overflow: bool,
    capture_scratch: Vec<u8>,
    capture_ranges: Vec<(u32, u32)>,
    ttl_batch_scratch: TtlBatchScratch,
    erase_scratch: EraseScratch,
}

impl UndoState {
    pub fn count(&self) -> u32 {
        self.entries.len() as u32
    }

    pub fn delta_count(&self) -> u32 {
        self.delta_count
    }

    fn reserve_entries(&mut self, count: u32) -> Result<(), ErrorCode> {
        if self.overflow {
            return Err(ErrorCode::JournalOverflow);
        }
        if self
            .count()
            .checked_add(self.reserved_entries)
            .and_then(|total| total.checked_add(count))
            .is_none_or(|total| total > UNDO_CAPACITY)
        {
            self.overflow = true;
            return Err(ErrorCode::JournalOverflow);
        }
        // Back the admitted entries with storage now, grown geometrically to
        // actual demand, so no later push allocates. Storage that cannot be
        // had refuses like the budget: before any write, and sticky.
        let total = self.entries.len() + (self.reserved_entries + count) as usize;
        if self
            .entries
            .try_reserve(total - self.entries.len())
            .is_err()
            || (self.redo_active && self.redo.try_reserve(total - self.redo.len()).is_err())
        {
            self.overflow = true;
            return Err(ErrorCode::JournalOverflow);
        }
        self.reserved_entries += count;
        Ok(())
    }

    /// Admit scenario storage before its first writer, and before any entry
    /// reservation that must cover it. Pure production windows allocate and
    /// write no redo buffer.
    fn prepare_redo(&mut self, delta_mode: bool) -> Result<(), ErrorCode> {
        if !self.enabled || !delta_mode || self.redo_active {
            return Ok(());
        }
        debug_assert!(self.redo.is_empty());
        let total = self.entries.len() + self.reserved_entries as usize;
        if self.redo.try_reserve(total).is_err() {
            self.overflow = true;
            return Err(ErrorCode::JournalOverflow);
        }
        self.redo.resize(self.entries.len(), None);
        self.redo_active = true;
        Ok(())
    }

    fn push_reserved(&mut self, undo: FlatUndoEntry, redo: Option<FlatUndoEntry>) {
        assert!(
            self.entries.len() < UNDO_CAPACITY as usize,
            "physical journal reservation lost"
        );
        debug_assert!(
            redo.is_none() || self.redo_active,
            "paired lane must be admitted before writing"
        );
        debug_assert!(
            self.entries.len() < self.entries.capacity(),
            "undo entry storage must be admitted before writing"
        );
        self.entries.push(undo);
        if self.redo_active {
            debug_assert_eq!(self.redo.len() + 1, self.entries.len());
            debug_assert!(
                self.redo.len() < self.redo.capacity(),
                "redo storage must be admitted before writing"
            );
            self.redo.push(redo);
        }
        if redo.is_some() {
            self.delta_count = self.count();
        }
    }

    fn boundary_flag(&self, offset: u32) -> bool {
        if self.saved_change_flags.is_empty() {
            return false;
        }
        if offset == StateHeaderOffset::DERIVED_FACTS_CHANGE_FLAG {
            return true;
        }
        let first = columine_types::types::STATE_HEADER_SIZE + SlotMetaOffset::CHANGE_FLAGS;
        offset.checked_sub(first).is_some_and(|relative| {
            relative % SLOT_META_SIZE == 0
                && (relative / SLOT_META_SIZE) < self.saved_change_flags.len() as u32 - 1
        })
    }

    fn chunk_len(&self, offset: u32, remaining: u32) -> u32 {
        let max = remaining.min(8);
        (1..max)
            .find(|relative| self.boundary_flag(offset + relative))
            .unwrap_or(max)
    }

    /// Reserve all possible changed chunks before copying any before-bytes.
    /// Frames are disjoint and LIFO; a child cannot overwrite a parent's scratch.
    pub(crate) fn begin_capture(
        &mut self,
        state: &[u8],
        ranges: &[(u32, u32)],
    ) -> Result<ByteCapture, ErrorCode> {
        let state_size = u32::try_from(state.len()).map_err(|_| ErrorCode::InvalidState)?;
        if self.enabled && self.overflow {
            return Err(ErrorCode::JournalOverflow);
        }
        if self.enabled && state_size != self.state_size {
            return Err(ErrorCode::InvalidState);
        }
        let mut count = 0u32;
        for (index, &(offset, len)) in ranges.iter().enumerate() {
            let end = offset.checked_add(len).ok_or(ErrorCode::InvalidState)?;
            if end > state_size {
                return Err(ErrorCode::InvalidState);
            }
            if len == 0 {
                continue;
            }
            for &(other, size) in &ranges[..index] {
                let other_end = other.checked_add(size).ok_or(ErrorCode::InvalidState)?;
                if size != 0 && offset < other_end && other < end {
                    return Err(ErrorCode::InvalidState);
                }
            }
            for &(other, size) in &self.capture_ranges {
                if offset < other + size && other < end {
                    return Err(ErrorCode::InvalidState);
                }
            }
            if !self.enabled {
                continue;
            }
            let mut cursor = offset;
            while cursor < end {
                if self.boundary_flag(cursor) {
                    cursor += 1;
                } else {
                    count = count.checked_add(1).ok_or(ErrorCode::InvalidState)?;
                    cursor += self.chunk_len(cursor, end - cursor);
                }
            }
        }
        if !self.enabled {
            return Ok(ByteCapture { frame: None });
        }
        // Before-bytes and their ranges never exceed the declared bytes and
        // the reserved chunks; both are backed before the budget is taken.
        let bytes: usize = ranges.iter().map(|&(_, len)| len as usize).sum();
        if self.overflow
            || self.capture_scratch.try_reserve(bytes).is_err()
            || self.capture_ranges.try_reserve(count as usize).is_err()
        {
            self.overflow = true;
            return Err(ErrorCode::JournalOverflow);
        }
        self.reserve_entries(count)?;
        let frame = CaptureFrame {
            generation: self.generation,
            image: state.as_ptr() as usize,
            depth: self.capture_depth,
            scratch_start: self.capture_scratch.len(),
            ranges_start: self.capture_ranges.len(),
            entries: count,
        };
        self.capture_depth += 1;
        for &(offset, len) in ranges {
            let end = offset + len;
            let mut cursor = offset;
            while cursor < end {
                if self.boundary_flag(cursor) {
                    cursor += 1;
                } else {
                    let size = self.chunk_len(cursor, end - cursor);
                    debug_assert!(
                        self.capture_ranges.len() < self.capture_ranges.capacity()
                            && usize::try_from(size).is_ok_and(|size| {
                                self.capture_scratch.capacity() - self.capture_scratch.len() >= size
                            }),
                        "capture storage must be admitted before copying"
                    );
                    self.capture_ranges.push((cursor, size));
                    self.capture_scratch
                        .extend_from_slice(&state[cursor as usize..(cursor + size) as usize]);
                    cursor += size;
                }
            }
        }
        Ok(ByteCapture { frame: Some(frame) })
    }

    /// Consume one physical authority. No budget decision is possible after write.
    pub(crate) fn finish_capture(
        &mut self,
        delta_mode: bool,
        state: &[u8],
        ranges: &[(u32, u32)],
        capture: ByteCapture,
    ) {
        let Some(frame) = capture.frame else {
            return;
        };
        assert_eq!(
            frame.generation, self.generation,
            "capture belongs to another undo window"
        );
        assert_eq!(
            frame.image,
            state.as_ptr() as usize,
            "capture belongs to another state image"
        );
        assert_eq!(
            frame.depth + 1,
            self.capture_depth,
            "capture must finish in LIFO order"
        );
        assert_eq!(
            state.len(),
            self.state_size as usize,
            "captured image changed layout"
        );
        let mut scratch = frame.scratch_start;
        for index in frame.ranges_start..self.capture_ranges.len() {
            let (offset, len) = self.capture_ranges[index];
            assert!(
                ranges
                    .iter()
                    .any(|&(start, size)| offset >= start && offset + len <= start + size),
                "capture finished with another range plan"
            );
            let before = &self.capture_scratch[scratch..scratch + len as usize];
            let after = &state[offset as usize..(offset + len) as usize];
            if before != after {
                let undo = state_bytes_entry(offset, len as u8, before);
                let redo = delta_mode.then(|| state_bytes_entry(offset, len as u8, after));
                self.push_reserved(undo, redo);
            }
            scratch += len as usize;
        }
        self.capture_scratch.truncate(frame.scratch_start);
        self.capture_ranges.truncate(frame.ranges_start);
        self.reserved_entries -= frame.entries;
        self.capture_depth -= 1;
    }

    fn open_change_flags(&mut self, state: &[u8]) -> Result<bool, ErrorCode> {
        if !self.enabled {
            return Ok(false);
        }
        if self.overflow {
            return Err(ErrorCode::JournalOverflow);
        }
        if state.len() != self.state_size as usize
            || state.len() < columine_types::types::STATE_HEADER_SIZE as usize
        {
            return Err(ErrorCode::InvalidState);
        }
        if !self.saved_change_flags.is_empty() {
            return Ok(false);
        }
        let slots = state_num_slots(state);
        let end = slots
            .checked_mul(SLOT_META_SIZE)
            .and_then(|len| len.checked_add(columine_types::types::STATE_HEADER_SIZE))
            .ok_or(ErrorCode::InvalidState)?;
        if end as usize > state.len() {
            return Err(ErrorCode::InvalidState);
        }
        let count = slots.checked_add(1).ok_or(ErrorCode::InvalidState)?;
        self.reserve_entries(count)?;
        self.saved_change_flags.extend(
            (0..slots)
                .map(|slot| state[(slot_meta_base(slot) + SlotMetaOffset::CHANGE_FLAGS) as usize]),
        );
        self.saved_change_flags
            .push(state[StateHeaderOffset::DERIVED_FACTS_CHANGE_FLAG as usize]);
        Ok(true)
    }

    fn finish_change_flags(&mut self, delta_mode: bool, state: &[u8]) {
        for index in 0..self.saved_change_flags.len() {
            let offset = if index + 1 == self.saved_change_flags.len() {
                StateHeaderOffset::DERIVED_FACTS_CHANGE_FLAG
            } else {
                slot_meta_base(index as u32) + SlotMetaOffset::CHANGE_FLAGS
            };
            let before = self.saved_change_flags[index];
            let after = state[offset as usize];
            if before != after {
                self.push_reserved(
                    state_bytes_entry(offset, 1, &[before]),
                    delta_mode.then(|| state_bytes_entry(offset, 1, &[after])),
                );
            }
        }
        self.reserved_entries -= self.saved_change_flags.len() as u32;
        self.saved_change_flags.clear();
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

/// One long-lived reducer VM instance.
#[derive(Debug)]
pub struct Vm {
    pub(crate) undo: UndoState,
    pub bitmap_env: BitmapEnv,
    accepted_program_magics: &'static [u32],
    /// Reused physical program/shape declaration admission scratch.
    declared_slots: Vec<bool>,
    /// The plan of the FOR_EACH body being run (`element_body`), made once
    /// per batch; kept so a batch reuses its allocations.
    element_plan: BodyPlan,
    /// Decode targets of the per-element instructions that carry fixed
    /// operand arrays: an element decodes into these instead of building
    /// (zeroing and copying) the arrays per row.
    element_struct_map: StructMapUpsertOperands,
    element_struct_map2: StructMap2UpsertOperands,
    element_struct_map2_max: StructMap2MaxI64x2Operands,
    element_list_vals: ScalarOperands,
    nested_scratch: nested::NestedScratch,
    /// Batch capacity admission (`capacity`), retained across batches.
    capacity: CapacityScratch,
}

impl Vm {
    /// Construct a VM that admits exactly the supplied program magic values.
    ///
    /// The slice is `'static` so `Vm` does not carry a viral lifetime parameter;
    /// embedders with a runtime-computed set can provide a leaked boxed slice.
    pub fn new(accepted_program_magics: &'static [u32]) -> Self {
        Self {
            undo: UndoState::default(),
            bitmap_env: BitmapEnv::default(),
            accepted_program_magics,
            declared_slots: Vec::new(),
            element_plan: BodyPlan::default(),
            element_struct_map: StructMapUpsertOperands::EMPTY,
            element_struct_map2: StructMap2UpsertOperands::EMPTY,
            element_struct_map2_max: StructMap2MaxI64x2Operands::EMPTY,
            element_list_vals: ScalarOperands::EMPTY,
            nested_scratch: nested::NestedScratch::default(),
            capacity: CapacityScratch::default(),
        }
    }
}

impl Default for Vm {
    fn default() -> Self {
        Self::new(DEFAULT_ACCEPTED_PROGRAM_MAGICS)
    }
}

/// The VM's undo lane as the container-ops hook boundary. It borrows only
/// `undo`, so a bitmap operation can take `&mut self.bitmap_env` alongside it
/// as a split borrow instead of a throwaway environment.
pub(crate) struct VmCtx<'a> {
    pub(crate) undo: &'a mut UndoState,
}

impl Vm {
    /// Container operations capture their flags physically unless an enclosing
    /// [`Vm::change_flag_boundary`] has reserved their exclusive flag owner.
    pub(crate) fn ctx(&mut self) -> VmCtx<'_> {
        VmCtx {
            undo: &mut self.undo,
        }
    }

    /// Reserve the declared physical ranges before calling the writer and
    /// journal every changed byte on every normal return, including an error
    /// value or an early-stop value returned as `T`.
    ///
    /// The writer MUST mutate only the declared ranges. It cannot access the
    /// VM's journal, cancel this capture, or publish an unjournaled prefix.
    ///
    /// Outside a batch, no internal flag boundary is open: a writer that
    /// changes slot or derived-fact flags MUST include those actual flag bytes
    /// in `ranges`. Flag bytes are filtered from a capture only when an
    /// internal boundary already owns and has reserved their journal records.
    /// Returning an error stops the writer; it does not discard prior writes.
    /// Those writes are always captured on normal callback return.
    pub fn capture_mutation<T>(
        &mut self,
        delta_mode: bool,
        state: &mut [u8],
        ranges: &[(u32, u32)],
        mutate: impl FnOnce(&mut [u8]) -> T,
    ) -> Result<T, ErrorCode> {
        self.undo.prepare_redo(delta_mode)?;
        let capture = self.undo.begin_capture(state, ranges)?;
        let result = mutate(state);
        self.undo.finish_capture(delta_mode, state, ranges, capture);
        Ok(result)
    }

    /// Run `mutate` as one change-flag boundary. The slot change flags and the
    /// derived-facts flag it leaves different from how it found them are
    /// journaled at its close as exact before-bytes (and, on the delta lane,
    /// after-bytes), so a rollback to a checkpoint taken before it restores
    /// them and a rollback to one taken after leaves them. The outermost
    /// boundary owns the journal: one opened inside it only runs `mutate`, so
    /// a flag is never journaled twice.
    ///
    /// Every mutating entrypoint is one: the `execute_batch` family,
    /// [`Vm::set_insert`], [`Vm::set_remove`] and [`Vm::evict_all_expired`].
    /// A reservation refusal returns before `mutate` runs. Raw extension
    /// writers acquire their own bounded physical capture before writing.
    pub(crate) fn change_flag_boundary<T>(
        &mut self,
        delta_mode: bool,
        state: &mut [u8],
        mutate: impl FnOnce(&mut Vm, &mut [u8]) -> T,
    ) -> Result<T, ErrorCode> {
        self.undo.prepare_redo(delta_mode)?;
        let owner = self.undo.open_change_flags(state)?;
        let result = mutate(self, state);
        if owner {
            self.undo.finish_change_flags(delta_mode, state);
        }
        Ok(result)
    }

    /// Upsert a parallel key/value batch into an authoritatively bound map.
    /// Comparison and TTL columns use the supplied comparison encoding.
    #[allow(clippy::too_many_arguments)]
    pub fn map_upsert(
        &mut self,
        strategy: Strategy,
        delta_mode: bool,
        state: &mut [u8],
        slot: u32,
        keys: &[u32],
        vals: &[u32],
        cmp_col: Option<&[u8]>,
        cmp_type: CmpType,
    ) -> ErrorCode {
        let meta = match state_init::bind_slot(state, slot) {
            Ok(meta) => meta,
            Err(error) => return error,
        };
        if meta.slot_type() != SlotType::HashMap {
            return ErrorCode::InvalidSlot;
        }
        if keys.len() != vals.len() || u32::try_from(keys.len()).is_err() {
            return ErrorCode::ColumnUnderrun;
        }
        if strategy.needs_timestamps() || meta.has_ttl() {
            let Some(required) = keys.len().checked_mul(cmp_type.stride()) else {
                return ErrorCode::ColumnUnderrun;
            };
            if cmp_col.is_none_or(|column| column.len() < required) {
                return ErrorCode::ColumnUnderrun;
            }
        }
        self.change_flag_boundary(delta_mode, state, |vm, state| {
            hashmap_ops::batch_map_upsert(
                strategy,
                delta_mode,
                state,
                &meta,
                slot,
                keys,
                vals,
                cmp_col,
                cmp_type,
                &mut vm.ctx(),
            )
        })
        .unwrap_or_else(|error| error)
    }

    /// Remove keys through the same checked map and flag/capture authority.
    pub fn map_remove(
        &mut self,
        delta_mode: bool,
        state: &mut [u8],
        slot: u32,
        keys: &[u32],
    ) -> ErrorCode {
        let meta = match state_init::bind_slot(state, slot) {
            Ok(meta) => meta,
            Err(error) => return error,
        };
        if meta.slot_type() != SlotType::HashMap {
            return ErrorCode::InvalidSlot;
        }
        self.change_flag_boundary(delta_mode, state, |vm, state| {
            hashmap_ops::batch_map_remove(delta_mode, state, &meta, slot, keys, &mut vm.ctx())
        })
        .unwrap_or_else(|error| error)
    }

    /// Set insertion routed by slot type: BITMAP slots run in `bitmap_env`,
    /// flat sets in the hash table. A TTL slot requires one timestamp per
    /// element. A change-flag boundary of its own: a direct call journals the
    /// flags it sets, and inside a batch the batch's boundary already owns
    /// them. Journal refusal is returned before any unlogged byte is written.
    pub fn set_insert(
        &mut self,
        delta_mode: bool,
        state: &mut [u8],
        slot_idx: u32,
        elems: &[u32],
        ts_col: Option<&[f64]>,
    ) -> ErrorCode {
        let meta = match state_init::bind_slot(state, slot_idx) {
            Ok(meta) => meta,
            Err(error) => return error,
        };
        if !matches!(meta.slot_type(), SlotType::HashSet | SlotType::Bitmap) {
            return ErrorCode::InvalidSlot;
        }
        if u32::try_from(elems.len()).is_err()
            || (meta.has_ttl() && ts_col.is_none_or(|timestamps| timestamps.len() < elems.len()))
        {
            return ErrorCode::ColumnUnderrun;
        }
        self.change_flag_boundary(delta_mode, state, |vm, state| {
            vm.route_set_insert(delta_mode, state, &meta, slot_idx, elems, ts_col)
        })
        .unwrap_or_else(|error| error)
    }

    /// [`Vm::set_insert`] inside its boundary.
    fn route_set_insert(
        &mut self,
        delta_mode: bool,
        state: &mut [u8],
        meta: &SlotMetaView,
        slot_idx: u32,
        elems: &[u32],
        ts_col: Option<&[f64]>,
    ) -> ErrorCode {
        if meta.slot_type() == SlotType::Bitmap {
            let hooks = &mut VmCtx {
                undo: &mut self.undo,
            };
            return batch_bitmap_add(
                &mut self.bitmap_env,
                hooks,
                delta_mode,
                state,
                meta,
                elems,
                ts_col,
            );
        }
        hashset_ops::batch_set_insert(
            delta_mode,
            state,
            meta,
            slot_idx,
            elems,
            ts_col,
            &mut self.ctx(),
        )
    }

    /// Set removal routed by slot type; see [`Vm::set_insert`].
    pub fn set_remove(
        &mut self,
        delta_mode: bool,
        state: &mut [u8],
        slot_idx: u32,
        elems: &[u32],
    ) -> ErrorCode {
        let meta = match state_init::bind_slot(state, slot_idx) {
            Ok(meta) => meta,
            Err(error) => return error,
        };
        if !matches!(meta.slot_type(), SlotType::HashSet | SlotType::Bitmap) {
            return ErrorCode::InvalidSlot;
        }
        self.change_flag_boundary(delta_mode, state, |vm, state| {
            vm.route_set_remove(delta_mode, state, &meta, slot_idx, elems)
        })
        .unwrap_or_else(|error| error)
    }

    /// [`Vm::set_remove`] inside its boundary.
    fn route_set_remove(
        &mut self,
        delta_mode: bool,
        state: &mut [u8],
        meta: &SlotMetaView,
        slot_idx: u32,
        elems: &[u32],
    ) -> ErrorCode {
        if meta.slot_type() == SlotType::Bitmap {
            let hooks = &mut VmCtx {
                undo: &mut self.undo,
            };
            return batch_bitmap_remove(
                &mut self.bitmap_env,
                hooks,
                delta_mode,
                state,
                meta,
                elems,
            );
        }
        hashset_ops::batch_set_remove(delta_mode, state, meta, slot_idx, elems, &mut self.ctx())
    }

    /// Apply bitmap algebra with authoritative target/source slot ownership.
    /// TTL merging is admitted before either payload or expiry state changes.
    pub fn bitmap_algebra(
        &mut self,
        delta_mode: bool,
        op: BitmapAlgebraOp,
        state: &mut [u8],
        target_slot: u32,
        source: BitmapSource<'_>,
    ) -> ErrorCode {
        let target_meta = match state_init::bind_slot(state, target_slot) {
            Ok(meta) => meta,
            Err(error) => return error,
        };
        if target_meta.slot_type() != SlotType::Bitmap {
            return ErrorCode::InvalidSlot;
        }
        self.change_flag_boundary(delta_mode, state, |vm, state| {
            batch_bitmap_algebra(
                &mut vm.bitmap_env,
                &mut VmCtx { undo: &mut vm.undo },
                delta_mode,
                op,
                state,
                &target_meta,
                source,
            )
        })
        .unwrap_or_else(|error| error)
    }
}

impl VmHooks for VmCtx<'_> {
    fn undo_enabled(&self) -> bool {
        self.undo.enabled
    }

    fn take_erase_scratch(&mut self) -> EraseScratch {
        core::mem::take(&mut self.undo.erase_scratch)
    }

    fn restore_erase_scratch(&mut self, scratch: EraseScratch) {
        self.undo.erase_scratch = scratch;
    }

    fn begin_capture(
        &mut self,
        state: &[u8],
        ranges: &[(u32, u32)],
    ) -> Result<ByteCapture, ErrorCode> {
        self.undo.begin_capture(state, ranges)
    }

    fn finish_capture(
        &mut self,
        delta_mode: bool,
        state: &[u8],
        ranges: &[(u32, u32)],
        capture: ByteCapture,
    ) {
        self.undo.finish_capture(delta_mode, state, ranges, capture);
    }

    fn plan_ttl_batch(
        &mut self,
        state: &[u8],
        meta: &SlotMetaView,
        mutations: impl IntoIterator<Item = crate::hooks::TtlMutation>,
    ) -> Result<TtlBatchPlan, ErrorCode> {
        prepare_ttl_batch(state, meta, mutations, &mut self.undo.ttl_batch_scratch)
    }

    fn consume_ttl_batch_plan(
        &mut self,
        state: &mut [u8],
        meta: &SlotMetaView,
        plan: TtlBatchPlan,
    ) {
        consume_ttl_batch(state, meta, plan, &mut self.undo.ttl_batch_scratch);
    }
}

// =============================================================================
// Rollback
// =============================================================================

/// Apply one validated physical record without probing, resizing or rewriting
/// any other allocated byte.
pub fn rollback_entry(state: &mut [u8], entry: &FlatUndoEntry) -> Result<(), ErrorCode> {
    let len = u32::from(entry.pad1);
    if !(1..=8).contains(&len) {
        return Err(ErrorCode::InvalidState);
    }
    let end = entry.key.checked_add(len).ok_or(ErrorCode::InvalidState)?;
    let destination = state
        .get_mut(entry.key as usize..end as usize)
        .ok_or(ErrorCode::InvalidState)?;
    destination.copy_from_slice(&entry.aux.to_le_bytes()[..len as usize]);
    Ok(())
}

// =============================================================================
// Struct-map row writes: one physical capture per row mutation
// =============================================================================

/// Run `write` as one physical mutation of row `target` in struct map `map`
/// (slot `slot`, key `key`). The capture is taken before any byte moves: the key
/// and size cells when the key is new, the slot's change-flag byte, and the whole
/// row with its bitset and every field cell, stale bytes included. A new key is
/// applied and its bitset cleared before `write` runs. The capture is finished
/// even when `write` refuses partway, so the accepted prefix is journaled exactly
/// as it now stands.
#[allow(clippy::too_many_arguments)]
fn journal_row_write(
    undo: &mut UndoState,
    delta_mode: bool,
    state: &mut [u8],
    map: &StructMapSlot,
    slot: u32,
    key: u32,
    target: crate::struct_map::Upsert,
    write: impl FnOnce(&mut [u8]) -> ErrorCode,
) -> ErrorCode {
    let ranges = row_capture_ranges(map, slot, target);
    let capture = match undo.begin_capture(state, &ranges) {
        Ok(capture) => capture,
        Err(error) => return error,
    };
    apply_planned_row(map, state, key, target);
    let result = write(&mut *state);
    undo.finish_capture(delta_mode, state, &ranges, capture);
    result
}

/// The bytes one row mutation may touch, shared by every capture of that row: the
/// key and size cells when the key is new, the slot's change-flag byte, and the
/// whole row with its bitset and field cells, stale bytes included.
fn row_capture_ranges(
    map: &StructMapSlot,
    slot: u32,
    target: crate::struct_map::Upsert,
) -> [(u32, u32); 4] {
    let key_cells = if target.is_new { 4 } else { 0 };
    [
        (map.size_off, key_cells),
        (slot_meta_base(slot) + SlotMetaOffset::CHANGE_FLAGS, 1),
        (map.keys_off + target.pos * 4, key_cells),
        (map.row_off(target.pos), map.row_size),
    ]
}

/// Apply a planned new key and clear its row's bitset, so stale bits never count as
/// present. An existing row is left as it stands.
fn apply_planned_row(
    map: &StructMapSlot,
    state: &mut [u8],
    key: u32,
    target: crate::struct_map::Upsert,
) {
    if target.is_new {
        map.apply_upsert(state, key, target);
        map.clear_bitset(state, map.row_off(target.pos));
    }
}

/// Whether row `pos` holds `cell` as the live value of card-one `field`.
fn card_one_held(
    state: &[u8],
    map: &StructMapSlot,
    pos: u32,
    field: u8,
    cell: &[u8; 8],
    cell_size: u32,
) -> bool {
    let row = map.row_off(pos);
    let dst = (row + map.field_offset(state, field)) as usize;
    let size = cell_size as usize;
    StructMapSlot::is_field_set(state, row, field) && state[dst..dst + size] == cell[..size]
}

/// Whether a card-one element changes `up`'s row as it stands: an assert changes
/// a value that is not held, a retract changes one that is. A new row's bytes are
/// stale until cleared, so they never hold the value.
fn card_one_changes(
    state: &[u8],
    map: &StructMapSlot,
    up: crate::struct_map::Upsert,
    field: u8,
    cell: &[u8; 8],
    cell_size: u32,
    is_retract: bool,
) -> bool {
    let held = !up.is_new && card_one_held(state, map, up.pos, field, cell, cell_size);
    held == is_retract
}

/// The card-one write of one row, under the caller's capture. An assert stores
/// `cell` and sets the presence bit, recording INSERTED or UPDATED from the bit as
/// it stood. A retract clears the bit and the cell only while the stored cell
/// still equals `cell`, recording REMOVED.
#[allow(clippy::too_many_arguments)]
fn write_card_one(
    state: &mut [u8],
    map: &StructMapSlot,
    slot: u32,
    pos: u32,
    field: u8,
    cell: &[u8; 8],
    cell_size: u32,
    is_retract: bool,
) {
    let row = map.row_off(pos);
    let dst_off = row + map.field_offset(state, field);
    let flags = (slot_meta_base(slot) + SlotMetaOffset::CHANGE_FLAGS) as usize;
    let held = card_one_held(state, map, pos, field, cell, cell_size);
    match (held, is_retract) {
        (true, true) => {
            StructMapSlot::clear_scalar_field(state, row, field);
            bytes::zero(state, dst_off, cell_size);
            state[flags] |= ChangeFlag::REMOVED;
        }
        (false, false) => {
            let prior_bit = StructMapSlot::is_field_set(state, row, field);
            StructMapSlot::set_field_bit(state, row, field);
            state[dst_off as usize..(dst_off + cell_size) as usize]
                .copy_from_slice(&cell[..cell_size as usize]);
            state[flags] |= if prior_bit {
                ChangeFlag::UPDATED
            } else {
                ChangeFlag::INSERTED
            };
        }
        // A retract of an absent value and an assert of a held one write nothing.
        _ => {}
    }
}

/// One card-one scatter element on struct map `map` (slot `slot`): an assert
/// stores `cell` in `field` of `key`'s row, a retract clears it while the stored
/// cell still equals `cell`. The key is planned once, and an element that changes
/// nothing reserves and journals nothing.
#[allow(clippy::too_many_arguments)]
fn scatter_card_one(
    undo: &mut UndoState,
    delta_mode: bool,
    state: &mut [u8],
    map: &StructMapSlot,
    slot: u32,
    key: u32,
    field: u8,
    cell: &[u8; 8],
    cell_size: u32,
    is_retract: bool,
) -> ErrorCode {
    // A sentinel output key names no row: no growth could place it.
    if key == EMPTY_KEY || key == TOMBSTONE {
        return ErrorCode::InvalidKey;
    }
    let target = if is_retract {
        map.find(state, key)
            .map(|pos| crate::struct_map::Upsert { pos, is_new: false })
    } else {
        let Some(planned) = map.plan_upsert(state, key) else {
            return ErrorCode::CapacityExceeded;
        };
        Some(planned)
    };
    let Some(up) = target else {
        return ErrorCode::Ok;
    };
    if !card_one_changes(state, map, up, field, cell, cell_size, is_retract) {
        return ErrorCode::Ok;
    }
    journal_row_write(
        undo,
        delta_mode,
        state,
        map,
        slot,
        key,
        up,
        |state: &mut [u8]| {
            write_card_one(state, map, slot, up.pos, field, cell, cell_size, is_retract);
            ErrorCode::Ok
        },
    )
}

// =============================================================================
// Guarded-scatter guards
// =============================================================================

/// The guard of a `BATCH_STRUCT_MAP_SCATTER_GUARDED` instruction: which
/// batch columns carry the element's guard components and which fields of
/// the guarded row remember them. Decoded once per element from the body,
/// like the route table beside it, and held in fixed arrays so an element
/// costs no allocation.
///
/// A guard is compared as `[u64; _]`, most significant component first,
/// through [`scalar_order_key`] — one image for the element's cell and the
/// stored field, so the two sides cannot drift into different orders.
#[derive(Clone, Copy)]
struct GuardTuple {
    cols: [u32; MAX_GUARD_COMPONENTS],
    fields: [u8; MAX_GUARD_COMPONENTS],
    len: usize,
}

/// A guard as compared: only the leading `len` lanes are meaningful, and
/// both sides zero the rest, so a slice compare over `len` is the order.
type GuardKey = [u64; MAX_GUARD_COMPONENTS];

impl GuardTuple {
    /// Read `len` `(guard_col:index, guard_field_idx:u8)` pairs.
    /// `None` for a width the compare array cannot hold, and for a width of
    /// zero: an unguarded scatter is `0x3e`, not a degenerate `0x3f`.
    fn decode<'a, R: OperandRead<'a>>(operands: &mut R, len: usize) -> Option<Self> {
        if len == 0 || len > MAX_GUARD_COMPONENTS {
            return None;
        }
        let mut tuple = Self {
            cols: [0; MAX_GUARD_COMPONENTS],
            fields: [0; MAX_GUARD_COMPONENTS],
            len,
        };
        for gi in 0..len {
            tuple.cols[gi] = operands.index()?;
            tuple.fields[gi] = operands.byte()?;
        }
        Some(tuple)
    }

    /// The element's guard, imaged through the guarded row's own field
    /// types. `None` when a component names an array field, which has no
    /// column-cell form to order.
    fn incoming(
        &self,
        state: &[u8],
        smap: &StructMapSlot,
        cols: &[&[u8]],
        element_idx: u32,
    ) -> Option<GuardKey> {
        let mut key = [0u64; MAX_GUARD_COMPONENTS];
        for gi in 0..self.len {
            let ft = smap.field_type(state, self.fields[gi]);
            key[gi] = scalar_cell_order_key(ft, col_at(cols, self.cols[gi] as usize), element_idx)?;
        }
        Some(key)
    }

    /// The guard `row` remembers, or `None` when it remembers none — a row
    /// missing any component has claimed nothing and loses to every
    /// element, which is also what an absent row does.
    fn stored(&self, state: &[u8], smap: &StructMapSlot, row: u32) -> Option<GuardKey> {
        let mut key = [0u64; MAX_GUARD_COMPONENTS];
        for gi in 0..self.len {
            let fi = self.fields[gi];
            if !StructMapSlot::is_field_set(state, row, fi) {
                return None;
            }
            let ft = smap.field_type(state, fi);
            let off = (row + smap.field_offset(state, fi)) as usize;
            let size = struct_field_size(ft) as usize;
            key[gi] = scalar_order_key(ft, &state[off..off + size])?;
        }
        Some(key)
    }

    /// Whether `incoming` loses to what the row remembers. Strictly less
    /// loses; EQUAL WINS, because one transaction is many elements sharing
    /// one guard and all of them have to land.
    fn loses(&self, incoming: &GuardKey, stored: Option<GuardKey>) -> bool {
        stored.is_some_and(|s| incoming[..self.len] < s[..self.len])
    }

    /// Whether the row already remembers exactly this guard, so the stamp
    /// would rewrite identical bytes.
    fn already_stamped(&self, incoming: &GuardKey, stored: Option<GuardKey>) -> bool {
        stored.is_some_and(|s| s[..self.len] == incoming[..self.len])
    }

    /// Write the element's guard into row `pos`: a pure writer under the
    /// caller's row capture (see [`journal_row_write`]). Every component's cell
    /// is encoded before the first byte moves, so a refused guard writes nothing.
    fn stamp(
        &self,
        state: &mut [u8],
        smap: &StructMapSlot,
        pos: u32,
        cols: &[&[u8]],
        element_idx: u32,
    ) -> ErrorCode {
        let mut cells = [([0u8; 8], 0u32); MAX_GUARD_COMPONENTS];
        for gi in 0..self.len {
            let ft = smap.field_type(state, self.fields[gi]);
            let Some(cell) =
                scalar_cell_encoded(ft, col_at(cols, self.cols[gi] as usize), element_idx)
            else {
                return ErrorCode::InvalidProgram;
            };
            cells[gi] = cell;
        }
        let row = smap.row_off(pos);
        for gi in 0..self.len {
            let fi = self.fields[gi];
            let (cell, cell_size) = cells[gi];
            let f_off = row + smap.field_offset(state, fi);
            StructMapSlot::set_field_bit(state, row, fi);
            state[f_off as usize..(f_off + cell_size) as usize]
                .copy_from_slice(&cell[..cell_size as usize]);
        }
        ErrorCode::Ok
    }
}

// =============================================================================
// Single-element struct-map operations

pub(crate) const MAX_STRUCT_SCALAR_OPERANDS: usize = 32;
pub(crate) const MAX_STRUCT_ARRAY_OPERANDS: usize = 16;

/// A struct-map instruction's `(val_col:index, field_idx:u8)` pairs, decoded
/// into fixed arrays. The array width is part of the accepted-program
/// contract: a program declaring more pairs is refused at decode.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ScalarOperands {
    cols: [u32; MAX_STRUCT_SCALAR_OPERANDS],
    fields: [u8; MAX_STRUCT_SCALAR_OPERANDS],
    len: usize,
}

impl ScalarOperands {
    const EMPTY: Self = Self {
        cols: [0; MAX_STRUCT_SCALAR_OPERANDS],
        fields: [0; MAX_STRUCT_SCALAR_OPERANDS],
        len: 0,
    };

    /// Read `len` pairs over this value, writing only those pairs; room for
    /// `reserve` more is kept for the caller.
    fn decode_into<'a, R: OperandRead<'a>>(
        &mut self,
        operands: &mut R,
        len: usize,
        reserve: usize,
    ) -> Option<()> {
        if len + reserve > MAX_STRUCT_SCALAR_OPERANDS {
            return None;
        }
        self.len = 0;
        for i in 0..len {
            self.cols[i] = operands.index()?;
            self.fields[i] = operands.byte()?;
        }
        self.len = len;
        Some(())
    }

    pub(crate) fn cols(&self) -> &[u32] {
        &self.cols[..self.len]
    }

    pub(crate) fn fields(&self) -> &[u8] {
        &self.fields[..self.len]
    }

    /// Append one pair past the decoded ones (room was reserved at decode).
    fn push(&mut self, col: u32, field: u8) {
        self.cols[self.len] = col;
        self.fields[self.len] = field;
        self.len += 1;
    }
}

/// One CSR array field of a struct-map upsert: the offsets and values
/// columns that carry the element arrays, and the field they fill.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ArrayField {
    pub(crate) offsets_col: u32,
    pub(crate) values_col: u32,
    pub(crate) field_idx: u8,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct StructMapUpsertOperands {
    pub(crate) slot: u32,
    pub(crate) key_col: u32,
    pub(crate) vals: ScalarOperands,
    arrays: [ArrayField; MAX_STRUCT_ARRAY_OPERANDS],
    num_arrays: usize,
    pub(crate) comparison_field_idx: Option<u8>,
    pub(crate) end: usize,
}

impl StructMapUpsertOperands {
    const EMPTY: Self = Self {
        slot: 0,
        key_col: 0,
        vals: ScalarOperands::EMPTY,
        arrays: [ArrayField {
            offsets_col: 0,
            values_col: 0,
            field_idx: 0,
        }; MAX_STRUCT_ARRAY_OPERANDS],
        num_arrays: 0,
        comparison_field_idx: None,
        end: 0,
    };

    pub(crate) fn array_fields(&self) -> &[ArrayField] {
        &self.arrays[..self.num_arrays]
    }

    /// Decode over this value, writing only what the instruction carries.
    fn decode_from<'a, R: OperandRead<'a>>(
        &mut self,
        operands: &mut R,
        has_comparison: bool,
    ) -> Option<()> {
        self.num_arrays = 0;
        self.slot = operands.index()?;
        self.key_col = operands.index()?;
        let num_vals = usize::from(operands.byte()?);
        self.vals.decode_into(operands, num_vals, 0)?;
        let num_arrays = usize::from(operands.byte()?);
        if num_arrays > MAX_STRUCT_ARRAY_OPERANDS {
            return None;
        }
        for array in &mut self.arrays[..num_arrays] {
            *array = ArrayField {
                offsets_col: operands.index()?,
                values_col: operands.index()?,
                field_idx: operands.byte()?,
            };
        }
        self.num_arrays = num_arrays;
        self.comparison_field_idx = if has_comparison {
            Some(operands.byte()?)
        } else {
            None
        };
        self.end = operands.pos();
        Some(())
    }
}

/// Decode the shared 0x80/0x81 row operands and 0x82's trailing comparison
/// ordinal without mutating state: `slot:index, key_col:index, num_vals:u8,
/// (val_col:index, field_idx:u8) × num_vals, num_arrays:u8, (offsets_col:index,
/// values_col:index, field_idx:u8) × num_arrays [, comparison_field_idx:u8]`.
/// Fixed operand arrays in both dispatch paths make their encoded maxima part
/// of the accepted-program contract.
pub(crate) fn decode_struct_map_upsert_operands(
    code: &[u8],
    start: usize,
    has_comparison: bool,
) -> Option<StructMapUpsertOperands> {
    let mut operands = StructMapUpsertOperands::EMPTY;
    operands.decode_from(&mut Operands::at(code, start), has_comparison)?;
    Some(operands)
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct StructMap2UpsertOperands {
    pub(crate) slot: u32,
    pub(crate) key1_col: u32,
    pub(crate) key2_col: u32,
    pub(crate) vals: ScalarOperands,
    pub(crate) end: usize,
}

/// `slot:index, key1_col:index, key2_col:index, num_vals:u8,
/// (val_col:index, field_idx:u8) × num_vals`; `reserve` pairs of room stay
/// free for operands an extension appends.
fn decode_struct_map2_row(
    operands: &mut Operands<'_>,
    reserve: usize,
) -> Option<StructMap2UpsertOperands> {
    let mut row = StructMap2UpsertOperands::EMPTY;
    row.decode_from(operands, reserve)?;
    Some(row)
}

impl StructMap2UpsertOperands {
    const EMPTY: Self = Self {
        slot: 0,
        key1_col: 0,
        key2_col: 0,
        vals: ScalarOperands::EMPTY,
        end: 0,
    };

    /// Decode over this value, writing only what the instruction carries.
    fn decode_from<'a, R: OperandRead<'a>>(
        &mut self,
        operands: &mut R,
        reserve: usize,
    ) -> Option<()> {
        self.slot = operands.index()?;
        self.key1_col = operands.index()?;
        self.key2_col = operands.index()?;
        let num_vals = usize::from(operands.byte()?);
        self.vals.decode_into(operands, num_vals, reserve)?;
        self.end = operands.pos();
        Some(())
    }
}

pub(crate) fn decode_struct_map2_upsert_operands(
    code: &[u8],
    start: usize,
) -> Option<StructMap2UpsertOperands> {
    decode_struct_map2_row(&mut Operands::at(code, start), 0)
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct StructMap2MaxI64x2Operands {
    pub(crate) row: StructMap2UpsertOperands,
    pub(crate) cmp1_col: u32,
    pub(crate) cmp1_field: u8,
    pub(crate) cmp2_col: u32,
    pub(crate) cmp2_field: u8,
    pub(crate) end: usize,
}

/// 0x83's row operands followed by `cmp1_col:index, cmp1_field:u8,
/// cmp2_col:index, cmp2_field:u8`. The two comparison lanes are written with
/// the row, so they take two of its pair slots.
pub(crate) fn decode_struct_map2_max_i64x2_operands(
    code: &[u8],
    start: usize,
) -> Option<StructMap2MaxI64x2Operands> {
    let mut operands = StructMap2MaxI64x2Operands::EMPTY;
    operands.decode_from(&mut Operands::at(code, start))?;
    Some(operands)
}

impl StructMap2MaxI64x2Operands {
    const EMPTY: Self = Self {
        row: StructMap2UpsertOperands::EMPTY,
        cmp1_col: 0,
        cmp1_field: 0,
        cmp2_col: 0,
        cmp2_field: 0,
        end: 0,
    };

    /// Decode over this value, writing only what the instruction carries.
    fn decode_from<'a, R: OperandRead<'a>>(&mut self, operands: &mut R) -> Option<()> {
        self.row.decode_from(operands, 2)?;
        self.cmp1_col = operands.index()?;
        self.cmp1_field = operands.byte()?;
        self.cmp2_col = operands.index()?;
        self.cmp2_field = operands.byte()?;
        self.end = operands.pos();
        Some(())
    }
}

fn validate_struct_map2_max_i64x2(
    state: &[u8],
    smap: &StructMap2Slot,
    operands: &StructMap2MaxI64x2Operands,
) -> bool {
    if operands.cmp1_field == operands.cmp2_field
        || operands.cmp1_field >= smap.num_fields
        || operands.cmp2_field >= smap.num_fields
        || smap.field_type(state, operands.cmp1_field) != StructFieldType::Int64
        || smap.field_type(state, operands.cmp2_field) != StructFieldType::Int64
    {
        return false;
    }
    operands.row.vals.fields().iter().all(|&field| {
        field < smap.num_fields && field != operands.cmp1_field && field != operands.cmp2_field
    })
}

fn should_upsert_struct_map2_max_i64x2(
    state: &[u8],
    smap: &StructMap2Slot,
    key1: u32,
    key2: u32,
    operands: &StructMap2MaxI64x2Operands,
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
        bytes::read_i64(col_at(cols, operands.cmp1_col as usize), comparison_offset),
        bytes::read_i64(col_at(cols, operands.cmp2_col as usize), comparison_offset),
    );
    candidate > existing
}

#[derive(Clone, Copy)]
struct StructMapMaxComparison {
    field_idx: u8,
    col: u32,
    field_type: StructFieldType,
    cmp_type: CmpType,
}

/// Resolve and validate 0x82's comparison lane before processing any rows.
/// The comparison ordinal must name a mapped scalar field.
fn resolve_struct_map_max_comparison(
    state: &[u8],
    smap: &StructMapSlot,
    vals: &ScalarOperands,
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
        | StructFieldType::ArrayBool
        | StructFieldType::Vo => return None,
    };
    let col = vals
        .fields()
        .iter()
        .position(|&field| field == comparison_field_idx)
        .map(|at| vals.cols()[at])?;
    Some(StructMapMaxComparison {
        field_idx: comparison_field_idx,
        col,
        field_type,
        cmp_type,
    })
}

/// Operand kinds are a program property, admitted before FIRST/MAX can skip
/// a row. Skipping execution must not make an invalid mapping valid.
fn validate_struct_map_operands(
    state: &[u8],
    map: &StructMapSlot,
    operands: &StructMapUpsertOperands,
) -> Result<(), ErrorCode> {
    for &field in operands.vals.fields() {
        if field >= map.num_fields {
            return Err(ErrorCode::InvalidProgram);
        }
        if is_arena_field_type(map.field_type(state, field)) {
            return Err(ErrorCode::InvalidProgram);
        }
    }
    for field in operands.array_fields() {
        if field.field_idx >= map.num_fields
            || !is_arena_field_type(map.field_type(state, field.field_idx))
        {
            return Err(ErrorCode::InvalidProgram);
        }
    }
    Ok(())
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
) -> Result<bool, ErrorCode> {
    let width = comparison.cmp_type.stride();
    let start = (element_idx as usize)
        .checked_mul(width)
        .ok_or(ErrorCode::ColumnUnderrun)?;
    let end = start.checked_add(width).ok_or(ErrorCode::ColumnUnderrun)?;
    let cell = cols
        .get(comparison.col as usize)
        .and_then(|col| col.get(start..end))
        .ok_or(ErrorCode::ColumnUnderrun)?;
    let incoming = hashmap_ops::read_cmp_value(cell, 0, comparison.cmp_type);
    let Some(position) = smap.find(state, key) else {
        return Ok(true);
    };
    let row = smap.row_off(position);
    if !StructMapSlot::is_field_set(state, row, comparison.field_idx) {
        return Ok(true);
    }

    let field_offset = row + smap.field_offset(state, comparison.field_idx);
    let existing = match comparison.field_type {
        StructFieldType::UInt32 | StructFieldType::String => {
            u64::from(bytes::read_u32(state, field_offset))
        }
        StructFieldType::Bool => u64::from(state[field_offset as usize]),
        StructFieldType::Int64 | StructFieldType::Float64 => bytes::read_u64(state, field_offset),
        StructFieldType::ArrayU32
        | StructFieldType::ArrayI64
        | StructFieldType::ArrayF64
        | StructFieldType::ArrayString
        | StructFieldType::ArrayBool
        | StructFieldType::Vo => unreachable!("comparison kind validated as scalar"),
    };
    Ok(hashmap_ops::cmp_gt(incoming, existing, comparison.cmp_type))
}

/// A checked row mutation. Probing and scalar source admission leave the
/// position and borrowed sources the writer actually consumes.
struct PreparedRow<'a> {
    map: StructMapSlot,
    slot: u32,
    key: u32,
    upsert: crate::struct_map::Upsert,
    arena: Option<ArenaRegion>,
    val_cols: &'a [u32],
    field_idxs: &'a [u8],
    cols: &'a [&'a [u8]],
    element_idx: u32,
}

impl<'a> PreparedRow<'a> {
    #[allow(clippy::too_many_arguments)]
    fn prepare(
        state: &[u8],
        map: StructMapSlot,
        slot: u32,
        key: u32,
        val_cols: &'a [u32],
        field_idxs: &'a [u8],
        cols: &'a [&'a [u8]],
        element_idx: u32,
    ) -> Result<Self, ErrorCode> {
        if val_cols.len() != field_idxs.len() {
            return Err(ErrorCode::InvalidProgram);
        }
        validate_scalar_fields(
            state,
            map.field_types_off,
            map.num_fields,
            val_cols,
            field_idxs,
            cols,
            element_idx,
        )?;
        let upsert = map
            .plan_upsert(state, key)
            .ok_or(ErrorCode::CapacityExceeded)?;
        let arena = if map.arena_header_off == 0 {
            None
        } else {
            let arena = ArenaRegion::bind(state, map.arena_header_off)?;
            if !upsert.is_new {
                let row = map.row_off(upsert.pos);
                let mut offset = map.bitset_bytes;
                for field_idx in 0..map.num_fields {
                    let kind = map.field_type(state, field_idx);
                    if is_arena_field_type(kind)
                        && StructMapSlot::is_field_set(state, row, field_idx)
                    {
                        arena.bytes(
                            state,
                            bytes::read_u32(state, row + offset),
                            bytes::read_u32(state, row + offset + 4),
                            arena_elem_size(kind),
                        )?;
                    }
                    offset += struct_field_size(kind);
                }
            }
            Some(arena)
        };
        Ok(Self {
            map,
            slot,
            key,
            upsert,
            arena,
            val_cols,
            field_idxs,
            cols,
            element_idx,
        })
    }

    fn write(&self, state: &mut [u8]) {
        self.map.apply_upsert(state, self.key, self.upsert);
        self.map
            .clear_bitset(state, self.map.row_off(self.upsert.pos));
        for (&val_col, &field_idx) in self.val_cols.iter().zip(self.field_idxs) {
            self.map.write_scalar_field(
                state,
                self.upsert.pos,
                field_idx,
                self.cols,
                val_col,
                self.element_idx,
            );
        }
        state[(slot_meta_base(self.slot) + SlotMetaOffset::CHANGE_FLAGS) as usize] |=
            if self.upsert.is_new {
                ChangeFlag::INSERTED
            } else {
                ChangeFlag::UPDATED
            };
    }

    /// One journal owner per mutation. An arena slot captures only the
    /// affected key/row/metadata and append destination, never the whole arena.
    fn apply(
        &self,
        undo: &mut UndoState,
        delta_mode: bool,
        state: &mut [u8],
        arrays: Option<&ArenaWrites>,
    ) -> Result<(), ErrorCode> {
        let meta = slot_meta_base(self.slot);
        let (header, append) = self.arena.map_or(((0, 0), (0, 0)), |arena| {
            (
                (arena.header + 4, if arrays.is_some() { 4 } else { 0 }),
                (
                    arena.data + arena.used,
                    arrays.map_or(0, |plan| plan.used - arena.used),
                ),
            )
        });
        let ranges = [
            (
                meta + SlotMetaOffset::SIZE,
                if self.upsert.is_new { 4 } else { 0 },
            ),
            (meta + SlotMetaOffset::CHANGE_FLAGS, 1),
            (
                self.map.keys_off + self.upsert.pos * 4,
                if self.upsert.is_new { 4 } else { 0 },
            ),
            (self.map.row_off(self.upsert.pos), self.map.row_size),
            header,
            append,
        ];
        let capture = undo.begin_capture(state, &ranges)?;
        self.write(state);
        if let Some(arrays) = arrays {
            arrays.write(state, &self.map, self.upsert.pos, self.cols);
        }
        undo.finish_capture(delta_mode, state, &ranges, capture);
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn single_struct_map2_upsert_last(
    undo: &mut UndoState,
    delta_mode: bool,
    state: &mut [u8],
    slot: u32,
    key1: u32,
    key2: u32,
    val_cols: &[u32],
    field_idxs: &[u8],
    cols: &[&[u8]],
    element_idx: u32,
) -> ErrorCode {
    if key1 == EMPTY_KEY || key1 == TOMBSTONE {
        return ErrorCode::InvalidKey;
    }
    let smap = StructMap2Slot::bind(state, slot);
    if let Err(err) = validate_scalar_fields(
        state,
        smap.field_types_off,
        smap.num_fields,
        val_cols,
        field_idxs,
        cols,
        element_idx,
    ) {
        return err;
    }
    let Some(result) = smap.plan_upsert(state, key1, key2) else {
        NEEDS_GROWTH_SLOT.store(slot, Ordering::Relaxed);
        return ErrorCode::CapacityExceeded;
    };
    let row = smap.row_off(result.pos);
    let ranges = [
        (smap.size_off, if result.is_new { 4 } else { 0 }),
        (slot_meta_base(slot) + SlotMetaOffset::CHANGE_FLAGS, 1),
        (
            smap.keys1_off + result.pos * 4,
            if result.is_new { 4 } else { 0 },
        ),
        (
            smap.keys2_off + result.pos * 4,
            if result.is_new { 4 } else { 0 },
        ),
        (row, smap.row_size),
    ];
    let capture = match undo.begin_capture(state, &ranges) {
        Ok(capture) => capture,
        Err(error) => return error,
    };
    smap.apply_upsert(state, key1, key2, result);
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
    let meta_base = slot_meta_base(slot);
    state[(meta_base + SlotMetaOffset::CHANGE_FLAGS) as usize] |= if result.is_new {
        ChangeFlag::INSERTED
    } else {
        ChangeFlag::UPDATED
    };
    undo.finish_capture(delta_mode, state, &ranges, capture);
    ErrorCode::Ok
}

#[allow(clippy::too_many_arguments)]
fn single_struct_map2_upsert_max_i64x2(
    undo: &mut UndoState,
    delta_mode: bool,
    state: &mut [u8],
    operands: &StructMap2MaxI64x2Operands,
    key1: u32,
    key2: u32,
    cols: &[&[u8]],
    element_idx: u32,
) -> ErrorCode {
    if key1 == EMPTY_KEY || key1 == TOMBSTONE {
        return ErrorCode::InvalidKey;
    }
    let smap = StructMap2Slot::bind(state, operands.row.slot);
    if !should_upsert_struct_map2_max_i64x2(state, &smap, key1, key2, operands, cols, element_idx) {
        return ErrorCode::Ok;
    }

    // Room for the two comparison lanes was reserved at decode.
    let mut vals = operands.row.vals;
    vals.push(operands.cmp1_col, operands.cmp1_field);
    vals.push(operands.cmp2_col, operands.cmp2_field);
    single_struct_map2_upsert_last(
        undo,
        delta_mode,
        state,
        operands.row.slot,
        key1,
        key2,
        vals.cols(),
        vals.fields(),
        cols,
        element_idx,
    )
}

fn single_struct_map2_remove(
    undo: &mut UndoState,
    delta_mode: bool,
    state: &mut [u8],
    slot: u32,
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
    let ranges = [
        (smap.size_off, 4),
        (slot_meta_base(slot) + SlotMetaOffset::CHANGE_FLAGS, 1),
        (smap.keys1_off + pos * 4, 4),
        (row, smap.bitset_bytes),
    ];
    let capture = match undo.begin_capture(state, &ranges) {
        Ok(capture) => capture,
        Err(error) => return error,
    };
    smap.tombstone_at(state, pos);
    smap.set_size(state, smap.size(state) - 1);
    smap.clear_bitset(state, row);
    state[(slot_meta_base(slot) + SlotMetaOffset::CHANGE_FLAGS) as usize] |= ChangeFlag::REMOVED;
    undo.finish_capture(delta_mode, state, &ranges, capture);
    ErrorCode::Ok
}

//#region reduce-typed-state.probe-upsert
/// Copy remapped fields from a probed source row into the output slot.
#[allow(clippy::too_many_arguments)]
fn single_struct_map_upsert_from_probe(
    undo: &mut UndoState,
    delta_mode: bool,
    state: &mut [u8],
    out_slot: u32,
    out_key: u32,
    probe: &StructMapSlot,
    probe_row: u32,
    probe_field_idxs: &[u8],
    out_field_idxs: &[u8],
) -> ErrorCode {
    if out_key == EMPTY_KEY || out_key == TOMBSTONE {
        return ErrorCode::Ok;
    }

    let out = match state_init::bind_struct_map(state, out_slot) {
        Ok(out) => out,
        Err(err) => return err,
    };
    for (&source, &destination) in probe_field_idxs.iter().zip(out_field_idxs) {
        if source >= probe.num_fields || destination >= out.num_fields {
            return ErrorCode::InvalidProgram;
        }
        let source_type = probe.field_type(state, source);
        if source_type != out.field_type(state, destination) || is_arena_field_type(source_type) {
            return ErrorCode::InvalidProgram;
        }
    }
    let Some(result) = out.plan_upsert(state, out_key) else {
        NEEDS_GROWTH_SLOT.store(out_slot, Ordering::Relaxed);
        return ErrorCode::CapacityExceeded;
    };

    let out_row = out.row_off(result.pos);

    let ranges = [
        (out.size_off, if result.is_new { 4 } else { 0 }),
        (slot_meta_base(out_slot) + SlotMetaOffset::CHANGE_FLAGS, 1),
        (
            out.keys_off + result.pos * 4,
            if result.is_new { 4 } else { 0 },
        ),
        (out_row, out.row_size),
    ];
    let capture = match undo.begin_capture(state, &ranges) {
        Ok(capture) => capture,
        Err(error) => return error,
    };
    out.apply_upsert(state, out_key, result);

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

    let meta_base = slot_meta_base(out_slot);
    state[(meta_base + SlotMetaOffset::CHANGE_FLAGS) as usize] |= if result.is_new {
        ChangeFlag::INSERTED
    } else {
        ChangeFlag::UPDATED
    };
    undo.finish_capture(delta_mode, state, &ranges, capture);
    ErrorCode::Ok
}
//#endregion reduce-typed-state.probe-upsert

/// Validate scalar source geometry without writing or allocating. Arena-backed
/// logical scalars must enter through CSR operands, never through scalar cells.
fn validate_scalar_fields(
    state: &[u8],
    descriptor_off: u32,
    num_fields: u8,
    val_cols: &[u32],
    field_idxs: &[u8],
    cols: &[&[u8]],
    element_idx: u32,
) -> Result<(), ErrorCode> {
    for (&col_idx, &field_idx) in val_cols.iter().zip(field_idxs) {
        if field_idx >= num_fields {
            return Err(ErrorCode::InvalidProgram);
        }
        let field_type =
            StructFieldType::from_u8(state[(descriptor_off + u32::from(field_idx)) as usize])
                .ok_or(ErrorCode::InvalidState)?;
        if is_arena_field_type(field_type) {
            return Err(ErrorCode::InvalidProgram);
        }
        let width = if field_type == StructFieldType::Bool {
            4
        } else {
            struct_field_size(field_type)
        };
        let end = element_idx
            .checked_add(1)
            .and_then(|n| n.checked_mul(width))
            .ok_or(ErrorCode::ColumnUnderrun)?;
        if cols
            .get(col_idx as usize)
            .is_none_or(|col| end as usize > col.len())
        {
            return Err(ErrorCode::ColumnUnderrun);
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Default)]
struct ArenaWrite {
    field_idx: u8,
    field_offset: u32,
    values_col: u32,
    source_offset: u32,
    byte_len: u32,
    len: u32,
    arena_offset: u32,
}

struct ArenaWrites {
    arena: Option<ArenaRegion>,
    fields: [ArenaWrite; MAX_STRUCT_ARRAY_OPERANDS],
    len: usize,
    used: u32,
}

impl ArenaWrites {
    fn prepare(
        state: &[u8],
        row: &PreparedRow<'_>,
        fields: &[ArrayField],
    ) -> Result<Self, ErrorCode> {
        let arena = row.arena.ok_or(ErrorCode::InvalidProgram)?;
        let mut plan = Self {
            arena: Some(arena),
            fields: [ArenaWrite::default(); MAX_STRUCT_ARRAY_OPERANDS],
            len: fields.len(),
            used: arena.used,
        };
        for (write, field) in plan.fields.iter_mut().zip(fields) {
            if field.field_idx >= row.map.num_fields {
                return Err(ErrorCode::InvalidProgram);
            }
            let field_type =
                StructFieldType::from_u8(row.map.field_type_byte(state, field.field_idx))
                    .ok_or(ErrorCode::InvalidState)?;
            if !is_arena_field_type(field_type) {
                return Err(ErrorCode::InvalidProgram);
            }
            let elem_size = arena_elem_size(field_type);
            let offsets = row
                .cols
                .get(field.offsets_col as usize)
                .ok_or(ErrorCode::ColumnUnderrun)?;
            let offset_pos = row
                .element_idx
                .checked_mul(4)
                .ok_or(ErrorCode::ColumnUnderrun)?;
            let offset_end = offset_pos.checked_add(8).ok_or(ErrorCode::ColumnUnderrun)?;
            let pair = offsets
                .get(offset_pos as usize..offset_end as usize)
                .ok_or(ErrorCode::ColumnUnderrun)?;
            let start = bytes::read_u32(pair, 0);
            let end = bytes::read_u32(pair, 4);
            let len = end.checked_sub(start).ok_or(ErrorCode::InvalidCellValue)?;
            let source_offset = start
                .checked_mul(elem_size)
                .ok_or(ErrorCode::InvalidCellValue)?;
            let source_end = end
                .checked_mul(elem_size)
                .ok_or(ErrorCode::InvalidCellValue)?;
            let byte_len = source_end - source_offset;
            let values = row
                .cols
                .get(field.values_col as usize)
                .ok_or(ErrorCode::ColumnUnderrun)?;
            if source_end as usize > values.len() {
                return Err(ErrorCode::ColumnUnderrun);
            }
            let next_used = plan
                .used
                .checked_add(byte_len)
                .ok_or(ErrorCode::ArenaOverflow)?;
            if next_used > arena.capacity {
                return Err(ErrorCode::ArenaOverflow);
            }
            *write = ArenaWrite {
                field_idx: field.field_idx,
                field_offset: row.map.field_offset(state, field.field_idx),
                values_col: field.values_col,
                source_offset,
                byte_len,
                len,
                arena_offset: plan.used,
            };
            plan.used = next_used;
        }
        Ok(plan)
    }

    fn write(&self, state: &mut [u8], smap: &StructMapSlot, pos: u32, cols: &[&[u8]]) {
        let Some(arena) = self.arena else {
            return;
        };
        let row = smap.row_off(pos);
        for field in &self.fields[..self.len] {
            StructMapSlot::set_field_bit(state, row, field.field_idx);
            bytes::write_u32(state, row + field.field_offset, field.arena_offset);
            bytes::write_u32(state, row + field.field_offset + 4, field.len);
            bytes::copy(
                state,
                arena.data + field.arena_offset,
                cols[field.values_col as usize],
                field.source_offset,
                field.byte_len,
            );
        }
        bytes::write_u32(state, arena.header + 4, self.used);
    }
}

/// Both batch and nested execution apply the same row replacement and arena journal.
#[allow(clippy::too_many_arguments)]
fn upsert_struct_map_row(
    undo: &mut UndoState,
    delta_mode: bool,
    state: &mut [u8],
    smap: &StructMapSlot,
    slot_idx: u32,
    key: u32,
    val_cols: &[u32],
    field_idxs: &[u8],
    array_fields: &[ArrayField],
    cols: &[&[u8]],
    element_idx: u32,
) -> u32 {
    if key == EMPTY_KEY || key == TOMBSTONE {
        return OK;
    }
    let row = match PreparedRow::prepare(
        state,
        *smap,
        slot_idx,
        key,
        val_cols,
        field_idxs,
        cols,
        element_idx,
    ) {
        Ok(row) => row,
        Err(ErrorCode::CapacityExceeded) => {
            NEEDS_GROWTH_SLOT.store(slot_idx, Ordering::Relaxed);
            return NEEDS_GROWTH;
        }
        Err(error) => return error as u32,
    };
    let arrays = if array_fields.is_empty() {
        None
    } else {
        match ArenaWrites::prepare(state, &row, array_fields) {
            Ok(plan) => Some(plan),
            Err(ErrorCode::ArenaOverflow) => {
                NEEDS_GROWTH_SLOT.store(slot_idx, Ordering::Relaxed);
                return NEEDS_GROWTH;
            }
            Err(error) => return error as u32,
        }
    };
    match row.apply(undo, delta_mode, state, arrays.as_ref()) {
        Ok(()) => OK,
        Err(error) => error as u32,
    }
}

fn counts_error(error: crate::counts_fold::CountsFoldError) -> ErrorCode {
    use crate::counts_fold::CountsFoldError;
    match error {
        CountsFoldError::InvalidProgram => ErrorCode::InvalidProgram,
        CountsFoldError::ColumnUnderrun => ErrorCode::ColumnUnderrun,
        CountsFoldError::InvalidCellValue => ErrorCode::InvalidCellValue,
        CountsFoldError::FrameMismatch => ErrorCode::InvalidState,
        CountsFoldError::MetadataConflict => ErrorCode::MetadataConflict,
        CountsFoldError::CountOverflow => ErrorCode::CountOverflow,
    }
}

/// Apply one physically planned fold. Schema-role certification belongs to
/// the admitted program; this mutation owns its exact physical journal.
fn fold_counts_row(
    undo: &mut UndoState,
    delta_mode: bool,
    state: &mut [u8],
    op: &crate::counts_fold::CountsFoldOp<'_>,
    cols: &[&[u8]],
    element: u32,
) -> u32 {
    use crate::counts_fold::{CountsFoldTarget, MAX_COUNTS_HISTOGRAMS, MAX_COUNTS_MASKS};
    let map = match state_init::bind_struct_map(state, op.slot) {
        Ok(map) => map,
        Err(error) => return error as u32,
    };
    if op.field_idx >= map.num_fields || map.field_type(state, op.field_idx) != StructFieldType::Vo
    {
        return INVALID_PROGRAM;
    }
    let key = match op.read_key(cols, element) {
        Ok(key) => key,
        Err(error) => return counts_error(error) as u32,
    };
    if key == EMPTY_KEY || key == TOMBSTONE {
        return OK;
    }
    let sample = match op.read_sample(cols, element) {
        Ok(sample) => sample,
        Err(error) => return counts_error(error) as u32,
    };
    let Some(upsert) = map.plan_upsert(state, key) else {
        NEEDS_GROWTH_SLOT.store(op.slot, Ordering::Relaxed);
        return NEEDS_GROWTH;
    };
    let stored = if upsert.is_new {
        None
    } else {
        match map.arena_field(state, upsert.pos, op.field_idx) {
            Ok(frame) => frame,
            Err(error) => return error as u32,
        }
    };
    let plan = match op.prepare(stored, &sample) {
        Ok(plan) => plan,
        Err(error) => return counts_error(error) as u32,
    };
    let arena = match ArenaRegion::bind(state, map.arena_header_off) {
        Ok(arena) => arena,
        Err(error) => return error as u32,
    };
    let row = map.row_off(upsert.pos);
    let cell = row + map.field_offset(state, op.field_idx);
    let (frame_offset, frame_len, next_used) = match plan.target() {
        CountsFoldTarget::Insert => {
            let Ok(len) = u32::try_from(sample.init().len()) else {
                return ErrorCode::InvalidCellValue as u32;
            };
            let Some(next_used) = arena.used.checked_add(len) else {
                NEEDS_GROWTH_SLOT.store(op.slot, Ordering::Relaxed);
                return NEEDS_GROWTH;
            };
            if next_used > arena.capacity {
                NEEDS_GROWTH_SLOT.store(op.slot, Ordering::Relaxed);
                return NEEDS_GROWTH;
            }
            (arena.data + arena.used, len, next_used)
        }
        CountsFoldTarget::Stored => (
            arena.data + bytes::read_u32(state, cell),
            bytes::read_u32(state, cell + 4),
            arena.used,
        ),
    };
    const MAX_RANGES: usize = 2 + MAX_COUNTS_HISTOGRAMS + MAX_COUNTS_MASKS;
    let mut ranges = [(0, 0); MAX_RANGES];
    let meta = slot_meta_base(op.slot);
    let range_count = match plan.target() {
        CountsFoldTarget::Insert => {
            ranges[0] = (
                meta + SlotMetaOffset::SIZE,
                if upsert.is_new { 4 } else { 0 },
            );
            ranges[1] = (
                map.keys_off + upsert.pos * 4,
                if upsert.is_new { 4 } else { 0 },
            );
            ranges[2] = (row, map.row_size);
            ranges[3] = (arena.header + 4, 4);
            ranges[4] = (frame_offset, frame_len);
            5
        }
        CountsFoldTarget::Stored => op.journal_ranges(&plan, frame_offset, &mut ranges),
    };
    ranges[range_count] = (meta + SlotMetaOffset::CHANGE_FLAGS, 1);
    let ranges = &ranges[..range_count + 1];
    let capture = match undo.begin_capture(state, ranges) {
        Ok(capture) => capture,
        Err(error) => return error as u32,
    };
    if plan.target() == CountsFoldTarget::Insert {
        map.apply_upsert(state, key, upsert);
        if upsert.is_new {
            map.clear_bitset(state, row);
        }
        StructMapSlot::set_field_bit(state, row, op.field_idx);
        bytes::write_u32(state, cell, arena.used);
        bytes::write_u32(state, cell + 4, frame_len);
        bytes::copy(state, frame_offset, sample.init(), 0, frame_len);
        bytes::write_u32(state, arena.header + 4, next_used);
    }
    op.apply(
        &plan,
        &mut state[frame_offset as usize..(frame_offset + frame_len) as usize],
    );
    state[(meta + SlotMetaOffset::CHANGE_FLAGS) as usize] |= if upsert.is_new {
        ChangeFlag::INSERTED
    } else {
        ChangeFlag::UPDATED
    };
    undo.finish_capture(delta_mode, state, ranges, capture);
    OK
}

// =============================================================================
// Aggregate execution
// =============================================================================

/// Reduce the aggregate value, capturing only its actual value lane and flag.
/// The adjacent allocated count lane remains byte-identical.
#[allow(clippy::too_many_arguments)]
fn exec_agg_f64(
    undo: &mut UndoState,
    delta_mode: bool,
    state: &mut [u8],
    slot: u32,
    kind: AggKind,
    vals: &[f64],
    type_mask: Option<TypeMask<'_>>,
    pred_col: Option<&[u32]>,
) -> ErrorCode {
    let meta = SlotMetaView::read(state, slot);
    let old_val = bytes::read_f64(state, meta.offset);
    let new_val = aggregates::reduce_col_f64(kind, vals, old_val, type_mask, pred_col);
    #[allow(clippy::float_cmp)]
    if new_val != old_val {
        let ranges = [
            (meta.offset, 8),
            (meta.meta_base + SlotMetaOffset::CHANGE_FLAGS, 1),
        ];
        let capture = match undo.begin_capture(state, &ranges) {
            Ok(capture) => capture,
            Err(error) => return error,
        };
        bytes::write_f64(state, meta.offset, new_val);
        meta.set_change_flag(state, ChangeFlag::SIZE_CHANGED);
        undo.finish_capture(delta_mode, state, &ranges, capture);
    }
    ErrorCode::Ok
}

/// Execute an aggregate on i64 slots.
#[allow(clippy::too_many_arguments)]
fn exec_agg_i64(
    undo: &mut UndoState,
    delta_mode: bool,
    state: &mut [u8],
    slot: u32,
    kind: AggKind,
    vals: &[i64],
    type_mask: Option<TypeMask<'_>>,
    pred_col: Option<&[u32]>,
) -> ErrorCode {
    let meta = SlotMetaView::read(state, slot);
    let old_val = bytes::read_i64(state, meta.offset);
    let new_val = aggregates::reduce_col_i64(kind, vals, old_val, type_mask, pred_col);
    if new_val != old_val {
        let ranges = [
            (meta.offset, 8),
            (meta.meta_base + SlotMetaOffset::CHANGE_FLAGS, 1),
        ];
        let capture = match undo.begin_capture(state, &ranges) {
            Ok(capture) => capture,
            Err(error) => return error,
        };
        bytes::write_i64(state, meta.offset, new_val);
        meta.set_change_flag(state, ChangeFlag::SIZE_CHANGED);
        undo.finish_capture(delta_mode, state, &ranges, capture);
    }
    ErrorCode::Ok
}

/// Bump a COUNT slot.
fn exec_agg_count(
    undo: &mut UndoState,
    delta_mode: bool,
    state: &mut [u8],
    slot: u32,
    matched: u64,
) -> ErrorCode {
    let meta = SlotMetaView::read(state, slot);
    if matched == 0 {
        return ErrorCode::Ok;
    }
    let previous = bytes::read_u64(state, meta.offset);
    let Some(next) = previous.checked_add(matched) else {
        return ErrorCode::CountOverflow;
    };
    let ranges = [
        (meta.offset, 8),
        (meta.meta_base + SlotMetaOffset::CHANGE_FLAGS, 1),
    ];
    let capture = match undo.begin_capture(state, &ranges) {
        Ok(capture) => capture,
        Err(error) => return error,
    };
    bytes::write_u64(state, meta.offset, next);
    meta.set_change_flag(state, ChangeFlag::SIZE_CHANGED);
    undo.finish_capture(delta_mode, state, &ranges, capture);
    ErrorCode::Ok
}

/// Select a scalar latest value without writes, then capture and publish its
/// actual value width, timestamp and change flag once.
#[allow(clippy::too_many_arguments)]
fn exec_scalar_latest(
    undo: &mut UndoState,
    delta_mode: bool,
    state: &mut [u8],
    slot: u32,
    val_col: &[u8],
    cmp_vals: &[f64],
    batch_len: u32,
    type_mask: Option<(&[u32], u32)>,
) -> ErrorCode {
    let meta = SlotMetaView::read(state, slot);
    let data = meta.offset;
    let scalar_type = meta.agg_type_byte(state);
    let Some(kind) = AggType::from_u8(scalar_type) else {
        return ErrorCode::InvalidProgram;
    };
    if meta.slot_type() != SlotType::Scalar
        || !matches!(
            kind,
            AggType::ScalarU32 | AggType::ScalarF64 | AggType::ScalarI64
        )
    {
        return ErrorCode::InvalidProgram;
    }
    // Every arm indexes val/cmp/type data with row indices up to batch_len;
    // a column that cannot cover the batch is a malformed batch, refused
    // here rather than a panic mid-loop.
    if cmp_vals.len() < batch_len as usize {
        return ErrorCode::ColumnUnderrun;
    }
    if let Some((td, _)) = type_mask
        && td.len() < batch_len as usize
    {
        return ErrorCode::ColumnUnderrun;
    }
    let mut latest = bytes::read_f64(state, data + 8);
    let mut selected = None;
    let matches = |i: usize| type_mask.is_none_or(|(td, id)| td[i] == id);
    match kind {
        AggType::ScalarU32 => {
            let Some(vals) = col_u32_exact(val_col, batch_len) else {
                return ErrorCode::ColumnUnderrun;
            };
            // A scalar cell stores a value: every u32, 0xffffffff included, is
            // stored on a matching row. A reader decodes wrapped absence from
            // the field's declared wrapper, not the kernel.
            for i in 0..batch_len as usize {
                if matches(i) && cmp_vals[i] > latest {
                    latest = cmp_vals[i];
                    selected = Some(u64::from(vals[i]));
                }
            }
        }
        AggType::ScalarF64 => {
            let Some(vals) = col_f64_exact(val_col, batch_len) else {
                return ErrorCode::ColumnUnderrun;
            };
            for i in 0..batch_len as usize {
                if matches(i) && cmp_vals[i] > latest {
                    latest = cmp_vals[i];
                    selected = Some(vals[i].to_bits());
                }
            }
        }
        AggType::ScalarI64 => {
            let Some(vals) = col_i64_exact(val_col, batch_len) else {
                return ErrorCode::ColumnUnderrun;
            };
            for i in 0..batch_len as usize {
                if matches(i) && cmp_vals[i] > latest {
                    latest = cmp_vals[i];
                    selected = Some(u64::from_le_bytes(vals[i].to_le_bytes()));
                }
            }
        }
        _ => return ErrorCode::InvalidProgram,
    }
    let Some(value) = selected else {
        return ErrorCode::Ok;
    };
    let width = if kind == AggType::ScalarU32 { 4 } else { 8 };
    let ranges = [
        (data, width),
        (data + 8, 8),
        (meta.meta_base + SlotMetaOffset::CHANGE_FLAGS, 1),
    ];
    let capture = match undo.begin_capture(state, &ranges) {
        Ok(capture) => capture,
        Err(error) => return error,
    };
    let encoded = value.to_le_bytes();
    state[data as usize..(data + width) as usize].copy_from_slice(&encoded[..width as usize]);
    bytes::write_f64(state, data + 8, latest);
    meta.set_change_flag(state, ChangeFlag::UPDATED);
    undo.finish_capture(delta_mode, state, &ranges, capture);
    ErrorCode::Ok
}

// =============================================================================
// Block-based execution tables
// =============================================================================

pub(crate) const fn is_aggregate_op(op: Opcode) -> bool {
    matches!(
        op,
        Opcode::BatchAggSum
            | Opcode::BatchAggCount
            | Opcode::BatchAggMin
            | Opcode::BatchAggMax
            | Opcode::BatchAggSumIf
            | Opcode::BatchAggCountIf
            | Opcode::BatchAggMinIf
            | Opcode::BatchAggMaxIf
            | Opcode::BatchScalarLatest
            | Opcode::BatchAggSumI64
            | Opcode::BatchAggMinI64
            | Opcode::BatchAggMaxI64
    )
}

/// Length of the body instruction at `pc` (opcode included). `None` for an
/// opcode with no body layout, or operands short of their layout.
pub(crate) fn body_op_len(code: &[u8], pc: usize) -> Option<usize> {
    let op = Opcode::from_u8(*code.get(pc)?)?;
    let mut operands = Operands::at(code, pc.checked_add(1)?);
    walk_operands(op, &mut operands, &mut Skip)?;
    operands.pos().checked_sub(pc)
}

/// Whether `slot` is a scalar a `BATCH_SCALAR_LATEST` can fold into: the
/// state check a FOR_EACH body plan leaves to its caller.
fn is_scalar_latest_slot(state: &[u8], slot: u32) -> bool {
    if slot >= state_num_slots(state) {
        return false;
    }
    let meta = SlotMetaView::read(state, slot);
    meta.slot_type() == SlotType::Scalar
        && matches!(
            AggType::from_u8(meta.agg_type_byte(state)),
            Some(AggType::ScalarU32 | AggType::ScalarF64 | AggType::ScalarI64)
        )
}

// =============================================================================
// VM dispatch
// =============================================================================

const OK: u32 = ErrorCode::Ok as u32;
const INVALID_STATE: u32 = ErrorCode::InvalidState as u32;
const INVALID_PROGRAM: u32 = ErrorCode::InvalidProgram as u32;
const NEEDS_GROWTH: u32 = ErrorCode::NeedsGrowth as u32;
const OUT_OF_MEMORY: u32 = ErrorCode::OutOfMemory as u32;

/// Map a capacity failure to NEEDS_GROWTH and record its slot.
fn signal_growth(slot_idx: u32, result: ErrorCode) -> u32 {
    if result == ErrorCode::CapacityExceeded {
        NEEDS_GROWTH_SLOT.store(slot_idx, Ordering::Relaxed);
        return NEEDS_GROWTH;
    }
    result as u32
}

impl Vm {
    /// Execute one batch using the VM's normal mutation lane, over a batch
    /// with no live derived columns (`row_exprs`).
    pub fn execute_batch(
        &mut self,
        state: &mut [u8],
        program: &[u8],
        cols: &[&[u8]],
        batch_len: u32,
    ) -> u32 {
        self.execute_impl(false, state, program, cols, batch_len, None)
    }

    /// Execute one batch using the VM's delta mutation lane, over a batch
    /// with no live derived columns (`row_exprs`).
    pub fn execute_batch_delta(
        &mut self,
        state: &mut [u8],
        program: &[u8],
        cols: &[&[u8]],
        batch_len: u32,
    ) -> u32 {
        self.execute_impl(true, state, program, cols, batch_len, None)
    }

    /// Execute one batch using the VM's normal mutation lane; the conditional
    /// per-element opcodes read `live` columns at their row, against the
    /// state the rows before it left.
    pub fn execute_batch_live(
        &mut self,
        state: &mut [u8],
        program: &[u8],
        cols: &[&[u8]],
        batch_len: u32,
        live: &mut LiveColumns<'_, '_>,
    ) -> u32 {
        self.execute_impl(false, state, program, cols, batch_len, Some(live))
    }

    /// [`Self::execute_batch_live`] on the delta mutation lane.
    pub fn execute_batch_delta_live(
        &mut self,
        state: &mut [u8],
        program: &[u8],
        cols: &[&[u8]],
        batch_len: u32,
        live: &mut LiveColumns<'_, '_>,
    ) -> u32 {
        self.execute_impl(true, state, program, cols, batch_len, Some(live))
    }

    /// Evict expired entries on the selected lane. A delta operation retains
    /// complete paired physical redo; an undo-only operation does not.
    pub fn evict_all_expired(
        &mut self,
        delta_mode: bool,
        state: &mut [u8],
        now: f64,
    ) -> Result<u32, ErrorCode> {
        if state.len() < columine_types::types::STATE_HEADER_SIZE as usize
            || bytes::read_u32(state, 0) != STATE_MAGIC
            || state[StateHeaderOffset::FORMAT_VERSION as usize] != STATE_FORMAT_VERSION
        {
            return Err(ErrorCode::InvalidState);
        }
        let slots = state_num_slots(state);
        self.change_flag_boundary(delta_mode, state, |vm, state| {
            let mut total = 0u32;
            for slot in 0..slots {
                let meta = SlotMetaView::read(state, slot);
                if meta.has_ttl() {
                    total = total
                        .checked_add(evict_expired(
                            delta_mode,
                            &mut vm.undo,
                            &mut vm.bitmap_env,
                            state,
                            &meta,
                            now,
                        )?)
                        .ok_or(ErrorCode::InvalidState)?;
                }
            }
            Ok(total)
        })?
    }

    /// Execute the core batch implementation for normal or delta mode.
    fn execute_impl(
        &mut self,
        delta_mode: bool,
        state: &mut [u8],
        program: &[u8],
        cols: &[&[u8]],
        batch_len: u32,
        live: Option<&mut LiveColumns<'_, '_>>,
    ) -> u32 {
        if state.len() < columine_types::types::STATE_HEADER_SIZE as usize
            || bytes::read_u32(state, 0) != STATE_MAGIC
            || state[StateHeaderOffset::FORMAT_VERSION as usize] != STATE_FORMAT_VERSION
        {
            return INVALID_STATE;
        }
        let Some(sections) = ProgramHeader::sections(program) else {
            return INVALID_PROGRAM;
        };
        if !self
            .accepted_program_magics
            .contains(&sections.header.magic)
        {
            return INVALID_PROGRAM;
        }
        if !state_init::program_matches_state(
            sections.init_code,
            sections.header.num_slots,
            state,
            &mut self.declared_slots,
        ) {
            return INVALID_PROGRAM;
        }
        self.change_flag_boundary(delta_mode, state, |vm, state| {
            let mut live = live;
            let admission =
                vm.capacity
                    .admit(state, sections.reduce_code, cols, batch_len, &vm.bitmap_env);
            if admission == Admission::DryRun {
                let result = vm.dry_run(
                    state,
                    sections.reduce_code,
                    cols,
                    batch_len,
                    live.as_deref_mut(),
                );
                if result != OK {
                    if result == NEEDS_GROWTH {
                        vm.capacity.counts.refused += 1;
                    }
                    return result;
                }
                vm.capacity.counts.dry_run_fit += 1;
            }
            let result = vm.execute_ops(
                delta_mode,
                state,
                sections.reduce_code,
                cols,
                batch_len,
                live,
            );
            debug_assert!(
                result != NEEDS_GROWTH && result != ErrorCode::CapacityExceeded as u32,
                "capacity admission cleared a batch that ran out of capacity: {result}"
            );
            result
        })
        .unwrap_or_else(|error| error as u32)
    }

    /// Tier 3 of capacity admission: run the batch on a retained copy of the
    /// state with the journal off and answer what it answered, or
    /// `OutOfMemory` when the copy cannot be backed. Execution is
    /// deterministic, so a batch this run admits runs the same way for real,
    /// and one it refuses has written nothing to `state`.
    fn dry_run(
        &mut self,
        state: &[u8],
        code: &[u8],
        cols: &[&[u8]],
        batch_len: u32,
        live: Option<&mut LiveColumns<'_, '_>>,
    ) -> u32 {
        let mut image = core::mem::take(&mut self.capacity.image);
        image.clear();
        // The image grows to the largest state this VM has dry-run; memory
        // that cannot back it refuses before any write, never an abort.
        if image.try_reserve(state.len()).is_err() {
            self.capacity.image = image;
            return OUT_OF_MEMORY;
        }
        image.extend_from_slice(state);
        let enabled = core::mem::replace(&mut self.undo.enabled, false);
        let result = self.execute_ops(false, &mut image, code, cols, batch_len, live);
        self.undo.enabled = enabled;
        self.capacity.image = image;
        result
    }

    /// How this VM has admitted batches so far.
    pub fn capacity_counts(&self) -> CapacityCounts {
        self.capacity.counts
    }

    fn execute_ops(
        &mut self,
        delta_mode: bool,
        state: &mut [u8],
        code: &[u8],
        cols: &[&[u8]],
        batch_len: u32,
        mut live: Option<&mut LiveColumns<'_, '_>>,
    ) -> u32 {
        let mut pc = 0usize;
        while pc < code.len() {
            let Some(_) = body_op_len(code, pc) else {
                return INVALID_PROGRAM;
            };
            let Some(op) = Opcode::from_u8(code[pc]) else {
                return INVALID_PROGRAM;
            };
            pc += 1;
            let mut r = Operands::at(code, pc);

            match op {
                Opcode::Halt => break,

                Opcode::BatchMapUpsertLatest | Opcode::BatchMapUpsertLatestTtl => {
                    let (slot, key_col, val_col, ts_col) = (
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                    );
                    let Some(cmp_type) = CmpType::from_u8(operand!(r.byte())) else {
                        return INVALID_PROGRAM;
                    };
                    pc = r.pos();
                    let meta = SlotMetaView::read(state, slot);
                    let result = hashmap_ops::batch_map_upsert(
                        Strategy::Latest,
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        batch_col!(col_u32_exact(col_at(cols, key_col as usize), batch_len)),
                        batch_col!(col_u32_exact(col_at(cols, val_col as usize), batch_len)),
                        Some(batch_col!(cmp_col_exact(
                            col_at(cols, ts_col as usize),
                            batch_len,
                            cmp_type
                        ))),
                        cmp_type,
                        &mut self.ctx(),
                    );
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                Opcode::BatchMapUpsertFirst => {
                    let (slot, key_col, val_col) = (
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                    );
                    pc = r.pos();
                    let meta = SlotMetaView::read(state, slot);
                    let result = hashmap_ops::batch_map_upsert(
                        Strategy::First,
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        batch_col!(col_u32_exact(col_at(cols, key_col as usize), batch_len)),
                        batch_col!(col_u32_exact(col_at(cols, val_col as usize), batch_len)),
                        None,
                        CmpType::F64,
                        &mut self.ctx(),
                    );
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                Opcode::BatchMapUpsertLast => {
                    let (slot, key_col, val_col) = (
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                    );
                    pc = r.pos();
                    let meta = SlotMetaView::read(state, slot);
                    let ts = if meta.has_ttl() {
                        Some(batch_col!(cmp_col_exact(
                            col_at(cols, meta.timestamp_col(state) as usize),
                            batch_len,
                            CmpType::F64
                        )))
                    } else {
                        None
                    };
                    let result = hashmap_ops::batch_map_upsert(
                        Strategy::Last,
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        batch_col!(col_u32_exact(col_at(cols, key_col as usize), batch_len)),
                        batch_col!(col_u32_exact(col_at(cols, val_col as usize), batch_len)),
                        ts,
                        CmpType::F64,
                        &mut self.ctx(),
                    );
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                Opcode::BatchMapUpsertLastTtl => {
                    let (slot, key_col, val_col, ts_col) = (
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                    );
                    pc = r.pos();
                    let meta = SlotMetaView::read(state, slot);
                    let result = hashmap_ops::batch_map_upsert(
                        Strategy::Last,
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        batch_col!(col_u32_exact(col_at(cols, key_col as usize), batch_len)),
                        batch_col!(col_u32_exact(col_at(cols, val_col as usize), batch_len)),
                        Some(batch_col!(cmp_col_exact(
                            col_at(cols, ts_col as usize),
                            batch_len,
                            CmpType::F64
                        ))),
                        CmpType::F64,
                        &mut self.ctx(),
                    );
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                Opcode::BatchMapRemove => {
                    let (slot, key_col) = (operand!(r.index()), operand!(r.index()));
                    pc = r.pos();
                    let meta = SlotMetaView::read(state, slot);
                    let result = hashmap_ops::batch_map_remove(
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        batch_col!(col_u32_exact(col_at(cols, key_col as usize), batch_len)),
                        &mut self.ctx(),
                    );
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                Opcode::BatchMapUpsertMax | Opcode::BatchMapUpsertMin => {
                    let (slot, key_col, val_col, cmp_col) = (
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                    );
                    let Some(cmp_type) = CmpType::from_u8(operand!(r.byte())) else {
                        return INVALID_PROGRAM;
                    };
                    pc = r.pos();
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
                        batch_col!(col_u32_exact(col_at(cols, key_col as usize), batch_len)),
                        batch_col!(col_u32_exact(col_at(cols, val_col as usize), batch_len)),
                        Some(batch_col!(cmp_col_exact(
                            col_at(cols, cmp_col as usize),
                            batch_len,
                            cmp_type
                        ))),
                        cmp_type,
                        &mut self.ctx(),
                    );
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                Opcode::BatchSetInsert => {
                    let (slot, elem_col) = (operand!(r.index()), operand!(r.index()));
                    pc = r.pos();
                    let meta = SlotMetaView::read(state, slot);
                    let ts = if meta.has_ttl() {
                        Some(batch_col!(col_f64_exact(
                            col_at(cols, meta.timestamp_col(state) as usize),
                            batch_len
                        )))
                    } else {
                        None
                    };
                    let result = self.set_insert(
                        delta_mode,
                        state,
                        slot,
                        batch_col!(col_u32_exact(col_at(cols, elem_col as usize), batch_len)),
                        ts,
                    );
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                Opcode::BatchSetInsertTtl => {
                    let (slot, elem_col, ts_col) = (
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                    );
                    pc = r.pos();
                    let result = self.set_insert(
                        delta_mode,
                        state,
                        slot,
                        batch_col!(col_u32_exact(col_at(cols, elem_col as usize), batch_len)),
                        Some(batch_col!(col_f64_exact(
                            col_at(cols, ts_col as usize),
                            batch_len
                        ))),
                    );
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                Opcode::BatchSetRemove => {
                    let (slot, elem_col) = (operand!(r.index()), operand!(r.index()));
                    pc = r.pos();
                    let result = self.set_remove(
                        delta_mode,
                        state,
                        slot,
                        batch_col!(col_u32_exact(col_at(cols, elem_col as usize), batch_len)),
                    );
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                Opcode::BatchBitmapAdd => {
                    let (slot, elem_col) = (operand!(r.index()), operand!(r.index()));
                    pc = r.pos();
                    let meta = SlotMetaView::read(state, slot);
                    let ts = if meta.has_ttl() {
                        Some(batch_col!(col_f64_exact(
                            col_at(cols, meta.timestamp_col(state) as usize),
                            batch_len
                        )))
                    } else {
                        None
                    };
                    let result = self.set_insert(
                        delta_mode,
                        state,
                        slot,
                        batch_col!(col_u32_exact(col_at(cols, elem_col as usize), batch_len)),
                        ts,
                    );
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                Opcode::BatchBitmapRemove => {
                    let (slot, elem_col) = (operand!(r.index()), operand!(r.index()));
                    pc = r.pos();
                    let result = self.set_remove(
                        delta_mode,
                        state,
                        slot,
                        batch_col!(col_u32_exact(col_at(cols, elem_col as usize), batch_len)),
                    );
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                Opcode::BatchBitmapAnd
                | Opcode::BatchBitmapOr
                | Opcode::BatchBitmapAndNot
                | Opcode::BatchBitmapXor => {
                    let (target_slot, source_slot) = (operand!(r.index()), operand!(r.index()));
                    pc = r.pos();
                    let alg_op = match op {
                        Opcode::BatchBitmapAnd => BitmapAlgebraOp::And,
                        Opcode::BatchBitmapOr => BitmapAlgebraOp::Or,
                        Opcode::BatchBitmapAndNot => BitmapAlgebraOp::AndNot,
                        _ => BitmapAlgebraOp::Xor,
                    };
                    let result = self.bitmap_algebra(
                        delta_mode,
                        alg_op,
                        state,
                        target_slot,
                        BitmapSource::Slot(source_slot),
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
                    let target_slot = operand!(r.index());
                    pc = r.pos();
                    let alg_op = match op {
                        Opcode::BatchBitmapAndScratch => BitmapAlgebraOp::And,
                        Opcode::BatchBitmapOrScratch => BitmapAlgebraOp::Or,
                        Opcode::BatchBitmapAndNotScratch => BitmapAlgebraOp::AndNot,
                        _ => BitmapAlgebraOp::Xor,
                    };
                    let result = self.bitmap_algebra(
                        delta_mode,
                        alg_op,
                        state,
                        target_slot,
                        BitmapSource::Scratch,
                    );
                    if result == ErrorCode::CapacityExceeded {
                        return signal_growth(target_slot, result);
                    }
                    if result != ErrorCode::Ok {
                        return result as u32;
                    }
                }

                Opcode::BatchAggSum | Opcode::BatchAggMin | Opcode::BatchAggMax => {
                    let (slot, val_col) = (operand!(r.index()), operand!(r.index()));
                    pc = r.pos();
                    let kind = match op {
                        Opcode::BatchAggSum => AggKind::Sum,
                        Opcode::BatchAggMin => AggKind::Min,
                        _ => AggKind::Max,
                    };
                    let result = exec_agg_f64(
                        &mut self.undo,
                        delta_mode,
                        state,
                        slot,
                        kind,
                        batch_col!(col_f64_exact(col_at(cols, val_col as usize), batch_len)),
                        None,
                        None,
                    );
                    if result != ErrorCode::Ok {
                        return result as u32;
                    }
                }

                Opcode::BatchAggCount => {
                    let slot = operand!(r.index());
                    pc = r.pos();
                    let result = exec_agg_count(
                        &mut self.undo,
                        delta_mode,
                        state,
                        slot,
                        u64::from(batch_len),
                    );
                    if result != ErrorCode::Ok {
                        return result as u32;
                    }
                }

                Opcode::BatchAggSumI64 | Opcode::BatchAggMinI64 | Opcode::BatchAggMaxI64 => {
                    let (slot, val_col) = (operand!(r.index()), operand!(r.index()));
                    pc = r.pos();
                    let kind = match op {
                        Opcode::BatchAggSumI64 => AggKind::Sum,
                        Opcode::BatchAggMinI64 => AggKind::Min,
                        _ => AggKind::Max,
                    };
                    let result = exec_agg_i64(
                        &mut self.undo,
                        delta_mode,
                        state,
                        slot,
                        kind,
                        batch_col!(col_i64_exact(col_at(cols, val_col as usize), batch_len)),
                        None,
                        None,
                    );
                    if result != ErrorCode::Ok {
                        return result as u32;
                    }
                }

                Opcode::BatchScalarLatest => {
                    let (slot, val_col, cmp_col) = (
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                    );
                    pc = r.pos();
                    let cmp_vals =
                        batch_col!(col_f64_exact(col_at(cols, cmp_col as usize), batch_len));
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

                    let smap = match state_init::bind_struct_map(state, operands.slot) {
                        Ok(map) => map,
                        Err(error) => return error as u32,
                    };
                    if let Err(error) = validate_struct_map_operands(state, &smap, &operands) {
                        return error as u32;
                    }
                    let comparison = match operands.comparison_field_idx {
                        Some(field_idx) => {
                            let Some(comparison) = resolve_struct_map_max_comparison(
                                state,
                                &smap,
                                &operands.vals,
                                field_idx,
                            ) else {
                                return INVALID_PROGRAM;
                            };
                            Some(comparison)
                        }
                        None => None,
                    };

                    let keys = batch_col!(col_u32_exact(
                        col_at(cols, operands.key_col as usize),
                        batch_len
                    ));
                    for i in 0..batch_len {
                        let key = keys[i as usize];
                        let should_write = match op {
                            Opcode::BatchStructMapUpsertFirst => smap.find(state, key).is_none(),
                            Opcode::BatchStructMapUpsertMax => {
                                let Some(comparison) = comparison else {
                                    return INVALID_PROGRAM;
                                };
                                match should_upsert_struct_map_max(
                                    state, &smap, key, comparison, cols, i,
                                ) {
                                    Ok(should_write) => should_write,
                                    Err(error) => return error as u32,
                                }
                            }
                            _ => true,
                        };
                        if !should_write {
                            continue;
                        }

                        let result = upsert_struct_map_row(
                            &mut self.undo,
                            delta_mode,
                            state,
                            &smap,
                            operands.slot,
                            key,
                            operands.vals.cols(),
                            operands.vals.fields(),
                            operands.array_fields(),
                            cols,
                            i,
                        );
                        if result != OK {
                            return result;
                        }
                    }
                }

                Opcode::BatchStructMapCountsFold => {
                    let operands = match crate::counts_fold::CountsFoldOp::decode(code, pc) {
                        Ok(operands) => operands,
                        Err(error) => return counts_error(error) as u32,
                    };
                    pc = operands.end;
                    for row in 0..batch_len {
                        let result = fold_counts_row(
                            &mut self.undo,
                            delta_mode,
                            state,
                            &operands,
                            cols,
                            row,
                        );
                        if result != OK {
                            return result;
                        }
                    }
                }

                Opcode::BatchStructMap2UpsertLast => {
                    let Some(operands) = decode_struct_map2_upsert_operands(code, pc) else {
                        return INVALID_PROGRAM;
                    };
                    pc = operands.end;
                    let keys1 = batch_col!(col_u32_exact(
                        col_at(cols, operands.key1_col as usize),
                        batch_len
                    ));
                    let keys2 = batch_col!(col_u32_exact(
                        col_at(cols, operands.key2_col as usize),
                        batch_len
                    ));
                    for i in 0..batch_len {
                        let index = i as usize;
                        let result = single_struct_map2_upsert_last(
                            &mut self.undo,
                            delta_mode,
                            state,
                            operands.slot,
                            keys1[index],
                            keys2[index],
                            operands.vals.cols(),
                            operands.vals.fields(),
                            cols,
                            i,
                        );
                        if result == ErrorCode::CapacityExceeded {
                            return NEEDS_GROWTH;
                        }
                        if result != ErrorCode::Ok {
                            return result as u32;
                        }
                    }
                }

                Opcode::BatchStructMap2UpsertMaxI64x2 => {
                    let Some(operands) = decode_struct_map2_max_i64x2_operands(code, pc) else {
                        return INVALID_PROGRAM;
                    };
                    pc = operands.end;
                    let smap = StructMap2Slot::bind(state, operands.row.slot);
                    if !validate_struct_map2_max_i64x2(state, &smap, &operands) {
                        return INVALID_PROGRAM;
                    }
                    let keys1 = batch_col!(col_u32_exact(
                        col_at(cols, operands.row.key1_col as usize),
                        batch_len
                    ));
                    let keys2 = batch_col!(col_u32_exact(
                        col_at(cols, operands.row.key2_col as usize),
                        batch_len
                    ));
                    for i in 0..batch_len {
                        let index = i as usize;
                        let result = single_struct_map2_upsert_max_i64x2(
                            &mut self.undo,
                            delta_mode,
                            state,
                            &operands,
                            keys1[index],
                            keys2[index],
                            cols,
                            i,
                        );
                        if result == ErrorCode::CapacityExceeded {
                            return NEEDS_GROWTH;
                        }
                        if result != ErrorCode::Ok {
                            return result as u32;
                        }
                    }
                }

                Opcode::BatchStructMap2Remove => {
                    let (slot, key1_col, key2_col) = (
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                    );
                    pc = r.pos();
                    let keys1 =
                        batch_col!(col_u32_exact(col_at(cols, key1_col as usize), batch_len));
                    let keys2 =
                        batch_col!(col_u32_exact(col_at(cols, key2_col as usize), batch_len));
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
                    // type_col:index, match_count:index, match_ids:u32le[N],
                    // body_len:u16le, body.
                    let col_idx = operand!(r.index());
                    let Ok(match_count) = usize::try_from(operand!(r.index())) else {
                        return INVALID_PROGRAM;
                    };
                    let match_ids = operand!(r.bytes(match_count * 4));
                    let body_len = usize::from(operand!(r.u16()));
                    let body = operand!(r.bytes(body_len));
                    pc = r.pos();

                    // One scan plans the body for the batch and validates it
                    // before either pass mutates anything: every instruction
                    // decodes and every BATCH_SCALAR_LATEST names a scalar.
                    let mut plan = core::mem::take(&mut self.element_plan);
                    let result = if plan_element_body(body, &mut plan).is_none()
                        || !plan
                            .scalar_latest_slots
                            .iter()
                            .all(|&slot| is_scalar_latest_slot(state, slot))
                    {
                        INVALID_PROGRAM
                    } else {
                        self.run_for_each(
                            delta_mode,
                            state,
                            body,
                            &mut plan,
                            cols,
                            batch_len,
                            col_at(cols, col_idx as usize),
                            match_ids,
                            live.as_deref_mut(),
                        )
                    };
                    self.element_plan = plan;
                    if result != OK {
                        return result;
                    }
                }

                // Registry-only and init-section opcodes are invalid here.
                _ => return INVALID_PROGRAM,
            }
        }

        OK
    }

    /// Run one planned FOR_EACH. Pass one folds the body's aggregates once
    /// per match id; pass two widens the body at the first row whose type
    /// matches and runs that form over every such row. A body with no
    /// aggregates skips pass one, and one with nothing but aggregates widens
    /// to nothing and runs no row.
    #[allow(clippy::too_many_arguments)]
    fn run_for_each(
        &mut self,
        delta_mode: bool,
        state: &mut [u8],
        body: &[u8],
        plan: &mut BodyPlan,
        cols: &[&[u8]],
        batch_len: u32,
        type_col: &[u8],
        match_ids: &[u8],
        mut live: Option<&mut LiveColumns<'_, '_>>,
    ) -> u32 {
        // Both passes index the type column with row indices up to
        // batch_len; a short column is a malformed batch.
        let type_data = batch_col!(col_u32_exact(type_col, batch_len));
        let match_ids = match_ids.as_chunks::<4>().0;
        if !plan.aggregates.is_empty() {
            for id in match_ids {
                let result = self.execute_batch_aggregates(
                    delta_mode,
                    state,
                    body,
                    &plan.aggregates,
                    cols,
                    batch_len,
                    type_data,
                    u32::from_le_bytes(*id),
                );
                if result != OK {
                    return result;
                }
            }
        }
        let mut widened = false;
        for (ei, &val) in (0..batch_len).zip(type_data) {
            if !match_ids.iter().any(|id| val == u32::from_le_bytes(*id)) {
                continue;
            }
            if !widened {
                widened = true;
                // The scan accepted every instruction this decodes.
                if widen_element_body(body, plan).is_none() {
                    return INVALID_PROGRAM;
                }
            }
            if plan.wide.is_empty() {
                return OK;
            }
            let result = self.execute_element_opcodes(
                delta_mode,
                state,
                &plan.wide,
                cols,
                ei,
                ei,
                NO_PARENT_TS_COL,
                live.as_deref_mut(),
            );
            if result != OK {
                return result;
            }
        }
        OK
    }

    /// Execute pass one of FOR_EACH for one match id: the aggregates at
    /// `aggregates` (offsets into `body`), in order.
    #[allow(clippy::too_many_arguments)]
    fn execute_batch_aggregates(
        &mut self,
        delta_mode: bool,
        state: &mut [u8],
        body: &[u8],
        aggregates: &[usize],
        cols: &[&[u8]],
        batch_len: u32,
        type_data: &[u32],
        type_id: u32,
    ) -> u32 {
        for &at in aggregates {
            let Some(op) = body.get(at).copied().and_then(Opcode::from_u8) else {
                return INVALID_PROGRAM;
            };
            let mut r = Operands::at(body, at + 1);
            match op {
                Opcode::BatchAggSum | Opcode::BatchAggMin | Opcode::BatchAggMax => {
                    let (slot, val_col) = (operand!(r.index()), operand!(r.index()));
                    let kind = match op {
                        Opcode::BatchAggSum => AggKind::Sum,
                        Opcode::BatchAggMin => AggKind::Min,
                        _ => AggKind::Max,
                    };
                    let result = exec_agg_f64(
                        &mut self.undo,
                        delta_mode,
                        state,
                        slot,
                        kind,
                        batch_col!(col_f64_exact(col_at(cols, val_col as usize), batch_len)),
                        Some(TypeMask {
                            data: type_data,
                            id: type_id,
                        }),
                        None,
                    );
                    if result != ErrorCode::Ok {
                        return result as u32;
                    }
                }
                Opcode::BatchAggCount => {
                    let slot = operand!(r.index());
                    let matched = aggregates::masked_agg_count(type_data, type_id);
                    let result =
                        exec_agg_count(&mut self.undo, delta_mode, state, slot, u64::from(matched));
                    if result != ErrorCode::Ok {
                        return result as u32;
                    }
                }
                Opcode::BatchAggSumIf | Opcode::BatchAggMinIf | Opcode::BatchAggMaxIf => {
                    let (slot, val_col, pred_col) = (
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                    );
                    let kind = match op {
                        Opcode::BatchAggSumIf => AggKind::Sum,
                        Opcode::BatchAggMinIf => AggKind::Min,
                        _ => AggKind::Max,
                    };
                    let result = exec_agg_f64(
                        &mut self.undo,
                        delta_mode,
                        state,
                        slot,
                        kind,
                        batch_col!(col_f64_exact(col_at(cols, val_col as usize), batch_len)),
                        Some(TypeMask {
                            data: type_data,
                            id: type_id,
                        }),
                        Some(batch_col!(col_u32_exact(
                            col_at(cols, pred_col as usize),
                            batch_len
                        ))),
                    );
                    if result != ErrorCode::Ok {
                        return result as u32;
                    }
                }
                Opcode::BatchAggCountIf => {
                    let (slot, pred_col) = (operand!(r.index()), operand!(r.index()));
                    let preds =
                        batch_col!(col_u32_exact(col_at(cols, pred_col as usize), batch_len));
                    let mut matched = 0u64;
                    for i in 0..batch_len as usize {
                        if type_data[i] == type_id && preds[i] != 0 {
                            matched += 1;
                        }
                    }
                    let result = exec_agg_count(&mut self.undo, delta_mode, state, slot, matched);
                    if result != ErrorCode::Ok {
                        return result as u32;
                    }
                }
                Opcode::BatchScalarLatest => {
                    let (slot, val_col, cmp_col) = (
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                    );
                    let cmp_vals =
                        batch_col!(col_f64_exact(col_at(cols, cmp_col as usize), batch_len));
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
                Opcode::BatchAggSumI64 | Opcode::BatchAggMinI64 | Opcode::BatchAggMaxI64 => {
                    let (slot, val_col) = (operand!(r.index()), operand!(r.index()));
                    let kind = match op {
                        Opcode::BatchAggSumI64 => AggKind::Sum,
                        Opcode::BatchAggMinI64 => AggKind::Min,
                        _ => AggKind::Max,
                    };
                    let result = exec_agg_i64(
                        &mut self.undo,
                        delta_mode,
                        state,
                        slot,
                        kind,
                        batch_col!(col_i64_exact(col_at(cols, val_col as usize), batch_len)),
                        Some(TypeMask {
                            data: type_data,
                            id: type_id,
                        }),
                        None,
                    );
                    if result != ErrorCode::Ok {
                        return result as u32;
                    }
                }
                _ => return INVALID_PROGRAM,
            }
        }
        OK
    }

    /// Execute element opcodes one at a time, recursing through FLAT_MAP.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn execute_element_opcodes(
        &mut self,
        delta_mode: bool,
        state: &mut [u8],
        body: &[u8],
        cols: &[&[u8]],
        child_idx: u32,
        parent_idx: u32,
        parent_ts_col: u32,
        mut live: Option<&mut LiveColumns<'_, '_>>,
    ) -> u32 {
        // Column cell reads at an element index. Bounds are the column's own
        // length (child columns can be longer than batch_len under FLAT_MAP).
        let cell_u32 =
            |cols: &[&[u8]], idx: u32, i: u32| bytes::read_u32(col_at(cols, idx as usize), i * 4);
        let cell_f64 =
            |cols: &[&[u8]], idx: u32, i: u32| bytes::read_f64(col_at(cols, idx as usize), i * 8);
        // A conditional opcode's predicate cell: a live derived column is
        // evaluated here, against the state the rows before this one left,
        // so the answer does not depend on where the host cut the batch.
        macro_rules! predicate {
            ($pred_col:expr) => {{
                let pred = match live.as_deref_mut() {
                    Some(live) => match live.cell($pred_col, child_idx, state, cols) {
                        Some(Ok(cell)) => cell,
                        Some(Err(code)) => return code as u32,
                        None => cell_u32(cols, $pred_col, child_idx),
                    },
                    None => cell_u32(cols, $pred_col, child_idx),
                };
                if pred == 0 {
                    continue;
                }
            }};
        }

        // `body` is a widened body (`element_body`): no aggregates, every
        // index a `u32`, so each instruction is read once and advances past
        // what it read.
        let mut bpc = 0usize;
        while bpc < body.len() {
            let Some(op) = Opcode::from_u8(body[bpc]) else {
                return INVALID_PROGRAM;
            };
            let mut r = WideOperands::at(body, bpc + 1);

            match op {
                Opcode::BatchMapUpsertLatest | Opcode::BatchMapUpsertLatestTtl => {
                    let (slot, key_col, val_col, ts_col) = (
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                    );
                    let Some(cmp_type) = CmpType::from_u8(operand!(r.byte())) else {
                        return INVALID_PROGRAM;
                    };
                    bpc = r.pos();

                    let (cmp_raw, cmp_idx) = if parent_ts_col != NO_PARENT_TS_COL {
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
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                // MAP_UPSERT_FIRST (0x21)
                Opcode::BatchMapUpsertFirst => {
                    let (slot, key_col, val_col) = (
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                    );
                    bpc = r.pos();
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
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                // MAP_UPSERT_LAST (0x22)
                Opcode::BatchMapUpsertLast => {
                    let (slot, key_col, val_col) = (
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                    );
                    bpc = r.pos();
                    let meta = SlotMetaView::read(state, slot);
                    let ttl_cmp = if meta.has_ttl() {
                        cell_f64(cols, meta.timestamp_col(state), child_idx).to_bits()
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
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                // MAP_UPSERT_LAST_TTL (0x25)
                Opcode::BatchMapUpsertLastTtl => {
                    let (slot, key_col, val_col, ts_col) = (
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                    );
                    bpc = r.pos();
                    let (ts_raw, ts_idx) = if parent_ts_col != NO_PARENT_TS_COL {
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
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                // MAP_REMOVE (0x23)
                Opcode::BatchMapRemove => {
                    let (slot, key_col) = (operand!(r.index()), operand!(r.index()));
                    bpc = r.pos();
                    let meta = SlotMetaView::read(state, slot);
                    let result = hashmap_ops::single_map_remove(
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        cell_u32(cols, key_col, child_idx),
                        &mut self.ctx(),
                    );
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                // MAP_UPSERT_MAX (0x26) / MIN (0x27)
                Opcode::BatchMapUpsertMax | Opcode::BatchMapUpsertMin => {
                    let (slot, key_col, val_col, cmp_col) = (
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                    );
                    let Some(cmp_type) = CmpType::from_u8(operand!(r.byte())) else {
                        return INVALID_PROGRAM;
                    };
                    bpc = r.pos();
                    let strategy = if op == Opcode::BatchMapUpsertMax {
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
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                // MAP_UPSERT_LATEST_IF (0x28)
                Opcode::BatchMapUpsertLatestIf => {
                    let (slot, key_col, val_col, ts_col) = (
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                    );
                    let Some(cmp_type) = CmpType::from_u8(operand!(r.byte())) else {
                        return INVALID_PROGRAM;
                    };
                    let pred_col = operand!(r.index());
                    bpc = r.pos();

                    predicate!(pred_col);

                    let (cmp_raw, cmp_idx) = if parent_ts_col != NO_PARENT_TS_COL {
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
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                // MAP_UPSERT_FIRST_IF (0x29) / LAST_IF (0x2A)
                Opcode::BatchMapUpsertFirstIf | Opcode::BatchMapUpsertLastIf => {
                    let (slot, key_col, val_col, pred_col) = (
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                    );
                    bpc = r.pos();

                    predicate!(pred_col);

                    let meta = SlotMetaView::read(state, slot);
                    let (strategy, cmp) = if op == Opcode::BatchMapUpsertFirstIf {
                        (Strategy::First, 0u64)
                    } else {
                        let ttl_cmp = if meta.has_ttl() {
                            cell_f64(cols, meta.timestamp_col(state), child_idx).to_bits()
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
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                // MAP_REMOVE_IF (0x2B)
                Opcode::BatchMapRemoveIf => {
                    let (slot, key_col, pred_col) = (
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                    );
                    bpc = r.pos();
                    predicate!(pred_col);
                    let meta = SlotMetaView::read(state, slot);
                    let result = hashmap_ops::single_map_remove(
                        delta_mode,
                        state,
                        &meta,
                        slot,
                        cell_u32(cols, key_col, child_idx),
                        &mut self.ctx(),
                    );
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                // MAP_UPSERT_MAX_IF (0x2C) / MIN_IF (0x2D)
                Opcode::BatchMapUpsertMaxIf | Opcode::BatchMapUpsertMinIf => {
                    let (slot, key_col, val_col, cmp_col) = (
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                    );
                    let Some(cmp_type) = CmpType::from_u8(operand!(r.byte())) else {
                        return INVALID_PROGRAM;
                    };
                    let pred_col = operand!(r.index());
                    bpc = r.pos();

                    predicate!(pred_col);

                    let strategy = if op == Opcode::BatchMapUpsertMaxIf {
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
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                // SET_INSERT and BITMAP_ADD share the single-element set path.
                Opcode::BatchSetInsert | Opcode::BatchBitmapAdd => {
                    let (slot, elem_col) = (operand!(r.index()), operand!(r.index()));
                    bpc = r.pos();
                    let meta = SlotMetaView::read(state, slot);
                    let ts = if meta.has_ttl() {
                        cell_f64(cols, meta.timestamp_col(state), child_idx)
                    } else {
                        0.0
                    };
                    let result = self.set_insert(
                        delta_mode,
                        state,
                        slot,
                        &[cell_u32(cols, elem_col, child_idx)],
                        meta.has_ttl().then_some(&[ts][..]),
                    );
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                // SET_INSERT_TTL (0x32)
                Opcode::BatchSetInsertTtl => {
                    let (slot, elem_col, ts_col) = (
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                    );
                    bpc = r.pos();
                    let ts = if parent_ts_col != NO_PARENT_TS_COL {
                        cell_f64(cols, parent_ts_col, parent_idx)
                    } else {
                        cell_f64(cols, ts_col, child_idx)
                    };
                    let meta = SlotMetaView::read(state, slot);
                    let result = self.set_insert(
                        delta_mode,
                        state,
                        slot,
                        &[cell_u32(cols, elem_col, child_idx)],
                        meta.has_ttl().then_some(&[ts][..]),
                    );
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                // SET_INSERT_IF (0x33)
                Opcode::BatchSetInsertIf => {
                    let (slot, elem_col, pred_col) = (
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                    );
                    bpc = r.pos();
                    predicate!(pred_col);
                    let meta = SlotMetaView::read(state, slot);
                    let ts = if meta.has_ttl() {
                        cell_f64(cols, meta.timestamp_col(state), child_idx)
                    } else {
                        0.0
                    };
                    let result = self.set_insert(
                        delta_mode,
                        state,
                        slot,
                        &[cell_u32(cols, elem_col, child_idx)],
                        meta.has_ttl().then_some(&[ts][..]),
                    );
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                // SET_REMOVE (0x31) / BITMAP_REMOVE (0x35)
                Opcode::BatchSetRemove | Opcode::BatchBitmapRemove => {
                    let (slot, elem_col) = (operand!(r.index()), operand!(r.index()));
                    bpc = r.pos();
                    let result = self.set_remove(
                        delta_mode,
                        state,
                        slot,
                        &[cell_u32(cols, elem_col, child_idx)],
                    );
                    if result != ErrorCode::Ok {
                        return signal_growth(slot, result);
                    }
                }

                // STRUCT_MAP_UPSERT_LAST/FIRST/MAX (0x80/0x81/0x82)
                Opcode::BatchStructMapUpsertLast
                | Opcode::BatchStructMapUpsertFirst
                | Opcode::BatchStructMapUpsertMax => {
                    let operands = &mut self.element_struct_map;
                    if operands
                        .decode_from(&mut r, op == Opcode::BatchStructMapUpsertMax)
                        .is_none()
                    {
                        return INVALID_PROGRAM;
                    }
                    bpc = operands.end;

                    let smap = match state_init::bind_struct_map(state, operands.slot) {
                        Ok(map) => map,
                        Err(error) => return error as u32,
                    };
                    if let Err(error) = validate_struct_map_operands(state, &smap, operands) {
                        return error as u32;
                    }
                    let comparison = match operands.comparison_field_idx {
                        Some(field_idx) => {
                            let Some(comparison) = resolve_struct_map_max_comparison(
                                state,
                                &smap,
                                &operands.vals,
                                field_idx,
                            ) else {
                                return INVALID_PROGRAM;
                            };
                            Some(comparison)
                        }
                        None => None,
                    };
                    let key = cell_u32(cols, operands.key_col, child_idx);
                    let should_write = match op {
                        Opcode::BatchStructMapUpsertFirst => smap.find(state, key).is_none(),
                        Opcode::BatchStructMapUpsertMax => {
                            let Some(comparison) = comparison else {
                                return INVALID_PROGRAM;
                            };
                            match should_upsert_struct_map_max(
                                state, &smap, key, comparison, cols, child_idx,
                            ) {
                                Ok(should_write) => should_write,
                                Err(error) => return error as u32,
                            }
                        }
                        _ => true,
                    };
                    if should_write {
                        let result = upsert_struct_map_row(
                            &mut self.undo,
                            delta_mode,
                            state,
                            &smap,
                            operands.slot,
                            key,
                            operands.vals.cols(),
                            operands.vals.fields(),
                            operands.array_fields(),
                            cols,
                            child_idx,
                        );
                        if result != OK {
                            return result;
                        }
                    }
                }

                Opcode::BatchStructMapCountsFold => {
                    let Some(operands) = crate::counts_fold::CountsFoldOp::decode_from(&mut r)
                    else {
                        return INVALID_PROGRAM;
                    };
                    bpc = operands.end;
                    let result = fold_counts_row(
                        &mut self.undo,
                        delta_mode,
                        state,
                        &operands,
                        cols,
                        child_idx,
                    );
                    if result != OK {
                        return result;
                    }
                }

                // Exact two-lane struct map upsert/remove.
                Opcode::BatchStructMap2UpsertLast => {
                    let operands = &mut self.element_struct_map2;
                    if operands.decode_from(&mut r, 0).is_none() {
                        return INVALID_PROGRAM;
                    }
                    bpc = operands.end;
                    let result = single_struct_map2_upsert_last(
                        &mut self.undo,
                        delta_mode,
                        state,
                        operands.slot,
                        cell_u32(cols, operands.key1_col, child_idx),
                        cell_u32(cols, operands.key2_col, child_idx),
                        operands.vals.cols(),
                        operands.vals.fields(),
                        cols,
                        child_idx,
                    );
                    if result == ErrorCode::CapacityExceeded {
                        return NEEDS_GROWTH;
                    }
                    if result != ErrorCode::Ok {
                        return result as u32;
                    }
                }
                Opcode::BatchStructMap2UpsertMaxI64x2 => {
                    let operands = &mut self.element_struct_map2_max;
                    if operands.decode_from(&mut r).is_none() {
                        return INVALID_PROGRAM;
                    }
                    bpc = operands.end;
                    let smap = StructMap2Slot::bind(state, operands.row.slot);
                    if !validate_struct_map2_max_i64x2(state, &smap, operands) {
                        return INVALID_PROGRAM;
                    }
                    let result = single_struct_map2_upsert_max_i64x2(
                        &mut self.undo,
                        delta_mode,
                        state,
                        operands,
                        cell_u32(cols, operands.row.key1_col, child_idx),
                        cell_u32(cols, operands.row.key2_col, child_idx),
                        cols,
                        child_idx,
                    );
                    if result == ErrorCode::CapacityExceeded {
                        return NEEDS_GROWTH;
                    }
                    if result != ErrorCode::Ok {
                        return result as u32;
                    }
                }
                Opcode::BatchStructMap2Remove => {
                    let (slot, key1_col, key2_col) = (
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                    );
                    bpc = r.pos();
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

                //#region reduce-typed-state.probe-exec
                // STRUCT_MAP_PROBE (0x2e)
                Opcode::BatchStructMapProbe => {
                    let probe_slot = operand!(r.index());
                    let key_col = operand!(r.index());
                    let _miss_mode = operand!(r.byte());
                    let out_slot = operand!(r.index());
                    let num_fields = usize::from(operand!(r.byte()));
                    if num_fields > 32 {
                        return INVALID_PROGRAM;
                    }

                    let mut probe_fis = [0u8; 32];
                    let mut out_fis = [0u8; 32];
                    for f in 0..num_fields {
                        probe_fis[f] = operand!(r.byte());
                        out_fis[f] = operand!(r.byte());
                    }
                    let out_key_col = operand!(r.index());
                    bpc = r.pos();

                    let key = cell_u32(cols, key_col, child_idx);
                    let probe = match state_init::bind_struct_map(state, probe_slot) {
                        Ok(probe) => probe,
                        Err(error) => return error as u32,
                    };
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
                    if result == ErrorCode::CapacityExceeded {
                        NEEDS_GROWTH_SLOT.store(out_slot, Ordering::Relaxed);
                        return NEEDS_GROWTH;
                    }
                    if result != ErrorCode::Ok {
                        return result as u32;
                    }
                }
                //#endregion reduce-typed-state.probe-exec

                //#region reduce-typed-state.scatter-exec
                // STRUCT_MAP_PROBE_SCATTER (0x2f)
                Opcode::BatchStructMapProbeScatter => {
                    let probe_slot = operand!(r.index());
                    let key_col = operand!(r.index());
                    let _miss_mode = operand!(r.byte());
                    let route_field_idx = operand!(r.byte());
                    let op_field_idx = operand!(r.byte());
                    let num_routes = usize::from(operand!(r.byte()));
                    if num_routes > MAX_SCATTER_ROUTES {
                        return INVALID_PROGRAM;
                    }

                    let mut route_kinds = [0u8; MAX_SCATTER_ROUTES];
                    let mut route_dest_slots = [0u32; MAX_SCATTER_ROUTES];
                    let mut route_dest_fields = [0u8; MAX_SCATTER_ROUTES];
                    let mut route_out_key_fields = [0u8; MAX_SCATTER_ROUTES];
                    let mut route_v_src_fields = [0u8; MAX_SCATTER_ROUTES];
                    for ri in 0..num_routes {
                        route_kinds[ri] = operand!(r.byte());
                        route_dest_slots[ri] = operand!(r.index());
                        route_dest_fields[ri] = operand!(r.byte());
                        route_out_key_fields[ri] = operand!(r.byte());
                        route_v_src_fields[ri] = operand!(r.byte());
                    }
                    bpc = r.pos();

                    let key = cell_u32(cols, key_col, child_idx);
                    let probe = match state_init::bind_struct_map(state, probe_slot) {
                        Ok(probe) => probe,
                        Err(error) => return error as u32,
                    };
                    for field in [route_field_idx, op_field_idx] {
                        if field >= probe.num_fields
                            || !matches!(
                                probe.field_type(state, field),
                                StructFieldType::UInt32 | StructFieldType::String
                            )
                        {
                            return INVALID_PROGRAM;
                        }
                    }
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
                    let out_key_field = route_out_key_fields[ri];
                    if out_key_field >= probe.num_fields
                        || !matches!(
                            probe.field_type(state, out_key_field),
                            StructFieldType::UInt32 | StructFieldType::String
                        )
                        || v_src_field_idx >= probe.num_fields
                    {
                        return INVALID_PROGRAM;
                    }
                    let out_key_off = probe_row + probe.field_offset(state, out_key_field);
                    let out_key = bytes::read_u32(state, out_key_off);

                    match route_kind {
                        // kind 0: card-one struct field on the `nodes` map.
                        0 => {
                            let out = match state_init::bind_struct_map(state, dest_slot) {
                                Ok(out) => out,
                                Err(error) => return error as u32,
                            };
                            if dest_field_idx >= out.num_fields
                                || v_src_field_idx >= probe.num_fields
                            {
                                return INVALID_PROGRAM;
                            }
                            let dst_ft = out.field_type(state, dest_field_idx);
                            if dst_ft != probe.field_type(state, v_src_field_idx)
                                || is_arena_field_type(dst_ft)
                            {
                                return INVALID_PROGRAM;
                            }
                            // The source cell is copied out of the probed row before
                            // the output row is touched, so no write reads its own
                            // stale bytes.
                            let fsize = struct_field_size(dst_ft);
                            let src_off = probe_row + probe.field_offset(state, v_src_field_idx);
                            let mut cell = [0u8; 8];
                            cell[..fsize as usize].copy_from_slice(
                                &state[src_off as usize..(src_off + fsize) as usize],
                            );
                            let code = scatter_card_one(
                                &mut self.undo,
                                delta_mode,
                                state,
                                &out,
                                dest_slot,
                                out_key,
                                dest_field_idx,
                                &cell,
                                fsize,
                                is_retract,
                            );
                            if code != ErrorCode::Ok {
                                return signal_growth(dest_slot, code);
                            }
                        }
                        // kind 1: card-many set — element is the precomputed
                        // interned composite (e, v) read off the probed row.
                        1 => {
                            if dest_slot >= state_num_slots(state)
                                || is_arena_field_type(probe.field_type(state, v_src_field_idx))
                            {
                                return INVALID_PROGRAM;
                            }
                            let meta = SlotMetaView::read(state, dest_slot);
                            if !matches!(meta.slot_type(), SlotType::HashSet | SlotType::Bitmap) {
                                return INVALID_PROGRAM;
                            }
                            let src_off = probe_row + probe.field_offset(state, v_src_field_idx);
                            let v = bytes::read_u32(state, src_off);
                            if is_retract {
                                let result = self.set_remove(delta_mode, state, dest_slot, &[v]);
                                if result != ErrorCode::Ok {
                                    return signal_growth(dest_slot, result);
                                }
                            } else {
                                let set_result = self.set_insert(
                                    delta_mode,
                                    state,
                                    dest_slot,
                                    &[v],
                                    meta.has_ttl().then_some(&[0.0][..]),
                                );
                                if set_result != ErrorCode::Ok {
                                    return signal_growth(dest_slot, set_result);
                                }
                            }
                        }
                        // kind 2: deferred to Phase 2 — never emitted.
                        _ => return INVALID_PROGRAM,
                    }
                }
                //#endregion reduce-typed-state.scatter-exec

                //#region reduce-typed-state.scatter-element-exec
                // STRUCT_MAP_SCATTER (0x3e) — probe-free attribute-routed
                // dispatch. The FLAT_MAP element already carries the resolved
                // route/op/destination-key/value as columns (the producer
                // resolved them before building the batch), so they are read
                // directly and no intermediate row is staged in a slot. Kind
                // arms, the identical-assert no-op, and the retract-iff-current
                // rule are byte-identical to 0x2f minus the probe.
                Opcode::BatchStructMapScatter => {
                    let route_col = operand!(r.index());
                    let op_col = operand!(r.index());
                    let key_col = operand!(r.index());
                    let num_routes = usize::from(operand!(r.byte()));
                    if num_routes > MAX_SCATTER_ROUTES {
                        return INVALID_PROGRAM;
                    }

                    let mut route_kinds = [0u8; MAX_SCATTER_ROUTES];
                    let mut route_dest_slots = [0u32; MAX_SCATTER_ROUTES];
                    let mut route_dest_fields = [0u8; MAX_SCATTER_ROUTES];
                    let mut route_v_cols = [0u32; MAX_SCATTER_ROUTES];
                    for ri in 0..num_routes {
                        route_kinds[ri] = operand!(r.byte());
                        route_dest_slots[ri] = operand!(r.index());
                        route_dest_fields[ri] = operand!(r.byte());
                        route_v_cols[ri] = operand!(r.index());
                    }
                    bpc = r.pos();

                    let key = cell_u32(cols, key_col, child_idx);
                    if key == EMPTY_KEY || key == TOMBSTONE {
                        continue;
                    }
                    let route_ord = cell_u32(cols, route_col, child_idx);
                    if route_ord == 0xFFFF_FFFF || route_ord as usize >= num_routes {
                        continue; // SKIP / out-of-range → SKIP
                    }
                    let is_retract = cell_u32(cols, op_col, child_idx) != 0;
                    let ri = route_ord as usize;
                    let route_kind = route_kinds[ri];
                    let dest_slot = route_dest_slots[ri];
                    let dest_field_idx = route_dest_fields[ri];
                    let v_col = route_v_cols[ri];

                    match route_kind {
                        // kind 0: card-one struct field on the `nodes` map.
                        0 => {
                            let out = match state_init::bind_struct_map(state, dest_slot) {
                                Ok(out) => out,
                                Err(error) => return error as u32,
                            };
                            if dest_field_idx >= out.num_fields {
                                return INVALID_PROGRAM;
                            }
                            let dst_ft = out.field_type(state, dest_field_idx);
                            let Some((cell, cell_size)) = scalar_cell_encoded(
                                dst_ft,
                                col_at(cols, v_col as usize),
                                child_idx,
                            ) else {
                                // Array destinations have no column-cell value
                                // form; refuse by name instead of comparing
                                // field bytes against garbage.
                                return INVALID_PROGRAM;
                            };
                            let code = scatter_card_one(
                                &mut self.undo,
                                delta_mode,
                                state,
                                &out,
                                dest_slot,
                                key,
                                dest_field_idx,
                                &cell,
                                cell_size,
                                is_retract,
                            );
                            if code != ErrorCode::Ok {
                                return signal_growth(dest_slot, code);
                            }
                        }
                        // kind 1: card-many set — element is the precomputed
                        // interned composite (e, v) read from its column.
                        1 => {
                            if dest_slot >= state_num_slots(state) {
                                return INVALID_PROGRAM;
                            }
                            let meta = SlotMetaView::read(state, dest_slot);
                            if !matches!(meta.slot_type(), SlotType::HashSet | SlotType::Bitmap) {
                                return INVALID_PROGRAM;
                            }
                            let v = cell_u32(cols, v_col, child_idx);
                            if is_retract {
                                let result = self.set_remove(delta_mode, state, dest_slot, &[v]);
                                if result != ErrorCode::Ok {
                                    return signal_growth(dest_slot, result);
                                }
                            } else {
                                let set_result = self.set_insert(
                                    delta_mode,
                                    state,
                                    dest_slot,
                                    &[v],
                                    meta.has_ttl().then_some(&[0.0][..]),
                                );
                                if set_result != ErrorCode::Ok {
                                    return signal_growth(dest_slot, set_result);
                                }
                            }
                        }
                        // kind 2: deferred to Phase 2 — never emitted.
                        _ => return INVALID_PROGRAM,
                    }
                }
                //#endregion reduce-typed-state.scatter-element-exec

                //#region reduce-typed-state.scatter-element-guarded-exec
                // STRUCT_MAP_SCATTER_GUARDED (0x3f) — 0x3e's routing and
                // write arms behind a per-row transaction guard. The
                // semantics are stated once, on the opcode's declaration;
                // what is here is how they run in one pass.
                //
                // Order of work per element, and why: one probe resolves the
                // guarded row, the guard decides whether the element may act,
                // and the stamp and the kind-0 card write then land in one
                // physical capture of that row. The stamp precedes the card
                // write because an identical assert skips the card write but
                // must still land the guard, or a transaction between the two
                // would write over it.
                Opcode::BatchStructMapScatterGuarded => {
                    let route_col = operand!(r.index());
                    let op_col = operand!(r.index());
                    let key_col = operand!(r.index());
                    let guard_slot = operand!(r.index());
                    let num_guards = usize::from(operand!(r.byte()));
                    let Some(guard) = GuardTuple::decode(&mut r, num_guards) else {
                        return INVALID_PROGRAM;
                    };

                    let num_routes = usize::from(operand!(r.byte()));
                    if num_routes > MAX_SCATTER_ROUTES {
                        return INVALID_PROGRAM;
                    }

                    let mut route_kinds = [0u8; MAX_SCATTER_ROUTES];
                    let mut route_dest_slots = [0u32; MAX_SCATTER_ROUTES];
                    let mut route_dest_fields = [0u8; MAX_SCATTER_ROUTES];
                    let mut route_v_cols = [0u32; MAX_SCATTER_ROUTES];
                    for ri in 0..num_routes {
                        route_kinds[ri] = operand!(r.byte());
                        route_dest_slots[ri] = operand!(r.index());
                        route_dest_fields[ri] = operand!(r.byte());
                        route_v_cols[ri] = operand!(r.index());
                        // The guarded row IS the destination row. A kind-0 route
                        // pointing elsewhere would be a card-one write gated by a
                        // guard no row of its own slot keeps — refused by name
                        // rather than left silently unguarded.
                        if route_kinds[ri] == 0 && route_dest_slots[ri] != guard_slot {
                            return INVALID_PROGRAM;
                        }
                    }
                    bpc = r.pos();

                    let key = cell_u32(cols, key_col, child_idx);
                    if key == EMPTY_KEY || key == TOMBSTONE {
                        continue;
                    }
                    let route_ord = cell_u32(cols, route_col, child_idx);
                    if route_ord == 0xFFFF_FFFF || route_ord as usize >= num_routes {
                        continue; // SKIP / out-of-range → SKIP
                    }
                    let is_retract = cell_u32(cols, op_col, child_idx) != 0;
                    let ri = route_ord as usize;
                    let route_kind = route_kinds[ri];
                    let dest_slot = route_dest_slots[ri];
                    let dest_field_idx = route_dest_fields[ri];
                    let v_col = route_v_cols[ri];

                    let guarded = match state_init::bind_struct_map(state, guard_slot) {
                        Ok(guarded) => guarded,
                        Err(error) => return error as u32,
                    };
                    let selected_cell = match route_kind {
                        0 => {
                            if dest_field_idx >= guarded.num_fields {
                                return INVALID_PROGRAM;
                            }
                            let kind = guarded.field_type(state, dest_field_idx);
                            if is_arena_field_type(kind) {
                                return INVALID_PROGRAM;
                            }
                            if let Err(error) = validate_scalar_fields(
                                state,
                                guarded.field_types_off,
                                guarded.num_fields,
                                &[v_col],
                                &[dest_field_idx],
                                cols,
                                child_idx,
                            ) {
                                return error as u32;
                            }
                            let Some(cell) =
                                scalar_cell_encoded(kind, col_at(cols, v_col as usize), child_idx)
                            else {
                                return INVALID_PROGRAM;
                            };
                            Some(cell)
                        }
                        1 => {
                            if dest_slot >= state_num_slots(state)
                                || !matches!(
                                    SlotMetaView::read(state, dest_slot).slot_type(),
                                    SlotType::HashSet | SlotType::Bitmap
                                )
                            {
                                return INVALID_PROGRAM;
                            }
                            None
                        }
                        _ => return INVALID_PROGRAM,
                    };
                    let Some(incoming) = guard.incoming(state, &guarded, cols, child_idx) else {
                        return INVALID_PROGRAM;
                    };
                    // One probe per element. An assert plans its guarded row, found
                    // or new; a retract only looks it up, because removing a value
                    // never creates a row.
                    let target = if is_retract {
                        guarded
                            .find(state, key)
                            .map(|pos| crate::struct_map::Upsert { pos, is_new: false })
                    } else {
                        let Some(planned) = guarded.plan_upsert(state, key) else {
                            return signal_growth(guard_slot, ErrorCode::CapacityExceeded);
                        };
                        Some(planned)
                    };
                    let stored = target
                        .filter(|up| !up.is_new)
                        .and_then(|up| guard.stored(state, &guarded, guarded.row_off(up.pos)));
                    if guard.loses(&incoming, stored) {
                        continue; // an older transaction writes nothing, by any route
                    }
                    // A retract of an absent row has no row to stamp, and creating
                    // one to remember it would grow the map from keys nothing ever
                    // asserted.
                    let stamp_needed =
                        target.is_some() && !guard.already_stamped(&incoming, stored);
                    let card_changes = match (target, selected_cell) {
                        (Some(up), Some((cell, cell_size))) => card_one_changes(
                            state,
                            &guarded,
                            up,
                            dest_field_idx,
                            &cell,
                            cell_size,
                            is_retract,
                        ),
                        _ => false,
                    };
                    // Kind 0's stamp and card write share one row capture. Kind 1's
                    // set writes other rows, so the guard's capture is reserved before
                    // the set and the stamp lands only once the set is accepted: a
                    // refused set leaves the guard unstamped.
                    let guard_flags = slot_meta_base(guard_slot) + SlotMetaOffset::CHANGE_FLAGS;
                    if route_kind == 0 {
                        if let Some(up) = target.filter(|_| stamp_needed || card_changes) {
                            let written = journal_row_write(
                                &mut self.undo,
                                delta_mode,
                                state,
                                &guarded,
                                guard_slot,
                                key,
                                up,
                                |state: &mut [u8]| {
                                    if stamp_needed {
                                        let stamped =
                                            guard.stamp(state, &guarded, up.pos, cols, child_idx);
                                        if stamped != ErrorCode::Ok {
                                            return stamped;
                                        }
                                        state[guard_flags as usize] |= if up.is_new {
                                            ChangeFlag::INSERTED
                                        } else {
                                            ChangeFlag::UPDATED
                                        };
                                    }
                                    if let Some((cell, cell_size)) = selected_cell {
                                        write_card_one(
                                            state,
                                            &guarded,
                                            dest_slot,
                                            up.pos,
                                            dest_field_idx,
                                            &cell,
                                            cell_size,
                                            is_retract,
                                        );
                                    }
                                    ErrorCode::Ok
                                },
                            );
                            if written != ErrorCode::Ok {
                                return written as u32;
                            }
                        }
                    } else {
                        // kind 1: card-many set — the element carries the precomputed
                        // interned composite in its column. The set keeps no guard of
                        // its own; the guarded row's guard gates this write too, which
                        // is what makes a stale transaction lose the whole group.
                        // Kind 0 was written above, and the route selection refused
                        // every other kind.
                        let meta = SlotMetaView::read(state, dest_slot);
                        let v = cell_u32(cols, v_col, child_idx);
                        let guard_frame = match target.filter(|_| stamp_needed) {
                            Some(up) => {
                                let ranges = row_capture_ranges(&guarded, guard_slot, up);
                                match self.undo.begin_capture(state, &ranges) {
                                    Ok(capture) => Some((up, ranges, capture)),
                                    Err(error) => return error as u32,
                                }
                            }
                            None => None,
                        };
                        let set_result = if is_retract {
                            self.set_remove(delta_mode, state, dest_slot, &[v])
                        } else {
                            self.set_insert(
                                delta_mode,
                                state,
                                dest_slot,
                                &[v],
                                meta.has_ttl().then_some(&[0.0][..]),
                            )
                        };
                        let mut guard_result = ErrorCode::Ok;
                        if let Some((up, ranges, capture)) = guard_frame {
                            if set_result == ErrorCode::Ok {
                                apply_planned_row(&guarded, state, key, up);
                                guard_result =
                                    guard.stamp(state, &guarded, up.pos, cols, child_idx);
                                if guard_result == ErrorCode::Ok {
                                    state[guard_flags as usize] |= if up.is_new {
                                        ChangeFlag::INSERTED
                                    } else {
                                        ChangeFlag::UPDATED
                                    };
                                }
                            }
                            // Finished even when the set refused: nothing of the guard
                            // changed, so the frame journals nothing.
                            self.undo
                                .finish_capture(delta_mode, state, &ranges, capture);
                        }
                        if set_result != ErrorCode::Ok {
                            return signal_growth(dest_slot, set_result);
                        }
                        if guard_result != ErrorCode::Ok {
                            return guard_result as u32;
                        }
                    }
                }
                //#endregion reduce-typed-state.scatter-element-guarded-exec

                // LIST_APPEND (0x84)
                Opcode::ListAppend => {
                    let (slot, val_col) = (operand!(r.index()), operand!(r.index()));
                    bpc = r.pos();
                    if slot >= state_num_slots(state)
                        || SlotMetaView::read(state, slot).slot_type() != SlotType::OrderedList
                    {
                        return INVALID_PROGRAM;
                    }

                    // Read the repurposed ORDERED_LIST metadata fields directly.
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

                    if !matches!(elem_size, 1 | 4 | 8) {
                        return INVALID_STATE;
                    }
                    let source_width = if elem_size == 8 { 8usize } else { 4usize };
                    let Some(source_offset) = (child_idx as usize).checked_mul(source_width) else {
                        return ErrorCode::ColumnUnderrun as u32;
                    };
                    let Some(source) = col_at(cols, val_col as usize)
                        .get(source_offset..)
                        .and_then(|cells| cells.get(..source_width))
                    else {
                        return ErrorCode::ColumnUnderrun as u32;
                    };
                    let mut cell = [0u8; 8];
                    cell[..source_width].copy_from_slice(source);
                    let Some(write_off) = count
                        .checked_mul(elem_size)
                        .and_then(|offset| slot_offset.checked_add(offset))
                    else {
                        return INVALID_STATE;
                    };
                    let payload_ranges = [
                        (write_off, elem_size),
                        (meta_base + SlotMetaOffset::SIZE, 4),
                        (meta_base + SlotMetaOffset::CHANGE_FLAGS, 1),
                    ];
                    let capture = match self.undo.begin_capture(state, &payload_ranges) {
                        Ok(capture) => capture,
                        Err(error) => return error as u32,
                    };
                    if elem_size == 1 {
                        state[write_off as usize] = u8::from(
                            u32::from_le_bytes(cell[..4].try_into().expect("four-byte cell")) != 0,
                        );
                    } else {
                        state[write_off as usize..(write_off + elem_size) as usize]
                            .copy_from_slice(&cell[..elem_size as usize]);
                    }

                    count += 1;
                    bytes::write_u32(state, meta_base + SlotMetaOffset::SIZE, count);
                    state[(meta_base + SlotMetaOffset::CHANGE_FLAGS) as usize] |=
                        ChangeFlag::INSERTED;
                    self.undo
                        .finish_capture(delta_mode, state, &payload_ranges, capture);
                }

                // LIST_APPEND_STRUCT (0x85)
                Opcode::ListAppendStruct => {
                    let slot = operand!(r.index());
                    let num_vals = usize::from(operand!(r.byte()));
                    let vals = &mut self.element_list_vals;
                    if vals.decode_into(&mut r, num_vals, 0).is_none() {
                        return INVALID_PROGRAM;
                    }
                    bpc = r.pos();
                    let (vc, fi) = (vals.cols(), vals.fields());
                    if slot >= state_num_slots(state)
                        || SlotMetaView::read(state, slot).slot_type() != SlotType::OrderedList
                    {
                        return INVALID_PROGRAM;
                    }

                    let meta_base = slot_meta_base(slot);
                    let slot_offset = bytes::read_u32(state, meta_base + SlotMetaOffset::OFFSET);
                    let capacity = bytes::read_u32(state, meta_base + SlotMetaOffset::CAPACITY);
                    let mut count = bytes::read_u32(state, meta_base + SlotMetaOffset::SIZE);
                    let num_fields = state[(meta_base + SlotMetaOffset::AGG_TYPE) as usize];
                    let bitset_bytes = u32::from(
                        state[(meta_base + SlotMetaOffset::STRUCT_BITSET_BYTES) as usize],
                    );
                    let row_size = u32::from(bytes::read_u16(
                        state,
                        meta_base + SlotMetaOffset::TTL_SECONDS,
                    ));
                    if let Err(error) = validate_scalar_fields(
                        state,
                        slot_offset,
                        num_fields,
                        vc,
                        fi,
                        cols,
                        child_idx,
                    ) {
                        return error as u32;
                    }

                    if count >= capacity {
                        NEEDS_GROWTH_SLOT.store(slot, Ordering::Relaxed);
                        return NEEDS_GROWTH;
                    }

                    let descriptor_size = align8(u32::from(num_fields));
                    let rows_base = slot_offset + descriptor_size;
                    let Some(row_off) = count
                        .checked_mul(row_size)
                        .and_then(|offset| rows_base.checked_add(offset))
                    else {
                        return INVALID_STATE;
                    };
                    let payload_ranges = [
                        (row_off, row_size),
                        (meta_base + SlotMetaOffset::SIZE, 4),
                        (meta_base + SlotMetaOffset::CHANGE_FLAGS, 1),
                    ];
                    let capture = match self.undo.begin_capture(state, &payload_ranges) {
                        Ok(capture) => capture,
                        Err(error) => return error as u32,
                    };

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
                            // Array fields in ordered-list rows are unsupported.
                            StructFieldType::ArrayU32
                            | StructFieldType::ArrayI64
                            | StructFieldType::ArrayF64
                            | StructFieldType::ArrayString
                            | StructFieldType::ArrayBool => {}
                            StructFieldType::Vo => return INVALID_PROGRAM,
                        }
                    }

                    count += 1;
                    bytes::write_u32(state, meta_base + SlotMetaOffset::SIZE, count);
                    state[(meta_base + SlotMetaOffset::CHANGE_FLAGS) as usize] |=
                        ChangeFlag::INSERTED;
                    self.undo
                        .finish_capture(delta_mode, state, &payload_ranges, capture);
                }

                // FLAT_MAP (0xE1)
                Opcode::FlatMap => {
                    let offsets_col = operand!(r.index());
                    let inner_parent_ts_col = operand!(r.index());
                    // A widened FLAT_MAP carries its widened inner body
                    // behind a u32 length.
                    let Ok(inner_body_len) = usize::try_from(operand!(r.u32())) else {
                        return INVALID_PROGRAM;
                    };
                    let inner_body = operand!(r.bytes(inner_body_len));
                    bpc = r.pos();

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
                            live.as_deref_mut(),
                        );
                        if result != OK {
                            return result;
                        }
                    }
                }

                // NESTED_SET_INSERT (0x90)
                Opcode::NestedSetInsert => {
                    let (slot, outer_key_col, elem_col) = (
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                    );
                    bpc = r.pos();
                    let meta = SlotMetaView::read(state, slot);
                    let plan = match nested::prepare_nested_set_insert(
                        state,
                        &meta,
                        cell_u32(cols, outer_key_col, child_idx),
                        cell_u32(cols, elem_col, child_idx),
                        &mut self.nested_scratch,
                    ) {
                        Ok(plan) => plan,
                        Err(error) => return signal_growth(slot, error),
                    };
                    let capture = match self.undo.begin_capture(plan.state(), plan.ranges()) {
                        Ok(capture) => capture,
                        Err(error) => return error as u32,
                    };
                    let (state, ranges) = plan.apply();
                    self.undo.finish_capture(delta_mode, state, ranges, capture);
                }

                // NESTED_MAP_UPSERT_LAST (0x92)
                Opcode::NestedMapUpsertLast => {
                    let (slot, outer_key_col, inner_key_col, val_col) = (
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                    );
                    bpc = r.pos();
                    let meta = SlotMetaView::read(state, slot);
                    let plan = match nested::prepare_nested_map_upsert_last(
                        state,
                        &meta,
                        cell_u32(cols, outer_key_col, child_idx),
                        cell_u32(cols, inner_key_col, child_idx),
                        cell_u32(cols, val_col, child_idx),
                        &mut self.nested_scratch,
                    ) {
                        Ok(plan) => plan,
                        Err(error) => return signal_growth(slot, error),
                    };
                    let capture = match self.undo.begin_capture(plan.state(), plan.ranges()) {
                        Ok(capture) => capture,
                        Err(error) => return error as u32,
                    };
                    let (state, ranges) = plan.apply();
                    self.undo.finish_capture(delta_mode, state, ranges, capture);
                }

                // NESTED_AGG_UPDATE (0x95)
                Opcode::NestedAggUpdate => {
                    let (slot, outer_key_col, val_col) = (
                        operand!(r.index()),
                        operand!(r.index()),
                        operand!(r.index()),
                    );
                    bpc = r.pos();
                    let meta = SlotMetaView::read(state, slot);
                    let plan = match nested::prepare_nested_agg_update(
                        state,
                        &meta,
                        cell_u32(cols, outer_key_col, child_idx),
                        cell_f64(cols, val_col, child_idx).to_bits(),
                        &mut self.nested_scratch,
                    ) {
                        Ok(plan) => plan,
                        Err(error) => return signal_growth(slot, error),
                    };
                    let capture = match self.undo.begin_capture(plan.state(), plan.ranges()) {
                        Ok(capture) => capture,
                        Err(error) => return error as u32,
                    };
                    let (state, ranges) = plan.apply();
                    self.undo.finish_capture(delta_mode, state, ranges, capture);
                }

                _ => return INVALID_PROGRAM,
            }
        }
        OK
    }

    // =========================================================================
    // Undo-log surface

    /// Grow only a committed image. The owner of a speculative window must
    /// roll back and end that window, grow, then retry the full window.
    pub fn grow_state(
        &mut self,
        old_state: &[u8],
        new_state: &mut [u8],
        slot: u32,
    ) -> Result<(), ErrorCode> {
        if self.undo.enabled {
            return Err(ErrorCode::NeedsGrowth);
        }
        state_init::grow_state(old_state, new_state, slot)
    }

    /// Begin an inactive speculation window over an admitted image span.
    /// An active window must be ended first; refusal changes no journal state.
    ///
    /// ```
    /// use columine_types::types::ErrorCode;
    /// let mut vm = columine_vm::vm::Vm::default();
    /// let before = vm.undo_status();
    /// assert_eq!(vm.undo_enable(&[]), Err(ErrorCode::InvalidState));
    /// assert_eq!(vm.undo_status(), before);
    /// ```
    pub fn undo_enable(&mut self, state: &[u8]) -> Result<(), ErrorCode> {
        if self.undo.enabled {
            return Err(ErrorCode::SpeculationActive);
        }
        let state_size = u32::try_from(state.len()).map_err(|_| ErrorCode::InvalidState)?;
        let (slots, _) = state_init::state_geometry(state)?;
        let u = &mut self.undo;
        u.enabled = true;
        u.entries.clear();
        u.redo.clear();
        u.redo_active = false;
        u.delta_count = 0;
        u.overflow = false;
        u.export_start = 0;
        u.export_count = 0;
        u.export_overflow = false;
        u.generation = next_generation();
        u.capture_depth = 0;
        u.reserved_entries = 0;
        u.capture_scratch.clear();
        u.capture_ranges.clear();
        u.ttl_batch_scratch.reset();
        u.erase_scratch.ranges.clear();
        u.saved_change_flags.reserve(slots as usize + 1);
        u.saved_change_flags.clear();
        u.state_size = state_size;
        Ok(())
    }

    /// Return the current undo checkpoint position.
    pub fn undo_checkpoint(&self) -> u32 {
        self.undo.count()
    }

    /// Roll back to a checkpoint, restoring the state and change flags.
    pub fn undo_rollback(
        &mut self,
        state: &mut [u8],
        checkpoint_pos: u32,
    ) -> Result<u32, ErrorCode> {
        let size = u32::try_from(state.len()).map_err(|_| ErrorCode::InvalidState)?;
        let u = &mut self.undo;
        if checkpoint_pos > u.count() || (u.enabled && u.state_size != size) {
            return Err(ErrorCode::InvalidState);
        }
        let mut i = u.count();
        while i > checkpoint_pos {
            i -= 1;
            let entry = u.entries[i as usize];
            rollback_entry(state, &entry)?;
        }
        let u = &mut self.undo;
        u.entries.truncate(checkpoint_pos as usize);
        u.redo.truncate(checkpoint_pos as usize);
        if u.delta_count > u.count() {
            u.delta_count = u.count();
        }
        u.export_start = u.export_start.min(u.count());
        u.export_count = u.export_count.min(u.count() - u.export_start);
        Ok(size)
    }

    /// End the speculative window and retain only reusable journal storage.
    pub fn undo_commit(&mut self) {
        let u = &mut self.undo;
        u.capture_depth = 0;
        u.reserved_entries = 0;
        u.capture_scratch.clear();
        u.capture_ranges.clear();
        u.ttl_batch_scratch.reset();
        u.generation = next_generation();
        u.entries.clear();
        u.redo.clear();
        u.redo_active = false;
        u.delta_count = 0;
        u.overflow = false;
        u.export_start = 0;
        u.export_count = 0;
        u.export_overflow = false;
        u.saved_change_flags.clear();
        u.enabled = false;
    }

    /// Return whether delta export overflowed.
    pub fn undo_has_overflow(&self) -> bool {
        self.undo.overflow
    }

    /// Observe a journal without being able to reset its live window or refusal.
    ///
    /// External consumers cannot disable logging:
    /// ```compile_fail,E0616
    /// let mut vm = columine_vm::vm::Vm::default();
    /// vm.undo.enabled = false;
    /// ```
    ///
    /// They cannot clear an overflow:
    /// ```compile_fail,E0616
    /// let mut vm = columine_vm::vm::Vm::default();
    /// vm.undo.overflow = false;
    /// ```
    ///
    /// They cannot change the journal's image extent:
    /// ```compile_fail,E0616
    /// let mut vm = columine_vm::vm::Vm::default();
    /// vm.undo.state_size = 0;
    /// ```
    pub fn undo_status(&self) -> UndoStatus {
        UndoStatus {
            enabled: self.undo.enabled,
            overflow: self.undo.overflow,
            checkpoint: self.undo.count(),
            delta_count: self.undo.delta_count(),
        }
    }

    /// Observe lazy redo admission without exposing mutable journal storage.
    pub fn undo_redo_lane_status(&self) -> RedoLaneStatus {
        RedoLaneStatus {
            active: self.undo.redo_active,
            entries: self.undo.redo.len(),
            capacity: self.undo.redo.capacity(),
        }
    }

    // =========================================================================
    // Delta export and apply
    // =========================================================================

    /// Export exactly the requested complete paired range. Refuse an invalid
    /// range or any undo-only record before changing the previous export.
    pub fn delta_export_segment(&mut self, from_pos: u32, to_pos: u32) -> Result<u32, ErrorCode> {
        let u = &mut self.undo;
        if from_pos > to_pos || to_pos > u.count() {
            return Err(ErrorCode::InvalidState);
        }
        if from_pos != to_pos {
            if !u.redo_active {
                return Err(ErrorCode::DeltaLaneMismatch);
            }
            let redo = u
                .redo
                .get(from_pos as usize..to_pos as usize)
                .ok_or(ErrorCode::InvalidState)?;
            if redo.iter().any(Option::is_none) {
                return Err(ErrorCode::DeltaLaneMismatch);
            }
        }
        u.export_start = from_pos;
        u.export_count = to_pos - from_pos;
        u.export_overflow = u.overflow;
        Ok(u.export_count)
    }

    /// The exported undo lane as serialized 24-byte entries (bindings expose
    /// the raw pointer; core code returns owned bytes).
    pub fn delta_export_undo_bytes(&self) -> Vec<u8> {
        serialize_entries(
            self.undo
                .entries
                .iter()
                .skip(self.undo.export_start as usize)
                .copied()
                .map(Some),
            self.undo.export_count,
        )
    }

    /// The exported redo lane.
    pub fn delta_export_redo_bytes(&self) -> Vec<u8> {
        serialize_entries(
            self.undo
                .redo
                .iter()
                .skip(self.undo.export_start as usize)
                .copied(),
            self.undo.export_count,
        )
    }

    /// Return the byte length of the exported delta segment.
    pub fn delta_export_len_bytes(&self) -> u32 {
        self.undo.export_count * FLAT_UNDO_ENTRY_SIZE
    }

    /// Return whether delta export overflowed.
    pub fn delta_export_overflow(&self) -> bool {
        self.undo.export_overflow
    }

    /// Validate a serialized delta before mutation. Physical entries cannot
    /// address outside the image that owns this stable-layout segment.
    pub fn delta_required_state_size(
        segment: &[u8],
        entry_size: u32,
        initial_size: u32,
    ) -> Result<u32, ErrorCode> {
        if entry_size != FLAT_UNDO_ENTRY_SIZE
            || !segment.len().is_multiple_of(FLAT_UNDO_ENTRY_SIZE as usize)
        {
            return Err(ErrorCode::InvalidState);
        }
        for row in segment.as_chunks::<{ FLAT_UNDO_ENTRY_SIZE as usize }>().0 {
            if row.iter().all(|byte| *byte == 0) {
                continue;
            }
            let entry = FlatUndoEntry::read_from(row).ok_or(ErrorCode::InvalidState)?;
            if entry.op == FlatUndoOp::StateBytes
                && (!(1..=8).contains(&entry.pad1)
                    || entry
                        .key
                        .checked_add(u32::from(entry.pad1))
                        .is_none_or(|end| end > initial_size))
            {
                return Err(ErrorCode::InvalidState);
            }
        }
        Ok(initial_size)
    }

    /// Reverse-apply an undo segment only after its complete preflight.
    pub fn delta_apply_rollback_segment(
        &mut self,
        state: &mut [u8],
        undo_segment: &[u8],
        entry_size: u32,
    ) -> Result<u32, ErrorCode> {
        let size = u32::try_from(state.len()).map_err(|_| ErrorCode::InvalidState)?;
        Self::delta_required_state_size(undo_segment, entry_size, size)?;
        let count = undo_segment.len() / FLAT_UNDO_ENTRY_SIZE as usize;
        for i in (0..count).rev() {
            let row = &undo_segment
                [i * FLAT_UNDO_ENTRY_SIZE as usize..(i + 1) * FLAT_UNDO_ENTRY_SIZE as usize];
            if row.iter().all(|byte| *byte == 0) {
                continue;
            }
            let entry = parse_entry(undo_segment, i)?;
            rollback_entry(state, &entry)?;
        }
        Ok(size)
    }

    /// Forward-apply a redo segment; entries move the state toward the target.
    pub fn delta_apply_rollforward_segment(
        &mut self,
        state: &mut [u8],
        redo_segment: &[u8],
        entry_size: u32,
    ) -> Result<u32, ErrorCode> {
        let size = u32::try_from(state.len()).map_err(|_| ErrorCode::InvalidState)?;
        Self::delta_required_state_size(redo_segment, entry_size, size)?;
        let count = redo_segment.len() / FLAT_UNDO_ENTRY_SIZE as usize;
        for i in 0..count {
            let row = &redo_segment
                [i * FLAT_UNDO_ENTRY_SIZE as usize..(i + 1) * FLAT_UNDO_ENTRY_SIZE as usize];
            if row.iter().all(|byte| *byte == 0) {
                continue;
            }
            let entry = parse_entry(redo_segment, i)?;
            rollback_entry(state, &entry)?;
        }
        Ok(size)
    }

    /// Return the serialized size of one delta entry.
    pub const fn delta_export_entry_size() -> u32 {
        FLAT_UNDO_ENTRY_SIZE
    }
}

fn serialize_entries(entries: impl Iterator<Item = Option<FlatUndoEntry>>, count: u32) -> Vec<u8> {
    let mut out = vec![0u8; (count * FLAT_UNDO_ENTRY_SIZE) as usize];
    for (index, entry) in entries.take(count as usize).enumerate() {
        if let Some(entry) = entry {
            let mut encoded = [0u8; FLAT_UNDO_ENTRY_SIZE as usize];
            entry.write_to(&mut encoded);
            out[index * FLAT_UNDO_ENTRY_SIZE as usize..(index + 1) * FLAT_UNDO_ENTRY_SIZE as usize]
                .copy_from_slice(&encoded);
        }
    }
    out
}

fn parse_entry(segment: &[u8], i: usize) -> Result<FlatUndoEntry, ErrorCode> {
    let start = i
        .checked_mul(FLAT_UNDO_ENTRY_SIZE as usize)
        .ok_or(ErrorCode::InvalidState)?;
    let encoded = segment
        .get(start..)
        .and_then(|row| row.first_chunk::<{ FLAT_UNDO_ENTRY_SIZE as usize }>())
        .ok_or(ErrorCode::InvalidState)?;
    FlatUndoEntry::read_from(encoded).ok_or(ErrorCode::InvalidState)
}

// =============================================================================
// Read and iteration exports
// =============================================================================

/// The ascending cursor rule shared by every reader iterator. `current` lies in
/// `0..=capacity`; `capacity` itself is the terminal end and is returned as a
/// success. A position past it is a corrupted cursor, refused as
/// [`ErrorCode::InvalidState`]. `step` yields the successor of a cursor that is
/// still below the end, from `current + 1`.
fn cursor_step(
    current: u32,
    capacity: u32,
    step: impl FnOnce(u32) -> u32,
) -> Result<u32, ErrorCode> {
    if current > capacity {
        return Err(ErrorCode::InvalidState);
    }
    if current == capacity {
        return Ok(capacity);
    }
    Ok(step(current + 1))
}

/// Whether a key cell holds data; `EMPTY_KEY` and `TOMBSTONE` mark dead cells.
fn is_live_key(key: u32) -> bool {
    key != EMPTY_KEY && key != TOMBSTONE
}

/// The key of live cell `pos` in a `capacity`-cell key lane. The iteration end,
/// any position past it, and a dead cell are [`ErrorCode::InvalidState`].
fn live_key_at(state: &[u8], lane: u32, capacity: u32, pos: u32) -> Result<u32, ErrorCode> {
    if pos >= capacity {
        return Err(ErrorCode::InvalidState);
    }
    let key = bytes::read_u32(state, lane + pos * 4);
    if is_live_key(key) {
        Ok(key)
    } else {
        Err(ErrorCode::InvalidState)
    }
}

/// A bound HASHMAP slot. It borrows the state image immutably for its whole life.
///
/// [`CheckedMap::bind`] validates the image geometry once through
/// [`state_init::bind_slot`]. Every method then reuses the bound table's probe and
/// live-cell walk on those same bytes, so nothing is copied or decoded.
#[derive(Clone, Copy, Debug)]
pub struct CheckedMap<'a> {
    state: &'a [u8],
    keys: u32,
    values: u32,
    capacity: u32,
}

impl<'a> CheckedMap<'a> {
    /// Bind HASHMAP `slot`. A slot index past the state's slot count, or a slot of
    /// another kind, is [`ErrorCode::InvalidSlot`]. Malformed geometry is
    /// [`ErrorCode::InvalidState`]. Absence is an `Ok` answer from [`Self::get`].
    pub fn bind(state: &'a [u8], slot: u32) -> Result<Self, ErrorCode> {
        let meta = state_init::bind_slot(state, slot)?;
        if meta.slot_type() != SlotType::HashMap {
            return Err(ErrorCode::InvalidSlot);
        }
        Ok(Self {
            state,
            keys: meta.offset,
            values: meta.offset + meta.capacity * 4,
            capacity: meta.capacity,
        })
    }

    /// The element capacity, which is also the ascending iteration end.
    pub fn capacity(&self) -> u32 {
        self.capacity
    }

    /// The value mapped to `key`, or `None` when absent. A mapped `u32::MAX` is
    /// `Some(u32::MAX)`, never absence. Sentinel keys are never present.
    pub fn get(&self, key: u32) -> Option<u32> {
        hash_table::find_key(self.state, self.keys, self.capacity, key)
            .map(|pos| bytes::read_u32(self.state, self.values + pos * 4))
    }

    /// The first live cell, or [`Self::capacity`] when the map is empty.
    pub fn iter_start(&self) -> u32 {
        hash_table::next_live_key(self.state, self.keys, self.capacity, 0)
    }

    /// The next live cell after `current`; `capacity` is the terminal value.
    /// `current > capacity` is [`ErrorCode::InvalidState`].
    pub fn iter_next(&self, current: u32) -> Result<u32, ErrorCode> {
        cursor_step(current, self.capacity, |from| {
            hash_table::next_live_key(self.state, self.keys, self.capacity, from)
        })
    }

    /// The key and value of live cell `pos`, packed as `(value << 32) | key`.
    /// A dead cell or `pos >= capacity` is [`ErrorCode::InvalidState`].
    pub fn iter_get(&self, pos: u32) -> Result<u64, ErrorCode> {
        let key = live_key_at(self.state, self.keys, self.capacity, pos)?;
        let value = bytes::read_u32(self.state, self.values + pos * 4);
        Ok((u64::from(value) << 32) | u64::from(key))
    }
}

/// A bound HASHSET or BITMAP slot. It borrows the state image immutably for its
/// whole life.
///
/// `capacity` is the slot's element capacity and the ascending iteration end. A
/// HASHSET walks its live key cells. A BITMAP walks ranks `0..cardinality` over
/// the borrowed native image: `bind` validates that image once, so membership and
/// rank reads never meet a malformed payload, and `false` always means absence.
#[derive(Clone, Copy, Debug)]
pub struct CheckedSet<'a> {
    state: &'a [u8],
    capacity: u32,
    members: SetMembers<'a>,
}

#[derive(Clone, Copy, Debug)]
enum SetMembers<'a> {
    /// HASHSET: the key lane at the slot offset.
    Keys { lane: u32 },
    /// BITMAP: the borrowed image (`None` is the empty set) and the cardinality
    /// the slot records, which `bind` has checked against that image.
    Ranks {
        image: Option<BitmosaicView<'a>>,
        cardinality: u32,
    },
}

impl<'a> CheckedSet<'a> {
    /// Bind HASHSET or BITMAP `slot`. A slot index past the slot count, or a slot
    /// of another kind, is [`ErrorCode::InvalidSlot`]. For a BITMAP, a malformed
    /// image, a payload cardinality that differs from the slot size, or a
    /// cardinality above the element capacity is [`ErrorCode::InvalidState`],
    /// never an empty set.
    pub fn bind(state: &'a [u8], slot: u32) -> Result<Self, ErrorCode> {
        let meta = state_init::bind_slot(state, slot)?;
        let members = match meta.slot_type() {
            SlotType::HashSet => SetMembers::Keys { lane: meta.offset },
            SlotType::Bitmap => {
                let bytes = get_bitmap_storage(&meta).serialized_data(state)?;
                let image = BitmosaicView::open_verified(bytes).ok_or(ErrorCode::InvalidState)?;
                let cardinality = meta.size(state);
                if image.len() != u64::from(cardinality) || cardinality > meta.capacity {
                    return Err(ErrorCode::InvalidState);
                }
                SetMembers::Ranks {
                    image: Some(image),
                    cardinality,
                }
            }
            _ => return Err(ErrorCode::InvalidSlot),
        };
        Ok(Self {
            state,
            capacity: meta.capacity,
            members,
        })
    }

    /// The element capacity, which is also the ascending iteration end.
    pub fn capacity(&self) -> u32 {
        self.capacity
    }

    /// Whether `elem` is a member. A malformed BITMAP was refused at [`Self::bind`],
    /// so `false` is always a genuine absence.
    pub fn contains(&self, elem: u32) -> bool {
        match self.members {
            SetMembers::Keys { lane } => {
                hash_table::find_key(self.state, lane, self.capacity, elem).is_some()
            }
            SetMembers::Ranks { image, .. } => image.is_some_and(|view| view.contains(elem)),
        }
    }

    /// Rank `0` when the set is non-empty, otherwise [`Self::capacity`]. For a
    /// HASHSET this is the first live cell.
    pub fn iter_start(&self) -> u32 {
        match self.members {
            SetMembers::Keys { lane } => {
                hash_table::next_live_key(self.state, lane, self.capacity, 0)
            }
            SetMembers::Ranks { cardinality, .. } => {
                if cardinality == 0 {
                    self.capacity
                } else {
                    0
                }
            }
        }
    }

    /// The next position after `current`; `capacity` is the terminal value. For a
    /// BITMAP, a rank in `cardinality..capacity` is [`ErrorCode::InvalidState`], as
    /// is any position past `capacity`.
    pub fn iter_next(&self, current: u32) -> Result<u32, ErrorCode> {
        match self.members {
            SetMembers::Keys { lane } => cursor_step(current, self.capacity, |from| {
                hash_table::next_live_key(self.state, lane, self.capacity, from)
            }),
            SetMembers::Ranks { cardinality, .. } => {
                if current >= cardinality && current < self.capacity {
                    return Err(ErrorCode::InvalidState);
                }
                cursor_step(current, self.capacity, |next| {
                    if next < cardinality {
                        next
                    } else {
                        self.capacity
                    }
                })
            }
        }
    }

    /// The element of live HASHSET cell or BITMAP rank `pos`. A dead cell, a rank
    /// at or past the cardinality, or a HASHSET position at or past `capacity` is
    /// [`ErrorCode::InvalidState`].
    pub fn iter_get(&self, pos: u32) -> Result<u32, ErrorCode> {
        match self.members {
            SetMembers::Keys { lane } => live_key_at(self.state, lane, self.capacity, pos),
            SetMembers::Ranks { image, cardinality } => {
                if pos >= cardinality {
                    return Err(ErrorCode::InvalidState);
                }
                image
                    .and_then(|view| view.select(u64::from(pos)))
                    .ok_or(ErrorCode::InvalidState)
            }
        }
    }
}

/// A bound unary STRUCT_MAP slot. It borrows the state image immutably for its
/// whole life.
///
/// [`CheckedStructMap::bind`] validates the slot geometry and row layout once
/// through [`state_init::bind_slot`]. `row` and the iterators read the same keys
/// and rows through the bound [`StructMapSlot`]. Row offsets are absolute.
#[derive(Clone, Copy, Debug)]
pub struct CheckedStructMap<'a> {
    state: &'a [u8],
    view: StructMapSlot,
}

impl<'a> CheckedStructMap<'a> {
    /// Bind STRUCT_MAP `slot`. A slot index past the slot count, or a slot of
    /// another kind, is [`ErrorCode::InvalidSlot`]. Malformed geometry or a row
    /// layout that disagrees with the field descriptor is [`ErrorCode::InvalidState`].
    pub fn bind(state: &'a [u8], slot: u32) -> Result<Self, ErrorCode> {
        if state_init::bind_slot(state, slot)?.slot_type() != SlotType::StructMap {
            return Err(ErrorCode::InvalidSlot);
        }
        Ok(Self {
            state,
            view: StructMapSlot::bind(state, slot),
        })
    }

    /// The element capacity, which is also the ascending iteration end.
    pub fn capacity(&self) -> u32 {
        self.view.capacity
    }

    /// The absolute offset of the row stored under `key`, or `None` when absent.
    pub fn row(&self, key: u32) -> Option<u32> {
        self.view
            .find(self.state, key)
            .map(|pos| self.view.row_off(pos))
    }

    /// The first live cell, or [`Self::capacity`] when the map is empty.
    pub fn iter_start(&self) -> u32 {
        hash_table::next_live_key(self.state, self.view.keys_off, self.view.capacity, 0)
    }

    /// The next live cell after `current`; `capacity` is the terminal value.
    /// `current > capacity` is [`ErrorCode::InvalidState`].
    pub fn iter_next(&self, current: u32) -> Result<u32, ErrorCode> {
        cursor_step(current, self.view.capacity, |from| {
            hash_table::next_live_key(self.state, self.view.keys_off, self.view.capacity, from)
        })
    }

    /// The key of live cell `pos`. A dead cell or `pos >= capacity` is
    /// [`ErrorCode::InvalidState`].
    pub fn iter_key(&self, pos: u32) -> Result<u32, ErrorCode> {
        live_key_at(self.state, self.view.keys_off, self.view.capacity, pos)
    }
}

/// A bound exact-pair STRUCT_MAP2 slot. It borrows the state image immutably for
/// its whole life.
///
/// Lane one owns the EMPTY/TOMBSTONE sentinel state and lane two carries every
/// u32 value, so the liveness of a cell is decided by lane one alone.
#[derive(Clone, Copy, Debug)]
pub struct CheckedStructMap2<'a> {
    state: &'a [u8],
    view: StructMap2Slot,
}

impl<'a> CheckedStructMap2<'a> {
    /// Bind STRUCT_MAP2 `slot`, with the same refusals as [`CheckedStructMap::bind`].
    pub fn bind(state: &'a [u8], slot: u32) -> Result<Self, ErrorCode> {
        if state_init::bind_slot(state, slot)?.slot_type() != SlotType::StructMap2 {
            return Err(ErrorCode::InvalidSlot);
        }
        Ok(Self {
            state,
            view: StructMap2Slot::bind(state, slot),
        })
    }

    /// The element capacity, which is also the ascending iteration end.
    pub fn capacity(&self) -> u32 {
        self.view.capacity
    }

    /// The absolute offset of the row stored under the exact pair, or `None`.
    pub fn row(&self, key1: u32, key2: u32) -> Option<u32> {
        self.view
            .find(self.state, key1, key2)
            .map(|pos| self.view.row_off(pos))
    }

    /// The first live cell, or [`Self::capacity`] when the map is empty.
    pub fn iter_start(&self) -> u32 {
        hash_table::next_live_key(self.state, self.view.keys1_off, self.view.capacity, 0)
    }

    /// The next live cell after `current`; `capacity` is the terminal value.
    /// `current > capacity` is [`ErrorCode::InvalidState`].
    pub fn iter_next(&self, current: u32) -> Result<u32, ErrorCode> {
        cursor_step(current, self.view.capacity, |from| {
            hash_table::next_live_key(self.state, self.view.keys1_off, self.view.capacity, from)
        })
    }

    /// The first key of live cell `pos`. A dead cell or `pos >= capacity` is
    /// [`ErrorCode::InvalidState`].
    pub fn iter_key1(&self, pos: u32) -> Result<u32, ErrorCode> {
        live_key_at(self.state, self.view.keys1_off, self.view.capacity, pos)
    }

    /// The second key of live cell `pos`. Liveness comes from the first key, so the
    /// same refusals apply.
    pub fn iter_key2(&self, pos: u32) -> Result<u32, ErrorCode> {
        live_key_at(self.state, self.view.keys1_off, self.view.capacity, pos)?;
        Ok(self.view.key2_at(self.state, pos))
    }

    /// The authoritative row offset for one live iterator position.
    pub fn iter_row(&self, pos: u32) -> Result<u32, ErrorCode> {
        live_key_at(self.state, self.view.keys1_off, self.view.capacity, pos)?;
        Ok(self.view.row_off(pos))
    }
}

#[cfg(test)]
mod tests {
    use super::Vm;
    use crate::state_init::{DEFAULT_ACCEPTED_PROGRAM_MAGICS, calculate_state_size, init_state};
    use columine_types::PROGRAM_MAGIC;
    use columine_types::types::{AggType, Opcode, SlotType};

    /// A state holding one COUNT aggregate slot.
    fn count_state() -> Vec<u8> {
        let init = [
            Opcode::SlotDef as u8,
            0,
            SlotType::Aggregate as u8,
            AggType::Count as u8,
            0,
            Opcode::Halt as u8,
        ];
        let reduce = [Opcode::Halt as u8];
        let mut prog = vec![0u8; 32];
        prog.extend(PROGRAM_MAGIC.to_le_bytes());
        prog.extend([2, 0, 0, 0]);
        prog.extend(1u32.to_le_bytes());
        prog.extend(0u32.to_le_bytes());
        prog.extend(6u32.to_le_bytes());
        prog.extend(1u32.to_le_bytes());
        prog.extend(init);
        prog.extend(reduce);
        let size = calculate_state_size(&prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("admitted");
        let mut state = vec![0u8; usize::try_from(size).expect("size fits usize")];
        init_state(&mut state, &prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("init_state");
        state
    }

    /// Generations are unique across journals: a frame begun in one VM's
    /// window cannot be finished by another VM's window over the same image,
    /// even though both windows opened at the same point.
    #[test]
    #[should_panic(expected = "another undo window")]
    fn a_capture_cannot_finish_in_another_journal() {
        let state = count_state();
        let mut a = Vm::default();
        let mut b = Vm::default();
        a.undo_enable(&state).expect("window a opens");
        b.undo_enable(&state).expect("window b opens");
        let ranges = [(0, 4)];
        let capture = a
            .undo
            .begin_capture(&state, &ranges)
            .expect("capture begins");
        // B's own open frame gives it the depth A's frame carries, so only the
        // generation tells the two windows apart.
        let _open_in_b = b.undo.begin_capture(&state, &[]).expect("b opens a frame");
        b.undo.finish_capture(false, &state, &ranges, capture);
    }
}
