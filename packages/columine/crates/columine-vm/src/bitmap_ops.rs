//! The BITMAP slot: an AXR1 string (the `axroar` canonical bitmap) patched
//! where it lies, read through a zero-copy view.
//!
//! # Slot layout
//!
//! `[serialized_len: u32][reserved: u32 = 0][payload: bitmap_payload_capacity bytes]`
//!
//! Slot data offsets are 8-aligned, so the 8-byte header keeps the payload
//! on the 8-byte boundary axroar's word kernels take their fast path on.
//! `serialized_len == 0` is the empty set and its only form; a non-empty set
//! occupies the first `serialized_len` payload bytes as the FOREST root of
//! the set, and every payload byte past the string is zero. Together those
//! make the slot image a pure function of the set: byte-equal, whatever the
//! history, to a fresh `Axroar::write_forest_into_slice` of the same members
//! (the forest is the mutable root — `axroar::patch` keeps the root arm, and
//! a forest patch touches only the chunks a batch names).
//!
//! # Mutation
//!
//! Every write is one `axroar::patch_witnessed` over the payload: the batch
//! is sorted and deduplicated into VM-owned buffers, the patch re-ladders
//! only the touched chunks and reports per value which adds were new and
//! which removes were held — the bit the edit overwrote, so the undo
//! records and TTL entries cost no second probe — and its refusals leave
//! the slot untouched: a `Capacity` refusal becomes
//! `ErrorCode::CapacityExceeded`, which the slot-growth retry answers by
//! doubling the slot and replaying. Nothing on the steady path allocates:
//! the `PatchScratch`, the batch buffers and the outcome planes keep their
//! high-water mark inside [`BitmapEnv`].
//!
//! # Reads
//!
//! Membership, cardinality, rank/select, iteration and intersection open an
//! `AxroarView` over the slot bytes (or over caller-supplied bytes for the
//! decision-side `*_serialized` queries) and answer from the string; no read
//! materialises the set.
use crate::bytes;
use crate::hooks::{MutationRecord, VmHooks};
use crate::meta::SlotMetaView;
use axroar::{
    AndNotRange, AndRange, Axroar, AxroarView, BatchOutcome, OrRange, PatchError, PatchReport,
    PatchScratch, Range, XorRange, patch_witnessed,
};
use columine_types::types::{
    BITMAP_BASE_BYTES, BITMAP_BYTES_PER_CAPACITY, BITMAP_SERIALIZED_LEN_BYTES, ChangeFlag,
    EMPTY_KEY, ErrorCode, TOMBSTONE,
};

/// Canonical payload-capacity formula used by allocation, grow-copy, and
/// readers. Keeping one formula prevents those paths from disagreeing.
pub const fn bitmap_payload_capacity(slot_capacity: u32) -> u32 {
    slot_capacity * BITMAP_BYTES_PER_CAPACITY + BITMAP_BASE_BYTES
}

/// Bitmap storage view carrying offsets into the state buffer rather than
/// references into it.
#[derive(Clone, Copy, Debug)]
pub struct BitmapStorage {
    /// Offset of the `serialized_len: u32` field (== the slot data offset).
    pub data_offset: u32,
    pub payload_capacity: u32,
}

impl BitmapStorage {
    pub fn payload_offset(&self) -> u32 {
        self.data_offset + BITMAP_SERIALIZED_LEN_BYTES
    }

    pub fn serialized_len(&self, state: &[u8]) -> u32 {
        bytes::read_u32(state, self.data_offset)
    }

    pub fn set_serialized_len(&self, state: &mut [u8], len: u32) {
        bytes::write_u32(state, self.data_offset, len);
    }

    /// The AXR1 string, or `None` when the set is empty or the length field
    /// is outside the payload.
    pub fn serialized_data<'a>(&self, state: &'a [u8]) -> Option<&'a [u8]> {
        let len = self.serialized_len(state);
        if len == 0 || len > self.payload_capacity {
            return None;
        }
        let start = self.payload_offset() as usize;
        Some(&state[start..start + len as usize])
    }

    /// The slot's set as a zero-copy view. `Ok(None)` is the empty set;
    /// `Err` a length field or string the payload does not hold.
    pub fn view<'a>(&self, state: &'a [u8]) -> Result<Option<AxroarView<'a>>, ErrorCode> {
        if self.serialized_len(state) == 0 {
            return Ok(None);
        }
        match self.serialized_data(state).and_then(AxroarView::open) {
            Some(view) => Ok(Some(view)),
            None => Err(ErrorCode::InvalidState),
        }
    }
}

/// Build a bitmap storage view from slot metadata.
pub fn get_bitmap_storage(meta: &SlotMetaView) -> BitmapStorage {
    BitmapStorage {
        data_offset: meta.offset,
        payload_capacity: bitmap_payload_capacity(meta.capacity),
    }
}

#[derive(Clone, Copy, Debug)]
enum PendingBitmapMutation {
    Insert { key: u32, timestamp: f64 },
    RefreshTtl { key: u32, timestamp: f64 },
    Remove { key: u32, previous_ts_bits: u64 },
}

/// Observable bitmap operation state: the patch scratch and the batch
/// buffers every write reuses, the decision-side algebra result, and the
/// last diagnostic.
pub struct BitmapEnv {
    /// `g_bitmap_last_error` — diagnostic code readable after a failure.
    pub last_error: u32,
    scratch: PatchScratch,
    /// Staged adds as `elem << 32 | column index`, so one sort orders them by
    /// value and, within a value, by first occurrence.
    staged_adds: Vec<u64>,
    adds: Vec<u32>,
    removes: Vec<u32>,
    /// The witness planes: bit `i` of `adds_new` for `adds[i]`, bit `i` of
    /// `removes_held` for `removes[i]`.
    adds_new: Vec<u64>,
    removes_held: Vec<u64>,
    algebra_result: Vec<u8>,
    pending_mutations: Vec<PendingBitmapMutation>,
    /// The forest string of the empty set: what a `serialized_len == 0` slot
    /// is seeded with so a patch has a string to edit.
    empty_forest: Vec<u8>,
}

impl Default for BitmapEnv {
    fn default() -> Self {
        BitmapEnv {
            last_error: 0,
            scratch: PatchScratch::new(),
            staged_adds: Vec::new(),
            adds: Vec::new(),
            removes: Vec::new(),
            adds_new: Vec::new(),
            removes_held: Vec::new(),
            algebra_result: Vec::new(),
            pending_mutations: Vec::new(),
            empty_forest: Axroar::from_sorted(core::iter::empty()).to_forest_bytes(),
        }
    }
}

impl core::fmt::Debug for BitmapEnv {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BitmapEnv")
            .field("last_error", &self.last_error)
            .field("algebra_result_len", &self.algebra_result.len())
            .finish_non_exhaustive()
    }
}

impl BitmapEnv {
    /// `vm_rbmp_algebra_result_{ptr,len}` equivalent — the bindings stage
    /// exposes the buffer; core code returns the slice.
    pub fn algebra_result(&self) -> &[u8] {
        &self.algebra_result
    }

    /// Whether `adds[i]` of the last successful [`bitmap_patch`] was new.
    fn add_was_new(&self, i: usize) -> bool {
        self.adds_new[i / 64] >> (i % 64) & 1 == 1
    }

    /// Whether `removes[i]` of the last successful [`bitmap_patch`] was held.
    fn remove_was_held(&self, i: usize) -> bool {
        self.removes_held[i / 64] >> (i % 64) & 1 == 1
    }
}

/// Whether a column is already the batch a patch takes: strictly ascending
/// and free of the sentinel keys. Such a column is patched as it is, with
/// no copy, sort or dedup; the sentinels are the two largest `u32`s, so an
/// ascending column can only carry them at its end and the last value
/// decides.
fn is_batch(column: &[u32]) -> bool {
    column.last().is_none_or(|&last| last < TOMBSTONE) && column.is_sorted_by(|a, b| a < b)
}

fn commit_bitmap_mutations(
    env: &mut BitmapEnv,
    hooks: &mut impl VmHooks,
    delta_mode: bool,
    state: &mut [u8],
    meta: &SlotMetaView,
    slot_idx: u8,
) -> ErrorCode {
    for idx in 0..env.pending_mutations.len() {
        match env.pending_mutations[idx] {
            PendingBitmapMutation::Insert { key, timestamp } => {
                if hooks.undo_enabled() {
                    hooks.append_mutation(
                        delta_mode,
                        state,
                        MutationRecord::set_insert(slot_idx, key),
                        MutationRecord::set_delete(slot_idx, key, 0),
                    );
                }
                if meta.has_ttl() {
                    let result = hooks.insert_with_ttl(state, meta, key, timestamp);
                    if result != ErrorCode::Ok {
                        env.pending_mutations.clear();
                        return result;
                    }
                }
            }
            PendingBitmapMutation::RefreshTtl { key, timestamp } => {
                let result = hooks.insert_with_ttl(state, meta, key, timestamp);
                if result != ErrorCode::Ok {
                    env.pending_mutations.clear();
                    return result;
                }
            }
            PendingBitmapMutation::Remove {
                key,
                previous_ts_bits,
            } => {
                if hooks.undo_enabled() {
                    hooks.append_mutation(
                        delta_mode,
                        state,
                        MutationRecord::set_delete(slot_idx, key, previous_ts_bits),
                        MutationRecord::set_insert(slot_idx, key),
                    );
                }
                if meta.has_ttl() {
                    hooks.remove_ttl_entries_for_key(state, meta, key);
                }
            }
        }
    }
    env.pending_mutations.clear();
    ErrorCode::Ok
}

/// Patch the slot's string in place: the set becomes `(old ∪ adds) \ removes`.
/// `adds` and `removes` are strictly ascending. On success the length field
/// and the zero tail are maintained, and `env.add_was_new(i)` /
/// `env.remove_was_held(i)` answer per value what the patch found; a
/// refusal leaves the slot exactly as it was and `env.last_error` names why.
pub fn bitmap_patch(
    env: &mut BitmapEnv,
    state: &mut [u8],
    storage: BitmapStorage,
    adds: &[u32],
    removes: &[u32],
) -> Result<PatchReport, ErrorCode> {
    let old_len = storage.serialized_len(state) as usize;
    let capacity = storage.payload_capacity as usize;
    if old_len > capacity {
        env.last_error = 102;
        return Err(ErrorCode::InvalidState);
    }
    let payload = storage.payload_offset() as usize;
    let slot = &mut state[payload..payload + capacity];
    let seed = env.empty_forest.len();
    if old_len == 0 {
        // The empty set has no string; seed one so the patch has a string
        // to edit. A refusal below zeroes the seed back out.
        slot[..seed].copy_from_slice(&env.empty_forest);
    }
    env.adds_new.resize(adds.len().div_ceil(64), 0);
    env.removes_held.resize(removes.len().div_ceil(64), 0);
    let mut outcome = BatchOutcome::new(&mut env.adds_new, &mut env.removes_held);
    match patch_witnessed(slot, adds, removes, &mut env.scratch, &mut outcome) {
        Ok(report) => {
            let new_len = if report.len == 0 {
                0
            } else {
                report.serialized_len
            };
            // Every payload byte past the string is zero, so the image is a
            // function of the set alone: clear whatever the old string (or
            // the seed) occupied beyond the new one.
            let stale_end = old_len.max(seed).max(report.serialized_len);
            if new_len < stale_end {
                slot[new_len..stale_end].fill(0);
            }
            storage.set_serialized_len(state, new_len as u32);
            Ok(report)
        }
        Err(PatchError::Capacity { .. }) => {
            if old_len == 0 {
                slot[..seed].fill(0);
            }
            env.last_error = 60;
            Err(ErrorCode::CapacityExceeded)
        }
        Err(PatchError::Malformed) => {
            if old_len == 0 {
                slot[..seed].fill(0);
            }
            env.last_error = 102;
            Err(ErrorCode::InvalidState)
        }
        Err(PatchError::Unsorted) => {
            columine_types::die!("bitmap batches are sorted and deduplicated before patching")
        }
    }
}

/// Select the element at `rank` in ascending order.
pub fn bitmap_select(state: &[u8], storage: BitmapStorage, rank: u32) -> Option<u32> {
    let view = AxroarView::open(storage.serialized_data(state)?)?;
    view.select(u64::from(rank))
}

/// Add a batch of elements. Elements already present refresh their TTL;
/// a value repeated within the batch is inserted once, with its first
/// timestamp. When the batch would exceed `meta.capacity` elements, the
/// earliest of the batch that fit are committed and the call returns
/// `CapacityExceeded`; a payload-capacity refusal commits nothing.
#[allow(clippy::too_many_arguments)]
pub fn batch_bitmap_add(
    env: &mut BitmapEnv,
    hooks: &mut impl VmHooks,
    delta_mode: bool,
    state: &mut [u8],
    meta: &SlotMetaView,
    slot_idx: u8,
    elem_col: &[u32],
    ts_col: Option<&[f64]>,
) -> ErrorCode {
    env.last_error = 0;
    env.pending_mutations.clear();
    env.staged_adds.clear();
    let storage = get_bitmap_storage(meta);
    let has_ttl = meta.has_ttl();
    let timestamp_at = |i: usize| -> f64 {
        if has_ttl {
            ts_col.unwrap_or_else(|| columine_types::die!("TTL slot requires a timestamp column"))
                [i]
        } else {
            0.0
        }
    };

    let cardinality = meta.size(state);
    let room = meta.capacity.saturating_sub(cardinality) as usize;

    // A column that is already the batch, and cannot reach the element
    // cap, is patched as it is: its index in the column is its index in
    // the batch.
    if elem_col.len() <= room && is_batch(elem_col) {
        if elem_col.is_empty() {
            return ErrorCode::Ok;
        }
        let report = match bitmap_patch(env, state, storage, elem_col, &[]) {
            Ok(report) => report,
            Err(code) => return code,
        };
        if hooks.undo_enabled() || has_ttl {
            for (i, &key) in elem_col.iter().enumerate() {
                let timestamp = timestamp_at(i);
                if env.add_was_new(i) {
                    env.pending_mutations
                        .push(PendingBitmapMutation::Insert { key, timestamp });
                } else if has_ttl {
                    env.pending_mutations
                        .push(PendingBitmapMutation::RefreshTtl { key, timestamp });
                }
            }
        }
        meta.set_size(state, cardinality + report.added);
        if report.added > 0 {
            meta.set_change_flag(state, ChangeFlag::INSERTED);
        }
        return commit_bitmap_mutations(env, hooks, delta_mode, state, meta, slot_idx);
    }

    // Value order with first occurrence first, so the dedup keeps the
    // earliest column index of a repeated value.
    for (i, &elem) in elem_col.iter().enumerate() {
        if elem == EMPTY_KEY || elem == TOMBSTONE {
            continue;
        }
        env.staged_adds.push((u64::from(elem) << 32) | i as u64);
    }
    env.staged_adds.sort_unstable();
    env.staged_adds
        .dedup_by_key(|staged| (*staged >> 32) as u32);

    let overflow = if env.staged_adds.len() > room {
        // The batch may exceed the element cap: only the values not already
        // held count against it, so this path probes for them and keeps
        // the `room` earliest new values by column index. The common path
        // below never probes — the patch reports what was new.
        let view = match storage.view(state) {
            Ok(view) => view,
            Err(code) => {
                env.last_error = 102;
                return code;
            }
        };
        let mut kept = 0usize;
        env.staged_adds.retain(|staged| {
            let elem = (*staged >> 32) as u32;
            let present = view.is_some_and(|v| v.contains(elem));
            present || {
                kept += 1;
                true
            }
        });
        let overflow = kept > room;
        if overflow {
            let mut admitted = 0usize;
            env.staged_adds
                .sort_unstable_by_key(|staged| *staged as u32);
            env.staged_adds.retain(|staged| {
                let present = view.is_some_and(|v| v.contains((*staged >> 32) as u32));
                present || {
                    admitted += 1;
                    admitted <= room
                }
            });
            env.staged_adds.sort_unstable();
        }
        overflow
    } else {
        false
    };

    let mut added = 0;
    if !env.staged_adds.is_empty() {
        let mut adds = core::mem::take(&mut env.adds);
        adds.clear();
        adds.extend(env.staged_adds.iter().map(|staged| (*staged >> 32) as u32));
        let patched = bitmap_patch(env, state, storage, &adds, &[]);
        env.adds = adds;
        match patched {
            Ok(report) => added = report.added,
            Err(code) => {
                env.pending_mutations.clear();
                return code;
            }
        }
        // The undo records and TTL entries of what the patch found new —
        // only when something consumes them.
        if hooks.undo_enabled() || has_ttl {
            for (i, staged) in env.staged_adds.iter().enumerate() {
                let key = (*staged >> 32) as u32;
                let timestamp = timestamp_at(*staged as u32 as usize);
                if env.add_was_new(i) {
                    env.pending_mutations
                        .push(PendingBitmapMutation::Insert { key, timestamp });
                } else if has_ttl {
                    env.pending_mutations
                        .push(PendingBitmapMutation::RefreshTtl { key, timestamp });
                }
            }
        }
    }

    meta.set_size(state, cardinality + added);
    if added > 0 {
        meta.set_change_flag(state, ChangeFlag::INSERTED);
    }
    let commit_result = commit_bitmap_mutations(env, hooks, delta_mode, state, meta, slot_idx);
    if commit_result != ErrorCode::Ok {
        return commit_result;
    }
    if overflow {
        ErrorCode::CapacityExceeded
    } else {
        ErrorCode::Ok
    }
}

/// Remove a batch of elements; failures leave slot bytes unchanged.
pub fn batch_bitmap_remove(
    env: &mut BitmapEnv,
    hooks: &mut impl VmHooks,
    delta_mode: bool,
    state: &mut [u8],
    meta: &SlotMetaView,
    slot_idx: u8,
    elem_col: &[u32],
) -> ErrorCode {
    env.last_error = 0;
    env.pending_mutations.clear();
    let storage = get_bitmap_storage(meta);
    let cardinality = meta.size(state);
    if cardinality == 0 || elem_col.is_empty() {
        return ErrorCode::Ok;
    }
    // A column that is already the batch is patched as it is; any other is
    // staged, sorted and deduplicated into the retained buffer.
    let mut removes = core::mem::take(&mut env.removes);
    let batch: &[u32] = if is_batch(elem_col) {
        elem_col
    } else {
        removes.clear();
        removes.extend(
            elem_col
                .iter()
                .copied()
                .filter(|&elem| elem != EMPTY_KEY && elem != TOMBSTONE),
        );
        removes.sort_unstable();
        removes.dedup();
        &removes
    };

    let patched = bitmap_patch(env, state, storage, &[], batch);
    let report = match patched {
        Ok(report) => report,
        Err(code) => {
            env.removes = removes;
            return code;
        }
    };
    // The undo records and TTL entries of what the patch found held — only
    // when something consumes them. The eviction timestamp is read after
    // the patch: the eviction index is a separate plane the patch does not
    // touch.
    let journaled = hooks.undo_enabled() || meta.has_ttl();
    for (i, &elem) in batch.iter().enumerate() {
        if !journaled || !env.remove_was_held(i) {
            continue;
        }
        let previous_ts_bits = if meta.has_ttl() {
            hooks
                .latest_eviction_ts(state, meta, elem)
                .unwrap_or_else(|| {
                    columine_types::die!("live TTL bitmap key is missing its eviction entry")
                })
                .to_bits()
        } else {
            0
        };
        env.pending_mutations.push(PendingBitmapMutation::Remove {
            key: elem,
            previous_ts_bits,
        });
    }
    env.removes = removes;
    meta.set_size(state, cardinality - report.removed);
    if report.removed > 0 {
        meta.set_change_flag(state, ChangeFlag::REMOVED);
    }
    commit_bitmap_mutations(env, hooks, delta_mode, state, meta, slot_idx)
}

/// Set-algebra operation applied to a target bitmap.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BitmapAlgebraOp {
    And,
    Or,
    AndNot,
    Xor,
}

/// The right-hand operand of [`batch_bitmap_algebra`]. Naming the operand's
/// home rather than handing over bytes lets the operation read it in place:
/// a slot operand lives in the same `state` the target is written to, and the
/// scratch operand lives in the `env` whose buffers the store path reuses.
#[derive(Clone, Copy, Debug)]
pub enum BitmapSource<'a> {
    /// Another BITMAP slot of the same state buffer.
    Slot(BitmapStorage),
    /// `env.algebra_result` — the image the previous decision-side algebra
    /// left behind.
    Scratch,
    /// A serialized image supplied by the caller.
    Bytes(&'a [u8]),
}

/// Apply in-place set algebra to a target slot. The operation is expressed
/// as the patch that turns the target into the result — the values to add
/// and the values to remove, each computed by a lazy merge of the two
/// views — so the slot is edited only where the result differs from the
/// target. Bulk mutations use one undo snapshot because per-element
/// tracking is impractical.
pub fn batch_bitmap_algebra(
    env: &mut BitmapEnv,
    hooks: &mut impl VmHooks,
    op: BitmapAlgebraOp,
    state: &mut [u8],
    target_meta: &SlotMetaView,
    source: BitmapSource<'_>,
) -> ErrorCode {
    let target_storage = get_bitmap_storage(target_meta);
    let original_size = target_meta.size(state);

    let source_data: Option<&[u8]> = match source {
        BitmapSource::Slot(storage) => storage.serialized_data(state),
        BitmapSource::Scratch => {
            (!env.algebra_result.is_empty()).then_some(&env.algebra_result[..])
        }
        BitmapSource::Bytes(bytes) => (!bytes.is_empty()).then_some(bytes),
    };
    let source_view = match source_data {
        None => None,
        Some(data) => match AxroarView::open(data) {
            Some(view) => Some(view),
            None => {
                env.last_error = 80;
                return ErrorCode::InvalidState;
            }
        },
    };
    let target_view = match target_storage.view(state) {
        Ok(view) => view,
        Err(code) => {
            env.last_error = 102;
            return code;
        }
    };

    let mut adds = core::mem::take(&mut env.adds);
    let mut removes = core::mem::take(&mut env.removes);
    adds.clear();
    removes.clear();
    match (target_view, source_view) {
        // AND/ANDNOT against nothing leaves an empty target empty; OR/XOR
        // against nothing leaves any target as it is.
        (None, None) => {}
        (None, Some(source)) => match op {
            BitmapAlgebraOp::And | BitmapAlgebraOp::AndNot => {}
            BitmapAlgebraOp::Or | BitmapAlgebraOp::Xor => adds.extend(source.range()),
        },
        (Some(target), None) => match op {
            BitmapAlgebraOp::And => removes.extend(target.range()),
            BitmapAlgebraOp::Or | BitmapAlgebraOp::AndNot | BitmapAlgebraOp::Xor => {}
        },
        (Some(target), Some(source)) => match op {
            BitmapAlgebraOp::And => {
                removes.extend(AndNotRange::new(target.range(), source.range()));
            }
            BitmapAlgebraOp::Or => {
                adds.extend(AndNotRange::new(source.range(), target.range()));
            }
            BitmapAlgebraOp::AndNot => {
                removes.extend(AndRange::leapfrog(target.range(), source.range()));
            }
            BitmapAlgebraOp::Xor => {
                adds.extend(AndNotRange::new(source.range(), target.range()));
                removes.extend(AndRange::leapfrog(target.range(), source.range()));
            }
        },
    }

    if adds.is_empty() && removes.is_empty() {
        env.adds = adds;
        env.removes = removes;
        return ErrorCode::Ok;
    }

    // Force undo snapshot before bulk mutation.
    if hooks.undo_enabled() && !hooks.undo_overflow() {
        hooks.force_undo_snapshot(state);
    }

    let patched = bitmap_patch(env, state, target_storage, &adds, &removes);
    env.adds = adds;
    env.removes = removes;
    let report = match patched {
        Ok(report) => report,
        Err(code) => {
            target_meta.set_size(state, original_size);
            return code;
        }
    };

    let new_card = report.len as u32;
    target_meta.set_size(state, new_card);
    if new_card != original_size {
        target_meta.set_change_flag(state, ChangeFlag::SIZE_CHANGED);
    }
    ErrorCode::Ok
}

/// Replace the slot's set with `input`, an AXR1 string that crossed a trust
/// boundary: it is verified in full, its cardinality checked against the
/// slot's element capacity, and a sealed Elias-Fano root is re-encoded as
/// the forest the slot keeps. Zero bytes are the empty set.
pub fn bitmap_import(
    env: &mut BitmapEnv,
    state: &mut [u8],
    meta: &SlotMetaView,
    input: &[u8],
) -> ErrorCode {
    let storage = get_bitmap_storage(meta);
    let payload = storage.payload_offset() as usize;
    let capacity = storage.payload_capacity as usize;
    let Some(view) = (if input.is_empty() {
        None
    } else {
        AxroarView::open_verified(input)
    }) else {
        if !input.is_empty() {
            env.last_error = 102;
            return ErrorCode::InvalidState;
        }
        storage.set_serialized_len(state, 0);
        state[payload..payload + capacity].fill(0);
        meta.set_size(state, 0);
        return ErrorCode::Ok;
    };
    let Ok(card) = u32::try_from(view.len()) else {
        return ErrorCode::CapacityExceeded;
    };
    if card > meta.capacity {
        return ErrorCode::CapacityExceeded;
    }
    if card == 0 {
        return bitmap_import(env, state, meta, &[]);
    }
    let slot = &mut state[payload..payload + capacity];
    let written = if view.is_elias_fano() {
        match Axroar::from_sorted(view.range()).write_forest_into_slice(slot) {
            Ok(written) => written,
            Err(_) => {
                env.last_error = 60;
                return ErrorCode::CapacityExceeded;
            }
        }
    } else {
        if input.len() > capacity {
            env.last_error = 60;
            return ErrorCode::CapacityExceeded;
        }
        slot[..input.len()].copy_from_slice(input);
        input.len()
    };
    slot[written..].fill(0);
    storage.set_serialized_len(state, written as u32);
    meta.set_size(state, card);
    ErrorCode::Ok
}

// =============================================================================
// Serialized-level queries and set algebra (decision-function side)
// =============================================================================

/// Test whether a serialized bitmap contains `value`. Invalid or empty
/// payloads return false.
pub fn contains_serialized(data: &[u8], value: u32) -> bool {
    AxroarView::open(data).is_some_and(|view| view.contains(value))
}

/// Return serialized bitmap cardinality, saturating at `u32::MAX`.
pub fn cardinality_serialized(data: &[u8]) -> u32 {
    AxroarView::open(data).map_or(0, |view| u32::try_from(view.len()).unwrap_or(u32::MAX))
}

/// Verify a serialized bitmap in full and return its cardinality. A `None`
/// result distinguishes malformed bytes from an empty bitmap.
pub fn cardinality_validated(data: &[u8]) -> Option<u32> {
    if data.is_empty() {
        return Some(0);
    }
    let view = AxroarView::open_verified(data)?;
    u32::try_from(view.len()).ok()
}

/// Extract ascending values into `out`, capped at its length; return the count.
pub fn extract_serialized(data: &[u8], out: &mut [u32]) -> u32 {
    let Some(view) = AxroarView::open(data) else {
        return 0;
    };
    let mut count = 0usize;
    for (slot, value) in out.iter_mut().zip(view.range()) {
        *slot = value;
        count += 1;
    }
    count as u32
}

/// Test whether two serialized bitmaps intersect: the leapfrog merge stops
/// at the first common member.
pub fn intersects_serialized(left: &[u8], right: &[u8]) -> bool {
    match (AxroarView::open(left), AxroarView::open(right)) {
        (Some(l), Some(r)) => !AndRange::leapfrog(l.range(), r.range()).empty(),
        _ => false,
    }
}

/// Count the intersection of two serialized bitmaps, saturating at `u32::MAX`.
pub fn intersect_count_serialized(left: &[u8], right: &[u8]) -> u32 {
    match (AxroarView::open(left), AxroarView::open(right)) {
        (Some(l), Some(r)) => u32::try_from(l.and_len(&r)).unwrap_or(u32::MAX),
        _ => 0,
    }
}

/// Apply set algebra and store the result in `env.algebra_result`, the
/// VM-owned buffer exported by the wasm layer. Zero bytes are the empty
/// result.
pub fn set_algebra(
    env: &mut BitmapEnv,
    op: BitmapAlgebraOp,
    left: &[u8],
    right: &[u8],
) -> ErrorCode {
    env.algebra_result.clear();

    // Empty-set identities — copy the survivor directly.
    if left.is_empty() && right.is_empty() {
        return ErrorCode::Ok;
    }
    if left.is_empty() {
        return match op {
            BitmapAlgebraOp::And | BitmapAlgebraOp::AndNot => ErrorCode::Ok,
            BitmapAlgebraOp::Or | BitmapAlgebraOp::Xor => {
                env.algebra_result.extend_from_slice(right);
                ErrorCode::Ok
            }
        };
    }
    if right.is_empty() {
        return match op {
            BitmapAlgebraOp::And => ErrorCode::Ok,
            BitmapAlgebraOp::Or | BitmapAlgebraOp::AndNot | BitmapAlgebraOp::Xor => {
                env.algebra_result.extend_from_slice(left);
                ErrorCode::Ok
            }
        };
    }

    let Some(l) = AxroarView::open(left) else {
        env.last_error = 71;
        return ErrorCode::InvalidState;
    };
    let Some(r) = AxroarView::open(right) else {
        env.last_error = 72;
        return ErrorCode::InvalidState;
    };
    let result = match op {
        BitmapAlgebraOp::And => Axroar::from_sorted(AndRange::leapfrog(l.range(), r.range())),
        BitmapAlgebraOp::Or => Axroar::from_sorted(OrRange::new(l.range(), r.range())),
        BitmapAlgebraOp::AndNot => Axroar::from_sorted(AndNotRange::new(l.range(), r.range())),
        BitmapAlgebraOp::Xor => Axroar::from_sorted(XorRange::new(l.range(), r.range())),
    };
    if result.is_empty() {
        return ErrorCode::Ok;
    }
    env.algebra_result.resize(result.forest_len(), 0);
    if result
        .write_forest_into_slice(&mut env.algebra_result)
        .is_err()
    {
        columine_types::die!("forest_len sized the buffer the forest writer refused");
    }
    ErrorCode::Ok
}
