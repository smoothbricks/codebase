//! Roaring-bitmap slot storage, load/store, batch mutation, and set algebra.
//!
//! # Roaring backend
//!
//! The implementation follows the standard portable RoaringFormatSpec
//! (cookies 12346/12347), so serialized payloads are mutually readable.
//! Freshly serialized byte images may differ within the spec because
//! container optimization choices are implementation-dependent.
//! # Allocation behavior
//!
//! Rust backing storage does not expose allocator-failure branches; invalid
//! serialization and capacity failures are the observable error paths.
//! Scratch-related failure codes are therefore unreachable and are not
//! emulated.
use crate::bytes;
use crate::hooks::{MutationRecord, VmHooks};
use crate::meta::SlotMetaView;
use crate::minroar::MiniRoaring as RoaringBitmap;
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

    /// `None` when empty or invalid.
    pub fn serialized_data<'a>(&self, state: &'a [u8]) -> Option<&'a [u8]> {
        let len = self.serialized_len(state);
        if len == 0 || len > self.payload_capacity {
            return None;
        }
        let start = self.payload_offset() as usize;
        Some(&state[start..start + len as usize])
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

/// Observable bitmap operation state. Reusable buffers avoid allocation churn
/// on store, algebra, and mutation-commit paths.
#[derive(Debug, Default)]
pub struct BitmapEnv {
    /// `g_bitmap_last_error` — diagnostic code readable after a failure.
    pub last_error: u32,
    store_temp: Vec<u8>,
    algebra_result: Vec<u8>,
    pending_mutations: Vec<PendingBitmapMutation>,
}

impl BitmapEnv {
    /// `vm_rbmp_algebra_result_{ptr,len}` equivalent — the bindings stage
    /// exposes the buffer; core code returns the slice.
    pub fn algebra_result(&self) -> &[u8] {
        &self.algebra_result
    }
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

/// Load a serialized bitmap. `None` maps to an error path and
/// `env.last_error` carries the diagnostic; deserialization failure is the
/// surviving failure mode.
pub fn bitmap_load(
    env: &mut BitmapEnv,
    state: &[u8],
    storage: BitmapStorage,
) -> Option<RoaringBitmap> {
    let serialized_len = storage.serialized_len(state);
    if serialized_len == 0 {
        return Some(RoaringBitmap::new());
    }
    if serialized_len > storage.payload_capacity {
        return None;
    }
    let start = storage.payload_offset() as usize;
    let data = &state[start..start + serialized_len as usize];
    match RoaringBitmap::deserialize_from(data) {
        Ok(bm) => Some(bm),
        Err(_) => {
            env.last_error = 102; // error.InvalidFormat lane
            None
        }
    }
}

/// Store a bitmap with run optimization, size checking, and a two-phase commit
/// through a reusable temporary buffer. Failed serialization leaves slot bytes
/// unmodified; the payload tail is zeroed after a successful copy.
pub fn bitmap_store(
    env: &mut BitmapEnv,
    state: &mut [u8],
    storage: BitmapStorage,
    bitmap: &mut RoaringBitmap,
) -> ErrorCode {
    bitmap.optimize();

    let serialized_size_needed = bitmap.serialized_size();
    if serialized_size_needed > storage.payload_capacity as usize {
        env.last_error = 60;
        return ErrorCode::CapacityExceeded;
    }

    env.store_temp.clear();
    env.store_temp.reserve(serialized_size_needed);
    if bitmap.serialize_into(&mut env.store_temp).is_err() {
        // A Vec sink is infallible in practice; retain the error code for a
        // uniform failure path.
        env.last_error = 61;
        return ErrorCode::InvalidState;
    }

    let serialized_size = env.store_temp.len() as u32;
    if serialized_size > storage.payload_capacity {
        return ErrorCode::CapacityExceeded;
    }

    storage.set_serialized_len(state, serialized_size);
    let payload = storage.payload_offset() as usize;
    state[payload..payload + serialized_size as usize].copy_from_slice(&env.store_temp);
    if serialized_size < storage.payload_capacity {
        bytes::zero(
            state,
            storage.payload_offset() + serialized_size,
            storage.payload_capacity - serialized_size,
        );
    }
    ErrorCode::Ok
}

/// Select the element at `rank` in ascending order.
pub fn bitmap_select(state: &[u8], storage: BitmapStorage, rank: u32) -> Option<u32> {
    let data = storage.serialized_data(state)?;
    let bm = RoaringBitmap::deserialize_from(data).ok()?;
    bm.iter().nth(rank as usize)
}

/// Add a batch of elements. Rust's backing storage has no scratch-allocation
/// failure path.
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
    env.pending_mutations.reserve(elem_col.len());
    let storage = get_bitmap_storage(meta);
    let Some(mut bitmap) = bitmap_load(env, state, storage) else {
        if env.last_error == 0 {
            env.last_error = 1;
        }
        return ErrorCode::InvalidState;
    };

    let mut cardinality = bitmap.len() as u32;
    let mut had_insert = false;

    for (i, &elem) in elem_col.iter().enumerate() {
        if elem == EMPTY_KEY || elem == TOMBSTONE {
            continue;
        }
        let timestamp = if meta.has_ttl() {
            ts_col.unwrap_or_else(|| columine_types::die!("TTL slot requires a timestamp column"))
                [i]
        } else {
            0.0
        };

        if bitmap.contains(elem) {
            if meta.has_ttl() {
                env.pending_mutations
                    .push(PendingBitmapMutation::RefreshTtl {
                        key: elem,
                        timestamp,
                    });
            }
            continue;
        }
        if cardinality >= meta.capacity {
            let store_result = bitmap_store(env, state, storage, &mut bitmap);
            if store_result != ErrorCode::Ok {
                env.pending_mutations.clear();
                return store_result;
            }
            meta.set_size(state, cardinality);
            if had_insert {
                meta.set_change_flag(state, ChangeFlag::INSERTED);
            }
            let commit_result =
                commit_bitmap_mutations(env, hooks, delta_mode, state, meta, slot_idx);
            return if commit_result == ErrorCode::Ok {
                ErrorCode::CapacityExceeded
            } else {
                commit_result
            };
        }
        if bitmap.insert(elem) {
            cardinality += 1;
            had_insert = true;
            env.pending_mutations.push(PendingBitmapMutation::Insert {
                key: elem,
                timestamp,
            });
        }
    }

    let store_result = bitmap_store(env, state, storage, &mut bitmap);
    if store_result != ErrorCode::Ok {
        env.pending_mutations.clear();
        return store_result;
    }

    meta.set_size(state, cardinality);
    if had_insert {
        meta.set_change_flag(state, ChangeFlag::INSERTED);
    }
    commit_bitmap_mutations(env, hooks, delta_mode, state, meta, slot_idx)
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
    env.pending_mutations.clear();
    env.pending_mutations.reserve(elem_col.len());
    let storage = get_bitmap_storage(meta);
    let Some(mut bitmap) = bitmap_load(env, state, storage) else {
        return ErrorCode::InvalidState;
    };

    let mut cardinality = bitmap.len() as u32;
    let mut had_remove = false;

    for &elem in elem_col {
        if cardinality == 0 {
            break;
        }
        if !bitmap.remove(elem) {
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
        cardinality -= 1;
        had_remove = true;
    }

    if had_remove {
        let store_result = bitmap_store(env, state, storage, &mut bitmap);
        if store_result != ErrorCode::Ok {
            env.pending_mutations.clear();
            return store_result;
        }
        meta.set_size(state, cardinality);
        meta.set_change_flag(state, ChangeFlag::REMOVED);
    } else {
        meta.set_size(state, cardinality);
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

/// Apply in-place set algebra to a target slot. Bulk mutations use one undo
/// snapshot because per-element tracking is impractical.
pub fn batch_bitmap_algebra(
    env: &mut BitmapEnv,
    hooks: &mut impl VmHooks,
    op: BitmapAlgebraOp,
    state: &mut [u8],
    target_meta: &SlotMetaView,
    source_data: &[u8],
) -> ErrorCode {
    let target_storage = get_bitmap_storage(target_meta);
    let original_size = target_meta.size(state);

    // Empty-source identities.
    if source_data.is_empty() {
        match op {
            BitmapAlgebraOp::And => {
                // AND with empty = clear target.
                if hooks.undo_enabled() && !hooks.undo_overflow() {
                    hooks.force_undo_snapshot(state);
                }
                target_storage.set_serialized_len(state, 0);
                bytes::zero(
                    state,
                    target_storage.payload_offset(),
                    target_storage.payload_capacity,
                );
                target_meta.set_size(state, 0);
                if original_size != 0 {
                    target_meta.set_change_flag(state, ChangeFlag::SIZE_CHANGED);
                }
                return ErrorCode::Ok;
            }
            // OR/ANDNOT/XOR with empty = no change.
            BitmapAlgebraOp::Or | BitmapAlgebraOp::AndNot | BitmapAlgebraOp::Xor => {
                return ErrorCode::Ok;
            }
        }
    }

    // Force undo snapshot before bulk mutation.
    if hooks.undo_enabled() && !hooks.undo_overflow() {
        hooks.force_undo_snapshot(state);
    }

    let Some(mut target) = bitmap_load(env, state, target_storage) else {
        return ErrorCode::InvalidState;
    };
    let Ok(source) = RoaringBitmap::deserialize_from(source_data) else {
        env.last_error = 80;
        return ErrorCode::InvalidState;
    };

    // In-place operation; backing storage allocation failures are not exposed
    // by this implementation.
    match op {
        BitmapAlgebraOp::And => target &= &source,
        BitmapAlgebraOp::Or => target |= &source,
        BitmapAlgebraOp::AndNot => target -= &source,
        BitmapAlgebraOp::Xor => target ^= &source,
    }

    let store_result = bitmap_store(env, state, target_storage, &mut target);
    if store_result != ErrorCode::Ok {
        target_meta.set_size(state, original_size);
        return store_result;
    }

    let new_card = target.len() as u32;
    target_meta.set_size(state, new_card);
    if new_card != original_size {
        target_meta.set_change_flag(state, ChangeFlag::SIZE_CHANGED);
    }
    ErrorCode::Ok
}

// =============================================================================
// Serialized-level queries and set algebra (decision-function side)
// =============================================================================

/// Test whether a serialized bitmap contains `value`. Invalid or empty
/// payloads return false.
pub fn contains_serialized(data: &[u8], value: u32) -> bool {
    if data.is_empty() {
        return false;
    }
    let Ok(contains) = RoaringBitmap::contains_bytes(data, value) else {
        return false;
    };
    contains
}

/// Return serialized bitmap cardinality, saturating at `u32::MAX`.
pub fn cardinality_serialized(data: &[u8]) -> u32 {
    if data.is_empty() {
        return 0;
    }
    let Ok(cardinality) = RoaringBitmap::len_bytes(data) else {
        return 0;
    };
    cardinality
}

/// Validate a serialized bitmap and return its cardinality. A `None` result
/// distinguishes malformed bytes from an empty bitmap.
pub fn cardinality_validated(data: &[u8]) -> Option<u32> {
    RoaringBitmap::len_bytes(data).ok()
}

/// Extract ascending values into `out`, capped at its length; return the count.
pub fn extract_serialized(data: &[u8], out: &mut [u32]) -> u32 {
    if data.is_empty() {
        return 0;
    }
    let Ok(bm) = RoaringBitmap::deserialize_from(data) else {
        return 0;
    };
    let mut count = 0usize;
    for v in bm.iter() {
        if count >= out.len() {
            break;
        }
        out[count] = v;
        count += 1;
    }
    count as u32
}

/// Test whether two serialized bitmaps intersect.
pub fn intersects_serialized(left: &[u8], right: &[u8]) -> bool {
    if left.is_empty() || right.is_empty() {
        return false;
    }
    match (
        RoaringBitmap::deserialize_from(left),
        RoaringBitmap::deserialize_from(right),
    ) {
        (Ok(l), Ok(r)) => !l.is_disjoint(&r),
        _ => false,
    }
}

/// Count the intersection of two serialized bitmaps, saturating at `u32::MAX`.
pub fn intersect_count_serialized(left: &[u8], right: &[u8]) -> u32 {
    if left.is_empty() || right.is_empty() {
        return 0;
    }
    match (
        RoaringBitmap::deserialize_from(left),
        RoaringBitmap::deserialize_from(right),
    ) {
        (Ok(l), Ok(r)) => u32::try_from(l.intersection_len(&r)).unwrap_or(u32::MAX),
        _ => 0,
    }
}

/// Apply set algebra and store the result in `env.algebra_result`, the
/// VM-owned buffer exported by the wasm layer.
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

    let Ok(l) = RoaringBitmap::deserialize_from(left) else {
        env.last_error = 71;
        return ErrorCode::InvalidState;
    };
    let Ok(r) = RoaringBitmap::deserialize_from(right) else {
        env.last_error = 72;
        return ErrorCode::InvalidState;
    };
    let mut result = match op {
        BitmapAlgebraOp::And => l & r,
        BitmapAlgebraOp::Or => l | r,
        BitmapAlgebraOp::AndNot => l - r,
        BitmapAlgebraOp::Xor => l ^ r,
    };

    result.optimize();

    if result.serialize_into(&mut env.algebra_result).is_err() {
        env.last_error = 75;
        return ErrorCode::InvalidState;
    }
    ErrorCode::Ok
}
