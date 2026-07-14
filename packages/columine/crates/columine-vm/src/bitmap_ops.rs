//! Port of `packages/columine/src/vm/bitmap_ops.zig` — roaring-bitmap slot
//! storage, load/store, batch add/remove, and set algebra.
//!
//! # Roaring backend
//!
//! The Zig side uses `rawr` (smoothbricks' roaring library), which implements
//! the standard portable RoaringFormatSpec (cookies 12346/12347 — verified in
//! rawr `src/format.zig`). This port uses the pure-Rust `roaring` crate,
//! which implements the SAME spec, so serialized payloads are mutually
//! readable in both directions. Freshly-SERIALIZED byte images may differ
//! within the spec (rawr's `runOptimize` and roaring-rs's `optimize` may pick
//! different container encodings for the same logical set); the cutover
//! consequence is recorded in the crate README's ledger.
//!
//! # Globals become `BitmapEnv`
//!
//! bitmap_ops.zig keeps module globals (`g_bitmap_last_error`, the reusable
//! store/algebra buffers, and the wasm scratch FixedBufferAllocator wired by
//! `vm_set_rbmp_scratch`). Rust models the observable ones as [`BitmapEnv`],
//! which the wasm bindings stage will instantiate once per instance. The
//! scratch-FBA machinery exists to bound allocation inside a wasm call and
//! surface OOM as `CAPACITY_EXCEEDED` (error codes 100/101, plus the
//! non-scratch OOM fallbacks); Rust's global allocator has no observable
//! failure path here, so those code paths are unreachable and documented at
//! their sites rather than emulated.

use crate::bytes;
use crate::hooks::{MutationOp, MutationRecord, VmHooks};
use crate::meta::SlotMetaView;
use crate::minroar::MiniRoaring as RoaringBitmap;
use columine_types::types::{
    BITMAP_BASE_BYTES, BITMAP_BYTES_PER_CAPACITY, BITMAP_SERIALIZED_LEN_BYTES, ChangeFlag,
    EMPTY_KEY, ErrorCode, TOMBSTONE,
};

/// bitmap_ops.zig:144-146. THE canonical payload-capacity formula —
/// allocation, grow copy, and readers all derive from it (fixed semantics of
/// telos idea i-87c94893; the drifted private growth formula is deleted).
pub const fn bitmap_payload_capacity(slot_capacity: u32) -> u32 {
    slot_capacity * BITMAP_BYTES_PER_CAPACITY + BITMAP_BASE_BYTES
}

/// bitmap_ops.zig:125 `BitmapStorage` — offsets instead of pointers (crate
/// convention: no references into the state buffer).
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

    /// bitmap_ops.zig:686 `slotSerializedData` — `None` if empty or invalid.
    pub fn serialized_data<'a>(&self, state: &'a [u8]) -> Option<&'a [u8]> {
        let len = self.serialized_len(state);
        if len == 0 || len > self.payload_capacity {
            return None;
        }
        let start = self.payload_offset() as usize;
        Some(&state[start..start + len as usize])
    }
}

/// bitmap_ops.zig:148 `getBitmapStorage`.
pub fn get_bitmap_storage(meta: &SlotMetaView) -> BitmapStorage {
    BitmapStorage {
        data_offset: meta.offset,
        payload_capacity: bitmap_payload_capacity(meta.capacity),
    }
}

/// The observable module state of bitmap_ops.zig (minus the wasm scratch
/// machinery — module docs). Reusable buffers exist for the same reason the
/// Zig globals do: no per-call allocation churn on the store path.
#[derive(Debug, Default)]
pub struct BitmapEnv {
    /// `g_bitmap_last_error` — diagnostic code readable after a failure.
    pub last_error: u32,
    store_temp: Vec<u8>,
    algebra_result: Vec<u8>,
}

impl BitmapEnv {
    /// `vm_rbmp_algebra_result_{ptr,len}` equivalent — the bindings stage
    /// exposes the buffer; core code returns the slice.
    pub fn algebra_result(&self) -> &[u8] {
        &self.algebra_result
    }
}

/// bitmap_ops.zig:172 `bitmapLoad`. `None` ⇒ caller maps to an error path;
/// `env.last_error` carries the diagnostic exactly like the Zig global.
/// (The Zig OOM fallbacks — codes 100/101/103 and the scratch-retry — are
/// unreachable here; deserialize failure is the surviving failure mode.)
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

/// bitmap_ops.zig:219 `bitmapStore` — run-optimize, size check, two-phase
/// commit through a reusable temp buffer (a failed serialize must leave the
/// slot bytes unmodified), then copy + zero the payload tail.
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
        // A Vec sink cannot fail in practice; preserved as the Zig `else` lane.
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

/// bitmap_ops.zig:271 `bitmapSelect` — element at `rank` in ascending order.
pub fn bitmap_select(state: &[u8], storage: BitmapStorage, rank: u32) -> Option<u32> {
    let data = storage.serialized_data(state)?;
    let bm = RoaringBitmap::deserialize_from(data).ok()?;
    bm.iter().nth(rank as usize)
}

/// bitmap_ops.zig:282 `batchBitmapAdd`. Control flow ported branch-for-branch;
/// the `bitmap.add() catch` scratch-OOM branch (error codes 3/4) is
/// unreachable in Rust and therefore absent.
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
    let storage = get_bitmap_storage(meta);
    let Some(mut bitmap) = bitmap_load(env, state, storage) else {
        if env.last_error == 0 {
            env.last_error = 1;
        }
        return ErrorCode::InvalidState;
    };

    let original_size = meta.size(state);
    let mut cardinality = bitmap.len() as u32;
    if cardinality != original_size {
        meta.set_size(state, cardinality);
    }
    let mut had_insert = false;

    for (i, &elem) in elem_col.iter().enumerate() {
        if elem == EMPTY_KEY || elem == TOMBSTONE {
            continue;
        }
        let ts = if meta.has_ttl() {
            ts_col.unwrap_or_else(|| columine_types::die!("TTL slot requires a timestamp column"))
                [i]
        } else {
            0.0
        };

        let already_present = bitmap.contains(elem);
        if !already_present && cardinality >= meta.capacity {
            if had_insert {
                let flush_result = bitmap_store(env, state, storage, &mut bitmap);
                if flush_result != ErrorCode::Ok {
                    env.last_error = 2;
                    meta.set_size(state, original_size);
                    return flush_result;
                }
                meta.set_size(state, cardinality);
                meta.set_change_flag(state, ChangeFlag::INSERTED);
            } else {
                meta.set_size(state, original_size);
            }
            return ErrorCode::CapacityExceeded;
        }

        let inserted = !already_present && bitmap.insert(elem);

        if !inserted {
            if meta.has_ttl() {
                let ttl_result = hooks.insert_with_ttl(state, meta, elem, ts);
                if ttl_result != ErrorCode::Ok {
                    if had_insert {
                        let flush_result = bitmap_store(env, state, storage, &mut bitmap);
                        if flush_result != ErrorCode::Ok {
                            meta.set_size(state, original_size);
                            return flush_result;
                        }
                        meta.set_change_flag(state, ChangeFlag::INSERTED);
                    }
                    meta.set_size(state, cardinality);
                    return ttl_result;
                }
            }
            continue;
        }

        if hooks.undo_enabled() {
            meta.set_size(state, cardinality);
            hooks.append_mutation(
                delta_mode,
                state,
                MutationRecord {
                    op: MutationOp::SetInsert,
                    slot: slot_idx,
                    key: elem,
                    prev_value: 0,
                    aux: 0,
                },
                MutationRecord {
                    op: MutationOp::SetDelete,
                    slot: slot_idx,
                    key: elem,
                    prev_value: 0,
                    aux: 0,
                },
            );
        }

        cardinality += 1;
        had_insert = true;

        if meta.has_ttl() {
            let ttl_result = hooks.insert_with_ttl(state, meta, elem, ts);
            if ttl_result != ErrorCode::Ok {
                let flush_result = bitmap_store(env, state, storage, &mut bitmap);
                if flush_result != ErrorCode::Ok {
                    env.last_error = 5;
                    meta.set_size(state, original_size);
                    return flush_result;
                }
                meta.set_size(state, cardinality);
                if had_insert {
                    meta.set_change_flag(state, ChangeFlag::INSERTED);
                }
                return ttl_result;
            }
        }
    }

    let store_result = bitmap_store(env, state, storage, &mut bitmap);
    if store_result != ErrorCode::Ok {
        if env.last_error == 0 {
            env.last_error = 6;
        }
        meta.set_size(state, original_size);
        return store_result;
    }

    meta.set_size(state, cardinality);
    if had_insert {
        meta.set_change_flag(state, ChangeFlag::INSERTED);
    }
    ErrorCode::Ok
}

/// bitmap_ops.zig:425 `batchBitmapRemove` — void return like the Zig
/// (failures leave the slot bytes unchanged).
pub fn batch_bitmap_remove(
    env: &mut BitmapEnv,
    hooks: &mut impl VmHooks,
    delta_mode: bool,
    state: &mut [u8],
    meta: &SlotMetaView,
    slot_idx: u8,
    elem_col: &[u32],
) {
    let storage = get_bitmap_storage(meta);
    let Some(mut bitmap) = bitmap_load(env, state, storage) else {
        return;
    };

    let mut cardinality = bitmap.len() as u32;
    let mut had_remove = false;

    for &elem in elem_col {
        if cardinality == 0 {
            break;
        }
        let removed = bitmap.remove(elem);
        if !removed {
            continue;
        }

        if hooks.undo_enabled() {
            let mut prev_ts_bits: u64 = 0;
            if meta.has_ttl()
                && let Some(prev_ts) = hooks.latest_eviction_ts(state, meta, elem)
            {
                prev_ts_bits = prev_ts.to_bits();
            }
            meta.set_size(state, cardinality);
            hooks.append_mutation(
                delta_mode,
                state,
                MutationRecord {
                    op: MutationOp::SetDelete,
                    slot: slot_idx,
                    key: elem,
                    prev_value: 0,
                    aux: prev_ts_bits,
                },
                MutationRecord {
                    op: MutationOp::SetInsert,
                    slot: slot_idx,
                    key: elem,
                    prev_value: 0,
                    aux: 0,
                },
            );
        }

        cardinality -= 1;
        had_remove = true;
        if meta.has_ttl() {
            hooks.remove_ttl_entries_for_key(state, meta, elem);
        }
    }

    if !had_remove {
        meta.set_size(state, cardinality);
        return;
    }

    if bitmap_store(env, state, storage, &mut bitmap) != ErrorCode::Ok {
        return;
    }

    meta.set_size(state, cardinality);
    meta.set_change_flag(state, ChangeFlag::REMOVED);
}

/// bitmap_ops.zig:507 `BitmapAlgebraOp`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BitmapAlgebraOp {
    And,
    Or,
    AndNot,
    Xor,
}

/// bitmap_ops.zig:531 `batchBitmapAlgebra` — in-place set algebra on a
/// target slot. Bulk mutations are covered by an undo SNAPSHOT (per-element
/// tracking is impractical), requested through the hooks boundary.
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

    // In-place operation (assign operators; the Zig per-op error lanes 81-84
    // are alloc failures, unreachable here).
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

/// bitmap_ops.zig:861 `vm_rbmp_contains_serialized` (logic only; the export
/// shell is the bindings stage). rawr answers through a zero-copy
/// FrozenBitmap; the roaring crate has no frozen view, so this deserializes —
/// logically identical, allocation noted for the bindings-stage perf review.
pub fn contains_serialized(data: &[u8], value: u32) -> bool {
    if data.is_empty() {
        return false;
    }
    RoaringBitmap::deserialize_from(data)
        .map(|bm| bm.contains(value))
        .unwrap_or(false)
}

/// bitmap_ops.zig:868 `vm_rbmp_cardinality_serialized` (saturates at u32::MAX).
pub fn cardinality_serialized(data: &[u8]) -> u32 {
    if data.is_empty() {
        return 0;
    }
    RoaringBitmap::deserialize_from(data)
        .map(|bm| u32::try_from(bm.len()).unwrap_or(u32::MAX))
        .unwrap_or(0)
}

/// `vm_rbmp_import_copy` (bitmap_ops.zig FrozenBitmap.init + cardinality)
/// needs validity and cardinality from ONE probe — `cardinality_serialized`'s
/// 0-on-invalid is ambiguous with an empty bitmap. `None` ⇒ malformed bytes.
pub fn cardinality_validated(data: &[u8]) -> Option<u32> {
    RoaringBitmap::deserialize_from(data)
        .ok()
        .map(|bm| u32::try_from(bm.len()).unwrap_or(u32::MAX))
}

/// bitmap_ops.zig:877 `vm_rbmp_extract_serialized` — ascending values,
/// capped at the output buffer; returns the number written.
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

/// bitmap_ops.zig:679 `deserializedIntersects`.
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

/// bitmap_ops.zig:671 `deserializedIntersectCount` (saturates at u32::MAX).
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

/// bitmap_ops.zig:779 `rbmpSetAlgebra` — the result lands in
/// `env.algebra_result` (the VM-owned buffer the wasm exports point at).
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
