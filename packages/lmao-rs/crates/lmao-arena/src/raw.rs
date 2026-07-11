//! Function-by-function port of allocator.zig's allocator core, generic over a
//! [`Mem`] linear-memory backend so the identical logic runs natively (over
//! `Vec<u8>`, see [`crate::Arena`]) and on wasm32 (over WASM linear memory, see
//! the `lmao-wasm` crate).
//!
//! Offsets, sentinel conventions, stat cascading, and merge order are ABI /
//! behavior-identical to the Zig (`packages/lmao/src/lib/wasm/allocator.zig`);
//! each `pub fn` names its Zig counterpart. Time is passed IN (`current_ms`,
//! `wall_ms`) instead of imported, keeping this module deterministic — the
//! wasm exports fetch host clocks, native callers use `Clock` from lmao-core.

use crate::{FREE_BLOCK_SIZE, HEADER_SIZE, IDENTITY_SIZE, NUM_TIERS, SizeClass, block_size, capacity_to_tier, tier_to_capacity};

/// Linear memory backend. Offsets are absolute byte offsets; offset 0 holds the
/// header, so 0 doubles as the null sentinel exactly as in the Zig.
///
/// Growth is behind this trait: the native backend grows a `Vec`, the wasm
/// backend calls `memory.grow`. `grow_to` returns false on OOM (alloc then
/// returns the 0 sentinel).
pub trait Mem {
    fn size(&self) -> u32;
    /// Ensure at least `new_size` bytes are addressable. False = OOM.
    fn grow_to(&mut self, new_size: u32) -> bool;
    fn read_u8(&self, off: u32) -> u8;
    fn write_u8(&mut self, off: u32, v: u8);
    fn read_u32(&self, off: u32) -> u32;
    fn write_u32(&mut self, off: u32, v: u32);
    fn read_u64(&self, off: u32) -> u64;
    fn write_u64(&mut self, off: u32, v: u64);
    fn read_i64(&self, off: u32) -> i64 {
        self.read_u64(off) as i64
    }
    fn write_i64(&mut self, off: u32, v: i64) {
        self.write_u64(off, v as u64);
    }
    fn read_f64(&self, off: u32) -> f64 {
        f64::from_bits(self.read_u64(off))
    }
    fn write_f64(&mut self, off: u32, v: f64) {
        self.write_u64(off, v.to_bits());
    }
}

// --- Header field offsets (Header extern struct in allocator.zig) ---
const H_BUMP_PTR: u32 = 0;
const H_SPAN_ID_COUNTER: u32 = 4;
const H_ALLOC_COUNT: u32 = 8;
const H_FREE_COUNT: u32 = 12;
const H_FREELIST_IDENTITY: u32 = 16;
const H_THREAD_ID: u32 = 24;
const H_FREELISTS: u32 = 32; // [u32; 28]
const H_THREAD_ID_SET: u32 = 144;

// --- FreeBlock field offsets (overlaid on freed memory) ---
const FB_NEXT_PTR: u32 = 0;
const FB_FREELIST_LEN: u32 = 4;
const FB_REUSE_COUNT: u32 = 8;
const FB_SPLIT_COUNT: u32 = 12;
const FB_MERGE_COUNT: u32 = 16;
const _: () = assert!(FB_MERGE_COUNT as usize + 4 == FREE_BLOCK_SIZE);

// --- Identity field offsets ---
const ID_WRITE_INDEX: u32 = 0;
const ID_SPAN_ID: u32 = 4;
const ID_TRACE_ID_LEN: u32 = 8;
pub const ID_TRACE_ID: u32 = 9; // @offsetOf(Identity, "trace_id")
const ID_TRACE_ID_MAX: u32 = 119;

// --- TraceRoot field offsets ---
const TR_WALL_CLOCK_NANOS: u32 = 0;
const TR_MONOTONIC_MS: u32 = 8;

// Entry types (specs/lmao/01h)
pub const ENTRY_TYPE_SPAN_START: u8 = 1;
pub const ENTRY_TYPE_SPAN_OK: u8 = 2;
pub const ENTRY_TYPE_SPAN_ERR: u8 = 3;
pub const ENTRY_TYPE_SPAN_EXCEPTION: u8 = 4;

#[inline]
fn freelist_off(sc: SizeClass, tier: usize) -> u32 {
    H_FREELISTS + 4 * (sc as u32 * NUM_TIERS as u32 + tier as u32)
}

#[inline]
fn freelist_head<M: Mem>(m: &M, sc: SizeClass, tier: usize) -> u32 {
    m.read_u32(freelist_off(sc, tier))
}

#[inline]
fn set_freelist_head<M: Mem>(m: &mut M, sc: SizeClass, tier: usize, v: u32) {
    m.write_u32(freelist_off(sc, tier), v);
}

/// `init()` — idempotent header initialization (first call only).
pub fn init<M: Mem>(m: &mut M) {
    if m.read_u32(H_BUMP_PTR) == 0 {
        reset(m);
    }
    // thread_id persists across init calls
}

/// `reset()` — testing/benchmark reset; leaks all live blocks by design.
pub fn reset<M: Mem>(m: &mut M) {
    m.write_u32(H_BUMP_PTR, HEADER_SIZE as u32);
    m.write_u32(H_ALLOC_COUNT, 0);
    m.write_u32(H_FREE_COUNT, 0);
    m.write_u32(H_SPAN_ID_COUNTER, 0);
    for i in 0..crate::NUM_FREELISTS {
        m.write_u32(H_FREELISTS + 4 * i as u32, 0);
    }
    m.write_u32(H_FREELIST_IDENTITY, 0);
    // thread_id persists across reset calls
}

/// `allocAtTier` — freelist pop with cascading stats → recursive buddy split → aligned bump.
fn alloc_at_tier<M: Mem>(m: &mut M, sc: SizeClass, tier: usize) -> u32 {
    let head_offset = freelist_head(m, sc, tier);

    if head_offset != 0 {
        // Pop from freelist; cascade stats forward into the new head.
        let next = m.read_u32(head_offset + FB_NEXT_PTR);
        set_freelist_head(m, sc, tier, next);
        if next != 0 {
            let len = m.read_u32(head_offset + FB_FREELIST_LEN);
            let reuse = m.read_u32(head_offset + FB_REUSE_COUNT);
            let split = m.read_u32(head_offset + FB_SPLIT_COUNT);
            let merge = m.read_u32(head_offset + FB_MERGE_COUNT);
            m.write_u32(next + FB_FREELIST_LEN, len - 1);
            m.write_u32(next + FB_REUSE_COUNT, reuse + 1);
            m.write_u32(next + FB_SPLIT_COUNT, split);
            m.write_u32(next + FB_MERGE_COUNT, merge);
        }
        bump_counter(m, H_ALLOC_COUNT);
        return head_offset;
    }

    // Freelist empty — try buddy split from the next tier up.
    if tier + 1 < NUM_TIERS {
        let parent_offset = alloc_at_tier(m, sc, tier + 1);
        if parent_offset != 0 {
            let child_size = block_size(sc, tier_to_capacity(tier));
            let second_child = parent_offset + child_size;
            // Fresh split: push second child without a merge attempt.
            push_to_freelist(m, second_child, sc, tier, false);
            let new_head = freelist_head(m, sc, tier);
            if new_head != 0 {
                let split = m.read_u32(new_head + FB_SPLIT_COUNT);
                m.write_u32(new_head + FB_SPLIT_COUNT, split + 1);
            }
            return parent_offset;
        }
    }

    // Bump allocate, 8-byte aligned, growing memory as needed.
    let size = block_size(sc, tier_to_capacity(tier));
    let aligned = (m.read_u32(H_BUMP_PTR) + 7) & !7u32;
    let new_bump = aligned + size;
    if new_bump > m.size() && !m.grow_to(new_bump) {
        return 0; // OOM sentinel
    }
    m.write_u32(H_BUMP_PTR, new_bump);
    bump_counter(m, H_ALLOC_COUNT);
    aligned
}

#[inline]
fn bump_counter<M: Mem>(m: &mut M, off: u32) {
    let v = m.read_u32(off);
    m.write_u32(off, v + 1);
}

/// `freeAtTier` — address-based neighbor merge (right, then left), else freelist push.
fn free_at_tier<M: Mem>(m: &mut M, offset: u32, sc: SizeClass, tier: usize) {
    if tier + 1 < NUM_TIERS {
        let size = block_size(sc, tier_to_capacity(tier));
        let right = offset + size;
        if find_and_remove_by_offset(m, sc, tier, right) {
            free_at_tier_with_merge(m, offset, sc, tier + 1);
            return;
        }
        if offset >= size {
            let left = offset - size;
            if find_and_remove_by_offset(m, sc, tier, left) {
                free_at_tier_with_merge(m, left, sc, tier + 1);
                return;
            }
        }
    }
    push_to_freelist(m, offset, sc, tier, false);
}

/// `freeAtTierWithMerge` — cascade merges upward; final push counts the merge.
fn free_at_tier_with_merge<M: Mem>(m: &mut M, offset: u32, sc: SizeClass, tier: usize) {
    if tier + 1 < NUM_TIERS {
        let size = block_size(sc, tier_to_capacity(tier));
        let right = offset + size;
        if find_and_remove_by_offset(m, sc, tier, right) {
            free_at_tier_with_merge(m, offset, sc, tier + 1);
            return;
        }
        if offset >= size {
            let left = offset - size;
            if find_and_remove_by_offset(m, sc, tier, left) {
                free_at_tier_with_merge(m, left, sc, tier + 1);
                return;
            }
        }
    }
    push_to_freelist(m, offset, sc, tier, true);
}

/// `pushToFreelist` — write a FreeBlock into the freed memory with cascading stats.
fn push_to_freelist<M: Mem>(m: &mut M, offset: u32, sc: SizeClass, tier: usize, is_merge: bool) {
    let old_head = freelist_head(m, sc, tier);
    m.write_u32(offset + FB_NEXT_PTR, old_head);
    if old_head != 0 {
        let len = m.read_u32(old_head + FB_FREELIST_LEN);
        let reuse = m.read_u32(old_head + FB_REUSE_COUNT);
        let split = m.read_u32(old_head + FB_SPLIT_COUNT);
        let merge = m.read_u32(old_head + FB_MERGE_COUNT);
        m.write_u32(offset + FB_FREELIST_LEN, len + 1);
        m.write_u32(offset + FB_REUSE_COUNT, reuse);
        m.write_u32(offset + FB_SPLIT_COUNT, split);
        m.write_u32(offset + FB_MERGE_COUNT, merge + u32::from(is_merge));
    } else {
        m.write_u32(offset + FB_FREELIST_LEN, 1);
        m.write_u32(offset + FB_REUSE_COUNT, 0);
        m.write_u32(offset + FB_SPLIT_COUNT, 0);
        m.write_u32(offset + FB_MERGE_COUNT, u32::from(is_merge));
    }
    set_freelist_head(m, sc, tier, offset);
    bump_counter(m, H_FREE_COUNT);
}

/// `findAndRemoveByOffset` — O(n) freelist scan; head removal cascades stats.
fn find_and_remove_by_offset<M: Mem>(m: &mut M, sc: SizeClass, tier: usize, target: u32) -> bool {
    let head_offset = freelist_head(m, sc, tier);
    if head_offset == 0 {
        return false;
    }
    if head_offset == target {
        let next = m.read_u32(head_offset + FB_NEXT_PTR);
        set_freelist_head(m, sc, tier, next);
        if next != 0 {
            let len = m.read_u32(head_offset + FB_FREELIST_LEN);
            let reuse = m.read_u32(head_offset + FB_REUSE_COUNT);
            let split = m.read_u32(head_offset + FB_SPLIT_COUNT);
            let merge = m.read_u32(head_offset + FB_MERGE_COUNT);
            m.write_u32(next + FB_FREELIST_LEN, len - 1);
            m.write_u32(next + FB_REUSE_COUNT, reuse);
            m.write_u32(next + FB_SPLIT_COUNT, split);
            m.write_u32(next + FB_MERGE_COUNT, merge);
        }
        return true;
    }
    let mut prev = head_offset;
    let mut current = m.read_u32(head_offset + FB_NEXT_PTR);
    while current != 0 {
        if current == target {
            let next = m.read_u32(current + FB_NEXT_PTR);
            m.write_u32(prev + FB_NEXT_PTR, next);
            // Update freelist length in the (unchanged) head.
            let head = freelist_head(m, sc, tier);
            let len = m.read_u32(head + FB_FREELIST_LEN);
            m.write_u32(head + FB_FREELIST_LEN, len - 1);
            return true;
        }
        prev = current;
        current = m.read_u32(current + FB_NEXT_PTR);
    }
    false
}

/// Effective allocation tier for a request.
///
/// DELIBERATE DEVIATION from allocator.zig: a freed block is overlaid with a
/// 20-byte `FreeBlock`, but `col_1b` blocks at capacity 8/16 are only 9/18
/// bytes — in the Zig, freeing/splitting those silently corrupts the adjacent
/// split-sibling's FreeBlock (latent memory-corruption bug, caught here by
/// Rust's checked arithmetic). We clamp such requests up to the first tier
/// whose block size fits a FreeBlock; buddy doubling stays intact, callers
/// still get a block valid for the requested capacity (just over-provisioned).
/// Affects only col_1b cap 8 → tier 2 (36B) and cap 16 → tier 2.
#[inline]
fn effective_tier(sc: SizeClass, tier: usize) -> usize {
    let mut t = tier;
    while (block_size(sc, tier_to_capacity(t)) as usize) < FREE_BLOCK_SIZE && t + 1 < NUM_TIERS {
        t += 1;
    }
    t
}

/// The byte extent a request actually occupies (post tier-clamping); use this
/// for adjacency/overlap reasoning instead of [`block_size`].
pub fn effective_block_size(sc: SizeClass, capacity: u32) -> u32 {
    block_size(sc, tier_to_capacity(effective_tier(sc, capacity_to_tier(capacity))))
}

/// `allocWithCapacity`.
pub fn alloc_with_capacity<M: Mem>(m: &mut M, sc: SizeClass, capacity: u32) -> u32 {
    alloc_at_tier(m, sc, effective_tier(sc, capacity_to_tier(capacity)))
}

/// `freeWithCapacity`.
pub fn free_with_capacity<M: Mem>(m: &mut M, offset: u32, sc: SizeClass, capacity: u32) {
    free_at_tier(m, offset, sc, effective_tier(sc, capacity_to_tier(capacity)));
}

// --- Identity blocks (fixed 128B, separate freelist, no buddy) ---

/// `allocIdentity` — pop identity freelist or aligned bump. NOTE: faithfully
/// ports the Zig, including its quirk that the identity bump path does NOT grow
/// memory (native Vec backends should over-provision or rely on `grow_to` in
/// `alloc_at_tier` having grown enough; the wasm host sizes memory up front).
fn alloc_identity_block<M: Mem>(m: &mut M) -> u32 {
    let head_offset = m.read_u32(H_FREELIST_IDENTITY);
    if head_offset != 0 {
        let next = m.read_u32(head_offset + FB_NEXT_PTR);
        m.write_u32(H_FREELIST_IDENTITY, next);
        if next != 0 {
            let reuse = m.read_u32(next + FB_REUSE_COUNT);
            m.write_u32(next + FB_REUSE_COUNT, reuse + 1);
        }
        bump_counter(m, H_ALLOC_COUNT);
        return head_offset;
    }
    let aligned = (m.read_u32(H_BUMP_PTR) + 7) & !7u32;
    let new_bump = aligned + IDENTITY_SIZE as u32;
    // Deviation from Zig (which cannot fail here and would trap): grow if the
    // backend supports it, so native Vec arenas stay safe.
    if new_bump > m.size() && !m.grow_to(new_bump) {
        return 0;
    }
    m.write_u32(H_BUMP_PTR, new_bump);
    bump_counter(m, H_ALLOC_COUNT);
    aligned
}

/// `freeIdentity`.
pub fn free_identity<M: Mem>(m: &mut M, offset: u32) {
    let old_head = m.read_u32(H_FREELIST_IDENTITY);
    m.write_u32(offset + FB_NEXT_PTR, old_head);
    if old_head != 0 {
        let len = m.read_u32(old_head + FB_FREELIST_LEN);
        let reuse = m.read_u32(old_head + FB_REUSE_COUNT);
        let split = m.read_u32(old_head + FB_SPLIT_COUNT);
        let merge = m.read_u32(old_head + FB_MERGE_COUNT);
        m.write_u32(offset + FB_FREELIST_LEN, len + 1);
        m.write_u32(offset + FB_REUSE_COUNT, reuse);
        m.write_u32(offset + FB_SPLIT_COUNT, split);
        m.write_u32(offset + FB_MERGE_COUNT, merge);
    } else {
        m.write_u32(offset + FB_FREELIST_LEN, 1);
        m.write_u32(offset + FB_REUSE_COUNT, 0);
        m.write_u32(offset + FB_SPLIT_COUNT, 0);
        m.write_u32(offset + FB_MERGE_COUNT, 0);
    }
    m.write_u32(H_FREELIST_IDENTITY, offset);
    bump_counter(m, H_FREE_COUNT);
}

/// `alloc_identity_root_for_js_write` — returns `(identity_offset << 32) | trace_id_field_offset`,
/// 0 if the trace id is too long (or OOM).
pub fn alloc_identity_root_for_js_write<M: Mem>(m: &mut M, trace_id_len: u32) -> u64 {
    if trace_id_len > ID_TRACE_ID_MAX {
        return 0;
    }
    let offset = alloc_identity_block(m);
    if offset == 0 {
        return 0;
    }
    let span_id = m.read_u32(H_SPAN_ID_COUNTER) + 1;
    m.write_u32(H_SPAN_ID_COUNTER, span_id);
    m.write_u32(offset + ID_SPAN_ID, span_id);
    m.write_u32(offset + ID_WRITE_INDEX, 0);
    m.write_u8(offset + ID_TRACE_ID_LEN, trace_id_len as u8);
    let trace_id_field_offset = offset + ID_TRACE_ID;
    (u64::from(offset) << 32) | u64::from(trace_id_field_offset)
}

/// `alloc_identity_child`.
pub fn alloc_identity_child<M: Mem>(m: &mut M) -> u32 {
    let offset = alloc_identity_block(m);
    if offset == 0 {
        return 0;
    }
    let span_id = m.read_u32(H_SPAN_ID_COUNTER) + 1;
    m.write_u32(H_SPAN_ID_COUNTER, span_id);
    m.write_u32(offset + ID_SPAN_ID, span_id);
    m.write_u32(offset + ID_WRITE_INDEX, 0);
    m.write_u8(offset + ID_TRACE_ID_LEN, 0); // child uses parent's trace_id
    offset
}

pub fn read_identity_span_id<M: Mem>(m: &M, identity_ptr: u32) -> u32 {
    m.read_u32(identity_ptr + ID_SPAN_ID)
}

pub fn read_identity_trace_id_len<M: Mem>(m: &M, identity_ptr: u32) -> u32 {
    u32::from(m.read_u8(identity_ptr + ID_TRACE_ID_LEN))
}

pub fn identity_trace_id_ptr(identity_ptr: u32) -> u32 {
    identity_ptr + ID_TRACE_ID
}

pub fn read_write_index<M: Mem>(m: &M, identity_ptr: u32) -> u32 {
    m.read_u32(identity_ptr + ID_WRITE_INDEX)
}

// --- Thread id / stats ---

pub fn set_thread_id<M: Mem>(m: &mut M, high: u32, low: u32) {
    m.write_u64(H_THREAD_ID, (u64::from(high) << 32) | u64::from(low));
    m.write_u8(H_THREAD_ID_SET, 1);
}

pub fn thread_id<M: Mem>(m: &M) -> u64 {
    m.read_u64(H_THREAD_ID)
}

pub fn is_thread_id_set<M: Mem>(m: &M) -> u8 {
    m.read_u8(H_THREAD_ID_SET)
}

pub fn bump_ptr<M: Mem>(m: &M) -> u32 {
    m.read_u32(H_BUMP_PTR)
}

pub fn alloc_count<M: Mem>(m: &M) -> u32 {
    m.read_u32(H_ALLOC_COUNT)
}

pub fn free_count<M: Mem>(m: &M) -> u32 {
    m.read_u32(H_FREE_COUNT)
}

pub fn span_id_counter<M: Mem>(m: &M) -> u32 {
    m.read_u32(H_SPAN_ID_COUNTER)
}

pub fn debug_freelist_head<M: Mem>(m: &M, sc: SizeClass, capacity: u32) -> u32 {
    freelist_head(m, sc, effective_tier(sc, capacity_to_tier(capacity)))
}

pub fn debug_next_ptr<M: Mem>(m: &M, offset: u32) -> u32 {
    if offset == 0 { 0 } else { m.read_u32(offset + FB_NEXT_PTR) }
}

fn freelist_head_stat<M: Mem>(m: &M, sc: SizeClass, capacity: u32, field: u32) -> u32 {
    let head = freelist_head(m, sc, effective_tier(sc, capacity_to_tier(capacity)));
    if head == 0 { 0 } else { m.read_u32(head + field) }
}

pub fn freelist_len<M: Mem>(m: &M, sc: SizeClass, capacity: u32) -> u32 {
    freelist_head_stat(m, sc, capacity, FB_FREELIST_LEN)
}

pub fn freelist_reuse_count<M: Mem>(m: &M, sc: SizeClass, capacity: u32) -> u32 {
    freelist_head_stat(m, sc, capacity, FB_REUSE_COUNT)
}

pub fn freelist_split_count<M: Mem>(m: &M, sc: SizeClass, capacity: u32) -> u32 {
    freelist_head_stat(m, sc, capacity, FB_SPLIT_COUNT)
}

pub fn freelist_merge_count<M: Mem>(m: &M, sc: SizeClass, capacity: u32) -> u32 {
    freelist_head_stat(m, sc, capacity, FB_MERGE_COUNT)
}

// --- TraceRoot + timestamps (time passed in; wasm exports fetch host clocks) ---

/// `init_trace_root` with the two host clock samples supplied by the caller.
pub fn init_trace_root<M: Mem>(m: &mut M, trace_root_ptr: u32, wall_ms: f64, monotonic_ms: f64) {
    let ms = wall_ms as i64;
    m.write_i64(trace_root_ptr + TR_WALL_CLOCK_NANOS, ms * 1_000_000);
    m.write_f64(trace_root_ptr + TR_MONOTONIC_MS, monotonic_ms);
}

/// `getTimestampNanos` with `performanceNow()` supplied as `current_ms`.
pub fn timestamp_nanos<M: Mem>(m: &M, trace_root_ptr: u32, current_ms: f64) -> i64 {
    let wall = m.read_i64(trace_root_ptr + TR_WALL_CLOCK_NANOS);
    let mono = m.read_f64(trace_root_ptr + TR_MONOTONIC_MS);
    let elapsed_nanos = ((current_ms - mono) * 1_000_000.0) as i64;
    wall + elapsed_nanos
}

// --- Span lifecycle ---
// Span system layout: [timestamp: i64 × capacity][entry_type: u8 × capacity]

/// `span_start` — row 0 = span-start, row 1 pre-armed span-exception, write_index = 2.
pub fn span_start<M: Mem>(m: &mut M, system_ptr: u32, identity_ptr: u32, trace_root_ptr: u32, capacity: u32, current_ms: f64) {
    let ts = timestamp_nanos(m, trace_root_ptr, current_ms);
    m.write_i64(system_ptr, ts);
    m.write_u8(system_ptr + capacity * 8, ENTRY_TYPE_SPAN_START);
    m.write_i64(system_ptr + 8, 0);
    m.write_u8(system_ptr + capacity * 8 + 1, ENTRY_TYPE_SPAN_EXCEPTION);
    m.write_u32(identity_ptr + ID_WRITE_INDEX, 2);
}

/// `span_end_ok` / `span_end_err` share the row-1 completion write.
pub fn span_end<M: Mem>(m: &mut M, system_ptr: u32, trace_root_ptr: u32, capacity: u32, entry_type: u8, current_ms: f64) {
    m.write_u8(system_ptr + capacity * 8 + 1, entry_type);
    let ts = timestamp_nanos(m, trace_root_ptr, current_ms);
    m.write_i64(system_ptr + 8, ts);
}

/// `write_log_entry` — bump write_index, stamp row, return the row index written.
pub fn write_log_entry<M: Mem>(m: &mut M, system_ptr: u32, identity_ptr: u32, trace_root_ptr: u32, entry_type: u8, capacity: u32, current_ms: f64) -> u32 {
    let idx = m.read_u32(identity_ptr + ID_WRITE_INDEX);
    let ts = timestamp_nanos(m, trace_root_ptr, current_ms);
    m.write_i64(system_ptr + idx * 8, ts);
    m.write_u8(system_ptr + capacity * 8 + idx, entry_type);
    m.write_u32(identity_ptr + ID_WRITE_INDEX, idx + 1);
    idx
}

// --- Column IO (null bitmap precedes values, sharing one block) ---

/// `write_col_f64` — lazily allocates the column block on first write.
pub fn write_col_f64<M: Mem>(m: &mut M, col_offset: u32, row_idx: u32, value: f64, capacity: u32) -> u32 {
    let offset = if col_offset == 0 { alloc_with_capacity(m, SizeClass::Col8B, capacity) } else { col_offset };
    if offset == 0 {
        return 0;
    }
    let null_bitmap_size = (capacity + 7) >> 3;
    m.write_f64(offset + null_bitmap_size + row_idx * 8, value);
    set_valid_bit(m, offset, row_idx);
    offset
}

/// `write_col_u32`.
pub fn write_col_u32<M: Mem>(m: &mut M, col_offset: u32, row_idx: u32, value: u32, capacity: u32) -> u32 {
    let offset = if col_offset == 0 { alloc_with_capacity(m, SizeClass::Col4B, capacity) } else { col_offset };
    if offset == 0 {
        return 0;
    }
    let null_bitmap_size = (capacity + 7) >> 3;
    m.write_u32(offset + null_bitmap_size + row_idx * 4, value);
    set_valid_bit(m, offset, row_idx);
    offset
}

/// `write_col_u8`.
pub fn write_col_u8<M: Mem>(m: &mut M, col_offset: u32, row_idx: u32, value: u8, capacity: u32) -> u32 {
    let offset = if col_offset == 0 { alloc_with_capacity(m, SizeClass::Col1B, capacity) } else { col_offset };
    if offset == 0 {
        return 0;
    }
    let null_bitmap_size = (capacity + 7) >> 3;
    m.write_u8(offset + null_bitmap_size + row_idx, value);
    set_valid_bit(m, offset, row_idx);
    offset
}

#[inline]
fn set_valid_bit<M: Mem>(m: &mut M, block_offset: u32, row_idx: u32) {
    let byte_off = block_offset + (row_idx >> 3);
    let byte = m.read_u8(byte_off);
    m.write_u8(byte_off, byte | (1u8 << (row_idx & 7)));
}

pub fn read_timestamp<M: Mem>(m: &M, system_ptr: u32, row_idx: u32) -> i64 {
    m.read_i64(system_ptr + row_idx * 8)
}

pub fn read_entry_type<M: Mem>(m: &M, system_ptr: u32, row_idx: u32, capacity: u32) -> u8 {
    m.read_u8(system_ptr + capacity * 8 + row_idx)
}

pub fn read_col_f64<M: Mem>(m: &M, col_offset: u32, row_idx: u32, capacity: u32) -> f64 {
    let null_bitmap_size = (capacity + 7) >> 3;
    m.read_f64(col_offset + null_bitmap_size + row_idx * 8)
}

pub fn read_col_is_valid<M: Mem>(m: &M, col_offset: u32, row_idx: u32) -> u8 {
    let byte = m.read_u8(col_offset + (row_idx >> 3));
    u8::from(byte & (1u8 << (row_idx & 7)) != 0)
}
