//! # lmao-arena
//!
//! Arena implementation for the tiered-freelist allocator used by the lmao WASM
//! host integration.
//!
//! Layout facts pinned here MUST stay byte-identical to the TypeScript/WASM ABI
//! (the TS host reads these structs from linear memory):
//! - `Header`   = 192 bytes (3 cache lines), 28 freelist heads + identity freelist
//! - `Identity` = 128 bytes (write_index, span_id, trace_id_len, trace_id[119])
//! - `TraceRoot` = 16 bytes (wall_clock_nanos i64, monotonic_ms f64)
//! - `FreeBlock` = 20 bytes, overlaid on freed memory (zero-overhead freelist,
//!   cascading stats: each push/pop recomputes aggregates into the head node)
//! - Tiers: 8,16,32,64,128,256,512 (7); size classes: span_system(9B/row),
//!   col_1b, col_4b, col_8b (null-bitmap + values sharing one block); identity is
//!   a separate fixed-size class.
//! - Alloc: freelist pop → recursive buddy split from tier+1 → bump (64-byte aligned).
//! - Free: push + address-based buddy-merge cascade (right buddy = offset + size).
//! - Sentinel conventions: offset 0 = null/none; OOM returns 0.

/// Minimum span-buffer capacity (rows) — tier 0.
pub const MIN_CAPACITY: u32 = 8;
/// Maximum arena capacity tier (rows). The arena tops out at 512; the per-schema
/// ratchet in lmao-core allows 1024 for the pure-Rust heap path. Do not "fix" one
/// to match the other.
pub const MAX_CAPACITY: u32 = 512;
pub const NUM_TIERS: usize = 7;
pub const NUM_SIZE_CLASSES: usize = 4;
pub const NUM_FREELISTS: usize = NUM_SIZE_CLASSES * NUM_TIERS; // 28

pub const HEADER_SIZE: usize = 192;
pub const IDENTITY_SIZE: usize = 128;
pub const TRACE_ROOT_SIZE: usize = 16;
pub const FREE_BLOCK_SIZE: usize = 20;

/// Size classes, with discriminants shared by the TypeScript/WASM ABI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SizeClass {
    SpanSystem = 0,
    Col1B = 1,
    Col4B = 2,
    Col8B = 3,
}

/// Arena header at offset 0; field order and padding are ABI, verified by the
/// const asserts below.
#[repr(C)]
#[derive(Debug)]
pub struct Header {
    pub bump_ptr: u32,
    pub span_id_counter: u32,
    pub alloc_count: u32,
    pub free_count: u32,
    pub freelist_identity: u32,
    _pad0: u32, // align thread_id to 8
    pub thread_id: u64,
    pub freelists: [u32; NUM_FREELISTS],
    pub thread_id_set: u8,
    _pad1: [u8; 3],
    pub freelist_exact: u32,
    _reserved: [u8; 40],
}

/// Per-span identity block in the linear-memory ABI.
#[repr(C)]
#[derive(Debug)]
pub struct Identity {
    pub write_index: u32,
    pub span_id: u32,
    pub trace_id_len: u8,
    pub trace_id: [u8; 119],
}

/// Per-trace timing anchor in the linear-memory ABI.
#[repr(C)]
#[derive(Debug)]
pub struct TraceRoot {
    pub wall_clock_nanos: i64,
    pub monotonic_ms: f64,
}

/// Freelist node overlaid on freed block memory in the linear-memory ABI.
#[repr(C)]
#[derive(Debug)]
pub struct FreeBlock {
    pub next_ptr: u32,
    pub freelist_len: u32,
    pub reuse_count: u32,
    pub split_count: u32,
    pub merge_count: u32,
}

const _: () = assert!(size_of::<Header>() == HEADER_SIZE);
const _: () = assert!(size_of::<Identity>() == IDENTITY_SIZE);
const _: () = assert!(size_of::<TraceRoot>() == TRACE_ROOT_SIZE);
const _: () = assert!(size_of::<FreeBlock>() == FREE_BLOCK_SIZE);

/// Capacity (power of 2, 8..=512) → tier index 0..=6.
#[inline]
pub const fn capacity_to_tier(capacity: u32) -> Option<usize> {
    match capacity {
        8 => Some(0),
        16 => Some(1),
        32 => Some(2),
        64 => Some(3),
        128 => Some(4),
        256 => Some(5),
        512 => Some(6),
        _ => None,
    }
}

#[inline]
pub fn tier_to_capacity(tier: usize) -> u32 {
    MIN_CAPACITY << tier
}

/// Capacities whose rows the arena can address: any row count the layout planner
/// may choose, up to [`MAX_CAPACITY`].
///
/// This is deliberately *not* [`capacity_to_tier`]. Tiers describe the power-of-two
/// buddy freelists behind `alloc_with_capacity`/`free_with_capacity` and nothing
/// else. Span superblocks are served by the exact-extent allocator, and lifecycle
/// and column row writes index `capacity` directly, so gating them on tier
/// membership would refuse every planned non-power-of-two capacity — which the
/// runtime hint's initial-capacity field routinely produces.
#[inline]
pub const fn capacity_addresses_rows(capacity: u32) -> bool {
    capacity != 0 && capacity <= MAX_CAPACITY
}

/// Bytes in a column validity bitmap for `capacity` rows.
#[inline]
pub const fn null_bitmap_bytes(capacity: u32) -> u32 {
    capacity.div_ceil(8)
}

/// Block byte size for a size class at a capacity (null bitmap + values share the
/// block for column classes; span_system is 9 bytes/row: i64 timestamp + u8 entry_type).
#[inline]
pub fn block_size(sc: SizeClass, capacity: u32) -> u32 {
    let null_bitmap = null_bitmap_bytes(capacity);
    match sc {
        SizeClass::SpanSystem => capacity * 9,
        SizeClass::Col1B => null_bitmap + capacity,
        SizeClass::Col4B => null_bitmap + capacity * 4,
        SizeClass::Col8B => null_bitmap + capacity * 8,
    }
}

/// Byte offset into the arena. 0 is the null sentinel (header lives at 0, so no
/// valid block ever starts there).
pub type Offset = u32;

pub mod raw;
pub use raw::Mem;

/// `Vec<u8>`-backed [`raw::Mem`] — the native linear-memory backend. The
/// constructor reserves the complete admitted region; allocations return the
/// offset-0 OOM sentinel rather than reallocating the arena under load.
#[derive(Debug)]
pub struct VecMem(Vec<u8>);

impl VecMem {
    /// A zeroed backing of `initial_bytes` with an initialized header
    /// (used by [`Arena`] and the lmao-wasm native-fallback arena).
    pub fn with_zeroed(initial_bytes: usize) -> Self {
        assert!(initial_bytes >= HEADER_SIZE);
        let mut mem = VecMem(vec![0u8; initial_bytes]);
        raw::init(&mut mem);
        mem
    }
}

impl raw::Mem for VecMem {
    #[inline]
    fn size(&self) -> u32 {
        self.0.len() as u32
    }
    fn grow_to(&mut self, new_size: u32) -> bool {
        new_size <= self.size()
    }
    #[inline]
    fn fill(&mut self, off: u32, len: u32, value: u8) {
        self.0[off as usize..(off + len) as usize].fill(value);
    }
    #[inline]
    fn read_u8(&self, off: u32) -> u8 {
        self.0[off as usize]
    }
    #[inline]
    fn write_u8(&mut self, off: u32, v: u8) {
        self.0[off as usize] = v;
    }
    #[inline]
    fn read_u32(&self, off: u32) -> u32 {
        u32::from_le_bytes(self.0[off as usize..off as usize + 4].try_into().unwrap())
    }
    #[inline]
    fn write_u32(&mut self, off: u32, v: u32) {
        self.0[off as usize..off as usize + 4].copy_from_slice(&v.to_le_bytes());
    }
    #[inline]
    fn read_u64(&self, off: u32) -> u64 {
        u64::from_le_bytes(self.0[off as usize..off as usize + 8].try_into().unwrap())
    }
    #[inline]
    fn write_u64(&mut self, off: u32, v: u64) {
        self.0[off as usize..off as usize + 8].copy_from_slice(&v.to_le_bytes());
    }
}

/// The arena over one owned linear-memory region (native path). The `lmao-wasm`
/// crate reuses the identical [`raw`] logic over WASM linear memory.
#[derive(Debug)]
pub struct Arena {
    mem: VecMem,
}

impl Arena {
    /// Initialize with a zeroed header and bump pointer at `HEADER_SIZE` by
    /// calling the idempotent raw allocator initializer.
    pub fn new(initial_bytes: usize) -> Self {
        assert!(initial_bytes >= HEADER_SIZE);
        let mut mem = VecMem(vec![0u8; initial_bytes]);
        raw::init(&mut mem);
        Self { mem }
    }

    /// Allocate a block: freelist pop → buddy split → 8-byte-aligned bump (growing
    /// memory as needed). Returns 0 on OOM (sentinel convention preserved).
    pub fn alloc(&mut self, sc: SizeClass, capacity: u32) -> Offset {
        raw::alloc_with_capacity(&mut self.mem, sc, capacity)
    }

    /// Free a block: attempt address-based buddy merge cascade, else push to the
    /// tier freelist writing a `FreeBlock` into the freed memory.
    pub fn free(&mut self, offset: Offset, sc: SizeClass, capacity: u32) {
        raw::free_with_capacity(&mut self.mem, offset, sc, capacity);
    }

    /// Fixed-size identity block alloc (separate freelist, mirrors `alloc_identity_*`).
    /// Allocates a CHILD identity (no trace id); use [`raw::alloc_identity_root_for_js_write`]
    /// via [`Arena::mem_mut`] for the root+trace-id path.
    pub fn alloc_identity(&mut self) -> Offset {
        raw::alloc_identity_child(&mut self.mem)
    }

    pub fn free_identity(&mut self, offset: Offset) {
        raw::free_identity(&mut self.mem, offset);
    }

    /// Direct access to the backing memory for the `raw` free functions
    /// (span lifecycle, column IO, stats).
    pub fn mem_mut(&mut self) -> &mut VecMem {
        &mut self.mem
    }

    pub fn mem(&self) -> &VecMem {
        &self.mem
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.mem.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        false // header always present
    }

    pub fn bump_ptr(&self) -> u32 {
        raw::bump_ptr(&self.mem)
    }

    pub fn alloc_count(&self) -> u32 {
        raw::alloc_count(&self.mem)
    }

    pub fn free_count(&self) -> u32 {
        raw::free_count(&self.mem)
    }

    pub fn freelist_len(&self, sc: SizeClass, capacity: u32) -> u32 {
        raw::freelist_len(&self.mem, sc, capacity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::offset_of;

    #[test]
    fn tier_mapping_rejects_out_of_domain_capacities() {
        assert_eq!(capacity_to_tier(8), Some(0));
        assert_eq!(capacity_to_tier(512), Some(6));
        assert_eq!(capacity_to_tier(0), None);
        assert_eq!(capacity_to_tier(24), None);
        assert_eq!(capacity_to_tier(1024), None);
        for tier in 0..NUM_TIERS {
            assert_eq!(capacity_to_tier(tier_to_capacity(tier)), Some(tier));
        }
    }

    #[test]
    fn row_addressing_domain_is_wider_than_the_tier_domain() {
        // Non-power-of-two capacities are addressable rows even though no buddy
        // freelist tier serves them; only the arena ceiling is out of domain.
        for capacity in [1, 2, 6, 17, 24, 100, MAX_CAPACITY] {
            assert!(capacity_addresses_rows(capacity), "capacity {capacity}");
        }
        assert!(!capacity_addresses_rows(0));
        assert!(!capacity_addresses_rows(MAX_CAPACITY + 1));
        assert!(!capacity_addresses_rows(1024));
    }

    #[test]
    fn block_sizes_match_zig_formulas() {
        assert_eq!(block_size(SizeClass::SpanSystem, 8), 72);
        assert_eq!(block_size(SizeClass::Col1B, 8), 1 + 8);
        assert_eq!(block_size(SizeClass::Col4B, 8), 1 + 32);
        assert_eq!(block_size(SizeClass::Col8B, 8), 1 + 64);
    }

    #[test]
    fn abi_field_offsets_are_frozen() {
        assert_eq!(offset_of!(Header, bump_ptr), 0);
        assert_eq!(offset_of!(Header, span_id_counter), 4);
        assert_eq!(offset_of!(Header, alloc_count), 8);
        assert_eq!(offset_of!(Header, free_count), 12);
        assert_eq!(offset_of!(Header, freelist_identity), 16);
        assert_eq!(offset_of!(Header, thread_id), 24);
        assert_eq!(offset_of!(Header, freelists), 32);
        assert_eq!(offset_of!(Header, thread_id_set), 144);
        assert_eq!(offset_of!(Header, freelist_exact), 148);
        assert_eq!(offset_of!(Identity, write_index), 0);
        assert_eq!(offset_of!(Identity, span_id), 4);
        assert_eq!(offset_of!(Identity, trace_id_len), 8);
        assert_eq!(offset_of!(Identity, trace_id), 9);
        assert_eq!(offset_of!(TraceRoot, wall_clock_nanos), 0);
        assert_eq!(offset_of!(TraceRoot, monotonic_ms), 8);
        assert_eq!(offset_of!(FreeBlock, next_ptr), 0);
        assert_eq!(offset_of!(FreeBlock, freelist_len), 4);
        assert_eq!(offset_of!(FreeBlock, reuse_count), 8);
        assert_eq!(offset_of!(FreeBlock, split_count), 12);
        assert_eq!(offset_of!(FreeBlock, merge_count), 16);
    }

    #[test]
    fn admitted_native_region_returns_oom_without_growth() {
        let mut arena = Arena::new(256);
        assert_eq!(arena.alloc(SizeClass::SpanSystem, 64), 0);
        assert_eq!(arena.mem().size(), 256);
    }

    #[test]
    fn new_arena_has_header_and_bump_past_it() {
        let a = Arena::new(4096);
        assert_eq!(a.bump_ptr(), HEADER_SIZE as u32);
    }
}
