//! # lmao-wasm
//!
//! Drop-in replacement for `dist/allocator.wasm` (built from
//! `packages/lmao/src/lib/wasm/allocator.zig`). The TS host
//! (`packages/lmao/src/lib/wasm/wasmAllocator.ts`) does raw
//! `WebAssembly.instantiate` with manual `env` imports — NO wasm-bindgen here;
//! we match that low-level ABI exactly:
//!
//! - JS owns the memory (`import_memory` semantics): built with
//!   `-C link-arg=--import-memory` (see .cargo/config.toml in the workspace).
//! - Imports: `env.performanceNow() -> f64`, `env.dateNow() -> f64` (the only host calls).
//! - Sentinel returns: offset 0 = null/OOM; no Result marshaling across the boundary.
//! - Packed u64 convention preserved: `alloc_identity_root_for_js_write` returns
//!   `(identity_offset << 32) | trace_id_field_offset`.
//! - Export names mirror allocator.zig's export list 1:1, including debug exports.
//!
//! All logic lives in `lmao_arena::raw`, generic over [`lmao_arena::Mem`]; this
//! crate only supplies the linear-memory backend ([`WasmMem`]: absolute-offset
//! loads/stores + `memory.size`/`memory.grow`) and fetches the host clocks.
//! On non-wasm targets the exports compile against an in-process static arena so
//! `cargo test --workspace` can exercise the ABI surface natively.

#![allow(clippy::missing_safety_doc)]

use lmao_arena::SizeClass;
#[cfg(target_arch = "wasm32")]
use lmao_arena::raw::Mem;
use lmao_arena::raw::{self};

// --- host imports (env module) ---
#[cfg(target_arch = "wasm32")]
#[link(wasm_import_module = "env")]
unsafe extern "C" {
    #[link_name = "performanceNow"]
    fn host_performance_now() -> f64;
    #[link_name = "dateNow"]
    fn host_date_now() -> f64;
}

#[cfg(not(target_arch = "wasm32"))]
unsafe fn host_performance_now() -> f64 {
    0.0
}
#[cfg(not(target_arch = "wasm32"))]
unsafe fn host_date_now() -> f64 {
    0.0
}

fn performance_now() -> f64 {
    unsafe { host_performance_now() }
}

fn date_now() -> f64 {
    unsafe { host_date_now() }
}

/// [`Mem`] over WASM linear memory: absolute byte offsets from address 0
/// (valid on wasm32 — memory starts at 0, mirroring Zig's `allowzero`), with
/// `memory.size`/`memory.grow` for growth. All accesses are unaligned-safe
/// (`read_unaligned`/`write_unaligned`) because column values sit at arbitrary
/// byte offsets after the null bitmap.
#[cfg(target_arch = "wasm32")]
struct WasmMem;

/// Absolute offset → pointer, laundered through `black_box` so LLVM cannot
/// prove offset 0 is a null pointer: the header legitimately lives at address 0
/// of WASM linear memory (Zig needed `allowzero` for the same reason; without
/// this, LLVM folds header writes into `unreachable` and `reset()` traps).
#[cfg(target_arch = "wasm32")]
#[inline(always)]
fn laundered(off: u32) -> *mut u8 {
    core::hint::black_box(off as usize) as *mut u8
}

#[cfg(target_arch = "wasm32")]
impl Mem for WasmMem {
    #[inline]
    fn size(&self) -> u32 {
        (core::arch::wasm32::memory_size(0) as u32).saturating_mul(65536)
    }
    fn grow_to(&mut self, new_size: u32) -> bool {
        let current = self.size();
        if new_size <= current {
            return true;
        }
        let pages_needed = ((new_size - current) as usize).div_ceil(65536);
        core::arch::wasm32::memory_grow(0, pages_needed) != usize::MAX
    }
    #[inline]
    fn read_u8(&self, off: u32) -> u8 {
        unsafe { laundered(off).read() }
    }
    #[inline]
    fn write_u8(&mut self, off: u32, v: u8) {
        unsafe { laundered(off).write(v) }
    }
    #[inline]
    fn read_u32(&self, off: u32) -> u32 {
        unsafe { laundered(off).cast::<u32>().read_unaligned() }
    }
    #[inline]
    fn write_u32(&mut self, off: u32, v: u32) {
        unsafe { laundered(off).cast::<u32>().write_unaligned(v) }
    }
    #[inline]
    fn read_u64(&self, off: u32) -> u64 {
        unsafe { laundered(off).cast::<u64>().read_unaligned() }
    }
    #[inline]
    fn write_u64(&mut self, off: u32, v: u64) {
        unsafe { laundered(off).cast::<u64>().write_unaligned(v) }
    }
}

#[cfg(target_arch = "wasm32")]
#[inline]
fn with_mem<R>(f: impl FnOnce(&mut WasmMem) -> R) -> R {
    f(&mut WasmMem)
}

/// Native fallback backend: one process-wide arena so the export surface is
/// testable off-wasm. Single-threaded by contract (same as the wasm instance).
#[cfg(not(target_arch = "wasm32"))]
fn with_mem<R>(f: impl FnOnce(&mut lmao_arena::VecMem) -> R) -> R {
    use std::cell::RefCell;
    thread_local! {
        static MEM: RefCell<lmao_arena::VecMem> = RefCell::new(lmao_arena::VecMem::with_zeroed(1 << 20));
    }
    MEM.with(|m| f(&mut m.borrow_mut()))
}

fn size_class(sc: u8) -> SizeClass {
    match sc {
        0 => SizeClass::SpanSystem,
        1 => SizeClass::Col1B,
        2 => SizeClass::Col4B,
        _ => SizeClass::Col8B,
    }
}

// =============================================================================
// Lifecycle
// =============================================================================

#[unsafe(no_mangle)]
pub extern "C" fn init() {
    with_mem(raw::init);
}

#[unsafe(no_mangle)]
pub extern "C" fn reset() {
    with_mem(raw::reset);
}

// =============================================================================
// Stats / debug
// =============================================================================

#[unsafe(no_mangle)]
pub extern "C" fn get_bump_ptr() -> u32 {
    with_mem(|m| raw::bump_ptr(m))
}

#[unsafe(no_mangle)]
pub extern "C" fn get_alloc_count() -> u32 {
    with_mem(|m| raw::alloc_count(m))
}

#[unsafe(no_mangle)]
pub extern "C" fn get_free_count() -> u32 {
    with_mem(|m| raw::free_count(m))
}

#[unsafe(no_mangle)]
pub extern "C" fn debug_get_freelist_head(sc: u8, capacity: u32) -> u32 {
    with_mem(|m| raw::debug_freelist_head(m, size_class(sc), capacity))
}

#[unsafe(no_mangle)]
pub extern "C" fn debug_read_next_ptr(offset: u32) -> u32 {
    with_mem(|m| raw::debug_next_ptr(m, offset))
}

#[unsafe(no_mangle)]
pub extern "C" fn get_freelist_len(sc: u8, capacity: u32) -> u32 {
    with_mem(|m| raw::freelist_len(m, size_class(sc), capacity))
}

#[unsafe(no_mangle)]
pub extern "C" fn get_freelist_reuse_count(sc: u8, capacity: u32) -> u32 {
    with_mem(|m| raw::freelist_reuse_count(m, size_class(sc), capacity))
}

#[unsafe(no_mangle)]
pub extern "C" fn get_freelist_split_count(sc: u8, capacity: u32) -> u32 {
    with_mem(|m| raw::freelist_split_count(m, size_class(sc), capacity))
}

#[unsafe(no_mangle)]
pub extern "C" fn get_freelist_merge_count(sc: u8, capacity: u32) -> u32 {
    with_mem(|m| raw::freelist_merge_count(m, size_class(sc), capacity))
}


// =============================================================================
// Thread id
// =============================================================================

#[unsafe(no_mangle)]
pub extern "C" fn set_thread_id(high: u32, low: u32) {
    with_mem(|m| raw::set_thread_id(m, high, low));
}

#[unsafe(no_mangle)]
pub extern "C" fn get_thread_id_high() -> u32 {
    with_mem(|m| (raw::thread_id(m) >> 32) as u32)
}

#[unsafe(no_mangle)]
pub extern "C" fn get_thread_id_low() -> u32 {
    with_mem(|m| raw::thread_id(m) as u32)
}

#[unsafe(no_mangle)]
pub extern "C" fn is_thread_id_set() -> u8 {
    with_mem(|m| raw::is_thread_id_set(m))
}

#[unsafe(no_mangle)]
pub extern "C" fn get_span_id_counter() -> u32 {
    with_mem(|m| raw::span_id_counter(m))
}

// =============================================================================
// Identity blocks (packed-u64 convention preserved)
// =============================================================================

#[unsafe(no_mangle)]
pub extern "C" fn alloc_identity_root_for_js_write(trace_id_len: u32) -> u64 {
    with_mem(|m| raw::alloc_identity_root_for_js_write(m, trace_id_len))
}

#[unsafe(no_mangle)]
pub extern "C" fn alloc_identity_child() -> u32 {
    with_mem(raw::alloc_identity_child)
}

#[unsafe(no_mangle)]
pub extern "C" fn free_identity(offset: u32) {
    with_mem(|m| raw::free_identity(m, offset));
}

#[unsafe(no_mangle)]
pub extern "C" fn read_identity_span_id(identity_ptr: u32) -> u32 {
    with_mem(|m| raw::read_identity_span_id(m, identity_ptr))
}

#[unsafe(no_mangle)]
pub extern "C" fn read_identity_trace_id_len(identity_ptr: u32) -> u32 {
    with_mem(|m| raw::read_identity_trace_id_len(m, identity_ptr))
}

#[unsafe(no_mangle)]
pub extern "C" fn get_identity_trace_id_ptr(identity_ptr: u32) -> u32 {
    raw::identity_trace_id_ptr(identity_ptr)
}

#[unsafe(no_mangle)]
pub extern "C" fn read_write_index(identity_ptr: u32) -> u32 {
    with_mem(|m| raw::read_write_index(m, identity_ptr))
}

// Exact physical slab allocation

#[unsafe(no_mangle)]
pub extern "C" fn alloc_exact(byte_len: u32, alignment: u32) -> u32 {
    with_mem(|m| raw::alloc_exact(m, byte_len, alignment))
}

#[unsafe(no_mangle)]
pub extern "C" fn free_exact(offset: u32, byte_len: u32, alignment: u32) {
    with_mem(|m| raw::free_exact(m, offset, byte_len, alignment));
}


// =============================================================================
// TraceRoot + span lifecycle + column IO
// =============================================================================

#[unsafe(no_mangle)]
pub extern "C" fn init_trace_root(trace_root_ptr: u32) {
    let (wall, mono) = (date_now(), performance_now());
    with_mem(|m| raw::init_trace_root(m, trace_root_ptr, wall, mono));
}

#[unsafe(no_mangle)]
pub extern "C" fn span_start(
    system_ptr: u32,
    identity_ptr: u32,
    trace_root_ptr: u32,
    capacity: u32,
) {
    let now = performance_now();
    with_mem(|m| raw::span_start(m, system_ptr, identity_ptr, trace_root_ptr, capacity, now));
}

#[unsafe(no_mangle)]
pub extern "C" fn span_end_ok(system_ptr: u32, trace_root_ptr: u32, capacity: u32) {
    let now = performance_now();
    with_mem(|m| {
        raw::span_end(
            m,
            system_ptr,
            trace_root_ptr,
            capacity,
            raw::ENTRY_TYPE_SPAN_OK,
            now,
        )
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn span_end_err(system_ptr: u32, trace_root_ptr: u32, capacity: u32) {
    let now = performance_now();
    with_mem(|m| {
        raw::span_end(
            m,
            system_ptr,
            trace_root_ptr,
            capacity,
            raw::ENTRY_TYPE_SPAN_ERR,
            now,
        )
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn write_log_entry(
    system_ptr: u32,
    identity_ptr: u32,
    trace_root_ptr: u32,
    entry_type: u8,
    capacity: u32,
) -> u32 {
    let now = performance_now();
    with_mem(|m| {
        raw::write_log_entry(
            m,
            system_ptr,
            identity_ptr,
            trace_root_ptr,
            entry_type,
            capacity,
            now,
        )
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn write_col_f64(col_offset: u32, row_idx: u32, value: f64, capacity: u32) -> u32 {
    with_mem(|m| raw::write_col_f64(m, col_offset, row_idx, value, capacity))
}

#[unsafe(no_mangle)]
pub extern "C" fn write_col_u32(col_offset: u32, row_idx: u32, value: u32, capacity: u32) -> u32 {
    with_mem(|m| raw::write_col_u32(m, col_offset, row_idx, value, capacity))
}

#[unsafe(no_mangle)]
pub extern "C" fn write_col_u8(col_offset: u32, row_idx: u32, value: u8, capacity: u32) -> u32 {
    with_mem(|m| raw::write_col_u8(m, col_offset, row_idx, value, capacity))
}

// =============================================================================
// Debug/read operations
// =============================================================================

#[unsafe(no_mangle)]
pub extern "C" fn get_performance_now() -> f64 {
    performance_now()
}

#[unsafe(no_mangle)]
pub extern "C" fn debug_compute_timestamp(trace_root_ptr: u32) -> i64 {
    let now = performance_now();
    with_mem(|m| raw::timestamp_nanos(m, trace_root_ptr, now))
}

#[unsafe(no_mangle)]
pub extern "C" fn read_timestamp(system_ptr: u32, row_idx: u32) -> i64 {
    with_mem(|m| raw::read_timestamp(m, system_ptr, row_idx))
}

#[unsafe(no_mangle)]
pub extern "C" fn read_entry_type(system_ptr: u32, row_idx: u32, capacity: u32) -> u8 {
    with_mem(|m| raw::read_entry_type(m, system_ptr, row_idx, capacity))
}

#[unsafe(no_mangle)]
pub extern "C" fn read_col_f64(col_offset: u32, row_idx: u32, capacity: u32) -> f64 {
    with_mem(|m| raw::read_col_f64(m, col_offset, row_idx, capacity))
}

#[unsafe(no_mangle)]
pub extern "C" fn read_col_is_valid(col_offset: u32, row_idx: u32) -> u8 {
    with_mem(|m| raw::read_col_is_valid(m, col_offset, row_idx))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    /// ABI smoke over the native fallback arena: the export surface drives a
    /// full span lifecycle exactly like wasmSpanBuffer.ts does.
    #[test]
    fn export_surface_span_lifecycle() {
        init();
        reset();
        set_thread_id(0xAABB, 0xCCDD);
        assert_eq!(is_thread_id_set(), 1);

        let packed = alloc_identity_root_for_js_write(4);
        assert_ne!(packed, 0);
        let identity = (packed >> 32) as u32;
        let trace_field = packed as u32;
        assert_eq!(trace_field, get_identity_trace_id_ptr(identity));
        assert_eq!(read_identity_trace_id_len(identity), 4);

        let cap = 64u32;
        let system = alloc_exact(cap * 9, 8);
        assert_ne!(system, 0);
        let root = alloc_exact(16, 8);
        init_trace_root(root);

        span_start(system, identity, root, cap);
        assert_eq!(read_entry_type(system, 0, cap), raw::ENTRY_TYPE_SPAN_START);
        assert_eq!(
            read_entry_type(system, 1, cap),
            raw::ENTRY_TYPE_SPAN_EXCEPTION
        );
        assert_eq!(read_write_index(identity), 2);

        let idx = write_log_entry(system, identity, root, 5, cap);
        assert_eq!(idx, 2);

        span_end_ok(system, root, cap);
        assert_eq!(read_entry_type(system, 1, cap), raw::ENTRY_TYPE_SPAN_OK);

        free_exact(root, 16, 8);
        free_exact(system, cap * 9, 8);
        free_identity(identity);
        assert!(get_free_count() >= 3);

        // Exact-size slab allocations preserve the caller's alignment and own
        // disjoint byte ranges without rounding the request into a column family.
        reset();
        let requests = [(1, 1), (3, 2), (7, 4), (17, 8), (65, 16), (257, 64)];
        let mut slabs: Vec<(u32, u32, u32)> = Vec::new();
        for (byte_len, alignment) in requests {
            let offset = alloc_exact(byte_len, alignment);
            assert_ne!(offset, 0);
            assert_eq!(offset % alignment, 0, "exact slab alignment");
            for &(other_offset, other_len, _) in &slabs {
                assert!(
                    offset + byte_len <= other_offset || offset >= other_offset + other_len,
                    "exact slab ranges must not overlap"
                );
            }
            slabs.push((offset, byte_len, alignment));
        }
        assert_eq!(get_alloc_count(), slabs.len() as u32);

        // Releasing an exact allocation is idempotent and returns precisely the
        // same address to the exact-size/alignment family without aliasing live owners.
        let (released, byte_len, alignment) = slabs[3];
        free_exact(released, byte_len, alignment);
        let free_count = get_free_count();
        free_exact(released, byte_len, alignment);
        assert_eq!(get_free_count(), free_count);
        let recycled = alloc_exact(byte_len, alignment);
        assert_eq!(recycled, released);
        let distinct = alloc_exact(byte_len, alignment);
        assert_ne!(distinct, recycled);
    }
}
