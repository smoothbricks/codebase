//! An allocator that limits this thread's allocations, either by count (a
//! budget) or by live bytes (a ceiling, as a capped linear memory does), and
//! measures the peak of live bytes; every other call forwards to the system
//! allocator unchanged. A test binary that includes this module installs it.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct LimitingAllocator;

thread_local! {
    /// Allocations this thread may still make; `None` is unlimited.
    static BUDGET: Cell<Option<u64>> = const { Cell::new(None) };
    /// Live bytes this thread may hold while a ceiling is set.
    static CEILING: Cell<Option<usize>> = const { Cell::new(None) };
    /// Live bytes allocated since the ceiling or measurement began.
    static LIVE: Cell<usize> = const { Cell::new(0) };
    /// The most `LIVE` has been since it began.
    static PEAK: Cell<usize> = const { Cell::new(0) };
}

/// Admit `grow` more live bytes (`shrink` released by a reallocation);
/// `false` refuses the allocation.
fn grant(grow: usize, shrink: usize) -> bool {
    let counted = BUDGET
        .try_with(|budget| match budget.get() {
            None => true,
            Some(0) => false,
            Some(left) => {
                budget.set(Some(left - 1));
                true
            }
        })
        .unwrap_or(true);
    if !counted {
        return false;
    }
    let live = LIVE.try_with(Cell::get).unwrap_or(0);
    let next = live.saturating_sub(shrink).saturating_add(grow);
    let fits = CEILING
        .try_with(|ceiling| ceiling.get().is_none_or(|limit| next <= limit))
        .unwrap_or(true);
    if fits {
        let _ = LIVE.try_with(|cell| cell.set(next));
        let _ = PEAK.try_with(|peak| peak.set(peak.get().max(next)));
    }
    fits
}

fn release(bytes: usize) {
    let _ = LIVE.try_with(|cell| cell.set(cell.get().saturating_sub(bytes)));
}

// SAFETY: every call forwards the original pointer and layout to System; a
// refused allocation returns null, which GlobalAlloc permits for any call.
unsafe impl GlobalAlloc for LimitingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if !grant(layout.size(), 0) {
            return core::ptr::null_mut();
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if !grant(layout.size(), 0) {
            return core::ptr::null_mut();
        }
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        release(layout.size());
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if !grant(new_size, layout.size()) {
            return core::ptr::null_mut();
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: LimitingAllocator = LimitingAllocator;

struct LimitScope;

impl Drop for LimitScope {
    fn drop(&mut self) {
        BUDGET.with(|budget| budget.set(None));
        CEILING.with(|ceiling| ceiling.set(None));
    }
}

fn begin() -> LimitScope {
    LIVE.with(|live| live.set(0));
    PEAK.with(|peak| peak.set(0));
    LimitScope
}

/// Run `run` while this thread may make only `allocations` more allocations.
#[allow(
    dead_code,
    reason = "shared by test binaries that use only some helpers"
)]
pub fn with_allocations<T>(allocations: u64, run: impl FnOnce() -> T) -> T {
    let scope = begin();
    BUDGET.with(|budget| budget.set(Some(allocations)));
    let result = run();
    drop(scope);
    result
}

/// Run `run` while this thread may hold at most `bytes` more live bytes, as
/// a linear memory with a fixed maximum allows.
#[allow(
    dead_code,
    reason = "shared by test binaries that use only some helpers"
)]
pub fn with_ceiling<T>(bytes: usize, run: impl FnOnce() -> T) -> T {
    let scope = begin();
    CEILING.with(|ceiling| ceiling.set(Some(bytes)));
    let result = run();
    drop(scope);
    result
}

/// Run `run` unlimited and return its result with the most bytes it held
/// live at once.
#[allow(
    dead_code,
    reason = "shared by test binaries that use only some helpers"
)]
pub fn peak_bytes<T>(run: impl FnOnce() -> T) -> (T, usize) {
    let scope = begin();
    let result = run();
    let peak = PEAK.with(Cell::get);
    drop(scope);
    (result, peak)
}

/// Run `run` over a fresh `setup()` once per allocation budget from zero
/// up, until it returns `Ok`: each shorter budget fails the next allocation
/// on its path, so every allocation the operation makes is refused once.
/// `setup` runs outside the budget. `refused` must accept each error; a site
/// that aborts instead ends the test process. Returns the first success and
/// the budget it needed.
#[allow(
    dead_code,
    reason = "shared by test binaries that use only some helpers"
)]
pub fn at_every_allocation<I, T, E>(
    mut setup: impl FnMut() -> I,
    mut run: impl FnMut(I) -> Result<T, E>,
    mut refused: impl FnMut(u64, E),
) -> (T, u64) {
    for allocations in 0..10_000 {
        let input = setup();
        match with_allocations(allocations, || run(input)) {
            Ok(value) => return (value, allocations),
            Err(error) => refused(allocations, error),
        }
    }
    panic!("no budget under 10000 allocations let the operation succeed");
}
