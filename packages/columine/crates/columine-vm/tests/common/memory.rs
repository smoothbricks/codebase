//! An allocator that lets this thread make only a budgeted number of
//! allocations, forwarding every other call to the system allocator
//! unchanged. A test binary that declares `mod common;` installs it.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct BudgetAllocator;

thread_local! {
    /// Allocations this thread may still make; `None` is unlimited.
    static BUDGET: Cell<Option<u64>> = const { Cell::new(None) };
}

/// Spend one allocation; `false` when the budget is exhausted.
fn grant() -> bool {
    BUDGET
        .try_with(|budget| match budget.get() {
            None => true,
            Some(0) => false,
            Some(left) => {
                budget.set(Some(left - 1));
                true
            }
        })
        .unwrap_or(true)
}

// SAFETY: every call forwards the original pointer and layout to System; a
// refused allocation returns null, which GlobalAlloc permits for any call.
unsafe impl GlobalAlloc for BudgetAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if !grant() {
            return core::ptr::null_mut();
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if !grant() {
            return core::ptr::null_mut();
        }
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if !grant() {
            return core::ptr::null_mut();
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: BudgetAllocator = BudgetAllocator;

struct BudgetScope;

impl Drop for BudgetScope {
    fn drop(&mut self) {
        BUDGET.with(|budget| budget.set(None));
    }
}

/// Run `run` while this thread may make only `allocations` more allocations.
pub fn with_allocations<T>(allocations: u64, run: impl FnOnce() -> T) -> T {
    BUDGET.with(|budget| budget.set(Some(allocations)));
    let scope = BudgetScope;
    let result = run();
    drop(scope);
    result
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
