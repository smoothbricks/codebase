//! The lib test binary's one global allocator: a thread-local census of allocation calls and bytes, so concurrent
//! tests do not see each other's allocations.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;

thread_local! {
    static ALLOCATIONS: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
}
struct Counting;
fn charge(bytes: usize) {
    ALLOCATIONS.with(|count| {
        let (calls, total) = count.get();
        count.set((calls + 1, total + bytes));
    });
}
// SAFETY: ownership and layouts are forwarded unchanged to the system allocator. The
// const-initialized thread-local census allocates nothing and isolates concurrent tests.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        charge(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        charge(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        charge(size);
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: Counting = Counting;

pub(crate) fn allocations<T>(f: impl FnOnce() -> T) -> (T, (usize, usize)) {
    ALLOCATIONS.with(|count| count.set((0, 0)));
    let result = black_box(f());
    let count = ALLOCATIONS.with(Cell::get);
    (result, count)
}
