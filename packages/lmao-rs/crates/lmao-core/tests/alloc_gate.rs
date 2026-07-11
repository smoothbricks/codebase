//! AxE overhead gate: ZERO heap allocations per event after warmup
//! (`AxE/specs/sim/01-deterministic-scheduler.md` §5 — the scheduler's steady state
//! must be zero bytes / zero allocations per event; the tracer hot path must not
//! break that).
//!
//! Counting global allocator: every test in THIS file runs under it. Keep gate
//! tests here; do not add unrelated tests to this binary.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct CountingAlloc;

// Thread-local so concurrently running tests (and the libtest harness's own
// threads) don't bleed counts into each other's measurements.
std::thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|c| c.set(c.get() + 1));
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

fn allocations() -> u64 {
    ALLOCATIONS.with(|c| c.get())
}

/// Appending within capacity must not allocate. Overflow chaining MAY allocate
/// (it's the amortized warmup path).
#[test]
fn append_within_capacity_is_alloc_free() {
    use lmao_core::clock::{Clock, TraceAnchor};
    use lmao_core::{EntryType, SpanBuffer, SpanIdentity, TraceId};
    use std::sync::Arc;

    struct FixedClock;
    impl Clock for FixedClock {
        fn wall_nanos(&self) -> i64 {
            0
        }
        fn monotonic_nanos(&self) -> u64 {
            0
        }
    }

    let clock = FixedClock;
    let anchor = TraceAnchor::capture(&clock);
    let identity = Arc::new(SpanIdentity {
        thread_id: 1,
        span_id: 0,
        trace_id: TraceId::new("alloc-gate").unwrap(),
        parent: None,
    });
    // Warmup: buffer creation allocates (pre-allocation IS the design).
    let mut buf = SpanBuffer::start(identity, 1024, &anchor, &clock);

    let before = allocations();
    for _ in 0..1000 {
        buf.append(EntryType::Info, &anchor, &clock);
    }
    let after = allocations();
    assert_eq!(
        after - before,
        0,
        "hot-path appends within capacity must be allocation-free"
    );
}

/// The FULL traced hot path is alloc-free after warmup: static log templates
/// (SharedStr::Static), numeric tag writes into pre-touched lazy columns, and
/// Arc<str> category values (refcount bump only).
#[test]
fn traced_hot_path_is_alloc_free_after_warmup() {
    use lmao_core::clock::{Clock, TraceAnchor};
    use lmao_core::{EntryType, F64Column, SpanBuffer, SpanIdentity, StrColumn, TraceId};
    use std::sync::Arc;

    struct FixedClock;
    impl Clock for FixedClock {
        fn wall_nanos(&self) -> i64 {
            0
        }
        fn monotonic_nanos(&self) -> u64 {
            0
        }
    }

    let clock = FixedClock;
    let anchor = TraceAnchor::capture(&clock);
    let identity = Arc::new(SpanIdentity {
        thread_id: 1,
        span_id: 1,
        trace_id: TraceId::new("alloc-gate-hot").unwrap(),
        parent: None,
    });

    // Warmup: buffer + first-touch of every column + the shared category value.
    let mut buf = SpanBuffer::start(identity, 1024, &anchor, &clock);
    let mut latency = F64Column::new();
    let mut route = StrColumn::new();
    let route_value: Arc<str> = "GET /api/v1/sessions".into();
    latency.set(0, 1024, 0.0);
    route.set(0, 1024, route_value.clone());
    // First log warms the lazy messages column (first-touch alloc is warmup).
    buf.append_msg(EntryType::Info, "warmup", 0, &anchor, &clock);

    let before = allocations();
    for i in 0..500usize {
        let row = buf.append_msg(
            EntryType::Info,
            "handled {route} in {latency} ms", // 'static: SharedStr::Static, no alloc
            42,
            &anchor,
            &clock,
        );
        latency.set(row, 1024, i as f64);
        route.set(row, 1024, route_value.clone()); // Arc clone: refcount bump
        buf.set_name("hot-span"); // 'static overwrite, row-0 semantics
    }
    let after = allocations();
    assert_eq!(
        after - before,
        0,
        "traced hot path (template log + tag writes + Arc category) must be allocation-free"
    );
}
