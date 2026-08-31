//! Overhead gate: ZERO heap allocations per event after warmup
//! (deterministic scheduler specification §5 — the scheduler's steady state
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
    let mut buf = SpanBuffer::start_dynamic(identity, 1024, "span".into(), &anchor, &clock);

    let before = allocations();
    for _ in 0..1000 {
        buf.append_dynamic(EntryType::Info, None, 0, &anchor, &clock);
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
    use lmao_core::{
        EntryType, F64Column, SharedStr, SpanBuffer, SpanIdentity, StrColumn, TraceId,
    };
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
    let mut buf = SpanBuffer::start_dynamic(identity, 1024, "span".into(), &anchor, &clock);
    let mut latency = F64Column::new();
    let mut route = StrColumn::new();
    let route_value: Arc<str> = "GET /api/v1/sessions".into();
    latency.set(0, 1024, 0.0);
    route.set(0, 1024, route_value.clone());
    // First log warms the lazy messages column (first-touch alloc is warmup).
    buf.append_dynamic(
        EntryType::Info,
        Some(SharedStr::Static("warmup")),
        0,
        &anchor,
        &clock,
    );

    let before = allocations();
    for i in 0..500usize {
        let row = buf.append_dynamic(
            EntryType::Info,
            Some(SharedStr::Static("handled {route} in {latency} ms")),
            42,
            &anchor,
            &clock,
        );
        latency.set(row, 1024, i as f64);
        route.set(row, 1024, route_value.clone()); // Arc clone: refcount bump
    }
    let after = allocations();
    assert_eq!(
        after - before,
        0,
        "traced hot path (template log + tag writes + Arc category) must be allocation-free"
    );
}

/// `01i` claims scope inheritance is zero-cost precisely because it is a reference
/// share. This measures the claim instead of restating it, and measures the invariant
/// that actually matters: **the allocation cost of a scope must not depend on how
/// many children inherit it.**
///
/// Pinning an absolute number would pin an incidental constant — `set_scope` builds
/// its immutable value through a `Vec`, so ONE update legitimately costs the `Vec`
/// plus the `Arc` it is collected into (two transient allocations, one resident
/// block). Comparing the scoped-versus-unscoped delta at two different child counts
/// isolates the per-child cost exactly: if inheritance allocated anything at all, the
/// delta would grow with N.
#[test]
fn scope_inheritance_cost_does_not_grow_with_children() {
    use lmao_core::clock::Clock;
    use lmao_core::{ScopeValue, SharedStr, TraceContext, TraceId};
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

    fn cost(scope: &[(&'static str, Option<ScopeValue>)], children: usize) -> u64 {
        let trace = TraceContext::new(
            TraceId::new("alloc-gate-scope").unwrap(),
            1,
            Arc::new(FixedClock),
        );
        // Warmup outside the measurement, same shape, so the allocator's first-touch
        // costs are never attributed to inheritance.
        let _ = trace.span("warmup", None, 16, |ctx| {
            ctx.set_scope(scope);
            ctx.child("kid", 16, |_| Ok::<_, ()>(()))
        });

        let before = allocations();
        let _ = trace.span("measured", None, 16, |ctx| {
            ctx.set_scope(scope);
            for _ in 0..children {
                ctx.child("kid", 16, |_| Ok::<_, ()>(()))?;
            }
            Ok::<_, ()>(())
        });
        allocations() - before
    }

    const SCOPE: [(&str, Option<ScopeValue>); 4] = [
        ("alpha", Some(ScopeValue::Uint64(1))),
        ("beta", Some(ScopeValue::Text(SharedStr::Static("b")))),
        ("gamma", Some(ScopeValue::Number(2.5))),
        ("delta", Some(ScopeValue::Boolean(true))),
    ];

    let few = cost(&SCOPE, 5) - cost(&[], 5);
    let many = cost(&SCOPE, 200) - cost(&[], 200);
    assert_eq!(
        few, many,
        "a 4-field scope cost {few} extra allocations across 5 children but {many} \
         across 200; inheritance is allocating per child instead of sharing a handle",
    );

    // And the fixed overhead is the one documented `set_scope` merge, not a surprise.
    assert_eq!(
        few, 2,
        "one set_scope should cost exactly the Vec it builds plus the Arc it becomes",
    );
}

/// Reading scope during execution — the `ctx.scope()` path a host uses to make a
/// decision from inherited context — must not allocate at all.
#[test]
fn reading_scope_is_alloc_free() {
    use lmao_core::clock::Clock;
    use lmao_core::{ScopeValue, TraceContext, TraceId};
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

    let trace = TraceContext::new(
        TraceId::new("alloc-gate-read").unwrap(),
        1,
        Arc::new(FixedClock),
    );
    let _ = trace.span("op", None, 16, |ctx| {
        ctx.set_scope(&[
            ("alpha", Some(ScopeValue::Uint64(7))),
            ("beta", Some(ScopeValue::Number(1.0))),
        ]);

        let before = allocations();
        let mut found = 0usize;
        for _ in 0..1000 {
            if let Some(scope) = ctx.scope() {
                found += usize::from(scope.get("alpha").is_some());
                found += usize::from(scope.get("missing").is_some());
            }
        }
        assert_eq!(
            found, 1000,
            "the binary search must actually be finding things"
        );
        assert_eq!(
            allocations() - before,
            0,
            "scope lookup must be a pure read",
        );
        Ok::<_, ()>(())
    });
}
