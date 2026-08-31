//! Independent pair oracles for the contiguous string arena.
//!
//! These attack the cases a write-path number cannot see: offset stability
//! under growth, `'static` pass-through, wrong-arena resolve, and UTF-8 edges.
//! They use [`StringArena`] / [`SharedStr::resolve`] directly so they do not
//! depend on the in-flight `thread_buffer.rs` cutover.

use lmao_core::clock::Clock;
use lmao_core::{EntryType, ScopeValue, SharedStr, StringArena, TextInput, TraceContext, TraceId};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

fn arena() -> StringArena {
    StringArena::new(StringArena::OFFSET_SPACE)
}

/// Vocabulary and log templates are already `'static`. Copying them into the
/// arena would look like a flush win (contiguous bytes) and a hot-path loss
/// (memcpy on every template) that cancel in an aggregate number.
#[test]
fn static_shared_str_is_the_literal_pointer() {
    let literal: &'static str = "handled {route} in {latency} ms";
    let shared = SharedStr::Static(literal);
    let unused = arena();
    assert!(
        unused.is_empty(),
        "resolving a Static must not allocate or intern"
    );
    assert_eq!(
        shared.resolve(&unused).as_ptr(),
        literal.as_ptr(),
        "Static must not copy into an arena"
    );
    assert_eq!(shared.resolve(&unused), literal);
    match shared {
        SharedStr::Static(value) => assert_eq!(value.as_ptr(), literal.as_ptr()),
        SharedStr::Arena(_) => panic!("a literal must not become an arena handle"),
    }
}

/// Force the byte buffer to realloc while a previously interned handle is
/// still held, then read it back. If a cached slice pointer survives a realloc,
/// that is the use-after-free. Offsets are supposed to be the thing that
/// survives; this test stores only the handle.
#[test]
fn interned_handles_survive_byte_buffer_growth() {
    let mut arena = arena();
    let keep = arena.intern_str("keep-me").unwrap();
    let empty = arena.intern_str("").unwrap();
    let cafe = arena.intern_str("café").unwrap();
    let japanese = arena.intern_str("日本語").unwrap();
    let rocket = arena.intern_str("🚀").unwrap();
    let keep_ordinal = arena.intern("keep-me").unwrap();

    for i in 0..4096u32 {
        let unique = format!("unique-{i:04}-{}", "x".repeat(48));
        let id = arena.intern(&unique).unwrap();
        assert_eq!(arena.get(id), Some(unique.as_str()));
    }

    assert!(arena.len() > 4096, "the arena must actually have grown");
    assert_eq!(arena.resolve(keep), "keep-me");
    assert_eq!(arena.resolve(empty), "");
    assert_eq!(arena.resolve(cafe), "café");
    assert_eq!(arena.resolve(japanese), "日本語");
    assert_eq!(arena.resolve(rocket), "🚀");
    assert_eq!(arena.get(keep_ordinal), Some("keep-me"));
    assert_eq!(arena.intern("keep-me").unwrap(), keep_ordinal);
    assert_eq!(arena.intern("").unwrap(), arena.intern("").unwrap());
}

/// Same attack with a raw slice pointer cached across growth. Safe code cannot
/// hold that pointer across `&mut intern`, so this is the `unsafe` form of the
/// bug the type system is supposed to make unrepresentable. After realloc the
/// handle must still resolve; the cached pointer is the thing that must not be
/// treated as live.
#[test]
fn cached_slice_pointer_is_not_the_handle() {
    let mut arena = arena();
    let keep = arena.intern_str("keep-me").unwrap();
    let cached_ptr = arena.resolve(keep).as_ptr();
    let cached_len = arena.resolve(keep).len();
    let before_capacity = arena.as_str().len();

    for i in 0..8192u32 {
        arena
            .intern(&format!("grow-{i:08}-{}", "y".repeat(64)))
            .unwrap();
    }

    assert_eq!(arena.resolve(keep), "keep-me");
    assert!(
        arena.as_str().len() > before_capacity,
        "growth must have appended past the original contents"
    );
    // Reconstructing from the cached pointer is the UAF. We do not dereference
    // it; we only assert the live resolve path did not need it.
    let _ = (cached_ptr, cached_len);
}

#[test]
fn intern_empty_unique_repeated_and_multibyte() {
    let mut arena = arena();
    let empty = arena.intern("").unwrap();
    let once = arena.intern("appears-once").unwrap();
    let repeated = arena.intern("repeats").unwrap();
    for _ in 0..4096 {
        assert_eq!(arena.intern("repeats").unwrap(), repeated);
    }
    let utf8 = arena.intern("naïve — 日本語 — 𝄞").unwrap();

    assert_eq!(arena.get(empty), Some(""));
    assert_eq!(arena.get(once), Some("appears-once"));
    assert_eq!(arena.get(repeated), Some("repeats"));
    assert_eq!(arena.get(utf8), Some("naïve — 日本語 — 𝄞"));
    assert_ne!(empty, once);
    assert_ne!(once, repeated);
    assert_eq!(arena.distinct(), 4);
}

/// Two arenas that both intern a 5-byte string at offset 0. Without an arena
/// identity on the handle this silently returns the other arena's bytes.
/// Debug builds detect it; the message is the planted red.
#[test]
#[should_panic(expected = "arena handle resolved against a different arena")]
fn resolve_does_not_silently_read_another_arenas_bytes() {
    let mut left = arena();
    let mut right = arena();
    let handle = left.intern_str("alpha").unwrap();
    right.intern_str("gamma").unwrap();
    let _ = right.resolve(handle);
}

struct TickClock(AtomicU64);
impl Clock for TickClock {
    fn wall_nanos(&self) -> i64 {
        1_700_000_000_000_000_000
    }
    fn monotonic_nanos(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed)
    }
}

/// Scope is shared by refcount across parent and child buffers. An arena
/// handle in that snapshot would resolve against a different buffer's arena.
/// A non-static scope string must still read back on a child that never
/// interned it.
#[test]
fn child_reads_parent_dynamic_scope_text() {
    let trace = TraceContext::new(
        TraceId::new("arena-scope").unwrap(),
        7,
        Arc::new(TickClock(AtomicU64::new(0))),
    );
    let (parent_out, parent) = trace.__span(
        TextInput::Static("parent"),
        None,
        8,
        lmao_core::SourceMetadata::UNATTRIBUTED,
        |ctx| {
            ctx.set_scope(&[(
                "route",
                Some(ScopeValue::Text(std::borrow::Cow::Owned(String::from(
                    "dyn-scope-café",
                )))),
            )]);
            Ok::<_, ()>(())
        },
    );
    parent_out.unwrap();

    let (child_out, mut child) = trace.__span(
        TextInput::Static("child"),
        None,
        8,
        lmao_core::SourceMetadata::UNATTRIBUTED,
        |ctx| {
            ctx.__log(EntryType::Info, "from-child", 1);
            Ok::<_, ()>(())
        },
    );
    child_out.unwrap();
    child.inherit_scope(parent.scope_handle());

    match child.scope().unwrap().get("route") {
        Some(ScopeValue::Text(text)) => assert_eq!(text.as_ref(), "dyn-scope-café"),
        other => panic!("child scope lost the parent's dynamic text: {other:?}"),
    }
    assert_eq!(parent.scope(), child.scope());
}
