//! Span identity, per `specs/lmao/01b4_span_identity.md`.
//!
//! Deliberately NOT OpenTelemetry random 128-bit span ids:
//! - `thread_id`: 64-bit crypto-random, generated once per process/worker (cold path).
//! - `span_id`: 32-bit thread-local monotonic counter, NEVER zero — counts all spans
//!   on that thread across all traces. Zero coordination.
//! - `trace_id`: validated, shared by reference (`Arc<str>`) across the whole tree.
//! - Parent linkage is by reference, not copied bytes.
//!
//! Zero is reserved rather than merely unused, and the reason is about transports
//! rather than about this counter. A span with no parent expresses that as
//! `parent: None`, which reaches Arrow as a genuine NULL. But a fixed-width wire field
//! has no NULL, so a transport that must say "no parent" in 32 bits says `0`. That
//! sentinel is only unambiguous if no emitter can ever mint 0 as a REAL span id —
//! otherwise a receiver cannot tell "deliberately no parent" from "a genuine first
//! span on some thread". Reserving zero here is what makes the sentinel sound, so it
//! belongs in the generator that every emitter shares, not in each reader.
//!
//! Global uniqueness: `(trace_id, thread_id, span_id)`.
//!
//! Deterministic simulation (`01-deterministic-scheduler.md`): the thread id
//! must come through the injectable [`Entropy`] seam, never `rand::thread_rng`,
//! so a simulated run gets a seeded, reproducible identity stream.

use std::cell::Cell;
use std::sync::Arc;

/// Entropy seam. Production uses OS randomness; deterministic simulations inject a seeded PCG stream.
pub trait Entropy {
    fn next_u64(&mut self) -> u64;
}

thread_local! {
    /// Starts at 1: zero is reserved as the wire's "no parent" sentinel, so no real
    /// span may ever carry it.
    static SPAN_ID_COUNTER: Cell<u32> = const { Cell::new(1) };
}

/// Successor of a span id, skipping zero.
///
/// Separated from the thread-local plumbing so the wrap boundary is directly
/// testable: driving a real counter to `u32::MAX` is not a test anyone can run.
#[inline]
const fn successor(id: u32) -> u32 {
    // `wrapping_add` yields 0 exactly at `u32::MAX`, and that 0 is redirected to 1, so
    // zero stays reserved for all time rather than only until the counter wraps.
    // Without this, exhausting u32 re-mints 0 as a real span id and the wire sentinel
    // silently becomes ambiguous again — a four-billion-span fuse rather than an
    // invariant, and reachable on a long-lived pinned per-core thread, which is
    // precisely where spans are emitted fastest.
    let next = id.wrapping_add(1);
    if next == 0 { 1 } else { next }
}

/// Next thread-local monotonic span id. NEVER returns 0 (see [`successor`]).
#[inline]
pub fn next_span_id() -> u32 {
    SPAN_ID_COUNTER.with(|c| {
        let v = c.get();
        c.set(successor(v));
        v
    })
}

/// Validated trace id: non-empty, ≤128 ASCII chars (`01b4`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TraceId(Arc<str>);

impl TraceId {
    pub fn new(s: impl Into<Arc<str>>) -> Result<Self, TraceIdError> {
        let s: Arc<str> = s.into();
        if s.is_empty() {
            return Err(TraceIdError::Empty);
        }
        if s.len() > 128 || !s.is_ascii() {
            return Err(TraceIdError::Invalid);
        }
        Ok(Self(s))
    }

    /// W3C-format generator (32 lowercase hex chars) from the entropy seam.
    pub fn generate(entropy: &mut dyn Entropy) -> Self {
        let (a, b) = (entropy.next_u64(), entropy.next_u64());
        Self(format!("{a:016x}{b:016x}").into())
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceIdError {
    Empty,
    Invalid,
}

/// Identity of one span. Children share the `TraceId` allocation (zero-copy) and
/// point at their parent; `parent_span_id`/`parent_thread_id` are derived, not stored.
#[derive(Debug, Clone)]
pub struct SpanIdentity {
    pub thread_id: u64,
    pub span_id: u32,
    pub trace_id: TraceId,
    pub parent: Option<Arc<SpanIdentity>>,
}

impl SpanIdentity {
    /// O(1) pointer-equality parentage check (`01b4`: `isParentOf` is `this === other.parent`).
    pub fn is_child_of(&self, other: &Arc<SpanIdentity>) -> bool {
        self.parent.as_ref().is_some_and(|p| Arc::ptr_eq(p, other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixedEntropy(u64);
    impl Entropy for FixedEntropy {
        fn next_u64(&mut self) -> u64 {
            self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1);
            self.0
        }
    }

    #[test]
    fn span_ids_are_monotonic_and_never_zero_per_thread() {
        // Fresh thread so the counter starts where a real emitter's does.
        std::thread::spawn(|| {
            let first = next_span_id();
            assert_ne!(
                first, 0,
                "the very first id on a thread must not be the sentinel"
            );
            let second = next_span_id();
            assert_eq!(second, successor(first));
            assert!(second > first);
        })
        .join()
        .expect("id thread");
    }

    /// The wrap boundary, which is the whole point of the zero-skip and is
    /// unreachable through the counter itself: 2^32 calls is not a test.
    #[test]
    fn the_successor_skips_zero_on_wrap() {
        assert_eq!(
            successor(u32::MAX),
            1,
            "wrapping past MAX must not re-mint the sentinel"
        );
        assert_eq!(successor(0), 1, "even a zero seed advances to a legal id");
        assert_eq!(successor(1), 2);
        assert_eq!(
            successor(u32::MAX - 1),
            u32::MAX,
            "the last legal id is still reachable"
        );
    }

    #[test]
    fn trace_id_validation() {
        assert!(TraceId::new("").is_err());
        assert!(TraceId::new("ok-trace").is_ok());
        assert!(TraceId::new("x".repeat(129)).is_err());
        assert!(TraceId::new("héllo").is_err());
    }

    #[test]
    fn generated_trace_id_is_w3c_shaped_and_deterministic() {
        let t1 = TraceId::generate(&mut FixedEntropy(42));
        let t2 = TraceId::generate(&mut FixedEntropy(42));
        assert_eq!(t1, t2, "same entropy seed must yield same trace id");
        assert_eq!(t1.as_str().len(), 32);
        assert!(t1.as_str().chars().all(|c| c.is_ascii_hexdigit()));
    }
}
