//! Span identity, per `specs/lmao/01b4_span_identity.md`.
//!
//! Deliberately NOT OpenTelemetry random 128-bit span ids:
//! - `thread_id`: 64-bit crypto-random, generated once per process/worker (cold path).
//! - `span_id`: 32-bit thread-local monotonic counter — counts all spans on that
//!   thread across all traces. Zero coordination.
//! - `trace_id`: validated, shared by reference (`Arc<str>`) across the whole tree.
//! - Parent linkage is by reference, not copied bytes.
//!
//! Global uniqueness: `(trace_id, thread_id, span_id)`.
//!
//! AxE determinism (`AxE/specs/sim/01-deterministic-scheduler.md`): the thread id
//! must come through the injectable [`Entropy`] seam, never `rand::thread_rng`,
//! so a simulated run gets a seeded, reproducible identity stream.

use std::cell::Cell;
use std::sync::Arc;

/// Entropy seam. Production uses OS randomness; AxE sim injects a seeded PCG stream.
pub trait Entropy {
    fn next_u64(&mut self) -> u64;
}

thread_local! {
    static SPAN_ID_COUNTER: Cell<u32> = const { Cell::new(0) };
}

/// Next thread-local monotonic span id (`i++`, wraps are a non-goal at u32 scale).
#[inline]
pub fn next_span_id() -> u32 {
    SPAN_ID_COUNTER.with(|c| {
        let v = c.get();
        c.set(v.wrapping_add(1));
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
        self.parent
            .as_ref()
            .is_some_and(|p| Arc::ptr_eq(p, other))
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
    fn span_ids_are_monotonic_per_thread() {
        let a = next_span_id();
        let b = next_span_id();
        assert_eq!(b, a.wrapping_add(1));
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
        assert_eq!(t1, t2, "same entropy seed must yield same trace id (AxE)");
        assert_eq!(t1.as_str().len(), 32);
        assert!(t1.as_str().chars().all(|c| c.is_ascii_hexdigit()));
    }
}
