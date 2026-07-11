//! Per-trace timestamp anchoring, per `specs/lmao/01b3_high_precision_timestamps.md`.
//!
//! One wall-clock + monotonic snapshot is captured at trace-root creation; every
//! subsequent timestamp is `anchor_wall_nanos + (monotonic_now - anchor_monotonic)`.
//! Avoids repeated wall-clock syscalls and long-run drift; each trace is a fresh
//! self-contained time reference.
//!
//! The [`Clock`] trait is the AxE determinism seam
//! (`AxE/specs/sim/01-deterministic-scheduler.md`): kernel/sim code must never call
//! `SystemTime::now()`/`Instant::now()` directly — a simulated run injects a
//! `SimClock` here and gets bit-identical trace bytes for a given seed.

use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Injectable time source. Production: [`SystemClock`]. AxE sim: a deterministic clock.
pub trait Clock: Send + Sync {
    /// Wall-clock nanoseconds since the Unix epoch (captured once per trace).
    fn wall_nanos(&self) -> i64;
    /// Monotonic nanoseconds from an arbitrary origin (captured per entry).
    fn monotonic_nanos(&self) -> u64;
}

/// OS-backed clock for production use.
#[derive(Debug)]
pub struct SystemClock {
    origin: Instant,
}

impl SystemClock {
    pub fn new() -> Self {
        Self { origin: Instant::now() }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn wall_nanos(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0)
    }

    fn monotonic_nanos(&self) -> u64 {
        self.origin.elapsed().as_nanos() as u64
    }
}

/// The 16-byte per-trace anchor (mirrors `TraceRoot` in `allocator.zig`, which stores
/// wall nanos + monotonic millis; here both sides are nanos and the WASM ABI crate
/// converts at the boundary).
#[derive(Debug, Clone, Copy)]
pub struct TraceAnchor {
    pub wall_nanos: i64,
    pub monotonic_nanos: u64,
}

impl TraceAnchor {
    pub fn capture(clock: &dyn Clock) -> Self {
        Self {
            wall_nanos: clock.wall_nanos(),
            monotonic_nanos: clock.monotonic_nanos(),
        }
    }

    /// Anchored entry timestamp in epoch nanoseconds.
    #[inline]
    pub fn timestamp(&self, clock: &dyn Clock) -> i64 {
        let delta = clock.monotonic_nanos().wrapping_sub(self.monotonic_nanos);
        self.wall_nanos.wrapping_add(delta as i64)
    }
}

/// Coarse/batch-stamping clock, the top native optimization identified by the
/// hot-path benchmarks (`docs/optimization-investigation.md`): `Instant::now()`
/// is ~80% of the per-event cost, so this wrapper re-reads the inner clock only
/// every `refresh_every` reads and serves the cached value in between.
///
/// Trade-off: entries stamped from the cache share a timestamp (ordering within
/// a span is still exact — row order is authoritative, `08-trace-testing.md`
/// orders by parentage + row, not by distinct timestamps). Under the AxE sim the
/// inner clock is already virtual and this wrapper is harmless.
#[derive(Debug)]
pub struct CoarseClock<C> {
    inner: C,
    refresh_every: u32,
    reads: std::sync::atomic::AtomicU32,
    cached: std::sync::atomic::AtomicU64,
}

impl<C: Clock> CoarseClock<C> {
    pub fn new(inner: C, refresh_every: u32) -> Self {
        assert!(refresh_every > 0);
        let cached = inner.monotonic_nanos();
        Self {
            inner,
            refresh_every,
            reads: std::sync::atomic::AtomicU32::new(0),
            cached: std::sync::atomic::AtomicU64::new(cached),
        }
    }

    /// Force a fresh read on the next timestamp (e.g. at span boundaries, so
    /// span start/completion stay precise while bulk logs ride the cache).
    pub fn invalidate(&self) {
        self.reads
            .store(self.refresh_every, std::sync::atomic::Ordering::Relaxed);
    }
}

impl<C: Clock> Clock for CoarseClock<C> {
    fn wall_nanos(&self) -> i64 {
        self.inner.wall_nanos()
    }

    #[inline]
    fn monotonic_nanos(&self) -> u64 {
        use std::sync::atomic::Ordering::Relaxed;
        let n = self.reads.fetch_add(1, Relaxed);
        if n >= self.refresh_every {
            self.reads.store(0, Relaxed);
            let fresh = self.inner.monotonic_nanos();
            self.cached.store(fresh, Relaxed);
            fresh
        } else {
            self.cached.load(Relaxed)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Deterministic clock: fixed wall time, monotonic ticks on every read.
    /// This is the shape an AxE `SimClock` adapter will take.
    pub struct TickClock {
        ticks: AtomicU64,
    }

    impl Clock for TickClock {
        fn wall_nanos(&self) -> i64 {
            1_700_000_000_000_000_000
        }
        fn monotonic_nanos(&self) -> u64 {
            self.ticks.fetch_add(1, Ordering::Relaxed)
        }
    }

    #[test]
    fn coarse_clock_caches_between_refreshes() {
        struct Counting(AtomicU64);
        impl Clock for Counting {
            fn wall_nanos(&self) -> i64 {
                0
            }
            fn monotonic_nanos(&self) -> u64 {
                self.0.fetch_add(1, Ordering::Relaxed)
            }
        }
        let coarse = CoarseClock::new(Counting(AtomicU64::new(0)), 4);
        let first: Vec<u64> = (0..4).map(|_| coarse.monotonic_nanos()).collect();
        // reads 0..3 serve the cached construction-time value (0)
        assert_eq!(first, vec![0, 0, 0, 0]);
        // read 4 refreshes
        assert!(coarse.monotonic_nanos() > 0);
        coarse.invalidate();
        let after_invalidate = coarse.monotonic_nanos();
        assert!(after_invalidate > 0);
    }

    #[test]
    fn anchored_timestamps_are_deterministic_and_monotonic() {
        let clock = TickClock { ticks: AtomicU64::new(0) };
        let anchor = TraceAnchor::capture(&clock);
        let t1 = anchor.timestamp(&clock);
        let t2 = anchor.timestamp(&clock);
        assert!(t2 > t1);
        assert_eq!(t1, 1_700_000_000_000_000_001);
    }
}
