//! Capacity self-tuning, per `specs/lmao/01b2_buffer_self_tuning.md` (the SHIPPED
//! mechanism only — the aspirational designs in that doc are explicitly out of scope).
//!
//! Single utilization ratchet, tracked PER SCHEMA (all buffers of one shape share
//! capacity learning, not per-instance):
//!
//! ```text
//! utilization = total_writes / (spans_created × (capacity − 2))
//! grow  ×2 when utilization > 1.5
//! shrink ÷2 when utilization < 0.5
//! bounded [MIN_CAPACITY = 8, MAX_CAPACITY = 1024]
//! stats reset after each adjustment
//! ```

pub const MIN_CAPACITY: usize = 8;
pub const MAX_CAPACITY: usize = 1024;

const GROW_THRESHOLD: f64 = 1.5;
const SHRINK_THRESHOLD: f64 = 0.5;
/// Don't adjust on tiny samples.
const MIN_SPANS_SAMPLE: u64 = 16;

/// Per-schema capacity ratchet. One instance lives on each generated buffer class.
#[derive(Debug)]
pub struct CapacityRatchet {
    capacity: usize,
    total_writes: u64,
    spans_created: u64,
}

impl CapacityRatchet {
    pub fn new(initial_capacity: usize) -> Self {
        assert!(
            initial_capacity.is_power_of_two()
                && (MIN_CAPACITY..=MAX_CAPACITY).contains(&initial_capacity)
        );
        Self {
            capacity: initial_capacity,
            total_writes: 0,
            spans_created: 0,
        }
    }

    /// Current recommended capacity for new buffers of this schema.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Record one finished span's write count, possibly adjusting capacity.
    pub fn record_span(&mut self, writes: u64) {
        self.total_writes += writes;
        self.spans_created += 1;
        if self.spans_created < MIN_SPANS_SAMPLE {
            return;
        }
        let usable = (self.capacity - 2) as f64;
        let utilization = self.total_writes as f64 / (self.spans_created as f64 * usable);
        if utilization > GROW_THRESHOLD && self.capacity < MAX_CAPACITY {
            self.capacity *= 2;
            self.reset_stats();
        } else if utilization < SHRINK_THRESHOLD && self.capacity > MIN_CAPACITY {
            self.capacity /= 2;
            self.reset_stats();
        }
    }

    fn reset_stats(&mut self) {
        self.total_writes = 0;
        self.spans_created = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grows_under_pressure_and_stays_bounded() {
        let mut r = CapacityRatchet::new(8);
        for _ in 0..10_000 {
            r.record_span(1000); // way over capacity → overflow-heavy
        }
        assert_eq!(r.capacity(), MAX_CAPACITY);
    }

    #[test]
    fn shrinks_when_idle_and_stays_bounded() {
        let mut r = CapacityRatchet::new(1024);
        for _ in 0..10_000 {
            r.record_span(0);
        }
        assert_eq!(r.capacity(), MIN_CAPACITY);
    }
}
