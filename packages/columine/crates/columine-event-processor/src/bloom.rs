//! Bloom filter for duplicate event detection (`dedup/bloom.zig`).
//!
//! Probabilistic duplicate detection with a 0.1% target false-positive rate,
//! double hashing (FNV-1a + a Murmur3-finalizer-mixed second hash). The
//! sizing/hash-count formulas are ported to the constant so checkpointed
//! filters restore onto identically sized bit arrays.

/// Collision policy when a duplicate is detected (u8 values are FFI
/// contract: 0=LATEST, 1=DISCARD).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum CollisionPolicy {
    /// Keep the latest event (replace).
    Latest = 0,
    /// Discard the new event.
    Discard = 1,
}

impl CollisionPolicy {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Latest),
            1 => Some(Self::Discard),
            _ => None,
        }
    }
}

/// Bloom filter for event-ID deduplication (`BloomFilter`).
#[derive(Clone, Debug)]
pub struct BloomFilter {
    pub bits: Vec<u8>,
    /// Expected number of elements.
    pub capacity: u32,
    pub hash_count: u8,
    pub total_added: u64,
}

impl BloomFilter {
    /// Size for the 0.1% false-positive target:
    /// `m = ceil(n * 6.907755 / 0.480453)` bits (≈14.4 bits/element),
    /// minimum 64 bytes; `k = ceil(m/n * ln 2)` clamped to [3, 16]
    /// (bloom.zig:26-56, formulas kept bit-for-bit so restored checkpoints
    /// match).
    pub fn new(expected_elements: u32) -> Self {
        let n = f64::from(expected_elements);
        let m_bits = (n * 6.907755 / 0.480453).ceil() as u64;
        let m_bytes = m_bits.div_ceil(8);
        let actual_bytes = m_bytes.max(64) as u32;

        // Zig uses the truncated literal 0.693147, not LN_2 — the exact
        // constant flips ceil() for some capacities and would desync
        // checkpoint geometry across runtimes.
        #[allow(clippy::approx_constant)]
        let ln2_zig = 0.693147_f64;
        let k = ((f64::from(actual_bytes) * 8.0) / n * ln2_zig).ceil() as u8;
        let hash_count = k.clamp(3, 16);

        Self {
            bits: vec![0; actual_bytes as usize],
            capacity: expected_elements,
            hash_count,
            total_added: 0,
        }
    }

    /// Add an event ID to the filter.
    pub fn add(&mut self, key: &[u8]) {
        let h1 = hash_fnv1a(key);
        let h2 = hash_murmur3_seed(key);
        let bit_count = (self.bits.len() * 8) as u64;
        for i in 0..self.hash_count {
            let combined = h1.wrapping_add(u64::from(i).wrapping_mul(h2));
            let bit_idx = combined % bit_count;
            self.bits[(bit_idx / 8) as usize] |= 1u8 << (bit_idx % 8);
        }
        self.total_added += 1;
    }

    /// True if the ID may have been seen; false means definitely not seen.
    pub fn maybe_contains(&self, key: &[u8]) -> bool {
        let h1 = hash_fnv1a(key);
        let h2 = hash_murmur3_seed(key);
        let bit_count = (self.bits.len() * 8) as u64;
        for i in 0..self.hash_count {
            let combined = h1.wrapping_add(u64::from(i).wrapping_mul(h2));
            let bit_idx = combined % bit_count;
            if self.bits[(bit_idx / 8) as usize] & (1u8 << (bit_idx % 8)) == 0 {
                return false;
            }
        }
        true
    }

    /// Clear all bits.
    pub fn reset(&mut self) {
        self.bits.fill(0);
        self.total_added = 0;
    }

    /// Fill ratio (for monitoring).
    pub fn fill_ratio(&self) -> f64 {
        let set_bits: u64 = self.bits.iter().map(|b| u64::from(b.count_ones())).sum();
        set_bits as f64 / (self.bits.len() * 8) as f64
    }

    /// True when the filter should be grown (>70% fill).
    pub fn should_grow(&self) -> bool {
        self.fill_ratio() > 0.7
    }
}

/// FNV-1a hash (64-bit).
fn hash_fnv1a(data: &[u8]) -> u64 {
    let mut hash: u64 = 14_695_981_039_346_656_037; // FNV offset basis
    for byte in data {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(1_099_511_628_211); // FNV prime
    }
    hash
}

/// MurmurHash3-finalizer-based second hash for double hashing.
fn hash_murmur3_seed(data: &[u8]) -> u64 {
    let mut h: u64 = 0x9e37_79b9_7f4a_7c15; // golden-ratio seed
    for byte in data {
        h ^= u64::from(*byte);
        h = h.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        h ^= h >> 27;
    }
    // fmix64
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    h
}

/// Deduplication state (`DedupState`): bloom filter + policy + counters.
#[derive(Clone, Debug)]
pub struct DedupState {
    pub bloom: BloomFilter,
    pub policy: CollisionPolicy,
    pub duplicates_detected: u64,
    pub total_events: u64,
}

impl DedupState {
    pub fn new(capacity: u32, policy: CollisionPolicy) -> Self {
        Self {
            bloom: BloomFilter::new(capacity),
            policy,
            duplicates_detected: 0,
            total_events: 0,
        }
    }

    /// True if the event should be processed. A (probable) duplicate is
    /// still processed under LATEST (it replaces), refused under DISCARD.
    pub fn should_process(&mut self, event_id: &[u8]) -> bool {
        self.total_events += 1;
        if self.bloom.maybe_contains(event_id) {
            self.duplicates_detected += 1;
            return self.policy == CollisionPolicy::Latest;
        }
        self.bloom.add(event_id);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // test "bloom filter basic operations"
    #[test]
    fn basic_operations() {
        let mut filter = BloomFilter::new(1000);
        filter.add(b"event-001");
        filter.add(b"event-002");
        filter.add(b"event-003");
        assert!(filter.maybe_contains(b"event-001"));
        assert!(filter.maybe_contains(b"event-002"));
        assert!(filter.maybe_contains(b"event-003"));
    }

    // test "bloom filter false positive rate"
    #[test]
    fn false_positive_rate() {
        let mut filter = BloomFilter::new(10_000);
        for i in 0..10_000 {
            filter.add(format!("event-{i:0>10}").as_bytes());
        }
        let mut false_positives = 0u32;
        for i in 10_000..20_000 {
            if filter.maybe_contains(format!("event-{i:0>10}").as_bytes()) {
                false_positives += 1;
            }
        }
        // Target 0.1%; assert < 1% like the Zig test.
        assert!((f64::from(false_positives) / 10_000.0) < 0.01);
    }

    // test "dedup state with DISCARD policy"
    #[test]
    fn dedup_discard() {
        let mut state = DedupState::new(1000, CollisionPolicy::Discard);
        assert!(state.should_process(b"event-001"));
        assert_eq!(state.total_events, 1);
        assert_eq!(state.duplicates_detected, 0);
        assert!(!state.should_process(b"event-001"));
        assert_eq!(state.total_events, 2);
        assert_eq!(state.duplicates_detected, 1);
    }

    // test "dedup state with LATEST policy"
    #[test]
    fn dedup_latest() {
        let mut state = DedupState::new(1000, CollisionPolicy::Latest);
        assert!(state.should_process(b"event-001"));
        assert!(state.should_process(b"event-001"));
        assert_eq!(state.total_events, 2);
        assert_eq!(state.duplicates_detected, 1);
    }

    /// Sizing/hash-count formulas pinned so restored checkpoints always
    /// match the freshly initialized filter's geometry (deserialize
    /// hard-errors on bit-length mismatch).
    #[test]
    fn sizing_formula_pinned() {
        let f = BloomFilter::new(1000);
        // ceil(1000 * 6.907755 / 0.480453) = 14377 bits -> 1798 bytes.
        assert_eq!(f.bits.len(), 1798);
        // ceil(1798*8 / 1000 * ln2) = ceil(9.969) = 10.
        assert_eq!(f.hash_count, 10);
        let small = BloomFilter::new(10);
        assert_eq!(small.bits.len(), 64); // 64-byte floor
        assert_eq!(small.hash_count, 16); // capped at 16
    }
}
