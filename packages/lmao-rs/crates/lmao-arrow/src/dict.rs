//! Pass-1 string dictionary accumulation, per `01k_tree_walker_and_arrow_conversion.md`
//! and the string-strategy table in `01a_trace_schema_system.md`.
//!
//! Counting uses `FxHashMap` (rustc-hash): the benchmark investigation measured Rust's
//! default SipHash `HashMap` LOSING to JS `Map` on dictionary building (5.8 µs vs
//! 3.3 µs for 256 strings); FxHashMap is the remedy and is re-measured in
//! `benches/flush.rs`.

use rustc_hash::FxHashMap;

/// Dictionary accumulated in pass 1 for one string column
/// (`ColumnDictionary` in convertToArrow.ts).
///
/// Tracks per-value occurrence counts and running byte totals so `text` columns can
/// apply the `>128 bytes saved` heuristic (`01a`: only dictionary-encode a text
/// column if it saves more than 128 bytes vs plain UTF-8).
#[derive(Debug, Default)]
pub struct ColumnDictionary {
    counts: FxHashMap<Box<str>, u64>,
    /// Total UTF-8 bytes if stored plain (every occurrence).
    plain_bytes: u64,
    /// Total values observed (occurrences, not distinct).
    total: u64,
}

/// Finalized, sorted dictionary plus the value → index map used by pass 2.
#[derive(Debug)]
pub struct FinalizedDictionary {
    /// Sorted, deduplicated values. Sorted order is deterministic for a given
    /// multiset regardless of observation order — load-bearing for AxE's
    /// bit-identical trace bytes.
    pub values: Vec<String>,
    index: FxHashMap<Box<str>, u32>,
}

impl FinalizedDictionary {
    #[inline]
    pub fn index_of(&self, value: &str) -> Option<u32> {
        self.index.get(value).copied()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl ColumnDictionary {
    pub fn observe(&mut self, value: &str) {
        // Flush-only path (no hot-path interning, `01a`). One hash lookup per value.
        self.plain_bytes += value.len() as u64;
        self.total += 1;
        if let Some(count) = self.counts.get_mut(value) {
            *count += 1;
        } else {
            self.counts.insert(value.into(), 1);
        }
    }

    /// Bytes saved by dictionary-encoding instead of plain UTF-8:
    /// plain = every occurrence's bytes; dict = each distinct value once + one
    /// u32 index per occurrence (offset overhead treated as equal either way).
    pub fn dictionary_savings(&self) -> i64 {
        let distinct_bytes: u64 = self.counts.keys().map(|k| k.len() as u64).sum();
        self.plain_bytes as i64 - (distinct_bytes + 4 * self.total) as i64
    }

    /// `text` column heuristic from `01a`: dictionary-encode only if it saves >128 bytes.
    /// (`category`/`enum` columns are always dictionary-encoded and skip this check.)
    pub fn should_dictionary_encode(&self) -> bool {
        self.dictionary_savings() > 128
    }

    /// Sort + dedupe into the final dictionary (compatibility surface; prefer
    /// [`Self::finalize_indexed`] in conversion code).
    pub fn finalize(self) -> Vec<String> {
        self.finalize_indexed().values
    }

    /// Sort distinct values and build the value → index map for pass 2.
    pub fn finalize_indexed(self) -> FinalizedDictionary {
        let mut values: Vec<String> = self.counts.keys().map(|k| k.to_string()).collect();
        values.sort_unstable();
        let index = values
            .iter()
            .enumerate()
            .map(|(i, v)| (v.as_str().into(), i as u32))
            .collect();
        FinalizedDictionary { values, index }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn savings_heuristic() {
        let mut d = ColumnDictionary::default();
        // 100 occurrences of a 40-byte string: plain 4000, dict 40 + 400 → saves 3560.
        let s = "x".repeat(40);
        for _ in 0..100 {
            d.observe(&s);
        }
        assert!(d.should_dictionary_encode());

        // All-unique strings: dict always loses (adds 4 bytes/occurrence).
        let mut u = ColumnDictionary::default();
        for i in 0..100 {
            u.observe(&format!("unique-{i}"));
        }
        assert!(!u.should_dictionary_encode());
    }

    #[test]
    fn index_map_matches_sorted_positions() {
        let mut d = ColumnDictionary::default();
        for v in ["zeta", "alpha", "mid", "alpha"] {
            d.observe(v);
        }
        let f = d.finalize_indexed();
        assert_eq!(f.values, ["alpha", "mid", "zeta"]);
        assert_eq!(f.index_of("alpha"), Some(0));
        assert_eq!(f.index_of("zeta"), Some(2));
        assert_eq!(f.index_of("missing"), None);
    }
}
