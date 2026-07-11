//! Pass-1 string dictionary accumulation, per `01k_tree_walker_and_arrow_conversion.md`
//! and the string-strategy table in `01a_trace_schema_system.md`.
//!
//! Keys BORROW from the span buffers being flushed (`&'a str`) — the flush bench
//! showed owned-key allocation dominating hash choice: FxHashMap with `Box<str>`
//! keys ran 5.9 µs for the 256-string shape vs 4.9 µs for a borrowing std map.
//! Borrowed FxHashMap keys remove the per-distinct-value allocation entirely.
//! (Rust's default SipHash was separately measured losing to JS Map — 5.8 µs vs
//! 3.3 µs — which is why the hasher is FxHash; see `benches/flush.rs`.)

use rustc_hash::{FxHashMap, FxHashSet};

/// Dictionary accumulated in pass 1 for one string column
/// (`ColumnDictionary` in convertToArrow.ts). Borrows its keys from the buffers.
///
/// Tracks per-value occurrence counts and running byte totals so `text` columns can
/// apply the `>128 bytes saved` heuristic (`01a`: only dictionary-encode a text
/// column if it saves more than 128 bytes vs plain UTF-8).
#[derive(Debug, Default)]
pub struct ColumnDictionary<'a> {
    /// Distinct values (occurrence counts proved dead weight: nothing downstream
    /// reads them — the savings heuristic needs only totals + distinct bytes).
    distinct: FxHashSet<&'a str>,
    /// Total UTF-8 bytes if stored plain (every occurrence).
    plain_bytes: u64,
    /// Total values observed (occurrences, not distinct).
    total: u64,
}

/// Finalized, sorted dictionary plus the value → index map used by pass 2.
#[derive(Debug)]
pub struct FinalizedDictionary<'a> {
    /// Sorted, deduplicated values. Sorted order is deterministic for a given
    /// multiset regardless of observation order — load-bearing for AxE's
    /// bit-identical trace bytes.
    pub values: Vec<&'a str>,
    index: FxHashMap<&'a str, u32>,
}

impl<'a> FinalizedDictionary<'a> {
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

impl<'a> ColumnDictionary<'a> {
    pub fn observe(&mut self, value: &'a str) {
        // Flush-only path (no hot-path interning, `01a`). One hash lookup per value,
        // zero allocations.
        self.plain_bytes += value.len() as u64;
        self.total += 1;
        self.distinct.insert(value);
    }

    /// Bytes saved by dictionary-encoding instead of plain UTF-8:
    /// plain = every occurrence's bytes; dict = each distinct value once + one
    /// u32 index per occurrence (offset overhead treated as equal either way).
    pub fn dictionary_savings(&self) -> i64 {
        let distinct_bytes: u64 = self.distinct.iter().map(|k| k.len() as u64).sum();
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
        self.finalize_indexed()
            .values
            .into_iter()
            .map(str::to_string)
            .collect()
    }

    /// Sort distinct values and build the value → index map for pass 2.
    pub fn finalize_indexed(self) -> FinalizedDictionary<'a> {
        let mut values: Vec<&'a str> = self.distinct.into_iter().collect();
        values.sort_unstable();
        let index = values
            .iter()
            .enumerate()
            .map(|(i, v)| (*v, i as u32))
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
        let unique: Vec<String> = (0..100).map(|i| format!("unique-{i}")).collect();
        let mut u = ColumnDictionary::default();
        for s in &unique {
            u.observe(s);
        }
        assert!(!u.should_dictionary_encode());
    }

    #[test]
    fn savings_arithmetic_is_exact() {
        // 5 occurrences of a 37-byte string: plain 185, dict 37 + 4·5 = 57 → 128.
        // Exactly at the threshold: the heuristic is STRICTLY greater-than (`01a`),
        // so 128 must NOT encode (kills the > vs >= boundary mutant).
        let s = "x".repeat(37);
        let mut d = ColumnDictionary::default();
        for _ in 0..5 {
            d.observe(&s);
        }
        assert_eq!(d.dictionary_savings(), 128);
        assert!(!d.should_dictionary_encode());

        // One more occurrence: 222 − (37 + 24) = 161 > 128 → encode.
        d.observe(&s);
        assert_eq!(d.dictionary_savings(), 161);
        assert!(d.should_dictionary_encode());
    }

    #[test]
    fn finalize_returns_sorted_owned_values() {
        let mut d = ColumnDictionary::default();
        for v in ["b", "a", "b"] {
            d.observe(v);
        }
        assert_eq!(d.finalize(), vec!["a".to_string(), "b".to_string()]);

        let mut e = ColumnDictionary::default();
        e.observe("only");
        let f = e.finalize_indexed();
        assert_eq!(f.len(), 1);
        assert!(!f.is_empty());
        assert!(
            FinalizedDictionary {
                values: vec![],
                index: Default::default()
            }
            .is_empty()
        );
        assert_eq!(
            FinalizedDictionary::<'_> {
                values: vec![],
                index: Default::default()
            }
            .len(),
            0
        );
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
