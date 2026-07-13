//! Pass-1 string dictionary accumulation, per `01k_tree_walker_and_arrow_conversion.md`
//! and the string-strategy table in `01a_trace_schema_system.md`.
//!
//! Keys BORROW from the span buffers being flushed (`&'a str`) — the flush bench
//! showed owned-key allocation dominating hash choice: FxHashMap with `Box<str>`
//! keys ran 5.9 µs for the 256-string shape vs 4.9 µs for a borrowing std map.
//! Borrowed FxHashMap keys remove the per-distinct-value allocation entirely.
//! (Rust's default SipHash was separately measured losing to JS Map — 5.8 µs vs
//! 3.3 µs — which is why the hasher is FxHash; see `benches/flush.rs`.)

use std::error::Error;
use std::fmt;
use std::sync::{Arc, OnceLock};

use arrow_array::StringArray;
use arrow_buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::ArrowError;
use rustc_hash::{FxHashMap, FxHashSet};

/// Semantic kind carried by a stable vocabulary entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StableVocabularyKind {
    LogTemplate = 1,
    SpanName = 2,
}

impl TryFrom<u8> for StableVocabularyKind {
    type Error = StableVocabularyKindError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::LogTemplate),
            2 => Ok(Self::SpanName),
            _ => Err(StableVocabularyKindError(value)),
        }
    }
}

/// An invalid serialized stable-vocabulary kind tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StableVocabularyKindError(pub u8);

impl fmt::Display for StableVocabularyKindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid stable vocabulary kind tag {}", self.0)
    }
}

impl Error for StableVocabularyKindError {}

/// One caller-owned stable vocabulary entry, in stable-ID order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StableVocabularyEntry<'a> {
    pub id: u32,
    pub kind: StableVocabularyKind,
    pub value: &'a str,
}

/// Validation failure for [`StableVocabularyCatalog::try_new`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StableVocabularyCatalogError {
    TooManyEntries {
        count: usize,
    },
    InvalidId {
        ordinal: usize,
        id: u32,
    },
    IdsNotStrictlyIncreasing {
        ordinal: usize,
        previous: u32,
        id: u32,
    },
    ValueOrderLengthMismatch {
        entries: usize,
        ordinals: usize,
    },
    ValueOrdinalOutOfRange {
        position: usize,
        ordinal: u32,
        entry_count: usize,
    },
    ValueOrderNotStrictlyIncreasing {
        position: usize,
        previous_ordinal: u32,
        ordinal: u32,
    },
    Utf8LengthOverflow,
}

impl fmt::Display for StableVocabularyCatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyEntries { count } => {
                write!(
                    f,
                    "stable vocabulary has {count} entries, exceeding u32 ordinals"
                )
            }
            Self::InvalidId { ordinal, id } => write!(
                f,
                "stable vocabulary entry {ordinal} has ID {id}, outside the nonzero u24 range",
            ),
            Self::IdsNotStrictlyIncreasing {
                ordinal,
                previous,
                id,
            } => write!(
                f,
                "stable vocabulary IDs are not strictly increasing at entry {ordinal}: {previous} then {id}",
            ),
            Self::ValueOrderLengthMismatch { entries, ordinals } => write!(
                f,
                "stable vocabulary has {entries} entries but {ordinals} value-order ordinals",
            ),
            Self::ValueOrdinalOutOfRange {
                position,
                ordinal,
                entry_count,
            } => write!(
                f,
                "stable vocabulary value-order position {position} references ordinal {ordinal}, but entry count is {entry_count}",
            ),
            Self::ValueOrderNotStrictlyIncreasing {
                position,
                previous_ordinal,
                ordinal,
            } => write!(
                f,
                "stable vocabulary value order is not strictly increasing at position {position}: ordinal {previous_ordinal} then {ordinal}",
            ),
            Self::Utf8LengthOverflow => {
                f.write_str("stable vocabulary exceeds Arrow Utf8's i32 offset range")
            }
        }
    }
}

impl Error for StableVocabularyCatalogError {}

/// Stable-ID lookup failure against a validated catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StableVocabularyLookupError {
    UnknownId(u32),
    KindMismatch {
        id: u32,
        expected: StableVocabularyKind,
        actual: StableVocabularyKind,
    },
}

/// Validated immutable vocabulary provenance supplied by the producer.
///
/// `entries_by_id` is strictly increasing by stable u24 ID. `ordinals_by_value`
/// is a permutation of Arrow ordinals into that slice, sorted by
/// `(UTF-8 value, ordinal)`; it is not a JavaScript process-dense index table.
/// Construction validates both borrowed slices without allocating. The first
/// static-only conversion caches this catalog's Arrow-owned prefix.
#[derive(Debug)]
pub struct StableVocabularyCatalog<'a> {
    entries_by_id: &'a [StableVocabularyEntry<'a>],
    ordinals_by_value: &'a [u32],
    total_utf8_bytes: usize,
    arrow_values: OnceLock<Arc<StringArray>>,
}

impl<'a> StableVocabularyCatalog<'a> {
    // A const lets every caller own its cache; using a static would recreate a
    // process-global Arrow allocation and conflate unrelated producers.
    #[allow(clippy::declare_interior_mutable_const)]
    pub const EMPTY: Self = Self {
        entries_by_id: &[],
        ordinals_by_value: &[],
        total_utf8_bytes: 0,
        arrow_values: OnceLock::new(),
    };

    pub fn try_new(
        entries_by_id: &'a [StableVocabularyEntry<'a>],
        ordinals_by_value: &'a [u32],
    ) -> Result<Self, StableVocabularyCatalogError> {
        if u32::try_from(entries_by_id.len()).is_err() {
            return Err(StableVocabularyCatalogError::TooManyEntries {
                count: entries_by_id.len(),
            });
        }

        let mut previous_id = None;
        let mut total_utf8_bytes = 0usize;
        for (ordinal, entry) in entries_by_id.iter().enumerate() {
            if entry.id == 0 || entry.id > 0x00ff_ffff {
                return Err(StableVocabularyCatalogError::InvalidId {
                    ordinal,
                    id: entry.id,
                });
            }
            if let Some(previous) = previous_id
                && entry.id <= previous
            {
                return Err(StableVocabularyCatalogError::IdsNotStrictlyIncreasing {
                    ordinal,
                    previous,
                    id: entry.id,
                });
            }
            previous_id = Some(entry.id);
            total_utf8_bytes = total_utf8_bytes
                .checked_add(entry.value.len())
                .ok_or(StableVocabularyCatalogError::Utf8LengthOverflow)?;
            if total_utf8_bytes > i32::MAX as usize {
                return Err(StableVocabularyCatalogError::Utf8LengthOverflow);
            }
        }

        if ordinals_by_value.len() != entries_by_id.len() {
            return Err(StableVocabularyCatalogError::ValueOrderLengthMismatch {
                entries: entries_by_id.len(),
                ordinals: ordinals_by_value.len(),
            });
        }

        let mut previous_value_ordinal = None;
        for (position, &ordinal) in ordinals_by_value.iter().enumerate() {
            let Some(entry) = entries_by_id.get(ordinal as usize) else {
                return Err(StableVocabularyCatalogError::ValueOrdinalOutOfRange {
                    position,
                    ordinal,
                    entry_count: entries_by_id.len(),
                });
            };
            if let Some(previous_ordinal) = previous_value_ordinal {
                let previous = &entries_by_id[previous_ordinal as usize];
                if (previous.value.as_bytes(), previous_ordinal)
                    >= (entry.value.as_bytes(), ordinal)
                {
                    return Err(
                        StableVocabularyCatalogError::ValueOrderNotStrictlyIncreasing {
                            position,
                            previous_ordinal,
                            ordinal,
                        },
                    );
                }
            }
            previous_value_ordinal = Some(ordinal);
        }

        Ok(Self {
            entries_by_id,
            ordinals_by_value,
            total_utf8_bytes,
            arrow_values: OnceLock::new(),
        })
    }

    #[inline]
    pub fn entries(&self) -> &'a [StableVocabularyEntry<'a>] {
        self.entries_by_id
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries_by_id.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries_by_id.is_empty()
    }

    #[inline]
    pub fn key_for_id(
        &self,
        id: u32,
        required_kind: StableVocabularyKind,
    ) -> Result<u32, StableVocabularyLookupError> {
        let ordinal = self
            .entries_by_id
            .binary_search_by_key(&id, |entry| entry.id)
            .map_err(|_| StableVocabularyLookupError::UnknownId(id))?;
        let actual = self.entries_by_id[ordinal].kind;
        if actual != required_kind {
            return Err(StableVocabularyLookupError::KindMismatch {
                id,
                expected: required_kind,
                actual,
            });
        }
        Ok(ordinal as u32)
    }

    #[inline]
    pub fn key_for_value(&self, value: &str) -> Option<u32> {
        let value = value.as_bytes();
        let mut lower = 0usize;
        let mut upper = self.ordinals_by_value.len();
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            let ordinal = self.ordinals_by_value[middle];
            if self.entries_by_id[ordinal as usize].value.as_bytes() < value {
                lower = middle + 1;
            } else {
                upper = middle;
            }
        }
        let ordinal = *self.ordinals_by_value.get(lower)?;
        (self.entries_by_id[ordinal as usize].value.as_bytes() == value).then_some(ordinal)
    }

    fn static_arrow_values(&self) -> Arc<StringArray> {
        Arc::clone(self.arrow_values.get_or_init(|| {
            build_vocabulary_dictionary(self, &[])
                .expect("validated stable vocabulary must fit Arrow Utf8")
        }))
    }
}

fn build_vocabulary_dictionary(
    catalog: &StableVocabularyCatalog<'_>,
    dynamic_values: &[&str],
) -> Result<Arc<StringArray>, ArrowError> {
    let dynamic_bytes = dynamic_values.iter().try_fold(0usize, |total, value| {
        total.checked_add(value.len()).ok_or_else(|| {
            ArrowError::InvalidArgumentError("message dictionary byte length overflow".into())
        })
    })?;
    let total_bytes = catalog
        .total_utf8_bytes
        .checked_add(dynamic_bytes)
        .ok_or_else(|| {
            ArrowError::InvalidArgumentError("message dictionary byte length overflow".into())
        })?;
    if total_bytes > i32::MAX as usize {
        return Err(ArrowError::InvalidArgumentError(
            "message dictionary exceeds Utf8 offset range".into(),
        ));
    }
    let offset_count = catalog
        .len()
        .checked_add(dynamic_values.len())
        .and_then(|count| count.checked_add(1))
        .ok_or_else(|| {
            ArrowError::InvalidArgumentError("message dictionary entry count overflow".into())
        })?;

    let mut bytes = Vec::with_capacity(total_bytes);
    let mut offsets = Vec::with_capacity(offset_count);
    offsets.push(0);
    for entry in catalog.entries_by_id {
        bytes.extend_from_slice(entry.value.as_bytes());
        offsets.push(bytes.len() as i32);
    }
    for value in dynamic_values {
        bytes.extend_from_slice(value.as_bytes());
        offsets.push(bytes.len() as i32);
    }
    Ok(Arc::new(StringArray::new(
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Buffer::from_vec(bytes),
        None,
    )))
}

/// Return this catalog's static Arrow prefix, or build the unavoidable combined
/// static-prefix/dynamic-suffix Arrow dictionary for a mixed conversion.
pub(crate) fn vocabulary_dictionary(
    catalog: &StableVocabularyCatalog<'_>,
    dynamic_values: &[&str],
) -> Result<Arc<StringArray>, ArrowError> {
    if dynamic_values.is_empty() {
        Ok(catalog.static_arrow_values())
    } else {
        build_vocabulary_dictionary(catalog, dynamic_values)
    }
}

/// Flush-local novel dynamic values in first-observation order. Empty construction
/// is allocation-free, so static-only batches never allocate a dynamic map/vector.
#[derive(Debug, Default)]
pub(crate) struct FirstSeenDictionary<'a> {
    pub values: Vec<&'a str>,
    index: FxHashMap<&'a str, u32>,
}

impl<'a> FirstSeenDictionary<'a> {
    #[inline]
    pub fn observe(&mut self, value: &'a str) {
        if self.index.contains_key(value) {
            return;
        }
        let index = self.values.len() as u32;
        self.values.push(value);
        self.index.insert(value, index);
    }

    #[inline]
    pub fn index_of(&self, value: &str) -> Option<u32> {
        self.index.get(value).copied()
    }
}

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
