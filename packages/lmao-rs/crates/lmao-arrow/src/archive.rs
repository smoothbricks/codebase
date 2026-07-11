//! Archive-pipeline primitives, per `01t_trace_archive_pipeline.md`.
//!
//! Library-owned, PURE and DETERMINISTIC only (same input → same output, safe for
//! retries): envelope identity, partition inspection/split, chunk stats. Control-plane
//! fan-out belongs to the consuming system and is out of scope.

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, UInt32Type};

/// FNV-1a 64-bit over canonicalized content — deterministic chunk identity (`01t`:
/// chunk ids are content hashes, never random).
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    bytes
        .iter()
        .fold(OFFSET, |h, b| (h ^ (*b as u64)).wrapping_mul(PRIME))
}

/// Envelope referencing already-flushed Arrow payload by ref, not inline bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceChunkEnvelope {
    /// `fnv1a64` over the canonicalized content descriptor (`file_ref`, refs, row
    /// count, time bounds) — NOT over the payload bytes, matching the TS
    /// `buildTraceChunkEnvelope` behavior of hashing the canonical descriptor.
    pub chunk_id: u64,
    pub file_ref: String,
    pub row_count: usize,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
}

/// Chunk-level rollups (`extractChunkStats`): row count and time bounds.
/// (Per-column min/max/null-count rollups are not yet built upstream either.)
pub fn extract_chunk_stats(batch: &RecordBatch) -> (usize, i64, i64) {
    let timestamps = batch.column(0).as_primitive::<Int64Type>();
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    for ts in timestamps.values() {
        min = min.min(*ts);
        max = max.max(*ts);
    }
    if timestamps.is_empty() {
        (0, 0, 0)
    } else {
        (batch.num_rows(), min, max)
    }
}

pub fn build_trace_chunk_envelope(file_ref: &str, batch: &RecordBatch) -> TraceChunkEnvelope {
    let (row_count, min_timestamp, max_timestamp) = extract_chunk_stats(batch);
    // Canonical descriptor: stable field order, unambiguous separators.
    let canonical =
        format!("v1\x1f{file_ref}\x1f{row_count}\x1f{min_timestamp}\x1f{max_timestamp}");
    TraceChunkEnvelope {
        chunk_id: fnv1a64(canonical.as_bytes()),
        file_ref: file_ref.to_string(),
        row_count,
        min_timestamp,
        max_timestamp,
    }
}

/// Partition-key cardinality of a chunk over the `trace_id` column
/// (`inspectPartitionCardinality`): `single` | `mixed` | `unknown`(empty).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionCardinality {
    Single,
    Mixed,
    Unknown,
}

fn trace_key_at(batch: &RecordBatch, row: usize) -> u32 {
    batch
        .column(1)
        .as_dictionary::<UInt32Type>()
        .keys()
        .value(row)
}

pub fn inspect_partition_cardinality(batch: &RecordBatch) -> PartitionCardinality {
    if batch.num_rows() == 0 {
        return PartitionCardinality::Unknown;
    }
    let first = trace_key_at(batch, 0);
    for row in 1..batch.num_rows() {
        if trace_key_at(batch, row) != first {
            return PartitionCardinality::Mixed;
        }
    }
    PartitionCardinality::Single
}

/// Deterministic split by partition key (trace_id), sorted by key
/// (`splitChunkByPartition`): returns per-partition row-index runs in sorted-key
/// order. Row indices are returned (not sliced batches) so callers control slicing.
pub fn split_chunk_by_partition(batch: &RecordBatch) -> Vec<(u32, Vec<usize>)> {
    let mut groups: std::collections::BTreeMap<u32, Vec<usize>> = std::collections::BTreeMap::new();
    for row in 0..batch.num_rows() {
        groups
            .entry(trace_key_at(batch, row))
            .or_default()
            .push(row);
    }
    groups.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_stats_values_are_exact() {
        use crate::convert::convert_span_trees;
        use crate::source::MockSpan;
        use lmao_core::{SpanIdentity, TraceId};
        use std::sync::Arc;

        let span = MockSpan {
            identity: Arc::new(SpanIdentity {
                thread_id: 1,
                span_id: 1,
                trace_id: TraceId::new("stats-trace").unwrap(),
                parent: None,
            }),
            timestamps: vec![50, 900, 200],
            entry_types: vec![1, 2, 5],
            messages: vec![None, None, None],
            overflow: None,
            children: vec![],
        };
        let batch = convert_span_trees(&[span]).unwrap();
        assert_eq!(extract_chunk_stats(&batch), (3, 50, 900));

        let empty = convert_span_trees::<MockSpan>(&[]).unwrap();
        assert_eq!(extract_chunk_stats(&empty), (0, 0, 0));

        let env = build_trace_chunk_envelope("ref", &batch);
        assert_eq!(
            (env.row_count, env.min_timestamp, env.max_timestamp),
            (3, 50, 900)
        );
    }

    #[test]
    fn fnv_matches_reference_vectors() {
        // Standard FNV-1a 64 test vectors.
        assert_eq!(fnv1a64(b""), 0xcbf29ce484222325);
        assert_eq!(fnv1a64(b"a"), 0xaf63dc4c8601ec8c);
        assert_eq!(fnv1a64(b"foobar"), 0x85944171f73967e8);
    }
}
