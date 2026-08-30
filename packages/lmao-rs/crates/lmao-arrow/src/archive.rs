//! Archive-pipeline primitives, per `01t_trace_archive_pipeline.md`.

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::{TimestampNanosecondType, UInt32Type};
use serde_json::Value;

/// FNV-1a 64-bit over bytes.
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    fnv1a64_units(bytes.iter().map(|byte| u64::from(*byte)))
}

/// TypeScript's `charCodeAt` hashes UTF-16 code units, not UTF-8 bytes.
fn fnv1a64_utf16(value: &str) -> u64 {
    fnv1a64_units(value.encode_utf16().map(u64::from))
}

fn fnv1a64_units(units: impl IntoIterator<Item = u64>) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    units
        .into_iter()
        .fold(OFFSET, |hash, unit| (hash ^ unit).wrapping_mul(PRIME))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceChunkEnvelope {
    pub chunk_id: String,
    pub file_ref: String,
    pub chunk_ref: String,
    pub row_count: usize,
    pub started_at_ms: i64,
    pub ended_at_ms: i64,
    pub partition_keys: Vec<String>,
    pub metadata: Option<Value>,
}

pub struct TraceChunkEnvelopeInput<'a> {
    pub file_ref: &'a str,
    pub chunk_ref: &'a str,
    pub batch: &'a RecordBatch,
    pub partition_keys: &'a [&'a str],
    pub metadata: Option<&'a Value>,
}

/// Chunk-level rollups: row count and nanosecond timestamp bounds.
pub fn extract_chunk_stats(batch: &RecordBatch) -> (usize, i64, i64) {
    let timestamp_index = batch
        .schema_ref()
        .index_of("timestamp")
        .expect("canonical trace batch contains timestamp");
    let timestamps = batch
        .column(timestamp_index)
        .as_primitive::<TimestampNanosecondType>();
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    for timestamp in timestamps.values() {
        min = min.min(*timestamp);
        max = max.max(*timestamp);
    }
    if timestamps.is_empty() {
        (0, 0, 0)
    } else {
        (batch.num_rows(), min, max)
    }
}

pub fn build_trace_chunk_envelope(input: TraceChunkEnvelopeInput<'_>) -> TraceChunkEnvelope {
    let (row_count, min_timestamp, max_timestamp) = extract_chunk_stats(input.batch);
    let started_at_ms = min_timestamp.div_euclid(1_000_000);
    let ended_at_ms = max_timestamp.div_euclid(1_000_000);
    let mut partition_keys: Vec<String> = input
        .partition_keys
        .iter()
        .map(|value| (*value).to_owned())
        .collect();
    partition_keys.sort_unstable();

    // This field order and stable recursive serialization are byte-for-byte the
    // TypeScript `buildTraceChunkEnvelope` canonical descriptor.
    let canonical = format!(
        "{{\"chunk_ref\":{},\"ended_at_ms\":{ended_at_ms},\"file_ref\":{},\"metadata\":{},\"partition_keys\":{},\"row_count\":{row_count},\"started_at_ms\":{started_at_ms}}}",
        serde_json::to_string(input.chunk_ref).expect("string serialization is infallible"),
        serde_json::to_string(input.file_ref).expect("string serialization is infallible"),
        input
            .metadata
            .map(stable_serialize)
            .unwrap_or_else(|| "null".to_owned()),
        stable_serialize(&Value::Array(
            partition_keys.iter().cloned().map(Value::String).collect(),
        )),
    );
    let chunk_id = format!("chunk_{:016x}", fnv1a64_utf16(&canonical));

    TraceChunkEnvelope {
        chunk_id,
        file_ref: input.file_ref.to_owned(),
        chunk_ref: input.chunk_ref.to_owned(),
        row_count,
        started_at_ms,
        ended_at_ms,
        partition_keys,
        metadata: input.metadata.cloned(),
    }
}

fn stable_serialize(value: &Value) -> String {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            serde_json::to_string(value).expect("JSON value serialization is infallible")
        }
        Value::Array(values) => {
            let values = values
                .iter()
                .map(stable_serialize)
                .collect::<Vec<_>>()
                .join(",");
            format!("[{values}]")
        }
        Value::Object(values) => {
            let mut entries: Vec<_> = values.iter().collect();
            entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            let entries = entries
                .into_iter()
                .map(|(key, value)| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(key).expect("object key serialization is infallible"),
                        stable_serialize(value)
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{entries}}}")
        }
    }
}

/// Partition-key cardinality of a chunk over the `trace_id` column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionCardinality {
    Single,
    Mixed,
    Unknown,
}

fn trace_key_at(batch: &RecordBatch, row: usize) -> u32 {
    let trace_index = batch
        .schema_ref()
        .index_of("trace_id")
        .expect("canonical trace batch contains trace_id");
    batch
        .column(trace_index)
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

/// Deterministic split by partition key, sorted by dictionary key.
pub fn split_chunk_by_partition(batch: &RecordBatch) -> Vec<(u32, Vec<usize>)> {
    let mut groups = std::collections::BTreeMap::<u32, Vec<usize>>::new();
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
    use crate::convert::convert_span_trees;
    use crate::dict::StableVocabularyCatalog;
    use crate::source::MockSpan;
    use lmao_core::{SpanIdentity, TraceId};
    use std::sync::Arc;

    fn fixture() -> RecordBatch {
        let span = MockSpan {
            identity: Arc::new(SpanIdentity {
                thread_id: 1,
                span_id: 1,
                trace_id: TraceId::new("stats-trace").unwrap(),
                parent: None,
            }),
            timestamps: vec![50, 900, 200],
            packed_headers: vec![1, 2, 8],
            messages: vec![Some("stats-span".into()), None, Some("stats-log".into())],
            overflow: None,
            children: vec![],
        };
        convert_span_trees(&[span], &StableVocabularyCatalog::EMPTY).unwrap()
    }

    #[test]
    fn chunk_stats_values_are_exact() {
        let batch = fixture();
        assert_eq!(extract_chunk_stats(&batch), (3, 50, 900));
        let empty = convert_span_trees::<MockSpan>(&[], &StableVocabularyCatalog::EMPTY).unwrap();
        assert_eq!(extract_chunk_stats(&empty), (0, 0, 0));
    }

    #[test]
    fn envelope_matches_the_typescript_canonical_contract() {
        let batch = fixture();
        let metadata = serde_json::json!({"z": 1, "a": "é"});
        let envelope = build_trace_chunk_envelope(TraceChunkEnvelopeInput {
            file_ref: "file://trace.arrow",
            chunk_ref: "chunk/ref",
            batch: &batch,
            partition_keys: &["z", "a"],
            metadata: Some(&metadata),
        });
        assert_eq!(envelope.chunk_id, "chunk_9554ebf6c00f2da5");
        assert_eq!(envelope.partition_keys, ["a", "z"]);
        assert_eq!((envelope.started_at_ms, envelope.ended_at_ms), (0, 0));
    }

    #[test]
    fn fnv_matches_reference_vectors() {
        assert_eq!(fnv1a64(b""), 0xcbf29ce484222325);
        assert_eq!(fnv1a64(b"a"), 0xaf63dc4c8601ec8c);
        assert_eq!(fnv1a64(b"foobar"), 0x85944171f73967e8);
    }
}
