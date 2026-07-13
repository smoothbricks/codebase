//! # lmao-arrow
//!
//! SpanBuffer tree → Arrow `RecordBatch`, per `specs/lmao/01k_tree_walker_and_arrow_conversion.md`
//! and `01f_arrow_table_structure.md`; archive-pipeline primitives per `01t`.
//!
//! Rules carried over from the TS implementation (flechette-based):
//! - Zero-copy mandate: no per-value builder appends; columns are built from
//!   exact-size Vecs handed to arrow-buffer wholesale.
//! - Two passes, because dictionaries must see all values before allocation:
//!   1. depth-first PRE-ORDER walk (overflow chain yielded immediately after its
//!      primary so a span's rows stay contiguous) accumulating per-column string
//!      dictionaries;
//!   2. finalize SORTED dictionaries (sorted dicts are also the determinism
//!      guarantee AxE trace-byte identity relies on), then walk again writing
//!      indices + copying numeric columns.
//! - ONE RecordBatch per flush, all root buffers included, dictionaries shared.
//! - Flat table: every event is one row; `message` holds format-string TEMPLATES
//!   (dictionary-encoded), never interpolated text.

mod archive;
mod convert;
mod dict;
mod generated {
    pub mod vocabulary;
}
mod source;

pub use archive::{
    PartitionCardinality, TraceChunkEnvelope, build_trace_chunk_envelope, extract_chunk_stats,
    fnv1a64, inspect_partition_cardinality, split_chunk_by_partition,
};
pub use convert::{ConvertError, ENTRY_TYPE_NAMES, convert_span_trees, trace_schema};
pub use dict::{
    ColumnDictionary, FinalizedDictionary, LOG_TEMPLATE_KIND, SPAN_NAME_KIND,
    static_vocabulary_dictionary, static_vocabulary_key, static_vocabulary_value_key,
};
pub use generated::vocabulary::{
    VOCABULARY_CONTENT_HASH, VOCABULARY_DENSE_INDICES, VOCABULARY_FRAGMENT_UTF8,
    VOCABULARY_FRAGMENT_UTF8_OFFSETS, VOCABULARY_ID_ALGORITHM, VOCABULARY_IDS,
    VOCABULARY_KIND_TAGS, VOCABULARY_SCHEMA_VERSION, VOCABULARY_UTF8, VOCABULARY_UTF8_OFFSETS,
    VOCABULARY_VALUES, lookup_vocabulary_id,
};
pub use source::{MockSpan, SpanSource, walk_pre_order};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dictionary_finalize_is_order_independent() {
        let mut a = ColumnDictionary::default();
        let mut b = ColumnDictionary::default();
        for v in ["zeta", "alpha", "alpha", "mid"] {
            a.observe(v);
        }
        for v in ["alpha", "mid", "zeta", "alpha"] {
            b.observe(v);
        }
        assert_eq!(a.finalize(), b.finalize());
    }
}
