//! # lmao-arrow
//!
//! SpanBuffer tree → Arrow `RecordBatch`, per `specs/lmao/01k_tree_walker_and_arrow_conversion.md`
//! and `01f_arrow_table_structure.md`; archive-pipeline primitives per `01t`.
//!
//! Rules carried over from the TS implementation (flechette-based):
//! - Zero-copy mandate: no per-value builder appends; columns are built from
//!   exact-size Vecs handed to arrow-buffer wholesale.
//! - The topology walk first fixes exact row counts and dictionaries. Each source
//!   row is then validated once while its Arrow columns are emitted; fallible reads
//!   never become plausible zero values.
//! - ONE RecordBatch per flush, all root buffers included, dictionaries shared.
//! - Flat table: every event is one row; `message` holds format-string TEMPLATES
//!   (dictionary-encoded), never interpolated text.

mod archive;
mod convert;
mod dict;
mod ipc;
mod source;

pub use archive::{
    PartitionCardinality, TraceChunkEnvelope, TraceChunkEnvelopeInput, build_trace_chunk_envelope,
    extract_chunk_stats, fnv1a64, inspect_partition_cardinality, split_chunk_by_partition,
};
pub use convert::{ConvertError, ENTRY_TYPE_NAMES, convert_span_trees, trace_schema};
pub use dict::{
    ColumnDictionary, FinalizedDictionary, StableVocabularyCatalog, StableVocabularyCatalogError,
    StableVocabularyEntry, StableVocabularyKind, StableVocabularyKindError,
    StableVocabularyLookupError,
};
pub use ipc::{read_single_batch, write_ipc_stream};
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
