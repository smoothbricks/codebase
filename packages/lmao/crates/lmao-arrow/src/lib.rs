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
mod thread_convert;

// Arrow types cross this crate's public API — `RecordBatch` is returned,
// `ArrayRef` and `DataType` appear in signatures — so a caller must compile
// against the SAME arrow crates this crate did.
//
// A caller reached by `path = "../lmao-arrow"` gets no version negotiation with
// us. If it declares `arrow-array = "55"` while this crate resolves 56, cargo
// builds both, and the two `RecordBatch` types are unrelated. Every call then
// fails with a message that looks like a module-path typo rather than a version
// split, because both spellings are the same crate at different versions:
//
//     error[E0308]: mismatched types
//       expected `arrow_array::RecordBatch`,
//          found `arrow_array::record_batch::RecordBatch`
//
// Re-exporting removes the choice instead of documenting it: a caller writes
// `use lmao_arrow::arrow_array::RecordBatch` and declares no arrow dependency
// at all, so it cannot select a version and the mismatch is unrepresentable.
// Bumping arrow here moves every caller with no coordination and no pin to
// update in any downstream Cargo.toml.
pub use arrow_array;
pub use arrow_buffer;
pub use arrow_ipc;
pub use arrow_schema;

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
pub use thread_convert::{convert_thread_buffer, convert_thread_span_buffer};

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
