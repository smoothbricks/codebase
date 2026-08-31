//! # lmao-core
//!
//! Rust port of the LMAO trace-logging data model. Spec sources (in
//! `smoothbricks/specs/lmao/`): `01_trace_logging_system.md`,
//! `01a_trace_schema_system.md`, `01b_columnar_buffer_architecture.md` (+ `01b1`..`01b5`),
//! `01f_arrow_table_structure.md`, `01h_entry_types_and_logging_primitives.md`.
//!
//! Determinism constraints come from the deterministic scheduler and trace-testing specs:
//! time and entropy only via the [`clock::Clock`] and [`identity::Entropy`] traits; same
//! `(build, seed, config)` must produce bit-identical trace bytes; zero heap allocations per event after warmup.

pub mod arena;
pub mod buffer;
pub mod clock;
pub mod columns;
pub mod thread_schema {
    include!(concat!(env!("OUT_DIR"), "/thread_schema.rs"));
}
pub mod thread_kinds {
    include!(concat!(env!("OUT_DIR"), "/thread_kinds.rs"));
}
pub mod context;
pub mod entry_type;
pub mod identity;
pub mod packed_header;
pub mod result;
pub mod scope;
pub mod thread_buffer;
pub mod thread_ffi;
pub mod tuning;

pub use arena::{ArenaFull, ArenaStr, ScopeText, StringArena, TextInput};
pub use buffer::{SourceMetadata, SpanBuffer};
pub use clock::{Clock, CoarseClock, SystemClock, TraceAnchor};
pub use columns::{
    BoolColumn, EnumColumn, EnumIndexError, F64Column, FieldMeta, FieldStrategy, NumColumn,
    SharedStr, StrColumn, U64Column,
};
pub use context::{SpanContext, TraceContext};
pub use entry_type::EntryType;
pub use identity::{Entropy, SpanIdentity, TraceId};
pub use packed_header::{
    InvalidVocabularyId, MAX_VOCABULARY_ID, StaticVocabularyNotAllowed, VocabularyId,
    entry_type_from_header, pack_dynamic, pack_static, supports_static_vocabulary,
    vocabulary_id_from_header,
};
pub use result::{RetryPolicy, SpanOutcome, Transient};
pub use scope::{ScopeEntry, ScopeValue, SpanScope, report_scope_mismatch};
pub use thread_buffer::{
    AttributeValue, ColumnValue, ColumnValueKind, ColumnValueRef, FlushWindow, ThreadBufferError,
    ThreadSpanBuffer,
};
pub use thread_ffi::ThreadSpanBufferHandle;
pub use thread_kinds::{
    ATTRIBUTE_KIND_BOOLEAN, ATTRIBUTE_KIND_ENUM, ATTRIBUTE_KIND_NUMBER, ATTRIBUTE_KIND_TEXT,
    ATTRIBUTE_KIND_UINT64, AttributeKind,
};
pub use thread_schema::{SYSTEM_COLUMN_COUNT, SYSTEM_COLUMNS, SystemColumnKind, SystemColumnMeta};
pub use tuning::{CapacityRatchet, DEFAULT_CAPACITY, MAX_CAPACITY, MIN_CAPACITY};
