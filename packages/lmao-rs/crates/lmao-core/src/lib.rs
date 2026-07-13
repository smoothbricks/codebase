//! # lmao-core
//!
//! Rust port of the LMAO trace-logging data model. Spec sources (in
//! `smoothbricks/specs/lmao/`): `01_trace_logging_system.md`,
//! `01a_trace_schema_system.md`, `01b_columnar_buffer_architecture.md` (+ `01b1`..`01b5`),
//! `01f_arrow_table_structure.md`, `01h_entry_types_and_logging_primitives.md`.
//!
//! Determinism constraints come from `AxE/specs/sim/` (esp. `01-deterministic-scheduler.md`
//! and `08-trace-testing.md`): time and entropy only via the [`clock::Clock`] and
//! [`identity::Entropy`] traits; same `(build, seed, config)` must produce bit-identical
//! trace bytes; zero heap allocations per event after warmup.

pub mod buffer;
pub mod clock;
pub mod columns;
pub mod context;
pub mod entry_type;
pub mod identity;
pub mod packed_header;
pub mod result;
pub mod tuning;

pub use buffer::SpanBuffer;
pub use clock::{Clock, CoarseClock, SystemClock, TraceAnchor};
pub use columns::{BoolColumn, EnumColumn, F64Column, NumColumn, SharedStr, StrColumn, U64Column};
pub use context::{SpanContext, TraceContext};
pub use entry_type::EntryType;
pub use identity::{Entropy, SpanIdentity, TraceId};
pub use packed_header::{
    InvalidVocabularyId, MAX_VOCABULARY_ID, StaticVocabularyNotAllowed, VocabularyId,
    entry_type_from_header, pack_dynamic, pack_static, supports_static_vocabulary,
    vocabulary_id_from_header,
};
pub use result::{RetryPolicy, SpanOutcome, Transient};
pub use tuning::CapacityRatchet;
