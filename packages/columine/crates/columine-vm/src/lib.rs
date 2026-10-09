//! Byte-addressed reducer VM core for state lifecycle, hash containers,
//! nested containers, aggregates, undo/delta, and reads/iteration.
//!
//! Modules share explicit little-endian byte layouts and typed offset views.
//! Container operations use hooks to reach VM-owned undo, TTL, and bitmap
//! services without retaining references into the state buffer.
//!
//! [`program_admission::AdmittedProgram`] owns immutable bytecode, declared VO
//! metadata and an injected admission capability. Storage treats VO as opaque
//! arena bytes; schema decoding and retained-writer projection stay in that
//! capability. A runtime without it must refuse semantic VO admission.
//!
//! Undo and delta use bounded actual-prior-byte StateBytes records, including
//! overwritten dead cells, unused auxiliary lanes and payload/index tails.
//! Journal budget is reserved before writes; JournalOverflow refuses the first
//! unlogged mutation and leaves the accepted prefix exactly reversible.
//! Change-flag boundaries return a typed Result before running a refused writer.
//! Layout growth has no journal: roll back and end the complete speculative
//! window, grow its committed image, reenable undo, and retry the full window.
//!
//! [`vm::CheckedMap`], [`vm::CheckedSet`], [`vm::CheckedStructMap`] and
//! [`vm::CheckedStructMap2`] bind authoritative slot geometry once and borrow
//! the original image for their whole lifetime. Malformed images are typed
//! errors, while ordinary missing keys are optional values; every mapped u32,
//! including u32::MAX, stays distinct from absence. Bitmap views verify their
//! complete payload once at the trust boundary before infallible membership.
//!
//! Byte-order note: the VM state and program bytecode are little-endian byte
//! contracts (wasm32 + the LE native targets). All multi-byte accesses go
//! through explicit `to_le_bytes`/`from_le_bytes` copies, so this crate is
//! correct even on a big-endian host and needs no `unsafe` so far.

pub mod aggregates;
pub mod bitmap_ops;
mod bitmap_ttl;
pub mod bytes;
pub mod column_uses;
pub mod counts_fold;
mod element_body;
pub mod hash_table;
pub mod hashmap_ops;
pub mod hashset_ops;
pub mod hooks;
pub mod meta;
pub mod nested;
pub mod program_admission;
mod program_inputs;
pub mod row_exprs;
pub mod slot_growth;
pub mod state_init;
pub mod struct_map;
pub mod ttl_plan;
pub mod undo_log;
pub mod vm;
