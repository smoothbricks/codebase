//! Shared bytecode ABI tables and raw-state layout definitions.
//!
//! [`types`] is the single registry: one `Opcode`, one `SlotType`, one
//! `AggType`, one `ErrorCode`, one set of state/program layout constants. It
//! is re-exported at the crate root so consumers name the ABI once
//! (`columine_types::Opcode`) rather than choosing between module paths that
//! could disagree. `abi_registry_fixture` holds the frozen cutover snapshot
//! that `tests/registry_audit.rs` audits that registry against.

pub mod abi_registry_fixture;
pub mod abort;
#[doc(hidden)]
pub mod audit_parser;
pub mod types;

pub use types::*;
