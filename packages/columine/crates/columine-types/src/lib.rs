//! Shared bytecode ABI tables and raw-state layout definitions.
//!
//! [`types`] is the single registry: one `Opcode`, one `SlotType`, one
//! `AggType`, one `ErrorCode`, one set of state/program layout constants. It
//! is re-exported at the crate root so consumers name the ABI once
//! (`columine_types::Opcode`) rather than choosing between module paths that
//! could disagree. [`wasm_abi`] is the matching table for the wasm export
//! surface.
//!
//! The `audit` feature adds the test-support surface the ABI tripwires need:
//! `abi_registry_fixture` (the frozen cutover snapshot), `audit_parser` (the
//! source scraper that harvests live declarations to compare against it), and
//! `wasm_abi::parse_exports`. It is off by default so none of that enters the
//! link set of the shipped wasm artifacts.
//!
//! The ABI tables are layout facts with no effects, so the crate links no std
//! and carries no `unsafe`: the linker, not review, keeps them that way. Only
//! the `audit` surface reads files and formats errors, and it pulls std in for
//! itself.

#![no_std]
#![forbid(unsafe_code)]

#[cfg(feature = "audit")]
extern crate std;

#[cfg(feature = "audit")]
pub mod abi_registry_fixture;
pub mod abort;
#[cfg(feature = "audit")]
#[doc(hidden)]
pub mod audit_parser;
pub mod types;
pub mod wasm_abi;

pub use types::*;
