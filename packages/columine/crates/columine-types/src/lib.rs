//! Shared bytecode ABI tables and raw-state layout definitions.
//!
//! The modules remain separate because each registry represents a distinct
//! contract consumed by later VM stages. Frozen cutover snapshots provide
//! deliberate audit tripwires for those registries.

pub mod abi_registry_fixture;
pub mod abort;
#[doc(hidden)]
pub mod audit_parser;
pub mod opcodes;
pub mod types;
