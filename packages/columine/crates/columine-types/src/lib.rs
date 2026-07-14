//! Foundation tables replacing `packages/columine/src/vm/types.zig` and
//! `packages/columine/src/vm/opcodes.zig`.
//!
//! The modules intentionally remain separate because later Rust VM stages map
//! one-to-one to the Zig source inventory.

pub mod abort;
pub mod opcodes;
pub mod types;
pub mod zig_abi_fixture;
#[doc(hidden)]
pub mod zig_audit;
