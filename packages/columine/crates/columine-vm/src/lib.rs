//! Rust port of the columine VM core — stage 2 of `specs/axe/91-zig-to-rust-port.md`.
//!
//! Slice 1 ports `packages/columine/src/vm/state_init.zig` plus the minimal
//! surface of its sibling modules that state initialization and slot growth
//! call into. Slice 2 ports the hash-container family (`hash_table.zig`,
//! `hashmap_ops.zig`, `hashset_ops.zig`); its calls into vm.zig globals go
//! through the `hooks` boundary until the vm dispatch/undo/bitmap slices
//! land. Each module here names the Zig file it replaces; modules that exist
//! only as a boundary for a later slice say so in their header.
//!
//! Byte-order note: the VM state and program bytecode are little-endian byte
//! contracts (wasm32 + the LE native targets). All multi-byte accesses go
//! through explicit `to_le_bytes`/`from_le_bytes` copies, so this crate is
//! correct even on a big-endian host and needs no `unsafe` so far.

pub mod aggregates;
pub mod bitmap_ops;
pub mod bytes;
pub mod hash_table;
pub mod hashmap_ops;
pub mod hashset_ops;
pub mod hooks;
pub mod intern;
pub mod meta;
pub mod minroar;
pub mod nested;
pub mod slot_growth;
pub mod state_init;
pub mod struct_map;
pub mod undo_log;
pub mod vm;
