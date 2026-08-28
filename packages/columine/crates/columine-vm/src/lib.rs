//! Byte-addressed reducer VM core for state lifecycle, hash containers,
//! nested containers, aggregates, undo/delta, and reads/iteration.
//!
//! Modules share explicit little-endian byte layouts and typed offset views.
//! Container operations use hooks to reach VM-owned undo, TTL, and bitmap
//! services without retaining references into the state buffer.
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
