# columine-types

Scope: `packages/columine/crates/columine-types/src/lib.rs` (12), `abort.rs` (49), `audit_parser.rs` (173),
`abi_registry_fixture.rs` (186), `opcodes.rs` (384), `types.rs` (1114). Doctrine: `BYPRODUCT-ENGINEERING.md`,
`docs/handbook/04-mechanisms.md`, `05-memory-toolkit.md`, `02-measurement.md` §4.1. Targeted greps:
`packages/columine/{crates,src}` (opcode bytes, magics, offsets, type tags). Neighbor reads (not audited):
`crates/columine-types/tests/registry_audit.rs`, `crates/columine-types/Cargo.toml`, `packages/columine/src/types.ts`,
`wasm-backend.ts`, `reducer-bytecode.ts`,
`crates/columine-vm/src/{meta.rs,state_init.rs,hashmap_ops.rs,undo_log.rs,vm.rs}`.

## Summary

- TypeScript `src/types.ts` hand-restates the ABI and has already diverged (missing Nested, TTL ops, `SLOT_ARRAY`,
  `0x48`, `ErrorCode=8`).
- `wasm-backend.ts` restates `STATE_HEADER_SIZE`/`SLOT_META_SIZE` and slot-meta field offsets as literals (comments
  still say `vm.zig`).
- `CmpType` is an opcode operand byte but does not live in this crate.
- Zero Cargo.toml dependencies. `abort.rs` is load-bearing for wasm. No hot-path clone soup in this crate (helpers are
  unused or once-per-parse).

## Findings

### F2 — HIGH — SSOT — TS ABI tables restated; live gaps vs Rust

Evidence: `packages/columine/src/types.ts:24-54`, `206-215`, `221-329` vs `types.rs:120-134`, `329-459`, `541-554`. Live
raw-byte use: `packages/columine/src/__tests__/columine-integration.test.ts:401`. Mapper:
`packages/columine/src/wasm-backend.ts:248-265`.

```
export enum SlotType { /* … */ BITMAP = 8, STRUCT_MAP2 = 10 } // no NESTED = 9
export enum ErrorCode { /* … */ INVALID_KEY = 7 }            // no COLUMN_UNDERRUN = 8
export enum Opcode {
  SLOT_DEF = 0x10,
  // no SLOT_ARRAY = 0x14
  // no BATCH_MAP_UPSERT_LATEST_TTL = 0x24 / LAST_TTL = 0x25 / SET_INSERT_TTL = 0x32
  // no BATCH_SCALAR_LATEST = 0x48
  // no SLOT_NESTED / NESTED_*
}
// test emits the missing opcode as a literal:
reduceOps: [0x48, 0, 0, 3, 0x48, 1, 1, 3, 0x48, 2, 2, 3],
```

Problem: Comments still say "Must match Zig … vm.zig". The numbers are a third copy of this crate. Diverged from both
Rust registries: missing `SlotType.NESTED=9`; missing `ErrorCode.COLUMN_UNDERRUN=8` (VM returns it; TS `decodeStatus`
throws "TypeScript ErrorCode enum is out of sync"); missing `SLOT_ARRAY`, TTL map/set ops, `BATCH_SCALAR_LATEST`, nested
ops. Generation direction: **Rust `types.rs` is SSOT → generate or bind TS**. Do not keep a hand table. Fix: Emit
`packages/columine/src/types.ts` enums/constants from `columine-types` (build script or napi bindgen). Delete the hand
tables. Add `COLUMN_UNDERRUN = 8` immediately so the wasm mapper cannot throw on a legal VM status. Cost/Risk: Every TS
bytecode emitter/test that names `Opcode.*` must take the generated names. `wasm-backend.ts` switch must grow one arm.

### F3 — HIGH — SSOT — TS restates state-header / slot-meta layout as literals

Evidence: `packages/columine/src/wasm-backend.ts:33-36`, `472-477`;
`packages/columine/src/__tests__/columine-integration.test.ts:483-489` vs `types.rs:10`, `49`, `53-68`.

```
const STATE_HEADER_SIZE = 32;
// Must match vm.zig SLOT_META_SIZE (48 bytes with TTL/eviction fields)
const SLOT_META_SIZE = 48;
const EVICTION_ENTRY_SIZE = 16;
const meta = STATE_HEADER_SIZE + slot * SLOT_META_SIZE;
if ((view.getUint8(meta + 12) & SlotTypeFlag.HAS_EVICT_TRIGGER) === 0) continue;
const bufferOffset = view.getUint32(meta + 36, true);
const count = view.getUint32(meta + 40, true);
```

Problem: `12` is `SlotMetaOffset::TYPE_FLAGS`, `36` is `EVICTED_BUFFER_OFFSET`, `40` is `EVICTED_COUNT`. The comment
names `vm.zig`, not this crate. A layout bump in `types.rs` will not fail TS until a state blob is misread. Fix:
Generate the offset constants into TS from `StateHeaderOffset`/`SlotMetaOffset`/`size_of::<EvictionEntry>()`. Delete the
literals in `wasm-backend.ts` and the integration test. Cost/Risk: TS backend and that one test. VM also restates
`EVICTION_ENTRY_SIZE = 16` (`columine-vm/src/vm.rs:155`) — see Cross-slice.

### F4 — MEDIUM — SSOT — `cmp_type` operand has no type in this crate

Evidence: `types.rs:323-325`; `opcodes.rs:30`; `packages/columine/src/types.ts:90-96`;
`packages/columine/crates/columine-vm/src/hashmap_ops.rs:40-51`.

```
/// Map upserts that compare values carry a trailing `cmp_type:u8`
/// (0=u32, 1=f64, 2=i64)
export enum ComparisonType { U32 = 0, F64 = 1, I64 = 2 }
pub enum CmpType { U32 = 0, F64 = 1, I64 = 2 }
```

Problem: The operand is bytecode ABI. The crate that claims to be ABI SSOT only documents it in comments. The executable
type lives in the VM; TS restates it as `ComparisonType` ("Must match Zig CmpType"). Fix: Add
`#[repr(u8)] pub enum CmpType { U32=0, F64=1, I64=2 }` with `from_u8` here. VM `hashmap_ops::CmpType` becomes a
re-export. TS generated from this enum. Cost/Risk: `columine-vm` hashmap/dispatch imports. One rename.

### F9 — LOW — STRUCTURE — Public ABI surface with no in-tree consumer

Evidence: `types.rs:9`, `13`, `307-318`, `581-600`. `grep` of `packages/` for `RETE_MAGIC`, `RETE_HEADER_SIZE`,
`CT_NODE_EQ`, `V4f64`, `V4u32`, `V2i64` hits only this file.

```
pub const RETE_MAGIC: u32 = 0x4554_4552;
pub const RETE_HEADER_SIZE: u32 = 16;
pub const CT_NODE_EQ: u8 = 1;
// … CT_NODE_DESTINATION: u8 = 11
pub struct V4f64 { pub lanes: [f64; 4] }
```

Problem: Greenfield: unused public ABI is dead code. Vector layout structs exist only to pin `size_of` in tests.
Condition-tree node tags and RETE header magics are not consumed by columine-vm in this tree. Fix: Delete until a caller
exists. If RETE lives in another package not under `packages/`, that caller should import these constants rather than
restating — none found. Cost/Risk: None if truly unused. Confirm with the RETE slice before deleting `RETE_*` /
`CT_NODE_*`.

## Cross-slice questions

- `columine-vm` `hashmap_ops::CmpType` should move here (F4).
- `columine-vm` `vm.rs:155` `pub const EVICTION_ENTRY_SIZE: u32 = 16` restates `size_of::<EvictionEntry>()`.
- `columine-vm` `intern.rs` `const EMPTY: u32 = 0xFFFF_FFFF` restates `EMPTY_KEY`.
- `abi_registry_fixture::{FLAT_UNDO_OPS, RETE_OPCODES, DISPATCHED_OPCODE_BYTES}` are frozen snapshots of enums this
  crate does not define (`FlatUndoOp` in `undo_log.rs`; RETE elsewhere). Fixture `FLAT_UNDO_OPS` omits `ScalarUpdate=14`
  / `StateBytes=15` (named post-parity extensions in `opcode_audit.rs`). Undo/RETE slices own whether those tables
  belong here at all.
- `opcodes.rs` comment reserves `0x50-0x53` as "time filters (0x50+ also RETE)". `RETE_OPCODES` fixture assigns `0x50`
  to `alphaslotbind`. Which contract owns `0x50`?

## Non-findings (checked, clean)

- Cargo.toml: zero `[dependencies]`. No git2-class bloat, no default-features leak. `abort.rs` `die!`/`check!`/`trap` is
  load-bearing for `panic=abort` wasm (cfg, not `cfg!`; strings never reach wasm codegen).
- `hash_key` / `hash_key_pair` are the SSOT; VM probes import them. Not restated in TS. Regime: per-probe, but the
  copies are not here.
- `SlotTypeFlags` bit layout matches TS `SlotTypeFlag` (0x10/0x20/0x40/0x80). No divergence found on those four bits.
- `PROGRAM_MAGIC = 0x314D_4C43` lives once in Rust (`opcodes.rs:185`); TS restates it (F2) but Rust does not duplicate
  it into `types.rs`.
- No hot-loop `to_vec`/`clone`/`format!` in this crate. `audit_parser` `String` harvests are test-regime.
- `next_power_of_2` floor at 16 is a domain rule, tested. Not a copy bug.
