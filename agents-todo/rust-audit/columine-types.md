# columine-types

Scope: `packages/columine/crates/columine-types/src/lib.rs` (12), `abort.rs` (49), `audit_parser.rs` (173),
`abi_registry_fixture.rs` (186), `opcodes.rs` (384), `types.rs` (1114). Doctrine: `BYPRODUCT-ENGINEERING.md`,
`docs/handbook/04-mechanisms.md`, `05-memory-toolkit.md`, `02-measurement.md` §4.1. Targeted greps:
`packages/columine/{crates,src}` (opcode bytes, magics, offsets, type tags). Neighbor reads (not audited):
`crates/columine-types/tests/registry_audit.rs`, `crates/columine-types/Cargo.toml`, `packages/columine/src/types.ts`,
`wasm-backend.ts`, `reducer-bytecode.ts`,
`crates/columine-vm/src/{meta.rs,state_init.rs,hashmap_ops.rs,undo_log.rs,vm.rs}`.

## Summary

- Two public `Opcode`/`SlotType`/`AggType`/`ErrorCode`/`DurationUnit`/`ChangeFlag` registries in one crate; Nested
  family already split, everything else copied.
- TypeScript `src/types.ts` hand-restates the ABI and has already diverged (missing Nested, TTL ops, `SLOT_ARRAY`,
  `0x48`, `ErrorCode=8`).
- `wasm-backend.ts` restates `STATE_HEADER_SIZE`/`SLOT_META_SIZE` and slot-meta field offsets as literals (comments
  still say `vm.zig`).
- `CmpType` is an opcode operand byte but does not live in this crate.
- `abi_registry_fixture` + `audit_parser` ship in the lib, not `cfg(test)`.
- `SlotMeta`/`get_slot_meta` and `ProgramHeader::{from,to}_wire_bytes` have no in-tree callers; VM/TS reimplemented
  both.
- Discriminant unit tests restate incomplete variant lists (§7.10bb).
- `get_slot_meta` mixes named `SlotMetaOffset` with magic `4/24/28/36`; invalid `AggType` becomes `Sum`.
- Zero Cargo.toml dependencies. `abort.rs` is load-bearing for wasm. No hot-path clone soup in this crate (helpers are
  unused or once-per-parse).

## Findings

### F1 — HIGH — SSOT — Dual Rust ABI registries already split, rest copied

Evidence: `packages/columine/crates/columine-types/src/types.rs:8-14` + `329-459` vs `opcodes.rs:15-116` + `253-256`;
`types.rs:120-134` vs `opcodes.rs:138-149`; `registry_audit.rs` pins the split.

```
// types.rs
pub const STATE_MAGIC: u32 = 0x5354_4154;
pub const PROGRAM_HEADER_SIZE: u32 = 46;
SlotNested = 0x1a,
NestedSetInsert = 0x90,
Nested = 9,
StructMap2 = 10,

// opcodes.rs
pub const STATE_MAGIC: u32 = 0x5354_4154;
pub const PROGRAM_HEADER_SIZE: u32 = 46;
// no SlotNested / Nested* variants
// SlotType has StructMap2 = 10 but not Nested
```

Problem: `lib.rs` publishes both `types` and `opcodes`. Each defines `Opcode`, `AggType`, `SlotType`, `StructFieldType`,
`DurationUnit`, `ChangeFlag`, `ErrorCode`, plus
`STATE_MAGIC`/`STATE_HEADER_SIZE`/`PROGRAM_HASH_PREFIX`/`PROGRAM_HEADER_SIZE`/`STATE_FORMAT_VERSION`/`SLOT_META_SIZE`.
The Nested family (`SlotNested=0x1a`, `0x90/0x92/0x95`, `SlotType::Nested=9`) exists only in `types`. VM consumers
import `columine_types::types::Opcode` and only `opcodes::{PROGRAM_MAGIC, DEFAULT_ACCEPTED_PROGRAM_MAGICS}`. Two public
types named `Opcode` is not a contract; it is two tables. `grep` found no `opcodes::Opcode` consumer outside this crate.
Fix: Single registry in `types.rs` (VM already consumes it). `opcodes.rs` keeps only what is unique: `PROGRAM_MAGIC`,
`DEFAULT_ACCEPTED_PROGRAM_MAGICS`, `ProgramHeader`. Nested ops stay as variants on that one enum (they are wire bytes).
Delete the second `Opcode`/`SlotType`/`AggType`/`ErrorCode`/`DurationUnit`/`ChangeFlag` and the duplicated constants.
Collapse `TYPES_OPCODE_REGISTRY`/`OPCODES_OPCODE_REGISTRY` to one fixture. Cost/Risk: `registry_audit.rs` and
`abi_registry_fixture.rs` must move in the same commit; any stray `opcodes::Opcode` import (none found) would fail to
compile.

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

### F5 — MEDIUM — STRUCTURE — Test-only ABI snapshots and source scrapers ship in the lib

Evidence: `lib.rs:7-10`; `abi_registry_fixture.rs:1-11`; `audit_parser.rs:1-27`.

```
pub mod abi_registry_fixture;
#[doc(hidden)]
pub mod audit_parser;
/// WHY this lives in the library (doc-hidden) … compiled only when referenced
pub fn read_source(...) -> String {
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!(...))
}
```

Problem: Both modules are test tripwires (`registry_audit.rs`, `columine-vm` `opcode_audit.rs`). They are not
`cfg(test)`. `pub mod` always compiles; the "compiled only when referenced" claim is false. `audit_parser` pulls
`std::fs` + `BTreeSet` + `String` harvests into every rlib of this crate, including `columine-wasm`'s dependency edge.
`abi_registry_fixture` is ~200 string/byte table entries of opcode names. Fix: `#[cfg(test)]` cannot share across
crates. Decision: a `columine-types` feature `audit` (default off) or a tiny `columine-abi-audit` dev crate owned by the
tests. Do not ship the fixture or the scraper in the wasm artifact's link set. Cost/Risk: `columine-vm`/`rete` audit
tests add the feature/dev-dep. No runtime callers to migrate.

### F6 — MEDIUM — STRUCTURE — Dead layout decoders; VM/TS reimplemented them

Evidence: `types.rs:602-757` (`SlotMeta`, `get_slot_meta`, `set_change_flag`, `clear_all_change_flags`,
`has_relevant_changes`) — `grep` across `packages/columine` found no caller. `opcodes.rs:210-249`
(`ProgramHeader::{from,to}_wire_bytes`) — no caller outside `opcodes.rs` tests. Live reimplementation:
`columine-vm/src/meta.rs:20-48`, `state_init.rs:167-182`; `packages/columine/src/reducer-bytecode.ts:116-128`.

```
pub unsafe fn get_slot_meta(state_base: *mut u8, slot: u8) -> SlotMeta { ... }
pub const fn from_wire_bytes(bytes: [u8; Self::WIRE_SIZE]) -> Self { ... }
// VM:
let magic = u32::from(content[0]) | (u32::from(content[1]) << 8) | ...
let num_slots = content[6];
let init_len = u16::from(content[10]) | (u16::from(content[11]) << 8);
```

Problem: The typed decoders this crate exists to own are unused. Consumers parse the same 14-byte header and 48-byte
slot meta by magic index. Evaporating work (Byproduct L0): the layout type does not become the parse. Fix: Delete
`SlotMeta`/`get_slot_meta`/`set_change_flag`/`clear_all_change_flags`/`has_relevant_changes` (VM `SlotMetaView` is the
live reader). Make `parse_program` / `parseReducerProgram` call `ProgramHeader::from_wire_bytes`. If the pointer-based
helpers were a wasm FFI sketch, they are not wired; do not keep them. Cost/Risk: None for SlotMeta (zero callers).
Program header: `state_init.rs` and `reducer-bytecode.ts` must switch.

### F7 — MEDIUM — SSOT — `get_slot_meta` restates offsets; invalid `AggType` becomes `Sum`

Evidence: `types.rs:53-68`, `674-716`.

```
pub const CAPACITY: u32 = 4;
pub const EVICTION_INDEX_OFFSET: u32 = 24;
pub const EVICTION_INDEX_CAPACITY: u32 = 28;
pub const EVICTED_BUFFER_OFFSET: u32 = 36;
let agg_type = if matches!(slot_type, SlotType::Aggregate | SlotType::Scalar) {
    AggType::from_u8(agg_byte).unwrap_or(AggType::Sum)
} else {
    AggType::Sum
};
capacity: unsafe { meta_bytes.add(4).cast::<u32>().read_unaligned() },
eviction_index_offset: unsafe { meta_bytes.add(24).cast::<u32>().read_unaligned() },
eviction_index_capacity: unsafe { meta_bytes.add(28).cast::<u32>().read_unaligned() },
evicted_buffer_offset: unsafe { meta_bytes.add(36).cast::<u32>().read_unaligned() },
```

Problem: Same function uses `SlotMetaOffset::TYPE_FLAGS` and raw `4/24/28/36`. A moved field updates one table and not
the loads. Invalid aggregate subtype is fail-open to `Sum` while invalid `SlotType`/`DurationUnit` `die!`. If this path
is deleted with F6, the fail-open still documents the intended decode rule for whoever reads meta next
(`SlotMetaView::agg_type_byte` returns the raw byte — better). Fix: Delete with F6. If kept: every load through
`SlotMetaOffset::*`; `AggType::from_u8` miss `die!`s like the other tags. Cost/Risk: Only this function (currently
unused).

### F8 — MEDIUM — TESTS — Discriminant tests restate incomplete variant lists

Evidence: `types.rs:889-936` vs enum at `120-134` and `329-459`; `opcodes.rs:297-330` vs enum at `15-116` and `138-149`.

```
// types.rs slot_type test: no StructMap2 = 10
assert_discriminants!(SlotType, u8;
    HashMap = 0, … Bitmap = 8, Nested = 9
);
// types.rs opcode test: no SlotStructMap2, BatchStructMapUpsertMax,
// BatchStructMap2UpsertLast, BatchStructMap2Remove, BatchStructMap2UpsertMaxI64x2
// opcodes.rs opcode test: same holes vs its own enum (which has those variants)
```

Problem: PERFORMANCE-HANDBOOK §7.10bb: a guard that cannot go red is not a guard. Adding `Opcode::Foo = 0x99` does not
fail these tests. The real tripwire is `registry_audit.rs` source harvest vs fixture — these unit tests are a second,
weaker, drifting copy of the same table. Fix: Delete `assert_discriminants!` opcode/slot tests. Keep
`types_rs_from_u8_matches_declarations` and the fixture harvest. If a unit test stays, iterate `0..=255` through
`from_u8` / exhaustiveness, do not paste variant lists. Cost/Risk: Test-only.

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

- `columine-vm` `SlotMetaView` (`src/meta.rs`) is the live slot-meta reader; this crate's `get_slot_meta` is unused. VM
  owns the cutover.
- `columine-vm` `parse_program` (`src/state_init.rs:167-182`) and TS `parseReducerProgram` re-decode the 14-byte header
  instead of `ProgramHeader::from_wire_bytes`.
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
- `types.rs` `Opcode::from_u8` matches the enum (enforced by `types_rs_from_u8_matches_declarations`). `opcodes.rs` has
  no `from_u8`.
- `unsafe` on `get_slot_meta` / column getters has a `# Safety` section (helpers themselves unused).
- No hot-loop `to_vec`/`clone`/`format!` in this crate. `audit_parser` `String` harvests are test-regime.
- `next_power_of_2` floor at 16 is a domain rule, tested. Not a copy bug.
- Dual-registry Nested split is intentional (`registry_audit.rs:7-8`); the finding is the copied non-Nested remainder
  plus two public types, not the Nested presence itself.
