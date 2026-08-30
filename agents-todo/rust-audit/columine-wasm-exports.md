# columine wasm exports

Scope: `packages/columine/crates/columine-wasm/src/lib.rs` (884), `packages/columine/crates/columine-ep-wasm/src/lib.rs`
(264), `packages/columine/Cargo.toml` (62), `packages/columine/Cargo.lock` (921), `packages/columine/src/{index.ts`
(85), `types.ts` (470), `wasm-backend.ts` (785), `parse-backend.ts` (1100), `wasm-memory-contract.ts` (118),
`pipeline.ts` (283), `reducer-bytecode.ts` (314), `__tests__/wasm-memory-contract.test.ts`,
`__tests__/package-exports.test.ts`, `__tests__/opcode-registry.test.ts`,
`__tests__/debug-memory.ts`}`. Supporting reads (not owned): crate `Cargo.toml`/`build.rs`/`tests/{export_checklist,smoke}.rs`for both wasm crates,`packages/columine/justfile`(49),`packages/columine/.cargo/config.toml`(6),`columine-types`
Opcode/ErrorCode/SlotType tables for SSOT comparison.

## Summary

- HIGH: TS `VmExports` binds 30 of 62 `vm_*` exports; the other 32 have zero TS callers in this repo. No generator.
  `columine-types` should be the source.
- HIGH: TS `ErrorCode`/`Opcode`/`SlotType`/layout constants are hand-restated against `columine-types` and have already
  diverged (`ColumnUnderrun=8` and `Nested=9` absent on the TS side).
- HIGH: Every reduce call copies state+program+columns into linear memory and copies state back out. `cols_vec` also
  heap-allocates a `Vec<&[u8]>` per batch.
- HIGH: Documented wasm memory map (state at 64 KiB, heap below 8 MiB) is not enforced by linker flags; `wasm-loader.ts`
  does not exist; `debug-memory.ts` still queries deleted debug exports.
- MEDIUM: `wasm-perf` and `wasm-s` profiles have no script/justfile consumers; only `wasm-release` (plus its per-package
  opt-level=3 overrides) is used.
- MEDIUM: EP host still hardcodes `capacity=256` after rust stopped clamping; `ResultCode`/`RESULT_HEADER_SIZE` restated
  in TS.
- MEDIUM: `loadWasmBytes` and the wasm export-section parser are each written twice; wasm-backend grows memory without
  the parse-backend cap contract.
- LOW: Cargo.lock carries three `getrandom` and two `r-efi` versions (dev/proptest/arrow graph); unused workspace
  `criterion`; tests pin one opcode and cannot catch ABI drift.

## Findings

### F1 — HIGH — SSOT — Wasm ABI is restated by hand; 32 of 62 `vm_*` exports have no TS binding

Evidence: `packages/columine/crates/columine-wasm/src/lib.rs:1-7` (+
`packages/columine/crates/columine-wasm/tests/export_checklist.rs:1-69` +
`packages/columine/src/wasm-backend.ts:58-186`)

```
//! The artifact exports 56 `vm_*` functions plus memory, enumerated by
//! `tests/export_checklist.rs`.
```

```
pub const EXPECTED_COLUMINE_EXPORTS: [&str; 62] = [
    "vm_calculate_grown_state_size",
    ...
    "vm_undo_rollback",
];
```

```
const VM_EXPORT_NAMES = [
  'vm_calculate_state_size',
  ...
  'vm_delta_apply_rollforward_segment',
] as const;
```

Problem: Three restatements of one ABI (crate rustdoc, rust checklist, TS `VmExports`/`VM_EXPORT_NAMES`) already
disagree: rustdoc says 56, the checklist pins 62, TS binds 30 names. The 32 unbound rust exports (`vm_map_iter_*`,
`vm_set_iter_*`, `vm_struct_map_*` unary, all 22 `vm_rbmp_*`/`vm_set_rbmp_*`/`vm_get_rbmp_*`) have no caller under
`packages/columine/src/` and no caller elsewhere in this repo (grep of
`vm_map_iter_start`/`vm_rbmp_and`/`vm_struct_map_iter_start` hits only rust). `hasVmExports` only checks the TS subset,
so a wasm build that dropped an unbound export still loads. `lib.rs:15-21` still documents `resume_when` and
`vm_ax_eval` contracts that this crate does not export — leftover from the "adapted copy of the superset wasm wrapper"
(`lib.rs:9-10`). Fix: One table in `columine-types` (dependency-free) listing every export name + C signature + whether
the published `@smoothbricks/columine` host binds it. Generate `EXPECTED_COLUMINE_EXPORTS`,
`VmExports`/`VM_EXPORT_NAMES`, and the rustdoc count from that table. Decision: drop the 32 unbound exports from the
shipped `columine.wasm` unless an out-of-repo consumer is named; the checklist currently freezes dead surface.
Cost/Risk: Removing exports is an ABI break for any unpublished direct-wasm caller; generating TS from rust is a
build-step addition. Blast radius is `columine-wasm` + `wasm-backend.ts` + the two checklists.

### F2 — HIGH — SSOT — ErrorCode / Opcode / SlotType / layout constants already diverge from columine-types

Evidence: `packages/columine/src/types.ts:206-215` vs `packages/columine/crates/columine-types/src/types.rs:539-555`;
`packages/columine/src/types.ts:41-54` vs `types.rs:120-133`; `packages/columine/src/wasm-backend.ts:33-35,247-267`;
`packages/columine/src/types.ts:221-329`

```
export enum ErrorCode {
  OK = 0,
  ...
  INVALID_KEY = 7,
}
```

```
pub enum ErrorCode {
    Ok = 0,
    ...
    InvalidKey = 7,
    ColumnUnderrun = 8,
}
```

```
export enum SlotType {
  BITMAP = 8,
  STRUCT_MAP2 = 10,
}
```

```
    Nested = 9,
    StructMap2 = 10,
```

```
const STATE_HEADER_SIZE = 32;
// Must match vm.zig SLOT_META_SIZE (48 bytes with TTL/eviction fields)
const SLOT_META_SIZE = 48;
```

Problem: The executing ABI lives in rust (`columine-wasm` returns `ErrorCode as u32`; slot meta is
`STATE_HEADER_SIZE`/`SLOT_META_SIZE` from `columine-types`). TS restates the same numbers and has already lost variants
the wasm binary can return: `ColumnUnderrun = 8` is absent, so `vmErrorCode` (`wasm-backend.ts:247-267`) throws on that
status (evict path) and `executeBatch` returns a bare `8` that is not an `ErrorCode` member. `SlotType` skips
`Nested = 9`. `Opcode` in `types.ts` is missing rust variants `SlotArray=0x14`, `BatchMapUpsertLatestTtl=0x24`,
`BatchMapUpsertLastTtl=0x25`, `BatchSetInsertTtl=0x32`, `BatchScalarLatest=0x48`, `SlotNested=0x1a`,
`NestedSetInsert=0x90`, `NestedMapUpsertLast=0x92`, `NestedAggUpdate=0x95`. Comments still name `vm.zig`.
`PROGRAM_MAGIC`/`HEADER_SIZE`/`PROGRAM_HASH_PREFIX` are copied again in `types.ts:221-224` (rust: `opcodes.rs:185-191`,
`ProgramHeader::WIRE_SIZE = 14`). Fix: Generate the TS enums and the two layout constants from `columine-types`. Single
source is rust: it is what `columine.wasm` actually returns. Add `COLUMN_UNDERRUN = 8` and `NESTED = 9` on the TS side
immediately; do not leave `vmErrorCode`'s default throw as the only detector. Cost/Risk: Encoder/parser callers of
`Opcode` must learn the missing variants or the generator must mark them host-unused. `ColTypes` owns the rust tables.

### F3 — HIGH — COPIES — Host copies the whole working set in and out of linear memory on every call

Evidence: `packages/columine/src/wasm-backend.ts:1-10,305-357,443,513-515` (+
`packages/columine/crates/columine-wasm/src/lib.rs:138-145,567-574` +
`packages/columine/src/parse-backend.ts:854-896,1018-1020`)

```
 *   - State copied INTO WASM memory before reduce
 *   - State copied OUT of WASM memory after reduce
 *   - Input columns copied into WASM memory for each batch
```

```
    wasmU8.set(new Uint8Array(state.buffer), statePtr);
    wasmU8.set(program.bytecode, programPtr);
    ...
      wasmU8.set(bytes, cursor);
```

```
      new Uint8Array(state.buffer).set(new Uint8Array(wasmInstance.memory.buffer, statePtr, state.size));
```

```
unsafe fn cols_vec<'a>(col_ptrs: *const *const u8, num_cols: u32) -> Vec<&'a [u8]> {
    let mut cols = Vec::with_capacity(num_cols as usize);
    for i in 0..num_cols as usize {
        let p = unsafe { *col_ptrs.add(i) };
        cols.push(unsafe { state_ref(p) });
    }
    cols
}
```

```
    r.delta_undo = r.vm.delta_export_undo_bytes();
    r.delta_redo = r.vm.delta_export_redo_bytes();
```

Problem: Regime is the hot reduce path (every `executeBatch` / `executeBatchDelta`), not startup. State already lives in
a JS `ArrayBuffer` (`WasmStateHandle.buffer`); the host then memcpy's it into wasm linear memory, memcpy's every input
column, and memcpy's the whole state back. That is a second copy of bytes the VM could have been given as a view.
Independently, `cols_vec` heap-allocates a `Vec<&[u8]>` on every batch (L7 / handbook §7.2 — size is `num_cols`, a
closed form, but the allocation is still inside the call). `vm_delta_export_segment` materializes two `Vec<u8>` lanes in
the rust runtime, then TS `.slice()`s them again (`wasm-backend.ts:688-690`). Parse/Compact is better on the rust side
(caller-owned in/out buffers, `ep-wasm/src/lib.rs:7-11`) but JS still `memory.set`s input and copies Arrow IPC out
(`parse-backend.ts:855,895-896,1018-1020`). Fix: Keep state in wasm linear memory (handle = `{ptr, len}` into the
exported `memory`) and pass column pointers that already live there; delete `copyStateIn` / the post-execute copy-out.
Replace `cols_vec` with a stack array or a reused scratch (`num_cols` is tiny and known). For delta export, return
pointers into the VM's existing undo buffer instead of `Vec` materialization + JS `.slice()`. Parse/Compact: return a
view into `memory.buffer` (or a transferred `ArrayBuffer` slice) instead of allocating `new Uint8Array(arrowLen)` and
copying. Cost/Risk: GC story changes (state is no longer a standalone JS `ArrayBuffer`). Growth (`vm_grow_state`)
already relocates; the host would update the handle pointer instead of swapping buffers. Column producers that today
hand JS typed arrays would write into wasm memory once at ingest.

### F4 — HIGH — STRUCTURE — Memory map and Runtime singleton are unenforced; debug overlap probe is dead

Evidence: `packages/columine/crates/columine-wasm/build.rs:1-13` (+
`packages/columine/src/wasm-backend.ts:37-38,283-287` + `packages/columine/src/__tests__/debug-memory.ts:4-46` +
`packages/columine/crates/columine-wasm/src/lib.rs:33-67`)

```
    // wasm-backend.ts and wasm-loader.ts: stack [0, 1 MiB), JS state at 64 KiB
    // inside its lower band, module data/BSS from 1 MiB, and JS input/output
    // regions from 8 MiB (MIN_INPUT_REGION_OFFSET). The Rust heap must stay
    // below 8 MiB
        println!("cargo::rustc-link-arg=--initial-memory=4194304");
        println!("cargo::rustc-link-arg=--max-memory=268435456");
```

```
const STATE_REGION_OFFSET = WASM_PAGE_SIZE;
...
  const importedMemory = new WebAssembly.Memory({ initial: memoryPages });
  const instance = await WebAssembly.instantiate(wasmModule, { env: { memory: importedMemory } });
  const memory = getExportedMemory(instance.exports) ?? importedMemory;
```

```
type DebugVmExports = {
  vm_debug_shadow_addr: () => number;
  vm_debug_undo_entries_addr: () => number;
};
```

```
static mut RUNTIME: Option<Runtime> = None;
```

Problem: `build.rs` emits only initial/max memory. It does not pass `--stack-first`, `--global-base`, or
`--import-memory`. `wasm-loader.ts` and `MIN_INPUT_REGION_OFFSET` do not exist in this tree (grep hits only that
comment). TS still writes agent state at 64 KiB into the _exported_ memory (`STATE_REGION_OFFSET`), after constructing
an imported `WebAssembly.Memory` the module does not import — `getExportedMemory` discards it when `memory` is exported
(which `export_checklist.rs:156-158` requires). `.cargo/config.toml:1-4` still talks about `build.zig`.
`debug-memory.ts` exists to detect state/shadow/undo overlap, but it instantiates looking for `vm_debug_shadow_addr` /
`vm_debug_undo_entries_addr`, which `columine-wasm` does not export — the probe cannot run against the shipping
artifact. Separately, `Runtime` is one `static mut` per wasm instance: undo log, `last_evicted_count`,
`delta_undo`/`delta_redo`, `rbmp_scratch` are shared across every JS `StateHandle` created by that backend
(`wasm-backend.ts:4-7` documents one instance, many agent buffers). `vm_undo_checkpoint` ignores its `state_base`
(`lib.rs:542-544`); `undoRollback` in TS (`wasm-backend.ts:663-667`) does not `copyStateIn` first. Fix: Encode the map
in linker args (`--global-base` / `--stack-first` / `__heap_base` assertion in `build.rs`) or delete the comment and
treat linear memory as one bump allocator the host asks rust to place into. Drop the unused `env.memory` import. Either
re-export the two debug address getters and make `debug-memory.ts` a real test, or delete it. Put undo/delta/evict
scratch on the state blob, not on `RUNTIME`, if multiple `StateHandle`s on one instance is a supported API (it is:
`createState` is per-instance). Cost/Risk: Linker-map change can shift every pointer the TS host hardcodes. Moving undo
off `RUNTIME` touches `columine-vm` (owned by another slice).

### F5 — MEDIUM — DEP-BLOAT — `wasm-perf` and `wasm-s` are dead profiles

Evidence: `packages/columine/Cargo.toml:28-52` (+ `packages/columine/justfile:20-48`)

```
[profile.wasm-release]
inherits = "release"
opt-level = "z"
...
[profile.wasm-perf]
inherits = "release"
opt-level = 3
...
[profile.wasm-s]
inherits = "release"
opt-level = "s"
...
[profile.wasm-release.package.columine-vm]
opt-level = 3
[profile.wasm-release.package.columine-parsing]
opt-level = 3
```

```
    cargo build -p columine-wasm --target wasm32-unknown-unknown --profile wasm-release
...
    cargo build -p columine-ep-wasm --target wasm32-unknown-unknown --profile wasm-release
```

Problem: Grep of `packages/columine` for `wasm-perf` / `wasm-s` / `--profile wasm-s` hits only the profile definitions.
Shipping and checklist paths use `wasm-release` only. The per-package `opt-level = 3` overrides on `columine-vm` and
`columine-parsing` _are_ live (they attach to `wasm-release`). `wasm-perf` was the handbook-§4.1 escape from opt-z
outlining; it is now unused, so any size-vs-speed attribution has no recipe. Workspace `criterion` (`Cargo.toml:19`)
does not appear in `Cargo.lock` at all. Fix: Delete `[profile.wasm-perf]`, `[profile.wasm-s]`, and workspace `criterion`
unless a just recipe is added that builds `--profile wasm-perf` for measurement (PH-4.1: do not measure the opt-z
artifact and call it speed). Keep `wasm-release` + the two package overrides. Cost/Risk: None if nothing invokes them.
If an out-of-tree script uses `--profile wasm-perf`, that script breaks — none is in this package.

### F6 — MEDIUM — SSOT — EP ABI: TS still clamps create-capacity to 256; result codes restated

Evidence: `packages/columine/crates/columine-ep-wasm/src/lib.rs:28-32,111-137` (+
`packages/columine/src/parse-backend.ts:90-129,831-847,925-931` +
`packages/columine/crates/columine-event-processor/src/lib.rs:50-67`)

```
/// clamped every wasm instance to 256 events regardless of the requested
/// capacity; the requested capacity is honored now.
const MAX_EVENT_CAPACITY: u32 = 1 << 20;
```

```
        handle = fieldNamesBuffer
          ? wasm.ep_create_with_schema_and_names(
              256,
```

```
        handle = wasm.ep_create_with_schema(
          256,
```

```
const WASM_OUTPUT_HEADER_SIZE = 32;
const RESULT_OK = 0;
const COMPACT_STATUS_CODE = {
  1: 'INVALID_HANDLE',
  ...
  7: 'SCHEMA_MISMATCH',
} as const;
```

```
pub const RESULT_HEADER_SIZE: usize = 32;
pub enum ResultCode {
    Ok = 0,
    InvalidHandle = 1,
    ...
    SchemaMismatch = 7,
}
```

Problem: Rust export layer and TS host disagree on the capacity argument the rust side documents as load-bearing. TS
`MAX_EVENTS_PER_BATCH = 65_536` (`parse-backend.ts:98`) cannot be honored by a 256-event EP instance.
`EventProcessorWasmExports` matches the six rust exports (`ep_version`, `ep_create_with_schema`,
`ep_create_with_schema_and_names`, `ep_destroy`, `ep_create_log_entry`, `ep_compact`) — that side is aligned — but
`ep_version` is required at load (`parse-backend.ts:60-81`) and never called. `EXPECTED_COLUMINE_EP_EXPORTS` is still a
5-name list without `ep_compact` (`columine-ep-wasm/tests/export_checklist.rs:5-11`) beside `COLUMINE_EP_EXPORTS` of 6.
`VERSION: u32 = 2` (`ep-wasm/src/lib.rs:26`) has no TS constant. Header size 32 and the seven `ResultCode` values are
copied into TS. Fix: Pass `MAX_EVENTS_PER_BATCH` (or a derived closed form) as `capacity`. Generate
`EventProcessorWasmExports`, `COMPACT_STATUS_CODE`, `WASM_OUTPUT_HEADER_SIZE`, and `VERSION` from the rust EP ABI
module. Delete `EXPECTED_COLUMINE_EP_EXPORTS`. Call `ep_version()` at instantiate and refuse mismatch. Cost/Risk: Larger
EP column buffers; need to confirm `EventProcessor::with_column_capacity` cost (owned by `ColEventProc`).

### F7 — MEDIUM — DUPLICATION — Host helpers and memory growth are each written twice

Evidence: `packages/columine/src/wasm-backend.ts:235-237,297-303,747-785` (+
`packages/columine/src/parse-backend.ts:242,87-88,1065-1100` +
`packages/columine/src/wasm-memory-contract.ts:1-3,87-109` + both `tests/export_checklist.rs` `wasm_exports`)

```
function align8(n: number): number {
  return Math.ceil(n / 8) * 8;
}
const ensureMemory = (endExclusive: number): void => {
    ...
    if (missing > 0) wasmInstance.memory.grow(Math.ceil(missing / WASM_PAGE_SIZE));
};
```

```
export const WASM_PAGE_BYTES = 64 * 1024;
export const WASM_MAX_PAGES = 4096;
export function ensureWasmMemoryForWorkingSet(...)
```

Problem: `loadWasmBytes` in `wasm-backend.ts:747-785` and `parse-backend.ts:1065-1100` is the same function (the parse
copy even says "Follows the same pattern as wasm-backend.ts"). `align8` is defined in both files. Page size is
`WASM_PAGE_SIZE` in wasm-backend and `WASM_PAGE_BYTES` in the memory contract; wasm-backend never imports the contract
and grows without `WASM_MAX_PAGES`, while parse-backend caps at 4096 pages (matching `build.rs --max-memory=268435456`).
The wasm section-7 parser is copy-pasted between `columine-wasm/tests/export_checklist.rs:71-109` and
`columine-ep-wasm/tests/export_checklist.rs:22-60`. Fix: One `loadWasmBytes` / `align8` in `wasm-memory-contract.ts` (or
a tiny `wasm-host.ts`). Route wasm-backend growth through `ensureWasmMemoryForWorkingSet`. One `wasm_exports` helper
used by both checklists (or generate both checklists from the F1 table). Cost/Risk: wasm-backend currently grows past
the parse cap until the engine's max-memory trap; unifying on the contract is a behavior change for oversized reduce
states (fail closed instead of trap).

### F8 — LOW — TESTS — Guards cannot catch the ABI drift this slice is for

Evidence: `packages/columine/src/__tests__/opcode-registry.test.ts:5-8` (+
`packages/columine/src/__tests__/debug-memory.ts:13-27` +
`packages/columine/crates/columine-wasm/tests/export_checklist.rs:111-121` +
`packages/columine/src/__tests__/wasm-memory-contract.test.ts:19-87`)

```
test('BATCH_STRUCT_MAP_UPSERT_MAX keeps the public 0x82 ABI value', () => {
  expect(Opcode.BATCH_STRUCT_MAP_UPSERT_MAX).toBe(0x82);
  expect(Opcode[0x82]).toBe('BATCH_STRUCT_MAP_UPSERT_MAX');
});
```

```
fn export_list_is_complete_and_deduped() {
    let mut names: Vec<&str> = EXPECTED_COLUMINE_EXPORTS.to_vec();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), EXPECTED_COLUMINE_EXPORTS.len(), ...);
}
```

Problem: Substitution test (handbook §7.10bb): deleting any Opcode except `0x82` does not turn `opcode-registry.test.ts`
red. `export_list_is_complete_and_deduped` only asserts the rust array has unique names — it cannot go red if TS
`VM_EXPORT_NAMES` drifts, which is the live F1 bug. `debug-memory.ts` fails on missing debug exports before it can
report overlap (L8: the fixture is part of the system, and this fixture is aimed at a previous binary).
`wasm-memory-contract.test.ts` never instantiates `columine.wasm` / `event_processor.wasm` and does not cover
wasm-backend's uncapped `ensureMemory`. `rbmp_serialized_ops_route_through_the_env` (`smoke.rs:111-123`) asserts
empty∧empty → 0; that passes if `vm_rbmp_and` is a no-op. Fix: One test that diffs `VM_EXPORT_NAMES` against the
generated export table (F1) and diffs TS `ErrorCode`/`Opcode`/`SlotType` discriminants against `columine-types`. Point
`debug-memory.ts` at real exports or delete it. Give the rbmp smoke a non-empty bitmap pair. Cost/Risk: Needs a built
wasm or a source-level export list; the ignored `built_wasm_exports_expected_symbols_and_memory` already requires
`just wasm`.

### F9 — LOW — DEP-BLOAT — Lockfile duplicate versions are real but off the shipped wasm crates' direct graph

Evidence: `packages/columine/Cargo.lock:337-368,585-595` (+ `packages/columine/crates/columine-wasm/Cargo.toml:13-15` +
`packages/columine/crates/columine-ep-wasm/Cargo.toml:14-16`)

```
name = "getrandom"
version = "0.2.17"
...
name = "getrandom"
version = "0.3.4"
...
name = "getrandom"
version = "0.4.3"
...
name = "r-efi"
version = "5.3.0"
...
name = "r-efi"
version = "6.0.0"
```

Problem: Three `getrandom` and two `r-efi` versions sit in the workspace lockfile
(proptest/tempfile/const-random/ahash). Direct wasm-crate deps are thin and load-bearing: `columine-wasm` →
`columine-types` + `columine-vm`; `columine-ep-wasm` → `columine-event-processor` + `columine-arrow`. `roaring` is a
_dev_-dep of `columine-vm` (comment: must not re-enter the shipped artifact) — do not recommend shelling it out of this
slice; the `vm_rbmp_*` wrappers call in-tree `bitmap_ops`. `wasm-bindgen` appears in the lockfile via
`chrono`/`iana-time-zone` from the arrow graph; whether it links into `event_processor.wasm` is not shown without a
build (forbidden here). Fix: No change in the two wasm manifests. Duplicate `getrandom`/`r-efi` are a workspace-lock
cleanup for the arrow/proptest owners. Do not add `wasm-bindgen` as a direct dep; if `ColArrow`/`ColEventProc` confirm
it links, disable chrono's clock/iana features for wasm32. Cost/Risk: Feature-gating chrono is an arrow-crate change,
not this slice.

## Cross-slice questions

- `ColTypes`: `columine-types/src/types.rs` and `opcodes.rs` both define `Opcode`, `ErrorCode`, `SlotType`,
  `STATE_HEADER_SIZE`, `SLOT_META_SIZE`. This slice treats `types.rs` as the ABI the wasm crate actually imports
  (`columine-wasm` uses `columine_types::types::ErrorCode` and
  `columine_types::opcodes::DEFAULT_ACCEPTED_PROGRAM_MAGICS`). Which file is the generator source for the TS enums in
  F2?
- `ColEventProc`: does `EventProcessor::with_column_capacity(256)` drop/refuse JSON with more than 256 events, making F6
  a live parse bug rather than a stale constant?
- `ColArrow` / `ColEventProc`: does the `event_processor.wasm` cdylib actually link
  `wasm-bindgen`/`chrono`/`iana-time-zone` (present in `Cargo.lock` via `arrow-array`)?
- `ColVmCore`: `Runtime.vm` undo/delta/bitmap_env is process-global in the wasm wrapper. Is per-state undo supposed to
  live in `columine-vm` so F4 can stop stashing it on `static mut RUNTIME`?
- Whoever owns a consumer "superset" wasm wrapper: F1's 32 unbound exports and the `resume_when`/`vm_ax_eval` comments
  look copied from that crate. If that wrapper is the real ABI owner, this crate should not duplicate it.

## Non-findings (checked, clean)

- `columine-wasm` / `columine-ep-wasm` direct deps are load-bearing (types+vm; event-processor+arrow). No
  openssl/git2-class crate on these two manifests. `roaring` is not a wasm-crate dep.
- EP six-function export list matches `EventProcessorWasmExports` 1:1 (names and arity). `ep_compact` overlap check
  (`ep-wasm/src/lib.rs:252-256`) is real.
- `wasm-release` + `columine-vm`/`columine-parsing` opt-level=3 overrides are used by `just wasm` / `just wasm-ep`.
- Pointer bounding: wasm32 `bound_of` uses `memory_size(0)` (`lib.rs:73-78`); native tests require `__register_region`.
  Single-threaded `static mut` is documented.
- `FLAT_UNDO_ENTRY_SIZE` is not restated in TS; the host asks `vm_delta_export_entry_size`.
- `parse-backend.ts` caller-buffer protocol matches `ep-wasm` rustdocs (input/output ptr+len,
  `[ResultHeader][Arrow IPC]`).
- No `unsafe` in the two wasm `lib.rs` files lacks a crate-level contract; the two called-out exceptions in the allow
  (`resume_when`, `vm_ax_eval`) are themselves stale (F1), not missing safety comments on live functions.
- `export_checklist` built-artifact tests (ignored) do require exported `memory` (kind 2), which matches TS
  `getExportedMemory`.
