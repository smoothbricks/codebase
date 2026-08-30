# XCUT rust-vs-typescript duplication

Scope: FFI/ABI seams only (no crate-internal audits). Files read, with line counts:

Cowshed TS: `packages/cowshed/src/types.ts` (353), `index.ts` (484), `native.ts` (133), `platform.ts` (12), `cli.ts`
(8), `cli-trampoline.ts` (164), `cli-trampoline.test.ts` (254), `native.test.ts` (98). Scripts/skills:
`packages/cowshed/scripts/verify-packaged-bin-bits.ts` (52), `packages/cowshed/skills/cowshed/SKILL.md`. Rust seam:
`packages/cowshed/crates/cowshed-napi/src/lib.rs` (1036), `cowshed-core/src/error.rs` (177),
`cowshed-core/src/api/dto.rs` (2517; DTO/option/JobInfo/CommandArg ranges), `cowshed-core/src/metadata.rs`
(GrantSet/ImageFormat/SimVerb ranges), `cowshed-core/tests/public_api_contracts.rs` (LandOptions/JobInfo JSON),
`cowshed-cli/src/args.rs` + `runtime.rs` (land `--no-retire`).

Columine TS: `packages/columine/src/types.ts` (470), `wasm-backend.ts` (785), `parse-backend.ts` (1100),
`reducer-bytecode.ts` (314), `wasm-memory-contract.ts` (118), `index.ts` (85), `__tests__/opcode-registry.test.ts`,
`__tests__/columine-integration.test.ts` (scalar 0x48 path). Rust seam: `columine-types/src/opcodes.rs` (384),
`types.rs` (1114), `lib.rs` (12), `columine-wasm/src/lib.rs` (884), `columine-wasm/tests/export_checklist.rs`,
`columine-ep-wasm/src/lib.rs` (264), `columine-event-processor/src/lib.rs` (ResultCode), `compact.rs` (header
constants), `columine-vm/src/hashmap_ops.rs` (CmpType).

LMAO TS seam: `packages/lmao/src/lib/schema/systemSchema.ts` (341), `lib/wasm/wasmAllocator.ts` (514),
`lib/wasm/wasmPhysicalLayout.ts` (~309). Rust seam: `lmao-rs/crates/lmao-wasm/src/lib.rs` (523),
`lmao-core/src/entry_type.rs` (82), `lmao-arena/src/lib.rs` (284).

## Summary

- CRITICAL: cowshed `JobInfo.argv` is `string[]` in TS; Rust serializes tagged `{encoding,data}` objects. `parseJobInfo`
  will reject every real job JSON.
- CRITICAL: columine `Opcode` in TS is a stale subset of the VM bytecode the Rust VM dispatches (TTL, scalar-latest,
  nested). Integration tests already emit `0x48` as a raw byte.
- CRITICAL: columine `ErrorCode` omits `ColumnUnderrun = 8`; the WASM host throws "unknown status" on a legal VM return.
- HIGH: `Opcode`/`SlotType`/`ErrorCode` exist twice inside `columine-types` (`opcodes.rs` vs `types.rs`) and already
  disagree on nested ops / `Nested = 9`.
- HIGH: lmao `SizeClass.Identity = 4` exists only in TS; the WASM `size_class` mapper folds every unknown byte onto
  `Col8B`.
- HIGH: cowshed `JobInfo.exit`/`stdout`/`stderr`/`stdin` are `unknown`, so typia does not check the Rust wire shapes.
- HIGH: the only TS opcode-registry test pins `0x82` and cannot go red on missing opcodes.
- MEDIUM: compact CPB1 header, lmao entry types 1–24, cowshed ErrorCode/DTO unions, and WASM layout sizes are
  hand-restated on both sides (currently agreeing where checked).
- Generation direction: cowshed and columine wire/bytecode SSOT is Rust; generate TS (typia schemas / opcode enum) from
  it. LMAO entry-type SSOT is TS; generate the Rust `EntryType` enum from it.
- Stale "Must match Zig" comments in columine TS are leftover from the Zig→Rust port, not a second runtime.

## Findings

### F1 — CRITICAL — SSOT — cowshed JobInfo.argv type diverged from the CommandArg wire

Evidence: `packages/cowshed/src/types.ts:257-264` + `packages/cowshed/crates/cowshed-core/src/api/dto.rs:780-799` +
`packages/cowshed/src/index.ts:110-111,372-373,419-420,435-436`

```ts
export interface JobInfo {
  readonly argv: readonly string[];
  // ...
}
const parseJobInfo = typia.json.createAssertParse<JobInfo>();
return parseJobInfos(await callNativeAsync(() => this.#native.listJobs()));
```

```rust
Ok(data) => CommandArgRef { encoding: CommandArgEncoding::Utf8, data }.serialize(serializer)
```

Rust JobInfo JSON (proven in `cowshed-core/src/api/capability.rs:1933`) is
`"argv": [{"encoding": "utf8", "data": "true"}]`. typia asserting `string[]` rejects that object. `listJobs` / `status`
/ `wait` cannot parse a real job.

Problem: TS restated argv as a UTF-8 string list. The napi JSON is the tagged CommandArg DTO. The copies no longer
agree.

Fix: Make `packages/cowshed/src/types.ts` `JobInfo.argv` the tagged union
`{ encoding: 'utf8' | 'base64'; data: string }[]` (or a branded `CommandArg`). Rust `dto.rs` `CommandArg` is the single
source; generate the TS type from it. Keep `ExecRequest.argv` as `string[]` — that is a different napi wire
(`NapiExecRequest.argv: Vec<String>` at `cowshed-napi/src/lib.rs:111`).

Cost/Risk: every TS caller of `JobInfo.argv` must decode `data`. Blast radius is the cowshed npm API, not the
controller.

### F2 — CRITICAL — SSOT — columine Opcode tables have already diverged

Evidence: `packages/columine/src/types.ts:226-329` vs
`packages/columine/crates/columine-types/src/opcodes.rs:38-41,66,87` vs
`packages/columine/crates/columine-types/src/types.rs:450-459` + live emission at
`packages/columine/src/__tests__/columine-integration.test.ts:401`

TS `Opcode` is missing VM-live bytes that Rust both names and dispatches:

| byte                 | Rust (`opcodes.rs` / `types.rs`) | TS `Opcode` |
| -------------------- | -------------------------------- | ----------- |
| `0x14`               | `SlotArray`                      | absent      |
| `0x1a`               | `SlotNested` (`types.rs` only)   | absent      |
| `0x24`               | `BatchMapUpsertLatestTtl`        | absent      |
| `0x25`               | `BatchMapUpsertLastTtl`          | absent      |
| `0x32`               | `BatchSetInsertTtl`              | absent      |
| `0x48`               | `BatchScalarLatest`              | absent      |
| `0x90`/`0x92`/`0x95` | nested ops (`types.rs` only)     | absent      |

```ts
reduceOps: [0x48, 0, 0, 3, 0x48, 1, 1, 3, 0x48, 2, 2, 3],
```

`parseReducerSlotDefs` (`reducer-bytecode.ts:272-273`) throws `unknown init opcode` on `0x14`/`0x1a`. The public TS enum
cannot even name `0x48`, so the integration test pokes the VM with a raw literal.

Problem: three hand-maintained opcode tables. TS is behind the executable VM; `opcodes.rs` is behind `types.rs` on
nested ops. The compiler-facing ABI and the dispatch ABI are different languages' copies of one number space.

Fix: delete the TS enum and the extra Rust enum. Single source is the dispatch table the VM actually executes
(`columine-types` — after collapsing `opcodes.rs`/`types.rs`; see F4). Generate `packages/columine/src/types.ts`
`Opcode` from that table (build.rs → `.ts`, or a JSON registry both compile). Direction: Rust → TS. Do not generate Rust
from today's TS; TS is the stale copy.

Cost/Risk: any TS bytecode encoder outside this package that imports `Opcode` must be regenerated in the same change.
`parseReducerSlotDefs` must gain the missing init ops or keep failing closed on them — that choice is the product
decision; the table must still name every byte the VM accepts.

### F3 — CRITICAL — SSOT — columine ErrorCode missing ColumnUnderrun=8

Evidence: `packages/columine/src/types.ts:206-215` + `packages/columine/crates/columine-types/src/opcodes.rs:268-282` +
`packages/columine/src/wasm-backend.ts:247-267`

```ts
export enum ErrorCode {
  OK = 0,
  // ...
  INVALID_KEY = 7,
}
```

```rust
pub enum ErrorCode {
    Ok = 0,
    // ...
    InvalidKey = 7,
    ColumnUnderrun = 8,
}
```

```ts
case ErrorCode.INVALID_KEY:
  return ErrorCode.INVALID_KEY;
default:
  throw new Error(
    `WASM VM returned unknown status ${status}: the TypeScript ErrorCode enum is out of sync ` +
      'with the loaded columine.wasm binary. ...',
  );
```

`vmErrorCode` is exhaustive against the TS enum and therefore throws on the legal WASM `u32` 8. Dispatch tests in
`columine-vm` document `COLUMN_UNDERRUN` as the named refusal for a short column. The TS host converts that refusal into
an untyped throw.

Problem: the WASM status ABI grew a member; the TS mirror did not. The switch even admits this is a contract violation.

Fix: add `COLUMN_UNDERRUN = 8` to `packages/columine/src/types.ts` and a `case` in `vmErrorCode`. Then generate
ErrorCode from Rust (`columine-types`) so this cannot happen again. Direction: Rust → TS.

Cost/Risk: `ColumineBackend.executeBatch` callers that only special-case 0/1/2 (the JSDoc at `types.ts:386` is itself
stale) must treat 8 as a typed error, not a crash.

### F4 — HIGH — SSOT — columine-types hosts two Opcode/SlotType/ErrorCode registries that disagree

Evidence: `packages/columine/crates/columine-types/src/lib.rs:1-12` + `opcodes.rs:12-116,135-149` +
`types.rs:120-134,329-464`

`lib.rs` keeps `opcodes` and `types` as "distinct contracts". Both define `Opcode`, `SlotType`, `AggType`,
`StructFieldType`, `ErrorCode`, `PROGRAM_MAGIC`, `STATE_MAGIC`, `SLOT_META_SIZE`. They already disagree:

- `types.rs` `SlotType::Nested = 9`; `opcodes.rs` `SlotType` jumps 8 → 10 (same skip as TS).
- `types.rs` `Opcode` has `SlotNested = 0x1a`, `NestedSetInsert = 0x90`, `NestedMapUpsertLast = 0x92`,
  `NestedAggUpdate = 0x95`; `opcodes.rs` `Opcode` does not, even though its file header names those nested ops as
  implemented.

Problem: there is no single Rust source for the bytecode ABI, so TS has nothing truthful to generate from. This is the
in-language copy of F2.

Fix: one module. Keep `types.rs` (it matches dispatch, including nested) and make `opcodes.rs` a `pub use` façade, or
the reverse after adding the missing variants. Delete the duplicate enums/constants. ColTypes owns the crate; this slice
only names the seam.

Cost/Risk: every `columine_types::opcodes::Opcode` vs `columine_types::types::Opcode` import must collapse. Tests in
`opcodes.rs`/`types.rs` that re-assert the same discriminants become one table.

### F5 — HIGH — SSOT — lmao SizeClass.Identity=4 is TS-only; WASM maps unknown to Col8B

Evidence: `packages/lmao/src/lib/wasm/wasmAllocator.ts:130-139` +
`packages/lmao-rs/crates/lmao-arena/src/lib.rs:27-43` + `packages/lmao-rs/crates/lmao-wasm/src/lib.rs:130-136`

```ts
export enum SizeClass {
  SpanSystem = 0,
  Col1B = 1,
  Col4B = 2,
  Col8B = 3,
  Identity = 4,
}
```

```rust
pub const NUM_SIZE_CLASSES: usize = 4;
pub enum SizeClass { SpanSystem = 0, Col1B = 1, Col4B = 2, Col8B = 3 }
fn size_class(sc: u8) -> SizeClass {
    match sc { 0 => SpanSystem, 1 => Col1B, 2 => Col4B, _ => Col8B }
}
```

Arena comments (`lmao-arena/src/lib.rs:14-15`) say identity is a separate fixed-size class, not size-class 4. No TS
caller currently passes `SizeClass.Identity` (definition is the only hit). If anyone does, `get_freelist_len(4, cap)`
silently reports the Col8B freelist.

Problem: TS invented a discriminant the ABI does not have, and the WASM mapper is not fail-closed.

Fix: delete `Identity = 4` from the TS enum. Identity stays on `alloc_identity_*`. Make `size_class` return a sentinel /
ignore unknown rather than `_ => Col8B`. SSOT is `lmao-arena::SizeClass` (4 classes); generate the TS enum from it.
Direction: Rust → TS for this ABI (the opposite of entry types).

Cost/Risk: none if Identity is unused; one deleted variant.

### F6 — HIGH — SSOT — cowshed JobInfo output/exit/stdin are `unknown` against typed Rust DTOs

Evidence: `packages/cowshed/src/types.ts:257-274` +
`packages/cowshed/crates/cowshed-core/src/api/dto.rs:1475-1492,1167-1177,1068-1074`

```ts
readonly exit?: unknown;
readonly stdout: unknown;
readonly stderr: unknown;
readonly stdin: unknown;
readonly outputLimit?: unknown;
```

Rust `JobInfo` carries `Option<ExitStatus>` (`{kind, code}` / `{kind, signal, coreDumped}`), `StreamInfo`
(storage/bytes/sha256/summary), `StdinInfo`, `Option<OutputLimitInfo>`. typia `createAssertParse<JobInfo>()` accepts any
JSON in those slots, so a truncated or swapped stream object survives the TS boundary after argv (F1) is fixed.

Problem: the TS type is not a restatement of the DTO; it is a hole. Validation that exists in `JobInfo::validate`
evaporates at the napi JSON parse.

Fix: copy the tagged unions from `dto.rs` into `types.ts` (or generate them). `ExitStatus`, `StreamInfo`, `StdinInfo`,
`OutputLimitInfo` belong next to `JobInfo`. Direction: Rust → TS.

Cost/Risk: TS callers currently treating those fields as `unknown` must narrow; that is the point.

### F7 — HIGH — TESTS — opcode-registry test cannot go red on F2

Evidence: `packages/columine/src/__tests__/opcode-registry.test.ts:5-8`

```ts
test('BATCH_STRUCT_MAP_UPSERT_MAX keeps the public 0x82 ABI value', () => {
  expect(Opcode.BATCH_STRUCT_MAP_UPSERT_MAX).toBe(0x82);
  expect(Opcode[0x82]).toBe('BATCH_STRUCT_MAP_UPSERT_MAX');
});
```

PERFORMANCE-HANDBOOK §7.10bb: a guard that cannot go red is not a guard. Deleting `0x24`/`0x48`/`0x1a` from the TS enum
(already gone) does not fail this file. The substitution test is one hardcoded success.

Fix: generate the TS enum and assert `Object.values(Opcode)` equals the Rust dispatch byte set
(`abi_registry_fixture.rs` `DISPATCHED_OPCODE_BYTES`, including nested). One table, two languages, one test that fails
if either drops a byte.

Cost/Risk: the fixture becomes load-bearing; that is cheaper than a third opcode enum.

### F8 — MEDIUM — SSOT — CPB1 compact header and ResultCode restated in parse-backend.ts

Evidence: `packages/columine/src/parse-backend.ts:90-129` +
`packages/columine/crates/columine-event-processor/src/compact.rs:14-17` +
`packages/columine/crates/columine-event-processor/src/lib.rs:50-67` +
`packages/columine/crates/columine-ep-wasm/src/lib.rs:97-99`

```ts
const COMPACT_MAGIC = 0x3142_5043;
const COMPACT_VERSION = 1;
const COMPACT_HEADER_SIZE = 16;
const COMPACT_DESCRIPTOR_SIZE = 32;
const COMPACT_STATUS_CODE = {
  1: 'INVALID_HANDLE',
  2: 'PARSE_ERROR',
  3: 'ENCODE_ERROR',
  4: 'OUT_OF_MEMORY',
  5: 'INVALID_FORMAT',
  6: 'INVALID_INPUT',
  7: 'SCHEMA_MISMATCH',
};
```

```rust
pub const COMPACT_BATCH_MAGIC: u32 = 0x3142_5043;
pub const COMPACT_ABI_VERSION: u16 = 1;
pub enum ResultCode { Ok = 0, InvalidHandle = 1, ..., SchemaMismatch = 7 }
pub const VERSION: u32 = 2; // ep_version, a different constant
```

Currently agreeing on magic/version/header/status 1–7. `ep_version = 2` is a third number sitting next to
`COMPACT_VERSION = 1` with no shared name. `COMPACT_KIND_TAG` (`null=0 … i64=6`) is restated only on the TS side in this
slice; rust kind discriminants were not fully read here.

Problem: the same wire constants as F2/F3, waiting to drift.

Fix: export the compact constants from `columine-event-processor` and generate the TS const object. Direction: Rust →
TS. `EventProcessorWasmExports` already matches the six `ep_*` symbols; keep that as a typed import list generated from
`columine-ep-wasm` `no_mangle` names.

Cost/Risk: parse-backend.ts encoding path must keep using the generated numbers; no behavior change if values stay
identical.

### F9 — MEDIUM — SSOT — lmao entry types 1–24 restated, currently equal

Evidence: `packages/lmao/src/lib/schema/systemSchema.ts:218-300` +
`packages/lmao-rs/crates/lmao-core/src/entry_type.rs:10-38`

TS `ENTRY_TYPE_SPAN_START = 1` … `ENTRY_TYPE_BUFFER_CAPACITY = 24` match Rust
`EntryType::{SpanStart=1, … BufferCapacity=24}` and `COUNT: 24`. Rust's own comment (`entry_type.rs:1-6`) says the
discriminants "MUST remain stable for span lifecycle entries consumed by the WASM and TypeScript ABI" and names TS as
the alignment target.

Problem: two tables, one ABI. They agree today; F2/F3 show what happens when they stop.

Fix: TS `systemSchema.ts` is the product SSOT (the TS runtime writes these bytes; Rust/WASM consume them). Generate
`lmao-core/src/entry_type.rs` from the TS constants (or from a JSON table both compile). Direction: TS → Rust. Pin
`ENTRY_TYPE_NAMES` strings in the same table so Arrow dictionary labels cannot drift independently.

Cost/Risk: `lmao-macros` / rust tracer must take the generated enum; no numeric change if the generator is honest.

### F10 — MEDIUM — SSOT — cowshed public DTO/enum surface is hand-copied into types.ts

Evidence: `packages/cowshed/src/types.ts:1-255` vs `packages/cowshed/crates/cowshed-core/src/error.rs:8-16` + `dto.rs`
(`WorkspaceInfo` 463-483, `LandOptions` 2153-2168, `GcReason` 610-618, `JobState` 640-650) + `metadata.rs`
(`ImageFormat` 162-164, `GrantSet` 892-908, `SimVerb` 885-889)

ErrorCode kebab-case, WorkspaceState, JobState (`outputLimit`), GcReason, ImageFormat, SimVerb, GrantSet field names
currently match serde `camelCase`/`kebab-case`. LandOptions omit-`{}` deserializes `retire: true`
(`public_api_contracts.rs:726-731`), so the TS optional `retire?: boolean` is not a live default bug. The copy is still
a second schema: F1 already diverged inside this same file.

Problem: typia asserts a hand-written TS type against JSON produced by a hand-written Rust type. Nothing generates one
from the other.

Fix: Rust `dto.rs` + `error.rs` + `metadata.rs` GrantSet are the SSOT (napi `parse_json`/`canonical_json` already treats
them as such; `AddonFailure` refuses to invent ErrorCode spellings at `cowshed-napi/src/lib.rs:39-41`). Generate
`packages/cowshed/src/types.ts` from those serde types (schemars → typia, or a small ABI crate). Direction: Rust → TS.

Cost/Risk: the whole cowshed npm type surface moves in lockstep with dto.rs. That is the desired blast radius.

### F11 — MEDIUM — SSOT — WASM layout constants restated (state header, slot meta, identity, program magic)

Evidence:

- `packages/columine/src/wasm-backend.ts:33-36` `STATE_HEADER_SIZE = 32`, `SLOT_META_SIZE = 48`,
  `EVICTION_ENTRY_SIZE = 16` vs `columine-types/src/types.rs:8-14,49` and `opcodes.rs:253-256`. Comment still says "Must
  match vm.zig".
- `packages/columine/src/types.ts:221-224` `PROGRAM_MAGIC = 0x314d_4c43`, `HEADER_SIZE = 14`, `PROGRAM_HASH_PREFIX = 32`
  vs `opcodes.rs:184-191` (`PROGRAM_HEADER_SIZE = 46` = 32+14). Parser uses `PROGRAM_HASH_PREFIX + HEADER_SIZE`
  (`reducer-bytecode.ts:111`) so the split currently agrees.
- `packages/lmao/src/lib/wasm/wasmPhysicalLayout.ts:8` `WASM_IDENTITY_BYTE_LENGTH = 128` vs `lmao-arena/src/lib.rs:31`
  `IDENTITY_SIZE = 128`. Tests hard-code bump-ptr `192` (`wasmAllocator.test.ts:82`) vs `HEADER_SIZE = 192`.

Problem: sizes that are closed-form ABI (Byproduct L4) are copied as literals. A 48→64 slot-meta bump would compile on
both sides and mis-parse state.

Fix: generate a `layout.ts` / `layout.rs` pair from one table. Columine: Rust → TS. LMAO arena header/identity: Rust →
TS (same direction as SizeClass).

Cost/Risk: wasm-backend slot-offset arithmetic must import the generated constants; no other callers.

### F12 — MEDIUM — SSOT — ComparisonType vs CmpType vs DurationUnit/TtlStartOf

Evidence: `packages/columine/src/types.ts:90-109` + `packages/columine/crates/columine-vm/src/hashmap_ops.rs:42-51` +
`packages/columine/crates/columine-types/src/opcodes.rs:167-182`

```ts
export enum ComparisonType {
  U32 = 0,
  F64 = 1,
  I64 = 2,
}
export enum TtlStartOf {
  NONE = 0,
  SECOND = 1,
  /* ... */ YEAR = 8,
}
```

```rust
pub enum CmpType { U32 = 0, F64 = 1, I64 = 2 }          // hashmap_ops.rs
pub enum DurationUnit { None = 0, Second = 1, ..., Year = 8 } // opcodes.rs and again types.rs
```

Currently equal. CmpType is a third home (VM crate, not columine-types). DurationUnit is already duplicated inside
columine-types.

Fix: move `CmpType` next to `Opcode` in the single registry (F4). Generate TS `ComparisonType` / `TtlStartOf` from it.
Direction: Rust → TS.

Cost/Risk: `hashmap_ops.rs` imports the shared enum; numeric values stay.

### F13 — LOW — SSOT — NEXT_HINT_MARKER and platform directory literals are acknowledged copies

Evidence: `packages/cowshed/src/index.ts:116-118` + `packages/cowshed/crates/cowshed-napi/src/lib.rs:74-76` +
`packages/cowshed/src/platform.ts:1-10`

```ts
const NEXT_HINT_MARKER = '\nnext: ';
```

```rust
let reason = format!("{message}\nnext: {hint}");
```

Both comments say the spellings must stay byte-identical. `platformDirectory` is "mirrored by the napi `--output-dir`
literals in package.json" (`darwin-${arch}`, `linux-${arch}-gnu`). Not drifted today.

Fix: one `const NEXT_HINT_MARKER: &str = "\nnext: ";` in cowshed-core, format from it, and have TS import a generated
string. Platform triples: generate `platform.ts` from the same list package.json napi targets use.

Cost/Risk: error-hint parsing (`index.ts:125-133`) breaks if the marker changes; that is why it should not be copied.

### F14 — LOW — SSOT — skill.md restates ErrorCode exit mapping

Evidence: `packages/cowshed/skills/cowshed/SKILL.md:87-89` + `packages/cowshed/crates/cowshed-core/src/error.rs:19-40`

Skill text: exits `1` internal … `7` integrity; exec wrapper `100`–`106`. Matches `ErrorCode::exit_code` /
`exec_wrapper_exit_code`. A docs copy of an ABI.

Fix: generate the skill table from `error.rs`, or delete the numbers and point at `cowshed --help` / `--json`. Rust
remains SSOT.

Cost/Risk: agents reading the skill; no runtime.

### F15 — LOW — STRUCTURE — stale Zig comments and executeBatch JSDoc

Evidence: `packages/columine/src/types.ts:23,41,88,229` ("Must match Zig … vm.zig") +
`packages/columine/src/wasm-backend.ts:34` + `packages/columine/src/types.ts:386`

```ts
* @returns 0 = OK, 1 = CAPACITY_EXCEEDED, 2 = INVALID_PROGRAM
```

The VM is Rust. The JSDoc names two of eight ErrorCode members and hides F3.

Fix: delete Zig references; point at `columine-types`. Document `executeBatch` as returning `ErrorCode`.

Cost/Risk: comments only.

## Cross-slice questions

- ColTypes (`columine-types`): F2/F4 need a single Opcode/SlotType/ErrorCode module. This slice did not pick
  `opcodes.rs` vs `types.rs` as the survivor beyond "keep the one dispatch actually runs" (`types.rs` has nested; VM
  `vm.rs` dispatches `0x90`/`0x92`/`0x95`).
- ColVmCore (`columine-vm` dispatch): confirm every byte in `DISPATCHED_OPCODE_BYTES` is reachable from
  `vm_execute_batch` and should therefore appear in the generated TS enum (especially `0x14` SlotArray and nested init
  `0x1a`).
- CsCoreApi (`cowshed-core` dto.rs): F1/F6 are the TS mirror of JobInfo. If that slice changes CommandArg tagging, the
  generated TS type must move with it.
- LmaoCore / LmaoWasm / LmaoArena: F5 SizeClass and F9 EntryType generation direction (SizeClass Rust→TS, EntryType
  TS→Rust). F5's `_ => Col8B` mapper lives in `lmao-wasm`.
- XcutCowshedDup: GrantSet/CLI flag restatements inside cowshed crates were not audited here beyond the TS↔Rust JSON
  seam.

## Non-findings (checked, clean)

- Cowshed napi export list (`coordinatorEndpoint`, `openProject`, `connectCoordinator`, `runCli`, and the `#[napi]`
  methods on Coordinator/WorkspaceHandle/Session/JobHandle/JobAttachment/Project/WorkspaceRef) matches `native.ts`
  `NativeModule` / handle interfaces. No missing/extra public symbol on that seam.
- Cowshed ErrorCode kebab-case spellings agree (`internal`…`integrity`). LandOptions `{}` → `retire: true` agrees with
  CLI `--no-retire` default. GcReason, JobState, ImageFormat, SimVerb, WorkspaceState currently agree.
- Columine EP WASM six-function ABI (`ep_version`, `ep_destroy`, `ep_create_with_schema`,
  `ep_create_with_schema_and_names`, `ep_create_log_entry`, `ep_compact`) is wrapped 1:1 by `parse-backend.ts`
  `EventProcessorWasmExports`.
- Columine reducer WASM exports 62 `vm_*` names (`export_checklist.rs`); TS `VM_EXPORT_NAMES` is an intentional subset
  (no rbmp/map-iter/struct-map-1key). Not duplication; the host does not claim those symbols.
- LMAO WASM export names (`init`, `alloc_exact`, `create_and_start_span`, identity/span/column/thread-id/freelist) match
  `WasmExports` in `wasmAllocator.ts`. `write_col_*` is exported by Rust and not on the public `WasmAllocator` interface
  because TS writes columns through linear-memory views — by design, not a second writer.
- LMAO entry-type discriminants 1–24 agree byte-for-byte (F9 is restatement, not drift).
- Compact magic `0x3142_5043`, version 1, header 16, descriptor 32 currently agree (F8).
- `NEXT_HINT_MARKER` spellings currently agree (F13).
- Cowshed scripts (`verify-packaged-bin-bits.ts`) do not restate DTO/opcode ABI. Skill.md restates exit codes only
  (F14).
- No napi/wasm dependency-bloat or copy/alloc findings in this slice: the defect is duplicated _tables_, not duplicated
  _kernels_. Regime: all of the above are once-per-call boundary checks, not hot loops.
