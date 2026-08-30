# lmao-wasm

Scope: `packages/lmao-rs/crates/lmao-wasm/src/lib.rs` (523), `packages/lmao-rs/crates/lmao-wasm/Cargo.toml` (13),
`packages/lmao-rs/Cargo.toml` (50), `packages/lmao-rs/Cargo.lock` (3051; name/version census + `lmao-*` entries),
`packages/lmao-rs/package.json` (42), `packages/lmao-rs/justfile` (49), `packages/lmao-rs/mutants.toml` (30). Targeted
confirmation (not owned): `packages/lmao/src/lib/wasm/wasmAllocator.ts`, `packages/lmao-rs/.cargo/config.toml`,
`packages/lmao-rs/crates/lmao-arena/src/lib.rs` (`SizeClass`), `packages/lmao-rs/crates/lmao-arena/src/raw.rs` (ABI
constants), `AGENTS.md` (Nx `{tool}-{output}` rule).

## Summary

- Production `just wasm` / Nx `cargo-wasm` builds `--release`, not `[profile.wasm-release]` — the profile whose comment
  exists for this allocator.
- TS `SizeClass.Identity = 4` has no Rust variant; `size_class` maps every `sc >= 3` to `Col8B`.
- Four Rust debug/clock exports are absent from TS `WasmExports`; three `write_col_*` names are restated there and never
  wrapped.
- `black_box` launders every linear-memory load/store, not just offset 0 (hot path).
- Native ABI smoke never hits `WasmMem`, `memory.grow`, or host clocks (clocks are hardcoded 0).
- Crate runtime deps are clean (`lmao-arena` only). Linear-memory boundary is offset in-place, not a copy.
- Nx `cargo-wasm` follows `{tool}-{output}`; justfile recipes have no colon names.

## Findings

### F1 — HIGH — SSOT — `wasm-release` is defined for this allocator and not used by it

Evidence: `packages/lmao-rs/Cargo.toml:36-40`

```
# wasm32 export size matters for the allocator module consumed by the TypeScript host.
[profile.wasm-release]
inherits = "release"
opt-level = "z"
panic = "abort"
```

`packages/lmao-rs/justfile:18-21`

```
wasm:
    cargo build -p lmao-wasm --target wasm32-unknown-unknown --release
    mkdir -p dist
    cp target/wasm32-unknown-unknown/release/lmao_wasm.wasm dist/lmao_wasm.wasm
```

`packages/lmao-rs/package.json:18-36` (`cargo-wasm` → `just wasm`). Contrast `justfile:36-43`: `lmao-timestamp-proof`
_does_ pass `--profile wasm-release` and copies from `target/wasm32-unknown-unknown/wasm-release/`. Neighbour
`packages/lmao-rs/.cargo/config.toml:10` repeats the same `--release` alias.

Problem: The workspace comment names this crate as the reason `wasm-release` exists (`opt-z` + `panic = abort`). The
artifact the TypeScript host actually loads is `[profile.release]` (`lto = true`, `codegen-units = 1`, default
`opt-level = 3`, default unwind panics), copied from `target/.../release/`. PH-4.1: two profiles, one claimed purpose.
Size claims about the allocator module describe a binary that is not built; any later opt-z measurement of this crate
would not match production.

Fix: Point `just wasm` (and the cargo alias) at `--profile wasm-release` and copy from
`target/wasm32-unknown-unknown/wasm-release/lmao_wasm.wasm` **or** delete the “allocator module” comment and keep
`--release` if the host wants the speed profile. Do not leave both. Decision: keep `--release` for the allocator (column
IO is a hot ABI) and retarget the profile comment at `lmao-timestamp-proof`, which is the only consumer today.

Cost/Risk: Changing the shipped profile changes both size and ns/op of every WASM span write. Same-profile A/B required
(PH-4.1). Nx `cargo-wasm` inputs already include `justfile` + `Cargo.toml`.

### F2 — MEDIUM — SSOT — `SizeClass` restated in TS with an extra discriminant this crate silently aliases to `Col8B`

Evidence: `packages/lmao-rs/crates/lmao-wasm/src/lib.rs:130-136`

```
fn size_class(sc: u8) -> SizeClass {
    match sc {
        0 => SizeClass::SpanSystem,
        1 => SizeClass::Col1B,
        2 => SizeClass::Col4B,
        _ => SizeClass::Col8B,
    }
}
```

`packages/lmao-rs/crates/lmao-arena/src/lib.rs:37-43` (SSOT): `SpanSystem=0, Col1B=1, Col4B=2, Col8B=3` — no `Identity`.
`packages/lmao/src/lib/wasm/wasmAllocator.ts:133-138`

```
export enum SizeClass {
  SpanSystem = 0,
  Col1B = 1,
  Col4B = 2,
  Col8B = 3,
  Identity = 4,
}
```

Those `u8` values are the ABI for `get_freelist_len` / `debug_get_freelist_head` / reuse/split/merge (`lib.rs:173-199`).
Grep found no `SizeClass.Identity` _call_ in `packages/lmao`.

Problem: Three tables for one discriminant. TS invents `Identity = 4`. This crate’s catch-all maps `3`, `4`, and every
other `u8` to `Col8B`. Passing `SizeClass.Identity` would inspect the Col8B freelist and look successful. Illegal states
are representable at the ABI.

Fix: SSOT is `lmao_arena::SizeClass`. Delete `Identity` from the TS enum (identity is a separate freelist, not a column
size class). Replace the catch-all with an explicit `3 => Col8B` and a sentinel (return 0 / ignore) for anything else —
same convention as offset-0 OOM. Do not `TryFrom` across the C ABI; keep the `u8` and fail closed.

Cost/Risk: TS `SizeClass` is part of the public wasm wrapper. Removing `Identity` is a greenfield delete; no current
caller. Freelist debug exports are the only consumers of `size_class`.

### F3 — MEDIUM — DUPLICATION — crate comment claims the export list matches the TS host; it does not

Evidence: `packages/lmao-rs/crates/lmao-wasm/src/lib.rs:13-14`

```
//! - Export names match the allocator export list consumed by the TypeScript
//!   host, including debug exports.
```

Rust `#[unsafe(no_mangle)]` exports in this file, vs `packages/lmao/src/lib/wasm/wasmAllocator.ts:154-218`
(`WasmExports`) and `:326-397` (`wrapWasmInstance`):

| Rust export                               | `WasmExports`                    | wrapped on `WasmAllocator` |
| ----------------------------------------- | -------------------------------- | -------------------------- |
| `debug_get_freelist_head` (`lib.rs:173`)  | no                               | no                         |
| `debug_read_next_ptr` (`lib.rs:178`)      | no                               | no                         |
| `get_performance_now` (`lib.rs:417`)      | no                               | no                         |
| `debug_compute_timestamp` (`lib.rs:422`)  | no                               | no                         |
| `write_col_f64/u32/u8` (`lib.rs:398-409`) | yes (`wasmAllocator.ts:182-184`) | no                         |

`isWasmExports` (`wasmAllocator.ts:220-231`) only probes `init`, `alloc_exact`, `create_and_start_span`,
`create_overflow_span`, `free_span_superblock`.

Problem: The host ABI is hand-restated twice (Rust `no_mangle` names, TS `WasmExports`) plus a third camelCase
`WasmAllocator` façade. Four debug/clock exports have zero TS callers (grep of `packages/lmao` is empty). `write_col_*`
are listed as required exports then dropped on the floor — production writes go through TypedArray views (the zero-copy
path). The type guard cannot go red if most of the ABI vanishes (PH-7.10bb).

Fix: Rust `no_mangle` names are the SSOT. Generate `WasmExports` from them, or delete the unused exports
(`debug_get_freelist_head`, `debug_read_next_ptr`, `get_performance_now`, `debug_compute_timestamp`) and drop
`write_col_*` from `WasmExports` if the host is not going to call them (keep the symbols if `wasm-boundary.bench.ts`
stays a direct-export bench). Expand `isWasmExports` to every name `wrapWasmInstance` actually calls.

Cost/Risk: `wasm-boundary.bench.ts` calls `write_col_f64` by export name — do not delete that symbol without moving the
bench. `get_performance_now` / `debug_compute_timestamp` are also exported by `lmao-timestamp-proof` (cross-slice).

### F4 — MEDIUM — COPIES — `black_box` on every load/store, not only header offset 0

Evidence: `packages/lmao-rs/crates/lmao-wasm/src/lib.rs:63-110`

```
/// Absolute offset → pointer, laundered through `black_box` so LLVM cannot prove
/// offset 0 is a null pointer: the header legitimately lives at address 0 of WASM
/// linear memory. Without this, LLVM folds header writes into `unreachable` and
/// `reset()` traps.
#[cfg(target_arch = "wasm32")]
#[inline(always)]
fn laundered(off: u32) -> *mut u8 {
    core::hint::black_box(off as usize) as *mut u8
}
```

Every `read_*`/`write_*` in `impl Mem for WasmMem` goes through `laundered(off)`.

Problem: Regime is **hot** — column IO and span lifecycle are per-row ABI calls, each a handful of unaligned
loads/stores. The invariant that needs laundering is “offset 0 is a valid header address”, not “every offset is opaque”.
`black_box` on the common path blocks LLVM from folding address arithmetic across the `Mem` helpers (PH-4.1: this crate
is also the one built `opt-z` _if_ F1 is flipped). Not a memcpy; it is a compiler barrier paid on every access.

Fix: Launder only `off == 0` (or only the header-sized prefix). Keep the comment. Do not spread `black_box` to
`read_u64` of column values.

Cost/Risk: Wrong narrowing reintroduces the `reset()` `unreachable` trap. Prove with a wasm32 test that writes the
header at 0, then a census of `WasmMem::write_u64` with and without the barrier.

### F5 — MEDIUM — TESTS — native smoke cannot go red on the wasm backend, clocks, or grow

Evidence: `packages/lmao-rs/crates/lmao-wasm/src/lib.rs:39-46` (native clocks)

```
unsafe fn host_performance_now() -> f64 { 0.0 }
unsafe fn host_date_now() -> f64 { 0.0 }
```

`lib.rs:121-127` (native arena): `VecMem::with_zeroed(1 << 20)` behind `thread_local!`. `lib.rs:447-522`: one
`#[cfg(all(test, not(target_arch = "wasm32")))]` test, `export_surface_span_lifecycle`. It drives
`init/reset/identity/alloc_exact/span_start/write_log_entry/span_end_ok` and never `create_and_start_span`,
`create_overflow_span`, `write_col_*`, `memory.grow`, or a non-zero clock. Timestamps are unasserted.

Problem: The production backend is `WasmMem` + imported memory + `env.performanceNow`/`dateNow`. The only test compiles
the native fallback, so `laundered`, `memory_size`/`memory_grow`, and host-clock wiring have no oracle. Clocks fixed at
0 means `init_trace_root` / `span_start` / `debug_compute_timestamp` cannot fail a timestamp assertion even if the
import is dropped (PH-7.10bb). Fixture is a 1 MiB `VecMem` (16 pages); TS host clamps to 17 pages (`MIN_INITIAL_PAGES`)
— the “matches WASM module minimum” comment on the TS side is already stale relative to this fallback.

Fix: Keep the native smoke. Add a wasm32 instantiation test (or a host-side ABI test) that (1) writes header at offset
0, (2) grows, (3) asserts `debug_compute_timestamp` moves when `performanceNow` moves. Assert timestamps as `i64`
values, not absence.

Cost/Risk: Needs `wasm32-unknown-unknown` in CI and a JS or wasmtime harness. Do not pretend the native test covers
`WasmMem`.

### F6 — LOW — SSOT — npm package version and Cargo workspace version disagree

Evidence: `packages/lmao-rs/package.json:3` `"version": "0.1.0"` vs `packages/lmao-rs/Cargo.toml:14` `version = "0.0.1"`
(and `Cargo.lock` `lmao-wasm` `version = "0.0.1"` at line 1775).

Problem: Two version numbers for one package. Not a runtime bug (`publish = false`), but the restated constant will
drift.

Fix: One version. Cargo workspace is the Rust SSOT; set `package.json` to `0.0.1` or stop claiming a version there.

Cost/Risk: None for the wasm ABI.

### F7 — LOW — STRUCTURE — crate-wide `missing_safety_doc` allow; `unsafe` blocks state no invariant

Evidence: `packages/lmao-rs/crates/lmao-wasm/src/lib.rs:22` `#![allow(clippy::missing_safety_doc)]` `lib.rs:88-109`:
`unsafe { laundered(off).cast::<u32>().read_unaligned() }` (and u8/u64/write twins) with no `// SAFETY:` citing “offset
is inside the imported linear memory; unaligned column payloads are the ABI”. Host imports (`lib.rs:32-37`) and native
stubs (`lib.rs:40-46`) are `unsafe fn` without docs.

Problem: The `laundered` comment explains the null-pointer fold, not memory validity or unaligned access. Clippy is
silenced for the whole crate instead of documenting the two `unsafe` sites.

Fix: Delete the allow. Put a `SAFETY` line on each `Mem` method: in-bounds by arena contract, unaligned by column
layout, offset 0 is the header.

Cost/Risk: None.

### F8 — LOW — STRUCTURE — WASM page size `65536` written twice as a bare literal

Evidence: `packages/lmao-rs/crates/lmao-wasm/src/lib.rs:77` and `:84`

```
(core::arch::wasm32::memory_size(0) as u32).saturating_mul(65536)
...
let pages_needed = ((new_size - current) as usize).div_ceil(65536);
```

Problem: Same closed-form constant, two sites. Not hot-path waste (grow is the slow path); it is a restated 64 KiB page
size.

Fix: `const WASM_PAGE_SIZE: u32 = 65536;` next to `WasmMem`.

Cost/Risk: None.

## Cross-slice questions

- `packages/lmao/src/lib/wasm/wasmAllocator.ts:328` binds `allocExact: exports.alloc_exact` with no `refreshViews()`.
  `createAndStartSpan` / `createOverflowSpan` do refresh. If `alloc_exact` → `memory.grow`, TypedArray views go stale.
  Owned by the TS wasm wrapper, not this crate.
- `packages/lmao-rs/crates/lmao-timestamp-proof/src/lib.rs:99-106` re-exports `get_performance_now` /
  `debug_compute_timestamp` under the same names. If those symbols are deleted from this crate (F3), confirm the proof
  crate remains the timestamp-harness ABI. Slice: timestamp-proof.
- `ENTRY_TYPE_SPAN_{START,OK,ERR,EXCEPTION}` live in `lmao-arena` `raw.rs:86-89` and again in
  `packages/lmao/src/lib/schema/systemSchema.ts:219-228` and again in timestamp-proof `layout.rs`. Values currently
  agree (1..=4). This crate only forwards them.
- `Cargo.lock` duplicate versions (`getrandom` 0.2.17 / 0.3.4 / 0.4.3; `hashbrown` 0.14.5 / 0.15.5 / 0.17.1; `itertools`
  0.10.5 / 0.14.0; `rand` 0.8.7 / 0.9.5; `r-efi` 5.3.0 / 6.0.0) and the `datafusion` / `napi` / `rusqlite` subtrees are
  **not** reachable from `lmao-wasm` (`Cargo.lock:1773-1778` depends only on `lmao-arena`). Workspace dep-bloat slice
  owns the rest.

## Non-findings (checked, clean)

- **Zero-copy at the linear-memory boundary.** `WasmMem` is absolute-offset load/store into imported memory
  (`lib.rs:56-111`). No `to_vec`/`clone`/`to_owned` on the wasm path. Host `Uint8Array`/`BigInt64Array` views alias
  `memory.buffer`. Packed `u64` from `alloc_identity_root_for_js_write` (`lib.rs:11-12,236-237`) exists so JS writes
  trace-id bytes in place. `write_col_*` are optional ABI; production TS does not wrap them.
- **DEP-BLOAT for this crate.** `lmao-wasm` depends only on `lmao-arena` (`Cargo.toml:12-13`; lock `1773-1778`).
  `lmao-arena` has no runtime deps. No wasm-bindgen, no crypto/TLS, no parser crates. `crate-type = ["cdylib", "rlib"]`
  — `rlib` is what the native test module links. Load-bearing: in-process `Mem` + `memory.grow` cannot be a shell-out.
- **Nx target naming.** `package.json` `nx.targets.cargo-wasm` is `{tool}-{output}`. Aggregate `lint` is allowed.
  justfile recipes (`test`, `wasm`, `bench`, `check`, `mutants`, `timestamp-proof-artifacts`, `proptest-heavy`) contain
  no colon names. justfile is not an Nx target graph; `cargo-wasm` → `just wasm` is the policy-compliant wiring.
- **No operational `unwrap`/`expect`/`panic` in the export surface.** OOM is offset 0 (`lib.rs:10`). `size_class`
  catch-all is F2, not a panic.
- **`mutants.toml`** excludes `lmao-macros` and named `SpanBuffer::append` / `MockSpan` regexes only; this crate is in
  the mutation set.
- **Identity / layout constants that do agree:** `WASM_SPAN_IDENTITY_ROOT/CHILD = 0/1` match `raw::SPAN_IDENTITY_*`;
  `WASM_NO_LAYOUT_OFFSET = 0xffffffff` matches `raw::NO_LAYOUT_OFFSET = u32::MAX`.
- **God-file / 100-line functions:** 523-line ABI shim; exports are thin `with_mem` wrappers. No finding.
- Native `with_mem` 1 MiB `thread_local` arena is **once-per-thread test startup**, not a production copy finding.
