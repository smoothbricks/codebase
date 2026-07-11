# lmao-rs

Rust port of the LMAO trace-logging system. Lives beside the TS implementation in `packages/lmao`; the specs in
`specs/lmao/` are the source of truth.

`lmao-wasm` is the production allocator: `packages/lmao/dist/allocator.wasm` is built from this workspace
(`nx run lmao-rs:cargo-wasm` → `nx run lmao:rust-wasm`) and shipped in the npm package. The original Zig allocator
remains in-tree as an opt-in reference build (`bun run build:zig-wasm` in `packages/lmao`, loaded via
`LMAO_WASM_ALLOCATOR=zig`); note it carries a latent freelist bug (FreeBlock bookkeeping overruns sub-20-byte `col_1b`
blocks at capacity 8/16) that the Rust port fixes by clamping to the first ≥20-byte tier.

Nx targets for this project: `build` (via `cargo-wasm`), `test` (`cargo test --workspace`), `lint`
(`cargo fmt --check` + `clippy -D warnings`), `bench`.

## Crate map

| Crate         | Responsibility                                                                                                                                                                                                                                                                             | Primary specs                                      |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------- |
| `lmao-core`   | Entry types (23), span identity (thread_id + counter, `Arc<str>` trace ids, parent by reference), fixed row layout (row 0 span-start / tag overwrite, row 1 pre-armed span-exception, rows 2+ append), per-trace timestamp anchoring behind a `Clock` trait, SoA buffers, capacity ratchet | `01h`, `01b4`, `01b`, `01b1`–`01b5`, `01b3`, `01a` |
| `lmao-arena`  | Buddy/tiered-freelist arena ported from `packages/lmao/src/lib/wasm/allocator.zig` (28 freelists = 7 tiers × 4 size classes, zero-overhead freelists, 192-byte header — layouts pinned with `#[repr(C)]` + const asserts)                                                                  | `01q`                                              |
| `lmao-arrow`  | Two-pass tree walk → one Arrow `RecordBatch` per flush (pass 1 dictionary accumulation, sorted-dict finalize, pass 2 memcpy columns)                                                                                                                                                       | `01k`, `01f`                                       |
| `lmao-macros` | `define_log_schema!` / `span!` proc-macros — compile-time replacement for the TS `new Function()` codegen and AST transformer                                                                                                                                                              | `01a`, `01g`, `01j`, `01o`, `01b6`                 |
| `lmao-wasm`   | `cdylib` for `wasm32-unknown-unknown` exporting the exact `allocator.zig` ABI (same names, sentinel returns, packed-u64 identity convention, `--import-memory`) so `wasmAllocator.ts` loads it unchanged                                                                                   | `01q`                                              |
| `lmao-query`  | Tracer-agnostic assertion/query surface (selector → count/never), Arrow + SQLite backends behind features                                                                                                                                                                                  | `AxE/specs/sim/08-trace-testing.md`                |

## AxE determinism constraints (non-negotiable design rules)

From `AxE/specs/sim/01-deterministic-scheduler.md` and `08-trace-testing.md`:

1. **No ambient time or entropy.** Wall/monotonic time only via `lmao_core::Clock`; randomness only via
   `lmao_core::Entropy`. A sim run injects seeded impls and must get **bit-identical trace bytes** for the same
   `(build, seed, config)`.
2. **Zero heap allocations per event after warmup** (`tests/alloc_gate.rs` enforces with a counting global allocator).
   Growth = overflow chaining, never realloc.
3. **Overhead gates:** tracing on vs off ≤20% median throughput delta, ≤25% peak RSS, at a 10^6-event run
   (`benches/overhead.rs` harness; gate comparison TBD).
4. **Deterministic encoding:** sorted dictionaries, no HashMap-iteration-order or float-formatting leakage into emitted
   bytes.
5. **Tracer-agnostic queries:** the same selector must run against SQLite (`.trace-results.db` parity) and in-process
   Arrow batches.

## TDD workflow

Red → green → mutate:

1. **Red:** the property tests in `crates/*/tests/properties.rs` marked `#[ignore = "TDD red: ..."]` are the acceptance
   criteria for the next implementation step (arena alloc/free, `SpanBuffer::append`, `convert_span_trees`). Un-ignore
   the one you're implementing; watch it fail.
2. **Green:** implement until `just proptest-heavy` passes (10k cases).
3. **Mutate:** `just mutants` — classify survivors per the AxE kernel rule (equivalent / invalid / budget-timeout /
   unreached / oracle-gap). Remove the corresponding `exclude_re` entry in `mutants.toml` as each stub becomes real.

## Commands

```sh
cd packages/lmao-rs
just test            # fast tier: unit + property tests (green suite)
just proptest-heavy  # heavy tier: 10k proptest cases, includes TDD-red ignored tests
just mutants         # mutation tier (cargo install cargo-mutants)
just wasm            # wasm32 ABI build (rustup target add wasm32-unknown-unknown)
just bench           # criterion harness for the AxE overhead gates
just check           # cargo check + clippy -D warnings
```

Without `just`: the same commands are spelled out in `justfile`, and `.cargo/config.toml` provides `cargo t`,
`cargo proptest-heavy`, `cargo wasm`.

## Port order (suggested)

1. `lmao-arena` alloc/free (port `allocator.zig` mechanically; the property tests encode buddy conservation, reuse, and
   no-overlap).
2. `lmao-core` `SpanBuffer::append` + overflow chaining (alloc-gate test goes green).
3. `lmao-arrow` `convert_span_trees` (bit-identical serialization test goes green).
4. `lmao-macros` `define_log_schema!` (then re-include macros in `mutants.toml`).
5. `lmao-wasm` real bodies over linear memory; validate against the TS host's 52 allocator tests in
   `packages/lmao/src/lib/wasm/__tests__/`.
6. `lmao-query` Arrow scan backend; SQLite parity backend.

## Backlog (downstream consumers depend on these)

- **`lmao-inspect`** — a thin generic CLI over `lmao-query`: tail/follow Arrow segment dirs, run selectors and SQL,
  render tables, export `--json`/`--ndjson`. Reusable by cowshed, AxE, and jcode; spec at
  `specs/lmao/04_inspect_cli.md`.
- **W3C `traceparent` interop** — the runtime adopts an inbound `TRACEPARENT` (env / carrier) at tracer init as the root
  span context, and stamps outbound `fetch`/HTTP requests with the current span's `traceparent`. This is what lets a
  first-party tool's traffic join the caller's trace end-to-end (cowshed's tier-1 gateway attribution depends on it —
  `specs/cowshed/13_telemetry.md`). Applies to both the TS/`packages/lmao` and Rust tracers; keep the wire format
  identical across them.
