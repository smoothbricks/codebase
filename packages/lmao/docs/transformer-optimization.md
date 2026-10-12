# Transformer × lmao: optimization analysis

Companion to [optimization-investigation.md](./optimization-investigation.md). This document keeps the historical wasm
boundary and tag-write measurements, then records the transformer/runtime optimizations that have since shipped. Do not
extrapolate the measured tag-write numbers to the newer span setup or Op-local log-template-ID paths: no speedup is
claimed for either path without a dedicated benchmark.

Measured on Apple M5 Max, bun/JSC. The tag-write numbers below come from
`packages/lmao-ttsc/benchmarks/inline-vs-codegen.bench.ts`; boundary numbers are cited from
`packages/lmao/benchmarks/wasm-boundary.bench.ts`. They are historical evidence for those exact shapes only. No result
in this document measures the shipped Op-local template-ID pipeline.

## Bench: 6 tag writes (1 enum, 2 category, 2 number, 1 bool)

| Shape                                                             | ns/iter  | alloc/iter |
| ----------------------------------------------------------------- | -------- | ---------- |
| A. fluent chain (no transformer, runtime fallback)                | 22.1     | ~4 B       |
| B. runtime-codegen writer (`new Function()`, lmao today)          | 24.2     | 0          |
| C. **transformer-inlined direct writes** (tagChainInliner output) | **19.5** | 0          |
| D. compile-time micro-batching (staging buffer + `.set()`)        | 77.2     | 0          |

Reference boundary costs: wasm export call ~39–42 ns/value; JS TypedArray view write into wasm memory ~7.6 ns; plain JS
write 0.5–2 ns.

## Verdicts

### 1. Inlining replaces runtime codegen at zero cost — ship it (CSP win)

C ≈ B within noise (actually slightly ahead) and both are allocation-free. The transformer's statically-emitted writes
fully replace the `new Function()` writer **with no performance loss** — which matters on CSP-restricted hosts
(Cloudflare Workers, spec 01q/01s) where runtime codegen is banned and lmao today must fall back to slower paths. The
transformer is the zero-eval codegen. (Caveat: bun/JSC numbers; the spec's V8 hidden-class claims were not re-validated
here.)

### 2. Compile-time write batching is a myth at tag-write scale

D is **4x worse** than C: `subarray()` + `.set()` overhead swamps any benefit when the batch is a handful of values.
Batching only pays when it _eliminates wasm export calls_ (~40 ns each) — but lmao's design never makes export calls for
writes in the first place (see 3). Do not build write-batching into the transformer.

### 3. The right wasm strategy is direct-view emission, not export amortization

With the Rust allocator (`lmao-wasm`), per-value exports (`write_col_f64`, 42 ns) are for correctness testing, not the
hot path. The production pattern stays exactly lmao's original design: **JS-side TypedArray view writes into wasm linear
memory** (~7.6 ns), with export calls only for lifecycle (`alloc_*`, `span_start`, `span_end_*` — 1–2 per span, already
amortized over the span's writes).

The transformer's lever: the inliner currently emits `buf.field_values[0] = v` against the generated buffer class's
views. For the wasm-backed `WasmBufferStrategy`, those views point into wasm memory already — so the inliner output
composes with lmao **unchanged**. An optional future mode could fold the schema's column offsets into constants
(`u8view[BASE + 12] = v`) and skip the view-object indirection, but the measured gap (view write 7.6 ns vs plain 0.5–2
ns) bounds the win at a few ns per write; only worth it bundled with the checker-aware ttsc port.

### 4. Column-name discipline under library composition (spec 01e)

Any inliner emission — TS or Go — must write **library-local (unprefixed)** column names. `.prefix()`/`.mapColumns()`
remapping is cold-path-only via the span's `RemapDescriptor` at Arrow conversion. The transformer must never resolve
prefixes into hot-path writes; offsets/constants it folds are per-library-schema, not per-application-schema.

### 5. Native lmao hosts don't need the transformer

In-process Rust (jcode, deterministic simulation) gets the same effect from `define_log_schema!` proc-macro expansion at
Rust compile time (1.9 ns append, 2.5 ns tag write, per the main investigation). The native ttsc transformer is the
_TypeScript-host_ analog of that macro — same role and schema-static philosophy, in one compiler implementation.

## Current shipped span optimization

The ttsc Go plugin implements bounded, Promise-preserving span setup plus packed runtime hints. Automatic public
`span()` → `spanAutoN()` lowering is intentionally disabled.

### Promise-preserving fixed-arity lowering and ABI boundary

A checker-proved `ctx.span(name, op, ...args)` with zero through eight trailing arguments may become the Promise-based
`ctx.spanN(...)` ABI when both receiver and Op are stable and every expanded value can be evaluated once. The emitted
child is `Object.create(ctx)`, and the Op's buffer class, remap descriptor, metadata, function, and runtime hint are
passed directly. Inline arrow/function spans use the same fixed-arity Promise ABI with the receiver's buffer data.

Dynamic or unstable expressions such as `getCtx().span(name, getOp(), ...args)` remain byte-shape calls to public
`span()`: receiver and Op are evaluated once in source order, and Promise scheduling is unchanged. The transformer also
bails for unproved types, missing checker data, overrides, more than eight trailing arguments, or an unsupported
function shape. Destructured bare-span rewriting uses the same all-or-nothing rule; any unsupported call leaves the
function's original destructuring intact.

The public variadic `span()` and transformed `spanN()` paths always return `Promise<Result>`. `spanAutoN` remains an
**internal/explicit** runtime seam returning `Result | Promise<Result>`; it is never selected automatically. Direct
`await`, direct async `return`, and concise async-arrow positions are not sufficient proof: substituting a synchronous
`Result` changes observable Promise-assimilation microtask turns even when the final awaited value is identical.

### Direct inherited child and specialized `_spanPre`

The Promise-preserving transformer path passes `Object.create(receiver)` directly to `_spanPre`; there is no object
copy, `Object.assign`, or `_newCtx0()` call in emitted code. For explicit internal `spanAutoN`, `_spanAutoPre` likewise
performs `Object.create(this)` and passes that exact child to `_spanPre`. `_spanPre` always rebinds its buffer, schema,
and log binding. Valid capability hints let it install only the own properties the Op can use: `tag`, `log`, `ff`,
nested `span`/`spanSync`, `ok`/`err`, scope/setScope, and deps. Logger construction is shared by log, feature flags, and
scope. Feature-flag rebinding calls `forContext(ctx)` with the **exact inherited child**, preserving context identity
and prototype-inherited user fields rather than evaluating against a copy.

Invalid, absent, or unanalyzable hints take the full setup path. Specialization is therefore an optimization only; it is
not required for correctness.

### Packed runtime hints and capacity

The transformer appends one packed unsigned 32-bit hint to eligible `defineOp` calls and emits per-key hints for a
single-object-literal `defineOps` call. Eligibility is checker-proved from the result type: `Op<...>`/symbol `Op` for
`defineOp`, and `OpGroup<...>`/symbol `OpGroup` for `defineOps`. Same-named unrelated functions and calls without
checker-backed LMAO declaration provenance are not annotated.

| Bits     | Payload                                               |
| -------- | ----------------------------------------------------- |
| `0..15`  | initial row capacity; zero means adaptive/unspecified |
| `16..22` | required context-capability mask                      |
| `23`     | analyzed/valid marker                                 |
| `24..31` | reserved; must be zero                                |

Static capacity begins at two reserved rows and increments once for each direct, statically encountered known log call.
Any loop makes capacity unknown; exceeding `0xffff` does the same. In both cases the capacity field is zero while safely
proven capability bits remain usable. Runtime capacity decoding clamps a nonzero analyzed value to at least two.
Overflow remains correct through the normal overflow chain even if a manually supplied hint underestimates rows.

Capability analysis is intentionally closed-world. Only recognized direct forms are accepted. The whole inline
callback's hint becomes zero when its context parameter is not a simple identifier, the context escapes, a computed or
unknown member is used, a nested function is crossed, or usage cannot be proven. For `defineOps`, only inline arrows,
function expressions, and method declarations receive map entries. Existing Op identifiers/expressions and shorthand
properties are omitted rather than encoded as zero; a present inline callback that fails analysis retains its key with
value zero.

Hint injection composes with destructured-context rewriting: analysis runs on the original callback, then the
hint-bearing call is offered to the destructuring pass. A destructured first parameter therefore receives conservative
hint zero, but the call can still be rewritten to `__ctx`; conversely, a later destructuring bailout preserves any
already-injected hint. The runtime independently validates integer/range, validity, and reserved bits before
specializing; failure selects full capabilities and adaptive capacity.

### Explicit synchronous terminal seam and async fallback

When called explicitly by internal code, `spanAuto0`–`spanAuto8` invoke the Op once on the initial path. Only a genuine
non-retryable `Ok`/`Err` terminal result is written and returned synchronously. Every other value—including a custom
thenable—is handed to the async helper and assimilated once; a synchronous retryable error also takes that path. The
helper awaits the first value and reuses a synchronous first error's ownership/retry classification when the resolved
object is still that same result. Promise-resolved and subsequent results are checked normally. It applies the existing
retry policy, re-invokes only for actual retry attempts, records retry/terminal rows, and ends the span exactly once.
Throws and rejected thenables use the existing exception path. Because automatic lowering is disabled, this internal
`Result | Promise<Result>` behavior cannot alter the public Promise API's microtask scheduling. No performance magnitude
is asserted here.

## Vocabulary registry and Op-local messages

`packages/lmao/src/lib/vocabularyRegistry.ts` accepts schema version `1` with `idAlgorithm: 'sha256-24-v1'`. It
validates each fragment's arrays, record bytes and content hash before returning a runtime-owned vocabulary binding. The
registry rejects incompatible algorithms and distinct records that share an ID.

The compiler's checker-proved Op-local literal log templates still use private `u16` message IDs and decode their
strings at cold conversion. Span names remain strings.

The global 32-bit registration ABI, expanded checker coverage and removal of the Op-local lane are defined in
[01o §6.1–6.2](../../../specs/lmao/01o_typescript_transformer.md#vocabulary-fragment-and-library-without-source-contract).
The [implementation gap list](../../../agent-todo/lmao-wasm-transformer-errors.md) records that cutover; this package
doc does not claim it is shipped.

## The bigger design space: typed macros over the schema, not just call sites

The verdicts above treat the transformer as a call-site rewriter. Its real power is that it sees **the schema
definitions themselves** (`defineLogSchema`/`defineOpContext` object literals: field kinds, enum values, eagerness)
_and_ **every call site in the program**. That enables a class of whole-program, compile-time decisions that runtime
codegen structurally cannot make — lmao's runtime `new Function()` sees one schema value at startup; it never sees
usage. Ordered by expected value:

### A. Compile-time buffer-class emission (kill runtime codegen entirely)

Today only tag _writes_ are inlined; the SpanBuffer class itself is still generated at runtime via `new Function()`
(per-schema, at startup). The transformer can emit the entire generated class as source at build time — the true
`define_log_schema!` analog. Wins: zero runtime codegen anywhere (full CSP compliance, not just hot-path), no per-schema
compile at startup (cold-start win for Workers/Lambda), and the emitted class is visible to the bundler/minifier. This
subsumes verdict 1.

### B. Static eager/lazy sets — pre-allocation decided at compile time

The lazy-to-eager promotion ratchet (01b2) is unshipped runtime machinery that needs ≥100 samples to converge; meanwhile
every lazy column pays a first-touch alloc (measured 133 ns) plus a branch per access. The transformer counts actual
column writes across **all** call sites and rewrites the schema literal with the exact eager set — used columns
pre-allocated at construction, never-written columns flagged (dead-column lint) or elided. Deletes the runtime stats
machinery and converges at build time, per op rather than per schema.

### C. Static capacity seeding — implemented for bounded direct logs

The shipped transformer computes a per-Op packed initial-capacity hint: two reserved rows plus direct known log calls.
Loops, counts above `0xffff`, or unsafe context analysis encode adaptive capacity instead. This is deliberately narrower
than whole-program or profile-guided row prediction, and normal overflow chaining remains the correctness fallback.
Per-op specialized buffer-class generation remains future work.

### D. Allocation batching across the WASM boundary — runtime superblocks shipped

Verdict 2 refuted _write_ batching — writes were never boundary crossings. Allocation batching does pay. The shipped
WASM path now caches aligned root/child and overflow superblock descriptors, then uses one `create_and_start_span(...)`
export for identity + system + every present numeric-family slab + lifecycle initialization, or one
`create_overflow_span(...)` export for identity-free overflow storage. Release is likewise one export. The packed byte
length is asserted never to exceed the previous separate-allocation footprint. Numeric first touch now binds a view into
already-owned family storage; it does not allocate another WASM block.

This runtime change does not need transformer proof and preserves the full-schema layout. A remaining transformer
extension could emit each Op's proven numeric field subset so the superblock omits families or columns that operation
cannot write. That is a memory/layout specialization, not another export-call batching requirement, and remains
unimplemented pending semantic coverage for dynamic access, `.with(...)`, composed libraries, and untransformed callers.

**Measured result (Apple M5 Max, Bun 1.3.14, three Mitata runs).** The retained
`packages/lmao/benchmarks/wasm-boundary.bench.ts` compares balanced allocate/start/free cycles using identical logical
identity, system, u8, u32, and f64 storage:

| Cycle             | Separate allocations | Superblock |                  Result |
| ----------------- | -------------------: | ---------: | ----------------------: |
| child span, run 1 |            460.50 ns |  464.43 ns |  superblock 0.9% slower |
| child span, run 2 |            477.37 ns |  476.32 ns |  superblock 0.2% faster |
| child span, run 3 |            453.03 ns |  463.58 ns |  superblock 2.3% slower |
| overflow, run 1   |            391.17 ns |  345.90 ns | superblock 11.6% faster |
| overflow, run 2   |            408.72 ns |  359.90 ns | superblock 11.9% faster |
| overflow, run 3   |            402.54 ns |  352.01 ns | superblock 12.6% faster |

The same machine measured a near-empty WASM call at 0.81–0.87 ns, not the historical ~40 ns estimate. The root/child
path is therefore neutral within the 3% gate, not a demonstrated speed win; descriptor checks and identity/lifecycle
initialization consume the saved call overhead. Overflow consistently improves because it avoids four exact
alloc/free-list operations without identity setup. A representative capacity-64 mixed schema retained exactly the same
allocator-visible bytes (span 1496B, overflow 1368B); property tests prove packed bytes are never greater across
capacities 1–512. Keep the superblock for the bounded footprint, simpler ownership, one-call FFI surface, and measured
overflow win—not for the superseded 10× span-setup projection.

**Chrome confirmation (Apple M5 Max, Headless Chrome 150, three fresh-page runs).** The Expo web benchmark uses the
production `WasmAllocator` wrapper, fetches the shipped `allocator.wasm` over HTTP, performs 4,096 warmups, then records
30 alternating samples of 8,192 cycles. It compares the same capacity-64 identity, system, u8, u32, and f64 storage:

| Cycle             | Separate allocations | Superblock |                 Result |
| ----------------- | -------------------: | ---------: | ---------------------: |
| child span, run 1 |            720.21 ns |  683.59 ns | superblock 5.1% faster |
| child span, run 2 |            781.25 ns |  732.42 ns | superblock 6.3% faster |
| child span, run 3 |            756.84 ns |  720.21 ns | superblock 4.8% faster |
| overflow, run 1   |            573.73 ns |  537.11 ns | superblock 6.4% faster |
| overflow, run 2   |            610.35 ns |  585.94 ns | superblock 4.0% faster |
| overflow, run 3   |            598.14 ns |  543.21 ns | superblock 9.2% faster |

The identical wrapper harness under Bun measured child-span results from 0.5% slower to 3.6% faster (median run: 3.2%
faster) and overflow results from 9.6% to 9.9% faster (median run: 9.7% faster). Chrome therefore confirms a small
root/child improvement and a repeatable overflow improvement, not the superseded 10x projection. The retained browser
harness is `packages/lmao-expo-benchmark/src/superblock-benchmark.ts`; its CLI entry point is
`superblock-benchmark.bench.ts`.

### E. Compile-time row-header packing (the write batching that works)

The refuted micro-batching staged values and paid `.set()` overhead. A possible future variant would pack **compile-time
constants** such as entry type, the already-implemented Op-local template ID, and line number into one u32/u64 literal
and emit one element write instead of separate lane writes. No packed row header is implemented or measured.

### F. Template/name dictionaries: Op-local store and the global vocabulary ABI

**Implemented subset.** Literal log messages eligible under the shipped checker rules use private per-Op `u16` IDs in
the hot store. This subset deduplicates and avoids the per-row literal string write, then restores the exact public
string at cold conversion. It does **not** create a program-wide dictionary, a stable trace vocabulary, or a new Arrow
column. The local-store benchmark did not meet the promotion gate, so no performance win is claimed.

**Global vocabulary design.**
[01o §6.1–6.2](../../../specs/lmao/01o_typescript_transformer.md#vocabulary-fragment-and-library-without-source-contract)
owns the 32-bit ABI, checker coverage and Op-local-lane cutover. The
[implementation gap list](../../../agent-todo/lmao-wasm-transformer-errors.md) distinguishes that design from the
shipped registry and local store.

### G. Smaller, cheap wins

- **Promise-preserving fixed-arity span lowering — implemented**: stable checker-proved Ops and inline functions may
  lower to `spanN`; dynamic expressions stay on public `span()` for exactly-once evaluation. Automatic `spanAutoN`
  selection was rejected because it changes observable Promise microtask scheduling.
- **Compile-time masking**: `.mask(preset)` fields could have the mask applied in the emitted write so unmasked values
  never reach the buffer; this is not described here as shipped.
- **Layout-table emission for lmao**: capacities and column layouts are deterministic from schema; a future emitted
  offset table could let both the TS host and Rust arena assert against ABI drift.

The through-line: **compile out all runtime schema interpretation** — classes, layouts, eager sets, capacities,
dictionaries — so the TS host reaches the same "everything static" regime the Rust macro host gets, plus whole-program
analyses (usage-driven pre-allocation, dead columns, vocabulary contracts) that per-crate Rust macros can't see either.

## ttsc-specific opportunities, split by whether lmao must change

### Needing NO lmao changes (pure ttsc/TS host)

- **Lint-stage contract enforcement.** ttsc plugins can also run at the check stage and emit real diagnostics. A future
  program-wide trace-vocabulary contract could make renaming a span/template without updating a checked-in manifest, or
  a dynamic message where stable vocabulary is required, a compile error. Today dynamic messages are a supported
  fallback, and no checked-in vocabulary manifest is generated. Masking gaps on fields typed as PII could likewise
  become a diagnostic rather than a runtime failure.
- **Compile-time log-level stripping.** Production config → `log.debug`/ `log.trace` statements deleted at emit (not
  branched over). Zero-cost disabled levels; composes with feature flags (01p) for flag-gated spans. The "stripped, not
  stubbed" law from the deterministic simulation toolchain, applied to TS.
- **Checker-powered §4 at native speed — implemented.** The ttsc plugin uses checker proof for the tag-chain inliner and
  preserves conservative runtime fallback when a receiver/schema cannot be proven.
- **Compile-time schema-composition checking.** With project references the plugin sees library schemas AND the app's
  `prefix()`/`mapColumns()` wiring — column-name collisions across composed libraries become build-time diagnostics
  instead of runtime startup errors.
- **Profile-guided emission (novel loop).** Test suites already run under `SQLiteTracer` (`DEFAULT_TRACE_DB_PATH`). The
  plugin can read that database as build input: measured column-usage frequencies and span row-count distributions drive
  the eager sets (B) and capacity seeds (C) with real data instead of static bounds — PGO where the profile is lmao's
  own trace output. Deterministic given a checked-in profile snapshot.

### Requiring lmao / lmao-wasm cooperation

- **`alloc_span_with_columns(descriptor)`** — allocation batching (D above): one boundary crossing for span-system +
  identity + the op's full column set; descriptor is a transformer-emitted constant.
- **Layout-hash handshake.** Transformer emits a schema-layout hash constant; a lmao-wasm export validates it at init.
  TS/Rust ABI drift becomes a startup error instead of silent memory corruption.
- **Pre-serialized Arrow schema.** The Arrow schema is static per program; the transformer can emit its IPC bytes as a
  constant so the Rust flush path skips schema construction entirely and every flush reuses one buffer.
- **Cross-host dictionary ids.** A future global template/name dictionary from the proposed portion of F could be
  emitted as a Rust-consumable table, giving TS-host and native-Rust-host (jcode) traces the _same_ stable IDs. Current
  Op-local IDs are private and unsuitable for cross-host comparison.
- **Monomorphized flush kernels.** With the schema contract exported at build time, `lmao-arrow` can generate per-schema
  flush kernels (const generics / codegen) instead of dynamic column dispatch — the Rust-side mirror of A.

## Recommended roadmap

1. **Done**: destructured-context rewriting; Promise-preserving stable `span0`–`span8` lowering; direct inherited child
   creation; packed capacity/capability hints; capability-specialized `_spanPre`; exact-child feature-flag evaluation;
   explicit internal `spanAutoN` retry/thenable fallback; and checker-proved Op-local literal log-template IDs with
   conditional `u16` storage and cold exact-string decode.
2. **Done**: checker-dependent tag/log/result inlining and the line, metadata, span, runtime-hint, and structured
   template-metadata transforms in the native ttsc Go plugin.
3. **Correctness boundary**: keep automatic public `span()` → `spanAutoN()` disabled. Await-compatible type shape does
   not preserve the public Promise path's observable microtask schedule.
4. **Next, measure first**: benchmark the shipped fixed-arity/hint path and the separate Op-local template-ID path on
   target runtimes. Keep structural correctness and ABI claims separate from performance claims.
5. **Implement behind measurement**: compile-time buffer-class emission (A), static eager sets (B), and the specified
   registered vocabulary target (F). The clean cutover removes the private Op-local lane in the same change, which must
   pass every semantic and performance gate.
6. **With lmao-wasm, only after measurement**: evaluate an `alloc_span_with_columns` descriptor export for allocation
   batching (D).
7. **Don't**: value-level write batching or export-call amortization for tag/log writes; the measurements above refute
   that shape at tag-write scale.
