# Transformer × lmao-rs: optimization analysis

Companion to [optimization-investigation.md](./optimization-investigation.md).
Question: with the Rust wasm allocator in place, what can the **compile-time
transformer** (ts-transformer or the new ttsc Go plugin in
`packages/lmao-transformer`) do to avoid or amortize the measured
~39–42 ns/call JS↔wasm export boundary tax?

Measured on Apple M5 Max, bun/JSC. New numbers from
`packages/lmao-transformer/benchmarks/inline-vs-codegen.bench.ts`; boundary
numbers cited from `packages/lmao/benchmarks/wasm-boundary.bench.ts`.

## Bench: 6 tag writes (1 enum, 2 category, 2 number, 1 bool)

| Shape | ns/iter | alloc/iter |
|---|---|---|
| A. fluent chain (no transformer, runtime fallback) | 22.1 | ~4 B |
| B. runtime-codegen writer (`new Function()`, lmao today) | 24.2 | 0 |
| C. **transformer-inlined direct writes** (tagChainInliner output) | **19.5** | 0 |
| D. compile-time micro-batching (staging buffer + `.set()`) | 77.2 | 0 |

Reference boundary costs: wasm export call ~39–42 ns/value; JS TypedArray
view write into wasm memory ~7.6 ns; plain JS write 0.5–2 ns.

## Verdicts

### 1. Inlining replaces runtime codegen at zero cost — ship it (CSP win)

C ≈ B within noise (actually slightly ahead) and both are allocation-free.
The transformer's statically-emitted writes fully replace the
`new Function()` writer **with no performance loss** — which matters on
CSP-restricted hosts (Cloudflare Workers, spec 01q/01s) where runtime
codegen is banned and lmao today must fall back to slower paths. The
transformer is the zero-eval codegen. (Caveat: bun/JSC numbers; the spec's
V8 hidden-class claims were not re-validated here.)

### 2. Compile-time write batching is a myth at tag-write scale

D is **4x worse** than C: `subarray()` + `.set()` overhead swamps any
benefit when the batch is a handful of values. Batching only pays when it
*eliminates wasm export calls* (~40 ns each) — but lmao's design never
makes export calls for writes in the first place (see 3). Do not build
write-batching into the transformer.

### 3. The right wasm strategy is direct-view emission, not export amortization

With the Rust allocator (`lmao-wasm`), per-value exports
(`write_col_f64`, 42 ns) are for correctness testing, not the hot path.
The production pattern stays exactly lmao's original design: **JS-side
TypedArray view writes into wasm linear memory** (~7.6 ns), with export
calls only for lifecycle (`alloc_*`, `span_start`, `span_end_*` — 1–2 per
span, already amortized over the span's writes).

The transformer's lever: the inliner currently emits
`buf.field_values[0] = v` against the generated buffer class's views. For
the wasm-backed `WasmBufferStrategy`, those views point into wasm memory
already — so the inliner output composes with lmao-rs **unchanged**. An
optional future mode could fold the schema's column offsets into constants
(`u8view[BASE + 12] = v`) and skip the view-object indirection, but the
measured gap (view write 7.6 ns vs plain 0.5–2 ns) bounds the win at a few
ns per write; only worth it bundled with the checker-aware ttsc port.

### 4. Column-name discipline under library composition (spec 01e)

Any inliner emission — TS or Go — must write **library-local (unprefixed)**
column names. `.prefix()`/`.mapColumns()` remapping is cold-path-only via
`RemappedBufferView` at Arrow conversion. The transformer must never
resolve prefixes into hot-path writes; offsets/constants it folds are
per-library-schema, not per-application-schema.

### 5. Native lmao-rs hosts don't need the transformer

In-process Rust (jcode, AxE sim) gets the same effect from
`define_log_schema!` proc-macro expansion at Rust compile time (1.9 ns
append, 2.5 ns tag write, per the main investigation). The transformer is
the *TypeScript-host* analog of that macro — same role, same schema-static
philosophy, two compilers.

## The bigger design space: typed macros over the schema, not just call sites

The verdicts above treat the transformer as a call-site rewriter. Its real
power is that it sees **the schema definitions themselves**
(`defineLogSchema`/`defineModule` object literals: field kinds, enum
values, eagerness) *and* **every call site in the program**. That enables a
class of whole-program, compile-time decisions that runtime codegen
structurally cannot make — lmao's runtime `new Function()` sees one schema
value at startup; it never sees usage. Ordered by expected value:

### A. Compile-time buffer-class emission (kill runtime codegen entirely)

Today only tag *writes* are inlined; the SpanBuffer class itself is still
generated at runtime via `new Function()` (per-schema, at startup). The
transformer can emit the entire generated class as source at build time —
the true `define_log_schema!` analog. Wins: zero runtime codegen anywhere
(full CSP compliance, not just hot-path), no per-schema compile at startup
(cold-start win for Workers/Lambda), and the emitted class is visible to
the bundler/minifier. This subsumes verdict 1.

### B. Static eager/lazy sets — pre-allocation decided at compile time

The lazy-to-eager promotion ratchet (01b2) is unshipped runtime machinery
that needs ≥100 samples to converge; meanwhile every lazy column pays a
first-touch alloc (measured 133 ns) plus a branch per access. The
transformer counts actual column writes across **all** call sites and
rewrites the schema literal with the exact eager set — used columns
pre-allocated at construction, never-written columns flagged (dead-column
lint) or elided. Deletes the runtime stats machinery and converges at
build time, per op rather than per schema.

### C. Static capacity seeding — no overflow chaining, no convergence

The capacity ratchet starts at a default and converges by grow/shrink,
paying overflow-chain allocations and chained flush walks in between. A
span body's row count has a static component: 2 fixed rows + #log
statements + tag writes (dynamic loops bounded with a fallback). The
transformer computes a per-op initial-capacity hint and folds it into op
metadata: spans start at the right tier. Composes with B into **per-op
specialized buffer classes** whose constructors are straight-line
allocations of constant sizes.

### D. Allocation batching across the wasm boundary (the batching that pays)

Verdict 2 refuted *write* batching — writes were never boundary crossings.
Allocations are: with `WasmBufferStrategy`, span_start + identity + each
lazy column's first touch are export calls (~40 ns each). A span touching
8 columns pays ~10 crossings ≈ 400 ns. With B giving the transformer the
op's full column set, it can emit one constant column-descriptor and call
a (to-be-added) `lmao-wasm` export `alloc_span_with_columns(desc)` — one
crossing, arena-side loop at ~20 ns/alloc native. ~10x on the per-span
allocation overhead, purely additive to the existing ABI.

### E. Compile-time row-header packing (the write batching that works)

The refuted micro-batching staged values and paid `.set()` overhead. The
working variant packs **compile-time constants**: entry_type, template id
(see F), and line number are all static per call site — fold them into one
u32/u64 literal and emit a single element write
(`buf.header[idx] = 0x2A0007_04 | dynBits`) instead of three. No staging,
no `.set()`, and it needs F's template ids to exist.

### F. Program-wide template/name dictionaries at build time

Message templates and span names are static strings. The transformer can
collect them across the whole program, assign stable u16 ids, emit
`log.info(TEMPLATE_42)` as a numeric write, and ship the pre-built, sorted
dictionary as a constant. Hot path: string push → int write. Flush: the
2.89–3.3 µs dictionary build then covers only dynamic categories — the
template/name dictionary is precomputed and shared across every flush.
Bonus: the emitted dictionary IS the versioned trace-vocabulary artifact
AxE 08-trace-testing requires (selector-by-template becomes selector-by-id;
renaming a span visibly changes a checked-in contract file — schema review
for free).

### G. Smaller, cheap wins

- **spanSync selection**: bodies with no `await` → rewrite `span` to
  `spanSync`, skipping promise allocation.
- **Compile-time masking**: `.mask(preset)` fields get the mask applied in
  the emitted write — unmasked values never reach the buffer.
- **Layout-table emission for lmao-rs**: capacities and column layouts are
  deterministic from schema; emit the offset table both the TS host and
  the Rust arena assert against (catches ABI drift at build time).

The through-line: **compile out all runtime schema interpretation** —
classes, layouts, eager sets, capacities, dictionaries — so the TS host
reaches the same "everything static" regime the Rust macro host gets, plus
whole-program analyses (usage-driven pre-allocation, dead columns,
vocabulary contracts) that per-crate Rust macros can't see either.

## Recommended roadmap

1. **Done**: destructured-context rewriting (01o §3) closes the last
   spec-listed transformer gap; ttsc Go plugin covers the structural
   transforms for tsgo builds.
2. **Next**: port the Checker-dependent tag-chain inliner to the ttsc
   plugin (tsgo Checker shims), keeping output byte-identical to the TS
   inliner (snapshot parity tests between both implementations).
3. **Design next (highest leverage)**: compile-time buffer-class emission
   (A) + static eager sets (B) — together they kill runtime codegen and the
   lazy first-touch cost; then template dictionaries (F) which unlock
   header packing (E) and the AxE vocabulary contract.
4. **With lmao-wasm**: `alloc_span_with_columns` descriptor export for
   allocation batching (D) — the one boundary amortization with real
   arithmetic behind it (~10 crossings → 1).
5. **Later, measure-first**: constant-offset direct-view emission
   (bounded ~5 ns/write win); capacity seeding (C) once B lands.
6. **Don't**: value-level write-batching or export-call amortization for
   tag/log writes (refuted above).
