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

## Recommended roadmap

1. **Done**: destructured-context rewriting (01o §3) closes the last
   spec-listed transformer gap; ttsc Go plugin covers the structural
   transforms for tsgo builds.
2. **Next**: port the Checker-dependent tag-chain inliner to the ttsc
   plugin (tsgo Checker shims), keeping output byte-identical to the TS
   inliner (snapshot parity tests between both implementations).
3. **Later, measure-first**: constant-offset direct-view emission mode for
   wasm-backed buffers (bounded ~5 ns/write win; only with real flush
   benches showing it matters end-to-end).
4. **Don't**: write-batching, export-call amortization for tag/log writes.
