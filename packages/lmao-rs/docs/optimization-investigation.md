# lmao-rs Optimization Investigation

Date: 2026-07-11. Hardware: Apple M5 Max (~4.43 GHz), bun 1.3.14 (JSC), rustc 1.96.0 (aarch64, `lto=true` release).
Question: does lmao-rs unlock optimizations beyond the current TS+WASM design, given the design history that
JS TypedArray writes beat NAPI and JIT-compiled WASM beat NAPI?

## 1. Existing benchmark survey

| Bench | Location | Measures | Status on this machine |
|---|---|---|---|
| `js-vs-wasm.bench.ts` | `packages/lmao/benchmarks/` | JsBufferStrategy vs WasmBufferStrategy, warm + cold, 6-col schema | **Fully runnable after rebuilding the artifact.** The checked-in `dist/allocator.wasm` was stale and trapped (`unreachable`) inside `alloc8B`; rebuilding with zig 0.16.0 via the smoothbricks devenv (`cd tooling/direnv && devenv shell -q -- sh -c 'cd .../packages/lmao && zig build -Doptimize=ReleaseSmall'`) fixed it. |
| `typed-array-views.bench.ts` | same | lazy vs cached vs hybrid TypedArray/DataView creation | runnable (not re-run: not decision-relevant here) |
| `timestamps.bench.ts` | same | timestamp API strategies | runnable |
| `op.bench.ts` | same | `Op._invoke` monomorphism | runnable |
| `lmao-vs-wasm.bench.ts` | `axe-runtime/src/__tests__/` | real lmao vs WASM-backed SpanBuffer (spec 01q) | needs `zig build` artifacts — blocked |
| `column-write-realistic.bench.ts` | same | JS vs WASM realistic trace shapes | blocked: `dist/column_write.wasm` missing |
| `freelist-allocator.bench.ts` | same | JS TypedArrays vs WASM freelist allocator | blocked (same) |
| `span-start-ffi.bench.ts` | same | **JS vs NAPI vs WASM** span lifecycle — the source of the "TypedArray beat NAPI" claim | blocked: proof artifacts not built |
| `ptr-caching.bench.ts` | same | NAPI ArrayBuffer pointer caching | blocked |
| Recorded results | `packages/lmao/.planning/phases/04-fix-wasm-allocator/04-01-SUMMARY.md` | warm WASM 1.06–1.44x over JS; cold WASM 127–271x slower | trusted as recorded |

**Gaps in existing coverage** (before this investigation): no isolated boundary-crossing cost bench, no
bulk-vs-per-value crossing comparison, no string-flush strategy bench, no native (non-WASM, non-NAPI) column,
and no flush/Arrow-conversion benchmark anywhere.

### Fresh JS numbers (mitata, bun, this machine)

From `js-vs-wasm.bench.ts` (JS side):

| Shape | JS (warm) | JS (cold) |
|---|---|---|
| Simple trace | JS 1.94 µs / WASM 1.49 µs (WASM 1.3x) | JS 2.94 µs / WASM 615.9 µs (JS 209x) |
| Trace with 6 tags | JS 2.35 µs / WASM 1.47 µs (WASM 1.6x) | JS 3.39 µs / WASM ~cold-dominated |
| Nested spans (3 levels) | JS 6.92 µs / WASM 5.56 µs (WASM 1.25x) | — |
| 50 log entries | JS 5.42 µs / WASM 5.50 µs (tie; ≈ 60–70 ns/log marginal) | JS 6.48 µs |
| 100-trace reuse | JS 174.9 µs / WASM 129.5 µs (WASM 1.35x) | — |

Fresh numbers CONFIRM the recorded 04-01 results: warm WASM wins are modest (1.25–1.6x), cold start is
catastrophic (209x), and per-log marginal cost is a tie — the strategy split is stable across machines.

## 2. New benchmarks added

- `packages/lmao-rs/crates/lmao-core/benches/hot_path.rs` (criterion): span lifecycle, span+50 logs,
  1000-append per-event cost, tag-write proxy (bitmap set + f64 store), 256-string dictionary build.
  `SpanBuffer::append` was implemented (overflow chaining per 01b2) to make these honest — it also turned the
  `appends_are_ordered_and_lossless` proptest green (its overflow-row accounting was corrected to match
  01b4/01b5: overflow buffers carry no reserved rows).
- `packages/lmao/benchmarks/wasm-boundary.bench.ts` (mitata): instantiates the checked-in `allocator.wasm`
  via **raw exports** (bypassing the stale TS wrapper) and isolates boundary costs.

## 3. Results

### Boundary-crossing costs (bun/JSC, real Zig allocator.wasm)

| Operation | Cost |
|---|---|
| Near-empty wasm export call (`get_bump_ptr`) | **0.97 ns** |
| `write_col_f64(offset, idx, value)` via export | **39.2 ns** |
| Same store via JS Float64Array view over wasm memory | 7.6 ns |
| Same store via plain JS Float64Array | 0.54 ns |
| 64 f64 writes via 64 export calls | 2.51 µs (**147x slower** than plain JS) |
| 64 f64 writes via JS view, 0 crossings | 24.1 ns |
| 64 plain JS writes | 17.1 ns |
| `span_start` via wasm export | 53.1 ns |
| JS equivalent (2 typed stores + anchored timestamp) | 37.7 ns (**1.41x faster**) |

Note the asymmetry: the *call* itself is ~1 ns; it's argument marshalling (3 args incl. f64) plus the work
being trivial that makes per-value crossing a 40 ns tax. **The original design decision is re-confirmed on
current hardware/runtimes: never cross the boundary per value; JS TypedArray writes into (wasm or JS) memory
are the right hot path for a JS host.**

### String handling (approach d)

| Strategy (256 category strings, ~40 unique) | Cost |
|---|---|
| JS Map dictionary count+dedupe (current design) | **3.30 µs** |
| Bulk `encodeInto` all strings into wasm memory + offsets | 7.00 µs |
| Map dedupe then bulk-encode unique | 10.62 µs |

**"Strings stay in JS" is validated.** Shipping raw string bytes into WASM for dictionary building there
loses even before the WASM-side hashing work is counted.

### Native Rust (criterion, lmao-rs)

| Shape | Rust native | JS equivalent | Speedup |
|---|---|---|---|
| Span lifecycle (start+ok, cap 8, incl. 2 Vec allocs) | 19.0 ns (fixed clock) / 49.9 ns (system clock) | ~1,700 ns warm trace | **~34–89x** |
| Span + 50 logs | 120 ns (fixed) / 931 ns (system clock) | 5,390 ns | **~6–45x** |
| Per-event append (warm, no overflow) | **1.9 ns** | ~60–70 ns/log | **~30x** |
| Tag write (bitmap + f64 store) | 1.5 ns | ~7.6 ns (JS view) / ~0.5 ns (plain array store, excl. framework) | comparable at the raw-store level |
| Dictionary build, 256 strings, std HashMap | 5.8 µs | 3.3 µs (JS Map) | **JS wins 1.8x** (see caveat) |

Caveats: (1) the JS "warm trace" numbers include lmao's framework overhead (async wrappers, ctx objects,
generated writers) that a Rust facade would also add some of — but Rust's facade overhead is nanoseconds, not
microseconds; the JS marginal per-log cost (~60 ns) vs Rust append (1.9 ns) is the cleanest apples-to-apples.
(2) The dictionary loss is std's SipHash vs JSC's cached string hashes; `rustc-hash`/`fxhash` typically
brings this to ~1–2 µs — benchmark before claiming. (3) `Instant::now()` dominates the Rust hot path
(19 ns → 50 ns lifecycle; 120 ns → 931 ns for 51 stamps): ~15–16 ns/read on macOS. A coarse/TSC clock or
one-stamp-per-batch policy is the highest-leverage native optimization available.

## 4. Verdicts per approach

| Approach | Verdict |
|---|---|
| (a) Native in-process Rust (jcode, AxE sim) | **Real, and the big win.** 30–90x on comparable shapes; per-event cost 1.9 ns satisfies AxE's zero-alloc/≤20%-overhead gates with enormous margin. This is where lmao-rs pays for itself, no boundary exists at all. |
| (b) Rust-WASM hot path for the TS host | **Myth (for per-event work).** Not directly testable on this machine (no wasm32 std in the nix toolchain, no rustup/zig), but the boundary measurements make it moot: 39 ns per crossed value vs 0.5–7.6 ns staying in JS. No codegen quality difference (rustc vs Zig) can recover a 5–70x boundary tax. Rust-WASM could only match the current design by adopting the same shape: JS writes TypedArray views, WASM only allocates — i.e., a drop-in allocator.wasm replacement with identical economics (recorded: 1.06–1.44x). Port for maintainability if desired, not for speed. |
| (c) Flush/Arrow conversion in Rust-WASM | **Plausible, unproven, bounded.** Cold path, one crossing per flush (1 ns fixed cost is nothing amortized over a batch). But the JS dictionary work it would replace is already fast (3.3 µs/256 strings, and strings would still need to cross as bytes: +7 µs). Only worth it if profiling shows the 1811-line convertToArrow dominating real flushes; requires the string-crossing cost to be amortized by doing MORE per crossing (full batch conversion + IPC serialization in one call). Build `lmao-arrow` native first (jcode needs it anyway), then decide with a real flush benchmark. |
| (d) Bulk string bytes into WASM at flush | **Refuted.** Measured slower (7.0–10.6 µs vs 3.3 µs). Keep strings in JS for JS hosts; in native Rust the question disappears. |
| (e) Timestamp cost | **New finding.** `Instant::now()` is ~80% of the native hot path. Investigate `mach_continuous_approximate_time`/coarse clocks or batch stamping; in the AxE sim the injected deterministic clock makes this free. |

## 5. Recommended roadmap

1. **Native-first.** Implement lmao-rs for in-process hosts (jcode tracer, AxE sim). Every measured number
   says this is where the order-of-magnitude win lives. Keep the zero-alloc append path (already 1.9 ns).
2. **Clock strategy.** Add a `CoarseClock`/batch-stamp option behind the `Clock` trait; benchmark
   `Instant::now()` alternatives on macOS/Linux before committing.
3. **Use `rustc-hash` for flush dictionaries** (and benchmark vs the JS Map numbers above).
4. **Fix the TS wrapper/artifact drift** in `packages/lmao` (wrapper calls `alloc8B`; wasm exports
   `alloc_col_8b`) and check a rebuilt artifact + a wrapper-binding test into CI so `js-vs-wasm.bench.ts`'s
   WASM column runs again.
5. **Defer Rust-WASM for the TS host.** If/when wanted for single-implementation maintenance, ship it as a
   drop-in `allocator.wasm` replacement (same exports, JS keeps writing views) — economics unchanged, so this
   is a maintenance decision, not a performance one.
6. **Decide (c) with data**: once `lmao-arrow` native conversion exists, add a flush bench (JS convertToArrow
   vs native) before investing in a WASM conversion path.

## 6. Reproduction

```
# JS side (bun)
cd packages/lmao
bun run benchmarks/js-vs-wasm.bench.ts        # JS column only until wrapper drift is fixed
bun run benchmarks/wasm-boundary.bench.ts     # new: boundary costs, raw exports

# Rust side
cd packages/lmao-rs
cargo bench -p lmao-core --bench hot_path
```

Environment notes: zig 0.16.0 is available via the smoothbricks devenv (`cd tooling/direnv && devenv shell -q -- zig ...`)
and was used to rebuild `allocator.wasm`. Rust→WASM remains uncompiled here (nix rustc has no wasm32 std,
no rustup, devenv has no rust) — approach (b) is argued from measured boundary costs + the confirmed
JS-vs-WASM numbers rather than a compiled Rust-WASM artifact.
