# Transformer × lmao-rs: optimization analysis

Companion to [optimization-investigation.md](./optimization-investigation.md). This document keeps the historical wasm
boundary and tag-write measurements, then records the transformer/runtime optimizations that have since shipped. Do not
extrapolate the measured tag-write numbers to the newer span setup or Op-local log-template-ID paths: no speedup is
claimed for either path without a dedicated benchmark.

Measured on Apple M5 Max, bun/JSC. The tag-write numbers below come from
`packages/lmao-ttsc/benchmarks/inline-vs-codegen.bench.ts`; boundary numbers are cited from
`packages/lmao/benchmarks/wasm-boundary.bench.ts`. They are historical evidence for those exact shapes only. No result
in this document measures the shipped Op-local template-ID pipeline.
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

In-process Rust (jcode, AxE sim) gets the same effect from `define_log_schema!` proc-macro expansion at Rust compile
time (1.9 ns append, 2.5 ns tag write, per the main investigation). The native ttsc transformer is the _TypeScript-host_
analog of that macro — same role and schema-static philosophy, in one compiler implementation.
## Current shipped span optimization

The ttsc Go plugin implements bounded, Promise-preserving span setup plus packed runtime hints. Automatic public
`span()` → `spanAutoN()` lowering is intentionally disabled.

### Promise-preserving fixed-arity lowering and ABI boundary

A checker-proved `ctx.span(name, op, ...args)` with zero through eight trailing arguments may become the Promise-based
`ctx.spanN(...)` ABI when both receiver and Op are stable and every expanded value can be evaluated once. The emitted
child is `Object.create(ctx)`, and the Op's buffer class, remapped view, metadata, function, and runtime hint are passed
directly. Inline arrow/function spans use the same fixed-arity Promise ABI with the receiver's buffer data.

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
helper awaits the first value, applies the existing retry policy, re-invokes only for actual retry attempts, records the
retry/terminal rows, and ends the span exactly once. Throws and rejected thenables use the existing exception path.
Because automatic lowering is disabled, this internal `Result | Promise<Result>` behavior cannot alter the public
Promise API's microtask scheduling. No performance magnitude is asserted here.


## Shipped clean cutover: registered structured vocabulary

This section is the authoritative compiler/runtime ABI. It replaces the former Op-local `u16` representation rather
than layering another ID space over it. The clean cutover removes the per-Op template table and its private lane after
every caller uses the registration ABI. No compatibility alias or dual write survives the cutover.

### Checker recognition and source policy

The compiler recognizes a call by checker semantics, not by the spelling `ctx.log` or by containment in `defineOp`. The
resolved receiver must be LMAO `SpanLogger`/`GeneratedSpanLogger`, and the invoked `info`/`debug`/`warn`/`error`/`trace`
declaration must have LMAO provenance. Typed aliases, parameters, imported logger-bearing helpers, and property paths
therefore participate; unrelated same-named methods, `any`/`unknown`, unproved unions, and dynamic computed members do
not. All proved LMAO logger callsites in source are subject to the same policy:

- operational `info`/`warn`/`error` text is literal; dynamic text, interpolation, and concatenation are compile errors;
- a `structured` call uses a second plain object literal and matching `{field}` placeholders. `{{` and `}}` escape
  literal braces. This clean-cutover grammar replaces the shipped `{{field}}` placeholder spelling;
- a `static` call has literal text and no fields. Literal `debug`/`trace` may be `static` or `structured`; an existing
  runtime string reference is `dynamic`, unregistered, and stored on the raw lane, while avoidable interpolation is
  rejected; and
- `mixed` is reserved for a physical buffer family carrying both numeric vocabulary and dynamic lanes. It is not a
  callsite class or kind tag. `kindTags` are entry semantic kinds.

Checker-proven literal trace/span names are registered as `SPAN_NAME`; dynamic names remain on the dynamic string lane.
Shipped code stores all span names as strings, so span-name registration is target-only and must preserve the public
Promise/microtask contract.

```typescript
// Source in an application or independently compiled library.
export function report(log: SpanLogger, ctx: OpContext, job: Job, elapsedMs: number) {
  log.info('completed {jobId} in {elapsedMs}ms', { jobId: job.id, elapsedMs });
  log.info('literal braces: {{ok}}');
  log.trace(traceText); // supported dynamic lane
  return ctx.span('complete-job', completeJobOp, job.id); // registered SPAN_NAME
}
```

The compiler removes the fields object. The following registration is emitted **JavaScript**, and the installer import
is a compiler-emitted/imported side-effect dependency so ESM installs the callback first:

```javascript
import '@smoothbricks/lmao/vocabulary/register/v1';
const REGISTER_VOCABULARY = Symbol.for('@smoothbricks/lmao/vocabulary/register/v1');
const $$binding = globalThis[REGISTER_VOCABULARY]($$typedArrayFragment);

const $$v0 = job.id;
const $$v1 = elapsedMs;
log._infoStructured2($$binding[0], LINE, $$v0, $$v1);
log._infoStatic0($$binding[1], LINE);
log.trace(traceText);
return ctx.spanVocabulary1(LINE, $$binding[2], completeJobOp, job.id);
```

Private seam names may change, but warmed registered writes use direct `binding[ordinal]` loads and fixed monomorphic
stores with zero object, array, rest-argument, or interpolated-string allocation. The target span-name seam remains
Promise-based like `span1`; dynamic names continue through public `span(name, ...)`. Dense index zero is valid, and
nullable rows use a validity bitmap rather than an index sentinel. A predictable overflow/slow path remains correct.

### Versioned fragment registration

The runtime type contract, separate from emitted JavaScript, is:

```typescript
type VocabularyFragmentV1 = {
  schemaVersion: 1;
  idAlgorithm: 'sha256-24-v1';
  contentHash: string;
  ids: Uint32Array;
  kindTags: Uint8Array;
  utf8: Uint8Array;
  offsets: Int32Array;
};
type VocabularyBinding = Uint32Array;
type RegisterVocabularyV1 = (fragment: VocabularyFragmentV1) => VocabularyBinding;
```

For `N = ids.length`, `kindTags.length` and binding length are `N`; `offsets.length` is `N + 1`, `offsets[0]` is zero,
`offsets[N]` is `utf8.length`, and offsets are monotonic and in range. Record `i` is
`utf8.subarray(offsets[i], offsets[i + 1])` with exact bytes:

```text
u32le textLen | text UTF8 | u16le fieldCount |
repeated(u16le nameLen | name UTF8 | u16le columnLen | column UTF8)
```

`kindTags[i]` is `1` (`LOG_TEMPLATE`) or `2` (`SPAN_NAME`). `ids[i]` is the first 24 bits, interpreted big-endian, of
`SHA-256(kindTag || recordBytes)`. `contentHash` is lowercase SHA-256 hex of:

```text
u8 schemaVersion |
u16le algorithmLen | idAlgorithm UTF8 |
u32le ids.length | repeated(u32le id) |
u32le kindTags.length | kindTags bytes |
u32le utf8.length | utf8 bytes |
u32le offsets.length | repeated(i32le offset)
```

This makes numeric array serialization explicitly little-endian, independent of host typed-array byte order. The
callback validates lengths, integer ranges, tags, UTF-8, grammar, IDs, and hash.

Every precompiled library/chunk imports the LMAO installer side effect, obtains the shared Symbol, registers during
module evaluation, and closes over the binding. The consuming build needs neither source/checker graph nor a second
transform. Bundling retains installation/registration side effects and each callsite-to-binding pairing.

The runtime validates then copies or merges inputs into a runtime-owned immutable dictionary generation. New fragments
append unseen values in registration order; dense indices are not sorted or canonical. Existing bindings remain
prefix-valid forever, and each generation is exactly its predecessor plus appended values. An `ArrowLease` pins the
exact backing store, WASM epoch, chunks, and dictionary generation used by its batch until explicit release. Stable IDs
and content bytes—not dense indices—provide deterministic cross-order identity and checksums; only decoded values and
schema semantics are deterministic across registration orders.

Registration is idempotent for byte-identical fragments and deduplicates equal `(kindTag,id,recordBytes)` records.
Reusing an ID for different kind/bytes, or `contentHash` for another fragment, is fatal before the fragment is writable.
An unavailable/incompatible callback is a strict-build diagnostic or startup failure, never a guessed index. Startup
contributes bindings to the immutable `PhysicalLayoutPlan`; live shapes never mutate and warmed writes have no
registration/version branch.

### Diagnostics, fallbacks, and order

`LMAO_DYNAMIC_OPERATIONAL_TEXT`, `LMAO_AVOIDABLE_INTERPOLATION`, `LMAO_FIELDS_NOT_OBJECT_LITERAL`, and
`LMAO_PLACEHOLDER_MISMATCH` reject the source-policy violations above. In strict optimized mode,
`LMAO_LOGGER_PROOF_REQUIRED` rejects an apparent typed LMAO call that cannot be proven. A genuinely unrelated/unproved
call is left byte-shape unchanged; a proved LMAO policy violation never silently takes the raw fallback. This prevents
optimizer success from changing vocabulary coverage.

The emitted call evaluates its receiver first, arguments left-to-right, and each field initializer exactly once in
source property order. Compiler temporaries may precede stores only to preserve this order. Throws, getters reached by
field value expressions, and other side effects remain at their source-relative point; runtime field expressions never
move into module registration. Vocabulary lowering also preserves the public span contract: automatic `span()` to
`spanAutoN` selection remains forbidden, so Promise assimilation and microtask turns do not change.

### Promotion gates

Repo-standard Mitata scenarios must compare `plugin-off`, `plugin-on/current`, and target variants with a
position-balanced schedule in order-reversed independent processes. They record machine-readable raw samples, semantic
checksums, p50/p95/p99/p99.9, and allocation/GC metadata where available, and produce a commit-message-ready summary.
Any checksum, decode, collision, or evaluation-order difference fails promotion.

Semantic acceptance covers `static` templates, `structured` fields and brace escapes, dynamic debug/trace text, literal
`SPAN_NAME` entries, and dynamic span-name fallback. It checks binding length, append-only prefix stability, dense zero
with validity-based nulls, exact record bytes/hashes, decoded Arrow values, and unchanged Promise/microtask observations
under at least two reversed registration orders.

Measurements are separate for `startup`, `span setup`, `warmed entry write`, `overflow/slow path`, and
`per-request flush`; one phase cannot hide another. The warmed target must allocate zero, use fixed monomorphic stores,
improve p50 by at least 10% over `plugin-on/current`, and regress no reported tail percentile by more than 3%. The
overflow path must remain correct and bounded with no greater than 3% p99.9 regression. Per-request flush must reuse the
`PhysicalLayoutPlan`, process-dense Arrow vocabulary, and `ArrowLease`, allocate few or no objects, and regress no
percentile by more than 3%. Startup and span setup costs are explicit and must not regress more than 3% unless the
commit summary names and justifies the trade. Until every gate passes, the target remains unpromoted and no
zero-overhead claim is permitted.

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

### C. Static capacity seeding — implemented for bounded direct logs

The shipped transformer computes a per-Op packed initial-capacity hint: two reserved rows plus direct known log calls.
Loops, counts above `0xffff`, or unsafe context analysis encode adaptive capacity instead. This is deliberately narrower
than whole-program or profile-guided row prediction, and normal overflow chaining remains the correctness fallback.
Per-op specialized buffer-class generation remains future work.
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

The refuted micro-batching staged values and paid `.set()` overhead. A possible future variant would pack **compile-time
constants** such as entry type, the already-implemented Op-local template ID, and line number into one u32/u64 literal
and emit one element write instead of separate lane writes. No packed row header is implemented or measured.

### F. Template/name dictionaries: shipped Op-local store versus specified global target
**Implemented subset.** Literal log messages eligible under the shipped checker rules use private per-Op `u16` IDs in
the hot store. This subset deduplicates and avoids the per-row literal string write, then restores the exact public
string at cold conversion. It does **not** create a program-wide dictionary, a stable trace vocabulary, or a new Arrow
column. The local-store benchmark did not meet the promotion gate, so no performance win is claimed.

**Specified, not shipped.** The clean-cutover ABI above expands checker-semantic coverage to `static`/`structured`
logger callsites and literal `SPAN_NAME` records, emits stable content-derived `u24` identities, and registers fragments
into append-only runtime-dense Arrow bindings. Independently compiled libraries self-register without source at the
consuming build. None of stable IDs, fragment registration, `VocabularyBinding`, registered span names, or runtime-owned
immutable dictionary generations is shipped. The target cannot be promoted until its phase-specific gates pass.

### G. Smaller, cheap wins

- **Promise-preserving fixed-arity span lowering — implemented**: stable checker-proved Ops and inline functions may
  lower to `spanN`; dynamic expressions stay on public `span()` for exactly-once evaluation. Automatic `spanAutoN`
  selection was rejected because it changes observable Promise microtask scheduling.
- **Compile-time masking**: `.mask(preset)` fields could have the mask applied in the emitted write so unmasked values
  never reach the buffer; this is not described here as shipped.
- **Layout-table emission for lmao-rs**: capacities and column layouts are deterministic from schema; a future emitted
  offset table could let both the TS host and Rust arena assert against ABI drift.

The through-line: **compile out all runtime schema interpretation** — classes, layouts, eager sets, capacities,
dictionaries — so the TS host reaches the same "everything static" regime the Rust macro host gets, plus whole-program
analyses (usage-driven pre-allocation, dead columns, vocabulary contracts) that per-crate Rust macros can't see either.

## ttsc-specific opportunities, split by whether lmao-rs must change

### Needing NO lmao-rs changes (pure ttsc/TS host)

- **Lint-stage contract enforcement.** ttsc plugins can also run at the check stage and emit real diagnostics. A future
  program-wide trace-vocabulary contract could make renaming a span/template without updating a checked-in manifest, or
  a dynamic message where stable vocabulary is required, a compile error. Today dynamic messages are a supported
  fallback, and no checked-in vocabulary manifest is generated. Masking gaps on fields typed as PII and the
  one-`defineModule`-per-file invariant could likewise become diagnostics rather than runtime/build failures.
- **Compile-time log-level stripping.** Production config → `log.debug`/ `log.trace` statements deleted at emit (not
  branched over). Zero-cost disabled levels; composes with feature flags (01p) for flag-gated spans. The "stripped, not
  stubbed" law from AxE's sim toolchain, applied to TS.
- **Checker-powered §4 at native speed — implemented.** The ttsc plugin uses checker proof for the tag-chain inliner and
  preserves conservative runtime fallback when a receiver/schema cannot be proven.
- **Compile-time schema-composition checking.** With project references the plugin sees library schemas AND the app's
  `prefix()`/`mapColumns()` wiring — column-name collisions across composed libraries become build-time diagnostics
  instead of runtime startup errors.
- **Profile-guided emission (novel loop).** Test suites already run under `SQLiteTracer` (`.trace-results.db`). The
  plugin can read that database as build input: measured column-usage frequencies and span row-count distributions drive
  the eager sets (B) and capacity seeds (C) with real data instead of static bounds — PGO where the profile is lmao's
  own trace output. Deterministic given a checked-in profile snapshot.

### Requiring lmao-rs / lmao-wasm cooperation

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
   registered vocabulary target (F). Keep their benchmark variants separate from the shipped private Op-local lane;
   clean cutover removes that lane only after all semantic and performance gates pass.
6. **With lmao-wasm, only after measurement**: evaluate an `alloc_span_with_columns` descriptor export for allocation
   batching (D).
7. **Don't**: value-level write batching or export-call amortization for tag/log writes; the measurements above refute
   that shape at tag-write scale.