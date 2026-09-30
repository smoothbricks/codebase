# Span/Log Writer Lanes <a id="smoo/lmao!n/span-writer-lanes"></a>

One writer architecture serves five execution lanes: Browser, Node.js, react-native, plain Bun, and an embedding host
runtime — a process that links `lmao-core` natively and runs JavaScript inside it. The architecture is: **one shared
seam (`TraceRootFactory` + `ThreadSpanBufferBinding`), one host-first registration inversion (`span-buffer/aot/v1`), one
native row store (`lmao-core`'s `ThreadSpanBuffer`), and per-lane providers selected at entrypoint or registration time
— never at call time.** On every lane that writes the native row store, the row _lifecycle_ crosses the binding as
scalar calls and attribute _values_ do not cross at all: JavaScript stores them into the store's own memory through
TypedArray views.

Measured floors (ABBA, 96 blocks × 10,000 calls, positions 1↔4/2↔3, three-repetition floors, Apple M5 Max arm64-darwin;
the release target is x64 Linux — `tooling/direnv/devenv.smoo.nix` declares `x86_64-unknown-linux-gnu` as the only
non-host Rust target, and floors do not transfer between the two):

| Arm                                        | Floor          |
| ------------------------------------------ | -------------- |
| `bun:ffi` no-op                            | 1.21–1.62 ns   |
| 4×u32 native store                         | 3.82–4.99 ns   |
| pre-encoded `ptr,len` row                  | 5.33–5.66 ns   |
| generic JSC host scalar refusal            | 6.60–7.16 ns   |
| log-shaped string refusal                  | 21.12–22.01 ns |
| JS-heap coarse row / exact row             | 58.8 / ≈84 ns  |
| Rust exact row (16.75 ns clock + 2.65 buf) | ≈19.4 ns       |
| Rust row, `LOG_STAMP_REFRESH = 16` cache   | 2.91 ns        |

Per attribute store, M5 Max / Bun 1.4.0, 128-row spans, best-of-five: a Proxy trap forwarding `lane[i] = v` costs 10.8
ns, a plain method call 2.7 ns, a TypedArray store 0.4 ns (`src/lib/wasm/threadSpanView.ts`, `laneProxy`). That gap is
why an attribute value is a store into the row store's cells and not a call into the store.

## 1. The seam

**Two layers, and no third.** The logical seam every lane implements is `TraceRootFactory`
(`packages/lmao/src/lib/traceRoot.ts`) with its four monomorphic primitives (`_timestampNow`, `_appendLogEntry`,
`_writeSpanStart`, `_writeSpanEnd`). The physical seam for row storage is `ThreadSpanBufferBinding`
(`packages/lmao/src/lib/wasm/threadSpanBuffer.ts`), which is provider-neutral and string-level:

- **Lifecycle, as calls:** `openSpan`, `openSpanStatic`, `end`, `appendLog`, `appendLogStatic`, `setScope`,
  `setCompletionMessage`, `intern`, `reset`, `free`. Each is one call into the store. What crosses: integers (span id,
  entry type, ordinal, vocabulary id, line), a `bigint` timestamp on lanes where JavaScript owns the clock, and strings
  only on cold paths — `intern` caches, so a warm string crosses nothing and encodes nothing (hit rate measured
  4,507,371/129 on the benchmark workload). Row-producing calls return the packed receipt `(span_id << 32) | row`, and
  bare `0` on refusal (`lmao-core/tests/thread_ffi_oracles.rs`).
- **Values, as memory:** `attributeCells(block)` hands out the bytes of one block's attribute cells. The lane keeps one
  set of TypedArray views per block (`ThreadSpanCells`, created once per store, not per span) and writes a value as a
  store plus one validity bit.

`lmao-core`'s `AttributeCells` (`crates/lmao-core/src/attribute_cells.rs`) fixes the cell layout both sides compute: per
schema attribute `i`, `capacity` little-endian `u64` value words followed by a validity bitmap of `ceil(capacity/64)`
words, field stride `capacity + ceil(capacity/64)`. `number` stores its `f64` bits, `uint64` its value, and `boolean`,
`category`/`text` (a 1-based intern ordinal) and `enum` (a variant index) store the low 32 bits. The words are shared
memory, not a copy: the Rust writer, the JavaScript views and the Arrow converter read and write the same allocation.
Decoding is total — a text ordinal the store never issued or an enum index outside the variants reads as absent — so no
bit pattern a view can store is a panic.

Blocks are recycled, never freed while the store lives, so a view's address stays valid across `reset` and
`retain_open`, and a flushed store writes its next window without allocating.

**Text is reclaimed by epoch.** Every dynamic string — a span name, a log or completion message, a text attribute —
lives once in the store's arena, and an intern ordinal names it. A long-lived store writes unbounded distinct text, so
the arena cannot be append-only forever: once it passes half its ceiling (`ARENA_RECLAIM_BYTES`), the next `reset` or
`retain_open` rebuilds it from the text the kept rows still name — nothing after a reset, the open spans after a retain
— renumbers those rows' message and text-attribute cells in place, and moves the store's text epoch
(`thread_span_buffer_text_epoch` on both ABIs). An ordinal is valid within the epoch that issued it. A binding that
caches ordinals reads the epoch after each reset and drops its cache when it moved; below the threshold the epoch never
moves, so a warm cache survives every window. **Rejected:** a per-string refcount (the arena exists to avoid per-string
bookkeeping); dropping the cache on every reset (one crossing per distinct string per window, for a renumbering that
happens only when the arena is large).

**Rejected:** a per-lane bespoke writer interface (every provider shares the store's semantics; a second interface would
fork `ThreadBufferStrategy` per provider); a batched JS-side row queue (a JS-owned row index plus double-buffering, for
nothing the per-call floor does not already buy); and attribute values as per-call ABI writes (the 10.8 ns proxy trap or
a 2.7 ns call per value, against a 0.4 ns store, to move a value into memory JavaScript can already address).

## 2. The registration inversion

**Host-first adoption by conformance, in `packages/lmao/src/lib/span-buffer/aot/{abi.ts,v1.ts}`.** The realm-global slot
`Symbol.for('@smoothbricks/lmao/span-buffer/aot/v1')` stays the single ABI point that compiler-generated code reads
(`packages/lmao-ttsc/plugin/driver/spanbuffer_aot.go`). Rules, decided once at realm setup:

- empty slot → `v1` installs lmao's frozen default (non-enumerable, non-configurable, non-writable);
- occupant conforming to `SpanBufferAotRuntime` (`abi.ts`) → `v1` adopts it and installs nothing — generated writers
  read the slot, so installing nothing **is** the inversion;
- non-conforming occupant → `TypeError('Conflicting LMAO SpanBuffer AOT runtime registrations')`.

Single-writer rule: whichever registration wins defines the slot non-configurable/non-writable, so a second host's
`defineProperty` throws **in the loser's stack** — the misconfigured deploy, not the innocent trace call. Registration
conflict is an _invariant_ (one realm, one ABI; a broken realm must fail at setup) and therefore throws; per-row
refusals remain operational _values_ (bare 0 / `false` / a refusal code the host counts). Conformance is
member-callability, not identity and not Typia: function signatures are not runtime-checkable, so the structural guard
checks presence/callability and the exported `SpanBufferAotRuntime` type pins signatures at the host's compile time.
lmao names no consumer anywhere in this path: the host imports lmao's published symbol, never the reverse.

**Rejected:** identity guarding (it made host substitution impossible by construction) and a registration _function_
exported from lmao (a second way to do the same thing; the slot plus evaluation order is already the complete protocol).

## 3. Span start

**Span start lives with the buffer allocator of each lane; the compiler never emits it.** ttsc's only span-adjacent
output is the generated `span_id` getter (`packages/lmao-ttsc/plugin/driver/spanbuffer_aot.go`).

- **Browser / Node / react-native (JS heap):** `writeSpanStart` at buffer creation through the class-carried lifecycle
  writers (`buffer._appenders.writeSpanStart`, installed once per generated buffer class prototype by
  `src/lib/lifecycleAppenders.ts`), rows 0/1 pre-armed in TypedArrays (`traceRoot.node.ts`, `traceRoot.es.ts`).
- **WASM-core lanes:** span start happens _inside the allocation export_ — `createAndStartSpan` pre-arms rows 0/1 in one
  WASM call (`src/lib/wasm/wasmSpanBuffer.ts` sets `_spanStartedAtAllocation`), and the primitive consumes the marker
  (`wasmTraceRoot.ts`, `consumeSpanStartedAtAllocation`).
- **Thread lane (every provider):** `openSpan*` on the binding both allocates the row pair and stamps it (`lmao-core`'s
  `ThreadSpanBuffer::open_span` reserves the completion row immediately). The allocation-time handshake generalizes: the
  allocator pre-arms the lifecycle rows, and the binding's open _is_ the allocation.

## 4. Views are not pointers

**Every thread-lane provider hands JavaScript views of the store's attribute cells; none hands it a pointer, a row
store, or a lifecycle it could forge.** A view is a bounds-checked TypedArray over one block's cells. What a writer can
reach through it is exactly the attribute values of rows in that store — never timestamps, entry types, span ids,
parentage, trace ids or messages, which live in columns only the store's lifecycle calls write. So the authority a view
grants is the authority `setAttribute` already granted, with the call removed.

An embedding host that runs code it does not trust gives each principal its **own** row store: the host's engine rows
and each realm's rows live in different stores, so no view reaches a row its holder did not open. The host stamps
identity, parentage and time itself (§5), validates nothing it does not have to — decoding is total — and converts each
store on its own thread, at a point where the realm is not running, so a view never races the converter. Views live
exactly as long as the realm: the host detaches every buffer it handed out before it frees the store behind it.

**Rejected:** a per-span pointer into linear memory as the public lane (`writeSpanStartPtr` survives only as a low-level
test surface on `WasmTraceRoot`); one store shared by every principal of a process, which would let a view reach rows it
did not write; and denying views altogether, which buys no authority the per-principal store does not already provide
and pays a call or a trap per value for it.

## 5. Timestamp ownership

**Per lane, no runtime branch anywhere:**

- **Browser / Node / react-native:** JS owns the clock. Boundaries read fresh (`boundaryTimestamp`,
  `traceRoot.node.ts`); log rows ride the `LOG_STAMP_REFRESH = 16` cache (`src/lib/coarseClock.ts`, bounded to a quarter
  of the 64-row capacity; durations never coarsen because span start/end always read fresh).
- **Plain Bun and Wasm thread lanes:** JS owns the clock and passes `timestamp: bigint` on each lifecycle call — the
  same coarse cache applies on the view (`src/lib/wasm/threadSpanView.ts` `_stampCache`/`_stampReads`). The measured JS
  clock + bigint cost (31.31 ns/row decomposition) is exactly what the cache amortizes.
- **Embedding host:** the host stamps every row inside the lifecycle call from its own clock. Its binding ignores the
  timestamp argument, which is how the seam inverts ownership without a branch: _who stamps_ is a property of the
  provider selected at registration, and no call site tests which world it is in. A native coarse clock does not satisfy
  an exact-stamp requirement; removing it is gated on quiescent ABBA arms on the deployment CPU, never on a Darwin
  result.

## 6. ttsc output

**One compiler output serves all five lanes; there is no second artifact.** The emitted call shapes are lane-neutral:

- Static templates lower to `_infoTemplate(denseIndex)`-family calls
  (`packages/lmao/src/lib/codegen/spanLoggerGenerator.ts`); on JS-heap lanes they store a `u16` local id, on the thread
  lane the id routes to `appendLogStatic` as a one-based `VocabularyId` (0 in a packed header means "this row's message
  is dynamic"). A provider whose store holds no copy of the process vocabulary resolves the id with
  `threadVocabularyText` and interns the text once.
- `loginline.go`'s emitted `$$l._writeIndex = $$i;` and the direct message/line stores are writes against the _buffer
  the slot runtime materialized_. On JS-heap lanes those are real TypedArray/array stores; on the thread lane the
  message and line lanes forward to the binding, because their index is the span-local write index rather than the
  store's row, while schema attributes are written by the generated writer methods, which store straight into the cells.

The emitted call shape is fixed at compile time and the implementation behind it at registration time, so a call site is
monomorphic on whichever runtime the realm registered. **Rejected:** two artifacts behind one specifier selected at
build target — it duplicates every generated writer and moves lane selection into the build graph, whose export-map
conditions cannot tell two runtimes that both answer to `bun` apart.

## 7. react-native

**`./react-native` entrypoint re-exporting the pure-TypedArray ES lane.** No WASM (Hermes executes wasm nowhere fast;
the wasm lane's value is shared linear memory with a native drain, which RN lacks), no JSI/TurboModule (a native module
would re-open the per-row boundary at N calls/row with no engine to amortize it), no `node:*` imports, no bun preloads.
Clock: `performance.now()` exists on Hermes; entropy: `crypto.getRandomValues` is bound once at module load and a host
without it fails loudly at import (`src/lib/traceId.ts`) — RN apps below the Hermes version that ships WebCrypto install
a provider before importing lmao; a silent `Math.random` fallback stays rejected.

## 8. Providers

**One lane (the native thread row store), several providers, chosen at construction — `createThreadBufferStrategy()`
(`@smoothbricks/lmao/wasm`) for Wasm, `ThreadBufferStrategy.fromProvider(provider)` for any other.** A
`ThreadSpanBufferProvider` creates a binding per schema and owns the lane's JavaScript Arrow conversion; the strategy
never asks which provider it holds, and never imports one: the allocator.wasm loader names `node:` modules, so a bundle
built over another provider must not reach it through the strategy.

- **Wasm** (`createThreadSpanBufferRuntime`): `allocator.wasm` slots, strings encoded into a scratch page, cells viewed
  in linear memory at the offset `thread_span_buffer_attribute_cells` exports. A `memory.grow` detaches views but never
  moves cells; the lane re-derives a detached view on its next store.
- **Plain Bun** (`src/bun/threadSpanBufferFfi.ts`): `bun:ffi` over `lmao-core`'s `thread_ffi` symbols
  (`lmao-ffi-dylib`), cells aliased with `toArrayBuffer` rather than copied.
- **Embedding host:** the host implements the binding over its own row stores, one per principal (§4), stamps rows
  itself (§5), and converts natively; its provider refuses JavaScript conversion because its rows are never read back
  into JS.

Unpatched `bun:ffi` (1.15–2.10 ns no-op, measured here) and a host's typed JSC functions (1.21–1.62 ns) reach the same
`JSFFIFunction` fast path; what differs between providers is authority, not speed.

## 9. Enforcement

**The writer is held by the existing machinery, plus one twin rule.**

- **Tier-0:** the workspace deny set applies to `lmao-core` and `lmao-wasm` via `[lints] workspace = true`; the
  disallowed-types list (no `std::sync::Mutex`, no default-hasher maps on row paths) binds the row store.
- **Allocation census:** `lmao-core/tests/thread_buffer_alloc.rs` runs a counting global allocator: open, end, log and
  tag allocate nothing after warmup, and neither does a whole write–flush–retain cycle of a long-lived store, overflow
  block included.
- **Tier-1b row:**

| Rule       | Input                                      | Pass condition                                                                                                                                               | Start tier  |
| ---------- | ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------- |
| `ABI-TWIN` | `thread_ffi.rs`, wasm adapter, JS bindings | the native and wasm thread-buffer surfaces expose the same entry set with the same packed-return/bare-zero contract; a member present on one side only fails | warn → gate |

## 10. Flushing a long-lived store

A process that traces forever flushes while spans are still open. `ThreadSpanBuffer::flush_rows` selects every written
row except the reserved completion row of a span that is still open, and `lmao-arrow`'s `convert_thread_span_rows`
converts exactly that selection by reference; `retain_open` then moves each open span's start and completion rows to the
front and releases everything else. Every finished row therefore reaches exactly one flush; an open span's start row
reaches every flush while it stays open — the last copy carrying its final attribute values — and its completion row the
first flush after it ends. A reader takes the latest start row of a span. Each span's `SourceMetadata` rides its record,
so every row it emits carries its provenance.
