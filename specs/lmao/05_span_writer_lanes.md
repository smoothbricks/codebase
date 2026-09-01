# Span/Log Writer Lanes <a id="smoo/lmao!n/span-writer-lanes"></a>

One writer architecture serves five execution lanes: Browser, Node.js, react-native, plain Bun, and containium-bun. The
architecture is: **one shared seam (`TraceRootFactory` + `ThreadSpanBufferBinding`), one host-first registration
inversion (`span-buffer/aot/v1`), one native row store (`lmao-core`), and per-lane providers selected at entrypoint or
registration time — never at call time.** AxE paths below are cited relative to the AxE repo
(`packages/containium-bun/…`, `specs/…`); everything else is this repo.

Measured floors quoted from `AxE specs/axe/30-lmao-integration.md:71-75,84-104` (ABBA, 96 blocks × 10,000 calls,
positions 1↔4/2↔3, three-repetition floors, Apple M5 Max arm64-darwin; release target is x64 Linux —
`tooling/direnv/devenv.smoo.nix` declares `x86_64-unknown-linux-gnu` as the only non-host Rust target, and floors do not
transfer between the two):

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

Independently corroborated on the same machine class with a fresh `/tmp` probe (unpatched Bun 1.4.0, same ABBA
methodology, warmup added): `bun:ffi` no-op floor 1.15–2.10 ns, 4×u32 store floor 3.07–3.45 ns — plain Bun's `bun:ffi`
reaches the same `JSFFIFunction` fast path the landed spec measured for containium's private typed functions.
Thread-lane JS-side numbers from the landed lineage: 32-row span 155–163 ns/row after stamp coarsening (was 179–190);
`phase2-warm` js-heap 11.33 µs vs wasm-thread 34.04 µs (~3.0×); JSC proxy set-by-val 10.8 ns vs sparse-accessor
`putByIndex` 18–22 ns/store (commit provenance: `2038b65a`, `1b6430d7` bodies, landed here as part of the thread-buffer
lineage).

## 1. The seam

**Decision: two layers, both already landed, and no third.** The logical seam every lane implements is
`TraceRootFactory` (`packages/lmao/src/lib/traceRoot.ts:92`) with its four monomorphic primitives (`_timestampNow`,
`_appendLogEntry`, `_writeSpanStart`, `_writeSpanEnd`, `traceRoot.ts:139-142`); the physical seam for row storage is the
handle-based `ThreadSpanBufferBinding` (`packages/lmao/src/lib/wasm/threadSpanBuffer.ts:117-166`):
`openSpan{,Static,Dynamic}`, `end`, `appendLog{,Static,Dynamic}`, `writeAttr`, `writeTag`, `setScope`, `intern`.

What crosses, per call: integers (span id, entry type, ordinal, vocabulary id, line), one `bigint` timestamp on the
JS-clock lanes, and — only on the cold dynamic paths — `ptr,len` UTF-8 runs. What never crosses: strings on warm paths
(interned once to a `u32` ordinal, `lmao-core/src/thread_buffer.rs` `intern`; hit rate measured 4,507,371/129 on the
benchmark workload), row indices (the packed `(span_id << 32) | row` return is an opaque receipt whose failure form is
bare 0 — `lmao-core/tests/thread_ffi_oracles.rs`), and JS-heap objects. Against `SIG-CORE` (AxE
`specs/containium/91-perf-enforcement.md:176`): parameters are borrows (`&str`, `*const u8 + len`), returns are
`u64`/`u8` scalars, no `String`/`Vec`/`Box<dyn>` crosses, and the Rust side's out-state is the arena-shaped row store
sized at startup. Against the floors: a warm static row is one exact-arity call carrying five integers — the 3.82–4.99
ns shape — not the 21–22 ns string shape.

**Rejected:** a per-lane bespoke writer interface (the wasm adapter and native `thread_ffi` already share semantics; a
second interface would fork `ThreadBufferStrategy` per provider), and a batched JS-side row queue (amortizes the
boundary but re-introduces a JS-owned row index and double-buffering; the measured per-call floor is already below the
JS-heap row cost, so batching buys nothing the census can see).

## 2. The registration inversion

**Decision: host-first adoption by conformance, landed in `packages/lmao/src/lib/span-buffer/aot/{abi.ts,v1.ts}`.** The
realm-global slot `Symbol.for('@smoothbricks/lmao/span-buffer/aot/v1')` stays the single ABI point that
compiler-generated code reads (`packages/lmao-ttsc/plugin/driver/spanbuffer_aot.go:15-16,26-53`). Rules, decided once at
realm setup:

- empty slot → `v1` installs lmao's frozen default (non-enumerable, non-configurable, non-writable);
- occupant conforming to `SpanBufferAotRuntime` (`abi.ts`) → `v1` adopts it and installs nothing — generated writers
  read the slot, so installing nothing **is** the inversion;
- non-conforming occupant → `TypeError('Conflicting LMAO SpanBuffer AOT runtime registrations')`.

Single-writer rule: whichever registration wins defines the slot non-configurable/non-writable, so a second host's
`defineProperty` throws **in the loser's stack** — the misconfigured deploy, not the innocent trace call.
Result-vs-throw reconciliation: registration conflict is an _invariant_ (one realm, one ABI; a broken realm must fail at
setup) and therefore throws; per-row refusals remain operational _values_ (bare 0 / `false` / `Refusal` codes — AxE
`crates/containium-realm-spans/src/abi.rs:122-131`). Conformance is member-callability, not identity and not Typia:
function signatures are not runtime-checkable, so the structural guard checks presence/callability and the exported
`SpanBufferAotRuntime` type pins signatures at the host's compile time. lmao names no consumer anywhere in this path:
the host imports lmao's published symbol, never the reverse.

**Rejected:** identity guarding (the previous form; it made host substitution impossible by construction) and a
registration _function_ exported from lmao (a second way to do the same thing; the slot plus evaluation order is already
the complete protocol, and Bun preloads / containium's pre-authored host scripts already guarantee order).

## 3. Span start

**Decision: span start lives with the buffer allocator of each lane; the compiler never emits it.** Verified: ttsc's
only span-adjacent output is the generated `span_id` getter (`packages/lmao-ttsc/plugin/driver/spanbuffer_aot.go:392`).

- **Browser / Node / react-native (JS heap):** `writeSpanStart` at buffer creation through the class-carried lifecycle
  writers (`buffer._appenders.writeSpanStart`, installed once per generated buffer class prototype by
  `src/lib/lifecycleAppenders.ts`), rows 0/1 pre-armed in TypedArrays (`traceRoot.node.ts:90-100`,
  `traceRoot.es.ts:92-102`).
- **WASM-core lanes:** span start happens _inside the allocation export_ — `createAndStartSpan` pre-arms rows 0/1 in one
  WASM call (`src/lib/wasm/wasmSpanBuffer.ts:876-904` sets `_spanStartedAtAllocation`), and the primitive consumes the
  marker (`wasmTraceRoot.ts:117-122`, `consumeSpanStartedAtAllocation`, `traceRoot.ts:31-35`).
- **Thread lane (plain Bun FFI and the wasm thread binding):** `openSpan*` on the binding both allocates the row pair
  and stamps it (`lmao-core/src/thread_buffer.rs` `open_span` reserves the completion row immediately). The
  allocation-time handshake **generalizes**: it is the same "the allocator pre-arms the lifecycle rows" contract with
  the marker made implicit, because the binding's open _is_ the allocation.
- **containium:** authored `open` crosses the realm binding into `containium-realm-spans/src/thread.rs:538-575`
  (`open`), which parents on the engine-installed wake-up root (`parent_span == 0` semantics, `thread.rs:533`); the
  engine installs that root per Decide/ExecuteOps phase (`containium-exec/src/native_engine.rs`, `AuthoredSpanRoot` +
  `PhaseSpan`). Engine spans open in Rust — "a span opens where its control flow lives" (AxE
  `specs/axe/30-lmao-integration.md:112-116`).

## 4. The pointer contradiction

**Decision: the pointer entry is not part of the shared seam; containium does not relax; the per-span WASM pointer lane
is the loser and is already retired from the public surface.** `wasmTraceRoot.ts:288`'s
`writeSpanStartPtr(systemPtr, identityPtr)` passes raw linear-memory offsets from JS. That is legal _only_ because WASM
linear memory is the sandbox: a wasm "pointer" is an index into memory the JS realm already owns, so no authority
crosses. The containium prohibition (AxE `specs/axe/30-lmao-integration.md:137-145`: no pointer, row index, persistent
pin, or timestamp from JS) is about _process_ memory behind a _privilege_ boundary — a JS-held native pointer is
authority forgery, which `containium-realm-spans` exists to deny. The two lanes therefore share the primitive signatures
and the row-store semantics, not the addressing mode. The landed lineage already retired the public per-span wasm lane
(`refactor(lmao): retire public per-span wasm lane`); `writeSpanStartPtr` survives only as a low-level test surface on
`WasmTraceRoot`, and the thread lane's handles are opaque `u32` tokens minted by the store, never addresses
(`threadSpanBuffer.ts:17`).

## 5. Timestamp ownership

**Decision per lane, no runtime branch anywhere:**

- **Browser / Node / react-native:** JS owns the clock. Boundaries read fresh (`boundaryTimestamp`,
  `traceRoot.node.ts:43-50`); log rows ride the `LOG_STAMP_REFRESH = 16` cache (`src/lib/coarseClock.ts:40`, bounded to
  a quarter of the 64-row capacity; durations never coarsen because span start/end always read fresh,
  `coarseClock.ts:26-30`).
- **Plain Bun thread lane:** JS owns the clock and passes `timestamp: bigint` as an FFI argument
  (`threadSpanBufferBinding` signatures) — the same coarse cache applies on the view (`src/lib/wasm/threadSpanView.ts`
  `_stampCache`/`_stampReads`). This is correct for plain Bun because there is no engine-side writer to own a better
  clock, and the measured JS clock+bigint cost (31.31 ns/row decomposition, AxE
  `specs/axe/30-lmao-integration.md:84-86`) is exactly what the cache amortizes.
- **containium:** Rust stamps every row inside the write call; timestamp is _not_ an FFI argument on the realm binding
  (AxE `host/span-buffer-host.js` carries no time anywhere; `containium-trace/src/tracer.rs` owns the clock with its own
  `LOG_STAMP_REFRESH = 16` and `CoarseClock`). The landed gate stands: `CoarseClock` does not satisfy the exact-stamp
  requirement, and its removal is gated on quiescent x64 Linux ABBA arms (AxE `specs/axe/30-lmao-integration.md:87-110`)
  — a Darwin win is not evidence for the release path.

The seam inverts ownership without a branch because _who stamps_ is a property of the binding selected at registration:
JS-clock bindings have a timestamp parameter; the containium binding does not have one to pass. No call site tests which
world it is in.

## 6. ttsc output

**Decision: one compiler output serves all five lanes; there is no second artifact.** The emitted call shapes are
already lane-neutral:

- Static templates lower to `_infoTemplate(denseIndex)`-family calls
  (`packages/lmao/src/lib/codegen/spanLoggerGenerator.ts:247,468`); on JS-heap lanes they store a `u16` local id, on the
  thread lane the id routes to `appendLogStatic` as a one-based `VocabularyId` (0 in a packed header means "this row's
  message is dynamic"; readers decode `encodedDenseIndex − 1`) — an integer register crossing, nothing to intern or
  encode.
- `loginline.go:20`'s emitted `$$l._writeIndex = $$i;` and the direct column stores are writes against the _buffer the
  slot runtime materialized_. On JS-heap lanes those are real TypedArray/array stores; on the thread lane they land on
  lane proxies that forward to the binding (`threadSpanView.ts:111` `laneProxy`; measured 10.8 ns/store vs the 18–22 ns
  sparse-accessor alternative). The row index is _buffer-local JS state of a JS-owned buffer class_, so it does not
  violate containium's no-row-index rule: in containium, authored code's compiled writers come from the host's
  registered AOT runtime (§2), whose materialized classes write through the realm binding and expose no `_writeIndex`
  backed by native rows.

This preserves the JSC exact-arity monomorphic fast path because the emitted call shape is fixed at compile time and the
_implementation_ behind it is fixed at registration time — a call site is monomorphic on whichever runtime the realm
registered. **Rejected:** two artifacts behind one specifier selected at build target — it duplicates every generated
writer, moves lane selection into the build graph where the export-map conditions cannot express containium-vs-plain-Bun
(both are `bun`), and buys nothing: the single artifact's calls are already indirected through the slot exactly once, at
class-materialization time, not per call.

## 7. react-native

**Decision: `./react-native` entrypoint re-exporting the pure-TypedArray ES lane.** No WASM (Hermes executes wasm
nowhere fast; the wasm lane's value is shared linear memory with a native drain, which RN lacks), no JSI/TurboModule (a
native module would re-open the per-row boundary at N calls/row with none of containium's engine to amortize it — a
design smell per the TigerStyle addendum, AxE `specs/containium/91-perf-enforcement.md:160-161`), no `node:*` imports,
no bun preloads. Clock: `performance.now()` exists on Hermes; entropy: `crypto.getRandomValues` is bound once at module
load and a host without it fails loudly at import (`src/lib/traceId.ts:114-130`) — RN apps below the Hermes version that
ships WebCrypto must install a provider before importing lmao; silent `Math.random` fallback stays rejected for the
reason recorded at the bind site. Delivered on branch `deliver/react-native-entrypoint` (implementation in flight at
time of writing; the export follows `./es`'s condition structure, `packages/lmao/package.json:40-45`).

## 8. Plain Bun vs containium-bun

**Decision: one lane (the native thread row store), two providers (two bindings), and the providers are not
interchangeable at runtime — they are different registrations.** Plain Bun binds `ThreadSpanBufferBinding` over
`bun:ffi` against `lmao-core`'s `thread_ffi` surface (`lmao-core/src/thread_ffi.rs`, native-only `no_mangle`; delivered
on branch `deliver/bun-ffi-lane` as a cdylib + `bun:ffi` binding). containium binds the same row-store semantics through
its private exact-arity `JSFFIFunction`s captured before authored code evaluates (AxE `host/span-buffer-host.js:76-79`,
`crates/containium-realm-spans/src/abi.rs:397-405` `ENTRIES: [&str; 7]`). The floors justify calling them one lane:
unpatched `bun:ffi` (1.15–2.10 ns no-op, measured here) and containium's typed path (1.21–1.62 ns, landed spec) are the
same fast path; what differs is _authority_ — containium's functions are never published, validate realm
capability/generation/kind per call, and refuse rather than trap. The landed coherence note stands unchanged: static
`bun:ffi` imports are graph-refused inside containium realms while `globalThis.Bun.FFI` exists in Op realms — the
plain-Bun provider is for _plain_ Bun processes, and containium realms get the host binding or nothing. That is not a
runtime branch; it is which preload/host script ran.

## 9. Enforcement

**Decision: the writer is held by the existing machinery; one new Tier-1b row.**

- **Tier-0** (AxE `specs/containium/91-perf-enforcement.md:27-77`): the workspace deny set applies to `lmao-core`,
  `lmao-wasm`, `containium-trace`, `containium-realm-spans` via `[lints] workspace = true`; the disallowed-types list
  (no `std::sync::Mutex`, no default-hasher maps on row paths) already binds the row store — `thread_buffer`'s interner
  is the key-as-ordinal shape, not a hasher workaround.
- **Tier-1 shapes:** the writer must not trip `SHAPE-FMTROW` (messages cross as ordinals/vocabulary ids, never
  `format!`), `SHAPE-CAP0`/`SHAPE-SEED` (blocks are pushed whole at fixed capacity, `thread_buffer.rs` `ensure_rows`),
  `SHAPE-RMWROW` (span-id allocation is thread-local, no shared atomics per row), `SHAPE-SPILL` (the arena is the Arena
  shape: sized, `clear()`ed, never grown mid-lane).
- **Tier-2 census, per lane:** containium realm writes: `calls ≤ 2` per per-element symbol (`span_log` → interned append
  is one call chain), `grow = 0`, allocation census 0 after warmup with the itemized residue being first-seen interns
  (owner: the arena; AxE `crates/containium-realm-spans/tests/warm_writes_do_not_allocate.rs` is the counting-allocator
  control). Plain-Bun thread_ffi: same symbols, same counts, measured over the cdylib. JS-heap lanes are outside the
  Rust census; their gate is the ABBA harness floors (`packages/lmao/benchmarks/abba-lanes.ts`, floors not means).
- **New Tier-1b row**, in the work-order format of `91-perf-enforcement.md:170-183`:

| Rule       | Input                                      | Pass condition                                                                                                                                               | Start tier  |
| ---------- | ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------- |
| `ABI-TWIN` | `thread_ffi.rs`, wasm adapter, JS bindings | the native and wasm thread-buffer surfaces expose the same entry set with the same packed-return/bare-zero contract; a member present on one side only fails | warn → gate |

- The `git_sha` provenance column is a declared build input on the Rust side (landed in AxE `65b9b2b7bc`:
  `containium-trace/build.rs` with `rerun-if-changed` on the git head/ref files), and TS artifact identity rides realm
  registration once per realm (`register_realm` widened with `git_sha`/`package_name`/`package_file` ptr+len;
  `js-hash:<hex>` content identity, `<unversioned>` when absent) — per-row file bytes stay rejected for the reason
  `thread.rs:90-97` records.

## 10. Ordering and prerequisites

Dependency-ordered state of the world; items landed by this unit are marked **[landed]**, everything else names its
blocker:

1. **[landed]** String-arena substrate (`arena.rs`, handle columns) — smoothbricks `7eae796a` lineage.
2. **[landed]** Thread-buffer cutover (binding, `appendLogStatic`, one-based vocabulary ids, intern-to-ordinal, coarse
   thread-lane stamps, ABBA harness) — 23 commits rebased onto the arena, plus two reconciliation commits.
3. **[landed]** AOT registration inversion — `90274fc9`. Unblocks: any host-supplied compiled-writer runtime.
4. **[landed]** AxE authored-span root (`AuthoredSpanRoot`/`PhaseSpan`, `containium-exec` → `containium-realm-spans`
   dependency) — AxE `b876b4512f`. Unblocks: every authored `open` that previously refused `NoTrace` (AxE
   `thread.rs:551`). Live acceptance: authored counters non-zero on a real session (known-good shape 104 total / 63
   authored vs 41/0 control), decoded off the native Arrow IPC lane (AxE `containium-trace/src/sink.rs:13-15`), authored
   rows keyed on `package_file` = `js-hash:…`/`<unversioned>`.
5. **In flight:** plain-Bun `bun:ffi` provider (`deliver/bun-ffi-lane`) and `./react-native` entrypoint
   (`deliver/react-native-entrypoint`) — both additive; block nothing else.
6. **Unlanded, deliberately:** the `ttsc-template-gate` sheds (compile-time refusal hardening for uninlinable logs; 14 +
   3 commits with unresolved `UU` conflicts in `lmao-core/src/{context.rs,thread_buffer.rs}`, preserved at
   `refs/keep/shed-ttsc-template-gate{,-pair}-*`). They harden the compiler gate; nothing in the five lanes depends on
   them. Rebase over the landed arena+thread world before resolving.
7. **Unlanded, pending evaluation:** the `string-arena` shed's three commits (byte-arena handles beyond `Arc<str>`,
   JS-convert dictionary-key remap — verified JS-path-only, `permHits 0` on the native lane — and planted reds), at
   `refs/keep/shed-string-arena-*`. They deepen the arena; the seam does not change shape when they land.
8. **Containium provider wiring** (the host-registered AOT runtime materializing realm-binding-backed writer classes):
   now unblocked by 3 + 4 + the `register_realm` provenance widening; this is the remaining containium work, and its
   acceptance is item 4's live counters moving from dynamic-only to template rows.
9. **The exact-clock gate** (removal of `LOG_STAMP_REFRESH` on the containium lane): blocked on a dedicated x64 Linux
   host, per the landed platform-gate reasoning — unchanged by this document.

`[INFERENCE]` markers: Hermes wasm/JIT posture in §7 (documented platform behavior, not measured here); the §6 claim
that slot-indirected class materialization preserves DFG/FTL monomorphism at generated call sites is grounded in the
landed `CallFFI` description (AxE `specs/axe/30-lmao-integration.md:66-75`) but has not been re-profiled post-inversion;
and the §9 census counts for the plain-Bun cdylib are the expected values of the same symbols already gated in-tree, not
yet measured over the cdylib artifact.
