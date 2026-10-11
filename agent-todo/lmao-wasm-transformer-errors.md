# lmao wasm, transformer, and errors: gaps between the specs and the code

This file covers four lmao specs: `specs/lmao/01q_wasm_memory_architecture.md`,
`specs/lmao/01r_transient_errors_and_retry.md`, `specs/lmao/01k_tree_walker_and_arrow_conversion.md`, and
`specs/lmao/01o_typescript_transformer.md`. Each item cites the current spec lines and the current code lines. The items
are independent unless stated. Items under "Open decisions" need a choice before anyone implements them. Three of the
four Deferred Work items that the 01q spec removed (WasmTracer, string pooling, hot-path inlining) are briefed under
"WASM memory follow-ups" below. The fourth, per-capacity freelists, was dropped, as that section explains.

## Blocked `resource` reason variant

- Spec: `specs/lmao/01r_transient_errors_and_retry.md:38-39`, `:220`, `:227`, `:237-238`: the `Blocked` reason union has
  a `resource` member with `resourceType` and `resourceId`. `Blocked.resource(...)` builds it. The message reads
  `${resourceType}/${resourceId}`.
- Code: `packages/lmao/src/lib/errors/Blocked.ts:61-64` (union has `service`, `ended`, `index` only), `:69-78`
  (`getBlockedReasonName` has no `resource` case), `:128-140` (statics for `service`, `ended`, `index` only).
  `grep resource packages/lmao/src/lib/errors` → no hits.
- Work: add the `resource` member to the reason union. Add `static resource(resourceType, resourceId, config?)`. Add the
  `resource` case to `getBlockedReasonName`, so the message reads `resourceType/resourceId`. Add tests.
- Acceptance: `Blocked.resource('lease', 'L1').reason` deep-equals
  `{ type: 'resource', resourceType: 'lease', resourceId: 'L1' }`. Its `.message` contains `lease/L1`.

## UTF-8 surrogate byte count

- Spec: `specs/lmao/01k_tree_walker_and_arrow_conversion.md:152-165` (`utf8ByteLength` surrogate branch) and `:143-144`:
  a surrogate pair counts as 4 bytes. A lone surrogate encodes as U+FFFD, which is 3 bytes.
- Code: `packages/arrow-builder/src/lib/arrow/utf8.ts:25-39`. The surrogate branch at `:32-36` adds 4 bytes for any
  surrogate. It does not check for a following low surrogate. A lone surrogate is counted as 4.
- Work: when a high surrogate is followed by a low surrogate, count 4 and consume both code units. For any other lone
  surrogate, count 3.
- Acceptance: `"\uD800\u20AC"` is 6 bytes, which matches `TextEncoder`. `"\uDC00"` is 3. `"\uD83D\uDE00"` is 4. A slice
  that splits a surrogate pair round-trips through `TextDecoder`.

## Encode truncation check in dictionary finalize

- Spec: `specs/lmao/01k_tree_walker_and_arrow_conversion.md:263-269`: finalize calls `encodeInto` and advances `offset`
  by `result.written`. The check at `:264-268` throws `DictionaryEncodeInvariantError` when the encoding is short.
- Code: `packages/arrow-builder/src/lib/arrow/dictionary.ts:216` (`encodeInto(...).written ?? 0`, with no `read` check)
  and `:217` (`offset += written`). `packages/arrow-builder/src/lib/arrow/utf8.ts:51-54` (`encodeInto` returns
  `written ?? 0`). `grep DictionaryEncodeInvariantError packages/arrow-builder packages/lmao/src` → no hits.
- Work: after `encodeInto`, check that `read` equals `str.length` and that `written` equals the byte count from
  `utf8ByteLength`. On mismatch, throw `DictionaryEncodeInvariantError`, a class that does not exist yet. Never return a
  partial offsets table.
- Acceptance: a test double whose `encodeInto` returns `{ read, written: 0 }` makes finalize throw
  `DictionaryEncodeInvariantError`. No offsets table is returned.

## Re-rooted span-name registration

- Spec: `specs/lmao/01o_typescript_transformer.md:307-308`: "A re-rooted call follows the §2 span-name rule".
- Code: `packages/lmao-ttsc/plugin/driver/optimization.go:887-904`. Registration needs a `recv` identifier or `this`. A
  re-rooted call has `recv == nil`, so its literal name is never registered (literal registration is at `:900-903`).
  `packages/lmao-ttsc/plugin/driver/destructured.go:293` rewrites the callee to `<generatedContextName>.span`.
  `trySpanRewrite` is at `packages/lmao-ttsc/plugin/driver/lmao.go:395`.
- Work: register a re-rooted literal name through the `__ctx` receiver, as a direct call does. The rewritten call then
  passes `$$lmaoVocabulary[n]` as its name operand.
- Acceptance: a destructured `span('x', op)` emits `$$lmaoVocabulary[n]` as the name operand. A Go golden test asserts
  it.
- Cost: interning re-rooted names adds them to the vocabulary catalog, which moves catalog ordinals, so it is not a free
  change. Update every test that pins an ordinal in the same commit.

## Structured field writers

- Spec: `specs/lmao/01o_typescript_transformer.md:548-549`: a structured log lowers to
  `log._infoStructured2($$lmaoVocabulary[0], 42, $$field0, $$field1);`. A template lowers to
  `log._infoTemplate($$lmaoVocabulary[1]).line(43);`.
- Code: `grep _infoStructured\|spanVocabulary packages/lmao/src` → no hits. `_infoTemplate` is declared at
  `packages/lmao/src/lib/codegen/spanLoggerGenerator.ts:247` and generated at `:468`. The comment at `:457` says lowered
  calls use `_infoTemplate` with direct setters. `{fieldName}` parsing is in
  `packages/lmao-ttsc/plugin/driver/structuredlog.go:21` (`parseStructuredTemplate`).
- Work: generate `_infoStructured<N>` beside `_infoTemplate` in `spanLoggerGenerator.ts`, where N is the field count.
  Have the driver call it with the vocabulary index and the field values.
- Acceptance: a structured log call emits `_infoStructured2(...)` in the transformed output. A Bun test reads the field
  values back from the buffer.

## Source-map verification pass

- Spec: `specs/lmao/01o_typescript_transformer.md:747` ("Source-map preservation is verified by a dedicated pass"),
  under the Source Maps heading at `:741`.
- Code: `grep -i "source.\?map" packages/lmao-ttsc/plugin` → no hits.
- Work: add a pass after the rewrite. It maps each emitted replacement node back to the line of the original call.
- Acceptance: a Bun or Go test asserts that a transformed `ctx.span` call maps to its original source line.

## Benchmark gate variants

- Spec: `specs/lmao/01o_typescript_transformer.md:614-617`: the gate runs `plugin-off` and the global-vocabulary
  variants in position-balanced, order-reversed runs. A checksum mismatch fails the gate (`:617`).
- Code: no Mitata scenario carries the label `plugin-off`. Similar labels appear only in tests and docs:
  `packages/lmao/src/lib/__tests__/eager-columns.test.ts:334`,
  `packages/lmao/src/lib/__tests__/message-buffer-families.test.ts:802-803` (`plugin-off-current`, `plugin-on-current`),
  and `packages/lmao/docs/transformer-optimization.md:282`.
- Work: add Mitata scenarios for both variants. Emit raw samples and a semantic checksum for each variant. Alternate the
  run order. Fail the run when checksums differ.
- Acceptance: the bench writes raw samples and semantic checksums for both variants. A checksum mismatch fails the run.

## 32-bit vocabulary ids; a collision is never fatal

- Spec: `specs/lmao/01o_typescript_transformer.md` "Vocabulary fragment and library-without-source contract":
  `idAlgorithm: 'sha256-32-v1'`; `ids[i]` is the first 32 bits of `SHA-256(kindTag || recordBytes)`; an id is a hash,
  not a unique key; registration probes the records filed under an id, reuses an equal record's dense index and appends
  any other; the transformer's whole-program catalog follows the same rule.
  `01j_module_context_and_spanlogger_generation.md` states the same id definition.
- Code: the 24-bit algorithm and the fatal collision live in the runtime registry and the transformer catalog
  (`packages/lmao/docs/transformer-optimization.md` already states the 32-bit design).
  - `packages/lmao/src/lib/vocabularyRegistry.ts:5-6` (`'sha256-24-v1'`, `MAX_VOCABULARY_ID = 0x00ff_ffff`), `:299`
    (rejects id 0 and ids above 24 bits), `:309` (24-bit derivation), `:312-318` (`LMAO_VOCABULARY_ID_COLLISION` within
    a fragment), `:371-377` (the same across fragments), `:386-394` (`entriesById` maps one id to one entry).
  - `packages/lmao-ttsc/plugin/driver/vocabulary.go:20` (`"sha256-24-v1"`), `:240-241` (`LMAO1008`, fatal for a record
    whose id is 0) and `:243-248` (`LMAO1009 vocabulary id collision`, fatal to the compilation).
  - Past the catalog, the transformer keys by id in three more places, so colliding records would silently share one
    operand: `vocabulary.go:341-357` (`resolveVocabularyIDs` resolves each call to an id), `lmao.go:145` and `:258-268`
    (`vocabularyOrdinals` maps an id to its fragment ordinal; `staticVocabularyOperand` also treats id 0 as "none"), and
    `optimization.go:246-262` (`localIDsByGlobalID` keys the Op-local message dictionary by id).
- Work: change both sides to `sha256-32-v1` with a 32-bit derivation, and accept every 32-bit value, zero included (with
  a 32-bit hash, rejecting zero is itself a rare fatal case). Make `entriesById` map an id to the list of entries filed
  under it; registration walks that list comparing `(kindTag, recordBytes)`, reuses an equal entry and appends
  otherwise. Drop `LMAO_VOCABULARY_ID_COLLISION`, `LMAO1008` and `LMAO1009`, and change the Go catalog's `byID` the same
  way. In the transformer, key the per-call resolution, the per-file ordinals and the Op-local dictionary by the record
  key (`vocabularyKey`), not by id (the Op-local dictionary goes away instead if the Op-local lane cutover below lands
  first), and give `staticVocabularyOperand` an explicit "no entry" instead of id 0. Keep the `contentHash` reuse check
  fatal. Bump nothing else: `schemaVersion` stays 1 and the record grammar is unchanged, but every emitted fragment and
  golden test changes ids.
- Acceptance: the existing collision fixture (`vocabulary_test.go:259-287`, `vocabularyRegistry.test.ts:305-309`)
  becomes a pair of records that share a 32-bit id, found by a deterministic search over a range wide enough to contain
  one (100,000 candidates finds a 32-bit collision only about 69 % of the time, so widen it and pin the found pair):
  both register in either order, each decodes to its own text, and re-registering either is idempotent. A Go catalog
  holding the two records compiles. Two colliding templates logged in one Op in one file each emit their own
  `$$lmaoVocabulary[n]` operand and resolve to their own text in the Arrow output.

## 32-bit vocabulary ids; a collision is never fatal

- Spec: `specs/lmao/01o_typescript_transformer.md` "Vocabulary fragment and library-without-source contract":
  `idAlgorithm: 'sha256-32-v1'`; `ids[i]` is the first 32 bits of `SHA-256(kindTag || recordBytes)`; an id is a hash,
  not a unique key; registration probes the records filed under an id, reuses an equal record's dense index and appends
  any other; the transformer's whole-program catalog follows the same rule.
  `01j_module_context_and_spanlogger_generation.md` states the same id definition.
- Code: the 24-bit algorithm and the fatal collision live in the runtime registry and the transformer catalog
  (`packages/lmao/docs/transformer-optimization.md` already states the 32-bit design).
  - `packages/lmao/src/lib/vocabularyRegistry.ts:5-6` (`'sha256-24-v1'`, `MAX_VOCABULARY_ID = 0x00ff_ffff`), `:299`
    (rejects id 0 and ids above 24 bits), `:309` (24-bit derivation), `:312-318` (`LMAO_VOCABULARY_ID_COLLISION` within
    a fragment), `:371-377` (the same across fragments), `:386-394` (`entriesById` maps one id to one entry).
  - `packages/lmao-ttsc/plugin/driver/vocabulary.go:20` (`"sha256-24-v1"`), `:240-241` (`LMAO1008`, fatal for a record
    whose id is 0) and `:243-248` (`LMAO1009 vocabulary id collision`, fatal to the compilation).
  - Past the catalog, the transformer keys by id in three more places, so colliding records would silently share one
    operand: `vocabulary.go:341-357` (`resolveVocabularyIDs` resolves each call to an id), `lmao.go:145` and `:258-268`
    (`vocabularyOrdinals` maps an id to its fragment ordinal; `staticVocabularyOperand` also treats id 0 as "none"), and
    `optimization.go:246-262` (`localIDsByGlobalID` keys the Op-local message dictionary by id).
- Work: change both sides to `sha256-32-v1` with a 32-bit derivation, and accept every 32-bit value, zero included (with
  a 32-bit hash, rejecting zero is itself a rare fatal case). Make `entriesById` map an id to the list of entries filed
  under it; registration walks that list comparing `(kindTag, recordBytes)`, reuses an equal entry and appends
  otherwise. Drop `LMAO_VOCABULARY_ID_COLLISION`, `LMAO1008` and `LMAO1009`, and change the Go catalog's `byID` the same
  way. In the transformer, key the per-call resolution, the per-file ordinals and the Op-local dictionary by the record
  key (`vocabularyKey`), not by id, and give `staticVocabularyOperand` an explicit "no entry" instead of id 0. Keep the
  `contentHash` reuse check fatal. Bump nothing else: `schemaVersion` stays 1 and the record grammar is unchanged, but
  every emitted fragment and golden test changes ids.
- Acceptance: the existing collision fixture (`vocabulary_test.go:259-287`, `vocabularyRegistry.test.ts:305-309`)
  becomes a pair of records that share a 32-bit id, found by a deterministic search over a range wide enough to contain
  one (100,000 candidates finds a 32-bit collision only about 69 % of the time, so widen it and pin the found pair):
  both register in either order, each decodes to its own text, and re-registering either is idempotent. A Go catalog
  holding the two records compiles. Two colliding templates logged in one Op in one file each emit their own
  `$$lmaoVocabulary[n]` operand and resolve to their own text in the Arrow output.

## WASM memory follow-ups

These were listed as open work in `specs/lmao/01q_wasm_memory_architecture.md` and now live here. Each is an
optimization, not a correctness requirement. Per-capacity freelists are not listed: the Resolved Decision at
`01q_wasm_memory_architecture.md:889` discards orphaned blocks on a capacity change instead.

- **WasmTracer.** A tracer that reads directly from WASM memory. Code: `packages/lmao/src/lib/wasm/__tests__/` holds a
  disabled `wasmTracer.test.ts.disabled` that imports a `wasmTracer.ts` which does not exist. Work: implement the tracer
  and enable the test, or delete the disabled test. Acceptance: the test runs and passes, or no reference remains.
- **String array pooling.** Pool `string[]` arrays to cut GC pressure in long-running processes. Code: no pool exists in
  `packages/lmao/src`. Acceptance: a long-running Mitata scenario shows lower GC time with the pool and an unchanged
  semantic checksum.
- **Hot-path inlining.** Inline the WASM exports further and elide bounds checks through capacity constraints.
  Acceptance: the warmed-write Mitata scenario improves with an unchanged semantic checksum.

## Decisions

### Warmed-write zero-allocation claim

- Spec says: "warmed fixed-capacity writes introduce zero new JS heap allocations…"
  (`specs/lmao/01q_wasm_memory_architecture.md:22-24`). The benchmark section rejects "warmed-write allocations"
  (`:881`).
- Code does: `packages/lmao/src/lib/wasm/threadSpanView.ts:341, 344, 346` allocate a `Float64Array(1)` for each view
  instance. `packages/lmao/src/lib/wasm/wasmSpanBuffer.ts:655-656` creates a fresh `Uint8Array` and `DataView` when it
  builds its identity views. The allocation count is checked only in tests
  (`packages/lmao/src/lib/wasm/__tests__/wasmAllocator.test.ts:80-81`,
  `packages/lmao/src/lib/wasm/WasmBufferStrategy.ts:224`). The Rust gate
  `packages/lmao/crates/lmao-core/tests/alloc_gate.rs` covers the Rust core only.
- Options: (a) add a JS allocation-counting gate that runs warmed writes and asserts zero new heap allocations, then
  remove the per-view allocations that fail it; (b) narrow the sentence to the Rust core, and say JS writes are not
  covered.
- Work: add a Bun allocation gate for warmed writes on the JS and the WASM buffer strategies. Build a span buffer, write
  every column once so its views are bound, then run N more writes to the same columns between
  `Bun.gc(true)`/`heapStats()` (`bun:jsc`) samples and assert the object count does not grow. The per-view
  `Float64Array(1)` fields at `packages/lmao/src/lib/wasm/threadSpanView.ts:341-350` and the identity views at
  `packages/lmao/src/lib/wasm/wasmSpanBuffer.ts:655-656` are span-setup and view-refresh cost, which 01q's Packed Span
  Superblocks section already excludes ("this is allocation-call batching, not a zero-allocation claim"); remove an
  allocation only when the gate finds it on the warmed path.
- Acceptance: `bun test` runs the gate for both strategies and passes; a control setter that allocates one object per
  write makes the gate fail.
- Decision: keep the spec. Narrowing the claim to the Rust core would drop the one hot-path property the JS write path
  promises, and nothing cited shows a warmed write allocating, only span setup.

### Per-block lifecycle states vs pin counter

- Spec says: each `WebAssembly.Memory.buffer` is an epoch (`specs/lmao/01q_wasm_memory_architecture.md:680`). The block
  states are `FREE → BUFFER_OWNED → FLUSH_PINNED → FREE` (`:682`), with `FLUSH_PINNED` defined at `:684-686`. The whole
  lifecycle is at `:675-687`.
- Code does: no `BUFFER_OWNED`, `FLUSH_PINNED`, or `blockState` in `packages/lmao/src/lib`. The pin is a counter:
  `packages/lmao/src/lib/wasm/wasmAllocator.ts:272` (`memoryEpochPins`), `:276-277` (throws "WASM memory grew while an
  Arrow lease pinned the previous epoch"), and `:299-316` (`pinMemoryEpoch()`, with `++` at `:302` and `--` at `:313`).
  Leases come from `packages/lmao/src/lib/traceTopology.ts:70-71` (`acquireLease()`). The caller is
  `packages/lmao/src/lib/convertToArrow.ts:1309`.
- Options: (a) add per-block states (`FREE`, `BUFFER_OWNED`, `FLUSH_PINNED`) to the allocator; (b) rewrite `:675-687` to
  the pin-counter design that the code has.
- Decision: adopt the shipped shape. A per-tree topology lease plus an epoch pin held only for the synchronous build
  covers every case the per-block states did, with no per-block state to keep consistent; 01q "Lifecycle and
  Asynchronous Ownership" now describes it.

### WASM borrow under ArrowLease

- Spec says: "per-request flush may borrow compatible WASM regions only under `ArrowLease`…"
  (`specs/lmao/01q_wasm_memory_architecture.md:23-25`). See also `:676-678` and `:711-713`.
- Code does: `packages/lmao/src/lib/convertToArrow.ts:1309-1312` pins the epoch and then copies. The comment at
  `:1310-1313` says WASM growth cannot keep borrowed views valid, so the leased result uses the Arrow-owned copy. The
  code always copies.
- Options: (a) implement borrowing under `ArrowLease`, using the pin to keep the region valid; (b) delete the borrow
  sentence and keep the copy boundary.
- Decision: adopt the shipped shape. `memory.grow()` detaches every view of the old buffer, so borrowing WASM regions
  would force growth to wait on lease release; the copy boundary keeps Arrow output valid with no such coupling, and 01q
  now states it.

### Lazy per-column alloc8B vs packed superblock

- Spec says: lazy column allocation calls `this._opContextMemory.alloc8B()`
  (`specs/lmao/01q_wasm_memory_architecture.md:698-709`, the call at `:701-705`). The same call appears at `:614-616`.
  The Phase Contract (`:13-14`) and the packed superblock (`:447-477`, with `create_and_start_span` at `:458`) describe
  one packed allocation. The two designs conflict.
- Code does: `grep alloc8B packages/lmao/src packages/lmao/crates` → no hits. The packed path is at
  `packages/lmao/src/lib/wasm/wasmAllocator.ts:161` (interface) and `:348` (export call),
  `packages/lmao/crates/lmao-arena/src/raw.rs:732`, and `packages/lmao/crates/lmao-wasm/src/lib.rs:354`.
- Options: (a) delete the lazy design (`:698-709`, and the `:614-616` call), and keep the packed superblock; (b)
  describe the packed path the code takes.
- Decision: adopt the shipped shape. One packed superblock per span holds every numeric family slab, and a lazy column
  only binds views over its slot on first write, so 01q's write path, span start, and allocator export list now describe
  that instead of `alloc8B`.

### Blocked snippet members

- Spec says: the `Blocked` snippet lists statics and fields only
  (`specs/lmao/01r_transient_errors_and_retry.md:211-221`).
- Code does: the instance members are not in the snippet. `packages/lmao/src/lib/errors/Blocked.ts:109-111`
  (`get _tag`), `:116-125` (constructor), and `:146-164` (`toJSON` and inspect).
- Options: (a) list the instance members in the spec snippet; (b) state that the snippet is an excerpt.
- Decision: adopt the shipped shape. The snippet in 01r now lists `get _tag`, the constructor, `toJSON`, and the inspect
  hook as `Blocked.ts` declares them.

### Layer that owns the Blocked budget

- Spec says: the budget is enforced at the "engine level", and the LMAO span retry loop does not retry `Blocked`
  (`specs/lmao/01r_transient_errors_and_retry.md:204-206`).
- Code does: `packages/lmao/src/lib/errors/RetriesExhausted.ts:4-5` says "the retry layer" converts the final failure.
  `packages/lmao/src/lib/spanContext.ts:205-212` (`getRetryableError`) retries only `TransientError`.
- Options: (a) convert at the engine level, as the spec says; (b) convert in the span retry layer, as the code comment
  says.
- Work: correct the doc comment at `packages/lmao/src/lib/errors/RetriesExhausted.ts:4-5` to say that the engine
  re-executing a blocked Op converts its final `Blocked`, never the span retry layer. `getRetryableError`
  (`packages/lmao/src/lib/spanContext.ts:200-213`) already returns a `Blocked` unchanged. The engine is the consumer's;
  see "Where the Blocked budget lives".
- Acceptance: a Bun test runs a span whose Op returns `Blocked.service('x', { maxAttempts: 3 })` and observes one Op
  invocation, no `span-retry` row, and `Err(Blocked)` as the span result.
- Decision: keep the spec. Converting in the span loop would make that loop retry `Blocked`, which erases the
  `TransientError`/`Blocked` distinction 01r is built on.

### executeWithRetry shape

- Spec says: `async function executeWithRetry<T extends LogSchema, S, E>(buffer: SpanBuffer<T>, …)`
  (`specs/lmao/01r_transient_errors_and_retry.md:284-287`).
- Code does: nine arity variants, `executeWithRetry0` through `executeWithRetry8`, at
  `packages/lmao/src/lib/spanContext.ts:333, 350, 368, 387, 407, 428, 450, 473, 506`. Call sites are at `:1890-2137`.
- Options: (a) one generic `executeWithRetry` that matches the spec signature; (b) keep the arity variants and document
  them in the spec.
- Decision: adopt the shipped shape. Fixed-arity loops pass the context and arguments straight through, so a span call
  allocates no closure, and they mirror `span0`–`span8`; 01r now shows `executeWithRetry1` and `getRetryableError`.

### Name ColumnDictionary

- Spec says: `class ColumnDictionary {` (`specs/lmao/01k_tree_walker_and_arrow_conversion.md:165`).
- Code does: the TS class is `DictionaryBuilder` (`packages/arrow-builder/src/lib/arrow/dictionary.ts:132`).
  `packages/lmao/src/lib/convertToArrow.ts:17, 90` imports it as `DictBuilder`. The Rust port declares
  `pub struct ColumnDictionary` (`packages/lmao/crates/lmao-arrow/src/dict.rs:402, 408`). No TS file declares
  `ColumnDictionary`.
- Options: (a) rename the spec to `DictionaryBuilder`; (b) rename the TS class and the Rust struct to
  `ColumnDictionary`.
- Work: rename the Rust port's `ColumnDictionary` to `DictionaryBuilder`: the struct at
  `packages/lmao/crates/lmao-arrow/src/dict.rs:408`, its doc comment at `:401-402` (which should name arrow-builder's
  `DictionaryBuilder`), the re-export in `packages/lmao/crates/lmao-arrow/src/lib.rs`, and the uses in
  `packages/lmao/crates/lmao-arrow/src/convert.rs:19`, `:172-175`,
  `packages/lmao/crates/lmao-arrow/tests/properties.rs`, and `packages/lmao/crates/lmao-arrow/benches/flush.rs`. The
  Rust `FinalizedDictionary` already shares the TS name.
- Acceptance: `grep -rn ColumnDictionary packages/lmao/crates` finds no hits, and the lmao-arrow tests pass.
- Decision: adopt the shipped shape. `DictionaryBuilder` is the TS class every caller uses, so 01k now names it (and
  places it in `arrow-builder`), and the Rust port takes the same name so one concept has one name in both languages.

### Name finalizeDictionary

- Spec says: `finalizeDictionary` (`specs/lmao/01k_tree_walker_and_arrow_conversion.md:255`; call at `:493`).
- Code does: the work is in `DictionaryBuilder.finalize` at
  `packages/arrow-builder/src/lib/arrow/dictionary.ts:185-223`. `grep finalizeDictionary packages/` → no hits.
- Options: (a) rename the spec to `DictionaryBuilder.finalize`; (b) add a free function `finalizeDictionary` that calls
  `finalize`. Either way, `['a', '\u00E9']` finalizes to values bytes `[0x61, 0xc3, 0xa9]`.
- Decision: adopt the shipped shape. `DictionaryBuilder.finalize(sorted)` returns a `FinalizedDictionary` class, and a
  free-function alias would be a second name for one operation; 01k now shows the method.

### Utf8Cache shortcut in utf8ByteLength

- Spec says: `utf8ByteLength` takes a shortcut through `this.utf8Cache.get(str)`
  (`specs/lmao/01k_tree_walker_and_arrow_conversion.md:198-200`).
- Code does: `packages/arrow-builder/src/lib/arrow/utf8.ts:25` has no cache lookup.
  `packages/arrow-builder/src/lib/arrow/dictionary.ts:169-171` calls `utf8ByteLength(str)` directly.
  `packages/lmao/src/lib/utf8Cache.ts:82` has `byteLength`, not `get`.
- Options: (a) route the byte count through `Utf8Cache.byteLength`; (b) drop the shortcut from the spec.
- Decision: adopt the shipped shape. `Utf8Cache.byteLength` encodes and caches on a miss, so routing first occurrences
  through it would allocate for strings seen once; 01k now counts bytes with `utf8ByteLength` directly.

### Utf8Cache API names

- Spec says: `Utf8Cache` has `getOrEncode` (`specs/lmao/01k_tree_walker_and_arrow_conversion.md:126`), `has` (`:138`),
  `get(str)` (`:145`), and `get textEncoder()` (`:152`). See also `:188-189` and `:282`.
- Code does: `packages/lmao/src/lib/utf8Cache.ts` has `encode` (`:61`), `byteLength` (`:82`), `encodeInto` (`:105`), and
  a batch method near line 126. It has no `has`, `getOrEncode`, `get`, or `textEncoder` member. The module exports
  `globalUtf8Cache` (`:189`) and `createUtf8Cache` (`:200`). Finalize uses the module-level `encoder` at
  `packages/arrow-builder/src/lib/arrow/dictionary.ts:129`.
- Options: (a) pick the method set in the code and align the spec names to it; (b) keep the spec names and add them to
  `Utf8Cache`.
- Decision: adopt the shipped shape. The shipped `Utf8Cache` is a bounded SIEVE cache implementing `Utf8Encoder`
  (`encode`, `byteLength`, `encodeInto`, `encodeMany`), which beats an unbounded `Map`; 01k now lists that API.

### convertSpanTreeToArrowTable signature and multi-root

- Spec says: `convertSpanTreeToArrowTable(rootBuffers: SpanBuffer[], schema: arrow.Schema, utf8Cache: Utf8Cache)`
  (`specs/lmao/01k_tree_walker_and_arrow_conversion.md:439-443`). The multi-root section is at `:749`. Runtime
  enforcement at `:779-780` throws on mismatched schemas.
- Code does: `packages/lmao/src/lib/convertToArrow.ts:1394-1399` takes one `rootBuffer: AnySpanBuffer`, plus
  `modulesToLogStats?`, `periodStartNs?`, and `borrowChunks = false`. It has no `utf8Cache` parameter and no multi-root
  input. The leased variant is `convertSpanTreeToLeasedArrowTable` at `:1645-1657`.
- Options: (a) implement the spec signature with multi-root input, and throw on mismatched schemas as `:779-780` says;
  (b) keep the single-root signature and rewrite `:439-443`, `:749`, and `:779-780` to match it.
- Decision: adopt the shipped shape. One tree per call matches the per-trace `ArrowLease`, isolates a failing trace, and
  lets library spans keep their own schema through the Pass 0 merge; 01k now documents the single-root signature.

### RemappedBufferView wrapper vs RemapDescriptor

- Spec says: the tree walk may contain a `RemappedBufferView`
  (`specs/lmao/01k_tree_walker_and_arrow_conversion.md:644-645`). Its child is `_children[0]` (`:650`), and
  `getColumnIfAllocated()` is at `:664`. The section runs `:642-664`. Registration is at `:722-725`, and the table row
  is at `:747`.
- Code does: no `RemappedBufferView` class exists. `grep RemappedBufferView packages/` finds only a comment at
  `packages/lmao/docs/transformer-optimization.md:57`. The remap is the `RemapDescriptor` interface at
  `packages/lmao/src/lib/logBinding.ts:30` (`sourceNames` at `:32`), attached as `remapDescriptor?` at `:60`.
  `createRemapDescriptor` is at `packages/lmao/src/lib/library.ts:105-108`. The `overflow_metric` column is asserted at
  `packages/lmao/src/lib/__tests__/remap-descriptor-integration.test.ts:225`.
- Options: (a) rewrite `:642-664` to describe the realized model: canonical buffers, with the descriptor applied during
  the cold traversal; (b) add a `RemappedBufferView` wrapper class to the code.
- Decision: adopt the shipped shape. A `RemapDescriptor` on the canonical child buffer keeps one object shape on the hot
  path and needs no wrapper class; 01k and 01q now describe the descriptor and the topology registration.

### Dynamic span name under a proven Op

- Spec says: "A dynamic name continues through public `span(name, ...)`"
  (`specs/lmao/01o_typescript_transformer.md:561-563`). The spec also says "A dynamic span name is not registered and
  keeps its string operand" (`:183-184`, and `:507`).
- Code does: `packages/lmao-ttsc/plugin/driver/lmao.go:416-421` builds `methodName := fmt.Sprintf("span%d", len(rest))`.
  The name operand is replaced only when a static ID exists for the name. A dynamic name therefore lowers to `spanN`
  with its string operand. The spec at `:183` cites `lmao.go:418-421` for the same code.
- Options: (a) keep `spanN(name, ...)` with the string operand, and update `:561-563` to say so; (b) emit the public
  `span(name, ...)` form, so the lowering matches `:561-563`.
- Decision: adopt the shipped shape. The fixed-arity `spanN` call with a string operand skips the variadic dispatcher
  and keeps the Promise schedule; 01o and `packages/lmao/docs/transformer-optimization.md` now say so.

### Op-local u16 lane removal and cutover

- Spec says: §6.1 says "the Op-local table and lane are removed, and no alias or dual-write shim exists"
  (`specs/lmao/01o_typescript_transformer.md:468-476`). The intro at `:18-20` says the global vocabulary "replaces the
  Op-local `u16` lane".
- Code does: the lane is still in use.
  - `packages/lmao-ttsc/plugin/driver/lmao.go:126` (`currentLogLocalIDs` init) and `:249` (field).
  - `packages/lmao-ttsc/plugin/driver/optimization.go:147` (param) and `:246-266` (builds the local dictionary).
  - `packages/lmao-ttsc/plugin/driver/loginline.go:68-69` (`templateID`, `localMessageID uint16`).
  - `packages/lmao/src/lib/wasm/threadSpanView.ts:410-414` (`vocabularyIdFor` reads `localMessageDictionary`).
  - `packages/lmao/src/lib/resolveMessage.ts:49-52`.
  - `packages/lmao/src/lib/convertToArrow.ts:391-392`.
- Options: (a) cut over in one change, removing the lane and its readers together; (b) stage the cutover. Staging needs
  a temporary dual path, which §6.1 forbids, so the spec would need a named exception.
- Work: remove the lane and every reader in one change, which must pass the §6.2 gates (see "Benchmark gate variants"):
  `currentLogLocalIDs` (`packages/lmao-ttsc/plugin/driver/lmao.go:126`, `:249`), the local dictionary build and its
  `localMessageDictionary` emission (`packages/lmao-ttsc/plugin/driver/optimization.go:147`, `:246-266`,
  `compileMetadataNode`), `localMessageID` (`packages/lmao-ttsc/plugin/driver/loginline.go:68-69`),
  `OpCompileMetadata.localMessageDictionary` (`packages/lmao/src/lib/op.ts:49`) and its normalization
  (`packages/lmao/src/lib/opContext/defineOp.ts:73-84`), and the `_messageIds` lane readers
  (`packages/lmao/src/lib/resolveMessage.ts:49-52`, `packages/lmao/src/lib/convertToArrow.ts:391-392`,
  `packages/lmao/src/lib/wasm/threadSpanView.ts:410-414`). Static log writes then store the dense vocabulary index in
  the packed row header.
- Acceptance:
  `grep -rn 'localMessageDictionary\|currentLogLocalIDs\|localMessageID\|_messageIds' packages/lmao/src packages/lmao-ttsc/plugin`
  finds no hits, and a Bun test logs a literal message through the transformer and reads it back from Arrow through the
  global vocabulary.
- Decision: keep the spec. One static-message representation is simpler than a `u16` lane indirecting into the global
  vocabulary, and a staged dual path is the shim §6.1 forbids. 01o no longer describes the lane: the §6 Op-local
  subsection, the intro's `u16` phrase, the `logTemplateIds` compile-metadata field, and the `plugin-on/current` gate
  baseline were removed, and the "logTemplateIds missing from compile metadata" work item was dropped.

### eagerColumns undocumented

- Spec says: the `defineOp` fourth argument at `specs/lmao/01o_typescript_transformer.md:238-240` lists `runtimeHint`
  and `logTemplateIds` only. `grep eagerColumns` on 01o → no spec hits.
- Code does: `packages/lmao-ttsc/plugin/driver/optimization.go:953-960` emits `eagerColumns`.
  `packages/lmao/src/lib/opContext/defineOp.ts:77-80` reads it.
- Options: (a) document `eagerColumns` as a compile-metadata field in §2; (b) remove the emission and the reader.
- Decision: adopt the shipped shape. Preallocating the columns every path writes is a real compile-time win the runtime
  already consumes; 01o §2 now documents `eagerColumns`.

### Metadata injection site

- Spec says: `metadata` is injected into `defineModule` (`specs/lmao/01o_typescript_transformer.md:343-347`, example
  `:349-359`). The cross-reference blockquote at `:374-378` names `defineOpContext` and `__metadata`.
  `specs/lmao/01l_op_context_pattern.md:795` says the transformer "injects metadata at the `defineOpContext` call site",
  and the example at `:804` uses `__metadata`.
- Code does: `packages/lmao-ttsc/plugin/driver/lmao.go:314-353` (`tryDefineModuleMetadata`) injects `git_sha`,
  `package_name`, and `package_file` into `defineModule({ ... metadata })`. The call site is at `:277`. A second
  `defineModule` panics at `:329-330`. `packages/lmao/src/lib/opContext/defineOp.ts:21-23` says the transformer injects
  at `defineOpContext`. `grep 'defineOpContext\|__metadata' packages/lmao-ttsc` → no hits.
- Options: (a) inject at `defineModule`, as the code does, and update `01l` `:795` and `:804`, the `defineOp.ts:21-23`
  comment, and the `:374-378` blockquote; (b) inject at `defineOpContext`, and change the transformer to match `01l` and
  the comment.
- Work: replace `tryDefineModuleMetadata` (`packages/lmao-ttsc/plugin/driver/lmao.go:314-353`) and its one-per-file
  panic with injection into the third argument of each proven `defineOp` call:
  `{ package_name, package_file, git_sha, line }`, where `applyHintRewrites` now inserts `undefined`
  (`packages/lmao-ttsc/plugin/driver/optimization.go:1011-1013`). A call that already has a third argument keeps it.
  Correct the comment at `packages/lmao/src/lib/opContext/defineOp.ts:21-23` to name that argument.
- Acceptance: a Go golden test shows `defineOp('x', fn)` compiling to
  `defineOp('x', fn, { package_name, package_file, git_sha, line: N }, …)`, and a Bun test reads `line === N` from the
  Op's `OpMetadata` without the stack fallback.
- Decision: adopt the shipped shape. The runtime has no `defineModule` and no context-level metadata key; its only
  intake is `defineOp`'s per-Op `metadata` argument, so 01o §5 (and 01l, 01a, 01g) now name that argument, and the
  transformer moves to it.

### V8 table rows vs §6.1

- Spec says: the V8 table rows at `specs/lmao/01o_typescript_transformer.md:709` and `:714-715` describe "Structured
  compile metadata with packed hint and Op-local templates when analysis is valid" (`:714`) and a private Op-local `u16`
  replaced by a stable message `u24` (`:715`). Section §6.1 (`:468-476`) says the Op-local lane is removed.
- Code does: the code still uses the Op-local lane. See "Op-local u16 lane removal and cutover" above.
- Options: (a) update the table rows to the global vocabulary; (b) keep the table as a "current" column and add a
  "target" column for §6.1.
- Work: none beyond "Op-local u16 lane removal and cutover"; the rows now describe the global vocabulary and eager
  columns.
- Acceptance: the V8 table in 01o names no Op-local template or `u16` lane.
- Decision: keep the spec. §6.1 makes the global vocabulary the design, and a current/target column pair is the status
  overlay specs must not carry, so the rows were updated to §6.1.

### Where the Blocked budget lives

- Spec says: the budget is enforced "at the engine level", and `RetriesExhausted` is the terminal error once a `Blocked`
  budget is consumed, without naming who owns that engine (`specs/lmao/01r_transient_errors_and_retry.md:39-42`,
  `:205-210`).
- Code does: lmao ships `Blocked`, `BlockedConfig`, and `RetriesExhausted` (`packages/lmao/src/lib/errors/`) and no
  engine. `grep 'new RetriesExhausted' packages/lmao/src` → no hits, and no package in this repo reads `blockedConfig`.
- Options: (a) 01r names an lmao helper that converts an exhausted `Blocked`; (b) the engine is the consumer's, and lmao
  ships only the classes.
- Decision: adopt the shipped shape. lmao has no engine, and the spec gives no delay for a `Blocked` without
  `nextRetry`, so a helper would invent schedule semantics; 01r now says the engine is the consumer's and lists what it
  applies, and the "RetriesExhausted for an exhausted Blocked budget" work item left this briefing because its
  conversion is not lmao code.

### `task()` line injection without a runtime `task`

- Spec says: 01o §7 described `tryTaskLine`, which appends the call-site line to `task('name', fn)` calls.
  `specs/lmao/01_trace_logging_system.md:40-62` records `task('name', fn)` as an alternative considered and rejected in
  favor of `defineOp`.
- Code does: `packages/lmao-ttsc/plugin/driver/lmao.go:453-466` (`tryTaskLine`, dispatched at `:280`) rewrites any
  two-argument call named `task` whose first argument is a string literal, with no checker provenance.
  `packages/lmao/src` has no `task` API.
- Options: (a) add a runtime `task` API; (b) delete §7 and the transform.
- Work: delete `tryTaskLine` (`packages/lmao-ttsc/plugin/driver/lmao.go:453-466`), its dispatch at `:280`, and the §7
  line of the header comment at `:22`. Drop the `task` cases from `packages/lmao-ttsc/plugin/driver/corpus_test.go` and
  `packages/lmao-ttsc/plugin/driver/corpus_dump_test.go`.
- Acceptance: `grep -rn tryTaskLine packages/lmao-ttsc/plugin` finds no hits, and a Go test shows a foreign
  `task('x', fn)` call emitted unchanged.
- Decision: adopt the shipped shape. The runtime has no `task` API and 01 rejected that design, so the transform can
  only append an argument to an unrelated same-named function; 01o §7 is deleted, and the transform goes with it.

### Output names a remapped span does not map

- Spec says: a mapped span exposes only its descriptor's output columns; an output name missing from `sourceNames` reads
  as null cells for that span, and scope values follow the same rule (`specs/lmao/01e_library_integration_pattern.md`,
  "Columns the library doesn't own" and "Arrow Conversion Example"). Before this pass 01e said a missing name is read
  under its own name.
- Code does: `resolveColumnName` (`packages/lmao/src/lib/convertToArrow.ts:258-259`) returns
  `remaps.get(buffer)?.sourceNames[outputName] ?? outputName`, so for a remapped buffer an unmapped output name falls
  back to itself. Every column pass reads through it: string and unknown columns (`:267`, `:280`), scope values
  (`:290-306`), binary (`:758-760`), number (`:1942-1945`, `:1969-1973`), boolean (`:2045-2049`) and enum
  (`:2132-2136`). With an application `status` column next to `httpOps.prefix('http')`, the HTTP span's own `status`
  values (and its `status` scope value) are written into the application's `status` column as well as `http_status`. The
  same fallback lets a library column dropped with `mapColumns({ x: null })` reach an application column named `x`.
- Options: (a) a remapped buffer resolves only names in its `sourceNames`, and any other name is an unallocated column;
  (b) keep the fallback and require applications to avoid output names equal to a library's source names.
- Work: `resolveColumnName` returns `string | undefined`: a buffer without a descriptor resolves to the output name, a
  remapped buffer to `sourceNames[outputName]`. Every caller listed above treats `undefined` as an unallocated column
  with no scope value, so the rows stay null.
- Acceptance: a Bun test gives the application schema `status: S.number()` and wires a library whose schema also has
  `status` as `libOps.prefix('http')`. The library op writes `tag.status(200)` and sets scope `status: 500`; the
  application op writes its own `status`. In the Arrow table the library span's rows have `http_status` 200 and a null
  `status`, and the application span keeps its own `status`. A second case drops a library column with
  `mapColumns({ debug: null })` next to an application `debug` column and finds `debug` null on the library rows.
- Decision: keep the spec, option (a). A column's meaning is the application's wiring; a fallback that depends on
  whether two schemas happen to share a field name leaks one library's data into an unrelated column, and the
  `defineOpContext` conflict check cannot see it because the library's source names are not output names.
