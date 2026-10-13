# lmao buffers, schema, and codegen: gaps between the specs and the code

The specs `specs/lmao/01a_trace_schema_system.md`, `01b1_buffer_performance_optimizations.md`,
`01b2_buffer_self_tuning.md`, `01b5_spanbuffer_memory_layout.md`, `01b6_buffer_codegen_extension.md`,
`01c_context_flow_and_op_wrappers.md`, and `01g_trace_context_api_codegen.md` state the buffer, schema, and codegen
design in present tense. The items below record where `packages/lmao/src` does not do that yet. Work items are
independent unless stated. Each former open decision now records its choice under `## Decisions`.

## Warmed fixed-capacity writes have no heap-measuring test

- Spec: `specs/lmao/01b1_buffer_performance_optimizations.md:48` and `:63-67`,
  `specs/lmao/01b2_buffer_self_tuning.md:60-63`, `specs/lmao/01b5_spanbuffer_memory_layout.md:28-30`: after warmup, a
  warmed fixed-capacity write makes zero new heap allocations.
- Code: `grep -rn "heapUsed\|memoryUsage" packages/lmao/src --include=*.test.ts` → no hits. Tests named for "allocates
  nothing" exist (`packages/lmao/src/lib/__tests__/coarseClock.test.ts:164`), but none measures heap. The only heap read
  in the package is `packages/lmao/src/lib/flushScheduler.ts:299`.
- Work: Add a test that builds a span buffer at fixed capacity, runs a warmup pass, then reads `heapUsed` before and
  after at least 10,000 warmed writes. Call `globalThis.gc()` before each reading. If `globalThis.gc` is missing, the
  test fails with a message that says so. It must not skip.
- Acceptance: the test asserts a heap delta of zero across the warmed writes. It passes when GC is exposed and fails
  with the explicit message when GC is not exposed.

## Effective memory accounting is not implemented

- Spec: `specs/lmao/01b2_buffer_self_tuning.md:55-58`: effective memory counts the JS heap, external and backing-store
  bytes, committed WASM pages, Arrow output, bytes pinned by `ArrowLease`, dictionary, UTF-8, and null buffers, and
  allocator overhead.
- Code: `grep -rn "effectiveMemory\|arrayBuffers" packages/lmao/src` → no hits.
  `packages/lmao/src/lib/flushScheduler.ts:299-303` reads only `heapTotal - heapUsed` from `process.memoryUsage()`.
- Work: Add one pure function over those components. It returns the total and each component separately. A component the
  runtime cannot read is reported as absent, never as zero.
- Acceptance: a unit test with fixed inputs gives the expected total and breakdown. A test with one component absent
  reports that component as absent, and the total is the sum of the components that are present.

## Lazy columns are not promoted from runtime usage

- Spec: `specs/lmao/01b2_buffer_self_tuning.md:275` and `:282-288`: frequently used lazy columns become eager. The
  per-column stats are `LazyColumnStats` (`:304`). The runtime counters are `stats._totalSpanBuffersCreated++` and
  `stats.{columnName}.instantiationCount++` (`:345-346`). `_totalSpanBuffersCreated` counts every buffer instance the
  class constructs, overflow segments included, because each instance runs the lazy getters; it is a different quantity
  from `SpanBufferStats.spansCreated`.
- Code: `grep` for `LazyColumnStats`, `instantiationCount`, `promoteColumnsToEager`, and `_totalSpanBuffersCreated` in
  `packages/lmao/src` → no hits.
- Work: Add the per-column instantiation counter and the buffer-created counter. Promote a column to eager for later
  buffers once the promotion rule in `specs/lmao/01b2_buffer_self_tuning.md:282-332` says it qualifies. Read that range
  before implementing. It sets the static and runtime rules.
- Acceptance: a test shows that a column that qualifies under the rule is eager for the next buffer. A test shows that a
  column that does not qualify stays lazy.

## Context-assembly sketch names types the code does not have

- Spec: `specs/lmao/01g_trace_context_api_codegen.md:191-203`: the sketch names
  `SpanContext<Schema, Deps, FF, Extra = {}>` (`:194`), `ScopeAPI<Schema>` (`:197`), `FluentSuccessResult` (`:201`),
  `FluentErrorResult` (`:202`), and `} & Extra;` (`:203`).
- Code: `grep -rn "ScopeAPI\|FluentSuccessResult\|FluentErrorResult" packages/lmao/src` → no hits. The span context type
  is `SpanContext<Ctx extends OpContext>` at `packages/lmao/src/lib/opContext/spanContextTypes.ts:284`. The result type
  is `ErrResult<E, T extends LogSchema>` at `packages/lmao/src/lib/result.ts:466`.
- Work: Add `ScopeAPI`, `FluentSuccessResult`, and `FluentErrorResult` as exported types. Build the two fluent result
  types on the existing result types in `packages/lmao/src/lib/result.ts`. Do not add a second result type hierarchy.
  Make `SpanContext` carry them.
- Acceptance: a type-level fixture imports the three names from the package entry point. It assigns the return values of
  `ctx.ok(...)` and `ctx.err(...)` to the fluent result types, and `tsc` accepts it.

## SpanContext block in 01c omits `spanSync` and `spanAuto0`–`spanAuto8`

- Spec: `specs/lmao/01c_context_flow_and_op_wrappers.md:41-43` documents `spanSync(name, fn)`. The `SpanContext` type
  block at `:534-562` lists neither `spanSync` nor any `spanAuto*` member.
- Code: `packages/lmao/src/lib/opContext/spanContextTypes.ts:406` `spanSync: SpanSyncFn<Ctx>;`, `:409`
  `spanAuto0<S, E>(...)`, through `spanAuto8` (declared at `:473`).
- Work: Add `spanSync` and `spanAuto0` through `spanAuto8` to the block at
  `specs/lmao/01c_context_flow_and_op_wrappers.md:534-562`. The block must list every `spanSync` and `spanAuto*` member
  declared in the `SpanContext` type of `packages/lmao/src/lib/opContext/spanContextTypes.ts`, and no others.
- Acceptance: a grep of the spec block and a grep of the code's `SpanContext` declaration list the same members.

## Codegen follow-ups outside the 01g design

- Spec: `specs/lmao/01g_trace_context_api_codegen.md` "Out of Scope" (end of file) lists dynamic schema updates
  (hot-reload, schema evolution for stored traces, versioning and migration), advanced type features (union attribute
  types, generic constraints for library composition, conditional types by operation context), and performance work
  (SIMD bulk attribute writes, memory pools for large traces, compression-aware column layouts). The 01g codegen does
  not design them; each is a follow-up, not a silent stub.
- Code: none of them exists in `packages/lmao/src`; nothing claims them.
- Work: each follow-up starts with a spec section that states its design, before any code.
- Acceptance: a follow-up leaves this list only when its spec section lands; any code that needs one of them fails with
  an explicit error until then, never a stub that silently ignores the request.

## Code comments describe superseded designs

- Spec: `specs/lmao/01c_context_flow_and_op_wrappers.md:552` (`err<E>(error: E)`),
  `specs/lmao/01b2_buffer_self_tuning.md` §Observability Hook (hook called from `getOrCreateOverflow()`), and
  `specs/lmao/01b5_spanbuffer_memory_layout.md` §Complete SpanBuffer Interface (`OpMetadata`, one per op).
- Code: `packages/lmao/src/lib/opContext/spanContextTypes.ts:371-372` documents `err` with `@param code` and
  `@param details`, but the signature takes one `error`. `packages/lmao/src/lib/capacityTuning.ts:63-64` says the hook
  runs in spanLoggerGenerator's `_getNextBuffer`; it runs in `getOrCreateOverflow`
  (`packages/lmao/src/lib/spanBuffer.ts`). `packages/lmao/src/lib/op.ts:42` says `OpMetadata` is a renamed
  `ModuleContext`.
- Work: rewrite the three comments to the current shape: one `@param error` for `err`, `getOrCreateOverflow` as the hook
  site, and the `op.ts` region comment without the rename history.
- Acceptance: `grep -n "@param code\|@param details" packages/lmao/src/lib/opContext/spanContextTypes.ts`,
  `grep -n "_getNextBuffer" packages/lmao/src/lib/capacityTuning.ts`, and
  `grep -n "ModuleContext" packages/lmao/src/lib/op.ts` find nothing.

## The root wiring is not checked against the libraries' declared deps

- Spec: `specs/lmao/01c_context_flow_and_op_wrappers.md` §SpanContext Properties (`deps`) and
  `specs/lmao/01e_library_integration_pattern.md` §How Nested Libraries Compose: every span shares the root's deps, so
  the root `defineOpContext` must wire, under the same key, a compatible group for every dep that any included group
  declares, transitively; a missing or incompatible dep is a type error.
- Code: `packages/lmao/src/lib/opContext/opGroupTypes.ts:286-296` types `DepsConfig` as
  `Record<string, AnyOpGroupPublic>`, and `packages/lmao/src/lib/defineOpContext.ts:316-342` accepts any such record.
  Nothing relates an included group's own `Ctx['deps']` to the root's keys, so a library span that calls an unwired
  `ctx.deps.x` type-checks and reads `undefined` at runtime. The `SpanContext` side types `deps` as
  `Record<string, any>` (`spanContextTypes.ts:63`) to avoid a recursive type.
- Work: add a `RequiredDeps<Deps>` type that collects, from each group in `Deps` (through `OpGroup<Ctx, …>` and
  `MappedOpGroup<Ctx, …>`), the keys and group types of `Ctx['deps']`, recursively, and constrain the root
  `defineOpContext` overloads so `Deps` must extend it. Report a missing key by name in the error. Keep the recursion
  out of `SpanContext` (only the root wiring is checked).
- Acceptance: a type-level test where an application includes `httpOps` (which declares `retry`) without wiring `retry`
  fails to compile and names `retry`; wiring `retry` with a group of another context type fails; the full wiring
  compiles, and the 01e library example, which calls `ctx.deps.retry` with no presence guard, type-checks.

## Decisions

### Dynamic reference lanes or raw `string[]` for CATEGORY and TEXT

- Spec says: "Dynamic reference lane for runtime strings" (`specs/lmao/01a_trace_schema_system.md:47`), and "dynamic
  CATEGORY and TEXT stay dynamic" (`specs/lmao/01a_trace_schema_system.md:58-59`). Separately,
  `specs/lmao/01b1_buffer_performance_optimizations.md:444-446` says CATEGORY is a raw `string[]`.
- Code does: stores CATEGORY and TEXT as raw `string[]`. Tests assert `Array.isArray` at
  `packages/lmao/src/lib/__tests__/arrow-builder-integration/integration.test.ts:117-119`,
  `packages/lmao/src/lib/__tests__/arrow-builder-integration/foundation.test.ts:89` and `:95`, and
  `packages/lmao/src/lib/__tests__/arrow-builder-integration/lazy-columns.test.ts:152`. The vocabulary binding type
  `VocabularyBinding = Uint32Array` exists at `packages/lmao/src/lib/vocabularyRegistry.ts:32`.
- Options: (a) migrate CATEGORY and TEXT to reference lanes backed by `VocabularyBinding`, and change the four
  assertions to check the lane type. (b) Change `specs/lmao/01a_trace_schema_system.md:44-48` and `:58-59` to raw
  `string[]`, matching `01b1:444-446`, and remove the vocabulary-binding text in `01a:60-72` that only serves dynamic
  lanes.
- Decision: adopt the shipped shape. A dynamic reference lane is the `string[]` of string references the code stores,
  `VocabularyBinding` cannot back runtime strings because it only maps compiler-known fragments, and `01a` now defines
  the lane as that array (`specs/lmao/01a_trace_schema_system.md:44-53`) while keeping the vocabulary text, which serves
  the static message lanes.

### Package metadata: which key carries it

- Spec says: the authoring surface is `defineLogSchema(...)` plus `defineOpContext({ logSchema, deps, flags, ctx })`
  (`specs/lmao/01a_trace_schema_system.md:134-138`). No metadata key is named in
  `specs/lmao/01a_trace_schema_system.md:117-138`.
- Code does: `OpContextConfig` (`packages/lmao/src/lib/opContext/types.ts:146-205`) has `logSchema`, `deps?`, `flags?`,
  and `ctx?`, and no metadata. The transformer injects `git_sha`, `package_name`, and `package_file`
  (`packages/lmao-ttsc/plugin/driver/lmao.go:344-346`). `OpMetadata` holds them at `packages/lmao/src/lib/op.ts:58-62`.
- Options: (a) add a metadata key to `OpContextConfig`, and name it in `specs/lmao/01a_trace_schema_system.md:117-138`.
  (b) Keep transformer injection as the only source, and document it in `01a`. Either way,
  `specs/lmao/01g_trace_context_api_codegen.md:363-364` still shows `packageName`, `packagePath`, and `gitSha`. Those
  keys must match the chosen option.
- Decision: adopt the shipped shape. Attribution is per op, so the per-op `defineOp(name, fn, metadata)` argument is the
  precise carrier and `OpContextConfig` needs no key; `specs/lmao/01a_trace_schema_system.md:145-148` states this and
  `specs/lmao/01g_trace_context_api_codegen.md:364-395` drops the `defineModule` keys. The transformer still targets
  `defineModule`; moving its injection to `defineOp` is tracked in `agent-todo/lmao-wasm-transformer-errors.md`
  ("Metadata injection site").

### Predictive growth, workload patterns, and multi-buffer coordination

- Spec says: "Predictive growth, memory-pressure compaction, usage-driven promotion, and multi-buffer coordination are
  described in the sections that follow" (`specs/lmao/01b2_buffer_self_tuning.md:53-54`). The examples at `:46-47` are
  labeled "illustrative examples".
- Code does: `grep -rn "predict\|workloadPattern\|BufferCoordinator\|SelfTuningBuffer" packages/lmao/src` finds no code
  symbol. "predictable" appears only in comments: `packages/lmao/src/lib/types.ts:187` and
  `packages/lmao/src/lib/__tests__/buffer-overflow.test.ts:315`.
- Options: (a) build predictive growth, memory-pressure compaction, usage promotion, and multi-buffer coordination as
  the spec describes them. (b) Mark `specs/lmao/01b2_buffer_self_tuning.md:46-47` and `:53-54` as target design, not
  built. Name which parts are normative.
- Decision: adopt the shipped shape. The utilization ratchet is the whole capacity tuner; the removed sketches (adaptive
  growth, predictive pre-allocation, workload detection, multi-buffer coordination, environment-based initial capacity,
  the `SelfTuningBuffer` example and its tests) grew buffers in place and walked `Object.entries` on the write path,
  which the spec's own performance contract forbids. `specs/lmao/01b2_buffer_self_tuning.md:41-53` and `:256-260` now
  state the ratchet, the fixed starting capacity, and promotion as the only other mechanism.

### Memory-pressure response: compaction or early flush

- Spec says: "### Memory Pressure Response" (`specs/lmao/01b2_buffer_self_tuning.md:260-262`). The FlushScheduler sketch
  calls `this.isMemoryPressure()` (`:438`).
- Code does: flushes on pressure. `packages/lmao/src/lib/flushScheduler.ts:299-303` reads `heapTotal - heapUsed` and
  calls `this.doFlush('memory')` below `memoryPressureThreshold`. The response in the code is that flush.
- Options: (a) add buffer compaction as the spec's response section describes. (b) Rewrite
  `specs/lmao/01b2_buffer_self_tuning.md:260-262` and `:53-54` so the response is an early flush, as the code does.
- Decision: adopt the shipped shape. An early flush frees memory without copying live columns, while compaction would
  add a resize-and-copy path to buffers that are still being written; `specs/lmao/01b2_buffer_self_tuning.md:218-224`
  describes the scheduler's `'memory'` flush and the compaction sketches are gone.

### One `ArrowLease` per request, or per converted buffer

- Spec says: "The scheduler defines one logical request/trace flush measurement and one `ArrowLease` per request result"
  (`specs/lmao/01b2_buffer_self_tuning.md:339-341`).
- Code does: `packages/lmao/src/lib/flushScheduler.ts:384-392` collects one lease per converted buffer. `:462-463`
  releases all of them in a `finally`.
- Options: (a) group a request's buffers under one lease, as the spec says. (b) Keep one lease per buffer, and rewrite
  `specs/lmao/01b2_buffer_self_tuning.md:339-341` to say so.
- Decision: adopt the shipped shape. Each registered root buffer is one request's span tree, so one lease per converted
  root is one lease per request; the code already matches the spec, and `specs/lmao/01b2_buffer_self_tuning.md:88-92`
  now names the per-root conversion.

### Name and scope of the per-module metadata record

- Spec says: "ModuleContext" composition at `specs/lmao/01b2_buffer_self_tuning.md:335-336`, and `Set<ModuleContext>` at
  `:1244`.
- Code does: no `ModuleContext` symbol exists. Per-op metadata is `OpMetadata` at `packages/lmao/src/lib/op.ts:54-81`. A
  comment at `packages/lmao/src/lib/op.ts:42` records that `ModuleContext` was renamed.
- Options: (a) rename the spec's `ModuleContext` to `OpMetadata` at `specs/lmao/01b2_buffer_self_tuning.md:335-336` and
  `:1244`. (b) Rename the code's `OpMetadata` to `ModuleContext`, and reconcile its per-op fields with the per-module
  scope the spec describes.
- Decision: adopt the shipped shape. No per-module record exists: attribution is `OpMetadata` (one per op), the schema
  is on the op context's `LogBinding`, and stats are on the generated class; `01b2` (`:83-85`, `:625-627`), `01b`
  (`:185-188`), and `01b1` (`:1181-1212`, `:1308-1336`, `:1399`) now use those names, and the stats tracker is the
  `Map<SpanBufferConstructor, OpMetadata>` the scheduler keeps.

### Stats entry types

- Spec says: the Entry Types table lists `buffer-writes`, `buffer-overflow-writes`, `buffer-created`, and
  `buffer-overflows` (`specs/lmao/01b2_buffer_self_tuning.md:1223-1230`). Each tracked module emits 4 entries (`:1250`),
  and `entry_type` is one of those four (`:1254`).
- Code does: `packages/lmao/src/lib/arrow/capacityStats.ts:69` lists `period-start`, `buffer-writes`, `buffer-spans`,
  and `buffer-capacity`.
- Options: (a) change the code to the spec's four entry types. (b) Change
  `specs/lmao/01b2_buffer_self_tuning.md:1223-1230`, `:1250`, and `:1254` to the code's four entry types.
- Decision: adopt the shipped shape. The shipped rows are exactly the ratchet's inputs, so a reader can recompute
  utilization, while the spec's table had two rows with the same meaning;
  `specs/lmao/01b2_buffer_self_tuning.md:603-666` now lists `period-start`, `buffer-writes`, `buffer-spans`, and
  `buffer-capacity`, matching `01n`.

### Hook order around capacity tuning

- Spec says: the hook runs "After capacity tuning decision is made" (`specs/lmao/01b2_buffer_self_tuning.md:1337`),
  "Before stats counters are reset to zero" (`:1338`), and "immediately before `checkCapacityTuning()`" (`:1341-1342`).
- Code does: `packages/lmao/src/lib/spanBuffer.ts:621-623` calls `tracer.onStatsWillResetFor(this)`, then
  `checkCapacityTuning(...)`. The hook runs before tuning. That matches `:1341-1342`, not `:1337`.
- Options: (a) keep "before", and delete the "After" bullet at `specs/lmao/01b2_buffer_self_tuning.md:1337`. (b) Move
  the call after `checkCapacityTuning()`, and rewrite `:1338` and `:1341-1342` to match.
- Decision: adopt the shipped shape. Calling the hook before the tuning check is the only order that shows tracers the
  counters before a reset; `specs/lmao/01b2_buffer_self_tuning.md:678` and `:715-734` now say it runs on every overflow,
  before `checkCapacityTuning()`, whether or not capacity changes.

### `_children` on the span buffer

- Spec says: `_children: SpanBuffer[]` is a field. It is set to `[]` in the constructor sketch and pushed to when a
  child span is created (`specs/lmao/01b5_spanbuffer_memory_layout.md:254-255`, `:283`, `:364`). It appears in the
  layout diagram (`:72`, `:101`, `:126`), in the tree-navigation row (`:386`), and in the generated class
  (`specs/lmao/01b6_buffer_codegen_extension.md:113`).
- Code does: no non-disabled source file declares `_children`.
  `packages/lmao/src/lib/__tests__/traceTopology.property.test.ts:41-42` asserts that the property is absent.
  `wasmTracer.test.ts.disabled` still uses it.
- Options: (a) add `_children` to the span buffer as the spec describes. (b) Remove `_children` from `01b5` (`:72`,
  `:101`, `:126`, `:254-255`, `:283`, `:364`, `:386`) and from `01b6:113`, and keep the test's absence assertion as the
  contract.
- Decision: adopt the shipped shape. The trace-owned `TraceTopology` arena links children through typed arrays and walks
  them without allocating, where `_children` allocated one array per span; `01b5`, `01b6:113-114`, `01b1:141-214`,
  `01b`, and `01c` (`:114`, `:460-461`) now show `_nodeIndex` and topology registration, matching the topology walk in
  `specs/lmao/01k_tree_walker_and_arrow_conversion.md`.

### Fields of `OpMetadata`

- Spec says: `OpMetadata` has `prefix?` (`specs/lmao/01b5_spanbuffer_memory_layout.md:562`), `logSchema` (`:563`), and
  the buffer fields `sb_capacity` (`:566`), `sb_totalWrites` (`:567`), `sb_overflows` (`:568`), and `sb_totalCreated`
  (`:569`).
- Code does: `OpMetadata` (`packages/lmao/src/lib/op.ts:54-81`) has `name`, `package_name`, `package_file`, `git_sha`,
  `line`, the `*_entry` fields, and `_physicalLayoutPlan?`. It has no `prefix`, `logSchema`, or `sb_*` field.
- Options: (a) add the listed fields to `OpMetadata`. (b) Rewrite `specs/lmao/01b5_spanbuffer_memory_layout.md:557-570`
  to the code's field list. The `sb_*` names follow the counter-name decision below.
- Decision: adopt the shipped shape. The schema belongs to the op context's `LogBinding` and the stats belong to the
  generated class, so copying them onto every op's metadata would duplicate state; `01b5` now lists the code's fields
  and reads stats through `buffer._stats`.

### Fields of `SpanBufferStats`

- Spec says: `SpanBufferStats` has `capacity`, `totalWrites`, `overflowWrites`, `totalCreated`, and `overflows`
  (`specs/lmao/01b5_spanbuffer_memory_layout.md:740-745`).
- Code does: `packages/lmao/src/lib/spanBufferStats.ts:32-41` has `capacity` (`:34`), `totalWrites` (`:37`), and
  `spansCreated` (`:40`). It has no overflow counter.
- Options: (a) add `overflowWrites` and `overflows` to `SpanBufferStats`, and name the created counter per the decision
  below. (b) Rewrite `specs/lmao/01b5_spanbuffer_memory_layout.md:740-745` to the code's three fields.
- Decision: adopt the shipped shape. Overflow is already visible through utilization above 100%, so separate overflow
  counters would be a second name for the same signal; `specs/lmao/01b5_spanbuffer_memory_layout.md:747-751` and its
  update table now show `capacity`, `totalWrites`, and `spansCreated`.

### Name of the created-buffer counter

- Spec says: `stats._totalSpanBuffersCreated++` (`specs/lmao/01b2_buffer_self_tuning.md:957`), `totalCreated`
  (`specs/lmao/01b5_spanbuffer_memory_layout.md:744`), and `sb_totalCreated` (`:569`). The constructor sketch takes
  `stats: SpanBufferStats` (`:270`) but never uses it. It has no created-counter update (`:268-337`). The same counter
  is dropped from `getSpanBufferClass` in `specs/lmao/01b6_buffer_codegen_extension.md:318-325` and `:334-377`.
- Code does: the counter is `spansCreated` (`packages/lmao/src/lib/spanBufferStats.ts:40`). It is initialized at
  `packages/lmao/src/lib/spanBuffer.ts:218`, read at `packages/lmao/src/lib/capacityTuning.ts:85` and
  `packages/lmao/src/lib/arrow/capacityStats.ts:108`, and reset at `packages/lmao/src/lib/capacityTuning.ts:68`.
- Options: (a) rename the code's `spansCreated` to one spec name, and make the constructor and `getSpanBufferClass`
  update it. (b) Rename the spec's three names to `spansCreated`.
- Decision: adopt the shipped shape. `spansCreated` names what is counted (non-chained spans, incremented by the root
  and child factories, never by overflow); `01b5` and `01b6:272-275` now use it, and
  `LazyColumnStats._totalSpanBuffersCreated` stays because it counts buffer instances for the promotion ratio, a
  different quantity.

### `err` signature

- Spec says: `err<E>(code: string, data?: E): ErrResult<E>;` (`specs/lmao/01c_context_flow_and_op_wrappers.md:553`).
  Call sites use two arguments: `ctx.err('HTTP_ERROR', { status: response.status })`
  (`specs/lmao/01e_library_integration_pattern.md:322`) and `ctx.err('NOT_FOUND', { id })`
  (`specs/lmao/01l_op_context_pattern.md:269`).
- Code does: `packages/lmao/src/lib/opContext/spanContextTypes.ts:379` declares
  `err<E>(error: E): ErrResult<E, Ctx['logSchema']>;`. The single-argument form matches `ctx.err(NOT_FOUND({ id }))` in
  `specs/lmao/01h_entry_types_and_logging_primitives.md:130`.
- Options: (a) change the code to `(code, data?)`, and update
  `specs/lmao/01h_entry_types_and_logging_primitives.md:130`. (b) Change
  `specs/lmao/01c_context_flow_and_op_wrappers.md:553` and the sketch at
  `specs/lmao/01g_trace_context_api_codegen.md:202` to the single-value form, and update the call sites in `01e:322` and
  `01l:269`, `:271`.
- Decision: adopt the shipped shape. One error value carries its code and data with a precise type (`defineCodeError`
  results), and a child's error passes through unchanged; `01c:552` and `:796` and `01g:202` now use `err<E>(error: E)`.
  The `01l` call sites change in `01l`, and the `01e:322` call site is listed for the integrator.

### Scope set cost and `_getScopeValues`

- Spec says: "Zero allocation on scope set - just sets a property" (`specs/lmao/01g_trace_context_api_codegen.md:220`).
  Inheritance uses "`_getScopeValues()`" (`:222`). The `_setScope` sketch is at `:230-233`.
- Code does: the generated `_setScope` builds a new object and freezes it on every call
  (`packages/lmao/src/lib/codegen/spanLoggerGenerator.ts:305-318`, with `Object.freeze(next)` at `:318`). Inheritance
  goes through `_prefillScopedAttributesOn` (`:254`, `:329`, `:390`). No generator source emits `_getScopeValues`:
  `grep -rn "getScopeValues" packages/lmao/src` finds only the snapshot
  `packages/lmao/src/lib/codegen/__tests__/__snapshots__/scopeGenerator.test.ts.snap` (lines `12`, `76`, `126`, `183`).
  That snapshot is stale.
- Options: (a) keep the per-set frozen object, rewrite `specs/lmao/01g_trace_context_api_codegen.md:220-222` to describe
  it, and regenerate the snapshot without `_getScopeValues`. (b) Make `_setScope` write without allocating, as `:220`
  says. That drops the freeze and its immutability guarantee. Either way, decide whether `_getScopeValues()` (`:222`) is
  built or removed from the spec.
- Work: delete `packages/lmao/src/lib/codegen/__tests__/__snapshots__/scopeGenerator.test.ts.snap`. No
  `scopeGenerator.test.ts` exists, so the snapshot is orphaned and is the only place `_getScopeValues` appears.
- Acceptance: `grep -rn "getScopeValues" packages/lmao/src` finds nothing, and the lmao test suite reports no obsolete
  snapshot.
- Decision: adopt the shipped shape. A frozen snapshot per `setScope` lets children inherit by reference with no copy at
  span creation, which is the hotter path, and keeps a parent's later scope changes out of children; `01g:213-255` and
  the comparison table now describe it, and `_getScopeValues()` is gone from the spec.

### Library entry point: `defineModule` or `defineOpContext`

- Spec says: libraries use `defineModule({...})` with metadata `packageName`, `packagePath`, and `gitSha`
  (`specs/lmao/01g_trace_context_api_codegen.md:357-364`), then `.ctx<...>(...)` (`:374`), `.make()` (`:375`), and
  `const { defineOp } = httpLib;` (`:377`).
- Code does: `defineModule` is absent from `packages/lmao/src` except in comments:
  `packages/lmao/src/lib/defineOpContext.ts:258` and `packages/lmao/src/lib/__tests__/library.test.ts:5`. The shipped
  entry point is `defineOpContext` (`packages/lmao/src/lib/defineOpContext.ts:298-342`).
  `specs/lmao/01d_ai_agent_integration.md:551` already uses it.
- Options: (a) implement `defineModule(...).ctx(...).make()` as the library entry point, and reconcile it with
  `defineOpContext`. (b) Rewrite `specs/lmao/01g_trace_context_api_codegen.md:357-377` to `defineOpContext({...})`, and
  drop the `defineModule` metadata keys. Option (b) follows the package-metadata decision above.
- Decision: adopt the shipped shape. `defineOpContext` is the one entry point the code, `01a`, `01d`, and `01j` already
  use, and library prefixing goes through `defineOps(...).prefix(...)` in the consumer's `deps`;
  `specs/lmao/01g_trace_context_api_codegen.md:364-395` now shows that form.

### FlushScheduler root collection: array cleared per flush, or a Set with delete after success

- Spec says: "**Array-Based Buffer Collection**: Uses `SpanBuffer[]` array … cleared after each flush" and "**No
  Individual Unregister**: The entire array is cleared after flush (`this.buffers = []`)"
  (`specs/lmao/01b2_buffer_self_tuning.md` §FlushScheduler Design items 1-2, before this decision), with the matching
  sketch and §How It Works item 1.
- Code does: `packages/lmao/src/lib/flushScheduler.ts:129` keeps `private buffers = new Set<AnySpanBuffer>()`, `:166`
  adds on `register`, `:451` deletes each flushed root only after the handler resolved, and `:461` leaves every root
  registered and unreset when the handler throws.
- Options: (a) clear an array at the start of each flush, as the spec said. (b) Keep the Set and delete after success.
- Work: a root whose conversion throws (`packages/lmao/src/lib/flushScheduler.ts:387-397` catches and only calls
  `console.error`) is still reset and deleted at `:436-452` once the handler succeeds, so its rows are lost. Keep such a
  root registered and unreset, and report the conversion error through the flush result instead of the console.
- Acceptance: a test with two roots, one whose conversion throws, shows the failing root still registered with its rows
  after the flush, the other root flushed and removed, and the error visible to the caller.
- Decision: adopt the shipped shape. Clearing the array before the handler runs loses every root when the handler fails,
  while the Set keeps them for the next flush and also deduplicates repeated registration;
  `specs/lmao/01b2_buffer_self_tuning.md:69-91`, its Flush Cycle sketch, and §How It Works item 1 now describe it.

### Single RecordBatch per flush, or one record batch per request

- Spec says: "**Single RecordBatch Per Flush**: All root buffers in a flush are converted to a **single RecordBatch**
  for maximum dictionary reuse. This requires all buffers to share the same schema (enforced at runtime)", and "The
  conversion function validates this requirement and throws if schemas differ" (`specs/lmao/01b2_buffer_self_tuning.md`
  §FlushScheduler Design items 3-4, before this decision).
- Code does: `packages/lmao/src/lib/flushScheduler.ts:389` converts each root separately with
  `convertSpanTreeToLeasedArrowTable`, so each root gets its own batch and dictionaries. `mergeTables` (`:38-53`)
  concatenates the per-root batches column by column into one `Table`. No code compares schemas across roots or throws.
- Options: (a) convert all roots into one batch with shared dictionaries. (b) Keep one batch per root, coalesced into
  one table.
- Work: fix three defects in the shipped merge. (1) `mergeTables` takes column names from the first table only (`:41`)
  and skips a column a later table lacks (`:46`), so a root whose tree carries a different library schema loses columns
  or misaligns rows; build the union of names and null-fill a batch's missing columns. (2) Every root's conversion
  receives the capacity-stats entries (`:389`), so a stats-logging flush with N roots emits the stats rows N times; pass
  them to one conversion only. (3) The scheduler never passes `periodStartNs`, so
  `packages/lmao/src/lib/convertToArrow.ts:1630` writes period start `0n`; track the period start in the scheduler and
  pass it.
- Acceptance: a flush of two roots with different column sets yields a table whose row count is the sum of both, whose
  columns are the union, and whose cells are null where a root lacked the column; a stats-logging flush with three roots
  yields exactly four stats rows per tracked buffer class, with a nonzero `period-start` value.
- Decision: adopt the shipped shape. One batch per request keeps each request's dictionaries, lease, and boundary
  separate, as §Ownership requires, while one shared batch would erase request boundaries for dictionary reuse that
  nothing measured; `specs/lmao/01b2_buffer_self_tuning.md:83-91` and the Flush Cycle sketch now describe it, and the
  merge defects above are work.
