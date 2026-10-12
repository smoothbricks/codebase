# lmao context and entry types: gaps between the specs and the code

The lmao specs (`specs/lmao/01h`, `01j`, `01l`, `01m`, `01n`, `01p`) state the context and entry-type design in present
tense. The items below list where the code does not do that yet. Items are independent unless stated. The flush item
depends on the per-op counters item. Work items come first. Items that need a design choice are under Open decisions.
Resolve those before implementing the work they affect.

## Warmed entry writes allocate nothing

- Spec: `specs/lmao/01h_entry_types_and_logging_primitives.md:555`: a warmed entry write allocates nothing while
  capacity remains. The same rule appears at `specs/lmao/01j_module_context_and_spanlogger_generation.md:16` and `:41`.
- Code: no test measures allocation on the JS entry write path. The only identity-path test is
  `packages/lmao/src/lib/__tests__/identity-view.property.test.ts:44`, and it covers reads. The wasm tests count
  allocator blocks for numeric writes: `packages/lmao/src/lib/wasm/__tests__/wasmSpanBuffer.test.ts:203`, `:231`, and
  `:561-563`.
- Work: Measure first. Warm a callsite, then write entries within capacity. On the wasm path, reuse the allocator-count
  approach from the tests above. On the JS path, measure heap growth with a probe; do not infer it. If the probe shows
  an allocation, remove it from the write path and run the probe again.
- Acceptance: A test warms a callsite and writes N entries within capacity. It asserts zero allocator growth on wasm and
  zero measured heap growth on JS. Adding one allocation per write makes the test fail.

## Lint rule against capturing the fluent interface

- Spec: `specs/lmao/01j_module_context_and_spanlogger_generation.md:820-822`: an ESLint rule named
  `eslint-no-capture-fluent` enforces the rule stated at `:822`: because `tag` writes entries, users must not capture
  the fluent interface.
- Code: no hits for `eslint-no-capture` or `no-capture-fluent` under `packages/lmao`.
- Work: Add the rule next to the custom ESLint rules the repo already has. Report a fluent chain whose result is stored
  in a variable, a property, or a closure. Accept a chain used in place on one expression.
- Acceptance: RuleTester cases pass. A stored chain is reported. An in-place chain is accepted.

## Kind tags for log templates and span names

- Spec: `specs/lmao/01j_module_context_and_spanlogger_generation.md:118-120`: `kindTags[i] = 1` means `LOG_TEMPLATE`,
  and `kindTags[i] = 2` means `SPAN_NAME`.
- Code: `packages/lmao/src/lib/vocabularyRegistry.ts:300` checks `kindTag !== 1 && kindTag !== 2` with numeric literals.
  No `LOG_TEMPLATE` or `SPAN_NAME` symbol exists under `packages/lmao/src/lib`. The two names appear only in
  `docs/transformer-optimization.md`.
- Work: Define the two constants in `packages/lmao/src/lib/vocabularyRegistry.ts` and export them. Replace the literals
  at `:300` and every other kind-tag comparison. Keep the numeric values unchanged.
- Acceptance: No numeric `kindTag` comparison remains outside the constants definition. A registry test rejects any kind
  tag other than the two named constants.

## Masked text fields

- Spec: `specs/lmao/01j_module_context_and_spanlogger_generation.md:760`: `url: S.text().masked('url')`. The same line
  appears at `specs/lmao/01m_lambda_routing_example.md:109` with the comment `// URL masking for PII`.
- Code: no hits for `masked(` under `packages/lmao`.
- Work: Add `masked(kind)` to the text schema builder. `S.text().masked('url')` must type-check, and the column
  definition must record the kind. The specs do not define the mask transform or the point where it runs. Take that rule
  from the spec that owns PII handling. If no such rule exists, stop and raise it as a spec question before writing the
  transform.
- Acceptance: The example schema type-checks. The column definition exposes the mask kind. A test writes a raw value to
  a masked column and checks that the raw value does not appear in the Arrow output.

## Per-op counters on `Op`

- Spec: `specs/lmao/01n_op_and_buffer_metrics.md:210-211`: the `Op` class keeps metrics counters that reset on each
  flush. The sketch at `:214-233` names `invocationCount`, `errorCount`, and `exceptionCount` (`:220-222`) and
  `periodStartNs` (`:232`). `invoke` increments `invocationCount` at `:245-246`.
- Code: `packages/lmao/src/lib/op.ts:117-166` (`class Op`) has the fields `metadata`, `callsitePlan`, `fn`,
  `_resultTypes`, and `_opContextBinding`. It has no counter field. No hits for `invocationCount` under `packages/lmao`.
- Work: Add the counters from the sketch to `Op`. Increment `invocationCount` on each invocation. Increment `errorCount`
  and `exceptionCount` at the span-err and span-exception outcome points the tracer already has. Track `periodStartNs`
  as the sketch describes. Keep the counters on `Op`. Do not add a second store in the tracer.
- Acceptance: A test runs three invocations, one with an error and one with an exception. The counters read 3, 1, and 1.
  After a flush, all counters read zero.

## Per-op rows written at flush

- Spec: `specs/lmao/01n_op_and_buffer_metrics.md:287`: the per-op rows are written by the same flush that writes
  `period-start` and the `buffer-*` rows. The flush steps at `:300-313` write the rows per module, then reset the
  counters.
- Code: `packages/lmao/src/lib/arrow/capacityStats.ts:69` lists `period-start`, `buffer-writes`, `buffer-spans`, and
  `buffer-capacity`, and `:108` writes their values. That file writes no op rows. The op entry types are declared in
  `packages/lmao/src/lib/schema/systemSchema.ts:375-377`.
- Work: In the flush, after the buffer rows, write one op row per op for each op entry type. Read the counters, then
  reset them, in that order. This item depends on the per-op counters item above.
- Acceptance: After a flush with known counts, the Arrow output holds op rows with those values. The next flush reports
  zero for ops with no invocations.

## Open decisions

### Deferred tag closures on results (`applyTags`)

- Spec says: "Each chained method captures the write operation as a closure in `applyTags`:"
  (`specs/lmao/01h_entry_types_and_logging_primitives.md:887`). The same design appears at `:897`, `:907`, `:936`, and
  `:961`.
- Code does: `packages/lmao/src/lib/result.ts:111-121`: `Ok` holds `value` and `_state` only. Fluent writes go to the
  creating span's row 1 (`result.ts:105-107`). No hits for `applyTags` under `packages/lmao`.
  `packages/lmao/src/lib/spanContext.ts:559-569`: `writeSpanEnd` writes span-ok or span-err at once, from the result's
  class.
- Options: (a) Implement the closure chain. Each fluent method records a closure on the result, and `writeSpanEnd` runs
  them against the buffer. (b) Change the spec to the eager design the code has: fluent methods write row 1 when called.
  Delete the `applyTags` text at `:887`, `:897`, `:907`, `:936`, and `:961`.

### Whether results may defer closures

- Spec says: "They do not allocate deferred closure chains or retarget attributes to whichever span later receives the
  result." (`specs/lmao/01h_entry_types_and_logging_primitives.md:136`).
- Code does: no deferred closures. `packages/lmao/src/lib/result.ts:105-115` writes fluent values to the creating span's
  row 1 through `_state`. This matches `:136`. It contradicts the `applyTags` text at `:887`.
- Options: (a) Keep `:136`, and remove the `applyTags` design (see the previous decision, option b). (b) Keep the
  `applyTags` design, and rewrite `:136` to allow deferred closures. The code then follows the previous decision, option
  (a).

### Placeholder syntax and brace escapes

- Spec says: "Placeholders use `{fieldName}`; `{{` and `}}` encode literal braces."
  (`specs/lmao/01h_entry_types_and_logging_primitives.md:292-293`). Other passages use `{field}`: `:679`, `:1133`, and
  `specs/lmao/01j_module_context_and_spanlogger_generation.md:140`.
- Code does: `packages/lmao/src/lib/tracers/StdioTracer.ts:158` matches `/\{([A-Za-z0-9_]+)\}/g`. It has no `{{` or `}}`
  escape branch. The escape is missing under either name.
- Options: (a) Use `{fieldName}` in every passage: `01h:292`, `:344`, `:679`, `:1133`, and `01j:140`. Then add the
  escape branch. (b) Use `{field}` in every passage. Change `01h:292-293` and `:344`. Then add the escape branch.

### Buffer metric names

- Spec says: "SpanBuffers allocated" for `buffer-created` (`specs/lmao/01h_entry_types_and_logging_primitives.md:478`).
  The metric list is `buffer-writes`, `buffer-overflow-writes`, `buffer-created`, and `buffer-overflows` (`:470-482`,
  and again at `:1255`).
- Code does: `packages/lmao/src/lib/schema/systemSchema.ts:384-386` defines `buffer-writes`, `buffer-spans`, and
  `buffer-capacity`. `packages/lmao/src/lib/arrow/capacityStats.ts:69` and `:108` write those three. No hits under
  `packages/lmao` for `buffer-created`, `buffer-overflows`, or `buffer-overflow-writes`.
- Options: (a) Add the three missing codes to `systemSchema.ts` as new entries at the end, and write them in
  `capacityStats.ts`. Decide whether `buffer-created` and `buffer-spans` are the same value. (b) Change the spec to the
  three shipped codes. Also update the same list in other specs: the `01b2` spec at lines 1228-1230, the `01f` spec at
  `:858`, and the `01d` spec at `:623`. An earlier pass found these. This pass did not re-check them.

### Physical layout cache key

- Spec says: "A three-dimensional cache keyed by **schema identity + physical layout version + backend kind**"
  (`specs/lmao/01j_module_context_and_spanlogger_generation.md:24-26`).
- Code does: `packages/lmao/src/lib/physicalLayoutPlan.ts:260-269` caches by schema in a `WeakMap`, then by SpanContext
  class. `:278` builds a string key. The key includes `PHYSICAL_LAYOUT_VERSION`, so the version term is present. It also
  includes the runtime hint, message layout, context layout, vocabulary generation, eager columns, and local message
  dictionary. No backend-kind term is present. The buffer class comes from the plan
  (`packages/lmao/src/lib/JsBufferStrategy.ts:92`).
- Options: (a) Add the backend kind to the string key at `physicalLayoutPlan.ts:278`, so JS and wasm plans never share
  an entry. (b) Change the spec to the inputs the key uses today: schema identity, layout version, and the other terms
  listed above.

### Flush unit ("one request")

- Spec says: "The flush unit is one request." (`specs/lmao/01j_module_context_and_spanlogger_generation.md:50-51`).
- Code does: no hits for `coalesc` under `packages/lmao`. The flush is triggered by
  `packages/lmao/src/lib/flushScheduler.ts:299-303`, which includes a memory-pressure path. The explicit growth error is
  already present. `packages/lmao/src/lib/wasm/wasmAllocator.ts:276-277` throws "WASM memory grew while an Arrow lease
  pinned the previous epoch". The pin counter is at `:272`, `:302`, and `:313`.
- Options: (a) Add a per-request flush unit, and make the scheduler flush at request end. (b) Change the spec sentence
  to the unit the scheduler flushes today. Read `flushScheduler.ts` before choosing the wording.

### Callsite module attribution name

- Spec says: "**Dual module references**: `callsiteModule` for row 0's git_sha/package_name/package_file, `module` for
  rows 1+" (`specs/lmao/01j_module_context_and_spanlogger_generation.md:862`). The field is
  `_callsiteModule?: OpMetadata` at `:163`.
- Code does: no `callsiteModule` symbol under `packages/lmao`. The equivalent is `callsiteMetadata`. It is documented at
  `packages/lmao/src/lib/bufferStrategy.ts:63` as the metadata for where `span()` was called (row 0). It is a parameter
  at `packages/lmao/src/lib/JsBufferStrategy.ts:83-85` and `packages/lmao/src/lib/spanBuffer.ts:1119-1121`. `opMetadata`
  covers rows 1+ (`spanBuffer.ts:1147-1148`). `module` and `callee_package` read `_opMetadata` at
  `packages/lmao/src/lib/spanContext.ts:1093-1098`.
- Options: (a) Rename `callsiteMetadata` to `callsiteModule` in the code, and add the `_callsiteModule` field that
  `:163` names. (b) Change `:163` and `:862` in the spec to `callsiteMetadata`. The row 0 and rows 1+ split is the same
  in both.

### Stats fields on `OpMetadata` (`sb_*`)

- Spec says: the fields `sb_capacity`, `sb_totalWrites`, `sb_overflows`, and `sb_totalCreated`
  (`specs/lmao/01j_module_context_and_spanlogger_generation.md:157-160`). The class fields are at `:302-305`.
- Code does: no hits for `sb_totalCreated` or `sb_totalWrites` under `packages/lmao`. The counts live on the SpanBuffer
  class: `packages/lmao/src/lib/spanBuffer.ts:1119` describes that class as having "static schema + stats". The flush
  writes `totalWrites`, `spansCreated`, and `capacity` (`packages/lmao/src/lib/arrow/capacityStats.ts:108`).
- Options: (a) Add the four fields to `OpMetadata`, and update them at each write. (b) Change the spec to the stats the
  code keeps on the SpanBuffer class. Remove the four `sb_*` fields from the `OpMetadata` list.

### `scope()` prefill

- Spec says: "`scope(attributes) { this._prefillRemainingCapacity(attributes);`"
  (`specs/lmao/01j_module_context_and_spanlogger_generation.md:605-606`). The method signature is at `:643`:
  `scope(attributes: Partial<Schema>): void;`.
- Code does: `packages/lmao/src/lib/codegen/spanLoggerClosureMaterializer.ts:467-472` defines the `scope` getter.
  `:474-489` (`_setScope`) builds a new frozen object from `buf._scopeValues`. `:491` (`_prefillScopedAttributesOn`) is
  the prefill function. No hits for `_prefillRemainingCapacity` under `packages/lmao`.
- Options: (a) Implement `_prefillRemainingCapacity`. `scope()` writes its values into rows that are reserved and not
  yet written, so later rows need no per-row copy. (b) Change the spec to what `_setScope` and
  `_prefillScopedAttributesOn` do today.

### `SpanContext` type and `SpanFn` overloads

- Spec says: "`type SpanContext<Schema, Deps, FF, Extra = {}> = {`"
  (`specs/lmao/01j_module_context_and_spanlogger_generation.md:622`). Also "`// SpanFn type with 6 overloads`" (`:649`).
- Code does: `packages/lmao/src/lib/spanContext.ts:595` declares `SpanContextInstance<Ctx extends OpContext>`. The
  `span()` dispatcher at `:906-914` names `span0` through `span8` (`:909`) and calls `self.span8(` at `:1049`. The code
  has nine arity members, not a six-overload type.
- Options: (a) Rewrite the spec sketch (`:622-624` and `:649-652`) to `SpanContextInstance<Ctx extends OpContext>` and
  its nine members. (b) Reduce the code to six overloads: three with a line number, and three without, as the spec
  describes.

### Generator names and signatures

- Spec says: `compileTagOperations(logSchema)` (`specs/lmao/01j_module_context_and_spanlogger_generation.md:427`),
  `createSpanLoggerClass(attributeNames, logSchema)` (`:430`), and
  `generateFixedPositionWriterClass(methodNames: string[], columnNames: string[], schema: LogSchema)` (`:458`). The base
  class `BaseTagWriter` appears at `:483`, `:495`, `:506`, and `:536`.
- Code does: `packages/lmao/src/lib/codegen/fixedPositionWriterGenerator.ts:149` declares
  `generateFixedPositionWriterClass(schema, position, className, ...)`.
  `packages/lmao/src/lib/codegen/spanLoggerGenerator.ts:743` declares `createSpanLoggerClass(schema, ...)`. No hits for
  `compileTagOperations`, `compilePrefixedTagOperations`, or `BaseTagWriter` under `packages/lmao`.
- Options: (a) Rename and re-sign the code to the spec's names and parameters. Add the two `compile*TagOperations`
  functions and the `BaseTagWriter` base class. (b) Rewrite the spec sketch (`:427-458` and `:483-536`) to the names and
  parameters `codegen/` has today.

### Span lifecycle API (`_invoke`, `writeSpanOk`, `writeSpanException`)

- Spec says: `async _invoke(` (`specs/lmao/01j_module_context_and_spanlogger_generation.md:351`). It creates the buffer
  with `createChildSpanBuffer(parentBuffer, callsiteModule, this.module, spanName)` (`:363`) and
  `createSpanBuffer(callsiteModule, this.module, spanName, traceRoot)` (`:364`). It calls `buffer.writeSpanStart()` with
  no name (`:383`), `writeSpanOk()` (`:410`), and `writeSpanException(error)` (`:413`).
- Code does: no hits for `writeSpanOk`, `writeSpanException`, or `_invoke` under `packages/lmao`.
  `packages/lmao/src/lib/spanContext.ts:233` declares `writeSpanStart(buffer, spanName)`, which takes a name. The span
  end is `writeSpanEnd(buffer, result)` at `packages/lmao/src/lib/spanContext.ts:559`. The start is written at
  `packages/lmao/src/lib/tracer.ts:741` and `packages/lmao/src/lib/spanContext.ts:1199`. The buffer factories are
  `packages/lmao/src/lib/JsBufferStrategy.ts:73` (`createSpanBuffer(`) and `:83` (`createChildSpanBuffer(`). Their
  parameters differ from the spec's.
- Options: (a) Rewrite the spec's `_invoke` sketch (`:351-413`) to the code's split: the start at `tracer.ts:741`, and
  the end at `spanContext.ts:559` with the result passed in. (b) Change the code to the spec's names and signatures: add
  `_invoke`, `writeSpanOk`, and `writeSpanException`, and make `writeSpanStart` take no name.

### Wiring with `appDeps`

- Spec says: "`const appDeps = {`" with `http: httpLib.prefix('http')` and `retry: retryLib.prefix('http_retry')`
  (`specs/lmao/01j_module_context_and_spanlogger_generation.md:811-813`).
- Code does: `packages/lmao/src/lib/defineOpContext.ts:272-283` documents
  `deps: { http: httpOps.prefix('http'), auth: authOps.prefix('auth') }`. `prefix` is defined at
  `packages/lmao/src/lib/opContext/createOpGroup.ts:273` and `:366`. No `use(` call exists under
  `packages/lmao/src/lib/opContext/`.
- Options: (a) Keep the `deps` object form. Change `01j:811-813` to match `defineOpContext.ts:272-283`. (b) Add the
  `.use(...)` injection form (see the next decision), and keep the `appDeps` form in the spec.

### Module-level metadata for source attribution

- Spec says: "OpMetadata - per-op configuration and source attribution, one per `defineOp`"
  (`specs/lmao/01j_module_context_and_spanlogger_generation.md:8-9`). Also "OpMetadata (class) - ONE per defineOp"
  (`:149`).
- Code does: `packages/lmao/src/lib/opContext/types.ts:146` (`OpContextConfig`) has no metadata field. The per-op
  metadata parameter is at `:355`, in `defineOp(...)`. No metadata hits in `packages/lmao/src/lib/defineOpContext.ts`.
  Attribution is per op only.
- Options: (a) Keep per-op attribution only. The code matches the spec as written. (b) Add an optional module-level
  default to `OpContextConfig` (`types.ts:146`). Ops without their own metadata use it. Then restore the module-level
  text in the spec.

### Tracer span-end write for fluent results

- Spec says: "If yes: Entry type already written, Tracer doesn't overwrite"
  (`specs/lmao/01l_op_context_pattern.md:403`). "If no (plain Result): Tracer writes `span-ok` to row 1" (`:404`). The
  gap entry cited `:405`. The current text is at `:403-404`.
- Code does: `packages/lmao/src/lib/tracer.ts:806-830` (`_executeResultWithContext`) calls `writeSpanEnd` at `:830`, and
  on the promise path at `:810`. `writeSpanEnd` (`packages/lmao/src/lib/spanContext.ts:559-569`) writes span-ok or
  span-err for every `Ok` or `Err`. It has no check for an entry type already written.
- Options: (a) The tracer checks whether a fluent result already wrote row 1, and skips the span-end write if it did.
  (b) Change `:403-404` to the behavior the code has: the tracer always writes the span-end row.

### `forContext()` and `.use()` injection

- Spec says: "`const authRoot = authModule.prefix('auth').use();`" (`specs/lmao/01m_lambda_routing_example.md:359`).
  "`const httpRoot = httpModule.prefix('http').use({`" (`:364`). The bullet at `:405` reads
  `httpModule.prefix('http').use({ auth: authRoot })`. The call `lambdaRoot.forContext({` is at `:393`.
- Code does: `prefix` is defined at `packages/lmao/src/lib/opContext/createOpGroup.ts:273` and `:366`. No `use(` call
  exists under `packages/lmao/src/lib/opContext/`. `forContext` is defined only on the flag evaluator
  (`packages/lmao/src/lib/schema/evaluator.ts:162`).
- Options: (a) Add `.use(deps)` to prefixed op groups, and `forContext()` to the root span context, as the spec shows.
  (b) Rewrite the example (`:359-374`, `:393`, and `:404-408`) to the `deps` object form that `defineOpContext` uses
  (`packages/lmao/src/lib/defineOpContext.ts:272-283`).

### Flag evaluator type parameters

- Spec says: "`interface FlagEvaluator<T extends LogSchema, FF extends FeatureFlagSchema, Env> {`"
  (`specs/lmao/01p_feature_flags.md:177`). Also
  "`forContext(ctx: SpanContextWithoutFf<T, FF, Env>): FeatureFlagEvaluator<FF, T, Env>;`" (`:201`).
- Code does: `packages/lmao/src/lib/schema/evaluator.ts:107` declares
  `FlagEvaluator<Ctx extends OpContext = OpContext>`. `FeatureFlagEvaluator` is at `:324`, `createFeatureFlagEvaluator`
  at `:347`, and `InMemoryFlagEvaluator` at `:365`.
- Options: (a) Change the code to the spec's three parameters: the schema `T`, the flag schema `FF`, and `Env`. (b)
  Change the spec to `FlagEvaluator<Ctx extends OpContext>`, and derive the schema, flags, and env from `Ctx`.
