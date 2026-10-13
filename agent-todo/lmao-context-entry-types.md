# lmao context and entry types: gaps between the specs and the code

The lmao specs (`specs/lmao/01h`, `01j`, `01l`, `01m`, `01n`, `01p`) state the context and entry-type design in present
tense. The items below list where the code does not do that yet. Items are independent unless stated. The flush item
depends on the per-op counters item. Work items come first. Items that needed a design choice are under Decisions; each
ends with the choice made, and a kept spec carries its own Work and Acceptance bullets.

## Warmed entry writes allocate nothing

- Spec: `specs/lmao/01h_entry_types_and_logging_primitives.md:553`: a warmed entry write allocates nothing while
  capacity remains. The same rule appears at `specs/lmao/01j_module_context_and_spanlogger_generation.md:16` and `:44`.
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

- Spec: `specs/lmao/01j_module_context_and_spanlogger_generation.md:739-752`: an ESLint rule named
  `eslint-no-capture-fluent` enforces the rule stated at `:741`: because `tag` writes entries, users must not capture
  the fluent interface.
- Code: no hits for `eslint-no-capture` or `no-capture-fluent` under `packages/lmao`.
- Work: Add the rule next to the custom ESLint rules the repo already has. Report a fluent chain whose result is stored
  in a variable, a property, or a closure. Accept a chain used in place on one expression.
- Acceptance: RuleTester cases pass. A stored chain is reported. An in-place chain is accepted.

## Kind tags for log templates and span names

- Spec: `specs/lmao/01j_module_context_and_spanlogger_generation.md:128`: `kindTags[i] = 1` means `LOG_TEMPLATE`, and
  `kindTags[i] = 2` means `SPAN_NAME`.
- Code: `packages/lmao/src/lib/vocabularyRegistry.ts:300` checks `kindTag !== 1 && kindTag !== 2` with numeric literals.
  No `LOG_TEMPLATE` or `SPAN_NAME` symbol exists under `packages/lmao/src/lib`. The two names appear only in
  `docs/transformer-optimization.md`.
- Work: Define the two constants in `packages/lmao/src/lib/vocabularyRegistry.ts` and export them. Replace the literals
  at `:300` and every other kind-tag comparison. Keep the numeric values unchanged.
- Acceptance: No numeric `kindTag` comparison remains outside the constants definition. A registry test rejects any kind
  tag other than the two named constants.

## Masked text fields

- Spec: `specs/lmao/01j_module_context_and_spanlogger_generation.md:669`: `url: S.text().masked('url')`. The same line
  appears at `specs/lmao/01m_lambda_routing_example.md:109` with the comment `// URL masking for PII`.
- Code: no hits for `masked(` under `packages/lmao`.
- Work: Add `masked(kind)` to the text schema builder. `S.text().masked('url')` must type-check, and the column
  definition must record the kind. The specs do not define the mask transform or the point where it runs. Take that rule
  from the spec that owns PII handling. If no such rule exists, stop and raise it as a spec question before writing the
  transform.
- Acceptance: The example schema type-checks. The column definition exposes the mask kind. A test writes a raw value to
  a masked column and checks that the raw value does not appear in the Arrow output.

## Per-op counters on `Op`

- Spec: `specs/lmao/01n_op_and_buffer_metrics.md:210`: the `Op` class keeps metrics counters that reset on each flush.
  The sketch at `:214-233` names `invocationCount`, `errorCount`, and `exceptionCount` (`:220-222`) and `periodStartNs`
  (`:232`). The span envelope (tracer for root spans, SpanContext for child spans) increments `invocationCount` at
  `:250`.
- Code: `packages/lmao/src/lib/op.ts:117-166` (`class Op`) has the fields `metadata`, `callsitePlan`, `fn`,
  `_resultTypes`, and `_opContextBinding`. It has no counter field. No hits for `invocationCount` under `packages/lmao`.
- Work: Add the counters from the sketch to `Op`. Increment `invocationCount` on each invocation. Increment `errorCount`
  and `exceptionCount` at the span-err and span-exception outcome points the tracer already has. Track `periodStartNs`
  as the sketch describes. Keep the counters on `Op`. Do not add a second store in the tracer.
- Acceptance: A test runs three invocations, one with an error and one with an exception. The counters read 3, 1, and 1.
  After a flush, all counters read zero.

## Per-op rows written at flush

- Spec: `specs/lmao/01n_op_and_buffer_metrics.md:291`: the per-op rows are written by the same flush that writes
  `period-start` and the `buffer-*` rows. The flush steps at `:304-319` write the rows per module, then reset the
  counters.
- Code: `packages/lmao/src/lib/arrow/capacityStats.ts:69` lists `period-start`, `buffer-writes`, `buffer-spans`, and
  `buffer-capacity`, and `:108` writes their values. That file writes no op rows. The op entry types are declared in
  `packages/lmao/src/lib/schema/systemSchema.ts:375-377`.
- Work: In the flush, after the buffer rows, write one op row per op for each op entry type. Read the counters, then
  reset them, in that order. This item depends on the per-op counters item above.
- Acceptance: After a flush with known counts, the Arrow output holds op rows with those values. The next flush reports
  zero for ops with no invocations.

## `defineOps` types a `this` it never binds

- Spec: `specs/lmao/01l_op_context_pattern.md` "Defining Ops": `defineOps` wraps each function with `defineOp`, and an
  op calls a sibling through the sibling's own `Op` value; nothing binds `this`.
- Code: `packages/lmao/src/lib/opContext/defineOp.ts:324` and `:332` type the definitions as
  `Defs & ThisType<OpsFromRecord<Ctx, Defs>>`, but `defineOpsImpl` wraps each function with `defineOp(name, def, …)` and
  the span machinery calls `op.fn(ctx, ...args)` (`spanContext.ts:1251`, `tracer.ts:604`), whose receiver is the `Op`
  itself, not the group, so a method that uses `this.sibling` type-checks and reads `undefined` at runtime.
- Work: type the definitions as `Defs & ThisType<void>` in both signatures, so a `this` reference in a definition is a
  type error (without any `ThisType`, a method's `this` would be the object literal and still type-check).
- Acceptance: a type-level test where a `defineOps` method reads `this.fetchUser` fails to compile.

## Decisions

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
- Decision: adopt the shipped shape. Eager row-1 writes through the captured writer state need no closure allocation per
  chained call and target the creating span by construction; 01h's Fluent Result API section now describes them.

### Whether results may defer closures

- Spec says: "They do not allocate deferred closure chains or retarget attributes to whichever span later receives the
  result." (`specs/lmao/01h_entry_types_and_logging_primitives.md:136`).
- Code does: no deferred closures. `packages/lmao/src/lib/result.ts:105-115` writes fluent values to the creating span's
  row 1 through `_state`. This matches `:136`. It contradicts the `applyTags` text at `:887`.
- Options: (a) Keep `:136`, and remove the `applyTags` design (see the previous decision, option b). (b) Keep the
  `applyTags` design, and rewrite `:136` to allow deferred closures. The code then follows the previous decision, option
  (a).
- Decision: adopt the shipped shape. This is option (a): `01h:136` stays, and the `applyTags` design is removed, as
  decided in the previous entry.

### Placeholder syntax and brace escapes

- Spec says: "Placeholders use `{fieldName}`; `{{` and `}}` encode literal braces."
  (`specs/lmao/01h_entry_types_and_logging_primitives.md:292-293`). Other passages use `{field}`: `:679`, `:1133`, and
  `specs/lmao/01j_module_context_and_spanlogger_generation.md:140`.
- Code does: `packages/lmao/src/lib/tracers/StdioTracer.ts:158` matches `/\{([A-Za-z0-9_]+)\}/g`. It has no `{{` or `}}`
  escape branch. The escape is missing under either name.
- Options: (a) Use `{fieldName}` in every passage: `01h:292`, `:344`, `:679`, `:1133`, and `01j:140`. Then add the
  escape branch. (b) Use `{field}` in every passage. Change `01h:292-293` and `:344`. Then add the escape branch.
- Work: `{fieldName}` is now the one placeholder name in the specs (`01h:292`, `:344`, `:679`, `:1138`, `01j:150`,
  matching `01o:439`). The specs also state that the stored template keeps its escapes as written and that readers
  decode `{{` and `}}` only when rendering (`01h:292-294`, `:344-346`, `01j:150-152`, `01o:439-444`). Code changes: (1)
  `packages/lmao-ttsc/plugin/driver/structuredlog.go:27-55` (`parseStructuredTemplate`) keeps validating escapes and
  collecting placeholders, but the record text registered at `:197` and in the structured path is the source literal,
  not the decoded `cooked` string. (2) `packages/lmao/src/lib/tracers/StdioTracer.ts:157-175` (`formatTemplateMessage`)
  parses the stored text with the same grammar: `{{` renders `{`, `}}` renders `}`, and `{fieldName}` renders that
  column's value.
- Acceptance: A transformed `log.info('{{userId}} is {userId}', { userId: 42 })` renders `{userId} is 42` in StdioTracer
  output, and its vocabulary record text is `{{userId}} is {userId}`. `'Parser emitted {{token}}'` renders
  `Parser emitted {token}` even when a `token` column holds a value.
- Decision: keep the spec. Without escapes a template cannot print a literal brace. Decoding only at render keeps a
  literal `{x}` distinct from a placeholder `{x}`, which decoded record text cannot do.

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
- Decision: adopt the shipped shape. The three shipped codes come straight from `SpanBufferStats` (`totalWrites`,
  `spansCreated`, `capacity`), which the self-tuning utilization already uses, and `01n:190-206` already lists them.

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
- Decision: adopt the shipped shape. One plan carries both the JS SpanBuffer class and the WASM layout template, so a
  backend-kind key term would split a plan both backends share.

### Flush unit ("one request")

- Spec says: "The flush unit is one request." (`specs/lmao/01j_module_context_and_spanlogger_generation.md:50-51`).
- Code does: no hits for `coalesc` under `packages/lmao`. The flush is triggered by
  `packages/lmao/src/lib/flushScheduler.ts:299-303`, which includes a memory-pressure path. The explicit growth error is
  already present. `packages/lmao/src/lib/wasm/wasmAllocator.ts:276-277` throws "WASM memory grew while an Arrow lease
  pinned the previous epoch". The pin counter is at `:272`, `:302`, and `:313`.
- Options: (a) Add a per-request flush unit, and make the scheduler flush at request end. (b) Change the spec sentence
  to the unit the scheduler flushes today. Read `flushScheduler.ts` before choosing the wording.
- Decision: adopt the shipped shape. The scheduler converts each registered root span tree to its own leased table and
  batches all trees since the previous flush, which is the unit `01j:53-56` now states; a per-request flush has no
  mechanism behind it.

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
- Decision: adopt the shipped shape. `_callsiteMetadata` names the value it holds (an `OpMetadata`), and the shipped
  name already appears in `01b1` and `01b5`.

### Stats fields on `OpMetadata` (`sb_*`)

- Spec says: the fields `sb_capacity`, `sb_totalWrites`, `sb_overflows`, and `sb_totalCreated`
  (`specs/lmao/01j_module_context_and_spanlogger_generation.md:157-160`). The class fields are at `:302-305`.
- Code does: no hits for `sb_totalCreated` or `sb_totalWrites` under `packages/lmao`. The counts live on the SpanBuffer
  class: `packages/lmao/src/lib/spanBuffer.ts:1119` describes that class as having "static schema + stats". The flush
  writes `totalWrites`, `spansCreated`, and `capacity` (`packages/lmao/src/lib/arrow/capacityStats.ts:108`).
- Options: (a) Add the four fields to `OpMetadata`, and update them at each write. (b) Change the spec to the stats the
  code keeps on the SpanBuffer class. Remove the four `sb_*` fields from the `OpMetadata` list.
- Decision: adopt the shipped shape. One `SpanBufferStats` object per SpanBuffer class is the single store the tuner and
  the flush read, so `OpMetadata` copies would be a second store to keep in sync.

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
- Decision: adopt the shipped shape. `setScope` keeps scope as a frozen object filled at Arrow conversion (`01i:41-47`),
  and `_prefillScopedAttributesOn` is needed only on the overflow slow path.

### `SpanContext` type and `SpanFn` overloads

- Spec says: "`type SpanContext<Schema, Deps, FF, Extra = {}> = {`"
  (`specs/lmao/01j_module_context_and_spanlogger_generation.md:622`). Also "`// SpanFn type with 6 overloads`" (`:649`).
- Code does: `packages/lmao/src/lib/spanContext.ts:595` declares `SpanContextInstance<Ctx extends OpContext>`. The
  `span()` dispatcher at `:906-914` names `span0` through `span8` (`:909`) and calls `self.span8(` at `:1049`. The code
  has nine arity members, not a six-overload type.
- Options: (a) Rewrite the spec sketch (`:622-624` and `:649-652`) to `SpanContextInstance<Ctx extends OpContext>` and
  its nine members. (b) Reduce the code to six overloads: three with a line number, and three without, as the spec
  describes.
- Decision: adopt the shipped shape. The public type is `SpanContext<Ctx extends OpContext>` with four `SpanFn`
  overloads (`opContext/spanContextTypes.ts:150-231`, `:284`); one bundled parameter replaces four and matches
  `01c:535`.

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
- Decision: adopt the shipped shape. One fixed-position generator serves row 0 and row 1, and prefixes ride a
  `RemapDescriptor` read at Arrow conversion, so no second prefixed writer class exists.

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
- Decision: adopt the shipped shape. The shipped envelope checks result ownership before `writeSpanEnd`, and an `Op`
  without span logic keeps one lifecycle in the tracer and SpanContext instead of a third copy on `Op`.

### Wiring with `appDeps`

- Spec says: "`const appDeps = {`" with `http: httpLib.prefix('http')` and `retry: retryLib.prefix('http_retry')`
  (`specs/lmao/01j_module_context_and_spanlogger_generation.md:811-813`).
- Code does: `packages/lmao/src/lib/defineOpContext.ts:272-283` documents
  `deps: { http: httpOps.prefix('http'), auth: authOps.prefix('auth') }`. `prefix` is defined at
  `packages/lmao/src/lib/opContext/createOpGroup.ts:273` and `:366`. No `use(` call exists under
  `packages/lmao/src/lib/opContext/`.
- Options: (a) Keep the `deps` object form. Change `01j:811-813` to match `defineOpContext.ts:272-283`. (b) Add the
  `.use(...)` injection form (see the next decision), and keep the `appDeps` form in the spec.
- Decision: adopt the shipped shape. `01j:722-737` now wires deps inside `defineOpContext`, the same form `01c:663-666`
  and `01l:24-29` use.

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
- Decision: adopt the shipped shape. This is option (a): attribution stays per op, and the transformer injects it as
  `defineOp`'s third argument (`01l:797-814`).

### Tracer span-end write for fluent results

- Spec says: "If yes: Entry type already written, Tracer doesn't overwrite"
  (`specs/lmao/01l_op_context_pattern.md:403`). "If no (plain Result): Tracer writes `span-ok` to row 1" (`:404`). The
  gap entry cited `:405`. The current text is at `:403-404`.
- Code does: `packages/lmao/src/lib/tracer.ts:806-830` (`_executeResultWithContext`) calls `writeSpanEnd` at `:830`, and
  on the promise path at `:810`. `writeSpanEnd` (`packages/lmao/src/lib/spanContext.ts:559-569`) writes span-ok or
  span-err for every `Ok` or `Err`. It has no check for an entry type already written.
- Options: (a) The tracer checks whether a fluent result already wrote row 1, and skips the span-end write if it did.
  (b) Change `:403-404` to the behavior the code has: the tracer always writes the span-end row.
- Decision: adopt the shipped shape. Fluent methods write only attributes, message, and line, so the tracer must still
  write the entry type; `01l:400-408` now says so, including the `TypeError` for a foreign result.

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
- Decision: adopt the shipped shape. Child spans already share the root context's `deps` (`01c:259`), so the deps object
  gives the shared instances without a `.use()` step; request-scoped values go through `trace()` overrides.

### Flag evaluator type parameters

- Spec says: "`interface FlagEvaluator<T extends LogSchema, FF extends FeatureFlagSchema, Env> {`"
  (`specs/lmao/01p_feature_flags.md:177`). Also
  "`forContext(ctx: SpanContextWithoutFf<T, FF, Env>): FeatureFlagEvaluator<FF, T, Env>;`" (`:201`).
- Code does: `packages/lmao/src/lib/schema/evaluator.ts:107` declares
  `FlagEvaluator<Ctx extends OpContext = OpContext>`. `FeatureFlagEvaluator` is at `:324`, `createFeatureFlagEvaluator`
  at `:347`, and `InMemoryFlagEvaluator` at `:365`.
- Options: (a) Change the code to the spec's three parameters: the schema `T`, the flag schema `FF`, and `Env`. (b)
  Change the spec to `FlagEvaluator<Ctx extends OpContext>`, and derive the schema, flags, and env from `Ctx`.
- Decision: adopt the shipped shape. One `Ctx` parameter matches `SpanContext<Ctx>` and removes the `ctx as SpanContext`
  casts the three-parameter sketch needed.

### Propagating a child's result (`Blocked` pass-through)

- Spec says: "`return result; // Pass through - the engine handles retry`" (`specs/lmao/01l_op_context_pattern.md:366`
  before this pass, and `:310` in the Result Class example, which also built results with `Result.ok(...)`).
- Code does: `packages/lmao/src/lib/tracer.ts:809` and `:829` call `result._assertOwner(ctx)`, which throws
  `SPAN_COMPLETION_OWNER_ERROR` for a result another context created (`packages/lmao/src/lib/result.ts:124-129`).
  `Result` is a type only (`result.ts:416`); there is no `Result.ok`. The shipped propagation is
  `return ctx.err(r.error)` (`packages/lmao/src/lib/opContext/types.ts:389-391`), as `01h:147-148` already states.
- Decision: adopt the shipped shape. Ownership is what keeps a foreign result from completing or retrying the receiving
  span, so `01l:289-315` and `:366-368` now rebuild the child's outcome with `ctx.err(result.error)` / `ctx.ok(...)`.

### Object-based tag API (`tag({...})` vs `tag.with({...})`)

- Spec says: "`(attributes: Partial<TAttributes>): TagWriter<TAttributes>;`" in a callable `TagWriter` interface, and
  `tag({ userId: 'user-123', ... })` in the examples (`specs/lmao/01h_entry_types_and_logging_primitives.md`, before
  this pass).
- Code does: `packages/lmao/src/lib/opContext/spanContextTypes.ts:78-83` types `TagWriter<T>` as the setters plus
  `with(attributes)`, and it is not callable. The generated writer is a plain class instance whose only field is
  `_state` (`packages/lmao/src/lib/codegen/fixedPositionWriterGenerator.ts:327-334`).
- Decision: adopt the shipped shape. A callable writer would need a function object per span in place of one class
  instance, and `with` is already reserved for the bulk setter; `01h:608-633`, `:676`, `:749-756`, `:794`, `:825`, and
  `:839` now use `tag.with({...})`.

### Timestamp of metric rows (period end vs period start)

- Spec says: "All metrics rows from the same flush share identical `timestamp` (the period end time)."
  (`specs/lmao/01h_entry_types_and_logging_primitives.md:425`). `specs/lmao/01n_op_and_buffer_metrics.md:115`, `:167`,
  and `:304` say the same, and `period-start` carries the period start in `uint64_value`, so duration is
  `timestamp - uint64_value`.
- Code does: `packages/lmao/src/lib/arrow/capacityStats.ts:120` writes `timestamps[row] = periodStartNs` for every row,
  so the period-start row reports a zero-length period. The scheduler passes no period start at all:
  `packages/lmao/src/lib/flushScheduler.ts:389` calls `convertSpanTreeToLeasedArrowTable(buffer, modules)` without one,
  and `packages/lmao/src/lib/convertToArrow.ts:1628-1631` then uses `0n`.
- Work: `FlushScheduler` keeps the period start (the previous stats flush time, or its start time) and passes both the
  period start and this flush's time down to `createCapacityStatsTable`. `capacityStats.ts:120` writes the flush time as
  `timestamp`, and `:108` keeps the period start as the `period-start` row's `uint64_value`. After writing, the
  scheduler sets the period start to this flush's time.
- Acceptance: Two consecutive stats flushes produce `period-start` rows whose `uint64_value` is nonzero and equals the
  previous row's `timestamp`. Each row's `timestamp - uint64_value` equals the elapsed period.
- Decision: keep the spec. With the period end in `timestamp` and the start in `uint64_value`, a row carries both
  bounds; the shipped rows carry only the start, and in production that start is `0n`.

### Prefix of a library's internal spans

- Spec says: "Prefix is applied at composition time, not authoring time."
  (`specs/lmao/01m_lambda_routing_example.md:472`). The trace at `:497-515` shows the HTTP library's spans, including
  `http/request` (`:508`), under `http_*` columns, and `:439` says HTTP and Lambda both write the same `auth_*` columns.
- Code does: `packages/lmao/src/lib/spanContext.ts:1198` gives a child buffer only the invoked op's own
  `callsitePlan.remapDescriptor`. A prefixed op therefore writes prefixed columns, but when it calls a sibling op
  through a module-local reference (`01m:167` and `:171`: `span('request', request, ...)`), the sibling writes
  unprefixed `method`, `url`, `status`, and `duration`. Deps pass from parent to child (`spanContext.ts:1214`), so
  `deps.auth` resolves to the application's `authModule.prefix('auth')` group everywhere in the trace, and auth spans
  write `auth_*`, as `01m:439` states. Separately, `prefix()` maps the group's whole effective schema
  (`packages/lmao/src/lib/opContext/createOpGroup.ts:279` and `:290`), so the type-level schema also lists
  `lambda_method` and similar columns that no buffer writes at run time.
- Work: In `_spanPre` (`spanContext.ts:1198`), when the invoked op's plan has no `remapDescriptor` and its schema is the
  parent buffer's schema, the child takes the parent buffer's `_remapDescriptor`. An op that has its own descriptor, or
  a different schema, keeps its own.
- Acceptance: An application wires `httpModule.prefix('http')` and runs `GET`, which calls the module-local `request`
  op. The Arrow output carries `http_method` and `http_url` on the `request` span and has no unprefixed `method` or
  `url` column. Auth spans reached through `deps.auth` still write `auth_*`.
- Decision: keep the spec. A column's name must not depend on whether library code reaches its own op through a local
  reference or through the prefixed group.

### Columns contributed by a prefixed library's own deps

- Spec says: a mapped group contributes only its own schema fields, and the columns of a library's deps come from the
  groups the application wires for them, because every span sees the `deps` of the Tracer's context
  (`specs/lmao/01e_library_integration_pattern.md`, "How Nested Libraries Compose"). Before this pass 01e was silent and
  its diagram attributed the auth columns to the HTTP library's own wiring.
- Code does: a context's ops carry its merged schema, the context's own fields plus every dep's contributed fields
  (`packages/lmao/src/lib/defineOpContext.ts:154-191`, `:208`, `:227-235`; `createOpGroup` receives it at
  `packages/lmao/src/lib/opContext/defineOp.ts:369`). `prefix()` and `mapColumns()` map that whole schema
  (`packages/lmao/src/lib/opContext/createOpGroup.ts:279`, `:282`, `:290`, `:304`, `:308`), so `httpOps.prefix('http')`
  over a library whose own deps contribute `cache_*` adds `http_cache_operation`, `http_cache_key` and `http_cache_hit`
  to the application's type-level schema and to the descriptor's columns. At run time the cache spans use the
  application's `deps.cache` (children share the root's deps, `spanContext.ts:1214`, `tracer.ts:291` and `:749`) and
  write `cache_*`, so Arrow Pass 0 also emits the three `http_cache_*` columns, always null
  (`packages/lmao/examples/library-integration.ts:41` and `:60-61` show the shape).
- Options: (a) map only the context's own fields, so the type-level schema and the runtime columns agree; (b) keep
  mapping the merged schema and make library deps resolve per Op, so a library's own wiring names its deps' columns; (c)
  keep both and document the always-null columns.
- Work: `defineOpContext` keeps the context's own user fields (its `logSchema` fields, without dep contributions) next
  to the merged schema and hands them to the OpGroup that `defineOps` creates. `prefix()` and `mapColumns()` on both
  `OpGroupImpl` and `MappedOpGroupImpl` (`createOpGroup.ts:273-323`, `:366-412`) build their mapping, their
  `RemapDescriptor` and their contributed schema from those fields only. The contributed-schema type parameters
  (`PrefixedSchema`, `MappedSchema` in `packages/lmao/src/lib/opContext/opGroupTypes.ts`) take the own-fields type in
  place of `Ctx['logSchema']['fields']`.
- Acceptance: a Bun test built like `examples/library-integration.ts` converts the trace and finds `http_status`,
  `http_method`, `http_url`, `cache_operation`, `cache_key` and `cache_hit`, and no `http_cache_*` column. A type test
  in the same file has `// @ts-expect-error` on `ctx.tag.http_cache_key(...)` inside an application op.
- Decision: keep the spec, option (a). Deps resolve once at the root and 01c makes children share them, so the
  application's wiring is what names a dep's columns. A group that also claims those columns under its own prefix
  declares columns no span writes.
