# Composed StateBus consumer contract

The value-level API composes independently owned libraries without ambient module augmentation.
The legacy API remains available. Both APIs use the same capacity-retaining dispatch queue; this is
not a copied application runtime. State remains in scalar and keyed `@tldraw/state` atoms.

## Public imports

```ts
import {
  defineCapability, defineLibrary, mountLibrary, composeLibraries,
  ManualScheduler, microtaskScheduler,
  defineEffect, bindEffect, publishEffectOutcome,
  captureCheckpoint, recordScenario, replayScenario,
  captureEffectOutcome, decodeEffectOutcome, classifyScenario,
  type ValueCodec, type StateReader, type EffectDefinition,
} from '@smoothbricks/statebus-core';
import {
  createStateBusReact, createLibraryReact,
  useStateValue, useKeyedState, useEventPublisher, createSelectionHook,
} from '@smoothbricks/statebus-react';
import {
  reduceComposedLoader, composedLoaderChannel,
  initialLoadState, type LoadState, type LoaderEvent,
} from '@smoothbricks/statebus-data-loader';
import { bindComposedQueryLoader } from '@smoothbricks/statebus-tanstack-query';
```

`StateBusComposition.createRuntime()` creates a `ComposedRuntime`. Its principal methods are
`read`, `readKeyed`, `publisher`, `publish`, `listen`, `binding`, `selection`, `acquire`, `flush`,
`manage`, and `dispose`. `interestSource.snapshot()` and `interestSource.subscribe()` expose exact
addresses, not aggregate property demand. `ScalarHandle`, `KeyedHandle`, `ResourceHandle`,
`EventHandle`, `Capability`, `CapabilityBinding`, `LibraryDefinition`, `MountedLibrary`,
`ReducerState`, `StateInterestHandle`, `RecordedScenario` and `EffectOutcome` are exported types.

## Independent ownership, one hosted runtime

A library declares local names inside `defineLibrary({ name, requires, setup })`. The setup function
receives a `LibraryScope` with `scalar`, `keyed`, `event`, `command`, `reduce`, and `require` methods.
`scalar` takes a pure initial-value factory. `keyed` takes a pure ID-to-default factory and an
`idCodec`, preserving branded IDs through the public handle and loader binding.

`scope.command(name, admit, options)` requires a pure boolean admission function. It may change
only its owner's state through the provided `ReducerState`. `false` records a refused command and
prevents its effect from executing. `scope.reduce(event, reducer)` registers ordinary pure state
transitions; another library may observe a public event but still only write its own state.
The reducer's public context deliberately contains no bus, publisher or effect installer.

A capability is a typed composition input, not a DI or tracing container. For example:

```ts
const allowed = defineCapability<boolean>('inventory.can-adjust');
// inventoryLibrary declares requires: [allowed] and reads scope.require(allowed) during setup.
const embedded = mountLibrary(inventoryLibrary, 'inventory', [allowed.provide(true)]);
const secondary = mountLibrary(inventoryLibrary, 'secondary', [allowed.provide(false)]);
const host = mountLibrary(hostLibrary, 'host');
const app = composeLibraries(embedded, secondary, host);
const runtime = app.createRuntime();
```

Each mount runs the library's setup to obtain its own state/event handles, reducer registrations,
effect definitions and metadata. Composition rejects duplicate owners, missing or incompatible
capability tokens, duplicate bindings and reducers or handle-valued requirements referencing an
uncomposed library. Provider and
boundary installation reject foreign mounts/handles. Effect command/result handles and loader
state/event handles must have the same owner. Binding an effect slot twice in one runtime fails.
Capability value types are checked by TypeScript; validate untrusted configuration before `provide`.

Resolved names live in immutable metadata. Reads, publications and React renders do not build
prefixes, stringify resource IDs, or reconstruct schemas. Wire addresses are exposed for existing
loader interoperability; consumers use handles, `at(id)` and the binding's typed `resourceId` rather
than inventing namespace strings. Keyed defaults are materialized lazily. Reading a default does
not turn it into a persistent state change or alter checkpoint equivalence.

Composition does not create a bus. Every application mount, test or story calls `createRuntime`
separately. A host includes the public library mount/reducers in its composition; the public library
never imports the host's private state or event types.

## The same React connector standalone and embedded

Create `createLibraryReact(inventoryLibrary)` in the library connector module. Its `Provider`
receives a `MountedLibrary` and its `useLibrary()` hook returns that mount's typed exports. It
selects the existing application runtime; it never creates a library store.

```tsx
const InventoryUI = createLibraryReact(inventoryLibrary);

function InventoryConnector() {
  const model = InventoryUI.useLibrary();
  const id = useStateValue(model.selected);
  const inventory = useKeyedState(model.inventory, id);
  const incoming = useKeyedState(model.incoming, id);
  const emit = useEventPublisher(model.adjust);
  return <InventoryView inventory={inventory} incoming={incoming} onAdjust={emit} />;
}

const App = createStateBusReact(app);
root.render(
  <App.Provider runtime={runtime}>
    <InventoryUI.Provider mount={embedded}>
      <InventoryConnector />
    </InventoryUI.Provider>
  </App.Provider>,
);
```

A standalone app uses exactly that connector and library provider under a composition containing
only the library mount. The host uses the same connector under its full composition. Providers
check compatibility before descendants read state.

Subscriptions use `useSyncExternalStore`, per-atom signals and exact demand leases. Changing the
runtime, handle or resource ID replaces the binding and releases the previous lease. Unchanged
publishers/subscriptions retain identity; unrelated atoms do not rebuild computed snapshots.
`createSelectionHook(name, select, interests)` captures primitive props only when they change and
uses the existing computed-signal engine. It does not use an application-wide snapshot atom.
For a reusable connector, use the library-bound variant so switching between two mounts in the
same runtime also replaces the computed binding and its exact resource leases:

```ts
const useInventorySummary = InventoryUI.createSelectionHook(
  'inventory.summary',
  (state, model, props: { id: ShelfId }) => ({
    inventory: state.readKeyed(model.inventory, props.id),
    incoming: state.readKeyed(model.incoming, props.id),
  }),
  (model, props) => [model.inventory.at(props.id), model.incoming.at(props.id)],
);
```

The generated hook receives the current library exports from its provider. It does not close over
a standalone mount or require a host-specific schema. Runtime, mount and resource changes are all
part of binding identity; unchanged calls reuse their committed computed signal.

The runtime is explicitly owned outside the provider. Unmount the React root, then dispose the
runtime at the actual application lifecycle boundary. Do not dispose a shared runtime in an
unconditional React effect cleanup: StrictMode intentionally exercises cleanup and resubscription.
Unmounting a screen releases interest, not application data. Runtime disposal also closes remaining
subscriptions and reports terminal zero demand.

## Exact demand to the existing loader and QueryClient execution boundary

Inside the library setup, register the existing lifecycle reducer for each resource separately:

```ts
reduceComposedLoader(scope, inventory, inventoryEvents);
reduceComposedLoader(scope, incoming, incomingEvents);
```

Inside the application's execution boundary:

```ts
const binding = composedLoaderChannel(runtime, model.inventory, model.inventoryEvents);
const stop = bindComposedQueryLoader(binding, {
  queryClient, requestId, now, failure, fingerprint,
  query: (request) => ({
    queryKey: [model.inventory.metadata.key, binding.resourceId(request.interest)],
    execute: (execution) => existingReadOperation(request, execution),
  }),
});
```

The binding contains its runtime, so an executor cannot accidentally attach an already-bound
channel to a different runtime argument. `resourceId` returns the handle's branded ID. The
existing `installTanStackQueryLoader`, `reduceLoadState`, progress, demand, byte-stream and timer
helpers are reused, not reimplemented. QueryClient is not React state authority.

Exact interest includes the state declaration and resource ID, terminal zero, and a snapshot for
providers installed after subscribers. Repeated subscribers and StrictMode churn coalesce into
correct final counts. Existing QueryObserver leases and cancellation grace preserve an in-flight
request while another observer needs it; releasing a screen never clears an entire QueryClient.
The application owns the client and its query-key sharing policy. Use independent clients for
independent application execution boundaries unless sharing is deliberate.

The existing loader's `LoadRequest` survives QueryClient retries. Domain fingerprints, permissions,
stale-result policy and failure classification remain application inputs. To adapt a LMAO read Op,
call the existing `context.span(name, op, ...)` from `execute`. QueryClient represents query failure
with a rejected Promise; preserve the Op's original error at that boundary and classify it through
the application's `failure` function. Do not invent a parallel result or tracing hierarchy.

## Admitted commands, plans, LMAO operations, outcomes

`defineEffect({ command, result, plan, decode, codec })` declares a pure planner and decoder.
`bindEffect(runtime, definition, { execute, failure, capture })` injects execution. The ordering is:

1. Reduce the entire command wave, remembering admission per publication.
2. After successful reduction, plan from batch-final read-only state. Refused commands never plan.
3. Execute the injected operation once for each admitted, currently unexecuted request ID.
4. Decode the typed outcome and publish the result event into a successor wave.

The composed API uses atom transactions: a failed reducer wave rolls back its atom writes and
starts no effects. Interest acquisition/final-zero facts describe live subscriptions, so they are
redelivered in a successor wave after rollback; failed domain commands are not retried. Capture
observer failures are reported without suppressing already-admitted effects. The legacy API keeps
its previous non-transactional reducer semantics.
Programmer errors in planners/decoders go to the runtime's error reporter. Execution rejection is
mapped by the supplied typed `failure` function; no Promise rejection is left unobserved.

The external consumer includes this binding typechecked against **built LMAO exports**:

```ts
function bindInventoryOperation<Ctx extends OpContext>(
  runtime: ComposedRuntime,
  definition: EffectDefinition<Adjust, AdjustPlan, Result<number, string>, AdjustResult>,
  context: SpanContext<Ctx>,
  operation: Op<Ctx, [AdjustPlan, AbortSignal], number, string>,
) {
  return bindEffect(runtime, definition, {
    execute: (plan, { signal }) => context.span('inventory.adjust', operation, plan, signal),
    failure: () => new Err('unexpected-operation-rejection'),
  });
}
```

`Op`, `OpContext`, `Result`, `SpanContext` and `Err` above come from `@smoothbricks/lmao`.
The application supplies the existing tracing context/operation and owns that context's lifetime.
No fallback tracer or second dependency-injection system is installed. `Result` flows unchanged
into the pure decoder. Captured results may reconstruct detached `Ok`/`Err` values for decoding;
never use a restored/foreign result to complete a live LMAO span.

`EffectBinding.cancel(requestId)` retains the plan's request-ID type, aborts client work/observation,
and suppresses late publications. Disposal performs the same cleanup, including async iterator
`return`. **Cancellation is not server rollback.** A server mutation can commit after the client
stops observing it. The application must reconcile that fact rather than emit a fictional undo.
There is no automatic mutation retry; the application's existing operation owns retries and must
preserve the admitted request ID.

## Deterministic scenarios and replay

Use `new ManualScheduler()` for tests and `microtaskScheduler` (the runtime default) for production.
Both use the same complete-wave/successor-wave dispatch engine. `runtime.flush()` is also available
for an explicit synchronous test boundary.

`ValueCodec<T>` has `schema`, positive integer `version`, `encode` and `decode` hooks. `encode` must
return an owned portable value; `decode` validates stored input and preserves branded/domain types.
Declarations may also supply `classify(value)` returning `public`, `sensitive` or `excluded`.
These hooks run at capture/replay boundaries, never during ordinary reads or publication.

```ts
const recorder = recordScenario(runtime, { maxEvents: 1024 });
// Drive the real library through events and controlled operations.
const scenario = recorder.snapshot();
const replay = replayScenario(app, scenario);
```

A recorder takes one explicit checkpoint and retains successful event waves up to `maxEvents`.
It never clones the whole application state on publication, and never retains the dispatcher's
recycled queue arrays. Reading defaults is omitted from checkpoints. Once old waves are evicted,
`complete` becomes false and replay refuses the incomplete history; `reset()` creates a fresh
checkpoint at an explicit quiescent boundary. This is an **event-count bound**, not a claim that
arbitrary application payloads have a fixed byte size. Codecs own payload-size limits.

Recording requires codecs up front rather than silently dropping unsupported declarations.
`captureCheckpoint` and `replayScenario` check schema/version compatibility. The scenario envelope
is a typed contract; validate envelopes loaded from untrusted storage before passing them in.
Individual state/event/outcome values are decoded through their declared codecs. The runtime does
not treat arbitrary JSON as validated application data.

Replay creates an isolated `mode: 'replay'` runtime. `bindEffect` and `bindComposedQueryLoader` do not
install execution subscriptions in that mode. Events reproduce state without QueryClient or other
production I/O. `captureEffectOutcome` and `decodeEffectOutcome` retain/version the operation input
and outcome; `publishEffectOutcome` applies the very same pure decoder without executing an Op.

`classifyScenario(scenario, include)` supports policy-filtered support exports. Removing anything
makes the result non-replayable. Classification is not automatic field-level redaction: sensitive
fields must be removed by the application's codec/policy before an export is shared. A DevTools,
Help or diagnostics UI is not part of this API.

## Executable external consumer and verification

Run the repository target:

```sh
nx run statebus-core:verify-packages
```

It is also a dependency of `statebus-core:test`. The verifier packs the real packages using the
release export-manifest transform, installs tarballs and exact direct dependency versions into a
temporary external consumer, and retains its resolved lockfile. Consumer overrides ensure every
transitive workspace dependency also uses those same tarballs, not an unpublished registry version.
Strict declaration checks (including negative branded-ID/capability tests) retain `skipLibCheck: false`.
Every relevant package resolution must point inside the installed `node_modules/.../dist` directory.
The platform and composed consumers have separate checked-in TypeScript configurations.

The executable example lives in `scripts/consumer/composed/` as ordinary `.ts` files, with its own
`tsconfig.json`. The verifier copies the consumer project unchanged outside the workspace. Its codecs are generated from
the actual public types by the normal ttsc/Typia compiler, not hand-maintained loader schemas. The
edge cases include a JSON-transported checkpoint/event envelope validated before no-I/O replay.
Both suites run against built package exports under Node and Bun. They use synthetic inventory resources,
real ReactDOM/StrictMode, real QueryClient, real LMAO `Result` values, and controlled operations.
The LMAO `Op` binding is typechecked; no production service is contacted by the fixture.

The scenarios cover subscription-triggered keyed loading and mutation; standalone/hosted connector
reuse; two mounts/two runtime instances; duplicate admission; late old-resource outcomes; exact
interest and grace; stable publishers and computed subscriptions; cleanup, cancellation, iterator
return, async failure, failed reductions; count-bounded support capture; and equivalent no-I/O replay.

### Support classification

| Requirement | Status in this change |
| --- | --- |
| Optimized dispatch, exact-interest registry/batching, loader lifecycle/progress, QueryObserver/grace | Already supported; reused |
| Value-level owned libraries, typed capabilities/handles, composition validation | Newly implemented |
| Composition/library-bound React providers and scalar/keyed/computed/event hooks | Newly implemented |
| Composed bindings to the existing loader and QueryClient helpers | Newly implemented |
| Admitted effects, injected typed LMAO-compatible execution, pure outcomes/decoders, disposal | Newly implemented |
| Manual/production scheduler equivalence, versioned checkpoints/events/outcomes, bounded replay | Newly implemented |
| Browser navigation core/driver package | Already supported; unchanged by this integration |
| Application-specific permissions, fingerprints, stale decisions, failure classes and actual screen wiring | Application-owned; not implemented by this generic example |
| Expo, collaboration, generic forms and diagnostics/Help UI | Unsupported here; not required for this consumer contract |

These source changes do not publish package versions or migrate a downstream screen by themselves.
The application must replace its old store/query boundary, bind its real operations, and verify its
own checkpoint/event stream before calling that screen fully migrated.

## Rolling local journals

`recordJournal` adds a replayable rolling window without replacing composition, effects or the finite
`recordScenario` API. It is **lossless local capture, not a sanitized support export**. Import
`recordJournal`, `JournalRecorder`, `JournalLimits`, `JournalSnapshot`, `JournalCapture`,
`JournalOutcome`, `JournalOutcomeReceipt`, `JournalUsage` and `JournalRefusal` from
`@smoothbricks/statebus-core`.

```ts
const journal = recordJournal(runtime, {
  maxEvents: 512,
  maxEventBytes: 512 * 1024,
  maxOutcomes: 64,
  maxOutcomeBytes: 256 * 1024,
  maxCheckpointBytes: 4 * 1024 * 1024,
  maxCaptureBytes: 5 * 1024 * 1024,
});
const effect = bindEffect(runtime, model.effect, {
  execute,
  failure,
  capture: (outcome) => {
    const receipt = journal.captureOutcome(model.effect, outcome);
    // The application may surface a refusal without cancelling the admitted operation.
    if (receipt.kind === 'refused') reportCaptureRefusal(receipt.reason);
  },
});
const result = journal.snapshot();
if (result.kind === 'recorded') {
  const replay = replayScenario(composition, result.capture.scenario);
  // Read/compare replayed state, then dispose this independently owned runtime.
  replay.dispose();
} else {
  reportCaptureRefusal(result.reason);
}
```

All limits are optional overrides of bounded defaults: 1,024 events / 1 MiB of event waves,
256 outcomes / 1 MiB of outcome history, an 8 MiB checkpoint and a 12 MiB cold capture.
Resolved limits are available as `journal.limits`. Start recording at a quiescent runtime boundary;
`dispose()` releases the journal observer, replay cursor and its change trackers. Runtime disposal
also stops its journal. Already retained snapshots remain independent and immutable.

### What advances and what is bounded

The recorder takes an initial checkpoint once. When count or encoded-byte pressure evicts a **whole
successful wave**, an isolated instance of the existing replay runtime applies that wave to the
checkpoint cursor. No operations, query loaders or imperative event listeners are installed there.
Only cells written by that replayed wave are encoded into the checkpoint index; unrelated keyed
cells are not traversed or encoded again. A checkpoint's total bound is checked after the atomic
wave, so moving a value between keys is not refused merely because addition preceded deletion.

Live reads are unchanged. Live writes do not collect capture addresses; the small write-tracking
branch applies only to a replay cursor with tracking installed. The journal's wave and outcome rings
retain capacity but clear removed payload slots; no surviving suffix is repeatedly shifted. The
finite recorder's behavior is unchanged: after eviction it still reports `complete: false` and
`replayScenario` still refuses it. A short finite recording is not proof of rolling acceptance.

The returned `JournalUsage` reports **UTF-8 JSON transport bytes**, not a heap-size estimate:

| Limit | Counted representation |
| --- | --- |
| `maxEvents` | Sum of event counts in retained whole waves |
| `maxEventBytes` | The retained `RecordedWave[]`, including brackets and commas |
| `maxOutcomes`, `maxOutcomeBytes` | Retained `JournalOutcome[]` count and its complete JSON array bytes |
| `maxCheckpointBytes` | The complete `StateCheckpoint`, including schema and declaration framing |
| `maxCaptureBytes` | The complete `JournalCapture` envelope returned by `snapshot()` |

Section sizes are maintained incrementally. The cold export bound is checked from those sizes and
small envelope framing **before** constructing the checkpoint arrays. No whole-application clone
or JSON encoding occurs on ordinary publication or checkpoint advancement. Payload encoding/copying
occurs only while a journal is installed, at its explicit capture boundary.

Codecs must produce JSON-portable representations with lossless application-owned decode semantics.
The journal detaches and freezes that representation; it never retains a mutable dispatch scratch
array, an operation's plan object, or a codec's reusable output object. As with JSON transport,
undefined object properties are omitted and codecs must handle that deliberately. Cycles and other
JSON encoding failures refuse capture. The library cannot certify an arbitrary user-supplied codec's
round-trip fidelity: use generated validators and domain round-trip properties.

These limits do **not** bound application state, the replay cursor's live atom/cache footprint,
codec execution/temporary allocation, or snapshots retained by the caller. An application must
bound a single payload before its codec constructs it. The current implementation reports no
allocation bytes/op, universal heap bound, GC-pause improvement or V8 optimization-status claim.

### Refusal, outcomes and causal positions

Oversized initial/growing checkpoints, oversized indivisible waves and oversized outcomes stop the
journal with an explicit `kind: 'refused'` result. It never keeps a partial wave or labels a missing
history replayable. Codec/replay failures likewise stop capture, report a programming error through
the runtime error channel, and return a portable refusal without the raw exception. Production
reduction and admitted operations continue independently. Start a new journal explicitly after
correcting a refusal. A `capture-limit` refusal is export-only: it leaves local recording active.

`checkpointWave` identifies the last evicted domain wave (or the initial checkpoint position);
`lastWave` identifies the last successful live wave, including interest-only waves. Wave positions
are retained, not renumbered. `runtime.waveNumber` exposes the current successful position.
The checkpoint plus suffix replays domain admission decisions as well as ordinary state changes.
Failed reducer waves do not enter either the retained suffix or the checkpoint cursor.

Outcome history is a separately bounded decoder-input archive, not a second source of state events.
Each entry retains its original plan/request identity, effect codec identity/version, sequence and
`afterWave` position. A request may have moved into the checkpoint before its outcome arrives; that
late outcome still works with `decodeEffectOutcome` and the same pure decoder. The result event in
the main journal remains the authority for state replay. When old outcomes are evicted,
`outcomesDropped` explicitly reports the truncated side history; a replayable state window is not a
claim to retain every past operation outcome. Do not publish the archived outcomes a second time
when replaying a scenario that already includes their result events.

Checkpoint entries have canonical **raw resource-key order**: finite numbers numerically first,
then strings in UTF-16 code-unit order. Numeric `7` and string `'7'` remain separate; `0` and `-0`
share native Map identity. Application ID codecs must preserve that identity on a JSON round trip.
Sorting happens only at checkpoint/export boundaries, never during a keyed read or subscription.
This ordering is not a version-migration registry or a canonical privacy/support envelope.

`scripts/consumer/composed/journal.ts` exercises long generated streams, both bounds independently,
failed waves and admission decisions, late outcomes, mount isolation, canonical numeric/string
keys, immutable retained captures, byte accounting, atomic relocation, refusal and disposal. It runs
through the same real-package gate under Node/Bun and isolated/hoisted installations. The 1,500-cell
capture-work diagnostic counts codec invocations during 1,199 evictions; it is **not** an allocation
measurement. Library migrations, strict deny-by-default support export, reactions and awaitable
operation drain remain separate contracts, not claims made by this local journal.
