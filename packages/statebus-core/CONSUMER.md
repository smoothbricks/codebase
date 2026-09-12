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
starts no effects. The legacy API keeps its previous non-transactional reducer semantics.
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
bunx nx run statebus-core:verify-packages
```

It is also a dependency of `statebus-core:test`. The verifier packs the real packages using the
release export-manifest transform, extracts the tarballs into a temporary external consumer,
strictly typechecks public declarations (including negative branded-ID/capability tests), and runs
the consumer under Node and Bun. Every relevant package resolution must point inside an extracted
`node_modules/.../dist` directory. The composed fixture excludes the legacy ambient fixture.

The executable example lives in `scripts/consumer/{library,codecs,scenario}.fixture.txt`; the verifier
materializes these as `.ts` files outside the workspace. It uses synthetic inventory resources,
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
| Browser navigation core/driver package | Already present in the underlying branch; not rewritten here |
| Application-specific permissions, fingerprints, stale decisions, failure classes and actual screen wiring | Application-owned; not implemented by this generic example |
| Expo, collaboration, generic forms and diagnostics/Help UI | Unsupported here; not required for this consumer contract |

These source changes do not publish package versions or migrate a downstream screen by themselves.
The application must replace its old store/query boundary, bind its real operations, and verify its
own checkpoint/event stream before calling that screen fully migrated.
