# React Bindings and Render Measurement <a id="smoo/statebus!n/react"></a>

## Provider <a id="smoo/statebus!n/react-provider"></a>

```tsx
<app.Provider runtime={runtime}>
  <App />
</app.Provider>
```

The Provider context value is the stable runtime identity. State changes never replace the context value. Runtime
replacement is an explicit remount operation.

Provider unmount disposes React bindings but does not implicitly dispose a runtime that may be owned outside the tree.
The composition target defines ownership and calls `runtime.dispose()` exactly once.

## `useBus` <a id="smoo/statebus!n/react-use-bus"></a>

```ts
const { state, events, ff } = app.useBus();
```

`useBus()` reads the nearest runtime and returns a memoized composed facade. It does not subscribe to all state or
flags.

```ts
interface ReactBusFacade<State, Events, Flags> {
  readonly state: BoundStateFacade<State>;
  readonly events: BoundPublicEventFacade<Events>;
  readonly ff: BoundFlagFacade<Flags>;
}
```

The facade and every event publisher have stable identity for the runtime. Passing a publisher through props does not
cause child rerenders.

## State handles <a id="smoo/statebus!n/react-state-handles"></a>

Scalar state:

```ts
const session = state.auth.session.use();
```

Keyed state:

```ts
const members = state.members.byOrg.use(orgId);
```

Computed state:

```ts
const model = state.members.screen.use({ orgId });
```

Signatures are inferred from handle declarations:

```ts
interface BoundScalarState<T> {
  use(): T;
}

interface BoundByIdState<Id, T> {
  use(id: Id): T | undefined;
}

interface BoundComputed<Args, T> {
  use(args: Args): T;
}
```

`.use()` is a React hook and follows Rules of Hooks. StateBus supplies ESLint rules that recognize chained StateBus
`.use()` calls and prohibit conditional invocation. React compiler compatibility is a release gate.

## Fine-grained subscription <a id="smoo/statebus!n/react-fine-grained"></a>

A state handle's `.use()` resolves exactly one scalar/keyed/computed signal and subscribes with the signal React
binding. `useBus()` itself remains unsubscribed.

A keyed value update rerenders only consumers of that exact key and computed values that read it. A computed value
tracks dynamic signal reads during computation. Changing a flag or state handle not read by the computed value does not
invalidate it.

No hook serializes or reconstructs a broad application state object.

## Interest propagation <a id="smoo/statebus!n/react-interest"></a>

State hooks register interest in the exact source values required by the mounted state/computed handle. Computed
declarations expose or dynamically record their source-interest dependencies.

```ts
state.members.screen.use({ orgId });
```

may register exact interest in:

```text
members.byOrg/orgId
permissions.byOrg/orgId
flags.members bundle
```

Interest starts after mount and stops on cleanup. Render attempts that never commit do not create provider demand.
StrictMode churn coalesces before providers run.

## Events <a id="smoo/statebus!n/react-events"></a>

Public intent events are runtime-bound functions:

```ts
const { events } = useBus();
events.members.inviteSubmitted({ orgId, email });
```

Event-only components use:

```ts
const { inviteSubmitted } = app.useBusEvents(membersLibrary.events);
```

The argument is a typed event namespace/capability handle, not a string. Result and provider lifecycle events do not
appear in the public publisher facade unless intentionally exported as intents.

## Connector and view <a id="smoo/statebus!n/react-connector-view"></a>

A feature UI has a thin connector and props-only view:

```tsx
export function MembersScreen({ orgId }: MembersScreenProps) {
  const { state, events } = app.useBus();
  const model = state.members.screen.use({ orgId });

  return (
    <MembersView
      {...model}
      onRefresh={() => events.members.refreshRequested({ orgId })}
      onInvite={(email) => events.members.inviteSubmitted({ orgId, email })}
    />
  );
}
```

The connector performs no validation, request admission, status derivation, cache invalidation, transport mapping, or
workflow branching. The computed model and reducer own those decisions.

The view imports no StateBus, LMAO, QueryClient, client, runtime, or browser service. It receives values and callbacks.

## Component-state budget <a id="smoo/statebus!n/react-component-state"></a>

Support-relevant state belongs in StateBus:

- selected resources and routes;
- forms and domain validation;
- dialogs and drawers;
- loading, progress, error, retry, and conflicts;
- command admission and operation identity;
- permission and feature-flag-derived UI;
- editor lifecycle and dirty/revision summaries;
- durable notification intent.

Local React state is limited to browser mechanics whose replay does not explain application behavior:

- focus, hover, pressed, and IME composition;
- measurements and observer handles;
- animation progress;
- uncontrolled file handles;
- third-party editor engine internals.

Conformance tooling rejects feature-level `useState`, `useReducer`, private domain contexts, and server-data hooks
outside declared adapter/view exceptions.

## Render instrumentation <a id="smoo/statebus!n/react-render-instrumentation"></a>

LMAO integration measures StateBus-connected renders at three levels.

### Always-on counters

Each instrumented component type receives a numeric slot at composition/build time. Each mounted instance receives an ID
once. Preallocated typed arrays store:

```text
render attempts
committed renders
StateBus invalidations
flag exposures
mounts/unmounts
```

A render increment performs no per-render allocation. Counters flush on threshold, diagnostic capture, operation
completion, unmount, or configured interval.

### Render entries

Development, sampling, anomaly mode, and Help capture write lightweight LMAO entries into an existing runtime/component
trace buffer:

```text
component type and instance
attempt/commit sequence
latest StateBus event sequence
state dependency/version hash
flag bundle revisions
cause classification
optional duration
```

An entry is not necessarily a child span and avoids per-render span setup.

### Render spans

Full spans are available for screens, editors, explicitly annotated components, and diagnostic sampling when causal
duration/tree structure matters. Whether every connected render receives a span is decided by browser benchmarks, not
assumption.

## Render causality <a id="smoo/statebus!n/react-render-causality"></a>

StateBus records, for every bound state handle:

- signal version read by the last committed render;
- latest event sequence/wave that changed it;
- owning state slot and exact encoded ID;
- computed dependency versions.

A render is classified as:

- mount;
- StateBus state invalidation;
- flag revision change;
- parent/props/context;
- StrictMode repeat;
- concurrent retry/abandoned attempt;
- unknown.

“No StateBus dependency changed” is diagnostic evidence, not proof that the render was unnecessary. Props or parent
behavior may justify it.

The trace can link:

```text
members/loadSucceeded
  -> reducer patch members.byOrg/acme
  -> computed members.screen/acme invalidation
  -> MembersScreen instance 41 render
  -> commit
```

## Committed feature-flag exposure <a id="smoo/statebus!n/react-flag-exposure"></a>

A computed/state read records which evaluated flag revision influenced its value. Exposure is recorded only after a
component commits a render that consumed the flag. Abandoned concurrent renders and prefetched/evaluated-but-unread
flags do not count as exposure.

The bridge deduplicates `(component instance, flag, evaluation revision)` and writes LMAO feature-flag access/exposure
data without storing `.track()` functions in StateBus.

A later intent publisher captures the component's last committed flag revisions in its causal context. Declared usage
can call LMAO tracking against the evaluation that produced the visible action.

## Testing React bindings <a id="smoo/statebus!n/react-testing"></a>

Connected screen tests use a real isolated runtime:

```tsx
const runtime = app.createRuntime(testRuntimeConfig);

render(
  <app.Provider runtime={runtime}>
    <MembersScreen orgId={orgId} />
  </app.Provider>
);
```

Tests verify representative wiring:

- exact interest on mount and zero on unmount;
- computed dependency rerenders and unrelated-state non-renders;
- event publishers target the test runtime;
- provider results reach rendered state;
- StrictMode count/interest correctness;
- render attempt/commit diagnostics;
- committed flag exposure.

Props-only views are tested without Provider. Reducer/selector decisions are not duplicated in component tests.

## Prohibited designs <a id="smoo/statebus!n/react-prohibited"></a>

React bindings must not:

- return a plain snapshot from `useBus()` that subscribes broadly;
- use a process-global current runtime;
- require UI code to publish initial load events;
- expose QueryClient/DataLoader state;
- make event publisher identity change per render;
- count an abandoned render as committed flag exposure;
- use render traces as application state;
- infer reducer decisions from component lifecycle.
