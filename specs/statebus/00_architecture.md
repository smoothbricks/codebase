# StateBus Architecture <a id="smoo/statebus!n/architecture"></a>

## Purpose <a id="smoo/statebus!n/architecture-purpose"></a>

StateBus is a typed application-state runtime built around four contracts:

1. applications compose state and event definitions from reusable libraries;
2. one explicit application reducer is the only application-state mutation path;
3. mounted React state expresses demand through exact interest;
4. effectful work executes as LMAO Ops and returns typed events.

StateBus is not a server cache, persistence engine, durable workflow runtime, or replacement for a collaborative
document model. It owns serializable application-visible state, event ordering, exact interest, deterministic reduction,
effect-to-event publication, React subscriptions, replay, and development tooling.

## Package architecture <a id="smoo/statebus!n/architecture-packages"></a>

```text
@smoothbricks/statebus-core
  value-level library and application composition
  event queue and dispatch waves
  lazy Immer-style reducer drafts
  exact scalar and ByID interest
  LMAO Op execution bridge
  evaluated feature-flag bundles
  serialization, replay, and instrumentation records

@smoothbricks/statebus-react
  StatebusProvider
  useBus and useBusEvents
  fine-grained state/computed handles
  React interest lifecycle
  render attempt/commit instrumentation

@smoothbricks/statebus-react-query
  exact-interest to React Query execution
  QueryClient-backed LMAO Ops
  cancellation, in-flight sharing, bounded cache mechanics

@smoothbricks/statebus-dataloader
  exact-interest batching
  runtime-scoped DataLoader Ops
  batch scheduling, cancellation, and result distribution

@smoothbricks/statebus-redux-devtools
  dispatch-wave projection
  state and reducer-patch inspection
  no-I/O time travel and import/export

@smoothbricks/statebus-testing
  manual scheduler and runtime
  snapshot reducer
  deterministic LMAO test tracer
  generated property-test helpers
  in-memory provider primitives
```

`statebus-core` depends on `@smoothbricks/lmao`. LMAO is not an optional observability adapter. It is the effect
execution, dependency injection, feature-flag evaluation, typed Result, and causal tracing substrate.

React Query and DataLoader are execution mechanisms. Neither is an application-visible state authority. Components read
StateBus only.

## Runtime flow <a id="smoo/statebus!n/architecture-runtime-flow"></a>

```text
React state handle mounted
  -> exact interest change
  -> provider maps demand to a request event
  -> application reducer admits or refuses request
  -> bound LMAO Op executes admitted work
  -> Ok/Err maps to a result event
  -> application reducer writes result state
  -> exact interested signals notify React
```

User actions enter at the event boundary:

```text
React user intent
  -> bound event publisher
  -> application reducer
  -> optional LMAO Op
  -> result event
  -> application reducer
```

All queued events in one dispatch wave reduce before any event handler or effect observes the wave. Events published by
effects or handlers enter a later wave. Reducers are synchronous and never await.

## Functional core and imperative shell <a id="smoo/statebus!n/architecture-functional-core"></a>

Application decisions are plain deterministic functions:

- state transitions and invariants;
- request admission, freshness, deduplication, and stale-result rejection;
- query keys and batch partitioning;
- retry eligibility and delay;
- transport encoding and typed outcome decoding;
- feature-flag-dependent state and view decisions;
- selectors and props-only view models;
- workflow phase transitions.

The imperative shell consists of LMAO Ops and platform primitives:

- HTTP, QueryClient, DataLoader, browser, storage, Git, and SDK calls;
- timers and cancellation;
- stream and subscription lifecycle;
- Worker and Durable Object bindings;
- trace export.

An Op may call a pure planner before I/O and a pure decoder afterward. It does not mutate StateBus. It returns an
`Ok`/`Err`, event, promise, iterable, or async iterable accepted by the StateBus-LMAO bridge.

## Global reducer <a id="smoo/statebus!n/architecture-global-reducer"></a>

The global reducer is an intentional application artifact. It makes the global mutation topology reviewable. StateBus
preserves three reducer scopes:

```text
*/*                 application reducer
members/*            topic reducer
members/loadComplete exact event reducer
```

Libraries may export pure transition helpers and reducer fragments. Application composition explicitly selects and
assembles them. Libraries do not invisibly install generated application reducers.

Reducers receive a lazy Immer-style draft of composed application state. The reducer writes ordinary-looking paths:

```ts
members: {
  loadSucceeded(state, payload) {
    const value = state.members.byOrg[payload.orgId];
    value.kind = 'ready';
    value.members = payload.members;
  },
}
```

Only touched scalar cells and exact `ByID` entries are drafted and committed. StateBus never materializes and diffs the
whole application state for an event.

## Application-global, fine-grained state <a id="smoo/statebus!n/architecture-global-fine-grained"></a>

The composed state graph is application-global but is not one giant atom. Every declared scalar state and exact keyed
entry has an independent signal. Computed state tracks only signals it reads.

Support-relevant UI state belongs in StateBus, including resource state, selections, routes, forms, dialogs, progress,
failures, feature-flag bundles, permissions, and coarse editor lifecycle. Component-local state is reserved for
ephemeral browser mechanics such as focus, pointer state, measurements, animation progress, IME buffers, DOM handles,
and third-party editor internals.

State and events are serializable. QueryClient, DataLoader, Requests, Responses, AbortSignals, DOM objects, LMAO
contexts, and open transports never enter application state.

## LMAO integration <a id="smoo/statebus!n/architecture-lmao"></a>

LMAO provides:

- `defineOpContext`, `defineOp`, and `defineOps` for effectful code;
- typed Op-group dependency injection;
- cold-path schema prefixing and column mapping;
- typed sync and async feature-flag evaluation;
- feature-flag access and usage recording;
- typed Results and exception boundaries;
- columnar causal traces across browser and server operations.

StateBus adds event and state context to the active LMAO trace:

```text
trace -> event -> reducer patches -> Op -> dependency Ops -> result event -> reducer patches -> render
```

Reducers and selectors never call the live LMAO evaluator. Evaluated values enter a replayable StateBus flag bundle
first. Reducer and selector `ff` views are synchronous typed projections of that bundle.

## State interest <a id="smoo/statebus!n/architecture-interest"></a>

Reading a resource is demand:

```tsx
const { state } = useBus();
const members = state.members.byOrg.use(orgId);
```

The mount increments exact interest in `(members.byOrg, orgId)`. It does not require the screen to emit `loadRequested`.
A configured provider observes interest and publishes request lifecycle events. The application reducer remains the
admission authority.

Interest is reference-counted, exact, coalesced across React StrictMode churn, and publishes a final zero. Numeric and
string IDs never alias. Unmounting may cancel or unsubscribe provider work according to declared policy; it does not
imply data eviction.

## Feature flags <a id="smoo/statebus!n/architecture-flags"></a>

LMAO flag values are not limited to booleans. A flag schema may yield boolean, string, number, or structured schema
output supported by LMAO. Every definition supplies a validated default, so the StateBus `ff` view is always synchronous
and set.

Flag evaluation runs in an Op. Values for one declared bundle are published atomically with evaluation context, request
ID, and revision. StateBus stores the latest accepted bundle in replayable internal state. Reducers and selectors
receive:

```ts
(state, event, { ff, ffMeta });
```

`ff` contains plain effective values. `ffMeta` contains status, revision, and context identity. No evaluator or
`.track()` function crosses into the reducer.

Visual gating belongs in computed selectors. Reducers use `ff` only when transition semantics depend on a rollout.
Committed renders record flag exposure; user-intent traces record usage against the evaluation revision that produced
the UI.

## Replay and tooling <a id="smoo/statebus!n/architecture-replay"></a>

A StateBus runtime maintains a bounded checkpoint and event-wave journal. Replay disables Ops and reduces captured
events from the checkpoint. The resulting canonical state must equal the captured final state.

The same protocol supports:

- Help/support bundles;
- automatic diagnostics;
- Redux DevTools time travel;
- deterministic bug reproduction;
- schema-version migration tests.

LMAO traces complement replay. StateBus explains what state resulted; LMAO explains which effectful paths, dependencies,
flags, timings, and failures produced the events.

## Non-goals <a id="smoo/statebus!n/architecture-non-goals"></a>

StateBus does not:

- persist application state as an event-sourced database;
- execute durable workflows;
- replace React Query or DataLoader mechanics;
- put external cache contents into replay state;
- send every collaborative document update through global state;
- hide state mutation behind generated feature reducers;
- evaluate feature flags in reducers;
- infer application behavior from component render order;
- provide compatibility with ambient `States` or `Events` declarations.

## Required invariants <a id="smoo/statebus!n/architecture-invariants"></a>

1. One composed application has one explicit reducer authority.
2. Reducers are synchronous, deterministic, and free of I/O.
3. Only reducer commits mutate application state.
4. LMAO Ops never mutate StateBus directly; they publish typed events.
5. Fine-grained state identity survives snapshots, replay, and React subscriptions.
6. Exact interest includes the typed state handle and exact ID.
7. Flags observed by reducers/selectors are replayable values, not live evaluations.
8. Replay executes no Op or external effect.
9. Every external result is represented by a typed event before it affects state.
10. Libraries compose without ambient declaration merging or runtime name remapping.
