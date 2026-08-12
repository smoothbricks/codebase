# Events, Reducers, and Draft Transactions <a id="smoo/statebus!n/reducers"></a>

## Event model <a id="smoo/statebus!n/reducers-events"></a>

Every event has a stable composed identity and serializable payload.

```ts
interface Event<Topic, Type, Payload> {
  readonly topic: Topic;
  readonly type: Type;
  readonly payload: Payload;
}
```

Runtime envelopes may carry sequence, wave, LMAO context, request correlation, and replay metadata. Envelope metadata is
not application payload and is removed or explicitly encoded by serialization.

Event definitions are classified:

- `intent`: publishable by application/React callers;
- `lifecycle`: published by providers and Ops;
- `fact`: an observed domain occurrence that may have multiple consumers;
- internal StateBus events: interest, flags, runtime errors, and tooling controls.

Classification controls publication capability, not reducer matching. Every event remains available to the global
reducer.

## Publisher API <a id="smoo/statebus!n/reducers-publishers"></a>

The runtime-bound React facade publishes intents as functions:

```ts
const { events } = useBus();
events.members.inviteSubmitted({ orgId, email });
```

Definition handles construct without publishing:

```ts
const event = membersLibrary.events.inviteSubmitted.create({ orgId, email });
runtime.publish(event);
```

Event handles also provide type narrowing and stable diagnostics identity. A lifecycle event does not become a public
React function unless an application explicitly re-exports it as an intent.

## Three reducer scopes <a id="smoo/statebus!n/reducers-scopes"></a>

StateBus preserves all three reducer definition forms.

### Application reducer: `*/*`

```ts
const reduceApp: AppReducer = (state, event, context) => {
  switch (event.topic) {
    case 'auth':
      reduceAuth(state, event, context);
      return;
    case 'members':
      reduceMembers(state, event, context);
      return;
    case 'publication':
      reducePublication(state, event, context);
      return;
  }
};
```

### Topic reducer: `topic/*`

```ts
const reducers = {
  members(state, event, context) {
    switch (event.type) {
      case 'loadSucceeded':
        // Explicit writes.
        return;
    }
  },
};
```

### Exact reducer: `topic/event`

```ts
const reducers = {
  members: {
    loadSucceeded(state, payload, context) {
      // Explicit writes.
    },
  },
};
```

The application reducer file is the reviewable map from events to global state writes. A library may export topic/exact
fragments or pure helpers, but application composition explicitly includes them. StateBus does not silently generate
feature reducers from resource declarations.

## Reducer context <a id="smoo/statebus!n/reducers-context"></a>

```ts
interface ReducerContext<Flags> {
  readonly ff: ResolvedFlagValues<Flags>;
  readonly ffMeta: ResolvedFlagMetadata<Flags>;
  readonly sequence: number;
  readonly wave: number;
}
```

The context contains deterministic values captured in replay state/envelopes. It has no clock, random generator,
AbortSignal, LMAO evaluator, logger, dependency, publisher, or external client.

Reducers never publish directly. A reducer may return later events through the reducer result:

```ts
interface ReducerResult<E> {
  readonly events?: readonly E[];
}
```

Returned events are queued for the next wave after the current wave's reductions. Reducers cannot reduce reentrantly.

## Immer-style global draft <a id="smoo/statebus!n/reducers-draft"></a>

Reducers receive a virtual `Draft<AppState>` whose paths look like ordinary application state:

```ts
members: {
  loadSucceeded(state, payload) {
    const members = state.members.byOrg[payload.orgId];
    if (members.requestId !== payload.requestId) return;

    members.kind = 'ready';
    members.value = payload.members;
    members.receivedAt = payload.receivedAt;
  },
}
```

The draft is virtual because underlying state remains fine-grained signals. The draft manager creates an Immer draft
only for a cell when the reducer first reads or writes it.

### Scalar state

```ts
state.workspace.active = nextWorkspace;
state.auth.session.kind = 'signed-in';
```

### String-keyed `ByID`

```ts
state.members.byOrg[orgId].kind = 'loading';
delete state.members.byOrg[orgId];
```

### General typed IDs

JavaScript property access coerces keys. Numeric, composite, or union identity uses the explicit keyed draft API:

```ts
const point = state.points.at(pointId);
point.selected = true;

state.points.set(pointId, nextPoint);
state.points.delete(pointId);
```

A string-index convenience exists only when the declaration's ID codec proves that property access is injective and
cannot collide with draft API members.

### Missing entries

A keyed declaration with an initializer materializes its initial draft on first mutation:

```ts
const value = state.members.byOrg.ensure(orgId);
value.kind = 'loading';
```

A declaration without an initializer requires explicit assignment. Reading a missing entry returns `undefined` and does
not insert it.

## Lazy draft transaction <a id="smoo/statebus!n/reducers-draft-transaction"></a>

For each event reduction:

1. create an empty draft transaction;
2. expose stable generated accessors for composed state slots;
3. lazily create an Immer draft on first cell access;
4. execute the reducer synchronously;
5. finish drafts for touched cells;
6. set only cells whose finished value is not referentially equal to the previous value;
7. record compact patches and inverse patches when tooling requires them;
8. revoke drafts;
9. queue returned events.

If a reducer throws, no touched cell commits and all drafts revoke. The runtime emits a typed StateBus failure
entry/event according to application policy. It never continues with a partially committed event.

StateBus may wrap all event commits in one underlying signal transaction so observers notify after the dispatch wave.
Logical draft finalization remains per event, and a later reducer in the wave sees earlier committed event state.

## No whole-state production <a id="smoo/statebus!n/reducers-no-whole-state"></a>

The production reducer path never:

- serializes the entire application before reduction;
- calls Immer `produce` over one giant application object;
- diffs the whole state after reduction;
- rewrites untouched state slots;
- enumerates every `ByID` entry.

The virtual draft routes access directly to scalar or exact keyed cells. Complexity is proportional to touched cells and
touched nested values.

## Pure nested transitions <a id="smoo/statebus!n/reducers-pure-helpers"></a>

Reducers may use immutable helpers:

```ts
loadSucceeded(state, payload) {
  state.members.byOrg[payload.orgId] = membersLoaded(
    state.members.byOrg[payload.orgId],
    payload,
  );
}
```

Or draft-mutating helpers:

```ts
loadSucceeded(state, payload) {
  applyMembersLoaded(state.members.byOrg[payload.orgId], payload);
}
```

A draft-mutating helper is treated as a pure transition when its only observable result is the returned finished state.
It accepts no runtime context or dependency and never retains a draft reference.

## Snapshot reduction <a id="smoo/statebus!n/reducers-snapshot"></a>

Every application definition exposes the same reducer over plain canonical state:

```ts
const next = app.reduceSnapshot(previous, event, flagSnapshot);
```

Snapshot reduction uses the same global reducer and Immer semantics without signals, React, Ops, or schedulers. It
powers property tests, replay validation, migrations, and tooling.

Production lazy-cell reduction and snapshot reduction must be differential-tested: given equivalent canonical state,
event, and flags, both produce the same canonical next state and returned events.

## Dispatch waves <a id="smoo/statebus!n/reducers-waves"></a>

```text
queued A, B
  -> reduce A
  -> reduce B, observing A's state
  -> commit signal transaction
  -> handlers/effects for A see state after A+B
  -> handlers/effects for B see state after A+B
  -> emitted C enters next wave
```

Reducers are ordered by event queue order. Handlers are not allowed to depend on intermediate state after only their
event; they observe batch-final state. This enables reducer-side admission to collapse duplicate commands before
external work starts.

## Admission and stale results <a id="smoo/statebus!n/reducers-admission"></a>

Request admission is reducer-owned application state. Common policies are pure helpers, not runtime magic:

```ts
type RequestPolicy = 'drop-duplicate' | 'latest-wins' | 'serialize' | 'parallel';
```

A request event includes request ID, stable operation key, and fingerprint. The reducer records the admitted operation.
The later Op planner reads batch-final state and executes only the admitted request. Result reducers ignore stale
request IDs and incompatible fingerprints.

The request ID used for StateBus correlation may also supply HTTP `X-Request-ID` and mutation `Idempotency-Key`, but
server idempotency remains an explicit server contract.

## Reducer patches <a id="smoo/statebus!n/reducers-patches"></a>

The draft transaction can emit compact patches:

```ts
{
  operation: 'replace',
  stateSlot: membersByOrgSlot,
  entityKey: encodedOrgId,
  path: ['kind'],
  value: 'ready',
}
```

Patches support Redux DevTools, LMAO diagnostics, and optional inverse-patch time travel. They are not the persistence
or replay contract; canonical checkpoint plus events is authoritative.

Production tracing writes patch metadata directly to LMAO columns where possible. It does not allocate JSON patch
objects unless a consumer requests them.

## Reducer restrictions <a id="smoo/statebus!n/reducers-restrictions"></a>

Reducers and their helpers must not:

- await or return promises;
- publish through the bus;
- invoke Ops or dependencies;
- evaluate live feature flags;
- read clock, random, process, browser, network, or storage state;
- write a state handle not included in the composed application;
- retain a draft beyond the call;
- mutate event payloads;
- store unserializable values.

Development builds freeze event payloads and revoke finished drafts. Tests verify failure before state commit.

## Required tests <a id="smoo/statebus!n/reducers-tests"></a>

StateBus core tests cover:

- all three reducer scopes and dispatch-table equivalence;
- exact event payload inference;
- lazy scalar and keyed drafts;
- string, numeric, and composite ID behavior;
- missing, ensure, set, and delete semantics;
- untouched cells preserving identity;
- reducer throw rollback;
- sequential event visibility within a wave;
- observer notification only after the wave;
- returned events entering the next wave;
- production/snapshot differential reduction;
- canonical patches and inverse patches;
- flag snapshot determinism;
- no draft escape.
