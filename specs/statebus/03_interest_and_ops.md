# Exact Interest and LMAO Ops <a id="smoo/statebus!n/interest-ops"></a>

## Exact interest <a id="smoo/statebus!n/interest-ops-interest"></a>

State interest expresses mounted demand for a state value.

```ts
type Interest =
  | { readonly state: ScalarStateHandle<unknown> }
  | { readonly state: ByIdStateHandle<unknown, unknown>; readonly id: unknown };

interface InterestChange {
  readonly interest: Interest;
  readonly subscribers: number;
}
```

A scalar subscription counts its exact state handle. A keyed subscription counts the exact `(handle, typed ID)` pair.
Counts are not collapsed by topic, mount, feature, or state key alone.

## React lifecycle <a id="smoo/statebus!n/interest-ops-react-lifecycle"></a>

```tsx
const { state } = useBus();
const member = state.members.byId.use(memberId);
```

The hook:

1. resolves the exact signal;
2. subscribes through the signal React binding;
3. increments exact interest after mount;
4. decrements on cleanup;
5. returns the current value;
6. resubscribes if the runtime, handle, or ID changes.

StrictMode mount/unmount/remount churn is coalesced at the scheduler boundary. Providers observe the final count for the
wave. Final unsubscribe publishes zero. A count never becomes negative.

Removing and recreating a keyed value while interest remains must preserve the signal identity or explicitly rebind
every interested subscriber. A mounted component cannot remain attached to an orphaned signal.

## Interest does not imply eviction <a id="smoo/statebus!n/interest-ops-no-eviction"></a>

A transition to zero means no current consumer requires provider activity. It may cause cancellation, polling shutdown,
or transport unsubscribe. It does not delete data unless the resource declaration includes an explicit eviction policy
and event handled by the application reducer.

## Provider flow <a id="smoo/statebus!n/interest-ops-provider-flow"></a>

A provider translates interest into lifecycle events and LMAO Ops. It never writes StateBus state.

```text
interest count 0 -> 1
  -> pure demand decision
  -> lifecycle request event
  -> application reducer admits/refuses
  -> Op binding observes batch-final state
  -> admitted Op executes
  -> Ok/Err becomes lifecycle result event
  -> application reducer writes state
```

Interest-driven loading is declarative at composition:

```ts
const membersReactQuery = reactQuery.provide({
  state: membersLibrary.state.byOrg,
  requested: membersLibrary.events.loadRequested,
  succeeded: membersLibrary.events.loadSucceeded,
  failed: membersLibrary.events.loadFailed,
  op: membersOps.load,
  demand: decideMembersDemand,
  input: planMembersLoad,
  key: ({ orgId }) => orgId,
  policy: 'latest-wins',
});
```

Exact builder names are package API, but the configuration must identify state demand, request/result events, pure
decisions, Op, operation key, and policy without hiding reducer writes.

## Pure demand decision <a id="smoo/statebus!n/interest-ops-demand"></a>

```ts
function decideMembersDemand(input: {
  readonly value: MembersState;
  readonly subscribers: number;
  readonly previousSubscribers: number;
  readonly now: Timestamp;
  readonly requestId: RequestId;
}): LoadMembersRequested | undefined;
```

The provider supplies clock/request-ID values. The pure function decides whether to request. It covers initial load,
staleness, refresh, retry, deduplication, and zero-interest behavior.

An interest adapter does not infer domain freshness from `undefined` or QueryClient internals. StateBus application
state carries explicit `not-requested`, `loading`, `ready`, and `failed` states.

## LMAO is the effect runtime <a id="smoo/statebus!n/interest-ops-lmao"></a>

Effectful application code is an LMAO Op. StateBus does not define a competing dependency injection or Result system.

```ts
const { defineOp, defineOps } = defineOpContext({
  logSchema: membersLogSchema,
  flags: membersFlags,
  deps: {
    query: queryOps,
    api: membersApiOps,
  },
  ctx: {
    clock: null as Clock,
  },
});

const load = defineOp('load', async (ctx, request: MembersRequest) => {
  return ctx.span('query', ctx.deps.query.fetch, {
    key: membersQueryKey(request),
    op: ctx.deps.api.list,
    input: request,
  });
});

export const membersOps = defineOps({ load });
```

Op groups compose through LMAO prefixing/mapping. Raw clients are wrapped in boundary Ops or supplied through explicit
Op context where appropriate. Feature Ops call dependency Ops through `ctx.span`, preserving causal traces.

## Event-to-Op binding <a id="smoo/statebus!n/interest-ops-binding"></a>

```ts
const memberEffects = effect({
  on: membersLibrary.events.loadRequested,
  op: membersOps.load,
  input: planMembersRequest,
  operationKey: ({ orgId }) => orgId,
  policy: 'latest-wins',
  ok: membersLoadSucceeded,
  err: membersLoadFailed,
});
```

The StateBus-LMAO bridge:

1. receives the reduced event and its LMAO causal context;
2. calls the pure input planner against batch-final readonly state and synchronous flags;
3. skips when the reducer did not admit the operation;
4. creates an operation cancellation scope;
5. invokes the Op;
6. normalizes accepted output;
7. maps success/error through pure event builders;
8. publishes result events into a later wave;
9. closes the Op scope and records lifecycle.

The binding is declarative shell wiring. State mutation remains explicit in the application reducer.

## Output contract <a id="smoo/statebus!n/interest-ops-output"></a>

A binding accepts:

```ts
type OpOutput<E> =
  void | E | Result<E, unknown> | Promise<void | E | Result<E, unknown>> | Iterable<E> | AsyncIterable<E>;
```

LMAO's existing Op/Result contract is authoritative for ordinary one-shot work. StateBus extends the bridge for
iterable/async-iterable event streams without changing reducer semantics. An iterator is always closed on
cancellation/disposal, including `return()` and generator `finally`.

A rejected promise or thrown exception becomes a typed infrastructure failure and LMAO exception record. It never
escapes as an unhandled rejection.

## Cancellation and concurrency <a id="smoo/statebus!n/interest-ops-concurrency"></a>

Each admitted operation has a stable operation key and `AbortSignal`. Supported policies are:

- `drop-duplicate`: retain the first equivalent admitted request;
- `latest-wins`: abort/suppress older work for the key;
- `serialize`: execute admitted operations for the key in order;
- `parallel`: independent operation instances.

The application reducer decides admission and records request identity. The bridge enforces execution/cancellation
mechanics. A result event retains request ID and fingerprint, and the reducer remains responsible for stale-result
refusal.

Disposal aborts all active operations, closes iterators, unsubscribes providers, and prevents later event publication.

## React Query integration <a id="smoo/statebus!n/interest-ops-react-query"></a>

`statebus-react-query` supplies QueryClient-backed Ops and provider composition. QueryClient owns in-flight sharing,
timers, and bounded execution cache. StateBus owns application-visible state and correctness.

Pure definitions supply:

- query key;
- demand/freshness decision;
- retry eligibility and delay;
- request admission/fingerprint;
- outcome decoding;
- success/failure event construction.

Components never call `useQuery`, `useMutation`, `useQueryClient`, or `invalidateQueries` for a migrated resource. Query
cache state is not replay state.

A provider may satisfy new interest from a QueryClient cache, but it still publishes the same typed lifecycle result
event so StateBus state, replay, DevTools, and tracing remain coherent.

## DataLoader integration <a id="smoo/statebus!n/interest-ops-dataloader"></a>

`statebus-dataloader` groups exact newly demanded keys for a runtime-scoped batch:

```text
users.byId/alice 0 -> 1
users.byId/bob   0 -> 1
  -> pure key selection/deduplication
  -> one admitted batch request event
  -> DataLoader Op loadMany([alice, bob])
  -> pure distribution
  -> typed per-key/batch result events
  -> global reducer writes exact entries
```

Pure definitions supply key selection, ordering, batch partitioning, result correspondence, missing-key behavior, and
failure classification. The imperative package owns scheduling, DataLoader instance lifecycle, cancellation, and
invocation.

A DataLoader instance is never module-global. Browser runtimes and server requests receive independent instances unless
an explicit safe sharing contract says otherwise.

## Manual refresh and mutation <a id="smoo/statebus!n/interest-ops-intents"></a>

Initial resource demand is interest-driven. Explicit user intent still uses events:

```ts
events.members.refreshRequested({ orgId });
events.members.inviteSubmitted({ orgId, email });
```

Refresh modifies demand/admission state or provider freshness through the global reducer and later provider reaction.
Mutations are never inferred solely from state interest.

## Causal context <a id="smoo/statebus!n/interest-ops-causality"></a>

Every queued event retains internal LMAO causal context without putting a live context in serializable payload. Events
emitted by an Op inherit that Op's context. A user intent or new interest without an active trace starts a root
operation.

Dispatch waves may contain events from different causal roots. StateBus preserves per-event contexts; it does not force
unrelated events under one false parent. Wave records link participating traces through sequence/wave metadata.

Request IDs propagate through StateBus events, LMAO scope, transport headers, Worker traces, and result events.

## No effectful reducers <a id="smoo/statebus!n/interest-ops-no-effectful-reducers"></a>

Reducers cannot call Ops, providers, QueryClient, DataLoader, clocks, request-ID generators, or flag evaluators.
Providers and Ops cannot write draft state. The only bridge between the two sides is a typed event.

## Required tests <a id="smoo/statebus!n/interest-ops-tests"></a>

Core and integration packages cover:

- exact scalar and keyed interest increments/decrements;
- StrictMode coalescing and final zero;
- ID changes, numeric/string distinction, and signal recreation;
- one provider per resource validation;
- interest-to-request decision ordering;
- reducer admission before Op execution;
- every concurrency policy;
- AbortSignal propagation and late-result suppression;
- iterator cleanup and runtime disposal;
- QueryClient cache-hit/network equivalence at the domain event boundary;
- DataLoader batching, missing keys, ordering, and failure distribution;
- LMAO parent/child context across event, Op, dependency Op, and result event;
- no direct provider state mutation;
- no Op execution during replay.
