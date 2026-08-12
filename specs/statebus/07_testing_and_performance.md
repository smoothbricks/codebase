# Testing and Performance <a id="smoo/statebus!n/testing"></a>

## Testing objective <a id="smoo/statebus!n/testing-objective"></a>

Every business decision is expressible below the imperative shell and covered by pure unit/property tests. At least 99%
of feature scenario volume runs as unit/property tests. No product decision exists only in browser, Worker, or deployed
E2E coverage.

E2E proves that production shell paths, bindings, configuration, and external systems are wired. It does not enumerate
state-machine branches.

## Behavior classification <a id="smoo/statebus!n/testing-classification"></a>

Each feature classifies behavior rows as:

```text
pure transition
pure selector/view model
pure interest/admission decision
pure query/batch/retry decision
pure outcome decoder/codec
application reducer wiring
StateBus runtime contract
LMAO Op/shell contract
React/browser behavior
Worker/platform behavior
external deployment boundary
```

Anything classifiable as pure is tested there. Generated property samples do not artificially change the classification
ratio; the ratio concerns owned behavior and scenario architecture.

## Pure property tests <a id="smoo/statebus!n/testing-properties"></a>

Use `fast-check` for valid/invalid states, event sequences, external outcomes, IDs, clocks, flag values, and context
changes.

Required feature properties include:

- transition totality over the declared state/event domain;
- state invariants after every generated sequence;
- stale result never overwrites current state;
- duplicate fingerprint admission follows policy;
- request ID reuse with a different fingerprint is refused;
- failed refresh retains previous data when declared;
- unrelated events are observationally neutral;
- exact keyed events mutate only their target;
- batch distribution preserves key/result correspondence;
- retry functions satisfy declared bounds;
- flag-dependent decisions use supplied replayable values;
- serialization round trips and redaction invariants;
- workflow ordering and terminal-state invariants.

Shrinking must preserve meaningful invalid/valid partitions and produce reproducible seeds.

## Global reducer tests <a id="smoo/statebus!n/testing-reducer"></a>

The global reducer is tested as a whole with `reduceSnapshot`:

```ts
const next = app.reduceSnapshot(previous, event, flags);
```

Tests assert which global paths change for each event family. Generated sequences cover cross-feature invariants.
Production lazy-cell reduction is differential-tested against snapshot reduction.

Core tests cover the three reducer scopes, Immer-style draft semantics, rollback, exact keyed writes, returned next-wave
events, and dispatch ordering once. Feature suites do not repeat generic adapter behavior.

## Interest/provider tests <a id="smoo/statebus!n/testing-interest"></a>

Pure tests cover demand/freshness/admission decisions. Integration-package tests cover mechanics:

- exact interest 0→1 and final 1→0;
- StrictMode coalescing;
- key changes and multiple consumers;
- cancellation/subscribe/unsubscribe;
- provider request publication before execution;
- reducer admission before Op invocation;
- cache-hit and network paths producing equivalent domain result events;
- DataLoader partition/distribution behavior;
- runtime disposal.

A feature adds one composed provider test only when its configuration introduces behavior not already covered by pure
functions or the generic package.

## LMAO Op tests <a id="smoo/statebus!n/testing-ops"></a>

Tests construct the same Op groups used by production with in-memory dependency Ops and a deterministic test tracer:

```ts
const testContext = defineOpContext({
  logSchema: testSchema,
  flags: testFlags,
  deps: {
    api: inMemoryApiOps.prefix('api'),
    query: deterministicQueryOps.prefix('query'),
  },
  ctx: {
    clock: manualClock,
    requestIds: sequentialRequestIds,
  },
});
```

Assert typed Result/events and observable dependency behavior, not internal mock call choreography. Trace assertions
cover schema, parent/child relationships, flags, redaction, and exceptions only where those are the contract.

Do not globally mock fetch, QueryClient, StateBus hooks, feature modules, or LMAO contexts.

## React tests <a id="smoo/statebus!n/testing-react"></a>

Props-only views receive literal values/callback recorders and cover rendering, accessibility, and intent payloads.

Connected screens use a real isolated runtime and cover one representative path per wiring family:

```tsx
render(
  <app.Provider runtime={runtime}>
    <MembersScreen orgId={orgId} />
  </app.Provider>
);
```

Assert exact interest, rendered state, public event publication, unrelated-state non-renders, cleanup, and replay
equivalence. Provider forests and feature-hook mocks are prohibited.

Browser tests remain for focus, keyboard/IME, history, file input/drag-drop, editor integration, observers, cross-tab
behavior, and DevTools/Help UI.

## Shell-path manifest <a id="smoo/statebus!n/testing-shell-manifest"></a>

Applications maintain a validated manifest:

```text
shell path ID | production Op/route | owner | smoke | environment
```

Every concrete production Op adapter/route branch has at least one focused local browser/Worker or deployed smoke. Many
domain decisions share one shell path. CI rejects unowned paths.

Local Worker/workerd coverage exercises real Request/Response, middleware ordering, CORS, auth extraction,
representative CRUD, idempotency, storage/DO bindings, and stream lifecycle with in-memory external systems.

Deployed tests are reserved for boundaries that local execution cannot prove, such as real external OAuth/install flows,
domain/CSP/cookie configuration, external Git publication, or platform-specific streaming.

## Replay differential gate <a id="smoo/statebus!n/testing-replay"></a>

Every composed feature test and retained diagnostic smoke may capture a bounded replay artifact and verify:

1. load checkpoint;
2. disable Ops/providers/evaluator;
3. replay waves;
4. compare canonical final state;
5. compare declared reducer patch summaries;
6. validate redaction.

Small fixtures cover current and supported old formats. Property tests provide breadth.

## Performance architecture <a id="smoo/statebus!n/testing-performance"></a>

Performance-sensitive paths are designed before benchmarking:

- cold-path library/LMAO composition;
- numeric state/event slots;
- stable generated accessors and publisher identities;
- lazy draft only for touched cells;
- no whole-state reduction copy;
- exact signal notification;
- preallocated render counters;
- bounded columnar traces and replay journal;
- no per-read string prefixing or schema lookup.

## Benchmark matrix <a id="smoo/statebus!n/testing-benchmarks"></a>

StateBus publishes reproducible browser and non-browser benchmarks for:

- event publication and dispatch wave sizes;
- top-level/topic/exact reducer dispatch;
- scalar and exact keyed draft mutations;
- nested Immer draft depth and touched-cell counts;
- rollback;
- exact interest subscribe/unsubscribe churn;
- computed invalidation fanout;
- mandatory LMAO baseline;
- detailed patch/event tracing;
- React Query and DataLoader providers;
- render counter, render entry, and render span modes;
- snapshot serialization and replay throughput;
- browser bundle size/startup.

Compare mandatory LMAO against an internal measurement reference with tracing work removed to quantify cost. The public
architecture remains mandatory LMAO; the reference is benchmark-only.

Regression thresholds derive from measured baselines and are stored with benchmark methodology. No specification invents
unsupported nanosecond or allocation claims.

## Allocation checks <a id="smoo/statebus!n/testing-allocations"></a>

Benchmarks and targeted instrumentation verify:

- render counter increments allocate zero per render;
- unchanged reducer drafts commit no new state value;
- untouched state cells allocate no drafts;
- event/Op tracing writes known columns without object-log allocation;
- detailed JSON patches allocate only when enabled;
- event queue buffers are reused where safe;
- bounded overflow behavior has deterministic limits.

LMAO spans are not assumed equivalent to counters or log rows. Full render-span deployment follows benchmark evidence.

## Conformance tests <a id="smoo/statebus!n/testing-conformance"></a>

Static rules reject:

- ambient StateBus module augmentation;
- raw string state/event access;
- effectful imports in reducers/selectors;
- live flag evaluation in reducers/selectors;
- direct provider state mutation;
- Query hooks in StateBus-connected feature React code;
- application-domain `useState`/`useReducer` outside allowlists;
- private lifecycle event publication by UI;
- unserializable state/event declarations;
- missing support classification;
- global runtime singletons;
- unbounded journals/traces;
- direct collaborative update storage in StateBus.

## Mock deletion rule <a id="smoo/statebus!n/testing-mock-deletion"></a>

Delete a mock-heavy test when:

1. its observable contract is named;
2. decisions are covered by pure properties;
3. reducer/provider/Op wiring has focused coverage;
4. no unique browser/platform behavior remains;
5. the old production seam is removed;
6. the replacement fails under a plausible regression.

Do not replace React Query mocks with StateBus-hook mocks. Lower the test to pure values or raise it to a real isolated
runtime with in-memory Ops.

## Release gates <a id="smoo/statebus!n/testing-release-gates"></a>

A release requires:

- type/build/tests for every StateBus package;
- property-test seed reporting;
- production/snapshot reducer equivalence;
- replay no-I/O proof;
- LMAO schema/redaction validation;
- React StrictMode and compiler compatibility;
- browser bundle and performance regression checks;
- integration package disposal/cancellation tests;
- package export and isolated-consumer tests;
- no compatibility exports for removed ambient APIs.
