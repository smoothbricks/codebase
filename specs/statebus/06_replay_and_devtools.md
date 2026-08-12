# Serialization, Replay, and Redux DevTools <a id="smoo/statebus!n/replay"></a>

## Serializable state contract <a id="smoo/statebus!n/replay-serialization"></a>

The complete composed application state, accepted flag bundles, and application events have deterministic canonical
encodings.

Allowed values are:

- primitives;
- readonly objects and arrays;
- branded values represented by codecs;
- explicitly encoded byte arrays or other declared data;
- typed failure data.

Forbidden values include functions, promises, Errors, AbortSignals, Requests, Responses, DOM/React objects,
QueryClient/DataLoader internals, LMAO contexts, live Ops, open streams, class instances without codecs, and
collaborative document instances.

Computed values are not checkpointed. They recompute from canonical state and flags.

## Library serialization metadata <a id="smoo/statebus!n/replay-library-metadata"></a>

Every library declares:

- stable library ID and schema version;
- state/event codecs;
- canonical keyed-ID encoding;
- support-data classification;
- migrations from each supported version;
- deterministic default state;
- optional invariant validation.

Application composition derives a schema fingerprint from mounted library identities, mount names, versions, codecs,
flag bundles, and relevant runtime format versions.

A bundle with an incompatible fingerprint does not guess. It selects explicit migrations or returns a precise
incompatibility error.

## Canonical snapshot <a id="smoo/statebus!n/replay-snapshot"></a>

A snapshot has stable mount/state ordering and stable keyed-entry ordering:

```ts
interface StateBusSnapshot {
  readonly formatVersion: number;
  readonly schemaFingerprint: string;
  readonly sequence: number;
  readonly wave: number;
  readonly libraries: readonly LibrarySnapshotMetadata[];
  readonly state: unknown;
  readonly flags: unknown;
}
```

Typed IDs preserve their codec identity. Two declarations that can represent numeric `1` and string `'1'` encode
distinct keys.

Serialization is a cold path. Dispatch does not JSON-encode or clone the complete state.

## Bounded journal <a id="smoo/statebus!n/replay-journal"></a>

The runtime retains a bounded rolling checkpoint and subsequent dispatch-wave entries:

```ts
interface EventWaveRecord {
  readonly wave: number;
  readonly firstSequence: number;
  readonly events: readonly SerializedEvent[];
  readonly flagRevisionIds: readonly string[];
  readonly patchSummary?: readonly SerializedPatchSummary[];
  readonly traceIds?: readonly string[];
}
```

When retained entries would exceed byte/event limits, the runtime advances the checkpoint to a canonical state at a wave
boundary and drops earlier journal entries. It never cuts inside a wave.

Result events are sufficient to replay application state. Raw external outcomes, Query caches, and dependency objects
are not required. Optional redacted LMAO records provide effect diagnostics without becoming replay input.

## Replay <a id="smoo/statebus!n/replay-execution"></a>

```ts
const result = app.replay(bundle, {
  effects: 'disabled',
});
```

Replay:

1. validates format, application fingerprint, libraries, and codecs;
2. applies explicit migrations;
3. constructs canonical state/flag state from the checkpoint;
4. reduces captured events wave by wave using the application reducer;
5. executes no Op, provider, timer, navigation, network, storage, or flag evaluator;
6. recomputes requested computed values;
7. compares canonical final state and reducer patches when present.

Replay failure identifies library, version, wave, sequence, event, and invariant. It does not continue with silently
truncated or substituted data.

## Support bundle <a id="smoo/statebus!n/replay-support-bundle"></a>

```ts
interface SupportBundle {
  readonly snapshot: StateBusSnapshot;
  readonly journal: readonly EventWaveRecord[];
  readonly finalState: unknown;
  readonly appBuild: string;
  readonly environment: RedactedEnvironmentSummary;
  readonly lmao?: RedactedTraceSegment | TraceReference;
  readonly renderCounters?: RedactedRenderSummary;
}
```

A Help action captures the bundle, previews data classes, applies consent policy, and uploads/downloads through an
explicit support Op. Automatic diagnostics use the same serializer/redaction path.

Support classification is deny-by-default. Each field is `include`, `redact`, `hash`, or `omit`. Credentials, cookies,
tokens, secret headers, and private keys are always omitted. Content, drafts, repository names, paths, emails,
identifiers, and uploaded bytes require explicit product policy.

Omitted values use typed redaction markers where needed to explain replay limitations.

## Collaborative data boundary <a id="smoo/statebus!n/replay-collaboration"></a>

StateBus snapshots may include document address, content/state-vector hash, revision counters, dirty/acknowledged
status, coarse connection state, and redacted participant summaries.

Raw collaborative updates, awareness frames, document instances, editor instances, and undo stacks remain outside the
general journal. A product may attach a separately classified opt-in sanitized fixture for editor reproduction.

## Redux DevTools package <a id="smoo/statebus!n/replay-redux-devtools"></a>

`@smoothbricks/statebus-redux-devtools` consumes the same serializer, journal, patches, and replay engine.

```ts
const runtime = app.createRuntime({
  reduxDevtools: {
    name: 'Application',
    enabled: development,
  },
});
```

The extension is part of static application composition; connection options are runtime configuration.

## State projection <a id="smoo/statebus!n/replay-devtools-state"></a>

DevTools state mirrors composed namespaces:

```ts
{
  auth: { ... },
  workspace: { ... },
  members: { ... },
  $flags: { ... },
  $statebus: {
    sequence,
    wave,
    interests,
    activeOperations,
    renderCounters,
  },
}
```

`$statebus` is diagnostic runtime projection. It is not application reducer state and is not passed to application
selectors.

## Dispatch-wave actions <a id="smoo/statebus!n/replay-devtools-wave"></a>

StateBus sends one DevTools action per dispatch wave, not one misleading intermediate state per event:

```ts
{
  type: '@@statebus/WAVE',
  wave: 92,
  events: [
    'statebus/interestChanged',
    'members/loadRequested',
  ],
}
```

The attached state is batch-final state observed by handlers. The inspector expands events, reducer scope, patches,
interest changes, request IDs, flag revisions, trace links, and render effects.

Exact interest diagnostics show previous/current count, state handle, redacted ID, provider, cache satisfaction,
operation, cancellation, and final zero.

## Time travel <a id="smoo/statebus!n/replay-devtools-time-travel"></a>

On jump/import/rollback:

1. pause providers and Op bindings;
2. abort active operations and close streams;
3. preserve actual mounted React interest as inactive runtime metadata;
4. load/replay the selected canonical state and flags;
5. render historical state without external work.

Returning to live mode restores the saved live head, resumes providers, and reconciles current exact interest once.
Historical `not-requested` state does not immediately trigger a live fetch while time travel remains active.

A development-only “fork and resume” operation is explicit. It clears incompatible execution caches, creates a new
journal branch, and reconciles interest. It is never the default jump behavior.

QueryClient and DataLoader caches are not time-travelled. They pause/reconcile as execution mechanisms.

## LMAO relationship <a id="smoo/statebus!n/replay-lmao"></a>

Redux DevTools and replay answer state questions. LMAO answers execution questions. Each wave and relevant operation
carries trace IDs so tools can navigate between:

```text
state/event/patch timeline
<-> Op/dependency/flag/timing trace
<-> render attempt/commit trace
```

The replay engine does not interpret Arrow traces to recover state. LMAO export failure cannot corrupt the journal.

## Migrations <a id="smoo/statebus!n/replay-migrations"></a>

Migrations are pure functions over canonical library slices/events:

```ts
migrateV1ToV2(input): V2
```

They perform no I/O and are property-tested for determinism, totality over valid old data, invariant preservation, and
idempotent canonical encoding. Migration order follows library dependency order and explicit format steps.

Unsupported versions fail. There are no indefinite compatibility aliases in runtime APIs.

## Required tests <a id="smoo/statebus!n/replay-tests"></a>

Tests cover:

- deterministic canonical encoding;
- stable keyed ordering and typed-ID distinction;
- codec round trips and malformed data;
- journal byte/event bounds and checkpoint advancement;
- no checkpoint split inside a wave;
- checkpoint plus events equals captured final state;
- no Op/provider/evaluator execution during replay;
- current and supported-version migrations;
- redaction and omission of every sensitive classification;
- DevTools wave projection and patch display;
- time-travel pause, live restoration, and interest reconciliation;
- import rejection on incompatible application schemas;
- QueryClient/DataLoader cache exclusion;
- trace-link preservation without trace dependency.
