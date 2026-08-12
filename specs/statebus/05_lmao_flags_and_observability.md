# LMAO, Feature Flags, and Observability <a id="smoo/statebus!n/lmao-flags"></a>

## Mandatory substrate <a id="smoo/statebus!n/lmao-flags-mandatory"></a>

Every StateBus runtime has an LMAO context. Export, sampling, and retention are configurable; effect execution and
causal context are not optional alternate architectures.

LMAO supplies:

- Ops and nested spans for effectful code;
- Op-group dependency injection;
- typed `Ok`/`Err` Results and exception recording;
- schema composition, prefixing, and column mapping;
- sync/async feature-flag evaluation and value validation;
- feature-flag access and usage recording;
- columnar buffers suitable for low-allocation diagnostics.

StateBus supplies state, events, reducers, exact interest, React bindings, replay, and reducer patches.

## Application Op context <a id="smoo/statebus!n/lmao-flags-op-context"></a>

```ts
const appOpContext = defineOpContext({
  logSchema: appLogSchema,
  flags: appFeatureFlags,
  deps: {
    http: httpOps.prefix('http'),
    query: queryOps.prefix('query'),
    loader: loaderOps.prefix('loader'),
    github: githubOps.prefix('github'),
    browser: browserOps.prefix('browser'),
  },
  ctx: {
    clock: null as Clock,
    requestIds: null as RequestIds,
  },
});
```

Library Ops use clean local schemas and dependency names. The application maps/prefixes them at composition. Mapping
occurs on the cold path; transformed Op code writes directly to local buffers on the hot path.

StateBus does not define a second arbitrary dependency container. Non-Op environmental values use the typed LMAO user
context only when they cannot be represented as an Op. External actions are normally wrapped as Ops so invocation,
failure, timing, and causality remain observable.

## Event causal context <a id="smoo/statebus!n/lmao-flags-event-context"></a>

An event queue item carries an internal reference to its LMAO context. The reference is excluded from event payload
serialization. Publication rules:

- an event emitted inside an Op inherits that context;
- a React intent publisher starts or continues the component/user-interaction context;
- a new state interest starts an interest context when none exists;
- replay supplies recorded sequence/trace metadata but starts no live effect context;
- unrelated events coalesced into one wave retain independent parents.

StateBus records sequence and wave IDs in LMAO entries so traces and Redux DevTools can cross-link.

## StateBus trace vocabulary <a id="smoo/statebus!n/lmao-flags-vocabulary"></a>

The StateBus contribution includes stable fields for:

```text
runtime_id
library_id
mount_id
state_slot
entity_key_hash
event_topic
event_type
event_sequence
dispatch_wave
reducer_scope
patch_count
interest_previous
interest_current
operation_key_hash
request_id
admission
operation_phase
component_type
component_instance
render_attempt
render_commit
flag_bundle
flag_revision
```

High-cardinality and sensitive values use hash/redaction policies. Raw content, credentials, event bodies, Yjs updates,
and arbitrary IDs are not default trace columns.

StateBus uses log entries for events, interest, reducer patch summaries, and render measurements. LMAO Ops/spans
represent effectful operations. A span is not created for every field assignment or state read.

## Existing LMAO feature-flag API <a id="smoo/statebus!n/lmao-flags-existing-api"></a>

StateBus uses LMAO's existing `defineFeatureFlags` and bound evaluator:

```ts
const flags = defineFeatureFlags(flagSchema);

const sync = ctx.ff.someFlag;
const asyncValue = await ctx.ff.get('someAsyncFlag');
```

An enabled/evaluated result carries a typed `value` and `.track()` usage function. Values may be boolean, string,
number, or other schema output accepted by the LMAO flag definition. StateBus does not add boolean-only flag builders or
duplicate evaluation APIs.

## Flag bundles <a id="smoo/statebus!n/lmao-flags-bundles"></a>

StateBus groups related LMAO flag definitions into an atomic evaluated bundle:

```ts
const appFlagBundles = {
  members: {
    definitions: membersFlags,
    context: selectMembersFlagContext,
    pending: 'retain-previous',
  },
};
```

This is declarative StateBus integration metadata, not a replacement for `defineFeatureFlags`.

Each bundle declares:

- LMAO definitions;
- pure evaluation-context selector from application state;
- pending policy: retain previous or use defaults;
- context codec/hash and support classification;
- optional refresh/subscription policy.

The runtime evaluates all required values through an LMAO Op and publishes one internal typed bundle event:

```ts
{
  bundle: 'members',
  contextHash,
  requestId,
  revision,
  values,
  evaluatedAt,
}
```

The internal StateBus flag reducer accepts only the latest request for the current context. Bundle values become visible
atomically. A single remote flag change reevaluates and republishes the affected bundle rather than exposing a mixed
revision.

## Defaults and synchronous view <a id="smoo/statebus!n/lmao-flags-sync-view"></a>

Every flag definition has a validated default. Therefore reducer and selector `ff` views are always synchronous and
contain plain values:

```ts
ff.members.inviteExperience;
ff.members.batchSize;
ff.editor.variant;
```

Metadata is separate:

```ts
ffMeta.members.status;
ffMeta.members.contextHash;
ffMeta.members.revision;
```

Status is one of:

```text
default
evaluating
ready
failed
```

`pending: 'retain-previous'` retains a previous value only when its declaration permits use across the new context.
Otherwise defaults apply until evaluation completes.

## Reducer and selector access <a id="smoo/statebus!n/lmao-flags-consumers"></a>

Reducers receive plain replayable values:

```ts
members: {
  inviteSubmitted(state, payload, { ff }) {
    if (ff.members.inviteExperience === 'disabled') {
      state.members.invite[payload.orgId] = {
        kind: 'rejected',
        reason: 'feature-disabled',
      };
      return;
    }

    state.members.invite[payload.orgId] = {
      kind: 'submitting',
      email: payload.email,
    };
  },
}
```

Visual gating belongs in computed state:

```ts
const screen = computed(({ state, ff }, { orgId }) => ({
  members: state.members.byOrg.read(orgId),
  showInvite: ff.members.inviteExperience !== 'disabled' && canInvite(state.permissions.byOrg.read(orgId)),
}));
```

A flag bundle update invalidates only computed values that read changed flag values. Reducers use flags only when
transition semantics depend on them; they do not duplicate visual derived state.

## Determinism and replay <a id="smoo/statebus!n/lmao-flags-replay"></a>

Reducers and selectors never call `ctx.ff`, `ctx.ff.get`, or `.track()`. They read StateBus's accepted bundle.
Checkpoints include bundle values/metadata, and journals include bundle evaluation events. Replay never invokes a live
evaluator.

An Op may evaluate a flag for an effectful decision. The value/revision influencing a domain result must be represented
in the resulting event envelope or a preceding accepted bundle event so replay and support diagnostics can explain the
branch.

## Exposure and usage <a id="smoo/statebus!n/lmao-flags-exposure"></a>

Evaluation, exposure, and usage are distinct:

- evaluation: the LMAO evaluator produced a validated value;
- exposure: a committed UI render consumed the value;
- usage: a user or effect exercised behavior controlled by the value.

StateBus React records flag dependencies during computed/state reads. After commit, the LMAO bridge records exposure
once per `(component instance, flag, revision)`. Abandoned renders do not expose.

Public intent event definitions may declare flag usage metadata using existing flag handles. When the bound event
publisher emits after a committed render, it records usage against the revision that produced the UI. Explicit
application tracking remains available for non-UI outcomes.

No `.track()` function is stored in StateBus state or event payload.

## Render telemetry <a id="smoo/statebus!n/lmao-flags-render-telemetry"></a>

Always-on render counts use preallocated typed arrays and do not allocate per render. Lightweight render entries append
to an existing LMAO buffer under configured diagnostic modes. Full render spans are reserved for annotated/sampled
boundaries unless benchmarks support broader use.

Mandatory LMAO makes render, event, patch, interest, flag, and Op identities share one vocabulary and causal trace. It
does not justify tracing every leaf component or high-frequency collaborative update.

## Frontend-to-backend propagation <a id="smoo/statebus!n/lmao-flags-cross-runtime"></a>

Request IDs and LMAO trace context propagate from browser Ops through HTTP headers to Worker, Durable Object, Git, and
storage Ops. Result events retain request ID and link to the server trace.

The support/debugging path can answer:

```text
which intent/interest initiated work
which reducer admitted it
which flag revision selected the path
which dependency Ops ran
which result event arrived
which state paths changed
which components rerendered
```

## Performance model <a id="smoo/statebus!n/lmao-flags-performance"></a>

Mandatory LMAO is not faster than no instrumentation. It is designed to be faster and simpler than equivalent separate
tracing, DI, flag, Result, and lifecycle layers.

Performance rules:

- compose schemas/dependencies and generate writers on the cold path;
- use numeric StateBus slots and dictionary-coded names;
- append known fields directly to columnar buffers;
- avoid optional observer branches throughout dispatch;
- avoid object logs and JSON patches unless requested;
- aggregate counters before export;
- bound every buffer and journal;
- sample detailed render/patch data independently from correctness data.

Release benchmarks compare uninstrumented reference operations, mandatory baseline StateBus/LMAO, detailed diagnostics,
React Query, DataLoader, draft reduction, and render instrumentation. Browser bundle compatibility and size are release
gates.

## Error handling <a id="smoo/statebus!n/lmao-flags-errors"></a>

An Op `Err` maps through a pure binding to a typed domain failure event. A thrown/rejected failure receives an LMAO
exception record and maps to a typed infrastructure failure. Reducer failure rolls back the draft transaction and
records a StateBus reducer error without continuing partial state.

Sensitive errors are redacted before trace/support export. Error objects do not enter serializable StateBus state; typed
failure data does.

## Required tests <a id="smoo/statebus!n/lmao-flags-tests"></a>

Tests cover:

- LMAO dependency schema composition and prefix/mapping;
- event causal context through nested Ops and result publication;
- non-boolean sync/async flag values and defaults;
- atomic bundle publication and stale evaluation refusal;
- pending policies and context changes;
- synchronous reducer/selector `ff` access;
- no live flag evaluation during reduction/replay;
- committed exposure deduplication;
- usage tied to the rendering revision;
- render counter no-allocation benchmark;
- lightweight entry versus full span benchmarks;
- redaction of IDs, content, credentials, and flag contexts;
- frontend/server trace correlation.
