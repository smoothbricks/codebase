# Library and Application Composition <a id="smoo/statebus!n/composition"></a>

## Overview <a id="smoo/statebus!n/composition-overview"></a>

StateBus state and event definitions are ordinary typed values. Reusable libraries define local names. Applications
compose libraries, select mounts, provide one global reducer, and receive a fully inferred runtime and React API.

Ambient TypeScript declaration merging is not part of StateBus.

## Library definition <a id="smoo/statebus!n/composition-library"></a>

```ts
export const membersLibrary = defineBusLibrary({
  id: '@product/members',
  schemaVersion: 1,

  state: ({ state }) => ({
    byOrg: state.byId<OrgId, MembersState>({
      initial: () => ({ kind: 'not-requested' }),
      codec: membersStateCodec,
      support: membersStateSupportPolicy,
    }),

    invite: state.byId<OrgId, InviteState>({
      initial: () => ({ kind: 'idle' }),
      codec: inviteStateCodec,
      support: inviteStateSupportPolicy,
    }),
  }),

  events: ({ event }) => ({
    loadRequested: event.lifecycle<LoadMembersRequested>(),
    loadSucceeded: event.lifecycle<LoadMembersSucceeded>(),
    loadFailed: event.lifecycle<LoadMembersFailed>(),
    refreshRequested: event.intent<RefreshMembersRequested>(),
    inviteSubmitted: event.intent<InviteSubmitted>(),
    inviteSucceeded: event.lifecycle<InviteSucceeded>(),
    inviteFailed: event.lifecycle<InviteFailed>(),
  }),

  computed: ({ computed, state, ff }) => ({
    screen: computed({ orgId: orgIdCodec }, ({ orgId }) =>
      membersScreenModel(state.byOrg.read(orgId), state.invite.read(orgId), ff.members.invites)
    ),
  }),
});
```

The example is normative in shape, not in builder spelling. A library definition contains:

- stable library identity and schema version;
- local scalar, keyed, and computed state handles;
- local event handles with publication visibility;
- codecs and support-data classifications;
- optional flag definitions, LMAO Op groups, effect bindings, and provider contributions;
- public exports.

A library does not import an application schema and does not augment a global module.

## Handle identity <a id="smoo/statebus!n/composition-handles"></a>

State and event handles are nominal values created once. They are not bare string keys.

```ts
membersLibrary.state.byOrg;
membersLibrary.events.inviteSubmitted;
```

A handle carries local identity, value type, ID type where applicable, codec, visibility, owner, and composition
metadata. Application composition resolves it to a numeric runtime slot and stable diagnostic name.

Hot-path reads, writes, publications, and reducer dispatch use resolved handles/slots. They do not concatenate prefixes
or search by library name.

## Application definition <a id="smoo/statebus!n/composition-application"></a>

```ts
export const conlocaBus = defineBus(
  [
    appLibrary,
    authLibrary,
    workspaceLibrary,
    membersLibrary,
    membersReactQuery,
    contentLibrary,
    contentDataLoader,
    publicationLibrary,
  ] as const,
  {
    reducers: appReducers,
    flags: appFlagBundles,
    lmao: appOpContext,
    replay: appReplayPolicy,
  }
);
```

The definition exposes:

```ts
conlocaBus.createRuntime(runtimeConfig);
conlocaBus.Provider;
conlocaBus.useBus;
conlocaBus.useBusEvents;
conlocaBus.reduceSnapshot;
conlocaBus.serialize;
conlocaBus.replay;
conlocaBus.schemaFingerprint;
```

`defineBus` performs no external I/O and creates no process-global runtime.

## Runtime creation <a id="smoo/statebus!n/composition-runtime"></a>

```ts
const runtime = conlocaBus.createRuntime({
  scheduler: microtaskScheduler,
  tracer: appTracer,
  flagEvaluator,
  queryClient,
  clock,
  requestIds,
});
```

Runtime requirements are inferred from the composed libraries and integrations. Missing dependencies are compile-time
errors when statically visible and runtime construction errors otherwise.

Every runtime owns:

- state signals and keyed repositories;
- event queues and scheduler;
- exact interest counts;
- LMAO root/runtime context;
- evaluated flag bundles;
- active Ops and cancellation scopes;
- QueryClient/DataLoader integration instances;
- replay checkpoint and journal;
- render counters and instrumentation state;
- disposal.

Tests, stories, browser roots, and embedded instances create independent runtimes from the same definition.

## React API <a id="smoo/statebus!n/composition-react"></a>

```tsx
function MembersScreen({ orgId }: Props) {
  const { state, events } = conlocaBus.useBus();
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

`useBus()` returns one stable runtime-bound facade from React context. Calling `useBus()` does not subscribe to
application state. A state handle's `.use()` performs the exact signal subscription and interest lifecycle. Event
functions publish to the runtime selected by the nearest Provider.

Event-only code may narrow capabilities:

```ts
const { inviteSubmitted } = conlocaBus.useBusEvents(membersLibrary.events);
```

The public event facade includes only events declared publishable to the caller. Lifecycle/result events remain
available to reducers, Ops, providers, replay, and testing without exposing a UI publisher.

## Mounts and multiple instances <a id="smoo/statebus!n/composition-mounts"></a>

A library has a stable default mount derived from its ID. Multiple instances require explicit mounts:

```ts
const memberSearch = searchLibrary.mount('memberSearch');
const contentSearch = searchLibrary.mount('contentSearch');

defineBus([memberSearch, contentSearch], { reducers });
```

A mount scopes state, events, computed state, Ops, flags, codecs, and diagnostics together. Partial prefixing is
forbidden.

The application facade is nested by mount:

```ts
state.memberSearch.results.use(query);
state.contentSearch.results.use(query);
```

Snapshots and traces use stable `<mount>.<local-name>` names. Runtime hot paths use numeric slots.

## Composition validation <a id="smoo/statebus!n/composition-validation"></a>

Composition rejects:

- duplicate mount identities;
- duplicate state ownership;
- duplicate event identities;
- incompatible codec/schema versions;
- missing required Op groups or provider capabilities;
- incompatible LMAO contributed columns;
- an interested resource with no provider;
- two exclusive providers for one resource;
- an effect binding whose input/result event types do not match its Op;
- a public event export that exposes a private lifecycle event;
- a library with unserializable state or events;
- a dependency cycle that requires synchronous reentrant publication.

Static tuples report type errors where TypeScript can prove the conflict. Runtime validation always runs because modules
may be assembled dynamically.

## Library dependencies <a id="smoo/statebus!n/composition-dependencies"></a>

Libraries communicate through explicit public handles and LMAO Op groups.

A feature may depend on another feature's public event capability:

```ts
publicationLibrary.bind({
  saveRequested: contentLibrary.events.saveRequested,
  saveSucceeded: contentLibrary.events.saveSucceeded,
  saveFailed: contentLibrary.events.saveFailed,
});
```

It may depend on effectful behavior through a mapped Op group:

```ts
const publicationOps = defineOpContext({
  logSchema: publicationLogSchema,
  deps: {
    content: contentOps.prefix('content'),
    git: gitOps.prefix('git'),
  },
});
```

One library never receives writable handles owned by another library. Cross-feature state changes occur because the
global reducer responds to typed events.

## App-local definitions <a id="smoo/statebus!n/composition-app-local"></a>

An application may define an app-local library for shell-specific state and events:

```ts
const appLibrary = defineBusLibrary({
  id: '@app/shell',
  state: ...,
  events: ...,
});
```

App-local state follows the same codec, reducer, replay, and visibility rules. It is not a special untyped escape hatch.

## State namespace and granularity <a id="smoo/statebus!n/composition-granularity"></a>

Use independent scalar or keyed state when values have independent subscription, interest, persistence, or invalidation
lifecycles. Keep tightly coupled invariants in one aggregate value.

```ts
state: {
  session: State<SessionState>;
  activeWorkspace: State<ActiveWorkspace>;
  membersByOrg: ByID<OrgId, MembersState>;
  contentByAddress: ByID<DocumentAddressKey, ContentState>;
  editorBySession: ByID<EditorSessionId, EditorSessionState>;
}
```

`ByID` carries both ID and value type. Unrelated branded IDs are not interchangeable. A keyed declaration defines
canonical key encoding for serialization and diagnostics while preserving the typed ID at API boundaries.

Do not create a single `AppState` atom. The application-global reducer operates over a virtual draft composed from
fine-grained cells.

## Cold-path compilation <a id="smoo/statebus!n/composition-cold-path"></a>

`defineBus` may generate specialized runtime structures at composition time:

- numeric state and event IDs;
- event dispatch tables for the three reducer scopes;
- state-draft accessors;
- codec and redaction tables;
- public React facade classes;
- LMAO schema and Op-binding plans;
- Redux DevTools name dictionaries;
- render-counter slots.

Generated structures have stable shapes and direct properties. No Proxy is required on the event dispatch, reducer, or
LMAO hot path. A development-only Proxy may improve diagnostics but cannot define production semantics.

## Clean cutover <a id="smoo/statebus!n/composition-clean-cutover"></a>

StateBus exposes only value-level composition. There is no compatibility layer for ambient `States`/`Events`, global
runtime lookup, string-only state access, or untyped event publication. All callers migrate to handles and composed
application definitions in one accepted unit.
