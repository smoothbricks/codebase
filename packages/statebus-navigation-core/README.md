# StateBus Navigation Core

Platform-neutral navigation intent, observed-location, reducer, driver, and event-channel contracts. Neither React,
browser globals, React Router, nor Expo Router is a production dependency.

## Intent is not location

`NavigationIntent<Target>` supports `navigate`, `push`, `replace`, `back`, `forward`, a nonzero history `go` delta, and
explicit external navigation (`assign`, `replace`, or `new-tab`). Target and Location are independently generic. A web
adapter can accept URL strings/objects; a future Expo adapter can use its typed Href and route snapshot without changing
the base event/reducer contract. Unsupported platform capabilities return a typed failure rather than guessing.

A request never predicts the location. `navigationDispatched` means the driver accepted an instruction, not that a new
screen, document, checkout, or external page finished loading. `locationObserved` records what the platform actually
reports. Stale acknowledgements are ignored. An older correlated location fact remains observable without acknowledging
a newer pending intent; uncorrelated browser traversal is authoritative.

`reduceNavigation` owns the renderable operation and optional navigation guard. A guarded user intent becomes blocked;
only matching `navigationConfirmed`/`navigationCancelled` can resolve it. A newer intent supersedes the pending intent.
Every logical request needs a fresh ID; replaying the current request ID is idempotent. The reducer does not retain an
unbounded historic deduplication ledger.

## Composition

`NavigationChannel<Target, Location>` binds typed events and readonly state to an application-owned namespace. Its
subscriber must run after the entire StateBus dispatch wave is reduced. `connectNavigation({ channel, driver })` binds
one driver instance, publishes the initial location, executes only reducer-admitted commands, observes platform facts,
and converts rejected/thrown driver calls to typed failures. Supersession/disposal aborts driver scopes and suppresses
late acknowledgements. A driver must honour the AbortSignal while doing asynchronous work; completed history side
effects cannot be undone by aborting a promise.

The browser implementation is `@smoothbricks/statebus-navigation-browser`. The future Expo implementation is **not**
included. It should implement `current`, `subscribe`, and `execute`, map supported native intents to its router, and
feed actual route changes back as facts. Browser-only operations should return `unsupported` on a platform that cannot
perform them.

## Offline scenarios

`createMemoryNavigation({ entries, index?, equal })` implements deterministic in-memory history for stories and tests.
Push truncates forward history, replace edits the current entry, and Back/Forward/Go traverse only existing entries.
External navigation is disabled, so a story cannot accidentally open a real authentication or payment URL. Location
values should be immutable serializable data; the driver never receives the application bus writer.

`src/__tests__/navigation.test.ts` composes the actual StateBus reducer/channel with memory history and tests a blocked
billing deeplink, confirmation, replace, Back, Forward, cleanup, and offline reducer replay. Pure properties cover
admission, stale results, guard transitions, arbitrary event streams, and checkpoint partitioning.

## Guard limitations

A user-intent guard is not a browser `popstate` veto. Browser traversal has already happened when its observation
arrives; the state must not lie about that location. Native navigation blockers, browser history rollback/entry
tracking, and `beforeunload` confirmation require a platform-specific policy and smoke tests. They are not claimed by
this core or the initial browser driver. Third-party router interception must likewise be implemented at that router's
boundary, not by silently monkey-patching global history.

Run `nx lint statebus-navigation-core` and `nx test statebus-navigation-core`. The normal repository Bun/TypeScript lane
is retained. Application Help replay codecs, bounded journals and generic StateBus-LMAO execution are separate runtime
capabilities; reducing recorded navigation events alone does not implement the whole replay gate.
