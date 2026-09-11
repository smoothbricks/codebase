# StateBus TanStack Query

Framework-independent QueryClient middleware for `@smoothbricks/statebus-data-loader`. Components read StateBus and emit
intent; they do not call query hooks, read the execution cache, or coordinate invalidation callbacks.

## Composition

Install one `installTanStackQueryLoader` per non-overlapping resource owner at the runtime composition boundary. Supply:

- the runtime-owned `queryClient`, typed `channel`, and exact `interests` source;
- `matches` for that owner's namespace, and `query(request)` for semantic query identity/options and the boundary
  `execute` operation;
- total pure `failure` classification, plus boundary `now` and `requestId` functions.

For ByID resources, `matches` must also require an ID. Include tenant, repository, branch, locale, and other relevant
inputs in the query key. Equal keys must mean equivalent execution/data, not merely equal display labels. Binding two
resources to one query key intentionally shares transport/cache data but not StateBus request IDs or state entries. The
composition root must reject overlapping provider registrations; this adapter cannot discover arbitrary predicates'
overlap. This is not the not-yet-implemented generic library composer.

`execute({ signal, request, attempt, reportBytes })` is where the composition invokes its existing traced boundary Op.
No second Op/Result/DI framework is introduced. The application supplies result decoding/classification and serializable
payloads. Callback contracts are total; programmer errors in an application's channel or classifier are not a substitute
for typed operational failures. The generic LMAO causal-context/effect/replay bridge remains a separate readiness gate.

Defaults are a 250 ms cancellation grace, 100 ms measured-progress interval, and 30 seconds of StateBus demand
freshness. All are configurable. `timer` can be deterministic for tests. `demand` can replace the default pure
first-interest/stale policy. Query cache freshness and retry settings come from `query(request)`; retry is not invented
by this middleware. Choose cache and demand freshness coherently. Initial demand is automatic; manual refresh/retry is a
typed `loadRequested` event with a new logical request ID. This is a read loader, not mutation orchestration.

See `src/__tests__/fixture.ts` for a complete, typechecked StateBus channel/namespace/reducer installation. It is also a
minimal example of the composition seam applications should bind to their own public state/event contracts.

## Ordering and cancellation

An interest change publishes `loadRequested`. Only a subsequent post-reducer listener can start an admitted request.
Duplicate commands reduced in the same wave execute once. Successful cached reads publish the same success event as
network reads. Failed refresh retains previous state. Late/superseded results and refused cancellation events cannot
mutate or cancel a newer operation.

Each accepted read holds a disabled QueryObserver lease: it pins the query without creating a second fetch. Final zero
schedules release; renewed interest during grace cancels that timer and reuses the in-flight request. Replacement
observation is acquired before old observation is released. Releasing the last observer cancels the exact query,
including an offline/paused retryer that has not yet consumed its signal. Other observers and their shared work survive.
The transport must honour the query's signal; non-cooperative work cannot be forcibly stopped, but its late result is
suppressed at the application boundary.

**Ownership:** use a runtime-owned QueryClient, or ensure every independent in-flight consumer on a shared QueryClient
holds an observer lease. Arbitrary external imperative `fetchQuery` callers do not expose consumer reference counts;
they must not silently share a cancellable query without an observer. The middleware does not call broad
`cancelQueries`, clear somebody else's client, or remove cached application data on unmount. The caller owns final
QueryClient disposal.

Periodic byte samples come only from `execute`'s transport. Retry attempts reset byte counts, preserve logical request
identity, and flush final measurements before the terminal result. Multiple consumers of a matching query key receive
separate correlated result/progress events. Disposal unregisters interest/event listeners, clears grace timers, releases
owned query observations, and suppresses future publications.

## Validation and migration boundary

Run `nx lint statebus-tanstack-query` before `nx test statebus-tanstack-query`. The Bun suite drives real StateBus waves
and a real QueryClient: exact numeric/string IDs, shared and external observers, first/late interest, cached data,
admission, retries, measured bytes, offline cancellation, grace renewal, late outcomes, and disposal.

The package does not migrate application screens by itself. Keep the public CMS reducer independent; compose it into the
hosted reducer and bind separate resource namespaces at that target. Do not dual-write query-hook state and StateBus or
use the presence of these adapters as evidence that composition, no-I/O replay, or app screen migration is complete.
