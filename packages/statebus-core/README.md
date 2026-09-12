# StateBus Core

Platform-agnostic core types and runtime for StateBus.

Use this package for:

- module augmentation of `States` and `Events`
- reducers, listeners, and typed state access
- `ManualStateBus` in tests and non-DOM environments
- `MicrotaskStateBus` for application commands without waiting for a browser frame
- `computed()` helpers that do not depend on React

If you are building a React app, import from `@smoothbricks/statebus-react` for the app-facing package surface.

## Install

```bash
bun add @smoothbricks/statebus-core
```

## Define Your App Types

Declare your application `States` and `Events` by augmenting `@smoothbricks/statebus-core`:

```ts
import type { ByID } from '@smoothbricks/statebus-core';

declare module '@smoothbricks/statebus-core' {
  interface States {
    counter: number;
    post: ByID<{ title: string; body: string }>;
  }

  interface Events {
    count: {
      increment: number;
      decrement: number;
    };
  }
}
```

Make sure the declaration file is included by TypeScript.

## Basic Usage

```ts
import { ManualStateBus } from '@smoothbricks/statebus-core';

const bus = new ManualStateBus({
  initialState: {
    counter: 0,
    post: () => undefined,
  },
  reducers: {
    count: {
      increment: (state, payload) => {
        state.counter.update((value) => value + payload);
      },
      decrement: (state, payload) => {
        state.counter.update((value) => value - payload);
      },
    },
  },
});

bus.publish({ topic: 'count', type: 'increment', payload: 1 });
bus.dispatchEvents();
```

## Derived State

```ts
import { computed, ManualStateBus } from '@smoothbricks/statebus-core';

const bus = new ManualStateBus({
  initialState: { counter: 0, post: () => undefined },
  reducers: {},
});

const doubledCounter = computed(bus, 'doubledCounter', (state) => state.counter.get() * 2, undefined);
```

## Public Exports

`@smoothbricks/statebus-core` exports the platform-agnostic surface, including:

- `StateBus`
- `ManualStateBus`
- `MicrotaskStateBus`
- `computed`
- `sameViewProps`, `captureViewProps`
- `StateInterest`, nominal `StateInterestKey`, `StateInterestMap`, `StateInterestBatch`, and `StateInterestRegistry`
- bus, event, state, and reducer types
- `Computed`, `Atom`, and related signal-adjacent types needed by consumers

## Migration From `@smoothbricks/statebus`

The legacy monolithic package has been removed.

- `declare module '@smoothbricks/statebus'` -> `declare module '@smoothbricks/statebus-core'`
- `StateBus` from the old package split into:
  - `ManualStateBus` in `@smoothbricks/statebus-core`
  - `StateBus` (RAF-backed browser implementation) in `@smoothbricks/statebus-react`

## Dispatch and subscription lifecycle

`MicrotaskStateBus` batches synchronous publications into a microtask. Its callback is bound once per runtime;
listener publications are drained as subsequent waves without scheduling an extra empty microtask. `ManualStateBus`
uses the same dispatch logic with explicit flushing. The React package's `StateBus` alias remains animation-frame
based; import `MicrotaskStateBus` explicitly for command-driven application state.

All events in a wave reduce before any listener runs. Nested flush requests do not interrupt that wave. Interest
listeners receive the last count for each property, including terminal zero. Cleanup is idempotent and captures the
original subscribed addresses, not the caller's mutable array. Exact `{ key, id? }` changes accompany the aggregate
property counts. Keyed loaders must use these exact changes: numeric `7`, string `'7'`, and absent IDs are distinct.
Removing and recreating an interested ByID entry preserves its signal; final release does not evict application data.

A single interest event passes through unchanged. Multiple interest payloads use one wave-owned subscriber record,
with each incoming payload traversed once. The record is transferred at publication and is never recycled: callers
may retain old events without seeing later counts. There are no intermediate prefix copies per notification. Ordinary
waves with no interest do not construct coalescing records.

A throwing reducer is still a programmer error, not an operational-result event or a transactional rollback. Its
remaining wave is discarded, queue storage is cleared, and already-enqueued successor work remains scheduled. Earlier
state changes in that failed wave are not rolled back.

Validate with `nx lint statebus-core`, `nx test statebus-core`, and the corresponding `statebus-react` targets.
The focused [interest benchmark](https://github.com/smoothbricks/codebase/blob/main/packages/statebus-core/benchmarks/README.md) records owned-flush latency separately from allocation claims.


## Exact-interest ownership and hot paths

`stateInterestKey(address)` is a cold-path wire/fingerprint codec returning a branded `StateInterestKey`. It is not a
runtime indexing strategy. `StateInterestMap` indexes the original key/ID primitives; count reads and keyed-signal
retention checks do not construct a descriptor or encode JSON. Scalar interest is not a wildcard subscription to every
keyed value. ByID consumers acquire the exact ID they render.

Each runtime has one `StateInterestBatch`. Appending a wave visits each change once, writes reusable dirty slots, and
keeps the last count per address in first-occurrence order. A unique single payload passes through unchanged. Combining
multiple payloads allocates only the final owned change array; the runtime never mutates it after transfer. Numeric
`0` and `-0` share an ID, consistent with native Map semantics. Non-finite numeric IDs are refused on lease acquisition.

The resolved-slot cache is capped at 1,024 addresses between waves. A larger wave is not dropped: it is processed and
then its address cache is cleared. Scratch/queue arrays retain high-water capacity while clearing all used references.
This bounds cached identities, not total application state or the maximum size of a caller's wave. Lease construction,
new addresses, cache resets/growth, and published snapshots have allocation costs; warmed reads and slot updates avoid
new address encodings and intermediate collections. Public snapshot ownership is never weakened to recycle storage.

`sameViewProps` compares own enumerable primitive fields using `Object.is`, without sorting/encoding or coercion.
`captureViewProps` copies record props only when binding a new computation; the previously exported formatting/JSON
identity helpers are removed. Computation names are diagnostic labels, not identity keys. This is still the existing
ambient-schema API, not the separate value-level library composition/replay implementation.
