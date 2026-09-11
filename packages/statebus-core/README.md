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
- `sortedKeyValuePairs`
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
original subscribed keys, not the caller's mutable array. This release reports property interest, not exact ByID
interest; that extension is separate.

A single interest event passes through unchanged. Multiple interest payloads use one wave-owned subscriber record,
with each incoming payload traversed once. The record is transferred at publication and is never recycled: callers
may retain old events without seeing later counts. There are no intermediate prefix copies per notification. Ordinary
waves with no interest do not construct coalescing records.

A throwing reducer is still a programmer error, not an operational-result event or a transactional rollback. Its
remaining wave is discarded, queue storage is cleared, and already-enqueued successor work remains scheduled. Earlier
state changes in that failed wave are not rolled back.

Validate with `nx lint statebus-core`, `nx test statebus-core`, and the corresponding `statebus-react` targets.
The focused [interest benchmark](https://github.com/smoothbricks/codebase/blob/main/packages/statebus-core/benchmarks/README.md) records owned-flush latency separately from allocation claims.
