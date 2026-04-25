# StateBus Core

Platform-agnostic core types and runtime for StateBus.

Use this package for:

- module augmentation of `States` and `Events`
- reducers, listeners, and typed state access
- `ManualStateBus` in tests and non-DOM environments
- `computed()` helpers that do not depend on React

If you are building a React app, import from `@smoothbricks/statebus-react` for the app-facing package surface.

## Install

```bash
bun add @smoothbricks/statebus-core
```

## Define Your App Types

Declare your application `States` and `Events` by augmenting `@smoothbricks/statebus-core`:

```ts
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
  initialState: { counter: 0 },
  reducers: {},
});

const doubledCounter = computed(bus, 'doubledCounter', (state) => state.counter.get() * 2, undefined);
```

## Public Exports

`@smoothbricks/statebus-core` exports the platform-agnostic surface, including:

- `StateBus`
- `ManualStateBus`
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
