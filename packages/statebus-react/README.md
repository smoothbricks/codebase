# StateBus React

React bindings and browser scheduling for StateBus.

`@smoothbricks/statebus-react` is the app-facing package for React applications. It broadly re-exports
`@smoothbricks/statebus-core`, then adds:

- `AnimationFrameStateBus`
- `StateBus` as the browser-friendly alias for `AnimationFrameStateBus`
- `StatebusProvider`
- `useBus`, `useStateBus`, `useSubstate`
- `computedHook`
- `track` and `useStateTracking`

## Install

```bash
bun add @smoothbricks/statebus-react react
```

Type declarations still live on `@smoothbricks/statebus-core`, so your augmentation file should target that module.

## Define Your App Types

```ts
declare module '@smoothbricks/statebus-core' {
  interface States {
    counter: number;
  }

  interface Events {
    count: {
      increment: number;
      decrement: number;
    };
  }
}
```

## React Usage

```tsx
import { MicrotaskStateBus, StatebusProvider, useBus, useSubstate } from '@smoothbricks/statebus-react';

export const createCounterRuntime = () => new MicrotaskStateBus({
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

function Counter() {
  const bus = useBus();
  const counter = useSubstate('counter');

  return (
    <div>
      <p>Count: {counter}</p>
      <button onClick={() => bus.count.increment(1)}>Increment</button>
    </div>
  );
}

export function App({ eventBus }: { eventBus: ReturnType<typeof createCounterRuntime> }) {
  return (
    <StatebusProvider value={eventBus}>
      <Counter />
    </StatebusProvider>
  );
}
```

## Testing

For tests, import `ManualStateBus` from `@smoothbricks/statebus-react` or `@smoothbricks/statebus-core`.

```ts
import { ManualStateBus } from '@smoothbricks/statebus-react';
```

## Migration From `@smoothbricks/statebus`

- `import { useBus, useSubstate } from '@smoothbricks/statebus/react'` ->
  `import { useBus, useSubstate } from '@smoothbricks/statebus-react'`
- `declare module '@smoothbricks/statebus'` -> `declare module '@smoothbricks/statebus-core'`
- `StateBus` is now exported from `@smoothbricks/statebus-react`

## Application command scheduling

`MicrotaskStateBus` is re-exported from core. Use it explicitly when application commands should reduce in a microtask
rather than wait for an animation frame. It uses the same `StatebusProvider` and typed hooks. The existing `StateBus`
alias still refers to `AnimationFrameStateBus`; this release does not silently change existing applications' scheduling.


## Stable bindings and exact demand

`useSubstate(key, id)` acquires an exact interest lease in its effect. Unchanged primitive addresses reuse the lease and
signal; they do not create descriptor arrays or JSON identity strings on every render. Runtime/ID changes release the
old lease and acquire the new one. StrictMode churn and removal/recreation of an interested ByID entry are covered by
real ReactDOM tests.

`computedHook` accepts primitive props or plain records of primitive props, and optional static interests or a pure
`props => interests` function. Equivalent props reuse the existing computation without sorting/serialization. Meaningful
changes capture the new props once. This binding cache uses React-managed render state, not a render-mutated ref:
concurrent/suspended renders cannot overwrite the committed binding. The cache is internal renderer bookkeeping, not
application/domain state. Changing unrelated view props does not churn unchanged resource interest.

`useBus(topic)` and `useBus()` memoize their facade per runtime. A topic/event property binds its publisher function on
first access and returns the same function on subsequent reads. A changed provider gets new publishers; old callbacks
never silently retarget another runtime. The cache belongs to the facade, not a process-global registry. Event envelopes
are owned publications, not reusable objects. Do not use this typed facade as an unbounded dynamic-string interner.

Create a runtime at the composition boundary and pass it to `App`; create a fresh runtime per isolated test/story. None
of these hooks introduces a module-global runtime or a QueryClient provider. The app-bound generic library composer is
separate work; this package does not claim the full Conloca migration gate is complete.
