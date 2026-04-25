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
import { StateBus, StatebusProvider, useBus, useSubstate } from '@smoothbricks/statebus-react';

const eventBus = new StateBus({
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

export function App() {
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
