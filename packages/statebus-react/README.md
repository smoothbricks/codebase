# StateBus React

One `createBusApi` factory builds the API for either a reusable library or an application. It
returns ready-to-use hooks, a provider, and a function that creates independent StateBus instances.
A hosted library's hooks use the application's bus, not a second library store.

## Build the library API

```tsx
import { createBusApi } from '@smoothbricks/statebus-react';

export const counter = createBusApi({
  name: 'counter',
  setup(builder) {
    const count = builder.scalar('count', () => 0);
    const increment = builder.event<number>('increment');
    builder.reduce(increment, (state, amount) => {
      state.set(count, state.read(count) + amount);
    });
    return { count, increment };
  },
});

export const { useBus, useStateValue, useEventPublisher } = counter;

export function Counter() {
  const count = useStateValue(model => model.count);
  const increment = useEventPublisher(model => model.increment);
  return <button type="button" onClick={() => increment(1)}>{count}</button>;
}
```

The setup function declares state and reducers but does not create live state or perform I/O.
The returned functions are bound to this library's identity, not to a particular standalone bus.
Destructuring them does not lose a `this` binding. Their types are inferred from setup's exports.

## Use it standalone or inside an application

A standalone entry point creates its own bus and supplies it to the provider:

```tsx
const { createBus, Provider } = counter;
const bus = createBus();
root.render(<Provider bus={bus}><Counter /></Provider>);
// At the real application boundary: root.unmount(); await bus.disposeAsync();
```

A host uses the same factory and the unchanged library component:

```tsx
const app = createBusApi({
  name: 'application',
  libraries: { counter },
  setup(builder, libraries) {
    const publicCounter = libraries.get('counter');
    const updates = builder.scalar('updates', () => 0);
    builder.reduce(publicCounter.increment, state => {
      state.set(updates, state.read(updates) + 1);
    });
    return { updates };
  },
});

const { createBus, Provider, useBus } = app;
const bus = createBus();
root.render(<Provider bus={bus}><Counter /></Provider>);
```

These snippets are separate entry points. `root` is the application's React root. No extra counter
provider is needed for a unique inclusion. `counter.useBus().instance` and `app.useBus().instance`
refer to that exact same live bus, while their `exports` contain the appropriate typed handles.
A public library never imports its host's state or proprietary types.

`useBus()` returns stable `BusAccess`: typed `exports`, the actual `instance`, reader/publication
functions and `library(name)` for included APIs. It **does not subscribe to a whole-state snapshot**.
Rendering values requires a state/selection hook. Repeated calls return the same access object until
the provider selects another bus or library occurrence.

## Fine-grained reads, events and selections

| Returned hook | Signature and behavior |
| --- | --- |
| `useBus()` | Stable access to this API in the provider's bus; no state subscription |
| `useStateValue(model => model.scalar)` | Subscribe to one scalar/resource handle |
| `useKeyedState(model => model.keyed, id)` | Subscribe to one keyed atom; preserves the handle's branded ID type |
| `useEventPublisher(model => model.event)` | Stable typed event publisher; no state subscription |
| `createSelectionHook(name, select, interests?)` | Build a hook whose computed value depends on selected atoms and primitive props |

The scalar/keyed/event selector chooses a declaration from the library's resolved exports, not a
snapshot. The binding keys on the resulting handle, bus and resource ID, **not the selector lambda's
identity**. Inline selectors therefore do not resubscribe merely because a component renders again.

For a resource library exporting `stock` and `incoming`, a computed hook has this shape:

```ts
const useSummary = inventory.createSelectionHook(
  'inventory.summary',
  (state, model, props: { id: ShelfId }) => ({
    stock: state.readKeyed(model.stock, props.id),
    incoming: state.readKeyed(model.incoming, props.id),
  }),
  (model, props) => [model.stock.at(props.id), model.incoming.at(props.id)],
);
```

`inventory` and `ShelfId` above are the application's declared API and branded ID type. The complete
synthetic example is `statebus-core/scripts/consumer/composed/bus-api.ts` and its local library/codecs.

Hooks reuse the existing per-atom external-store subscriptions and committed computed binding.
Unrelated host state does not rebuild library snapshots. Switching IDs releases old interest and
acquires new interest. StrictMode cleanup releases leases; it does not dispose the shared bus.

## Include a library twice

Repeated inclusion is intentionally explicit at the subtree that needs a particular occurrence:

```tsx
const app = createBusApi({
  name: 'comparison',
  libraries: { left: counter, right: counter },
  setup: () => ({}),
});
const bus = app.createBus();
const selected = app.getBus(bus).library('left');

root.render(
  <app.Provider bus={bus}>
    <counter.Provider scope={selected}>
      <Counter />
    </counter.Provider>
  </app.Provider>,
);
```

The scope only selects that occurrence's handles inside the existing bus. It allocates no second
state store and starts no operations. Unqualified `counter.useBus()` under this app refuses the
ambiguity instead of choosing a random/first occurrence. A scope from another bus is rejected.
Scopes and publishers are stable; changing the scope replaces subscriptions correctly. Nested APIs
resolve the nearest selected occurrence and can still resolve unambiguous ancestor APIs.

## Execution boundary, testing and replay

`api.getBus(bus)` provides the same typed access outside React. Use its exports when binding the
existing `bindEffect`, `composedLoaderChannel` and `bindComposedQueryLoader` APIs. The application
owns QueryClient, production clients, navigation, request IDs, tracing contexts and disposal.
There is no new operation framework or implicit global bus.

Providers call the existing required-effect preflight before rendering children. They never create,
dispose or replace an instance on their own. The core factory can also create included libraries
without depending on React; its recipe identity works with the React factory's host provider.

```ts
const bus = app.createBus({ scheduler: new ManualScheduler() });
const recorder = recordScenario(bus);
// Drive the same reducers, planners and decoders through real events/controlled operations.
const replay = app.replayScenario(recorder.snapshot());
// replay is another StateBus with effect/query execution disabled.
```

`ManualScheduler` and `recordScenario` are core exports, also re-exported by the React package.
Supply codecs before recording. Each test/story creates and disposes its own bus. Envelope capture,
owned migrations and replay remain available through `createCaptureEnvelope`,
`migrateCaptureEnvelope` and `replayCaptureEnvelope` on the returned API; see the
[core guide](../statebus-core/README.md) and [replay contract](../statebus-core/REPLAY.md).

The existing package-level ambient hooks and lower-level composition bindings remain exported;
import the new hooks from your returned API, rather than mixing its provider with ambient `useBus`.
The factory's instance is the existing composed engine, not the separate animation-frame alias.

## Verification

`nx run statebus-core:verify-packages` checks strict declarations and executes this factory's
standalone/hosted/repeated-library, real ReactDOM StrictMode, exact loader interest, duplicate-command,
late-resource-result and no-I/O replay scenarios through actual built package exports under
Node/Bun and isolated/hoisted installs. The existing browser and package suites remain enabled.

Names and library routing are resolved during API construction. Per-bus access objects are prepared
before rendering. No schema reconstruction, resource stringification or global snapshot is added
to ordinary reads/publications. This is an implementation contract, not a claim of measured
allocation, GC or latency improvement for the factory itself.
