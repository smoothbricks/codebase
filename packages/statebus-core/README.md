# StateBus Core

Build a typed API for a reusable library or an application. The factory assembles existing StateBus
declarations and bindings; only the returned `createBus` creates a live StateBus instance.

```ts
import { createBusApi, ManualScheduler } from '@smoothbricks/statebus-core';

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

const app = createBusApi({
  name: 'application',
  libraries: { counter },
  setup(builder, libraries) {
    const counterEvents = libraries.get('counter');
    const updates = builder.scalar('updates', () => 0);
    builder.reduce(counterEvents.increment, state => {
      state.set(updates, state.read(updates) + 1);
    });
    return { updates };
  },
});

const { createBus } = app;
const bus = createBus({ scheduler: new ManualScheduler() });
const access = counter.getBus(bus);
access.publish(access.exports.increment, 3);
bus.flush();
access.read(access.exports.count); // 3
bus.read(app.getBus(bus).exports.updates); // 1
bus.dispose();
```

`counter.createBus()` creates a standalone instance; `app.createBus()` includes the counter and
application declarations in one instance. `counter.getBus(appBus)` selects the counter's handles
inside that app bus. It neither creates nor synchronizes a second store. The same API can be
included by another API, nested or included more than once.

The React package's `createBusApi` adds `Provider`, `useBus` and fine-grained subscription hooks to
this same factory. Core stays independent of React. See the [React guide](../statebus-react/README.md)
for the complete standalone/hosted connector and repeated-library selection.

## Public factory contract

`createBusApi({ name, setup, libraries?, requires?, bindings?, version?, previousVersions? })`
returns a `BusApi<Exports, Libraries>`. `Exports` is inferred from `setup`; child names and exports
are inferred from `libraries`. A parent receives its children's resolved exports through the
second setup argument, `libraries.get(name)`. The existing builder owns scalar/keyed declarations,
event/command reducers, reactions, capabilities and required effects.

Setup is synchronous declaration work, not an operation or state initializer. It runs for each
inclusion while assembling its containing API. It must not start I/O or close over a live bus.
Initial scalar state is created only by `createBus`; keyed defaults remain lazy. Required capability
bindings and ownership references are checked before the factory returns.

| Returned function | Purpose |
| --- | --- |
| `createBus(options?)` | Create an independently owned live instance, defaulting to microtask scheduling |
| `getBus(instance, within?)` | Obtain stable typed access to this API in that instance |
| `replayScenario(scenario)` | Restore a new execution-disabled instance using the existing replay engine |
| `createCaptureEnvelope(scenario, options)` | Add the assembled declaration/library manifest and build provenance |
| `migrateCaptureEnvelope(envelope, options)` | Apply registered codec migrations against this API's declarations |
| `replayCaptureEnvelope(envelope, effects?)` | Validate and replay an envelope without executing production operations |

`BusAccess` exposes `exports`, `instance`, `read`, `readKeyed`, `publish`, `publisher` and
`library(name)`. It is a stable selection of declarations, not a state snapshot or independently
running object. `access.instance` is exactly the StateBus returned by `createBus`. The child access
returned by `app.getBus(bus).library('counter')` equals `counter.getBus(bus)` for a unique inclusion.

Including the same API twice creates distinct handles, metadata, state and effect slots. An
unqualified lookup of that API then refuses ambiguity; use the parent's typed `library(name)`
selection as `within` or as a React provider scope. Foreign-instance scopes and disposed buses
are rejected rather than silently retargeted.

## Execution, capture and lifetime

Bind effects and loaders once at the application's execution boundary using the selected exports
and the actual bus instance. `bindEffect`, `composedLoaderChannel` and `bindComposedQueryLoader`
keep their existing execution, sharing, request identity and cancellation behavior. The API factory
does not create a QueryClient or another dependency-injection/operation framework.

Each application mount, test or Storybook scenario owns its bus and operation lifetime. Unmount
React roots before disposing a shared bus. Screen cleanup releases exact interest, not the entire
application instance. `bus.drain()` and `bus.disposeAsync()` retain their actual-completion contracts.

Recording still requires declaration codecs. `recordScenario(bus)` and `recordRollingScenario(bus)`
use the same capture implementation; replay instances disable operations and QueryClient bindings.
See [REPLAY.md](./REPLAY.md) for lossless local capture versus sanitized support export and
[EXECUTION.md](./EXECUTION.md) for execution policies, performance evidence and explicit bounds.

Library namespaces and identity routing are resolved while building the API, never reconstructed
by reads, publications or renders. Stable access objects are prepared once per bus before rendering.
This does not claim zero allocations, measured throughput improvement or universal V8 monomorphism.

The lower-level composed and ambient APIs remain exported for existing boundary integrations;
[CONSUMER.md](./CONSUMER.md) documents those primitives. New application/library connectors can use
`createBusApi` without assembling mounts, compositions and React factories themselves.

## Verification

```sh
nx lint statebus-core
nx test statebus-core
nx run statebus-core:verify-packages
```

The real-package gate installs built tarballs outside the workspace, checks strict public
TypeScript declarations, and executes Node and Bun consumers under isolated and hoisted layouts.
`scripts/consumer/composed/bus-api.ts` reuses the existing synthetic inventory reducer/planner/
decoder and real ReactDOM/QueryClient tests for the unified factory. No source-path aliases or
application-specific ambient schema are required.
