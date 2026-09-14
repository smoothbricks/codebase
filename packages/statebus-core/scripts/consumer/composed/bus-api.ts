import assert from 'node:assert/strict';
import { Err, Ok } from '@smoothbricks/lmao';
import {
  bindEffect,
  type BusApiLibraries,
  captureCheckpoint,
  createBusApi as createCoreBusApi,
  defineCapability,
  ManualScheduler,
  recordScenario,
  type StateBusInstance,
} from '@smoothbricks/statebus-core';
import { composedLoaderChannel, loadRequestId, previousData } from '@smoothbricks/statebus-data-loader';
import { createBusApi, type ReactBusApi } from '@smoothbricks/statebus-react';
import { bindComposedQueryLoader } from '@smoothbricks/statebus-tanstack-query';
import { QueryClient } from '@tanstack/query-core';
import { Window } from 'happy-dom';
import { act, createElement as h, StrictMode } from 'react';
import { type Adjust, numberCodec, requestId, type ShelfId, shelfId } from './codecs.js';
import { canAdjust, inventoryLibrary } from './library.js';

// Reuse the existing consumer's production reducer/planner/decoder through one API factory.
const inventory = createBusApi({
  name: 'factory-inventory',
  requires: inventoryLibrary.requires,
  bindings: [canAdjust.provide(true)],
  setup: inventoryLibrary.setup,
});
const telemetry = createCoreBusApi({
  name: 'telemetry',
  setup(builder) {
    const ticks = builder.scalar('ticks', () => 0, { codec: numberCodec });
    const tick = builder.event<number>('tick', { codec: numberCodec });
    builder.reduce(tick, (state, amount) => state.set(ticks, state.read(ticks) + amount));
    return { ticks, tick };
  },
});
const app = createBusApi({
  name: 'factory-app',
  libraries: { inventory, telemetry },
  setup(builder, libraries) {
    const model = libraries.get('inventory');
    const completed = builder.scalar('completed', () => 0, { codec: numberCodec });
    builder.reduce(model.adjusted, (state) => state.set(completed, state.read(completed) + 1));
    return { completed };
  },
});
const repeated = createBusApi({
  name: 'repeated',
  libraries: { primary: inventory, secondary: inventory },
  setup: () => ({}),
});
let passed = 0;
async function test(name: string, run: () => void | Promise<void>): Promise<void> {
  await run();
  passed++;
  console.log(`PASS bus API: ${name}`);
}

await test('one factory builds libraries and applications without creating live state', () => {
  let initialized = 0;
  const leaf = createCoreBusApi({
    name: 'leaf',
    setup(builder) {
      return { value: builder.scalar('value', () => ++initialized) };
    },
  });
  const host = createCoreBusApi({ name: 'host', libraries: { leaf }, setup: () => ({}) });
  assert.equal(initialized, 0);
  const { createBus } = host;
  const one = createBus({ scheduler: new ManualScheduler() });
  const two = createBus({ scheduler: new ManualScheduler() });
  try {
    assert.equal(initialized, 2);
    assert.equal(leaf.getBus(one).instance, one);
    assert.equal(leaf.getBus(one), leaf.getBus(one));
    assert.equal(host.getBus(one).library('leaf'), leaf.getBus(one));
    assert.notEqual(leaf.getBus(one), leaf.getBus(two));
    assert.equal(leaf.getBus(one).read(leaf.getBus(one).exports.value), 1);
    assert.equal(leaf.getBus(two).read(leaf.getBus(two).exports.value), 2);
    for (const key of ['createRuntime', 'composition', 'mounts']) assert.equal(Reflect.has(host, key), false);
  } finally {
    one.dispose();
    two.dispose();
  }
  assert.throws(() => leaf.getBus(one), /disposed/);
});

await test('two occurrences and two buses isolate handles, state, metadata and imperative access', () => {
  const a = repeated.createBus({ scheduler: new ManualScheduler() });
  const b = repeated.createBus({ scheduler: new ManualScheduler() });
  const left = repeated.getBus(a).library('primary');
  const right = repeated.getBus(a).library('secondary');
  try {
    assert.throws(() => inventory.getBus(a), /Ambiguous/);
    assert.equal(inventory.getBus(a, left), left);
    assert.equal(inventory.getBus(a, right), right);
    assert.notEqual(left.exports.selected, right.exports.selected);
    assert.notEqual(left.exports.effect.metadata.key, right.exports.effect.metadata.key);
    assert.equal(left.instance, right.instance);
    left.publish(left.exports.selectionChanged, shelfId('shelf:changed'));
    a.flush();
    assert.equal(left.read(left.exports.selected), shelfId('shelf:changed'));
    assert.equal(right.read(right.exports.selected), shelfId('shelf:a'));
    assert.equal(repeated.getBus(b).library('primary').read(left.exports.selected), shelfId('shelf:a'));
    assert.throws(() => inventory.getBus(b, left), /different StateBus/);
    assert.throws(() => app.getBus(a), /not included/);
  } finally {
    a.dispose();
    b.dispose();
  }
});

await test('nested inclusion resolves local children and ancestor API access once', () => {
  const host = createCoreBusApi({ name: 'nested', libraries: { inner: app }, setup: () => ({}) });
  const bus = host.createBus({ scheduler: new ManualScheduler() });
  try {
    const root = host.getBus(bus);
    const nested = root.library('inner');
    const child = nested.library('inventory');
    assert.equal(inventory.getBus(bus), child);
    assert.equal(app.getBus(bus, child), nested);
    assert.equal(host.getBus(bus, child), root);
    assert.equal(telemetry.getBus(bus).instance, bus);
  } finally {
    bus.dispose();
  }
});

await test('required bindings refuse before a bus or component can be created', () => {
  const permission = defineCapability<boolean>('factory-permission');
  assert.throws(
    () => createCoreBusApi({ name: 'missing', requires: [permission], setup: () => ({}) }),
    /Missing or incompatible/,
  );
  assert.throws(
    () =>
      createCoreBusApi({
        name: 'duplicate',
        requires: [permission],
        bindings: [permission.provide(true), permission.provide(false)],
        setup: () => ({}),
      }),
    /Duplicate/,
  );
});

await test('bound access reuses functions and performs no codec or serialization work on reads/publication', () => {
  const bus = app.createBus({ scheduler: new ManualScheduler() });
  const access = telemetry.getBus(bus);
  const original = JSON.stringify;
  try {
    JSON.stringify = () => {
      throw new Error('Serialization outside a capture boundary');
    };
    const publish = access.publisher(access.exports.tick);
    for (let index = 0; index < 100; index++) {
      assert.equal(telemetry.getBus(bus), access);
      assert.equal(access.publisher(access.exports.tick), publish);
      publish(1);
      bus.flush();
      assert.equal(access.read(access.exports.ticks), index + 1);
    }
  } finally {
    JSON.stringify = original;
    bus.dispose();
  }
});

const browser = new Window({ url: 'https://bus-api.example.test/' });
const saved = new Map<string, PropertyDescriptor | undefined>();
for (const [key, value] of Object.entries({
  window: browser,
  document: browser.document,
  HTMLElement: browser.HTMLElement,
  Node: browser.Node,
  IS_REACT_ACT_ENVIRONMENT: true,
})) {
  saved.set(key, Object.getOwnPropertyDescriptor(globalThis, key));
  Object.defineProperty(globalThis, key, { configurable: true, writable: true, value });
}
try {
  const { createRoot } = await import('react-dom/client');
  let selections = 0;
  let observedInstance: StateBusInstance | undefined;
  const publishers: ((command: Adjust) => void)[] = [];
  const accesses: ReturnType<typeof inventory.getBus>[] = [];
  const {
    useBus: useInventoryBus,
    useStateValue: useInventoryState,
    useKeyedState: useInventoryResource,
    useEventPublisher: useInventoryPublisher,
  } = inventory;
  const useSummary = inventory.createSelectionHook(
    'inventory.factory-summary',
    (state, model, props: { id: ShelfId }) => {
      selections++;
      return {
        stock: state.readKeyed(model.inventory, props.id),
        incoming: state.readKeyed(model.incoming, props.id),
      };
    },
    (model, props) => [model.inventory.at(props.id), model.incoming.at(props.id)],
  );
  function Connector() {
    const access = useInventoryBus();
    observedInstance = access.instance;
    accesses.push(access);
    const id = useInventoryState((model) => model.selected);
    const value = useInventoryResource((model) => model.inventory, id);
    const emit = useInventoryPublisher((model) => model.adjust);
    publishers.push(emit);
    const summary = useSummary({ id });
    return h(
      'button',
      {
        type: 'button',
        onClick: () => emit({ shelfId: id, requestId: requestId('request:click'), add: 1 }),
      },
      `${id}:${previousData(value)?.value ?? value.kind}:${summary.incoming.kind}`,
    );
  }
  function connect(bus: StateBusInstance, scope = inventory.getBus(bus), pendingMutation?: Promise<number>) {
    const model = scope.exports;
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false, gcTime: Number.POSITIVE_INFINITY } },
    });
    let sequence = 0;
    let reads = 0;
    let mutations = 0;
    for (const [state, event, value] of [
      [model.inventory, model.load, 5],
      [model.incoming, model.loadIncoming, 2],
    ] as const) {
      const binding = composedLoaderChannel(bus, state, event);
      bindComposedQueryLoader(binding, {
        queryClient,
        requestId: () => loadRequestId(`factory-read-${++sequence}`),
        now: () => 0,
        failure: () => 'network',
        query: (request) => ({
          queryKey: [state.metadata.key, binding.resourceId(request.interest)],
          execute: async () => {
            reads++;
            return value;
          },
        }),
      });
    }
    bindEffect(bus, model.effect, {
      execute: async (plan) => {
        mutations++;
        return new Ok(pendingMutation ? await pendingMutation : plan.next);
      },
      failure: () => new Err('network'),
    });
    return { queryClient, reads: () => reads, mutations: () => mutations };
  }

  async function exerciseConnector<Exports, Libraries extends BusApiLibraries>(
    api: ReactBusApi<Exports, Libraries>,
    hosted: boolean,
  ): Promise<void> {
    const { createBus, Provider } = api;
    const bus = createBus({ scheduler: new ManualScheduler() });
    const model = inventory.getBus(bus).exports;
    const execution = connect(bus);
    const recorder = recordScenario(bus);
    const node = document.createElement('div');
    const root = createRoot(node);
    try {
      await act(() => root.render(h(StrictMode, null, h(Provider, { bus }, h(Connector)))));
      await act(() => bus.drain());
      assert.equal(observedInstance, bus, 'Library hooks must use the host bus, never a nested store.');
      assert.equal(execution.reads(), 2, 'Stock and incoming demand load independently.');
      assert.ok(node.textContent?.includes('shelf:a:5:ready'));
      const command = { shelfId: shelfId('shelf:a'), requestId: requestId('request:factory'), add: 3 };
      await act(async () => {
        bus.publish(model.adjust, command);
        bus.publish(model.adjust, command);
        await bus.drain();
      });
      assert.equal(execution.mutations(), 1);
      assert.ok(node.textContent?.includes('shelf:a:8:ready'));
      if (hosted) {
        const before = selections;
        const host = app.getBus(bus);
        const child = host.library('telemetry');
        await act(() => {
          bus.publish(child.exports.tick, 1);
          bus.flush();
        });
        assert.equal(selections, before, 'Unrelated host state cannot reconstruct library snapshots.');
        assert.equal(bus.read(host.exports.completed), 1);
      }
      const capture = recorder.snapshot();
      const replay = api.replayScenario(capture);
      try {
        assert.equal(inventory.getBus(replay).instance, replay);
        assert.deepEqual(captureCheckpoint(replay), captureCheckpoint(bus));
        let io = 0;
        bindEffect(replay, inventory.getBus(replay).exports.effect, {
          execute: () => {
            io++;
            return new Ok(0);
          },
          failure: () => new Err('forbidden'),
        });
        assert.equal(io, 0);
      } finally {
        replay.dispose();
      }
    } finally {
      await act(() => root.unmount());
      bus.flush();
      assert.deepEqual(bus.interestSource.snapshot(), []);
      recorder.dispose();
      bus.dispose();
      execution.queryClient.clear();
    }
  }
  await test('the unchanged library connector works standalone or under only its host Provider', async () => {
    await exerciseConnector(inventory, false);
    await exerciseConnector(app, true);
  });

  await test('explicit repeated-library selection and bus switching replace leases without replacing the connector', async () => {
    const a = repeated.createBus({ scheduler: new ManualScheduler() });
    const b = repeated.createBus({ scheduler: new ManualScheduler() });
    const left = repeated.getBus(a).library('primary');
    const right = repeated.getBus(a).library('secondary');
    const other = repeated.getBus(b).library('primary');
    const execution = [connect(a, left), connect(a, right), connect(b, other)];
    const node = document.createElement('div');
    const root = createRoot(node);
    const render = async (bus: StateBusInstance, scope: typeof left) => {
      await act(() => root.render(h(repeated.Provider, { bus }, h(inventory.Provider, { scope }, h(Connector)))));
      await act(() => bus.drain());
    };
    try {
      await render(a, left);
      const previous = accesses.at(-1);
      const publisher = publishers.at(-1);
      await render(a, left);
      assert.equal(accesses.at(-1), previous);
      assert.equal(publishers.at(-1), publisher);
      await render(a, right);
      a.flush();
      assert.equal(accesses.at(-1), right);
      assert.notEqual(publishers.at(-1), publisher);
      assert.equal(
        a.interestSource.snapshot().some((entry) => entry.interest.key === left.exports.inventory.metadata.key),
        false,
      );
      await render(b, other);
      a.flush();
      assert.equal(observedInstance, b);
      assert.deepEqual(a.interestSource.snapshot(), []);
      assert.equal(accesses.at(-1), other);
    } finally {
      await act(() => root.unmount());
      b.flush();
      assert.deepEqual(b.interestSource.snapshot(), []);
      a.dispose();
      b.dispose();
      for (const boundary of execution) boundary.queryClient.clear();
    }
  });

  await test('a late old-resource mutation cannot retarget the new factory-hook selection', async () => {
    const bus = app.createBus({ scheduler: new ManualScheduler() });
    const scope = inventory.getBus(bus);
    const model = scope.exports;
    const pending = Promise.withResolvers<number>();
    const execution = connect(bus, scope, pending.promise);
    const node = document.createElement('div');
    const root = createRoot(node);
    const oldId = shelfId('shelf:a');
    const newId = shelfId('shelf:b');
    try {
      await act(() => root.render(h(app.Provider, { bus }, h(Connector))));
      await act(() => bus.drain());
      await act(async () => {
        bus.publish(model.adjust, { shelfId: oldId, requestId: requestId('request:late'), add: 3 });
        bus.flush();
        bus.publish(model.selectionChanged, newId);
        bus.flush();
      });
      await act(async () => {
        for (let step = 0; step < 24; step++) {
          await Promise.resolve();
          bus.flush();
        }
      });
      assert.equal(execution.mutations(), 1);
      assert.ok(node.textContent?.includes('shelf:b:5:ready'));
      assert.equal(
        bus.interestSource.snapshot().some((entry) => entry.interest.id === oldId),
        false,
      );
      await act(async () => {
        pending.resolve(8);
        await bus.drain();
      });
      assert.ok(node.textContent?.includes('shelf:b:5:ready'));
      assert.equal(previousData(bus.readKeyed(model.inventory, oldId))?.value, 8);
      assert.equal(previousData(bus.readKeyed(model.inventory, newId))?.value, 5);
    } finally {
      pending.resolve(8);
      await act(() => bus.drain());
      await act(() => root.unmount());
      bus.flush();
      assert.deepEqual(bus.interestSource.snapshot(), []);
      bus.dispose();
      execution.queryClient.clear();
    }
  });

  function negativeTypes(): void {
    // @ts-expect-error Library keys come from the declared child APIs.
    app.getBus(app.createBus()).library('missing');
    // @ts-expect-error Branded IDs remain inferred through factory-bound subscription hooks.
    inventory.useKeyedState((model) => model.inventory, 'shelf:unbranded');
    // @ts-expect-error Factory-bound publishers retain the actual command payload.
    inventory.useEventPublisher((model) => model.adjust)('untyped');
    // @ts-expect-error An included library is an API, not a live bus instance.
    createCoreBusApi({ name: 'invalid', libraries: { inventory: inventory.createBus() }, setup: () => ({}) });
  }
  void negativeTypes;
} finally {
  await browser.happyDOM.close();
  for (const [key, descriptor] of saved) {
    if (descriptor) Object.defineProperty(globalThis, key, descriptor);
    else Reflect.deleteProperty(globalThis, key);
  }
}
console.log(JSON.stringify({ busApiScenarios: passed, builtExports: true }));
