import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { Err, Ok, type Result } from '@smoothbricks/lmao';
import {
  bindEffect,
  type ComposedRuntime,
  captureCheckpoint,
  captureEffectOutcome,
  classifyScenario,
  composeLibraries,
  decodeEffectOutcome,
  defineCapability,
  defineEffect,
  defineLibrary,
  ManualScheduler,
  mountLibrary,
  publishEffectOutcome,
  type RecordedEffectOutcome,
  recordScenario,
  replayScenario,
  type ScalarHandle,
  type StateInterestChange,
} from '@smoothbricks/statebus-core';
import {
  composedLoaderChannel,
  type LoadRequest,
  loadFingerprint,
  loadRequestId,
  previousData,
  type TimerPort,
} from '@smoothbricks/statebus-data-loader';
import {
  createLibraryReact,
  createSelectionHook,
  createStateBusReact,
  useEventPublisher,
  useKeyedState,
  useStateValue,
} from '@smoothbricks/statebus-react';
import { bindComposedQueryLoader } from '@smoothbricks/statebus-tanstack-query';
import { QueryClient } from '@tanstack/query-core';
import { Window } from 'happy-dom';
import { act, createElement, StrictMode } from 'react';
import { numberCodec, requestId, type ShelfId, shelfId } from './codecs.js';
import { canAdjust, hostLibrary, type Inventory, inventoryLibrary } from './library.js';

class Timer implements TimerPort {
  now = 0;
  jobs: { at: number; run: () => void; active: boolean }[] = [];
  after(milliseconds: number, run: () => void): () => void {
    const job = { at: this.now + milliseconds, run, active: true };
    this.jobs.push(job);
    return () => {
      job.active = false;
    };
  }
  advance(milliseconds: number): void {
    this.now += milliseconds;
    for (const job of this.jobs)
      if (job.active && job.at <= this.now) {
        job.active = false;
        job.run();
      }
    this.jobs = this.jobs.filter((job) => job.active);
  }
}
async function settle(runtime: ComposedRuntime) {
  for (let index = 0; index < 12; index++) {
    await Promise.resolve();
    runtime.flush();
  }
}
const firstId = shelfId('shelf:a');
const secondId = shelfId('shelf:b');
const left = mountLibrary(inventoryLibrary, 'left', [canAdjust.provide(true)]);
const right = mountLibrary(inventoryLibrary, 'right', [canAdjust.provide(true)]);
const host = mountLibrary(hostLibrary, 'host');
const composition = composeLibraries(left, right, host);
const scheduler = new ManualScheduler();
const failures: unknown[] = [];
const runtime = composition.createRuntime({ scheduler, onError: (cause) => failures.push(cause) });
const otherRuntime = composition.createRuntime({ scheduler: new ManualScheduler() });
const recorder = recordScenario(runtime, { maxEvents: 1000 });
const l = left.exports;
const r = right.exports;
const browser = new Window({ url: 'https://inventory.example.test/' });
const saved = new Map<string, PropertyDescriptor | undefined>();
for (const [key, value] of Object.entries({
  window: browser,
  document: browser.document,
  HTMLElement: browser.HTMLElement,
  Node: browser.Node,
  IS_REACT_ACT_ENVIRONMENT: true,
})) {
  saved.set(key, Object.getOwnPropertyDescriptor(globalThis, key));
  Object.defineProperty(globalThis, key, { value, writable: true, configurable: true });
}
const { createRoot } = await import('react-dom/client');
const App = createStateBusReact(composition);
const Library = createLibraryReact(inventoryLibrary);
let renders = 0;
const published: unknown[] = [];
function Connector() {
  const model = Library.useLibrary();
  const id = useStateValue(model.selected);
  const data = useKeyedState(model.inventory, id);
  const incoming = useKeyedState(model.incoming, id);
  const publish = useEventPublisher(model.adjust);
  published.push(publish);
  renders++;
  return createElement(
    'output',
    null,
    `${id}:${previousData(data)?.value ?? 'loading'}:${previousData(incoming)?.value ?? 'loading'}`,
  );
}
const rootNode = document.createElement('div');
const root = createRoot(rootNode);
const render = (bus = runtime, mount = left) =>
  act(() =>
    root.render(
      createElement(
        StrictMode,
        null,
        createElement(
          App.Provider,
          { runtime: bus },
          createElement(Library.Provider, { mount }, createElement(Connector)),
        ),
      ),
    ),
  );
const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: false, gcTime: Number.POSITIVE_INFINITY } },
});
const timer = new Timer();
let sequence = 0;
let loadCalls = 0;
const oldRead = Promise.withResolvers<number>();
const freshRead = Promise.withResolvers<number>();
const reads = new Map<ShelfId, PromiseWithResolvers<number>>([
  [firstId, oldRead],
  [secondId, freshRead],
]);
let readAborts = 0;
function installRead(model: Inventory, field: 'inventory' | 'incoming') {
  const state = model[field];
  const channel = composedLoaderChannel(runtime, state, field === 'inventory' ? model.load : model.loadIncoming);
  return bindComposedQueryLoader(channel, {
    queryClient,
    timer,
    graceMs: 250,
    requestId: () => loadRequestId(`read:${++sequence}`),
    now: () => timer.now,
    fingerprint: (interest) => loadFingerprint(`${field}/${String(interest.id)}`),
    failure: () => 'network',
    query: (request) => ({
      queryKey: [state.metadata.key, state.resourceId(request.interest)],
      execute: async ({ signal }) => {
        loadCalls++;
        signal.addEventListener(
          'abort',
          () => {
            readAborts++;
          },
          { once: true },
        );
        if (field === 'incoming') return 3;
        const read = reads.get(state.resourceId(request.interest));
        if (!read) throw new Error('Unexpected read');
        return read.promise;
      },
    }),
  });
}
const names: string[] = [];
async function test(name: string, run: () => void | Promise<void>) {
  await run();
  names.push(name);
  console.log(`PASS ${name}`);
}
try {
  await test('composition rejects duplicate ownership and missing/incompatible bindings before runtime creation', () => {
    assert.throws(() => composeLibraries(left, left), /Duplicate ownership/);
    assert.throws(
      () => composeLibraries(left, mountLibrary(inventoryLibrary, 'left', [canAdjust.provide(true)])),
      /Duplicate ownership/,
    );
    assert.throws(() => mountLibrary(inventoryLibrary, 'missing'), /Missing/);
    assert.throws(
      () => mountLibrary(inventoryLibrary, 'wrong', [defineCapability<boolean>('other').provide(true)]),
      /incompatible/,
    );
    assert.notEqual(l.inventory.metadata.key, r.inventory.metadata.key);
    assert.notEqual(l.effect.metadata.key, r.effect.metadata.key);
  });
  await test('required state handles retain types and reject a missing owning library during composition', () => {
    const requirement = defineCapability<ScalarHandle<number>>('public.counter');
    const dependent = defineLibrary({
      name: 'dependent',
      requires: [requirement],
      setup(scope) {
        const source = scope.require(requirement);
        const copied = scope.scalar('copied', () => 0, { codec: numberCodec });
        const copy = scope.event<number>('copy', { codec: numberCodec });
        scope.reduce(copy, (state) => state.set(copied, state.read(source)));
        return { copied, copy };
      },
    });
    const mounted = mountLibrary(dependent, 'dependent', [requirement.provide(host.exports.ticks)]);
    assert.throws(() => composeLibraries(mounted), /uncomposed/);
    const bus = composeLibraries(host, mounted).createRuntime({ scheduler: new ManualScheduler() });
    bus.publish(host.exports.tick, 7);
    bus.publish(mounted.exports.copy, 0);
    bus.flush();
    assert.equal(bus.read(mounted.exports.copied), 7);
    bus.dispose();
  });
  await test('the identical library connector runs standalone and hosted without allocating a library store', async () => {
    const standaloneMount = mountLibrary(inventoryLibrary, 'standalone', [canAdjust.provide(false)]);
    const standaloneComposition = composeLibraries(standaloneMount);
    const standalone = standaloneComposition.createRuntime({ scheduler: new ManualScheduler() });
    const Standalone = createStateBusReact(standaloneComposition);
    const node = document.createElement('div');
    const standaloneRoot = createRoot(node);
    await act(() =>
      standaloneRoot.render(
        createElement(
          Standalone.Provider,
          { runtime: standalone },
          createElement(Library.Provider, { mount: standaloneMount }, createElement(Connector)),
        ),
      ),
    );
    standalone.flush();
    await act(() => {
      standalone.publish(standaloneMount.exports.selectionChanged, secondId);
      standalone.flush();
    });
    assert.equal(node.textContent, 'shelf:b:loading:loading');
    assert.equal(standalone.interestSource.snapshot().length, 3);
    assert.equal(runtime.interestSource.snapshot().length, 0);
    assert.equal(runtime.read(l.selected), firstId);
    await act(() => standaloneRoot.unmount());
    standalone.flush();
    assert.equal(standalone.interestSource.snapshot().length, 0);
    standalone.dispose();
  });
  const interestEvents: (readonly StateInterestChange[])[] = [];
  runtime.interestSource.subscribe((changes) => interestEvents.push(changes));
  await test('real StrictMode subscriptions expose exact initial demand to late-installed loaders', async () => {
    await render();
    runtime.flush();
    assert.equal(runtime.interestSource.snapshot().filter((entry) => entry.interest.id === firstId).length, 2);
    assert.equal(
      runtime.interestSource.snapshot().find((entry) => entry.interest.key === l.inventory.metadata.key)?.subscribers,
      1,
    );
    installRead(l, 'inventory');
    installRead(l, 'incoming');
    await act(async () => {
      await settle(runtime);
    });
    assert.equal(loadCalls, 2);
    assert.equal(readAborts, 0);
    assert.equal(rootNode.textContent, 'shelf:a:loading:3');
    const retained = structuredClone(interestEvents);
    const second = runtime.acquire([l.inventory.at(firstId)]);
    runtime.flush();
    second();
    runtime.flush();
    assert.deepEqual(interestEvents.slice(0, retained.length), retained);
  });
  await test('two mounts and two runtime instances share neither state, events nor demand', () => {
    assert.equal(runtime.readKeyed(r.inventory, firstId).kind, 'not-requested');
    assert.equal(otherRuntime.readKeyed(l.inventory, firstId).kind, 'not-requested');
    assert.equal(otherRuntime.interestSource.snapshot().length, 0);
    runtime.publish(r.selectionChanged, secondId);
    runtime.flush();
    assert.equal(runtime.read(l.selected), firstId);
    assert.equal(runtime.read(r.selected), secondId);
    assert.equal(otherRuntime.read(r.selected), firstId);
    assert.equal(runtime.publisher(l.adjust), runtime.publisher(l.adjust));
  });
  await test('unchanged publishers/subscriptions and unrelated state updates do not rerender the connector', async () => {
    const before = renders;
    const interestsBefore = interestEvents.length;
    await act(() => {
      runtime.publish(host.exports.tick, 1);
      runtime.flush();
    });
    assert.equal(renders, before);
    assert.equal(interestEvents.length, interestsBefore);
    assert.equal(published.at(-1), published.at(-2));
  });
  await test('resource changes release old exact demand; grace preserves shared requests', async () => {
    const shared = runtime.acquire([l.inventory.at(firstId)]);
    runtime.flush();
    await act(async () => {
      runtime.publish(l.selectionChanged, secondId);
      runtime.flush();
      await settle(runtime);
    });
    timer.advance(300);
    await settle(runtime);
    assert.equal(readAborts, 0);
    assert.equal(
      runtime.interestSource
        .snapshot()
        .find((entry) => entry.interest.key === l.inventory.metadata.key && entry.interest.id === firstId)?.subscribers,
      1,
    );
    await act(async () => {
      freshRead.resolve(20);
      await settle(runtime);
    });
    assert.equal(rootNode.textContent, 'shelf:b:20:3');
    await act(async () => {
      oldRead.resolve(10);
      await settle(runtime);
    });
    assert.equal(
      rootNode.textContent,
      'shelf:b:20:3',
      'An old-resource completion must not replace the current selection.',
    );
    shared();
    runtime.flush();
    assert.ok(
      interestEvents.some((changes) =>
        changes.some(
          (entry) =>
            entry.interest.key === l.inventory.metadata.key && entry.interest.id === firstId && entry.subscribers === 0,
        ),
      ),
    );
  });
  const mutation = Promise.withResolvers<Result<number, string>>();
  let mutations = 0;
  const outcomes: RecordedEffectOutcome[] = [];
  const effect = bindEffect(runtime, l.effect, {
    execute: async (plan) => {
      mutations++;
      assert.equal(plan.requestId, requestId('request:adjust-1'));
      assert.equal(plan.next, 25);
      return mutation.promise;
    },
    failure: () => new Err('network'),
    capture: (outcome) => outcomes.push(captureEffectOutcome(l.effect, outcome)),
  });
  await test('one admitted mutation executes once for same-wave duplicate submissions', async () => {
    const command = { shelfId: secondId, requestId: requestId('request:adjust-1'), add: 5 };
    await act(() => {
      runtime.publish(l.adjust, command);
      runtime.publish(l.adjust, command);
      runtime.flush();
    });
    assert.equal(mutations, 1);
    assert.equal(runtime.readKeyed(l.pending, secondId), command.requestId);
    await act(async () => {
      mutation.resolve(new Ok(25));
      await settle(runtime);
    });
    assert.equal(rootNode.textContent, 'shelf:b:25:3');
    assert.equal(outcomes.length, 1);
    runtime.publish(l.adjust, command);
    runtime.flush();
    assert.equal(mutations, 1, 'Already completed request IDs remain refused by domain admission.');
    assert.equal(runtime.readKeyed(r.inventory, secondId).kind, 'not-requested');
    assert.equal(otherRuntime.readKeyed(l.inventory, secondId).kind, 'not-requested');
  });
  await test('captured LMAO outcomes round-trip through the same pure decoder and reject a foreign mount', () => {
    const recorded = outcomes[0];
    assert.ok(recorded);
    const value = decodeEffectOutcome(l.effect, recorded);
    assert.equal(value.plan.requestId, requestId('request:adjust-1'));
    assert.deepEqual(l.effect.decode(value.plan, value.outcome), {
      kind: 'ok',
      shelfId: secondId,
      requestId: requestId('request:adjust-1'),
      value: 25,
    });
    assert.throws(() => decodeEffectOutcome(r.effect, recorded), /Incompatible/);
  });
  await test('computed selections preserve snapshot identity and skip unrelated recomputation', () => {
    let selections = 0;
    const binding = runtime.selection(
      'inventory-summary',
      (state) => {
        selections++;
        return { value: previousData(state.readKeyed(l.inventory, secondId))?.value };
      },
      [l.inventory.at(secondId)],
    );
    let changes = 0;
    const stop = binding.subscribe(() => {
      changes++;
    });
    runtime.flush();
    const before = binding.getSnapshot();
    const reads = selections;
    runtime.publish(host.exports.tick, 1);
    runtime.flush();
    assert.equal(binding.getSnapshot(), before);
    assert.equal(selections, reads);
    assert.equal(changes, 0);
    stop();
    runtime.flush();
  });
  await test('React runtime switching releases old subscriptions and binds the same connector to the supplied runtime', async () => {
    await render(otherRuntime);
    runtime.flush();
    otherRuntime.flush();
    assert.equal(runtime.interestSource.snapshot().length, 0);
    assert.equal(rootNode.textContent, 'shelf:a:loading:loading');
    assert.equal(otherRuntime.interestSource.snapshot().length, 3);
    await render(runtime);
    await act(async () => {
      await settle(runtime);
    });
    otherRuntime.flush();
    assert.equal(otherRuntime.interestSource.snapshot().length, 0);
    assert.equal(rootNode.textContent, 'shelf:b:25:3');
  });
  await test('React unmount emits final zero; encoded checkpoint/events replay to equivalent final state with no I/O', async () => {
    await act(() => root.unmount());
    runtime.flush();
    assert.equal(runtime.interestSource.snapshot().length, 0);
    const scenario = recorder.snapshot();
    // Compare explicit checkpoints; the edge-case suite separately exercises validated JSON transport.
    const replay = replayScenario(composition, scenario);
    let io = 0;
    bindEffect(replay, l.effect, {
      execute: async () => {
        io++;
        throw new Error('I/O disabled');
      },
      failure: () => new Err('unexpected'),
    });
    bindComposedQueryLoader(composedLoaderChannel(replay, l.inventory, l.load), {
      queryClient,
      requestId: () => loadRequestId('forbidden'),
      now: () => 0,
      failure: () => 'forbidden',
      query: () => ({
        queryKey: ['forbidden'],
        execute: async () => {
          io++;
          throw new Error('I/O disabled');
        },
      }),
    });
    assert.deepEqual(captureCheckpoint(replay), captureCheckpoint(runtime));
    assert.equal(io, 0);
    const decoded = decodeEffectOutcome(l.effect, outcomes[0]);
    publishEffectOutcome(replay, l.effect, decoded);
    replay.flush(); // already-completed outcome is a pure no-op
    assert.deepEqual(captureCheckpoint(replay), captureCheckpoint(runtime));
    replay.dispose();
    assert.equal(mutations, 1);
    assert.equal(failures.length, 0);
  });
  await test('support filtering and bounded retention explicitly refuse incomplete replay', () => {
    const scenario = classifyScenario(recorder.snapshot(), (classification) => classification === 'public');
    assert.equal(scenario.complete, false);
    assert.throws(() => replayScenario(composition, scenario), /Incomplete/);
    const small = recordScenario(otherRuntime, { maxEvents: 1 });
    otherRuntime.publish(host.exports.tick, 1);
    otherRuntime.flush();
    otherRuntime.publish(host.exports.tick, 1);
    otherRuntime.flush();
    assert.equal(small.snapshot().complete, false);
    assert.equal(small.snapshot().waves.length, 1);
    assert.throws(() => replayScenario(composition, small.snapshot()), /Incomplete/);
    small.dispose();
  });
  effect.dispose();
  await test('failed reducer waves start no effects and roll back atom writes', () => {
    const model = defineLibrary({
      name: 'failure',
      requires: [],
      setup(scope) {
        const value = scope.scalar('value', () => 0, { codec: numberCodec });
        const command = scope.command<number>(
          'command',
          (state, amount) => {
            state.set(value, state.read(value) + amount);
            return true;
          },
          { codec: numberCodec },
        );
        const fail = scope.event<number>('fail', { codec: numberCodec });
        const result = scope.event<number>('result', { codec: numberCodec });
        scope.reduce(fail, () => {
          throw new Error('reducer invariant');
        });
        return {
          value,
          command,
          fail,
          effect: defineEffect({
            command,
            result,
            plan: (state, amount) => ({ requestId: amount, value: state.read(value) }),
            decode: (_plan, outcome: number) => outcome,
          }),
        };
      },
    });
    const mount = mountLibrary(model, 'failure');
    const bus = composeLibraries(mount).createRuntime({ scheduler: new ManualScheduler() });
    let executed = 0;
    bindEffect(bus, mount.exports.effect, {
      execute: async () => {
        executed++;
        return 1;
      },
      failure: () => -1,
    });
    bus.publish(mount.exports.command, 2);
    bus.publish(mount.exports.fail, 0);
    assert.throws(() => bus.flush(), /reducer invariant/);
    assert.equal(executed, 0);
    assert.equal(bus.read(mount.exports.value), 0);
    bus.dispose();
  });
  await test('explicit cancellation and disposal suppress late outcomes and close iterators without claiming server rollback', async () => {
    const mount = mountLibrary(hostLibrary, 'stream');
    const bus = composeLibraries(mount).createRuntime({ scheduler: new ManualScheduler() });
    const command = mount.exports.tick;
    const result = mount.exports.tick;
    const next = Promise.withResolvers<IteratorResult<number>>();
    let returned = 0;
    let observed = 0;
    bus.listen(result, () => {
      observed++;
    });
    const boundary = bindEffect(
      bus,
      defineEffect({
        command,
        result,
        plan: (_state, amount) => (amount === 1 ? { requestId: amount } : undefined),
        decode: (_plan, outcome: number) => outcome,
      }),
      {
        stream: () => ({
          [Symbol.asyncIterator]: () => ({
            next: () => next.promise,
            return: async () => {
              returned++;
              return { done: true, value: undefined };
            },
          }),
        }),
        failure: () => -1,
      },
    );
    bus.publish(command, 1);
    bus.flush();
    assert.equal(observed, 1);
    assert.equal(boundary.cancel(1), true);
    assert.equal(returned, 1);
    next.resolve({ done: false, value: 20 });
    await settle(bus);
    assert.equal(observed, 1);
    assert.equal(returned, 1);
    bus.dispose();
  });
  await test('effect registrations and execution remain isolated across both mounts and both runtimes', async () => {
    function seed(bus: ComposedRuntime, model: Inventory, value: number) {
      const request: LoadRequest = {
        interest: model.inventory.at(firstId).interest,
        requestId: loadRequestId('seed'),
        fingerprint: loadFingerprint('seed'),
        at: 1,
        reason: 'interest',
        policy: 'latest-wins',
      };
      bus.publish(model.load, { type: 'loadRequested', request });
      bus.publish(model.load, { type: 'loadSucceeded', request, value, at: 1 });
      bus.flush();
    }
    seed(runtime, r, 100);
    seed(otherRuntime, l, 200);
    let rightOperations = 0;
    let otherOperations = 0;
    const stopRight = bindEffect(runtime, r.effect, {
      execute: async (plan) => {
        rightOperations++;
        return new Ok(plan.next);
      },
      failure: () => new Err('failure'),
    });
    const stopOther = bindEffect(otherRuntime, l.effect, {
      execute: async (plan) => {
        otherOperations++;
        return new Ok(plan.next);
      },
      failure: () => new Err('failure'),
    });
    runtime.publish(r.adjust, { shelfId: firstId, requestId: requestId('request:shared-id'), add: 1 });
    otherRuntime.publish(l.adjust, { shelfId: firstId, requestId: requestId('request:shared-id'), add: 2 });
    await settle(runtime);
    await settle(otherRuntime);
    assert.equal(rightOperations, 1);
    assert.equal(otherOperations, 1);
    assert.equal(previousData(runtime.readKeyed(r.inventory, firstId))?.value, 101);
    assert.equal(previousData(otherRuntime.readKeyed(l.inventory, firstId))?.value, 202);
    assert.equal(previousData(runtime.readKeyed(l.inventory, firstId))?.value, 10);
    assert.equal(otherRuntime.readKeyed(r.inventory, firstId).kind, 'not-requested');
    stopRight.dispose();
    stopOther.dispose();
  });
  await test('React computed hooks keep stable snapshots and release resource IDs on changes', async () => {
    let builds = 0;
    let renders = 0;
    const useSummary = createSelectionHook(
      'summary',
      (state, props: { id: ShelfId }) => {
        builds++;
        return { value: previousData(state.readKeyed(l.inventory, props.id))?.value };
      },
      (props) => [l.inventory.at(props.id)],
    );
    function Summary({ id }: { id: ShelfId }) {
      renders++;
      return createElement('span', null, useSummary({ id }).value);
    }
    const node = document.createElement('div');
    const summaryRoot = createRoot(node);
    const renderSummary = (id: ShelfId) =>
      act(() => summaryRoot.render(createElement(App.Provider, { runtime }, createElement(Summary, { id }))));
    await renderSummary(firstId);
    runtime.flush();
    const before = { builds, renders };
    await act(() => {
      runtime.publish(host.exports.tick, 1);
      runtime.flush();
    });
    assert.deepEqual({ builds, renders }, before);
    await renderSummary(secondId);
    runtime.flush();
    assert.equal(node.textContent, '25');
    assert.equal(
      runtime.interestSource.snapshot().find((entry) => entry.interest.id === firstId),
      undefined,
    );
    await act(() => summaryRoot.unmount());
    runtime.flush();
    assert.equal(runtime.interestSource.snapshot().length, 0);
  });
  await test('library computed hooks switch mounts in one hosted runtime and also work standalone', async () => {
    let builds = 0;
    const useModel = Library.createSelectionHook(
      'library-inventory',
      (state, model, props: { id: ShelfId }) => {
        builds++;
        return { value: previousData(state.readKeyed(model.inventory, props.id))?.value ?? 0 };
      },
      (model, props) => [model.inventory.at(props.id)],
    );
    function Summary() {
      return createElement('span', null, useModel({ id: firstId }).value);
    }
    const node = document.createElement('div');
    const view = createRoot(node);
    const renderMount = (mount: typeof left) =>
      act(() =>
        view.render(
          createElement(App.Provider, { runtime }, createElement(Library.Provider, { mount }, createElement(Summary))),
        ),
      );
    await renderMount(left);
    runtime.flush();
    assert.equal(node.textContent, '10');
    const before = builds;
    await act(() => {
      runtime.publish(host.exports.tick, 1);
      runtime.flush();
    });
    assert.equal(builds, before);
    await renderMount(right);
    runtime.flush();
    assert.equal(node.textContent, '101');
    assert.equal(
      runtime.interestSource.snapshot().some((entry) => entry.interest.key === l.inventory.metadata.key),
      false,
    );
    const standalone = composeLibraries(left);
    const standaloneRuntime = standalone.createRuntime({ scheduler: new ManualScheduler() });
    const Standalone = createStateBusReact(standalone);
    await act(() =>
      view.render(
        createElement(
          Standalone.Provider,
          { runtime: standaloneRuntime },
          createElement(Library.Provider, { mount: left }, createElement(Summary)),
        ),
      ),
    );
    runtime.flush();
    standaloneRuntime.flush();
    assert.equal(node.textContent, '0');
    assert.equal(runtime.interestSource.snapshot().length, 0);
    await act(() => view.unmount());
    standaloneRuntime.flush();
    assert.equal(standaloneRuntime.interestSource.snapshot().length, 0);
    standaloneRuntime.dispose();
  });
  await test('rejected async operations produce typed failure outcomes without an unhandled rejection', async () => {
    const mount = mountLibrary(hostLibrary, 'failure');
    const errors: unknown[] = [];
    const bus = composeLibraries(mount).createRuntime({
      scheduler: new ManualScheduler(),
      onError: (cause) => errors.push(cause),
    });
    const events: number[] = [];
    bus.listen(mount.exports.tick, (value) => events.push(value));
    bindEffect(
      bus,
      defineEffect({
        command: mount.exports.tick,
        result: mount.exports.tick,
        plan: (_state, value) => (value === 1 ? { requestId: value } : undefined),
        decode: (_plan, result: Result<number, string>) => (result.success ? result.value : -1),
      }),
      {
        execute: async () => {
          throw new Error('offline');
        },
        failure: () => new Err('offline'),
      },
    );
    bus.publish(mount.exports.tick, 1);
    await settle(bus);
    assert.deepEqual(events, [1, -1]);
    assert.deepEqual(errors, []);
    bus.dispose();
  });
  await test('disposal suppresses late Promise publications but cannot undo a committed server mutation', async () => {
    const mount = mountLibrary(hostLibrary, 'disposal');
    const bus = composeLibraries(mount).createRuntime({ scheduler: new ManualScheduler() });
    const pending = Promise.withResolvers<number>();
    let serverCommits = 0;
    let aborted = 0;
    const events: number[] = [];
    bus.listen(mount.exports.tick, (value) => events.push(value));
    bindEffect(
      bus,
      defineEffect({
        command: mount.exports.tick,
        result: mount.exports.tick,
        plan: (_state, value) => ({ requestId: value }),
        decode: (_plan, value: number) => value,
      }),
      {
        execute: async (_plan, { signal }) => {
          signal.addEventListener('abort', () => {
            aborted++;
          });
          const value = await pending.promise;
          serverCommits++;
          return value;
        },
        failure: () => -1,
      },
    );
    const demands: number[] = [];
    bus.interestSource.subscribe((changes) => {
      for (const change of changes) demands.push(change.subscribers);
    });
    bus.acquire([mount.exports.ticks]);
    bus.publish(mount.exports.tick, 1);
    bus.flush();
    bus.dispose();
    pending.resolve(7);
    await Promise.resolve();
    await Promise.resolve();
    assert.deepEqual(events, [1]);
    assert.equal(serverCommits, 1);
    assert.equal(aborted, 1);
    assert.deepEqual(demands, [1, 0]);
  });
  await test('manual and production microtask schedulers use identical complete-wave ordering', async () => {
    async function run(manual: boolean) {
      const mount = mountLibrary(hostLibrary, 'waves');
      const bus = composeLibraries(mount).createRuntime(manual ? { scheduler: new ManualScheduler() } : {});
      const values: number[] = [];
      const record = recordScenario(bus);
      bus.listen(mount.exports.tick, (amount) => {
        values.push(bus.read(mount.exports.ticks));
        if (amount === 1) bus.publish(mount.exports.tick, 2);
      });
      bus.publish(mount.exports.tick, 1);
      bus.publish(mount.exports.tick, 3);
      if (manual) bus.flush();
      else {
        await Promise.resolve();
        await Promise.resolve();
      }
      const waves = record.snapshot().waves.map((wave) => wave.events.length);
      assert.equal(bus.idle, true);
      bus.dispose();
      return { values, waves };
    }
    const manual = await run(true);
    assert.deepEqual(manual, { values: [4, 4, 6], waves: [2, 1] });
    assert.deepEqual(await run(false), manual);
  });
  const require = createRequire(import.meta.url);
  for (const name of ['statebus-core', 'statebus-react', 'statebus-data-loader', 'statebus-tanstack-query', 'lmao'])
    assert.match(require.resolve(`@smoothbricks/${name}`), /node_modules.*\/dist\//);
  console.log(JSON.stringify({ passed: names.length, tests: names, builtExports: true }));
} finally {
  runtime.dispose();
  otherRuntime.dispose();
  recorder.dispose();
  queryClient.clear();
  await browser.happyDOM.close();
  for (const [key, descriptor] of saved) {
    if (descriptor) Object.defineProperty(globalThis, key, descriptor);
    else Reflect.deleteProperty(globalThis, key);
  }
}
