import assert from 'node:assert/strict';
import {
  bindEffect,
  type ComposedRuntime,
  captureCheckpoint,
  composeLibraries,
  defineEffect,
  defineLibrary,
  EffectCapacityError,
  type EffectPolicy,
  ManualScheduler,
  mountLibrary,
  RuntimeCapacityError,
  recordScenario,
  replayScenario,
  type WorkAdmission,
} from '@smoothbricks/statebus-core';
import {
  composedLoaderChannel,
  initialLoadState,
  type LoaderEvent,
  loadRequestId,
  reduceComposedLoader,
} from '@smoothbricks/statebus-data-loader';
import { connectBrowserNavigation } from '@smoothbricks/statebus-navigation-browser';
import {
  connectNavigation,
  initialNavigationState,
  NAVIGATION_DISPATCHED,
  type NavigationChannel,
  type NavigationEvent,
  type NavigationOutcome,
  navigationRequestId,
  reduceNavigation,
} from '@smoothbricks/statebus-navigation-core';
import { bindComposedQueryLoader, type QueryExecutionContext } from '@smoothbricks/statebus-tanstack-query';
import { onlineManager, QueryClient } from '@tanstack/query-core';
import fc from 'fast-check';
import { Window } from 'happy-dom';
import { loadEventCodec, loadStateCodec, numberCodec, parseScenario } from './codecs.js';

function library(policy: EffectPolicy) {
  return defineLibrary({
    name: `aggregate-${policy}`,
    requires: [],
    setup(scope) {
      const total = scope.scalar('total', () => 0, { codec: numberCodec });
      const command = scope.command<number>('command', () => true, { codec: numberCodec });
      const result = scope.event<number>('result', { codec: numberCodec });
      const fail = scope.event<number>('fail', { codec: numberCodec });
      scope.reduce(result, (state, value) => state.set(total, state.read(total) + value));
      scope.reduce(fail, () => {
        throw new Error('failed aggregate wave');
      });
      const effect = defineEffect({
        command,
        result,
        policy,
        plan: (_state, requestId) => ({ requestId, operationKey: 'shared-local-key' }),
        decode: (_plan, value: number) => value,
      });
      const resource = scope.keyed('resource', (_id: number) => initialLoadState<number, string>(), {
        idCodec: numberCodec,
        codec: loadStateCodec,
      });
      const loaded = scope.event<LoaderEvent<number, string>>('loaded', { codec: loadEventCodec });
      reduceComposedLoader(scope, resource, loaded);
      return { total, command, result, fail, effect, resource, loaded };
    },
  });
}
const parallel = library('parallel');
const leftMount = mountLibrary(parallel, 'first');
const rightMount = mountLibrary(parallel, 'second');
const serialMount = mountLibrary(library('serialize'), 'serial');
const latestMount = mountLibrary(library('latest-wins'), 'latest');
const app = composeLibraries(leftMount, rightMount, serialMount, latestMount);
const first = leftMount.exports;
const second = rightMount.exports;
const serial = serialMount.exports;
const latest = latestMount.exports;
function create(maxPendingWork: number, onError?: (cause: unknown) => void): ComposedRuntime {
  return app.createRuntime({ maxPendingWork, scheduler: new ManualScheduler(), onError });
}
async function microtasks(): Promise<void> {
  for (let index = 0; index < 16; index++) await Promise.resolve();
}
function client(): QueryClient {
  return new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: Number.POSITIVE_INFINITY } } });
}
function readBinding(
  runtime: ComposedRuntime,
  model: typeof first,
  queryClient: QueryClient,
  execute: (context: QueryExecutionContext) => Promise<number>,
  key = model.resource.metadata.key,
  onQuery: () => void = () => {},
  failure: (cause: unknown) => string = (cause) => (cause instanceof RuntimeCapacityError ? 'capacity' : 'network'),
  retry = false,
): () => void {
  let id = 0;
  return bindComposedQueryLoader(composedLoaderChannel(runtime, model.resource, model.loaded), {
    queryClient,
    query(request) {
      onQuery();
      return {
        queryKey: [key, request.interest.id],
        execute,
        retry,
        retryDelay: 0,
      };
    },
    failure,
    graceMs: 100_000,
    requestId: () => loadRequestId(`work-${++id}`),
    now: () => 0,
  });
}
function observe(runtime: ComposedRuntime, model: typeof first, id: number): () => void {
  const stop = runtime.acquire([model.resource.at(id)]);
  runtime.flush();
  return stop;
}
function capacity(cause: unknown): number {
  assert.ok(cause instanceof RuntimeCapacityError);
  assert.equal(cause.code, 'runtime-capacity');
  return -1;
}
let passed = 0;
async function test(name: string, run: () => void | Promise<void>): Promise<void> {
  await run();
  passed++;
  console.log(`PASS aggregate admission: ${name}`);
}

await test('one limit spans different definitions and mounts, but never a second runtime', async () => {
  const runtime = create(3);
  const other = create(1);
  const pending = Promise.withResolvers<number>();
  let calls = 0;
  const bind = (bus: ComposedRuntime, model: typeof first) =>
    bindEffect(bus, model.effect, {
      execute: () => {
        calls++;
        return pending.promise;
      },
      failure: capacity,
    });
  for (const model of [first, second, serial]) bind(runtime, model);
  bind(other, first);
  try {
    runtime.publish(first.command, 1);
    runtime.publish(second.command, 1);
    runtime.publish(serial.command, 1);
    runtime.publish(first.command, 2);
    runtime.publish(first.command, 2);
    runtime.flush();
    assert.equal(runtime.work.pending, 3);
    assert.equal(runtime.work.limit, 3);
    assert.equal(runtime.read(first.total), -1, 'A same-wave duplicate refusal is decoded only once.');
    other.publish(first.command, 1);
    other.flush();
    assert.equal(other.work.pending, 1);
    assert.equal(calls, 4);
    pending.resolve(5);
    await runtime.drain();
    await other.drain();
    assert.equal(runtime.work.pending, 0);
    assert.equal(other.work.pending, 0);
    assert.equal(runtime.read(second.total), 5);
  } finally {
    runtime.dispose();
    other.dispose();
  }
});

await test('queued jobs count globally and cancelled running jobs keep their slots', async () => {
  const runtime = create(3);
  const pending = Promise.withResolvers<number>();
  const calls: number[] = [];
  const queue = bindEffect(runtime, serial.effect, {
    execute: ({ requestId }) => {
      calls.push(requestId);
      return pending.promise;
    },
    failure: capacity,
  });
  bindEffect(runtime, first.effect, { execute: () => pending.promise, failure: capacity });
  try {
    for (const id of [1, 2, 3]) runtime.publish(serial.command, id);
    runtime.flush();
    assert.deepEqual(calls, [1]);
    assert.equal(runtime.work.pending, 3);
    assert.equal(queue.cancel(2), true);
    assert.equal(runtime.work.pending, 2);
    runtime.publish(first.command, 1);
    runtime.flush();
    assert.equal(runtime.work.pending, 3);
    assert.equal(queue.cancel(1), true);
    assert.equal(runtime.work.pending, 3, 'Abort is not physical completion.');
    runtime.publish(first.command, 2);
    runtime.flush();
    assert.equal(runtime.read(first.total), -1);
    assert.equal(queue.cancel(3), true);
    assert.equal(runtime.work.pending, 2);
    pending.resolve(10);
    await runtime.drain();
    assert.equal(runtime.work.pending, 0);
    assert.deepEqual(calls, [1]);
  } finally {
    runtime.dispose();
  }
});

await test('binding disposal and repeated rebinding cannot reset aggregate admission', async () => {
  const runtime = create(1);
  const pending = Promise.withResolvers<number>();
  let calls = 0;
  const install = () =>
    bindEffect(runtime, first.effect, {
      execute: () => {
        calls++;
        return pending.promise;
      },
      failure: capacity,
    });
  let binding = install();
  runtime.publish(first.command, 1);
  runtime.flush();
  try {
    for (let id = 2; id < 202; id++) {
      binding.dispose();
      binding = install();
      runtime.publish(first.command, id);
      runtime.flush();
      assert.equal(runtime.work.pending, 1);
      assert.equal(calls, 1);
    }
    pending.resolve(9);
    await runtime.drain();
    assert.equal(runtime.work.pending, 0);
    runtime.publish(first.command, 300);
    await runtime.drain();
    assert.equal(calls, 2);
    assert.equal(runtime.work.pending, 0);
  } finally {
    runtime.dispose();
  }
});

await test('admission is visible to reentrant drain before a completion Promise exists', async () => {
  const runtime = create(1);
  const pending = Promise.withResolvers<number>();
  let drained = false;
  let waiting: Promise<void> | undefined;
  bindEffect(runtime, first.effect, {
    captureInstruction() {
      assert.equal(runtime.work.pending, 1);
      waiting = runtime.drain().then(() => {
        drained = true;
      });
    },
    execute: () => pending.promise,
    failure: capacity,
  });
  runtime.publish(first.command, 1);
  runtime.flush();
  await microtasks();
  assert.equal(drained, false);
  pending.resolve(3);
  await waiting;
  assert.equal(drained, true);
  assert.equal(runtime.work.pending, 0);
  assert.equal(runtime.read(first.total), 3);
  runtime.dispose();
});

await test('reentrant cancellation in capture releases exactly one unstarted reservation', async () => {
  const runtime = create(1);
  let calls = 0;
  const binding = bindEffect(runtime, first.effect, {
    captureInstruction(plan) {
      assert.equal(binding.cancel(plan.requestId), true);
    },
    execute: () => {
      throw new Error('Cancelled before execution.');
    },
    failure: capacity,
  });
  bindEffect(runtime, second.effect, {
    execute: () => {
      calls++;
      return 7;
    },
    failure: capacity,
  });
  try {
    runtime.publish(first.command, 1);
    runtime.publish(second.command, 1);
    await runtime.drain();
    assert.equal(calls, 1);
    assert.equal(runtime.work.pending, 0);
    assert.equal(runtime.read(second.total), 7);
  } finally {
    runtime.dispose();
  }
});

await test('failed reductions, thrown operations and failing decoders cannot leak slots', async () => {
  const errors: unknown[] = [];
  const runtime = create(1, (cause) => errors.push(cause));
  let calls = 0;
  const binding = bindEffect(runtime, first.effect, {
    execute() {
      calls++;
      throw new Error('transport bug');
    },
    failure() {
      throw new Error('classifier bug');
    },
  });
  try {
    runtime.publish(first.command, 1);
    runtime.publish(first.fail, 0);
    assert.throws(() => runtime.flush(), /failed aggregate wave/);
    assert.equal(runtime.work.pending, 0);
    assert.equal(calls, 0);
    runtime.publish(first.command, 2);
    runtime.flush();
    assert.equal(runtime.work.pending, 1);
    await runtime.drain();
    assert.equal(runtime.work.pending, 0);
    assert.equal(errors.length, 1);
    binding.dispose();
    const broken = defineEffect({
      command: first.command,
      result: first.result,
      plan: (_state, requestId) => ({ requestId }),
      decode: (_plan, _outcome: number): number => {
        throw new Error('decoder bug');
      },
    });
    bindEffect(runtime, broken, { execute: () => 1, failure: capacity });
    runtime.publish(first.command, 3);
    await runtime.drain();
    assert.equal(runtime.work.pending, 0);
    assert.equal(errors.length, 2);
  } finally {
    runtime.dispose();
  }
});

await test('runtime disposal closes admission before abort callbacks and awaits real cleanup', async () => {
  const runtime = create(2);
  const next = Promise.withResolvers<IteratorResult<number>>();
  const returned = Promise.withResolvers<IteratorResult<number>>();
  let closes = 0;
  let admissionAtAbort: boolean | undefined;
  bindEffect(runtime, first.effect, {
    stream: (_plan, { signal }) => {
      signal.addEventListener('abort', () => {
        admissionAtAbort = runtime.work.tryAcquire();
      });
      return {
        [Symbol.asyncIterator]: () => ({
          next: () => next.promise,
          return: () => {
            closes++;
            return returned.promise;
          },
        }),
      };
    },
    failure: capacity,
  });
  runtime.publish(first.command, 1);
  runtime.flush();
  let drained = false;
  const waiting = runtime.disposeAsync().then(() => {
    drained = true;
  });
  assert.equal(admissionAtAbort, false);
  assert.equal(runtime.work.pending, 1);
  assert.equal(closes, 1);
  next.resolve({ done: false, value: 1 });
  await microtasks();
  assert.equal(runtime.work.pending, 1);
  assert.equal(drained, false);
  returned.resolve({ done: true, value: undefined });
  await waiting;
  assert.equal(runtime.work.pending, 0);
  assert.equal(closes, 1);
  assert.equal(runtime.work.tryAcquire(), false);
});

await test('supersession reserves globally before abort callbacks and rejects without aborting at capacity', async () => {
  const runtime = create(2);
  const pending = Promise.withResolvers<number>();
  let aborts = 0;
  let calls = 0;
  let intruderCalls = 0;
  bindEffect(runtime, first.effect, {
    execute: () => {
      intruderCalls++;
      return 5;
    },
    failure: capacity,
  });
  bindEffect(runtime, latest.effect, {
    execute: (_plan, { signal }) => {
      calls++;
      signal.addEventListener(
        'abort',
        () => {
          aborts++;
          assert.equal(runtime.work.pending, 2);
          runtime.publish(first.command, 100);
          runtime.flush();
        },
        { once: true },
      );
      return pending.promise;
    },
    failure: capacity,
  });
  try {
    runtime.publish(latest.command, 1);
    runtime.flush();
    runtime.publish(latest.command, 2);
    runtime.flush();
    assert.equal(calls, 2);
    assert.equal(aborts, 1);
    assert.equal(intruderCalls, 0);
    assert.equal(runtime.work.pending, 2);
    runtime.publish(latest.command, 3);
    runtime.flush();
    assert.equal(calls, 2);
    assert.equal(aborts, 1, 'Refusal must not cancel an admitted incumbent.');
    pending.resolve(2);
    await runtime.drain();
    assert.equal(runtime.work.pending, 0);
  } finally {
    runtime.dispose();
  }
});

await test('local binding limits still apply within a larger runtime budget', async () => {
  const runtime = create(3);
  const pending = Promise.withResolvers<number>();
  const failures: unknown[] = [];
  bindEffect(runtime, first.effect, {
    maxPending: 1,
    execute: () => pending.promise,
    failure: (cause) => {
      failures.push(cause);
      return -1;
    },
  });
  bindEffect(runtime, second.effect, { execute: () => pending.promise, failure: capacity });
  runtime.publish(first.command, 1);
  runtime.publish(first.command, 2);
  runtime.publish(second.command, 1);
  runtime.flush();
  assert.ok(failures[0] instanceof EffectCapacityError);
  assert.equal(runtime.work.pending, 2);
  pending.resolve(1);
  await runtime.drain();
  assert.equal(runtime.work.pending, 0);
  runtime.dispose();
});

await test('mixed effect and loader bindings share capacity before query/observer setup', async () => {
  const runtime = create(2);
  const queryClient = client();
  const mutation = Promise.withResolvers<number>();
  const read = Promise.withResolvers<number>();
  let queries = 0;
  let transports = 0;
  bindEffect(runtime, first.effect, { execute: () => mutation.promise, failure: capacity });
  readBinding(
    runtime,
    first,
    queryClient,
    () => {
      transports++;
      return read.promise;
    },
    'one',
    () => {
      queries++;
    },
  );
  readBinding(
    runtime,
    second,
    queryClient,
    () => {
      transports++;
      return read.promise;
    },
    'two',
    () => {
      queries++;
    },
  );
  try {
    runtime.publish(first.command, 1);
    runtime.flush();
    observe(runtime, first, 1);
    observe(runtime, second, 1);
    assert.equal(runtime.work.pending, 2);
    assert.equal(queries, 1);
    assert.equal(transports, 1);
    assert.equal(queryClient.getQueryCache().getAll().length, 1);
    const state = runtime.readKeyed(second.resource, 1);
    assert.equal(state.kind, 'failed');
    if (state.kind === 'failed') assert.equal(state.error, 'capacity');
    mutation.resolve(3);
    read.resolve(4);
    await runtime.drain();
    assert.equal(runtime.work.pending, 0);
    assert.equal(runtime.readKeyed(first.resource, 1).kind, 'ready');
  } finally {
    runtime.dispose();
    queryClient.clear();
  }
});

await test('shared query observers count logical reads without duplicating transport work', async () => {
  const runtime = create(2);
  const queryClient = client();
  const pending = Promise.withResolvers<number>();
  let calls = 0;
  const read = () => {
    calls++;
    return pending.promise;
  };
  const stopFirst = readBinding(runtime, first, queryClient, read, 'shared');
  readBinding(runtime, second, queryClient, read, 'shared');
  try {
    observe(runtime, first, 1);
    observe(runtime, second, 1);
    assert.equal(runtime.work.pending, 2);
    assert.equal(calls, 1);
    stopFirst();
    await microtasks();
    assert.equal(runtime.work.pending, 2, 'The shared query and both completion waiters are still running.');
    pending.resolve(11);
    await runtime.drain();
    assert.equal(runtime.work.pending, 0);
    assert.equal(runtime.readKeyed(second.resource, 1).kind, 'ready');
  } finally {
    runtime.dispose();
    queryClient.clear();
  }
});

await test('query retries keep one reservation and the same logical request identity', async () => {
  const runtime = create(1);
  const queryClient = client();
  const ids: string[] = [];
  let calls = 0;
  readBinding(
    runtime,
    first,
    queryClient,
    async (context) => {
      assert.equal(runtime.work.pending, 1);
      ids.push(context.request.requestId);
      if (++calls < 3) throw new Error('temporary network failure');
      return 9;
    },
    'retries',
    undefined,
    undefined,
    true,
  );
  try {
    observe(runtime, first, 1);
    await runtime.drain();
    assert.equal(calls, 3);
    assert.equal(new Set(ids).size, 1);
    assert.equal(runtime.work.pending, 0);
    assert.equal(runtime.readKeyed(first.resource, 1).kind, 'ready');
  } finally {
    runtime.dispose();
    queryClient.clear();
  }
});

await test('cancelled QueryClient work cannot free aggregate capacity before transport settles', async () => {
  const runtime = create(1);
  const queryClient = client();
  const pending = Promise.withResolvers<number>();
  const stop = readBinding(runtime, first, queryClient, () => pending.promise);
  let executions = 0;
  bindEffect(runtime, second.effect, {
    execute: () => {
      executions++;
      return 2;
    },
    failure: capacity,
  });
  try {
    observe(runtime, first, 1);
    stop();
    await microtasks();
    assert.equal(runtime.work.pending, 1);
    runtime.publish(second.command, 1);
    runtime.flush();
    assert.equal(executions, 0);
    assert.equal(runtime.read(second.total), -1);
    assert.equal(runtime.work.pending, 1);
    let drained = false;
    const waiting = runtime.drain().then(() => {
      drained = true;
    });
    await microtasks();
    assert.equal(drained, false);
    pending.resolve(8);
    await waiting;
    assert.equal(runtime.work.pending, 0);
    assert.notEqual(runtime.readKeyed(first.resource, 1).kind, 'ready', 'A cancelled read cannot publish late data.');
    runtime.publish(second.command, 2);
    await runtime.drain();
    assert.equal(executions, 1);
  } finally {
    runtime.dispose();
    queryClient.clear();
  }
});

await test('paused reads reserve capacity without transport and release it on cancellation', async () => {
  const runtime = create(1);
  const queryClient = client();
  let calls = 0;
  const stop = readBinding(runtime, first, queryClient, async () => {
    calls++;
    return 1;
  });
  onlineManager.setOnline(false);
  try {
    observe(runtime, first, 1);
    await microtasks();
    assert.equal(runtime.work.pending, 1);
    assert.equal(calls, 0);
    stop();
    await runtime.drain();
    assert.equal(runtime.work.pending, 0);
    assert.equal(calls, 0);
  } finally {
    onlineManager.setOnline(true);
    runtime.dispose();
    queryClient.clear();
  }
});

await test('stored QueryClient callbacks cannot restart transport without fresh runtime admission', async () => {
  const runtime = create(1);
  const queryClient = client();
  let calls = 0;
  readBinding(
    runtime,
    first,
    queryClient,
    async () => {
      calls++;
      return calls;
    },
    'retained',
  );
  const pending = Promise.withResolvers<number>();
  bindEffect(runtime, second.effect, { execute: () => pending.promise, failure: capacity });
  try {
    observe(runtime, first, 1);
    await runtime.drain();
    assert.equal(calls, 1);
    runtime.publish(second.command, 1);
    runtime.flush();
    // Refetch preserves the query's existing queryFn; fetchQuery with replacement options
    // deliberately replaces it and would test missing query configuration instead.
    await assert.rejects(
      queryClient.refetchQueries({ queryKey: ['retained', 1], exact: true }, { throwOnError: true }),
      RuntimeCapacityError,
    );
    assert.equal(calls, 1);
    assert.equal(runtime.work.pending, 1);
    pending.resolve(1);
    await runtime.drain();
    await queryClient.refetchQueries({ queryKey: ['retained', 1], exact: true }, { throwOnError: true });
    assert.equal(queryClient.getQueryData(['retained', 1]), 2);
    await runtime.drain();
    assert.equal(runtime.work.pending, 0);
  } finally {
    runtime.dispose();
    queryClient.clear();
  }
});

await test('query setup failure and reentrant disposal release unstarted work', async () => {
  for (const action of ['throw', 'dispose']) {
    const runtime = create(1);
    const queryClient = client();
    let calls = 0;
    readBinding(
      runtime,
      first,
      queryClient,
      async () => {
        calls++;
        return 1;
      },
      'setup',
      () => {
        if (action === 'dispose') runtime.dispose();
        else throw new Error('query options failed');
      },
    );
    try {
      observe(runtime, first, 1);
      await runtime.drain();
      assert.equal(runtime.work.pending, 0);
      assert.equal(calls, 0);
      assert.equal(queryClient.getQueryCache().getAll().length, 0);
    } finally {
      runtime.dispose();
      queryClient.clear();
    }
  }
});

await test('capacity outcomes replay as ordinary domain events without admission or I/O', async () => {
  const runtime = create(1);
  const recorder = recordScenario(runtime);
  const pending = Promise.withResolvers<number>();
  bindEffect(runtime, first.effect, { execute: () => pending.promise, failure: capacity });
  bindEffect(runtime, second.effect, { execute: () => pending.promise, failure: capacity });
  runtime.publish(first.command, 1);
  runtime.publish(second.command, 1);
  runtime.flush();
  pending.resolve(5);
  await runtime.drain();
  const replay = replayScenario(app, parseScenario(JSON.stringify(recorder.snapshot())));
  try {
    assert.deepEqual(captureCheckpoint(replay), captureCheckpoint(runtime));
    assert.equal(replay.work.pending, 0);
    assert.equal(replay.work.tryAcquire(), false);
  } finally {
    replay.dispose();
    recorder.dispose();
    runtime.dispose();
  }
});

await test('generated multi-binding cancel/settle streams agree with actual pending work', async () => {
  await fc.assert(
    fc.asyncProperty(
      fc.array(fc.record({ action: fc.integer({ min: 0, max: 3 }), owner: fc.boolean() }), {
        minLength: 150,
        maxLength: 250,
      }),
      async (actions) => {
        const runtime = create(3);
        const held = new Map<number, ReturnType<typeof Promise.withResolvers<number>>>();
        const bindings = [first, second].map((model) =>
          bindEffect(runtime, model.effect, {
            execute: ({ requestId }) => {
              const deferred = Promise.withResolvers<number>();
              held.set(requestId, deferred);
              return deferred.promise;
            },
            failure: capacity,
          }),
        );
        try {
          let id = 0;
          for (const action of actions) {
            const owner = action.owner ? 0 : 1;
            if (action.action < 2) {
              runtime.publish((action.owner ? first : second).command, ++id);
              runtime.flush();
            } else if (action.action === 2) bindings[owner].cancelKey('shared-local-key');
            else {
              const entry = held.entries().next().value;
              if (entry) {
                held.delete(entry[0]);
                entry[1].resolve(1);
              }
            }
            await microtasks();
            runtime.flush();
            assert.equal(runtime.work.pending, held.size);
            assert.ok(runtime.work.pending <= 3);
          }
          for (const operation of held.values()) operation.resolve(1);
          await runtime.drain();
          assert.equal(runtime.work.pending, 0);
        } finally {
          runtime.dispose();
        }
      }),
    ),
    { seed: 148201, numRuns: 50 },
  );
});

function navigationChannel() {
  let state = initialNavigationState<string, { pathname: string; search: string; hash: string }>({
    pathname: '/',
    search: '',
    hash: '',
  });
  const listeners = new Set<(event: NavigationEvent<string, typeof state.location>) => void>();
  const port: NavigationChannel<string, typeof state.location> = {
    read: () => state,
    publish(event) {
      state = reduceNavigation(state, event);
      for (const listener of listeners) listener(event);
    },
    subscribe(listener) {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
  };
  return port;
}

await test('navigation shares the budget and abort/disconnect cannot release unfinished driver work', async () => {
  const runtime = create(1);
  const channel = navigationChannel();
  const pending = Promise.withResolvers<NavigationOutcome>();
  let calls = 0;
  let aborted = 0;
  const stop = connectNavigation({
    channel,
    work: runtime.work,
    trackExecution: (task) => runtime.trackExecution(task),
    driver: {
      current: () => ({ pathname: '/', search: '', hash: '' }),
      subscribe: () => () => {},
      execute: (_request, { signal }) => {
        calls++;
        signal.addEventListener('abort', () => {
          aborted++;
        });
        return pending.promise;
      },
    },
  });
  bindEffect(runtime, first.effect, { execute: () => 1, failure: capacity });
  channel.publish({
    type: 'navigationRequested',
    request: {
      requestId: navigationRequestId('a'),
      intent: { kind: 'push', to: '/a' },
    },
  });
  assert.equal(runtime.work.pending, 1);
  stop();
  stop();
  assert.equal(aborted, 1);
  runtime.publish(first.command, 1);
  runtime.flush();
  assert.equal(runtime.read(first.total), -1);
  assert.equal(runtime.work.pending, 1);
  let done = false;
  const drain = runtime.drain().then(() => {
    done = true;
  });
  await microtasks();
  assert.equal(done, false);
  pending.resolve(NAVIGATION_DISPATCHED);
  await drain;
  assert.equal(runtime.work.pending, 0);
  assert.equal(calls, 1);
  assert.equal(channel.read().operation.kind, 'requested', 'Disconnected late acknowledgements are suppressed.');
  runtime.dispose();
});

await test('browser history refuses before write and synchronous navigation releases immediately', async () => {
  const runtime = create(1);
  const browser = new Window({ url: 'https://example.test/' });
  const channel = navigationChannel();
  // A real DOM Window is structurally supplied by happy-dom's browser test boundary.
  const original = Object.getOwnPropertyDescriptor(globalThis, 'window');
  Object.defineProperty(globalThis, 'window', { value: browser, configurable: true });
  const stop = connectBrowserNavigation({ window, channel, work: runtime.work });
  try {
    assert.equal(runtime.work.tryAcquire(), true);
    channel.publish({
      type: 'navigationRequested',
      request: {
        requestId: navigationRequestId('full'),
        intent: { kind: 'push', to: '/full' },
      },
    });
    assert.equal(browser.location.pathname, '/');
    const failed = channel.read().operation;
    assert.equal(failed.kind, 'failed');
    if (failed.kind === 'failed') assert.equal(failed.error.code, 'capacity');
    runtime.work.release();
    channel.publish({
      type: 'navigationRequested',
      request: {
        requestId: navigationRequestId('available'),
        intent: { kind: 'push', to: '/available' },
      },
    });
    assert.equal(browser.location.pathname, '/available');
    assert.equal(runtime.work.pending, 0);
    runtime.dispose();
    channel.publish({
      type: 'navigationRequested',
      request: {
        requestId: navigationRequestId('closed'),
        intent: { kind: 'push', to: '/closed' },
      },
    });
    assert.equal(browser.location.pathname, '/available');
  } finally {
    stop();
    runtime.dispose();
    if (original) Object.defineProperty(globalThis, 'window', original);
    else Reflect.deleteProperty(globalThis, 'window');
    await browser.happyDOM.close();
  }
});

await test('configuration is validated before initial state and the adapter port stays typed', () => {
  let initialized = 0;
  const definition = defineLibrary({
    name: 'configuration',
    requires: [],
    setup(scope) {
      scope.scalar('value', () => {
        initialized++;
        return 0;
      });
      return {};
    },
  });
  const composition = composeLibraries(mountLibrary(definition, 'configuration'));
  for (const maxPendingWork of [0, -1, 0.5, Number.POSITIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1])
    assert.throws(() => composition.createRuntime({ maxPendingWork }), /maxPendingWork/);
  assert.equal(initialized, 0);
  const runtime = composition.createRuntime();
  assert.equal(runtime.work.limit, 4096);
  const admission: WorkAdmission = runtime.work;
  assert.equal(admission.tryAcquire(), true);
  admission.release();
  assert.throws(() => admission.release(), /without admission/);
  runtime.dispose();
});

// Type-only contract, never executed. No fallback aliases or casts in the consumer.
function typeErrors(runtime: ComposedRuntime): void {
  // @ts-expect-error Capacity is a number, not a coercible transport string.
  app.createRuntime({ maxPendingWork: '4' });
  // @ts-expect-error The counter is read-only at the public boundary.
  runtime.work.pending = 0;
  // @ts-expect-error A foreign model handle does not gain type-erased publication.
  runtime.publish(first.command, 'untyped');
}
void typeErrors;
console.log(
  JSON.stringify({ aggregateAdmissionScenarios: passed, builtExports: true, measuredAllocationBytes: false }),
);
