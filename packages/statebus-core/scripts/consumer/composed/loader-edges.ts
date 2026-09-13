import assert from 'node:assert/strict';
import {
  composeLibraries,
  defineLibrary,
  ManualScheduler,
  mountLibrary,
  type StateInterestChange,
} from '@smoothbricks/statebus-core';
import {
  composedLoaderChannel,
  type InterestSource,
  initialLoadState,
  type LoaderChannel,
  type LoaderEvent,
  type LoadState,
  loadRequestId,
  reduceComposedLoader,
} from '@smoothbricks/statebus-data-loader';
import {
  bindComposedQueryLoader,
  installTanStackQueryLoader,
  type QueryExecutionContext,
} from '@smoothbricks/statebus-tanstack-query';
import { QueryClient } from '@tanstack/query-core';
import { type ShelfId, shelfCodec, shelfId } from './codecs.js';

const definition = defineLibrary({
  name: 'loader-lifecycle',
  requires: [],
  setup(scope) {
    const resource = scope.keyed('resource', (_id: ShelfId) => initialLoadState<number, string>(), {
      idCodec: shelfCodec,
    });
    const loaded = scope.event<LoaderEvent<number, string>>('loaded');
    reduceComposedLoader(scope, resource, loaded);
    return { resource, loaded };
  },
});
const mount = mountLibrary(definition, 'loader');
const composition = composeLibraries(mount);
const model = mount.exports;
const id = shelfId('shelf:loader');
function client(): QueryClient {
  return new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: Number.POSITIVE_INFINITY } } });
}
function fixture(
  execute: (context: QueryExecutionContext) => Promise<number>,
  failure = (_cause: unknown) => 'network',
) {
  const errors: unknown[] = [];
  const events: LoaderEvent<number, string>[] = [];
  const runtime = composition.createRuntime({
    scheduler: new ManualScheduler(),
    onError: (cause) => errors.push(cause),
  });
  const queryClient = client();
  runtime.listen(model.loaded, (event) => events.push(event));
  let requests = 0;
  const stop = bindComposedQueryLoader(composedLoaderChannel(runtime, model.resource, model.loaded), {
    queryClient,
    query: () => ({ queryKey: ['loader-lifecycle'], execute }),
    failure,
    now: () => 0,
    requestId: () => loadRequestId(`load-${++requests}`),
  });
  const release = runtime.acquire([model.resource.at(id)]);
  runtime.flush();
  return {
    runtime,
    queryClient,
    errors,
    events,
    release,
    stop,
    dispose() {
      stop();
      release();
      runtime.dispose();
      queryClient.clear();
    },
  };
}
async function microtasks(): Promise<void> {
  for (let index = 0; index < 12; index++) await Promise.resolve();
}
let passed = 0;
async function test(name: string, run: () => void | Promise<void>): Promise<void> {
  await run();
  passed++;
  console.log(`PASS loader boundary: ${name}`);
}

await test('failed setup releases every installed subscription and permits a clean retry', () => {
  for (const phase of ['interest-subscription', 'snapshot', 'demand']) {
    const queryClient = client();
    const error = new Error(`setup-${phase}`);
    let requestSubscriptions = 0;
    let interestSubscriptions = 0;
    let failing = true;
    const changes: readonly StateInterestChange[] = [{ interest: model.resource.at(id).interest, subscribers: 1 }];
    const channel: LoaderChannel<number, string> = {
      read: () => initialLoadState(),
      publish() {},
      subscribe() {
        requestSubscriptions++;
        return () => {
          requestSubscriptions--;
        };
      },
    };
    const interests: InterestSource = {
      snapshot() {
        if (failing && phase === 'snapshot') throw error;
        return changes;
      },
      subscribe() {
        if (failing && phase === 'interest-subscription') throw error;
        interestSubscriptions++;
        return () => {
          interestSubscriptions--;
        };
      },
    };
    const install = () =>
      installTanStackQueryLoader({
        channel,
        interests,
        queryClient,
        matches: () => true,
        query: () => ({ queryKey: ['unused'], execute: async () => 1 }),
        failure: () => 'network',
        now: () => 0,
        requestId: () => loadRequestId('setup'),
        demand() {
          if (failing && phase === 'demand') throw error;
          return false;
        },
      });
    try {
      assert.throws(install, (cause) => cause === error);
      assert.equal(requestSubscriptions, 0, phase);
      assert.equal(interestSubscriptions, 0, phase);
      failing = false;
      const dispose = install();
      assert.equal(requestSubscriptions, 1);
      assert.equal(interestSubscriptions, 1);
      dispose();
      dispose();
      assert.equal(requestSubscriptions, 0);
      assert.equal(interestSubscriptions, 0);
    } finally {
      queryClient.clear();
    }
  }
});

await test('runtime drain waits for QueryClient reads and their successor result waves', async () => {
  const pending = Promise.withResolvers<number>();
  let calls = 0;
  const f = fixture(async () => {
    calls++;
    return pending.promise;
  });
  let drained = false;
  const drain = f.runtime.drain().then(() => {
    drained = true;
  });
  try {
    await microtasks();
    assert.equal(calls, 1);
    assert.equal(drained, false);
    assert.equal(f.runtime.readKeyed(model.resource, id).kind, 'loading');
    pending.resolve(42);
    await drain;
    const state = f.runtime.readKeyed(model.resource, id);
    assert.equal(state.kind, 'ready');
    if (state.kind === 'ready') assert.equal(state.data.value, 42);
    assert.deepEqual(f.errors, []);
  } finally {
    pending.resolve(42);
    await drain;
    f.dispose();
  }
});

await test('async disposal waits for non-cooperative transport completion and suppresses late publications', async () => {
  const pending = Promise.withResolvers<number>();
  let aborts = 0;
  let finished = false;
  const f = fixture(async ({ signal }) => {
    signal.addEventListener(
      'abort',
      () => {
        aborts++;
      },
      { once: true },
    );
    const value = await pending.promise;
    finished = true;
    return value;
  });
  const count = f.events.length;
  let disposed = false;
  const disposal = f.runtime.disposeAsync().then(() => {
    disposed = true;
  });
  try {
    await microtasks();
    assert.equal(aborts, 1);
    assert.equal(disposed, false);
    assert.equal(finished, false);
    pending.resolve(21);
    await disposal;
    assert.equal(disposed, true);
    assert.equal(finished, true);
    assert.equal(f.events.length, count);
    assert.deepEqual(f.errors, []);
    assert.equal(f.queryClient.getQueryCache().getAll()[0]?.getObserversCount(), 0);
  } finally {
    pending.resolve(21);
    await disposal;
    f.dispose();
  }
});

await test('failure-classifier bugs reach the runtime error channel without unhandled rejections', async () => {
  const bug = new Error('classifier invariant');
  const f = fixture(
    async () => {
      throw new Error('network failure');
    },
    () => {
      throw bug;
    },
  );
  try {
    await f.runtime.drain();
    assert.deepEqual(f.errors, [bug]);
    assert.equal(
      f.events.some((event) => event.type === 'loadFailed'),
      false,
    );
    assert.equal(f.queryClient.getQueryCache().getAll()[0]?.getObserversCount(), 0);
  } finally {
    f.dispose();
  }
});

await test('expected query failures remain typed outcomes, not runtime diagnostics', async () => {
  const f = fixture(async () => {
    throw new Error('network failure');
  });
  try {
    await f.runtime.drain();
    const state: LoadState<number, string> = f.runtime.readKeyed(model.resource, id);
    assert.equal(state.kind, 'failed');
    if (state.kind === 'failed') assert.equal(state.error, 'network');
    assert.deepEqual(f.errors, []);
    assert.equal(f.events.filter((event) => event.type === 'loadFailed').length, 1);
  } finally {
    f.dispose();
  }
});

await test('cleanup attempts all registered releases even when one provider cleanup throws', () => {
  const queryClient = client();
  const errors: unknown[] = [];
  const failure = new Error('interest cleanup');
  let requests = 0;
  let interests = 0;
  const dispose = installTanStackQueryLoader<number, string>({
    queryClient,
    onError: (cause) => errors.push(cause),
    channel: {
      read: () => initialLoadState(),
      publish() {},
      subscribe() {
        requests++;
        return () => {
          requests--;
        };
      },
    },
    interests: {
      snapshot: () => [],
      subscribe() {
        interests++;
        return () => {
          interests--;
          throw failure;
        };
      },
    },
    matches: () => true,
    query: () => ({ queryKey: ['unused'], execute: async () => 1 }),
    failure: () => 'network',
    now: () => 0,
    requestId: () => loadRequestId('unused'),
  });
  try {
    dispose();
    dispose();
    assert.equal(requests, 0);
    assert.equal(interests, 0);
    assert.deepEqual(errors, [failure]);
  } finally {
    queryClient.clear();
  }
});

console.log(JSON.stringify({ loaderBoundaryScenarios: passed, builtExports: true }));
