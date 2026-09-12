import assert from 'node:assert/strict';
import {
  bindEffect,
  bindRuntimeDiagnostics,
  CaptureError,
  type ComposedRuntime,
  canonicalCapture,
  captureCheckpoint,
  composeLibraries,
  defineCapability,
  defineEffect,
  defineLibrary,
  type EffectPolicy,
  ManualScheduler,
  mountLibrary,
  type RuntimeDiagnostic,
  recordRollingScenario,
  recordScenario,
  replayScenario,
  type ValueCodec,
} from '@smoothbricks/statebus-core';
import fc from 'fast-check';
import typia from 'typia';

function codec<T>(schema: string, decode: (value: unknown) => T): ValueCodec<T> {
  return { schema, version: 1, encode: (value) => structuredClone(value), decode };
}
const numberCodec = codec('number', typia.createAssert<number>());
const valuesCodec = codec('values', typia.createAssert<readonly number[]>());
interface Plan {
  readonly requestId: number;
  readonly operationKey: string | number;
  readonly value: number;
}
const planCodec = codec('plan', typia.createAssert<Plan>());
const outcomeCodec = codec('outcome', typia.createAssert<{ plan: Plan; outcome: number }>());
function model(policy: EffectPolicy = 'parallel') {
  return defineLibrary({
    name: 'execution',
    requires: [],
    setup(scope) {
      const values = scope.scalar('values', (): readonly number[] => [], { codec: valuesCodec });
      const command = scope.command('command', (_state, plan: Plan) => plan.value >= 0, { codec: planCodec });
      const result = scope.event('result', { codec: numberCodec });
      scope.reduce(result, (state, value) => state.set(values, [...state.read(values), value]));
      const effect = scope.requireEffect(
        defineEffect({
          command,
          result,
          policy,
          plan: (_state, command) => command,
          decode: (_plan, outcome: number) => outcome,
          codec: outcomeCodec,
          instructionCodec: planCodec,
          cancelled: (plan) => -plan.requestId,
        }),
      );
      return { values, command, result, effect };
    },
  });
}
async function tick(bus: ComposedRuntime) {
  for (let index = 0; index < 12; index++) {
    await Promise.resolve();
    bus.flush();
  }
}
let passed = 0;
async function test(name: string, run: () => void | Promise<void>) {
  await run();
  passed++;
  console.log(`PASS execution: ${name}`);
}

await test('a declared interpreter fails preflight until bound; disposal invalidates the cached completion check', () => {
  const mount = mountLibrary(model(), 'required');
  const bus = composeLibraries(mount).createRuntime({ scheduler: new ManualScheduler() });
  assert.throws(() => bus.assertReady(), /Missing required effect/);
  const binding = bindEffect(bus, mount.exports.effect, { execute: (plan) => plan.value, failure: () => -1 });
  bus.assertReady();
  bus.assertReady();
  binding.dispose();
  assert.throws(() => bus.assertReady(), /Missing required effect/);
  bus.dispose();
});

await test('serialized operations never execute cancelled queue entries, and drain includes the successor operation', async () => {
  const mount = mountLibrary(model('serialize'), 'serial');
  const bus = composeLibraries(mount).createRuntime({ scheduler: new ManualScheduler() });
  const waits = new Map<number, ReturnType<typeof Promise.withResolvers<number>>>();
  const calls: number[] = [];
  for (const id of [1, 2, 3]) waits.set(id, Promise.withResolvers<number>());
  const binding = bindEffect(bus, mount.exports.effect, {
    execute: (plan) => {
      calls.push(plan.requestId);
      const waiter = waits.get(plan.requestId);
      assert.ok(waiter);
      return waiter.promise;
    },
    failure: () => -99,
  });
  for (const requestId of [1, 2, 3])
    bus.publish(mount.exports.command, { requestId, operationKey: 'document', value: requestId });
  bus.flush();
  assert.deepEqual(calls, [1]);
  assert.equal(binding.cancel(2), true);
  waits.get(1)?.resolve(10);
  await tick(bus);
  assert.deepEqual(calls, [1, 3]);
  waits.get(3)?.resolve(30);
  await binding.drain();
  await bus.drain();
  assert.deepEqual(bus.read(mount.exports.values), [-2, 10, 30]);
  await bus.disposeAsync();
});

await test('latest-wins suppresses superseded outcomes without claiming the server operation was rolled back', async () => {
  const mount = mountLibrary(model('latest-wins'), 'latest');
  const bus = composeLibraries(mount).createRuntime({ scheduler: new ManualScheduler() });
  const first = Promise.withResolvers<number>();
  const second = Promise.withResolvers<number>();
  let aborted = 0;
  let serverCommits = 0;
  const binding = bindEffect(bus, mount.exports.effect, {
    execute: async (plan, { signal }) => {
      signal.addEventListener('abort', () => {
        aborted++;
      });
      const value = await (plan.requestId === 1 ? first.promise : second.promise);
      serverCommits++;
      return value;
    },
    failure: () => -99,
  });
  bus.publish(mount.exports.command, { requestId: 1, operationKey: 'document', value: 1 });
  bus.publish(mount.exports.command, { requestId: 2, operationKey: 'document', value: 2 });
  bus.flush();
  assert.equal(aborted, 1);
  second.resolve(20);
  await tick(bus);
  first.resolve(10);
  await binding.drain();
  await bus.drain();
  assert.deepEqual(bus.read(mount.exports.values), [-1, 20]);
  assert.equal(serverCommits, 2);
  await bus.disposeAsync();
});

await test('drop-duplicate and domain refusal do not execute, while unrelated operation keys remain independent', async () => {
  const mount = mountLibrary(model('drop-duplicate'), 'drop');
  const bus = composeLibraries(mount).createRuntime({ scheduler: new ManualScheduler() });
  const pending = Promise.withResolvers<number>();
  const calls: number[] = [];
  bindEffect(bus, mount.exports.effect, {
    execute: (plan) => {
      calls.push(plan.requestId);
      return pending.promise;
    },
    failure: () => -99,
  });
  for (const plan of [
    { requestId: 1, operationKey: 7, value: 1 },
    { requestId: 2, operationKey: 7, value: 2 },
    { requestId: 3, operationKey: '7', value: 3 },
    { requestId: 4, operationKey: 8, value: -1 },
  ])
    bus.publish(mount.exports.command, plan);
  bus.flush();
  assert.deepEqual(calls, [1, 3]);
  pending.resolve(9);
  await bus.drain();
  assert.deepEqual(bus.read(mount.exports.values), [-2, 9, 9]);
  await bus.disposeAsync();
});

await test('generated same-wave duplicate submissions execute exactly once across direct success and synchronous failure', async () => {
  await fc.assert(
    fc.asyncProperty(fc.array(fc.integer({ min: 0, max: 40 }), { minLength: 50, maxLength: 150 }), async (ids) => {
      const mount = mountLibrary(model(), 'duplicates');
      const bus = composeLibraries(mount).createRuntime({ scheduler: new ManualScheduler() });
      const calls: number[] = [];
      bindEffect(bus, mount.exports.effect, {
        execute: (plan) => {
          calls.push(plan.requestId);
          if (plan.requestId % 2 === 0) throw new Error('synthetic failure');
          return plan.value;
        },
        failure: (_cause, plan) => -plan.requestId,
      });
      for (const id of ids) bus.publish(mount.exports.command, { requestId: id, operationKey: id, value: id });
      await bus.drain();
      assert.deepEqual(calls, [...new Set(ids)]);
      await bus.disposeAsync();
    }),
    { numRuns: 30, seed: 260903 },
  );
});

await test('direct array outcomes are values, while explicit sync/async streams share cleanup semantics', async () => {
  const mount = mountLibrary(model(), 'stream');
  const composition = composeLibraries(mount);
  const sync = composition.createRuntime({ scheduler: new ManualScheduler() });
  let returned = 0;
  bindEffect(sync, mount.exports.effect, {
    stream: function* () {
      try {
        yield 1;
        yield 2;
      } finally {
        returned++;
      }
    },
    failure: () => -1,
  });
  sync.publish(mount.exports.command, { requestId: 1, operationKey: 1, value: 0 });
  await sync.drain();
  assert.deepEqual(sync.read(mount.exports.values), [1, 2]);
  assert.equal(returned, 1);
  await sync.disposeAsync();
  const async = composition.createRuntime({ scheduler: new ManualScheduler() });
  bindEffect(async, mount.exports.effect, {
    stream: async function* () {
      try {
        yield 3;
        yield 4;
      } finally {
        returned++;
      }
    },
    failure: () => -1,
  });
  async.publish(mount.exports.command, { requestId: 1, operationKey: 1, value: 0 });
  await async.drain();
  assert.deepEqual(async.read(mount.exports.values), [3, 4]);
  assert.equal(returned, 2);
  await async.disposeAsync();
  const array = composition.createRuntime({ scheduler: new ManualScheduler() });
  const arrayEffect = defineEffect({
    command: mount.exports.command,
    result: mount.exports.result,
    plan: (_state, command) => command,
    decode: (_plan, outcome: readonly number[]) => outcome.length,
  });
  bindEffect(array, arrayEffect, { execute: () => [1, 2, 3], failure: () => [] });
  array.publish(mount.exports.command, { requestId: 1, operationKey: 1, value: 0 });
  await array.drain();
  assert.deepEqual(array.read(mount.exports.values), [3]);
  await array.disposeAsync();
});

await test('awaitable disposal waits for cooperative iterator.return exactly once and suppresses late publications', async () => {
  const mount = mountLibrary(model(), 'finalization');
  const bus = composeLibraries(mount).createRuntime({ scheduler: new ManualScheduler() });
  const next = Promise.withResolvers<IteratorResult<number>>();
  const finalized = Promise.withResolvers<void>();
  let returns = 0;
  let drained = false;
  bindEffect(bus, mount.exports.effect, {
    stream: () => ({
      [Symbol.asyncIterator]: () => ({
        next: () => next.promise,
        return: async () => {
          returns++;
          next.resolve({ done: true, value: undefined });
          await finalized.promise;
          return { done: true, value: undefined };
        },
      }),
    }),
    failure: () => -99,
  });
  bus.publish(mount.exports.command, { requestId: 1, operationKey: 1, value: 0 });
  bus.flush();
  const closed = bus.disposeAsync().then(() => {
    drained = true;
  });
  await Promise.resolve();
  assert.equal(returns, 1);
  assert.equal(drained, false);
  finalized.resolve();
  await closed;
  assert.equal(returns, 1);
  assert.equal(drained, true);
});

await test('cross-library save/commit/push reactions use successor waves and replay recorded commands without I/O or duplication', async () => {
  const source = defineLibrary({
    name: 'source',
    requires: [],
    setup(scope) {
      const saved = scope.event('saved', { codec: numberCodec });
      return { saved };
    },
  });
  const sourceMount = mountLibrary(source, 'public');
  const savedCapability = defineCapability<typeof sourceMount.exports.saved>('saved');
  const hosted = defineLibrary({
    name: 'host',
    requires: [savedCapability],
    setup(scope) {
      const saved = scope.require(savedCapability);
      const committed = scope.scalar('committed', () => 0, { codec: numberCodec });
      const pushed = scope.scalar('pushed', () => 0, { codec: numberCodec });
      const commit = scope.command('commit', (_state, _request: number) => true, { codec: numberCodec });
      const commitResult = scope.event('commitResult', { codec: numberCodec });
      const push = scope.command('push', (_state, _request: number) => true, { codec: numberCodec });
      const pushResult = scope.event('pushResult', { codec: numberCodec });
      scope.react(saved, commit, (_state, revision) => revision);
      scope.react(commitResult, push, (state, revision) => (state.read(committed) === revision ? revision : undefined));
      scope.reduce(commitResult, (state, revision) => state.set(committed, revision));
      scope.reduce(pushResult, (state, revision) => state.set(pushed, revision));
      const committing = scope.requireEffect(
        defineEffect({
          command: commit,
          result: commitResult,
          plan: (_state, revision) => ({ requestId: revision }),
          decode: (_plan, outcome: number) => outcome,
        }),
      );
      const pushing = scope.requireEffect(
        defineEffect({
          command: push,
          result: pushResult,
          plan: (_state, revision) => ({ requestId: revision }),
          decode: (_plan, outcome: number) => outcome,
        }),
      );
      return { committed, pushed, committing, pushing };
    },
  });
  const host = mountLibrary(hosted, 'private', [savedCapability.provide(sourceMount.exports.saved)]);
  const composition = composeLibraries(sourceMount, host);
  const bus = composition.createRuntime({ scheduler: new ManualScheduler() });
  let io = 0;
  for (const effect of [host.exports.committing, host.exports.pushing])
    bindEffect(bus, effect, {
      execute: (plan) => {
        io++;
        return plan.requestId;
      },
      failure: () => -1,
    });
  bus.assertReady();
  const record = recordRollingScenario(bus, { maxEvents: 3 });
  bus.publish(sourceMount.exports.saved, 7);
  await bus.drain();
  assert.equal(bus.read(host.exports.pushed), 7);
  assert.equal(io, 2);
  const replay = replayScenario(composition, record.snapshot());
  replay.assertReady();
  assert.equal(canonicalCapture(captureCheckpoint(bus)), canonicalCapture(captureCheckpoint(replay)));
  assert.equal(io, 2);
  await replay.disposeAsync();
  await bus.disposeAsync();
});

await test('reaction step budget bounds total fanout per causal root, not merely depth', () => {
  const cyclic = defineLibrary({
    name: 'cycle',
    requires: [],
    setup(scope) {
      const count = scope.scalar('count', () => 0, { codec: numberCodec });
      const ping = scope.event('ping', { codec: numberCodec });
      scope.reduce(ping, (state) => state.set(count, state.read(count) + 1));
      scope.react(ping, ping, (_state, value) => value);
      scope.react(ping, ping, (_state, value) => value);
      return { ping, count };
    },
  });
  const mount = mountLibrary(cyclic, 'bounded');
  const errors: unknown[] = [];
  const bus = composeLibraries(mount).createRuntime({
    scheduler: new ManualScheduler(),
    maxReactionSteps: 8,
    onError: (cause) => errors.push(cause),
  });
  bus.publish(mount.exports.ping, 1);
  bus.flush();
  assert.equal(bus.read(mount.exports.count), 9);
  assert.ok(errors.some((error) => error instanceof CaptureError && error.issue.boundary === 'reaction steps'));
  const record = recordScenario(bus);
  record.dispose();
  bus.dispose();
});

await test('runtime diagnostic facts are typed, codec-compatible, replayable, and contain no raw exception text', () => {
  const diagnosticCodec = codec('diagnostic', typia.createAssert<RuntimeDiagnostic>());
  const nullableDiagnostic = codec('last-diagnostic', typia.createAssert<RuntimeDiagnostic | null>());
  const library = defineLibrary({
    name: 'diagnostic',
    requires: [],
    setup(scope) {
      const last = scope.scalar('last', (): RuntimeDiagnostic | null => null, { codec: nullableDiagnostic });
      const failed = scope.event('failed', { codec: diagnosticCodec });
      scope.reduce(failed, (state, diagnostic) => state.set(last, diagnostic));
      return { failed, last };
    },
  });
  const mount = mountLibrary(library, 'diagnostic');
  const composition = composeLibraries(mount);
  const bus = composition.createRuntime({ scheduler: new ManualScheduler(), onError: () => {} });
  const record = recordScenario(bus);
  bindRuntimeDiagnostics(bus, mount.exports.failed, (diagnostic) => diagnostic);
  bus.reportError(new Error('Bearer SYNTHETIC_SECRET'), 'planner');
  bus.flush();
  assert.equal(bus.read(mount.exports.last)?.phase, 'planner');
  assert.ok(!canonicalCapture(record.snapshot()).includes('SYNTHETIC_SECRET'));
  const replay = replayScenario(composition, record.snapshot());
  assert.deepEqual(replay.read(mount.exports.last), bus.read(mount.exports.last));
  replay.dispose();
  bus.dispose();
});
console.log(
  JSON.stringify({ executionScenarios: passed, generatedDuplicateRuns: 30, source: 'packed public exports' }),
);
