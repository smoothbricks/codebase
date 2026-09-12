import assert from 'node:assert/strict';
import { Err, Ok } from '@smoothbricks/lmao';
import {
  bindEffect,
  type ComposedRuntime,
  captureCheckpoint,
  composeLibraries,
  defineEffect,
  defineLibrary,
  ManualScheduler,
  mountLibrary,
  recordScenario,
  replayScenario,
} from '@smoothbricks/statebus-core';
import { type LoadRequest, loadFingerprint, loadRequestId, previousData } from '@smoothbricks/statebus-data-loader';
import { parseScenario, requestId, shelfCodec, shelfId } from './codecs.js';
import { canAdjust, inventoryLibrary } from './library.js';

const definition = defineLibrary({
  name: 'execution-edges',
  requires: [],
  setup(scope) {
    const value = scope.scalar('value', () => 0);
    // Deliberately permissive admission tests the executor's in-flight request identity guard.
    const command = scope.command<number>('command', () => true);
    const result = scope.event<number>('result');
    const fail = scope.event<number>('fail');
    scope.reduce(command, (state, amount) => state.set(value, state.read(value) + amount));
    scope.reduce(fail, () => {
      throw new Error('reducer invariant');
    });
    const effect = defineEffect({
      command,
      result,
      plan: (state, requestId) => ({ requestId, value: state.read(value) }),
      decode: (_plan, outcome: number) => outcome,
    });
    return { value, command, result, fail, effect };
  },
});
const mount = mountLibrary(definition, 'edges');
const composition = composeLibraries(mount);
const model = mount.exports;
async function settle(runtime: ComposedRuntime): Promise<void> {
  for (let i = 0; i < 6; i++) {
    await Promise.resolve();
    runtime.flush();
  }
}
const passed: string[] = [];
async function test(name: string, run: () => void | Promise<void>): Promise<void> {
  await run();
  passed.push(name);
  console.log(`PASS ${name}`);
}

await test('synchronous operation rejection reserves the request through the whole command wave', async () => {
  const runtime = composition.createRuntime({ scheduler: new ManualScheduler() });
  const outcomes: number[] = [];
  let executions = 0;
  runtime.listen(model.result, (outcome) => outcomes.push(outcome));
  bindEffect(runtime, model.effect, {
    execute: (plan) => {
      executions++;
      assert.equal(plan.value, 2, 'Planner must read batch-final state.');
      throw new Error('synchronous transport failure');
    },
    failure: () => -1,
  });
  try {
    runtime.publish(model.command, 1);
    runtime.publish(model.command, 1);
    runtime.flush();
    assert.equal(executions, 1);
    assert.deepEqual(outcomes, [], 'Outcomes cannot reduce in the command wave.');
    await settle(runtime);
    assert.deepEqual(outcomes, [-1]);
  } finally {
    runtime.dispose();
  }
});

await test('failed reductions preserve acquisition and terminal-zero interest without retrying domain commands', () => {
  const runtime = composition.createRuntime({ scheduler: new ManualScheduler() });
  const counts: number[] = [];
  let executions = 0;
  runtime.interestSource.subscribe((changes) => {
    for (const change of changes) counts.push(change.subscribers);
  });
  bindEffect(runtime, model.effect, {
    execute: () => {
      executions++;
      return Promise.resolve(1);
    },
    failure: () => -1,
  });
  try {
    const release = runtime.acquire([model.value]);
    runtime.publish(model.command, 1);
    runtime.publish(model.fail, 0);
    assert.throws(() => runtime.flush(), /reducer invariant/);
    assert.equal(runtime.read(model.value), 0);
    assert.deepEqual(counts, []);
    assert.equal(executions, 0);
    runtime.flush();
    assert.deepEqual(counts, [1]);
    assert.equal(executions, 0);
    release();
    runtime.publish(model.fail, 0);
    assert.throws(() => runtime.flush(), /reducer invariant/);
    runtime.flush();
    assert.deepEqual(counts, [1, 0]);
    assert.equal(runtime.interestSource.snapshot().length, 0);
  } finally {
    runtime.dispose();
  }
});

await test('capture observer failures do not strand admitted effects or suppress other observers', async () => {
  const errors: unknown[] = [];
  const runtime = composition.createRuntime({
    scheduler: new ManualScheduler(),
    onError: (cause) => errors.push(cause),
  });
  let executions = 0;
  let observedWaves = 0;
  const outcomes: number[] = [];
  runtime.observeWaves({
    committed() {
      throw new Error('capture failure');
    },
  });
  runtime.observeWaves({
    committed() {
      observedWaves++;
    },
  });
  runtime.listen(model.result, (outcome) => outcomes.push(outcome));
  bindEffect(runtime, model.effect, {
    execute: (plan) => {
      executions++;
      return Promise.resolve(plan.value);
    },
    failure: () => -1,
  });
  try {
    runtime.publish(model.command, 1);
    await settle(runtime);
    assert.equal(executions, 1);
    assert.deepEqual(outcomes, [1]);
    assert.equal(observedWaves, 2);
    assert.equal(errors.length, 2);
  } finally {
    runtime.dispose();
  }
});
await test('portable JSON envelopes and branded resource codecs replay the same final state without operations', async () => {
  const inventory = mountLibrary(inventoryLibrary, 'portable', [canAdjust.provide(true)]);
  const app = composeLibraries(inventory);
  const runtime = app.createRuntime({ scheduler: new ManualScheduler() });
  const recorder = recordScenario(runtime);
  const model = inventory.exports;
  const id = shelfId('shelf:json');
  const request: LoadRequest = {
    interest: model.inventory.at(id).interest,
    requestId: loadRequestId('portable-read'),
    fingerprint: loadFingerprint('portable-read'),
    at: 1,
    reason: 'interest',
    policy: 'latest-wins',
  };
  let operations = 0;
  bindEffect(runtime, model.effect, {
    execute: (plan) => {
      operations++;
      return Promise.resolve(new Ok(plan.next));
    },
    failure: () => new Err('unexpected'),
  });
  try {
    runtime.publish(model.selectionChanged, id);
    runtime.publish(model.load, { type: 'loadRequested', request });
    runtime.publish(model.load, { type: 'loadSucceeded', request, value: 8, at: 1 });
    runtime.flush();
    runtime.publish(model.adjust, { shelfId: id, requestId: requestId('request:json'), add: 1 });
    await settle(runtime);
    assert.equal(operations, 1);
    assert.equal(previousData(runtime.readKeyed(model.inventory, id))?.value, 9);
    const recorded = parseScenario(JSON.stringify(recorder.snapshot()));
    const replay = replayScenario(app, recorded);
    try {
      bindEffect(replay, model.effect, {
        execute: () => {
          operations++;
          throw new Error('Production I/O is forbidden');
        },
        failure: () => new Err('unexpected'),
      });
      assert.deepEqual(captureCheckpoint(replay), captureCheckpoint(runtime));
      assert.equal(operations, 1);
    } finally {
      replay.dispose();
    }
    assert.throws(() => shelfCodec.decode('request:not-a-shelf'));
    assert.throws(() => parseScenario(JSON.stringify({ ...recorded, formatVersion: 2 })));
    assert.throws(
      () =>
        replayScenario(app, {
          ...recorded,
          checkpoint: {
            ...recorded.checkpoint,
            schema: recorded.checkpoint.schema.map((entry) => ({ ...entry, version: 2 })),
          },
        }),
      /Incompatible/,
    );
  } finally {
    recorder.dispose();
    runtime.dispose();
  }
});
console.log(JSON.stringify({ passed: passed.length, tests: passed, builtExports: true }));
