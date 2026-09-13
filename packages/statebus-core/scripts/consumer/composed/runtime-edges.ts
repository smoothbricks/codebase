import assert from 'node:assert/strict';
import {
  bindEffect,
  composeLibraries,
  DispatchCycleError,
  defineEffect,
  defineLibrary,
  EffectCapacityError,
  type EffectPolicy,
  ManualScheduler,
  mountLibrary,
} from '@smoothbricks/statebus-core';
import fc from 'fast-check';

interface Plan {
  readonly requestId: number;
  readonly operationKey: string | number;
  readonly value: number;
}
function fixture(policy: EffectPolicy = 'parallel') {
  const library = defineLibrary({
    name: 'execution-audit',
    requires: [],
    setup(scope) {
      const values = scope.scalar('values', (): readonly number[] => []);
      const command = scope.command('command', (_state, _plan: Plan) => true);
      const result = scope.event<number>('result');
      const fail = scope.event<number>('fail');
      scope.reduce(result, (state, value) => state.set(values, [...state.read(values), value]));
      scope.reduce(fail, () => {
        throw new Error('failed admission wave');
      });
      const effect = defineEffect({
        command,
        result,
        policy,
        plan: (_state, plan) => plan,
        decode: (_plan, value: number) => value,
        cancelled: (plan) => -plan.requestId,
      });
      return { values, command, fail, effect };
    },
  });
  const mount = mountLibrary(library, 'audit');
  const bus = composeLibraries(mount).createRuntime({ scheduler: new ManualScheduler() });
  return { bus, ...mount.exports };
}
const plan = (requestId: number, operationKey: string | number = 'key'): Plan => ({
  requestId,
  operationKey,
  value: requestId,
});
let passed = 0;
async function test(name: string, run: () => void | Promise<void>): Promise<void> {
  await run();
  passed++;
  console.log(`PASS runtime audit: ${name}`);
}

await test('serialized cancellation removes queued jobs without compaction and preserves FIFO', async () => {
  const { bus, command, values, effect } = fixture('serialize');
  const first = Promise.withResolvers<number>();
  const calls: number[] = [];
  const binding = bindEffect(bus, effect, {
    maxPending: 2500,
    execute: (input) => {
      calls.push(input.requestId);
      return input.requestId === 1 ? first.promise : input.value;
    },
    failure: () => -9999,
  });
  try {
    for (let id = 1; id <= 2000; id++) bus.publish(command, plan(id));
    bus.flush();
    for (let id = 2; id <= 2000; id += 2) assert.equal(binding.cancel(id), true);
    assert.deepEqual(calls, [1]);
    const drained = binding.drain();
    assert.equal(binding.drain(), drained, 'Concurrent waiters share completion, not per-waiter task arrays.');
    first.resolve(1);
    await drained;
    await bus.drain();
    assert.deepEqual(
      calls,
      Array.from({ length: 1000 }, (_, index) => index * 2 + 1),
    );
    assert.deepEqual(
      bus.read(values).filter((value) => value > 0),
      calls,
    );
  } finally {
    bus.dispose();
  }
});

await test('capacity counts active, queued, and cancelled-but-unsettled operations', async () => {
  const { bus, command, values, effect } = fixture('serialize');
  const pending = Promise.withResolvers<number>();
  const calls: number[] = [];
  const rejected: number[] = [];
  const binding = bindEffect(bus, effect, {
    maxPending: 2,
    execute: (input) => {
      calls.push(input.requestId);
      return input.requestId === 1 ? pending.promise : input.value;
    },
    failure: (cause, input) => {
      assert.ok(cause instanceof EffectCapacityError);
      assert.equal(cause.code, 'effect-capacity');
      assert.equal(cause.limit, 2);
      rejected.push(input.requestId);
      return -1000 - input.requestId;
    },
  });
  try {
    for (const id of [1, 2, 3, 3]) bus.publish(command, plan(id));
    bus.flush();
    assert.deepEqual(rejected, [3]);
    assert.deepEqual(calls, [1]);
    assert.equal(binding.cancel(2), true);
    bus.publish(command, plan(4));
    bus.flush();
    assert.deepEqual(rejected, [3], 'Cancelling queued work releases capacity immediately.');
    assert.equal(binding.cancel(1), true);
    bus.publish(command, plan(5));
    bus.flush();
    assert.deepEqual(rejected, [3, 5], 'An aborted non-cooperative operation still consumes capacity.');
    pending.resolve(1);
    await bus.drain();
    assert.deepEqual(calls, [1, 4]);
    assert.ok(!bus.read(values).includes(1));
    bus.publish(command, plan(6));
    await bus.drain();
    assert.deepEqual(calls, [1, 4, 6]);
  } finally {
    bus.dispose();
  }
});

await test('latest-wins is bounded even when superseded operations ignore abort', async () => {
  const { bus, command, values, effect } = fixture('latest-wins');
  const pending = Promise.withResolvers<number>();
  let calls = 0;
  let rejected = 0;
  bindEffect(bus, effect, {
    maxPending: 2,
    execute: () => {
      calls++;
      return pending.promise;
    },
    failure: (cause) => {
      assert.ok(cause instanceof EffectCapacityError);
      rejected++;
      return -999;
    },
  });
  try {
    for (const id of [1, 2, 3, 3]) bus.publish(command, plan(id));
    bus.flush();
    assert.equal(calls, 2);
    assert.equal(rejected, 1);
    pending.resolve(42);
    await bus.drain();
    assert.equal(bus.read(values).filter((value) => value === 42).length, 1);
  } finally {
    bus.dispose();
  }
});

await test('key cancellation isolates numeric/string keys and unrelated parallel requests', async () => {
  const { bus, command, values, effect } = fixture();
  const pending = Promise.withResolvers<number>();
  const aborted: number[] = [];
  const binding = bindEffect(bus, effect, {
    execute: (input, { signal }) => {
      signal.addEventListener('abort', () => aborted.push(input.requestId), { once: true });
      return pending.promise;
    },
    failure: () => -999,
  });
  try {
    for (const input of [plan(1, 7), plan(2, '7'), plan(3, 'other'), plan(4, 7)]) bus.publish(command, input);
    bus.flush();
    assert.equal(binding.cancelKey(7), 2);
    assert.equal(binding.cancelKey(7), 0);
    assert.deepEqual(aborted, [1, 4]);
    pending.resolve(100);
    await bus.drain();
    assert.equal(bus.read(values).filter((value) => value === 100).length, 2);
  } finally {
    bus.dispose();
  }
});

await test('a reentrant abort observer cannot make key cancellation consume newly submitted work', async () => {
  const { bus, command, values, effect } = fixture();
  const pending = Promise.withResolvers<number>();
  const calls: number[] = [];
  const binding = bindEffect(bus, effect, {
    execute: (input, { signal }) => {
      calls.push(input.requestId);
      if (input.requestId === 1)
        signal.addEventListener(
          'abort',
          () => {
            bus.publish(command, plan(2));
            bus.flush();
          },
          { once: true },
        );
      return pending.promise;
    },
    failure: () => -999,
  });
  try {
    bus.publish(command, plan(1));
    bus.flush();
    assert.equal(binding.cancelKey('key'), 1);
    assert.deepEqual(calls, [1, 2]);
    pending.resolve(20);
    await bus.drain();
    assert.equal(bus.read(values).filter((value) => value === 20).length, 1);
  } finally {
    bus.dispose();
  }
});

await test('capture-time cancellation happens before execution and releases its reservation', async () => {
  const { bus, command, effect } = fixture();
  let calls = 0;
  const binding = bindEffect(bus, effect, {
    maxPending: 1,
    execute: (input) => {
      calls++;
      return input.value;
    },
    failure: () => -999,
    captureInstruction: (input) => {
      if (input.requestId === 1) assert.equal(binding.cancel(1), true);
    },
  });
  try {
    bus.publish(command, plan(1));
    bus.publish(command, plan(2));
    await bus.drain();
    assert.equal(calls, 1);
  } finally {
    bus.dispose();
  }
});

await test('disposal from capture suppresses execution and leaves a reusable binding slot', async () => {
  const { bus, command, effect } = fixture();
  let calls = 0;
  const binding = bindEffect(bus, effect, {
    execute: () => {
      calls++;
      return 1;
    },
    failure: () => -1,
    captureInstruction: () => binding.dispose(),
  });
  try {
    bus.publish(command, plan(1));
    bus.flush();
    await binding.drain();
    assert.equal(calls, 0);
    const replacement = bindEffect(bus, effect, { execute: () => ++calls, failure: () => -1 });
    bus.publish(command, plan(2));
    await bus.drain();
    assert.equal(calls, 1);
    replacement.dispose();
  } finally {
    bus.dispose();
  }
});

await test('failed reductions neither reserve execution slots nor report capacity failures', async () => {
  const { bus, command, fail, effect } = fixture();
  let calls = 0;
  let failures = 0;
  bindEffect(bus, effect, { maxPending: 1, execute: () => ++calls, failure: () => --failures });
  try {
    bus.publish(command, plan(1));
    bus.publish(fail, 1);
    assert.throws(() => bus.flush(), /failed admission wave/);
    bus.publish(command, plan(2));
    await bus.drain();
    assert.equal(calls, 1);
    assert.equal(failures, 0);
  } finally {
    bus.dispose();
  }
});

await test('invalid capacity fails before registration and direct failure does not poison the next request', async () => {
  const { bus, command, effect } = fixture();
  try {
    for (const maxPending of [0, -1, 0.5, Number.NaN, Number.POSITIVE_INFINITY])
      assert.throws(() => bindEffect(bus, effect, { maxPending, execute: () => 1, failure: () => -1 }), RangeError);
    let calls = 0;
    const binding = bindEffect(bus, effect, {
      maxPending: 1,
      execute: () => {
        calls++;
        throw Error('transport');
      },
      failure: () => -1,
    });
    for (let id = 1; id < 20; id++) {
      bus.publish(command, plan(id));
      await bus.drain();
    }
    assert.equal(calls, 19);
    await binding.disposeAsync();
  } finally {
    bus.dispose();
  }
});

await test('generated serialized traces never start cancelled queue entries and keep FIFO', async () => {
  await fc.assert(
    fc.asyncProperty(fc.array(fc.boolean(), { minLength: 40, maxLength: 200 }), async (cancelled) => {
      const { bus, command, effect } = fixture('serialize');
      const first = Promise.withResolvers<number>();
      const calls: number[] = [];
      const binding = bindEffect(bus, effect, {
        execute: (input) => {
          calls.push(input.requestId);
          return input.requestId === 1 ? first.promise : input.value;
        },
        failure: () => -999,
      });
      try {
        bus.publish(command, plan(1));
        for (let index = 0; index < cancelled.length; index++) bus.publish(command, plan(index + 2));
        bus.flush();
        const expected = [1];
        for (let index = 0; index < cancelled.length; index++) {
          if (cancelled[index]) binding.cancel(index + 2);
          else expected.push(index + 2);
        }
        first.resolve(1);
        await bus.drain();
        assert.deepEqual(calls, expected);
      } finally {
        bus.dispose();
      }
    }),
    { numRuns: 50, seed: 912873 },
  );
});

function counter() {
  const library = defineLibrary({
    name: 'cascade',
    requires: [],
    setup(scope) {
      const value = scope.scalar('value', () => 0);
      const add = scope.event<number>('add');
      scope.reduce(add, (state, amount) => state.set(value, state.read(value) + amount));
      return { value, add };
    },
  });
  const mounted = mountLibrary(library, 'cascade');
  return { composition: composeLibraries(mounted), ...mounted.exports };
}
await test('imperative listener cycles stop at complete-wave boundaries and preserve queued events for recovery', () => {
  const { composition, value, add } = counter();
  const scheduler = new ManualScheduler();
  const bus = composition.createRuntime({ scheduler, maxWavesPerFlush: 3 });
  const stop = bus.listen(add, () => bus.publish(add, 1));
  try {
    bus.publish(add, 1);
    assert.throws(
      () => bus.flush(),
      (cause) => cause instanceof DispatchCycleError && cause.limit === 3,
    );
    assert.equal(bus.read(value), 3);
    assert.equal(bus.dispatchPaused, true);
    assert.equal(bus.idle, false);
    assert.equal(scheduler.pending, 0);
    stop();
    bus.publish(add, 10);
    assert.equal(scheduler.pending, 0);
    bus.flush();
    assert.equal(bus.read(value), 14);
    assert.equal(bus.dispatchPaused, false);
    assert.equal(bus.idle, true);
  } finally {
    bus.dispose();
  }
});

await test('stale microtasks cannot resume a cycle-paused queue; default scheduling reports once', async () => {
  const { composition, value, add } = counter();
  const errors: unknown[] = [];
  const bus = composition.createRuntime({ maxWavesPerFlush: 2, onError: (cause) => errors.push(cause) });
  const stop = bus.listen(add, () => bus.publish(add, 1));
  try {
    bus.publish(add, 1);
    assert.throws(() => bus.flush(), DispatchCycleError);
    for (let index = 0; index < 5; index++) await Promise.resolve();
    assert.equal(bus.read(value), 2);
    assert.equal(errors.length, 0);
    stop();
    bus.flush();
    assert.equal(bus.read(value), 3);
    const stopAgain = bus.listen(add, () => bus.publish(add, 1));
    bus.publish(add, 1);
    for (let index = 0; index < 5; index++) await Promise.resolve();
    assert.equal(bus.read(value), 5);
    assert.equal(errors.length, 1);
    assert.ok(errors[0] instanceof DispatchCycleError);
    stopAgain();
    bus.flush();
    assert.equal(bus.read(value), 6);
  } finally {
    bus.dispose();
  }
});

console.log(JSON.stringify({ runtimeAuditScenarios: passed, publicExports: true }));
