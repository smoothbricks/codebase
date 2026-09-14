import assert from 'node:assert/strict';
import {
  bindEffect,
  captureCheckpoint,
  createBusApi,
  defineCapability,
  defineEffect,
  ManualScheduler,
  recordScenario,
  type ValueCodec,
} from '@smoothbricks/statebus-core';

const numberCodec: ValueCodec<number> = {
  schema: 'sibling.number',
  version: 1,
  encode: (value) => value,
  decode(value) {
    if (typeof value !== 'number' || !Number.isFinite(value)) throw new Error('Expected a finite number.');
    return value;
  },
};
const booleanCodec: ValueCodec<boolean> = {
  schema: 'sibling.boolean',
  version: 1,
  encode: (value) => value,
  decode(value) {
    if (typeof value !== 'boolean') throw new Error('Expected a boolean.');
    return value;
  },
};
let initializations = 0;
const session = createBusApi({
  name: 'session',
  setup(scope) {
    const allowed = scope.scalar(
      'allowed',
      () => {
        initializations++;
        return false;
      },
      { codec: booleanCodec },
    );
    const changed = scope.event('changed', { codec: booleanCodec });
    scope.reduce(changed, (state, value) => state.set(allowed, value));
    return { allowed, changed };
  },
});
type Session = ReturnType<typeof session.getBus>['exports'];
const sessionAccess = defineCapability<Session | null>('inventory.session');
const inventory = createBusApi({
  name: 'inventory',
  requires: [sessionAccess],
  // Explicit standalone policy. The application replaces this binding with its real session.
  bindings: [sessionAccess.provide(null)],
  setup(scope) {
    const access = scope.require(sessionAccess);
    const value = scope.scalar('value', () => 0, { codec: numberCodec });
    const command = scope.command('command', (state, _id: number) => access === null || state.read(access.allowed), {
      codec: numberCodec,
    });
    const completed = scope.event('completed', { codec: numberCodec });
    scope.reduce(completed, (state, amount) => state.set(value, state.read(value) + amount));
    if (access)
      scope.reduce(access.changed, (state, allowed) => {
        if (!allowed) state.set(value, 0);
      });
    const effect = defineEffect({
      command,
      result: completed,
      plan: (state, requestId) => (access === null || state.read(access.allowed) ? { requestId } : undefined),
      decode: (_plan, outcome: number) => outcome,
    });
    return { value, command, completed, effect, access };
  },
});
let resolutions = 0;
const app = createBusApi({
  name: 'sibling-app',
  // Declaration order is not dependency order: bindings resolve the sibling first.
  libraries: { inventory, session },
  libraryBindings: {
    inventory: (libraries) => {
      resolutions++;
      return [sessionAccess.provide(libraries.get('session'))];
    },
  },
  setup: () => ({}),
});
let passed = 0;
async function test(name: string, run: () => void | Promise<void>): Promise<void> {
  await run();
  passed++;
  console.log(`PASS API bindings: ${name}`);
}

await test('typed siblings resolve before live state and only once per containing definition', () => {
  assert.equal(initializations, 0);
  assert.equal(resolutions, 1);
  const a = app.createBus({ scheduler: new ManualScheduler() });
  const b = app.createBus({ scheduler: new ManualScheduler() });
  try {
    const model = inventory.getBus(a).exports;
    const auth = session.getBus(a).exports;
    assert.equal(model.access, auth);
    assert.equal(initializations, 2);
    for (let i = 0; i < 100; i++) {
      a.read(model.value);
      assert.equal(inventory.getBus(a), app.getBus(a).library('inventory'));
    }
    assert.equal(resolutions, 1, 'Reads, bus creation and publication cannot rerun the resolver.');
    a.publish(auth.changed, true);
    a.flush();
    assert.equal(a.read(auth.allowed), true);
    assert.equal(b.read(session.getBus(b).exports.allowed), false);
  } finally {
    a.dispose();
    b.dispose();
  }
});

await test('session loss and a queued mutation share pure admission and no-I/O decision replay', async () => {
  const bus = app.createBus({ scheduler: new ManualScheduler() });
  const model = inventory.getBus(bus).exports;
  const auth = session.getBus(bus).exports;
  const record = recordScenario(bus);
  let calls = 0;
  bindEffect(bus, model.effect, { execute: () => ++calls, failure: () => -1 });
  try {
    bus.publish(auth.changed, true);
    bus.publish(model.command, 1);
    await bus.drain();
    assert.equal(calls, 1);
    assert.equal(bus.read(model.value), 1);
    bus.publish(auth.changed, false);
    bus.publish(model.command, 2);
    await bus.drain();
    assert.equal(calls, 1);
    assert.equal(bus.read(model.value), 0);
    const replay = app.replayScenario(record.snapshot());
    try {
      assert.deepEqual(captureCheckpoint(replay), captureCheckpoint(bus));
      bindEffect(replay, inventory.getBus(replay).exports.effect, {
        execute: () => {
          throw new Error('Replay cannot execute.');
        },
        failure: () => -1,
      });
      assert.equal(calls, 1);
    } finally {
      replay.dispose();
    }
  } finally {
    record.dispose();
    bus.dispose();
  }
});

await test('nested repeated applications bind each feature to its own session occurrence', () => {
  const host = createBusApi({ name: 'two-apps', libraries: { left: app, right: app }, setup: () => ({}) });
  const bus = host.createBus({ scheduler: new ManualScheduler() });
  const other = host.createBus({ scheduler: new ManualScheduler() });
  try {
    const left = host.getBus(bus).library('left');
    const right = host.getBus(bus).library('right');
    const a = left.library('inventory').exports;
    const b = right.library('inventory').exports;
    assert.equal(a.access, left.library('session').exports);
    assert.equal(b.access, right.library('session').exports);
    assert.notEqual(a.access, b.access);
    left.publish(left.library('session').exports.changed, true);
    bus.flush();
    assert.equal(bus.read(left.library('session').exports.allowed), true);
    assert.equal(bus.read(right.library('session').exports.allowed), false);
    assert.equal(other.read(host.getBus(other).library('left').library('session').exports.allowed), false);
  } finally {
    bus.dispose();
    other.dispose();
  }
});

await test('missing, duplicate, incompatible and cyclic bindings fail before any state initializes', () => {
  const before = initializations;
  assert.throws(
    () =>
      createBusApi({
        name: 'missing',
        libraries: { inventory },
        libraryBindings: { inventory: () => [] },
        setup: () => ({}),
      }),
    /Missing/,
  );
  assert.throws(
    () =>
      createBusApi({
        name: 'duplicate',
        libraries: { inventory },
        libraryBindings: { inventory: () => [sessionAccess.provide(null), sessionAccess.provide(null)] },
        setup: () => ({}),
      }),
    /Duplicate/,
  );
  const foreign = defineCapability<string>('foreign');
  assert.throws(
    () =>
      createBusApi({
        name: 'foreign',
        libraries: { inventory },
        libraryBindings: { inventory: () => [foreign.provide('no')] },
        setup: () => ({}),
      }),
    /incompatible/,
  );
  assert.throws(
    () =>
      createBusApi({
        name: 'cycle',
        libraries: { left: inventory, right: inventory },
        libraryBindings: {
          left: (libraries) => {
            libraries.get('right');
            return [sessionAccess.provide(null)];
          },
          right: (libraries) => {
            libraries.get('left');
            return [sessionAccess.provide(null)];
          },
        },
        setup: () => ({}),
      }),
    /Cyclic library bindings/,
  );
  assert.equal(initializations, before);
});

await test('resolver configuration is captured rather than read from a caller-mutable map', () => {
  const libraryBindings = { inventory: () => [sessionAccess.provide(null)] };
  const child = createBusApi({ name: 'captured', libraries: { inventory }, libraryBindings, setup: () => ({}) });
  libraryBindings.inventory = () => {
    throw new Error('Changed after declaration');
  };
  const host = createBusApi({ name: 'host', libraries: { child }, setup: () => ({}) });
  const bus = host.createBus();
  try {
    assert.equal(inventory.getBus(bus).exports.access, null);
  } finally {
    bus.dispose();
  }
});

function invalidCalls(): void {
  createBusApi({
    name: 'typed',
    libraries: { inventory, session },
    libraryBindings: {
      inventory: (libraries) => {
        // @ts-expect-error Sibling names are inferred, not an arbitrary string address.
        libraries.get('missing');
        // @ts-expect-error The session capability cannot accept unrelated feature exports.
        sessionAccess.provide(libraries.get('inventory'));
        return [sessionAccess.provide(libraries.get('session'))];
      },
    },
    setup: () => ({}),
  });
  createBusApi({
    name: 'typed-keys',
    libraries: { session },
    libraryBindings: {
      // @ts-expect-error Cannot bind an undeclared child.
      missing: () => [],
    },
    setup: () => ({}),
  });
}
void invalidCalls;
console.log(JSON.stringify({ siblingBindingScenarios: passed, builtExports: true }));
