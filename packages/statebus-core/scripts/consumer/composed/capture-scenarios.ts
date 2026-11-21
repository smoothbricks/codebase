import assert from 'node:assert/strict';
import {
  bindEffect,
  type CaptureEnvelope,
  CaptureError,
  canonicalCapture,
  captureBytes,
  captureCheckpoint,
  captureEffectOutcome,
  captureValue,
  composeLibraries,
  consentSupport,
  createCaptureEnvelope,
  decodeEffectInstruction,
  decodeEffectOutcome,
  defineEffect,
  defineLibrary,
  type EncodedStateEntry,
  evolveCodec,
  exportSupportCapture,
  ManualScheduler,
  migrateCaptureEnvelope,
  mountLibrary,
  publicSupport,
  type RecordedScenario,
  recordRollingScenario,
  recordScenario,
  replayCaptureEnvelope,
  replayScenario,
  type SupportClassification,
  secretSupport,
  type ValueCodec,
} from '@smoothbricks/statebus-core';
import fc from 'fast-check';
import typia from 'typia';

function codec<T>(schema: string, decode: (value: unknown) => T, version = 1): ValueCodec<T> {
  return { schema, version, decode, encode: (value) => structuredClone(value) };
}
const numberCodec = codec('number', typia.createAssert<number>());
const idCodec = codec('resource', typia.createAssert<string | number>());
interface Update {
  readonly id: string | number;
  readonly amount: number;
  readonly requestId: number;
}
const updateCodec = codec('update', typia.createAssert<Update>());
const counter = defineLibrary({
  name: 'counter',
  version: 1,
  requires: [],
  setup(scope) {
    const values = scope.keyed('values', (_id: string | number) => 0, { idCodec, codec: numberCodec });
    const last = scope.scalar('last', () => 0, { codec: numberCodec });
    const update = scope.command(
      'update',
      (state, command: Update) => {
        if (state.read(last) >= command.requestId) return false;
        state.set(last, command.requestId);
        state.setKeyed(values, command.id, state.readKeyed(values, command.id) + command.amount);
        return true;
      },
      { codec: updateCodec },
    );
    const fail = scope.event('fail', { codec: numberCodec });
    scope.reduce(fail, () => {
      throw new Error('synthetic reducer failure');
    });
    return { values, last, update, fail };
  },
});
let passed = 0;
async function test(name: string, run: () => void | Promise<void>): Promise<void> {
  await run();
  passed++;
  console.log(`PASS capture: ${name}`);
}
function sizeFailure(error: unknown): boolean {
  return error instanceof CaptureError && error.issue.code === 'size-limit';
}

await test('UTF-8 canonical byte accounting matches JSON for generated portable values without double-cloning', () => {
  fc.assert(
    fc.property(fc.jsonValue(), (value) => {
      const captured = captureValue(value);
      const text = canonicalCapture(value);
      assert.equal(captured.bytes, Buffer.byteLength(text));
      assert.equal(captureBytes(value), captured.bytes);
      assert.deepEqual(captured.value, JSON.parse(text));
    }),
    { numRuns: 250, seed: 260901 },
  );
  for (const value of ['\ud800', '\udfff', '😃', '\r\t\b\f\n', '雪', '\u0001', -0])
    assert.equal(captureBytes(value), Buffer.byteLength(JSON.stringify(value)));
  let getters = 0;
  assert.throws(
    () =>
      captureValue({
        get secret() {
          getters++;
          return 'must-not-run';
        },
      }),
    CaptureError,
  );
  assert.equal(getters, 0);
  assert.throws(() => captureValue(new Error('do not serialize me')), CaptureError);
  assert.throws(() => captureValue([undefined]), CaptureError);
  assert.throws(() => captureValue({ huge: 'x'.repeat(500) }, 50), sizeFailure);
});

await test('long generated streams retain replayable whole-wave suffixes under both count and byte bounds', () => {
  fc.assert(
    fc.property(fc.array(fc.integer({ min: -20, max: 20 }), { minLength: 100, maxLength: 250 }), (amounts) => {
      const mount = mountLibrary(counter, 'generated');
      const composition = composeLibraries(mount);
      const errors: unknown[] = [];
      const runtime = composition.createRuntime({
        scheduler: new ManualScheduler(),
        onError: (cause) => errors.push(cause),
      });
      const record = recordRollingScenario(runtime, { maxEvents: 5, maxEventBytes: 1800, maxEntryBytes: 1024 });
      let requestId = 0;
      for (const amount of amounts) {
        const command = { id: requestId % 2 === 0 ? 7 : '7', amount, requestId: ++requestId };
        runtime.publish(mount.exports.update, command);
        runtime.publish(mount.exports.update, command);
        runtime.flush();
        const stats = record.stats();
        assert.ok(stats.events <= 5);
        assert.ok(stats.eventBytes <= 1800);
        assert.ok(stats.checkpointBytes <= record.limits.maxCheckpointBytes);
      }
      const snapshot = record.snapshot();
      assert.ok(snapshot.checkpointWave > 0);
      assert.ok(snapshot.waves.every((wave) => wave.events.length === 2));
      assert.deepEqual(
        snapshot.waves.flatMap((wave) => wave.events.map((event) => event.admitted)),
        [true, false, true, false],
      );
      const replay = replayScenario(composition, snapshot);
      assert.equal(canonicalCapture(captureCheckpoint(replay)), canonicalCapture(captureCheckpoint(runtime)));
      assert.equal(
        record.stats().checkpointMaterializations,
        2,
        'No whole checkpoint rebuild during publication/eviction.',
      );
      assert.ok(record.stats().encodedCheckpointEntries <= amounts.length * 2 + 1);
      assert.equal(captureBytes(snapshot.checkpoint), record.stats().checkpointBytes);
      assert.deepEqual(errors, []);
      replay.dispose();
      record.dispose();
      runtime.dispose();
    }),
    { numRuns: 50, seed: 260902 },
  );
});

await test('byte pressure alone evicts whole waves, and failed reductions never enter the retained journal', () => {
  const mount = mountLibrary(counter, 'bytes');
  const composition = composeLibraries(mount);
  const runtime = composition.createRuntime({ scheduler: new ManualScheduler() });
  const record = recordRollingScenario(runtime, { maxEvents: 100, maxEventBytes: 800, maxEntryBytes: 512 });
  for (let requestId = 1; requestId <= 60; requestId++) {
    runtime.publish(mount.exports.update, { id: 'x', amount: 1, requestId });
    runtime.flush();
  }
  const before = canonicalCapture(record.snapshot());
  runtime.publish(mount.exports.update, { id: 'x', amount: 1000, requestId: 61 });
  runtime.publish(mount.exports.fail, 0);
  assert.throws(() => runtime.flush(), /synthetic reducer failure/);
  assert.equal(canonicalCapture(record.snapshot()), before);
  assert.equal(runtime.readKeyed(mount.exports.values, 'x'), 60);
  const replay = replayScenario(composition, record.snapshot());
  assert.equal(replay.readKeyed(mount.exports.values, 'x'), 60);
  assert.ok(record.stats().events < 100 && record.stats().evictedWaves > 0);
  replay.dispose();
  runtime.dispose();
});

await test('retained snapshots are owned and deeply immutable after journal reuse and caller payload mutation', () => {
  const mount = mountLibrary(counter, 'immutable');
  const composition = composeLibraries(mount);
  const runtime = composition.createRuntime({ scheduler: new ManualScheduler() });
  const record = recordRollingScenario(runtime, { maxEvents: 2 });
  const command = { id: 7, amount: 4, requestId: 1 };
  runtime.publish(mount.exports.update, command);
  runtime.flush();
  const retained = record.snapshot();
  const text = canonicalCapture(retained);
  command.amount = 99;
  for (let requestId = 2; requestId <= 150; requestId++) {
    runtime.publish(mount.exports.update, { id: '7', amount: 1, requestId });
    runtime.flush();
  }
  assert.equal(canonicalCapture(retained), text);
  assert.throws(
    () => Object.assign(updateCodec.decode(retained.waves[0].events[0].payload.value), { amount: 42 }),
    TypeError,
  );
  const replay = replayScenario(composition, retained);
  assert.equal(replay.readKeyed(mount.exports.values, 7), 4);
  replay.dispose();
  runtime.dispose();
});

await test('oversized checkpoint, wave and cold export are explicit refusals without suppressing committed work', () => {
  const mount = mountLibrary(counter, 'bounds');
  const composition = composeLibraries(mount);
  const errors: unknown[] = [];
  const runtime = composition.createRuntime({
    scheduler: new ManualScheduler(),
    onError: (cause) => errors.push(cause),
  });
  assert.throws(() => recordRollingScenario(runtime, { maxCheckpointBytes: 1 }), sizeFailure);
  const record = recordRollingScenario(runtime, { maxEvents: 1 });
  let notified = 0;
  runtime.listen(mount.exports.update, () => {
    notified++;
  });
  runtime.publish(mount.exports.update, { id: 'a', amount: 1, requestId: 1 });
  runtime.publish(mount.exports.update, { id: 'a', amount: 1, requestId: 2 });
  runtime.flush();
  assert.equal(notified, 2);
  assert.equal(record.stats().refusal?.code, 'size-limit');
  assert.throws(() => record.snapshot(), sizeFailure);
  record.reset();
  const smallExport = recordRollingScenario(runtime, { maxCaptureBytes: 1 });
  assert.throws(() => smallExport.snapshot(), sizeFailure);
  assert.equal(errors.length, 1);
  runtime.dispose();
});

await test('canonical keyed state separates numeric/string IDs and ignores Map insertion order', () => {
  const model = defineLibrary({
    name: 'canonical',
    requires: [],
    setup(scope) {
      const values = scope.keyed('values', (_id: string | number) => 0, { codec: numberCodec, idCodec });
      const write = scope.event('write', { codec: updateCodec });
      scope.reduce(write, (state, value) => state.setKeyed(values, value.id, value.amount));
      return { values, write };
    },
  });
  const mount = mountLibrary(model, 'canonical');
  const composition = composeLibraries(mount);
  const a = composition.createRuntime({ scheduler: new ManualScheduler() });
  const b = composition.createRuntime({ scheduler: new ManualScheduler() });
  const entries = [7, '7', 'a', 2];
  for (const [bus, order] of [
    [a, entries],
    [b, [...entries].reverse()],
  ] as const) {
    for (const id of order)
      bus.publish(mount.exports.write, { id, amount: typeof id === 'number' ? id : 100, requestId: 0 });
    bus.flush();
  }
  assert.equal(canonicalCapture(captureCheckpoint(a)), canonicalCapture(captureCheckpoint(b)));
  const encoded = captureCheckpoint(a).states[0].entries;
  assert.deepEqual(
    encoded.map((entry: EncodedStateEntry) => entry.id?.primitive),
    ['number', 'number', 'string', 'string'],
  );
  a.dispose();
  b.dispose();
});

await test('captured outcomes arriving after their command was checkpointed remain usable through the same decoder', async () => {
  const model = defineLibrary({
    name: 'operation',
    requires: [],
    setup(scope) {
      const value = scope.scalar('value', () => 0, { codec: numberCodec });
      const command = scope.command('command', (_state, _id: number) => true, { codec: numberCodec });
      const result = scope.event('result', { codec: numberCodec });
      const tick = scope.event('tick', { codec: numberCodec });
      scope.reduce(result, (state, result) => state.set(value, result));
      const effect = defineEffect({
        command,
        result,
        plan: (_state, requestId) => ({ requestId }),
        decode: (_plan, outcome: number) => outcome,
        codec: codec('operation-result', typia.createAssert<{ plan: { requestId: number }; outcome: number }>()),
      });
      return { value, command, tick, effect };
    },
  });
  const mount = mountLibrary(model, 'one');
  const composition = composeLibraries(mount);
  const runtime = composition.createRuntime({ scheduler: new ManualScheduler() });
  const record = recordRollingScenario(runtime, { maxEvents: 2, maxEffectBytes: 500 });
  const pending = Promise.withResolvers<number>();
  let calls = 0;
  const binding = bindEffect(runtime, mount.exports.effect, {
    execute: async () => {
      calls++;
      return pending.promise;
    },
    failure: () => -1,
    capture: record.outcomeSink(mount.exports.effect),
  });
  runtime.publish(mount.exports.command, 1);
  runtime.flush();
  for (let index = 0; index < 20; index++) {
    runtime.publish(mount.exports.tick, 1);
    runtime.flush();
  }
  assert.ok(record.stats().checkpointWave > 1);
  pending.resolve(42);
  for (let index = 0; index < 8; index++) {
    await Promise.resolve();
    runtime.flush();
  }
  assert.equal(runtime.read(mount.exports.value), 42);
  const sink = record.outcomeSink(mount.exports.effect);
  for (let requestId = 2; requestId < 20; requestId++) sink({ plan: { requestId }, outcome: 42 });
  assert.ok(record.stats().effectBytes <= 500 && record.stats().evictedEffects > 0);
  const snapshot = record.snapshot();
  const last = snapshot.effects.at(-1);
  assert.ok(last);
  assert.equal(mount.exports.effect.migrateCapture(last.capture).version, 1);
  const envelope = createCaptureEnvelope(composition, snapshot, {
    buildId: 'build-1',
    effects: [mount.exports.effect],
  });
  const replay = replayCaptureEnvelope(composition, envelope, [mount.exports.effect]);
  bindEffect(replay, mount.exports.effect, {
    execute: async () => {
      calls++;
      return 0;
    },
    failure: () => -1,
  });
  assert.equal(replay.read(mount.exports.value), 42);
  assert.equal(calls, 1);
  binding.dispose();
  replay.dispose();
  runtime.dispose();
});

const oldValue = codec('balance', typia.createAssert<number>(), 1);
const newValue = evolveCodec(codec('balance', typia.createAssert<{ balance: number }>(), 2), oldValue, (balance) => ({
  balance,
}));
const oldChange = codec('change', typia.createAssert<number>(), 1);
const newChange = evolveCodec(codec('change', typia.createAssert<{ add: number }>(), 2), oldChange, (add) => ({ add }));
const oldLibrary = defineLibrary({
  name: 'accounts',
  version: 1,
  requires: [],
  setup(scope) {
    const value = scope.scalar('balance', () => 0, { codec: oldValue });
    const event = scope.event('change', { codec: oldChange });
    scope.reduce(event, (state, by) => state.set(value, state.read(value) + by));
    return { value, event };
  },
});
const nextLibrary = defineLibrary({
  name: 'accounts',
  version: 2,
  previousVersions: [1],
  requires: [],
  setup(scope) {
    const value = scope.scalar('balance', () => ({ balance: 0 }), { codec: newValue });
    const event = scope.event('change', { codec: newChange });
    scope.reduce(event, (state, by) => state.set(value, { balance: state.read(value).balance + by.add }));
    return { value, event };
  },
});
const parseEnvelope = typia.json.createAssertParse<CaptureEnvelope>();

await test('library-owned previous-version migration is deterministic and preserves causal positions and build provenance', () => {
  const oldMount = mountLibrary(oldLibrary, 'accounts');
  const oldOther = mountLibrary(oldLibrary, 'other');
  const before = composeLibraries(oldMount, oldOther);
  const runtime = before.createRuntime({ scheduler: new ManualScheduler() });
  runtime.publish(oldMount.exports.event, 5);
  runtime.flush();
  const recorder = recordRollingScenario(runtime, { maxEvents: 3 });
  for (let index = 1; index <= 10; index++) {
    runtime.publish(oldMount.exports.event, index);
    runtime.publish(oldOther.exports.event, -index);
    runtime.flush();
  }
  const original = createCaptureEnvelope(before, recorder.snapshot(), { buildId: 'v1' });
  const mount = mountLibrary(nextLibrary, 'accounts');
  const other = mountLibrary(nextLibrary, 'other');
  const after = composeLibraries(mount, other);
  const wire = parseEnvelope(canonicalCapture(original));
  const upgrade = () => migrateCaptureEnvelope(after, wire, { buildId: 'v2' });
  const migrated = upgrade();
  assert.equal(canonicalCapture(migrated), canonicalCapture(upgrade()));
  assert.equal(migrated.application.buildId, 'v1');
  assert.equal(migrated.migratedForBuild, 'v2');
  assert.deepEqual(
    migrated.scenario.waves.map((wave) => [wave.wave, wave.events.map((event) => event.admitted)]),
    original.scenario.waves.map((wave) => [wave.wave, wave.events.map((event) => event.admitted)]),
  );
  const replay = replayCaptureEnvelope(after, migrated);
  assert.deepEqual(replay.read(mount.exports.value), { balance: 60 });
  assert.deepEqual(replay.read(other.exports.value), { balance: -55 });
  assert.throws(() => replayCaptureEnvelope(after, wire), CaptureError);
  const future: CaptureEnvelope = {
    ...wire,
    manifest: {
      ...wire.manifest,
      libraries: [{ owner: 'accounts', name: 'accounts', version: 99 }, wire.manifest.libraries[1]],
    },
  };
  assert.throws(
    () => migrateCaptureEnvelope(after, future, { buildId: 'v2' }),
    (error) => error instanceof CaptureError && error.issue.owner === 'accounts' && error.issue.fromVersion === 99,
  );
  assert.throws(() => parseEnvelope('{"kind":"local-replay"}'));
  const broken: RecordedScenario = {
    ...migrated.scenario,
    waves: [...migrated.scenario.waves, migrated.scenario.waves[0]],
  };
  assert.throws(() => replayScenario(after, broken));
  replay.dispose();
  runtime.dispose();
});

await test('replay refuses a mutated admission decision instead of claiming equivalent state', () => {
  const mount = mountLibrary(counter, 'decision');
  const composition = composeLibraries(mount);
  const runtime = composition.createRuntime({ scheduler: new ManualScheduler() });
  const record = recordScenario(runtime);
  runtime.publish(mount.exports.update, { id: 1, amount: 1, requestId: 1 });
  runtime.flush();
  const scenario = record.snapshot();
  const changed: RecordedScenario = {
    ...scenario,
    waves: scenario.waves.map((wave) => ({
      ...wave,
      events: wave.events.map((event) => ({ ...event, admitted: false })),
    })),
  };
  assert.throws(
    () => replayScenario(composition, changed),
    (error) => error instanceof CaptureError && error.issue.code === 'decision',
  );
  runtime.dispose();
});

await test('strict support denies unclassified data, honors consent, and cannot broadly include classified secrets', () => {
  interface Data {
    readonly email: string;
    readonly token: string;
    readonly headers: { cookie: string };
    readonly nested: { password: string; label: string };
  }
  const dataCodec = codec('privacy-data', typia.createAssert<Data>());
  const data: Data = {
    email: 'person@example.invalid',
    token: 'SYNTHETIC_AUTH_TOKEN',
    headers: { cookie: 'SYNTHETIC_COOKIE' },
    nested: { password: 'SYNTHETIC_PASSWORD', label: 'visible' },
  };
  const model = defineLibrary({
    name: 'privacy',
    requires: [],
    setup(scope) {
      const unclassified = scope.scalar('unclassified', () => 'UNCLASSIFIED_DATA', { codec: idCodec });
      const secret = scope.scalar('secret', () => data, {
        codec: dataCodec,
        classify: () => 'secret',
        support: publicSupport((value: Data) => value),
      });
      const consent = scope.scalar('consent', () => data, {
        codec: dataCodec,
        support: consentSupport((value: Data) => value),
      });
      const keyed = scope.keyed('keyed', (_id: string | number) => data, {
        codec: dataCodec,
        idCodec,
        classifyId: (): SupportClassification => 'secret',
        supportId: publicSupport((id: string | number) => id),
        support: publicSupport((value: Data) => value),
      });
      const command = scope.event('command', {
        codec: dataCodec,
        classify: () => 'secret',
        support: publicSupport((value: Data) => value),
      });
      const result = scope.event('result', { codec: dataCodec, support: secretSupport });
      scope.reduce(command, (state, value) => state.setKeyed(keyed, 'SECRET_RESOURCE_ID', { ...value }));
      const effect = defineEffect({
        command,
        result,
        plan: () => ({ requestId: 'private-request' }),
        decode: (_plan, outcome: Data) => outcome,
        codec: codec('privacy-outcome', typia.createAssert<{ plan: { requestId: string }; outcome: Data }>()),
        support: publicSupport((value: { plan: { requestId: string }; outcome: Data }) => value),
      });
      return { unclassified, secret, consent, keyed, command, effect };
    },
  });
  const mount = mountLibrary(model, 'privacy');
  const composition = composeLibraries(mount);
  const runtime = composition.createRuntime({ scheduler: new ManualScheduler() });
  const record = recordRollingScenario(runtime, { maxEvents: 1 });
  runtime.publish(mount.exports.command, data);
  runtime.flush();
  runtime.publish(mount.exports.command, data);
  runtime.flush();
  record.outcomeSink(mount.exports.effect)({ plan: { requestId: 'Bearer SYNTHETIC_REQUEST_TOKEN' }, outcome: data });
  const local = createCaptureEnvelope(composition, record.snapshot(), {
    buildId: 'privacy-build',
    effects: [mount.exports.effect],
    environment: { authorization: 'Bearer SYNTHETIC_ENV_TOKEN' },
  });
  const original = canonicalCapture(local);
  const noConsent = exportSupportCapture(composition, local, { effects: [mount.exports.effect] });
  assert.equal(noConsent.replayable, false);
  const exported = canonicalCapture(noConsent);
  for (const secret of [
    'UNCLASSIFIED_DATA',
    'SECRET_RESOURCE_ID',
    'SYNTHETIC_AUTH_TOKEN',
    'SYNTHETIC_COOKIE',
    'SYNTHETIC_PASSWORD',
    'SYNTHETIC_REQUEST_TOKEN',
    'SYNTHETIC_ENV_TOKEN',
  ])
    assert.ok(!exported.includes(secret), secret);
  const consent = canonicalCapture(
    exportSupportCapture(composition, local, {
      consent: true,
      effects: [mount.exports.effect],
      environment: publicSupport((value: unknown) => value),
    }),
  );
  assert.ok(consent.includes('person@example.invalid'));
  for (const secret of [
    'SYNTHETIC_AUTH_TOKEN',
    'SYNTHETIC_COOKIE',
    'SYNTHETIC_PASSWORD',
    'SECRET_RESOURCE_ID',
    'SYNTHETIC_ENV_TOKEN',
  ])
    assert.ok(!consent.includes(secret), secret);
  assert.ok(consent.includes('redacted'));
  assert.equal(canonicalCapture(local), original, 'Support projection cannot change lossless local replay.');
  assert.throws(
    () => exportSupportCapture(composition, local, { effects: [mount.exports.effect], maxBytes: 1 }),
    sizeFailure,
  );
  runtime.dispose();
});

await test('instruction and outcome codec migrations preserve causal positions across effect journal eviction', async () => {
  interface OldPlan {
    readonly requestId: number;
    readonly requested: number;
  }
  interface NewPlan {
    readonly requestId: number;
    readonly amount: number;
  }
  const oldInstruction = codec('instruction', typia.createAssert<OldPlan>());
  const instruction = evolveCodec(codec('instruction', typia.createAssert<NewPlan>(), 2), oldInstruction, (plan) => ({
    requestId: plan.requestId,
    amount: plan.requested,
  }));
  const oldOutcome = codec('effect-data', typia.createAssert<{ plan: OldPlan; outcome: number }>());
  const outcome = evolveCodec(
    codec('effect-data', typia.createAssert<{ plan: NewPlan; outcome: { value: number } }>(), 2),
    oldOutcome,
    (value) => ({
      plan: { requestId: value.plan.requestId, amount: value.plan.requested },
      outcome: { value: value.outcome },
    }),
  );
  const old = defineLibrary({
    name: 'versioned-operation',
    version: 1,
    requires: [],
    setup(scope) {
      const value = scope.scalar('value', () => 0, { codec: numberCodec });
      const command = scope.command('command', (_state, _value: number) => true, { codec: numberCodec });
      const result = scope.event('result', { codec: numberCodec });
      scope.reduce(result, (state, number) => state.set(value, number));
      const effect = scope.requireEffect(
        defineEffect({
          command,
          result,
          plan: (_state, value) => ({ requestId: value, requested: value }),
          decode: (_plan, value: number) => value,
          codec: oldOutcome,
          instructionCodec: oldInstruction,
        }),
      );
      return { value, command, effect };
    },
  });
  const next = defineLibrary({
    name: 'versioned-operation',
    version: 2,
    previousVersions: [1],
    requires: [],
    setup(scope) {
      const value = scope.scalar('value', () => 0, { codec: numberCodec });
      const command = scope.command('command', (_state, _value: number) => true, { codec: numberCodec });
      const result = scope.event('result', { codec: numberCodec });
      scope.reduce(result, (state, number) => state.set(value, number));
      const effect = scope.requireEffect(
        defineEffect({
          command,
          result,
          plan: (_state, value) => ({ requestId: value, amount: value }),
          decode: (_plan, value: { value: number }) => value.value,
          codec: outcome,
          instructionCodec: instruction,
        }),
      );
      return { value, command, effect };
    },
  });
  const before = mountLibrary(old, 'operation');
  const oldComposition = composeLibraries(before);
  const bus = oldComposition.createRuntime({ scheduler: new ManualScheduler() });
  const recorder = recordRollingScenario(bus, { maxEvents: 3, maxEffectBytes: 1000 });
  bindEffect(bus, before.exports.effect, {
    execute: (plan) => plan.requested,
    failure: () => -1,
    capture: recorder.outcomeSink(before.exports.effect),
    captureInstruction: recorder.instructionSink(before.exports.effect),
  });
  for (let id = 1; id <= 30; id++) {
    bus.publish(before.exports.command, id);
    await bus.drain();
  }
  const captured = createCaptureEnvelope(oldComposition, recorder.snapshot(), { buildId: 'old-operation' });
  assert.ok(captured.evictedEffects > 0);
  assert.ok(captured.captures.some((entry) => entry.capture.kind === 'instruction'));
  const after = mountLibrary(next, 'operation');
  const composition = composeLibraries(after);
  const migrated = migrateCaptureEnvelope(composition, captured, { buildId: 'new-operation' });
  assert.deepEqual(
    migrated.captures.map((entry) => [entry.sequence, entry.afterWave]),
    captured.captures.map((entry) => [entry.sequence, entry.afterWave]),
  );
  for (const { capture } of migrated.captures) {
    if (capture.kind === 'instruction') {
      const plan = decodeEffectInstruction(after.exports.effect, capture);
      assert.equal(plan.amount, plan.requestId);
    } else {
      const decoded = decodeEffectOutcome(after.exports.effect, capture);
      assert.equal(after.exports.effect.decode(decoded.plan, decoded.outcome), decoded.plan.requestId);
      assert.equal(captureEffectOutcome(after.exports.effect, decoded).version, 2);
    }
  }
  const replay = replayCaptureEnvelope(composition, migrated);
  replay.assertReady();
  assert.equal(replay.read(after.exports.value), 30);
  const foreign = mountLibrary(next, 'other');
  const last = migrated.captures.at(-1)?.capture;
  assert.ok(last);
  assert.throws(() => foreign.exports.effect.migrateCapture(last), CaptureError);
  const future: CaptureEnvelope = {
    ...captured,
    captures: [],
    manifest: { ...captured.manifest, effects: captured.manifest.effects.map((entry) => ({ ...entry, version: 99 })) },
  };
  assert.throws(() => migrateCaptureEnvelope(composition, future, { buildId: 'future-refused' }), CaptureError);
  await replay.disposeAsync();
  await bus.disposeAsync();
});

await test('checkpoint growth is refused at its byte bound instead of retaining an unbounded replay interpreter', () => {
  const mount = mountLibrary(counter, 'growing');
  const composition = composeLibraries(mount);
  const errors: unknown[] = [];
  const bus = composition.createRuntime({ scheduler: new ManualScheduler(), onError: (cause) => errors.push(cause) });
  const record = recordRollingScenario(bus, { maxEvents: 1, maxCheckpointBytes: 1600 });
  for (let requestId = 1; requestId <= 100; requestId++) {
    bus.publish(mount.exports.update, { requestId, id: requestId, amount: 1 });
    bus.flush();
  }
  assert.equal(record.stats().refusal?.code, 'size-limit');
  assert.equal(errors.length, 1);
  assert.throws(() => record.snapshot(), sizeFailure);
  bus.dispose();
});
console.log(JSON.stringify({ captureScenarios: passed, generatedStreamRuns: 50, source: 'packed public exports' }));
