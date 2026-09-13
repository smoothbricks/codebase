import assert from 'node:assert/strict';
import {
  CaptureError,
  canonicalCapture,
  captureBytes,
  captureCheckpoint,
  composeLibraries,
  decodeEffectOutcome,
  defineEffect,
  defineLibrary,
  type EffectOutcome,
  ManualScheduler,
  mountLibrary,
  recordRollingScenario,
  replayScenario,
  type RollingScenarioRecorder,
  type ValueCodec,
} from '@smoothbricks/statebus-core';
import typia from 'typia';

interface Write {
  readonly id: string | number;
  readonly value: number;
}
interface Move {
  readonly from: string | number;
  readonly to: string | number;
}
const numberCodec: ValueCodec<number> = {
  schema: 'journal.number',
  version: 1,
  encode: (value) => value,
  decode: typia.createAssert<number>(),
};
const idCodec: ValueCodec<string | number> = {
  schema: 'journal.id',
  version: 1,
  encode: (value) => value,
  decode: typia.createAssert<string | number>(),
};
const definition = defineLibrary({
  name: 'journal-capacity',
  requires: [],
  setup(scope) {
    const values = scope.keyed('values', (_id: string | number) => 0, { codec: numberCodec, idCodec });
    const write = scope.event<Write>('write', {
      codec: {
        schema: 'journal.write',
        version: 1,
        encode: (value) => ({ ...value }),
        decode: typia.createAssert<Write>(),
      },
    });
    const move = scope.event<Move>('move', {
      codec: {
        schema: 'journal.move',
        version: 1,
        encode: (value) => ({ ...value }),
        decode: typia.createAssert<Move>(),
      },
    });
    const fail = scope.event<number>('fail', { codec: numberCodec });
    scope.reduce(write, (state, event) => state.setKeyed(values, event.id, event.value));
    scope.reduce(move, (state, event) => {
      // Intentionally add before removing. Retention must check the atomic final checkpoint.
      state.setKeyed(values, event.to, state.readKeyed(values, event.from));
      state.setKeyed(values, event.from, 0);
    });
    scope.reduce(fail, () => {
      throw new Error('failed capacity wave');
    });
    const command = scope.command<number>('command', () => true, { codec: numberCodec });
    const result = scope.event<number>('result', { codec: numberCodec });
    const effect = defineEffect({
      command,
      result,
      plan: (_state, requestId) => ({ requestId }),
      decode: (_plan, outcome: number) => outcome,
      codec: {
        schema: 'journal.outcome',
        version: 1,
        encode: (value) => ({ plan: { ...value.plan }, outcome: value.outcome }),
        decode: typia.createAssert<EffectOutcome<{ requestId: number }, number>>(),
      },
    });
    return { values, write, move, fail, effect };
  },
});
const mount = mountLibrary(definition, 'capacity');
const composition = composeLibraries(mount);
const model = mount.exports;
let passed = 0;
function test(name: string, run: () => void): void {
  run();
  passed++;
  console.log(`PASS journal capacity: ${name}`);
}
function accounting(recorder: RollingScenarioRecorder): void {
  const snapshot = recorder.snapshot();
  const stats = recorder.stats();
  assert.equal(stats.events, snapshot.waves.reduce((sum, wave) => sum + wave.events.length, 0));
  assert.equal(stats.eventBytes, snapshot.waves.reduce((sum, wave) => sum + captureBytes(wave), 0));
  assert.equal(stats.effects, snapshot.effects.length);
  assert.equal(stats.effectBytes, snapshot.effects.reduce((sum, entry) => sum + captureBytes(entry), 0));
  assert.equal(stats.checkpointBytes, captureBytes(snapshot.checkpoint));
  assert.ok(captureBytes(snapshot) <= recorder.limits.maxCaptureBytes);
  assert.ok(stats.events <= recorder.limits.maxEvents);
  assert.ok(stats.effects <= recorder.limits.maxEffects);
  assert.ok(stats.eventBytes <= recorder.limits.maxEventBytes);
  assert.ok(stats.effectBytes <= recorder.limits.maxEffectBytes);
}

test('add-before-delete relocation fits the final bound across repeated queue wraps', () => {
  const runtime = composition.createRuntime({ scheduler: new ManualScheduler() });
  runtime.publish(model.write, { id: 1, value: 7 });
  runtime.flush();
  const bound = captureBytes(captureCheckpoint(runtime));
  const recorder = recordRollingScenario(runtime, { maxEvents: 3, maxCheckpointBytes: bound });
  try {
    for (let index = 0; index < 1000; index++) {
      runtime.publish(model.move, { from: (index % 2) + 1, to: ((index + 1) % 2) + 1 });
      runtime.flush();
      if (index % 67 === 0 || index === 999) {
        accounting(recorder);
        assert.equal(recorder.stats().checkpointBytes, bound);
        const replay = replayScenario(composition, recorder.snapshot());
        try {
          assert.deepEqual(captureCheckpoint(replay), captureCheckpoint(runtime));
        } finally {
          replay.dispose();
        }
      }
    }
    assert.equal(recorder.stats().evictedWaves, 997);
  } finally {
    recorder.dispose();
    runtime.dispose();
  }
});

test('capacity-one wrap, failed waves and reset preserve independently owned snapshots', () => {
  const runtime = composition.createRuntime({ scheduler: new ManualScheduler() });
  const recorder = recordRollingScenario(runtime, { maxEvents: 1, maxEffects: 1 });
  try {
    runtime.publish(model.write, { id: 7, value: 10 });
    runtime.flush();
    const retained = recorder.snapshot();
    const before = canonicalCapture(retained);
    for (let index = 1; index <= 700; index++) {
      runtime.publish(model.write, { id: '7', value: index });
      runtime.flush();
    }
    const committed = canonicalCapture(recorder.snapshot());
    runtime.publish(model.write, { id: 7, value: 99 });
    runtime.publish(model.fail, 0);
    assert.throws(() => runtime.flush(), /failed capacity wave/);
    assert.equal(canonicalCapture(recorder.snapshot()), committed);
    assert.equal(runtime.readKeyed(model.values, 7), 10);
    assert.equal(runtime.readKeyed(model.values, '7'), 700);
    recorder.reset();
    accounting(recorder);
    assert.equal(recorder.snapshot().waves.length, 0);
    for (let index = 1; index <= 70; index++) {
      runtime.publish(model.write, { id: 7, value: index });
      runtime.flush();
    }
    accounting(recorder);
    assert.equal(canonicalCapture(retained), before);
    const replay = replayScenario(composition, recorder.snapshot());
    try {
      assert.deepEqual(captureCheckpoint(replay), captureCheckpoint(runtime));
    } finally {
      replay.dispose();
    }
  } finally {
    recorder.dispose();
    runtime.dispose();
  }
});

test('instruction/outcome history obeys independent count and byte limits without aliasing old snapshots', () => {
  for (const budget of [
    { maxEffects: 3, maxEffectBytes: 1_000_000 },
    { maxEffects: 100, maxEffectBytes: 700 },
  ]) {
    const runtime = composition.createRuntime({ scheduler: new ManualScheduler() });
    const recorder = recordRollingScenario(runtime, budget);
    const capture = recorder.outcomeSink(model.effect);
    try {
      capture({ plan: { requestId: 0 }, outcome: 0 });
      const retained = recorder.snapshot();
      const before = canonicalCapture(retained);
      for (let index = 1; index <= 1200; index++) {
        const payload = { plan: { requestId: index }, outcome: index };
        capture(payload);
        payload.plan.requestId = -1;
        if (index % 97 === 0) accounting(recorder);
      }
      accounting(recorder);
      const snapshot = recorder.snapshot();
      assert.ok(snapshot.evictedEffects > 1000);
      assert.equal(snapshot.effects.length + snapshot.evictedEffects, 1201);
      if (budget.maxEffects === 3) assert.equal(snapshot.effects.length, 3);
      let previous = 0;
      for (const entry of snapshot.effects) {
        assert.ok(entry.sequence > previous);
        previous = entry.sequence;
        assert.equal(entry.capture.kind, 'outcome');
        if (entry.capture.kind !== 'outcome') throw new Error('Unexpected instruction.');
        const outcome = decodeEffectOutcome(model.effect, entry.capture);
        assert.equal(outcome.plan.requestId, entry.sequence - 1);
        assert.equal(outcome.outcome, entry.sequence - 1);
      }
      assert.equal(canonicalCapture(retained), before);
    } finally {
      recorder.dispose();
      runtime.dispose();
    }
  }
});

test('oversized checkpoints clear retained payloads and do not cancel production writes', () => {
  const errors: unknown[] = [];
  const runtime = composition.createRuntime({ scheduler: new ManualScheduler(), onError: (cause) => errors.push(cause) });
  runtime.publish(model.write, { id: 1, value: 7 });
  runtime.flush();
  const bound = captureBytes(captureCheckpoint(runtime));
  const recorder = recordRollingScenario(runtime, { maxEvents: 1, maxCheckpointBytes: bound });
  try {
    const retained = recorder.snapshot();
    const before = canonicalCapture(retained);
    recorder.outcomeSink(model.effect)({ plan: { requestId: 1 }, outcome: 1 });
    runtime.publish(model.write, { id: 2, value: 8 });
    runtime.flush();
    runtime.publish(model.write, { id: 3, value: 9 });
    runtime.flush();
    assert.throws(() => recorder.snapshot(), (error) => error instanceof CaptureError && error.issue.code === 'size-limit');
    const stats = recorder.stats();
    assert.equal(stats.events, 0);
    assert.equal(stats.effects, 0);
    assert.equal(stats.eventBytes, 0);
    assert.equal(stats.effectBytes, 0);
    assert.equal(stats.checkpointBytes, 0);
    assert.equal(errors.length, 1);
    assert.equal(runtime.readKeyed(model.values, 2), 8);
    assert.equal(runtime.readKeyed(model.values, 3), 9);
    assert.equal(canonicalCapture(retained), before);
  } finally {
    recorder.dispose();
    runtime.dispose();
  }
});

test('exact cold-envelope limit is checked before allocating a checkpoint snapshot', () => {
  const runtime = composition.createRuntime({ scheduler: new ManualScheduler() });
  const reference = recordRollingScenario(runtime);
  const limit = captureBytes(reference.snapshot());
  reference.dispose();
  const recorder = recordRollingScenario(runtime, { maxCaptureBytes: limit - 1 });
  try {
    const before = recorder.stats().checkpointMaterializations;
    assert.throws(() => recorder.snapshot(), (error) => error instanceof CaptureError && error.issue.code === 'size-limit');
    assert.equal(recorder.stats().checkpointMaterializations, before);
    assert.equal(recorder.stats().refusal, undefined, 'An export refusal must not stop local recording.');
    const exact = recordRollingScenario(runtime, { maxCaptureBytes: limit });
    try {
      assert.equal(captureBytes(exact.snapshot()), limit);
      accounting(exact);
    } finally {
      exact.dispose();
    }
  } finally {
    recorder.dispose();
    runtime.dispose();
  }
});

console.log(JSON.stringify({ journalCapacityScenarios: passed, measuredAllocationBytes: false, builtExports: true }));
