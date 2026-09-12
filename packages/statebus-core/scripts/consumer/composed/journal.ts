import assert from 'node:assert/strict';
import { Err, Ok } from '@smoothbricks/lmao';
import {
  bindEffect,
  captureCheckpoint,
  composeLibraries,
  decodeEffectOutcome,
  defineEffect,
  defineLibrary,
  type EffectOutcome,
  type JournalCapture,
  type JournalLimits,
  type JournalRecorder,
  type JournalSnapshot,
  ManualScheduler,
  mountLibrary,
  recordJournal,
  recordScenario,
  replayScenario,
  type ValueCodec,
} from '@smoothbricks/statebus-core';
import fc from 'fast-check';
import typia from 'typia';
import { requestId, shelfId } from './codecs.js';
import { canAdjust, inventoryLibrary } from './library.js';

// Codecs come from the actual contracts and are generated in the external consumer by ttsc.
declare const idBrand: unique symbol;
type CellId = (string | number) & { readonly [idBrand]: true };
interface Change {
  readonly id: CellId;
  readonly requestId: number;
  readonly add: number;
  readonly remote: boolean;
  readonly note: string;
}
interface Plan {
  readonly id: CellId;
  readonly requestId: number;
}
interface Result {
  readonly id: CellId;
  readonly requestId: number;
  readonly value: number;
}
const idCodec: ValueCodec<CellId> = {
  schema: 'journal.id',
  version: 1,
  encode: (value) => value,
  decode: typia.createAssert<CellId>(),
};
function codec<T>(schema: string, decode: (value: unknown) => T): ValueCodec<T> {
  return { schema, version: 1, encode: (value) => structuredClone(value), decode };
}
const numberCodec = codec('journal.number', typia.createAssert<number>());
const changeCodec = codec('journal.change', typia.createAssert<Change>());
const resultCodec = codec('journal.result', typia.createAssert<Result>());
const outcomeCodec = codec('journal.outcome', typia.createAssert<EffectOutcome<Plan, number>>());
const parseCapture = typia.json.createAssertParse<JournalCapture>();
let noiseEncodes = 0;
const noiseCodec: ValueCodec<number> = {
  ...numberCodec,
  encode: (value) => {
    noiseEncodes++;
    return value;
  },
};
const library = defineLibrary({
  name: 'journal-test',
  requires: [],
  setup(scope) {
    const values = scope.keyed('values', (_id: CellId) => 0, { idCodec, codec: numberCodec });
    const latest = scope.keyed('latest', (_id: CellId) => 0, { idCodec, codec: numberCodec });
    const pending = scope.keyed('pending', (_id: CellId) => 0, { idCodec, codec: numberCodec });
    const noise = scope.keyed('noise', (_id: CellId) => 0, { idCodec, codec: noiseCodec });
    const accepted = scope.scalar('accepted', () => 0, { codec: numberCodec });
    const refused = scope.scalar('refused', () => 0, { codec: numberCodec });
    const seed = scope.event<Result>('seed', { codec: resultCodec });
    scope.reduce(seed, (state, payload) => state.setKeyed(noise, payload.id, payload.value));
    const changed = scope.command<Change>(
      'change',
      (state, command) => {
        if (command.requestId <= state.readKeyed(latest, command.id)) {
          state.set(refused, state.read(refused) + 1);
          return false;
        }
        state.set(accepted, state.read(accepted) + 1);
        state.setKeyed(latest, command.id, command.requestId);
        // Zero is a reset so checkpoint deletion/default restoration participates in generated streams.
        state.setKeyed(values, command.id, command.add === 0 ? 0 : state.readKeyed(values, command.id) + command.add);
        if (command.remote) state.setKeyed(pending, command.id, command.requestId);
        return true;
      },
      { codec: changeCodec },
    );
    const completed = scope.event<Result>('completed', { codec: resultCodec });
    scope.reduce(completed, (state, result) => {
      if (state.readKeyed(pending, result.id) !== result.requestId) return;
      state.setKeyed(values, result.id, result.value);
      state.setKeyed(pending, result.id, 0);
    });
    const fail = scope.event<number>('fail', { codec: numberCodec });
    scope.reduce(fail, () => {
      throw new Error('failed journal wave');
    });
    const effect = defineEffect({
      command: changed,
      result: completed,
      codec: outcomeCodec,
      plan: (_state, change): Plan | undefined =>
        change.remote ? { id: change.id, requestId: change.requestId } : undefined,
      decode: (plan: Plan, value: number): Result => ({ ...plan, value }),
    });
    return { values, latest, pending, noise, seed, accepted, refused, changed, completed, fail, effect };
  },
});
const left = mountLibrary(library, 'left');
const right = mountLibrary(library, 'right');
const composition = composeLibraries(left, right);
const id = (value: string | number) => idCodec.decode(value);
const limits: JournalLimits = {
  maxEvents: 6,
  maxEventBytes: 2400,
  maxOutcomes: 4,
  maxOutcomeBytes: 1800,
  maxCheckpointBytes: 256_000,
  maxCaptureBytes: 280_000,
};
const createRuntime = () => composition.createRuntime({ scheduler: new ManualScheduler() });
const bytes = (value: unknown) => new TextEncoder().encode(JSON.stringify(value)).byteLength;
function recorded(journal: JournalRecorder): Extract<JournalSnapshot, { kind: 'recorded' }> {
  const snapshot = journal.snapshot();
  assert.equal(snapshot.kind, 'recorded', JSON.stringify(snapshot));
  if (snapshot.kind !== 'recorded') throw new Error('Expected usable journal.');
  const { usage, capture } = snapshot;
  assert.equal(usage.eventBytes, bytes(capture.scenario.waves));
  assert.equal(usage.outcomeBytes, bytes(capture.outcomes));
  assert.equal(usage.checkpointBytes, bytes(capture.scenario.checkpoint));
  assert.equal(usage.captureBytes, bytes(capture));
  assert.equal(
    usage.events,
    capture.scenario.waves.reduce((sum, wave) => sum + wave.events.length, 0),
  );
  assert.ok(usage.events <= journal.limits.maxEvents);
  assert.ok(usage.eventBytes <= journal.limits.maxEventBytes);
  assert.ok(usage.outcomes <= journal.limits.maxOutcomes);
  assert.ok(usage.outcomeBytes <= journal.limits.maxOutcomeBytes);
  assert.ok(usage.checkpointBytes <= journal.limits.maxCheckpointBytes);
  assert.ok(usage.captureBytes <= journal.limits.maxCaptureBytes);
  assert.equal(capture.scenario.complete, true);
  for (const wave of capture.scenario.waves) assert.ok(wave.wave > capture.checkpointWave);
  return snapshot;
}
function command(index: number, cell: CellId = id(7), add = 1, note = '', remote = false): Change {
  return { id: cell, requestId: index, add, note, remote };
}
const passed: string[] = [];
async function test(name: string, run: () => void | Promise<void>): Promise<void> {
  await run();
  passed.push(name);
  console.log(`PASS ${name}`);
}

await test('rolling checkpoints retain state and admission decisions beyond count and byte limits', () => {
  const generated = fc.array(
    fc.record({
      cell: fc.constantFrom(7, '7', 0, -3, 'shelf', '😀'),
      add: fc.integer({ min: -3, max: 3 }),
      note: fc.string({ maxLength: 50 }),
      failed: fc.boolean(),
      owner: fc.boolean(),
    }),
    { minLength: 180, maxLength: 260 },
  );
  for (const budget of [
    { maxEvents: 4, maxEventBytes: 10_000 },
    { maxEvents: 100, maxEventBytes: 1400 },
  ]) {
    fc.assert(
      fc.property(generated, (actions) => {
        const runtime = createRuntime();
        const journal = recordJournal(runtime, { ...limits, ...budget });
        let first: ReturnType<typeof recorded> | undefined;
        let firstText = '';
        try {
          for (let index = 0; index < actions.length; index++) {
            const action = actions[index];
            const model = action.owner ? left.exports : right.exports;
            const value = command(index + 1, id(action.cell), action.add, action.note);
            const before = runtime.read(model.accepted);
            runtime.publish(model.changed, value);
            runtime.publish(model.changed, value);
            if (action.failed) {
              runtime.publish(model.fail, 0);
              assert.throws(() => runtime.flush(), /failed journal wave/);
              assert.equal(runtime.read(model.accepted), before);
            } else runtime.flush();
            if (index % 29 === 0 || index === actions.length - 1) {
              const current = recorded(journal);
              const transported = parseCapture(JSON.stringify(current.capture));
              const replay = replayScenario(composition, transported.scenario);
              try {
                assert.deepEqual(captureCheckpoint(replay), captureCheckpoint(runtime));
              } finally {
                replay.dispose();
              }
              if (!first) {
                first = current;
                firstText = JSON.stringify(first);
              }
            }
          }
          assert.ok(
            recorded(journal).capture.checkpointWave > 0,
            'Must actually exceed retention, not just record a short scenario.',
          );
          assert.equal(JSON.stringify(first), firstText, 'Recycled storage cannot corrupt a retained snapshot.');
        } finally {
          journal.dispose();
          runtime.dispose();
        }
      }),
      { seed: 261203, numRuns: 60 },
    );
  }
});
await test('canonical keyed checkpoints separate numeric/string IDs and ignore insertion order', () => {
  const a = createRuntime();
  const b = createRuntime();
  const keys = [id(7), id('7'), id(-3), id('03'), id(0), id('😀')];
  try {
    for (const key of keys) {
      a.publish(left.exports.changed, command(1, key));
      a.flush();
    }
    for (const key of [...keys].reverse()) {
      b.publish(left.exports.changed, command(1, key));
      b.flush();
    }
    assert.deepEqual(captureCheckpoint(a), captureCheckpoint(b));
    const ja = recordJournal(a, limits);
    const jb = recordJournal(b, limits);
    assert.deepEqual(recorded(ja).capture.scenario.checkpoint, recorded(jb).capture.scenario.checkpoint);
    const values = recorded(ja).capture.scenario.checkpoint.states.find(
      (state) => state.key === left.exports.values.metadata.key,
    );
    assert.deepEqual(
      values?.entries.map((entry) => entry.id?.value),
      [-3, 0, 7, '03', '7', '😀'],
    );
    assert.equal(a.readKeyed(left.exports.values, id(7)), 1);
    assert.equal(a.readKeyed(left.exports.values, id('7')), 1);
    ja.dispose();
    jb.dispose();
  } finally {
    a.dispose();
    b.dispose();
  }
});
await test('outcomes spanning eviction use their original plan and decoder without replay I/O', async () => {
  const runtime = createRuntime();
  const journal = recordJournal(runtime, { ...limits, maxEvents: 2 });
  const pending = Promise.withResolvers<number>();
  let operations = 0;
  bindEffect(runtime, left.exports.effect, {
    execute: async () => {
      operations++;
      return pending.promise;
    },
    failure: () => -1,
    capture: (outcome) => {
      assert.equal(journal.captureOutcome(left.exports.effect, outcome).kind, 'captured');
    },
  });
  try {
    runtime.publish(left.exports.changed, command(1, id(7), 1, '', true));
    runtime.flush();
    for (let index = 2; index < 140; index++) {
      runtime.publish(right.exports.changed, command(index, id('7')));
      runtime.flush();
    }
    assert.ok(recorded(journal).capture.checkpointWave > 1);
    assert.equal(runtime.readKeyed(left.exports.pending, id(7)), 1);
    pending.resolve(77);
    for (let index = 0; index < 8; index++) {
      await Promise.resolve();
      runtime.flush();
    }
    const capture = parseCapture(JSON.stringify(recorded(journal).capture));
    assert.equal(capture.outcomes.length, 1);
    const outcome = decodeEffectOutcome(left.exports.effect, capture.outcomes[0].captured);
    assert.equal(outcome.plan.requestId, 1);
    assert.equal(outcome.plan.id, 7);
    assert.deepEqual(left.exports.effect.decode(outcome.plan, outcome.outcome), { id: 7, requestId: 1, value: 77 });
    assert.throws(() => decodeEffectOutcome(right.exports.effect, capture.outcomes[0].captured), /Incompatible/);
    const replay = replayScenario(composition, capture.scenario);
    bindEffect(replay, left.exports.effect, {
      execute: async () => {
        operations++;
        return 999;
      },
      failure: () => -1,
    });
    assert.deepEqual(captureCheckpoint(replay), captureCheckpoint(runtime));
    assert.equal(replay.readKeyed(left.exports.values, id(7)), 77);
    assert.equal(operations, 1);
    replay.dispose();
  } finally {
    journal.dispose();
    runtime.dispose();
  }
});
await test('outcome count/byte retention is independent and explicitly reports dropped history', () => {
  for (const budget of [
    { maxOutcomes: 2, maxOutcomeBytes: 5000 },
    { maxOutcomes: 100, maxOutcomeBytes: 650 },
  ]) {
    const runtime = createRuntime();
    const journal = recordJournal(runtime, { ...limits, ...budget });
    let first: ReturnType<typeof recorded> | undefined;
    let firstText = '';
    try {
      for (let index = 1; index <= 200; index++) {
        const plan = { id: id(7), requestId: index };
        const receipt = journal.captureOutcome(left.exports.effect, { plan, outcome: index });
        assert.equal(receipt.kind, 'captured');
        plan.requestId = 9000; // The capture cannot retain caller-owned plan storage.
        const current = recorded(journal);
        const last = current.capture.outcomes.at(-1);
        assert.ok(last);
        assert.equal(decodeEffectOutcome(left.exports.effect, last.captured).plan.requestId, index);
        if (!first) {
          first = current;
          firstText = JSON.stringify(first);
        }
      }
      const current = recorded(journal);
      assert.equal(current.capture.outcomesDropped + current.capture.outcomes.length, 200);
      assert.ok(current.capture.outcomesDropped > 0);
      assert.equal(current.usage.events, 0, 'Outcomes alone are not secretly replayed as result events.');
      assert.equal(JSON.stringify(first), firstText);
    } finally {
      journal.dispose();
      runtime.dispose();
    }
  }
});
await test('oversized initial checkpoints, whole waves, outcomes and cold exports are explicit refusals', () => {
  for (const budget of [
    { maxCheckpointBytes: 1, code: 'checkpoint-limit' },
    { maxEvents: 1, code: 'wave-limit' },
    { maxEventBytes: 100, code: 'wave-limit' },
    { maxOutcomeBytes: 100, code: 'outcome-limit' },
    { maxCaptureBytes: 1, code: 'capture-limit' },
  ]) {
    const runtime = createRuntime();
    const journal = recordJournal(runtime, { ...limits, ...budget });
    try {
      if (budget.code === 'wave-limit') {
        runtime.publish(left.exports.changed, command(1));
        runtime.publish(left.exports.changed, command(1));
        runtime.flush();
        assert.equal(runtime.read(left.exports.accepted), 1);
        assert.equal(runtime.read(left.exports.refused), 1);
      }
      if (budget.code === 'outcome-limit')
        journal.captureOutcome(left.exports.effect, { plan: { id: id(7), requestId: 1 }, outcome: 5 });
      const snapshot = journal.snapshot();
      assert.equal(snapshot.kind, 'refused');
      if (snapshot.kind === 'refused') assert.equal(snapshot.reason.code, budget.code);
    } finally {
      journal.dispose();
      runtime.dispose();
    }
  }
});
await test('checkpoint growth can refuse retention without discarding a wave and claiming complete replay', () => {
  const runtime = createRuntime();
  const initialBytes = bytes(captureCheckpoint(runtime));
  const journal = recordJournal(runtime, { ...limits, maxEvents: 1, maxCheckpointBytes: initialBytes + 50 });
  try {
    runtime.publish(left.exports.changed, command(1));
    runtime.flush();
    assert.equal(journal.snapshot().kind, 'recorded');
    runtime.publish(left.exports.changed, command(2));
    runtime.flush();
    const snapshot = journal.snapshot();
    assert.equal(snapshot.kind, 'refused');
    if (snapshot.kind === 'refused') {
      assert.equal(snapshot.reason.code, 'checkpoint-limit');
    }
    assert.equal(runtime.readKeyed(left.exports.values, id(7)), 2, 'Capture cannot cancel production state changes.');
  } finally {
    journal.dispose();
    runtime.dispose();
  }
});
await test('rolling eviction encodes touched checkpoint cells, not the entire application', () => {
  const runtime = createRuntime();
  noiseEncodes = 0;
  for (let index = 0; index < 1500; index++)
    runtime.publish(left.exports.seed, { id: id(index), requestId: 0, value: index + 1 });
  runtime.flush();
  assert.equal(noiseEncodes, 0, 'Without a recorder live publication invokes no capture codecs.');
  const journal = recordJournal(runtime, {
    ...limits,
    maxEvents: 1,
    maxCheckpointBytes: 1_000_000,
    maxCaptureBytes: 1_020_000,
  });
  const initialEncodes = noiseEncodes;
  assert.equal(initialEncodes, 1500);
  try {
    for (let index = 1; index <= 1200; index++) {
      runtime.publish(left.exports.changed, command(index));
      runtime.flush();
    }
    const snapshot = recorded(journal);
    assert.ok(snapshot.capture.checkpointWave >= 1200);
    assert.equal(
      noiseEncodes,
      initialEncodes,
      'Unchanged keyed cells must not be re-encoded on checkpoint advancement or export.',
    );
    assert.ok(Object.isFrozen(snapshot.capture));
    assert.ok(Object.isFrozen(snapshot.capture.scenario.waves));
    const event = snapshot.capture.scenario.waves[0]?.events[0];
    assert.ok(event);
    assert.ok(Object.isFrozen(event.payload.value));
    assert.equal(Reflect.set(event, 'key', 'corrupt'), false);
    const replay = replayScenario(composition, snapshot.capture.scenario);
    assert.equal(replay.read(left.exports.accepted), 1200);
    replay.dispose();
    console.log(
      JSON.stringify({
        diagnostic: 'capture-work-counts',
        seededCells: 1500,
        evictionWaves: 1199,
        initialCellEncodes: initialEncodes,
        unchangedCellReencodes: noiseEncodes - initialEncodes,
        allocationBytesMeasured: false,
      }),
    );
  } finally {
    journal.dispose();
    runtime.dispose();
  }
});
await test('disposal releases capture; finite-scenario eviction remains honestly incomplete', () => {
  const runtime = createRuntime();
  const journal = recordJournal(runtime, limits);
  const finite = recordScenario(runtime, { maxEvents: 1 });
  runtime.publish(left.exports.changed, command(1));
  runtime.flush();
  journal.dispose();
  const before = JSON.stringify(journal.snapshot());
  runtime.publish(left.exports.changed, command(2));
  runtime.flush();
  assert.equal(JSON.stringify(journal.snapshot()), before);
  const receipt = journal.captureOutcome(left.exports.effect, { plan: { id: id(7), requestId: 1 }, outcome: 5 });
  assert.equal(receipt.kind, 'refused');
  if (receipt.kind === 'refused') assert.equal(receipt.reason.code, 'disposed');
  assert.equal(finite.snapshot().complete, false);
  assert.throws(() => replayScenario(composition, finite.snapshot()), /Incomplete/);
  finite.dispose();
  runtime.dispose();
  journal.dispose();
});
await test('atomic relocation respects the final checkpoint bound rather than an intermediate prefix', () => {
  const moves = defineLibrary({
    name: 'moves',
    requires: [],
    setup(scope) {
      const values = scope.keyed('values', (_id: CellId) => 0, { idCodec, codec: numberCodec });
      const put = scope.event<Result>('put', { codec: resultCodec });
      const move = scope.event<Result>('move', { codec: resultCodec });
      scope.reduce(put, (state, event) => state.setKeyed(values, event.id, event.value));
      scope.reduce(move, (state, event) => {
        state.setKeyed(values, event.id, state.readKeyed(values, id(event.requestId)));
        state.setKeyed(values, id(event.requestId), 0);
      });
      return { values, put, move };
    },
  });
  const mount = mountLibrary(moves, 'move');
  const app = composeLibraries(mount);
  const runtime = app.createRuntime({ scheduler: new ManualScheduler() });
  runtime.publish(mount.exports.put, { id: id(1), requestId: 0, value: 1 });
  runtime.flush();
  const bound = bytes(captureCheckpoint(runtime));
  const journal = recordJournal(runtime, { ...limits, maxEvents: 1, maxCheckpointBytes: bound });
  try {
    for (let index = 0; index < 90; index++) {
      runtime.publish(mount.exports.move, { requestId: (index % 3) + 1, id: id(((index + 1) % 3) + 1), value: 0 });
      runtime.flush();
      const snapshot = recorded(journal);
      assert.equal(snapshot.usage.checkpointBytes, bound);
      const replay = replayScenario(app, snapshot.capture.scenario);
      assert.deepEqual(captureCheckpoint(replay), captureCheckpoint(runtime));
      replay.dispose();
    }
  } finally {
    journal.dispose();
    runtime.dispose();
  }
});
await test('journal outcomes use the same built LMAO result codecs and pure decoder', () => {
  const mount = mountLibrary(inventoryLibrary, 'journal-inventory', [canAdjust.provide(true)]);
  const app = composeLibraries(mount);
  const runtime = app.createRuntime({ scheduler: new ManualScheduler() });
  const journal = recordJournal(runtime, limits);
  try {
    const plan = { shelfId: shelfId('shelf:journal'), requestId: requestId('request:journal'), next: 5 };
    for (const outcome of [new Ok(5), new Err('synthetic failure')]) {
      assert.equal(journal.captureOutcome(mount.exports.effect, { plan, outcome }).kind, 'captured');
      const stored = recorded(journal).capture.outcomes.at(-1);
      assert.ok(stored);
      const decoded = decodeEffectOutcome(mount.exports.effect, stored.captured);
      assert.deepEqual(
        mount.exports.effect.decode(decoded.plan, decoded.outcome),
        mount.exports.effect.decode(plan, outcome),
      );
    }
  } finally {
    journal.dispose();
    runtime.dispose();
  }
});
await test('capture codec failure cannot suppress an admitted operation or insert raw exceptions into a journal', async () => {
  const definition = defineLibrary({
    name: 'capture-failure',
    requires: [],
    setup(scope) {
      const value = scope.scalar('value', () => 0, { codec: numberCodec });
      const command = scope.command<number>('command', () => true, {
        codec: {
          ...numberCodec,
          encode() {
            throw new Error('synthetic private codec detail');
          },
        },
      });
      const result = scope.event<number>('result', { codec: numberCodec });
      scope.reduce(result, (state, outcome) => state.set(value, outcome));
      return {
        value,
        command,
        effect: defineEffect({
          command,
          result,
          plan: (_state, requestId) => ({ requestId }),
          decode: (_plan, outcome: number) => outcome,
        }),
      };
    },
  });
  const mount = mountLibrary(definition, 'broken-capture');
  const app = composeLibraries(mount);
  const errors: unknown[] = [];
  let executions = 0;
  const runtime = app.createRuntime({ scheduler: new ManualScheduler(), onError: (cause) => errors.push(cause) });
  const journal = recordJournal(runtime, limits);
  bindEffect(runtime, mount.exports.effect, {
    execute: async () => {
      executions++;
      return 42;
    },
    failure: () => -1,
  });
  try {
    runtime.publish(mount.exports.command, 1);
    for (let index = 0; index < 6; index++) {
      await Promise.resolve();
      runtime.flush();
    }
    assert.equal(executions, 1);
    assert.equal(runtime.read(mount.exports.value), 42);
    assert.equal(errors.length, 1);
    const snapshot = journal.snapshot();
    assert.equal(snapshot.kind, 'refused');
    if (snapshot.kind === 'refused') assert.equal(snapshot.reason.code, 'codec');
    assert.ok(!JSON.stringify(snapshot).includes('private codec detail'));
  } finally {
    journal.dispose();
    runtime.dispose();
  }
});
console.log(JSON.stringify({ passed: passed.length, tests: passed, builtExports: true }));
