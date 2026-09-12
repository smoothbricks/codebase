import assert from 'node:assert/strict';
import {
  type ArchiveResult,
  bindEffect,
  captureCheckpoint,
  composeLibraries,
  createReplayArchive,
  createSupportExporter,
  defineEffect,
  defineLibrary,
  defineLibraryHistory,
  type EffectOutcome,
  type LibraryMigration,
  ManualScheduler,
  mountLibrary,
  recordArchiveOutcome,
  recordScenario,
  type ReplayEnvelope,
  supportEffect,
  supportEvent,
  supportKeyed,
  supportScalar,
  type ValueCodec,
} from '@smoothbricks/statebus-core';
import typia from 'typia';

declare const cellBrand: unique symbol;
type CellId = (string | number) & { readonly [cellBrand]: true };
interface Value { readonly total: number; readonly secret: string; readonly future?: string }
interface Change { readonly id: CellId; readonly add: number; readonly requestId: string }
interface Plan { readonly requestId: string }
const assertId = typia.createAssert<CellId>();
const assertValue = typia.createAssert<Value>();
const assertChange = typia.createAssert<Change>();
const assertOutcome = typia.createAssert<EffectOutcome<Plan, number>>();
const parseEnvelope = typia.json.createAssertParse<ReplayEnvelope>();
const number = typia.createAssert<number>();
function codec<T>(schema: string, version: number, decode: (value: unknown) => T): ValueCodec<T> {
  return { schema, version, encode: (value) => structuredClone(value), decode };
}
const idCodec = codec('archive.id', 1, assertId);
const provenance = {
  build: { application: 'fixture', revision: 'private-build-revision' },
  environment: { runtime: 'private-runtime-detail', platform: 'private-platform-detail' },
};
const safeProvenance = {
  build: { application: 'fixture', revision: 'test' },
  environment: { runtime: 'javascript', platform: 'test' },
};
function library(version: number) {
  const valueCodec = codec('archive.value', version, assertValue);
  const changeCodec = codec('archive.change', version, assertChange);
  const outcomeCodec = codec('archive.outcome', version, assertOutcome);
  const numberCodec = codec('archive.number', version, number);
  const definition = defineLibrary({
    name: 'archive-fixture',
    requires: [],
    setup(scope) {
      const initial = (): Value => ({ total: 0, secret: 'private-state-secret', future: 'private-added-field' });
      const count = scope.scalar(version === 1 ? 'count' : 'quantity', initial, { codec: valueCodec });
      const values = scope.keyed('values', (_id: CellId) => initial(), { codec: valueCodec, idCodec });
      const changed = scope.command<Change>(version === 1 ? 'added' : 'incremented', () => true, { codec: changeCodec });
      scope.reduce(changed, (state, change) => {
        state.set(count, { ...state.read(count), total: state.read(count).total + change.add });
        const before = state.readKeyed(values, change.id);
        state.setKeyed(values, change.id, { ...before, total: before.total + change.add });
      });
      const completed = scope.event<number>('completed', { codec: numberCodec });
      const effect = defineEffect({
        command: changed,
        result: completed,
        codec: outcomeCodec,
        plan: (_state, change): Plan => ({ requestId: change.requestId }),
        decode: (_plan, outcome: number) => outcome,
      });
      return { count, values, changed, effect };
    },
  });
  return { definition, valueCodec, changeCodec };
}
const old = library(1);
const next = library(3);
function upgrade(from: number): LibraryMigration {
  return {
    from,
    migrate(source, context) {
      const keys = new Map(source.schema.map((entry) => {
        const name = entry.name === 'count' ? 'quantity' : entry.name === 'added' ? 'incremented' : entry.name;
        const target = context.targetSchema.find((candidate) => candidate.name === name);
        assert.ok(target);
        assert.equal(entry.version, context.from);
        return [entry.key, target.key];
      }));
      const key = (before: string): string => {
        const after = keys.get(before);
        assert.ok(after);
        return after;
      };
      const command = source.schema.find((entry) => entry.kind === 'command');
      const result = source.schema.find((entry) => entry.name === 'completed');
      assert.ok(command);
      assert.ok(result);
      return {
        schema: context.targetSchema.map((entry) => ({ ...entry, version: context.to })),
        states: source.states.map((state) => ({
          key: key(state.key),
          entries: state.entries.map((entry) => ({
            ...entry,
            value: { ...entry.value, version: context.to, value: { ...assertValue(entry.value.value), total: assertValue(entry.value.value).total * 10 } },
          })),
        })),
        events: source.events.map((event) => ({
          key: key(event.key),
          payload: {
            ...event.payload,
            version: context.to,
            value: event.key === command.key
              ? { ...assertChange(event.payload.value), add: assertChange(event.payload.value).add * 10 }
              : number(event.payload.value) * 10,
          },
        })),
        outcomes: source.outcomes.map((outcome) => ({
          effect: `${key(command.key)}/${key(result.key)}`,
          schema: outcome.schema,
          version: context.to,
          value: { ...assertOutcome(outcome.value), outcome: assertOutcome(outcome.value).outcome * 10 },
        })),
      };
    },
  };
}
const oldHistory = defineLibraryHistory(old.definition, { version: 1 });
const nextHistory = defineLibraryHistory(next.definition, { version: 3, migrations: [upgrade(1), upgrade(2)] });
const left = mountLibrary(old.definition, 'private-left-owner');
const right = mountLibrary(old.definition, 'private-right-owner');
const app = composeLibraries(left, right);
const newLeft = mountLibrary(next.definition, left.owner);
const newRight = mountLibrary(next.definition, right.owner);
const nextApp = composeLibraries(newRight, newLeft);
const archive = createReplayArchive(app, [oldHistory], { effects: [left.exports.effect, right.exports.effect] });
const nextArchive = createReplayArchive(nextApp, [nextHistory], { effects: [newLeft.exports.effect, newRight.exports.effect] });
function ok<T>(result: ArchiveResult<T>): T {
  assert.equal(result.kind, 'ok', JSON.stringify(result));
  if (result.kind !== 'ok') throw new Error('Expected archive success.');
  return result.value;
}
const runtime = app.createRuntime({ scheduler: new ManualScheduler() });
const publish = (index: number) => {
  const model = index % 2 === 0 ? left.exports : right.exports;
  runtime.publish(model.changed, { id: assertId(index % 3 === 0 ? 7 : '7'), add: (index % 7) - 2, requestId: 'private-request-id' });
};
for (let index = 0; index < 20; index++) publish(index);
runtime.flush();
const recorder = recordScenario(runtime);
for (let index = 20; index < 180; index += 4) {
  for (let offset = 0; offset < 4; offset++) publish(index + offset);
  runtime.flush();
}
const scenario = recorder.snapshot();
const outcome = recordArchiveOutcome(left.exports.effect, { plan: { requestId: 'private-outcome-request' }, outcome: 5 });
const source = ok(archive.capture(scenario, provenance, [outcome]));
const passed: string[] = [];
function test(name: string, run: () => void): void {
  run();
  passed.push(name);
  console.log(`PASS ${name}`);
}
try {
  test('multi-step owned migrations preserve checkpoint, mixed-owner waves, IDs and original build provenance', () => {
    const transported = parseEnvelope(ok(archive.stringify(source)));
    const migrated = ok(nextArchive.migrate(transported));
    assert.equal(migrated.applied.length, 4);
    assert.deepEqual(migrated.envelope.build, provenance.build);
    assert.deepEqual(migrated.envelope.scenario.waves.map((wave) => [wave.wave, wave.events.length]), scenario.waves.map((wave) => [wave.wave, wave.events.length]));
    const replay = ok(nextArchive.replay(transported));
    try {
      for (const [before, after] of [[left, newLeft], [right, newRight]]) {
        assert.equal(replay.runtime.read(after.exports.count).total, runtime.read(before.exports.count).total * 100);
        for (const id of [assertId(7), assertId('7')])
          assert.equal(replay.runtime.readKeyed(after.exports.values, id).total, runtime.readKeyed(before.exports.values, id).total * 100);
      }
      assert.equal(assertOutcome(replay.outcomes[0].record.value).outcome, 500);
      assert.equal(replay.runtime.mode, 'replay');
      let executions = 0;
      bindEffect(replay.runtime, newLeft.exports.effect, { execute: async () => ++executions, failure: () => -1 });
      replay.runtime.publish(newLeft.exports.changed, { id: assertId(7), add: 1, requestId: 'new' });
      replay.runtime.flush();
      assert.equal(executions, 0);
    } finally { replay.runtime.dispose(); }
  });
  test('missing, future, foreign, incomplete and schema-incompatible records refuse without executing a runtime', () => {
    const missing = createReplayArchive(nextApp, [defineLibraryHistory(next.definition, { version: 3 })]);
    assert.deepEqual(missing.migrate(source), { kind: 'refused', code: 'migration' });
    assert.equal(nextArchive.migrate({ ...source, libraries: source.libraries.map((entry) => ({ ...entry, version: 4 })) }).kind, 'refused');
    assert.equal(nextArchive.migrate({ ...source, libraries: source.libraries.map((entry) => ({ ...entry, library: 'foreign' })) }).kind, 'refused');
    assert.deepEqual(nextArchive.migrate({ ...source, scenario: { ...source.scenario, complete: false } }), { kind: 'refused', code: 'incomplete' });
    assert.equal(archive.replay({ ...source, scenario: { ...source.scenario, checkpoint: { ...source.scenario.checkpoint, states: [] } } }).kind, 'refused');
    assert.equal(createReplayArchive(app, [oldHistory]).replay(source).kind, 'refused', 'Unknown outcome codecs must not be silently accepted.');
  });
  test('migration cardinality and ownership checks prevent dropping events or writing another library', () => {
    for (const migrate of [
      (recording: Parameters<LibraryMigration['migrate']>[0]) => ({ ...recording, events: [] }),
      (recording: Parameters<LibraryMigration['migrate']>[0]) => ({ ...recording, schema: recording.schema.map((entry) => ({ ...entry, owner: 'other' })) }),
    ]) {
      const invalid = createReplayArchive(nextApp, [defineLibraryHistory(next.definition, { version: 3, migrations: [{ from: 1, migrate }] })]);
      assert.equal(invalid.migrate(source).kind, 'refused');
    }
    assert.deepEqual(captureCheckpoint(runtime), { ...captureCheckpoint(runtime) });
  });
  test('canonical archive order is independent of composition and object insertion order', () => {
    const reversed = createReplayArchive(composeLibraries(right, left), [oldHistory], { effects: [right.exports.effect, left.exports.effect] });
    const other = ok(reversed.capture(scenario, provenance, [outcome]));
    assert.equal(ok(archive.stringify(source)), ok(reversed.stringify(other)));
    const replay = ok(reversed.replay(source));
    assert.equal(replay.runtime.read(left.exports.count).total, runtime.read(left.exports.count).total);
    replay.runtime.dispose();
    assert.ok(Object.isFrozen(source.scenario.waves[0].events[0].payload.value));
  });
  test('support defaults deny everything, including raw mount names, values, IDs, outcomes and source provenance', () => {
    const result = createSupportExporter(app).export(source, safeProvenance);
    assert.equal(result.kind, 'exported');
    if (result.kind !== 'exported') return;
    assert.equal(result.envelope.replayable, false);
    assert.equal(result.envelope.kind, 'statebus-support');
    assert.deepEqual(result.envelope.states, []);
    assert.deepEqual(result.envelope.outcomes, []);
    assert.deepEqual(result.envelope.waves, []);
    assert.ok(!result.json.includes('private-'));
    assert.equal(result.bytes, new TextEncoder().encode(result.json).byteLength);
  });
  const approved = {
    mounts: [{ mount: left, history: oldHistory, name: 'primary' }],
    states: [
      supportScalar(left.exports.count, { name: 'count', codec: old.valueCodec, fields: [{ name: 'total', select: (value) => value.total }] }),
      supportKeyed(left.exports.values, { name: 'values', codec: old.valueCodec, fields: [{ name: 'total', select: (value) => value.total }], id: { codec: idCodec, select: (id) => id } }),
    ],
    events: [supportEvent(left.exports.changed, { name: 'change', codec: old.changeCodec, fields: [{ name: 'add', select: (change) => change.add }] })],
    outcomes: [supportEffect(left.exports.effect, { name: 'operation', fields: [{ name: 'result', select: (value) => value.outcome }] })],
  };
  test('explicit field and ID policies omit newly added private fields and whole private mounts', () => {
    const result = createSupportExporter(app, approved).export(source, safeProvenance);
    assert.equal(result.kind, 'exported');
    if (result.kind !== 'exported') return;
    assert.ok(!result.json.includes('private-'));
    assert.ok(!result.json.includes('secret'));
    assert.ok(!result.json.includes('future'));
    assert.ok(!result.json.includes('requestId'));
    assert.equal(result.envelope.libraries.length, 1);
    assert.equal(result.envelope.states.length, 2);
    assert.equal(result.envelope.outcomes[0].fields.result, 5);
    const values = result.envelope.states.find((entry) => entry.name === 'values');
    assert.ok(values);
    assert.deepEqual(new Set(values.entries.map((entry) => entry.id)), new Set([7, '7']));
    assert.ok(Object.isFrozen(result.envelope.states[0].entries[0].fields));
  });
  test('denied IDs are dropped before value decoding, and excluded values cannot be overridden', () => {
    let decoded = 0;
    const policy = supportKeyed(left.exports.values, {
      name: 'values', codec: { ...old.valueCodec, decode: (value) => { decoded++; return assertValue(value); } },
      fields: [{ name: 'total', select: (value) => value.total }], id: { codec: idCodec, select: () => undefined },
    });
    const result = createSupportExporter(app, { mounts: approved.mounts, states: [policy] }).export(source, safeProvenance);
    assert.equal(result.kind, 'exported');
    assert.equal(decoded, 0);
    const entry = source.scenario.checkpoint.states.find((state) => state.key === left.exports.count.metadata.key)?.entries[0];
    assert.ok(entry);
    assert.equal(approved.states[0].project({ ...entry, value: { ...entry.value, classification: 'excluded' } }), undefined);
  });
  test('unknown versions and unapproved payloads cannot reach a decoder or leak through fallback export', () => {
    const future = { ...source, libraries: source.libraries.map((entry) => ({ ...entry, version: 999 })) };
    const result = createSupportExporter(app, approved).export(future, safeProvenance);
    assert.equal(result.kind, 'exported');
    if (result.kind === 'exported') assert.equal(result.envelope.states.length, 0);
    let reads = 0;
    const poison = { get secret() { reads++; throw new Error('private-getter'); } };
    const denied = { ...source, scenario: { ...source.scenario, checkpoint: {
      ...source.scenario.checkpoint,
      states: source.scenario.checkpoint.states.map((state) => ({ ...state, entries: state.entries.map((entry) => ({ ...entry, value: { ...entry.value, value: poison } })) })),
    } } };
    assert.equal(createSupportExporter(app).export(denied, safeProvenance).kind, 'exported');
    assert.equal(reads, 0);
  });
  test('bounds and policy exceptions return portable refusals without partial captures or raw exceptions', () => {
    assert.deepEqual(createSupportExporter(app, { ...approved, limits: { maxBytes: 50 } }).export(source, safeProvenance), { kind: 'refused', code: 'limit' });
    const failing = supportScalar(left.exports.count, { name: 'count', codec: old.valueCodec, fields: [{ name: 'total', select() { throw new Error('private-exception-detail'); } }] });
    const result = createSupportExporter(app, { mounts: approved.mounts, states: [failing] }).export(source, safeProvenance);
    assert.deepEqual(result, { kind: 'refused', code: 'policy' });
    assert.ok(!JSON.stringify(result).includes('private-'));
    assert.equal(createReplayArchive(app, [oldHistory], { limits: { maxBytes: 100 } }).capture(scenario, provenance).kind, 'refused');
  });
  test('cyclic codec output refuses local capture and no partially owned record escapes', () => {
    const value: { self?: unknown } = {};
    value.self = value;
    const malformed = { ...scenario, waves: scenario.waves.map((wave) => ({ ...wave, events: wave.events.map((event) => ({ ...event, payload: { ...event.payload, value } })) })) };
    assert.deepEqual(archive.capture(malformed, provenance), { kind: 'refused', code: 'json' });
  });
  test('foreign policies and duplicate library migration registrations fail at setup', () => {
    assert.throws(() => createSupportExporter(app, { mounts: [{ mount: newLeft, history: nextHistory, name: 'foreign' }] }), /Foreign/);
    assert.throws(() => defineLibraryHistory(next.definition, { version: 3, migrations: [upgrade(1), upgrade(1)] }), /duplicate/);
    assert.throws(() => createReplayArchive(app, []), /history/);
  });
  // Compile-only inference failures: these are never executed.
  function invalidTypes(): void {
    supportScalar(left.exports.count, { name: 'bad', codec: old.valueCodec, fields: [
      // @ts-expect-error Raw object passthrough is not a support field.
      { name: 'raw', select: (value) => value },
    ] });
    supportKeyed(left.exports.values, { name: 'bad', codec: old.valueCodec, fields: [], id: {
      // @ts-expect-error Branded string/number IDs cannot be decoded using a boolean codec.
      codec: codec('wrong', 1, typia.createAssert<boolean>()), select: () => null,
    } });
  }
  void invalidTypes;
  console.log(JSON.stringify({ passed: passed.length, tests: passed, builtExports: true }));
} finally {
  recorder.dispose();
  runtime.dispose();
}
