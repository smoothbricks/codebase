import assert from 'node:assert/strict';
import {
  type CaptureEnvelope,
  CaptureError,
  canonicalCapture,
  composeLibraries,
  createCaptureEnvelope,
  defineEffect,
  defineLibrary,
  exportSupportCapture,
  ManualScheduler,
  migrateCaptureEnvelope,
  mountLibrary,
  publicSupport,
  recordRollingScenario,
  recordScenario,
  replayCaptureEnvelope,
  type SupportClassification,
  type ValueCodec,
} from '@smoothbricks/statebus-core';
import typia from 'typia';

interface Row {
  readonly phase: 'ready' | 'idle';
  readonly email: string;
  readonly futurePrivateText?: string;
}
const rowCodec: ValueCodec<Row> = {
  schema: 'PRIVATE_ROW_SCHEMA',
  version: 1,
  encode: (value) => structuredClone(value),
  decode: typia.createAssert<Row>(),
};
const idCodec: ValueCodec<string | number> = {
  schema: 'PRIVATE_ID_SCHEMA',
  version: 1,
  encode: (id) => id,
  decode: typia.createAssert<string | number>(),
};
const row: Row = {
  phase: 'ready',
  email: 'PRIVATE_EMAIL',
  futurePrivateText: 'PRIVATE_FUTURE_FIELD',
};
const library = defineLibrary({
  name: 'PRIVATE_LIBRARY_NAME',
  requires: [],
  setup(scope) {
    const value = scope.scalar('PRIVATE_STATE_NAME', () => row, {
      codec: rowCodec,
      support: publicSupport((value: Row) => ({ phase: value.phase })),
    });
    const command = scope.command('PRIVATE_COMMAND_NAME', (_state, _row: Row) => true, {
      codec: rowCodec,
      support: publicSupport((value: Row) => ({ phase: value.phase })),
    });
    const result = scope.event('PRIVATE_RESULT_NAME', { codec: rowCodec });
    scope.reduce(command, (state, next) => state.set(value, next));
    const effect = defineEffect({
      command,
      result,
      plan: () => ({ requestId: 'PRIVATE_REQUEST_ID' }),
      decode: (_plan, outcome: Row) => outcome,
      codec: {
        schema: 'PRIVATE_OUTCOME_SCHEMA',
        version: 1,
        encode: (value) => structuredClone(value),
        decode: typia.createAssert<{ plan: { requestId: string }; outcome: Row }>(),
      },
      support: publicSupport((value: { plan: { requestId: string }; outcome: Row }) => ({
        phase: value.outcome.phase,
      })),
    });
    return { value, command, effect };
  },
});
const mount = mountLibrary(library, 'PRIVATE_OWNER_NAME');
const composition = composeLibraries(mount);
const runtime = composition.createRuntime({ scheduler: new ManualScheduler() });
const recorder = recordRollingScenario(runtime);
runtime.publish(mount.exports.command, row);
runtime.flush();
const sink = recorder.outcomeSink(mount.exports.effect);
sink({ plan: { requestId: 'PRIVATE_REQUEST_ID' }, outcome: row });
runtime.publish(mount.exports.command, row);
runtime.flush();
sink({ plan: { requestId: 'PRIVATE_REQUEST_ID_2' }, outcome: row });
const source = createCaptureEnvelope(composition, recorder.snapshot(), {
  buildId: 'PRIVATE_BUILD_ID',
  environment: { privateHost: 'PRIVATE_ENVIRONMENT' },
  effects: [mount.exports.effect],
});
const effects = [mount.exports.effect];
let passed = 0;
function test(name: string, run: () => void): void {
  run();
  passed++;
  console.log(`PASS support boundary: ${name}`);
}

try {
  test('metadata and newly added private fields are absent unless deliberately projected', () => {
    const before = canonicalCapture(source);
    const artifact = exportSupportCapture(composition, source, { effects });
    const text = canonicalCapture(artifact);
    assert.equal(artifact.replayable, false);
    assert.ok(text.includes('"phase":"ready"'));
    assert.ok(!text.includes('PRIVATE_'), text);
    const approved = exportSupportCapture(composition, source, {
      effects,
      metadata: publicSupport((metadata) => ({
        build: metadata.application.buildId,
        declarations: metadata.manifest.declarations.map((_entry, reference) => ({ reference, name: 'approved' })),
      })),
    });
    const approvedText = canonicalCapture(approved);
    assert.ok(approvedText.includes('PRIVATE_BUILD_ID'));
    assert.ok(approvedText.includes('"name":"approved"'));
    assert.ok(!approvedText.replaceAll('PRIVATE_BUILD_ID', '').includes('PRIVATE_'), approvedText);
    assert.equal(canonicalCapture(source), before);
    assert.ok(Object.isFrozen(approved.payload));
  });

  test('projection failures expose neither private messages nor nested causes', () => {
    for (const failure of [
      new Error('PRIVATE_POLICY_ERROR', { cause: new Error('PRIVATE_NESTED_CAUSE') }),
      new CaptureError({ code: 'schema', boundary: 'PRIVATE_BOUNDARY', owner: 'PRIVATE_ERROR_OWNER' }),
    ]) {
      assert.throws(
        () =>
          exportSupportCapture(composition, source, {
            effects,
            metadata: () => {
              throw failure;
            },
          }),
        (error) => {
          assert.ok(error instanceof CaptureError);
          assert.deepEqual(error.issue, { code: 'schema', boundary: 'support projection' });
          assert.equal(error.cause, undefined);
          assert.ok(!String(error).includes('PRIVATE_'));
          return true;
        },
      );
    }
  });

  test('direct replay rejects incompatible effect headers, foreign effects, and corrupt causal positions', () => {
    const captures = source.captures;
    assert.equal(captures.length, 2);
    const invalid: readonly CaptureEnvelope[] = [
      { ...source, captures: captures.map((entry) => ({ ...entry, capture: { ...entry.capture, version: 99 } })) },
      {
        ...source,
        captures: captures.map((entry) => ({ ...entry, capture: { ...entry.capture, effect: 'foreign' } })),
      },
      { ...source, captures: [captures[0], captures[0]] },
      { ...source, captures: [{ ...captures[0], sequence: 0 }] },
      { ...source, captures: [{ ...captures[0], afterWave: -1 }] },
      { ...source, captures: [captures[0], { ...captures[1], afterWave: 0 }] },
      { ...source, evictedEffects: -1 },
      {
        ...source,
        manifest: { ...source.manifest, effects: [...source.manifest.effects, ...source.manifest.effects] },
      },
    ];
    for (const input of invalid) {
      for (const check of [
        () => replayCaptureEnvelope(composition, input, effects),
        () => migrateCaptureEnvelope(composition, input, { buildId: 'target', effects }),
        () => exportSupportCapture(composition, input, { effects }),
      ])
        assert.throws(check, (error) => error instanceof CaptureError && error.issue.code === 'schema');
    }
    const replay = replayCaptureEnvelope(composition, source, effects);
    try {
      assert.deepEqual(replay.read(mount.exports.value), runtime.read(mount.exports.value));
    } finally {
      replay.dispose();
    }
  });

  test('missing scalar checkpoint cells refuse capture, migration, replay and support export', () => {
    const scenario = {
      ...source.scenario,
      checkpoint: { ...source.scenario.checkpoint, states: [] },
    };
    const input: CaptureEnvelope = { ...source, scenario };
    for (const check of [
      () => createCaptureEnvelope(composition, scenario, { buildId: 'target', effects }),
      () => migrateCaptureEnvelope(composition, input, { buildId: 'target', effects }),
      () => replayCaptureEnvelope(composition, input, effects),
      () => exportSupportCapture(composition, input, { effects }),
    ])
      assert.throws(check, (error) => error instanceof CaptureError && error.issue.code === 'schema');
  });

  test('denied resource IDs prevent value decoding and projection, even with broad value permission', () => {
    const classifications: readonly SupportClassification[] = [
      'secret',
      'excluded',
      'sensitive',
      'unclassified',
      'public',
    ];
    for (const classification of classifications) {
      let decodes = 0;
      let projections = 0;
      const model = defineLibrary({
        name: 'PRIVATE_KEYED_LIBRARY',
        requires: [],
        setup(scope) {
          const values = scope.keyed('PRIVATE_KEYED_STATE', (_id: string | number) => row, {
            idCodec,
            codec: {
              ...rowCodec,
              decode: (value) => {
                decodes++;
                return rowCodec.decode(value);
              },
            },
            classifyId: () => classification,
            supportId: classification === 'unclassified' ? undefined : publicSupport((id: string | number) => id),
            support: publicSupport((value: Row) => {
              projections++;
              return { phase: value.phase };
            }),
          });
          const write = scope.event('PRIVATE_WRITE', { codec: idCodec });
          scope.reduce(write, (state, id) => state.setKeyed(values, id, { ...row }));
          return { write };
        },
      });
      const mounted = mountLibrary(model, 'PRIVATE_KEYED_OWNER');
      const app = composeLibraries(mounted);
      const bus = app.createRuntime({ scheduler: new ManualScheduler() });
      try {
        for (const id of [7, '7']) bus.publish(mounted.exports.write, id);
        bus.flush();
        const record = recordScenario(bus);
        try {
          const local = createCaptureEnvelope(app, record.snapshot(), { buildId: 'PRIVATE_KEYED_BUILD' });
          assert.equal(local.scenario.checkpoint.states[0].entries.length, 2);
          decodes = 0;
          projections = 0;
          const text = canonicalCapture(
            exportSupportCapture(app, local, {
              consent: classification === 'secret' || classification === 'excluded',
            }),
          );
          const expected = classification === 'public' ? 2 : 0;
          assert.equal(decodes, expected, classification);
          assert.equal(projections, expected, classification);
          assert.equal(text.includes('"phase":"ready"'), classification === 'public');
          assert.ok(!text.includes('PRIVATE_'));
        } finally {
          record.dispose();
        }
      } finally {
        bus.dispose();
      }
    }
  });
  console.log(JSON.stringify({ supportBoundaryScenarios: passed, source: 'packed public exports' }));
} finally {
  recorder.dispose();
  runtime.dispose();
}
