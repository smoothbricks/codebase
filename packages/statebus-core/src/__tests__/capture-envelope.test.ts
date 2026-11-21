import { describe, expect, it } from 'bun:test';
import { CaptureError } from '../capture.js';
import { createCaptureEnvelope, migrateCaptureEnvelope, replayCaptureEnvelope } from '../capture-envelope.js';
import { composeLibraries, defineLibrary, mountLibrary, type ValueCodec } from '../composition.js';
import { ManualScheduler } from '../dispatch.js';
import { defineEffect, type EffectOutcome } from '../effects.js';
import { recordScenario } from '../recording.js';

const amount: ValueCodec<number> = {
  schema: 'jobs.amount',
  version: 1,
  encode: (value) => value,
  decode(value) {
    if (typeof value !== 'number') throw new TypeError('An amount is a number.');
    return value;
  },
};
type JobOutcome = EffectOutcome<{ readonly requestId: number }, number>;
// These envelopes retain no outcome, so no test ever decodes one.
const outcome: ValueCodec<JobOutcome> = {
  schema: 'jobs.outcome',
  version: 1,
  encode: (value) => ({ requestId: value.plan.requestId, outcome: value.outcome }),
  decode() {
    throw new Error('No outcome is retained in these envelopes.');
  },
};

/** One library whose only difference between builds is whether its required effect declares an outcome codec. */
function jobs(codec?: ValueCodec<JobOutcome>) {
  const mount = mountLibrary(
    defineLibrary({
      name: 'jobs',
      requires: [],
      setup(scope) {
        const total = scope.scalar('total', () => 0, { codec: amount });
        const start = scope.command('start', (_state, _requested: number) => true, { codec: amount });
        const finished = scope.event('finished', { codec: amount });
        scope.reduce(finished, (state, done) => state.set(total, state.read(total) + done));
        scope.requireEffect(
          defineEffect({
            command: start,
            result: finished,
            plan: (_state, requested) => ({ requestId: requested }),
            decode: (_plan, done: number) => done,
            codec,
          }),
        );
        return { total, finished };
      },
    }),
    'jobs',
  );
  return { mount, composition: composeLibraries(mount) };
}

/** Record one committed wave and capture it for the build that produced it. */
function capture(build: ReturnType<typeof jobs>) {
  const runtime = build.composition.createRuntime({ scheduler: new ManualScheduler() });
  try {
    const recorder = recordScenario(runtime);
    runtime.publish(build.mount.exports.finished, 3);
    runtime.flush();
    return createCaptureEnvelope(build.composition, recorder.snapshot(), { buildId: 'captured' });
  } finally {
    runtime.dispose();
  }
}

describe('capture envelope migration: effect outcome codecs', () => {
  it('migrates an effect that never declared an outcome codec', () => {
    const build = jobs();
    const captured = capture(build);
    expect(captured.manifest.effects.map((effect) => [effect.schema, effect.version])).toEqual([
      [undefined, undefined],
    ]);

    const migrated = migrateCaptureEnvelope(build.composition, captured, { buildId: 'migrated' });

    expect(migrated.migratedForBuild).toBe('migrated');
    const replay = replayCaptureEnvelope(build.composition, migrated);
    try {
      expect(replay.read(build.mount.exports.total)).toBe(3);
    } finally {
      replay.dispose();
    }
  });

  it('still refuses an effect whose captured outcome codec the target build cannot read', () => {
    const captured = capture(jobs(outcome));
    const target = jobs();

    let refusal: unknown;
    try {
      migrateCaptureEnvelope(target.composition, captured, { buildId: 'migrated' });
    } catch (cause) {
      refusal = cause;
    }

    expect(refusal).toBeInstanceOf(CaptureError);
    expect(refusal instanceof CaptureError && refusal.issue.boundary).toBe('unsupported effect codec version');
  });
});
