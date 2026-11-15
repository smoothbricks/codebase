import { describe, expect, it } from 'bun:test';
import { CaptureError } from '../capture.js';
import { composeLibraries, defineLibrary, mountLibrary, type ValueCodec } from '../composition.js';
import { ManualScheduler } from '../dispatch.js';
import { recordScenario } from '../recording.js';

const text: ValueCodec<string> = {
  schema: 'notes.text',
  version: 1,
  encode: (value) => value,
  decode(value) {
    if (typeof value !== 'string') throw new TypeError('A note is text.');
    return value;
  },
};

/** A live runtime with one scalar and one keyed declaration; `decodes` counts validations of resource IDs. */
function notes() {
  let decodes = 0;
  const id: ValueCodec<string | number> = {
    schema: 'notes.id',
    version: 1,
    encode: (value) => value,
    decode(value) {
      decodes++;
      if (typeof value !== 'string' && typeof value !== 'number') throw new TypeError('A note ID is text or a number.');
      return value;
    },
  };
  const errors: unknown[] = [];
  const mount = mountLibrary(
    defineLibrary({
      name: 'notes',
      requires: [],
      setup(scope) {
        const title = scope.scalar('title', () => '', { codec: text });
        const body = scope.keyed('body', (_note: string | number) => '', { codec: text, idCodec: id });
        const titled = scope.event('titled', { codec: text });
        const noted = scope.event('noted', { codec: id });
        scope.reduce(titled, (state, next) => state.set(title, next));
        scope.reduce(noted, (state, note) => state.setKeyed(body, note, `note ${note}`));
        return { titled, noted };
      },
    }),
    'notes',
  );
  const runtime = composeLibraries(mount).createRuntime({
    scheduler: new ManualScheduler(),
    onError: (cause) => errors.push(cause),
  });
  return { runtime, events: mount.exports, errors, decodes: () => decodes };
}

/** Whether `run` is refused for size. Any other failure is the test's own error. */
function refusedForSize(run: () => unknown): boolean {
  try {
    run();
  } catch (cause) {
    if (cause instanceof CaptureError && cause.issue.code === 'size-limit') return true;
    throw cause;
  }
  return false;
}

describe('recordScenario size limits', () => {
  // Larger than the checkpoint framing of this composition, smaller than the title written below.
  const limit = 4096;
  const long = 'x'.repeat(2 * limit);

  it('refuses at installation when the checkpoint exceeds maxCheckpointBytes', () => {
    const { runtime, events } = notes();
    try {
      runtime.publish(events.titled, long);
      runtime.flush();

      expect(refusedForSize(() => recordScenario(runtime, { maxCheckpointBytes: limit }))).toBe(true);
      expect(refusedForSize(() => recordScenario(runtime, { maxCheckpointBytes: 4 * limit }))).toBe(false);
    } finally {
      runtime.dispose();
    }
  });

  it('refuses a reset whose checkpoint has outgrown maxCheckpointBytes', () => {
    const { runtime, events } = notes();
    try {
      const recorder = recordScenario(runtime, { maxCheckpointBytes: limit });
      runtime.publish(events.titled, long);
      runtime.flush();

      expect(refusedForSize(() => recorder.reset())).toBe(true);
    } finally {
      runtime.dispose();
    }
  });

  it('marks the recording incomplete when an event exceeds maxEntryBytes', () => {
    const { runtime, events, errors } = notes();
    try {
      const recorder = recordScenario(runtime, { maxEntryBytes: 256 });
      runtime.publish(events.titled, 'short');
      runtime.flush();
      expect(recorder.snapshot().complete).toBe(true);

      runtime.publish(events.titled, 'x'.repeat(512));
      runtime.flush();

      expect(recorder.snapshot().complete).toBe(false);
      expect(errors.map((cause) => cause instanceof CaptureError && cause.issue.code)).toEqual(['size-limit']);
    } finally {
      runtime.dispose();
    }
  });
});
