import { describe, expect, it } from 'bun:test';
import { CaptureError, captureBytes } from '../capture.js';
import {
  composeLibraries,
  defineLibrary,
  type EncodedEvent,
  type EncodedStateEntry,
  type EncodedValue,
  mountLibrary,
  type ValueCodec,
} from '../composition.js';
import { ManualScheduler } from '../dispatch.js';
import { captureCheckpoint, recordScenario, sizedEntry, sizedEvent } from '../recording.js';

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

describe('captureCheckpoint keyed entries', () => {
  it('orders resource IDs canonically without validating the IDs it already holds', () => {
    const { runtime, events, decodes } = notes();
    try {
      for (const note of ['b', 10, 'a', 2]) runtime.publish(events.noted, note);
      runtime.flush();
      const before = decodes();

      const checkpoint = captureCheckpoint(runtime);

      const body = checkpoint.states.find((state) => state.key.endsWith('body'));
      expect(body?.entries.map((entry) => entry.id?.value)).toEqual([2, 10, 'a', 'b']);
      expect(decodes() - before).toBe(0);
    } finally {
      runtime.dispose();
    }
  });
});

describe('sized capture records', () => {
  const encoded = (payload: unknown): EncodedValue => ({
    schema: 'notes.any',
    version: 1,
    value: payload,
    classification: 'unclassified',
  });
  const payloads: readonly unknown[] = [
    '',
    'ü "quoted"\n𝄞',
    0,
    -1.5e21,
    null,
    true,
    [1, [2, { a: 'b' }]],
    { z: 1, a: { nested: [null] } },
  ];

  it('reports exactly the canonical bytes of the detached record', () => {
    for (const payload of payloads) {
      const entries: readonly EncodedStateEntry[] = [
        { value: encoded(payload) },
        { id: { ...encoded('note-1'), primitive: 'string' }, value: encoded(payload) },
      ];
      for (const entry of entries) {
        const sized = sizedEntry(entry);
        expect(sized.bytes).toBe(captureBytes(sized.value));
      }
      const events: readonly EncodedEvent[] = [
        { key: 'notes/titled', payload: encoded(payload) },
        { key: 'notes/titled', payload: encoded(payload), admitted: false, reactionDepth: 2 },
      ];
      for (const event of events) {
        const sized = sizedEvent(event);
        expect(sized.bytes).toBe(captureBytes(sized.value));
      }
    }
  });

  it('refuses a record whose payload fits but whose framed size does not', () => {
    const event: EncodedEvent = { key: 'notes/titled', payload: encoded('short') };
    const framed = sizedEvent(event).bytes;

    expect(refusedForSize(() => sizedEvent(event, framed, framed))).toBe(false);
    expect(refusedForSize(() => sizedEvent(event, framed, framed - 1))).toBe(true);
    expect(refusedForSize(() => sizedEntry({ value: event.payload }, framed, 8))).toBe(true);
  });
});
