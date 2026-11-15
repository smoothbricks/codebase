import { CaptureError, captureBytes, captureLimit, captureValue } from './capture.js';
import { decodeCodec } from './codec.js';
import type {
  ComposedRuntime,
  DeclarationMetadata,
  EncodedEvent,
  EncodedState,
  EncodedStateEntry,
  EncodedValue,
  StateBusComposition,
  SupportClassification,
  WaveObserver,
} from './composition.js';
import { ManualScheduler } from './dispatch.js';
import type { EffectDefinition, EffectOutcome, EffectPlan } from './effects.js';

export interface StateCheckpoint {
  readonly formatVersion: 1;
  readonly schema: readonly DeclarationMetadata[];
  readonly states: readonly EncodedState[];
}
export interface RecordedWave {
  readonly wave: number;
  readonly events: readonly EncodedEvent[];
}
export interface RecordedScenario {
  readonly formatVersion: 1;
  readonly checkpoint: StateCheckpoint;
  readonly waves: readonly RecordedWave[];
  /** False after eviction or support filtering; such a record MUST NOT be called replay-equivalent. */
  readonly complete: boolean;
  readonly checkpointWave?: number;
}
export interface ScenarioRecorder {
  snapshot(): RecordedScenario;
  /** Explicit cold boundary. No checkpoint is cloned on ordinary publication. */
  reset(): void;
  dispose(): void;
}
/** A detached record together with the canonical JSON bytes of that whole record. */
export interface Sized<T> {
  readonly value: T;
  readonly bytes: number;
}
// Canonical JSON size is additive per member. A record's size is therefore its frame measured around
// `null`, plus the payload that takes the place of that `null`. The payload is walked once, by the
// copy that detaches it; only the few scalar members of the frame are measured separately.
const NULL_BYTES = captureBytes(null);
const ENTRY_BYTES = captureBytes({ value: null }) - NULL_BYTES;
const KEYED_ENTRY_BYTES = captureBytes({ id: null, value: null }) - 2 * NULL_BYTES;
function withinRecord(bytes: number, maxRecordBytes: number | undefined): number {
  if (maxRecordBytes !== undefined && bytes > maxRecordBytes)
    throw new CaptureError({ code: 'size-limit', boundary: 'portable value', limit: maxRecordBytes });
  return bytes;
}
function sizedEncoded(value: EncodedValue, maxBytes?: number): Sized<EncodedValue> {
  const payload = captureValue(value.value, maxBytes);
  return {
    value: Object.freeze({ ...value, value: payload.value }),
    bytes: captureBytes({ ...value, value: null }) - NULL_BYTES + payload.bytes,
  };
}
/** `maxBytes` bounds each encoded payload, `maxRecordBytes` the whole detached entry. */
export function sizedEntry(
  entry: EncodedStateEntry,
  maxBytes?: number,
  maxRecordBytes?: number,
): Sized<EncodedStateEntry> {
  const id = entry.id ? sizedEncoded(entry.id, maxBytes) : undefined;
  const value = sizedEncoded(entry.value, maxBytes);
  const bytes = id ? KEYED_ENTRY_BYTES + id.bytes + value.bytes : ENTRY_BYTES + value.bytes;
  return {
    value: Object.freeze({ ...(id ? { id: id.value } : {}), value: value.value }),
    bytes: withinRecord(bytes, maxRecordBytes),
  };
}
/** `maxBytes` bounds the encoded payload, `maxRecordBytes` the whole detached event. */
export function sizedEvent(event: EncodedEvent, maxBytes?: number, maxRecordBytes?: number): Sized<EncodedEvent> {
  const payload = sizedEncoded(event.payload, maxBytes);
  return {
    value: Object.freeze({ ...event, payload: payload.value }),
    bytes: withinRecord(captureBytes({ ...event, payload: null }) - NULL_BYTES + payload.bytes, maxRecordBytes),
  };
}
export function ownEntry(entry: EncodedStateEntry, maxBytes?: number): EncodedStateEntry {
  return sizedEntry(entry, maxBytes).value;
}
export function ownEvent(event: EncodedEvent, maxBytes?: number): EncodedEvent {
  return sizedEvent(event, maxBytes).value;
}
export interface CheckpointCaptureOptions {
  readonly maxBytes?: number;
  readonly maxEntryBytes?: number;
}
export function captureCheckpoint(runtime: ComposedRuntime, options: CheckpointCaptureOptions = {}): StateCheckpoint {
  if (!runtime.idle || runtime.disposed) throw new Error('Checkpoint requires a live, quiescent dispatch queue.');
  const maxBytes = captureLimit(options.maxBytes ?? 8 * 1024 * 1024, 'maxBytes');
  const maxEntryBytes = captureLimit(options.maxEntryBytes ?? maxBytes, 'maxEntryBytes');
  const states: EncodedState[] = [];
  let bytes = captureBytes({ formatVersion: 1, schema: runtime.composition.metadata, states: [] }, maxBytes);
  function account(size: number): void {
    bytes += size;
    if (bytes > maxBytes) throw new CaptureError({ code: 'size-limit', boundary: 'checkpoint', limit: maxBytes });
  }
  for (const declaration of runtime.composition.states) {
    const key = declaration.metadata.key;
    account(captureBytes({ key, entries: [] }, maxBytes) + (states.length > 0 ? 1 : 0));
    const entries: EncodedStateEntry[] = [];
    for (const entry of declaration.captureEntries(runtime)) {
      if (bytes >= maxBytes) throw new CaptureError({ code: 'size-limit', boundary: 'checkpoint', limit: maxBytes });
      const sized = sizedEntry(entry, Math.min(maxEntryBytes, maxBytes - bytes), maxEntryBytes);
      account(sized.bytes + (entries.length > 0 ? 1 : 0));
      entries.push(sized.value);
    }
    // A declaration yields its entries in canonical resource-ID order.
    states.push(Object.freeze({ key, entries: Object.freeze(entries) }));
  }
  return Object.freeze({ formatVersion: 1, schema: runtime.composition.metadata, states: Object.freeze(states) });
}

export interface ScenarioRecorderOptions {
  readonly maxEvents?: number;
  /** Canonical bytes of each explicit checkpoint. Omitted, `captureCheckpoint`'s own default applies. */
  readonly maxCheckpointBytes?: number;
  /** Canonical bytes of one encoded checkpoint entry or event. Omitted, the capture defaults apply. */
  readonly maxEntryBytes?: number;
}
export function recordScenario(
  runtime: ComposedRuntime,
  { maxEvents = 1024, maxCheckpointBytes, maxEntryBytes }: ScenarioRecorderOptions = {},
): ScenarioRecorder {
  if (!Number.isSafeInteger(maxEvents) || maxEvents < 1)
    throw new RangeError('maxEvents must be a positive safe integer.');
  // Fail at boundary installation, not halfway through a production dispatch wave.
  for (const entry of runtime.composition.metadata)
    if (!entry.schema || entry.version === undefined)
      throw new Error(`Replay codec missing: ${entry.owner}/${entry.name}`);
  // The first checkpoint also validates both byte limits before any wave is observed.
  const capture = () => captureCheckpoint(runtime, { maxBytes: maxCheckpointBytes, maxEntryBytes });
  let checkpoint = capture();
  const waves: RecordedWave[] = [];
  let count = 0;
  let complete = true;
  let closed = false;
  const observer: WaveObserver = {
    committed(wave, queue, length) {
      const events: EncodedEvent[] = [];
      try {
        for (let index = 0; index < length; index++) {
          const event = queue[index];
          if (event?.kind === 'event') events.push(sizedEvent(event.capture(), maxEntryBytes, maxEntryBytes).value);
        }
      } catch (cause) {
        complete = false;
        runtime.reportError(cause);
        return;
      }
      if (events.length === 0) return;
      if (events.length > maxEvents) {
        // A partial wave cannot be replayed: retain no misleading tail of an oversized wave.
        waves.length = 0;
        count = 0;
        complete = false;
        return;
      }
      while (count + events.length > maxEvents) {
        const removed = waves.shift();
        if (!removed) break;
        count -= removed.events.length;
        complete = false;
      }
      // Never retain the dispatcher's recyclable queue or a mutable scratch array.
      waves.push(Object.freeze({ wave, events: Object.freeze(events) }));
      count += events.length;
    },
  };
  const stop = runtime.observeWaves(observer);
  return Object.freeze({
    snapshot: () => Object.freeze({ formatVersion: 1, checkpoint, waves: Object.freeze([...waves]), complete }),
    reset() {
      if (closed) throw new Error('Recorder is disposed.');
      checkpoint = capture();
      waves.length = 0;
      count = 0;
      complete = true;
    },
    dispose() {
      closed = true;
      stop();
    },
  });
}
function checkSchema(composition: StateBusComposition, checkpoint: StateCheckpoint): void {
  if (checkpoint.formatVersion !== 1 || checkpoint.schema.length !== composition.metadata.length)
    throw new CaptureError({ code: 'schema', boundary: 'checkpoint declaration set' });
  for (let index = 0; index < checkpoint.schema.length; index++) {
    const before = checkpoint.schema[index];
    const after = composition.metadata[index];
    if (
      before.key !== after.key ||
      before.kind !== after.kind ||
      before.schema !== after.schema ||
      before.version !== after.version ||
      before.idSchema !== after.idSchema ||
      before.idVersion !== after.idVersion
    )
      throw new CaptureError({
        code: 'schema',
        boundary: 'checkpoint declaration',
        owner: after.owner,
        declaration: after.name,
        schema: before.schema,
        fromVersion: before.version,
        toVersion: after.version,
      });
  }
}
export function replayScenario(composition: StateBusComposition, scenario: RecordedScenario): ComposedRuntime {
  if (scenario.formatVersion !== 1 || !scenario.complete)
    throw new Error('Incomplete or unsupported scenario cannot be replayed.');
  checkSchema(composition, scenario.checkpoint);
  const scheduler = new ManualScheduler();
  const runtime = composition.createRuntime({ mode: 'replay', scheduler });
  try {
    const states = new Map(scenario.checkpoint.states.map((state) => [state.key, state]));
    if (states.size !== composition.states.length || states.size !== scenario.checkpoint.states.length)
      throw new Error('Missing or duplicate checkpoint state.');
    for (const declaration of composition.states) {
      const state = states.get(declaration.metadata.key);
      if (!state) throw new Error('Missing checkpoint state.');
      declaration.restore(runtime, state);
    }
    let previousWave = scenario.checkpointWave ?? 0;
    if (!Number.isSafeInteger(previousWave) || previousWave < 0)
      throw new CaptureError({ code: 'schema', boundary: 'checkpoint wave' });
    for (const wave of scenario.waves) {
      if (!Number.isSafeInteger(wave.wave) || wave.wave <= previousWave)
        throw new Error('Invalid recorded wave order.');
      previousWave = wave.wave;
      for (const event of wave.events) runtime.enqueueRecorded(event);
      runtime.flush();
    }
    return runtime;
  } catch (cause) {
    runtime.dispose();
    throw cause;
  }
}

export interface RecordedEffectInstruction {
  readonly kind: 'instruction';
  readonly effect: string;
  readonly classification?: SupportClassification;
  readonly schema: string;
  readonly version: number;
  readonly value: unknown;
}
export type RecordedEffectCapture = RecordedEffectOutcome | RecordedEffectInstruction;
export interface RecordedEffectOutcome {
  readonly kind?: 'outcome';
  readonly effect: string;
  readonly classification?: SupportClassification;
  readonly schema: string;
  readonly version: number;
  readonly value: unknown;
}
export function captureEffectOutcome<C, P extends EffectPlan, O, R>(
  definition: EffectDefinition<C, P, O, R>,
  outcome: EffectOutcome<P, O>,
  maxBytes?: number,
): RecordedEffectOutcome {
  const codec = definition.codec;
  if (!codec) throw new Error('An effect outcome codec is required for capture.');
  return Object.freeze({
    kind: 'outcome',
    effect: definition.metadata.key,
    classification: definition.classify?.(outcome) ?? 'unclassified',
    schema: codec.schema,
    version: codec.version,
    value: captureValue(codec.encode(outcome), maxBytes).value,
  });
}
export function decodeEffectOutcome<C, P extends EffectPlan, O, R>(
  definition: EffectDefinition<C, P, O, R>,
  outcome: RecordedEffectOutcome,
): EffectOutcome<P, O> {
  if (outcome.effect !== definition.metadata.key)
    throw new CaptureError({
      code: 'schema',
      boundary: 'Incompatible effect outcome',
      owner: definition.metadata.owner,
      declaration: outcome.effect,
    });
  try {
    return decodeCodec(definition.codec, outcome);
  } catch (cause) {
    throw new CaptureError(
      {
        code: 'schema',
        boundary: 'Incompatible effect outcome',
        owner: definition.metadata.owner,
        declaration: outcome.effect,
        schema: outcome.schema,
        fromVersion: outcome.version,
        toVersion: definition.codec?.version,
      },
      { cause },
    );
  }
}
export function captureEffectInstruction<C, P extends EffectPlan, O, R>(
  definition: EffectDefinition<C, P, O, R>,
  plan: P,
  maxBytes?: number,
): RecordedEffectInstruction {
  const codec = definition.instructionCodec;
  if (!codec) throw new Error('An instruction codec is required for capture.');
  return Object.freeze({
    kind: 'instruction',
    effect: definition.metadata.key,
    schema: codec.schema,
    version: codec.version,
    value: captureValue(codec.encode(plan), maxBytes).value,
    classification: definition.classifyInstruction?.(plan) ?? 'unclassified',
  });
}
export function decodeEffectInstruction<C, P extends EffectPlan, O, R>(
  definition: EffectDefinition<C, P, O, R>,
  instruction: RecordedEffectInstruction,
): P {
  try {
    if (instruction.effect !== definition.metadata.key) throw new Error('Foreign instruction.');
    return decodeCodec(definition.instructionCodec, instruction);
  } catch (cause) {
    throw new CaptureError(
      {
        code: 'schema',
        boundary: 'effect instruction',
        owner: definition.metadata.owner,
        declaration: instruction.effect,
        schema: instruction.schema,
        fromVersion: instruction.version,
        toVersion: definition.instructionCodec?.version,
      },
      { cause },
    );
  }
}
/** Support exports are intentionally non-replayable when a policy removes data. Codecs own field-level redaction. */
export function classifyScenario(
  scenario: RecordedScenario,
  include: (classification: SupportClassification) => boolean,
): RecordedScenario {
  let complete = scenario.complete;
  const states = scenario.checkpoint.states.map((state) => ({
    ...state,
    entries: state.entries.filter((entry) => {
      const keep = include(entry.value.classification) && (!entry.id || include(entry.id.classification));
      if (!keep) complete = false;
      return keep;
    }),
  }));
  const waves = scenario.waves.map((wave) => ({
    ...wave,
    events: wave.events.filter((event) => {
      const keep = include(event.payload.classification);
      if (!keep) complete = false;
      return keep;
    }),
  }));
  return { ...scenario, checkpoint: { ...scenario.checkpoint, states }, waves, complete };
}
