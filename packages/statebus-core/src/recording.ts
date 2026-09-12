import type {
  ComposedRuntime,
  DeclarationMetadata,
  EncodedEvent,
  EncodedState,
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
}
export interface ScenarioRecorder {
  snapshot(): RecordedScenario;
  /** Explicit cold boundary. No checkpoint is cloned on ordinary publication. */
  reset(): void;
  dispose(): void;
}
export function captureCheckpoint(runtime: ComposedRuntime): StateCheckpoint {
  if (!runtime.idle || runtime.disposed) throw new Error('Checkpoint requires a live, quiescent dispatch queue.');
  return Object.freeze({
    formatVersion: 1,
    schema: runtime.composition.metadata,
    states: Object.freeze(runtime.composition.states.map((state) => state.capture(runtime))),
  });
}
export function recordScenario(
  runtime: ComposedRuntime,
  { maxEvents = 1024 }: { readonly maxEvents?: number } = {},
): ScenarioRecorder {
  if (!Number.isSafeInteger(maxEvents) || maxEvents < 1)
    throw new RangeError('maxEvents must be a positive safe integer.');
  // Fail at boundary installation, not halfway through a production dispatch wave.
  for (const entry of runtime.composition.metadata)
    if (!entry.schema || entry.version === undefined)
      throw new Error(`Replay codec missing: ${entry.owner}/${entry.name}`);
  let checkpoint = captureCheckpoint(runtime);
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
          if (event?.kind === 'event') events.push(event.capture());
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
      checkpoint = captureCheckpoint(runtime);
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
    throw new Error('Incompatible checkpoint schema.');
  for (let index = 0; index < checkpoint.schema.length; index++) {
    const before = checkpoint.schema[index];
    const after = composition.metadata[index];
    if (
      before.key !== after.key ||
      before.kind !== after.kind ||
      before.schema !== after.schema ||
      before.version !== after.version
    )
      throw new Error('Incompatible checkpoint schema.');
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
    let previousWave = 0;
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

export interface RecordedEffectOutcome {
  readonly effect: string;
  readonly schema: string;
  readonly version: number;
  readonly value: unknown;
}
export function captureEffectOutcome<C, P extends EffectPlan, O, R>(
  definition: EffectDefinition<C, P, O, R>,
  outcome: EffectOutcome<P, O>,
): RecordedEffectOutcome {
  const codec = definition.codec;
  if (!codec) throw new Error('An effect outcome codec is required for capture.');
  return Object.freeze({
    effect: definition.metadata.key,
    schema: codec.schema,
    version: codec.version,
    value: codec.encode(outcome),
  });
}
export function decodeEffectOutcome<C, P extends EffectPlan, O, R>(
  definition: EffectDefinition<C, P, O, R>,
  outcome: RecordedEffectOutcome,
): EffectOutcome<P, O> {
  const codec = definition.codec;
  if (
    !codec ||
    definition.metadata.key !== outcome.effect ||
    codec.schema !== outcome.schema ||
    codec.version !== outcome.version
  )
    throw new Error('Incompatible effect outcome.');
  return codec.decode(outcome.value);
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
