import { CaptureError, type CaptureIssue, captureBytes, captureLimit, compareResourceIds } from './capture.js';
import type { ComposedRuntime, EncodedStateEntry, StateDeclaration, WaveObserver } from './composition.js';
import type { EffectDefinition, EffectOutcome, EffectPlan } from './effects.js';
import {
  captureCheckpoint,
  captureEffectInstruction,
  captureEffectOutcome,
  ownEntry,
  ownEvent,
  type RecordedEffectCapture,
  type RecordedScenario,
  type RecordedWave,
  replayScenario,
  type StateCheckpoint,
} from './recording.js';

export interface RollingCaptureLimits {
  readonly maxEvents: number;
  readonly maxEventBytes: number;
  /** Includes captured instructions/outcomes and their causal framing, not just payloads. */
  readonly maxEffectBytes: number;
  readonly maxCheckpointBytes: number;
  readonly maxCaptureBytes: number;
  readonly maxEntryBytes: number;
}
const defaults: RollingCaptureLimits = Object.freeze({
  maxEvents: 1024,
  maxEventBytes: 2 * 1024 * 1024,
  maxEffectBytes: 1024 * 1024,
  maxCheckpointBytes: 4 * 1024 * 1024,
  maxCaptureBytes: 8 * 1024 * 1024,
  maxEntryBytes: 256 * 1024,
});

export interface RecordedEffectPosition {
  readonly sequence: number;
  /** Last committed live wave at outcome capture. Result events, if any, follow in a successor wave. */
  readonly afterWave: number;
  readonly capture: RecordedEffectCapture;
}
export interface RollingScenario extends RecordedScenario {
  readonly checkpointWave: number;
  readonly effects: readonly RecordedEffectPosition[];
  /** Outcome retention is separate from state replay: events already contain decoded result facts. */
  readonly evictedEffects: number;
}
export interface RollingCaptureStats {
  readonly events: number;
  readonly eventBytes: number;
  readonly effectBytes: number;
  readonly checkpointBytes: number;
  readonly checkpointWave: number;
  readonly evictedWaves: number;
  readonly evictedEffects: number;
  readonly encodedCheckpointEntries: number;
  readonly checkpointMaterializations: number;
  readonly refusal?: CaptureIssue;
}
export interface RollingScenarioRecorder {
  readonly limits: RollingCaptureLimits;
  snapshot(): RollingScenario;
  stats(): RollingCaptureStats;
  outcomeSink<C, P extends EffectPlan, O, R>(
    definition: EffectDefinition<C, P, O, R>,
  ): (outcome: EffectOutcome<P, O>) => void;
  instructionSink<C, P extends EffectPlan, O, R>(definition: EffectDefinition<C, P, O, R>): (plan: P) => void;
  reset(): void;
  dispose(): void;
}

/** A cursor queue releases retired references; compaction is amortized, never a shift per publication. */
class Journal<T> {
  private items: (T | undefined)[] = [];
  private start = 0;
  get length(): number {
    return this.items.length - this.start;
  }
  push(item: T): void {
    this.items.push(item);
  }
  take(): T | undefined {
    if (this.start === this.items.length) return undefined;
    const item = this.items[this.start];
    this.items[this.start++] = undefined;
    if (this.start >= 64 && this.start * 2 >= this.items.length) {
      this.items = this.items.slice(this.start);
      this.start = 0;
    }
    return item;
  }
  values(): T[] {
    const values: T[] = [];
    for (let index = this.start; index < this.items.length; index++) {
      const item = this.items[index];
      if (item !== undefined) values.push(item);
    }
    return values;
  }
  clear(): void {
    this.items.length = 0;
    this.start = 0;
  }
}
interface Sized<T> {
  readonly value: T;
  readonly bytes: number;
}
interface CellIndex {
  readonly declaration: StateDeclaration;
  readonly entries: Map<string | number | undefined, Sized<EncodedStateEntry>>;
}

/**
 * One normal execution-disabled runtime interprets ONLY evicted whole waves. A changed-cell index
 * advances the retained checkpoint without capturing/cloning the application on each publish.
 * Unrecorded runtimes have no journal, dirty-address map, serialization or migration work.
 * Limits count canonical JSON UTF-8 bytes; they are not a JS heap/GC bound or a bound on codec allocations.
 */
export function recordRollingScenario(
  runtime: ComposedRuntime,
  options: Partial<RollingCaptureLimits> = {},
): RollingScenarioRecorder {
  const limits = Object.freeze({ ...defaults, ...options });
  for (const [name, value] of Object.entries(limits)) captureLimit(value, name);
  for (const metadata of runtime.composition.metadata)
    if (!metadata.schema || metadata.version === undefined)
      throw new CaptureError({
        code: 'schema',
        boundary: 'missing capture codec',
        owner: metadata.owner,
        declaration: metadata.name,
      });
  const journal = new Journal<Sized<RecordedWave>>();
  const effects = new Journal<Sized<RecordedEffectPosition>>();
  const cells = new Map<string, CellIndex>();
  const dirty = new Map<StateDeclaration, Set<string | number | undefined>>();
  const dirtyStates: (StateDeclaration | undefined)[] = [];
  let dirtyCount = 0;
  let mirror: ComposedRuntime;
  let checkpointWave = 0;
  let eventCount = 0;
  let eventBytes = 0;
  let effectBytes = 0;
  let checkpointBytes = 0;
  let evictedWaves = 0;
  let evictedEffects = 0;
  let encodedEntries = 0;
  let materializations = 0;
  let sequence = 0;
  let refusal: CaptureIssue | undefined;
  let disposed = false;

  function bound(bytes: number, limit: number, boundary: string): void {
    if (bytes > limit) throw new CaptureError({ code: 'size-limit', boundary, limit });
  }
  function sizedEntry(entry: EncodedStateEntry): Sized<EncodedStateEntry> {
    const value = ownEntry(entry, limits.maxEntryBytes);
    return { value, bytes: captureBytes(value, limits.maxEntryBytes) };
  }
  function checkpoint(): StateCheckpoint {
    materializations++;
    return Object.freeze({
      formatVersion: 1,
      schema: runtime.composition.metadata,
      states: Object.freeze(
        [...cells].map(([key, cell]) =>
          Object.freeze({
            key,
            entries: Object.freeze(
              [...cell.entries]
                .sort(([a], [b]) => (a === undefined || b === undefined ? 0 : compareResourceIds(a, b)))
                .map(([, entry]) => entry.value),
            ),
          }),
        ),
      ),
    });
  }
  function initialize(): ComposedRuntime {
    const initial = captureCheckpoint(runtime, {
      maxBytes: limits.maxCheckpointBytes,
      maxEntryBytes: limits.maxEntryBytes,
    });
    cells.clear();
    dirty.clear();
    dirtyStates.length = 0;
    dirtyCount = 0;
    checkpointBytes = captureBytes(
      { ...initial, states: initial.states.map((state) => ({ key: state.key, entries: [] })) },
      limits.maxCheckpointBytes,
    );
    for (const declaration of runtime.composition.states) {
      const state = initial.states.find((entry) => entry.key === declaration.metadata.key);
      if (!state) throw new Error('Missing initial checkpoint declaration.');
      const entries: CellIndex['entries'] = new Map();
      cells.set(state.key, { declaration, entries });
      for (const entry of state.entries) {
        const owned = sizedEntry(entry);
        checkpointBytes += owned.bytes + (entries.size > 0 ? 1 : 0);
        bound(checkpointBytes, limits.maxCheckpointBytes, 'checkpoint');
        entries.set(declaration.entryId(entry), owned);
        encodedEntries++;
      }
    }
    checkpointWave = runtime.waveNumber;
    const replay = replayScenario(runtime.composition, {
      formatVersion: 1,
      checkpoint: checkpoint(),
      checkpointWave,
      waves: [],
      complete: true,
    });
    replay.observeCapturedWrites((state, id) => {
      let ids = dirty.get(state);
      if (!ids) {
        ids = new Set();
        dirty.set(state, ids);
      }
      if (ids.size === 0) dirtyStates[dirtyCount++] = state;
      ids.add(id);
    });
    return replay;
  }
  mirror = initialize();

  function refuse(cause: unknown): void {
    refusal =
      cause instanceof CaptureError
        ? cause.issue
        : Object.freeze({ code: 'incomplete', boundary: 'checkpoint interpreter' });
    // Do not keep an unbounded/invalid interpreter alive after a refused checkpoint.
    mirror.dispose();
    dirty.clear();
    dirtyStates.length = 0;
    dirtyCount = 0;
    runtime.reportError(new CaptureError(refusal, { cause }));
  }
  function advance(wave: RecordedWave): void {
    for (const event of wave.events) mirror.enqueueRecorded(event);
    mirror.flush();
    for (let dirtyPosition = 0; dirtyPosition < dirtyCount; dirtyPosition++) {
      const declaration = dirtyStates[dirtyPosition];
      if (!declaration) throw new Error('Missing dirty declaration.');
      const ids = dirty.get(declaration);
      if (!ids) throw new Error('Missing dirty address set.');
      const index = cells.get(declaration.metadata.key);
      if (!index) throw new Error('Unknown checkpoint write.');
      for (const id of ids) {
        const before = index.entries.get(id);
        const next = declaration.captureEntry(mirror, id);
        const after = next && sizedEntry(next);
        if (before) {
          checkpointBytes -= before.bytes + (index.entries.size > 1 ? 1 : 0);
          index.entries.delete(id);
        }
        if (after) {
          checkpointBytes += after.bytes + (index.entries.size > 0 ? 1 : 0);
          bound(checkpointBytes, limits.maxCheckpointBytes, 'rolling checkpoint');
          index.entries.set(id, after);
          encodedEntries++;
        }
      }
      ids.clear();
      dirtyStates[dirtyPosition] = undefined;
    }
    dirtyCount = 0;
    checkpointWave = wave.wave;
    evictedWaves++;
  }
  function retainEffect(capture: RecordedEffectCapture): void {
    const value: RecordedEffectPosition = Object.freeze({
      sequence: ++sequence,
      afterWave: runtime.waveNumber,
      capture,
    });
    const bytes = captureBytes(value, Math.min(limits.maxEntryBytes, limits.maxEffectBytes));
    while (effectBytes + bytes > limits.maxEffectBytes) {
      const removed = effects.take();
      if (!removed) throw new Error('Invalid effect journal accounting.');
      effectBytes -= removed.bytes;
      evictedEffects++;
    }
    effects.push({ value, bytes });
    effectBytes += bytes;
  }
  const observer: WaveObserver = {
    committed(wave, queue, length) {
      if (refusal || disposed) return;
      try {
        const events = [];
        let bytes = captureBytes({ wave, events: [] }, limits.maxEventBytes);
        for (let index = 0; index < length; index++) {
          const event = queue[index];
          if (event?.kind === 'event') {
            bound(events.length + 1, limits.maxEvents, 'whole wave event count');
            if (bytes >= limits.maxEventBytes)
              throw new CaptureError({ code: 'size-limit', boundary: 'whole wave bytes', limit: limits.maxEventBytes });
            const owned = ownEvent(event.capture(), Math.min(limits.maxEntryBytes, limits.maxEventBytes - bytes));
            bytes += captureBytes(owned, limits.maxEntryBytes) + (events.length > 0 ? 1 : 0);
            bound(bytes, limits.maxEventBytes, 'whole wave bytes');
            events.push(owned);
          }
        }
        if (events.length === 0) return;
        const value: RecordedWave = Object.freeze({ wave, events: Object.freeze(events) });
        // Evict BEFORE append; an oversized incoming whole wave is refused without dropping its prefix.
        while (eventCount + events.length > limits.maxEvents || eventBytes + bytes > limits.maxEventBytes) {
          const removed = journal.take();
          if (!removed) throw new Error('Invalid journal accounting.');
          advance(removed.value);
          eventCount -= removed.value.events.length;
          eventBytes -= removed.bytes;
        }
        journal.push({ value, bytes });
        eventCount += events.length;
        eventBytes += bytes;
      } catch (cause) {
        refuse(cause);
      }
    },
  };
  const stop = runtime.observeWaves(observer);
  const dispose = runtime.manage(() => {
    if (disposed) return;
    disposed = true;
    stop();
    mirror.dispose();
    dirty.clear();
    dirtyStates.length = 0;
    dirtyCount = 0;
  });
  return Object.freeze({
    limits,
    stats: () =>
      Object.freeze({
        events: eventCount,
        eventBytes,
        effectBytes,
        checkpointBytes,
        checkpointWave,
        evictedWaves,
        evictedEffects,
        encodedCheckpointEntries: encodedEntries,
        checkpointMaterializations: materializations,
        refusal,
      }),
    snapshot() {
      if (refusal) throw new CaptureError(refusal);
      if (!runtime.disposed && !runtime.idle) throw new Error('Snapshot requires a quiescent event queue.');
      // This is a user-requested cold materialization, not a per-wave whole-state copy.
      bound(checkpointBytes + eventBytes + effectBytes, limits.maxCaptureBytes, 'capture envelope');
      const value: RollingScenario = Object.freeze({
        formatVersion: 1,
        checkpoint: checkpoint(),
        checkpointWave,
        waves: Object.freeze(journal.values().map((entry) => entry.value)),
        effects: Object.freeze(effects.values().map((entry) => entry.value)),
        evictedEffects,
        complete: true,
      });
      captureBytes(value, limits.maxCaptureBytes);
      return value;
    },
    outcomeSink<C, P extends EffectPlan, O, R>(definition: EffectDefinition<C, P, O, R>) {
      runtime.assertOwner(definition.command.ownerToken);
      if (!definition.codec) throw new Error('An outcome sink requires an effect codec.');
      return (outcome: EffectOutcome<P, O>): void => {
        if (refusal || disposed) return;
        try {
          retainEffect(
            captureEffectOutcome(definition, outcome, Math.min(limits.maxEntryBytes, limits.maxEffectBytes)),
          );
        } catch (cause) {
          refuse(cause);
        }
      };
    },
    instructionSink<C, P extends EffectPlan, O, R>(definition: EffectDefinition<C, P, O, R>) {
      runtime.assertOwner(definition.command.ownerToken);
      if (!definition.instructionCodec) throw new Error('An instruction sink requires an instruction codec.');
      return (plan: P): void => {
        if (refusal || disposed) return;
        try {
          retainEffect(
            captureEffectInstruction(definition, plan, Math.min(limits.maxEntryBytes, limits.maxEffectBytes)),
          );
        } catch (cause) {
          refuse(cause);
        }
      };
    },
    reset() {
      if (disposed) throw new Error('Recorder is disposed.');
      if (!runtime.idle) throw new Error('Reset requires a quiescent event queue.');
      mirror.dispose();
      journal.clear();
      effects.clear();
      eventCount =
        eventBytes =
        effectBytes =
        evictedWaves =
        evictedEffects =
        encodedEntries =
        materializations =
        sequence =
          0;
      refusal = undefined;
      try {
        mirror = initialize();
      } catch (cause) {
        refuse(cause);
        throw cause;
      }
    },
    dispose,
  });
}
