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
  /** Maximum retained instruction/outcome records; reserves queue slots at setup. Default: 256. */
  readonly maxEffects: number;
  /** Includes captured instructions/outcomes and their causal framing, not just payloads. */
  readonly maxEffectBytes: number;
  readonly maxCheckpointBytes: number;
  readonly maxCaptureBytes: number;
  readonly maxEntryBytes: number;
}
const defaults: RollingCaptureLimits = Object.freeze({
  maxEvents: 1024,
  maxEventBytes: 2 * 1024 * 1024,
  maxEffects: 256,
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
  readonly effects: number;
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

/** Fixed queue storage. Retiring a record never copies the suffix or allocates a size wrapper. */
class Journal<T extends object> {
  private readonly items: (T | undefined)[] = [];
  private readonly sizes: Float64Array;
  private start = 0;
  private count = 0;
  private retainedBytes = 0;

  constructor(private readonly capacity: number) {
    if (!Number.isSafeInteger(capacity) || capacity < 1 || capacity > 0xffff_ffff)
      throw new RangeError('Journal capacity must fit the array index range.');
    // Initialize actual elements rather than holes. These two buffers never grow or shrink.
    this.sizes = new Float64Array(capacity);
    for (let index = 0; index < capacity; index++) this.items.push(undefined);
  }
  get length(): number {
    return this.count;
  }
  get bytes(): number {
    return this.retainedBytes;
  }
  push(item: T, bytes: number): void {
    if (this.count === this.capacity) throw new Error('Journal capacity invariant violated.');
    let index = this.start + this.count;
    if (index >= this.capacity) index -= this.capacity;
    this.items[index] = item;
    this.sizes[index] = bytes;
    this.retainedBytes += bytes;
    this.count++;
  }
  take(): T | undefined {
    if (this.count === 0) return undefined;
    const item = this.items[this.start];
    if (item === undefined) throw new Error('Missing retained journal record.');
    this.items[this.start] = undefined;
    this.retainedBytes -= this.sizes[this.start];
    this.sizes[this.start] = 0;
    this.start++;
    if (this.start === this.capacity) this.start = 0;
    this.count--;
    return item;
  }
  values(): readonly T[] {
    const values: T[] = [];
    let position = this.start;
    for (let index = 0; index < this.count; index++) {
      const item = this.items[position];
      if (item === undefined) throw new Error('Missing retained journal record.');
      values.push(item);
      position++;
      if (position === this.capacity) position = 0;
    }
    return Object.freeze(values);
  }
  clear(): void {
    for (let remaining = this.count; remaining > 0; remaining--) this.take();
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
function compareCellIds(left: string | number | undefined, right: string | number | undefined): number {
  return left === undefined || right === undefined ? 0 : compareResourceIds(left, right);
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
  // Every retained wave has at least one event, so maxEvents also bounds wave slots.
  const journal = new Journal<RecordedWave>(limits.maxEvents);
  const effects = new Journal<RecordedEffectPosition>(limits.maxEffects);
  const cells = new Map<string, CellIndex>();
  const dirty = new Map<StateDeclaration, Set<string | number | undefined>>();
  const dirtyStates: (StateDeclaration | undefined)[] = [];
  let dirtyCount = 0;
  let mirror: ComposedRuntime;
  let checkpointWave = 0;
  let eventCount = 0;
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
    const states = [];
    for (const [key, cell] of cells) {
      const entries: EncodedStateEntry[] = [];
      // Sort primitive keys, not temporary [key, value] pairs for every cell.
      const ids = [...cell.entries.keys()].sort(compareCellIds);
      for (const id of ids) {
        const entry = cell.entries.get(id);
        if (!entry) throw new Error('Missing checkpoint cell.');
        entries.push(entry.value);
      }
      states.push(Object.freeze({ key, entries: Object.freeze(entries) }));
    }
    return Object.freeze({
      formatVersion: 1,
      schema: runtime.composition.metadata,
      states: Object.freeze(states),
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
    for (let position = 0; position < runtime.composition.states.length; position++) {
      const declaration = runtime.composition.states[position];
      const state = initial.states[position];
      if (!state || state.key !== declaration.metadata.key)
        throw new Error('Initial checkpoint declaration order differs from composition.');
      const entries: CellIndex['entries'] = new Map();
      cells.set(state.key, { declaration, entries });
      for (const entry of state.entries) {
        // captureCheckpoint already detached and froze these entries. Do not copy them again.
        const bytes = captureBytes(entry, limits.maxEntryBytes);
        checkpointBytes += bytes + (entries.size > 0 ? 1 : 0);
        bound(checkpointBytes, limits.maxCheckpointBytes, 'checkpoint');
        entries.set(declaration.entryId(entry), { value: entry, bytes });
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
    mirror.dispose();
    dirty.clear();
    dirtyStates.length = 0;
    dirtyCount = 0;
    // Refusal must release oversized/partial retained data, not merely hide it behind snapshot().
    cells.clear();
    journal.clear();
    effects.clear();
    eventCount = 0;
    checkpointBytes = 0;
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
        if (after) {
          bound(after.bytes, limits.maxCheckpointBytes, 'rolling checkpoint entry');
          checkpointBytes += after.bytes - (before?.bytes ?? 0) + (!before && index.entries.size > 0 ? 1 : 0);
          // Replacing an existing cell does not delete/reinsert its Map key.
          index.entries.set(id, after);
          encodedEntries++;
        } else if (before) {
          checkpointBytes -= before.bytes + (index.entries.size > 1 ? 1 : 0);
          index.entries.delete(id);
        }
      }
      ids.clear();
      dirtyStates[dirtyPosition] = undefined;
    }
    dirtyCount = 0;
    // A successful wave is atomic. Add-before-delete relocation must be checked at its final size,
    // not refused because a transient prefix happens to contain both the old and new addresses.
    bound(checkpointBytes, limits.maxCheckpointBytes, 'rolling checkpoint');
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
    while (effects.length === limits.maxEffects || effects.bytes + bytes > limits.maxEffectBytes) {
      if (!effects.take()) throw new Error('Invalid effect journal accounting.');
      evictedEffects++;
    }
    effects.push(value, bytes);
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
        while (eventCount + events.length > limits.maxEvents || journal.bytes + bytes > limits.maxEventBytes) {
          const removed = journal.take();
          if (!removed) throw new Error('Invalid journal accounting.');
          advance(removed);
          eventCount -= removed.events.length;
        }
        journal.push(value, bytes);
        eventCount += events.length;
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
        eventBytes: journal.bytes,
        effects: effects.length,
        effectBytes: effects.bytes,
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
      const framing = captureBytes({
        formatVersion: 1,
        checkpoint: null,
        checkpointWave,
        waves: [],
        effects: [],
        evictedEffects,
        complete: true,
      });
      // Replace null (4 bytes) and add the retained array contents/commas. Section sizes are
      // already known: refuse the exact whole envelope before materializing any checkpoint arrays.
      const bytes =
        framing -
        4 +
        checkpointBytes +
        journal.bytes +
        Math.max(0, journal.length - 1) +
        effects.bytes +
        Math.max(0, effects.length - 1);
      bound(bytes, limits.maxCaptureBytes, 'capture envelope');
      return Object.freeze({
        formatVersion: 1,
        checkpoint: checkpoint(),
        checkpointWave,
        waves: journal.values(),
        effects: effects.values(),
        evictedEffects,
        complete: true,
      });
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
      eventCount = evictedWaves = evictedEffects = encodedEntries = materializations = sequence = 0;
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
