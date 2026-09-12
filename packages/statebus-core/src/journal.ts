import type { ComposedRuntime, EncodedEvent, StateBusComposition, WaveObserver } from './composition.js';
import type { EffectDefinition, EffectOutcome, EffectPlan } from './effects.js';
import { CheckpointIndex, JournalQueue, jsonBytes, ownEntry, ownOutcomeValue, ownValue } from './journal-storage.js';
import {
  captureEffectOutcome,
  type RecordedEffectOutcome,
  type RecordedScenario,
  type RecordedWave,
  replayScenario,
} from './recording.js';
import { type CapturedEntry, type ResourceKey, type StateCapture, trackState, visitState } from './state-capture.js';

export interface JournalLimits {
  readonly maxEvents: number;
  /** Encoded retained wave array, including array framing. */
  readonly maxEventBytes: number;
  readonly maxOutcomes: number;
  /** Encoded outcome array, including array framing. */
  readonly maxOutcomeBytes: number;
  readonly maxCheckpointBytes: number;
  /** Maximum serialized JournalCapture at the explicit snapshot boundary. */
  readonly maxCaptureBytes: number;
}
export interface JournalOutcome {
  readonly sequence: number;
  /** Capture occurs before its decoded result is published into a successor wave. */
  readonly afterWave: number;
  readonly captured: RecordedEffectOutcome;
}
export interface JournalCapture {
  readonly formatVersion: 1;
  readonly checkpointWave: number;
  readonly lastWave: number;
  readonly scenario: RecordedScenario;
  readonly outcomes: readonly JournalOutcome[];
  /** Independent outcome-history truncation; state replay uses the result event journal. */
  readonly outcomesDropped: number;
}
export interface JournalUsage {
  readonly events: number;
  readonly eventBytes: number;
  readonly outcomes: number;
  readonly outcomeBytes: number;
  readonly checkpointBytes: number;
  readonly captureBytes: number;
}
export interface JournalRefusal {
  readonly code:
    | 'checkpoint-limit'
    | 'wave-limit'
    | 'outcome-limit'
    | 'capture-limit'
    | 'codec'
    | 'replay'
    | 'disposed';
  readonly wave: number;
  readonly owner?: string;
  readonly declaration?: string;
  readonly bytes?: number;
  readonly count?: number;
}
export type JournalSnapshot =
  | { readonly kind: 'recorded'; readonly capture: JournalCapture; readonly usage: JournalUsage }
  | { readonly kind: 'refused'; readonly reason: JournalRefusal };
export type JournalOutcomeReceipt =
  | { readonly kind: 'captured'; readonly sequence: number }
  | { readonly kind: 'refused'; readonly reason: JournalRefusal };
export interface JournalRecorder {
  readonly limits: JournalLimits;
  snapshot(): JournalSnapshot;
  captureOutcome<C, P extends EffectPlan, O, R>(
    definition: EffectDefinition<C, P, O, R>,
    outcome: EffectOutcome<NoInfer<P>, NoInfer<O>>,
  ): JournalOutcomeReceipt;
  /** Stops capture and disposes its replay cursor. Already retained records remain inspectable. */
  dispose(): void;
}

/**
 * Rolling lossless LOCAL capture. This is not a sanitized support export.
 * Application codecs must produce JSON-portable data and enforce per-payload allocation policies.
 * Byte limits cover encoded retained data, not arbitrary live state, transient encoders or total heap.
 */
export function recordJournal(runtime: ComposedRuntime, limits: Partial<JournalLimits> = {}): JournalRecorder {
  return new RollingJournal(runtime, {
    maxEvents: limits.maxEvents ?? defaultLimits.maxEvents,
    maxEventBytes: limits.maxEventBytes ?? defaultLimits.maxEventBytes,
    maxOutcomes: limits.maxOutcomes ?? defaultLimits.maxOutcomes,
    maxOutcomeBytes: limits.maxOutcomeBytes ?? defaultLimits.maxOutcomeBytes,
    maxCheckpointBytes: limits.maxCheckpointBytes ?? defaultLimits.maxCheckpointBytes,
    maxCaptureBytes: limits.maxCaptureBytes ?? defaultLimits.maxCaptureBytes,
  });
}
const defaultLimits: JournalLimits = Object.freeze({
  maxEvents: 1024,
  maxEventBytes: 1_048_576,
  maxOutcomes: 256,
  maxOutcomeBytes: 1_048_576,
  maxCheckpointBytes: 8_388_608,
  maxCaptureBytes: 12_582_912,
});
class RollingJournal implements JournalRecorder {
  readonly limits: JournalLimits;
  private readonly composition: StateBusComposition;
  private readonly checkpoint: CheckpointIndex;
  private readonly waves = new JournalQueue<RecordedWave>();
  private readonly outcomes = new JournalQueue<JournalOutcome>();
  private readonly trackers: StateCapture[] = [];
  private cursor: ComposedRuntime | undefined;
  private stop: () => void = () => {};
  private closed = false;
  private failure: JournalRefusal | undefined;
  private events = 0;
  private sequence = 0;
  private outcomesDropped = 0;
  private checkpointWave: number;
  private lastWave: number;

  constructor(
    private readonly runtime: ComposedRuntime,
    limits: JournalLimits,
  ) {
    for (const [name, value] of Object.entries(limits))
      if (!Number.isSafeInteger(value) || value < 1) throw new RangeError(`${name} must be a positive safe integer.`);
    if (limits.maxEventBytes < 2 || limits.maxOutcomeBytes < 2)
      throw new RangeError('Array byte limits must include their brackets.');
    if (!runtime.idle || runtime.disposed) throw new Error('Start journal capture at a live quiescent boundary.');
    this.limits = Object.freeze(limits);
    this.composition = runtime.composition;
    this.checkpointWave = runtime.waveNumber;
    this.lastWave = runtime.waveNumber;
    this.checkpoint = new CheckpointIndex(this.composition);
    if (this.checkpoint.bytes > limits.maxCheckpointBytes) {
      this.refuse({ code: 'checkpoint-limit', wave: this.lastWave, bytes: this.checkpoint.bytes });
      return;
    }
    for (const metadata of this.composition.metadata) {
      if (!metadata.schema || metadata.version === undefined) {
        this.refuse({ code: 'codec', wave: this.lastWave, owner: metadata.owner, declaration: metadata.name });
        return;
      }
    }
    try {
      for (let index = 0; index < this.composition.states.length; index++) {
        const state = this.composition.states[index];
        if (!state[visitState](runtime, (id, entry) => this.updateCheckpoint(index, id, entry))) return;
      }
      // Reuse the real reducer/replay implementation. It installs no effects or data loaders.
      this.cursor = replayScenario(this.composition, {
        formatVersion: 1,
        checkpoint: this.checkpoint.snapshot(),
        waves: [],
        complete: true,
      });
      for (const state of this.composition.states) this.trackers.push(state[trackState](this.cursor));
    } catch (cause) {
      this.refuse({ code: 'codec', wave: this.lastWave });
      runtime.reportError(cause);
      return;
    }
    const observer: WaveObserver = { committed: (wave, queue, count) => this.committed(wave, queue, count) };
    const stop = runtime.observeWaves(observer);
    this.stop = runtime.manage(() => {
      stop();
      this.closeCursor();
      this.closed = true;
    });
  }
  private closeCursor(): void {
    for (const tracker of this.trackers) tracker.dispose();
    this.trackers.length = 0;
    this.cursor?.dispose();
    this.cursor = undefined;
  }
  private refuse(reason: JournalRefusal): void {
    this.failure ??= Object.freeze(reason);
    this.stop();
    this.closeCursor();
    // A failed capture must not keep an oversized partial checkpoint or stale journal payloads.
    this.checkpoint.clear();
    this.waves.clear();
    this.outcomes.clear();
    this.events = 0;
  }
  private updateCheckpoint(
    index: number,
    id: ResourceKey,
    entry: CapturedEntry | undefined,
    wholeWave = false,
  ): boolean {
    const entryBytes = entry === undefined ? undefined : jsonBytes(entry);
    const bytes = this.checkpoint.entryBytes(index, id, entryBytes);
    if ((!wholeWave && bytes > this.limits.maxCheckpointBytes) || (entryBytes ?? 0) > this.limits.maxCheckpointBytes) {
      const metadata = this.composition.states[index].metadata;
      this.refuse({
        code: 'checkpoint-limit',
        wave: this.lastWave,
        bytes,
        owner: metadata.owner,
        declaration: metadata.name,
      });
      return false;
    }
    // Check the encoded bound before retaining or making a detached copy of the payload.
    this.checkpoint.set(
      index,
      id,
      entry === undefined ? undefined : { value: ownEntry(entry), bytes: entryBytes ?? 0 },
    );
    return true;
  }
  private advanceCheckpoint(): boolean {
    const removed = this.waves.peek();
    const cursor = this.cursor;
    if (!removed || !cursor) throw new Error('Cannot advance an empty rolling journal.');
    try {
      for (const event of removed.value.events) cursor.enqueueRecorded(event);
      cursor.flush();
      for (let index = 0; index < this.trackers.length; index++)
        if (!this.trackers[index].drain((id, entry) => this.updateCheckpoint(index, id, entry, true))) return false;
      // A whole-wave move may delete one entry and create another. Check its final total,
      // not an order-dependent transient prefix of that atomic transition.
      if (this.checkpoint.bytes > this.limits.maxCheckpointBytes) {
        this.refuse({ code: 'checkpoint-limit', wave: removed.value.wave, bytes: this.checkpoint.bytes });
        return false;
      }
      this.checkpointWave = removed.value.wave;
      this.events -= removed.value.events.length;
      this.waves.take();
      return true;
    } catch (cause) {
      this.refuse({ code: 'replay', wave: removed.value.wave });
      this.runtime.reportError(cause);
      return false;
    }
  }
  private committed(wave: number, queue: Parameters<WaveObserver['committed']>[1], count: number): void {
    if (this.failure || this.closed) return;
    this.lastWave = wave;
    const events: EncodedEvent[] = [];
    let bytes = jsonBytes({ wave, events });
    try {
      for (let index = 0; index < count; index++) {
        const event = queue[index];
        if (event?.kind !== 'event') continue;
        if (events.length === this.limits.maxEvents) {
          this.refuse({ code: 'wave-limit', wave, count: events.length + 1 });
          return;
        }
        const captured = event.capture();
        bytes += jsonBytes(captured) + (events.length === 0 ? 0 : 1);
        if (bytes + 2 > this.limits.maxEventBytes) {
          this.refuse({ code: 'wave-limit', wave, bytes: bytes + 2, count: events.length + 1 });
          return;
        }
        events.push(Object.freeze({ key: captured.key, payload: ownValue(captured.payload) }));
      }
      if (events.length === 0) return;
      const retained: RecordedWave = Object.freeze({ wave, events: Object.freeze(events) });
      while (
        this.events + events.length > this.limits.maxEvents ||
        this.waves.appendedBytes(bytes) > this.limits.maxEventBytes
      )
        if (!this.advanceCheckpoint()) return;
      this.waves.push(retained, bytes);
      this.events += events.length;
    } catch (cause) {
      this.refuse({ code: 'codec', wave });
      this.runtime.reportError(cause);
    }
  }
  captureOutcome<C, P extends EffectPlan, O, R>(
    definition: EffectDefinition<C, P, O, R>,
    outcome: EffectOutcome<NoInfer<P>, NoInfer<O>>,
  ): JournalOutcomeReceipt {
    if (this.failure) return { kind: 'refused', reason: this.failure };
    if (this.closed) return { kind: 'refused', reason: { code: 'disposed', wave: this.lastWave } };
    this.runtime.assertOwner(definition.command.ownerToken);
    this.runtime.assertOwner(definition.result.ownerToken);
    try {
      const captured = captureEffectOutcome(definition, outcome);
      const sequence = this.sequence + 1;
      const bytes = jsonBytes({ sequence, afterWave: this.lastWave, captured });
      if (bytes + 2 > this.limits.maxOutcomeBytes) {
        this.refuse({ code: 'outcome-limit', wave: this.lastWave, bytes: bytes + 2, owner: definition.metadata.owner });
      } else {
        while (
          this.outcomes.length === this.limits.maxOutcomes ||
          this.outcomes.appendedBytes(bytes) > this.limits.maxOutcomeBytes
        ) {
          this.outcomes.take();
          this.outcomesDropped++;
        }
        const entry: JournalOutcome = Object.freeze({
          sequence,
          afterWave: this.lastWave,
          captured: Object.freeze({ ...captured, value: ownOutcomeValue(captured.value) }),
        });
        this.outcomes.push(entry, bytes);
        this.sequence = sequence;
        return { kind: 'captured', sequence };
      }
    } catch (cause) {
      this.refuse({
        code: 'codec',
        wave: this.lastWave,
        owner: definition.metadata.owner,
        declaration: definition.metadata.key,
      });
      this.runtime.reportError(cause);
    }
    if (!this.failure) throw new Error('Missing outcome capture refusal.');
    return { kind: 'refused', reason: this.failure };
  }
  snapshot(): JournalSnapshot {
    if (this.failure) return { kind: 'refused', reason: this.failure };
    // Exact envelope framing plus incrementally maintained section sizes. Refuse before
    // materializing a potentially oversized checkpoint/capture, not after cloning the app.
    const framing = jsonBytes({
      formatVersion: 1,
      checkpointWave: this.checkpointWave,
      lastWave: this.lastWave,
      scenario: { formatVersion: 1, checkpoint: null, waves: [], complete: true },
      outcomes: [],
      outcomesDropped: this.outcomesDropped,
    });
    const captureBytes = framing - 8 + this.checkpoint.bytes + this.waves.bytes + this.outcomes.bytes;
    if (captureBytes > this.limits.maxCaptureBytes)
      return { kind: 'refused', reason: { code: 'capture-limit', wave: this.lastWave, bytes: captureBytes } };
    const capture: JournalCapture = Object.freeze({
      formatVersion: 1,
      checkpointWave: this.checkpointWave,
      lastWave: this.lastWave,
      scenario: Object.freeze({
        formatVersion: 1,
        checkpoint: this.checkpoint.snapshot(),
        waves: this.waves.snapshot(),
        complete: true,
      }),
      outcomes: this.outcomes.snapshot(),
      outcomesDropped: this.outcomesDropped,
    });
    return Object.freeze({
      kind: 'recorded',
      capture,
      usage: Object.freeze({
        events: this.events,
        eventBytes: this.waves.bytes,
        outcomes: this.outcomes.length,
        outcomeBytes: this.outcomes.bytes,
        checkpointBytes: this.checkpoint.bytes,
        captureBytes,
      }),
    });
  }
  dispose(): void {
    if (this.closed) return;
    this.stop();
    this.closeCursor();
    this.closed = true;
  }
}
