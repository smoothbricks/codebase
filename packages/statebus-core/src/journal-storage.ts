import type { DeclarationMetadata, EncodedState, EncodedValue, StateBusComposition } from './composition.js';
import type { StateCheckpoint } from './recording.js';
import { type CapturedEntry, compareResourceKeys, type ResourceKey } from './state-capture.js';

const utf8 = new TextEncoder();
export function jsonBytes(value: unknown): number {
  const text = JSON.stringify(value);
  if (text === undefined) throw new Error('A capture must have a JSON representation.');
  return utf8.encode(text).byteLength;
}
function freezeJSON(value: unknown): unknown {
  if (value !== null && typeof value === 'object') {
    const children: readonly unknown[] = Object.values(value);
    for (const child of children) freezeJSON(child);
    Object.freeze(value);
  }
  return value;
}
/** The application codec owns JSON round-trip fidelity. Only its encoded data crosses this boundary. */
export function ownValue(value: EncodedValue): EncodedValue {
  const text = JSON.stringify(value.value);
  // JSON omits undefined object properties; a codec accepting undefined must handle that on decode.
  if (text === undefined && value.value !== undefined) throw new Error('Nonportable codec output.');
  const payload: unknown = text === undefined ? undefined : JSON.parse(text);
  return Object.freeze({ ...value, value: freezeJSON(payload) });
}
export function ownEntry(entry: CapturedEntry): CapturedEntry {
  return Object.freeze(
    entry.id === undefined
      ? { value: ownValue(entry.value) }
      : { id: ownValue(entry.id), value: ownValue(entry.value) },
  );
}
export function ownOutcomeValue(value: unknown): unknown {
  const text = JSON.stringify(value);
  if (text === undefined) throw new Error('An outcome codec must produce portable data.');
  const detached: unknown = JSON.parse(text);
  return freezeJSON(detached);
}
export interface Sized<T> {
  readonly value: T;
  readonly bytes: number;
}
/** Retains capacity without retaining evicted payloads or shifting the surviving suffix. */
export class JournalQueue<T> {
  private slots: (Sized<T> | undefined)[] = new Array<Sized<T> | undefined>(8).fill(undefined);
  private offset = 0;
  private count = 0;
  private payloadBytes = 0;
  get length(): number {
    return this.count;
  }
  /** Exact JSON array bytes, including brackets and commas. */
  get bytes(): number {
    return 2 + this.payloadBytes + Math.max(0, this.count - 1);
  }
  appendedBytes(bytes: number): number {
    return this.bytes + bytes + (this.count === 0 ? 0 : 1);
  }
  peek(): Sized<T> | undefined {
    return this.slots[this.offset];
  }
  push(value: T, bytes: number): void {
    if (this.count === this.slots.length) {
      const next = new Array<Sized<T> | undefined>(this.slots.length * 2).fill(undefined);
      for (let index = 0; index < this.count; index++)
        next[index] = this.slots[(this.offset + index) % this.slots.length];
      this.slots = next;
      this.offset = 0;
    }
    this.slots[(this.offset + this.count) % this.slots.length] = { value, bytes };
    this.payloadBytes += bytes;
    this.count++;
  }
  take(): Sized<T> | undefined {
    if (this.count === 0) return undefined;
    const entry = this.slots[this.offset];
    if (!entry) throw new Error('Invalid journal queue.');
    this.slots[this.offset] = undefined;
    this.offset = (this.offset + 1) % this.slots.length;
    this.payloadBytes -= entry.bytes;
    this.count--;
    return entry;
  }
  snapshot(): readonly T[] {
    const values: T[] = [];
    for (let index = 0; index < this.count; index++) {
      const entry = this.slots[(this.offset + index) % this.slots.length];
      if (!entry) throw new Error('Invalid journal queue.');
      values.push(entry.value);
    }
    return Object.freeze(values);
  }
  clear(): void {
    while (this.count > 0) this.take();
  }
}
interface StateIndex {
  readonly key: string;
  readonly entries: Map<ResourceKey, Sized<CapturedEntry>>;
}
export class CheckpointIndex {
  private readonly states: StateIndex[];
  private readonly schema: readonly DeclarationMetadata[];
  bytes: number;
  constructor(composition: StateBusComposition) {
    this.schema = Object.freeze(composition.metadata.map((entry) => Object.freeze({ ...entry })));
    this.states = composition.states.map((state) => ({ key: state.metadata.key, entries: new Map() }));
    // Metadata/declaration framing is sized once, not every time the checkpoint advances.
    this.bytes = jsonBytes(this.snapshot());
  }
  /** Stage each encoded entry before replacing the index; the caller enforces the byte bound. */
  entryBytes(index: number, id: ResourceKey, bytes: number | undefined): number {
    const state = this.states[index];
    if (!state) throw new Error('Unknown checkpoint declaration.');
    const previous = state.entries.get(id);
    if (bytes === undefined) return this.bytes - (previous ? previous.bytes + (state.entries.size > 1 ? 1 : 0) : 0);
    return this.bytes + bytes - (previous?.bytes ?? 0) + (!previous && state.entries.size > 0 ? 1 : 0);
  }
  set(index: number, id: ResourceKey, entry: Sized<CapturedEntry> | undefined): void {
    this.bytes = this.entryBytes(index, id, entry?.bytes);
    if (entry) this.states[index].entries.set(id, entry);
    else this.states[index].entries.delete(id);
  }
  clear(): void {
    for (const state of this.states) state.entries.clear();
    this.bytes = jsonBytes(this.snapshot());
  }
  snapshot(): StateCheckpoint {
    const states: EncodedState[] = this.states.map((state) => {
      const entries: CapturedEntry[] = [];
      for (const id of [...state.entries.keys()].sort(compareResourceKeys)) {
        const entry = state.entries.get(id);
        if (!entry) throw new Error('Invalid checkpoint index.');
        entries.push(entry.value);
      }
      return Object.freeze({ key: state.key, entries: Object.freeze(entries) });
    });
    return Object.freeze({ formatVersion: 1, schema: this.schema, states: Object.freeze(states) });
  }
}
