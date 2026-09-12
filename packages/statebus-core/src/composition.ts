import { atom, computed as signalComputed, react, transaction, type Atom, type Signal } from '@tldraw/state';
import type { CapturedOutcome } from './effect.js';
import { DispatchQueue } from './dispatch-queue.js';
import { StateInterestBatch, StateInterestRegistry, type StateInterest, type StateInterestChange } from './interest.js';
import { microtaskScheduler, type DispatchScheduler } from './scheduler.js';

export type ResourceId = string | number;
export interface ResourceInterest<ID extends ResourceId> extends StateInterest { readonly id: ID }
export type SupportClassification = 'replayable' | 'redacted' | 'unsupported';
/** The application owns validation, migrations, and redaction at this boundary. */
export interface StateBusCodec<T> {
  readonly schema: string;
  readonly version: number;
  encode(value: T): unknown;
  decode(value: unknown, version: number): T;
  classify?(value: T): SupportClassification;
}
export interface HandleDescription { readonly key: string; readonly kind: 'state' | 'event' | 'effect'; readonly mount: string; readonly library: string }
export interface RecordingOptions { readonly maxWaves?: number; readonly maxEvents?: number; readonly maxOutcomes?: number }
export interface HandleMetadata<T> {
  readonly codec?: StateBusCodec<T>;
  readonly description?: string;
}
export interface SavedValue { readonly schema: string; readonly version: number; readonly value: unknown }
export interface SavedState { readonly key: string; readonly values: readonly { readonly id?: SavedValue; readonly data: SavedValue }[] }
export interface RecordedEvent { readonly key: string; readonly data: SavedValue }
export interface StateBusCheckpoint { readonly version: 1; readonly composition: readonly string[]; readonly states: readonly SavedState[] }
export interface StateBusRecording { readonly version: 1; readonly checkpoint: StateBusCheckpoint; readonly waves: readonly (readonly RecordedEvent[])[]; readonly outcomes: readonly CapturedOutcome[] }

function encode<T>(codec: StateBusCodec<T> | undefined, value: T): SavedValue {
  if (!codec || (codec.classify?.(value) ?? 'replayable') !== 'replayable') {
    throw new Error('A replayable codec is required for every captured value.');
  }
  return { schema: codec.schema, version: codec.version, value: structuredClone(codec.encode(value)) };
}
function decode<T>(codec: StateBusCodec<T> | undefined, saved: SavedValue): T {
  if (!codec || codec.schema !== saved.schema) throw new Error('Incompatible StateBus codec schema.');
  return codec.decode(saved.value, saved.version);
}

const createCell = Symbol('createCell');
const cellAccess = Symbol('cellAccess');
const writerAccess = Symbol('writerAccess');
const enqueueMessage = Symbol('enqueueMessage');

interface StateSlot {
  capture(): SavedState;
  restore(value: SavedState): () => void;
}
export abstract class StateDeclaration {
  abstract [createCell](scope: LibraryScope, key: string): StateSlot;
}
/** Handles carry types, not runtime state. Each scope gets independent cells. */
export class ScalarHandle<T> extends StateDeclaration {
  private readonly cells = new WeakMap<LibraryScope, Atom<T>>();
  private readonly addresses = new WeakMap<LibraryScope, StateInterest>();
  constructor(readonly initial: () => T, readonly metadata: HandleMetadata<T> = {}) { super(); Object.freeze(metadata); if (metadata.codec) Object.freeze(metadata.codec); }
  override [createCell](scope: LibraryScope, key: string): StateSlot {
    const cell = atom(key, this.initial());
    this.cells.set(scope, cell);
    this.addresses.set(scope, Object.freeze({ key }));
    return {
      capture: () => ({ key, values: [{ data: encode(this.metadata.codec, cell.get()) }] }),
      restore: (saved) => {
        if (saved.values.length !== 1 || saved.values[0].id !== undefined) throw new Error('Invalid scalar checkpoint.');
        const value = decode(this.metadata.codec, saved.values[0].data);
        return () => { cell.set(value); };
      },
    };
  }
  [cellAccess](scope: LibraryScope): Atom<T> {
    const cell = this.cells.get(scope);
    if (!cell) throw new Error('Scalar handle is not owned by this library scope.');
    return cell;
  }
  interest(scope: LibraryScope): StateInterest {
    const address = this.addresses.get(scope);
    if (!address) throw new Error('Scalar handle is not owned by this library scope.');
    return address;
  }
}
export function scalarState<T>(initial: () => T, metadata?: HandleMetadata<T>): ScalarHandle<T> {
  return new ScalarHandle(initial, metadata);
}

interface KeyedCell<T> { readonly initial: T; readonly signal: Atom<T> }
export class KeyedHandle<ID extends ResourceId, T> extends StateDeclaration {
  private readonly cells = new WeakMap<LibraryScope, Map<ID, KeyedCell<T>>>();
  private readonly keys = new WeakMap<LibraryScope, string>();
  constructor(readonly initial: (id: ID) => T, readonly metadata: HandleMetadata<T> & { readonly idCodec?: StateBusCodec<ID> } = {}) { super(); Object.freeze(metadata); if (metadata.codec) Object.freeze(metadata.codec); if (metadata.idCodec) Object.freeze(metadata.idCodec); }
  override [createCell](scope: LibraryScope, key: string): StateSlot {
    const cells = new Map<ID, KeyedCell<T>>();
    this.cells.set(scope, cells);
    this.keys.set(scope, key);
    return {
      // A read-created default cell is not a state change. It must not require replaying subscriptions.
      capture: () => ({ key, values: Array.from(cells).flatMap(([id, cell]) => Object.is(cell.initial, cell.signal.get()) ? [] : [{ id: encode(this.metadata.idCodec, id), data: encode(this.metadata.codec, cell.signal.get()) }]) }),
      restore: (saved) => {
        const values = new Map<ID, T>();
        for (const item of saved.values) {
          if (!item.id) throw new Error('Missing checkpoint resource ID.');
          const id = decode(this.metadata.idCodec, item.id);
          if (values.has(id)) throw new Error('Duplicate checkpoint resource ID.');
          values.set(id, decode(this.metadata.codec, item.data));
        }
        return () => {
          // Preserve mounted signal identities while replacing checkpoint values.
          for (const [id, cell] of cells) cell.signal.set(values.has(id) ? values.get(id)! : cell.initial);
          for (const [id, value] of values) this[cellAccess](scope, id).set(value);
        };
      },
    };
  }
  matcher(scope: LibraryScope): (interest: StateInterest) => interest is ResourceInterest<ID> {
    const key = this.keys.get(scope);
    if (key === undefined) throw new Error('Keyed handle is not owned by this library scope.');
    return (interest): interest is ResourceInterest<ID> => interest.key === key && interest.id !== undefined;
  }
  [cellAccess](scope: LibraryScope, id: ID): Atom<T> {
    const cells = this.cells.get(scope);
    const key = this.keys.get(scope);
    if (!cells || key === undefined) throw new Error('Keyed handle is not owned by this library scope.');
    let cell = cells.get(id);
    if (!cell) {
      if (typeof id === 'number' && !Number.isFinite(id)) throw new RangeError('Resource IDs must be finite.');
      // The diagnostic name was resolved at composition; do not stringify resource IDs on reads.
      const initial = this.initial(id);
      cell = { initial, signal: atom(key, initial) };
      cells.set(id, cell);
    }
    return cell.signal;
  }
  interest(scope: LibraryScope, id: ID): ResourceInterest<ID> {
    const key = this.keys.get(scope);
    if (key === undefined) throw new Error('Keyed handle is not owned by this library scope.');
    return { key, id };
  }
}
export function keyedState<ID extends ResourceId, T>(initial: (id: ID) => T, metadata?: HandleMetadata<T> & { readonly idCodec?: StateBusCodec<ID> }): KeyedHandle<ID, T> {
  return new KeyedHandle(initial, metadata);
}

export interface LibraryState {
  get<T>(handle: ScalarHandle<T>): T;
  getKeyed<ID extends ResourceId, T>(handle: KeyedHandle<ID, T>, id: NoInfer<ID>): T;
}
export interface ReducerState extends LibraryState {
  set<T>(handle: ScalarHandle<T>, value: NoInfer<T>): void;
  setKeyed<ID extends ResourceId, T>(handle: KeyedHandle<ID, T>, id: NoInfer<ID>, value: NoInfer<T>): void;
}
export interface ExternalSubscription<T> {
  readonly getSnapshot: () => T;
  readonly subscribe: (listener: () => void) => () => void;
}
interface PendingMessage {
  reduce(): void;
  notify(): void;
  capture(): RecordedEvent | undefined;
}
interface EventSlot {
  restore(event: RecordedEvent): PendingMessage;
}
export abstract class EventDeclaration {
  abstract [createCell](scope: LibraryScope, key: string): EventSlot;
}
class EventCell<E> {
  reducer: ((state: ReducerState, payload: E) => boolean | void) | undefined;
  readonly listeners = new Set<(payload: E, admitted: boolean) => void>();
  readonly publish = (payload: E): void => { this.scope.runtime[enqueueMessage](new EventMessage(this, payload)); };
  constructor(readonly scope: LibraryScope, readonly key: string, readonly metadata: HandleMetadata<E>) {}
  restore(event: RecordedEvent): PendingMessage { return new EventMessage(this, decode(this.metadata.codec, event.data)); }
}
class EventMessage<E> implements PendingMessage {
  private admitted = false;
  constructor(private readonly cell: EventCell<E>, private readonly payload: E) {}
  reduce(): void { this.admitted = this.cell.reducer?.(this.cell.scope[writerAccess], this.payload) !== false; }
  notify(): void {
    for (const listener of this.cell.listeners) this.cell.scope.runtime.guard(() => listener(this.payload, this.admitted));
  }
  capture(): RecordedEvent { return { key: this.cell.key, data: encode(this.cell.metadata.codec, this.payload) }; }
}
export class EventHandle<E> extends EventDeclaration {
  private readonly cells = new WeakMap<LibraryScope, EventCell<E>>();
  constructor(readonly metadata: HandleMetadata<E> = {}) { super(); Object.freeze(metadata); if (metadata.codec) Object.freeze(metadata.codec); }
  override [createCell](scope: LibraryScope, key: string): EventSlot {
    const cell = new EventCell<E>(scope, key, this.metadata);
    this.cells.set(scope, cell);
    return cell;
  }
  [cellAccess](scope: LibraryScope): EventCell<E> {
    const cell = this.cells.get(scope);
    if (!cell) throw new Error('Event handle is not owned by this library scope.');
    return cell;
  }
}
export function eventType<E>(metadata?: HandleMetadata<E>): EventHandle<E> { return new EventHandle(metadata); }

export interface ReducerBinding {
  readonly event: EventDeclaration;
  install(scope: LibraryScope): void;
}
/** Returning false refuses this exact command, even when an identical command was admitted in the wave. */
export function onEvent<E>(event: EventHandle<E>, reducer: (state: ReducerState, payload: E) => boolean | void): ReducerBinding {
  return { event, install(scope) { event[cellAccess](scope).reducer = reducer; } };
}
export abstract class BindingDeclaration {}
export interface ProvidedBinding {
  readonly requirement: BindingDeclaration;
  install(scope: LibraryScope): void;
}
export class RequiredBinding<T> extends BindingDeclaration {
  private readonly factories = new WeakMap<LibraryScope, () => T>();
  private readonly values = new WeakMap<LibraryScope, { readonly value: T }>();
  constructor(readonly name: string) { super(); }
  provide(factory: (scope: LibraryScope) => T): ProvidedBinding {
    return { requirement: this, install: (scope) => { this.factories.set(scope, () => factory(scope)); } };
  }
  value(scope: LibraryScope): T {
    const existing = this.values.get(scope);
    if (existing) return existing.value;
    const factory = this.factories.get(scope);
    if (!factory) throw new Error(`Missing required binding: ${this.name}`);
    const value = factory();
    this.values.set(scope, { value });
    return value;
  }
}
export function requiredBinding<T>(name: string): RequiredBinding<T> { return new RequiredBinding(name); }

export interface LibraryEffect {
  readonly name: string;
  bind(scope: LibraryScope, key: string): void;
  readonly event: EventDeclaration;
  readonly requires?: readonly BindingDeclaration[];
  readonly results?: readonly EventDeclaration[];
  install(scope: LibraryScope): () => void;
}
export interface LibraryDefinition {
  readonly name: string;
  readonly states: Readonly<Record<string, StateDeclaration>>;
  readonly events: Readonly<Record<string, EventDeclaration>>;
  readonly reducers: readonly ReducerBinding[];
  readonly effects?: readonly LibraryEffect[];
  readonly requires?: readonly BindingDeclaration[];
}
export function defineLibrary<const L extends LibraryDefinition>(definition: L): L {
  Object.freeze(definition.states); Object.freeze(definition.events); Object.freeze(definition.reducers);
  if (definition.effects) Object.freeze(definition.effects);
  if (definition.requires) Object.freeze(definition.requires);
  return Object.freeze(definition);
}
export interface LibraryMount<L extends LibraryDefinition = LibraryDefinition> {
  readonly name: string;
  readonly library: L;
  readonly bindings: readonly ProvidedBinding[];
}
export function mountLibrary<L extends LibraryDefinition>(name: string, library: L, bindings: readonly ProvidedBinding[] = []): LibraryMount<L> {
  return Object.freeze({ name, library, bindings: Object.freeze([...bindings]) });
}
interface ResolvedMount {
  readonly mount: LibraryMount;
  readonly states: readonly { readonly handle: StateDeclaration; readonly key: string }[];
  readonly events: readonly { readonly handle: EventDeclaration; readonly key: string }[];
  readonly effects: readonly { readonly effect: LibraryEffect; readonly key: string }[];
}

export class LibraryScope implements LibraryState {
  readonly [writerAccess]: ReducerState = {
    get: <T>(handle: ScalarHandle<T>) => this.get(handle),
    getKeyed: <ID extends ResourceId, T>(handle: KeyedHandle<ID, T>, id: ID) => this.getKeyed(handle, id),
    set: <T>(handle: ScalarHandle<T>, value: T) => { this.runtime.assertReducing(); handle[cellAccess](this).set(value); },
    setKeyed: <ID extends ResourceId, T>(handle: KeyedHandle<ID, T>, id: ID, value: T) => { this.runtime.assertReducing(); handle[cellAccess](this, id).set(value); },
  };
  constructor(readonly runtime: ComposedStateBus, readonly mount: LibraryMount) {}
  get<T>(handle: ScalarHandle<T>): T { return handle[cellAccess](this).get(); }
  getKeyed<ID extends ResourceId, T>(handle: KeyedHandle<ID, T>, id: NoInfer<ID>): T { return handle[cellAccess](this, id).get(); }
  publisher<E>(event: EventHandle<E>): (payload: E) => void { return event[cellAccess](this).publish; }
  subscribe<E>(event: EventHandle<E>, listener: (payload: E, admitted: boolean) => void): () => void {
    const cell = event[cellAccess](this);
    cell.listeners.add(listener);
    return this.runtime.own(() => { cell.listeners.delete(listener); });
  }
  interest<T>(handle: ScalarHandle<T>): StateInterest { return handle.interest(this); }
  keyedInterest<ID extends ResourceId, T>(handle: KeyedHandle<ID, T>, id: NoInfer<ID>): ResourceInterest<ID> { return handle.interest(this, id); }
  observe<T>(handle: ScalarHandle<T>): ExternalSubscription<T> { return this.observeSignal(handle[cellAccess](this), [handle.interest(this)]); }
  observeKeyed<ID extends ResourceId, T>(handle: KeyedHandle<ID, T>, id: NoInfer<ID>): ExternalSubscription<T> {
    return this.observeSignal(handle[cellAccess](this, id), [handle.interest(this, id)]);
  }
  select<T>(name: string, derive: (state: LibraryState) => T, interests: readonly StateInterest[], isEqual?: (a: T, b: T) => boolean): ExternalSubscription<T> {
    const signal = signalComputed(name, () => derive(this), { isEqual });
    return this.observeSignal(signal, interests);
  }
  private observeSignal<T>(signal: Signal<T>, interests: readonly StateInterest[]): ExternalSubscription<T> {
    return {
      getSnapshot: () => signal.get(),
      subscribe: (listener) => {
        this.runtime.assertActive();
        const release = this.runtime.acquire(interests);
        let initial = true;
        try {
          const stop = react(signal.name, () => { signal.get(); if (!initial) listener(); initial = false; });
          return this.runtime.own(() => { stop(); release(); }, 'state');
        } catch (error) { release(); throw error; }
      },
    };
  }
  /** Install existing boundary helpers. In replay mode the factory is never invoked. */
  install(factory: () => () => void): () => void {
    this.runtime.assertActive();
    return this.runtime.execution === 'replay' ? () => {} : this.runtime.own(factory(), 'effect');
  }
}

export interface RuntimeOptions {
  readonly scheduler?: DispatchScheduler;
  readonly execution?: 'live' | 'replay';
  /** Unexpected boundary errors are observable; they are not domain error classifications. */
  readonly onError: (error: unknown) => void;
}
export class StateBusComposition {
  readonly schema: readonly string[];
  readonly metadata: readonly HandleDescription[];
  private readonly resolved: readonly ResolvedMount[];
  constructor(mounts: readonly LibraryMount[]) {
    const names = new Set<string>();
    const owners = new Map<StateDeclaration | EventDeclaration, LibraryDefinition>();
    this.resolved = mounts.map((mount) => {
      if (names.has(mount.name)) throw new Error(`Duplicate library mount: ${mount.name}`);
      names.add(mount.name);
      const local = new Set<StateDeclaration | EventDeclaration>();
      const resolve = <H extends StateDeclaration | EventDeclaration>(handles: Readonly<Record<string, H>>, kind: string) => Object.entries(handles).map(([name, handle]) => {
        if (local.has(handle) || (owners.has(handle) && owners.get(handle) !== mount.library)) throw new Error('Duplicate handle ownership.');
        local.add(handle); owners.set(handle, mount.library);
        return Object.freeze({ handle, key: JSON.stringify([mount.name, kind, name]) });
      });
      const states = resolve(mount.library.states, 'state');
      const events = resolve(mount.library.events, 'event');
      const reduced = new Set<EventDeclaration>();
      for (const reducer of mount.library.reducers) {
        if (!local.has(reducer.event) || reduced.has(reducer.event)) throw new Error('Missing or duplicate reducer ownership.');
        reduced.add(reducer.event);
      }
      const required = new Set(mount.library.requires ?? []);
      for (const effect of mount.library.effects ?? []) {
        if (!local.has(effect.event) || !reduced.has(effect.event)) throw new Error('Effect requires an owned reducer.');
        for (const result of effect.results ?? []) if (!local.has(result)) throw new Error('Effect result requires an owned event.');
        for (const binding of effect.requires ?? []) required.add(binding);
      }
      const provided = new Set<BindingDeclaration>();
      for (const binding of mount.bindings) {
        if (!required.has(binding.requirement) || provided.has(binding.requirement)) throw new Error('Incompatible or duplicate library binding.');
        provided.add(binding.requirement);
      }
      if (provided.size !== required.size) throw new Error('Missing required library binding.');
      const effectNames = new Set<string>();
      const effects = (mount.library.effects ?? []).map((effect) => {
        if (effectNames.has(effect.name)) throw new Error('Duplicate effect ownership.');
        effectNames.add(effect.name);
        return { effect, key: JSON.stringify([mount.name, 'effect', effect.name]) };
      });
      return { mount, states, events, effects };
    });
    this.metadata = Object.freeze(this.resolved.flatMap(({ mount, states, events, effects }) => [
      ...states.map(({ key }) => Object.freeze({ key, kind: 'state' as const, mount: mount.name, library: mount.library.name })),
      ...events.map(({ key }) => Object.freeze({ key, kind: 'event' as const, mount: mount.name, library: mount.library.name })),
      ...effects.map(({ key }) => Object.freeze({ key, kind: 'effect' as const, mount: mount.name, library: mount.library.name })),
    ]));
    this.schema = Object.freeze(this.resolved.flatMap(({ states, events }) => [...states.map((entry) => entry.key), ...events.map((entry) => entry.key)]));
  }
  createRuntime(options: RuntimeOptions): ComposedStateBus { return new ComposedStateBus(this, this.resolved, options); }
}
export function composeLibraries(...mounts: readonly LibraryMount[]): StateBusComposition { return new StateBusComposition(mounts); }

export class ComposedStateBus extends DispatchQueue<PendingMessage> {
  private readonly scopes = new Map<LibraryMount, LibraryScope>();
  private readonly states = new Map<string, StateSlot>();
  private readonly events = new Map<string, EventSlot>();
  private readonly stops = { state: new Set<() => void>(), effect: new Set<() => void>(), listener: new Set<() => void>() };
  private readonly interestListeners = new Set<(changes: readonly StateInterestChange[]) => void>();
  private readonly interestBatch = new StateInterestBatch();
  private readonly registry = new StateInterestRegistry((changes) => {
    if (this.disposed) { for (const listener of this.interestListeners) this.guard(() => listener(changes)); return; }
    this[enqueueMessage]({ reduce: () => {}, notify: () => this.interestBatch.append(changes), capture: () => undefined });
  });
  readonly interests = {
    snapshot: (): readonly StateInterestChange[] => this.registry.snapshot(),
    subscribe: (listener: (changes: readonly StateInterestChange[]) => void): (() => void) => {
      this.assertActive(); this.interestListeners.add(listener);
      return this.own(() => { this.interestListeners.delete(listener); });
    },
  };
  readonly execution: 'live' | 'replay';
  private readonly scheduler: DispatchScheduler;
  private cancelScheduled: (() => void) | undefined;
  private disposed = false;
  private reducing = false;
  private recordingFailure: { readonly error: unknown } | undefined;
  private recording: { checkpoint: StateBusCheckpoint; waves: RecordedEvent[][]; outcomes: CapturedOutcome[]; maxWaves: number; maxEvents: number; maxOutcomes: number; eventCount: number } | undefined;
  constructor(readonly composition: StateBusComposition, resolved: readonly ResolvedMount[], private readonly options: RuntimeOptions) {
    super();
    this.scheduler = options.scheduler ?? microtaskScheduler();
    this.execution = options.execution ?? 'live';
    try {
      for (const { mount, states, events, effects } of resolved) {
        const scope = new LibraryScope(this, mount);
        this.scopes.set(mount, scope);
        for (const entry of states) this.states.set(entry.key, entry.handle[createCell](scope, entry.key));
        for (const entry of events) this.events.set(entry.key, entry.handle[createCell](scope, entry.key));
        for (const binding of mount.bindings) binding.install(scope);
        for (const reducer of mount.library.reducers) reducer.install(scope);
        for (const { effect, key } of effects) effect.bind(scope, key);
      }
      if (this.execution === 'live') for (const [mount, scope] of this.scopes) for (const effect of mount.library.effects ?? []) scope.install(() => effect.install(scope));
    } catch (error) { this.dispose(); throw error; }
  }
  scope(mount: LibraryMount): LibraryScope {
    const scope = this.scopes.get(mount);
    if (!scope) throw new Error('Library mount does not belong to this composition.');
    return scope;
  }
  assertActive(): void { if (this.disposed) throw new Error('StateBus runtime has been disposed.'); }
  assertReducing(): void { if (!this.reducing) throw new Error('State writes are only supported inside reducers.'); }
  [enqueueMessage](event: PendingMessage): void { if (this.reducing) throw new Error('Reducers must not publish events.'); if (!this.disposed) this.queue(event); }
  acquire(interests: readonly StateInterest[]): () => void { this.assertActive(); return this.own(this.registry.acquire(interests), 'state'); }
  own(stop: () => void, kind: 'state' | 'effect' | 'listener' = 'listener'): () => void {
    let stopped = false;
    const dispose = () => { if (stopped) return; stopped = true; this.stops[kind].delete(dispose); stop(); };
    if (this.disposed) dispose(); else this.stops[kind].add(dispose);
    return dispose;
  }
  reportError(error: unknown): void { this.options.onError(error); }
  guard(callback: () => void): void { try { callback(); } catch (error) { this.options.onError(error); } }
  private readonly flush = () => {
    this.cancelScheduled = undefined;
    try { this.dispatchEvents(); } catch (error) { this.options.onError(error); }
  };
  protected override scheduleDispatch(): void { if (!this.cancelScheduled) this.cancelScheduled = this.scheduler.schedule(this.flush); }
  protected override dispatchWave(events: readonly (PendingMessage | undefined)[], count: number): void {
    transaction(() => {
      this.reducing = true;
      try { for (let index = 0; index < count; index += 1) events[index]?.reduce(); }
      finally { this.reducing = false; }
    });
    if (this.recording) this.captureSafely(() => this.captureWave(events, count));
    for (let index = 0; index < count && !this.disposed; index += 1) events[index]?.notify();
    const changes = this.interestBatch.take();
    if (changes.length > 0) for (const listener of this.interestListeners) this.guard(() => listener(changes));
  }
  protected override clearWave(): void { this.interestBatch.clear(); }
  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.cancelScheduled?.(); this.cancelScheduled = undefined;
    this.stopDispatch();
    for (const kind of ['effect', 'state', 'listener'] as const) for (const stop of this.stops[kind]) this.guard(stop);
    this.recording = undefined;
  }
  checkpoint(): StateBusCheckpoint {
    return { version: 1, composition: this.composition.schema, states: Array.from(this.states.values(), (slot) => slot.capture()) };
  }
  restore(checkpoint: StateBusCheckpoint): void {
    this.assertActive();
    if (this.execution !== 'replay') throw new Error('Checkpoint restoration requires an I/O-disabled replay runtime.');
    if (checkpoint.version !== 1 || checkpoint.composition.length !== this.composition.schema.length || checkpoint.composition.some((key, index) => key !== this.composition.schema[index])) throw new Error('Incompatible composition checkpoint.');
    if (checkpoint.states.length !== this.states.size) throw new Error('Incomplete checkpoint.');
    const seen = new Set<string>();
    const apply = checkpoint.states.map((saved) => {
      const slot = this.states.get(saved.key);
      if (!slot || seen.has(saved.key)) throw new Error('Unknown or duplicate checkpoint state.');
      seen.add(saved.key); return slot.restore(saved);
    });
    transaction(() => { for (const set of apply) set(); });
  }
  startRecording(options: RecordingOptions = {}): void {
    this.assertActive();
    const { maxWaves = 128, maxEvents = 1024, maxOutcomes = 128 } = options;
    if ([maxWaves, maxEvents, maxOutcomes].some((value) => !Number.isSafeInteger(value) || value < 1)) throw new RangeError('Recording limits must be positive safe integers.');
    this.recordingFailure = undefined;
    this.recording = { checkpoint: this.checkpoint(), waves: [], outcomes: [], maxWaves, maxEvents, maxOutcomes, eventCount: 0 };
  }
  private captureWave(events: readonly (PendingMessage | undefined)[], count: number): void {
    const recording = this.recording;
    if (!recording) return;
    if (recording.waves.length === recording.maxWaves || recording.eventCount + count > recording.maxEvents) {
      recording.checkpoint = this.checkpoint(); recording.waves = []; recording.eventCount = 0; return;
    }
    const wave: RecordedEvent[] = [];
    for (let index = 0; index < count; index += 1) { const event = events[index]?.capture(); if (event) wave.push(event); }
    if (wave.length > 0) { recording.waves.push(wave); recording.eventCount += wave.length; }
  }
  private captureSafely(capture: () => void): void {
    try { capture(); } catch (error) {
      this.recording = undefined;
      this.recordingFailure = { error };
      this.reportError(error);
    }
  }
  captureOutcome<P, O>(key: string, plan: P, outcome: O, planCodec: StateBusCodec<P>, outcomeCodec: StateBusCodec<O>): void {
    if (!this.recording) return;
    this.captureSafely(() => {
      const recording = this.recording;
      if (!recording) return;
      const captured = { key, plan: encode(planCodec, plan), outcome: encode(outcomeCodec, outcome) };
      if (recording.outcomes.length === recording.maxOutcomes) recording.outcomes.shift();
      recording.outcomes.push(captured);
    });
  }
  recordingSnapshot(): StateBusRecording {
    if (this.recordingFailure) throw new Error('Recording stopped at an unsupported capture boundary.', { cause: this.recordingFailure.error });
    if (!this.recording) throw new Error('No recording is active.');
    return structuredClone({ version: 1, checkpoint: this.recording.checkpoint, waves: this.recording.waves, outcomes: this.recording.outcomes });
  }
  replay(recording: StateBusRecording): void {
    if (recording.version !== 1) throw new Error('Unsupported recording version.');
    this.restore(recording.checkpoint);
    for (const wave of recording.waves) {
      // Decode the whole wave before enqueueing any event, so invalid input cannot leave a partial queue.
      const events = wave.map((saved) => { const slot = this.events.get(saved.key); if (!slot) throw new Error('Unknown recorded event.'); return slot.restore(saved); });
      for (const event of events) this[enqueueMessage](event);
      this.dispatchEvents();
    }
  }
}
