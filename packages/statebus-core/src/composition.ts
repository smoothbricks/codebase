import { type Atom, atom, computed, react, type Signal, transaction } from '@tldraw/state';
import { DispatchQueue, type DispatchScheduler, microtaskScheduler } from './dispatch.js';
import { type StateInterest, StateInterestBatch, type StateInterestChange, StateInterestRegistry } from './interest.js';
import {
  type CapturedEntry,
  type CaptureSink,
  compareResourceKeys,
  type StateCapture,
  trackState,
  visitState,
} from './state-capture.js';

export interface ValueCodec<T> {
  readonly schema: string;
  readonly version: number;
  /** Return an owned representation. Validation and redaction belong at this boundary. */
  encode(value: T): unknown;
  /** Validate untrusted input, including branded identifiers, before returning T. */
  decode(value: unknown): T;
}
export type SupportClassification = 'public' | 'sensitive' | 'excluded';
export interface DeclarationOptions<T> {
  readonly codec?: ValueCodec<T>;
  readonly classify?: (value: T) => SupportClassification;
  readonly description?: string;
}
export interface DeclarationMetadata {
  readonly key: string;
  readonly owner: string;
  readonly name: string;
  readonly kind: 'scalar' | 'keyed' | 'event' | 'command';
  readonly schema?: string;
  readonly version?: number;
  readonly description?: string;
}
export interface EncodedValue {
  readonly schema: string;
  readonly version: number;
  readonly value: unknown;
  readonly classification: SupportClassification;
}
export interface EncodedState {
  readonly key: string;
  readonly entries: readonly { readonly id?: EncodedValue; readonly value: EncodedValue }[];
}
export interface EncodedEvent {
  readonly key: string;
  readonly payload: EncodedValue;
}
function encode<T>(options: DeclarationOptions<T>, value: T): EncodedValue {
  const codec = options.codec;
  if (!codec) throw new Error('Replay requires a codec for every retained declaration.');
  return Object.freeze({
    schema: codec.schema,
    version: codec.version,
    value: codec.encode(value),
    classification: options.classify?.(value) ?? 'public',
  });
}
function decode<T>(codec: ValueCodec<T> | undefined, value: EncodedValue): T {
  if (!codec || codec.schema !== value.schema || codec.version !== value.version)
    throw new Error('Incompatible codec/schema version.');
  return codec.decode(value.value);
}
interface Owner {
  readonly name: string;
}
interface BindingIdentity {
  readonly capability: object;
}
export class CapabilityBinding<T> implements BindingIdentity {
  // Invariant type witness; no consumer casts or ambient augmentation are needed.
  private declare readonly type: (value: T) => T;
  constructor(readonly capability: object) {
    Object.freeze(this);
  }
}
export class Capability<T> {
  private readonly values = new WeakMap<BindingIdentity, { value: T }>();
  constructor(readonly name: string) {}
  provide(value: T): CapabilityBinding<T> {
    const binding = new CapabilityBinding<T>(this);
    this.values.set(binding, { value });
    return binding;
  }
  /** @internal Resolution is performed exactly once, while mounting. */
  resolve(binding: BindingIdentity | undefined): T {
    const entry = binding && this.values.get(binding);
    if (!entry) throw new Error(`Missing or incompatible required binding: ${this.name}`);
    return entry.value;
  }
}
export function defineCapability<T>(name: string): Capability<T> {
  return new Capability(name);
}

interface StateDeclaration {
  readonly ownerToken: Owner;
  readonly metadata: DeclarationMetadata;
  initialize(runtime: ComposedRuntime): void;
  release(runtime: ComposedRuntime): void;
  capture(runtime: ComposedRuntime): EncodedState;
  [visitState](runtime: ComposedRuntime, sink: CaptureSink): boolean;
  [trackState](runtime: ComposedRuntime): StateCapture;
  restore(runtime: ComposedRuntime, state: EncodedState): void;
}
export interface StateInterestHandle {
  readonly ownerToken: object;
  readonly interest: StateInterest;
}
export interface ReadableHandle<T> extends StateInterestHandle {
  signal(runtime: ComposedRuntime): Signal<T>;
}
interface WritableHandle<T> extends ReadableHandle<T> {
  write(state: ReducerState, value: T): void;
}

export class ScalarHandle<T> implements StateDeclaration, WritableHandle<T> {
  private readonly cells = new WeakMap<ComposedRuntime, Atom<T>>();
  private checkpointWrites: WeakMap<ComposedRuntime, { dirty: boolean }> | undefined;
  readonly interest: StateInterest;
  constructor(
    readonly ownerToken: Owner,
    readonly metadata: DeclarationMetadata,
    private readonly initial: () => T,
    private readonly options: DeclarationOptions<T>,
  ) {
    this.interest = Object.freeze({ key: metadata.key });
  }
  initialize(runtime: ComposedRuntime): void {
    this.cells.set(runtime, atom(this.metadata.key, this.initial()));
  }
  release(runtime: ComposedRuntime): void {
    this.cells.delete(runtime);
    this.checkpointWrites?.delete(runtime);
  }
  private cell(runtime: ComposedRuntime): Atom<T> {
    const cell = this.cells.get(runtime);
    if (!cell) throw new Error('State handle does not belong to this live runtime.');
    return cell;
  }
  signal(runtime: ComposedRuntime): Signal<T> {
    return this.cell(runtime);
  }
  write(state: ReducerState, value: T): void {
    const runtime = writableRuntime(state, this.ownerToken);
    this.cell(runtime).set(value);
    // Live reductions do not allocate, collect addresses or consult capture registries.
    if (runtime.mode === 'replay') {
      const writes = this.checkpointWrites?.get(runtime);
      if (writes) writes.dirty = true;
    }
  }
  [visitState](runtime: ComposedRuntime, sink: CaptureSink): boolean {
    return sink(undefined, { value: encode(this.options, this.cell(runtime).get()) });
  }
  [trackState](runtime: ComposedRuntime): StateCapture {
    runtime.assertOwner(this.ownerToken);
    if (runtime.mode !== 'replay' || this.checkpointWrites?.has(runtime))
      throw new Error('Checkpoint write tracking requires an untracked replay runtime.');
    const writes = { dirty: false };
    this.checkpointWrites ??= new WeakMap();
    this.checkpointWrites.set(runtime, writes);
    return {
      drain: (sink) => {
        if (!writes.dirty) return true;
        writes.dirty = false;
        return this[visitState](runtime, sink);
      },
      dispose: () => {
        this.checkpointWrites?.delete(runtime);
      },
    };
  }
  capture(runtime: ComposedRuntime): EncodedState {
    return { key: this.metadata.key, entries: [{ value: encode(this.options, this.cell(runtime).get()) }] };
  }
  restore(runtime: ComposedRuntime, state: EncodedState): void {
    if (state.entries.length !== 1 || state.entries[0].id !== undefined) throw new Error('Invalid scalar checkpoint.');
    this.cell(runtime).set(decode(this.options.codec, state.entries[0].value));
  }
}

interface KeyedCell<T> {
  readonly signal: Atom<T>;
  readonly initial: T;
}
export class KeyedHandle<T, ID extends string | number> implements StateDeclaration {
  private readonly cells = new WeakMap<ComposedRuntime, Map<ID, KeyedCell<T>>>();
  private checkpointWrites: WeakMap<ComposedRuntime, Set<ID>> | undefined;
  constructor(
    readonly ownerToken: Owner,
    readonly metadata: DeclarationMetadata,
    private readonly initial: (id: ID) => T,
    private readonly options: DeclarationOptions<T> & { readonly idCodec: ValueCodec<ID> },
  ) {}
  initialize(runtime: ComposedRuntime): void {
    this.cells.set(runtime, new Map());
  }
  release(runtime: ComposedRuntime): void {
    this.cells.delete(runtime);
    this.checkpointWrites?.delete(runtime);
  }
  private map(runtime: ComposedRuntime): Map<ID, KeyedCell<T>> {
    const map = this.cells.get(runtime);
    if (!map) throw new Error('State handle does not belong to this live runtime.');
    return map;
  }
  private cell(runtime: ComposedRuntime, id: ID): Atom<T> {
    const map = this.map(runtime);
    let cell = map.get(id);
    if (!cell) {
      if (typeof id === 'number' && !Number.isFinite(id)) throw new RangeError('Resource IDs must be finite.');
      // The diagnostic name was resolved at composition, never stringify IDs on reads.
      const initial = this.initial(id);
      cell = { signal: atom(this.metadata.key, initial), initial };
      map.set(id, cell);
    }
    return cell.signal;
  }
  signal(runtime: ComposedRuntime, id: ID): Signal<T> {
    return this.cell(runtime, id);
  }
  at(id: ID): ResourceHandle<T, ID> {
    return new ResourceHandle(this, id);
  }
  write(state: ReducerState, id: ID, value: T): void {
    const runtime = writableRuntime(state, this.ownerToken);
    this.cell(runtime, id).set(value);
    if (runtime.mode === 'replay') this.checkpointWrites?.get(runtime)?.add(id);
  }
  /** Validated bridge from an existing loader's wire address to this branded resource. */
  resourceId(interest: StateInterest): ID {
    if (interest.key !== this.metadata.key || interest.id === undefined)
      throw new Error('Foreign or non-keyed resource interest.');
    return this.options.idCodec.decode(interest.id);
  }
  private captureCell(id: ID, cell: KeyedCell<T>): CapturedEntry | undefined {
    const value = cell.signal.get();
    // Merely reading a default must not become application state or change replay equivalence.
    return Object.is(value, cell.initial)
      ? undefined
      : {
          id: encode({ codec: this.options.idCodec }, id),
          value: encode(this.options, value),
        };
  }
  [visitState](runtime: ComposedRuntime, sink: CaptureSink): boolean {
    for (const [id, cell] of this.map(runtime)) {
      const entry = this.captureCell(id, cell);
      if (entry && !sink(id, entry)) return false;
    }
    return true;
  }
  [trackState](runtime: ComposedRuntime): StateCapture {
    runtime.assertOwner(this.ownerToken);
    if (runtime.mode !== 'replay' || this.checkpointWrites?.has(runtime))
      throw new Error('Checkpoint write tracking requires an untracked replay runtime.');
    const writes = new Set<ID>();
    this.checkpointWrites ??= new WeakMap();
    this.checkpointWrites.set(runtime, writes);
    return {
      drain: (sink) => {
        const cells = this.map(runtime);
        for (const id of writes) {
          const cell = cells.get(id);
          if (cell && !sink(id, this.captureCell(id, cell))) return false;
        }
        writes.clear();
        return true;
      },
      dispose: () => {
        writes.clear();
        this.checkpointWrites?.delete(runtime);
      },
    };
  }
  capture(runtime: ComposedRuntime): EncodedState {
    const entries: CapturedEntry[] = [];
    // Canonicalization is an explicit checkpoint boundary, never an ordinary keyed read.
    const cells = this.map(runtime);
    for (const id of [...cells.keys()].sort(compareResourceKeys)) {
      const cell = cells.get(id);
      if (!cell) continue;
      const entry = this.captureCell(id, cell);
      if (entry) entries.push(entry);
    }
    return { key: this.metadata.key, entries };
  }
  restore(runtime: ComposedRuntime, state: EncodedState): void {
    const map = this.map(runtime);
    if (map.size !== 0) throw new Error('Restore requires a fresh runtime.');
    for (const entry of state.entries) {
      if (!entry.id) throw new Error('Missing resource ID in checkpoint.');
      const id = decode(this.options.idCodec, entry.id);
      if (map.has(id)) throw new Error('Duplicate resource in checkpoint.');
      map.set(id, {
        signal: atom(this.metadata.key, decode(this.options.codec, entry.value)),
        initial: this.initial(id),
      });
    }
  }
}
export class ResourceHandle<T, ID extends string | number> implements WritableHandle<T> {
  readonly ownerToken: object;
  readonly interest: StateInterest;
  constructor(
    readonly state: KeyedHandle<T, ID>,
    readonly id: ID,
  ) {
    this.ownerToken = state.ownerToken;
    this.interest = Object.freeze({ key: state.metadata.key, id });
  }
  signal(runtime: ComposedRuntime): Signal<T> {
    return this.state.signal(runtime, this.id);
  }
  write(state: ReducerState, value: T): void {
    this.state.write(state, this.id, value);
  }
}
export interface StateReader {
  read<T>(handle: ReadableHandle<T>): T;
  readKeyed<T, ID extends string | number>(handle: KeyedHandle<T, ID>, id: ID): T;
}
const reducerContexts = new WeakMap<ReducerState, { readonly runtime: ComposedRuntime; readonly owner: Owner }>();
function reducerContext(state: ReducerState) {
  const context = reducerContexts.get(state);
  if (!context) throw new Error('Unknown reducer context.');
  return context;
}
function writableRuntime(state: ReducerState, owner: object): ComposedRuntime {
  const context = reducerContext(state);
  if (context.owner !== owner || !context.runtime.reducing) throw new Error('State writes require the owning reducer.');
  return context.runtime;
}
export class ReducerState implements StateReader {
  constructor(runtime: ComposedRuntime, owner: Owner) {
    reducerContexts.set(this, { runtime, owner });
  }
  read<T>(handle: ReadableHandle<T>): T {
    return reducerContext(this).runtime.read(handle);
  }
  readKeyed<T, ID extends string | number>(handle: KeyedHandle<T, ID>, id: ID): T {
    return reducerContext(this).runtime.readKeyed(handle, id);
  }
  set<T>(handle: WritableHandle<T>, value: NoInfer<T>): void {
    handle.write(this, value);
  }
  setKeyed<T, ID extends string | number>(handle: KeyedHandle<T, ID>, id: ID, value: NoInfer<T>): void {
    handle.write(this, id, value);
  }
}

interface EventSlot<T> {
  readonly publisher: (payload: T) => void;
  readonly reducers: ((payload: T) => void)[];
  readonly listeners: Set<(payload: T, admitted: boolean) => void>;
  admission?: (payload: T) => boolean;
}
interface EventDeclaration {
  readonly ownerToken: Owner;
  readonly metadata: DeclarationMetadata;
  initialize(runtime: ComposedRuntime): void;
  release(runtime: ComposedRuntime): void;
  decode(event: EncodedEvent): Publication;
}
export abstract class Publication {
  abstract readonly kind: 'event';
  abstract reduce(runtime: ComposedRuntime): void;
  abstract notify(runtime: ComposedRuntime): void;
  abstract capture(): EncodedEvent;
}
class TypedPublication<T> extends Publication {
  readonly kind = 'event';
  private admitted = false;
  constructor(
    private readonly handle: EventHandle<T>,
    private readonly payload: T,
  ) {
    super();
  }
  reduce(runtime: ComposedRuntime): void {
    this.admitted = this.handle.reduce(runtime, this.payload);
  }
  notify(runtime: ComposedRuntime): void {
    this.handle.notify(runtime, this.payload, this.admitted);
  }
  capture(): EncodedEvent {
    return this.handle.capture(this.payload);
  }
}
export class EventHandle<T> implements EventDeclaration {
  private readonly slots = new WeakMap<ComposedRuntime, EventSlot<T>>();
  constructor(
    readonly ownerToken: Owner,
    readonly metadata: DeclarationMetadata,
    private readonly options: DeclarationOptions<T>,
  ) {}
  initialize(runtime: ComposedRuntime): void {
    this.slots.set(runtime, {
      reducers: [],
      listeners: new Set(),
      publisher: (payload) => runtime.publish(this, payload),
    });
  }
  release(runtime: ComposedRuntime): void {
    this.slots.delete(runtime);
  }
  private slot(runtime: ComposedRuntime): EventSlot<T> {
    const slot = this.slots.get(runtime);
    if (!slot) throw new Error('Event handle does not belong to this live runtime.');
    return slot;
  }
  /** @internal */ addReducer(runtime: ComposedRuntime, reducer: (payload: T) => void): void {
    this.slot(runtime).reducers.push(reducer);
  }
  /** @internal */ setAdmission(runtime: ComposedRuntime, admission: (payload: T) => boolean): void {
    this.slot(runtime).admission = admission;
  }
  /** @internal */ reduce(runtime: ComposedRuntime, payload: T): boolean {
    const slot = this.slot(runtime);
    if (slot.admission && !slot.admission(payload)) return false;
    for (const reducer of slot.reducers) reducer(payload);
    return true;
  }
  /** @internal */ notify(runtime: ComposedRuntime, payload: T, admitted: boolean): void {
    for (const listener of this.slot(runtime).listeners) {
      if (runtime.disposed) break;
      try {
        listener(payload, admitted);
      } catch (cause) {
        runtime.reportError(cause);
      }
    }
  }
  listen(runtime: ComposedRuntime, listener: (payload: T, admitted: boolean) => void): () => void {
    const listeners = this.slot(runtime).listeners;
    listeners.add(listener);
    return runtime.manage(() => {
      listeners.delete(listener);
    });
  }
  publisher(runtime: ComposedRuntime): (payload: T) => void {
    return this.slot(runtime).publisher;
  }
  publication(payload: T): Publication {
    return new TypedPublication(this, payload);
  }
  capture(payload: T): EncodedEvent {
    return { key: this.metadata.key, payload: encode(this.options, payload) };
  }
  decode(event: EncodedEvent): Publication {
    return this.publication(decode(this.options.codec, event.payload));
  }
}

export interface LibraryDefinition<Exports> {
  readonly name: string;
  readonly requires: readonly { readonly name: string; resolve(binding: BindingIdentity | undefined): unknown }[];
  readonly setup: (scope: LibraryScope) => Exports;
}
export function defineLibrary<Exports>(definition: LibraryDefinition<Exports>): LibraryDefinition<Exports> {
  return Object.freeze({ ...definition, requires: Object.freeze([...definition.requires]) });
}
export interface MountedLibrary<Exports> {
  readonly definition: LibraryDefinition<Exports>;
  readonly owner: string;
  readonly exports: Exports;
  /** @internal */ readonly scope: LibraryScope;
}
export class LibraryScope {
  readonly ownerToken: Owner;
  readonly states: StateDeclaration[] = [];
  readonly events: EventDeclaration[] = [];
  readonly references = new Set<object>();
  readonly installations: ((runtime: ComposedRuntime, state: ReducerState) => void)[] = [];
  private readonly names = new Set<string>();
  private sealed = false;
  constructor(
    owner: string,
    private readonly bindings: ReadonlyMap<object, BindingIdentity>,
    private readonly requirements: ReadonlySet<object>,
  ) {
    this.ownerToken = Object.freeze({ name: owner });
  }
  private metadata<T>(
    name: string,
    kind: DeclarationMetadata['kind'],
    options: DeclarationOptions<T>,
  ): DeclarationMetadata {
    if (this.sealed || this.names.has(name) || name.length === 0)
      throw new Error(`Duplicate or closed declaration: ${name}`);
    if (
      options.codec &&
      (!Number.isSafeInteger(options.codec.version) || options.codec.version < 1 || !options.codec.schema)
    )
      throw new Error('A codec needs a nonempty schema and positive integer version.');
    this.names.add(name);
    const owner = this.ownerToken.name;
    return Object.freeze({
      key: `${owner.length}:${owner}${name.length}:${name}`,
      owner,
      name,
      kind,
      schema: options.codec?.schema,
      version: options.codec?.version,
      description: options.description,
    });
  }
  require<T>(capability: Capability<T>): T {
    if (this.sealed) throw new Error('Bindings are resolved only during library setup.');
    if (!this.requirements.has(capability)) throw new Error(`Undeclared required binding: ${capability.name}`);
    const value = capability.resolve(this.bindings.get(capability));
    // Handle-valued requirements also require their owning mount in the composition.
    if (
      value instanceof ScalarHandle ||
      value instanceof KeyedHandle ||
      value instanceof ResourceHandle ||
      value instanceof EventHandle
    )
      this.references.add(value.ownerToken);
    return value;
  }
  scalar<T>(name: string, initial: () => T, options: DeclarationOptions<T> = {}): ScalarHandle<T> {
    const handle = new ScalarHandle(this.ownerToken, this.metadata(name, 'scalar', options), initial, options);
    this.states.push(handle);
    return handle;
  }
  keyed<T, ID extends string | number>(
    name: string,
    initial: (id: ID) => T,
    options: DeclarationOptions<T> & { readonly idCodec: ValueCodec<ID> },
  ): KeyedHandle<T, ID> {
    const handle = new KeyedHandle(this.ownerToken, this.metadata(name, 'keyed', options), initial, options);
    this.states.push(handle);
    return handle;
  }
  event<T>(name: string, options: DeclarationOptions<T> = {}): EventHandle<T> {
    const handle = new EventHandle<T>(this.ownerToken, this.metadata(name, 'event', options), options);
    this.events.push(handle);
    return handle;
  }
  command<T>(
    name: string,
    admit: (state: ReducerState, payload: T) => boolean,
    options: DeclarationOptions<T> = {},
  ): EventHandle<T> {
    const handle = new EventHandle<T>(this.ownerToken, this.metadata(name, 'command', options), options);
    this.events.push(handle);
    this.installations.push((runtime, state) => handle.setAdmission(runtime, (payload) => admit(state, payload)));
    return handle;
  }
  reduce<T>(event: EventHandle<T>, reducer: (state: ReducerState, payload: T) => void): void {
    if (this.sealed) throw new Error('Cannot change a mounted library.');
    this.references.add(event.ownerToken);
    this.installations.push((runtime, state) => event.addReducer(runtime, (payload) => reducer(state, payload)));
  }
  /** @internal */ seal(): void {
    this.sealed = true;
    Object.freeze(this.states);
    Object.freeze(this.events);
    Object.freeze(this.installations);
  }
}
export function mountLibrary<Exports>(
  definition: LibraryDefinition<Exports>,
  owner: string,
  bindings: readonly BindingIdentity[] = [],
): MountedLibrary<Exports> {
  if (!owner) throw new Error('A library mount needs an owner.');
  const required = new Set<object>(definition.requires);
  if (required.size !== definition.requires.length) throw new Error('Duplicate required capability.');
  const supplied = new Map<object, BindingIdentity>();
  for (const binding of bindings) {
    if (!required.has(binding.capability) || supplied.has(binding.capability))
      throw new Error('Duplicate or incompatible capability binding.');
    supplied.set(binding.capability, binding);
  }
  for (const capability of definition.requires) capability.resolve(supplied.get(capability));
  const scope = new LibraryScope(owner, supplied, required);
  const exports = definition.setup(scope);
  scope.seal();
  return Object.freeze({ definition, owner, exports, scope });
}

export interface RuntimeOptions {
  readonly scheduler?: DispatchScheduler;
  readonly mode?: 'live' | 'replay';
  readonly onError?: (cause: unknown) => void;
}
export interface ExternalStore<T> {
  readonly getSnapshot: () => T;
  readonly subscribe: (listener: () => void) => () => void;
}
export interface WaveObserver {
  /** Only successful waves arrive here. Borrowed queue slots must not be retained. */
  committed(wave: number, events: readonly (Publication | InterestPublication | undefined)[], count: number): void;
}
interface InterestPublication {
  readonly kind: 'interest';
  readonly changes: readonly StateInterestChange[];
}
export class StateBusComposition {
  readonly metadata: readonly DeclarationMetadata[];
  readonly owners: ReadonlySet<object>;
  readonly states: readonly StateDeclaration[];
  readonly events: ReadonlyMap<string, EventDeclaration>;
  constructor(readonly mounts: readonly MountedLibrary<unknown>[]) {
    const names = new Set<string>();
    const owners = new Set<object>();
    const states: StateDeclaration[] = [];
    const events = new Map<string, EventDeclaration>();
    for (const mount of mounts) {
      if (names.has(mount.owner) || owners.has(mount.scope.ownerToken))
        throw new Error(`Duplicate ownership: ${mount.owner}`);
      names.add(mount.owner);
      owners.add(mount.scope.ownerToken);
      states.push(...mount.scope.states);
      for (const event of mount.scope.events) events.set(event.metadata.key, event);
    }
    for (const mount of mounts)
      for (const owner of mount.scope.references)
        if (!owners.has(owner)) throw new Error('A reducer requires an uncomposed library.');
    this.owners = owners;
    this.states = Object.freeze(states);
    this.events = events;
    this.metadata = Object.freeze([...states, ...events.values()].map((entry) => entry.metadata));
  }
  createRuntime(options: RuntimeOptions = {}): ComposedRuntime {
    return new ComposedRuntime(this, options);
  }
}
export function composeLibraries(...mounts: readonly MountedLibrary<unknown>[]): StateBusComposition {
  return new StateBusComposition(Object.freeze([...mounts]));
}

export class ComposedRuntime implements StateReader {
  readonly mode: 'live' | 'replay';
  readonly reader: StateReader;
  private readonly scheduler: DispatchScheduler;
  private readonly cleanups = new Set<() => void>();
  private readonly interestListeners = new Set<(changes: readonly StateInterestChange[]) => void>();
  private readonly waveObservers = new Set<WaveObserver>();
  private readonly interestBatch = new StateInterestBatch();
  private readonly interests = new StateInterestRegistry((changes) => {
    if (this.closing) this.interestBatch.append(changes);
    else this.queue.publish({ kind: 'interest', changes });
  });
  private readonly queue: DispatchQueue<Publication | InterestPublication>;
  private scheduled = false;
  private closing = false;
  private closed = false;
  private inReducer = false;
  private wave = 0;
  private readonly flushScheduled = () => {
    this.scheduled = false;
    if (!this.closed) {
      try {
        this.flush();
      } catch (cause) {
        this.reportError(cause);
      }
    }
  };
  constructor(
    readonly composition: StateBusComposition,
    private readonly options: RuntimeOptions = {},
  ) {
    this.mode = options.mode ?? 'live';
    this.scheduler = options.scheduler ?? microtaskScheduler;
    this.reader = Object.freeze({
      read: <T>(handle: ReadableHandle<T>) => this.read(handle),
      readKeyed: <T, ID extends string | number>(handle: KeyedHandle<T, ID>, id: ID) => this.readKeyed(handle, id),
    });
    this.queue = new DispatchQueue(
      (events, count) => this.dispatchWave(events, count),
      () => {
        if (!this.scheduled && !this.closed) {
          this.scheduled = true;
          this.scheduler.schedule(this.flushScheduled);
        }
      },
    );
    try {
      for (const state of composition.states) state.initialize(this);
      for (const event of composition.events.values()) event.initialize(this);
      for (const mount of composition.mounts) {
        const state = new ReducerState(this, mount.scope.ownerToken);
        for (const install of mount.scope.installations) install(this, state);
      }
    } catch (cause) {
      this.dispose();
      throw cause;
    }
  }
  /** Last successfully committed dispatch wave; interest-only waves also have positions. */
  get waveNumber(): number {
    return this.wave;
  }
  get disposed(): boolean {
    return this.closed || this.closing;
  }
  /** @internal */ get reducing(): boolean {
    return this.inReducer;
  }
  get idle(): boolean {
    return this.queue.idle;
  }
  assertOwner(owner: object): void {
    if (this.disposed || !this.composition.owners.has(owner)) throw new Error('Foreign handle or disposed runtime.');
  }
  read<T>(handle: ReadableHandle<T>): T {
    return handle.signal(this).get();
  }
  readKeyed<T, ID extends string | number>(handle: KeyedHandle<T, ID>, id: ID): T {
    return handle.signal(this, id).get();
  }
  publish<T>(event: EventHandle<T>, payload: NoInfer<T>): void {
    this.assertOwner(event.ownerToken);
    this.queue.publish(event.publication(payload));
  }
  publisher<T>(event: EventHandle<T>): (payload: T) => void {
    this.assertOwner(event.ownerToken);
    return event.publisher(this);
  }
  listen<T>(event: EventHandle<T>, listener: (payload: T, admitted: boolean) => void): () => void {
    return event.listen(this, listener);
  }
  manage(cleanup: () => void): () => void {
    if (this.disposed) {
      cleanup();
      return () => {};
    }
    let active = true;
    const stop = () => {
      if (!active) return;
      active = false;
      this.cleanups.delete(stop);
      cleanup();
    };
    this.cleanups.add(stop);
    return stop;
  }
  reportError(cause: unknown): void {
    // Error reporters must not turn an already handled async failure into an unhandled rejection.
    try {
      if (this.options.onError) this.options.onError(cause);
      else console.error('StateBus boundary error', cause);
    } catch (error) {
      console.error('StateBus error reporter failed', error);
    }
  }
  acquire(handles: readonly StateInterestHandle[]): () => void {
    for (const handle of handles) this.assertOwner(handle.ownerToken);
    return this.manage(this.interests.acquire(handles.map((handle) => handle.interest)));
  }
  readonly interestSource = {
    snapshot: (): readonly StateInterestChange[] => this.interests.snapshot(),
    subscribe: (listener: (changes: readonly StateInterestChange[]) => void): (() => void) => {
      if (this.disposed) throw new Error('Disposed runtime.');
      this.interestListeners.add(listener);
      return this.manage(() => {
        this.interestListeners.delete(listener);
      });
    },
  };
  binding<T>(handle: ReadableHandle<T>): ExternalStore<T> {
    return this.bindSignal(handle.signal(this), [handle]);
  }
  selection<T>(
    name: string,
    select: (state: StateReader) => T,
    interests: readonly StateInterestHandle[] = [],
  ): ExternalStore<T> {
    for (const interest of interests) this.assertOwner(interest.ownerToken);
    return this.bindSignal(
      computed(name, () => select(this.reader)),
      interests,
    );
  }
  private bindSignal<T>(signal: Signal<T>, interests: readonly StateInterestHandle[]): ExternalStore<T> {
    const getSnapshot = () => signal.get();
    return Object.freeze({
      getSnapshot,
      subscribe: (listener: () => void) => {
        if (this.disposed) throw new Error('Disposed runtime.');
        const release = this.acquire(interests);
        try {
          let previous = signal.get();
          const stop = react('StateBus subscription', () => {
            const value = signal.get();
            if (!Object.is(previous, value)) {
              previous = value;
              listener();
            }
          });
          return this.manage(() => {
            stop();
            release();
          });
        } catch (cause) {
          release();
          throw cause;
        }
      },
    });
  }
  observeWaves(observer: WaveObserver): () => void {
    this.waveObservers.add(observer);
    return this.manage(() => {
      this.waveObservers.delete(observer);
    });
  }
  private dispatchWave(events: readonly (Publication | InterestPublication | undefined)[], count: number): void {
    try {
      try {
        transaction(() => {
          this.inReducer = true;
          try {
            for (let index = 0; index < count; index++) {
              const event = events[index];
              if (event?.kind === 'event') event.reduce(this);
            }
          } finally {
            this.inReducer = false;
          }
        });
      } catch (cause) {
        // Leases describe live subscriptions, not reducer state. A rollback must not lose
        // their acquisition/final-zero facts. Requeue only interest in a successor wave;
        // never retry the failed domain events or notify their execution boundaries.
        if (!this.disposed)
          for (let index = 0; index < count; index++) {
            const event = events[index];
            if (event?.kind === 'interest') this.queue.publish(event);
          }
        throw cause;
      }
      const wave = ++this.wave;
      for (const observer of this.waveObservers) {
        try {
          observer.committed(wave, events, count);
        } catch (cause) {
          // Capture/diagnostic failures must not suppress effects after committed admission.
          this.reportError(cause);
        }
      }
      for (let index = 0; index < count && !this.disposed; index++) {
        const event = events[index];
        if (event?.kind === 'event') event.notify(this);
        else if (event) this.interestBatch.append(event.changes);
      }
      const changes = this.interestBatch.take();
      if (changes.length > 0)
        for (const listener of this.interestListeners) {
          try {
            listener(changes);
          } catch (cause) {
            this.reportError(cause);
          }
        }
    } finally {
      this.interestBatch.clear();
    }
  }
  flush(): void {
    this.scheduler.cancel(this.flushScheduled);
    this.scheduled = false;
    this.queue.flush();
  }
  /** @internal Replay accepts validated declarations, never a serialized execution plan. */
  enqueueRecorded(event: EncodedEvent): void {
    if (this.mode !== 'replay') throw new Error('Recorded events require a replay-only runtime.');
    const declaration = this.composition.events.get(event.key);
    if (!declaration) throw new Error('Unknown recorded event.');
    this.queue.publish(declaration.decode(event));
  }
  dispose(): void {
    if (this.disposed) return;
    const finalInterest = this.interests.snapshot().map(({ interest }) => Object.freeze({ interest, subscribers: 0 }));
    this.closing = true;
    if (finalInterest.length > 0)
      for (const listener of this.interestListeners) {
        try {
          listener(finalInterest);
        } catch (cause) {
          this.reportError(cause);
        }
      }
    for (const stop of this.cleanups) {
      try {
        stop();
      } catch (cause) {
        this.reportError(cause);
      }
    }
    this.cleanups.clear();
    this.queue.dispose();
    this.scheduler.cancel(this.flushScheduled);
    this.interestBatch.clear();
    this.interestListeners.clear();
    this.waveObservers.clear();
    for (const event of this.composition.events.values()) event.release(this);
    for (const state of this.composition.states) state.release(this);
    this.closed = true;
    this.closing = false;
  }
}
