import { type Atom, atom, computed, react, type Signal, transaction } from '@tldraw/state';
import { CaptureError, compareResourceIds } from './capture.js';
import { type CodecMigration, decodeCodec, supportsCodec } from './codec.js';
import { DispatchQueue, type DispatchScheduler, microtaskScheduler } from './dispatch.js';
import type { EffectCodecDescriptor } from './effects.js';
import { type StateInterest, StateInterestBatch, type StateInterestChange, StateInterestRegistry } from './interest.js';
import type { SupportPolicy } from './support-policy.js';

export interface ValueCodec<T> {
  readonly schema: string;
  readonly version: number;
  readonly migrations?: readonly CodecMigration<T>[];
  /** Return an owned representation. Validation and redaction belong at this boundary. */
  encode(value: T): unknown;
  /** Validate untrusted input, including branded identifiers, before returning T. */
  decode(value: unknown): T;
}
export type SupportClassification = 'public' | 'sensitive' | 'excluded' | 'secret' | 'unclassified';
export interface DeclarationOptions<T> {
  readonly codec?: ValueCodec<T>;
  readonly classify?: (value: T) => SupportClassification;
  readonly description?: string;
  readonly support?: SupportPolicy<T>;
}
export interface DeclarationMetadata {
  readonly key: string;
  readonly owner: string;
  readonly name: string;
  readonly kind: 'scalar' | 'keyed' | 'event' | 'command';
  readonly schema?: string;
  readonly version?: number;
  readonly description?: string;
  readonly idSchema?: string;
  readonly idVersion?: number;
}
export interface EncodedValue {
  readonly schema: string;
  readonly version: number;
  readonly value: unknown;
  readonly classification: SupportClassification;
  readonly primitive?: 'number' | 'string';
}
export interface EncodedStateEntry {
  readonly id?: EncodedValue;
  readonly value: EncodedValue;
}
export interface EncodedState {
  readonly key: string;
  readonly entries: readonly EncodedStateEntry[];
}
export interface EncodedEvent {
  readonly key: string;
  readonly payload: EncodedValue;
  readonly admitted?: boolean;
  readonly reactionDepth?: number;
}
function encode<T>(options: DeclarationOptions<T>, value: T): EncodedValue {
  const codec = options.codec;
  if (!codec) throw new Error('Replay requires a codec for every retained declaration.');
  return Object.freeze({
    schema: codec.schema,
    version: codec.version,
    value: codec.encode(value),
    classification: options.classify?.(value) ?? 'unclassified',
  });
}
function decode<T>(codec: ValueCodec<T> | undefined, value: EncodedValue, metadata?: DeclarationMetadata): T {
  try {
    return decodeCodec(codec, value);
  } catch (cause) {
    throw new CaptureError(
      {
        code: 'schema',
        boundary: 'declaration decode',
        owner: metadata?.owner,
        declaration: metadata?.name,
        schema: value.schema,
        fromVersion: value.version,
        toVersion: codec?.version,
      },
      { cause },
    );
  }
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

export interface StateDeclaration {
  readonly ownerToken: Owner;
  readonly metadata: DeclarationMetadata;
  initialize(runtime: ComposedRuntime): void;
  release(runtime: ComposedRuntime): void;
  capture(runtime: ComposedRuntime): EncodedState;
  captureEntries(runtime: ComposedRuntime): Iterable<EncodedStateEntry>;
  restore(runtime: ComposedRuntime, state: EncodedState): void;
  captureEntry(runtime: ComposedRuntime, id: string | number | undefined): EncodedStateEntry | undefined;
  entryId(entry: EncodedStateEntry): string | number | undefined;
  migrateEntry(entry: EncodedStateEntry): EncodedStateEntry;
  acceptsMetadata(metadata: DeclarationMetadata): boolean;
  readonly support: { readonly value?: SupportPolicy<unknown>; readonly id?: SupportPolicy<unknown> };
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
    runtime.markCapturedWrite(this, undefined);
  }
  get support() {
    return {
      value:
        this.options.support &&
        ((value: unknown) =>
          this.options.support?.(
            decodeCodec(this.options.codec, {
              schema: this.options.codec?.schema ?? '',
              version: this.options.codec?.version ?? 0,
              value,
            }),
          )),
    };
  }
  captureEntry(runtime: ComposedRuntime): EncodedStateEntry {
    return { value: encode(this.options, this.cell(runtime).get()) };
  }
  entryId(entry: EncodedStateEntry): undefined {
    if (entry.id)
      throw new CaptureError({
        code: 'schema',
        boundary: 'scalar resource ID',
        owner: this.metadata.owner,
        declaration: this.metadata.name,
      });
    return undefined;
  }
  acceptsMetadata(metadata: DeclarationMetadata): boolean {
    return supportsCodec(this.options.codec, metadata);
  }
  migrateEntry(entry: EncodedStateEntry): EncodedStateEntry {
    this.entryId(entry);
    return { value: encode(this.options, decode(this.options.codec, entry.value, this.metadata)) };
  }
  *captureEntries(runtime: ComposedRuntime): Iterable<EncodedStateEntry> {
    yield this.captureEntry(runtime);
  }
  capture(runtime: ComposedRuntime): EncodedState {
    return { key: this.metadata.key, entries: [...this.captureEntries(runtime)] };
  }
  restore(runtime: ComposedRuntime, state: EncodedState): void {
    if (state.entries.length !== 1 || state.entries[0].id !== undefined) throw new Error('Invalid scalar checkpoint.');
    this.cell(runtime).set(decode(this.options.codec, state.entries[0].value, this.metadata));
  }
}

interface KeyedCell<T> {
  readonly signal: Atom<T>;
  readonly initial: T;
}
export class KeyedHandle<T, ID extends string | number> implements StateDeclaration {
  private readonly cells = new WeakMap<ComposedRuntime, Map<ID, KeyedCell<T>>>();
  constructor(
    readonly ownerToken: Owner,
    readonly metadata: DeclarationMetadata,
    private readonly initial: (id: ID) => T,
    private readonly options: DeclarationOptions<T> & {
      readonly idCodec: ValueCodec<ID>;
      readonly classifyId?: (id: ID) => SupportClassification;
      readonly supportId?: SupportPolicy<ID>;
    },
  ) {}
  initialize(runtime: ComposedRuntime): void {
    this.cells.set(runtime, new Map());
  }
  release(runtime: ComposedRuntime): void {
    this.cells.delete(runtime);
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
    runtime.markCapturedWrite(this, id);
  }
  /** Validated bridge from an existing loader's wire address to this branded resource. */
  resourceId(interest: StateInterest): ID {
    if (interest.key !== this.metadata.key || interest.id === undefined)
      throw new Error('Foreign or non-keyed resource interest.');
    return this.options.idCodec.decode(interest.id);
  }
  get support() {
    return {
      value:
        this.options.support &&
        ((value: unknown) =>
          this.options.support?.(
            decodeCodec(this.options.codec, {
              schema: this.options.codec?.schema ?? '',
              version: this.options.codec?.version ?? 0,
              value,
            }),
          )),
      id: this.options.supportId && ((value: unknown) => this.options.supportId?.(this.options.idCodec.decode(value))),
    };
  }
  entryId(entry: EncodedStateEntry): ID {
    if (!entry.id)
      throw new CaptureError({
        code: 'schema',
        boundary: 'missing resource ID',
        owner: this.metadata.owner,
        declaration: this.metadata.name,
      });
    const id = decode(this.options.idCodec, entry.id, this.metadata);
    if ((entry.id.primitive && typeof id !== entry.id.primitive) || (typeof id === 'number' && !Number.isFinite(id)))
      throw new CaptureError({
        code: 'schema',
        boundary: 'resource primitive kind',
        owner: this.metadata.owner,
        declaration: this.metadata.name,
      });
    return id;
  }
  private encodeId(id: ID): EncodedValue {
    return {
      ...encode({ codec: this.options.idCodec, classify: this.options.classifyId }, id),
      primitive: typeof id === 'number' ? 'number' : 'string',
    };
  }
  acceptsMetadata(metadata: DeclarationMetadata): boolean {
    return (
      supportsCodec(this.options.codec, metadata) &&
      supportsCodec(this.options.idCodec, { schema: metadata.idSchema, version: metadata.idVersion })
    );
  }
  migrateEntry(entry: EncodedStateEntry): EncodedStateEntry {
    return {
      id: this.encodeId(this.entryId(entry)),
      value: encode(this.options, decode(this.options.codec, entry.value, this.metadata)),
    };
  }
  captureEntry(runtime: ComposedRuntime, address: string | number | undefined): EncodedStateEntry | undefined {
    const id = this.options.idCodec.decode(address);
    const cell = this.map(runtime).get(id);
    if (!cell || Object.is(cell.signal.get(), cell.initial)) return undefined;
    return { id: this.encodeId(id), value: encode(this.options, cell.signal.get()) };
  }
  *captureEntries(runtime: ComposedRuntime): Iterable<EncodedStateEntry> {
    // Incremental cold capture can stop at its size bound before encoding another cell.
    for (const id of this.map(runtime).keys()) {
      const entry = this.captureEntry(runtime, id);
      if (entry) yield entry;
    }
  }
  capture(runtime: ComposedRuntime): EncodedState {
    const entries = [...this.captureEntries(runtime)];
    entries.sort((a, b) => compareResourceIds(this.entryId(a), this.entryId(b)));
    return { key: this.metadata.key, entries };
  }
  restore(runtime: ComposedRuntime, state: EncodedState): void {
    const map = this.map(runtime);
    if (map.size !== 0) throw new Error('Restore requires a fresh runtime.');
    for (const entry of state.entries) {
      if (!entry.id) throw new Error('Missing resource ID in checkpoint.');
      const id = this.entryId(entry);
      if (map.has(id)) throw new Error('Duplicate resource in checkpoint.');
      map.set(id, {
        signal: atom(this.metadata.key, decode(this.options.codec, entry.value, this.metadata)),
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
  migrate(event: EncodedEvent): EncodedEvent;
  acceptsMetadata(metadata: DeclarationMetadata): boolean;
  support(value: unknown): import('./support-policy.js').SupportDecision | undefined;
}
interface ReactionBudget {
  remaining: number;
}
export abstract class Publication {
  abstract readonly kind: 'event';
  abstract readonly reactionDepth: number;
  abstract readonly reactionBudget: ReactionBudget | undefined;
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
    private readonly expectedAdmission?: boolean,
    readonly reactionDepth = 0,
    readonly reactionBudget: ReactionBudget | undefined = undefined,
  ) {
    super();
  }
  reduce(runtime: ComposedRuntime): void {
    this.admitted = this.handle.reduce(runtime, this.payload);
    if (this.expectedAdmission !== undefined && this.expectedAdmission !== this.admitted)
      throw new CaptureError({
        code: 'decision',
        boundary: 'replayed admission',
        owner: this.handle.metadata.owner,
        declaration: this.handle.metadata.name,
      });
  }
  notify(runtime: ComposedRuntime): void {
    this.handle.notify(runtime, this.payload, this.admitted);
  }
  capture(): EncodedEvent {
    return { ...this.handle.capture(this.payload), admitted: this.admitted, reactionDepth: this.reactionDepth };
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
  publication(payload: T, reactionDepth = 0, reactionBudget?: ReactionBudget): Publication {
    return new TypedPublication(this, payload, undefined, reactionDepth, reactionBudget);
  }
  capture(payload: T): EncodedEvent {
    return { key: this.metadata.key, payload: encode(this.options, payload) };
  }
  support(value: unknown) {
    if (!this.options.support) return undefined;
    return this.options.support(
      decodeCodec(this.options.codec, {
        schema: this.options.codec?.schema ?? '',
        version: this.options.codec?.version ?? 0,
        value,
      }),
    );
  }
  acceptsMetadata(metadata: DeclarationMetadata): boolean {
    return supportsCodec(this.options.codec, metadata);
  }
  migrate(event: EncodedEvent): EncodedEvent {
    return { ...event, payload: encode(this.options, decode(this.options.codec, event.payload, this.metadata)) };
  }
  decode(event: EncodedEvent): Publication {
    return new TypedPublication(
      this,
      decode(this.options.codec, event.payload, this.metadata),
      event.admitted,
      event.reactionDepth ?? 0,
    );
  }
}

export interface LibraryDefinition<Exports> {
  readonly name: string;
  readonly version?: number;
  /** Payload upgrades remain codec-owned; accepting a library version never bypasses value validation. */
  readonly previousVersions?: readonly number[];
  readonly requires: readonly { readonly name: string; resolve(binding: BindingIdentity | undefined): unknown }[];
  readonly setup: (scope: LibraryScope) => Exports;
}
export function defineLibrary<Exports>(definition: LibraryDefinition<Exports>): LibraryDefinition<Exports> {
  if (definition.version !== undefined && (!Number.isSafeInteger(definition.version) || definition.version < 1))
    throw new RangeError('Library version must be a positive safe integer.');
  for (const previous of definition.previousVersions ?? [])
    if (!Number.isSafeInteger(previous) || previous < 1 || previous >= (definition.version ?? 1))
      throw new RangeError('Accepted library versions must precede the current version.');
  return Object.freeze({
    ...definition,
    previousVersions: Object.freeze([...(definition.previousVersions ?? [])]),
    requires: Object.freeze([...definition.requires]),
  });
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
  readonly requiredEffects: EffectCodecDescriptor[] = [];
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
    options: DeclarationOptions<T> & {
      readonly idCodec: ValueCodec<ID>;
      readonly classifyId?: (id: ID) => SupportClassification;
      readonly supportId?: SupportPolicy<ID>;
    },
  ): KeyedHandle<T, ID> {
    const handle = new KeyedHandle(
      this.ownerToken,
      Object.freeze({
        ...this.metadata(name, 'keyed', options),
        idSchema: options.idCodec.schema,
        idVersion: options.idCodec.version,
      }),
      initial,
      options,
    );
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
  /** Pure successor-wave publication. Replay consumes the recorded outputs instead of generating them twice. */
  react<Input, Output>(
    source: EventHandle<Input>,
    target: EventHandle<Output>,
    plan: (state: StateReader, event: Input) => Output | undefined,
  ): void {
    if (this.sealed) throw new Error('Cannot change a mounted library.');
    this.references.add(source.ownerToken);
    this.references.add(target.ownerToken);
    this.installations.push((runtime) => {
      if (runtime.mode === 'replay') return;
      runtime.listen(source, (event, admitted) => {
        if (!admitted) return;
        const result = plan(runtime.reader, event);
        if (result !== undefined) runtime.publishReaction(target, result);
      });
    });
  }
  /** A checked cold-path requirement, not a dependency container or fallback interpreter. */
  requireEffect<Effect extends EffectCodecDescriptor>(effect: Effect): Effect {
    if (this.sealed || effect.ownerToken !== this.ownerToken || this.requiredEffects.includes(effect))
      throw new Error('Foreign, duplicate or closed required effect.');
    this.requiredEffects.push(effect);
    return effect;
  }
  /** @internal */ seal(): void {
    this.sealed = true;
    Object.freeze(this.states);
    Object.freeze(this.events);
    Object.freeze(this.installations);
    Object.freeze(this.requiredEffects);
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

export type RuntimeDiagnosticPhase = 'boundary' | 'planner' | 'decoder' | 'reducer' | 'reaction' | 'cleanup';
export interface RuntimeDiagnostic {
  readonly code: 'runtime-boundary-failure';
  readonly phase: RuntimeDiagnosticPhase;
  readonly wave: number;
}
export interface RuntimeOptions {
  readonly scheduler?: DispatchScheduler;
  readonly mode?: 'live' | 'replay';
  readonly maxReactionSteps?: number;
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
  private reactionDepth = 0;
  private reactionBudget: ReactionBudget | undefined;
  private readonly pendingExecutions = new Set<Promise<void>>();
  private readonly providedEffects = new Set<EffectCodecDescriptor>();
  private readinessChecked = false;
  private readonly diagnosticListeners = new Set<(diagnostic: RuntimeDiagnostic) => void>();
  private reportingDiagnostic = false;

  get waveNumber(): number {
    return this.wave;
  }
  private capturedWrite: ((state: StateDeclaration, id: string | number | undefined) => void) | undefined;
  /** @internal Only an execution-disabled checkpoint interpreter needs incremental write capture. */
  observeCapturedWrites(listener: (state: StateDeclaration, id: string | number | undefined) => void): () => void {
    if (this.mode !== 'replay' || this.capturedWrite)
      throw new Error('Checkpoint write capture requires an unobserved replay runtime.');
    this.capturedWrite = listener;
    return this.manage(() => {
      this.capturedWrite = undefined;
    });
  }
  /** @internal One optional branch when capture is absent; no live dirty map or payload allocation. */
  markCapturedWrite(state: StateDeclaration, id: string | number | undefined): void {
    this.capturedWrite?.(state, id);
  }

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
    if (!Number.isSafeInteger(options.maxReactionSteps ?? 64) || (options.maxReactionSteps ?? 64) < 1)
      throw new RangeError('maxReactionSteps must be a positive safe integer.');
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
  /** Fail before rendering a feature with a declared but unbound interpreter. */
  assertReady(): void {
    if (this.disposed) throw new Error('Disposed runtime.');
    if (this.readinessChecked || this.mode === 'replay') return;
    for (const mount of this.composition.mounts)
      for (const requirement of mount.scope.requiredEffects)
        if (!this.providedEffects.has(requirement))
          throw new Error(`Missing required effect: ${requirement.metadata.owner}/${requirement.metadata.key}`);
    this.readinessChecked = true;
  }
  /** @internal Called by the existing effect binder, never by screen components. */
  provideEffect(effect: EffectCodecDescriptor): () => void {
    this.assertOwner(effect.ownerToken);
    this.providedEffects.add(effect);
    this.readinessChecked = false;
    return this.manage(() => {
      this.providedEffects.delete(effect);
      this.readinessChecked = false;
    });
  }
  /** @internal Track actual completion, including cooperative iterator finalization. */
  trackExecution(task: Promise<void>): void {
    this.pendingExecutions.add(task);
    void task.then(
      () => {
        this.pendingExecutions.delete(task);
      },
      (cause: unknown) => {
        this.pendingExecutions.delete(task);
        this.reportError(cause, 'cleanup');
      },
    );
  }
  async drain(): Promise<void> {
    do {
      if (!this.disposed) this.flush();
      if (this.pendingExecutions.size === 0) return;
      await Promise.allSettled([...this.pendingExecutions]);
    } while (this.pendingExecutions.size > 0 || (!this.disposed && !this.idle));
  }
  /** Abort is not physical termination. This awaits actual settlement and can wait on non-cooperative work. */
  async disposeAsync(): Promise<void> {
    this.dispose();
    await this.drain();
  }
  /** @internal Pure library reactions are the only producer of nonzero reaction depth. */
  publishReaction<T>(event: EventHandle<T>, payload: NoInfer<T>): void {
    this.assertOwner(event.ownerToken);
    if (this.mode === 'replay') throw new Error('Replay must not regenerate recorded reactions.');
    if (!this.reactionBudget) this.reactionBudget = { remaining: this.options.maxReactionSteps ?? 64 };
    const budget = this.reactionBudget;
    if (budget.remaining === 0)
      throw new CaptureError({
        code: 'size-limit',
        boundary: 'reaction steps',
        limit: this.options.maxReactionSteps ?? 64,
      });
    budget.remaining--;
    this.queue.publish(event.publication(payload, this.reactionDepth + 1, budget));
  }
  observeDiagnostics(listener: (diagnostic: RuntimeDiagnostic) => void): () => void {
    this.diagnosticListeners.add(listener);
    return this.manage(() => {
      this.diagnosticListeners.delete(listener);
    });
  }
  private diagnostic(phase: RuntimeDiagnosticPhase): void {
    if (this.reportingDiagnostic || this.diagnosticListeners.size === 0 || this.disposed) return;
    this.reportingDiagnostic = true;
    try {
      const diagnostic: RuntimeDiagnostic = Object.freeze({ code: 'runtime-boundary-failure', phase, wave: this.wave });
      for (const listener of this.diagnosticListeners) {
        try {
          listener(diagnostic);
        } catch {
          /* A failing diagnostic observer cannot recursively diagnose itself. */
        }
      }
    } finally {
      this.reportingDiagnostic = false;
    }
  }
  reportError(cause: unknown, phase: RuntimeDiagnosticPhase = 'boundary'): void {
    this.diagnostic(phase);
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
        this.diagnostic('reducer');
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
        if (event?.kind === 'event') {
          this.reactionDepth = event.reactionDepth;
          this.reactionBudget = event.reactionBudget;
          event.notify(this);
          this.reactionDepth = 0;
          this.reactionBudget = undefined;
        } else if (event) this.interestBatch.append(event.changes);
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
    this.diagnosticListeners.clear();
    this.providedEffects.clear();
    for (const event of this.composition.events.values()) event.release(this);
    for (const state of this.composition.states) state.release(this);
    this.closed = true;
    this.closing = false;
  }
}
