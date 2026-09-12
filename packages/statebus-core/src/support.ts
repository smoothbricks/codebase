import { ArchiveFailure, type ArchiveLimits, canonicalJson, compareText } from './archive-json.js';
import type { CaptureProvenance, LibraryHistory, ReplayEnvelope } from './archive.js';
import type {
  DeclarationMetadata,
  EncodedState,
  EncodedValue,
  EventHandle,
  KeyedHandle,
  MountedLibrary,
  ScalarHandle,
  StateBusComposition,
  ValueCodec,
} from './composition.js';
import type { EffectDefinition, EffectOutcome, EffectPlan } from './effects.js';
import type { RecordedEffectOutcome } from './recording.js';

export type SupportAtom = string | number | boolean | null;
export type SupportFields = Readonly<Record<string, SupportAtom>>;
export interface SupportField<T> {
  readonly name: string;
  /** Return only the explicitly approved scalar field; undefined omits it. Objects cannot pass through. */
  readonly select: (value: T) => SupportAtom | undefined;
}
export interface SupportEntry {
  readonly id?: SupportAtom;
  readonly fields: SupportFields;
}
export interface StateSupportPolicy {
  readonly ownerToken: object;
  readonly metadata: DeclarationMetadata;
  readonly name: string;
  readonly project: (entry: EncodedState['entries'][number]) => SupportEntry | undefined;
}
export interface EventSupportPolicy {
  readonly ownerToken: object;
  readonly metadata: DeclarationMetadata;
  readonly name: string;
  readonly project: (value: EncodedValue) => SupportFields | undefined;
}
export interface OutcomeSupportPolicy {
  readonly ownerToken: object;
  readonly key: string;
  readonly name: string;
  readonly project: (outcome: RecordedEffectOutcome) => SupportFields | undefined;
}
function checkedAtom(value: SupportAtom): SupportAtom {
  if (value !== null && typeof value !== 'string' && typeof value !== 'boolean' &&
    (typeof value !== 'number' || !Number.isFinite(value))) throw new ArchiveFailure('json');
  return value;
}
function compileFields<T>(fields: readonly SupportField<T>[]): (value: T) => SupportFields | undefined {
  const ordered = fields.map((field) => Object.freeze({ name: field.name, select: field.select }))
    .sort((a, b) => compareText(a.name, b.name));
  if (ordered.some((field) => !field.name) || new Set(ordered.map((field) => field.name)).size !== ordered.length)
    throw new Error('Support field names must be nonempty and unique.');
  return (value) => {
    const result: [string, SupportAtom][] = [];
    for (const field of ordered) {
      const selected = field.select(value);
      if (selected !== undefined) result.push([field.name, checkedAtom(selected)]);
    }
    return result.length === 0 ? undefined : Object.freeze(Object.fromEntries(result));
  };
}
function compatible<T>(codec: ValueCodec<T>, encoded: EncodedValue): boolean {
  return (encoded.classification === 'public' || encoded.classification === 'sensitive') &&
    encoded.schema === codec.schema && encoded.version === codec.version;
}
export function supportScalar<T>(
  handle: ScalarHandle<T>,
  options: { readonly name: string; readonly codec: ValueCodec<NoInfer<T>>; readonly fields: readonly SupportField<NoInfer<T>>[] },
): StateSupportPolicy {
  const { name, codec } = options;
  const project = compileFields(options.fields);
  return Object.freeze({
    ownerToken: handle.ownerToken,
    metadata: handle.metadata,
    name,
    project: (entry: EncodedState['entries'][number]) => {
      if (entry.id !== undefined || !compatible(codec, entry.value)) return undefined;
      const fields = project(codec.decode(entry.value.value));
      return fields && Object.freeze({ fields });
    },
  });
}
export function supportKeyed<T, ID extends string | number>(
  handle: KeyedHandle<T, ID>,
  options: {
    readonly name: string;
    readonly codec: ValueCodec<NoInfer<T>>;
    readonly fields: readonly SupportField<NoInfer<T>>[];
    readonly id: { readonly codec: ValueCodec<NoInfer<ID>>; readonly select: (id: NoInfer<ID>) => SupportAtom | undefined };
  },
): StateSupportPolicy {
  const { name, codec } = options;
  const idCodec = options.id.codec;
  const selectId = options.id.select;
  const project = compileFields(options.fields);
  return Object.freeze({
    ownerToken: handle.ownerToken,
    metadata: handle.metadata,
    name,
    project: (entry: EncodedState['entries'][number]) => {
      if (!entry.id || !compatible(idCodec, entry.id) || !compatible(codec, entry.value)) return undefined;
      const id = selectId(idCodec.decode(entry.id.value));
      // A refused ID drops the whole record before decoding its value.
      if (id === undefined) return undefined;
      const fields = project(codec.decode(entry.value.value));
      return fields && Object.freeze({ id: checkedAtom(id), fields });
    },
  });
}
export function supportEvent<T>(
  handle: EventHandle<T>,
  options: { readonly name: string; readonly codec: ValueCodec<NoInfer<T>>; readonly fields: readonly SupportField<NoInfer<T>>[] },
): EventSupportPolicy {
  const { name, codec } = options;
  const project = compileFields(options.fields);
  return Object.freeze({
    ownerToken: handle.ownerToken,
    metadata: handle.metadata,
    name,
    project: (encoded: EncodedValue) => compatible(codec, encoded) ? project(codec.decode(encoded.value)) : undefined,
  });
}
export function supportEffect<C, P extends EffectPlan, O, R>(
  effect: EffectDefinition<C, P, O, R>,
  options: { readonly name: string; readonly fields: readonly SupportField<EffectOutcome<NoInfer<P>, NoInfer<O>>>[] },
): OutcomeSupportPolicy {
  const codec = effect.codec;
  if (!codec) throw new Error('A support outcome policy needs the effect codec.');
  const project = compileFields(options.fields);
  return Object.freeze({
    ownerToken: effect.command.ownerToken,
    key: effect.metadata.key,
    name: options.name,
    project: (outcome: RecordedEffectOutcome) =>
      outcome.effect === effect.metadata.key && outcome.schema === codec.schema && outcome.version === codec.version
        ? project(codec.decode(outcome.value)) : undefined,
  });
}
export interface SupportMount {
  readonly mount: MountedLibrary<unknown>;
  readonly history: LibraryHistory;
  /** A deliberate public alias, never an inferred owner/resource/library name. */
  readonly name: string;
}
export interface SupportEnvelope extends CaptureProvenance {
  readonly formatVersion: 1;
  readonly kind: 'statebus-support';
  readonly replayable: false;
  readonly sourceComplete: boolean;
  readonly libraries: readonly { readonly name: string; readonly version: number }[];
  readonly states: readonly { readonly library: string; readonly name: string; readonly entries: readonly SupportEntry[] }[];
  readonly waves: readonly {
    readonly wave: number;
    readonly events: readonly { readonly library: string; readonly name: string; readonly fields: SupportFields }[];
  }[];
  readonly outcomes: readonly { readonly library: string; readonly name: string; readonly fields: SupportFields }[];
}
export type SupportExport =
  | { readonly kind: 'exported'; readonly envelope: SupportEnvelope; readonly json: string; readonly bytes: number }
  | { readonly kind: 'refused'; readonly code: 'input' | 'policy' | 'limit' };
export interface SupportExporter {
  /** `provenance` is explicitly approved output, NOT copied from the potentially private source envelope. */
  export(source: ReplayEnvelope, provenance: CaptureProvenance): SupportExport;
}
export function createSupportExporter(
  composition: StateBusComposition,
  options: {
    readonly mounts?: readonly SupportMount[];
    readonly states?: readonly StateSupportPolicy[];
    readonly events?: readonly EventSupportPolicy[];
    readonly outcomes?: readonly OutcomeSupportPolicy[];
    readonly limits?: ArchiveLimits;
  } = {},
): SupportExporter {
  const limits = Object.freeze({ ...options.limits });
  canonicalJson(null, limits);
  const mounts = new Map((options.mounts ?? []).map((entry) => [entry.mount.owner, Object.freeze({ ...entry })]));
  if (mounts.size !== (options.mounts ?? []).length ||
    new Set([...mounts.values()].map((entry) => entry.name)).size !== mounts.size) throw new Error('Duplicate support mount.');
  for (const entry of mounts.values())
    if (!entry.name || !composition.mounts.includes(entry.mount) || entry.history.definition !== entry.mount.definition)
      throw new Error('Foreign or unnamed support mount.');
  const byToken = new Map([...mounts.values()].map((entry) => [entry.mount.scope.ownerToken, entry]));
  function policies<T extends { readonly ownerToken: object; readonly name: string }>(
    entries: readonly T[], key: (entry: T) => string,
  ): ReadonlyMap<string, T> {
    const result = new Map<string, T>();
    const names = new Map<object, Set<string>>();
    for (const entry of entries) {
      if (!entry.name || !byToken.has(entry.ownerToken) || result.has(key(entry))) throw new Error('Foreign or duplicate support policy.');
      let used = names.get(entry.ownerToken);
      if (!used) names.set(entry.ownerToken, used = new Set());
      if (used.has(entry.name)) throw new Error('Duplicate support alias.');
      used.add(entry.name);
      result.set(key(entry), entry);
    }
    return result;
  }
  const states = policies(options.states ?? [], (entry) => entry.metadata.key);
  const events = policies(options.events ?? [], (entry) => entry.metadata.key);
  const outcomes = policies(options.outcomes ?? [], (entry) => entry.key);
  const exporter: SupportExporter = {
    export(source, provenance) {
      if (source.kind !== 'statebus-replay' || source.formatVersion !== 1) return { kind: 'refused', code: 'input' };
      try {
        const revisions = new Map(source.libraries.map((entry) => [entry.owner, entry]));
        if (revisions.size !== source.libraries.length) return { kind: 'refused', code: 'input' };
        const admitted = new Map([...mounts].filter(([owner, entry]) => {
          const revision = revisions.get(owner);
          return revision?.library === entry.mount.definition.name && revision.version === entry.history.version;
        }));
        const schema = new Map(source.scenario.checkpoint.schema.map((entry) => [entry.key, entry]));
        if (schema.size !== source.scenario.checkpoint.schema.length) return { kind: 'refused', code: 'input' };
        function ownerFor(policy: StateSupportPolicy | EventSupportPolicy): SupportMount | undefined {
          const entry = schema.get(policy.metadata.key);
          if (!entry || entry.owner !== policy.metadata.owner || entry.name !== policy.metadata.name ||
            entry.kind !== policy.metadata.kind || entry.schema !== policy.metadata.schema || entry.version !== policy.metadata.version)
            return undefined;
          return admitted.get(entry.owner);
        }
        const retainedStates: SupportEnvelope['states'][number][] = [];
        for (const state of source.scenario.checkpoint.states) {
          const policy = states.get(state.key);
          const owner = policy && ownerFor(policy);
          if (!policy || !owner) continue;
          const entries: SupportEntry[] = [];
          for (const entry of state.entries) {
            const selected = policy.project(entry);
            if (selected) entries.push(selected);
          }
          if (entries.length > 0) retainedStates.push(Object.freeze({
            library: owner.name,
            name: policy.name,
            entries: Object.freeze(entries.sort((a, b) => compareText(canonicalJson(a, limits), canonicalJson(b, limits)))),
          }));
        }
        const waves: SupportEnvelope['waves'][number][] = [];
        for (const wave of source.scenario.waves) {
          const selected: SupportEnvelope['waves'][number]['events'][number][] = [];
          for (const event of wave.events) {
            const policy = events.get(event.key);
            const owner = policy && ownerFor(policy);
            if (!policy || !owner) continue;
            const fields = policy.project(event.payload);
            if (fields) selected.push(Object.freeze({ library: owner.name, name: policy.name, fields }));
          }
          if (selected.length > 0) waves.push(Object.freeze({ wave: wave.wave, events: Object.freeze(selected) }));
        }
        const selectedOutcomes: SupportEnvelope['outcomes'][number][] = [];
        for (const entry of source.outcomes) {
          const policy = outcomes.get(entry.record.effect);
          const owner = policy && byToken.get(policy.ownerToken);
          if (!policy || !owner || owner.mount.owner !== entry.owner || !admitted.has(entry.owner)) continue;
          const fields = policy.project(entry.record);
          if (fields) selectedOutcomes.push(Object.freeze({ library: owner.name, name: policy.name, fields }));
        }
        const envelope: SupportEnvelope = Object.freeze({
          formatVersion: 1,
          kind: 'statebus-support',
          replayable: false,
          sourceComplete: source.scenario.complete,
          build: Object.freeze({ application: provenance.build.application, revision: provenance.build.revision }),
          environment: Object.freeze({ runtime: provenance.environment.runtime, platform: provenance.environment.platform }),
          libraries: Object.freeze([...admitted.values()].map((entry) => Object.freeze({
            name: entry.name, version: entry.history.version,
          })).sort((a, b) => compareText(a.name, b.name))),
          states: Object.freeze(retainedStates.sort((a, b) => compareText(a.library, b.library) || compareText(a.name, b.name))),
          waves: Object.freeze(waves),
          outcomes: Object.freeze(selectedOutcomes),
        });
        const json = canonicalJson(envelope, limits);
        return Object.freeze({ kind: 'exported', envelope, json, bytes: new TextEncoder().encode(json).byteLength });
      } catch (cause) {
        return { kind: 'refused', code: cause instanceof ArchiveFailure && cause.code === 'limit' ? 'limit' : 'policy' };
      }
    },
  };
  return Object.freeze(exporter);
}
