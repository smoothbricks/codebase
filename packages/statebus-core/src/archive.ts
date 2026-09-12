import { ArchiveFailure, type ArchiveLimits, canonicalJson, compareText, ownJson } from './archive-json.js';
import type {
  ComposedRuntime,
  DeclarationMetadata,
  EncodedEvent,
  EncodedState,
  EncodedValue,
  LibraryDefinition,
  StateBusComposition,
} from './composition.js';
import type { EffectDefinition, EffectOutcome, EffectPlan } from './effects.js';
import {
  captureEffectOutcome,
  type RecordedEffectOutcome,
  type RecordedScenario,
  replayScenario,
} from './recording.js';

export type { ArchiveLimits } from './archive-json.js';
export interface CaptureProvenance {
  readonly build: { readonly application: string; readonly revision: string };
  readonly environment: { readonly runtime: string; readonly platform: string };
}
export interface ArchiveOutcome {
  readonly owner: string;
  readonly record: RecordedEffectOutcome;
}
export function recordArchiveOutcome<C, P extends EffectPlan, O, R>(
  definition: EffectDefinition<C, P, O, R>,
  outcome: EffectOutcome<NoInfer<P>, NoInfer<O>>,
): ArchiveOutcome {
  return { owner: definition.metadata.owner, record: captureEffectOutcome(definition, outcome) };
}
export interface LibraryRevision {
  readonly owner: string;
  readonly library: string;
  readonly version: number;
}
export interface ReplayEnvelope extends CaptureProvenance {
  readonly formatVersion: 1;
  readonly kind: 'statebus-replay';
  readonly libraries: readonly LibraryRevision[];
  readonly scenario: RecordedScenario;
  /** Side history only. Replay never publishes these outcomes a second time. */
  readonly outcomes: readonly ArchiveOutcome[];
}
export interface LibraryRecording {
  readonly schema: readonly DeclarationMetadata[];
  readonly states: readonly EncodedState[];
  /** Original relative publication order; a migration must preserve cardinality. */
  readonly events: readonly EncodedEvent[];
  readonly outcomes: readonly RecordedEffectOutcome[];
}
export interface LibraryMigrationContext {
  readonly owner: string;
  readonly from: number;
  readonly to: number;
  readonly targetSchema: readonly DeclarationMetadata[];
}
export interface LibraryMigration {
  readonly from: number;
  /** Pure, library-owned conversion from `from` to `from + 1`. No runtime or operations are provided. */
  readonly migrate: (recording: LibraryRecording, context: LibraryMigrationContext) => LibraryRecording;
}
export interface LibraryHistory<Exports = unknown> {
  readonly definition: LibraryDefinition<Exports>;
  readonly version: number;
  readonly migrations: readonly LibraryMigration[];
}
export function defineLibraryHistory<Exports>(
  definition: LibraryDefinition<Exports>,
  history: { readonly version: number; readonly migrations?: readonly LibraryMigration[] },
): LibraryHistory<Exports> {
  if (!Number.isSafeInteger(history.version) || history.version < 1)
    throw new RangeError('Library versions must be positive safe integers.');
  const seen = new Set<number>();
  const migrations = (history.migrations ?? []).map((step) => {
    if (!Number.isSafeInteger(step.from) || step.from < 1 || step.from >= history.version || seen.has(step.from))
      throw new Error('Invalid or duplicate library migration.');
    seen.add(step.from);
    return Object.freeze({ from: step.from, migrate: step.migrate });
  });
  return Object.freeze({ definition, version: history.version, migrations: Object.freeze(migrations) });
}
export type ArchiveResult<T> =
  | { readonly kind: 'ok'; readonly value: T }
  | { readonly kind: 'refused'; readonly code: ArchiveFailure['code'] };
export interface AppliedMigration {
  readonly owner: string;
  readonly from: number;
  readonly to: number;
}
export interface MigratedEnvelope {
  readonly envelope: ReplayEnvelope;
  readonly applied: readonly AppliedMigration[];
}
export interface ReplayedEnvelope {
  readonly runtime: ComposedRuntime;
  readonly outcomes: readonly ArchiveOutcome[];
  readonly applied: readonly AppliedMigration[];
}
export type ArchiveEffect = Pick<EffectDefinition<unknown, EffectPlan, unknown, unknown>, 'metadata' | 'codec'>;
export interface ReplayArchive {
  capture(
    scenario: RecordedScenario,
    provenance: CaptureProvenance,
    outcomes?: readonly ArchiveOutcome[],
  ): ArchiveResult<ReplayEnvelope>;
  /** Accept a typed envelope validated at the caller's storage boundary, e.g. with Typia assertParse. */
  migrate(envelope: ReplayEnvelope): ArchiveResult<MigratedEnvelope>;
  replay(envelope: ReplayEnvelope): ArchiveResult<ReplayedEnvelope>;
  stringify(envelope: ReplayEnvelope): ArchiveResult<string>;
}
function attempt<T>(action: () => T, fallback: ArchiveFailure['code'] = 'codec'): ArchiveResult<T> {
  try {
    return { kind: 'ok', value: action() };
  } catch (cause) {
    return { kind: 'refused', code: cause instanceof ArchiveFailure ? cause.code : fallback };
  }
}
function indexByKey<T extends { readonly key: string }>(entries: readonly T[]): Map<string, T> {
  const index = new Map(entries.map((entry) => [entry.key, entry]));
  if (index.size !== entries.length) throw new ArchiveFailure('schema');
  return index;
}
function metadata(entry: DeclarationMetadata): DeclarationMetadata {
  // Descriptions are documentation, not part of the replay schema or a portable failure message.
  return Object.freeze({
    key: entry.key,
    owner: entry.owner,
    name: entry.name,
    kind: entry.kind,
    schema: entry.schema,
    version: entry.version,
  });
}
function ownedValue(value: EncodedValue, limits: ArchiveLimits): EncodedValue {
  return Object.freeze({
    schema: value.schema,
    version: value.version,
    classification: value.classification,
    value: ownJson(value.value, limits),
  });
}
function ownedRecording(source: LibraryRecording, limits: ArchiveLimits): LibraryRecording {
  return Object.freeze({
    schema: Object.freeze(source.schema.map(metadata).sort((a, b) => compareText(a.key, b.key))),
    states: Object.freeze(
      source.states.map((state) => Object.freeze({
        key: state.key,
        entries: Object.freeze(
          state.entries.map((entry) => Object.freeze({
            ...(entry.id ? { id: ownedValue(entry.id, limits) } : {}),
            value: ownedValue(entry.value, limits),
          })).sort((a, b) => compareText(canonicalJson(a.id ?? null, limits), canonicalJson(b.id ?? null, limits))),
        ),
      })).sort((a, b) => compareText(a.key, b.key)),
    ),
    events: Object.freeze(source.events.map((event) => Object.freeze({
      key: event.key,
      payload: ownedValue(event.payload, limits),
    }))),
    outcomes: Object.freeze(source.outcomes.map((outcome) => Object.freeze({
      effect: outcome.effect,
      schema: outcome.schema,
      version: outcome.version,
      value: ownJson(outcome.value, limits),
    }))),
  });
}
function sameSchema(before: DeclarationMetadata, after: DeclarationMetadata): boolean {
  return before.key === after.key && before.owner === after.owner && before.name === after.name &&
    before.kind === after.kind && before.schema === after.schema && before.version === after.version;
}
function checkRecording(owner: string, recording: LibraryRecording): void {
  const schema = indexByKey(recording.schema);
  const names = new Set<string>();
  for (const entry of schema.values()) {
    if (entry.owner !== owner || names.has(entry.name) || !entry.schema ||
      !Number.isSafeInteger(entry.version) || (entry.version ?? 0) < 1)
      throw new ArchiveFailure('schema');
    names.add(entry.name);
  }
  const states = indexByKey(recording.states);
  for (const entry of schema.values())
    if ((entry.kind === 'scalar' || entry.kind === 'keyed') !== states.has(entry.key))
      throw new ArchiveFailure('schema');
  for (const state of states.values()) {
    const declaration = schema.get(state.key);
    if (!declaration || (declaration.kind !== 'scalar' && declaration.kind !== 'keyed'))
      throw new ArchiveFailure('schema');
    if (declaration.kind === 'scalar' && (state.entries.length !== 1 || state.entries[0].id !== undefined))
      throw new ArchiveFailure('schema');
    for (const entry of state.entries)
      if ((declaration.kind === 'keyed' && !entry.id) || entry.value.schema !== declaration.schema ||
        entry.value.version !== declaration.version) throw new ArchiveFailure('schema');
  }
  for (const event of recording.events) {
    const declaration = schema.get(event.key);
    if (!declaration || (declaration.kind !== 'event' && declaration.kind !== 'command') ||
      declaration.schema !== event.payload.schema || declaration.version !== event.payload.version)
      throw new ArchiveFailure('schema');
  }
  const events = recording.schema.filter((entry) => entry.kind === 'event' || entry.kind === 'command');
  const eventKeys = new Set(events.map((entry) => entry.key));
  for (const outcome of recording.outcomes) {
    // Resolve the existing effect key against declared endpoints; never infer an owner by splitting a string.
    if (!events.some((event) => outcome.effect.startsWith(`${event.key}/`) &&
      eventKeys.has(outcome.effect.slice(event.key.length + 1)))) throw new ArchiveFailure('schema');
  }
}

/** A cold archive adapter over the existing composition, codecs and replay runtime; not another state store. */
export function createReplayArchive(
  composition: StateBusComposition,
  histories: readonly LibraryHistory[],
  options: { readonly effects?: readonly ArchiveEffect[]; readonly limits?: ArchiveLimits } = {},
): ReplayArchive {
  const limits = Object.freeze({ ...options.limits });
  canonicalJson(null, limits); // Validate configuration once, before observing any application data.
  const historyByDefinition = new Map(histories.map((history) => [history.definition, history]));
  if (historyByDefinition.size !== histories.length) throw new Error('Duplicate library history.');
  const mounts = new Map(composition.mounts.map((mount) => [mount.owner, mount]));
  for (const mount of mounts.values())
    if (!historyByDefinition.has(mount.definition)) throw new Error('Missing library-owned history.');
  for (const history of histories)
    if (!composition.mounts.some((mount) => mount.definition === history.definition))
      throw new Error('Foreign library history.');
  const effects = new Map((options.effects ?? []).map((effect) => [effect.metadata.key, effect]));
  if (effects.size !== (options.effects ?? []).length) throw new Error('Duplicate archive effect.');
  for (const effect of effects.values())
    if (!mounts.has(effect.metadata.owner) || !effect.codec) throw new Error('Foreign or unversioned archive effect.');
  const currentSchema = indexByKey(composition.metadata);
  const revisions: readonly LibraryRevision[] = Object.freeze(composition.mounts.map((mount) => {
    const history = historyByDefinition.get(mount.definition);
    if (!history) throw new Error('Missing archive history.');
    return Object.freeze({ owner: mount.owner, library: mount.definition.name, version: history.version });
  }).sort((a, b) => compareText(a.owner, b.owner)));

  function transform(source: ReplayEnvelope, migrate: boolean): MigratedEnvelope {
    if (source.kind !== 'statebus-replay' || source.formatVersion !== 1 ||
      source.scenario.formatVersion !== 1 || source.scenario.checkpoint.formatVersion !== 1)
      throw new ArchiveFailure('schema');
    if (migrate && !source.scenario.complete) throw new ArchiveFailure('incomplete');
    canonicalJson(source, limits); // Bound and verify portable data before invoking migration callbacks.
    const sourceRevisions = new Map(source.libraries.map((entry) => [entry.owner, entry]));
    if (sourceRevisions.size !== source.libraries.length || sourceRevisions.size !== mounts.size)
      throw new ArchiveFailure('libraries');
    const sourceSchema = indexByKey(source.scenario.checkpoint.schema);
    indexByKey(source.scenario.checkpoint.states);
    const recordings = new Map<string, LibraryRecording>();
    const applied: AppliedMigration[] = [];
    let previousWave = 0;
    for (const wave of source.scenario.waves) {
      if (!Number.isSafeInteger(wave.wave) || wave.wave <= previousWave) throw new ArchiveFailure('schema');
      previousWave = wave.wave;
      for (const event of wave.events) if (!sourceSchema.has(event.key)) throw new ArchiveFailure('schema');
    }
    for (const declaration of sourceSchema.values())
      if (!mounts.has(declaration.owner)) throw new ArchiveFailure('libraries');
    for (const state of source.scenario.checkpoint.states)
      if (!sourceSchema.has(state.key)) throw new ArchiveFailure('schema');
    for (const outcome of source.outcomes)
      if (!mounts.has(outcome.owner)) throw new ArchiveFailure('libraries');

    for (const revision of revisions) {
      const mount = mounts.get(revision.owner);
      const history = mount && historyByDefinition.get(mount.definition);
      const before = sourceRevisions.get(revision.owner);
      if (!history || !before || before.library !== revision.library || !Number.isSafeInteger(before.version) ||
        before.version < 1 || before.version > revision.version) throw new ArchiveFailure('libraries');
      const targetSchema = Object.freeze(composition.metadata.filter((entry) => entry.owner === revision.owner));
      let recording = ownedRecording({
        schema: [...sourceSchema.values()].filter((entry) => entry.owner === revision.owner),
        states: source.scenario.checkpoint.states.filter((entry) => sourceSchema.get(entry.key)?.owner === revision.owner),
        events: source.scenario.waves.flatMap((wave) => wave.events.filter((event) =>
          sourceSchema.get(event.key)?.owner === revision.owner)),
        outcomes: source.outcomes.filter((entry) => entry.owner === revision.owner).map((entry) => entry.record),
      }, limits);
      checkRecording(revision.owner, recording);
      for (let version = before.version; version < revision.version; version++) {
        const step = migrate && history.migrations.find((candidate) => candidate.from === version);
        if (!step) throw new ArchiveFailure('migration');
        const context = Object.freeze({ owner: revision.owner, from: version, to: version + 1, targetSchema });
        const result = attempt(() => step.migrate(recording, context), 'migration');
        if (result.kind === 'refused') throw new ArchiveFailure('migration');
        if (result.value.events.length !== recording.events.length || result.value.outcomes.length !== recording.outcomes.length)
          throw new ArchiveFailure('migration');
        recording = ownedRecording(result.value, limits);
        checkRecording(revision.owner, recording);
        applied.push(Object.freeze({ owner: revision.owner, from: version, to: version + 1 }));
      }
      if (recording.schema.length !== targetSchema.length) throw new ArchiveFailure('schema');
      for (const entry of recording.schema) {
        const target = currentSchema.get(entry.key);
        if (!target || !sameSchema(entry, target)) throw new ArchiveFailure('schema');
      }
      for (const outcome of recording.outcomes) {
        const effect = effects.get(outcome.effect);
        if (!effect?.codec || effect.metadata.owner !== revision.owner ||
          effect.codec.schema !== outcome.schema || effect.codec.version !== outcome.version)
          throw new ArchiveFailure('codec');
        effect.codec.decode(outcome.value);
      }
      recordings.set(revision.owner, recording);
    }
    const eventOffsets = new Map<string, number>();
    const outcomeOffsets = new Map<string, number>();
    const waves = source.scenario.waves.map((wave) => Object.freeze({
      wave: wave.wave,
      events: Object.freeze(wave.events.map((event) => {
        const owner = sourceSchema.get(event.key)?.owner;
        if (owner === undefined) throw new ArchiveFailure('schema');
        const offset = eventOffsets.get(owner) ?? 0;
        const next = recordings.get(owner)?.events[offset];
        if (!next) throw new ArchiveFailure('schema');
        eventOffsets.set(owner, offset + 1);
        return next;
      })),
    }));
    const outcomes = source.outcomes.map((entry) => {
      const offset = outcomeOffsets.get(entry.owner) ?? 0;
      const record = recordings.get(entry.owner)?.outcomes[offset];
      if (!record) throw new ArchiveFailure('schema');
      outcomeOffsets.set(entry.owner, offset + 1);
      return Object.freeze({ owner: entry.owner, record });
    });
    const envelope: ReplayEnvelope = Object.freeze({
      formatVersion: 1,
      kind: 'statebus-replay',
      build: Object.freeze({ application: source.build.application, revision: source.build.revision }),
      environment: Object.freeze({ runtime: source.environment.runtime, platform: source.environment.platform }),
      libraries: revisions,
      scenario: Object.freeze({
        formatVersion: 1,
        complete: source.scenario.complete,
        checkpoint: Object.freeze({
          formatVersion: 1,
          schema: Object.freeze([...recordings.values()].flatMap((entry) => entry.schema)),
          states: Object.freeze([...recordings.values()].flatMap((entry) => entry.states)),
        }),
        waves: Object.freeze(waves),
      }),
      outcomes: Object.freeze(outcomes),
    });
    canonicalJson(envelope, limits);
    return Object.freeze({ envelope, applied: Object.freeze(applied) });
  }
  return Object.freeze({
    capture: (scenario, provenance, outcomes = []) => attempt(() => transform({
      formatVersion: 1,
      kind: 'statebus-replay',
      build: provenance.build,
      environment: provenance.environment,
      libraries: revisions,
      scenario,
      outcomes,
    }, false).envelope),
    migrate: (envelope) => attempt(() => transform(envelope, true)),
    replay: (envelope) => attempt(() => {
      const result = transform(envelope, true);
      const scenario = result.envelope.scenario;
      // The existing replay API checks schema in composition order. Archive order is canonical instead.
      const schema = indexByKey(scenario.checkpoint.schema);
      const ordered = composition.metadata.map((entry) => {
        const recorded = schema.get(entry.key);
        if (!recorded || !sameSchema(recorded, entry)) throw new ArchiveFailure('schema');
        return recorded;
      });
      const runtime = replayScenario(composition, {
        ...scenario,
        checkpoint: { ...scenario.checkpoint, schema: ordered },
      });
      return Object.freeze({ runtime, outcomes: result.envelope.outcomes, applied: result.applied });
    }),
    stringify: (envelope) => attempt(() => canonicalJson(envelope, limits)),
  });
}
