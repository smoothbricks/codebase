import {
  CaptureError,
  type CaptureValue,
  canonicalCapture,
  captureBytes,
  captureValue,
  compareResourceIds,
} from './capture.js';
import type { DeclarationMetadata, EncodedStateEntry, EncodedValue, StateBusComposition } from './composition.js';
import type { EffectCodecDescriptor } from './effects.js';
import { ownEntry, ownEvent, type RecordedScenario, replayScenario, type StateCheckpoint } from './recording.js';
import type { RecordedEffectPosition, RollingScenario } from './rolling.js';
import { projectSupport, type SupportPolicy } from './support-policy.js';

export interface CaptureLibraryVersion {
  readonly owner: string;
  readonly name: string;
  readonly version: number;
}
export interface CaptureManifest {
  readonly libraries: readonly CaptureLibraryVersion[];
  readonly declarations: readonly DeclarationMetadata[];
  readonly effects: readonly {
    readonly key: string;
    readonly owner: string;
    readonly schema?: string;
    readonly version?: number;
    readonly instructionSchema?: string;
    readonly instructionVersion?: number;
  }[];
}
export interface CaptureEnvelope {
  readonly formatVersion: 1;
  readonly kind: 'local-replay';
  readonly application: { readonly buildId: string; readonly environment?: unknown };
  /** Set by an explicit migration; captured build identity is never overwritten. */
  readonly migratedForBuild?: string;
  readonly manifest: CaptureManifest;
  readonly scenario: RecordedScenario;
  readonly captures: readonly RecordedEffectPosition[];
  readonly evictedEffects: number;
}
export interface CaptureEnvelopeOptions {
  readonly buildId: string;
  /** Already validated application summary, not process.env, request headers or live ports. */
  readonly environment?: unknown;
  readonly effects?: readonly EffectCodecDescriptor[];
  readonly maxBytes?: number;
}

function effectMap(composition: StateBusComposition, effects: readonly EffectCodecDescriptor[]) {
  const map = new Map<string, EffectCodecDescriptor>();
  for (const effect of [...composition.mounts.flatMap((mount) => mount.scope.requiredEffects), ...effects]) {
    if (map.get(effect.metadata.key) === effect) continue;
    if (!composition.owners.has(effect.ownerToken) || map.has(effect.metadata.key))
      throw new CaptureError({
        code: 'schema',
        boundary: 'foreign or duplicate effect',
        owner: effect.metadata.owner,
        declaration: effect.metadata.key,
      });
    map.set(effect.metadata.key, effect);
  }
  return map;
}
function manifest(composition: StateBusComposition, effects: readonly EffectCodecDescriptor[]): CaptureManifest {
  const resolved = effectMap(composition, effects);
  return Object.freeze({
    libraries: Object.freeze(
      composition.mounts
        .map((mount) =>
          Object.freeze({ owner: mount.owner, name: mount.definition.name, version: mount.definition.version ?? 1 }),
        )
        .sort((a, b) => compareResourceIds(a.owner, b.owner)),
    ),
    declarations: Object.freeze([...composition.metadata].sort((a, b) => compareResourceIds(a.key, b.key))),
    effects: Object.freeze(
      [...resolved.values()].map((effect) => effect.metadata).sort((a, b) => compareResourceIds(a.key, b.key)),
    ),
  });
}
function refuse(metadata: DeclarationMetadata | undefined, boundary: string): never {
  throw new CaptureError({
    code: 'schema',
    boundary,
    owner: metadata?.owner,
    declaration: metadata?.name,
    schema: metadata?.schema,
    fromVersion: metadata?.version,
  });
}
function validateVersions(composition: StateBusComposition, capture: CaptureManifest, allowPrevious: boolean): void {
  const remaining = new Map(composition.mounts.map((mount) => [mount.owner, mount]));
  for (const library of capture.libraries) {
    const current = remaining.get(library.owner);
    if (
      !current ||
      current.definition.name !== library.name ||
      !Number.isSafeInteger(library.version) ||
      library.version < 1 ||
      (library.version !== (current.definition.version ?? 1) &&
        (!allowPrevious || !current.definition.previousVersions?.includes(library.version)))
    )
      throw new CaptureError({
        code: 'schema',
        boundary: 'library version',
        owner: library.owner,
        declaration: library.name,
        fromVersion: library.version,
        toVersion: current?.definition.version ?? 1,
      });
    remaining.delete(library.owner);
  }
  if (remaining.size > 0) throw new CaptureError({ code: 'schema', boundary: 'missing library' });
}
function checkEnvelope(
  composition: StateBusComposition,
  envelope: CaptureEnvelope,
  effects: readonly EffectCodecDescriptor[],
  allowPrevious: boolean,
): void {
  if (envelope.formatVersion !== 1 || envelope.kind !== 'local-replay' || !envelope.scenario.complete)
    throw new CaptureError({ code: 'incomplete', boundary: 'local replay envelope' });
  validateVersions(composition, envelope.manifest, allowPrevious);
  if (
    canonicalCapture(envelope.manifest.declarations) !==
    canonicalCapture([...envelope.scenario.checkpoint.schema].sort((a, b) => compareResourceIds(a.key, b.key)))
  )
    throw new CaptureError({ code: 'schema', boundary: 'manifest/checkpoint declaration mismatch' });
  const headers = new Map(envelope.manifest.declarations.map((entry) => [entry.key, entry]));
  if (headers.size !== envelope.manifest.declarations.length) refuse(undefined, 'duplicate manifest declaration');
  const stateKeys = new Set<string>();
  for (const state of envelope.scenario.checkpoint.states) {
    const header = headers.get(state.key);
    if (!header || (header.kind !== 'scalar' && header.kind !== 'keyed') || stateKeys.has(state.key))
      refuse(header, 'invalid checkpoint declaration');
    stateKeys.add(state.key);
    for (const entry of state.entries) {
      if (
        entry.value.schema !== header.schema ||
        entry.value.version !== header.version ||
        (header.kind === 'keyed' &&
          (!entry.id || entry.id.schema !== header.idSchema || entry.id.version !== header.idVersion)) ||
        (header.kind === 'scalar' && entry.id)
      )
        refuse(header, 'value header differs from manifest');
    }
  }
  for (const wave of envelope.scenario.waves)
    for (const event of wave.events) {
      const header = headers.get(event.key);
      if (
        !header ||
        (header.kind !== 'event' && header.kind !== 'command') ||
        event.payload.schema !== header.schema ||
        event.payload.version !== header.version
      )
        refuse(header, 'event header differs from manifest');
    }
  const current = manifest(composition, effects);
  if (!allowPrevious && canonicalCapture(current) !== canonicalCapture(envelope.manifest))
    throw new CaptureError({ code: 'schema', boundary: 'composition/effect codec manifest' });
}

/** Lossless local capture; this is not permission to upload it. Structural import validation belongs in an application codec. */
export function createCaptureEnvelope(
  composition: StateBusComposition,
  scenario: RecordedScenario | RollingScenario,
  options: CaptureEnvelopeOptions,
): CaptureEnvelope {
  if (!options.buildId) throw new Error('A capture needs its application build ID.');
  const effects = options.effects ?? [];
  const captured: CaptureEnvelope = Object.freeze({
    formatVersion: 1,
    kind: 'local-replay',
    application: Object.freeze({
      buildId: options.buildId,
      ...(options.environment === undefined
        ? {}
        : { environment: captureValue(options.environment, options.maxBytes).value }),
    }),
    manifest: manifest(composition, effects),
    scenario: Object.freeze({
      formatVersion: scenario.formatVersion,
      checkpoint: scenario.checkpoint,
      waves: scenario.waves,
      complete: scenario.complete,
      checkpointWave: scenario.checkpointWave,
    }),
    captures: 'effects' in scenario ? scenario.effects : Object.freeze([]),
    evictedEffects: 'evictedEffects' in scenario ? scenario.evictedEffects : 0,
  });
  checkEnvelope(composition, captured, effects, false);
  // Every retained outcome must have a declared codec version in the same mounted composition.
  const codecs = effectMap(composition, effects);
  for (const { capture } of captured.captures) {
    const descriptor = codecs.get(capture.effect);
    if (
      !descriptor ||
      (capture.kind === 'instruction' ? descriptor.metadata.instructionSchema : descriptor.metadata.schema) !==
        capture.schema ||
      (capture.kind === 'instruction' ? descriptor.metadata.instructionVersion : descriptor.metadata.version) !==
        capture.version
    )
      throw new CaptureError({
        code: 'schema',
        boundary: 'missing or incompatible outcome codec',
        declaration: capture.effect,
      });
  }
  captureBytes(captured, options.maxBytes);
  return captured;
}

/** Payload-only upgrades cannot change ownership, wave numbers, event order or admission decisions. */
export function migrateScenario(composition: StateBusComposition, scenario: RecordedScenario): RecordedScenario {
  if (scenario.formatVersion !== 1 || scenario.checkpoint.formatVersion !== 1 || !scenario.complete)
    throw new CaptureError({ code: 'incomplete', boundary: 'scenario migration' });
  const oldMetadata = new Map<string, DeclarationMetadata>();
  for (const entry of scenario.checkpoint.schema) {
    if (oldMetadata.has(entry.key)) refuse(entry, 'duplicate source declaration');
    oldMetadata.set(entry.key, entry);
  }
  if (oldMetadata.size !== composition.metadata.length)
    refuse(undefined, 'declaration additions/removals require an explicit new scenario');
  for (const current of composition.metadata) {
    const old = oldMetadata.get(current.key);
    if (!old || old.owner !== current.owner || old.name !== current.name || old.kind !== current.kind)
      refuse(old ?? current, 'changed declaration ownership/kind');
  }
  for (const declaration of [...composition.states, ...composition.events.values()]) {
    const old = oldMetadata.get(declaration.metadata.key);
    if (!old || !declaration.acceptsMetadata(old))
      refuse(old ?? declaration.metadata, 'unsupported declaration codec version');
  }
  function sourceValue(metadata: DeclarationMetadata, value: EncodedValue, id = false): void {
    if (
      value.schema !== (id ? metadata.idSchema : metadata.schema) ||
      value.version !== (id ? metadata.idVersion : metadata.version)
    )
      refuse(metadata, id ? 'source ID codec differs from manifest' : 'source value codec differs from manifest');
  }
  const states = new Map(scenario.checkpoint.states.map((state) => [state.key, state]));
  if (states.size !== scenario.checkpoint.states.length || states.size !== composition.states.length)
    refuse(undefined, 'checkpoint state set');
  const checkpoint: StateCheckpoint = Object.freeze({
    formatVersion: 1,
    schema: composition.metadata,
    states: Object.freeze(
      composition.states.map((declaration) => {
        const state = states.get(declaration.metadata.key);
        const before = oldMetadata.get(declaration.metadata.key);
        if (!state || !before) refuse(before, 'missing checkpoint state');
        const ids = new Set<string | number | undefined>();
        const entries = state.entries
          .map((entry) => {
            sourceValue(before, entry.value);
            if (entry.id) sourceValue(before, entry.id, true);
            const migrated = ownEntry(declaration.migrateEntry(entry));
            const id = declaration.entryId(migrated);
            if (ids.has(id)) refuse(before, 'migration collapsed distinct resource IDs');
            ids.add(id);
            return { id, migrated };
          })
          .sort((a, b) => (a.id === undefined || b.id === undefined ? 0 : compareResourceIds(a.id, b.id)))
          .map((entry) => entry.migrated);
        if (before.kind === 'scalar' && entries.length !== 1) refuse(before, 'invalid scalar checkpoint');
        return Object.freeze({ key: state.key, entries: Object.freeze(entries) });
      }),
    ),
  });
  let previousWave = scenario.checkpointWave ?? 0;
  if (!Number.isSafeInteger(previousWave) || previousWave < 0) refuse(undefined, 'invalid checkpoint position');
  const waves = scenario.waves.map((wave) => {
    if (!Number.isSafeInteger(wave.wave) || wave.wave <= previousWave)
      refuse(undefined, 'invalid causal wave position');
    previousWave = wave.wave;
    return Object.freeze({
      wave: wave.wave,
      events: Object.freeze(
        wave.events.map((event) => {
          const declaration = composition.events.get(event.key);
          const before = oldMetadata.get(event.key);
          if (!declaration || !before) refuse(before, 'unknown recorded event');
          sourceValue(before, event.payload);
          return ownEvent(declaration.migrate(event));
        }),
      ),
    });
  });
  return Object.freeze({ ...scenario, checkpoint, waves: Object.freeze(waves) });
}

export function migrateCaptureEnvelope(
  composition: StateBusComposition,
  envelope: CaptureEnvelope,
  options: CaptureEnvelopeOptions,
): CaptureEnvelope {
  if (!options.buildId) throw new Error('A migration needs its target application build ID.');
  const effects = options.effects ?? [];
  captureBytes(envelope, options.maxBytes);
  checkEnvelope(composition, envelope, effects, true);
  const codecs = effectMap(composition, effects);
  const oldEffects = new Map(envelope.manifest.effects.map((effect) => [effect.key, effect]));
  if (oldEffects.size !== envelope.manifest.effects.length || oldEffects.size !== codecs.size)
    refuse(undefined, 'changed effect declaration set');
  for (const [key, descriptor] of codecs) {
    const old = oldEffects.get(key);
    if (!old || old.owner !== descriptor.metadata.owner) refuse(undefined, 'changed effect ownership');
    if (
      !descriptor.acceptsCodec('outcome', old.schema, old.version) ||
      ((old.instructionSchema !== undefined || old.instructionVersion !== undefined) &&
        !descriptor.acceptsCodec('instruction', old.instructionSchema, old.instructionVersion))
    )
      throw new CaptureError({
        code: 'schema',
        boundary: 'unsupported effect codec version',
        owner: old.owner,
        declaration: key,
        schema: old.schema,
        fromVersion: old.version,
        toVersion: descriptor.metadata.version,
      });
  }
  let sequence = 0;
  const outcomes = envelope.captures.map((entry) => {
    const descriptor = codecs.get(entry.capture.effect);
    const old = oldEffects.get(entry.capture.effect);
    if (
      !descriptor ||
      !old ||
      entry.capture.schema !== (entry.capture.kind === 'instruction' ? old.instructionSchema : old.schema) ||
      entry.capture.version !== (entry.capture.kind === 'instruction' ? old.instructionVersion : old.version) ||
      !Number.isSafeInteger(entry.sequence) ||
      entry.sequence <= sequence ||
      !Number.isSafeInteger(entry.afterWave) ||
      entry.afterWave < 0
    )
      refuse(undefined, 'outcome codec/causal position');
    sequence = entry.sequence;
    return Object.freeze({ ...entry, capture: descriptor.migrateCapture(entry.capture) });
  });
  const result: CaptureEnvelope = Object.freeze({
    ...envelope,
    migratedForBuild: options.buildId,
    manifest: manifest(composition, effects),
    scenario: migrateScenario(composition, envelope.scenario),
    captures: Object.freeze(outcomes),
  });
  captureBytes(result, options.maxBytes);
  return result;
}

export function replayCaptureEnvelope(
  composition: StateBusComposition,
  envelope: CaptureEnvelope,
  effects: readonly EffectCodecDescriptor[] = [],
) {
  checkEnvelope(composition, envelope, effects, false);
  return replayScenario(composition, envelope.scenario);
}

export interface SupportExportOptions {
  readonly consent?: boolean;
  readonly environment?: SupportPolicy<unknown>;
  readonly effects?: readonly EffectCodecDescriptor[];
  readonly maxBytes?: number;
  readonly maxEntryBytes?: number;
}
export interface SupportArtifact {
  readonly formatVersion: 1;
  readonly kind: 'sanitized-support';
  readonly replayable: false;
  readonly reason: 'support projections are not local replay codecs';
  readonly payload: CaptureValue;
}

/** Strict export is opt-in and DENY by default, independently of local replay encoding. */
export function exportSupportCapture(
  composition: StateBusComposition,
  envelope: CaptureEnvelope,
  options: SupportExportOptions = {},
): SupportArtifact {
  const effects = options.effects ?? [];
  checkEnvelope(composition, envelope, effects, false);
  const maxBytes = options.maxBytes ?? 8 * 1024 * 1024;
  const maxEntryBytes = options.maxEntryBytes ?? 256 * 1024;
  // Refuse an oversized input before constructing projections or redaction markers.
  captureBytes(envelope, maxBytes);
  const consent = options.consent ?? false;
  const stateMap = new Map(composition.states.map((state) => [state.metadata.key, state]));
  const codecs = effectMap(composition, effects);
  function projected(value: EncodedValue, policy: SupportPolicy<unknown> | undefined): unknown {
    return projectSupport(value.classification, policy, value.value, consent, maxEntryBytes);
  }
  const states = envelope.scenario.checkpoint.states.map((state) => {
    const declaration = stateMap.get(state.key);
    if (!declaration) refuse(undefined, 'unknown support state');
    const policy = declaration.support;
    return {
      key: state.key,
      entries: state.entries.map((entry: EncodedStateEntry) => ({
        ...(entry.id ? { id: projected(entry.id, policy.id) } : {}),
        value: projected(entry.value, policy.value),
      })),
    };
  });
  const waves = envelope.scenario.waves.map((wave) => ({
    wave: wave.wave,
    events: wave.events.map((event) => {
      const declaration = composition.events.get(event.key);
      if (!declaration) refuse(undefined, 'unknown support event');
      return {
        key: event.key,
        admitted: event.admitted,
        payload: projected(event.payload, (value) => declaration.support(value)),
      };
    }),
  }));
  const outcomes = envelope.captures.map((entry) => {
    const descriptor = codecs.get(entry.capture.effect);
    if (!descriptor) refuse(undefined, 'unknown support outcome');
    return {
      sequence: entry.sequence,
      afterWave: entry.afterWave,
      effect: entry.capture.effect,
      value: projectSupport(
        entry.capture.classification ?? 'unclassified',
        () => descriptor.supportCapture(entry.capture),
        entry.capture.value,
        consent,
        maxEntryBytes,
      ),
    };
  });
  const payload = captureValue(
    {
      application: {
        buildId: envelope.application.buildId,
        environment: projectSupport(
          'unclassified',
          options.environment,
          envelope.application.environment,
          consent,
          maxEntryBytes,
        ),
      },
      libraries: envelope.manifest.libraries,
      effects: envelope.manifest.effects,
      checkpointWave: envelope.scenario.checkpointWave ?? 0,
      states,
      waves,
      captures: outcomes,
      evictedEffects: envelope.evictedEffects,
    },
    maxBytes,
  ).value;
  const result: SupportArtifact = Object.freeze({
    formatVersion: 1,
    kind: 'sanitized-support',
    replayable: false,
    reason: 'support projections are not local replay codecs',
    payload,
  });
  captureBytes(result, maxBytes);
  return result;
}
