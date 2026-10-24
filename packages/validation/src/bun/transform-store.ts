/**
 * A persistent, content-keyed store of ttsc project generations for the Bun
 * preload.
 *
 * WHY this exists: ttsc compiles a whole tsconfig project to deliver one
 * module, and a Bun runtime keeps that generation only for its own process.
 * Every `bun test` process, and every child that runs `bun --preload`, paid the
 * whole compile again for every project it touched. Measured on nx-plugin's
 * `release-config-policy.test.ts` (one lib project): 7-19 s per project and
 * process, of which the native transform itself was under 1 s. The rest was
 * plugin-binary key resolution (`go env`, GOROOT and plugin-source hashing),
 * plugin descriptor evaluation and the generation's own proofs, all repeated
 * per process.
 *
 * A stored generation is served only while everything it was derived from is
 * unchanged:
 *
 * - The transformer ({@link transformerIdentity}): this preload's own modules,
 *   the content of the `@ttsc/unplugin` and `ttsc` packages (a patch rewrites
 *   both JS and Go there), the platform binary package's install path and
 *   manifest, the native sources of every active plugin package, the tsgo
 *   binary, and every `TTSC_*`, `GO*` and `CGO_*` variable.
 * - The project walk ttsc itself validates its cache against
 *   (`collectProjectInputHashSnapshot` under the config's membership policy):
 *   every project file's bytes and the directory membership, so a new root file
 *   or a new global declaration anywhere in the project invalidates.
 * - Every watch input ttsc reported for every stored module, compared with
 *   `watchInputEvidenceMatchesBaseline`: the reference-graph closure, globals,
 *   the config chain, resolution candidates (absent ones included) and plugin
 *   host inputs. This is the generation evidence `@ttsc/metro` keys its
 *   persistent cache on. Proving one costs about half a millisecond, and a
 *   generation has thousands, mostly absent resolution candidates; so each
 *   input also carries the metadata it had when it was proven
 *   ({@link observeSignature}), and an input whose metadata is unchanged needs
 *   no second proof.
 * - The module's own source bytes.
 *
 * Within one process a validated generation is trusted for each module's first
 * delivery, the contract ttsc's own Bun pass (`beginTtscTransformBuild`) keeps.
 *
 * Non-fatal plugin warnings and ttsc's "file is not in its program" notice are
 * printed when a generation is compiled; serving a stored module does not
 * replay them.
 */

import { createHash, type Hash, randomUUID } from 'node:crypto';
import {
  type BigIntStats,
  mkdirSync,
  readdirSync,
  readFileSync,
  readlinkSync,
  realpathSync,
  renameSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { createRequire } from 'node:module';
import { basename, dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  captureWatchInputBaseline,
  collectProjectInputHashSnapshot,
  mergeMembershipPolicyOverlay,
  readProjectMembershipPolicy,
  type TtscTransformHooks,
  type TtscWatchInputEvidence,
  type TtscWatchInputState,
  watchInputEvidenceMatchesBaseline,
} from '@ttsc/unplugin/api';
import { isRecord } from '../guards.js';

/** Bumped whenever the stored shape, or what a stored generation proves, changes. */
const STORE_FORMAT = 1;

/** One delivered module of a stored generation. */
export interface StoredModule {
  readonly sourceSha256: string;
  /** `null` when ttsc returned no change for the module: its source is served as is. */
  readonly code: string | null;
}

/** One watch input: ttsc's generation evidence, and metadata that may stand in for re-proving it. */
export interface StoredInput {
  readonly evidence: TtscWatchInputEvidence;
  /**
   * The input's metadata when the store proved it against `evidence`, kept only
   * when its last change was settled by then (see {@link observeSignature}), so
   * any later write moves it. Absent: prove the input again on every read.
   */
  readonly signature?: string;
}

/** A project generation as stored, and as served once validated. */
export interface StoredGeneration {
  readonly format: typeof STORE_FORMAT;
  readonly project: string;
  readonly transformer: string;
  /** Project-relative path to content hash, as `collectProjectInputHashSnapshot` reports it. */
  readonly walk: Readonly<Record<string, string>>;
  /** Realpaths of the plugin packages whose native sources the transformer identity covers. */
  readonly plugins: readonly string[];
  readonly inputs: Readonly<Record<string, StoredInput>>;
  readonly modules: Readonly<Record<string, StoredModule>>;
}

/** What one process observed while compiling a project, before it is stored. */
export interface Recording {
  readonly project: string;
  /** The project walk taken before this process first compiled the project. */
  readonly walk: Readonly<Record<string, string>>;
  readonly inputs: Map<string, TtscWatchInputEvidence>;
  readonly modules: Map<string, StoredModule>;
  /** Why the recording cannot prove a generation, once it cannot. */
  unprovable: string | undefined;
}

/** Outcome of {@link storeGeneration}. */
export type StoreOutcome =
  | { readonly kind: 'stored'; readonly generation: StoredGeneration }
  | { readonly kind: 'unproven'; readonly reason: string };

/** Compiler-options overlay the transform applies, which also shapes the walk. */
export type CompilerOptionsOverlay = Readonly<Record<string, unknown>>;

export function sha256(data: string | Uint8Array): string {
  return createHash('sha256').update(data).digest('hex');
}

/** Hash every walk-visible project input, or `undefined` when the walk was not coherent. */
export function projectWalk(
  project: string,
  compilerOptions: CompilerOptionsOverlay,
): Readonly<Record<string, string>> | undefined {
  const root = dirname(project);
  const policy = mergeMembershipPolicyOverlay(readProjectMembershipPolicy(project), { ...compilerOptions }, root);
  const snapshot = collectProjectInputHashSnapshot(root, undefined, undefined, policy);
  return snapshot.complete ? snapshot.hashes : undefined;
}

export function createRecording(project: string, walk: Readonly<Record<string, string>>): Recording {
  return { project, walk, inputs: new Map(), modules: new Map(), unprovable: undefined };
}

/** Hooks that record every watch input ttsc derives for a delivered module. */
export function recordingHooks(recording: Recording): TtscTransformHooks {
  return {
    addWatchFile(file, evidence) {
      if (evidence?.state === undefined) {
        recording.unprovable ??= `ttsc reported no generation state for ${file}`;
        return;
      }
      const previous = recording.inputs.get(file);
      if (previous === undefined) {
        recording.inputs.set(file, evidence);
      } else if (JSON.stringify(previous) !== JSON.stringify(evidence)) {
        recording.unprovable ??= `two generations disagree about ${file}`;
      }
    },
    markVolatile() {
      recording.unprovable ??= 'a plugin declared an output volatile';
    },
  };
}

export function recordModule(recording: Recording, file: string, source: string, code: string | undefined): void {
  recording.modules.set(file, { sourceSha256: sha256(source), code: code ?? null });
}

/** The file one project's generation is stored in. */
export function generationPath(store: string, project: string): string {
  return join(store, `${sha256(project).slice(0, 32)}.json`);
}

/**
 * Load the stored generation for `project` and prove it current, or return
 * `undefined` when there is none or any part of its proof fails.
 */
export function loadGeneration(
  store: string,
  project: string,
  compilerOptions: CompilerOptionsOverlay,
): StoredGeneration | undefined {
  const text = readIfPresent(generationPath(store, project));
  if (text === undefined) return undefined;
  const generation = decodeGeneration(JSON.parse(text));
  if (generation === undefined || generation.project !== project) return undefined;
  if (generation.transformer !== transformerIdentity(generation.plugins)) return undefined;
  const walk = projectWalk(project, compilerOptions);
  if (walk === undefined || !sameHashes(walk, generation.walk)) return undefined;
  for (const [file, input] of Object.entries(generation.inputs)) {
    if (input.signature !== undefined && observeSignature(file).signature === input.signature) continue;
    const baseline = captureWatchInputBaseline(file);
    if (baseline === undefined || !watchInputEvidenceMatchesBaseline(input.evidence, baseline)) return undefined;
  }
  return generation;
}

/**
 * Store what `recording` proved, merged with `base` (a generation this process
 * already validated) when both describe the same inputs.
 */
export function storeGeneration(
  store: string,
  recording: Recording,
  base: StoredGeneration | undefined,
  compilerOptions: CompilerOptionsOverlay,
): StoreOutcome {
  if (recording.unprovable !== undefined) return { kind: 'unproven', reason: recording.unprovable };
  const walk = projectWalk(recording.project, compilerOptions);
  if (walk === undefined) return { kind: 'unproven', reason: 'the project walk was not coherent' };
  if (!sameHashes(walk, recording.walk))
    return { kind: 'unproven', reason: 'project files changed during the compile' };

  const inputs = new Map<string, TtscWatchInputEvidence>();
  let modules: Record<string, StoredModule> = {};
  if (base !== undefined && sameHashes(base.walk, walk)) {
    for (const [file, input] of Object.entries(base.inputs)) inputs.set(file, input.evidence);
    modules = { ...base.modules };
  }
  for (const [file, evidence] of recording.inputs) {
    const previous = inputs.get(file);
    if (previous !== undefined && JSON.stringify(previous) !== JSON.stringify(evidence)) {
      // An input moved since `base` was proven, so nothing of it is current.
      inputs.clear();
      for (const [own, ownEvidence] of recording.inputs) inputs.set(own, ownEvidence);
      modules = {};
      break;
    }
    inputs.set(file, evidence);
  }
  for (const [file, module] of recording.modules) modules[file] = module;

  // Prove every input against the bytes on disk now, and sign the ones whose
  // last change was settled before the proof read them.
  const settled = (BigInt(Date.now()) - SETTLED_MARGIN_MS) * 1_000_000n;
  const proven: Record<string, StoredInput> = {};
  for (const [file, evidence] of inputs) {
    const before = observeSignature(file);
    const baseline = captureWatchInputBaseline(file);
    if (baseline === undefined || !watchInputEvidenceMatchesBaseline(evidence, baseline)) {
      return { kind: 'unproven', reason: `${file} changed after the compile read it` };
    }
    const signed = before.changedAt < settled && observeSignature(file).signature === before.signature;
    proven[file] = signed ? { evidence, signature: before.signature } : { evidence };
  }

  const plugins = pluginPackages(inputs);
  const generation: StoredGeneration = {
    format: STORE_FORMAT,
    project: recording.project,
    transformer: transformerIdentity(plugins),
    walk,
    plugins,
    inputs: proven,
    modules,
  };
  const path = generationPath(store, recording.project);
  mkdirSync(store, { recursive: true });
  const temporary = `${path}.${process.pid}.${randomUUID()}.tmp`;
  writeFileSync(temporary, JSON.stringify(generation));
  renameSync(temporary, path);
  return { kind: 'stored', generation };
}

function readIfPresent(path: string): string | undefined {
  try {
    return readFileSync(path, 'utf8');
  } catch (error) {
    if (errorCode(error) === 'ENOENT') return undefined;
    throw error;
  }
}

function errorCode(error: unknown): unknown {
  return typeof error === 'object' && error !== null && 'code' in error ? error.code : undefined;
}

/**
 * How long before a proof an input's last change must lie for its metadata to
 * stand in for its bytes. A write is stamped from the filesystem's own clock,
 * rounded down to its granularity: a nanosecond on APFS, a scheduler tick on
 * ext4, two seconds on FAT. Once that tick has passed, any later write stamps a
 * strictly later time, so equal metadata means unwritten bytes.
 */
const SETTLED_MARGIN_MS = 2_000n;
const ABSENT = { signature: 'absent', changedAt: 0n } as const;

/**
 * The metadata that moves whenever a path's bytes, kind or target move:
 * identity, mode, size, both clocks, and the resolved path, which a retargeted
 * link changes. `changedAt` is the later clock, since `utimes` can set mtime
 * but nothing but a write sets ctime.
 */
function observeSignature(file: string): { readonly signature: string; readonly changedAt: bigint } {
  let stats: BigIntStats;
  try {
    stats = statSync(file, { bigint: true });
  } catch (error) {
    const code = errorCode(error);
    if (code === 'ENOENT' || code === 'ENOTDIR') return ABSENT;
    throw error;
  }
  return {
    signature: [
      stats.dev,
      stats.ino,
      stats.mode,
      stats.size,
      stats.mtimeNs,
      stats.ctimeNs,
      realpathSync.native(file),
    ].join(':'),
    changedAt: stats.ctimeNs > stats.mtimeNs ? stats.ctimeNs : stats.mtimeNs,
  };
}

function sameHashes(left: Readonly<Record<string, string>>, right: Readonly<Record<string, string>>): boolean {
  const keys = Object.keys(left);
  return keys.length === Object.keys(right).length && keys.every((key) => left[key] === right[key]);
}

// ---------------------------------------------------------------------------
// Transformer identity
// ---------------------------------------------------------------------------

const requireHere = createRequire(import.meta.url);
const SOURCE_MODULE = /\.[cm]?[jt]s$/;
const NATIVE_SOURCE = /\.(?:go|mod|sum|work)$/;
const TRANSFORMER_ENV = /^(?:TTSC_|GO|CGO_)/;
let transformerBase: string | undefined;
const transformerByPlugins = new Map<string, string>();

/**
 * Everything besides the project that decides what a transform emits. The base
 * is computed once per process; each plugin set adds the content of its
 * packages' native sources, because a workspace plugin (lmao-ttsc) changes them
 * without changing any manifest.
 */
export function transformerIdentity(plugins: readonly string[]): string {
  const key = plugins.join('\0');
  const existing = transformerByPlugins.get(key);
  if (existing !== undefined) return existing;
  transformerBase ??= computeTransformerBase();
  const hash = createHash('sha256').update(transformerBase);
  for (const plugin of plugins) {
    hash.update(`plugin ${plugin}\0`);
    hashTree(hash, plugin, (name) => name === 'package.json' || NATIVE_SOURCE.test(name));
  }
  const identity = hash.digest('hex');
  transformerByPlugins.set(key, identity);
  return identity;
}

function computeTransformerBase(): string {
  const hash = createHash('sha256');
  hash.update(`format ${STORE_FORMAT}\0platform ${process.platform}/${process.arch}\0`);
  hashTree(hash, dirname(fileURLToPath(import.meta.url)), (name) => SOURCE_MODULE.test(name));
  const unplugin = packageRoot(requireHere, '@ttsc/unplugin');
  const ttsc = unplugin === undefined ? undefined : packageRoot(createRequire(join(unplugin, 'package.json')), 'ttsc');
  for (const [name, root] of [
    ['@ttsc/unplugin', unplugin],
    ['ttsc', ttsc],
  ] as const) {
    hash.update(`package ${name} ${root ?? 'absent'}\0`);
    if (root !== undefined) hashTree(hash, root, (file) => !file.endsWith('.map'));
  }
  // The platform package carries the native host and its Go toolchain, far too
  // large to read per process. Bun installs it at a content-hashed path.
  const platform =
    ttsc === undefined
      ? undefined
      : packageRoot(createRequire(join(ttsc, 'package.json')), `@ttsc/${process.platform}-${process.arch}`);
  hash.update(`platform-package ${platform ?? 'absent'}\0`);
  if (platform !== undefined) hash.update(readFileSync(join(platform, 'package.json')));
  const tsgo = process.env.TTSC_TSGO_BINARY;
  hash.update(`tsgo ${tsgo === undefined ? 'unset' : realpathSync(tsgo)}\0`);
  for (const name of Object.keys(process.env)
    .filter((variable) => TRANSFORMER_ENV.test(variable))
    .sort()) {
    hash.update(`env ${name}=${process.env[name]}\0`);
  }
  return hash.digest('hex');
}

function packageRoot(require: ReturnType<typeof createRequire>, name: string): string | undefined {
  try {
    return dirname(realpathSync(require.resolve(`${name}/package.json`)));
  } catch (error) {
    if (errorCode(error) === 'MODULE_NOT_FOUND') return undefined;
    throw error;
  }
}

/** The plugin packages among a generation's inputs: manifests declaring `ttsc.plugin`. */
function pluginPackages(inputs: ReadonlyMap<string, TtscWatchInputEvidence>): string[] {
  const packages = new Set<string>();
  for (const [file, evidence] of inputs) {
    if (evidence.missing || basename(file) !== 'package.json') continue;
    const manifest: unknown = JSON.parse(readFileSync(file, 'utf8'));
    if (isRecord(manifest) && isRecord(manifest.ttsc) && manifest.ttsc.plugin !== undefined) {
      packages.add(realpathSync(dirname(file)));
    }
  }
  return [...packages].sort();
}

/** Hash a tree's accepted files and every link, in a stable order, skipping `node_modules`. */
function hashTree(hash: Hash, root: string, accept: (name: string) => boolean): void {
  const walk = (directory: string, relative: string): void => {
    const entries = readdirSync(directory, { withFileTypes: true }).sort((left, right) =>
      left.name < right.name ? -1 : left.name > right.name ? 1 : 0,
    );
    for (const entry of entries) {
      const path = join(directory, entry.name);
      const name = relative === '' ? entry.name : `${relative}/${entry.name}`;
      if (entry.isDirectory()) {
        if (entry.name !== 'node_modules') walk(path, name);
      } else if (entry.isSymbolicLink()) {
        hash.update(`link ${name}\0${readlinkSync(path)}\0`);
      } else if (entry.isFile() && accept(entry.name)) {
        hash.update(`file ${name}\0`);
        hash.update(readFileSync(path));
        hash.update('\0');
      }
    }
  };
  walk(root, '');
}

// ---------------------------------------------------------------------------
// Decoding a stored generation
//
// By hand, not with typia: these modules load before the preload registers the
// transform, so a typia callsite here would never be transformed.
// ---------------------------------------------------------------------------

type Observation = Extract<TtscWatchInputState, { codec: 'predicates' }>['observation'];

function isStringArray(value: unknown): value is readonly string[] {
  return Array.isArray(value) && value.every((item) => typeof item === 'string');
}

function oneOf<const T extends string>(value: unknown, options: readonly T[]): T | undefined {
  return options.find((option) => option === value);
}

function decodeRecord<T>(value: unknown, decode: (item: unknown) => T | undefined): Record<string, T> | undefined {
  if (!isRecord(value)) return undefined;
  const decoded: Record<string, T> = {};
  for (const [key, item] of Object.entries(value)) {
    const entry = decode(item);
    if (entry === undefined) return undefined;
    decoded[key] = entry;
  }
  return decoded;
}

function decodeGeneration(value: unknown): StoredGeneration | undefined {
  if (!isRecord(value) || value.format !== STORE_FORMAT) return undefined;
  const { project, transformer, plugins } = value;
  if (typeof project !== 'string' || typeof transformer !== 'string' || !isStringArray(plugins)) return undefined;
  const walk = decodeRecord(value.walk, (hash) => (typeof hash === 'string' ? hash : undefined));
  const inputs = decodeRecord(value.inputs, decodeInput);
  const modules = decodeRecord(value.modules, decodeModule);
  if (walk === undefined || inputs === undefined || modules === undefined) return undefined;
  return { format: STORE_FORMAT, project, transformer, walk, plugins, inputs, modules };
}

function decodeModule(value: unknown): StoredModule | undefined {
  if (!isRecord(value) || typeof value.sourceSha256 !== 'string') return undefined;
  const code = value.code;
  if (code !== null && typeof code !== 'string') return undefined;
  return { sourceSha256: value.sourceSha256, code };
}

function decodeInput(value: unknown): StoredInput | undefined {
  if (!isRecord(value)) return undefined;
  const evidence = decodeEvidence(value.evidence);
  const signature = value.signature;
  if (evidence === undefined || (signature !== undefined && typeof signature !== 'string')) return undefined;
  return signature === undefined ? { evidence } : { evidence, signature };
}

function decodeEvidence(value: unknown): TtscWatchInputEvidence | undefined {
  if (!isRecord(value) || typeof value.identity !== 'string' || typeof value.missing !== 'boolean') return undefined;
  const state = decodeState(value.state);
  const unavailable = oneOf(value.unavailable, ['missing', 'not-file']);
  if (state === undefined || (value.unavailable !== undefined && unavailable === undefined)) return undefined;
  return unavailable === undefined
    ? { identity: value.identity, missing: value.missing, state }
    : { identity: value.identity, missing: value.missing, state, unavailable };
}

function decodeState(value: unknown): TtscWatchInputState | undefined {
  if (!isRecord(value)) return undefined;
  switch (value.codec) {
    case 'host':
      return typeof value.hash === 'string' ? { codec: 'host', hash: value.hash } : undefined;
    case 'graph': {
      const realpath = value.realpath === null ? null : typeof value.realpath === 'string' ? value.realpath : undefined;
      return typeof value.hash === 'string' && realpath !== undefined
        ? { codec: 'graph', hash: value.hash, realpath }
        : undefined;
    }
    case 'predicates': {
      const observation = decodeObservation(value.observation);
      return observation === undefined ? undefined : { codec: 'predicates', observation };
    }
    default:
      return undefined;
  }
}

const OBSERVATION_FIELDS: Readonly<Record<string, true>> = {
  accessibleEntries: true,
  directoryExists: true,
  fileExists: true,
  readFile: true,
  realpath: true,
  stat: true,
};

/** Every field optional, as the compiler records them; an unknown field refuses the generation. */
function decodeObservation(value: unknown): Observation | undefined {
  if (!isRecord(value) || Object.keys(value).some((key) => !Object.hasOwn(OBSERVATION_FIELDS, key))) return undefined;
  const observation: Observation = {};
  const { accessibleEntries, directoryExists, fileExists, readFile, realpath, stat } = value;
  if (accessibleEntries !== undefined) {
    if (
      !isRecord(accessibleEntries) ||
      !isStringArray(accessibleEntries.directories) ||
      !isStringArray(accessibleEntries.files)
    ) {
      return undefined;
    }
    observation.accessibleEntries = {
      directories: [...accessibleEntries.directories],
      files: [...accessibleEntries.files],
    };
  }
  if (directoryExists !== undefined) {
    if (typeof directoryExists !== 'boolean') return undefined;
    observation.directoryExists = directoryExists;
  }
  if (fileExists !== undefined) {
    if (typeof fileExists !== 'boolean') return undefined;
    observation.fileExists = fileExists;
  }
  if (readFile !== undefined) {
    if (!isRecord(readFile)) return undefined;
    if (readFile.ok === false) observation.readFile = { ok: false };
    else if (readFile.ok === true && typeof readFile.hash === 'string')
      observation.readFile = { ok: true, hash: readFile.hash };
    else return undefined;
  }
  if (realpath !== undefined) {
    if (!isRecord(realpath)) return undefined;
    if (realpath.ok === false) observation.realpath = { ok: false };
    else if (realpath.ok === true && typeof realpath.path === 'string')
      observation.realpath = { ok: true, path: realpath.path };
    else return undefined;
  }
  if (stat !== undefined) {
    const kind = oneOf(stat, ['directory', 'file', 'missing']);
    if (kind === undefined) return undefined;
    observation.stat = kind;
  }
  return observation;
}
