import { createHash } from 'node:crypto';
import { realpath } from 'node:fs/promises';
import { isAbsolute, join, posix, relative, sep } from 'node:path';

import { CARGO_ANCESTOR_INPUTS } from '@smoothbricks/cargo-retain/cargo-ancestor-inputs';
import type { ProjectGraphExternalNode, TargetConfiguration } from 'nx/src/devkit-exports.js';

import {
  type CargoResolve,
  type CompiledCargoInput,
  compiledInFiles,
  dependencyClosure,
  HASH_SKIPPED_DIRECTORIES,
  readCargoResolve,
} from './cargo-source-hash.js';
import { runtimeInput, shellWord } from './runtime-input.js';
import { globIndexedFiles } from './workspace-index.js';

/**
 * The named input the plugin infers for every project with crates of a Cargo
 * workspace under its root: the mutable sources of the Cargo-resolved
 * dependency closure of those crates, and the lock-scope node that stands for
 * the Cargo.lock entries the closure reaches ({@link cargoLockNode}).
 */
export const CARGO_CLOSURE_INPUT = 'cargoClosure';

/** Every lock-scope node's name starts with this; the rest is the directory whose crates it covers. */
export const CARGO_LOCK_NODE_PREFIX = 'cargo-lock:';

/** The lock-scope node for the crates under `directory`, workspace-relative (`.` for the root). */
export function cargoLockNodeName(directory: string): string {
  return `${CARGO_LOCK_NODE_PREFIX}${posix.normalize(directory === '' ? '.' : directory)}`;
}

/** An input naming only lock-scope nodes: it keys a target on locked crates, never on npm packages. */
export function isCargoLockInput(input: unknown): boolean {
  if (typeof input !== 'object' || input === null || !('externalDependencies' in input)) return false;
  const names: unknown = input.externalDependencies;
  return (
    Array.isArray(names) &&
    names.length > 0 &&
    names.every((name) => typeof name === 'string' && name.startsWith(CARGO_LOCK_NODE_PREFIX))
  );
}

type Input = NonNullable<TargetConfiguration['inputs']>[number];

/** A closure Nx cannot express exactly is a refusal, never a broader cache key. */
export class CargoClosureInputError extends Error {
  readonly projectRoot: string;

  constructor(projectRoot: string, cause: unknown) {
    super(
      `Cannot infer ${CARGO_CLOSURE_INPUT} for ${projectRoot}: ${cause instanceof Error ? cause.message : String(cause)}`,
      {
        cause,
      },
    );
    this.name = 'CargoClosureInputError';
    this.projectRoot = projectRoot;
  }
}

/**
 * How a runtime input reaches the hash command. Nx runs it with `sh -c` from
 * the workspace root and puts no `node_modules/.bin` on its PATH; a plugin Nx
 * loads by name is installed at exactly this path.
 */
const CARGO_HASH_COMMAND = 'node node_modules/@smoothbricks/nx-plugin/dist/bin/smoo-nx-cargo-hash.js';

/** A package's own Rust sources and Cargo configuration, exactly what the hash command's walk selects. */
const PACKAGE_FILES = '**/{*.rs,Cargo.toml,.cargo/config,.cargo/config.toml}';
const ANCESTOR_FILES = `{${CARGO_ANCESTOR_INPUTS.join(',')}}`;
const SKIPPED_TREES = `**/{${HASH_SKIPPED_DIRECTORIES.join(',')}}/**`;
/** A path holding any of these would be read as a pattern, not as itself. */
const GLOB_SYNTAX = /[*?[\]{}()!,]/;

/**
 * One Cargo workspace as one project-graph computation sees it. `manifest` is
 * the workspace's root manifest relative to the Nx workspace root.
 */
export interface CargoClosureSource {
  readonly manifest: string;
  /** The Nx workspace root as the computation names it, which Nx's file index is keyed by. */
  readonly workspaceRoot: string;
  /** The canonical Nx workspace root. */
  readonly root: string;
  readonly cargo: CargoResolve;
  /** Every `Cargo.toml` in Nx's file index, relative to the workspace root. */
  readonly indexed: ReadonlySet<string>;
  readonly compiledFilesCache: Map<string, CompiledCargoInput[]>;
}

/**
 * Every `Cargo.toml` Nx's workspace file index holds: .gitignore and .nxignore
 * applied, `node_modules` never entered. A package whose manifest is missing
 * here has no files a fileset input can hash.
 */
export async function indexedCargoManifests(workspaceRoot: string): Promise<ReadonlySet<string>> {
  return new Set(await globIndexedFiles(workspaceRoot, ['**/Cargo.toml']));
}

/**
 * Content-keyed locked offline metadata. A refusal fails graph inference:
 * a stale closure or runtime whole-workspace fallback cannot establish the
 * precise set of cache inputs.
 */
export async function resolveCargoClosureSource(
  manifest: string,
  workspaceRoot: string,
  indexed: Promise<ReadonlySet<string>>,
  compiledFilesCache: Map<string, CompiledCargoInput[]>,
): Promise<CargoClosureSource> {
  const [root, index] = await Promise.all([realpath(workspaceRoot), indexed]);
  const cargo = await readCargoResolve(join(root, manifest), root, index);
  return { manifest, workspaceRoot, root, cargo, indexed: index, compiledFilesCache };
}

/**
 * The `cargoClosure` definition for the project at `projectRoot`: for each
 * Cargo workspace with crates under it, the resolve closure of those crates,
 * and the lock-scope node of `projectRoot` for the locked rest.
 *
 * Closure members in Nx's file index are hashed by Nx itself, as filesets:
 * each package's Rust sources, manifests and Cargo configuration below its
 * directory, target sources outside it, its governing manifest, and the
 * manifests and Cargo configuration of every directory from the package up
 * to the workspace root. Nx keeps those file hashes current, so an unchanged
 * tree costs no process at all. Members Nx cannot see — outside the
 * workspace, or installed under `node_modules` — are hashed by one runtime
 * `smoo-nx-cargo-hash --closure` entry, present only while such members
 * exist. That command resolves the closure again when it runs, so a member
 * that only an edit outside the workspace brings in is still covered.
 *
 * An in-workspace member missing from Nx's file index is a typed refusal,
 * never a whole-workspace runtime hash. Only outside-workspace members need
 * the external-only runtime input.
 *
 * A compiled-in file is named only when Nx's file index holds both it and the
 * source that includes it. An ignored one is a build output: no fileset can
 * hash its bytes, which reach a consumer through its producer's dependency
 * outputs instead, so naming it would add nothing to any task's key except
 * the names of whatever output last ran. That state is exactly what a project
 * graph must not read (`globIndexedFiles`): a producer that regenerates
 * content-named files would otherwise move every key of the graph it was
 * hashed under, and the run that compiled a tree would leave its verdict
 * under a key no later run of the same tree computes.
 */
export async function cargoClosureInputs(
  projectRoot: string,
  sources: readonly CargoClosureSource[],
): Promise<Input[]> {
  const inputs: Input[] = [];
  const seen = new Set<string>();
  const ordered = [...sources].sort((left, right) =>
    left.manifest < right.manifest ? -1 : left.manifest > right.manifest ? 1 : 0,
  );
  for (const source of ordered) {
    for (const input of await workspaceClosureInputs(projectRoot, source)) {
      const key = typeof input === 'string' ? input : JSON.stringify(input);
      if (seen.has(key)) continue;
      seen.add(key);
      inputs.push(input);
    }
  }
  inputs.push({ externalDependencies: [cargoLockNodeName(projectRoot)] });
  return inputs;
}

async function workspaceClosureInputs(projectRoot: string, source: CargoClosureSource): Promise<Input[]> {
  const closureArguments = `--closure ${shellWord(projectRoot)} ${shellWord(source.manifest)}`;

  const { root, cargo, indexed } = source;
  const packageDirectories = new Set<string>();
  const files = new Set<string>();
  const compiledIn: CompiledInclusion[] = [];
  let compiledFiles: readonly string[] = [];
  let external = false;
  try {
    const members = await dependencyClosure(cargo, join(root, projectRoot));
    for (const pkg of cargo.local) {
      if (!members.has(pkg.id)) continue;
      const directory = indexablePath(root, pkg.directory);
      if (directory === null) {
        external = true;
        continue;
      }
      if (!indexed.has(posix.join(directory, 'Cargo.toml'))) {
        throw new Error(`the Cargo package ${directory} is not in Nx's file index (an ignored directory)`);
      }
      for (const compiled of compiledInFiles(pkg.directory, source.compiledFilesCache)) {
        const file = indexablePath(root, compiled.file);
        if (file === null) throw new Error(`${directory} compiles ${compiled.file}, which Nx's file index cannot hold`);
        // The scan starts inside this package's indexable directory, so its sources can always be named.
        const declaring = indexablePath(root, compiled.source);
        if (declaring === null) throw new Error(`${compiled.source} was scanned outside ${directory}`);
        compiledIn.push({ declaring, file });
      }
      packageDirectories.add(directory);
      for (const file of [...pkg.sources, pkg.governingManifest]) {
        const path = indexablePath(root, file);
        if (path === null) throw new Error(`${directory} compiles ${file}, which Nx's file index cannot hold`);
        files.add(path);
      }
    }
    const compiledPaths = [...new Set(compiledIn.flatMap(({ declaring, file }) => [declaring, file]))];
    const unglobbable = [...packageDirectories, ...files, ...compiledPaths].find((path) => GLOB_SYNTAX.test(path));
    if (unglobbable !== undefined) throw new Error(`${unglobbable} contains glob syntax, so no fileset can name it`);
    // Literal paths, so each matches exactly itself in the index.
    const held = new Set(compiledPaths.length === 0 ? [] : await globIndexedFiles(source.workspaceRoot, compiledPaths));
    compiledFiles = indexedCompiledFiles(compiledIn, held);
  } catch (error) {
    throw new CargoClosureInputError(projectRoot, error);
  }

  // Query the directory set by path ancestors, not by expanding all pairs.
  // Removing nested members leaves an antichain with the same covered files.
  const directories = [...packageDirectories]
    .sort()
    .filter((directory) => !covered(packageDirectories, parent(directory)));
  const ancestors = new Set<string>();
  for (const directory of directories) {
    // An ancestor of an outermost member cannot be inside another member.
    // Once this walk joins an earlier walk, all remaining ancestors are held.
    for (let ancestor = parent(directory); ancestor !== null && !ancestors.has(ancestor); ancestor = parent(ancestor)) {
      ancestors.add(ancestor);
    }
  }
  // Target sources outside every package directory, and governing manifests
  // that are not already one of the ancestors' manifests.
  const loose = new Set(
    [...files].filter(
      (file) =>
        !covered(packageDirectories, file) &&
        !(posix.basename(file) === 'Cargo.toml' && ancestors.has(parent(file) ?? '')),
    ),
  );
  // Sibling packages share one pattern, `crates/{a,b}/…`, so a closure of
  // dozens of crates reads as a few lines in `nx show project`.
  const siblings = new Map<string, string[]>();
  for (const directory of directories) {
    const key = parent(directory);
    if (key === null) continue;
    const names = siblings.get(key);
    if (names === undefined) siblings.set(key, [posix.basename(directory)]);
    else names.push(posix.basename(directory));
  }
  const packageGlobs = directories.includes('')
    ? ['']
    : [...siblings].map(([under, names]) =>
        posix.join(under, names.length === 1 ? names.join('') : `{${names.join(',')}}`),
      );
  const anchored = (path: string, pattern = '') => posix.join('{workspaceRoot}', path, pattern);
  return [
    ...packageGlobs.map((glob) => anchored(glob, PACKAGE_FILES)),
    ...packageGlobs.map((glob) => `!${anchored(glob, SKIPPED_TREES)}`),
    ...[...ancestors].sort().map((ancestor) => anchored(ancestor, ANCESTOR_FILES)),
    ...compiledFiles.map((file) => anchored(file)),
    ...[...loose].sort().map((file) => anchored(file)),
    ...(external ? [runtimeInput(`${CARGO_HASH_COMMAND} ${closureArguments}`)] : []),
  ];
}

/**
 * The external project-graph node that stands for the part of Cargo.lock the
 * crates under `directory` reach, over every source with a crate there.
 *
 * Its hash digests, per Cargo workspace in manifest order, every package of
 * the resolve closure of those crates: a locked package by its Cargo package
 * id and source (registry, name and version; a git dependency's commit), a
 * path package by name, each with the packages its built dependencies resolved to
 * (a dependency's own dev-dependencies are not built for these crates; see `buildClosure`).
 * That is everything Cargo.lock decides for the closure. An edit to an entry
 * no crate under `directory` reaches leaves the digest alone, and Nx hashes
 * the node only into the targets that name it, so a lockfile change re-keys
 * the users of what changed and nothing else. A registry package's checksum is
 * not in Cargo's metadata and adds nothing: a published version's bytes never
 * change, and every `--locked` build verifies them against Cargo.lock.
 *
 * Path packages are named, never identified by their ids: an id carries the
 * checkout's absolute path, and a task key must not.
 */
export async function cargoLockNode(
  directory: string,
  sources: readonly CargoClosureSource[],
): Promise<ProjectGraphExternalNode> {
  const name = cargoLockNodeName(directory);
  if (sources.length === 0) {
    throw new CargoClosureInputError(directory, new Error('no Cargo workspace has a crate under it'));
  }
  const hash = createHash('sha256');
  try {
    for (const source of [...sources].sort((left, right) =>
      left.manifest < right.manifest ? -1 : left.manifest > right.manifest ? 1 : 0,
    )) {
      const { cargo, root } = source;
      const closure = await dependencyClosure(cargo, join(root, directory));
      const paths = new Map(cargo.local.map((pkg) => [pkg.id, `path:${pkg.name}`]));
      const label = (id: string) => paths.get(id) ?? id;
      const rows = [...closure]
        .map(([id, built]) => [label(id), cargo.locked.get(id) ?? '', ...built.map(label).sort()].join('\t'))
        .sort();
      hash.update(`${source.manifest}\n${rows.join('\n')}\n\0`);
    }
  } catch (error) {
    throw new CargoClosureInputError(directory, error);
  }
  const digest = hash.digest('hex');
  return { type: 'cargo', name, data: { packageName: name, version: digest, hash: digest } };
}
/** A compiled-in file and the source whose `include_str!`/`include_bytes!` names it, relative to the workspace root. */
export interface CompiledInclusion {
  readonly declaring: string;
  readonly file: string;
}

/**
 * The compiled-in files a `cargoClosure` names, sorted: each one Nx's file index holds together with the source that
 * includes it. `held` is the index's answer for exactly those paths. A file it does not hold is a build output or
 * ignored, and naming it would key the graph on whatever last wrote it rather than on the tree.
 */
export function indexedCompiledFiles(compiledIn: readonly CompiledInclusion[], held: ReadonlySet<string>): string[] {
  const named = new Set<string>();
  for (const { declaring, file } of compiledIn) {
    if (held.has(declaring) && held.has(file)) named.add(file);
  }
  return [...named].sort();
}

/**
 * `path` relative to the workspace root, `/`-separated, when it is somewhere
 * Nx's file index can hold: inside the workspace and not under `node_modules`.
 * The root itself is `''`.
 */
function indexablePath(root: string, path: string): string | null {
  const fromRoot = relative(root, path);
  if (fromRoot === '..' || fromRoot.startsWith(`..${sep}`) || isAbsolute(fromRoot)) return null;
  const segments = fromRoot === '' ? [] : fromRoot.split(sep);
  return segments.includes('node_modules') ? null : segments.join('/');
}

/** The directory holding `path`, `''` being the workspace root, which has none. */
function parent(path: string): string | null {
  if (path === '') return null;
  const directory = posix.dirname(path);
  return directory === '.' ? '' : directory;
}

/** Membership in the union of package subtrees, bounded by path depth rather than package count. */
function covered(directories: ReadonlySet<string>, path: string | null): boolean {
  for (let candidate = path; candidate !== null; candidate = parent(candidate)) {
    if (directories.has(candidate)) return true;
  }
  return false;
}
