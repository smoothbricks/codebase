import { realpath } from 'node:fs/promises';
import { isAbsolute, join, posix, relative, sep } from 'node:path';

import type { TargetConfiguration } from 'nx/src/devkit-exports.js';
import { globWithWorkspaceContext, globWithWorkspaceContextSync } from 'nx/src/utils/workspace-context.js';
import { workspaceRoot as daemonWorkspaceRoot } from 'nx/src/utils/workspace-root.js';

import {
  CARGO_ANCESTOR_INPUTS,
  type CargoResolve,
  dependencyClosure,
  HASH_SKIPPED_DIRECTORIES,
  readCargoResolve,
} from './cargo-source-hash.js';

/**
 * The named input the plugin infers for every project with crates of a Cargo
 * workspace under its root: the mutable sources of the Cargo-resolved
 * dependency closure of those crates, and the lockfile that pins the rest.
 */
export const CARGO_CLOSURE_INPUT = 'cargoClosure';

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
  /** The canonical Nx workspace root. */
  readonly root: string;
  readonly cargo: CargoResolve;
  /** Every `Cargo.toml` in Nx's file index, relative to the workspace root. */
  readonly indexed: ReadonlySet<string>;
}

/**
 * Every `Cargo.toml` Nx's workspace file index holds: .gitignore and .nxignore
 * applied, `node_modules` never entered. A package whose manifest is missing
 * here has no files a fileset input can hash.
 */
export async function indexedCargoManifests(workspaceRoot: string): Promise<ReadonlySet<string>> {
  const manifests =
    workspaceRoot === daemonWorkspaceRoot
      ? await globWithWorkspaceContext(workspaceRoot, ['**/Cargo.toml'])
      : globWithWorkspaceContextSync(workspaceRoot, ['**/Cargo.toml']);
  return new Set(manifests);
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
): Promise<CargoClosureSource> {
  const [root, index] = await Promise.all([realpath(workspaceRoot), indexed]);
  const cargo = await readCargoResolve(join(root, manifest), root, index);
  return { manifest, root, cargo, indexed: index };
}

/**
 * The `cargoClosure` definition for the project at `projectRoot`: for each
 * Cargo workspace with crates under it, the workspace's `Cargo.lock` plus the
 * resolve closure of those crates.
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
  return inputs;
}

async function workspaceClosureInputs(projectRoot: string, source: CargoClosureSource): Promise<Input[]> {
  const lock = posix.join('{workspaceRoot}', posix.dirname(source.manifest), 'Cargo.lock');
  const closureArguments = `--closure ${shellWord(projectRoot)} ${shellWord(source.manifest)}`;

  const { root, cargo, indexed } = source;
  const packageDirectories: string[] = [];
  const files: string[] = [];
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
      packageDirectories.push(directory);
      for (const file of [...pkg.sources, pkg.governingManifest]) {
        const path = indexablePath(root, file);
        if (path === null) throw new Error(`${directory} compiles ${file}, which Nx's file index cannot hold`);
        files.push(path);
      }
    }
    const unglobbable = [...packageDirectories, ...files].find((path) => GLOB_SYNTAX.test(path));
    if (unglobbable !== undefined) throw new Error(`${unglobbable} contains glob syntax, so no fileset can name it`);
  } catch (error) {
    throw new CargoClosureInputError(projectRoot, error);
  }

  // A package nested in another member's directory is already covered by it.
  const directories = [...new Set(packageDirectories)]
    .sort()
    .filter((directory, index, sorted) => !sorted.slice(0, index).some((outer) => contains(outer, directory)));
  const ancestors = new Set<string>();
  for (const directory of directories) {
    for (let ancestor = parent(directory); ancestor !== null; ancestor = parent(ancestor)) {
      const candidate = ancestor;
      if (!directories.some((outer) => contains(outer, candidate))) ancestors.add(candidate);
    }
  }
  // Target sources outside every package directory, and governing manifests
  // that are not already one of the ancestors' manifests.
  const loose = new Set(
    files.filter(
      (file) =>
        !directories.some((directory) => contains(directory, file)) &&
        !(posix.basename(file) === 'Cargo.toml' && ancestors.has(parent(file) ?? '')),
    ),
  );
  // Sibling packages share one pattern, `crates/{a,b}/…`, so a closure of
  // dozens of crates reads as a few lines in `nx show project`.
  const siblings = new Map<string, string[]>();
  for (const directory of directories) {
    const key = parent(directory);
    if (key === null) continue;
    siblings.set(key, [...(siblings.get(key) ?? []), posix.basename(directory)]);
  }
  const packageGlobs = directories.includes('')
    ? ['']
    : [...siblings].map(([under, names]) =>
        posix.join(under, names.length === 1 ? names.join('') : `{${names.join(',')}}`),
      );
  const anchored = (path: string, pattern = '') => posix.join('{workspaceRoot}', path, pattern);
  return [
    lock,
    ...packageGlobs.map((glob) => anchored(glob, PACKAGE_FILES)),
    ...packageGlobs.map((glob) => `!${anchored(glob, SKIPPED_TREES)}`),
    ...[...ancestors].sort().map((ancestor) => anchored(ancestor, ANCESTOR_FILES)),
    ...[...loose].sort().map((file) => anchored(file)),
    ...(external ? [{ runtime: `${CARGO_HASH_COMMAND} ${closureArguments}` }] : []),
  ];
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

function contains(directory: string, path: string): boolean {
  return directory === '' || path === directory || path.startsWith(`${directory}/`);
}

/** One shell word: bare when nothing in it is special to `sh`, single-quoted otherwise. */
function shellWord(word: string): string {
  return /^[\w@%+=:,./-]+$/.test(word) ? word : `'${word.replaceAll("'", `'"'"'`)}'`;
}
