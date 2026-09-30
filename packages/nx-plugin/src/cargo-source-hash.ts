import { execFile, execFileSync, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { realpathSync, statSync } from 'node:fs';
import { lstat, readdir, readFile, readlink, realpath } from 'node:fs/promises';
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';

/** The fields of `cargo metadata --format-version 1` this module reads. */
interface CargoMetadata {
  packages: {
    id: string;
    source: string | null;
    manifest_path: string;
    targets: { src_path: string }[];
  }[];
  resolve: { nodes: { id: string; deps: { pkg: string }[] }[] } | null;
  workspace_members: string[];
  workspace_root: string;
}

/**
 * Narrowed field by field, not by typia: graph inference imports this module,
 * and a workspace that loads the plugin from its sources runs them without
 * typia's compile-time transform.
 */
function parseCargoMetadata(json: string): CargoMetadata {
  const member = (value: unknown, key: string): unknown =>
    typeof value === 'object' && value !== null && key in value ? Reflect.get(value, key) : undefined;
  const text = (value: unknown, field: string): string => {
    if (typeof value !== 'string') throw new Error(`cargo metadata reported no valid ${field}`);
    return value;
  };
  const list = (value: unknown, field: string): unknown[] => {
    if (!Array.isArray(value)) throw new Error(`cargo metadata reported no valid ${field}`);
    return value;
  };
  const document: unknown = JSON.parse(json);
  const resolveGraph = member(document, 'resolve');
  return {
    packages: list(member(document, 'packages'), 'packages').map((pkg) => ({
      id: text(member(pkg, 'id'), 'package id'),
      source: member(pkg, 'source') === null ? null : text(member(pkg, 'source'), 'package source'),
      manifest_path: text(member(pkg, 'manifest_path'), 'package manifest_path'),
      targets: list(member(pkg, 'targets'), 'package targets').map((target) => ({
        src_path: text(member(target, 'src_path'), 'target src_path'),
      })),
    })),
    resolve:
      resolveGraph === null
        ? null
        : {
            nodes: list(member(resolveGraph, 'nodes'), 'resolve').map((node) => ({
              id: text(member(node, 'id'), 'resolve node id'),
              deps: list(member(node, 'deps'), 'resolve node deps').map((dependency) => ({
                pkg: text(member(dependency, 'pkg'), 'resolve edge'),
              })),
            })),
          },
    workspace_members: list(member(document, 'workspace_members'), 'workspace_members').map((id) =>
      text(id, 'workspace member'),
    ),
    workspace_root: text(member(document, 'workspace_root'), 'workspace_root'),
  };
}

/** A mutable (path) package in Cargo's locked resolve. Every path is canonical. */
export interface LocalCargoPackage {
  readonly id: string;
  readonly directory: string;
  readonly manifest: string;
  /** Every target's source file: library, binaries, tests, build script. A target may live outside `directory`. */
  readonly sources: readonly string[];
}

/** What locked, offline `cargo metadata` reports about one Cargo workspace's mutable packages. */
export interface CargoResolve {
  /** The canonical Cargo workspace root. */
  readonly root: string;
  /** The workspace's own members: `root`'s manifest governs each of them. */
  readonly members: ReadonlySet<string>;
  readonly local: readonly LocalCargoPackage[];
  /** Every dependency edge by package id, or null when Cargo reported no resolution. */
  readonly edges: ReadonlyMap<string, readonly string[]> | null;
}

export interface CargoPathInputsOptions {
  /** Also hash packages inside both the Nx and Cargo workspaces, for custom targets without inferred Cargo inputs. */
  includeWorkspace?: boolean;
  /**
   * Hash only the resolved dependency closure of the local packages whose manifests lie under this directory
   * (resolved like `manifestPath`), instead of every package in the Cargo workspace.
   */
  closure?: string;
}

/**
 * Cargo's stdout, or its failure with Cargo's stderr in the message. Not
 * `promisify` from `node:util`: importing that module makes Node probe color
 * support at startup, and with FORCE_COLOR and NO_COLOR both set (an Nx
 * inside an Nx task, in a shell that exports NO_COLOR) it warns on stderr
 * under its own pid, which a runtime input would hash as a new digest on
 * every run.
 */
function runCargo(args: readonly string[], cwd: string): Promise<string> {
  // The executor form: the workspace compiles against es2022, which has no `Promise.withResolvers`.
  return new Promise((settle, reject) => {
    execFile('cargo', args, { cwd, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 }, (error, stdout) =>
      error === null ? settle(stdout) : reject(error),
    );
  });
}

/** Directories the hash never descends into: build output, VCS and volume metadata, installed packages. */
export const HASH_SKIPPED_DIRECTORIES = [
  'target',
  '.git',
  'node_modules',
  '.cache',
  '.cowshed',
  '.fseventsd',
  '.TemporaryItems',
  '.Trashes',
  '.Spotlight-V100',
  '.DocumentRevisions-V100',
] as const;
/** Files in every ancestor of a package directory that Cargo reads while building it. */
export const CARGO_ANCESTOR_INPUTS = ['Cargo.toml', '.cargo/config', '.cargo/config.toml'] as const;

/**
 * Cargo owns resolution, including workspace inheritance, target dependencies,
 * patches, and transitive path dependencies. A locked offline query observes
 * that graph without fetching dependencies or changing the lockfile. Git and
 * registry packages are immutable inputs already identified by Cargo.lock.
 * By default, Nx's in-workspace inputs own workspace packages; includeWorkspace
 * also covers those packages for custom targets without inferred Cargo inputs.
 * `closure` narrows the packages to what the local packages under one
 * directory actually depend on, so an edit to an unrelated workspace member
 * leaves the digest alone.
 *
 * This covers Rust sources and Cargo configuration, matching the plugin's
 * in-workspace Rust inputs. Build scripts' other data/environment inputs remain
 * explicit Nx inputs, just as they are for in-workspace crates. Files git
 * ignores beneath a package are left out, like Nx's own file inputs: a
 * generated source is its producer's output, which the consuming target hashes
 * through `dependentTasksOutputFiles`, and a tree that has not generated it yet
 * must hash like one that has.
 */
// Nx hashes a runtime input's stdout AND stderr. Cargo reports lock contention
// on stderr ("Blocking waiting for file lock on package cache") a
// timing-dependent number of times when Nx hashes many tasks at once, which
// made one unchanged tree produce four different task hashes per run. Cargo's
// stderr is kept for the failure path only, where execFile attaches it to the
// rejected error. Git runs under the same rule.
const CHILD_STDIO: ['ignore', 'pipe', 'pipe'] = ['ignore', 'pipe', 'pipe'];

/** Locked, offline `cargo metadata` for the workspace `manifestPath` names, run from `cwd`. */
export async function readCargoResolve(manifestPath: string, cwd: string): Promise<CargoResolve> {
  const stdout = await runCargo(
    ['metadata', '--format-version', '1', '--locked', '--offline', '--manifest-path', resolve(manifestPath)],
    cwd,
  );
  const metadata = parseCargoMetadata(stdout);
  const local = await Promise.all(
    metadata.packages
      .filter((pkg) => pkg.source === null)
      .map(async (pkg): Promise<LocalCargoPackage> => {
        const manifest = await realpath(pkg.manifest_path);
        const sources = await Promise.all(pkg.targets.map((target) => realpath(target.src_path)));
        return { id: pkg.id, directory: dirname(manifest), manifest, sources };
      }),
  );
  return {
    root: await realpath(metadata.workspace_root),
    members: new Set(metadata.workspace_members),
    local,
    edges:
      metadata.resolve === null
        ? null
        : new Map(metadata.resolve.nodes.map((node) => [node.id, node.deps.map((dependency) => dependency.pkg)])),
  };
}

/**
 * The canonical root manifest of the Cargo workspace that governs `pkg`.
 * Metadata's `workspace_root` describes only the invoking workspace, which is
 * the answer for its own members. Any other path package asks Cargo, which
 * resolves an explicit `package.workspace` as well as the ancestor search.
 */
export async function governingManifest(cargo: CargoResolve, pkg: LocalCargoPackage, cwd: string): Promise<string> {
  if (cargo.members.has(pkg.id)) return realpath(join(cargo.root, 'Cargo.toml'));
  const stdout = await runCargo(
    ['locate-project', '--workspace', '--manifest-path', pkg.manifest, '--message-format', 'plain'],
    cwd,
  );
  return realpath(stdout.trim());
}

export async function hashCargoPathInputs(
  manifestPath: string,
  workspaceRoot: string,
  options?: CargoPathInputsOptions,
): Promise<string> {
  const root = await realpath(workspaceRoot);
  const cargo = await readCargoResolve(manifestPath, root);
  const members = options?.closure === undefined ? undefined : await dependencyClosure(cargo, resolve(options.closure));

  const directories = new Set<string>();
  const files = new Set<string>();
  const ancestors = new Set<string>();
  const links = new Map<string, string>();
  const packageSources: string[] = [];
  for (const pkg of cargo.local) {
    if (members !== undefined && !members.has(pkg.id)) continue;
    const directory = pkg.directory;
    if (!options?.includeWorkspace) {
      const fromRoot = relative(root, directory);
      const fromCargo = relative(cargo.root, directory);
      if (
        fromRoot !== '..' &&
        !fromRoot.startsWith(`..${sep}`) &&
        !isAbsolute(fromRoot) &&
        fromCargo !== '..' &&
        !fromCargo.startsWith(`..${sep}`) &&
        !isAbsolute(fromCargo) &&
        !fromRoot.split(sep).includes('node_modules')
      )
        continue;
    }
    directories.add(directory);
    files.add(pkg.manifest);
    files.add(await governingManifest(cargo, pkg, root));
    packageSources.push(...pkg.sources);
    // A member can inherit edition, lint policy, dependencies and
    // profiles from a workspace above its package directory. Those manifests
    // are not descendants of the source roots returned by Cargo metadata.
    for (let ancestor = dirname(directory); !ancestors.has(ancestor); ancestor = dirname(ancestor)) {
      ancestors.add(ancestor);
      for (const name of CARGO_ANCESTOR_INPUTS) {
        const file = join(ancestor, name);
        if (statSync(file, { throwIfNoEntry: false })?.isFile()) files.add(await realpath(file));
      }
    }
  }

  const ignored = gitIgnoredBeneath([...directories]);
  for (const source of packageSources) if (!ignored(source)) files.add(source);
  const skipped: ReadonlySet<string> = new Set(HASH_SKIPPED_DIRECTORIES);
  // Overlapping packages (a facade root plus its child crates) share one walk.
  const visited = new Set<string>();
  async function walk(directory: string): Promise<void> {
    const canonical = await realpath(directory);
    if (visited.has(canonical)) return;
    visited.add(canonical);
    for (const entry of await readdir(canonical, { withFileTypes: true })) {
      if (skipped.has(entry.name)) continue;
      const path = join(canonical, entry.name);
      if (ignored(path)) continue;
      if (entry.isSymbolicLink()) links.set(path, await readlink(path));
      const info = entry.isSymbolicLink() ? await lstat(await realpath(path)) : entry;
      if (info.isDirectory()) {
        await walk(path);
      } else if (
        info.isFile() &&
        (entry.name.endsWith('.rs') ||
          entry.name === 'Cargo.toml' ||
          (canonical.endsWith(`${sep}.cargo`) && (entry.name === 'config' || entry.name === 'config.toml')))
      ) {
        files.add(await realpath(path));
      }
    }
  }
  for (const directory of directories) await walk(directory);
  const hash = createHash('sha256');
  hash.update('cargo-path-inputs-v1\0');
  for (const [path, target] of [...links].sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))) {
    hash.update(relative(root, path));
    hash.update('\0symlink\0');
    hash.update(target);
    hash.update('\0');
  }
  for (const file of [...files].sort()) {
    const bytes = await readFile(file);
    // Both names and bytes matter: moving a module can change resolution even
    // when its contents are unchanged. Length framing makes boundaries unique.
    hash.update(relative(root, file));
    hash.update('\0');
    hash.update(String(bytes.length));
    hash.update('\0');
    hash.update(bytes);
  }
  return hash.digest('hex');
}

/**
 * The ids Cargo's resolve graph reaches from the local packages under
 * `directory`, over every dependency kind: a target may build, test or run
 * build scripts, and any of those compiles the edge's source.
 */
export async function dependencyClosure(cargo: CargoResolve, directory: string): Promise<ReadonlySet<string>> {
  const base = await realpath(directory);
  const pending = cargo.local
    .filter((pkg) => pkg.directory === base || pkg.directory.startsWith(`${base}${sep}`))
    .map((pkg) => pkg.id);
  if (pending.length === 0) throw new Error(`no local Cargo package lies under ${directory}`);
  if (cargo.edges === null) throw new Error('cargo metadata reported no dependency resolution');
  const reached = new Set<string>();
  for (let id = pending.pop(); id !== undefined; id = pending.pop()) {
    if (reached.has(id)) continue;
    reached.add(id);
    for (const dependency of cargo.edges.get(id) ?? []) pending.push(dependency);
  }
  return reached;
}

/**
 * Whether git ignores a path beneath one of the package directories. A
 * package whose own directory is ignored (an installed dependency, say) keeps
 * every file, and a directory outside any git repository ignores nothing.
 */
function gitIgnoredBeneath(directories: readonly string[]): (path: string) => boolean {
  const byRepository = new Map<string, string[]>();
  for (const directory of directories) {
    const repository = gitRepository(directory);
    if (repository === undefined) continue;
    const members = byRepository.get(repository) ?? [];
    members.push(directory);
    byRepository.set(repository, members);
  }
  const files = new Set<string>();
  const trees: string[] = [];
  for (const [repository, members] of byRepository) {
    const relativeMembers = members.map((member) => relative(repository, member) || '.');
    // A package whose own directory git ignores is an installed tree, not a source tree with
    // generated files in it. It also stays out of ls-files, which fails outright on a pathspec
    // inside an ignored directory ("directory entry not superset of prefix").
    const ignoredRoots = new Set(gitCheckIgnore(repository, relativeMembers));
    const sourceRoots = relativeMembers.filter((member) => !ignoredRoots.has(member));
    if (sourceRoots.length === 0) continue;
    const installed = [...ignoredRoots].map((member) => join(repository, member));
    const listing = execFileSync(
      'git',
      [
        '--literal-pathspecs',
        '-C',
        repository,
        'ls-files',
        '-z',
        '--others',
        '--ignored',
        '--exclude-standard',
        '--directory',
        '--',
        ...sourceRoots,
      ],
      { encoding: 'utf8', maxBuffer: 64 * 1024 * 1024, stdio: CHILD_STDIO },
    );
    for (const entry of listing.split('\0')) {
      if (entry.length === 0) continue;
      const path = join(repository, entry);
      if (!entry.endsWith('/')) {
        files.add(path);
      } else if (!installed.some((member) => member === path || member.startsWith(`${path}${sep}`))) {
        // An ignored directory holding a nested installed package leaves that package whole.
        trees.push(path);
      }
    }
  }
  return (path) => files.has(path) || trees.some((tree) => path === tree || path.startsWith(`${tree}${sep}`));
}

/** The members of `paths` (relative to `repository`) that git ignores, own patterns or an ancestor's. */
function gitCheckIgnore(repository: string, paths: readonly string[]): string[] {
  const result = spawnSync('git', ['-C', repository, 'check-ignore', '-z', '--stdin'], {
    input: `${paths.join('\0')}\0`,
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024,
  });
  // check-ignore answers 1 when nothing is ignored; only 0 carries a listing.
  if (result.status === 1) return [];
  if (result.status !== 0) {
    throw new Error(`git check-ignore in ${repository} failed (${result.status ?? result.signal}): ${result.stderr}`);
  }
  return result.stdout.split('\0').filter((path) => path.length > 0);
}

/** The canonical top level of the git work tree holding `directory`, or undefined outside any repository. */
function gitRepository(directory: string): string | undefined {
  try {
    // The walk compares realpaths, so git's answer is canonicalized the same way.
    return realpathSync(
      execFileSync('git', ['-C', directory, 'rev-parse', '--show-toplevel'], {
        encoding: 'utf8',
        stdio: CHILD_STDIO,
      }).trim(),
    );
  } catch (error) {
    if (
      typeof error === 'object' &&
      error !== null &&
      'status' in error &&
      error.status === 128 &&
      'stderr' in error &&
      String(error.stderr).includes('not a git repository')
    )
      return undefined;
    throw error;
  }
}
