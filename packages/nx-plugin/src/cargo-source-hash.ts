import { execFileSync, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { realpathSync, statSync } from 'node:fs';
import { lstat, readdir, readFile, readlink, realpath } from 'node:fs/promises';
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import typia from 'typia';

interface CargoMetadata {
  packages: {
    id: string;
    source: string | null;
    manifest_path: string;
    targets: { src_path: string }[];
  }[];
  resolve: { nodes: { id: string; deps: { pkg: string }[] }[] } | null;
  workspace_root: string;
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

const parseMetadata = typia.json.createAssertParse<CargoMetadata>();
const ignoredDirectories = new Set([
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
]);
const ancestorInputs = ['Cargo.toml', '.cargo/config', '.cargo/config.toml'];

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
// stderr is kept for the failure path only, where execFileSync attaches it to
// the thrown error. Git runs under the same rule.
const CHILD_STDIO: ['ignore', 'pipe', 'pipe'] = ['ignore', 'pipe', 'pipe'];

export async function hashCargoPathInputs(
  manifestPath: string,
  workspaceRoot: string,
  options?: CargoPathInputsOptions,
): Promise<string> {
  const root = await realpath(workspaceRoot);
  const metadata = parseMetadata(
    execFileSync(
      'cargo',
      ['metadata', '--format-version', '1', '--locked', '--offline', '--manifest-path', resolve(manifestPath)],
      { cwd: root, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024, stdio: CHILD_STDIO },
    ),
  );
  const cargoRoot = await realpath(metadata.workspace_root);
  const local: { id: string; directory: string; manifest: string; sources: string[] }[] = [];
  for (const pkg of metadata.packages) {
    if (pkg.source !== null) continue;
    const manifest = await realpath(pkg.manifest_path);
    const sources: string[] = [];
    for (const target of pkg.targets) sources.push(await realpath(target.src_path));
    local.push({ id: pkg.id, directory: dirname(manifest), manifest, sources });
  }
  const members =
    options?.closure === undefined ? undefined : await dependencyClosure(metadata, local, resolve(options.closure));

  const directories = new Set<string>();
  const files = new Set<string>();
  const ancestors = new Set<string>();
  const links = new Map<string, string>();
  const packageSources: string[] = [];
  for (const pkg of local) {
    if (members !== undefined && !members.has(pkg.id)) continue;
    const directory = pkg.directory;
    if (!options?.includeWorkspace) {
      const fromRoot = relative(root, directory);
      const fromCargo = relative(cargoRoot, directory);
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
    // Cargo resolves explicit package.workspace ownership as well as ancestor
    // workspaces. Metadata's workspace_root describes only the invoking root.
    const governingManifest = execFileSync(
      'cargo',
      ['locate-project', '--workspace', '--manifest-path', pkg.manifest, '--message-format', 'plain'],
      { cwd: root, encoding: 'utf8', stdio: CHILD_STDIO },
    ).trim();
    files.add(await realpath(governingManifest));
    packageSources.push(...pkg.sources);
    // A member can inherit edition, lint policy, dependencies and
    // profiles from a workspace above its package directory. Those manifests
    // are not descendants of the source roots returned by Cargo metadata.
    for (let ancestor = dirname(directory); !ancestors.has(ancestor); ancestor = dirname(ancestor)) {
      ancestors.add(ancestor);
      for (const name of ancestorInputs) {
        const file = join(ancestor, name);
        if (statSync(file, { throwIfNoEntry: false })?.isFile()) files.add(await realpath(file));
      }
    }
  }

  const ignored = gitIgnoredBeneath([...directories]);
  for (const source of packageSources) if (!ignored(source)) files.add(source);
  // Overlapping packages (a facade root plus its child crates) share one walk.
  const visited = new Set<string>();
  async function walk(directory: string): Promise<void> {
    const canonical = await realpath(directory);
    if (visited.has(canonical)) return;
    visited.add(canonical);
    for (const entry of await readdir(canonical, { withFileTypes: true })) {
      if (ignoredDirectories.has(entry.name)) continue;
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
async function dependencyClosure(
  metadata: CargoMetadata,
  local: readonly { id: string; directory: string }[],
  directory: string,
): Promise<ReadonlySet<string>> {
  const base = await realpath(directory);
  const pending = local
    .filter((pkg) => pkg.directory === base || pkg.directory.startsWith(`${base}${sep}`))
    .map((pkg) => pkg.id);
  if (pending.length === 0) throw new Error(`no local Cargo package lies under ${directory}`);
  if (metadata.resolve === null) throw new Error('cargo metadata reported no dependency resolution');
  const edges = new Map(metadata.resolve.nodes.map((node) => [node.id, node.deps]));
  const reached = new Set<string>();
  for (let id = pending.pop(); id !== undefined; id = pending.pop()) {
    if (reached.has(id)) continue;
    reached.add(id);
    for (const dependency of edges.get(id) ?? []) pending.push(dependency.pkg);
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
