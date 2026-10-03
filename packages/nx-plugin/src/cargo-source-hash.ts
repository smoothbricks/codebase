import { type ChildProcess, execFileSync, spawn, spawnSync } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { constants, realpathSync, statSync } from 'node:fs';
import {
  access,
  lstat,
  mkdir,
  readdir,
  readFile,
  readlink,
  realpath,
  rename,
  rm,
  stat,
  writeFile,
} from 'node:fs/promises';
import { homedir } from 'node:os';
import { delimiter, dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import native from 'nx/src/native/index.js';
import { workspaceDataDirectoryForWorkspace } from 'nx/src/utils/cache-directory.js';
import { CARGO_TOOLCHAIN_PIN_INPUTS } from './cargo-toolchain-policy.js';

const { FileLock, IS_WASM, WorkspaceContext } = native;

/** The fields of `cargo metadata --format-version 1` this module reads. */
interface CargoMetadata {
  packages: {
    id: string;
    source: string | null;
    manifest_path: string;
    targets: { src_path: string }[];
    /** Added by the cache after Cargo identifies each local package's governing workspace. */
    governing_manifest_path?: string;
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
      ...(member(pkg, 'governing_manifest_path') === undefined
        ? {}
        : { governing_manifest_path: text(member(pkg, 'governing_manifest_path'), 'governing manifest') }),
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
  readonly governingManifest: string;
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
 * Nx workers handle termination by exiting, so their exit hook kills owned
 * Cargo groups. Standalone callers need signal forwarding after detaching
 * Cargo from the terminal; handlers are installed only where the caller has
 * not already chosen a signal disposition, and removed after the last child.
 * Uncatchable SIGKILL cannot execute either hook.
 */
const cargoChildren = new Set<ChildProcess>();

function killCargoChild(child: ChildProcess): void {
  if (process.platform === 'win32' || child.pid === undefined) child.kill('SIGKILL');
  else {
    try {
      process.kill(-child.pid, 'SIGKILL');
    } catch (error) {
      if (!(error instanceof Error && 'code' in error && error.code === 'ESRCH')) throw error;
    }
  }
}

function killCargoChildren(): void {
  for (const child of cargoChildren) killCargoChild(child);
}

type CargoSignal = 'SIGINT' | 'SIGTERM' | 'SIGHUP';
const forwardedSignals = new Set<CargoSignal>();
const cargoSignalHandlers: Record<CargoSignal, () => void> = {
  SIGINT: () => forwardCargoSignal('SIGINT'),
  SIGTERM: () => forwardCargoSignal('SIGTERM'),
  SIGHUP: () => forwardCargoSignal('SIGHUP'),
};

function removeCargoSignalHandlers(): void {
  for (const signal of forwardedSignals) process.removeListener(signal, cargoSignalHandlers[signal]);
  forwardedSignals.clear();
}

function forwardCargoSignal(signal: CargoSignal): void {
  killCargoChildren();
  removeCargoSignalHandlers();
  process.kill(process.pid, signal);
}

function ownCargoChildren(): void {
  process.on('exit', killCargoChildren);
  for (const signal of Object.keys(cargoSignalHandlers)) {
    if (signal !== 'SIGINT' && signal !== 'SIGTERM' && signal !== 'SIGHUP') continue;
    if (process.listenerCount(signal) !== 0) continue;
    process.on(signal, cargoSignalHandlers[signal]);
    forwardedSignals.add(signal);
  }
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
  // spawn forwards detached groups; execFile does not. The public ES2022
  // library API requires the constructor form, not Promise.withResolvers.
  return new Promise((settle, reject) => {
    const child = spawn('cargo', args, {
      cwd,
      detached: process.platform !== 'win32',
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderr = '';
    let stdoutBytes = 0;
    let stderrBytes = 0;
    let failure: Error | undefined;
    const fail = (error: Error): void => {
      failure ??= error;
      killCargoChild(child);
    };
    child.stdout.setEncoding('utf8');
    child.stderr.setEncoding('utf8');
    child.stdout.on('data', (chunk: string) => {
      if (failure !== undefined) return;
      stdoutBytes += Buffer.byteLength(chunk);
      if (stdoutBytes > 64 * 1024 * 1024) {
        fail(new Error('Cargo stdout exceeded 64 MiB'));
        return;
      }
      stdout += chunk;
    });
    child.stderr.on('data', (chunk: string) => {
      if (failure !== undefined) return;
      stderrBytes += Buffer.byteLength(chunk);
      if (stderrBytes > 64 * 1024 * 1024) {
        fail(new Error('Cargo stderr exceeded 64 MiB'));
        return;
      }
      stderr += chunk;
    });
    child.on('error', (error) => {
      failure ??= error;
    });
    child.stdout.on('error', fail);
    child.stderr.on('error', fail);
    child.on('close', (code, signal) => {
      cargoChildren.delete(child);
      if (cargoChildren.size === 0) {
        process.removeListener('exit', killCargoChildren);
        removeCargoSignalHandlers();
      }
      if (failure !== undefined) reject(failure);
      else if (code !== 0) {
        reject(new Error(`cargo ${args.join(' ')} failed (${signal ?? code}): ${stderr.trim()}`));
      } else settle(stdout);
    });
    if (cargoChildren.size === 0) ownCargoChildren();
    cargoChildren.add(child);
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
// stderr is kept for the failure path only, where runCargo includes it in the
// rejected error. Git runs under the same rule.
const CHILD_STDIO: ['ignore', 'pipe', 'pipe'] = ['ignore', 'pipe', 'pipe'];

/** A Cargo resolution refusal: no stale closure or runtime whole-workspace substitute is safe. */
export class CargoMetadataError extends Error {
  readonly manifestPath: string;

  constructor(manifestPath: string, cause: unknown) {
    const detail =
      cause instanceof Error
        ? `${cause.name}: ${cause.message}`
        : typeof cause === 'object' && cause !== null && 'message' in cause
          ? String(cause.message)
          : String(cause);
    super(`Locked offline Cargo resolution failed for ${manifestPath}: ${detail}`, { cause });
    this.name = 'CargoMetadataError';
    this.manifestPath = manifestPath;
  }
}

interface ResolveCache {
  readonly key: string;
  readonly cargo: CargoResolve;
}
interface ResolveState {
  ready: Promise<void>;
  cached: ResolveCache | null;
  pending?: { readonly key: string; readonly promise: Promise<CargoResolve> };
}
const resolves = new Map<string, ResolveState>();
const RESOLVE_CACHE_SCHEMA = 'cargo-resolve-v1';

/**
 * The full manifest SET, not merely the last closure: adding a glob workspace
 * member must invalidate resolution even when Cargo.lock did not change.
 * Graph callers supply Nx's already-current index; standalone hash callers
 * take a fresh native snapshot. File contents below are read directly so an
 * edit within Nx's mtime granularity cannot reuse a stale resolution.
 */
function resolutionFiles(
  root: string,
  manifest: string,
  cargo: CargoResolve | null,
  indexed: ReadonlySet<string>,
): ReadonlySet<string> {
  const paths = new Set<string>([manifest]);
  for (const file of indexed) paths.add(resolve(root, file));
  for (const pkg of cargo?.local ?? []) {
    paths.add(pkg.manifest);
    paths.add(pkg.governingManifest);
  }
  if (cargo !== null) paths.add(join(cargo.root, 'Cargo.toml'));
  const directories = new Set<string>([root]);
  for (const file of paths) directories.add(dirname(file));
  for (const directory of directories) {
    paths.add(join(directory, 'Cargo.lock'));
    for (let ancestor = directory; ; ancestor = dirname(ancestor)) {
      paths.add(join(ancestor, '.cargo/config'));
      paths.add(join(ancestor, '.cargo/config.toml'));
      paths.add(join(ancestor, 'rust-toolchain'));
      paths.add(join(ancestor, 'rust-toolchain.toml'));
      paths.add(join(ancestor, 'Cargo.toml'));
      if (dirname(ancestor) === ancestor) break;
    }
  }
  for (const pin of CARGO_TOOLCHAIN_PIN_INPUTS) paths.add(pin.replace('{workspaceRoot}', root));
  const cargoHome = resolve(process.env.CARGO_HOME ?? join(homedir(), '.cargo'));
  paths.add(join(cargoHome, 'config'));
  paths.add(join(cargoHome, 'config.toml'));
  return paths;
}

/** Toolchain selection can change even while the declared pin's bytes do not. */
async function cargoExecutable(): Promise<string> {
  for (const directory of (process.env.PATH ?? '').split(delimiter)) {
    const executable = resolve(directory, process.platform === 'win32' ? 'cargo.exe' : 'cargo');
    try {
      await access(executable, constants.X_OK);
      return await realpath(executable);
    } catch (error) {
      if (
        !(
          error instanceof Error &&
          'code' in error &&
          (error.code === 'ENOENT' || error.code === 'EACCES' || error.code === 'ENOTDIR')
        )
      )
        throw error;
    }
  }
  return '<cargo-not-on-PATH>';
}

async function resolutionKey(
  root: string,
  manifest: string,
  cargo: CargoResolve | null,
  indexed: ReadonlySet<string>,
): Promise<string> {
  const cargoHome = resolve(process.env.CARGO_HOME ?? join(homedir(), '.cargo'));
  const executable = await cargoExecutable();
  const hash = createHash('sha256').update(
    `${RESOLVE_CACHE_SCHEMA}\0${root}\0${manifest}\0${cargoHome}\0${executable}\0${process.env.RUSTUP_TOOLCHAIN ?? ''}\0${process.env.RUSTUP_HOME ?? ''}\0`,
  );
  for (const file of [...resolutionFiles(root, manifest, cargo, indexed)].sort()) {
    hash.update(file).update('\0');
    try {
      const [bytes, canonical] = await Promise.all([readFile(file), realpath(file)]);
      hash.update(`${canonical}\0${bytes.length}\0`).update(bytes);
    } catch (error) {
      if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) throw error;
      hash.update('missing');
    }
    hash.update('\0');
  }
  return hash.digest('hex');
}

function cargoFromMetadata(metadata: CargoMetadata): CargoResolve {
  return {
    root: metadata.workspace_root,
    members: new Set(metadata.workspace_members),
    local: metadata.packages
      .filter((pkg) => pkg.source === null)
      .map((pkg) => {
        if (pkg.governing_manifest_path === undefined) throw new Error('Cargo cache omitted a governing manifest');
        return {
          id: pkg.id,
          directory: dirname(pkg.manifest_path),
          manifest: pkg.manifest_path,
          sources: pkg.targets.map((target) => target.src_path),
          governingManifest: pkg.governing_manifest_path,
        };
      }),
    edges:
      metadata.resolve === null
        ? null
        : new Map(metadata.resolve.nodes.map((node) => [node.id, node.deps.map((dep) => dep.pkg)])),
  };
}

async function loadResolveCache(file: string): Promise<ResolveCache | null> {
  let content: string;
  try {
    content = await readFile(file, 'utf8');
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return null;
    throw error;
  }
  try {
    const newline = content.indexOf('\n');
    const key = content.slice(0, newline);
    if (newline === -1 || !/^[a-f0-9]{64}$/.test(key)) throw new Error('invalid content-key header');
    const metadata = parseCargoMetadata(content.slice(newline + 1));
    return { key, cargo: cargoFromMetadata(metadata) };
  } catch (error) {
    process.stderr.write(
      `@smoothbricks/nx-plugin: discarding corrupt Cargo resolution cache ${file}: ${error instanceof Error ? error.message : String(error)}\n`,
    );
    return null;
  }
}

/**
 * One content-keyed resolution per Cargo workspace, across concurrent graph
 * calls and workers. Nx's kernel FileLock releases even if its worker dies;
 * in-process callers join before taking that lock, so a synchronous lock()
 * cannot block the continuation that owns it. A changed key queues behind
 * the old child instead of receiving its stale answer.
 */
export async function readCargoResolve(
  manifestPath: string,
  cwd: string,
  indexed?: ReadonlySet<string>,
): Promise<CargoResolve> {
  let manifest = resolve(manifestPath);
  try {
    const root = await realpath(cwd);
    manifest = await realpath(manifest);
    const index =
      indexed ?? new Set(new WorkspaceContext(root, workspaceDataDirectoryForWorkspace(root)).glob(['**/Cargo.toml']));
    const identity = createHash('sha256').update(`${RESOLVE_CACHE_SCHEMA}\0${root}\0${manifest}`).digest('hex');
    const directory = join(workspaceDataDirectoryForWorkspace(root), 'cargo-resolve', identity);
    const file = join(directory, 'entry.cache');
    let state = resolves.get(identity);
    if (state === undefined) {
      const created: ResolveState = { ready: Promise.resolve(), cached: null };
      created.ready = loadResolveCache(file).then((cached) => {
        created.cached = cached;
      });
      resolves.set(identity, created);
      state = created;
      try {
        await created.ready;
      } catch (error) {
        resolves.delete(identity);
        throw error;
      }
    } else await state.ready;
    for (let joined = 0; joined <= 3; joined += 1) {
      const key = await resolutionKey(root, manifest, state.cached?.cargo ?? null, index);
      const pending = state.pending;
      if (pending !== undefined) {
        if (joined === 3) break;
        try {
          await pending.promise;
        } catch (error) {
          // Sharers of a refused key receive its refusal; a changed key still
          // needs its own resolution after the old flight releases the lock.
          if (pending.key === key) throw error;
        }
        // Re-key every joiner with the discovered paths, including an external
        // manifest edited after the flight's last validation, before publication.
        continue;
      }
      if (state.cached?.key === key) return state.cached.cargo;
      if (joined === 3) break;
      const active = state;
      const promise = resolveAndCache(root, manifest, index, directory, file, active);
      active.pending = { key, promise };
      try {
        return await promise;
      } finally {
        if (active.pending?.promise === promise) delete active.pending;
      }
    }
    throw new Error('Cargo resolution inputs kept changing across three consecutive in-flight queries');
  } catch (error) {
    if (error instanceof CargoMetadataError) throw error;
    throw new CargoMetadataError(manifest, error);
  }
}

async function resolveAndCache(
  root: string,
  manifest: string,
  indexed: ReadonlySet<string>,
  directory: string,
  file: string,
  state: ResolveState,
): Promise<CargoResolve> {
  if (IS_WASM)
    throw new Error('Cargo resolution requires the native Nx file lock; a WASM Nx host cannot own Cargo children');
  await mkdir(directory, { recursive: true });
  const lockFile = join(directory, 'resolve.lock');
  const lock = new FileLock(lockFile);
  while (lock.check()) await lock.wait();
  // Nx's lock() is synchronous: losing this inter-process check/acquire race
  // can block for one foreign flight. Only the in-process leader reaches it.
  lock.lock();
  try {
    // Another worker may have populated it while this worker waited.
    state.cached = await loadResolveCache(file);
    let inputs = state.cached?.cargo ?? null;
    for (let attempt = 0; attempt < 3; attempt += 1) {
      const before = await resolutionKey(root, manifest, inputs, indexed);
      if (state.cached?.key === before) return state.cached.cargo;
      const knownFiles = resolutionFiles(root, manifest, inputs, indexed);
      // A filesystem timestamp, not a sandbox's wall clock: a newly
      // discovered external input edited during this flight requires replay.
      await writeFile(lockFile, '');
      const started = (await stat(lockFile)).mtimeMs;
      const metadata = await resolveCargoMetadata(manifest, root);
      const cargo = cargoFromMetadata(metadata);
      // Capture the published key before validation. Reading it afterwards
      // could pair a post-edit key with Cargo's pre-edit answer.
      const key = await resolutionKey(root, manifest, cargo, indexed);
      let changed = before !== (await resolutionKey(root, manifest, inputs, indexed));
      for (const added of resolutionFiles(root, manifest, cargo, indexed)) {
        if (knownFiles.has(added)) continue;
        try {
          if ((await stat(added)).mtimeMs >= started) changed = true;
        } catch (error) {
          if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) throw error;
        }
      }
      if (changed) {
        inputs = cargo;
        continue;
      }
      const temporary = `${file}.${process.pid}.${randomUUID()}.tmp`;
      try {
        await writeFile(temporary, `${key}\n${JSON.stringify(metadata)}\n`);
        await rename(temporary, file);
      } finally {
        await rm(temporary, { force: true });
      }
      state.cached = { key, cargo };
      return cargo;
    }
    throw new Error('Cargo resolution inputs kept changing during three consecutive locked offline queries');
  } finally {
    lock.unlock();
  }
}

async function resolveCargoMetadata(manifestPath: string, cwd: string): Promise<CargoMetadata> {
  const stdout = await runCargo(
    ['metadata', '--format-version', '1', '--locked', '--offline', '--manifest-path', manifestPath],
    cwd,
  );
  const metadata = parseCargoMetadata(stdout);
  metadata.workspace_root = await realpath(metadata.workspace_root);
  const members = new Set(metadata.workspace_members);
  // Sequential: the metadata flight owns every Cargo process it starts,
  // including the one-time governing-workspace lookups for path dependencies.
  for (const pkg of metadata.packages) {
    if (pkg.source !== null) continue;
    pkg.manifest_path = await realpath(pkg.manifest_path);
    for (const target of pkg.targets) target.src_path = await realpath(target.src_path);
    pkg.governing_manifest_path = members.has(pkg.id)
      ? join(metadata.workspace_root, 'Cargo.toml')
      : await realpath(
          (
            await runCargo(
              ['locate-project', '--workspace', '--manifest-path', pkg.manifest_path, '--message-format', 'plain'],
              cwd,
            )
          ).trim(),
        );
  }
  return metadata;
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
    files.add(pkg.governingManifest);
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
