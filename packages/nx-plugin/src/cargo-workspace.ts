import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { dirname, isAbsolute, join, normalize, posix, relative, resolve, sep } from 'node:path';
import { readNxJson } from 'nx/src/config/nx-json.js';
import { parse as parseToml, type TomlTable } from 'smol-toml';

export interface CargoWorkspacePackage {
  name: string;
  dir: string;
  /**
   * How many bounded Nx targets this crate's tests are split across. See
   * `readTestShards` for why this is declared per crate rather than inferred.
   */
  testShards: number;
}

export interface CargoWorkspaceProject {
  name: string;
  root: string;
}

export interface AttributedCargoWorkspacePackage extends CargoWorkspacePackage {
  projectName: string;
  projectRoot: string;
}

export function listCargoWorkspacePackages(absoluteProjectRoot: string): CargoWorkspacePackage[] {
  const workspaceTomlPath = join(absoluteProjectRoot, 'Cargo.toml');
  if (!existsSync(workspaceTomlPath)) {
    return [];
  }
  const parsed: unknown = parseToml(readFileSync(workspaceTomlPath, 'utf-8'));
  if (!isRecord(parsed) || !isRecord(parsed.workspace)) {
    return [];
  }
  const excludedDirs = new Set<string>();
  if (Array.isArray(parsed.workspace.exclude)) {
    for (const excluded of parsed.workspace.exclude) {
      if (typeof excluded === 'string' && excluded.length > 0) {
        for (const excludedDir of expandCargoMemberDirs(absoluteProjectRoot, excluded, false)) {
          excludedDirs.add(excludedDir);
        }
      }
    }
  }
  const packageDirs = new Set<string>();
  if (isRecord(parsed.package)) packageDirs.add('.');
  const members = Array.isArray(parsed.workspace.members) ? parsed.workspace.members : [];
  for (const member of members) {
    if (typeof member !== 'string' || member.length === 0) {
      continue;
    }
    for (const memberDir of expandCargoMemberDirs(absoluteProjectRoot, member)) {
      if (!isExcludedCargoMember(memberDir, excludedDirs)) {
        packageDirs.add(memberDir);
      }
    }
  }
  const packages: CargoWorkspacePackage[] = [];
  const workspacePathDeps = workspacePathDependencies(parsed, absoluteProjectRoot);
  const external = new Map<string, string>();
  for (const memberDir of packageDirs) {
    const crateTomlPath = join(absoluteProjectRoot, memberDir, 'Cargo.toml');
    if (!existsSync(crateTomlPath)) {
      continue;
    }
    const crateParsed: unknown = memberDir === '.' ? parsed : parseToml(readFileSync(crateTomlPath, 'utf-8'));
    if (!isRecord(crateParsed) || !isRecord(crateParsed.package) || typeof crateParsed.package.name !== 'string') {
      continue;
    }
    packages.push({
      name: crateParsed.package.name,
      dir: memberDir,
      testShards: readTestShards(crateParsed.package, crateTomlPath),
    });
    // Cargo automatically includes in-workspace path dependencies as members,
    // except explicitly excluded packages.
    const dependencies: string[] = [];
    enqueuePathDependencies(
      crateParsed,
      memberDir,
      workspacePathDeps,
      dependencies,
      external,
      absoluteProjectRoot,
      absoluteProjectRoot,
    );
    if (isRecord(crateParsed.target)) {
      for (const target of Object.values(crateParsed.target)) {
        if (isRecord(target)) {
          enqueuePathDependencies(
            target,
            memberDir,
            workspacePathDeps,
            dependencies,
            external,
            absoluteProjectRoot,
            absoluteProjectRoot,
          );
        }
      }
    }
    for (const dependency of dependencies) {
      if (!isExcludedCargoMember(dependency, excludedDirs)) packageDirs.add(dependency);
    }
  }
  packages.sort((left, right) => left.name.localeCompare(right.name));
  return packages;
}

function isExcludedCargoMember(memberDir: string, excludedDirs: ReadonlySet<string>): boolean {
  for (const excludedDir of excludedDirs) {
    if (memberDir === excludedDir || memberDir.startsWith(`${excludedDir}/`)) {
      return true;
    }
  }
  return false;
}

/**
 * Cargo accepts glob patterns at any depth in `workspace.members` and
 * `workspace.exclude`. Walk one path segment at a time so patterns with
 * wildcards in multiple segments cannot silently erase every crate from the
 * Nx graph.
 */
function expandCargoMemberDirs(absoluteProjectRoot: string, member: string, requireGlobMatch = true): string[] {
  const normalized = member.split('\\').join('/').replace(/\/+$/, '');
  if (normalized === '.') return ['.'];
  const segments = normalized.split('/');
  if (
    normalized.length === 0 ||
    normalized.startsWith('/') ||
    segments.some((segment) => segment.length === 0 || segment === '.' || segment === '..')
  ) {
    throw new Error(`Cargo workspace member must be a non-empty relative path: ${member}`);
  }
  if (segments.some((segment) => segment === '**' || /[[\]{}!]/.test(segment))) {
    throw new Error(`Cargo workspace member uses an unsupported glob pattern: ${member}`);
  }

  const hasGlob = segments.some((segment) => segment.includes('*') || segment.includes('?'));
  let candidates = [''];
  for (const segment of segments) {
    if (!segment.includes('*') && !segment.includes('?')) {
      candidates = candidates.map((candidate) => posix.join(candidate, segment));
      continue;
    }
    const matcher = cargoGlobSegmentMatcher(segment);
    const expanded: string[] = [];
    for (const candidate of candidates) {
      const absoluteParent = join(absoluteProjectRoot, candidate);
      if (!existsSync(absoluteParent)) {
        continue;
      }
      for (const entry of readdirSync(absoluteParent, { withFileTypes: true })) {
        if (entry.isDirectory() && matcher.test(entry.name)) {
          expanded.push(posix.join(candidate, entry.name));
        }
      }
    }
    candidates = expanded;
  }
  if (requireGlobMatch && hasGlob && candidates.length === 0) {
    throw new Error(`Cargo workspace member glob matched no directories: ${member}`);
  }
  return candidates.sort((left, right) => left.localeCompare(right));
}

function cargoGlobSegmentMatcher(segment: string): RegExp {
  let source = '^';
  for (const character of segment) {
    if (character === '*') {
      source += '.*';
    } else if (character === '?') {
      source += '.';
    } else {
      source += character.replace(/[\\^$.*+?()[\]{}|]/g, '\\$&');
    }
  }
  return new RegExp(`${source}$`);
}

/**
 * Assign each workspace crate to the most specific Nx project that contains
 * its directory. The repository root is the fallback owner; a nested package
 * wins by path depth so projects under `packages/*` own their own crates.
 */
export function attributeCargoWorkspacePackages(
  packages: readonly CargoWorkspacePackage[],
  projects: readonly CargoWorkspaceProject[],
): AttributedCargoWorkspacePackage[] {
  const bySpecificity = [...projects].sort((left, right) => {
    const leftDepth = left.root === '.' ? 0 : left.root.split('/').length;
    const rightDepth = right.root === '.' ? 0 : right.root.split('/').length;
    return rightDepth - leftDepth || left.name.localeCompare(right.name);
  });
  const attributed: AttributedCargoWorkspacePackage[] = [];
  for (const pkg of packages) {
    const owner = bySpecificity.find(
      (project) => project.root === '.' || pkg.dir === project.root || pkg.dir.startsWith(`${project.root}/`),
    );
    if (owner) {
      attributed.push({ ...pkg, projectName: owner.name, projectRoot: owner.root });
    }
  }
  return attributed;
}

/**
 * The named input a workspace must declare when a crate's path dependencies
 * resolve outside the Nx workspace. Nx hashes filesets against the workspace
 * file map only, so a glob pointing above the root — or an absolute path
 * spliced under `{workspaceRoot}` — matches nothing and every cached verdict
 * silently ignores the dependency. A runtime named input can hash any tree.
 */
export const EXTERNAL_RUST_CRATES_INPUT = 'externalRustCrates';

export interface CargoPackageTestInputsOptions {
  /** The Nx workspace root, where `nx.json` declares named inputs. */
  workspaceRoot: string;
  /** The cargo workspace root holding the top-level `Cargo.toml`. */
  absoluteProjectRoot: string;
  memberDir: string;
  inputRoot?: string;
  /** Shared across every call of one graph computation; see {@link createCargoInputsCache}. */
  cache?: CargoInputsCache;
}

/**
 * Memo for one project-graph computation. Every crate's compile, lint and
 * test targets derive the same closure, and every crate re-reads the same
 * manifests and re-walks the same trees for nested `Cargo.toml` files; with
 * three targets over ~45 crates that was ~135 derivations and the dominant
 * cost of loading the graph. The cache lives exactly as long as one
 * `createNodes` call — Nx recomputes the graph when a manifest changes, so
 * nothing here can go stale across runs.
 */
export interface CargoInputsCache {
  readonly manifests: Map<string, Promise<TomlTable>>;
  readonly nestedCrateDirs: Map<string, string[]>;
  readonly results: Map<string, Promise<string[]>>;
}

export function createCargoInputsCache(): CargoInputsCache {
  return { manifests: new Map(), nestedCrateDirs: new Map(), results: new Map() };
}

export function cargoPackageTestInputs(options: CargoPackageTestInputsOptions): Promise<string[]> {
  const cache = options.cache ?? createCargoInputsCache();
  const key = `${options.absoluteProjectRoot}\0${options.memberDir}\0${options.inputRoot ?? '{projectRoot}'}`;
  const cached = cache.results.get(key);
  if (cached !== undefined) return cached;
  const derived = deriveCargoPackageTestInputs(options, cache);
  cache.results.set(key, derived);
  return derived;
}

async function deriveCargoPackageTestInputs(
  { workspaceRoot, absoluteProjectRoot, memberDir, inputRoot = '{projectRoot}' }: CargoPackageTestInputsOptions,
  cache: CargoInputsCache,
): Promise<string[]> {
  const loadManifest = (path: string): Promise<TomlTable> => {
    const normalized = resolve(path);
    const cached = cache.manifests.get(normalized);
    if (cached !== undefined) return cached;
    const parsed = readFile(normalized, 'utf-8').then(parseToml);
    cache.manifests.set(normalized, parsed);
    return parsed;
  };
  const rootManifest = await loadManifest(join(absoluteProjectRoot, 'Cargo.toml'));
  // The closure over in-tree path dependencies, not the direct edge set: a
  // test binary links every crate beneath it, and the chained cargo-test
  // targets order runs without contributing to each other's hash, so a
  // dependency reachable only through another crate would otherwise change
  // nothing in the target that links it.
  const dirs = new Set<string>();
  const external = new Map<string, string>();
  const pending = [normalize(memberDir).split(sep).join('/')];
  const declaredSources = new Set<string>();
  // Workspace patches/replacements can substitute local code for locked sources.
  if (isRecord(rootManifest.patch)) {
    for (const replacements of Object.values(rootManifest.patch)) {
      enqueuePathDependencies(
        { dependencies: replacements },
        '.',
        new Map(),
        pending,
        external,
        absoluteProjectRoot,
        workspaceRoot,
      );
    }
  }
  if (isRecord(rootManifest.replace)) {
    enqueuePathDependencies(
      { dependencies: rootManifest.replace },
      '.',
      new Map(),
      pending,
      external,
      absoluteProjectRoot,
      workspaceRoot,
    );
  }
  while (pending.length > 0) {
    const dir = pending.pop();
    if (dir === undefined || dirs.has(dir)) {
      continue;
    }
    dirs.add(dir);
    const crateTomlPath = join(absoluteProjectRoot, dir, 'Cargo.toml');
    if (!existsSync(crateTomlPath)) {
      continue;
    }
    const crateParsed = await loadManifest(crateTomlPath);
    if (!isRecord(crateParsed)) {
      continue;
    }
    let owner = resolve(absoluteProjectRoot, dir);
    if (isRecord(crateParsed.package) && typeof crateParsed.package.workspace === 'string') {
      owner = resolve(owner, crateParsed.package.workspace);
      if (isOutsideRoot(relative(workspaceRoot, owner))) {
        external.set(`${dir} workspace`, owner);
      }
    }
    let workspacePathDeps = new Map<string, string>();
    while (!isOutsideRoot(relative(workspaceRoot, owner))) {
      const manifestPath = join(owner, 'Cargo.toml');
      if (existsSync(manifestPath)) {
        const manifest = await loadManifest(manifestPath);
        if (isRecord(manifest.workspace)) {
          workspacePathDeps = workspacePathDependencies(manifest, owner);
          declaredSources.add(manifestPath);
          break;
        }
      }
      const parent = dirname(owner);
      if (parent === owner) break;
      owner = parent;
    }
    enqueuePathDependencies(crateParsed, dir, workspacePathDeps, pending, external, absoluteProjectRoot, workspaceRoot);
    // All cfg variants can affect a compiled target; do not evaluate Cargo cfg here.
    if (isRecord(crateParsed.target)) {
      for (const target of Object.values(crateParsed.target)) {
        if (isRecord(target)) {
          enqueuePathDependencies(
            target,
            dir,
            workspacePathDeps,
            pending,
            external,
            absoluteProjectRoot,
            workspaceRoot,
          );
        }
      }
    }
    if (isRecord(crateParsed.package) && typeof crateParsed.package.build === 'string') {
      declaredSources.add(resolve(absoluteProjectRoot, dir, crateParsed.package.build));
    }
    for (const targetName of ['lib', 'bin', 'example', 'test', 'bench']) {
      const targets = crateParsed[targetName];
      for (const target of Array.isArray(targets) ? targets : [targets]) {
        if (isRecord(target) && typeof target.path === 'string') {
          declaredSources.add(resolve(absoluteProjectRoot, dir, target.path));
        }
      }
    }
  }
  const inputPath = (absolutePath: string): string => {
    const cargoRelative = relative(absoluteProjectRoot, absolutePath).split(sep).join('/');
    return isOutsideRoot(cargoRelative)
      ? posix.join('{workspaceRoot}', relative(workspaceRoot, absolutePath).split(sep).join('/'))
      : posix.join(inputRoot, cargoRelative);
  };
  const inputs = new Set<string>([`${inputRoot}/Cargo.toml`, `${inputRoot}/Cargo.lock`]);
  const exclusions = new Set<string>();
  for (const dir of [...dirs].sort()) {
    const absoluteDir = resolve(absoluteProjectRoot, dir);
    // Cargo packages can compile include_bytes!, headers, schemas and build.rs,
    // not only Rust sources. Nested packages are separate source owners.
    inputs.add(`${inputPath(absoluteDir)}/**/*`);
    for (const child of nestedCrateDirs(absoluteDir, cache)) {
      const childDir = relative(absoluteProjectRoot, child).split(sep).join('/');
      if (!dirs.has(childDir)) exclusions.add(`!${inputPath(child)}/**`);
    }
  }
  for (const source of [...declaredSources].sort()) {
    const sourceRelative = relative(workspaceRoot, source);
    if (isOutsideRoot(sourceRelative)) {
      external.set(source, source);
    } else {
      inputs.add(inputPath(source));
    }
  }
  // Cargo/rustup search from the command's working directory upwards. Do not
  // hash unrelated descendant configuration or generated Cargo target caches.
  for (let current = resolve(absoluteProjectRoot); ; current = dirname(current)) {
    for (const config of [
      '.cargo/config',
      '.cargo/config.toml',
      'rust-toolchain',
      'rust-toolchain.toml',
      'rustfmt.toml',
      '.rustfmt.toml',
      'clippy.toml',
      '.clippy.toml',
    ]) {
      inputs.add(inputPath(join(current, config)));
      if (config === '.cargo/config' || config === '.cargo/config.toml') {
        const configPath = join(current, config);
        if (existsSync(configPath)) {
          const cargoConfig = await loadManifest(configPath);
          if (isRecord(cargoConfig.build) && typeof cargoConfig.build['target-dir'] === 'string') {
            const targetDir = resolve(current, cargoConfig.build['target-dir']);
            if (!isOutsideRoot(relative(workspaceRoot, targetDir))) {
              exclusions.add(`!${inputPath(targetDir)}/**`);
            }
          }
        }
      }
    }
    if (current === resolve(workspaceRoot) || dirname(current) === current) break;
  }
  inputs.add(`${inputRoot}/.config/nextest.toml`);
  for (const generated of ['.git', 'node_modules', '.nx']) {
    inputs.add(`!{workspaceRoot}/**/${generated}/**`);
  }
  inputs.add(`!${inputRoot}/**/target/**`);
  inputs.add('!{workspaceRoot}/**/target/**');
  for (const exclusion of [...exclusions].sort()) inputs.add(exclusion);
  // Arbitrary build-script reads outside package trees, custom output trees,
  // and ambient tools/environment require explicit target or named inputs.
  if (external.size > 0) {
    if (readNxJson(workspaceRoot).namedInputs?.[EXTERNAL_RUST_CRATES_INPUT] === undefined) {
      const listed = [...external]
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([name, dir]) => `${name} -> ${dir}`)
        .join(', ');
      throw new Error(
        `${memberDir}/Cargo.toml references inputs outside the Nx workspace (${listed}) that no fileset can hash; ` +
          `declare namedInputs.${EXTERNAL_RUST_CRATES_INPUT} in ${join(workspaceRoot, 'nx.json')} ` +
          'with runtime inputs that hash those external sources and configuration',
      );
    }
    inputs.add(EXTERNAL_RUST_CRATES_INPUT);
  }
  return [...inputs];
}

/** Every directory beneath `root` that holds its own `Cargo.toml`, the walk stopping at each. */
function nestedCrateDirs(root: string, cache: CargoInputsCache): string[] {
  const cached = cache.nestedCrateDirs.get(root);
  if (cached !== undefined) return cached;
  const found: string[] = [];
  const scan = [root];
  while (scan.length > 0) {
    const current = scan.pop();
    if (current === undefined || !existsSync(current)) continue;
    for (const entry of readdirSync(current, { withFileTypes: true })) {
      if (!entry.isDirectory()) continue;
      const child = join(current, entry.name);
      if (['target', '.git', 'node_modules', '.nx'].includes(entry.name)) {
      } else if (existsSync(join(child, 'Cargo.toml'))) {
        found.push(child);
      } else {
        scan.push(child);
      }
    }
  }
  cache.nestedCrateDirs.set(root, found);
  return found;
}

function enqueuePathDependencies(
  scope: Record<string, unknown>,
  memberDir: string,
  workspacePathDeps: Map<string, string>,
  pending: string[],
  external: Map<string, string>,
  absoluteProjectRoot: string,
  workspaceRoot: string,
): void {
  for (const tableName of ['dependencies', 'dev-dependencies', 'build-dependencies'] as const) {
    const table = scope[tableName];
    if (!isRecord(table)) {
      continue;
    }
    for (const [depName, spec] of Object.entries(table)) {
      const pathDep = pathDependencyDir(
        memberDir,
        depName,
        spec,
        workspacePathDeps,
        absoluteProjectRoot,
        workspaceRoot,
      );
      if (pathDep === null) {
        continue;
      }
      if (pathDep.external) {
        external.set(depName, pathDep.dir);
      } else {
        pending.push(pathDep.dir);
      }
    }
  }
}

function workspacePathDependencies(parsed: unknown, root: string): Map<string, string> {
  const deps = new Map<string, string>();
  if (!isRecord(parsed) || !isRecord(parsed.workspace) || !isRecord(parsed.workspace.dependencies)) {
    return deps;
  }
  for (const [name, spec] of Object.entries(parsed.workspace.dependencies)) {
    if (isRecord(spec) && typeof spec.path === 'string') {
      const path = spec.path.split('\\').join('/');
      deps.set(name, resolve(root, path));
    }
  }
  return deps;
}

interface PathDependencyDir {
  /** Cargo-workspace-relative directory, or the raw path when external. */
  dir: string;
  /** Resolves outside the Nx workspace root, regardless of spelling. */
  external: boolean;
}

/**
 * Resolve inherited dependencies from their owning workspace and local paths
 * from their member directory. Paths inside Nx remain in the ordinary source
 * closure even when they escape the Cargo workspace; paths outside Nx require
 * an explicit runtime named input.
 */
function pathDependencyDir(
  memberDir: string,
  depName: string,
  spec: unknown,
  workspacePathDeps: Map<string, string>,
  absoluteProjectRoot: string,
  workspaceRoot: string,
): PathDependencyDir | null {
  if (!isRecord(spec)) {
    return null;
  }
  let raw: string;
  if (spec.workspace === true) {
    const workspacePath = workspacePathDeps.get(depName);
    if (workspacePath === undefined) {
      return null;
    }
    raw = workspacePath;
  } else if (typeof spec.path === 'string') {
    const localPath = spec.path.split('\\').join('/');
    raw =
      isAbsolute(localPath) || posix.isAbsolute(localPath)
        ? localPath
        : posix.join(memberDir.split('\\').join('/'), localPath);
  } else {
    return null;
  }
  const absolute = resolve(absoluteProjectRoot, raw);
  const local = relative(absoluteProjectRoot, absolute).split(sep).join(posix.sep) || '.';
  return { dir: local, external: isOutsideRoot(relative(workspaceRoot, absolute)) };
}

function isOutsideRoot(path: string): boolean {
  return path === '..' || path.startsWith(`..${sep}`) || isAbsolute(path);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * `[package.metadata.smoothbricks.test] shards = N` splits a crate's tests
 * across N bounded targets, each running `--partition hash:i/N`.
 *
 * The COUNT is declared and the MEMBERSHIP is derived: nextest hashes each test
 * name into a shard, so a new test — or a whole new test binary — is absorbed
 * with no edit here and no list to forget to update. A stale count can only
 * make a target slow, never make a test disappear, because the shards remain an
 * exact partition of whatever the crate currently contains.
 *
 * It is declared rather than inferred because the only honest input is how long
 * the suite takes, which target inference cannot measure. Defaulting to 1 keeps
 * small crates on a single target without paying another Cargo freshness check.
 */
function readTestShards(cratePackage: Record<string, unknown>, crateTomlPath: string): number {
  const metadata = isRecord(cratePackage.metadata) ? cratePackage.metadata : null;
  const smoothbricks = metadata && isRecord(metadata.smoothbricks) ? metadata.smoothbricks : null;
  const test = smoothbricks && isRecord(smoothbricks.test) ? smoothbricks.test : null;
  const shards = test?.shards;
  if (shards === undefined) {
    return 1;
  }
  if (typeof shards !== 'number' || !Number.isInteger(shards) || shards < 1) {
    throw new Error(`${crateTomlPath}: smoothbricks.test.shards must be an integer >= 1, got ${String(shards)}`);
  }
  return shards;
}

/**
 * The aggregate that actually runs a cargo workspace's tests. Named here because
 * both target inference and the policy that verifies `test` reaches it need the
 * same string, and a drifted copy would make the policy vacuous.
 */
export const CARGO_TEST_TARGET = 'cargo-test';

/** Prerequisite that compiles the workspace; not a per-crate runner. */
export const CARGO_TEST_COMPILE_TARGET = 'cargo-test-compile';

/**
 * The one workspace-wide `cargo nextest archive` every per-crate runner
 * executes from. Named here beside the other cargo target names so the policy
 * that checks a workspace's test graph and the inference that builds it read
 * the same string.
 */
export const CARGO_TEST_ARCHIVE_TARGET = 'cargo-test-archive';

/**
 * Where that archive lands, relative to the cargo workspace root — the cwd both
 * the archive command and every runner already use. `cargo nextest archive`
 * does NOT create this parent directory and fails the whole build when it is
 * missing ("error writing to archive ... No such file or directory"), so the
 * target mkdir's it first.
 */
export const CARGO_TEST_ARCHIVE_FILE = 'target/nextest/archive.tar.zst';

/**
 * Tests that nextest.toml singles out are pinned to this suffix instead of
 * being sharded. Only a sharded crate has one: an unsharded crate runs its
 * whole suite in a single nextest process, which is all the pin restores.
 */
export const CARGO_TEST_EXCEPTIONS_SUFFIX = 'exceptions';

/**
 * A crate on one target keeps the bare name; a split crate suffixes the piece,
 * so `cargo-test-example-core-shard2` still reads as "example-core's tests".
 */
export function cargoTestPackageTargetName(packageName: string, piece?: string): string {
  const base = `cargo-test-${packageName}`;
  return piece === undefined ? base : `${base}-${piece}`;
}

/**
 * Inverse of `cargoTestPackageTargetName`, used by the reachability policy to
 * check that the per-crate targets cover every workspace member. Pieces of one
 * crate collapse back to that crate, so a split crate counts as covered once.
 *
 * The two workspace-wide `cargo-test-*` targets are not crates: reading them as
 * one would invent members named "compile" and "archive" and report every real
 * crate's coverage against a set that can never match.
 */
export function packageNameFromCargoTestTarget(targetName: string): string | null {
  if (
    !targetName.startsWith('cargo-test-') ||
    targetName === CARGO_TEST_COMPILE_TARGET ||
    targetName === CARGO_TEST_ARCHIVE_TARGET
  ) {
    return null;
  }
  const name = targetName
    .slice('cargo-test-'.length)
    .replace(new RegExp(`-(shard[1-9][0-9]*|${CARGO_TEST_EXCEPTIONS_SUFFIX})$`), '');
  return name.length === 0 ? null : name;
}

/**
 * The tests nextest.toml singles out with an override, as one filterset.
 *
 * Overrides can encode constraints that must survive sharding: a test group
 * limits concurrency within one nextest run, while a raised slow timeout can
 * identify expensive tests that should not delay an otherwise bounded shard.
 * Keep the configured exceptions in one run so group limits remain effective.
 *
 * Deriving this from the config that declares the classes, rather than
 * restating their filters, means adding an override there is the whole change —
 * the pin follows and cannot drift from what it protects.
 *
 * Returns null when nothing is singled out, the "nothing to pin" case.
 */
export function exceptionalTestFilter(nextestConfigPath: string): string | null {
  const parsed: unknown = parseToml(readFileSync(nextestConfigPath, 'utf-8'));
  if (!isRecord(parsed)) {
    return null;
  }
  const profiles = isRecord(parsed.profile) ? parsed.profile : {};
  const filters: string[] = [];
  for (const profile of Object.values(profiles)) {
    const overrides = isRecord(profile) ? profile.overrides : undefined;
    if (!Array.isArray(overrides)) {
      continue;
    }
    for (const override of overrides) {
      if (isRecord(override) && typeof override.filter === 'string' && override.filter.length > 0) {
        filters.push(override.filter);
      }
    }
  }
  const unique = [...new Set(filters)];
  return unique.length === 0 ? null : unique.map((filter) => `(${filter})`).join(' or ');
}

/**
 * The plugin's own nextest settings, as a config layer BENEATH the repository's.
 *
 * `--config-file` replaces `<workspace>/.config/nextest.toml` outright, which
 * made every plugin default unoverridable and — now that per-crate runs execute
 * from an archive — left a repository no way to declare `archive.include` for a
 * cdylib or fixture its tests need. `--tool-config-file` is nextest's mechanism
 * for exactly this: "lower than --config-file in priority but above the default
 * config shipped with nextest". Measured: with only this layer a 1s
 * slow-timeout terminates a 3s test; adding `.config/nextest.toml` with 10s
 * lets the same test pass.
 *
 * The path must be absolute, and an absolute path written into the command text
 * would differ per checkout and split one Nx cache entry per machine. `$PWD`
 * resolves at exec time — both the archive (`nx:run-commands`) and the runners
 * (`bounded-exec`, which spawns with `shell: true`) go through a shell — so the
 * command text stays identical everywhere. Quoted, because a workspace path may
 * contain spaces.
 */
export function nextestToolConfigArg(workspaceRoot: string, projectRoot: string, configAbs: string): string {
  const rel = relative(join(workspaceRoot, projectRoot), configAbs);
  const path = rel.length === 0 ? configAbs : rel.split(sep).join('/');
  return `--tool-config-file "smoo:$PWD/${path}"`;
}

/** The repository's own nextest config, which now layers over the plugin's. */
export const NEXTEST_REPO_CONFIG_PATH = '.config/nextest.toml';
