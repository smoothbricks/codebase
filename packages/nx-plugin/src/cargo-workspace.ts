import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { readFile, realpath } from 'node:fs/promises';
import { dirname, isAbsolute, join, posix, relative, resolve, sep } from 'node:path';
import { readNxJson } from 'nx/src/config/nx-json.js';
import { parse as parseToml, type TomlTable } from 'smol-toml';

import { buildClosure, type CargoResolve, type CompiledCargoInput, compiledInFiles } from './cargo-source-hash.js';

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
  /** Cargo's locked resolve of that workspace, which decides the crates the member's tests build. */
  cargo: CargoResolve;
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
  readonly compiledInFiles: Map<string, CompiledCargoInput[]>;
  readonly results: Map<string, Promise<string[]>>;
}

export function createCargoInputsCache(): CargoInputsCache {
  return { manifests: new Map(), nestedCrateDirs: new Map(), compiledInFiles: new Map(), results: new Map() };
}

export function cargoPackageTestInputs(options: CargoPackageTestInputsOptions): Promise<string[]> {
  const cache = options.cache ?? createCargoInputsCache();
  const key = `${options.absoluteProjectRoot}\0${options.memberDir}`;
  const cached = cache.results.get(key);
  if (cached !== undefined) return cached;
  const derived = deriveCargoPackageTestInputs(options, cache);
  cache.results.set(key, derived);
  return derived;
}

async function deriveCargoPackageTestInputs(
  { workspaceRoot, absoluteProjectRoot, memberDir, cargo }: CargoPackageTestInputsOptions,
  cache: CargoInputsCache,
): Promise<string[]> {
  // Every fileset is anchored at `{workspaceRoot}`, never `{projectRoot}`: Nx
  // hashes a `{projectRoot}` glob against only the files its project OWNS, and
  // a nested Nx project owns the crates under it. A repository-root cargo
  // workspace whose members live in `packages/*` would then hash none of its
  // members' sources, and its archive would cache-hit against edited crates
  // while every per-crate runner reran on the stale binaries.
  const inputRoot = posix.join('{workspaceRoot}', relative(workspaceRoot, absoluteProjectRoot).split(sep).join('/'));
  const loadManifest = (path: string): Promise<TomlTable> => {
    const normalized = resolve(path);
    const cached = cache.manifests.get(normalized);
    if (cached !== undefined) return cached;
    const parsed = readFile(normalized, 'utf-8').then(parseToml);
    cache.manifests.set(normalized, parsed);
    return parsed;
  };
  // Cargo reports canonical paths, so every path below is related to the canonical roots.
  const [nxRoot, cargoRoot, crateDirectory] = await Promise.all([
    realpath(workspaceRoot),
    realpath(absoluteProjectRoot),
    realpath(join(absoluteProjectRoot, memberDir)),
  ]);
  const crate = cargo.local.find((pkg) => pkg.directory === crateDirectory);
  if (crate === undefined) {
    throw new Error(`Cargo's resolve of ${join(absoluteProjectRoot, 'Cargo.toml')} holds no package at ${memberDir}`);
  }
  // Exactly the crates Cargo builds for this crate's tests, read from Cargo's
  // own resolve: the crate's normal, build and dev dependencies, and below it
  // only normal and build ones (`buildClosure`), with the workspace's patches
  // and replacements already applied. The closure, not the direct edge set: a
  // test binary links every crate beneath it, and the chained cargo-test
  // targets order runs without contributing to each other's hash. A
  // dependency's own dev-dependencies never compile into this crate's test
  // binaries, so an edit to one leaves this key alone.
  const closure = buildClosure(cargo, [crate.id]);
  const dirs = new Set<string>();
  const external = new Map<string, string>();
  const declaredSources = new Set<string>();
  for (const pkg of cargo.local) {
    if (!closure.has(pkg.id)) continue;
    if (isOutsideRoot(relative(nxRoot, pkg.directory))) {
      external.set(pkg.name, pkg.directory);
    } else {
      dirs.add(relative(cargoRoot, pkg.directory).split(sep).join('/') || '.');
    }
    // Target sources outside the package's directory (a shared build script, a `[lib] path`), and its governing
    // manifest.
    for (const source of [...pkg.sources, pkg.governingManifest]) {
      if (isOutsideRoot(relative(pkg.directory, source))) declaredSources.add(source);
    }
  }
  const inputPath = (absolutePath: string): string => {
    const cargoRelative = relative(cargoRoot, absolutePath).split(sep).join('/');
    return isOutsideRoot(cargoRelative)
      ? posix.join('{workspaceRoot}', relative(nxRoot, absolutePath).split(sep).join('/'))
      : posix.join(inputRoot, cargoRelative);
  };
  // Cargo.lock is not a fileset here: the runner names the crate's lock-scope
  // node, the lock entries its closure reaches (cargo-closure-input.ts).
  const inputs = new Set<string>([`${inputRoot}/Cargo.toml`]);
  const exclusions = new Set<string>();
  for (const dir of [...dirs].sort()) {
    const absoluteDir = resolve(cargoRoot, dir);
    // Cargo packages can compile include_bytes!, headers, schemas and build.rs,
    // not only Rust sources. Nested packages are separate source owners.
    inputs.add(`${inputPath(absoluteDir)}/**/*`);
    for (const child of nestedCrateDirs(absoluteDir, cache)) {
      const childDir = relative(cargoRoot, child).split(sep).join('/');
      if (!dirs.has(childDir)) exclusions.add(`!${inputPath(child)}/**`);
    }
    // `include_str!("../../../skills/SKILL.md")` compiles a file the package's own
    // fileset does not reach; a target that does not hash it serves a binary
    // built from the file's old bytes.
    for (const { file } of compiledInFiles(absoluteDir, cache.compiledInFiles)) {
      if (isOutsideRoot(relative(absoluteDir, file))) declaredSources.add(file);
    }
  }
  for (const source of [...declaredSources].sort()) {
    const sourceRelative = relative(nxRoot, source);
    if (isOutsideRoot(sourceRelative)) {
      external.set(source, source);
    } else {
      inputs.add(inputPath(source));
    }
  }
  // Cargo/rustup search from the command's working directory upwards. Do not
  // hash unrelated descendant configuration or generated Cargo target caches.
  for (let current = cargoRoot; ; current = dirname(current)) {
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
            if (!isOutsideRoot(relative(nxRoot, targetDir))) {
              exclusions.add(`!${inputPath(targetDir)}/**`);
            }
          }
        }
      }
    }
    if (current === nxRoot || dirname(current) === current) break;
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
    requireExternalRustCratesInput(workspaceRoot, `${memberDir}/Cargo.toml`, external);
    inputs.add(EXTERNAL_RUST_CRATES_INPUT);
  }
  return [...inputs];
}

/**
 * Refuses inputs outside the Nx workspace that `nx.json` gives no `externalRustCrates` runtime hash for: no fileset
 * can hash them, and a key without them would replay a verdict across their edits. `external` maps each input's name
 * to its path.
 */
export function requireExternalRustCratesInput(
  workspaceRoot: string,
  manifest: string,
  external: ReadonlyMap<string, string>,
): void {
  if (readNxJson(workspaceRoot).namedInputs?.[EXTERNAL_RUST_CRATES_INPUT] !== undefined) return;
  const listed = [...external]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([name, dir]) => `${name} -> ${dir}`)
    .join(', ');
  throw new Error(
    `${manifest} references inputs outside the Nx workspace (${listed}) that no fileset can hash; ` +
      `declare namedInputs.${EXTERNAL_RUST_CRATES_INPUT} in ${join(workspaceRoot, 'nx.json')} ` +
      'with runtime inputs that hash those external sources and configuration',
  );
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
 * the suite takes, which target inference cannot measure. Measured time places a
 * crate's slow tests instead of the hash once `.config/nextest-timings.json` names
 * them (nextest-timings.ts). Defaulting to 1 keeps
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
 * The archive is an Nx-restored output, so it lives on the source volume, never
 * beneath Cargo's symlinked target directory (16_build_volumes.md).
 */
export const CARGO_TEST_ARCHIVE_FILE = '.cache/nextest/archive.tar.zst';

/**
 * One foreign target triple this cargo workspace also archives its tests for.
 */
export interface CargoCrossTestTarget {
  target: string;
  /**
   * The repository's own cargo driver for this triple, relative to the cargo
   * workspace root — a program that accepts cargo's argv and links for the
   * target. Exported to the archive command as `CARGO`, which is cargo's own
   * driver variable and the only seam nextest offers: nextest shells out to
   * `$CARGO metadata` and `$CARGO test --no-run --target <triple>` (measured),
   * so a repository whose cross link needs zig, an SDK or a sysroot puts that
   * knowledge in one script instead of in the generated command.
   *
   * Absent means the ambient toolchain already links the triple.
   */
  cargo?: string;
}

/**
 * Foreign target triples this cargo workspace archives tests for, from
 * `[workspace.metadata.smoothbricks.test] cross-targets`. An entry is either
 * the triple as a string, or a table adding that triple's cargo driver:
 *
 * ```toml
 * [workspace.metadata.smoothbricks.test]
 * cross-targets = [{ target = "aarch64-apple-darwin", cargo = "scripts/cargo-for-nextest.sh" }]
 * ```
 *
 * The DECLARATION lives in Cargo.toml rather than in a CI config because a
 * Rust target triple is a cargo fact: the same list decides which archives the
 * graph can produce whether or not anything is running CI, and it is read from
 * the same manifest `shards` comes out of, beside it in the same
 * `smoothbricks.test` table, with the parser this module already uses.
 */
export function listCargoCrossTestTargets(absoluteProjectRoot: string): CargoCrossTestTarget[] {
  const workspaceTomlPath = join(absoluteProjectRoot, 'Cargo.toml');
  if (!existsSync(workspaceTomlPath)) {
    return [];
  }
  const parsed: unknown = parseToml(readFileSync(workspaceTomlPath, 'utf-8'));
  if (!isRecord(parsed) || !isRecord(parsed.workspace)) {
    return [];
  }
  const metadata = isRecord(parsed.workspace.metadata) ? parsed.workspace.metadata : null;
  const smoothbricks = metadata && isRecord(metadata.smoothbricks) ? metadata.smoothbricks : null;
  const test = smoothbricks && isRecord(smoothbricks.test) ? smoothbricks.test : null;
  const declared = test?.['cross-targets'];
  if (declared === undefined) {
    return [];
  }
  if (!Array.isArray(declared)) {
    throw new Error(`${workspaceTomlPath}: smoothbricks.test.cross-targets must be an array of target triples`);
  }
  const crossTargets: CargoCrossTestTarget[] = [];
  for (const entry of declared) {
    const target = typeof entry === 'string' ? entry : isRecord(entry) ? entry.target : undefined;
    // A triple becomes an Nx target NAME and an archive FILE name, so anything
    // a shell or Nx would have to quote is refused where it is declared rather
    // than emitted into a command.
    if (typeof target !== 'string' || !/^[a-z0-9][a-z0-9._-]*$/.test(target)) {
      throw new Error(
        `${workspaceTomlPath}: smoothbricks.test.cross-targets entries must be target triples like ` +
          `"aarch64-apple-darwin", got ${JSON.stringify(entry)}`,
      );
    }
    const cargo = isRecord(entry) ? entry.cargo : undefined;
    // The driver is an input of every archive built through it, so it has to be
    // a path inside this workspace rather than a command line: `{projectRoot}/`
    // plus this string is what makes editing it invalidate the archive.
    if (cargo !== undefined && (typeof cargo !== 'string' || cargo.length === 0 || !/^[\w./-]+$/.test(cargo))) {
      throw new Error(
        `${workspaceTomlPath}: smoothbricks.test.cross-targets ${target} cargo must be a path inside the ` +
          `cargo workspace, got ${JSON.stringify(cargo)}`,
      );
    }
    if (!crossTargets.some((existing) => existing.target === target)) {
      crossTargets.push(cargo === undefined ? { target } : { target, cargo });
    }
  }
  return crossTargets;
}

/**
 * Tests that nextest.toml singles out use this suffix before their shard name.
 * The class is hash-partitioned by the same declared count as the ordinary
 * shards, so it never sits in one run that outgrows the target's bound.
 */
export const CARGO_TEST_EXCEPTIONS_SUFFIX = 'exceptions';

/** Name prefix of the runner a serial test group gets; see `serialTestGroups`. */
export const CARGO_TEST_GROUP_PREFIX = 'cargo-test-group-';

/**
 * A crate on one target keeps the bare name; a split crate suffixes the piece,
 * so `cargo-test-example-core-shard2` still reads as "example-core's tests".
 */
export function cargoTestPackageTargetName(packageName: string, piece?: string): string {
  const base = `cargo-test-${packageName}`;
  const name = piece === undefined ? base : `${base}-${piece}`;
  // A crate named `group-*` would take the name of a serial group's runner.
  if (name.startsWith(CARGO_TEST_GROUP_PREFIX)) {
    throw new Error(`cargo crate ${packageName} would be named ${name}, reserved for a nextest test group runner`);
  }
  return name;
}

/**
 * A serial group's runner. It selects by filter across the whole workspace, so
 * it is no crate: `packageNameFromCargoTestTarget` reads it as none.
 */
export function cargoTestGroupTargetName(group: string): string {
  return `${CARGO_TEST_GROUP_PREFIX}${group}`;
}

/**
 * Inverse of `cargoTestPackageTargetName`, used by the reachability policy to
 * check that the per-crate targets cover every workspace member. Pieces of one
 * crate collapse back to that crate, so a split crate counts as covered once.
 *
 * The workspace-wide `cargo-test-*` targets are not crates: reading them as one
 * would invent members named "compile" and "archive", and a serial group's runner
 * would invent one named after its group. Each reports every crate's coverage
 * against a set that can never match.
 */
export function packageNameFromCargoTestTarget(targetName: string): string | null {
  if (
    !targetName.startsWith('cargo-test-') ||
    targetName === CARGO_TEST_ARCHIVE_TARGET ||
    targetName.startsWith(CARGO_TEST_GROUP_PREFIX)
  ) {
    return null;
  }
  const name = targetName
    .slice('cargo-test-'.length)
    .replace(new RegExp(`-(shard[1-9][0-9]*|${CARGO_TEST_EXCEPTIONS_SUFFIX}-shard[1-9][0-9]*)$`), '');
  return name.length === 0 ? null : name;
}

/**
 * The tests nextest.toml singles out with an override, as one filterset.
 *
 * An override can give a test a bound of its own: a raised slow timeout
 * identifies an expensive test that should not delay an otherwise bounded
 * shard. Partition those tests separately, so every target keeps its time
 * bound and each class runs beside the ordinary shards.
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
  return filterUnion(filters);
}

/** The filters as one filterset that any of them matches, or null for none. */
function filterUnion(filters: readonly string[]): string | null {
  const unique = [...new Set(filters)];
  return unique.length === 0 ? null : unique.map((filter) => `(${filter})`).join(' or ');
}

/**
 * The only `platform` a serial group's overrides may declare: nextest's cfg for macOS. The group
 * runs on macOS hosts only, a host decision made by the planner, never a cfg parse here.
 */
export const SERIAL_TEST_GROUP_PLATFORM = 'cfg(target_os="macos")';

/**
 * Which workspace members a serial group's filter can select tests from: exactly the named
 * packages when every clause of the filter is scoped to one, otherwise every member.
 */
export type SerialTestGroupScope =
  | { readonly kind: 'packages'; readonly packages: readonly string[] }
  | { readonly kind: 'workspace' };

/**
 * One nextest test group capped at a single thread, and the tests its overrides
 * assign it.
 */
export interface SerialTestGroup {
  /** The group's name, as `[test-groups]` declares it. */
  group: string;
  /** Every default-profile override filter that names the group, as one filterset. */
  filter: string;
  /** The members the filter can select from, which are the members whose sources its runner hashes. */
  scope: SerialTestGroupScope;
}

/**
 * The repository's test groups capped at one thread (`max-threads = 1`), each
 * with the tests its default-profile overrides assign it.
 *
 * A serial group is declared exactly once, in `.config/nextest.toml`: the name
 * under `[test-groups]` plus the overrides that name it. Its runner's filter is
 * derived from those overrides rather than restated, so the runner and the
 * per-crate exclusions cannot drift from what nextest actually serializes.
 *
 * Each group also carries the `scope` of its filter (`filterScope`): the members its runner hashes.
 * A group whose every clause is scoped to a `package(<name>)` reruns only when those crates or their
 * local dependencies change; any other group is scoped to the whole workspace. Nothing here orders one
 * group against another: they are separate runners, and a repository that needs two never concurrent
 * declares the edge between their targets itself.
 *
 * Every override naming a serial group must declare `SERIAL_TEST_GROUP_PLATFORM`.
 * This plugin has no cfg evaluator, so it honours exactly that platform and
 * refuses any other rather than guessing where the group applies.
 *
 * Returns [] when the file is absent or declares no serial group, so a workspace
 * without one keeps exactly its existing targets.
 */
export function serialTestGroups(nextestConfigPath: string): SerialTestGroup[] {
  if (!existsSync(nextestConfigPath)) {
    return [];
  }
  const parsed: unknown = parseToml(readFileSync(nextestConfigPath, 'utf-8'));
  if (!isRecord(parsed)) {
    return [];
  }
  const declaredValue = parsed['test-groups'];
  const declaredGroups: Record<string, unknown> = isRecord(declaredValue) ? declaredValue : {};
  const serial = Object.keys(declaredGroups)
    .filter((name) => {
      const declared = declaredGroups[name];
      return isRecord(declared) && declared['max-threads'] === 1;
    })
    .sort((left, right) => left.localeCompare(right));
  const profiles: Record<string, unknown> = isRecord(parsed.profile) ? parsed.profile : {};
  const defaults: Record<string, unknown> = isRecord(profiles.default) ? profiles.default : {};
  const overridesValue: unknown = defaults.overrides;
  const overrides: unknown[] = Array.isArray(overridesValue) ? overridesValue : [];
  return serial.flatMap((group) => {
    const filters: string[] = [];
    for (const override of overrides) {
      if (!isRecord(override) || override['test-group'] !== group) {
        continue;
      }
      if (override.platform !== SERIAL_TEST_GROUP_PLATFORM) {
        throw new Error(
          `${nextestConfigPath}: test-group "${group}" overrides must declare platform = '${SERIAL_TEST_GROUP_PLATFORM}' ` +
            `(the only platform a serial group runs on here), got ${JSON.stringify(override.platform ?? null)}`,
        );
      }
      if (typeof override.filter === 'string' && override.filter.length > 0) {
        filters.push(override.filter);
      }
    }
    const filter = filterUnion(filters);
    return filter === null ? [] : [{ group, filter, scope: filterScope(filter) }];
  });
}

/**
 * The packages `filter` can select tests from, read off its `package(<name>)` predicates. The scope is
 * never narrower than the selection: whatever this does not read with certainty is the whole workspace.
 *
 * The filter is read as nextest reads it, with nextest's precedence — parentheses, then `not` and `!`,
 * then `and`, `&` and `-`, then `or`, `|` and `+`, each level left to right — and with nextest's blanks,
 * which are spaces and line endings. Each expression is bounded by the packages it can select from:
 *
 * - `package(<name>)` and `package(=<name>)`, for a plain Cargo name, bound it to that package. A glob,
 *   `~contains`, `/regex/` or `#glob` name can match any package, so it bounds nothing.
 * - Every other predicate bounds nothing, and nor does `not`: `not package(a)` selects from every other
 *   package, and `deps` and `rdeps` reach beyond the package they name.
 * - `a and b` selects only what both select, so it is bounded by the narrower of the two bounds, and
 *   `a - b`, which is `a and not b`, by `a`. `a or b` is bounded by both together, so one unbounded
 *   clause leaves the union unbounded however the union is spelled.
 *
 * A predicate's argument ends where nextest ends it: a regex at its closing `/`, any other matcher at its
 * first `)`. Only arguments that need none of nextest's escape sequences are read: a backslash, or a comma
 * outside a regex, makes where the predicate ends depend on escapes this does not implement. Those, a
 * filter that does not parse, and one whose packages exclude each other have no bound.
 */
export function filterScope(filter: string): SerialTestGroupScope {
  const read = readUnion(filter, 0);
  if (read === null || skipBlank(filter, read.end) !== filter.length || read.bound === 'any' || read.bound.size === 0) {
    return { kind: 'workspace' };
  }
  return { kind: 'packages', packages: [...read.bound].sort((left, right) => left.localeCompare(right)) };
}

/** What a filterset can select from: only the named packages, or `'any'` when no package bounds it. */
type PackageBound = ReadonlySet<string> | 'any';

/** A filterset prefix that has been read: the packages it can select from, and the offset just past it. */
interface ReadFilter {
  readonly bound: PackageBound;
  readonly end: number;
}

/** nextest's loosest-binding level: `or`, `|` and `+` between chains of `and`. */
function readUnion(text: string, from: number): ReadFilter | null {
  let left = readIntersection(text, from);
  while (left !== null) {
    const operatorEnd = skipUnionOperator(text, left.end);
    if (operatorEnd === null) return left;
    const right = readIntersection(text, operatorEnd);
    if (right === null) return null;
    const bound = left.bound === 'any' || right.bound === 'any' ? 'any' : new Set([...left.bound, ...right.bound]);
    left = { bound, end: right.end };
  }
  return null;
}

/** A chain of `and`, `&` and `-`, which bind tighter than a union, left to right. */
function readIntersection(text: string, from: number): ReadFilter | null {
  let left = readOperand(text, from);
  while (left !== null) {
    const operator = readIntersectionOperator(text, left.end);
    if (operator === null) return left;
    const right = readOperand(text, operator.end);
    left =
      right === null
        ? null
        : { bound: operator.difference ? left.bound : boundIntersection(left.bound, right.bound), end: right.end };
  }
  return null;
}

/** A negation, a parenthesised filterset, or one predicate. */
function readOperand(text: string, from: number): ReadFilter | null {
  const at = skipBlank(text, from);
  const word = readWord(text, at, 'not');
  const negated = word ?? (text.charAt(at) === '!' ? at + 1 : null);
  if (negated !== null) {
    const operand = readOperand(text, negated);
    return operand === null ? null : { bound: 'any', end: operand.end };
  }
  if (text.charAt(at) !== '(') return readPredicate(text, at);
  const inner = readUnion(text, at + 1);
  if (inner === null) return null;
  const close = skipBlank(text, inner.end);
  return text.charAt(close) === ')' ? { bound: inner.bound, end: close + 1 } : null;
}

/** What follows a predicate's `(`: nothing, or a name matcher. */
const PREDICATE_ARGUMENTS: Record<string, 'none' | 'matcher'> = {
  all: 'none',
  none: 'none',
  default: 'none',
  test: 'matcher',
  group: 'matcher',
  package: 'matcher',
  deps: 'matcher',
  rdeps: 'matcher',
  binary_id: 'matcher',
  kind: 'matcher',
  binary: 'matcher',
  platform: 'matcher',
};

/** One predicate: `all()` and its kin, or a name followed by its parenthesised matcher. */
function readPredicate(text: string, at: number): ReadFilter | null {
  const identifier = /[a-z_]+/y;
  identifier.lastIndex = at;
  const predicate = identifier.exec(text)?.[0];
  if (predicate === undefined) return null;
  const open = skipBlank(text, at + predicate.length);
  if (text.charAt(open) !== '(') return null;
  const argument = Object.hasOwn(PREDICATE_ARGUMENTS, predicate) ? PREDICATE_ARGUMENTS[predicate] : undefined;
  if (argument === undefined) return null;
  if (argument === 'none') {
    const close = skipBlank(text, open + 1);
    return text.charAt(close) === ')' ? { bound: 'any', end: close + 1 } : null;
  }
  const matcher = readMatcher(text, open + 1);
  return matcher === null
    ? null
    : { bound: predicate === 'package' ? packageBound(matcher.text) : 'any', end: matcher.end };
}

/**
 * The argument of a predicate whose `(` ends just before `from`, as written between its parentheses, and
 * the offset past its `)`; null when it cannot be delimited exactly as nextest delimits it.
 */
function readMatcher(text: string, from: number): { readonly text: string; readonly end: number } | null {
  const start = skipBlank(text, from);
  const close = text.charAt(start) === '/' ? regexMatcherClose(text, start) : plainMatcherClose(text, start);
  return close === null ? null : { text: text.slice(from, close), end: close + 1 };
}

/** The `)` that follows the `/regex/` starting at `start`, when the regex holds no backslash. */
function regexMatcherClose(text: string, start: number): number | null {
  const slash = text.indexOf('/', start + 1);
  if (slash < 0 || text.slice(start, slash).includes('\\')) return null;
  const close = skipBlank(text, slash + 1);
  return text.charAt(close) === ')' ? close : null;
}

/** The first `)` at or after `start`, when nothing before it is a backslash or a comma. */
function plainMatcherClose(text: string, start: number): number | null {
  const close = text.indexOf(')', start);
  return close < 0 || /[,\\]/.test(text.slice(start, close)) ? null : close;
}

/** A plain Cargo package name, with or without nextest's `=` prefix: nothing a glob can widen. */
const PLAIN_PACKAGE_NAME = /^=?([A-Za-z0-9_-]+)$/;

function packageBound(matcher: string): PackageBound {
  const name = PLAIN_PACKAGE_NAME.exec(matcher)?.[1];
  return name === undefined ? 'any' : new Set([name]);
}

function boundIntersection(left: PackageBound, right: PackageBound): PackageBound {
  if (left === 'any') return right;
  if (right === 'any') return left;
  return new Set([...left].filter((name) => right.has(name)));
}

/** The offset of the first character at or after `from` that nextest does not read as blank. */
function skipBlank(text: string, from: number): number {
  const blank = /(?: |\r?\n)*/y;
  blank.lastIndex = from;
  return from + (blank.exec(text)?.[0].length ?? 0);
}

/** `word` at `at`, read as nextest reads an operator word: not the start of a longer identifier. */
function readWord(text: string, at: number, word: string): number | null {
  const end = at + word.length;
  return text.startsWith(word, at) && !/[A-Za-z0-9_]/.test(text.charAt(end)) ? end : null;
}

/** The offset past the `or`, `|` or `+` that follows `from` after any blanks, or null. */
function skipUnionOperator(text: string, from: number): number | null {
  const at = skipBlank(text, from);
  const character = text.charAt(at);
  return readWord(text, at, 'or') ?? (character === '|' || character === '+' ? at + 1 : null);
}

/** The `and`, `&` or `-` that follows `from` after any blanks: where it ends, and whether it subtracts. */
function readIntersectionOperator(
  text: string,
  from: number,
): { readonly end: number; readonly difference: boolean } | null {
  const at = skipBlank(text, from);
  const word = readWord(text, at, 'and');
  if (word !== null) return { end: word, difference: false };
  const character = text.charAt(at);
  if (character === '&') return { end: at + 1, difference: false };
  return character === '-' ? { end: at + 1, difference: true } : null;
}

/** The plugin's nextest.toml as the workspace root's `node_modules` links it. */
export const PLUGIN_NEXTEST_CONFIG_PATH = 'node_modules/@smoothbricks/nx-plugin/nextest.toml';

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
 *
 * The file is named through the workspace root's `node_modules` link, as
 * tool package CLIs are (tool-bins.ts), never through where the plugin really lives: a
 * plugin linked from a checkout outside the workspace sits a different number
 * of `../` away from each workspace depending on how deep that workspace is,
 * and that count re-keyed every target of every Cargo project — and every
 * dependent task — in each workspace that sat at a new depth.
 *
 * `projectRoot` is the Cargo workspace's root relative to the Nx workspace
 * root, which is the directory these commands run in.
 */
export function nextestToolConfigArg(projectRoot: string): string {
  const path = posix.join(posix.relative(projectRoot, '.'), PLUGIN_NEXTEST_CONFIG_PATH);
  return `--tool-config-file "smoo:$PWD/${path}"`;
}

/** The repository's own nextest config, which now layers over the plugin's. */
export const NEXTEST_REPO_CONFIG_PATH = '.config/nextest.toml';
