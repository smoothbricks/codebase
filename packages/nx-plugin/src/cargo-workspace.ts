import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { isAbsolute, join, normalize, posix, relative, sep } from 'node:path';
import { readNxJson } from 'nx/src/config/nx-json.js';
import { parse as parseToml } from 'smol-toml';

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
  if (!isRecord(parsed) || !isRecord(parsed.workspace) || !Array.isArray(parsed.workspace.members)) {
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
  for (const member of parsed.workspace.members) {
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
  for (const memberDir of packageDirs) {
    const crateTomlPath = join(absoluteProjectRoot, memberDir, 'Cargo.toml');
    if (!existsSync(crateTomlPath)) {
      continue;
    }
    const crateParsed: unknown = parseToml(readFileSync(crateTomlPath, 'utf-8'));
    if (!isRecord(crateParsed) || !isRecord(crateParsed.package) || typeof crateParsed.package.name !== 'string') {
      continue;
    }
    packages.push({
      name: crateParsed.package.name,
      dir: memberDir,
      testShards: readTestShards(crateParsed.package, crateTomlPath),
    });
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
}

export async function cargoPackageTestInputs({
  workspaceRoot,
  absoluteProjectRoot,
  memberDir,
  inputRoot = '{projectRoot}',
}: CargoPackageTestInputsOptions): Promise<string[]> {
  const workspacePathDeps = await workspacePathDependencies(absoluteProjectRoot);
  const dirs = new Set<string>([memberDir.split('\\').join('/')]);
  const external = new Map<string, string>();
  const crateParsed: unknown = parseToml(await readFile(join(absoluteProjectRoot, memberDir, 'Cargo.toml'), 'utf-8'));
  if (isRecord(crateParsed)) {
    for (const tableName of ['dependencies', 'dev-dependencies', 'build-dependencies'] as const) {
      const table = crateParsed[tableName];
      if (!isRecord(table)) {
        continue;
      }
      for (const [depName, spec] of Object.entries(table)) {
        const pathDep = pathDependencyDir(memberDir, depName, spec, workspacePathDeps);
        if (pathDep === null) {
          continue;
        }
        if (pathDep.external) {
          external.set(depName, pathDep.dir);
        } else {
          dirs.add(pathDep.dir);
        }
      }
    }
  }
  const inputs: string[] = [`${inputRoot}/Cargo.toml`, `${inputRoot}/Cargo.lock`];
  for (const dir of [...dirs].sort()) {
    inputs.push(`${inputRoot}/${dir}/**/*.rs`, `${inputRoot}/${dir}/Cargo.toml`);
  }
  inputs.push(`${inputRoot}/**/.cargo/config.toml`, `${inputRoot}/scripts/*.sh`, `!${inputRoot}/**/target/**`);
  if (external.size > 0) {
    if (readNxJson(workspaceRoot).namedInputs?.[EXTERNAL_RUST_CRATES_INPUT] === undefined) {
      const listed = [...external]
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([name, dir]) => `${name} -> ${dir}`)
        .join(', ');
      throw new Error(
        `${memberDir}/Cargo.toml depends on crates outside the Nx workspace (${listed}) that no fileset can hash; ` +
          `declare namedInputs.${EXTERNAL_RUST_CRATES_INPUT} in ${join(workspaceRoot, 'nx.json')} as a runtime input ` +
          'that hashes every *.rs and Cargo.toml under those trees, and the per-crate cargo test targets will list it',
      );
    }
    inputs.push(EXTERNAL_RUST_CRATES_INPUT);
  }
  return inputs;
}

async function workspacePathDependencies(absoluteProjectRoot: string): Promise<Map<string, string>> {
  const parsed: unknown = parseToml(await readFile(join(absoluteProjectRoot, 'Cargo.toml'), 'utf-8'));
  const deps = new Map<string, string>();
  if (!isRecord(parsed) || !isRecord(parsed.workspace) || !isRecord(parsed.workspace.dependencies)) {
    return deps;
  }
  for (const [name, spec] of Object.entries(parsed.workspace.dependencies)) {
    if (isRecord(spec) && typeof spec.path === 'string') {
      deps.set(name, spec.path.split('\\').join('/'));
    }
  }
  return deps;
}

interface PathDependencyDir {
  /** Cargo-workspace-relative directory, or the raw path when external. */
  dir: string;
  /** Resolves above the cargo workspace root or to an absolute path. */
  external: boolean;
}

/**
 * Where a path dependency lives relative to the cargo workspace root. A
 * `workspace = true` dependency is already root-relative; a crate-local
 * `path` is relative to the member. Anything that escapes the root is
 * reported as external rather than dropped, so the caller can demand the
 * runtime input that covers it.
 */
function pathDependencyDir(
  memberDir: string,
  depName: string,
  spec: unknown,
  workspacePathDeps: Map<string, string>,
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
    raw = posix.join(memberDir.split('\\').join('/'), spec.path.split('\\').join('/'));
  } else {
    return null;
  }
  if (isAbsolute(raw) || posix.isAbsolute(raw)) {
    return { dir: raw, external: true };
  }
  const resolved = normalize(raw).split(sep).join(posix.sep);
  if (resolved === '..' || resolved.startsWith('../')) {
    return { dir: resolved, external: true };
  }
  return { dir: resolved, external: false };
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
 * small crates on a single target: cowshed-cli's 226 tests run in 6.5s, so
 * sharding them would buy nothing and pay a per-target cargo freshness check.
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
 * Tests that nextest.toml singles out are pinned to this suffix instead of
 * being sharded. Only a sharded crate has one: an unsharded crate runs its
 * whole suite in a single nextest process, which is all the pin restores.
 */
export const CARGO_TEST_EXCEPTIONS_SUFFIX = 'exceptions';

/**
 * A crate on one target keeps the bare name; a split crate suffixes the piece,
 * so `cargo-test-cowshed-core-shard2` still reads as "cowshed-core's tests".
 */
export function cargoTestPackageTargetName(packageName: string, piece?: string): string {
  const base = `cargo-test-${packageName}`;
  return piece === undefined ? base : `${base}-${piece}`;
}

/**
 * Inverse of `cargoTestPackageTargetName`, used by the reachability policy to
 * check that the per-crate targets cover every workspace member. Pieces of one
 * crate collapse back to that crate, so a split crate counts as covered once.
 */
export function packageNameFromCargoTestTarget(targetName: string): string | null {
  if (!targetName.startsWith('cargo-test-') || targetName === CARGO_TEST_COMPILE_TARGET) {
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
 * An override is the config author saying "this class does not behave like the
 * rest of the suite", and both classes declared today break a shard, each in a
 * different way:
 *
 *   - a `test-group` is scoped to a single nextest RUN, so letting the hash
 *     scatter its members across shards silently dissolves it. The real-APFS
 *     group exists because those tests contend on Disk Arbitration
 *     machine-wide, where an unscoped `diskutil apfs list -plist` measured 0.7s
 *     idle against 14.4s while another process is attaching.
 *   - a raised `slow-timeout` marks a test whose cost is not the suite's cost.
 *     `lesser_capabilities_fail_to_compile_...` rustc's a fixture that
 *     `cargo test --no-run` cannot pre-build, so it costs 25.6s on a cold
 *     target directory against 1.8s warm. Every CI runner is cold, and left in
 *     a shard that spike lands on whichever 250-odd tests share it.
 *
 * One target for all of them rather than one each: they sit in different groups
 * so they run concurrently, making the combined wall the max of the classes
 * rather than their sum.
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

export function nextestConfigRelPath(workspaceRoot: string, projectRoot: string, configAbs: string): string {
  const rel = relative(join(workspaceRoot, projectRoot), configAbs);
  return rel.length === 0 ? configAbs : rel.split(sep).join('/');
}
