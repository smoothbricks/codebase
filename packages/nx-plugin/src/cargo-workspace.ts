import { existsSync, readFileSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { join, normalize, posix, relative, sep } from 'node:path';
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

export function listCargoWorkspacePackages(absoluteProjectRoot: string): CargoWorkspacePackage[] {
  const workspaceTomlPath = join(absoluteProjectRoot, 'Cargo.toml');
  if (!existsSync(workspaceTomlPath)) {
    return [];
  }
  const parsed: unknown = parseToml(readFileSync(workspaceTomlPath, 'utf-8'));
  if (!isRecord(parsed) || !isRecord(parsed.workspace) || !Array.isArray(parsed.workspace.members)) {
    return [];
  }
  const packages: CargoWorkspacePackage[] = [];
  for (const member of parsed.workspace.members) {
    if (typeof member !== 'string' || member.length === 0) {
      continue;
    }
    const crateTomlPath = join(absoluteProjectRoot, member, 'Cargo.toml');
    if (!existsSync(crateTomlPath)) {
      continue;
    }
    const crateParsed: unknown = parseToml(readFileSync(crateTomlPath, 'utf-8'));
    if (!isRecord(crateParsed) || !isRecord(crateParsed.package) || typeof crateParsed.package.name !== 'string') {
      continue;
    }
    packages.push({
      name: crateParsed.package.name,
      dir: member.split('\\').join('/'),
      testShards: readTestShards(crateParsed.package, crateTomlPath),
    });
  }
  packages.sort((left, right) => left.name.localeCompare(right.name));
  return packages;
}

export async function cargoPackageTestInputs(absoluteProjectRoot: string, memberDir: string): Promise<string[]> {
  const workspacePathDeps = await workspacePathDependencies(absoluteProjectRoot);
  const dirs = new Set<string>([memberDir.split('\\').join('/')]);
  const crateParsed: unknown = parseToml(await readFile(join(absoluteProjectRoot, memberDir, 'Cargo.toml'), 'utf-8'));
  if (isRecord(crateParsed)) {
    for (const tableName of ['dependencies', 'dev-dependencies', 'build-dependencies'] as const) {
      const table = crateParsed[tableName];
      if (!isRecord(table)) {
        continue;
      }
      for (const [depName, spec] of Object.entries(table)) {
        const pathDep = pathDependencyDir(memberDir, depName, spec, workspacePathDeps);
        if (pathDep !== null) {
          dirs.add(pathDep);
        }
      }
    }
  }
  const inputs: string[] = ['{projectRoot}/Cargo.toml', '{projectRoot}/Cargo.lock'];
  for (const dir of [...dirs].sort()) {
    inputs.push(`{projectRoot}/${dir}/**/*.rs`, `{projectRoot}/${dir}/Cargo.toml`);
  }
  inputs.push('{projectRoot}/**/.cargo/config.toml', '{projectRoot}/scripts/*.sh', '!{projectRoot}/**/target/**');
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

function pathDependencyDir(
  memberDir: string,
  depName: string,
  spec: unknown,
  workspacePathDeps: Map<string, string>,
): string | null {
  if (spec === undefined || spec === null) {
    return null;
  }
  if (typeof spec === 'string') {
    return null;
  }
  if (!isRecord(spec)) {
    return null;
  }
  if (spec.workspace === true) {
    return workspacePathDeps.get(depName) ?? null;
  }
  if (typeof spec.path !== 'string') {
    return null;
  }
  const resolved = normalize(join(memberDir, spec.path));
  if (resolved.startsWith('..')) {
    return null;
  }
  return resolved.split(sep).join(posix.sep);
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
