import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdtemp, readdir, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { ProjectGraph } from 'nx/src/config/project-graph';
import { createTaskGraph } from 'nx/src/tasks-runner/create-task-graph.js';
import { parse as parseToml } from 'smol-toml';
import typia from 'typia';
import {
  type CargoWorkspacePackage,
  cargoPackageTestInputs,
  listCargoWorkspacePackages,
  NEXTEST_REPO_CONFIG_PATH,
  SERIAL_TEST_GROUP_PLATFORM,
  serialTestGroups,
} from './cargo-workspace.js';
import { COWSHED_EXECUTABLE_ENV } from './executors/bounded-exec/ram-temp.js';
import { isRecord } from './type-guards.js';

/** The repository this package is built in: its own nextest config, nx.json and manifests are what these pin. */
const REPOSITORY = fileURLToPath(new URL('../../..', import.meta.url));
const ROOT_PROJECT = '@smoothbricks/codebase';
const DEVICE_GROUP = 'cargo-test-group-real-apfs';
const HOST_GROUP = 'cargo-test-group-real-apfs-host';
/** cowshed's build, which leaves the host's own CLI where the package's launcher finds it. */
const CHECKOUT_COWSHED_BUILD = { projects: ['cowshed'], target: 'build' };

describe('the real-APFS serial groups of this repository', () => {
  it('are exactly the device group and the host group, each scoped to the packages it holds', () => {
    const groups = serialTestGroups(join(REPOSITORY, NEXTEST_REPO_CONFIG_PATH));
    // A third group is a decision about ordering against these two, test-host and the explicit tests below.
    expect(groups.map(({ group, scope }) => ({ group, scope }))).toEqual([
      { group: 'real-apfs', scope: { kind: 'packages', packages: ['cowshed-apfs-tests'] } },
      { group: 'real-apfs-host', scope: { kind: 'packages', packages: ['cowshed-cli', 'cowshed-core'] } },
    ]);
  });

  it('hash a scenario edit for the device group alone, and the production substrate they share for both', async () => {
    const groups = serialTestGroups(join(REPOSITORY, NEXTEST_REPO_CONFIG_PATH));
    const members = listCargoWorkspacePackages(REPOSITORY);
    // The inputs Nx hashes for a group's runner: the closure of every package its filter names, as the plugin derives it.
    const inputsOf = async (group: string): Promise<string[]> => {
      const scope = groups.find((candidate) => candidate.group === group)?.scope;
      if (scope?.kind !== 'packages') throw new Error(`${group} is not scoped to packages`);
      const hashed = await Promise.all(
        scope.packages.map((name) => {
          const member = members.find((candidate) => candidate.name === name);
          if (member === undefined) throw new Error(`${name} is not a member of the Cargo workspace`);
          return cargoPackageTestInputs({
            workspaceRoot: REPOSITORY,
            absoluteProjectRoot: REPOSITORY,
            memberDir: member.dir,
          });
        }),
      );
      return hashed.flat();
    };
    const device = await inputsOf('real-apfs');
    const host = await inputsOf('real-apfs-host');
    const reruns = (path: string) => ({ device: hashes(device, path), host: hashes(host, path) });
    const crates = 'packages/cowshed/crates';

    // The three cases. An unrelated crate reruns neither group. A scenario edit reruns the device group alone,
    // because no host crate depends on the crate the scenarios live in. A change to the production substrate, or to
    // what it links, reruns both, because the host crates link it too.
    expect(reruns('packages/columine/crates/columine-vm/src/lib.rs')).toEqual({ device: false, host: false });
    expect(reruns(`${crates}/cowshed-apfs-tests/src/tests.rs`)).toEqual({ device: true, host: false });
    expect(reruns(`${crates}/cowshed-apfs/src/lib.rs`)).toEqual({ device: true, host: true });
    expect(reruns(`${crates}/cowshed-os/src/lib.rs`)).toEqual({ device: true, host: true });
    expect(reruns(`${crates}/cowshed-core/src/lib.rs`)).toEqual({ device: false, host: true });
    expect(reruns(`${crates}/cowshed-cli/src/main.rs`)).toEqual({ device: false, host: true });
    // The test support every real-image binary compiles in stays hashed for both, and no tests tree is excluded.
    for (const support of [
      `${crates}/cowshed-apfs/tests/support/scratch_apfs.rs`,
      `${crates}/cowshed-os/tests/support/scratch_processes.rs`,
    ]) {
      expect(reruns(support), support).toEqual({ device: true, host: true });
    }
    for (const inputs of [device, host]) {
      expect(inputs.filter((input) => input.startsWith('!') && /\btests?\b/.test(input))).toEqual([]);
    }
  }, 60_000);

  it('run the host group after the device group, so two real-image runners never overlap', async () => {
    const nx = await readRecord('nx.json', JSON.parse);
    const host = child(child(nx, 'targetDefaults'), HOST_GROUP);
    // `...` keeps the inferred archive and fetch edges ahead of the explicit ones.
    expect(host.dependsOn).toEqual(['...', CHECKOUT_COWSHED_BUILD, DEVICE_GROUP]);
  });

  it("run this checkout's cowshed, built first, for every disk command the code under test makes", async () => {
    const nx = await readRecord('nx.json', JSON.parse);
    const plugin = await readRecord('packages/nx-plugin/package.json', JSON.parse);
    // The groups and the RAM-volume host tests exercise the disk-command facade. An installed cowshed that predates a
    // command this checkout added would fail them, so each names the checkout's launcher, after cowshed's build.
    const targets = [
      child(child(nx, 'targetDefaults'), DEVICE_GROUP),
      child(child(nx, 'targetDefaults'), HOST_GROUP),
      child(child(child(plugin, 'nx'), 'targets'), 'test-host'),
    ];
    for (const target of targets) {
      expect(target.dependsOn).toContainEqual(CHECKOUT_COWSHED_BUILD);
      expect(child(child(target, 'options'), 'env')).toEqual({
        [COWSHED_EXECUTABLE_ENV]: '{workspaceRoot}/packages/cowshed/bin/cowshed',
      });
    }
  });

  it("are reached by no target of this repository's real task graph but the host group's edge to the device group", async () => {
    const graph = await repositoryProjectGraph();
    const projects = Object.keys(graph.nodes);
    const targets = [...new Set(projects.flatMap((name) => Object.keys(graph.nodes[name]?.data.targets ?? {})))];
    // Every task of every project, as one run-many of every target would plan it.
    const tasks = createTaskGraph(graph, {}, projects, targets, undefined, {});
    const device = `${ROOT_PROJECT}:${DEVICE_GROUP}`;
    const host = `${ROOT_PROJECT}:${HOST_GROUP}`;
    // package.json declares both targets even off macOS. Their explicit dependency edges remain,
    // but only macOS inference supplies a runner: a placeholder is not an executable APFS test.
    expect(Object.keys(tasks.tasks)).toEqual(expect.arrayContaining([device, host]));
    for (const name of [DEVICE_GROUP, HOST_GROUP]) {
      const target = graph.nodes[ROOT_PROJECT]?.data.targets?.[name];
      expect(target, name).toBeDefined();
      if (process.platform === 'darwin') {
        expect(target?.executor, name).toBe('@smoothbricks/nx-plugin:bounded-exec');
        expect(target?.options?.command, name).toContain('nextest run');
      } else {
        expect(target?.executor, name).toBeUndefined();
        expect(target?.options?.command, name).toBeUndefined();
      }
    }
    const edges = (id: string): string[] => [
      ...(tasks.dependencies[id] ?? []),
      ...(tasks.continuousDependencies?.[id] ?? []),
    ];
    const groupsReached: Record<string, string[]> = {};
    for (const id of Object.keys(tasks.tasks)) {
      const seen = new Set<string>();
      const pending = edges(id);
      for (let next = pending.pop(); next !== undefined; next = pending.pop()) {
        if (seen.has(next)) continue;
        seen.add(next);
        pending.push(...edges(next));
      }
      const groups = [...seen].filter((reached) => reached === device || reached === host);
      if (groups.length > 0) groupsReached[id] = groups;
    }
    // A real-image group runs only as its own target, named by the gate, and only when its own inputs changed: an
    // aggregate that reached one pulled 21 minutes of real-image tests into a plain `-t test`.
    // nx.json orders the explicitly declared host target after the device target on every host.
    // No other target may reach either, including when these nodes have no executable runner.
    expect(groupsReached).toEqual({ [host]: [device] });
  }, 120_000);

  it('wrap exactly their tests in the 30s code-bound class, behind the 60s class that wins first', async () => {
    const config = await readRecord(NEXTEST_REPO_CONFIG_PATH, parseToml);
    const profile = child(child(config, 'profile'), 'default');
    const overrides = records(profile.overrides);
    const scripts = records(profile.scripts);
    const grouped = overrides.filter(
      (override) => override['test-group'] === 'real-apfs' || override['test-group'] === 'real-apfs-host',
    );
    const thirtySecond = scripts.filter((script) => script['run-wrapper'] === 'real-apfs-code-30s');

    // A config file has no `group()` predicate, so the 30s rules repeat the groups' filters verbatim, one for one:
    // together they are exactly the tests the groups hold, and no ordinary test is bound to a real-image class.
    expect(thirtySecond.map((script) => script.filter).sort()).toEqual(
      grouped.map((override) => override.filter).sort(),
    );
    expect(scripts.map((script) => script['run-wrapper'])).toEqual([
      'real-apfs-code-60s',
      ...grouped.map(() => 'real-apfs-code-30s'),
    ]);
    // Everything here, groups and wrappers alike, applies on the one platform a serial group runs on.
    for (const rule of [...grouped, ...scripts]) {
      expect(rule.platform, JSON.stringify(rule.filter)).toBe(SERIAL_TEST_GROUP_PLATFORM);
    }
  });

  it('hold every real_apfs_ test: none sits in a package no group is scoped to', async () => {
    const groups = serialTestGroups(join(REPOSITORY, NEXTEST_REPO_CONFIG_PATH));
    const scoped = new Set(groups.flatMap(({ scope }) => (scope.kind === 'packages' ? scope.packages : [])));
    const owners = new Set<string>();
    for (const source of await rustSources(listCargoWorkspacePackages(REPOSITORY))) {
      if (/\bfn real_apfs_\w+/.test(source.text)) owners.add(source.owner);
    }
    // Scoping a group to packages made this a rule to keep: a `real_apfs_` test in any other crate would run in an
    // ordinary shard beside every other test, not in a serial group.
    expect([...owners].filter((name) => !scoped.has(name))).toEqual([]);
    expect([...owners].sort()).toEqual(['cowshed-apfs-tests', 'cowshed-cli', 'cowshed-core']);
  }, 60_000);

  it('pin each exact test by a function that exists at that module path', async () => {
    const groups = serialTestGroups(join(REPOSITORY, NEXTEST_REPO_CONFIG_PATH));
    const scoped = new Set(groups.flatMap(({ scope }) => (scope.kind === 'packages' ? scope.packages : [])));
    const sources = await rustSources(listCargoWorkspacePackages(REPOSITORY), scoped);
    // A group with no exact-name clause pins nothing, so there is nothing that can go stale.
    const pinned = groups.flatMap(({ filter }) => [...filter.matchAll(/test\(=([^),]+)\)/g)].map((match) => match[1]));
    for (const exactName of pinned) {
      // `a::b::tests::name`: a module path, the `tests` module, then the function. A name that moved with its
      // module matches no test and silently leaves the group, so each must still resolve to one function.
      const segments = (exactName ?? '').split('::');
      const name = segments[segments.length - 1];
      const modulePath = segments.slice(0, -2).join('/');
      const holders = sources.filter(({ text }) => text.includes(`fn ${name}(`));
      expect(holders, exactName).toHaveLength(1);
      expect(holders[0]?.path, exactName).toMatch(new RegExp(`/src/${modulePath}(?:/mod)?\\.rs$`));
      expect(holders[0]?.text, exactName).toContain('mod tests');
    }
  }, 60_000);
});

/** `path` of the repository, parsed by `parse`, which must be an object. */
async function readRecord(path: string, parse: (text: string) => unknown): Promise<Record<string, unknown>> {
  const parsed = parse(await readFile(join(REPOSITORY, path), 'utf8'));
  if (!isRecord(parsed)) throw new Error(`${path} is not an object`);
  return parsed;
}

function child(parent: Record<string, unknown>, key: string): Record<string, unknown> {
  const value = parent[key];
  if (!isRecord(value)) throw new Error(`${key} is not an object`);
  return value;
}

function records(value: unknown): Record<string, unknown>[] {
  if (!Array.isArray(value)) throw new Error('expected an array of tables');
  return value.map((entry) => {
    if (!isRecord(entry)) throw new Error('expected a table');
    return entry;
  });
}

const parseProjectGraph = typia.json.createAssertParse<ProjectGraph>();
// The child computes the graph outside this package's bunfig; it takes the same transform preload as the test runner.
const VALIDATION_PRELOAD = fileURLToPath(import.meta.resolve('@smoothbricks/validation/bun/preload'));
const GRAPH_SCRIPT = `const { createProjectGraphAsync } = await import('nx/src/project-graph/project-graph.js');
const graph = await createProjectGraphAsync({ exitOnError: false });
process.stdout.write(JSON.stringify({ nodes: graph.nodes, dependencies: graph.dependencies }));`;

/**
 * The repository's own project graph, as Nx builds it with this plugin: computed daemonless, in a child that exits when
 * it is done, with plugins in its own process and workspace data of its own, so it neither starts a daemon nor writes
 * the checkout's graph cache.
 */
async function repositoryProjectGraph(): Promise<ProjectGraph> {
  const data = await mkdtemp(join(tmpdir(), 'serial-group-graph-'));
  try {
    const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith('NX_')));
    const run = spawnSync(process.execPath, ['--preload', VALIDATION_PRELOAD, '-e', GRAPH_SCRIPT], {
      cwd: REPOSITORY,
      encoding: 'utf8',
      maxBuffer: 256 * 1024 * 1024,
      env: {
        ...env,
        NX_DAEMON: 'false',
        NX_ISOLATE_PLUGINS: 'false',
        NX_CACHE_PROJECT_GRAPH: 'false',
        NX_WORKSPACE_DATA_DIRECTORY: data,
      },
    });
    if (run.status !== 0) throw new Error(`the project graph child exited ${run.status}: ${run.stderr}`);
    return parseProjectGraph(run.stdout);
  } finally {
    await rm(data, { recursive: true, force: true });
  }
}

interface RustSource {
  /** The Cargo package whose tree holds the file. */
  readonly owner: string;
  /** The file, relative to the repository. */
  readonly path: string;
  readonly text: string;
}

/**
 * Every Rust source of `members` (of only the packages `only` names, when given), each credited to the deepest
 * member whose directory holds it: a crate nested inside another is its own owner's.
 */
async function rustSources(
  members: readonly CargoWorkspacePackage[],
  only?: ReadonlySet<string>,
): Promise<RustSource[]> {
  const sources: RustSource[] = [];
  for (const member of members.filter(({ name }) => only?.has(name) ?? true)) {
    const nested = members
      .filter(({ dir }) => dir.startsWith(`${member.dir}/`))
      .map(({ dir }) => dir.slice(member.dir.length + 1));
    const files = (await readdir(join(REPOSITORY, member.dir), { recursive: true })).filter(
      (file) =>
        file.endsWith('.rs') &&
        !file.split(sep).some((part) => part === 'target' || part === 'node_modules') &&
        !nested.some((dir) => file.startsWith(`${dir}${sep}`)),
    );
    const read = await Promise.all(
      files.map(async (file) => ({
        owner: member.name,
        path: join(member.dir, file),
        text: await readFile(join(REPOSITORY, member.dir, file), 'utf8'),
      })),
    );
    sources.push(...read);
  }
  return sources;
}

/** Whether Nx would hash `path` (workspace-relative) under the `{workspaceRoot}` filesets of `inputs`. */
function hashes(inputs: readonly string[], path: string): boolean {
  const filesets = inputs.filter((input) => input.includes('{workspaceRoot}'));
  const matches = (fileset: string): boolean => new Bun.Glob(fileset.replace('{workspaceRoot}/', '')).match(path);
  return (
    filesets.some((input) => !input.startsWith('!') && matches(input)) &&
    !filesets.some((input) => input.startsWith('!') && matches(input.slice(1)))
  );
}
