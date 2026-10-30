import { describe, expect, it } from 'bun:test';
import { readdir, readFile } from 'node:fs/promises';
import { join, sep } from 'node:path';
import type { ProjectGraph } from 'nx/src/config/project-graph';
import { createTaskGraph } from 'nx/src/tasks-runner/create-task-graph.js';
import { parse as parseToml } from 'smol-toml';
import typia from 'typia';
import { REPOSITORY, runInRepositoryNx } from './__tests__/repository-nx.js';
import { readCargoResolve } from './cargo-source-hash.js';
import {
  type CargoWorkspacePackage,
  cargoPackageTestInputs,
  listCargoWorkspacePackages,
  NEXTEST_REPO_CONFIG_PATH,
  packageNameFromCargoTestTarget,
  SERIAL_TEST_GROUP_PLATFORM,
  serialTestGroups,
} from './cargo-workspace.js';
import { COWSHED_EXECUTABLE_ENV } from './executors/bounded-exec/owned-subtree.js';
import { isRecord } from './type-guards.js';

const ROOT_PROJECT = '@smoothbricks/codebase';
const DEVICE_GROUP = 'cargo-test-group-real-apfs';
const HOST_GROUP = 'cargo-test-group-real-apfs-host';
/** cowshed's host proof of the built addon (`openHost` and the typed launchers), which needs a Mac. */
const HOST_NAPI = 'host-napi-test';
/** The host CLI producer, which leaves the development binary where the checkout's launcher finds it. */
const CHECKOUT_COWSHED_BUILD = { projects: ['cowshed'], target: 'cli-native' };
/** The target that provisions the per-user mount root the real-image fixtures keep their scratch trees below. */
const APFS_TEST_ROOT = { projects: ['cowshed'], target: 'apfs-test-root' };
const APFS_TEST_ROOT_TASK = 'cowshed:apfs-test-root';

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
    const cargo = await readCargoResolve(join(REPOSITORY, 'Cargo.toml'), REPOSITORY);
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
            cargo,
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
  });

  it('run the host group after the device group, so two real-image runners never overlap', async () => {
    const nx = await readRecord('nx.json', JSON.parse);
    const host = child(child(nx, 'targetDefaults'), HOST_GROUP);
    // `...` keeps the inferred archive and fetch edges ahead of the explicit ones.
    expect(host.dependsOn).toEqual(['...', CHECKOUT_COWSHED_BUILD, APFS_TEST_ROOT, DEVICE_GROUP]);
  });

  it("run this checkout's cowshed, built first, for every disk command the code under test makes", async () => {
    const nx = await readRecord('nx.json', JSON.parse);
    const plugin = await readRecord('packages/nx-plugin/package.json', JSON.parse);
    // The groups and the RAM-volume host tests exercise the disk-command facade. An installed cowshed that predates a
    // command this checkout added would fail them, so each names the checkout's launcher after its host CLI producer.
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

  it('reaches the host proofs only through the explicit host and land gates, adding them to the old land task set', async () => {
    const graph = await projectGraph();
    const projects = Object.keys(graph.nodes);
    const targets = [...new Set(projects.flatMap((name) => Object.keys(graph.nodes[name]?.data.targets ?? {})))];
    const tasks = createTaskGraph(graph, {}, projects, targets, undefined, {});
    const device = `${ROOT_PROJECT}:${DEVICE_GROUP}`;
    const host = `${ROOT_PROJECT}:${HOST_GROUP}`;
    const napi = `cowshed:${HOST_NAPI}`;
    const hostGate = `${ROOT_PROJECT}:host-e2e`;
    const gate = `${ROOT_PROJECT}:gate`;
    // Package-declared nodes and explicit edges exist on every host; only macOS inference supplies a group runner.
    // cowshed declares its host-napi-test whole, so every host plans that task too.
    expect(Object.keys(tasks.tasks)).toEqual(expect.arrayContaining([device, host, napi, hostGate, gate]));
    for (const name of [DEVICE_GROUP, HOST_GROUP]) {
      const target = graph.nodes[ROOT_PROJECT]?.data.targets?.[name];
      expect(target, name).toBeDefined();
      if (process.platform === 'darwin') {
        expect(target?.executor, name).toBe('@smoothbricks/nx-plugin:bounded-exec');
        expect(target?.options?.command, name).toContain('nextest run');
      } else {
        // Nx normalizes the package-declared placeholder to a no-op, never an APFS runner.
        expect(target?.executor, name).toBe('nx:noop');
        expect(target?.options?.command, name).toBeUndefined();
      }
    }
    const edges = (id: string): string[] => [
      ...(tasks.dependencies[id] ?? []),
      ...(tasks.continuousDependencies?.[id] ?? []),
    ];
    const proofsReached: Record<string, string[]> = {};
    for (const id of Object.keys(tasks.tasks)) {
      const seen = new Set<string>();
      const pending = edges(id);
      for (let next = pending.pop(); next !== undefined; next = pending.pop()) {
        if (seen.has(next)) continue;
        seen.add(next);
        pending.push(...edges(next));
      }
      const proofs = [...seen].filter((reached) => reached === device || reached === host || reached === napi).sort();
      if (proofs.length > 0) proofsReached[id] = proofs;
    }
    // Ordinary tests must not pull expensive images, or a proof that needs a Mac, into the inner loop. Only these
    // explicit aggregates may.
    expect(proofsReached).toEqual({ [host]: [device], [hostGate]: [device, host, napi], [gate]: [device, host, napi] });
    const planned = (selected: string[]): string[] => {
      const runnable = projects.filter((name) =>
        selected.some((target) => graph.nodes[name]?.data.targets?.[target] !== undefined),
      );
      return Object.keys(createTaskGraph(graph, {}, runnable, selected, undefined, {}).tasks).sort();
    };
    const landSet = planned(['lint', 'test', 'build', DEVICE_GROUP, HOST_GROUP, HOST_NAPI]);
    expect(planned(['gate']).filter((id) => id !== gate && id !== hostGate)).toEqual(landSet);
    expect(planned(['host-e2e']).filter((id) => id !== hostGate)).toEqual(
      planned([DEVICE_GROUP, HOST_GROUP, HOST_NAPI]),
    );
    expect(planned(['test'])).not.toContain(device);
    expect(planned(['test'])).not.toContain(host);
    expect(planned(['test'])).not.toContain(napi);
  });

  it('wrap exactly their tests in the 30s code-bound class, behind the 60s class that wins first', async () => {
    const config = await readRecord(NEXTEST_REPO_CONFIG_PATH, parseToml);
    const profile = child(child(config, 'profile'), 'default');
    const overrides = records(profile.overrides);
    // The run-wrapper rules alone: the setup rule beside them is the lanes' sweep (below).
    const scripts = records(profile.scripts).filter((script) => script['run-wrapper'] !== undefined);
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
  });

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
  });
});

describe('the per-user mount root the real-image fixtures keep their scratch trees below', () => {
  it('is provisioned by a target cached on its probe of the host, so no fresh runner replays a skip', async () => {
    const graph = await projectGraph();
    const setup = graph.nodes.cowshed?.data.targets?.[APFS_TEST_ROOT.target];
    expect(setup?.executor).toBe('nx:run-commands');
    expect(setup?.options?.command).toBe('bun scripts/apfs-test-root.ts');
    // The directories are host state, not an Nx output: the key is the probe, `provisioned` only on a host needing nothing.
    expect(setup?.cache).toBe(true);
    expect(setup?.inputs).toContainEqual({
      runtime:
        'sh node_modules/@smoothbricks/nx-plugin/runtime-input.sh bun packages/cowshed/scripts/apfs-test-root.ts --probe',
    });
  });

  it('sweeps dead runs first in each real-image lane that runs, and in none replayed from cache', async () => {
    const graph = await projectGraph();
    // No sweep target: a lane replayed from cache starts no fixture, so nothing sweeps for it.
    expect(graph.nodes.cowshed?.data.targets?.['apfs-test-sweep']).toBeUndefined();
    const config = await readRecord(NEXTEST_REPO_CONFIG_PATH, parseToml);
    expect(config.experimental).toContain('setup-scripts');
    expect(child(child(child(config, 'scripts'), 'setup'), 'apfs-sweep').command).toEqual({
      'command-line': 'bun packages/cowshed/scripts/apfs-test-sweep.ts',
      'relative-to': 'none',
    });
    // nextest runs it before the first selected test of either group or of the host-controller lane, on macOS.
    const profile = child(child(config, 'profile'), 'default');
    const grouped = records(profile.overrides).filter(
      (override) => override['test-group'] === 'real-apfs' || override['test-group'] === 'real-apfs-host',
    );
    const setups = records(profile.scripts).filter((rule) => rule.setup !== undefined);
    expect(setups.map(({ setup, platform, filter }) => ({ setup, platform, filter }))).toEqual([
      {
        setup: 'apfs-sweep',
        platform: SERIAL_TEST_GROUP_PLATFORM,
        filter: [...grouped.map((override) => `(${String(override.filter)})`), 'test(/host_controller_/)'].join(' or '),
      },
    ]);
    // integration-apfs runs under cargo test, so the sweep is its own first step, from the workspace root.
    const integration = graph.nodes.cowshed?.data.targets?.['integration-apfs'];
    expect(integration?.options?.cwd).toBe('.');
    expect(String(integration?.options?.command)).toStartWith('bun packages/cowshed/scripts/apfs-test-sweep.ts && ');
    // The script extracts the root's archive with the native extractor, so every lane that runs builds both before it
    // does. Linux CI infers its own graph, so this run checks that one too, not only the host's.
    for (const { platform, arch } of GRAPH_PLATFORMS) {
      const inferred = await projectGraph({ platform, arch });
      const tasks = createTaskGraph(
        inferred,
        {},
        [ROOT_PROJECT, 'cowshed'],
        [DEVICE_GROUP, HOST_GROUP, 'host-controller-test', 'integration-apfs'],
        undefined,
        {},
      );
      const extractor = 'nextest-extract:scriptc-native';
      const producer = `scriptc-${arch}-${platform === 'darwin' ? 'macos' : 'linux'}`;
      expect(tasks.dependencies[extractor], platform).toEqual([`nextest-extract:${producer}`]);
      expect(inferred.nodes['nextest-extract']?.data.targets?.[producer]?.options?.outputDirectory, platform).toBe(
        `dist/bin/${platform}-${arch}`,
      );
      for (const [project, target] of [
        [ROOT_PROJECT, DEVICE_GROUP],
        [ROOT_PROJECT, HOST_GROUP],
        ['cowshed', 'host-controller-test'],
        ['cowshed', 'integration-apfs'],
      ] as const) {
        const lane = `${project}:${target}`;
        expect(tasks.dependencies[lane], `${platform} ${lane}`).toContain(`${ROOT_PROJECT}:cargo-test-archive`);
        // A serial group is inferred only where its declared cfg holds, macOS: there it runs bounded by progress, and
        // elsewhere its root entry only orders its dependencies and runs no command, so it sweeps nothing.
        const runner = inferred.nodes[project]?.data.targets?.[target];
        const group = target === DEVICE_GROUP || target === HOST_GROUP;
        if (group && platform === 'darwin') expect(runner?.options?.idleTimeoutMs, lane).toBe(180000);
        if (group && platform !== 'darwin') {
          expect(runner?.options?.command, `${platform} ${lane}`).toBeUndefined();
          continue;
        }
        expect(runner?.options?.command, `${platform} ${lane}`).toBeString();
        const reached = (tasks.dependencies[lane] ?? []).flatMap((id) => [id, ...(tasks.dependencies[id] ?? [])]);
        expect(reached, `${platform} ${lane}`).toContain(`nextest-extract:${producer}`);
      }
    }
    const nx = await readRecord('nx.json', JSON.parse);
    expect(child(child(nx, 'targetDefaults'), 'cargo-test-cowshed-*').dependsOn).toEqual(['...', APFS_TEST_ROOT]);
  });
  it('is provisioned before every test that makes a scratch root there, in a shard, a group or a host proof', async () => {
    const graph = await projectGraph();
    // The crates whose Rust sources make scratch roots: by the function that does, or the support module that calls it.
    const makers = new Set<string>();
    for (const source of await rustSources(listCargoWorkspacePackages(REPOSITORY))) {
      if (/\bmacos_scratch_root\b|\bscratch_apfs\b/.test(source.text)) makers.add(source.owner);
    }
    const consumers = [
      { project: 'cowshed', target: 'host-controller-test' },
      { project: 'cowshed', target: 'integration-apfs' },
      { project: ROOT_PROJECT, target: DEVICE_GROUP },
      { project: ROOT_PROJECT, target: HOST_GROUP },
    ];
    for (const [project, node] of Object.entries(graph.nodes)) {
      for (const target of Object.keys(node.data.targets ?? {})) {
        const crate = packageNameFromCargoTestTarget(target);
        if (crate !== null && makers.has(crate)) consumers.push({ project, target });
      }
    }
    // Not vacuous: the three crates that hold the real_apfs_ tests are among those whose shards are checked.
    const crates = new Set(consumers.map(({ target }) => packageNameFromCargoTestTarget(target)));
    for (const crate of ['cowshed-apfs-tests', 'cowshed-cli', 'cowshed-core'])
      expect(crates.has(crate), crate).toBe(true);

    const projects = [...new Set(consumers.map(({ project }) => project))];
    const targets = [...new Set(consumers.map(({ target }) => target))];
    const tasks = createTaskGraph(graph, {}, projects, targets, undefined, {});
    for (const { project, target } of consumers) {
      const id = `${project}:${target}`;
      expect(tasks.dependencies[id], id).toContain(APFS_TEST_ROOT_TASK);
    }
  });
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

/** A platform the plugin infers targets for: Node's names, as `process.platform` and `process.arch` give them. */
interface GraphPlatform {
  readonly platform: NodeJS.Platform;
  readonly arch: string;
}

/** The host's graph, and the graph of the Linux x64 runner CI infers when the host is not that runner. */
const GRAPH_PLATFORMS: readonly GraphPlatform[] =
  process.platform === 'linux' && process.arch === 'x64'
    ? [{ platform: 'linux', arch: 'x64' }]
    : [
        { platform: process.platform, arch: process.arch },
        { platform: 'linux', arch: 'x64' },
      ];

/**
 * The child that builds the graph. For another platform it first loads Nx, whose native binding is the host's, and the
 * plugin itself: Nx runs a workspace plugin in a worker of its own, which would report the host's platform, so the
 * child loads it in-process instead. Only then does it report `platform` as the process's, so the plugin infers that
 * platform's targets everywhere it asks.
 */
function graphScript({ platform, arch }: GraphPlatform): string {
  const reported =
    platform === process.platform && arch === process.arch
      ? ''
      : `const { loadNxPlugin } = await import('nx/src/project-graph/plugins/in-process-loader.js');
require('nx/src/project-graph/plugins/get-plugins.js').loadingMethod = (plugin, root, index) => loadNxPlugin(plugin, root, index);
await import('@smoothbricks/nx-plugin');
Object.defineProperty(process, 'platform', { value: ${JSON.stringify(platform)} });
Object.defineProperty(process, 'arch', { value: ${JSON.stringify(arch)} });
`;
  return `const { createProjectGraphAsync } = await import('nx/src/project-graph/project-graph.js');
${reported}const graph = await createProjectGraphAsync({ exitOnError: false });
process.stdout.write(JSON.stringify({ nodes: graph.nodes, dependencies: graph.dependencies }));`;
}

const projectGraphs = new Map<string, Promise<ProjectGraph>>();

/**
 * The repository's own project graph as Nx builds it with this plugin on `target` (the host's by default), computed
 * once for the file per platform: each costs a child process.
 */
function projectGraph(
  target: GraphPlatform = { platform: process.platform, arch: process.arch },
): Promise<ProjectGraph> {
  const key = `${target.platform}-${target.arch}`;
  let graph = projectGraphs.get(key);
  if (graph === undefined) {
    graph = runInRepositoryNx(graphScript(target)).then(parseProjectGraph);
    projectGraphs.set(key, graph);
  }
  return graph;
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
