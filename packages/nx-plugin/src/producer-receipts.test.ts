import { describe, expect, it } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdir, mkdtemp, readdir, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { Task, TaskGraph } from 'nx/src/config/task-graph';
import { expandOutputs } from 'nx/src/native';
import type { TaskResults } from 'nx/src/tasks-runner/life-cycle';
import { readModulePackageJson } from 'nx/src/utils/package-json';
import { pastTick } from './__tests__/past-tick.js';
import type { EnsureBuiltProducerReceipt } from './ensure-built.js';
import {
  censusStart,
  type GraphInstallationCensus,
  movedNodes,
  type NodeWitness,
  readInstallationCensus,
} from './ensure-built-witness.js';
import {
  completedProducerTasks,
  installedClosureTool,
  movedToolNodes,
  PRODUCER_RECORDS,
  postTasksExecution,
  preTasksExecution,
  producerEnvironmentIdentity,
  readProducerReceipt,
} from './producer-receipts.js';

const repoRoot = dirname(dirname(dirname(dirname(fileURLToPath(import.meta.url)))));

const task: Task = {
  id: 'app:build',
  target: { project: 'app', target: 'build' },
  overrides: {},
  outputs: ['dist'],
  cache: false,
  hash: 'actual-nx-hash',
  hashDetails: { command: 'actual-command', nodes: { source: 'actual-source-identity' } },
};
const graph: TaskGraph = {
  roots: [task.id],
  tasks: { [task.id]: task },
  dependencies: { [task.id]: [] },
  continuousDependencies: {},
};
const inputs: EnsureBuiltProducerReceipt['inputs'] = [
  {
    taskId: task.id,
    overrides: {},
    declared: [],
    expanded: {
      files: [],
      runtime: ['declared-version-recipe'],
      environment: ['DECLARED_ENV'],
      depOutputs: [],
      external: [],
    },
    hashDetails: { command: 'actual-command', nodes: { 'env:DECLARED_ENV': 'producer-value-hash' } },
    files: [],
  },
];

describe('actual producer completion evidence', () => {
  it('accepts successful uncacheable prerequisites without looking for cache rows', () => {
    const results: TaskResults = { [task.id]: { task, status: 'success', code: 0 } };
    expect(completedProducerTasks(graph, results)).toEqual({ ok: true, value: [task] });
  });
  it('accepts actual Nx successful cache completion statuses, not invented hit receipts', () => {
    const statuses: readonly TaskResults[string]['status'][] = [
      'local-cache',
      'local-cache-kept-existing',
      'remote-cache',
    ];
    for (const status of statuses) {
      const cached: Task = { ...task, cache: true };
      expect(
        completedProducerTasks(
          { ...graph, tasks: { [task.id]: cached } },
          { [task.id]: { task: cached, status, code: 0 } },
        ).ok,
      ).toBe(true);
    }
  });
  it('refuses missing, failed, stopped and skipped completion evidence', () => {
    expect(completedProducerTasks(graph, {}).ok).toBe(false);
    const statuses: readonly TaskResults[string]['status'][] = ['failure', 'stopped', 'skipped'];
    for (const status of statuses)
      expect(completedProducerTasks(graph, { [task.id]: { task, status, code: 0 } }).ok).toBe(false);
    expect(completedProducerTasks(graph, { [task.id]: { task, status: 'success', code: 1 } }).ok).toBe(false);
  });
  it('refuses absent actual hash details, override changes, output changes and configuration changes', () => {
    for (const changed of [
      { ...task, hash: undefined },
      { ...task, hashDetails: undefined },
      { ...task, overrides: { configuration: 'other' } },
      { ...task, outputs: ['different'] },
      { ...task, target: { ...task.target, configuration: 'other' } },
    ]) {
      expect(completedProducerTasks(graph, { [task.id]: { task: changed, status: 'success', code: 0 } }).ok).toBe(
        false,
      );
    }
  });
  it('labels producer Nx runtime/environment identity without reading a consumer environment', () => {
    const identity = producerEnvironmentIdentity(inputs);
    const saved = process.env.DECLARED_ENV;
    process.env.DECLARED_ENV = 'different-consumer-value';
    try {
      expect(producerEnvironmentIdentity(inputs)).toBe(identity);
    } finally {
      if (saved === undefined) delete process.env.DECLARED_ENV;
      else process.env.DECLARED_ENV = saved;
    }
    expect(
      producerEnvironmentIdentity(
        inputs.map((input) => ({
          ...input,
          hashDetails: { ...input.hashDetails, nodes: { 'env:DECLARED_ENV': 'different-actual-producer-hash' } },
        })),
      ),
    ).not.toBe(identity);
  });
  it('disabled producer hooks perform no source, graph or publication work', async () => {
    const context = {
      id: 'disabled-hook',
      workspaceRoot: '/not-a-workspace',
      nxJsonConfiguration: {},
      argv: [],
      projectGraph: { nodes: {}, dependencies: {} },
      taskGraph: graph,
    };
    await preTasksExecution(undefined, context);
    await postTasksExecution(undefined, { ...context, taskResults: {}, startTime: 10, endTime: 20 });
  });
  it('does no capture or publication for absent roots and other configurations', async () => {
    const root = await realpath(await mkdtemp(join(tmpdir(), 'producer-selection-')));
    try {
      await symlink(join(repoRoot, 'node_modules'), join(root, 'node_modules'), 'dir');
      const other: Task = { ...task, id: 'app:build:other', target: { ...task.target, configuration: 'other' } };
      const phases = () =>
        performance.getEntriesByType('measure').filter(({ name }) => name.startsWith('producerReceipts:'));
      const before = phases();
      for (const results of [{}, { [other.id]: { task: other, status: 'success', code: 0 } }] satisfies TaskResults[]) {
        const context = {
          id: 'unselected-hook',
          workspaceRoot: root,
          nxJsonConfiguration: {},
          argv: [],
          projectGraph: {
            nodes: {
              app: { name: 'app', type: 'lib', data: { root: '.', targets: { build: { executor: 'nx:noop' } } } },
            },
            dependencies: {},
          } satisfies import('nx/src/config/project-graph').ProjectGraph,
          taskGraph: { ...graph, tasks: Object.fromEntries(Object.values(results).map(({ task }) => [task.id, task])) },
        };
        await preTasksExecution({ producerReceipts: { targets: [task.id] } }, context);
        await postTasksExecution(
          { producerReceipts: { targets: [task.id] } },
          {
            ...context,
            taskResults: results,
            startTime: 10,
            endTime: 20,
          },
        );
      }
      expect(phases()).toEqual(before);
      expect((await readdir(root)).sort()).toEqual(['node_modules']);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
  it('publishes a refusal beside a published receipt, never over it, and names it in the miss', async () => {
    const root = await realpath(await mkdtemp(join(tmpdir(), 'producer-refusal-')));
    try {
      await writeFile(join(root, 'package.json'), JSON.stringify({ name: 'refusal-fixture' }));
      await writeFile(join(root, 'nx.json'), '{}');
      await symlink(join(repoRoot, 'node_modules'), join(root, 'node_modules'), 'dir');
      const records = join(root, PRODUCER_RECORDS);
      await mkdir(records, { recursive: true });
      const name = createHash('sha256').update(JSON.stringify(task.id)).digest('hex');
      const receiptPath = join(records, `producer-receipt-${name}.json`);
      // What an earlier run published; this run's refusal is no evidence about it.
      const published = JSON.stringify({ version: 1, disposition: 'published', receipt: 'an earlier run' });
      await writeFile(receiptPath, published);
      // A run whose pre-execution frame never bound (the sandboxed git failure's shape) refuses its target.
      await postTasksExecution(
        { producerReceipts: { targets: [task.id] } },
        {
          id: 'unbound-frame',
          workspaceRoot: root,
          nxJsonConfiguration: {},
          argv: [],
          taskResults: { [task.id]: { task, status: 'success', code: 0 } },
          startTime: 10,
          endTime: 20,
        },
      );
      expect(await readFile(receiptPath, 'utf8')).toBe(published);
      expect((await readdir(records)).sort()).toEqual([
        '.gitignore',
        `producer-receipt-${name}.json`,
        `producer-refusal-${name}.json`,
      ]);
      // The records directory ignores itself, so no checkout tracks or hashes a record.
      expect(await readFile(join(records, '.gitignore'), 'utf8')).toBe('*\n');
      expect(readProducerReceipt(root, task.id)).toEqual({
        ok: false,
        detail: `Corrupt producer completion contract at ${receiptPath}; the producer's latest run refused: No bound producer pre-execution frame`,
      });
      // With no receipt at all, the miss names where the receipt would be and why the latest run published none.
      await rm(receiptPath);
      expect(readProducerReceipt(root, task.id)).toEqual({
        ok: false,
        detail: `No producer completion receipt at ${receiptPath}; the producer's latest run refused: No bound producer pre-execution frame`,
      });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
  it('tells a receipt it cannot read from a missing one, naming the path either way', async () => {
    const root = await realpath(await mkdtemp(join(tmpdir(), 'producer-unreadable-')));
    try {
      const records = join(root, PRODUCER_RECORDS);
      await mkdir(records, { recursive: true });
      const name = createHash('sha256').update(JSON.stringify(task.id)).digest('hex');
      const receiptPath = join(records, `producer-receipt-${name}.json`);
      expect(readProducerReceipt(root, task.id)).toEqual({
        ok: false,
        detail: `No producer completion receipt at ${receiptPath}`,
      });
      // A sandbox that may not read the receipt answers EPERM; an unreadable file answers EACCES the same way.
      await writeFile(receiptPath, '{}', { mode: 0 });
      const denied = readProducerReceipt(root, task.id);
      expect(denied.ok ? 'hit' : denied.detail).toStartWith('Cannot read producer completion receipt: EACCES');
      expect(denied.ok ? 'hit' : denied.detail).toContain(receiptPath);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

/**
 * A workspace whose Nx is installed and whose plugin is a developer link: its package is a checkout outside
 * `node_modules`, whose `dist` a rebuild rewrites in place without touching any lockfile or `node_modules` entry.
 */
async function developerLinkedWorkspace(): Promise<{ readonly root: string; readonly dist: string }> {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'producer-census-')));
  const checkout = join(root, 'checkout');
  await mkdir(join(checkout, 'dist'), { recursive: true });
  await writeFile(
    join(checkout, 'package.json'),
    JSON.stringify({ name: '@smoothbricks/nx-plugin', version: '0.0.0', files: ['dist'] }),
  );
  const dist = join(checkout, 'dist/index.js');
  await writeFile(dist, 'export const value = 1;\n');
  await mkdir(join(root, 'node_modules/nx'), { recursive: true });
  await writeFile(join(root, 'node_modules/nx/package.json'), JSON.stringify({ name: 'nx', version: '0.0.0' }));
  await writeFile(join(root, 'node_modules/nx/index.js'), 'module.exports = {};\n');
  await mkdir(join(root, 'node_modules/@smoothbricks'));
  await symlink(checkout, join(root, 'node_modules/@smoothbricks/nx-plugin'), 'dir');
  return { root, dist };
}

describe("the producer hook's installed-closure census", () => {
  const sentinel = '0'.repeat(64);
  /** One census of `root`'s closure, taking hashes from the census recorded at `record`. */
  function closure(root: string, record: string, observed: NodeWitness[] = []) {
    const tool = installedClosureTool(
      root,
      { plugins: ['@smoothbricks/nx-plugin'] },
      null,
      record,
      readModulePackageJson,
      expandOutputs,
      { start: censusStart(), observed },
    );
    if (!tool.ok) throw new Error(tool.detail);
    return tool.value;
  }
  /** Records a census whose every file hash is `sentinel`: a census that reads a file reports its real hash instead. */
  async function recordSentinels(record: string): Promise<GraphInstallationCensus> {
    const recorded = readInstallationCensus(record);
    if (recorded === undefined) throw new Error(`no census recorded at ${record}`);
    await writeFile(
      record,
      JSON.stringify({ ...recorded, files: recorded.files.map(({ path }) => ({ path, sha256: sentinel })) }),
    );
    return recorded;
  }

  it('reads no file bytes of a closure whose node stamps have not moved, and names the phase it spent', async () => {
    const { root } = await developerLinkedWorkspace();
    try {
      const record = join(root, 'census.json');
      await pastTick();
      closure(root, record);
      const recorded = await recordSentinels(record);
      const census = performance.getEntriesByName('producerReceipts:census', 'measure').length;
      const files = new Set(recorded.files.map(({ path }) => path));
      const hashed = closure(root, record).nodes.filter(({ path }) => files.has(path));
      expect(hashed).toHaveLength(files.size);
      expect(files.size).toBeGreaterThan(2);
      expect(hashed.map((node) => (node.type === 'file' ? node.sha256 : node.type))).toEqual(
        hashed.map(() => sentinel),
      );
      expect(performance.getEntriesByName('producerReceipts:census', 'measure').length).toBe(census + 1);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('reads again a developer-linked dist rewritten in place, with no lockfile or node_modules change', async () => {
    const { root, dist } = await developerLinkedWorkspace();
    try {
      const record = join(root, 'census.json');
      await pastTick();
      // What the capture recorded is what the stability check after execution re-reads.
      const captured: NodeWitness[] = [];
      closure(root, record, captured);
      await recordSentinels(record);
      const rebuilt = 'export const value = 2;\n';
      await writeFile(dist, rebuilt);
      expect(movedNodes(captured)).toContain(dist);
      const nodes = closure(root, record).nodes;
      expect(nodes.find(({ path }) => path === dist)).toMatchObject({
        type: 'file',
        sha256: createHash('sha256').update(rebuilt).digest('hex'),
      });
      expect(nodes.find(({ path }) => path === join(root, 'node_modules/nx/index.js'))).toMatchObject({
        type: 'file',
        sha256: sentinel,
      });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  // The patched Nx daemon's macOS watcher probe (patches/README.md, "A stopped watcher stream is never served from"):
  // where .nxignore hides .nx and .git, any nx command's first daemon request creates one in node_modules and removes
  // it once the stream reports it, at any moment of a producer run, from its capture to its post-execution census.
  it("takes the Nx daemon's watcher probes in node_modules for no tool change, and a new module for one", async () => {
    const { root } = await developerLinkedWorkspace();
    try {
      const record = join(root, 'census.json');
      const modules = join(root, 'node_modules');
      const probe = (n: number) => join(modules, `nx-watch-probe-${process.pid}-${n}`);
      await pastTick();
      // One is out while the capture lists node_modules, one comes and goes during the run, one is out at the census.
      await writeFile(probe(1), '');
      const captured: NodeWitness[] = [];
      const tools = [closure(root, record, captured)];
      expect(captured.map(({ path }) => path)).toContain(modules);
      await rm(probe(1));
      await writeFile(probe(2), '');
      await rm(probe(2));
      await writeFile(probe(3), '');
      expect(movedToolNodes(captured, tools)).toBeNull();
      await rm(probe(3));
      await mkdir(join(modules, 'left-pad'));
      expect(movedToolNodes(captured, tools)).toBe(modules);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('knows the probe by the name the installed patched watcher gives it, in the node_modules it writes it to', async () => {
    const watcher = await readFile(fileURLToPath(import.meta.resolve('nx/src/daemon/server/watcher.js')), 'utf8');
    expect(watcher).toContain("const PROBE_NAME = 'nx-watch-probe-';");
    expect(watcher).toContain('`${probeDirectory}/${PROBE_NAME}${process.pid}-${++probeSequence}`');
    expect(watcher).toContain("(0, path_1.join)(workspace_root_1.workspaceRoot, 'node_modules'),");
  });
});
