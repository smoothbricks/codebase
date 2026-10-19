import { describe, expect, it } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdir, mkdtemp, readdir, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { Task, TaskGraph } from 'nx/src/config/task-graph';
import type { TaskResults } from 'nx/src/tasks-runner/life-cycle';
import type { EnsureBuiltProducerReceipt } from './ensure-built.js';
import {
  completedProducerTasks,
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
    const context = { id: 'disabled-hook', workspaceRoot: '/not-a-workspace', nxJsonConfiguration: {}, argv: [] };
    await preTasksExecution(undefined, context);
    await postTasksExecution(undefined, { ...context, taskResults: {}, startTime: 10, endTime: 20 });
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
