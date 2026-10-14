import { describe, expect, it } from 'bun:test';
import type { Task, TaskGraph } from 'nx/src/config/task-graph';
import type { TaskResults } from 'nx/src/tasks-runner/life-cycle';
import type { EnsureBuiltProducerReceipt } from './ensure-built.js';
import {
  completedProducerTasks,
  postTasksExecution,
  preTasksExecution,
  producerEnvironmentIdentity,
} from './producer-receipts.js';

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
});
