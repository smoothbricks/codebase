import { describe, expect, it } from 'bun:test';
import { join, resolve } from 'node:path';
import { RETENTION_RECEIPT, type RetentionReport } from '@smoothbricks/cargo-retain';
import type { ProjectGraph, TargetConfiguration } from 'nx/src/devkit-exports.js';
import type { PostTasksExecutionContext } from 'nx/src/project-graph/plugins/public-api.js';
import {
  CARGO_PRODUCER_METADATA,
  isCoveredByWorkspace,
  mbxManagesTargets,
  type RetentionPassOutcome,
  type RetentionReportLog,
  ran,
  reportRetentionOutcome,
  retentionRoots,
  type TaskStatus,
} from './cargo-retention-hook.js';

type TaskResults = PostTasksExecutionContext['taskResults'];

const WORKSPACE_ROOT = '/repo';

const TASK_STATUS_EXECUTED = {
  success: true,
  failure: true,
  stopped: true,
  skipped: false,
  'local-cache-kept-existing': false,
  'local-cache': false,
  'remote-cache': false,
} as const satisfies Readonly<Record<TaskStatus, boolean>>;

const ALL_TASK_STATUSES: readonly TaskStatus[] = [
  'success',
  'failure',
  'stopped',
  'skipped',
  'local-cache-kept-existing',
  'local-cache',
  'remote-cache',
];

function taskResult(project: string, target: string, status: TaskStatus, projectRoot?: string): TaskResults[string] {
  return {
    status,
    code: status === 'failure' ? 1 : 0,
    task: {
      id: `${project}:${target}`,
      target: { project, target },
      overrides: {},
      outputs: [],
      cache: false,
      ...(projectRoot === undefined ? {} : { projectRoot }),
    },
  };
}

function projectGraph(
  projects: Record<string, { readonly root: string; readonly targets?: Record<string, TargetConfiguration> }>,
): ProjectGraph {
  const nodes: ProjectGraph['nodes'] = {};
  for (const [name, config] of Object.entries(projects)) {
    nodes[name] = {
      type: 'lib',
      name,
      data: {
        name,
        root: config.root,
        ...(config.targets === undefined ? {} : { targets: config.targets }),
      },
    };
  }
  return { nodes, dependencies: {} };
}

function reclaimedReport(
  workspaceRoot: string,
  targetDir: string,
  nestedRoots: RetentionReport['nestedRoots'] = [],
): RetentionReport {
  return {
    workspaceRoot,
    targetDir,
    nestedRoots,
    outcome: {
      kind: 'reclaimed',
      reclaim: [],
      reclaimedBytes: 0,
      linkedBytes: 0,
      retained: [],
      recency: { kind: 'no-units' },
    },
  };
}

describe('Cargo retention beside mbx', () => {
  // specs/cowshed/16_build_volumes.md, "Migration" step 4: an mbx project's managed targets are mbx's to collect.
  it("is mbx's wherever the canonical mbx environment names the store and managed targets", () => {
    for (const [environment, managed] of [
      [{}, false],
      [{ MBX_CACHE_DIR: '/bc/mbx' }, false],
      [{ MBX_TARGET_VIEWS: '1' }, false],
      [{ MBX_CACHE_DIR: '', MBX_TARGET_VIEWS: '1' }, false],
      [{ MBX_CACHE_DIR: '/bc/mbx', MBX_TARGET_VIEWS: '0' }, false],
      [{ MBX_CACHE_DIR: '/bc/mbx', MBX_TARGET_VIEWS: '1' }, true],
    ] as const) {
      expect(mbxManagesTargets(environment)).toBe(managed);
    }
  });
});

// packages/nx-plugin/README.md:259-270: only executed Cargo producers start passes; cache replays, fetch and noop start none.
describe('Cargo retention hook trigger and outcome decisions (packages/nx-plugin/README.md:259-270)', () => {
  it('starts a pass for every executed status × cargoProducer present, and never for non-executed statuses or non-producers', () => {
    expect(new Set<string>(ALL_TASK_STATUSES)).toEqual(new Set(Object.keys(TASK_STATUS_EXECUTED)));
    const graph = projectGraph({
      ws: {
        root: 'packages/ws',
        targets: {
          producer: { executor: 'nx:run-commands', metadata: CARGO_PRODUCER_METADATA },
          noop: { executor: 'nx:noop' },
          fetch: { executor: 'nx:run-commands', metadata: {} },
          explicitFalse: { executor: 'nx:run-commands', metadata: { cargoProducer: false } },
          nonBoolean: { executor: 'nx:run-commands', metadata: { cargoProducer: 'true' } },
        },
      },
    });
    const metadataCases = [
      { target: 'producer', isProducer: true },
      { target: 'noop', isProducer: false },
      { target: 'fetch', isProducer: false },
      { target: 'explicitFalse', isProducer: false },
      { target: 'nonBoolean', isProducer: false },
    ] as const;

    for (const status of ALL_TASK_STATUSES) {
      const executed = TASK_STATUS_EXECUTED[status];
      expect(ran(status), `ran(${status})`).toBe(executed);
      for (const { target, isProducer } of metadataCases) {
        const results: TaskResults = { [`ws:${target}`]: taskResult('ws', target, status, 'packages/ws') };
        const expected = executed && isProducer ? [join(WORKSPACE_ROOT, 'packages/ws')] : [];
        expect(retentionRoots(results, graph, WORKSPACE_ROOT), `${status} × ${target}`).toEqual(expected);
      }
    }
  });

  it('resolves nativeTest cwd vs project root and skips tasks missing from the project graph', () => {
    const graph = projectGraph({
      app: {
        root: 'packages/app',
        targets: {
          'native-relative-cwd': {
            executor: '@smoothbricks/nx-plugin:bounded-exec',
            metadata: CARGO_PRODUCER_METADATA,
            options: { nativeTest: { kind: 'cargo-nextest' }, cwd: 'crates/workspace' },
          },
          'native-absolute-cwd': {
            executor: '@smoothbricks/nx-plugin:bounded-exec',
            metadata: CARGO_PRODUCER_METADATA,
            options: { nativeTest: { kind: 'cargo-nextest' }, cwd: '/external/cargo-ws' },
          },
          'native-no-cwd': {
            executor: '@smoothbricks/nx-plugin:bounded-exec',
            metadata: CARGO_PRODUCER_METADATA,
            options: { nativeTest: { kind: 'cargo-nextest' } },
          },
          'native-non-string-cwd': {
            executor: '@smoothbricks/nx-plugin:bounded-exec',
            metadata: CARGO_PRODUCER_METADATA,
            options: { nativeTest: { kind: 'cargo-nextest' }, cwd: 42 },
          },
          'plain-with-cwd': {
            executor: 'nx:run-commands',
            metadata: CARGO_PRODUCER_METADATA,
            options: { cwd: 'ignored/cwd' },
          },
        },
      },
    });

    const cases: readonly {
      readonly name: string;
      readonly results: TaskResults;
      readonly expected: readonly string[];
    }[] = [
      {
        name: 'nativeTest with relative cwd resolves against workspaceRoot',
        results: { t: taskResult('app', 'native-relative-cwd', 'success', 'packages/app') },
        expected: [resolve(WORKSPACE_ROOT, 'crates/workspace')],
      },
      {
        name: 'nativeTest with absolute cwd resolves to that path',
        results: { t: taskResult('app', 'native-absolute-cwd', 'success', 'packages/app') },
        expected: [resolve(WORKSPACE_ROOT, '/external/cargo-ws')],
      },
      {
        name: 'nativeTest without cwd falls back to task.projectRoot',
        results: { t: taskResult('app', 'native-no-cwd', 'success', 'packages/override-root') },
        expected: [join(WORKSPACE_ROOT, 'packages/override-root')],
      },
      {
        name: 'nativeTest without cwd or task.projectRoot falls back to project.data.root',
        results: { t: taskResult('app', 'native-no-cwd', 'success') },
        expected: [join(WORKSPACE_ROOT, 'packages/app')],
      },
      {
        name: 'nativeTest with non-string cwd falls back to project root',
        results: { t: taskResult('app', 'native-non-string-cwd', 'success') },
        expected: [join(WORKSPACE_ROOT, 'packages/app')],
      },
      {
        name: 'non-nativeTest target ignores options.cwd and uses task.projectRoot',
        results: { t: taskResult('app', 'plain-with-cwd', 'success', 'packages/from-task') },
        expected: [join(WORKSPACE_ROOT, 'packages/from-task')],
      },
      {
        name: 'non-nativeTest target falls back to project.data.root when task.projectRoot is omitted',
        results: { t: taskResult('app', 'plain-with-cwd', 'success') },
        expected: [join(WORKSPACE_ROOT, 'packages/app')],
      },
      {
        name: 'project missing from graph is skipped',
        results: { t: taskResult('ghost', 'build', 'success', 'packages/ghost') },
        expected: [],
      },
      {
        name: 'target missing from project is skipped',
        results: { t: taskResult('app', 'unknown-target', 'success', 'packages/app') },
        expected: [],
      },
    ];

    for (const { name, results, expected } of cases) {
      expect(retentionRoots(results, graph, WORKSPACE_ROOT), name).toEqual([...expected]);
    }
  });

  it('sorts and deduplicates nested and sibling roots, and skips only roots covered by an answered workspace', () => {
    const graph = projectGraph({
      siblingB: { root: 'crates/b', targets: { build: { metadata: CARGO_PRODUCER_METADATA } } },
      nested: { root: 'crates/a/sub', targets: { build: { metadata: CARGO_PRODUCER_METADATA } } },
      parentA: {
        root: 'crates/a',
        targets: {
          build: { metadata: CARGO_PRODUCER_METADATA },
          lint: { metadata: CARGO_PRODUCER_METADATA },
        },
      },
      siblingPrefix: { root: 'crates/a-extra', targets: { build: { metadata: CARGO_PRODUCER_METADATA } } },
    });

    const results: TaskResults = {
      'siblingB:build': taskResult('siblingB', 'build', 'success'),
      'nested:build': taskResult('nested', 'build', 'failure'),
      'parentA:build': taskResult('parentA', 'build', 'success'),
      'parentA:lint': taskResult('parentA', 'lint', 'stopped'),
      'siblingPrefix:build': taskResult('siblingPrefix', 'build', 'success'),
    };

    // Nested roots stay in the sorted candidate list so a failed parent pass never suppresses a nested workspace.
    expect(retentionRoots(results, graph, WORKSPACE_ROOT)).toEqual([
      join(WORKSPACE_ROOT, 'crates/a'),
      join(WORKSPACE_ROOT, 'crates/a-extra'),
      join(WORKSPACE_ROOT, 'crates/a/sub'),
      join(WORKSPACE_ROOT, 'crates/b'),
    ]);

    const coverageCases: readonly {
      readonly name: string;
      readonly root: string;
      readonly done: readonly string[];
      readonly covered: boolean;
    }[] = [
      { name: 'no completed workspace covers nothing', root: '/repo/crates/a', done: [], covered: false },
      { name: 'exact workspace root is covered', root: '/repo/crates/a', done: ['/repo/crates/a'], covered: true },
      {
        name: 'nested root under completed workspace is covered',
        root: '/repo/crates/a/sub',
        done: ['/repo/crates/a'],
        covered: true,
      },
      { name: 'sibling root is not covered', root: '/repo/crates/b', done: ['/repo/crates/a'], covered: false },
      {
        name: 'sibling sharing a path prefix is not covered',
        root: '/repo/crates/a-extra',
        done: ['/repo/crates/a'],
        covered: false,
      },
      {
        name: 'sibling is covered once an earlier member discovered the enclosing Cargo workspace root',
        root: '/repo/crates/b',
        done: ['/repo'],
        covered: true,
      },
      {
        name: 'ancestor above completed workspace is not covered',
        root: '/repo',
        done: ['/repo/crates/a'],
        covered: false,
      },
    ];

    for (const { name, root, done, covered } of coverageCases) {
      expect(isCoveredByWorkspace(root, done), name).toBe(covered);
    }
  });

  it('maps every typed retention outcome and escaped error to its reported level, line and completed root', () => {
    const root = '/repo/ws';
    const targetDir = '/repo/ws/target';
    const nextDueAt = Date.UTC(2026, 10, 27, 12, 0, 0);
    const thrownWithStack = new Error('spawn EPERM');
    thrownWithStack.stack = 'Error: spawn EPERM\n    at cargo';
    const thrownWithoutStack = new Error('spawn EPERM');
    thrownWithoutStack.stack = undefined;

    const cases: readonly {
      readonly name: string;
      readonly outcome: RetentionPassOutcome;
      readonly expected: RetentionReportLog;
    }[] = [
      {
        name: 'started detached pass reports info and records workspaceRoot',
        outcome: {
          kind: 'result',
          result: { ok: true, value: { kind: 'started', workspaceRoot: root, targetDir, pid: 4242 } },
        },
        expected: {
          level: 'info',
          completedWorkspaceRoot: root,
          message:
            `Cargo retention ${targetDir}: due; its pass runs detached in pid 4242, ` +
            `which records its outcome in ${join(targetDir, RETENTION_RECEIPT)}`,
        },
      },
      {
        name: 'not-due answer reports info without scanning and records workspaceRoot',
        outcome: {
          kind: 'result',
          result: {
            ok: true,
            value: {
              kind: 'answered',
              report: { workspaceRoot: root, targetDir, nestedRoots: [], outcome: { kind: 'not-due', nextDueAt } },
            },
          },
        },
        expected: {
          level: 'info',
          completedWorkspaceRoot: root,
          message: `Cargo retention ${targetDir}: automatic maintenance not due until 2026-11-27T12:00:00.000Z; no scan or cleanup ran`,
        },
      },
      {
        name: 'busy answer reports info and records workspaceRoot',
        outcome: {
          kind: 'result',
          result: {
            ok: true,
            value: {
              kind: 'answered',
              report: {
                workspaceRoot: root,
                targetDir,
                nestedRoots: [],
                outcome: { kind: 'busy', lock: `${targetDir}/.smoo-cargo-retention.lock` },
              },
            },
          },
        },
        expected: {
          level: 'info',
          completedWorkspaceRoot: root,
          message: `Cargo retention ${targetDir}: skipped, ${targetDir}/.smoo-cargo-retention.lock is held`,
        },
      },
      {
        name: 'root lock refusal reports warn and records workspaceRoot',
        outcome: {
          kind: 'result',
          result: {
            ok: true,
            value: {
              kind: 'answered',
              report: {
                workspaceRoot: root,
                targetDir,
                nestedRoots: [],
                outcome: {
                  kind: 'refused',
                  profile: targetDir,
                  reason: `cannot attempt the retention lock: open '${targetDir}/.smoo-cargo-retention.lock': errno 1: operation not permitted`,
                },
              },
            },
          },
        },
        expected: {
          level: 'warn',
          completedWorkspaceRoot: root,
          message:
            `Cargo retention ${targetDir}: refused, ${targetDir}: cannot attempt the retention lock: ` +
            `open '${targetDir}/.smoo-cargo-retention.lock': errno 1: operation not permitted; nothing was changed`,
        },
      },
      {
        name: 'refused nested root elevates an otherwise reclaimed report to warn',
        outcome: {
          kind: 'result',
          result: {
            ok: true,
            value: {
              kind: 'answered',
              report: reclaimedReport(root, targetDir, [
                {
                  root: `${targetDir}/cargo-lint`,
                  outcome: { kind: 'refused', profile: 'debug', reason: 'unreadable fingerprint' },
                },
              ]),
            },
          },
        },
        expected: {
          level: 'warn',
          completedWorkspaceRoot: root,
          message:
            `Cargo retention ${targetDir}: reclaimed 0.00 GiB in 0 superseded unit generations ` +
            '(0.00 GiB more stays linked elsewhere); 0 retained; ' +
            `nested ${targetDir}/cargo-lint: refused, debug: unreadable fingerprint; nothing was changed`,
        },
      },
      {
        name: 'busy nested root beside reclaimed root stays info',
        outcome: {
          kind: 'result',
          result: {
            ok: true,
            value: {
              kind: 'answered',
              report: reclaimedReport(root, targetDir, [
                {
                  root: `${targetDir}/cargo-lint`,
                  outcome: { kind: 'busy', lock: `${targetDir}/cargo-lint/debug/.cargo-lock` },
                },
              ]),
            },
          },
        },
        expected: {
          level: 'info',
          completedWorkspaceRoot: root,
          message:
            `Cargo retention ${targetDir}: reclaimed 0.00 GiB in 0 superseded unit generations ` +
            '(0.00 GiB more stays linked elsewhere); 0 retained; ' +
            `nested ${targetDir}/cargo-lint: skipped, ${targetDir}/cargo-lint/debug/.cargo-lock is held`,
        },
      },
      {
        name: 'cargo-metadata error reports warn and leaves completedWorkspaceRoot unset',
        outcome: {
          kind: 'result',
          result: {
            ok: false,
            error: { kind: 'cargo-metadata', cwd: root, detail: 'no Cargo.toml found' },
          },
        },
        expected: {
          level: 'warn',
          message: `Cargo retention for ${root}: cargo metadata in ${root} failed: no Cargo.toml found`,
        },
      },
      {
        name: 'target-not-cargo error reports warn and leaves completedWorkspaceRoot unset',
        outcome: {
          kind: 'result',
          result: {
            ok: false,
            error: { kind: 'target-not-cargo', targetDir, detail: 'CACHEDIR.TAG signature mismatch' },
          },
        },
        expected: {
          level: 'warn',
          message: `Cargo retention for ${root}: ${targetDir} is not a Cargo target directory: CACHEDIR.TAG signature mismatch`,
        },
      },
      {
        name: 'io error reports warn and leaves completedWorkspaceRoot unset',
        outcome: {
          kind: 'result',
          result: {
            ok: false,
            error: { kind: 'io', operation: 'start the automatic retention pass', path: root, detail: 'spawn EPERM' },
          },
        },
        expected: {
          level: 'warn',
          message: `Cargo retention for ${root}: cannot start the automatic retention pass ${root}: spawn EPERM`,
        },
      },
      {
        name: 'escaped Error with stack reports unexpected failure warn and leaves completedWorkspaceRoot unset',
        outcome: { kind: 'thrown', error: thrownWithStack },
        expected: {
          level: 'warn',
          message: `Cargo retention for ${root}: unexpected failure: Error: spawn EPERM\n    at cargo`,
        },
      },
      {
        name: 'escaped Error without stack falls back to message',
        outcome: { kind: 'thrown', error: thrownWithoutStack },
        expected: {
          level: 'warn',
          message: `Cargo retention for ${root}: unexpected failure: spawn EPERM`,
        },
      },
      {
        name: 'escaped non-Error value stringifies into unexpected failure warn',
        outcome: { kind: 'thrown', error: 'cargo spawn denied' },
        expected: {
          level: 'warn',
          message: `Cargo retention for ${root}: unexpected failure: cargo spawn denied`,
        },
      },
    ];

    for (const { name, outcome, expected } of cases) {
      expect(reportRetentionOutcome(root, outcome), name).toEqual(expected);
    }
  });
});
