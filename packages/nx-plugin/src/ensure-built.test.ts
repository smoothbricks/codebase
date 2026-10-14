import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'bun:test';
import { spawn, spawnSync } from 'node:child_process';
import { chmod, mkdir, mkdtemp, readdir, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, isAbsolute, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { ProjectGraph } from 'nx/src/config/project-graph';
import type { Task } from 'nx/src/config/task-graph';
import type { DaemonProcessJson } from 'nx/src/daemon/cache';
import { expandOutputs } from 'nx/src/native';
import { getExecutorNameForTask } from 'nx/src/tasks-runner/utils';
import { signalToCode } from 'nx/src/utils/exit-codes';
import typia from 'typia';
import { nxFixtureRoot, withNxFixture } from './__tests__/fixture-nx-env.js';
import {
  assignEnvironment,
  callerOwnsWorkspace,
  cliExitOutcome,
  compareWithArtifacts,
  describeError,
  describeMiss,
  ensureBuilt,
  firstCacheMiss,
  firstMovedClaim,
  missBeforeProbe,
  outputClaims,
  parseExecArguments,
  parseTargetSelector,
  recordedTasks,
  selectorTaskId,
  unvouched,
  withoutOuterTaskCacheBypass,
  workspaceEnvironment,
  workspaceFileChanges,
} from './ensure-built.js';
import { managedAssetsRoot } from './managed-assets.js';
import { NX_LONGEST_SOCKET_NAME, ownedSocketDirectory, pidsWorkingIn } from './testing.js';

function task(overrides: Partial<Task> & Pick<Task, 'id'>): Task {
  const [project, target] = overrides.id.split(':');
  return {
    target: { project, target },
    overrides: {},
    outputs: [],
    cache: true,
    hash: `hash-of-${overrides.id}`,
    ...overrides,
  };
}

describe('parseTargetSelector', () => {
  it('accepts project:target and project:target:configuration', () => {
    expect(parseTargetSelector('app:build')).toEqual({
      project: 'app',
      target: 'build',
      configuration: undefined,
    });
    expect(parseTargetSelector('app:build:production')).toEqual({
      project: 'app',
      target: 'build',
      configuration: 'production',
    });
  });

  it('rejects anything that is not a two or three part selector', () => {
    expect(parseTargetSelector('build')).toBeNull();
    expect(parseTargetSelector('app:')).toBeNull();
    expect(parseTargetSelector(':build')).toBeNull();
    expect(parseTargetSelector('app:build:production:extra')).toBeNull();
  });
});

describe('selectorTaskId', () => {
  it('is the id Nx gives the task a selector names, configuration included', () => {
    expect(selectorTaskId({ project: 'app', target: 'build', configuration: undefined })).toBe('app:build');
    expect(selectorTaskId({ project: 'app', target: 'build', configuration: 'production' })).toBe(
      'app:build:production',
    );
  });
});

describe('parseExecArguments', () => {
  function usageOf(args: readonly string[]): string {
    const parsed = parseExecArguments(args);
    if (parsed.ok) {
      throw new Error(`${args.join(' ')} parsed as ${JSON.stringify(parsed.invocation)}`);
    }
    return parsed.usage;
  }

  it('reads the target, a workspace root in either spelling, and the binary with its arguments', () => {
    expect(parseExecArguments(['app:build', '--workspace-root', '/w', '--', './cli', '--flag'])).toEqual({
      ok: true,
      invocation: { target: 'app:build', workspaceRoot: '/w', command: ['./cli', '--flag'] },
    });
    expect(parseExecArguments(['--workspace-root=/w', 'app:build:production'])).toEqual({
      ok: true,
      invocation: { target: 'app:build:production', workspaceRoot: '/w', command: [] },
    });
  });

  it('rejects a malformed invocation, naming the mistake', () => {
    expect(usageOf(['app:build', '--'])).toContain('no binary given');
    expect(usageOf(['build', '--', './report'])).toContain('not a project:target');
    expect(usageOf(['app:build', '--', 'report'])).toContain('not a name to look up on PATH');
    expect(usageOf(['app:build', 'lib:build'])).toContain('more than one target');
    expect(usageOf(['app:build', '--workspace-root'])).toContain('--workspace-root needs a directory');
    expect(usageOf(['app:build', '--watch'])).toContain('unknown flag --watch');
    expect(usageOf([])).toContain('no project:target given');
  });
});

describe('describeError', () => {
  it('prints the message of a plain-object Nx rejection, never [object Object]', () => {
    expect(describeError({ stack: 'synthetic stack', message: 'daemon belongs to a different workspace' })).toBe(
      'daemon belongs to a different workspace',
    );
  });

  it('prints an Error by its message, a string as itself, and anything else inspected on one line', () => {
    expect(describeError(new Error('boom'))).toBe('boom');
    expect(describeError('plain')).toBe('plain');
    expect(describeError({ code: 7, nested: { deep: true } })).toBe('{ code: 7, nested: { deep: true } }');
  });
});

describe('firstCacheMiss', () => {
  const cached = new Map([
    ['hash-of-app:build', 0],
    ['hash-of-lib:build', 0],
  ]);

  it('clears a graph whose every task is a recorded success', () => {
    expect(firstCacheMiss([task({ id: 'lib:build' }), task({ id: 'app:build' })], cached)).toBeNull();
  });

  it('reports the uncacheable task rather than looking it up', () => {
    expect(firstCacheMiss([task({ id: 'app:build', cache: false })], cached)).toEqual({
      kind: 'uncacheable',
      taskId: 'app:build',
    });
  });

  it('reports a task that could not be hashed', () => {
    expect(firstCacheMiss([task({ id: 'app:build', hash: undefined })], cached)).toEqual({
      kind: 'unhashable',
      taskId: 'app:build',
    });
  });

  it('reports a task whose current inputs are not in the cache', () => {
    expect(firstCacheMiss([task({ id: 'other:build' })], cached)).toEqual({
      kind: 'not-cached',
      taskId: 'other:build',
    });
  });

  it('refuses to replay a cached failure', () => {
    const failures = new Map([['hash-of-app:build', 3]]);
    expect(firstCacheMiss([task({ id: 'app:build' })], failures)).toEqual({
      kind: 'cached-failure',
      taskId: 'app:build',
      code: 3,
    });
  });

  it('names the first failing task in graph order', () => {
    const tasks = [task({ id: 'lib:build' }), task({ id: 'app:build', cache: false }), task({ id: 'other:build' })];
    expect(firstCacheMiss(tasks, cached)).toEqual({ kind: 'uncacheable', taskId: 'app:build' });
  });
});

describe('recordedTasks', () => {
  // The project graph Nx hands the probe, reduced to the executors it is asked for.
  const graph: ProjectGraph = {
    nodes: {
      app: {
        name: 'app',
        type: 'app',
        data: {
          root: 'packages/app',
          targets: {
            build: { executor: 'nx:run-commands' },
            // A command-less target that only names dependencies, as Nx normalizes it.
            stage: { executor: 'nx:noop' },
            ship: { executor: 'nx:run-commands' },
            deploy: { executor: 'nx:run-commands' },
          },
        },
      },
    },
    dependencies: {},
  };
  const executorOf = (entry: Task) => getExecutorNameForTask(entry, graph);
  const records = new Map([
    ['hash-of-app:build', 0],
    ['hash-of-app:ship', 0],
  ]);

  it('needs no cache record for an uncacheable nx:noop aggregate, whose dependencies answer for it', () => {
    const tasks = [task({ id: 'app:build' }), task({ id: 'app:stage', cache: false }), task({ id: 'app:ship' })];
    const recorded = recordedTasks(tasks, executorOf);
    expect(recorded.map(({ id }) => id)).toEqual(['app:build', 'app:ship']);
    expect(firstCacheMiss(recorded, records)).toBeNull();
  });

  it('still holds an uncacheable task with a command to account', () => {
    const tasks = [task({ id: 'app:build' }), task({ id: 'app:deploy', cache: false })];
    expect(firstCacheMiss(recordedTasks(tasks, executorOf), records)).toEqual({
      kind: 'uncacheable',
      taskId: 'app:deploy',
    });
  });
});

describe('outputClaims', () => {
  it('claims the declared outputs of every hashed task that declares any, under its hash', () => {
    const tasks = [
      task({ id: 'lib:build', outputs: ['packages/lib/dist'] }),
      task({ id: 'app:stage' }),
      task({ id: 'app:build', outputs: ['packages/app/dist'], hash: undefined }),
    ];
    expect(outputClaims(tasks)).toEqual([
      { taskId: 'lib:build', outputs: ['packages/lib/dist'], hash: 'hash-of-lib:build' },
    ]);
  });
});

describe('unvouched', () => {
  const tasks = [task({ id: 'lib:build', outputs: ['lib/dist'] }), task({ id: 'app:build', outputs: ['app/dist'] })];

  it('clears outputs the daemon vouches for', () => {
    expect(unvouched(tasks, [true, true])).toEqual([]);
  });

  it('keeps the tasks whose outputs the daemon does not vouch for', () => {
    expect(unvouched(tasks, [true, false]).map(({ id }) => id)).toEqual(['app:build']);
  });

  it('treats a missing verdict as unvouched rather than as a hit', () => {
    expect(unvouched(tasks, [true]).map(({ id }) => id)).toEqual(['app:build']);
    expect(unvouched(tasks, []).map(({ id }) => id)).toEqual(['lib:build', 'app:build']);
  });
});

describe('compareWithArtifacts', () => {
  it('reuses a verified two-tree inventory without expanding or rereading artifact bytes', () => {
    const path = join(root, 'verified.json');
    let walks = 0;
    const expand: typeof expandOutputs = (...args) => {
      walks += 1;
      return expandOutputs(...args);
    };
    expect(compareWithArtifacts(workspace(), [claim], artifacts(), expand, path).kind).toBe('restorable');
    expect(walks).toBeGreaterThan(0);
    walks = 0;
    expect(compareWithArtifacts(workspace(), [claim], artifacts(), expand, path).kind).toBe('restorable');
    expect(walks).toBe(0);
  });

  it('invalidates verified equality for output or cache mutations even with restored mtime', async () => {
    const path = join(root, 'verified.json');
    const artifact = artifacts().get(claim.hash);
    if (artifact === undefined) throw new Error('Missing cache artifact in output-mutation fixture');
    const expected = join(artifact.outputsPath, 'packages/app/dist/marker.txt');
    for (const file of [marker(), expected]) {
      await writeFile(marker(), 'built\nlib\n');
      await writeFile(expected, 'built\nlib\n');
      expect(compareWithArtifacts(workspace(), [claim], artifacts(), expandOutputs, path).kind).toBe('restorable');
      const { stat, utimes } = await import('node:fs/promises');
      const original = await stat(file);
      await writeFile(file, 'BUILT\nLIB\n');
      await utimes(file, original.atime, original.mtime);
      expect(compareWithArtifacts(workspace(), [claim], artifacts(), expandOutputs, path)).toEqual({
        kind: 'stale-outputs',
        taskId: 'app:build',
      });
    }
  });

  it('invalidates inventories for deletion, permission changes, parent symlinks and added directory members', async () => {
    const path = join(root, 'verified.json');
    expect(compareWithArtifacts(workspace(), [claim], artifacts(), expandOutputs, path).kind).toBe('restorable');
    await chmod(marker(), 0o700);
    expect(compareWithArtifacts(workspace(), [claim], artifacts(), expandOutputs, path).kind).toBe('stale-outputs');
    await chmod(marker(), 0o644);
    expect(compareWithArtifacts(workspace(), [claim], artifacts(), expandOutputs, path).kind).toBe('restorable');
    await writeFile(join(workspace(), 'packages/app/dist/added'), 'added');
    expect(compareWithArtifacts(workspace(), [claim], artifacts(), expandOutputs, path).kind).toBe('stale-outputs');
    await rm(join(workspace(), 'packages/app/dist/added'));
    expect(compareWithArtifacts(workspace(), [claim], artifacts(), expandOutputs, path).kind).toBe('restorable');
    await rm(marker());
    expect(compareWithArtifacts(workspace(), [claim], artifacts(), expandOutputs, path).kind).toBe('stale-outputs');
    await writeFile(marker(), 'built\nlib\n');
    expect(compareWithArtifacts(workspace(), [claim], artifacts(), expandOutputs, path).kind).toBe('restorable');
    const { rename } = await import('node:fs/promises');
    await rename(join(workspace(), 'packages/app'), join(workspace(), 'packages/real-app'));
    await symlink('real-app', join(workspace(), 'packages/app'));
    expect(compareWithArtifacts(workspace(), [claim], artifacts(), expandOutputs, path).kind).toBe('stale-outputs');
  });

  it('falls back to real comparison for missing/unreadable witness and a changed cache binding', async () => {
    const path = join(root, 'verified.json');
    expect(compareWithArtifacts(workspace(), [claim], artifacts(), expandOutputs, path).kind).toBe('restorable');
    await chmod(path, 0);
    try {
      expect(compareWithArtifacts(workspace(), [claim], artifacts(), expandOutputs, path).kind).toBe('restorable');
    } finally {
      await chmod(path, 0o600);
    }
    const renamed = join(root, 'cache', 'other', 'outputs');
    await mkdir(join(renamed, 'packages/app/dist'), { recursive: true });
    await writeFile(join(renamed, 'packages/app/dist/marker.txt'), 'other');
    expect(
      compareWithArtifacts(workspace(), [claim], new Map([[claim.hash, { outputsPath: renamed }]]), expandOutputs, path)
        .kind,
    ).toBe('stale-outputs');
  });
  // A working tree and the artifact Nx's local cache holds for the claim's
  // hash, laid out as Nx stores one: the outputs under their workspace paths.
  let root = '';
  const claim = { taskId: 'app:build', outputs: ['packages/app/dist'], hash: 'app-hash' };
  const workspace = () => join(root, 'workspace');
  const marker = () => join(workspace(), 'packages', 'app', 'dist', 'marker.txt');
  const artifacts = () => new Map([[claim.hash, { outputsPath: join(root, 'cache', claim.hash, 'outputs') }]]);
  const compare = () => compareWithArtifacts(workspace(), [claim], artifacts(), expandOutputs);

  beforeEach(async () => {
    root = await realpath(await mkdtemp(join(tmpdir(), 'ensure-built-artifact-')));
    for (const base of [workspace(), join(root, 'cache', claim.hash, 'outputs')]) {
      await mkdir(join(base, 'packages', 'app', 'dist'), { recursive: true });
      await writeFile(join(base, 'packages', 'app', 'dist', 'marker.txt'), 'built\nlib\n');
    }
  });

  afterEach(async () => {
    await rm(root, { recursive: true, force: true });
  });

  it('vouches for a working tree that restoring the artifact would leave unchanged', () => {
    expect(compare().kind).toBe('restorable');
  });

  it('never vouches for rewritten output bytes', async () => {
    // Same length, different bytes: only a content comparison can tell.
    await writeFile(marker(), 'BUILT\nLIB\n');
    expect(compare()).toEqual({ kind: 'stale-outputs', taskId: 'app:build' });
  });

  it('runs again once a recorded output is gone from the working tree', async () => {
    await rm(marker());
    expect(compare()).toEqual({ kind: 'stale-outputs', taskId: 'app:build' });
  });

  it('runs again when the cache holds no artifact for the hash', () => {
    expect(compareWithArtifacts(workspace(), [claim], new Map(), expandOutputs)).toEqual({
      kind: 'stale-outputs',
      taskId: 'app:build',
    });
  });

  it('notices an output written after it was compared', async () => {
    const verdict = compare();
    if (verdict.kind !== 'restorable') {
      throw new Error(`expected a restorable tree, got ${JSON.stringify(verdict)}`);
    }
    expect(firstMovedClaim(verdict.observed)).toBeNull();
    await writeFile(marker(), 'built\nlib\nlater\n');
    expect(firstMovedClaim(verdict.observed)).toBe('app:build');
  });
});

describe('workspaceFileChanges', () => {
  it("splits the disk's file table from the daemon's into created, updated and deleted files", () => {
    const daemon = [
      { file: 'packages/lib/source.txt', hash: 'lib' },
      { file: 'packages/lib/added.txt', hash: 'added' },
      { file: 'packages/app/source.txt', hash: 'app' },
    ];
    const disk = [
      { file: 'packages/lib/source.txt', hash: 'lib changed' },
      { file: 'packages/app/source.txt', hash: 'app' },
      { file: 'packages/lib/new.txt', hash: 'new' },
    ];
    expect(workspaceFileChanges(daemon, disk)).toEqual({
      created: ['packages/lib/new.txt'],
      updated: ['packages/lib/source.txt'],
      deleted: ['packages/lib/added.txt'],
    });
  });
});

describe('missBeforeProbe', () => {
  it("drops an outer Nx task's cache bypass, which describes that task rather than the nested target", () => {
    const outer = {
      NX_TASK_TARGET_PROJECT: 'nx-plugin',
      NX_TASK_TARGET_TARGET: 'test',
      NX_SKIP_NX_CACHE: 'true',
      NX_DISABLE_NX_CACHE: 'true',
    };
    const nested = withoutOuterTaskCacheBypass(outer);
    expect(nested).toEqual({ NX_TASK_TARGET_PROJECT: 'nx-plugin', NX_TASK_TARGET_TARGET: 'test' });
    expect(missBeforeProbe(nested, 'app:build')).toBeNull();
  });

  it('honors a cache bypass the caller asked for outside an Nx task child', () => {
    for (const env of [{ NX_SKIP_NX_CACHE: 'true' }, { NX_DISABLE_NX_CACHE: 'true' }]) {
      const caller = withoutOuterTaskCacheBypass(env);
      expect(caller).toEqual(env);
      expect(missBeforeProbe(caller, 'app:build')).toEqual({ kind: 'cache-disabled', taskId: 'app:build' });
    }
  });

  it('leaves current input and artifact evaluation to the probe', () => {
    expect(missBeforeProbe({}, 'app:build')).toBeNull();
  });
});

describe('describeMiss', () => {
  it('names the task in every reason', () => {
    const reasons = [
      describeMiss({ kind: 'uncacheable', taskId: 'app:build' }),
      describeMiss({ kind: 'unhashable', taskId: 'app:build' }),
      describeMiss({ kind: 'not-cached', taskId: 'app:build' }),
      describeMiss({ kind: 'cached-failure', taskId: 'app:build', code: 7 }),
      describeMiss({ kind: 'stale-outputs', taskId: 'app:build' }),
      describeMiss({ kind: 'cache-disabled', taskId: 'app:build' }),
      describeMiss({ kind: 'no-daemon', taskId: 'app:build' }),
    ];
    for (const reason of reasons) {
      expect(reason).toContain('app:build');
    }
    expect(reasons[3]).toContain('7');
  });
});

describe('cliExitOutcome', () => {
  const reason = { kind: 'no-daemon', taskId: 'app:build' } as const;

  it('treats a clean exit as a completed build', () => {
    expect(cliExitOutcome(reason, { code: 0, signal: null }, signalToCode)).toEqual({
      disposition: 'built',
      reason,
    });
  });

  it('forwards a nonzero exit code verbatim', () => {
    expect(cliExitOutcome(reason, { code: 3, signal: null }, signalToCode)).toEqual({
      disposition: 'failed',
      reason,
      exitCode: 3,
      signal: null,
    });
  });

  it('reports a signal as a signal, not as a plain failure', () => {
    expect(cliExitOutcome(reason, { code: null, signal: 'SIGTERM' }, signalToCode)).toEqual({
      disposition: 'failed',
      reason,
      exitCode: signalToCode('SIGTERM'),
      signal: 'SIGTERM',
    });
    expect(cliExitOutcome(reason, { code: null, signal: 'SIGINT' }, signalToCode).disposition).toBe('failed');
  });
});

describe('the environment Nx runs under', () => {
  const caller = {
    PATH: '/usr/bin',
    NX_SOCKET_DIR: '/tmp/other-sock',
    NX_DAEMON_SOCKET_DIR: '/tmp/other-daemon-sock',
    NX_WORKSPACE_DATA_DIRECTORY: '/shared/workspace-data',
    NX_CACHE_DIRECTORY: '/shared/cache',
  };
  let root = '';
  const workspace = () => join(root, 'workspace');
  const elsewhere = () => join(root, 'elsewhere');

  beforeEach(async () => {
    root = await realpath(await mkdtemp(join(tmpdir(), 'ensure-built-caller-')));
    await mkdir(join(workspace(), 'packages'), { recursive: true });
    await mkdir(elsewhere());
    await writeFile(join(workspace(), 'nx.json'), '{}');
  });

  afterEach(async () => {
    await rm(root, { recursive: true, force: true });
  });

  it("passes none of another workspace's socket, database or cache to the root's Nx", () => {
    // A devenv shell set up for another workspace, entering this root from outside it.
    const owns = callerOwnsWorkspace(caller, elsewhere(), workspace());
    expect(owns).toBe(false);
    expect(workspaceEnvironment(caller, workspace(), owns)).toEqual({
      PATH: '/usr/bin',
      NX_WORKSPACE_ROOT_PATH: workspace(),
      NX_USE_LOCAL: 'true',
    });
  });

  it('keeps the socket, database and cache of a caller standing in the root it was set up for', () => {
    const owns = callerOwnsWorkspace(caller, join(workspace(), 'packages'), workspace());
    expect(owns).toBe(true);
    expect(workspaceEnvironment({ ...caller, NX_USE_LOCAL: 'false' }, workspace(), owns)).toEqual({
      ...caller,
      NX_WORKSPACE_ROOT_PATH: workspace(),
      NX_USE_LOCAL: 'true',
    });
  });

  it("takes the caller's workspace from NX_WORKSPACE_ROOT_PATH, relative to where it stands, over its nx.json", () => {
    expect(callerOwnsWorkspace({ NX_WORKSPACE_ROOT_PATH: '../workspace' }, elsewhere(), workspace())).toBe(true);
    expect(callerOwnsWorkspace({ NX_WORKSPACE_ROOT_PATH: elsewhere() }, workspace(), workspace())).toBe(false);
  });

  it("leaves the caller's environment exactly as it was, whatever Nx set, changed or removed", () => {
    const callerEnv = { PATH: '/usr/bin', NX_SKIP_NX_CACHE: 'caller-skip', NX_DISABLE_NX_CACHE: 'caller-disable' };
    const env: NodeJS.ProcessEnv = {
      PATH: '/usr/bin',
      NX_WORKSPACE_ROOT_PATH: workspace(),
      NX_STREAM_OUTPUT: 'true',
      NX_SKIP_NX_CACHE: 'false',
    };
    assignEnvironment(env, callerEnv);
    expect(env).toEqual(callerEnv);
  });

  it('retains a real socket-owner mkdir refusal before any target can run', async () => {
    const blocker = join(root, 'ordinary-file');
    const marker = join(workspace(), 'target-ran');
    await writeFile(blocker, 'not a directory');
    await writeFile(
      join(workspace(), 'project.json'),
      JSON.stringify({
        name: 'app',
        targets: { build: { executor: 'nx:run-commands', options: { command: 'touch target-ran' } } },
      }),
    );
    const saved = { ...process.env };
    try {
      process.env.DEVENV_RUNTIME = join(blocker, 'runtime');
      process.env.NX_WORKSPACE_ROOT_PATH = elsewhere();
      process.env.NX_SOCKET_DIR = elsewhere();
      const result = await ensureBuilt({ target: 'app:build', cwd: workspace() });
      expect(result.disposition).toBe('failed');
      if (result.disposition !== 'failed') throw new Error('the socket owner must refuse before the target runs');
      expect(result.reason.kind).toBe('workspace-socket');
      if (result.reason.kind !== 'workspace-socket') throw new Error('the refusal must retain its socket owner');
      expect(result.reason.taskId).toBe('app:build');
      expect(result.reason.workspaceRoot).toBe(workspace());
      expect(result.reason.detail).toContain('socket owner ended with exit');
      expect(result.reason.detail).toContain('mkdir:');
      expect(result.reason.detail).toContain(blocker);
      expect(result.exitCode).not.toBe(0);
      expect(result.signal).toBeNull();
      expect(await readFile(blocker, 'utf8')).toBe('not a directory');
      await expect(readFile(marker)).rejects.toMatchObject({ code: 'ENOENT' });
    } finally {
      assignEnvironment(process.env, saved);
    }
  });
});

const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const repoRoot = dirname(dirname(packageRoot));
const binEntry = join(packageRoot, 'src', 'bin', 'smoo-nx-exec.ts');
const builtBinEntry = join(packageRoot, 'dist', 'bin', 'smoo-nx-exec.js');
const nxEntry = join(repoRoot, 'node_modules', '.bin', 'nx');
const MARKER = 'EXEC_OK';
/**
 * Reports what the exec'd process inherited: the marker proves execve
 * happened, the cwd that it kept the caller's directory, and the root path
 * that the wrapper took its Nx configuration back out of the environment.
 */
const REPORT = `#!/bin/sh\necho ${MARKER} "$@"\necho "cwd=$PWD"\necho "NX_WORKSPACE_ROOT_PATH=\${NX_WORKSPACE_ROOT_PATH:-unset}"\n`;

interface BinRun {
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
  readonly stdout: string;
  readonly stderr: string;
}

/**
 * This process's environment without the `NX_*` an outer
 * `nx run nx-plugin:test` exported to it (its cache bypass and task identity
 * would reconfigure the fixture's Nx), and without NO_COLOR, which beside that
 * outer Nx's FORCE_COLOR makes Bun warn on stderr, where a hit must be silent.
 */
function childEnvironment(env: Readonly<Record<string, string>>): Record<string, string> {
  const childEnv: Record<string, string> = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (value !== undefined && !key.startsWith('NX_') && key !== 'NO_COLOR') {
      childEnv[key] = value;
    }
  }
  return { ...childEnv, CI: '', ...env };
}

/**
 * The wrapper runs in a child process because `execve` replaces the process,
 * which is the behaviour under test.
 */
function run(
  runtime: 'bun' | 'node',
  entry: string,
  cwd: string,
  args: readonly string[],
  env: Readonly<Record<string, string>>,
): Promise<BinRun> {
  const child = spawn(runtime, [entry, ...args], {
    cwd,
    env: childEnvironment(env),
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '';
  let stderr = '';
  child.stdout.setEncoding('utf8').on('data', (text: string) => {
    stdout += text;
  });
  child.stderr.setEncoding('utf8').on('data', (text: string) => {
    stderr += text;
  });
  const { promise, resolve, reject } = Promise.withResolvers<BinRun>();
  child.once('error', reject);
  child.once('close', (code, signal) => resolve({ code, signal, stdout, stderr }));
  return promise;
}

async function writeExecutable(path: string, content: string): Promise<void> {
  await writeFile(path, content);
  await chmod(path, 0o755);
}

describe('smoo-nx-exec without a daemon', () => {
  // A workspace whose `nx` is a script standing in for the CLI. With the
  // daemon off, the wrapper loads no Nx at all and hands the target straight
  // to that CLI, so what runs here is the wrapper's own process handling —
  // stdio, exit status, signals, cwd, environment, execve — and no Nx.
  let workspace = '';
  const daemonless = { NX_DAEMON: 'false' };
  const wrapper = (cwd: string, args: readonly string[], env: Readonly<Record<string, string>> = {}) =>
    run('bun', binEntry, cwd, args, { ...daemonless, ...env });

  beforeAll(async () => {
    workspace = await nxFixtureRoot('ensure-built-cli-');
    await mkdir(join(workspace, 'node_modules', '.bin'), { recursive: true });
    await mkdir(join(workspace, 'packages', 'app'), { recursive: true });
    await writeFile(join(workspace, 'nx.json'), '{}');
    // `nx run <task> --outputStyle=stream`, as the wrapper starts it on a miss.
    await writeExecutable(
      join(workspace, 'node_modules', '.bin', 'nx'),
      '#!/bin/sh\necho "nx run $2"\ncase "$2" in\n  app:broken) exit 3 ;;\n  app:killed) kill -TERM $$ ;;\nesac\n',
    );
    await writeExecutable(join(workspace, 'report'), REPORT);
  });

  afterAll(async () => {
    if (workspace) {
      await rm(workspace, { recursive: true, force: true });
    }
  });

  it('shows the run on stderr, then execs the binary with stdout its own', async () => {
    const result = await wrapper(workspace, ['app:build', '--', './report', 'one']);
    expect(result.code, result.stderr).toBe(0);
    // Stdout is the exec'd binary's and holds nothing else, or `wrapper | jq` reads Nx's log as data.
    expect(result.stderr).toContain('nx run app:build');
    expect(result.stdout).toBe(`${MARKER} one\ncwd=${workspace}\nNX_WORKSPACE_ROOT_PATH=unset\n`);
  });

  it('without a binary, shows the run on stderr, says why it ran, and exits 0', async () => {
    const result = await wrapper(workspace, ['app:build'], { NX_VERBOSE_LOGGING: 'true' });
    expect(result.code, result.stderr).toBe(0);
    expect(result.stdout).toBe('');
    expect(result.stderr).toContain('nx run app:build');
    expect(result.stderr).toContain(
      `smoo-nx-exec: ran app:build because ${describeMiss({ kind: 'no-daemon', taskId: 'app:build' })}`,
    );
  });

  it("forwards a failing target's exit code and never execs", async () => {
    const result = await wrapper(workspace, ['app:broken', '--', './report']);
    expect(result.code).toBe(3);
    expect(result.stderr).toContain('nx run app:broken');
    expect(result.stdout).toBe('');
  });

  it('re-raises the signal that killed nx instead of flattening it to a code', async () => {
    const result = await wrapper(workspace, ['app:killed', '--', './report']);
    expect(result.signal).toBe('SIGTERM');
    expect(result.stdout).toBe('');
  });

  it("leaves the exec'd binary the caller's cwd, with the binary named relative to it", async () => {
    const app = join(workspace, 'packages', 'app');
    const result = await wrapper(app, ['app:build', '--workspace-root', workspace, '--', '../../report']);
    expect(result.code, result.stderr).toBe(0);
    expect(result.stdout).toBe(`${MARKER}\ncwd=${app}\nNX_WORKSPACE_ROOT_PATH=unset\n`);
  });

  it("hands the exec'd binary blocking stdio, so a pipe its output fills is never cut", async () => {
    // Node leaves a pipe O_NONBLOCK on macOS, and the flag belongs to the open file description, so
    // it survives execve: the binary's writes then fail with EAGAIN once the pipe holds 64 KiB and
    // nobody is reading. A reader that starts late is what makes the pipe fill.
    const flood = join(workspace, 'flood');
    await writeExecutable(flood, '#!/bin/sh\nhead -c 300000 /dev/zero\n');
    const piped = Bun.spawn(
      ['sh', '-c', `"$0" "$1" app:build -- ./flood | { sleep 1; tr -cd '\\000' | wc -c; }`, 'node', builtBinEntry],
      { cwd: workspace, env: childEnvironment(daemonless), stdout: 'pipe', stderr: 'pipe' },
    );
    const received = (await new Response(piped.stdout).text()).trim();
    expect(await piped.exited).toBe(0);
    expect(received).toBe('300000');
  });
});

/** Fixture Nx configuration: its cache and database stay under its removable root. */
const FIXTURE_NX_JSON = { useDaemonProcess: true, cacheDirectory: '.nx/cache' };

/**
 * Two projects, `app:build` reading `lib:build`'s outputs through
 * `dependentTasksOutputFiles`: Nx's runner-warmup hasher leaves such a task
 * unhashed, so a probe that used it instead of hashing every task would call
 * this graph 'unhashable' and never hit. Each lib build appends to `builds`,
 * outside the workspace: a log inside it would be an undeclared output the
 * snapshot diff rightly reports as a changed input.
 */
async function writeTwoProjectWorkspace(workspace: string, builds: string): Promise<void> {
  // Cowshed's scratch is ignored by its enclosing Git checkout. This is an
  // independent Nx workspace: its own Git boundary keeps the daemon's
  // ignore-aware watcher from dropping all of its source changes.
  expect(Bun.spawnSync(['git', 'init', '--quiet', workspace]).exitCode).toBe(0);
  await writeFile(join(workspace, '.gitignore'), '.nx\nnode_modules\npackages/*/dist\n');
  await mkdir(join(workspace, 'packages', 'app'), { recursive: true });
  await mkdir(join(workspace, 'packages', 'lib'), { recursive: true });
  // The repository's own node_modules, so the fixture resolves the Nx this package is written against.
  await symlink(join(repoRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
  await writeFile(join(workspace, 'nx.json'), JSON.stringify(FIXTURE_NX_JSON));
  await writeFile(
    join(workspace, 'packages', 'lib', 'project.json'),
    JSON.stringify({
      name: 'lib',
      targets: {
        build: {
          executor: 'nx:run-commands',
          cache: true,
          inputs: ['{projectRoot}/source.txt'],
          outputs: ['{projectRoot}/dist'],
          options: {
            command: `mkdir -p dist && cp source.txt dist/lib.txt && cat dist/lib.txt >> ${builds}`,
            cwd: '{projectRoot}',
          },
        },
      },
    }),
  );
  await writeFile(join(workspace, 'packages', 'lib', 'source.txt'), 'lib\n');
  await writeFile(
    join(workspace, 'packages', 'app', 'project.json'),
    JSON.stringify({
      name: 'app',
      targets: {
        build: {
          executor: 'nx:run-commands',
          cache: true,
          inputs: ['{projectRoot}/source.txt', { dependentTasksOutputFiles: '**/*' }],
          outputs: ['{projectRoot}/dist'],
          dependsOn: ['^build'],
          options: {
            command: 'mkdir -p dist && cat source.txt ../lib/dist/lib.txt > dist/marker.txt',
            cwd: '{projectRoot}',
          },
        },
      },
      implicitDependencies: ['lib'],
    }),
  );
  await writeFile(join(workspace, 'packages', 'app', 'source.txt'), 'built\n');
  await writeExecutable(join(workspace, 'report'), REPORT);
}

describe('smoo-nx-exec with a fresh daemon', () => {
  /**
   * The socket directory a call entered from another workspace's shell is given for `workspace`, or null where the
   * packaged socket owner has no managed shell runtime to bind in (`DEVENV_RUNTIME` unset), and Nx keeps its own
   * defaults. Found by running that same packaged script in the workspace, as the call does, with none of the
   * caller's Nx state: neither the directory nor how it is named is restated here.
   */
  const providerSocketDirectory = (workspace: string): string | null => {
    const runtime = process.env.DEVENV_RUNTIME;
    if (runtime === undefined || runtime === '') return null;
    expect(isAbsolute(runtime), `DEVENV_RUNTIME=${runtime} must name the managed shell runtime directory`).toBe(true);
    const script = join(managedAssetsRoot, 'raw/tooling/direnv/nx-socket-dir.sh');
    const owner = spawnSync('/bin/sh', ['-c', '. "$1" && printf %s "$NX_SOCKET_DIR"', 'sh', script], {
      cwd: workspace,
      env: { ...childEnvironment({}), PWD: workspace },
      encoding: 'utf8',
    });
    expect(owner.status, `${script} in ${workspace}\n${owner.error?.message ?? ''}${owner.stderr}`).toBe(0);
    expect(isAbsolute(owner.stdout), `${script} named ${JSON.stringify(owner.stdout)}`).toBe(true);
    return owner.stdout;
  };

  // The one test here that runs real Nx, because its subject is the daemon:
  // the daemon a probe starts holds no output records, so every output must be
  // vouched for by comparing the working tree with Nx's cache artifact. The
  // daemon it starts is also the one whose socket is in question, so the same
  // lifetime answers where a root entered from another workspace's shell binds.
  it('replays current outputs and refreshes stale snapshots without running unrelated edits', async () => {
    // A socket path has a 95 character budget and macOS's temp directory spends most of it, so the
    // socket directory is made in the short socket namespace the sandbox admits.
    const socketDir = await ownedSocketDirectory('eb-sock-', NX_LONGEST_SOCKET_NAME);
    let root = '';
    let failure: { readonly error: unknown } | null = null;
    const providerDirectories: string[] = [];
    try {
      await withNxFixture(
        'ensure-built-daemon-',
        async (fixture) => {
          root = fixture.root;
          const { workspace } = fixture;
          const builds = join(root, 'lib-builds.log');
          await writeTwoProjectWorkspace(workspace, builds);
          // What the provider binds for this workspace when the call is entered from another one's shell.
          const providerSockets = providerSocketDirectory(workspace);
          if (providerSockets !== null) providerDirectories.push(providerSockets);
          const ownEnv: Record<string, string> = providerSockets === null ? {} : { NX_SOCKET_DIR: providerSockets };

          // Built by daemonless Nx, as CI or a restarted daemon leaves a tree:
          // cached and on disk, recorded by no daemon.
          const built = await run(
            'node',
            nxEntry,
            workspace,
            ['run-many', '-t', 'build', '-p', 'app', '--outputStyle=static-failures-only'],
            { NX_DAEMON: 'false', NX_WORKSPACE_ROOT_PATH: workspace },
          );
          expect(built.code, built.stdout + built.stderr).toBe(0);
          expect(await readFile(builds, 'utf-8')).toBe('lib\n');

          // Entered from outside the root by a shell set up for another
          // workspace, as a managed devenv's is: its socket directory belongs to
          // that workspace's daemon, and a second daemon listening there would
          // take the socket from it.
          const caller = join(root, 'caller');
          await mkdir(caller);
          const hit = await run(
            'bun',
            binEntry,
            caller,
            ['app:build', '--workspace-root', workspace, '--', join(workspace, 'report')],
            { NX_DAEMON: 'true', NX_USE_LOCAL: 'true', NX_SOCKET_DIR: socketDir },
          );
          expect(hit.code, hit.stdout + hit.stderr).toBe(0);
          expect(hit.stderr).toBe('');
          expect(hit.stdout.split('\n')[0], hit.stdout).toBe(MARKER);
          expect(await readFile(builds, 'utf-8')).toBe('lib\n');
          expect(await readdir(socketDir)).toEqual([]);
          const record = await readFile(join(workspace, '.nx', 'workspace-data', 'd', 'server-process.json'), 'utf-8');
          expect(record).not.toContain(socketDir);
          // The daemon it started binds in the directory the provider named, not the caller's nor Nx's default.
          const daemon = typia.json.assertParse<DaemonProcessJson>(record);
          if (providerSockets !== null) {
            expect(daemon.socketPath.startsWith(`${providerSockets}/`), record).toBe(true);
          }

          // That hit re-armed the daemon's records. The emitted wrapper, under
          // Node and with nothing to exec, is as silent.
          const again = await run('node', builtBinEntry, workspace, ['app:build'], {
            NX_DAEMON: 'true',
            NX_USE_LOCAL: 'true',
            ...ownEnv,
          });
          expect(again).toEqual({ code: 0, signal: null, stdout: '', stderr: '' });
          expect(await readFile(builds, 'utf-8')).toBe('lib\n');

          // Probe-only receipts use the real Nx keys/cache and independently
          // census public output bytes without starting a task runner.
          const committed = spawnSync(
            'git',
            [
              '-C',
              workspace,
              'add',
              '.gitignore',
              'report',
              'nx.json',
              'packages/app/project.json',
              'packages/app/source.txt',
              'packages/lib/project.json',
              'packages/lib/source.txt',
            ],
            { encoding: 'utf8' },
          );
          expect(committed.status, committed.stdout + committed.stderr).toBe(0);
          const sourceCommit = spawnSync(
            'git',
            [
              '-C',
              workspace,
              '-c',
              'user.name=EnsureBuilt fixture',
              '-c',
              'user.email=fixture@example.invalid',
              'commit',
              '-qm',
              'source fixture',
            ],
            { encoding: 'utf8' },
          );
          expect(sourceCommit.status, sourceCommit.stdout + sourceCommit.stderr).toBe(0);
          const probeEntry = join(root, 'probe-receipt.ts');
          await writeFile(
            probeEntry,
            `
import { readFile, stat, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { probeBuilt } from ${JSON.stringify(join(packageRoot, 'dist', 'ensure-built.js'))};
const output = join(process.cwd(), 'packages/app/dist/marker.txt');
const source = join(process.cwd(), 'packages/lib/source.txt');
const originalOutput = await readFile(output, 'utf8');
const originalSource = await readFile(source, 'utf8');
const before = await stat(output, { bigint: true });
const hit = await probeBuilt({ target: 'app:build', cwd: process.cwd() });
const after = await stat(output, { bigint: true });
process.env.NX_DAEMON = 'false';
const daemonless = await probeBuilt({ target: 'app:build', cwd: process.cwd() });
process.env.NX_DAEMON = 'true';
await writeFile(output, 'tampered');
const stale = await probeBuilt({ target: 'app:build', cwd: process.cwd() });
await writeFile(output, originalOutput);
await writeFile(source, 'new uncached source');
const uncached = await probeBuilt({ target: 'app:build', cwd: process.cwd() });
const outputAfterMiss = await readFile(output, 'utf8');
await writeFile(source, originalSource);
console.log(JSON.stringify({ hit, daemonless, stale, uncached, outputUnwritten: before.ino === after.ino && before.mtimeNs === after.mtimeNs && before.ctimeNs === after.ctimeNs, outputAfterMiss }));
`,
          );
          const probed = await run('bun', probeEntry, workspace, [], {
            NX_DAEMON: 'true',
            NX_USE_LOCAL: 'true',
            NX_WORKSPACE_ROOT_PATH: workspace,
            ...ownEnv,
          });
          expect(probed.code, probed.stdout + probed.stderr).toBe(0);
          const receipts = typia.json.assertParse<{
            hit: import('./ensure-built.js').EnsureBuiltProbeResult;
            daemonless: import('./ensure-built.js').EnsureBuiltProbeResult;
            stale: import('./ensure-built.js').EnsureBuiltProbeResult;
            uncached: import('./ensure-built.js').EnsureBuiltProbeResult;
            outputUnwritten: boolean;
            outputAfterMiss: string;
          }>(probed.stdout);
          expect(receipts.hit.disposition, probed.stderr).toBe('hit');
          if (receipts.hit.disposition === 'hit') {
            expect(receipts.hit.receipt.tasks.map(({ id }) => id).sort()).toEqual(['app:build', 'lib:build']);
            expect(
              receipts.hit.receipt.tasks
                .flatMap(({ outputs }) => outputs)
                .find(({ path }) => path === 'packages/app/dist/marker.txt'),
            ).toMatchObject({ type: 'file', sha256: expect.stringMatching(/^[a-f0-9]{64}$/) });
          }
          expect(receipts.daemonless).toEqual({
            disposition: 'miss',
            reason: { kind: 'no-daemon', taskId: 'app:build' },
          });
          expect(receipts.stale).toEqual({
            disposition: 'miss',
            reason: { kind: 'stale-outputs', taskId: 'app:build' },
          });
          expect(receipts.uncached.disposition).toBe('miss');
          expect(receipts.outputUnwritten).toBe(true);
          expect(receipts.outputAfterMiss).toBe('built\nlib\n');
          expect(await readFile(builds, 'utf-8')).toBe('lib\n');

          const sourceRaceEntry = join(root, 'receipt-source-race.ts');
          await writeFile(
            sourceRaceEntry,
            `
import { createRequire } from 'node:module';
import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { probeBuilt } from ${JSON.stringify(join(packageRoot, 'dist', 'ensure-built.js'))};
const source = join(process.cwd(), 'packages/lib/source.txt');
const text = await readFile(source, 'utf8');
const { daemonClient } = createRequire(process.cwd() + '/package.json')('nx/src/daemon/client/client');
const hash = daemonClient.hashTasks.bind(daemonClient);
daemonClient.hashTasks = async (...args) => {
  const result = await hash(...args);
  await writeFile(source, 'transient source');
  await writeFile(source, text);
  return result;
};
console.log(JSON.stringify(await probeBuilt({ target: 'app:build', cwd: process.cwd() })));
`,
          );
          const sourceRace = await run('bun', sourceRaceEntry, workspace, [], {
            NX_DAEMON: 'true',
            NX_USE_LOCAL: 'true',
            NX_WORKSPACE_ROOT_PATH: workspace,
            ...ownEnv,
          });
          expect(sourceRace.code, sourceRace.stdout + sourceRace.stderr).toBe(0);
          const sourceRaceResult = typia.json.assertParse<import('./ensure-built.js').EnsureBuiltProbeResult>(
            sourceRace.stdout,
          );
          expect(sourceRaceResult).toMatchObject({
            disposition: 'failed',
            reason: { kind: 'provider-proof', detail: expect.stringContaining('across Nx hashing') },
          });
          expect(sourceRace.stderr).toContain('Provider source');
          expect(await readFile(builds, 'utf-8')).toBe('lib\n');

          // An awaited daemon graph can see B while both disk snapshots see
          // A. Node stamps revoke that generation; the fresh daemon file table
          // supplies the A reconciliation instead of an empty A-to-A diff.
          const generationRaceEntry = join(root, 'graph-generation-race.ts');
          await writeFile(
            generationRaceEntry,
            `
import { createRequire } from 'node:module';
import { readFile, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { ensureBuilt } from ${JSON.stringify(join(packageRoot, 'dist', 'ensure-built.js'))};
const source = join(process.cwd(), 'packages/lib/source.txt');
const text = await readFile(source, 'utf8');
await rm(join(process.cwd(), '.nx/workspace-data/ensure-built-graph.json'), { force: true });
const { daemonClient } = createRequire(process.cwd() + '/package.json')('nx/src/daemon/client/client');
const request = daemonClient.getProjectGraphAndSourceMaps.bind(daemonClient);
let requests = 0;
daemonClient.getProjectGraphAndSourceMaps = async () => {
  requests += 1;
  const result = await request();
  if (requests !== 1) return result;
  await writeFile(source, 'transient graph input');
  await daemonClient.updateWorkspaceContext([], ['packages/lib/source.txt'], []);
  const next = await request();
  await writeFile(source, text);
  return next;
};
const result = await ensureBuilt({ target: 'app:build', cwd: process.cwd() });
console.log(JSON.stringify({ result, requests }));
`,
          );
          const generationRace = await run('bun', generationRaceEntry, workspace, [], {
            NX_DAEMON: 'true',
            NX_USE_LOCAL: 'true',
            NX_WORKSPACE_ROOT_PATH: workspace,
            ...ownEnv,
          });
          expect(generationRace.code, generationRace.stdout + generationRace.stderr).toBe(0);
          const generations = typia.json.assertParse<{
            result: import('./ensure-built.js').EnsureBuiltResult;
            requests: number;
          }>(generationRace.stdout);
          expect(generations.result).toEqual({ disposition: 'hit' });
          expect(generations.requests).toBeGreaterThanOrEqual(2);
          expect(generationRace.stderr).toBe('');
          expect(await readFile(builds, 'utf-8')).toBe('lib\n');
          // Count real graph RPCs, without substituting any response. The
          // second call must use the graph witness; every membership/config
          // change must return to the daemon's current generation.
          const reuseEntry = join(root, 'graph-reuse.ts');
          await writeFile(
            reuseEntry,
            `
import { createRequire } from 'node:module';
import { chmod, readFile, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { ensureBuilt } from ${JSON.stringify(join(packageRoot, 'dist', 'ensure-built.js'))};
const { daemonClient } = createRequire(process.cwd() + '/package.json')('nx/src/daemon/client/client');
const request = daemonClient.getProjectGraphAndSourceMaps.bind(daemonClient);
let requests = 0;
daemonClient.getProjectGraphAndSourceMaps = async () => { requests += 1; return request(); };
const results = [];
async function check(label) {
  const before = requests;
  const result = await ensureBuilt({ target: 'app:build', cwd: process.cwd() });
  results.push({ label, requests: requests - before, disposition: result.disposition });
}
await check('establish');
await check('reuse');
const added = join(process.cwd(), 'new-graph-input.txt');
await writeFile(added, 'new');
await check('created');
await rm(added);
await check('deleted');
const project = join(process.cwd(), 'packages/app/project.json');
const original = await readFile(project, 'utf8');
const config = JSON.parse(original);
config.implicitDependencies = [];
await writeFile(project, JSON.stringify(config));
await check('dependency');
await writeFile(project, original);
await check('dependency-restored');
const witness = join(process.cwd(), '.nx/workspace-data/ensure-built-graph.json');
await rm(witness);
await check('missing-witness');
await chmod(witness, 0);
try { await check('unreadable-witness'); } finally { await chmod(witness, 0o600); }
console.log(JSON.stringify(results));
`,
          );
          const reused = await run('bun', reuseEntry, workspace, [], {
            NX_DAEMON: 'true',
            NX_USE_LOCAL: 'true',
            NX_WORKSPACE_ROOT_PATH: workspace,
            ...ownEnv,
          });
          expect(reused.code, reused.stdout + reused.stderr).toBe(0);
          const graphChecks = typia.json.assertParse<{ label: string; requests: number; disposition: string }[]>(
            reused.stdout,
          );
          expect(graphChecks.find(({ label }) => label === 'reuse')).toEqual({
            label: 'reuse',
            requests: 0,
            disposition: 'hit',
          });
          for (const label of [
            'created',
            'deleted',
            'dependency',
            'dependency-restored',
            'missing-witness',
            'unreadable-witness',
          ]) {
            expect(graphChecks.find((check) => check.label === label)?.requests, label).toBeGreaterThan(0);
          }
          expect(await readFile(join(workspace, 'packages/app/dist/marker.txt'), 'utf8')).toBe('built\nlib\n');
          // Coordinate a real write after the real daemon returned its file
          // table. The response is not substituted: this makes the
          // graph→snapshot race deterministic without relying on FSEvents.
          const raceEntry = join(root, 'freshness-race.ts');
          await writeFile(
            raceEntry,
            `
import { createRequire } from 'node:module';
import { writeFile } from 'node:fs/promises';
import { ensureBuilt } from ${JSON.stringify(join(packageRoot, 'dist', 'ensure-built.js'))};
const { daemonClient } = createRequire(process.cwd() + '/package.json')('nx/src/daemon/client/client');
const readFiles = daemonClient.getWorkspaceContextFileData.bind(daemonClient);
daemonClient.getWorkspaceContextFileData = async () => {
  const files = await readFiles();
  await writeFile(process.argv[2], process.argv[3]);
  return files;
};
console.log(JSON.stringify(await ensureBuilt({ target: 'app:build', cwd: process.cwd() })));
`,
          );
          const refreshed = await run(
            'bun',
            raceEntry,
            workspace,
            [join(workspace, 'README.md'), 'an unrelated edit\n'],
            { NX_DAEMON: 'true', NX_USE_LOCAL: 'true', NX_WORKSPACE_ROOT_PATH: workspace, ...ownEnv },
          );
          expect(refreshed.code, refreshed.stdout + refreshed.stderr).toBe(0);
          expect(refreshed.stdout.trim()).toBe(JSON.stringify({ disposition: 'hit' }));
          expect(refreshed.stderr).toBe('');
          expect(await readFile(builds, 'utf-8')).toBe('lib\n');

          // STRICT-BY: the same boundary moving an actual declared input must
          // miss, run its producer, and rebuild the dependent artifact.
          const changed = await run(
            'bun',
            raceEntry,
            workspace,
            [join(workspace, 'packages', 'lib', 'source.txt'), 'changed\n'],
            { NX_DAEMON: 'true', NX_USE_LOCAL: 'true', NX_WORKSPACE_ROOT_PATH: workspace, ...ownEnv },
          );
          expect(changed.code, changed.stdout + changed.stderr).toBe(0);
          expect(changed.stdout).toContain('"disposition":"built"');
          expect(await readFile(builds, 'utf-8')).toBe('lib\nchanged\n');
          expect(await readFile(join(workspace, 'packages', 'app', 'dist', 'marker.txt'), 'utf-8')).toBe(
            'built\nchanged\n',
          );
        },
        'workspace',
      );
    } catch (error) {
      failure = { error };
    }
    // Only the directories this test made: the caller's, and the one the provider bound for this workspace.
    for (const directory of [socketDir, ...providerDirectories]) {
      await rm(directory, { recursive: true, force: true });
    }
    // The fixture retires on every exit path of its body, a throwing one
    // included: once it has, nothing works in its root.
    const leftovers = root === '' ? [] : await pidsWorkingIn(root);
    if (failure === null) {
      expect(leftovers).toEqual([]);
    } else if (leftovers.length === 0) {
      throw failure.error;
    } else {
      // A cause, not an AggregateError: the test harness prints an aggregate's
      // inner errors without its message.
      throw new Error(`processes ${leftovers.join(', ')} still work in ${root} after its body failed`, {
        cause: failure.error,
      });
    }
  }, 60_000);
});
