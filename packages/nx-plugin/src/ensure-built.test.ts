import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'bun:test';
import { spawn } from 'node:child_process';
import { chmod, mkdir, mkdtemp, readdir, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import type { ProjectGraph } from 'nx/src/config/project-graph';
import type { Task } from 'nx/src/config/task-graph';
import { expandOutputs, matchOutputPaths } from 'nx/src/native';
import { getExecutorNameForTask } from 'nx/src/tasks-runner/utils';
import { signalToCode } from 'nx/src/utils/exit-codes';
import { nxFixtureRoot, withNxFixture } from './__tests__/fixture-nx-env.js';
import {
  assignEnvironment,
  callerOwnsWorkspace,
  changesOutsideOutputs,
  cliExitOutcome,
  compareWithArtifacts,
  describeError,
  describeMiss,
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
import { pidsWorkingIn } from './testing.js';

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

describe('changesOutsideOutputs', () => {
  const tasks = [
    task({ id: 'lib:build', outputs: ['packages/lib/dist'] }),
    task({ id: 'app:build', outputs: ['packages/app/dist'] }),
  ];

  it("does not count a build's own output writes as changed inputs", () => {
    expect(
      changesOutsideOutputs(tasks, ['packages/lib/dist/lib.txt', 'packages/app/dist/marker.txt'], matchOutputPaths),
    ).toBe(false);
  });

  it('counts a dependency input added, edited or deleted outside every declared output', () => {
    expect(
      changesOutsideOutputs(tasks, ['packages/app/dist/marker.txt', 'packages/lib/source.txt'], matchOutputPaths),
    ).toBe(true);
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
    expect(missBeforeProbe(nested, false, 'app:build')).toBeNull();
  });

  it('honors a cache bypass the caller asked for outside an Nx task child', () => {
    for (const env of [{ NX_SKIP_NX_CACHE: 'true' }, { NX_DISABLE_NX_CACHE: 'true' }]) {
      const caller = withoutOuterTaskCacheBypass(env);
      expect(caller).toEqual(env);
      expect(missBeforeProbe(caller, false, 'app:build')).toEqual({ kind: 'cache-disabled', taskId: 'app:build' });
    }
  });

  it('runs the target when an input changed that the daemon has not seen yet', () => {
    expect(missBeforeProbe({}, true, 'app:build')).toEqual({ kind: 'stale-inputs', taskId: 'app:build' });
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
      describeMiss({ kind: 'stale-inputs', taskId: 'app:build' }),
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
    });
  });

  it('keeps the socket, database and cache of a caller standing in the root it was set up for', () => {
    const owns = callerOwnsWorkspace(caller, join(workspace(), 'packages'), workspace());
    expect(owns).toBe(true);
    expect(workspaceEnvironment(caller, workspace(), owns)).toEqual({
      ...caller,
      NX_WORKSPACE_ROOT_PATH: workspace(),
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
  // The one test here that runs real Nx, because its subject is the daemon:
  // the daemon a probe starts holds no output records, so every output must be
  // vouched for by comparing the working tree with Nx's cache artifact. The
  // daemon it starts is also the one whose socket is in question, so the same
  // lifetime answers where a root entered from another workspace's shell binds.
  it("replays outputs a fresh daemon holds no record of, on the root's own socket, running nothing", async () => {
    // Under /tmp, not the platform temp directory: a socket path has a 95
    // character budget and macOS's temp directory spends half of it.
    const socketDir = await mkdtemp(join('/tmp', 'eb-sock-'));
    let root = '';
    let failure: { readonly error: unknown } | null = null;
    try {
      await withNxFixture(
        'ensure-built-daemon-',
        async (fixture) => {
          root = fixture.root;
          const { workspace } = fixture;
          const builds = join(root, 'lib-builds.log');
          await writeTwoProjectWorkspace(workspace, builds);

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

          // That hit re-armed the daemon's records. The emitted wrapper, under
          // Node and with nothing to exec, is as silent.
          const again = await run('node', builtBinEntry, workspace, ['app:build'], {
            NX_DAEMON: 'true',
            NX_USE_LOCAL: 'true',
          });
          expect(again).toEqual({ code: 0, signal: null, stdout: '', stderr: '' });
          expect(await readFile(builds, 'utf-8')).toBe('lib\n');
        },
        'workspace',
      );
    } catch (error) {
      failure = { error };
    }
    await rm(socketDir, { recursive: true, force: true });
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
