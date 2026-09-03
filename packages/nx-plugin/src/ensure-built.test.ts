import { afterAll, beforeAll, describe, expect, it } from 'bun:test';
import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { chmod, mkdir, mkdtemp, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import type { Task } from 'nx/src/config/task-graph';

import { signalToCode } from 'nx/src/utils/exit-codes';

import {
  cliExitOutcome,
  describeMiss,
  firstCacheMiss,
  firstStaleOutputs,
  parseTargetSelector,
} from './ensure-built.js';

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

describe('firstStaleOutputs', () => {
  const tasks = [task({ id: 'lib:build', outputs: ['lib/dist'] }), task({ id: 'app:build', outputs: ['app/dist'] })];

  it('clears outputs the daemon vouches for', () => {
    expect(firstStaleOutputs(tasks, [true, true])).toBeNull();
  });

  it('reports the task whose outputs no longer match', () => {
    expect(firstStaleOutputs(tasks, [true, false])).toEqual({ kind: 'stale-outputs', taskId: 'app:build' });
  });

  it('treats a missing verdict as stale rather than as a hit', () => {
    expect(firstStaleOutputs(tasks, [true])).toEqual({ kind: 'stale-outputs', taskId: 'app:build' });
    expect(firstStaleOutputs(tasks, [])).toEqual({ kind: 'stale-outputs', taskId: 'lib:build' });
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

const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const repoRoot = dirname(dirname(packageRoot));
const binEntry = join(packageRoot, 'src', 'bin', 'ensure-built.ts');
const builtBinEntry = join(packageRoot, 'dist', 'bin', 'ensure-built.js');
const MARKER = 'EXEC_OK';
/** Nx configures itself through these; the exec'd binary must not inherit them. */
const LEAKABLE = ['NX_WORKSPACE_ROOT_PATH', 'NX_STREAM_OUTPUT', 'NX_PREFIX_OUTPUT', 'NX_LOAD_DOT_ENV_FILES'];

interface BinRun {
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
  readonly stdout: string;
  readonly stderr: string;
}

/**
 * The bin is exercised in a child process rather than by calling `ensureBuilt`
 * directly, for two reasons that are not incidental: Nx binds its workspace
 * root once per process, so a single test process cannot probe a fixture
 * workspace and then anything else; and `execve` replaces the process, which is
 * the behaviour under test.
 */
function runBinWith(
  runtime: 'bun' | 'node',
  entry: string,
  workspace: string,
  args: readonly string[],
  env: Record<string, string>,
): Promise<BinRun> {
  const childEnv = { ...process.env };
  for (const key of LEAKABLE) {
    delete childEnv[key];
  }
  // The parent Nx process may set FORCE_COLOR while the shell exports
  // NO_COLOR. Passing both to Bun emits a warning, which would make a genuine
  // full hit look noisy.
  delete childEnv.NO_COLOR;
  Object.assign(childEnv, { CI: '', NX_DAEMON: 'true' }, env);
  const child = spawn(runtime, [entry, ...args], {
    cwd: workspace,
    env: childEnv,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '';
  let stderr = '';
  child.stdout.on('data', (chunk: Buffer) => {
    stdout += chunk.toString();
  });
  child.stderr.on('data', (chunk: Buffer) => {
    stderr += chunk.toString();
  });
  // `Promise.withResolvers` would read better but needs lib es2024; this
  // package inherits lib es2022 from tsconfig.base.json.
  return new Promise((settle, reject) => {
    child.once('error', reject);
    child.once('close', (code, signal) => settle({ code, signal, stdout, stderr }));
  });
}

function runBin(workspace: string, args: readonly string[], env: Record<string, string> = {}): Promise<BinRun> {
  return runBinWith('bun', binEntry, workspace, args, env);
}

function runBuiltBin(workspace: string, args: readonly string[]): Promise<BinRun> {
  return runBinWith('node', builtBinEntry, workspace, args, {});
}

function nx(workspace: string, args: readonly string[]): Promise<number | null> {
  const child = spawn(join(repoRoot, 'node_modules', '.bin', 'nx'), [...args], {
    cwd: workspace,
    env: { ...process.env, CI: '', NX_DAEMON: 'true', NX_WORKSPACE_ROOT_PATH: workspace },
    stdio: 'ignore',
  });
  return new Promise((settle, reject) => {
    child.once('error', reject);
    child.once('close', (code) => settle(code));
  });
}

describe('smoothbricks-ensure-built', () => {
  let workspace = '';
  const report = () => join(workspace, 'report');
  const marker = () => join(workspace, 'packages', 'app', 'dist', 'marker.txt');

  beforeAll(async () => {
    // Realpath because macOS puts the temp directory behind a /private
    // symlink, and a process's reported cwd is the resolved one.
    workspace = await realpath(await mkdtemp(join(tmpdir(), 'ensure-built-')));
    await mkdir(join(workspace, 'packages', 'app'), { recursive: true });
    await mkdir(join(workspace, 'packages', 'lib'), { recursive: true });
    // The repository's own node_modules, so the fixture resolves the same Nx
    // this package is written against, including node_modules/.bin/nx for the
    // daemon-disabled path.
    await symlink(join(repoRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
    await writeFile(join(workspace, 'nx.json'), JSON.stringify({ useDaemonProcess: true }));
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
            options: { command: 'mkdir -p dist && cat source.txt > dist/lib.txt', cwd: '{projectRoot}' },
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
            // `dependentTasksOutputFiles` is the discriminating part of this
            // fixture: Nx's runner-warmup hasher deliberately leaves such a
            // task unhashed, so a probe that used it instead of hashing every
            // task would classify this graph 'unhashable' and never hit.
            inputs: ['{projectRoot}/source.txt', { dependentTasksOutputFiles: '**/*' }],
            outputs: ['{projectRoot}/dist'],
            dependsOn: ['^build'],
            options: { command: 'mkdir -p dist && cat source.txt > dist/marker.txt', cwd: '{projectRoot}' },
          },
          broken: {
            executor: 'nx:run-commands',
            cache: true,
            options: { command: 'exit 3', cwd: '{projectRoot}' },
          },
        },
        implicitDependencies: ['lib'],
      }),
    );
    await writeFile(join(workspace, 'packages', 'app', 'source.txt'), 'built\n');
    // Reports what the exec'd process actually inherited: the marker proves
    // execve happened, the cwd proves it kept the caller's directory, and the
    // NX_ lines prove Nx's process-global configuration did not leak into it.
    await writeFile(
      report(),
      `#!/bin/sh\necho ${MARKER} "$@"\necho "cwd=$PWD"\n${LEAKABLE.map((key) => `echo "${key}=\${${key}:-unset}"`).join('\n')}\n`,
    );
    await chmod(report(), 0o755);
  });

  afterAll(async () => {
    if (workspace) {
      await nx(workspace, ['daemon', '--stop']);
      await rm(workspace, { recursive: true, force: true });
    }
  });

  it('builds the whole dependency graph on the first run, then execs the binary', async () => {
    const run = await runBin(workspace, ['app:build', '--', './report', 'one']);
    expect(run.code).toBe(0);
    expect(run.stdout).toContain(`${MARKER} one`);
    expect(existsSync(marker())).toBe(true);
    expect(existsSync(join(workspace, 'packages', 'lib', 'dist', 'lib.txt'))).toBe(true);
  });

  it('stays completely silent on a full cache hit', async () => {
    const run = await runBin(workspace, ['app:build', '--', './report']);
    expect(run.code).toBe(0);
    expect(run.stderr).toBe('');
    // Nothing but the exec'd binary's own output: no Nx banner, no task log.
    expect(run.stdout.split('\n')[0]).toBe(MARKER);
    expect(run.stdout).not.toContain('nx run');
  });

  it('runs the emitted wrapper under Node without losing the hot path', async () => {
    const run = await runBuiltBin(workspace, ['app:build', '--', './report']);
    expect(run.code).toBe(0);
    expect(run.stderr).toBe('');
    expect(run.stdout.split('\n')[0]).toBe(MARKER);
  });

  it('leaves the exec\u0027d binary the caller\u0027s cwd and none of Nx\u0027s environment', async () => {
    // Invoked from a subdirectory, with the binary named relative to it: the
    // resolution the wrapper's users actually perform.
    const run = await runBin(join(workspace, 'packages', 'app'), [
      'app:build',
      '--workspace-root',
      workspace,
      '--',
      '../../report',
    ]);
    expect(run.code).toBe(0);
    expect(run.stdout).toContain(`cwd=${join(workspace, 'packages', 'app')}`);
    for (const key of LEAKABLE) {
      expect(run.stdout).toContain(`${key}=unset`);
    }
  });

  it('runs again once a recorded output is gone from the working tree', async () => {
    await rm(marker());
    const run = await runBin(workspace, ['app:build', '--', './report'], { NX_VERBOSE_LOGGING: 'true' });
    expect(run.code).toBe(0);
    expect(run.stderr).toContain('outputs are missing or modified on disk');
    expect(run.stdout).toContain(MARKER);
    expect(existsSync(marker())).toBe(true);
  });

  it('runs again when a dependency\u0027s own inputs change', async () => {
    await writeFile(join(workspace, 'packages', 'lib', 'source.txt'), 'lib changed\n');
    const changed = await runBin(workspace, ['app:build', '--', './report'], { NX_VERBOSE_LOGGING: 'true' });
    expect(changed.code).toBe(0);
    expect(changed.stderr).toContain('lib:build has no cached result');

    // And the graph settles back to silence, which is only reachable if the
    // dependent-outputs task was hashed too.
    const settled = await runBin(workspace, ['app:build', '--', './report']);
    expect(settled.code).toBe(0);
    expect(settled.stderr).toBe('');
    expect(settled.stdout.split('\n')[0]).toBe(MARKER);
  });

  it('forwards a failing target\u0027s exit code and never execs', async () => {
    const run = await runBin(workspace, ['app:broken', '--', './report']);
    expect(run.code).toBe(1);
    expect(run.stdout).not.toContain(MARKER);
  });

  it('falls back to the workspace nx CLI when the daemon is disabled', async () => {
    const run = await runBin(workspace, ['app:build', '--', './report'], {
      NX_DAEMON: 'false',
      NX_VERBOSE_LOGGING: 'true',
    });
    expect(run.code).toBe(0);
    expect(run.stderr).toContain('daemon is disabled');
    expect(run.stdout).toContain(MARKER);
  });

  it('prints the message from a plain-object Nx rejection', async () => {
    const rejectingWorkspace = await realpath(await mkdtemp(join(tmpdir(), 'ensure-built-rejection-')));
    try {
      await mkdir(join(rejectingWorkspace, 'node_modules', 'nx', 'src', 'daemon', 'client'), { recursive: true });
      await mkdir(join(rejectingWorkspace, 'node_modules', 'nx', 'src', 'utils'), { recursive: true });
      await writeFile(join(rejectingWorkspace, 'package.json'), '{}');
      await writeFile(join(rejectingWorkspace, 'nx.json'), '{}');
      await writeFile(
        join(rejectingWorkspace, 'node_modules', 'nx', 'package.json'),
        JSON.stringify({ name: 'nx', type: 'commonjs' }),
      );
      await writeFile(
        join(rejectingWorkspace, 'node_modules', 'nx', 'src', 'utils', 'workspace-root.js'),
        'exports.workspaceRoot = process.env.NX_WORKSPACE_ROOT_PATH;\n',
      );
      await writeFile(
        join(rejectingWorkspace, 'node_modules', 'nx', 'src', 'daemon', 'client', 'client.js'),
        [
          'exports.daemonClient = {',
          '  enabled() {',
          "    throw { stack: 'synthetic stack', message: 'daemon belongs to a different workspace' };",
          '  },',
          '};',
          '',
        ].join('\n'),
      );

      const run = await runBin(rejectingWorkspace, ['app:build', '--', './never-runs']);
      expect(run.code).toBe(1);
      expect(run.stderr).toContain('daemon belongs to a different workspace');
      expect(run.stderr).not.toContain('[object Object]');
    } finally {
      await rm(rejectingWorkspace, { recursive: true, force: true });
    }
  });

  it('rejects a malformed invocation with a usage error', async () => {
    const missingSeparator = await runBin(workspace, ['app:build']);
    expect(missingSeparator.code).toBe(2);
    expect(missingSeparator.stderr).toContain('missing `--`');

    const badTarget = await runBin(workspace, ['build', '--', './report']);
    expect(badTarget.code).toBe(2);
    expect(badTarget.stderr).toContain('not a project:target');

    const pathLookup = await runBin(workspace, ['app:build', '--', 'report']);
    expect(pathLookup.code).toBe(2);
    expect(pathLookup.stderr).toContain('not a name to look up on PATH');
  });
});

describe('smoothbricks-ensure-built signal forwarding', () => {
  let workspace = '';

  // A workspace whose `nx` is a script that kills itself. The daemon-disabled
  // path spawns exactly that binary, so this is the whole signal path end to
  // end — and deterministic, unlike racing a real build with a kill.
  beforeAll(async () => {
    workspace = await realpath(await mkdtemp(join(tmpdir(), 'ensure-built-signal-')));
    await mkdir(join(workspace, 'node_modules', '.bin'), { recursive: true });
    await writeFile(join(workspace, 'nx.json'), '{}');
    const fakeNx = join(workspace, 'node_modules', '.bin', 'nx');
    await writeFile(fakeNx, '#!/bin/sh\nkill -TERM $$\n');
    await chmod(fakeNx, 0o755);
    await writeFile(join(workspace, 'report'), `#!/bin/sh\necho ${MARKER}\n`);
    await chmod(join(workspace, 'report'), 0o755);
  });

  afterAll(async () => {
    if (workspace) {
      await rm(workspace, { recursive: true, force: true });
    }
  });

  it('re-raises the signal that killed nx instead of flattening it to a code', async () => {
    const run = await runBin(workspace, ['app:build', '--', './report'], { NX_DAEMON: 'false' });
    expect(run.signal).toBe('SIGTERM');
    expect(run.stdout).not.toContain(MARKER);
  });
});
