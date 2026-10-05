import { afterAll, beforeAll, describe, expect, it } from 'bun:test';
import { spawn } from 'node:child_process';
import { chmod, mkdir, mkdtemp, readdir, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { stripVTControlCharacters } from 'node:util';

import type { Task } from 'nx/src/config/task-graph';

import { signalToCode } from 'nx/src/utils/exit-codes';
import { nxFixtureRoot, retireNxFixture } from './__tests__/fixture-nx-env.js';
import { cliExitOutcome, describeMiss, firstCacheMiss, parseTargetSelector, unvouched } from './ensure-built.js';

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

const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const repoRoot = dirname(dirname(packageRoot));
const binEntry = join(packageRoot, 'src', 'bin', 'smoo-nx-exec.ts');
const builtBinEntry = join(packageRoot, 'dist', 'bin', 'smoo-nx-exec.js');
const MARKER = 'EXEC_OK';
/**
 * Caller directory overrides are deliberately stripped below. The fixture's
 * own Nx configuration keeps both its cache and database under its removable
 * root, even when smoo-nx-exec enters it from another workspace.
 */
const FIXTURE_NX_JSON = { useDaemonProcess: true, cacheDirectory: '.nx/cache' };
/** Nx keys controlled and reported by the fixture rather than inherited from its parent test task. */
const NX_ENV_KEYS = [
  'NX_WORKSPACE_ROOT_PATH',
  'NX_STREAM_OUTPUT',
  'NX_PREFIX_OUTPUT',
  'NX_LOAD_DOT_ENV_FILES',
  'NX_SKIP_NX_CACHE',
  'NX_DISABLE_NX_CACHE',
  'NX_CACHE_DIRECTORY',
  'NX_WORKSPACE_DATA_DIRECTORY',
];
const SHARED_NX_DIRECTORY_KEYS = ['NX_CACHE_DIRECTORY', 'NX_WORKSPACE_DATA_DIRECTORY'] as const;
/**
 * The caller's socket directory names one workspace's daemon, and Nx takes it
 * literally. A fixture never inherits it, so a daemon it starts or stops can
 * only touch its own socket; a test that wants one passes it explicitly.
 */
const NX_SOCKET_ENV_KEYS = ['NX_SOCKET_DIR', 'NX_DAEMON_SOCKET_DIR'] as const;

interface BinRun {
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
  readonly stdout: string;
  readonly stderr: string;
}

function fixtureNxEnvironment(env: Readonly<NodeJS.ProcessEnv> = {}): NodeJS.ProcessEnv {
  const childEnv = { ...process.env };
  for (const key of NX_ENV_KEYS) {
    delete childEnv[key];
  }
  for (const key of NX_SOCKET_ENV_KEYS) {
    delete childEnv[key];
  }
  // The parent Nx process may set FORCE_COLOR while the shell exports
  // NO_COLOR. Passing both to Bun emits a warning, which would make a genuine
  // full hit look noisy.
  delete childEnv.NO_COLOR;
  childEnv.CI = '';
  childEnv.NX_DAEMON = 'true';
  for (const [key, value] of Object.entries(env)) {
    if (value === undefined) {
      delete childEnv[key];
    } else {
      childEnv[key] = value;
    }
  }
  // CI shares these directories among the outer Nx tasks. A fixture Nx using
  // either one can replace the orchestrator's project graph while sibling
  // tasks are still running.
  for (const key of SHARED_NX_DIRECTORY_KEYS) {
    delete childEnv[key];
  }
  return childEnv;
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
  env: Readonly<NodeJS.ProcessEnv>,
): Promise<BinRun> {
  const childEnv = fixtureNxEnvironment(env);
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

function runBin(workspace: string, args: readonly string[], env: Readonly<NodeJS.ProcessEnv> = {}): Promise<BinRun> {
  return runBinWith('bun', binEntry, workspace, args, env);
}

function runBuiltBin(workspace: string, args: readonly string[]): Promise<BinRun> {
  return runBinWith('node', builtBinEntry, workspace, args, {});
}

function nx(workspace: string, args: readonly string[]): Promise<BinRun> {
  return runBinWith('node', join(repoRoot, 'node_modules', '.bin', 'nx'), workspace, args, {
    NX_WORKSPACE_ROOT_PATH: workspace,
  });
}

describe('smoo-nx-exec', () => {
  let workspace = '';
  const report = () => join(workspace, 'report');
  const marker = () => join(workspace, 'packages', 'app', 'dist', 'marker.txt');
  // Outside the workspace: a log inside it would be an undeclared output the
  // snapshot diff rightly reports as a changed input.
  const builds = () => `${workspace}-lib-builds.log`;
  const ships = () => `${workspace}-app-ships.log`;

  beforeAll(async () => {
    workspace = await nxFixtureRoot('ensure-built-');
    // Cowshed's scratch is ignored by its enclosing Git checkout. This is an
    // independent Nx workspace: its own Git boundary keeps the daemon's
    // ignore-aware watcher from dropping all of its source changes.
    const initialized = Bun.spawnSync(['git', 'init', '--quiet', workspace]);
    expect(initialized.exitCode).toBe(0);
    await mkdir(join(workspace, 'packages', 'app'), { recursive: true });
    await mkdir(join(workspace, 'packages', 'lib'), { recursive: true });
    // The repository's own node_modules, so the fixture resolves the same Nx
    // this package is written against, including node_modules/.bin/nx for the
    // daemon-disabled path.
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
            inputs: ['{projectRoot}/source*.txt'],
            outputs: ['{projectRoot}/dist'],
            options: {
              command: `mkdir -p dist && cat source*.txt > dist/lib.txt && cat dist/lib.txt >> ${builds()}`,
              cwd: '{projectRoot}',
            },
          },
        },
      }),
    );
    // Digit-suffixed so `source*.txt` expands in the same order under every
    // locale: en_US.UTF-8 collation ignores punctuation at the first level and
    // would put `source2.txt` before `source.txt`.
    await writeFile(join(workspace, 'packages', 'lib', 'source1.txt'), 'lib\n');
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
            options: {
              command: 'mkdir -p dist && cat source.txt ../lib/dist/lib.txt > dist/marker.txt',
              cwd: '{projectRoot}',
            },
          },
          broken: {
            executor: 'nx:run-commands',
            cache: true,
            options: { command: 'exit 3', cwd: '{projectRoot}' },
          },
          // A command-less aggregate, as Nx normalizes any target that only
          // names dependencies. It declares no `cache`, so it is uncacheable.
          stage: {
            executor: 'nx:noop',
            dependsOn: ['build'],
          },
          // A cacheable leaf that reaches its build only through the aggregate.
          ship: {
            executor: 'nx:run-commands',
            cache: true,
            inputs: ['{projectRoot}/source.txt'],
            dependsOn: ['stage'],
            options: { command: `cat dist/marker.txt >> ${ships()}`, cwd: '{projectRoot}' },
          },
        },
        implicitDependencies: ['lib'],
      }),
    );
    await writeFile(join(workspace, 'packages', 'app', 'source.txt'), 'built\n');
    // Reports what the exec'd process actually inherited: the marker proves
    // execve happened, the cwd proves it kept the caller's directory, and the
    // NX_ lines prove temporary Nx configuration was removed or restored.
    await writeFile(
      report(),
      `#!/bin/sh\necho ${MARKER} "$@"\necho "cwd=$PWD"\n${NX_ENV_KEYS.map((key) => `echo "${key}=\${${key}:-unset}"`).join('\n')}\n`,
    );
    await chmod(report(), 0o755);
  });

  afterAll(async () => {
    if (workspace) {
      await retireNxFixture({ root: workspace, workspace });
      await rm(builds(), { force: true });
      await rm(ships(), { force: true });
    }
  });

  it('reuses the normal Nx CLI cache before executing the binary', async () => {
    const built = await nx(workspace, ['run-many', '-t', 'build', '-p', 'app', '--outputStyle=static-failures-only']);
    expect(built.code, built.stdout + built.stderr).toBe(0);
    expect(await readFile(marker(), 'utf-8')).toBe('built\nlib\n');
    expect(await readFile(builds(), 'utf-8')).toBe('lib\n');

    const run = await runBin(workspace, ['app:build', '--', './report', 'one']);
    expect(run.code).toBe(0);
    expect(run.stderr).toBe('');
    expect(run.stdout.split('\n')[0]).toBe(`${MARKER} one`);
    expect(await readFile(marker(), 'utf-8')).toBe('built\nlib\n');
    expect(await readFile(builds(), 'utf-8')).toBe('lib\n');
  });

  it('stays completely silent on a full cache hit', async () => {
    const run = await runBin(workspace, ['app:build', '--', './report']);
    expect(run.code).toBe(0);
    expect(run.stderr).toBe('');
    // Nothing but the exec'd binary's own output: no Nx banner, no task log.
    expect(run.stdout.split('\n')[0]).toBe(MARKER);
    expect(run.stdout).not.toContain('nx run');
    expect(await readFile(marker(), 'utf-8')).toBe('built\nlib\n');
    expect(await readFile(builds(), 'utf-8')).toBe('lib\n');
  });

  it('owns its cache and database after caller directory overrides are stripped', async () => {
    const run = await runBin(workspace, [
      'app:build',
      '--',
      process.execPath,
      '-e',
      'const nx = require("nx/src/utils/cache-directory"); ' +
        'console.log(JSON.stringify({ cache: nx.cacheDir, data: nx.sharedDataDirectory(process.cwd(), "workspace-data") }));',
    ]);
    expect(run.code, run.stdout + run.stderr).toBe(0);
    expect(run.stdout).toContain(
      JSON.stringify({ cache: join(workspace, '.nx/cache'), data: join(workspace, '.nx/workspace-data') }),
    );
  });

  it('stays silent through an uncacheable nx:noop aggregate, which has no command to run', async () => {
    const first = await runBin(workspace, ['app:ship', '--', './report']);
    expect(first.code, first.stdout + first.stderr).toBe(0);
    expect(await readFile(ships(), 'utf-8')).toBe('built\nlib\n');

    // Every task but the aggregate is now a recorded success. Treating the
    // aggregate like an uncacheable command hands the whole graph back to Nx,
    // which replays each dependency's cached log on every invocation.
    const again = await runBin(workspace, ['app:ship', '--', './report']);
    expect(again.code).toBe(0);
    expect(again.stderr).toBe('');
    expect(again.stdout.split('\n')[0], again.stdout).toBe(MARKER);
    expect(again.stdout).not.toContain('nx run');
    expect(await readFile(ships(), 'utf-8')).toBe('built\nlib\n');
    expect(await readFile(builds(), 'utf-8')).toBe('lib\n');
  });

  it('stays silent when the daemon has lost its record of outputs that still match the cache', async () => {
    // The daemon's output records live in memory and are lossy: a restart
    // drops every one, and in a busy workspace a restore's own write events can
    // be processed after Nx's 2 s grace and erase the record just made. Either
    // way the working tree still holds exactly the cached bytes.
    const stopped = await nx(workspace, ['daemon', '--stop']);
    expect(stopped.code, stopped.stdout + stopped.stderr).toBe(0);

    const run = await runBin(workspace, ['app:build', '--', './report']);
    expect(run.code).toBe(0);
    expect(run.stderr).toBe('');
    expect(run.stdout.split('\n')[0], run.stdout).toBe(MARKER);
    expect(await readFile(builds(), 'utf-8')).toBe('lib\n');
  });

  it('never vouches for rewritten output bytes the daemon holds no record of', async () => {
    const stopped = await nx(workspace, ['daemon', '--stop']);
    expect(stopped.code, stopped.stdout + stopped.stderr).toBe(0);
    // Same length, different bytes: only a content comparison can tell.
    await writeFile(marker(), 'BUILT\nLIB\n');

    const run = await runBin(workspace, ['app:build', '--', './report'], { NX_VERBOSE_LOGGING: 'true' });
    expect(run.code).toBe(0);
    expect(run.stderr).toContain('app:build outputs are missing or modified on disk');
    expect(await readFile(marker(), 'utf-8')).toBe('built\nlib\n');
    expect(await readFile(builds(), 'utf-8')).toBe('lib\n');
  });

  it('runs the emitted wrapper under Node without losing the hot path', async () => {
    const run = await runBuiltBin(workspace, ['app:build', '--', './report']);
    expect(run.code).toBe(0);
    expect(run.stderr).toBe('');
    expect(run.stdout.split('\n')[0]).toBe(MARKER);
  });

  it("hands the exec'd binary blocking stdio, so a pipe its output fills is never cut", async () => {
    // Node leaves a pipe O_NONBLOCK on macOS, and the flag belongs to the open file description, so
    // it survives execve: the binary's writes then fail with EAGAIN once the pipe holds 64 KiB and
    // nobody is reading. A reader that starts late is what makes the pipe fill.
    const flood = join(workspace, 'flood');
    await writeFile(flood, '#!/bin/sh\nhead -c 300000 /dev/zero\n');
    await chmod(flood, 0o755);
    try {
      const piped = Bun.spawn(
        ['sh', '-c', `"$0" "$1" app:build -- ./flood | { sleep 1; tr -cd '\\000' | wc -c; }`, 'node', builtBinEntry],
        { cwd: workspace, env: fixtureNxEnvironment(), stdout: 'pipe', stderr: 'pipe' },
      );
      const received = (await new Response(piped.stdout).text()).trim();
      expect(await piped.exited).toBe(0);
      expect(received).toBe('300000');
    } finally {
      await rm(flood, { force: true });
    }
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
    for (const key of NX_ENV_KEYS) {
      expect(run.stdout).toContain(`${key}=unset`);
    }
  });

  it('keeps fixture Nx out of CI shared state directories', async () => {
    const run = await runBin(workspace, ['app:build', '--', './report'], {
      NX_CACHE_DIRECTORY: join(workspace, 'shared-cache'),
      NX_WORKSPACE_DATA_DIRECTORY: join(workspace, 'shared-workspace-data'),
    });
    expect(run.code).toBe(0);
    expect(run.stdout).toContain('NX_CACHE_DIRECTORY=unset');
    expect(run.stdout).toContain('NX_WORKSPACE_DATA_DIRECTORY=unset');
  });

  it('restores the caller cache controls before exec', async () => {
    const run = await runBin(workspace, ['app:build', '--', './report'], {
      NX_SKIP_NX_CACHE: 'caller-skip',
      NX_DISABLE_NX_CACHE: 'caller-disable',
    });
    expect(run.code).toBe(0);
    expect(run.stderr).toBe('');
    expect(run.stdout).toContain('NX_SKIP_NX_CACHE=caller-skip');
    expect(run.stdout).toContain('NX_DISABLE_NX_CACHE=caller-disable');
  });

  it('honors a user cache bypass outside an Nx task child', async () => {
    const run = await runBin(workspace, ['app:build', '--', './report'], {
      NX_TASK_TARGET_PROJECT: undefined,
      NX_TASK_TARGET_TARGET: undefined,
      NX_SKIP_NX_CACHE: 'true',
      NX_VERBOSE_LOGGING: 'true',
    });
    expect(run.code).toBe(0);
    expect(run.stderr).toContain('the Nx cache is disabled, so app:build must run');
    expect(run.stdout).toContain(MARKER);
    expect(run.stdout).toContain('NX_SKIP_NX_CACHE=true');
  });

  it('runs again once a recorded output is gone from the working tree', async () => {
    const before = await readFile(builds(), 'utf-8');
    await rm(marker());
    const run = await runBin(workspace, ['app:build', '--', './report'], { NX_VERBOSE_LOGGING: 'true' });
    expect(run.code).toBe(0);
    expect(run.stderr).toContain('outputs are missing or modified on disk');
    expect(run.stdout).toContain(MARKER);
    expect(await readFile(marker(), 'utf-8')).toBe('built\nlib\n');
    expect(await readFile(builds(), 'utf-8')).toBe(before);
  });

  it('runs again when a dependency\u0027s own inputs change, keeping stdout for the binary alone', async () => {
    const before = await readFile(builds(), 'utf-8');
    await writeFile(join(workspace, 'packages', 'lib', 'source1.txt'), 'lib changed\n');
    const changed = await runBin(workspace, ['app:build', '--', './report']);
    expect(changed.code).toBe(0);
    expect(await readFile(join(workspace, 'packages', 'lib', 'dist', 'lib.txt'), 'utf-8')).toBe('lib changed\n');
    expect(await readFile(marker(), 'utf-8')).toBe('built\nlib changed\n');
    expect(await readFile(builds(), 'utf-8')).toBe(`${before}lib changed\n`);
    // The run's log is on stderr. Stdout is the exec'd binary's and holds
    // nothing else, or `wrapper | jq` reads Nx's banner as data.
    expect(stripVTControlCharacters(changed.stderr)).toContain('nx run lib:build');
    expect(changed.stdout.split('\n')[0], changed.stdout).toBe(MARKER);
    for (const line of changed.stdout.split('\n').filter((line) => line !== '')) {
      expect(line === MARKER || line.startsWith('cwd=') || /^NX_[A-Z_]+=/.test(line), line).toBe(true);
    }

    // And the graph settles back to silence, which is only reachable if the
    // dependent-outputs task was hashed too.
    const settled = await runBin(workspace, ['app:build', '--', './report']);
    expect(settled.code).toBe(0);
    expect(settled.stderr).toBe('');
    expect(settled.stdout.split('\n')[0]).toBe(MARKER);
    expect(await readFile(builds(), 'utf-8')).toBe(`${before}lib changed\n`);
  });

  it('runs again when a dependency input is added or deleted', async () => {
    const addedSource = join(workspace, 'packages', 'lib', 'source2.txt');
    await writeFile(addedSource, 'added\n');
    const added = await runBin(workspace, ['app:build', '--', './report']);
    expect(added.code).toBe(0);
    expect(await readFile(marker(), 'utf-8')).toBe('built\nlib changed\nadded\n');

    await rm(addedSource);
    const removed = await runBin(workspace, ['app:build', '--', './report']);
    expect(removed.code).toBe(0);
    expect(await readFile(marker(), 'utf-8')).toBe('built\nlib changed\n');

    const settled = await runBin(workspace, ['app:build', '--', './report']);
    expect(settled.code).toBe(0);
    expect(settled.stderr).toBe('');
    expect(settled.stdout.split('\n')[0]).toBe(MARKER);
  });

  it('without a binary, stays silent and exits 0 when nothing is stale', async () => {
    const run = await runBin(workspace, ['app:build']);
    expect(run.code, run.stdout + run.stderr).toBe(0);
    expect(run.stdout).toBe('');
    expect(run.stderr).toBe('');
  });

  it('without a binary, runs what is stale, shows that run and exits 0', async () => {
    const before = await readFile(builds(), 'utf-8');
    await writeFile(join(workspace, 'packages', 'lib', 'source1.txt'), 'lib checked\n');
    const run = await runBin(workspace, ['app:build']);
    expect(run.code, run.stdout + run.stderr).toBe(0);
    // An outer `nx run nx-plugin:test` exports FORCE_COLOR, so the fixture's
    // Nx styles its task lines; what the test pins is the line, not its colour.
    expect(stripVTControlCharacters(run.stderr)).toContain('nx run lib:build');
    expect(run.stdout).toBe('');
    expect(await readFile(marker(), 'utf-8')).toBe('built\nlib checked\n');
    expect(await readFile(builds(), 'utf-8')).toBe(`${before}lib checked\n`);

    const settled = await runBin(workspace, ['app:build']);
    expect(settled.code, settled.stdout + settled.stderr).toBe(0);
    expect(settled.stdout).toBe('');
    expect(settled.stderr).toBe('');
  });

  it('without a binary, exits with a failing target\u0027s code', async () => {
    const run = await runBin(workspace, ['app:broken']);
    expect(run.code).toBe(1);
    expect(stripVTControlCharacters(run.stderr)).toContain('nx run app:broken');
    expect(run.stdout).toBe('');
  });

  it('forwards a failing target\u0027s exit code and never execs', async () => {
    const run = await runBin(workspace, ['app:broken', '--', './report']);
    expect(run.code).toBe(1);
    expect(run.stdout).not.toContain(MARKER);
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
    const emptyCommand = await runBin(workspace, ['app:build', '--']);
    expect(emptyCommand.code).toBe(2);
    expect(emptyCommand.stderr).toContain('no binary given');

    const badTarget = await runBin(workspace, ['build', '--', './report']);
    expect(badTarget.code).toBe(2);
    expect(badTarget.stderr).toContain('not a project:target');

    const pathLookup = await runBin(workspace, ['app:build', '--', 'report']);
    expect(pathLookup.code).toBe(2);
    expect(pathLookup.stderr).toContain('not a name to look up on PATH');
  });
});

describe('smoo-nx-exec signal forwarding', () => {
  let workspace = '';

  // A workspace whose `nx` is a script that kills itself. A target that has
  // never run is a miss, and a miss spawns exactly that binary, so this is the
  // whole signal path end to end — and deterministic, unlike racing a real
  // build with a kill. The daemon is off, so the probe hands the run straight
  // to that CLI; the workspace still carries the repository's own Nx, whose
  // daemon client the probe asks.
  beforeAll(async () => {
    workspace = await nxFixtureRoot('ensure-built-signal-');
    const initialized = Bun.spawnSync(['git', 'init', '--quiet', workspace]);
    expect(initialized.exitCode).toBe(0);
    await mkdir(join(workspace, 'node_modules', '.bin'), { recursive: true });
    await symlink(join(repoRoot, 'node_modules', 'nx'), join(workspace, 'node_modules', 'nx'), 'dir');
    await writeFile(join(workspace, 'nx.json'), JSON.stringify(FIXTURE_NX_JSON));
    await mkdir(join(workspace, 'packages', 'app'), { recursive: true });
    await writeFile(
      join(workspace, 'packages', 'app', 'project.json'),
      JSON.stringify({
        name: 'app',
        targets: {
          build: { executor: 'nx:run-commands', cache: true, options: { command: 'true', cwd: '{projectRoot}' } },
        },
      }),
    );
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

describe('smoo-nx-exec daemon socket', () => {
  let workspace = '';
  let socketDir = '';
  let callerDir = '';
  const record = () => readFile(join(workspace, '.nx', 'workspace-data', 'd', 'server-process.json'), 'utf-8');

  beforeAll(async () => {
    workspace = await nxFixtureRoot('ensure-built-socket-');
    // Under /tmp, not the platform temp directory: a socket path has a 95
    // character budget and macOS's temp directory spends half of it.
    socketDir = await mkdtemp(join('/tmp', 'eb-sock-'));
    callerDir = await realpath(await mkdtemp(join(tmpdir(), 'ensure-built-caller-')));
    const initialized = Bun.spawnSync(['git', 'init', '--quiet', workspace]);
    expect(initialized.exitCode).toBe(0);
    await symlink(join(repoRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
    await writeFile(join(workspace, 'nx.json'), JSON.stringify(FIXTURE_NX_JSON));
    await mkdir(join(workspace, 'packages', 'app'), { recursive: true });
    await writeFile(
      join(workspace, 'packages', 'app', 'project.json'),
      JSON.stringify({
        name: 'app',
        targets: {
          build: { executor: 'nx:run-commands', cache: true, options: { command: 'true', cwd: '{projectRoot}' } },
        },
      }),
    );
  });

  afterAll(async () => {
    try {
      if (workspace) {
        await retireNxFixture({ root: workspace, workspace });
      }
    } finally {
      await rm(socketDir, { recursive: true, force: true });
      await rm(callerDir, { recursive: true, force: true });
    }
  });

  it('starts the daemon of a root the caller does not stand in on its own socket, not the caller\u0027s', async () => {
    // The caller's environment was set up for another workspace, as a managed
    // devenv's is: its socket directory belongs to that workspace's daemon,
    // and a second daemon listening there would take the socket from it.
    const stopped = await nx(workspace, ['daemon', '--stop']);
    expect(stopped.code, stopped.stdout + stopped.stderr).toBe(0);
    const run = await runBinWith('bun', binEntry, callerDir, ['app:build', '--workspace-root', workspace], {
      NX_SOCKET_DIR: socketDir,
    });
    expect(run.code, run.stdout + run.stderr).toBe(0);
    expect(await readdir(socketDir)).toEqual([]);
    expect(await record()).not.toContain(socketDir);
  });

  it('keeps the socket directory of a caller standing in the root it was set up for', async () => {
    const stopped = await nx(workspace, ['daemon', '--stop']);
    expect(stopped.code, stopped.stdout + stopped.stderr).toBe(0);
    const run = await runBin(workspace, ['app:build'], { NX_SOCKET_DIR: socketDir });
    expect(run.code, run.stdout + run.stderr).toBe(0);
    expect(await record()).toContain(join(socketDir, 'd.sock'));
  });
});
