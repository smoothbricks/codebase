import { expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdir, readFile, symlink, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { join } from 'node:path';
import type * as NxDaemonEnvironment from 'nx/src/daemon/client/daemon-environment';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import { RESERVATIONS_ENV, RESERVATIONS_UNSERVED_ENV } from './executors/bounded-exec/mbx-reservations.js';

/**
 * Guards the run-environment hunk of `patches/nx@24.0.6.patch` in `daemon-environment.js` (`patches/README.md`, "A
 * run's reservation endpoint is no graph identity"). Per-job cowshed/mbx scheduling variables, a run's reservation
 * endpoint, and task-process variables (`BOUNDED_EXEC_*`, runner thread caps, Nx task-invocation markers,
 * `DEVENV_TASK_FILE`) differ between clients while no graph plugin reads them. Leaving them out of
 * `normalizeDaemonEnvironmentForGraph` keeps the daemon's cached graph across clients while still handing the daemon
 * and its plugin workers the full runtime environment.
 */

const repositoryRoot = join(import.meta.dir, '../../..');
const nxEntry = join(repositoryRoot, 'node_modules', '.bin', 'nx');
const workspaceRequire = createRequire(join(repositoryRoot, 'package.json'));
const environment: typeof NxDaemonEnvironment = workspaceRequire('nx/src/daemon/client/daemon-environment');

const graph = (env: NodeJS.ProcessEnv) => environment.normalizeDaemonEnvironmentForGraph(env, 'darwin');
const shell: NodeJS.ProcessEnv = { PATH: '/usr/bin:/bin', HOME: '/home/u' };

const RECOMPUTED = 'Graph recompute necessary due to env variable refresh. Changed keys: ';
const KEPT = /Env variable refresh changed only runtime values \(([^)]*)\); graph identity is unchanged/;

const NON_IDENTITY_ENV: Readonly<Record<string, string>> = {
  // Per-job environment variables
  MAKEFLAGS: '-j --jobserver-auth=fifo:/tmp/cowshed-job-1/fifo',
  CARGO_MAKEFLAGS: '-j --jobserver-auth=fifo:/tmp/cowshed-job-1/fifo',
  COWSHED_INSTALL_OBSERVED: '1732670000000',
  MBX_SCHEDULER_JOB_STARTED_MS: '1732670000000',
  MBX_SCHEDULER_JOB_ID: 'job-1-42',
  COWSHED_MBX_SCHEDULER_PRIORITY: 'interactive',
  MBX_SCHEDULER_PRIORITY: 'interactive',
  // Task-process environment variables
  RUST_TEST_THREADS: '4',
  NEXTEST_TEST_THREADS: '4',
  MBX_SCHED_DISABLE: '1',
  MBX_SCHED_DIR: '/tmp/mbx-sched',
  MBX_RESERVE_CPUS: '4',
  NX_INVOCATION_ANCESTOR_PIDS: '1001,1002',
  LERNA_PACKAGE_NAME: 'nx-plugin',
  NX_WORKSPACE_ROOT: '/work/repo',
  DEVENV_TASK_FILE: '/nix/store/0000-devenv-tasks.json',
  // bounded-exec task-time namespace
  BOUNDED_EXEC_CPU_POOL: '/tmp/pool-1/p.sock',
  BOUNDED_EXEC_CPU_TOKENS: '8',
  BOUNDED_EXEC_CPU_WANT: '4',
  BOUNDED_EXEC_MBX_AGING_MS: '30000',
  BOUNDED_EXEC_MBX_TEST_GATE: '1',
  BOUNDED_EXEC_PROGRESS_SOCKET: '/tmp/prog-1/p.sock',
  BOUNDED_EXEC_JUNIT: '/tmp/report.xml',
  BOUNDED_EXEC_MUTATION_REPORT_DIR: '/tmp/mut-1',
  BOUNDED_EXEC_FIXTURE_DIR: '/tmp/fix-1',
  BOUNDED_EXEC_TEMPLATE_DIR: '/tmp/tpl-1',
  [RESERVATIONS_UNSERVED_ENV]: 'mbx reservations could not run: ENOENT',
};

const FIXTURE_PLUGIN = `exports.createNodesV2 = [
  '**/project.json',
  (files) =>
    files.map((file) => [file, { projects: { p: { tags: ['env:' + (process.env.FIXTURE_PLUGIN_TAG || 'default')] } } }]),
];
`;

async function fixtureWorkspace(workspace: string): Promise<void> {
  await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
  await writeFile(join(workspace, 'env-tag-plugin.cjs'), FIXTURE_PLUGIN);
  await writeFile(join(workspace, 'nx.json'), JSON.stringify({ plugins: ['./env-tag-plugin.cjs'] }));
  await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'run-env-identity', private: true }));
  await mkdir(join(workspace, 'p'));
  await writeFile(join(workspace, 'p', 'project.json'), JSON.stringify({ name: 'p' }));
}

function projectTags(stdout: string): string[] {
  const parsed: unknown = JSON.parse(stdout.trim().split('\n').at(-1) ?? '');
  if (typeof parsed !== 'object' || parsed === null || !('tags' in parsed) || !Array.isArray(parsed.tags)) {
    throw new Error(`unexpected show project output: ${stdout}`);
  }
  return parsed.tags.filter((tag): tag is string => typeof tag === 'string');
}

async function showProject(
  workspace: string,
  env: Record<string, string>,
): Promise<{ readonly log: string; readonly tags: string[] }> {
  const logPath = join(workspace, '.nx/workspace-data/d/daemon.log');
  const before = (await readFile(logPath)).length;
  const ran = spawnSync('bun', [nxEntry, 'show', 'project', 'p', '--json'], { cwd: workspace, env, encoding: 'utf8' });
  expect(ran.status, `nx show project p\n${ran.stdout}${ran.stderr}`).toBe(0);
  const log = (await readFile(logPath)).subarray(before).toString('utf8');
  return { log, tags: projectTags(ran.stdout) };
}
//#region smoo!n/nx-run-endpoint-graph-identity
it("leaves a run's reservation socket out of the graph identity, whatever its path", () => {
  const without = graph(shell);
  expect(graph({ ...shell, [RESERVATIONS_ENV]: '/tmp/mbx-r-a1b2/r.sock' })).toEqual(without);
  expect(graph({ ...shell, [RESERVATIONS_ENV]: '/tmp/mbx-r-c3d4/r.sock' })).toEqual(without);
});

it("leaves the reason a run's endpoint did not start out of the graph identity too", () => {
  expect(graph({ ...shell, [RESERVATIONS_UNSERVED_ENV]: 'mbx reservations could not run: ENOENT' })).toEqual(
    graph(shell),
  );
});

it('still hands the daemon the endpoint: only the graph identity leaves it out', () => {
  const saved = process.env[RESERVATIONS_ENV];
  process.env[RESERVATIONS_ENV] = '/tmp/mbx-r-e5f6/r.sock';
  try {
    expect(environment.getDaemonEnv()[RESERVATIONS_ENV]).toBe('/tmp/mbx-r-e5f6/r.sock');
  } finally {
    if (saved === undefined) delete process.env[RESERVATIONS_ENV];
    else process.env[RESERVATIONS_ENV] = saved;
  }
});

it('leaves every per-job, per-task and BOUNDED_EXEC_* variable out of the graph identity', () => {
  const without = graph(shell);
  for (const [key, value] of Object.entries(NON_IDENTITY_ENV)) {
    expect(graph({ ...shell, [key]: value }), key).toEqual(without);
  }
  expect(graph({ ...shell, ...NON_IDENTITY_ENV })).toEqual(without);
});

it('still hands the daemon per-job variables: only the graph identity leaves them out', () => {
  const savedJobId = process.env.MBX_SCHEDULER_JOB_ID;
  const savedMakeflags = process.env.CARGO_MAKEFLAGS;
  process.env.MBX_SCHEDULER_JOB_ID = 'job-9-77';
  process.env.CARGO_MAKEFLAGS = '-j --jobserver-auth=fifo:/tmp/job-77/fifo';
  try {
    const daemonEnv = environment.getDaemonEnv();
    expect(daemonEnv.MBX_SCHEDULER_JOB_ID).toBe('job-9-77');
    expect(daemonEnv.CARGO_MAKEFLAGS).toBe('-j --jobserver-auth=fifo:/tmp/job-77/fifo');
  } finally {
    if (savedJobId === undefined) delete process.env.MBX_SCHEDULER_JOB_ID;
    else process.env.MBX_SCHEDULER_JOB_ID = savedJobId;
    if (savedMakeflags === undefined) delete process.env.CARGO_MAKEFLAGS;
    else process.env.CARGO_MAKEFLAGS = savedMakeflags;
  }
});

it('keeps control variables a client or plugin reads as graph identity', () => {
  const without = graph(shell);
  for (const [key, value] of [
    ['JAVA_HOME', '/opt/jdk-21'],
    ['SMOO_LINT_BUN', '/nix/store/lint-bun/bin/bun'],
    ['FIXTURE_PLUGIN_TAG', 'custom'],
    ['MBX_CACHE_DIR', '/tmp/mbx-cache'],
  ] as const) {
    expect(graph({ ...shell, [key]: value }), key).not.toEqual(without);
  }
});

it('keeps the fixture daemon graph across per-job changes and recomputes on a plugin-read variable', async () => {
  await withNxFixture('nx-run-env-identity-', async ({ workspace, socketDirectory }) => {
    await fixtureWorkspace(workspace);
    const baseEnv: Record<string, string> = {
      ...fixtureNxEnv(workspace, socketDirectory),
      NX_DAEMON: 'true',
      FIXTURE_PLUGIN_TAG: 'first',
      MBX_SCHEDULER_JOB_ID: 'job-1-101',
      CARGO_MAKEFLAGS: '-j --jobserver-auth=fifo:/tmp/job-101/fifo',
    };
    const started = spawnSync('bun', [nxEntry, 'daemon', '--start'], {
      cwd: workspace,
      env: baseEnv,
      encoding: 'utf8',
    });
    expect(started.status, started.stdout + started.stderr).toBe(0);

    const first = await showProject(workspace, baseEnv);
    expect(first.tags).toContain('env:first');

    const second = await showProject(workspace, {
      ...baseEnv,
      MBX_SCHEDULER_JOB_ID: 'job-1-202',
      CARGO_MAKEFLAGS: '-j --jobserver-auth=fifo:/tmp/job-202/fifo',
    });
    expect(second.log, 'second client changing only per-job vars').not.toContain(RECOMPUTED);
    const runtimeKeys = KEPT.exec(second.log)?.[1]?.split(', ');
    expect(runtimeKeys, `runtime keys logged\n${second.log}`).toContain('MBX_SCHEDULER_JOB_ID');
    expect(runtimeKeys, `runtime keys logged\n${second.log}`).toContain('CARGO_MAKEFLAGS');
    expect(second.tags).toContain('env:first');

    const third = await showProject(workspace, {
      ...baseEnv,
      MBX_SCHEDULER_JOB_ID: 'job-1-202',
      CARGO_MAKEFLAGS: '-j --jobserver-auth=fifo:/tmp/job-202/fifo',
      FIXTURE_PLUGIN_TAG: 'second',
    });
    expect(third.log).toContain('Graph recompute necessary');
    expect(third.log).toContain(`${RECOMPUTED}FIXTURE_PLUGIN_TAG`);
    expect(third.tags).toContain('env:second');
  });
});
//#endregion smoo!n/nx-run-endpoint-graph-identity
