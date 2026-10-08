import { expect, it } from 'bun:test';
import { existsSync, lstatSync } from 'node:fs';
import { mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import {
  fixtureSocketDirectory,
  isRunning,
  NX_LONGEST_SOCKET_NAME,
  nxDaemonProcesses,
  ownedFixtureRoot,
  ownedWorkspaceSocketDirectory,
  type ProcessEntry,
  pidsWorkingIn,
  processTable,
  terminate,
  withDescendants,
} from './testing.js';

const repositoryRoot = join(import.meta.dir, '../../..');

/**
 * Make `workspace` an Nx workspace and start its daemon. The daemon and every
 * process it started, once `nx daemon --start` has returned with it up.
 */
async function startDaemon(workspace: string, environment: Record<string, string> = {}): Promise<ProcessEntry[]> {
  await mkdir(workspace, { recursive: true });
  await writeFile(join(workspace, 'package.json'), '{"name":"daemon-fixture","private":true}\n');
  await writeFile(join(workspace, '.gitignore'), 'node_modules\n.nx\n');
  await writeFile(join(workspace, 'nx.json'), '{}\n');
  await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
  expect(Bun.spawnSync(['git', 'init', '--quiet', workspace]).exitCode).toBe(0);
  const child = Bun.spawn(['bun', join(repositoryRoot, 'node_modules/.bin/nx'), 'daemon', '--start'], {
    cwd: workspace,
    env: { ...fixtureNxEnv(workspace), NX_DAEMON: 'true', ...environment },
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const [code, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  expect(code, stdout + stderr).toBe(0);
  const daemon = await nxDaemonProcesses(workspace);
  expect(daemon, stdout + stderr).not.toEqual([]);
  return daemon;
}

/** Which of `processes` still run, zombies (already exited) aside. */
async function stillRunning(processes: readonly ProcessEntry[]): Promise<string[]> {
  const live = (await processTable()).filter((entry) => !entry.stat.startsWith('Z')).map((entry) => entry.pid);
  return processes.filter((entry) => live.includes(entry.pid)).map((entry) => `${entry.pid} ${entry.command}`);
}

it('lists a process once when it descends from another of the pids it is asked about', () => {
  const table: ProcessEntry[] = [
    { pid: 1, ppid: 0, stat: 'Ss', command: 'launchd' },
    { pid: 10, ppid: 1, stat: 'Ss', command: 'sh -c sleep 600 & wait' },
    { pid: 11, ppid: 10, stat: 'S', command: 'sleep 600' },
    { pid: 12, ppid: 11, stat: 'S', command: 'sleep 1' },
    { pid: 20, ppid: 1, stat: 'S', command: 'bystander' },
  ];

  expect(
    withDescendants(table, [11, 10])
      .map((entry) => entry.pid)
      .sort((a, b) => a - b),
  ).toEqual([10, 11, 12]);
});

it('stops the Nx daemon of a fixture whose body throws, and deletes its root', async () => {
  let root = '';
  let daemon: ProcessEntry[] = [];
  const thrown = withNxFixture('throwing-body-', async (fixture) => {
    root = fixture.root;
    daemon = await startDaemon(fixture.workspace);
    throw new Error('the fixture body failed on purpose');
  });

  await expect(thrown).rejects.toThrow('the fixture body failed on purpose');
  expect(daemon).not.toEqual([]);
  expect(await stillRunning(daemon)).toEqual([]);
  expect(await pidsWorkingIn(root)).toEqual([]);
  expect(existsSync(root)).toBe(false);
}, 120_000);

it('starts and connects to a fixture daemon through owned short aliases with a long private TMPDIR', async () => {
  await withNxFixture('long-tmp-socket-', async ({ workspace }) => {
    const privateTemp = join(workspace, 'private-temp-'.padEnd(110, 'x'));
    await mkdir(privateTemp);
    expect(Buffer.byteLength(join(privateTemp, NX_LONGEST_SOCKET_NAME))).toBeGreaterThan(95);
    const daemon = await startDaemon(workspace, { TMPDIR: privateTemp });
    const directory = fixtureSocketDirectory(workspace);
    const alias = dirname(directory);
    expect(lstatSync(alias).isSymbolicLink()).toBe(true);
    expect(lstatSync(directory).isDirectory()).toBe(true);
    expect(lstatSync(directory).isSymbolicLink()).toBe(false);
    expect(await realpath(directory)).toBe(join(await realpath(workspace), '.nx', 'sockets', 'nx'));
    expect(Buffer.byteLength(join(directory, NX_LONGEST_SOCKET_NAME))).toBeLessThanOrEqual(95);
    const other = await ownedWorkspaceSocketDirectory(workspace, 'nc-', NX_LONGEST_SOCKET_NAME);
    expect(await realpath(other)).toBe(
      join(await realpath(workspace), '.nx', 'sockets', other.slice(alias.length + 1)),
    );
    expect(lstatSync(other).isSymbolicLink()).toBe(false);
    await rm(other, { recursive: true });
    const client = Bun.spawn(['bun', join(repositoryRoot, 'node_modules/.bin/nx'), 'show', 'projects', '--json'], {
      cwd: workspace,
      env: { ...fixtureNxEnv(workspace), NX_DAEMON: 'true', TMPDIR: privateTemp },
      stdout: 'pipe',
      stderr: 'pipe',
    });
    const [code, stdout, stderr] = await Promise.all([
      client.exited,
      new Response(client.stdout).text(),
      new Response(client.stderr).text(),
    ]);
    expect(code, stdout + stderr).toBe(0);
    expect(JSON.parse(stdout)).toEqual([]);
    const record = JSON.parse(
      await readFile(join(workspace, '.nx', 'workspace-data', 'd', 'server-process.json'), 'utf8'),
    );
    expect(daemon.some(({ pid }) => pid === record.processId)).toBe(true);
    expect(await stillRunning(daemon)).not.toEqual([]);
  });
}, 120_000);

it("reclaims a dead run's Nx daemon and helper processes, and nothing of a live run", async () => {
  // A suite of its own, so this process's first fixture of it is the one that sweeps.
  const suite = `sweep-regression-${process.pid}`;
  const suiteDirectory = join(await realpath(tmpdir()), 'smoothbricks-fixtures', suite);
  // A pid that names no process: one this test started and saw exit.
  const exited = Bun.spawn(['true']);
  await exited.exited;
  const deadRun = join(suiteDirectory, `run-${exited.pid}`);
  // A run whose owner is alive: the process that started this test.
  const liveRun = join(suiteDirectory, `run-${process.ppid}`);
  const abandoned = join(deadRun, 'abandoned-fixture');
  const kept = join(liveRun, 'live-fixture');
  await mkdir(kept, { recursive: true });
  await mkdir(abandoned, { recursive: true });
  let daemon: ProcessEntry[] = [];
  const helper = Bun.spawn(['sleep', '600'], { cwd: abandoned });
  const bystander = Bun.spawn(['sleep', '600'], { cwd: kept });
  try {
    // A real daemon, as a killed test leaves it, beside a stand-in for the
    // helper processes (watchers, clients) that work in the same root.
    daemon = await startDaemon(abandoned);

    const fresh = await ownedFixtureRoot(tmpdir(), suite, 'fresh-');

    expect(await stillRunning(daemon)).toEqual([]);
    expect(await helper.exited).not.toBe(0);
    expect(helper.signalCode).toBe('SIGTERM');
    expect(existsSync(deadRun)).toBe(false);
    expect(bystander.exitCode).toBeNull();
    expect(bystander.signalCode).toBeNull();
    expect(existsSync(kept)).toBe(true);
    expect(fresh.startsWith(join(suiteDirectory, `run-${process.pid}`, 'fresh-'))).toBe(true);
  } finally {
    for (const entry of [...daemon, { pid: helper.pid }, { pid: bystander.pid }]) {
      terminate(entry.pid);
    }
    await Promise.all([helper.exited, bystander.exited]);
    await rm(suiteDirectory, { recursive: true, force: true });
  }
}, 120_000);

it('stops what works in every run it owns when the process is signalled, then dies of the signal', async () => {
  const suite = `signal-regression-${process.pid}`;
  const suiteDirectory = join(await realpath(tmpdir()), 'smoothbricks-fixtures', suite);
  // A second parent for the same suite: the owner holds a run in each, and one signal has to reclaim both.
  const otherParent = await mkdtemp(join(await realpath(tmpdir()), 'signal-regression-parent-'));
  const otherSuiteDirectory = join(otherParent, 'smoothbricks-fixtures', suite);
  const script = join(suiteDirectory, 'owner.ts');
  await mkdir(suiteDirectory, { recursive: true });
  // A detached stand-in for a fixture daemon in each run: its own session, so only the
  // owner's handler, not a process-group signal, can stop it. The interval
  // keeps the owner alive until it is signalled.
  await writeFile(
    script,
    `import { spawn } from 'node:child_process';
import { tmpdir } from 'node:os';
import { ownedFixtureRoot } from ${JSON.stringify(join(import.meta.dir, 'testing.ts'))};
const helpers = [];
for (const parent of [tmpdir(), ${JSON.stringify(otherParent)}]) {
  const root = await ownedFixtureRoot(parent, ${JSON.stringify(suite)}, 'signalled-');
  const helper = spawn('sleep', ['600'], { cwd: root, detached: true, stdio: 'ignore' });
  helper.unref();
  helpers.push(helper.pid);
}
console.log('READY ' + helpers.join(' '));
setInterval(() => {}, 1 << 30);
`,
  );
  const owner = Bun.spawn([process.execPath, script], { stdout: 'pipe', stderr: 'pipe' });
  let helpers: number[] = [];
  try {
    const reader = owner.stdout.getReader();
    const decoder = new TextDecoder();
    let printed = '';
    while (helpers.length === 0) {
      const chunk = await reader.read();
      if (chunk.done) throw new Error(`the owner exited before it was ready: ${printed}`);
      printed += decoder.decode(chunk.value, { stream: true });
      helpers = /^READY (\d+) (\d+)$/m.exec(printed)?.slice(1).map(Number) ?? [];
    }
    for (const helper of helpers) {
      expect(isRunning(helper)).toBe(true);
    }
    // One run for each parent, though both name one suite.
    const runs = [suiteDirectory, otherSuiteDirectory].map((directory) => join(directory, `run-${owner.pid}`));
    for (const run of runs) {
      expect(existsSync(run), run).toBe(true);
    }

    owner.kill('SIGTERM');
    await owner.exited;

    expect(owner.signalCode, await new Response(owner.stderr).text()).toBe('SIGTERM');
    expect(await stillRunning(helpers.map((pid) => ({ pid, ppid: 0, stat: '', command: 'sleep 600' })))).toEqual([]);
    for (const run of runs) {
      expect(existsSync(run), run).toBe(false);
    }
  } finally {
    for (const helper of helpers) terminate(helper);
    owner.kill('SIGKILL');
    await owner.exited;
    await rm(suiteDirectory, { recursive: true, force: true });
    await rm(otherParent, { recursive: true, force: true });
  }
}, 120_000);
