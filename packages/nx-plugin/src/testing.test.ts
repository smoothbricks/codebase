import { expect, it } from 'bun:test';
import { existsSync } from 'node:fs';
import { mkdir, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import {
  isRunning,
  nxDaemonProcesses,
  ownedFixtureRoot,
  type ProcessEntry,
  pidsWorkingIn,
  processTable,
  terminate,
} from './testing.js';

const repositoryRoot = join(import.meta.dir, '../../..');

/**
 * Make `workspace` an Nx workspace and start its daemon. The daemon and every
 * process it started, once `nx daemon --start` has returned with it up.
 */
async function startDaemon(workspace: string): Promise<ProcessEntry[]> {
  await mkdir(workspace, { recursive: true });
  await writeFile(join(workspace, 'package.json'), '{"name":"daemon-fixture","private":true}\n');
  await writeFile(join(workspace, '.gitignore'), 'node_modules\n.nx\n');
  await writeFile(join(workspace, 'nx.json'), '{}\n');
  await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
  expect(Bun.spawnSync(['git', 'init', '--quiet', workspace]).exitCode).toBe(0);
  const child = Bun.spawn(['bun', join(repositoryRoot, 'node_modules/.bin/nx'), 'daemon', '--start'], {
    cwd: workspace,
    env: { ...fixtureNxEnv(workspace), NX_DAEMON: 'true' },
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

    const fresh = await ownedFixtureRoot(suite, 'fresh-');

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

it('stops what works in its own run when the owning process is signalled, then dies of the signal', async () => {
  const suite = `signal-regression-${process.pid}`;
  const suiteDirectory = join(await realpath(tmpdir()), 'smoothbricks-fixtures', suite);
  const script = join(suiteDirectory, 'owner.ts');
  await mkdir(suiteDirectory, { recursive: true });
  // A detached stand-in for a fixture daemon: its own session, so only the
  // owner's handler, not a process-group signal, can stop it. The interval
  // keeps the owner alive until it is signalled.
  await writeFile(
    script,
    `import { spawn } from 'node:child_process';
import { ownedFixtureRoot } from ${JSON.stringify(join(import.meta.dir, 'testing.ts'))};
const root = await ownedFixtureRoot(${JSON.stringify(suite)}, 'signalled-');
const helper = spawn('sleep', ['600'], { cwd: root, detached: true, stdio: 'ignore' });
helper.unref();
console.log('READY ' + helper.pid);
setInterval(() => {}, 1 << 30);
`,
  );
  const owner = Bun.spawn([process.execPath, script], { stdout: 'pipe', stderr: 'pipe' });
  let helper = 0;
  try {
    const reader = owner.stdout.getReader();
    const decoder = new TextDecoder();
    let printed = '';
    while (helper === 0) {
      const chunk = await reader.read();
      if (chunk.done) throw new Error(`the owner exited before it was ready: ${printed}`);
      printed += decoder.decode(chunk.value, { stream: true });
      helper = Number(/^READY (\d+)$/m.exec(printed)?.[1] ?? 0);
    }
    expect(isRunning(helper)).toBe(true);

    owner.kill('SIGTERM');
    await owner.exited;

    expect(owner.signalCode, await new Response(owner.stderr).text()).toBe('SIGTERM');
    expect(await stillRunning([{ pid: helper, ppid: 0, stat: '', command: 'sleep 600' }])).toEqual([]);
    expect(existsSync(join(suiteDirectory, `run-${owner.pid}`))).toBe(false);
  } finally {
    if (helper !== 0) terminate(helper);
    owner.kill('SIGKILL');
    await owner.exited;
    await rm(suiteDirectory, { recursive: true, force: true });
  }
}, 120_000);
