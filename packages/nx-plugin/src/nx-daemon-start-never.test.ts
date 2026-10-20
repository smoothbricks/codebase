import { expect, it } from 'bun:test';
import { spawn, spawnSync } from 'node:child_process';
import { mkdir, readFile, symlink, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { join } from 'node:path';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import { awaitExit, isRunning, processTable, terminate } from './testing.js';

/**
 * Guards the `NX_DAEMON_START=never` hunks of `patches/nx@23.2.1.patch`. A stock client that finds no live daemon
 * starts one, detached into a session of its own, and the record shares it with every client of the checkout. Cowshed
 * ends a job's whole process tree when the job ends (`specs/cowshed/11_shell.md`), so a daemon one job's client
 * started would end with that job while another job's `nx run` still used it. Every cowshed job therefore runs with
 * `NX_DAEMON_START=never` (`specs/cowshed/04_sandbox.md`): its client uses the daemon the record names while that
 * runs, otherwise computes the command without one, and never starts a daemon. A daemon that stops under such a
 * client turns the daemon off for that process alone, and the command continues without it: its successor is its
 * starter's to start, and the stop is no reason to disable the checkout's daemon for anyone else.
 */

const repositoryRoot = join(import.meta.dir, '../../..');
const nxEntry = join(repositoryRoot, 'node_modules', '.bin', 'nx');
const nxVersion: string = createRequire(join(repositoryRoot, 'package.json'))('nx/package.json').version;

const record = (workspace: string) => join(workspace, '.nx/workspace-data/d/server-process.json');

async function recordedPid(workspace: string): Promise<number | null> {
  const text = await readFile(record(workspace), 'utf8').catch((error: unknown) => {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return null;
    throw error;
  });
  if (text === null) return null;
  const parsed: unknown = JSON.parse(text);
  if (
    typeof parsed !== 'object' ||
    parsed === null ||
    !('processId' in parsed) ||
    typeof parsed.processId !== 'number'
  ) {
    throw new Error(`the daemon record names no process: ${text}`);
  }
  return parsed.processId;
}

async function fixtureWorkspace(workspace: string): Promise<void> {
  await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
  await writeFile(join(workspace, 'nx.json'), '{}\n');
  await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'daemon-start-never', private: true }));
  await mkdir(join(workspace, 'p'));
  await writeFile(join(workspace, 'p', 'project.json'), JSON.stringify({ name: 'p' }));
}

/**
 * Run `nx show projects --json`, require it to list the fixture's one project, and return everything it printed. The
 * format is asked for, never inferred: unasked, Nx prints JSON only when it detects an AI agent in the environment
 * (`show/command-object.js`, `isAiAgent`) and a plain name per line otherwise. The list is stdout's last line: a note
 * Nx prints about its daemon goes to stdout before it.
 */
function showProjects(workspace: string, env: Record<string, string>): string {
  const ran = spawnSync('bun', [nxEntry, 'show', 'projects', '--json'], { cwd: workspace, env, encoding: 'utf8' });
  const output = `${ran.stdout}${ran.stderr}`;
  expect(ran.status, output).toBe(0);
  expect(JSON.parse(ran.stdout.trimEnd().split('\n').at(-1) ?? ''), output).toEqual(['p']);
  return output;
}

it('a client that may never start a daemon uses a running one and otherwise runs without', async () => {
  await withNxFixture('nx-daemon-start-never', async ({ workspace, socketDirectory }) => {
    await fixtureWorkspace(workspace);
    const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true' };
    const never = { ...env, NX_DAEMON_START: 'never' };

    // No daemon runs: the command is computed in the client, and no daemon is started.
    showProjects(workspace, never);
    expect(await recordedPid(workspace)).toBeNull();

    // Control: the same client without the variable starts one, so the absence above is the variable's doing.
    showProjects(workspace, env);
    const daemon = await recordedPid(workspace);
    if (daemon === null) throw new Error('a client allowed to start a daemon left no record');
    expect(isRunning(daemon)).toBe(true);

    // A daemon runs: the client uses it, and it is still the one the record names.
    const log = join(workspace, '.nx/workspace-data/d/daemon.log');
    const before = (await readFile(log, 'utf8')).length;
    showProjects(workspace, never);
    expect(await recordedPid(workspace)).toBe(daemon);
    expect((await readFile(log, 'utf8')).length).toBeGreaterThan(before);

    // A record whose daemon has gone (killed, so it never cleared its record) is no daemon to use and no reason to
    // start one.
    const killed = (await processTable()).filter((entry) => entry.pid === daemon);
    terminate(daemon, 'SIGKILL');
    await awaitExit(killed, 'the killed fixture daemon');
    showProjects(workspace, never);
    expect(await recordedPid(workspace)).toBe(daemon);
  });
});

it('a client that may never start a daemon continues without one that stopped under it, and disables nothing', async () => {
  await withNxFixture('nx-daemon-start-gone', async ({ workspace, socketDirectory }) => {
    await fixtureWorkspace(workspace);
    const never = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true', NX_DAEMON_START: 'never' };

    // The record names a running process that serves no socket: a daemon that stopped answering after the client
    // chose to use it.
    const holder = spawn('sleep', ['300'], { stdio: 'ignore' });
    try {
      const pid = holder.pid;
      if (pid === undefined) throw new Error('sleep did not start');
      await mkdir(join(workspace, '.nx/workspace-data/d'), { recursive: true });
      await writeFile(
        record(workspace),
        JSON.stringify({ processId: pid, socketPath: join(socketDirectory, 'gone.sock'), nxVersion }),
      );

      expect(showProjects(workspace, never)).toContain('NX_DAEMON_START=never: this client never starts a daemon');
      expect(await recordedPid(workspace)).toBe(pid);
      // Not disabled for the checkout: the marker only `nx reset` clears would follow it into every terminal.
      await expect(readFile(join(workspace, '.nx/workspace-data/d/disabled'), 'utf8')).rejects.toMatchObject({
        code: 'ENOENT',
      });
    } finally {
      holder.kill('SIGKILL');
    }
  });
});

/**
 * An explicit `nx daemon --start` calls the daemon start directly, past the implicit-start check above, so under the
 * job's rule it is refused as policy: a non-zero exit naming `NX_DAEMON_START=never` and the fixture escape, and no
 * daemon. The same command without the variable, which is what a test's named fixture escape runs, starts one.
 */
it('refuses an explicit daemon start under the job rule, and starts one without it', async () => {
  await withNxFixture('nx-daemon-start-explicit', async ({ workspace, socketDirectory }) => {
    await fixtureWorkspace(workspace);
    const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true' };

    const refused = spawnSync('bun', [nxEntry, 'daemon', '--start'], {
      cwd: workspace,
      env: { ...env, NX_DAEMON_START: 'never' },
      encoding: 'utf8',
    });
    const said = `${refused.stdout}${refused.stderr}`;
    expect(refused.status, said).toBe(1);
    expect(said).toContain('NX_DAEMON_START=never forbids this client starting a daemon');
    expect(said).toContain('FIXTURE_STARTS_ITS_OWN_DAEMON');
    expect(await recordedPid(workspace)).toBeNull();

    const started = spawnSync('bun', [nxEntry, 'daemon', '--start'], { cwd: workspace, env, encoding: 'utf8' });
    expect(started.status, `${started.stdout}${started.stderr}`).toBe(0);
    const daemon = await recordedPid(workspace);
    if (daemon === null) throw new Error('an explicit start without the variable left no daemon record');
    expect(isRunning(daemon)).toBe(true);
  });
});
