import { describe, expect, it } from 'bun:test';
import { spawn, spawnSync } from 'node:child_process';
import { once } from 'node:events';
import { access, mkdir, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import { isRunning } from './testing.js';

/**
 * Guards the `DaemonClient.stop` hunk of `patches/nx@23.2.1.patch`. Stock 23.2.1 sent SIGTERM to the pid the daemon
 * record names and, whatever the kernel answered other than ESRCH, printed the error, deleted the daemon's socket
 * directory and let `nx daemon --stop` print "Daemon Server - Stopped" and exit 0. A cowshed job may signal only its
 * own sandbox's processes (`specs/cowshed/04_sandbox.md`, `(allow signal (target same-sandbox))`), so its stop of the
 * shed's kept daemon got EPERM, the daemon kept running, and the job was told it had stopped. Deleting the socket of a
 * daemon that still runs hides it from the checkout's clients, and the next one starts a second daemon for the one Nx
 * state of the checkout (`specs/cowshed/16_build_volumes.md`, "Every checkout has exactly one Nx state"). That daemon
 * is its keeper's to stop (`04_sandbox.md`). The stop now fails naming the pid and the errno, exits non-zero, and keeps
 * the socket of the daemon that still serves.
 *
 * A host test: the Seatbelt case applies its own sandbox profile, which `sandbox_apply` refuses inside another sandbox,
 * such as a cowshed job's.
 */

const repositoryRoot = join(import.meta.dir, '../../..');
const nxEntry = join(repositoryRoot, 'node_modules', '.bin', 'nx');
const record = (workspace: string) => join(workspace, '.nx/workspace-data/d/server-process.json');

async function fixtureWorkspace(workspace: string): Promise<void> {
  await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
  await writeFile(join(workspace, 'nx.json'), '{}\n');
  await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'daemon-stop', private: true }));
  await mkdir(join(workspace, 'p'));
  await writeFile(join(workspace, 'p', 'project.json'), JSON.stringify({ name: 'p' }));
}

async function recordedPid(workspace: string): Promise<number> {
  const parsed: unknown = JSON.parse(await readFile(record(workspace), 'utf8'));
  if (
    typeof parsed !== 'object' ||
    parsed === null ||
    !('processId' in parsed) ||
    typeof parsed.processId !== 'number'
  ) {
    throw new Error(`the daemon record names no process: ${JSON.stringify(parsed)}`);
  }
  return parsed.processId;
}

async function exists(path: string): Promise<boolean> {
  return access(path).then(
    () => true,
    (error: unknown) => {
      if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return false;
      throw error;
    },
  );
}

interface Ran {
  readonly status: number | null;
  readonly output: string;
}

function nx(workspace: string, env: Record<string, string>, launch: readonly string[], ...args: string[]): Ran {
  const [command, ...prefix] = [...launch, 'bun', nxEntry];
  if (command === undefined) throw new Error('a launch names its command');
  const ran = spawnSync(command, [...prefix, ...args], { cwd: workspace, env, encoding: 'utf8' });
  return { status: ran.status, output: `${ran.stdout}${ran.stderr}` };
}

/** A refused stop: exit 1, the pid and the errno named, never the stock success line. */
function expectRefused(stop: Ran, pid: number): void {
  expect(stop.status, stop.output).toBe(1);
  expect(stop.output).toContain(`process ${pid}`);
  expect(stop.output).toContain('EPERM');
  expect(stop.output).not.toContain('Daemon Server - Stopped');
}

describe('nx daemon --stop when the kernel refuses its signal', () => {
  if (process.platform === 'darwin') {
    it('reports a daemon its sandbox may not signal as still running, and the daemon keeps serving', async () => {
      await withNxFixture('nx-stop-sandboxed-', async ({ workspace, socketDirectory }) => {
        await fixtureWorkspace(workspace);
        const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true' };
        const started = nx(workspace, env, [], 'daemon', '--start');
        expect(started.status, started.output).toBe(0);
        const daemon = await recordedPid(workspace);
        // The signal rule of a cowshed job's profile, over an otherwise open one.
        const sandboxed = [
          'sandbox-exec',
          '-p',
          '(version 1)(allow default)(deny signal)(allow signal (target same-sandbox))',
        ];
        expectRefused(nx(workspace, env, sandboxed, 'daemon', '--stop'), daemon);
        expect(isRunning(daemon)).toBe(true);
        expect(await exists(socketDirectory)).toBe(true);
        // Its clients still reach it: the same daemon answers, and none started beside it.
        const shown = nx(workspace, env, [], 'show', 'projects', '--json');
        expect(shown.status, shown.output).toBe(0);
        expect(JSON.parse(shown.output.trim().split('\n').at(-1) ?? '')).toEqual(['p']);
        expect(await recordedPid(workspace)).toBe(daemon);
        // A client allowed to signal it stops it, as the fixture's retirement does.
      });
    });
  }

  it('reports a recorded live process this user may not signal as still running', async () => {
    // Pid 1 is init, launchd on macOS: a process of another user, which the kernel refuses to let this one signal, as
    // it refuses a daemon left by `sudo nx` or a container's other uid. Run as root, it would be signalled.
    expect(process.getuid?.(), 'this test signals pid 1 and must not run as root').not.toBe(0);
    await withNxFixture('nx-stop-foreign-', async ({ workspace, socketDirectory }) => {
      await fixtureWorkspace(workspace);
      await mkdir(join(workspace, '.nx/workspace-data/d'), { recursive: true });
      const foreign = JSON.stringify({ processId: 1, socketPath: join(socketDirectory, 'd.sock') });
      await writeFile(record(workspace), foreign);
      const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true' };
      expectRefused(nx(workspace, env, [], 'daemon', '--stop'), 1);
      expect(await readFile(record(workspace), 'utf8')).toBe(foreign);
      // The record names no daemon of this fixture's, so the fixture's retirement must not try to stop pid 1.
      await rm(record(workspace));
    });
  });

  it('reports a recorded process that has exited as stopped', async () => {
    await withNxFixture('nx-stop-exited-', async ({ workspace, socketDirectory }) => {
      await fixtureWorkspace(workspace);
      const exited = spawn(process.execPath, ['-e', ''], { stdio: 'ignore' });
      await once(exited, 'exit');
      if (exited.pid === undefined) throw new Error('the exited process had no pid');
      await mkdir(join(workspace, '.nx/workspace-data/d'), { recursive: true });
      await writeFile(
        record(workspace),
        JSON.stringify({ processId: exited.pid, socketPath: join(socketDirectory, 'd.sock') }),
      );
      const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true' };
      const stop = nx(workspace, env, [], 'daemon', '--stop');
      expect(stop.status, stop.output).toBe(0);
      expect(stop.output).toContain('Daemon Server - Stopped');
    });
  });
});
