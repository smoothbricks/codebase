import { expect, it } from 'bun:test';
import { spawn } from 'node:child_process';
import { mkdir, mkdtemp, rm, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { guardEvent } from './__tests__/counted-cargo.js';
import { fixtureNxEnv, stopFixtureNxDaemon, withNxFixture } from './__tests__/fixture-nx-env.js';
import { nxDaemonProcesses } from './testing.js';

/**
 * Guards the foreign-environment hunk of `patches/nx@23.2.1.patch`. A shell set up for one workspace keeps its
 * `NX_WORKSPACE_ROOT_PATH`, `NX_WORKSPACE_DATA_DIRECTORY`, `NX_CACHE_DIRECTORY` and `NX_SOCKET_DIR` when it moves into
 * another checkout. Nx takes `NX_WORKSPACE_ROOT_PATH` verbatim as its root, so `nx daemon --stop` run there stopped the
 * first workspace's daemon and deleted its socket directory.
 *
 * The CLI now refuses before it reaches any daemon, socket or file, and names the variables and both workspaces. Drop
 * the hunk, and keep this test, once the Nx version in use refuses here.
 */

const repositoryRoot = join(import.meta.dir, '../../..');
const nxEntry = join(repositoryRoot, 'node_modules', '.bin', 'nx');

interface NxRun {
  readonly code: number | null;
  readonly stdout: string;
  readonly stderr: string;
}

async function runNx(cwd: string, env: Record<string, string>, args: readonly string[]): Promise<NxRun> {
  const child = spawn('bun', [nxEntry, ...args], {
    cwd,
    env,
    detached: process.platform !== 'win32',
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
  const { promise: ended, resolve, reject } = Promise.withResolvers<number | null>();
  child.once('error', reject);
  child.once('close', resolve);
  try {
    const code = await guardEvent(
      ended,
      `nx ${args.join(' ')} in ${cwd} to close`,
      () => `pid ${child.pid}, exit ${child.exitCode}, signal ${child.signalCode}\n${stdout}${stderr}`,
    );
    return { code, stdout, stderr };
  } catch (error) {
    if (child.pid !== undefined && child.exitCode === null && child.signalCode === null) {
      try {
        process.kill(process.platform === 'win32' ? child.pid : -child.pid, 'SIGKILL');
      } catch (killError) {
        if (!(killError instanceof Error && 'code' in killError && killError.code === 'ESRCH')) {
          throw new AggregateError([error, killError], `could not retire the failed nx ${args.join(' ')}`);
        }
      }
    }
    await ended.catch(() => undefined);
    throw error;
  }
}

async function writeWorkspace(root: string, name: string): Promise<void> {
  await mkdir(join(root, 'p'), { recursive: true });
  await symlink(join(repositoryRoot, 'node_modules'), join(root, 'node_modules'), 'dir');
  await writeFile(join(root, 'nx.json'), '{}\n');
  await writeFile(join(root, 'package.json'), JSON.stringify({ name, private: true }));
  await writeFile(join(root, 'p', 'project.json'), JSON.stringify({ name: `${name}-p` }));
}

it("refuses nx run in one workspace under another workspace's environment, and leaves that daemon running", async () => {
  // A socket path has a ~104 byte budget and the fixture root spends most of it, so A's socket directory, which
  // lies in A, is reached through a short link under /tmp, as a managed shell's runtime link reaches its checkout's.
  const link = await mkdtemp(join('/tmp', 'nx-fe-'));
  try {
    await withNxFixture(
      'nx-foreign-env-',
      async ({ root, workspace: a }) => {
        const b = join(root, 'b');
        await writeWorkspace(a, 'a');
        await writeWorkspace(b, 'b');
        await mkdir(join(a, '.nx', 'run'), { recursive: true });
        await symlink(join(a, '.nx', 'run'), join(link, 'a'), 'dir');
        const state = {
          NX_WORKSPACE_ROOT_PATH: a,
          NX_WORKSPACE_DATA_DIRECTORY: join(a, '.nx', 'workspace-data'),
          NX_CACHE_DIRECTORY: join(a, '.nx', 'cache'),
          NX_SOCKET_DIR: join(link, 'a', 'nx'),
        };
        const aEnv = { ...fixtureNxEnv(a), ...state, NX_DAEMON: 'true' };

        const started = await runNx(a, aEnv, ['daemon', '--start']);
        expect(started.code, started.stdout + started.stderr).toBe(0);
        const listed = await runNx(a, aEnv, ['show', 'projects']);
        expect(listed.code, listed.stdout + listed.stderr).toBe(0);
        expect(listed.stdout.split('\n')).toContain('a-p');
        const [daemon] = await nxDaemonProcesses(a);
        if (daemon === undefined)
          throw new Error(`A's daemon left no running record\n${listed.stdout}${listed.stderr}`);

        const stop = await runNx(b, aEnv, ['daemon', '--stop']);
        expect(stop.code, stop.stdout + stop.stderr).not.toBe(0);
        for (const [name, value] of Object.entries(state)) {
          expect(stop.stderr).toContain(`${name}=${value}`);
        }
        expect(stop.stderr).toContain(`direnv exec ${b} nx`);

        expect((await nxDaemonProcesses(a))[0]?.pid).toBe(daemon.pid);
        const answered = await runNx(a, aEnv, ['show', 'projects']);
        expect(answered.code, answered.stdout + answered.stderr).toBe(0);
        expect(answered.stdout.split('\n')).toContain('a-p');
        expect((await nxDaemonProcesses(a))[0]?.pid).toBe(daemon.pid);

        await stopFixtureNxDaemon(a, aEnv);
        expect(await nxDaemonProcesses(a)).toEqual([]);
      },
      'a',
    );
  } finally {
    await rm(link, { recursive: true, force: true });
  }
});
