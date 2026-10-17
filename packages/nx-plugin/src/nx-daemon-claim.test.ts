import { describe, expect, it } from 'bun:test';
import { spawn, spawnSync } from 'node:child_process';
import { mkdir, readFile, rm, symlink, unlink, utimes, watch, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { join } from 'node:path';
import { guardEvent } from './__tests__/counted-cargo.js';
import { fixtureNxEnv, stopFixtureNxDaemon, withNxFixture } from './__tests__/fixture-nx-env.js';
import { awaitExit, isRunning, ownedWorkspaceSocketDirectory, processTable, terminate } from './testing.js';

/**
 * Guards the daemon-claim hunk of `patches/nx@23.2.1.patch`. Every Nx 23.2.1 daemon overwrote the workspace's
 * `server-process.json` with its own pid, so two clients that both found no daemon started two, and the later one
 * displaced the first: the first's clients lost their socket in the middle of a request (EPIPE on
 * `RECORD_OUTPUTS_HASH_BATCH`). Under a configured `NX_SOCKET_DIR` both daemons bind the same path, and closing a
 * listening Unix socket unlinks its path: the displaced daemon's shutdown removed the successor's socket, so the
 * successor ran on unreachable, the next client started yet another daemon, and that one displaced it the same way.
 *
 * A daemon now claims the record exclusively, and a second daemon started while one serves the workspace exits without
 * touching the record or the socket. A daemon whose socket is gone, which no client can reach, exits instead of
 * holding the record, and leaves a path another daemon bound alone. A record whose live pid has not answered for longer
 * than a daemon takes to start is stale, whatever process holds that pid now. Drop the hunk, and keep this test, once
 * the Nx version in use leaves a serving daemon alone.
 *
 * Nx's default socket root is the literal `/tmp/.nx` with the daemon's pid hashed into the name, which a sandbox does
 * not admit. So the two daemons' socket paths are named: one directory each, which is what two daemons on the default
 * root bind, and one directory for both, which is what a configured `NX_SOCKET_DIR` does.
 */

const repositoryRoot = join(import.meta.dir, '../../..');
const nxEntry = join(repositoryRoot, 'node_modules', '.bin', 'nx');
const daemonStart = createRequire(join(repositoryRoot, 'package.json')).resolve('nx/src/daemon/server/start.js');

interface DaemonRecord {
  readonly processId: number;
  readonly socketPath: string;
}

async function daemonRecord(workspace: string): Promise<DaemonRecord | null> {
  const text = await readFile(join(workspace, '.nx/workspace-data/d/server-process.json'), 'utf8').catch(
    (error: unknown) => {
      if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return null;
      throw error;
    },
  );
  if (text === null) return null;
  const record: unknown = JSON.parse(text);
  if (
    typeof record !== 'object' ||
    record === null ||
    !('processId' in record) ||
    typeof record.processId !== 'number' ||
    !('socketPath' in record) ||
    typeof record.socketPath !== 'string'
  ) {
    throw new Error(`the fixture daemon record names no process and socket: ${text}`);
  }
  return { processId: record.processId, socketPath: record.socketPath };
}

async function fixtureWorkspace(workspace: string): Promise<void> {
  await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
  await writeFile(join(workspace, 'nx.json'), '{}\n');
  await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'daemon-claim', private: true }));
  await mkdir(join(workspace, 'p'));
  await writeFile(join(workspace, 'p', 'project.json'), JSON.stringify({ name: 'p' }));
}

function nx(workspace: string, env: Record<string, string>, ...args: string[]): string {
  const ran = spawnSync('bun', [nxEntry, ...args], { cwd: workspace, env, encoding: 'utf8' });
  expect(ran.status, `nx ${args.join(' ')}\n${ran.stdout}${ran.stderr}`).toBe(0);
  return ran.stdout;
}

async function startedDaemon(workspace: string, env: Record<string, string>): Promise<DaemonRecord> {
  nx(workspace, env, 'daemon', '--start');
  const record = await daemonRecord(workspace);
  if (record === null) throw new Error('nx daemon --start left no daemon record');
  return record;
}

/**
 * A socket directory of its own, made below the workspace's short socket alias so that a configured `NX_SOCKET_DIR`
 * names a real directory inside the workspace, which the sandbox admits. The caller stops any daemon bound in it before
 * this returns; this removes only the directory it made, and `force` takes what is left.
 */
async function withSocketDir<T>(workspace: string, slot: 'a' | 'b', body: (dir: string) => Promise<T>): Promise<T> {
  const dir = await ownedWorkspaceSocketDirectory(workspace, slot);
  try {
    return await body(dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

/**
 * A daemon serves `workspace` under `servingEnv`, then a second one is started under `challengerEnv`. The two share
 * a socket path only when their environments name one.
 */
async function secondDaemonDefers(
  workspace: string,
  servingEnv: Record<string, string>,
  challengerEnv: Record<string, string>,
): Promise<void> {
  const serving = await startedDaemon(workspace, servingEnv);

  // What a second client does when its probe found no daemon: start one. The record's directory is watched from
  // before the spawn, so no write the second daemon makes to it can be missed.
  const recordChanges = new AbortController();
  const changes = watch(join(workspace, '.nx/workspace-data/d'), { signal: recordChanges.signal });
  let output = '';
  const second = spawn(process.execPath, [daemonStart], {
    cwd: workspace,
    env: challengerEnv,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  second.stdout.setEncoding('utf8').on('data', (text: string) => {
    output += text;
  });
  second.stderr.setEncoding('utf8').on('data', (text: string) => {
    output += text;
  });
  const exited = new Promise<number | null>((resolve) => second.once('exit', resolve));
  // Unpatched, the second daemon never exits: it takes the record and serves. Either ends the wait.
  let displaced = false;
  const tookRecord = (async () => {
    try {
      for await (const _ of changes) {
        if ((await daemonRecord(workspace))?.processId === second.pid) {
          displaced = true;
          return;
        }
      }
    } catch (error) {
      if (!(error instanceof Error && error.name === 'AbortError')) throw error;
    }
  })();
  try {
    await guardEvent(
      Promise.race([exited, tookRecord]),
      'the second daemon to exit or take the record',
      () => `second daemon ${second.pid}, exit ${second.exitCode}\n${output}`,
    );
    expect(displaced, `the second daemon ${second.pid} took the record from ${serving.processId}\n${output}`).toBe(
      false,
    );
    expect(await exited, output).toBe(0);
    expect(await daemonRecord(workspace)).toEqual(serving);
    expect(isRunning(serving.processId)).toBe(true);

    // The serving daemon still answers on its socket: no client starts another.
    expect(JSON.parse(nx(workspace, servingEnv, 'show', 'projects', '--json'))).toEqual(['p']);
    expect(await daemonRecord(workspace)).toEqual(serving);
  } finally {
    if (second.pid !== undefined && second.exitCode === null) {
      terminate(second.pid);
      await exited;
    }
    recordChanges.abort();
    await tookRecord;
    // Stopped under the environment that started it, before the caller removes the socket directory it bound.
    await stopFixtureNxDaemon(workspace, servingEnv);
  }
}

describe('a second daemon started for a workspace a daemon serves', () => {
  it('exits and leaves the serving daemon alone when each binds a socket directory of its own', async () => {
    await withNxFixture('nx-daemon-claim-', async ({ socketDirectory, workspace }) => {
      await fixtureWorkspace(workspace);
      const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true' };
      await withSocketDir(workspace, 'a', async (servingDir) => {
        await withSocketDir(workspace, 'b', async (challengerDir) => {
          await secondDaemonDefers(
            workspace,
            { ...env, NX_SOCKET_DIR: servingDir },
            { ...env, NX_SOCKET_DIR: challengerDir },
          );
        });
      });
    });
  });

  it('exits and leaves the serving daemon alone when both use one socket directory', async () => {
    await withNxFixture('nx-daemon-claim-shared-', async ({ socketDirectory, workspace }) => {
      await fixtureWorkspace(workspace);
      await withSocketDir(workspace, 'a', async (socketDir) => {
        const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true', NX_SOCKET_DIR: socketDir };
        await secondDaemonDefers(workspace, env, env);
      });
    });
  });
});

it('replaces a daemon whose socket was removed, which no client could reach', async () => {
  await withNxFixture('nx-daemon-claim-lost-', async ({ socketDirectory, workspace }) => {
    await fixtureWorkspace(workspace);
    const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true' };
    const lost = await startedDaemon(workspace, env);
    const lostProcess = (await processTable()).filter((entry) => entry.pid === lost.processId);
    // What a temp-directory reaper does to a socket that has not been used for days.
    await unlink(lost.socketPath);
    await awaitExit(lostProcess, `the Nx daemon ${lost.processId}, whose socket was removed`);

    expect(JSON.parse(nx(workspace, env, 'show', 'projects', '--json'))).toEqual(['p']);
    const replacement = await daemonRecord(workspace);
    expect(replacement?.processId).not.toBe(lost.processId);
  });
});

it('keeps serving clients whose socket directory differs from the one it bound', async () => {
  await withNxFixture('nx-daemon-claim-env-', async ({ socketDirectory, workspace }) => {
    await fixtureWorkspace(workspace);
    const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true' };
    await withSocketDir(workspace, 'a', async (socketDir) => {
      await withSocketDir(workspace, 'b', async (otherSocketDir) => {
        const servingEnv = { ...env, NX_SOCKET_DIR: socketDir };
        try {
          const serving = await startedDaemon(workspace, servingEnv);
          // The daemon takes on each client's environment; NX_SOCKET_DIR is part of it.
          expect(JSON.parse(nx(workspace, env, 'show', 'projects', '--json'))).toEqual(['p']);
          expect(
            JSON.parse(nx(workspace, { ...env, NX_SOCKET_DIR: otherSocketDir }, 'show', 'projects', '--json')),
          ).toEqual(['p']);
          expect(await daemonRecord(workspace)).toEqual(serving);
          expect(isRunning(serving.processId)).toBe(true);
        } finally {
          // Stopped under the environment that started it, before the caller removes the socket directory it bound.
          await stopFixtureNxDaemon(workspace, servingEnv);
        }
      });
    });
  });
});

it('replaces a record whose pid now belongs to an unrelated live process', async () => {
  await withNxFixture('nx-daemon-claim-reused-', async ({ socketDirectory, workspace }) => {
    await fixtureWorkspace(workspace);
    const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true' };
    // A process that lives until its stdin closes: what a daemon's pid can be reused by after a crash.
    const unrelated = spawn(process.execPath, ['-e', 'process.stdin.resume()'], {
      stdio: ['pipe', 'ignore', 'ignore'],
    });
    const unrelatedExited = new Promise<void>((resolve) => unrelated.once('exit', () => resolve()));
    try {
      if (unrelated.pid === undefined) throw new Error('the unrelated process did not start');
      const nxPackage: unknown = JSON.parse(
        await readFile(join(repositoryRoot, 'node_modules/nx/package.json'), 'utf8'),
      );
      if (typeof nxPackage !== 'object' || nxPackage === null || !('version' in nxPackage)) {
        throw new Error('the installed nx package names no version');
      }
      const record = join(workspace, '.nx/workspace-data/d/server-process.json');
      await mkdir(join(workspace, '.nx/workspace-data/d'), { recursive: true });
      await writeFile(
        record,
        `${JSON.stringify({
          processId: unrelated.pid,
          socketPath: join(workspace, 'crashed.sock'),
          nxVersion: nxPackage.version,
          workspaceRoot: workspace,
        })}\n`,
      );
      // Written by a daemon that crashed long before this client came.
      const crashed = new Date(Date.now() - 10 * 60_000);
      await utimes(record, crashed, crashed);

      const started = Date.now();
      expect(JSON.parse(nx(workspace, env, 'show', 'projects', '--json'))).toEqual(['p']);
      // Well inside the minute a client waits for a daemon, which a deferral to the stale record would use up.
      expect(Date.now() - started).toBeLessThan(30_000);
      expect((await daemonRecord(workspace))?.processId).not.toBe(unrelated.pid);
      expect(isRunning(unrelated.pid)).toBe(true);
    } finally {
      unrelated.stdin.end();
      await unrelatedExited;
    }
  });
});
