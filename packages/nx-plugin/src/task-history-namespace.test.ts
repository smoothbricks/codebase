import { Database } from 'bun:sqlite';
import { expect, it } from 'bun:test';
import { execFileSync, spawn } from 'node:child_process';
import { mkdir, readdir, readFile, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { guardEvent } from './__tests__/counted-cargo.js';
import { fixtureNxEnv, stopFixtureNxDaemon, withNxFixture } from './__tests__/fixture-nx-env.js';

/**
 * Guards `patches/nx@23.2.1.patch`, the local build of nrwl/nx#37268 (issue
 * nrwl/nx#37118): Nx 23.2.1 forwards task history to its daemon, whose database
 * scope can differ from the client's, and a run then exits 1 with SQLite
 * FOREIGN KEY 787 after its tasks succeed. Drop the patch, and keep this test,
 * once the Nx version in use contains that fix.
 */

const repositoryRoot = join(import.meta.dir, '../../..');

async function runNx(root: string, env: Record<string, string>, configuration: string): Promise<string> {
  const child = spawn(
    'node',
    [join(repositoryRoot, 'node_modules/.bin/nx'), 'run', `app:build:${configuration}`, '--outputStyle=static'],
    {
      cwd: root,
      env,
      detached: process.platform !== 'win32',
      stdio: ['ignore', 'pipe', 'pipe'],
    },
  );
  let output = '';
  child.stdout?.setEncoding('utf8').on('data', (text: string) => {
    output += text;
  });
  child.stderr?.setEncoding('utf8').on('data', (text: string) => {
    output += text;
  });
  const { promise: ended, resolve, reject } = Promise.withResolvers<number | null>();
  child.once('error', reject);
  child.once('close', resolve);
  try {
    const code = await guardEvent(
      ended,
      `Nx app:build:${configuration} to close`,
      () => `pid ${child.pid}, exit ${child.exitCode}, signal ${child.signalCode}\n${output}`,
    );
    expect(code, output).toBe(0);
    return output;
  } catch (error) {
    if (child.pid !== undefined && child.exitCode === null && child.signalCode === null) {
      try {
        process.kill(process.platform === 'win32' ? child.pid : -child.pid, 'SIGKILL');
      } catch (killError) {
        if (!(killError instanceof Error && 'code' in killError && killError.code === 'ESRCH')) {
          throw new AggregateError([error, killError], 'could not retire the failed fixture Nx command');
        }
      }
    }
    await ended.catch(() => undefined);
    throw error;
  }
}

async function recordedRuns(directory: string) {
  const files = (await readdir(directory)).filter((file) => file.endsWith('.db'));
  expect(files).toHaveLength(1);
  const file = files[0];
  if (file === undefined) throw new Error(`no Nx database in ${directory}`);
  // Nx keeps this database in WAL mode, and its last connection deletes the
  // -wal/-shm pair on close. A SQLITE_OPEN_READONLY connection cannot recreate
  // the -shm index and fails with SQLITE_CANTOPEN, so read through an ordinary
  // read-write (never create) connection, which also sees any un-checkpointed
  // WAL frames. The query below only reads.
  const database = new Database(join(directory, file), { readwrite: true });
  try {
    return database
      .query<{ configuration: string; status: string; code: number; cacheCode: number }, []>(`
      SELECT details.configuration, history.status, history.code, cache.code AS cacheCode
      FROM task_history history
      JOIN task_details details ON details.hash = history.hash
      JOIN cache_outputs cache ON cache.hash = history.hash
      WHERE history.status = 'success'
      ORDER BY history.id
    `)
      .all();
  } finally {
    database.close();
  }
}

it('records task history in the client database when the running daemon resolved the shared one', async () => {
  await withNxFixture(
    'nx-history-owner-',
    async ({ root: base, workspace: root }) => {
      // Nx shares its DB and cache across a repository's checkouts under ~/.nx/<id>;
      // a private HOME keeps that shared scope inside the fixture.
      const home = join(base, 'home');
      const { NX_WORKSPACE_DATA_DIRECTORY: _data, NX_CACHE_DIRECTORY: _cache, ...defaultScope } = fixtureNxEnv(root);
      // NX_DAEMON: this regression needs the daemon, which fixtureNxEnv turns off.
      const sharedEnv = { ...defaultScope, HOME: home, NX_DAEMON: 'true' };
      // Naming the checkout's default workspace-data directory explicitly keeps the
      // same daemon record, but any explicit data/cache directory turns Nx's sharing
      // off: this client's task details and cache rows land in the checkout DB.
      const localEnv = { ...fixtureNxEnv(root), HOME: home, NX_DAEMON: 'true' };
      const localData = join(root, '.nx/workspace-data');
      await mkdir(join(root, 'app'), { recursive: true });
      await mkdir(home);
      await symlink(join(repositoryRoot, 'node_modules'), join(root, 'node_modules'), 'dir');
      await writeFile(join(root, '.gitignore'), 'node_modules\n.nx\nexecutions.log\napp/result.txt\n');
      await writeFile(join(root, 'package.json'), JSON.stringify({ name: 'history-owner-fixture', private: true }));
      await writeFile(join(root, 'nx.json'), '{}\n');
      await writeFile(
        join(root, 'app/project.json'),
        JSON.stringify({
          name: 'app',
          targets: {
            build: {
              executor: 'nx:run-commands',
              cache: true,
              inputs: ['{projectRoot}/build.cjs'],
              outputs: ['{projectRoot}/result.txt'],
              options: { cwd: 'app' },
              configurations: {
                first: { command: 'node build.cjs first' },
                second: { command: 'node build.cjs second' },
              },
            },
          },
        }),
      );
      await writeFile(
        join(root, 'app/build.cjs'),
        `const fs = require('node:fs');\nconst value = process.argv[2];\nfs.writeFileSync('result.txt', value);\nfs.appendFileSync('../executions.log', value + '\\n');\n`,
      );

      execFileSync('git', ['init', '--quiet', root], { stdio: 'pipe' });
      // A remote is what gives the checkout the repository identity Nx shares by.
      execFileSync('git', ['-C', root, 'remote', 'add', 'origin', 'https://github.com/example/history-owner.git'], {
        stdio: 'pipe',
      });
      const firstOutput = await runNx(root, sharedEnv, 'first');
      const daemonRecord = join(localData, 'd/server-process.json');
      const initialDaemon = await readFile(daemonRecord, 'utf8').catch(async (error: unknown) => {
        const log = await readFile(join(localData, 'd/daemon.log'), 'utf8').catch(
          (readError: unknown) => `daemon log unreadable: ${String(readError)}`,
        );
        throw new Error(`fixture Nx did not retain its normal daemon record\n${firstOutput}\n${log}`, { cause: error });
      });

      // Same daemon, but this client's TaskDetails select the checkout DB. Unpatched
      // Nx 23.2.1 forwards RECORD_TASK_RUNS to the daemon, whose DB scope was frozen
      // shared at startup and lacks the new hash: after the successful task footer
      // it prints "DB transaction error ... extended_code: 787" and exits 1.
      await runNx(root, localEnv, 'second');
      expect(await readFile(daemonRecord, 'utf8')).toBe(initialDaemon);
      expect(await readFile(join(root, 'executions.log'), 'utf8')).toBe('first\nsecond\n');

      // The checkout cache answers the repeat: the recorded hash is usable.
      await runNx(root, localEnv, 'second');
      expect(await readFile(join(root, 'executions.log'), 'utf8')).toBe('first\nsecond\n');
      expect(await readFile(join(root, 'app/result.txt'), 'utf8')).toBe('second');
      expect(await readFile(daemonRecord, 'utf8')).toBe(initialDaemon);
      // Stop the fixture's daemon so no Nx process still holds either database
      // when the independent SQLite reader inspects them.
      await stopFixtureNxDaemon(root);
      const [sharedId, ...otherShared] = await readdir(join(home, '.nx'));
      expect(otherShared).toEqual([]);
      if (sharedId === undefined) throw new Error(`no shared Nx scope under ${home}/.nx`);
      expect(await recordedRuns(join(home, '.nx', sharedId, 'databases'))).toEqual([
        { configuration: 'first', status: 'success', code: 0, cacheCode: 0 },
      ]);
      expect(await recordedRuns(localData)).toEqual([
        { configuration: 'second', status: 'success', code: 0, cacheCode: 0 },
      ]);
    },
    'workspace',
  );
});
