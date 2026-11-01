import { expect, it } from 'bun:test';
import { execFileSync, spawn, spawnSync } from 'node:child_process';
import { statfsSync } from 'node:fs';
import { mkdir, readFile, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { guardEvent } from './__tests__/counted-cargo.js';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

/**
 * Guards the cache bound of nrwl/nx#37269, which Nx carries natively since 24.0.2 (this repository carried it as a
 * patch hunk on 23.2.1). When nx.json sets no `maxCacheSize`, Nx bounds its cache at a tenth of the filesystem that
 * holds it. Nx 23.2.1's native `getDefaultMaxCacheSize` found that filesystem by listing every mounted disk
 * (`sysinfo` `Disks::new_with_refreshed_list`), on macOS a `getfsstat` plus IOKit and CacheDelete round trips per
 * mount on the client's main thread: about 80 ms on a quiet host, and up to 2.3 s while disk images attach, detach or
 * resize. Nx 24's asks only the cache directory's filesystem (`statfs`), through its nearest existing ancestor.
 *
 * The run below takes the default bound with no cache directory yet, as on a checkout's first run; the installed native
 * bound for a directory that does not exist must equal a tenth of its existing ancestor's filesystem.
 */

const repositoryRoot = join(import.meta.dir, '../../..');

async function runNx(root: string, env: Record<string, string>): Promise<{ code: number | null; output: string }> {
  const child = spawn(
    'node',
    [join(repositoryRoot, 'node_modules/.bin/nx'), 'run', 'app:build', '--outputStyle=static'],
    { cwd: root, env, detached: process.platform !== 'win32', stdio: ['ignore', 'pipe', 'pipe'] },
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
      'Nx app:build to close',
      () => `pid ${child.pid}, exit ${child.exitCode}, signal ${child.signalCode}\n${output}`,
    );
    return { code, output };
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

it('bounds the default cache from its own filesystem, whose directory may not exist yet', async () => {
  await withNxFixture('nx-cache-bound-', async ({ workspace: root, socketDirectory }) => {
    await mkdir(join(root, 'app'));
    await symlink(join(repositoryRoot, 'node_modules'), join(root, 'node_modules'), 'dir');
    await writeFile(join(root, '.gitignore'), 'node_modules\n.nx\napp/result.txt\n');
    await writeFile(join(root, 'package.json'), JSON.stringify({ name: 'cache-bound-fixture', private: true }));
    // No maxCacheSize: the bound is Nx's default, the code under test.
    await writeFile(join(root, 'nx.json'), '{}\n');
    await writeFile(
      join(root, 'app/project.json'),
      JSON.stringify({
        name: 'app',
        targets: {
          build: {
            executor: 'nx:run-commands',
            cache: true,
            inputs: [],
            outputs: ['{projectRoot}/result.txt'],
            options: { cwd: 'app', command: 'printf built > result.txt' },
          },
        },
      }),
    );
    execFileSync('git', ['init', '--quiet', root], { stdio: 'pipe' });

    // The bound is taken by the client that runs the tasks, daemon or not.
    // The cache directory does not exist yet, as on a checkout's first run.
    const { code, output } = await runNx(root, fixtureNxEnv(root, socketDirectory));
    expect(code, output).toBe(0);
    expect(await readFile(join(root, 'app/result.txt'), 'utf8')).toBe('built');
    // The native bound of a directory that does not exist is a tenth of the filesystem of its nearest existing ancestor.
    const missing = join(root, 'not', 'created', 'cache');
    const measured = spawnSync(
      'node',
      [
        '-e',
        `process.stdout.write(String(require('nx/src/native').getDefaultMaxCacheSize(${JSON.stringify(missing)})))`,
      ],
      { cwd: root, encoding: 'utf8' },
    );
    expect(measured.status, measured.stderr).toBe(0);
    const { blocks, bsize } = statfsSync(root);
    expect(Math.abs(Number(measured.stdout) - Math.floor((blocks * bsize) / 10))).toBeLessThanOrEqual(1);
  });
});
