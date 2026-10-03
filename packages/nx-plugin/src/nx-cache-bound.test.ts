import { expect, it } from 'bun:test';
import { execFileSync, spawn } from 'node:child_process';
import { mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { guardEvent } from './__tests__/counted-cargo.js';
import { fixtureNxEnv } from './__tests__/fixture-nx-env.js';

/**
 * Guards the cache-bound hunk of `patches/nx@23.2.1.patch`, the local build of
 * nrwl/nx#37269. When nx.json sets no `maxCacheSize`, Nx 23.2.1 bounds its
 * cache at a tenth of the disk holding it, and asks its native
 * `getDefaultMaxCacheSize`, which lists every mounted disk (`sysinfo`
 * `Disks::new_with_refreshed_list`). On macOS that is a `getfsstat` plus IOKit
 * and CacheDelete round trips per mount, on the client's main thread, between
 * hashing and the first task: about 80 ms on a quiet host, and up to 2.3 s
 * measured while disk images attach, detach or resize. Fixture `release pack`
 * runs stalled 16 s and 24 s in that same window with their daemon idle; that
 * this call was the stall is inferred, not captured. The patched bound asks
 * only the cache directory's filesystem (`statfs`).
 *
 * The fixture's Nx runs with that native call replaced by one that throws, so
 * a run that still consults the host's disk inventory fails instead of passing
 * whenever the host happens to be quiet. Drop the hunk, and keep this test,
 * once the Nx version in use contains that fix.
 */

const repositoryRoot = join(import.meta.dir, '../../..');

/** Loaded into every fixture Nx process before Nx itself. */
const FORBID_DISK_INVENTORY = `require('nx/src/native').getDefaultMaxCacheSize = () => {
  throw new Error('Nx enumerated the host disks to bound its cache');
};
`;

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

it('bounds the default cache from its own filesystem without listing the host disks', async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'nx-cache-bound-')));
  try {
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
    await writeFile(join(root, 'forbid-disk-inventory.cjs'), FORBID_DISK_INVENTORY);
    execFileSync('git', ['init', '--quiet', root], { stdio: 'pipe' });

    // The bound is taken by the client that runs the tasks, daemon or not.
    // The cache directory does not exist yet, as on a checkout's first run.
    const { code, output } = await runNx(root, {
      ...fixtureNxEnv(root),
      NX_DAEMON: 'false',
      NODE_OPTIONS: `--require ${join(root, 'forbid-disk-inventory.cjs')}`,
    });
    expect(output).not.toContain('Nx enumerated the host disks');
    expect(code, output).toBe(0);
    expect(await readFile(join(root, 'app/result.txt'), 'utf8')).toBe('built');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
