import { expect, it } from 'bun:test';
import { mkdir, readFile, symlink, utimes, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

/**
 * Guards the racy-archive hunk of `patches/nx@23.2.1.patch`. Every Nx 23.2.1
 * context (each daemonless run, and the daemon at its start) hashes the
 * workspace's files through `<workspace-data>/nx_files.nxt`, which on Unix
 * keeps each file's hash beside its mtime in whole seconds, and reuses an
 * archived hash while that second is unchanged (`selective_files_hash`; size
 * and nanoseconds are not compared). A file written again in the second it
 * was hashed in keeps its mtime, so the next run hashes it as the bytes it
 * replaced: a revert replays the cache entry of the edit it reverts, and a
 * new edit hits the old entry. The patch applies Git's racy-index rule: an
 * archive holding an mtime from the seconds its own hashing pass ran in, or
 * later, is not trusted, and every file is hashed again.
 *
 * The case writes each input with a fixed mtime second and gives the archive
 * the same second, as if the input had been written and hashed in the second
 * the archive was written: the race, without depending on the clock. Last,
 * an input dated a minute ahead of the archive, as under a clock that runs
 * ahead, is rewritten under that date.
 */

const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const repoRoot = dirname(dirname(packageRoot));
const nxEntry = join(repoRoot, 'node_modules', '.bin', 'nx');

it('hashes an input rewritten in the second its archived hash was taken', async () => {
  await withNxFixture('nx-files-archive-racy-', async ({ workspace }) => {
    await mkdir(join(workspace, 'app'), { recursive: true });
    const git = Bun.spawnSync(['git', 'init', '--quiet', workspace]);
    expect(git.exitCode, git.stderr.toString()).toBe(0);
    // The repository's own node_modules: the Nx under test.
    await symlink(join(repoRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
    await writeFile(join(workspace, 'nx.json'), JSON.stringify({ cacheDirectory: '.nx/cache' }));
    await writeFile(
      join(workspace, 'app', 'project.json'),
      JSON.stringify({
        name: 'app',
        targets: {
          build: {
            executor: 'nx:run-commands',
            cache: true,
            inputs: ['{projectRoot}/source.txt'],
            outputs: ['{projectRoot}/result.txt'],
            options: { cwd: 'app', command: 'cp source.txt result.txt' },
          },
        },
      }),
    );
    // The output stays out of the file hashes and every other file is dated an hour back, so only
    // the input's dates come near an archive's write.
    await writeFile(join(workspace, '.gitignore'), 'app/result.txt\n');
    const anHourAgo = Math.floor(Date.now() / 1000) - 3600;
    for (const file of ['.gitignore', 'nx.json', 'app/project.json']) {
      await utimes(join(workspace, file), anHourAgo, anHourAgo);
    }
    const env = fixtureNxEnv(workspace);
    const source = join(workspace, 'app', 'source.txt');
    const archive = join(env.NX_WORKSPACE_DATA_DIRECTORY, 'nx_files.nxt');
    // Writes the input dated `second`, builds, and reports the log and the output.
    const build = async (text: string, second: number) => {
      await writeFile(source, text);
      await utimes(source, second, second);
      const ran = Bun.spawnSync(['bun', nxEntry, 'run', 'app:build', '--outputStyle=static'], { cwd: workspace, env });
      const log = `${ran.stdout}${ran.stderr}`;
      expect(ran.exitCode, log).toBe(0);
      return { log, result: await readFile(join(workspace, 'app', 'result.txt'), 'utf8') };
    };

    const first = Math.floor(Date.now() / 1000) - 60;
    expect((await build('A', first)).result).toBe('A');
    const second = first + 1;
    expect((await build('B', second)).result).toBe('B');

    // The archive written in B's second, then a revert to A within that second: A's entry, never B's.
    await utimes(archive, second, second);
    const reverted = await build('A', second);
    expect(reverted.result).toBe('A');

    // Again, and a new edit within that second: no entry at all.
    await utimes(archive, second, second);
    const edited = await build('C', second);
    expect(edited.log).not.toContain('[local cache]');
    expect(edited.result).toBe('C');

    // An input dated ahead of the archive's write, rewritten under that date: no entry either.
    const ahead = Math.floor(Date.now() / 1000) + 60;
    expect((await build('D', ahead)).result).toBe('D');
    const skewed = await build('E', ahead);
    expect(skewed.log).not.toContain('[local cache]');
    expect(skewed.result).toBe('E');
  });
}, 60_000);
