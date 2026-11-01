import { expect, it } from 'bun:test';
import { mkdir, readFile, symlink, utimes, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

/**
 * Guards Git's racy-index rule for Nx's file hashes, which Nx 24 applies natively (this repository carried a JS check
 * of the archive as a patch hunk on Nx 23.2.1). Every Nx context (each daemonless run, and the daemon at its start)
 * hashes the workspace's files through its archive in the workspace data directory, which on Unix keeps each file's
 * hash beside its mtime in whole seconds and reuses an archived hash while that second is unchanged; size and
 * nanoseconds are not compared. A file written again in the second it was hashed in keeps its mtime, so a reused hash
 * would hash it as the bytes it replaced: a revert would replay the cache entry of the edit it reverts, and a new edit
 * would hit the old entry. Nx 24's archive (`nx_files_v2.nxt`) records when the gather that wrote it began
 * (`gathered_at`) and reuses no entry whose mtime is that second or later.
 *
 * The case dates each input a minute ahead, so every gather below begins before the input's mtime second: the race,
 * without depending on the clock. Each rewrite within that second must hash the bytes written, never an entry.
 */

const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const repoRoot = dirname(dirname(packageRoot));
const nxEntry = join(repoRoot, 'node_modules', '.bin', 'nx');

it('hashes an input rewritten in the second its archived hash was taken', async () => {
  await withNxFixture('nx-files-archive-racy-', async ({ workspace, socketDirectory }) => {
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
    const env = fixtureNxEnv(workspace, socketDirectory);
    const source = join(workspace, 'app', 'source.txt');
    // Writes the input dated `second`, builds, and reports the log and the output.
    const build = async (text: string, second: number) => {
      await writeFile(source, text);
      await utimes(source, second, second);
      const ran = Bun.spawnSync(['bun', nxEntry, 'run', 'app:build', '--outputStyle=static'], { cwd: workspace, env });
      const log = `${ran.stdout}${ran.stderr}`;
      expect(ran.exitCode, log).toBe(0);
      return { log, result: await readFile(join(workspace, 'app', 'result.txt'), 'utf8') };
    };

    // Every build below gathers before this second, so each input dated in it was read while it could still change.
    const racy = Math.floor(Date.now() / 1000) + 60;
    expect((await build('A', racy)).result).toBe('A');
    expect((await build('B', racy)).result).toBe('B');

    // A revert within that second: A's entry, never B's.
    const reverted = await build('A', racy);
    expect(reverted.result).toBe('A');

    // A new edit within that second: no entry at all.
    const edited = await build('C', racy);
    expect(edited.log).not.toContain('[local cache]');
    expect(edited.result).toBe('C');
  });
});
