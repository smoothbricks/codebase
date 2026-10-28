import { expect, it } from 'bun:test';
import { appendFile, mkdir, readFile, rm, stat, symlink, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

/**
 * Guards the outputs-stamp hunk of `patches/nx@23.2.1.patch`. Nx 23.2.1 keeps
 * a cache hit's outputs in place only when its daemon recorded them
 * (`outputsHashesMatchBatch`); a client with no daemon "can't verify on-disk
 * outputs, always copy". A cowshed job's client starts none
 * (`NX_DAEMON_START=never`), and main keeps none, so every cached task with
 * outputs was restored on every run: the restore rewrote each output and the
 * restore-time hunk re-dated it, so a producer that runs another task's output
 * as its tool never saw the same tool twice and no producer receipt could
 * vouch for it. The patched run records each task's outputs (path, inode,
 * size, mtime, ctime) after it runs or restores them, and a later hit keeps
 * outputs that are still exactly those files.
 *
 * Drop the hunk, and keep this test, once the Nx version in use verifies
 * on-disk outputs without a daemon.
 */

const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const repoRoot = dirname(dirname(packageRoot));
const nxEntry = join(repoRoot, 'node_modules', '.bin', 'nx');

const OUTPUTS = ['app/result.txt', 'app/dist/nested/copy.txt'] as const;

interface Output {
  readonly text: string;
  readonly ino: bigint;
  readonly mtimeNs: bigint;
}

interface Run {
  readonly log: string;
  readonly outputs: readonly Output[];
}

/** A fixture workspace with one cached `app:build` copying its input into a file output and a directory output. */
async function keptFixture(workspace: string, env: Record<string, string>): Promise<() => Promise<Run>> {
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
          outputs: ['{projectRoot}/result.txt', '{projectRoot}/dist/'],
          options: {
            cwd: 'app',
            command: 'cp source.txt result.txt && mkdir -p dist/nested && cp source.txt dist/nested/copy.txt',
          },
        },
      },
    }),
  );
  await writeFile(join(workspace, 'app', 'source.txt'), 'A');
  return async () => {
    const child = Bun.spawn(['bun', nxEntry, 'run', 'app:build', '--outputStyle=static'], {
      cwd: workspace,
      env,
      stdout: 'pipe',
      stderr: 'pipe',
    });
    const [exitCode, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ]);
    const log = `${stdout}${stderr}`;
    expect(exitCode, log).toBe(0);
    const outputs = await Promise.all(
      OUTPUTS.map(async (output) => {
        const stats = await stat(join(workspace, output), { bigint: true });
        return { text: await readFile(join(workspace, output), 'utf8'), ino: stats.ino, mtimeNs: stats.mtimeNs };
      }),
    );
    return { log, outputs };
  };
}

it('keeps a cache hit whose outputs are untouched, without a daemon, and restores outputs changed since', async () => {
  await withNxFixture('nx-kept-outputs-', async ({ workspace, socketDirectory }) => {
    const run = await keptFixture(workspace, fixtureNxEnv(workspace, socketDirectory));

    const built = await run();
    expect(built.log).not.toContain('[local cache]');

    const kept = await run();
    expect(kept.log).toContain('[existing outputs match the cache, left as is]');
    expect(kept.outputs, 'untouched outputs stay the very files the run left').toEqual(built.outputs);

    // A kept hit is kept again: keeping them recorded nothing new and changed nothing.
    expect((await run()).outputs).toEqual(built.outputs);

    // A write to an output since the record restores it.
    await appendFile(join(workspace, OUTPUTS[0]), 'edited');
    const restored = await run();
    expect(restored.log).toContain('[local cache]');
    expect(restored.outputs.map((output) => output.text)).toEqual(OUTPUTS.map(() => 'A'));

    // The restore is recorded: the next hit keeps what it restored.
    const keptAfterRestore = await run();
    expect(keptAfterRestore.log).toContain('[existing outputs match the cache, left as is]');
    expect(keptAfterRestore.outputs).toEqual(restored.outputs);

    // A removed output file restores.
    await rm(join(workspace, OUTPUTS[1]));
    const refilled = await run();
    expect(refilled.log).toContain('[local cache]');
    expect(refilled.outputs.map((output) => output.text)).toEqual(OUTPUTS.map(() => 'A'));
  });
});
