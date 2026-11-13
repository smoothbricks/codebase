import { expect, it } from 'bun:test';
import { lstat, mkdir, readlink, rm, symlink, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

/**
 * Guards the linked-output hunks of `patches/nx@24.0.3.patch` (patches/README.md, "A restore keeps a linked output").
 * A task whose declared output is a relative link to a file outside its outputs (a consumer's runtime Bun,
 * `.runtime/bun -> ../.cache/runtime-bun/bun`) is cached as the link. The native restore removed the workspace's link
 * and copied nothing back: it follows the link inside the entry, where its target is not. The daemon then recorded the
 * hash of what the restore left, no file, and every later hit answered "existing outputs match the cache, left as is"
 * for an output that was gone; each task that ran the tool failed with `No such file or directory`.
 */

const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const repoRoot = dirname(dirname(packageRoot));
const nxEntry = join(repoRoot, 'node_modules', '.bin', 'nx');
const LINK = 'app/.runtime/tool';
const TARGET = '../.cache/tool';

/** What a run said and where its output link points, or null when it is absent. */
interface Run {
  readonly log: string;
  readonly link: string | null;
}

async function linkedFixture(workspace: string, env: Record<string, string>): Promise<() => Promise<Run>> {
  await mkdir(join(workspace, 'app', '.cache'), { recursive: true });
  const git = Bun.spawnSync(['git', 'init', '--quiet', workspace]);
  expect(git.exitCode, git.stderr.toString()).toBe(0);
  await symlink(join(repoRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
  await writeFile(join(workspace, 'nx.json'), JSON.stringify({ cacheDirectory: '.nx/cache' }));
  await writeFile(join(workspace, 'app', '.cache', 'tool'), 'the tool');
  await writeFile(
    join(workspace, 'app', 'project.json'),
    JSON.stringify({
      name: 'app',
      targets: {
        link: {
          executor: 'nx:run-commands',
          cache: true,
          inputs: ['{projectRoot}/project.json'],
          outputs: ['{projectRoot}/.runtime/tool'],
          options: { cwd: 'app', command: `mkdir -p .runtime && ln -sfn ${TARGET} .runtime/tool` },
        },
      },
    }),
  );
  return async () => {
    const child = Bun.spawn(['bun', nxEntry, 'run', 'app:link', '--outputStyle=static'], {
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
    const present = await lstat(join(workspace, LINK)).catch(() => null);
    return { log, link: present?.isSymbolicLink() ? await readlink(join(workspace, LINK)) : null };
  };
}

it.each([
  { daemon: 'without a daemon', env: { NX_DAEMON: 'false' } },
  // The fixture's own daemon, which the job rule would refuse to start; the fixture stops it at retirement.
  { daemon: 'with a daemon', env: { NX_DAEMON: 'true' } },
] as const)(
  'restores a linked output removed outside Nx, $daemon, and keeps it only while it is there',
  async ({ env: daemon }) => {
    await withNxFixture('nx-linked-output-', async ({ workspace, socketDirectory }) => {
      const env: Record<string, string> = { ...fixtureNxEnv(workspace, socketDirectory), ...daemon };
      delete env.NX_DAEMON_START;
      const run = await linkedFixture(workspace, env);

      expect((await run()).link).toBe(TARGET);
      const kept = await run();
      expect(kept.log).toContain('[existing outputs match the cache, left as is]');
      expect(kept.link).toBe(TARGET);

      await rm(join(workspace, LINK));
      const restored = await run();
      expect(restored.link, restored.log).toBe(TARGET);
      expect(restored.log).toContain('[local cache]');
      // A fresh restore into a workspace without the output's directory brings it back too.
      await rm(join(workspace, 'app', '.runtime'), { recursive: true });
      const fresh = await run();
      expect(fresh.link, fresh.log).toBe(TARGET);
      expect(fresh.log).toContain('[local cache]');
    });
  },
);
