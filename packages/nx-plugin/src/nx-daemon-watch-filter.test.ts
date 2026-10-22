import { expect, it } from 'bun:test';
import { execFileSync, spawnSync } from 'node:child_process';
import { mkdir, readFile, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { WorkspaceContext } from 'nx/src/native/index.js';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

/**
 * Guards the workspace filter of the single-watcher hunk in `patches/nx@23.2.1.patch` (`daemon/server/watcher.js`). The
 * daemon's one native watcher reports every change but Nx's hard-coded ignores, and the daemon decides which reach the
 * workspace context with the ignore files Nx's own watcher read, in its order: the root .nxignore first, then each
 * directory's .gitignore nearest first, then the .gitignore files between a nested workspace and its git root. A file
 * the walker indexes but the filter drops leaves the daemon hashing its old bytes.
 */

const repositoryRoot = join(import.meta.dir, '../../..');
const nxEntry = join(repositoryRoot, 'node_modules', '.bin', 'nx');

function nx(
  workspace: string,
  env: Record<string, string>,
  ...args: string[]
): { status: number | null; output: string } {
  const ran = spawnSync('bun', [nxEntry, ...args], { cwd: workspace, env, encoding: 'utf8' });
  return { status: ran.status, output: `${ran.stdout}${ran.stderr}` };
}

/**
 * In the fixture `workspace`, with its ignore files already written: a cached target copying `input` must rebuild once
 * `input` changes. A fresh walk indexes `input`, so a daemon that hashes it from old bytes answers what no fresh walk
 * would.
 */
async function rebuildsAfterEdit(workspace: string, socketDirectory: string, input: string): Promise<void> {
  await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
  await writeFile(join(workspace, 'nx.json'), JSON.stringify({ targetDefaults: { copy: { cache: true } } }));
  await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'watch-filter', private: true }));
  await mkdir(join(workspace, 'p'), { recursive: true });
  await writeFile(
    join(workspace, 'p', 'project.json'),
    JSON.stringify({
      name: 'p',
      targets: {
        copy: {
          command: `cat ${input} > p/out.txt`,
          inputs: [`{workspaceRoot}/${input}`],
          outputs: ['{projectRoot}/out.txt'],
        },
      },
    }),
  );
  await writeFile(join(workspace, input), 'one\n');
  const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true', NX_TUI: 'false' };
  const run = async () => {
    const ran = nx(workspace, env, 'run', 'p:copy', '--outputStyle=static');
    expect(ran.status, ran.output).toBe(0);
    return { output: ran.output, out: await readFile(join(workspace, 'p', 'out.txt'), 'utf8') };
  };
  expect((await run()).out).toBe('one\n');
  await writeFile(join(workspace, input), 'two\n');
  const walked = new WorkspaceContext(workspace, join(workspace, '.nx/walk-proof')).allFileData();
  expect(walked.map(({ file }) => file)).toContain(input);
  const second = await run();
  expect(second.output).not.toContain('existing outputs match the cache');
  expect(second.out).toBe('two\n');
}

it('keeps a nested workspace negation of its parent repository ignore, as the walker does', async () => {
  // The git repository is the fixture root; the Nx workspace is nested in it.
  await withNxFixture(
    'nx-watch-filter-parent-',
    async ({ root, workspace, socketDirectory }) => {
      execFileSync('git', ['init', '--quiet', root]);
      await writeFile(join(root, '.gitignore'), '*.txt\n');
      await writeFile(join(workspace, '.gitignore'), 'node_modules\n.nx\n!keep.txt\n');
      await rebuildsAfterEdit(workspace, socketDirectory, 'keep.txt');
    },
    'workspace',
  );
});

it('lets the root .nxignore re-include what a nested .gitignore ignores, as the watcher and walker do', async () => {
  await withNxFixture('nx-watch-filter-nxignore-', async ({ workspace, socketDirectory }) => {
    await writeFile(join(workspace, '.gitignore'), 'node_modules\n.nx\np/out.txt\n');
    await writeFile(join(workspace, '.nxignore'), '!p/keep.txt\n');
    await mkdir(join(workspace, 'p'));
    await writeFile(join(workspace, 'p', '.gitignore'), '*.txt\n');
    await rebuildsAfterEdit(workspace, socketDirectory, 'p/keep.txt');
  });
});
