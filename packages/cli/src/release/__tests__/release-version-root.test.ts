import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { mergeEnv } from '../../lib/run.js';
import type { ReleasePackageInfo } from '../core.js';
import { releasePackagesAtRef } from '../index.js';
import { git, tag, withFixtureRepo } from './helpers/fixture-repo.js';

const REPOSITORY = 'https://github.com/example/version-root.git';
const a: ReleasePackageInfo = { name: '@scope/a', projectName: 'a', path: 'packages/a', version: '1.0.0' };

/**
 * The CLI as the publish workflow runs it: `bin/smoo` over the built `dist`, in
 * its own process. The dry run asks Nx in-process, and Nx fixes its workspace
 * data and daemon record locations once, when its modules load. A test process
 * has loaded them for the workspace running the tests, so an in-process preview
 * here would poll that workspace's daemon record for the fixture's daemon.
 */
const SMOO = join(import.meta.dir, '..', '..', '..', 'bin', 'smoo');

describe('release version against a workspace root named through another path', () => {
  it('creates the versions the dry-run planned when the inherited Nx root names the tree by an alias', async () => {
    await withFixtureRepo(async (root) => {
      await writeReleaseWorkspace(root);
      await git(root, ['add', '-A']);
      await git(root, ['commit', '-m', 'chore: fixture workspace']);
      await tag(root, 'a@1.0.0', '2025-01-01T00:00:00Z');
      await writeFile(join(root, a.path, 'README.md'), '# a\n');
      await git(root, ['add', '-A']);
      await git(root, ['commit', '-m', 'feat(a): document the package']);

      // CI's devenv exports NX_WORKSPACE_ROOT_PATH as a stable bind mount of the
      // checkout while every step runs in the checkout: one tree, two spellings.
      // The alias stands in for that mount; smoo resolves `root` from git.
      const scratch = await realpath(await mkdtemp(join(tmpdir(), 'smoo-release-alias-')));
      const alias = join(scratch, 'workspace');
      await symlink(root, alias, 'dir');
      const summary = join(scratch, 'summary.md');
      const output = join(scratch, 'output.txt');
      // The fixture owns its Nx state exactly as `runFixtureNx` gives it: no
      // outer socket, cache and workspace data under the fixture. Only the root
      // keeps the alias spelling -- the planted defect. The daemon is on
      // whatever CI says: daemon clients disagreeing on which workspace owns it
      // is half of that defect, and the fixture stops it when it retires.
      const env = mergeEnv(
        {
          NX_WORKSPACE_ROOT_PATH: alias,
          NX_CACHE_DIRECTORY: join(root, '.nx', 'cache'),
          NX_WORKSPACE_DATA_DIRECTORY: join(root, '.nx', 'workspace-data'),
          NX_DAEMON: 'true',
          GITHUB_STEP_SUMMARY: summary,
        },
        ['NX_SOCKET_DIR', 'NX_DAEMON_SOCKET_DIR', 'GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE'],
      );
      try {
        await smoo(root, env, ['release', 'version', '--bump', 'auto', '--projects', '', '--dry-run', 'true']);
        expect(await readFile(summary, 'utf8')).toContain('- `@scope/a`: `1.0.0` -> `1.1.0`');

        await smoo(root, env, [
          'release',
          'version',
          '--bump',
          'auto',
          '--projects',
          '',
          '--dry-run',
          'false',
          '--github-output',
          output,
        ]);
        expect(await readFile(output, 'utf8')).toBe('mode=new\nprojects=a\n');
        await expect(releasePackagesAtRef(root, [a], 'HEAD')).resolves.toEqual([{ ...a, version: '1.1.0' }]);
      } finally {
        await rm(scratch, { recursive: true, force: true });
      }
    });
  });
});

async function smoo(cwd: string, env: Record<string, string>, args: string[]): Promise<void> {
  const child = Bun.spawn(['bun', SMOO, ...args], { cwd, env, stdout: 'pipe', stderr: 'pipe' });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  if (exitCode !== 0) {
    throw new Error(`smoo ${args.join(' ')} exited with code ${exitCode}\n${stdout}\n${stderr}`);
  }
}

async function writeReleaseWorkspace(root: string): Promise<void> {
  await writeFile(join(root, '.gitignore'), 'node_modules\n.nx\n');
  await writeJson(join(root, 'package.json'), {
    name: '@scope/source',
    private: true,
    version: '0.0.0',
    workspaces: ['packages/*'],
    repository: { type: 'git', url: REPOSITORY },
  });
  await writeJson(join(root, 'nx.json'), {
    release: {
      projects: ['tag:npm:public'],
      projectsRelationship: 'independent',
      version: {
        specifierSource: 'conventional-commits',
        currentVersionResolver: 'git-tag',
        fallbackCurrentVersionResolver: 'disk',
        versionActionsOptions: { skipLockFileUpdate: true },
      },
      releaseTag: { pattern: '{projectName}@{version}' },
      changelog: { workspaceChangelog: false, projectChangelogs: { createRelease: false, file: false } },
    },
  });
  await mkdir(join(root, a.path), { recursive: true });
  await writeJson(join(root, a.path, 'package.json'), {
    name: a.name,
    version: a.version,
    repository: { type: 'git', url: REPOSITORY, directory: a.path },
    nx: { name: a.projectName, tags: ['npm:public'] },
  });
  // Nx resolves its release version actions from the workspace; reuse this one's.
  await symlink(join(import.meta.dir, '../../../../../node_modules'), join(root, 'node_modules'), 'dir');
}

async function writeJson(path: string, value: object): Promise<void> {
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}
