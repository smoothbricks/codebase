import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { publishFixtureSocketDirectory } from '@smoothbricks/nx-plugin/testing';
import { mergeEnv } from '../../lib/run.js';
import type { ReleasePackageInfo } from '../core.js';
import { releasePackagesAtRef } from '../index.js';
import { fixtureNxEnv, git, gitOutput, tag, withFixtureRepo } from './helpers/fixture-repo.js';

const REPOSITORY = 'https://github.com/example/version-root.git';
const a: ReleasePackageInfo = { name: '@scope/a', projectName: 'a', path: 'packages/a', version: '1.0.0' };

/** A fixture package: what Nx Release reads from its manifest. */
interface FixturePackage extends ReleasePackageInfo {
  dependencies?: Record<string, string>;
}
// A first release at the version it declares, which depends on `a`: Nx versions it as a dependent of `a` when `a` is bumped.
const b: FixturePackage = {
  name: '@scope/b',
  projectName: 'b',
  path: 'packages/b',
  version: '2.3.0',
  dependencies: { '@scope/a': 'workspace:*' },
};
// A first release whose manifest declares no version to release.
const c: FixturePackage = { name: '@scope/c', projectName: 'c', path: 'packages/c', version: '0.0.0' };
// A first release at the version it declares, with nothing else to release beside it.
const d: FixturePackage = { name: '@scope/d', projectName: 'd', path: 'packages/d', version: '4.5.6' };

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
      // The fixture owns its Nx state exactly as `runFixtureNx` gives it: its
      // own published socket directory, cache and workspace data under the
      // fixture, no outer socket. Only the root keeps the alias spelling -- the
      // planted defect. The daemon is on whatever CI says: daemon clients
      // disagreeing on which workspace owns it is half of that defect, and the
      // fixture stops it, through the same socket directory, when it retires.
      const env = mergeEnv(
        {
          NX_WORKSPACE_ROOT_PATH: alias,
          NX_SOCKET_DIR: await publishFixtureSocketDirectory(root),
          NX_CACHE_DIRECTORY: join(root, '.nx', 'cache'),
          NX_WORKSPACE_DATA_DIRECTORY: join(root, '.nx', 'workspace-data'),
          NX_DAEMON: 'true',
          GITHUB_STEP_SUMMARY: summary,
        },
        ['NX_DAEMON_SOCKET_DIR', 'GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE'],
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

describe('release version graph diagnostics', () => {
  it('prints the graph plugin cause when the real Nx release child fails', async () => {
    await withFixtureRepo(async (root) => {
      await writeReleaseWorkspace(root, ['./broken-graph.mjs']);
      const cause = 'SB_FORCED_GRAPH_FAILURE: broken graph fixture';
      await writeFile(
        join(root, 'broken-graph.mjs'),
        `export const createNodesV2 = ['**/package.json', async () => { throw new Error(${JSON.stringify(cause)}); }];\n`,
      );
      await git(root, ['add', '-A']);
      await git(root, ['commit', '-m', 'chore: fixture workspace']);
      const env = fixtureNxEnv(root, { daemon: 'off' }, await publishFixtureSocketDirectory(root));

      await expect(
        smoo(root, env, ['release', 'version', '--bump', 'patch', '--projects', 'all', '--dry-run', 'false']),
      ).rejects.toThrow(cause);
    });
  });
});

describe('release version of a first release beside a tagged package', () => {
  it('versions both in one release commit: the first releases at the versions they declare, the tagged package from its commits', async () => {
    await withFixtureRepo(async (root) => {
      await writeReleaseWorkspace(root, undefined, [a, b, c]);
      await git(root, ['add', '-A']);
      await git(root, ['commit', '-m', 'chore: fixture workspace']);
      await tag(root, 'a@1.0.0', '2025-01-01T00:00:00Z');
      await writeFile(join(root, a.path, 'README.md'), '# a\n');
      await git(root, ['add', '-A']);
      await git(root, ['commit', '-m', 'feat(a): document the package']);

      const scratch = await realpath(await mkdtemp(join(tmpdir(), 'smoo-release-first-')));
      const summary = join(scratch, 'summary.md');
      const output = join(scratch, 'output.txt');
      const env = {
        ...fixtureNxEnv(root, { daemon: 'off' }, await publishFixtureSocketDirectory(root)),
        GITHUB_STEP_SUMMARY: summary,
      };
      try {
        await smoo(root, env, ['release', 'version', '--bump', 'auto', '--projects', '', '--dry-run', 'true']);
        const preview = await readFile(summary, 'utf8');
        expect(preview).toContain('- `@scope/a`: `1.0.0` -> `1.1.0`');
        expect(preview).toContain('- `@scope/b`: `2.3.0` -> `2.3.0`');
        expect(preview).toContain('- `@scope/c`: `0.0.0` -> `0.0.1`');

        const before = await gitOutput(root, ['rev-parse', 'HEAD']);
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

        expect(await readFile(output, 'utf8')).toBe('mode=new\nprojects=a,b,c\n');
        expect(await gitOutput(root, ['rev-list', '--count', `${before}..HEAD`])).toBe('1');
        expect((await gitOutput(root, ['diff', '--name-only', before, 'HEAD'])).split('\n')).toEqual([
          'bun.lock',
          'packages/a/package.json',
          'packages/c/package.json',
        ]);
        const released = await releasePackagesAtRef(root, [a, b, c], 'HEAD');
        expect(released.map((pkg) => `${pkg.projectName}@${pkg.version}`)).toEqual(['a@1.1.0', 'b@2.3.0', 'c@0.0.1']);
      } finally {
        await rm(scratch, { recursive: true, force: true });
      }
    });
  });

  it('commits a lone first release at the version it declares, which changes no file', async () => {
    await withFixtureRepo(async (root) => {
      await writeReleaseWorkspace(root, undefined, [a, d]);
      await git(root, ['add', '-A']);
      await git(root, ['commit', '-m', 'chore: fixture workspace']);
      await tag(root, 'a@1.0.0', '2025-01-01T00:00:00Z');
      await writeFile(join(root, 'NOTES.md'), 'notes\n');
      await git(root, ['add', '-A']);
      await git(root, ['commit', '-m', 'docs: add notes']);

      const scratch = await realpath(await mkdtemp(join(tmpdir(), 'smoo-release-first-')));
      const output = join(scratch, 'output.txt');
      const env = fixtureNxEnv(root, { daemon: 'off' }, await publishFixtureSocketDirectory(root));
      try {
        const before = await gitOutput(root, ['rev-parse', 'HEAD']);
        await smoo(root, env, [
          'release',
          'version',
          '--bump',
          'auto',
          '--projects',
          'd',
          '--dry-run',
          'false',
          '--github-output',
          output,
        ]);

        expect(await readFile(output, 'utf8')).toBe('mode=new\nprojects=d\n');
        expect(await gitOutput(root, ['rev-list', '--count', `${before}..HEAD`])).toBe('1');
        expect(await gitOutput(root, ['diff', '--name-only', before, 'HEAD'])).toBe('');
        const released = await releasePackagesAtRef(root, [a, d], 'HEAD');
        expect(released.map((pkg) => `${pkg.projectName}@${pkg.version}`)).toEqual(['d@4.5.6']);
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

async function writeReleaseWorkspace(
  root: string,
  plugins?: string[],
  packages: FixturePackage[] = [a],
): Promise<void> {
  await writeFile(join(root, '.gitignore'), 'node_modules\n.nx\n');
  await writeJson(join(root, 'package.json'), {
    name: '@scope/source',
    private: true,
    version: '0.0.0',
    workspaces: ['packages/*'],
    repository: { type: 'git', url: REPOSITORY },
  });
  await writeJson(join(root, 'nx.json'), {
    plugins,
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
      changelog: {
        automaticFromRef: true,
        workspaceChangelog: false,
        projectChangelogs: { createRelease: false, file: false },
      },
    },
  });
  for (const pkg of packages) {
    await mkdir(join(root, pkg.path), { recursive: true });
    await writeJson(join(root, pkg.path, 'package.json'), {
      name: pkg.name,
      version: pkg.version,
      repository: { type: 'git', url: REPOSITORY, directory: pkg.path },
      nx: { name: pkg.projectName, tags: ['npm:public'] },
      dependencies: pkg.dependencies,
    });
  }
  // The first release commit syncs the lockfile's workspace versions to the manifests, as the release's version actions do.
  await writeFile(join(root, 'bun.lock'), bunLock(packages));
  // Nx resolves its release version actions from the workspace; reuse this one's.
  await symlink(join(import.meta.dir, '../../../../../node_modules'), join(root, 'node_modules'), 'dir');
}

async function writeJson(path: string, value: object): Promise<void> {
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}

function bunLock(packages: FixturePackage[]): string {
  const workspaces = packages.map(
    (pkg) => `    "${pkg.path}": { "name": "${pkg.name}", "version": "${pkg.version}" },`,
  );
  return [
    '{',
    '  "lockfileVersion": 1,',
    '  "workspaces": {',
    ...workspaces,
    '  },',
    '  "packages": {},',
    '}',
    '',
  ].join('\n');
}
