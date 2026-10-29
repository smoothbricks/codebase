import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { mergeEnv } from '../lib/run.js';
import { newWorkspacePackageProjects } from './new-workspace-package.js';

function git(root: string, ...args: string[]): void {
  const child = Bun.spawnSync(['git', '-c', 'user.name=t', '-c', 'user.email=t@example.com', ...args], {
    cwd: root,
    env: mergeEnv(undefined, ['GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE', 'GIT_COMMON_DIR']),
  });
  expect(child.exitCode, `git ${args.join(' ')}: ${child.stderr.toString()}`).toBe(0);
}

async function manifest(root: string, dir: string, json: object): Promise<void> {
  await mkdir(join(root, dir), { recursive: true });
  await writeFile(join(root, dir, 'package.json'), `${JSON.stringify(json)}\n`);
}

async function withRepo(run: (root: string) => Promise<void>): Promise<void> {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'smoo-new-package-')));
  try {
    git(root, 'init', '--quiet');
    await writeFile(join(root, 'package.json'), '{ "name": "@acme/root", "workspaces": ["packages/*", "tools/*"] }\n');
    await run(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

describe('newWorkspacePackageProjects', () => {
  it('names the Nx project of each package the index adds directly under a workspace glob', async () => {
    await withRepo(async (root) => {
      await manifest(root, 'packages/web', { name: '@acme/web', version: '0.0.0' });
      await manifest(root, 'tools/gen', { name: '@acme/gen', version: '0.0.0', nx: { name: 'gen' } });
      await manifest(root, 'packages/web/fixtures/inner', { name: 'inner' });
      await manifest(root, 'docs', { name: 'docs' });
      git(root, 'add', '-A');
      expect((await newWorkspacePackageProjects(root)).sort()).toEqual(['@acme/web', 'gen']);
    });
  });

  it('names nothing when the commit only changes a package that already exists', async () => {
    await withRepo(async (root) => {
      await manifest(root, 'packages/web', { name: '@acme/web', version: '0.0.0' });
      git(root, 'add', '-A');
      git(root, 'commit', '--quiet', '-m', 'add web');
      await manifest(root, 'packages/web', { name: '@acme/web', version: '0.0.1' });
      git(root, 'add', '-A');
      expect(await newWorkspacePackageProjects(root)).toEqual([]);
    });
  });

  it('refuses a new workspace manifest that names no package', async () => {
    await withRepo(async (root) => {
      await manifest(root, 'packages/anon', { version: '0.0.0' });
      git(root, 'add', '-A');
      await expect(newWorkspacePackageProjects(root)).rejects.toThrow(
        'packages/anon/package.json is staged as a new workspace package but names no package',
      );
    });
  });
});
