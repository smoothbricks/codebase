import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { $ } from 'bun';
import { syncBunLockfileVersions } from '../lockfile.js';

describe('nx-version-actions Bun lockfile workaround', () => {
  it('repairs stale workspace versions that Bun still uses when packing workspace:* dependencies', async () => {
    await withBunWorkspace(async (root) => {
      await writePackage(root, 'a', {
        name: '@fixture/a',
        version: '1.0.0',
        dependencies: { '@fixture/b': 'workspace:*' },
      });
      await writePackage(root, 'b', { name: '@fixture/b', version: '1.0.0' });
      await $`bun install --lockfile-only`.cwd(root).quiet();

      await writePackage(root, 'b', { name: '@fixture/b', version: '1.0.1' });
      await $`bun install --lockfile-only`.cwd(root).quiet();

      const stalePackedVersion = await packedDependencyVersion(root, 1);
      if (stalePackedVersion !== '1.0.0') {
        throw new Error(
          'Hurrah! Bun no longer reproduces the stale workspace lockfile pack bug. ' +
            `Raw bun pm pack resolved @fixture/b to ${stalePackedVersion}; remove the nx-version-actions hook and update the release docs.`,
        );
      }

      expect(syncBunLockfileVersions(root, { log: false })).toBe(1);
      await expect(packedDependencyVersion(root, 2)).resolves.toBe('1.0.1');
    });
  });
});

async function withBunWorkspace(fn: (root: string) => Promise<void>): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-bun-lock-test-'));
  try {
    await writeFile(
      join(root, 'package.json'),
      `${JSON.stringify({ name: 'fixture-root', version: '0.0.0', private: true, packageManager: 'bun@1.3.13', workspaces: ['packages/*'] }, null, 2)}\n`,
    );
    await fn(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

async function writePackage(
  root: string,
  directory: string,
  manifest: { name: string; version: string; dependencies?: Record<string, string> },
): Promise<void> {
  const packageRoot = join(root, 'packages', directory);
  await mkdir(packageRoot, { recursive: true });
  await writeFile(join(packageRoot, 'index.js'), 'export {}\n');
  await writeFile(
    join(packageRoot, 'package.json'),
    `${JSON.stringify({ ...manifest, type: 'module', exports: './index.js', files: ['index.js'] }, null, 2)}\n`,
  );
}

async function packedDependencyVersion(root: string, index: number): Promise<string> {
  const tarball = join(root, `a-${index}.tgz`);
  const unpacked = join(root, `unpacked-${index}`);
  await mkdir(unpacked);
  await $`bun pm pack --filename ${tarball} --ignore-scripts --quiet`.cwd(join(root, 'packages/a')).quiet();
  await $`tar -xzf ${tarball} -C ${unpacked}`.quiet();
  const manifest = JSON.parse(await readFile(join(unpacked, 'package', 'package.json'), 'utf8'));
  const version = manifest.dependencies?.['@fixture/b'];
  if (typeof version !== 'string') {
    throw new Error('Packed @fixture/a manifest did not include @fixture/b dependency.');
  }
  return version;
}
