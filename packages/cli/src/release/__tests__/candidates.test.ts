import { describe, expect, it } from 'bun:test';
import { writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { $ } from 'bun';
import { type AutoReleaseCandidateShell, autoReleaseCandidatePackages } from '../candidates.js';
import type { ReleasePackageInfo } from '../core.js';
import { git, tag, withFixtureRepo, writePackage } from './helpers/fixture-repo.js';

const a: ReleasePackageInfo = { name: '@scope/a', projectName: 'a', path: 'packages/a', version: '1.0.0' };
const b: ReleasePackageInfo = { name: '@scope/b', projectName: 'b', path: 'packages/b', version: '1.0.0' };
const c: ReleasePackageInfo = { name: '@scope/c', projectName: 'c', path: 'packages/c', version: '0.1.0' };
const d: ReleasePackageInfo = { name: '@scope/d', projectName: 'd', path: 'packages/d', version: '0.1.0' };

describe('auto release candidate filtering', () => {
  it('selects tagged packages only when their package path changed', async () => {
    await withFixtureRepo(async (root) => {
      await writePackage(root, a.name, a.path, a.version);
      await writePackage(root, b.name, b.path, b.version);
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'initial packages']);
      await tag(root, '@scope/a@1.0.0', '2025-01-01T00:00:00Z');
      await tag(root, '@scope/b@1.0.0', '2025-01-01T00:00:01Z');

      await writeFile(join(root, 'README.md'), 'root-only change\n');
      await git(root, ['add', 'README.md']);
      await git(root, ['commit', '-m', 'docs: root only']);

      await expect(autoReleaseCandidatePackages(gitCandidateShell(root), [a, b])).resolves.toEqual([]);

      await writeFile(join(root, a.path, 'index.ts'), 'export const changed = true;\n');
      await git(root, ['add', join(a.path, 'index.ts')]);
      await git(root, ['commit', '-m', 'feat(a): package local']);

      await expect(autoReleaseCandidatePackages(gitCandidateShell(root), [a, b])).resolves.toEqual([a]);
    });
  });

  it('includes untagged packages only when their package path has history', async () => {
    await withFixtureRepo(async (root) => {
      await writePackage(root, c.name, c.path, c.version);
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'feat(c): add package']);

      await expect(autoReleaseCandidatePackages(gitCandidateShell(root), [c, d])).resolves.toEqual([c]);
    });
  });
});

function gitCandidateShell(root: string): AutoReleaseCandidateShell {
  return {
    gitRefExists: async (ref) => {
      const result = await $`git rev-parse --verify ${ref}`.cwd(root).quiet().nothrow();
      return result.exitCode === 0;
    },
    packageChangedSince: async (ref, packagePath) => {
      const result = await $`git diff --quiet ${`${ref}..HEAD`} -- ${packagePath}`.cwd(root).quiet().nothrow();
      if (result.exitCode === 0) {
        return false;
      }
      if (result.exitCode === 1) {
        return true;
      }
      throw new Error(`Unable to inspect package changes under ${packagePath}.`);
    },
    packageHasHistory: async (packagePath) => {
      const result = await $`git log --format=%H -- ${packagePath}`.cwd(root).quiet().nothrow();
      if (result.exitCode !== 0) {
        throw new Error(`Unable to inspect package history under ${packagePath}.`);
      }
      return new TextDecoder().decode(result.stdout).trim().length > 0;
    },
  };
}
