import { describe, expect, it } from 'bun:test';
import type { ReleasePackageInfo } from '../core.js';
import { releasePackagesAtRef } from '../index.js';
import { git, tag, withFixtureRepo, writePackage } from './helpers/fixture-repo.js';

const a: ReleasePackageInfo = { name: '@scope/a', projectName: 'a', path: 'packages/a', version: '1.0.0' };
const b: ReleasePackageInfo = { name: '@scope/b', projectName: 'b', path: 'packages/b', version: '1.0.0' };

/**
 * `smoo release version` creates the release commit and `smoo release tag`
 * creates the tags, in a later job. Everything between those two points — the
 * candidate's own validation, the artifact bundle it hands over, and the tag
 * step itself — has to recognise a release commit that carries no tags at all.
 */
describe('release packages at a ref', () => {
  it('recognises an untagged release commit from its subject and bumped versions', async () => {
    await withFixtureRepo(async (root) => {
      await writePackage(root, a.name, a.path, '1.0.0-next.0');
      await writePackage(root, b.name, b.path, '1.0.0-next.0');
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'feat: initial packages']);

      // Only `a` is versioned, exactly as Nx does for an independent release.
      await writePackage(root, a.name, a.path, '1.0.0');
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'chore(release): publish']);

      expect(await gitTagsAt(root)).toEqual([]);
      await expect(releasePackagesAtRef(root, [a, b], 'HEAD')).resolves.toEqual([{ ...a, version: '1.0.0' }]);
    });
  });

  it('prefers the tags once the publishing job has created them', async () => {
    await withFixtureRepo(async (root) => {
      await writePackage(root, a.name, a.path, '1.0.0-next.0');
      await writePackage(root, b.name, b.path, '1.0.0-next.0');
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'feat: initial packages']);

      await writePackage(root, a.name, a.path, '1.0.0');
      await writePackage(root, b.name, b.path, '2.0.0');
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'chore(release): publish']);
      await tag(root, 'a@1.0.0', '2025-01-01T00:00:00Z');

      // b@2.0.0 is untagged, so the tag evidence deliberately narrows the set.
      await expect(releasePackagesAtRef(root, [a, b], 'HEAD')).resolves.toEqual([{ ...a, version: '1.0.0' }]);
    });
  });

  it('reports no release for an ordinary commit, tagged or not', async () => {
    await withFixtureRepo(async (root) => {
      await writePackage(root, a.name, a.path, '1.0.0');
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'feat: initial packages']);

      await writePackage(root, a.name, a.path, '1.1.0');
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'feat(a): a change that bumps nothing durably']);

      await expect(releasePackagesAtRef(root, [a], 'HEAD')).resolves.toEqual([]);
    });
  });

  it('treats a root release commit with no parent as releasing every present package', async () => {
    await withFixtureRepo(async (root) => {
      await writePackage(root, a.name, a.path, '1.0.0');
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'chore(release): publish']);

      await expect(releasePackagesAtRef(root, [a, b], 'HEAD')).resolves.toEqual([{ ...a, version: '1.0.0' }]);
    });
  });
});

async function gitTagsAt(root: string): Promise<string[]> {
  const result = await Bun.$`git tag --points-at HEAD`.cwd(root).quiet().nothrow();
  return result.stdout
    .toString()
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);
}
