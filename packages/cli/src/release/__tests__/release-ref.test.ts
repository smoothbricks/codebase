import { describe, expect, it } from 'bun:test';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { listReleasePackages } from '../../lib/workspace.js';
import type { ReleasePackageInfo } from '../core.js';
import { commitReleaseWithFirstReleases, manifestWithVersion } from '../first-release.js';
import { releasePackagesAtRef } from '../index.js';
import { git, gitOutput, tag, withFixtureRepo, writePackage } from './helpers/fixture-repo.js';

const a: ReleasePackageInfo = { name: '@scope/a', projectName: 'a', path: 'packages/a', version: '1.0.0' };
const b: ReleasePackageInfo = { name: '@scope/b', projectName: 'b', path: 'packages/b', version: '1.0.0' };
const c: ReleasePackageInfo = { name: '@scope/c', projectName: 'c', path: 'packages/c', version: '1.0.0' };

/**
 * `smoo release version` creates the release commit and `smoo release tag`
 * creates the tags, in a later job. Everything between those two points — the
 * candidate's own validation, the artifact bundle it hands over, and the tag
 * step itself — has to recognise a release commit that carries no tags at all.
 */
describe('release packages at a ref', () => {
  it('recognises an untagged release commit from its subject and bumped versions', async () => {
    await withFixtureRepo(async (root) => {
      // HEAD is unborn: Git runs and refuses the read, rather than Bun refusing a missing cwd.
      await expect(gitTagsAt(root)).rejects.toThrow();
      await writePackage(root, a.name, a.path, '1.0.0-next.0');
      await writePackage(root, b.name, b.path, '1.0.0-next.0');
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'feat: initial packages']);

      // Only `a` is versioned, exactly as Nx does for an independent release.
      await writePackage(root, a.name, a.path, '1.0.0');
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'chore(release): publish']);

      expect(await gitTagsAt(root)).toEqual([]);
      await expect(gitTagsAt(join(root, 'missing-repository'))).rejects.toThrow();
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

  it('recognises a first release at the version its manifest declares from the message that lists it', async () => {
    await withFixtureRepo(async (root) => {
      for (const pkg of [a, b, c]) {
        await writePackage(root, pkg.name, pkg.path, pkg.version);
      }
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'feat: initial packages']);

      // Nx versions `a`. `b` is released at the version it already declares, so the commit changes none of its files and
      // only the message records it. `c` is in neither.
      await writePackage(root, a.name, a.path, '1.1.0');
      await git(root, ['add', '.']);
      await git(root, [
        'commit',
        '-m',
        'chore(release): publish',
        '-m',
        '- project: a 1.1.0',
        '-m',
        '- project: b 1.0.0',
      ]);

      await expect(releasePackagesAtRef(root, [a, b, c], 'HEAD')).resolves.toEqual([{ ...a, version: '1.1.0' }, b]);
    });
  });

  it('does not take a project the message lists at another version than the manifest has for a release', async () => {
    await withFixtureRepo(async (root) => {
      await writePackage(root, b.name, b.path, '1.0.0');
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'feat: initial packages']);

      await writePackage(root, a.name, a.path, '1.1.0');
      await git(root, ['add', '.']);
      await git(root, [
        'commit',
        '-m',
        'chore(release): publish',
        '-m',
        '- project: a 1.1.0',
        '-m',
        '- project: b 0.9.0',
      ]);

      await expect(releasePackagesAtRef(root, [a, b], 'HEAD')).resolves.toEqual([{ ...a, version: '1.1.0' }]);
    });
  });
});

const REPOSITORY = 'https://github.com/example/first-release.git';

/**
 * The commit smoo writes when a release has first releases in it: Nx staged what it versioned, smoo writes the rest, and
 * one commit holds both. Real git, a real workspace and the real lockfile sync: that is what the release commit is.
 */
describe('the release commit of a release with first releases', () => {
  it('writes each first release beside what Nx staged, undoes what Nx did to one, and commits once', async () => {
    await withFixtureRepo(async (root) => {
      await writeFirstReleaseWorkspace(root, [
        { ...a, version: '1.0.0' }, // tagged: Nx derives its bump
        { ...b, version: '0.0.0' }, // declares no version to release: 0.0.1
        { ...c, version: '2.3.0' }, // declares the version it releases
      ]);
      await git(root, ['add', '-A']);
      await git(root, ['commit', '-m', 'chore: fixture workspace']);
      const bBefore = await readFile(join(root, b.path, 'package.json'), 'utf8');

      // What `nx release version --stage-changes` leaves: `a` at its derived version, and `c`, which depends on `a`,
      // versioned as a dependent from the version on disk -- not the version it releases.
      await setManifestVersion(root, a, '1.0.0', '1.1.0');
      await setManifestVersion(root, c, '2.3.0', '2.3.1');
      await writeBunLock(root, [
        { ...a, version: '1.1.0' },
        { ...b, version: '0.0.0' },
        { ...c, version: '2.3.1' },
      ]);
      await git(root, ['add', '-A']);

      const firstReleases = listReleasePackages(root)
        .filter((pkg) => pkg.projectName !== 'a')
        .map((pkg) => ({ ...pkg, version: pkg.projectName === 'b' ? '0.0.1' : '2.3.0' }));
      await commitReleaseWithFirstReleases(root, firstReleases);

      expect(await gitOutput(root, ['rev-list', '--count', 'HEAD'])).toBe('2');
      expect(await gitOutput(root, ['show', '-s', '--format=%B', 'HEAD'])).toBe(
        ['chore(release): publish', '', '- project: a 1.1.0', '', '- project: b 0.0.1', '', '- project: c 2.3.0'].join(
          '\n',
        ),
      );
      // `c` released at the version it declares is no change of its manifest, so the commit does not touch it.
      expect((await gitOutput(root, ['diff', '--name-only', 'HEAD~1', 'HEAD'])).split('\n')).toEqual([
        'bun.lock',
        'packages/a/package.json',
        'packages/b/package.json',
      ]);
      expect(await readFile(join(root, b.path, 'package.json'), 'utf8')).toBe(
        bBefore.replace('"version": "0.0.0"', '"version": "0.0.1"'),
      );
      const lock = await readFile(join(root, 'bun.lock'), 'utf8');
      expect(lock).toContain('"packages/a": { "name": "@scope/a", "version": "1.1.0" }');
      expect(lock).toContain('"packages/b": { "name": "@scope/b", "version": "0.0.1" }');
      expect(lock).toContain('"packages/c": { "name": "@scope/c", "version": "2.3.0" }');
      expect(await gitOutput(root, ['status', '--porcelain', '--untracked-files=no'])).toBe('');
      const released = await releasePackagesAtRef(root, listReleasePackages(root), 'HEAD');
      expect(released.map((pkg) => `${pkg.projectName}@${pkg.version}`)).toEqual(['a@1.1.0', 'b@0.0.1', 'c@2.3.0']);
    });
  });

  it('commits a first release at its declared version when nothing else changes', async () => {
    await withFixtureRepo(async (root) => {
      await writeFirstReleaseWorkspace(root, [
        { ...a, version: '1.0.0' },
        { ...c, version: '2.3.0' },
      ]);
      await git(root, ['add', '-A']);
      await git(root, ['commit', '-m', 'chore: fixture workspace']);

      await commitReleaseWithFirstReleases(
        root,
        listReleasePackages(root).filter((pkg) => pkg.projectName === 'c'),
      );

      expect(await gitOutput(root, ['rev-list', '--count', 'HEAD'])).toBe('2');
      expect(await gitOutput(root, ['diff', '--name-only', 'HEAD~1', 'HEAD'])).toBe('');
      const released = await releasePackagesAtRef(root, listReleasePackages(root), 'HEAD');
      expect(released.map((pkg) => `${pkg.projectName}@${pkg.version}`)).toEqual(['c@2.3.0']);
    });
  });
});

describe('replacing the version of a manifest', () => {
  it.each([
    [
      'a formatted manifest',
      '{\n  "name": "x",\n  "version": "0.0.0",\n  "files": ["dist"]\n}\n',
      '{\n  "name": "x",\n  "version": "0.0.1",\n  "files": ["dist"]\n}\n',
    ],
    ['a compact one', '{"name":"x","version":"0.0.0"}', '{"name":"x","version":"0.0.1"}'],
    [
      'a version that follows nested objects holding a version of their own',
      '{"nx":{"version":"9"},"dependencies":{"version":"^1.0.0"},"version":"0.0.0"}',
      '{"nx":{"version":"9"},"dependencies":{"version":"^1.0.0"},"version":"0.0.1"}',
    ],
    [
      'a string that spells the key out',
      '{"name":"version","description":"a \\"version\\": \\"0.0.0\\"","version":"0.0.0"}',
      '{"name":"version","description":"a \\"version\\": \\"0.0.0\\"","version":"0.0.1"}',
    ],
    [
      'tabs, CRLF and spaces around the colon',
      '{\r\n\t"version" : "0.0.0"\r\n}\r\n',
      '{\r\n\t"version" : "0.0.1"\r\n}\r\n',
    ],
  ])('keeps every other byte of %s', (_case, manifest, replaced) => {
    expect(manifestWithVersion(manifest, '0.0.1')).toBe(replaced);
  });

  it.each([
    ['no version', '{"name":"x"}'],
    ['a version that is no string', '{"version":0}'],
    ['a version only a nested object has', '{"nx":{"version":"1"}}'],
    ['a manifest that is no object', '["version", "0.0.0"]'],
  ])('finds nothing to replace in %s', (_case, manifest) => {
    expect(manifestWithVersion(manifest, '0.0.1')).toBeNull();
  });
});

async function gitTagsAt(root: string): Promise<string[]> {
  return (await gitOutput(root, ['tag', '--points-at', 'HEAD']))
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);
}

async function writeFirstReleaseWorkspace(root: string, packages: ReleasePackageInfo[]): Promise<void> {
  const rootManifest = {
    name: '@scope/source',
    private: true,
    version: '0.0.0',
    workspaces: ['packages/*'],
    repository: { type: 'git', url: REPOSITORY },
  };
  await writeFile(join(root, 'package.json'), `${JSON.stringify(rootManifest, null, 2)}\n`);
  for (const pkg of packages) {
    await mkdir(join(root, pkg.path), { recursive: true });
    const manifest = {
      name: pkg.name,
      version: pkg.version,
      repository: { type: 'git', url: REPOSITORY, directory: pkg.path },
      nx: { name: pkg.projectName, tags: ['npm:public'] },
      files: ['dist'],
    };
    await writeFile(join(root, pkg.path, 'package.json'), `${JSON.stringify(manifest, null, 2)}\n`);
  }
  await writeBunLock(root, packages);
}

async function writeBunLock(root: string, packages: ReleasePackageInfo[]): Promise<void> {
  const workspaces = packages.map(
    (pkg) => `    "${pkg.path}": { "name": "${pkg.name}", "version": "${pkg.version}" },`,
  );
  const lock = [
    '{',
    '  "lockfileVersion": 1,',
    '  "packages": {},',
    '  "workspaces": {',
    '    "": { "name": "@scope/source" },',
    ...workspaces,
  ];
  await writeFile(join(root, 'bun.lock'), `${[...lock, '  },', '}'].join('\n')}\n`);
}

async function setManifestVersion(root: string, pkg: ReleasePackageInfo, from: string, to: string): Promise<void> {
  const path = join(root, pkg.path, 'package.json');
  await writeFile(path, (await readFile(path, 'utf8')).replace(`"version": "${from}"`, `"version": "${to}"`));
}
