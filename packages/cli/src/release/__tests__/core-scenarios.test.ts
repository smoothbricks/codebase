import { describe, expect, it } from 'bun:test';
import {
  collectOwnedReleaseTagRecords,
  type GitReleaseTagInfo,
  groupReleaseTargets,
  npmDistTagForVersion,
  pendingReleaseTargets,
  type ReleasePackageInfo,
  type ReleasePlanningShell,
  type ReleaseTagRecord,
} from '../core.js';

const stablePackage: ReleasePackageInfo = {
  name: '@scope/stable',
  projectName: 'stable',
  path: 'packages/stable',
  version: '1.0.0',
};
const prereleasePackage: ReleasePackageInfo = {
  name: '@scope/prerelease',
  projectName: 'prerelease',
  path: 'packages/prerelease',
  version: '2.0.0-beta.1',
};

function record(
  pkg: ReleasePackageInfo,
  sha: string,
  timestamp: number,
  needs: { npm?: boolean; github?: boolean },
): ReleaseTagRecord {
  return {
    tag: `${pkg.projectName}@${pkg.version}`,
    sha,
    timestamp,
    pkg,
    needsNpmPublish: needs.npm === true,
    needsGithubRelease: needs.github === true,
  };
}

describe('release core scenario coverage', () => {
  it('fails loudly for duplicate release tags for the same package on one commit', () => {
    expect(() =>
      groupReleaseTargets([
        record(stablePackage, 'same-commit', 10, { npm: true }),
        record({ ...stablePackage, version: '1.0.1' }, 'same-commit', 11, { github: true }),
      ]),
    ).toThrow('Release target same-commit has more than one release tag for @scope/stable.');
  });

  it('groups mixed stable and prerelease npm packages while dist-tags remain package-version specific', () => {
    const targets = groupReleaseTargets([
      record(stablePackage, 'mixed', 10, { npm: true }),
      record(prereleasePackage, 'mixed', 11, { npm: true }),
    ]);

    expect(targets).toHaveLength(1);
    expect(targets[0]?.npmPackages.map((pkg) => pkg.name)).toEqual(['@scope/stable', '@scope/prerelease']);
    expect(targets[0]?.npmPackages.map((pkg) => [pkg.name, npmDistTagForVersion(pkg.version)])).toEqual([
      ['@scope/stable', 'latest'],
      ['@scope/prerelease', 'next'],
    ]);
  });

  it('skips a complete HEAD and stops at the next older complete target so older gaps are ignored', () => {
    const pending = pendingReleaseTargets(
      [
        record(stablePackage, 'head', 30, {}),
        record(stablePackage, 'complete', 20, {}),
        record(stablePackage, 'ignored-gap', 10, { npm: true, github: true }),
      ],
      'head',
    );

    expect(pending).toEqual([]);
  });

  it('sorts targets by newest tag timestamp before pending reverses them oldest-first', () => {
    const records = [
      record(stablePackage, 'older-target', 10, { npm: true }),
      record(prereleasePackage, 'newer-target', 30, { github: true }),
      record({ ...stablePackage, version: '1.0.1' }, 'newer-target', 40, { npm: true }),
    ];

    expect(groupReleaseTargets(records).map((target) => target.sha)).toEqual(['newer-target', 'older-target']);
    expect(pendingReleaseTargets(records, 'head').map((target) => target.sha)).toEqual([
      'older-target',
      'newer-target',
    ]);
  });

  it('ignores unrelated tags and tags not reachable from the selected ref through shell callbacks', async () => {
    const packageVersionRefs: string[] = [];
    const durableTags: string[] = [];
    const versions = new Map([
      ['packages/stable:reachable', '1.0.0'],
      ['packages/stable:unreachable', '1.0.1'],
    ]);
    const shell: ReleasePlanningShell = {
      listReleaseTagsByCreatorDate: async () => [
        tag('stable@1.0.0', 'reachable', 30),
        tag('@other/pkg@9.9.9', 'reachable', 20),
        tag('stable@1.0.1', 'unreachable', 10),
      ],
      isAncestor: async (ancestor) => ancestor === 'reachable',
      packageVersionAtRef: async (packagePath, ref) => {
        packageVersionRefs.push(`${packagePath}:${ref}`);
        return versions.get(`${packagePath}:${ref}`) ?? null;
      },
      durableTagState: async (_pkg, tagName) => {
        durableTags.push(tagName);
        return { npmPublished: false, githubReleaseExists: true };
      },
    };

    const records = await collectOwnedReleaseTagRecords(
      [{ name: stablePackage.name, projectName: stablePackage.projectName, path: stablePackage.path }],
      'selected-ref',
      shell,
    );

    expect(records.map((releaseRecord) => releaseRecord.tag)).toEqual(['stable@1.0.0']);
    expect(packageVersionRefs).toEqual(['packages/stable:reachable']);
    expect(durableTags).toEqual(['stable@1.0.0']);
  });

  it('stops querying older package tags after the newest tag for that package is fully durable', async () => {
    const durableTags: string[] = [];
    const versions = new Map([
      ['packages/stable:newer-stable', '1.1.0'],
      ['packages/stable:older-stable', '1.0.0'],
      ['packages/prerelease:older-prerelease', '2.0.0-beta.1'],
    ]);
    const shell: ReleasePlanningShell = {
      listReleaseTagsByCreatorDate: async () => [
        tag('stable@1.1.0', 'newer-stable', 30),
        tag('prerelease@2.0.0-beta.1', 'older-prerelease', 20),
        tag('stable@1.0.0', 'older-stable', 10),
      ],
      isAncestor: async () => true,
      packageVersionAtRef: async (packagePath, ref) => versions.get(`${packagePath}:${ref}`) ?? null,
      durableTagState: async (_pkg, tagName) => {
        durableTags.push(tagName);
        return { npmPublished: true, githubReleaseExists: true };
      },
    };

    const records = await collectOwnedReleaseTagRecords(
      [
        { name: stablePackage.name, projectName: stablePackage.projectName, path: stablePackage.path },
        { name: prereleasePackage.name, projectName: prereleasePackage.projectName, path: prereleasePackage.path },
      ],
      'selected-ref',
      shell,
    );

    expect(records.map((releaseRecord) => releaseRecord.tag)).toEqual(['stable@1.1.0', 'prerelease@2.0.0-beta.1']);
    expect(durableTags).toEqual(['stable@1.1.0', 'prerelease@2.0.0-beta.1']);
  });

  it('throws when a release tag version does not match package.json at the peeled commit', async () => {
    const shell: ReleasePlanningShell = {
      listReleaseTagsByCreatorDate: async () => [tag('stable@1.0.0', 'mismatch', 10)],
      isAncestor: async () => true,
      packageVersionAtRef: async () => '1.0.1',
      durableTagState: async () => ({ npmPublished: true, githubReleaseExists: true }),
    };

    await expect(
      collectOwnedReleaseTagRecords(
        [{ name: stablePackage.name, projectName: stablePackage.projectName, path: stablePackage.path }],
        'selected-ref',
        shell,
      ),
    ).rejects.toThrow(
      'Release tag stable@1.0.0 points at mismatch, but packages/stable/package.json has version 1.0.1.',
    );
  });
});

function tag(name: string, sha: string, timestamp: number): GitReleaseTagInfo {
  return { name, sha, timestamp };
}
