import { describe, expect, it } from 'bun:test';
import {
  classifyReleaseBranchPush,
  collectOwnedReleaseTagRecords,
  firstReleaseVersion,
  groupReleaseTargets,
  legacyReleaseTag,
  npmDistTagForVersion,
  pendingReleaseTargets,
  type ReleasePackageInfo,
  type ReleaseTagRecord,
  releaseCommitParagraphs,
  releaseCommitProjects,
  releasedByCommit,
  releasePackageForTag,
  releaseTag,
  releaseTagAliases,
} from '../core.js';

const a: ReleasePackageInfo = { name: '@scope/a', projectName: 'a', path: 'packages/a', version: '1.0.0' };
const b: ReleasePackageInfo = { name: '@scope/b', projectName: 'b', path: 'packages/b', version: '2.0.0-beta.1' };

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

describe('release core planning', () => {
  it('matches owned release tags without semantic version parsing', () => {
    expect(releasePackageForTag([a], 'a@1.2.3')?.version).toBe('1.2.3');
    expect(releasePackageForTag([a], '@scope/a@1.2.3')).toBeNull();
    expect(releasePackageForTag([a], 'c@1.2.3')).toBeNull();
  });

  it('groups only repair-needed tags by commit', () => {
    const targets = groupReleaseTargets([record(a, 'aaa', 10, {}), record(b, 'aaa', 11, { npm: true })]);

    expect(targets).toHaveLength(1);
    expect(targets[0]?.packages.map((pkg) => pkg.name)).toEqual(['@scope/b']);
    expect(targets[0]?.npmPackages.map((pkg) => pkg.name)).toEqual(['@scope/b']);
    expect(targets[0]?.githubPackages).toEqual([]);
    expect(targets[0]?.timestamp).toBe(11);
  });

  it('repairs the newest incomplete suffix oldest-to-newest and stops at the first complete target', () => {
    const pending = pendingReleaseTargets(
      [
        record(a, 'newest', 30, { github: true }),
        record(a, 'middle', 20, { npm: true }),
        record(a, 'complete', 10, {}),
        record(a, 'ignored-gap', 1, { npm: true, github: true }),
      ],
      'head',
    );

    expect(pending.map((target) => target.sha)).toEqual(['middle', 'newest']);
  });

  it('excludes HEAD from older pending repairs', () => {
    const pending = pendingReleaseTargets(
      [record(a, 'head', 30, { npm: true }), record(a, 'older', 20, { github: true })],
      'head',
    );

    expect(pending.map((target) => target.sha)).toEqual(['older']);
  });

  it('uses package-version dist-tags instead of commit-level repair tags', () => {
    expect(npmDistTagForVersion('1.0.0')).toBe('latest');
    expect(npmDistTagForVersion('1.0.0-beta.1')).toBe('next');
  });

  it('keeps project-name release tags canonical while recognizing legacy package-name aliases', () => {
    const scoped = { name: '@scope/a', projectName: 'a', version: '1.0.0' };

    expect(releaseTag(scoped)).toBe('a@1.0.0');
    expect(legacyReleaseTag(scoped)).toBe('@scope/a@1.0.0');
    expect(releaseTagAliases(scoped)).toEqual(['a@1.0.0', '@scope/a@1.0.0']);
  });

  it('checks durable release state concurrently while preserving tag-order records', async () => {
    const durableCalls: string[] = [];
    let active = 0;
    let maxActive = 0;
    let unblockDurableCalls: (() => void) | undefined;
    const durableCallsBlocked = new Promise<void>((resolve) => {
      unblockDurableCalls = resolve;
    });

    const records = await collectOwnedReleaseTagRecords([a, b], 'head', {
      listReleaseTagsByCreatorDate: async () => [
        { name: 'a@1.0.0', sha: 'newer', timestamp: 2 },
        { name: 'b@2.0.0-beta.1', sha: 'older', timestamp: 1 },
      ],
      isAncestor: async () => true,
      packageVersionAtRef: async (packagePath) => (packagePath === a.path ? a.version : b.version),
      durableTagState: async (_pkg, tag) => {
        durableCalls.push(tag);
        active += 1;
        maxActive = Math.max(maxActive, active);
        if (durableCalls.length === 2) {
          unblockDurableCalls?.();
        }
        await durableCallsBlocked;
        active -= 1;
        return { npmPublished: false, githubReleaseExists: false };
      },
    });

    expect(durableCalls).toEqual(['a@1.0.0', 'b@2.0.0-beta.1']);
    expect(maxActive).toBeGreaterThan(1);
    expect(records.map((releaseRecord) => releaseRecord.tag)).toEqual(['a@1.0.0', 'b@2.0.0-beta.1']);
  });

  it('classifies every release branch position against the remote', () => {
    // A brand new branch has nothing to compare against.
    expect(
      classifyReleaseBranchPush({ remoteExists: false, headIsAncestorOfRemote: false, remoteIsAncestorOfHead: false }),
    ).toBe('push');
    // Equal refs answer true to both ancestor checks; nothing is left to push.
    expect(
      classifyReleaseBranchPush({ remoteExists: true, headIsAncestorOfRemote: true, remoteIsAncestorOfHead: true }),
    ).toBe('skip');
    // The repair path: the release commit already landed and the branch moved past it.
    expect(
      classifyReleaseBranchPush({ remoteExists: true, headIsAncestorOfRemote: true, remoteIsAncestorOfHead: false }),
    ).toBe('skip');
    // The normal release: a fast-forward from the remote tip.
    expect(
      classifyReleaseBranchPush({ remoteExists: true, headIsAncestorOfRemote: false, remoteIsAncestorOfHead: true }),
    ).toBe('push');
    // A concurrent push: neither ref reaches the other.
    expect(
      classifyReleaseBranchPush({ remoteExists: true, headIsAncestorOfRemote: false, remoteIsAncestorOfHead: false }),
    ).toBe('diverged');
  });
});

describe('the version a first release publishes', () => {
  it.each([
    ['a declared stable version stands', '2.3.0', '2.3.0'],
    ['a version of one digit per part stands', '1.0.0', '1.0.0'],
    ['a development prerelease stands for the version it precedes', '0.4.2-next.0', '0.4.2'],
    ['so does a named prerelease', '1.0.0-beta.1', '1.0.0'],
    ['build metadata is no part of a release', '1.2.3+build.5', '1.2.3'],
    ['0.0.0 names no version to release', '0.0.0', '0.0.1'],
    ['nor does a prerelease of it', '0.0.0-next.0', '0.0.1'],
    ['the first patch stands for itself while in development', '0.0.1-next.0', '0.0.1'],
    ['a declared version that is no semver names none', 'latest', '0.0.1'],
    ['nor does a version missing a part', '1.2', '0.0.1'],
    ['nor does one with a leading zero', '01.2.3', '0.0.1'],
    ['nor does an empty one', '', '0.0.1'],
  ])('%s: %s releases %s', (_case, declared, released) => {
    expect(firstReleaseVersion(declared)).toBe(released);
  });
});

describe('the message of a release commit', () => {
  it('lists the projects Nx lists in the commit it writes for an independent release', () => {
    const nxMessage = [
      'chore(release): publish',
      '',
      '- project: nx-plugin 0.6.1',
      '',
      '- project: cowshed 0.4.1',
      '',
      '- project: cli 0.12.23',
      '',
    ].join('\n');

    expect([...releaseCommitProjects(nxMessage)]).toEqual([
      ['nx-plugin', '0.6.1'],
      ['cowshed', '0.4.1'],
      ['cli', '0.12.23'],
    ]);
  });

  it('is written as the paragraphs git joins into that same message', () => {
    const paragraphs = releaseCommitParagraphs([a, { ...b, version: '2.1.0' }]);

    expect(paragraphs).toEqual(['chore(release): publish', '- project: a 1.0.0', '- project: b 2.1.0']);
    expect([...releaseCommitProjects(paragraphs.join('\n\n'))]).toEqual([
      ['a', '1.0.0'],
      ['b', '2.1.0'],
    ]);
  });

  it('lists nothing for a message that only looks like a list', () => {
    const message = ['feat: add a project', '', 'project: a 1.0.0', '- projects: a 1.0.0', '- project: a', ''].join(
      '\n',
    );

    expect(releaseCommitProjects(message).size).toBe(0);
  });
});

describe('what a release commit releases', () => {
  const listed = new Map([['a', '1.0.0']]);

  it.each([
    ['a version the commit changes', '0.9.0', new Map<string, string>(), true],
    ['a package the commit adds', null, new Map<string, string>(), true],
    ['a version the commit leaves alone and does not list', '1.0.0', new Map<string, string>(), false],
    ['a version the commit leaves alone and lists, a first release at its declared version', '1.0.0', listed, true],
    ['a version the commit leaves alone and lists at another version', '1.0.0', new Map([['a', '0.9.0']]), false],
  ])('%s', (_case, versionBefore, commitList, released) => {
    expect(releasedByCommit(a, versionBefore, commitList)).toBe(released);
  });
});
