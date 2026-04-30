import { describe, expect, it } from 'bun:test';
import {
  collectOwnedReleaseTagRecords,
  groupReleaseTargets,
  legacyReleaseTag,
  npmDistTagForVersion,
  pendingReleaseTargets,
  type ReleasePackageInfo,
  type ReleaseTagRecord,
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
});
