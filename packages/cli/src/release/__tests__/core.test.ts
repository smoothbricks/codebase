import { describe, expect, it } from 'bun:test';
import {
  groupReleaseTargets,
  npmDistTagForVersion,
  pendingReleaseTargets,
  type ReleasePackageInfo,
  type ReleaseTagRecord,
  releasePackageForTag,
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
});
