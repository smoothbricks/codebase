import { describe, expect, it } from 'bun:test';
import type { ReleasePackageInfo } from '../core.js';
import { planPublishActions } from '../publish-plan.js';

const stable: ReleasePackageInfo = { name: '@scope/stable', path: 'packages/stable', version: '1.0.0' };
const prerelease: ReleasePackageInfo = {
  name: '@scope/prerelease',
  path: 'packages/prerelease',
  version: '2.0.0-beta.1',
};

describe('publish plan', () => {
  it('returns empty actions when durable release state is complete', () => {
    expect(
      planPublishActions({
        releasePackages: [stable, prerelease],
        npmMissingPackages: [],
        githubMissingPackages: [],
      }),
    ).toEqual({ buildProjects: [], publishPackages: [], githubReleasePackages: [] });
  });

  it('builds and publishes exactly npm-missing packages', () => {
    const plan = planPublishActions({
      releasePackages: [stable, prerelease],
      npmMissingPackages: [stable],
      githubMissingPackages: [],
    });

    expect(plan.buildProjects).toEqual(['@scope/stable']);
    expect(plan.publishPackages).toEqual([{ pkg: stable, distTag: 'latest' }]);
    expect(plan.githubReleasePackages).toEqual([]);
  });

  it('does not build for GitHub-only repair actions', () => {
    const plan = planPublishActions({
      releasePackages: [stable, prerelease],
      npmMissingPackages: [],
      githubMissingPackages: [prerelease],
    });

    expect(plan.buildProjects).toEqual([]);
    expect(plan.publishPackages).toEqual([]);
    expect(plan.githubReleasePackages).toEqual([prerelease]);
  });

  it('uses latest for stable packages and next for prerelease packages', () => {
    const plan = planPublishActions({
      releasePackages: [stable, prerelease],
      npmMissingPackages: [stable, prerelease],
      githubMissingPackages: [],
    });

    expect(plan.buildProjects).toEqual(['@scope/stable', '@scope/prerelease']);
    expect(plan.publishPackages).toEqual([
      { pkg: stable, distTag: 'latest' },
      { pkg: prerelease, distTag: 'next' },
    ]);
    expect(plan.githubReleasePackages).toEqual([]);
  });
});
