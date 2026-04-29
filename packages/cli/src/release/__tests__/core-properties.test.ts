import { describe, expect, it } from 'bun:test';
import fc from 'fast-check';
import { groupReleaseTargets, pendingReleaseTargets, type ReleasePackageInfo, type ReleaseTagRecord } from '../core.js';
import { planPublishActions } from '../publish-plan.js';

const packages: ReleasePackageInfo[] = [
  { name: '@scope/a', projectName: 'a', path: 'packages/a', version: '1.0.0' },
  { name: '@scope/b', projectName: 'b', path: 'packages/b', version: '2.0.0-beta.1' },
  { name: '@scope/c', projectName: 'c', path: 'packages/c', version: '3.0.0' },
];

describe('release core properties', () => {
  it('groups release tags by commit while preserving repair classifications', () => {
    fc.assert(
      fc.property(releaseRecords(), (records) => {
        const targets = groupReleaseTargets(records);

        for (const target of targets) {
          const recordsForTarget = records.filter((record) => record.sha === target.sha);
          const expectedTimestamp = Math.max(...recordsForTarget.map((record) => record.timestamp));
          const expectedPackages = recordsForTarget
            .filter((record) => record.needsNpmPublish || record.needsGithubRelease)
            .map((record) => record.pkg.name);
          const expectedNpmPackages = recordsForTarget
            .filter((record) => record.needsNpmPublish)
            .map((record) => record.pkg.name);
          const expectedGithubPackages = recordsForTarget
            .filter((record) => record.needsGithubRelease)
            .map((record) => record.pkg.name);

          expect(target.timestamp).toBe(expectedTimestamp);
          expect(target.packages.map((pkg) => pkg.name)).toEqual(expectedPackages);
          expect(target.npmPackages.map((pkg) => pkg.name)).toEqual(expectedNpmPackages);
          expect(target.githubPackages.map((pkg) => pkg.name)).toEqual(expectedGithubPackages);
        }

        expect(targets.map((target) => target.timestamp)).toEqual(
          [...targets].map((target) => target.timestamp).sort((left, right) => right - left),
        );
      }),
      { numRuns: 300 },
    );
  });

  it('repairs exactly the newest incomplete non-HEAD suffix in oldest-first order', () => {
    fc.assert(
      fc.property(
        releaseRecords(),
        fc.option(fc.constantFrom('commit-0', 'commit-1', 'commit-2', 'commit-3')),
        (records, headSha) => {
          const newestFirst = groupReleaseTargets(records);
          const expected: string[] = [];
          for (const target of newestFirst) {
            if (target.sha === headSha) {
              continue;
            }
            if (target.npmPackages.length === 0 && target.githubPackages.length === 0) {
              break;
            }
            expected.push(target.sha);
          }

          const pending = pendingReleaseTargets(records, headSha ?? 'not-head');
          expect(pending.map((target) => target.sha)).toEqual(expected.reverse());
          expect(pending.some((target) => target.sha === headSha)).toBe(false);
          for (const target of pending) {
            expect(target.npmPackages.length + target.githubPackages.length).toBeGreaterThan(0);
          }
        },
      ),
      { numRuns: 300 },
    );
  });

  it('builds exactly npm-missing release packages and never builds GitHub-only repairs', () => {
    fc.assert(
      fc.property(packageSubset(), packageSubset(), (npmMissingPackages, githubMissingPackages) => {
        const plan = planPublishActions({ releasePackages: packages, npmMissingPackages, githubMissingPackages });

        expect(plan.buildProjects).toEqual(npmMissingPackages.map((pkg) => pkg.projectName));
        expect(plan.publishPackages.map((action) => action.pkg.name)).toEqual(
          npmMissingPackages.map((pkg) => pkg.name),
        );
        expect(plan.githubReleasePackages.map((pkg) => pkg.name)).toEqual(githubMissingPackages.map((pkg) => pkg.name));
        for (const action of plan.publishPackages) {
          expect(action.distTag).toBe(action.pkg.version.includes('-') ? 'next' : 'latest');
        }
      }),
      { numRuns: 300 },
    );
  });
});

function packageSubset(): fc.Arbitrary<ReleasePackageInfo[]> {
  return fc.subarray(packages, { minLength: 0, maxLength: packages.length });
}

function releaseRecords(): fc.Arbitrary<ReleaseTagRecord[]> {
  return fc
    .array(
      fc.record({
        shaIndex: fc.integer({ min: 0, max: 3 }),
        pkgIndex: fc.integer({ min: 0, max: packages.length - 1 }),
        timestamp: fc.integer({ min: 1, max: 10_000 }),
        needsNpmPublish: fc.boolean(),
        needsGithubRelease: fc.boolean(),
      }),
      { minLength: 1, maxLength: 12 },
    )
    .map((rows) => {
      const seen = new Set<string>();
      const records: ReleaseTagRecord[] = [];
      for (const row of rows) {
        const pkg = packages[row.pkgIndex];
        if (!pkg) {
          continue;
        }
        const sha = `commit-${row.shaIndex}`;
        const key = `${sha}:${pkg.name}`;
        if (seen.has(key)) {
          continue;
        }
        seen.add(key);
        records.push({
          tag: `${pkg.projectName}@${pkg.version}`,
          sha,
          timestamp: row.timestamp,
          pkg,
          needsNpmPublish: row.needsNpmPublish,
          needsGithubRelease: row.needsGithubRelease,
        });
      }
      return records.length > 0 ? records : [record(packages[0], 'commit-0')];
    });
}

function record(pkg: ReleasePackageInfo | undefined, sha: string): ReleaseTagRecord {
  if (!pkg) {
    throw new Error('Missing test package fixture.');
  }
  return {
    tag: `${pkg.projectName}@${pkg.version}`,
    sha,
    timestamp: 1,
    pkg,
    needsNpmPublish: false,
    needsGithubRelease: false,
  };
}
