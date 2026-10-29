import { describe, expect, it } from 'bun:test';
import { type ReleasePackageInfo, type ReleaseTarget, releaseTag } from '../core.js';
import {
  bumpStableReleaseToNext,
  collectRepairPlatformOutputs,
  completeReleaseAtHead,
  prepareReleaseCandidate,
  type ReleaseRepairOutputsShell,
  type ReleaseRepairShell,
  type ReleaseTagShell,
  type ReleaseVersionPlan,
  type ReleaseVersionShell,
  repairPendingTargets,
  runReleaseTag,
  runReleaseVersion,
} from '../orchestration.js';

const stable: ReleasePackageInfo = {
  name: '@scope/stable',
  projectName: 'stable',
  path: 'packages/stable',
  version: '1.0.0',
};
const prerelease: ReleasePackageInfo = {
  name: '@scope/prerelease',
  projectName: 'prerelease',
  path: 'packages/prerelease',
  version: '2.0.0-beta.1',
};

describe('release orchestration', () => {
  it('repairs an older target and refreshes devenv after restoring the caller ref', async () => {
    const target = releaseTarget('older-release', [stable, prerelease], [stable], [prerelease]);
    const shell = new RecordingRepairShell();

    const summaries = await repairPendingTargets(shell, [target], 'restore-ref', false);

    expect(shell.checkouts).toEqual(['older-release', 'restore-ref']);
    expect(shell.devenvLoads).toBe(2);
    expect(shell.devenvRefs).toEqual(['older-release', 'restore-ref']);
    expect(shell.prepares).toEqual(['older-release']);
    expect(shell.builds).toEqual([['@scope/stable']]);
    expect(shell.publishes).toEqual([{ name: '@scope/stable', distTag: 'latest', dryRun: false }]);
    expect(shell.githubCreates).toEqual([{ name: '@scope/prerelease', dryRun: false }]);
    expect(shell.pushes).toEqual([['stable@1.0.0', 'prerelease@2.0.0-beta.1']]);
    expect(summaries).toHaveLength(1);
    expect(summaries[0]?.published.map((pkg) => pkg.name)).toEqual(['@scope/stable']);
    expect(summaries[0]?.githubReleases.map((pkg) => pkg.name)).toEqual(['@scope/prerelease']);
  });

  it('skips build and publish for GitHub-only repair targets', async () => {
    const target = releaseTarget('github-only', [prerelease], [], [prerelease]);
    const shell = new RecordingRepairShell();

    await repairPendingTargets(shell, [target], 'restore-ref', false);

    expect(shell.builds).toEqual([]);
    expect(shell.publishes).toEqual([]);
    expect(shell.githubCreates).toEqual([{ name: '@scope/prerelease', dryRun: false }]);
  });

  it('dry-run repair reports planned targets without checkout, devenv, build, publish, or GitHub calls', async () => {
    const target = releaseTarget('github-only', [prerelease], [], [prerelease]);
    const shell = new RecordingRepairShell();

    const summaries = await repairPendingTargets(shell, [target], 'restore-ref', true);

    expect(shell.checkouts).toEqual([]);
    expect(shell.devenvLoads).toBe(0);
    expect(shell.builds).toEqual([]);
    expect(shell.publishes).toEqual([]);
    expect(shell.githubCreates).toEqual([]);
    expect(summaries).toEqual([
      {
        sha: 'github-only',
        dryRun: true,
        packages: [prerelease],
        pushed: false,
        published: [],
        alreadyPublished: [prerelease],
        githubReleases: [],
        githubReleaseLinks: [],
        rerunRequired: false,
        noRelease: false,
      },
    ]);
  });

  it('collects historical platform outputs only for npm-missing repair targets', async () => {
    const githubOnly = releaseTarget('github-only', [prerelease], [], [prerelease]);
    const npmMissing = releaseTarget('npm-missing', [stable], [stable], []);
    const shell = new RecordingRepairOutputsShell();

    await collectRepairPlatformOutputs(shell, [githubOnly, npmMissing], 'restore-ref');

    expect(shell.checkouts).toEqual(['npm-missing', 'restore-ref']);
    expect(shell.devenvRefs).toEqual(['npm-missing', 'restore-ref']);
    expect(shell.collected).toEqual(['npm-missing']);
  });

  it('does nothing when no pending release needs npm platform outputs', async () => {
    const shell = new RecordingRepairOutputsShell();
    const githubOnly = releaseTarget('github-only', [prerelease], [], [prerelease]);

    await collectRepairPlatformOutputs(shell, [githubOnly], 'restore-ref');

    expect(shell.checkouts).toEqual([]);
    expect(shell.devenvRefs).toEqual([]);
    expect(shell.collected).toEqual([]);
  });

  it('publishes a partial HEAD release by building npm-missing packages and creating missing GitHub Releases', async () => {
    const shell = new RecordingRepairShell({ npmMissing: ['@scope/stable'], githubMissing: ['@scope/prerelease'] });

    const summary = await completeReleaseAtHead(shell, [stable, prerelease], false, true);

    expect(shell.checkouts).toEqual([]);
    expect(shell.devenvLoads).toBe(0);
    expect(shell.builds).toEqual([['@scope/stable']]);
    expect(shell.publishes).toEqual([{ name: '@scope/stable', distTag: 'latest', dryRun: false }]);
    expect(shell.githubCreates).toEqual([{ name: '@scope/prerelease', dryRun: false }]);
    expect(shell.pushes).toEqual([['stable@1.0.0', 'prerelease@2.0.0-beta.1']]);
    expect(summary.published.map((pkg) => pkg.name)).toEqual(['@scope/stable']);
    expect(summary.alreadyPublished.map((pkg) => pkg.name)).toEqual(['@scope/prerelease']);
    expect(summary.githubReleases.map((pkg) => pkg.name)).toEqual(['@scope/prerelease']);
    expect(summary.githubReleaseLinks).toEqual([
      { pkg: prerelease, url: 'https://github.test/prerelease@2.0.0-beta.1' },
    ]);
    expect(summary.rerunRequired).toBe(true);
  });

  it('publishes verified prebuilt artifacts without building them again', async () => {
    const shell = new RecordingRepairShell({ npmMissing: ['@scope/stable'], prebuilt: true });

    const summary = await completeReleaseAtHead(shell, [stable], false, false);

    expect(shell.builds).toEqual([]);
    expect(shell.prebuiltChecks).toEqual(['@scope/stable']);
    expect(shell.publishes).toEqual([{ name: '@scope/stable', distTag: 'latest', dryRun: false }]);
    expect(summary.published).toEqual([stable]);
  });

  it('refuses to publish when a named prebuilt artifact is missing', async () => {
    const shell = new RecordingRepairShell({
      npmMissing: ['@scope/stable'],
      prebuilt: true,
      missingPrebuilt: ['@scope/stable'],
    });

    await expect(completeReleaseAtHead(shell, [stable], false, false)).rejects.toThrow(
      'Verified prebuilt artifact for @scope/stable (stable) is missing: declared build output is absent',
    );
    expect(shell.builds).toEqual([]);
    expect(shell.publishes).toEqual([]);
  });

  it('leaves a complete HEAD release idempotent', async () => {
    const shell = new RecordingRepairShell();

    const summary = await completeReleaseAtHead(shell, [stable, prerelease], false, false);

    expect(shell.builds).toEqual([]);
    expect(shell.publishes).toEqual([]);
    expect(shell.githubCreates).toEqual([]);
    expect(summary.published).toEqual([]);
    expect(summary.alreadyPublished.map((pkg) => pkg.name)).toEqual(['@scope/stable', '@scope/prerelease']);
    expect(summary.githubReleases).toEqual([]);
    expect(summary.githubReleaseLinks).toEqual([]);
  });

  it('reports a HEAD release target as complete without running Nx versioning', async () => {
    const shell = new RecordingVersionShell({ releasePackagesAtHead: [[stable]] });

    const result = await runReleaseVersion(shell, { bump: 'patch', dryRun: false });

    expect(result).toEqual({ mode: 'complete', packages: [stable] });
    expect(shell.runs).toEqual([]);
  });

  it('plans a HEAD release target as complete in dry run, so the gates validate what publish ships', async () => {
    // Tags moved to HEAD by retag-unpublished leave no package-local change for
    // the Nx preview to find; the plan must still name the packages at HEAD.
    const shell = new RecordingVersionShell({ releasePackagesAtHead: [[stable]], releaseVersionPackages: [] });

    const result = await runReleaseVersion(shell, { bump: 'auto', dryRun: true });

    expect(result).toEqual({ mode: 'complete', packages: [stable] });
    expect(shell.runs).toEqual([]);
  });

  it('reports complete when Nx leaves HEAD in place on a release target', async () => {
    const shell = new RecordingVersionShell({
      releasePackagesAtHead: [[], [stable]],
      releaseVersionPackages: [stable],
      heads: ['same', 'same'],
    });

    const result = await runReleaseVersion(shell, { bump: 'auto', dryRun: false });

    expect(result).toEqual({ mode: 'complete', packages: [stable] });
    expect(shell.cleanChecks).toBe(1);
  });

  it('runs forced Nx versioning for an untagged HEAD and reports a new release commit', async () => {
    const shell = new RecordingVersionShell({
      releasePackagesAtHead: [[], [stable]],
      releaseVersionPackages: [stable, prerelease],
      heads: ['before', 'after'],
    });

    const result = await runReleaseVersion(shell, { bump: 'patch', dryRun: false });

    expect(result).toEqual({ mode: 'new', packages: [stable] });
    expect(shell.runs).toEqual([
      { bump: 'patch', nx: ['@scope/stable', '@scope/prerelease'], firstReleases: [], dryRun: false },
    ]);
    expect(shell.cleanChecks).toBe(1);
  });

  it.each([
    ['skips auto Nx versioning when no package-local candidates exist', 'auto'],
    ['no-ops an explicit bump when the changed-set selection is empty', 'patch'],
  ] as const)('%s', async (_case, bump) => {
    const shell = new RecordingVersionShell({ releasePackagesAtHead: [[]], releaseVersionPackages: [] });

    const result = await runReleaseVersion(shell, { bump, dryRun: false });

    expect(result).toEqual({ mode: 'none', packages: [] });
    expect(shell.runs).toEqual([]);
    expect(shell.cleanChecks).toBe(0);
  });

  it('refuses a release commit that versions none of the current release packages', async () => {
    const shell = new RecordingVersionShell({
      releasePackagesAtHead: [[], []],
      releaseVersionPackages: [stable],
      heads: ['before', 'after'],
    });

    await expect(runReleaseVersion(shell, { bump: 'auto', dryRun: false })).rejects.toThrow(
      'The release commit versions none of the current release packages.',
    );
  });

  describe('a package with no release tag under auto', () => {
    const fresh: ReleasePackageInfo = {
      name: '@scope/fresh',
      projectName: 'fresh',
      path: 'packages/fresh',
      version: '2.3.0',
    };

    it('is released at the version it declares while Nx derives the tagged package, in one run', async () => {
      const shell = new RecordingVersionShell({
        releasePackagesAtHead: [[], [stable, fresh]],
        releaseVersionPackages: [stable, fresh],
        heads: ['before', 'after'],
      });
      shell.untagged.add(fresh.projectName);

      const result = await runReleaseVersion(shell, { bump: 'auto', dryRun: false });

      expect(shell.runs).toEqual([
        {
          bump: 'auto',
          nx: ['@scope/stable'],
          firstReleases: [{ name: '@scope/fresh', version: '2.3.0' }],
          dryRun: false,
        },
      ]);
      expect(result).toEqual({ mode: 'new', packages: [stable, fresh] });
      expect(shell.cleanChecks).toBe(1);
    });

    it.each([
      ['at the stable version it declares', '2.3.0', '2.3.0'],
      ['at 0.0.1 when it declares 0.0.0', '0.0.0', '0.0.1'],
      ['at the stable version a declared development prerelease stands for', '0.4.2-next.0', '0.4.2'],
      ['at 0.0.1 when it declares no semver version', 'latest', '0.0.1'],
    ])('is released %s', async (_case, declared, released) => {
      const pkg = { ...fresh, version: declared };
      const shell = new RecordingVersionShell({ releasePackagesAtHead: [[]], releaseVersionPackages: [pkg] });
      shell.untagged.add(pkg.projectName);

      const result = await runReleaseVersion(shell, { bump: 'auto', dryRun: true });

      expect(shell.runs).toEqual([
        { bump: 'auto', nx: [], firstReleases: [{ name: '@scope/fresh', version: released }], dryRun: true },
      ]);
      expect(result).toEqual({ mode: 'dry-run', packages: [{ ...pkg, version: released }] });
    });

    it('keeps its own version in a dry run when Nx also versions it as a dependent of a tagged package', async () => {
      const derived = { ...stable, version: '1.1.0' };
      const dependent = { ...fresh, version: '2.3.1' };
      const shell = new RecordingVersionShell({
        releasePackagesAtHead: [[]],
        releaseVersionPackages: [stable, fresh],
        preview: [derived, dependent],
      });
      shell.untagged.add(fresh.projectName);

      const result = await runReleaseVersion(shell, { bump: 'auto', dryRun: true });

      expect(result).toEqual({ mode: 'dry-run', packages: [derived, fresh] });
    });

    it('is versioned by an explicit bump like any other package', async () => {
      const shell = new RecordingVersionShell({ releasePackagesAtHead: [[]], releaseVersionPackages: [stable, fresh] });
      shell.untagged.add(fresh.projectName);

      await runReleaseVersion(shell, { bump: 'minor', dryRun: true });

      expect(shell.tagLookups).toEqual([]);
      expect(shell.runs).toEqual([
        { bump: 'minor', nx: ['@scope/stable', '@scope/fresh'], firstReleases: [], dryRun: true },
      ]);
    });
  });

  it('runs auto Nx versioning only for selected package-local candidates', async () => {
    const shell = new RecordingVersionShell({
      releasePackagesAtHead: [[], [stable]],
      releaseVersionPackages: [stable],
      heads: ['before', 'after'],
    });

    const result = await runReleaseVersion(shell, { bump: 'auto', dryRun: false });

    expect(result).toEqual({ mode: 'new', packages: [stable] });
    expect(shell.runs).toEqual([{ bump: 'auto', nx: ['@scope/stable'], firstReleases: [], dryRun: false }]);
  });

  it('reports dry-run version packages as a release candidate for validation', async () => {
    const preview = { ...stable, version: '1.1.0' };
    const shell = new RecordingVersionShell({ releasePackagesAtHead: [[]], releaseVersionPackages: [stable], preview });

    const result = await runReleaseVersion(shell, { bump: 'auto', dryRun: true });

    expect(result).toEqual({ mode: 'dry-run', packages: [preview] });
    expect(shell.runs).toEqual([{ bump: 'auto', nx: ['@scope/stable'], firstReleases: [], dryRun: true }]);
  });

  it('tags every release package at HEAD without touching versioning', async () => {
    const shell = new RecordingTagShell([stable, prerelease]);

    const result = await runReleaseTag(shell, { dryRun: false });

    expect(result).toEqual({ packages: [stable, prerelease], status: 'tagged' });
    expect(shell.ensureCalls).toEqual([['@scope/stable', '@scope/prerelease']]);
  });

  it('reports the tags a dry run would create and writes none', async () => {
    const shell = new RecordingTagShell([stable]);

    const result = await runReleaseTag(shell, { dryRun: true });

    expect(result).toEqual({ packages: [stable], status: 'dry-run' });
    expect(shell.ensureCalls).toEqual([]);
  });

  it('writes no tags when HEAD carries no release', async () => {
    const shell = new RecordingTagShell([]);

    const result = await runReleaseTag(shell, { dryRun: false });

    expect(result).toEqual({ packages: [], status: 'no-release' });
    expect(shell.ensureCalls).toEqual([]);
  });

  it('bumps stable release packages to next after publish completion', async () => {
    const shell = new RecordingNextShell();

    const bumped = await bumpStableReleaseToNext(shell, [stable, prerelease], false, false);

    expect(bumped.map((pkg) => pkg.name)).toEqual(['@scope/stable']);
    expect(shell.bumped).toEqual([['@scope/stable']]);
  });

  it('does not bump prerelease-only or dry-run publishes to next', async () => {
    const shell = new RecordingNextShell();

    await expect(bumpStableReleaseToNext(shell, [prerelease], false, false)).resolves.toEqual([]);
    await expect(bumpStableReleaseToNext(shell, [stable], true, false)).resolves.toEqual([]);

    expect(shell.bumped).toEqual([]);
  });

  it('does not bump stable packages to next when newer branch commits remain', async () => {
    const shell = new RecordingNextShell();

    await expect(bumpStableReleaseToNext(shell, [stable], false, true)).resolves.toEqual([]);

    expect(shell.bumped).toEqual([]);
  });
});

function releaseTarget(
  sha: string,
  packages: ReleasePackageInfo[],
  npmPackages: ReleasePackageInfo[],
  githubPackages: ReleasePackageInfo[],
): ReleaseTarget<ReleasePackageInfo> {
  return { sha, timestamp: 1, packages, npmPackages, githubPackages };
}

class RecordingRepairShell implements ReleaseRepairShell<ReleasePackageInfo> {
  readonly checkouts: string[] = [];
  readonly pushes: string[][] = [];
  readonly builds: string[][] = [];
  readonly publishes: Array<{ name: string; distTag: string; dryRun: boolean }> = [];
  readonly prebuiltChecks: string[] = [];
  readonly githubCreates: Array<{ name: string; dryRun: boolean }> = [];
  readonly npmQueries: string[][] = [];
  readonly githubQueries: string[][] = [];
  devenvLoads = 0;
  readonly devenvRefs: string[] = [];
  currentRef = 'head';
  private readonly npmMissing: Set<string>;
  private readonly githubMissing: Set<string>;
  private readonly prebuilt: boolean;
  private readonly missingPrebuilt: Set<string>;
  readonly prepares: string[] = [];

  constructor(
    options: {
      npmMissing?: string[];
      githubMissing?: string[];
      prebuilt?: boolean;
      missingPrebuilt?: string[];
    } = {},
  ) {
    this.npmMissing = new Set(options.npmMissing ?? []);
    this.githubMissing = new Set(options.githubMissing ?? []);
    this.prebuilt = options.prebuilt === true;
    this.missingPrebuilt = new Set(options.missingPrebuilt ?? []);
  }

  async prepareRepairTarget(target: ReleaseTarget<ReleasePackageInfo>): Promise<void> {
    this.prepares.push(target.sha);
  }

  async gitHead(): Promise<string> {
    return this.currentRef;
  }

  async pushReleaseRefs(packages: ReleasePackageInfo[]): Promise<boolean> {
    this.pushes.push(packages.map((pkg) => releaseTag(pkg)));
    return true;
  }

  async listNpmMissingPackages(packages: ReleasePackageInfo[]): Promise<ReleasePackageInfo[]> {
    this.npmQueries.push(packageNames(packages));
    return packages.filter((pkg) => this.npmMissing.has(pkg.name));
  }

  async buildReleaseCandidate(packages: ReleasePackageInfo[]): Promise<void> {
    await prepareReleaseCandidate(
      {
        build: async (selected) => {
          this.builds.push(packageNames(selected));
        },
        assertPrebuilt: async (pkg) => {
          this.prebuiltChecks.push(pkg.name);
          if (this.missingPrebuilt.has(pkg.name)) {
            throw new Error('declared build output is absent');
          }
        },
      },
      packages,
      this.prebuilt,
    );
  }

  async publishPackage(pkg: ReleasePackageInfo, distTag: string, dryRun: boolean): Promise<void> {
    this.publishes.push({ name: pkg.name, distTag, dryRun });
  }

  async listGithubMissingPackages(packages: ReleasePackageInfo[]): Promise<ReleasePackageInfo[]> {
    this.githubQueries.push(packageNames(packages));
    return packages.filter((pkg) => this.githubMissing.has(pkg.name));
  }

  async createGithubRelease(pkg: ReleasePackageInfo, dryRun: boolean): Promise<string | null> {
    this.githubCreates.push({ name: pkg.name, dryRun });
    return dryRun ? null : `https://github.test/${releaseTag(pkg)}`;
  }

  async checkout(ref: string): Promise<void> {
    this.currentRef = ref;
    this.checkouts.push(ref);
  }

  async withDevenvEnv<T>(runWithEnv: () => Promise<T>): Promise<T> {
    this.devenvLoads += 1;
    this.devenvRefs.push(this.currentRef);
    return runWithEnv();
  }
}

class RecordingRepairOutputsShell implements ReleaseRepairOutputsShell<ReleasePackageInfo> {
  readonly checkouts: string[] = [];
  readonly collected: string[] = [];
  readonly devenvRefs: string[] = [];
  private currentRef = 'head';

  async checkout(ref: string): Promise<void> {
    this.currentRef = ref;
    this.checkouts.push(ref);
  }

  async withDevenvEnv<T>(runWithEnv: () => Promise<T>): Promise<T> {
    this.devenvRefs.push(this.currentRef);
    return runWithEnv();
  }

  async collectRepairTargetOutputs(target: ReleaseTarget<ReleasePackageInfo>): Promise<void> {
    this.collected.push(target.sha);
  }
}

class RecordingVersionShell implements ReleaseVersionShell<ReleasePackageInfo> {
  readonly runs: Array<{
    bump: string;
    nx: string[];
    firstReleases: Array<{ name: string; version: string }>;
    dryRun: boolean;
  }> = [];
  readonly tagLookups: string[] = [];
  cleanChecks = 0;
  private readonly releaseBatches: ReleasePackageInfo[][];
  private readonly versionPackages: ReleasePackageInfo[];
  private readonly heads: string[];
  private readonly preview: ReleasePackageInfo[];

  constructor(options: {
    releasePackagesAtHead: ReleasePackageInfo[][];
    releaseVersionPackages?: ReleasePackageInfo[];
    heads?: string[];
    preview?: ReleasePackageInfo | ReleasePackageInfo[];
  }) {
    this.releaseBatches = [...options.releasePackagesAtHead];
    this.versionPackages = options.releaseVersionPackages ?? [stable, prerelease];
    this.heads = [...(options.heads ?? [])];
    this.preview = Array.isArray(options.preview) ? options.preview : options.preview ? [options.preview] : [];
  }

  async releasePackagesAtHead(): Promise<ReleasePackageInfo[]> {
    return this.releaseBatches.shift() ?? [];
  }

  readonly untagged = new Set<string>();

  async hasReleaseTag(pkg: ReleasePackageInfo): Promise<boolean> {
    this.tagLookups.push(pkg.projectName);
    return !this.untagged.has(pkg.projectName);
  }

  async releaseVersionPackages(): Promise<ReleasePackageInfo[]> {
    return this.versionPackages;
  }

  async gitHead(): Promise<string> {
    return this.heads.shift() ?? 'head';
  }

  async versionRelease(plan: ReleaseVersionPlan<ReleasePackageInfo>, dryRun: boolean): Promise<ReleasePackageInfo[]> {
    this.runs.push({
      bump: plan.bump,
      nx: packageNames(plan.nx),
      firstReleases: plan.firstReleases.map((pkg) => ({ name: pkg.name, version: pkg.version })),
      dryRun,
    });
    return this.preview;
  }

  async assertCleanGitTree(): Promise<void> {
    this.cleanChecks += 1;
  }
}

class RecordingTagShell implements ReleaseTagShell<ReleasePackageInfo> {
  readonly ensureCalls: string[][] = [];

  constructor(private readonly packagesAtHead: ReleasePackageInfo[]) {}

  async releasePackagesAtHead(): Promise<ReleasePackageInfo[]> {
    return this.packagesAtHead;
  }

  async ensureLocalReleaseTags(packages: ReleasePackageInfo[]): Promise<void> {
    this.ensureCalls.push(packageNames(packages));
  }
}

class RecordingNextShell {
  readonly bumped: string[][] = [];

  async bumpStablePackagesToNext(packages: ReleasePackageInfo[]): Promise<void> {
    this.bumped.push(packageNames(packages));
  }
}

function packageNames(packages: ReleasePackageInfo[]): string[] {
  return packages.map((pkg) => pkg.name);
}
