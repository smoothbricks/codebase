import { describe, expect, it } from 'bun:test';
import { type ReleasePackageInfo, type ReleaseTarget, releaseTag } from '../core.js';
import {
  bumpStableReleaseToNext,
  completeReleaseAtHead,
  type ReleaseRepairShell,
  type ReleaseVersionShell,
  repairPendingTargets,
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
  it('repairs an older target with one checkout, one devenv load, npm-only build, and GitHub create calls', async () => {
    const target = releaseTarget('older-release', [stable, prerelease], [stable], [prerelease]);
    const shell = new RecordingRepairShell();

    const summaries = await repairPendingTargets(shell, [target], 'restore-ref', false);

    expect(shell.checkouts).toEqual(['older-release', 'restore-ref']);
    expect(shell.devenvLoads).toBe(1);
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
    expect(summary.rerunRequired).toBe(true);
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
  });

  it('does not run Nx versioning when HEAD is already a release target', async () => {
    const shell = new RecordingVersionShell({ releasePackagesAtHead: [[stable]] });

    const result = await runReleaseVersion(shell, { bump: 'patch', dryRun: false });

    expect(result).toEqual({ mode: 'none', packages: [stable], status: 'already-release-target' });
    expect(shell.nxRuns).toEqual([]);
    expect(shell.ensureCalls).toEqual([['@scope/stable']]);
  });

  it('runs forced Nx versioning for an untagged HEAD and reports a new release commit', async () => {
    const shell = new RecordingVersionShell({
      releasePackagesAtHead: [[], [stable]],
      releaseVersionPackages: [stable, prerelease],
      heads: ['before', 'after'],
    });

    const result = await runReleaseVersion(shell, { bump: 'patch', dryRun: false });

    expect(result).toEqual({ mode: 'new', packages: [stable], status: 'new-release' });
    expect(shell.nxRuns).toEqual([{ packages: ['@scope/stable', '@scope/prerelease'], bump: 'patch', dryRun: false }]);
    expect(shell.cleanChecks).toBe(1);
    expect(shell.ensureCalls).toEqual([['@scope/stable']]);
  });

  it('skips auto Nx versioning when no package-local candidates exist', async () => {
    const shell = new RecordingVersionShell({ releasePackagesAtHead: [[]], releaseVersionPackages: [] });

    const result = await runReleaseVersion(shell, { bump: 'auto', dryRun: false });

    expect(result).toEqual({ mode: 'none', packages: [], status: 'no-release-needed' });
    expect(shell.nxRuns).toEqual([]);
    expect(shell.cleanChecks).toBe(0);
    expect(shell.ensureCalls).toEqual([]);
  });

  it('runs auto Nx versioning only for selected package-local candidates', async () => {
    const shell = new RecordingVersionShell({
      releasePackagesAtHead: [[], [stable]],
      releaseVersionPackages: [stable],
      heads: ['before', 'after'],
    });

    const result = await runReleaseVersion(shell, { bump: 'auto', dryRun: false });

    expect(result).toEqual({ mode: 'new', packages: [stable], status: 'new-release' });
    expect(shell.nxRuns).toEqual([{ packages: ['@scope/stable'], bump: 'auto', dryRun: false }]);
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
  readonly githubCreates: Array<{ name: string; dryRun: boolean }> = [];
  readonly npmQueries: string[][] = [];
  readonly githubQueries: string[][] = [];
  devenvLoads = 0;
  currentRef = 'head';
  private readonly npmMissing: Set<string>;
  private readonly githubMissing: Set<string>;

  constructor(options: { npmMissing?: string[]; githubMissing?: string[] } = {}) {
    this.npmMissing = new Set(options.npmMissing ?? []);
    this.githubMissing = new Set(options.githubMissing ?? []);
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
    this.builds.push(packageNames(packages));
  }

  async publishPackage(pkg: ReleasePackageInfo, distTag: string, dryRun: boolean): Promise<void> {
    this.publishes.push({ name: pkg.name, distTag, dryRun });
  }

  async listGithubMissingPackages(packages: ReleasePackageInfo[]): Promise<ReleasePackageInfo[]> {
    this.githubQueries.push(packageNames(packages));
    return packages.filter((pkg) => this.githubMissing.has(pkg.name));
  }

  async createGithubRelease(pkg: ReleasePackageInfo, dryRun: boolean): Promise<void> {
    this.githubCreates.push({ name: pkg.name, dryRun });
  }

  async checkout(ref: string): Promise<void> {
    this.currentRef = ref;
    this.checkouts.push(ref);
  }

  async withDevenvEnv<T>(runWithEnv: () => Promise<T>): Promise<T> {
    this.devenvLoads += 1;
    return runWithEnv();
  }
}

class RecordingVersionShell implements ReleaseVersionShell<ReleasePackageInfo> {
  readonly ensureCalls: string[][] = [];
  readonly nxRuns: Array<{ packages: string[]; bump: string; dryRun: boolean }> = [];
  cleanChecks = 0;
  private readonly releaseBatches: ReleasePackageInfo[][];
  private readonly versionPackages: ReleasePackageInfo[];
  private readonly heads: string[];

  constructor(options: {
    releasePackagesAtHead: ReleasePackageInfo[][];
    releaseVersionPackages?: ReleasePackageInfo[];
    heads?: string[];
  }) {
    this.releaseBatches = [...options.releasePackagesAtHead];
    this.versionPackages = options.releaseVersionPackages ?? [stable, prerelease];
    this.heads = [...(options.heads ?? [])];
  }

  async releasePackagesAtHead(): Promise<ReleasePackageInfo[]> {
    return this.releaseBatches.shift() ?? [];
  }

  async releaseVersionPackages(): Promise<ReleasePackageInfo[]> {
    return this.versionPackages;
  }

  async ensureLocalReleaseTags(packages: ReleasePackageInfo[]): Promise<void> {
    this.ensureCalls.push(packageNames(packages));
  }

  async gitHead(): Promise<string> {
    return this.heads.shift() ?? 'head';
  }

  async runNxReleaseVersion(packages: ReleasePackageInfo[], bump: string, dryRun: boolean): Promise<void> {
    this.nxRuns.push({ packages: packageNames(packages), bump, dryRun });
  }

  async assertCleanGitTree(): Promise<void> {
    this.cleanChecks += 1;
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
