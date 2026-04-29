import type { ReleasePackageInfo, ReleaseTarget } from './core.js';
import { planPublishActions } from './publish-plan.js';

export type ReleaseVersionMode = 'new' | 'none';

export interface ReleaseSummary<Package extends ReleasePackageInfo = ReleasePackageInfo> {
  sha: string;
  dryRun: boolean;
  packages: Package[];
  pushed: boolean;
  published: Package[];
  alreadyPublished: Package[];
  githubReleases: Package[];
  rerunRequired: boolean;
  noRelease: boolean;
}

export interface ReleaseCompletionShell<Package extends ReleasePackageInfo = ReleasePackageInfo> {
  gitHead(): Promise<string>;
  pushReleaseRefs(packages: Package[]): Promise<boolean>;
  listNpmMissingPackages(packages: Package[]): Promise<Package[]>;
  buildReleaseCandidate(packages: Package[]): Promise<void>;
  publishPackage(pkg: Package, distTag: string, dryRun: boolean): Promise<void>;
  listGithubMissingPackages(packages: Package[]): Promise<Package[]>;
  createGithubRelease(pkg: Package, dryRun: boolean): Promise<void>;
}

export interface ReleaseRepairShell<Package extends ReleasePackageInfo = ReleasePackageInfo>
  extends ReleaseCompletionShell<Package> {
  checkout(ref: string): Promise<void>;
  withDevenvEnv<T>(runWithEnv: () => Promise<T>): Promise<T>;
  beforeRepairTarget?(target: ReleaseTarget<Package>): void;
  afterRepairTarget?(target: ReleaseTarget<Package>): void;
}

export interface ReleaseVersionShell<Package extends ReleasePackageInfo = ReleasePackageInfo> {
  releasePackagesAtHead(): Promise<Package[]>;
  releaseVersionPackages(bump: string): Promise<Package[]>;
  ensureLocalReleaseTags(packages: Package[]): Promise<void>;
  gitHead(): Promise<string>;
  runNxReleaseVersion(packages: Package[], bump: string, dryRun: boolean): Promise<void>;
  assertCleanGitTree(): Promise<void>;
}

export interface ReleaseVersionResult<Package extends ReleasePackageInfo = ReleasePackageInfo> {
  mode: ReleaseVersionMode;
  packages: Package[];
  status: 'already-release-target' | 'dry-run' | 'new-release' | 'no-release-needed';
}

export async function runReleaseVersion<Package extends ReleasePackageInfo>(
  shell: ReleaseVersionShell<Package>,
  options: { bump: string; dryRun: boolean },
): Promise<ReleaseVersionResult<Package>> {
  if (!options.dryRun) {
    const localRelease = await shell.releasePackagesAtHead();
    if (localRelease.length > 0) {
      await shell.ensureLocalReleaseTags(localRelease);
      return { mode: 'none', packages: localRelease, status: 'already-release-target' };
    }
  }

  const versionPackages = await shell.releaseVersionPackages(options.bump);
  if (versionPackages.length === 0) {
    if (options.bump !== 'auto') {
      // invariant throw: the CLI resolves forced bumps from the full owned release package set.
      throw new Error(`No release packages were selected for forced --bump ${options.bump}.`);
    }
    return { mode: 'none', packages: [], status: 'no-release-needed' };
  }

  const headBeforeVersioning = await shell.gitHead();
  await shell.runNxReleaseVersion(versionPackages, options.bump, options.dryRun);
  if (options.dryRun) {
    return { mode: 'none', packages: [], status: 'dry-run' };
  }

  await shell.assertCleanGitTree();
  const headAfterVersioning = await shell.gitHead();
  const releasedPackages = await shell.releasePackagesAtHead();
  await shell.ensureLocalReleaseTags(releasedPackages);
  if (headAfterVersioning === headBeforeVersioning) {
    if (releasedPackages.length > 0) {
      return { mode: 'none', packages: releasedPackages, status: 'already-release-target' };
    }
    if (options.bump !== 'auto') {
      throw new Error(`Nx did not create a release commit for forced --bump ${options.bump}.`);
    }
    return { mode: 'none', packages: [], status: 'no-release-needed' };
  }
  if (releasedPackages.length === 0) {
    throw new Error('Nx created a release commit without current package release tags at HEAD.');
  }
  return { mode: 'new', packages: releasedPackages, status: 'new-release' };
}

export async function completeReleaseAtHead<Package extends ReleasePackageInfo>(
  shell: ReleaseCompletionShell<Package>,
  packages: Package[],
  dryRun: boolean,
  rerunRequired: boolean,
): Promise<ReleaseSummary<Package>> {
  const npmMissingPackages = await shell.listNpmMissingPackages(packages);
  const githubMissingPackages = dryRun ? packages : await shell.listGithubMissingPackages(packages);
  return completePlannedRelease(shell, {
    sha: await shell.gitHead(),
    dryRun,
    packages,
    pushed: dryRun ? false : await shell.pushReleaseRefs(packages),
    npmMissingPackages,
    githubMissingPackages,
    rerunRequired,
  });
}

export async function repairPendingTargets<Package extends ReleasePackageInfo>(
  shell: ReleaseRepairShell<Package>,
  targets: Array<ReleaseTarget<Package>>,
  restoreRef: string,
  dryRun: boolean,
): Promise<Array<ReleaseSummary<Package>>> {
  const summaries: Array<ReleaseSummary<Package>> = [];
  try {
    for (const target of targets) {
      await shell.checkout(target.sha);
      if (target.packages.length === 0) {
        throw new Error(`Release commit ${target.sha} has no release packages after checkout.`);
      }
      shell.beforeRepairTarget?.(target);
      try {
        summaries.push(await shell.withDevenvEnv(() => completeRepairTargetAtHead(shell, target, dryRun)));
      } finally {
        shell.afterRepairTarget?.(target);
      }
    }
  } finally {
    await shell.checkout(restoreRef);
  }
  return summaries;
}

export async function completeRepairTargetAtHead<Package extends ReleasePackageInfo>(
  shell: ReleaseCompletionShell<Package>,
  target: ReleaseTarget<Package>,
  dryRun: boolean,
): Promise<ReleaseSummary<Package>> {
  return completePlannedRelease(shell, {
    sha: await shell.gitHead(),
    dryRun,
    packages: target.packages,
    pushed: dryRun ? false : await shell.pushReleaseRefs(target.packages),
    npmMissingPackages: target.npmPackages,
    githubMissingPackages: target.githubPackages,
    rerunRequired: false,
  });
}

async function completePlannedRelease<Package extends ReleasePackageInfo>(
  shell: ReleaseCompletionShell<Package>,
  input: {
    sha: string;
    dryRun: boolean;
    packages: Package[];
    pushed: boolean;
    npmMissingPackages: Package[];
    githubMissingPackages: Package[];
    rerunRequired: boolean;
  },
): Promise<ReleaseSummary<Package>> {
  const plan = planPublishActions({
    releasePackages: input.packages,
    npmMissingPackages: input.npmMissingPackages,
    githubMissingPackages: input.githubMissingPackages,
  });
  if (plan.buildProjects.length > 0) {
    await shell.buildReleaseCandidate(plan.publishPackages.map((action) => action.pkg));
  }
  for (const { pkg, distTag } of plan.publishPackages) {
    await shell.publishPackage(pkg, distTag, input.dryRun);
  }
  for (const pkg of plan.githubReleasePackages) {
    await shell.createGithubRelease(pkg, input.dryRun);
  }

  return {
    sha: input.sha,
    dryRun: input.dryRun,
    packages: input.packages,
    pushed: input.pushed,
    published: input.dryRun ? [] : plan.publishPackages.map((action) => action.pkg),
    alreadyPublished: input.packages.filter((pkg) => !input.npmMissingPackages.includes(pkg)),
    githubReleases: input.dryRun ? [] : plan.githubReleasePackages,
    rerunRequired: input.rerunRequired,
    noRelease: false,
  };
}
