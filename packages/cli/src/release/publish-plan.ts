import { npmDistTagForVersion, type ReleasePackageInfo } from './core.js';

export interface PublishPackageAction<Package extends ReleasePackageInfo = ReleasePackageInfo> {
  pkg: Package;
  distTag: string;
}

export interface PublishPlan<Package extends ReleasePackageInfo = ReleasePackageInfo> {
  buildProjects: string[];
  publishPackages: Array<PublishPackageAction<Package>>;
  githubReleasePackages: Package[];
}

export interface PublishPlanInput<Package extends ReleasePackageInfo = ReleasePackageInfo> {
  releasePackages: Package[];
  npmMissingPackages: Package[];
  githubMissingPackages: Package[];
}

export function planPublishActions<Package extends ReleasePackageInfo>(
  input: PublishPlanInput<Package>,
): PublishPlan<Package> {
  const npmMissingNames = packageNameSet(input.npmMissingPackages);
  const githubMissingNames = packageNameSet(input.githubMissingPackages);
  const npmPackages = input.releasePackages.filter((pkg) => npmMissingNames.has(pkg.name));
  const githubReleasePackages = input.releasePackages.filter((pkg) => githubMissingNames.has(pkg.name));

  return {
    buildProjects: npmPackages.map((pkg) => pkg.projectName),
    publishPackages: npmPackages.map((pkg) => ({ pkg, distTag: npmDistTagForVersion(pkg.version) })),
    githubReleasePackages,
  };
}

function packageNameSet(packages: ReleasePackageInfo[]): Set<string> {
  return new Set(packages.map((pkg) => pkg.name));
}
