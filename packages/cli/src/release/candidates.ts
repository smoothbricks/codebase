import type { ReleasePackageInfo } from './core.js';
import { releaseTag } from './core.js';

export interface AutoReleaseCandidateShell {
  gitRefExists(ref: string): Promise<boolean>;
  packageChangedSince(ref: string, packagePath: string): Promise<boolean>;
  packageHasHistory(packagePath: string): Promise<boolean>;
}

export async function autoReleaseCandidatePackages<Package extends ReleasePackageInfo>(
  shell: AutoReleaseCandidateShell,
  packages: Package[],
): Promise<Package[]> {
  const candidates: Package[] = [];
  for (const pkg of packages) {
    if (await isAutoReleaseCandidate(shell, pkg)) {
      candidates.push(pkg);
    }
  }
  return candidates;
}

async function isAutoReleaseCandidate<Package extends ReleasePackageInfo>(
  shell: AutoReleaseCandidateShell,
  pkg: Package,
): Promise<boolean> {
  const tagRef = `refs/tags/${releaseTag(pkg)}`;
  if (await shell.gitRefExists(tagRef)) {
    return shell.packageChangedSince(tagRef, pkg.path);
  }
  return shell.packageHasHistory(pkg.path);
}
