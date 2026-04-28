export interface ReleasePackageInfo {
  name: string;
  path: string;
  version: string;
}

export interface GitReleaseTagInfo {
  name: string;
  sha: string;
  timestamp: number;
}

export interface MatchedReleaseTag<Package extends ReleasePackageInfo = ReleasePackageInfo> {
  tag: string;
  sha: string;
  timestamp: number;
  pkg: Package;
}

export interface ReleaseTagRecord<Package extends ReleasePackageInfo = ReleasePackageInfo>
  extends MatchedReleaseTag<Package> {
  needsNpmPublish: boolean;
  needsGithubRelease: boolean;
}

export interface ReleaseTarget<Package extends ReleasePackageInfo = ReleasePackageInfo> {
  sha: string;
  timestamp: number;
  packages: Package[];
  npmPackages: Package[];
  githubPackages: Package[];
}

export interface DurableTagState {
  npmPublished: boolean;
  githubReleaseExists: boolean;
}

export interface ReleasePlanningShell {
  listReleaseTagsByCreatorDate(): Promise<GitReleaseTagInfo[]>;
  isAncestor(ancestor: string, descendant: string): Promise<boolean>;
  packageVersionAtRef(packagePath: string, ref: string): Promise<string | null>;
  durableTagState(pkg: ReleasePackageInfo, tag: string): Promise<DurableTagState>;
}

export function releaseTag(pkg: Pick<ReleasePackageInfo, 'name' | 'version'>): string {
  return `${pkg.name}@${pkg.version}`;
}

export function npmDistTagForVersion(version: string): string {
  return version.includes('-') ? 'next' : 'latest';
}

export function releasePackageForTag<Package extends Omit<ReleasePackageInfo, 'version'>>(
  packages: Package[],
  tag: string,
): { pkg: Package; version: string } | null {
  for (const pkg of packages) {
    const prefix = `${pkg.name}@`;
    if (tag.startsWith(prefix)) {
      const version = tag.slice(prefix.length);
      if (!version) {
        throw new Error(`Release tag ${tag} does not include a version.`);
      }
      return { pkg, version };
    }
  }
  return null;
}

export function classifyReleaseTag<Package extends ReleasePackageInfo>(
  tag: MatchedReleaseTag<Package>,
  state: DurableTagState,
): ReleaseTagRecord<Package> {
  return {
    ...tag,
    needsNpmPublish: !state.npmPublished,
    needsGithubRelease: !state.githubReleaseExists,
  };
}

export async function collectOwnedReleaseTagRecords<Package extends Omit<ReleasePackageInfo, 'version'>>(
  packages: Package[],
  ref: string,
  shell: ReleasePlanningShell,
): Promise<Array<ReleaseTagRecord<Package & { version: string }>>> {
  const records: Array<ReleaseTagRecord<Package & { version: string }>> = [];
  const completedPackages = new Set<string>();
  for (const tag of await shell.listReleaseTagsByCreatorDate()) {
    const match = releasePackageForTag(packages, tag.name);
    if (match && completedPackages.has(match.pkg.name)) {
      continue;
    }
    if (!match || !(await shell.isAncestor(tag.sha, ref))) {
      continue;
    }
    const versionAtTag = await shell.packageVersionAtRef(match.pkg.path, tag.sha);
    if (versionAtTag !== match.version) {
      throw new Error(
        `Release tag ${tag.name} points at ${tag.sha.slice(0, 12)}, but ${match.pkg.path}/package.json has version ${
          versionAtTag ?? 'missing'
        }.`,
      );
    }
    const pkg = { ...match.pkg, version: match.version };
    const state = await shell.durableTagState(pkg, tag.name);
    records.push(classifyReleaseTag({ tag: tag.name, sha: tag.sha, timestamp: tag.timestamp, pkg }, state));
    if (state.npmPublished && state.githubReleaseExists) {
      completedPackages.add(pkg.name);
    }
  }
  return records;
}

export function groupReleaseTargets<Package extends ReleasePackageInfo>(
  records: ReleaseTagRecord<Package>[],
): ReleaseTarget<Package>[] {
  const targets = new Map<string, ReleaseTarget<Package>>();
  const packageNamesByTarget = new Map<string, Set<string>>();
  for (const record of records) {
    let target = targets.get(record.sha);
    let packageNames = packageNamesByTarget.get(record.sha);
    if (!target) {
      target = { sha: record.sha, timestamp: record.timestamp, packages: [], npmPackages: [], githubPackages: [] };
      targets.set(record.sha, target);
      packageNames = new Set<string>();
      packageNamesByTarget.set(record.sha, packageNames);
    } else if (record.timestamp > target.timestamp) {
      target.timestamp = record.timestamp;
    }
    if (!packageNames) {
      throw new Error(`Release target ${record.sha.slice(0, 12)} lost package tracking state.`);
    }
    if (packageNames.has(record.pkg.name)) {
      throw new Error(
        `Release target ${record.sha.slice(0, 12)} has more than one release tag for ${record.pkg.name}.`,
      );
    }
    packageNames.add(record.pkg.name);
    if (record.needsNpmPublish || record.needsGithubRelease) {
      target.packages.push(record.pkg);
    }
    if (record.needsNpmPublish) {
      target.npmPackages.push(record.pkg);
    }
    if (record.needsGithubRelease) {
      target.githubPackages.push(record.pkg);
    }
  }
  return [...targets.values()].sort(
    (left, right) => right.timestamp - left.timestamp || right.sha.localeCompare(left.sha),
  );
}

export function pendingReleaseTargets<Package extends ReleasePackageInfo>(
  records: ReleaseTagRecord<Package>[],
  headSha: string,
): ReleaseTarget<Package>[] {
  const pending: ReleaseTarget<Package>[] = [];
  for (const target of groupReleaseTargets(records)) {
    if (target.sha === headSha) {
      continue;
    }
    if (target.npmPackages.length === 0 && target.githubPackages.length === 0) {
      break;
    }
    pending.push(target);
  }
  return pending.reverse();
}
