/**
 * What `--bump` accepts. `auto` has Nx derive each tagged package's bump from its commits and gives a package with no
 * release tag the version its first release publishes (`firstReleaseVersion`); the rest force one.
 */
export const RELEASE_BUMPS = ['auto', 'patch', 'minor', 'major', 'prerelease'] as const;

export interface ReleasePackageInfo {
  name: string;
  projectName: string;
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

export function releaseTag(pkg: Pick<ReleasePackageInfo, 'projectName' | 'version'>): string {
  return `${pkg.projectName}@${pkg.version}`;
}

export function legacyReleaseTag(pkg: Pick<ReleasePackageInfo, 'name' | 'version'>): string {
  return `${pkg.name}@${pkg.version}`;
}

export function releaseTagAliases(pkg: Pick<ReleasePackageInfo, 'name' | 'projectName' | 'version'>): string[] {
  return [...new Set([releaseTag(pkg), legacyReleaseTag(pkg)])];
}

export function npmDistTagForVersion(version: string): string {
  return version.includes('-') ? 'next' : 'latest';
}

/** semver.org's grammar for a version: the captures are its major, minor and patch numbers. */
const SEMVER_VERSION =
  /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*)?(?:\+[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*)?$/;

/**
 * The version a package's first release publishes.
 *
 * `--bump auto` has Nx derive each bump from the commits since the package's last release tag. A package with no tag
 * has no such commits, and the history Nx would read instead reaches back to the package's birth and fails on the first
 * commit whose scope it cannot resolve, so a first release is never derived: it is what the manifest declares.
 *
 * - `X.Y.Z` is released as it stands.
 * - `X.Y.Z-<prerelease>` is released as `X.Y.Z`. A prerelease version is the development version smoo leaves a package
 *   on after each stable release (`prerelease --preid next`), and that version is neither tagged nor published (README,
 *   "Publishing"), so it stands for the stable version it precedes. An explicit `patch` bump of it gives the same.
 * - `0.0.0`, and a declared version that is no semver at all, name no version to release: the first release is the
 *   patch after `0.0.0`, `0.0.1`.
 */
export function firstReleaseVersion(declared: string): string {
  const stable = SEMVER_VERSION.test(declared) ? declared.replace(SEMVER_VERSION, '$1.$2.$3') : '0.0.0';
  return stable === '0.0.0' ? '0.0.1' : stable;
}

/**
 * The subject of the commit that versions a release. `smoo release version` writes it and every later step finds the
 * release commit by it: the tags that would name the release are created afterwards, by the publishing job.
 */
export const RELEASE_COMMIT_SUBJECT = 'chore(release): publish';

const RELEASE_COMMIT_PROJECT_LINE = /^- project: (\S+) (\S+)$/;

/**
 * The paragraphs of a release commit's message: the subject, then a `- project: <name> <version>` paragraph for each
 * project the commit releases. That is the message Nx writes for an independent release, and the one smoo writes for a
 * release with first releases in it, so one reader (`releaseCommitProjects`) serves both.
 */
export function releaseCommitParagraphs(
  packages: readonly Pick<ReleasePackageInfo, 'projectName' | 'version'>[],
): string[] {
  return [RELEASE_COMMIT_SUBJECT, ...packages.map((pkg) => `- project: ${pkg.projectName} ${pkg.version}`)];
}

/** The versions, by project name, that a release commit's message lists. */
export function releaseCommitProjects(message: string): Map<string, string> {
  const listed = new Map<string, string>();
  for (const line of message.split('\n')) {
    const [, projectName, version] = RELEASE_COMMIT_PROJECT_LINE.exec(line.trim()) ?? [];
    if (projectName !== undefined && version !== undefined) {
      listed.set(projectName, version);
    }
  }
  return listed;
}

/**
 * Whether a release commit releases a package that stands at `pkg.version` in it. Changing the version is how a commit
 * usually does, and the whole story for a package Nx versions. A first release is the exception: it publishes the
 * version its manifest already declares, so the commit changes no manifest and the only record of it is the commit's
 * own list.
 */
export function releasedByCommit(
  pkg: Pick<ReleasePackageInfo, 'projectName' | 'version'>,
  versionBefore: string | null,
  listed: ReadonlyMap<string, string>,
): boolean {
  return pkg.version !== versionBefore || listed.get(pkg.projectName) === pkg.version;
}

export type ReleaseBranchPushAction = 'push' | 'skip' | 'diverged';

/**
 * Decide how the release commit relates to the release branch on the remote.
 *
 * `skip` covers the release commit already being on the remote, which is the repair
 * path re-pushing only missing tags. `diverged` is unresolvable inside the run: the
 * release candidate's artifacts are built from the release commit's tree, so rebasing
 * onto the new remote tip would publish package contents that do not match the tagged
 * commit. Refusing keeps the tag and the tarball describing the same tree.
 */
export function classifyReleaseBranchPush(state: {
  remoteExists: boolean;
  headIsAncestorOfRemote: boolean;
  remoteIsAncestorOfHead: boolean;
}): ReleaseBranchPushAction {
  if (!state.remoteExists) {
    return 'push';
  }
  if (state.headIsAncestorOfRemote) {
    return 'skip';
  }
  return state.remoteIsAncestorOfHead ? 'push' : 'diverged';
}

export function divergedReleaseBranchMessage(state: {
  branch: string;
  remoteRef: string;
  head: string;
  remoteSha: string;
}): string {
  return [
    `Release branch ${state.branch} diverged: ${state.remoteRef} is at ${state.remoteSha},`,
    `which is not in the release commit history at ${state.head}.`,
    'Something pushed to the branch after this release started.',
    'Nothing was published: the atomic ref push is the release transaction boundary,',
    'so this attempt left no tags on the remote and no versions on npm.',
    `Re-run the release workflow to release from ${state.remoteRef}.`,
  ].join(' ');
}

export function releasePackageForTag<Package extends Omit<ReleasePackageInfo, 'version'>>(
  packages: Package[],
  tag: string,
): { pkg: Package; version: string } | null {
  for (const pkg of packages) {
    const prefix = `${pkg.projectName}@`;
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

/** Latest ancestor tags a repair considers per package: the newest release and the one before it. */
export const REPAIR_CANDIDATES_PER_PACKAGE = 2;

export async function collectOwnedReleaseTagRecords<Package extends Omit<ReleasePackageInfo, 'version'>>(
  packages: Package[],
  ref: string,
  shell: ReleasePlanningShell,
): Promise<Array<ReleaseTagRecord<Package & { version: string }>>> {
  const candidates: Array<{
    tag: GitReleaseTagInfo;
    pkg: Package & { version: string };
  }> = [];
  const durableStates = new BoundedPromiseRunner(8);
  const candidatesPerPackage = new Map<string, number>();

  for (const tag of await shell.listReleaseTagsByCreatorDate()) {
    const match = releasePackageForTag(packages, tag.name);
    if (!match) {
      continue;
    }
    // Tags arrive newest first, so a package's first two ancestor tags are its two
    // latest releases. A repair only ever concerns the latest incomplete release
    // and the one before it (an older tag's state is settled by the newer one
    // proving complete), so anything past that is a git call per tag for nothing.
    if ((candidatesPerPackage.get(match.pkg.name) ?? 0) >= REPAIR_CANDIDATES_PER_PACKAGE) {
      continue;
    }
    if (!(await shell.isAncestor(tag.sha, ref))) {
      continue;
    }
    candidatesPerPackage.set(match.pkg.name, (candidatesPerPackage.get(match.pkg.name) ?? 0) + 1);
    const versionAtTag = await shell.packageVersionAtRef(match.pkg.path, tag.sha);
    if (versionAtTag !== match.version) {
      throw new Error(
        `Release tag ${tag.name} points at ${tag.sha.slice(0, 12)}, but ${match.pkg.path}/package.json has version ${
          versionAtTag ?? 'missing'
        }.`,
      );
    }
    const pkg = { ...match.pkg, version: match.version };
    candidates.push({ tag, pkg });
  }

  const records: Array<ReleaseTagRecord<Package & { version: string }>> = [];
  const completedPackages = new Set<string>();
  const activePackages = new Set<string>();
  const states = new Map<number, Promise<DurableTagState>>();

  // Run durable-state checks concurrently across packages, but do not query an
  // older tag for the same package until its newer tag proves incomplete.
  const scheduleFrom = (startIndex: number) => {
    for (let index = startIndex; index < candidates.length; index += 1) {
      if (states.size >= 8) {
        return;
      }
      const candidate = candidates[index];
      if (!candidate || states.has(index) || completedPackages.has(candidate.pkg.name)) {
        continue;
      }
      if (activePackages.has(candidate.pkg.name)) {
        continue;
      }
      activePackages.add(candidate.pkg.name);
      states.set(
        index,
        durableStates.run(() => shell.durableTagState(candidate.pkg, candidate.tag.name)),
      );
    }
  };

  for (const candidate of candidates) {
    const index = candidates.indexOf(candidate);
    scheduleFrom(index);
    if (completedPackages.has(candidate.pkg.name)) {
      continue;
    }
    const state = await states.get(index);
    if (!state) {
      throw new Error(`Release tag ${candidate.tag.name} lost durable state planning.`);
    }
    activePackages.delete(candidate.pkg.name);
    states.delete(index);
    records.push(
      classifyReleaseTag(
        { tag: candidate.tag.name, sha: candidate.tag.sha, timestamp: candidate.tag.timestamp, pkg: candidate.pkg },
        state,
      ),
    );
    if (state.npmPublished && state.githubReleaseExists) {
      completedPackages.add(candidate.pkg.name);
    }
  }
  return records;
}

class BoundedPromiseRunner {
  private active = 0;
  private readonly waiting: Array<() => void> = [];

  constructor(private readonly limit: number) {}

  async run<T>(operation: () => Promise<T>): Promise<T> {
    await this.acquire();
    try {
      return await operation();
    } finally {
      this.release();
    }
  }

  private async acquire(): Promise<void> {
    if (this.active < this.limit) {
      this.active += 1;
      return;
    }
    await new Promise<void>((resolve) => this.waiting.push(resolve));
    this.active += 1;
  }

  private release(): void {
    this.active -= 1;
    this.waiting.shift()?.();
  }
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

/**
 * Compile an Nx-style glob: a single `*` stops at a path separator, a doubled one crosses them,
 * and a doubled star followed by a separator also matches zero segments, so `a/` + `**` + `/b`
 * covers plain `a/b`. Shared by the two things release selection matches — changed file paths
 * against build-input patterns, and `dependsOn` entries against target names.
 */
export function globToRegExp(pattern: string): RegExp {
  let source = '^';
  for (let index = 0; index < pattern.length; index += 1) {
    const char = pattern[index];
    if (char === '*') {
      if (pattern[index + 1] === '*') {
        if (pattern[index + 2] === '/') {
          source += '(?:.*/)?';
          index += 2;
        } else {
          source += '.*';
          index += 1;
        }
      } else {
        source += '[^/]*';
      }
    } else {
      source += escapeRegExpChar(char);
    }
  }
  return new RegExp(`${source}$`);
}

function escapeRegExpChar(char: string | undefined): string {
  if (!char) {
    return '';
  }
  return /[\\^$+?.()|[\]{}]/.test(char) ? `\\${char}` : char;
}
