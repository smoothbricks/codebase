import { appendFile, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createInterface } from 'node:readline/promises';
import { Writable } from 'node:stream';
import { $ } from 'bun';
import { withDirenvEnv } from '../lib/direnv.js';
import { isRecord, stringProperty } from '../lib/json.js';
import { decode, run, runStatus } from '../lib/run.js';
import { listReleasePackages, readPackageJson, repositoryInfo } from '../lib/workspace.js';
import { readPackedPackageJson, validatePackedWorkspaceDependencies } from '../monorepo/packed-manifest.js';

export interface ReleaseVersionOptions {
  bump: string;
  dryRun?: boolean;
  githubOutput?: string;
}

export interface ReleasePublishOptions {
  bump: string;
  tag?: string;
  npmTag?: string;
  dryRun?: boolean;
}

export interface ReleaseRepairPendingOptions {
  tag?: string;
  npmTag?: string;
  dryRun?: boolean;
}

type ReleaseVersionMode = 'new' | 'none';

export interface ReleaseTrustPublisherOptions {
  dryRun?: boolean;
  otp?: string;
  skipLogin?: boolean;
}

export async function releaseVersion(root: string, options: ReleaseVersionOptions): Promise<void> {
  const bump = releaseBumpArg(options.bump);
  const packages = releasePackages(root);
  const projects = releasePackageProjects(packages);

  if (!options.dryRun) {
    const localRelease = await releasePackagesAtHead(root, packages);
    if (localRelease.length > 0 && !(await releaseIsComplete(root, localRelease))) {
      throw new Error('HEAD is already an incomplete release target. Run smoo release repair-pending first.');
    }
  }

  const headBeforeVersioning = await gitHead(root);

  // Nx owns local release mutation: package versions, bun.lock updates, the
  // release commit, and annotated tags. smoo owns remote publication after the
  // workflow validates the exact release commit Nx produced.
  const nxArgs = ['release', 'version'];
  if (bump !== 'auto') {
    nxArgs.push(bump);
  }
  nxArgs.push(`--projects=${projects}`, '--git-commit=true', '--git-tag=true', '--git-push=false');
  if (options.dryRun) {
    nxArgs.push('--dry-run');
  }
  await run('nx', nxArgs, root);
  if (options.dryRun) {
    await writeReleaseGithubOutput(options.githubOutput, [], 'none');
    return;
  }

  // Guard against future Nx/config regressions that leave release files, such
  // as bun.lock, outside the release commit after tagging/pushing.
  await assertCleanGitTree(root);
  const headAfterVersioning = await gitHead(root);
  const releasedPackages = await releasePackagesAtHead(root, releasePackages(root));
  await ensureLocalReleaseTags(root, releasedPackages);
  if (headAfterVersioning === headBeforeVersioning) {
    if (releasedPackages.length > 0 && !(await releaseIsComplete(root, releasedPackages))) {
      throw new Error('HEAD is already an incomplete release target. Run smoo release repair-pending first.');
    }
    if (bump !== 'auto') {
      throw new Error(`Nx did not create a release commit for forced --bump ${bump}.`);
    }
    console.log('Nx did not create a release commit; no release needed.');
    await writeReleaseGithubOutput(options.githubOutput, [], 'none');
    return;
  }
  if (releasedPackages.length === 0) {
    throw new Error('Nx created a release commit without current package release tags at HEAD.');
  }
  await writeReleaseGithubOutput(options.githubOutput, releasedPackages, 'new');
}

export async function releasePublish(root: string, options: ReleasePublishOptions): Promise<void> {
  const bump = releaseBumpArg(options.bump);
  const tag = releaseNpmTagArg(options);
  const packages = await releasePackagesAtHead(root, releasePackages(root));
  if (packages.length === 0) {
    if (bump === 'auto') {
      console.log('No release tags found at HEAD; no release to publish.');
      const summary: ReleaseSummary = {
        sha: await gitHead(root),
        dryRun: options.dryRun === true,
        packages,
        pushed: false,
        published: [],
        alreadyPublished: [],
        githubReleases: [],
        rerunRequired: false,
        noRelease: true,
      };
      await writeReleaseSummary(summary);
      return;
    }
    throw new Error('No release tags found at HEAD for current package versions. Run smoo release version first.');
  }
  const summary = await completeReleaseAtHead(
    root,
    packages,
    tag,
    options.dryRun === true,
    await newerCommitsRemain(root),
  );
  await writeReleaseSummary(summary);
}

export async function releaseRepairPending(root: string, options: ReleaseRepairPendingOptions): Promise<void> {
  const branch = await releaseBranch(root);
  const remote = await releaseRemote(root, branch);
  await fetchReleaseRefs(root, remote, branch);
  const remoteRef = `${remote}/${branch}`;
  const restoreRef = (await gitRefExists(root, remoteRef)) ? remoteRef : await gitHead(root);
  const candidates = await listIncompleteReleaseCommits(root, restoreRef);
  const summaries: ReleaseSummary[] = [];
  try {
    for (const sha of candidates) {
      await run('git', ['switch', '--detach', sha], root);
      const packages = await releasePackagesAtHead(root, releasePackages(root));
      if (packages.length === 0) {
        throw new Error(`Release commit ${sha} has no release packages after checkout.`);
      }
      console.log(`::group::Repair pending release ${sha.slice(0, 12)} (${packageSummary(packages)})`);
      try {
        await withDirenvEnv(root, async () => {
          await buildReleaseCandidate(root, packages);
          const tag = releaseNpmTagForRepair(options, packages);
          summaries.push(await completeReleaseAtHead(root, packages, tag, options.dryRun === true, false));
        });
      } finally {
        console.log('::endgroup::');
      }
    }
  } finally {
    await run('git', ['switch', '--detach', restoreRef], root);
  }
  await writeRepairSummary(summaries, options.dryRun === true);
}

export async function releaseTrustPublisher(root: string, options: ReleaseTrustPublisherOptions): Promise<void> {
  const repository = githubRepositoryFromRootPackage(root);
  const workflow = 'publish.yml';
  const packages = listReleasePackages(root);
  if (packages.length === 0) {
    throw new Error('No owned release packages found.');
  }

  if (!options.dryRun) {
    const packageStates = await Promise.all(
      packages.map(async (pkg) => ((await npmPackageExists(root, pkg.name)) ? null : pkg.name)),
    );
    const missingPackages = packageStates.filter((name): name is string => name !== null);
    if (missingPackages.length > 0) {
      throw new Error(
        'npm trusted publishing can only be configured after packages exist on the registry. ' +
          'Bootstrap the first publish with a temporary NPM_TOKEN, then rerun trust-publisher. ' +
          `Missing packages: ${missingPackages.join(', ')}`,
      );
    }
  }

  if (!options.dryRun && !options.skipLogin) {
    await runLatestNpm(root, ['login', '--auth-type=web']);
  }

  for (const pkg of packages) {
    console.log(`${pkg.name}: trusting GitHub Actions ${repository}/${workflow}`);
    const args = ['trust', 'github', pkg.name, '--file', workflow, '--repo', repository, '--yes'];
    if (options.dryRun) {
      args.push('--dry-run');
    }
    const otp = options.dryRun ? undefined : (options.otp ?? (await promptForNpmOtp(pkg.name)));
    await runLatestNpm(root, args, otp ? { NPM_CONFIG_OTP: otp } : undefined);
  }
}

export async function printReleaseState(root: string): Promise<void> {
  console.log(JSON.stringify(await getReleaseState(root), null, 2));
}

interface ReleaseState {
  packages: { name: string; version: string; published: boolean }[];
  allPublished: boolean;
}

type ReleasePackage = ReturnType<typeof listReleasePackages>[number];

interface ReleaseSummary {
  sha: string;
  dryRun: boolean;
  packages: ReleasePackage[];
  pushed: boolean;
  published: ReleasePackage[];
  alreadyPublished: ReleasePackage[];
  githubReleases: ReleasePackage[];
  rerunRequired: boolean;
  noRelease: boolean;
}

interface PublishReleasePackagesResult {
  published: ReleasePackage[];
  alreadyPublished: ReleasePackage[];
}

async function getReleaseState(root: string): Promise<ReleaseState> {
  return getReleaseStateForPackages(root, listReleasePackages(root));
}

async function getReleaseStateForPackages(root: string, packages: ReleasePackage[]): Promise<ReleaseState> {
  const states = await Promise.all(
    packages.map(async (pkg) => {
      const published = await npmVersionExists(root, pkg.name, pkg.version);
      return { name: pkg.name, version: pkg.version, published };
    }),
  );
  return { packages: states, allPublished: states.every((state) => state.published) };
}

async function listUnpublishedPackages(root: string, packages: ReleasePackage[]): Promise<ReleasePackage[]> {
  const states = await Promise.all(
    packages.map(async (pkg) => ({ pkg, published: await npmVersionExists(root, pkg.name, pkg.version) })),
  );
  return states.filter((state) => !state.published).map((state) => state.pkg);
}

async function publishPackedPackage(
  root: string,
  pkg: ReleasePackage,
  tag: string,
  dryRun: boolean,
  useBootstrapToken: boolean,
): Promise<void> {
  const tempDir = await mkdtemp(join(tmpdir(), 'smoo-publish-'));
  const tarball = join(tempDir, `${safeTarballPrefix(pkg.name)}-${pkg.version}.tgz`);
  try {
    console.log(`${pkg.name}@${pkg.version}: packing with bun pm pack`);
    await run('bun', ['pm', 'pack', '--filename', tarball, '--ignore-scripts', '--quiet'], join(root, pkg.path));
    await assertPackedWorkspaceDependencies(root, tarball, pkg);
    // npm CLI owns authentication here: trusted publishing OIDC when configured,
    // or the temporary NODE_AUTH_TOKEN bootstrap path below before trust exists.
    // Bun still produces the tarball so workspace:* dependencies are resolved the
    // same way smoo validates packed packages before release.
    const args = ['publish', tarball, '--access', 'public', '--tag', tag, '--provenance'];
    if (dryRun) {
      args.push('--dry-run');
    }
    try {
      await runLatestNpmPublish(root, args, useBootstrapToken);
    } catch (error) {
      if (await npmVersionExists(root, pkg.name, pkg.version)) {
        console.log(`${pkg.name}@${pkg.version}: publish result already visible on npm; continuing.`);
        return;
      }
      throw error;
    }
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
}

function safeTarballPrefix(name: string): string {
  return name.replace(/^@/, '').replace(/[^a-zA-Z0-9._-]+/g, '-');
}

async function assertPackedWorkspaceDependencies(root: string, tarball: string, pkg: ReleasePackage): Promise<void> {
  const manifest = await readPackedPackageJson(root, tarball, pkg.name);
  const failures = validatePackedWorkspaceDependencies(root, pkg, manifest);
  if (failures.length > 0) {
    throw new Error(failures.join('\n'));
  }
}

function releasePackages(root: string): ReleasePackage[] {
  const packages = listReleasePackages(root);
  if (packages.length === 0) {
    throw new Error('No owned release packages found.');
  }
  return packages;
}

function releasePackageProjects(packages: ReleasePackage[]): string {
  return packages.map((pkg) => pkg.name).join(',');
}

async function releasePackagesAtHead(root: string, packages: ReleasePackage[]): Promise<ReleasePackage[]> {
  return releasePackagesAtRef(root, packages, 'HEAD');
}

function releaseTag(pkg: ReleasePackage): string {
  return `${pkg.name}@${pkg.version}`;
}

async function writeReleaseGithubOutput(
  outputPath: string | undefined,
  packages: ReleasePackage[],
  mode: ReleaseVersionMode,
): Promise<void> {
  if (!outputPath) {
    return;
  }
  await appendFile(outputPath, `mode=${mode}\nprojects=${releasePackageProjects(packages)}\n`);
}

async function releasePackagesAtRef(root: string, packages: ReleasePackage[], ref: string): Promise<ReleasePackage[]> {
  const packagesAtRef = await Promise.all(packages.map(async (pkg) => releasePackageAtRef(root, pkg, ref)));
  const presentPackages = packagesAtRef.filter((pkg): pkg is ReleasePackage => pkg !== null);
  const tags = new Set(await gitTagsAtRef(root, ref));
  const taggedPackages = presentPackages.filter((pkg) => tags.has(releaseTag(pkg)));
  if (taggedPackages.length > 0) {
    return taggedPackages;
  }
  if (!(await isNxReleaseCommit(root, ref))) {
    return [];
  }
  return releasePackagesChangedAtRef(root, presentPackages, ref);
}

async function releasePackageAtRef(root: string, pkg: ReleasePackage, ref: string): Promise<ReleasePackage | null> {
  const version = await packageVersionAtRef(root, pkg.path, ref);
  return version ? { ...pkg, version } : null;
}

async function packageVersionAtRef(root: string, packagePath: string, ref: string): Promise<string | null> {
  const result = await $`git show ${`${ref}:${packagePath}/package.json`}`.cwd(root).quiet().nothrow();
  if (result.exitCode !== 0) {
    return null;
  }
  try {
    const parsed = JSON.parse(decode(result.stdout));
    return isRecord(parsed) ? stringProperty(parsed, 'version') : null;
  } catch {
    return null;
  }
}

async function isNxReleaseCommit(root: string, ref: string): Promise<boolean> {
  const result = await $`git show -s --format=%s ${ref}`.cwd(root).quiet().nothrow();
  return result.exitCode === 0 && decode(result.stdout).trim() === 'chore(release): publish';
}

async function releasePackagesChangedAtRef(
  root: string,
  packages: ReleasePackage[],
  ref: string,
): Promise<ReleasePackage[]> {
  if (!(await gitRefExists(root, `${ref}^`))) {
    return packages;
  }
  const changed: ReleasePackage[] = [];
  for (const pkg of packages) {
    const previousVersion = await packageVersionAtRef(root, pkg.path, `${ref}^`);
    if (previousVersion !== pkg.version) {
      changed.push(pkg);
    }
  }
  return changed;
}

async function gitTagsAtRef(root: string, ref: string): Promise<string[]> {
  const result = await $`git tag --points-at ${ref}`.cwd(root).quiet().nothrow();
  if (result.exitCode !== 0) {
    return [];
  }
  return decode(result.stdout)
    .split('\n')
    .map((tag) => tag.trim())
    .filter(Boolean);
}

async function previousReleaseTag(root: string, pkg: ReleasePackage, currentTag: string): Promise<string | null> {
  const result = await $`git tag --list ${`${pkg.name}@*`} --sort=-v:refname --merged ${currentTag}`
    .cwd(root)
    .quiet()
    .nothrow();
  if (result.exitCode !== 0) {
    throw new Error(`Unable to list release tags for ${pkg.name}.`);
  }
  return (
    decode(result.stdout)
      .split('\n')
      .map((tag) => tag.trim())
      .find((tag) => tag && tag !== currentTag) ?? null
  );
}

async function releaseIsComplete(root: string, packages: ReleasePackage[], ref = 'HEAD'): Promise<boolean> {
  const sha = await gitSha(root, ref);
  const branch = await releaseBranch(root);
  const remote = await releaseRemote(root, branch);
  await fetchReleaseRefs(root, remote, branch);
  const remoteRef = `${remote}/${branch}`;
  if (!(await gitRefExists(root, remoteRef)) || !(await gitIsAncestor(root, sha, remoteRef))) {
    return false;
  }
  for (const pkg of packages) {
    if (!(await remoteReleaseTagExists(root, remote, releaseTag(pkg)))) {
      return false;
    }
    if (!(await npmVersionExists(root, pkg.name, pkg.version))) {
      return false;
    }
    if (!(await githubReleaseExists(root, releaseTag(pkg)))) {
      return false;
    }
  }
  return true;
}

async function ensureLocalReleaseTags(root: string, packages: ReleasePackage[]): Promise<void> {
  for (const pkg of packages) {
    const tag = releaseTag(pkg);
    if (await gitRefExists(root, `refs/tags/${tag}`)) {
      if (!(await gitTagPointsAt(root, tag, 'HEAD'))) {
        throw new Error(`Release tag ${tag} exists but does not point at HEAD.`);
      }
      continue;
    }
    await run('git', ['tag', '-a', tag, '-m', tag, 'HEAD'], root);
  }
}

async function pushReleaseRefs(root: string, packages: ReleasePackage[]): Promise<boolean> {
  const branch = await releaseBranch(root);
  const remote = await releaseRemote(root, branch);
  await fetchReleaseRefs(root, remote, branch);
  await ensureLocalReleaseTags(root, packages);
  const refspecs: string[] = [];
  const remoteRef = `${remote}/${branch}`;
  const head = await gitHead(root);
  if (!(await gitRefExists(root, remoteRef)) || !(await gitIsAncestor(root, head, remoteRef))) {
    refspecs.push(`HEAD:refs/heads/${branch}`);
  }
  for (const pkg of packages) {
    const tag = releaseTag(pkg);
    if (!(await remoteReleaseTagExists(root, remote, tag))) {
      refspecs.push(`refs/tags/${tag}:refs/tags/${tag}`);
    }
  }
  if (refspecs.length === 0) {
    console.log('Release branch and tags are already present on the remote.');
    return false;
  }
  await run('git', ['push', '--atomic', remote, ...refspecs], root);
  return true;
}

async function completeReleaseAtHead(
  root: string,
  packages: ReleasePackage[],
  tag: string,
  dryRun: boolean,
  rerunRequired: boolean,
): Promise<ReleaseSummary> {
  const summary: ReleaseSummary = {
    sha: await gitHead(root),
    dryRun,
    packages,
    pushed: false,
    published: [],
    alreadyPublished: [],
    githubReleases: [],
    rerunRequired,
    noRelease: false,
  };
  if (!dryRun) {
    summary.pushed = await pushReleaseRefs(root, packages);
  }
  const publishResult = await publishReleasePackages(root, packages, tag, dryRun);
  summary.published = publishResult.published;
  summary.alreadyPublished = publishResult.alreadyPublished;
  summary.githubReleases = await createGithubReleases(root, packages, dryRun);
  return summary;
}

async function listIncompleteReleaseCommits(root: string, ref: string): Promise<string[]> {
  const candidates = await firstParentCommits(root, ref);
  const packages = releasePackages(root);
  const incomplete: string[] = [];
  for (const sha of candidates) {
    const releasePackagesForCommit = await releasePackagesAtRef(root, packages, sha);
    if (releasePackagesForCommit.length === 0) {
      continue;
    }
    if (!(await releaseIsComplete(root, releasePackagesForCommit, sha))) {
      incomplete.push(sha);
    }
  }
  return incomplete;
}

async function buildReleaseCandidate(root: string, packages: ReleasePackage[]): Promise<void> {
  await run('nx', ['run-many', '-t', 'build', `--projects=${releasePackageProjects(packages)}`], root);
}

function releaseNpmTagForRepair(options: ReleaseRepairPendingOptions, packages: ReleasePackage[]): string {
  const derivedTags = new Set(packages.map((pkg) => (pkg.version.includes('-') ? 'next' : 'latest')));
  if (derivedTags.size !== 1) {
    throw new Error(`Pending release mixes stable and prerelease package versions: ${packageSummary(packages)}`);
  }
  const derivedTag = [...derivedTags][0];
  if (!derivedTag) {
    throw new Error('Unable to derive npm dist-tag for pending release.');
  }
  const explicitTag = options.tag ?? options.npmTag;
  if (explicitTag && explicitTag !== derivedTag) {
    throw new Error(`Pending release publishes with npm dist-tag ${derivedTag}, not ${explicitTag}.`);
  }
  return explicitTag ?? derivedTag;
}

async function publishReleasePackages(
  root: string,
  packages: ReleasePackage[],
  tag: string,
  dryRun: boolean,
): Promise<PublishReleasePackagesResult> {
  const unpublishedPackages = await listUnpublishedPackages(root, packages);
  const alreadyPublished = packages.filter((pkg) => !unpublishedPackages.includes(pkg));
  for (const pkg of unpublishedPackages) {
    const packageExists = dryRun ? true : await npmPackageExists(root, pkg.name);
    await publishPackedPackage(root, pkg, tag, dryRun, !dryRun && !packageExists);
  }
  return { published: dryRun ? [] : unpublishedPackages, alreadyPublished };
}

async function createGithubReleases(
  root: string,
  packages: ReleasePackage[],
  dryRun: boolean,
): Promise<ReleasePackage[]> {
  for (const pkg of packages) {
    await createGithubRelease(root, pkg, dryRun);
  }
  return dryRun ? [] : packages;
}

async function createGithubRelease(root: string, pkg: ReleasePackage, dryRun: boolean): Promise<void> {
  const currentTag = releaseTag(pkg);
  if (!dryRun) {
    await assertRemoteTagExists(root, currentTag);
  }
  const previousTag = await previousReleaseTag(root, pkg, currentTag);
  const args = [
    'release',
    'changelog',
    pkg.version,
    `--projects=${pkg.name}`,
    '--git-commit=false',
    '--git-tag=false',
    '--git-push=false',
    '--stage-changes=false',
  ];
  if (previousTag) {
    args.push(`--from=${previousTag}`);
  } else {
    args.push('--first-release');
  }
  if (dryRun) {
    args.push('--dry-run');
  }
  await run('nx', args, root);
}

async function newerCommitsRemain(root: string): Promise<boolean> {
  const branch = await releaseBranch(root);
  const remote = await releaseRemote(root, branch);
  await fetchReleaseRefs(root, remote, branch);
  const remoteRef = `${remote}/${branch}`;
  if (!(await gitRefExists(root, remoteRef))) {
    return false;
  }
  const head = await gitHead(root);
  return head !== (await gitSha(root, remoteRef)) && (await gitIsAncestor(root, head, remoteRef));
}

async function writeReleaseSummary(summary: ReleaseSummary): Promise<void> {
  const shortSha = summary.sha.slice(0, 12);
  const lines = [
    '## Release Summary',
    '',
    `- Commit: \`${shortSha}\``,
    `- Mode: ${summary.dryRun ? 'dry run' : 'release'}`,
  ];
  if (summary.noRelease) {
    lines.push('- Result: no release tags found at HEAD');
  } else {
    lines.push(`- Packages: ${packageSummary(summary.packages)}`);
    lines.push(`- Git refs pushed: ${summary.pushed ? 'yes' : 'already current'}`);
    lines.push(`- npm published: ${packageSummary(summary.published)}`);
    lines.push(`- npm already published: ${packageSummary(summary.alreadyPublished)}`);
    lines.push(`- GitHub Releases created/updated: ${packageSummary(summary.githubReleases)}`);
  }
  if (summary.rerunRequired) {
    const message = 'A previous incomplete release was repaired; newer commits remain. Run Publish again.';
    console.log(`::warning::${message}`);
    lines.push(`- Warning: ${message}`);
  }
  const text = `${lines.join('\n')}\n`;
  console.log(text.trimEnd());
  const summaryPath = process.env.GITHUB_STEP_SUMMARY;
  if (summaryPath) {
    await appendFile(summaryPath, `${text}\n`);
  }
}

async function writeRepairSummary(summaries: ReleaseSummary[], dryRun: boolean): Promise<void> {
  const lines = ['## Pending Release Repair', '', `- Mode: ${dryRun ? 'dry run' : 'release'}`];
  if (summaries.length === 0) {
    lines.push('- Result: no pending releases needed repair');
  } else {
    for (const summary of summaries) {
      lines.push(`- Repaired \`${summary.sha.slice(0, 12)}\`: ${packageSummary(summary.packages)}`);
    }
  }
  const text = `${lines.join('\n')}\n`;
  console.log(text.trimEnd());
  const summaryPath = process.env.GITHUB_STEP_SUMMARY;
  if (summaryPath) {
    await appendFile(summaryPath, `${text}\n`);
  }
}

function packageSummary(packages: ReleasePackage[]): string {
  if (packages.length === 0) {
    return 'none';
  }
  return packages.map((pkg) => `${pkg.name}@${pkg.version}`).join(', ');
}

async function fetchReleaseRefs(root: string, remote: string, branch: string): Promise<void> {
  await run('git', ['fetch', '--tags', remote, branch], root);
}

async function firstParentCommits(root: string, ref: string): Promise<string[]> {
  const result = await $`git rev-list --first-parent --reverse ${ref}`.cwd(root).quiet().nothrow();
  if (result.exitCode !== 0) {
    throw new Error(`Unable to list first-parent release history for ${ref}.`);
  }
  return decode(result.stdout)
    .split('\n')
    .map((sha) => sha.trim())
    .filter(Boolean);
}

async function gitRefExists(root: string, ref: string): Promise<boolean> {
  const result = await $`git rev-parse --verify ${ref}`.cwd(root).quiet().nothrow();
  return result.exitCode === 0;
}

async function gitIsAncestor(root: string, ancestor: string, descendant: string): Promise<boolean> {
  const result = await $`git merge-base --is-ancestor ${ancestor} ${descendant}`.cwd(root).quiet().nothrow();
  return result.exitCode === 0;
}

async function gitSha(root: string, ref: string): Promise<string> {
  const result = await $`git rev-parse ${ref}`.cwd(root).quiet().nothrow();
  if (result.exitCode !== 0) {
    throw new Error(`Unable to resolve git ref ${ref}.`);
  }
  return decode(result.stdout).trim();
}

async function gitTagPointsAt(root: string, tag: string, ref: string): Promise<boolean> {
  const result = await $`git rev-list -n 1 ${tag}`.cwd(root).quiet().nothrow();
  return result.exitCode === 0 && decode(result.stdout).trim() === (await gitSha(root, ref));
}

async function remoteReleaseTagExists(root: string, remote: string, tag: string): Promise<boolean> {
  const result = await $`git ls-remote --exit-code --tags ${remote} ${`refs/tags/${tag}`}`.cwd(root).quiet().nothrow();
  return result.exitCode === 0;
}

async function githubReleaseExists(root: string, tag: string): Promise<boolean> {
  return (await runStatus('gh', ['release', 'view', tag, '--json', 'tagName'], root, true)) === 0;
}

async function assertRemoteTagExists(root: string, tag: string): Promise<void> {
  const branch = await releaseBranch(root);
  const remote = await releaseRemote(root, branch);
  if (!(await remoteReleaseTagExists(root, remote, tag))) {
    throw new Error(`Release tag ${tag} is not present on remote ${remote}. Run smoo release version first.`);
  }
}

async function npmVersionExists(root: string, name: string, version: string): Promise<boolean> {
  const result = await $`bun pm view ${`${name}@${version}`} version`.cwd(root).quiet().nothrow();
  return result.exitCode === 0 && decode(result.stdout).trim() === version;
}

async function npmPackageExists(root: string, name: string): Promise<boolean> {
  const result = await $`bun pm view ${name} name`.cwd(root).quiet().nothrow();
  return result.exitCode === 0;
}

async function assertCleanGitTree(root: string): Promise<void> {
  const result = await $`git status --porcelain --untracked-files=no`.cwd(root).quiet().nothrow();
  if (result.exitCode !== 0) {
    throw new Error('Unable to inspect git status after release versioning.');
  }
  const status = decode(result.stdout).trim();
  if (status) {
    throw new Error(`nx release version left tracked changes after tagging:\n${status}`);
  }
}

async function gitHead(root: string): Promise<string> {
  const result = await $`git rev-parse HEAD`.cwd(root).quiet().nothrow();
  if (result.exitCode !== 0) {
    throw new Error('Unable to resolve git HEAD before release versioning.');
  }
  return decode(result.stdout).trim();
}

async function releaseBranch(root: string): Promise<string> {
  const githubBranch = process.env.GITHUB_REF_NAME?.trim();
  if (githubBranch) {
    return githubBranch;
  }
  const result = await $`git branch --show-current`.cwd(root).quiet().nothrow();
  const branch = decode(result.stdout).trim();
  if (result.exitCode === 0 && branch) {
    return branch;
  }
  throw new Error('Unable to resolve current git branch for release push.');
}

async function releaseRemote(root: string, branch: string): Promise<string> {
  const configured = await gitConfigValue(root, `branch.${branch}.remote`);
  if (configured) {
    return configured;
  }
  if (await gitRemoteExists(root, 'origin')) {
    return 'origin';
  }
  const result = await $`git remote`.cwd(root).quiet().nothrow();
  if (result.exitCode !== 0) {
    throw new Error('Unable to list git remotes for release push.');
  }
  const remotes = decode(result.stdout)
    .split('\n')
    .map((remote) => remote.trim())
    .filter(Boolean);
  if (remotes.length === 1 && remotes[0]) {
    return remotes[0];
  }
  throw new Error('Unable to choose git remote for release push. Configure branch upstream or add origin.');
}

async function gitConfigValue(root: string, key: string): Promise<string | null> {
  const result = await $`git config --get ${key}`.cwd(root).quiet().nothrow();
  const value = decode(result.stdout).trim();
  return result.exitCode === 0 && value ? value : null;
}

async function gitRemoteExists(root: string, remote: string): Promise<boolean> {
  const result = await $`git remote get-url ${remote}`.cwd(root).quiet().nothrow();
  return result.exitCode === 0;
}

function releaseBumpArg(bump = 'auto'): string {
  if (!['auto', 'patch', 'minor', 'major', 'prerelease'].includes(bump)) {
    throw new Error(`Invalid --bump "${bump}". Expected auto, patch, minor, major, or prerelease.`);
  }
  return bump;
}

function releaseNpmTagArg(options: ReleasePublishOptions): string {
  const bump = releaseBumpArg(options.bump);
  const derivedTag = bump === 'prerelease' ? 'next' : 'latest';
  const explicitTag = options.tag ?? options.npmTag;
  if (!explicitTag) {
    return derivedTag;
  }
  if (explicitTag !== derivedTag) {
    throw new Error(`--bump ${bump} publishes with npm dist-tag ${derivedTag}, not ${explicitTag}.`);
  }
  return explicitTag;
}

function githubRepositoryFromRootPackage(root: string): string {
  const pkg = readPackageJson(join(root, 'package.json'));
  const repository = pkg ? repositoryInfo(pkg.json) : null;
  if (!repository) {
    throw new Error('Root package.json must define repository.url before configuring npm trusted publishing.');
  }
  return githubRepositoryFromUrl(repository.url);
}

function githubRepositoryFromUrl(url: string): string {
  const normalized = url
    .replace(/^git\+/, '')
    .replace(/#.*$/, '')
    .replace(/\.git$/, '');
  const https = /^https:\/\/github\.com\/([^/]+)\/([^/]+)$/.exec(normalized);
  if (https?.[1] && https[2]) {
    return `${https[1]}/${https[2]}`;
  }
  const ssh = /^git@github\.com:([^/]+)\/([^/]+)$/.exec(normalized);
  if (ssh?.[1] && ssh[2]) {
    return `${ssh[1]}/${ssh[2]}`;
  }
  const shorthand = /^github:([^/]+)\/([^/]+)$/.exec(normalized);
  if (shorthand?.[1] && shorthand[2]) {
    return `${shorthand[1]}/${shorthand[2]}`;
  }
  throw new Error(`Root package.json repository.url must be a GitHub repository URL, got ${url}`);
}

async function runLatestNpm(root: string, npmArgs: string[], env?: Record<string, string>): Promise<void> {
  await run('nix', ['shell', 'nixpkgs#nodejs_latest', '-c', 'npm', ...npmArgs], root, env);
}

async function runLatestNpmPublish(root: string, npmArgs: string[], useBootstrapToken: boolean): Promise<void> {
  if (!useBootstrapToken) {
    await runLatestNpm(root, npmArgs, { NODE_AUTH_TOKEN: '', NPM_TOKEN: '' });
    return;
  }
  const token = process.env.NODE_AUTH_TOKEN || process.env.NPM_TOKEN;
  if (!token || process.env.NPM_CONFIG_USERCONFIG) {
    if (!token && !process.env.NPM_CONFIG_USERCONFIG) {
      throw new Error(
        'First publish for a package requires NODE_AUTH_TOKEN or NPM_TOKEN until trusted publishing exists.',
      );
    }
    await runLatestNpm(root, npmArgs);
    return;
  }

  const tempDir = await mkdtemp(join(tmpdir(), 'smoo-npm-auth-'));
  const npmrc = join(tempDir, '.npmrc');
  try {
    await writeFile(npmrc, `registry=https://registry.npmjs.org/\n//registry.npmjs.org/:_authToken=${token}\n`);
    await runLatestNpm(root, npmArgs, { NPM_CONFIG_USERCONFIG: npmrc });
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
}

async function promptForNpmOtp(packageName: string): Promise<string> {
  if (!process.stdin.isTTY || !process.stdout.isTTY) {
    throw new Error(
      `npm trust requires a one-time password for ${packageName}. Pass --otp <code> in non-interactive shells.`,
    );
  }

  const mutedOutput = new Writable({
    write(_chunk, _encoding, callback) {
      callback();
    },
  });
  const rl = createInterface({ input: process.stdin, output: mutedOutput, terminal: true });
  process.stdout.write(`Enter npm OTP for ${packageName}: `);
  try {
    const otp = (await rl.question('')).trim();
    process.stdout.write('\n');
    if (!otp) {
      throw new Error(`npm OTP is required for ${packageName}.`);
    }
    return otp;
  } finally {
    rl.close();
  }
}
