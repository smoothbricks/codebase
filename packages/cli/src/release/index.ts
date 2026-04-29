import { appendFile, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createInterface } from 'node:readline/promises';
import { Writable } from 'node:stream';
import { $ } from 'bun';
import { withDevenvEnv } from '../lib/devenv.js';
import { isRecord, readJsonObject, stringProperty } from '../lib/json.js';
import { decode, run, runInteractiveStatus, runResult, runStatus } from '../lib/run.js';
import { listReleasePackages, readPackageJson, repositoryInfo } from '../lib/workspace.js';
import { readPackedPackageJson, validatePackedWorkspaceDependencies } from '../monorepo/packed-manifest.js';
import {
  type BootstrapNpmPackagesOptions,
  bootstrapNpmPackages,
  NPM_BOOTSTRAP_DIST_TAG,
  NPM_BOOTSTRAP_VERSION,
} from './bootstrap-npm-packages.js';
import { autoReleaseCandidatePackages } from './candidates.js';
import {
  type ReleaseTagRecord as CoreReleaseTagRecord,
  type ReleaseTarget as CoreReleaseTarget,
  collectOwnedReleaseTagRecords,
  type GitReleaseTagInfo,
  pendingReleaseTargets,
  type ReleasePackageInfo,
  releaseTag,
} from './core.js';
import { createOrUpdateGithubRelease, renderNxProjectChangelogContents } from './github-release.js';
import { publishWithAuthDiagnostics } from './npm-auth.js';
import {
  completeReleaseAtHead as completeReleaseAtHeadWithShell,
  type ReleaseCompletionShell,
  type ReleaseSummary,
  type ReleaseVersionMode,
  repairPendingTargets,
  runReleaseVersion,
} from './orchestration.js';
import { type RetagUnpublishedTagUpdate, retagUnpublished } from './retag-unpublished.js';

export interface ReleaseVersionOptions {
  bump: string;
  dryRun?: boolean;
  githubOutput?: string;
}

export interface ReleasePublishOptions {
  bump: string;
  dryRun?: boolean;
}

export interface ReleaseRepairPendingOptions {
  dryRun?: boolean;
}

export interface ReleaseTrustPublisherOptions {
  dryRun?: boolean;
  bootstrap?: boolean;
  bootstrapOtp?: string;
  skipLogin?: boolean;
  packages?: string[];
}

export interface ReleaseBootstrapNpmPackagesOptions {
  dryRun?: boolean;
  skipLogin?: boolean;
  otp?: string;
  packages?: string[];
}

export interface ReleaseRetagUnpublishedOptions {
  tags: string[];
  to?: string;
  push?: boolean;
  dispatch?: boolean;
  dryRun?: boolean;
  remote?: string;
  branch?: string;
}

export async function releaseVersion(root: string, options: ReleaseVersionOptions): Promise<void> {
  const bump = releaseBumpArg(options.bump);
  const packages = releasePackages(root);
  const result = await runReleaseVersion(
    {
      releasePackagesAtHead: () => releasePackagesAtHead(root, packages),
      releaseVersionPackages: (releaseBump) => releaseVersionPackages(root, packages, releaseBump),
      ensureLocalReleaseTags: (releasePackages) => ensureLocalReleaseTags(root, releasePackages),
      gitHead: () => gitHead(root),
      runNxReleaseVersion: (releasePackages, releaseBump, dryRun) =>
        runNxReleaseVersion(root, releasePackageProjects(releasePackages), releaseBump, dryRun),
      assertCleanGitTree: () => assertCleanGitTree(root),
    },
    { bump, dryRun: options.dryRun === true },
  );
  if (result.status === 'already-release-target') {
    console.log('HEAD is already a release target; publish will complete any missing durable state.');
  } else if (result.status === 'no-release-needed') {
    console.log('Nx did not create a release commit; no release needed.');
  }
  await writeReleaseGithubOutput(options.githubOutput, result.packages, result.mode);
}

export async function releasePublish(root: string, options: ReleasePublishOptions): Promise<void> {
  const bump = releaseBumpArg(options.bump);
  const packages = await releasePackagesAtHead(root, releasePackages(root));
  if (packages.length === 0) {
    if (bump === 'auto') {
      console.log('No release tags found at HEAD; no release to publish.');
      const summary: ReleaseSummary<ReleasePackage> = {
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
  const summary = await completeReleaseAtHeadWithShell(
    releaseCompletionShell(root),
    packages,
    options.dryRun === true,
    await newerCommitsRemain(root),
  );
  await writeReleaseSummary(summary);
}

export async function releaseRepairPending(root: string, options: ReleaseRepairPendingOptions): Promise<void> {
  const branch = await releaseBranch(root);
  const remote = await releaseRemote(root, branch);
  console.log(`Repair pending releases: fetching ${remote}/${branch} and tags.`);
  await fetchReleaseRefs(root, remote, branch);
  const remoteRef = `${remote}/${branch}`;
  const restoreRef = (await gitRefExists(root, remoteRef)) ? remoteRef : await gitHead(root);
  console.log(`Repair pending releases: planning from ${restoreRef}.`);
  const targets = await listPendingReleaseTargets(root, restoreRef);
  console.log(
    targets.length === 0
      ? 'Repair pending releases: no pending durable state repairs found.'
      : `Repair pending releases: ${targets.length} release target${targets.length === 1 ? '' : 's'} need repair.`,
  );
  const summaries = await repairPendingTargets(releaseRepairShell(root), targets, restoreRef, options.dryRun === true);
  await writeRepairSummary(summaries, options.dryRun === true);
}

export async function releaseTrustPublisher(root: string, options: ReleaseTrustPublisherOptions): Promise<void> {
  const repository = githubRepositoryFromRootPackage(root);
  const workflow = 'publish.yml';
  await configureTrustedPublishers(
    {
      repository,
      workflow,
      listReleasePackages: () => listReleasePackages(root),
      packageExists: (name) => npmPackageExists(root, name),
      bootstrapNpmPackages: (bootstrapOptions) =>
        bootstrapNpmPackages(
          {
            listReleasePackages: () => listReleasePackages(root),
            packageExists: (name) => npmPackageExists(root, name),
            login: () => runLatestNpm(root, ['login', '--auth-type=web']),
            publishPlaceholder: (pkg, env) => publishPlaceholderPackage(root, pkg, env),
            promptOtp: (packageName) => promptForNpmOtp(packageName),
            log: (message) => console.log(message),
          },
          bootstrapOptions,
        ),
      trustPublisher: (pkg, dryRun, env) => {
        const args = ['trust', 'github', pkg.name, '--file', workflow, '--repo', repository, '--yes'];
        if (dryRun) {
          args.push('--dry-run');
        }
        return runLatestNpmTrust(root, args, env);
      },
      trustedPublishers: (pkg) => listTrustedPublishers(root, pkg),
      log: (message) => console.log(message),
      error: (message) => console.error(message),
    },
    options,
  );
}

export interface TrustPublisherShell<Package extends ReleasePackageInfo = ReleasePackageInfo> {
  repository: string;
  workflow: string;
  listReleasePackages(): Package[];
  packageExists(name: string): Promise<boolean>;
  bootstrapNpmPackages(options: BootstrapNpmPackagesOptions): Promise<Package[]>;
  trustPublisher(pkg: Package, dryRun: boolean, env?: Record<string, string>): Promise<TrustPublisherResult>;
  trustedPublishers(pkg: Package): Promise<TrustedPublisher[]>;
  log(message: string): void;
  error(message: string): void;
}

export type TrustPublisherResult = 'configured' | 'already-configured';

export interface TrustedPublisher {
  id: string;
  type: string;
  file?: string;
  repository?: string;
}

export async function configureTrustedPublishers<Package extends ReleasePackageInfo>(
  shell: TrustPublisherShell<Package>,
  options: ReleaseTrustPublisherOptions,
): Promise<void> {
  const packages = shell.listReleasePackages();
  const selectedPackages = selectedTrustPublisherPackages(packages, options.packages ?? []);
  if (selectedPackages.length === 0) {
    throw new Error('No owned release packages found.');
  }

  if (options.bootstrap) {
    await shell.bootstrapNpmPackages({
      dryRun: options.dryRun === true,
      skipLogin: options.skipLogin === true,
      otp: options.bootstrapOtp,
      packages: options.packages ?? [],
    });
  }

  if (!options.dryRun) {
    const packageStates = await Promise.all(
      selectedPackages.map(async (pkg) => ((await shell.packageExists(pkg.name)) ? null : pkg.name)),
    );
    const missingPackages = packageStates.filter((name): name is string => name !== null);
    if (missingPackages.length > 0) {
      throw new Error(
        'npm trusted publishing can only be configured after packages exist on the registry. ' +
          'Run smoo release trust-publisher --bootstrap locally. ' +
          `Missing packages: ${missingPackages.join(', ')}`,
      );
    }
  }

  const failedPackages: string[] = [];
  for (const pkg of selectedPackages) {
    if (!options.dryRun) {
      const trustedPublishers = await shell.trustedPublishers(pkg);
      if (hasMatchingGithubTrustedPublisher(trustedPublishers, shell.repository, shell.workflow)) {
        shell.log(`${pkg.name}: npm trusted publisher is already configured; skipping.`);
        continue;
      }
      if (trustedPublishers.length > 0) {
        throw new Error(
          `${pkg.name}: npm trusted publisher exists but does not match GitHub Actions ${shell.repository}/${shell.workflow}. ` +
            'Use npm trust list to inspect it and npm trust revoke --id <trust-id> before reconfiguring.',
        );
      }
    }

    shell.log(`${pkg.name}: trusting GitHub Actions ${shell.repository}/${shell.workflow}`);
    while (true) {
      try {
        const result = await shell.trustPublisher(pkg, options.dryRun === true);
        if (result === 'already-configured') {
          shell.log(`${pkg.name}: npm trusted publisher is already configured; skipping.`);
        }
        break;
      } catch (error) {
        shell.error(`${pkg.name}: npm trusted publisher setup failed.`);
        shell.error(error instanceof Error ? error.message : String(error));
        if (!options.dryRun) {
          shell.error('npm owns trusted publishing authentication. Complete the npm browser challenge, then retry.');
        }
        if (options.dryRun) {
          failedPackages.push(pkg.name);
          break;
        }
        shell.error('Retrying this package. Press Ctrl-C to stop.');
      }
    }
  }
  if (failedPackages.length > 0) {
    shell.error(`Trusted publishing was not configured for: ${failedPackages.join(', ')}`);
  }
}

function selectedTrustPublisherPackages<Package extends ReleasePackageInfo>(
  packages: Package[],
  selections: string[],
): Package[] {
  if (selections.length === 0) {
    return packages;
  }
  const byName = new Map(packages.map((pkg) => [pkg.name, pkg]));
  const selected: Package[] = [];
  const unknown: string[] = [];
  for (const name of selections) {
    const pkg = byName.get(name);
    if (pkg) {
      selected.push(pkg);
    } else {
      unknown.push(name);
    }
  }
  if (unknown.length > 0) {
    throw new Error(`Unknown owned release package selection: ${unknown.join(', ')}`);
  }
  return selected;
}

function hasMatchingGithubTrustedPublisher(
  trustedPublishers: TrustedPublisher[],
  repository: string,
  workflow: string,
): boolean {
  return trustedPublishers.some(
    (trustedPublisher) =>
      trustedPublisher.type === 'github' &&
      trustedPublisher.repository === repository &&
      trustedPublisher.file === workflow,
  );
}

export async function releaseBootstrapNpmPackages(
  root: string,
  options: ReleaseBootstrapNpmPackagesOptions,
): Promise<void> {
  await bootstrapNpmPackages(
    {
      listReleasePackages: () => listReleasePackages(root),
      packageExists: (name) => npmPackageExists(root, name),
      login: () => runLatestNpm(root, ['login', '--auth-type=web']),
      publishPlaceholder: (pkg, env) => publishPlaceholderPackage(root, pkg, env),
      promptOtp: (packageName) => promptForNpmOtp(packageName),
      log: (message) => console.log(message),
    },
    {
      dryRun: options.dryRun === true,
      skipLogin: options.skipLogin === true,
      otp: options.otp,
      packages: options.packages ?? [],
    },
  );
}

export async function releaseRetagUnpublished(root: string, options: ReleaseRetagUnpublishedOptions): Promise<void> {
  const toRef = options.to ?? 'HEAD';
  const dispatch = options.dispatch === true;
  const push = options.push === true || dispatch;
  const branch = options.branch ?? (await releaseBranch(root));
  const remote = options.remote ?? (push ? await releaseRemote(root, branch) : 'origin');
  await retagUnpublished(releaseRetagShell(root, remote), {
    tags: options.tags,
    toRef,
    push,
    dispatch,
    dryRun: options.dryRun === true,
    branch,
    workflow: 'publish.yml',
  });
}

export async function printReleaseState(root: string): Promise<void> {
  console.log(JSON.stringify(await getReleaseState(root), null, 2));
}

interface ReleaseState {
  packages: { name: string; version: string; published: boolean }[];
  allPublished: boolean;
}

type ReleasePackage = ReturnType<typeof listReleasePackages>[number];

type ReleaseTarget = CoreReleaseTarget<ReleasePackage>;
type ReleaseTagRecord = CoreReleaseTagRecord<ReleasePackage>;

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

async function publishPackedPackage(root: string, pkg: ReleasePackage, tag: string, dryRun: boolean): Promise<void> {
  const tempDir = await mkdtemp(join(tmpdir(), 'smoo-publish-'));
  const tarball = join(tempDir, `${safeTarballPrefix(pkg.name)}-${pkg.version}.tgz`);
  try {
    console.log(`${pkg.name}@${pkg.version}: packing with bun pm pack`);
    await run('bun', ['pm', 'pack', '--filename', tarball, '--ignore-scripts', '--quiet'], join(root, pkg.path));
    await assertPackedWorkspaceDependencies(root, tarball, pkg);
    // npm CLI owns authentication here: trusted publishing OIDC when configured.
    // Bun still produces the tarball so workspace:* dependencies are resolved the
    // same way smoo validates packed packages before release.
    const args = ['publish', tarball, '--access', 'public', '--tag', tag, '--provenance'];
    if (dryRun) {
      args.push('--dry-run');
    }
    await publishWithAuthDiagnostics(
      pkg,
      {
        publish: () => runLatestNpmPublish(root, args),
        versionExists: () => npmVersionExists(root, pkg.name, pkg.version),
        log: (message) => console.log(message),
        error: (message) => console.error(message),
        appendSummary: async (markdown) => {
          const summaryPath = process.env.GITHUB_STEP_SUMMARY;
          if (summaryPath) {
            await appendFile(summaryPath, `${markdown}\n\n`);
          }
        },
      },
      {
        tokenPresent: npmAuthTokenPresent(),
        repository: githubRepositoryFromRootPackage(root),
      },
    );
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
}

function npmAuthTokenPresent(): boolean {
  return Boolean(process.env.NODE_AUTH_TOKEN || process.env.NPM_TOKEN || process.env.NPM_CONFIG_USERCONFIG);
}

async function publishPlaceholderPackage(
  root: string,
  pkg: ReleasePackage,
  env?: Record<string, string>,
): Promise<void> {
  const tempDir = await mkdtemp(join(tmpdir(), 'smoo-npm-bootstrap-'));
  try {
    await writeFile(
      join(tempDir, 'package.json'),
      `${JSON.stringify(
        {
          name: pkg.name,
          version: NPM_BOOTSTRAP_VERSION,
          description: `Bootstrap placeholder for ${pkg.name}. Real releases are published by SmoothBricks CI.`,
          license: stringProperty(pkg.json, 'license') ?? 'UNLICENSED',
          repository: pkg.json.repository,
          publishConfig: { access: 'public' },
        },
        null,
        2,
      )}\n`,
    );
    await writeFile(
      join(tempDir, 'README.md'),
      `# ${pkg.name}\n\nThis is a bootstrap placeholder. Real package versions are published by SmoothBricks release automation.\n`,
    );
    await runLatestNpm(root, ['publish', tempDir, '--access', 'public', '--tag', NPM_BOOTSTRAP_DIST_TAG], env);
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
  return packages.map((pkg) => pkg.projectName).join(',');
}

async function releaseVersionPackages(
  root: string,
  packages: ReleasePackage[],
  bump: string,
): Promise<ReleasePackage[]> {
  if (bump !== 'auto') {
    return packages;
  }
  return autoReleaseCandidatePackages(
    {
      gitRefExists: (ref) => gitRefExists(root, ref),
      packageChangedFilesSince: (ref, packagePath) => packageChangedFilesSince(root, ref, packagePath),
      packageJsonAtRef: (ref, packagePath) => packageJsonAtRef(root, ref, packagePath),
      currentPackageJson: (packagePath) => currentPackageJson(root, packagePath),
      packageBuildInputPatterns: (projectName, packagePath) =>
        packageBuildInputPatterns(root, projectName, packagePath),
      packageHasHistory: (packagePath) => packageHasHistory(root, packagePath),
    },
    packages,
  );
}

async function runNxReleaseVersion(root: string, projects: string, bump: string, dryRun: boolean): Promise<void> {
  // Nx owns local release mutation: package versions, bun.lock updates, the
  // release commit, and annotated tags. smoo owns remote publication after the
  // workflow validates the exact release commit Nx produced.
  const nxArgs = ['release', 'version'];
  if (bump !== 'auto') {
    nxArgs.push(bump);
  }
  nxArgs.push(`--projects=${projects}`, '--git-commit=true', '--git-tag=true', '--git-push=false');
  if (dryRun) {
    nxArgs.push('--dry-run');
  }
  await run('nx', nxArgs, root);
}

async function releasePackagesAtHead(root: string, packages: ReleasePackage[]): Promise<ReleasePackage[]> {
  return releasePackagesAtRef(root, packages, 'HEAD');
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
  const result = await $`git tag --list ${`${pkg.projectName}@*`} --sort=-v:refname --merged ${currentTag}`
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

function releaseCompletionShell(root: string): ReleaseCompletionShell<ReleasePackage> {
  return {
    gitHead: () => gitHead(root),
    pushReleaseRefs: (packages) => pushReleaseRefs(root, packages),
    listNpmMissingPackages: (packages) => listUnpublishedPackages(root, packages),
    buildReleaseCandidate: (packages) => buildReleaseCandidate(root, packages),
    publishPackage: async (pkg, distTag, dryRun) => {
      const packageExists = dryRun ? true : await npmPackageExists(root, pkg.name);
      if (!packageExists) {
        throw new Error(missingNpmPackagePublishGuidance(pkg));
      }
      await publishPackedPackage(root, pkg, distTag, dryRun);
    },
    listGithubMissingPackages: (packages) => listMissingGithubReleasePackages(root, packages),
    createGithubRelease: (pkg, dryRun) => createGithubRelease(root, pkg, dryRun),
  };
}

function releaseRepairShell(root: string): ReleaseCompletionShell<ReleasePackage> & {
  checkout(ref: string): Promise<void>;
  withDevenvEnv<T>(runWithEnv: () => Promise<T>): Promise<T>;
  beforeRepairTarget(target: ReleaseTarget): void;
  afterRepairTarget(target: ReleaseTarget): void;
} {
  return {
    ...releaseCompletionShell(root),
    checkout: (ref) => run('git', ['switch', '--detach', ref], root),
    withDevenvEnv: (runWithEnv) => withDevenvEnv(root, runWithEnv),
    beforeRepairTarget: (target) => {
      console.log(`::group::Repair pending release ${target.sha.slice(0, 12)} (${packageSummary(target.packages)})`);
    },
    afterRepairTarget: () => {
      console.log('::endgroup::');
    },
  };
}

function releaseRetagShell(root: string, remote: string) {
  return {
    listReleasePackages: () => releasePackages(root),
    resolveRef: (ref: string) => gitSha(root, ref),
    resolveDispatchRef: (branch: string) => remoteBranchSha(root, remote, branch),
    packageVersionAtRef: (packagePath: string, ref: string) => packageVersionAtRef(root, packagePath, ref),
    npmVersionExists: (name: string, version: string) => npmVersionExists(root, name, version),
    githubReleaseExists: (tag: string) => githubReleaseExists(root, tag),
    remoteTagObject: (tag: string) => remoteReleaseTagObject(root, remote, tag),
    createOrMoveTag: (tag: string, ref: string) => run('git', ['tag', '-fa', tag, '-m', tag, ref], root),
    pushTags: (updates: RetagUnpublishedTagUpdate<ReleasePackage>[]) => pushRetaggedReleaseTags(root, remote, updates),
    dispatchPublishWorkflow: (workflow: string, branch: string) =>
      run('gh', ['workflow', 'run', workflow, '--ref', branch, '-f', 'bump=auto', '-f', 'dry_run=false'], root),
    log: (message: string) => console.log(message),
  };
}

async function listPendingReleaseTargets(root: string, ref: string): Promise<ReleaseTarget[]> {
  const head = await gitHead(root);
  return pendingReleaseTargets(await listOwnedReleaseTagRecords(root, ref), head);
}

async function listOwnedReleaseTagRecords(root: string, ref: string): Promise<ReleaseTagRecord[]> {
  const tags = await gitReleaseTagsByCreatorDate(root);
  console.log(`Repair pending releases: scanning ${tags.length} local tag${tags.length === 1 ? '' : 's'}.`);
  return collectOwnedReleaseTagRecords(releasePackages(root), ref, {
    listReleaseTagsByCreatorDate: async () => tags,
    isAncestor: (ancestor, descendant) => gitIsAncestor(root, ancestor, descendant),
    packageVersionAtRef: (packagePath, tagRef) => packageVersionAtRef(root, packagePath, tagRef),
    durableTagState: (pkg, tag) => durableReleaseTagState(root, pkg, tag),
  });
}

async function durableReleaseTagState(root: string, pkg: Pick<ReleasePackage, 'name' | 'version'>, tag: string) {
  const packageVersion = `${pkg.name}@${pkg.version}`;
  const start = Date.now();
  console.log(`${packageVersion}: checking durable state (npm + GitHub Release ${tag}).`);
  const [npmPublished, githubReleasePresent] = await Promise.all([
    npmVersionExists(root, pkg.name, pkg.version),
    githubReleaseExists(root, tag),
  ]);
  console.log(
    `${packageVersion}: durable state npm=${yesNo(npmPublished)} github=${yesNo(githubReleasePresent)} (${Date.now() - start}ms).`,
  );
  return { npmPublished, githubReleaseExists: githubReleasePresent };
}

async function gitReleaseTagsByCreatorDate(root: string): Promise<GitReleaseTagInfo[]> {
  const result =
    await $`git for-each-ref --sort=-creatordate --format=${'%(refname:short)%09%(creatordate:unix)%09%(*objectname)%09%(objectname)'} refs/tags`
      .cwd(root)
      .quiet()
      .nothrow();
  if (result.exitCode !== 0) {
    throw new Error('Unable to list release tags by creator date.');
  }
  return decode(result.stdout)
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const [name, timestampText, peeledSha, objectSha] = line.split('\t');
      const timestamp = Number(timestampText);
      const sha = peeledSha || objectSha;
      if (!name || !sha || !Number.isSafeInteger(timestamp)) {
        throw new Error(`Unable to parse release tag ref: ${line}`);
      }
      return { name, sha, timestamp };
    });
}

async function buildReleaseCandidate(root: string, packages: ReleasePackage[]): Promise<void> {
  await run('nx', ['run-many', '-t', 'build', `--projects=${releasePackageProjects(packages)}`], root);
}

async function listMissingGithubReleasePackages(root: string, packages: ReleasePackage[]): Promise<ReleasePackage[]> {
  return (
    await Promise.all(packages.map(async (pkg) => ((await githubReleaseExists(root, releaseTag(pkg))) ? null : pkg)))
  ).filter((pkg): pkg is ReleasePackage => pkg !== null);
}

async function createGithubRelease(root: string, pkg: ReleasePackage, dryRun: boolean): Promise<void> {
  const currentTag = releaseTag(pkg);
  console.log(`${pkg.name}@${pkg.version}: rendering GitHub Release notes for ${currentTag}.`);
  console.log(`GitHub release auth: ${envPresence('GH_TOKEN')}, ${envPresence('GITHUB_TOKEN')}.`);
  if (!dryRun) {
    await assertRemoteTagExists(root, currentTag);
  }
  const previousTag = await previousReleaseTag(root, pkg, currentTag);
  if (dryRun) {
    await renderNxProjectChangelogContents({ root, pkg, previousTag, dryRun });
    return;
  }
  const contents = await renderNxProjectChangelogContents({ root, pkg, previousTag, dryRun });
  await createOrUpdateGithubRelease(pkg, contents, {
    githubReleaseExists: (tag) => githubReleaseExists(root, tag),
    runGhRelease: (args) => run('gh', args, root),
    log: (message) => console.log(message),
  });
}

function envPresence(name: string): string {
  return `${name}=${process.env[name] ? 'present' : 'missing'}`;
}

function yesNo(value: boolean): string {
  return value ? 'yes' : 'no';
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

async function writeReleaseSummary(summary: ReleaseSummary<ReleasePackage>): Promise<void> {
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

async function writeRepairSummary(summaries: Array<ReleaseSummary<ReleasePackage>>, dryRun: boolean): Promise<void> {
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

async function gitRefExists(root: string, ref: string): Promise<boolean> {
  const result = await $`git rev-parse --verify ${ref}`.cwd(root).quiet().nothrow();
  return result.exitCode === 0;
}

async function packageChangedFilesSince(root: string, ref: string, packagePath: string): Promise<string[]> {
  const result = await $`git diff --name-only ${`${ref}..HEAD`} -- ${packagePath}`.cwd(root).quiet().nothrow();
  if (result.exitCode !== 0) {
    throw new Error(`Unable to inspect package changes under ${packagePath}.`);
  }
  const packagePrefix = `${packagePath}/`;
  return decode(result.stdout)
    .split('\n')
    .map((path) => path.trim())
    .filter(Boolean)
    .map((path) => (path.startsWith(packagePrefix) ? path.slice(packagePrefix.length) : path));
}

async function packageJsonAtRef(
  root: string,
  ref: string,
  packagePath: string,
): Promise<Record<string, unknown> | null> {
  const result = await $`git show ${`${ref}:${packagePath}/package.json`}`.cwd(root).quiet().nothrow();
  if (result.exitCode !== 0) {
    return null;
  }
  try {
    const parsed = JSON.parse(decode(result.stdout));
    return isRecord(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

async function currentPackageJson(root: string, packagePath: string): Promise<Record<string, unknown> | null> {
  return readPackageJson(join(root, packagePath, 'package.json'))?.json ?? null;
}

async function packageBuildInputPatterns(root: string, projectName: string, _packagePath: string): Promise<string[]> {
  const project = await nxProjectJson(root, projectName);
  const nxJson = readJsonObject(join(root, 'nx.json')) ?? {};
  return resolveBuildInputPatterns(project, nxJson);
}

async function nxProjectJson(root: string, projectName: string): Promise<Record<string, unknown>> {
  const result = await $`nx show project ${projectName} --json`.cwd(root).quiet();
  const parsed = JSON.parse(decode(result.stdout));
  if (!isRecord(parsed)) {
    throw new Error(`Unable to inspect Nx project ${projectName}.`);
  }
  return parsed;
}

function resolveBuildInputPatterns(project: Record<string, unknown>, nxJson: Record<string, unknown>): string[] {
  const targets = recordProperty(project, 'targets');
  if (!targets) {
    return [];
  }
  return normalizeInputPatterns(collectBuildInputs(targets), nxJson);
}

function collectBuildInputs(targets: Record<string, unknown>): string[] {
  const build = recordProperty(targets, 'build');
  if (!build) {
    return ['production'];
  }
  const directInputs = stringArrayProperty(build, 'inputs');
  if (directInputs.length > 0) {
    return directInputs;
  }
  const inputs: string[] = [];
  for (const dependency of stringArrayProperty(build, 'dependsOn')) {
    if (dependency.startsWith('^')) {
      continue;
    }
    const targetName = dependency.includes(':') ? dependency.split(':')[1] : dependency;
    if (!targetName) {
      continue;
    }
    inputs.push(...stringArrayProperty(recordProperty(targets, targetName), 'inputs'));
  }
  return inputs.length > 0 ? inputs : ['production'];
}

function normalizeInputPatterns(inputs: string[], nxJson: Record<string, unknown>): string[] {
  const patterns: string[] = [];
  const seen = new Set<string>();
  for (const input of inputs) {
    for (const pattern of expandInputPattern(input, nxJson, seen)) {
      patterns.push(pattern);
    }
  }
  return patterns;
}

function expandInputPattern(input: string, nxJson: Record<string, unknown>, seen: Set<string>): string[] {
  if (seen.has(input)) {
    return [];
  }
  seen.add(input);
  if (!input.includes('{')) {
    const namedInputs = recordProperty(nxJson, 'namedInputs');
    const namedInput = namedInputs?.[input];
    if (Array.isArray(namedInput)) {
      return namedInput.flatMap((entry) => (typeof entry === 'string' ? expandInputPattern(entry, nxJson, seen) : []));
    }
    return [];
  }
  const excluded = input.startsWith('!');
  const rawInput = excluded ? input.slice(1) : input;
  if (!rawInput.startsWith('{projectRoot}/')) {
    return [];
  }
  const packageRelative = rawInput.slice('{projectRoot}/'.length);
  return [`${excluded ? '!' : ''}${packageRelative}`];
}

function recordProperty(record: Record<string, unknown> | null, key: string): Record<string, unknown> | null {
  if (!record) {
    return null;
  }
  const value = record[key];
  return isRecord(value) ? value : null;
}

function stringArrayProperty(record: Record<string, unknown> | null, key: string): string[] {
  const value = record?.[key];
  return Array.isArray(value) ? value.filter((entry): entry is string => typeof entry === 'string') : [];
}

async function packageHasHistory(root: string, packagePath: string): Promise<boolean> {
  const result = await $`git log --format=%H -- ${packagePath}`.cwd(root).quiet().nothrow();
  if (result.exitCode !== 0) {
    throw new Error(`Unable to inspect package history under ${packagePath}.`);
  }
  return decode(result.stdout).trim().length > 0;
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
  return (await gitCommitForTag(root, tag)) === (await gitSha(root, ref));
}

async function gitCommitForTag(root: string, tag: string): Promise<string> {
  const result = await $`git rev-list -n 1 ${tag}`.cwd(root).quiet().nothrow();
  if (result.exitCode !== 0) {
    throw new Error(`Unable to resolve release tag ${tag}.`);
  }
  return decode(result.stdout).trim();
}

async function remoteReleaseTagExists(root: string, remote: string, tag: string): Promise<boolean> {
  const result = await $`git ls-remote --exit-code --tags ${remote} ${`refs/tags/${tag}`}`.cwd(root).quiet().nothrow();
  return result.exitCode === 0;
}

async function remoteReleaseTagObject(root: string, remote: string, tag: string): Promise<string | null> {
  const result = await $`git ls-remote --exit-code --tags ${remote} ${`refs/tags/${tag}`}`.cwd(root).quiet().nothrow();
  if (result.exitCode === 2) {
    return null;
  }
  if (result.exitCode !== 0) {
    throw new Error(`Unable to inspect remote release tag ${tag} on ${remote}.`);
  }
  const [object] = decode(result.stdout).trim().split('\t');
  if (!object) {
    throw new Error(`Unable to parse remote release tag ${tag} on ${remote}.`);
  }
  return object;
}

async function remoteBranchSha(root: string, remote: string, branch: string): Promise<string | null> {
  const result = await $`git ls-remote --exit-code --heads ${remote} ${`refs/heads/${branch}`}`
    .cwd(root)
    .quiet()
    .nothrow();
  if (result.exitCode === 2) {
    return null;
  }
  if (result.exitCode !== 0) {
    throw new Error(`Unable to inspect remote branch ${branch} on ${remote}.`);
  }
  const [sha] = decode(result.stdout).trim().split('\t');
  if (!sha) {
    throw new Error(`Unable to parse remote branch ${branch} on ${remote}.`);
  }
  return sha;
}

async function pushRetaggedReleaseTags(
  root: string,
  remote: string,
  updates: Array<RetagUnpublishedTagUpdate<ReleasePackage>>,
): Promise<void> {
  const leases = updates.map(
    (update) => `--force-with-lease=refs/tags/${update.tag}:${update.expectedRemoteObject ?? ''}`,
  );
  const refspecs = updates.map((update) => `refs/tags/${update.tag}:refs/tags/${update.tag}`);
  await run('git', ['push', '--atomic', ...leases, remote, ...refspecs], root);
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

async function runLatestNpmTrust(
  root: string,
  npmArgs: string[],
  env?: Record<string, string>,
): Promise<TrustPublisherResult> {
  const status = await runInteractiveStatus(
    'nix',
    ['shell', 'nixpkgs#nodejs_latest', '-c', 'npm', ...npmArgs],
    root,
    env,
  );
  if (status === 0) {
    return 'configured';
  }
  throw new Error(`nix shell nixpkgs#nodejs_latest -c npm ${npmArgs.join(' ')} failed with exit code ${status}`);
}

async function listTrustedPublishers(root: string, pkg: Pick<ReleasePackage, 'name'>): Promise<TrustedPublisher[]> {
  const args = ['trust', 'list', pkg.name, '--json'];
  let result = await runResult('nix', ['shell', 'nixpkgs#nodejs_latest', '-c', 'npm', ...args], root);
  if (result.exitCode !== 0 && /\bEOTP\b/.test(`${result.stdout}\n${result.stderr}`)) {
    const status = await runInteractiveStatus('nix', ['shell', 'nixpkgs#nodejs_latest', '-c', 'npm', ...args], root);
    if (status !== 0) {
      throw new Error(`nix shell nixpkgs#nodejs_latest -c npm ${args.join(' ')} failed with exit code ${status}`);
    }
    result = await runResult('nix', ['shell', 'nixpkgs#nodejs_latest', '-c', 'npm', ...args], root);
  }
  if (result.exitCode !== 0) {
    throw new Error(
      `nix shell nixpkgs#nodejs_latest -c npm ${args.join(' ')} failed with exit code ${result.exitCode}`,
    );
  }
  return parseTrustedPublishers(result.stdout, pkg.name);
}

export function parseTrustedPublishers(stdout: string, packageName: string): TrustedPublisher[] {
  if (stdout.trim().length === 0) {
    return [];
  }
  const parsed = JSON.parse(stdout) as unknown;
  if (parsed === null) {
    return [];
  }
  if (Array.isArray(parsed)) {
    return parsed.map((value) => parseTrustedPublisher(value, packageName));
  }
  if (isRecord(parsed)) {
    return [parseTrustedPublisher(parsed, packageName)];
  }
  throw new Error(`${packageName}: npm trust list returned invalid JSON.`);
}

function parseTrustedPublisher(value: unknown, packageName: string): TrustedPublisher {
  if (!isRecord(value)) {
    throw new Error(`${packageName}: npm trust list returned invalid trusted publisher entry.`);
  }
  const id = stringProperty(value, 'id');
  const type = stringProperty(value, 'type');
  if (!id || !type) {
    throw new Error(`${packageName}: npm trust list returned a trusted publisher without id or type.`);
  }
  return {
    id,
    type,
    file: stringProperty(value, 'file') ?? undefined,
    repository: stringProperty(value, 'repository') ?? undefined,
  };
}

async function runLatestNpmPublish(root: string, npmArgs: string[]): Promise<void> {
  await runLatestNpm(root, npmArgs, { NODE_AUTH_TOKEN: '', NPM_TOKEN: '' });
}

function missingNpmPackagePublishGuidance(pkg: Pick<ReleasePackage, 'name'>): string {
  return `${pkg.name} does not exist on npm yet. Run smoo release trust-publisher --bootstrap locally before rerunning the Publish workflow.`;
}

async function promptForNpmOtp(packageName: string): Promise<string> {
  if (!process.stdin.isTTY || !process.stdout.isTTY) {
    throw new Error(
      `npm requires a one-time password for ${packageName}. Pass --otp <code> in non-interactive shells.`,
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
