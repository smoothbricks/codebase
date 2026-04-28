import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createInterface } from 'node:readline/promises';
import { Writable } from 'node:stream';
import { $ } from 'bun';
import { decode, run, runStatus } from '../lib/run.js';
import { listPublicPackages, readPackageJson, repositoryInfo } from '../lib/workspace.js';

export interface ReleaseVersionOptions {
  bump: string;
  dryRun?: boolean;
}

export interface ReleasePublishOptions {
  bump: string;
  tag?: string;
  npmTag?: string;
  dryRun?: boolean;
}

export interface ReleaseGithubOptions extends ReleasePublishOptions {
  tags?: string;
}

export interface ReleaseTrustPublisherOptions {
  dryRun?: boolean;
  otp?: string;
  skipLogin?: boolean;
}

export async function releaseVersion(root: string, options: ReleaseVersionOptions): Promise<void> {
  const bump = releaseBumpArg(options.bump);
  const projects = listPublicPackages(root)
    .map((pkg) => pkg.name)
    .join(',');
  const state = await getReleaseState(root);
  if (state.allPublished) {
    console.log('Current package versions are already published; skipping version bump.');
    return;
  }

  const headBeforeVersioning = await gitHead(root);

  // Nx owns local release mutation: package versions, bun.lock updates, the
  // release commit, and annotated tags. smoo owns the remote push so retries do
  // not push an older checkout when Nx skips the release commit.
  const nxArgs = ['release', 'version'];
  if (bump !== 'auto') {
    nxArgs.push(bump);
  }
  nxArgs.push(`--projects=${projects}`, '--git-commit=true', '--git-tag=true', '--git-push=false');
  if (options.dryRun) {
    nxArgs.push('--dryRun');
  }
  await run('nx', nxArgs, root);
  if (!options.dryRun) {
    // Guard against future Nx/config regressions that leave release files, such
    // as bun.lock, outside the release commit after tagging/pushing.
    await assertCleanGitTree(root);
    const headAfterVersioning = await gitHead(root);
    if (headAfterVersioning === headBeforeVersioning) {
      console.log('Nx did not create a release commit; skipping git push.');
      return;
    }
    await pushReleaseCommit(root);
  }
}

export async function releasePublish(root: string, options: ReleasePublishOptions): Promise<void> {
  const tag = releaseNpmTagArg(options);
  const unpublishedPackages = await listUnpublishedPackages(root);
  if (unpublishedPackages.length === 0) {
    console.log('Current package versions are already published; skipping npm publish.');
    return;
  }
  // npm package versions are immutable. Publishing only missing name@version
  // pairs makes auth/network retries idempotent after a partial publish failure.
  for (const pkg of unpublishedPackages) {
    await publishPackedPackage(root, pkg, tag, options.dryRun === true);
  }
}

export async function releaseGithubRelease(root: string, options: ReleaseGithubOptions): Promise<void> {
  if (options.dryRun) {
    console.log('Dry run; skipping GitHub Release creation.');
    return;
  }
  const npmTag = releaseNpmTagArg(options);
  const tags = options.tags ? options.tags.split(/\s+/).filter(Boolean) : await gitTagsAtHead(root);
  if (tags.length === 0) {
    throw new Error('No release tags found. Pass --tags or run from a tagged release commit.');
  }
  const latestFlag = npmTag === 'latest' ? 'true' : 'false';
  for (const tag of tags) {
    const exists = (await runStatus('gh', ['release', 'view', tag], root, true)) === 0;
    if (exists) {
      await run('gh', ['release', 'edit', tag, '--title', tag, `--latest=${latestFlag}`], root);
    } else {
      await run('gh', ['release', 'create', tag, '--title', tag, '--generate-notes', `--latest=${latestFlag}`], root);
    }
  }
}

export async function releaseTrustPublisher(root: string, options: ReleaseTrustPublisherOptions): Promise<void> {
  const repository = githubRepositoryFromRootPackage(root);
  const workflow = 'publish.yml';
  const packages = listPublicPackages(root);
  if (packages.length === 0) {
    throw new Error('No npm:public packages found.');
  }

  if (!options.dryRun) {
    const packageStates = await Promise.all(
      packages.map(async (pkg) => ((await npmPackageExists(pkg.name)) ? null : pkg.name)),
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

type PublicPackage = ReturnType<typeof listPublicPackages>[number];

async function getReleaseState(root: string): Promise<ReleaseState> {
  const packages = listPublicPackages(root);
  const states = await Promise.all(
    packages.map(async (pkg) => {
      const published = await npmVersionExists(pkg.name, pkg.version);
      return { name: pkg.name, version: pkg.version, published };
    }),
  );
  return { packages: states, allPublished: states.every((state) => state.published) };
}

async function listUnpublishedPackages(root: string): Promise<PublicPackage[]> {
  const packages = listPublicPackages(root);
  const states = await Promise.all(
    packages.map(async (pkg) => ({ pkg, published: await npmVersionExists(pkg.name, pkg.version) })),
  );
  return states.filter((state) => !state.published).map((state) => state.pkg);
}

async function publishPackedPackage(root: string, pkg: PublicPackage, tag: string, dryRun: boolean): Promise<void> {
  const tempDir = await mkdtemp(join(tmpdir(), 'smoo-publish-'));
  const tarball = join(tempDir, `${safeTarballPrefix(pkg.name)}-${pkg.version}.tgz`);
  try {
    console.log(`${pkg.name}@${pkg.version}: packing with bun pm pack`);
    await run('bun', ['pm', 'pack', '--filename', tarball, '--ignore-scripts', '--quiet'], join(root, pkg.path));
    await assertNoWorkspaceProtocolInTarball(root, tarball, pkg);
    // npm CLI owns authentication here: trusted publishing OIDC when configured,
    // or the temporary NODE_AUTH_TOKEN bootstrap path below before trust exists.
    // Bun still produces the tarball so workspace:* dependencies are resolved the
    // same way smoo validates packed packages before release.
    const args = ['publish', tarball, '--access', 'public', '--tag', tag, '--provenance'];
    if (dryRun) {
      args.push('--dry-run');
    }
    await runLatestNpmPublish(root, args);
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
}

function safeTarballPrefix(name: string): string {
  return name.replace(/^@/, '').replace(/[^a-zA-Z0-9._-]+/g, '-');
}

async function assertNoWorkspaceProtocolInTarball(root: string, tarball: string, pkg: PublicPackage): Promise<void> {
  const result = await $`tar -xOf ${tarball} package/package.json`.cwd(root).quiet().nothrow();
  if (result.exitCode !== 0) {
    throw new Error(`${pkg.name}: unable to inspect packed package.json before publish.`);
  }
  const manifest = decode(result.stdout);
  if (manifest.includes('workspace:')) {
    throw new Error(`${pkg.name}: packed package.json still contains workspace: dependency references.`);
  }
}

async function npmVersionExists(name: string, version: string): Promise<boolean> {
  const result = await $`bun pm view ${`${name}@${version}`} version`.cwd(process.cwd()).quiet().nothrow();
  return result.exitCode === 0 && decode(result.stdout).trim() === version;
}

async function npmPackageExists(name: string): Promise<boolean> {
  const result = await $`bun pm view ${name} name`.cwd(process.cwd()).quiet().nothrow();
  return result.exitCode === 0;
}

async function gitTagsAtHead(root: string): Promise<string[]> {
  const result = await $`git tag --points-at HEAD`.cwd(root).quiet().nothrow();
  if (result.exitCode !== 0) {
    return [];
  }
  return decode(result.stdout)
    .split('\n')
    .map((tag) => tag.trim())
    .filter(Boolean);
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

async function pushReleaseCommit(root: string): Promise<void> {
  const branch = await releaseBranch(root);
  const remote = await releaseRemote(root, branch);
  await run('git', ['push', '--follow-tags', '--atomic', remote, `HEAD:refs/heads/${branch}`], root);
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

async function runLatestNpmPublish(root: string, npmArgs: string[]): Promise<void> {
  const token = process.env.NODE_AUTH_TOKEN || process.env.NPM_TOKEN;
  if (!token || process.env.NPM_CONFIG_USERCONFIG) {
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
