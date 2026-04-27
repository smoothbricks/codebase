import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { $ } from 'bun';
import { decode, run, runStatus } from '../lib/run.js';
import { listPublicPackages, readPackageJson, repositoryInfo } from '../lib/workspace.js';
import { syncBunLockfileVersions } from '../monorepo/lockfile.js';

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
  if ((await gitTagsAtHead(root)).length > 0) {
    console.log('HEAD already has release tags; skipping version bump and resuming publish.');
    return;
  }

  const nxArgs = ['release', 'version'];
  if (bump !== 'auto') {
    nxArgs.push(bump);
  }
  nxArgs.push(`--projects=${projects}`);
  if (options.dryRun) {
    nxArgs.push('--dryRun');
  }
  await run('nx', nxArgs, root);

  if (existsSync(join(root, 'bun.lock'))) {
    syncBunLockfileVersions(root);
  }
  if (options.dryRun) {
    return;
  }
  if (existsSync(join(root, 'bun.lock'))) {
    await run('git', ['add', 'bun.lock'], root);
  }
  if ((await runStatus('git', ['diff', '--cached', '--quiet'], root)) !== 0) {
    await run('git', ['commit', '-m', 'chore(release): sync bun lockfile versions'], root);
  }
  await run('git', ['push'], root);
  await run('git', ['push', '--tags'], root);
}

export async function releasePublish(root: string, options: ReleasePublishOptions): Promise<void> {
  if (options.dryRun) {
    // Bun still requires npm authentication for `bun publish --dry-run`.
    // Package packing is already validated by `smoo monorepo validate`, so dry
    // runs stop before the network/auth boundary.
    console.log('Dry run; skipping npm publish.');
    return;
  }
  const tag = releaseNpmTagArg(options);
  const projects = listPublicPackages(root)
    .map((pkg) => pkg.name)
    .join(',');
  const nxArgs = ['release', 'publish', `--projects=${projects}`, '--tag', tag];
  await run('nx', nxArgs, root);
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

  if (!options.dryRun && !options.skipLogin) {
    await runLatestNpm(root, ['login', '--auth-type=web']);
  }

  for (const pkg of packages) {
    console.log(`${pkg.name}: trusting GitHub Actions ${repository}/${workflow}`);
    const args = ['trust', 'github', pkg.name, '--file', workflow, '--repo', repository, '--yes'];
    if (options.dryRun) {
      args.push('--dry-run');
    }
    await runLatestNpm(root, args);
  }
}

export async function printReleaseState(root: string): Promise<void> {
  console.log(JSON.stringify(await getReleaseState(root), null, 2));
}

interface ReleaseState {
  packages: { name: string; version: string; published: boolean }[];
  allPublished: boolean;
}

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

async function npmVersionExists(name: string, version: string): Promise<boolean> {
  const result = await $`bun pm view ${`${name}@${version}`} version`.cwd(process.cwd()).quiet().nothrow();
  return result.exitCode === 0 && decode(result.stdout).trim() === version;
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

async function runLatestNpm(root: string, npmArgs: string[]): Promise<void> {
  await run('nix', ['shell', 'nixpkgs#nodejs_latest', '-c', 'npm', ...npmArgs], root);
}
