import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { $ } from 'bun';
import type { NxJson, NxProjectJson, PackageJson } from '../lib/json.js';
import { parseNxJsonText, parseNxProjectJsonText, parsePackageJsonText } from '../lib/json.js';
import { decode, runText } from '../lib/run.js';
import { readPackageJson } from '../lib/workspace.js';
import { resolveBuildInputPatterns } from './build-inputs.js';
import type { AutoReleaseCandidateShell } from './candidates.js';

/** The git- and Nx-backed shell the release candidate rules read the repository through. */
export async function releaseCandidateShell(root: string): Promise<AutoReleaseCandidateShell> {
  return {
    gitRefExists: (ref) => gitRefExists(root, ref),
    latestStableReleaseRef: (projectName) => latestStableReleaseRef(root, projectName),
    packageChangedFilesSince: (ref, packagePath) => packageChangedFilesSince(root, ref, packagePath),
    packageJsonAtRef: (ref, packagePath) => packageJsonAtRef(root, ref, packagePath),
    currentPackageJson: (packagePath) => currentPackageJson(root, packagePath),
    packageBuildInputPatterns: (projectName, packagePath) => packageBuildInputPatterns(root, projectName, packagePath),
    packageHasHistory: (packagePath) => packageHasHistory(root, packagePath),
  };
}

export async function gitRefExists(root: string, ref: string): Promise<boolean> {
  const result = await $`git rev-parse --verify ${ref}`.cwd(root).quiet().nothrow();
  return result.exitCode === 0;
}

async function latestStableReleaseRef(root: string, projectName: string): Promise<string | null> {
  // List tags matching <projectName>@* sorted newest-first by version, then
  // return the first non-prerelease tag as a full ref.
  const pattern = `${projectName}@*`;
  const result = await $`git tag --list ${pattern} --sort=-v:refname`.cwd(root).quiet().nothrow();
  if (result.exitCode !== 0) {
    return null;
  }
  const tags = decode(result.stdout)
    .split('\n')
    .map((t) => t.trim())
    .filter(Boolean);
  const prefix = `${projectName}@`;
  for (const tagName of tags) {
    const version = tagName.slice(prefix.length);
    if (version && !version.includes('-')) {
      return `refs/tags/${tagName}`;
    }
  }
  return null;
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

async function packageJsonAtRef(root: string, ref: string, packagePath: string): Promise<PackageJson | null> {
  const result = await $`git show ${`${ref}:${packagePath}/package.json`}`.cwd(root).quiet().nothrow();
  if (result.exitCode !== 0) {
    return null;
  }
  try {
    return parsePackageJsonText(decode(result.stdout));
  } catch {
    return null;
  }
}

async function currentPackageJson(root: string, packagePath: string): Promise<PackageJson | null> {
  return readPackageJson(join(root, packagePath, 'package.json'))?.json ?? null;
}

async function packageBuildInputPatterns(root: string, projectName: string, _packagePath: string): Promise<string[]> {
  const project = await nxProjectJson(root, projectName);
  return resolveBuildInputPatterns(project, readNxJson(join(root, 'nx.json')));
}

async function nxProjectJson(root: string, projectName: string): Promise<NxProjectJson> {
  // `nx show project` computes the project graph, so it fails for real
  // environmental reasons -- an unbuilt plugin, an unreadable cache, a graph
  // error. runText prints what nx said before throwing; a bare `.quiet()`
  // template here reported only "Failed with exit code 1" and dropped the rest.
  const stdout = await runText('nx', ['show', 'project', projectName, '--json'], root);
  const parsed = parseNxProjectJsonText(stdout);
  if (!parsed) {
    throw new Error(`Unable to inspect Nx project ${projectName}: nx show project returned unparseable JSON.`);
  }
  return parsed;
}

function readNxJson(path: string): NxJson {
  if (!existsSync(path)) {
    return {};
  }
  try {
    return parseNxJsonText(readFileSync(path, 'utf8')) ?? {};
  } catch {
    return {};
  }
}

async function packageHasHistory(root: string, packagePath: string): Promise<boolean> {
  const result = await $`git log --format=%H -- ${packagePath}`.cwd(root).quiet().nothrow();
  if (result.exitCode !== 0) {
    throw new Error(`Unable to inspect package history under ${packagePath}.`);
  }
  return decode(result.stdout).trim().length > 0;
}
