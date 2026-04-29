import { appendFileSync, readFileSync, writeFileSync } from 'node:fs';
import { $ } from 'bun';
import { decode } from '../lib/run.js';
import { escapeRegex, getWorkspacePatterns, listReleasePackages } from '../lib/workspace.js';
import { formatCommitMessage, validateCommitMessage } from './commit-msg.js';
import { applyWorkspaceGitConfig } from './git-config.js';
import { syncBunLockfileVersions } from './lockfile.js';
import { applyManagedFiles, printResults } from './managed-files.js';
import { listValidCommitScopes, validatePublicTags } from './package-policy.js';
import { runInitPacks, runValidatePacks } from './packs/index.js';
import { syncRootRuntimeVersions } from './runtime.js';

export interface InitOptions {
  runtimeOnly?: boolean;
  syncRuntime?: boolean;
}

export interface ValidateOptions {
  failFast?: boolean;
  onlyIfNewWorkspacePackage?: boolean;
  fix?: boolean;
  verbose?: boolean;
}

export interface ValidateCommitMessageOptions {
  fix?: boolean;
}

export interface ListReleasePackagesOptions {
  failEmpty?: boolean;
  githubOutput?: string;
}

export async function initMonorepo(root: string, options: InitOptions): Promise<void> {
  if (options.runtimeOnly) {
    await syncRootRuntimeVersions(root);
    return;
  }

  printResults(applyManagedFiles(root, 'update'));
  await runInitPacks({ root, syncRuntime: process.env.DEVENV_ROOT !== undefined || options.syncRuntime === true });
}

export async function validateMonorepo(root: string, options: ValidateOptions = {}): Promise<void> {
  if (options.onlyIfNewWorkspacePackage && !(await hasNewWorkspacePackage(root))) {
    return;
  }
  const result = await runValidatePacks({ root, syncRuntime: false, verbose: options.verbose === true }, options);
  if (result.failures > 0) {
    const checkNoun = result.failedChecks === 1 ? 'check' : 'checks';
    const problemNoun = result.failures === 1 ? 'problem' : 'problems';
    throw new Error(
      `\n🔴 Monorepo validation failed: ${result.failedChecks} ${checkNoun} failed with ${result.failures} ${problemNoun}.`,
    );
  }
  if (options.verbose) {
    console.log('\n== summary ==');
    console.log('Monorepo configuration is valid.');
  } else {
    console.log('🟢 Monorepo configuration is valid.');
  }
}

export function updateManagedFiles(root: string): void {
  printResults(applyManagedFiles(root, 'update'));
}

export function checkManagedFiles(root: string): void {
  const results = applyManagedFiles(root, 'check');
  printResults(results);
  if (results.some((result) => result.action === 'drifted')) {
    throw new Error('Managed monorepo files are out of date. Run: smoo monorepo update');
  }
}

export function diffManagedFiles(root: string): void {
  printResults(applyManagedFiles(root, 'diff'));
}

export function validateCommitMessageFile(
  path: string | undefined,
  options: ValidateCommitMessageOptions = {},
  root = process.cwd(),
): void {
  if (!path) {
    throw new Error('Usage: smoo monorepo validate-commit-msg <commit-msg-file>');
  }
  let message = readFileSync(path, 'utf8');
  if (options.fix) {
    const formatted = formatCommitMessage(message);
    if (formatted !== message) {
      writeFileSync(path, formatted);
      message = formatted;
    }
  }
  const error = validateCommitMessage(message, { validScopes: listValidCommitScopes(root) });
  if (error) {
    throw new Error(error);
  }
}

export function listReleasePackagesForNx(root: string, options: ListReleasePackagesOptions = {}): string {
  const packages = listReleasePackages(root)
    .map((pkg) => pkg.name)
    .join(',');
  if (!packages && options.failEmpty) {
    throw new Error('No owned release packages found.');
  }
  if (options.githubOutput) {
    appendFileSync(options.githubOutput, `projects=${packages}\n`);
  }
  return packages;
}

export function validatePublicPackageTags(root: string): void {
  if (validatePublicTags(root) > 0) {
    throw new Error('npm:public tag validation failed.');
  }
}

export { applyWorkspaceGitConfig, syncBunLockfileVersions };

async function hasNewWorkspacePackage(root: string): Promise<boolean> {
  const result = await $`git diff --cached --name-only --diff-filter=A -- ${'*/package.json'}`
    .cwd(root)
    .quiet()
    .nothrow();
  if (result.exitCode !== 0) {
    throw new Error('Unable to inspect staged package manifests.');
  }
  const manifests = decode(result.stdout)
    .split('\n')
    .map((path) => path.trim())
    .filter(Boolean);
  if (manifests.length === 0) {
    return false;
  }
  const patterns = getWorkspacePatterns(root)
    .filter((pattern) => pattern.endsWith('/*'))
    .map((pattern) => new RegExp(`^${escapeRegex(pattern.slice(0, -2))}/[^/]+/package\\.json$`));
  return manifests.some((manifest) => patterns.some((pattern) => pattern.test(manifest)));
}
