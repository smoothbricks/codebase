import { readFileSync } from 'node:fs';
import { listPublicPackages } from '../lib/workspace.js';
import { validateCommitMessage } from './commit-msg.js';
import { applyWorkspaceGitConfig } from './git-config.js';
import { syncBunLockfileVersions } from './lockfile.js';
import { applyManagedFiles, printResults } from './managed-files.js';
import { validatePublicTags } from './package-policy.js';
import { runInitPacks, runValidatePacks } from './packs/index.js';
import { syncRootRuntimeVersions } from './runtime.js';

export interface InitOptions {
  runtimeOnly?: boolean;
  syncRuntime?: boolean;
}

export async function initMonorepo(root: string, options: InitOptions): Promise<void> {
  if (options.runtimeOnly) {
    await syncRootRuntimeVersions(root);
    return;
  }

  printResults(applyManagedFiles(root, 'update'));
  await runInitPacks({ root, syncRuntime: process.env.DEVENV_ROOT !== undefined || options.syncRuntime === true });
}

export async function validateMonorepo(root: string): Promise<void> {
  const failures = await runValidatePacks({ root, syncRuntime: false });
  if (failures > 0) {
    throw new Error(`Monorepo validation failed with ${failures} problem(s). Run: smoo monorepo init`);
  }
  console.log('Monorepo configuration is valid.');
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

export function validateCommitMessageFile(path: string | undefined): void {
  if (!path) {
    throw new Error('Usage: smoo monorepo validate-commit-msg <commit-msg-file>');
  }
  const message = readFileSync(path, 'utf8');
  const error = validateCommitMessage(message);
  if (error) {
    throw new Error(error);
  }
}

export function listPublicProjects(root: string): string {
  return listPublicPackages(root)
    .map((pkg) => pkg.name)
    .join(',');
}

export function validatePublicPackageTags(root: string): void {
  if (validatePublicTags(root) > 0) {
    throw new Error('npm:public tag validation failed.');
  }
}

export { applyWorkspaceGitConfig, syncBunLockfileVersions };
