import { appendFileSync, readFileSync, writeFileSync } from 'node:fs';
import { $ } from 'bun';
import { decode, run } from '../lib/run.js';
import { escapeRegex, getWorkspacePackages, getWorkspacePatterns, listReleasePackages } from '../lib/workspace.js';
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

export interface SetupTestTracingOptions {
  all?: boolean;
  projects?: string;
  opContextExport?: string;
  tracerModule?: string;
  dryRun?: boolean;
}

export interface SetupTestTracingShell {
  run(command: string, args: string[], cwd: string): Promise<void>;
  log(message: string): void;
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

export function listReleaseProjectNamesForNx(root: string, options: ListReleasePackagesOptions = {}): string {
  const packages = listReleasePackages(root)
    .map((pkg) => pkg.projectName)
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

export async function setupTestTracing(
  root: string,
  options: SetupTestTracingOptions,
  shell: SetupTestTracingShell = defaultSetupTestTracingShell,
): Promise<void> {
  const selectedPackages = selectTestTracingPackages(root, options);
  const opContextExport = options.opContextExport ?? 'opContext';
  const tracerModule = options.tracerModule ?? '@smoothbricks/lmao/testing/bun';

  if (selectedPackages.length === 0) {
    throw new Error('No workspace packages matched LMAO test tracing setup selection.');
  }

  for (const pkg of selectedPackages) {
    const args = [
      'g',
      '@smoothbricks/nx-plugin:bun-test-tracing',
      '--project',
      pkg.projectName,
      '--opContextModule',
      pkg.name,
      '--opContextExport',
      opContextExport,
      '--tracerModule',
      tracerModule,
    ];
    const commandPreview = `nx ${args.join(' ')}`;
    if (options.dryRun) {
      shell.log(`would run      ${commandPreview}`);
      continue;
    }
    shell.log(`running        ${commandPreview}`);
    await shell.run('nx', args, root);
  }
}

export { applyWorkspaceGitConfig, syncBunLockfileVersions };

const defaultSetupTestTracingShell: SetupTestTracingShell = {
  run,
  log(message) {
    console.log(message);
  },
};

function selectTestTracingPackages(root: string, options: SetupTestTracingOptions) {
  const packages = getWorkspacePackages(root);
  const requested = splitCommaList(options.projects);

  if (options.all && requested.length > 0) {
    throw new Error('Use either --all or --projects, not both.');
  }
  if (!options.all && requested.length === 0) {
    throw new Error('Pass --all or --projects <projects> to select packages for LMAO test tracing setup.');
  }
  if (options.all) {
    return packages;
  }

  const bySelector = new Map<string, (typeof packages)[number]>();
  for (const pkg of packages) {
    bySelector.set(pkg.projectName, pkg);
    bySelector.set(pkg.name, pkg);
    bySelector.set(pkg.path, pkg);
  }

  const selected = [];
  const missing = [];
  for (const selector of requested) {
    const pkg = bySelector.get(selector);
    if (pkg) {
      selected.push(pkg);
    } else {
      missing.push(selector);
    }
  }

  if (missing.length > 0) {
    throw new Error(`Unknown workspace package selection for LMAO test tracing setup: ${missing.join(', ')}`);
  }

  return selected;
}

function splitCommaList(value: string | undefined): string[] {
  return (value ?? '')
    .split(',')
    .map((entry) => entry.trim())
    .filter(Boolean);
}

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
