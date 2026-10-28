import { appendFileSync, readFileSync, writeFileSync } from 'node:fs';
import { run } from '../lib/run.js';
import { getWorkspacePackages, listReleasePackages } from '../lib/workspace.js';
import { type ProjectTargets, readProjectTargets } from '../nx/index.js';
import {
  applyCargoFeatureUnification,
  validateCargoCachePolicy,
  validateCargoToolchainInputs,
} from './cargo-policy.js';
import {
  formatCommitMessage,
  stagedDeletedPublicPackages,
  validateBreakingDisclosure,
  validateCommitMessage,
} from './commit-msg.js';
import { keepDeveloperLinks } from './developer-links.js';
import { applyWorkspaceGitConfig } from './git-config.js';
import { validateGoToolchainAgreement } from './go-toolchain.js';
import { syncBunLockfileVersions } from './lockfile.js';
import {
  applyManagedFiles,
  printResults,
  validateDevenvModuleImport,
  validateDevenvReleaseLock,
  warnOnManagedFileDrift,
} from './managed-files.js';
import {
  applyFixableMonorepoDefaults,
  applyNxReleaseDefaults,
  applyPublicPackageDefaults,
  applyWorkspaceDependencyDefaults,
  listValidCommitScopes,
  validateAggregateMemberCache,
  validateExplicitTaskInputs,
  validateNoStaleTestOutput,
  validateNxProjectNames,
  validateNxReleaseConfig,
  validatePublicPackageMetadata,
  validatePublicTags,
  validateRootPackagePolicy,
  validateSingletonPeerDependencies,
  validateTestFileLocations,
  validateWorkspaceDependencies,
} from './package-policy.js';
import { resolvedTargetsByProject, runInitPacks, runValidatePacks } from './packs/index.js';
import { syncRootRuntimeVersions } from './runtime.js';
import { validateSccachePatches } from './sccache-patches.js';
import { applyToolConfigDefaults } from './tool-validation.js';

export interface InitOptions {
  runtimeOnly?: boolean;
  syncRuntime?: boolean;
}

export interface ValidateOptions {
  failFast?: boolean;
  fix?: boolean;
  verbose?: boolean;
  /** Nx project names to build and pack-validate; see MonorepoContext.projects. */
  projects?: readonly string[];
  /** See MonorepoContext.repairsPending. */
  repairsPending?: boolean;
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

  printResults(await applyManagedFiles(root, 'update'));
  await runInitPacks({ root, syncRuntime: process.env.DEVENV_ROOT !== undefined || options.syncRuntime === true });
}

export async function validateMonorepo(root: string, options: ValidateOptions = {}): Promise<void> {
  const result = await runValidatePacks(
    {
      root,
      syncRuntime: false,
      verbose: options.verbose === true,
      projects: options.projects,
      repairsPending: options.repairsPending,
    },
    options,
  );
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

export async function updateManagedFiles(root: string): Promise<void> {
  printResults(await applyManagedFiles(root, 'update'));
  applyFixableMonorepoDefaults(root);
  applyNxReleaseDefaults(root);
  applyPublicPackageDefaults(root);
  applyWorkspaceDependencyDefaults(root, {
    resolvedTargetsByProject: resolvedTargetsByProject(await readProjectTargets(root)),
  });
  // Tool dependency policy (typescript API 6, @typescript/native for ttsc, nx, …)
  // lives next to managed templates — update must install them, not only rewrite files.
  await applyToolConfigDefaults(root);
  // Rust's half of the same job: a multi-crate Cargo workspace must resolve
  // features once for every member, or each per-crate cargo invocation
  // recompiles the shared graph for its own selection.
  applyCargoFeatureUnification(root);
  // packageManager, engines.node and the @types pins are derived from the
  // resolved toolchain, so update recomputes them.
  await syncRootRuntimeVersions(root);
  syncBunLockfileVersions(root, { mode: 'install' });
  console.log('installing     workspace dependencies (bun install)');
  // Nx comes from the lockfile, so a developer link to a local Nx checkout gives way to it; every other link is kept.
  await keepDeveloperLinks(root, () => run('bun', ['install', '--no-summary'], root), { relink: ['nx'] });
}

/**
 * Managed-file drift plus every package policy.
 *
 * `warn` makes only the DRIFT non-blocking: CI reports it and dispatches the managed-files
 * workflow, which heals it in a review PR. Nothing heals a policy failure, so the policies fail
 * the check either way — a `--warn` run that skipped them left every policy enforced nowhere but
 * on a developer's machine.
 */
export async function checkManagedFiles(root: string, options: { warn?: boolean } = {}): Promise<void> {
  let drifted = false;
  if (options.warn === true) {
    await warnOnManagedFileDrift(root);
  } else {
    const results = await applyManagedFiles(root, 'check');
    printResults(results);
    drifted = results.some((result) => result.action === 'drifted');
  }
  const packageFailures = await checkPolicyFailures(root, await readProjectTargets(root));
  if (drifted) {
    throw new Error('Managed monorepo files or package conventions are out of date. Run: smoo monorepo update');
  }
  if (packageFailures > 0) {
    throw new Error(`${packageFailures} monorepo package convention check(s) failed; fix the findings above.`);
  }
}

/**
 * The findings `smoo monorepo check` fails on besides drift, over the project targets Nx
 * resolved. Each validator prints its own findings; the sum is how many there were.
 */
export async function checkPolicyFailures(root: string, projectTargets: ProjectTargets[]): Promise<number> {
  return (
    // The devenv profile's Go against the SDK ttsc vendors: two releases are two
    // cache keys and a GOROOT crossing between them fails the build.
    (await validateGoToolchainAgreement(root)) +
    validateRootPackagePolicy(root) +
    validateNxProjectNames(root) +
    validateExplicitTaskInputs(projectTargets) +
    validateAggregateMemberCache(projectTargets) +
    validateNxReleaseConfig(root) +
    validatePublicTags(root) +
    validatePublicPackageMetadata(root) +
    validateSingletonPeerDependencies(root) +
    validateTestFileLocations(root) +
    validateNoStaleTestOutput(root) +
    validateWorkspaceDependencies(root, { resolvedTargetsByProject: resolvedTargetsByProject(projectTargets) }) +
    validateDevenvModuleImport(root) +
    (await validateDevenvReleaseLock(root)) +
    validateCargoCachePolicy(root) +
    validateCargoToolchainInputs(root, projectTargets) +
    validateSccachePatches(root)
  );
}

export async function diffManagedFiles(root: string): Promise<void> {
  printResults(await applyManagedFiles(root, 'diff'));
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
  const disclosure = validateBreakingDisclosure(message, stagedDeletedPublicPackages(root));
  if (disclosure) {
    throw new Error(disclosure);
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
