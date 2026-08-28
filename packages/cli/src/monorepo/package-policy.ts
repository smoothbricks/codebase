import { type Dirent, readdirSync } from 'node:fs';
import { join, relative } from 'node:path';
import {
  applyWorkspaceBoundedTestTargetPolicy,
  checkWorkspaceBoundedTestTargetPolicy,
} from '@smoothbricks/nx-plugin/bounded-test-policy';
import {
  CARGO_CROSS_LINT_TARGET,
  CROSS_CHECK_SCRIPT_COMMAND,
  CROSS_CHECK_SCRIPT_NAME,
} from '@smoothbricks/nx-plugin/cross-check-policy';
import {
  applyPackageTargetPolicy,
  checkPackageTargetPolicy,
  type PackageTargetPolicyOptions,
  type ResolvedProjectTargets,
} from '@smoothbricks/nx-plugin/package-target-policy';
import {
  applyReleaseConfigPolicy,
  checkReleaseConfigPolicy,
  SMOO_NX_RELEASE_TAG_PATTERN,
  SMOO_NX_VERSION_ACTIONS,
} from '@smoothbricks/nx-plugin/release-config-policy';
import { applyTypecheckTestPolicy, checkTypecheckTestPolicy } from '@smoothbricks/nx-plugin/typecheck-test-policy';
import {
  applyWorkspaceConfigPolicy,
  checkWorkspaceConfigPolicy,
} from '@smoothbricks/nx-plugin/workspace-config-policy';
import {
  ensureNx,
  ensurePublishConfig,
  ensureRepositoryObject,
  ensureScripts,
  type PackageExportMap,
  type PackageExports,
  type PackageJson,
  readJsonObject,
  requiredJsonObject,
  type StringMap,
  setMissingPackageStringField,
  setMissingRepositoryField,
  setNxName,
  setPublishAccess,
  setRepositoryField,
  setStringProperty,
  writeJsonObject,
} from '../lib/json.js';
import {
  getWorkspacePackageManifests,
  getWorkspacePackages,
  listPackageJsonRecords,
  listPublicPackages,
  packageRepositoryInfo,
  repositoryInfo,
  sameRepositoryAfterNormalization,
  workspaceDependencyFields,
} from '../lib/workspace.js';

export type { PackageTargetPolicyOptions as WorkspaceDependencyDefaultOptions, ResolvedProjectTargets };
export { SMOO_NX_RELEASE_TAG_PATTERN, SMOO_NX_VERSION_ACTIONS };

// Cross-project scopes with no Nx project of their own: release bookkeeping,
// and `deps` — the ecosystem-standard dependency-update scope (Dependabot and
// Renovate both emit it) for lockfile/patch/root-manifest changes.
const extraCommitScopes = ['release', 'deps'];
const rootScriptPolicy: Record<string, string> = {
  // The Linux compile gate's developer entry point. Installed by `smoo` into
  // every repo alongside the managed devenv profile it activates, so the fleet
  // rule that devenv equals CI holds for the cross arm too.
  [CROSS_CHECK_SCRIPT_NAME]: CROSS_CHECK_SCRIPT_COMMAND,
  clean: 'nx run-many -t clean; nx reset',
  'clean:node_modules': 'rm -rf node_modules && find e* t* p* -type d -name node_modules -print0 | xargs -0 rm -rvf',
  'format:changed': 'git-format-staged --config tooling/git-hooks/git-format-staged.yml --also-unstaged',
  'format:staged': 'git-format-staged --config tooling/git-hooks/git-format-staged.yml',
  lint: 'nx run-many -t lint',
  'lint:fix': 'git-format-staged --config tooling/git-hooks/git-format-staged.yml --unstaged',
};

export function applyFixableMonorepoDefaults(root: string): void {
  applyRootScriptDefaults(root);
  applyNxPluginDefaults(root);
  applyNxProjectNameDefaults(root);
}

export function applyRootScriptDefaults(root: string): void {
  const rootPackagePath = join(root, 'package.json');
  const rootPackage = readJsonObject(rootPackagePath);
  if (!rootPackage) {
    return;
  }
  const scripts = ensureScripts(rootPackage);
  let changed = false;
  for (const [name, command] of Object.entries(rootScriptPolicy)) {
    changed = setStringProperty(scripts, name, command) || changed;
  }
  const nx = ensureNx(rootPackage);
  if (!Array.isArray(nx.includedScripts) || nx.includedScripts.length !== 0) {
    nx.includedScripts = [];
    changed = true;
  }
  changed = sortRecordInPlace(scripts) || changed;
  if (changed) {
    writeJsonObject(rootPackagePath, rootPackage);
    console.log('updated        package.json root smoo scripts');
  } else {
    console.log('unchanged      package.json root smoo scripts');
  }
}

export function applyNxPluginDefaults(root: string): void {
  if (applyWorkspaceConfigPolicy(root)) {
    console.log('updated        nx.json smoo plugin config');
  } else {
    console.log('unchanged      nx.json smoo plugin config');
  }
}

export function applyPublicPackageDefaults(root: string): void {
  const rootPackage = requiredJsonObject(join(root, 'package.json'));
  const rootLicense = rootPackage.license;
  const rootRepository = repositoryInfo(rootPackage);

  for (const pkg of listPublicPackages(root)) {
    let changed = false;
    const existingRepository = packageRepositoryInfo(pkg);
    if (
      existingRepository &&
      rootRepository &&
      existingRepository.url === rootRepository.url &&
      rootLicense &&
      rootLicense !== 'UNLICENSED'
    ) {
      changed = setMissingPackageStringField(pkg.json, 'license', rootLicense) || changed;
    }
    const publishConfig = ensurePublishConfig(pkg.json);
    changed = setPublishAccess(publishConfig, 'public') || changed;

    if (typeof pkg.json.repository !== 'string') {
      const repository = ensureRepositoryObject(pkg.json);
      changed =
        setRepositoryField(repository, 'type', existingRepository?.type ?? rootRepository?.type ?? 'git') || changed;
      if (existingRepository) {
        changed = setMissingRepositoryField(repository, 'url', existingRepository.url) || changed;
      }
      changed = setRepositoryField(repository, 'directory', pkg.path.replaceAll('\\', '/')) || changed;
    }

    changed = normalizeExportConditionOrder(pkg.json.exports) || changed;
    if (hasDevelopmentSourceExport(pkg.json.exports)) {
      changed = addFileEntry(pkg.json, 'src') || changed;
    }

    if (changed) {
      writeJsonObject(pkg.packageJsonPath, pkg.json);
      console.log(`updated        ${pkg.path}/package.json public metadata`);
    } else {
      console.log(`unchanged      ${pkg.path}/package.json public metadata`);
    }
  }
}

export function applyWorkspaceDependencyDefaults(root: string, options: PackageTargetPolicyOptions = {}): void {
  const workspaceNames = new Set(getWorkspacePackages(root).map((pkg) => pkg.name));
  for (const pkg of listPackageJsonRecords(root)) {
    const changed = fixWorkspaceDependencyRanges(pkg.json, workspaceNames);
    if (changed) {
      writeJsonObject(pkg.packageJsonPath, pkg.json);
      console.log(`updated        ${pkg.path}/package.json workspace dependency ranges`);
    }
  }
  if (applyPackageTargetPolicy(root, options)) {
    console.log('updated        package Nx target policy');
  } else {
    console.log('unchanged      package Nx target policy');
  }
  if (applyTypecheckTestPolicy(root)) {
    console.log('updated        tsconfig.test.json policy');
  } else {
    console.log('unchanged      tsconfig.test.json policy');
  }
  if (applyWorkspaceBoundedTestTargetPolicy(root, options)) {
    console.log('updated        package test targets bounded execution policy');
  } else {
    console.log('unchanged      package test targets bounded execution policy');
  }
}

export function applyNxReleaseDefaults(root: string): void {
  if (applyReleaseConfigPolicy(root)) {
    console.log('updated        nx.json release config');
  } else {
    console.log('unchanged      nx.json release config');
  }
}

export function applyNxProjectNameDefaults(root: string): void {
  const rootPackage = requiredJsonObject(join(root, 'package.json'));
  const rootName = rootPackage.name;
  if (!rootName) {
    return;
  }
  for (const pkg of getWorkspacePackageManifests(root)) {
    const suggestedName = suggestNxProjectName(rootName, pkg.name);
    if (!suggestedName) {
      continue;
    }
    const nx = ensureNx(pkg.json);
    const changed = setNxName(nx, suggestedName);
    if (changed) {
      writeJsonObject(pkg.packageJsonPath, pkg.json);
      console.log(`updated        ${pkg.path}/package.json nx.name`);
    } else {
      console.log(`unchanged      ${pkg.path}/package.json nx.name`);
    }
  }
}

export function listValidCommitScopes(root: string): ReadonlySet<string> {
  return new Set([...listNxProjectNames(root), ...extraCommitScopes]);
}

export function listNxProjectNames(root: string): string[] {
  const rootPackage = readJsonObject(join(root, 'package.json'));
  const rootName = rootPackage?.name ?? null;
  const names: string[] = [];
  for (const pkg of getWorkspacePackageManifests(root)) {
    const configuredName = pkg.json.nx?.name;
    const suggestedName = rootName ? suggestNxProjectName(rootName, pkg.name) : null;
    if (configuredName) {
      names.push(configuredName);
    } else if (suggestedName) {
      names.push(suggestedName);
    }
  }
  return names;
}

export function validateRootPackagePolicy(root: string): number {
  const rootPackage = readJsonObject(join(root, 'package.json'));
  if (!rootPackage) {
    console.error('package.json not found or invalid');
    return 1;
  }
  let failures = 0;
  if (!rootPackage.name) {
    console.error('package.json must define name');
    failures++;
  }
  if (!rootPackage.license) {
    console.error('package.json must define repo-wide license');
    failures++;
  }
  if (!repositoryInfo(rootPackage)) {
    console.error('package.json must define repository.url');
    failures++;
  }
  failures += validateRootScripts(rootPackage);
  failures += validateRootNxScriptInference(rootPackage);
  const packageManager = rootPackage.packageManager;
  if (!packageManager?.startsWith('bun@')) {
    console.error('package.json packageManager must use bun@<version>');
    failures++;
  }
  const bunVersion = packageManager?.startsWith('bun@') ? packageManager.slice('bun@'.length) : null;
  const devDependencies = rootPackage.devDependencies;
  if (!bunVersion || !devDependencies || devDependencies['@types/bun'] !== bunVersion) {
    console.error('package.json devDependencies.@types/bun must match packageManager bun version');
    failures++;
  }
  const engines = rootPackage.engines;
  if (!engines?.node) {
    console.error('package.json engines.node must be defined');
    failures++;
  }
  return failures;
}

export function validateNxReleaseConfig(root: string): number {
  let failures = 0;
  for (const issue of checkWorkspaceConfigPolicy(root)) {
    console.error(issue.message);
    failures++;
  }
  for (const issue of checkReleaseConfigPolicy(root)) {
    console.error(issue.message);
    failures++;
  }
  return failures;
}

export function validateNxProjectNames(root: string): number {
  const rootPackage = readJsonObject(join(root, 'package.json'));
  const rootName = rootPackage?.name ?? null;
  if (!rootName) {
    return 0;
  }
  let failures = 0;
  for (const pkg of getWorkspacePackageManifests(root)) {
    const suggestedName = suggestNxProjectName(rootName, pkg.name);
    if (!suggestedName) {
      continue;
    }
    const configuredName = pkg.json.nx?.name ?? null;
    if (configuredName !== suggestedName) {
      console.error(
        `${pkg.path}: package.json nx.name must be "${suggestedName}" so fix(${suggestedName}): maps to this project`,
      );
      failures++;
    }
  }
  if (failures === 0) {
    console.log('Nx project names are valid.');
  }
  return failures;
}

export function validatePublicTags(root: string): number {
  let failures = 0;
  for (const pkg of getWorkspacePackages(root)) {
    const hasPublicTag = pkg.tags.includes('npm:public');
    if (pkg.private && hasPublicTag) {
      console.error(`${pkg.path}: private package must not have nx tag npm:public`);
      failures++;
    }
  }
  if (failures > 0) {
    return failures;
  }
  console.log('npm:public tags are valid.');
  return 0;
}

const testFilePattern = /\.(test|spec)\.tsx?$/;

/** Directories that legitimately hold generated/vendored trees a test-file
 * scan must never descend into (cargo target/ alone holds 100k+ files). */
const testScanSkippedDirectories = new Set([
  'node_modules',
  'dist',
  'dist-test',
  'target',
  'coverage',
  'tmp',
  '.git',
  '.devenv',
  '.direnv',
  '.nx',
  '.cache',
]);

/**
 * Every test file must live under the package's `src/`. The convention is
 * otherwise only encoded generatively — tsconfig.test.json includes are
 * `src/**` and the bounded bun-test targets run with cwd `<package>/src` —
 * so a test file anywhere else is neither typechecked nor executed, with no
 * diagnostic. This check turns that silent gap into a validation failure.
 */
export function validateTestFileLocations(root: string): number {
  let failures = 0;
  for (const pkg of getWorkspacePackages(root)) {
    for (const stray of strayTestFiles(join(root, pkg.path))) {
      console.error(
        `${pkg.projectName}: test file outside src/ is neither typechecked nor run by the bounded test targets: ${stray}`,
      );
      failures++;
    }
  }
  return failures;
}

function strayTestFiles(packagePath: string): string[] {
  const stray: string[] = [];
  const walk = (directory: string): void => {
    let entries: Dirent[];
    try {
      entries = readdirSync(directory, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) {
        if (testScanSkippedDirectories.has(entry.name)) {
          continue;
        }
        if (directory === packagePath && entry.name === 'src') {
          continue; // src/ is where tests belong.
        }
        walk(path);
        continue;
      }
      if (entry.isFile() && testFilePattern.test(entry.name)) {
        stray.push(relative(packagePath, path));
      }
    }
  };
  walk(packagePath);
  return stray;
}

export function validatePublicPackageMetadata(root: string): number {
  const rootPackage = readJsonObject(join(root, 'package.json'));
  const rootRepository = rootPackage ? repositoryInfo(rootPackage) : null;
  let failures = 0;
  for (const pkg of listPublicPackages(root)) {
    if (pkg.private) {
      console.error(`${pkg.path}: npm:public package must not be private`);
      failures++;
    }
    if (!pkg.json.license) {
      console.error(`${pkg.path}: public package must define license`);
      failures++;
    }
    const publishConfig = pkg.json.publishConfig;
    if (publishConfig?.access !== 'public') {
      console.error(`${pkg.path}: public package must define publishConfig.access = public`);
      failures++;
    }
    const repository =
      pkg.json.repository !== null && pkg.json.repository !== undefined && typeof pkg.json.repository !== 'string'
        ? pkg.json.repository
        : null;
    const packageRepository = packageRepositoryInfo(pkg);
    if (!packageRepository) {
      console.error(`${pkg.path}: public package must define repository.url`);
      failures++;
    }
    if (
      rootRepository &&
      packageRepository &&
      packageRepository.url !== rootRepository.url &&
      sameRepositoryAfterNormalization(packageRepository.url, rootRepository.url)
    ) {
      console.error(
        `${pkg.path}: repository.url refers to the root repository but is not an exact match. ` +
          `Use ${rootRepository.url}`,
      );
      failures++;
    }
    if (!repository?.type) {
      console.error(`${pkg.path}: public package must define repository.type`);
      failures++;
    }
    if (!repository || repository.directory !== pkg.path.replaceAll('\\', '/')) {
      console.error(`${pkg.path}: public package repository.directory must be ${pkg.path.replaceAll('\\', '/')}`);
      failures++;
    }
    if (!Array.isArray(pkg.json.files)) {
      console.error(`${pkg.path}: public package must define files`);
      failures++;
    }
    const hasBin =
      typeof pkg.json.bin === 'string' ||
      (pkg.json.bin !== null && pkg.json.bin !== undefined && typeof pkg.json.bin === 'object');
    if (!isPackageExportMap(pkg.json.exports) && !hasBin) {
      console.error(`${pkg.path}: public package must define exports or bin`);
      failures++;
    }
    if (typeof pkg.json.types !== 'string' && !hasBin) {
      console.error(`${pkg.path}: public library package must define types`);
      failures++;
    }
  }
  return failures;
}

export function validateWorkspaceDependencies(root: string, options: PackageTargetPolicyOptions = {}): number {
  let failures = 0;
  failures += validateCiSkipTags(root, options);
  failures += validateCargoCrossLintTargets(root, options);
  const workspaceNames = new Set(getWorkspacePackages(root).map((pkg) => pkg.name));
  for (const pkg of listPackageJsonRecords(root)) {
    for (const field of workspaceDependencyFields) {
      const dependencies = pkg.json[field];
      if (!dependencies) {
        continue;
      }
      for (const [name, range] of Object.entries(dependencies)) {
        if (workspaceNames.has(name) && range !== 'workspace:*') {
          console.error(`${pkg.path}: ${field}.${name} must use workspace:*`);
          failures++;
        }
      }
    }
  }
  for (const issue of checkPackageTargetPolicy(root, options)) {
    console.error(`${issue.path}: ${issue.message}`);
    failures++;
  }
  for (const issue of checkTypecheckTestPolicy(root)) {
    console.error(`${issue.path}: ${issue.message}`);
    failures++;
  }
  for (const issue of checkWorkspaceBoundedTestTargetPolicy(root, options)) {
    console.error(`${issue.path}: ${issue.message}`);
    failures++;
  }
  if (failures === 0) {
    console.log('Workspace dependency policy is valid.');
  }
  return failures;
}

const ciSkipTagPrefix = 'ci:skip:';
const invalidCiSkipTargetSyntax = /[,*?[\]{}]/;

function validateCiSkipTags(root: string, options: PackageTargetPolicyOptions): number {
  let failures = 0;
  for (const pkg of getWorkspacePackages(root)) {
    const resolved = options.resolvedTargetsByProject?.get(pkg.projectName);
    const resolvedTargets = resolved && 'targets' in resolved ? resolved.targets : resolved;
    for (const tag of pkg.tags) {
      if (tag !== 'ci:skip' && !tag.startsWith(ciSkipTagPrefix)) {
        continue;
      }
      const target = tag.startsWith(ciSkipTagPrefix) ? tag.slice(ciSkipTagPrefix.length) : '';
      if (!target || invalidCiSkipTargetSyntax.test(target)) {
        console.error(`${pkg.path}: nx tag ${tag} must use ci:skip:<target> with one exact Nx target name`);
        failures++;
        continue;
      }
      if (resolvedTargets && !resolvedTargets.has(target)) {
        console.error(`${pkg.path}: nx tag ${tag} names missing Nx target ${target}`);
        failures++;
      }
    }
  }
  return failures;
}

/**
 * Every Cargo workspace must carry the Linux cross-lint target. The plugin
 * infers it, so an absence means the target was declared away, the plugin is
 * stale, or the project is no longer recognised as a Cargo workspace — and in all
 * three cases a Rust project silently stops being checked against the platform CI
 * actually validates on. That silence is the whole defect class this target
 * exists to end, so it is reported rather than repaired: which of the three
 * causes applies changes the correct fix, and guessing would hide the reason.
 *
 * Keyed off `cargo-lint`, the sibling target inferred from the same `[workspace]`
 * Cargo.toml, so this check needs no second opinion about what a Rust project is.
 */
function validateCargoCrossLintTargets(root: string, options: PackageTargetPolicyOptions): number {
  let failures = 0;
  for (const pkg of getWorkspacePackages(root)) {
    const resolved = options.resolvedTargetsByProject?.get(pkg.projectName);
    const resolvedTargets = resolved && 'targets' in resolved ? resolved.targets : resolved;
    if (!resolvedTargets?.has('cargo-lint') || resolvedTargets.has(CARGO_CROSS_LINT_TARGET)) {
      continue;
    }
    console.error(
      `${pkg.path}: Cargo workspace has cargo-lint but no ${CARGO_CROSS_LINT_TARGET}, so its Linux arm is never compiled. ` +
        `Remove any local ${CARGO_CROSS_LINT_TARGET} override, or rebuild @smoothbricks/nx-plugin if it is stale.`,
    );
    failures++;
  }
  return failures;
}

// ---------------------------------------------------------------------------
// Helpers kept in CLI (not Nx-specific)
// ---------------------------------------------------------------------------

function fixWorkspaceDependencyRanges(pkg: PackageJson, workspaceNames: Set<string>): boolean {
  let changed = false;
  for (const field of workspaceDependencyFields) {
    const dependencies = pkg[field];
    if (!dependencies) {
      continue;
    }
    for (const name of Object.keys(dependencies)) {
      if (workspaceNames.has(name) && dependencies[name] !== 'workspace:*') {
        dependencies[name] = 'workspace:*';
        changed = true;
      }
    }
  }
  return changed;
}

function validateRootScripts(rootPackage: PackageJson): number {
  const scripts = rootPackage.scripts;
  let failures = 0;
  for (const [name, command] of Object.entries(rootScriptPolicy)) {
    if (scripts?.[name] !== command) {
      console.error(`package.json scripts.${name} must be ${command}`);
      failures++;
    }
  }
  if (scripts && !recordKeysAreSorted(scripts)) {
    console.error(
      'package.json scripts must be sorted alphabetically so root command policy stays stable across fixes.',
    );
    failures++;
  }
  return failures;
}

function validateRootNxScriptInference(rootPackage: PackageJson): number {
  const includedScripts = rootPackage.nx?.includedScripts;
  if (Array.isArray(includedScripts) && includedScripts.length === 0) {
    return 0;
  }
  console.error('package.json nx.includedScripts must be [] so root scripts do not become recursive Nx targets.');
  return 1;
}

function suggestNxProjectName(rootPackageName: string, packageName: string): string | null {
  const rootScope = npmScope(rootPackageName);
  if (!rootScope || npmScope(packageName) !== rootScope) {
    return null;
  }
  return unscopedPackageName(packageName);
}

function npmScope(packageName: string): string | null {
  const match = /^(@[^/]+)\//.exec(packageName);
  return match?.[1] ?? null;
}

function unscopedPackageName(packageName: string): string {
  return packageName.startsWith('@') ? packageName.slice(packageName.indexOf('/') + 1) : packageName;
}

function recordKeysAreSorted(record: StringMap): boolean {
  const keys = Object.keys(record);
  return keys.every((key, index) => index === 0 || (keys[index - 1] ?? '') <= key);
}

function sortRecordInPlace(record: StringMap): boolean {
  if (recordKeysAreSorted(record)) {
    return false;
  }
  const entries = Object.entries(record).sort(([a], [b]) => a.localeCompare(b));
  for (const key of Object.keys(record)) {
    delete record[key];
  }
  for (const [key, value] of entries) {
    record[key] = value;
  }
  return true;
}

function isPackageExportMap(value: PackageExports): value is PackageExportMap {
  return value !== null && value !== undefined && typeof value === 'object';
}
function normalizeExportConditionOrder(value: PackageExports): boolean {
  if (!isPackageExportMap(value)) {
    return false;
  }
  let changed = false;
  for (const child of Object.values(value)) {
    changed = normalizeExportConditionOrder(child) || changed;
  }
  const keys = Object.keys(value);
  if (!keys.includes('types') && !keys.includes('default')) {
    return changed;
  }
  const ordered = [
    ...(keys.includes('types') ? ['types'] : []),
    ...keys.filter((key) => key !== 'types' && key !== 'default'),
    ...(keys.includes('default') ? ['default'] : []),
  ];
  if (keys.join('\n') === ordered.join('\n')) {
    return changed;
  }
  const entries = new Map(keys.map((key) => [key, value[key]]));
  for (const key of keys) {
    delete value[key];
  }
  for (const key of ordered) {
    value[key] = entries.get(key);
  }
  return true;
}

function hasDevelopmentSourceExport(value: PackageExports): boolean {
  if (!isPackageExportMap(value)) {
    return false;
  }
  for (const [key, child] of Object.entries(value)) {
    if ((key === 'development' || key === 'bun') && typeof child === 'string' && child.startsWith('./src/')) {
      return true;
    }
    if (hasDevelopmentSourceExport(child)) {
      return true;
    }
  }
  return false;
}

function addFileEntry(pkg: PackageJson, entry: string): boolean {
  const files = pkg.files;
  if (!Array.isArray(files) || files.includes(entry)) {
    return false;
  }
  const firstNegated = files.findIndex((file) => file.startsWith('!'));
  if (firstNegated === -1) {
    files.push(entry);
  } else {
    files.splice(firstNegated, 0, entry);
  }
  return true;
}
