import { join } from 'node:path';
import {
  applyWorkspaceBoundedTestTargetPolicy,
  checkWorkspaceBoundedTestTargetPolicy,
} from '@smoothbricks/nx-plugin/bounded-test-policy';
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
  getOrCreateRecord,
  hasOwnString,
  isRecord,
  readJsonObject,
  recordProperty,
  requiredJsonObject,
  setMissingStringProperty,
  setStringProperty,
  stringProperty,
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

const extraCommitScopes = ['release'];
const rootScriptPolicy: Record<string, string> = {
  lint: 'nx run-many -t lint',
  'lint:fix': 'git-format-staged --config tooling/git-hooks/git-format-staged.yml --unstaged',
  'format:staged': 'git-format-staged --config tooling/git-hooks/git-format-staged.yml',
  'format:changed': 'git-format-staged --config tooling/git-hooks/git-format-staged.yml --also-unstaged',
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
  const scripts = getOrCreateRecord(rootPackage, 'scripts');
  let changed = false;
  for (const [name, command] of Object.entries(rootScriptPolicy)) {
    changed = setStringProperty(scripts, name, command) || changed;
  }
  const nx = getOrCreateRecord(rootPackage, 'nx');
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
  const rootLicense = stringProperty(rootPackage, 'license');
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
      changed = setMissingStringProperty(pkg.json, 'license', rootLicense) || changed;
    }
    const publishConfig = getOrCreateRecord(pkg.json, 'publishConfig');
    changed = setStringProperty(publishConfig, 'access', 'public') || changed;

    const repository = getOrCreateRecord(pkg.json, 'repository');
    changed =
      setStringProperty(repository, 'type', existingRepository?.type ?? rootRepository?.type ?? 'git') || changed;
    if (existingRepository && !stringProperty(repository, 'url')) {
      changed = setStringProperty(repository, 'url', existingRepository.url) || changed;
    }
    changed = setStringProperty(repository, 'directory', pkg.path.replaceAll('\\', '/')) || changed;
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
  if (applyWorkspaceBoundedTestTargetPolicy(root)) {
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
  const rootName = stringProperty(rootPackage, 'name');
  if (!rootName) {
    return;
  }
  for (const pkg of getWorkspacePackageManifests(root)) {
    const suggestedName = suggestNxProjectName(rootName, pkg.name);
    if (!suggestedName) {
      continue;
    }
    const nx = getOrCreateRecord(pkg.json, 'nx');
    const changed = setStringProperty(nx, 'name', suggestedName);
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
  const rootName = rootPackage ? stringProperty(rootPackage, 'name') : null;
  const names: string[] = [];
  for (const pkg of getWorkspacePackageManifests(root)) {
    const nx = recordProperty(pkg.json, 'nx');
    const configuredName = nx ? stringProperty(nx, 'name') : null;
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
  if (!stringProperty(rootPackage, 'name')) {
    console.error('package.json must define name');
    failures++;
  }
  if (!stringProperty(rootPackage, 'license')) {
    console.error('package.json must define repo-wide license');
    failures++;
  }
  if (!repositoryInfo(rootPackage)) {
    console.error('package.json must define repository.url');
    failures++;
  }
  failures += validateRootScripts(rootPackage);
  failures += validateRootNxScriptInference(rootPackage);
  const packageManager = stringProperty(rootPackage, 'packageManager');
  if (!packageManager?.startsWith('bun@')) {
    console.error('package.json packageManager must use bun@<version>');
    failures++;
  }
  const bunVersion = packageManager?.startsWith('bun@') ? packageManager.slice('bun@'.length) : null;
  const devDependencies = recordProperty(rootPackage, 'devDependencies');
  if (!bunVersion || !devDependencies || devDependencies['@types/bun'] !== bunVersion) {
    console.error('package.json devDependencies.@types/bun must match packageManager bun version');
    failures++;
  }
  const engines = recordProperty(rootPackage, 'engines');
  if (!engines || !stringProperty(engines, 'node')) {
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
  const rootName = rootPackage ? stringProperty(rootPackage, 'name') : null;
  if (!rootName) {
    return 0;
  }
  let failures = 0;
  for (const pkg of getWorkspacePackageManifests(root)) {
    const suggestedName = suggestNxProjectName(rootName, pkg.name);
    if (!suggestedName) {
      continue;
    }
    const nx = recordProperty(pkg.json, 'nx');
    const configuredName = nx ? stringProperty(nx, 'name') : null;
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

export function validatePublicPackageMetadata(root: string): number {
  const rootPackage = readJsonObject(join(root, 'package.json'));
  const rootRepository = rootPackage ? repositoryInfo(rootPackage) : null;
  let failures = 0;
  for (const pkg of listPublicPackages(root)) {
    if (pkg.private) {
      console.error(`${pkg.path}: npm:public package must not be private`);
      failures++;
    }
    if (!stringProperty(pkg.json, 'license')) {
      console.error(`${pkg.path}: public package must define license`);
      failures++;
    }
    const publishConfig = recordProperty(pkg.json, 'publishConfig');
    if (!publishConfig || stringProperty(publishConfig, 'access') !== 'public') {
      console.error(`${pkg.path}: public package must define publishConfig.access = public`);
      failures++;
    }
    const repository = recordProperty(pkg.json, 'repository');
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
    if (!repository || !stringProperty(repository, 'type')) {
      console.error(`${pkg.path}: public package must define repository.type`);
      failures++;
    }
    if (!repository || stringProperty(repository, 'directory') !== pkg.path.replaceAll('\\', '/')) {
      console.error(`${pkg.path}: public package repository.directory must be ${pkg.path.replaceAll('\\', '/')}`);
      failures++;
    }
    if (!Array.isArray(pkg.json.files)) {
      console.error(`${pkg.path}: public package must define files`);
      failures++;
    }
    if (!isRecord(pkg.json.exports) && !isRecord(pkg.json.bin)) {
      console.error(`${pkg.path}: public package must define exports or bin`);
      failures++;
    }
    if (!hasOwnString(pkg.json, 'types') && !isRecord(pkg.json.bin)) {
      console.error(`${pkg.path}: public library package must define types`);
      failures++;
    }
  }
  return failures;
}

export function validateWorkspaceDependencies(root: string, options: PackageTargetPolicyOptions = {}): number {
  let failures = 0;
  const workspaceNames = new Set(getWorkspacePackages(root).map((pkg) => pkg.name));
  for (const pkg of listPackageJsonRecords(root)) {
    for (const field of workspaceDependencyFields) {
      const dependencies = recordProperty(pkg.json, field);
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
  for (const issue of checkWorkspaceBoundedTestTargetPolicy(root)) {
    console.error(`${issue.path}: ${issue.message}`);
    failures++;
  }
  if (failures === 0) {
    console.log('Workspace dependency policy is valid.');
  }
  return failures;
}

// ---------------------------------------------------------------------------
// Helpers kept in CLI (not Nx-specific)
// ---------------------------------------------------------------------------

function fixWorkspaceDependencyRanges(pkg: Record<string, unknown>, workspaceNames: Set<string>): boolean {
  let changed = false;
  for (const field of workspaceDependencyFields) {
    const dependencies = recordProperty(pkg, field);
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

function validateRootScripts(rootPackage: Record<string, unknown>): number {
  const scripts = recordProperty(rootPackage, 'scripts');
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

function validateRootNxScriptInference(rootPackage: Record<string, unknown>): number {
  const nx = recordProperty(rootPackage, 'nx');
  if (nx && Array.isArray(nx.includedScripts) && nx.includedScripts.length === 0) {
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

function recordKeysAreSorted(record: Record<string, unknown>): boolean {
  const keys = Object.keys(record);
  return keys.every((key, index) => index === 0 || keys[index - 1] <= key);
}

function sortRecordInPlace(record: Record<string, unknown>): boolean {
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

function normalizeExportConditionOrder(value: unknown): boolean {
  if (!isRecord(value)) {
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

function hasDevelopmentSourceExport(value: unknown): boolean {
  if (!isRecord(value)) {
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

function addFileEntry(pkg: Record<string, unknown>, entry: string): boolean {
  const files = pkg.files;
  if (!Array.isArray(files) || files.includes(entry)) {
    return false;
  }
  const firstNegated = files.findIndex((file) => typeof file === 'string' && file.startsWith('!'));
  if (firstNegated === -1) {
    files.push(entry);
  } else {
    files.splice(firstNegated, 0, entry);
  }
  return true;
}
