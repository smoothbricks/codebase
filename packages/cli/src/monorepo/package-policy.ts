import { join } from 'node:path';
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
  getWorkspacePackages,
  listPackageJsonRecords,
  listPublicPackages,
  repositoryInfo,
  workspaceDependencyFields,
} from '../lib/workspace.js';

export function applyPublicPackageDefaults(root: string): void {
  const rootPackage = requiredJsonObject(join(root, 'package.json'));
  const rootLicense = stringProperty(rootPackage, 'license');
  const rootRepository = repositoryInfo(rootPackage);
  if (!rootLicense) {
    throw new Error('Root package.json must define license before public package defaults can be applied.');
  }
  if (!rootRepository) {
    throw new Error('Root package.json must define repository.url before public package defaults can be applied.');
  }

  for (const pkg of listPublicPackages(root)) {
    let changed = false;
    changed = setMissingStringProperty(pkg.json, 'license', rootLicense) || changed;
    const publishConfig = getOrCreateRecord(pkg.json, 'publishConfig');
    changed = setStringProperty(publishConfig, 'access', 'public') || changed;

    const repository = getOrCreateRecord(pkg.json, 'repository');
    changed = setStringProperty(repository, 'type', rootRepository.type) || changed;
    changed = setStringProperty(repository, 'url', rootRepository.url) || changed;
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

export function applyWorkspaceDependencyDefaults(root: string): void {
  const workspaceNames = new Set(getWorkspacePackages(root).map((pkg) => pkg.name));
  for (const pkg of listPackageJsonRecords(root)) {
    const changed = fixWorkspaceDependencyRanges(pkg.json, workspaceNames);
    if (changed) {
      writeJsonObject(pkg.packageJsonPath, pkg.json);
      console.log(`updated        ${pkg.path}/package.json workspace dependency ranges`);
    } else {
      console.log(`unchanged      ${pkg.path}/package.json workspace dependency ranges`);
    }
  }
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
  const nxJson = readJsonObject(join(root, 'nx.json'));
  if (!nxJson) {
    console.error('nx.json not found or invalid');
    return 1;
  }
  const release = recordProperty(nxJson, 'release');
  const version = release ? recordProperty(release, 'version') : null;
  let failures = 0;
  if (!release) {
    console.error('nx.json release config is missing');
    failures++;
  }
  if (release && stringProperty(release, 'projectsRelationship') !== 'independent') {
    console.error('nx.json release.projectsRelationship must be independent');
    failures++;
  }
  if (!version) {
    console.error('nx.json release.version config is missing');
    failures++;
  }
  if (version && stringProperty(version, 'specifierSource') !== 'conventional-commits') {
    console.error('nx.json release.version.specifierSource must be conventional-commits');
    failures++;
  }
  if (version && !stringProperty(version, 'preVersionCommand')) {
    console.error('nx.json release.version.preVersionCommand must be defined');
    failures++;
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
    if (!pkg.private && !hasPublicTag) {
      console.error(`${pkg.path}: public package must have nx tag npm:public`);
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
    const packageRepository = repository ? repositoryInfo(pkg.json) : null;
    if (!rootRepository || !packageRepository || packageRepository.url !== rootRepository.url) {
      console.error(`${pkg.path}: public package repository.url must match root package.json repository.url`);
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

export function validateWorkspaceDependencies(root: string): number {
  const workspaceNames = new Set(getWorkspacePackages(root).map((pkg) => pkg.name));
  let failures = 0;
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
  if (failures === 0) {
    console.log('Workspace dependency ranges are valid.');
  }
  return failures;
}

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
