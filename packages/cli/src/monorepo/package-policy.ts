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
  packageRepositoryInfo,
  repositoryInfo,
  sameRepositoryAfterNormalization,
  workspaceDependencyFields,
} from '../lib/workspace.js';

export const SMOO_NX_VERSION_ACTIONS = '@smoothbricks/cli/nx-version-actions';

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

export function applyNxReleaseDefaults(root: string): void {
  const nxJsonPath = join(root, 'nx.json');
  const nxJson = requiredJsonObject(nxJsonPath);
  let changed = false;
  const release = getOrCreateRecord(nxJson, 'release');
  changed = setStringProperty(release, 'projectsRelationship', 'independent') || changed;
  const version = getOrCreateRecord(release, 'version');
  changed = setStringProperty(version, 'specifierSource', 'conventional-commits') || changed;
  changed = setStringProperty(version, 'currentVersionResolver', 'git-tag') || changed;
  changed = setStringProperty(version, 'fallbackCurrentVersionResolver', 'disk') || changed;
  changed = setStringProperty(version, 'versionActions', SMOO_NX_VERSION_ACTIONS) || changed;
  changed = setMissingStringProperty(version, 'preVersionCommand', 'nx run-many -t build') || changed;
  const changelog = getOrCreateRecord(release, 'changelog');
  changed = setBooleanProperty(changelog, 'workspaceChangelog', false) || changed;
  const projectChangelogs = getOrCreateRecord(changelog, 'projectChangelogs');
  changed = setStringProperty(projectChangelogs, 'createRelease', 'github') || changed;
  changed = setBooleanProperty(projectChangelogs, 'file', false) || changed;
  const renderOptions = getOrCreateRecord(projectChangelogs, 'renderOptions');
  if (typeof renderOptions.authors !== 'boolean') {
    renderOptions.authors = true;
    changed = true;
  }
  if (typeof renderOptions.applyUsernameToAuthors !== 'boolean') {
    renderOptions.applyUsernameToAuthors = true;
    changed = true;
  }

  if (changed) {
    writeJsonObject(nxJsonPath, nxJson);
    console.log('updated        nx.json release config');
  } else {
    console.log('unchanged      nx.json release config');
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
  const changelog = release ? recordProperty(release, 'changelog') : null;
  const projectChangelogs = changelog ? recordProperty(changelog, 'projectChangelogs') : null;
  const renderOptions = projectChangelogs ? recordProperty(projectChangelogs, 'renderOptions') : null;
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
  // Nx requires git-tag as the primary resolver when deriving versions from
  // conventional commits. Disk is only a fallback for initial untagged packages.
  if (version && stringProperty(version, 'currentVersionResolver') !== 'git-tag') {
    console.error('nx.json release.version.currentVersionResolver must be git-tag');
    failures++;
  }
  if (version && stringProperty(version, 'fallbackCurrentVersionResolver') !== 'disk') {
    console.error('nx.json release.version.fallbackCurrentVersionResolver must be disk');
    failures++;
  }
  if (version && stringProperty(version, 'versionActions') !== SMOO_NX_VERSION_ACTIONS) {
    console.error(`nx.json release.version.versionActions must be ${SMOO_NX_VERSION_ACTIONS}`);
    failures++;
  }
  if (version && !stringProperty(version, 'preVersionCommand')) {
    console.error('nx.json release.version.preVersionCommand must be defined');
    failures++;
  }
  if (!changelog) {
    console.error('nx.json release.changelog config is missing');
    failures++;
  }
  if (changelog && changelog.workspaceChangelog !== false) {
    console.error('nx.json release.changelog.workspaceChangelog must be false');
    failures++;
  }
  if (!projectChangelogs) {
    console.error('nx.json release.changelog.projectChangelogs config is missing');
    failures++;
  }
  if (projectChangelogs && stringProperty(projectChangelogs, 'createRelease') !== 'github') {
    console.error('nx.json release.changelog.projectChangelogs.createRelease must be github');
    failures++;
  }
  if (projectChangelogs && projectChangelogs.file !== false) {
    console.error('nx.json release.changelog.projectChangelogs.file must be false');
    failures++;
  }
  if (!renderOptions) {
    console.error('nx.json release.changelog.projectChangelogs.renderOptions config is missing');
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

function setBooleanProperty(record: Record<string, unknown>, key: string, value: boolean): boolean {
  if (record[key] === value) {
    return false;
  }
  record[key] = value;
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
