import { existsSync, readFileSync } from 'node:fs';
import { join, relative } from 'node:path';
import {
  getOrCreateRecord,
  hasOwn,
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

export interface WorkspaceDependencyDefaultOptions {
  resolvedTargetsByProject?: ReadonlyMap<string, ReadonlySet<string> | ResolvedProjectTargets>;
}

export interface ResolvedProjectTargets {
  targets: ReadonlySet<string>;
  buildDependsOn?: readonly string[];
}

export const SMOO_NX_VERSION_ACTIONS = '@smoothbricks/cli/nx-version-actions';
export const SMOO_NX_RELEASE_TAG_PATTERN = '{projectName}@{version}';
const extraCommitScopes = ['release'];
const rootScriptPolicy: Record<string, string> = {
  lint: 'nx run-many -t lint',
  'lint:fix': 'git-format-staged --config tooling/git-hooks/git-format-staged.yml --unstaged',
  'format:staged': 'git-format-staged --config tooling/git-hooks/git-format-staged.yml',
  'format:changed': 'git-format-staged --config tooling/git-hooks/git-format-staged.yml --also-unstaged',
};
const nxJsTypescriptPlugin = '@nx/js/typescript';
const smoothBricksNxPlugin = '@smoothbricks/nx-plugin';
const expectedSharedGlobalsNamedInput = ['{workspaceRoot}/.github/workflows/ci.yml'];
const defaultProductionNamedInput = [
  '{projectRoot}/src/**/*',
  '{projectRoot}/package.json',
  '!{projectRoot}/**/__tests__/**',
  '!{projectRoot}/**/*.test.*',
  '!{projectRoot}/**/*.spec.*',
];
const impreciseProductionInputs = new Set(['default', '{projectRoot}/**/*', '{projectRoot}/**']);

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
  const nxJsonPath = join(root, 'nx.json');
  const nxJson = readJsonObject(nxJsonPath);
  if (!nxJson) {
    return;
  }
  let changed = removeColonTargetDefaults(nxJson);
  changed = applyBuildTargetDefault(nxJson) || changed;
  changed = applyNamedInputDefaults(nxJson) || changed;
  const currentPlugins = Array.isArray(nxJson.plugins) ? nxJson.plugins : [];
  const nextPlugins = upsertNxPlugin(
    upsertNxPlugin(currentPlugins, expectedNxJsTypescriptPlugin()),
    smoothBricksNxPlugin,
  );
  if (JSON.stringify(currentPlugins) !== JSON.stringify(nextPlugins)) {
    nxJson.plugins = nextPlugins;
    changed = true;
  }
  if (changed) {
    writeJsonObject(nxJsonPath, nxJson);
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

export function applyWorkspaceDependencyDefaults(root: string, options: WorkspaceDependencyDefaultOptions = {}): void {
  const workspaceNames = new Set(getWorkspacePackages(root).map((pkg) => pkg.name));
  for (const pkg of listPackageJsonRecords(root)) {
    let changed = fixWorkspaceDependencyRanges(pkg.json, workspaceNames);
    const projectName = packageNxProjectName(pkg.json);
    const resolvedProject = projectName ? options.resolvedTargetsByProject?.get(projectName) : undefined;
    const resolvedTargets = resolvedProjectTargetNames(resolvedProject);
    changed = migratePackageColonTargets(pkg.json, resolvedTargets) || changed;
    changed = rewriteColonTargetDependenciesInPackage(pkg.json, resolvedTargets) || changed;
    changed = removePackageColonTargets(pkg.json) || changed;
    changed = removeRedundantNoopBuildTarget(pkg.json, resolvedProject) || changed;
    changed = applyPackageScriptPolicy(pkg.json, pkg.path, workspaceNames, { resolvedTargets }) || changed;
    if (changed) {
      writeJsonObject(pkg.packageJsonPath, pkg.json);
      console.log(`updated        ${pkg.path}/package.json workspace dependency policy`);
    } else {
      console.log(`unchanged      ${pkg.path}/package.json workspace dependency policy`);
    }
    applyBunTestTsconfigDefaults(root, pkg.path, pkg.json, workspaceNames);
    applyTsconfigTestReferenceDefaults(root, pkg.path);
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
  if (delete version.preVersionCommand) {
    changed = true;
  }
  const releaseTag = getOrCreateRecord(release, 'releaseTag');
  changed = setStringProperty(releaseTag, 'pattern', SMOO_NX_RELEASE_TAG_PATTERN) || changed;
  const changelog = getOrCreateRecord(release, 'changelog');
  changed = setBooleanProperty(changelog, 'workspaceChangelog', false) || changed;
  const projectChangelogs = getOrCreateRecord(changelog, 'projectChangelogs');
  changed = setBooleanProperty(projectChangelogs, 'createRelease', false) || changed;
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

export function applyNxProjectNameDefaults(root: string): void {
  const rootPackage = requiredJsonObject(join(root, 'package.json'));
  const rootName = stringProperty(rootPackage, 'name');
  if (!rootName) {
    return;
  }
  for (const pkg of getWorkspacePackages(root)) {
    const suggestedName = suggestNxProjectName(rootName, pkg.name);
    if (!suggestedName) {
      continue;
    }
    const changed = applyPackageNxConfig(pkg.json, { projectName: suggestedName }).changed;
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
  for (const pkg of getWorkspacePackages(root)) {
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
  const nxJson = readJsonObject(join(root, 'nx.json'));
  if (!nxJson) {
    console.error('nx.json not found or invalid');
    return 1;
  }
  const release = recordProperty(nxJson, 'release');
  const version = release ? recordProperty(release, 'version') : null;
  const releaseTag = release ? recordProperty(release, 'releaseTag') : null;
  const changelog = release ? recordProperty(release, 'changelog') : null;
  const projectChangelogs = changelog ? recordProperty(changelog, 'projectChangelogs') : null;
  const renderOptions = projectChangelogs ? recordProperty(projectChangelogs, 'renderOptions') : null;
  let failures = 0;
  if (!release) {
    console.error('nx.json release config is missing');
    failures++;
  }
  failures += validateNxPluginConfig(nxJson);
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
  if (version && stringProperty(version, 'preVersionCommand')) {
    console.error(
      'nx.json release.version.preVersionCommand must not be defined; smoo builds npm-missing packages before publish',
    );
    failures++;
  }
  if (!releaseTag) {
    console.error('nx.json release.releaseTag config is missing');
    failures++;
  }
  if (releaseTag && stringProperty(releaseTag, 'pattern') !== SMOO_NX_RELEASE_TAG_PATTERN) {
    console.error(`nx.json release.releaseTag.pattern must be ${SMOO_NX_RELEASE_TAG_PATTERN}`);
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
  if (projectChangelogs && projectChangelogs.createRelease !== false) {
    console.error('nx.json release.changelog.projectChangelogs.createRelease must be false');
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

export function validateNxProjectNames(root: string): number {
  const rootPackage = readJsonObject(join(root, 'package.json'));
  const rootName = rootPackage ? stringProperty(rootPackage, 'name') : null;
  if (!rootName) {
    return 0;
  }
  let failures = 0;
  for (const pkg of getWorkspacePackages(root)) {
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

export function validateWorkspaceDependencies(root: string, options: WorkspaceDependencyDefaultOptions = {}): number {
  const workspaceNames = new Set(getWorkspacePackages(root).map((pkg) => pkg.name));
  let failures = 0;
  for (const pkg of listPackageJsonRecords(root)) {
    const projectName = packageNxProjectName(pkg.json);
    const resolvedTargets = resolvedProjectTargetNames(
      projectName ? options.resolvedTargetsByProject?.get(projectName) : undefined,
    );
    failures += validateExplicitNxTargets(pkg.json, pkg.path, resolvedTargets);
    failures += validateBunTestTsconfigPresence(root, pkg.path, pkg.json);
    failures += validateTsconfigTestPolicy(root, pkg.path);
    failures += validateTsconfigTestReferencePolicy(root, pkg.path);
    failures += validateBuildZigPolicy(root, pkg.path);
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
    failures += validatePackageScriptPolicy(pkg.json, pkg.path, workspaceNames, { resolvedTargets });
  }
  if (failures === 0) {
    console.log('Workspace dependency policy is valid.');
  }
  return failures;
}

export function applyPackageScriptPolicy(
  pkg: Record<string, unknown>,
  _packagePath: string,
  workspaceNames: ReadonlySet<string>,
  options: { resolvedTargets?: ReadonlySet<string> } = {},
): boolean {
  if (!hasWorkspaceDependency(pkg, workspaceNames)) {
    return false;
  }
  const scripts = recordProperty(pkg, 'scripts');
  if (!scripts) {
    return false;
  }
  const projectName = packageNxProjectName(pkg);
  if (!projectName) {
    return false;
  }
  const nxConfig = applyPackageNxConfig(pkg, { projectName, targets: true });
  const targets = nxConfig.targets;
  let changed = nxConfig.changed;
  for (const [scriptName, rawCommand] of Object.entries(scripts)) {
    if (typeof rawCommand !== 'string') {
      continue;
    }
    const rewrite = classifyScriptRewrite(scriptName, rawCommand);
    if (!rewrite) {
      continue;
    }
    const targetName = rewrite.targetName;
    const alias = nxRunAlias(projectName, targetName, rewrite.continuous);
    const existingTarget = recordProperty(targets, targetName);
    if (
      !existingTarget &&
      targetName !== scriptName &&
      targetExistsInResolvedProject(targetName, options.resolvedTargets)
    ) {
      if (scripts[scriptName] !== alias) {
        scripts[scriptName] = alias;
        changed = true;
      }
      continue;
    }
    const existingOptions = existingTarget ? recordProperty(existingTarget, 'options') : null;
    const existingCommand = existingOptions ? stringProperty(existingOptions, 'command') : null;
    const command = isScriptRunnerCommand(existingCommand, scriptName)
      ? rewrite.command
      : (existingCommand ?? rewrite.command);
    const target = existingTarget ?? {};
    changed = setStringProperty(target, 'executor', 'nx:run-commands') || changed;
    changed = setStringArrayProperty(target, 'dependsOn', expectedTargetDependencies(targetName)) || changed;
    if (rewrite.continuous && target.continuous !== true) {
      target.continuous = true;
      changed = true;
    }
    const targetOptions = getOrCreateRecord(target, 'options');
    changed = setStringProperty(targetOptions, 'command', command) || changed;
    changed = setStringProperty(targetOptions, 'cwd', '{projectRoot}') || changed;
    for (const [name, value] of Object.entries(rewrite.env)) {
      const env = getOrCreateRecord(targetOptions, 'env');
      changed = setStringProperty(env, name, value) || changed;
    }
    if (targets[targetName] !== target) {
      targets[targetName] = target;
      changed = true;
    }
    if (scripts[scriptName] !== alias) {
      scripts[scriptName] = alias;
      changed = true;
    }
  }
  return changed;
}

export function validatePackageScriptPolicy(
  pkg: Record<string, unknown>,
  packagePath: string,
  workspaceNames: ReadonlySet<string>,
  options: { resolvedTargets?: ReadonlySet<string> } = {},
): number {
  if (!hasWorkspaceDependency(pkg, workspaceNames)) {
    return 0;
  }
  const scripts = recordProperty(pkg, 'scripts');
  if (!scripts) {
    return 0;
  }
  const nx = recordProperty(pkg, 'nx');
  const projectName = nx ? stringProperty(nx, 'name') : stringProperty(pkg, 'name');
  const targets = nx ? recordProperty(nx, 'targets') : null;
  let failures = 0;
  for (const [scriptName, rawCommand] of Object.entries(scripts)) {
    if (typeof rawCommand !== 'string') {
      continue;
    }
    const alias = parseNxRunAlias(rawCommand);
    const rewrite = alias
      ? { targetName: alias.targetName, continuous: isContinuousTarget(alias.targetName, '') }
      : classifyScriptRewrite(scriptName, rawCommand);
    if (!rewrite || (alias && projectName && alias.projectName !== projectName)) {
      if (alias && projectName && alias.projectName !== projectName) {
        console.error(`${packagePath}: scripts.${scriptName} must delegate to project ${projectName}`);
        failures++;
      }
      continue;
    }
    if (!projectName) {
      console.error(`${packagePath}: package scripts that use workspace dependencies require package.json nx.name`);
      failures++;
      continue;
    }
    const expectedAlias = nxRunAlias(projectName, rewrite.targetName, rewrite.continuous);
    if (rawCommand !== expectedAlias) {
      console.error(`${packagePath}: scripts.${scriptName} must delegate to ${expectedAlias}`);
      failures++;
      continue;
    }
    const target = targets ? recordProperty(targets, rewrite.targetName) : null;
    if (
      !target &&
      rewrite.targetName !== scriptName &&
      targetExistsInResolvedProject(rewrite.targetName, options.resolvedTargets)
    ) {
      continue;
    }
    if (rewrite.targetName.includes(':')) {
      continue;
    }
    const targetOptions = target ? recordProperty(target, 'options') : null;
    const command = targetOptions ? stringProperty(targetOptions, 'command') : null;
    if (!target || stringProperty(target, 'executor') !== 'nx:run-commands' || !targetOptions || !command) {
      console.error(`${packagePath}: nx.targets.${rewrite.targetName} must use nx:run-commands with options.command`);
      failures++;
      continue;
    }
    if (stringProperty(targetOptions, 'cwd') !== '{projectRoot}') {
      console.error(`${packagePath}: nx.targets.${rewrite.targetName}.options.cwd must be {projectRoot}`);
      failures++;
    }
    if (!targetDependsOn(target, expectedTargetDependencies(rewrite.targetName))) {
      console.error(
        `${packagePath}: nx.targets.${rewrite.targetName}.dependsOn must include ${expectedTargetDependencies(
          rewrite.targetName,
        ).join(', ')}`,
      );
      failures++;
    }
    if (rewrite.continuous && target.continuous !== true) {
      console.error(`${packagePath}: nx.targets.${rewrite.targetName}.continuous must be true`);
      failures++;
    }
    if (isScriptRunnerCommand(command, scriptName)) {
      console.error(
        `${packagePath}: nx.targets.${rewrite.targetName}.options.command must not call scripts.${scriptName}`,
      );
      failures++;
    }
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

function validateNxPluginConfig(nxJson: Record<string, unknown>): number {
  let failures = 0;
  const targetDefaults = recordProperty(nxJson, 'targetDefaults');
  if (targetDefaults) {
    for (const targetName of Object.keys(targetDefaults)) {
      if (targetName.includes(':')) {
        console.error(
          `nx.json targetDefaults.${targetName} must not use colon target names. ` +
            'Nx CLI syntax already uses project:target:configuration, so smoo Nx target names must be unambiguous tool-output names.',
        );
        failures++;
      }
    }
  }
  failures += validateBuildTargetDefault(nxJson);
  failures += validateNamedInputDefaults(nxJson);
  const plugins = Array.isArray(nxJson.plugins) ? nxJson.plugins : [];
  const nxJsPlugin = plugins.find(isNxJsTypescriptPlugin);
  if (!nxJsPlugin) {
    console.error(
      `nx.json plugins must configure ${nxJsTypescriptPlugin}. ` +
        'Official Nx owns TypeScript library inference; smoo configures it so tsconfig.lib.json produces tsc-js and leaves build available as an aggregate target.',
    );
    failures++;
  } else if (nxJsBuildTargetName(nxJsPlugin) !== 'tsc-js') {
    console.error(
      `nx.json ${nxJsTypescriptPlugin} build.targetName must be tsc-js. ` +
        'TypeScript library output is a concrete tool-output target; build is reserved for aggregate targets that depend on concrete build work.',
    );
    failures++;
  }
  if (!plugins.includes(smoothBricksNxPlugin) && !plugins.some(isSmoothBricksNxPluginRecord)) {
    console.error(
      `nx.json plugins must include ${smoothBricksNxPlugin}. ` +
        'Smoo relies on this plugin to infer convention targets that official Nx does not provide, including typecheck-tests, non-TypeScript build-tool targets, and aggregate build/lint targets.',
    );
    failures++;
  }
  return failures;
}

function validateExplicitNxTargets(
  pkg: Record<string, unknown>,
  packagePath: string,
  resolvedTargets?: ReadonlySet<string>,
): number {
  const nx = recordProperty(pkg, 'nx');
  const targets = nx ? recordProperty(nx, 'targets') : null;
  if (!targets) {
    return 0;
  }
  let failures = 0;
  for (const [targetName, rawTarget] of Object.entries(targets)) {
    if (targetName.includes(':')) {
      console.error(
        `${packagePath}: package.json nx.targets.${targetName} must not use colon target names. ` +
          'Nx CLI syntax already uses project:target:configuration; use a concrete tool-output target name and keep colon names only as package-script aliases.',
      );
      failures++;
    }
    if (!isRecord(rawTarget)) {
      continue;
    }
    failures += validateTargetDependencies(rawTarget, `${packagePath}: nx.targets.${targetName}`, resolvedTargets);
  }
  return failures;
}

function validateTargetDependencies(
  target: Record<string, unknown>,
  label: string,
  resolvedTargets?: ReadonlySet<string>,
): number {
  if (!Array.isArray(target.dependsOn)) {
    return 0;
  }
  let failures = 0;
  for (const dependency of target.dependsOn) {
    if (typeof dependency !== 'string') {
      continue;
    }
    if (dependency.includes(':')) {
      console.error(`${label}.dependsOn must not include colon target dependency ${dependency}`);
      failures++;
      continue;
    }
    if (
      label.endsWith('nx.targets.build') &&
      !dependency.startsWith('^') &&
      !targetExistsInResolvedProject(dependency, resolvedTargets)
    ) {
      console.error(`${label}.dependsOn references missing target ${dependency}`);
      failures++;
    }
  }
  return failures;
}

function validateTsconfigTestPolicy(root: string, packagePath: string): number {
  const path = join(root, packagePath, 'tsconfig.test.json');
  const tsconfig = readJsonObject(path);
  if (!tsconfig) {
    return 0;
  }
  const compilerOptions = recordProperty(tsconfig, 'compilerOptions');
  let failures = 0;
  if (!compilerOptions || compilerOptions.noEmit !== true) {
    console.error(`${packagePath}/tsconfig.test.json compilerOptions.noEmit must be true`);
    failures++;
  }
  if (compilerOptions?.composite === true) {
    console.error(
      `${packagePath}/tsconfig.test.json must not set compilerOptions.composite = true. ` +
        'Bun test typechecking is a no-emit validation pass, not a TypeScript build-mode project.',
    );
    failures++;
  }
  if (compilerOptions?.declaration === true) {
    console.error(`${packagePath}/tsconfig.test.json must not set compilerOptions.declaration = true`);
    failures++;
  }
  if (compilerOptions?.declarationMap === true) {
    console.error(`${packagePath}/tsconfig.test.json must not set compilerOptions.declarationMap = true`);
    failures++;
  }
  if (compilerOptions?.outDir === 'dist-test') {
    console.error(`${packagePath}/tsconfig.test.json must not emit to dist-test`);
    failures++;
  }
  if (typeof compilerOptions?.tsBuildInfoFile === 'string' && compilerOptions.tsBuildInfoFile.includes('dist-test')) {
    console.error(`${packagePath}/tsconfig.test.json must not write tsbuildinfo under dist-test`);
    failures++;
  }
  return failures;
}

function validateTsconfigTestReferencePolicy(root: string, packagePath: string): number {
  const testTsconfigPath = join(root, packagePath, 'tsconfig.test.json');
  if (!existsSync(testTsconfigPath)) {
    return 0;
  }
  const projectTsconfig = readJsonObject(join(root, packagePath, 'tsconfig.json'));
  if (!projectTsconfigHasTestReference(projectTsconfig)) {
    return 0;
  }
  console.error(
    `${packagePath}/tsconfig.json must not reference ./tsconfig.test.json. ` +
      'Test typechecking is run by the inferred typecheck-tests target with tsc --noEmit, not TypeScript build mode.',
  );
  return 1;
}

function validateBunTestTsconfigPresence(root: string, packagePath: string, pkg: Record<string, unknown>): number {
  if (!usesBunTest(pkg)) {
    return 0;
  }
  const path = join(root, packagePath, 'tsconfig.test.json');
  if (existsSync(path)) {
    return 0;
  }
  console.error(
    `${packagePath}: bun test requires tsconfig.test.json because Bun executes tests without typechecking. ` +
      'Run smoo monorepo validate --fix to create the no-emit test typecheck config.',
  );
  return 1;
}

function applyBunTestTsconfigDefaults(
  root: string,
  packagePath: string,
  pkg: Record<string, unknown>,
  workspaceNames: ReadonlySet<string>,
): void {
  if (!usesBunTest(pkg)) {
    return;
  }
  const tsconfigTestPath = join(root, packagePath, 'tsconfig.test.json');
  const existing = readJsonObject(tsconfigTestPath);
  const tsconfigTest = existing ?? {};
  let changed = existing === null;

  changed = applyTsconfigTestDefaults(root, packagePath, pkg, tsconfigTest, workspaceNames) || changed;
  if (changed) {
    writeJsonObject(tsconfigTestPath, tsconfigTest);
    console.log(`updated        ${packagePath}/tsconfig.test.json bun test typecheck config`);
  } else {
    console.log(`unchanged      ${packagePath}/tsconfig.test.json bun test typecheck config`);
  }
}

function applyTsconfigTestReferenceDefaults(root: string, packagePath: string): void {
  const projectTsconfigPath = join(root, packagePath, 'tsconfig.json');
  const projectTsconfig = readJsonObject(projectTsconfigPath);
  if (!projectTsconfig || !projectTsconfigHasTestReference(projectTsconfig)) {
    return;
  }
  const references = Array.isArray(projectTsconfig.references) ? projectTsconfig.references : [];
  projectTsconfig.references = references.filter((entry) => !isRecord(entry) || entry.path !== './tsconfig.test.json');
  writeJsonObject(projectTsconfigPath, projectTsconfig);
  console.log(`updated        ${packagePath}/tsconfig.json removed test project reference`);
}

function applyTsconfigTestDefaults(
  root: string,
  packagePath: string,
  pkg: Record<string, unknown>,
  tsconfigTest: Record<string, unknown>,
  workspaceNames: ReadonlySet<string>,
): boolean {
  let changed = setMissingStringProperty(tsconfigTest, 'extends', defaultTsconfigTestExtends(root, packagePath));
  const compilerOptions = getOrCreateRecord(tsconfigTest, 'compilerOptions');
  changed = copyLibCompilerOptions(root, packagePath, compilerOptions) || changed;
  changed = setBooleanProperty(compilerOptions, 'composite', false) || changed;
  changed = setBooleanProperty(compilerOptions, 'declaration', false) || changed;
  changed = setBooleanProperty(compilerOptions, 'declarationMap', false) || changed;
  changed = setBooleanProperty(compilerOptions, 'emitDeclarationOnly', false) || changed;
  changed = setBooleanProperty(compilerOptions, 'noEmit', true) || changed;
  changed = mergeStringListProperty(compilerOptions, 'types', ['bun']) || changed;
  if (delete compilerOptions.outDir) {
    changed = true;
  }
  if (delete compilerOptions.tsBuildInfoFile) {
    changed = true;
  }
  changed =
    mergeStringListProperty(tsconfigTest, 'include', [
      'src/**/*.test.ts',
      'src/**/*.spec.ts',
      'src/**/__tests__/**/*.ts',
      'src/**/__tests__/**/*.tsx',
      'src/test-suite-tracer.ts',
    ]) || changed;
  for (const referencePath of collectTsconfigTestReferencePaths(root, packagePath, pkg, workspaceNames)) {
    changed = addTsconfigReference(tsconfigTest, referencePath) || changed;
  }
  return changed;
}

function defaultTsconfigTestExtends(root: string, packagePath: string): string {
  const libTsconfig = readJsonObject(join(root, packagePath, 'tsconfig.lib.json'));
  return stringProperty(libTsconfig ?? {}, 'extends') ?? '../../tsconfig.base.json';
}

function copyLibCompilerOptions(root: string, packagePath: string, target: Record<string, unknown>): boolean {
  const libTsconfig = readJsonObject(join(root, packagePath, 'tsconfig.lib.json'));
  const libCompilerOptions = libTsconfig ? recordProperty(libTsconfig, 'compilerOptions') : null;
  if (!libCompilerOptions) {
    return false;
  }
  let changed = false;
  for (const key of ['baseUrl', 'module', 'moduleResolution', 'jsx', 'lib']) {
    if (hasOwn(libCompilerOptions, key) && target[key] !== libCompilerOptions[key]) {
      target[key] = libCompilerOptions[key];
      changed = true;
    }
  }
  return changed;
}

function collectTsconfigTestReferencePaths(
  root: string,
  packagePath: string,
  pkg: Record<string, unknown>,
  workspaceNames: ReadonlySet<string>,
): string[] {
  const paths = existsSync(join(root, packagePath, 'tsconfig.lib.json')) ? ['./tsconfig.lib.json'] : [];
  const packagesByName = new Map(getWorkspacePackages(root).map((workspacePkg) => [workspacePkg.name, workspacePkg]));
  for (const field of workspaceDependencyFields) {
    const dependencies = recordProperty(pkg, field);
    if (!dependencies) {
      continue;
    }
    for (const dependencyName of Object.keys(dependencies)) {
      if (!workspaceNames.has(dependencyName)) {
        continue;
      }
      const dependencyPackage = packagesByName.get(dependencyName);
      if (!dependencyPackage) {
        continue;
      }
      const dependencyTsconfig = join(root, dependencyPackage.path, 'tsconfig.lib.json');
      if (!existsSync(dependencyTsconfig)) {
        continue;
      }
      const refPath = relative(join(root, packagePath), dependencyTsconfig).replaceAll('\\', '/');
      if (!paths.includes(refPath)) {
        paths.push(refPath);
      }
    }
  }
  return paths;
}

function usesBunTest(pkg: Record<string, unknown>): boolean {
  const scripts = recordProperty(pkg, 'scripts');
  if (scripts && Object.values(scripts).some((command) => typeof command === 'string' && isBunTestCommand(command))) {
    return true;
  }
  const nx = recordProperty(pkg, 'nx');
  const targets = nx ? recordProperty(nx, 'targets') : null;
  if (!targets) {
    return false;
  }
  for (const target of Object.values(targets)) {
    if (!isRecord(target)) {
      continue;
    }
    const options = recordProperty(target, 'options');
    const command = options ? stringProperty(options, 'command') : null;
    if (command && isBunTestCommand(command)) {
      return true;
    }
  }
  return false;
}

function isBunTestCommand(command: string): boolean {
  return /^bun\s+test(?:\s|$)/.test(parseEnvPrefixedCommand(command).command.trim());
}

function mergeStringListProperty(record: Record<string, unknown>, key: string, values: string[]): boolean {
  const rawCurrent = record[key];
  const current = Array.isArray(rawCurrent)
    ? rawCurrent.filter((entry): entry is string => typeof entry === 'string')
    : [];
  const next = [...current];
  for (const value of values) {
    if (!next.includes(value)) {
      next.push(value);
    }
  }
  if (
    Array.isArray(rawCurrent) &&
    next.length === rawCurrent.length &&
    next.every((entry, index) => entry === rawCurrent[index])
  ) {
    return false;
  }
  record[key] = next;
  return true;
}

function addTsconfigReference(tsconfig: Record<string, unknown>, path: string): boolean {
  const current = Array.isArray(tsconfig.references)
    ? tsconfig.references.filter((entry): entry is Record<string, unknown> => isRecord(entry))
    : [];
  if (current.some((entry) => entry.path === path)) {
    return false;
  }
  tsconfig.references = [...current, { path }];
  return true;
}

function projectTsconfigHasTestReference(projectTsconfig: Record<string, unknown> | null): boolean {
  return Boolean(
    projectTsconfig &&
      Array.isArray(projectTsconfig.references) &&
      projectTsconfig.references.some((entry) => isRecord(entry) && entry.path === './tsconfig.test.json'),
  );
}

function validateBuildZigPolicy(root: string, packagePath: string): number {
  const path = join(root, packagePath, 'build.zig');
  if (!existsSync(path)) {
    return 0;
  }
  if (/\bb\.step\s*\(/.test(readFileSync(path, 'utf8'))) {
    return 0;
  }
  console.error(`${packagePath}/build.zig must define at least one b.step(...) target`);
  return 1;
}

function applyBuildTargetDefault(nxJson: Record<string, unknown>): boolean {
  const targetDefaults = getOrCreateRecord(nxJson, 'targetDefaults');
  const build = getOrCreateRecord(targetDefaults, 'build');
  let changed = setBooleanProperty(build, 'cache', true);
  changed = setStringArrayProperty(build, 'outputs', ['{projectRoot}/dist']) || changed;
  return changed;
}

function validateBuildTargetDefault(nxJson: Record<string, unknown>): number {
  const targetDefaults = recordProperty(nxJson, 'targetDefaults');
  const build = targetDefaults ? recordProperty(targetDefaults, 'build') : null;
  let failures = 0;
  if (!build || build.cache !== true) {
    console.error('nx.json targetDefaults.build.cache must be true');
    failures++;
  }
  const outputs = build?.outputs;
  if (!Array.isArray(outputs) || outputs.length !== 1 || outputs[0] !== '{projectRoot}/dist') {
    console.error('nx.json targetDefaults.build.outputs must be ["{projectRoot}/dist"]');
    failures++;
  }
  return failures;
}

function applyNamedInputDefaults(nxJson: Record<string, unknown>): boolean {
  const namedInputs = getOrCreateRecord(nxJson, 'namedInputs');
  let changed = false;
  if (!Array.isArray(namedInputs.default)) {
    namedInputs.default = ['{projectRoot}/**/*', 'sharedGlobals'];
    changed = true;
  }
  changed = setStringArrayProperty(namedInputs, 'sharedGlobals', expectedSharedGlobalsNamedInput) || changed;
  const production = namedInputs.production;
  if (!Array.isArray(production) || !isPreciseProductionNamedInput(production)) {
    namedInputs.production = defaultProductionNamedInput;
    changed = true;
  }
  return changed;
}

function validateNamedInputDefaults(nxJson: Record<string, unknown>): number {
  const namedInputs = recordProperty(nxJson, 'namedInputs');
  const production = namedInputs?.production;
  let failures = 0;
  if (!namedInputs) {
    console.error('nx.json namedInputs must be configured so production builds have precise cache inputs.');
    return 1;
  }
  if (!Array.isArray(namedInputs.default)) {
    console.error(
      'nx.json namedInputs.default must be an array; smoo allows it to remain broad for non-production tasks.',
    );
    failures++;
  }
  if (!stringArrayEquals(namedInputs.sharedGlobals, expectedSharedGlobalsNamedInput)) {
    console.error('nx.json namedInputs.sharedGlobals must include only {workspaceRoot}/.github/workflows/ci.yml');
    failures++;
  }
  if (!Array.isArray(production)) {
    console.error('nx.json namedInputs.production must be an array of precise production inputs.');
    return failures + 1;
  }
  if (!isPreciseProductionNamedInput(production)) {
    console.error(
      'nx.json namedInputs.production must enumerate precise production inputs. Do not include default or broad {projectRoot}/** globs; use language/tool-specific paths such as {projectRoot}/src/**/*, {projectRoot}/Cargo.toml, or {projectRoot}/pyproject.toml.',
    );
    failures++;
  }
  return failures;
}

function isPreciseProductionNamedInput(production: unknown[]): boolean {
  let hasPositiveProjectInput = false;
  for (const input of production) {
    if (typeof input !== 'string') {
      return false;
    }
    const normalized = input.startsWith('!') ? input.slice(1) : input;
    if (impreciseProductionInputs.has(input) || impreciseProductionInputs.has(normalized)) {
      return false;
    }
    if (!input.startsWith('!') && normalized.startsWith('{projectRoot}/')) {
      hasPositiveProjectInput = true;
    }
  }
  return hasPositiveProjectInput;
}

function removeColonTargetDefaults(nxJson: Record<string, unknown>): boolean {
  const targetDefaults = recordProperty(nxJson, 'targetDefaults');
  if (!targetDefaults) {
    return false;
  }
  let changed = false;
  for (const targetName of Object.keys(targetDefaults)) {
    if (targetName.includes(':')) {
      delete targetDefaults[targetName];
      changed = true;
    }
  }
  return changed;
}

function rewriteColonTargetDependenciesInPackage(
  pkg: Record<string, unknown>,
  resolvedTargets?: ReadonlySet<string>,
): boolean {
  const nx = recordProperty(pkg, 'nx');
  const targets = nx ? recordProperty(nx, 'targets') : null;
  if (!targets) {
    return false;
  }
  const projectName = packageNxProjectName(pkg);
  const scriptTargetAliases = scriptTargetAliasesForProject(pkg, projectName);
  let changed = false;
  for (const target of Object.values(targets)) {
    if (!isRecord(target) || !Array.isArray(target.dependsOn)) {
      continue;
    }
    target.dependsOn = target.dependsOn.map((dependency) => {
      if (typeof dependency !== 'string' || !dependency.includes(':')) {
        return dependency;
      }
      const next = scriptTargetAliases.get(dependency) ?? replacementTargetName(dependency, null, resolvedTargets);
      if (!next) {
        return dependency;
      }
      changed = true;
      return next;
    });
  }
  return changed;
}

function scriptTargetAliasesForProject(
  pkg: Record<string, unknown>,
  projectName: string | null,
): ReadonlyMap<string, string> {
  if (!projectName) {
    return new Map();
  }
  const scripts = recordProperty(pkg, 'scripts');
  if (!scripts) {
    return new Map();
  }
  const aliases = new Map<string, string>();
  for (const [scriptName, rawCommand] of Object.entries(scripts)) {
    if (typeof rawCommand !== 'string' || !scriptName.includes(':')) {
      continue;
    }
    const alias = parseNxRunAlias(rawCommand);
    if (alias?.projectName === projectName && !alias.targetName.includes(':')) {
      aliases.set(scriptName, alias.targetName);
    }
  }
  return aliases;
}

function removePackageColonTargets(pkg: Record<string, unknown>): boolean {
  const nx = recordProperty(pkg, 'nx');
  const targets = nx ? recordProperty(nx, 'targets') : null;
  if (!targets) {
    return false;
  }
  let changed = false;
  for (const targetName of Object.keys(targets)) {
    if (targetName.includes(':')) {
      delete targets[targetName];
      changed = true;
    }
  }
  return changed;
}

function resolvedProjectTargetNames(
  resolvedProject?: ReadonlySet<string> | ResolvedProjectTargets,
): ReadonlySet<string> | undefined {
  if (!resolvedProject) {
    return undefined;
  }
  return isResolvedProjectTargets(resolvedProject) ? resolvedProject.targets : resolvedProject;
}

function resolvedProjectBuildDependsOn(
  resolvedProject?: ReadonlySet<string> | ResolvedProjectTargets,
): readonly string[] | undefined {
  if (!isResolvedProjectTargets(resolvedProject)) {
    return undefined;
  }
  return resolvedProject.buildDependsOn;
}

function isResolvedProjectTargets(
  value: ReadonlySet<string> | ResolvedProjectTargets | undefined,
): value is ResolvedProjectTargets {
  return isRecord(value) && value.targets instanceof Set;
}

function removeRedundantNoopBuildTarget(
  pkg: Record<string, unknown>,
  resolvedProject?: ReadonlySet<string> | ResolvedProjectTargets,
): boolean {
  const resolvedTargets = resolvedProjectTargetNames(resolvedProject);
  const resolvedBuildDependsOn = resolvedProjectBuildDependsOn(resolvedProject);
  if (!resolvedTargets?.has('build') || !resolvedBuildDependsOn) {
    return false;
  }
  const nx = recordProperty(pkg, 'nx');
  const targets = nx ? recordProperty(nx, 'targets') : null;
  const build = targets ? recordProperty(targets, 'build') : null;
  if (
    !targets ||
    !build ||
    !isNoopTarget(build) ||
    !targetDependenciesMatchResolvedBuild(build, resolvedBuildDependsOn)
  ) {
    return false;
  }
  delete targets.build;
  return true;
}

function isNoopTarget(target: Record<string, unknown>): boolean {
  const executor = stringProperty(target, 'executor');
  if (executor !== null && executor !== 'nx:noop') {
    return false;
  }
  const options = recordProperty(target, 'options');
  return !options || stringProperty(options, 'command') === null;
}

function targetDependenciesMatchResolvedBuild(
  target: Record<string, unknown>,
  resolvedBuildDependsOn: readonly string[],
): boolean {
  if (!Array.isArray(target.dependsOn)) {
    return false;
  }
  const expected = new Set(resolvedBuildDependsOn);
  if (target.dependsOn.length !== expected.size) {
    return false;
  }
  return target.dependsOn.every((dependency) => {
    if (typeof dependency !== 'string') {
      return false;
    }
    return expected.has(dependency);
  });
}

function migratePackageColonTargets(pkg: Record<string, unknown>, resolvedTargets?: ReadonlySet<string>): boolean {
  const nx = recordProperty(pkg, 'nx');
  const targets = nx ? recordProperty(nx, 'targets') : null;
  if (!targets) {
    return false;
  }
  const projectName = packageNxProjectName(pkg);
  const scripts = recordProperty(pkg, 'scripts');
  let changed = false;
  const renamedTargets = new Map<string, string>();

  for (const [targetName, rawTarget] of Object.entries(targets)) {
    if (!targetName.includes(':') || !isRecord(rawTarget)) {
      continue;
    }
    const nextTargetName = replacementTargetName(targetName, targetCommand(rawTarget), resolvedTargets);
    if (!nextTargetName || nextTargetName.includes(':')) {
      continue;
    }
    if (!targetExistsInResolvedProject(nextTargetName, resolvedTargets)) {
      targets[nextTargetName] = rawTarget;
    }
    delete targets[targetName];
    renamedTargets.set(targetName, nextTargetName);
    changed = true;
  }

  if (renamedTargets.size === 0) {
    return changed;
  }

  for (const target of Object.values(targets)) {
    if (isRecord(target)) {
      changed = rewriteTargetDependencies(target, renamedTargets) || changed;
    }
  }

  if (scripts && projectName) {
    for (const [scriptName, rawCommand] of Object.entries(scripts)) {
      if (typeof rawCommand !== 'string') {
        continue;
      }
      const alias = parseNxRunAlias(rawCommand);
      if (!alias || alias.projectName !== projectName) {
        continue;
      }
      const nextTargetName = renamedTargets.get(alias.targetName);
      if (!nextTargetName) {
        continue;
      }
      scripts[scriptName] = nxRunAlias(projectName, nextTargetName, isContinuousTarget(nextTargetName, ''));
      changed = true;
    }
  }

  return changed;
}

function rewriteTargetDependencies(
  target: Record<string, unknown>,
  renamedTargets: ReadonlyMap<string, string>,
): boolean {
  if (!Array.isArray(target.dependsOn)) {
    return false;
  }
  let changed = false;
  target.dependsOn = target.dependsOn.map((dependency) => {
    if (typeof dependency !== 'string') {
      return dependency;
    }
    const next = renamedTargets.get(dependency);
    if (!next) {
      return dependency;
    }
    changed = true;
    return next;
  });
  return changed;
}

function targetCommand(target: Record<string, unknown>): string | null {
  const options = recordProperty(target, 'options');
  return options ? stringProperty(options, 'command') : null;
}

function replacementTargetName(
  targetName: string,
  command: string | null,
  resolvedTargets?: ReadonlySet<string>,
): string | null {
  if (command) {
    const commandTargetName = targetNameForCommand(command);
    if (commandTargetName) {
      return commandTargetName;
    }
  }
  const suffix = targetName.slice(targetName.lastIndexOf(':') + 1);
  if (suffix && targetExistsInResolvedProject(suffix, resolvedTargets)) {
    return suffix;
  }
  const dashed = targetName.replaceAll(':', '-');
  if (targetExistsInResolvedProject(dashed, resolvedTargets)) {
    return dashed;
  }
  return null;
}

function expectedNxJsTypescriptPlugin(): Record<string, unknown> {
  return {
    plugin: nxJsTypescriptPlugin,
    options: {
      typecheck: { targetName: 'typecheck' },
      build: {
        targetName: 'tsc-js',
        configName: 'tsconfig.lib.json',
        buildDepsName: 'build-deps',
        watchDepsName: 'watch-deps',
      },
    },
  };
}

function upsertNxPlugin(plugins: readonly unknown[], plugin: string | Record<string, unknown>): unknown[] {
  const pluginName = typeof plugin === 'string' ? plugin : stringProperty(plugin, 'plugin');
  const next = plugins.filter((entry) => nxPluginName(entry) !== pluginName);
  next.push(plugin);
  return next;
}

function nxPluginName(value: unknown): string | null {
  if (typeof value === 'string') {
    return value;
  }
  return isRecord(value) ? stringProperty(value, 'plugin') : null;
}

function isNxJsTypescriptPlugin(value: unknown): value is Record<string, unknown> {
  return isRecord(value) && stringProperty(value, 'plugin') === nxJsTypescriptPlugin;
}

function isSmoothBricksNxPluginRecord(value: unknown): boolean {
  return isRecord(value) && stringProperty(value, 'plugin') === smoothBricksNxPlugin;
}

function nxJsBuildTargetName(plugin: Record<string, unknown>): string | null {
  const options = recordProperty(plugin, 'options');
  const build = options ? recordProperty(options, 'build') : null;
  return build ? stringProperty(build, 'targetName') : null;
}

interface PackageNxConfig {
  nx: Record<string, unknown>;
  targets: Record<string, unknown> | null;
  changed: boolean;
}

interface PackageNxTargetsConfig {
  nx: Record<string, unknown>;
  targets: Record<string, unknown>;
  changed: boolean;
}

function applyPackageNxConfig(
  pkg: Record<string, unknown>,
  options: { projectName: string; targets: true },
): PackageNxTargetsConfig;
function applyPackageNxConfig(pkg: Record<string, unknown>, options: { projectName: string }): PackageNxConfig;
function applyPackageNxConfig(
  pkg: Record<string, unknown>,
  options: { projectName: string; targets?: boolean },
): PackageNxConfig {
  const nx = getOrCreateRecord(pkg, 'nx');
  const changed = setStringProperty(nx, 'name', options.projectName);
  const targets = options.targets === true ? getOrCreateRecord(nx, 'targets') : null;
  return { nx, targets, changed };
}

function packageNxProjectName(pkg: Record<string, unknown>): string | null {
  const nx = recordProperty(pkg, 'nx');
  return (nx ? stringProperty(nx, 'name') : null) ?? stringProperty(pkg, 'name');
}

interface ScriptRewrite {
  targetName: string;
  continuous: boolean;
  command: string;
  env: Record<string, string>;
}

function classifyScriptRewrite(scriptName: string, command: string): ScriptRewrite | null {
  if (isNxRunAlias(command) || isBlockedScriptCommand(scriptName, command)) {
    return null;
  }
  const parsed = parseEnvPrefixedCommand(command);
  const targetName = rewriteTargetName(scriptName, parsed.command);
  if (!targetName || !isSafeNxScriptCommand(parsed.command)) {
    return null;
  }
  return {
    targetName,
    continuous: isContinuousTarget(targetName, parsed.command),
    command: parsed.command,
    env: parsed.env,
  };
}

function rewriteTargetName(scriptName: string, command: string): string | null {
  return targetNameForCommand(command) ?? (scriptName.includes(':') ? null : scriptName);
}

function targetNameForCommand(command: string): string | null {
  const trimmed = command.trim();
  if (/^tsc\s+--build\s+tsconfig\.lib\.json(?:\s|$)/.test(trimmed)) {
    return 'tsc-js';
  }
  const zigStep = /^zig\s+build\s+([A-Za-z0-9_-]+)(?:\s|$)/.exec(trimmed)?.[1];
  if (zigStep) {
    return `zig-${zigStep}`;
  }
  if (/^wrangler\s+build(?:\s|$)/.test(trimmed)) {
    return 'build';
  }
  return null;
}

function nxRunAlias(projectName: string, targetName: string, continuous: boolean): string {
  const flags = continuous ? ' --tui=false --outputStyle=stream' : '';
  return `nx run ${projectName}:${targetName}${flags}`;
}

function expectedTargetDependencies(targetName: string): string[] {
  return targetName === 'preview' ? ['build'] : ['^build'];
}

function targetExistsInResolvedProject(targetName: string, resolvedTargets?: ReadonlySet<string>): boolean {
  return resolvedTargets?.has(targetName) === true;
}

function setStringArrayProperty(record: Record<string, unknown>, key: string, value: string[]): boolean {
  const current = record[key];
  if (stringArrayEquals(current, value)) {
    return false;
  }
  record[key] = value;
  return true;
}

function stringArrayEquals(value: unknown, expected: readonly string[]): boolean {
  return (
    Array.isArray(value) && value.length === expected.length && value.every((entry, index) => entry === expected[index])
  );
}

function targetDependsOn(target: Record<string, unknown>, expected: string[]): boolean {
  const dependsOn = target.dependsOn;
  return Array.isArray(dependsOn) && expected.every((entry) => dependsOn.includes(entry));
}

function hasWorkspaceDependency(pkg: Record<string, unknown>, workspaceNames: ReadonlySet<string>): boolean {
  for (const field of workspaceDependencyFields) {
    const dependencies = recordProperty(pkg, field);
    if (!dependencies) {
      continue;
    }
    for (const name of Object.keys(dependencies)) {
      if (workspaceNames.has(name)) {
        return true;
      }
    }
  }
  return false;
}

function isSafeNxScriptCommand(command: string): boolean {
  const trimmed = command.trim();
  return (
    /^bun\s+test(?:\s|$)/.test(trimmed) ||
    /^tsc-bun-test(?:\s|$)/.test(trimmed) ||
    /^vitest(?:\s|$)/.test(trimmed) ||
    /^tsc\s+(?:--build|--noEmit)(?:\s|$)/.test(trimmed) ||
    /^tsdown(?:\s|$)/.test(trimmed) ||
    /^vite\s+(?:build|dev|preview)(?:\s|$)/.test(trimmed) ||
    /^astro\s+(?:build|dev|preview|check)(?:\s|$)/.test(trimmed) ||
    /^zig\s+build(?:\s|$)/.test(trimmed) ||
    /^bun\s+[./\w-]*build[\w.-]*\.ts(?:\s|$)/.test(trimmed) ||
    /(?:^|\s)(?:bench|benchmark)(?:\s|$)/.test(trimmed) ||
    /^wrangler\s+build(?:\s|$)/.test(trimmed)
  );
}

function isBlockedScriptCommand(scriptName: string, command: string): boolean {
  if (/^(?:deploy|db|release|sync|subtree|publish|pack)(?::|$)/.test(scriptName)) {
    return true;
  }
  const trimmed = parseEnvPrefixedCommand(command).command;
  return /^(?:deploy|db|release|sync|subtree|publish|pack)(?:\s|$)/.test(trimmed) || trimmed === 'astro';
}

function parseEnvPrefixedCommand(command: string): { command: string; env: Record<string, string> } {
  const env: Record<string, string> = {};
  let rest = command.trimStart();
  while (true) {
    const match = /^([A-Za-z_][A-Za-z0-9_]*)=/.exec(rest);
    if (!match?.[1]) {
      return { command: rest.trim(), env };
    }
    let index = match[0].length;
    let value = '';
    const quote = rest[index];
    if (quote === '"' || quote === "'") {
      index += 1;
      const end = rest.indexOf(quote, index);
      if (end === -1) {
        return { command: command.trim(), env: {} };
      }
      value = rest.slice(index, end);
      index = end + 1;
    } else {
      const end = rest.slice(index).search(/\s/);
      const valueEnd = end === -1 ? rest.length : index + end;
      value = rest.slice(index, valueEnd);
      index = valueEnd;
    }
    if (index < rest.length && !/\s/.test(rest[index] ?? '')) {
      return { command: command.trim(), env: {} };
    }
    env[match[1]] = value;
    rest = rest.slice(index).trimStart();
  }
}

function isContinuousTarget(targetName: string, command: string): boolean {
  return (
    /(?:^|:|-)(?:dev|serve|preview|watch)(?:$|:|-)/.test(targetName) ||
    /(?:^|\s)(?:dev|serve|preview|--watch|-w)(?:\s|$)/.test(command)
  );
}

function isNxRunAlias(command: string): boolean {
  return /^nx\s+run\s+\S+:\S+/.test(command.trim());
}

function parseNxRunAlias(command: string): { projectName: string; targetName: string } | null {
  const match = /^nx\s+run\s+([^:\s]+):([^\s]+)(?:\s|$)/.exec(command.trim());
  if (!match?.[1] || !match[2]) {
    return null;
  }
  return { projectName: match[1], targetName: match[2] };
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

function isScriptRunnerCommand(command: string | null, scriptName: string): boolean {
  if (!command) {
    return false;
  }
  const escaped = escapeRegex(scriptName);
  return new RegExp(`^(?:bun|npm)\\s+run\\s+${escaped}(?:\\s|$)`).test(command.trim());
}

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
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
