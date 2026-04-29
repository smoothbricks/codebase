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
export const SMOO_NX_RELEASE_TAG_PATTERN = '{projectName}@{version}';
const extraCommitScopes = ['release'];

export function applyFixableMonorepoDefaults(root: string): void {
  applyNxProjectNameDefaults(root);
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

export function applyWorkspaceDependencyDefaults(root: string): void {
  const workspaceNames = new Set(getWorkspacePackages(root).map((pkg) => pkg.name));
  for (const pkg of listPackageJsonRecords(root)) {
    let changed = fixWorkspaceDependencyRanges(pkg.json, workspaceNames);
    changed = applyPackageScriptPolicy(pkg.json, pkg.path, workspaceNames) || changed;
    if (changed) {
      writeJsonObject(pkg.packageJsonPath, pkg.json);
      console.log(`updated        ${pkg.path}/package.json workspace dependency policy`);
    } else {
      console.log(`unchanged      ${pkg.path}/package.json workspace dependency policy`);
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
    failures += validatePackageScriptPolicy(pkg.json, pkg.path, workspaceNames);
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
    const options = getOrCreateRecord(target, 'options');
    changed = setStringProperty(options, 'command', command) || changed;
    changed = setStringProperty(options, 'cwd', '{projectRoot}') || changed;
    for (const [name, value] of Object.entries(rewrite.env)) {
      const env = getOrCreateRecord(options, 'env');
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
    const options = target ? recordProperty(target, 'options') : null;
    const command = options ? stringProperty(options, 'command') : null;
    if (!target || stringProperty(target, 'executor') !== 'nx:run-commands' || !options || !command) {
      console.error(`${packagePath}: nx.targets.${rewrite.targetName} must use nx:run-commands with options.command`);
      failures++;
      continue;
    }
    if (stringProperty(options, 'cwd') !== '{projectRoot}') {
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
  const trimmed = command.trim();
  if (/^wrangler\s+build(?:\s|$)/.test(trimmed)) {
    return 'build';
  }
  return scriptName;
}

function nxRunAlias(projectName: string, targetName: string, continuous: boolean): string {
  const flags = continuous ? ' --tui=false --outputStyle=stream' : '';
  return `nx run ${projectName}:${targetName}${flags}`;
}

function expectedTargetDependencies(targetName: string): string[] {
  return targetName === 'preview' ? ['build'] : ['^build'];
}

function setStringArrayProperty(record: Record<string, unknown>, key: string, value: string[]): boolean {
  const current = record[key];
  if (
    Array.isArray(current) &&
    current.length === value.length &&
    current.every((entry, index) => entry === value[index])
  ) {
    return false;
  }
  record[key] = value;
  return true;
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
  const match = /^nx\s+run\s+(.+):([^\s]+)(?:\s|$)/.exec(command.trim());
  if (!match?.[1] || !match[2]) {
    return null;
  }
  return { projectName: match[1], targetName: match[2] };
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
