import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { getOrCreateRecord, readJsonObject, recordProperty, setStringProperty, writeJsonObject } from '../lib/json.js';
import { getWorkspacePackages } from '../lib/workspace.js';

interface RequiredDependency {
  name: string;
  fallbackVersion: string;
  minimumVersion?: string;
  prefix?: string;
}

const rootDevDependencies: RequiredDependency[] = [
  { name: '@biomejs/biome', fallbackVersion: '^2.3.5', minimumVersion: '2.3.0', prefix: '^' },
  { name: '@nx/js', fallbackVersion: '22.0.3', minimumVersion: '22.0.0' },
  { name: 'eslint', fallbackVersion: '^9.39.1', minimumVersion: '9.39.0', prefix: '^' },
  { name: 'eslint-stdout', fallbackVersion: 'workspace:*' },
  { name: 'nx', fallbackVersion: '22.5.4', minimumVersion: '22.5.0' },
  { name: 'prettier', fallbackVersion: '^3.6.1', minimumVersion: '3.6.0', prefix: '^' },
  { name: 'typescript', fallbackVersion: '^5.9.3', minimumVersion: '5.9.0', prefix: '^' },
];

const cliPackageName = '@smoothbricks/cli';

const requiredDevenvPackages = ['bun', 'git', 'git-format-staged', 'jq', 'alejandra', 'coreutils', 'gnutar'];
const allowedNodePackages = ['nodejs_24', 'nodejs_latest'];

export async function applyToolConfigDefaults(root: string): Promise<void> {
  await applyRootDevDependencyDefaults(root);
  applyToolingPackageDefaults(root);
  applyToolingWorkspaceDefault(root);
  applyDevenvPackageDefaults(root);
}

export function validateToolConfig(root: string): number {
  return (
    validateRootDevDependencies(root) +
    validateToolingPackage(root) +
    validateToolingWorkspace(root) +
    validateDevenvPackages(root)
  );
}

export async function applyRootDevDependencyDefaults(root: string): Promise<void> {
  const path = join(root, 'package.json');
  const pkg = readJsonObject(path);
  if (!pkg) {
    return;
  }
  let changed = false;
  const devDependencies = getOrCreateRecord(pkg, 'devDependencies');
  for (const dependency of rootDevDependencies) {
    const current = devDependencies[dependency.name];
    if (typeof current !== 'string' || !satisfiesDependencyPolicy(current, dependency)) {
      const version = await resolveDependencyVersion(dependency);
      changed = setStringProperty(devDependencies, dependency.name, version) || changed;
    }
  }
  if (delete devDependencies['@smoothbricks/cli']) {
    changed = true;
  }
  if (changed) {
    writeJsonObject(path, pkg);
    console.log('updated        package.json workspace tool dependencies');
  } else {
    console.log('unchanged      package.json workspace tool dependencies');
  }
}

export function applyToolingPackageDefaults(root: string): void {
  const path = join(root, 'tooling', 'package.json');
  const pkg = readJsonObject(path) ?? { name: toolingPackageName(root), private: true, dependencies: {} };
  let changed = false;
  changed = setStringProperty(pkg, 'name', toolingPackageName(root)) || changed;
  if (pkg.private !== true) {
    pkg.private = true;
    changed = true;
  }
  const dependencies = getOrCreateRecord(pkg, 'dependencies');
  changed = setStringProperty(dependencies, cliPackageName, cliDependencyRange(root)) || changed;
  if (changed || !existsSync(path)) {
    mkdirSync(dirname(path), { recursive: true });
    writeJsonObject(path, pkg);
    console.log('updated        tooling/package.json tooling dependencies');
  } else {
    console.log('unchanged      tooling/package.json tooling dependencies');
  }
}

export function applyToolingWorkspaceDefault(root: string): void {
  const path = join(root, 'package.json');
  const pkg = readJsonObject(path);
  if (!pkg) {
    return;
  }
  if (addWorkspacePattern(pkg, 'tooling')) {
    writeJsonObject(path, pkg);
    console.log('updated        package.json tooling workspace');
  } else {
    console.log('unchanged      package.json tooling workspace');
  }
}

export function applyDevenvPackageDefaults(root: string): void {
  const path = join(root, 'tooling', 'direnv', 'devenv.nix');
  if (!existsSync(path)) {
    return;
  }
  let content = readFileSync(path, 'utf8');
  let changed = false;
  if (!allowedNodePackages.some((name) => hasNixPackage(content, name))) {
    const next = addNixPackage(content, 'nodejs_latest', '# Node.js for workspace tooling');
    changed = next !== content || changed;
    content = next;
  }
  for (const name of requiredDevenvPackages) {
    if (hasNixPackage(content, name)) {
      continue;
    }
    const next = addNixPackage(content, name, nixPackageComment(name));
    changed = next !== content || changed;
    content = next;
  }
  if (changed) {
    writeFileSync(path, content);
    console.log('updated        tooling/direnv/devenv.nix packages');
  } else {
    console.log('unchanged      tooling/direnv/devenv.nix packages');
  }
}

export function validateRootDevDependencies(root: string): number {
  const pkg = readJsonObject(join(root, 'package.json'));
  if (!pkg) {
    console.error('package.json not found or invalid');
    return 1;
  }
  const devDependencies = recordProperty(pkg, 'devDependencies');
  let failures = 0;
  for (const dependency of rootDevDependencies) {
    const version = devDependencies?.[dependency.name];
    if (typeof version !== 'string') {
      console.error(`package.json devDependencies.${dependency.name} must be defined`);
      failures++;
    } else if (!satisfiesDependencyPolicy(version, dependency)) {
      console.error(
        `package.json devDependencies.${dependency.name} must be >= ${formatMinimum(dependency)}; found ${version}`,
      );
      failures++;
    }
  }
  if (typeof devDependencies?.[cliPackageName] === 'string') {
    console.error(`package.json devDependencies.${cliPackageName} must move to tooling/package.json dependencies`);
    failures++;
  }
  return failures;
}

export function validateToolingPackage(root: string): number {
  const path = join(root, 'tooling', 'package.json');
  const pkg = readJsonObject(path);
  if (!pkg) {
    console.error('tooling/package.json not found or invalid');
    return 1;
  }
  const dependencies = recordProperty(pkg, 'dependencies');
  let failures = 0;
  const expectedName = toolingPackageName(root);
  const actualName = typeof pkg.name === 'string' ? pkg.name : null;
  if (actualName !== expectedName) {
    console.error(`tooling/package.json name must be ${expectedName}`);
    failures++;
  }
  const expectedCliRange = cliDependencyRange(root);
  const actualCliRange = dependencies?.[cliPackageName];
  if (actualCliRange !== expectedCliRange) {
    console.error(`tooling/package.json dependencies.${cliPackageName} must be ${expectedCliRange}`);
    failures++;
  }
  return failures;
}

export function validateToolingWorkspace(root: string): number {
  const pkg = readJsonObject(join(root, 'package.json'));
  if (!pkg) {
    console.error('package.json not found or invalid');
    return 1;
  }
  if (!hasWorkspacePattern(pkg, 'tooling')) {
    console.error('package.json workspaces must include tooling so tooling/package.json participates in installs');
    return 1;
  }
  return 0;
}

export function validateDevenvPackages(root: string): number {
  const path = join(root, 'tooling', 'direnv', 'devenv.nix');
  if (!existsSync(path)) {
    console.error('tooling/direnv/devenv.nix not found');
    return 1;
  }
  const content = readFileSync(path, 'utf8');
  let failures = 0;
  if (!allowedNodePackages.some((name) => hasNixPackage(content, name))) {
    console.error(`tooling/direnv/devenv.nix packages must include ${allowedNodePackages.join(' or ')}`);
    failures++;
  }
  for (const name of requiredDevenvPackages) {
    if (!hasNixPackage(content, name)) {
      console.error(`tooling/direnv/devenv.nix packages must include ${name}`);
      failures++;
    }
  }
  return failures;
}

function addNixPackage(content: string, name: string, comment: string): string {
  const packageLine = `    ${name}${comment ? ` ${comment}` : ''}\n`;
  if (hasNixPackage(content, name)) {
    return content;
  }
  const packagesStart = content.indexOf('  packages = with pkgs; [');
  if (packagesStart === -1) {
    return content;
  }
  const insertAt = content.indexOf('\n', packagesStart) + 1;
  return `${content.slice(0, insertAt)}${packageLine}${content.slice(insertAt)}`;
}

function hasNixPackage(content: string, name: string): boolean {
  return new RegExp(`(^|\\s)${escapeRegex(name)}(\\s|#|$)`, 'm').test(content);
}

function nixPackageComment(name: string): string {
  if (name === 'coreutils') {
    return '# Provides fmt for commit message wrapping';
  }
  if (name === 'gnutar') {
    return '# Tarball inspection for package validation';
  }
  if (name === 'git') {
    return '# Git hooks and repository inspection';
  }
  return '';
}

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function satisfiesDependencyPolicy(version: string, dependency: RequiredDependency): boolean {
  if (dependency.minimumVersion === undefined) {
    return version === dependency.fallbackVersion;
  }
  const parsed = parseVersion(version);
  const minimum = parseVersion(dependency.minimumVersion);
  if (!parsed || !minimum) {
    return false;
  }
  return compareVersions(parsed, minimum) >= 0;
}

function formatMinimum(dependency: RequiredDependency): string {
  return dependency.minimumVersion ?? dependency.fallbackVersion;
}

function toolingPackageName(root: string): string {
  const rootPackage = readJsonObject(join(root, 'package.json'));
  const name = typeof rootPackage?.name === 'string' ? rootPackage.name : null;
  const scope = name?.match(/^(@[^/]+)\//)?.[1];
  return scope ? `${scope}/tooling` : 'tooling';
}

function cliDependencyRange(root: string): string {
  return workspaceHasCliPackage(root) ? 'workspace:*' : `^${currentCliVersion()}`;
}

function workspaceHasCliPackage(root: string): boolean {
  try {
    return getWorkspacePackages(root).some((pkg) => pkg.name === cliPackageName);
  } catch {
    return false;
  }
}

function currentCliVersion(): string {
  const pkg = readJsonObject(fileURLToPath(new URL('../../package.json', import.meta.url)));
  const version = typeof pkg?.version === 'string' ? pkg.version : null;
  if (!version) {
    throw new Error('Unable to read @smoothbricks/cli package version.');
  }
  return version;
}

async function resolveDependencyVersion(dependency: RequiredDependency): Promise<string> {
  if (!dependency.minimumVersion) {
    return dependency.fallbackVersion;
  }
  const latest = await fetchLatestPatchVersion(dependency.name, dependency.minimumVersion);
  return `${dependency.prefix ?? ''}${latest ?? stripRangePrefix(dependency.fallbackVersion)}`;
}

async function fetchLatestPatchVersion(packageName: string, minimumVersion: string): Promise<string | null> {
  const minorRange = sameMajorMinorRange(minimumVersion);
  const url = `https://registry.npmjs.org/${encodeURIComponent(packageName).replace('%40', '@')}`;
  const response = await fetch(url, { headers: { accept: 'application/vnd.npm.install-v1+json' } });
  if (!response.ok) {
    return null;
  }
  const body: unknown = await response.json();
  if (!isRegistryPackument(body)) {
    return null;
  }
  return latestVersionInSameMajorMinor(Object.keys(body.versions), minorRange);
}

function sameMajorMinorRange(minimumVersion: string): Version | null {
  return parseVersion(minimumVersion);
}

interface Version {
  major: number;
  minor: number;
  patch: number;
}

function parseVersion(version: string): Version | null {
  const match = /^[~^]?(\d+)\.(\d+)\.(\d+)(?:[-+].*)?$/.exec(version.trim());
  if (!match?.[1] || !match[2] || !match[3]) {
    return null;
  }
  return { major: Number(match[1]), minor: Number(match[2]), patch: Number(match[3]) };
}

function latestVersionInSameMajorMinor(versions: string[], minimum: Version | null): string | null {
  if (!minimum) {
    return null;
  }
  let latest: Version | null = null;
  for (const raw of versions) {
    const version = parseVersion(raw);
    if (!version || version.major !== minimum.major || version.minor !== minimum.minor) {
      continue;
    }
    if (compareVersions(version, minimum) < 0) {
      continue;
    }
    if (!latest || compareVersions(version, latest) > 0) {
      latest = version;
    }
  }
  return latest ? `${latest.major}.${latest.minor}.${latest.patch}` : null;
}

function compareVersions(left: Version, right: Version): number {
  return left.major - right.major || left.minor - right.minor || left.patch - right.patch;
}

function isRegistryPackument(value: unknown): value is { versions: Record<string, unknown> } {
  return typeof value === 'object' && value !== null && 'versions' in value && isObjectRecord(value.versions);
}

function isObjectRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function stripRangePrefix(version: string): string {
  return version.replace(/^[~^]/, '');
}

function addWorkspacePattern(pkg: Record<string, unknown>, pattern: string): boolean {
  const workspaces = pkg.workspaces;
  if (Array.isArray(workspaces)) {
    if (workspaces.includes(pattern)) {
      return false;
    }
    workspaces.push(pattern);
    return true;
  }
  if (isObjectRecord(workspaces) && Array.isArray(workspaces.packages)) {
    if (workspaces.packages.includes(pattern)) {
      return false;
    }
    workspaces.packages.push(pattern);
    return true;
  }
  pkg.workspaces = ['packages/*', pattern];
  return true;
}

function hasWorkspacePattern(pkg: Record<string, unknown>, pattern: string): boolean {
  const workspaces = pkg.workspaces;
  if (Array.isArray(workspaces)) {
    return workspaces.includes(pattern);
  }
  return isObjectRecord(workspaces) && Array.isArray(workspaces.packages) && workspaces.packages.includes(pattern);
}
