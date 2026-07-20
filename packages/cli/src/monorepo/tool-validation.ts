import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import typia from 'typia';
import { cliPackageVersion, isSmoothBricksCodebasePackageName } from '../lib/cli-package.js';
import {
  ensureDependencyMap,
  readJsonObject,
  setPackageStringField,
  setStringProperty,
  writeJsonObject,
} from '../lib/json.js';
import { type PackageJson, readPackageJsonObject } from '../lib/workspace.js';

interface RequiredDependency {
  name: string;
  fallbackVersion: string;
  minimumVersion?: string;
  prefix?: string;
  useWorkspaceRangeInCodebase?: boolean;
}

export interface ToolPolicy {
  isSmoothBricksCodebase: boolean;
  toolingPackageName: string;
  cliDependencyRange: string;
}

export interface ToolContext {
  rootPackage: PackageJson | null;
  policy: ToolPolicy;
}

interface RegistryPackument {
  versions: Record<string, unknown>;
  'dist-tags'?: Record<string, string>;
}

const isRegistryPackument = typia.createIs<RegistryPackument>();
const rootDevDependencies: RequiredDependency[] = [
  { name: '@biomejs/biome', fallbackVersion: '^2.3.5', minimumVersion: '2.3.0', prefix: '^' },
  { name: '@nx/js', fallbackVersion: '23.1.0', minimumVersion: '23.1.0' },
  {
    name: '@smoothbricks/nx-plugin',
    fallbackVersion: '^0.3.0',
    minimumVersion: '0.3.0',
    prefix: '^',
    useWorkspaceRangeInCodebase: true,
  },
  { name: 'eslint', fallbackVersion: '^9.39.1', minimumVersion: '9.39.0', prefix: '^' },
  {
    name: 'eslint-stdout',
    fallbackVersion: '^1.1.1',
    minimumVersion: '1.1.1',
    prefix: '^',
    useWorkspaceRangeInCodebase: true,
  },
  { name: 'nx', fallbackVersion: '23.1.0', minimumVersion: '23.1.0' },
  { name: 'prettier', fallbackVersion: '^3.6.1', minimumVersion: '3.6.0', prefix: '^' },
  { name: 'ttsc', fallbackVersion: '^0.18.4', minimumVersion: '0.18.4', prefix: '^' },
  // Nx and typescript-eslint still load the TypeScript JS API (6.x).
  // Compilation is exclusively delegated to ttsc by the Nx plugin targets.
  { name: 'typescript', fallbackVersion: '^5.9.3', minimumVersion: '5.9.0', prefix: '^' },
  // TS7 native compiler for ttsc (TTSC_TSGO_BINARY). Not the unscoped API package.
  // npm alias form required — Bun cannot install @typescript/typescript6 cleanly (bun#33834).
  { name: '@typescript/native', fallbackVersion: 'npm:typescript@^7.0.2' },
];

const cliPackageName = '@smoothbricks/cli';

const requiredDevenvPackages = ['bun', 'git', 'git-format-staged', 'jq', 'alejandra', 'coreutils', 'gnutar'];
// Any explicit nodejs provider is fine — the pinned major is the repo's choice
// (Lambda parity, org convergence, …). Whether the pins in package.json agree
// with the runtime is validated against the live PATH (validateRootRuntimeVersions),
// never against a version template here.
const nodePackagePattern = /(^|\s)nodejs(_\d+|_latest)?(\s|#|$)/m;

export async function applyToolConfigDefaults(root: string): Promise<void> {
  const context = await readToolContext(root);
  await applyRootPackageToolDefaults(root, context);
  applyToolingPackageDefaults(root, context.policy);
  applyDevenvPackageDefaults(root);
}

export async function validateToolConfig(root: string): Promise<number> {
  const context = await readToolContext(root);
  return (
    validateRootDevDependencies(context.policy, context.rootPackage) +
    validateToolingPackage(root, context.policy) +
    validateToolingWorkspace(context.rootPackage) +
    validateDevenvPackages(root)
  );
}

export async function applyRootDevDependencyDefaults(root: string, context: ToolContext): Promise<void> {
  const pkg = context.rootPackage;
  if (!pkg) {
    return;
  }
  let changed = false;
  const devDependencies = ensureDependencyMap(pkg, 'devDependencies');
  for (const dependency of rootDevDependencies) {
    const current = devDependencies[dependency.name];
    if (typeof current !== 'string' || !satisfiesDependencyPolicy(context.policy, current, dependency)) {
      const version = await resolveDependencyVersion(context.policy, dependency);
      changed = setStringProperty(devDependencies, dependency.name, version) || changed;
    }
  }
  if (delete devDependencies['@smoothbricks/cli']) {
    changed = true;
  }
  if (changed) {
    writeJsonObject(join(root, 'package.json'), pkg);
    console.log('updated        package.json workspace tool dependencies');
  } else {
    console.log('unchanged      package.json workspace tool dependencies');
  }
}

async function applyRootPackageToolDefaults(root: string, context: ToolContext): Promise<void> {
  const pkg = context.rootPackage;
  if (!pkg) {
    return;
  }
  let dependencyChanged = false;
  let workspaceChanged = false;
  const devDependencies = ensureDependencyMap(pkg, 'devDependencies');
  for (const dependency of rootDevDependencies) {
    const current = devDependencies[dependency.name];
    if (typeof current !== 'string' || !satisfiesDependencyPolicy(context.policy, current, dependency)) {
      const version = await resolveDependencyVersion(context.policy, dependency);
      dependencyChanged = setStringProperty(devDependencies, dependency.name, version) || dependencyChanged;
    }
  }
  if (delete devDependencies[cliPackageName]) {
    dependencyChanged = true;
  }
  workspaceChanged = addWorkspacePattern(pkg, 'tooling');
  if (dependencyChanged || workspaceChanged) {
    writeJsonObject(join(root, 'package.json'), pkg);
  }
  console.log(
    dependencyChanged
      ? 'updated        package.json workspace tool dependencies'
      : 'unchanged      package.json workspace tool dependencies',
  );
  console.log(
    workspaceChanged
      ? 'updated        package.json tooling workspace'
      : 'unchanged      package.json tooling workspace',
  );
}

export function applyToolingPackageDefaults(root: string, policy: ToolPolicy): void {
  const path = join(root, 'tooling', 'package.json');
  const pkg = readJsonObject(path) ?? { name: policy.toolingPackageName, private: true, dependencies: {} };
  let changed = false;
  changed = setPackageStringField(pkg, 'name', policy.toolingPackageName) || changed;
  if (pkg.private !== true) {
    pkg.private = true;
    changed = true;
  }
  const dependencies = ensureDependencyMap(pkg, 'dependencies');
  changed = setStringProperty(dependencies, cliPackageName, policy.cliDependencyRange) || changed;
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
  if (!nodePackagePattern.test(content)) {
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

export function validateRootDevDependencies(policy: ToolPolicy, rootPackage: PackageJson | null): number {
  const pkg = rootPackage;
  if (!pkg) {
    console.error('package.json not found or invalid');
    return 1;
  }
  const devDependencies = pkg.devDependencies;
  let failures = 0;
  for (const dependency of rootDevDependencies) {
    const version = devDependencies?.[dependency.name];
    if (typeof version !== 'string') {
      console.error(`package.json devDependencies.${dependency.name} must be defined`);
      failures++;
    } else if (!satisfiesDependencyPolicy(policy, version, dependency)) {
      console.error(
        `package.json devDependencies.${dependency.name} must be ${formatExpectedDependency(policy, dependency)}; found ${version}`,
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

export function validateToolingPackage(root: string, policy: ToolPolicy): number {
  const path = join(root, 'tooling', 'package.json');
  const pkg = readJsonObject(path);
  if (!pkg) {
    console.error('tooling/package.json not found or invalid');
    return 1;
  }
  const dependencies = pkg.dependencies;
  let failures = 0;
  const actualName = pkg.name ?? null;
  if (actualName !== policy.toolingPackageName) {
    console.error(`tooling/package.json name must be ${policy.toolingPackageName}`);
    failures++;
  }
  const actualCliRange = dependencies?.[cliPackageName];
  if (actualCliRange !== policy.cliDependencyRange) {
    console.error(`tooling/package.json dependencies.${cliPackageName} must be ${policy.cliDependencyRange}`);
    failures++;
  }
  return failures;
}

export function validateToolingWorkspace(rootPackage: PackageJson | null): number {
  const pkg = rootPackage;
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
  if (!nodePackagePattern.test(content)) {
    console.error(
      'tooling/direnv/devenv.nix packages must include a nodejs provider (nodejs_<major> or nodejs_latest)',
    );
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

function satisfiesDependencyPolicy(policy: ToolPolicy, version: string, dependency: RequiredDependency): boolean {
  if (workspaceDependencyExpected(policy, dependency)) {
    return version === 'workspace:*';
  }
  // Dual-package native compiler: must stay an npm:typescript@7 alias, never unscoped "typescript".
  if (dependency.name === '@typescript/native') {
    return isTypeScriptNativeAlias(version);
  }
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

function isTypeScriptNativeAlias(version: string): boolean {
  // Accept npm:typescript@7, npm:typescript@^7.0.2, npm:typescript@~7.0.2, etc.
  return /^npm:typescript@(?:\^|~)?7(?:\.|$)/.test(version.trim());
}

function formatExpectedDependency(policy: ToolPolicy, dependency: RequiredDependency): string {
  if (workspaceDependencyExpected(policy, dependency)) {
    return 'workspace:*';
  }
  if (dependency.name === '@typescript/native') {
    return 'npm:typescript@^7 (TypeScript 7 native compiler alias for ttsc)';
  }
  return dependency.minimumVersion ? `>= ${dependency.minimumVersion}` : dependency.fallbackVersion;
}

export async function readToolContext(root: string): Promise<ToolContext> {
  const rootPackage = readPackageJsonObject(join(root, 'package.json'));
  const toolingPackage = readJsonObject(join(root, 'tooling', 'package.json'));
  const configuredCliRange = toolingPackage?.dependencies?.[cliPackageName];
  return {
    rootPackage,
    policy: await toolPolicy(rootPackage, typeof configuredCliRange === 'string' ? configuredCliRange : null),
  };
}

async function toolPolicy(rootPackage: PackageJson | null, configuredCliRange: string | null): Promise<ToolPolicy> {
  const name = rootPackage?.name ?? null;
  const isCodebase = isSmoothBricksCodebasePackageName(name ?? undefined);
  const toolingName = toolingPackageName(name);
  return {
    isSmoothBricksCodebase: isCodebase,
    toolingPackageName: toolingName,
    // Consumers pin the latest *published* CLI. Running a linked prerelease must not
    // freeze tooling/package.json on an older range or rewrite it to an unpublished -next.
    cliDependencyRange: isCodebase ? 'workspace:*' : await resolvePublishedCliDependencyRange(configuredCliRange),
  };
}

function toolingPackageName(rootName: string | null): string {
  const name = rootName;
  const scope = name?.match(/^(@[^/]+)\//)?.[1];
  return scope ? `${scope}/tooling` : 'tooling';
}

function workspaceDependencyExpected(policy: ToolPolicy, dependency: RequiredDependency): boolean {
  return dependency.useWorkspaceRangeInCodebase === true && policy.isSmoothBricksCodebase;
}

async function resolveDependencyVersion(policy: ToolPolicy, dependency: RequiredDependency): Promise<string> {
  if (workspaceDependencyExpected(policy, dependency)) {
    return 'workspace:*';
  }
  if (!dependency.minimumVersion) {
    return dependency.fallbackVersion;
  }
  const latest = await fetchLatestPatchVersion(dependency.name, dependency.minimumVersion);
  return `${dependency.prefix ?? ''}${latest ?? stripRangePrefix(dependency.fallbackVersion)}`;
}

/**
 * Consumer monorepos pin `@smoothbricks/cli` to the latest stable release.
 * Prefer npm `dist-tags.latest`. Never write a running `-next` package version into
 * consumer manifests; never leave an older pin just because the running CLI is prerelease.
 */
async function resolvePublishedCliDependencyRange(configuredCliRange: string | null): Promise<string> {
  const latest = await fetchLatestStableVersion(cliPackageName);
  if (latest) {
    return `^${latest}`;
  }
  // Registry unavailable: if the running CLI is a stable publish, use it.
  if (!cliPackageVersion.includes('-')) {
    return `^${cliPackageVersion}`;
  }
  // Linked prerelease + offline down: keep an existing installable stable pin.
  if (configuredCliRange && parseVersion(configuredCliRange)) {
    return configuredCliRange;
  }
  // Last resort: caret of the prerelease base (0.10.5-next.0 → ^0.10.5) so manifests stay valid.
  const base = parseVersion(cliPackageVersion);
  return base ? `^${base.major}.${base.minor}.${base.patch}` : `^${cliPackageVersion}`;
}

async function fetchLatestStableVersion(packageName: string): Promise<string | null> {
  const url = `https://registry.npmjs.org/${encodeURIComponent(packageName).replace('%40', '@')}`;
  const response = await fetch(url, { headers: { accept: 'application/vnd.npm.install-v1+json' } });
  if (!response.ok) {
    return null;
  }
  const body: unknown = await response.json();
  if (!isRegistryPackument(body)) {
    return null;
  }
  const tagged = body['dist-tags']?.latest;
  if (typeof tagged === 'string' && parseVersion(tagged) && !tagged.includes('-')) {
    return tagged;
  }
  let latest: Version | null = null;
  for (const raw of Object.keys(body.versions)) {
    if (raw.includes('-')) {
      continue;
    }
    const version = parseVersion(raw);
    if (!version) {
      continue;
    }
    if (!latest || compareVersions(version, latest) > 0) {
      latest = version;
    }
  }
  return latest ? `${latest.major}.${latest.minor}.${latest.patch}` : null;
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

function stripRangePrefix(version: string): string {
  return version.replace(/^[~^]/, '');
}

function addWorkspacePattern(pkg: PackageJson, pattern: string): boolean {
  const workspaces = pkg.workspaces;
  if (Array.isArray(workspaces)) {
    if (workspaces.includes(pattern)) {
      return false;
    }
    workspaces.push(pattern);
    return true;
  }
  if (workspaces && Array.isArray(workspaces.packages)) {
    if (workspaces.packages.includes(pattern)) {
      return false;
    }
    workspaces.packages.push(pattern);
    return true;
  }
  pkg.workspaces = ['packages/*', pattern];
  return true;
}

function hasWorkspacePattern(pkg: PackageJson, pattern: string): boolean {
  const workspaces = pkg.workspaces;
  if (Array.isArray(workspaces)) {
    return workspaces.includes(pattern);
  }
  return Boolean(workspaces && Array.isArray(workspaces.packages) && workspaces.packages.includes(pattern));
}
